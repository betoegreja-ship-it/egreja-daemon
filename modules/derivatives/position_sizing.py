"""
Derivatives Position Sizing Module

Dynamic position sizing based on:
- Liquidity score (from LiquidityScoreEngine)
- Strategy confidence / edge magnitude
- Capital availability
- Risk budget (Kelly fraction with safety cap)
- Greeks-based margin requirements

Thread-safe, pure-parameterized.
"""

import logging
import math
import threading
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

logger = logging.getLogger('egreja.derivatives.sizing')


@dataclass
class SizingResult:
    """Output of position sizing calculation."""
    notional: float          # Total notional to trade
    contracts: int           # Number of option/future contracts
    margin_required: float   # Estimated margin requirement
    confidence: float        # Signal confidence (0-1)
    kelly_fraction: float    # Raw Kelly fraction
    applied_fraction: float  # After safety cap
    risk_budget_used: float  # Fraction of available risk budget consumed
    reason: str              # Human-readable sizing rationale


class DerivativesPositionSizer:
    """
    Computes optimal position size for derivatives trades.

    Pipeline:
        1. Base size from strategy max_notional / max_positions
        2. Scale by liquidity tier (PAPER_FULL=1.0 … OBSERVE=0.0)
        3. Scale by edge confidence (Kelly-inspired)
        4. Cap by available capital and daily loss headroom
        5. Floor by minimum viable notional (covers fees)

    Usage:
        sizer = DerivativesPositionSizer(config)
        result = sizer.compute_size(
            strategy='pcp', symbol='PETR4',
            edge_bps=12.0, confidence=0.72,
            liquidity_tier='PAPER_FULL',
            capital_available=800_000,
            daily_loss_remaining=50_000,
        )
    """

    # Liquidity tier multipliers
    TIER_MULTIPLIERS = {
        'PAPER_FULL':  1.00,
        'PAPER_SMALL': 0.50,
        'SHADOW_EXEC': 0.0,   # shadow = no real capital
        'OBSERVE':     0.0,
    }

    # Minimum notional to justify fees (R$)
    MIN_NOTIONAL = 5_000.0

    # Maximum Kelly fraction (safety cap)
    MAX_KELLY = 0.04  # 4% of capital per trade

    # [v10.29d] Arb strategies have tiny edge_bps by nature (convergence, not direction)
    ARBITRAGE_STRATEGIES = {'pcp', 'fst', 'roll_arb', 'skew_arb', 'vol_arb', 'etf_basket', 'dividend_arb', 'interlisted'}
    PAPER_ARB_MIN_FRACTION = 0.25  # Paper mode: use at least 25% of tier-scaled base

    def __init__(self, config):
        """
        Args:
            config: DerivativesConfig instance
        """
        self._lock = threading.RLock()
        self.config = config

    def compute_size(
        self,
        strategy: str,
        symbol: str,
        edge_bps: float,
        confidence: float,
        liquidity_tier: str,
        capital_available: float,
        daily_loss_remaining: float,
        spot_price: float = 0.0,
        contract_multiplier: float = 1.0,
        margin_pct: float = 0.15,
    ) -> SizingResult:
        """
        Compute position size.

        Args:
            strategy: Strategy name
            symbol: Underlying symbol
            edge_bps: Expected edge in basis points
            confidence: Signal confidence (0.0 to 1.0)
            liquidity_tier: Current tier string
            capital_available: Available capital in R$
            daily_loss_remaining: Remaining daily loss budget in R$
            spot_price: Underlying spot price (for contract conversion)
            contract_multiplier: Contract multiplier (e.g., 1 for options, 5 for mini-index)
            margin_pct: Margin requirement as fraction

        Returns:
            SizingResult
        """
        with self._lock:
            strat_cfg = self.config.get_strategy_config(strategy)

            # Step 1: Base notional from strategy limits
            max_notional = strat_cfg.get('max_notional', 500_000)
            max_positions = strat_cfg.get('max_positions', 20)
            base_notional = max_notional / max(1, max_positions)

            # Step 2: Liquidity tier scaling
            tier_mult = self.TIER_MULTIPLIERS.get(liquidity_tier, 0.0)
            if tier_mult == 0.0:
                return SizingResult(
                    notional=0, contracts=0, margin_required=0,
                    confidence=confidence, kelly_fraction=0,
                    applied_fraction=0, risk_budget_used=0,
                    reason=f"Tier '{liquidity_tier}' does not allow capital deployment",
                )

            scaled_notional = base_notional * tier_mult

            # Step 3: Kelly-inspired confidence scaling
            # Kelly: f* = (p*b - q) / b  where p=confidence, b=edge, q=1-p
            # Simplified: fraction = confidence * edge_bps / 10000
            edge_frac = edge_bps / 10_000
            kelly_raw = confidence * edge_frac
            kelly_capped = min(kelly_raw, self.MAX_KELLY)
            kelly_capped = max(kelly_capped, 0.0)

            # Apply Kelly to capital
            kelly_notional = capital_available * kelly_capped

            # Take the minimum of Kelly-sized and strategy-base-sized
            notional = min(scaled_notional, kelly_notional)

            # [v10.29d] Paper mode: arb edges are 0.01-0.5 bps → Kelly gives ~R$50 → always rejected.
            # Floor at MIN_NOTIONAL for arb strategies in paper mode so trades actually execute.
            if (strategy.lower() in self.ARBITRAGE_STRATEGIES
                    and liquidity_tier.startswith('PAPER')
                    and notional < self.MIN_NOTIONAL):
                notional = max(self.MIN_NOTIONAL, scaled_notional * self.PAPER_ARB_MIN_FRACTION)

            # Step 4: Cap by available capital (margin) and daily loss headroom
            safety_factor = strat_cfg.get('safety_factor', 1.75)
            max_from_capital = capital_available / margin_pct if margin_pct > 0 else capital_available
            max_from_daily_loss = daily_loss_remaining * safety_factor

            notional = min(notional, max_from_capital, max_from_daily_loss)

            # Step 5: Floor by minimum viable
            if notional < self.MIN_NOTIONAL:
                return SizingResult(
                    notional=0, contracts=0, margin_required=0,
                    confidence=confidence, kelly_fraction=kelly_raw,
                    applied_fraction=kelly_capped, risk_budget_used=0,
                    reason=f"Sized notional R${notional:,.0f} below minimum R${self.MIN_NOTIONAL:,.0f}",
                )

            # Contract conversion
            contracts = 0
            if spot_price > 0 and contract_multiplier > 0:
                contracts = max(1, int(notional / (spot_price * contract_multiplier)))
                # Re-derive notional from integer contracts
                notional = contracts * spot_price * contract_multiplier

            margin_required = notional * margin_pct
            risk_budget_used = margin_required / capital_available if capital_available > 0 else 0

            return SizingResult(
                notional=round(notional, 2),
                contracts=contracts,
                margin_required=round(margin_required, 2),
                confidence=confidence,
                kelly_fraction=round(kelly_raw, 6),
                applied_fraction=round(kelly_capped, 6),
                risk_budget_used=round(risk_budget_used, 4),
                reason=(
                    f"Sized {contracts} contracts @ R${notional:,.0f} notional "
                    f"(tier={liquidity_tier}, conf={confidence:.2f}, edge={edge_bps:.1f}bps)"
                ),
            )

    def compute_greeks_margin(
        self,
        delta: float,
        gamma: float,
        vega: float,
        spot_price: float,
        notional: float,
    ) -> float:
        """
        Estimate margin requirement based on Greeks exposure.

        SPAN-like margin: max(delta_margin, vega_margin) + gamma_addon

        Args:
            delta: Portfolio delta
            gamma: Portfolio gamma
            vega: Portfolio vega
            spot_price: Current spot price
            notional: Position notional

        Returns:
            Estimated margin in R$
        """
        with self._lock:
            # Delta margin: 15% of delta-equivalent notional
            delta_equiv = abs(delta) * spot_price
            delta_margin = delta_equiv * 0.15

            # Vega margin: vega * 5 vol-points stress
            vega_margin = abs(vega) * 5.0

            # Gamma add-on: 0.5 * gamma * (10% spot move)^2
            spot_shock = spot_price * 0.10
            gamma_addon = 0.5 * abs(gamma) * spot_shock ** 2

            margin = max(delta_margin, vega_margin) + gamma_addon

            # Floor at 10% of notional
            margin = max(margin, notional * 0.10)

            return round(margin, 2)
