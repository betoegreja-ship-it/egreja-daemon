"""
Derivatives Trading Configuration Module

Environment-driven configuration for all derivatives trading strategies.
Supports multiple asset classes, strategies, and execution modes.
"""

import os
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from enum import Enum

logger = logging.getLogger('egreja.derivatives')


class ActiveStatus(Enum):
    """Execution status for strategies and symbols."""
    OBSERVE = "observe"
    SHADOW_EXEC = "shadow_exec"
    PAPER_SMALL = "paper_small"
    PAPER_FULL = "paper_full"
    DISABLED = "disabled"


class B3Fees:
    """B3 fee structure (percentage basis)."""
    EMOLUMENTS = 0.005  # 0.005%
    REGISTRATION = 0.014  # 0.014%
    LIQUIDATION = 0.0275  # 0.0275%
    PIS_COFINS = 9.25  # 9.25% tax on fees


@dataclass
class PCPConfig:
    """Put-Call Parity strategy configuration."""
    enabled: bool = True
    max_notional: float = 500_000.0
    max_daily_loss: float = 10_000.0
    max_positions: int = 50
    safety_factor: float = 1.75
    min_edge_bps: float = 5.0
    
    # Strike selection
    strikes: List[str] = field(default_factory=lambda: ["ATM", "1_ITM", "2_OTM"])
    vencimentos: List[int] = field(default_factory=lambda: [10, 20, 40])  # days to expiry
    
    # Order book requirements
    depth_min: int = 10  # minimum quotes on each side
    oi_min: int = 100  # minimum open interest
    volume_min: int = 50  # minimum daily volume
    spread_max_pct: float = 2.0  # maximum bid-ask spread %
    
    # Heartbeat check interval (seconds)
    heartbeat_timeout: int = 30


@dataclass
class FSTConfig:
    """Futures Spread Trading strategy configuration."""
    enabled: bool = True
    max_notional: float = 1_000_000.0
    max_daily_loss: float = 20_000.0
    max_positions: int = 30
    safety_factor: float = 2.0
    min_edge_bps: float = 30.0
    
    # FST-specific parameters
    caso0_min_days: int = 5  # minimum days between contract dates
    edge_min_bps: float = 30.0
    confirmation_min: int = 2  # confirmations before execution
    
    # Heartbeat check interval (seconds)
    heartbeat_timeout: int = 45


@dataclass
class LiquidityScore:
    """Liquidity tier thresholds."""
    PAPER_FULL = 80
    PAPER_SMALL = 65
    SHADOW_EXEC = 50
    OBSERVE = 0  # anything below SHADOW_EXEC


@dataclass
class PromotionDemotionRules:
    """Strategy state transition rules."""
    min_days_observe: int = 5
    min_days_shadow: int = 5
    min_days_paper_small: int = 10


@dataclass
class DerivativesConfig:
    """Root configuration for derivatives trading module."""
    
    # Capital allocation for derivatives trading
    initial_capital: float = float(os.getenv("DERIVATIVES_CAPITAL", "5_000_000"))
    max_daily_loss_global: float = float(os.getenv("DERIVATIVES_MAX_DAILY_LOSS", "100_000"))

    # Market rates (from environment, with defaults)
    selic_rate: float = float(os.getenv("SELIC_RATE", "14.75"))
    cdi_rate: float = float(os.getenv("CDI_RATE", "14.90"))
    
    # Execution mode (never LIVE for now)
    derivatives_mode: str = os.getenv("DERIVATIVES_MODE", "PAPER")
    
    # Asset universes
    universe_tier_a: List[str] = field(
        default_factory=lambda: [
            "PETR4", "VALE3", "BOVA11", "ITUB4",
            "BBDC4", "BBAS3", "ABEV3", "B3SA3"
        ]
    )
    universe_tier_b: List[str] = field(
        default_factory=lambda: ["PETR4", "VALE3", "BOVA11", "ITUB4"]
    )
    
    # Active strategies
    active_strategies: List[str] = field(
        default_factory=lambda: [
            "pcp",
            "fst",
            "roll_arb",
            "etf_basket",
            "skew_arb",
            "interlisted",
            "dividend_arb",
            "vol_arb",
        ]
    )
    
    # Per-strategy configurations
    pcp: PCPConfig = field(default_factory=PCPConfig)
    fst: FSTConfig = field(default_factory=FSTConfig)
    
    # Generic strategy config (for other strategies)
    strategy_defaults: Dict[str, Dict] = field(
        default_factory=lambda: {
            "roll_arb": {
                "enabled": True,
                "max_notional": 300_000.0,
                "max_daily_loss": 8_000.0,
                "max_positions": 25,
                "safety_factor": 1.8,
                "min_edge_bps": 8.0,
                "heartbeat_timeout": 40,
            },
            "etf_basket": {
                "enabled": True,
                "max_notional": 750_000.0,
                "max_daily_loss": 15_000.0,
                "max_positions": 40,
                "safety_factor": 1.6,
                "min_edge_bps": 4.0,
                "heartbeat_timeout": 35,
            },
            "skew_arb": {
                "enabled": True,
                "max_notional": 200_000.0,
                "max_daily_loss": 5_000.0,
                "max_positions": 15,
                "safety_factor": 2.0,
                "min_edge_bps": 15.0,
                "heartbeat_timeout": 50,
            },
            "interlisted": {
                "enabled": True,
                "max_notional": 400_000.0,
                "max_daily_loss": 10_000.0,
                "max_positions": 20,
                "safety_factor": 1.5,
                "min_edge_bps": 10.0,
                "heartbeat_timeout": 25,
            },
            "dividend_arb": {
                "enabled": True,
                "max_notional": 350_000.0,
                "max_daily_loss": 7_000.0,
                "max_positions": 30,
                "safety_factor": 1.4,
                "min_edge_bps": 20.0,
                "heartbeat_timeout": 60,
            },
            "vol_arb": {
                "enabled": True,
                "max_notional": 600_000.0,
                "max_daily_loss": 18_000.0,
                "max_positions": 35,
                "safety_factor": 1.9,
                "min_edge_bps": 12.0,
                "heartbeat_timeout": 45,
            },
        }
    )
    
    # Liquidity tier thresholds
    liquidity_tiers: LiquidityScore = field(default_factory=LiquidityScore)
    
    # Promotion/demotion rules (days in state before transition)
    promotion_demotion: PromotionDemotionRules = field(
        default_factory=PromotionDemotionRules
    )
    
    # B3 fee structure
    b3_fees: B3Fees = B3Fees()
    
    # Kill switch scopes
    kill_switch_scopes: List[str] = field(
        default_factory=lambda: [
            "system_wide",
            "all_derivatives",
            "all_options",
            "all_futures",
            "by_strategy",
            "by_symbol",
        ]
    )
    
    # Default heartbeat timeout for unspecified strategies (seconds)
    default_heartbeat_timeout: int = 40
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.derivatives_mode not in ["PAPER", "SHADOW", "LIVE"]:
            logger.warning(
                f"Invalid DERIVATIVES_MODE: {self.derivatives_mode}. Defaulting to PAPER."
            )
            self.derivatives_mode = "PAPER"
        
        if self.derivatives_mode == "LIVE":
            logger.error("LIVE mode is not permitted. Forcing to PAPER.")
            self.derivatives_mode = "PAPER"
        
        # Validate rates are positive
        if self.selic_rate <= 0:
            logger.warning(
                f"Invalid SELIC_RATE: {self.selic_rate}. Using default 14.75."
            )
            self.selic_rate = 14.75
        
        if self.cdi_rate <= 0:
            logger.warning(f"Invalid CDI_RATE: {self.cdi_rate}. Using default 14.90.")
            self.cdi_rate = 14.90
        
        logger.info(
            f"Derivatives config initialized: mode={self.derivatives_mode}, "
            f"capital=R${self.initial_capital:,.0f}, max_daily_loss=R${self.max_daily_loss_global:,.0f}, "
            f"selic={self.selic_rate}%, cdi={self.cdi_rate}%"
        )
    
    def get_strategy_config(self, strategy_name: str) -> Dict:
        """Get configuration dict for a given strategy."""
        if strategy_name == "pcp":
            return {
                "enabled": self.pcp.enabled,
                "max_notional": self.pcp.max_notional,
                "max_daily_loss": self.pcp.max_daily_loss,
                "max_positions": self.pcp.max_positions,
                "safety_factor": self.pcp.safety_factor,
                "min_edge_bps": self.pcp.min_edge_bps,
                "strikes": self.pcp.strikes,
                "vencimentos": self.pcp.vencimentos,
                "depth_min": self.pcp.depth_min,
                "oi_min": self.pcp.oi_min,
                "volume_min": self.pcp.volume_min,
                "spread_max_pct": self.pcp.spread_max_pct,
                "heartbeat_timeout": self.pcp.heartbeat_timeout,
            }
        elif strategy_name == "fst":
            return {
                "enabled": self.fst.enabled,
                "max_notional": self.fst.max_notional,
                "max_daily_loss": self.fst.max_daily_loss,
                "max_positions": self.fst.max_positions,
                "safety_factor": self.fst.safety_factor,
                "min_edge_bps": self.fst.min_edge_bps,
                "caso0_min_days": self.fst.caso0_min_days,
                "confirmation_min": self.fst.confirmation_min,
                "heartbeat_timeout": self.fst.heartbeat_timeout,
            }
        else:
            return self.strategy_defaults.get(
                strategy_name, {"heartbeat_timeout": self.default_heartbeat_timeout}
            )


# Global singleton instance
_config: Optional[DerivativesConfig] = None


def get_config() -> DerivativesConfig:
    """Get or create the global configuration instance."""
    global _config
    if _config is None:
        _config = DerivativesConfig()
    return _config


def reload_config() -> DerivativesConfig:
    """Reload configuration from environment."""
    global _config
    _config = DerivativesConfig()
    return _config
