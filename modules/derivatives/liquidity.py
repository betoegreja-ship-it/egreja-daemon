"""
Liquidity Score Engine for Derivatives Trading Infrastructure.

Computes liquidity scores for options/futures strategies across multiple dimensions,
categorizing execution tiers and managing promotion/demotion between status levels.
"""

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Optional, Tuple
from collections import defaultdict
import time

logger = logging.getLogger(__name__)


class ExecutionTier(Enum):
    """Execution tier based on liquidity score."""
    PAPER_FULL = "PAPER_FULL"          # Score >= 80
    PAPER_SMALL = "PAPER_SMALL"        # Score 65-79
    SHADOW_EXEC = "SHADOW_EXEC"        # Score 50-64
    OBSERVE = "OBSERVE"                # Score < 50


class PromotionEvent(Enum):
    """Types of status changes."""
    PROMOTION = "PROMOTION"
    DEMOTION = "DEMOTION"
    INITIALIZATION = "INITIALIZATION"


@dataclass
class SubScore:
    """Individual component score."""
    name: str
    value: float  # 0-100
    weight: float  # Proportion of total
    metadata: Dict = field(default_factory=dict)


@dataclass
class LiquidityResult:
    """Complete liquidity assessment result."""
    score: float  # 0-100 composite
    sub_scores: Dict[str, SubScore]
    tier: ExecutionTier
    timestamp: datetime
    asset: str
    strategy: str
    expiry: str
    strike: float
    window: str


@dataclass
class StatusChange:
    """Record of a tier change."""
    asset: str
    strategy: str
    from_tier: ExecutionTier
    to_tier: ExecutionTier
    reason: str
    timestamp: datetime
    event_type: PromotionEvent


class LiquidityScoreEngine:
    """
    Computes liquidity scores across 7 weighted dimensions.
    
    Score components:
    - SpreadScore (20%): bid/ask spread analysis
    - DepthScore (20%): volume at price levels
    - OIVolumeScore (15%): open interest and flow
    - ExecutionPlausibilityScore (15%): price execution feasibility
    - PersistenceScore (10%): liquidity stability over time
    - ExitLiquidityScore (10%): ability to unwind
    - DataQualityScore (10%): data reliability
    """

    WEIGHTS = {
        "spread_score": 0.20,
        "depth_score": 0.20,
        "oi_volume_score": 0.15,
        "execution_plausibility_score": 0.15,
        "persistence_score": 0.10,
        "exit_liquidity_score": 0.10,
        "data_quality_score": 0.10,
    }

    def __init__(self):
        self._lock = threading.RLock()
        self.history: Dict[Tuple, list] = defaultdict(list)
        
    def compute_score(
        self,
        asset: str,
        strategy: str,
        expiry: str,
        strike: float,
        window: str,
        market_data: Optional[Dict] = None,
    ) -> LiquidityResult:
        """
        Compute complete liquidity score.
        
        Args:
            asset: Symbol (e.g., 'PETR4', 'VALE5')
            strategy: Strategy type (e.g., 'FST', 'CALL_SPREAD')
            expiry: Expiration date (YYYYMMDD format)
            strike: Strike price
            window: Time window ('OPEN', 'MID', 'CLOSE')
            market_data: Optional dict with market snapshot
            
        Returns:
            LiquidityResult with composite score and sub-scores
        """
        with self._lock:
            timestamp = datetime.utcnow()
            market_data = market_data or {}
            
            sub_scores = {}
            
            # 1. Spread Score (20%) - relative bid/ask spread
            sub_scores["spread_score"] = self._compute_spread_score(
                asset, strategy, expiry, strike, window, market_data
            )
            
            # 2. Depth Score (20%) - volume at price levels
            sub_scores["depth_score"] = self._compute_depth_score(
                asset, strategy, expiry, strike, window, market_data
            )
            
            # 3. OI/Volume Score (15%) - open interest and flow
            sub_scores["oi_volume_score"] = self._compute_oi_volume_score(
                asset, strategy, expiry, strike, window, market_data
            )
            
            # 4. Execution Plausibility Score (15%)
            sub_scores["execution_plausibility_score"] = self._compute_execution_plausibility_score(
                asset, strategy, expiry, strike, window, market_data
            )
            
            # 5. Persistence Score (10%) - stability across day windows
            sub_scores["persistence_score"] = self._compute_persistence_score(
                asset, strategy, expiry, strike, market_data
            )
            
            # 6. Exit Liquidity Score (10%)
            sub_scores["exit_liquidity_score"] = self._compute_exit_liquidity_score(
                asset, strategy, expiry, strike, window, market_data
            )
            
            # 7. Data Quality Score (10%)
            sub_scores["data_quality_score"] = self._compute_data_quality_score(
                asset, strategy, market_data
            )
            
            # Weighted composite
            composite = sum(
                sub_scores[key].value * self.WEIGHTS[key]
                for key in self.WEIGHTS
            )
            composite = max(0, min(100, composite))
            
            # Determine tier
            tier = self._score_to_tier(composite)
            
            result = LiquidityResult(
                score=composite,
                sub_scores=sub_scores,
                tier=tier,
                timestamp=timestamp,
                asset=asset,
                strategy=strategy,
                expiry=expiry,
                strike=strike,
                window=window,
            )
            
            # Track history
            key = (asset, strategy, expiry, strike, window)
            self.history[key].append(result)
            if len(self.history[key]) > 1000:
                self.history[key] = self.history[key][-1000:]
            
            return result

    def _compute_spread_score(
        self, asset: str, strategy: str, expiry: str, strike: float,
        window: str, data: Dict
    ) -> SubScore:
        """Score bid/ask spread tightness."""
        bid = data.get("bid", 0)
        ask = data.get("ask", 0)
        last = data.get("last", (bid + ask) / 2 if bid and ask else 0)
        
        if not bid or not ask or last == 0:
            return SubScore("spread_score", 0, self.WEIGHTS["spread_score"])
        
        spread_bps = (ask - bid) / last * 10000 if last > 0 else 0
        mean_spread = data.get("mean_spread_bps", spread_bps)
        stress_spread = data.get("stress_spread_bps", spread_bps * 3)
        
        # Score: tighter spread = higher score
        # 0-5 bps = 100, 20+ bps = 0, linear interpolation
        base_score = max(0, 100 * (1 - spread_bps / 20)) if spread_bps >= 0 else 100
        
        # Adjust for stress scenarios
        stress_factor = 1.0 if stress_spread < spread_bps * 5 else 0.8
        
        score = base_score * stress_factor
        return SubScore(
            "spread_score",
            score,
            self.WEIGHTS["spread_score"],
            {
                "spread_bps": spread_bps,
                "mean_spread_bps": mean_spread,
                "stress_spread_bps": stress_spread,
            }
        )

    def _compute_depth_score(
        self, asset: str, strategy: str, expiry: str, strike: float,
        window: str, data: Dict
    ) -> SubScore:
        """Score book depth at various price levels."""
        volume_best = data.get("volume_best_level", 0)
        volume_1_2_ticks = data.get("volume_1_2_ticks", 0)
        book_notional = data.get("book_notional_usd", 0)
        
        # Target: 10k+ contracts at best, 50k+ within 2 ticks
        best_score = min(100, (volume_best / 10000) * 100) if volume_best > 0 else 0
        depth_score = min(100, (volume_1_2_ticks / 50000) * 100) if volume_1_2_ticks > 0 else 0
        notional_score = min(100, (book_notional / 5_000_000) * 100) if book_notional > 0 else 0
        
        composite = (best_score * 0.4 + depth_score * 0.4 + notional_score * 0.2)
        
        return SubScore(
            "depth_score",
            composite,
            self.WEIGHTS["depth_score"],
            {
                "volume_best": volume_best,
                "volume_1_2_ticks": volume_1_2_ticks,
                "book_notional": book_notional,
            }
        )

    def _compute_oi_volume_score(
        self, asset: str, strategy: str, expiry: str, strike: float,
        window: str, data: Dict
    ) -> SubScore:
        """Score open interest, daily volume, and flow consistency."""
        oi = data.get("open_interest", 0)
        daily_volume = data.get("daily_volume", 0)
        flow_consistency = data.get("flow_consistency", 0.8)  # 0-1
        
        # Target: 100k+ OI, 50k+ daily volume
        oi_score = min(100, (oi / 100000) * 100) if oi > 0 else 0
        vol_score = min(100, (daily_volume / 50000) * 100) if daily_volume > 0 else 0
        flow_score = flow_consistency * 100
        
        composite = (oi_score * 0.4 + vol_score * 0.4 + flow_score * 0.2)
        
        return SubScore(
            "oi_volume_score",
            composite,
            self.WEIGHTS["oi_volume_score"],
            {
                "open_interest": oi,
                "daily_volume": daily_volume,
                "flow_consistency": flow_consistency,
            }
        )

    def _compute_execution_plausibility_score(
        self, asset: str, strategy: str, expiry: str, strike: float,
        window: str, data: Dict
    ) -> SubScore:
        """Score likelihood of successful execution."""
        expected_price = data.get("expected_price", 0)
        executable_price = data.get("executable_price", expected_price)
        slippage_estimate = data.get("slippage_estimate", 0)
        depth_sufficient = data.get("depth_sufficient", True)
        multi_leg_feasible = data.get("multi_leg_feasible", True)
        
        if expected_price == 0:
            return SubScore("execution_plausibility_score", 0, self.WEIGHTS["execution_plausibility_score"])
        
        # Price slippage tolerance: 0.5-2% slippage = 100->50, >5% = 0
        slippage_pct = abs(slippage_estimate / expected_price * 100) if expected_price > 0 else 0
        slippage_score = max(0, 100 - (slippage_pct / 5) * 100)
        
        depth_score = 100 if depth_sufficient else 50
        multi_leg_score = 100 if multi_leg_feasible else 30
        
        composite = (slippage_score * 0.5 + depth_score * 0.3 + multi_leg_score * 0.2)
        
        return SubScore(
            "execution_plausibility_score",
            composite,
            self.WEIGHTS["execution_plausibility_score"],
            {
                "expected_price": expected_price,
                "executable_price": executable_price,
                "slippage_estimate": slippage_estimate,
                "depth_sufficient": depth_sufficient,
                "multi_leg_feasible": multi_leg_feasible,
            }
        )

    def _compute_persistence_score(
        self, asset: str, strategy: str, expiry: str, strike: float, data: Dict
    ) -> SubScore:
        """Score liquidity stability across trading windows."""
        open_liq = data.get("liquidity_open", 50)
        mid_liq = data.get("liquidity_mid", 50)
        close_liq = data.get("liquidity_close", 50)
        
        avg_liq = (open_liq + mid_liq + close_liq) / 3
        variance = (
            (open_liq - avg_liq) ** 2 +
            (mid_liq - avg_liq) ** 2 +
            (close_liq - avg_liq) ** 2
        ) / 3
        stability = max(0, 100 - variance)
        
        return SubScore(
            "persistence_score",
            stability,
            self.WEIGHTS["persistence_score"],
            {
                "open": open_liq,
                "mid": mid_liq,
                "close": close_liq,
                "variance": variance,
            }
        )

    def _compute_exit_liquidity_score(
        self, asset: str, strategy: str, expiry: str, strike: float,
        window: str, data: Dict
    ) -> SubScore:
        """Score ability to unwind position without edge collapse."""
        unwind_volume = data.get("unwind_volume", 0)
        unwind_spread = data.get("unwind_spread_bps", 50)
        edge_resilience = data.get("edge_resilience", 0.7)
        
        # Target: 50k+ contracts can be unwound, spread doesn't widen >20 bps
        volume_score = min(100, (unwind_volume / 50000) * 100) if unwind_volume > 0 else 0
        spread_score = max(0, 100 * (1 - unwind_spread / 100))
        resilience_score = edge_resilience * 100
        
        composite = (volume_score * 0.5 + spread_score * 0.3 + resilience_score * 0.2)
        
        return SubScore(
            "exit_liquidity_score",
            composite,
            self.WEIGHTS["exit_liquidity_score"],
            {
                "unwind_volume": unwind_volume,
                "unwind_spread_bps": unwind_spread,
                "edge_resilience": edge_resilience,
            }
        )

    def _compute_data_quality_score(
        self, asset: str, strategy: str, data: Dict
    ) -> SubScore:
        """Score data freshness and integrity."""
        stale_count = data.get("stale_count", 0)
        breaker_events = data.get("breaker_events", 0)
        gaps = data.get("gaps", 0)
        
        # Penalize each issue
        stale_penalty = min(50, stale_count * 5)
        breaker_penalty = min(30, breaker_events * 10)
        gap_penalty = min(20, gaps * 2)
        
        score = max(0, 100 - stale_penalty - breaker_penalty - gap_penalty)
        
        return SubScore(
            "data_quality_score",
            score,
            self.WEIGHTS["data_quality_score"],
            {
                "stale_count": stale_count,
                "breaker_events": breaker_events,
                "gaps": gaps,
            }
        )

    def _score_to_tier(self, score: float) -> ExecutionTier:
        """Map score to execution tier."""
        if score >= 80:
            return ExecutionTier.PAPER_FULL
        elif score >= 65:
            return ExecutionTier.PAPER_SMALL
        elif score >= 50:
            return ExecutionTier.SHADOW_EXEC
        else:
            return ExecutionTier.OBSERVE


class PromotionEngine:
    """
    Manages status transitions between execution tiers.
    Enforces rules for promotion/demotion based on time, performance, and risk metrics.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self.promotion_rules = {
            ExecutionTier.OBSERVE: {
                "target": ExecutionTier.SHADOW_EXEC,
                "min_score": 50,
                "min_days": 0,
                "conditions": ["data_quality_ok", "spread_acceptable", "oi_volume_min"],
            },
            ExecutionTier.SHADOW_EXEC: {
                "target": ExecutionTier.PAPER_SMALL,
                "min_score": 65,
                "min_days": 5,
                "conditions": ["fill_plausibility_ok", "legging_risk_low"],
            },
            ExecutionTier.PAPER_SMALL: {
                "target": ExecutionTier.PAPER_FULL,
                "min_score": 80,
                "min_days": 10,
                "conditions": ["multi_leg_completion_ok", "slippage_in_band"],
            },
        }

    def can_promote(
        self,
        current_tier: ExecutionTier,
        days_in_status: int,
        score: float,
        metrics: Dict,
    ) -> Tuple[bool, str]:
        """
        Determine if promotion is warranted.
        
        Args:
            current_tier: Current execution tier
            days_in_status: Days in current tier
            score: Current liquidity score
            metrics: Performance metrics dict
            
        Returns:
            (can_promote: bool, reason: str)
        """
        with self._lock:
            if current_tier not in self.promotion_rules:
                return False, "Tier does not support promotion"
            
            rule = self.promotion_rules[current_tier]
            
            if score < rule["min_score"]:
                return False, f"Score {score:.1f} < min {rule['min_score']}"
            
            if days_in_status < rule["min_days"]:
                return False, f"Only {days_in_status} days in status, need {rule['min_days']}"
            
            # Check conditions
            failures = []
            for condition in rule["conditions"]:
                if not metrics.get(condition, False):
                    failures.append(condition)
            
            if failures:
                return False, f"Conditions failed: {', '.join(failures)}"
            
            return True, "All promotion criteria met"

    def should_demote(
        self,
        current_tier: ExecutionTier,
        score: float,
        metrics: Dict,
    ) -> Tuple[bool, str]:
        """
        Determine if demotion is necessary.
        
        Args:
            current_tier: Current execution tier
            score: Current liquidity score
            metrics: Performance metrics dict
            
        Returns:
            (should_demote: bool, reason: str)
        """
        with self._lock:
            if current_tier == ExecutionTier.OBSERVE:
                return False, "Already at lowest tier"
            
            # Check if score drops below threshold
            min_score_for_tier = {
                ExecutionTier.PAPER_SMALL: 65,
                ExecutionTier.PAPER_FULL: 80,
                ExecutionTier.SHADOW_EXEC: 50,
            }
            
            if score < min_score_for_tier.get(current_tier, 0):
                return True, f"Score {score:.1f} below minimum for tier"
            
            # Check for data issues
            if metrics.get("data_stale", False):
                return True, "Data is stale"
            
            if metrics.get("breaker_events", 0) > 0:
                return True, "Circuit breaker events detected"
            
            if metrics.get("legging_incidents", 0) > 2:
                return True, "Too many legging incidents"
            
            if metrics.get("slippage_excess", False):
                return True, "Slippage exceeded acceptable band"
            
            fill_ratio = metrics.get("fill_ratio", 1.0)
            if fill_ratio < 0.7:
                return True, f"Fill ratio {fill_ratio:.2%} too low"
            
            return False, "No demotion triggered"


class ActiveStatusRegistry:
    """
    Tracks and persists execution status per (asset, strategy) pair.
    Provides thread-safe status management with promotion/demotion logic.
    """

    def __init__(self, db_connection=None):
        self._lock = threading.RLock()
        self.db_connection = db_connection
        self.registry: Dict[Tuple[str, str], Dict] = {}
        self.history: Dict[Tuple[str, str], list] = defaultdict(list)
        self.promotion_engine = PromotionEngine()

    def get_status(self, asset: str, strategy: str) -> Optional[ExecutionTier]:
        """Get current execution tier for (asset, strategy)."""
        with self._lock:
            key = (asset, strategy)
            if key in self.registry:
                return self.registry[key]["current_status"]
            return None

    def set_status(
        self,
        asset: str,
        strategy: str,
        status: ExecutionTier,
        reason: str = "",
    ) -> None:
        """Set execution tier and record change."""
        with self._lock:
            key = (asset, strategy)
            now = datetime.utcnow()
            
            if key in self.registry:
                old_status = self.registry[key]["current_status"]
            else:
                old_status = None
            
            self.registry[key] = {
                "current_status": status,
                "prev_status": old_status,
                "updated_at": now,
                "days_in_status": 0,
                "liquidity_score_avg": 0,
            }
            
            change = StatusChange(
                asset=asset,
                strategy=strategy,
                from_tier=old_status or status,
                to_tier=status,
                reason=reason,
                timestamp=now,
                event_type=PromotionEvent.INITIALIZATION if old_status is None else PromotionEvent.PROMOTION,
            )
            self.history[key].append(change)
            
            if self.db_connection:
                self._persist_to_db(asset, strategy, status)

    def promote(
        self,
        asset: str,
        strategy: str,
        reason: str = "",
    ) -> Tuple[bool, str]:
        """Attempt to promote to next tier."""
        with self._lock:
            key = (asset, strategy)
            current_tier = self.get_status(asset, strategy)
            
            if current_tier is None:
                return False, "No status registered for this pair"
            
            rule = self.promotion_engine.promotion_rules.get(current_tier)
            if not rule:
                return False, "Current tier cannot be promoted"
            
            new_tier = rule["target"]
            self.set_status(asset, strategy, new_tier, reason or "Promotion")
            logger.info(f"Promoted {asset}/{strategy} from {current_tier.value} to {new_tier.value}")
            return True, f"Promoted to {new_tier.value}"

    def demote(
        self,
        asset: str,
        strategy: str,
        reason: str = "",
    ) -> Tuple[bool, str]:
        """Demote to previous tier."""
        with self._lock:
            key = (asset, strategy)
            current_tier = self.get_status(asset, strategy)
            
            if current_tier == ExecutionTier.OBSERVE:
                return False, "Already at lowest tier"
            
            demotion_map = {
                ExecutionTier.PAPER_FULL: ExecutionTier.PAPER_SMALL,
                ExecutionTier.PAPER_SMALL: ExecutionTier.SHADOW_EXEC,
                ExecutionTier.SHADOW_EXEC: ExecutionTier.OBSERVE,
            }
            
            new_tier = demotion_map.get(current_tier)
            if not new_tier:
                return False, "Cannot demote from this tier"
            
            self.set_status(asset, strategy, new_tier, reason or "Demotion")
            logger.info(f"Demoted {asset}/{strategy} from {current_tier.value} to {new_tier.value}")
            return True, f"Demoted to {new_tier.value}"

    def _persist_to_db(self, asset: str, strategy: str, status: ExecutionTier) -> None:
        """Persist status to database."""
        if not self.db_connection:
            return
        
        try:
            # Implementation depends on DB schema
            # This is a placeholder for actual DB operations
            pass
        except Exception as e:
            logger.error(f"Failed to persist status for {asset}/{strategy}: {e}")

    def get_history(self, asset: str, strategy: str) -> list:
        """Get promotion/demotion history."""
        with self._lock:
            return list(self.history[(asset, strategy)])
