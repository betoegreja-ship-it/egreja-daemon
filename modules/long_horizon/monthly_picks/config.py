"""
Monthly Picks — Configuration & Enums.

Define sleeve status levels, selection rules, exit triggers,
and portfolio governance parameters.
"""

import os
import logging
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger('egreja.monthly_picks.config')


# ──────────────────────────────────────────────────────────────
# SLEEVE STATUS  (mesmos níveis de maturidade dos derivativos)
# ──────────────────────────────────────────────────────────────

class SleeveStatus(Enum):
    """Execution maturity level — mirrors derivatives ActiveStatus."""
    OBSERVE = "observe"
    SHADOW_EXEC = "shadow_exec"
    PAPER_SMALL = "paper_small"
    PAPER_FULL = "paper_full"
    DISABLED = "disabled"


class PositionStatus(Enum):
    """Lifecycle state of a pick position."""
    OPEN = "open"
    REDUCED = "reduced"
    CLOSED = "closed"
    SHADOW = "shadow"          # shadow-mode: tracked but not "bought"


class ExitReason(Enum):
    """Standardized exit reasons for auditability."""
    TARGET_HIT = "target_hit"           # +15%
    STOP_LOSS = "stop_loss"             # -8%
    TRAILING_STOP = "trailing_stop"     # fell 5% from peak
    TIMEOUT = "timeout"                 # 9 months max
    SCORE_LOW = "score_low"             # current score < 50
    SCORE_DROP = "score_drop"           # score fell 15+ pts from entry
    THESIS_BROKEN = "thesis_broken"     # qualitative thesis invalidation
    HUMAN_OVERRIDE = "human_override"   # manual close
    CORRELATION_BREACH = "correlation_breach"
    SECTOR_REBALANCE = "sector_rebalance"


class ReviewAction(Enum):
    """Actions a weekly review can produce."""
    HOLD = "hold"
    REDUCE = "reduce"
    CLOSE = "close"
    MONITOR = "monitor"         # flag for closer watch


class LearningEvent(Enum):
    """Structured events sent to the brain via learning_bridge."""
    MONTHLY_PICK_OPENED = "MONTHLY_PICK_OPENED"
    MONTHLY_PICK_REJECTED = "MONTHLY_PICK_REJECTED"
    MONTHLY_PICK_CLOSED = "MONTHLY_PICK_CLOSED"
    MONTHLY_PICK_THESIS_BROKEN = "MONTHLY_PICK_THESIS_BROKEN"
    MONTHLY_PICK_TIMEOUT = "MONTHLY_PICK_TIMEOUT"
    MONTHLY_PICK_TARGET_HIT = "MONTHLY_PICK_TARGET_HIT"
    MONTHLY_PICK_STOP_LOSS = "MONTHLY_PICK_STOP_LOSS"
    MONTHLY_PICK_REVIEW_HOLD = "MONTHLY_PICK_REVIEW_HOLD"
    MONTHLY_PICK_REVIEW_REDUCE = "MONTHLY_PICK_REVIEW_REDUCE"


# ──────────────────────────────────────────────────────────────
# PROMOTION THRESHOLDS
# ──────────────────────────────────────────────────────────────

PROMOTION_RULES = {
    SleeveStatus.OBSERVE: {
        'target': SleeveStatus.SHADOW_EXEC,
        'min_score': 50.0,
        'min_days': 0,
        'min_completed_scans': 1,
    },
    SleeveStatus.SHADOW_EXEC: {
        'target': SleeveStatus.PAPER_SMALL,
        'min_score': 65.0,
        'min_days': 30,
        'min_completed_scans': 2,
        'min_shadow_trades': 3,
    },
    SleeveStatus.PAPER_SMALL: {
        'target': SleeveStatus.PAPER_FULL,
        'min_score': 80.0,
        'min_days': 60,
        'min_win_rate': 0.45,
        'min_completed_trades': 6,
    },
}


# ──────────────────────────────────────────────────────────────
# MAIN CONFIG
# ──────────────────────────────────────────────────────────────

@dataclass
class MonthlyPicksConfig:
    """Central configuration for the Monthly Picks sleeve."""

    # ── Sleeve control ─────────────────────────────────────
    enabled: bool = True
    initial_status: str = "observe"     # start conservatively

    # ── Selection rules ────────────────────────────────────
    picks_per_month: int = 3
    candidates_per_scan: int = 10
    min_score_entry: float = 65.0
    min_data_quality: float = 60.0
    min_liquidity_score: float = 50.0
    max_sector_concentration: int = 2   # max N picks per sector
    max_correlation: float = 0.75       # max pairwise correlation
    avoid_open_risk_triggers: bool = True

    # ── Exit triggers ──────────────────────────────────────
    target_gain_pct: float = 15.0
    stop_loss_pct: float = -8.0
    trailing_stop_pct: float = 5.0      # from peak
    max_hold_months: int = 9
    min_score_keep: float = 50.0
    score_drop_threshold: float = 15.0

    # ── Capital ────────────────────────────────────────────
    capital_per_pick: float = 100_000.0
    max_total_allocated: float = 2_000_000.0
    max_daily_loss: float = 40_000.0

    # ── Scheduling ─────────────────────────────────────────
    scan_day_of_month: int = 1          # 1st business day
    review_day_of_week: str = 'monday'
    review_hour: int = 9                # 09:00 local

    # ── Conviction weighting for final pick selection ──────
    # Prioritize conviction × inverse risk
    conviction_weight: float = 0.6
    risk_weight: float = 0.4

    def __post_init__(self):
        """Apply env overrides if present."""
        if os.environ.get('MP_PICKS_PER_MONTH'):
            self.picks_per_month = int(os.environ['MP_PICKS_PER_MONTH'])
        if os.environ.get('MP_CAPITAL_PER_PICK'):
            self.capital_per_pick = float(os.environ['MP_CAPITAL_PER_PICK'])
        if os.environ.get('MP_TARGET_GAIN'):
            self.target_gain_pct = float(os.environ['MP_TARGET_GAIN'])
        if os.environ.get('MP_STOP_LOSS'):
            self.stop_loss_pct = float(os.environ['MP_STOP_LOSS'])
        if os.environ.get('MP_INITIAL_STATUS'):
            self.initial_status = os.environ['MP_INITIAL_STATUS']


# ── Singleton ──────────────────────────────────────────────

_config: Optional[MonthlyPicksConfig] = None


def get_config() -> MonthlyPicksConfig:
    """Return singleton config, creating if needed."""
    global _config
    if _config is None:
        _config = MonthlyPicksConfig()
        logger.info(f'[MP] Config loaded: status={_config.initial_status}, '
                     f'picks/month={_config.picks_per_month}, '
                     f'target={_config.target_gain_pct}%')
    return _config


def reset_config():
    """Reset singleton (for testing)."""
    global _config
    _config = None
