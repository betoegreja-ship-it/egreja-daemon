"""
Derivatives Capital Management Module

Manages capital allocation, daily loss limits, per-strategy budgets,
and reconciliation for the derivatives trading engine.
Thread-safe, pure-parameterized, MySQL-compatible (%s placeholders).
"""

import logging
import threading
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass, field

logger = logging.getLogger('egreja.derivatives.capital')


# ── Data Structures ─────────────────────────────────────────────────

@dataclass
class CapitalSnapshot:
    """Point-in-time capital state."""
    total_capital: float
    allocated: float
    available: float
    daily_pnl: float
    daily_loss_remaining: float
    positions_count: int
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class AllocationRecord:
    """Single capital allocation event."""
    trade_id: str
    strategy: str
    symbol: str
    amount: float
    direction: str  # 'ALLOCATE' or 'RELEASE'
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ── Capital Manager ─────────────────────────────────────────────────

class DerivativesCapitalManager:
    """
    Manages derivatives capital with:
    - Global capital pool with per-strategy sub-allocations
    - Daily P&L tracking with hard loss limits
    - Per-strategy notional limits from config
    - Margin/collateral reservation
    - Thread-safe operations

    Usage:
        cfg = get_config()
        mgr = DerivativesCapitalManager(cfg)
        ok, reason = mgr.request_allocation('trade_001', 'pcp', 'PETR4', 50_000)
        if ok:
            # execute trade ...
            mgr.release_allocation('trade_001', realized_pnl=1200.0)
    """

    def __init__(self, config, get_db_fn=None):
        """
        Args:
            config: DerivativesConfig instance
            get_db_fn: Optional callable returning MySQL connection
        """
        self._lock = threading.RLock()
        self.config = config
        self.get_db = get_db_fn

        # Capital state
        self.total_capital: float = config.initial_capital
        self.allocated: float = 0.0  # sum of all active allocations

        # Per-strategy tracking
        self.strategy_allocated: Dict[str, float] = defaultdict(float)
        self.strategy_pnl_today: Dict[str, float] = defaultdict(float)

        # Daily state
        self._current_date: date = date.today()
        self.daily_pnl: float = 0.0
        self.daily_trades: int = 0

        # Active allocations: trade_id → AllocationRecord
        self.active_allocations: Dict[str, AllocationRecord] = {}

        # Ledger history (bounded)
        self.ledger: List[AllocationRecord] = []
        self._max_ledger = 5000

        logger.info(
            f"Capital manager initialized: total=R${self.total_capital:,.0f}, "
            f"max_daily_loss=R${config.max_daily_loss_global:,.0f}"
        )

    # ── Daily Reset ──────────────────────────────────────────────

    def _check_day_rollover(self):
        """Reset daily counters if date changed. Must hold _lock."""
        today = date.today()
        if today != self._current_date:
            logger.info(
                f"Capital day rollover: {self._current_date} → {today}, "
                f"yesterday PnL=R${self.daily_pnl:,.2f}"
            )
            self._persist_daily_summary()
            self._current_date = today
            self.daily_pnl = 0.0
            self.daily_trades = 0
            self.strategy_pnl_today.clear()

    # ── Allocation ───────────────────────────────────────────────

    def request_allocation(
        self,
        trade_id: str,
        strategy: str,
        symbol: str,
        notional: float,
        margin_pct: float = 0.15,
    ) -> Tuple[bool, str]:
        """
        Request capital allocation for a new trade.

        Args:
            trade_id: Unique trade identifier
            strategy: Strategy name (e.g., 'pcp', 'fst')
            symbol: Underlying symbol
            notional: Total notional exposure requested
            margin_pct: Margin requirement as fraction (default 15%)

        Returns:
            (approved, reason)
        """
        with self._lock:
            self._check_day_rollover()

            margin_required = notional * margin_pct

            # 1. Check global daily loss limit
            if self.daily_pnl <= -self.config.max_daily_loss_global:
                return False, (
                    f"Global daily loss limit hit: "
                    f"R${self.daily_pnl:,.2f} <= -R${self.config.max_daily_loss_global:,.0f}"
                )

            # 2. Check available capital
            available = self.total_capital - self.allocated
            if margin_required > available:
                return False, (
                    f"Insufficient capital: need R${margin_required:,.0f}, "
                    f"available R${available:,.0f}"
                )

            # 3. Check per-strategy notional limit
            strat_cfg = self.config.get_strategy_config(strategy)
            max_notional = strat_cfg.get('max_notional', float('inf'))
            current_strat_alloc = self.strategy_allocated[strategy]
            if current_strat_alloc + notional > max_notional:
                return False, (
                    f"Strategy '{strategy}' notional limit: "
                    f"current R${current_strat_alloc:,.0f} + R${notional:,.0f} "
                    f"> max R${max_notional:,.0f}"
                )

            # 4. Check per-strategy daily loss limit
            strat_daily_loss_max = strat_cfg.get('max_daily_loss', float('inf'))
            strat_pnl = self.strategy_pnl_today[strategy]
            if strat_pnl <= -strat_daily_loss_max:
                return False, (
                    f"Strategy '{strategy}' daily loss limit: "
                    f"R${strat_pnl:,.2f} <= -R${strat_daily_loss_max:,.0f}"
                )

            # 5. Check per-strategy max positions
            max_positions = strat_cfg.get('max_positions', float('inf'))
            current_positions = sum(
                1 for a in self.active_allocations.values()
                if a.strategy == strategy
            )
            if current_positions >= max_positions:
                return False, (
                    f"Strategy '{strategy}' max positions reached: "
                    f"{current_positions}/{max_positions}"
                )

            # 6. Check duplicate trade_id
            if trade_id in self.active_allocations:
                return False, f"Trade '{trade_id}' already has active allocation"

            # ── Approve ──
            record = AllocationRecord(
                trade_id=trade_id,
                strategy=strategy,
                symbol=symbol,
                amount=margin_required,
                direction='ALLOCATE',
            )
            self.active_allocations[trade_id] = record
            self.allocated += margin_required
            self.strategy_allocated[strategy] += notional
            self.daily_trades += 1
            self._append_ledger(record)

            logger.info(
                f"Capital allocated: {trade_id} {strategy}/{symbol} "
                f"margin=R${margin_required:,.0f} (notional=R${notional:,.0f})"
            )
            return True, "Approved"

    def release_allocation(
        self,
        trade_id: str,
        realized_pnl: float = 0.0,
    ) -> Tuple[bool, str]:
        """
        Release capital after trade closes.

        Args:
            trade_id: Trade identifier
            realized_pnl: P&L realized on this trade

        Returns:
            (success, reason)
        """
        with self._lock:
            self._check_day_rollover()

            if trade_id not in self.active_allocations:
                return False, f"No active allocation for trade '{trade_id}'"

            record = self.active_allocations.pop(trade_id)
            self.allocated -= record.amount
            self.allocated = max(0, self.allocated)  # safety floor

            # Update P&L
            self.daily_pnl += realized_pnl
            self.strategy_pnl_today[record.strategy] += realized_pnl
            self.total_capital += realized_pnl

            release = AllocationRecord(
                trade_id=trade_id,
                strategy=record.strategy,
                symbol=record.symbol,
                amount=record.amount,
                direction='RELEASE',
            )
            self._append_ledger(release)

            # Recalculate strategy notional (approximate: release proportional)
            strat_cfg = self.config.get_strategy_config(record.strategy)
            margin_pct = 0.15
            approx_notional = record.amount / margin_pct if margin_pct > 0 else record.amount
            self.strategy_allocated[record.strategy] = max(
                0, self.strategy_allocated[record.strategy] - approx_notional
            )

            logger.info(
                f"Capital released: {trade_id} pnl=R${realized_pnl:,.2f}, "
                f"daily_pnl=R${self.daily_pnl:,.2f}"
            )
            return True, "Released"

    # ── Queries ──────────────────────────────────────────────────

    def get_snapshot(self) -> CapitalSnapshot:
        """Get current capital state."""
        with self._lock:
            self._check_day_rollover()
            return CapitalSnapshot(
                total_capital=self.total_capital,
                allocated=self.allocated,
                available=self.total_capital - self.allocated,
                daily_pnl=self.daily_pnl,
                daily_loss_remaining=self.config.max_daily_loss_global + self.daily_pnl,
                positions_count=len(self.active_allocations),
            )

    def get_strategy_summary(self) -> Dict[str, Dict]:
        """Get per-strategy capital summary."""
        with self._lock:
            self._check_day_rollover()
            result = {}
            for strategy in self.config.active_strategies:
                strat_cfg = self.config.get_strategy_config(strategy)
                active_count = sum(
                    1 for a in self.active_allocations.values()
                    if a.strategy == strategy
                )
                result[strategy] = {
                    'allocated': self.strategy_allocated[strategy],
                    'max_notional': strat_cfg.get('max_notional', 0),
                    'utilization_pct': (
                        self.strategy_allocated[strategy]
                        / strat_cfg.get('max_notional', 1) * 100
                    ),
                    'daily_pnl': self.strategy_pnl_today[strategy],
                    'max_daily_loss': strat_cfg.get('max_daily_loss', 0),
                    'active_positions': active_count,
                    'max_positions': strat_cfg.get('max_positions', 0),
                }
            return result

    def is_trading_allowed(self, strategy: str = None) -> Tuple[bool, str]:
        """Check if trading is allowed globally or per-strategy."""
        with self._lock:
            self._check_day_rollover()

            # Global loss limit
            if self.daily_pnl <= -self.config.max_daily_loss_global:
                return False, "Global daily loss limit reached"

            if strategy:
                strat_cfg = self.config.get_strategy_config(strategy)
                strat_daily_max = strat_cfg.get('max_daily_loss', float('inf'))
                if self.strategy_pnl_today[strategy] <= -strat_daily_max:
                    return False, f"Strategy '{strategy}' daily loss limit reached"

            return True, "Trading allowed"

    # ── Persistence ──────────────────────────────────────────────

    def _append_ledger(self, record: AllocationRecord):
        """Add to in-memory ledger, trim if needed."""
        self.ledger.append(record)
        if len(self.ledger) > self._max_ledger:
            self.ledger = self.ledger[-self._max_ledger:]

    def _persist_daily_summary(self):
        """Persist end-of-day summary to DB."""
        if not self.get_db:
            return
        conn = None
        try:
            conn = self.get_db()
            if conn is None:
                return
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO derivatives_daily_capital
                   (trade_date, total_capital, allocated, daily_pnl, trades_count, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                   total_capital=VALUES(total_capital),
                   allocated=VALUES(allocated),
                   daily_pnl=VALUES(daily_pnl),
                   trades_count=VALUES(trades_count)
                """,
                (
                    self._current_date.isoformat(),
                    self.total_capital,
                    self.allocated,
                    self.daily_pnl,
                    self.daily_trades,
                    datetime.utcnow(),
                ),
            )
            conn.commit()
            cursor.close()
        except Exception as e:
            logger.warning(f"Failed to persist daily capital summary: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def reconcile(self) -> Dict:
        """
        Reconcile in-memory state against DB positions.
        Returns discrepancy report.
        """
        with self._lock:
            report = {
                'timestamp': datetime.utcnow().isoformat(),
                'in_memory_positions': len(self.active_allocations),
                'in_memory_allocated': self.allocated,
                'discrepancies': [],
            }

            if not self.get_db:
                report['db_available'] = False
                return report

            conn = None
            try:
                conn = self.get_db()
                if conn is None:
                    report['db_available'] = False
                    return report

                cursor = conn.cursor(dictionary=True)
                cursor.execute(
                    "SELECT trade_id, strategy_type, symbol, status "
                    "FROM strategy_master_trades WHERE status = 'OPEN'"
                )
                db_positions = {row['trade_id']: row for row in cursor.fetchall()}
                cursor.close()

                report['db_positions'] = len(db_positions)

                # Check for orphans
                for tid in self.active_allocations:
                    if tid not in db_positions:
                        report['discrepancies'].append({
                            'type': 'MEMORY_ONLY',
                            'trade_id': tid,
                        })

                for tid in db_positions:
                    if tid not in self.active_allocations:
                        report['discrepancies'].append({
                            'type': 'DB_ONLY',
                            'trade_id': tid,
                        })

                report['db_available'] = True

            except Exception as e:
                logger.warning(f"Reconciliation error: {e}")
                report['error'] = str(e)
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

            return report
