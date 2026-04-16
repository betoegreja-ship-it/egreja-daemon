"""
Derivatives Trade Monitoring Module

Real-time P&L tracking and exit trigger evaluation for open derivatives positions.
Handles:
- Greeks-based P&L estimation (δΔS + ½γ(ΔS)² + θΔt + νΔσ)
- Stop-loss / take-profit / trailing stop
- Time-based exit (theta decay threshold)
- Edge collapse detection
- Legging risk monitoring on multi-leg positions
- Background monitoring loop

Thread-safe, MySQL-compatible.
"""

import logging
import math
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger('egreja.derivatives.monitoring')


# ── Data Structures ──────────────────────────────────────────────────

@dataclass
class PositionSnapshot:
    """Point-in-time snapshot of an open position."""
    trade_id: str
    strategy: str
    symbol: str
    structure_type: str
    notional: float
    # Current market state
    spot_price: float = 0.0
    entry_spot: float = 0.0
    # Greeks (portfolio-level)
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    # P&L
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    greeks_pnl: float = 0.0
    # Time
    time_in_trade_hours: float = 0.0
    days_to_expiry: int = 0
    # Risk
    edge_remaining: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ExitSignal:
    """Signal to close a position."""
    trade_id: str
    trigger: str   # 'STOP_LOSS', 'TAKE_PROFIT', 'TRAILING_STOP', 'TIME_EXIT',
                   # 'EDGE_COLLAPSE', 'THETA_DECAY', 'EXPIRY_APPROACH', 'MANUAL'
    reason: str
    urgency: str   # 'IMMEDIATE', 'NORMAL', 'LOW'
    estimated_pnl: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ── Exit Rules Configuration ────────────────────────────────────────

@dataclass
class ExitRules:
    """Exit trigger thresholds per strategy."""
    # Stop loss as fraction of notional (negative)
    stop_loss_pct: float = -0.02       # -2%
    # Take profit as fraction of notional
    take_profit_pct: float = 0.03      # +3%
    # Trailing stop: distance from peak P&L
    trailing_stop_pct: float = 0.005   # 0.5% from peak
    trailing_stop_enabled: bool = True
    # Time-based: max hours in trade
    max_hours_in_trade: float = 72.0   # 3 days
    # Expiry approach: close N days before expiry
    close_before_expiry_days: int = 2
    # Edge collapse: if remaining edge < threshold (bps)
    min_edge_bps: float = -50.0  # [v10.42] was 1.0 — too aggressive, trades closed immediately
    # Grace period: skip edge-collapse check during first N minutes after open
    edge_collapse_grace_min: float = 10.0  # [v10.42] 10 min warmup before edge checks
    # Theta decay: close if daily theta > X% of remaining edge
    theta_pnl_ratio_max: float = 0.5   # theta eats >50% of edge → exit


DEFAULT_EXIT_RULES = {
    'pcp': ExitRules(stop_loss_pct=-0.015, take_profit_pct=0.025, max_hours_in_trade=48),
    'fst': ExitRules(stop_loss_pct=-0.02, take_profit_pct=0.04, max_hours_in_trade=72),
    'roll_arb': ExitRules(stop_loss_pct=-0.015, take_profit_pct=0.03, max_hours_in_trade=120),
    'etf_basket': ExitRules(stop_loss_pct=-0.01, take_profit_pct=0.02, max_hours_in_trade=24),
    'skew_arb': ExitRules(stop_loss_pct=-0.025, take_profit_pct=0.05, max_hours_in_trade=96),
    'interlisted': ExitRules(stop_loss_pct=-0.02, take_profit_pct=0.035, max_hours_in_trade=48),
    'dividend_arb': ExitRules(stop_loss_pct=-0.01, take_profit_pct=0.02, max_hours_in_trade=168),
    'vol_arb': ExitRules(stop_loss_pct=-0.03, take_profit_pct=0.06, max_hours_in_trade=120,
                         trailing_stop_pct=0.01),
}


# ── Monitoring Engine ────────────────────────────────────────────────

class DerivativesMonitor:
    """
    Monitors open derivatives positions and generates exit signals.

    Usage:
        monitor = DerivativesMonitor(execution_engine, greeks_calc, provider_mgr)
        monitor.start_monitoring_loop(beat_fn)  # runs in background thread
    """

    def __init__(
        self,
        execution_engine,
        greeks_calculator,
        provider_mgr,
        learning_engine=None,
        get_db_fn=None,
        exit_rules: Dict[str, ExitRules] = None,
    ):
        """
        Args:
            execution_engine: DerivativesExecutionEngine instance
            greeks_calculator: GreeksCalculator from services.py
            provider_mgr: Market data provider manager
            learning_engine: DerivativesLearningEngine instance (for feedback)
            get_db_fn: Optional DB connection callable
            exit_rules: Per-strategy exit rules (defaults if None)
        """
        self._lock = threading.RLock()
        self.engine = execution_engine
        self.greeks = greeks_calculator
        self.provider = provider_mgr
        self.learner = learning_engine
        self.get_db = get_db_fn
        self.exit_rules = exit_rules or DEFAULT_EXIT_RULES

        # Peak P&L per trade (for trailing stop)
        self.peak_pnl: Dict[str, float] = defaultdict(float)

        # Entry spot prices per trade
        self.entry_spots: Dict[str, float] = {}

        # Snapshots history (bounded)
        self.snapshots: Dict[str, List[PositionSnapshot]] = defaultdict(list)
        self._max_snapshots = 500

        # Pending exit signals
        self.pending_exits: List[ExitSignal] = []

        # Stats
        self.stats = {
            'monitoring_cycles': 0,
            'exit_signals_generated': 0,
            'exits_by_trigger': defaultdict(int),
        }

        logger.info("Derivatives monitor initialized")

    # ── Monitoring Loop ──────────────────────────────────────────

    def monitoring_loop(self, beat_fn, log):
        """
        Background monitoring loop. Call from a daemon thread.

        Args:
            beat_fn: Heartbeat function (callable with loop_name)
            log: Logger instance
        """
        loop_name = "deriv_monitor_loop"

        # Startup beats
        for _ in range(3):
            beat_fn(loop_name)
            time.sleep(2)

        while True:
            try:
                beat_fn(loop_name)
                self._monitoring_cycle(log)
                self.stats['monitoring_cycles'] += 1
            except Exception as e:
                log.error(f"Derivatives monitor error: {e}\n{traceback.format_exc()}")

            time.sleep(15)  # Check every 15 seconds

    def _monitoring_cycle(self, log):
        """Single monitoring cycle across all active trades."""
        active_trades = self.engine.get_active_trades()

        # Log cycle heartbeat every 20 cycles (~5 min) for observability
        if self.stats['monitoring_cycles'] % 20 == 0:
            log.info(
                f"[deriv_monitor] cycle={self.stats['monitoring_cycles']} "
                f"active_trades={len(active_trades)} "
                f"tracked={len(self.snapshots)} "
                f"exits_generated={self.stats['exit_signals_generated']}"
            )

        if not active_trades:
            return

        for trade in active_trades:
            try:
                snapshot = self._compute_snapshot(trade)
                if snapshot is None:
                    continue

                # Store snapshot
                self.snapshots[trade.trade_id].append(snapshot)
                if len(self.snapshots[trade.trade_id]) > self._max_snapshots:
                    self.snapshots[trade.trade_id] = (
                        self.snapshots[trade.trade_id][-self._max_snapshots:]
                    )

                # Evaluate exit triggers
                exit_signal = self._evaluate_exits(trade, snapshot)
                if exit_signal:
                    self.pending_exits.append(exit_signal)
                    self.stats['exit_signals_generated'] += 1
                    self.stats['exits_by_trigger'][exit_signal.trigger] += 1

                    log.info(
                        f"EXIT SIGNAL [{exit_signal.trigger}] {trade.trade_id}: "
                        f"{exit_signal.reason} (pnl≈R${exit_signal.estimated_pnl:,.2f})"
                    )

                    # Execute the exit
                    self.engine.close_trade(
                        trade.trade_id,
                        realized_pnl=exit_signal.estimated_pnl,
                        close_reason=f"{exit_signal.trigger}: {exit_signal.reason}",
                    )

                    # Feed outcome to learning engine
                    if self.learner:
                        try:
                            from modules.derivatives.learning import TradeOutcome
                            avg_latency = (
                                sum(leg.latency_ms for leg in trade.legs) / len(trade.legs)
                                if trade.legs else 0
                            )
                            outcome = TradeOutcome(
                                trade_id=trade.trade_id,
                                strategy=trade.strategy,
                                symbol=trade.symbol,
                                structure_type=trade.structure_type,
                                expected_edge=trade.expected_edge,
                                realized_pnl=exit_signal.estimated_pnl,
                                slippage_total=sum(l.slippage * l.qty for l in trade.legs),
                                latency_avg_ms=avg_latency,
                                time_in_trade_hours=snapshot.time_in_trade_hours,
                                close_reason=exit_signal.trigger,
                                liquidity_score=trade.liquidity_score,
                                active_status=trade.active_status,
                                legs_count=len(trade.legs),
                                legging_incidents=trade.legging_incidents,
                            )
                            self.learner.record_outcome(outcome)

                            # Update slippage model per leg type
                            for leg in trade.legs:
                                if leg.intended_price > 0:
                                    slip_frac = leg.slippage / leg.intended_price
                                    self.learner.update_slippage_model(
                                        trade.strategy, leg.leg_type, slip_frac
                                    )
                        except Exception as le:
                            log.warning(f"Learning feedback error for {trade.trade_id}: {le}")

                    # Cleanup tracking
                    self.peak_pnl.pop(trade.trade_id, None)
                    self.entry_spots.pop(trade.trade_id, None)

            except Exception as e:
                log.warning(f"Monitor error for {trade.trade_id}: {e}")

    # ── Snapshot Computation ─────────────────────────────────────

    def _compute_snapshot(self, trade) -> Optional[PositionSnapshot]:
        """Compute current position snapshot with Greeks-based P&L."""
        with self._lock:
            try:
                # Get current spot — if unavailable, fall back to entry so we can
                # still evaluate time-based and expiry-based exits (critical for paper
                # trades that outlived their provider session).
                spot_price = 0.0
                try:
                    spot_quote = self.provider.get_spot(trade.symbol)
                    if spot_quote:
                        spot_price = spot_quote.mid
                except Exception:
                    spot_price = 0.0

                # Get or set entry spot
                if trade.trade_id not in self.entry_spots:
                    self.entry_spots[trade.trade_id] = spot_price
                entry_spot = self.entry_spots[trade.trade_id]
                if spot_price <= 0:
                    # No market data — use entry so delta_s = 0 and only time/expiry
                    # triggers fire.
                    spot_price = entry_spot

                # Calculate time in trade
                time_in_trade = (datetime.utcnow() - trade.opened_at).total_seconds() / 3600

                # Days to expiry — robust to multiple formats and negative drift.
                # days_to_expiry can be negative → caller uses it to trigger expiry exits.
                days_to_expiry = 0
                expired = False
                if trade.expiry:
                    exp_str = str(trade.expiry).strip()
                    exp_date = None
                    for fmt in ('%Y%m%d', '%Y-%m-%d', '%Y/%m/%d', '%d/%m/%Y'):
                        try:
                            exp_date = datetime.strptime(exp_str, fmt)
                            break
                        except ValueError:
                            continue
                    if exp_date is not None:
                        raw_days = (exp_date - datetime.utcnow()).days
                        days_to_expiry = max(0, raw_days)
                        if raw_days < 0:
                            expired = True

                # Calculate portfolio Greeks
                port_delta = 0.0
                port_gamma = 0.0
                port_theta = 0.0
                port_vega = 0.0

                for leg in trade.legs:
                    if leg.leg_type in ('CALL', 'PUT') and trade.strike > 0 and days_to_expiry > 0:
                        leg_greeks = self.greeks.calculate_greeks(
                            option_type=leg.leg_type,
                            spot=spot_price,
                            strike=trade.strike,
                            days_to_expiry=days_to_expiry,
                            volatility=0.30,  # default IV, ideally from IV engine
                        )
                        sign = 1 if leg.side == 'BUY' else -1
                        port_delta += leg_greeks.delta * leg.qty * sign
                        port_gamma += leg_greeks.gamma * leg.qty * sign
                        port_theta += leg_greeks.theta * leg.qty * sign
                        port_vega += leg_greeks.vega * leg.qty * sign
                    elif leg.leg_type == 'STOCK':
                        sign = 1 if leg.side == 'BUY' else -1
                        port_delta += sign * leg.qty

                # Greeks-based P&L: δΔS + ½γ(ΔS)² + θΔt
                delta_s = spot_price - entry_spot
                delta_t = time_in_trade / 24  # in days
                greeks_pnl = (
                    port_delta * delta_s
                    + 0.5 * port_gamma * delta_s ** 2
                    + port_theta * delta_t
                )

                # Simple unrealized P&L (from execution prices vs current)
                unrealized_pnl = greeks_pnl  # In paper mode, Greeks P&L is our best estimate
                unrealized_pnl_pct = unrealized_pnl / trade.notional if trade.notional > 0 else 0

                # Edge remaining estimate
                edge_remaining = trade.expected_edge - abs(
                    sum(leg.slippage * leg.qty for leg in trade.legs)
                ) + greeks_pnl

                snapshot = PositionSnapshot(
                    trade_id=trade.trade_id,
                    strategy=trade.strategy,
                    symbol=trade.symbol,
                    structure_type=trade.structure_type,
                    notional=trade.notional,
                    spot_price=spot_price,
                    entry_spot=entry_spot,
                    delta=port_delta,
                    gamma=port_gamma,
                    theta=port_theta,
                    vega=port_vega,
                    unrealized_pnl=round(unrealized_pnl, 2),
                    unrealized_pnl_pct=round(unrealized_pnl_pct, 6),
                    greeks_pnl=round(greeks_pnl, 2),
                    time_in_trade_hours=round(time_in_trade, 2),
                    days_to_expiry=days_to_expiry,
                    edge_remaining=round(edge_remaining, 4),
                )
                # Expose expired flag via attribute (PositionSnapshot is a dataclass)
                try:
                    snapshot.expired = expired  # type: ignore[attr-defined]
                except Exception:
                    pass

                # Update peak P&L for trailing stop
                if unrealized_pnl > self.peak_pnl.get(trade.trade_id, 0):
                    self.peak_pnl[trade.trade_id] = unrealized_pnl

                return snapshot

            except Exception as e:
                logger.warning(f"Snapshot computation failed for {trade.trade_id}: {e}")
                return None

    # ── Exit Evaluation ──────────────────────────────────────────

    def _evaluate_exits(self, trade, snapshot: PositionSnapshot) -> Optional[ExitSignal]:
        """Evaluate all exit triggers for a position."""
        rules = self.exit_rules.get(trade.strategy, ExitRules())

        # 0. Expired: option expiry date is already in the past → settle immediately.
        if getattr(snapshot, 'expired', False):
            return ExitSignal(
                trade_id=trade.trade_id,
                trigger='EXPIRED',
                reason=f"Expiry {trade.expiry} already in the past",
                urgency='IMMEDIATE',
                estimated_pnl=snapshot.unrealized_pnl,
            )

        # 1. Stop Loss
        if snapshot.unrealized_pnl_pct <= rules.stop_loss_pct:
            return ExitSignal(
                trade_id=trade.trade_id,
                trigger='STOP_LOSS',
                reason=(
                    f"P&L {snapshot.unrealized_pnl_pct:.2%} <= "
                    f"stop {rules.stop_loss_pct:.2%}"
                ),
                urgency='IMMEDIATE',
                estimated_pnl=snapshot.unrealized_pnl,
            )

        # 2. Take Profit
        if snapshot.unrealized_pnl_pct >= rules.take_profit_pct:
            return ExitSignal(
                trade_id=trade.trade_id,
                trigger='TAKE_PROFIT',
                reason=(
                    f"P&L {snapshot.unrealized_pnl_pct:.2%} >= "
                    f"target {rules.take_profit_pct:.2%}"
                ),
                urgency='NORMAL',
                estimated_pnl=snapshot.unrealized_pnl,
            )

        # 3. Trailing Stop
        if rules.trailing_stop_enabled and trade.notional > 0:
            peak = self.peak_pnl.get(trade.trade_id, 0)
            if peak > 0:
                drawdown_from_peak = (peak - snapshot.unrealized_pnl) / trade.notional
                if drawdown_from_peak >= rules.trailing_stop_pct:
                    return ExitSignal(
                        trade_id=trade.trade_id,
                        trigger='TRAILING_STOP',
                        reason=(
                            f"Drawdown {drawdown_from_peak:.2%} from peak "
                            f"R${peak:,.2f} >= trail {rules.trailing_stop_pct:.2%}"
                        ),
                        urgency='NORMAL',
                        estimated_pnl=snapshot.unrealized_pnl,
                    )

        # 4. Time Exit
        if snapshot.time_in_trade_hours >= rules.max_hours_in_trade:
            return ExitSignal(
                trade_id=trade.trade_id,
                trigger='TIME_EXIT',
                reason=(
                    f"In trade {snapshot.time_in_trade_hours:.1f}h >= "
                    f"max {rules.max_hours_in_trade:.0f}h"
                ),
                urgency='NORMAL',
                estimated_pnl=snapshot.unrealized_pnl,
            )

        # 5. Expiry Approach
        if 0 < snapshot.days_to_expiry <= rules.close_before_expiry_days:
            return ExitSignal(
                trade_id=trade.trade_id,
                trigger='EXPIRY_APPROACH',
                reason=(
                    f"Only {snapshot.days_to_expiry}d to expiry, "
                    f"threshold={rules.close_before_expiry_days}d"
                ),
                urgency='IMMEDIATE',
                estimated_pnl=snapshot.unrealized_pnl,
            )

        # 6. Edge Collapse (with grace period to avoid instant close)
        if trade.notional > 0:
            grace_min = getattr(rules, 'edge_collapse_grace_min', 10.0)
            if snapshot.time_in_trade_hours * 60 >= grace_min:
                edge_bps = (snapshot.edge_remaining / trade.notional) * 10_000
                if edge_bps < rules.min_edge_bps:
                    return ExitSignal(
                        trade_id=trade.trade_id,
                        trigger='EDGE_COLLAPSE',
                        reason=(
                            f"Remaining edge {edge_bps:.1f}bps < "
                            f"min {rules.min_edge_bps:.1f}bps"
                        ),
                        urgency='NORMAL',
                        estimated_pnl=snapshot.unrealized_pnl,
                    )

        # 7. Theta Decay
        if snapshot.edge_remaining > 0 and snapshot.theta != 0:
            daily_theta_cost = abs(snapshot.theta)
            theta_ratio = daily_theta_cost / snapshot.edge_remaining
            if theta_ratio > rules.theta_pnl_ratio_max:
                return ExitSignal(
                    trade_id=trade.trade_id,
                    trigger='THETA_DECAY',
                    reason=(
                        f"Daily theta R${daily_theta_cost:,.2f} = "
                        f"{theta_ratio:.0%} of remaining edge"
                    ),
                    urgency='LOW',
                    estimated_pnl=snapshot.unrealized_pnl,
                )

        return None

    # ── Queries ──────────────────────────────────────────────────

    def get_position_snapshot(self, trade_id: str) -> Optional[PositionSnapshot]:
        """Get latest snapshot for a trade."""
        with self._lock:
            snaps = self.snapshots.get(trade_id, [])
            return snaps[-1] if snaps else None

    def get_all_snapshots(self) -> Dict[str, PositionSnapshot]:
        """Get latest snapshot for all active trades."""
        with self._lock:
            result = {}
            for tid, snaps in self.snapshots.items():
                if snaps and tid in self.engine.active_trades:
                    result[tid] = snaps[-1]
            return result

    def get_pending_exits(self) -> List[ExitSignal]:
        """Get recent exit signals."""
        with self._lock:
            return list(self.pending_exits[-100:])

    def get_stats(self) -> Dict:
        """Get monitoring statistics."""
        with self._lock:
            return {
                **self.stats,
                'exits_by_trigger': dict(self.stats['exits_by_trigger']),
                'tracked_positions': len(self.snapshots),
            }
