"""
Derivatives Adaptive Learning Module

Feedback loop that learns from closed trades to improve:
- Signal confidence calibration
- Slippage prediction
- Edge estimation accuracy
- Strategy parameter tuning
- Promotion/demotion decisions

Mirrors the learning engine used by stocks/arbi but adapted for
multi-leg derivatives with Greeks-based P&L attribution.

Thread-safe, MySQL-compatible.
"""

import logging
import math
import threading
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger('egreja.derivatives.learning')


# ── Data Structures ──────────────────────────────────────────────────

@dataclass
class TradeOutcome:
    """Outcome of a closed derivatives trade for learning."""
    trade_id: str
    strategy: str
    symbol: str
    structure_type: str
    expected_edge: float
    realized_pnl: float
    slippage_total: float
    latency_avg_ms: float
    time_in_trade_hours: float
    close_reason: str
    liquidity_score: float
    active_status: str
    legs_count: int
    legging_incidents: int
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class StrategyStats:
    """Aggregated statistics per strategy+symbol pair."""
    strategy: str
    symbol: str
    trade_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    total_pnl: float = 0.0
    avg_edge_expected: float = 0.0
    avg_edge_realized: float = 0.0
    edge_accuracy: float = 0.0       # realized/expected ratio
    avg_slippage: float = 0.0
    avg_latency_ms: float = 0.0
    legging_rate: float = 0.0
    sharpe: float = 0.0
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    confidence_adjustment: float = 1.0  # multiplier for future confidence
    last_updated: datetime = field(default_factory=datetime.utcnow)


# ── Learning Engine ──────────────────────────────────────────────────

class DerivativesLearningEngine:
    """
    Adaptive learning engine for derivatives strategies.

    Pipeline:
        1. Record trade outcome
        2. Update rolling statistics per (strategy, symbol)
        3. Compute confidence adjustment factor
        4. Update slippage prediction model
        5. Trigger promotion/demotion evaluation
        6. Persist learning data to DB

    Usage:
        learner = DerivativesLearningEngine(config, get_db_fn)
        learner.record_outcome(outcome)
        adj = learner.get_confidence_adjustment('pcp', 'PETR4')
    """

    # Minimum trades before learning kicks in
    MIN_TRADES_FOR_LEARNING = 5

    # Exponential decay for rolling statistics
    DECAY_FACTOR = 0.95

    # Maximum confidence adjustment (up or down)
    MAX_CONFIDENCE_ADJ = 2.0
    MIN_CONFIDENCE_ADJ = 0.3

    def __init__(self, config, get_db_fn=None):
        """
        Args:
            config: DerivativesConfig instance
            get_db_fn: Optional callable returning MySQL connection
        """
        self._lock = threading.RLock()
        self.config = config
        self.get_db = get_db_fn

        # Per-(strategy, symbol) stats
        self.stats: Dict[Tuple[str, str], StrategyStats] = {}

        # Raw outcomes for detailed analysis (bounded)
        self.outcomes: Dict[Tuple[str, str], List[TradeOutcome]] = defaultdict(list)
        self._max_outcomes = 500

        # Slippage model: running average per (strategy, leg_type)
        self.slippage_model: Dict[Tuple[str, str], float] = defaultdict(lambda: 0.001)

        # Edge prediction error history
        self.edge_errors: Dict[Tuple[str, str], List[float]] = defaultdict(list)

        logger.info("Derivatives learning engine initialized")

    # ── Record Outcomes ──────────────────────────────────────────

    def record_outcome(self, outcome: TradeOutcome):
        """
        Record a closed trade outcome and update statistics.

        Args:
            outcome: TradeOutcome from closed trade
        """
        with self._lock:
            key = (outcome.strategy, outcome.symbol)

            # Store raw outcome
            self.outcomes[key].append(outcome)
            if len(self.outcomes[key]) > self._max_outcomes:
                self.outcomes[key] = self.outcomes[key][-self._max_outcomes:]

            # Update stats
            stats = self.stats.get(key)
            if stats is None:
                stats = StrategyStats(strategy=outcome.strategy, symbol=outcome.symbol)
                self.stats[key] = stats

            stats.trade_count += 1
            stats.total_pnl += outcome.realized_pnl

            if outcome.realized_pnl > 0:
                stats.win_count += 1
            else:
                stats.loss_count += 1

            stats.win_rate = stats.win_count / stats.trade_count if stats.trade_count > 0 else 0

            # Exponential moving averages
            alpha = 1 - self.DECAY_FACTOR
            stats.avg_edge_expected = (
                self.DECAY_FACTOR * stats.avg_edge_expected
                + alpha * outcome.expected_edge
            )
            stats.avg_edge_realized = (
                self.DECAY_FACTOR * stats.avg_edge_realized
                + alpha * outcome.realized_pnl
            )
            stats.avg_slippage = (
                self.DECAY_FACTOR * stats.avg_slippage
                + alpha * outcome.slippage_total
            )
            stats.avg_latency_ms = (
                self.DECAY_FACTOR * stats.avg_latency_ms
                + alpha * outcome.latency_avg_ms
            )

            # Edge accuracy
            if stats.avg_edge_expected != 0:
                stats.edge_accuracy = stats.avg_edge_realized / stats.avg_edge_expected
            else:
                stats.edge_accuracy = 0

            # Edge prediction error
            edge_error = outcome.realized_pnl - outcome.expected_edge
            self.edge_errors[key].append(edge_error)
            if len(self.edge_errors[key]) > 200:
                self.edge_errors[key] = self.edge_errors[key][-200:]

            # Legging rate
            if stats.trade_count > 0:
                total_legging = sum(
                    o.legging_incidents for o in self.outcomes[key][-50:]
                )
                stats.legging_rate = total_legging / len(self.outcomes[key][-50:])

            # Compute derived metrics
            self._update_derived_metrics(key)

            # Persist
            self._persist_outcome(outcome)
            self._persist_stats(stats)

            stats.last_updated = datetime.utcnow()

            logger.info(
                f"Learning recorded: {outcome.strategy}/{outcome.symbol} "
                f"pnl=R${outcome.realized_pnl:,.2f}, "
                f"win_rate={stats.win_rate:.1%}, "
                f"conf_adj={stats.confidence_adjustment:.3f}"
            )

    def _update_derived_metrics(self, key: Tuple[str, str]):
        """Compute Sharpe, profit factor, max drawdown, confidence adjustment."""
        stats = self.stats[key]
        recent = self.outcomes[key][-50:]  # last 50 trades

        if len(recent) < self.MIN_TRADES_FOR_LEARNING:
            stats.confidence_adjustment = 1.0
            return

        pnls = [o.realized_pnl for o in recent]

        # Sharpe ratio
        if len(pnls) >= 2:
            mean_pnl = sum(pnls) / len(pnls)
            variance = sum((p - mean_pnl) ** 2 for p in pnls) / len(pnls)
            std_pnl = math.sqrt(variance) if variance > 0 else 1
            stats.sharpe = (mean_pnl / std_pnl) * math.sqrt(252) if std_pnl > 0 else 0
        else:
            stats.sharpe = 0

        # Profit factor
        gross_profit = sum(p for p in pnls if p > 0)
        gross_loss = abs(sum(p for p in pnls if p < 0))
        stats.profit_factor = gross_profit / gross_loss if gross_loss > 0 else (
            10.0 if gross_profit > 0 else 0
        )

        # Max drawdown
        cumulative = 0
        peak = 0
        max_dd = 0
        for pnl in pnls:
            cumulative += pnl
            peak = max(peak, cumulative)
            dd = peak - cumulative
            max_dd = max(max_dd, dd)
        stats.max_drawdown = max_dd

        # Confidence adjustment formula:
        # Base: edge_accuracy (capped 0.3-2.0)
        # Boost: win_rate bonus
        # Penalty: legging incidents, high drawdown
        base_adj = max(0.5, min(1.5, stats.edge_accuracy))

        # Win rate bonus: +20% if win rate > 60%, -20% if < 40%
        wr_bonus = 0
        if stats.win_rate > 0.6:
            wr_bonus = 0.2 * (stats.win_rate - 0.6) / 0.4
        elif stats.win_rate < 0.4:
            wr_bonus = -0.2 * (0.4 - stats.win_rate) / 0.4

        # Legging penalty
        legging_penalty = min(0.3, stats.legging_rate * 0.5)

        # Drawdown penalty (if drawdown > 2x average PnL magnitude)
        avg_mag = sum(abs(p) for p in pnls) / len(pnls) if pnls else 1
        dd_penalty = 0
        if avg_mag > 0 and max_dd > 2 * avg_mag:
            dd_penalty = min(0.2, (max_dd / avg_mag - 2) * 0.1)

        adj = base_adj + wr_bonus - legging_penalty - dd_penalty
        stats.confidence_adjustment = max(
            self.MIN_CONFIDENCE_ADJ,
            min(self.MAX_CONFIDENCE_ADJ, adj)
        )

    # ── Queries ──────────────────────────────────────────────────

    def get_confidence_adjustment(self, strategy: str, symbol: str) -> float:
        """
        Get confidence adjustment multiplier for a (strategy, symbol) pair.
        Returns 1.0 if insufficient data.
        """
        with self._lock:
            stats = self.stats.get((strategy, symbol))
            if stats and stats.trade_count >= self.MIN_TRADES_FOR_LEARNING:
                return stats.confidence_adjustment
            return 1.0

    def get_predicted_slippage(self, strategy: str, leg_type: str) -> float:
        """Get predicted slippage fraction for a leg type."""
        with self._lock:
            return self.slippage_model[(strategy, leg_type)]

    def get_strategy_stats(self, strategy: str = None) -> Dict[str, StrategyStats]:
        """Get all strategy stats, optionally filtered."""
        with self._lock:
            result = {}
            for key, stats in self.stats.items():
                if strategy is None or key[0] == strategy:
                    result[f"{key[0]}/{key[1]}"] = stats
            return result

    def get_edge_prediction_error(self, strategy: str, symbol: str) -> Dict:
        """Get edge prediction error statistics."""
        with self._lock:
            errors = self.edge_errors.get((strategy, symbol), [])
            if len(errors) < 3:
                return {'mean': 0, 'std': 0, 'count': len(errors)}

            mean_err = sum(errors) / len(errors)
            variance = sum((e - mean_err) ** 2 for e in errors) / len(errors)
            std_err = math.sqrt(variance)

            return {
                'mean': round(mean_err, 4),
                'std': round(std_err, 4),
                'count': len(errors),
                'bias': 'OVER_ESTIMATE' if mean_err < 0 else 'UNDER_ESTIMATE',
            }

    def get_learning_summary(self) -> Dict:
        """Get overall learning engine summary."""
        with self._lock:
            total_trades = sum(s.trade_count for s in self.stats.values())
            total_pnl = sum(s.total_pnl for s in self.stats.values())
            strategies_tracked = len(set(k[0] for k in self.stats.keys()))
            symbols_tracked = len(set(k[1] for k in self.stats.keys()))

            return {
                'total_trades_learned': total_trades,
                'total_pnl': round(total_pnl, 2),
                'strategies_tracked': strategies_tracked,
                'symbols_tracked': symbols_tracked,
                'pairs_tracked': len(self.stats),
                'avg_confidence_adj': (
                    round(
                        sum(s.confidence_adjustment for s in self.stats.values())
                        / len(self.stats), 3
                    )
                    if self.stats else 1.0
                ),
            }

    # ── Learning Loop ────────────────────────────────────────────

    def learning_loop(self, beat_fn, log, execution_engine):
        """
        Background learning loop. Periodically:
        - Aggregates closed trades
        - Updates strategy scorecards
        - Evaluates promotion/demotion criteria

        Args:
            beat_fn: Heartbeat callable
            log: Logger
            execution_engine: DerivativesExecutionEngine
        """
        loop_name = "deriv_learning_loop"

        for _ in range(5):
            beat_fn(loop_name)
            time.sleep(2)

        while True:
            try:
                beat_fn(loop_name)
                self._learning_cycle(log)
            except Exception as e:
                log.error(f"Learning loop error: {e}\n{traceback.format_exc()}")

            time.sleep(300)  # Every 5 minutes

    def _learning_cycle(self, log):
        """Single learning cycle: persist scorecards and evaluate promotion."""
        with self._lock:
            for key, stats in self.stats.items():
                if stats.trade_count < self.MIN_TRADES_FOR_LEARNING:
                    continue

                # Persist strategy scorecard
                self._persist_scorecard(stats)

                log.debug(
                    f"Learning [{key[0]}/{key[1]}]: "
                    f"trades={stats.trade_count}, win_rate={stats.win_rate:.1%}, "
                    f"sharpe={stats.sharpe:.2f}, conf_adj={stats.confidence_adjustment:.3f}"
                )

    # ── DB Persistence ───────────────────────────────────────────

    def _persist_outcome(self, outcome: TradeOutcome):
        """Persist trade outcome to learning outcomes table."""
        if not self.get_db:
            return
        conn = None
        try:
            conn = self.get_db()
            if conn is None:
                return
            cursor = conn.cursor()

            # Get confidence adjustment at time of recording
            key = (outcome.strategy, outcome.symbol)
            conf_adj = self.stats[key].confidence_adjustment if key in self.stats else 1.0

            cursor.execute(
                """INSERT INTO derivatives_learning_outcomes
                   (trade_id, strategy_type, symbol, structure_type,
                    expected_edge, realized_pnl, slippage_total,
                    latency_avg_ms, time_in_trade_hours, close_reason,
                    liquidity_score, active_status, legs_count,
                    legging_incidents, confidence_at_entry, confidence_adj_after)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    outcome.trade_id, outcome.strategy, outcome.symbol,
                    outcome.structure_type, outcome.expected_edge,
                    outcome.realized_pnl, outcome.slippage_total,
                    outcome.latency_avg_ms, outcome.time_in_trade_hours,
                    outcome.close_reason, outcome.liquidity_score,
                    outcome.active_status, outcome.legs_count,
                    outcome.legging_incidents, 0.65, conf_adj,
                ),
            )
            conn.commit()
            cursor.close()
        except Exception as e:
            logger.warning(f"Failed to persist learning outcome: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _persist_stats(self, stats: StrategyStats):
        """Persist strategy stats to DB."""
        if not self.get_db:
            return
        conn = None
        try:
            conn = self.get_db()
            if conn is None:
                return
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO strategy_scorecards
                   (strategy_type, symbol, period,
                    opportunities_seen, opportunities_approved, opportunities_rejected,
                    trades_executed, legging_incidents,
                    edge_realized_mean, edge_expected_mean,
                    slippage_mean, latency_mean, pnl_total,
                    sharpe, profit_factor, max_drawdown,
                    fill_ratio, multi_leg_completion_rate, data_quality_incidents,
                    timestamp)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    stats.strategy, stats.symbol, 'ROLLING_50',
                    stats.trade_count, stats.win_count, stats.loss_count,
                    stats.trade_count, 0,
                    stats.avg_edge_realized, stats.avg_edge_expected,
                    stats.avg_slippage, stats.avg_latency_ms,
                    stats.total_pnl,
                    stats.sharpe, stats.profit_factor, stats.max_drawdown,
                    stats.win_rate, 1.0 - stats.legging_rate, 0,
                    datetime.utcnow(),
                ),
            )
            conn.commit()
            cursor.close()
        except Exception as e:
            logger.warning(f"Failed to persist learning stats: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _persist_scorecard(self, stats: StrategyStats):
        """Alias for _persist_stats in scorecard context."""
        self._persist_stats(stats)

    # ── Slippage Model Update ────────────────────────────────────

    def update_slippage_model(self, strategy: str, leg_type: str, observed_slippage: float):
        """
        Update the slippage prediction model with a new observation.

        Args:
            strategy: Strategy name
            leg_type: Leg type (CALL, PUT, STOCK, FUTURE)
            observed_slippage: Observed slippage fraction
        """
        with self._lock:
            key = (strategy, leg_type)
            current = self.slippage_model[key]
            alpha = 0.1  # Learning rate
            self.slippage_model[key] = current * (1 - alpha) + observed_slippage * alpha
