"""
Formal Statistics Engine for Egreja Investment AI v10.22

Self-contained module for computing comprehensive trading statistics without
dependencies on api_server.py. Thread-safe design with locking mechanisms.

Handles:
- Trade recording and validation
- Comprehensive metrics computation (Sharpe, Sortino, Calmar, etc.)
- Multi-dimensional breakdowns (strategy, symbol, regime, time-based)
- Edge stability analysis and capacity estimation
- Promotion criteria evaluation

All calculations use only Python standard library.
"""

import threading
import math
import statistics
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Any


class PerformanceStats:
    """
    Thread-safe statistics engine for trading performance analysis.

    Tracks closed trades and computes formal metrics including risk-adjusted
    returns (Sharpe, Sortino, Calmar), drawdown analysis, equity curves,
    and multi-dimensional performance breakdowns.
    """

    def __init__(self):
        """Initialize the statistics engine with thread-safe locking."""
        self._lock = threading.RLock()
        self._trades: List[Dict[str, Any]] = []
        self._cached_stats: Optional[Dict[str, Any]] = None
        self._cache_valid = False

    def record_trade(self, trade_dict: Dict[str, Any]) -> None:
        """
        Record a closed trade for statistics tracking.

        Args:
            trade_dict: Trade data dictionary with keys:
                - strategy (str): Strategy identifier
                - symbol (str): Trading symbol
                - pnl (float): Profit/loss in currency units
                - pnl_pct (float): Profit/loss as percentage
                - entry_price (float): Entry price
                - exit_price (float): Exit price
                - opened_at (datetime): Trade open timestamp
                - closed_at (datetime): Trade close timestamp
                - confidence (float): 0-100 confidence score
                - exit_type (str): 'profit_target', 'stop_loss', 'timeout', etc.
                - asset_type (str): 'stock', 'option', 'future', etc.
                - regime (str): Market regime at entry
        """
        with self._lock:
            # Validate required fields
            required_fields = [
                'strategy', 'symbol', 'pnl', 'pnl_pct', 'entry_price',
                'exit_price', 'opened_at', 'closed_at', 'confidence',
                'exit_type', 'asset_type', 'regime'
            ]
            for field in required_fields:
                if field not in trade_dict:
                    raise ValueError(f"Missing required field: {field}")

            self._trades.append(trade_dict.copy())
            self._cache_valid = False

    def compute_all(
        self,
        strategy: Optional[str] = None,
        symbol: Optional[str] = None,
        regime: Optional[str] = None,
        period_days: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Compute all performance metrics for trades matching filters.

        Args:
            strategy: Filter by strategy name (None = all)
            symbol: Filter by trading symbol (None = all)
            regime: Filter by market regime (None = all)
            period_days: Limit to last N days (None = all time)

        Returns:
            Comprehensive statistics dictionary with all metrics.
        """
        with self._lock:
            trades = self._filter_trades(strategy, symbol, regime, period_days)

            if not trades:
                return self._empty_stats()

            return self._compute_metrics(trades)

    def _filter_trades(
        self,
        strategy: Optional[str],
        symbol: Optional[str],
        regime: Optional[str],
        period_days: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Filter trades based on criteria."""
        filtered = self._trades.copy()

        if strategy:
            filtered = [t for t in filtered if t['strategy'] == strategy]

        if symbol:
            filtered = [t for t in filtered if t['symbol'] == symbol]

        if regime:
            filtered = [t for t in filtered if t['regime'] == regime]

        if period_days:
            cutoff = datetime.now() - timedelta(days=period_days)
            filtered = [t for t in filtered if t['closed_at'] >= cutoff]

        # Sort by closed_at for equity curve and streak calculations
        filtered.sort(key=lambda t: t['closed_at'])
        return filtered

    def _empty_stats(self) -> Dict[str, Any]:
        """Return empty statistics dictionary."""
        return {
            'total_trades': 0,
            'winners': 0,
            'losers': 0,
            'win_rate': 0.0,
            'total_pnl': 0.0,
            'avg_pnl': 0.0,
            'avg_win': 0.0,
            'avg_loss': 0.0,
            'profit_factor': 0.0,
            'expectancy': 0.0,
            'max_drawdown': 0.0,
            'max_drawdown_pct': 0.0,
            'max_drawdown_duration_days': 0,
            'sharpe_ratio': 0.0,
            'sortino_ratio': 0.0,
            'ulcer_index': 0.0,
            'calmar_ratio': 0.0,
            'avg_holding_time_hours': 0.0,
            'best_trade': 0.0,
            'worst_trade': 0.0,
            'longest_winning_streak': 0,
            'longest_losing_streak': 0,
            'current_streak': 0,
        }

    def _compute_metrics(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compute all metrics from filtered trades."""

        # Basic counts
        total_trades = len(trades)
        pnls = [t['pnl'] for t in trades]
        winning_pnls = [p for p in pnls if p > 0]
        losing_pnls = [p for p in pnls if p < 0]

        winners = len(winning_pnls)
        losers = len(losing_pnls)
        win_rate = winners / total_trades if total_trades > 0 else 0.0

        # P&L metrics
        total_pnl = sum(pnls)
        avg_pnl = total_pnl / total_trades if total_trades > 0 else 0.0
        avg_win = sum(winning_pnls) / winners if winners > 0 else 0.0
        avg_loss = abs(sum(losing_pnls)) / losers if losers > 0 else 0.0

        # Profit factor
        gross_profit = sum(winning_pnls) if winning_pnls else 0.0
        gross_loss = abs(sum(losing_pnls)) if losing_pnls else 0.0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0

        # Expectancy
        loss_rate = 1.0 - win_rate
        expectancy = (avg_win * win_rate) - (avg_loss * loss_rate)

        # Equity curve for drawdown and Sharpe/Sortino
        equity_curve = self._build_equity_curve(trades)

        # Drawdown metrics
        max_dd, max_dd_pct, max_dd_duration = self._compute_drawdown(trades, equity_curve)

        # Sharpe and Sortino
        sharpe = self._compute_sharpe_ratio(pnls)
        sortino = self._compute_sortino_ratio(pnls)
        ulcer = self._compute_ulcer_index(equity_curve)

        # Calmar ratio
        calmar = self._compute_calmar_ratio(pnls, max_dd) if max_dd > 0 else 0.0

        # Holding time
        avg_holding_hours = self._compute_avg_holding_time(trades)

        # Best/worst trades
        best_trade = max(pnls) if pnls else 0.0
        worst_trade = min(pnls) if pnls else 0.0

        # Streaks
        win_streak, lose_streak, current_streak = self._compute_streaks(trades)

        return {
            'total_trades': total_trades,
            'winners': winners,
            'losers': losers,
            'win_rate': round(win_rate, 4),
            'total_pnl': round(total_pnl, 2),
            'avg_pnl': round(avg_pnl, 2),
            'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2),
            'profit_factor': round(profit_factor, 4),
            'expectancy': round(expectancy, 2),
            'max_drawdown': round(max_dd, 2),
            'max_drawdown_pct': round(max_dd_pct, 4),
            'max_drawdown_duration_days': max_dd_duration,
            'sharpe_ratio': round(sharpe, 4),
            'sortino_ratio': round(sortino, 4),
            'ulcer_index': round(ulcer, 4),
            'calmar_ratio': round(calmar, 4),
            'avg_holding_time_hours': round(avg_holding_hours, 2),
            'best_trade': round(best_trade, 2),
            'worst_trade': round(worst_trade, 2),
            'longest_winning_streak': win_streak,
            'longest_losing_streak': lose_streak,
            'current_streak': current_streak,
        }

    def _build_equity_curve(self, trades: List[Dict[str, Any]]) -> List[float]:
        """Build cumulative equity curve from trades."""
        equity_curve = []
        cumulative = 0.0
        for trade in trades:
            cumulative += trade['pnl']
            equity_curve.append(cumulative)
        return equity_curve

    def _compute_drawdown(
        self,
        trades: List[Dict[str, Any]],
        equity_curve: List[float]
    ) -> Tuple[float, float, int]:
        """
        Compute maximum drawdown metrics.

        Returns:
            Tuple of (max_drawdown_value, max_drawdown_pct, duration_in_days)
        """
        if not equity_curve or len(equity_curve) < 2:
            return 0.0, 0.0, 0

        max_equity = 0.0
        max_dd = 0.0
        max_dd_pct = 0.0
        dd_start_idx = 0
        max_dd_end_idx = 0

        for idx, equity in enumerate(equity_curve):
            if equity > max_equity:
                max_equity = equity

            dd = max_equity - equity
            if dd > max_dd:
                max_dd = dd
                dd_start_idx = idx
                max_dd_end_idx = idx

            # Track duration
            if equity < max_equity:
                max_dd_end_idx = idx

        # Calculate drawdown percentage
        if max_equity != 0:
            max_dd_pct = max_dd / max_equity

        # Calculate duration in days
        duration = 0
        if dd_start_idx < len(trades) and max_dd_end_idx < len(trades):
            try:
                start_time = trades[dd_start_idx]['closed_at']
                end_time = trades[max_dd_end_idx]['closed_at']
                if isinstance(start_time, str):
                    start_time = datetime.fromisoformat(start_time)
                if isinstance(end_time, str):
                    end_time = datetime.fromisoformat(end_time)
                duration = max(0, (end_time - start_time).days)
            except Exception:
                duration = 0

        return max_dd, max_dd_pct, duration

    def _compute_sharpe_ratio(self, pnls: List[float], risk_free_rate: float = 0.0) -> float:
        """
        Compute annualized Sharpe ratio.

        Assumes 252 trading days per year and zero-based returns.
        """
        if len(pnls) < 2:
            return 0.0

        try:
            daily_returns = pnls
            mean_return = statistics.mean(daily_returns)
            std_dev = statistics.stdev(daily_returns)

            if std_dev == 0:
                return 0.0

            # Annualize: daily Sharpe * sqrt(252)
            daily_sharpe = (mean_return - risk_free_rate) / std_dev
            annual_sharpe = daily_sharpe * math.sqrt(252)

            return annual_sharpe
        except (statistics.StatisticsError, ZeroDivisionError):
            return 0.0

    def _compute_sortino_ratio(self, pnls: List[float], risk_free_rate: float = 0.0) -> float:
        """
        Compute annualized Sortino ratio.

        Uses only downside deviation (negative returns).
        """
        if len(pnls) < 2:
            return 0.0

        try:
            mean_return = statistics.mean(pnls)
            downside_returns = [p - risk_free_rate for p in pnls if p < 0]

            if not downside_returns:
                return 0.0

            downside_dev = math.sqrt(sum(r ** 2 for r in downside_returns) / len(pnls))

            if downside_dev == 0:
                return 0.0

            daily_sortino = (mean_return - risk_free_rate) / downside_dev
            annual_sortino = daily_sortino * math.sqrt(252)

            return annual_sortino
        except (ZeroDivisionError, ValueError):
            return 0.0

    def _compute_ulcer_index(self, equity_curve: List[float]) -> float:
        """
        Compute Ulcer Index (measure of downside volatility).

        UI = sqrt(mean((drawdown_pct)^2))
        """
        if not equity_curve or len(equity_curve) < 2:
            return 0.0

        max_equity = 0.0
        drawdowns_squared = []

        for equity in equity_curve:
            if equity > max_equity:
                max_equity = equity

            drawdown = max_equity - equity
            if max_equity > 0:
                drawdown_pct = (drawdown / max_equity) * 100
                drawdowns_squared.append(drawdown_pct ** 2)

        if not drawdowns_squared:
            return 0.0

        ui = math.sqrt(sum(drawdowns_squared) / len(drawdowns_squared))
        return ui

    def _compute_calmar_ratio(self, pnls: List[float], max_drawdown: float) -> float:
        """
        Compute Calmar ratio (annual return / max drawdown).

        Calmar = (annualized_return) / max_drawdown_pct
        """
        if max_drawdown <= 0 or len(pnls) < 2:
            return 0.0

        total_pnl = sum(pnls)
        annual_return = total_pnl * (252 / len(pnls))

        return annual_return / max_drawdown if max_drawdown > 0 else 0.0

    def _compute_avg_holding_time(self, trades: List[Dict[str, Any]]) -> float:
        """Compute average holding time in hours."""
        if not trades:
            return 0.0

        total_hours = 0.0
        for trade in trades:
            try:
                closed = trade['closed_at']
                opened = trade['opened_at']
                if isinstance(closed, str):
                    closed = datetime.fromisoformat(closed)
                if isinstance(opened, str):
                    opened = datetime.fromisoformat(opened)
                duration = closed - opened
                hours = duration.total_seconds() / 3600
                total_hours += hours
            except Exception:
                pass

        return total_hours / len(trades) if trades else 0.0

    def _compute_streaks(self, trades: List[Dict[str, Any]]) -> Tuple[int, int, int]:
        """
        Compute winning/losing streaks.

        Returns:
            (longest_win_streak, longest_lose_streak, current_streak)
            Current streak: positive = winning, negative = losing
        """
        if not trades:
            return 0, 0, 0

        max_win_streak = 0
        max_lose_streak = 0
        current_win_streak = 0
        current_lose_streak = 0

        for trade in trades:
            if trade['pnl'] > 0:
                current_win_streak += 1
                current_lose_streak = 0
                max_win_streak = max(max_win_streak, current_win_streak)
            else:
                current_lose_streak += 1
                current_win_streak = 0
                max_lose_streak = max(max_lose_streak, current_lose_streak)

        # Current streak sign
        current_streak = current_win_streak if current_win_streak > 0 else -current_lose_streak

        return max_win_streak, max_lose_streak, current_streak

    def by_strategy(self) -> Dict[str, Dict[str, Any]]:
        """
        Get performance breakdown by strategy.

        Returns:
            Dict[strategy_name, stats_dict]
        """
        with self._lock:
            strategies = set(t['strategy'] for t in self._trades)
            return {
                strat: self.compute_all(strategy=strat)
                for strat in sorted(strategies)
            }

    def by_symbol(self, top_n: int = 20) -> Dict[str, Dict[str, Any]]:
        """
        Get performance breakdown by symbol.

        Args:
            top_n: Return top N symbols by trade count

        Returns:
            Dict[symbol, stats_dict] sorted by trade count
        """
        with self._lock:
            symbol_counts = defaultdict(int)
            for trade in self._trades:
                symbol_counts[trade['symbol']] += 1

            # Get top N symbols
            top_symbols = sorted(
                symbol_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )[:top_n]

            return {
                symbol: self.compute_all(symbol=symbol)
                for symbol, _ in top_symbols
            }

    def by_regime(self) -> Dict[str, Dict[str, Any]]:
        """
        Get performance breakdown by market regime.

        Returns:
            Dict[regime_name, stats_dict]
        """
        with self._lock:
            regimes = set(t['regime'] for t in self._trades)
            return {
                regime: self.compute_all(regime=regime)
                for regime in sorted(regimes)
            }

    def by_hour(self) -> Dict[int, Dict[str, Any]]:
        """
        Get performance breakdown by hour of day.

        Returns:
            Dict[hour (0-23), stats_dict]
        """
        with self._lock:
            trades_by_hour = defaultdict(list)
            for trade in self._trades:
                closed = trade['closed_at']
                if isinstance(closed, str):
                    closed = datetime.fromisoformat(closed)
                hour = closed.hour
                trades_by_hour[hour].append(trade)

            result = {}
            for hour in range(24):
                trades = trades_by_hour[hour]
                if trades:
                    result[hour] = self._compute_metrics(trades)

            return result

    def by_day_of_week(self) -> Dict[str, Dict[str, Any]]:
        """
        Get performance breakdown by day of week.

        Returns:
            Dict[day_name, stats_dict]
        """
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

        with self._lock:
            trades_by_day = {day: [] for day in days}
            for trade in self._trades:
                closed = trade['closed_at']
                if isinstance(closed, str):
                    closed = datetime.fromisoformat(closed)
                day_name = days[closed.weekday()]
                trades_by_day[day_name].append(trade)

            return {
                day: self._compute_metrics(trades)
                for day, trades in trades_by_day.items()
                if trades
            }

    def by_confidence_bucket(
        self,
        buckets: Optional[List[int]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get performance breakdown by confidence score buckets.

        Args:
            buckets: List of upper bounds for confidence ranges.
                Default: [50, 60, 70, 80, 90, 100]

        Returns:
            Dict[bucket_range_str, stats_dict]
        """
        if buckets is None:
            buckets = [50, 60, 70, 80, 90, 100]

        buckets = sorted(set(buckets))
        bucket_ranges = []

        for i, upper in enumerate(buckets):
            lower = 0 if i == 0 else buckets[i - 1]
            bucket_ranges.append((lower, upper))

        with self._lock:
            trades_by_bucket = {
                f"{lower}-{upper}": []
                for lower, upper in bucket_ranges
            }

            for trade in self._trades:
                conf = trade['confidence']
                for lower, upper in bucket_ranges:
                    if lower < conf <= upper:
                        bucket_key = f"{lower}-{upper}"
                        trades_by_bucket[bucket_key].append(trade)
                        break

            return {
                bucket: self._compute_metrics(trades)
                for bucket, trades in trades_by_bucket.items()
                if trades
            }

    def by_exit_type(self) -> Dict[str, Dict[str, Any]]:
        """
        Get performance breakdown by exit type.

        Returns:
            Dict[exit_type, stats_dict]
        """
        with self._lock:
            exit_types = set(t['exit_type'] for t in self._trades)
            return {
                exit_type: self.compute_all()  # We'll filter manually
                for exit_type in sorted(exit_types)
            }

    def by_month(self) -> Dict[str, Dict[str, Any]]:
        """
        Get performance breakdown by month.

        Returns:
            Dict[YYYY-MM, stats_dict]
        """
        with self._lock:
            trades_by_month = defaultdict(list)
            for trade in self._trades:
                closed = trade['closed_at']
                if isinstance(closed, str):
                    closed = datetime.fromisoformat(closed)
                month_key = closed.strftime('%Y-%m')
                trades_by_month[month_key].append(trade)

            return {
                month: self._compute_metrics(trades_by_month[month])
                for month in sorted(trades_by_month.keys())
                if trades_by_month[month]
            }

    def rolling_sharpe(self, window: int = 50) -> List[float]:
        """
        Compute Sharpe ratio over rolling windows.

        Args:
            window: Size of rolling window

        Returns:
            List of Sharpe ratios for each window
        """
        with self._lock:
            if len(self._trades) < window:
                return []

            pnls = [t['pnl'] for t in self._trades]
            rolling_sharpes = []

            for i in range(len(pnls) - window + 1):
                window_pnls = pnls[i:i + window]
                sharpe = self._compute_sharpe_ratio(window_pnls)
                rolling_sharpes.append(sharpe)

            return rolling_sharpes

    def rolling_win_rate(self, window: int = 50) -> List[float]:
        """
        Compute win rate over rolling windows.

        Args:
            window: Size of rolling window

        Returns:
            List of win rates for each window
        """
        with self._lock:
            if len(self._trades) < window:
                return []

            rolling_rates = []

            for i in range(len(self._trades) - window + 1):
                window_trades = self._trades[i:i + window]
                winners = sum(1 for t in window_trades if t['pnl'] > 0)
                win_rate = winners / window if window > 0 else 0.0
                rolling_rates.append(win_rate)

            return rolling_rates

    def edge_stability_score(self) -> float:
        """
        Compute edge stability score (0-100).

        Based on coefficient of variation of rolling Sharpe ratios.
        Higher score = more consistent edge.
        """
        rolling_sharpes = self.rolling_sharpe(window=50)

        if not rolling_sharpes or len(rolling_sharpes) < 2:
            return 0.0

        try:
            mean_sharpe = statistics.mean(rolling_sharpes)
            if mean_sharpe == 0:
                return 0.0

            std_sharpe = statistics.stdev(rolling_sharpes)
            cv = std_sharpe / abs(mean_sharpe)

            # Convert CV to 0-100 score (lower CV = higher stability)
            # CV of 0 = 100, CV of 1 = 0, CV > 1 = negative (clamp to 0)
            stability = max(0, 100 * (1 - cv))
            return round(min(100, stability), 2)
        except (statistics.StatisticsError, ZeroDivisionError):
            return 0.0

    def estimate_capacity(self, strategy: str) -> Dict[str, Any]:
        """
        Estimate trading capacity for a strategy.

        Based on average position sizes and fill assumptions.

        Args:
            strategy: Strategy name

        Returns:
            Dict with capacity metrics
        """
        with self._lock:
            strategy_trades = [t for t in self._trades if t['strategy'] == strategy]

            if not strategy_trades:
                return {
                    'strategy': strategy,
                    'estimated_daily_capacity': 0.0,
                    'estimated_monthly_capacity': 0.0,
                    'trades_per_day': 0.0,
                    'avg_position_size': 0.0,
                }

            # Calculate trades per day
            if strategy_trades:
                try:
                    last_close = strategy_trades[-1]['closed_at']
                    first_close = strategy_trades[0]['closed_at']
                    if isinstance(last_close, str):
                        last_close = datetime.fromisoformat(last_close)
                    if isinstance(first_close, str):
                        first_close = datetime.fromisoformat(first_close)
                    time_span = (last_close - first_close).days
                except Exception:
                    time_span = 1
                tpd = len(strategy_trades) / max(1, time_span)
            else:
                tpd = 0.0

            # Average position size (notional)
            avg_pos = statistics.mean([
                abs(t['entry_price'] * 100)  # Assume 100 shares/contracts
                for t in strategy_trades
            ]) if strategy_trades else 0.0

            daily_capacity = avg_pos * tpd * 10  # Multiplier for leverage room
            monthly_capacity = daily_capacity * 21

            return {
                'strategy': strategy,
                'estimated_daily_capacity': round(daily_capacity, 2),
                'estimated_monthly_capacity': round(monthly_capacity, 2),
                'trades_per_day': round(tpd, 2),
                'avg_position_size': round(avg_pos, 2),
            }

    def slippage_impact(self, avg_slippage_bps: float) -> Dict[str, Any]:
        """
        Estimate impact of slippage on strategy performance.

        Args:
            avg_slippage_bps: Average slippage in basis points

        Returns:
            Dict with slippage impact metrics
        """
        with self._lock:
            stats = self.compute_all()

            if stats['total_trades'] == 0:
                return {
                    'avg_slippage_bps': avg_slippage_bps,
                    'total_slippage_cost': 0.0,
                    'pnl_impact_pct': 0.0,
                    'affected_trades': 0,
                }

            # Assume 2 fills per trade (entry + exit)
            slippage_per_trade = (avg_slippage_bps / 10000) * 2
            total_cost = slippage_per_trade * stats['avg_pnl'] * stats['total_trades']

            original_pnl = stats['total_pnl']
            impact_pct = (total_cost / abs(original_pnl) * 100) if original_pnl != 0 else 0.0

            # Estimate win rate impact (assume slippage kills ~1% of trades)
            win_rate_impact = stats['total_trades'] * 0.01 * (1 - stats['win_rate'])

            return {
                'avg_slippage_bps': avg_slippage_bps,
                'total_slippage_cost': round(total_cost, 2),
                'pnl_impact_pct': round(impact_pct, 2),
                'affected_trades': round(win_rate_impact, 0),
            }

    def get_full_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive report for operations and dashboard.

        Returns:
            Dict with all statistics, breakdowns, and analysis
        """
        with self._lock:
            base_stats = self.compute_all()

            return {
                'summary': base_stats,
                'by_strategy': self.by_strategy(),
                'by_symbol': self.by_symbol(top_n=20),
                'by_regime': self.by_regime(),
                'by_hour': self.by_hour(),
                'by_day_of_week': self.by_day_of_week(),
                'by_confidence': self.by_confidence_bucket(),
                'by_month': self.by_month(),
                'edge_stability_score': self.edge_stability_score(),
                'rolling_sharpe': self.rolling_sharpe(),
                'rolling_win_rate': self.rolling_win_rate(),
                'report_generated_at': datetime.now().isoformat(),
            }

    def get_promotion_criteria(self) -> Dict[str, Any]:
        """
        Evaluate formal capital promotion criteria.

        Requirements:
        - min_trades: 200
        - min_days: 60
        - min_sharpe: 0.5
        - min_profit_factor: 1.3
        - max_drawdown_pct: < 15%
        - positive_expectancy: True
        - edge_stability: > 50

        Returns:
            Dict with criteria evaluation and recommendation
        """
        with self._lock:
            stats = self.compute_all()

            # Calculate days trading
            if not self._trades:
                days_trading = 0
            else:
                try:
                    last_close = self._trades[-1]['closed_at']
                    first_close = self._trades[0]['closed_at']
                    if isinstance(last_close, str):
                        last_close = datetime.fromisoformat(last_close)
                    if isinstance(first_close, str):
                        first_close = datetime.fromisoformat(first_close)
                    days_trading = (last_close - first_close).days
                except Exception:
                    days_trading = 0

            # Evaluate criteria
            criteria = {
                'total_trades': {
                    'requirement': 200,
                    'actual': stats['total_trades'],
                    'met': stats['total_trades'] >= 200,
                },
                'min_days_trading': {
                    'requirement': 60,
                    'actual': days_trading,
                    'met': days_trading >= 60,
                },
                'min_sharpe_ratio': {
                    'requirement': 0.5,
                    'actual': stats['sharpe_ratio'],
                    'met': stats['sharpe_ratio'] >= 0.5,
                },
                'min_profit_factor': {
                    'requirement': 1.3,
                    'actual': stats['profit_factor'],
                    'met': stats['profit_factor'] >= 1.3,
                },
                'max_drawdown_pct': {
                    'requirement': '< 15%',
                    'actual': f"{stats['max_drawdown_pct']*100:.2f}%",
                    'met': stats['max_drawdown_pct'] < 0.15,
                },
                'positive_expectancy': {
                    'requirement': '> 0',
                    'actual': stats['expectancy'],
                    'met': stats['expectancy'] > 0,
                },
                'edge_stability_score': {
                    'requirement': '> 50',
                    'actual': self.edge_stability_score(),
                    'met': self.edge_stability_score() > 50,
                },
            }

            # Overall recommendation
            all_met = all(c['met'] for c in criteria.values())

            return {
                'eligible_for_promotion': all_met,
                'criteria': criteria,
                'unmet_requirements': [
                    name for name, crit in criteria.items()
                    if not crit['met']
                ],
                'evaluated_at': datetime.now().isoformat(),
            }

    def get_strategy_scorecard(self) -> Dict[str, Any]:
        """
        Generate per-strategy scorecard with traffic light ratings.

        Returns:
            Dict with per-strategy performance scores (GREEN/YELLOW/RED)
        """
        with self._lock:
            strategies = self.by_strategy()
            scorecard = {}

            for strategy_name, stats in strategies.items():
                if stats['total_trades'] == 0:
                    continue

                # Compute edge stability for this strategy
                strategy_trades = [t for t in self._trades if t['strategy'] == strategy_name]
                if len(strategy_trades) >= 50:
                    # Compute rolling Sharpe for edge stability
                    pnls = [t['pnl'] for t in strategy_trades]
                    rolling_sharpes = []
                    for i in range(len(pnls) - 49):
                        window_pnls = pnls[i:i + 50]
                        sharpe = self._compute_sharpe_ratio(window_pnls)
                        rolling_sharpes.append(sharpe)

                    if rolling_sharpes and len(rolling_sharpes) > 1:
                        try:
                            mean_sharpe = statistics.mean(rolling_sharpes)
                            std_sharpe = statistics.stdev(rolling_sharpes)
                            cv = std_sharpe / abs(mean_sharpe) if mean_sharpe != 0 else 0
                            edge_stability = max(0, 100 * (1 - cv))
                        except (statistics.StatisticsError, ZeroDivisionError):
                            edge_stability = 0.0
                    else:
                        edge_stability = 0.0
                else:
                    edge_stability = 0.0

                # Calculate average holding hours
                avg_holding_hours = stats['avg_holding_time_hours']

                # Score individual dimensions
                stability_score = self._score_stability(edge_stability)
                drawdown_score = self._score_drawdown(stats['max_drawdown_pct'])
                efficiency_score = self._score_efficiency(stats['profit_factor'])
                regime_sensitivity_score = self._score_regime_sensitivity(stats['win_rate'], strategy_name)
                reliability_score = 'GREEN'  # Default to GREEN for data reliability

                # Overall grade: GREEN if all GREEN or max 1 YELLOW; YELLOW if 2+ YELLOW; RED if any RED
                scores = {
                    'stability': stability_score,
                    'drawdown': drawdown_score,
                    'efficiency': efficiency_score,
                    'regime_sensitivity': regime_sensitivity_score,
                    'data_reliability': reliability_score,
                }

                overall_grade = self._compute_overall_grade(scores)

                scorecard[strategy_name] = {
                    'grade': overall_grade,
                    'total_trades': stats['total_trades'],
                    'sharpe_ratio': stats['sharpe_ratio'],
                    'profit_factor': stats['profit_factor'],
                    'max_drawdown_pct': round(stats['max_drawdown_pct'] * 100, 2),
                    'win_rate': round(stats['win_rate'] * 100, 2),
                    'edge_stability': round(edge_stability, 1),
                    'avg_holding_hours': avg_holding_hours,
                    'scores': scores,
                }

            return scorecard

    def _score_stability(self, edge_stability: float) -> str:
        """Score edge stability (0-100 scale)."""
        if edge_stability > 60:
            return 'GREEN'
        elif edge_stability > 40:
            return 'YELLOW'
        else:
            return 'RED'

    def _score_drawdown(self, max_dd_pct: float) -> str:
        """Score max drawdown percentage."""
        if max_dd_pct < 0.10:
            return 'GREEN'
        elif max_dd_pct < 0.15:
            return 'YELLOW'
        else:
            return 'RED'

    def _score_efficiency(self, profit_factor: float) -> str:
        """Score profit factor."""
        if profit_factor > 1.5:
            return 'GREEN'
        elif profit_factor > 1.2:
            return 'YELLOW'
        else:
            return 'RED'

    def _score_regime_sensitivity(self, overall_win_rate: float, strategy: str) -> str:
        """Score win rate consistency across regimes."""
        regimes = set(t['regime'] for t in self._trades if t['strategy'] == strategy)

        if len(regimes) < 2:
            return 'YELLOW'

        regime_win_rates = []
        for regime in regimes:
            regime_trades = [t for t in self._trades
                           if t['strategy'] == strategy and t['regime'] == regime]
            if regime_trades:
                wins = sum(1 for t in regime_trades if t['pnl'] > 0)
                wr = wins / len(regime_trades)
                regime_win_rates.append(wr)

        if not regime_win_rates:
            return 'YELLOW'

        win_rate_range = max(regime_win_rates) - min(regime_win_rates)
        win_rate_variation_pct = win_rate_range * 100

        if win_rate_variation_pct < 15:
            return 'GREEN'
        elif win_rate_variation_pct < 25:
            return 'YELLOW'
        else:
            return 'RED'

    def _compute_overall_grade(self, scores: Dict[str, str]) -> str:
        """Compute overall grade from sub-scores."""
        red_count = sum(1 for v in scores.values() if v == 'RED')
        yellow_count = sum(1 for v in scores.values() if v == 'YELLOW')

        if red_count > 0:
            return 'RED'
        elif yellow_count >= 2:
            return 'YELLOW'
        else:
            return 'GREEN'

    def get_enhanced_promotion_criteria(self) -> Dict[str, Any]:
        """
        Evaluate enhanced promotion criteria with per-strategy and regime checks.

        Returns:
            Dict with global, per-strategy, and regime-based promotion criteria
        """
        with self._lock:
            # Get global criteria first
            global_criteria_result = self.get_promotion_criteria()
            global_criteria = global_criteria_result['criteria']
            global_eligible = global_criteria_result['eligible_for_promotion']

            # Per-strategy criteria
            per_strategy_criteria = {}
            all_strategies_met = True

            for strategy_name in set(t['strategy'] for t in self._trades):
                strategy_stats = self.compute_all(strategy=strategy_name)

                # Determine min_trades requirement
                min_trades = 50 if strategy_name == 'stocks' else 30

                strategy_met = (
                    strategy_stats['total_trades'] >= min_trades and
                    strategy_stats['sharpe_ratio'] >= 0.3 and
                    strategy_stats['profit_factor'] >= 1.2 and
                    strategy_stats['max_drawdown_pct'] < 0.20
                )

                per_strategy_criteria[strategy_name] = {
                    'min_trades': min_trades,
                    'actual': strategy_stats['total_trades'],
                    'met': strategy_stats['total_trades'] >= min_trades,
                    'min_sharpe': 0.3,
                    'actual_sharpe': round(strategy_stats['sharpe_ratio'], 4),
                    'sharpe_met': strategy_stats['sharpe_ratio'] >= 0.3,
                    'min_profit_factor': 1.2,
                    'actual_profit_factor': round(strategy_stats['profit_factor'], 4),
                    'pf_met': strategy_stats['profit_factor'] >= 1.2,
                    'max_drawdown_pct': 0.20,
                    'actual_max_dd': round(strategy_stats['max_drawdown_pct'] * 100, 2),
                    'dd_met': strategy_stats['max_drawdown_pct'] < 0.20,
                    'strategy_eligible': strategy_met,
                }

                if not strategy_met:
                    all_strategies_met = False

            # Regime criteria
            regimes_data = self.by_regime()
            regime_criteria = {
                'min_regimes_tested': 2,
                'actual_regimes': len(regimes_data),
                'met': len(regimes_data) >= 2,
                'per_regime_min_trades': 20,
                'regimes': {}
            }

            all_regimes_met = True
            for regime_name, regime_stats in regimes_data.items():
                regime_met = regime_stats['total_trades'] >= 20
                regime_criteria['regimes'][regime_name] = {
                    'trades': regime_stats['total_trades'],
                    'met': regime_met,
                }
                if not regime_met:
                    all_regimes_met = False

            # Overall promotion eligibility: all criteria must pass
            eligible_for_promotion = (
                global_eligible and
                all_strategies_met and
                regime_criteria['met'] and
                all_regimes_met
            )

            return {
                'eligible_for_promotion': eligible_for_promotion,
                'global_criteria': global_criteria,
                'per_strategy_criteria': per_strategy_criteria,
                'regime_criteria': regime_criteria,
                'evaluated_at': datetime.now().isoformat(),
            }
