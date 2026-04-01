"""
InstitutionalRiskManager Module
Egreja Investment AI v10.22

Autonomous trading system risk management module.
Tracks intraday P&L, weekly/monthly losses, per-asset limits, concentration limits,
and losing streaks with automatic risk reduction and market regime awareness.

Self-contained module with NO imports from api_server.py to avoid circular dependencies.
Thread-safe implementation using threading.Lock.

Author: Egreja Investment AI
Version: 10.22
"""

import os
import logging
import threading
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Tuple, Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class InstitutionalRiskManager:
    """
    Institutional Risk Manager for autonomous trading system.
    
    Manages:
    - Intraday P&L per strategy and global (with configurable limits)
    - Weekly and monthly loss limits
    - Per-asset loss tracking and limits
    - Per-strategy concentration limits
    - Losing streak tracking with auto risk reduction
    - Market regime awareness (multiplier adjustment)
    
    Thread-safe implementation using internal locks.
    """
    
    def __init__(self):
        """Initialize the risk manager with configuration from environment variables."""
        # Configuration from environment with sensible defaults
        self.max_daily_loss = float(os.getenv('RISK_MAX_DAILY_LOSS', '200000'))
        self.max_weekly_loss = float(os.getenv('RISK_MAX_WEEKLY_LOSS', '500000'))
        self.max_monthly_loss = float(os.getenv('RISK_MAX_MONTHLY_LOSS', '1000000'))
        self.max_loss_per_asset = float(os.getenv('RISK_MAX_LOSS_PER_ASSET', '50000'))
        self.max_concentration_pct = float(os.getenv('RISK_MAX_CONCENTRATION_PCT', '15.0'))
        self.losing_streak_threshold = int(os.getenv('RISK_LOSING_STREAK_THRESHOLD', '5'))
        self.reduction_factor = float(os.getenv('RISK_REDUCTION_FACTOR', '0.5'))
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Daily P&L tracking (per strategy and global)
        self.daily_pnl_by_strategy = defaultdict(float)
        self.global_daily_pnl = 0.0
        self.daily_reset_date = datetime.now().date()
        
        # Weekly P&L tracking
        self.weekly_pnl = 0.0
        self.weekly_reset_date = self._get_week_start(datetime.now())
        
        # Monthly P&L tracking
        self.monthly_pnl = 0.0
        self.monthly_reset_date = datetime.now().replace(day=1).date()
        
        # Per-asset loss tracking
        self.asset_losses = defaultdict(float)  # symbol -> cumulative loss
        
        # Per-strategy concentration tracking (position value)
        self.strategy_positions = defaultdict(lambda: defaultdict(float))  # strategy -> symbol -> position_value
        
        # Losing streak tracking (per strategy)
        self.losing_streaks = defaultdict(int)  # strategy -> count
        self.last_trade_winning = defaultdict(bool)  # strategy -> bool
        
        # Market regime multiplier (1.0 = normal, 0.5 = conservative, etc.)
        self.market_regime_multiplier = 1.0
        
        # Breach log (last 100 breaches)
        self.breach_log = []
        
        logger.info(f"InstitutionalRiskManager initialized with limits: "
                   f"daily={self.max_daily_loss}, weekly={self.max_weekly_loss}, "
                   f"monthly={self.max_monthly_loss}, per_asset={self.max_loss_per_asset}, "
                   f"concentration={self.max_concentration_pct}%, "
                   f"streak_threshold={self.losing_streak_threshold}, "
                   f"reduction_factor={self.reduction_factor}")
    
    def _get_week_start(self, dt: datetime) -> datetime:
        """Get the start of the week (Monday) for a given datetime."""
        days_since_monday = dt.weekday()
        week_start = dt - timedelta(days=days_since_monday)
        return week_start.replace(hour=0, minute=0, second=0, microsecond=0)
    
    def _check_and_reset_daily(self) -> None:
        """Check if date has changed and reset daily counters if needed."""
        current_date = datetime.now().date()
        if current_date != self.daily_reset_date:
            logger.info(f"Daily reset triggered: {self.daily_reset_date} -> {current_date}")
            self.reset_daily()
    
    def _check_and_reset_weekly(self) -> None:
        """Check if week has changed and reset weekly counters if needed."""
        current_week_start = self._get_week_start(datetime.now())
        if current_week_start > self.weekly_reset_date:
            logger.info(f"Weekly reset triggered: {self.weekly_reset_date} -> {current_week_start}")
            self.reset_weekly()
    
    def _check_and_reset_monthly(self) -> None:
        """Check if month has changed and reset monthly counters if needed."""
        current_month_start = datetime.now().replace(day=1).date()
        if current_month_start > self.monthly_reset_date:
            logger.info(f"Monthly reset triggered: {self.monthly_reset_date} -> {current_month_start}")
            self.reset_monthly()
    
    def record_trade_result(self, strategy: str, symbol: str, pnl: float, 
                           position_value: float, capital: float) -> None:
        """
        Record a closed trade and update risk tracking.
        
        Args:
            strategy: Strategy identifier
            symbol: Asset symbol
            pnl: Profit/loss from the trade
            position_value: Position value at close
            capital: Total account capital
        """
        with self._lock:
            self._check_and_reset_daily()
            self._check_and_reset_weekly()
            self._check_and_reset_monthly()
            
            # Update P&L trackers
            self.global_daily_pnl += pnl
            self.daily_pnl_by_strategy[strategy] += pnl
            self.weekly_pnl += pnl
            self.monthly_pnl += pnl
            
            # Update per-asset loss tracking (only for losses)
            if pnl < 0:
                self.asset_losses[symbol] += abs(pnl)
            
            # Update losing streak tracking
            is_winning = pnl > 0
            if is_winning:
                self.losing_streaks[strategy] = 0
                self.last_trade_winning[strategy] = True
            else:
                self.losing_streaks[strategy] += 1
                self.last_trade_winning[strategy] = False
                if self.losing_streaks[strategy] >= self.losing_streak_threshold:
                    logger.warning(f"Strategy {strategy} hit losing streak threshold: "
                                 f"{self.losing_streaks[strategy]} consecutive losses")
            
            logger.debug(f"Trade recorded: strategy={strategy}, symbol={symbol}, "
                        f"pnl={pnl:.2f}, streak={self.losing_streaks[strategy]}")
    
    def check_can_open(self, strategy: str, symbol: str, position_value: float, 
                      total_capital: float) -> Tuple[bool, str]:
        """
        Check if a new position can be opened based on risk limits.
        
        Args:
            strategy: Strategy identifier
            symbol: Asset symbol
            position_value: Proposed position value
            total_capital: Total account capital
        
        Returns:
            Tuple of (allowed: bool, reason: str)
        """
        with self._lock:
            self._check_and_reset_daily()
            self._check_and_reset_weekly()
            self._check_and_reset_monthly()
            
            breached_reasons = []
            
            # Check daily loss limit
            if self.global_daily_pnl < -self.max_daily_loss:
                breached_reasons.append(
                    f"Daily loss limit breached: ${abs(self.global_daily_pnl):.2f} "
                    f"/ ${self.max_daily_loss:.2f}"
                )
            
            # Check weekly loss limit
            if self.weekly_pnl < -self.max_weekly_loss:
                breached_reasons.append(
                    f"Weekly loss limit breached: ${abs(self.weekly_pnl):.2f} "
                    f"/ ${self.max_weekly_loss:.2f}"
                )
            
            # Check monthly loss limit
            if self.monthly_pnl < -self.max_monthly_loss:
                breached_reasons.append(
                    f"Monthly loss limit breached: ${abs(self.monthly_pnl):.2f} "
                    f"/ ${self.max_monthly_loss:.2f}"
                )
            
            # Check per-asset loss limit
            current_asset_loss = self.asset_losses.get(symbol, 0)
            if current_asset_loss >= self.max_loss_per_asset:
                breached_reasons.append(
                    f"Asset {symbol} loss limit breached: ${current_asset_loss:.2f} "
                    f"/ ${self.max_loss_per_asset:.2f}"
                )
            
            # Check concentration limit
            adjusted_position_value = position_value * self.market_regime_multiplier
            concentration_pct = (adjusted_position_value / total_capital) * 100
            if concentration_pct > self.max_concentration_pct:
                breached_reasons.append(
                    f"Concentration limit exceeded for {symbol}: {concentration_pct:.2f}% "
                    f"/ {self.max_concentration_pct}%"
                )
            
            if breached_reasons:
                reason_str = " | ".join(breached_reasons)
                self._log_breach(reason_str)
                return False, reason_str
            
            return True, "Risk check passed"
    
    def get_risk_multiplier(self) -> float:
        """
        Get the current risk multiplier based on losing streaks and market regime.
        
        Returns:
            Risk multiplier (1.0 = normal, <1.0 = reduced)
        """
        with self._lock:
            # If any strategy has hit losing streak threshold, apply reduction
            if any(streak >= self.losing_streak_threshold 
                   for streak in self.losing_streaks.values()):
                return self.reduction_factor * self.market_regime_multiplier
            
            return self.market_regime_multiplier
    
    def get_daily_pnl(self, strategy: Optional[str] = None) -> float:
        """
        Get daily P&L for a specific strategy or global.
        
        Args:
            strategy: Strategy identifier, or None for global P&L
        
        Returns:
            Daily P&L amount
        """
        with self._lock:
            self._check_and_reset_daily()
            if strategy is None:
                return self.global_daily_pnl
            return self.daily_pnl_by_strategy.get(strategy, 0.0)
    
    def get_weekly_pnl(self) -> float:
        """Get weekly P&L."""
        with self._lock:
            self._check_and_reset_weekly()
            return self.weekly_pnl
    
    def get_monthly_pnl(self) -> float:
        """Get monthly P&L."""
        with self._lock:
            self._check_and_reset_monthly()
            return self.monthly_pnl
    
    def reset_daily(self) -> None:
        """Reset daily P&L counters."""
        with self._lock:
            self.daily_pnl_by_strategy.clear()
            self.global_daily_pnl = 0.0
            self.daily_reset_date = datetime.now().date()
            logger.info(f"Daily counters reset on {self.daily_reset_date}")
    
    def reset_weekly(self) -> None:
        """Reset weekly P&L counters."""
        with self._lock:
            self.weekly_pnl = 0.0
            self.weekly_reset_date = self._get_week_start(datetime.now())
            logger.info(f"Weekly counters reset on {self.weekly_reset_date}")
    
    def reset_monthly(self) -> None:
        """Reset monthly P&L counters."""
        with self._lock:
            self.monthly_pnl = 0.0
            self.monthly_reset_date = datetime.now().replace(day=1).date()
            logger.info(f"Monthly counters reset on {self.monthly_reset_date}")
    
    def set_market_regime_multiplier(self, multiplier: float) -> None:
        """
        Set market regime awareness multiplier.
        
        Args:
            multiplier: Multiplier value (1.0 = normal, 0.5 = conservative, etc.)
        """
        with self._lock:
            old_multiplier = self.market_regime_multiplier
            self.market_regime_multiplier = max(0.1, min(2.0, multiplier))
            logger.info(f"Market regime multiplier changed: {old_multiplier} -> {self.market_regime_multiplier}")
    
    def _log_breach(self, reason: str) -> None:
        """Log a risk limit breach."""
        breach_entry = {
            'timestamp': datetime.now().isoformat(),
            'reason': reason
        }
        self.breach_log.append(breach_entry)
        
        # Keep only last 100 breaches
        if len(self.breach_log) > 100:
            self.breach_log = self.breach_log[-100:]
        
        logger.warning(f"Risk limit breach detected: {reason}")
    
    def is_breached(self) -> Tuple[bool, List[str]]:
        """
        Check if any risk limits are currently breached.
        
        Returns:
            Tuple of (is_breached: bool, reasons: list[str])
        """
        with self._lock:
            self._check_and_reset_daily()
            self._check_and_reset_weekly()
            self._check_and_reset_monthly()
            
            breached_reasons = []
            
            if self.global_daily_pnl < -self.max_daily_loss:
                breached_reasons.append(f"Daily loss: ${abs(self.global_daily_pnl):.2f} > ${self.max_daily_loss:.2f}")
            
            if self.weekly_pnl < -self.max_weekly_loss:
                breached_reasons.append(f"Weekly loss: ${abs(self.weekly_pnl):.2f} > ${self.max_weekly_loss:.2f}")
            
            if self.monthly_pnl < -self.max_monthly_loss:
                breached_reasons.append(f"Monthly loss: ${abs(self.monthly_pnl):.2f} > ${self.max_monthly_loss:.2f}")
            
            for symbol, loss in self.asset_losses.items():
                if loss > self.max_loss_per_asset:
                    breached_reasons.append(f"Asset {symbol} loss: ${loss:.2f} > ${self.max_loss_per_asset:.2f}")
            
            is_breached = len(breached_reasons) > 0
            return is_breached, breached_reasons
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get full risk status for /ops endpoint.
        
        Returns:
            Dictionary with complete risk manager status
        """
        with self._lock:
            self._check_and_reset_daily()
            self._check_and_reset_weekly()
            self._check_and_reset_monthly()
            
            is_breached, breach_reasons = self.is_breached()
            
            return {
                'timestamp': datetime.now().isoformat(),
                'is_breached': is_breached,
                'breach_reasons': breach_reasons,
                'pnl': {
                    'daily_global': self.global_daily_pnl,
                    'daily_by_strategy': dict(self.daily_pnl_by_strategy),
                    'weekly': self.weekly_pnl,
                    'monthly': self.monthly_pnl,
                },
                'limits': {
                    'max_daily_loss': self.max_daily_loss,
                    'max_weekly_loss': self.max_weekly_loss,
                    'max_monthly_loss': self.max_monthly_loss,
                    'max_loss_per_asset': self.max_loss_per_asset,
                    'max_concentration_pct': self.max_concentration_pct,
                },
                'asset_losses': dict(self.asset_losses),
                'losing_streaks': dict(self.losing_streaks),
                'risk_multiplier': self.get_risk_multiplier(),
                'market_regime_multiplier': self.market_regime_multiplier,
                'recent_breaches': self.breach_log[-10:] if self.breach_log else [],
                'dates': {
                    'daily_reset': self.daily_reset_date.isoformat(),
                    'weekly_reset': self.weekly_reset_date.isoformat(),
                    'monthly_reset': self.monthly_reset_date.isoformat(),
                }
            }
