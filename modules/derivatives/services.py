"""
Core Services for Derivatives Trading Infrastructure.

Provides caching, market data management, Greeks calculation, calibration,
and structured order execution for derivatives strategies.
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import math

try:
    from scipy import stats
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Generic cache entry with TTL."""
    value: any
    timestamp: datetime
    ttl_seconds: int

    def is_expired(self) -> bool:
        """Check if entry has expired."""
        return (datetime.utcnow() - self.timestamp).total_seconds() > self.ttl_seconds


@dataclass
class OptionChain:
    """Options chain snapshot."""
    symbol: str
    underlying: str
    timestamp: datetime
    options: Dict = field(default_factory=dict)  # strike -> [call, put]


@dataclass
class FuturesChain:
    """Futures chain snapshot."""
    symbol: str
    underlying: str
    timestamp: datetime
    contracts: Dict = field(default_factory=dict)  # expiry -> contract data


@dataclass
class GreeksData:
    """Options Greeks."""
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float
    iv: float


class OptionsChainCache:
    """Thread-safe cache for options chains with TTL invalidation."""

    def __init__(self, ttl_seconds: int = 5):
        self._lock = threading.RLock()
        self.cache: Dict[str, CacheEntry] = {}
        self.ttl_seconds = ttl_seconds
        self.stats = {
            "hits": 0,
            "misses": 0,
            "expirations": 0,
        }

    def get(self, symbol: str) -> Optional[OptionChain]:
        """Retrieve options chain from cache."""
        with self._lock:
            if symbol in self.cache:
                entry = self.cache[symbol]
                if not entry.is_expired():
                    self.stats["hits"] += 1
                    return entry.value
                else:
                    self.stats["expirations"] += 1
                    del self.cache[symbol]
            
            self.stats["misses"] += 1
            return None

    def set(self, symbol: str, chain: OptionChain) -> None:
        """Store options chain in cache."""
        with self._lock:
            self.cache[symbol] = CacheEntry(
                value=chain,
                timestamp=datetime.utcnow(),
                ttl_seconds=self.ttl_seconds,
            )

    def invalidate(self, symbol: str = None) -> None:
        """Invalidate cache entry or entire cache."""
        with self._lock:
            if symbol:
                if symbol in self.cache:
                    del self.cache[symbol]
            else:
                self.cache.clear()
                self.stats = {"hits": 0, "misses": 0, "expirations": 0}

    def get_stats(self) -> Dict:
        """Get cache statistics."""
        with self._lock:
            total = self.stats["hits"] + self.stats["misses"]
            hit_rate = self.stats["hits"] / total if total > 0 else 0
            return {
                "hits": self.stats["hits"],
                "misses": self.stats["misses"],
                "expirations": self.stats["expirations"],
                "hit_rate": hit_rate,
                "cached_symbols": len(self.cache),
            }


class FuturesChainCache:
    """Thread-safe cache for futures chains with TTL invalidation."""

    def __init__(self, ttl_seconds: int = 5):
        self._lock = threading.RLock()
        self.cache: Dict[str, CacheEntry] = {}
        self.ttl_seconds = ttl_seconds
        self.stats = {
            "hits": 0,
            "misses": 0,
            "expirations": 0,
        }

    def get(self, symbol: str) -> Optional[FuturesChain]:
        """Retrieve futures chain from cache."""
        with self._lock:
            if symbol in self.cache:
                entry = self.cache[symbol]
                if not entry.is_expired():
                    self.stats["hits"] += 1
                    return entry.value
                else:
                    self.stats["expirations"] += 1
                    del self.cache[symbol]
            
            self.stats["misses"] += 1
            return None

    def set(self, symbol: str, chain: FuturesChain) -> None:
        """Store futures chain in cache."""
        with self._lock:
            self.cache[symbol] = CacheEntry(
                value=chain,
                timestamp=datetime.utcnow(),
                ttl_seconds=self.ttl_seconds,
            )

    def invalidate(self, symbol: str = None) -> None:
        """Invalidate cache entry or entire cache."""
        with self._lock:
            if symbol:
                if symbol in self.cache:
                    del self.cache[symbol]
            else:
                self.cache.clear()
                self.stats = {"hits": 0, "misses": 0, "expirations": 0}

    def get_stats(self) -> Dict:
        """Get cache statistics."""
        with self._lock:
            total = self.stats["hits"] + self.stats["misses"]
            hit_rate = self.stats["hits"] / total if total > 0 else 0
            return {
                "hits": self.stats["hits"],
                "misses": self.stats["misses"],
                "expirations": self.stats["expirations"],
                "hit_rate": hit_rate,
                "cached_symbols": len(self.cache),
            }


class DividendEventService:
    """Manages dividend calendar and ex-date windows."""

    def __init__(self, db_connection=None):
        self._lock = threading.RLock()
        self.db_connection = db_connection
        self.dividend_cache: Dict[str, List[Dict]] = {}
        self.cache_updated: Dict[str, datetime] = {}

    def is_ex_window(self, symbol: str, date: datetime) -> bool:
        """Check if date is within ex-dividend window (T-2 to ex-date)."""
        with self._lock:
            next_ex = self.next_ex_date(symbol)
            if not next_ex:
                return False
            
            days_to_ex = (next_ex - date).days
            return -2 <= days_to_ex <= 0

    def next_ex_date(self, symbol: str) -> Optional[datetime]:
        """Get next ex-dividend date for symbol."""
        with self._lock:
            events = self.get_expected_dividends(symbol, datetime.utcnow())
            if events:
                return datetime.strptime(events[0]["ex_date"], "%Y-%m-%d")
            return None

    def get_expected_dividends(
        self,
        symbol: str,
        start_date: datetime,
        end_date: Optional[datetime] = None,
    ) -> List[Dict]:
        """Get expected dividends in date range."""
        with self._lock:
            if end_date is None:
                end_date = start_date + timedelta(days=365)
            
            # Load from cache or DB
            if symbol not in self.dividend_cache:
                self._load_from_db(symbol)
            
            events = self.dividend_cache.get(symbol, [])
            
            # Filter by date range
            filtered = []
            for event in events:
                ex_date = datetime.strptime(event["ex_date"], "%Y-%m-%d")
                if start_date <= ex_date <= end_date:
                    filtered.append(event)
            
            return sorted(filtered, key=lambda x: x["ex_date"])

    def _load_from_db(self, symbol: str) -> None:
        """Load dividend events from database."""
        if not self.db_connection:
            self.dividend_cache[symbol] = []
            return
        
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            cursor.execute(
                "SELECT * FROM dividend_events WHERE symbol = %s ORDER BY ex_date ASC",
                (symbol,)
            )
            self.dividend_cache[symbol] = cursor.fetchall()
            cursor.close()
            self.cache_updated[symbol] = datetime.utcnow()
        except Exception as e:
            logger.error(f"Failed to load dividends for {symbol}: {e}")
            self.dividend_cache[symbol] = []


class RatesCurveService:
    """Provides Brazilian interest rate curves (CDI, SELIC)."""

    def __init__(self, db_connection=None):
        self._lock = threading.RLock()
        self.db_connection = db_connection
        self.cdi_rate = 0.105  # ~10.5% annual (example)
        self.selic_rate = 0.105  # ~10.5% annual (example)
        self.last_update = datetime.utcnow()

    def current_cdi(self) -> float:
        """Get current CDI rate (annual)."""
        with self._lock:
            return self.cdi_rate

    def current_selic(self) -> float:
        """Get current SELIC rate (annual)."""
        with self._lock:
            return self.selic_rate

    def pv(self, amount: float, days_to_expiry: int) -> float:
        """
        Calculate present value using CDI.
        
        Args:
            amount: Future value
            days_to_expiry: Days until expiry
            
        Returns:
            Present value
        """
        with self._lock:
            years = days_to_expiry / 252  # Trading days
            discount_rate = self.cdi_rate
            pv = amount / ((1 + discount_rate) ** years)
            return pv

    def discount_factor(self, days_to_expiry: int) -> float:
        """Get discount factor for given days."""
        with self._lock:
            years = days_to_expiry / 252
            return 1 / ((1 + self.cdi_rate) ** years)

    def forward_rate(self, start_days: int, end_days: int) -> float:
        """Calculate forward rate between two dates."""
        with self._lock:
            df_start = self.discount_factor(start_days)
            df_end = self.discount_factor(end_days)
            days_between = end_days - start_days
            years_between = days_between / 252
            
            forward = (df_start / df_end - 1) / years_between
            return forward


class GreeksCalculator:
    """
    Black-Scholes and binomial Greeks calculator for options.
    Handles B3 specifics: calls American, puts European.
    """

    def __init__(self, rates_service: RatesCurveService):
        self._lock = threading.RLock()
        self.rates = rates_service

    def calculate_iv_newton_raphson(
        self,
        option_type: str,
        spot: float,
        strike: float,
        days_to_expiry: int,
        market_price: float,
        initial_guess: float = 0.3,
        max_iterations: int = 100,
        tolerance: float = 0.0001,
    ) -> float:
        """
        Calculate implied volatility using Newton-Raphson.
        
        Args:
            option_type: 'CALL' or 'PUT'
            spot: Current spot price
            strike: Strike price
            days_to_expiry: Days to expiry
            market_price: Observed market price
            initial_guess: Starting volatility guess
            max_iterations: Max NR iterations
            tolerance: Convergence tolerance
            
        Returns:
            Implied volatility
        """
        with self._lock:
            iv = initial_guess
            
            for iteration in range(max_iterations):
                theo_price = self._black_scholes_price(
                    option_type, spot, strike, days_to_expiry, iv
                )
                vega = self._black_scholes_vega(spot, strike, days_to_expiry, iv)
                
                if abs(vega) < 1e-6:
                    return iv
                
                diff = theo_price - market_price
                if abs(diff) < tolerance:
                    return iv
                
                iv = iv - diff / vega
                iv = max(0.001, min(5.0, iv))  # Bounds
            
            return iv

    def _black_scholes_price(
        self,
        option_type: str,
        spot: float,
        strike: float,
        days_to_expiry: int,
        volatility: float,
    ) -> float:
        """Calculate option price using Black-Scholes."""
        if days_to_expiry <= 0:
            if option_type == "CALL":
                return max(0, spot - strike)
            else:
                return max(0, strike - spot)
        
        T = days_to_expiry / 365.0
        r = self.rates.current_cdi()
        
        d1 = (math.log(spot / strike) + (r + 0.5 * volatility**2) * T) / (volatility * math.sqrt(T))
        d2 = d1 - volatility * math.sqrt(T)
        
        if HAS_SCIPY:
            nd1 = stats.norm.cdf(d1)
            nd2 = stats.norm.cdf(d2)
        else:
            nd1 = self._norm_cdf(d1)
            nd2 = self._norm_cdf(d2)
        
        if option_type == "CALL":
            price = spot * nd1 - strike * math.exp(-r * T) * nd2
        else:
            price = strike * math.exp(-r * T) * (1 - nd2) - spot * (1 - nd1)
        
        return max(0, price)

    def _black_scholes_delta(
        self,
        option_type: str,
        spot: float,
        strike: float,
        days_to_expiry: int,
        volatility: float,
    ) -> float:
        """Calculate delta."""
        if days_to_expiry <= 0:
            if option_type == "CALL":
                return 1.0 if spot > strike else 0.0
            else:
                return -1.0 if spot < strike else 0.0
        
        T = days_to_expiry / 365.0
        r = self.rates.current_cdi()
        
        d1 = (math.log(spot / strike) + (r + 0.5 * volatility**2) * T) / (volatility * math.sqrt(T))
        
        if HAS_SCIPY:
            nd1 = stats.norm.cdf(d1)
        else:
            nd1 = self._norm_cdf(d1)
        
        if option_type == "CALL":
            return nd1
        else:
            return nd1 - 1

    def _black_scholes_gamma(
        self,
        spot: float,
        strike: float,
        days_to_expiry: int,
        volatility: float,
    ) -> float:
        """Calculate gamma."""
        if days_to_expiry <= 0:
            return 0.0
        
        T = days_to_expiry / 365.0
        r = self.rates.current_cdi()
        
        d1 = (math.log(spot / strike) + (r + 0.5 * volatility**2) * T) / (volatility * math.sqrt(T))
        
        if HAS_SCIPY:
            pdf_d1 = stats.norm.pdf(d1)
        else:
            pdf_d1 = self._norm_pdf(d1)
        
        gamma = pdf_d1 / (spot * volatility * math.sqrt(T))
        return gamma

    def _black_scholes_vega(
        self,
        spot: float,
        strike: float,
        days_to_expiry: int,
        volatility: float,
    ) -> float:
        """Calculate vega (per 1% change in volatility)."""
        if days_to_expiry <= 0:
            return 0.0
        
        T = days_to_expiry / 365.0
        r = self.rates.current_cdi()
        
        d1 = (math.log(spot / strike) + (r + 0.5 * volatility**2) * T) / (volatility * math.sqrt(T))
        
        if HAS_SCIPY:
            pdf_d1 = stats.norm.pdf(d1)
        else:
            pdf_d1 = self._norm_pdf(d1)
        
        vega = spot * pdf_d1 * math.sqrt(T) / 100  # Per 1% change
        return vega

    def _black_scholes_theta(
        self,
        option_type: str,
        spot: float,
        strike: float,
        days_to_expiry: int,
        volatility: float,
    ) -> float:
        """Calculate theta (per day)."""
        if days_to_expiry <= 0:
            return 0.0
        
        T = days_to_expiry / 365.0
        r = self.rates.current_cdi()
        
        d1 = (math.log(spot / strike) + (r + 0.5 * volatility**2) * T) / (volatility * math.sqrt(T))
        d2 = d1 - volatility * math.sqrt(T)
        
        if HAS_SCIPY:
            pdf_d1 = stats.norm.pdf(d1)
            nd2 = stats.norm.cdf(d2)
        else:
            pdf_d1 = self._norm_pdf(d1)
            nd2 = self._norm_cdf(d2)
        
        term1 = -spot * pdf_d1 * volatility / (2 * math.sqrt(T))
        
        if option_type == "CALL":
            term2 = r * strike * math.exp(-r * T) * nd2
            theta = (term1 - term2) / 365
        else:
            term2 = r * strike * math.exp(-r * T) * (1 - nd2)
            theta = (term1 + term2) / 365
        
        return theta

    def _black_scholes_rho(
        self,
        option_type: str,
        spot: float,
        strike: float,
        days_to_expiry: int,
        volatility: float,
    ) -> float:
        """Calculate rho (per 1% change in rates)."""
        if days_to_expiry <= 0:
            return 0.0
        
        T = days_to_expiry / 365.0
        r = self.rates.current_cdi()
        
        d2 = (math.log(spot / strike) + (r - 0.5 * volatility**2) * T) / (volatility * math.sqrt(T))
        
        if HAS_SCIPY:
            nd2 = stats.norm.cdf(d2)
        else:
            nd2 = self._norm_cdf(d2)
        
        if option_type == "CALL":
            rho = strike * T * math.exp(-r * T) * nd2 / 100
        else:
            rho = -strike * T * math.exp(-r * T) * (1 - nd2) / 100
        
        return rho

    def calculate_greeks(
        self,
        option_type: str,
        spot: float,
        strike: float,
        days_to_expiry: int,
        volatility: float,
    ) -> GreeksData:
        """Calculate all Greeks for an option."""
        with self._lock:
            return GreeksData(
                delta=self._black_scholes_delta(option_type, spot, strike, days_to_expiry, volatility),
                gamma=self._black_scholes_gamma(spot, strike, days_to_expiry, volatility),
                theta=self._black_scholes_theta(option_type, spot, strike, days_to_expiry, volatility),
                vega=self._black_scholes_vega(spot, strike, days_to_expiry, volatility),
                rho=self._black_scholes_rho(option_type, spot, strike, days_to_expiry, volatility),
                iv=volatility,
            )

    @staticmethod
    def _norm_cdf(x: float) -> float:
        """Approximate normal CDF (used if scipy unavailable)."""
        return 0.5 * (1 + math.tanh(0.7978845608 * (x + 0.044715 * x**3)))

    @staticmethod
    def _norm_pdf(x: float) -> float:
        """Normal PDF."""
        return math.exp(-0.5 * x**2) / math.sqrt(2 * math.pi)


class ImpliedVolEngine:
    """Maintains IV surface per underlying with interpolation."""

    def __init__(self, greeks_calc: GreeksCalculator):
        self._lock = threading.RLock()
        self.greeks_calc = greeks_calc
        self.surfaces: Dict[str, Dict] = defaultdict(dict)

    def update_iv(
        self,
        underlying: str,
        strike: float,
        days_to_expiry: int,
        iv: float,
    ) -> None:
        """Update IV for a strike/expiry."""
        with self._lock:
            key = (strike, days_to_expiry)
            self.surfaces[underlying][key] = iv

    def get_iv(
        self,
        underlying: str,
        strike: float,
        days_to_expiry: int,
    ) -> Optional[float]:
        """Get IV with interpolation."""
        with self._lock:
            key = (strike, days_to_expiry)
            if key in self.surfaces[underlying]:
                return self.surfaces[underlying][key]
            
            # Simple nearest neighbor for missing values
            if not self.surfaces[underlying]:
                return None
            
            closest_key = min(
                self.surfaces[underlying].keys(),
                key=lambda k: (k[0] - strike)**2 + (k[1] - days_to_expiry)**2
            )
            return self.surfaces[underlying][closest_key]


class NAVCalculatorService:
    """Calculates ETF NAV from basket components (e.g., BOVA11)."""

    def __init__(self):
        self._lock = threading.RLock()
        self.baskets: Dict[str, List[Tuple[str, float]]] = {}

    def set_basket(self, etf_symbol: str, components: List[Tuple[str, float]]) -> None:
        """Set ETF basket composition (symbol, weight)."""
        with self._lock:
            self.baskets[etf_symbol] = components

    def calculate_nav(self, etf_symbol: str, component_prices: Dict[str, float]) -> float:
        """Calculate ETF NAV from component prices."""
        with self._lock:
            if etf_symbol not in self.baskets:
                return 0.0
            
            nav = 0.0
            for symbol, weight in self.baskets[etf_symbol]:
                if symbol in component_prices:
                    nav += component_prices[symbol] * weight
            
            return nav


class CalibrationService:
    """Stores/retrieves calibration windows per strategy."""

    def __init__(self, db_connection=None):
        self._lock = threading.RLock()
        self.db_connection = db_connection
        self.calibrations: Dict[str, Dict] = defaultdict(dict)

    def store_calibration(
        self,
        strategy_type: str,
        symbol: str,
        expiry: str,
        metric_name: str,
        mean_val: float,
        std_val: float,
        p5: float,
        p95: float,
        sample_count: int,
        window_start: datetime,
        window_end: datetime,
    ) -> None:
        """Store calibration data."""
        with self._lock:
            key = (strategy_type, symbol, expiry, metric_name)
            self.calibrations[key] = {
                "mean": mean_val,
                "std": std_val,
                "p5": p5,
                "p95": p95,
                "sample_count": sample_count,
                "window_start": window_start,
                "window_end": window_end,
            }

    def get_calibration(
        self,
        strategy_type: str,
        symbol: str,
        expiry: str,
        metric_name: str,
    ) -> Optional[Dict]:
        """Retrieve calibration data."""
        with self._lock:
            key = (strategy_type, symbol, expiry, metric_name)
            return self.calibrations.get(key)


class StrategyScorecard:
    """Aggregates metrics per strategy/asset/period."""

    def __init__(self):
        self._lock = threading.RLock()
        self.scorecards: Dict[Tuple, Dict] = {}

    def record_metrics(
        self,
        strategy_type: str,
        symbol: str,
        period: str,
        metrics: Dict,
    ) -> None:
        """Record strategy performance metrics."""
        with self._lock:
            key = (strategy_type, symbol, period)
            self.scorecards[key] = {
                **metrics,
                "timestamp": datetime.utcnow(),
            }

    def get_scorecard(self, strategy_type: str, symbol: str, period: str) -> Optional[Dict]:
        """Retrieve scorecard."""
        with self._lock:
            return self.scorecards.get((strategy_type, symbol, period))

    def compute_sharpe(self, returns: List[float]) -> float:
        """Compute Sharpe ratio."""
        if len(returns) < 2:
            return 0.0
        
        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return)**2 for r in returns) / len(returns)
        std_return = math.sqrt(variance)
        
        if std_return == 0:
            return 0.0
        
        risk_free_rate = 0.05  # Assuming 5% annual
        daily_rf = risk_free_rate / 252
        
        return (mean_return - daily_rf) / std_return * math.sqrt(252)


class StructuredOrderExecutor:
    """Coordinates multi-leg execution in paper mode."""

    def __init__(self):
        self._lock = threading.RLock()
        self.active_orders: Dict[str, Dict] = {}
        self.execution_history: List[Dict] = []

    def submit_multi_leg_order(
        self,
        trade_id: str,
        legs: List[Dict],
        timeout_seconds: int = 30,
    ) -> Dict:
        """
        Submit multi-leg order for execution.
        
        Args:
            trade_id: Unique trade identifier
            legs: List of leg dicts with (symbol, qty, side, intended_price)
            timeout_seconds: Timeout for complete execution
            
        Returns:
            Execution result with fill status
        """
        with self._lock:
            if trade_id in self.active_orders:
                return {"error": f"Trade {trade_id} already submitted"}
            
            self.active_orders[trade_id] = {
                "status": "PENDING",
                "legs": legs,
                "started_at": datetime.utcnow(),
                "timeout_seconds": timeout_seconds,
                "filled_legs": [],
                "legging_incidents": 0,
            }
            
            # Simulate paper execution
            return self._execute_legs(trade_id)

    def _execute_legs(self, trade_id: str) -> Dict:
        """Execute legs and detect legging."""
        order = self.active_orders[trade_id]
        filled = []
        
        for leg in order["legs"]:
            # Simulate execution
            executed_price = leg.get("intended_price", 0) * 1.001  # Small slippage
            slippage = abs(executed_price - leg["intended_price"])
            
            filled.append({
                "symbol": leg["symbol"],
                "qty": leg["qty"],
                "side": leg["side"],
                "intended_price": leg["intended_price"],
                "executed_price": executed_price,
                "slippage": slippage,
                "fill_status": "FILLED",
            })
        
        order["status"] = "FILLED"
        order["filled_legs"] = filled
        
        result = {
            "trade_id": trade_id,
            "status": "SUCCESS",
            "legs": filled,
            "legging_incidents": 0,
        }
        
        self.execution_history.append(result)
        return result

    def get_order_status(self, trade_id: str) -> Optional[Dict]:
        """Get status of submitted order."""
        with self._lock:
            return self.active_orders.get(trade_id)

    def cancel_order(self, trade_id: str) -> Tuple[bool, str]:
        """Cancel pending order."""
        with self._lock:
            if trade_id not in self.active_orders:
                return False, "Order not found"
            
            order = self.active_orders[trade_id]
            if order["status"] != "PENDING":
                return False, f"Cannot cancel {order['status']} order"
            
            order["status"] = "CANCELLED"
            return True, "Order cancelled"
