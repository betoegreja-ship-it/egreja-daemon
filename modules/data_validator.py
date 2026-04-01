"""
Market Data Validator for Egreja Investment AI v10.22

Self-contained validator module that tracks price data quality, validates incoming prices,
detects anomalies, and manages circuit breakers for data sources. No imports from api_server.py.

Thread-safe price tracking with configurable validation rules via environment variables.
"""

import os
import time
import logging
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
from math import isnan, isinf

__all__ = [
    'MarketDataValidator',
    'ValidationResult',
    'CircuitBreakerState',
    'PriceAnomalyDetector',
    'HistoricalSnapshot',
]

logger = logging.getLogger(__name__)


class CircuitBreakerState(Enum):
    """States for circuit breaker implementation."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Breaker tripped, rejecting requests
    HALF_OPEN = "half_open"  # Testing if source recovered


@dataclass
class ValidationResult:
    """Result from price validation."""
    valid: bool
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    adjusted_price: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class PriceRecord:
    """Internal record of a single price observation."""
    price: float
    source: str
    timestamp: float
    symbol: str


class CircuitBreaker:
    """
    Per-source circuit breaker for tracking consecutive errors.

    States:
    - CLOSED: Normal operation, accepting requests
    - OPEN: Tripped after N consecutive errors, rejecting requests
    - HALF_OPEN: Testing recovery after timeout
    """

    def __init__(self, error_threshold: int, reset_timeout_s: float):
        """
        Initialize circuit breaker.

        Args:
            error_threshold: Number of consecutive errors to trip breaker
            reset_timeout_s: Seconds after trip to auto-reset to HALF_OPEN
        """
        self.error_threshold = error_threshold
        self.reset_timeout_s = reset_timeout_s

        self.state = CircuitBreakerState.CLOSED
        self.error_count = 0
        self.last_error_time: Optional[float] = None
        self.last_trip_time: Optional[float] = None

        self._lock = threading.RLock()

    def record_error(self) -> None:
        """Record an error and potentially trip the breaker."""
        with self._lock:
            self.error_count += 1
            self.last_error_time = time.time()

            if self.error_count >= self.error_threshold:
                self.state = CircuitBreakerState.OPEN
                self.last_trip_time = time.time()
                logger.warning(
                    f"Circuit breaker tripped after {self.error_count} errors"
                )

    def record_success(self) -> None:
        """Reset error count on success."""
        with self._lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                # Recovery successful
                self.state = CircuitBreakerState.CLOSED
                self.error_count = 0
                logger.info("Circuit breaker recovered to CLOSED state")

    def check_state(self) -> CircuitBreakerState:
        """
        Check current state and auto-reset if timeout reached.

        Returns:
            Current circuit breaker state
        """
        with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                elapsed = time.time() - (self.last_trip_time or 0)
                if elapsed >= self.reset_timeout_s:
                    self.state = CircuitBreakerState.HALF_OPEN
                    logger.info(
                        f"Circuit breaker auto-reset to HALF_OPEN after "
                        f"{elapsed:.1f}s timeout"
                    )

            return self.state

    def is_open(self) -> bool:
        """Check if breaker is OPEN or HALF_OPEN (rejecting requests)."""
        state = self.check_state()
        return state in (CircuitBreakerState.OPEN, CircuitBreakerState.HALF_OPEN)

    def reset(self) -> None:
        """Manually reset circuit breaker to CLOSED."""
        with self._lock:
            self.state = CircuitBreakerState.CLOSED
            self.error_count = 0
            self.last_error_time = None
            self.last_trip_time = None
            logger.info("Circuit breaker manually reset to CLOSED")

    def get_reason(self) -> str:
        """Get human-readable reason for current state."""
        with self._lock:
            state = self.check_state()
            if state == CircuitBreakerState.CLOSED:
                return "CLOSED (normal)"
            elif state == CircuitBreakerState.OPEN:
                return f"OPEN (error_count={self.error_count})"
            else:
                return f"HALF_OPEN (testing recovery)"


class PriceAnomalyDetector:
    """Detects anomalous price movements and data quality issues."""

    def __init__(self):
        """Initialize anomaly detector."""
        self.last_prices: Dict[str, float] = {}
        self.frozen_count: Dict[str, int] = defaultdict(int)
        self.zero_price_count: Dict[str, int] = defaultdict(int)

    def detect_anomalies(
        self,
        symbol: str,
        price: float,
        asset_class: str = "unknown"
    ) -> List[str]:
        """
        Detect anomalous prices.

        Args:
            symbol: Asset symbol
            price: Price to check
            asset_class: Asset class (stock, crypto, etc.)

        Returns:
            List of anomaly warnings
        """
        warnings = []

        # Check for zero prices
        if price == 0:
            self.zero_price_count[symbol] += 1
            warnings.append(
                f"Zero price detected for {symbol} "
                f"(count: {self.zero_price_count[symbol]})"
            )
        else:
            self.zero_price_count[symbol] = 0

        # Check for frozen feed (identical sequential prices)
        if symbol in self.last_prices:
            if self.last_prices[symbol] == price:
                self.frozen_count[symbol] += 1
                if self.frozen_count[symbol] > 10:
                    warnings.append(
                        f"Frozen feed detected for {symbol} "
                        f"({self.frozen_count[symbol]} identical prices)"
                    )
            else:
                self.frozen_count[symbol] = 0

        self.last_prices[symbol] = price

        # Check asset class expectations
        if asset_class == "crypto" and symbol.endswith("USD"):
            # Bitcoin/major crypto should be >$100
            if price < 100:
                warnings.append(
                    f"Price {price} unusually low for crypto {symbol}"
                )
        elif asset_class == "stock":
            # Penny stocks start around $0.01, but most should be >$1
            if price < 0.01:
                warnings.append(
                    f"Price {price} below typical stock range for {symbol}"
                )

        return warnings

    def reset(self, symbol: Optional[str] = None) -> None:
        """Reset anomaly tracking for a symbol or all symbols."""
        if symbol:
            self.frozen_count.pop(symbol, None)
            self.zero_price_count.pop(symbol, None)
            self.last_prices.pop(symbol, None)
        else:
            self.frozen_count.clear()
            self.zero_price_count.clear()
            self.last_prices.clear()


class HistoricalSnapshot:
    """Stores historical price snapshots for audit and replay."""

    def __init__(self, max_snapshots: int = 1000):
        """
        Initialize snapshot storage.

        Args:
            max_snapshots: Maximum number of snapshots to keep
        """
        self.max_snapshots = max_snapshots
        self.snapshots: Dict[float, Dict[str, float]] = {}
        self.snapshot_times: deque = deque(maxlen=max_snapshots)
        self._lock = threading.RLock()

    def save_snapshot(
        self,
        timestamp: float,
        prices_dict: Dict[str, float]
    ) -> None:
        """
        Save a price snapshot at a specific timestamp.

        Args:
            timestamp: Snapshot timestamp
            prices_dict: Dictionary of symbol -> price
        """
        with self._lock:
            self.snapshots[timestamp] = prices_dict.copy()
            self.snapshot_times.append(timestamp)

            # If we exceeded max, remove oldest
            if len(self.snapshots) > self.max_snapshots:
                oldest = min(self.snapshots.keys())
                del self.snapshots[oldest]

    def get_snapshot(self, timestamp: float) -> Optional[Dict[str, float]]:
        """
        Retrieve a snapshot by timestamp.

        Args:
            timestamp: Snapshot timestamp

        Returns:
            Dictionary of symbol -> price, or None if not found
        """
        with self._lock:
            return self.snapshots.get(timestamp)

    def get_snapshots_in_range(
        self,
        start_timestamp: float,
        end_timestamp: float
    ) -> Dict[float, Dict[str, float]]:
        """
        Get all snapshots in a time range.

        Args:
            start_timestamp: Start time
            end_timestamp: End time

        Returns:
            Dictionary of timestamp -> prices
        """
        with self._lock:
            return {
                ts: prices
                for ts, prices in self.snapshots.items()
                if start_timestamp <= ts <= end_timestamp
            }

    def clear(self) -> None:
        """Clear all snapshots."""
        with self._lock:
            self.snapshots.clear()
            self.snapshot_times.clear()


class MarketDataValidator:
    """
    Primary validator for market data with price tracking, anomaly detection,
    and circuit breaker management.

    Configuration via environment variables:
    - DATA_MAX_PRICE_CHANGE_PCT: Max % change vs last known (default: 50.0)
    - DATA_STALE_THRESHOLD_S: Staleness threshold for stocks (default: 300)
    - DATA_STALE_THRESHOLD_CRYPTO_S: Staleness threshold for crypto (default: 60)
    - DATA_MIN_SOURCES_REQUIRED: Min sources for trade decision (default: 1)
    - DATA_CIRCUIT_BREAKER_ERRORS: Errors to trip breaker (default: 5)
    - DATA_CIRCUIT_BREAKER_RESET_S: Reset timeout (default: 300)
    """

    def __init__(self):
        """Initialize the market data validator."""
        # Load configuration from environment
        self.max_price_change_pct = float(
            os.getenv('DATA_MAX_PRICE_CHANGE_PCT', '50.0')
        )
        self.stale_threshold_s = float(
            os.getenv('DATA_STALE_THRESHOLD_S', '300')
        )
        self.stale_threshold_crypto_s = float(
            os.getenv('DATA_STALE_THRESHOLD_CRYPTO_S', '60')
        )
        self.min_sources_required = int(
            os.getenv('DATA_MIN_SOURCES_REQUIRED', '1')
        )
        self.circuit_breaker_errors = int(
            os.getenv('DATA_CIRCUIT_BREAKER_ERRORS', '5')
        )
        self.circuit_breaker_reset_s = float(
            os.getenv('DATA_CIRCUIT_BREAKER_RESET_S', '300')
        )

        # Price tracking
        self.last_prices: Dict[str, PriceRecord] = {}
        self.price_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=20)
        )

        # Circuit breakers per source
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}

        # Anomaly detection
        self.anomaly_detector = PriceAnomalyDetector()

        # Historical snapshots
        self.snapshots = HistoricalSnapshot()

        # Thread safety
        self._lock = threading.RLock()

        logger.info("MarketDataValidator initialized")
        logger.info(f"  max_price_change_pct: {self.max_price_change_pct}%")
        logger.info(f"  stale_threshold_s: {self.stale_threshold_s}s")
        logger.info(f"  stale_threshold_crypto_s: {self.stale_threshold_crypto_s}s")
        logger.info(f"  min_sources_required: {self.min_sources_required}")
        logger.info(f"  circuit_breaker_errors: {self.circuit_breaker_errors}")
        logger.info(f"  circuit_breaker_reset_s: {self.circuit_breaker_reset_s}s")

    def validate_price(
        self,
        symbol: str,
        price: float,
        source: str,
        timestamp: Optional[float] = None,
        asset_class: str = "unknown"
    ) -> ValidationResult:
        """
        Validate an incoming price.

        Checks:
        - Price > 0
        - Price is not NaN or inf
        - Price change within configured range vs last known
        - Data not stale

        Args:
            symbol: Asset symbol
            price: Price value
            source: Data source name
            timestamp: Price timestamp (defaults to now)
            asset_class: Asset class for anomaly detection

        Returns:
            ValidationResult with validity, warnings, and errors
        """
        if timestamp is None:
            timestamp = time.time()

        errors = []
        warnings = []
        adjusted_price = None

        # Check for NaN/inf
        if isnan(price) or isinf(price):
            errors.append(f"Price is NaN or inf: {price}")
            return ValidationResult(valid=False, warnings=warnings, errors=errors)

        # Check price > 0
        if price <= 0:
            errors.append(f"Price must be > 0, got {price}")
            return ValidationResult(valid=False, warnings=warnings, errors=errors)

        with self._lock:
            # Check price change vs last known
            if symbol in self.last_prices:
                last_price = self.last_prices[symbol].price
                pct_change = abs((price - last_price) / last_price) * 100

                if pct_change > self.max_price_change_pct:
                    errors.append(
                        f"Price change {pct_change:.2f}% exceeds max "
                        f"{self.max_price_change_pct}% (${last_price} -> ${price})"
                    )

            # Check staleness
            is_stale, age_s = self.check_staleness(symbol, asset_class)
            if is_stale:
                warnings.append(
                    f"Data is stale: {age_s:.1f}s old for {symbol}"
                )

            # Detect anomalies
            anomalies = self.anomaly_detector.detect_anomalies(
                symbol, price, asset_class
            )
            warnings.extend(anomalies)

        # Return result
        valid = len(errors) == 0
        adjusted_price = price if valid else None

        return ValidationResult(
            valid=valid,
            warnings=warnings,
            errors=errors,
            adjusted_price=adjusted_price
        )

    def check_staleness(
        self,
        symbol: str,
        asset_class: str = "unknown"
    ) -> Tuple[bool, float]:
        """
        Check if price data is stale.

        Args:
            symbol: Asset symbol
            asset_class: Asset class (stock or crypto)

        Returns:
            Tuple of (is_stale, age_in_seconds)
        """
        with self._lock:
            if symbol not in self.last_prices:
                return (False, 0.0)

            last_record = self.last_prices[symbol]
            age_s = time.time() - last_record.timestamp

            # Use appropriate threshold based on asset class
            if asset_class == "crypto":
                threshold = self.stale_threshold_crypto_s
            else:
                threshold = self.stale_threshold_s

            is_stale = age_s > threshold
            return (is_stale, age_s)

    def check_source_divergence(
        self,
        symbol: str,
        prices_by_source: Dict[str, float]
    ) -> Tuple[bool, float]:
        """
        Check for significant price divergence across sources.

        Args:
            symbol: Asset symbol
            prices_by_source: Dictionary of source -> price

        Returns:
            Tuple of (has_divergence, max_divergence_pct)
        """
        if not prices_by_source or len(prices_by_source) < 2:
            return (False, 0.0)

        prices = list(prices_by_source.values())
        min_price = min(prices)
        max_price = max(prices)

        if min_price == 0:
            return (True, 100.0)

        divergence_pct = ((max_price - min_price) / min_price) * 100

        # Flag if divergence > 5%
        has_divergence = divergence_pct > 5.0

        return (has_divergence, divergence_pct)

    def record_price(
        self,
        symbol: str,
        price: float,
        source: str,
        timestamp: Optional[float] = None
    ) -> None:
        """
        Record a validated price for tracking.

        Args:
            symbol: Asset symbol
            price: Price value
            source: Data source
            timestamp: Price timestamp (defaults to now)
        """
        if timestamp is None:
            timestamp = time.time()

        with self._lock:
            record = PriceRecord(
                price=price,
                source=source,
                timestamp=timestamp,
                symbol=symbol
            )
            self.last_prices[symbol] = record
            self.price_history[symbol].append(record)

        logger.debug(f"Recorded price: {symbol} ${price} from {source}")

    def record_error(self, source: str) -> None:
        """
        Record an error from a data source for circuit breaker tracking.

        Args:
            source: Data source name
        """
        with self._lock:
            if source not in self.circuit_breakers:
                self.circuit_breakers[source] = CircuitBreaker(
                    self.circuit_breaker_errors,
                    self.circuit_breaker_reset_s
                )

            self.circuit_breakers[source].record_error()

    def record_success(self, source: str) -> None:
        """
        Record successful data retrieval from a source.

        Args:
            source: Data source name
        """
        with self._lock:
            if source in self.circuit_breakers:
                self.circuit_breakers[source].record_success()

    def is_circuit_broken(
        self,
        source: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Check if a source's circuit breaker is open.

        Args:
            source: Data source name (None to check any)

        Returns:
            Tuple of (is_broken, reason_string)
        """
        with self._lock:
            if source:
                if source not in self.circuit_breakers:
                    return (False, "No circuit breaker for this source")

                cb = self.circuit_breakers[source]
                is_open = cb.is_open()
                reason = cb.get_reason()
                return (is_open, reason)
            else:
                # Check any source
                for src, cb in self.circuit_breakers.items():
                    if cb.is_open():
                        return (True, f"{src}: {cb.get_reason()}")

                return (False, "All circuits CLOSED")

    def reset_circuit_breaker(self, source: Optional[str] = None) -> None:
        """
        Manually reset a circuit breaker.

        Args:
            source: Data source name (None to reset all)
        """
        with self._lock:
            if source:
                if source in self.circuit_breakers:
                    self.circuit_breakers[source].reset()
                    logger.info(f"Reset circuit breaker for {source}")
            else:
                for cb in self.circuit_breakers.values():
                    cb.reset()
                logger.info("Reset all circuit breakers")

    def get_last_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get the last recorded price for a symbol.

        Args:
            symbol: Asset symbol

        Returns:
            Dictionary with price, source, timestamp; or None if not found
        """
        with self._lock:
            if symbol not in self.last_prices:
                return None

            record = self.last_prices[symbol]
            return {
                'symbol': symbol,
                'price': record.price,
                'source': record.source,
                'timestamp': record.timestamp,
                'age_s': time.time() - record.timestamp
            }

    def get_price_history(
        self,
        symbol: str,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get price history for a symbol.

        Args:
            symbol: Asset symbol
            limit: Max number of records to return

        Returns:
            List of price records, most recent first
        """
        with self._lock:
            if symbol not in self.price_history:
                return []

            history = list(self.price_history[symbol])
            history.reverse()

            if limit:
                history = history[:limit]

            return [
                {
                    'price': r.price,
                    'source': r.source,
                    'timestamp': r.timestamp
                }
                for r in history
            ]

    def get_data_quality_status(self) -> Dict[str, Any]:
        """
        Get comprehensive data quality status for /ops endpoint.

        Returns:
            Dictionary with staleness %, error rates, breaker status
        """
        with self._lock:
            now = time.time()

            # Calculate staleness metrics
            stale_count = 0
            total_count = 0

            for symbol, record in self.last_prices.items():
                total_count += 1
                age = now - record.timestamp

                # Use conservative threshold for status
                if age > self.stale_threshold_s:
                    stale_count += 1

            staleness_pct = (
                (stale_count / total_count * 100) if total_count > 0 else 0
            )

            # Circuit breaker status
            breaker_status = {}
            for source, cb in self.circuit_breakers.items():
                state = cb.check_state()
                breaker_status[source] = {
                    'state': state.value,
                    'error_count': cb.error_count,
                    'reason': cb.get_reason()
                }

            return {
                'timestamp': now,
                'total_symbols_tracked': total_count,
                'stale_symbols': stale_count,
                'staleness_pct': round(staleness_pct, 2),
                'circuit_breakers': breaker_status,
                'frozen_feeds': dict(self.anomaly_detector.frozen_count),
                'zero_prices': dict(self.anomaly_detector.zero_price_count)
            }

    def save_snapshot(
        self,
        timestamp: Optional[float] = None
    ) -> None:
        """
        Save a snapshot of current prices for audit/replay.

        Args:
            timestamp: Snapshot timestamp (defaults to now)
        """
        if timestamp is None:
            timestamp = time.time()

        with self._lock:
            prices_dict = {
                symbol: record.price
                for symbol, record in self.last_prices.items()
            }

        self.snapshots.save_snapshot(timestamp, prices_dict)
        logger.debug(f"Saved snapshot with {len(prices_dict)} prices")

    def get_snapshot(self, timestamp: float) -> Optional[Dict[str, float]]:
        """
        Retrieve a historical snapshot.

        Args:
            timestamp: Snapshot timestamp

        Returns:
            Dictionary of symbol -> price, or None if not found
        """
        return self.snapshots.get_snapshot(timestamp)

    def clear_all(self) -> None:
        """Clear all internal state (for testing)."""
        with self._lock:
            self.last_prices.clear()
            self.price_history.clear()
            self.circuit_breakers.clear()
            self.anomaly_detector.reset()
            self.snapshots.clear()
            logger.info("Cleared all validator state")
