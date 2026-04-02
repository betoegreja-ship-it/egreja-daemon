"""
Broker Abstraction Layer for Egreja Investment AI v10.22

This module provides a self-contained abstraction for interacting with multiple
trading brokers (B3/BTG, Binance, NYSE). It defines order tracking, execution
simulation, and broker implementations with zero dependencies on api_server.

No imports from api_server.py - fully independent.
"""

import logging
import os
import threading
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from statistics import mean, stdev
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS
# ============================================================================

class OrderStatus(Enum):
    """Order lifecycle status."""
    PENDING = "pending"
    SENT = "sent"
    PARTIAL_FILL = "partial_fill"
    FILLED = "filled"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    ERROR = "error"


class OrderSide(Enum):
    """Order direction: buy or sell."""
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Order execution type."""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class AssetClass(Enum):
    """Asset class categories."""
    STOCK_BR = "stock_br"  # B3 (Brazil)
    STOCK_US = "stock_us"  # NYSE/NASDAQ
    CRYPTO = "crypto"      # Binance


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class FillRecord:
    """Partial fill record."""
    qty: float
    price: float
    ts: float  # Unix timestamp
    fee: float = 0.0


@dataclass
class OrderRecord:
    """Complete order record with execution details."""
    order_id: str
    trade_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    asset_class: AssetClass
    quantity: float
    decision_price: float
    sent_price: Optional[float] = None
    executed_price: Optional[float] = None
    average_price: Optional[float] = None
    slippage: Optional[float] = None
    latency_ms: Optional[float] = None
    status: OrderStatus = OrderStatus.PENDING
    fills: List[FillRecord] = field(default_factory=list)
    fees_estimated: float = 0.0
    fees_actual: float = 0.0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    retry_count: int = 0
    idempotency_key: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, handling enums and dataclasses."""
        data = asdict(self)
        data["side"] = self.side.value
        data["order_type"] = self.order_type.value
        data["asset_class"] = self.asset_class.value
        data["status"] = self.status.value
        data["fills"] = [asdict(f) for f in self.fills]
        return data

    def calculate_slippage(self) -> float:
        """Calculate slippage: average_price - decision_price."""
        if self.average_price is not None:
            return self.average_price - self.decision_price
        return 0.0

    def calculate_average_fill_price(self) -> float:
        """Calculate volume-weighted average fill price."""
        if not self.fills:
            return 0.0
        total_qty = sum(f.qty for f in self.fills)
        if total_qty == 0:
            return 0.0
        total_value = sum(f.qty * f.price for f in self.fills)
        return total_value / total_qty


# ============================================================================
# ABSTRACT BROKER BASE CLASS
# ============================================================================

class AbstractBroker(ABC):
    """
    Abstract base class for broker implementations.

    All broker integrations must implement this interface to ensure
    consistent order submission, cancellation, and position reconciliation.
    """

    def __init__(self, asset_class: AssetClass, config: Optional[Dict[str, str]] = None):
        """
        Initialize broker.

        Args:
            asset_class: The asset class this broker handles
            config: Optional configuration dictionary
        """
        self.asset_class = asset_class
        self.config = config or {}
        self._lock = RLock()
        logger.info(
            f"Initializing {self.__class__.__name__} for {asset_class.value}"
        )

    @abstractmethod
    def submit_order(self, order: OrderRecord) -> OrderRecord:
        """
        Submit order to broker.

        Args:
            order: OrderRecord to submit

        Returns:
            Updated OrderRecord with sent status and timestamp

        Raises:
            ValueError: Invalid order
            ConnectionError: Broker connection failure
        """
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an open order.

        Args:
            order_id: Order ID to cancel

        Returns:
            True if cancellation was successful, False otherwise
        """
        pass

    @abstractmethod
    def get_order_status(self, order_id: str) -> OrderStatus:
        """
        Get current order status.

        Args:
            order_id: Order ID

        Returns:
            OrderStatus enum value
        """
        pass

    @abstractmethod
    def get_positions(self) -> Dict[str, float]:
        """
        Get current account positions.

        Returns:
            Dict mapping symbol to quantity (positive for long, negative for short)
        """
        pass

    @abstractmethod
    def get_balance(self) -> float:
        """
        Get current account balance/cash.

        Returns:
            Available cash balance in account currency
        """
        pass

    @abstractmethod
    def reconcile_positions(
        self, internal_positions: Dict[str, float]
    ) -> List[Dict[str, Any]]:
        """
        Reconcile internal positions with broker positions.

        Args:
            internal_positions: Dict of symbol -> quantity from internal system

        Returns:
            List of discrepancies: [{"symbol": "PETR4", "internal": 100, "broker": 95, "diff": -5}]
        """
        pass

    @abstractmethod
    def reconcile_orders(
        self, internal_orders: List[OrderRecord]
    ) -> List[Dict[str, Any]]:
        """
        Reconcile internal orders with broker orders.

        Args:
            internal_orders: List of OrderRecord from internal system

        Returns:
            List of discrepancies: [{"order_id": "...", "internal_status": "filled", "broker_status": "pending"}]
        """
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if broker connection is active.

        Returns:
            True if connected and ready, False otherwise
        """
        pass


# ============================================================================
# PAPER BROKER (SIMULATION)
# ============================================================================

class PaperBroker(AbstractBroker):
    """
    Institutional Execution Simulator - Enhanced PaperBroker.

    Implements realistic execution simulation with:
    - Asset class & regime-based slippage
    - Market-specific fees
    - Order rejection logic
    - Variable latency simulation
    - Realistic partial fills with price impact
    - Per-symbol liquidity limits
    - Execution statistics tracking
    """

    def __init__(
        self,
        asset_class: AssetClass,
        initial_balance: float = 100000.0,
        slippage_bps: float = 5.0,
        fill_rate: float = 0.99,
        partial_fill_prob: float = 0.05,
        market_regime: str = "normal",
        enable_smart_rejections: bool = True,
        enable_latency_spikes: bool = True,
        rejection_rate: float = 0.01,
    ):
        """
        Initialize paper broker with institutional execution features.

        Args:
            asset_class: Asset class to simulate (STOCK_BR, STOCK_US, CRYPTO)
            initial_balance: Starting cash balance (default $100,000)
            slippage_bps: Base slippage in basis points (default 5 bps)
            fill_rate: Probability of fill (default 0.99 = 99%)
            partial_fill_prob: Probability of partial fill (default 0.05 = 5%)
            market_regime: Market regime ('normal', 'volatile', 'low_liquidity')
                          multiplies slippage by regime factor
            enable_smart_rejections: Enable rejection logic based on liquidity (default True)
            enable_latency_spikes: Enable random latency spikes (default True)
            rejection_rate: Base rejection rate (0.01 = 1%) before smart logic (default 0.01)
        """
        super().__init__(asset_class)
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.slippage_bps = slippage_bps / 10000.0  # Convert to decimal
        self.fill_rate = fill_rate
        self.partial_fill_prob = partial_fill_prob
        self.market_regime = market_regime
        self.enable_smart_rejections = enable_smart_rejections
        self.enable_latency_spikes = enable_latency_spikes
        self.rejection_rate = rejection_rate

        self._positions: Dict[str, float] = {}
        self._orders: Dict[str, OrderRecord] = {}
        self._connected = True

        # Execution statistics
        self._stats = {
            "total_orders": 0,
            "filled_orders": 0,
            "partial_fill_orders": 0,
            "rejected_orders": 0,
            "rejected_by_liquidity": 0,
            "rejected_by_price_anomaly": 0,
            "total_fees": 0.0,
            "total_slippage": 0.0,
            "latency_spikes": 0,
        }

        # Per-symbol liquidity tracking
        self._symbol_liquidity_limits: Dict[str, float] = {}
        self._symbol_notional_today: Dict[str, float] = {}

        logger.info(
            f"PaperBroker (Institutional Execution Simulator) initialized for {asset_class.value} "
            f"with ${initial_balance} balance, {slippage_bps}bps slippage, "
            f"regime={market_regime}, rejections={enable_smart_rejections}"
        )

    def _get_slippage_profile(self) -> Dict[str, Any]:
        """Get slippage profile for asset class, hour, and regime."""
        import random
        from datetime import datetime

        now = datetime.utcnow()
        hour = now.hour

        # Base slippage ranges by asset class
        profiles = {
            AssetClass.STOCK_BR: {
                "normal": {"min": 3, "max": 8, "auction_min": 15, "auction_max": 30},
                "volatile": {"min": 8, "max": 15, "auction_min": 30, "auction_max": 50},
                "low_liquidity": {"min": 15, "max": 25, "auction_min": 50, "auction_max": 100},
            },
            AssetClass.STOCK_US: {
                "normal": {"min": 2, "max": 5, "prepost_min": 10, "prepost_max": 20},
                "volatile": {"min": 5, "max": 12, "prepost_min": 20, "prepost_max": 40},
                "low_liquidity": {"min": 12, "max": 25, "prepost_min": 40, "prepost_max": 80},
            },
            AssetClass.CRYPTO: {
                "normal": {"min": 5, "max": 15, "low_liq_min": 15, "low_liq_max": 50},
                "volatile": {"min": 15, "max": 40, "low_liq_min": 40, "low_liq_max": 100},
                "low_liquidity": {"min": 40, "max": 100, "low_liq_min": 100, "low_liq_max": 200},
            },
        }

        profile = profiles.get(self.asset_class, profiles[AssetClass.STOCK_BR])
        regime_profile = profile.get(self.market_regime, profile["normal"])

        # Determine if it's low liquidity hours or auction
        is_br_auction = hour in [16, 17, 18]  # B3 auction hours
        is_us_prepost = hour in [0, 1, 2, 3, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]  # Pre/post
        is_crypto_low = hour in [2, 3, 4, 5, 6]  # Low liquidity UTC hours

        if self.asset_class == AssetClass.STOCK_BR and is_br_auction:
            return {
                "min_bps": regime_profile.get("auction_min", 15),
                "max_bps": regime_profile.get("auction_max", 30),
            }
        elif self.asset_class == AssetClass.STOCK_US and is_us_prepost:
            return {
                "min_bps": regime_profile.get("prepost_min", 10),
                "max_bps": regime_profile.get("prepost_max", 20),
            }
        elif self.asset_class == AssetClass.CRYPTO and is_crypto_low:
            return {
                "min_bps": regime_profile.get("low_liq_min", 15),
                "max_bps": regime_profile.get("low_liq_max", 50),
            }
        else:
            # Normal hours
            return {
                "min_bps": regime_profile.get("min", 3),
                "max_bps": regime_profile.get("max", 8),
            }

    def _calculate_fee(self, symbol: str, quantity: float, price: float) -> float:
        """Calculate market-specific fees."""
        notional = quantity * price

        if self.asset_class == AssetClass.STOCK_BR:
            # B3: emoluments 0.0325% + B3 fee 0.005% + broker 0.03% = ~0.0675%
            return notional * 0.000675

        elif self.asset_class == AssetClass.STOCK_US:
            # $0.005 per share, minimum $1.00
            return max(1.0, quantity * 0.005)

        elif self.asset_class == AssetClass.CRYPTO:
            # 0.1% taker fee (assuming market orders are taker)
            return notional * 0.001

        return notional * 0.001  # Default fallback

    def _get_liquidity_limit(self, symbol: str) -> float:
        """Get per-symbol max order size limit."""
        if symbol in self._symbol_liquidity_limits:
            return self._symbol_liquidity_limits[symbol]

        # Default limits by asset class
        default_limits = {
            AssetClass.STOCK_BR: 500000.0,    # $500K per order
            AssetClass.STOCK_US: 1000000.0,   # $1M per order
            AssetClass.CRYPTO: 200000.0,      # $200K per order
        }
        return default_limits.get(self.asset_class, 500000.0)

    def _get_reference_price(self, symbol: str, decision_price: float) -> float:
        """Get reference price for anomaly detection (simulated)."""
        # In a real system, this would fetch from market data
        # For now, use decision_price as reference
        return decision_price

    def _should_reject_order(self, order: OrderRecord, slippage_bps: float) -> Tuple[bool, str]:
        """Smart rejection logic based on liquidity and price anomalies."""
        if not self.enable_smart_rejections:
            return False, ""

        import random

        # Base rejection by configured rate
        if random.random() < self.rejection_rate:
            return True, "random_rejection"

        # Reject if notional exceeds liquidity limit
        notional = order.quantity * order.decision_price
        limit = self._get_liquidity_limit(order.symbol)
        if notional > limit:
            self._stats["rejected_by_liquidity"] += 1
            return True, "exceeds_liquidity_limit"

        # Reject if price anomaly detected (>5% from reference)
        ref_price = self._get_reference_price(order.symbol, order.decision_price)
        price_diff_pct = abs(order.decision_price - ref_price) / ref_price * 100
        if price_diff_pct > 5.0:
            self._stats["rejected_by_price_anomaly"] += 1
            return True, "price_anomaly_detected"

        return False, ""

    def _get_variable_latency(self) -> float:
        """Calculate realistic variable latency in seconds."""
        import random

        base_latencies = {
            AssetClass.STOCK_BR: (0.05, 0.2),      # 50-200ms normal
            AssetClass.STOCK_US: (0.01, 0.05),     # 10-50ms
            AssetClass.CRYPTO: (0.02, 0.1),        # 20-100ms
        }

        base_range = base_latencies.get(self.asset_class, (0.05, 0.2))
        latency = random.uniform(base_range[0], base_range[1])

        # Random latency spike (1-5s at 2% probability)
        if self.enable_latency_spikes and random.random() < 0.02:
            latency += random.uniform(1.0, 5.0)
            self._stats["latency_spikes"] += 1

        return latency

    def _get_partial_fill_rate(self, notional: float) -> float:
        """Determine partial fill rate based on order size."""
        # Large orders (>$50K notional) get 30% partial fill rate
        # Small orders get 5% partial fill rate
        if notional > 50000.0:
            return 0.30
        else:
            return 0.05

    def submit_order(self, order: OrderRecord) -> OrderRecord:
        """
        Submit and execute order with institutional execution simulation.

        Implements:
        - Asset class/regime-based slippage
        - Market-specific fees
        - Smart rejection logic
        - Variable latency
        - Realistic partial fills with price impact
        - Liquidity limits

        Args:
            order: OrderRecord to submit

        Returns:
            Updated OrderRecord with execution details
        """
        with self._lock:
            if not self._connected:
                order.status = OrderStatus.ERROR
                logger.error(f"PaperBroker not connected, rejecting order {order.order_id}")
                return order

            import random

            self._stats["total_orders"] += 1
            send_time = time.time()
            order.sent_price = order.decision_price

            # Step 1: Basic fill rate check
            if random.random() > self.fill_rate:
                order.status = OrderStatus.REJECTED
                self._stats["rejected_orders"] += 1
                logger.warning(f"Order {order.order_id} rejected (fill_rate)")
                return order

            # Step 2: Get regime-aware slippage profile
            slippage_profile = self._get_slippage_profile()
            slippage_bps = random.uniform(
                slippage_profile["min_bps"],
                slippage_profile["max_bps"]
            ) / 10000.0  # Convert to decimal

            # Step 3: Smart rejection logic
            should_reject, reason = self._should_reject_order(order, slippage_bps * 10000)
            if should_reject:
                order.status = OrderStatus.REJECTED
                self._stats["rejected_orders"] += 1
                logger.warning(f"Order {order.order_id} rejected ({reason})")
                return order

            # Step 4: Calculate slippage with regime multiplier
            regime_multipliers = {"normal": 1.0, "volatile": 1.5, "low_liquidity": 2.0}
            regime_mult = regime_multipliers.get(self.market_regime, 1.0)
            slippage_amount = order.decision_price * slippage_bps * regime_mult

            if order.side == OrderSide.BUY:
                executed_price = order.decision_price + slippage_amount
            else:
                executed_price = order.decision_price - slippage_amount

            # Step 5: Get variable latency
            base_latency = self._get_variable_latency()

            # Step 6: Calculate fees
            fee_per_unit = self._calculate_fee(order.symbol, order.quantity, executed_price)

            # Step 7: Determine partial fill rate and create fills
            fills: List[FillRecord] = []
            notional = order.quantity * executed_price
            partial_fill_rate = self._get_partial_fill_rate(notional)

            qty_remaining = order.quantity

            if random.random() < partial_fill_rate and qty_remaining > 1:
                # Partial fill with price impact
                partial_qty = max(1, int(qty_remaining * random.uniform(0.3, 0.7)))
                # First partial gets slightly better price (volatility)
                partial_price_1 = executed_price + (random.uniform(-0.001, 0.001) * executed_price)

                fill_ts = send_time + base_latency + random.uniform(0.01, 0.1)
                fills.append(FillRecord(
                    qty=partial_qty,
                    price=partial_price_1,
                    ts=fill_ts,
                    fee=self._calculate_fee(order.symbol, partial_qty, partial_price_1)
                ))
                qty_remaining -= partial_qty

                # Second partial at different price
                partial_price_2 = executed_price + (random.uniform(-0.002, 0.002) * executed_price)
                fill_ts = send_time + base_latency + random.uniform(0.1, 0.5)
                fills.append(FillRecord(
                    qty=qty_remaining,
                    price=partial_price_2,
                    ts=fill_ts,
                    fee=self._calculate_fee(order.symbol, qty_remaining, partial_price_2)
                ))
                order.status = OrderStatus.PARTIAL_FILL
                self._stats["partial_fill_orders"] += 1
            else:
                # Full fill
                fill_ts = send_time + base_latency + random.uniform(0.01, 0.2)
                fills.append(FillRecord(
                    qty=qty_remaining,
                    price=executed_price,
                    ts=fill_ts,
                    fee=fee_per_unit
                ))
                order.status = OrderStatus.FILLED
                self._stats["filled_orders"] += 1

            order.fills = fills
            order.executed_price = executed_price
            order.average_price = order.calculate_average_fill_price()
            order.slippage = order.calculate_slippage()
            order.latency_ms = (fills[0].ts - send_time) * 1000
            order.fees_actual = sum(f.fee for f in fills)
            order.updated_at = time.time()

            # Update stats
            self._stats["total_fees"] += order.fees_actual
            self._stats["total_slippage"] += abs(order.slippage)

            # Update positions and balance
            symbol = order.symbol
            qty_change = order.quantity if order.side == OrderSide.BUY else -order.quantity
            self._positions[symbol] = self._positions.get(symbol, 0.0) + qty_change
            self.balance -= order.average_price * order.quantity + order.fees_actual

            # Track daily notional by symbol
            self._symbol_notional_today[symbol] = \
                self._symbol_notional_today.get(symbol, 0.0) + notional

            self._orders[order.order_id] = order

            logger.info(
                f"PaperBroker executed order {order.order_id} "
                f"{order.side.value} {order.quantity} {symbol} @ {executed_price:.6f} "
                f"(slippage: {order.slippage:.6f}, fee: {order.fees_actual:.2f}, latency: {order.latency_ms:.1f}ms)"
            )

            return order

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order (if still PENDING)."""
        with self._lock:
            order = self._orders.get(order_id)
            if not order:
                logger.warning(f"Order {order_id} not found")
                return False

            if order.status not in [OrderStatus.PENDING, OrderStatus.SENT]:
                logger.warning(f"Cannot cancel order {order_id}, status={order.status.value}")
                return False

            order.status = OrderStatus.CANCELLED
            order.updated_at = time.time()
            logger.info(f"Order {order_id} cancelled")
            return True

    def get_order_status(self, order_id: str) -> OrderStatus:
        """Get order status."""
        with self._lock:
            order = self._orders.get(order_id)
            return order.status if order else OrderStatus.ERROR

    def get_positions(self) -> Dict[str, float]:
        """Get current positions."""
        with self._lock:
            return dict(self._positions)

    def get_balance(self) -> float:
        """Get current balance."""
        with self._lock:
            return self.balance

    def reconcile_positions(
        self, internal_positions: Dict[str, float]
    ) -> List[Dict[str, Any]]:
        """Check for position discrepancies."""
        with self._lock:
            differences = []
            all_symbols = set(internal_positions.keys()) | set(self._positions.keys())

            for symbol in all_symbols:
                internal_qty = internal_positions.get(symbol, 0.0)
                broker_qty = self._positions.get(symbol, 0.0)

                if internal_qty != broker_qty:
                    differences.append({
                        "symbol": symbol,
                        "internal": internal_qty,
                        "broker": broker_qty,
                        "diff": broker_qty - internal_qty,
                    })

            return differences

    def reconcile_orders(
        self, internal_orders: List[OrderRecord]
    ) -> List[Dict[str, Any]]:
        """Check for order discrepancies."""
        with self._lock:
            differences = []

            for internal_order in internal_orders:
                broker_order = self._orders.get(internal_order.order_id)
                if not broker_order:
                    differences.append({
                        "order_id": internal_order.order_id,
                        "internal_status": internal_order.status.value,
                        "broker_status": "NOT_FOUND",
                    })
                elif broker_order.status != internal_order.status:
                    differences.append({
                        "order_id": internal_order.order_id,
                        "internal_status": internal_order.status.value,
                        "broker_status": broker_order.status.value,
                    })

            return differences

    def is_connected(self) -> bool:
        """Always connected in paper trading."""
        return self._connected

    def set_liquidity_limit(self, symbol: str, limit: float) -> None:
        """
        Set custom liquidity limit for a symbol.

        Args:
            symbol: Trading symbol
            limit: Max notional value per order
        """
        with self._lock:
            self._symbol_liquidity_limits[symbol] = limit
            logger.info(f"Liquidity limit set for {symbol}: ${limit:.2f}")

    def set_market_regime(self, regime: str) -> None:
        """
        Set market regime for slippage multiplier.

        Args:
            regime: 'normal', 'volatile', or 'low_liquidity'
        """
        if regime not in ("normal", "volatile", "low_liquidity"):
            logger.warning(f"Invalid regime {regime}, keeping current {self.market_regime}")
            return

        with self._lock:
            self.market_regime = regime
            logger.info(f"Market regime set to {regime}")

    def get_execution_profile(self) -> Dict[str, Any]:
        """
        Get current execution characteristics and statistics.

        Returns:
            Dict with execution profile, configuration, and statistics
        """
        with self._lock:
            slippage_prof = self._get_slippage_profile()

            return {
                "asset_class": self.asset_class.value,
                "market_regime": self.market_regime,
                "connected": self._connected,
                "configuration": {
                    "initial_balance": self.initial_balance,
                    "current_balance": self.balance,
                    "slippage_bps_base": self.slippage_bps * 10000,
                    "fill_rate": self.fill_rate,
                    "partial_fill_prob": self.partial_fill_prob,
                    "enable_smart_rejections": self.enable_smart_rejections,
                    "enable_latency_spikes": self.enable_latency_spikes,
                    "rejection_rate": self.rejection_rate,
                },
                "current_slippage_profile": {
                    "min_bps": slippage_prof["min_bps"],
                    "max_bps": slippage_prof["max_bps"],
                },
                "statistics": {
                    "total_orders": self._stats["total_orders"],
                    "filled_orders": self._stats["filled_orders"],
                    "partial_fill_orders": self._stats["partial_fill_orders"],
                    "rejected_orders": self._stats["rejected_orders"],
                    "rejected_by_liquidity": self._stats["rejected_by_liquidity"],
                    "rejected_by_price_anomaly": self._stats["rejected_by_price_anomaly"],
                    "total_fees": self._stats["total_fees"],
                    "total_slippage": self._stats["total_slippage"],
                    "latency_spikes": self._stats["latency_spikes"],
                    "fill_rate_achieved": (
                        (self._stats["filled_orders"] + self._stats["partial_fill_orders"]) /
                        self._stats["total_orders"]
                        if self._stats["total_orders"] > 0 else 0.0
                    ),
                    "partial_fill_rate_achieved": (
                        self._stats["partial_fill_orders"] / self._stats["total_orders"]
                        if self._stats["total_orders"] > 0 else 0.0
                    ),
                },
                "positions": dict(self._positions),
                "symbol_liquidity_limits": dict(self._symbol_liquidity_limits),
                "symbol_notional_today": dict(self._symbol_notional_today),
            }

    def reset_statistics(self) -> None:
        """Reset execution statistics."""
        with self._lock:
            self._stats = {
                "total_orders": 0,
                "filled_orders": 0,
                "partial_fill_orders": 0,
                "rejected_orders": 0,
                "rejected_by_liquidity": 0,
                "rejected_by_price_anomaly": 0,
                "total_fees": 0.0,
                "total_slippage": 0.0,
                "latency_spikes": 0,
            }
            self._symbol_notional_today.clear()
            logger.info("Execution statistics reset")


# ============================================================================
# STUB BROKERS (NOT YET IMPLEMENTED)
# ============================================================================

class BTGBroker(AbstractBroker):
    """
    B3/BTG Pactual broker integration (STUB).

    Requires BTG_API_KEY, BTG_API_SECRET, BTG_ACCOUNT_ID, BTG_ENVIRONMENT
    environment variables. All methods raise NotImplementedError until configured.
    """

    def __init__(self):
        """Initialize BTG broker from environment config."""
        super().__init__(AssetClass.STOCK_BR)
        self.api_key = os.getenv("BTG_API_KEY")
        self.api_secret = os.getenv("BTG_API_SECRET")
        self.account_id = os.getenv("BTG_ACCOUNT_ID")
        self.environment = os.getenv("BTG_ENVIRONMENT", "sandbox")
        logger.info(f"BTGBroker stub initialized for {self.environment} environment")

    def has_credentials(self) -> bool:
        """Check if BTG credentials are configured."""
        return bool(self.api_key and self.api_secret and self.account_id)

    def submit_order(self, order: OrderRecord) -> OrderRecord:
        """BTG integration pending."""
        raise NotImplementedError(
            "BTG integration pending — configure BTG_API_KEY, BTG_API_SECRET, BTG_ACCOUNT_ID"
        )

    def cancel_order(self, order_id: str) -> bool:
        """BTG integration pending."""
        raise NotImplementedError(
            "BTG integration pending — configure BTG_API_KEY, BTG_API_SECRET, BTG_ACCOUNT_ID"
        )

    def get_order_status(self, order_id: str) -> OrderStatus:
        """BTG integration pending."""
        raise NotImplementedError(
            "BTG integration pending — configure BTG_API_KEY, BTG_API_SECRET, BTG_ACCOUNT_ID"
        )

    def get_positions(self) -> Dict[str, float]:
        """BTG integration pending."""
        raise NotImplementedError(
            "BTG integration pending — configure BTG_API_KEY, BTG_API_SECRET, BTG_ACCOUNT_ID"
        )

    def get_balance(self) -> float:
        """BTG integration pending."""
        raise NotImplementedError(
            "BTG integration pending — configure BTG_API_KEY, BTG_API_SECRET, BTG_ACCOUNT_ID"
        )

    def reconcile_positions(
        self, internal_positions: Dict[str, float]
    ) -> List[Dict[str, Any]]:
        """BTG integration pending."""
        raise NotImplementedError(
            "BTG integration pending — configure BTG_API_KEY, BTG_API_SECRET, BTG_ACCOUNT_ID"
        )

    def reconcile_orders(
        self, internal_orders: List[OrderRecord]
    ) -> List[Dict[str, Any]]:
        """BTG integration pending."""
        raise NotImplementedError(
            "BTG integration pending — configure BTG_API_KEY, BTG_API_SECRET, BTG_ACCOUNT_ID"
        )

    def is_connected(self) -> bool:
        """BTG integration pending."""
        raise NotImplementedError(
            "BTG integration pending — configure BTG_API_KEY, BTG_API_SECRET, BTG_ACCOUNT_ID"
        )


class BinanceBroker(AbstractBroker):
    """
    Binance crypto broker integration (STUB).

    Requires BINANCE_API_KEY, BINANCE_API_SECRET, BINANCE_ENVIRONMENT
    environment variables. All methods raise NotImplementedError until configured.
    """

    def __init__(self):
        """Initialize Binance broker from environment config."""
        super().__init__(AssetClass.CRYPTO)
        self.api_key = os.getenv("BINANCE_API_KEY")
        self.api_secret = os.getenv("BINANCE_API_SECRET")
        self.environment = os.getenv("BINANCE_ENVIRONMENT", "testnet")
        logger.info(f"BinanceBroker stub initialized for {self.environment} environment")

    def has_credentials(self) -> bool:
        """Check if Binance credentials are configured."""
        return bool(self.api_key and self.api_secret)

    def submit_order(self, order: OrderRecord) -> OrderRecord:
        """Binance integration pending."""
        raise NotImplementedError(
            "Binance integration pending — configure BINANCE_API_KEY, BINANCE_API_SECRET"
        )

    def cancel_order(self, order_id: str) -> bool:
        """Binance integration pending."""
        raise NotImplementedError(
            "Binance integration pending — configure BINANCE_API_KEY, BINANCE_API_SECRET"
        )

    def get_order_status(self, order_id: str) -> OrderStatus:
        """Binance integration pending."""
        raise NotImplementedError(
            "Binance integration pending — configure BINANCE_API_KEY, BINANCE_API_SECRET"
        )

    def get_positions(self) -> Dict[str, float]:
        """Binance integration pending."""
        raise NotImplementedError(
            "Binance integration pending — configure BINANCE_API_KEY, BINANCE_API_SECRET"
        )

    def get_balance(self) -> float:
        """Binance integration pending."""
        raise NotImplementedError(
            "Binance integration pending — configure BINANCE_API_KEY, BINANCE_API_SECRET"
        )

    def reconcile_positions(
        self, internal_positions: Dict[str, float]
    ) -> List[Dict[str, Any]]:
        """Binance integration pending."""
        raise NotImplementedError(
            "Binance integration pending — configure BINANCE_API_KEY, BINANCE_API_SECRET"
        )

    def reconcile_orders(
        self, internal_orders: List[OrderRecord]
    ) -> List[Dict[str, Any]]:
        """Binance integration pending."""
        raise NotImplementedError(
            "Binance integration pending — configure BINANCE_API_KEY, BINANCE_API_SECRET"
        )

    def is_connected(self) -> bool:
        """Binance integration pending."""
        raise NotImplementedError(
            "Binance integration pending — configure BINANCE_API_KEY, BINANCE_API_SECRET"
        )


class NYSEBroker(AbstractBroker):
    """
    NYSE/NASDAQ broker integration (STUB).

    Requires NYSE_BROKER_API_KEY, NYSE_BROKER_API_SECRET, NYSE_BROKER_NAME
    environment variables. All methods raise NotImplementedError until configured.
    """

    def __init__(self):
        """Initialize NYSE broker from environment config."""
        super().__init__(AssetClass.STOCK_US)
        self.api_key = os.getenv("NYSE_BROKER_API_KEY")
        self.api_secret = os.getenv("NYSE_BROKER_API_SECRET")
        self.broker_name = os.getenv("NYSE_BROKER_NAME", "unknown")
        logger.info(f"NYSEBroker stub initialized for {self.broker_name}")

    def has_credentials(self) -> bool:
        """Check if NYSE credentials are configured."""
        return bool(self.api_key and self.api_secret)

    def submit_order(self, order: OrderRecord) -> OrderRecord:
        """NYSE integration pending."""
        raise NotImplementedError(
            "NYSE integration pending — configure NYSE_BROKER_API_KEY, NYSE_BROKER_API_SECRET"
        )

    def cancel_order(self, order_id: str) -> bool:
        """NYSE integration pending."""
        raise NotImplementedError(
            "NYSE integration pending — configure NYSE_BROKER_API_KEY, NYSE_BROKER_API_SECRET"
        )

    def get_order_status(self, order_id: str) -> OrderStatus:
        """NYSE integration pending."""
        raise NotImplementedError(
            "NYSE integration pending — configure NYSE_BROKER_API_KEY, NYSE_BROKER_API_SECRET"
        )

    def get_positions(self) -> Dict[str, float]:
        """NYSE integration pending."""
        raise NotImplementedError(
            "NYSE integration pending — configure NYSE_BROKER_API_KEY, NYSE_BROKER_API_SECRET"
        )

    def get_balance(self) -> float:
        """NYSE integration pending."""
        raise NotImplementedError(
            "NYSE integration pending — configure NYSE_BROKER_API_KEY, NYSE_BROKER_API_SECRET"
        )

    def reconcile_positions(
        self, internal_positions: Dict[str, float]
    ) -> List[Dict[str, Any]]:
        """NYSE integration pending."""
        raise NotImplementedError(
            "NYSE integration pending — configure NYSE_BROKER_API_KEY, NYSE_BROKER_API_SECRET"
        )

    def reconcile_orders(
        self, internal_orders: List[OrderRecord]
    ) -> List[Dict[str, Any]]:
        """NYSE integration pending."""
        raise NotImplementedError(
            "NYSE integration pending — configure NYSE_BROKER_API_KEY, NYSE_BROKER_API_SECRET"
        )

    def is_connected(self) -> bool:
        """NYSE integration pending."""
        raise NotImplementedError(
            "NYSE integration pending — configure NYSE_BROKER_API_KEY, NYSE_BROKER_API_SECRET"
        )


# ============================================================================
# ORDER TRACKER (THREAD-SAFE)
# ============================================================================

class OrderTracker:
    """
    Thread-safe order tracking with idempotency checking,
    statistics, and reconciliation support.
    """

    def __init__(self):
        """Initialize order tracker."""
        self._orders: Dict[str, OrderRecord] = {}
        self._idempotency_map: Dict[str, str] = {}  # idempotency_key -> order_id
        self._lock = RLock()
        logger.info("OrderTracker initialized")

    def add_order(self, order: OrderRecord) -> None:
        """
        Add order to tracker.

        Args:
            order: OrderRecord to add
        """
        with self._lock:
            self._orders[order.order_id] = order
            if order.idempotency_key:
                self._idempotency_map[order.idempotency_key] = order.order_id
            logger.debug(f"Order {order.order_id} added to tracker")

    def update_order(self, order_id: str, updates: Dict[str, Any]) -> None:
        """
        Update order with new values.

        Args:
            order_id: Order ID to update
            updates: Dict of field -> value to update
        """
        with self._lock:
            order = self._orders.get(order_id)
            if not order:
                logger.warning(f"Order {order_id} not found for update")
                return

            for key, value in updates.items():
                if hasattr(order, key):
                    setattr(order, key, value)

            order.updated_at = time.time()
            logger.debug(f"Order {order_id} updated: {updates}")

    def get_order(self, order_id: str) -> Optional[OrderRecord]:
        """Get order by ID."""
        with self._lock:
            return self._orders.get(order_id)

    def get_open_orders(self) -> List[OrderRecord]:
        """Get all non-terminal orders."""
        with self._lock:
            open_statuses = {
                OrderStatus.PENDING,
                OrderStatus.SENT,
                OrderStatus.PARTIAL_FILL,
            }
            return [
                order
                for order in self._orders.values()
                if order.status in open_statuses
            ]

    def get_orders_by_trade(self, trade_id: str) -> List[OrderRecord]:
        """Get all orders for a specific trade."""
        with self._lock:
            return [
                order
                for order in self._orders.values()
                if order.trade_id == trade_id
            ]

    def check_idempotency(self, idempotency_key: str) -> Optional[OrderRecord]:
        """
        Check if order with this idempotency key already exists.

        Args:
            idempotency_key: Idempotency key to check

        Returns:
            OrderRecord if exists, None otherwise
        """
        with self._lock:
            order_id = self._idempotency_map.get(idempotency_key)
            return self._orders.get(order_id) if order_id else None

    def get_slippage_stats(self) -> Dict[str, Any]:
        """
        Get slippage statistics by asset class.

        Returns:
            Dict with avg/min/max slippage per asset class
        """
        with self._lock:
            filled_orders = [
                order
                for order in self._orders.values()
                if order.status == OrderStatus.FILLED and order.slippage is not None
            ]

            if not filled_orders:
                return {}

            by_asset = {}
            for asset_class in AssetClass:
                slippages = [
                    order.slippage
                    for order in filled_orders
                    if order.asset_class == asset_class
                ]

                if slippages:
                    by_asset[asset_class.value] = {
                        "count": len(slippages),
                        "avg": mean(slippages),
                        "min": min(slippages),
                        "max": max(slippages),
                        "stdev": stdev(slippages) if len(slippages) > 1 else 0.0,
                    }

            return by_asset

    def get_latency_stats(self) -> Dict[str, Any]:
        """
        Get latency statistics by asset class.

        Returns:
            Dict with avg/min/max latency per asset class in ms
        """
        with self._lock:
            filled_orders = [
                order
                for order in self._orders.values()
                if order.status in [OrderStatus.FILLED, OrderStatus.PARTIAL_FILL]
                and order.latency_ms is not None
            ]

            if not filled_orders:
                return {}

            by_asset = {}
            for asset_class in AssetClass:
                latencies = [
                    order.latency_ms
                    for order in filled_orders
                    if order.asset_class == asset_class
                ]

                if latencies:
                    by_asset[asset_class.value] = {
                        "count": len(latencies),
                        "avg_ms": mean(latencies),
                        "min_ms": min(latencies),
                        "max_ms": max(latencies),
                        "stdev_ms": stdev(latencies) if len(latencies) > 1 else 0.0,
                    }

            return by_asset

    def get_fill_stats(self) -> Dict[str, Any]:
        """
        Get fill statistics (fill rates, partial fill rates).

        Returns:
            Dict with fill stats per asset class
        """
        with self._lock:
            all_orders = self._orders.values()

            stats = {}
            for asset_class in AssetClass:
                asset_orders = [
                    o for o in all_orders
                    if o.asset_class == asset_class
                ]

                if asset_orders:
                    filled = len([
                        o for o in asset_orders
                        if o.status == OrderStatus.FILLED
                    ])
                    partial = len([
                        o for o in asset_orders
                        if o.status == OrderStatus.PARTIAL_FILL
                    ])
                    rejected = len([
                        o for o in asset_orders
                        if o.status == OrderStatus.REJECTED
                    ])
                    total = len(asset_orders)

                    stats[asset_class.value] = {
                        "total": total,
                        "filled": filled,
                        "partial_fill": partial,
                        "rejected": rejected,
                        "fill_rate": (filled + partial) / total if total > 0 else 0.0,
                        "partial_fill_rate": partial / total if total > 0 else 0.0,
                    }

            return stats

    def get_reconciliation_status(self) -> Dict[str, Any]:
        """
        Get reconciliation status for /ops endpoint.

        Returns:
            Dict with order tracking health metrics
        """
        with self._lock:
            all_orders = list(self._orders.values())
            open_orders = self.get_open_orders()
            fill_stats = self.get_fill_stats()
            slippage_stats = self.get_slippage_stats()
            latency_stats = self.get_latency_stats()

            return {
                "total_orders": len(all_orders),
                "open_orders": len(open_orders),
                "fill_stats": fill_stats,
                "slippage_stats": slippage_stats,
                "latency_stats": latency_stats,
                "last_updated": datetime.utcnow().isoformat(),
            }


# ============================================================================
# BROKER FACTORY
# ============================================================================

class BrokerFactory:
    """
    Factory for creating and managing broker instances.

    Returns appropriate broker based on asset class and configuration.
    Falls back to PaperBroker if no credentials configured.
    """

    _brokers: Dict[AssetClass, AbstractBroker] = {}
    _lock = RLock()

    @classmethod
    def get_broker(cls, asset_class: AssetClass) -> AbstractBroker:
        """
        Get or create broker for asset class.

        Args:
            asset_class: Asset class to get broker for

        Returns:
            AbstractBroker implementation (real or paper)
        """
        with cls._lock:
            if asset_class in cls._brokers:
                return cls._brokers[asset_class]

            broker = cls._create_broker(asset_class)
            cls._brokers[asset_class] = broker
            return broker

    @classmethod
    def _create_broker(cls, asset_class: AssetClass) -> AbstractBroker:
        """
        Create appropriate broker for asset class.

        Returns PaperBroker if no credentials, otherwise instantiates
        real broker stub.
        """
        if asset_class == AssetClass.STOCK_BR:
            btg = BTGBroker()
            if btg.has_credentials():
                logger.info("Using BTGBroker for STOCK_BR")
                return btg
            else:
                logger.info("No BTG credentials, using PaperBroker for STOCK_BR")
                return PaperBroker(asset_class)

        elif asset_class == AssetClass.STOCK_US:
            nyse = NYSEBroker()
            if nyse.has_credentials():
                logger.info("Using NYSEBroker for STOCK_US")
                return nyse
            else:
                logger.info("No NYSE credentials, using PaperBroker for STOCK_US")
                return PaperBroker(asset_class)

        elif asset_class == AssetClass.CRYPTO:
            binance = BinanceBroker()
            if binance.has_credentials():
                logger.info("Using BinanceBroker for CRYPTO")
                return binance
            else:
                logger.info("No Binance credentials, using PaperBroker for CRYPTO")
                return PaperBroker(asset_class)

        else:
            logger.warning(f"Unknown asset class {asset_class}, defaulting to Paper")
            return PaperBroker(asset_class)

    @classmethod
    def reset(cls) -> None:
        """
        Reset all broker instances.

        Useful for testing.
        """
        with cls._lock:
            cls._brokers.clear()
            logger.info("BrokerFactory reset")


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def create_order_record(
    trade_id: str,
    symbol: str,
    side: OrderSide,
    order_type: OrderType,
    asset_class: AssetClass,
    quantity: float,
    decision_price: float,
    idempotency_key: Optional[str] = None,
) -> OrderRecord:
    """
    Factory function to create a new OrderRecord.

    Args:
        trade_id: Trade identifier from trading system
        symbol: Trading symbol
        side: BUY or SELL
        order_type: MARKET, LIMIT, STOP, STOP_LIMIT
        asset_class: STOCK_BR, STOCK_US, CRYPTO
        quantity: Order quantity
        decision_price: Price at decision time
        idempotency_key: Optional key for idempotency checking

    Returns:
        New OrderRecord with UUID and timestamps
    """
    order_id = str(uuid.uuid4())
    now = time.time()

    return OrderRecord(
        order_id=order_id,
        trade_id=trade_id,
        symbol=symbol,
        side=side,
        order_type=order_type,
        asset_class=asset_class,
        quantity=quantity,
        decision_price=decision_price,
        created_at=now,
        updated_at=now,
        idempotency_key=idempotency_key,
    )


__all__ = [
    "OrderStatus",
    "OrderSide",
    "OrderType",
    "AssetClass",
    "FillRecord",
    "OrderRecord",
    "AbstractBroker",
    "PaperBroker",
    "BTGBroker",
    "BinanceBroker",
    "NYSEBroker",
    "OrderTracker",
    "BrokerFactory",
    "create_order_record",
]
