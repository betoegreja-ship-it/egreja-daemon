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
    Simulated broker for paper trading and testing.

    Implements instant fills at decision_price ± slippage with configurable
    execution profile. Tracks virtual positions and cash balance.
    """

    def __init__(
        self,
        asset_class: AssetClass,
        initial_balance: float = 100000.0,
        slippage_bps: float = 5.0,
        fill_rate: float = 0.99,
        partial_fill_prob: float = 0.05,
    ):
        """
        Initialize paper broker.

        Args:
            asset_class: Asset class to simulate
            initial_balance: Starting cash balance
            slippage_bps: Slippage in basis points (default 5 bps)
            fill_rate: Probability of fill (default 0.99 = 99%)
            partial_fill_prob: Probability of partial fill (default 0.05 = 5%)
        """
        super().__init__(asset_class)
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.slippage_bps = slippage_bps / 10000.0  # Convert to decimal
        self.fill_rate = fill_rate
        self.partial_fill_prob = partial_fill_prob

        self._positions: Dict[str, float] = {}
        self._orders: Dict[str, OrderRecord] = {}
        self._connected = True

        logger.info(
            f"PaperBroker initialized for {asset_class.value} "
            f"with ${initial_balance} balance, {slippage_bps}bps slippage"
        )

    def submit_order(self, order: OrderRecord) -> OrderRecord:
        """
        Submit and immediately execute order (simulated).

        Args:
            order: OrderRecord to submit

        Returns:
            Updated OrderRecord with FILLED status and execution details
        """
        with self._lock:
            if not self._connected:
                order.status = OrderStatus.ERROR
                logger.error(f"PaperBroker not connected, rejecting order {order.order_id}")
                return order

            send_time = time.time()
            order.sent_price = order.decision_price

            # Simulate fill decision
            import random
            if random.random() > self.fill_rate:
                order.status = OrderStatus.REJECTED
                logger.warning(f"Order {order.order_id} rejected by PaperBroker (simulated)")
                return order

            # Calculate slippage
            slippage_amount = order.decision_price * self.slippage_bps
            if order.side == OrderSide.BUY:
                executed_price = order.decision_price + slippage_amount
            else:
                executed_price = order.decision_price - slippage_amount

            # Create fill(s)
            fills: List[FillRecord] = []
            qty_remaining = order.quantity

            if random.random() < self.partial_fill_prob and qty_remaining > 1:
                # Partial fill
                partial_qty = max(1, int(qty_remaining * random.uniform(0.3, 0.7)))
                fill_ts = send_time + random.uniform(0.01, 0.1)
                fills.append(FillRecord(
                    qty=partial_qty,
                    price=executed_price,
                    ts=fill_ts,
                    fee=partial_qty * executed_price * 0.001  # 0.1% fee
                ))
                qty_remaining -= partial_qty

                # Second fill
                fill_ts = send_time + random.uniform(0.1, 0.5)
                fills.append(FillRecord(
                    qty=qty_remaining,
                    price=executed_price,
                    ts=fill_ts,
                    fee=qty_remaining * executed_price * 0.001
                ))
                order.status = OrderStatus.PARTIAL_FILL
            else:
                # Full fill
                fill_ts = send_time + random.uniform(0.01, 0.2)
                fills.append(FillRecord(
                    qty=qty_remaining,
                    price=executed_price,
                    ts=fill_ts,
                    fee=qty_remaining * executed_price * 0.001
                ))
                order.status = OrderStatus.FILLED

            order.fills = fills
            order.executed_price = executed_price
            order.average_price = order.calculate_average_fill_price()
            order.slippage = order.calculate_slippage()
            order.latency_ms = (fills[0].ts - send_time) * 1000
            order.fees_actual = sum(f.fee for f in fills)
            order.updated_at = time.time()

            # Update positions and balance
            symbol = order.symbol
            qty_change = order.quantity if order.side == OrderSide.BUY else -order.quantity
            self._positions[symbol] = self._positions.get(symbol, 0.0) + qty_change
            self.balance -= order.average_price * order.quantity + order.fees_actual

            self._orders[order.order_id] = order
            logger.info(
                f"PaperBroker executed order {order.order_id} "
                f"{order.side.value} {order.quantity} {symbol} @ {executed_price:.4f}"
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
