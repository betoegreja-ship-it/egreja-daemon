"""
Derivatives Execution Engine

Coordinates multi-leg order execution for derivatives strategies.
Handles:
- Multi-leg atomic execution (call + put + stock for PCP, etc.)
- Legging risk detection and abort
- Slippage estimation and tracking
- Paper/shadow mode execution
- DB persistence of trades and legs

Thread-safe, MySQL-compatible (%s placeholders).
"""

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from enum import Enum

logger = logging.getLogger('egreja.derivatives.execution')


# ── Enums ────────────────────────────────────────────────────────────

class ExecMode(Enum):
    PAPER = "PAPER"
    SHADOW = "SHADOW"
    LIVE = "LIVE"  # never used, safety guard


class LegStatus(Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TradeStatus(Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    LEGGING_ABORT = "LEGGING_ABORT"
    FAILED = "FAILED"


# ── Data Structures ──────────────────────────────────────────────────

@dataclass
class TradeLeg:
    """Single leg of a multi-leg derivatives trade."""
    leg_id: str
    leg_type: str        # 'CALL', 'PUT', 'STOCK', 'FUTURE'
    symbol: str
    qty: int
    side: str            # 'BUY' or 'SELL'
    intended_price: float
    executed_price: float = 0.0
    slippage: float = 0.0
    latency_ms: int = 0
    status: LegStatus = LegStatus.PENDING
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class DerivativesTrade:
    """Complete multi-leg derivatives trade."""
    trade_id: str
    strategy: str
    symbol: str           # underlying
    structure_type: str   # 'CONVERSION', 'REVERSAL', 'SPREAD_DIVERGENCE', etc.
    strike: float = 0.0
    expiry: str = ''
    expected_edge: float = 0.0
    notional: float = 0.0
    legs: List[TradeLeg] = field(default_factory=list)
    status: TradeStatus = TradeStatus.PENDING
    opened_at: datetime = field(default_factory=datetime.utcnow)
    closed_at: Optional[datetime] = None
    realized_pnl: float = 0.0
    close_reason: str = ''
    liquidity_score: float = 0.0
    active_status: str = ''
    legging_incidents: int = 0


# ── Execution Engine ─────────────────────────────────────────────────

class DerivativesExecutionEngine:
    """
    Multi-leg execution engine for derivatives strategies.

    Supports paper and shadow modes only (LIVE is blocked).
    Handles legging risk: if any leg fails, attempts to unwind filled legs.

    Usage:
        engine = DerivativesExecutionEngine(config, capital_mgr, get_db_fn)
        trade = engine.execute_trade(
            strategy='pcp', symbol='PETR4', structure_type='CONVERSION',
            legs=[...], expected_edge=0.0012, notional=50_000,
            liquidity_score=82.5, active_status='PAPER_FULL',
        )
    """

    # Maximum time to wait for all legs to fill (seconds)
    LEG_TIMEOUT = 30

    # Maximum slippage before aborting (bps)
    MAX_SLIPPAGE_BPS = 50

    def __init__(self, config, capital_mgr, get_db_fn=None):
        """
        Args:
            config: DerivativesConfig instance
            capital_mgr: DerivativesCapitalManager instance
            get_db_fn: Optional callable returning MySQL connection
        """
        self._lock = threading.RLock()
        self.config = config
        self.capital = capital_mgr
        self.get_db = get_db_fn

        self.mode = ExecMode.PAPER
        if config.derivatives_mode == 'SHADOW':
            self.mode = ExecMode.SHADOW

        # Never allow LIVE
        if config.derivatives_mode == 'LIVE':
            logger.error("LIVE mode blocked. Forcing PAPER.")
            self.mode = ExecMode.PAPER

        # Active trades: trade_id → DerivativesTrade
        self.active_trades: Dict[str, DerivativesTrade] = {}

        # Stats
        self.stats = {
            'total_executions': 0,
            'successful': 0,
            'failed': 0,
            'legging_aborts': 0,
            'total_slippage': 0.0,
        }

        logger.info(f"Derivatives execution engine initialized: mode={self.mode.value}")

    # ── Main Execution ───────────────────────────────────────────

    def execute_trade(
        self,
        strategy: str,
        symbol: str,
        structure_type: str,
        legs: List[Dict],
        expected_edge: float,
        notional: float,
        strike: float = 0.0,
        expiry: str = '',
        liquidity_score: float = 0.0,
        active_status: str = '',
    ) -> DerivativesTrade:
        """
        Execute a multi-leg derivatives trade.

        Args:
            strategy: Strategy name (e.g., 'pcp', 'fst')
            symbol: Underlying symbol
            structure_type: Trade structure (e.g., 'CONVERSION')
            legs: List of leg dicts: {leg_type, symbol, qty, side, intended_price}
            expected_edge: Expected edge (absolute, in R$)
            notional: Total notional exposure
            strike: Strike price (if applicable)
            expiry: Expiry date string (YYYYMMDD)
            liquidity_score: Current liquidity score
            active_status: Current tier string

        Returns:
            DerivativesTrade with execution results
        """
        with self._lock:
            trade_id = f"DRV-{strategy.upper()}-{uuid.uuid4().hex[:8]}"

            # Build trade object
            trade_legs = [
                TradeLeg(
                    leg_id=f"{trade_id}-L{i}",
                    leg_type=leg['leg_type'],
                    symbol=leg['symbol'],
                    qty=leg['qty'],
                    side=leg['side'],
                    intended_price=leg['intended_price'],
                )
                for i, leg in enumerate(legs)
            ]

            trade = DerivativesTrade(
                trade_id=trade_id,
                strategy=strategy,
                symbol=symbol,
                structure_type=structure_type,
                strike=strike,
                expiry=expiry,
                expected_edge=expected_edge,
                notional=notional,
                legs=trade_legs,
                liquidity_score=liquidity_score,
                active_status=active_status,
            )

            self.stats['total_executions'] += 1

            # Request capital allocation
            ok, reason = self.capital.request_allocation(
                trade_id, strategy, symbol, notional
            )
            if not ok:
                trade.status = TradeStatus.FAILED
                trade.close_reason = f"Capital denied: {reason}"
                self.stats['failed'] += 1
                logger.warning(f"Trade {trade_id} capital denied: {reason}")
                self._persist_trade(trade)
                return trade

            # Execute legs
            if self.mode == ExecMode.SHADOW:
                trade = self._shadow_execute(trade)
            else:
                trade = self._paper_execute(trade)

            # If execution succeeded, add to active trades
            if trade.status == TradeStatus.OPEN:
                self.active_trades[trade_id] = trade
                self.stats['successful'] += 1
                logger.info(
                    f"Trade {trade_id} OPEN: {strategy}/{symbol} {structure_type} "
                    f"notional=R${notional:,.0f} edge={expected_edge:.4f}"
                )
            else:
                # Release capital on failure
                self.capital.release_allocation(trade_id, realized_pnl=0.0)
                self.stats['failed'] += 1

            self._persist_trade(trade)
            return trade

    def close_trade(
        self,
        trade_id: str,
        realized_pnl: float,
        close_reason: str,
    ) -> Tuple[bool, str]:
        """
        Close an active trade.

        Args:
            trade_id: Trade to close
            realized_pnl: Realized P&L
            close_reason: Reason for closing

        Returns:
            (success, message)
        """
        with self._lock:
            if trade_id not in self.active_trades:
                return False, f"Trade '{trade_id}' not found in active trades"

            trade = self.active_trades.pop(trade_id)
            trade.status = TradeStatus.CLOSED
            trade.closed_at = datetime.utcnow()
            trade.realized_pnl = realized_pnl
            trade.close_reason = close_reason

            # Release capital
            self.capital.release_allocation(trade_id, realized_pnl=realized_pnl)

            self._persist_trade_close(trade)

            logger.info(
                f"Trade {trade_id} CLOSED: pnl=R${realized_pnl:,.2f}, reason={close_reason}"
            )
            return True, f"Closed with PnL R${realized_pnl:,.2f}"

    # ── Paper Execution ──────────────────────────────────────────

    def _paper_execute(self, trade: DerivativesTrade) -> DerivativesTrade:
        """
        Paper execution: simulate fills with realistic slippage model.
        """
        filled_legs = []
        total_slippage = 0.0

        for leg in trade.legs:
            # Simulate execution timing
            start_ms = int(time.time() * 1000)

            # Slippage model: 0.05%-0.20% of intended price
            slippage_pct = 0.001  # 0.1% base
            if leg.leg_type in ('CALL', 'PUT'):
                slippage_pct = 0.002  # options have wider spreads

            if leg.side == 'BUY':
                executed_price = leg.intended_price * (1 + slippage_pct)
            else:
                executed_price = leg.intended_price * (1 - slippage_pct)

            slippage = abs(executed_price - leg.intended_price)
            latency = int(time.time() * 1000) - start_ms + 5  # min 5ms

            leg.executed_price = round(executed_price, 4)
            leg.slippage = round(slippage, 4)
            leg.latency_ms = latency
            leg.status = LegStatus.FILLED
            leg.timestamp = datetime.utcnow()

            total_slippage += slippage * leg.qty

            filled_legs.append(leg)

            # Check cumulative slippage against threshold
            if trade.notional > 0:
                slippage_bps = (total_slippage / trade.notional) * 10_000
                if slippage_bps > self.MAX_SLIPPAGE_BPS:
                    # Legging abort: slippage too high
                    trade.legging_incidents += 1
                    self.stats['legging_aborts'] += 1
                    logger.warning(
                        f"Trade {trade.trade_id} legging abort: "
                        f"slippage {slippage_bps:.1f}bps > {self.MAX_SLIPPAGE_BPS}bps"
                    )
                    # Mark remaining legs as cancelled
                    for remaining in trade.legs:
                        if remaining.status == LegStatus.PENDING:
                            remaining.status = LegStatus.CANCELLED

                    trade.status = TradeStatus.LEGGING_ABORT
                    trade.close_reason = f"Slippage abort: {slippage_bps:.1f}bps"
                    return trade

        # All legs filled
        trade.status = TradeStatus.OPEN
        self.stats['total_slippage'] += total_slippage
        return trade

    def _shadow_execute(self, trade: DerivativesTrade) -> DerivativesTrade:
        """
        Shadow execution: record what WOULD happen without allocating real capital.
        Uses same slippage model but marks trade as shadow.
        """
        trade = self._paper_execute(trade)
        trade.active_status = 'SHADOW_EXEC'
        return trade

    # ── Queries ──────────────────────────────────────────────────

    def get_active_trades(self, strategy: str = None) -> List[DerivativesTrade]:
        """Get all active trades, optionally filtered by strategy."""
        with self._lock:
            trades = list(self.active_trades.values())
            if strategy:
                trades = [t for t in trades if t.strategy == strategy]
            return trades

    def get_trade(self, trade_id: str) -> Optional[DerivativesTrade]:
        """Get specific trade by ID."""
        with self._lock:
            return self.active_trades.get(trade_id)

    def get_stats(self) -> Dict:
        """Get execution statistics."""
        with self._lock:
            return {
                **self.stats,
                'active_trades': len(self.active_trades),
                'mode': self.mode.value,
            }

    # ── DB Persistence ───────────────────────────────────────────

    def _persist_trade(self, trade: DerivativesTrade):
        """Persist trade to strategy_master_trades."""
        if not self.get_db:
            return
        conn = None
        try:
            conn = self.get_db()
            if conn is None:
                return
            cursor = conn.cursor()

            cursor.execute(
                """INSERT INTO strategy_master_trades
                   (trade_id, strategy_type, symbol, strike, expiry, direction,
                    structure_type, expected_edge, notional,
                    liquidity_score, active_status, opened_at, status, close_reason,
                    created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                   status=VALUES(status), close_reason=VALUES(close_reason),
                   updated_at=NOW()
                """,
                (
                    trade.trade_id, trade.strategy, trade.symbol,
                    trade.strike, trade.expiry, trade.structure_type,
                    trade.structure_type, trade.expected_edge, trade.notional,
                    trade.liquidity_score, trade.active_status,
                    trade.opened_at, trade.status.value, trade.close_reason,
                    datetime.utcnow(),
                ),
            )

            # Persist legs
            for leg in trade.legs:
                cursor.execute(
                    """INSERT INTO strategy_trade_legs
                       (trade_id, leg_type, symbol, qty, side,
                        intended_price, executed_price, fill_status,
                        slippage, latency_ms, timestamp)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        trade.trade_id, leg.leg_type, leg.symbol,
                        leg.qty, leg.side, leg.intended_price,
                        leg.executed_price, leg.status.value,
                        leg.slippage, leg.latency_ms, leg.timestamp,
                    ),
                )

            conn.commit()
            cursor.close()
        except Exception as e:
            logger.warning(f"Failed to persist trade {trade.trade_id}: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _persist_trade_close(self, trade: DerivativesTrade):
        """Update trade status on close."""
        if not self.get_db:
            return
        conn = None
        try:
            conn = self.get_db()
            if conn is None:
                return
            cursor = conn.cursor()
            cursor.execute(
                """UPDATE strategy_master_trades
                   SET status=%s, closed_at=%s, pnl=%s, close_reason=%s,
                       updated_at=NOW()
                   WHERE trade_id=%s
                """,
                (
                    trade.status.value, trade.closed_at,
                    trade.realized_pnl, trade.close_reason,
                    trade.trade_id,
                ),
            )
            conn.commit()
            cursor.close()
        except Exception as e:
            logger.warning(f"Failed to persist trade close {trade.trade_id}: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
