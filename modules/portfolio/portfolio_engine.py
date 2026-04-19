"""
PortfolioEngine — singleton que mantém StrategyCapitalState canônico
em memória para stocks/crypto/arbi, sincronizado com capital_ledger.

Shadow mode (Fase 1/2): engine boota, replay do ledger, atualiza em
background mas NÃO é consultado no caminho crítico. Quando
PORTFOLIO_ENGINE_ACTIVE=true, vira fonte de verdade.
"""
import json
import logging
import os
import threading
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from typing import Callable, Optional

from modules.portfolio.config_loader import ConfigLoader, StrategyConfig
from modules.portfolio.events import (
    EventType, build_idempotency_key, validate_amount_sign,
)
from modules.portfolio.reducer import (
    apply_event_to_state, recompute_derived_limits,
)
from modules.portfolio.state import StrategyCapitalState, empty_state

log = logging.getLogger(__name__)

SUPPORTED_STRATEGIES = ('stocks', 'crypto', 'arbi')


# ═══════════════════════════════════════════════════════════════════════
# Exceções
# ═══════════════════════════════════════════════════════════════════════

class InsufficientCapitalError(Exception):
    """Reserva pedida > free_capital."""


class DuplicateIdempotencyError(Exception):
    """Evento com mesma idempotency_key já existe — NOOP."""


class IntegrityViolationError(Exception):
    """Invariante de integridade violada (replay != canonical)."""


# ═══════════════════════════════════════════════════════════════════════
# PortfolioEngine
# ═══════════════════════════════════════════════════════════════════════

class PortfolioEngine:
    _instance = None
    _cls_lock = threading.Lock()

    def __new__(cls, *a, **kw):
        if cls._instance is None:
            with cls._cls_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    @classmethod
    def instance(cls) -> 'PortfolioEngine':
        return cls()

    def __init__(self):
        if self._initialized:
            return
        self.db_fn: Optional[Callable] = None
        self.config_loader: Optional[ConfigLoader] = None
        self._states: dict[str, StrategyCapitalState] = {}
        self._strategy_locks: dict[str, threading.RLock] = {
            s: threading.RLock() for s in SUPPORTED_STRATEGIES
        }
        self._booted = False
        self._initialized = True

    # ─────────────────────────── Lifecycle ─────────────────────────────

    def boot(self, db_fn: Callable, config_loader: ConfigLoader = None) -> None:
        """Inicializa engine. Chamar 1x no startup do daemon."""
        self.db_fn = db_fn
        self.config_loader = config_loader or ConfigLoader(db_fn=db_fn)
        if not self.config_loader.db_fn:
            self.config_loader.set_db_fn(db_fn)

        for strat in SUPPORTED_STRATEGIES:
            try:
                self._states[strat] = self._rebuild_from_ledger(strat)
                s = self._states[strat]
                log.info(
                    f'[PortfolioEngine] boot {strat}: '
                    f'equity={float(s.gross_equity):,.2f} '
                    f'free={float(s.free_capital):,.2f} '
                    f'reserved={float(s.reserved_capital):,.2f} '
                    f'positions={s.open_positions_count}/{s.max_positions_allowed}'
                )
            except Exception as e:
                log.error(f'[PortfolioEngine] boot {strat} falhou: {e}')
                # Fallback: state vazio da config
                cfg = self.config_loader.get(strat)
                self._states[strat] = empty_state(strat, cfg.initial_capital)
        self._booted = True

    @property
    def booted(self) -> bool:
        return self._booted

    @property
    def active(self) -> bool:
        """Se True, engine assume caminho crítico. Se False, shadow."""
        return os.environ.get('PORTFOLIO_ENGINE_ACTIVE', 'false').lower() == 'true'

    # ─────────────────────────── Leitura ───────────────────────────────

    def get_state(self, strategy: str) -> StrategyCapitalState:
        if strategy not in SUPPORTED_STRATEGIES:
            raise ValueError(f'strategy inválida: {strategy}')
        with self._strategy_locks[strategy]:
            return self._states[strategy]

    def get_all_states(self) -> dict[str, StrategyCapitalState]:
        return {s: self.get_state(s) for s in SUPPORTED_STRATEGIES}

    # ─────────────────────────── Escrita (transacional) ────────────────

    def apply_event(
        self,
        strategy: str,
        event_type: str,
        amount,
        *,
        trade_id: Optional[str] = None,
        symbol: str = 'SYSTEM',
        metadata: Optional[dict] = None,
        created_by: str = 'system',
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """
        Grava evento no ledger + atualiza mirror em memória, tudo em 1
        transação atômica. Idempotente via idempotency_key.
        """
        if strategy not in SUPPORTED_STRATEGIES:
            raise ValueError(f'strategy inválida: {strategy}')

        amount_dec = Decimal(str(amount))
        validate_amount_sign(event_type, float(amount_dec))
        idem = idempotency_key or build_idempotency_key(event_type, strategy, trade_id)

        conn = self.db_fn()
        if conn is None:
            raise RuntimeError('db_fn retornou None')

        try:
            # Isolation: SERIALIZABLE para operações críticas de capital
            try:
                conn.autocommit = False
                cur = conn.cursor()
                cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            except Exception:
                pass

            # Advisory lock por estratégia (10s timeout)
            cur = conn.cursor()
            cur.execute("SELECT GET_LOCK(%s, 10)", (f'egreja:strat:{strategy}',))
            got_lock = cur.fetchone()[0]
            if got_lock != 1:
                cur.close()
                conn.rollback()
                raise RuntimeError(f'advisory lock timeout: {strategy}')

            # 1. Computa estado novo em memória (ainda não commita)
            with self._strategy_locks[strategy]:
                old_state = self._states[strategy]
                balance_before = float(old_state.gross_equity)

                new_state = apply_event_to_state(
                    old_state, event_type, amount_dec, event_ts=datetime.utcnow()
                )
                balance_after = float(new_state.gross_equity)

                # 2. INSERT no ledger — IntegrityError se idem duplicado
                try:
                    cur.execute(
                        """INSERT INTO capital_ledger
                           (ts, strategy, event, symbol, amount, balance_before,
                            balance_after, trade_id, idempotency_key,
                            metadata_json, created_by)
                           VALUES (NOW(3), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (strategy, event_type, symbol, float(amount_dec),
                         balance_before, balance_after,
                         trade_id, idem,
                         json.dumps(metadata or {}), created_by)
                    )
                    event_id = cur.lastrowid
                except Exception as e:
                    emsg = str(e).lower()
                    if 'duplicate' in emsg and 'idempotency_key' in emsg:
                        cur.execute("SELECT RELEASE_LOCK(%s)", (f'egreja:strat:{strategy}',))
                        cur.close()
                        conn.rollback()
                        raise DuplicateIdempotencyError(idem)
                    raise

                # 3. Commit transacional
                conn.commit()

                # 4. Só depois do commit: atualiza mirror em memória
                new_state = replace(new_state, last_event_id=event_id)
                self._states[strategy] = new_state

            cur.execute("SELECT RELEASE_LOCK(%s)", (f'egreja:strat:{strategy}',))
            cur.close()

            return {
                'strategy': strategy,
                'event_type': event_type,
                'event_id': event_id,
                'amount': float(amount_dec),
                'balance_before': balance_before,
                'balance_after': balance_after,
                'version': new_state.version,
                'idempotency_key': idem,
            }

        except DuplicateIdempotencyError:
            raise
        except Exception:
            try: conn.rollback()
            except Exception: pass
            raise
        finally:
            try: conn.close()
            except Exception: pass

    # ─────────────────────────── High-level helpers ────────────────────

    def reserve_on_open(
        self, strategy: str, trade_id: str, reserve_amount,
        metadata: dict = None
    ) -> dict:
        """Chama em open_trade: reserva capital. Falha se sem free_capital."""
        state = self.get_state(strategy)
        reserve = Decimal(str(reserve_amount))
        if reserve > state.free_capital:
            raise InsufficientCapitalError(
                f'{strategy}: reserve {reserve} > free {state.free_capital}'
            )
        return self.apply_event(
            strategy, EventType.TRADE_OPEN_RESERVE.value, reserve,
            trade_id=trade_id, symbol='RESERVE', metadata=metadata,
        )

    def release_and_realize(
        self, strategy: str, trade_id: str, reserved_amount, realized_pnl,
        fees_total=0, metadata: dict = None
    ) -> dict:
        """Chama em close_trade: libera reserva + credita PnL + deduz fees.
        Executa 3 apply_event em sequência. Cada um idempotente."""
        results = []
        try:
            r1 = self.apply_event(
                strategy, EventType.TRADE_CLOSE_RELEASE.value,
                Decimal(str(reserved_amount)),
                trade_id=trade_id, symbol='RELEASE', metadata=metadata,
            )
            results.append(r1)
        except DuplicateIdempotencyError:
            log.info(f'[release_and_realize] {trade_id} release já processado')

        if fees_total and float(fees_total) != 0:
            try:
                # FEE sempre negativo
                fee_amt = Decimal(str(-abs(float(fees_total))))
                r2 = self.apply_event(
                    strategy, EventType.FEE.value, fee_amt,
                    trade_id=trade_id, symbol='FEE',
                )
                results.append(r2)
            except DuplicateIdempotencyError:
                log.info(f'[release_and_realize] {trade_id} fee já processado')

        try:
            r3 = self.apply_event(
                strategy, EventType.REALIZED_PNL.value,
                Decimal(str(realized_pnl)),
                trade_id=trade_id, symbol='PNL',
            )
            results.append(r3)
        except DuplicateIdempotencyError:
            log.info(f'[release_and_realize] {trade_id} pnl já processado')

        return {'trade_id': trade_id, 'events': results}

    # ─────────────────────────── Rebuild / integrity ───────────────────

    def _rebuild_from_ledger(self, strategy: str) -> StrategyCapitalState:
        """Replay completo do ledger. Pura — não muta state."""
        cfg = self.config_loader.get(strategy)
        state = empty_state(strategy, cfg.initial_capital)

        conn = self.db_fn()
        if conn is None:
            return state
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(
                """SELECT id, event, amount, ts FROM capital_ledger
                   WHERE strategy=%s
                   ORDER BY ts ASC, id ASC""",
                (strategy,)
            )
            for row in cur.fetchall():
                amount = Decimal(str(row['amount']))
                state = apply_event_to_state(
                    state, row['event'], amount,
                    event_id=row['id'], event_ts=row['ts'],
                )
            cur.close()
        finally:
            try: conn.close()
            except Exception: pass

        # Calcula current_gross_exposure e open_positions do trades
        exposure, pos_count = self._query_live_exposure(strategy)
        state = recompute_derived_limits(
            state,
            current_gross_exposure=exposure,
            max_gross_exposure_pct=cfg.max_gross_exposure_pct,
            configured_max_positions=cfg.configured_max_positions,
            min_capital_per_trade=cfg.min_capital_per_trade,
            risk_per_trade_pct=cfg.risk_per_trade_pct,
            open_positions_count=pos_count,
        )
        return state

    def _query_live_exposure(self, strategy: str) -> tuple[Decimal, int]:
        """Soma exposição atual de trades OPEN + count."""
        conn = self.db_fn()
        if conn is None:
            return Decimal('0'), 0
        try:
            cur = conn.cursor()
            if strategy == 'arbi':
                cur.execute(
                    "SELECT COALESCE(SUM(position_size),0), COUNT(*) "
                    "FROM arbi_trades WHERE status='OPEN'"
                )
            else:
                asset = 'stock' if strategy == 'stocks' else 'crypto'
                cur.execute(
                    "SELECT COALESCE(SUM(position_value),0), COUNT(*) "
                    "FROM trades WHERE status='OPEN' AND asset_type=%s",
                    (asset,)
                )
            row = cur.fetchone()
            cur.close()
            return Decimal(str(row[0] or 0)), int(row[1] or 0)
        finally:
            try: conn.close()
            except Exception: pass

    def integrity_check(self) -> dict:
        """Compara replay(ledger) com canonical. Alerta se divergência."""
        report = {}
        for strat in SUPPORTED_STRATEGIES:
            try:
                fresh = self._rebuild_from_ledger(strat)
                canonical = self.get_state(strat)
                delta = abs(fresh.gross_equity - canonical.gross_equity)
                ok = delta < Decimal('0.01')
                report[strat] = {
                    'ok': ok,
                    'replay_equity': float(fresh.gross_equity),
                    'canonical_equity': float(canonical.gross_equity),
                    'delta': float(delta),
                    'canonical_version': canonical.version,
                }
                if not ok:
                    log.critical(
                        f'[PortfolioEngine] INTEGRITY VIOLATION {strat}: '
                        f'replay={fresh.gross_equity} canonical={canonical.gross_equity}'
                    )
            except Exception as e:
                report[strat] = {'error': str(e)}
        return report

    def rebuild(self, strategy: str) -> StrategyCapitalState:
        """Força rebuild de um strategy (usado em endpoint de manutenção)."""
        with self._strategy_locks[strategy]:
            new_state = self._rebuild_from_ledger(strategy)
            self._states[strategy] = new_state
            return new_state
