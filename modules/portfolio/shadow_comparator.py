"""
ShadowComparator — compara PortfolioEngine (v11) com variáveis globais
legacy do api_server.py (stocks_capital, crypto_capital, arbi_capital)
a cada N segundos. Loga divergência + persiste em reconciliation_v11_log.

Rodado em thread separada chamada SHADOW no watchdog. Não afeta
caminho crítico. Ativado quando PORTFOLIO_ENGINE_ACTIVE != true.

Objetivo: durante 24-48h em Fase 1/2, acumular evidência de que v11 bate
com legacy. Divergência > $0.50 cumulativa dispara warning; > $10
dispara alerta.
"""
import json
import logging
import threading
import time
from decimal import Decimal
from typing import Callable

from modules.portfolio.portfolio_engine import PortfolioEngine, SUPPORTED_STRATEGIES

log = logging.getLogger(__name__)


class ShadowComparator:

    def __init__(
        self,
        db_fn: Callable,
        legacy_state_fn: Callable,
        interval_s: int = 60,
        alert_threshold_usd: float = 10.0,
        warn_threshold_usd: float = 0.50,
        beat_fn: Callable = None,
    ):
        """
        Args:
            db_fn: retorna conexão MySQL
            legacy_state_fn: callable() -> dict com
              {'stocks': capital_float, 'crypto': ..., 'arbi': ...}
              (lê as vars globais do api_server.py)
            interval_s: período entre comparações
            alert_threshold_usd: delta acima disso → log CRITICAL
            warn_threshold_usd: delta acima disso → log WARNING
            beat_fn: função beat(name) do watchdog
        """
        self.db_fn = db_fn
        self.legacy_state_fn = legacy_state_fn
        self.interval_s = interval_s
        self.alert_threshold = alert_threshold_usd
        self.warn_threshold = warn_threshold_usd
        self.beat_fn = beat_fn or (lambda _: None)
        self._stop = threading.Event()
        self._last_run_at = 0

    def run_forever(self) -> None:
        """Loop. Bloqueante — rode em thread própria."""
        log.info(f'[ShadowComparator] iniciado, intervalo={self.interval_s}s')
        # Sleep inicial em chunks com beat
        for _ in range(min(6, self.interval_s // 10)):
            self.beat_fn('portfolio_shadow_comparator')
            if self._stop.wait(10):
                return

        while not self._stop.is_set():
            try:
                self.beat_fn('portfolio_shadow_comparator')
                self.run_once()
            except Exception as e:
                log.error(f'[ShadowComparator] erro: {e}')
            # sleep em chunks com beat
            for _ in range(self.interval_s // 10 or 1):
                self.beat_fn('portfolio_shadow_comparator')
                if self._stop.wait(10):
                    return
        log.info('[ShadowComparator] parando')

    def stop(self) -> None:
        self._stop.set()

    def run_once(self) -> dict:
        """Uma iteração. Retorna dict com resultado e persiste em DB."""
        engine = PortfolioEngine.instance()
        if not engine.booted:
            return {'skipped': 'engine_not_booted'}

        legacy_snap = self.legacy_state_fn() or {}
        report = {'ts': time.time(), 'strategies': {}, 'max_abs_delta': 0.0}

        conn = self.db_fn()
        if conn is None:
            return {'skipped': 'no_db'}

        try:
            cur = conn.cursor()
            for strat in SUPPORTED_STRATEGIES:
                v11_state = engine.get_state(strat)
                legacy_cap = float(legacy_snap.get(strat, 0) or 0)

                # Legacy "capital" é equivalente ao free_capital (variável
                # global do api_server é capital livre, não gross_equity)
                v11_free = float(v11_state.free_capital)
                delta = v11_free - legacy_cap
                abs_delta = abs(delta)

                report['strategies'][strat] = {
                    'v11_free_capital': v11_free,
                    'v11_gross_equity': float(v11_state.gross_equity),
                    'v11_reserved': float(v11_state.reserved_capital),
                    'v11_realized_pnl': float(v11_state.realized_pnl),
                    'legacy_capital': legacy_cap,
                    'delta': round(delta, 2),
                    'abs_delta': round(abs_delta, 2),
                }
                if abs_delta > report['max_abs_delta']:
                    report['max_abs_delta'] = abs_delta

                # Log escalado
                if abs_delta >= self.alert_threshold:
                    log.critical(
                        f'[ShadowComparator] {strat}: DELTA CRÍTICO ${abs_delta:.2f} '
                        f'v11={v11_free:.2f} legacy={legacy_cap:.2f}'
                    )
                elif abs_delta >= self.warn_threshold:
                    log.warning(
                        f'[ShadowComparator] {strat}: delta ${abs_delta:.2f} '
                        f'v11={v11_free:.2f} legacy={legacy_cap:.2f}'
                    )
                else:
                    log.info(
                        f'[ShadowComparator] {strat}: ok (delta ${abs_delta:.4f})'
                    )

                # Persiste no log de reconciliation v11
                try:
                    cur.execute(
                        """INSERT INTO reconciliation_v11_log
                           (strategy, check_type, canonical_equity, legacy_capital,
                            delta_canonical_vs_legacy, ok, notes)
                           VALUES (%s, 'shadow_vs_legacy', %s, %s, %s, %s, %s)""",
                        (strat, v11_free, legacy_cap, abs_delta,
                         1 if abs_delta < self.warn_threshold else 0,
                         json.dumps(report['strategies'][strat]))
                    )
                except Exception as e:
                    log.debug(f'[ShadowComparator] insert log falhou: {e}')
            conn.commit()
            cur.close()
        finally:
            try: conn.close()
            except Exception: pass

        self._last_run_at = time.time()
        return report
