"""Backfill batch — puxa historico denso (2-5y) de BRAPI + Cedro pra todos os pares.

Uso (chamado por endpoint admin /pairs/backfill ou script standalone):
    from modules.pairs_engine.backfill import run_backfill
    result = run_backfill(days=730)   # 2 anos
"""
import logging
import time
from typing import Dict
from .config import PAIRS_CONFIG
from .data_fetcher import fetch_pair_history_brapi, fetch_pair_history_cedro
from . import persistence as _persist

log = logging.getLogger('egreja.pairs.backfill')


def run_backfill(days: int = 730, sleep_between: float = 0.5,
                 use_cedro: bool = True, use_brapi: bool = True) -> Dict:
    """Backfill historico daily para todos os simbolos ativos.

    Strategy:
    1. Tentar Cedro primeiro (mais denso, pago)
    2. Tentar BRAPI (cobre +1y free, max plano pago)
    3. Merge no MySQL via upsert (idempotente)

    Returns:
        Dict {symbol: {brapi: n_bars, cedro: n_bars, persisted: n}}
    """
    syms = set()
    for p in PAIRS_CONFIG:
        if p.get('enabled', True):
            syms.add(p['leg_a'])
            syms.add(p['leg_b'])
    syms = sorted(syms)
    log.info(f'[BACKFILL] starting | {len(syms)} simbolos | target={days}d | cedro={use_cedro} brapi={use_brapi}')

    result = {}
    for i, sym in enumerate(syms, 1):
        info = {'brapi': 0, 'cedro': 0, 'persisted': 0}

        # 1) Cedro (datafeed pago — mais denso)
        if use_cedro:
            try:
                cedro_bars = fetch_pair_history_cedro(sym, days=days)
                info['cedro'] = len(cedro_bars)
                if cedro_bars:
                    n = _persist.bulk_upsert_daily_bars(sym, cedro_bars, source='cedro')
                    info['persisted'] += n
            except Exception as e:
                log.warning(f'[BACKFILL] Cedro {sym}: {e}')
            time.sleep(sleep_between)

        # 2) BRAPI (complementa quando Cedro nao cobriu ou pra cross-check)
        if use_brapi:
            try:
                brapi_bars = fetch_pair_history_brapi(sym, days=days)
                info['brapi'] = len(brapi_bars)
                if brapi_bars:
                    # Upsert: BRAPI sobrescreve so se Cedro nao gravou nessa data
                    # Mas pra simplicidade fazemos upsert geral (idempotente)
                    n = _persist.bulk_upsert_daily_bars(sym, brapi_bars, source='brapi')
                    info['persisted'] = max(info['persisted'], n)
            except Exception as e:
                log.warning(f'[BACKFILL] BRAPI {sym}: {e}')
            time.sleep(sleep_between)

        result[sym] = info
        log.info(f'[BACKFILL] {i:>2}/{len(syms)} {sym}: brapi={info["brapi"]} '
                 f'cedro={info["cedro"]} persisted={info["persisted"]}')

    total_persisted = sum(r.get('persisted', 0) for r in result.values())
    log.info(f'[BACKFILL] COMPLETED | total bars persisted: {total_persisted}')
    return {
        'symbols': len(syms),
        'target_days': days,
        'total_bars_persisted': total_persisted,
        'detail': result,
    }
