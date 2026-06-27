"""Brain Specialist Worker — recalibra os 3 markets hora-a-hora.

Loop infinito. Cada iteracao chama recalibrate_all_markets() que itera B3/NYSE/CRYPTO
sequencialmente. Se um market falha, os outros continuam.

Apos cada recalibracao, recarrega o cache do scorer pra novos pesos virem online.
Primeira execucao apos INITIAL_DELAY_S (default 60s) pra app estabilizar.
"""
import os, time, logging, threading

from . import MARKETS
from .learner import recalibrate_all_markets

log = logging.getLogger('egreja.brain_specialist.worker')

INTERVAL_S = int(os.environ.get('SPECIALIST_INTERVAL_S', 3600))
INITIAL_DELAY_S = int(os.environ.get('SPECIALIST_INITIAL_DELAY_S', 60))

_worker_thread = None
_worker_lock = threading.Lock()


def specialist_loop(beat_fn=None, audit_fn=None):
    """Loop infinito. Roda recalibrate_all_markets a cada INTERVAL_S."""
    log.info(f'[SPECIALIST] worker iniciando | interval={INTERVAL_S}s | initial_delay={INITIAL_DELAY_S}s')
    time.sleep(INITIAL_DELAY_S)

    iteration = 0
    while True:
        if beat_fn:
            try: beat_fn('brain_specialist_loop')
            except Exception: pass
        iteration += 1
        try:
            results = recalibrate_all_markets()
            for market in MARKETS:
                r = results.get(market, {})
                if r.get('error'):
                    log.warning(f'[SPECIALIST] {market}: iter#{iteration} error: {r["error"]}')
                elif r.get('skipped'):
                    log.info(f'[SPECIALIST] {market}: iter#{iteration} skipped (poucos trades: {r.get("n_trades", 0)})')
                else:
                    log.info(f'[SPECIALIST] {market}: recalibrated {r.get("n_trades", 0)} trades, '
                             f'{r.get("features_updated", 0)} features updated, '
                             f'{r.get("combos_updated", 0)} combos, {r.get("symbols_updated", 0)} symbols, '
                             f'baseline={r.get("baseline_wr", 0):.2f}% '
                             f'calib_q={r.get("calibration_quality", 0):.4f}')
                    if audit_fn:
                        try: audit_fn('SPECIALIST_CALIBRATED', r)
                        except Exception: pass

            # Reload cache do scorer (novos pesos online imediatamente)
            try:
                from .scorer import _reload_cache
                _reload_cache()
            except Exception as e:
                log.debug(f'[SPECIALIST] cache reload pos-recalib: {e}')

        except Exception as e:
            log.error(f'[SPECIALIST] worker iter#{iteration} crash: {e}')
            import traceback; traceback.print_exc()

        time.sleep(INTERVAL_S)


def start_worker(beat_fn=None, audit_fn=None):
    """Inicia a thread do worker (idempotente). Chamado pelo api_server.py no boot."""
    global _worker_thread
    with _worker_lock:
        if _worker_thread is not None and _worker_thread.is_alive():
            log.info('[SPECIALIST] worker ja rodando — skip start')
            return _worker_thread
        t = threading.Thread(
            target=specialist_loop,
            args=(beat_fn, audit_fn),
            daemon=True,
            name='brain_specialist_worker',
        )
        t.start()
        _worker_thread = t
        log.info('[SPECIALIST] worker thread iniciada')
        return t
