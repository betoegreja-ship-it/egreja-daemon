"""Worker que roda recalibrate_brain() a cada hora.

Sem comando manual. Roda enquanto o sistema estiver vivo.
Primeira execução dispara 30s após start (deixar app estabilizar).
"""
import os, time, logging
from .learner import recalibrate_brain

log = logging.getLogger('egreja.calibrator.worker')

INTERVAL_S = int(os.environ.get('CALIBRATOR_INTERVAL_S', 3600))   # 1h default
INITIAL_DELAY_S = int(os.environ.get('CALIBRATOR_INITIAL_DELAY_S', 30))


def calibrator_loop(beat_fn=None, audit_fn=None):
    """Loop infinito. Roda recalibrate a cada INTERVAL_S."""
    log.info(f'[calibrator] worker iniciando | interval={INTERVAL_S}s | initial_delay={INITIAL_DELAY_S}s')
    time.sleep(INITIAL_DELAY_S)

    iteration = 0
    while True:
        if beat_fn:
            try: beat_fn('brain_calibrator_loop')
            except Exception: pass
        iteration += 1
        try:
            result = recalibrate_brain()
            if result.get('error'):
                log.warning(f'[calibrator] iter#{iteration} error: {result["error"]}')
            elif result.get('skipped'):
                log.info(f'[calibrator] iter#{iteration} skipped (poucos trades)')
            else:
                log.info(f'[calibrator] iter#{iteration} OK | n={result["n_trades"]} '
                         f'feats={result["features_updated"]} combos={result["combos_updated"]} '
                         f'syms={result["symbols_updated"]} '
                         f'calib_q={result["calibration_quality"]:.4f}')
                if audit_fn:
                    try: audit_fn('BRAIN_CALIBRATED', result)
                    except Exception: pass

            # Reload cache do scorer (pra novos pesos virem online imediatamente)
            try:
                from .scorer import _reload_cache
                _reload_cache()
            except Exception: pass

        except Exception as e:
            log.error(f'[calibrator] worker iter#{iteration} crash: {e}')
            import traceback; traceback.print_exc()

        time.sleep(INTERVAL_S)
