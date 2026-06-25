"""Worker que recalibra todos os pares a cada 1h + gera insights a cada 4h.

Sem comando manual. Roda enquanto o app vive.
"""
import os, time, logging
from .config import PAIRS_CONFIG
from .learning import recalibrate_pair, generate_insights

log = logging.getLogger('egreja.pairs.learning_worker')

RECAL_INTERVAL_S = int(os.environ.get('PAIRS_RECALIB_INTERVAL_S', 3600))   # 1h
INSIGHTS_INTERVAL_S = int(os.environ.get('PAIRS_INSIGHTS_INTERVAL_S', 14400))  # 4h
INITIAL_DELAY_S = 120  # 2min pra app estabilizar


def pairs_learning_loop(beat_fn=None, audit_fn=None):
    log.info(f'[pairs.learning] worker iniciando | recal_interval={RECAL_INTERVAL_S}s '
             f'insights_interval={INSIGHTS_INTERVAL_S}s')
    time.sleep(INITIAL_DELAY_S)
    last_insights = 0
    iteration = 0
    while True:
        iteration += 1
        if beat_fn:
            try: beat_fn('pairs_learning_loop')
            except: pass
        now = time.time()
        try:
            # Recalibracao: todos os pares ativos
            n_recal = 0
            n_changed_tier = 0
            for cfg in PAIRS_CONFIG:
                if not cfg.get('enabled', True): continue
                if beat_fn:
                    try: beat_fn('pairs_learning_loop')
                    except: pass
                try:
                    r = recalibrate_pair(cfg['id'], window_days=60)
                    if r and 'tier_recommended' in r:
                        n_recal += 1
                        if r['tier_recommended'] != cfg.get('tier'):
                            n_changed_tier += 1
                            log.info(f'[learning] {cfg["id"]} tier={cfg.get("tier")} '
                                     f'→ recomendado={r["tier_recommended"]} '
                                     f'(adf={r["adf_tstat"]:+.2f} hl={r["half_life_days"]:.1f}d '
                                     f'corr={r["return_corr"]:.2f} regime={r["regime"]})')
                except Exception as e:
                    log.debug(f'recalibrate {cfg["id"]}: {e}')
                # Pequeno sleep entre pares (evita spike DB)
                time.sleep(0.1)
            log.info(f'[learning] iter#{iteration} recalibration OK | '
                     f'recal={n_recal} tier_changes={n_changed_tier}')

            # Insights: cada 4h
            if (now - last_insights) > INSIGHTS_INTERVAL_S:
                n_insights = 0
                for cfg in PAIRS_CONFIG:
                    if not cfg.get('enabled', True): continue
                    try:
                        generate_insights(cfg['id'])
                        n_insights += 1
                    except Exception as e:
                        log.debug(f'insights {cfg["id"]}: {e}')
                    time.sleep(0.1)
                last_insights = now
                log.info(f'[learning] insights generated for {n_insights} pares')
                if audit_fn:
                    try: audit_fn('PAIRS_INSIGHTS_GENERATED', {'n': n_insights})
                    except: pass

        except Exception as e:
            log.error(f'[learning] iter#{iteration} crash: {e}')
            import traceback; traceback.print_exc()

        time.sleep(RECAL_INTERVAL_S)
