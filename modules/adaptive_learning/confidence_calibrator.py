"""Confidence Calibrator — mede se learning_confidence prediz corretamente.

Função: calibrate_confidence(db_fn, log, asset_type) -> dict

Segmenta trades por banda de confidence e verifica:
- wins vs losses por banda
- PnL total por banda
- inversão (Spearman: confidence alta -> PnL pior?)
- bandas que deveriam virar dead_zone
- remapeamento sugerido (delta de score a aplicar por banda)

Escreve em learning_confidence_calibration.
"""
from __future__ import annotations
import uuid
from typing import Any, Callable, Dict
from .isolation import should_bypass_adaptive_learning
from .stats_helpers import is_confidence_inverted, expectancy


# Bandas fixas — podem virar env var no futuro
_BANDS = [
    ('baixa',       0,  50),
    ('media',      50,  60),
    ('alta',       60,  70),
    ('muito_alta', 70,  80),
    ('extrema',    80, 101),
]


def calibrate_confidence(
    db_fn: Callable,
    log,
    asset_type: str,
    lookback_days: int = 30,
    direction: str = None,
    persist: bool = True,
    run_id: str = None,
) -> Dict[str, Any]:
    """Analisa se confidence está calibrada pra `asset_type`.

    Retorna:
      {
        "bypassed": bool,
        "asset_type": str,
        "direction": str|None,
        "bands": [ {band, n, wr, total_pnl, avg_pnl_pct, expectancy}, ... ],
        "inverted": bool,          # confidence alta dá pior?
        "dead_zone_suggested": [lower, upper] | None,
        "best_band": str,          # banda que dá melhor PnL
        "recommendations": [str, ...],
      }
    """
    if should_bypass_adaptive_learning(asset_type):
        return {'bypassed': True, 'asset_type': asset_type}

    run_id = run_id or f"CC-{uuid.uuid4().hex[:12]}"
    conn = None
    try:
        conn = db_fn()
        if not conn:
            return {'bypassed': False, 'error': 'no_db'}
        c = conn.cursor(dictionary=True)

        bands_data = []
        for band_name, lo, hi in _BANDS:
            q = f"""
                SELECT COUNT(*) AS n,
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS w,
                       SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) AS l,
                       SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) AS sum_wins,
                       SUM(CASE WHEN pnl < 0 THEN pnl ELSE 0 END) AS sum_losses,
                       AVG(pnl_pct) AS avg_pnl_pct,
                       SUM(pnl) AS total_pnl
                FROM trades
                WHERE asset_type = %s
                  AND status = 'CLOSED'
                  AND learning_confidence IS NOT NULL
                  AND learning_confidence >= %s AND learning_confidence < %s
                  AND closed_at > NOW() - INTERVAL {int(lookback_days)} DAY
            """
            params = [asset_type, lo, hi]
            if direction:
                q += " AND direction = %s"
                params.append(direction)
            c.execute(q, tuple(params))
            row = c.fetchone() or {}
            n = int(row.get('n') or 0)
            w = int(row.get('w') or 0)
            l = int(row.get('l') or 0)
            wr = w / max(w + l, 1) if (w + l) else 0.0
            sum_w = float(row.get('sum_wins') or 0)
            sum_l = float(row.get('sum_losses') or 0)
            avg_win = sum_w / max(w, 1) if w > 0 else 0.0
            avg_loss = sum_l / max(l, 1) if l > 0 else 0.0
            bands_data.append({
                'band': band_name,
                'band_lower': lo,
                'band_upper': hi,
                'n': n,
                'win_rate': round(wr, 4),
                'total_pnl': round(float(row.get('total_pnl') or 0), 2),
                'avg_pnl_pct': round(float(row.get('avg_pnl_pct') or 0), 4),
                'expectancy': round(expectancy(wr, avg_win, avg_loss), 4),
            })

        # Detectar inversão (apenas considerando bandas com n>=20)
        bands_signif = [b for b in bands_data if b['n'] >= 20]
        inverted = is_confidence_inverted(bands_signif) if len(bands_signif) >= 3 else False

        # Sugerir dead_zone: maior faixa contígua com PnL negativo
        dead_zone = _suggest_dead_zone(bands_data)

        # Melhor banda (maior total_pnl)
        best = max(bands_data, key=lambda b: b['total_pnl']) if bands_data else None
        best_band = best['band'] if best else None

        # Recomendações textuais
        recs = []
        if inverted:
            recs.append(f'INVERSAO_DETECTADA:{asset_type}_confidence_alta_performa_pior')
        if dead_zone:
            recs.append(f'DEAD_ZONE_SUGERIDA:[{dead_zone[0]},{dead_zone[1]})')
        if best_band:
            recs.append(f'MELHOR_BANDA:{best_band}')
        pnl_neg_bands = [b['band'] for b in bands_data if b['total_pnl'] < 0 and b['n'] >= 20]
        if pnl_neg_bands:
            recs.append(f'BANDAS_DEFICITARIAS:{",".join(pnl_neg_bands)}')

        result = {
            'bypassed': False,
            'run_id': run_id,
            'asset_type': asset_type,
            'direction': direction,
            'bands': bands_data,
            'inverted': inverted,
            'dead_zone_suggested': dead_zone,
            'best_band': best_band,
            'recommendations': recs,
        }

        if persist:
            _persist_calibration(conn, result, log)
        return result
    except Exception as e:
        log.warning(f'[ADAPTIVE] calibrate_confidence erro: {e}')
        return {'bypassed': False, 'error': str(e)}
    finally:
        if conn:
            try: conn.close()
            except Exception: pass


def _suggest_dead_zone(bands_data):
    """Acha maior sequência contígua de bandas com PnL negativo (n>=20).
    Retorna [lower, upper] ou None.
    """
    best_run = None
    best_len = 0
    cur_run = None
    for b in bands_data:
        if b['total_pnl'] < 0 and b['n'] >= 20:
            if cur_run is None:
                cur_run = [b['band_lower'], b['band_upper']]
            else:
                cur_run[1] = b['band_upper']
        else:
            if cur_run is not None:
                ln = cur_run[1] - cur_run[0]
                if ln > best_len:
                    best_len = ln
                    best_run = cur_run
                cur_run = None
    if cur_run is not None:
        ln = cur_run[1] - cur_run[0]
        if ln > best_len:
            best_run = cur_run
    return best_run


def _persist_calibration(conn, result, log):
    c = conn.cursor()
    for b in result.get('bands', []):
        try:
            c.execute("""INSERT INTO learning_confidence_calibration
                (run_id, asset_type, direction, confidence_band,
                 band_lower, band_upper, sample_size, win_rate,
                 total_pnl, avg_pnl_pct, expectancy,
                 inversion_flag, recommended_action, recommended_dead_zone)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                result['run_id'], result['asset_type'], result.get('direction'),
                b['band'], b['band_lower'], b['band_upper'],
                b['n'], b['win_rate'], b['total_pnl'],
                b['avg_pnl_pct'], b['expectancy'],
                1 if result.get('inverted') else 0,
                None,
                1 if (result.get('dead_zone_suggested') and
                      result['dead_zone_suggested'][0] <= b['band_lower'] < result['dead_zone_suggested'][1])
                  else 0,
            ))
        except Exception as e:
            log.debug(f'[ADAPTIVE] persist calibration skip: {e}')
    conn.commit()
