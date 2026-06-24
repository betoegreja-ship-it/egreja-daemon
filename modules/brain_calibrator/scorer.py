"""Brain Scorer — aplica pesos aprendidos no momento da decisão de entry.

Recebe: score_original (int do brain), features (dict), symbol, asset_type, direction
Retorna: score_adjusted (float), adj_total (float), breakdown (dict)

Cache: 5min em memoria. Reload se diff > 5min.
"""
import os, time, json, logging
from datetime import datetime
from threading import Lock

log = logging.getLogger('egreja.calibrator.scorer')

# ── Cache em memoria ──
_weights_cache = {'ts': 0, 'features': {}, 'combos': {}, 'symbols': {}}
_cache_lock = Lock()
CACHE_TTL_S = int(os.environ.get('CALIBRATOR_CACHE_TTL_S', 300))  # 5min


def _get_conn():
    try:
        import api_server
        if hasattr(api_server, 'get_db'): return api_server.get_db()
        if hasattr(api_server, '_pool') and api_server._pool: return api_server._pool.get_connection()
    except Exception: pass
    try:
        import mysql.connector
        return mysql.connector.connect(
            host=os.environ.get('MYSQLHOST') or os.environ.get('MYSQL_HOST'),
            user=os.environ.get('MYSQLUSER') or os.environ.get('MYSQL_USER'),
            password=os.environ.get('MYSQLPASSWORD') or os.environ.get('MYSQL_PASSWORD'),
            database=os.environ.get('MYSQLDATABASE') or os.environ.get('MYSQL_DATABASE'),
            port=int(os.environ.get('MYSQLPORT') or os.environ.get('MYSQL_PORT') or 3306),
            connection_timeout=10,
        )
    except Exception as e:
        log.debug(f'no DB: {e}')
        return None


def _reload_cache():
    """Recarrega cache do MySQL. Retorna True se sucesso."""
    conn = _get_conn()
    if not conn: return False
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT feature_name, feature_value, asset_scope, adj_pts FROM brain_feature_weights")
        feats = {}
        for r in cur.fetchall():
            key = (r['feature_name'], str(r['feature_value']), r['asset_scope'])
            feats[key] = float(r['adj_pts'] or 0)

        cur.execute("SELECT combo_key, combo_value, adj_pts FROM brain_combo_weights")
        combos = {}
        for r in cur.fetchall():
            key = (r['combo_key'], r['combo_value'])
            combos[key] = float(r['adj_pts'] or 0)

        cur.execute("SELECT symbol, asset_type, symbol_skill_pts FROM brain_symbol_stats")
        symbols = {}
        for r in cur.fetchall():
            key = (r['symbol'], r['asset_type'])
            symbols[key] = float(r['symbol_skill_pts'] or 0)

        cur.close()
        with _cache_lock:
            _weights_cache['ts'] = time.time()
            _weights_cache['features'] = feats
            _weights_cache['combos'] = combos
            _weights_cache['symbols'] = symbols
        log.info(f'[scorer] cache reloaded: {len(feats)} features, {len(combos)} combos, {len(symbols)} symbols')
        return True
    except Exception as e:
        log.warning(f'[scorer] reload cache: {e}')
        return False
    finally:
        try: conn.close()
        except Exception: pass


def _ensure_cache():
    """Recarrega se cache stale."""
    with _cache_lock:
        age = time.time() - _weights_cache['ts']
    if age > CACHE_TTL_S:
        _reload_cache()


def get_active_weights() -> dict:
    """Retorna snapshot dos pesos atuais. Pra endpoint /brain/calibration."""
    _ensure_cache()
    with _cache_lock:
        return {
            'cache_age_s': int(time.time() - _weights_cache['ts']),
            'features_count': len(_weights_cache['features']),
            'combos_count': len(_weights_cache['combos']),
            'symbols_count': len(_weights_cache['symbols']),
            'top_boosts': sorted(_weights_cache['features'].items(),
                                key=lambda x: -x[1])[:20],
            'top_penalties': sorted(_weights_cache['features'].items(),
                                   key=lambda x: x[1])[:20],
        }


def apply_calibration(score_original: int, features: dict, symbol: str,
                      asset_type: str = 'stock', direction: str = 'LONG',
                      hour_utc: int = None) -> tuple:
    """Aplica calibração ao score original.

    Returns:
        (score_adjusted: float, adj_total: float, breakdown: list[dict])
    """
    if not os.environ.get('BRAIN_CALIBRATOR_ENABLED', 'true').lower() == 'true':
        return float(score_original), 0.0, []

    _ensure_cache()
    breakdown = []
    adj_total = 0.0

    asset_norm = 'stock' if asset_type in ('stock','stocks') else asset_type

    # ── 1. Features universais ──
    feature_values = {
        'time_bucket': features.get('time_bucket'),
        'ema_alignment': features.get('ema_alignment'),
        'volume_bucket': features.get('volume_bucket'),
        'atr_bucket': features.get('atr_bucket'),
        'rsi_bucket': features.get('rsi_bucket'),
        'market_type': features.get('market_type'),
        'regime': features.get('regime'),
        'signal_type': features.get('signal_type') or features.get('signal_v2'),
        'direction': direction,
        'market': features.get('market'),
    }
    if hour_utc is not None:
        feature_values['hour'] = hour_utc

    with _cache_lock:
        feats_cache = dict(_weights_cache['features'])
        combos_cache = dict(_weights_cache['combos'])
        syms_cache = dict(_weights_cache['symbols'])

    for fname, fval in feature_values.items():
        if fval is None or fval == '': continue
        fval = str(fval)
        # Prefer asset-specific se existir; fallback ALL
        for scope in [asset_norm, 'ALL']:
            adj = feats_cache.get((fname, fval, scope))
            if adj is not None and adj != 0:
                adj_total += adj
                breakdown.append({'src': f'feat:{fname}={fval}({scope})', 'pts': round(adj, 2)})
                break

    # ── 2. Combos cross-feature ──
    combos_check = [
        ('time_bucket×volume_bucket', f"{feature_values.get('time_bucket','')}/{feature_values.get('volume_bucket','')}"),
        ('time_bucket×direction',     f"{feature_values.get('time_bucket','')}/{direction}"),
        ('ema_alignment×direction',   f"{feature_values.get('ema_alignment','')}/{direction}"),
        ('atr_bucket×volume_bucket',  f"{feature_values.get('atr_bucket','')}/{feature_values.get('volume_bucket','')}"),
        ('regime×direction',          f"{feature_values.get('regime','')}/{direction}"),
        ('market×time_bucket',        f"{feature_values.get('market','')}/{feature_values.get('time_bucket','')}"),
        ('rsi_bucket×direction',      f"{feature_values.get('rsi_bucket','')}/{direction}"),
    ]
    if hour_utc is not None:
        combos_check.append(('hour×asset_type', f'{hour_utc}/{asset_norm}'))

    for ckey, cval in combos_check:
        if '/' in cval and cval not in ('/',):
            adj = combos_cache.get((ckey, cval))
            if adj is not None and adj != 0:
                adj_total += adj
                breakdown.append({'src': f'combo:{ckey}={cval}', 'pts': round(adj, 2)})

    # ── 3. Symbol skill ──
    sym_adj = syms_cache.get((symbol, asset_norm))
    if sym_adj is not None and sym_adj != 0:
        adj_total += sym_adj
        breakdown.append({'src': f'symbol:{symbol}({asset_norm})', 'pts': round(sym_adj, 2)})

    # Cap total adjustment pra evitar amplificacao extrema
    MAX_TOTAL_ADJ = float(os.environ.get('CALIBRATOR_MAX_TOTAL_ADJ', 25.0))
    adj_total = max(-MAX_TOTAL_ADJ, min(MAX_TOTAL_ADJ, adj_total))

    score_adjusted = max(0.0, min(100.0, float(score_original) + adj_total))

    return round(score_adjusted, 2), round(adj_total, 2), breakdown


def score_breakdown(features: dict, symbol: str, asset_type: str = 'stock',
                    direction: str = 'LONG', hour_utc: int = None) -> dict:
    """Retorna breakdown detalhado pra debug/endpoint."""
    _, adj, bd = apply_calibration(0, features, symbol, asset_type, direction, hour_utc)
    return {'adj_total': adj, 'components': bd}


def log_score_decision(symbol: str, asset_type: str, direction: str,
                       score_original: int, score_adjusted: float, adj_total: float,
                       breakdown: list, decision: str, trade_id: str = None):
    """Persiste A/B log (pra medir efetividade depois)."""
    conn = _get_conn()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO brain_score_ab_log
            (ts, symbol, asset_type, direction, score_original, score_adjusted,
             adj_total, adj_breakdown, decision, trade_id)
            VALUES (NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (symbol[:16], asset_type[:16], direction[:8],
             score_original, score_adjusted, adj_total,
             json.dumps(breakdown)[:8000], decision[:16], trade_id))
        conn.commit()
        cur.close()
    except Exception as e:
        log.debug(f'log_score_decision: {e}')
    finally:
        try: conn.close()
        except Exception: pass
