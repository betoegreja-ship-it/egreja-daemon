"""Brain Specialist Scorer — aplica pesos por MARKET, isolamento total.

Recebe: score_original, market, symbol, features, direction
Retorna: dict com score_adjusted, breakdown, market

Cache em memoria por 5min, particionado por market. SEM fallback cross-market —
se um market nao tem pesos pra uma feature, simplesmente ignora (adj=0).
"""
import os, time, json, logging
from threading import Lock

from . import MARKETS

log = logging.getLogger('egreja.brain_specialist.scorer')

# ── Cache em memoria, particionado por market ──
_weights_cache = {
    'ts': 0,
    'features': {},   # {market: {(fname, fval): adj}}
    'combos': {},     # {market: {(combo_key, combo_val): adj}}
    'symbols': {},    # {market: {symbol: adj}}
}
_cache_lock = Lock()
CACHE_TTL_S = int(os.environ.get('SPECIALIST_CACHE_TTL_S', 300))
MAX_TOTAL_ADJ = float(os.environ.get('SPECIALIST_MAX_TOTAL_ADJ', 25.0))


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


def _reload_cache() -> bool:
    """Recarrega cache do MySQL particionando por market. True se sucesso."""
    conn = _get_conn()
    if not conn: return False
    try:
        cur = conn.cursor(dictionary=True)

        feats = {m: {} for m in MARKETS}
        cur.execute("SELECT market, feature_name, feature_value, adj_pts FROM brain_specialist_feature_weights")
        for r in cur.fetchall():
            m = r['market']
            if m not in feats: feats[m] = {}
            feats[m][(r['feature_name'], str(r['feature_value']))] = float(r['adj_pts'] or 0)

        combos = {m: {} for m in MARKETS}
        cur.execute("SELECT market, combo_key, combo_value, adj_pts FROM brain_specialist_combo_weights")
        for r in cur.fetchall():
            m = r['market']
            if m not in combos: combos[m] = {}
            combos[m][(r['combo_key'], r['combo_value'])] = float(r['adj_pts'] or 0)

        symbols = {m: {} for m in MARKETS}
        cur.execute("SELECT market, symbol, symbol_skill_pts FROM brain_specialist_symbol_stats")
        for r in cur.fetchall():
            m = r['market']
            if m not in symbols: symbols[m] = {}
            symbols[m][r['symbol']] = float(r['symbol_skill_pts'] or 0)

        cur.close()
        with _cache_lock:
            _weights_cache['ts'] = time.time()
            _weights_cache['features'] = feats
            _weights_cache['combos'] = combos
            _weights_cache['symbols'] = symbols
        total_feats = sum(len(v) for v in feats.values())
        total_combos = sum(len(v) for v in combos.values())
        total_syms = sum(len(v) for v in symbols.values())
        log.info(f'[specialist.scorer] cache reloaded: {total_feats} features, '
                 f'{total_combos} combos, {total_syms} symbols (over {len(MARKETS)} markets)')
        return True
    except Exception as e:
        log.warning(f'[specialist.scorer] reload cache: {e}')
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


def get_active_weights(market: str = None) -> dict:
    """Snapshot dos pesos atuais. Se market=None, retorna por market."""
    _ensure_cache()
    with _cache_lock:
        if market and market in MARKETS:
            feats = _weights_cache['features'].get(market, {})
            combos = _weights_cache['combos'].get(market, {})
            syms = _weights_cache['symbols'].get(market, {})
            return {
                'market': market,
                'cache_age_s': int(time.time() - _weights_cache['ts']),
                'features_count': len(feats),
                'combos_count': len(combos),
                'symbols_count': len(syms),
                'top_boosts': sorted(feats.items(), key=lambda x: -x[1])[:10],
                'top_penalties': sorted(feats.items(), key=lambda x: x[1])[:10],
            }
        out = {'cache_age_s': int(time.time() - _weights_cache['ts']), 'markets': {}}
        for m in MARKETS:
            out['markets'][m] = {
                'features_count': len(_weights_cache['features'].get(m, {})),
                'combos_count': len(_weights_cache['combos'].get(m, {})),
                'symbols_count': len(_weights_cache['symbols'].get(m, {})),
            }
        return out


def apply_specialist_calibration(score_original: int, market: str, symbol: str,
                                  features: dict, direction: str = None) -> dict:
    """Aplica calibracao SPECIALIST (market-isolated). Sem fallback cross-market.

    Returns: {
        'score_original': int,
        'score_adjusted': float,
        'adj_total': float,
        'adj_breakdown': [{'src':..., 'pts':...}, ...],
        'market': market,
    }
    """
    if market not in MARKETS:
        return {
            'score_original': int(score_original),
            'score_adjusted': float(score_original),
            'adj_total': 0.0,
            'adj_breakdown': [],
            'market': market,
            'error': f'invalid_market:{market}',
        }

    if not os.environ.get('BRAIN_SPECIALIST_ENABLED', 'true').lower() == 'true':
        return {
            'score_original': int(score_original),
            'score_adjusted': float(score_original),
            'adj_total': 0.0,
            'adj_breakdown': [],
            'market': market,
        }

    _ensure_cache()
    breakdown = []
    adj_total = 0.0
    direction = direction or features.get('direction') or 'LONG'

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
    }
    hour_utc = features.get('hour') or features.get('hour_utc')
    if hour_utc is not None:
        feature_values['hour'] = hour_utc

    with _cache_lock:
        feats_cache = dict(_weights_cache['features'].get(market, {}))
        combos_cache = dict(_weights_cache['combos'].get(market, {}))
        syms_cache = dict(_weights_cache['symbols'].get(market, {}))

    # 1. Features univariates — APENAS market-specific, sem fallback
    for fname, fval in feature_values.items():
        if fval is None or fval == '': continue
        fval = str(fval)
        adj = feats_cache.get((fname, fval))
        if adj is not None and adj != 0:
            adj_total += adj
            breakdown.append({'src': f'feat:{fname}={fval}', 'pts': round(adj, 2)})

    # 2. Combos cross-feature
    combos_check = [
        ('time_bucket×volume_bucket', f"{feature_values.get('time_bucket','')}/{feature_values.get('volume_bucket','')}"),
        ('time_bucket×direction',     f"{feature_values.get('time_bucket','')}/{direction}"),
        ('ema_alignment×direction',   f"{feature_values.get('ema_alignment','')}/{direction}"),
        ('atr_bucket×volume_bucket',  f"{feature_values.get('atr_bucket','')}/{feature_values.get('volume_bucket','')}"),
        ('regime×direction',          f"{feature_values.get('regime','')}/{direction}"),
        ('rsi_bucket×direction',      f"{feature_values.get('rsi_bucket','')}/{direction}"),
    ]
    if hour_utc is not None:
        combos_check.append(('hour×direction', f'{hour_utc}/{direction}'))

    for ckey, cval in combos_check:
        if cval and cval not in ('/',) and not cval.startswith('/') and not cval.endswith('/'):
            adj = combos_cache.get((ckey, cval))
            if adj is not None and adj != 0:
                adj_total += adj
                breakdown.append({'src': f'combo:{ckey}={cval}', 'pts': round(adj, 2)})

    # 3. Symbol skill — apenas pelo market correto
    sym_adj = syms_cache.get(symbol)
    if sym_adj is not None and sym_adj != 0:
        adj_total += sym_adj
        breakdown.append({'src': f'symbol:{symbol}', 'pts': round(sym_adj, 2)})

    # Cap total
    adj_total = max(-MAX_TOTAL_ADJ, min(MAX_TOTAL_ADJ, adj_total))
    score_adjusted = max(0.0, min(100.0, float(score_original) + adj_total))

    return {
        'score_original': int(score_original),
        'score_adjusted': round(score_adjusted, 2),
        'adj_total': round(adj_total, 2),
        'adj_breakdown': breakdown,
        'market': market,
    }


def log_ab_decision(market: str, symbol: str, direction: str,
                    score_original: int, score_unified: float, score_specialist: float,
                    adj_unified: float = 0.0, adj_specialist: float = 0.0,
                    adj_breakdown_specialist: list = None,
                    decision_unified: str = None, decision_specialist: str = None,
                    trade_id: str = None):
    """Persiste A/B log: unified vs specialist, pra medir efetividade no tempo."""
    conn = _get_conn()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO brain_specialist_ab_log
            (ts, market, symbol, direction, score_original,
             score_unified, score_specialist, adj_unified, adj_specialist,
             adj_breakdown_specialist, decision_unified, decision_specialist, trade_id)
            VALUES (NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            ((market or '')[:8], (symbol or '')[:16], (direction or '')[:8],
             int(score_original or 0),
             round(float(score_unified or 0), 2), round(float(score_specialist or 0), 2),
             round(float(adj_unified or 0), 2), round(float(adj_specialist or 0), 2),
             json.dumps(adj_breakdown_specialist or [])[:8000],
             (decision_unified or '')[:16], (decision_specialist or '')[:16],
             (trade_id or None)))
        conn.commit()
        cur.close()
    except Exception as e:
        log.debug(f'[specialist.scorer] log_ab_decision: {e}')
    finally:
        try: conn.close()
        except Exception: pass
