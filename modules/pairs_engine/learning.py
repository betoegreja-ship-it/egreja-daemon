"""Pairs learning engine — coleta, agrega, descobre padroes.

3 funcoes principais:

1. snapshot_persist(signal) — chamado a cada scan (30s) pelo scanner.
   Grava observacao bruta em pairs_snapshots.

2. recalibrate_pair(pair_id) — chamado a cada 1h pelo worker.
   Recalcula ADF, half-life, beta com janela rolling de 60d
   a partir de pairs_history_daily. Persiste em pairs_recalibration_history.

3. detect_events(signal, prev_signal) — chamado a cada scan.
   Detecta z_crossings, blowups, breakdowns. Grava em pairs_events.

4. generate_insights(pair_id) — chamado a cada 4h.
   Auto-descobre patterns (melhor hora, regime, etc) por par.

Tudo persistido. NUNCA DELETE. Histórico = ativo do sistema.
"""
import os, json, logging, math, time
from collections import defaultdict
from datetime import datetime, timedelta

log = logging.getLogger('egreja.pairs.learning')

# Throttling: nao salvar TODO snapshot — taxa configuravel
SNAPSHOT_PERSIST_INTERVAL_S = int(os.environ.get('PAIRS_SNAPSHOT_INTERVAL_S', 120))  # 2min default
_last_snapshot_ts = {}  # pair_id -> last ts


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
            connection_timeout=15,
        )
    except Exception: return None


# ═══════════════════════════════════════════════════════════════
# 1. SNAPSHOT PERSISTENCE
# ═══════════════════════════════════════════════════════════════
def snapshot_persist(signal: dict) -> bool:
    """Persiste observacao em pairs_snapshots. Throttle 2min por par."""
    if not signal or signal.get('z_score') is None:
        return False
    pid = signal.get('pair_id')
    if not pid: return False
    now = time.time()
    last = _last_snapshot_ts.get(pid, 0)
    # Sempre persiste eventos importantes (ENTRY, AVOID, CONVERGED) ignorando throttle
    important = signal.get('action') in ('ENTRY', 'AVOID', 'CONVERGED')
    if not important and (now - last) < SNAPSHOT_PERSIST_INTERVAL_S:
        return False
    conn = _get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO pairs_snapshots
            (ts, pair_id, price_a, price_b, spread, z_score, mean_60d, stdev_60d,
             correlation_60d, hedge_ratio, action)
            VALUES (NOW(3), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (pid, signal.get('price_a'), signal.get('price_b'),
             signal.get('spread_current'), signal.get('z_score'),
             signal.get('spread_mean_60d'), signal.get('spread_stdev_60d'),
             signal.get('correlation_60d'), signal.get('hedge_ratio'),
             signal.get('action')))
        conn.commit()
        cur.close()
        _last_snapshot_ts[pid] = now
        return True
    except Exception as e:
        log.debug(f'snapshot_persist {pid}: {e}')
        try: conn.rollback()
        except: pass
        return False
    finally:
        try: conn.close()
        except: pass


# ═══════════════════════════════════════════════════════════════
# 2. EVENT DETECTION
# ═══════════════════════════════════════════════════════════════
_last_signal_cache = {}  # pair_id -> last signal (in-memory)

def detect_events(signal: dict) -> list:
    """Detecta anomalias comparando com signal anterior. Persiste em pairs_events.
    Retorna lista dos events detectados."""
    if not signal or signal.get('z_score') is None: return []
    pid = signal.get('pair_id')
    events = []

    prev = _last_signal_cache.get(pid)
    z = float(signal['z_score'])
    corr = float(signal.get('correlation_60d') or 0)

    # Z-score crossing zero (mean reversion)
    if prev and prev.get('z_score') is not None:
        prev_z = float(prev['z_score'])
        if (prev_z > 0.5 and z < -0.5) or (prev_z < -0.5 and z > 0.5):
            events.append({'type': 'Z_CROSS_ZERO', 'severity': 'medium',
                           'detail': f'prev_z={prev_z:+.2f} → z={z:+.2f}'})

    # Blowup: |z| > 3
    if abs(z) > 3.0:
        events.append({'type': 'Z_BLOWUP', 'severity': 'high' if abs(z) > 4 else 'medium',
                       'detail': f'z={z:+.2f} — anomalia 3+ desvios'})

    # Breakdown: correlation rompida (correlação < 0.3 quando antes era > 0.6)
    if prev and prev.get('correlation_60d'):
        prev_corr = float(prev['correlation_60d'])
        if prev_corr > 0.6 and corr < 0.3:
            events.append({'type': 'CORRELATION_BREAKDOWN', 'severity': 'high',
                           'detail': f'corr {prev_corr:.2f} → {corr:.2f}'})

    # Entry signal
    if signal.get('action') == 'ENTRY':
        events.append({'type': 'ENTRY_SIGNAL', 'severity': 'high',
                       'detail': f'direction={signal.get("direction")} z={z:+.2f}'})

    _last_signal_cache[pid] = signal

    # Persistir
    if events:
        conn = _get_conn()
        if conn:
            try:
                cur = conn.cursor()
                for e in events:
                    cur.execute("""INSERT INTO pairs_events
                        (ts, pair_id, event_type, severity, z_score, details)
                        VALUES (NOW(3), %s, %s, %s, %s, %s)""",
                        (pid, e['type'], e['severity'], z, e['detail']))
                conn.commit()
                cur.close()
                for e in events:
                    log.info(f'[EVENT] {pid} {e["type"]} {e["severity"]}: {e["detail"]}')
            except Exception as ex:
                log.debug(f'detect_events persist {pid}: {ex}')
                try: conn.rollback()
                except: pass
            finally:
                try: conn.close()
                except: pass
    return events


# ═══════════════════════════════════════════════════════════════
# 3. PAIR RECALIBRATION (rolling ADF, half-life, beta)
# ═══════════════════════════════════════════════════════════════
def _adf_tstat_simple(spread: list) -> float:
    """ADF simplificado: t-stat de phi em delta_x = phi * x_lag + e
    Mais negativo = mais estacionario (cointegrado)."""
    if len(spread) < 30: return 0.0
    try:
        n = len(spread)
        x_lag = spread[:-1]
        dx = [spread[i+1] - spread[i] for i in range(n-1)]
        x_mean = sum(x_lag) / len(x_lag)
        dx_mean = sum(dx) / len(dx)
        num = sum((x_lag[i] - x_mean) * (dx[i] - dx_mean) for i in range(len(dx)))
        den = sum((x_lag[i] - x_mean) ** 2 for i in range(len(x_lag)))
        if abs(den) < 1e-9: return 0.0
        phi = num / den
        residuals = [dx[i] - phi * x_lag[i] for i in range(len(dx))]
        rss = sum(r ** 2 for r in residuals)
        var = rss / max(len(dx) - 2, 1)
        se = math.sqrt(var / den) if var > 0 else 1
        return phi / se if se > 0 else 0.0
    except Exception: return 0.0


def _half_life(spread: list) -> float:
    """Half-life via Ornstein-Uhlenbeck: HL = -ln(2) / phi"""
    if len(spread) < 30: return 999
    try:
        n = len(spread)
        x_lag = spread[:-1]
        dx = [spread[i+1] - spread[i] for i in range(n-1)]
        x_mean = sum(x_lag) / len(x_lag)
        num = sum((x_lag[i] - x_mean) * (dx[i] - sum(dx)/len(dx)) for i in range(len(dx)))
        den = sum((x_lag[i] - x_mean) ** 2 for i in range(len(x_lag)))
        if abs(den) < 1e-9: return 999
        phi = num / den
        if phi >= 0: return 999  # nao mean-reverting
        return -math.log(2) / phi
    except Exception: return 999


def _regime(adf_t: float, half_life: float, corr: float) -> str:
    """Classifica regime baseado em metricas."""
    if adf_t < -3.0 and half_life < 25 and corr > 0.6: return 'TRENDING_MEAN_REVERT'
    if adf_t < -2.5 and half_life < 40: return 'MEAN_REVERT_SLOW'
    if half_life > 80 or corr < 0.3: return 'BROKEN'
    if abs(adf_t) < 1.5: return 'RANDOM_WALK'
    return 'TRANSITIONAL'


def _tier_recommendation(adf_t: float, half_life: float, corr: float) -> str:
    """Recomenda tier baseado em metricas atuais."""
    if adf_t < -3.0 and half_life <= 21 and corr >= 0.5: return 'A'
    if adf_t < -2.5 and half_life <= 35: return 'B'
    if half_life > 60 or corr < 0.3: return 'WATCH'
    return 'B'


def recalibrate_pair(pair_id: str, window_days: int = 60) -> dict:
    """Recalcula ADF/half-life/beta a partir de pairs_history_daily."""
    from .config import get_pair
    from .zscore import calc_spread_series, calc_hedge_ratio, calc_correlation
    cfg = get_pair(pair_id)
    if not cfg: return {}

    # Pega historico
    try:
        from . import persistence as _persist
        ha = _persist.load_history_from_db(cfg['leg_a'], days=window_days + 10)
        hb = _persist.load_history_from_db(cfg['leg_b'], days=window_days + 10)
    except Exception as e:
        log.debug(f'recalibrate {pair_id}: load history {e}')
        return {}

    # Alinhar por data
    da = {h['date']: h['close'] for h in ha}
    db = {h['date']: h['close'] for h in hb}
    common = sorted(set(da.keys()) & set(db.keys()))
    if len(common) < 30:
        return {'pair_id': pair_id, 'error': 'insufficient_history', 'n': len(common)}
    prices_a = [da[d] for d in common[-window_days:]]
    prices_b = [db[d] for d in common[-window_days:]]

    # Calcular metricas
    pair_type = cfg.get('pair_type', 'SECTORIAL')
    method = 'pct_diff' if pair_type == 'CLASSES' else 'log_ratio'
    spread = calc_spread_series(prices_a, prices_b, method=method)
    beta = calc_hedge_ratio(prices_a, prices_b, window=min(len(prices_a), window_days)) or cfg.get('beta_a_to_b', 1.0)
    ret_corr = calc_correlation(prices_a, prices_b, window=min(len(prices_a), window_days)) or 0
    # Price correlation
    n = len(prices_a)
    if n > 5:
        mean_a = sum(prices_a)/n
        mean_b = sum(prices_b)/n
        cov_p = sum((prices_a[i]-mean_a)*(prices_b[i]-mean_b) for i in range(n))/n
        var_pa = sum((p-mean_a)**2 for p in prices_a)/n
        var_pb = sum((p-mean_b)**2 for p in prices_b)/n
        denom = (var_pa * var_pb) ** 0.5
        price_corr = cov_p / denom if denom > 1e-9 else 0
    else: price_corr = 0

    adf_t = _adf_tstat_simple(spread)
    hl = _half_life(spread)
    spread_mean = sum(spread)/len(spread) if spread else 0
    sp_std = math.sqrt(sum((x-spread_mean)**2 for x in spread)/len(spread)) if spread else 0

    regime = _regime(adf_t, hl, ret_corr)
    tier_rec = _tier_recommendation(adf_t, hl, ret_corr)

    result = {
        'pair_id': pair_id, 'window_days': len(prices_a),
        'adf_tstat': round(adf_t, 3), 'half_life_days': round(hl, 2),
        'hedge_beta': round(beta, 4), 'hedge_alpha': 0.0,
        'return_corr': round(ret_corr, 4), 'price_corr': round(price_corr, 4),
        'spread_mean': round(spread_mean, 6), 'spread_stdev': round(sp_std, 6),
        'regime': regime, 'tier_recommended': tier_rec,
    }

    # Persistir
    conn = _get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""INSERT INTO pairs_recalibration_history
                (ts, pair_id, window_days, adf_tstat, half_life_days,
                 hedge_beta, hedge_alpha, return_corr, price_corr,
                 spread_mean, spread_stdev, regime, tier_recommended)
                VALUES (NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (pair_id, result['window_days'], result['adf_tstat'],
                 result['half_life_days'], result['hedge_beta'], result['hedge_alpha'],
                 result['return_corr'], result['price_corr'],
                 result['spread_mean'], result['spread_stdev'],
                 result['regime'], result['tier_recommended']))
            conn.commit()
            cur.close()
        except Exception as e:
            log.debug(f'recalibrate persist {pair_id}: {e}')
        finally:
            try: conn.close()
            except: pass
    return result


# ═══════════════════════════════════════════════════════════════
# 4. INSIGHTS GENERATION
# ═══════════════════════════════════════════════════════════════
def generate_insights(pair_id: str) -> dict:
    """Analisa snapshots e signals dos ultimos 7d, descobre patterns."""
    conn = _get_conn()
    if not conn: return {}
    out = {'pair_id': pair_id, 'insights': {}}
    try:
        cur = conn.cursor(dictionary=True)

        # Best hour to enter (baseado em snapshots ENTRY)
        cur.execute("""SELECT HOUR(ts) AS hr, COUNT(*) AS n
                       FROM pairs_snapshots
                       WHERE pair_id=%s AND action='ENTRY'
                         AND ts >= NOW() - INTERVAL 30 DAY
                       GROUP BY hr ORDER BY n DESC LIMIT 3""", (pair_id,))
        best_hours = cur.fetchall()
        if best_hours:
            out['insights']['best_entry_hours_utc'] = [
                {'hour': h['hr'], 'n_signals': h['n']} for h in best_hours
            ]

        # Atual regime
        cur.execute("""SELECT regime, tier_recommended, adf_tstat, half_life_days,
                              return_corr FROM pairs_recalibration_history
                       WHERE pair_id=%s ORDER BY ts DESC LIMIT 1""", (pair_id,))
        last_recal = cur.fetchone()
        if last_recal:
            out['insights']['current_regime'] = last_recal['regime']
            out['insights']['tier_recommended'] = last_recal['tier_recommended']
            out['insights']['adf_tstat'] = float(last_recal['adf_tstat']) if last_recal['adf_tstat'] else 0
            out['insights']['half_life_days'] = float(last_recal['half_life_days']) if last_recal['half_life_days'] else 999
            out['insights']['return_corr_60d'] = float(last_recal['return_corr']) if last_recal['return_corr'] else 0

        # Evolucao da correlacao (degradou? melhorou?)
        cur.execute("""SELECT AVG(return_corr) AS recent FROM pairs_recalibration_history
                       WHERE pair_id=%s AND ts >= NOW() - INTERVAL 7 DAY""", (pair_id,))
        r = cur.fetchone()
        recent_corr = float(r['recent']) if r and r['recent'] else None
        cur.execute("""SELECT AVG(return_corr) AS older FROM pairs_recalibration_history
                       WHERE pair_id=%s AND ts < NOW() - INTERVAL 7 DAY
                         AND ts >= NOW() - INTERVAL 30 DAY""", (pair_id,))
        r = cur.fetchone()
        older_corr = float(r['older']) if r and r['older'] else None
        if recent_corr and older_corr:
            delta = recent_corr - older_corr
            if abs(delta) > 0.05:
                out['insights']['correlation_trend'] = {
                    'recent_7d': round(recent_corr, 4),
                    'older_30d': round(older_corr, 4),
                    'delta': round(delta, 4),
                    'direction': 'IMPROVING' if delta > 0 else 'DEGRADING'
                }

        # Eventos recentes
        cur.execute("""SELECT event_type, COUNT(*) AS n FROM pairs_events
                       WHERE pair_id=%s AND ts >= NOW() - INTERVAL 7 DAY
                       GROUP BY event_type""", (pair_id,))
        events = cur.fetchall()
        if events:
            out['insights']['events_7d'] = {e['event_type']: e['n'] for e in events}

        # Volatilidade do z (z range)
        cur.execute("""SELECT MIN(z_score) AS minz, MAX(z_score) AS maxz,
                              AVG(z_score) AS avgz, STDDEV(z_score) AS stdz,
                              COUNT(*) AS n
                       FROM pairs_snapshots
                       WHERE pair_id=%s AND ts >= NOW() - INTERVAL 7 DAY""", (pair_id,))
        zs = cur.fetchone()
        if zs and zs['n']:
            out['insights']['z_distribution_7d'] = {
                'min': float(zs['minz']) if zs['minz'] else 0,
                'max': float(zs['maxz']) if zs['maxz'] else 0,
                'avg': float(zs['avgz']) if zs['avgz'] else 0,
                'stdev': float(zs['stdz']) if zs['stdz'] else 0,
                'n_observations': int(zs['n']),
            }

        cur.close()

        # Persistir insights
        cur = conn.cursor()
        for k, v in out['insights'].items():
            try:
                cur.execute("""INSERT INTO pairs_insights
                    (pair_id, insight_key, insight_value, n_samples)
                    VALUES (%s, %s, %s, 0)
                    ON DUPLICATE KEY UPDATE
                      insight_value=VALUES(insight_value),
                      last_updated=NOW()""",
                    (pair_id, k, json.dumps(v, default=str)[:8000]))
            except Exception: pass
        conn.commit()
        cur.close()
    except Exception as e:
        log.debug(f'generate_insights {pair_id}: {e}')
    finally:
        try: conn.close()
        except: pass
    return out
