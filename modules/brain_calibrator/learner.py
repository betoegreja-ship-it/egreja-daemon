"""Brain Learner — analisa trades historicos e calcula pesos.

Roda a cada 1h. Para cada (feature, valor):
  adj_pts = (win_rate_observado - baseline) * scale_factor

Confidence threshold: minimo de N samples (default 30) pra aprender.
Pesos baixa confidence sao penalizados via shrinkage.

Symbol stats: track record por simbolo (skill points).
Combos: cross-features (time × volume, ema × direction).
"""
import os, json, logging, time
from datetime import datetime
from collections import defaultdict

log = logging.getLogger('egreja.calibrator.learner')

# ── Config ──
LOOKBACK_DAYS = int(os.environ.get('CALIBRATOR_LOOKBACK_DAYS', 90))
MIN_SAMPLES = int(os.environ.get('CALIBRATOR_MIN_SAMPLES', 30))
SCALE_FACTOR = float(os.environ.get('CALIBRATOR_SCALE_FACTOR', 0.9))  # pts por 1pp de win-rate
MAX_ADJ_PTS = float(os.environ.get('CALIBRATOR_MAX_ADJ', 15.0))       # cap por feature
EWMA_ALPHA = float(os.environ.get('CALIBRATOR_EWMA_ALPHA', 0.3))      # peso novo vs historico

# Features universais sempre aprendidas
LEARNED_FEATURES = [
    'time_bucket', 'ema_alignment', 'volume_bucket', 'atr_bucket',
    'rsi_bucket', 'market_type', 'regime', 'signal_type',
]

# Combos cross-feature (pares que mostraram spread > 15pp na analise)
LEARNED_COMBOS = [
    ('time_bucket', 'volume_bucket'),
    ('time_bucket', 'direction'),
    ('ema_alignment', 'direction'),
    ('atr_bucket', 'volume_bucket'),
    ('regime', 'direction'),
    ('market', 'time_bucket'),
    ('rsi_bucket', 'direction'),
    ('hour', 'asset_type'),
]


def _get_conn():
    """Helper conn pool."""
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
    except Exception as e:
        log.warning(f'no DB: {e}')
        return None


def get_baseline_winrate(asset_scope='ALL', lookback_days=None) -> float:
    """Win-rate global como baseline pra calcular ajustes."""
    lookback_days = lookback_days or LOOKBACK_DAYS
    conn = _get_conn()
    if not conn: return 47.0
    try:
        cur = conn.cursor(dictionary=True)
        if asset_scope == 'ALL':
            cur.execute("""SELECT 100.0 * AVG(CASE WHEN pnl_pct>0 THEN 1 ELSE 0 END) AS wr
                           FROM trades WHERE status='CLOSED'
                           AND closed_at >= NOW() - INTERVAL %s DAY""", (lookback_days,))
        else:
            cur.execute("""SELECT 100.0 * AVG(CASE WHEN pnl_pct>0 THEN 1 ELSE 0 END) AS wr
                           FROM trades WHERE status='CLOSED' AND asset_type=%s
                           AND closed_at >= NOW() - INTERVAL %s DAY""", (asset_scope, lookback_days))
        r = cur.fetchone()
        cur.close()
        return float(r['wr']) if r and r['wr'] is not None else 47.0
    except Exception as e:
        log.warning(f'baseline_winrate: {e}')
        return 47.0
    finally:
        try: conn.close()
        except Exception: pass


def _shrinkage(adj_raw: float, n: int, n_min: int = 30) -> float:
    """Bayesian-style shrinkage: poucos samples -> peso menor."""
    if n < n_min: return 0.0
    # Confianca cresce com sqrt(n) e satura em ~500 samples
    confidence = min(1.0, (n / 500) ** 0.5)
    return adj_raw * confidence


def _clip(x: float, lo: float = -MAX_ADJ_PTS, hi: float = MAX_ADJ_PTS) -> float:
    return max(lo, min(hi, x))


def recalibrate_brain(lookback_days=None) -> dict:
    """Roda análise completa e atualiza tabelas. Idempotente.

    Returns: dict com stats da execucao
    """
    lookback_days = lookback_days or LOOKBACK_DAYS
    t0 = time.time()
    conn = _get_conn()
    if not conn:
        return {'error': 'no_db', 'duration_s': 0}

    try:
        cur = conn.cursor(dictionary=True)

        # 1. Pega trades
        cur.execute("""SELECT id, symbol, market, asset_type, direction,
                              pnl_pct, score, features_json,
                              HOUR(opened_at) AS hr, DAYOFWEEK(opened_at) AS dow
                       FROM trades
                       WHERE status='CLOSED' AND features_json IS NOT NULL
                         AND features_json != '' AND pnl_pct IS NOT NULL
                         AND closed_at >= NOW() - INTERVAL %s DAY""", (lookback_days,))
        rows = cur.fetchall()
        n_trades = len(rows)
        if n_trades < 100:
            log.warning(f'[calibrator] poucos trades ({n_trades}) — pulando')
            return {'skipped': True, 'n_trades': n_trades}

        # Parse features
        for r in rows:
            try: r['_feats'] = json.loads(r['features_json'])
            except Exception: r['_feats'] = {}
            r['_pnl'] = float(r['pnl_pct'] or 0)
            r['_win'] = 1 if r['_pnl'] > 0 else 0

        baseline = 100 * sum(r['_win'] for r in rows) / n_trades
        baseline_stock = 100 * sum(r['_win'] for r in rows if r['asset_type'] in ('stock','stocks')) / max(sum(1 for r in rows if r['asset_type'] in ('stock','stocks')), 1)
        baseline_crypto = 100 * sum(r['_win'] for r in rows if r['asset_type'] == 'crypto') / max(sum(1 for r in rows if r['asset_type'] == 'crypto'), 1)

        log.info(f'[calibrator] iniciando | trades={n_trades} | baseline={baseline:.2f}% '
                 f'| stock={baseline_stock:.2f}% | crypto={baseline_crypto:.2f}%')

        features_updated = 0
        combos_updated = 0
        symbols_updated = 0

        # ═══ 2. UNIVARIATE FEATURE WEIGHTS ═══
        for fname in LEARNED_FEATURES + ['hour', 'weekday', 'market', 'symbol', 'direction']:
            buckets_all = defaultdict(list)
            buckets_stk = defaultdict(list)
            buckets_cry = defaultdict(list)
            for r in rows:
                if fname == 'hour': v = r['hr']
                elif fname == 'weekday': v = r['dow']
                elif fname == 'market': v = r['market']
                elif fname == 'symbol': v = r['symbol']
                elif fname == 'direction': v = r['direction']
                else: v = r['_feats'].get(fname)
                if v is None or v == '': continue
                v = str(v)
                buckets_all[v].append(r)
                if r['asset_type'] in ('stock','stocks'): buckets_stk[v].append(r)
                elif r['asset_type'] == 'crypto': buckets_cry[v].append(r)

            for scope, base, buckets in [('ALL', baseline, buckets_all),
                                          ('stock', baseline_stock, buckets_stk),
                                          ('crypto', baseline_crypto, buckets_cry)]:
                for v, rs in buckets.items():
                    n = len(rs)
                    if n < MIN_SAMPLES: continue
                    wins = sum(r['_win'] for r in rs)
                    wr = 100 * wins / n
                    avg = sum(r['_pnl'] for r in rs) / n
                    adj_raw = (wr - base) * SCALE_FACTOR
                    adj = _clip(_shrinkage(adj_raw, n))
                    if abs(adj) < 0.3: continue
                    try:
                        cur.execute("""INSERT INTO brain_feature_weights
                            (feature_name, feature_value, asset_scope, n_samples, win_rate, avg_pnl_pct, adj_pts)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            ON DUPLICATE KEY UPDATE
                              n_samples=VALUES(n_samples), win_rate=VALUES(win_rate),
                              avg_pnl_pct=VALUES(avg_pnl_pct), adj_pts=VALUES(adj_pts),
                              version=version+1""",
                            (fname, v[:64], scope, n, round(wr,2), round(avg,4), round(adj,2)))
                        features_updated += 1
                    except Exception as e:
                        log.debug(f'feat insert {fname}/{v}/{scope}: {e}')

        # ═══ 3. COMBO WEIGHTS ═══
        for f1, f2 in LEARNED_COMBOS:
            buckets = defaultdict(list)
            for r in rows:
                v1 = r['hr'] if f1 == 'hour' else r['_feats'].get(f1) if f1 != 'direction' else r['direction']
                v2 = r['hr'] if f2 == 'hour' else r['_feats'].get(f2) if f2 != 'direction' else r['direction']
                if v1 is None or v2 is None: continue
                key = f'{v1}/{v2}'
                buckets[key].append(r)

            for combo_val, rs in buckets.items():
                n = len(rs)
                if n < MIN_SAMPLES: continue
                wins = sum(r['_win'] for r in rs)
                wr = 100 * wins / n
                avg = sum(r['_pnl'] for r in rs) / n
                adj_raw = (wr - baseline) * SCALE_FACTOR * 0.7  # combos pesam menos
                adj = _clip(_shrinkage(adj_raw, n))
                if abs(adj) < 0.5: continue
                combo_key = f'{f1}×{f2}'
                try:
                    cur.execute("""INSERT INTO brain_combo_weights
                        (combo_key, combo_value, asset_scope, n_samples, win_rate, avg_pnl_pct, adj_pts)
                        VALUES (%s,%s,'ALL',%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                          n_samples=VALUES(n_samples), win_rate=VALUES(win_rate),
                          avg_pnl_pct=VALUES(avg_pnl_pct), adj_pts=VALUES(adj_pts)""",
                        (combo_key[:128], combo_val[:128], n, round(wr,2), round(avg,4), round(adj,2)))
                    combos_updated += 1
                except Exception as e:
                    log.debug(f'combo insert {combo_key}/{combo_val}: {e}')

        # ═══ 4. SYMBOL STATS ═══
        sym_buckets = defaultdict(list)
        for r in rows: sym_buckets[(r['symbol'], r['asset_type'])].append(r)
        for (sym, atype), rs in sym_buckets.items():
            n = len(rs)
            if n < 10: continue  # min 10 trades pra ter symbol skill
            wins = sum(r['_win'] for r in rs)
            wr = 100 * wins / n
            avg = sum(r['_pnl'] for r in rs) / n
            total = sum(r['_pnl'] for r in rs)
            base = baseline_stock if atype in ('stock','stocks') else baseline_crypto
            adj_raw = (wr - base) * 0.8
            adj = _clip(_shrinkage(adj_raw, n, n_min=10), -10, 10)
            try:
                cur.execute("""INSERT INTO brain_symbol_stats
                    (symbol, asset_type, n_samples, win_rate, avg_pnl_pct, ewma_pnl_pct,
                     total_pnl_pct, symbol_skill_pts)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      n_samples=VALUES(n_samples), win_rate=VALUES(win_rate),
                      avg_pnl_pct=VALUES(avg_pnl_pct),
                      ewma_pnl_pct = (1 - %s) * COALESCE(ewma_pnl_pct, 0) + %s * VALUES(avg_pnl_pct),
                      total_pnl_pct=VALUES(total_pnl_pct),
                      symbol_skill_pts=VALUES(symbol_skill_pts)""",
                    (sym[:16], atype[:16], n, round(wr,2), round(avg,4), round(avg,4),
                     round(total,3), round(adj,2),
                     EWMA_ALPHA, EWMA_ALPHA))
                symbols_updated += 1
            except Exception as e:
                log.debug(f'symbol insert {sym}: {e}')

        # ═══ 5. CALIBRATION METRIC ═══
        # Mede o quanto features atuais ja preveem PnL (variance explained)
        wr_by_q = []
        scored = [(float(r.get('score') or 0), r['_win']) for r in rows if r.get('score')]
        if scored:
            scored.sort()
            for i in range(4):
                chunk = scored[int(len(scored)*i/4):int(len(scored)*(i+1)/4)]
                if chunk:
                    wr_q = sum(w for _, w in chunk) / len(chunk)
                    wr_by_q.append(wr_q)
            calibration_quality = max(wr_by_q) - min(wr_by_q) if len(wr_by_q) >= 2 else 0.0
        else:
            calibration_quality = 0.0

        # ═══ 6. LOG HISTORY ═══
        duration = int(time.time() - t0)
        # Avg adj absolute
        cur.execute("SELECT AVG(ABS(adj_pts)) AS m FROM brain_feature_weights WHERE last_updated >= NOW() - INTERVAL 5 MINUTE")
        avg_adj_r = cur.fetchone()
        avg_adj = float(avg_adj_r['m']) if avg_adj_r and avg_adj_r['m'] else 0.0

        cur.execute("""INSERT INTO brain_calibration_history
            (run_ts, lookback_days, n_trades_used, baseline_wr,
             features_updated, combos_updated, symbols_updated,
             calibration_quality, avg_adj_pts, notes, duration_seconds)
            VALUES (NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (lookback_days, n_trades, round(baseline,2),
             features_updated, combos_updated, symbols_updated,
             round(calibration_quality,4), round(avg_adj,2),
             f'scale={SCALE_FACTOR} min_n={MIN_SAMPLES} ewma_alpha={EWMA_ALPHA}',
             duration))

        conn.commit()
        cur.close()

        log.info(f'[calibrator] OK | trades={n_trades} feats={features_updated} '
                 f'combos={combos_updated} syms={symbols_updated} '
                 f'baseline={baseline:.2f}% calib_quality={calibration_quality:.4f} '
                 f'dur={duration}s')

        return {
            'n_trades': n_trades,
            'baseline_wr': round(baseline, 2),
            'baseline_stock': round(baseline_stock, 2),
            'baseline_crypto': round(baseline_crypto, 2),
            'features_updated': features_updated,
            'combos_updated': combos_updated,
            'symbols_updated': symbols_updated,
            'calibration_quality': round(calibration_quality, 4),
            'avg_adj_pts': round(avg_adj, 2),
            'duration_seconds': duration,
        }
    except Exception as e:
        log.error(f'[calibrator] recalibrate_brain: {e}')
        try: conn.rollback()
        except Exception: pass
        import traceback; traceback.print_exc()
        return {'error': str(e), 'duration_s': int(time.time()-t0)}
    finally:
        try: conn.close()
        except Exception: pass
