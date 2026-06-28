"""Brain Specialist Learner — recalibra UM market por vez (B3/NYSE/CRYPTO).

Mesma logica do brain_calibrator.learner mas com chave MARKET e isolamento total.
Cada market gera seus proprios pesos (feature/combo/symbol) — sem fallback cross-market.

Roda hora-a-hora via worker. Para cada (market, feature, valor):
  adj_pts = blend(adj_wr, adj_ev) com shrinkage Bayesiano

MIN_SAMPLES=10 por market (mais baixo que o calibrator unified pq cada market
tem menos trades). Shrinkage compensa baixa amostra.
"""
import os, json, logging, time
from datetime import datetime
from collections import defaultdict

from . import MARKETS, detect_market

log = logging.getLogger('egreja.brain_specialist.learner')

# ── Config ──
LOOKBACK_DAYS = int(os.environ.get('SPECIALIST_LOOKBACK_DAYS', 30))
MIN_SAMPLES = int(os.environ.get('SPECIALIST_MIN_SAMPLES', 10))
SCALE_FACTOR = float(os.environ.get('SPECIALIST_SCALE_FACTOR', 0.9))
MAX_ADJ_PTS = float(os.environ.get('SPECIALIST_MAX_ADJ', 15.0))
EWMA_ALPHA = float(os.environ.get('SPECIALIST_EWMA_ALPHA', 0.3))

LEARNED_FEATURES = [
    'time_bucket', 'ema_alignment', 'volume_bucket', 'atr_bucket',
    'rsi_bucket', 'market_type', 'regime', 'signal_type',
]

LEARNED_COMBOS = [
    ('time_bucket', 'volume_bucket'),
    ('time_bucket', 'direction'),
    ('ema_alignment', 'direction'),
    ('atr_bucket', 'volume_bucket'),
    ('regime', 'direction'),
    ('rsi_bucket', 'direction'),
    ('hour', 'direction'),
]


def _get_conn():
    """Helper conn pool — tenta api_server primeiro, fallback mysql.connector."""
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


def _shrinkage(adj_raw: float, n: int, n_min: int = None) -> float:
    """Bayesian-style shrinkage: poucos samples -> peso menor."""
    n_min = n_min if n_min is not None else MIN_SAMPLES
    if n < n_min: return 0.0
    confidence = min(1.0, (n / 500) ** 0.5)
    return adj_raw * confidence


def _clip(x: float, lo: float = None, hi: float = None) -> float:
    lo = -MAX_ADJ_PTS if lo is None else lo
    hi = MAX_ADJ_PTS if hi is None else hi
    return max(lo, min(hi, x))


def _fetch_market_trades(conn, market: str, lookback_days: int) -> list:
    """Pega trades sanitizados e filtra em Python por market (via detect_market)."""
    cur = conn.cursor(dictionary=True)
    # Mesma sanitizacao do calibrator unified:
    cur.execute("""SELECT id, symbol, market, asset_type, direction,
                          pnl, pnl_pct, score, features_json, close_reason,
                          opened_at, closed_at,
                          HOUR(opened_at) AS hr, DAYOFWEEK(opened_at) AS dow
                   FROM trades
                   WHERE status='CLOSED' AND features_json IS NOT NULL
                     AND features_json != '' AND pnl_pct IS NOT NULL
                     AND pnl IS NOT NULL AND pnl != 0
                     AND ABS(pnl_pct) < 50
                     AND COALESCE(close_reason,'') NOT IN
                         ('VOIDED','price_zero_bug_fix','price_correction_bug_v108','MANUAL_CLOSE')
                     AND closed_at >= NOW() - INTERVAL %s DAY""", (lookback_days,))
    rows = cur.fetchall()
    cur.close()
    # Filtra por market usando detect_market (NAO confia no trades.market que pode ser nulo)
    filtered = []
    for r in rows:
        m = detect_market(r.get('symbol'), r.get('asset_type'))
        if m != market:
            continue
        filtered.append(r)
    return filtered


def recalibrate_specialist(market: str, lookback_days: int = None) -> dict:
    """Recalibra UM market (B3, NYSE ou CRYPTO). Isolamento total: nada cross-market.

    Returns: dict com stats da execucao.
    """
    if market not in MARKETS:
        return {'error': f'invalid_market:{market}', 'duration_s': 0}

    lookback_days = lookback_days or LOOKBACK_DAYS
    t0 = time.time()
    conn = _get_conn()
    if not conn:
        return {'error': 'no_db', 'duration_s': 0, 'market': market}

    try:
        rows = _fetch_market_trades(conn, market, lookback_days)
        n_trades = len(rows)
        if n_trades < MIN_SAMPLES:
            log.warning(f'[specialist:{market}] poucos trades ({n_trades}) — pulando')
            return {'skipped': True, 'n_trades': n_trades, 'market': market}

        cur = conn.cursor(dictionary=True)

        # Parse features
        for r in rows:
            try: r['_feats'] = json.loads(r['features_json'])
            except Exception: r['_feats'] = {}
            r['_pnl'] = float(r['pnl_pct'] or 0)
            r['_win'] = 1 if r['_pnl'] > 0 else 0

        baseline = 100 * sum(r['_win'] for r in rows) / n_trades

        log.info(f'[specialist:{market}] iniciando | trades={n_trades} | baseline={baseline:.2f}%')

        features_updated = 0
        combos_updated = 0
        symbols_updated = 0

        # ═══ 1. UNIVARIATE FEATURE WEIGHTS ═══
        for fname in LEARNED_FEATURES + ['hour', 'weekday', 'direction']:
            buckets = defaultdict(list)
            for r in rows:
                if fname == 'hour': v = r['hr']
                elif fname == 'weekday': v = r['dow']
                elif fname == 'direction': v = r['direction']
                else: v = r['_feats'].get(fname)
                # [P0-FIX 28-jun-2026] Skip UNKNOWN/null/empty — eram aprendidos
                # como "sinal" mas sao apenas ausencia de dado (bug RSI=50 corrompia tudo)
                if v is None or v == '': continue
                v_str = str(v).strip()
                if v_str.upper() in ('UNKNOWN', 'NONE', 'NULL', 'N/A', ''): continue
                buckets[v_str].append(r)

            for v, rs in buckets.items():
                n = len(rs)
                if n < MIN_SAMPLES: continue
                wins = sum(r['_win'] for r in rs)
                wr = 100 * wins / n
                avg = sum(r['_pnl'] for r in rs) / n
                win_pnls = [r['_pnl'] for r in rs if r['_pnl'] > 0]
                loss_pnls = [r['_pnl'] for r in rs if r['_pnl'] <= 0]
                avg_win_pct = sum(win_pnls)/len(win_pnls) if win_pnls else 0.0
                avg_loss_pct = sum(loss_pnls)/len(loss_pnls) if loss_pnls else 0.0
                p_win = wr/100
                p_loss = 1 - p_win
                ev = p_win * avg_win_pct + p_loss * avg_loss_pct
                adj_wr = (wr - baseline) * SCALE_FACTOR
                adj_ev = ev * 8.0
                adj_raw = 0.6 * adj_wr + 0.4 * adj_ev
                adj = _clip(_shrinkage(adj_raw, n))
                if abs(adj) < 0.3: continue
                try:
                    cur.execute("""INSERT INTO brain_specialist_feature_weights
                        (market, feature_name, feature_value, n_samples,
                         win_rate, avg_pnl_pct, avg_win_pct, avg_loss_pct,
                         expected_value, adj_pts)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                          n_samples=VALUES(n_samples), win_rate=VALUES(win_rate),
                          avg_pnl_pct=VALUES(avg_pnl_pct),
                          avg_win_pct=VALUES(avg_win_pct),
                          avg_loss_pct=VALUES(avg_loss_pct),
                          expected_value=VALUES(expected_value),
                          adj_pts=VALUES(adj_pts), version=version+1""",
                        (market, fname, v[:64], n, round(wr,2), round(avg,4),
                         round(avg_win_pct,4), round(avg_loss_pct,4),
                         round(ev,4), round(adj,2)))
                    features_updated += 1
                except Exception as e:
                    log.debug(f'[specialist:{market}] feat insert {fname}/{v}: {e}')

        # ═══ 2. COMBO WEIGHTS ═══
        for f1, f2 in LEARNED_COMBOS:
            buckets = defaultdict(list)
            for r in rows:
                if f1 == 'hour': v1 = r['hr']
                elif f1 == 'direction': v1 = r['direction']
                else: v1 = r['_feats'].get(f1)
                if f2 == 'hour': v2 = r['hr']
                elif f2 == 'direction': v2 = r['direction']
                else: v2 = r['_feats'].get(f2)
                # [P0-FIX 28-jun-2026] Skip UNKNOWN nos combos tambem
                if v1 is None or v2 is None: continue
                v1s = str(v1).strip(); v2s = str(v2).strip()
                if v1s.upper() in ('UNKNOWN', 'NONE', 'NULL', 'N/A', ''): continue
                if v2s.upper() in ('UNKNOWN', 'NONE', 'NULL', 'N/A', ''): continue
                key = f'{v1s}/{v2s}'
                buckets[key].append(r)

            for combo_val, rs in buckets.items():
                n = len(rs)
                if n < MIN_SAMPLES: continue
                wins = sum(r['_win'] for r in rs)
                wr = 100 * wins / n
                avg = sum(r['_pnl'] for r in rs) / n
                win_pnls = [r['_pnl'] for r in rs if r['_pnl'] > 0]
                loss_pnls = [r['_pnl'] for r in rs if r['_pnl'] <= 0]
                avg_win = sum(win_pnls)/len(win_pnls) if win_pnls else 0.0
                avg_loss = sum(loss_pnls)/len(loss_pnls) if loss_pnls else 0.0
                p_w = wr/100; p_l = 1-p_w
                ev = p_w * avg_win + p_l * avg_loss
                adj_wr = (wr - baseline) * SCALE_FACTOR * 0.7
                adj_ev = ev * 7.0
                adj_raw = 0.6 * adj_wr + 0.4 * adj_ev
                adj = _clip(_shrinkage(adj_raw, n))
                if abs(adj) < 0.5: continue
                combo_key = f'{f1}×{f2}'
                try:
                    cur.execute("""INSERT INTO brain_specialist_combo_weights
                        (market, combo_key, combo_value, n_samples,
                         win_rate, avg_pnl_pct, expected_value, adj_pts)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                          n_samples=VALUES(n_samples), win_rate=VALUES(win_rate),
                          avg_pnl_pct=VALUES(avg_pnl_pct),
                          expected_value=VALUES(expected_value),
                          adj_pts=VALUES(adj_pts)""",
                        (market, combo_key[:128], combo_val[:128], n, round(wr,2),
                         round(avg,4), round(ev,4), round(adj,2)))
                    combos_updated += 1
                except Exception as e:
                    log.debug(f'[specialist:{market}] combo insert {combo_key}/{combo_val}: {e}')

        # ═══ 3. SYMBOL STATS ═══
        sym_buckets = defaultdict(list)
        for r in rows: sym_buckets[r['symbol']].append(r)

        from datetime import datetime as _dt
        now_utc = _dt.utcnow()
        def _trade_weight(t):
            try:
                ca = t.get('closed_at') or t.get('opened_at')
                if not ca: return 1.0
                if isinstance(ca, str): ca = _dt.fromisoformat(ca.replace('Z',''))
                days_old = (now_utc - ca).total_seconds() / 86400
                return 0.5 ** (max(0, days_old) / 30.0)
            except Exception: return 1.0

        for sym, rs in sym_buckets.items():
            n = len(rs)
            if n < MIN_SAMPLES: continue
            wins = sum(r['_win'] for r in rs)
            wr = 100 * wins / n
            avg = sum(r['_pnl'] for r in rs) / n
            total = sum(r['_pnl'] for r in rs)
            weights = [_trade_weight(r) for r in rs]
            sum_w = sum(weights) or 1
            wr_recent = 100 * sum(w * r['_win'] for w, r in zip(weights, rs)) / sum_w
            win_pnls = [r['_pnl'] for r in rs if r['_pnl'] > 0]
            loss_pnls = [r['_pnl'] for r in rs if r['_pnl'] <= 0]
            avg_win = sum(win_pnls)/len(win_pnls) if win_pnls else 0.0
            avg_loss = sum(loss_pnls)/len(loss_pnls) if loss_pnls else 0.0
            p_w = wr/100; p_l = 1-p_w
            sym_ev = p_w * avg_win + p_l * avg_loss
            adj_wr = (wr_recent - baseline) * 0.8
            adj_ev = sym_ev * 6.0
            adj_raw = 0.5 * adj_wr + 0.5 * adj_ev
            adj = _clip(_shrinkage(adj_raw, n, n_min=MIN_SAMPLES), -10, 10)
            try:
                cur.execute("""INSERT INTO brain_specialist_symbol_stats
                    (market, symbol, n_samples, win_rate, avg_pnl_pct, ewma_pnl_pct,
                     total_pnl_pct, symbol_skill_pts, expected_value)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      n_samples=VALUES(n_samples), win_rate=VALUES(win_rate),
                      avg_pnl_pct=VALUES(avg_pnl_pct),
                      ewma_pnl_pct = (1 - %s) * COALESCE(ewma_pnl_pct, 0) + %s * VALUES(avg_pnl_pct),
                      total_pnl_pct=VALUES(total_pnl_pct),
                      symbol_skill_pts=VALUES(symbol_skill_pts),
                      expected_value=VALUES(expected_value)""",
                    (market, sym[:16], n, round(wr,2), round(avg,4), round(avg,4),
                     round(total,3), round(adj,2), round(sym_ev,4),
                     EWMA_ALPHA, EWMA_ALPHA))
                symbols_updated += 1
            except Exception as e:
                log.debug(f'[specialist:{market}] symbol insert {sym}: {e}')

        # ═══ 4. CALIBRATION QUALITY ═══
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

        # ═══ 5. HISTORY LOG ═══
        duration = int(time.time() - t0)
        cur.execute("""SELECT AVG(ABS(adj_pts)) AS m FROM brain_specialist_feature_weights
                       WHERE market = %s AND last_updated >= NOW() - INTERVAL 5 MINUTE""", (market,))
        avg_adj_r = cur.fetchone()
        avg_adj = float(avg_adj_r['m']) if avg_adj_r and avg_adj_r['m'] else 0.0

        cur.execute("""INSERT INTO brain_specialist_calibration_history
            (market, run_ts, lookback_days, n_trades_used, baseline_wr,
             features_updated, combos_updated, symbols_updated,
             calibration_quality, avg_adj_pts, notes, duration_seconds)
            VALUES (%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (market, lookback_days, n_trades, round(baseline,2),
             features_updated, combos_updated, symbols_updated,
             round(calibration_quality,4), round(avg_adj,2),
             f'scale={SCALE_FACTOR} min_n={MIN_SAMPLES} ewma_alpha={EWMA_ALPHA}',
             duration))

        conn.commit()
        cur.close()

        log.info(f'[specialist:{market}] OK | trades={n_trades} feats={features_updated} '
                 f'combos={combos_updated} syms={symbols_updated} '
                 f'baseline={baseline:.2f}% calib_quality={calibration_quality:.4f} '
                 f'dur={duration}s')

        return {
            'market': market,
            'n_trades': n_trades,
            'baseline_wr': round(baseline, 2),
            'features_updated': features_updated,
            'combos_updated': combos_updated,
            'symbols_updated': symbols_updated,
            'calibration_quality': round(calibration_quality, 4),
            'avg_adj_pts': round(avg_adj, 2),
            'duration_seconds': duration,
        }
    except Exception as e:
        log.error(f'[specialist:{market}] recalibrate_specialist: {e}')
        try: conn.rollback()
        except Exception: pass
        import traceback; traceback.print_exc()
        return {'error': str(e), 'market': market, 'duration_s': int(time.time()-t0)}
    finally:
        try: conn.close()
        except Exception: pass


def recalibrate_all_markets(lookback_days: int = None) -> dict:
    """Itera os 3 markets sequencialmente. Falha em um nao impede os outros."""
    lookback_days = lookback_days or LOOKBACK_DAYS
    results = {}
    for market in MARKETS:
        try:
            results[market] = recalibrate_specialist(market, lookback_days)
        except Exception as e:
            log.error(f'[specialist] market={market} crash: {e}')
            results[market] = {'error': str(e), 'market': market}
    return results
