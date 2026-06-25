"""Persistence layer pairs engine.

REGRA DE OURO: nunca DELETE em pairs_trades nem pairs_signals.
Toda escrita usa INSERT/UPSERT. Historico = ativo da empresa.
"""
import os
import logging
from datetime import datetime
from typing import Dict, Optional, List

log = logging.getLogger('egreja.pairs.persist')


def _get_conn():
    """Helper: pega conn MySQL do pool global do api_server."""
    try:
        # Reusa pool do api_server (importado tardiamente pra evitar circular)
        import api_server
        if hasattr(api_server, 'get_db'):
            return api_server.get_db()
        if hasattr(api_server, '_pool') and api_server._pool:
            return api_server._pool.get_connection()
    except Exception as e:
        log.debug(f'[pairs.persist] _get_conn fallback: {e}')
    # Fallback direto
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
        log.warning(f'[pairs.persist] no DB connection: {e}')
        return None


def persist_signal(signal: Dict) -> bool:
    """Insere um signal (mesmo HOLD) em pairs_signals."""
    if not signal:
        return False
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pairs_signals
            (ts, pair_id, leg_a, leg_b, pair_type,
             price_a, price_b, spread_method, spread_current,
             spread_mean_60d, spread_stdev_60d, z_score,
             hedge_ratio, correlation_60d, action, direction)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            signal.get('timestamp', datetime.utcnow()),
            signal.get('pair_id'),
            signal.get('leg_a'), signal.get('leg_b'),
            signal.get('pair_type'),
            signal.get('price_a'), signal.get('price_b'),
            signal.get('spread_method'),
            signal.get('spread_current'),
            signal.get('spread_mean_60d'),
            signal.get('spread_stdev_60d'),
            signal.get('z_score'),
            signal.get('hedge_ratio'),
            signal.get('correlation_60d'),
            signal.get('action'),
            signal.get('direction'),
        ))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        log.warning(f'[pairs.persist] persist_signal: {e}')
        try: conn.rollback()
        except Exception: pass
        return False
    finally:
        try: conn.close()
        except Exception: pass


def persist_trade_open(trade: Dict) -> bool:
    """INSERT new pairs_trade. Status=OPEN."""
    if not trade:
        return False
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pairs_trades
            (id, pair_id, pair_name, pair_type, leg_a, leg_b, direction, mode, status,
             opened_at, entry_z, entry_spread, entry_spread_mean, entry_spread_stdev,
             hedge_ratio, correlation_60d, price_a_entry, price_b_entry,
             qty_a, qty_b, position_size)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s)
        """, (
            trade['id'], trade['pair_id'], trade.get('name'), trade.get('pair_type'),
            trade.get('leg_a'), trade.get('leg_b'),
            trade.get('direction'), trade.get('mode', 'paper'),
            trade.get('opened_at'),
            trade.get('entry_z'), trade.get('entry_spread'),
            trade.get('entry_spread_mean'), trade.get('entry_spread_stdev'),
            trade.get('hedge_ratio'), trade.get('correlation_60d'),
            trade.get('price_a_entry'), trade.get('price_b_entry'),
            trade.get('qty_a'), trade.get('qty_b'), trade.get('position_size'),
        ))
        conn.commit()
        cur.close()
        log.info(f'[pairs.persist] OPEN persisted {trade["id"]}')
        return True
    except Exception as e:
        log.warning(f'[pairs.persist] persist_trade_open: {e}')
        try: conn.rollback()
        except Exception: pass
        return False
    finally:
        try: conn.close()
        except Exception: pass


def persist_trade_close(trade: Dict) -> bool:
    """UPDATE pairs_trade com close info. NUNCA DELETE."""
    if not trade or not trade.get('id'):
        return False
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        # Calcular duracao
        dur_s = None
        try:
            t0 = trade.get('opened_at')
            t1 = trade.get('closed_at')
            if t0 and t1:
                fmt = '%Y-%m-%dT%H:%M:%S.%f'
                dt0 = datetime.strptime(t0.split('.')[0], '%Y-%m-%dT%H:%M:%S') if isinstance(t0, str) else t0
                dt1 = datetime.strptime(t1.split('.')[0], '%Y-%m-%dT%H:%M:%S') if isinstance(t1, str) else t1
                dur_s = int((dt1 - dt0).total_seconds())
        except Exception: pass
        cur.execute("""
            UPDATE pairs_trades SET
              status='CLOSED',
              closed_at=%s,
              exit_z=%s,
              exit_spread=%s,
              price_a_exit=%s,
              price_b_exit=%s,
              close_reason=%s,
              pnl=%s,
              pnl_pct=%s,
              duration_seconds=%s
            WHERE id=%s
        """, (
            trade.get('closed_at'),
            trade.get('exit_z'),
            trade.get('exit_spread'),
            trade.get('price_a_exit'),
            trade.get('price_b_exit'),
            trade.get('close_reason'),
            trade.get('pnl'),
            trade.get('pnl_pct'),
            dur_s,
            trade['id'],
        ))
        conn.commit()
        cur.close()
        log.info(f'[pairs.persist] CLOSE persisted {trade["id"]} pnl=R${trade.get("pnl",0):+,.2f}')
        return True
    except Exception as e:
        log.warning(f'[pairs.persist] persist_trade_close: {e}')
        try: conn.rollback()
        except Exception: pass
        return False
    finally:
        try: conn.close()
        except Exception: pass


def upsert_daily_bar(symbol: str, date_str: str, ohlc: Dict, source: str = 'brapi') -> bool:
    """UPSERT em pairs_history_daily (idempotente)."""
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pairs_history_daily (symbol, date, open, high, low, close, volume, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              close=VALUES(close), high=VALUES(high), low=VALUES(low),
              volume=VALUES(volume), source=VALUES(source)
        """, (
            symbol, date_str,
            ohlc.get('open'), ohlc.get('high'), ohlc.get('low'), ohlc.get('close'),
            ohlc.get('volume') or 0, source
        ))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        log.warning(f'[pairs.persist] upsert_daily_bar {symbol} {date_str}: {e}')
        try: conn.rollback()
        except Exception: pass
        return False
    finally:
        try: conn.close()
        except Exception: pass


def bulk_upsert_daily_bars(symbol: str, bars: List[Dict], source: str = 'brapi') -> int:
    """Batch UPSERT de varios OHLCs (mais rapido que loop)."""
    if not bars:
        return 0
    conn = _get_conn()
    if not conn:
        return 0
    n = 0
    try:
        cur = conn.cursor()
        for b in bars:
            try:
                cur.execute("""
                    INSERT INTO pairs_history_daily (symbol, date, open, high, low, close, volume, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                      close=VALUES(close), high=VALUES(high), low=VALUES(low),
                      volume=VALUES(volume), source=VALUES(source)
                """, (
                    symbol, b.get('date'),
                    b.get('open'), b.get('high'), b.get('low'), b.get('close'),
                    b.get('volume') or 0, source
                ))
                n += 1
            except Exception as e:
                log.debug(f'[pairs.persist] bar fail {symbol} {b.get("date")}: {e}')
                continue
        conn.commit()
        cur.close()
        return n
    except Exception as e:
        log.warning(f'[pairs.persist] bulk_upsert_daily_bars {symbol}: {e}')
        try: conn.rollback()
        except Exception: pass
        return n
    finally:
        try: conn.close()
        except Exception: pass


def load_open_trades_from_db() -> List[Dict]:
    """[25-jun-2026] Restaura pairs_open do MySQL no boot.

    CRITICO: sem isso, cada deploy zera as trades em memoria, e o scanner
    abre TRADES NOVAS (com novo entry_z, novo opened_at) — o usuario veria
    a 'mesma' trade com parametros diferentes ao longo do tempo.

    Retorna lista de dicts compatibles com pairs_open.
    """
    conn = _get_conn()
    if not conn: return []
    out = []
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""SELECT id, pair_id, pair_name, pair_type, leg_a, leg_b,
                              direction, mode, status,
                              opened_at, entry_z, entry_spread, entry_spread_mean,
                              entry_spread_stdev, hedge_ratio, correlation_60d,
                              price_a_entry, price_b_entry, qty_a, qty_b,
                              position_size, pnl, pnl_pct
                       FROM pairs_trades
                       WHERE status='OPEN'
                       ORDER BY opened_at ASC""")
        rows = cur.fetchall()
        cur.close()
        for r in rows:
            try:
                t = {
                    'id': r['id'],
                    'pair_id': r['pair_id'],
                    'name': r['pair_name'],
                    'leg_a': r['leg_a'], 'leg_b': r['leg_b'],
                    'direction': r['direction'],
                    'pair_type': r['pair_type'],
                    'mode': r['mode'] or 'paper',
                    'status': r['status'],
                    'entry_z': float(r['entry_z']) if r['entry_z'] is not None else None,
                    'entry_spread': float(r['entry_spread']) if r['entry_spread'] is not None else None,
                    'entry_spread_mean': float(r['entry_spread_mean']) if r['entry_spread_mean'] is not None else None,
                    'entry_spread_stdev': float(r['entry_spread_stdev']) if r['entry_spread_stdev'] is not None else None,
                    'hedge_ratio': float(r['hedge_ratio']) if r['hedge_ratio'] is not None else 1.0,
                    'correlation_60d': float(r['correlation_60d']) if r['correlation_60d'] is not None else 0,
                    'price_a_entry': float(r['price_a_entry']) if r['price_a_entry'] is not None else 0,
                    'price_b_entry': float(r['price_b_entry']) if r['price_b_entry'] is not None else 0,
                    'qty_a': int(r['qty_a'] or 0),
                    'qty_b': int(r['qty_b'] or 0),
                    'position_size': float(r['position_size']) if r['position_size'] is not None else 0,
                    'opened_at': r['opened_at'].isoformat() if hasattr(r['opened_at'],'isoformat') else str(r['opened_at']),
                    'asset_type': 'pairs',
                    'pnl': float(r['pnl'] or 0),
                    'pnl_pct': float(r['pnl_pct'] or 0),
                    'current_z': float(r['entry_z']) if r['entry_z'] is not None else None,
                    'peak_pnl_pct': 0,
                }
                out.append(t)
            except Exception as e:
                log.warning(f'[pairs.persist] load_open_trades parse: {e}')
        return out
    except Exception as e:
        log.warning(f'[pairs.persist] load_open_trades_from_db: {e}')
        return []
    finally:
        try: conn.close()
        except: pass


def load_history_from_db(symbol: str, days: int = 500) -> List[Dict]:
    """Carrega historico OHLC do MySQL. Source-of-truth depois do backfill."""
    conn = _get_conn()
    if not conn:
        return []
    out = []
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT date, open, high, low, close, volume
            FROM pairs_history_daily
            WHERE symbol=%s
            ORDER BY date DESC
            LIMIT %s
        """, (symbol, days))
        rows = cur.fetchall()
        cur.close()
        # Reverter pra ordem cronologica (antigo -> recente)
        for r in reversed(rows):
            try:
                out.append({
                    'date': r['date'].strftime('%Y-%m-%d') if hasattr(r['date'], 'strftime') else str(r['date']),
                    'open': float(r['open']) if r['open'] is not None else None,
                    'high': float(r['high']) if r['high'] is not None else None,
                    'low': float(r['low']) if r['low'] is not None else None,
                    'close': float(r['close']),
                    'volume': int(r['volume']) if r['volume'] is not None else 0,
                })
            except Exception: continue
        return out
    except Exception as e:
        log.warning(f'[pairs.persist] load_history_from_db {symbol}: {e}')
        return []
    finally:
        try: conn.close()
        except Exception: pass


def aggregate_hourly_stats():
    """Roda agregacao hour-by-hour. Chamar a cada hora."""
    conn = _get_conn()
    if not conn:
        return 0
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pairs_hourly_stats
              (hour_bucket, pair_id, n_signals, n_entries, n_hold, n_converged, n_avoided,
               avg_z, max_abs_z, avg_correlation)
            SELECT
              DATE_FORMAT(ts, '%Y-%m-%d %H:00:00') AS hour_bucket,
              pair_id,
              COUNT(*) AS n_signals,
              SUM(CASE WHEN action='ENTRY' THEN 1 ELSE 0 END) AS n_entries,
              SUM(CASE WHEN action='HOLD' THEN 1 ELSE 0 END) AS n_hold,
              SUM(CASE WHEN action='CONVERGED' THEN 1 ELSE 0 END) AS n_converged,
              SUM(CASE WHEN action='AVOID' THEN 1 ELSE 0 END) AS n_avoided,
              AVG(z_score) AS avg_z,
              MAX(ABS(z_score)) AS max_abs_z,
              AVG(correlation_60d) AS avg_correlation
            FROM pairs_signals
            WHERE ts >= NOW() - INTERVAL 2 HOUR
              AND ts < DATE_FORMAT(NOW(), '%Y-%m-%d %H:00:00')
            GROUP BY hour_bucket, pair_id
            ON DUPLICATE KEY UPDATE
              n_signals=VALUES(n_signals),
              n_entries=VALUES(n_entries),
              n_hold=VALUES(n_hold),
              n_converged=VALUES(n_converged),
              n_avoided=VALUES(n_avoided),
              avg_z=VALUES(avg_z),
              max_abs_z=VALUES(max_abs_z),
              avg_correlation=VALUES(avg_correlation)
        """)
        affected = cur.rowcount
        conn.commit()
        cur.close()
        if affected > 0:
            log.info(f'[pairs.persist] hourly aggregate: {affected} rows updated')
        return affected
    except Exception as e:
        log.warning(f'[pairs.persist] aggregate_hourly_stats: {e}')
        try: conn.rollback()
        except Exception: pass
        return 0
    finally:
        try: conn.close()
        except Exception: pass


def update_pattern_stats(trade: Dict):
    """Atualiza pattern_stats por par quando trade fecha."""
    if not trade or trade.get('status') != 'CLOSED':
        return
    conn = _get_conn()
    if not conn:
        return
    try:
        # Pattern key: tipo + sinal de z + faixa
        z = abs(trade.get('entry_z', 0))
        z_band = '2.0-2.5' if z < 2.5 else '2.5-3.0' if z < 3.0 else '3.0+'
        pattern_key = f"{trade.get('pair_type')}_{trade.get('direction')}_z{z_band}"
        pnl = trade.get('pnl', 0) or 0
        pnl_pct = trade.get('pnl_pct', 0) or 0
        win = 1 if pnl > 0 else 0

        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pairs_pattern_stats
              (pair_id, pattern_key, n_trades, n_wins, total_pnl, avg_pnl_pct)
            VALUES (%s, %s, 1, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              n_trades = n_trades + 1,
              n_wins = n_wins + VALUES(n_wins),
              total_pnl = total_pnl + VALUES(total_pnl),
              avg_pnl_pct = (avg_pnl_pct * n_trades + VALUES(avg_pnl_pct)) / (n_trades + 1)
        """, (trade['pair_id'], pattern_key, win, pnl, pnl_pct))
        conn.commit()
        cur.close()
    except Exception as e:
        log.warning(f'[pairs.persist] update_pattern_stats: {e}')
        try: conn.rollback()
        except Exception: pass
    finally:
        try: conn.close()
        except Exception: pass
