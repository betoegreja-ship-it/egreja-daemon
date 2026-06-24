"""Symbol cooldown — bloqueia simbolo apos sequencia de perdas consecutivas.

P2 do especialista 24-jun-2026.

Regra:
- Calcula streak de perdas consecutivas por simbolo
- Se streak >= COOLDOWN_LOSS_STREAK (default 3), bloqueia novas entradas
  por COOLDOWN_HOURS (default 24h) ou ate o proximo trade ganhador
- Estado mantido em cache + MySQL pra sobreviver restart
"""
import os, time, logging
from collections import defaultdict
from threading import Lock

log = logging.getLogger('egreja.cooldown')

COOLDOWN_LOSS_STREAK = int(os.environ.get('COOLDOWN_LOSS_STREAK', 3))
COOLDOWN_HOURS = int(os.environ.get('COOLDOWN_HOURS', 24))

_cache = {}  # (symbol, asset_type) -> {streak, last_loss_ts, cooldown_until_ts}
_cache_lock = Lock()
_cache_loaded_ts = 0


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
    except Exception: return None


def refresh_cooldowns():
    """Le ultimos trades por simbolo, calcula streaks. Roda a cada 5min."""
    global _cache_loaded_ts
    conn = _get_conn()
    if not conn:
        return 0
    try:
        cur = conn.cursor(dictionary=True)
        # Pegar ultimos 50 trades por (symbol, asset_type) ordem closed_at DESC
        cur.execute("""
            SELECT symbol, asset_type, pnl_pct, closed_at,
                   ROW_NUMBER() OVER (PARTITION BY symbol, asset_type ORDER BY closed_at DESC) AS rn
            FROM trades
            WHERE status='CLOSED'
              AND closed_at >= NOW() - INTERVAL 7 DAY
              AND COALESCE(close_reason,'') NOT IN ('VOIDED','MANUAL_CLOSE')
            ORDER BY symbol, asset_type, closed_at DESC
        """)
        rows = cur.fetchall()
        cur.close()
        # Agrupar por simbolo, calcular streak
        by_sym = defaultdict(list)
        for r in rows:
            by_sym[(r['symbol'], r['asset_type'])].append(r)
        now = time.time()
        new_cache = {}
        n_locked = 0
        for key, trades in by_sym.items():
            trades.sort(key=lambda x: x['closed_at'], reverse=True)  # mais recente primeiro
            streak = 0
            last_loss_ts = None
            for t in trades:
                pnl = float(t.get('pnl_pct') or 0)
                if pnl < 0:
                    streak += 1
                    if last_loss_ts is None:
                        try:
                            from datetime import datetime as _dt
                            ca = t['closed_at']
                            if isinstance(ca, str):
                                ca = _dt.fromisoformat(ca.replace('Z',''))
                            last_loss_ts = ca.timestamp()
                        except Exception: last_loss_ts = now
                else:
                    break  # sequencia quebrada por win
            cooldown_until = 0
            if streak >= COOLDOWN_LOSS_STREAK and last_loss_ts:
                cooldown_until = last_loss_ts + (COOLDOWN_HOURS * 3600)
                if cooldown_until > now:
                    n_locked += 1
            new_cache[key] = {
                'streak': streak,
                'last_loss_ts': last_loss_ts,
                'cooldown_until_ts': cooldown_until,
            }
        with _cache_lock:
            _cache.clear()
            _cache.update(new_cache)
            _cache_loaded_ts = now
        if n_locked > 0:
            log.info(f'[cooldown] refresh OK | {len(new_cache)} simbolos | {n_locked} em cooldown')
        return n_locked
    except Exception as e:
        log.warning(f'[cooldown] refresh: {e}')
        return 0
    finally:
        try: conn.close()
        except Exception: pass


def is_symbol_locked(symbol: str, asset_type: str = 'stock') -> tuple:
    """Verifica se simbolo esta em cooldown.

    Returns:
        (locked: bool, reason: str)
    """
    if not os.environ.get('SYMBOL_COOLDOWN_ENABLED', 'true').lower() == 'true':
        return False, ''
    with _cache_lock:
        # Refresh se cache stale (>5min)
        age = time.time() - _cache_loaded_ts
    if age > 300:
        refresh_cooldowns()
    with _cache_lock:
        entry = _cache.get((symbol, asset_type))
    if not entry: return False, ''
    now = time.time()
    if entry['cooldown_until_ts'] > now:
        hours_left = (entry['cooldown_until_ts'] - now) / 3600
        return True, f'COOLDOWN streak={entry["streak"]} ({hours_left:.1f}h restantes)'
    return False, ''


def get_symbol_streak(symbol: str, asset_type: str = 'stock') -> dict:
    """Retorna info de streak pra observabilidade."""
    with _cache_lock:
        return dict(_cache.get((symbol, asset_type), {}))


def cooldown_status() -> dict:
    """Snapshot pra endpoint."""
    with _cache_lock:
        now = time.time()
        locked = []
        for key, e in _cache.items():
            if e['cooldown_until_ts'] > now:
                hours = (e['cooldown_until_ts'] - now) / 3600
                locked.append({
                    'symbol': key[0], 'asset_type': key[1],
                    'streak': e['streak'],
                    'hours_remaining': round(hours, 1),
                })
        return {
            'cache_age_s': int(time.time() - _cache_loaded_ts),
            'symbols_tracked': len(_cache),
            'currently_locked': locked,
        }
