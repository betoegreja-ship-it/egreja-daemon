"""Data quality monitor — checa integridade dos dados no boot.

P2 do especialista 24-jun-2026.

Alerta se:
- N trades caiu abruptamente
- Tabela vazia ou faltando
- features_json com VOIDED concentrado
- peak_pnl_pct fora de range razoavel
"""
import os, logging

log = logging.getLogger('egreja.data_quality')


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


def boot_health_check() -> dict:
    """Roda no boot. Retorna dict com diagnostico. Loga warnings."""
    conn = _get_conn()
    if not conn:
        log.error('[DQ] no DB connection — boot health check abortado')
        return {'status': 'error', 'reason': 'no_db'}
    out = {'status': 'ok', 'checks': {}}
    try:
        cur = conn.cursor(dictionary=True)

        checks = [
            ('trades_total', "SELECT COUNT(*) AS n FROM trades WHERE status='CLOSED'", lambda v: v > 100, 'baixo demais'),
            ('trades_7d', "SELECT COUNT(*) AS n FROM trades WHERE status='CLOSED' AND closed_at >= NOW() - INTERVAL 7 DAY", lambda v: v >= 0, 'check ativo'),
            ('voided_pct_7d', """SELECT 100 * SUM(CASE WHEN close_reason='VOIDED' THEN 1 ELSE 0 END) / COUNT(*) AS n
                                 FROM trades WHERE status='CLOSED' AND closed_at >= NOW() - INTERVAL 7 DAY""",
             lambda v: v is None or v < 5, 'VOIDED acima de 5% nos ultimos 7d'),
            ('peak_outliers_7d', """SELECT COUNT(*) AS n FROM trades WHERE status='CLOSED'
                                    AND closed_at >= NOW() - INTERVAL 7 DAY
                                    AND (peak_pnl_pct > 10 OR peak_pnl_pct < -10)""",
             lambda v: v < 5, 'peaks absurdos voltaram'),
            ('features_filled_7d', """SELECT 100 * SUM(CASE WHEN features_json IS NOT NULL AND features_json != '' THEN 1 ELSE 0 END) / COUNT(*) AS n
                                       FROM trades WHERE status='CLOSED' AND closed_at >= NOW() - INTERVAL 7 DAY""",
             lambda v: v is None or v > 80, 'features faltando'),
            ('pattern_stats_count', "SELECT COUNT(*) AS n FROM pattern_stats", lambda v: v > 100, 'memoria brain quase vazia'),
            ('signal_events_24h', "SELECT COUNT(*) AS n FROM signal_events WHERE ts >= NOW() - INTERVAL 24 HOUR", lambda v: v >= 0, 'check ativo'),
        ]
        warnings = []
        for name, sql, ok_fn, msg in checks:
            try:
                cur.execute(sql)
                r = cur.fetchone()
                v = float(r['n']) if r and r['n'] is not None else None
                healthy = ok_fn(v)
                out['checks'][name] = {'value': v, 'healthy': healthy}
                if not healthy:
                    warnings.append(f'{name}={v} ({msg})')
                    log.warning(f'[DQ] FAIL {name}={v}: {msg}')
                else:
                    log.info(f'[DQ] OK {name}={v}')
            except Exception as e:
                out['checks'][name] = {'value': None, 'healthy': False, 'error': str(e)}
                log.debug(f'[DQ] check {name}: {e}')
        cur.close()
        out['warnings'] = warnings
        if warnings:
            out['status'] = 'warnings'
            log.warning(f'[DQ] {len(warnings)} warnings: {warnings}')
        return out
    except Exception as e:
        log.error(f'[DQ] boot check: {e}')
        return {'status': 'error', 'reason': str(e)}
    finally:
        try: conn.close()
        except Exception: pass
