"""
[v10.28] Database connectivity module.

Provides MySQL connection pool and get_db() function for all database operations.
This is a singleton connection manager with mutable state (the pool).

Exports:
  - db_config: dict with MySQL connection parameters from environment
  - get_db(): function that returns a pooled MySQL connection
  - test_db(): function to test database connectivity
"""

import os
import threading
import logging

log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# MYSQL CONFIGURATION
# ═══════════════════════════════════════════════════════════════

db_config = {
    'host':     os.environ.get('MYSQLHOST', 'mysql.railway.internal'),
    'port':     int(os.environ.get('MYSQLPORT', 3306)),
    'user':     os.environ.get('MYSQLUSER', 'root'),
    'password': os.environ.get('MYSQLPASSWORD', ''),
    'database': os.environ.get('MYSQLDATABASE', 'railway'),
    'autocommit': True, 'connection_timeout': 10
}

# [v10.7-Fix1] Connection pool — elimina overhead de connect/disconnect por operação.
# Pool de 30 conexões: suficiente para persistence_worker + shadow_evaluator + workers simultâneos.
# Sem pool: cada get_db() abre uma TCP connection nova (~5-50ms), Railway tem limite baixo.
_db_pool = None
_db_pool_lock = threading.Lock()


def _get_pool():
    """Inicializa o pool na primeira chamada (lazy) e retorna a instância."""
    global _db_pool
    if _db_pool is not None:
        return _db_pool
    with _db_pool_lock:
        if _db_pool is not None:
            return _db_pool
        try:
            import mysql.connector
            from mysql.connector.pooling import MySQLConnectionPool
            pool_cfg = dict(db_config)
            pool_cfg.pop('autocommit', None)   # pooling não aceita autocommit no config
            pool_cfg.pop('connection_timeout', None)
            _db_pool = MySQLConnectionPool(
                pool_name='egreja', pool_size=30,
                autocommit=True, connection_timeout=10,
                **pool_cfg)
            log.info('[v10.7] MySQL connection pool inicializado (size=30)')
        except Exception as e:
            log.error(f'MySQL pool init: {e}')
    return _db_pool


def get_db():
    """Retorna uma conexão do pool. Caller é responsável por chamar .close()
    (que devolve a conexão ao pool, não fecha a TCP connection).
    Em caso de falha no pool, faz fallback para conexão direta.
    """
    import mysql.connector
    pool = _get_pool()
    if pool:
        try:
            return pool.get_connection()
        except Exception as e:
            log.warning(f'Pool get_connection: {e} — tentando conexão direta')
    # Fallback direto (ex.: pool esgotado ou erro de inicialização)
    try:
        return mysql.connector.connect(**db_config)
    except Exception as e:
        log.error(f'MySQL fallback connect: {e}')
        return None


def test_db():
    """Testa conectividade ao banco de dados."""
    c = get_db()
    if c:
        c.close()
        return True
    return False
