"""Schema MySQL Brain Calibrator. Tudo IF NOT EXISTS — JAMAIS DELETE."""
import logging
log = logging.getLogger('egreja.calibrator.schema')

SCHEMA_SQL = [
    # 1. Pesos por (feature_name, feature_value) — aprendido dos trades
    """
    CREATE TABLE IF NOT EXISTS brain_feature_weights (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      feature_name VARCHAR(64) NOT NULL,
      feature_value VARCHAR(64) NOT NULL,
      asset_scope VARCHAR(16) NOT NULL DEFAULT 'ALL',
      n_samples INT NOT NULL DEFAULT 0,
      win_rate DECIMAL(6,3),
      avg_pnl_pct DECIMAL(8,4),
      avg_win_pct DECIMAL(8,4),
      avg_loss_pct DECIMAL(8,4),
      expected_value DECIMAL(8,4),
      adj_pts DECIMAL(6,2),
      version INT DEFAULT 1,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_feat (feature_name, feature_value, asset_scope),
      INDEX idx_feat (feature_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # 2. Pesos por (par_de_features, valor_combinado) — combos (PRE_MARKET/SURGE)
    """
    CREATE TABLE IF NOT EXISTS brain_combo_weights (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      combo_key VARCHAR(128) NOT NULL,
      combo_value VARCHAR(128) NOT NULL,
      asset_scope VARCHAR(16) NOT NULL DEFAULT 'ALL',
      n_samples INT NOT NULL DEFAULT 0,
      win_rate DECIMAL(6,3),
      avg_pnl_pct DECIMAL(8,4),
      adj_pts DECIMAL(6,2),
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_combo (combo_key, combo_value, asset_scope),
      INDEX idx_key (combo_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # 3. Track record por símbolo — symbol skill
    """
    CREATE TABLE IF NOT EXISTS brain_symbol_stats (
      symbol VARCHAR(16) NOT NULL,
      asset_type VARCHAR(16) NOT NULL,
      n_samples INT DEFAULT 0,
      win_rate DECIMAL(6,3),
      avg_pnl_pct DECIMAL(8,4),
      ewma_pnl_pct DECIMAL(8,4),
      total_pnl_pct DECIMAL(10,3),
      symbol_skill_pts DECIMAL(6,2),
      streak INT DEFAULT 0,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (symbol, asset_type),
      INDEX idx_skill (symbol_skill_pts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # 4. Log de cada calibração — métricas de auto-aprendizado
    """
    CREATE TABLE IF NOT EXISTS brain_calibration_history (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      run_ts DATETIME NOT NULL,
      lookback_days INT,
      n_trades_used INT,
      baseline_wr DECIMAL(6,3),
      features_updated INT,
      combos_updated INT,
      symbols_updated INT,
      calibration_quality DECIMAL(6,4),
      avg_adj_pts DECIMAL(6,2),
      notes TEXT,
      duration_seconds INT,
      INDEX idx_ts (run_ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # 5. A/B log: cada decisão de entry registra score_original + score_adjusted
    """
    CREATE TABLE IF NOT EXISTS brain_score_ab_log (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      ts DATETIME NOT NULL,
      symbol VARCHAR(16),
      asset_type VARCHAR(16),
      direction VARCHAR(8),
      score_original INT,
      score_adjusted DECIMAL(6,2),
      adj_total DECIMAL(6,2),
      adj_breakdown TEXT,
      decision VARCHAR(16),
      trade_id VARCHAR(64),
      pnl_pct DECIMAL(8,4) NULL,
      INDEX idx_ts (ts),
      INDEX idx_symbol_ts (symbol, ts),
      INDEX idx_trade (trade_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


# Migrations idempotentes pra adicionar colunas novas
MIGRATIONS = [
    # v2 (24-jun-2026): Expected Value tracking — sugestao do especialista
    "ALTER TABLE brain_feature_weights ADD COLUMN IF NOT EXISTS avg_win_pct DECIMAL(8,4)",
    "ALTER TABLE brain_feature_weights ADD COLUMN IF NOT EXISTS avg_loss_pct DECIMAL(8,4)",
    "ALTER TABLE brain_feature_weights ADD COLUMN IF NOT EXISTS expected_value DECIMAL(8,4)",
    "ALTER TABLE brain_combo_weights ADD COLUMN IF NOT EXISTS expected_value DECIMAL(8,4)",
    "ALTER TABLE brain_symbol_stats ADD COLUMN IF NOT EXISTS expected_value DECIMAL(8,4)",
]


def create_calibrator_tables(conn):
    """Cria tabelas + migrations. Idempotente."""
    created = []
    try:
        cur = conn.cursor()
        for sql in SCHEMA_SQL:
            tname = sql.split('CREATE TABLE IF NOT EXISTS')[1].split('(')[0].strip()
            cur.execute(sql)
            created.append(tname)
        # Aplica migrations (idempotente via IF NOT EXISTS)
        for sql in MIGRATIONS:
            try: cur.execute(sql)
            except Exception as me:
                # MySQL antigo nao tem IF NOT EXISTS em ADD COLUMN — ignore Duplicate column
                if '1060' not in str(me): log.debug(f'migration: {me}')
        conn.commit()
        cur.close()
        log.info(f'[calibrator.schema] tabelas garantidas: {created} + {len(MIGRATIONS)} migrations')
        return created
    except Exception as e:
        log.error(f'[calibrator.schema] erro: {e}')
        try: conn.rollback()
        except Exception: pass
        return []
