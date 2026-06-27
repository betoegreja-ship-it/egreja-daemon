"""Schema MySQL Brain Specialist - tabelas paralelas as brain_*. JAMAIS DELETE."""
import logging
log = logging.getLogger('egreja.brain_specialist.schema')

SCHEMA_SQL = [
    # 1. Pesos por (market, feature_name, feature_value) — separados por mercado
    """
    CREATE TABLE IF NOT EXISTS brain_specialist_feature_weights (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      market VARCHAR(8) NOT NULL,
      feature_name VARCHAR(64) NOT NULL,
      feature_value VARCHAR(64) NOT NULL,
      n_samples INT NOT NULL DEFAULT 0,
      win_rate DECIMAL(6,3),
      avg_pnl_pct DECIMAL(8,4),
      avg_win_pct DECIMAL(8,4),
      avg_loss_pct DECIMAL(8,4),
      expected_value DECIMAL(8,4),
      adj_pts DECIMAL(6,2),
      version INT DEFAULT 1,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_feat (market, feature_name, feature_value),
      INDEX idx_market_feat (market, feature_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # 2. Combos por (market, combo_key, combo_value)
    """
    CREATE TABLE IF NOT EXISTS brain_specialist_combo_weights (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      market VARCHAR(8) NOT NULL,
      combo_key VARCHAR(128) NOT NULL,
      combo_value VARCHAR(128) NOT NULL,
      n_samples INT NOT NULL DEFAULT 0,
      win_rate DECIMAL(6,3),
      avg_pnl_pct DECIMAL(8,4),
      expected_value DECIMAL(8,4),
      adj_pts DECIMAL(6,2),
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_combo (market, combo_key, combo_value),
      INDEX idx_market_key (market, combo_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # 3. Symbol stats (ja era segregado por asset_type) — agora por market
    """
    CREATE TABLE IF NOT EXISTS brain_specialist_symbol_stats (
      market VARCHAR(8) NOT NULL,
      symbol VARCHAR(16) NOT NULL,
      n_samples INT DEFAULT 0,
      win_rate DECIMAL(6,3),
      avg_pnl_pct DECIMAL(8,4),
      ewma_pnl_pct DECIMAL(8,4),
      total_pnl_pct DECIMAL(10,3),
      symbol_skill_pts DECIMAL(6,2),
      expected_value DECIMAL(8,4),
      streak INT DEFAULT 0,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (market, symbol),
      INDEX idx_skill (market, symbol_skill_pts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # 4. Historico de calibracao — 1 entry por market por run
    """
    CREATE TABLE IF NOT EXISTS brain_specialist_calibration_history (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      market VARCHAR(8) NOT NULL,
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
      INDEX idx_market_ts (market, run_ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # 5. A/B log: unified_score vs specialist_score por decisao
    """
    CREATE TABLE IF NOT EXISTS brain_specialist_ab_log (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      ts DATETIME NOT NULL,
      market VARCHAR(8),
      symbol VARCHAR(16),
      direction VARCHAR(8),
      score_original INT,
      score_unified DECIMAL(6,2),
      score_specialist DECIMAL(6,2),
      adj_unified DECIMAL(6,2),
      adj_specialist DECIMAL(6,2),
      adj_breakdown_specialist TEXT,
      decision_unified VARCHAR(16),
      decision_specialist VARCHAR(16),
      trade_id VARCHAR(64),
      pnl_pct DECIMAL(8,4) NULL,
      INDEX idx_ts (ts),
      INDEX idx_market_ts (market, ts),
      INDEX idx_trade (trade_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


def create_specialist_tables(conn):
    """Cria as 5 tabelas specialist. Idempotente."""
    created = []
    try:
        cur = conn.cursor()
        for sql in SCHEMA_SQL:
            tname = sql.split('CREATE TABLE IF NOT EXISTS')[1].split('(')[0].strip()
            cur.execute(sql)
            created.append(tname)
        conn.commit()
        cur.close()
        log.info(f'[specialist.schema] tabelas garantidas: {created}')
        return created
    except Exception as e:
        log.error(f'[specialist.schema] erro: {e}')
        try: conn.rollback()
        except Exception: pass
        return []
