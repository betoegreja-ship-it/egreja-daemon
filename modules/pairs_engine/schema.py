"""Schema MySQL para pairs engine.

CRITICAL: tabelas sao criadas com IF NOT EXISTS. NUNCA DROP.
Historico de trades + signals = maior ativo do sistema (learning).
"""
import logging
log = logging.getLogger('egreja.pairs.schema')

SCHEMA_SQL = [
    # ─── 1. OHLC diario por simbolo (fonte BRAPI/Cedro) ───
    """
    CREATE TABLE IF NOT EXISTS pairs_history_daily (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      symbol VARCHAR(16) NOT NULL,
      date DATE NOT NULL,
      open DECIMAL(14,4) NULL,
      high DECIMAL(14,4) NULL,
      low DECIMAL(14,4) NULL,
      close DECIMAL(14,4) NOT NULL,
      volume BIGINT NULL,
      source VARCHAR(16) NOT NULL DEFAULT 'brapi',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_sym_date (symbol, date),
      INDEX idx_symbol (symbol),
      INDEX idx_date (date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ─── 2. Bars intraday (5min) — Cedro real-time ───
    """
    CREATE TABLE IF NOT EXISTS pairs_history_intraday (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      symbol VARCHAR(16) NOT NULL,
      ts DATETIME NOT NULL,
      open DECIMAL(14,4) NULL,
      high DECIMAL(14,4) NULL,
      low DECIMAL(14,4) NULL,
      close DECIMAL(14,4) NOT NULL,
      volume BIGINT NULL,
      source VARCHAR(16) NOT NULL DEFAULT 'cedro',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_sym_ts (symbol, ts),
      INDEX idx_symbol_ts (symbol, ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ─── 3. Todo signal computado (mesmo HOLD) — base de learning ───
    """
    CREATE TABLE IF NOT EXISTS pairs_signals (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      ts DATETIME NOT NULL,
      pair_id VARCHAR(32) NOT NULL,
      leg_a VARCHAR(16) NOT NULL,
      leg_b VARCHAR(16) NOT NULL,
      pair_type VARCHAR(16),
      price_a DECIMAL(14,4),
      price_b DECIMAL(14,4),
      spread_method VARCHAR(16),
      spread_current DECIMAL(20,8),
      spread_mean_60d DECIMAL(20,8),
      spread_stdev_60d DECIMAL(20,8),
      z_score DECIMAL(10,4),
      hedge_ratio DECIMAL(10,4),
      correlation_60d DECIMAL(8,4),
      action VARCHAR(16),
      direction VARCHAR(16),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_pair_ts (pair_id, ts),
      INDEX idx_ts (ts),
      INDEX idx_action (action)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ─── 4. Trades pairs — abertura + fechamento — NUNCA DELETE ───
    """
    CREATE TABLE IF NOT EXISTS pairs_trades (
      id VARCHAR(32) PRIMARY KEY,
      pair_id VARCHAR(32) NOT NULL,
      pair_name VARCHAR(64),
      pair_type VARCHAR(16),
      leg_a VARCHAR(16),
      leg_b VARCHAR(16),
      direction VARCHAR(16),
      mode VARCHAR(8) DEFAULT 'paper',
      status VARCHAR(16) DEFAULT 'OPEN',

      -- entrada
      opened_at DATETIME NOT NULL,
      entry_z DECIMAL(10,4),
      entry_spread DECIMAL(20,8),
      entry_spread_mean DECIMAL(20,8),
      entry_spread_stdev DECIMAL(20,8),
      hedge_ratio DECIMAL(10,4),
      correlation_60d DECIMAL(8,4),
      price_a_entry DECIMAL(14,4),
      price_b_entry DECIMAL(14,4),
      qty_a INT,
      qty_b INT,
      position_size DECIMAL(14,2),

      -- saida
      closed_at DATETIME NULL,
      exit_z DECIMAL(10,4) NULL,
      exit_spread DECIMAL(20,8) NULL,
      price_a_exit DECIMAL(14,4) NULL,
      price_b_exit DECIMAL(14,4) NULL,
      close_reason VARCHAR(32) NULL,
      pnl DECIMAL(14,2) NULL,
      pnl_pct DECIMAL(10,4) NULL,
      duration_seconds INT NULL,

      -- learning fields
      brain_score DECIMAL(8,2) NULL,
      regime_label VARCHAR(16) NULL,
      tags VARCHAR(255) NULL,

      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      INDEX idx_pair (pair_id),
      INDEX idx_status (status),
      INDEX idx_opened (opened_at),
      INDEX idx_closed (closed_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ─── 5. Hourly aggregates por par (learning rapido) ───
    """
    CREATE TABLE IF NOT EXISTS pairs_hourly_stats (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      hour_bucket DATETIME NOT NULL,
      pair_id VARCHAR(32) NOT NULL,
      n_signals INT DEFAULT 0,
      n_entries INT DEFAULT 0,
      n_hold INT DEFAULT 0,
      n_converged INT DEFAULT 0,
      n_avoided INT DEFAULT 0,
      avg_z DECIMAL(8,4),
      max_abs_z DECIMAL(8,4),
      avg_correlation DECIMAL(8,4),
      pnl_realized DECIMAL(14,2) DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_hour_pair (hour_bucket, pair_id),
      INDEX idx_hour (hour_bucket)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ─── 6. Brain pattern stats por par (extends pattern_stats geral) ───
    """
    CREATE TABLE IF NOT EXISTS pairs_pattern_stats (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      pair_id VARCHAR(32) NOT NULL,
      pattern_key VARCHAR(128) NOT NULL,
      n_trades INT DEFAULT 0,
      n_wins INT DEFAULT 0,
      total_pnl DECIMAL(14,2) DEFAULT 0,
      avg_pnl_pct DECIMAL(8,4) DEFAULT 0,
      avg_duration_s INT DEFAULT 0,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_pair_pattern (pair_id, pattern_key),
      INDEX idx_pair (pair_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ─── 7. Snapshots — TUDO que o scanner observa fica salvo (25-jun-2026) ───
    """
    CREATE TABLE IF NOT EXISTS pairs_snapshots (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      ts DATETIME(3) NOT NULL,
      pair_id VARCHAR(32) NOT NULL,
      price_a DECIMAL(14,4), price_b DECIMAL(14,4),
      spread DECIMAL(20,8),
      z_score DECIMAL(10,4),
      mean_60d DECIMAL(20,8),
      stdev_60d DECIMAL(20,8),
      correlation_60d DECIMAL(8,4),
      hedge_ratio DECIMAL(10,4),
      action VARCHAR(16),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_pair_ts (pair_id, ts),
      INDEX idx_ts (ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ─── 8. Recalibracao history — ADF/half-life/beta a cada hora ───
    """
    CREATE TABLE IF NOT EXISTS pairs_recalibration_history (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      ts DATETIME NOT NULL,
      pair_id VARCHAR(32) NOT NULL,
      window_days INT,
      adf_tstat DECIMAL(8,3),
      half_life_days DECIMAL(8,2),
      hedge_beta DECIMAL(10,4),
      hedge_alpha DECIMAL(14,4),
      return_corr DECIMAL(6,4),
      price_corr DECIMAL(6,4),
      spread_mean DECIMAL(20,8),
      spread_stdev DECIMAL(20,8),
      regime VARCHAR(16),
      tier_recommended VARCHAR(8),
      INDEX idx_pair_ts (pair_id, ts),
      INDEX idx_ts (ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ─── 9. Events — anomalias (blowups, breakdowns, opportunities) ───
    """
    CREATE TABLE IF NOT EXISTS pairs_events (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      ts DATETIME(3) NOT NULL,
      pair_id VARCHAR(32) NOT NULL,
      event_type VARCHAR(32) NOT NULL,
      severity VARCHAR(16),
      z_score DECIMAL(10,4),
      details TEXT,
      INDEX idx_pair_ts (pair_id, ts),
      INDEX idx_type (event_type),
      INDEX idx_ts (ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ─── 10. Cross-pair correlation matrix (clusters) ───
    """
    CREATE TABLE IF NOT EXISTS pairs_cross_correlation (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      pair_a VARCHAR(32) NOT NULL,
      pair_b VARCHAR(32) NOT NULL,
      window_days INT,
      z_correlation DECIMAL(6,4),
      n_observations INT,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_pair_pair (pair_a, pair_b, window_days),
      INDEX idx_correlation (z_correlation)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ─── 11. Insights auto-descobertos por par ───
    """
    CREATE TABLE IF NOT EXISTS pairs_insights (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      pair_id VARCHAR(32) NOT NULL,
      insight_key VARCHAR(64) NOT NULL,
      insight_value TEXT,
      confidence DECIMAL(6,4),
      n_samples INT,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_pair_insight (pair_id, insight_key),
      INDEX idx_pair (pair_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


def create_pairs_tables(conn):
    """Cria todas as tabelas pairs. Idempotente (IF NOT EXISTS)."""
    created = []
    try:
        cur = conn.cursor()
        for sql in SCHEMA_SQL:
            tname = sql.split('CREATE TABLE IF NOT EXISTS')[1].split('(')[0].strip()
            cur.execute(sql)
            created.append(tname)
        conn.commit()
        cur.close()
        log.info(f'[pairs.schema] tabelas garantidas: {created}')
        return created
    except Exception as e:
        log.error(f'[pairs.schema] erro criando tabelas: {e}')
        try: conn.rollback()
        except Exception: pass
        return []
