-- =========================================================================
-- Adaptive Learning Brain v1 — Schema MySQL
-- Tabelas que o sistema LÊ são externas (trades, signal_events, pattern_stats,
-- factor_stats, brain_shadow_entry_advisor, brain_shadow_exit_advisor).
-- Tabelas que o sistema ESCREVE estão aqui (4 tabelas).
-- Isolamento derivatives/arbi é feito em código (should_bypass_adaptive_learning).
-- Collation = utf8mb4_0900_ai_ci para match com 'trades'.
-- =========================================================================

CREATE TABLE IF NOT EXISTS learning_pattern_intelligence (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  run_id VARCHAR(40) NOT NULL,
  pattern_hash VARCHAR(128) NOT NULL,
  asset_type VARCHAR(20) NOT NULL,
  direction VARCHAR(10) NULL,
  regime VARCHAR(30) NULL,
  sample_size INT NOT NULL,
  win_rate DECIMAL(6,4) NULL,
  profit_factor DECIMAL(8,4) NULL,
  avg_pnl_pct DECIMAL(8,4) NULL,
  total_pnl DECIMAL(14,2) NULL,
  stop_loss_rate DECIMAL(6,4) NULL,
  trailing_rate DECIMAL(6,4) NULL,
  timeout_rate DECIMAL(6,4) NULL,
  reversal_rate DECIMAL(6,4) NULL,
  confidence_reliability DECIMAL(6,4) NULL,
  stability_score DECIMAL(6,4) NULL,
  actionability VARCHAR(20) NOT NULL,
  tags_json JSON NULL,
  analyzed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_pattern_ctx (pattern_hash, asset_type),
  INDEX idx_run (run_id),
  INDEX idx_actionability (actionability, asset_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS learning_confidence_calibration (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  run_id VARCHAR(40) NOT NULL,
  asset_type VARCHAR(20) NOT NULL,
  direction VARCHAR(10) NULL,
  confidence_band VARCHAR(20) NOT NULL,
  band_lower DECIMAL(6,2) NOT NULL,
  band_upper DECIMAL(6,2) NOT NULL,
  sample_size INT NOT NULL,
  win_rate DECIMAL(6,4) NULL,
  total_pnl DECIMAL(14,2) NULL,
  avg_pnl_pct DECIMAL(8,4) NULL,
  expectancy DECIMAL(8,4) NULL,
  inversion_flag TINYINT(1) DEFAULT 0,
  recommended_action VARCHAR(30) NULL,
  recommended_dead_zone TINYINT(1) DEFAULT 0,
  reliability_score DECIMAL(6,4) NULL,
  analyzed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_conf_asset (asset_type, confidence_band),
  INDEX idx_run (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS learning_policy_proposals (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  run_id VARCHAR(40) NOT NULL,
  proposal_type VARCHAR(50) NOT NULL,
  target_scope VARCHAR(50) NOT NULL,
  current_value VARCHAR(200) NULL,
  proposed_value VARCHAR(200) NOT NULL,
  rationale TEXT NULL,
  evidence_json JSON NULL,
  expected_impact_json JSON NULL,
  risk_level VARCHAR(20) NOT NULL,
  confidence_score DECIMAL(5,2) NULL,
  status VARCHAR(20) DEFAULT 'proposed',
  approved_by VARCHAR(80) NULL,
  approval_note TEXT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  approved_at DATETIME NULL,
  rolled_out_at DATETIME NULL,
  rolled_back_at DATETIME NULL,
  INDEX idx_status_type (status, proposal_type),
  INDEX idx_run (run_id),
  INDEX idx_scope (target_scope)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS learning_policy_outcomes (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  proposal_id BIGINT NOT NULL,
  metric_name VARCHAR(50) NOT NULL,
  metric_value DECIMAL(14,4) NULL,
  metric_unit VARCHAR(20) NULL,
  metadata_json JSON NULL,
  measured_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_proposal_metric (proposal_id, metric_name),
  CONSTRAINT fk_policy_outcome_proposal
    FOREIGN KEY (proposal_id) REFERENCES learning_policy_proposals(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
