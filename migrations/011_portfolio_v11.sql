-- ═══════════════════════════════════════════════════════════════════════
-- Migration 011 — Portfolio Accounting v11
-- ═══════════════════════════════════════════════════════════════════════
-- Data: 2026-04-19
-- Objetivo: adicionar estruturas para capital vivo por estratégia.
-- Convenção: additive-only (sem DROP, sem ALTER destrutivo).
-- Idempotente: uso de IF NOT EXISTS e IF EXISTS.
-- Rollback: DROP das 2 tabelas novas é seguro; colunas adicionadas em
-- trades/arbi_trades/capital_ledger ficam NULL para rows antigas.
-- ═══════════════════════════════════════════════════════════════════════

-- ─── 1. capital_ledger: reforçar com campos institucionais ──────────────

ALTER TABLE capital_ledger
  ADD COLUMN IF NOT EXISTS balance_before DECIMAL(18,2) NULL AFTER amount,
  ADD COLUMN IF NOT EXISTS metadata_json JSON NULL AFTER trade_id,
  ADD COLUMN IF NOT EXISTS created_by VARCHAR(40) DEFAULT 'system' AFTER metadata_json,
  ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(120) NULL AFTER created_by;

-- Índice para reconstrução cronológica rápida
ALTER TABLE capital_ledger
  ADD INDEX IF NOT EXISTS idx_ledger_strategy_time (strategy, ts);

-- Unique idempotency (permite NULL — rows legacy)
ALTER TABLE capital_ledger
  ADD UNIQUE INDEX IF NOT EXISTS uq_ledger_idempotency (idempotency_key);

-- ─── 2. strategy_configs ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS strategy_configs (
  id INT AUTO_INCREMENT PRIMARY KEY,
  strategy VARCHAR(20) NOT NULL UNIQUE,
  initial_capital DECIMAL(18,2) NOT NULL,
  risk_per_trade_pct DECIMAL(6,4) NOT NULL DEFAULT 0.0100,
  max_gross_exposure_pct DECIMAL(6,4) NOT NULL DEFAULT 0.8000,
  configured_max_positions INT NOT NULL DEFAULT 20,
  min_capital_per_trade DECIMAL(18,2) NOT NULL,
  position_hard_cap DECIMAL(18,2) NULL,
  sizing_mode VARCHAR(30) NOT NULL DEFAULT 'risk_based',
  capital_compounding_enabled TINYINT(1) DEFAULT 1,
  drawdown_hard_stop_pct DECIMAL(6,4) DEFAULT 0.2500,
  drawdown_soft_warn_pct DECIMAL(6,4) DEFAULT 0.1500,
  kill_switch_active TINYINT(1) DEFAULT 0,
  kill_switch_reason VARCHAR(200) NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  updated_by VARCHAR(40) DEFAULT 'system',
  INDEX idx_strategy (strategy)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed inicial (INSERT IGNORE não sobrescreve configs já ajustadas)
INSERT IGNORE INTO strategy_configs
  (strategy, initial_capital, risk_per_trade_pct, max_gross_exposure_pct,
   configured_max_positions, min_capital_per_trade, position_hard_cap)
VALUES
  ('stocks', 3500000.00, 0.0100, 0.8000, 25, 50000.00, 200000.00),
  ('crypto', 1500000.00, 0.0150, 0.7500, 20, 25000.00, 100000.00),
  ('arbi',   3000000.00, 0.0050, 0.9000, 50, 30000.00, 150000.00);

-- ─── 3. strategy_capital_snapshots ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS strategy_capital_snapshots (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  strategy VARCHAR(20) NOT NULL,
  ts DATETIME(3) NOT NULL,
  initial_capital DECIMAL(18,2) NOT NULL,
  net_deposits DECIMAL(18,2) NOT NULL DEFAULT 0,
  realized_pnl DECIMAL(18,2) NOT NULL DEFAULT 0,
  unrealized_pnl DECIMAL(18,2) NULL,
  gross_equity DECIMAL(18,2) NOT NULL,
  reserved_capital DECIMAL(18,2) NOT NULL,
  free_capital DECIMAL(18,2) NOT NULL,
  current_gross_exposure DECIMAL(18,2) NOT NULL,
  max_gross_exposure DECIMAL(18,2) NOT NULL,
  available_exposure DECIMAL(18,2) NOT NULL,
  open_positions_count INT NOT NULL DEFAULT 0,
  max_positions_allowed INT NOT NULL DEFAULT 0,
  operational_buying_power DECIMAL(18,2) NOT NULL,
  ledger_last_event_id BIGINT NULL,
  ledger_events_replayed INT NOT NULL DEFAULT 0,
  source VARCHAR(20) NOT NULL DEFAULT 'live',
  drift_vs_legacy DECIMAL(18,2) NULL,
  INDEX idx_snap_strategy_ts (strategy, ts DESC),
  INDEX idx_snap_last_event (ledger_last_event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── 4. trades: adicionar campos contábeis v11 ─────────────────────────

ALTER TABLE trades
  ADD COLUMN IF NOT EXISTS reserved_capital_at_entry DECIMAL(18,2) NULL,
  ADD COLUMN IF NOT EXISTS released_capital_at_close DECIMAL(18,2) NULL,
  ADD COLUMN IF NOT EXISTS realized_pnl_post_fees DECIMAL(18,2) NULL,
  ADD COLUMN IF NOT EXISTS fees_total DECIMAL(18,2) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS capital_state_snapshot_json JSON NULL,
  ADD COLUMN IF NOT EXISTS close_processed_flag TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS close_processed_at DATETIME(3) NULL;

ALTER TABLE trades
  ADD INDEX IF NOT EXISTS idx_close_processing (close_processed_flag, status);

-- ─── 5. arbi_trades: mesmos campos ────────────────────────────────────

ALTER TABLE arbi_trades
  ADD COLUMN IF NOT EXISTS reserved_capital_at_entry DECIMAL(18,2) NULL,
  ADD COLUMN IF NOT EXISTS released_capital_at_close DECIMAL(18,2) NULL,
  ADD COLUMN IF NOT EXISTS realized_pnl_post_fees DECIMAL(18,2) NULL,
  ADD COLUMN IF NOT EXISTS fees_total DECIMAL(18,2) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS capital_state_snapshot_json JSON NULL,
  ADD COLUMN IF NOT EXISTS close_processed_flag TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS close_processed_at DATETIME(3) NULL;

ALTER TABLE arbi_trades
  ADD INDEX IF NOT EXISTS idx_close_processing (close_processed_flag, status);

-- ─── 6. reconciliation_v11_log (nova — separada do log de v10.20 legacy) ─

CREATE TABLE IF NOT EXISTS reconciliation_v11_log (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  strategy VARCHAR(20) NOT NULL,
  check_type VARCHAR(40) NOT NULL,
  replay_equity DECIMAL(18,2) NULL,
  canonical_equity DECIMAL(18,2) NULL,
  legacy_capital DECIMAL(18,2) NULL,
  delta_replay_vs_canonical DECIMAL(18,2) NULL,
  delta_canonical_vs_legacy DECIMAL(18,2) NULL,
  ledger_events_count INT NULL,
  open_trades_count INT NULL,
  ok TINYINT(1) NOT NULL,
  notes JSON NULL,
  INDEX idx_recon_strategy_ts (strategy, ts DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ═══════════════════════════════════════════════════════════════════════
-- Fim migration 011. Próxima: 012 (Fase 3 — flip do caminho crítico).
-- ═══════════════════════════════════════════════════════════════════════
