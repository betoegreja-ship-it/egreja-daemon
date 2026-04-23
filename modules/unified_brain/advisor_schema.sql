-- ============================================================
-- Brain Advisor V4 — Schema
-- ============================================================
-- 3 tabelas para shadow mode, ground truth, e métricas agregadas.
-- Criadas pelo `ensure_advisor_schema()` em advisor_shadow.py.
-- TODAS linkam com trades.trade_id para análise ex-post.
-- ============================================================

-- 1) Shadow das decisões do Entry Advisor
-- Cada chamada registra: o que IA proporia + o que motor fez + outcome real.
CREATE TABLE IF NOT EXISTS brain_shadow_entry_advisor (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- Identificação
    symbol VARCHAR(32) NOT NULL,
    asset_type VARCHAR(16) NOT NULL,           -- 'stock' ou 'crypto'
    strategy VARCHAR(32) NULL,
    market_type VARCHAR(16) NULL,              -- B3/NYSE/crypto
    direction VARCHAR(16) NULL,                -- LONG/SHORT

    -- Estado V3 no momento
    score_v3 INT NULL,
    regime_v3 VARCHAR(32) NULL,
    atr_pct DECIMAL(6,3) NULL,
    hour_of_day TINYINT NULL,
    weekday TINYINT NULL,

    -- Decisão do Advisor (hipotética em shadow)
    would_action ENUM('pass','block','reduce','boost') NOT NULL,
    would_size_mult DECIMAL(4,2) NOT NULL DEFAULT 1.00,
    would_score_delta INT DEFAULT 0,
    would_threshold_delta INT DEFAULT 0,
    aggregate_score DECIMAL(5,3) NOT NULL,
    votes_json JSON NOT NULL,
    reason VARCHAR(255) NULL,
    shadow_mode TINYINT(1) NOT NULL DEFAULT 1,

    -- Ação real do motor (preenchida no mesmo INSERT)
    motor_opened TINYINT(1) NOT NULL,          -- 1 = trade abriu, 0 = não
    motor_size_used INT NULL,                  -- qty efetivamente usada

    -- [adaptive-v1] Overlay do Adaptive Learning Brain
    adaptive_overlay_json JSON NULL,

    -- Ground truth (preenchido pelo worker quando trade fechar)
    trade_id VARCHAR(64) NULL,
    actual_pnl DECIMAL(14,4) NULL,
    actual_pnl_pct DECIMAL(8,4) NULL,
    actual_hold_minutes INT NULL,
    actual_close_reason VARCHAR(64) NULL,

    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at DATETIME NULL,

    INDEX idx_symbol_created (symbol, created_at),
    INDEX idx_would_action (would_action),
    INDEX idx_resolved (resolved_at),
    INDEX idx_trade_id (trade_id),
    INDEX idx_asset_type (asset_type, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 2) Shadow das decisões do Exit Advisor
CREATE TABLE IF NOT EXISTS brain_shadow_exit_advisor (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    trade_id VARCHAR(64) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    asset_type VARCHAR(16) NOT NULL,
    strategy VARCHAR(32) NULL,

    -- Estado da trade no momento da consulta
    entry_price DECIMAL(16,6) NULL,
    current_price DECIMAL(16,6) NULL,
    current_pnl DECIMAL(14,4) NULL,
    current_pnl_pct DECIMAL(8,4) NULL,
    peak_pnl_pct DECIMAL(8,4) NULL,
    holding_minutes INT NULL,
    score_v3_current INT NULL,
    regime_v3_current VARCHAR(32) NULL,

    -- Decisão do Exit Advisor
    would_action ENUM('hold','reduce','close','tighten_stop') NOT NULL,
    would_size_reduction_pct DECIMAL(5,2) DEFAULT 0.00,
    would_stop_adjustment_pct DECIMAL(5,2) DEFAULT 0.00,
    confidence DECIMAL(5,3) NOT NULL,
    aggregate_score DECIMAL(5,3) NOT NULL,
    votes_json JSON NOT NULL,
    reason VARCHAR(255) NULL,
    shadow_mode TINYINT(1) NOT NULL DEFAULT 1,

    -- O que o motor V3 fez de fato nesse tick (pode ser HOLD ou fechamento)
    motor_action VARCHAR(64) NULL,             -- 'HOLD','TRAILING_STOP','STOP_LOSS','V3_REVERSAL' etc
    motor_applied TINYINT(1) DEFAULT 0,        -- 1 = advisor decision foi aplicada

    -- [adaptive-v1] Overlay do Adaptive Learning Brain
    adaptive_overlay_json JSON NULL,

    -- Ground truth (preenchido quando trade fecha)
    final_pnl DECIMAL(14,4) NULL,
    final_pnl_pct DECIMAL(8,4) NULL,
    final_close_reason VARCHAR(64) NULL,

    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at DATETIME NULL,

    INDEX idx_trade_id (trade_id),
    INDEX idx_symbol_created (symbol, created_at),
    INDEX idx_would_action (would_action),
    INDEX idx_resolved (resolved_at),
    INDEX idx_asset_type (asset_type, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 3) Métricas agregadas diárias (preenchidas por job noturno)
CREATE TABLE IF NOT EXISTS brain_advisor_metrics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    metric_date DATE NOT NULL,
    asset_type VARCHAR(16) NOT NULL,           -- 'stock' ou 'crypto'
    advisor_kind ENUM('entry','exit') NOT NULL,

    -- Contadores
    n_decisions INT DEFAULT 0,
    n_block INT DEFAULT 0,
    n_reduce INT DEFAULT 0,
    n_pass INT DEFAULT 0,
    n_boost INT DEFAULT 0,
    n_hold INT DEFAULT 0,
    n_close INT DEFAULT 0,
    n_tighten_stop INT DEFAULT 0,

    -- PnL contrafactual (se advisor tivesse sido ouvido)
    pnl_saved_by_block DECIMAL(14,4) DEFAULT 0,
    pnl_added_by_boost DECIMAL(14,4) DEFAULT 0,
    pnl_saved_by_close DECIMAL(14,4) DEFAULT 0,
    pnl_lost_by_early_close DECIMAL(14,4) DEFAULT 0,

    -- WR comparativo
    wr_with_advisor DECIMAL(5,3) NULL,
    wr_without_advisor DECIMAL(5,3) NULL,
    pf_with_advisor DECIMAL(6,3) NULL,
    pf_without_advisor DECIMAL(6,3) NULL,

    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uq_date_asset_kind (metric_date, asset_type, advisor_kind),
    INDEX idx_date (metric_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
