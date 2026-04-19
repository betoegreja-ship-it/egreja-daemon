-- ═══════════════════════════════════════════════════════════════════════
-- Migration 012 — Configs reais das 3 estrategias (capital_fraction)
-- ═══════════════════════════════════════════════════════════════════════
-- Data: 2026-04-19
-- Objetivo: calibrar strategy_configs com valores que batem com o
--           comportamento real do sistema, e ativar sizing_mode
--           'capital_fraction' para nao deixar capital parado.
--
-- Formula de sizing aplicada (mesma para as 3):
--     slots_restantes = configured_max_positions - open_positions
--     size = free_capital / slots_restantes
--
-- Exemplo arbi (equity 3.94M, 0 abertas):
--     size = 3.94M / 3 = 1.31M por trade → 3 trades = 100% do capital
--
-- Idempotente: UPDATE so se valor difere do alvo.
-- ═══════════════════════════════════════════════════════════════════════

-- ─── ARBI ───────────────────────────────────────────────────────────
-- Realidade: 3 trades simultaneas, ~$1M cada, alto WR (72%).
-- Estrategia: 100% exposure, 3 slots, sizing dinamico.
UPDATE strategy_configs
SET
  configured_max_positions = 3,
  min_capital_per_trade    = 100000.00,    -- min minimo razoavel
  position_hard_cap        = NULL,          -- sem teto absoluto (deixa capital_fraction mandar)
  max_gross_exposure_pct   = 1.0000,        -- 100% — nao deixa dinheiro parado
  risk_per_trade_pct       = 0.0050,        -- irrelevante com sizing_mode external/capital_fraction
  sizing_mode              = 'capital_fraction',
  drawdown_hard_stop_pct   = 0.2500,
  updated_by               = 'migration_012'
WHERE strategy = 'arbi';

-- ─── CRYPTO ─────────────────────────────────────────────────────────
-- Realidade (v10.51): max 20 posicoes, cada trade ~R$75K (~$15K USD).
-- Com equity ~$1.43M: 20 slots × $71K = $1.43M (100% usado).
UPDATE strategy_configs
SET
  configured_max_positions = 20,
  min_capital_per_trade    = 15000.00,      -- min razoavel em USD
  position_hard_cap        = NULL,           -- sem teto absoluto
  max_gross_exposure_pct   = 1.0000,         -- 100% do equity
  risk_per_trade_pct       = 0.0150,         -- irrelevante no capital_fraction
  sizing_mode              = 'capital_fraction',
  drawdown_hard_stop_pct   = 0.2500,
  updated_by               = 'migration_012'
WHERE strategy = 'crypto';

-- ─── STOCKS ─────────────────────────────────────────────────────────
-- Realidade: universo ~30 ativos B3+US, mas poucas abertas simultaneamente.
-- Com equity ~$3.56M e 25 slots: $142K por trade se todas abertas.
UPDATE strategy_configs
SET
  configured_max_positions = 25,
  min_capital_per_trade    = 30000.00,
  position_hard_cap        = NULL,
  max_gross_exposure_pct   = 1.0000,         -- 100% do equity
  risk_per_trade_pct       = 0.0100,         -- irrelevante no capital_fraction
  sizing_mode              = 'capital_fraction',
  drawdown_hard_stop_pct   = 0.2500,
  updated_by               = 'migration_012'
WHERE strategy = 'stocks';

-- ═══════════════════════════════════════════════════════════════════════
-- Fim migration 012.
-- ═══════════════════════════════════════════════════════════════════════
