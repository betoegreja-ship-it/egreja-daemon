-- Migration 013 — refinement crypto
-- Baixa min_capital_per_trade de $15K para $10K para permitir mais
-- granularidade de slots quando equity cair OU quando houver muitas
-- oportunidades simultâneas. Com equity $1.43M e min $10K, o dynamic
-- max_positions vira floor(1.43M/10K) = 143 (limitado pelo configured=20).
-- Idempotente: só aplica se valor difere.

UPDATE strategy_configs
SET
  min_capital_per_trade = 10000.00,
  updated_by            = 'migration_013'
WHERE strategy = 'crypto' AND min_capital_per_trade != 10000.00;
