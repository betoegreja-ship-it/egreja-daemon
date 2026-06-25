"""Configuracao dos pares stat arbi B3.

Versao 2 (25-jun-2026): substitui os 14 pares iniciais (escolhidos por
intuicao setorial) pelos 60 pares cientificamente selecionados via
estudo profundo do especialista:

- 1.485 pares cruzados de 55 acoes B3 (universo de 67)
- Criterios: cointegracao ADF, half-life, correlacao retorno + preco,
  hedge beta via regressao, backtest walk-forward 2y
- 26 pares Tier A (paper imediato) + 34 pares Tier B (validacao + watch)
- 957 pares rejeitados

Cada par tem:
  hedge_ratio_init: beta empirico do estudo (regressao OLS 2y)
  half_life_days: dias para spread converger 50% (Ornstein-Uhlenbeck)
  adf_tstat: t-stat do Augmented Dickey-Fuller (mais negativo = mais cointegrado)
  expected_bps: backtest 2y total em basis points
  return_corr: correlacao de retornos diarios 2y
  price_corr: correlacao de niveis de preco 2y
  tier: 'A' (paper imediato), 'B' (validar/watch), 'WATCH' (sem trade)
  score: score composto do estudo (referencia)

Pares com tier='WATCH' tem sizing reduzido (50%) e z_entry mais alto (2.5).
Pares Tier A operam normal: z_entry=2.0, exit=0.4, stop=3.5.
"""

PAIRS_CONFIG = [
    # ═══════════════════════════════════════════════════════════════
    # TIER A — 26 PARES PARA PAPER IMEDIATO (cointegrados + tradeable)
    # ═══════════════════════════════════════════════════════════════
    # 1. GGBR4-GOAU4 — Gerdau ON / Metalurgica Gerdau (holding)
    {'id': 'GGBR4-GOAU4', 'name': 'Gerdau / Met Gerdau', 'leg_a': 'GGBR4', 'leg_b': 'GOAU4',
     'pair_type': 'HOLDING', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.860, 'half_life_days': 14.8, 'adf_tstat': -3.47,
     'return_corr': 0.95, 'price_corr': 0.99, 'expected_bps': 832, 'score': 246.10,
     'enabled': True, 'liquidity_tier': 'A'},
    # 2. PETR4-PETR3 — Petrobras PN/ON (classes)
    {'id': 'PETR4-PETR3', 'name': 'Petrobras PN/ON', 'leg_a': 'PETR4', 'leg_b': 'PETR3',
     'pair_type': 'CLASSES', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.3, 'z_stop': 3.5,
     'beta_a_to_b': 0.931, 'half_life_days': 11.5, 'adf_tstat': -3.85,
     'return_corr': 0.96, 'price_corr': 1.00, 'expected_bps': 1074, 'score': 245.01,
     'enabled': True, 'liquidity_tier': 'A'},
    # 3. VALE3-BRAP4 — Vale / Bradespar (holding com Vale dentro)
    {'id': 'VALE3-BRAP4', 'name': 'Vale / Bradespar', 'leg_a': 'VALE3', 'leg_b': 'BRAP4',
     'pair_type': 'HOLDING', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.047, 'half_life_days': 18.0, 'adf_tstat': -3.18,
     'return_corr': 0.92, 'price_corr': 0.99, 'expected_bps': 905, 'score': 227.45,
     'enabled': True, 'liquidity_tier': 'A'},
    # 4. BBDC3-MULT3 — Bradesco ON / Multiplan (descoberta nao-obvia)
    {'id': 'BBDC3-MULT3', 'name': 'Bradesco / Multiplan', 'leg_a': 'BBDC3', 'leg_b': 'MULT3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.379, 'half_life_days': 11.6, 'adf_tstat': -3.91,
     'return_corr': 0.63, 'price_corr': 0.97, 'expected_bps': 3697, 'score': 211.69,
     'enabled': True, 'liquidity_tier': 'B'},
    # 5. ITUB4-BPAC11 — Itau / BTG Pactual
    {'id': 'ITUB4-BPAC11', 'name': 'Itau / BTG', 'leg_a': 'ITUB4', 'leg_b': 'BPAC11',
     'pair_type': 'SECTORIAL', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.776, 'half_life_days': 12.0, 'adf_tstat': -3.93,
     'return_corr': 0.68, 'price_corr': 0.98, 'expected_bps': 2912, 'score': 207.72,
     'enabled': True, 'liquidity_tier': 'A'},
    # 6. BBDC4-MULT3 — Bradesco PN / Multiplan
    {'id': 'BBDC4-MULT3', 'name': 'Bradesco / Multiplan', 'leg_a': 'BBDC4', 'leg_b': 'MULT3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.504, 'half_life_days': 13.2, 'adf_tstat': -3.66,
     'return_corr': 0.62, 'price_corr': 0.97, 'expected_bps': 4025, 'score': 196.16,
     'enabled': True, 'liquidity_tier': 'B'},
    # 7. MULT3-ITSA4 — Multiplan / Itausa
    {'id': 'MULT3-ITSA4', 'name': 'Multiplan / Itausa', 'leg_a': 'MULT3', 'leg_b': 'ITSA4',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.663, 'half_life_days': 17.6, 'adf_tstat': -3.04,
     'return_corr': 0.66, 'price_corr': 0.96, 'expected_bps': 2330, 'score': 191.41,
     'enabled': True, 'liquidity_tier': 'B'},
    # 8. ALOS3-EQTL3 — Aliansce Sonae / Equatorial
    {'id': 'ALOS3-EQTL3', 'name': 'Aliansce / Equatorial', 'leg_a': 'ALOS3', 'leg_b': 'EQTL3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.436, 'half_life_days': 15.3, 'adf_tstat': -3.54,
     'return_corr': 0.68, 'price_corr': 0.95, 'expected_bps': 1976, 'score': 189.76,
     'enabled': True, 'liquidity_tier': 'B'},
    # 9. GGBR4-GGBR3 — Gerdau PN/ON
    {'id': 'GGBR4-GGBR3', 'name': 'Gerdau PN/ON', 'leg_a': 'GGBR4', 'leg_b': 'GGBR3',
     'pair_type': 'CLASSES', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.3, 'z_stop': 3.5,
     'beta_a_to_b': 1.138, 'half_life_days': 16.7, 'adf_tstat': -3.25,
     'return_corr': 0.83, 'price_corr': 0.95, 'expected_bps': 384, 'score': 189.38,
     'enabled': True, 'liquidity_tier': 'B'},
    # 10. MULT3-EQTL3 — Multiplan / Equatorial
    {'id': 'MULT3-EQTL3', 'name': 'Multiplan / Equatorial', 'leg_a': 'MULT3', 'leg_b': 'EQTL3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.031, 'half_life_days': 10.7, 'adf_tstat': -4.09,
     'return_corr': 0.66, 'price_corr': 0.96, 'expected_bps': 875, 'score': 189.01,
     'enabled': True, 'liquidity_tier': 'B'},
    # 11. EQTL3-ITSA4 — Equatorial / Itausa
    {'id': 'EQTL3-ITSA4', 'name': 'Equatorial / Itausa', 'leg_a': 'EQTL3', 'leg_b': 'ITSA4',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.620, 'half_life_days': 11.7, 'adf_tstat': -3.86,
     'return_corr': 0.61, 'price_corr': 0.96, 'expected_bps': 2146, 'score': 187.35,
     'enabled': True, 'liquidity_tier': 'A'},
    # 12. ITUB4-EQTL3 — Itau / Equatorial
    {'id': 'ITUB4-EQTL3', 'name': 'Itau / Equatorial', 'leg_a': 'ITUB4', 'leg_b': 'EQTL3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.418, 'half_life_days': 11.9, 'adf_tstat': -3.79,
     'return_corr': 0.60, 'price_corr': 0.96, 'expected_bps': 2964, 'score': 187.17,
     'enabled': True, 'liquidity_tier': 'A'},
    # 13. BPAC11-MULT3 — BTG / Multiplan
    {'id': 'BPAC11-MULT3', 'name': 'BTG / Multiplan', 'leg_a': 'BPAC11', 'leg_b': 'MULT3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.674, 'half_life_days': 18.5, 'adf_tstat': -3.05,
     'return_corr': 0.64, 'price_corr': 0.96, 'expected_bps': 4189, 'score': 184.21,
     'enabled': True, 'liquidity_tier': 'B'},
    # 14. BPAC11-ALPA4 — BTG / Alpargatas (descoberta)
    {'id': 'BPAC11-ALPA4', 'name': 'BTG / Alpargatas', 'leg_a': 'BPAC11', 'leg_b': 'ALPA4',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.753, 'half_life_days': 20.7, 'adf_tstat': -3.40,
     'return_corr': 0.51, 'price_corr': 0.94, 'expected_bps': 6765, 'score': 182.56,
     'enabled': True, 'liquidity_tier': 'B'},
    # 15. SBSP3-BPAC11 — Sabesp / BTG
    {'id': 'SBSP3-BPAC11', 'name': 'Sabesp / BTG', 'leg_a': 'SBSP3', 'leg_b': 'BPAC11',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.884, 'half_life_days': 13.1, 'adf_tstat': -4.19,
     'return_corr': 0.57, 'price_corr': 0.97, 'expected_bps': 1751, 'score': 181.93,
     'enabled': True, 'liquidity_tier': 'B'},
    # 16. BBDC4-BPAC11 — Bradesco / BTG
    {'id': 'BBDC4-BPAC11', 'name': 'Bradesco / BTG', 'leg_a': 'BBDC4', 'leg_b': 'BPAC11',
     'pair_type': 'SECTORIAL', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.861, 'half_life_days': 19.8, 'adf_tstat': -3.02,
     'return_corr': 0.62, 'price_corr': 0.97, 'expected_bps': 2001, 'score': 176.10,
     'enabled': True, 'liquidity_tier': 'A'},
    # 17. BPAC11-BBDC3 — BTG / Bradesco ON
    {'id': 'BPAC11-BBDC3', 'name': 'BTG / Bradesco ON', 'leg_a': 'BPAC11', 'leg_b': 'BBDC3',
     'pair_type': 'SECTORIAL', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.200, 'half_life_days': 16.5, 'adf_tstat': -3.28,
     'return_corr': 0.62, 'price_corr': 0.97, 'expected_bps': 2331, 'score': 170.19,
     'enabled': True, 'liquidity_tier': 'A'},
    # 18. BBDC4-RDOR3 — Bradesco / Rede DOr (descoberta)
    {'id': 'BBDC4-RDOR3', 'name': 'Bradesco / Rede DOr', 'leg_a': 'BBDC4', 'leg_b': 'RDOR3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.110, 'half_life_days': 18.1, 'adf_tstat': -3.00,
     'return_corr': 0.55, 'price_corr': 0.96, 'expected_bps': 1994, 'score': 167.28,
     'enabled': True, 'liquidity_tier': 'B'},
    # 19. EGIE3-EQTL3 — Engie / Equatorial (utilities)
    {'id': 'EGIE3-EQTL3', 'name': 'Engie / Equatorial', 'leg_a': 'EGIE3', 'leg_b': 'EQTL3',
     'pair_type': 'SECTORIAL', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.658, 'half_life_days': 18.5, 'adf_tstat': -3.33,
     'return_corr': 0.52, 'price_corr': 0.87, 'expected_bps': 3024, 'score': 163.19,
     'enabled': True, 'liquidity_tier': 'B'},
    # 20. EQTL3-B3SA3 — Equatorial / B3
    {'id': 'EQTL3-B3SA3', 'name': 'Equatorial / B3', 'leg_a': 'EQTL3', 'leg_b': 'B3SA3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.638, 'half_life_days': 14.1, 'adf_tstat': -3.56,
     'return_corr': 0.54, 'price_corr': 0.94, 'expected_bps': 3290, 'score': 163.11,
     'enabled': True, 'liquidity_tier': 'A'},
    # 21. SBSP3-ITSA4 — Sabesp / Itausa
    {'id': 'SBSP3-ITSA4', 'name': 'Sabesp / Itausa', 'leg_a': 'SBSP3', 'leg_b': 'ITSA4',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.066, 'half_life_days': 19.6, 'adf_tstat': -3.36,
     'return_corr': 0.59, 'price_corr': 0.97, 'expected_bps': 377, 'score': 162.44,
     'enabled': True, 'liquidity_tier': 'A'},
    # 22. BBDC4-EQTL3 — Bradesco / Equatorial
    {'id': 'BBDC4-EQTL3', 'name': 'Bradesco / Equatorial', 'leg_a': 'BBDC4', 'leg_b': 'EQTL3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.573, 'half_life_days': 15.4, 'adf_tstat': -3.38,
     'return_corr': 0.55, 'price_corr': 0.94, 'expected_bps': 2042, 'score': 157.87,
     'enabled': True, 'liquidity_tier': 'A'},
    # 23. MULT3-ALPA4 — Multiplan / Alpargatas
    {'id': 'MULT3-ALPA4', 'name': 'Multiplan / Alpargatas', 'leg_a': 'MULT3', 'leg_b': 'ALPA4',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.416, 'half_life_days': 18.2, 'adf_tstat': -3.51,
     'return_corr': 0.48, 'price_corr': 0.91, 'expected_bps': 1966, 'score': 155.07,
     'enabled': True, 'liquidity_tier': 'B'},
    # 24. ALOS3-ALPA4 — Aliansce / Alpargatas
    {'id': 'ALOS3-ALPA4', 'name': 'Aliansce / Alpargatas', 'leg_a': 'ALOS3', 'leg_b': 'ALPA4',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.614, 'half_life_days': 19.5, 'adf_tstat': -3.34,
     'return_corr': 0.55, 'price_corr': 0.96, 'expected_bps': 434, 'score': 153.40,
     'enabled': True, 'liquidity_tier': 'B'},
    # 25. VIVT3-MULT3 — Vivo / Multiplan
    {'id': 'VIVT3-MULT3', 'name': 'Vivo / Multiplan', 'leg_a': 'VIVT3', 'leg_b': 'MULT3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.295, 'half_life_days': 17.6, 'adf_tstat': -3.19,
     'return_corr': 0.48, 'price_corr': 0.94, 'expected_bps': 1662, 'score': 148.65,
     'enabled': True, 'liquidity_tier': 'A'},
    # 26. EGIE3-MULT3 — Engie / Multiplan
    {'id': 'EGIE3-MULT3', 'name': 'Engie / Multiplan', 'leg_a': 'EGIE3', 'leg_b': 'MULT3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'A', 'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.614, 'half_life_days': 20.1, 'adf_tstat': -3.07,
     'return_corr': 0.49, 'price_corr': 0.87, 'expected_bps': 1485, 'score': 140.22,
     'enabled': True, 'liquidity_tier': 'B'},

    # ═══════════════════════════════════════════════════════════════
    # TIER B — 34 PARES PARA VALIDACAO (z_entry maior + sizing reduzido)
    # ═══════════════════════════════════════════════════════════════
    {'id': 'GGBR3-GOAU4', 'name': 'Gerdau ON / Met Gerdau', 'leg_a': 'GGBR3', 'leg_b': 'GOAU4',
     'pair_type': 'HOLDING', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.674, 'half_life_days': 20.9, 'adf_tstat': -2.94,
     'return_corr': 0.81, 'price_corr': 0.93, 'expected_bps': 1232, 'score': 192.93,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'BPAC11-EQTL3', 'name': 'BTG / Equatorial', 'leg_a': 'BPAC11', 'leg_b': 'EQTL3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.801, 'half_life_days': 10.5, 'adf_tstat': -4.17,
     'return_corr': 0.57, 'price_corr': 0.96, 'expected_bps': 5147, 'score': 187.54,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'VBBR3-UGPA3', 'name': 'Vibra / Ultrapar', 'leg_a': 'VBBR3', 'leg_b': 'UGPA3',
     'pair_type': 'SECTORIAL', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.089, 'half_life_days': 26.2, 'adf_tstat': -2.95,
     'return_corr': 0.72, 'price_corr': 0.95, 'expected_bps': 1370, 'score': 181.92,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'MULT3-VBBR3', 'name': 'Multiplan / Vibra', 'leg_a': 'MULT3', 'leg_b': 'VBBR3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.545, 'half_life_days': 24.2, 'adf_tstat': -2.72,
     'return_corr': 0.56, 'price_corr': 0.92, 'expected_bps': 3333, 'score': 175.56,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'ITUB4-MULT3', 'name': 'Itau / Multiplan', 'leg_a': 'ITUB4', 'leg_b': 'MULT3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.318, 'half_life_days': 19.2, 'adf_tstat': -2.93,
     'return_corr': 0.64, 'price_corr': 0.95, 'expected_bps': 0, 'score': 175.20,
     'enabled': True, 'liquidity_tier': 'A'},
    {'id': 'BBDC3-EQTL3', 'name': 'Bradesco ON / Equatorial', 'leg_a': 'BBDC3', 'leg_b': 'EQTL3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.444, 'half_life_days': 13.5, 'adf_tstat': -3.61,
     'return_corr': 0.54, 'price_corr': 0.95, 'expected_bps': 0, 'score': 173.70,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'BPAC11-TAEE11', 'name': 'BTG / Taesa', 'leg_a': 'BPAC11', 'leg_b': 'TAEE11',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.769, 'half_life_days': 22.4, 'adf_tstat': -2.79,
     'return_corr': 0.53, 'price_corr': 0.95, 'expected_bps': 0, 'score': 162.20,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'ITUB4-ALPA4', 'name': 'Itau / Alpargatas', 'leg_a': 'ITUB4', 'leg_b': 'ALPA4',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.589, 'half_life_days': 19.4, 'adf_tstat': -3.90,
     'return_corr': 0.47, 'price_corr': 0.95, 'expected_bps': 0, 'score': 160.20,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'ITSA4-B3SA3', 'name': 'Itausa / B3', 'leg_a': 'ITSA4', 'leg_b': 'B3SA3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.007, 'half_life_days': 21.4, 'adf_tstat': -2.69,
     'return_corr': 0.65, 'price_corr': 0.96, 'expected_bps': 1150, 'score': 160.04,
     'enabled': True, 'liquidity_tier': 'A'},
    {'id': 'BBDC3-ALOS3', 'name': 'Bradesco ON / Aliansce', 'leg_a': 'BBDC3', 'leg_b': 'ALOS3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.968, 'half_life_days': 24.5, 'adf_tstat': -2.86,
     'return_corr': 0.64, 'price_corr': 0.95, 'expected_bps': 0, 'score': 159.20,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'ALOS3-TAEE11', 'name': 'Aliansce / Taesa', 'leg_a': 'ALOS3', 'leg_b': 'TAEE11',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.437, 'half_life_days': 19.7, 'adf_tstat': -2.99,
     'return_corr': 0.57, 'price_corr': 0.95, 'expected_bps': 0, 'score': 157.60,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'ITUB4-B3SA3', 'name': 'Itau / B3', 'leg_a': 'ITUB4', 'leg_b': 'B3SA3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.954, 'half_life_days': 22.9, 'adf_tstat': -2.69,
     'return_corr': 0.62, 'price_corr': 0.95, 'expected_bps': 3603, 'score': 157.32,
     'enabled': True, 'liquidity_tier': 'A'},
    {'id': 'ALOS3-ENEV3', 'name': 'Aliansce / Eneva', 'leg_a': 'ALOS3', 'leg_b': 'ENEV3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.726, 'half_life_days': 19.5, 'adf_tstat': -2.75,
     'return_corr': 0.48, 'price_corr': 0.95, 'expected_bps': 0, 'score': 156.60,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'SBSP3-EQTL3', 'name': 'Sabesp / Equatorial', 'leg_a': 'SBSP3', 'leg_b': 'EQTL3',
     'pair_type': 'SECTORIAL', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.596, 'half_life_days': 19.6, 'adf_tstat': -2.98,
     'return_corr': 0.61, 'price_corr': 0.95, 'expected_bps': 0, 'score': 151.80,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'SBSP3-MULT3', 'name': 'Sabesp / Multiplan', 'leg_a': 'SBSP3', 'leg_b': 'MULT3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.472, 'half_life_days': 25.3, 'adf_tstat': -2.71,
     'return_corr': 0.56, 'price_corr': 0.95, 'expected_bps': 0, 'score': 151.80,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'ITUB4-VIVT3', 'name': 'Itau / Vivo', 'leg_a': 'ITUB4', 'leg_b': 'VIVT3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.969, 'half_life_days': 15.8, 'adf_tstat': -3.31,
     'return_corr': 0.45, 'price_corr': 0.95, 'expected_bps': 0, 'score': 150.80,
     'enabled': True, 'liquidity_tier': 'A'},
    {'id': 'ALPA4-ITSA4', 'name': 'Alpargatas / Itausa', 'leg_a': 'ALPA4', 'leg_b': 'ITSA4',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.437, 'half_life_days': 20.6, 'adf_tstat': -3.51,
     'return_corr': 0.49, 'price_corr': 0.95, 'expected_bps': 0, 'score': 149.90,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'VIVT3-BPAC11', 'name': 'Vivo / BTG', 'leg_a': 'VIVT3', 'leg_b': 'BPAC11',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.758, 'half_life_days': 14.3, 'adf_tstat': -3.69,
     'return_corr': 0.43, 'price_corr': 0.95, 'expected_bps': 0, 'score': 146.50,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'VIVT3-SBSP3', 'name': 'Vivo / Sabesp', 'leg_a': 'VIVT3', 'leg_b': 'SBSP3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.835, 'half_life_days': 16.8, 'adf_tstat': -3.24,
     'return_corr': 0.44, 'price_corr': 0.95, 'expected_bps': 0, 'score': 146.10,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'VIVT3-BBDC3', 'name': 'Vivo / Bradesco ON', 'leg_a': 'VIVT3', 'leg_b': 'BBDC3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.925, 'half_life_days': 18.3, 'adf_tstat': -3.12,
     'return_corr': 0.41, 'price_corr': 0.95, 'expected_bps': 0, 'score': 144.40,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'BBDC3-ALPA4', 'name': 'Bradesco ON / Alpargatas', 'leg_a': 'BBDC3', 'leg_b': 'ALPA4',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.598, 'half_life_days': 22.3, 'adf_tstat': -3.48,
     'return_corr': 0.46, 'price_corr': 0.95, 'expected_bps': 0, 'score': 142.70,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'EQTL3-VBBR3', 'name': 'Equatorial / Vibra', 'leg_a': 'EQTL3', 'leg_b': 'VBBR3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.488, 'half_life_days': 24.3, 'adf_tstat': -2.82,
     'return_corr': 0.51, 'price_corr': 0.95, 'expected_bps': 0, 'score': 142.60,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'VIVT3-ITSA4', 'name': 'Vivo / Itausa', 'leg_a': 'VIVT3', 'leg_b': 'ITSA4',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.912, 'half_life_days': 22.0, 'adf_tstat': -2.86,
     'return_corr': 0.47, 'price_corr': 0.95, 'expected_bps': 0, 'score': 142.30,
     'enabled': True, 'liquidity_tier': 'A'},
    {'id': 'EQTL3-COGN3', 'name': 'Equatorial / Cogna', 'leg_a': 'EQTL3', 'leg_b': 'COGN3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.275, 'half_life_days': 21.8, 'adf_tstat': -2.95,
     'return_corr': 0.44, 'price_corr': 0.95, 'expected_bps': 0, 'score': 140.70,
     'enabled': True, 'liquidity_tier': 'C'},
    {'id': 'BBDC3-RDOR3', 'name': 'Bradesco ON / Rede DOr', 'leg_a': 'BBDC3', 'leg_b': 'RDOR3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.009, 'half_life_days': 20.6, 'adf_tstat': -2.77,
     'return_corr': 0.55, 'price_corr': 0.95, 'expected_bps': 0, 'score': 139.60,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'RENT3-EGIE3', 'name': 'Localiza / Engie', 'leg_a': 'RENT3', 'leg_b': 'EGIE3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.284, 'half_life_days': 13.9, 'adf_tstat': -3.48,
     'return_corr': 0.35, 'price_corr': 0.95, 'expected_bps': 0, 'score': 135.10,
     'enabled': True, 'liquidity_tier': 'A'},
    {'id': 'RENT3-EQTL3', 'name': 'Localiza / Equatorial', 'leg_a': 'RENT3', 'leg_b': 'EQTL3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.925, 'half_life_days': 17.3, 'adf_tstat': -3.40,
     'return_corr': 0.45, 'price_corr': 0.95, 'expected_bps': 0, 'score': 132.20,
     'enabled': True, 'liquidity_tier': 'A'},
    {'id': 'EGIE3-ALOS3', 'name': 'Engie / Aliansce', 'leg_a': 'EGIE3', 'leg_b': 'ALOS3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.442, 'half_life_days': 20.5, 'adf_tstat': -2.97,
     'return_corr': 0.50, 'price_corr': 0.95, 'expected_bps': 0, 'score': 131.00,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'BBDC4-EGIE3', 'name': 'Bradesco / Engie', 'leg_a': 'BBDC4', 'leg_b': 'EGIE3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.794, 'half_life_days': 30.9, 'adf_tstat': -2.72,
     'return_corr': 0.40, 'price_corr': 0.95, 'expected_bps': 0, 'score': 129.50,
     'enabled': True, 'liquidity_tier': 'A'},
    {'id': 'BBDC4-VIVT3', 'name': 'Bradesco / Vivo', 'leg_a': 'BBDC4', 'leg_b': 'VIVT3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 1.076, 'half_life_days': 19.6, 'adf_tstat': -3.00,
     'return_corr': 0.41, 'price_corr': 0.95, 'expected_bps': 0, 'score': 129.30,
     'enabled': True, 'liquidity_tier': 'A'},
    {'id': 'EGIE3-VBBR3', 'name': 'Engie / Vibra', 'leg_a': 'EGIE3', 'leg_b': 'VBBR3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.381, 'half_life_days': 13.2, 'adf_tstat': -3.77,
     'return_corr': 0.38, 'price_corr': 0.95, 'expected_bps': 0, 'score': 127.90,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'EGIE3-BBDC3', 'name': 'Engie / Bradesco ON', 'leg_a': 'EGIE3', 'leg_b': 'BBDC3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.412, 'half_life_days': 24.7, 'adf_tstat': -2.77,
     'return_corr': 0.41, 'price_corr': 0.95, 'expected_bps': 0, 'score': 125.20,
     'enabled': True, 'liquidity_tier': 'B'},
    {'id': 'EGIE3-B3SA3', 'name': 'Engie / B3', 'leg_a': 'EGIE3', 'leg_b': 'B3SA3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.427, 'half_life_days': 23.6, 'adf_tstat': -2.80,
     'return_corr': 0.41, 'price_corr': 0.95, 'expected_bps': 0, 'score': 118.90,
     'enabled': True, 'liquidity_tier': 'A'},
    {'id': 'SMFT3-TOTS3', 'name': 'SmartFit / Totvs', 'leg_a': 'SMFT3', 'leg_b': 'TOTS3',
     'pair_type': 'CROSS_SECTOR', 'tier': 'B', 'z_entry': 2.5, 'z_exit': 0.4, 'z_stop': 3.5,
     'beta_a_to_b': 0.452, 'half_life_days': 21.9, 'adf_tstat': -2.85,
     'return_corr': 0.46, 'price_corr': 0.95, 'expected_bps': 0, 'score': 129.90,
     'enabled': False, 'liquidity_tier': 'C'},  # ambos com track ruim no audit recente

    # ═══════════════════════════════════════════════════════════════
    # LEGACY — pares antigos rebaixados a WATCH (sem trade) ou removidos
    # ═══════════════════════════════════════════════════════════════
    # Mantemos ITUB4-ITSA4 e BBDC4-BBDC3 como WATCH (correlacao real mas spread fraco)
    {'id': 'ITUB4-ITSA4', 'name': 'Itau / Itausa (LEGACY watch)', 'leg_a': 'ITUB4', 'leg_b': 'ITSA4',
     'pair_type': 'HOLDING', 'tier': 'WATCH', 'z_entry': 2.8, 'z_exit': 0.3, 'z_stop': 3.5,
     'beta_a_to_b': 1.0, 'half_life_days': 59.3, 'adf_tstat': -1.72,
     'return_corr': 0.95, 'price_corr': 0.99, 'expected_bps': 313, 'score': 186.40,
     'enabled': True, 'liquidity_tier': 'A'},
    {'id': 'BBDC4-BBDC3', 'name': 'Bradesco PN/ON (LEGACY watch)', 'leg_a': 'BBDC4', 'leg_b': 'BBDC3',
     'pair_type': 'CLASSES', 'tier': 'WATCH', 'z_entry': 2.8, 'z_exit': 0.3, 'z_stop': 3.5,
     'beta_a_to_b': 1.0, 'half_life_days': 18.3, 'adf_tstat': -3.05,
     'return_corr': 0.97, 'price_corr': 1.00, 'expected_bps': -155, 'score': 158.43,
     'enabled': True, 'liquidity_tier': 'A'},

    # REMOVIDOS (não passaram nos critérios — D_avoid ou sem evidência):
    # SBSP3-CSMG3, RDOR3-HAPV3, BBSE3-BBAS3 — correlação real baixa/negativa
    # CPFE3-CMIG4 — backtest negativo (-R$ 1.211)
    # CIEL3-STNE3 — STNE é ADR EUA
    # ELET3-ELET6, AZUL4-GOLL4 — sem evidência cointegrada
    # EQTL3-ENGI11 — não aparece no top 60
]

PAIRS_LIST = [p['id'] for p in PAIRS_CONFIG if p.get('enabled', True)]


def get_pair(pair_id):
    """Retorna config do par pelo ID, ou None."""
    for p in PAIRS_CONFIG:
        if p['id'] == pair_id:
            return p
    return None


def all_symbols():
    """Retorna conjunto de todos os simbolos B3 usados nos pares ativos."""
    syms = set()
    for p in PAIRS_CONFIG:
        if not p.get('enabled', True):
            continue
        syms.add(p['leg_a'])
        syms.add(p['leg_b'])
    return syms


def pairs_by_tier(tier: str):
    """Lista de pares de um tier especifico (A, B, WATCH)."""
    return [p for p in PAIRS_CONFIG if p.get('enabled', True) and p.get('tier') == tier]
