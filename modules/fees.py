"""
[v10.24] Fees Module
Brokerage fee simulation and calculation.
Pure functions - no mutable state.
"""

import os

# ═══════════════════════════════════════════════════════════════
# BINANCE VIP TIER CONFIG
# ═══════════════════════════════════════════════════════════════
BINANCE_VIP_TIER   = int(os.environ.get('BINANCE_VIP_TIER', 3))
USE_BNB_DISCOUNT   = bool(os.environ.get('USE_BNB_DISCOUNT', 'true').lower() == 'true')
BROKER             = 'BTG'   # B3, NYSE, Arbi via BTG | Crypto via Binance

# ═══════════════════════════════════════════════════════════════
# BINANCE FEE TABLE
# ═══════════════════════════════════════════════════════════════
# Tabela maker/taker Binance por VIP tier (valores por LADO, sem BNB)
# VIP 0:       0.100% maker + 0.100% taker = 0.200% rt
# VIP 0+BNB:   0.075% + 0.075% = 0.150% rt
# VIP 3:       0.042% + 0.060% = 0.102% rt   (elegível: vol>$20M/30d)
# VIP 3+BNB:   0.0315% + 0.045% = 0.0765% rt ← TAXA REAL com nosso volume
_BINANCE_FEES = {
    0: (0.0010, 0.0010),  # VIP 0
    1: (0.0009, 0.0010),  # VIP 1
    2: (0.0008, 0.0010),  # VIP 2
    3: (0.00042, 0.0006), # VIP 3
    4: (0.0002, 0.0004),  # VIP 4
    5: (0.00012, 0.0003), # VIP 5
}

# ═══════════════════════════════════════════════════════════════
# FEE FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def _binance_rt(vip_tier: int = None, use_bnb: bool = None) -> float:
    """Calculate Binance round-trip fee based on VIP tier and BNB discount.

    Args:
        vip_tier: Binance VIP tier (0-5). If None, uses BINANCE_VIP_TIER from config.
        use_bnb: Whether to apply BNB discount. If None, uses USE_BNB_DISCOUNT from config.

    Returns:
        Round-trip fee rate (maker + taker)
    """
    if vip_tier is None:
        vip_tier = BINANCE_VIP_TIER
    if use_bnb is None:
        use_bnb = USE_BNB_DISCOUNT

    m, t = _BINANCE_FEES.get(vip_tier, (0.001, 0.001))
    if use_bnb:
        m, t = m * 0.75, t * 0.75
    return round(m + t, 6)   # round trip = maker+taker (compra taker + venda taker)

def get_fees(vip_tier: int = None, use_bnb: bool = None) -> dict:
    """Get fee structure for all markets.

    Args:
        vip_tier: Binance VIP tier. If None, uses config.
        use_bnb: Whether to apply BNB discount. If None, uses config.

    Returns:
        dict with fees per market: B3, NYSE, CRYPTO, ARBI
    """
    crypto_fee = _binance_rt(vip_tier, use_bnb)
    return {
        'B3':    0.00030,        # BTG Day Trade: ZERO corretagem + emolumentos B3
        'NYSE':  0.00020,        # BTG US: ~0.020% rt spread+SEC
        'CRYPTO': crypto_fee,    # Binance VIP3+BNB = 0.0765% rt (or per config)
        'ARBI':  0.00010,        # BTG Day Trade: ZERO corretagem + emolumentos ~0.010% rt
    }

def calc_fee(position_value: float, market: str, asset_type: str = 'stock',
            vip_tier: int = None, use_bnb: bool = None) -> float:
    """[v10.14] Calcula taxa estimada de corretagem para uma operação round-trip.
    FEES já incorpora BNB discount via _binance_rt() — não precisa de FEES_BNB separado.

    Args:
        position_value: Position size/value
        market: Market type (B3, NYSE, CRYPTO, ARBI)
        asset_type: Asset type (stock, crypto, arbitrage, arbi)
        vip_tier: Binance VIP tier. If None, uses config.
        use_bnb: Whether to apply BNB discount. If None, uses config.

    Returns:
        Fee amount in currency
    """
    pv = abs(float(position_value or 0))
    fees = get_fees(vip_tier, use_bnb)

    if asset_type == 'stock':
        rate = fees.get(market, fees['NYSE'])
    elif asset_type == 'crypto':
        rate = fees['CRYPTO']   # já calculado com VIP tier + BNB por _binance_rt()
    else:                       # arbi — BTG Day Trade: emolumentos B3 ~0.010% rt
        # Já capturado em total_cost_estimated na abertura — não duplicar
        # Retornar só emolumentos mínimos para registro
        rate = fees['ARBI']  # 0.010%

    return round(pv * rate, 2)

# ═══════════════════════════════════════════════════════════════
# [P1-FIX 02-jul-2026] SLIPPAGE ESTIMADO (bps round-trip por mercado)
# Auditoria 02/jul: o learning treinava em P&L BRUTO (custo zero) —
# otimizando para um simulador que não existe. Estes valores alimentam
# pnl_learn/pnl_learn_pct, o alvo correto do aprendizado.
# Arbi NÃO usa isto: já modela fee+FX+slippage+aluguel no próprio P&L.
# ═══════════════════════════════════════════════════════════════
SLIPPAGE_BPS = {
    'CRYPTO': float(os.environ.get('SLIPPAGE_BPS_CRYPTO', 5.0)),   # majors líquidas ~2-3bps/lado
    'B3':     float(os.environ.get('SLIPPAGE_BPS_B3', 8.0)),       # spread médio large caps
    'NYSE':   float(os.environ.get('SLIPPAGE_BPS_NYSE', 4.0)),     # large caps líquidas
    'ARBI':   0.0,   # já embutido em total_cost_estimated
}

def calc_slippage(position_value: float, market: str) -> float:
    """Slippage estimado round-trip em moeda para uma posição."""
    pv = abs(float(position_value or 0))
    bps = SLIPPAGE_BPS.get(market, SLIPPAGE_BPS['NYSE'])
    return round(pv * bps / 10000.0, 2)

def learn_cost_pct(market: str, asset_type: str = 'stock',
                   vip_tier: int = None, use_bnb: bool = None) -> float:
    """Custo percentual round-trip (fee + slippage) para ajustar métricas de
    aprendizado calculadas a partir de pnl_pct bruto (ex.: brain_calibrator/
    brain_specialist, que leem pnl_pct direto do MySQL)."""
    fees = get_fees(vip_tier, use_bnb)
    if asset_type == 'crypto' or market == 'CRYPTO':
        rate = fees['CRYPTO']
    elif market in fees:
        rate = fees[market]
    else:
        rate = fees['NYSE']
    slip_pct = SLIPPAGE_BPS.get(market, SLIPPAGE_BPS['NYSE']) / 100.0 / 100.0
    return round(rate * 100 + slip_pct * 100, 4)   # em %

def apply_fee_to_trade(trade: dict, vip_tier: int = None, use_bnb: bool = None) -> dict:
    """[v10.14] Calcula e registra a taxa estimada de corretagem.
    IMPORTANTE: pnl e pnl_pct NÃO são alterados — permanecem como bruto
    (capital/display). [P1-FIX 02-jul-2026] O LEARNING agora usa os novos
    campos líquidos pnl_learn/pnl_learn_pct (fee + slippage descontados).
    Campos adicionados:
      - trade['pnl_gross']    = cópia do pnl bruto (igual a pnl)
      - trade['fee_estimated'] = taxa calculada
      - trade['slippage_estimated'] = slippage estimado round-trip
      - trade['pnl_net']      = pnl - fee (display)
      - trade['pnl_net_pct']  = pnl_net / position_value × 100
      - trade['pnl_learn']    = pnl - fee - slippage (ALVO DO LEARNING)
      - trade['pnl_learn_pct'] = pnl_learn / position_value × 100

    Args:
        trade: Trade dict
        vip_tier: Binance VIP tier. If None, uses config.
        use_bnb: Whether to apply BNB discount. If None, uses config.

    Returns:
        Updated trade dict with fee fields
    """
    if trade.get('_fee_applied'):
        return trade

    # [NO-FEES 22-jul-2026, decisao Beto] P&L de TODAS as estrategias e BRUTO —
    # nao descontar taxa nenhuma. pnl_net = pnl (bruto), fee = 0. Reversivel via
    # env DEDUCT_FEES=true (volta a descontar).
    if os.environ.get('DEDUCT_FEES', 'false').lower() != 'true':
        _g = float(trade.get('pnl', 0) or 0)
        trade['pnl_gross'] = _g
        trade['fee_estimated'] = 0.0
        trade['slippage_estimated'] = 0.0
        trade['pnl_net'] = _g
        _pv0 = float(trade.get('position_value', 0) or trade.get('position_size', 0) or 0)
        trade['pnl_net_pct'] = round(_g / _pv0 * 100, 4) if _pv0 > 0 else 0
        trade['pnl_learn'] = _g
        trade['pnl_learn_pct'] = trade['pnl_net_pct']
        trade['_fee_applied'] = True
        return trade

    pv    = float(trade.get('position_value', 0) or trade.get('position_size', 0) or 0)
    mkt   = trade.get('market', 'NYSE')
    atype = trade.get('asset_type', 'stock')

    # Para arbi: usar total_cost_estimated que já tem fee+slippage+fx_cost
    if atype in ('arbitrage', 'arbi') or mkt == 'ARBI':
        fee = float(trade.get('total_cost_estimated', 0) or 0)
        if fee == 0:  # fallback para trades antigas sem o campo
            fee = calc_fee(pv, mkt, atype, vip_tier, use_bnb)
    else:
        fee = calc_fee(pv, mkt, atype, vip_tier, use_bnb)

    gross = float(trade.get('pnl', 0) or 0)
    net   = round(gross - fee, 2)
    net_pct = round(net / pv * 100, 4) if pv > 0 else 0

    # [P1-FIX 02-jul-2026] slippage estimado (arbi: já dentro de total_cost_estimated)
    if atype in ('arbitrage', 'arbi') or mkt == 'ARBI':
        slip = 0.0
    else:
        slip = calc_slippage(pv, mkt)
    learn = round(gross - fee - slip, 2)
    learn_pct = round(learn / pv * 100, 4) if pv > 0 else 0

    # NUNCA sobrescreve pnl ou pnl_pct — capital/display usam bruto
    trade['pnl_gross']     = gross   # igual a pnl (bruto)
    trade['fee_estimated'] = fee
    trade['slippage_estimated'] = slip
    trade['pnl_net']       = net     # líquido = display
    trade['pnl_net_pct']   = net_pct
    trade['pnl_learn']     = learn      # [P1-FIX] alvo do learning
    trade['pnl_learn_pct'] = learn_pct  # [P1-FIX]
    trade['_fee_applied']  = True
    return trade
