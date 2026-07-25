# -*- coding: utf-8 -*-
"""[25-jul-2026, decisao Beto — Tier 1 logging de execucao] Metricas de custo
de execucao REAL, com a convencao do revisor: implementation_shortfall.

  POSITIVO = custo (execucao pior que a referencia)
  NEGATIVO = execucao favoravel

BUY/COVER  : (fill - ref) / ref * 10000   (comprou acima do ref -> custo +)
SELL/SHORT : (ref - fill) / ref * 10000   (vendeu abaixo do ref -> custo +)

total_cost_bps soma o shortfall (com sinal) + fee + taxas/funding/borrow,
NUNCA descartando shortfall favoravel (o revisor foi explicito).
"""


def implementation_shortfall_bps(side, price_ref, price_fill):
    try:
        pr = float(price_ref or 0); pf = float(price_fill or 0)
        if pr <= 0 or pf <= 0:
            return None
        s = str(side or '').upper()
        if s in ('BUY', 'COVER', 'LONG'):
            return round((pf - pr) / pr * 10000, 3)
        if s in ('SELL', 'SHORT'):
            return round((pr - pf) / pr * 10000, 3)
        return None
    except Exception:
        return None


def total_cost_bps(shortfall_bps, fee_usd, notional_usd, extra_bps=0.0):
    """fee convertida em bps sobre o notional + shortfall (com sinal) + extras."""
    try:
        fee_bps = 0.0
        if fee_usd and notional_usd and float(notional_usd) > 0:
            fee_bps = float(fee_usd) / float(notional_usd) * 10000
        sf = float(shortfall_bps) if shortfall_bps is not None else 0.0
        return round(sf + fee_bps + float(extra_bps or 0), 3)
    except Exception:
        return None
