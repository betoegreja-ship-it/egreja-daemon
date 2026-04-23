"""Regra absoluta de isolamento: derivatives e arbi NUNCA entram no Adaptive Learning.

Esta função é chamada no INÍCIO de TODO método público do sistema. Se retorna True,
o chamador deve retornar imediatamente sem ler DB nem gerar proposals.

Seguindo o mesmo padrão do Advisor V4 (should_bypass_ai) — mesma filosofia:
- derivatives/arbitragem têm lógica de execução própria e otimizada
- misturar o learning deles com stocks/crypto cria ruído estatístico
- nenhuma policy proposal pode ser APLICADA a derivatives/arbi
"""
from __future__ import annotations
from typing import Optional


_BYPASS_ASSET_TYPES = {
    'derivative', 'derivatives', 'deriv', 'option', 'future', 'fut', 'opt',
}

_BYPASS_STRATEGIES = {
    'arbi', 'pcp', 'fst', 'roll_arb', 'etf_basket', 'skew_arb',
    'interlisted', 'interlisted_hedged', 'dividend_arb',
    'vol_arb', 'ibov_basis', 'di_calendar', 'derivatives',
}

_ALLOWED_ASSET_TYPES = {'stock', 'stocks', 'crypto', 'equity'}


def should_bypass_adaptive_learning(
    asset_type: Optional[str],
    strategy: Optional[str] = None,
) -> bool:
    """Retorna True se Adaptive Learning NÃO deve analisar/propor nada.

    Regras (em ordem):
    1. asset_type em _BYPASS_ASSET_TYPES -> bypass (derivatives/options/futures)
    2. strategy em _BYPASS_STRATEGIES -> bypass (arbi/pcp/fst/etc.)
    3. asset_type == None ou desconhecido -> bypass (fail-safe)
    4. asset_type em _ALLOWED_ASSET_TYPES -> NÃO bypass (stock/crypto)
    """
    if asset_type is None:
        return True
    at = str(asset_type).strip().lower()
    if at in _BYPASS_ASSET_TYPES:
        return True
    if strategy is not None:
        st = str(strategy).strip().lower()
        if st in _BYPASS_STRATEGIES:
            return True
    if at in _ALLOWED_ASSET_TYPES:
        return False
    # asset_type desconhecido — fail-safe
    return True


def allowed_asset_filter_sql() -> str:
    """Gera cláusula SQL WHERE para filtrar SÓ stock/crypto em queries.
    Uso: f"SELECT ... FROM trades WHERE {allowed_asset_filter_sql()}"
    """
    return "asset_type IN ('stock', 'crypto')"
