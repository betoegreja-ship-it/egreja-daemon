"""
Reducer puro: (state, event) -> new_state.

Sem I/O, sem locks, sem side effects. Testável em isolamento total.
Chamado tanto pelo replay (rebuild) quanto pelo apply_event em runtime.

IMPORTANTE — consistência com modules/ledger.py:
  RESERVE (legacy)  == TRADE_OPEN_RESERVE
  RELEASE (legacy)  == TRADE_CLOSE_RELEASE
  PNL_CREDIT (legacy) == REALIZED_PNL
"""
import math
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from typing import Optional

from modules.portfolio.events import EventType
from modules.portfolio.state import StrategyCapitalState


ZERO = Decimal('0')


def apply_event_to_state(
    state: StrategyCapitalState,
    event_type: str,
    amount: Decimal,
    *,
    event_id: Optional[int] = None,
    event_ts: Optional[datetime] = None,
) -> StrategyCapitalState:
    """
    Produz novo state aplicando um evento. Função PURA.

    Args:
        state: state atual (imutável, será usado como base)
        event_type: string do EventType (aceita legacy aliases)
        amount: valor conforme convenção canônica v11 (ver events.py)
        event_id: id da row do ledger (para tracking)
        event_ts: timestamp do evento (para as_of)

    Returns:
        Novo StrategyCapitalState com version + 1 e derivados recalculados.
    """
    if not isinstance(amount, Decimal):
        amount = Decimal(str(amount))

    # Campos modificáveis
    initial_capital = state.initial_capital
    net_deposits = state.net_deposits
    realized_pnl = state.realized_pnl
    reserved_capital = state.reserved_capital

    et = event_type

    # ─── Despacho por event_type ────────────────────────────────────────
    if et in (EventType.BASELINE.value,):
        initial_capital = amount  # seta o baseline (pode ser diferente da config)

    elif et in (EventType.DEPOSIT.value,
                EventType.REBALANCE_TRANSFER_IN.value):
        net_deposits = net_deposits + amount

    elif et in (EventType.WITHDRAW.value,
                EventType.REBALANCE_TRANSFER_OUT.value):
        net_deposits = net_deposits - amount

    elif et in (EventType.TRADE_OPEN_RESERVE.value,
                EventType.RESERVE_LEGACY.value):
        reserved_capital = reserved_capital + amount

    elif et in (EventType.TRADE_CLOSE_RELEASE.value,
                EventType.RELEASE_LEGACY.value):
        reserved_capital = reserved_capital - amount

    elif et in (EventType.REALIZED_PNL.value,
                EventType.FEE.value,
                EventType.MANUAL_ADJUSTMENT.value,
                EventType.SLIPPAGE_ADJUSTMENT.value,
                EventType.PNL_CREDIT_LEGACY.value):
        # Amount signed — soma direto
        realized_pnl = realized_pnl + amount

    else:
        # Evento desconhecido: não muta state, mas loga warning
        # (não falha — permite evolução do schema sem quebrar replay)
        pass

    # ─── Recalcula derivados básicos ────────────────────────────────────
    gross_equity = initial_capital + net_deposits + realized_pnl
    free_capital = gross_equity - reserved_capital

    # Novos derivados dependem de config + exposure externa (current_gross_exposure),
    # que NÃO são calculados aqui. O PortfolioEngine chama
    # recompute_derived_limits() após apply_event para atualizar exposure,
    # max_positions_allowed, etc, usando ConfigLoader.

    return replace(
        state,
        initial_capital=initial_capital,
        net_deposits=net_deposits,
        realized_pnl=realized_pnl,
        reserved_capital=reserved_capital,
        gross_equity=gross_equity,
        free_capital=free_capital,
        version=state.version + 1,
        as_of=event_ts or datetime.utcnow(),
        last_event_id=event_id if event_id is not None else state.last_event_id,
    )


def recompute_derived_limits(
    state: StrategyCapitalState,
    *,
    current_gross_exposure: Decimal,
    max_gross_exposure_pct: Decimal,
    configured_max_positions: int,
    min_capital_per_trade: Decimal,
    risk_per_trade_pct: Decimal,
    open_positions_count: int,
) -> StrategyCapitalState:
    """
    Pós-reducer: atualiza exposure, limits e derivados de capacidade.

    Chamado pelo PortfolioEngine depois de apply_event_to_state ou após
    trade open/close para refletir novo current_gross_exposure.
    """
    gross_equity = state.gross_equity
    max_gross_exposure = gross_equity * max_gross_exposure_pct
    available_exposure = max(ZERO, max_gross_exposure - current_gross_exposure)
    operational_buying_power = min(state.free_capital, available_exposure)

    # Max positions dinâmico
    if min_capital_per_trade > 0:
        dynamic_positions = int(math.floor(
            float(operational_buying_power / min_capital_per_trade)
        ))
        exposure_based = int(math.floor(
            float(max_gross_exposure / min_capital_per_trade)
        ))
    else:
        dynamic_positions = 0
        exposure_based = 0

    # Limite baseado em risk (ex: risk 1% → max 50 posições, cada uma 1% do equity)
    if risk_per_trade_pct > 0:
        risk_based = int(math.floor(float(Decimal('0.5') / risk_per_trade_pct)))
    else:
        risk_based = configured_max_positions

    max_positions_allowed = max(0, min(
        configured_max_positions,
        dynamic_positions,
        exposure_based,
        risk_based,
    ))

    return replace(
        state,
        current_gross_exposure=current_gross_exposure,
        max_gross_exposure=max_gross_exposure,
        available_exposure=available_exposure,
        operational_buying_power=operational_buying_power,
        open_positions_count=open_positions_count,
        max_positions_allowed=max_positions_allowed,
    )
