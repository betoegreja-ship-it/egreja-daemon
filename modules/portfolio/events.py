"""
EventType enum + conveções canônicas de sinal.

CONVENÇÃO v11 (ÚNICA fonte de verdade para sinais de amount):

Eventos de reserva (amount SEMPRE positivo, sinal vem do event_type):
    TRADE_OPEN_RESERVE    → amount > 0, aumenta reserved_capital
    TRADE_CLOSE_RELEASE   → amount > 0, reduz reserved_capital

Eventos de equity (amount SIGNED — positivo ou negativo):
    REALIZED_PNL          → signed (+ ganho, - perda)
    FEE                   → sempre negativo (-valor absoluto)
    MANUAL_ADJUSTMENT     → signed
    SLIPPAGE_ADJUSTMENT   → signed

Eventos de funding (amount SEMPRE positivo, sinal vem do event_type):
    BASELINE              → seta initial_capital = amount
    DEPOSIT               → amount > 0, aumenta net_deposits
    WITHDRAW              → amount > 0 (magnitude), reduz net_deposits
    REBALANCE_TRANSFER_IN → amount > 0, aumenta net_deposits
    REBALANCE_TRANSFER_OUT→ amount > 0 (magnitude), reduz net_deposits

Essa convenção bate com modules/ledger.py atual — zero migração de
histórico necessária.
"""
from enum import Enum


class EventType(str, Enum):
    BASELINE = 'BASELINE'
    DEPOSIT = 'DEPOSIT'
    WITHDRAW = 'WITHDRAW'
    TRADE_OPEN_RESERVE = 'TRADE_OPEN_RESERVE'
    TRADE_CLOSE_RELEASE = 'TRADE_CLOSE_RELEASE'
    REALIZED_PNL = 'REALIZED_PNL'
    FEE = 'FEE'
    MANUAL_ADJUSTMENT = 'MANUAL_ADJUSTMENT'
    SLIPPAGE_ADJUSTMENT = 'SLIPPAGE_ADJUSTMENT'
    REBALANCE_TRANSFER_IN = 'REBALANCE_TRANSFER_IN'
    REBALANCE_TRANSFER_OUT = 'REBALANCE_TRANSFER_OUT'

    # Aliases legacy (modules/ledger.py usa "RESERVE", "RELEASE", "PNL_CREDIT")
    RESERVE_LEGACY = 'RESERVE'
    RELEASE_LEGACY = 'RELEASE'
    PNL_CREDIT_LEGACY = 'PNL_CREDIT'


# Conjuntos úteis para validação
AMOUNT_POSITIVE_ONLY = {
    EventType.BASELINE,
    EventType.DEPOSIT,
    EventType.WITHDRAW,
    EventType.TRADE_OPEN_RESERVE,
    EventType.TRADE_CLOSE_RELEASE,
    EventType.REBALANCE_TRANSFER_IN,
    EventType.REBALANCE_TRANSFER_OUT,
    EventType.RESERVE_LEGACY,
    EventType.RELEASE_LEGACY,
}

AMOUNT_SIGNED = {
    EventType.REALIZED_PNL,
    EventType.FEE,
    EventType.MANUAL_ADJUSTMENT,
    EventType.SLIPPAGE_ADJUSTMENT,
    EventType.PNL_CREDIT_LEGACY,
}


def validate_amount_sign(event_type: str, amount: float) -> None:
    """Raise ValueError se sinal de amount inconsistente com event_type."""
    try:
        et = EventType(event_type)
    except ValueError:
        raise ValueError(f'event_type desconhecido: {event_type}')
    if et in AMOUNT_POSITIVE_ONLY and amount < 0:
        raise ValueError(
            f'{event_type} exige amount positivo (convenção v11), got {amount}'
        )


def build_idempotency_key(event_type: str, strategy: str,
                           trade_id: str = None, extra: str = None) -> str:
    """
    Formato canônico de idempotency_key por event_type.
    Usado pela UNIQUE constraint em capital_ledger.idempotency_key.
    """
    prefixes = {
        EventType.TRADE_OPEN_RESERVE.value: 'OPEN',
        EventType.TRADE_CLOSE_RELEASE.value: 'REL',
        EventType.REALIZED_PNL.value: 'PNL',
        EventType.FEE.value: 'FEE',
        EventType.RESERVE_LEGACY.value: 'OPEN',
        EventType.RELEASE_LEGACY.value: 'REL',
        EventType.PNL_CREDIT_LEGACY.value: 'PNL',
    }
    prefix = prefixes.get(event_type, event_type[:6].upper())
    parts = [prefix, strategy]
    if trade_id:
        parts.append(str(trade_id))
    if extra:
        parts.append(str(extra))
    return ':'.join(parts)[:120]
