"""Testes unitários do reducer — funções puras, sem DB."""
import pytest
from decimal import Decimal

from modules.portfolio.reducer import (
    apply_event_to_state, recompute_derived_limits,
)
from modules.portfolio.state import empty_state


def _mk_state(strategy='stocks', initial=Decimal('3500000')):
    return empty_state(strategy, initial)


def test_baseline_seta_initial():
    s0 = _mk_state('stocks', Decimal('0'))
    s1 = apply_event_to_state(s0, 'BASELINE', Decimal('1000000'))
    assert s1.initial_capital == Decimal('1000000')
    assert s1.gross_equity == Decimal('1000000')
    assert s1.version == 1


def test_deposit_aumenta_equity():
    s = _mk_state()
    s = apply_event_to_state(s, 'DEPOSIT', Decimal('50000'))
    assert s.net_deposits == Decimal('50000')
    assert s.gross_equity == Decimal('3550000')
    assert s.free_capital == Decimal('3550000')  # sem reserva


def test_withdraw_reduz_equity():
    s = _mk_state()
    s = apply_event_to_state(s, 'WITHDRAW', Decimal('100000'))
    assert s.net_deposits == Decimal('-100000')
    assert s.gross_equity == Decimal('3400000')


def test_reserve_aumenta_reserved():
    s = _mk_state()
    s = apply_event_to_state(s, 'TRADE_OPEN_RESERVE', Decimal('50000'))
    assert s.reserved_capital == Decimal('50000')
    assert s.free_capital == Decimal('3450000')
    assert s.gross_equity == Decimal('3500000')  # gross não muda


def test_release_reduz_reserved():
    s = _mk_state()
    s = apply_event_to_state(s, 'TRADE_OPEN_RESERVE', Decimal('50000'))
    s = apply_event_to_state(s, 'TRADE_CLOSE_RELEASE', Decimal('50000'))
    assert s.reserved_capital == Decimal('0')
    assert s.free_capital == Decimal('3500000')


def test_realized_pnl_positivo_e_negativo():
    s = _mk_state()
    s = apply_event_to_state(s, 'REALIZED_PNL', Decimal('1500'))
    assert s.realized_pnl == Decimal('1500')
    assert s.gross_equity == Decimal('3501500')
    s = apply_event_to_state(s, 'REALIZED_PNL', Decimal('-500'))
    assert s.realized_pnl == Decimal('1000')
    assert s.gross_equity == Decimal('3501000')


def test_fee_reduz_realized_pnl():
    s = _mk_state()
    s = apply_event_to_state(s, 'FEE', Decimal('-25.50'))
    assert s.realized_pnl == Decimal('-25.50')
    assert s.gross_equity == Decimal('3499974.50')


def test_ciclo_completo_open_close_profit():
    """Simula trade completo que lucra $1000 com fee $5."""
    s = _mk_state()
    s = apply_event_to_state(s, 'TRADE_OPEN_RESERVE', Decimal('50000'))
    assert s.free_capital == Decimal('3450000')
    # Close: RELEASE + FEE + PNL
    s = apply_event_to_state(s, 'TRADE_CLOSE_RELEASE', Decimal('50000'))
    s = apply_event_to_state(s, 'FEE', Decimal('-5'))
    s = apply_event_to_state(s, 'REALIZED_PNL', Decimal('1000'))
    assert s.reserved_capital == Decimal('0')
    assert s.realized_pnl == Decimal('995')   # 1000 - 5 fee
    assert s.gross_equity == Decimal('3500995')
    assert s.free_capital == Decimal('3500995')


def test_ciclo_completo_open_close_loss():
    s = _mk_state()
    s = apply_event_to_state(s, 'TRADE_OPEN_RESERVE', Decimal('100000'))
    s = apply_event_to_state(s, 'TRADE_CLOSE_RELEASE', Decimal('100000'))
    s = apply_event_to_state(s, 'FEE', Decimal('-10'))
    s = apply_event_to_state(s, 'REALIZED_PNL', Decimal('-2500'))
    assert s.realized_pnl == Decimal('-2510')
    assert s.gross_equity == Decimal('3497490')


def test_legacy_aliases_compatibility():
    """Eventos legacy RESERVE/RELEASE/PNL_CREDIT devem funcionar."""
    s = _mk_state()
    s = apply_event_to_state(s, 'RESERVE', Decimal('30000'))
    assert s.reserved_capital == Decimal('30000')
    s = apply_event_to_state(s, 'RELEASE', Decimal('30000'))
    assert s.reserved_capital == Decimal('0')
    s = apply_event_to_state(s, 'PNL_CREDIT', Decimal('500'))
    assert s.realized_pnl == Decimal('500')


def test_version_incrementa():
    s = _mk_state()
    assert s.version == 0
    s = apply_event_to_state(s, 'DEPOSIT', Decimal('100'))
    s = apply_event_to_state(s, 'REALIZED_PNL', Decimal('50'))
    assert s.version == 2


def test_integrity_check_ok():
    s = _mk_state()
    s = apply_event_to_state(s, 'TRADE_OPEN_RESERVE', Decimal('50000'))
    s = apply_event_to_state(s, 'REALIZED_PNL', Decimal('123.45'))
    ok, msg = s.integrity_check()
    assert ok, msg


def test_recompute_derived_limits_normal():
    s = _mk_state('stocks', Decimal('3500000'))
    s = apply_event_to_state(s, 'TRADE_OPEN_RESERVE', Decimal('200000'))
    s = recompute_derived_limits(
        s,
        current_gross_exposure=Decimal('200000'),
        max_gross_exposure_pct=Decimal('0.80'),
        configured_max_positions=25,
        min_capital_per_trade=Decimal('50000'),
        risk_per_trade_pct=Decimal('0.01'),
        open_positions_count=4,
    )
    assert s.max_gross_exposure == Decimal('2800000.00')  # 3.5M * 0.80
    assert s.available_exposure == Decimal('2600000.00')   # 2.8M - 200K
    assert s.operational_buying_power == Decimal('2600000.00')
    assert s.max_positions_allowed == 25
    assert s.open_positions_count == 4


def test_max_positions_cai_com_drawdown():
    """Equity caiu de 3.5M para 1M — max_positions deve cair."""
    s = _mk_state('stocks', Decimal('3500000'))
    # Simula grande drawdown
    s = apply_event_to_state(s, 'REALIZED_PNL', Decimal('-2500000'))
    assert s.gross_equity == Decimal('1000000')
    s = recompute_derived_limits(
        s,
        current_gross_exposure=Decimal('0'),
        max_gross_exposure_pct=Decimal('0.80'),
        configured_max_positions=25,
        min_capital_per_trade=Decimal('50000'),
        risk_per_trade_pct=Decimal('0.01'),
        open_positions_count=0,
    )
    # Com equity 1M e min 50K: floor(800K / 50K) = 16 posições dinamicas
    # Mas configured = 25. Vence o menor: 16
    assert s.max_positions_allowed == 16
