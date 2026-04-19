"""Testes do SizingEngine e LimitsEngine — mock do PortfolioEngine."""
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from modules.portfolio.config_loader import StrategyConfig
from modules.portfolio.sizing_engine import SizingEngine
from modules.portfolio.limits_engine import LimitsEngine
from modules.portfolio.state import StrategyCapitalState


def _mk_config(**overrides):
    defaults = dict(
        strategy='stocks',
        initial_capital=Decimal('3500000'),
        risk_per_trade_pct=Decimal('0.01'),
        max_gross_exposure_pct=Decimal('0.80'),
        configured_max_positions=25,
        min_capital_per_trade=Decimal('50000'),
        position_hard_cap=Decimal('200000'),
        sizing_mode='risk_based',
        capital_compounding_enabled=True,
        drawdown_hard_stop_pct=Decimal('0.25'),
        drawdown_soft_warn_pct=Decimal('0.15'),
        kill_switch_active=False,
        kill_switch_reason=None,
    )
    defaults.update(overrides)
    return StrategyConfig(**defaults)


def _mk_state(**overrides):
    defaults = dict(
        strategy='stocks',
        initial_capital=Decimal('3500000'),
        net_deposits=Decimal('0'),
        realized_pnl=Decimal('0'),
        reserved_capital=Decimal('0'),
        gross_equity=Decimal('3500000'),
        free_capital=Decimal('3500000'),
        current_gross_exposure=Decimal('0'),
        max_gross_exposure=Decimal('2800000'),
        available_exposure=Decimal('2800000'),
        operational_buying_power=Decimal('2800000'),
        open_positions_count=0,
        max_positions_allowed=25,
        unrealized_pnl=None,
        version=1,
    )
    defaults.update(overrides)
    return StrategyCapitalState(**defaults)


class _MockEngine:
    def __init__(self, state, config):
        self._state = state
        self._config = config
        self.config_loader = MagicMock()
        self.config_loader.get.return_value = config

    def get_state(self, strategy):
        return self._state


@pytest.fixture
def patched_engine():
    """Patch do singleton PortfolioEngine.instance()."""
    def _factory(state, config):
        mock = _MockEngine(state, config)
        p = patch('modules.portfolio.sizing_engine.PortfolioEngine.instance',
                  return_value=mock)
        p2 = patch('modules.portfolio.limits_engine.PortfolioEngine.instance',
                   return_value=mock)
        p.start(); p2.start()
        return mock, (p, p2)
    return _factory


# ═══════════════════════════════════════════════════════════════════════
# SizingEngine tests
# ═══════════════════════════════════════════════════════════════════════

def test_sizing_normal(patched_engine):
    _, (p, p2) = patched_engine(_mk_state(), _mk_config())
    try:
        r = SizingEngine.calc_position_size('stocks', stop_distance_pct=0.02, score=70)
        # risk = 3.5M * 1% = 35000; raw = 35000/0.02 = 1,750,000; score_mult=1.0
        # Clamp: hard_cap 200000 (mais restritivo)
        assert not r['blocked']
        assert r['size'] == Decimal('200000')
        assert 'hard_cap:200000' in r['clamps_applied']
    finally:
        p.stop(); p2.stop()


def test_sizing_score_alto_bump(patched_engine):
    _, (p, p2) = patched_engine(_mk_state(), _mk_config())
    try:
        r = SizingEngine.calc_position_size('stocks', 0.02, score=90)
        assert r['score_multiplier'] == 1.5
    finally:
        p.stop(); p2.stop()


def test_sizing_kill_switch_bloqueia(patched_engine):
    _, (p, p2) = patched_engine(
        _mk_state(), _mk_config(kill_switch_active=True, kill_switch_reason='test')
    )
    try:
        r = SizingEngine.calc_position_size('stocks', 0.02, 70)
        assert r['blocked']
        assert 'kill_switch' in r['reason']
    finally:
        p.stop(); p2.stop()


def test_sizing_below_min_bloqueia(patched_engine):
    state = _mk_state(
        gross_equity=Decimal('100000'),
        free_capital=Decimal('100000'),
        available_exposure=Decimal('100000'),
    )
    _, (p, p2) = patched_engine(state, _mk_config(min_capital_per_trade=Decimal('50000')))
    try:
        # risk = 100000 * 1% = 1000; raw = 1000/0.02 = 50000. Score 55→0.8: 40000 < 50000 min
        r = SizingEngine.calc_position_size('stocks', 0.02, score=55)
        assert r['blocked']
        assert 'below_min' in r['reason']
    finally:
        p.stop(); p2.stop()


def test_sizing_free_capital_limita(patched_engine):
    state = _mk_state(
        gross_equity=Decimal('3500000'),
        free_capital=Decimal('100000'),
        reserved_capital=Decimal('3400000'),
        available_exposure=Decimal('500000'),
    )
    _, (p, p2) = patched_engine(state, _mk_config())
    try:
        r = SizingEngine.calc_position_size('stocks', 0.02, score=85)
        # size será clampado a free_capital=100000
        assert any('free_capital' in c for c in r['clamps_applied'])
        assert r['size'] <= Decimal('100000')
    finally:
        p.stop(); p2.stop()


# ═══════════════════════════════════════════════════════════════════════
# LimitsEngine tests
# ═══════════════════════════════════════════════════════════════════════

def test_limits_open_normal(patched_engine):
    _, (p, p2) = patched_engine(_mk_state(), _mk_config())
    try:
        r = LimitsEngine.can_open('stocks', proposed_size=Decimal('50000'))
        assert r['can_open']
        checks = {c[0]: c[1] for c in r['checks']}
        assert all(checks.values())
    finally:
        p.stop(); p2.stop()


def test_limits_max_positions_atingido(patched_engine):
    _, (p, p2) = patched_engine(
        _mk_state(open_positions_count=25, max_positions_allowed=25),
        _mk_config()
    )
    try:
        r = LimitsEngine.can_open('stocks')
        assert not r['can_open']
        checks = {c[0]: c[1] for c in r['checks']}
        assert not checks['max_positions']
    finally:
        p.stop(); p2.stop()


def test_limits_drawdown_hard_stop(patched_engine):
    state = _mk_state(
        gross_equity=Decimal('2500000'),     # dd de 28.5% vs initial 3.5M
        realized_pnl=Decimal('-1000000'),
    )
    _, (p, p2) = patched_engine(state, _mk_config(drawdown_hard_stop_pct=Decimal('0.25')))
    try:
        r = LimitsEngine.can_open('stocks')
        assert not r['can_open']
        checks = {c[0]: c[1] for c in r['checks']}
        assert not checks['drawdown_hard']
    finally:
        p.stop(); p2.stop()


def test_limits_proposed_size_excede_free(patched_engine):
    state = _mk_state(
        free_capital=Decimal('50000'),
        reserved_capital=Decimal('3450000'),
    )
    _, (p, p2) = patched_engine(state, _mk_config())
    try:
        r = LimitsEngine.can_open('stocks', proposed_size=Decimal('100000'))
        assert not r['can_open']
    finally:
        p.stop(); p2.stop()
