"""Testes do sizing_mode='capital_fraction' e 'external'."""
from decimal import Decimal
from unittest.mock import MagicMock, patch

from modules.portfolio.config_loader import StrategyConfig
from modules.portfolio.sizing_engine import SizingEngine
from modules.portfolio.state import StrategyCapitalState


def _mk(state_over=None, cfg_over=None):
    state_defaults = dict(
        strategy='arbi',
        initial_capital=Decimal('3000000'),
        net_deposits=Decimal('0'),
        realized_pnl=Decimal('936933'),
        reserved_capital=Decimal('0'),
        gross_equity=Decimal('3936933'),
        free_capital=Decimal('3936933'),
        current_gross_exposure=Decimal('0'),
        max_gross_exposure=Decimal('3936933'),
        available_exposure=Decimal('3936933'),
        operational_buying_power=Decimal('3936933'),
        open_positions_count=0,
        max_positions_allowed=3,
        unrealized_pnl=None,
        version=1,
    )
    if state_over:
        state_defaults.update(state_over)
    state = StrategyCapitalState(**state_defaults)

    cfg_defaults = dict(
        strategy='arbi',
        initial_capital=Decimal('3000000'),
        risk_per_trade_pct=Decimal('0.005'),
        max_gross_exposure_pct=Decimal('1.0'),
        configured_max_positions=3,
        min_capital_per_trade=Decimal('100000'),
        position_hard_cap=None,
        sizing_mode='capital_fraction',
        capital_compounding_enabled=True,
        drawdown_hard_stop_pct=Decimal('0.25'),
        drawdown_soft_warn_pct=Decimal('0.15'),
        kill_switch_active=False,
        kill_switch_reason=None,
    )
    if cfg_over:
        cfg_defaults.update(cfg_over)
    cfg = StrategyConfig(**cfg_defaults)

    mock = MagicMock()
    mock.get_state.return_value = state
    mock.config_loader = MagicMock()
    mock.config_loader.get.return_value = cfg
    return mock, state, cfg


# ═══════════════════════════════════════════════════════════════════════
# capital_fraction mode
# ═══════════════════════════════════════════════════════════════════════

def test_capital_fraction_arbi_3_slots_vazios():
    """Arbi equity $3.94M, 0 abertas → size = 3.94M / 3 = $1.31M."""
    mock, state, cfg = _mk()
    with patch('modules.portfolio.sizing_engine.PortfolioEngine.instance', return_value=mock):
        r = SizingEngine.calc_position_size('arbi')
        assert not r['blocked']
        expected = Decimal('3936933') / Decimal('3')
        assert abs(r['size'] - expected.quantize(Decimal('0.01'))) < Decimal('1')
        assert r['sizing_mode'] == 'capital_fraction'


def test_capital_fraction_arbi_1_aberta_1_reservada():
    """Arbi com 1 trade aberta, reserved=$1.3M → size = free/2."""
    mock, state, cfg = _mk(state_over={
        'open_positions_count': 1,
        'reserved_capital': Decimal('1312311'),
        'free_capital': Decimal('3936933') - Decimal('1312311'),
    })
    with patch('modules.portfolio.sizing_engine.PortfolioEngine.instance', return_value=mock):
        r = SizingEngine.calc_position_size('arbi')
        assert not r['blocked']
        free = Decimal('3936933') - Decimal('1312311')
        expected = free / Decimal('2')
        assert abs(r['size'] - expected.quantize(Decimal('0.01'))) < Decimal('1')


def test_capital_fraction_arbi_3_abertas_bloqueia():
    """3 slots ocupados → blocked com reason='no_slots_available'."""
    mock, state, cfg = _mk(state_over={
        'open_positions_count': 3,
        'reserved_capital': Decimal('3936933'),
        'free_capital': Decimal('0'),
    })
    with patch('modules.portfolio.sizing_engine.PortfolioEngine.instance', return_value=mock):
        r = SizingEngine.calc_position_size('arbi')
        assert r['blocked']
        assert 'no_slots' in r['reason']


def test_capital_fraction_crypto_20_slots():
    """Crypto equity $1.43M, 20 slots, 0 abertas → size = 1.43M/20 = $71.5K."""
    mock, state, cfg = _mk(
        state_over={
            'strategy': 'crypto',
            'gross_equity': Decimal('1430946'),
            'free_capital': Decimal('1430946'),
            'max_gross_exposure': Decimal('1430946'),
            'available_exposure': Decimal('1430946'),
            'operational_buying_power': Decimal('1430946'),
            'max_positions_allowed': 20,
        },
        cfg_over={
            'strategy': 'crypto',
            'configured_max_positions': 20,
            'min_capital_per_trade': Decimal('15000'),
        }
    )
    with patch('modules.portfolio.sizing_engine.PortfolioEngine.instance', return_value=mock):
        r = SizingEngine.calc_position_size('crypto')
        assert not r['blocked']
        expected = Decimal('1430946') / Decimal('20')
        assert abs(r['size'] - expected.quantize(Decimal('0.01'))) < Decimal('1')


def test_capital_fraction_size_cresce_com_equity():
    """Se equity sobe, próxima size sobe proporcional."""
    # Cenário A: equity $3M, 0 abertas
    mock_a, _, _ = _mk(state_over={
        'gross_equity': Decimal('3000000'),
        'free_capital': Decimal('3000000'),
    })
    # Cenário B: equity $4M após lucro, 0 abertas
    mock_b, _, _ = _mk(state_over={
        'gross_equity': Decimal('4000000'),
        'free_capital': Decimal('4000000'),
        'realized_pnl': Decimal('1000000'),
    })
    with patch('modules.portfolio.sizing_engine.PortfolioEngine.instance', return_value=mock_a):
        size_a = SizingEngine.calc_position_size('arbi')['size']
    with patch('modules.portfolio.sizing_engine.PortfolioEngine.instance', return_value=mock_b):
        size_b = SizingEngine.calc_position_size('arbi')['size']
    assert size_b > size_a
    assert abs((size_b - size_a) - Decimal('333333.33')) < Decimal('1')


def test_capital_fraction_size_cai_com_drawdown():
    """Se equity cai, size cai."""
    mock, _, _ = _mk(state_over={
        'gross_equity': Decimal('2000000'),  # perdeu $1.94M
        'free_capital': Decimal('2000000'),
        'realized_pnl': Decimal('-1000000'),
    })
    with patch('modules.portfolio.sizing_engine.PortfolioEngine.instance', return_value=mock):
        r = SizingEngine.calc_position_size('arbi')
        expected = Decimal('2000000') / Decimal('3')
        assert abs(r['size'] - expected.quantize(Decimal('0.01'))) < Decimal('1')


def test_capital_fraction_respeita_min_per_trade():
    """Se size calculado < min, bloqueia."""
    mock, _, _ = _mk(
        state_over={
            'gross_equity': Decimal('200000'),
            'free_capital': Decimal('200000'),
        },
        cfg_over={
            'min_capital_per_trade': Decimal('100000'),
        }
    )
    with patch('modules.portfolio.sizing_engine.PortfolioEngine.instance', return_value=mock):
        # 200K / 3 slots = $66K < $100K min → blocked
        r = SizingEngine.calc_position_size('arbi')
        assert r['blocked']
        assert 'below_min' in r['reason']


# ═══════════════════════════════════════════════════════════════════════
# external mode
# ═══════════════════════════════════════════════════════════════════════

def test_external_mode_delega_sizing():
    """Modo external: retorna size=0 sem bloqueio, com free_capital exposto."""
    mock, _, _ = _mk(cfg_over={'sizing_mode': 'external'})
    with patch('modules.portfolio.sizing_engine.PortfolioEngine.instance', return_value=mock):
        r = SizingEngine.calc_position_size('arbi')
        assert not r['blocked']
        assert r['size'] == Decimal('0')
        assert 'externo' in r['reason']
        assert 'free_capital' in r
