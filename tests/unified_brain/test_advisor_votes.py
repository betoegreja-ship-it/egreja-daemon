"""Testa votos individuais e orquestração."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
os.environ['ADVISOR_ENTRY_ENABLED'] = 'true'
os.environ['ADVISOR_EXIT_ENABLED'] = 'true'

from modules.unified_brain.advisor_entry import _calendar_vote, _bypass_decision
from modules.unified_brain.advisor_exit import _pnl_protection_vote, _time_decay_vote
from modules.unified_brain.advisor_common import (
    entry_weights, exit_weights,
    ENTRY_BLOCK_MAX, ENTRY_REDUCE_MAX, ENTRY_PASS_MAX,
    is_crypto_in_trailing_protection,
)


def test_weights_sum_to_one():
    ew = entry_weights()
    assert abs(sum(ew.values()) - 1.0) < 0.01, f'entry weights sum: {sum(ew.values())}'
    xw = exit_weights()
    assert abs(sum(xw.values()) - 1.0) < 0.01, f'exit weights sum: {sum(xw.values())}'


def test_calendar_late_session_lowers_vote():
    late = _calendar_vote(asset_type='stock', hour_of_day=15, weekday=2)
    early = _calendar_vote(asset_type='stock', hour_of_day=10, weekday=2)
    assert late['vote'] < early['vote'], f'late={late}, early={early}'


def test_crypto_weekend_lowers_vote():
    sat = _calendar_vote(asset_type='crypto', hour_of_day=12, weekday=5)
    wed = _calendar_vote(asset_type='crypto', hour_of_day=12, weekday=2)
    assert sat['vote'] < wed['vote']


def test_pnl_protection_deep_drawdown():
    v = _pnl_protection_vote(current_pnl_pct=0.3, peak_pnl_pct=2.5,
                              asset_type='stock')
    assert v['vote'] > 0.7, f'deep dd should vote high: {v}'


def test_pnl_protection_stable():
    v = _pnl_protection_vote(current_pnl_pct=1.9, peak_pnl_pct=2.0,
                              asset_type='stock')
    assert v['vote'] < 0.4, f'tight drawdown should vote low: {v}'


def test_time_decay_overstay():
    v = _time_decay_vote(holding_minutes=600, asset_type='stock',
                          current_pnl_pct=0.1)
    assert v['vote'] > 0.7


def test_time_decay_early():
    v = _time_decay_vote(holding_minutes=30, asset_type='stock',
                          current_pnl_pct=0.5)
    assert v['vote'] < 0.3


def test_crypto_trailing_protection_intocavel():
    # Com TRAILING_PEAK_CRYPTO=0.4 (Railway atual),
    # peak >= 0.4 significa zona protegida pelo motor
    os.environ['TRAILING_PEAK_CRYPTO'] = '0.4'
    assert is_crypto_in_trailing_protection(0.2, 0.5) is True
    assert is_crypto_in_trailing_protection(0.1, 0.3) is False
    # Peak atingiu o threshold mesmo se atual caiu: ainda protegido
    assert is_crypto_in_trailing_protection(-0.1, 0.6) is True


def test_bypass_decision_neutral():
    d = _bypass_decision('test')
    assert d['approve'] is True
    assert d['size_multiplier'] == 1.0
    assert d['action'] == 'pass'


def test_thresholds_monotonic():
    assert ENTRY_BLOCK_MAX < ENTRY_REDUCE_MAX < ENTRY_PASS_MAX < 1.0


if __name__ == '__main__':
    test_weights_sum_to_one()
    test_calendar_late_session_lowers_vote()
    test_crypto_weekend_lowers_vote()
    test_pnl_protection_deep_drawdown()
    test_pnl_protection_stable()
    test_time_decay_overstay()
    test_time_decay_early()
    test_crypto_trailing_protection_intocavel()
    test_bypass_decision_neutral()
    test_thresholds_monotonic()
    print('✅ Todos os testes de votos passaram')
