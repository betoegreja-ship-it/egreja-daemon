"""Testa isolamento absoluto de derivatives."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from modules.unified_brain.advisor_common import should_bypass_ai


def test_derivatives_always_bypass():
    # Variações escritas
    for at in ['derivative', 'derivatives', 'deriv', 'DERIVATIVE', 'Option', 'future']:
        assert should_bypass_ai(at, None) is True, f'Failed for asset_type={at}'


def test_arbi_strategy_bypass():
    # Mesmo se asset_type='stock', strategy derivativa = bypass
    strategies = ['arbi', 'pcp', 'fst', 'roll_arb', 'interlisted',
                  'vol_arb', 'dividend_arb', 'di_calendar']
    for st in strategies:
        assert should_bypass_ai('stock', st) is True, f'strategy {st} should bypass'


def test_unknown_asset_type_bypass():
    # Fail-safe: asset_type desconhecido → bypass
    assert should_bypass_ai('commodity', None) is True
    assert should_bypass_ai('', None) is True
    assert should_bypass_ai(None, None) is True


def test_stock_and_crypto_not_bypassed_when_enabled():
    # Requires env vars: STOCKS_ENABLED e CRYPTO_ENABLED default=true
    os.environ.pop('ADVISOR_STOCKS_ENABLED', None)
    os.environ.pop('ADVISOR_CRYPTO_ENABLED', None)
    assert should_bypass_ai('stock', 'day_trade') is False
    assert should_bypass_ai('crypto', None) is False


def test_asset_type_disable_switch():
    os.environ['ADVISOR_STOCKS_ENABLED'] = 'false'
    try:
        assert should_bypass_ai('stock', None) is True
    finally:
        os.environ.pop('ADVISOR_STOCKS_ENABLED', None)


if __name__ == '__main__':
    test_derivatives_always_bypass()
    test_arbi_strategy_bypass()
    test_unknown_asset_type_bypass()
    test_stock_and_crypto_not_bypassed_when_enabled()
    test_asset_type_disable_switch()
    print('✅ Todos os testes de isolamento passaram')
