"""Testa regra absoluta: derivatives/arbi SEMPRE bypass."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from modules.adaptive_learning.isolation import should_bypass_adaptive_learning

def run():
    # Derivatives
    assert should_bypass_adaptive_learning('derivative') is True
    assert should_bypass_adaptive_learning('option') is True
    assert should_bypass_adaptive_learning('future') is True

    # Strategies arbi
    assert should_bypass_adaptive_learning('stock', 'interlisted') is True
    assert should_bypass_adaptive_learning('stock', 'arbi') is True
    assert should_bypass_adaptive_learning('stock', 'pcp') is True
    assert should_bypass_adaptive_learning('crypto', 'vol_arb') is True

    # Permitidos
    assert should_bypass_adaptive_learning('stock') is False
    assert should_bypass_adaptive_learning('crypto') is False
    assert should_bypass_adaptive_learning('stock', 'day_trade') is False

    # Fail-safe: asset_type None ou desconhecido
    assert should_bypass_adaptive_learning(None) is True
    assert should_bypass_adaptive_learning('unknown_thing') is True

    print('✓ test_isolation OK (10 casos)')

if __name__ == '__main__': run()
