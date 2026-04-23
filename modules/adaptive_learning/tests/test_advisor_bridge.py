"""Smoke test do advisor_bridge — só chamadas básicas, sem DB."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from modules.adaptive_learning.advisor_bridge import (
    get_pattern_verdict, get_confidence_penalty, clear_cache, _apply_penalty,
)

def run():
    # Bypass derivatives
    assert get_pattern_verdict(None, None, 'abc', 'derivative') is None
    assert get_confidence_penalty(None, None, 'derivative', 75.0) == 0

    # Sem hash
    assert get_pattern_verdict(None, None, None, 'stock') is None

    # _apply_penalty: dead zone
    bands = {'50-80': {'band_lower': 50, 'band_upper': 80, 'inversion_flag': 0,
                       'recommended_dead_zone': 1, 'sample_size': 200, 'total_pnl': -10000}}
    assert _apply_penalty(bands, 65) == -10

    # _apply_penalty: inversion + high conf
    bands2 = {'70-80': {'band_lower': 70, 'band_upper': 80, 'inversion_flag': 1,
                        'recommended_dead_zone': 0, 'sample_size': 150, 'total_pnl': -5000}}
    assert _apply_penalty(bands2, 75) == -15

    # _apply_penalty: amostra pequena -> 0
    bands3 = {'70-80': {'band_lower': 70, 'band_upper': 80, 'inversion_flag': 1,
                        'recommended_dead_zone': 0, 'sample_size': 10, 'total_pnl': -5000}}
    assert _apply_penalty(bands3, 75) == 0

    # _apply_penalty: fora de todas as bandas
    assert _apply_penalty(bands, 90) == 0

    print('✓ test_advisor_bridge OK (6 casos)')

if __name__ == '__main__': run()
