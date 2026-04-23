"""Testa helpers estatísticos."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from modules.adaptive_learning.stats_helpers import (
    wilson_ci, profit_factor, expectancy, classify_actionability,
    is_confidence_inverted,
)

def run():
    # Wilson CI
    low, high = wilson_ci(50, 100)
    assert 0.40 <= low <= 0.45, f'wilson low {low}'
    assert 0.55 <= high <= 0.60, f'wilson high {high}'
    assert wilson_ci(0, 0) == (0.0, 0.0)

    # Profit factor
    assert profit_factor(100, -50) == 2.0
    assert profit_factor(50, -100) == 0.5
    assert profit_factor(0, -100) == 0.0
    assert profit_factor(100, 0) is None

    # Expectancy
    assert abs(expectancy(0.5, 100, -50) - 25.0) < 1e-6

    # Actionability
    assert classify_actionability(10, 0.5, 1.0) == 'GREY'          # poucos samples
    assert classify_actionability(50, 0.3, 0.5) == 'RED'           # tóxico
    assert classify_actionability(150, 0.70, 1.6, 0.7) == 'GOLD'   # excelente
    assert classify_actionability(60, 0.58, 1.3) == 'GREEN'
    assert classify_actionability(30, 0.50, 1.0) == 'YELLOW'

    # Inversão de confidence
    bands_inv = [
        {'total_pnl': 5000},
        {'total_pnl': 2000},
        {'total_pnl': -1000},
        {'total_pnl': -5000},
        {'total_pnl': -10000},
    ]
    assert is_confidence_inverted(bands_inv) is True

    bands_norm = [
        {'total_pnl': -5000},
        {'total_pnl': -1000},
        {'total_pnl': 1000},
        {'total_pnl': 5000},
        {'total_pnl': 10000},
    ]
    assert is_confidence_inverted(bands_norm) is False

    print('✓ test_stats_helpers OK (14 casos)')

if __name__ == '__main__': run()
