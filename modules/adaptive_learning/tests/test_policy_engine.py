"""Testa policy_update_engine com relatórios mockados."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from modules.adaptive_learning.policy_update_engine import generate_policy_proposals

def run():
    # Caso 1: bypass derivatives
    assert generate_policy_proposals('derivative', {}) == []

    # Caso 2: stop_loss_catastrao em stocks
    rep = {
        'confidence': {'dead_zone_suggested': [60, 80], 'inverted': False, 'bands': []},
        'pathology': {'pathologies': [{
            'type': 'STOP_LOSS_CATASTRAO', 'severity': 'HIGH',
            'n': 100, 'wr': 0.0, 'total_pnl': -200000,
            'hint': 'apertar', 'asset_type': 'stock',
        }]},
    }
    ps = generate_policy_proposals('stock', rep)
    types = [p['proposal_type'] for p in ps]
    assert 'ENV_VAR_UPDATE' in types
    # Deve ter uma proposta de ATR_SL_MULTIPLIER
    assert any('ATR_SL_MULTIPLIER' in (p.get('proposed_value') or '') for p in ps)
    # Deve ter uma proposta de dead_zone
    assert any('DEAD_ZONE' in (p.get('proposed_value') or '') for p in ps)

    # Caso 3: SHORT deficitário stocks
    rep2 = {
        'confidence': {},
        'pathology': {'pathologies': [{
            'type': 'SHORT_DEFICITARIO', 'severity': 'HIGH',
            'long_pnl': 20000, 'short_pnl': -50000, 'asset_type': 'stock',
        }]},
    }
    ps2 = generate_policy_proposals('stock', rep2)
    assert any(p.get('proposed_value') == 'ALLOW_SHORT_STOCKS=false' for p in ps2)

    # Caso 4: V3_REVERSAL crypto
    rep3 = {
        'confidence': {},
        'pathology': {'pathologies': [{
            'type': 'V3_REVERSAL_RUIM', 'n': 100, 'wr': 0.15,
            'total_pnl': -45000, 'asset_type': 'crypto',
        }]},
    }
    ps3 = generate_policy_proposals('crypto', rep3)
    assert any('V3_REVERSAL_CRYPTO_ENABLED=false' in (p.get('proposed_value') or '') for p in ps3)

    print('✓ test_policy_engine OK (4 cenários)')

if __name__ == '__main__': run()
