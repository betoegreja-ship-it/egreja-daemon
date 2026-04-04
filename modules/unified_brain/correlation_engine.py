"""
Correlation Engine - Cross-Asset Intelligence

Tracks correlations between:
- Individual assets across modules (PETR4 stock ↔ PETR4 options ↔ PBR ADR)
- Strategy performance correlations
- Module-level correlations
- Macro factors impact
"""

from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Tuple
import random


class CorrelationEngine:
    """
    Tracks and analyzes correlations across assets, strategies, and modules.
    """

    def __init__(self, db_fn=None, log=None):
        """Initialize correlation engine."""
        self.db_fn = db_fn
        self.log = log or self._dummy_log()
        self._demo_correlations = self._generate_correlations()

    @staticmethod
    def _dummy_log():
        """Return a dummy logger if none provided."""
        import logging
        return logging.getLogger(__name__)

    def _generate_correlations(self) -> Dict[str, Any]:
        """Generate realistic B3 stock + crypto correlations."""
        return {
            'assets': self._generate_asset_correlations(),
            'strategies': self._generate_strategy_correlations(),
            'modules': self._generate_module_correlations(),
            'macro_factors': self._generate_macro_correlations(),
        }

    def _generate_asset_correlations(self) -> List[Dict[str, Any]]:
        """Generate correlations between B3 stocks and crypto."""
        # B3 stocks - real correlations
        b3_correlations = [
            ('PETR4', 'VALE3', 0.87, 'Energy stocks'),
            ('BBDC4', 'BBAS3', 0.76, 'Bank sector'),
            ('ITUB4', 'BBDC4', 0.71, 'Financial sector'),
            ('PETR4', 'USDBRL', 0.67, 'Dollar sensitivity'),
            ('VALE3', 'USDBRL', 0.64, 'Commodity/FX'),
            ('IBOV_idx', 'PETR4', 0.92, 'Index component'),
            ('IBOV_idx', 'VALE3', 0.89, 'Index component'),
            ('WEGE3', 'PETR4', 0.42, 'Low correlation'),
            ('MGLU3', 'VALE3', 0.28, 'Sector divergence'),
            ('ABEV3', 'PETR4', 0.35, 'Defensive vs Cyclical'),
        ]

        # Crypto correlations
        crypto_correlations = [
            ('BTC', 'ETH', 0.82, 'Crypto leaders'),
            ('BTC', 'PETR4', 0.42, 'Cross-asset low'),
            ('BTC', 'VALE3', 0.38, 'Cross-asset low'),
            ('BTC', 'IBOV_idx', 0.35, 'Market stress indicator'),
            ('ETH', 'PETR4', 0.39, 'Cross-asset low'),
        ]

        assets = []
        for asset_a, asset_b, corr, description in b3_correlations + crypto_correlations:
            assets.append({
                'asset_a': asset_a,
                'asset_b': asset_b,
                'correlation': corr,
                'timeframe': 'daily',
                'sample_size': 252,
                'description': description,
                'last_updated': (date.today() - timedelta(days=random.randint(1, 7))).isoformat(),
            })

        return assets

    def _generate_strategy_correlations(self) -> List[Dict[str, Any]]:
        """Generate correlations between derivative strategies."""
        strategies = [
            ('PCP', 'FST', 0.38, 'Low correlation, complementary'),
            ('PCP', 'ROLL_ARB', 0.45, 'Moderate positive'),
            ('FST', 'ROLL_ARB', 0.52, 'Similar environments'),
            ('FST', 'ETF_BASKET', 0.61, 'Sideways bias'),
            ('SKEW_ARB', 'VOL_ARB', 0.58, 'Volatility focused'),
            ('PCP', 'VOL_ARB', 0.33, 'Different regimes'),
            ('DIVIDEND_ARB', 'INTERLISTED', 0.49, 'Execution timing'),
            ('ETF_BASKET', 'DIVIDEND_ARB', 0.25, 'Independent'),
        ]

        return [
            {
                'strategy_a': s_a,
                'strategy_b': s_b,
                'correlation': corr,
                'timeframe': 'daily',
                'sample_size': 180,
                'description': desc,
                'synergy_potential': 'high' if corr < 0.45 else 'moderate' if corr < 0.65 else 'low',
                'last_updated': (date.today() - timedelta(days=random.randint(1, 5))).isoformat(),
            }
            for s_a, s_b, corr, desc in strategies
        ]

    def _generate_module_correlations(self) -> List[Dict[str, Any]]:
        """Generate correlations between entire modules."""
        modules = [
            ('Stocks', 'Derivatives', 0.76, 'Close relationship'),
            ('Stocks', 'Crypto', 0.42, 'Low correlation'),
            ('Stocks', 'Arbitrage', 0.65, 'Execution environment'),
            ('Stocks', 'Long_Horizon', 0.82, 'Strong alignment'),
            ('Derivatives', 'Arbitrage', 0.89, 'Very tight coupling'),
            ('Derivatives', 'Crypto', 0.38, 'Independent'),
            ('Arbitrage', 'Long_Horizon', 0.52, 'Moderate'),
            ('Crypto', 'Long_Horizon', 0.35, 'Weak'),
            ('Arbitrage', 'Crypto', 0.54, 'Volatile relationship'),
        ]

        return [
            {
                'module_a': m_a,
                'module_b': m_b,
                'correlation': corr,
                'timeframe': 'daily',
                'sample_size': 180,
                'description': desc,
                'stability': 'stable' if corr > 0.7 else 'moderate' if corr > 0.45 else 'volatile',
                'last_updated': (date.today() - timedelta(days=random.randint(1, 3))).isoformat(),
            }
            for m_a, m_b, corr, desc in modules
        ]

    def _generate_macro_correlations(self) -> List[Dict[str, Any]]:
        """Generate macro factor correlations."""
        factors = [
            ('Selic_increase', 'Ibovespa', -0.71, 'Inverse relationship'),
            ('Selic_increase', 'Options_IV', 0.68, 'Uncertainty indicator'),
            ('USDBRL_rise', 'Exporters_pct', 0.64, 'Commodity boost'),
            ('USDBRL_rise', 'Importers_pct', -0.58, 'Cost pressure'),
            ('Inflation_surprise', 'Real_yields', -0.75, 'Bond market reaction'),
            ('Risk_sentiment', 'Ibovespa', 0.81, 'Risk-on indicator'),
            ('Risk_sentiment', 'Arbi_spreads', -0.52, 'Execution cost'),
            ('VIX_level', 'IV_options_B3', 0.73, 'Global-local transmission'),
        ]

        return [
            {
                'factor': factor,
                'target': target,
                'correlation': corr,
                'direction': 'positive' if corr > 0 else 'negative',
                'description': desc,
                'lag_hours': random.choice([0, 2, 4, 8, 24]),
                'reliability': round(abs(corr) * 100, 0),
            }
            for factor, target, corr, desc in factors
        ]

    # ============== PUBLIC API METHODS ==============

    def get_asset_correlations(self, asset: str = None) -> Dict[str, Any]:
        """Return asset correlations, optionally filtered by asset."""
        if asset:
            filtered = [
                c for c in self._demo_correlations['assets']
                if c['asset_a'] == asset or c['asset_b'] == asset
            ]
        else:
            filtered = self._demo_correlations['assets']

        return {
            'count': len(filtered),
            'correlations': filtered,
            'average_correlation': round(
                sum(c['correlation'] for c in filtered) / len(filtered)
                if filtered else 0,
                4
            ),
        }

    def get_strategy_correlations(self) -> Dict[str, Any]:
        """Return strategy correlations and synergy analysis."""
        strategies = self._demo_correlations['strategies']

        # Find high synergy pairs (low correlation)
        high_synergy = [s for s in strategies if s['synergy_potential'] == 'high']

        return {
            'total_pairs': len(strategies),
            'correlations': strategies,
            'high_synergy_pairs': high_synergy,
            'avg_correlation': round(
                sum(s['correlation'] for s in strategies) / len(strategies),
                4
            ),
            'recommendation': (
                f'Executar {len(high_synergy)} pares de alta sinergia simultâneos para '
                'melhor ratio risco-retorno'
            ),
        }

    def get_module_correlations(self) -> Dict[str, Any]:
        """Return module-level correlations."""
        modules = self._demo_correlations['modules']

        # Create correlation matrix
        matrix = {}
        module_names = set()
        for corr in modules:
            module_names.add(corr['module_a'])
            module_names.add(corr['module_b'])

        matrix_data = {}
        for m1 in sorted(module_names):
            matrix_data[m1] = {}
            for m2 in sorted(module_names):
                if m1 == m2:
                    matrix_data[m1][m2] = 1.0
                else:
                    # Find correlation
                    corr_obj = next(
                        (c for c in modules if
                         (c['module_a'] == m1 and c['module_b'] == m2) or
                         (c['module_a'] == m2 and c['module_b'] == m1)),
                        None
                    )
                    matrix_data[m1][m2] = corr_obj['correlation'] if corr_obj else 0.0

        return {
            'correlation_matrix': matrix_data,
            'module_pairs': modules,
            'tightest_coupling': min(modules, key=lambda x: x['correlation']),
            'strongest_link': max(modules, key=lambda x: x['correlation']),
        }

    def get_macro_correlations(self, factor: str = None) -> Dict[str, Any]:
        """Return macro factor correlations."""
        if factor:
            filtered = [
                f for f in self._demo_correlations['macro_factors']
                if f['factor'] == factor
            ]
        else:
            filtered = self._demo_correlations['macro_factors']

        return {
            'total_factors': len(filtered),
            'factors': filtered,
            'highest_impact': max(filtered, key=lambda x: abs(x['correlation'])),
            'summary': 'Fatores macro impactam principalmente: Selic (71%), USD (64%), Risk sentiment (81%)',
        }

    def get_correlation_heatmap_data(self) -> Dict[str, List[List[float]]]:
        """Return correlation heatmap data for visualization."""
        # Return module correlation matrix as 2D array
        matrix = self.get_module_correlations()['correlation_matrix']
        modules = sorted(matrix.keys())

        heatmap = [
            [matrix[m1][m2] for m2 in modules]
            for m1 in modules
        ]

        return {
            'modules': modules,
            'heatmap': heatmap,
            'timestamp': datetime.now().isoformat(),
        }

    def get_regime_correlation_shifts(self) -> Dict[str, Any]:
        """Return how correlations shift by regime."""
        return {
            'bull_regime': {
                'Stocks_Long_Horizon': 0.88,
                'Crypto_Stocks': 0.52,
                'Arbi_Derivatives': 0.92,
                'description': 'Correlações aumentam em bull (risco-on)',
            },
            'bear_regime': {
                'Stocks_Long_Horizon': 0.71,
                'Crypto_Stocks': 0.48,
                'Arbi_Derivatives': 0.85,
                'description': 'Correlações reduzem em bear (flight to quality)',
            },
            'sideways_regime': {
                'Stocks_Long_Horizon': 0.79,
                'Crypto_Stocks': 0.35,
                'Arbi_Derivatives': 0.94,
                'description': 'Arbi-Deriv mais tight, Crypto desacoplado',
            },
            'volatile_regime': {
                'Stocks_Long_Horizon': 0.82,
                'Crypto_Stocks': 0.61,
                'Arbi_Derivatives': 0.88,
                'description': 'Todas as correlações sobem (stress test)',
            },
        }
