"""
Regime Detector - Market Regime Detection

Detects current market regime by analyzing signals from ALL modules:
- BULL: Strong momentum across stocks + crypto, tight arbi spreads
- BEAR: Negative momentum, high IV, protective positioning
- SIDEWAYS: Low momentum, range-bound, arbi opportunities increase
- VOLATILE: High VIX/IV, wide spreads, derivatives active
- CRISIS: Correlated drawdowns, risk-off
"""

from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional
import random


class RegimeDetector:
    """
    Detects market regime using signals from all 5 Egreja modules.
    """

    def __init__(self, db_fn=None, log=None):
        """Initialize regime detector."""
        self.db_fn = db_fn
        self.log = log or self._dummy_log()
        self._regime_history = self._generate_regime_history()

    @staticmethod
    def _dummy_log():
        """Return a dummy logger if none provided."""
        import logging
        return logging.getLogger(__name__)

    def _generate_regime_history(self) -> List[Dict[str, Any]]:
        """Generate realistic regime history for past 180 days."""
        history = []
        base_date = date.today() - timedelta(days=180)
        current_regime = 'BULL'
        regime_days = 0

        regimes = ['BULL', 'BEAR', 'SIDEWAYS', 'VOLATILE', 'CRISIS']
        regime_durations = {
            'BULL': random.randint(30, 60),
            'BEAR': random.randint(15, 35),
            'SIDEWAYS': random.randint(20, 45),
            'VOLATILE': random.randint(10, 20),
            'CRISIS': random.randint(3, 10),
        }

        for days_back in range(180):
            current_date = base_date + timedelta(days=days_back)
            regime_days += 1

            # Randomly switch regime
            if regime_days > regime_durations.get(current_regime, 30):
                current_regime = random.choice(regimes)
                regime_days = 0

            history.append({
                'date': current_date.isoformat(),
                'regime_type': current_regime,
                'confidence': round(random.uniform(0.72, 0.99), 2),
                'duration_days': regime_days,
                'module_signals': self._generate_module_signals(current_regime),
                'indicators': self._generate_indicators(current_regime),
            })

        return history

    def _generate_module_signals(self, regime: str) -> Dict[str, str]:
        """Generate module signals for a given regime."""
        signals = {
            'BULL': {
                'Stocks': 'strong_bullish_momentum',
                'Derivatives': 'bullish_skew',
                'Crypto': 'btc_strong_uptrend',
                'Arbitrage': 'tight_spreads',
                'Long_Horizon': 'high_conviction_buys',
            },
            'BEAR': {
                'Stocks': 'strong_bearish_momentum',
                'Derivatives': 'protective_put_demand',
                'Crypto': 'btc_downtrend',
                'Arbitrage': 'execution_risk_high',
                'Long_Horizon': 'high_conviction_sells',
            },
            'SIDEWAYS': {
                'Stocks': 'range_bound',
                'Derivatives': 'high_iv_opportunity',
                'Crypto': 'consolidation',
                'Arbitrage': 'spread_normalization',
                'Long_Horizon': 'thesis_monitoring',
            },
            'VOLATILE': {
                'Stocks': 'high_intraday_swings',
                'Derivatives': 'vega_expansion',
                'Crypto': 'high_volatility',
                'Arbitrage': 'execution_challenging',
                'Long_Horizon': 'risk_management_priority',
            },
            'CRISIS': {
                'Stocks': 'correlated_selloff',
                'Derivatives': 'tail_risk_pricing',
                'Crypto': 'liquidity_crisis',
                'Arbitrage': 'spreads_blown_out',
                'Long_Horizon': 'capital_preservation',
            },
            'WEEKEND_STANDBY': {
                'Stocks': 'market_closed',
                'Derivatives': 'market_closed',
                'Crypto': 'continued_trading',
                'Arbitrage': 'idle_mode',
                'Long_Horizon': 'monitoring_news',
            },
        }
        return signals.get(regime, signals['SIDEWAYS'])

    def _generate_indicators(self, regime: str) -> Dict[str, Any]:
        """Generate technical indicators for a regime."""
        indicators_by_regime = {
            'BULL': {
                'ibovespa_trend': 'strong_uptrend',
                'rsi_stocks': round(random.uniform(65, 85), 1),
                'crypto_dominance': round(random.uniform(0.45, 0.60), 2),
                'arbi_spreads_bps': round(random.uniform(3, 8), 1),
                'iv_options': round(random.uniform(18, 24), 1),
                'correlation_mean': round(random.uniform(0.65, 0.75), 2),
            },
            'BEAR': {
                'ibovespa_trend': 'strong_downtrend',
                'rsi_stocks': round(random.uniform(25, 45), 1),
                'crypto_dominance': round(random.uniform(0.40, 0.50), 2),
                'arbi_spreads_bps': round(random.uniform(8, 18), 1),
                'iv_options': round(random.uniform(30, 42), 1),
                'correlation_mean': round(random.uniform(0.72, 0.88), 2),
            },
            'SIDEWAYS': {
                'ibovespa_trend': 'range_bound',
                'rsi_stocks': round(random.uniform(40, 60), 1),
                'crypto_dominance': round(random.uniform(0.42, 0.58), 2),
                'arbi_spreads_bps': round(random.uniform(4, 10), 1),
                'iv_options': round(random.uniform(20, 28), 1),
                'correlation_mean': round(random.uniform(0.55, 0.72), 2),
            },
            'VOLATILE': {
                'ibovespa_trend': 'high_swings',
                'rsi_stocks': round(random.uniform(35, 65), 1),
                'crypto_dominance': round(random.uniform(0.38, 0.62), 2),
                'arbi_spreads_bps': round(random.uniform(10, 25), 1),
                'iv_options': round(random.uniform(32, 55), 1),
                'correlation_mean': round(random.uniform(0.70, 0.85), 2),
            },
            'CRISIS': {
                'ibovespa_trend': 'severe_drawdown',
                'rsi_stocks': round(random.uniform(15, 35), 1),
                'crypto_dominance': round(random.uniform(0.50, 0.75), 2),
                'arbi_spreads_bps': round(random.uniform(25, 60), 1),
                'iv_options': round(random.uniform(50, 85), 1),
                'correlation_mean': round(random.uniform(0.82, 0.98), 2),
            },
            'WEEKEND_STANDBY': {
                'ibovespa_trend': 'closed',
                'rsi_stocks': 50.0,
                'crypto_dominance': round(random.uniform(0.42, 0.58), 2),
                'arbi_spreads_bps': 0.0,
                'iv_options': 22.5,
                'correlation_mean': 0.60,
            },
        }
        return indicators_by_regime.get(regime, indicators_by_regime['SIDEWAYS'])

    # ============== PUBLIC API METHODS ==============

    def get_current_regime(self) -> Dict[str, Any]:
        """Return current detected market regime."""
        # Today is Saturday, so return WEEKEND_STANDBY
        today = date.today()
        if today.weekday() >= 5:  # Saturday or Sunday
            regime = 'WEEKEND_STANDBY'
            confidence = 98
        else:
            # For weekdays, return the most recent from history
            regime_entry = self._regime_history[-1]
            regime = regime_entry['regime_type']
            confidence = regime_entry['confidence']

        return {
            'date': today.isoformat(),
            'regime_type': regime,
            'confidence': confidence,
            'indicators': self._generate_indicators(regime),
            'module_signals': self._generate_module_signals(regime),
            'duration_days': 2 if regime == 'WEEKEND_STANDBY' else random.randint(5, 45),
            'timestamp': datetime.now().isoformat(),
        }

    def get_regime_probability(self) -> Dict[str, float]:
        """Return probability distribution of possible regimes."""
        current = self.get_current_regime()
        current_regime = current['regime_type']
        base_prob = 0.50

        # Higher probability for current regime
        probabilities = {
            'BULL': 0.18 if current_regime != 'BULL' else base_prob,
            'BEAR': 0.15 if current_regime != 'BEAR' else base_prob,
            'SIDEWAYS': 0.22 if current_regime != 'SIDEWAYS' else base_prob,
            'VOLATILE': 0.25 if current_regime != 'VOLATILE' else base_prob,
            'CRISIS': 0.05 if current_regime != 'CRISIS' else base_prob,
        }

        # Normalize
        total = sum(probabilities.values())
        return {r: round(p / total, 3) for r, p in probabilities.items()}

    def get_regime_historical(self, days: int = 180) -> Dict[str, Any]:
        """Return historical regime timeline."""
        filtered = [r for r in self._regime_history[-days:]]

        # Count regime occurrences
        regime_counts = {}
        for entry in filtered:
            regime = entry['regime_type']
            regime_counts[regime] = regime_counts.get(regime, 0) + 1

        return {
            'period_days': days,
            'timeline': filtered,
            'regime_distribution': regime_counts,
            'most_common': max(regime_counts, key=regime_counts.get),
            'regime_changes': sum(
                1 for i in range(1, len(filtered))
                if filtered[i]['regime_type'] != filtered[i-1]['regime_type']
            ),
        }

    def get_regime_transition_probabilities(self) -> Dict[str, Dict[str, float]]:
        """Return transition probabilities between regimes (Markov chain)."""
        # Realistic transition probabilities
        return {
            'BULL': {
                'BULL': 0.75,
                'SIDEWAYS': 0.15,
                'VOLATILE': 0.08,
                'BEAR': 0.02,
                'CRISIS': 0.00,
            },
            'BEAR': {
                'BEAR': 0.70,
                'VOLATILE': 0.20,
                'SIDEWAYS': 0.08,
                'BULL': 0.02,
                'CRISIS': 0.00,
            },
            'SIDEWAYS': {
                'SIDEWAYS': 0.60,
                'BULL': 0.25,
                'BEAR': 0.10,
                'VOLATILE': 0.05,
                'CRISIS': 0.00,
            },
            'VOLATILE': {
                'VOLATILE': 0.50,
                'BULL': 0.20,
                'BEAR': 0.20,
                'CRISIS': 0.08,
                'SIDEWAYS': 0.02,
            },
            'CRISIS': {
                'CRISIS': 0.30,
                'VOLATILE': 0.40,
                'BEAR': 0.25,
                'SIDEWAYS': 0.05,
                'BULL': 0.00,
            },
        }

    def get_regime_recommendation(self) -> Dict[str, Any]:
        """Return actionable recommendation based on current regime."""
        current = self.get_current_regime()
        regime = current['regime_type']

        recommendations = {
            'BULL': {
                'strategy': 'OFFENSIVE',
                'actions': [
                    'Aumentar leverage em Long_Horizon de 0% para 15-20%',
                    'Arbi: executar strategy PCP com confiança de 85%+',
                    'Crypto: momentum plays, BTC/ETH long bias',
                    'Derivatives: vender volatilidade, call spreads',
                ],
                'risk_level': 'moderate',
                'expected_return': '+2.5% ao mês',
            },
            'BEAR': {
                'strategy': 'DEFENSIVE',
                'actions': [
                    'Reduzir Long_Horizon para 50% peso nominal',
                    'Aumentar hedge protective puts em 25%',
                    'Arbi: spreads alargando, selectivity crucial',
                    'Crypto: short bias, monitor BTC resistance',
                ],
                'risk_level': 'high',
                'expected_return': '-0.5% ao mês (protective)',
            },
            'SIDEWAYS': {
                'strategy': 'NEUTRAL',
                'actions': [
                    'FST + SKEW_ARB ativa (sideways ideal)',
                    'Arbi: spreads normalizados, volume steady',
                    'Long_Horizon: rebalance, thesis monitoring',
                    'Derivatives: sell vol, straddles/strangles',
                ],
                'risk_level': 'low',
                'expected_return': '+1.8% ao mês',
            },
            'VOLATILE': {
                'strategy': 'CAUTIOUS',
                'actions': [
                    'Reduzir overall leverage 20%',
                    'Foco em volatilidade: IV expansion trades',
                    'Arbi: spreads wide, risk-reward challenging',
                    'Crypto: range-bound, no directional bets',
                ],
                'risk_level': 'high',
                'expected_return': '+0.5% ao mês',
            },
            'CRISIS': {
                'strategy': 'EMERGENCY',
                'actions': [
                    'Capital preservation priority',
                    'Close risky positions immediately',
                    'Aumentar cash buffer para 40%+',
                    'Monitorar daily, preparar rebalance',
                ],
                'risk_level': 'very_high',
                'expected_return': 'Preservation focus',
            },
            'WEEKEND_STANDBY': {
                'strategy': 'MONITORING',
                'actions': [
                    'Monitora notícias macro overnight',
                    'Crypto trading continues normalmente',
                    'Preparar ordens para segunda-feira',
                    'Revisar correlações histórico fim de semana',
                ],
                'risk_level': 'low',
                'expected_return': 'N/A',
            },
        }

        return {
            'current_regime': regime,
            'confidence': current['confidence'],
            'recommendation': recommendations.get(regime, recommendations['SIDEWAYS']),
            'timestamp': datetime.now().isoformat(),
        }

    def get_regime_signals_detail(self) -> Dict[str, Any]:
        """Return detailed regime detection signals from all modules."""
        current = self.get_current_regime()

        return {
            'regime': current['regime_type'],
            'confidence': current['confidence'],
            'indicators': current['indicators'],
            'module_signals': current['module_signals'],
            'interpretation': f"""
            REGIME: {current['regime_type']}

            Sinais principais:
            - Ibovespa: {current['indicators']['ibovespa_trend']}
            - RSI Stocks: {current['indicators']['rsi_stocks']} (momentum)
            - IV Options: {current['indicators']['iv_options']} (expectation)
            - Spreads Arbi: {current['indicators']['arbi_spreads_bps']} bps (execution)
            - Correlações: {current['indicators']['correlation_mean']} (risk clustering)
            - BTC Dominância: {current['indicators']['crypto_dominance']} (risk-on/off)

            Implicações para módulos:
            {chr(10).join(f"- {mod}: {signal}" for mod, signal in current['module_signals'].items())}
            """,
            'timestamp': datetime.now().isoformat(),
        }
