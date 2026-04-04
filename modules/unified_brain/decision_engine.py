"""
Decision Engine - AI Decision Support

Provides intelligent recommendations:
- Cross-module signals combining multiple data sources
- Market regime based recommendations
- Risk management triggers
- Opportunity detection

Each decision includes confidence, supporting factors, and risk assessment.
"""

from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional
import random


class DecisionEngine:
    """
    Generates intelligent trading and risk management decisions
    by combining signals from all 5 Egreja modules.
    """

    def __init__(self, db_fn=None, log=None, learning_engine=None, regime_detector=None):
        """Initialize decision engine."""
        self.db_fn = db_fn
        self.log = log or self._dummy_log()
        self.learning_engine = learning_engine
        self.regime_detector = regime_detector
        self._demo_decisions = self._generate_decisions()

    @staticmethod
    def _dummy_log():
        """Return a dummy logger if none provided."""
        import logging
        return logging.getLogger(__name__)

    def _generate_decisions(self) -> List[Dict[str, Any]]:
        """Generate realistic AI decisions combining multiple modules."""
        decisions = []

        # Cross-module BUY signal
        decisions.append({
            'decision_id': 'DEC_20260404_001',
            'decision_type': 'STRONG_BUY',
            'asset': 'PETR4',
            'module': 'Multi-module',
            'recommendation': (
                'PETR4 FORTE COMPRA: Score LH=78 + Arbi oportunidade PCP 12bps + '
                'Momentum RSI=32 (oversold) + Regime BULL confirmado'
            ),
            'confidence': 86,
            'reasoning': (
                'Sinal cruzado robusto: (1) Score Long Horizon 78 (histórico +4.2% em 6m), '
                '(2) Spread PCP detectado 12bps vs normal 8.5bps, (3) RSI técnico < 40, '
                '(4) Setor Energy em bull regime'
            ),
            'factors': {
                'Long_Horizon_score': 78,
                'PCP_spread_bps': 12,
                'RSI_momentum': 32,
                'Regime': 'BULL',
                'Volume_confirmation': True,
            },
            'risk_assessment': {
                'drawdown_risk': 'moderate',
                'worst_case_loss': -3.5,
                'best_case_gain': 8.2,
                'risk_reward': 2.34,
            },
            'time_horizon': '5-15 days',
            'decided_at': (date.today() - timedelta(days=5)).isoformat(),
            'status': 'active',
        })

        # Risk Alert
        decisions.append({
            'decision_id': 'DEC_20260402_051',
            'decision_type': 'RISK_ALERT',
            'asset': 'Portfolio',
            'module': 'Derivatives',
            'recommendation': (
                'ALERTA VEGA: Portfolio exposure Vega > 8.5k BRL/1% IV — '
                'reduzir posição longa 20% ou implementar hedge com calls OTM'
            ),
            'confidence': 88,
            'reasoning': (
                'Análise Greeks: Portfolio concentrado em delta-neutral positions '
                'com Vega longo excessivo. Se IV suba 5 pontos (provável em volatile regime): '
                'perda potencial 42k BRL'
            ),
            'factors': {
                'Portfolio_Vega': 8620,
                'IV_current': 22.4,
                'IV_target_stress': 27.4,
                'Potential_loss': 42000,
                'Current_margin_utilization': 0.78,
            },
            'risk_assessment': {
                'drawdown_risk': 'high',
                'worst_case_loss': -5.2,
                'mitigation_available': True,
                'hedge_cost': 425,
            },
            'time_horizon': 'immediate',
            'decided_at': (date.today() - timedelta(days=2)).isoformat(),
            'status': 'urgent',
        })

        # Regime Shift
        decisions.append({
            'decision_id': 'DEC_20260403_031',
            'decision_type': 'REGIME_SHIFT',
            'asset': 'Portfolio',
            'module': 'Multi-module',
            'recommendation': (
                'TRANSIÇÃO REGIME: BULL → VOLATILE detectada — '
                'reduzir leverage Arbi 15%, aumentar hedge, tomar ganhos 30% em longs'
            ),
            'confidence': 82,
            'reasoning': (
                'Confirmação de 3+ módulos: Crypto vol +25% em 3d, IV options +18%, '
                'spreads alargando, correlation mean subindo para 0.74. Histórico: '
                'regime shifts levam a volatilidade +40% in next 10d'
            ),
            'factors': {
                'Crypto_vol_change_pct': 25,
                'IV_change_pct': 18,
                'Arbi_spreads_change_pct': 12,
                'Correlation_mean': 0.74,
                'Modules_confirming': 4,
            },
            'risk_assessment': {
                'drawdown_risk': 'moderate',
                'transition_window_hours': 24,
                'rebalance_urgency': 'high',
            },
            'time_horizon': '1-10 days',
            'decided_at': (date.today() - timedelta(days=1)).isoformat(),
            'status': 'active',
        })

        # Timing Opportunity
        decisions.append({
            'decision_id': 'DEC_20260401_087',
            'decision_type': 'TIMING_WINDOW',
            'asset': 'PETR4-VALE3',
            'module': 'Arbitrage',
            'recommendation': (
                'JANELA TIMING: BTC pump UTC 14:30 detectado → executar arbi '
                'PETR4-VALE3 em 45-90min (padrão: sucesso 87%)'
            ),
            'confidence': 79,
            'reasoning': (
                'Padrão cross-asset confirmado: BTC > +2% em 4h → B3 energia '
                'sobe 1-2% em lag 45-120min. Histórico: 87% acurácia em 28 ocorrências. '
                'Spread atual 11bps vs normal 8.5bps = opportunity'
            ),
            'factors': {
                'BTC_move_pct': 2.3,
                'Pattern_accuracy_historical': 0.87,
                'Pattern_occurrences': 28,
                'Current_spread_bps': 11,
                'Normal_spread_bps': 8.5,
                'Time_since_trigger_min': 15,
            },
            'risk_assessment': {
                'execution_risk': 'low',
                'slippage_expected_bps': 2.1,
                'window_closing_hours': 2,
            },
            'time_horizon': '1-2 hours',
            'decided_at': (date.today() - timedelta(days=3)).isoformat(),
            'status': 'expired',
        })

        # Earnings Play
        decisions.append({
            'decision_id': 'DEC_20260327_105',
            'decision_type': 'EARNINGS_CATALYST',
            'asset': 'VALE3',
            'module': 'Long_Horizon',
            'recommendation': (
                'VALE3 EARNINGS: Score 72 + tese estrutural COMPRA + '
                'histórico +82% earnings surprises = TARGET +8% em 10-15d'
            ),
            'confidence': 75,
            'reasoning': (
                'Múltiplas evidências: (1) Score LH 72 (COMPRA conviction), '
                '(2) Tese estrutural: commodity super cycle, policy support, '
                '(3) Histórico earnings: 82% surprises positivas resultam em '
                '+5% a +12% em 10-15d window'
            ),
            'factors': {
                'LH_score': 72,
                'Earnings_surprise_historical_pct': 82,
                'Expected_move_pct': 8,
                'Days_to_earnings': 0,
                'IV_implied_move_pct': 4.5,
            },
            'risk_assessment': {
                'worst_case_loss': -6.5,
                'best_case_gain': 12.0,
                'risk_reward': 1.85,
            },
            'time_horizon': '10-15 days',
            'decided_at': (date.today() - timedelta(days=7)).isoformat(),
            'status': 'active',
        })

        # Sector Rotation
        decisions.append({
            'decision_id': 'DEC_20260320_142',
            'decision_type': 'SECTOR_ROTATION',
            'asset': 'Portfolio Sectoral',
            'module': 'Stocks',
            'recommendation': (
                'ROTAÇÃO DETECTADA: Energy overweight → reduzir PETR4/VALE3 20%, '
                'aumentar Tech 15%, Financials 10% (padrão lead: 2-3 semanas)'
            ),
            'confidence': 81,
            'reasoning': (
                'Top momentum list mudou significativamente: Tech ativos subiram '
                '3 semanas consecutivas (histórico leva 2-3 semanas antes de rotação geral). '
                'Defensivos atraindo capital. FX-neutral positioning esperado'
            ),
            'factors': {
                'Momentum_change_days': 21,
                'Historical_lead_days': 18,
                'Tech_momentum_pct_gain': 7.5,
                'Energy_momentum_change': -2.3,
            },
            'risk_assessment': {
                'execution_risk': 'moderate',
                'market_impact': 'low',
                'liquidity_constraint': False,
            },
            'time_horizon': '2-4 weeks',
            'decided_at': (date.today() - timedelta(days=10)).isoformat(),
            'status': 'active',
        })

        # Kill Switch
        decisions.append({
            'decision_id': 'DEC_20260310_201',
            'decision_type': 'KILL_SWITCH',
            'asset': 'Portfolio',
            'module': 'Risk_Management',
            'recommendation': (
                'ATIVADO: 3+ módulos stress -2%+ → liquidar 40% posições, '
                'elevar cash para 35%, aguardar claridade'
            ),
            'confidence': 91,
            'reasoning': (
                'Stress testing multi-módulo: Stocks -2.1%, Crypto -3.5%, Arbi '
                'spreads +240%. Correlação agregada 0.92 (vs normal 0.60). '
                'Risco sistêmico elevado. Protocolo automático ativado'
            ),
            'factors': {
                'Stocks_drawdown_pct': -2.1,
                'Crypto_drawdown_pct': -3.5,
                'Correlation_aggregate': 0.92,
                'Modules_stressed': 3,
            },
            'risk_assessment': {
                'drawdown_risk': 'very_high',
                'recovery_time_estimate_days': 5,
                'capital_preservation': True,
            },
            'time_horizon': 'immediate',
            'decided_at': (date.today() - timedelta(days=4)).isoformat(),
            'status': 'activated',
        })

        # Strategy Synergy
        decisions.append({
            'decision_id': 'DEC_20260305_089',
            'decision_type': 'STRATEGY_SYNERGY',
            'asset': 'Derivatives_Portfolio',
            'module': 'Derivatives',
            'recommendation': (
                'PCP + FST SIMULTÂNEOS em regime SIDEWAYS: combinar estratégias '
                'para Sharpe ratio +0.41 vs isolados'
            ),
            'confidence': 79,
            'reasoning': (
                'Análise correlação: PCP ↔ FST correlação = 0.38 (baixa), '
                'complementariedade detectada. Em sideways regime: PCP efetivo '
                '+240%, FST efetivo +180%. Combinado esperado: +320% (sinergia)'
            ),
            'factors': {
                'Strategy_correlation': 0.38,
                'PCP_sideways_return_pct': 2.4,
                'FST_sideways_return_pct': 1.8,
                'Expected_synergy_pct': 3.2,
                'Risk_diversification': 0.15,
            },
            'risk_assessment': {
                'execution_risk': 'low',
                'correlation_risk_reduction': 0.25,
            },
            'time_horizon': '5-20 days',
            'decided_at': (date.today() - timedelta(days=6)).isoformat(),
            'status': 'active',
        })

        return decisions

    # ============== PUBLIC API METHODS ==============

    def get_all_decisions(self, status: Optional[str] = None) -> Dict[str, Any]:
        """Return all AI decisions, optionally filtered by status."""
        if status:
            filtered = [d for d in self._demo_decisions if d.get('status') == status]
        else:
            filtered = self._demo_decisions

        return {
            'total_decisions': len(filtered),
            'decisions': filtered,
            'by_type': self._group_by_field(filtered, 'decision_type'),
            'by_status': self._group_by_field(filtered, 'status'),
            'timestamp': datetime.now().isoformat(),
        }

    def get_decisions_by_module(self, module: str) -> Dict[str, Any]:
        """Return decisions for a specific module."""
        filtered = [
            d for d in self._demo_decisions
            if module in d.get('module', '') or d.get('asset', '').startswith(module)
        ]

        return {
            'module': module,
            'count': len(filtered),
            'decisions': filtered,
            'average_confidence': round(
                sum(d['confidence'] for d in filtered) / len(filtered)
                if filtered else 0,
                1
            ),
        }

    def get_urgent_decisions(self) -> Dict[str, Any]:
        """Return only urgent decisions requiring immediate action."""
        urgent = [d for d in self._demo_decisions if d.get('status') in ['urgent', 'activated']]

        return {
            'urgent_count': len(urgent),
            'decisions': urgent,
            'action_required': len(urgent) > 0,
            'priority_order': sorted(urgent, key=lambda x: x['confidence'], reverse=True),
        }

    def get_opportunity_decisions(self) -> Dict[str, Any]:
        """Return only opportunity decisions (positive)."""
        opportunities = [
            d for d in self._demo_decisions
            if d['decision_type'] in ['STRONG_BUY', 'TIMING_WINDOW', 'EARNINGS_CATALYST', 'STRATEGY_SYNERGY']
            and d.get('status') != 'expired'
        ]

        return {
            'opportunities_count': len(opportunities),
            'opportunities': opportunities,
            'total_potential_return': sum(d['risk_assessment']['best_case_gain'] for d in opportunities),
        }

    def get_risk_decisions(self) -> Dict[str, Any]:
        """Return only risk management decisions."""
        risk_decisions = [
            d for d in self._demo_decisions
            if d['decision_type'] in ['RISK_ALERT', 'REGIME_SHIFT', 'KILL_SWITCH']
        ]

        return {
            'risk_alerts_count': len(risk_decisions),
            'alerts': risk_decisions,
            'action_required': any(d.get('status') == 'urgent' for d in risk_decisions),
        }

    def get_decision_summary_by_confidence(self) -> Dict[str, Any]:
        """Return decisions grouped by confidence level."""
        high_conf = [d for d in self._demo_decisions if d['confidence'] >= 85]
        medium_conf = [d for d in self._demo_decisions if 70 <= d['confidence'] < 85]
        low_conf = [d for d in self._demo_decisions if d['confidence'] < 70]

        return {
            'high_confidence_85plus': {
                'count': len(high_conf),
                'decisions': high_conf,
                'recommendation': 'Executar com confiança alta',
            },
            'medium_confidence_70_85': {
                'count': len(medium_conf),
                'decisions': medium_conf,
                'recommendation': 'Executar com validação adicional',
            },
            'lower_confidence_below_70': {
                'count': len(low_conf),
                'decisions': low_conf,
                'recommendation': 'Monitor, não executar',
            },
        }

    def get_active_positions_impact(self) -> Dict[str, Any]:
        """Analyze impact of decisions on active positions."""
        active = [d for d in self._demo_decisions if d.get('status') == 'active']

        total_potential_gain = sum(
            d['risk_assessment'].get('best_case_gain', 0) for d in active
        )
        total_potential_loss = sum(
            d['risk_assessment'].get('worst_case_loss', 0) for d in active
        )

        return {
            'active_decisions': len(active),
            'positions_affected': len(set(d['asset'] for d in active)),
            'total_potential_gain_pct': round(total_potential_gain, 2),
            'total_potential_loss_pct': round(total_potential_loss, 2),
            'net_expected_outcome': round(total_potential_gain + total_potential_loss, 2),
            'risk_reward_aggregate': round(
                abs(total_potential_gain / total_potential_loss)
                if total_potential_loss != 0 else 0,
                2
            ),
        }

    def get_decision_confidence_distribution(self) -> Dict[str, Any]:
        """Return statistical distribution of decision confidence."""
        confidences = [d['confidence'] for d in self._demo_decisions]

        return {
            'count': len(confidences),
            'average': round(sum(confidences) / len(confidences), 1),
            'min': min(confidences),
            'max': max(confidences),
            'median': sorted(confidences)[len(confidences) // 2],
            'distribution': {
                '90-100': len([c for c in confidences if c >= 90]),
                '80-89': len([c for c in confidences if 80 <= c < 90]),
                '70-79': len([c for c in confidences if 70 <= c < 80]),
                '60-69': len([c for c in confidences if 60 <= c < 70]),
                '<60': len([c for c in confidences if c < 60]),
            },
        }

    # ============== HELPER METHODS ==============

    @staticmethod
    def _group_by_field(items: List[Dict], field: str) -> Dict[str, int]:
        """Group items by a field and count."""
        groups = {}
        for item in items:
            key = item.get(field, 'unknown')
            groups[key] = groups.get(key, 0) + 1
        return groups
