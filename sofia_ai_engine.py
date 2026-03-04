#!/usr/bin/env python3
"""
Sofia IA Engine - Sistema de Aprendizado Contínuo para Trading
Integra análise de mercado, detecção de padrões e aprendizado com histórico
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import numpy as np
from collections import defaultdict
import hashlib

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SofiaAIEngine:
    """Motor de IA Sofia com aprendizado contínuo"""
    
    def __init__(self, learning_history_file: str = 'data/sofia_learning.json'):
        """Inicializar Sofia IA Engine"""
        self.learning_history_file = learning_history_file
        self.learning_history = self._load_learning_history()
        self.pattern_database = defaultdict(list)
        self.market_behavior = {}
        self.accuracy_metrics = {}
        
        logger.info("✅ Sofia IA Engine Inicializado")
        logger.info(f"   Histórico de aprendizado carregado: {len(self.learning_history)} registros")
    
    def _load_learning_history(self) -> List[Dict]:
        """Carregar histórico de aprendizado"""
        try:
            with open(self.learning_history_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"⚠️ Arquivo de histórico não encontrado: {self.learning_history_file}")
            return []
    
    def _save_learning_history(self):
        """Salvar histórico de aprendizado"""
        import os
        os.makedirs(os.path.dirname(self.learning_history_file), exist_ok=True)
        
        with open(self.learning_history_file, 'w') as f:
            json.dump(self.learning_history, f, indent=2, default=str)
    
    def analyze_market_behavior(self, market_data: Dict) -> Dict:
        """Analisar comportamento do mercado"""
        
        analysis = {
            'timestamp': datetime.now().isoformat(),
            'volatility': self._calculate_volatility(market_data),
            'trend': self._detect_trend(market_data),
            'momentum': self._calculate_momentum(market_data),
            'support_resistance': self._find_support_resistance(market_data),
            'correlations': self._analyze_correlations(market_data),
        }
        
        return analysis
    
    def _calculate_volatility(self, market_data: Dict) -> Dict:
        """Calcular volatilidade"""
        
        volatility_data = {}
        
        for symbol, data in market_data.items():
            if 'prices' in data and len(data['prices']) > 1:
                prices = np.array(data['prices'])
                returns = np.diff(prices) / prices[:-1]
                volatility = np.std(returns) * 100
                
                volatility_data[symbol] = {
                    'value': float(volatility),
                    'level': 'ALTA' if volatility > 2 else 'MÉDIA' if volatility > 1 else 'BAIXA',
                    'risk_level': 'ALTO' if volatility > 2 else 'MÉDIO' if volatility > 1 else 'BAIXO'
                }
        
        return volatility_data
    
    def _detect_trend(self, market_data: Dict) -> Dict:
        """Detectar tendência de mercado"""
        
        trends = {}
        
        for symbol, data in market_data.items():
            if 'prices' in data and len(data['prices']) > 5:
                prices = np.array(data['prices'])
                
                # Média móvel simples
                sma_short = np.mean(prices[-5:])
                sma_long = np.mean(prices[-20:]) if len(prices) >= 20 else np.mean(prices)
                
                current_price = prices[-1]
                
                if current_price > sma_short > sma_long:
                    trend = 'FORTE ALTA'
                    strength = 'MUITO FORTE'
                elif current_price > sma_short:
                    trend = 'ALTA'
                    strength = 'FORTE'
                elif current_price < sma_short < sma_long:
                    trend = 'FORTE BAIXA'
                    strength = 'MUITO FORTE'
                elif current_price < sma_short:
                    trend = 'BAIXA'
                    strength = 'FORTE'
                else:
                    trend = 'LATERAL'
                    strength = 'FRACA'
                
                trends[symbol] = {
                    'direction': trend,
                    'strength': strength,
                    'current_price': float(current_price),
                    'sma_short': float(sma_short),
                    'sma_long': float(sma_long)
                }
        
        return trends
    
    def _calculate_momentum(self, market_data: Dict) -> Dict:
        """Calcular momentum do mercado"""
        
        momentum_data = {}
        
        for symbol, data in market_data.items():
            if 'prices' in data and len(data['prices']) > 1:
                prices = np.array(data['prices'])
                
                # Taxa de mudança
                price_change = prices[-1] - prices[-2]
                price_change_pct = (price_change / prices[-2]) * 100
                
                # Momentum (mudança em relação a 5 períodos atrás)
                if len(prices) > 5:
                    momentum = prices[-1] - prices[-6]
                    momentum_pct = (momentum / prices[-6]) * 100
                else:
                    momentum = price_change
                    momentum_pct = price_change_pct
                
                momentum_data[symbol] = {
                    'change': float(price_change),
                    'change_pct': float(price_change_pct),
                    'momentum': float(momentum),
                    'momentum_pct': float(momentum_pct),
                    'direction': 'POSITIVO' if momentum > 0 else 'NEGATIVO'
                }
        
        return momentum_data
    
    def _find_support_resistance(self, market_data: Dict) -> Dict:
        """Encontrar suportes e resistências"""
        
        support_resistance = {}
        
        for symbol, data in market_data.items():
            if 'prices' in data and len(data['prices']) > 10:
                prices = np.array(data['prices'])
                
                # Encontrar máximos e mínimos locais
                local_max = np.max(prices[-20:]) if len(prices) >= 20 else np.max(prices)
                local_min = np.min(prices[-20:]) if len(prices) >= 20 else np.min(prices)
                current = prices[-1]
                
                support_resistance[symbol] = {
                    'resistance': float(local_max),
                    'support': float(local_min),
                    'current_price': float(current),
                    'distance_to_resistance': float(local_max - current),
                    'distance_to_support': float(current - local_min),
                    'range': float(local_max - local_min)
                }
        
        return support_resistance
    
    def _analyze_correlations(self, market_data: Dict) -> Dict:
        """Analisar correlações entre ativos"""
        
        correlations = {}
        
        # Extrair preços de todos os símbolos
        symbols = list(market_data.keys())
        price_matrix = []
        
        for symbol in symbols:
            if 'prices' in market_data[symbol]:
                price_matrix.append(market_data[symbol]['prices'])
        
        if len(price_matrix) > 1:
            # Calcular matriz de correlação
            price_array = np.array(price_matrix)
            corr_matrix = np.corrcoef(price_array)
            
            # Armazenar correlações
            for i, sym1 in enumerate(symbols):
                correlations[sym1] = {}
                for j, sym2 in enumerate(symbols):
                    if i != j:
                        correlations[sym1][sym2] = float(corr_matrix[i][j])
        
        return correlations
    
    def detect_trading_patterns(self, market_data: Dict, historical_trades: List[Dict]) -> List[Dict]:
        """Detectar padrões de trading baseado em histórico"""
        
        patterns = []
        
        # Analisar histórico de trades bem-sucedidos
        successful_trades = [t for t in historical_trades if t.get('pnl', 0) > 0]
        
        if not successful_trades:
            logger.warning("⚠️ Nenhum trade bem-sucedido no histórico")
            return patterns
        
        # Agrupar por símbolo
        trades_by_symbol = defaultdict(list)
        for trade in successful_trades:
            trades_by_symbol[trade['symbol']].append(trade)
        
        # Analisar padrões por símbolo
        for symbol, trades in trades_by_symbol.items():
            if symbol in market_data:
                avg_pnl = np.mean([t['pnl'] for t in trades])
                avg_pnl_pct = np.mean([t['pnl_pct'] for t in trades])
                success_rate = len(trades) / len([t for t in historical_trades if t['symbol'] == symbol])
                
                pattern = {
                    'symbol': symbol,
                    'pattern_type': 'HISTÓRICO_SUCESSO',
                    'confidence': float(success_rate * 100),
                    'avg_profit': float(avg_pnl),
                    'avg_profit_pct': float(avg_pnl_pct),
                    'trades_analyzed': len(trades),
                    'recommendation': 'COMPRAR' if avg_pnl_pct > 0 else 'VENDER',
                    'timestamp': datetime.now().isoformat()
                }
                
                patterns.append(pattern)
        
        return patterns
    
    def generate_trading_suggestion(self, symbol: str, market_analysis: Dict, 
                                   patterns: List[Dict], current_price: float) -> Dict:
        """Gerar sugestão de trading inteligente"""
        
        suggestion = {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'current_price': current_price,
            'recommendation': None,
            'confidence': 0,
            'reasoning': [],
            'risk_level': 'MÉDIO',
            'profit_target': None,
            'stop_loss': None
        }
        
        # Verificar análise de mercado
        if symbol in market_analysis.get('trend', {}):
            trend = market_analysis['trend'][symbol]
            
            if 'ALTA' in trend['direction']:
                suggestion['recommendation'] = 'BUY'
                suggestion['reasoning'].append(f"Tendência {trend['direction']} detectada")
                suggestion['confidence'] += 30
            elif 'BAIXA' in trend['direction']:
                suggestion['recommendation'] = 'SELL'
                suggestion['reasoning'].append(f"Tendência {trend['direction']} detectada")
                suggestion['confidence'] += 30
        
        # Verificar padrões históricos
        symbol_patterns = [p for p in patterns if p['symbol'] == symbol]
        if symbol_patterns:
            best_pattern = max(symbol_patterns, key=lambda x: x['confidence'])
            suggestion['recommendation'] = best_pattern['recommendation']
            suggestion['reasoning'].append(f"Padrão histórico: {best_pattern['avg_profit_pct']:.2f}% de lucro médio")
            suggestion['confidence'] += best_pattern['confidence'] * 0.3
        
        # Calcular metas
        if suggestion['recommendation'] == 'BUY':
            suggestion['profit_target'] = current_price * 1.025  # 2.5% de lucro
            suggestion['stop_loss'] = current_price * 0.98      # 2% de perda
            suggestion['risk_level'] = 'BAIXO'
        elif suggestion['recommendation'] == 'SELL':
            suggestion['profit_target'] = current_price * 0.975  # 2.5% de lucro
            suggestion['stop_loss'] = current_price * 1.02       # 2% de perda
            suggestion['risk_level'] = 'BAIXO'
        
        # Limitar confiança
        suggestion['confidence'] = min(100, suggestion['confidence'])
        
        return suggestion
    
    def learn_from_trade(self, trade: Dict) -> Dict:
        """Aprender com resultado de trade"""
        
        learning_record = {
            'trade_id': trade.get('id'),
            'symbol': trade['symbol'],
            'entry_price': trade['entry_price'],
            'exit_price': trade['exit_price'],
            'pnl': trade['pnl'],
            'pnl_pct': trade['pnl_pct'],
            'duration': trade.get('duration_horas', 0),
            'success': trade['pnl'] > 0,
            'timestamp': datetime.now().isoformat(),
            'analysis': {
                'market_conditions': trade.get('market_conditions', {}),
                'entry_reason': trade.get('reason', 'UNKNOWN'),
                'exit_reason': trade.get('reason', 'UNKNOWN')
            }
        }
        
        # Adicionar ao histórico
        self.learning_history.append(learning_record)
        self._save_learning_history()
        
        # Atualizar métrica de acurácia
        symbol = trade['symbol']
        if symbol not in self.accuracy_metrics:
            self.accuracy_metrics[symbol] = {'total': 0, 'successful': 0}
        
        self.accuracy_metrics[symbol]['total'] += 1
        if trade['pnl'] > 0:
            self.accuracy_metrics[symbol]['successful'] += 1
        
        logger.info(f"✅ Sofia aprendeu com trade {trade['id']}")
        logger.info(f"   Símbolo: {symbol}")
        logger.info(f"   P&L: ${trade['pnl']:,.2f} ({trade['pnl_pct']:.2f}%)")
        logger.info(f"   Acurácia: {self._get_accuracy(symbol):.1f}%")
        
        return learning_record
    
    def _get_accuracy(self, symbol: str) -> float:
        """Obter acurácia para símbolo"""
        if symbol in self.accuracy_metrics:
            metrics = self.accuracy_metrics[symbol]
            if metrics['total'] > 0:
                return (metrics['successful'] / metrics['total']) * 100
        return 0
    
    def get_performance_insights(self) -> Dict:
        """Obter insights de performance"""
        
        if not self.learning_history:
            return {}
        
        trades = self.learning_history
        successful = [t for t in trades if t['success']]
        failed = [t for t in trades if not t['success']]
        
        insights = {
            'total_trades': len(trades),
            'successful_trades': len(successful),
            'failed_trades': len(failed),
            'success_rate': (len(successful) / len(trades) * 100) if trades else 0,
            'avg_profit': np.mean([t['pnl'] for t in successful]) if successful else 0,
            'avg_loss': np.mean([t['pnl'] for t in failed]) if failed else 0,
            'best_symbol': self._get_best_symbol(),
            'worst_symbol': self._get_worst_symbol(),
            'learning_progress': self._calculate_learning_progress(),
            'recommendations': self._generate_recommendations()
        }
        
        return insights
    
    def _get_best_symbol(self) -> str:
        """Obter melhor símbolo"""
        if not self.accuracy_metrics:
            return 'N/A'
        
        best = max(self.accuracy_metrics.items(), 
                  key=lambda x: x[1]['successful'] / max(x[1]['total'], 1))
        return best[0]
    
    def _get_worst_symbol(self) -> str:
        """Obter pior símbolo"""
        if not self.accuracy_metrics:
            return 'N/A'
        
        worst = min(self.accuracy_metrics.items(), 
                   key=lambda x: x[1]['successful'] / max(x[1]['total'], 1))
        return worst[0]
    
    def _calculate_learning_progress(self) -> Dict:
        """Calcular progresso de aprendizado"""
        
        if len(self.learning_history) < 2:
            return {'status': 'INICIANDO', 'progress': 0}
        
        # Dividir em duas metades
        mid = len(self.learning_history) // 2
        first_half = self.learning_history[:mid]
        second_half = self.learning_history[mid:]
        
        first_success_rate = len([t for t in first_half if t['success']]) / len(first_half) if first_half else 0
        second_success_rate = len([t for t in second_half if t['success']]) / len(second_half) if second_half else 0
        
        improvement = (second_success_rate - first_success_rate) * 100
        
        return {
            'first_half_success_rate': float(first_success_rate * 100),
            'second_half_success_rate': float(second_success_rate * 100),
            'improvement': float(improvement),
            'status': 'MELHORANDO' if improvement > 0 else 'ESTÁVEL' if improvement == 0 else 'PIORANDO'
        }
    
    def _generate_recommendations(self) -> List[str]:
        """Gerar recomendações baseadas em aprendizado"""
        
        recommendations = []
        
        if not self.learning_history:
            return ["Comece com operações pequenas para aprender"]
        
        # Analisar padrões
        success_rate = len([t for t in self.learning_history if t['success']]) / len(self.learning_history)
        
        if success_rate < 0.4:
            recommendations.append("Taxa de sucesso baixa - reduza tamanho das operações")
        elif success_rate > 0.6:
            recommendations.append("Taxa de sucesso alta - considere aumentar tamanho das operações")
        
        # Analisar duração
        avg_duration = np.mean([t['duration'] for t in self.learning_history])
        if avg_duration < 0.5:
            recommendations.append("Operações muito rápidas - aumente tempo de análise")
        elif avg_duration > 2:
            recommendations.append("Operações muito longas - implemente saída mais rápida")
        
        return recommendations


def main():
    """Executar Sofia IA Engine"""
    
    logger.info("\n" + "🤖"*40)
    logger.info("SOFIA IA ENGINE - APRENDIZADO CONTÍNUO")
    logger.info("🤖"*40)
    
    # Inicializar Sofia
    sofia = SofiaAIEngine()
    
    # Dados de mercado simulados
    market_data = {
        'BTCUSDT': {
            'prices': [45000, 45100, 45050, 45200, 45150, 45300, 45250, 45400, 45350, 45500]
        },
        'ETHUSDT': {
            'prices': [2500, 2510, 2505, 2520, 2515, 2530, 2525, 2540, 2535, 2550]
        }
    }
    
    # Histórico de trades simulados
    historical_trades = [
        {
            'symbol': 'BTCUSDT',
            'entry_price': 45000,
            'exit_price': 45450,
            'pnl': 450,
            'pnl_pct': 1.0
        },
        {
            'symbol': 'ETHUSDT',
            'entry_price': 2500,
            'exit_price': 2550,
            'pnl': 50,
            'pnl_pct': 2.0
        }
    ]
    
    # Analisar mercado
    logger.info("\n📊 Analisando comportamento do mercado...")
    market_analysis = sofia.analyze_market_behavior(market_data)
    
    logger.info(f"✅ Análise concluída")
    logger.info(f"   Volatilidade: {market_analysis['volatility']}")
    logger.info(f"   Tendências: {market_analysis['trend']}")
    
    # Detectar padrões
    logger.info("\n🔍 Detectando padrões de trading...")
    patterns = sofia.detect_trading_patterns(market_data, historical_trades)
    
    for pattern in patterns:
        logger.info(f"✅ Padrão encontrado: {pattern['symbol']}")
        logger.info(f"   Confiança: {pattern['confidence']:.1f}%")
        logger.info(f"   Lucro médio: {pattern['avg_profit_pct']:.2f}%")
    
    # Gerar sugestões
    logger.info("\n💡 Gerando sugestões de trading...")
    for symbol in market_data.keys():
        current_price = market_data[symbol]['prices'][-1]
        suggestion = sofia.generate_trading_suggestion(
            symbol, market_analysis, patterns, current_price
        )
        
        logger.info(f"✅ Sugestão para {symbol}")
        logger.info(f"   Recomendação: {suggestion['recommendation']}")
        logger.info(f"   Confiança: {suggestion['confidence']:.1f}%")
        if suggestion['profit_target']:
            logger.info(f"   Meta de lucro: ${suggestion['profit_target']:.2f}")
            logger.info(f"   Stop loss: ${suggestion['stop_loss']:.2f}")
        else:
            logger.info(f"   Meta de lucro: N/A")
            logger.info(f"   Stop loss: N/A")
    
    # Aprender com trades
    logger.info("\n🧠 Sofia aprendendo com histórico...")
    for trade in historical_trades:
        trade['id'] = f"trade_{datetime.now().timestamp()}"
        sofia.learn_from_trade(trade)
    
    # Obter insights
    logger.info("\n📈 Insights de Performance")
    insights = sofia.get_performance_insights()
    
    logger.info(f"   Total de trades: {insights['total_trades']}")
    logger.info(f"   Taxa de sucesso: {insights['success_rate']:.1f}%")
    logger.info(f"   Melhor símbolo: {insights['best_symbol']}")
    logger.info(f"   Progresso: {insights['learning_progress']['status']}")
    
    for rec in insights['recommendations']:
        logger.info(f"   💡 {rec}")
    
    logger.info("\n✅ Sofia IA Engine funcionando perfeitamente!")


if __name__ == "__main__":
    main()
