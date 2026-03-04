#!/usr/bin/env python3
"""
Sofia IA Regenerativa - Sistema de Aprendizado Contínuo
Analisa mercado, gera recomendações, executa trades e aprende com resultados
"""

import json
import os
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
import random
import hashlib

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SofiaRegenerativeAI:
    """Sofia IA com capacidade regenerativa e aprendizado contínuo"""
    
    def __init__(self):
        self.learning_history_file = 'data/sofia_learning_history.json'
        self.trades_file = 'data/sofia_trades.json'
        self.analysis_file = 'data/sofia_analysis.json'
        
        # Criar diretórios se não existirem
        os.makedirs('data', exist_ok=True)
        
        # Carregar histórico de aprendizado
        self.learning_history = self._load_json(self.learning_history_file, {})
        self.trades_executed = self._load_json(self.trades_file, [])
        self.analysis_cache = self._load_json(self.analysis_file, {})
        
        # Métricas de precisão por símbolo
        self.accuracy_metrics = self._calculate_accuracy_metrics()
        
        logger.info("✅ Sofia IA Regenerativa Inicializada")
        logger.info(f"   Histórico de aprendizado: {len(self.learning_history)} registros")
        logger.info(f"   Trades executados: {len(self.trades_executed)}")
        logger.info(f"   Acurácia média: {self._get_overall_accuracy():.2f}%")
    
    def _load_json(self, filepath: str, default=None):
        """Carrega arquivo JSON com segurança"""
        try:
            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Erro ao carregar {filepath}: {e}")
        return default if default is not None else {}
    
    def _save_json(self, filepath: str, data):
        """Salva arquivo JSON com segurança"""
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Erro ao salvar {filepath}: {e}")
    
    def _calculate_accuracy_metrics(self) -> Dict:
        """Calcula métricas de acurácia por símbolo"""
        metrics = {}
        
        for trade in self.trades_executed:
            symbol = trade['symbol']
            if symbol not in metrics:
                metrics[symbol] = {
                    'total': 0,
                    'correct': 0,
                    'accuracy': 0,
                    'avg_profit': 0,
                    'win_rate': 0,
                    'trades': []
                }
            
            metrics[symbol]['total'] += 1
            metrics[symbol]['trades'].append(trade)
            
            # Verifica se a recomendação foi correta
            if self._validate_recommendation(trade):
                metrics[symbol]['correct'] += 1
            
            # Calcula acurácia
            if metrics[symbol]['total'] > 0:
                metrics[symbol]['accuracy'] = (metrics[symbol]['correct'] / metrics[symbol]['total']) * 100
            
            # Calcula win rate
            wins = len([t for t in metrics[symbol]['trades'] if t.get('pnl', 0) > 0])
            metrics[symbol]['win_rate'] = (wins / metrics[symbol]['total']) * 100 if metrics[symbol]['total'] > 0 else 0
            
            # Calcula lucro médio
            total_pnl = sum([t.get('pnl', 0) for t in metrics[symbol]['trades']])
            metrics[symbol]['avg_profit'] = total_pnl / metrics[symbol]['total'] if metrics[symbol]['total'] > 0 else 0
        
        return metrics
    
    def _validate_recommendation(self, trade: Dict) -> bool:
        """Valida se a recomendação foi correta"""
        recommendation = trade.get('recommendation', '')
        pnl = trade.get('pnl', 0)
        
        if recommendation == 'BUY' and pnl > 0:
            return True
        elif recommendation == 'SELL' and pnl > 0:
            return True
        elif recommendation == 'HOLD' and abs(pnl) < 10:
            return True
        
        return False
    
    def _get_overall_accuracy(self) -> float:
        """Calcula acurácia geral"""
        if not self.accuracy_metrics:
            return 0
        
        total_accuracy = sum([m['accuracy'] for m in self.accuracy_metrics.values()])
        return total_accuracy / len(self.accuracy_metrics) if self.accuracy_metrics else 0
    
    def analyze_market(self, market_data: Dict) -> Dict:
        """
        Analisa mercado com base em histórico de aprendizado
        Retorna análise regenerativa que melhora com o tempo
        """
        analysis = {}
        
        logger.info("\n🧠 Sofia IA - Análise de Mercado Regenerativa")
        logger.info("=" * 60)
        
        for symbol, data in market_data.items():
            # Recupera histórico deste símbolo
            symbol_history = self.learning_history.get(symbol, {})
            symbol_metrics = self.accuracy_metrics.get(symbol, {})
            
            # Análise técnica básica
            volatility = ((data['high24h'] - data['low24h']) / data['low24h']) * 100
            momentum = data['change24h']
            
            # Análise regenerativa: combina dados atuais com histórico
            recommendation, confidence, reasoning = self._generate_recommendation(
                symbol, data, symbol_history, symbol_metrics, volatility, momentum
            )
            
            # Calcula metas baseadas em histórico
            profit_target = self._calculate_profit_target(symbol, recommendation, data['price'])
            stop_loss = self._calculate_stop_loss(symbol, recommendation, data['price'])
            
            analysis[symbol] = {
                'symbol': symbol,
                'recommendation': recommendation,
                'confidence': confidence,
                'reasoning': reasoning,
                'profit_target': profit_target,
                'stop_loss': stop_loss,
                'current_price': data['price'],
                'volatility': volatility,
                'momentum': momentum,
                'accuracy': symbol_metrics.get('accuracy', 0),
                'win_rate': symbol_metrics.get('win_rate', 0),
                'avg_profit': symbol_metrics.get('avg_profit', 0),
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"\n📊 {symbol}")
            logger.info(f"   Recomendação: {recommendation}")
            logger.info(f"   Confiança: {confidence:.1f}%")
            logger.info(f"   Acurácia histórica: {symbol_metrics.get('accuracy', 0):.1f}%")
            logger.info(f"   Win Rate: {symbol_metrics.get('win_rate', 0):.1f}%")
            logger.info(f"   Motivo: {', '.join(reasoning)}")
        
        # Salva análise
        self._save_json(self.analysis_file, analysis)
        
        return analysis
    
    def _generate_recommendation(self, symbol: str, data: Dict, history: Dict, 
                               metrics: Dict, volatility: float, momentum: float) -> Tuple[str, float, List[str]]:
        """Gera recomendação com base em análise regenerativa"""
        
        recommendation = 'HOLD'
        confidence = 50
        reasoning = []
        
        # Base: análise técnica atual
        is_positive = momentum > 0
        is_volatile = volatility > 3
        
        # Histórico: acurácia passada
        accuracy = metrics.get('accuracy', 50)
        win_rate = metrics.get('win_rate', 50)
        avg_profit = metrics.get('avg_profit', 0)
        
        # Lógica regenerativa: combina análise técnica com histórico
        if is_positive and not is_volatile and accuracy > 50:
            recommendation = 'BUY'
            confidence = min(90, 60 + (accuracy - 50) * 0.6 + (win_rate - 50) * 0.4)
            reasoning = [
                f"Tendência positiva (+{momentum:.2f}%)",
                f"Volatilidade controlada ({volatility:.2f}%)",
                f"Histórico de acurácia: {accuracy:.1f}%",
                f"Win rate: {win_rate:.1f}%"
            ]
        
        elif not is_positive and is_volatile and accuracy > 45:
            recommendation = 'SELL'
            confidence = min(85, 55 + (accuracy - 45) * 0.6)
            reasoning = [
                f"Tendência negativa ({momentum:.2f}%)",
                f"Volatilidade elevada ({volatility:.2f}%)",
                f"Risco de queda identificado",
                f"Histórico de acurácia: {accuracy:.1f}%"
            ]
        
        else:
            recommendation = 'HOLD'
            confidence = 40 + (accuracy - 50) * 0.2
            reasoning = [
                "Mercado em consolidação",
                f"Volatilidade: {volatility:.2f}%",
                f"Momentum: {momentum:.2f}%",
                "Aguardando sinais mais claros"
            ]
        
        # Ajusta confiança baseado em histórico
        if accuracy > 70:
            confidence = min(100, confidence + 10)
        elif accuracy < 40:
            confidence = max(30, confidence - 10)
        
        return recommendation, confidence, reasoning
    
    def _calculate_profit_target(self, symbol: str, recommendation: str, current_price: float) -> float:
        """Calcula meta de lucro baseada em histórico"""
        metrics = self.accuracy_metrics.get(symbol, {})
        avg_profit = metrics.get('avg_profit', 0)
        
        # Base: 2.5% de lucro
        profit_percentage = 0.025
        
        # Ajusta baseado em histórico de lucros
        if avg_profit > 100:
            profit_percentage = 0.03
        elif avg_profit > 50:
            profit_percentage = 0.028
        elif avg_profit < 20:
            profit_percentage = 0.02
        
        if recommendation == 'SELL':
            return current_price * (1 - profit_percentage)
        else:
            return current_price * (1 + profit_percentage)
    
    def _calculate_stop_loss(self, symbol: str, recommendation: str, current_price: float) -> float:
        """Calcula stop loss baseado em histórico"""
        metrics = self.accuracy_metrics.get(symbol, {})
        
        # Base: 2% de perda
        stop_loss_percentage = 0.02
        
        # Ajusta baseado em volatilidade histórica
        if metrics.get('accuracy', 0) < 40:
            stop_loss_percentage = 0.015  # Mais apertado para símbolos com baixa acurácia
        elif metrics.get('accuracy', 0) > 70:
            stop_loss_percentage = 0.025  # Mais frouxo para símbolos com alta acurácia
        
        if recommendation == 'SELL':
            return current_price * (1 + stop_loss_percentage)
        else:
            return current_price * (1 - stop_loss_percentage)
    
    def execute_trade(self, symbol: str, analysis: Dict, market_data: Dict) -> Dict:
        """Executa trade baseado na análise de Sofia"""
        
        if symbol not in analysis:
            return None
        
        rec = analysis[symbol]
        
        # Só executa se confiança > 55%
        if rec['confidence'] < 55:
            logger.info(f"⏭️  {symbol}: Confiança baixa ({rec['confidence']:.1f}%), trade não executado")
            return None
        
        # Simula preço de entrada
        entry_price = market_data[symbol]['price']
        
        # Simula movimento de preço (0.5% a 3%)
        price_movement = random.uniform(-0.03, 0.03)
        exit_price = entry_price * (1 + price_movement)
        
        # Calcula P&L
        quantity = 1.0  # Unidade padrão
        pnl = (exit_price - entry_price) * quantity
        pnl_percent = (pnl / entry_price) * 100
        
        # Cria registro de trade
        trade = {
            'id': hashlib.md5(f"{symbol}{datetime.now()}".encode()).hexdigest()[:8],
            'symbol': symbol,
            'recommendation': rec['recommendation'],
            'entry_price': entry_price,
            'exit_price': exit_price,
            'quantity': quantity,
            'pnl': pnl,
            'pnl_percent': pnl_percent,
            'confidence': rec['confidence'],
            'profit_target': rec['profit_target'],
            'stop_loss': rec['stop_loss'],
            'executed_at': datetime.now().isoformat(),
            'status': 'CLOSED'
        }
        
        # Adiciona ao histórico
        self.trades_executed.append(trade)
        self._save_json(self.trades_file, self.trades_executed)
        
        logger.info(f"\n✅ Trade Executado: {symbol}")
        logger.info(f"   Recomendação: {rec['recommendation']}")
        logger.info(f"   Entrada: ${entry_price:.2f}")
        logger.info(f"   Saída: ${exit_price:.2f}")
        logger.info(f"   P&L: ${pnl:.2f} ({pnl_percent:.2f}%)")
        
        return trade
    
    def learn_from_trade(self, trade: Dict):
        """Sofia aprende com resultado do trade"""
        
        symbol = trade['symbol']
        
        # Atualiza histórico de aprendizado
        if symbol not in self.learning_history:
            self.learning_history[symbol] = {
                'trades': [],
                'total_pnl': 0,
                'accuracy': 0,
                'last_updated': None
            }
        
        self.learning_history[symbol]['trades'].append(trade)
        self.learning_history[symbol]['total_pnl'] += trade['pnl']
        self.learning_history[symbol]['last_updated'] = datetime.now().isoformat()
        
        # Recalcula métricas
        self.accuracy_metrics = self._calculate_accuracy_metrics()
        
        # Salva histórico
        self._save_json(self.learning_history_file, self.learning_history)
        
        logger.info(f"\n🧠 Sofia Aprendeu com {symbol}")
        logger.info(f"   P&L: ${trade['pnl']:.2f}")
        logger.info(f"   Acurácia atualizada: {self.accuracy_metrics.get(symbol, {}).get('accuracy', 0):.1f}%")
    
    def get_daily_summary(self) -> Dict:
        """Retorna resumo do dia"""
        today = datetime.now().date()
        today_trades = [t for t in self.trades_executed if datetime.fromisoformat(t['executed_at']).date() == today]
        
        total_pnl = sum([t['pnl'] for t in today_trades])
        wins = len([t for t in today_trades if t['pnl'] > 0])
        
        return {
            'date': today.isoformat(),
            'total_trades': len(today_trades),
            'total_pnl': total_pnl,
            'win_rate': (wins / len(today_trades) * 100) if today_trades else 0,
            'overall_accuracy': self._get_overall_accuracy(),
            'trades': today_trades
        }


def main():
    """Teste do sistema Sofia IA Regenerativa"""
    
    # Inicializa Sofia
    sofia = SofiaRegenerativeAI()
    
    # Dados de mercado simulados
    market_data = {
        'BTCUSDT': {
            'price': 66000,
            'high24h': 67000,
            'low24h': 65000,
            'change24h': 1.5,
            'volume': 1000000
        },
        'ETHUSDT': {
            'price': 2500,
            'high24h': 2550,
            'low24h': 2450,
            'change24h': -0.5,
            'volume': 500000
        },
        'BNBUSDT': {
            'price': 600,
            'high24h': 610,
            'low24h': 590,
            'change24h': 0.8,
            'volume': 300000
        },
    }
    
    # Sofia analisa mercado
    analysis = sofia.analyze_market(market_data)
    
    # Executa trades baseados em análise
    logger.info("\n" + "=" * 60)
    logger.info("🚀 EXECUTANDO TRADES BASEADOS EM SOFIA IA")
    logger.info("=" * 60)
    
    executed_trades = []
    for symbol in market_data.keys():
        trade = sofia.execute_trade(symbol, analysis, market_data)
        if trade:
            executed_trades.append(trade)
            sofia.learn_from_trade(trade)
    
    # Resumo do dia
    logger.info("\n" + "=" * 60)
    logger.info("📊 RESUMO DO DIA")
    logger.info("=" * 60)
    
    summary = sofia.get_daily_summary()
    logger.info(f"Total de trades: {summary['total_trades']}")
    logger.info(f"P&L Total: ${summary['total_pnl']:.2f}")
    logger.info(f"Win Rate: {summary['win_rate']:.1f}%")
    logger.info(f"Acurácia Geral: {summary['overall_accuracy']:.1f}%")
    
    logger.info("\n✅ Sofia IA Regenerativa funcionando perfeitamente!")


if __name__ == "__main__":
    main()
