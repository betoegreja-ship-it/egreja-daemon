#!/usr/bin/env python3
"""
ArbitrageAI - Trade Signals Generator
Integra análise técnica + fundamental para gerar sinais de trade
"""

import logging
from typing import Dict, Optional, List
from technical_analysis import TechnicalAnalyzer
from fundamental_analysis import FundamentalAnalyzer
from datetime import datetime

logger = logging.getLogger(__name__)


class TradeSignalGenerator:
    """Gera sinais de trade combinando análise técnica e fundamental"""
    
    def __init__(self):
        """Inicializar geradores de sinal"""
        self.technical = TechnicalAnalyzer()
        self.fundamental = FundamentalAnalyzer()
    
    def generate_signal(self, symbol: str, price_history: List[float], current_price: float) -> Optional[Dict]:
        """
        Gera sinal de trade combinado: técnico (50%) + fundamental (50%)
        
        Args:
            symbol: Símbolo do ativo
            price_history: Histórico de preços
            current_price: Preço atual
        
        Returns:
            Dict com sinal completo ou None
        """
        
        # Análise técnica
        technical_signal = self.technical.analyze_symbol(symbol, price_history, current_price)
        if not technical_signal:
            return None
        
        technical_score = technical_signal['score']
        technical_rec = technical_signal['recommendation']
        
        # Análise fundamental
        fundamental_score = self.fundamental.combined_score(symbol, technical_score)
        
        # Score combinado
        combined_score = (technical_score * 0.50) + (fundamental_score['combined_score'] * 0.50)
        combined_score = max(0, min(100, combined_score))
        
        # Determinar recomendação final
        if combined_score >= 70 and technical_rec == 'BUY':
            final_signal = '🟢 COMPRA FORTE'
            confidence = combined_score / 100
        elif combined_score >= 60 and technical_rec == 'BUY':
            final_signal = '🟢 COMPRA'
            confidence = combined_score / 100
        elif combined_score <= 40 and technical_rec == 'SELL':
            final_signal = '🔴 VENDA FORTE'
            confidence = (100 - combined_score) / 100
        elif combined_score <= 50 and technical_rec == 'SELL':
            final_signal = '🔴 VENDA'
            confidence = (100 - combined_score) / 100
        else:
            final_signal = '🟡 MANTER'
            confidence = 0.5
        
        return {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'current_price': current_price,
            'signal': final_signal,
            'confidence': confidence,
            'technical_score': technical_score,
            'fundamental_score': fundamental_score['combined_score'],
            'combined_score': combined_score,
            'technical_analysis': technical_signal,
            'fundamental_analysis': fundamental_score,
            'recommendations': {
                'technical': technical_rec,
                'fundamental': fundamental_score['final_rating'],
                'combined': final_signal
            }
        }
    
    def generate_portfolio_signals(self, portfolio: Dict[str, List[float]]) -> List[Dict]:
        """
        Gera sinais para uma carteira inteira
        
        Args:
            portfolio: Dict com {symbol: [price_history], ...}
        
        Returns:
            Lista de sinais de trade
        """
        signals = []
        
        for symbol, price_history in portfolio.items():
            if not price_history or len(price_history) < 50:
                continue
            
            current_price = price_history[-1]
            signal = self.generate_signal(symbol, price_history, current_price)
            
            if signal:
                signals.append(signal)
        
        # Ordenar por combined_score (descendente)
        signals.sort(key=lambda x: x['combined_score'], reverse=True)
        
        return signals
    
    def filter_buy_signals(self, signals: List[Dict], min_confidence: float = 0.60) -> List[Dict]:
        """Filtra apenas sinais de compra acima da confiança mínima"""
        return [s for s in signals if s['confidence'] >= min_confidence and 'COMPRA' in s['signal']]
    
    def filter_sell_signals(self, signals: List[Dict], min_confidence: float = 0.60) -> List[Dict]:
        """Filtra apenas sinais de venda acima da confiança mínima"""
        return [s for s in signals if s['confidence'] >= min_confidence and 'VENDA' in s['signal']]


if __name__ == '__main__':
    import yfinance as yf
    
    # Teste
    print("=== TESTE GERADOR DE SINAIS ===\n")
    
    generator = TradeSignalGenerator()
    
    # Baixar histórico real
    symbols = ['PETR4.SA', 'VALE3.SA', 'ITUB4.SA']
    
    for symbol in symbols:
        print(f"\n{symbol}:")
        
        try:
            # Baixar dados
            data = yf.download(symbol, period='90d', progress=False)
            prices = data['Close'].values.tolist()
            current_price = prices[-1]
            
            # Gerar sinal
            signal = generator.generate_signal(symbol, prices, current_price)
            
            if signal:
                print(f"  Sinal: {signal['signal']}")
                print(f"  Confiança: {signal['confidence']:.2%}")
                print(f"  Score Combinado: {signal['combined_score']:.1f}/100")
                print(f"  Técnico: {signal['technical_score']:.1f}/100")
                print(f"  Fundamental: {signal['fundamental_score']:.1f}/100")
            else:
                print("  Sem sinal claro")
        
        except Exception as e:
            print(f"  Erro: {e}")
