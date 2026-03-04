#!/usr/bin/env python3
"""
Egreja Investment AI - VERSÃO LITE
Roda as análises principais sem dependências pesadas
"""

import logging
import yfinance as yf
import numpy as np
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ações B3 + NYSE
SYMBOLS = {
    'B3': ['EQTL3.SA', 'ITUB4.SA', 'EMBJ3.SA', 'WEGE3.SA', 'BPAC11.SA', 
           'BBDC4.SA', 'PETR4.SA', 'VALE3.SA', 'BRAP4.SA', 'NATU3.SA',
           'JBSS32.SA', 'BBAS3.SA', 'PRIO3.SA', 'POMO4.SA', 'CPFE3.SA',
           'HBRE3.SA', 'VTRU3.SA', 'ANIM3.SA', 'ALPA4.SA', 'RENT3.SA'],
    'NYSE': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX', 'AMD', 'BABA',
             'JPM', 'GS', 'C', 'BLK', 'SCHW', 'V', 'MA', 'JNJ', 'UNH', 'ABBV']
}

class SimpleTechnicalAnalyzer:
    """Análise técnica simplificada (rápida)"""
    
    @staticmethod
    def calculate_ema(prices, period):
        if len(prices) < period:
            return None
        prices_arr = np.array(prices[-period:])
        return float(np.mean(prices_arr))
    
    @staticmethod
    def calculate_rsi(prices, period=14):
        if len(prices) < period + 1:
            return None
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return float(rsi)
    
    @staticmethod
    def analyze(symbol, prices, current_price):
        """Análise rápida"""
        if len(prices) < 50:
            return None
        
        ema9 = SimpleTechnicalAnalyzer.calculate_ema(prices, 9)
        ema21 = SimpleTechnicalAnalyzer.calculate_ema(prices, 21)
        ema50 = SimpleTechnicalAnalyzer.calculate_ema(prices, 50)
        rsi = SimpleTechnicalAnalyzer.calculate_rsi(prices)
        
        if not all([ema9, ema21, ema50, rsi]):
            return None
        
        # Score simplificado
        score = 50
        
        if ema9 > ema21 > ema50:
            score += 20
        elif ema9 < ema21 < ema50:
            score -= 20
        
        if rsi < 30:
            score += 15
        elif rsi > 70:
            score -= 15
        
        score = max(0, min(100, score))
        
        # Recomendação
        if score >= 70:
            signal = '🟢 COMPRA FORTE'
        elif score >= 60:
            signal = '🟢 COMPRA'
        elif score <= 40:
            signal = '🔴 VENDA'
        else:
            signal = '🟡 MANTER'
        
        return {
            'symbol': symbol,
            'price': current_price,
            'score': score,
            'signal': signal,
            'rsi': rsi,
            'ema9': ema9,
            'ema21': ema21,
            'ema50': ema50
        }


def run_analysis():
    """Executa análise para todos os ativos"""
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando análise...")
    
    analyzer = SimpleTechnicalAnalyzer()
    results = {'timestamp': datetime.now().isoformat(), 'signals': []}
    
    all_symbols = SYMBOLS['B3'] + SYMBOLS['NYSE']
    
    for symbol in all_symbols:
        try:
            # Baixar dados (últimos 100 dias)
            data = yf.download(symbol, period='100d', progress=False, quiet=True)
            
            if data.empty:
                continue
            
            prices = data['Close'].values.tolist()
            current_price = prices[-1]
            
            # Análise
            analysis = analyzer.analyze(symbol, prices, current_price)
            
            if analysis:
                results['signals'].append(analysis)
                
                # Log
                if 'COMPRA' in analysis['signal'] or 'VENDA' in analysis['signal']:
                    logger.info(f"  {symbol}: {analysis['signal']} (Score: {analysis['score']}/100)")
        
        except Exception as e:
            logger.debug(f"  {symbol}: Erro - {e}")
            continue
    
    # Ordenar por score
    results['signals'].sort(key=lambda x: x['score'], reverse=True)
    
    # Top 10 COMPRA
    buy_signals = [s for s in results['signals'] if 'COMPRA' in s['signal']]
    sell_signals = [s for s in results['signals'] if 'VENDA' in s['signal']]
    
    logger.info(f"\n📊 RESULTADO:")
    logger.info(f"  Total analisados: {len(results['signals'])}")
    logger.info(f"  Sinais de COMPRA: {len(buy_signals)}")
    logger.info(f"  Sinais de VENDA: {len(sell_signals)}")
    
    if buy_signals:
        logger.info(f"\n🟢 TOP 5 COMPRA:")
        for sig in buy_signals[:5]:
            logger.info(f"    {sig['symbol']}: {sig['signal']} (Score: {sig['score']}, RSI: {sig['rsi']:.1f})")
    
    if sell_signals:
        logger.info(f"\n🔴 TOP 5 VENDA:")
        for sig in sell_signals[:5]:
            logger.info(f"    {sig['symbol']}: {sig['signal']} (Score: {sig['score']}, RSI: {sig['rsi']:.1f})")
    
    return results


if __name__ == '__main__':
    logger.info("🚀 Egreja Investment AI - LITE MODE (DAEMON)")
    logger.info("=" * 60)
    
    # Loop contínuo (roda a cada 15 minutos)
    while True:
        try:
            # Executar análise
            results = run_analysis()
            
            logger.info(f"\n✅ Análise concluída! ({datetime.now().strftime('%H:%M:%S')})")
            logger.info(f"Total de sinais gerados: {len(results['signals'])}")
            
            # Esperar 15 minutos antes da próxima análise
            logger.info("⏳ Próxima análise em 15 minutos...\n")
            import time
            time.sleep(900)  # 15 minutos
            
        except Exception as e:
            logger.error(f"❌ Erro na análise: {e}")
            logger.info("⏳ Tentando novamente em 5 minutos...")
            import time
            time.sleep(300)  # 5 minutos
