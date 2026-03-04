#!/usr/bin/env python3
"""
Egreja Investment AI - VERSÃO SIMPLES E ROBUSTA
Roda análise contínua com logging detalhado
Escreve resultados em JSON (sem MySQL por enquanto)
"""

import logging
import os
import time
import json
from datetime import datetime
import yfinance as yf
import numpy as np

# Setup logging - VERBOSE
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # stdout
    ]
)
logger = logging.getLogger(__name__)

# 40 Ativos: 20 B3 + 20 NYSE
SYMBOLS_B3 = [
    'EQTL3.SA', 'ITUB4.SA', 'EMBJ3.SA', 'WEGE3.SA', 'BPAC11.SA',
    'BBDC4.SA', 'PETR4.SA', 'VALE3.SA', 'BRAP4.SA', 'NATU3.SA',
    'JBSS32.SA', 'BBAS3.SA', 'PRIO3.SA', 'POMO4.SA', 'CPFE3.SA',
    'HBRE3.SA', 'VTRU3.SA', 'ANIM3.SA', 'ALPA4.SA', 'RENT3.SA'
]

SYMBOLS_NYSE = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX', 'AMD', 'BABA',
    'JPM', 'GS', 'C', 'BLK', 'SCHW', 'V', 'MA', 'JNJ', 'UNH', 'ABBV'
]

def calculate_ema(prices, period):
    """EMA simples"""
    if len(prices) < period:
        return None
    return float(np.mean(np.array(prices[-period:])))

def calculate_rsi(prices, period=14):
    """RSI"""
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
    return 100 - (100 / (1 + rs))

def analyze_symbol(symbol, market_type):
    """Análise de 1 ativo"""
    try:
        logger.debug(f"Baixando {symbol}...")
        
        # Baixar dados
        data = yf.download(symbol, period='100d', progress=False, quiet=True)
        
        if data.empty:
            logger.warning(f"  {symbol}: Sem dados")
            return None
        
        prices = data['Close'].values.tolist()
        current_price = prices[-1]
        
        logger.debug(f"  {symbol}: {len(prices)} candles, preço ${current_price:.2f}")
        
        if len(prices) < 50:
            logger.warning(f"  {symbol}: Poucos dados ({len(prices)} < 50)")
            return None
        
        # Análise
        ema9 = calculate_ema(prices, 9)
        ema21 = calculate_ema(prices, 21)
        ema50 = calculate_ema(prices, 50)
        rsi = calculate_rsi(prices)
        
        if not all([ema9, ema21, ema50, rsi]):
            logger.warning(f"  {symbol}: EMA/RSI falhou")
            return None
        
        # Score
        score = 50
        if ema9 > ema21 > ema50:
            score += 30
        elif ema9 < ema21 < ema50:
            score -= 30
        
        if rsi < 30:
            score += 20
        elif rsi > 70:
            score -= 20
        
        score = max(0, min(100, score))
        
        # Sinal
        if score >= 70:
            signal = '🟢 COMPRA FORTE'
        elif score >= 60:
            signal = '🟢 COMPRA'
        elif score <= 40:
            signal = '🔴 VENDA'
        else:
            signal = '🟡 MANTER'
        
        result = {
            'symbol': symbol,
            'market_type': market_type,
            'price': round(current_price, 8),
            'score': score,
            'signal': signal,
            'rsi': round(rsi, 2),
            'ema9': round(ema9, 8),
            'ema21': round(ema21, 8),
            'ema50': round(ema50, 8),
            'timestamp': datetime.now().isoformat()
        }
        
        # Log se sinal relevante
        if 'COMPRA' in signal or 'VENDA' in signal:
            logger.info(f"  ✅ {symbol}: {signal} (Score: {score}/100, RSI: {rsi:.1f})")
        
        return result
    
    except Exception as e:
        logger.error(f"  ❌ {symbol}: {type(e).__name__}: {e}")
        return None

def run_analysis():
    """Análise completa"""
    logger.info("\n" + "="*70)
    logger.info(f"🚀 ANÁLISE EM TEMPO REAL - {datetime.now().strftime('%H:%M:%S')}")
    logger.info("="*70 + "\n")
    
    results = []
    
    # B3
    logger.info(f"📊 B3 ({len(SYMBOLS_B3)} ativos)...")
    for symbol in SYMBOLS_B3:
        result = analyze_symbol(symbol, 'B3')
        if result:
            results.append(result)
        time.sleep(0.1)  # Rate limit
    
    # NYSE
    logger.info(f"\n📊 NYSE ({len(SYMBOLS_NYSE)} ativos)...")
    for symbol in SYMBOLS_NYSE:
        result = analyze_symbol(symbol, 'NYSE')
        if result:
            results.append(result)
        time.sleep(0.1)  # Rate limit
    
    # Resumo
    buy = len([r for r in results if 'COMPRA' in r['signal']])
    sell = len([r for r in results if 'VENDA' in r['signal']])
    hold = len([r for r in results if 'MANTER' in r['signal']])
    
    logger.info(f"\n{'='*70}")
    logger.info(f"📊 RESULTADO:")
    logger.info(f"  Total analisados: {len(results)}")
    logger.info(f"  🟢 COMPRA: {buy}")
    logger.info(f"  🔴 VENDA: {sell}")
    logger.info(f"  🟡 MANTER: {hold}")
    logger.info(f"{'='*70}\n")
    
    # Salvar em JSON
    output = {
        'timestamp': datetime.now().isoformat(),
        'total': len(results),
        'buy_signals': buy,
        'sell_signals': sell,
        'signals': results
    }
    
    with open('/tmp/egreja-signals.json', 'w') as f:
        json.dump(output, f, indent=2)
    logger.info(f"✅ Resultados salvos em /tmp/egreja-signals.json")
    
    return results

def main():
    logger.info("="*70)
    logger.info("🚀 Egreja Investment AI - DAEMON SIMPLES E ROBUSTO")
    logger.info("="*70)
    
    while True:
        try:
            run_analysis()
            logger.info("\n⏳ Próxima análise em 15 minutos...\n")
            time.sleep(900)  # 15 min
        
        except KeyboardInterrupt:
            logger.info("\n👋 Daemon parado")
            break
        
        except Exception as e:
            logger.error(f"❌ Erro crítico: {e}", exc_info=True)
            logger.info("⏳ Retrying em 5 minutos...")
            time.sleep(300)

if __name__ == '__main__':
    main()
