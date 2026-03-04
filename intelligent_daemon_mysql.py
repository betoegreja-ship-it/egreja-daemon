import sys
print("PYTHON INICIANDO", flush=True)
sys.stdout.flush()
#!/usr/bin/env python3
import os
import time
import logging
import requests
import numpy as np
import mysql.connector
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BRAPI_TOKEN = os.environ.get('BRAPI_TOKEN', 'cusMaXUT1eRuPtHTd1k4wy')

B3_STOCKS = ['PETR4','VALE3','ITUB4','BBDC4','ABEV3','WEGE3','RENT3','MGLU3',
             'BBAS3','PRIO3','EQTL3','BPAC11','GGBR4','RADL3','SUZB3',
             'JBSS3','HAPV3','TOTS3','VIVT3','CSAN3']

NYSE_STOCKS = ['AAPL','MSFT','GOOGL','AMZN','TSLA','NVDA','META','NFLX',
               'AMD','BABA','JPM','V','MA','DIS','PYPL',
               'INTC','CRM','UBER','SPOT','COIN']

DB_CONFIG = {
    'host': os.environ.get('MYSQLHOST', 'localhost'),
    'user': os.environ.get('MYSQLUSER', 'root'),
    'password': os.environ.get('MYSQLPASSWORD', ''),
    'database': os.environ.get('MYSQLDATABASE', 'railway'),
    'port': int(os.environ.get('MYSQLPORT', '3306')),
}

def get_db():
    return mysql.connector.connect(**DB_CONFIG)

def ensure_tables():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_signals (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(20),
            market_type VARCHAR(10),
            price DECIMAL(20, 8),
            score INT,
            `signal` VARCHAR(50),
            rsi DECIMAL(10, 2),
            ema9 DECIMAL(20, 8),
            ema21 DECIMAL(20, 8),
            ema50 DECIMAL(20, 8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_symbol (symbol),
            INDEX idx_created_at (created_at)
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(" Tabela market_signals OK")

def calculate_ema(prices, period):
    if len(prices) < period:
        return None
    k = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = price * k + ema * (1 - k)
    return ema

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
    return 100 - (100 / (1 + rs))

def fetch_brapi(symbols, market_type):
    results = []
    chunk_size = 10
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i+chunk_size]
        tickers = ','.join(chunk)
        if market_type == 'B3':
            tickers_sa = ','.join([s + '.SA' if not s.endswith('.SA') else s for s in chunk])
            url = f'https://brapi.dev/api/quote/{tickers_sa}?range=3mo&interval=1d&token={BRAPI_TOKEN}'
        else:
            url = f'https://brapi.dev/api/quote/{tickers}?range=3mo&interval=1d&token={BRAPI_TOKEN}'
        try:
            resp = requests.get(url, timeout=30)
            data = resp.json()
            if 'results' in data:
                results.extend(data['results'])
        except Exception as e:
            logger.error(f" Brapi erro: {e}")
        time.sleep(1)
    return results

def analyze_and_save(results, market_type):
    conn = get_db()
    cursor = conn.cursor()
    saved = 0
    for item in results:
        try:
            symbol = item.get('symbol', '').replace('.SA', '')
            current_price = item.get('regularMarketPrice', 0)
            historical = item.get('historicalDataPrice', [])
            if not historical or len(historical) < 50:
                logger.warning(f" {symbol}: dados insuficientes ({len(historical)} candles)")
                continue
            prices = [h['close'] for h in historical if h.get('close')]
            if len(prices) < 50:
                continue
            ema9 = calculate_ema(prices, 9)
            ema21 = calculate_ema(prices, 21)
            ema50 = calculate_ema(prices, 50)
            rsi = calculate_rsi(prices)
            if not all([ema9, ema21, ema50, rsi]):
                continue
            score = 50
            if ema9 > ema21 > ema50:
                score += 30
            elif ema9 < ema21 < ema50:
                score -= 30
            if rsi < 30:
                score += 20
            elif rsi > 70:
                score -= 20
            if score >= 70:
                signal = 'COMPRA'
            elif score <= 30:
                signal = 'VENDA'
            else:
                signal = 'MANTER'
            cursor.execute("""
                INSERT INTO market_signals 
                (symbol, market_type, price, score, `signal`, rsi, ema9, ema21, ema50)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (symbol, market_type, current_price, score, signal, rsi, ema9, ema21, ema50))
            saved += 1
            logger.info(f" {symbol}: {signal} (score={score}, RSI={rsi:.1f}, price={current_price})")
        except Exception as e:
            logger.error(f" Erro {item.get('symbol','?')}: {e}")
    conn.commit()
    cursor.close()
    conn.close()
    return saved

def main():
    logger.info("=" * 70)
    logger.info("🚀 Egreja Investment AI - MYSQL VERSION")
    logger.info("=" * 70)
    ensure_tables()
    cycle = 0
    while True:
        cycle += 1
        logger.info(f"\n[Ciclo {cycle}] 🚀 INICIANDO ANÁLISE")
        logger.info("B3 (20 ativos)...")
        b3_results = fetch_brapi(B3_STOCKS, 'B3')
        saved_b3 = analyze_and_save(b3_results, 'B3')
        logger.info(f"B3: {saved_b3} sinais salvos")
        logger.info("NYSE (20 ativos)...")
        nyse_results = fetch_brapi(NYSE_STOCKS, 'NYSE')
        saved_nyse = analyze_and_save(nyse_results, 'NYSE')
        logger.info(f"NYSE: {saved_nyse} sinais salvos")
        logger.info(f"Ciclo {cycle} completo! Total: {saved_b3 + saved_nyse} sinais")
        logger.info("Aguardando 15 minutos...")
        time.sleep(900)
