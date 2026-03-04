#!/usr/bin/env python3
"""
Egreja Investment AI - VERSÃO MYSQL
Roda análise contínua e PERSISTE TUDO NO MYSQL
"""

import logging
import os
import time
import mysql.connector
from datetime import datetime
import yfinance as yf
import numpy as np

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

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

class MySQLManager:
    def __init__(self):
        self.config = {
            'host': os.getenv('MYSQLHOST', 'localhost'),
            'user': os.getenv('MYSQLUSER', 'root'),
            'password': os.getenv('MYSQLPASSWORD', ''),
            'database': os.getenv('MYSQLDATABASE', 'railway'),
            'port': int(os.getenv('MYSQLPORT', 3306))
        }
        self.conn = None
        self.connect()
    
    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.config)
            logger.info(f"✅ MySQL conectado")
            self._ensure_tables()
        except Exception as e:
            logger.error(f"❌ MySQL erro: {e}")
            raise
    
    def _ensure_tables(self):
        """Cria tabelas se não existirem"""
        cursor = self.conn.cursor()
        sql = """
        CREATE TABLE IF NOT EXISTS market_signals (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(20),
            market_type VARCHAR(10),
            price DECIMAL(20, 8),
            score INT,
            signal VARCHAR(50),
            rsi DECIMAL(10, 2),
            ema9 DECIMAL(20, 8),
            ema21 DECIMAL(20, 8),
            ema50 DECIMAL(20, 8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_symbol (symbol),
            INDEX idx_created_at (created_at)
        )
        """
        cursor.execute(sql)
        self.conn.commit()
        cursor.close()
    
    def insert_signal(self, signal_data):
        """Insere um sinal no banco"""
        cursor = self.conn.cursor()
        sql = """
        INSERT INTO market_signals 
        (symbol, market_type, price, score, signal, rsi, ema9, ema21, ema50)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            signal_data['symbol'],
            signal_data['market_type'],
            signal_data['price'],
            signal_data['score'],
            signal_data['signal'],
            signal_data['rsi'],
            signal_data['ema9'],
            signal_data['ema21'],
            signal_data['ema50']
        )
        try:
            cursor.execute(sql, params)
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Erro inserindo {signal_data['symbol']}: {e}")
            self.conn.rollback()
            return False
        finally:
            cursor.close()
    
    def get_latest_signals(self, limit=40):
        """Busca os últimos sinais"""
        cursor = self.conn.cursor(dictionary=True)
        sql = "SELECT * FROM market_signals ORDER BY created_at DESC LIMIT %s"
        cursor.execute(sql, (limit,))
        results = cursor.fetchall()
        cursor.close()
        return results

def calculate_ema(prices, period):
    if len(prices) < period:
        return None
    return float(np.mean(np.array(prices[-period:])))

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

def analyze_symbol(symbol, market_type):
    try:
        logger.debug(f"Baixando {symbol}...")
        data = yf.download(symbol, period='100d', progress=False, quiet=True)
        
        if data.empty or len(data) < 50:
            logger.warning(f"  {symbol}: Dados insuficientes")
            return None
        
        prices = data['Close'].values.tolist()
        current_price = prices[-1]
        
        ema9 = calculate_ema(prices, 9)
        ema21 = calculate_ema(prices, 21)
        ema50 = calculate_ema(prices, 50)
        rsi = calculate_rsi(prices)
        
        if not all([ema9, ema21, ema50, rsi]):
            return None
        
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
            'ema50': round(ema50, 8)
        }
        
        if 'COMPRA' in signal or 'VENDA' in signal:
            logger.info(f"  ✅ {symbol}: {signal} (Score: {score}/100)")
        
        return result
    
    except Exception as e:
        logger.error(f"  ❌ {symbol}: {e}")
        return None

def main():
    logger.info("="*70)
    logger.info("🚀 Egreja Investment AI - MYSQL VERSION")
    logger.info("="*70)
    
    db = MySQLManager()
    
    while True:
        try:
            logger.info(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🚀 INICIANDO ANÁLISE")
            logger.info("="*70)
            
            results = []
            
            logger.info(f"📊 B3 ({len(SYMBOLS_B3)} ativos)...")
            for symbol in SYMBOLS_B3:
                analysis = analyze_symbol(symbol, 'B3')
                if analysis:
                    results.append(analysis)
                    db.insert_signal(analysis)
                time.sleep(0.1)
            
            logger.info(f"\n📊 NYSE ({len(SYMBOLS_NYSE)} ativos)...")
            for symbol in SYMBOLS_NYSE:
                analysis = analyze_symbol(symbol, 'NYSE')
                if analysis:
                    results.append(analysis)
                    db.insert_signal(analysis)
                time.sleep(0.1)
            
            buy = len([r for r in results if 'COMPRA' in r['signal']])
            sell = len([r for r in results if 'VENDA' in r['signal']])
            hold = len([r for r in results if 'MANTER' in r['signal']])
            
            logger.info(f"\n{'='*70}")
            logger.info(f"📊 RESULTADO FINAL:")
            logger.info(f"  Total analisados: {len(results)}")
            logger.info(f"  🟢 COMPRA: {buy}")
            logger.info(f"  🔴 VENDA: {sell}")
            logger.info(f"  🟡 MANTER: {hold}")
            logger.info(f"{'='*70}\n")
            
            logger.info("⏳ Próxima análise em 15 minutos...\n")
            time.sleep(900)
        
        except KeyboardInterrupt:
            logger.info("\n👋 Daemon parado")
            break
        
        except Exception as e:
            logger.error(f"❌ Erro crítico: {e}", exc_info=True)
            logger.info("⏳ Tentando novamente em 5 minutos...")
            time.sleep(300)

if __name__ == '__main__':
    main()
