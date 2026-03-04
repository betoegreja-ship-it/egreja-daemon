#!/usr/bin/env python3
"""
Egreja Investment AI - Daemon v2
Análise contínua com persistência em MySQL
Roda análise a cada 15 minutos, persiste resultados no banco
"""

import logging
import os
import time
import json
import mysql.connector
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import yfinance as yf
import numpy as np
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # stdout no Railway
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# ===== CONFIG =====
SYMBOLS = {
    'B3': ['EQTL3.SA', 'ITUB4.SA', 'EMBJ3.SA', 'WEGE3.SA', 'BPAC11.SA', 
           'BBDC4.SA', 'PETR4.SA', 'VALE3.SA', 'BRAP4.SA', 'NATU3.SA',
           'JBSS32.SA', 'BBAS3.SA', 'PRIO3.SA', 'POMO4.SA', 'CPFE3.SA',
           'HBRE3.SA', 'VTRU3.SA', 'ANIM3.SA', 'ALPA4.SA', 'RENT3.SA'],
    'NYSE': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX', 'AMD', 'BABA',
             'JPM', 'GS', 'C', 'BLK', 'SCHW', 'V', 'MA', 'JNJ', 'UNH', 'ABBV']
}

ANALYSIS_INTERVAL = 900  # 15 minutos em segundos
RETRY_INTERVAL = 300    # 5 minutos em caso de erro

class MySQLConnection:
    """Gerencia conexão com MySQL"""
    def __init__(self):
        self.config = {
            'host': os.getenv('MYSQLHOST', 'localhost'),
            'user': os.getenv('MYSQLUSER', 'root'),
            'password': os.getenv('MYSQLPASSWORD', ''),
            'database': os.getenv('MYSQLDATABASE', 'railway'),
            'port': int(os.getenv('MYSQLPORT', 3306))
        }
        self.connection = None
        self.connect()
    
    def connect(self):
        """Conecta ao banco de dados"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            logger.info(f"✅ Conectado ao MySQL ({self.config['host']})")
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao conectar ao MySQL: {e}")
            return False
    
    def execute_query(self, query: str, params: tuple = None) -> bool:
        """Executa query INSERT/UPDATE/DELETE"""
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao executar query: {e}")
            self.connection.rollback()
            return False
    
    def fetch_query(self, query: str, params: tuple = None) -> List[tuple]:
        """Executa SELECT e retorna resultados"""
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            logger.error(f"❌ Erro ao fazer fetch: {e}")
            return []

class TechnicalAnalyzer:
    """Análise técnica dos ativos"""
    
    @staticmethod
    def calculate_ema(prices: list, period: int) -> Optional[float]:
        """Calcula EMA"""
        if len(prices) < period:
            return None
        return float(np.mean(np.array(prices[-period:])))
    
    @staticmethod
    def calculate_rsi(prices: list, period: int = 14) -> Optional[float]:
        """Calcula RSI"""
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
    def analyze(symbol: str, prices: list, current_price: float) -> Optional[Dict]:
        """Análise técnica completa de um ativo"""
        if len(prices) < 50:
            return None
        
        ema9 = TechnicalAnalyzer.calculate_ema(prices, 9)
        ema21 = TechnicalAnalyzer.calculate_ema(prices, 21)
        ema50 = TechnicalAnalyzer.calculate_ema(prices, 50)
        rsi = TechnicalAnalyzer.calculate_rsi(prices)
        
        if not all([ema9, ema21, ema50, rsi]):
            return None
        
        # Calcular score (0-100)
        score = 50
        
        # Tendência (30 pontos)
        if ema9 > ema21 > ema50:
            score += 30
        elif ema9 < ema21 < ema50:
            score -= 30
        
        # RSI (20 pontos)
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

class IntelligentDaemon:
    """Daemon principal - Análise + Persistência"""
    
    def __init__(self):
        self.db = MySQLConnection()
        self.analyzer = TechnicalAnalyzer()
    
    def fetch_prices(self, symbol: str, period: str = '100d') -> Optional[List[float]]:
        """Baixa histórico de preços via yfinance"""
        try:
            data = yf.download(symbol, period=period, progress=False, quiet=True)
            if data.empty:
                return None
            return data['Close'].values.tolist()
        except Exception as e:
            logger.debug(f"Erro ao baixar {symbol}: {e}")
            return None
    
    def analyze_symbol(self, symbol: str, market_type: str) -> Optional[Dict]:
        """Analisa um símbolo e retorna resultado"""
        prices = self.fetch_prices(symbol)
        if not prices:
            return None
        
        current_price = prices[-1]
        analysis = self.analyzer.analyze(symbol, prices, current_price)
        
        if analysis:
            analysis['market_type'] = market_type
        
        return analysis
    
    def persist_signal(self, signal: Dict) -> bool:
        """Persiste um sinal no MySQL"""
        query = """
            INSERT INTO market_signals 
            (symbol, market_type, price, score, signal, rsi, ema9, ema21, ema50, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        params = (
            signal['symbol'],
            signal['market_type'],
            signal['price'],
            signal['score'],
            signal['signal'],
            signal['rsi'],
            signal['ema9'],
            signal['ema21'],
            signal['ema50']
        )
        return self.db.execute_query(query, params)
    
    def update_portfolio_metrics(self, total_signals: int, buy_signals: int, sell_signals: int):
        """Atualiza métricas agregadas do portfólio"""
        # Valores placeholder por enquanto (serão conectados ao trading real depois)
        query = """
            INSERT INTO portfolio_metrics 
            (total_portfolio_value, total_pnl, pnl_percentage, win_rate, total_trades, open_positions, capital_deployed, last_analysis_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        """
        params = (
            1000000,      # $1M initial
            0,            # PnL será calculado por trades reais
            0.0,          # PnL %
            50.0,         # Win rate inicial
            0,            # total trades
            0,            # open positions
            0,            # capital deployed
        )
        return self.db.execute_query(query, params)
    
    def log_analysis(self, results: List[Dict]):
        """Registra resumo da análise"""
        buy = len([r for r in results if 'COMPRA' in r['signal']])
        sell = len([r for r in results if 'VENDA' in r['signal']])
        hold = len([r for r in results if 'MANTER' in r['signal']])
        
        top_buy = ', '.join([r['symbol'] for r in sorted(results, key=lambda x: x['score'], reverse=True)[:5] if 'COMPRA' in r['signal']])
        top_sell = ', '.join([r['symbol'] for r in sorted(results, key=lambda x: x['score'])[:5] if 'VENDA' in r['signal']])
        
        query = """
            INSERT INTO analysis_logs 
            (total_analyzed, buy_signals, sell_signals, hold_signals, top_buy, top_sell)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (len(results), buy, sell, hold, top_buy[:100], top_sell[:100])
        return self.db.execute_query(query, params)
    
    def run_analysis(self) -> List[Dict]:
        """Executa análise para todos os ativos"""
        logger.info(f"\n{'='*60}")
        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] 🚀 INICIANDO ANÁLISE")
        logger.info(f"{'='*60}")
        
        results = []
        
        # Analisar B3
        logger.info(f"\n📊 B3 ({len(SYMBOLS['B3'])} ativos)...")
        for symbol in SYMBOLS['B3']:
            analysis = self.analyze_symbol(symbol, 'B3')
            if analysis:
                results.append(analysis)
                if 'COMPRA' in analysis['signal'] or 'VENDA' in analysis['signal']:
                    logger.info(f"  {symbol}: {analysis['signal']} (Score: {analysis['score']}/100)")
        
        # Analisar NYSE
        logger.info(f"\n📊 NYSE ({len(SYMBOLS['NYSE'])} ativos)...")
        for symbol in SYMBOLS['NYSE']:
            analysis = self.analyze_symbol(symbol, 'NYSE')
            if analysis:
                results.append(analysis)
                if 'COMPRA' in analysis['signal'] or 'VENDA' in analysis['signal']:
                    logger.info(f"  {symbol}: {analysis['signal']} (Score: {analysis['score']}/100)")
        
        # Persistir sinais no MySQL
        logger.info(f"\n💾 Persistindo {len(results)} sinais no MySQL...")
        persisted = 0
        for signal in results:
            if self.persist_signal(signal):
                persisted += 1
        logger.info(f"✅ {persisted}/{len(results)} sinais persistidos")
        
        # Atualizar métricas
        logger.info(f"\n📈 Atualizando métricas do portfólio...")
        buy_signals = len([r for r in results if 'COMPRA' in r['signal']])
        sell_signals = len([r for r in results if 'VENDA' in r['signal']])
        self.update_portfolio_metrics(len(results), buy_signals, sell_signals)
        
        # Log análise
        self.log_analysis(results)
        
        # Resumo
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 RESULTADO FINAL:")
        logger.info(f"  Total analisados: {len(results)}")
        logger.info(f"  🟢 Sinais de COMPRA: {buy_signals}")
        logger.info(f"  🔴 Sinais de VENDA: {sell_signals}")
        logger.info(f"  🟡 Sinais de MANTER: {len([r for r in results if 'MANTER' in r['signal']])}")
        logger.info(f"{'='*60}\n")
        
        return results
    
    def start(self):
        """Inicia o daemon em loop contínuo"""
        logger.info("🚀 Egreja Investment AI - DAEMON v2")
        logger.info("Sistema: Análise → MySQL Persistence → Ready for API")
        logger.info(f"Intervalo de análise: {ANALYSIS_INTERVAL/60:.0f} minutos\n")
        
        while True:
            try:
                self.run_analysis()
                logger.info(f"⏳ Próxima análise em {ANALYSIS_INTERVAL/60:.0f} minutos...\n")
                time.sleep(ANALYSIS_INTERVAL)
            
            except KeyboardInterrupt:
                logger.info("\n👋 Daemon parado pelo usuário")
                break
            
            except Exception as e:
                logger.error(f"❌ Erro crítico: {e}")
                logger.info(f"⏳ Tentando novamente em {RETRY_INTERVAL/60:.0f} minuto(s)...")
                time.sleep(RETRY_INTERVAL)

if __name__ == '__main__':
    daemon = IntelligentDaemon()
    daemon.start()
