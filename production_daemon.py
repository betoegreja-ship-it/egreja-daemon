#!/usr/bin/env python3
"""
ArbitrageAI Production Daemon
Daemon simplificado e otimizado para produção
Sem dependências complexas, apenas o essencial
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import mysql.connector
from dotenv import load_dotenv
import requests

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/ubuntu/arbitrage-dashboard/production.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()


class ProductionDaemon:
    """Daemon de produção simplificado"""
    
    def __init__(self):
        """Inicializar daemon"""
        self.db_config = self._parse_database_url()
        
        # Configurações de produção
        self.config = {
            'analysis_interval': 3600,  # 1 hora
            'min_score': 70,
            'max_open_trades': 3,
            'max_position_size': 0.30,
            'take_profit_pct': 0.03,
            'stop_loss_pct': 0.02,
            'max_trade_duration': 7200,  # 2 horas
            'circuit_breaker_loss': 0.05
        }
        
        # Símbolos para operar
        self.symbols = [
            'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT',
            'LTCUSDT', 'DOGEUSDT', 'MATICUSDT', 'SOLUSDT', 'DOTUSDT'
        ]
        
        self.running = True
        self.circuit_breaker_active = False
        self.circuit_breaker_until = None
        
        logger.info("="*70)
        logger.info("🚀 ARBITRAGEAI PRODUCTION DAEMON INICIADO")
        logger.info("="*70)
        logger.info(f"   Intervalo de análise: {self.config['analysis_interval']/60:.0f} minutos")
        logger.info(f"   Score mínimo: {self.config['min_score']}")
        logger.info(f"   Take-profit: {self.config['take_profit_pct']*100}%")
        logger.info(f"   Stop-loss: {self.config['stop_loss_pct']*100}%")
        logger.info(f"   Máximo trades: {self.config['max_open_trades']}")
        logger.info("="*70)
    
    def _parse_database_url(self) -> Dict[str, str]:
        """Parse DATABASE_URL"""
        url = os.getenv('DATABASE_URL')
        if not url:
            raise ValueError("DATABASE_URL não encontrada")
        
        parts = url.replace('mysql://', '').split('@')
        user_pass = parts[0].split(':')
        host_port_db = parts[1].split('/')
        host_port = host_port_db[0].split(':')
        db_name = host_port_db[1].split('?')[0]
        
        return {
            'host': host_port[0],
            'port': int(host_port[1]),
            'user': user_pass[0],
            'password': user_pass[1],
            'database': db_name,
            'ssl_disabled': False,
            'ssl_verify_cert': False,
            'ssl_verify_identity': False
        }
    
    def _get_db_connection(self):
        """Cria conexão com banco"""
        return mysql.connector.connect(**self.db_config)
    
    def get_current_price(self, symbol: str) -> Optional[float]:
        """Busca preço atual (Binance primeiro, CoinGecko como fallback)"""
        # Tentar Binance primeiro
        try:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            price = float(data['price'])
            if price > 0:
                return price
        except Exception as e:
            logger.debug(f"Binance falhou para {symbol}: {e}")
        
        # Fallback: CoinGecko
        try:
            coin_map = {
                'BTCUSDT': 'bitcoin', 'ETHUSDT': 'ethereum', 'BNBUSDT': 'binancecoin',
                'ADAUSDT': 'cardano', 'XRPUSDT': 'ripple', 'SOLUSDT': 'solana',
                'DOTUSDT': 'polkadot', 'MATICUSDT': 'matic-network',
                'LTCUSDT': 'litecoin', 'DOGEUSDT': 'dogecoin',
                'SHIBUSDT': 'shiba-inu', 'FLOKIUSDT': 'floki'
            }
            
            coin_id = coin_map.get(symbol)
            if not coin_id:
                logger.error(f"Símbolo {symbol} não mapeado para CoinGecko")
                return None
            
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if coin_id in data and 'usd' in data[coin_id]:
                price = float(data[coin_id]['usd'])
                logger.info(f"✅ Preço CoinGecko para {symbol}: ${price:,.8f}")
                return price
            
            logger.error(f"Preço não encontrado no CoinGecko para {symbol}")
            return None
            
        except Exception as e:
            logger.error(f"Erro ao buscar preço de {symbol} (CoinGecko): {e}")
            return None
    
    def calculate_simple_score(self, symbol: str) -> Dict:
        """
        Calcula score simplificado baseado em EMAs
        Retorna dict com score, signal e confidence
        """
        try:
            # Tentar Binance primeiro
            candles = self._fetch_candles_binance(symbol)
            
            # Se falhar, tentar CoinGecko
            if not candles:
                candles = self._fetch_candles_coingecko(symbol)
            
            # Se ainda falhar, usar dados simulados
            if not candles:
                logger.warning(f"Usando dados simulados para {symbol}")
                return self._generate_simulated_analysis(symbol)
            
            if not candles:
                return {'score': 0, 'signal': 'HOLD', 'confidence': 0}
            
            # Extrair closes
            closes = [float(c[4]) for c in candles]
            current_price = closes[-1]
            
            # Calcular EMAs simples
            ema_9 = self._simple_ema(closes, 9)
            ema_21 = self._simple_ema(closes, 21)
            ema_50 = self._simple_ema(closes, 50)
            
            # Determinar tendência
            score = 50  # Neutro
            
            # Tendência de alta forte
            if current_price > ema_9 > ema_21 > ema_50:
                score = 85
                signal = "BUY"
                confidence = 85
            # Tendência de alta moderada
            elif current_price > ema_21 > ema_50:
                score = 75
                signal = "BUY"
                confidence = 75
            # Tendência de baixa forte
            elif current_price < ema_9 < ema_21 < ema_50:
                score = 15
                signal = "SELL"
                confidence = 85
            # Tendência de baixa moderada
            elif current_price < ema_21 < ema_50:
                score = 25
                signal = "SELL"
                confidence = 75
            # Lateral
            else:
                score = 50
                signal = "HOLD"
                confidence = 40
            
            return {
                'score': score,
                'signal': signal,
                'confidence': confidence,
                'current_price': current_price,
                'ema_9': ema_9,
                'ema_21': ema_21,
                'ema_50': ema_50
            }
            
        except Exception as e:
            logger.error(f"Erro ao calcular score de {symbol}: {e}")
            return {'score': 0, 'signal': 'HOLD', 'confidence': 0}
    
    def _fetch_candles_binance(self, symbol: str) -> Optional[List]:
        """Busca candles da Binance"""
        try:
            url = "https://api.binance.com/api/v3/klines"
            params = {'symbol': symbol, 'interval': '1h', 'limit': 200}
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except:
            return None
    
    def _fetch_candles_coingecko(self, symbol: str) -> Optional[List]:
        """Busca dados do CoinGecko (fallback)"""
        try:
            # Mapear símbolo Binance para ID CoinGecko
            coin_map = {
                'BTCUSDT': 'bitcoin', 'ETHUSDT': 'ethereum', 'BNBUSDT': 'binancecoin',
                'ADAUSDT': 'cardano', 'XRPUSDT': 'ripple', 'LTCUSDT': 'litecoin',
                'DOGEUSDT': 'dogecoin', 'MATICUSDT': 'matic-network',
                'SOLUSDT': 'solana', 'DOTUSDT': 'polkadot'
            }
            
            coin_id = coin_map.get(symbol)
            if not coin_id:
                return None
            
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
            params = {'vs_currency': 'usd', 'days': '7', 'interval': 'hourly'}
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            # Converter formato CoinGecko para formato Binance
            prices = data.get('prices', [])
            candles = [[0, 0, 0, 0, price[1], 0, 0, 0, 0, 0, 0, 0] for price in prices]
            return candles[-200:]  # Últimos 200 pontos
        except Exception as e:
            logger.debug(f"CoinGecko falhou para {symbol}: {e}")
            return None
    
    def _generate_simulated_analysis(self, symbol: str) -> Dict:
        """Gera análise simulada quando APIs falham"""
        import random
        return {
            'score': random.randint(40, 60),
            'signal': 'HOLD',
            'confidence': 40,
            'current_price': 0,
            'ema_9': 0,
            'ema_21': 0,
            'ema_50': 0
        }
    
    def _simple_ema(self, prices: List[float], period: int) -> float:
        """Calcula EMA simples"""
        if len(prices) < period:
            return prices[-1] if prices else 0
        
        multiplier = 2 / (period + 1)
        ema = sum(prices[:period]) / period
        
        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema
        
        return ema
    
    def get_open_trades_count(self) -> int:
        """Conta trades abertos"""
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM trades WHERE status = 'OPEN'")
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return count
        except Exception as e:
            logger.error(f"Erro ao contar trades abertos: {e}")
            return 0
    
    def check_circuit_breaker(self) -> bool:
        """Verifica se circuit breaker deve ser ativado"""
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            # Calcular P&L das últimas 24h
            query = """
                SELECT SUM(pnl) as total_loss
                FROM trades
                WHERE status = 'CLOSED'
                AND closed_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            """
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result and result['total_loss']:
                total_loss = float(result['total_loss'])
                loss_pct = abs(total_loss / 1000000)  # Capital inicial $1M
                
                if loss_pct >= self.config['circuit_breaker_loss']:
                    self.circuit_breaker_active = True
                    self.circuit_breaker_until = datetime.now() + timedelta(hours=6)
                    logger.warning(f"⚠️  CIRCUIT BREAKER ATIVADO - Perda de {loss_pct*100:.2f}% em 24h")
                    logger.warning(f"   Trading pausado até {self.circuit_breaker_until.strftime('%H:%M:%S')}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Erro ao verificar circuit breaker: {e}")
            return False
    
    def open_trade(self, symbol: str, analysis: Dict) -> bool:
        """Abre um novo trade"""
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            current_price = analysis['current_price']
            quantity = (1000000 * self.config['max_position_size']) / current_price
            
            query = """
                INSERT INTO trades (
                    symbol, entry_price, quantity, status, signal,
                    confidence, opened_at
                ) VALUES (%s, %s, %s, 'OPEN', %s, %s, NOW())
            """
            
            cursor.execute(query, (
                symbol,
                current_price,
                quantity,
                analysis['signal'],
                analysis['confidence']
            ))
            
            conn.commit()
            trade_id = cursor.lastrowid
            cursor.close()
            conn.close()
            
            logger.info(f"✅ TRADE ABERTO #{trade_id}")
            logger.info(f"   Símbolo: {symbol}")
            logger.info(f"   Sinal: {analysis['signal']}")
            logger.info(f"   Preço: ${current_price:,.2f}")
            logger.info(f"   Confiança: {analysis['confidence']}%")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao abrir trade: {e}")
            return False
    
    def monitor_open_trades(self):
        """Monitora trades abertos e fecha se necessário"""
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            query = """
                SELECT * FROM trades 
                WHERE status = 'OPEN'
            """
            cursor.execute(query)
            open_trades = cursor.fetchall()
            
            for trade in open_trades:
                symbol = trade['symbol']
                entry_price = float(trade['entry_price'])
                quantity = float(trade['quantity'])
                opened_at = trade['opened_at']
                
                # Buscar preço atual
                current_price = self.get_current_price(symbol)
                if not current_price:
                    continue
                
                # Calcular P&L
                pnl_pct = ((current_price - entry_price) / entry_price)
                pnl = (current_price - entry_price) * quantity
                
                # Calcular duração
                duration = (datetime.now() - opened_at).total_seconds()
                
                # Verificar condições de fechamento
                should_close = False
                close_reason = None
                
                if pnl_pct >= self.config['take_profit_pct']:
                    should_close = True
                    close_reason = 'TAKE_PROFIT'
                elif pnl_pct <= -self.config['stop_loss_pct']:
                    should_close = True
                    close_reason = 'STOP_LOSS'
                elif duration >= self.config['max_trade_duration']:
                    should_close = True
                    close_reason = 'TIMEOUT'
                
                if should_close:
                    # Fechar trade
                    update_query = """
                        UPDATE trades
                        SET status = 'CLOSED',
                            exit_price = %s,
                            pnl = %s,
                            close_reason = %s,
                            closed_at = NOW()
                        WHERE id = %s
                    """
                    cursor.execute(update_query, (
                        current_price,
                        pnl,
                        close_reason,
                        trade['id']
                    ))
                    conn.commit()
                    
                    logger.info(f"🔴 TRADE FECHADO #{trade['id']}")
                    logger.info(f"   Símbolo: {symbol}")
                    logger.info(f"   Motivo: {close_reason}")
                    logger.info(f"   P&L: ${pnl:+,.2f} ({pnl_pct*100:+.2f}%)")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Erro ao monitorar trades: {e}")
    
    def analyze_and_trade(self):
        """Analisa mercado e executa trades"""
        logger.info("\n" + "="*70)
        logger.info(f"📊 ANÁLISE DE MERCADO - {datetime.now().strftime('%H:%M:%S')}")
        logger.info("="*70)
        
        # Verificar circuit breaker
        if self.circuit_breaker_active:
            if datetime.now() < self.circuit_breaker_until:
                logger.warning(f"⚠️  Circuit breaker ativo até {self.circuit_breaker_until.strftime('%H:%M:%S')}")
                return
            else:
                self.circuit_breaker_active = False
                logger.info("✅ Circuit breaker desativado - Retomando trading")
        
        self.check_circuit_breaker()
        if self.circuit_breaker_active:
            return
        
        # Verificar trades abertos
        open_count = self.get_open_trades_count()
        logger.info(f"   Trades abertos: {open_count}/{self.config['max_open_trades']}")
        
        if open_count >= self.config['max_open_trades']:
            logger.info("   ⚠️  Máximo de trades abertos atingido")
            return
        
        # Analisar símbolos
        opportunities = []
        
        for symbol in self.symbols:
            analysis = self.calculate_simple_score(symbol)
            
            if analysis['score'] >= self.config['min_score'] and analysis['signal'] != 'HOLD':
                opportunities.append((symbol, analysis))
                logger.info(f"   🎯 {symbol}: Score {analysis['score']} | {analysis['signal']} | Confiança {analysis['confidence']}%")
        
        # Ordenar por score
        opportunities.sort(key=lambda x: x[1]['score'], reverse=True)
        
        # Abrir trades
        trades_to_open = min(len(opportunities), self.config['max_open_trades'] - open_count)
        
        if trades_to_open == 0:
            logger.info("   ℹ️  Nenhuma oportunidade encontrada no momento")
        else:
            logger.info(f"\n   🚀 Abrindo {trades_to_open} trade(s)...")
            for i in range(trades_to_open):
                symbol, analysis = opportunities[i]
                self.open_trade(symbol, analysis)
                time.sleep(1)  # Aguardar 1s entre trades
    
    def run(self):
        """Loop principal do daemon"""
        logger.info("🔄 Daemon em execução...")
        
        iteration = 0
        
        while self.running:
            try:
                iteration += 1
                
                # Monitorar trades abertos
                self.monitor_open_trades()
                
                # Analisar mercado e executar trades
                if iteration % 1 == 0:  # A cada iteração
                    self.analyze_and_trade()
                
                # Aguardar intervalo
                logger.info(f"\n⏳ Próxima análise em {self.config['analysis_interval']/60:.0f} minutos...")
                time.sleep(self.config['analysis_interval'])
                
            except KeyboardInterrupt:
                logger.info("\n⚠️  Interrupção detectada - Encerrando daemon...")
                self.running = False
                break
            except Exception as e:
                logger.error(f"❌ Erro no loop principal: {e}")
                time.sleep(60)  # Aguardar 1 minuto antes de tentar novamente


def main():
    """Ponto de entrada"""
    try:
        daemon = ProductionDaemon()
        daemon.run()
    except Exception as e:
        logger.error(f"❌ Erro fatal: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
