#!/usr/bin/env python3
"""
ArbitrageAI - Daemon Unificado Inteligente
Sistema autônomo que integra: análise + trades + monitoramento + aprendizado

Características:
- Analisa mercado continuamente (15min)
- Abre/fecha trades automaticamente
- Aprende com resultados (ML básico)
- Se auto-aprimora diariamente
- Documenta tudo automaticamente
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv
import requests
from collections import defaultdict

# ⚠️ CONFIGURAR LOGGING ANTES DE QUALQUER OUTRO CÓDIGO
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/ubuntu/arbitrage-dashboard/intelligent_daemon.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Importar ML Predictor, Market Data, Technical Analysis e Price History
try:
    from ml_predictor import MLPredictor
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False

try:
    from market_data import get_current_price as md_get_price, get_all_symbols, CRYPTO_SYMBOLS, B3_STOCKS, NYSE_STOCKS
    MARKET_DATA_AVAILABLE = True
except ImportError:
    MARKET_DATA_AVAILABLE = False

try:
    from technical_analysis import TechnicalAnalyzer
    from price_history import PriceHistoryFetcher
    TECHNICAL_ANALYSIS_AVAILABLE = True
except ImportError:
    TECHNICAL_ANALYSIS_AVAILABLE = False
    logger.warning("⚠️  Módulos de análise técnica não disponíveis")

try:
    from telegram_notifier import TelegramNotifier
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    logger.warning("⚠️  Módulo Telegram não disponível")

try:
    from market_hours import market_hours
    MARKET_HOURS_AVAILABLE = True
except ImportError:
    MARKET_HOURS_AVAILABLE = False
    logger.warning("⚠️  Módulo Market Hours não disponível")


class IntelligentDaemon:
    """Daemon Unificado Inteligente - O Cérebro da Operação"""
    
    def __init__(self):
        """Inicializar daemon inteligente"""
        self.db_config = self._parse_database_url()
        self.db_pool = None
        self._init_connection_pool()
        
        # Configurações otimizadas
        self.config = {
            'analysis_interval': 900,  # 15 minutos
            'min_score': 30,  # Score mínimo para análise técnica real (0-100 baseado em indicadores)
            'max_open_trades': 10,  # Máximo de trades simultâneos
            'max_position_size': 0.10,  # 10% do capital por trade
            'take_profit_pct': 0.03,  # 3% de lucro
            'stop_loss_pct': 0.02,  # 2% de perda
            'max_trade_duration': 7200,  # 2 horas
            'circuit_breaker_loss': 0.05,  # 5% de perda diária
        }
        
        # Símbolos expandidos (40 ativos: 20 cryptos + 10 B3 + 10 NYSE)
        if MARKET_DATA_AVAILABLE:
            self.symbols = CRYPTO_SYMBOLS + B3_STOCKS + NYSE_STOCKS
            logger.info(f"✅ market_data.py carregado - {len(self.symbols)} ativos disponíveis")
        else:
            # Fallback: apenas cryptos
            self.symbols = [
                'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT',
                'LTCUSDT', 'DOGEUSDT', 'MATICUSDT', 'SOLUSDT', 'DOTUSDT',
                'AVAXUSDT', 'LINKUSDT', 'ATOMUSDT', 'UNIUSDT', 'FILUSDT',
                'NEARUSDT', 'ALGOUSDT', 'VETUSDT', 'SHIBUSDT', 'PEPEUSDT',
                'FLOKIUSDT', 'BONKUSDT', 'CRVUSDT', 'SUSHIUSDT', 'AAVEUSDT'
            ]
            logger.warning("⚠️  Usando apenas cryptos (market_data.py não disponível)")
        
        # Mapeamento CoinGecko
        self.coingecko_map = {
            'BTCUSDT': 'bitcoin', 'ETHUSDT': 'ethereum', 'BNBUSDT': 'binancecoin',
            'ADAUSDT': 'cardano', 'XRPUSDT': 'ripple', 'SOLUSDT': 'solana',
            'DOTUSDT': 'polkadot', 'MATICUSDT': 'matic-network',
            'LTCUSDT': 'litecoin', 'DOGEUSDT': 'dogecoin',
            'AVAXUSDT': 'avalanche-2', 'LINKUSDT': 'chainlink',
            'ATOMUSDT': 'cosmos', 'UNIUSDT': 'uniswap',
            'FILUSDT': 'filecoin', 'NEARUSDT': 'near',
            'ALGOUSDT': 'algorand', 'VETUSDT': 'vechain',
            'SHIBUSDT': 'shiba-inu', 'PEPEUSDT': 'pepe',
            'FLOKIUSDT': 'floki', 'BONKUSDT': 'bonk',
            'CRVUSDT': 'curve-dao-token', 'SUSHIUSDT': 'sushi',
            'AAVEUSDT': 'aave'
        }
        
        # Cache de preços (5 minutos)
        self.price_cache = {}
        self.cache_ttl = 300
        
        # Sistema de aprendizado
        self.performance_history = defaultdict(list)
        self.best_config = self.config.copy()
        
        # Contador de reconexões
        self.reconnect_count = 0
        self.last_reconnect = None
        
        # ML Predictor
        self.ml_predictor = None
        if ML_AVAILABLE:
            try:
                self.ml_predictor = MLPredictor(self.db_config)
                logger.info("🤖 ML Predictor inicializado")
            except Exception as e:
                logger.error(f"Erro ao inicializar ML Predictor: {e}")
        
        # Analisador Técnico e Fetcher de Histórico
        self.technical_analyzer = None
        self.price_fetcher = None
        if TECHNICAL_ANALYSIS_AVAILABLE:
            try:
                self.technical_analyzer = TechnicalAnalyzer()
                self.price_fetcher = PriceHistoryFetcher()
                logger.info("📈 Análise técnica REAL ativada (sem simulações)")
            except Exception as e:
                logger.error(f"Erro ao inicializar análise técnica: {e}")
        else:
            logger.warning("⚠️  Análise técnica não disponível - usando modo simplificado")
        
        # Telegram Notifier
        self.telegram = None
        if TELEGRAM_AVAILABLE:
            try:
                self.telegram = TelegramNotifier()
                if self.telegram.enabled:
                    logger.info("📢 Notificações Telegram ativadas")
            except Exception as e:
                logger.error(f"Erro ao inicializar Telegram: {e}")
        
        # Estado do daemon
        self.running = True
        self.circuit_breaker_active = False
        self.circuit_breaker_until = None
        self.last_backtest = None
        self.last_backup = None  # ✅ Inicializar corretamente (não usar hasattr)
        
        logger.info("="*80)
        logger.info("🧠 ARBITRAGEAI - DAEMON UNIFICADO INTELIGENTE INICIADO")
        logger.info("="*80)
        logger.info(f"   Intervalo de análise: {self.config['analysis_interval']/60:.0f} minutos")
        logger.info(f"   Score mínimo: {self.config['min_score']}")
        logger.info(f"   Máximo trades: {self.config['max_open_trades']}")
        logger.info(f"   Símbolos monitorados: {len(self.symbols)}")
        logger.info(f"   Sistema de aprendizado: ATIVO")
        logger.info("="*80)
    
    def _parse_database_url(self) -> Dict[str, str]:
        """Parse DATABASE_URL usando urllib (robusto com senhas especiais)"""
        from urllib.parse import urlparse, unquote
        
        url = os.getenv('DATABASE_URL')
        if not url:
            raise ValueError("DATABASE_URL não encontrada")
        
        try:
            parsed = urlparse(url)
            
            # Decodificar username e password (suporta senhas com @, :, /, etc)
            username = unquote(parsed.username) if parsed.username else ''
            password = unquote(parsed.password) if parsed.password else ''
            
            # Database name (remove query string)
            database = parsed.path.lstrip('/').split('?')[0] if parsed.path else ''
            
            if not username or not password or not database or not parsed.hostname:
                raise ValueError("DATABASE_URL inválida ou incompleta")
            
            port = parsed.port or 3306
            
            logger.info(f"✅ DATABASE_URL parseado: user={username}@{parsed.hostname}:{port}/{database}")
            
            return {
                'host': parsed.hostname,
                'port': port,
                'user': username,
                'password': password,
                'database': database,
                'ssl_disabled': False,
                'ssl_verify_cert': False,
                'ssl_verify_identity': False
            }
            
        except Exception as e:
            logger.error(f"❌ Erro ao parsear DATABASE_URL: {e}")
            raise ValueError(f"DATABASE_URL inválida: {e}")
    
    def _init_connection_pool(self):
        """Inicializa pool de conexões MySQL (elimina reconexões excessivas)"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                pool_config = {**self.db_config, 'pool_name': 'arbitrage_pool', 'pool_size': 5}
                self.db_pool = pooling.MySQLConnectionPool(**pool_config)
                logger.info("✅ Connection Pool MySQL inicializado (pool_size=5)")
                return
            except Exception as e:
                logger.error(f"❌ Erro ao criar pool MySQL (tentativa {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    raise
    
    def _execute_query(self, query: str, params: tuple = None, fetch: bool = True):
        """Executa query usando pool de conexões - sem reconexões manuais"""
        max_retries = 3
        for attempt in range(max_retries):
            conn = None
            cursor = None
            try:
                conn = self.db_pool.get_connection()
                cursor = conn.cursor(dictionary=True)
                cursor.execute(query, params or ())
                if fetch:
                    result = cursor.fetchall()
                else:
                    conn.commit()
                    result = cursor.lastrowid
                return result
            except mysql.connector.errors.PoolError as e:
                logger.error(f"❌ Pool esgotado (tentativa {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise
            except mysql.connector.errors.OperationalError as e:
                logger.error(f"❌ Erro operacional MySQL (tentativa {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    raise
            except Exception as e:
                logger.error(f"❌ Erro ao executar query: {e}")
                raise
            finally:
                if cursor:
                    try: cursor.close()
                    except: pass
                if conn:
                    try: conn.close()  # Devolve ao pool
                    except: pass
    
    def _get_db_connection(self):
        """Retorna conexão do pool (compatibilidade com código legado)"""
        return self.db_pool.get_connection()
    
    def get_current_price(self, symbol: str) -> Optional[float]:
        """Busca preço atual usando market_data.py (com fallback)"""
        # Verificar cache
        cache_key = symbol
        if cache_key in self.price_cache:
            cached_price, cached_time = self.price_cache[cache_key]
            if time.time() - cached_time < self.cache_ttl:
                return cached_price
        
        # Usar market_data.py se disponível
        if MARKET_DATA_AVAILABLE:
            try:
                price = md_get_price(symbol)
                if price and price > 0:
                    self.price_cache[cache_key] = (price, time.time())
                    return price
            except Exception as e:
                logger.warning(f"market_data.get_current_price falhou para {symbol}: {e}")
        
        # Fallback: Binance direto
        try:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            price = float(data['price'])
            if price > 0:
                self.price_cache[cache_key] = (price, time.time())
                return price
        except Exception as e:
            logger.debug(f"Binance falhou para {symbol}: {e}")
        
        # Fallback: CoinGecko
        try:
            coin_id = self.coingecko_map.get(symbol)
            if not coin_id:
                logger.error(f"Símbolo {symbol} não mapeado para CoinGecko")
                return None
            
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if coin_id in data and 'usd' in data[coin_id]:
                price = float(data[coin_id]['usd'])
                self.price_cache[cache_key] = (price, time.time())
                logger.info(f"✅ Preço CoinGecko para {symbol}: ${price:,.8f}")
                time.sleep(5)  # Rate limit aumentado para 5s
                return price
            
            logger.error(f"Preço não encontrado no CoinGecko para {symbol}")
            return None
            
        except Exception as e:
            logger.error(f"Erro ao buscar preço de {symbol}: {e}")
            return None
    
    def run(self):
        """Loop principal do daemon"""
        logger.info("🚀 Daemon iniciado - entrando em loop principal")
        
        # Exibir status de mercados na inicialização
        if MARKET_HOURS_AVAILABLE:
            logger.info("🌐 Status dos Mercados:")
            b3_open = market_hours.is_market_open('PETR4.SA')
            nyse_open = market_hours.is_market_open('AAPL')
            b3_status = '✅ ABERTO' if b3_open else '❌ FECHADO'
            nyse_status = '✅ ABERTO' if nyse_open else '❌ FECHADO'
            logger.info(f"   B3: {b3_status} (10h-17h BRT)")
            logger.info(f"   NYSE: {nyse_status} (9h30-16h EST)")
            logger.info("✅ Crypto: ABERTO (24/7)")
        
        while self.running:
            try:
                # 1. Verificar circuit breaker
                if self.circuit_breaker_active:
                    if datetime.now() < self.circuit_breaker_until:
                        logger.warning(f"⚠️  Circuit breaker ativo até {self.circuit_breaker_until}")
                        time.sleep(300)  # 5 minutos
                        continue
                    else:
                        self.circuit_breaker_active = False
                        logger.info("✅ Circuit breaker desativado")
                
                # 2. Fechar trades expirados
                self.close_expired_trades()
                
                # 3. Fechar trades com take-profit/stop-loss
                self.close_profitable_trades()
                
                # 4. Limpar análises antigas (>2h)
                self.clean_old_analyses()
                
                # 5. Analisar mercado e abrir trades
                self.analyze_and_trade()
                
                # 6. Aprender com resultados
                self.learn_from_results()
                
                # 7. Backup diário (3h da manhã)
                if self._should_run_backup():
                    self.run_daily_backup()
                    self.last_backup = datetime.now()
                
                # 8. Backtesting diário
                if self._should_run_backtest():
                    self.run_daily_backtest()
                
                # 7. Aguardar próximo ciclo com sleeps curtos (resistente a hibernação do sandbox)
                next_analysis = datetime.now() + timedelta(seconds=self.config['analysis_interval'])
                last_monitor = datetime.now()
                logger.info(f"⏳ Aguardando {self.config['analysis_interval']/60:.0f} minutos até próxima análise... (próxima: {next_analysis.strftime('%H:%M:%S')})")
                
                # Loop com sleeps de 30s para não travar em hibernação
                while datetime.now() < next_analysis:
                    time.sleep(30)  # Sleep curto - acorda a cada 30s mesmo após hibernação
                    now = datetime.now()
                    
                    # Verificar se o sandbox hibernou (salto de tempo > 5 min)
                    time_since_last = (now - last_monitor).total_seconds()
                    if time_since_last > 300:  # Mais de 5 min = sandbox hibernou
                        logger.warning(f"⚠️  Sandbox hibernou por {time_since_last/60:.1f} min - reiniciando ciclo imediatamente")
                        break  # Sair do loop de espera e executar novo ciclo
                    
                    # Monitorar trades a cada 2 minutos
                    if time_since_last >= 120:
                        self.close_expired_trades()
                        self.close_profitable_trades()
                        last_monitor = now  # Atualizar apenas após monitorar
                
            except KeyboardInterrupt:
                logger.info("🛑 Daemon interrompido pelo usuário")
                self.running = False
            except mysql.connector.errors.OperationalError as e:
                logger.error(f"❌ Erro de conexão MySQL: {e}")
                logger.info("🔄 Reinicializando pool de conexões...")
                try:
                    self._init_connection_pool()
                    logger.info("✅ Pool reinicializado com sucesso")
                except Exception as reconnect_error:
                    logger.error(f"❌ Falha ao reinicializar pool: {reconnect_error}")
                    time.sleep(60)
            except Exception as e:
                logger.error(f"❌ Erro no loop principal: {e}", exc_info=True)
                logger.info("🔄 Auto-recovery: reiniciando em 60 segundos...")
                time.sleep(60)
    
    def close_expired_trades(self):
        """Fecha trades que excederam duração máxima"""
        conn = None
        cursor = None
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT id, symbol, entry_price, quantity, recommendation, created_at
                FROM trades
                WHERE status = 'OPEN'
                AND TIMESTAMPDIFF(SECOND, created_at, NOW()) > %s
            """, (self.config['max_trade_duration'],))
            
            expired_trades = cursor.fetchall()
            
            for trade in expired_trades:
                logger.info(f"⏰ Fechando trade expirado #{trade['id']} - {trade['symbol']}")
                
                current_price = self.get_current_price(trade['symbol'])
                if not current_price:
                    logger.warning(f"Não foi possível obter preço para {trade['symbol']}")
                    continue
                
                entry_price = float(trade['entry_price'])
                quantity = float(trade['quantity'])
                position_size = entry_price * quantity  # Valor investido em USD
                
                # P&L correto: percentual * valor investido
                if trade['recommendation'] == 'BUY':
                    pnl_pct = (current_price - entry_price) / entry_price
                else:
                    pnl_pct = (entry_price - current_price) / entry_price
                
                pnl = pnl_pct * position_size
                pnl_percent = pnl_pct * 100
                
                cursor.execute("""
                    UPDATE trades
                    SET status = 'CLOSED',
                        exit_price = %s,
                        pnl = %s,
                        pnl_percent = %s,
                        closed_at = NOW(),
                        close_reason = 'TIMEOUT'
                    WHERE id = %s AND status = 'OPEN'
                """, (current_price, pnl, pnl_percent, trade['id']))
                
                conn.commit()
                logger.info(f"✅ Trade #{trade['id']} fechado - P&L: ${pnl:,.2f} ({pnl_percent:+.2f}%)")
        
        except Exception as e:
            logger.error(f"❌ Erro em close_expired_trades: {e}", exc_info=True)
        
        finally:
            # ⚠️ SEMPRE devolver conexão ao pool, mesmo com erro
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()  # Devolve ao pool
                except:
                    pass
    
    def close_profitable_trades(self):
        """Fecha trades que atingiram take-profit, stop-loss ou trailing stop dinâmico"""
        conn = None
        cursor = None
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT id, symbol, entry_price, quantity, recommendation, trailing_stop_price
                FROM trades
                WHERE status = 'OPEN'
            """)
            
            open_trades = cursor.fetchall()
        
            for trade in open_trades:
            current_price = self.get_current_price(trade['symbol'])
            if not current_price:
                continue
            
            entry_price = float(trade['entry_price'])
            quantity = float(trade['quantity'])
            trailing_stop = float(trade['trailing_stop_price']) if trade['trailing_stop_price'] else None
            
            # Calcular P&L atual
            if trade['recommendation'] == 'BUY':
                pnl_pct = ((current_price - entry_price) / entry_price)
            else:
                pnl_pct = ((entry_price - current_price) / entry_price)
            
            close_reason = None
            
            # Verificar trailing stop primeiro (se existir)
            if trailing_stop:
                if trade['recommendation'] == 'BUY':
                    # BUY: fechar se preço cair abaixo do trailing stop
                    if current_price <= trailing_stop:
                        close_reason = 'TRAILING_STOP'
                else:
                    # SELL: fechar se preço subir acima do trailing stop
                    if current_price >= trailing_stop:
                        close_reason = 'TRAILING_STOP'
            
            # Se não atingiu trailing stop, verificar TP/SL normais
            if not close_reason:
                if pnl_pct >= self.config['take_profit_pct']:
                    close_reason = 'TAKE_PROFIT'
                elif pnl_pct <= -self.config['stop_loss_pct']:
                    close_reason = 'STOP_LOSS'
            
            # Atualizar trailing stop se trade está lucrativo
            if not close_reason and pnl_pct > 0:
                # A cada +1% de ganho, subir trailing stop +0.5%
                gain_percent = int(pnl_pct * 100)  # Quantos % de ganho
                
                if gain_percent >= 1:
                    # Calcular novo trailing stop
                    trailing_adjustment = 0.005 * gain_percent  # 0.5% por cada 1% de ganho
                    
                    if trade['recommendation'] == 'BUY':
                        new_trailing_stop = entry_price * (1 + trailing_adjustment)
                    else:
                        new_trailing_stop = entry_price * (1 - trailing_adjustment)
                    
                    # Atualizar apenas se novo trailing stop é melhor
                    if trailing_stop is None or (
                        (trade['recommendation'] == 'BUY' and new_trailing_stop > trailing_stop) or
                        (trade['recommendation'] == 'SELL' and new_trailing_stop < trailing_stop)
                    ):
                        cursor.execute("""
                            UPDATE trades
                            SET trailing_stop_price = %s
                            WHERE id = %s
                        """, (new_trailing_stop, trade['id']))
                        conn.commit()
                        logger.info(f"📊 Trade #{trade['id']} - {trade['symbol']}: Trailing stop ajustado para ${new_trailing_stop:,.2f} (Ganho: {pnl_pct*100:.1f}%)")
            
            # Atualizar P&L em tempo real para trade aberta (mesmo sem fechar)
            position_size = entry_price * quantity  # Valor investido em USD
            current_pnl = pnl_pct * position_size
            current_pnl_pct = pnl_pct * 100
            cursor.execute("""
                UPDATE trades SET pnl = %s, pnl_percent = %s WHERE id = %s AND status = 'OPEN'
            """, (current_pnl, current_pnl_pct, trade['id']))
            conn.commit()
            
            # Fechar trade se atingiu alguma condição
            if close_reason:
                pnl = current_pnl
                
                # ⚠️  RACE CONDITION FIX: Só fechar se ainda está OPEN (evita duplo fechamento)
                cursor.execute("""
                    UPDATE trades
                    SET status = 'CLOSED',
                        exit_price = %s,
                        pnl = %s,
                        pnl_percent = %s,
                        closed_at = NOW(),
                        close_reason = %s
                    WHERE id = %s AND status = 'OPEN'
                """, (current_price, pnl, current_pnl_pct, close_reason, trade['id']))
                
                conn.commit()
                
                # Verificar se o UPDATE foi bem-sucedido (idempotência)
                if cursor.rowcount == 0:
                    logger.warning(f"⚠️  Trade #{trade['id']} já foi fechada por outro processo (possível duplo fechamento impedido)")
                    continue
                
                emoji = '🎯' if close_reason == 'TAKE_PROFIT' else ('📊' if close_reason == 'TRAILING_STOP' else '🛑')
                logger.info(f"{emoji} Trade #{trade['id']} - {trade['symbol']} fechado: {close_reason} - P&L: ${pnl:,.2f} ({pnl_pct*100:+.2f}%)")
                
                # Notificar via Telegram
                if self.telegram and self.telegram.enabled:
                    self.telegram.notify_trade_closed({
                        'id': trade['id'],
                        'symbol': trade['symbol'],
                        'recommendation': trade['recommendation'],
                        'entry_price': entry_price,
                        'exit_price': current_price,
                        'pnl': pnl,
                        'pnl_percent': pnl_pct * 100,
                        'close_reason': close_reason
                    })
        
        except Exception as e:
            logger.error(f"❌ Erro em close_profitable_trades: {e}", exc_info=True)
        
        finally:
            # ⚠️ SEMPRE devolver conexão ao pool, mesmo com erro
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()  # Devolve ao pool
                except:
                    pass
    
    def clean_old_analyses(self):
        """Remove análises antigas (>2 horas)"""
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                DELETE FROM sofia_analyses
                WHERE TIMESTAMPDIFF(HOUR, created_at, NOW()) > 2
            """)
            
            deleted = cursor.rowcount
            if deleted > 0:
                logger.info(f"🧽 {deleted} análises antigas removidas (>2h)")
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ Erro ao limpar análises antigas: {e}")
    
    def save_analysis(self, analysis: Dict):
        """Salva análise no banco de dados"""
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Preparar market_data JSON com indicadores reais
            market_status = analysis.get('market_status', 'OPEN')
            market_data = json.dumps({
                'price': analysis['current_price'],
                'ema_9': analysis.get('ema_9', 0),
                'ema_21': analysis.get('ema_21', 0),
                'ema_50': analysis.get('ema_50', 0),
                'rsi': analysis.get('rsi', 0),
                'macd': analysis.get('macd', 0),
                'market_status': market_status
            })
            
            # Reasoning detalhado com status do mercado
            reasoning_parts = [f"Score: {analysis['score']} | Confiança: {analysis['confidence']}%"]
            if market_status == 'PRE_MARKET':
                reasoning_parts.append('⏰ PRÉ-MERCADO: Sinal para quando o mercado abrir')
            if analysis.get('rsi'):
                reasoning_parts.append(f"RSI: {analysis['rsi']:.1f}")
            if analysis.get('macd'):
                reasoning_parts.append(f"MACD: {analysis['macd']:.4f}")
            reasoning = ' | '.join(reasoning_parts)
            
            cursor.execute("""
                INSERT INTO sofia_analyses 
                (symbol, recommendation, confidence, market_data, reasoning, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """, (
                analysis['symbol'],
                analysis['recommendation'],
                analysis['confidence'],
                market_data,
                reasoning
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"💾 Análise salva: {analysis['symbol']} {analysis['recommendation']} (Score: {analysis['score']})")
            
        except Exception as e:
            logger.error(f"❌ Erro ao salvar análise: {e}")
    
    def analyze_and_trade(self):
        """Analisa mercado e abre trades"""
        logger.info("📊 Iniciando análise de mercado...")
        
        # Verificar quantos trades já estão abertos
        conn = self._get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM trades WHERE status = 'OPEN'")
        open_count = cursor.fetchone()[0]
        
        # Obter símbolos que já têm trade aberto
        cursor.execute("SELECT DISTINCT symbol FROM trades WHERE status = 'OPEN'")
        open_symbols = {row[0] for row in cursor.fetchall()}
        
        cursor.close()
        conn.close()
        
        logger.info(f"📊 Trades abertos: {open_count}/{self.config['max_open_trades']} | Símbolos com trade: {len(open_symbols)}")
        
        # Analisar TODOS os símbolos (mesmo sem abrir trade)
        opportunities = []
        analyses_saved = 0
        for symbol in self.symbols:
            analysis = self.analyze_symbol(symbol)
            if analysis:
                # Salvar TODAS as análises (não apenas oportunidades)
                self.save_analysis(analysis)
                analyses_saved += 1
                
                # Adicionar às oportunidades se score >= min_score E não tem trade aberto
                if analysis['score'] >= self.config['min_score'] and symbol not in open_symbols:
                    opportunities.append(analysis)
                    logger.info(f"✅ Oportunidade detectada: {symbol} {analysis['recommendation']} (Score: {analysis['score']})")
        
        logger.info(f"💾 {analyses_saved} análises salvas | {len(opportunities)} oportunidades detectadas")
        
        # Verificar se pode abrir novos trades
        if open_count >= self.config['max_open_trades']:
            logger.info(f"⚠️  Máximo de trades abertos ({open_count}/{self.config['max_open_trades']})")
            return
        
        # Ordenar por score (maior primeiro)
        opportunities.sort(key=lambda x: x['score'], reverse=True)
        
        # Abrir trades para as melhores oportunidades
        slots_available = self.config['max_open_trades'] - open_count
        logger.info(f"🔓 {slots_available} slots disponíveis para novas trades")
        
        trades_opened = 0
        for opportunity in opportunities[:slots_available]:
            success = self.open_trade(opportunity)
            if success:
                trades_opened += 1
        
        logger.info(f"✅ {trades_opened} trades abertas neste ciclo")
    
    def analyze_symbol(self, symbol: str) -> Optional[Dict]:
        """Analisa um símbolo com indicadores técnicos REAIS (sem simulações)"""
        try:
            # Verificar se mercado está aberto (para ações)
            # IMPORTANTE: sempre analisa para gerar insights, mesmo com mercado fechado
            market_open = True
            if MARKET_HOURS_AVAILABLE:
                market_open = market_hours.is_market_open(symbol)
                if not market_open:
                    logger.debug(f"{symbol}: Mercado fechado - analisando para insights pré-mercado")
            
            # Obter preço atual
            price = self.get_current_price(symbol)
            if not price or price <= 0:
                return None
            
            # Se análise técnica disponível, usar indicadores reais
            if self.technical_analyzer and self.price_fetcher:
                # Buscar histórico de preços (50 períodos)
                price_history = self.price_fetcher.get_price_history(symbol, periods=50)
                
                if price_history is None or len(price_history) < 20:
                    logger.debug(f"{symbol}: Histórico insuficiente para análise técnica ({len(price_history) if price_history else 0} períodos)")
                    return None
                
                # Analisar com indicadores reais
                analysis = self.technical_analyzer.analyze_symbol(symbol, price_history, price)
                
                if analysis is None:
                    logger.debug(f"{symbol}: Sem sinal claro de trading")
                    return None
                
                # Adicionar flag de mercado aberto/fechado no insight
                analysis['market_open'] = market_open
                if not market_open:
                    analysis['market_status'] = 'PRE_MARKET'
                    logger.info(f"📊 {symbol}: Insight pré-mercado - {analysis['recommendation']} (Score: {analysis['score']})")
                else:
                    analysis['market_status'] = 'OPEN'
                    logger.info(f"📊 {symbol}: Análise técnica REAL - {analysis['recommendation']} (Score: {analysis['score']})")
                return analysis
            
            else:
                # Fallback: sem análise técnica, não gerar oportunidades
                logger.warning(f"{symbol}: Análise técnica não disponível - pulando")
                return None
            
        except Exception as e:
            logger.error(f"Erro ao analisar {symbol}: {e}")
            return None
    
    def open_trade(self, analysis: Dict) -> bool:
        """Abre uma nova trade"""
        try:
            symbol = analysis['symbol']
            recommendation = analysis['recommendation']
            score = analysis['score']
            price = analysis['current_price']
            
            logger.info(f"🔍 Tentando abrir trade: {symbol} {recommendation} @ ${price} (Score: {score})")
            
            # Verificar se mercado está aberto (proteção adicional)
            if MARKET_HOURS_AVAILABLE:
                if not market_hours.is_market_open(symbol):
                    logger.warning(f"❌ Mercado fechado para {symbol} - trade cancelado")
                    return False
            
            # Usar ML para ajustar score
            if self.ml_predictor and self.ml_predictor.model:
                ml_proba = self.ml_predictor.predict_success_probability(analysis)
                original_score = score
                score = int(score * ml_proba * 2)  # Ajustar score baseado em ML
                logger.info(f"🤖 ML ajustou score: {original_score} → {score} (prob: {ml_proba:.2%})")
                
                # Se ML prevê probabilidade muito baixa, não abrir
                # Limiar reduzido para 25% enquanto modelo acumula dados com novas trades corretas
                if ml_proba < 0.25:
                    logger.warning(f"❌ ML prevê probabilidade muito baixa ({ml_proba:.2%}) - trade cancelado para {symbol}")
                    return False
            else:
                logger.info(f"ℹ️ ML não disponível, usando score original: {score}")
            
            # Validar preço
            if not price or price <= 0 or abs(price - 500.0) < 0.01:
                logger.error(f"❌ Preço inválido para {symbol}: ${price}")
                return False
            
            # Calcular quantidade
            capital = 1000000  # $1M
            position_size = capital * self.config['max_position_size']
            quantity = position_size / price
            
            # Salvar no banco
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO trades (
                    symbol, recommendation, entry_price, quantity,
                    confidence, status, opened_at, created_at
                ) VALUES (%s, %s, %s, %s, %s, 'OPEN', NOW(), NOW())
            """, (symbol, recommendation, price, quantity, score))
            
            trade_id = cursor.lastrowid
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Trade #{trade_id} aberto - {symbol} {recommendation} @ ${price:,.8f} (Score: {score})")
            
            # Notificar via Telegram
            if self.telegram and self.telegram.enabled:
                self.telegram.notify_trade_opened({
                    'id': trade_id,
                    'symbol': symbol,
                    'recommendation': recommendation,
                    'entry_price': price,
                    'confidence': score
                })
            
            # Notificar oportunidade excepcional (score >90%)
            if score >= 90 and self.telegram and self.telegram.enabled:
                self.telegram.notify_exceptional_opportunity(analysis)
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao abrir trade: {e}")
            return False
    
    def learn_from_results(self):
        """Aprende com resultados de trades fechados - retreina modelo ML"""
        if not self.ml_predictor:
            return
        
        try:
            # Verificar quantas trades fechadas temos
            conn = self._get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM trades WHERE status = 'CLOSED' AND pnl IS NOT NULL")
            closed_count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            # Precisa de pelo menos 20 trades fechadas para retreinar
            if closed_count < 20:
                logger.info(f"🧠 Auto-aprendizado: {closed_count} trades fechadas (mínimo: 20)")
                return
            
            logger.info(f"🎯 Iniciando auto-aprendizado com {closed_count} trades...")
            
            # Retreinar modelo
            success = self.ml_predictor.train_model()
            
            if success:
                logger.info("✅ Modelo ML retreinado com sucesso!")
                
                # Salvar histórico de treinamento
                self._save_training_history(closed_count)
                
                # Notificar via Telegram
                if self.telegram and self.telegram.enabled:
                    # Calcular acurácia aproximada
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT COUNT(*) as total,
                               SUM(CASE WHEN CAST(pnl AS DECIMAL(20,8)) > 0 THEN 1 ELSE 0 END) as wins
                        FROM trades
                        WHERE status = 'CLOSED' AND pnl IS NOT NULL
                        LIMIT %s
                    """, (closed_count,))
                    result = cursor.fetchone()
                    cursor.close()
                    
                    accuracy = (result[1] / result[0] * 100) if result[0] > 0 else 0
                    
                    self.telegram.notify_ml_retrained({
                        'trades_used': closed_count,
                        'accuracy': accuracy
                    })
            else:
                logger.warning("⚠️  Falha ao retreinar modelo ML")
            
        except Exception as e:
            logger.error(f"Erro no auto-aprendizado: {e}", exc_info=True)
    
    def _save_training_history(self, trades_count: int):
        """Salva histórico de treinamento do ML"""
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Criar tabela se não existir
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ml_training_history (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    trained_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    trades_used INT NOT NULL,
                    model_version VARCHAR(50),
                    notes TEXT
                )
            """)
            
            # Inserir registro
            cursor.execute("""
                INSERT INTO ml_training_history (trades_used, model_version, notes)
                VALUES (%s, %s, %s)
            """, (trades_count, 'RandomForest_v1', 'Auto-retreinamento diário'))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"💾 Histórico de treinamento salvo ({trades_count} trades)")
            
        except Exception as e:
            logger.error(f"Erro ao salvar histórico de treinamento: {e}")
    
    def run_daily_backup(self):
        """Executa backup diário do banco de dados"""
        try:
            logger.info("🔄 Iniciando backup diário do banco de dados...")
            import subprocess
            result = subprocess.run(
                ['/usr/bin/python3', '/home/ubuntu/arbitrage-dashboard/backup_database.py'],
                capture_output=True,
                text=True,
                timeout=600  # 10 minutos
            )
            
            if result.returncode == 0:
                logger.info("✅ Backup diário concluído com sucesso")
            else:
                logger.error(f"❌ Backup falhou: {result.stderr}")
        
        except Exception as e:
            logger.error(f"❌ Erro ao executar backup: {e}")
    
    def run_daily_backtest(self):
        """Executa backtesting diário"""
        # TODO: Implementar backtesting
        self.last_backtest = datetime.now()
        logger.info("📈 Backtesting diário executado")
    
    def _should_run_backtest(self) -> bool:
        """Verifica se deve executar backtesting"""
        if not self.last_backtest:
            return True
        
        now = datetime.now()
        if now.hour == 0 and (now - self.last_backtest).days >= 1:
            return True
        
        return False
    
    def _should_run_backup(self) -> bool:
        """Verifica se deve executar backup (3h da manhã)"""
        if not self.last_backup:
            return True
        
        now = datetime.now()
        if now.hour == 3 and (now - self.last_backup).days >= 1:
            return True
        
        return False


if __name__ == "__main__":
    daemon = IntelligentDaemon()
    daemon.run()
