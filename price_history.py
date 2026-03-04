#!/usr/bin/env python3
"""
ArbitrageAI - Histórico de Preços Real
Busca dados históricos reais de preços via APIs
SEM SIMULAÇÕES - Apenas dados reais

Fontes de dados (em ordem de prioridade):
- Crypto: OKX (primário) → CoinGecko (fallback preço) 
- Ações B3/NYSE: Yahoo Finance
"""

import logging
import requests
from typing import List, Optional
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

# Mapeamento de símbolo Binance → OKX
# OKX usa formato BTC-USDT ao invés de BTCUSDT
def binance_to_okx(symbol: str) -> str:
    """Converte símbolo Binance para formato OKX"""
    # Exemplos: BTCUSDT → BTC-USDT, ETHUSDT → ETH-USDT
    if symbol.endswith('USDT'):
        base = symbol[:-4]
        return f"{base}-USDT"
    elif symbol.endswith('BTC'):
        base = symbol[:-3]
        return f"{base}-BTC"
    elif symbol.endswith('ETH'):
        base = symbol[:-3]
        return f"{base}-ETH"
    return symbol


class PriceHistoryFetcher:
    """Busca histórico de preços real de APIs - com fallbacks robustos"""
    
    def __init__(self):
        """Inicializar fetcher"""
        self.cache = {}  # Cache simples: {symbol: (timestamp, data)}
        self.cache_duration = 300  # 5 minutos
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'ArbitrageAI/1.0'})
    
    def get_price_history_crypto(self, symbol: str, periods: int = 50) -> Optional[List[float]]:
        """
        Busca histórico de preços de criptomoedas
        Fonte primária: OKX (não bloqueada)
        
        Args:
            symbol: Símbolo no formato Binance (ex: BTCUSDT)
            periods: Número de períodos (padrão: 50)
        
        Returns:
            Lista de preços de fechamento ou None
        """
        # Verificar cache
        cache_key = f"crypto_{symbol}_{periods}"
        if cache_key in self.cache:
            timestamp, data = self.cache[cache_key]
            if time.time() - timestamp < self.cache_duration:
                return data
        
        # 1. Tentar OKX (fonte primária - não bloqueada)
        prices = self._get_okx_klines(symbol, periods)
        if prices:
            self.cache[cache_key] = (time.time(), prices)
            logger.info(f"✅ {symbol}: {len(prices)} preços históricos obtidos da OKX")
            return prices
        
        # 2. Tentar Binance (pode estar bloqueada)
        prices = self._get_binance_klines(symbol, periods)
        if prices:
            self.cache[cache_key] = (time.time(), prices)
            logger.info(f"✅ {symbol}: {len(prices)} preços históricos obtidos da Binance")
            return prices
        
        logger.warning(f"{symbol}: Não foi possível obter histórico de preços de nenhuma fonte")
        return None
    
    def _get_okx_klines(self, symbol: str, periods: int) -> Optional[List[float]]:
        """
        Busca klines (velas) da OKX
        
        Args:
            symbol: Símbolo no formato Binance (ex: BTCUSDT)
            periods: Número de períodos
        
        Returns:
            Lista de preços de fechamento ou None
        """
        try:
            okx_symbol = binance_to_okx(symbol)
            url = "https://www.okx.com/api/v5/market/candles"
            params = {
                'instId': okx_symbol,
                'bar': '1H',  # Velas de 1 hora
                'limit': str(periods)
            }
            
            response = self.session.get(url, params=params, timeout=8)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') != '0' or not data.get('data'):
                logger.debug(f"OKX sem dados para {okx_symbol}: {data.get('msg', 'sem mensagem')}")
                return None
            
            candles = data['data']
            
            if len(candles) < periods:
                logger.debug(f"OKX: dados insuficientes para {okx_symbol} ({len(candles)} < {periods})")
                return None
            
            # OKX retorna: [timestamp, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
            # Índice 4 = preço de fechamento
            # OKX retorna do mais recente para o mais antigo, precisamos inverter
            prices = [float(candle[4]) for candle in reversed(candles)]
            
            return prices
            
        except requests.exceptions.Timeout:
            logger.debug(f"OKX timeout para {symbol}")
            return None
        except requests.exceptions.RequestException as e:
            logger.debug(f"OKX erro para {symbol}: {e}")
            return None
        except Exception as e:
            logger.debug(f"OKX erro inesperado para {symbol}: {e}")
            return None
    
    def _get_binance_klines(self, symbol: str, periods: int) -> Optional[List[float]]:
        """
        Busca klines da Binance (fallback - pode estar bloqueada)
        
        Args:
            symbol: Símbolo Binance (ex: BTCUSDT)
            periods: Número de períodos
        
        Returns:
            Lista de preços de fechamento ou None
        """
        try:
            url = "https://api.binance.com/api/v3/klines"
            params = {
                'symbol': symbol,
                'interval': '1h',
                'limit': periods
            }
            
            response = self.session.get(url, params=params, timeout=8)
            
            # Verificar se foi bloqueado (451 = restrição geográfica)
            if response.status_code == 451:
                logger.debug(f"Binance bloqueada (451) para {symbol}")
                return None
            
            response.raise_for_status()
            
            klines = response.json()
            
            if not klines or len(klines) < periods:
                return None
            
            # Índice 4 = preço de fechamento
            prices = [float(kline[4]) for kline in klines]
            return prices
            
        except requests.exceptions.Timeout:
            logger.debug(f"Binance timeout para {symbol}")
            return None
        except requests.exceptions.RequestException as e:
            logger.debug(f"Binance erro para {symbol}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Binance erro inesperado para {symbol}: {e}")
            return None
    
    def get_price_history_stock(self, symbol: str, periods: int = 50) -> Optional[List[float]]:
        """
        Busca histórico de preços de ações via Yahoo Finance
        ATENÇÃO: Só deve ser chamado com mercado aberto (dados em tempo real)
        
        Args:
            symbol: Símbolo (ex: PETR4.SA, AAPL)
            periods: Número de períodos
        
        Returns:
            Lista de preços de fechamento ou None
        """
        # Verificar cache
        cache_key = f"stock_{symbol}_{periods}"
        if cache_key in self.cache:
            timestamp, data = self.cache[cache_key]
            if time.time() - timestamp < self.cache_duration:
                return data
        
        try:
            import yfinance as yf
            
            ticker = yf.Ticker(symbol)
            
            # Para ter 50 períodos de 1h, precisamos de ~15 dias (inclui fins de semana)
            days_needed = max(20, int(periods / 5) + 5)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_needed)
            
            # Buscar dados horários
            hist = ticker.history(start=start_date, end=end_date, interval='1h')
            
            # Aceitar mínimo de 20 períodos para não bloquear análise
            min_periods = min(20, periods)
            if hist.empty or len(hist) < min_periods:
                logger.warning(f"{symbol}: Dados insuficientes do Yahoo Finance ({len(hist)} < {min_periods})")
                return None
            
            # Extrair preços de fechamento (usar o que tiver, até o máximo solicitado)
            available = min(len(hist), periods)
            prices = hist['Close'].tail(available).tolist()
            
            # Cachear resultado
            self.cache[cache_key] = (time.time(), prices)
            
            logger.info(f"✅ {symbol}: {len(prices)} preços históricos obtidos do Yahoo Finance")
            return prices
            
        except ImportError:
            logger.error("yfinance não instalado. Instale com: sudo pip3 install yfinance")
            return None
        except Exception as e:
            logger.error(f"Erro ao buscar histórico de {symbol} no Yahoo Finance: {e}")
            return None
    
    def get_price_history(self, symbol: str, periods: int = 50) -> Optional[List[float]]:
        """
        Busca histórico de preços (detecta automaticamente crypto vs stock)
        
        Args:
            symbol: Símbolo do ativo
            periods: Número de períodos
        
        Returns:
            Lista de preços ou None
        """
        # Detectar tipo de ativo
        if symbol.endswith('USDT') or symbol.endswith('BTC') or symbol.endswith('ETH'):
            return self.get_price_history_crypto(symbol, periods)
        else:
            return self.get_price_history_stock(symbol, periods)
    
    def clear_cache(self):
        """Limpa cache de preços"""
        self.cache.clear()
        logger.info("Cache de preços limpo")
