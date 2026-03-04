#!/usr/bin/env python3
"""
ArbitrageAI - Data Aggregator
Integra dados de múltiplas fontes:
1. yfinance (open source, gratuito, confiável)
2. Alpha Vantage (técnico, fundamentals, news)
3. Finnhub (financeiro, earnings, econômico)
4. Investing.com (web scrape público)
5. Crypto: CoinGecko (100% público, sem chave)
"""

import logging
from typing import Dict, List, Optional, Tuple
import yfinance as yf
import requests
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)


class DataAggregator:
    """Agrega dados de múltiplas fontes para análise completa"""
    
    def __init__(self):
        """Inicializar agregador"""
        self.cache = {}
        self.cache_ttl = 3600  # 1 hora
        self.alpha_vantage_key = None  # Opcional
        self.finnhub_key = None  # Opcional
    
    # ============ FONTE 1: YFINANCE (Gratuito, Confiável) ============
    
    def get_ohlcv_data(self, symbol: str, period: str = '1y', interval: str = '1d') -> Optional[Dict]:
        """
        Baixa dados OHLCV (Open, High, Low, Close, Volume)
        Fonte: yfinance (100% público, sem API key)
        
        Args:
            symbol: Símbolo (ex: PETR4.SA, AAPL)
            period: Período (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            interval: Intervalo (1m, 5m, 15m, 30m, 60m, 1d, 1wk, 1mo)
        
        Returns:
            Dict com histórico OHLCV + informações
        """
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period, interval=interval)
            
            if hist.empty:
                return None
            
            # Converter para formato padrão
            return {
                'symbol': symbol,
                'data': hist.to_dict('index'),
                'timestamp': datetime.now().isoformat(),
                'rows': len(hist),
                'source': 'yfinance'
            }
        
        except Exception as e:
            logger.warning(f"Erro yfinance {symbol}: {e}")
            return None
    
    # ============ FONTE 2: ALPHA VANTAGE (Técnico + Fundamentals) ============
    
    def get_alpha_vantage_data(self, symbol: str, data_type: str = 'intraday') -> Optional[Dict]:
        """
        Dados técnicos avançados via Alpha Vantage
        - SMA, EMA, RSI, MACD, Bollinger Bands, ADX
        - Earnings, news, fundamentals
        
        Nota: Requer API key (free: 5 req/min, 500 req/dia)
        Sem chave = usa apenas yfinance
        """
        if not self.alpha_vantage_key:
            return None
        
        try:
            base_url = "https://www.alphavantage.co/query"
            
            params = {
                'function': 'SMA',  # Pode usar RSI, MACD, etc
                'symbol': symbol,
                'interval': 'daily',
                'time_period': 20,
                'apikey': self.alpha_vantage_key
            }
            
            response = requests.get(base_url, params=params, timeout=5)
            if response.status_code == 200:
                return response.json()
            
            return None
        
        except Exception as e:
            logger.warning(f"Erro Alpha Vantage {symbol}: {e}")
            return None
    
    # ============ FONTE 3: FINNHUB (Financeiro + Earnings) ============
    
    def get_finnhub_data(self, symbol: str) -> Optional[Dict]:
        """
        Dados financeiros: earnings, estimates, analyst ratings
        Finnhub: Gratuito com rate limit, sem chave necessária para quotes básicas
        """
        try:
            # Endpoint público (sem chave)
            url = f"https://finnhub.io/api/v1/quote?symbol={symbol}"
            
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                return {
                    'symbol': symbol,
                    'price': data.get('c'),
                    'high_of_day': data.get('h'),
                    'low_of_day': data.get('l'),
                    'open': data.get('o'),
                    'previous_close': data.get('pc'),
                    'change': data.get('d'),
                    'change_percent': data.get('dp'),
                    'timestamp': data.get('t'),
                    'source': 'finnhub'
                }
        
        except Exception as e:
            logger.warning(f"Erro Finnhub {symbol}: {e}")
            return None
    
    # ============ FONTE 4: COINGECKO (Criptos 100% Público) ============
    
    def get_coingecko_crypto_data(self, crypto: str = 'bitcoin') -> Optional[Dict]:
        """
        Dados de criptos: preço, volume, market cap, dominance
        CoinGecko: 100% público, sem API key, sem rate limit agressivo
        
        Args:
            crypto: Nome da crypto (bitcoin, ethereum, etc)
        
        Returns:
            Dict com dados completos
        """
        try:
            url = f"https://api.coingecko.com/api/v3/simple/price"
            
            params = {
                'ids': crypto,
                'vs_currencies': 'usd,brl',
                'include_market_cap': 'true',
                'include_24hr_vol': 'true',
                'include_24hr_change': 'true',
                'include_last_updated_at': 'true'
            }
            
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()
                return {
                    'crypto': crypto,
                    'price_usd': data.get(crypto, {}).get('usd'),
                    'price_brl': data.get(crypto, {}).get('brl'),
                    'market_cap_usd': data.get(crypto, {}).get('usd_market_cap'),
                    'volume_24h': data.get(crypto, {}).get('usd_24h_vol'),
                    'change_24h': data.get(crypto, {}).get('usd_24h_change'),
                    'timestamp': data.get(crypto, {}).get('last_updated_at'),
                    'source': 'coingecko'
                }
        
        except Exception as e:
            logger.warning(f"Erro CoinGecko {crypto}: {e}")
            return None
    
    # ============ FONTE 5: INVESTING.COM (Web Scrape Público) ============
    
    def get_investing_sentiment(self, symbol: str) -> Optional[Dict]:
        """
        Extrai sentiment e análise técnica da Investing.com
        - Fear & Greed Index
        - Analyst Ratings
        - Technical Analysis Summary
        """
        try:
            from bs4 import BeautifulSoup
            
            # Exemplo: Fear & Greed Index
            url = "https://www.investing.com/crypto/fear-and-greed-index"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Procurar pelo valor do Fear & Greed
            fg_elem = soup.find('span', {'class': 'fng-value'})
            
            sentiment = {
                'symbol': symbol,
                'fear_greed_index': fg_elem.text if fg_elem else None,
                'timestamp': datetime.now().isoformat(),
                'source': 'investing.com'
            }
            
            return sentiment
        
        except Exception as e:
            logger.warning(f"Erro Investing.com {symbol}: {e}")
            return None
    
    # ============ AGREGAÇÃO FINAL ============
    
    def aggregate_all_sources(self, symbol: str) -> Dict:
        """
        Coleta dados de TODAS as fontes disponíveis
        Retorna análise consolidada
        """
        
        aggregated = {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'sources': {}
        }
        
        # Fonte 1: yfinance (SEMPRE)
        yf_data = self.get_ohlcv_data(symbol)
        if yf_data:
            aggregated['sources']['yfinance'] = yf_data
        
        # Fonte 2: Alpha Vantage (se houver chave)
        av_data = self.get_alpha_vantage_data(symbol)
        if av_data:
            aggregated['sources']['alpha_vantage'] = av_data
        
        # Fonte 3: Finnhub
        fh_data = self.get_finnhub_data(symbol)
        if fh_data:
            aggregated['sources']['finnhub'] = fh_data
        
        # Fonte 4: CoinGecko (se cripto)
        if any(x in symbol.lower() for x in ['btc', 'eth', 'bnb']):
            cg_data = self.get_coingecko_crypto_data('bitcoin')
            if cg_data:
                aggregated['sources']['coingecko'] = cg_data
        
        # Fonte 5: Investing.com Sentiment
        inv_data = self.get_investing_sentiment(symbol)
        if inv_data:
            aggregated['sources']['investing'] = inv_data
        
        return aggregated
    
    def consolidate_signals(self, symbol: str, aggregated_data: Dict) -> Dict:
        """
        Consolida sinais de múltiplas fontes
        Usa votação: qual fonte concorda mais?
        
        Returns:
            Score consolidado (0-100) baseado em consenso
        """
        
        buy_votes = 0
        sell_votes = 0
        neutral_votes = 0
        
        # Processar cada fonte
        for source, data in aggregated_data['sources'].items():
            if not data:
                continue
            
            # Lógica simples: se preço está em alta = BUY
            # (em produção: análise mais sofisticada)
            if source == 'yfinance':
                # Verificar momentum básico
                if 'data' in data:
                    prices = list(data['data'].values())
                    if len(prices) > 0:
                        if prices[-1]['Close'] > prices[-5]['Close']:
                            buy_votes += 1
                        else:
                            sell_votes += 1
            
            elif source == 'finnhub':
                if data.get('change_percent', 0) > 0:
                    buy_votes += 1
                else:
                    sell_votes += 1
            
            elif source == 'coingecko':
                if data.get('change_24h', 0) > 0:
                    buy_votes += 1
                else:
                    sell_votes += 1
        
        # Calcular score consolidado
        total_votes = buy_votes + sell_votes + neutral_votes
        if total_votes == 0:
            consensus_score = 50
        else:
            consensus_score = (buy_votes / total_votes) * 100
        
        return {
            'symbol': symbol,
            'consensus_score': consensus_score,
            'buy_votes': buy_votes,
            'sell_votes': sell_votes,
            'total_sources': len(aggregated_data['sources']),
            'timestamp': datetime.now().isoformat()
        }


if __name__ == '__main__':
    agg = DataAggregator()
    
    # Teste
    print("=== AGREGADOR DE DADOS ===\n")
    
    symbols = ['PETR4.SA', 'VALE3.SA']
    
    for symbol in symbols:
        print(f"\n{symbol}:")
        
        # Agregar dados
        all_data = agg.aggregate_all_sources(symbol)
        print(f"  Fontes carregadas: {len(all_data['sources'])}")
        
        # Consolidar sinais
        consensus = agg.consolidate_signals(symbol, all_data)
        print(f"  Score consenso: {consensus['consensus_score']:.1f}/100")
        print(f"  Votos: {consensus['buy_votes']} BUY, {consensus['sell_votes']} SELL")
