#!/usr/bin/env python3
"""
Multi-Market API Integrator
Integra Binance, Interactive Brokers, e Brapi em um único sistema
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional
import aiohttp
from datetime import datetime
import websockets

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BinanceIntegrator:
    """Integrador Binance para criptomoedas"""
    
    def __init__(self):
        self.base_url = 'https://api.binance.com/api/v3'
        self.ws_url = 'wss://stream.binance.com:9443/ws'
        self.symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT']
        self.market_data = {}
        
        logger.info("✅ Binance Integrator Inicializado")
    
    async def get_prices(self) -> Dict:
        """Obter preços em tempo real"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/ticker/price"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        
                        prices = {}
                        for item in data:
                            if item['symbol'] in self.symbols:
                                prices[item['symbol']] = {
                                    'price': float(item['price']),
                                    'timestamp': datetime.now().isoformat(),
                                    'source': 'BINANCE'
                                }
                        
                        self.market_data.update(prices)
                        logger.info(f"✅ Preços Binance atualizados: {len(prices)} símbolos")
                        return prices
        except Exception as e:
            logger.error(f"❌ Erro ao buscar preços Binance: {e}")
        
        return {}
    
    async def get_24h_stats(self, symbol: str) -> Dict:
        """Obter estatísticas 24h"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/ticker/24hr?symbol={symbol}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return {
                            'symbol': symbol,
                            'price': float(data['lastPrice']),
                            'change_24h': float(data['priceChangePercent']),
                            'high_24h': float(data['highPrice']),
                            'low_24h': float(data['lowPrice']),
                            'volume': float(data['volume']),
                            'source': 'BINANCE'
                        }
        except Exception as e:
            logger.error(f"❌ Erro ao buscar stats 24h {symbol}: {e}")
        
        return {}
    
    async def get_klines(self, symbol: str, interval: str = '1h', limit: int = 100) -> List[Dict]:
        """Obter dados de candlestick"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/klines"
                params = {
                    'symbol': symbol,
                    'interval': interval,
                    'limit': limit
                }
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        
                        klines = []
                        for kline in data:
                            klines.append({
                                'timestamp': datetime.fromtimestamp(kline[0]/1000).isoformat(),
                                'open': float(kline[1]),
                                'high': float(kline[2]),
                                'low': float(kline[3]),
                                'close': float(kline[4]),
                                'volume': float(kline[7]),
                                'source': 'BINANCE'
                            })
                        
                        logger.info(f"✅ {len(klines)} klines obtidas para {symbol}")
                        return klines
        except Exception as e:
            logger.error(f"❌ Erro ao buscar klines {symbol}: {e}")
        
        return []


class BrapiIntegrator:
    """Integrador Brapi para ações brasileiras (B3)"""
    
    def __init__(self, api_token: Optional[str] = None):
        self.base_url = 'https://brapi.dev/api'
        self.api_token = api_token or 'free'  # Token gratuito
        self.symbols = ['PETR4', 'VALE3', 'ITUB4', 'BBDC4', 'ABEV3']
        self.market_data = {}
        
        logger.info("✅ Brapi Integrator Inicializado")
    
    async def get_quotes(self, symbols: Optional[List[str]] = None) -> Dict:
        """Obter cotações de ações brasileiras"""
        if symbols is None:
            symbols = self.symbols
        
        try:
            async with aiohttp.ClientSession() as session:
                quotes = {}
                
                for symbol in symbols:
                    url = f"{self.base_url}/quote/{symbol}"
                    params = {'token': self.api_token}
                    
                    try:
                        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                
                                if 'results' in data and data['results']:
                                    result = data['results'][0]
                                    quotes[symbol] = {
                                        'price': float(result.get('regularMarketPrice', 0)),
                                        'change': float(result.get('regularMarketChange', 0)),
                                        'change_pct': float(result.get('regularMarketChangePercent', 0)),
                                        'high_52w': float(result.get('fiftyTwoWeekHigh', 0)),
                                        'low_52w': float(result.get('fiftyTwoWeekLow', 0)),
                                        'volume': float(result.get('regularMarketVolume', 0)),
                                        'source': 'BRAPI'
                                    }
                    except Exception as e:
                        logger.warning(f"⚠️ Erro ao buscar {symbol}: {e}")
                
                self.market_data.update(quotes)
                logger.info(f"✅ Cotações Brapi atualizadas: {len(quotes)} símbolos")
                return quotes
        except Exception as e:
            logger.error(f"❌ Erro ao buscar cotações Brapi: {e}")
        
        return {}
    
    async def get_fundamentals(self, symbol: str) -> Dict:
        """Obter dados fundamentalistas"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/quote/{symbol}"
                params = {'token': self.api_token}
                
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        
                        if 'results' in data and data['results']:
                            result = data['results'][0]
                            return {
                                'symbol': symbol,
                                'pb_ratio': float(result.get('priceToBook', 0)),
                                'pe_ratio': float(result.get('trailingPE', 0)),
                                'dividend_yield': float(result.get('dividendYield', 0)),
                                'market_cap': float(result.get('marketCap', 0)),
                                'source': 'BRAPI'
                            }
        except Exception as e:
            logger.error(f"❌ Erro ao buscar fundamentals {symbol}: {e}")
        
        return {}


class IBKRIntegrator:
    """Integrador Interactive Brokers para ações internacionais"""
    
    def __init__(self):
        # Nota: IBKR requer TWS/Gateway rodando localmente
        # Este é um placeholder para a integração futura
        self.symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        self.market_data = {}
        
        logger.info("✅ IBKR Integrator Inicializado (Placeholder)")
        logger.warning("⚠️ IBKR requer TWS/Gateway rodando localmente")
    
    async def get_prices(self) -> Dict:
        """Obter preços (simulado)"""
        # Em produção, conectar ao IBKR Gateway via socket
        logger.info("ℹ️ IBKR: Funcionalidade requer configuração local do TWS/Gateway")
        return {}
    
    async def get_account_info(self) -> Dict:
        """Obter informações da conta"""
        logger.info("ℹ️ IBKR: Funcionalidade requer configuração local do TWS/Gateway")
        return {}


class MultiMarketIntegrator:
    """Integrador unificado de múltiplos mercados"""
    
    def __init__(self):
        self.binance = BinanceIntegrator()
        self.brapi = BrapiIntegrator()
        self.ibkr = IBKRIntegrator()
        self.all_market_data = {}
        
        logger.info("✅ Multi-Market Integrator Inicializado")
    
    async def fetch_all_markets(self) -> Dict:
        """Buscar dados de todos os mercados"""
        
        logger.info("\n📊 Buscando dados de todos os mercados...")
        
        # Buscar em paralelo
        results = await asyncio.gather(
            self.binance.get_prices(),
            self.brapi.get_quotes(),
            return_exceptions=True
        )
        
        # Consolidar resultados
        market_data = {
            'crypto': results[0] if isinstance(results[0], dict) else {},
            'stocks_br': results[1] if isinstance(results[1], dict) else {},
            'stocks_intl': {},  # IBKR requer configuração local
            'timestamp': datetime.now().isoformat()
        }
        
        self.all_market_data = market_data
        
        # Log de resumo
        total_symbols = (
            len(market_data['crypto']) + 
            len(market_data['stocks_br']) + 
            len(market_data['stocks_intl'])
        )
        
        logger.info(f"\n✅ Dados consolidados:")
        logger.info(f"   Criptomoedas: {len(market_data['crypto'])} símbolos")
        logger.info(f"   Ações BR: {len(market_data['stocks_br'])} símbolos")
        logger.info(f"   Ações Intl: {len(market_data['stocks_intl'])} símbolos")
        logger.info(f"   Total: {total_symbols} símbolos")
        
        return market_data
    
    async def get_detailed_stats(self, market: str, symbol: str) -> Dict:
        """Obter estatísticas detalhadas de um ativo"""
        
        if market == 'crypto':
            return await self.binance.get_24h_stats(symbol)
        elif market == 'stocks_br':
            return await self.brapi.get_fundamentals(symbol)
        elif market == 'stocks_intl':
            logger.warning("⚠️ IBKR requer configuração local")
            return {}
        
        return {}
    
    async def get_historical_data(self, market: str, symbol: str, interval: str = '1h') -> List[Dict]:
        """Obter dados históricos"""
        
        if market == 'crypto':
            return await self.binance.get_klines(symbol, interval)
        else:
            logger.warning(f"⚠️ Dados históricos não disponíveis para {market}")
            return []
    
    def save_market_data(self, filename: str = 'data/market_data.json'):
        """Salvar dados de mercado em arquivo"""
        import os
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w') as f:
            json.dump(self.all_market_data, f, indent=2, default=str)
        
        logger.info(f"✅ Dados de mercado salvos: {filename}")
    
    def get_market_summary(self) -> Dict:
        """Obter resumo do mercado"""
        
        summary = {
            'timestamp': datetime.now().isoformat(),
            'markets': {}
        }
        
        # Resumo Crypto
        crypto_data = self.all_market_data.get('crypto', {})
        if crypto_data:
            prices = [float(d['price']) for d in crypto_data.values()]
            summary['markets']['crypto'] = {
                'count': len(crypto_data),
                'avg_price': sum(prices) / len(prices) if prices else 0,
                'symbols': list(crypto_data.keys())
            }
        
        # Resumo Ações BR
        stocks_br = self.all_market_data.get('stocks_br', {})
        if stocks_br:
            prices = [float(d['price']) for d in stocks_br.values()]
            summary['markets']['stocks_br'] = {
                'count': len(stocks_br),
                'avg_price': sum(prices) / len(prices) if prices else 0,
                'symbols': list(stocks_br.keys())
            }
        
        return summary


async def main():
    """Executar integrador de múltiplos mercados"""
    
    logger.info("\n" + "🌍"*40)
    logger.info("MULTI-MARKET API INTEGRATOR")
    logger.info("🌍"*40)
    
    # Inicializar integrador
    integrator = MultiMarketIntegrator()
    
    # Buscar dados
    market_data = await integrator.fetch_all_markets()
    
    # Obter resumo
    logger.info("\n📊 RESUMO DO MERCADO")
    logger.info("="*60)
    
    summary = integrator.get_market_summary()
    
    for market, data in summary['markets'].items():
        logger.info(f"\n{market.upper()}")
        logger.info(f"   Símbolos: {data['count']}")
        logger.info(f"   Preço médio: ${data['avg_price']:.2f}")
        logger.info(f"   Ativos: {', '.join(data['symbols'][:5])}")
    
    # Obter dados detalhados
    logger.info("\n\n📈 DADOS DETALHADOS")
    logger.info("="*60)
    
    # Binance
    logger.info("\n🔗 BINANCE - Criptomoedas")
    btc_stats = await integrator.get_detailed_stats('crypto', 'BTCUSDT')
    if btc_stats:
        logger.info(f"   BTCUSDT: ${btc_stats['price']:.2f}")
        logger.info(f"   Mudança 24h: {btc_stats['change_24h']:.2f}%")
        logger.info(f"   Alta 24h: ${btc_stats['high_24h']:.2f}")
        logger.info(f"   Baixa 24h: ${btc_stats['low_24h']:.2f}")
    
    # Brapi
    logger.info("\n🇧🇷 BRAPI - Ações Brasileiras")
    petr_fund = await integrator.get_detailed_stats('stocks_br', 'PETR4')
    if petr_fund:
        logger.info(f"   PETR4 - P/B: {petr_fund.get('pb_ratio', 'N/A')}")
        logger.info(f"   P/E: {petr_fund.get('pe_ratio', 'N/A')}")
        logger.info(f"   Dividend Yield: {petr_fund.get('dividend_yield', 'N/A')}")
    
    # Histórico
    logger.info("\n📊 DADOS HISTÓRICOS")
    logger.info("="*60)
    
    klines = await integrator.get_historical_data('crypto', 'BTCUSDT', '1h')
    if klines:
        logger.info(f"\n🔗 BTCUSDT - Últimas 5 velas (1h)")
        for kline in klines[-5:]:
            logger.info(f"   {kline['timestamp']}: O:{kline['open']:.2f} H:{kline['high']:.2f} L:{kline['low']:.2f} C:{kline['close']:.2f}")
    
    # Salvar dados
    integrator.save_market_data()
    
    logger.info("\n✅ Multi-Market Integrator funcionando perfeitamente!")


if __name__ == "__main__":
    asyncio.run(main())
