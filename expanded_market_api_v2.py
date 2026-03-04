"""
Expanded Market API V2 - Otimizado com Cache
Busca dados de múltiplos mercados com cache para evitar timeouts
"""

import requests
import json
from typing import Dict
from datetime import datetime, timedelta
from pathlib import Path

class ExpandedMarketAPIV2:
    def __init__(self):
        # Binance para cripto
        self.binance_base = "https://api.binance.com/api/v3"
        
        # Finnhub para ações (API gratuita, mais rápida que Yahoo)
        self.finnhub_key = "demo"  # Usar API key real em produção
        self.finnhub_base = "https://finnhub.io/api/v1"
        
        # Cache
        self.cache_dir = Path("/home/ubuntu/arbitrage-dashboard/cache")
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_ttl = 300  # 5 minutos
        
        # Símbolos
        self.crypto_symbols = [
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT",
            "LTCUSDT", "DOGEUSDT", "MATICUSDT", "SOLUSDT", "DOTUSDT"
        ]
        
        self.stock_symbols = [
            "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
            "NVDA", "META", "NFLX", "AMD", "BABA"
        ]
        
        # Metais via API pública
        self.metal_symbols = {
            "XAU": "XAU/USD",  # Ouro
            "XAG": "XAG/USD",  # Prata
        }
    
    def _get_cache(self, key: str) -> Dict:
        """Obter dados do cache se válidos"""
        cache_file = self.cache_dir / f"{key}.json"
        
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    cached = json.load(f)
                
                cached_time = datetime.fromisoformat(cached.get("cached_at", ""))
                if datetime.now() - cached_time < timedelta(seconds=self.cache_ttl):
                    return cached.get("data", {})
            except:
                pass
        
        return {}
    
    def _set_cache(self, key: str, data: Dict):
        """Salvar dados no cache"""
        cache_file = self.cache_dir / f"{key}.json"
        
        try:
            with open(cache_file, 'w') as f:
                json.dump({
                    "cached_at": datetime.now().isoformat(),
                    "data": data
                }, f)
        except:
            pass
    
    def get_crypto_data(self) -> Dict:
        """Buscar dados de criptomoedas da Binance"""
        cached = self._get_cache("crypto")
        if cached:
            print("  📦 Usando cache de criptomoedas")
            return cached
        
        crypto_data = {}
        
        for symbol in self.crypto_symbols:
            try:
                # Ticker 24h
                ticker_url = f"{self.binance_base}/ticker/24hr?symbol={symbol}"
                ticker_response = requests.get(ticker_url, timeout=5)
                ticker_data = ticker_response.json()
                
                # Preço atual
                price_url = f"{self.binance_base}/ticker/price?symbol={symbol}"
                price_response = requests.get(price_url, timeout=5)
                price_data = price_response.json()
                
                crypto_data[symbol] = {
                    "price": float(price_data["price"]),
                    "high24h": float(ticker_data["highPrice"]),
                    "low24h": float(ticker_data["lowPrice"]),
                    "volume24h": float(ticker_data["volume"]),
                    "change24h": float(ticker_data["priceChangePercent"]),
                    "market": "CRYPTO",
                    "timestamp": datetime.now().isoformat()
                }
                
            except Exception as e:
                print(f"⚠️ Erro ao buscar {symbol}: {e}")
                continue
        
        self._set_cache("crypto", crypto_data)
        return crypto_data
    
    def get_stock_data(self) -> Dict:
        """Buscar dados de ações via Finnhub (mais rápido)"""
        cached = self._get_cache("stocks")
        if cached:
            print("  📦 Usando cache de ações")
            return cached
        
        stock_data = {}
        
        # Nota: Finnhub free tier permite 60 requests/min
        # Muito mais rápido que Yahoo Finance
        
        for symbol in self.stock_symbols:
            try:
                # Quote atual
                quote_url = f"{self.finnhub_base}/quote?symbol={symbol}&token={self.finnhub_key}"
                response = requests.get(quote_url, timeout=5)
                data = response.json()
                
                if "c" in data:  # current price
                    price = float(data["c"])
                    high = float(data["h"])
                    low = float(data["l"])
                    prev_close = float(data["pc"])
                    change_percent = ((price - prev_close) / prev_close * 100) if prev_close else 0
                    
                    stock_data[symbol] = {
                        "price": price,
                        "high24h": high,
                        "low24h": low,
                        "volume24h": 0,  # Finnhub não retorna volume no quote
                        "change24h": change_percent,
                        "market": "STOCK",
                        "timestamp": datetime.now().isoformat()
                    }
                
            except Exception as e:
                print(f"⚠️ Erro ao buscar {symbol}: {e}")
                continue
        
        self._set_cache("stocks", stock_data)
        return stock_data
    
    def get_metal_data(self) -> Dict:
        """Buscar dados de metais preciosos"""
        cached = self._get_cache("metals")
        if cached:
            print("  📦 Usando cache de metais")
            return cached
        
        metal_data = {}
        
        # Usar metals-api.com (free tier: 50 requests/month)
        # Por enquanto, usar dados simulados baseados em preços reais
        # Em produção, integrar com API real
        
        metal_data["XAU/USD"] = {
            "price": 2650.00,  # Preço aproximado do ouro (atualizar com API real)
            "high24h": 2655.00,
            "low24h": 2645.00,
            "volume24h": 0,
            "change24h": 0.5,
            "market": "COMMODITY",
            "timestamp": datetime.now().isoformat()
        }
        
        metal_data["XAG/USD"] = {
            "price": 30.50,  # Preço aproximado da prata (atualizar com API real)
            "high24h": 30.70,
            "low24h": 30.30,
            "volume24h": 0,
            "change24h": 0.8,
            "market": "COMMODITY",
            "timestamp": datetime.now().isoformat()
        }
        
        self._set_cache("metals", metal_data)
        return metal_data
    
    def get_all_market_data(self) -> Dict:
        """Buscar dados de todos os mercados com cache"""
        print("\n📊 Buscando dados de múltiplos mercados (com cache)...")
        
        all_data = {}
        
        # Criptomoedas
        print("  🪙 Criptomoedas (Binance)...")
        crypto_data = self.get_crypto_data()
        all_data.update(crypto_data)
        print(f"     ✅ {len(crypto_data)} criptomoedas")
        
        # Ações
        print("  📈 Ações (Finnhub)...")
        stock_data = self.get_stock_data()
        all_data.update(stock_data)
        print(f"     ✅ {len(stock_data)} ações")
        
        # Metais
        print("  🥇 Metais Preciosos...")
        metal_data = self.get_metal_data()
        all_data.update(metal_data)
        print(f"     ✅ {len(metal_data)} metais")
        
        print(f"\n✅ Total: {len(all_data)} ativos\n")
        
        return all_data


if __name__ == "__main__":
    api = ExpandedMarketAPIV2()
    data = api.get_all_market_data()
    
    print("\n📊 RESUMO POR MERCADO:\n")
    
    crypto_count = sum(1 for v in data.values() if v.get("market") == "CRYPTO")
    stock_count = sum(1 for v in data.values() if v.get("market") == "STOCK")
    commodity_count = sum(1 for v in data.values() if v.get("market") == "COMMODITY")
    
    print(f"🪙 Criptomoedas: {crypto_count}")
    print(f"📈 Ações: {stock_count}")
    print(f"🥇 Commodities: {commodity_count}")
    print(f"📊 Total: {len(data)}")
    
    print("\n💰 Alguns Preços:\n")
    for symbol, info in list(data.items())[:15]:
        market_emoji = {"CRYPTO": "🪙", "STOCK": "📈", "COMMODITY": "🥇"}.get(info.get("market"), "📊")
        print(f"  {market_emoji} {symbol}: ${info['price']:.2f} ({info['change24h']:+.2f}%)")
