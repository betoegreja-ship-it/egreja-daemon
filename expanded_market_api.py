"""
Expanded Market API - Criptomoedas, Ações, Ouro e Prata
Busca dados de múltiplos mercados em tempo real via Yahoo Finance
"""

import requests
from typing import Dict
from datetime import datetime

class ExpandedMarketAPI:
    def __init__(self):
        # Binance para cripto
        self.binance_base = "https://api.binance.com/api/v3"
        
        # Yahoo Finance para ações e metais
        self.yahoo_base = "https://query1.finance.yahoo.com/v8/finance/chart"
        
        # Símbolos
        self.crypto_symbols = [
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT",
            "LTCUSDT", "DOGEUSDT", "MATICUSDT", "SOLUSDT", "DOTUSDT"
        ]
        
        self.stock_symbols = [
            "AAPL",   # Apple
            "MSFT",   # Microsoft
            "GOOGL",  # Google
            "AMZN",   # Amazon
            "TSLA",   # Tesla
            "NVDA",   # Nvidia
            "META",   # Meta/Facebook
            "NFLX",   # Netflix
            "AMD",    # AMD
            "BABA",   # Alibaba
        ]
        
        self.metal_symbols = {
            "GC=F": "XAU/USD",  # Ouro Futures
            "SI=F": "XAG/USD",  # Prata Futures
        }
    
    def get_crypto_data(self) -> Dict:
        """Buscar dados de criptomoedas da Binance"""
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
        
        return crypto_data
    
    def get_yahoo_quote(self, symbol: str) -> Dict:
        """Buscar cotação do Yahoo Finance"""
        try:
            url = f"{self.yahoo_base}/{symbol}?interval=1d&range=1d"
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=10)
            data = response.json()
            
            if "chart" in data and "result" in data["chart"] and data["chart"]["result"]:
                result = data["chart"]["result"][0]
                meta = result.get("meta", {})
                
                price = meta.get("regularMarketPrice", 0)
                prev_close = meta.get("previousClose", price)
                change_percent = ((price - prev_close) / prev_close * 100) if prev_close else 0
                
                # Pegar high/low do dia
                quotes = result.get("indicators", {}).get("quote", [{}])[0]
                high = quotes.get("high", [price])[0] if quotes.get("high") else price
                low = quotes.get("low", [price])[0] if quotes.get("low") else price
                volume = quotes.get("volume", [0])[0] if quotes.get("volume") else 0
                
                return {
                    "price": float(price),
                    "high24h": float(high) if high else float(price),
                    "low24h": float(low) if low else float(price),
                    "volume24h": float(volume) if volume else 0,
                    "change24h": float(change_percent),
                }
            
            return None
            
        except Exception as e:
            print(f"⚠️ Erro ao buscar {symbol}: {e}")
            return None
    
    def get_stock_data(self) -> Dict:
        """Buscar dados de ações via Yahoo Finance"""
        stock_data = {}
        
        for symbol in self.stock_symbols:
            quote = self.get_yahoo_quote(symbol)
            if quote:
                quote["market"] = "STOCK"
                quote["timestamp"] = datetime.now().isoformat()
                stock_data[symbol] = quote
        
        return stock_data
    
    def get_metal_data(self) -> Dict:
        """Buscar dados de metais (ouro e prata) via Yahoo Finance"""
        metal_data = {}
        
        for yahoo_symbol, display_symbol in self.metal_symbols.items():
            quote = self.get_yahoo_quote(yahoo_symbol)
            if quote:
                quote["market"] = "COMMODITY"
                quote["timestamp"] = datetime.now().isoformat()
                metal_data[display_symbol] = quote
        
        return metal_data
    
    def get_all_market_data(self) -> Dict:
        """Buscar dados de todos os mercados"""
        print("\n📊 Buscando dados de múltiplos mercados...")
        
        all_data = {}
        
        # Criptomoedas
        print("  🪙 Criptomoedas (Binance)...")
        crypto_data = self.get_crypto_data()
        all_data.update(crypto_data)
        print(f"     ✅ {len(crypto_data)} criptomoedas")
        
        # Ações
        print("  📈 Ações (Yahoo Finance)...")
        stock_data = self.get_stock_data()
        all_data.update(stock_data)
        print(f"     ✅ {len(stock_data)} ações")
        
        # Metais
        print("  🥇 Metais Preciosos (Yahoo Finance)...")
        metal_data = self.get_metal_data()
        all_data.update(metal_data)
        print(f"     ✅ {len(metal_data)} metais")
        
        print(f"\n✅ Total: {len(all_data)} ativos\n")
        
        return all_data


if __name__ == "__main__":
    api = ExpandedMarketAPI()
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
