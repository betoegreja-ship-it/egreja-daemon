"""
Simple Market API - Versão síncrona para Sofia IA
Busca dados de mercado em tempo real de forma simples
"""

import requests
from typing import Dict
from datetime import datetime

class SimpleMarketAPI:
    def __init__(self):
        self.binance_base = "https://api.binance.com/api/v3"
        # Criptomoedas principais
        self.crypto_symbols = [
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT",
            "LTCUSDT", "DOGEUSDT", "MATICUSDT", "SOLUSDT", "DOTUSDT",
            "AVAXUSDT", "LINKUSDT", "ATOMUSDT", "UNIUSDT", "FILUSDT"
        ]
        # Metais (via Binance - pares USDT)
        self.metal_symbols = [
            "PAXGUSDT",  # Ouro (PAX Gold)
        ]
    
    def get_crypto_data(self) -> Dict:
        """Buscar dados de criptomoedas da Binance (otimizado)"""
        crypto_data = {}
        
        for symbol in self.crypto_symbols:
            try:
                # Apenas ticker 24h (contém preço atual + stats)
                ticker_url = f"{self.binance_base}/ticker/24hr?symbol={symbol}"
                response = requests.get(ticker_url, timeout=3)
                data = response.json()
                
                crypto_data[symbol] = {
                    "price": float(data["lastPrice"]),
                    "high24h": float(data["highPrice"]),
                    "low24h": float(data["lowPrice"]),
                    "change24h": float(data["priceChangePercent"]),
                    "priceChange24h": float(data["priceChange"]),
                    "priceChangePercent24h": float(data["priceChangePercent"]),
                    "timestamp": datetime.now().isoformat()
                }
                
            except Exception as e:
                print(f"⚠️ Erro ao buscar {symbol}: {e}")
                continue
        
        return crypto_data
    

    def get_metal_data(self) -> Dict:
        """Buscar dados de metais via Binance (otimizado)"""
        metal_data = {}
        
        for symbol in self.metal_symbols:
            try:
                # Apenas ticker 24h
                ticker_url = f"{self.binance_base}/ticker/24hr?symbol={symbol}"
                response = requests.get(ticker_url, timeout=3)
                data = response.json()
                
                # Mapear para nomes amigáveis
                friendly_name = "GOLD" if symbol == "PAXGUSDT" else symbol
                
                metal_data[friendly_name] = {
                    "price": float(data["lastPrice"]),
                    "high24h": float(data["highPrice"]),
                    "low24h": float(data["lowPrice"]),
                    "change24h": float(data["priceChangePercent"]),
                    "priceChange24h": float(data["priceChange"]),
                    "priceChangePercent24h": float(data["priceChangePercent"]),
                    "timestamp": datetime.now().isoformat()
                }
                
            except Exception as e:
                print(f"⚠️ Erro ao buscar {symbol}: {e}")
                continue
        
        return metal_data
    
    def get_market_data(self) -> Dict:
        """Buscar todos os dados de mercado (crypto + metals)"""
        market_data = {}
        
        # Buscar criptomoedas
        crypto = self.get_crypto_data()
        market_data.update(crypto)
        
        # Buscar metais
        metals = self.get_metal_data()
        market_data.update(metals)
        
        return market_data


if __name__ == "__main__":
    api = SimpleMarketAPI()
    data = api.get_market_data()
    
    print(f"\n✅ Dados de {len(data)} símbolos obtidos:")
    for symbol, info in data.items():
        print(f"  {symbol}: ${info['price']:.2f} ({info['priceChangePercent24h']:+.2f}%)")
