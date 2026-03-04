#!/usr/bin/env python3.11
import requests

symbol = "BTCUSDT"
base = "https://api.binance.com/api/v3"

print(f"Testando {symbol}...")
price_url = f"{base}/ticker/price?symbol={symbol}"
print(f"URL: {price_url}")

response = requests.get(price_url, timeout=5)
print(f"Status: {response.status_code}")
print(f"Data: {response.json()}")
