#!/usr/bin/env python3.11
"""Teste rápido da API expandida"""

from simple_market_api import SimpleMarketAPI

api = SimpleMarketAPI()

print("\n🔍 Testando Criptomoedas...")
crypto = api.get_crypto_data()
print(f"✅ {len(crypto)} criptomoedas obtidas")
for symbol in list(crypto.keys())[:3]:
    print(f"  {symbol}: ${crypto[symbol]['price']:.2f}")

print("\n🔍 Testando Metais...")
metals = api.get_metal_data()
print(f"✅ {len(metals)} metais obtidos")
for symbol in metals.keys():
    print(f"  {symbol}: ${metals[symbol]['price']:.2f}")

print("\n✅ Teste completo!")
