"""
Módulo unificado para buscar dados de mercado de múltiplas fontes:
- Criptomoedas: Binance API
- Ações B3: Yahoo Finance (.SA suffix)
- Ações NYSE: Yahoo Finance
"""

import requests
import yfinance as yf
from datetime import datetime, time
from typing import Dict, Optional, List
import pytz
import os

# BTG Connector (opcional - requer token)
try:
    from btg_connector import BTGConnector, get_btg_price
    BTG_ENABLED = bool(os.getenv('BTG_TKNWF'))
    if BTG_ENABLED:
        btg_connector = BTGConnector()
        btg_connector.connect()
        print("[MARKET_DATA] BTG Connector ativado!")
    else:
        btg_connector = None
        print("[MARKET_DATA] BTG Connector desativado (token não configurado)")
except Exception as e:
    BTG_ENABLED = False
    btg_connector = None
    print(f"[MARKET_DATA] BTG Connector não disponível: {e}")

# Cache de preços (5 minutos)
price_cache = {}
cache_ttl = 300  # 5 minutos

# Símbolos de criptomoedas
CRYPTO_SYMBOLS = [
    'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT',
    'XRPUSDT', 'DOGEUSDT', 'DOTUSDT', 'MATICUSDT', 'LTCUSDT',
    'AVAXUSDT', 'LINKUSDT', 'ATOMUSDT', 'UNIUSDT', 'FILUSDT',
    'SHIBUSDT', 'PEPEUSDT', 'NEARUSDT', 'ALGOUSDT', 'VETUSDT'
]

# Ações B3 (Brasil) - 20 Principais da Watchlist do Beto
B3_STOCKS = [
    'EQTL3.SA',  # Equatorial ON
    'ITUB4.SA',  # Itaú Unibanco
    'EMBJ3.SA',  # Embraer
    'WEGE3.SA',  # WEG
    'BPAC11.SA', # BTG Pactual
    'BBDC4.SA',  # Bradesco
    'PETR4.SA',  # Petrobras
    'VALE3.SA',  # Vale
    'BRAP4.SA',  # Bradespar
    'NATU3.SA',  # Natura
    'JBSS32.SA', # JBS NV BDR
    'BBAS3.SA',  # Banco do Brasil
    'PRIO3.SA',  # Prio (HRT)
    'POMO4.SA',  # Marcopolo
    'CPFE3.SA',  # CPFL Energia
    'HBRE3.SA',  # HBR Realty
    'VTRU3.SA',  # Vitru
    'ANIM3.SA',  # Anima
    'ALPA4.SA',  # Alpargatas
    'RENT3.SA'   # Localiza
]

# Ações NYSE (EUA) - 20 Principais + BDRs monitorados pelo Beto
NYSE_STOCKS = [
    'AAPL',      # Apple
    'MSFT',      # Microsoft
    'GOOGL',     # Google
    'AMZN',      # Amazon
    'TSLA',      # Tesla
    'NVDA',      # Nvidia
    'META',      # Meta
    'NFLX',      # Netflix
    'AMD',       # AMD
    'BABA',      # Alibaba
    'JPM',       # JPMorgan
    'GS',        # Goldman Sachs
    'C',         # Citigroup
    'BLK',       # BlackRock
    'SCHW',      # Charles Schwab
    'V',         # Visa
    'MA',        # Mastercard
    'JNJ',       # Johnson & Johnson
    'UNH',       # UnitedHealth
    'ABBV'       # AbbVie
]

def is_market_open(symbol: str) -> bool:
    """Verifica se o mercado está aberto para o símbolo"""
    now = datetime.now(pytz.UTC)
    
    # Criptomoedas: 24/7
    if symbol in CRYPTO_SYMBOLS:
        return True
    
    # Ações B3: 10h-17h BRT (seg-sex)
    if symbol in B3_STOCKS:
        brt = pytz.timezone('America/Sao_Paulo')
        now_brt = now.astimezone(brt)
        if now_brt.weekday() >= 5:  # Sábado ou domingo
            return False
        market_open = time(10, 0)
        market_close = time(17, 0)
        return market_open <= now_brt.time() <= market_close
    
    # Ações NYSE: 9h30-16h EST (seg-sex)
    if symbol in NYSE_STOCKS:
        est = pytz.timezone('America/New_York')
        now_est = now.astimezone(est)
        if now_est.weekday() >= 5:  # Sábado ou domingo
            return False
        market_open = time(9, 30)
        market_close = time(16, 0)
        return market_open <= now_est.time() <= market_close
    
    return False

def binance_to_okx(symbol: str) -> str:
    """Converte símbolo Binance para formato OKX"""
    if symbol.endswith('USDT'):
        return f"{symbol[:-4]}-USDT"
    elif symbol.endswith('BTC'):
        return f"{symbol[:-3]}-BTC"
    elif symbol.endswith('ETH'):
        return f"{symbol[:-3]}-ETH"
    return symbol

def get_okx_price(symbol: str) -> Optional[float]:
    """Busca preço atual da OKX (não bloqueada)"""
    try:
        okx_symbol = binance_to_okx(symbol)
        url = f"https://www.okx.com/api/v5/market/ticker?instId={okx_symbol}"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get('code') == '0' and data.get('data'):
                return float(data['data'][0]['last'])
    except Exception as e:
        print(f"Erro ao buscar preço OKX para {symbol}: {e}")
    return None

def get_binance_price(symbol: str) -> Optional[float]:
    """Busca preço atual da Binance (fallback - pode estar bloqueada)"""
    try:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            return float(data['price'])
        elif response.status_code == 451:
            # Bloqueado por restrição geográfica
            return None
    except Exception as e:
        print(f"Erro ao buscar preço Binance para {symbol}: {e}")
    return None

def get_btg_price_wrapper(symbol: str) -> Optional[float]:
    """Busca preço atual do BTG (apenas ações B3)"""
    if not BTG_ENABLED or not btg_connector:
        return None
    
    try:
        # Remover .SA do símbolo para BTG
        btg_symbol = symbol.replace('.SA', '')
        return get_btg_price(btg_symbol, btg_connector)
    except Exception as e:
        print(f"Erro ao buscar preço BTG para {symbol}: {e}")
        return None

def get_yahoo_price(symbol: str) -> Optional[float]:
    """Busca preço atual do Yahoo Finance"""
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period='1d', interval='1m')
        if not data.empty:
            return float(data['Close'].iloc[-1])
    except Exception as e:
        print(f"Erro ao buscar preço Yahoo para {symbol}: {e}")
    return None

def get_current_price(symbol: str) -> Optional[float]:
    """
    Busca preço atual do símbolo (Binance ou Yahoo Finance)
    Usa cache de 5 minutos para evitar rate limit
    """
    # Verificar cache
    cache_key = symbol
    if cache_key in price_cache:
        cached_price, cached_time = price_cache[cache_key]
        if (datetime.now().timestamp() - cached_time) < cache_ttl:
            return cached_price
    
    price = None
    
    # Criptomoedas: OKX (primário) → Binance (fallback)
    if symbol in CRYPTO_SYMBOLS:
        price = get_okx_price(symbol)
        if not price:
            price = get_binance_price(symbol)
    
    # Ações B3: Tentar BTG primeiro, fallback para Yahoo
    elif symbol in B3_STOCKS:
        if BTG_ENABLED:
            price = get_btg_price_wrapper(symbol)
        if not price:  # Fallback para Yahoo
            price = get_yahoo_price(symbol)
    
    # Ações NYSE: Yahoo Finance
    elif symbol in NYSE_STOCKS:
        price = get_yahoo_price(symbol)
    
    # Atualizar cache
    if price and price > 0:
        price_cache[cache_key] = (price, datetime.now().timestamp())
    
    return price

def get_all_symbols() -> List[str]:
    """Retorna lista de todos os símbolos disponíveis"""
    return CRYPTO_SYMBOLS + B3_STOCKS + NYSE_STOCKS

def get_symbol_type(symbol: str) -> str:
    """Retorna tipo do símbolo: crypto, b3, nyse"""
    if symbol in CRYPTO_SYMBOLS:
        return 'crypto'
    elif symbol in B3_STOCKS:
        return 'b3'
    elif symbol in NYSE_STOCKS:
        return 'nyse'
    return 'unknown'

def get_market_data(symbols: List[str] = None) -> Dict[str, Dict]:
    """
    Busca dados de mercado para múltiplos símbolos
    Retorna: {symbol: {price, type, market_open}}
    """
    if symbols is None:
        symbols = get_all_symbols()
    
    result = {}
    for symbol in symbols:
        price = get_current_price(symbol)
        if price:
            result[symbol] = {
                'symbol': symbol,
                'price': price,
                'type': get_symbol_type(symbol),
                'market_open': is_market_open(symbol),
                'timestamp': datetime.now().isoformat()
            }
    
    return result

if __name__ == '__main__':
    # Teste
    print("Testando integração de mercados...")
    
    # Teste criptomoedas
    print("\n=== CRIPTOMOEDAS ===")
    btc_price = get_current_price('BTCUSDT')
    print(f"BTC: ${btc_price:,.2f}" if btc_price else "BTC: Erro")
    
    # Teste ações B3
    print("\n=== AÇÕES B3 ===")
    petr4_price = get_current_price('PETR4.SA')
    print(f"PETR4: R${petr4_price:,.2f}" if petr4_price else "PETR4: Erro")
    print(f"Mercado B3 aberto: {is_market_open('PETR4.SA')}")
    
    # Teste ações NYSE
    print("\n=== AÇÕES NYSE ===")
    aapl_price = get_current_price('AAPL')
    print(f"AAPL: ${aapl_price:,.2f}" if aapl_price else "AAPL: Erro")
    print(f"Mercado NYSE aberto: {is_market_open('AAPL')}")
    
    # Teste todos os mercados
    print("\n=== TODOS OS MERCADOS ===")
    all_data = get_market_data()
    print(f"Total de ativos: {len(all_data)}")
    for symbol, data in list(all_data.items())[:5]:
        print(f"{symbol}: ${data['price']:,.2f} ({data['type']}) - Aberto: {data['market_open']}")
