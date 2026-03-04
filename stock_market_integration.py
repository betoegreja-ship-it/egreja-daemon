#!/usr/bin/env python3
"""
Stock Market Integration - Real-time stock quotes and market hours
Integrates with Yahoo Finance API via Manus Data API Hub
"""

import sys
sys.path.append('/opt/.manus/.sandbox-runtime')
from data_api import ApiClient
from datetime import datetime, time
import pytz
import json

class StockMarketIntegration:
    def __init__(self):
        self.client = ApiClient()
        
        # Ações brasileiras (B3)
        self.brazilian_stocks = [
            'PETR4.SA',  # Petrobras
            'VALE3.SA',  # Vale
            'ITUB4.SA',  # Itaú
            'BBDC4.SA',  # Bradesco
            'ABEV3.SA',  # Ambev
            'WEGE3.SA',  # WEG
            'RENT3.SA',  # Localiza
            'MGLU3.SA',  # Magazine Luiza
            'SUZB3.SA',  # Suzano
            'ELET3.SA',  # Eletrobras
        ]
        
        # Ações americanas (NYSE/NASDAQ)
        self.us_stocks = [
            'AAPL',   # Apple
            'MSFT',   # Microsoft
            'GOOGL',  # Google
            'AMZN',   # Amazon
            'TSLA',   # Tesla
            'NVDA',   # Nvidia
            'META',   # Meta
            'NFLX',   # Netflix
            'AMD',    # AMD
            'BABA',   # Alibaba
        ]
        
        # Timezones
        self.nyse_tz = pytz.timezone('America/New_York')
        self.b3_tz = pytz.timezone('America/Sao_Paulo')
    
    def is_nyse_open(self):
        """Check if NYSE is currently open (9:30 AM - 4:00 PM EST, Mon-Fri)"""
        now_ny = datetime.now(self.nyse_tz)
        
        # Check if weekend
        if now_ny.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        
        # Check if within trading hours
        market_open = time(9, 30)
        market_close = time(16, 0)
        current_time = now_ny.time()
        
        return market_open <= current_time <= market_close
    
    def is_b3_open(self):
        """Check if B3 is currently open (10:00 AM - 5:00 PM BRT, Mon-Fri)"""
        now_sp = datetime.now(self.b3_tz)
        
        # Check if weekend
        if now_sp.weekday() >= 5:
            return False
        
        # Check if within trading hours
        market_open = time(10, 0)
        market_close = time(17, 0)
        current_time = now_sp.time()
        
        return market_open <= current_time <= market_close
    
    def get_stock_quote(self, symbol, region='US'):
        """Get real-time stock quote from Yahoo Finance"""
        try:
            response = self.client.call_api('YahooFinance/get_stock_chart', query={
                'symbol': symbol,
                'region': region,
                'interval': '1m',  # 1-minute intervals for real-time
                'range': '1d',     # Today's data
                'includeAdjustedClose': True
            })
            
            if response and 'chart' in response and 'result' in response['chart']:
                result = response['chart']['result'][0]
                meta = result['meta']
                
                # Get latest price data
                timestamps = result['timestamp']
                quotes = result['indicators']['quote'][0]
                
                if timestamps and len(timestamps) > 0:
                    latest_idx = len(timestamps) - 1
                    
                    return {
                        'symbol': meta['symbol'],
                        'name': meta.get('longName', meta['symbol']),
                        'price': meta['regularMarketPrice'],
                        'open': quotes['open'][latest_idx] if quotes['open'][latest_idx] else meta['regularMarketPrice'],
                        'high': meta['regularMarketDayHigh'],
                        'low': meta['regularMarketDayLow'],
                        'volume': meta['regularMarketVolume'],
                        'change': meta['regularMarketPrice'] - meta['previousClose'],
                        'change_percent': ((meta['regularMarketPrice'] - meta['previousClose']) / meta['previousClose']) * 100,
                        'currency': meta['currency'],
                        'exchange': meta['exchangeName'],
                        'timestamp': datetime.now().isoformat()
                    }
            
            return None
            
        except Exception as e:
            print(f"Error fetching quote for {symbol}: {e}")
            return None
    
    def get_all_stocks(self):
        """Get quotes for all configured stocks"""
        stocks = []
        
        # Get US stocks
        for symbol in self.us_stocks:
            quote = self.get_stock_quote(symbol, 'US')
            if quote:
                quote['market'] = 'US'
                stocks.append(quote)
        
        # Get Brazilian stocks
        for symbol in self.brazilian_stocks:
            quote = self.get_stock_quote(symbol, 'BR')
            if quote:
                quote['market'] = 'BR'
                stocks.append(quote)
        
        return stocks
    
    def get_market_status(self):
        """Get current market status"""
        return {
            'nyse': {
                'is_open': self.is_nyse_open(),
                'timezone': 'America/New_York',
                'hours': '9:30 AM - 4:00 PM EST',
                'current_time': datetime.now(self.nyse_tz).strftime('%Y-%m-%d %H:%M:%S %Z')
            },
            'b3': {
                'is_open': self.is_b3_open(),
                'timezone': 'America/Sao_Paulo',
                'hours': '10:00 AM - 5:00 PM BRT',
                'current_time': datetime.now(self.b3_tz).strftime('%Y-%m-%d %H:%M:%S %Z')
            }
        }

def main():
    """Test stock market integration"""
    print("=== Stock Market Integration Test ===\n")
    
    integration = StockMarketIntegration()
    
    # Check market status
    print("📊 Market Status:")
    status = integration.get_market_status()
    print(f"  NYSE: {'🟢 OPEN' if status['nyse']['is_open'] else '🔴 CLOSED'} ({status['nyse']['current_time']})")
    print(f"  B3:   {'🟢 OPEN' if status['b3']['is_open'] else '🔴 CLOSED'} ({status['b3']['current_time']})")
    print()
    
    # Get all stocks
    print("📈 Fetching stock quotes...")
    stocks = integration.get_all_stocks()
    
    print(f"\n✅ Retrieved {len(stocks)} stock quotes\n")
    
    # Display US stocks
    print("🇺🇸 US Stocks:")
    print(f"{'Symbol':<8} {'Name':<25} {'Price':<10} {'Change':<10} {'Volume':<15}")
    print("-" * 80)
    
    us_stocks = [s for s in stocks if s['market'] == 'US']
    for stock in us_stocks:
        change_str = f"{stock['change']:+.2f} ({stock['change_percent']:+.2f}%)"
        print(f"{stock['symbol']:<8} {stock['name'][:24]:<25} ${stock['price']:<9.2f} {change_str:<10} {stock['volume']:>14,}")
    
    print()
    
    # Display Brazilian stocks
    print("🇧🇷 Brazilian Stocks (B3):")
    print(f"{'Symbol':<10} {'Name':<25} {'Price':<10} {'Change':<10} {'Volume':<15}")
    print("-" * 80)
    
    br_stocks = [s for s in stocks if s['market'] == 'BR']
    for stock in br_stocks:
        change_str = f"{stock['change']:+.2f} ({stock['change_percent']:+.2f}%)"
        print(f"{stock['symbol']:<10} {stock['name'][:24]:<25} R${stock['price']:<9.2f} {change_str:<10} {stock['volume']:>14,}")
    
    print("\n✅ Stock Market Integration Test Complete!")

if __name__ == "__main__":
    main()
