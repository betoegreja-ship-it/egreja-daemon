#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BTG Pactual Home Broker WebSocket Connector
Integrado ao ArbitrageAI Dashboard

Funcionalidades:
- Conexão WebSocket em tempo real com BTG HB
- Limit Order Book (LOB) completo
- Trade Feed em tempo real
- Broker Ranking
- Book Analytics (imbalance, spread, liquidez)
"""

import os
import json
import websocket
import time
import ssl
import logging
from datetime import datetime
from typing import Dict, List, Optional, Callable
from threading import Thread, Lock
from dotenv import load_dotenv

# Importar classes do btg-api
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'btg-api'))

from AggregatedBook import AggregatedBookRequest, AggregatedBookType
from AggregatedBookAnalytics import AggregatedBookAnalytics
from Quote import QuoteRequest, QuoteType
from QuoteTrade import QuoteTradeRequest, QuoteTradeType

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('btg_connector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()


class BTGConnector:
    """
    Conector WebSocket para BTG Pactual Home Broker
    """
    
    def __init__(
        self,
        token: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        on_quote_callback: Optional[Callable] = None,
        on_book_callback: Optional[Callable] = None,
        on_trade_callback: Optional[Callable] = None
    ):
        """
        Inicializa conector BTG
        
        Args:
            token: Token TKNWF (ou lê de .env)
            symbols: Lista de símbolos B3 (ex: ['PETR4', 'VALE3'])
            on_quote_callback: Callback para cotações
            on_book_callback: Callback para order book
            on_trade_callback: Callback para trades
        """
        self.token = token or os.getenv('BTG_TKNWF')
        self.symbols = symbols or self._get_default_symbols()
        self.endpoint = "wss://webfeeder.btgpactual.com/ws?reconnect="
        
        # Callbacks
        self.on_quote_callback = on_quote_callback
        self.on_book_callback = on_book_callback
        self.on_trade_callback = on_trade_callback
        
        # Cache de dados em tempo real
        self.cache_lock = Lock()
        self.quotes_cache: Dict[str, Dict] = {}
        self.books_cache: Dict[str, Dict] = {}
        self.trades_cache: Dict[str, List[Dict]] = {}
        self.analytics_cache: Dict[str, Dict] = {}
        
        # WebSocket
        self.ws: Optional[websocket.WebSocketApp] = None
        self.is_connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        
        # Thread
        self.ws_thread: Optional[Thread] = None
        
        if not self.token:
            raise ValueError("Token TKNWF não fornecido. Configure BTG_TKNWF no .env")
        
        logger.info(f"BTGConnector inicializado para símbolos: {self.symbols}")
    
    def _get_default_symbols(self) -> List[str]:
        """Retorna lista padrão de símbolos B3"""
        return [
            'PETR4',  # Petrobras
            'VALE3',  # Vale
            'ITUB4',  # Itaú
            'BBDC4',  # Bradesco
            'ABEV3',  # Ambev
            'WEGE3',  # WEG
            'RENT3',  # Localiza
            'MGLU3',  # Magazine Luiza
        ]
    
    def _on_message(self, ws, raw_message):
        """Processa mensagens do WebSocket"""
        try:
            data = json.loads(raw_message)
            
            if 'type' not in data:
                return
            
            msg_type = data['type']
            
            # Quote (cotação)
            if msg_type == 'QuoteType':
                quote = QuoteType(data)
                self._process_quote(quote)
            
            # Aggregated Book (livro de ofertas)
            elif msg_type == 'AggregatedBookType':
                book = AggregatedBookType(data)
                analytics = AggregatedBookAnalytics(book)
                self._process_book(book, analytics)
            
            # Business Book (trades executados)
            elif msg_type == 'BusinessBookType':
                trade = QuoteTradeType(data)
                self._process_trade(trade)
        
        except Exception as e:
            logger.error(f"Erro ao processar mensagem: {e}")
    
    def _process_quote(self, quote: QuoteType):
        """Processa cotação recebida"""
        try:
            symbol = quote.symbol
            
            quote_data = {
                'symbol': symbol,
                'price': quote.last,
                'bid': quote.bid,
                'ask': quote.ask,
                'volume': quote.volume,
                'timestamp': datetime.now().isoformat(),
                'raw': quote.__dict__
            }
            
            with self.cache_lock:
                self.quotes_cache[symbol] = quote_data
            
            if self.on_quote_callback:
                self.on_quote_callback(quote_data)
            
            logger.debug(f"Quote {symbol}: R${quote.last:.2f}")
        
        except Exception as e:
            logger.error(f"Erro ao processar quote: {e}")
    
    def _process_book(self, book: AggregatedBookType, analytics: AggregatedBookAnalytics):
        """Processa order book recebido"""
        try:
            symbol = book.symbol
            
            book_data = {
                'symbol': symbol,
                'bids': book.bids,
                'asks': book.asks,
                'analytics': {
                    'spread': analytics.spread,
                    'book_imbalance': analytics.book_imbalance,
                    'weighted_mid_price': analytics.weighted_mid_price,
                    'best_bid': analytics.best_bid,
                    'best_ask': analytics.best_ask,
                    'balance_bid_pct': analytics.balance_bid_pct,
                    'balance_ask_pct': analytics.balance_ask_pct,
                },
                'timestamp': datetime.now().isoformat(),
                'raw_book': book.__dict__,
                'raw_analytics': analytics.__dict__
            }
            
            with self.cache_lock:
                self.books_cache[symbol] = book_data
                self.analytics_cache[symbol] = book_data['analytics']
            
            if self.on_book_callback:
                self.on_book_callback(book_data)
            
            logger.debug(f"Book {symbol}: Spread={analytics.spread:.4f}, Imbalance={analytics.book_imbalance:.4f}")
        
        except Exception as e:
            logger.error(f"Erro ao processar book: {e}")
    
    def _process_trade(self, trade: QuoteTradeType):
        """Processa trade executado"""
        try:
            symbol = trade.symbol
            
            trade_data = {
                'symbol': symbol,
                'price': trade.price,
                'quantity': trade.quantity,
                'side': trade.side,
                'timestamp': datetime.now().isoformat(),
                'raw': trade.__dict__
            }
            
            with self.cache_lock:
                if symbol not in self.trades_cache:
                    self.trades_cache[symbol] = []
                self.trades_cache[symbol].append(trade_data)
                # Manter apenas últimos 100 trades
                if len(self.trades_cache[symbol]) > 100:
                    self.trades_cache[symbol] = self.trades_cache[symbol][-100:]
            
            if self.on_trade_callback:
                self.on_trade_callback(trade_data)
            
            logger.debug(f"Trade {symbol}: {trade.side} {trade.quantity} @ R${trade.price:.2f}")
        
        except Exception as e:
            logger.error(f"Erro ao processar trade: {e}")
    
    def _on_error(self, ws, error):
        """Trata erros do WebSocket"""
        logger.error(f"WebSocket error: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Trata fechamento do WebSocket"""
        self.is_connected = False
        logger.warning(f"WebSocket fechado: {close_status_code} - {close_msg}")
        
        # Tentar reconectar
        if self.reconnect_attempts < self.max_reconnect_attempts:
            self.reconnect_attempts += 1
            wait_time = min(2 ** self.reconnect_attempts, 60)  # Exponential backoff
            logger.info(f"Tentando reconectar em {wait_time}s (tentativa {self.reconnect_attempts}/{self.max_reconnect_attempts})")
            time.sleep(wait_time)
            self.connect()
        else:
            logger.error("Máximo de tentativas de reconexão atingido")
    
    def _on_open(self, ws):
        """Trata abertura do WebSocket"""
        self.is_connected = True
        self.reconnect_attempts = 0
        logger.info("WebSocket conectado ao BTG HB")
        
        # Enviar requisições de subscrição
        def subscribe():
            for symbol in self.symbols:
                try:
                    # Subscrever cotações
                    quote_req = QuoteRequest(self.token, symbol).to_json()
                    ws.send(json.dumps(quote_req))
                    time.sleep(0.5)
                    
                    # Subscrever order book
                    book_req = AggregatedBookRequest(self.token, symbol).to_json()
                    ws.send(json.dumps(book_req))
                    time.sleep(0.5)
                    
                    # Subscrever trades
                    trade_req = QuoteTradeRequest(self.token, symbol).to_json()
                    ws.send(json.dumps(trade_req))
                    time.sleep(0.5)
                    
                    logger.info(f"Subscrito a {symbol}")
                
                except Exception as e:
                    logger.error(f"Erro ao subscrever {symbol}: {e}")
        
        # Iniciar thread de subscrição
        Thread(target=subscribe, daemon=True).start()
    
    def connect(self):
        """Conecta ao WebSocket BTG"""
        try:
            logger.info("Conectando ao BTG Home Broker...")
            
            websocket.enableTrace(False)
            self.ws = websocket.WebSocketApp(
                self.endpoint + self.token,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            
            # Executar em thread separada
            self.ws_thread = Thread(
                target=self.ws.run_forever,
                kwargs={'sslopt': {"cert_reqs": ssl.CERT_NONE}},
                daemon=True
            )
            self.ws_thread.start()
            
            logger.info("Thread WebSocket iniciada")
        
        except Exception as e:
            logger.error(f"Erro ao conectar: {e}")
            raise
    
    def disconnect(self):
        """Desconecta do WebSocket"""
        if self.ws:
            self.ws.close()
        self.is_connected = False
        logger.info("Desconectado do BTG HB")
    
    def get_quote(self, symbol: str) -> Optional[Dict]:
        """Retorna última cotação de um símbolo"""
        with self.cache_lock:
            return self.quotes_cache.get(symbol)
    
    def get_book(self, symbol: str) -> Optional[Dict]:
        """Retorna último order book de um símbolo"""
        with self.cache_lock:
            return self.books_cache.get(symbol)
    
    def get_analytics(self, symbol: str) -> Optional[Dict]:
        """Retorna analytics do order book"""
        with self.cache_lock:
            return self.analytics_cache.get(symbol)
    
    def get_recent_trades(self, symbol: str, limit: int = 10) -> List[Dict]:
        """Retorna trades recentes de um símbolo"""
        with self.cache_lock:
            trades = self.trades_cache.get(symbol, [])
            return trades[-limit:]
    
    def get_all_quotes(self) -> Dict[str, Dict]:
        """Retorna todas as cotações em cache"""
        with self.cache_lock:
            return self.quotes_cache.copy()
    
    def is_market_open(self) -> bool:
        """Verifica se mercado B3 está aberto (10h-17h BRT)"""
        from datetime import datetime
        import pytz
        
        brt = pytz.timezone('America/Sao_Paulo')
        now = datetime.now(brt)
        
        # Segunda a sexta
        if now.weekday() >= 5:
            return False
        
        # 10h às 17h
        market_open = now.replace(hour=10, minute=0, second=0, microsecond=0)
        market_close = now.replace(hour=17, minute=0, second=0, microsecond=0)
        
        return market_open <= now <= market_close


# Funções auxiliares para integração com market_data.py

def get_btg_price(symbol: str, connector: BTGConnector) -> Optional[float]:
    """
    Busca preço atual de uma ação B3 via BTG
    
    Args:
        symbol: Símbolo da ação (ex: 'PETR4')
        connector: Instância do BTGConnector
    
    Returns:
        Preço atual ou None
    """
    quote = connector.get_quote(symbol)
    if quote:
        return quote.get('price')
    return None


def get_btg_book_imbalance(symbol: str, connector: BTGConnector) -> Optional[float]:
    """
    Calcula book imbalance de uma ação
    
    Imbalance > 0: Pressão compradora
    Imbalance < 0: Pressão vendedora
    
    Args:
        symbol: Símbolo da ação
        connector: Instância do BTGConnector
    
    Returns:
        Book imbalance (-1 a 1) ou None
    """
    analytics = connector.get_analytics(symbol)
    if analytics:
        return analytics.get('book_imbalance')
    return None


# Exemplo de uso
if __name__ == "__main__":
    # Callbacks de exemplo
    def on_quote(quote_data):
        print(f"[QUOTE] {quote_data['symbol']}: R${quote_data['price']:.2f}")
    
    def on_book(book_data):
        analytics = book_data['analytics']
        print(f"[BOOK] {book_data['symbol']}: Spread={analytics['spread']:.4f}, Imbalance={analytics['book_imbalance']:.4f}")
    
    def on_trade(trade_data):
        print(f"[TRADE] {trade_data['symbol']}: {trade_data['side']} {trade_data['quantity']} @ R${trade_data['price']:.2f}")
    
    # Inicializar conector
    connector = BTGConnector(
        symbols=['PETR4', 'VALE3', 'ITUB4'],
        on_quote_callback=on_quote,
        on_book_callback=on_book,
        on_trade_callback=on_trade
    )
    
    # Conectar
    connector.connect()
    
    # Aguardar dados
    time.sleep(10)
    
    # Buscar dados
    print("\n=== DADOS EM CACHE ===")
    for symbol in connector.symbols:
        quote = connector.get_quote(symbol)
        analytics = connector.get_analytics(symbol)
        
        if quote:
            print(f"\n{symbol}:")
            print(f"  Preço: R${quote['price']:.2f}")
            print(f"  Bid: R${quote['bid']:.2f}")
            print(f"  Ask: R${quote['ask']:.2f}")
        
        if analytics:
            print(f"  Spread: R${analytics['spread']:.4f}")
            print(f"  Book Imbalance: {analytics['book_imbalance']:.4f}")
            print(f"  Pressão: {'COMPRADORA' if analytics['book_imbalance'] > 0 else 'VENDEDORA'}")
    
    # Manter rodando
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        connector.disconnect()
        print("\nDesconectado")
