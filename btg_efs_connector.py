#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BTG Pactual EFS (Electronic Financial Services) WebSocket Connector
Nova arquitetura da API BTG - Experimental

ATENÇÃO: Este conector é experimental e requer cookies de sessão válidos
do navegador para funcionar. A autenticação é mais complexa que a API antiga.

Funcionalidades:
- Conexão WebSocket com efs.btgpactual.com
- Parsing de mensagens JSON
- Subscrição a book-price, quotes, trades
- Heartbeat automático
"""

import os
import json
import websocket
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Callable
from threading import Thread, Lock
import http.cookiejar as cookielib

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('btg_efs_connector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BTGEFSConnector:
    """
    Conector WebSocket para BTG Pactual EFS (Nova API)
    
    IMPORTANTE: Requer cookies de sessão válidos do navegador
    """
    
    def __init__(
        self,
        socket_id: Optional[str] = None,
        cookies: Optional[Dict[str, str]] = None,
        account: Optional[str] = None,
        on_book_callback: Optional[Callable] = None,
        on_quote_callback: Optional[Callable] = None
    ):
        """
        Inicializa conector BTG EFS
        
        Args:
            socket_id: ID do socket (ex: 'socket-VlnJiReeD')
            cookies: Dicionário de cookies de sessão
            account: Número da conta BTG
            on_book_callback: Callback para order book
            on_quote_callback: Callback para cotações
        """
        self.socket_id = socket_id or os.getenv('BTG_SOCKET_ID')
        self.cookies = cookies or self._load_cookies_from_env()
        self.account = account or os.getenv('BTG_ACCOUNT')
        
        if not self.socket_id:
            raise ValueError("Socket ID não fornecido. Configure BTG_SOCKET_ID no .env")
        
        self.endpoint = f"wss://efs.btgpactual.com/trader/{self.socket_id}"
        
        # Callbacks
        self.on_book_callback = on_book_callback
        self.on_quote_callback = on_quote_callback
        
        # Cache de dados
        self.cache_lock = Lock()
        self.books_cache: Dict[str, Dict] = {}
        self.quotes_cache: Dict[str, Dict] = {}
        
        # WebSocket
        self.ws: Optional[websocket.WebSocketApp] = None
        self.is_connected = False
        self.msg_seq_num = 0
        self.subscriptions: Dict[str, str] = {}  # topic -> subscriptionId
        
        # Thread
        self.ws_thread: Optional[Thread] = None
        self.heartbeat_thread: Optional[Thread] = None
        
        logger.info(f"BTGEFSConnector inicializado para socket: {self.socket_id}")
    
    def _load_cookies_from_env(self) -> Dict[str, str]:
        """Carrega cookies do .env"""
        cookies = {}
        cookie_str = os.getenv('BTG_COOKIES', '')
        if cookie_str:
            # Formato: "name1=value1; name2=value2"
            for cookie in cookie_str.split(';'):
                if '=' in cookie:
                    name, value = cookie.strip().split('=', 1)
                    cookies[name] = value
        return cookies
    
    def _on_message(self, ws, raw_message):
        """Processa mensagens do WebSocket"""
        try:
            data = json.loads(raw_message)
            
            msg_type = data.get('msgType')
            topic = data.get('topic')
            
            logger.debug(f"Mensagem recebida: {msg_type} / {topic}")
            
            # Heartbeat
            if topic == 'heartbeat':
                self._process_heartbeat(data)
            
            # Book Price (Order Book)
            elif topic == 'book-price':
                self._process_book_price(data)
            
            # Quote (Cotação)
            elif topic == 'quote':
                self._process_quote(data)
            
            # Outras mensagens
            else:
                logger.debug(f"Mensagem não processada: {topic}")
        
        except Exception as e:
            logger.error(f"Erro ao processar mensagem: {e}")
            logger.debug(f"Mensagem raw: {raw_message}")
    
    def _process_heartbeat(self, data: Dict):
        """Processa heartbeat"""
        logger.debug(f"Heartbeat recebido: {data.get('data')}")
        # Responder heartbeat se necessário
        # (algumas APIs exigem resposta, outras não)
    
    def _process_book_price(self, data: Dict):
        """Processa order book"""
        try:
            symbol = data.get('filters', {}).get('symbol')
            if not symbol:
                return
            
            book_data = {
                'symbol': symbol,
                'data': data.get('data', {}),
                'timestamp': datetime.now().isoformat(),
                'raw': data
            }
            
            with self.cache_lock:
                self.books_cache[symbol] = book_data
            
            if self.on_book_callback:
                self.on_book_callback(book_data)
            
            logger.debug(f"Book atualizado: {symbol}")
        
        except Exception as e:
            logger.error(f"Erro ao processar book: {e}")
    
    def _process_quote(self, data: Dict):
        """Processa cotação"""
        try:
            symbol = data.get('filters', {}).get('symbol')
            if not symbol:
                return
            
            quote_data = {
                'symbol': symbol,
                'price': None,  # TODO: extrair preço do data
                'data': data.get('data', {}),
                'timestamp': datetime.now().isoformat(),
                'raw': data
            }
            
            with self.cache_lock:
                self.quotes_cache[symbol] = quote_data
            
            if self.on_quote_callback:
                self.on_quote_callback(quote_data)
            
            logger.debug(f"Quote atualizado: {symbol}")
        
        except Exception as e:
            logger.error(f"Erro ao processar quote: {e}")
    
    def _on_error(self, ws, error):
        """Trata erros do WebSocket"""
        logger.error(f"WebSocket error: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Trata fechamento do WebSocket"""
        self.is_connected = False
        logger.warning(f"WebSocket fechado: {close_status_code} - {close_msg}")
    
    def _on_open(self, ws):
        """Trata abertura do WebSocket"""
        self.is_connected = True
        logger.info("WebSocket conectado ao BTG EFS")
        
        # Iniciar heartbeat thread
        self.heartbeat_thread = Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
    
    def _heartbeat_loop(self):
        """Envia heartbeat periodicamente"""
        while self.is_connected:
            try:
                # TODO: Descobrir se precisa enviar heartbeat
                # ou apenas responder aos recebidos
                time.sleep(30)
            except Exception as e:
                logger.error(f"Erro no heartbeat: {e}")
                break
    
    def connect(self):
        """Conecta ao WebSocket BTG EFS"""
        try:
            logger.info("Conectando ao BTG EFS...")
            
            websocket.enableTrace(False)
            
            # Preparar cookies para WebSocket
            cookie_str = '; '.join([f"{k}={v}" for k, v in self.cookies.items()])
            
            self.ws = websocket.WebSocketApp(
                self.endpoint,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                header={
                    "Origin": "https://app.btgpactual.com",
                    "Cookie": cookie_str
                }
            )
            
            # Executar em thread separada
            self.ws_thread = Thread(
                target=self.ws.run_forever,
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
        logger.info("Desconectado do BTG EFS")
    
    def subscribe_book_price(self, symbol: str):
        """
        Subscreve a order book de um símbolo
        
        Args:
            symbol: Símbolo da ação (ex: 'PETR4')
        """
        if not self.is_connected:
            logger.warning("WebSocket não conectado")
            return
        
        self.msg_seq_num += 1
        subscription_id = f"book_{symbol}_{int(time.time())}"
        
        msg = {
            "msgSeqNum": str(self.msg_seq_num),
            "msgType": "subscribe",
            "topic": "book-price",
            "filters": {"symbol": symbol},
            "interval": "1000",
            "format": "",
            "subscriptionId": subscription_id
        }
        
        self.ws.send(json.dumps(msg))
        self.subscriptions[f"book-price:{symbol}"] = subscription_id
        logger.info(f"Subscrito a book-price: {symbol}")
    
    def unsubscribe_book_price(self, symbol: str):
        """Cancela subscrição de order book"""
        key = f"book-price:{symbol}"
        if key not in self.subscriptions:
            return
        
        self.msg_seq_num += 1
        subscription_id = self.subscriptions[key]
        
        msg = {
            "msgSeqNum": str(self.msg_seq_num),
            "msgType": "unsubscribe",
            "topic": "book-price",
            "filters": {"symbol": symbol},
            "subscriptionId": subscription_id
        }
        
        self.ws.send(json.dumps(msg))
        del self.subscriptions[key]
        logger.info(f"Dessubscrito de book-price: {symbol}")
    
    def get_book(self, symbol: str) -> Optional[Dict]:
        """Retorna último order book de um símbolo"""
        with self.cache_lock:
            return self.books_cache.get(symbol)
    
    def get_quote(self, symbol: str) -> Optional[Dict]:
        """Retorna última cotação de um símbolo"""
        with self.cache_lock:
            return self.quotes_cache.get(symbol)


# Funções auxiliares

def extract_cookies_from_browser():
    """
    Extrai cookies do navegador (Safari/Chrome)
    
    NOTA: Requer permissões de acesso aos cookies do navegador
    Implementação depende do sistema operacional
    """
    # TODO: Implementar extração de cookies
    # Opções:
    # 1. Usar browser_cookie3 (biblioteca Python)
    # 2. Ler diretamente do arquivo de cookies do navegador
    # 3. Usar Selenium/Playwright para obter cookies
    
    logger.warning("Extração automática de cookies não implementada")
    logger.info("Configure BTG_COOKIES manualmente no .env")
    return {}


def get_socket_id_from_network():
    """
    Tenta descobrir socket ID atual do BTG
    
    NOTA: Requer acesso às conexões de rede ativas
    Implementação complexa, pode não ser viável
    """
    # TODO: Implementar descoberta de socket ID
    logger.warning("Descoberta automática de socket ID não implementada")
    logger.info("Configure BTG_SOCKET_ID manualmente no .env")
    return None


# Exemplo de uso
if __name__ == "__main__":
    print("BTG EFS Connector - Experimental")
    print("=" * 50)
    print()
    print("ATENÇÃO: Este conector requer configuração manual:")
    print()
    print("1. Obtenha o Socket ID:")
    print("   - Abra DevTools no BTG Home Broker")
    print("   - Aba Rede → Filtro 'ws'")
    print("   - Copie o ID do socket (ex: socket-VlnJiReeD)")
    print()
    print("2. Obtenha os Cookies de sessão:")
    print("   - No DevTools, aba 'Armazenamento' ou 'Application'")
    print("   - Cookies → app.btgpactual.com")
    print("   - Copie todos os cookies relevantes")
    print()
    print("3. Configure no .env:")
    print("   BTG_SOCKET_ID=socket-VlnJiReeD")
    print("   BTG_COOKIES='cookie1=value1; cookie2=value2'")
    print("   BTG_ACCOUNT=005871112")
    print()
    print("4. Execute novamente este script")
    print()
    
    # Tentar conectar se configurado
    socket_id = os.getenv('BTG_SOCKET_ID')
    if socket_id:
        print(f"Socket ID encontrado: {socket_id}")
        print("Tentando conectar...")
        
        try:
            connector = BTGEFSConnector()
            connector.connect()
            
            time.sleep(5)
            
            if connector.is_connected:
                print("✅ Conectado com sucesso!")
                
                # Testar subscrição
                connector.subscribe_book_price('PETR4')
                
                # Aguardar dados
                time.sleep(30)
                
                # Verificar cache
                book = connector.get_book('PETR4')
                if book:
                    print(f"✅ Book PETR4 recebido: {book}")
                else:
                    print("⚠️ Nenhum dado de book recebido ainda")
                
                connector.disconnect()
            else:
                print("❌ Falha na conexão")
        
        except Exception as e:
            print(f"❌ Erro: {e}")
    else:
        print("❌ BTG_SOCKET_ID não configurado no .env")
