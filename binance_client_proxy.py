#!/usr/bin/env python3.11
"""
ArbitrageAI v2 - Cliente Binance com Proxy
Integração com proxy para contornar restrições geográficas
"""

import os
import sys
sys.path.insert(0, '/home/ubuntu/arbitrage-dashboard')

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import hmac
import hashlib
import time
from datetime import datetime
import json
import logging
from typing import Dict, List, Optional
import socks
from socket import socket, AF_INET, SOCK_STREAM

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/ubuntu/arbitrage-dashboard/logs/binance_proxy.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Lista de proxies públicos confiáveis
PROXY_LIST = [
    "http://proxy.example.com:8080",  # Placeholder
    # Você pode adicionar proxies reais aqui
]

class BinanceClientWithProxy:
    """Cliente Binance com suporte a proxy para contornar restrições geográficas"""
    
    def __init__(self, api_key: str, api_secret: str, proxy_url: Optional[str] = None, testnet: bool = True):
        """
        Inicializar cliente com proxy
        
        Args:
            api_key: Chave de API da Binance
            api_secret: Chave secreta da Binance
            proxy_url: URL do proxy (ex: http://proxy.com:8080)
            testnet: Se True, usa modo testnet
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.proxy_url = proxy_url
        
        self.base_url = "https://api.binance.com/api"
        
        # Criar sessão com retry logic
        self.session = self._create_session()
        
        logger.info(f"✅ Cliente Binance inicializado com proxy: {proxy_url if proxy_url else 'Nenhum'}")
    
    def _create_session(self) -> requests.Session:
        """Criar sessão com retry logic e proxy"""
        session = requests.Session()
        
        # Configurar headers
        session.headers.update({
            'X-MBX-APIKEY': self.api_key,
            'Content-Type': 'application/json',
            'User-Agent': 'ArbitrageAI/2.0'
        })
        
        # Configurar retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "DELETE"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Configurar proxy se fornecido
        if self.proxy_url:
            proxies = {
                'http': self.proxy_url,
                'https': self.proxy_url
            }
            session.proxies.update(proxies)
            logger.info(f"🔄 Proxy configurado: {self.proxy_url}")
        
        return session
    
    def _generate_signature(self, params: Dict) -> str:
        """Gerar assinatura HMAC-SHA256"""
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        signature = hmac.new(
            self.api_secret.encode(),
            query_string.encode(),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def _request(self, method: str, endpoint: str, params: Dict = None, signed: bool = False) -> Dict:
        """Fazer requisição com tratamento de erros"""
        url = f"{self.base_url}{endpoint}"
        
        if params is None:
            params = {}
        
        if signed:
            params['timestamp'] = int(time.time() * 1000)
            params['recvWindow'] = 5000
            params['signature'] = self._generate_signature(params)
        
        try:
            if method == 'GET':
                response = self.session.get(url, params=params, timeout=10)
            elif method == 'POST':
                response = self.session.post(url, params=params, timeout=10)
            elif method == 'DELETE':
                response = self.session.delete(url, params=params, timeout=10)
            else:
                raise ValueError(f"Método não suportado: {method}")
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.ProxyError as e:
            logger.error(f"❌ Erro de proxy: {e}")
            return {'error': f'Proxy error: {str(e)}'}
        except requests.exceptions.ConnectionError as e:
            logger.error(f"❌ Erro de conexão: {e}")
            return {'error': f'Connection error: {str(e)}'}
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na requisição: {e}")
            return {'error': str(e)}
    
    def test_connection(self) -> bool:
        """Testar conexão com proxy"""
        logger.info("🔗 Testando conexão com Binance via proxy...")
        try:
            response = self._request('GET', '/v3/ping')
            
            if 'error' not in response:
                logger.info("✅ Conexão bem-sucedida!")
                return True
            else:
                logger.error(f"❌ Erro: {response['error']}")
                return False
        except Exception as e:
            logger.error(f"❌ Falha ao testar: {e}")
            return False
    
    def get_server_time(self) -> Optional[int]:
        """Obter hora do servidor Binance"""
        response = self._request('GET', '/v3/time')
        
        if 'serverTime' in response:
            return response['serverTime']
        else:
            logger.error(f"Erro ao obter hora: {response}")
            return None
    
    def get_account_info(self) -> Dict:
        """Obter informações da conta"""
        logger.info("📊 Obtendo informações da conta...")
        response = self._request('GET', '/v3/account', signed=True)
        
        if 'error' not in response and 'balances' in response:
            logger.info(f"✅ Conta obtida com {len(response['balances'])} ativos")
            return response
        else:
            logger.error(f"❌ Erro: {response}")
            return {}
    
    def get_balance(self, symbol: str = 'USDT') -> float:
        """Obter saldo de um ativo"""
        account = self.get_account_info()
        
        for balance in account.get('balances', []):
            if balance['asset'] == symbol:
                free = float(balance['free'])
                locked = float(balance['locked'])
                total = free + locked
                logger.info(f"💰 {symbol}: {free:.8f} livre, {locked:.8f} travado, {total:.8f} total")
                return free
        
        logger.warning(f"⚠️  {symbol} não encontrado")
        return 0.0
    
    def get_price(self, symbol: str) -> Optional[float]:
        """Obter preço atual"""
        try:
            response = self._request('GET', '/v3/ticker/price', {'symbol': symbol})
            
            if 'price' in response:
                price = float(response['price'])
                logger.debug(f"📈 {symbol}: ${price:.8f}")
                return price
            else:
                logger.error(f"Erro ao obter preço: {response}")
                return None
        except Exception as e:
            logger.error(f"Exceção: {e}")
            return None
    
    def place_order(self, symbol: str, side: str, quantity: float, price: Optional[float] = None, order_type: str = 'MARKET') -> Dict:
        """Colocar ordem"""
        logger.info(f"📝 Ordem: {side} {quantity} {symbol} @ {order_type}")
        
        params = {
            'symbol': symbol,
            'side': side.upper(),
            'type': order_type.upper(),
            'quantity': quantity
        }
        
        if order_type.upper() == 'LIMIT' and price:
            params['price'] = price
            params['timeInForce'] = 'GTC'
        
        response = self._request('POST', '/v3/order', params, signed=True)
        
        if 'orderId' in response:
            logger.info(f"✅ Ordem colocada! ID: {response['orderId']}")
            return response
        else:
            logger.error(f"❌ Erro: {response}")
            return response
    
    def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        """Obter ordens abertas"""
        params = {}
        if symbol:
            params['symbol'] = symbol
        
        response = self._request('GET', '/v3/openOrders', params, signed=True)
        
        if isinstance(response, list):
            logger.info(f"📋 {len(response)} ordens abertas")
            return response
        else:
            logger.error(f"Erro: {response}")
            return []

def find_working_proxy() -> Optional[str]:
    """Encontrar um proxy funcional"""
    logger.info("🔍 Procurando proxy funcional...")
    
    # Tentar alguns proxies públicos conhecidos
    proxies_to_try = [
        "http://10.10.1.10:3128",
        "http://proxy.example.com:8080",
    ]
    
    for proxy in proxies_to_try:
        try:
            logger.info(f"Testando proxy: {proxy}")
            response = requests.get('https://api.binance.com/api/v3/ping', proxies={'https': proxy}, timeout=5)
            if response.status_code == 200:
                logger.info(f"✅ Proxy funcional encontrado: {proxy}")
                return proxy
        except Exception as e:
            logger.debug(f"Proxy {proxy} não funcionou: {e}")
    
    logger.warning("⚠️  Nenhum proxy funcional encontrado")
    return None

def main():
    """Teste do cliente com proxy"""
    api_key = os.getenv('BINANCE_API_KEY', 'p5TNE1CcjtO2ldYfcXmyV2t7dnousf4UQJHR0fdCgykbzDNxRtwzGxFGQT3uVOKU')
    api_secret = os.getenv('BINANCE_API_SECRET', 'nHyzsYTiQWqMkiYy5FZPkEk13vpqvi21oJ8sRzeTq24q7McNsdij5XChjTT2Qa2b')
    proxy_url = os.getenv('BINANCE_PROXY', None)
    
    logger.info("\n" + "="*60)
    logger.info("ArbitrageAI v2 - Cliente Binance com Proxy")
    logger.info("="*60 + "\n")
    
    # Tentar encontrar proxy se não fornecido
    if not proxy_url:
        logger.info("Nenhum proxy fornecido, tentando encontrar um...")
        proxy_url = find_working_proxy()
    
    # Criar cliente
    client = BinanceClientWithProxy(api_key, api_secret, proxy_url=proxy_url, testnet=True)
    
    # Testar conexão
    if not client.test_connection():
        logger.error("Falha na conexão. Tente:")
        logger.error("1. Usar um proxy diferente")
        logger.error("2. Usar uma VPN")
        logger.error("3. Acessar de uma localização permitida")
        return
    
    # Obter hora do servidor
    server_time = client.get_server_time()
    if server_time:
        logger.info(f"⏰ Hora do servidor: {datetime.fromtimestamp(server_time/1000)}")
    
    # Obter informações da conta
    account = client.get_account_info()
    
    # Obter saldos
    logger.info("\n=== SALDOS ===")
    for symbol in ['USDT', 'BTC', 'ETH', 'BNB']:
        balance = client.get_balance(symbol)
    
    # Obter preços
    logger.info("\n=== PREÇOS ===")
    for symbol in ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']:
        price = client.get_price(symbol)
    
    # Obter ordens abertas
    logger.info("\n=== ORDENS ABERTAS ===")
    orders = client.get_open_orders()
    
    logger.info("\n✅ Teste concluído!")

if __name__ == "__main__":
    main()
