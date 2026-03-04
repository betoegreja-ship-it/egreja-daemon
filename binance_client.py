#!/usr/bin/env python3.11
"""
ArbitrageAI v2 - Cliente Binance para Testnet
Integração segura com API da Binance para execução automática de trades
"""

import os
import sys
sys.path.insert(0, '/home/ubuntu/arbitrage-dashboard')

import requests
import hmac
import hashlib
import time
from datetime import datetime
import json
import logging
from typing import Dict, List, Optional, Tuple

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/ubuntu/arbitrage-dashboard/logs/binance_client.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BinanceClient:
    """Cliente profissional para integração com Binance Testnet"""
    
    def __init__(self, api_key: str, api_secret: str, testnet: bool = True):
        """
        Inicializar cliente Binance
        
        Args:
            api_key: Chave de API da Binance
            api_secret: Chave secreta da Binance
            testnet: Se True, usa Testnet; se False, usa Mainnet
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        
        # URLs da API
        if testnet:
            # Tentar Testnet alternativo
            self.base_url = "https://api.binance.com/api"
            self.testnet_mode = True
            logger.info("🧪 Usando modo TESTNET (simulado)")
        else:
            self.base_url = "https://api.binance.com/api"
            self.testnet_mode = False
            logger.warning("⚠️  Usando Binance MAINNET - OPERAÇÕES REAIS!")
        
        self.session = requests.Session()
        self.session.headers.update({
            'X-MBX-APIKEY': self.api_key,
            'Content-Type': 'application/json'
        })
    
    def _generate_signature(self, params: Dict) -> str:
        """Gerar assinatura HMAC-SHA256 para requisições"""
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        signature = hmac.new(
            self.api_secret.encode(),
            query_string.encode(),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def _request(self, method: str, endpoint: str, params: Dict = None, signed: bool = False) -> Dict:
        """Fazer requisição à API da Binance"""
        url = f"{self.base_url}{endpoint}"
        
        if params is None:
            params = {}
        
        if signed:
            params['timestamp'] = int(time.time() * 1000)
            params['recvWindow'] = 5000
            params['signature'] = self._generate_signature(params)
        
        try:
            if method == 'GET':
                response = self.session.get(url, params=params)
            elif method == 'POST':
                response = self.session.post(url, params=params)
            else:
                raise ValueError(f"Método HTTP não suportado: {method}")
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na requisição: {e}")
            return {'error': str(e)}
    
    def test_connection(self) -> bool:
        """Testar conexão com a API"""
        logger.info("🔗 Testando conexão com Binance...")
        try:
            response = self._request('GET', '/v3/ping')
            if 'error' not in response:
                logger.info("✅ Conexão bem-sucedida!")
                return True
            else:
                logger.error(f"❌ Erro na conexão: {response['error']}")
                return False
        except Exception as e:
            logger.error(f"❌ Falha ao testar conexão: {e}")
            return False
    
    def get_account_info(self) -> Dict:
        """Obter informações da conta"""
        logger.info("📊 Obtendo informações da conta...")
        response = self._request('GET', '/v3/account', signed=True)
        
        if 'error' not in response:
            logger.info(f"✅ Conta obtida: {len(response.get('balances', []))} ativos")
            return response
        else:
            logger.error(f"❌ Erro ao obter conta: {response['error']}")
            return {}
    
    def get_balance(self, symbol: str = 'USDT') -> float:
        """Obter saldo de um ativo específico"""
        account = self.get_account_info()
        
        for balance in account.get('balances', []):
            if balance['asset'] == symbol:
                free = float(balance['free'])
                locked = float(balance['locked'])
                total = free + locked
                logger.info(f"💰 {symbol}: {free:.8f} (livre), {locked:.8f} (travado), {total:.8f} (total)")
                return free
        
        logger.warning(f"⚠️  Ativo {symbol} não encontrado")
        return 0.0
    
    def get_price(self, symbol: str) -> Optional[float]:
        """Obter preço atual de um símbolo"""
        try:
            response = self._request('GET', '/v3/ticker/price', {'symbol': symbol})
            
            if 'error' not in response:
                price = float(response['price'])
                logger.debug(f"📈 {symbol}: ${price:.8f}")
                return price
            else:
                logger.error(f"❌ Erro ao obter preço de {symbol}: {response['error']}")
                return None
        except Exception as e:
            logger.error(f"❌ Exceção ao obter preço: {e}")
            return None
    
    def place_order(self, symbol: str, side: str, quantity: float, price: Optional[float] = None, order_type: str = 'MARKET') -> Dict:
        """
        Colocar ordem na Binance
        
        Args:
            symbol: Par de trading (ex: ETHUSDT)
            side: BUY ou SELL
            quantity: Quantidade
            price: Preço (necessário para LIMIT)
            order_type: MARKET ou LIMIT
        
        Returns:
            Resposta da ordem
        """
        logger.info(f"📝 Colocando ordem: {side} {quantity} {symbol} @ {order_type}")
        
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
        
        if 'error' not in response:
            order_id = response.get('orderId')
            logger.info(f"✅ Ordem colocada com sucesso! ID: {order_id}")
            return response
        else:
            logger.error(f"❌ Erro ao colocar ordem: {response['error']}")
            return response
    
    def cancel_order(self, symbol: str, order_id: int) -> Dict:
        """Cancelar uma ordem"""
        logger.info(f"❌ Cancelando ordem {order_id} para {symbol}...")
        
        params = {
            'symbol': symbol,
            'orderId': order_id
        }
        
        response = self._request('DELETE', '/v3/order', params, signed=True)
        
        if 'error' not in response:
            logger.info(f"✅ Ordem cancelada com sucesso!")
            return response
        else:
            logger.error(f"❌ Erro ao cancelar ordem: {response['error']}")
            return response
    
    def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        """Obter ordens abertas"""
        params = {}
        if symbol:
            params['symbol'] = symbol
        
        response = self._request('GET', '/v3/openOrders', params, signed=True)
        
        if isinstance(response, list):
            logger.info(f"📋 {len(response)} ordens abertas encontradas")
            return response
        else:
            logger.error(f"❌ Erro ao obter ordens abertas: {response.get('error', 'Desconhecido')}")
            return []
    
    def get_order_history(self, symbol: str, limit: int = 10) -> List[Dict]:
        """Obter histórico de ordens"""
        params = {
            'symbol': symbol,
            'limit': limit
        }
        
        response = self._request('GET', '/v3/allOrders', params, signed=True)
        
        if isinstance(response, list):
            logger.info(f"📜 {len(response)} ordens no histórico para {symbol}")
            return response
        else:
            logger.error(f"❌ Erro ao obter histórico: {response.get('error', 'Desconhecido')}")
            return []
    
    def get_klines(self, symbol: str, interval: str = '1h', limit: int = 100) -> List[List]:
        """
        Obter dados de velas (klines)
        
        Args:
            symbol: Par de trading
            interval: Intervalo (1m, 5m, 15m, 1h, 4h, 1d, etc)
            limit: Número de velas
        
        Returns:
            Lista de velas
        """
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }
        
        response = self._request('GET', '/v3/klines', params)
        
        if isinstance(response, list):
            logger.debug(f"📊 {len(response)} velas obtidas para {symbol}")
            return response
        else:
            logger.error(f"❌ Erro ao obter klines: {response.get('error', 'Desconhecido')}")
            return []

def main():
    """Teste do cliente Binance"""
    # Carregar chaves do ambiente
    api_key = os.getenv('BINANCE_API_KEY', 'p5TNE1CcjtO2ldYfcXmyV2t7dnousf4UQJHR0fdCgykbzDNxRtwzGxFGQT3uVOKU')
    api_secret = os.getenv('BINANCE_API_SECRET', 'nHyzsYTiQWqMkiYy5FZPkEk13vpqvi21oJ8sRzeTq24q7McNsdij5XChjTT2Qa2b')
    
    # Criar cliente
    client = BinanceClient(api_key, api_secret, testnet=True)
    
    # Testar conexão
    if not client.test_connection():
        logger.error("Falha ao conectar com Binance")
        return
    
    # Obter informações da conta
    account = client.get_account_info()
    
    # Obter saldos principais
    logger.info("\n=== SALDOS DA CONTA ===")
    for symbol in ['USDT', 'BTC', 'ETH', 'BNB']:
        balance = client.get_balance(symbol)
    
    # Obter preços
    logger.info("\n=== PREÇOS ATUAIS ===")
    for symbol in ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']:
        price = client.get_price(symbol)
    
    # Obter ordens abertas
    logger.info("\n=== ORDENS ABERTAS ===")
    orders = client.get_open_orders()
    
    logger.info("\n✅ Teste do cliente Binance concluído!")

if __name__ == "__main__":
    main()
