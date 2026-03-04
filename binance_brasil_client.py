#!/usr/bin/env python3
"""
ArbitrageAI v2 - Cliente Binance Brasil
Integração com conta ativa do usuário para execução automática de trades
"""

import os
import json
import logging
from datetime import datetime
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BinanceBrasilClient:
    """Cliente para integração com Binance Brasil"""
    
    def __init__(self, api_key, api_secret):
        """
        Inicializar cliente Binance Brasil
        
        Args:
            api_key: Chave API do usuário
            api_secret: Chave secreta do usuário
        """
        self.api_key = api_key
        self.api_secret = api_secret
        
        try:
            self.client = Client(api_key, api_secret)
            logger.info("✅ Cliente Binance Brasil inicializado com sucesso")
        except Exception as e:
            logger.error(f"❌ Erro ao inicializar cliente: {e}")
            raise
    
    def test_connection(self):
        """Testar conexão com Binance Brasil"""
        try:
            status = self.client.get_account()
            logger.info(f"✅ Conexão com Binance Brasil estabelecida")
            logger.info(f"   Saldo de contas: {len(status['balances'])} ativos")
            return True
        except BinanceAPIException as e:
            logger.error(f"❌ Erro na API: {e.status_code} - {e.message}")
            return False
        except Exception as e:
            logger.error(f"❌ Erro na conexão: {e}")
            return False
    
    def get_account_balance(self):
        """Obter saldo da conta"""
        try:
            account = self.client.get_account()
            balances = {}
            
            for balance in account['balances']:
                if float(balance['free']) > 0 or float(balance['locked']) > 0:
                    balances[balance['asset']] = {
                        'free': float(balance['free']),
                        'locked': float(balance['locked']),
                        'total': float(balance['free']) + float(balance['locked'])
                    }
            
            logger.info(f"✅ Saldo obtido: {len(balances)} ativos com saldo")
            return balances
        except Exception as e:
            logger.error(f"❌ Erro ao obter saldo: {e}")
            return {}
    
    def get_ticker_price(self, symbol):
        """Obter preço atual de um símbolo"""
        try:
            ticker = self.client.get_symbol_info(symbol)
            if ticker:
                price = self.client.get_symbol_ticker(symbol=symbol)
                return float(price['price'])
            return None
        except Exception as e:
            logger.error(f"❌ Erro ao obter preço de {symbol}: {e}")
            return None
    
    def get_klines(self, symbol, interval, limit=100):
        """Obter dados de velas (klines) para análise"""
        try:
            klines = self.client.get_klines(symbol=symbol, interval=interval, limit=limit)
            logger.info(f"✅ Obtidas {len(klines)} velas para {symbol}")
            return klines
        except Exception as e:
            logger.error(f"❌ Erro ao obter klines para {symbol}: {e}")
            return []
    
    def place_test_order(self, symbol, side, quantity, price=None):
        """
        Fazer uma ordem de teste (não executa)
        
        Args:
            symbol: Par de trading (ex: BTCUSDT)
            side: BUY ou SELL
            quantity: Quantidade
            price: Preço (se None, usa ordem de mercado)
        """
        try:
            if price:
                order = self.client.create_test_order(
                    symbol=symbol,
                    side=side,
                    type='LIMIT',
                    timeInForce='GTC',
                    quantity=quantity,
                    price=price
                )
            else:
                order = self.client.create_test_order(
                    symbol=symbol,
                    side=side,
                    type='MARKET',
                    quantity=quantity
                )
            
            logger.info(f"✅ Ordem de teste criada: {side} {quantity} {symbol}")
            return order
        except BinanceOrderException as e:
            logger.error(f"❌ Erro na ordem: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Erro ao criar ordem de teste: {e}")
            return None
    
    def place_real_order(self, symbol, side, quantity, price=None):
        """
        Fazer uma ordem real (executa no mercado)
        
        Args:
            symbol: Par de trading (ex: BTCUSDT)
            side: BUY ou SELL
            quantity: Quantidade
            price: Preço (se None, usa ordem de mercado)
        """
        try:
            logger.warning(f"⚠️  EXECUTANDO ORDEM REAL: {side} {quantity} {symbol}")
            
            if price:
                order = self.client.order_limit(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    price=price
                )
            else:
                order = self.client.order_market(
                    symbol=symbol,
                    side=side,
                    quantity=quantity
                )
            
            logger.info(f"✅ Ordem real executada: {order['orderId']}")
            return order
        except BinanceOrderException as e:
            logger.error(f"❌ Erro na ordem: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Erro ao criar ordem real: {e}")
            return None
    
    def get_open_orders(self, symbol=None):
        """Obter ordens abertas"""
        try:
            if symbol:
                orders = self.client.get_open_orders(symbol=symbol)
            else:
                orders = self.client.get_open_orders()
            
            logger.info(f"✅ Obtidas {len(orders)} ordens abertas")
            return orders
        except Exception as e:
            logger.error(f"❌ Erro ao obter ordens abertas: {e}")
            return []
    
    def cancel_order(self, symbol, order_id):
        """Cancelar uma ordem"""
        try:
            result = self.client.cancel_order(symbol=symbol, orderId=order_id)
            logger.info(f"✅ Ordem {order_id} cancelada")
            return result
        except Exception as e:
            logger.error(f"❌ Erro ao cancelar ordem: {e}")
            return None


def main():
    """Teste do cliente Binance Brasil"""
    
    # Obter chaves do ambiente
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')
    
    if not api_key or not api_secret:
        logger.error("❌ Chaves de API não encontradas nas variáveis de ambiente")
        logger.error("   Configure: BINANCE_API_KEY e BINANCE_API_SECRET")
        return
    
    logger.info("=" * 60)
    logger.info("ArbitrageAI v2 - Cliente Binance Brasil")
    logger.info("=" * 60)
    
    # Criar cliente
    client = BinanceBrasilClient(api_key, api_secret)
    
    # Testar conexão
    if not client.test_connection():
        logger.error("❌ Falha na conexão com Binance Brasil")
        return
    
    # Obter saldo
    balances = client.get_account_balance()
    if balances:
        logger.info("\n📊 Saldo da Conta:")
        for asset, balance in list(balances.items())[:10]:
            logger.info(f"   {asset}: {balance['total']:.8f}")
    
    # Obter preços
    logger.info("\n💰 Preços Atuais:")
    symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
    for symbol in symbols:
        price = client.get_ticker_price(symbol)
        if price:
            logger.info(f"   {symbol}: ${price:,.2f}")
    
    # Testar ordem (sem executar)
    logger.info("\n🧪 Testando ordem (modo teste):")
    test_order = client.place_test_order('BTCUSDT', 'BUY', 0.001, price=67000)
    if test_order:
        logger.info("   ✅ Ordem de teste criada com sucesso")
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ Cliente Binance Brasil pronto para uso!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
