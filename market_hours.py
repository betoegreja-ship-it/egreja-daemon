#!/usr/bin/env python3
"""
Market Hours Checker
Verifica se mercados (B3, NYSE, Crypto) estão abertos
"""

from datetime import datetime
import pytz
from typing import Literal

MarketType = Literal['B3', 'NYSE', 'CRYPTO']


class MarketHoursChecker:
    """Verifica horário de funcionamento dos mercados"""
    
    def __init__(self):
        """Inicializar checker de horários"""
        self.b3_tz = pytz.timezone('America/Sao_Paulo')
        self.nyse_tz = pytz.timezone('America/New_York')
    
    def is_market_open(self, symbol: str) -> bool:
        """
        Verifica se mercado do símbolo está aberto
        
        Args:
            symbol: Símbolo do ativo (ex: PETR4.SA, AAPL, BTCUSDT)
        
        Returns:
            True se mercado está aberto, False caso contrário
        """
        market = self._get_market_type(symbol)
        
        if market == 'CRYPTO':
            return True  # Crypto opera 24/7
        elif market == 'B3':
            return self._is_b3_open()
        elif market == 'NYSE':
            return self._is_nyse_open()
        
        return False
    
    def _get_market_type(self, symbol: str) -> MarketType:
        """
        Identifica tipo de mercado baseado no símbolo
        
        Args:
            symbol: Símbolo do ativo
        
        Returns:
            'B3', 'NYSE' ou 'CRYPTO'
        """
        if symbol.endswith('.SA'):
            return 'B3'
        elif symbol.endswith('USDT') or symbol.endswith('BTC') or symbol.endswith('ETH'):
            return 'CRYPTO'
        else:
            return 'NYSE'
    
    def _is_b3_open(self) -> bool:
        """
        Verifica se B3 está aberta
        
        Horário: Segunda a Sexta, 10:00 - 17:00 (Horário de Brasília)
        
        Returns:
            True se B3 está aberta
        """
        now = datetime.now(self.b3_tz)
        
        # Verificar dia da semana (0=Segunda, 6=Domingo)
        if now.weekday() >= 5:  # Sábado ou Domingo
            return False
        
        # Verificar horário (10h - 17h)
        hour = now.hour
        minute = now.minute
        
        if hour < 10:
            return False
        if hour >= 17:
            return False
        if hour == 17 and minute > 0:
            return False
        
        return True
    
    def _is_nyse_open(self) -> bool:
        """
        Verifica se NYSE está aberta
        
        Horário: Segunda a Sexta, 9:30 - 16:00 (Eastern Time)
        
        Returns:
            True se NYSE está aberta
        """
        now = datetime.now(self.nyse_tz)
        
        # Verificar dia da semana (0=Segunda, 6=Domingo)
        if now.weekday() >= 5:  # Sábado ou Domingo
            return False
        
        # Verificar horário (9:30 - 16:00)
        hour = now.hour
        minute = now.minute
        
        if hour < 9:
            return False
        if hour == 9 and minute < 30:
            return False
        if hour >= 16:
            return False
        
        return True
    
    def get_market_status(self, symbol: str) -> dict:
        """
        Retorna status detalhado do mercado
        
        Args:
            symbol: Símbolo do ativo
        
        Returns:
            Dict com informações do mercado
        """
        market = self._get_market_type(symbol)
        is_open = self.is_market_open(symbol)
        
        if market == 'B3':
            now = datetime.now(self.b3_tz)
            market_name = 'B3 (Brasil)'
            hours = '10:00 - 17:00 BRT'
        elif market == 'NYSE':
            now = datetime.now(self.nyse_tz)
            market_name = 'NYSE (EUA)'
            hours = '09:30 - 16:00 EST'
        else:
            now = datetime.now(pytz.UTC)
            market_name = 'Crypto'
            hours = '24/7'
        
        return {
            'symbol': symbol,
            'market': market,
            'market_name': market_name,
            'is_open': is_open,
            'hours': hours,
            'current_time': now.strftime('%Y-%m-%d %H:%M:%S %Z')
        }


# Instância global
market_hours = MarketHoursChecker()


if __name__ == '__main__':
    # Testes
    checker = MarketHoursChecker()
    
    test_symbols = [
        'PETR4.SA',  # B3
        'VALE3.SA',  # B3
        'AAPL',      # NYSE
        'TSLA',      # NYSE
        'BTCUSDT',   # Crypto
        'ETHUSDT',   # Crypto
    ]
    
    print("=== TESTE DE HORÁRIOS DE MERCADO ===\n")
    
    for symbol in test_symbols:
        status = checker.get_market_status(symbol)
        emoji = '✅' if status['is_open'] else '❌'
        print(f"{emoji} {status['symbol']}")
        print(f"   Mercado: {status['market_name']}")
        print(f"   Horário: {status['hours']}")
        print(f"   Status: {'ABERTO' if status['is_open'] else 'FECHADO'}")
        print(f"   Hora Atual: {status['current_time']}")
        print()
