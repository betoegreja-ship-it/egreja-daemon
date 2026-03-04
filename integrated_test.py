#!/usr/bin/env python3
"""
Integrated Test - Teste Completo do Sistema de Trading
Simula ciclos de trading com geração de alertas e dashboard
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict
import random
import numpy as np

from trade_executor import TradeExecutor, RiskManager
from alert_system import AlertSystem
from daily_dashboard import DailyDashboard

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IntegratedTradeTest:
    """Teste integrado do sistema de trading"""
    
    def __init__(self, api_key: str, api_secret: str):
        """Inicializar teste integrado"""
        # Não inicializar TradeExecutor em modo teste (evita conexão com Binance)
        self.alert_system = AlertSystem()
        self.dashboard = DailyDashboard()
        
        self.test_results = {
            'start_time': datetime.now().isoformat(),
            'cycles': [],
            'total_trades': 0,
            'total_pnl': 0
        }
        
        logger.info("✅ Teste Integrado Inicializado (Modo Simulado)")
    
    def simulate_trading_cycle(self, cycle_num: int) -> Dict:
        """Simular um ciclo de trading"""
        logger.info("\n" + "="*70)
        logger.info(f"CICLO DE TESTE #{cycle_num}")
        logger.info("="*70)
        
        cycle_data = {
            'cycle_num': cycle_num,
            'timestamp': datetime.now().isoformat(),
            'opportunities': [],
            'trades': [],
            'alerts': []
        }
        
        # 1. Simular oportunidades
        num_opportunities = random.randint(1, 5)
        logger.info(f"\n🔍 Simulando {num_opportunities} oportunidades...")
        
        symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT']
        selected_symbols = random.sample(symbols, min(num_opportunities, len(symbols)))
        
        for symbol in selected_symbols:
            signal = random.choice(['BUY', 'SELL'])
            confidence = random.uniform(0.70, 0.95)
            price = random.uniform(100, 50000)
            
            opportunity = {
                'symbol': symbol,
                'signal': signal,
                'confidence': confidence,
                'price': price
            }
            
            cycle_data['opportunities'].append(opportunity)
            
            # Alerta de oportunidade
            self.alert_system.alert_opportunity_found(symbol, signal, confidence, price)
            
            logger.info(f"✅ Oportunidade: {symbol} | {signal} | Confiança: {confidence:.1%}")
        
        # 2. Simular execução de trades
        num_trades = random.randint(0, len(cycle_data['opportunities']))
        logger.info(f"\n📈 Executando {num_trades} trades...")
        
        for i in range(num_trades):
            opp = cycle_data['opportunities'][i]
            entry_price = opp['price']
            
            if opp['signal'] == 'BUY':
                stop_loss = entry_price * 0.98
                take_profit = entry_price * 1.05
            else:
                stop_loss = entry_price * 1.02
                take_profit = entry_price * 0.95
            
            quantity = random.uniform(0.1, 1.0)
            
            trade = {
                'symbol': opp['symbol'],
                'side': opp['signal'],
                'entry_price': entry_price,
                'quantity': quantity,
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'position_value': entry_price * quantity
            }
            
            cycle_data['trades'].append(trade)
            
            # Alerta de trade executado
            self.alert_system.alert_trade_executed(
                opp['symbol'],
                opp['signal'],
                entry_price,
                quantity,
                stop_loss,
                take_profit
            )
            
            logger.info(f"✅ Trade Executado: {opp['symbol']} | {opp['signal']} | ${entry_price:,.2f}")
        
        # 3. Simular fechamento de trades
        num_closed = random.randint(0, num_trades)
        logger.info(f"\n💰 Fechando {num_closed} trades...")
        
        total_pnl = 0
        
        for i in range(num_closed):
            if i < len(cycle_data['trades']):
                trade = cycle_data['trades'][i]
                
                # Simular P&L
                pnl_pct = random.uniform(-2, 5)
                exit_price = trade['entry_price'] * (1 + pnl_pct / 100)
                pnl = (exit_price - trade['entry_price']) * trade['quantity']
                
                reason = random.choice(['TAKE_PROFIT', 'STOP_LOSS', 'MANUAL'])
                
                # Alerta de trade fechado
                self.alert_system.alert_trade_closed(
                    trade['symbol'],
                    exit_price,
                    pnl,
                    pnl_pct,
                    reason
                )
                
                total_pnl += pnl
                
                logger.info(f"✅ Trade Fechado: {trade['symbol']} | P&L: ${pnl:,.2f} ({pnl_pct:.2f}%)")
        
        # 4. Gerar resumo do ciclo
        summary = {
            'opportunities_found': len(cycle_data['opportunities']),
            'trades_executed': len(cycle_data['trades']),
            'trades_closed': num_closed,
            'cycle_pnl': total_pnl,
            'daily_summary': {
                'total_pnl': total_pnl,
                'total_pnl_pct': (total_pnl / 1000000) * 100,
                'win_trades': max(0, num_closed - random.randint(0, num_closed)),
                'loss_trades': random.randint(0, num_closed),
                'win_rate': random.uniform(40, 80),
                'open_positions': num_trades - num_closed,
                'daily_loss': max(0, -total_pnl) if total_pnl < 0 else 0,
                'remaining_capital': 1000000 - max(0, -total_pnl)
            }
        }
        
        cycle_data['summary'] = summary
        self.test_results['cycles'].append(cycle_data)
        self.test_results['total_trades'] += len(cycle_data['trades'])
        self.test_results['total_pnl'] += total_pnl
        
        # Salvar ciclo
        self._save_cycle(cycle_data)
        
        logger.info(f"\n📊 Ciclo P&L: ${total_pnl:,.2f}")
        
        return cycle_data
    
    def _save_cycle(self, cycle_data: Dict) -> None:
        """Salvar dados do ciclo"""
        os.makedirs("logs/trades", exist_ok=True)
        
        filename = f"logs/trades/cycle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w') as f:
            json.dump(cycle_data, f, indent=2, default=str)
        
        logger.info(f"✅ Ciclo salvo: {filename}")
    
    def run_full_test(self, num_cycles: int = 3) -> Dict:
        """Executar teste completo com múltiplos ciclos"""
        logger.info("\n\n" + "🚀"*35)
        logger.info("TESTE INTEGRADO DO SISTEMA DE TRADING (MODO SIMULADO)")
        logger.info("🚀"*35)
        
        # Executar ciclos
        for i in range(num_cycles):
            self.simulate_trading_cycle(i + 1)
        
        # Gerar dashboard
        logger.info("\n" + "="*70)
        logger.info("📊 GERANDO DASHBOARD")
        logger.info("="*70)
        
        report = self.dashboard.generate_daily_report()
        
        # Resumo final
        logger.info("\n" + "="*70)
        logger.info("📋 RESUMO DO TESTE")
        logger.info("="*70)
        logger.info(f"Ciclos Executados: {num_cycles}")
        logger.info(f"Total de Trades: {self.test_results['total_trades']}")
        logger.info(f"P&L Total: ${self.test_results['total_pnl']:,.2f}")
        logger.info(f"Dashboard: {report['html_file']}")
        logger.info("="*70 + "\n")
        
        # Salvar resultado do teste
        self.test_results['end_time'] = datetime.now().isoformat()
        self.test_results['dashboard'] = report
        
        self._save_test_results()
        
        return self.test_results
    
    def _save_test_results(self) -> None:
        """Salvar resultados do teste"""
        os.makedirs("data/tests", exist_ok=True)
        
        filename = f"data/tests/test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w') as f:
            json.dump(self.test_results, f, indent=2, default=str)
        
        logger.info(f"✅ Resultados do teste salvos: {filename}")


def main():
    """Executar teste integrado"""
    
    # Usar chaves de teste (não serão usadas em modo simulado)
    api_key = os.getenv('BINANCE_API_KEY', 'test_key')
    api_secret = os.getenv('BINANCE_API_SECRET', 'test_secret')
    
    # Criar teste integrado
    test = IntegratedTradeTest(api_key, api_secret)
    
    # Executar teste com 3 ciclos
    results = test.run_full_test(num_cycles=3)
    
    print("\n✅ TESTE INTEGRADO COMPLETO!")
    print(f"Total de Trades: {results['total_trades']}")
    print(f"P&L Total: ${results['total_pnl']:,.2f}")


if __name__ == "__main__":
    main()
