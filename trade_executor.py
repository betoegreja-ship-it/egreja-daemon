#!/usr/bin/env python3
"""
Trade Executor - Sistema de Execução Automática de Trades
Gerenciamento de risco profissional com stop-loss e position sizing
"""

import os
import json
import logging
from datetime import datetime, timedelta
import numpy as np
from binance_brasil_client import BinanceBrasilClient
from professional_strategies import ProfessionalStrategies

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RiskManager:
    """Gerenciador de risco profissional"""
    
    def __init__(self, daily_capital=1000000, max_loss_pct=0.05, max_position_pct=0.30):
        """
        Inicializar gerenciador de risco
        
        Args:
            daily_capital: Capital diário em USD
            max_loss_pct: Máximo de perda permitida (5%)
            max_position_pct: Máximo por operação (30%)
        """
        self.daily_capital = daily_capital
        self.max_loss_pct = max_loss_pct
        self.max_position_pct = max_position_pct
        self.max_loss_amount = daily_capital * max_loss_pct
        self.max_position_amount = daily_capital * max_position_pct
        
        self.daily_loss = 0
        self.open_positions = []
        self.closed_trades = []
        
        logger.info(f"✅ Risk Manager Inicializado")
        logger.info(f"   Capital Diário: ${daily_capital:,.0f}")
        logger.info(f"   Máximo de Perda: ${self.max_loss_amount:,.0f} ({max_loss_pct:.1%})")
        logger.info(f"   Máximo por Operação: ${self.max_position_amount:,.0f} ({max_position_pct:.1%})")
    
    def can_trade(self):
        """Verificar se pode abrir nova posição"""
        if self.daily_loss >= self.max_loss_amount:
            logger.warning(f"⛔ Limite de perda diária atingido: ${self.daily_loss:,.0f}")
            return False
        return True
    
    def calculate_position_size(self, entry_price, stop_loss_price):
        """Calcular tamanho da posição baseado em risco"""
        risk_per_trade = self.max_position_amount
        price_risk = abs(entry_price - stop_loss_price)
        
        if price_risk == 0:
            return 0
        
        position_size = risk_per_trade / price_risk
        return position_size
    
    def add_position(self, symbol, entry_price, stop_loss, quantity, side='BUY'):
        """Adicionar posição aberta"""
        position = {
            'symbol': symbol,
            'entry_price': entry_price,
            'stop_loss': stop_loss,
            'quantity': quantity,
            'side': side,
            'entry_time': datetime.now().isoformat(),
            'entry_amount': entry_price * quantity
        }
        self.open_positions.append(position)
        logger.info(f"📊 Posição Aberta: {symbol} | Qtd: {quantity:.4f} | Preço: ${entry_price:,.2f}")
        return position
    
    def close_position(self, symbol, exit_price, reason='MANUAL'):
        """Fechar posição"""
        for i, pos in enumerate(self.open_positions):
            if pos['symbol'] == symbol:
                exit_amount = exit_price * pos['quantity']
                pnl = exit_amount - pos['entry_amount']
                pnl_pct = (pnl / pos['entry_amount']) * 100
                
                trade = {
                    'symbol': symbol,
                    'entry_price': pos['entry_price'],
                    'exit_price': exit_price,
                    'quantity': pos['quantity'],
                    'entry_amount': pos['entry_amount'],
                    'exit_amount': exit_amount,
                    'pnl': pnl,
                    'pnl_pct': pnl_pct,
                    'reason': reason,
                    'entry_time': pos['entry_time'],
                    'exit_time': datetime.now().isoformat()
                }
                
                self.closed_trades.append(trade)
                self.daily_loss += pnl if pnl < 0 else 0
                
                logger.info(f"✅ Posição Fechada: {symbol} | P&L: ${pnl:,.2f} ({pnl_pct:.2f}%)")
                
                self.open_positions.pop(i)
                return trade
        
        return None
    
    def get_daily_summary(self):
        """Obter resumo diário"""
        total_pnl = sum(t['pnl'] for t in self.closed_trades)
        win_trades = len([t for t in self.closed_trades if t['pnl'] > 0])
        loss_trades = len([t for t in self.closed_trades if t['pnl'] < 0])
        
        return {
            'total_pnl': total_pnl,
            'total_pnl_pct': (total_pnl / self.daily_capital) * 100,
            'win_trades': win_trades,
            'loss_trades': loss_trades,
            'win_rate': (win_trades / len(self.closed_trades) * 100) if self.closed_trades else 0,
            'open_positions': len(self.open_positions),
            'daily_loss': self.daily_loss,
            'remaining_capital': self.daily_capital - self.daily_loss
        }


class TradeExecutor:
    """Executor de trades automático"""
    
    def __init__(self, api_key, api_secret, daily_capital=1000000):
        """Inicializar executor de trades"""
        self.binance = BinanceBrasilClient(api_key, api_secret)
        self.strategies = ProfessionalStrategies()
        self.risk_manager = RiskManager(daily_capital)
        
        self.logs_dir = "logs/trades"
        self.data_dir = "data/trades"
        
        os.makedirs(self.logs_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)
        
        logger.info("✅ Trade Executor Inicializado")
    
    def find_opportunities(self):
        """Encontrar oportunidades de trading"""
        logger.info("\n" + "="*70)
        logger.info("🔍 PROCURANDO OPORTUNIDADES")
        logger.info("="*70)
        
        opportunities = []
        symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT']
        
        for symbol in symbols:
            try:
                # Analisar usando mean reversion signal
                signal = self.strategies.mean_reversion_signal(symbol, period='1mo')
                
                if signal and signal.get('confidence', 0) > 0.70:
                    opportunity = {
                        'symbol': symbol,
                        'signal': signal.get('signal', 'NEUTRAL'),
                        'confidence': signal.get('confidence', 0),
                        'current_price': signal.get('current_price', 0),
                        'rsi': signal.get('rsi', 0),
                        'z_score': signal.get('z_score', 0),
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    opportunities.append(opportunity)
                    logger.info(f"✅ OPORTUNIDADE: {symbol} | {signal.get('signal')} | Confiança: {signal.get('confidence'):.1%}")
            
            except Exception as e:
                logger.error(f"❌ Erro ao analisar {symbol}: {e}")
        
        logger.info(f"\n📊 Total de oportunidades: {len(opportunities)}")
        return opportunities
    
    def execute_trade(self, opportunity):
        """Executar trade baseado em oportunidade"""
        if not self.risk_manager.can_trade():
            logger.warning(f"⛔ Não pode fazer trade - limite de risco atingido")
            return None
        
        symbol = opportunity['symbol']
        signal = opportunity['signal']
        confidence = opportunity['confidence']
        current_price = opportunity['current_price']
        
        logger.info(f"\n📈 EXECUTANDO TRADE: {symbol}")
        logger.info(f"   Direção: {signal}")
        logger.info(f"   Confiança: {confidence:.1%}")
        logger.info(f"   Preço: ${current_price:,.2f}")
        
        # Calcular stop-loss (2% de risco)
        if signal == 'BUY':
            stop_loss = current_price * 0.98  # 2% abaixo
            take_profit = current_price * 1.05  # 5% acima
        else:
            stop_loss = current_price * 1.02  # 2% acima
            take_profit = current_price * 0.95  # 5% abaixo
        
        # Calcular tamanho da posição
        quantity = self.risk_manager.calculate_position_size(current_price, stop_loss)
        
        if quantity <= 0:
            logger.warning(f"❌ Tamanho de posição inválido: {quantity}")
            return None
        
        # Adicionar posição
        position = self.risk_manager.add_position(
            symbol=symbol,
            entry_price=current_price,
            stop_loss=stop_loss,
            quantity=quantity,
            side=signal
        )
        
        logger.info(f"   Stop Loss: ${stop_loss:,.2f}")
        logger.info(f"   Take Profit: ${take_profit:,.2f}")
        logger.info(f"   Quantidade: {quantity:.4f}")
        logger.info(f"   Valor: ${current_price * quantity:,.2f}")
        
        # Simular execução (em produção, seria real)
        trade_result = {
            'status': 'EXECUTED',
            'symbol': symbol,
            'side': signal,
            'entry_price': current_price,
            'quantity': quantity,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'timestamp': datetime.now().isoformat()
        }
        
        return trade_result
    
    def close_profitable_trades(self):
        """Fechar trades lucrativos"""
        logger.info("\n" + "="*70)
        logger.info("💰 VERIFICANDO TRADES LUCRATIVOS")
        logger.info("="*70)
        
        closed_trades = []
        
        for position in self.risk_manager.open_positions[:]:  # Cópia para iterar
            try:
                # Obter preço atual
                klines = self.binance.get_klines(position['symbol'], '1h', limit=1)
                if not klines:
                    continue
                
                current_price = float(klines[0][4])
                pnl = (current_price - position['entry_price']) * position['quantity']
                pnl_pct = (pnl / position['entry_amount']) * 100
                
                # Fechar se atingir take profit ou stop loss
                if position['side'] == 'BUY':
                    if current_price >= position['entry_price'] * 1.05:  # Take profit
                        trade = self.risk_manager.close_position(
                            position['symbol'],
                            current_price,
                            'TAKE_PROFIT'
                        )
                        if trade:
                            closed_trades.append(trade)
                    elif current_price <= position['stop_loss']:  # Stop loss
                        trade = self.risk_manager.close_position(
                            position['symbol'],
                            current_price,
                            'STOP_LOSS'
                        )
                        if trade:
                            closed_trades.append(trade)
            
            except Exception as e:
                logger.error(f"❌ Erro ao fechar {position['symbol']}: {e}")
        
        return closed_trades
    
    def run_trading_cycle(self):
        """Executar ciclo completo de trading"""
        logger.info("\n\n" + "🚀"*35)
        logger.info("CICLO DE TRADING AUTOMÁTICO")
        logger.info("🚀"*35)
        
        cycle_result = {
            'timestamp': datetime.now().isoformat(),
            'opportunities_found': 0,
            'trades_executed': 0,
            'trades_closed': 0,
            'daily_summary': {}
        }
        
        # 1. Encontrar oportunidades
        opportunities = self.find_opportunities()
        cycle_result['opportunities_found'] = len(opportunities)
        
        # 2. Executar trades
        for opportunity in opportunities:
            trade = self.execute_trade(opportunity)
            if trade:
                cycle_result['trades_executed'] += 1
        
        # 3. Fechar trades lucrativos
        closed = self.close_profitable_trades()
        cycle_result['trades_closed'] = len(closed)
        
        # 4. Gerar resumo diário
        summary = self.risk_manager.get_daily_summary()
        cycle_result['daily_summary'] = summary
        
        # Salvar resultado
        result_file = f"{self.logs_dir}/cycle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(result_file, 'w') as f:
            json.dump(cycle_result, f, indent=2, default=str)
        
        # Log do resumo
        logger.info("\n" + "="*70)
        logger.info("📊 RESUMO DO CICLO")
        logger.info("="*70)
        logger.info(f"Oportunidades: {cycle_result['opportunities_found']}")
        logger.info(f"Trades Executados: {cycle_result['trades_executed']}")
        logger.info(f"Trades Fechados: {cycle_result['trades_closed']}")
        logger.info(f"P&L Diário: ${summary['total_pnl']:,.2f} ({summary['total_pnl_pct']:.2f}%)")
        logger.info(f"Win Rate: {summary['win_rate']:.1f}%")
        logger.info(f"Posições Abertas: {summary['open_positions']}")
        logger.info("="*70 + "\n")
        
        return cycle_result


def main():
    """Executar executor de trades"""
    
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')
    
    if not api_key or not api_secret:
        logger.error("❌ Chaves de API não encontradas")
        return
    
    # Criar executor
    executor = TradeExecutor(api_key, api_secret, daily_capital=1000000)
    
    # Executar ciclo
    result = executor.run_trading_cycle()
    
    print("\n✅ TRADING AUTOMÁTICO COMPLETO!")


if __name__ == "__main__":
    main()
