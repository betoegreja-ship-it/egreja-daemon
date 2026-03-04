#!/usr/bin/env python3
"""
Backtesting Engine
Valida estratégia de trading nos últimos 6 meses
Calcula: Win Rate, Profit Factor, Sharpe Ratio, Max Drawdown
"""

import os
import mysql.connector
from datetime import datetime, timedelta
import numpy as np
from collections import defaultdict

class BacktestingEngine:
    def __init__(self):
        self.db_config = {
            'host': os.environ.get('MYSQLHOST', 'localhost'),
            'user': os.environ.get('MYSQLUSER', 'root'),
            'password': os.environ.get('MYSQLPASSWORD', ''),
            'database': os.environ.get('MYSQLDATABASE', 'railway'),
            'port': int(os.environ.get('MYSQLPORT', '3306')),
        }
    
    def get_db(self):
        """Conecta ao banco"""
        try:
            return mysql.connector.connect(**self.db_config)
        except Exception as e:
            print(f"❌ Erro ao conectar MySQL: {e}")
            return None
    
    def get_historical_signals(self, days=180):
        """Busca sinais dos últimos N dias"""
        conn = self.get_db()
        if not conn:
            return []
        
        cursor = conn.cursor(dictionary=True)
        date_from = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        try:
            cursor.execute(f"""
                SELECT symbol, market_type, price, score, signal, created_at
                FROM market_signals
                WHERE created_at >= '{date_from}'
                ORDER BY symbol, created_at
            """)
            signals = cursor.fetchall()
            cursor.close()
            conn.close()
            return signals
        except Exception as e:
            print(f"❌ Erro ao buscar histórico: {e}")
            return []
    
    def generate_trades(self, signals):
        """Gera trades baseado em sinais históricos"""
        trades = []
        open_positions = {}
        
        for signal in signals:
            symbol = signal['symbol']
            score = signal['score']
            price = float(signal['price'])
            timestamp = signal['created_at']
            
            # Se há posição aberta e sinal muda
            if symbol in open_positions:
                open_trade = open_positions[symbol]
                
                # Fechar posição se sinal muda de direção
                if (open_trade['direction'] == 'BUY' and score < 50) or \
                   (open_trade['direction'] == 'SELL' and score > 50):
                    
                    # Calcular P&L
                    if open_trade['direction'] == 'BUY':
                        pnl = (price - open_trade['entry_price']) / open_trade['entry_price']
                    else:  # SELL
                        pnl = (open_trade['entry_price'] - price) / open_trade['entry_price']
                    
                    trade = {
                        'symbol': symbol,
                        'direction': open_trade['direction'],
                        'entry_price': open_trade['entry_price'],
                        'exit_price': price,
                        'entry_date': open_trade['entry_date'],
                        'exit_date': timestamp,
                        'pnl': pnl,
                        'win': pnl > 0
                    }
                    trades.append(trade)
                    del open_positions[symbol]
            
            # Abrir nova posição se score extremo
            if symbol not in open_positions:
                if score > 80:
                    open_positions[symbol] = {
                        'direction': 'BUY',
                        'entry_price': price,
                        'entry_date': timestamp
                    }
                elif score < 20:
                    open_positions[symbol] = {
                        'direction': 'SELL',
                        'entry_price': price,
                        'entry_date': timestamp
                    }
        
        return trades
    
    def calculate_metrics(self, trades):
        """Calcula métricas de performance"""
        if not trades:
            return None
        
        wins = [t for t in trades if t['win']]
        losses = [t for t in trades if not t['win']]
        
        # Básicas
        total_trades = len(trades)
        win_rate = len(wins) / total_trades if total_trades > 0 else 0
        
        # P&L
        total_pnl = sum(t['pnl'] for t in trades)
        avg_pnl = total_pnl / total_trades if total_trades > 0 else 0
        
        # Profit Factor
        win_sum = sum(t['pnl'] for t in wins) if wins else 0
        loss_sum = sum(abs(t['pnl']) for t in losses) if losses else 0
        profit_factor = win_sum / loss_sum if loss_sum > 0 else win_sum
        
        # Drawdown
        pnl_series = np.array([t['pnl'] for t in trades])
        cumulative_pnl = np.cumsum(pnl_series)
        running_max = np.maximum.accumulate(cumulative_pnl)
        drawdown = (cumulative_pnl - running_max) / (np.abs(running_max) + 1e-8)
        max_drawdown = np.min(drawdown) if len(drawdown) > 0 else 0
        
        # Sharpe Ratio (assuming 252 trading days/year)
        if len(pnl_series) > 1:
            daily_returns = pnl_series
            sharpe_ratio = np.mean(daily_returns) / (np.std(daily_returns) + 1e-8) * np.sqrt(252)
        else:
            sharpe_ratio = 0
        
        return {
            'total_trades': total_trades,
            'winning_trades': len(wins),
            'losing_trades': len(losses),
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'profit_factor': profit_factor,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe_ratio,
            'best_trade': max(t['pnl'] for t in trades) if trades else 0,
            'worst_trade': min(t['pnl'] for t in trades) if trades else 0,
        }
    
    def generate_report(self, days=180):
        """Gera relatório completo de backtesting"""
        print(f"\n{'='*70}")
        print(f"📈 BACKTESTING ENGINE - Últimos {days} dias")
        print(f"{'='*70}\n")
        
        # Buscar histórico
        print(f"📊 Buscando {days} dias de histórico...")
        signals = self.get_historical_signals(days)
        
        if not signals:
            print("❌ Nenhum sinal histórico encontrado")
            return None
        
        print(f"✅ {len(signals)} sinais encontrados\n")
        
        # Gerar trades
        print("🎯 Simulando trades...")
        trades = self.generate_trades(signals)
        print(f"✅ {len(trades)} trades gerados\n")
        
        if not trades:
            print("❌ Nenhum trade gerado (banco de dados muito novo?)")
            return None
        
        # Calcular métricas
        metrics = self.calculate_metrics(trades)
        
        # Exibir relatório
        print(f"{'='*70}")
        print("📊 MÉTRICAS DE PERFORMANCE")
        print(f"{'='*70}\n")
        
        print(f"Total de Trades: {metrics['total_trades']}")
        print(f"Trades Vencedores: {metrics['winning_trades']} ({metrics['win_rate']*100:.1f}%)")
        print(f"Trades Perdedores: {metrics['losing_trades']} ({(1-metrics['win_rate'])*100:.1f}%)")
        print(f"\n{'─'*70}\n")
        
        print(f"💰 P&L Total: {metrics['total_pnl']*100:+.2f}%")
        print(f"📊 P&L Médio por Trade: {metrics['avg_pnl']*100:+.2f}%")
        print(f"📈 Melhor Trade: {metrics['best_trade']*100:+.2f}%")
        print(f"📉 Pior Trade: {metrics['worst_trade']*100:+.2f}%")
        print(f"\n{'─'*70}\n")
        
        print(f"🎯 Profit Factor: {metrics['profit_factor']:.2f}x")
        print(f"   (> 1.5 é bom, > 2.0 é excelente)")
        print(f"\n📉 Max Drawdown: {metrics['max_drawdown']*100:.2f}%")
        print(f"   (Pior perda da estratégia)")
        print(f"\n📊 Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")
        print(f"   (> 1.0 é bom, > 2.0 é excelente)")
        
        print(f"\n{'='*70}")
        print("✅ RECOMENDAÇÕES")
        print(f"{'='*70}\n")
        
        if metrics['win_rate'] > 0.55:
            print("🟢 WIN RATE excelente! Estratégia é lucrativa.")
        elif metrics['win_rate'] > 0.50:
            print("🟡 WIN RATE aceitável. Estratégia pode funcionar.")
        else:
            print("🔴 WIN RATE baixo. Considere ajustar parâmetros.")
        
        if metrics['profit_factor'] > 1.5:
            print("🟢 PROFIT FACTOR forte. Ganhos superam perdas.")
        elif metrics['profit_factor'] > 1.0:
            print("🟡 PROFIT FACTOR positivo mas fraco.")
        else:
            print("🔴 PROFIT FACTOR ruim. Perdas superam ganhos.")
        
        if metrics['max_drawdown'] > -0.20:
            print("🟢 DRAWDOWN controlado. Risco gerenciável.")
        elif metrics['max_drawdown'] > -0.50:
            print("🟡 DRAWDOWN moderado. Risco aceitável.")
        else:
            print("🔴 DRAWDOWN alto. Risco muito elevado.")
        
        print(f"\n{'='*70}\n")
        
        return metrics

# Teste
def test_backtesting():
    """Testa backtesting engine"""
    engine = BacktestingEngine()
    engine.generate_report(days=180)

if __name__ == '__main__':
    test_backtesting()
