#!/usr/bin/env python3.11
"""
Engine de Backtesting Profissional
Testa estratégias com dados históricos reais e calcula métricas de performance
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime, timedelta
from data_collector import DataCollector
from professional_strategies import ProfessionalStrategies
import json

class BacktestingEngine:
    """Engine de backtesting com métricas profissionais"""
    
    def __init__(self, initial_capital: float = 100000.0):
        self.initial_capital = initial_capital
        self.data_collector = DataCollector()
        self.strategies = ProfessionalStrategies()
        
        # Custos de transação
        self.commission_pct = 0.001  # 0.1% por trade
        self.slippage_pct = 0.0005   # 0.05% slippage
        
    def calculate_sharpe_ratio(self, returns: pd.Series, risk_free_rate: float = 0.02) -> float:
        """
        Calcula Sharpe Ratio
        
        Args:
            returns: Série de retornos diários
            risk_free_rate: Taxa livre de risco anualizada
        
        Returns:
            Sharpe Ratio anualizado
        """
        if len(returns) == 0 or returns.std() == 0:
            return 0.0
        
        # Retorno médio anualizado
        mean_return = returns.mean() * 252
        
        # Volatilidade anualizada
        std_return = returns.std() * np.sqrt(252)
        
        sharpe = (mean_return - risk_free_rate) / std_return
        
        return sharpe
    
    def calculate_sortino_ratio(self, returns: pd.Series, risk_free_rate: float = 0.02) -> float:
        """
        Calcula Sortino Ratio (foca apenas no downside risk)
        """
        if len(returns) == 0:
            return 0.0
        
        # Retorno médio anualizado
        mean_return = returns.mean() * 252
        
        # Downside deviation (apenas retornos negativos)
        downside_returns = returns[returns < 0]
        if len(downside_returns) == 0:
            return np.inf
        
        downside_std = downside_returns.std() * np.sqrt(252)
        
        if downside_std == 0:
            return np.inf
        
        sortino = (mean_return - risk_free_rate) / downside_std
        
        return sortino
    
    def calculate_max_drawdown(self, equity_curve: pd.Series) -> Tuple[float, int]:
        """
        Calcula Maximum Drawdown e duração
        
        Returns:
            (max_drawdown_pct, duration_days)
        """
        if len(equity_curve) == 0:
            return 0.0, 0
        
        # Calcular running maximum
        running_max = equity_curve.expanding().max()
        
        # Drawdown em cada ponto
        drawdown = (equity_curve - running_max) / running_max
        
        # Maximum drawdown
        max_dd = drawdown.min()
        
        # Duração do drawdown
        dd_duration = 0
        current_duration = 0
        
        for dd in drawdown:
            if dd < 0:
                current_duration += 1
                dd_duration = max(dd_duration, current_duration)
            else:
                current_duration = 0
        
        return abs(max_dd), dd_duration
    
    def calculate_calmar_ratio(self, returns: pd.Series, max_drawdown: float) -> float:
        """
        Calcula Calmar Ratio = CAGR / Max Drawdown
        """
        if max_drawdown == 0:
            return np.inf
        
        cagr = (1 + returns.mean()) ** 252 - 1
        
        return cagr / max_drawdown
    
    def backtest_mean_reversion(self, symbol: str, period: str = '1y') -> Dict:
        """
        Backtest de estratégia Mean Reversion
        
        Returns:
            Dict com métricas de performance
        """
        print(f"\n{'='*70}")
        print(f"BACKTESTING: Mean Reversion - {symbol}")
        print(f"{'='*70}")
        
        # Buscar dados históricos
        df = self.data_collector.fetch_ohlcv(symbol, period=period, interval='1d')
        
        if df.empty or len(df) < 50:
            print("❌ Dados insuficientes")
            return None
        
        # Calcular indicadores
        df = self.strategies.calculate_technical_indicators(df)
        
        # Inicializar variáveis de trading
        capital = self.initial_capital
        position = 0  # 0 = sem posição, 1 = long, -1 = short
        entry_price = 0
        trades = []
        equity_curve = []
        
        # Simular trading
        for i in range(50, len(df)):
            current_price = df['Close'].iloc[i]
            current_rsi = df['RSI'].iloc[i]
            current_bb_pos = df['BB_position'].iloc[i]
            
            # Calcular z-score
            price_mean = df['Close'].iloc[i-50:i].mean()
            price_std = df['Close'].iloc[i-50:i].std()
            z_score = (current_price - price_mean) / price_std if price_std > 0 else 0
            
            # Lógica de entrada
            if position == 0:
                # Entrar LONG se oversold
                if (current_rsi < 30 or z_score < -2 or current_bb_pos < 0.1):
                    position = 1
                    entry_price = current_price * (1 + self.commission_pct + self.slippage_pct)
                    shares = capital * 0.2 / entry_price  # 20% do capital
                    
                    trades.append({
                        'date': df.index[i],
                        'type': 'BUY',
                        'price': entry_price,
                        'shares': shares,
                        'rsi': current_rsi,
                        'z_score': z_score
                    })
                
                # Entrar SHORT se overbought
                elif (current_rsi > 70 or z_score > 2 or current_bb_pos > 0.9):
                    position = -1
                    entry_price = current_price * (1 - self.commission_pct - self.slippage_pct)
                    shares = capital * 0.2 / entry_price
                    
                    trades.append({
                        'date': df.index[i],
                        'type': 'SHORT',
                        'price': entry_price,
                        'shares': shares,
                        'rsi': current_rsi,
                        'z_score': z_score
                    })
            
            # Lógica de saída
            elif position == 1:  # Long position
                # Sair se RSI normalizar ou z-score reverter
                if current_rsi > 50 or z_score > -0.5:
                    exit_price = current_price * (1 - self.commission_pct - self.slippage_pct)
                    pnl = (exit_price - entry_price) * shares
                    capital += pnl
                    
                    trades.append({
                        'date': df.index[i],
                        'type': 'SELL',
                        'price': exit_price,
                        'shares': shares,
                        'pnl': pnl,
                        'return_pct': (exit_price / entry_price - 1) * 100
                    })
                    
                    position = 0
                    shares = 0
            
            elif position == -1:  # Short position
                # Sair se RSI normalizar ou z-score reverter
                if current_rsi < 50 or z_score < 0.5:
                    exit_price = current_price * (1 + self.commission_pct + self.slippage_pct)
                    pnl = (entry_price - exit_price) * shares
                    capital += pnl
                    
                    trades.append({
                        'date': df.index[i],
                        'type': 'COVER',
                        'price': exit_price,
                        'shares': shares,
                        'pnl': pnl,
                        'return_pct': (entry_price / exit_price - 1) * 100
                    })
                    
                    position = 0
                    shares = 0
            
            # Registrar equity
            if position != 0:
                unrealized_pnl = (current_price - entry_price) * shares * position
                equity_curve.append(capital + unrealized_pnl)
            else:
                equity_curve.append(capital)
        
        # Calcular métricas
        equity_series = pd.Series(equity_curve, index=df.index[50:])
        returns = equity_series.pct_change().dropna()
        
        # Filtrar trades completos (com PnL)
        completed_trades = [t for t in trades if 'pnl' in t]
        
        if len(completed_trades) == 0:
            print("❌ Nenhum trade completo")
            return None
        
        total_return = (capital - self.initial_capital) / self.initial_capital
        num_trades = len(completed_trades)
        winning_trades = [t for t in completed_trades if t['pnl'] > 0]
        losing_trades = [t for t in completed_trades if t['pnl'] < 0]
        
        win_rate = len(winning_trades) / num_trades if num_trades > 0 else 0
        
        avg_win = np.mean([t['pnl'] for t in winning_trades]) if winning_trades else 0
        avg_loss = np.mean([t['pnl'] for t in losing_trades]) if losing_trades else 0
        
        profit_factor = abs(avg_win * len(winning_trades) / (avg_loss * len(losing_trades))) if losing_trades and avg_loss != 0 else np.inf
        
        sharpe = self.calculate_sharpe_ratio(returns)
        sortino = self.calculate_sortino_ratio(returns)
        max_dd, dd_duration = self.calculate_max_drawdown(equity_series)
        calmar = self.calculate_calmar_ratio(returns, max_dd)
        
        metrics = {
            'symbol': symbol,
            'strategy': 'mean_reversion',
            'period': period,
            'initial_capital': self.initial_capital,
            'final_capital': capital,
            'total_return': total_return,
            'total_return_pct': total_return * 100,
            'num_trades': num_trades,
            'win_rate': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
            'sharpe_ratio': sharpe,
            'sortino_ratio': sortino,
            'max_drawdown': max_dd,
            'max_drawdown_pct': max_dd * 100,
            'drawdown_duration_days': dd_duration,
            'calmar_ratio': calmar,
            'trades': completed_trades
        }
        
        # Exibir resumo
        print(f"\n📊 RESULTADOS:")
        print(f"   Capital Inicial: ${self.initial_capital:,.2f}")
        print(f"   Capital Final: ${capital:,.2f}")
        print(f"   Retorno Total: {total_return*100:.2f}%")
        print(f"   Número de Trades: {num_trades}")
        print(f"   Win Rate: {win_rate*100:.1f}%")
        print(f"   Profit Factor: {profit_factor:.2f}")
        print(f"   Sharpe Ratio: {sharpe:.2f}")
        print(f"   Sortino Ratio: {sortino:.2f}")
        print(f"   Max Drawdown: {max_dd*100:.2f}%")
        print(f"   Calmar Ratio: {calmar:.2f}")
        
        return metrics
    
    def run_full_backtest(self, symbols: List[str] = None, period: str = '1y') -> Dict:
        """
        Executa backtest completo em múltiplos símbolos
        
        Returns:
            Dict com resultados agregados
        """
        if symbols is None:
            symbols = self.data_collector.get_all_symbols()
        
        print("\n" + "="*70)
        print("BACKTESTING COMPLETO - ESTRATÉGIAS PROFISSIONAIS")
        print("="*70)
        
        all_results = []
        
        for symbol in symbols:
            result = self.backtest_mean_reversion(symbol, period)
            if result:
                all_results.append(result)
        
        # Agregar resultados
        if not all_results:
            print("\n❌ Nenhum resultado válido")
            return None
        
        avg_return = np.mean([r['total_return'] for r in all_results])
        avg_sharpe = np.mean([r['sharpe_ratio'] for r in all_results])
        avg_win_rate = np.mean([r['win_rate'] for r in all_results])
        
        summary = {
            'num_symbols': len(all_results),
            'avg_return': avg_return,
            'avg_return_pct': avg_return * 100,
            'avg_sharpe_ratio': avg_sharpe,
            'avg_win_rate': avg_win_rate,
            'best_performer': max(all_results, key=lambda x: x['total_return']),
            'worst_performer': min(all_results, key=lambda x: x['total_return']),
            'all_results': all_results
        }
        
        print(f"\n{'='*70}")
        print("RESUMO GERAL")
        print(f"{'='*70}")
        print(f"Símbolos Testados: {len(all_results)}")
        print(f"Retorno Médio: {avg_return*100:.2f}%")
        print(f"Sharpe Ratio Médio: {avg_sharpe:.2f}")
        print(f"Win Rate Médio: {avg_win_rate*100:.1f}%")
        print(f"\nMelhor: {summary['best_performer']['symbol']} ({summary['best_performer']['total_return_pct']:.2f}%)")
        print(f"Pior: {summary['worst_performer']['symbol']} ({summary['worst_performer']['total_return_pct']:.2f}%)")
        
        return summary

if __name__ == "__main__":
    engine = BacktestingEngine(initial_capital=100000.0)
    
    # Testar alguns símbolos
    test_symbols = ['SPY', 'QQQ', 'GLD', 'SLV']
    
    results = engine.run_full_backtest(symbols=test_symbols, period='1y')
    
    if results:
        # Salvar resultados
        output_file = '/home/ubuntu/backtest_results.json'
        
        # Converter para formato serializável
        def convert_for_json(obj):
            if isinstance(obj, pd.Timestamp):
                return obj.isoformat()
            elif isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {k: convert_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_for_json(item) for item in obj]
            return obj
        
        results_json = convert_for_json(results)
        
        with open(output_file, 'w') as f:
            json.dump(results_json, f, indent=2)
        
        print(f"\n✓ Resultados salvos em: {output_file}")
