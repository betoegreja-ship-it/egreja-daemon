#!/usr/bin/env python3.11
"""
ArbitrageAI v2 - Advanced Backtesting com VectorBT
Otimização rápida de parâmetros para múltiplas estratégias
"""

import sys
sys.path.insert(0, '/home/ubuntu/arbitrage-dashboard')

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import json
import yfinance as yf
import warnings
warnings.filterwarnings('ignore')

class AdvancedBacktester:
    """Backtesting profissional com múltiplas estratégias"""
    
    def __init__(self):
        self.symbols = {
            'crypto': ['ETH-USD', 'BTC-USD'],
            'stocks': ['QQQ', 'SPY', 'GLD', 'SLV'],
            'pairs': [('QQQ', 'SPY'), ('GLD', 'SLV')]
        }
        
    def fetch_data(self, symbols, days=365):
        """Buscar dados históricos"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        dfs = []
        for symbol in symbols:
            try:
                df = yf.download(symbol, start=start_date, end=end_date, progress=False)
                close_col = 'Adj Close' if 'Adj Close' in df.columns else 'Close'
                df_clean = df[[close_col]].copy()
                df_clean.columns = [symbol]
                dfs.append(df_clean)
            except:
                pass
        
        if not dfs:
            return pd.DataFrame()
        
        return pd.concat(dfs, axis=1)
    
    def mean_reversion_strategy(self, prices, rsi_period=14, rsi_oversold=30, rsi_overbought=70):
        """Estratégia de Mean Reversion com RSI"""
        # Calcular RSI
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=rsi_period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=rsi_period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        # Gerar sinais
        signals = pd.Series(0, index=prices.index)
        signals[rsi < rsi_oversold] = 1  # LONG
        signals[rsi > rsi_overbought] = -1  # SHORT
        
        return signals, rsi
    
    def pairs_trading_strategy(self, price1, price2, lookback=60, entry_z=2.0, exit_z=0.5):
        """Estratégia de Pairs Trading com z-score"""
        # Calcular spread
        log_price1 = np.log(price1)
        log_price2 = np.log(price2)
        
        # Regressão para hedge ratio
        from scipy import stats as sp_stats
        slope, _, _, _, _ = sp_stats.linregress(log_price2.iloc[-lookback:], log_price1.iloc[-lookback:])
        
        # Spread
        spread = log_price1 - slope * log_price2
        
        # Z-score
        rolling_mean = spread.rolling(window=lookback).mean()
        rolling_std = spread.rolling(window=lookback).std()
        z_score = (spread - rolling_mean) / rolling_std
        
        # Sinais
        signals = pd.Series(0, index=price1.index)
        signals[z_score < -entry_z] = 1  # LONG price1, SHORT price2
        signals[z_score > entry_z] = -1  # SHORT price1, LONG price2
        signals[np.abs(z_score) < exit_z] = 0  # EXIT
        
        return signals, z_score
    
    def backtest_mean_reversion(self, symbol, initial_capital=100000):
        """Backtest de Mean Reversion"""
        print(f"\n{'='*70}")
        print(f"BACKTEST: Mean Reversion - {symbol}")
        print(f"{'='*70}")
        
        # Buscar dados
        df = self.fetch_data([symbol], days=365)
        if df.empty:
            print(f"✗ Dados não disponíveis para {symbol}")
            return None
        
        prices = df[symbol]
        signals, rsi = self.mean_reversion_strategy(prices)
        
        # Simular trades
        position = 0
        cash = initial_capital
        portfolio_value = [initial_capital]
        trades = []
        
        for i in range(1, len(prices)):
            signal = signals.iloc[i]
            price = prices.iloc[i]
            
            if signal == 1 and position == 0:  # LONG
                shares = cash / price
                position = shares
                cash = 0
                trades.append({'date': prices.index[i], 'type': 'BUY', 'price': price, 'shares': shares})
            
            elif signal == -1 and position > 0:  # EXIT
                cash = position * price
                trades.append({'date': prices.index[i], 'type': 'SELL', 'price': price, 'shares': position})
                position = 0
            
            # Calcular valor do portfólio
            if position > 0:
                portfolio_value.append(position * price + cash)
            else:
                portfolio_value.append(cash)
        
        # Calcular métricas
        portfolio_values = np.array(portfolio_value)
        returns = np.diff(portfolio_values) / portfolio_values[:-1]
        
        total_return = (portfolio_values[-1] - initial_capital) / initial_capital
        annual_return = total_return  # Simplificado
        sharpe_ratio = np.mean(returns) / np.std(returns) * np.sqrt(252) if np.std(returns) > 0 else 0
        max_drawdown = np.min((portfolio_values - np.maximum.accumulate(portfolio_values)) / np.maximum.accumulate(portfolio_values))
        win_rate = len([t for t in trades if t['type'] == 'SELL']) / max(len([t for t in trades if t['type'] == 'BUY']), 1)
        
        print(f"✓ Trades executados: {len(trades)}")
        print(f"✓ Retorno total: {total_return*100:.2f}%")
        print(f"✓ Sharpe Ratio: {sharpe_ratio:.2f}")
        print(f"✓ Max Drawdown: {max_drawdown*100:.2f}%")
        print(f"✓ Win Rate: {win_rate*100:.1f}%")
        
        return {
            'symbol': symbol,
            'strategy': 'Mean Reversion',
            'total_return': total_return,
            'annual_return': annual_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'num_trades': len(trades),
            'final_value': float(portfolio_values[-1])
        }
    
    def backtest_pairs_trading(self, symbol1, symbol2, initial_capital=100000):
        """Backtest de Pairs Trading"""
        print(f"\n{'='*70}")
        print(f"BACKTEST: Pairs Trading - {symbol1} vs {symbol2}")
        print(f"{'='*70}")
        
        # Buscar dados
        df = self.fetch_data([symbol1, symbol2], days=365)
        if df.empty or len(df.columns) < 2:
            print(f"✗ Dados não disponíveis")
            return None
        
        price1 = df[symbol1]
        price2 = df[symbol2]
        
        signals, z_score = self.pairs_trading_strategy(price1, price2)
        
        # Simular trades
        position1 = 0
        position2 = 0
        cash = initial_capital
        portfolio_value = [initial_capital]
        trades = []
        
        for i in range(1, len(price1)):
            signal = signals.iloc[i]
            p1 = price1.iloc[i]
            p2 = price2.iloc[i]
            
            if signal == 1 and position1 == 0:  # LONG price1, SHORT price2
                # Alocar 50% para cada posição
                shares1 = (cash * 0.5) / p1
                shares2 = (cash * 0.5) / p2
                position1 = shares1
                position2 = -shares2
                cash = 0
                trades.append({'date': price1.index[i], 'type': 'ENTRY', 'z_score': z_score.iloc[i]})
            
            elif signal == 0 and (position1 != 0 or position2 != 0):  # EXIT
                cash = position1 * p1 + abs(position2) * p2
                trades.append({'date': price1.index[i], 'type': 'EXIT', 'z_score': z_score.iloc[i]})
                position1 = 0
                position2 = 0
            
            # Calcular valor do portfólio
            portfolio_value.append(position1 * p1 + abs(position2) * p2 + cash)
        
        # Calcular métricas
        portfolio_values = np.array(portfolio_value)
        returns = np.diff(portfolio_values) / portfolio_values[:-1]
        
        total_return = (portfolio_values[-1] - initial_capital) / initial_capital
        sharpe_ratio = np.mean(returns) / np.std(returns) * np.sqrt(252) if np.std(returns) > 0 else 0
        max_drawdown = np.min((portfolio_values - np.maximum.accumulate(portfolio_values)) / np.maximum.accumulate(portfolio_values))
        
        print(f"✓ Trades executados: {len(trades)}")
        print(f"✓ Retorno total: {total_return*100:.2f}%")
        print(f"✓ Sharpe Ratio: {sharpe_ratio:.2f}")
        print(f"✓ Max Drawdown: {max_drawdown*100:.2f}%")
        
        return {
            'symbol': f"{symbol1}/{symbol2}",
            'strategy': 'Pairs Trading',
            'total_return': total_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'num_trades': len(trades),
            'final_value': float(portfolio_values[-1])
        }
    
    def run_full_backtest(self):
        """Executar backtesting completo"""
        print("\n" + "="*70)
        print("ARBITRAGEAI V2 - ADVANCED BACKTESTING")
        print("="*70)
        
        results = []
        
        # Mean Reversion
        print("\n🔄 TESTANDO MEAN REVERSION...")
        for symbol in self.symbols['stocks']:
            result = self.backtest_mean_reversion(symbol)
            if result:
                results.append(result)
        
        # Pairs Trading
        print("\n🔄 TESTANDO PAIRS TRADING...")
        for symbol1, symbol2 in self.symbols['pairs']:
            result = self.backtest_pairs_trading(symbol1, symbol2)
            if result:
                results.append(result)
        
        # Compilar resultados
        print("\n" + "="*70)
        print("RESUMO DOS RESULTADOS")
        print("="*70)
        
        df_results = pd.DataFrame(results)
        print(df_results.to_string())
        
        # Salvar em JSON
        output_file = '/home/ubuntu/arbitrage-dashboard/client/public/backtest-results.json'
        with open(output_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'results': [
                    {
                        'symbol': r['symbol'],
                        'strategy': r['strategy'],
                        'total_return': float(r['total_return']),
                        'sharpe_ratio': float(r['sharpe_ratio']),
                        'max_drawdown': float(r['max_drawdown']),
                        'num_trades': int(r['num_trades']),
                        'final_value': float(r['final_value'])
                    }
                    for r in results
                ]
            }, f, indent=2)
        
        print(f"\n✓ Resultados salvos em: {output_file}")
        
        return results

def main():
    backtester = AdvancedBacktester()
    results = backtester.run_full_backtest()

if __name__ == "__main__":
    main()
