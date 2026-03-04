#!/usr/bin/env python3
"""
ArbitrageAI - Performance Analyzer
Análise de desempenho, backtesting e otimização contínua do sistema
Identifica quais indicadores/estratégias funcionam melhor
"""

import logging
from typing import Dict, List, Optional, Tuple
import numpy as np
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)


class PerformanceAnalyzer:
    """Analisa performance do sistema e otimiza estratégia"""
    
    def __init__(self):
        """Inicializar analisador"""
        self.backtest_results = {}
        self.optimization_history = []
        self.strategy_metrics = {}
    
    def backtest_strategy(self, symbol: str, price_history: List[float], 
                         strategy_func, initial_capital: float = 10000) -> Dict:
        """
        Faz backtesting de uma estratégia no histórico real
        
        Args:
            symbol: Símbolo do ativo
            price_history: Histórico de preços
            strategy_func: Função que retorna (action, confidence) para cada candle
            initial_capital: Capital inicial
        
        Returns:
            Dict com resultados de backtesting
        """
        
        try:
            capital = initial_capital
            positions = []  # [{'entry_price': x, 'qty': y, 'entry_idx': z}]
            trades = []
            
            for idx in range(1, len(price_history)):
                current_price = price_history[idx]
                
                # Gerar sinal da estratégia
                action, confidence = strategy_func(price_history[:idx+1])
                
                # BUY
                if action == 'BUY' and confidence > 0.6:
                    qty = (capital * 0.1) / current_price  # Usar 10% do capital
                    positions.append({
                        'entry_price': current_price,
                        'qty': qty,
                        'entry_idx': idx,
                        'entry_capital': qty * current_price
                    })
                    capital -= qty * current_price
                
                # SELL (sell all positions with profit)
                elif action == 'SELL' and positions:
                    for pos in positions:
                        exit_price = current_price
                        profit = pos['qty'] * (exit_price - pos['entry_price'])
                        capital += pos['qty'] * exit_price
                        
                        trades.append({
                            'entry_price': pos['entry_price'],
                            'exit_price': exit_price,
                            'qty': pos['qty'],
                            'profit': profit,
                            'return_pct': (profit / pos['entry_capital']) * 100 if pos['entry_capital'] > 0 else 0,
                            'duration': idx - pos['entry_idx']
                        })
                    positions = []
            
            # Fechar posições abertas no final
            for pos in positions:
                exit_price = price_history[-1]
                profit = pos['qty'] * (exit_price - pos['entry_price'])
                capital += pos['qty'] * exit_price
                
                trades.append({
                    'entry_price': pos['entry_price'],
                    'exit_price': exit_price,
                    'qty': pos['qty'],
                    'profit': profit,
                    'return_pct': (profit / pos['entry_capital']) * 100 if pos['entry_capital'] > 0 else 0,
                    'duration': len(price_history) - pos['entry_idx']
                })
            
            # Calcular métricas
            total_return = ((capital - initial_capital) / initial_capital) * 100
            wins = len([t for t in trades if t['profit'] > 0])
            losses = len([t for t in trades if t['profit'] <= 0])
            win_rate = (wins / len(trades) * 100) if trades else 0
            
            avg_profit = np.mean([t['profit'] for t in trades]) if trades else 0
            avg_loss = abs(np.mean([t['profit'] for t in trades if t['profit'] < 0])) if losses > 0 else 0
            profit_factor = avg_profit / avg_loss if avg_loss > 0 else 0
            
            # Drawdown
            equity = [initial_capital]
            for trade in trades:
                equity.append(equity[-1] + trade['profit'])
            
            peak = initial_capital
            max_drawdown = 0
            for e in equity:
                if e < peak:
                    drawdown = ((peak - e) / peak) * 100
                    max_drawdown = max(max_drawdown, drawdown)
                else:
                    peak = e
            
            results = {
                'symbol': symbol,
                'total_trades': len(trades),
                'wins': wins,
                'losses': losses,
                'win_rate': round(win_rate, 2),
                'total_return_pct': round(total_return, 2),
                'final_capital': round(capital, 2),
                'avg_profit': round(avg_profit, 2),
                'avg_loss': round(avg_loss, 2),
                'profit_factor': round(profit_factor, 2),
                'max_drawdown_pct': round(max_drawdown, 2),
                'trades': trades,
                'backtest_date': datetime.now().isoformat()
            }
            
            return results
        
        except Exception as e:
            logger.error(f"Erro ao fazer backtesting de {symbol}: {e}")
            return None
    
    def compare_strategies(self, symbol: str, price_history: List[float],
                          strategies: Dict[str, callable]) -> Dict:
        """
        Compara múltiplas estratégias no mesmo histórico
        Identifica qual estratégia funciona melhor
        
        Args:
            symbol: Símbolo do ativo
            price_history: Histórico de preços
            strategies: Dict com {nome: função_estratégia}
        
        Returns:
            Ranking das estratégias
        """
        
        results = {}
        
        for strategy_name, strategy_func in strategies.items():
            backtest = self.backtest_strategy(symbol, price_history, strategy_func)
            
            if backtest:
                results[strategy_name] = backtest
                logger.info(f"{symbol} - {strategy_name}: {backtest['win_rate']}% WR, {backtest['total_return_pct']:.2f}% return")
        
        # Rank por total return
        ranked = sorted(
            results.items(),
            key=lambda x: x[1]['total_return_pct'],
            reverse=True
        )
        
        return {
            'symbol': symbol,
            'comparison_date': datetime.now().isoformat(),
            'results': dict(ranked),
            'best_strategy': ranked[0][0] if ranked else None,
            'best_return': ranked[0][1]['total_return_pct'] if ranked else 0
        }
    
    def optimize_parameters(self, symbol: str, price_history: List[float],
                          base_strategy: callable, param_ranges: Dict) -> Dict:
        """
        Otimiza parâmetros da estratégia (ex: períodos de EMA, RSI levels)
        Busca combinação que maximiza lucro
        
        Args:
            symbol: Símbolo
            price_history: Histórico
            base_strategy: Função da estratégia (recebe price_history + params)
            param_ranges: {'param1': [10, 20, 30], 'param2': [0.5, 0.6, 0.7]}
        
        Returns:
            Parâmetros ótimos e seu performance
        """
        
        best_result = None
        best_params = None
        best_return = -float('inf')
        
        # Grid search: testar todas combinações
        import itertools
        
        param_names = list(param_ranges.keys())
        param_values = [param_ranges[name] for name in param_names]
        
        for combination in itertools.product(*param_values):
            params = dict(zip(param_names, combination))
            
            # Wrapper da estratégia com parâmetros
            def strategy_with_params(hist):
                return base_strategy(hist, **params)
            
            result = self.backtest_strategy(symbol, price_history, strategy_with_params)
            
            if result and result['total_return_pct'] > best_return:
                best_return = result['total_return_pct']
                best_params = params
                best_result = result
        
        return {
            'symbol': symbol,
            'optimization_date': datetime.now().isoformat(),
            'best_params': best_params,
            'best_return': round(best_return, 2),
            'backtest_result': best_result
        }
    
    def sensitivity_analysis(self, symbol: str, signal_analysis: Dict) -> Dict:
        """
        Análise de sensibilidade: como a decisão muda com pequenas variações nos dados?
        Se o sinal é robusto (insensível a pequenas mudanças), mais confiável
        """
        
        original_score = signal_analysis.get('combined_score', 50)
        original_signal = signal_analysis.get('signal', '🟡 MANTER')
        
        # Simular variações de ±5% nos indicadores principais
        sensitivity = {}
        
        indicators = signal_analysis.get('technical_analysis', {}).get('indicators', {})
        
        for indicator, value in indicators.items():
            if isinstance(value, (int, float)):
                # Testar -5% e +5%
                lower = value * 0.95
                upper = value * 1.05
                
                sensitivity[indicator] = {
                    'original': value,
                    'range': (lower, upper),
                    'impact': 'Sensível' if abs(upper - lower) > value * 0.1 else 'Robusta'
                }
        
        return {
            'symbol': symbol,
            'signal_robustness': 'Alta' if len([v for v in sensitivity.values() if 'Robusta' in v['impact']]) > len(sensitivity) * 0.7 else 'Média' if len([v for v in sensitivity.values() if 'Robusta' in v['impact']]) > len(sensitivity) * 0.4 else 'Baixa',
            'original_signal': original_signal,
            'original_score': original_score,
            'sensitivity_analysis': sensitivity
        }
    
    def generate_daily_report(self, portfolio_performance: Dict[str, Dict]) -> Dict:
        """
        Gera relatório diário de desempenho da carteira
        Mostra P&L, estratégias melhores, alertas
        """
        
        total_pl = 0
        win_symbols = []
        lose_symbols = []
        top_performer = None
        top_performance = -float('inf')
        
        for symbol, perf in portfolio_performance.items():
            pl = perf.get('profit_loss', 0)
            total_pl += pl
            
            if pl > 0:
                win_symbols.append((symbol, pl))
            else:
                lose_symbols.append((symbol, abs(pl)))
            
            if pl > top_performance:
                top_performance = pl
                top_performer = symbol
        
        # Sort
        win_symbols.sort(key=lambda x: x[1], reverse=True)
        lose_symbols.sort(key=lambda x: x[1], reverse=True)
        
        report = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'total_pl': round(total_pl, 2),
            'number_of_symbols': len(portfolio_performance),
            'winning_symbols': win_symbols[:5],
            'losing_symbols': lose_symbols[:5],
            'top_performer': top_performer,
            'top_performance': round(top_performance, 2),
            'average_pl': round(total_pl / len(portfolio_performance) if portfolio_performance else 0, 2),
            'generated_at': datetime.now().isoformat()
        }
        
        return report


if __name__ == '__main__':
    # Teste
    print("=== PERFORMANCE ANALYZER ===\n")
    
    analyzer = PerformanceAnalyzer()
    
    # Histórico simulado
    np.random.seed(42)
    price_history = [100 + np.cumsum(np.random.randn(100) * 2)]
    price_history = price_history[0].tolist()
    
    # Estratégia simples (buy se preco sobe, sell se cai)
    def simple_strategy(hist):
        if len(hist) < 2:
            return 'HOLD', 0
        
        if hist[-1] > hist[-2]:
            return 'BUY', 0.7
        else:
            return 'SELL', 0.7
    
    # Fazer backtesting
    result = analyzer.backtest_strategy('TEST', price_history, simple_strategy)
    
    print(f"Backtesting Result:")
    print(f"  Win Rate: {result['win_rate']}%")
    print(f"  Total Return: {result['total_return_pct']:.2f}%")
    print(f"  Profit Factor: {result['profit_factor']}")
    print(f"  Max Drawdown: {result['max_drawdown_pct']}%")
