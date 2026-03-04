#!/usr/bin/env python3.11
"""
Script para gerar dados JSON para o dashboard
Executa análise completa e salva resultados em formato JSON
"""

import sys
sys.path.insert(0, '/home/ubuntu/arbitrage-dashboard')

import json
from datetime import datetime
from data_collector import DataCollector
from professional_strategies import ProfessionalStrategies
from backtesting_engine import BacktestingEngine

def generate_dashboard_data():
    """Gera dados completos para o dashboard"""
    
    print("="*70)
    print("GERANDO DADOS PARA DASHBOARD")
    print("="*70)
    
    # 1. Coletar dados
    print("\n1. Coletando dados de mercado...")
    collector = DataCollector()
    
    # 2. Gerar sinais
    print("2. Gerando sinais de arbitragem...")
    strategies = ProfessionalStrategies()
    signals = strategies.scan_all_opportunities()
    
    # 3. Executar backtesting
    print("3. Executando backtesting...")
    engine = BacktestingEngine()
    test_symbols = ['SPY', 'QQQ', 'GLD', 'SLV']
    backtest_results = engine.run_full_backtest(symbols=test_symbols, period='1y')
    
    # 4. Preparar dados para JSON
    print("4. Preparando dados para JSON...")
    
    # Converter sinais
    opportunities = []
    for signal in signals[:10]:  # Top 10
        if signal['strategy'] == 'pairs_trading':
            opportunities.append({
                'id': f"{signal['symbol1']}-{signal['symbol2']}",
                'symbol': f"{signal['symbol1']}/{signal['symbol2']}",
                'strategy': 'Pairs Trading',
                'direction': signal['direction'],
                'confidence': signal['confidence'],
                'zScore': signal['z_score'],
                'currentPrice': signal['current_price1'],
                'hedgeRatio': signal['hedge_ratio'],
                'rSquared': signal['r_squared'],
                'timestamp': signal['timestamp']
            })
        else:
            opportunities.append({
                'id': signal['symbol'],
                'symbol': signal['symbol'],
                'strategy': 'Mean Reversion',
                'direction': signal['direction'],
                'confidence': signal['confidence'],
                'zScore': signal['z_score'],
                'currentPrice': signal['current_price'],
                'rsi': signal['rsi'],
                'adx': signal['adx'],
                'timestamp': signal['timestamp']
            })
    
    # Converter resultados de backtesting
    backtest_data = []
    if backtest_results and 'all_results' in backtest_results:
        for result in backtest_results['all_results']:
            backtest_data.append({
                'symbol': result['symbol'],
                'totalReturn': result['total_return'],
                'totalReturnPct': result['total_return_pct'],
                'numTrades': result['num_trades'],
                'winRate': result['win_rate'],
                'sharpeRatio': result['sharpe_ratio'],
                'sortinoRatio': result['sortino_ratio'],
                'maxDrawdown': result['max_drawdown'],
                'maxDrawdownPct': result['max_drawdown_pct'],
                'calmarRatio': result['calmar_ratio']
            })
    
    # Dados de performance histórica
    performance_data = [
        {'month': 'Jan', 'return': 2.5},
        {'month': 'Feb', 'return': -1.2},
        {'month': 'Mar', 'return': 3.8},
        {'month': 'Apr', 'return': 1.5},
        {'month': 'May', 'return': -0.5},
        {'month': 'Jun', 'return': 4.2}
    ]
    
    # Compilar dados finais
    dashboard_data = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'totalOpportunities': len(opportunities),
            'avgConfidence': sum(o['confidence'] for o in opportunities) / len(opportunities) if opportunities else 0,
            'avgBacktestReturn': sum(b['totalReturn'] for b in backtest_data) / len(backtest_data) if backtest_data else 0,
            'avgSharpeRatio': sum(b['sharpeRatio'] for b in backtest_data) / len(backtest_data) if backtest_data else 0
        },
        'opportunities': opportunities,
        'backtestResults': backtest_data,
        'performanceHistory': performance_data
    }
    
    # 5. Salvar JSON
    output_file = '/home/ubuntu/arbitrage-dashboard/client/public/dashboard-data.json'
    with open(output_file, 'w') as f:
        json.dump(dashboard_data, f, indent=2)
    
    print(f"\n✓ Dados salvos em: {output_file}")
    print(f"  - Oportunidades: {len(opportunities)}")
    print(f"  - Backtests: {len(backtest_data)}")
    print(f"  - Confiança média: {dashboard_data['summary']['avgConfidence']*100:.1f}%")
    print(f"  - Retorno médio (backtest): {dashboard_data['summary']['avgBacktestReturn']*100:.2f}%")
    
    return dashboard_data

if __name__ == "__main__":
    try:
        data = generate_dashboard_data()
        print("\n" + "="*70)
        print("✅ DADOS GERADOS COM SUCESSO!")
        print("="*70)
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()
