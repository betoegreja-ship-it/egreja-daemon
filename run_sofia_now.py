#!/usr/bin/env python3
"""
Executar Sofia IA para gerar análises novas
"""

from sofia_integrated import SofiaIntegrated

print('🚀 Iniciando Sofia IA...')
sofia = SofiaIntegrated()

# Lista de símbolos para analisar
symbols = [
    'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT',
    'LTCUSDT', 'DOGEUSDT', 'MATICUSDT', 'SOLUSDT', 'DOTUSDT',
    'AVAXUSDT', 'LINKUSDT', 'ATOMUSDT', 'UNIUSDT', 'FILUSDT'
]

print('\n📊 Executando ciclo diário de análises...')
result = sofia.execute_daily_cycle(symbols, capital=1000000)

print(f'\n✅ Ciclo completo!')
print(f'   - Trades executados: {len(result["trades"])}')
print(f'   - P&L Total: ${result["total_pnl"]:.2f}')
print(f'   - Trades vencedores: {result["winning_trades"]}')
print(f'   - Trades perdedores: {result["losing_trades"]}')

print('\n📋 Últimos trades:')
for trade in result["trades"][:5]:
    print(f'   • {trade["symbol"]}: {trade["recommendation"]} | P&L: ${trade["pnl"]:.2f}')
