#!/usr/bin/env python3
"""
Auditoria completa de TODAS as trades fechadas.
Detecta: sem exit_price, pnl zerado, pnl_percent errado, pnl_usd errado.
"""
import os
import re
import pymysql

db_url = os.environ.get('DATABASE_URL', '')
m = re.match(r'mysql://([^:]+):([^@]+)@([^:]+):(\d+)/([^?]+)', db_url)
if not m:
    print('Erro ao parsear DATABASE_URL')
    exit(1)
user, pwd, host, port, db = m.groups()

conn = pymysql.connect(
    host=host, port=int(port), user=user, password=pwd, database=db,
    cursorclass=pymysql.cursors.DictCursor,
    ssl={'ssl': True}
)
cur = conn.cursor()

cur.execute('''
    SELECT id, symbol, recommendation, entry_price, exit_price, quantity, 
           pnl, pnl_percent, close_reason
    FROM trades WHERE status='CLOSED' ORDER BY id ASC
''')
all_trades = cur.fetchall()

problems = []

for t in all_trades:
    entry = float(t['entry_price'] or 0)
    exit_p = float(t['exit_price'] or 0)
    pnl = float(t['pnl'] or 0)
    pnl_pct = float(t['pnl_percent'] or 0)
    qty = float(t['quantity'] or 0)
    pos_size = entry * qty  # calcular position size pela quantidade
    
    issues = []
    
    # Problema 1: sem preço de saída
    if exit_p == 0:
        issues.append('SEM_EXIT_PRICE')
    
    # Problema 2: sem preço de entrada
    if entry == 0:
        issues.append('SEM_ENTRY_PRICE')
    
    # Problema 3: entry = exit (preço não foi atualizado)
    if entry > 0 and exit_p > 0 and abs(entry - exit_p) < 0.000001:
        issues.append('ENTRY_IGUAL_EXIT')
    
    # Problema 4: P&L zerado mas preços diferentes
    if entry > 0 and exit_p > 0 and abs(entry - exit_p) > 0.000001 and pnl == 0 and pnl_pct == 0:
        issues.append('PNL_ZERADO')
    
    # Problema 5: pnl_percent inconsistente com os preços
    if entry > 0 and exit_p > 0:
        if t['recommendation'] == 'BUY':
            expected_pct = (exit_p - entry) / entry * 100
        else:
            expected_pct = (entry - exit_p) / entry * 100
        
        if abs(expected_pct - pnl_pct) > 0.5:
            issues.append(f'PNL_PCT_ERRADO(esperado={expected_pct:.3f}%,salvo={pnl_pct:.3f}%)')
    
    # Problema 6: pnl_usd inconsistente com pnl_pct e position_size
    if pnl_pct != 0 and pos_size > 0:
        expected_pnl = (pnl_pct / 100) * pos_size
        if abs(expected_pnl - pnl) > 1.0:
            issues.append(f'PNL_USD_ERRADO(esperado=${expected_pnl:.2f},salvo=${pnl:.2f})')
    
    if issues:
        problems.append({**t, 'issues': issues})

print(f'=== VARREDURA COMPLETA: {len(all_trades)} trades analisadas ===')
print(f'Com problemas: {len(problems)}')
print()

# Agrupar por tipo de problema
groups = {}
for p in problems:
    for issue in p['issues']:
        key = issue.split('(')[0]
        if key not in groups:
            groups[key] = []
        groups[key].append(p)

for issue, trades in groups.items():
    print(f'--- {issue}: {len(trades)} trades ---')
    for t in trades:
        entry = float(t['entry_price'] or 0)
        exit_p = float(t['exit_price'] or 0)
        qty = float(t['quantity'] or 0)
        pnl = float(t['pnl'] or 0)
        pnl_pct = float(t['pnl_percent'] or 0)
        pos_calc = entry * qty if entry > 0 and qty > 0 else 0
        print(f'  #{t["id"]} {t["symbol"]} {t["recommendation"]} '
              f'entry={entry} exit={exit_p} qty={qty:.4f} '
              f'pos_calc=${pos_calc:.0f} '
              f'pnl_pct={pnl_pct:.3f}% pnl=${pnl:.2f} '
              f'reason={t["close_reason"]}')
    print()

# Resumo de IDs a corrigir
all_problem_ids = list(set(p['id'] for p in problems))
print(f'IDs com problemas: {sorted(all_problem_ids)}')

conn.close()
