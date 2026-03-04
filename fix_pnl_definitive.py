#!/usr/bin/env python3
"""
Correção DEFINITIVA de P&L de todas as trades.

Regras:
1. position_size REAL = $100.000 (padrão atual do sistema)
2. Para trades antigas com pos_calc=$300.000 (qty muito alta), recalcular
3. pnl_percent = (exit - entry) / entry * 100  para BUY
   pnl_percent = (entry - exit) / entry * 100  para SELL
4. pnl_usd = (pnl_percent / 100) * position_size_real
5. Trades com ENTRY_IGUAL_EXIT: manter pnl_percent mas recalcular pnl_usd com $100k
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

# Buscar TODAS as trades fechadas
cur.execute('''
    SELECT id, symbol, recommendation, entry_price, exit_price, quantity, 
           pnl, pnl_percent, close_reason
    FROM trades WHERE status='CLOSED' ORDER BY id ASC
''')
all_trades = cur.fetchall()

POSITION_SIZE = 100000.0  # $100k - padrão atual

corrections = []
skipped = []

for t in all_trades:
    entry = float(t['entry_price'] or 0)
    exit_p = float(t['exit_price'] or 0)
    pnl = float(t['pnl'] or 0)
    pnl_pct = float(t['pnl_percent'] or 0)
    qty = float(t['quantity'] or 0)
    
    if entry == 0:
        skipped.append((t['id'], 'SEM_ENTRY'))
        continue
    
    if exit_p == 0:
        skipped.append((t['id'], 'SEM_EXIT'))
        continue
    
    # Calcular pnl_percent correto a partir dos preços
    if t['recommendation'] == 'BUY':
        correct_pct = (exit_p - entry) / entry * 100
    else:
        correct_pct = (entry - exit_p) / entry * 100
    
    # Calcular pnl_usd correto usando $100k
    correct_pnl = (correct_pct / 100) * POSITION_SIZE
    
    # Verificar se precisa corrigir
    pct_diff = abs(correct_pct - pnl_pct)
    pnl_diff = abs(correct_pnl - pnl)
    
    needs_fix = False
    reason = []
    
    if pct_diff > 0.01:  # diferença > 0.01%
        needs_fix = True
        reason.append(f'pct: {pnl_pct:.3f}% -> {correct_pct:.3f}%')
    
    if pnl_diff > 0.50:  # diferença > $0.50
        needs_fix = True
        reason.append(f'pnl: ${pnl:.2f} -> ${correct_pnl:.2f}')
    
    if needs_fix:
        corrections.append({
            'id': t['id'],
            'symbol': t['symbol'],
            'rec': t['recommendation'],
            'entry': entry,
            'exit': exit_p,
            'old_pct': pnl_pct,
            'new_pct': correct_pct,
            'old_pnl': pnl,
            'new_pnl': correct_pnl,
            'reason': ', '.join(reason)
        })

print(f'=== CORREÇÃO DEFINITIVA ===')
print(f'Total analisadas: {len(all_trades)}')
print(f'Precisam correção: {len(corrections)}')
print(f'Puladas (sem preços): {len(skipped)}')
print()

if skipped:
    print('Puladas:')
    for s in skipped:
        print(f'  #{s[0]}: {s[1]}')
    print()

print('Correções a aplicar:')
for c in corrections:
    print(f'  #{c["id"]} {c["symbol"]} {c["rec"]}: {c["reason"]}')

print()
print('Aplicando correções...')

fixed = 0
for c in corrections:
    cur.execute('''
        UPDATE trades 
        SET pnl_percent = %s, pnl = %s
        WHERE id = %s
    ''', (round(c['new_pct'], 6), round(c['new_pnl'], 2), c['id']))
    fixed += 1

conn.commit()
print(f'✅ {fixed} trades corrigidas com sucesso!')

# Calcular novo P&L total
cur.execute('''
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses,
        SUM(pnl) as total_pnl,
        AVG(pnl_percent) as avg_pct
    FROM trades WHERE status='CLOSED'
''')
stats = cur.fetchone()

print()
print('=== ESTATÍSTICAS APÓS CORREÇÃO ===')
print(f'Total trades: {stats["total"]}')
print(f'Lucros: {stats["wins"]} | Perdas: {stats["losses"]}')
win_rate = stats["wins"] / stats["total"] * 100 if stats["total"] > 0 else 0
print(f'Taxa de acerto: {win_rate:.1f}%')
print(f'P&L Total: ${stats["total_pnl"]:.2f}')
print(f'P&L Médio: {stats["avg_pct"]:.3f}%')

conn.close()
