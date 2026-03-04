#!/usr/bin/env python3
"""
Inspeção direta das trades problemáticas para entender o padrão real.
"""
import os
import re
import pymysql

db_url = os.environ.get('DATABASE_URL', '')
m = re.match(r'mysql://([^:]+):([^@]+)@([^:]+):(\d+)/([^?]+)', db_url)
user, pwd, host, port, db = m.groups()

conn = pymysql.connect(
    host=host, port=int(port), user=user, password=pwd, database=db,
    cursorclass=pymysql.cursors.DictCursor, ssl={'ssl': True}
)
cur = conn.cursor()

# Ver estrutura da tabela
cur.execute('DESCRIBE trades')
cols = cur.fetchall()
print('=== COLUNAS DA TABELA TRADES ===')
for c in cols:
    print(f"  {c['Field']:30s} {c['Type']:20s} {c['Null']:5s} {c['Default'] or 'NULL'}")
print()

# Ver amostra das primeiras 5 trades com IDs 30001-30005
cur.execute('''
    SELECT * FROM trades WHERE id IN (30001, 30002, 30003, 210005, 210006, 210010)
    ORDER BY id
''')
samples = cur.fetchall()
print('=== AMOSTRA DE TRADES PROBLEMÁTICAS ===')
for t in samples:
    entry = float(t['entry_price'] or 0)
    exit_p = float(t['exit_price'] or 0)
    qty = float(t['quantity'] or 0)
    pnl = float(t['pnl'] or 0)
    pnl_pct = float(t['pnl_percent'] or 0)
    pos_calc = entry * qty
    
    # Calcular pct correto
    if t['recommendation'] == 'BUY':
        correct_pct = (exit_p - entry) / entry * 100 if entry > 0 else 0
    else:
        correct_pct = (entry - exit_p) / entry * 100 if entry > 0 else 0
    
    print(f"\n#{t['id']} {t['symbol']} {t['recommendation']}")
    print(f"  entry_price: {entry}")
    print(f"  exit_price:  {exit_p}")
    print(f"  quantity:    {qty}")
    print(f"  pos_calc:    ${pos_calc:.0f}")
    print(f"  pnl_percent: {pnl_pct:.6f}%")
    print(f"  pnl:         ${pnl:.2f}")
    print(f"  pnl_correto_pct: {correct_pct:.6f}%")
    print(f"  pnl_com_100k:    ${correct_pct/100*100000:.2f}")
    print(f"  pnl_com_300k:    ${correct_pct/100*300000:.2f}")
    print(f"  pnl_com_pos_calc: ${correct_pct/100*pos_calc:.2f}")
    print(f"  close_reason: {t['close_reason']}")
    
    # Qual bate?
    if abs(correct_pct/100*100000 - pnl) < 1:
        print(f"  >>> P&L bate com $100k ✅")
    elif abs(correct_pct/100*300000 - pnl) < 1:
        print(f"  >>> P&L bate com $300k ⚠️")
    elif abs(correct_pct/100*pos_calc - pnl) < 1:
        print(f"  >>> P&L bate com pos_calc ⚠️")
    else:
        print(f"  >>> P&L NÃO BATE com nenhum ❌")

# Ver distribuição de position sizes
print('\n=== DISTRIBUIÇÃO DE pos_calc (entry*qty) ===')
cur.execute('''
    SELECT 
        ROUND(entry_price * quantity / 1000) * 1000 as pos_bucket,
        COUNT(*) as cnt
    FROM trades WHERE status='CLOSED' AND entry_price > 0 AND quantity > 0
    GROUP BY pos_bucket
    ORDER BY cnt DESC
    LIMIT 10
''')
dist = cur.fetchall()
for d in dist:
    print(f"  ~${d['pos_bucket']:.0f}: {d['cnt']} trades")

conn.close()
