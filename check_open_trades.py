#!/usr/bin/env python3.11
"""Script para verificar trades abertos no banco de dados"""

import mysql.connector
import os
import re
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL')

# Parse connection string
match = re.match(r'mysql://([^:]+):([^@]+)@([^:]+):(\d+)/(\w+)', db_url)
if match:
    user, password, host, port, database = match.groups()
    
    conn = mysql.connector.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=database
    )
    
    cursor = conn.cursor(dictionary=True)
    cursor.execute('SELECT COUNT(*) as count FROM trades WHERE status = %s', ('OPEN',))
    result = cursor.fetchone()
    print(f'Trades abertos: {result["count"]}')
    
    cursor.execute('''
        SELECT symbol, recommendation, entry_price, exit_price, pnl, opened_at 
        FROM trades 
        WHERE status = %s 
        ORDER BY opened_at DESC 
        LIMIT 10
    ''', ('OPEN',))
    trades = cursor.fetchall()
    
    print('\nÚltimos 10 trades abertos:')
    for t in trades:
        entry_price = float(t["entry_price"]) if t["entry_price"] else 0.0
        exit_price = float(t["exit_price"]) if t["exit_price"] else 0.0
        pnl_value = float(t["pnl"]) if t["pnl"] else 0.0
        print(f'  {t["symbol"]} {t["recommendation"]} @ ${entry_price:.2f} | Exit: ${exit_price:.2f} | P&L: ${pnl_value:.2f} | Aberto: {t["opened_at"]}')
    
    cursor.close()
    conn.close()
