#!/usr/bin/env python3
"""Fecha trades antigas com preço real do mercado"""
import os
import sys
from dotenv import load_dotenv
load_dotenv()

# Adicionar diretório ao path
sys.path.insert(0, '/home/ubuntu/arbitrage-dashboard')

# Importar daemon
from production_daemon import ProductionDaemon

# Criar instância
daemon = ProductionDaemon()

# Buscar trades abertas
import mysql.connector
conn = daemon._get_db_connection()
cursor = conn.cursor(dictionary=True)

cursor.execute("""
    SELECT id, symbol, entry_price, quantity, recommendation, opened_at,
           TIMESTAMPDIFF(HOUR, opened_at, NOW()) as hours_open
    FROM trades 
    WHERE status = 'OPEN'
    ORDER BY opened_at ASC
""")

trades = cursor.fetchall()
print(f"\n{'='*70}")
print(f"FECHANDO {len(trades)} TRADES ANTIGAS")
print(f"{'='*70}\n")

for trade in trades:
    trade_id = trade['id']
    symbol = trade['symbol']
    entry_price = float(trade['entry_price'])
    quantity = float(trade['quantity'])
    recommendation = trade['recommendation']
    hours_open = trade['hours_open']
    
    print(f"Trade #{trade_id} - {symbol}")
    print(f"  Entrada: ${entry_price:,.2f}")
    print(f"  Tempo aberto: {hours_open} horas")
    
    # Buscar preço atual
    current_price = daemon.get_current_price(symbol)
    
    if not current_price:
        print(f"  ❌ Não foi possível obter preço atual")
        continue
    
    print(f"  Preço atual: ${current_price:,.2f}")
    
    # Calcular P&L
    if recommendation == 'BUY':
        pnl = (current_price - entry_price) * quantity
    else:  # SELL
        pnl = (entry_price - current_price) * quantity
    
    pnl_pct = (pnl / (entry_price * quantity)) * 100
    
    print(f"  P&L: ${pnl:,.2f} ({pnl_pct:+.2f}%)")
    
    # Fechar trade
    cursor.execute("""
        UPDATE trades 
        SET status = 'CLOSED',
            exit_price = %s,
            pnl = %s,
            closed_at = NOW(),
            close_reason = 'TIMEOUT'
        WHERE id = %s
    """, (current_price, pnl, trade_id))
    
    conn.commit()
    print(f"  ✅ Trade fechado\n")

cursor.close()
conn.close()

print(f"{'='*70}")
print(f"TODAS AS TRADES ANTIGAS FORAM FECHADAS!")
print(f"{'='*70}\n")
