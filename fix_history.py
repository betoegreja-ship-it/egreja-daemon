#!/usr/bin/env python3
"""
Script de Auditoria e Correção Completa do Histórico de Trades
- Deleta trades inválidas (entry=exit, P&L absurdo, sem motivo real)
- Recalcula P&L correto para todas as trades válidas
- Atualiza taxa de acerto
"""

import os
import mysql.connector
from decimal import Decimal

def get_connection():
    db_url = os.environ.get('DATABASE_URL', '')
    # Parse mysql://user:pass@host:port/db?params
    import re
    m = re.match(r'mysql://([^:]+):([^@]+)@([^:]+):(\d+)/([^?]+)', db_url)
    if not m:
        raise ValueError(f"DATABASE_URL inválida: {db_url}")
    user, password, host, port, database = m.groups()
    return mysql.connector.connect(
        host=host, port=int(port), user=user,
        password=password, database=database, charset='utf8mb4',
        ssl_disabled=False
    )

def main():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    print("=" * 70)
    print("AUDITORIA E CORREÇÃO COMPLETA DO HISTÓRICO DE TRADES")
    print("=" * 70)
    
    # ── 1. ESTADO INICIAL ──────────────────────────────────────────────
    cursor.execute("SELECT COUNT(*) as n FROM trades WHERE status='CLOSED'")
    total_antes = cursor.fetchone()['n']
    print(f"\n📊 Total de trades fechadas ANTES: {total_antes}")
    
    # ── 2. DELETAR TRADES INVÁLIDAS ────────────────────────────────────
    print("\n🗑️  DELETANDO TRADES INVÁLIDAS...")
    
    # 2a. Trades MANUAL_RESET (forçadas sem movimentação real)
    cursor.execute("DELETE FROM trades WHERE status='CLOSED' AND close_reason='MANUAL_RESET'")
    d1 = cursor.rowcount
    print(f"   Deletadas {d1} trades MANUAL_RESET (sem movimentação real)")
    
    # 2b. Trades sem motivo com entry=exit (dados corrompidos)
    cursor.execute("DELETE FROM trades WHERE status='CLOSED' AND close_reason IS NULL AND entry_price = exit_price")
    d2 = cursor.rowcount
    print(f"   Deletadas {d2} trades sem motivo com entry=exit (dados corrompidos)")
    
    # 2c. Trades TIMEOUT com entry=exit (preço de saída não foi buscado)
    cursor.execute("DELETE FROM trades WHERE status='CLOSED' AND close_reason='TIMEOUT' AND entry_price = exit_price")
    d3 = cursor.rowcount
    print(f"   Deletadas {d3} trades TIMEOUT com entry=exit (saída não registrada)")
    
    # 2d. Trade #1 com entrada fictícia ($50.000 para BTC — preço irreal)
    cursor.execute("DELETE FROM trades WHERE id=1")
    d4 = cursor.rowcount
    print(f"   Deletadas {d4} trade(s) com preço de entrada fictício ($50k BTC)")
    
    # 2e. Trades com P&L absurdo > 25% (impossível em 2h de operação)
    cursor.execute("DELETE FROM trades WHERE status='CLOSED' AND ABS(CAST(pnl_percent AS DECIMAL(10,4))) > 25")
    d5 = cursor.rowcount
    print(f"   Deletadas {d5} trades com P&L absurdo (>25% em 2h)")
    
    total_deletadas = d1 + d2 + d3 + d4 + d5
    print(f"\n   ✅ Total deletadas: {total_deletadas} trades inválidas")
    
    # ── 3. RECALCULAR P&L DAS TRADES VÁLIDAS ──────────────────────────
    print("\n🔄 RECALCULANDO P&L DE TODAS AS TRADES VÁLIDAS...")
    
    cursor.execute("""
        SELECT id, symbol, recommendation, entry_price, exit_price, quantity, pnl, pnl_percent
        FROM trades 
        WHERE status='CLOSED' 
          AND entry_price IS NOT NULL AND entry_price > 0
          AND exit_price IS NOT NULL AND exit_price > 0
          AND entry_price != exit_price
        ORDER BY id
    """)
    trades = cursor.fetchall()
    
    corrigidas = 0
    erros = 0
    
    for t in trades:
        try:
            entry = float(t['entry_price'])
            exit_p = float(t['exit_price'])
            qty = float(t['quantity'])
            rec = t['recommendation']
            
            # Calcular position_size (capital investido em USD)
            # quantity = capital / entry_price  →  capital = quantity * entry_price
            position_size = qty * entry
            
            # Calcular P&L correto baseado na direção da trade
            if rec == 'BUY':
                # BUY: lucro quando preço sobe
                pnl_pct = ((exit_p - entry) / entry) * 100
            elif rec == 'SELL':
                # SELL (short): lucro quando preço cai
                pnl_pct = ((entry - exit_p) / entry) * 100
            else:
                continue
            
            # P&L em USD = percentual * capital investido / 100
            pnl_usd = (pnl_pct / 100) * position_size
            
            # Verificar se o valor atual está errado (diferença > 0.01%)
            current_pct = float(t['pnl_percent'] or 0)
            if abs(current_pct - pnl_pct) > 0.01:
                cursor.execute(
                    "UPDATE trades SET pnl=%s, pnl_percent=%s WHERE id=%s",
                    (round(pnl_usd, 6), round(pnl_pct, 6), t['id'])
                )
                corrigidas += 1
                
        except Exception as e:
            erros += 1
            print(f"   ⚠️  Erro na trade #{t['id']}: {e}")
    
    print(f"   ✅ {corrigidas} trades recalculadas | {erros} erros")
    
    # ── 4. ESTADO FINAL ────────────────────────────────────────────────
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) as n FROM trades WHERE status='CLOSED'")
    total_depois = cursor.fetchone()['n']
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN CAST(pnl_percent AS DECIMAL(10,4)) > 0 THEN 1 ELSE 0 END) as lucros,
            SUM(CASE WHEN CAST(pnl_percent AS DECIMAL(10,4)) < 0 THEN 1 ELSE 0 END) as perdas,
            SUM(CASE WHEN CAST(pnl_percent AS DECIMAL(10,4)) = 0 THEN 1 ELSE 0 END) as neutras,
            SUM(CAST(pnl AS DECIMAL(20,6))) as total_pnl,
            AVG(CAST(pnl_percent AS DECIMAL(10,4))) as avg_pct
        FROM trades 
        WHERE status='CLOSED'
    """)
    stats = cursor.fetchone()
    
    taxa_acerto = (stats['lucros'] / stats['total'] * 100) if stats['total'] > 0 else 0
    
    print("\n" + "=" * 70)
    print("RESULTADO FINAL")
    print("=" * 70)
    print(f"Trades antes:    {total_antes}")
    print(f"Trades deletadas: {total_deletadas}")
    print(f"Trades depois:   {total_depois}")
    print(f"\nTrades com lucro:  {stats['lucros']}")
    print(f"Trades com perda:  {stats['perdas']}")
    print(f"Trades neutras:    {stats['neutras']}")
    print(f"\nTaxa de acerto:    {taxa_acerto:.1f}%")
    print(f"P&L Total:         ${float(stats['total_pnl'] or 0):,.2f}")
    print(f"P&L Médio/trade:   {float(stats['avg_pct'] or 0):.3f}%")
    
    # Distribuição por motivo
    cursor.execute("""
        SELECT close_reason, COUNT(*) as n, 
               SUM(CASE WHEN CAST(pnl_percent AS DECIMAL(10,4)) > 0 THEN 1 ELSE 0 END) as wins,
               AVG(CAST(pnl_percent AS DECIMAL(10,4))) as avg_pct,
               SUM(CAST(pnl AS DECIMAL(20,6))) as total_pnl
        FROM trades WHERE status='CLOSED' 
        GROUP BY close_reason ORDER BY n DESC
    """)
    reasons = cursor.fetchall()
    print("\n--- Por Motivo de Fechamento ---")
    for r in reasons:
        win_rate = (r['wins'] / r['n'] * 100) if r['n'] > 0 else 0
        print(f"  {r['close_reason'] or 'NULL':15s}: {r['n']:3d} trades | {win_rate:.0f}% acerto | P&L médio: {float(r['avg_pct'] or 0):.3f}% | Total: ${float(r['total_pnl'] or 0):,.2f}")
    
    cursor.close()
    conn.close()
    print("\n✅ Auditoria e correção concluídas com sucesso!")

if __name__ == '__main__':
    main()
