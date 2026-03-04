import mysql.connector
import os
from dotenv import load_dotenv
import requests
from datetime import datetime

load_dotenv()

# Conectar ao banco
conn = mysql.connector.connect(
    host=os.getenv('DB_HOST', 'gateway01.us-east-1.prod.aws.tidbcloud.com'),
    port=int(os.getenv('DB_PORT', 4000)),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database=os.getenv('DB_NAME'),
    ssl_ca='/etc/ssl/certs/ca-certificates.crt'
)

cursor = conn.cursor(dictionary=True)

# Buscar trades abertas
cursor.execute("SELECT * FROM trades WHERE status = 'OPEN'")
trades = cursor.fetchall()

print(f"Encontradas {len(trades)} trades abertas")

for trade in trades:
    symbol = trade['symbol']
    entry_price = float(trade['entry_price'])
    quantity = float(trade['quantity'])
    recommendation = trade['recommendation']
    
    # Buscar preço atual da Binance
    try:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            current_price = float(response.json()['price'])
        else:
            print(f"Erro Binance para {symbol}, usando preço de entrada")
            current_price = entry_price
    except:
        current_price = entry_price
    
    # Calcular P&L
    if recommendation == 'BUY':
        pnl = (current_price - entry_price) * quantity
    else:  # SELL
        pnl = (entry_price - current_price) * quantity
    
    pnl_percent = (pnl / (entry_price * quantity)) * 100
    
    # Fechar trade
    cursor.execute("""
        UPDATE trades
        SET status = 'CLOSED',
            exit_price = %s,
            pnl = %s,
            pnl_percent = %s,
            closed_at = NOW()
        WHERE id = %s
    """, (current_price, pnl, pnl_percent, trade['id']))
    
    conn.commit()
    print(f"✅ Trade #{trade['id']} fechado - {symbol} - P&L: ${pnl:,.2f} ({pnl_percent:.2f}%)")

cursor.close()
conn.close()
print("\n✅ Todas as trades antigas foram fechadas!")
