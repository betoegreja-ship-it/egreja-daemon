import mysql.connector
import os

# Parse DATABASE_URL
db_url = os.getenv('DATABASE_URL')
parts = db_url.replace('mysql://', '').split('@')
user_pass = parts[0].split(':')
host_db = parts[1].split('/')
host_port = host_db[0].split(':')

conn = mysql.connector.connect(
    host=host_port[0],
    port=int(host_port[1]) if len(host_port) > 1 else 3306,
    user=user_pass[0],
    password=user_pass[1],
    database=host_db[1].split('?')[0]
)

cursor = conn.cursor()

# Fechar todas as trades abertas há mais de 60 minutos
cursor.execute("""
    UPDATE trades
    SET status = 'CLOSED',
        exit_price = entry_price,
        pnl = 0,
        closed_at = NOW(),
        close_reason = 'MANUAL_RESET'
    WHERE status = 'OPEN'
    AND TIMESTAMPDIFF(MINUTE, created_at, NOW()) > 60
""")

affected = cursor.rowcount
conn.commit()
print(f"✅ {affected} trades antigas fechadas manualmente")

cursor.close()
conn.close()
