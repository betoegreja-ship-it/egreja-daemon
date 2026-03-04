#!/bin/bash
# Roda daemon Python + API Node.js em paralelo

echo "=========================================="
echo "🚀 Egreja Investment AI - FULL SYSTEM"
echo "=========================================="

# Inicia daemon Python em background
echo "Starting Python Daemon..."
python intelligent_daemon_mysql.py > /tmp/daemon.log 2>&1 &
DAEMON_PID=$!
echo "✓ Daemon PID: $DAEMON_PID"

# Aguarda um pouco para daemon inicializar
sleep 2

# Inicia API Server (BLOQUEIA AQUI - importante para Railway)
echo "Starting API Server on port 3001..."
export PORT=3001
exec node api_signals.js
