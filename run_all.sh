#!/bin/bash
# Roda daemon Python + API Signals Node.js em paralelo

echo "=========================================="
echo "🚀 Egreja Investment AI - FULL SYSTEM"
echo "=========================================="
echo ""

# Inicia API Server em background
echo "Starting API Server on port 3001..."
PORT=3001 node api_signals.js &
API_PID=$!
echo "API Server PID: $API_PID"
echo ""

# Inicia daemon Python
echo "Starting Python Daemon..."
python intelligent_daemon_mysql.py &
DAEMON_PID=$!
echo "Daemon PID: $DAEMON_PID"
echo ""

echo "=========================================="
echo "System started!"
echo "- Daemon: PID $DAEMON_PID (analyzes every 15 min)"
echo "- API:    PID $API_PID (http://localhost:3001/signals)"
echo "=========================================="
echo ""

# Trap para killing ambos quando o container for parado
trap "kill $DAEMON_PID $API_PID 2>/dev/null" SIGTERM SIGINT

wait
