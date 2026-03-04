#!/bin/bash
# Script que roda daemon Python + API server Node.js em paralelo

echo "=========================================="
echo "🚀 Egreja Investment AI - FULL SYSTEM"
echo "=========================================="
echo ""

# Inicia API server em background
echo "Starting API Server on port 8000..."
node api_server.js &
API_PID=$!
echo "API Server PID: $API_PID"
echo ""

# Inicia daemon Python
echo "Starting Python Daemon..."
python intelligent_daemon_simple.py &
DAEMON_PID=$!
echo "Daemon PID: $DAEMON_PID"
echo ""

echo "=========================================="
echo "System started!"
echo "- Daemon: PID $DAEMON_PID"
echo "- API:    PID $API_PID (http://localhost:8000/signals)"
echo "=========================================="
echo ""

# Trap para killing ambos quando o container for parado
trap "kill $DAEMON_PID $API_PID" SIGTERM SIGINT

wait
