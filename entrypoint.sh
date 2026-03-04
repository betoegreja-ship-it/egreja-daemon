#!/bin/sh
# Entrypoint para Railway - roda API Node + Daemon Python

set -e

echo "=========================================="
echo "🚀 Egreja Investment AI - Railway Start"
echo "=========================================="
echo ""

# Verificar variáveis de ambiente críticas
if [ -z "$MYSQLHOST" ]; then
  echo "⚠️ WARNING: MYSQLHOST not set, using localhost"
  export MYSQLHOST="localhost"
fi

if [ -z "$PORT" ]; then
  export PORT=3001
fi

echo "Environment:"
echo "  PORT: $PORT"
echo "  MYSQLHOST: $MYSQLHOST"
echo "  MYSQLDATABASE: ${MYSQLDATABASE:-railway}"
echo ""

# Start API Server on port 3001
echo "🚀 Starting API Server on port $PORT..."
PORT=$PORT node api_signals.js &
API_PID=$!
echo "API Server PID: $API_PID"
echo ""

# Start Python Daemon
echo "🚀 Starting Python Daemon..."
python3 intelligent_daemon_mysql.py &
DAEMON_PID=$!
echo "Daemon PID: $DAEMON_PID"
echo ""

echo "=========================================="
echo "✅ All services started!"
echo "   - Node API: http://localhost:$PORT/signals"
echo "   - Daemon: Running (analyzes every 15 min)"
echo "=========================================="
echo ""

# Handle signals
trap "echo 'Shutting down...'; kill $DAEMON_PID $API_PID 2>/dev/null; exit 0" SIGTERM SIGINT

# Wait for all background processes
wait
