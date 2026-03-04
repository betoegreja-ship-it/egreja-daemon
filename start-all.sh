#!/bin/bash
echo "=========================================="
echo "Egreja Investment AI - FULL SYSTEM"
echo "=========================================="

# Inicia Node.js em background
echo "Starting API Server on port 3001..."
export PORT=3001
node api_signals.js &
NODE_PID=$!
echo "Node PID: $NODE_PID"

sleep 2

# Inicia Python em foreground (para ver logs)
echo "Starting Python Daemon..."
python -u intelligent_daemon_mysql.py
