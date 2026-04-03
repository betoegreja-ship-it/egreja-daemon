#!/bin/bash
echo "=========================================="
echo "Egreja Investment AI - FULL SYSTEM"
echo "=========================================="

# Flask (api_server.py) é o processo principal:
# - Inicializa DB, tabelas, preços
# - Sobe 20+ background threads (stock/crypto/arbi/derivativos)
# - Serve API REST em Flask (inclusive /strategies/*)
# - Serve /derivatives dashboard (standalone HTML)
#
# api_signals.js NÃO roda mais em produção — funcionalidade migrada para Flask.
# intelligent_daemon_mysql.py NÃO roda mais — api_server.py já tem todos os loops.

export PORT=${PORT:-3001}

echo "Starting Flask API + Daemon (api_server.py) on port $PORT..."
python -u api_server.py
