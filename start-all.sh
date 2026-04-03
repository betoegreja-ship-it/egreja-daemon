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

# ─── Market Data Providers ───────────────────────────────────
# OpLab: primary provider for B3 derivatives (options, Greeks, IV, book, rates)
# Move to Railway env vars when possible, then remove from here.
export OPLAB_ACCESS_TOKEN="${OPLAB_ACCESS_TOKEN:-GQdzADhil2e0c+T08TCtcS1s6sWPav3hw/VZMdWQ8FzUm8YSKlPDbRSpbygbXJ1D--+KdeASbQe4t/8MJbBfGzhg==--YmIyNzE1NzIzYjMzN2QxNTZlNjA5Nzk0NGI0ZDdjZmM=}"

echo "Starting Flask API + Daemon (api_server.py) on port $PORT..."
python -u api_server.py
