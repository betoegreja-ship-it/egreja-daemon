#!/bin/bash

# ============================================================
# START ALL - Daemon + API + Voice Agent + Scheduler
# ============================================================
# Criado: 2026-03-04 22:14 GMT-3
# Autor: Nina Egreja
# Status: ÉPICO! 🦅
# ============================================================

set -e

echo ""
echo "╔════════════════════════════════════════════════════════╗"
echo "║                                                        ║"
echo "║    🚀 EGREJA INVESTMENT AI - START ALL (v2)            ║"
echo "║                                                        ║"
echo "║    Daemon + API + Voice Agent + Scheduler             ║"
echo "║                                                        ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================
# 1. VERIFICAR DEPENDÊNCIAS
# ============================================================

echo -e "${BLUE}📋 Verificando dependências...${NC}"

# Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python3 não encontrado!${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Python3 OK${NC}"

# Node.js
if ! command -v node &> /dev/null; then
    echo -e "${RED}❌ Node.js não encontrado!${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Node.js OK${NC}"

# npm
if ! command -v npm &> /dev/null; then
    echo -e "${RED}❌ npm não encontrado!${NC}"
    exit 1
fi
echo -e "${GREEN}✅ npm OK${NC}"

echo ""

# ============================================================
# 2. INSTALAR DEPENDÊNCIAS (se não instalado)
# ============================================================

echo -e "${BLUE}📦 Verificando pacotes...${NC}"

if [ ! -d "node_modules" ]; then
    echo "Instalando Node packages..."
    npm install --silent
    echo -e "${GREEN}✅ Node packages instalados${NC}"
else
    echo -e "${GREEN}✅ Node packages já instalados${NC}"
fi

echo ""

# ============================================================
# 3. INICIAR PROCESSOS
# ============================================================

echo -e "${BLUE}🚀 Iniciando processos...${NC}"
echo ""

# Função para rodar em background e log
run_process() {
    local name=$1
    local cmd=$2
    local log_file=$3

    echo -e "${YELLOW}📌 ${name}${NC}"
    eval "$cmd" > "$log_file" 2>&1 &
    local pid=$!
    echo -e "${GREEN}   PID: $pid${NC}"
    echo -e "${GREEN}   Log: $log_file${NC}"
    echo ""
    
    # Armazenar PID para cleanup depois
    echo "$pid" >> .pids
}

# Limpar arquivo de PIDs antigos
rm -f .pids

# 3.1 Voice Agent (Port 3002)
echo -e "${BLUE}1️⃣  Voice Agent${NC}"
run_process "🎤 Voice Agent" "node voice_agent_realtime.js" "/tmp/voice-agent.log"

# Esperar Voice Agent iniciar
sleep 2

# 3.2 API (Port 3001)
echo -e "${BLUE}2️⃣  API Signals${NC}"
run_process "📊 API Signals" "PORT=3001 node api_signals.js" "/tmp/api.log"

# Esperar API iniciar
sleep 2

# 3.3 Daemon Python
echo -e "${BLUE}3️⃣  Daemon (Analysis)${NC}"
run_process "🐍 Daemon" "python3 intelligent_daemon_mysql.py" "/tmp/daemon.log"

# Esperar Daemon iniciar
sleep 2

# 3.4 Scheduler (opcional, comentado)
# Decomente se quiser rodar scheduler também
# echo -e "${BLUE}4️⃣  Advanced Scheduler${NC}"
# run_process "⏰ Scheduler" "python3 advanced_scheduler.py" "/tmp/scheduler.log"

echo ""

# ============================================================
# 4. VERIFICAR STATUS
# ============================================================

echo "╔════════════════════════════════════════════════════════╗"
echo "║          ✅ TODOS OS PROCESSOS INICIADOS               ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""
echo "📊 STATUS:"
echo ""
echo -e "${GREEN}✅ Voice Agent${NC}   - Port 3002 🎤"
echo -e "${GREEN}✅ API Signals${NC}   - Port 3001 📊"
echo -e "${GREEN}✅ Daemon${NC}        - Python 🐍"
echo ""
echo "📚 LOGS:"
echo "  Voice Agent: tail -f /tmp/voice-agent.log"
echo "  API:         tail -f /tmp/api.log"
echo "  Daemon:      tail -f /tmp/daemon.log"
echo ""
echo "🌐 ENDPOINTS:"
echo "  Voice Agent Status: curl http://localhost:3002/voice/status"
echo "  API Signals:        curl http://localhost:3001/signals"
echo "  Make Call:          curl -X POST http://localhost:3002/voice/call ..."
echo ""
echo "🛑 PARAR TODOS:"
echo "  kill \$(cat .pids)"
echo ""
echo "═════════════════════════════════════════════════════════"
echo ""
echo -e "${BLUE}🎤 Nina está ouvindo...${NC}"
echo -e "${BLUE}📊 Sistema rodando 24/7...${NC}"
echo -e "${BLUE}💪 Pronto para trabalhar!${NC}"
echo ""

# ============================================================
# 5. MANTER ABERTO
# ============================================================

# Função para limpar ao sair
cleanup() {
    echo ""
    echo "🛑 Encerrando processos..."
    
    if [ -f ".pids" ]; then
        while IFS= read -r pid; do
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null || true
                echo "  Parado PID: $pid"
            fi
        done < ".pids"
        rm -f .pids
    fi
    
    echo "✅ Todos os processos encerrados"
    exit 0
}

# Registrar trap para Ctrl+C
trap cleanup SIGINT SIGTERM

# Manter script rodando e monitorar processos
echo -e "${YELLOW}⏳ Monitorando processos (Ctrl+C para parar)...${NC}"
echo ""

while true; do
    sleep 5
    
    # Verificar se algum processo morreu
    if [ -f ".pids" ]; then
        while IFS= read -r pid; do
            if ! kill -0 "$pid" 2>/dev/null; then
                echo -e "${RED}❌ Processo $pid morreu!${NC}"
                echo "Reiniciando..."
                rm -f .pids
                exec "$0"
            fi
        done < ".pids"
    fi
done
