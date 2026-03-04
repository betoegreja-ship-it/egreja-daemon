FROM node:22-alpine

WORKDIR /app

# Instalar Python e dependências
RUN apk add --no-cache python3 py3-pip

# Copiar package.json e instalar dependências Node
COPY package.json package-lock.json* pnpm-lock.yaml* ./
RUN npm install --legacy-peer-deps --production 2>/dev/null || npm install --force --production

# Copiar requirements Python
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copiar código
COPY api_signals.js .
COPY intelligent_daemon_mysql.py .
COPY technical_analysis.py .
COPY market_data.py .
COPY trade_signals.py .

# Expose port 3001
EXPOSE 3001

# Run both processes via shell script
RUN echo '#!/bin/sh\n\
echo "Starting API Server on port 3001..."\n\
PORT=3001 node api_signals.js &\n\
API_PID=$!\n\
\n\
echo "Starting Python Daemon..."\n\
python3 intelligent_daemon_mysql.py &\n\
DAEMON_PID=$!\n\
\n\
trap "kill $DAEMON_PID $API_PID 2>/dev/null" SIGTERM SIGINT\n\
wait' > /app/start.sh && chmod +x /app/start.sh

CMD ["/app/start.sh"]
