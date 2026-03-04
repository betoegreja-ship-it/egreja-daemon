FROM node:22-alpine

WORKDIR /app

# Instalar Python e dependências do sistema
RUN apk add --no-cache python3 py3-pip bash

# Copiar requirements Python primeiro (melhor cache)
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copiar package.json e instalar dependências Node
COPY package.json package-lock.json* pnpm-lock.yaml* ./
RUN npm install --legacy-peer-deps --production 2>/dev/null || npm install --force --production

# Copiar código da aplicação
COPY api_signals.js .
COPY intelligent_daemon_mysql.py .
COPY technical_analysis.py .
COPY market_data.py .
COPY trade_signals.py .
COPY entrypoint.sh .
RUN chmod +x /app/entrypoint.sh

# Expose port 3001
EXPOSE 3001

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3001/health', (r) => {if (r.statusCode !== 200) throw new Error(r.statusCode)})"

# Run via entrypoint
CMD ["/app/entrypoint.sh"]
