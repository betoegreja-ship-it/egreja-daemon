FROM node:22-alpine

WORKDIR /app

# Copiar package.json
COPY package.json package-lock.json* ./

# Instalar dependências (sem cache, força limpo)
RUN npm ci --omit=dev 2>/dev/null || npm install --legacy-peer-deps --only=production 2>/dev/null || npm install --force --only=production

# Copiar apenas o que precisa da API
COPY api_signals.js .

# Expose port 3001
EXPOSE 3001

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:3001/health || exit 1

# Run API
CMD ["node", "api_signals.js"]
