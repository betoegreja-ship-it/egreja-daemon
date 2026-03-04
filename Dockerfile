FROM node:22-alpine

WORKDIR /app

# Copiar apenas arquivo da API (sem package.json - não precisa de deps)
COPY api_signals_simple.js .

# Expose port 3001
EXPOSE 3001

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:3001/health || exit 1

# Run API simples
CMD ["node", "api_signals_simple.js"]
