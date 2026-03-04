const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8000;
const SIGNALS_FILE = '/tmp/egreja-signals.json';

// Função para ler sinais do arquivo JSON
function getSignals() {
  try {
    if (fs.existsSync(SIGNALS_FILE)) {
      const data = fs.readFileSync(SIGNALS_FILE, 'utf-8');
      return JSON.parse(data);
    }
    return { error: 'Signals file not found' };
  } catch (err) {
    return { error: err.message };
  }
}

// Criar server HTTP
const server = http.createServer((req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Content-Type', 'application/json');

  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  // Rota: /signals
  if (req.url === '/signals' || req.url === '/') {
    const signals = getSignals();
    res.writeHead(200);
    res.end(JSON.stringify(signals, null, 2));
    return;
  }

  // Rota: /health
  if (req.url === '/health') {
    res.writeHead(200);
    res.end(JSON.stringify({ status: 'OK', timestamp: new Date().toISOString() }));
    return;
  }

  // 404
  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found' }));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n===============================================`);
  console.log(`🚀 Egreja Investment AI - API Server`);
  console.log(`Server running on port ${PORT}`);
  console.log(`Endpoints:`);
  console.log(`  GET /signals   - Latest market signals`);
  console.log(`  GET /health    - Health check`);
  console.log(`===============================================\n`);
});

// Handle errors
server.on('error', (err) => {
  console.error(`❌ Server error:`, err);
});

process.on('SIGTERM', () => {
  console.log('\n👋 Server shutting down...');
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});
