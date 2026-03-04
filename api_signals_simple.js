#!/usr/bin/env node
/**
 * SIMPLE API - Test without MySQL
 * Para verificar se Node está rodando
 */

import http from 'http';
import { URL } from 'url';

const PORT = process.env.PORT || 3001;

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');

  const url = new URL(req.url || '/', `http://${req.headers.host}`);
  const pathname = url.pathname;

  if (pathname === '/health') {
    res.writeHead(200);
    res.end(JSON.stringify({ status: 'OK', service: 'egreja-api' }));
    return;
  }

  if (pathname === '/signals') {
    // Mock data - 40 sinais
    const mockSignals = Array.from({ length: 40 }, (_, i) => ({
      symbol: `SYMBOL${i}`,
      market_type: 'B3',
      price: 100 + Math.random() * 50,
      score: 50 + Math.floor(Math.random() * 50),
      signal: '🟢 COMPRA',
      rsi: 50 + Math.random() * 30,
      ema9: 100,
      ema21: 105,
      ema50: 110,
      created_at: new Date().toISOString()
    }));

    res.writeHead(200);
    res.end(JSON.stringify({
      status: 'OK',
      timestamp: new Date().toISOString(),
      total: mockSignals.length,
      signals: mockSignals
    }, null, 2));
    return;
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found' }));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n${'='.repeat(50)}`);
  console.log(`🚀 Egreja API (SIMPLE - No MySQL)`);
  console.log(`Port: ${PORT}`);
  console.log(`GET /health  - Health check`);
  console.log(`GET /signals - Market signals (mock data)`);
  console.log(`${'='.repeat(50)}\n`);
});

process.on('SIGTERM', () => {
  console.log('\nShutting down...');
  server.close(() => process.exit(0));
});
