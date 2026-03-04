#!/usr/bin/env node
/**
 * API Signals Server - Retorna sinais de mercado do MySQL
 * GET /signals - Retorna últimos sinais
 * GET /health - Health check
 */

import http from 'http';
import mysql from 'mysql2/promise';
import { URL } from 'url';

const PORT = process.env.PORT || 3001;

// Config MySQL
const dbConfig = {
  host: process.env.MYSQLHOST || 'localhost',
  user: process.env.MYSQLUSER || 'root',
  password: process.env.MYSQLPASSWORD || '',
  database: process.env.MYSQLDATABASE || 'railway',
  port: parseInt(process.env.MYSQLPORT || '3306'),
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelayMs: 0
};

let pool = null;

async function initDB() {
  try {
    pool = mysql.createPool(dbConfig);
    console.log(`✅ MySQL pool criado - Host: ${dbConfig.host}, DB: ${dbConfig.database}`);
    // Test connection
    const conn = await pool.getConnection();
    await conn.ping();
    conn.release();
    console.log(`✅ MySQL conexão testada com sucesso`);
  } catch (err) {
    console.error(`❌ MySQL pool erro:`, err.message);
    throw err;
  }
}

async function getSignals() {
  try {
    if (!pool) throw new Error('Pool not initialized');
    const conn = await pool.getConnection();
    const [rows] = await conn.query(
      'SELECT symbol, market_type, price, score, signal, rsi, ema9, ema21, ema50, created_at FROM market_signals ORDER BY created_at DESC LIMIT 40'
    );
    conn.release();
    return rows || [];
  } catch (err) {
    console.error('Query error:', err.message);
    return [];
  }
}

// HTTP Server
const server = http.createServer(async (req, res) => {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Content-Type', 'application/json');

  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  const parsedUrl = new URL(req.url || '/', `http://${req.headers.host}`);
  const pathname = parsedUrl.pathname;

  // GET /signals
  if (pathname === '/signals' && req.method === 'GET') {
    const signals = await getSignals();
    res.writeHead(200);
    res.end(JSON.stringify({
      status: 'OK',
      timestamp: new Date().toISOString(),
      total: signals.length,
      signals: signals.map(s => ({
        symbol: s.symbol,
        market_type: s.market_type,
        price: parseFloat(s.price),
        score: s.score,
        signal: s.signal,
        rsi: parseFloat(s.rsi),
        ema9: parseFloat(s.ema9),
        ema21: parseFloat(s.ema21),
        ema50: parseFloat(s.ema50),
        created_at: s.created_at
      }))
    }, null, 2));
    return;
  }

  // GET /health
  if (pathname === '/health' && req.method === 'GET') {
    res.writeHead(200);
    res.end(JSON.stringify({
      status: 'OK',
      service: 'egreja-signals-api',
      timestamp: new Date().toISOString()
    }));
    return;
  }

  // 404
  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found' }));
});

// Start
async function start() {
  try {
    await initDB();
    
    server.listen(PORT, '0.0.0.0', () => {
      console.log(`\n===============================================`);
      console.log(`🚀 Egreja Signals API`);
      console.log(`Server running on port ${PORT}`);
      console.log(`Endpoints:`);
      console.log(`  GET /signals  - Latest market signals from MySQL`);
      console.log(`  GET /health   - Health check`);
      console.log(`===============================================\n`);
    });
  } catch (err) {
    console.error('Fatal error:', err);
    process.exit(1);
  }
}

start();

process.on('SIGTERM', () => {
  console.log('\n👋 Server shutting down...');
  server.close(() => {
    process.exit(0);
  });
});
