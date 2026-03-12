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
      'SELECT symbol, market_type, price, score, `signal`, rsi, ema9, ema21, ema50, created_at FROM market_signals ORDER BY created_at DESC LIMIT 40'
    );
    conn.release();
    return rows || [];
  } catch (err) {
    console.error('Query error:', err.message);
    return [];
  }
}

// ═══════════════════════════════════════════════════
// SYNC — Network Intelligence (Railway ↔ Manus)
// ═══════════════════════════════════════════════════
let railwaySyncSnapshot = null; // último dado recebido do Railway

async function getSyncExport() {
  try {
    if (!pool) return null;
    const conn = await pool.getConnection();

    // Sinais quentes (score >= 70, últimas 2h)
    const [hotRows] = await conn.query(`
      SELECT symbol, market_type, signal, score, created_at
      FROM market_signals
      WHERE score >= 70
        AND created_at >= NOW() - INTERVAL 2 HOUR
      ORDER BY score DESC
      LIMIT 20
    `);

    // Estatísticas de win/loss por mercado (se tabela trades existir)
    let marketStats = {};
    try {
      const [mktRows] = await conn.query(`
        SELECT market_type,
               COUNT(*) as total,
               SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
               AVG(pnl_pct) as avg_pnl_pct,
               SUM(pnl) as total_pnl
        FROM trades
        WHERE status = 'CLOSED'
          AND closed_at >= NOW() - INTERVAL 30 DAY
        GROUP BY market_type
      `);
      mktRows.forEach(r => {
        const t = parseInt(r.total || 0);
        marketStats[r.market_type || 'UNKNOWN'] = {
          total_trades: t,
          win_rate: t ? Math.round(parseInt(r.wins || 0) / t * 100) : 0,
          avg_pnl_pct: parseFloat(r.avg_pnl_pct || 0).toFixed(2),
          total_pnl: parseFloat(r.total_pnl || 0).toFixed(2),
        };
      });
    } catch (e) { /* tabela trades pode não existir */ }

    // Padrões por símbolo
    const [patRows] = await conn.query(`
      SELECT symbol,
             COUNT(*) as total,
             AVG(score) as avg_score,
             SUM(CASE WHEN signal='BUY' THEN 1 ELSE 0 END) as buys
      FROM market_signals
      WHERE created_at >= NOW() - INTERVAL 7 DAY
      GROUP BY symbol
      HAVING total >= 3
      ORDER BY avg_score DESC
      LIMIT 30
    `);

    conn.release();

    const hotSignals = hotRows.map(s => ({
      symbol: s.symbol,
      action: s.signal || 'BUY',
      score: parseFloat(s.score || 0),
      confidence: parseFloat(s.score || 0),
      market: s.market_type || 'CRYPTO',
      age_min: s.created_at ? Math.round((Date.now() - new Date(s.created_at)) / 60000) : 0,
    }));

    const topPatterns = patRows.map(p => ({
      key: p.symbol,
      win_rate: 50,
      total_samples: parseInt(p.total || 0),
      avg_pnl: 0,
      confidence: Math.round(parseFloat(p.avg_score || 0)),
    }));

    return {
      system: 'egreja-manus',
      sync_version: '1.0',
      exported_at: new Date().toISOString(),
      learning: {
        total_patterns: topPatterns.length,
        avg_confidence: topPatterns.length
          ? Math.round(topPatterns.reduce((s, p) => s + p.confidence, 0) / topPatterns.length)
          : 0,
        learning_enabled: true,
      },
      top_patterns: topPatterns,
      hot_signals: hotSignals,
      market_stats: marketStats,
    };
  } catch (e) {
    console.error('[SYNC] getSyncExport error:', e.message);
    return null;
  }
}

async function readBody(req) {
  return new Promise((resolve) => {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try { resolve(JSON.parse(body)); } catch { resolve({}); }
    });
  });
}

const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-API-Key, Authorization');
  res.setHeader('Content-Type', 'application/json');

  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  const parsedUrl = new URL(req.url || '/', `http://${req.headers.host}`);
  const pathname = parsedUrl.pathname;

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

  if (pathname === '/health' && req.method === 'GET') {
    res.writeHead(200);
    res.end(JSON.stringify({
      status: 'OK',
      service: 'egreja-manus',
      system: 'egreja-manus',
      timestamp: new Date().toISOString()
    }));
    return;
  }

  // ── SYNC: Export ──────────────────────────────────────
  if (pathname === '/sync/export' && req.method === 'GET') {
    const data = await getSyncExport();
    if (data) {
      res.writeHead(200);
      res.end(JSON.stringify(data, null, 2));
    } else {
      res.writeHead(503);
      res.end(JSON.stringify({ error: 'Data not available' }));
    }
    return;
  }

  // ── SYNC: Import (recebe dados do Railway) ────────────
  if (pathname === '/sync/import' && req.method === 'POST') {
    const body = await readBody(req);
    railwaySyncSnapshot = {
      data: body,
      receivedAt: new Date().toISOString(),
    };
    const signals = body.hot_signals || [];
    const patterns = body.top_patterns || [];
    console.log(`[SYNC] ✅ Recebido do Railway: ${signals.length} sinais quentes, ${patterns.length} padrões`);
    res.writeHead(200);
    res.end(JSON.stringify({
      status: 'ok',
      received: { source: body.system, hot_signals: signals.length, patterns: patterns.length },
      timestamp: new Date().toISOString(),
    }));
    return;
  }

  // ── SYNC: Ver último snapshot do Railway ──────────────
  if (pathname === '/sync/latest' && req.method === 'GET') {
    res.writeHead(200);
    res.end(JSON.stringify(railwaySyncSnapshot || { status: 'no_data', message: 'Nenhum sync recebido ainda' }));
    return;
  }
  res.end(JSON.stringify({ error: 'Not found' }));
});

async function start() {
  try {
    await initDB();
    server.listen(PORT, '0.0.0.0', () => {
      console.log(`\n===============================================`);
      console.log(`🚀 Egreja Signals API`);
      console.log(`Server running on port ${PORT}`);
      console.log(`Endpoints:`);
      console.log(`  GET /signals       - Latest market signals from MySQL`);
      console.log(`  GET /health        - Health check`);
      console.log(`  GET /sync/export   - Export intelligence for Railway sync`);
      console.log(`  POST /sync/import  - Receive intelligence from Railway`);
      console.log(`  GET /sync/latest   - Last Railway snapshot received`);
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
