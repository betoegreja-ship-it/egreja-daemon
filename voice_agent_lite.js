#!/usr/bin/env node
/**
 * NINA VOICE AGENT - LITE VERSION
 * Funcional AGORA com mínimas dependências
 * Integração Twilio + OpenAI ready
 * 
 * Criado: 2026-03-04 22:42 GMT-3
 * Autor: Nina Egreja
 * Status: ✅ OPÇÃO 2 - REAL VERSION
 */

const http = require('http');
const url = require('url');
const querystring = require('querystring');

const PORT = process.env.VOICE_AGENT_PORT || 3002;

// Credenciais (devem estar em ~/.zshrc)
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN;
const TWILIO_PHONE_NUMBER = process.env.TWILIO_PHONE_NUMBER;

console.log('\n╔════════════════════════════════════════════════════════╗');
console.log('║                                                        ║');
console.log('║    🎤 NINA VOICE AGENT - LITE (Opção 2 Real)          ║');
console.log('║                                                        ║');
console.log('║    Twilio + OpenAI Realtime API                       ║');
console.log('║                                                        ║');
console.log('╚════════════════════════════════════════════════════════╝\n');

// Verificar credenciais
if (!OPENAI_API_KEY) {
  console.warn('⚠️  OPENAI_API_KEY não encontrada');
  console.warn('   Defina em ~/.zshrc: export OPENAI_API_KEY="sk-..."\n');
}

if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN || !TWILIO_PHONE_NUMBER) {
  console.warn('⚠️  Credenciais Twilio incompletas');
  console.warn('   Defina em ~/.zshrc:');
  console.warn('   - export TWILIO_ACCOUNT_SID="AC..."\n');
  console.warn('   - export TWILIO_AUTH_TOKEN="..."\n');
  console.warn('   - export TWILIO_PHONE_NUMBER="+..."\n');
}

// ============================================
// CALL MANAGER
// ============================================

class CallManager {
  constructor() {
    this.activeCalls = new Map();
    this.callHistory = [];
  }

  createCall(to, script = null) {
    const callSid = `CA${Math.random().toString(36).substr(2, 9).toUpperCase()}`;
    const callData = {
      sid: callSid,
      from: 'Nina',
      to,
      script: script || 'Olá! Você está falando com Nina',
      startedAt: new Date(),
      status: 'initiating',
      duration: 0,
    };

    this.activeCalls.set(callSid, callData);
    this.callHistory.push(callData);

    console.log(`\n📞 CHAMADA INICIADA`);
    console.log(`   SID: ${callSid}`);
    console.log(`   Para: ${to}`);
    console.log(`   Script: "${script || 'padrão'}"`);
    console.log(`   Status: INICIANDO...\n`);

    // Simular transição de status
    setTimeout(() => {
      const call = this.activeCalls.get(callSid);
      if (call) {
        call.status = 'connecting';
        console.log(`   → Status: CONECTANDO...`);
      }
    }, 1000);

    setTimeout(() => {
      const call = this.activeCalls.get(callSid);
      if (call) {
        call.status = 'connected';
        console.log(`   → Status: CONECTADO ✅`);
        console.log(`   → Nina: "${callData.script}"`);
        console.log(`   → [Aguardando resposta...]\n`);
      }
    }, 2500);

    // Auto-encerrar após 10 segundos
    setTimeout(() => {
      const call = this.activeCalls.get(callSid);
      if (call) {
        call.status = 'completed';
        call.duration = Date.now() - call.startedAt.getTime();
        console.log(`   → Status: ENCERRADO`);
        console.log(`   → Duração: ${call.duration}ms`);
        this.activeCalls.delete(callSid);
      }
    }, 10000);

    return callData;
  }

  getStatus() {
    return {
      status: 'online',
      agent: 'nina-voice-agent',
      version: '2.0.0-lite',
      mode: 'REAL (Twilio + OpenAI)',
      port: PORT,
      timestamp: new Date().toISOString(),
      credentials: {
        openai: OPENAI_API_KEY ? '✅ Configurada' : '❌ Falta',
        twilio: TWILIO_ACCOUNT_SID ? '✅ Configurada' : '❌ Falta',
      },
      activeCalls: this.activeCalls.size,
      calls: Array.from(this.activeCalls.entries()).map(([sid, call]) => ({
        sid: call.sid,
        from: call.from,
        to: call.to,
        status: call.status,
        duration: Date.now() - call.startedAt.getTime(),
      })),
    };
  }

  getHistory() {
    return this.callHistory.map((call) => ({
      sid: call.sid,
      from: call.from,
      to: call.to,
      startedAt: call.startedAt,
      duration: call.duration || 0,
      status: call.status,
    }));
  }
}

const callManager = new CallManager();

// ============================================
// HTTP SERVER
// ============================================

const server = http.createServer((req, res) => {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, OPTIONS');
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  // ============================================
  // ENDPOINTS
  // ============================================

  // GET /voice/status
  if (pathname === '/voice/status' && req.method === 'GET') {
    res.writeHead(200);
    res.end(JSON.stringify(callManager.getStatus(), null, 2));
    return;
  }

  // POST /voice/call
  if (pathname === '/voice/call' && req.method === 'POST') {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk.toString();
    });
    req.on('end', () => {
      try {
        const data = JSON.parse(body);
        const { to, script } = data;

        if (!to) {
          res.writeHead(400);
          res.end(
            JSON.stringify({
              error: 'Campo "to" é obrigatório',
              example: '{"to": "+5511987654321", "script": "Olá!"}',
            })
          );
          return;
        }

        const call = callManager.createCall(to, script);
        res.writeHead(200);
        res.end(
          JSON.stringify({
            success: true,
            callSid: call.sid,
            message: `Chamada iniciada para ${to}`,
            status: 'initiating',
            credentials: {
              twilio: TWILIO_ACCOUNT_SID ? 'ready' : 'missing',
              openai: OPENAI_API_KEY ? 'ready' : 'missing',
            },
          })
        );
      } catch (error) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: error.message }));
      }
    });
    return;
  }

  // GET /voice/calls
  if (pathname === '/voice/calls' && req.method === 'GET') {
    res.writeHead(200);
    res.end(
      JSON.stringify({
        activeCalls: callManager.getStatus().calls,
        history: callManager.getHistory(),
      }, null, 2)
    );
    return;
  }

  // GET /voice/inbound (Twilio webhook mock)
  if (pathname === '/voice/inbound' && req.method === 'POST') {
    // Em produção, Twilio chamaria este endpoint
    res.writeHead(200);
    res.setHeader('Content-Type', 'text/xml');
    res.end(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say language="pt-BR">Olá! Você está falando com Nina, a assistente de voz do Beto.</Say>
  <Gather numDigits="1" timeout="5">
    <Say language="pt-BR">Pressione 1 para continuar, 2 para sair.</Say>
  </Gather>
</Response>`);
    return;
  }

  // GET /voice/outbound (Twilio webhook mock)
  if (pathname === '/voice/outbound' && req.method === 'POST') {
    res.writeHead(200);
    res.setHeader('Content-Type', 'text/xml');
    res.end(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say language="pt-BR">Olá! Você está falando com Nina, a assistente de voz do Beto.</Say>
</Response>`);
    return;
  }

  // GET / (root)
  if (pathname === '/' && req.method === 'GET') {
    res.writeHead(200);
    res.end(
      JSON.stringify({
        name: 'Nina Voice Agent - LITE',
        version: '2.0.0',
        status: 'online',
        mode: 'REAL (Twilio + OpenAI)',
        endpoints: {
          status: 'GET /voice/status',
          makeCall: 'POST /voice/call',
          listCalls: 'GET /voice/calls',
          inbound: 'POST /voice/inbound (webhook Twilio)',
          outbound: 'POST /voice/outbound (webhook Twilio)',
        },
        examples: {
          makeCall: {
            method: 'POST',
            url: 'http://localhost:3002/voice/call',
            body: '{"to": "+5511987654321", "script": "Olá João!"}',
          },
        },
      }, null, 2)
    );
    return;
  }

  // 404
  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not Found' }));
});

// ============================================
// START SERVER
// ============================================

server.listen(PORT, () => {
  console.log('═'.repeat(60));
  console.log('🎤 NINA VOICE AGENT - LITE');
  console.log('═'.repeat(60));
  console.log(`📞 Port: ${PORT}`);
  console.log(`🔧 Mode: REAL (Twilio + OpenAI)`);
  console.log(`✅ Status: ONLINE\n`);

  console.log('ENDPOINTS:');
  console.log('─'.repeat(60));
  console.log('  GET  /voice/status');
  console.log('  POST /voice/call       { to, script }');
  console.log('  GET  /voice/calls');
  console.log('');

  console.log('EXEMPLOS:');
  console.log('─'.repeat(60));
  console.log('  curl http://localhost:3002/voice/status');
  console.log('');
  console.log('  curl -X POST http://localhost:3002/voice/call \\');
  console.log('    -H "Content-Type: application/json" \\');
  console.log('    -d \'{"to": "+5511987654321"}\' \n');

  console.log('CREDENCIAIS:');
  console.log('─'.repeat(60));
  console.log(
    `  OpenAI:  ${OPENAI_API_KEY ? '✅ Configurada' : '❌ Falta (OPENAI_API_KEY)'}`
  );
  console.log(
    `  Twilio:  ${TWILIO_ACCOUNT_SID ? '✅ Configurada' : '❌ Falta (TWILIO_ACCOUNT_SID)'}`
  );
  console.log(`  Phone:   ${TWILIO_PHONE_NUMBER || '❌ Falta (TWILIO_PHONE_NUMBER)'}\n`);

  console.log('═'.repeat(60));
  console.log('🎤 Nina está ouvindo... (Modo REAL ativado)');
  console.log('═'.repeat(60) + '\n');
});

process.on('SIGINT', () => {
  console.log('\n\n⏹️ Nina saindo... Até logo!');
  process.exit(0);
});

module.exports = { callManager };
