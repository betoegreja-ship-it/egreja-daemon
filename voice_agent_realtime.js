#!/usr/bin/env node
/**
 * NINA VOICE AGENT - Realtime Conversation
 * 
 * Conversa bidirecional em tempo real usando:
 * - OpenAI Realtime API (STT + LLM + TTS)
 * - Twilio Voice (inbound/outbound calls)
 * - WebSocket para audio streaming
 * 
 * Criado: 2026-03-04 22:14 GMT-3
 * Autor: Nina Egreja
 * Status: ÉPICO! 🎤🦅
 */

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const twilio = require('twilio');
const openai = require('openai');
const { Readable } = require('stream');
const dotenv = require('dotenv');

dotenv.config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Configurações
const PORT = process.env.VOICE_AGENT_PORT || 3002;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN;
const TWILIO_PHONE_NUMBER = process.env.TWILIO_PHONE_NUMBER;
const YOUR_PHONE_NUMBER = process.env.YOUR_PHONE_NUMBER || '+5511948600022';

if (!OPENAI_API_KEY || !TWILIO_ACCOUNT_SID) {
  console.error('❌ Faltam credenciais: OPENAI_API_KEY ou TWILIO_ACCOUNT_SID');
  process.exit(1);
}

const twilioClient = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
const openaiClient = new openai.OpenAI({ apiKey: OPENAI_API_KEY });

// Store active calls
const activeCalls = new Map();

console.log('🎤 NINA VOICE AGENT - Iniciando...');
console.log(`📞 Twilio: ${TWILIO_PHONE_NUMBER}`);
console.log(`🎙️ OpenAI: Realtime API`);

// ============================================
// 1. WEBHOOK PARA TWILIO (Inbound Calls)
// ============================================

app.post('/voice/inbound', express.urlencoded({ extended: false }), (req, res) => {
  const { From, To, CallSid } = req.body;

  console.log(`📥 CHAMADA RECEBIDA: ${From} → ${To} (Call: ${CallSid})`);

  const twiml = new twilio.twiml.VoiceResponse();

  // Mensagem de boas-vindas
  twiml.say(
    { voice: 'alice', language: 'pt-BR' },
    'Olá! Você está falando com Nina, a assistente de voz do Beto. Como posso ajudar?'
  );

  // Conectar ao WebSocket para conversa bidirecional
  const wsUrl = `wss://${req.get('host')}/voice/stream?callSid=${CallSid}&from=${From}`;
  
  twiml.connect()
    .stream({ url: wsUrl });

  res.type('text/xml');
  res.send(twiml.toString());
});

// ============================================
// 2. WEBSOCKET PARA AUDIO STREAMING
// ============================================

wss.on('connection', (ws, req) => {
  const url = req.url;
  const params = new URL(url, `wss://dummy`).searchParams;
  const callSid = params.get('callSid');
  const from = params.get('from');

  console.log(`🔌 WebSocket conectado: ${callSid} de ${from}`);

  let realtimeSession = null;
  let audioBuffer = [];
  let isListening = true;

  // Criar sessão OpenAI Realtime
  const createRealtimeSession = async () => {
    try {
      console.log(`🎙️ Iniciando sessão Realtime para ${callSid}...`);

      // Conectar ao OpenAI Realtime API
      const response = await openaiClient.beta.realtime.sessions.create({
        model: 'gpt-4-realtime-preview-2024-12-26',
        modalities: ['text', 'audio'],
        instructions: `Você é Nina, a assistente de voz do Beto Egreja. 
          - Fale em português do Brasil, naturalmente
          - Seja amigável mas profissional
          - Se a pessoa perguntar algo sobre investimentos, ofereça ajuda
          - Se for assunto confidencial, conecte com o Beto
          - Nunca prometa coisas que não pode fazer
          - Seja breve e objetiva`,
        voice: 'alloy',
        temperature: 0.8,
      });

      realtimeSession = response;
      console.log(`✅ Sessão criada: ${realtimeSession.id}`);

      return response;
    } catch (error) {
      console.error(`❌ Erro ao criar sessão Realtime: ${error.message}`);
      throw error;
    }
  };

  // Processar áudio recebido do Twilio
  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);

      switch (message.event) {
        case 'start':
          console.log(`📱 Chamada iniciada: ${callSid}`);
          activeCalls.set(callSid, { from, ws, createdAt: new Date() });
          
          // Criar sessão Realtime
          await createRealtimeSession();
          
          break;

        case 'media':
          if (!realtimeSession || !isListening) break;

          // Audio payload do Twilio (base64 encoded)
          const audioData = message.media.payload;
          audioBuffer.push(Buffer.from(audioData, 'base64'));

          // Enviar para OpenAI (a cada 100ms de áudio)
          if (audioBuffer.length >= 2) {
            const combinedAudio = Buffer.concat(audioBuffer);
            audioBuffer = [];

            // Aqui você enviaria para a API Realtime
            // Por enquanto, simulamos uma resposta
            simulateResponse(ws, callSid);
          }

          break;

        case 'stop':
          console.log(`📵 Chamada finalizada: ${callSid}`);
          isListening = false;
          activeCalls.delete(callSid);
          ws.close();
          break;
      }
    } catch (error) {
      console.error(`❌ Erro processando mensagem: ${error.message}`);
    }
  });

  ws.on('close', () => {
    console.log(`🔌 WebSocket desconectado: ${callSid}`);
    activeCalls.delete(callSid);
  });

  ws.on('error', (error) => {
    console.error(`❌ WebSocket error: ${error.message}`);
    activeCalls.delete(callSid);
  });
});

// ============================================
// 3. FUNÇÃO PARA FAZER CHAMADAS (Outbound)
// ============================================

async function makeCall(toNumber, script = null) {
  try {
    console.log(`📞 Fazendo chamada para: ${toNumber}`);

    const call = await twilioClient.calls.create({
      from: TWILIO_PHONE_NUMBER,
      to: toNumber,
      url: `${process.env.NGROK_URL || 'http://localhost:3002'}/voice/outbound`,
      record: false,
      machineDetection: 'Enable',
    });

    console.log(`✅ Chamada criada: ${call.sid}`);
    return call;
  } catch (error) {
    console.error(`❌ Erro ao fazer chamada: ${error.message}`);
    throw error;
  }
}

// ============================================
// 4. WEBHOOK PARA CHAMADAS SAINTES
// ============================================

app.post('/voice/outbound', express.urlencoded({ extended: false }), (req, res) => {
  const { CallSid, AnsweredBy } = req.body;

  console.log(`📤 Chamada sainte: ${CallSid}, Atendida por: ${AnsweredBy}`);

  const twiml = new twilio.twiml.VoiceResponse();

  if (AnsweredBy === 'human') {
    // Pessoa atendeu
    twiml.say(
      { voice: 'alice', language: 'pt-BR' },
      'Olá! Você está falando com Nina, a assistante de voz do Beto. Como você está?'
    );

    // Conectar WebSocket para conversa
    const wsUrl = `wss://${req.get('host')}/voice/stream?callSid=${CallSid}&outbound=true`;
    twiml.connect()
      .stream({ url: wsUrl });
  } else {
    // Máquina atendeu ou sem resposta
    twiml.say(
      { voice: 'alice', language: 'pt-BR' },
      'Ninguém atendeu ou foi detectada uma máquina. Desligando.'
    );
    twiml.hangup();
  }

  res.type('text/xml');
  res.send(twiml.toString());
});

// ============================================
// 5. SIMULAÇÃO DE RESPOSTA (Enquanto integra IA)
// ============================================

function simulateResponse(ws, callSid) {
  const responses = [
    'Que bom te ouvir!',
    'Como posso ajudar?',
    'Tudo bem com você?',
    'Deixa eu anotar isso.',
    'Entendi perfeitamente.',
    'Isso é muito interessante.',
  ];

  const randomResponse = responses[Math.floor(Math.random() * responses.length)];

  // Enviar áudio de resposta (simulado)
  ws.send(JSON.stringify({
    type: 'response',
    text: randomResponse,
    timestamp: Date.now(),
  }));
}

// ============================================
// 6. API ENDPOINTS
// ============================================

app.get('/voice/status', (req, res) => {
  res.json({
    status: 'online',
    agent: 'nina-voice',
    activeCalls: activeCalls.size,
    calls: Array.from(activeCalls.entries()).map(([sid, call]) => ({
      sid,
      from: call.from,
      duration: Date.now() - call.createdAt.getTime(),
    })),
  });
});

app.post('/voice/call', express.json(), async (req, res) => {
  const { to, script } = req.body;

  if (!to) {
    return res.status(400).json({ error: 'Número é obrigatório' });
  }

  try {
    const call = await makeCall(to, script);
    res.json({ success: true, callSid: call.sid });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/voice/calls', (req, res) => {
  res.json({
    activeCalls: Array.from(activeCalls.entries()).map(([sid, call]) => ({
      callSid: sid,
      from: call.from,
      startedAt: call.createdAt,
      duration: Date.now() - call.createdAt.getTime(),
    })),
  });
});

// ============================================
// 7. INICIAR SERVER
// ============================================

server.listen(PORT, () => {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`🎤 NINA VOICE AGENT - ONLINE`);
  console.log(`${'='.repeat(60)}`);
  console.log(`📞 Port: ${PORT}`);
  console.log(`🔗 Inbound: POST /voice/inbound`);
  console.log(`🔗 Outbound: POST /voice/outbound`);
  console.log(`📊 Status: GET /voice/status`);
  console.log(`📞 Make Call: POST /voice/call (body: {to, script})`);
  console.log(`${'='.repeat(60)}\n`);

  console.log('✅ Pronto para receber e fazer chamadas!');
  console.log('✅ Nina está ouvindo...\n');
});

// ============================================
// 8. GRACEFUL SHUTDOWN
// ============================================

process.on('SIGINT', () => {
  console.log('\n⏹️ Encerrando servidor...');
  server.close(() => {
    console.log('✅ Servidor desligado');
    process.exit(0);
  });
});

module.exports = { makeCall, activeCalls };
