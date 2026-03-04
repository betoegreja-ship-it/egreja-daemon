import { getDb } from './server/db.js';
import { trades, sofiaAnalyses, sofiaMetrics } from './drizzle/schema.js';

async function checkDatabase() {
  try {
    const db = await getDb();
    if (!db) {
      console.log('❌ Banco de dados não disponível');
      return;
    }
    
    console.log('✅ Conectado ao banco de dados!\n');
    
    // Verificar trades
    const allTrades = await db.select().from(trades);
    console.log(`📊 Total de trades no banco: ${allTrades.length}`);
    
    const openTrades = allTrades.filter(t => t.status === 'OPEN');
    console.log(`📈 Trades abertos: ${openTrades.length}`);
    
    const closedTrades = allTrades.filter(t => t.status === 'CLOSED');
    console.log(`📉 Trades fechados: ${closedTrades.length}\n`);
    
    if (closedTrades.length > 0) {
      console.log('📋 Últimos 5 trades fechados:');
      closedTrades.slice(0, 5).forEach(t => {
        console.log(`  - ${t.symbol}: ${t.recommendation} | P&L: ${t.pnl} | ${t.closedAt}`);
      });
      console.log('');
    }
    
    // Verificar análises de Sofia
    const analyses = await db.select().from(sofiaAnalyses);
    console.log(`🧠 Total de análises de Sofia: ${analyses.length}`);
    
    if (analyses.length > 0) {
      console.log('📋 Últimas 3 análises:');
      analyses.slice(0, 3).forEach(a => {
        console.log(`  - ${a.symbol}: ${a.recommendation} (${a.confidence}%) | ${a.createdAt}`);
      });
      console.log('');
    }
    
    // Verificar métricas
    const metrics = await db.select().from(sofiaMetrics);
    console.log(`📊 Métricas de Sofia por símbolo: ${metrics.length}`);
    
    if (metrics.length > 0) {
      console.log('📋 Top 3 métricas:');
      metrics.slice(0, 3).forEach(m => {
        console.log(`  - ${m.symbol}: ${m.accuracy}% acurácia | ${m.totalTrades} trades`);
      });
    }
    
  } catch (error) {
    console.error('❌ Erro:', error.message);
  }
  
  process.exit(0);
}

checkDatabase();
