const mysql = require('mysql2/promise');

async function fullAudit() {
  const conn = await mysql.createConnection(process.env.DATABASE_URL);
  
  const [all] = await conn.execute(
    'SELECT id, symbol, recommendation, entry_price, exit_price, quantity, pnl, pnl_percent, close_reason FROM trades WHERE status=\'CLOSED\' ORDER BY id ASC'
  );
  
  const problems = [];
  
  for (const t of all) {
    const entry = parseFloat(t.entry_price || '0');
    const exit_p = parseFloat(t.exit_price || '0');
    const pnl = parseFloat(t.pnl || '0');
    const pnlPct = parseFloat(t.pnl_percent || '0');
    const qty = parseFloat(t.quantity || '0');
    
    const issues = [];
    
    if (!exit_p || exit_p === 0) issues.push('SEM_EXIT');
    if (!entry || entry === 0) issues.push('SEM_ENTRY');
    if (entry > 0 && exit_p > 0 && Math.abs(entry - exit_p) < 0.000001) issues.push('ENTRY_IGUAL_EXIT');
    if (entry > 0 && exit_p > 0 && Math.abs(entry - exit_p) > 0.000001 && pnl === 0) issues.push('PNL_ZERO_PRECOS_DIFERENTES');
    
    // Verificar se P&L percentual está correto
    if (entry > 0 && exit_p > 0 && qty > 0) {
      const expectedPct = t.recommendation === 'BUY' 
        ? (exit_p - entry) / entry * 100
        : (entry - exit_p) / entry * 100;
      if (Math.abs(expectedPct - pnlPct) > 0.5) {
        issues.push('PNL_PCT_ERRADO:esperado=' + expectedPct.toFixed(3) + '%,salvo=' + pnlPct.toFixed(3) + '%');
      }
    }
    
    // Verificar se P&L em USD está correto (deve ser pnl_pct/100 * position_size)
    if (entry > 0 && exit_p > 0 && qty > 0 && pnlPct !== 0) {
      const posSize = entry * qty;
      const expectedPnl = (pnlPct / 100) * posSize;
      if (Math.abs(expectedPnl - pnl) > 1.0) { // tolerância de $1
        issues.push('PNL_USD_ERRADO:esperado=$' + expectedPnl.toFixed(2) + ',salvo=$' + pnl.toFixed(2));
      }
    }
    
    if (issues.length > 0) {
      problems.push({ id: t.id, symbol: t.symbol, rec: t.recommendation, entry, exit_p, qty, pnl, pnlPct, reason: t.close_reason, issues });
    }
  }
  
  console.log('=== VARREDURA COMPLETA: ' + all.length + ' trades analisadas ===');
  console.log('Com problemas: ' + problems.length);
  console.log('');
  
  // Agrupar por tipo
  const groups = {};
  for (const p of problems) {
    for (const issue of p.issues) {
      const key = issue.split(':')[0];
      if (!groups[key]) groups[key] = [];
      groups[key].push(p);
    }
  }
  
  for (const [issue, trades] of Object.entries(groups)) {
    console.log('--- ' + issue + ': ' + trades.length + ' trades ---');
    for (const t of trades) {
      console.log('  #' + t.id, t.symbol, t.rec,
        'entry:', t.entry, 'exit:', t.exit_p,
        'qty:', t.qty.toFixed(2),
        'pos_size: $' + (t.entry * t.qty).toFixed(0),
        'pnl_pct:', t.pnlPct.toFixed(3) + '%',
        'pnl: $' + t.pnl.toFixed(2),
        'reason:', t.reason);
    }
    console.log('');
  }
  
  await conn.end();
}

fullAudit().catch(console.error);
