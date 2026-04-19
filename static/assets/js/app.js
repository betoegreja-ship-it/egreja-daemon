/* ═══════════════════════════════════════════════════════════════════════
   Egreja Investment AI — Frontend App (www.egreja.net)
   
   Estrutura lógica (não há import/namespace, é plain script — comentários
   servem como mapa mental para o próximo dev):

   [1] API CLIENT       : apiFetch, formatação (fmt*, fmtUSD, fmtBig, sc, pc)
   [2] STATE & BOOT     : const S = {}; fetchAll; setInterval
   [3] MARKET DATA      : updateMarkets, loadLivePrices, enrichment
   [4] OVERVIEW PAGE    : updateOverview, updateMarketStats, renderTable,
                          renderOpps, renderOpenTrades, renderHistory
   [5] TABS & FILTERS   : switchTab, switchHistTab, filterSignals,
                          sortSignals, makeSortable
   [6] REPORTS          : loadReport, renderReport, sendReport
   [7] OUTRAS PÁGINAS   : updatePortfolioPage, updateAnalytics,
                          loadArbitrage, loadWatchlist, wlAdd, wlRemove,
                          loadNetworkIntelligence, showPage
   [8] FX (USD/EUR)     : updateFxChips
   [9] CHARTS (Chart.js): _pc registry, _dc, _mc, barChart
   ═══════════════════════════════════════════════════════════════════════ */

const API_BASE='https://diligent-spirit-production.up.railway.app';
const API_KEY='262b29fb9a2d2b407fc3a2bbe9c48e819cc7a41b34195f9729b9612f6dc01c26';
function apiFetch(url, opts){
  opts=opts||{};
  opts.headers=Object.assign({'X-API-Key':API_KEY},opts.headers||{});
  return fetch(url, opts);
}
let allSignals=[],statsData={},currentFilter='ALL';

(function tick(){
  var el=document.getElementById('topbar-clock');
  if(el) el.textContent=new Date().toLocaleTimeString('pt-BR');
  setTimeout(tick,1000);
})();

function updateMarkets(){
  var n=new Date(),d=n.getDay(),h=n.getHours()*60+n.getMinutes(),wd=d>0&&d<6;
  var b3=wd&&h>=600&&h<1020,ny=wd&&h>=570&&h<960;
  var db=document.getElementById('dot-b3'),dnb=document.getElementById('b3-status');
  var dn=document.getElementById('dot-nyse'),dns=document.getElementById('nyse-status');
  if(db) db.style.background=b3?'#2ecc71':'#e74c3c';
  if(dnb) dnb.textContent=b3?'Aberto':'Fechado';
  if(dn) dn.style.background=ny?'#2ecc71':'#e74c3c';
  if(dns) dns.textContent=ny?'Aberto':'Fechado';
  ['b3-card-status','nyse-card-status'].forEach(function(id,i){
    var el=document.getElementById(id);if(!el)return;
    var open=i===0?b3:ny;
    el.textContent=open?'Open':'Closed';
    el.style.color=open?'#2ecc71':'#e74c3c';
  });
  var b3lbl=document.getElementById('b3-mkt-label');
  var nylbl=document.getElementById('nyse-mkt-label');
  if(b3lbl) b3lbl.textContent='● B3 BOVESPA — '+(b3?'Aberto':'Fechado');
  if(nylbl) nylbl.textContent='● NYSE / NASDAQ — '+(ny?'Aberto':'Fechado');
}
updateMarkets();setInterval(updateMarkets,60000);

function fmtNum(v,d){d=d||0;return v==null?'--':parseFloat(v).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});}
function fmtBig(v){
  if(v==null)return'--';
  var a=Math.abs(v),s=v<0?'-':'+'
  if(a>=1e6)return s+'$'+(a/1e6).toFixed(2)+'M';
  if(a>=1e3)return s+'$'+(a/1e3).toFixed(1)+'K';
  return s+'$'+a.toFixed(2);
}
function fmtUSD(v){if(v==null)return'--';var a=Math.abs(v),s=v<0?'-':'+';return s+'$'+a.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});}
function fmt(p,m){if(p==null)return'--';return m==='B3'?'R$'+parseFloat(p).toFixed(2):'$'+parseFloat(p).toFixed(2);}
function fmtT(ts){if(!ts)return'--';return new Date(ts).toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'});}
function fmtDT(ts){if(!ts)return'--';return new Date(ts).toLocaleString('pt-BR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'});}
function sc(s){return s>=70?'#2ecc71':s<=30?'#e74c3c':'#8fa3be';}
function pc(v){return v>0?'#2ecc71':v<0?'#e74c3c':'#8fa3be';}
function dedup(arr){var s=new Set(),r=[];for(var i=0;i<arr.length;i++){if(!s.has(arr[i].symbol)){s.add(arr[i].symbol);r.push(arr[i]);}}return r;}
function switchTab(tab,el){var tabs=document.querySelectorAll(".toolbar-tab");tabs.forEach(function(t){t.classList.remove("active");});if(el)el.classList.add("active");var panels=["signals","opportunities","open-trades","history"];panels.forEach(function(p){var el2=document.getElementById("tab-"+p);if(el2)el2.style.display=p===tab?"block":"none";});}
function set(id,v,col){var el=document.getElementById(id);if(!el)return;el.textContent=v;if(col)el.style.color=col;}

// ── REPORTS ──────────────────────────────────────────────────────────────
var _currentReport=null;
async function loadReport(period,el){
  document.querySelectorAll('[id^="rpt-btn-"]').forEach(function(b){b.style.background='var(--navy3)';b.style.color='var(--text2)';});
  if(el){el.style.background='var(--blue)';el.style.color='var(--white)';}
  var loading=document.getElementById('rpt-loading');
  if(loading) loading.style.display='inline';
  try{
    var r=await apiFetch(API_BASE+'/reports/'+period);
    var d=await r.json();
    _currentReport=d;
    renderReport(d);
  }catch(e){console.error('loadReport:',e);}
  if(loading) loading.style.display='none';
}
function renderReport(d){
  if(!d) return;
  var pf=d.portfolio||{}, ar=d.arbi||{}, cmb=d.combined||{};
  var sc=d.stocks||{}, cr=d.crypto||{}, ab=d.arbi_detail||{};
  // Summary cards
  var eid=function(id){return document.getElementById(id);};
  function setRpt(id,val,color){var e=eid(id);if(e){e.textContent=val;if(color)e.style.color=color;}}
  function pc(v){return v>=0?'#2ecc71':'#e74c3c';}
  setRpt('rpt-sc-pnl',(pf.closed_pnl>=0?'+':'')+fmtBig(pf.closed_pnl||0),pc(pf.closed_pnl||0));
  setRpt('rpt-sc-ret',(pf.return_pct>=0?'+':'')+((pf.return_pct||0).toFixed(3))+'% retorno',pc(pf.return_pct||0));
  setRpt('rpt-arbi-pnl',(ar.closed_pnl>=0?'+':'')+fmtBig(ar.closed_pnl||0),pc(ar.closed_pnl||0));
  setRpt('rpt-arbi-ret',(ar.return_pct>=0?'+':'')+((ar.return_pct||0).toFixed(3))+'% retorno',pc(ar.return_pct||0));
  var totalPnl=(pf.closed_pnl||0);  // [v10.14] já inclui arbi no backend
  setRpt('rpt-total-pnl',(totalPnl>=0?'+':'')+fmtBig(totalPnl),pc(totalPnl));
  setRpt('rpt-total-trades',(cmb.count||0)+' trades · WR '+(cmb.win_rate||0).toFixed(1)+'%',null);
  // Category detail helper
  function renderCatDetail(elId, cat){
    var e=eid(elId); if(!e) return;
    e.innerHTML=[
      ['P&L', (cat.total_pnl>=0?'+':'')+fmtBig(cat.total_pnl||0), pc(cat.total_pnl||0)],
      ['Win Rate', (cat.win_rate||0).toFixed(1)+'%', (cat.win_rate||0)>=50?'#2ecc71':'#e74c3c'],
      ['Trades', (cat.count||0)+' ('+( cat.wins||0)+'W / '+(cat.losses||0)+'L)', 'var(--text2)'],
      ['Melhor', '+'+ fmtBig(cat.best_trade||0), '#2ecc71'],
      ['Pior', fmtBig(cat.worst_trade||0), '#e74c3c'],
    ].map(function(row){
      return '<div style="display:flex;justify-content:space-between;align-items:center">'
        +'<span style="font-family:monospace;font-size:9px;color:var(--text3)">'+row[0]+'</span>'
        +'<span style="font-family:monospace;font-size:11px;font-weight:600;color:'+row[2]+'">'+row[1]+'</span>'
        +'</div>';
    }).join('');
  }
  renderCatDetail('rpt-stocks-detail', sc);
  renderCatDetail('rpt-crypto-detail', cr);
  renderCatDetail('rpt-arbi-detail', ab);
  // Top/Bot symbols
  function renderSymList(elId, syms){
    var e=eid(elId); if(!e) return;
    if(!syms||!syms.length){e.innerHTML='<span style="font-family:monospace;font-size:10px;color:var(--text3)">Nenhum</span>';return;}
    e.innerHTML=syms.map(function(s){
      return '<div style="display:flex;justify-content:space-between">'
        +'<span style="font-family:monospace;font-size:11px;font-weight:600;color:var(--white)">'+s.symbol+'</span>'
        +'<span style="font-family:monospace;font-size:11px;color:'+(s.pnl>=0?'#2ecc71':'#e74c3c')+'">'+(s.pnl>=0?'+':'')+fmtBig(s.pnl)+'</span>'
        +'</div>';
    }).join('');
  }
  var topAll=(sc.top5_symbols||[]).concat(cr.top5_symbols||[]).concat(ab.top5_symbols||[]).sort(function(a,b){return b.pnl-a.pnl;}).slice(0,5);
  var botAll=(sc.bot5_symbols||[]).concat(cr.bot5_symbols||[]).concat(ab.bot5_symbols||[]).sort(function(a,b){return a.pnl-b.pnl;}).slice(0,5);
  renderSymList('rpt-top-syms', topAll);
  renderSymList('rpt-bot-syms', botAll);
}
async function sendReport(period){
  if(!confirm('Enviar relatório '+period.toUpperCase()+' via WhatsApp?')) return;
  try{
    var r=await apiFetch(API_BASE+'/reports/send/'+period,{method:'POST'});
    var d=await r.json();
    if(d.ok) alert('✅ Relatório '+period+' enviado por WhatsApp!');
    else alert('Erro: '+JSON.stringify(d));
  }catch(e){alert('Erro ao enviar: '+e);}
}

async function loadAll(){
  try{
    set('api-status','Fetching...');
    var res=await Promise.all([
      apiFetch(API_BASE+'/signals'),
      apiFetch(API_BASE+'/stats'),
      apiFetch(API_BASE+'/trades/open'),
      apiFetch(API_BASE+'/trades/closed'),
      apiFetch(API_BASE+'/arbitrage/trades')
    ]);
    var sd=await res[0].json();
    statsData=await res[1].json();
    var od=await res[2].json();
    var cd=await res[3].json();
    var ad=await res[4].json();
    allSignals=sd.signals||[];
    // Enrich signals with real-time change% from ticker-tape API
    try {
      var ttRes = await apiFetch(API_BASE+'/api/ticker-tape');
      var ttData = await ttRes.json();
      var ttMap = {};
      (ttData.items||[]).forEach(function(i){ ttMap[i.t] = i; });
      allSignals.forEach(function(s){
        var sym = (s.symbol||'').replace('.SA','');
        var tt = ttMap[sym];
        if(tt) {
          if(!s.change_24h || s.change_24h === 0) s.change_24h = tt.c;
          s.change_pct = tt.c;
          if(!s.price || s.price === 0) s.price = tt.p;
        }
      });
    } catch(e) { console.warn('ticker-tape enrich:', e); }
    // Normalizar arbi trades para o formato da tabela histórica
    var arbiClosed=(ad.closed_trades||[]).map(function(t){
      return {
        id: t.id, symbol: t.name||t.pair_id||'ARBI',
        market: 'ARBI', asset_type: 'arbi',
        direction: t.buy_leg||'LONG',
        entry_price: t.entry_spread||0, exit_price: t.current_spread||0,
        pnl: t.pnl||0, pnl_pct: t.pnl_pct||0,
        position_value: t.position_size||0,
        close_reason: t.close_reason||'', closed_at: t.closed_at||'',
        opened_at: t.opened_at||'',
        _arbi_leg_a: t.leg_a||'', _arbi_leg_b: t.leg_b||'',
        _arbi_mkt_a: t.mkt_a||'', _arbi_mkt_b: t.mkt_b||'',
      };
    });
    set('api-status','Live');
    var fd=document.getElementById('foot-dot');
    if(fd) fd.style.background='#2ecc71';
    set('foot-api','Railway API · Online');
    set('daemon-val','Online');
    var now=new Date();
    set('last-update','Updated: '+now.toLocaleTimeString('pt-BR'));
    set('last-run',now.toLocaleTimeString('pt-BR'));
    // Calcular win rates por categoria a partir das trades fechadas
    var closedTrades=cd.trades||[];
    var stkT=closedTrades.filter(function(t){return (t.asset_type||'').toLowerCase()!=='crypto'&&(t.market||'').toUpperCase()!=='CRYPTO'&&(t.asset_type||'').toLowerCase()!=='arbi'&&(t.market||'').toUpperCase()!=='ARBI';});
    var cryT=closedTrades.filter(function(t){return (t.asset_type||'').toLowerCase()==='crypto'||(t.market||'').toUpperCase()==='CRYPTO';});
    statsData.stocks_win_rate=stkT.length?(stkT.filter(function(t){return(t.pnl||0)>0;}).length/stkT.length*100):0;
    statsData.crypto_win_rate=cryT.length?(cryT.filter(function(t){return(t.pnl||0)>0;}).length/cryT.length*100):0;
    statsData.stocks_total_pnl=statsData.stocks_closed_pnl||0;
    statsData.crypto_total_pnl=statsData.crypto_closed_pnl||0;
    updateOverview(statsData);
    updateMarketStats();
    updatePortfolioPage(statsData);
    updateAnalytics();
    renderTable(allSignals);
    renderOpps(allSignals);
    renderOpenTrades(od.trades||[]);
    renderHistory((cd.trades||[]).concat(arbiClosed),statsData);
  }catch(e){
    set('api-status','Error');
    var fd=document.getElementById('foot-dot');if(fd)fd.style.background='#e74c3c';
    set('daemon-val','Offline');
    console.error('loadAll error:',e);
  }
}

async function loadLivePrices(){
  try{
    var r=await apiFetch(API_BASE+'/prices/live');
    var d=await r.json();
    // Atualiza trades abertos
    (d.trades||[]).forEach(function(t){
      var pe=document.getElementById('lp-'+t.id);
      var pnle=document.getElementById('lpnl-'+t.id);
      var ppct=document.getElementById('lppct-'+t.id);
      if(pe) pe.textContent='$'+parseFloat(t.current_price||0).toFixed(2);
      if(pnle){pnle.textContent=fmtUSD(t.pnl||0);pnle.style.color=pc(t.pnl||0);}
      if(ppct){var pp=parseFloat(t.pnl_pct||0);ppct.textContent=(pp>=0?'+':'')+pp.toFixed(2)+'%';ppct.style.color=pc(pp);}
    });
    // Atualiza preços no Signal Monitor e Markets
    var prices=d.prices||{};
    Object.keys(prices).forEach(function(sym){
      var pd=prices[sym];
      var prEl=document.getElementById('price-'+sym);
      var chEl=document.getElementById('chg-'+sym);
      if(prEl) prEl.textContent='$'+parseFloat(pd.price||0).toFixed(pd.price>100?2:4);
      if(chEl){var ch=parseFloat(pd.change_24h||0);chEl.textContent=(ch>0?'+':'')+ch.toFixed(2)+'%';chEl.style.color=ch>0?'#2ecc71':ch<0?'#e74c3c':'#8fa3be';}
    });
  }catch(e){}
}

function updateOverview(s){
  // [v10.14] total_portfolio_value e total_pnl já incluem arbi
  var cap=s.total_portfolio_value||s.current_capital||0,pnl=s.total_pnl||0,gain=s.gain_percent||0,wr=s.win_rate||0;
  var openT=s.open_trades||0,closedT=s.closed_trades||0,openPnl=s.open_pnl||0;
  var u=dedup(allSignals),n=u.length;
  var buy=u.filter(function(x){return x.signal==='COMPRA';}).length;
  var sell=u.filter(function(x){return x.signal==='VENDA';}).length;
  var hold=u.filter(function(x){return x.signal==='MANTER';}).length;
  var b3c=u.filter(function(x){return x.market_type==='B3';}).length;
  var nyc=u.filter(function(x){return x.market_type==='NYSE';}).length;
  var ar=n?(u.reduce(function(a,b){return a+(parseFloat(b.rsi)||0);},0)/n).toFixed(1):'--';
  set('lp-capital','$'+fmtNum(cap));
  set('lp-gain',(gain>=0?'+ ':'')+gain.toFixed(2)+'% all-time',pc(gain));
  set('lp-pnl',fmtBig(pnl),pc(pnl));
  set('lp-winrate',wr.toFixed(1)+'%');
  set('lp-open',openT);set('lp-closed',closedT);
  set('left-rsi',ar);
  set('st-buy',buy);set('st-sell',sell);set('st-rsi',ar);
  set('st-buy-sub',n?Math.round(buy/n*100)+'% of signals':'');
  set('st-sell-sub',n?Math.round(sell/n*100)+'% of signals':'');
  set('st-open-trades',openT);
  set('st-open-pnl','Open P&L: '+(openPnl>=0?'+':'')+openPnl.toFixed(0));
  set('st-pnl',fmtBig(pnl),pc(pnl));
  set('st-winrate','Win rate: '+wr.toFixed(1)+'%');
  set('badge-open',openT);set('badge-history',closedT);
  set('cnt-buy',buy);set('cnt-hold',hold);set('cnt-sell',sell);
  set('cnt-b3',b3c);set('cnt-nyse',nyc);
  if(n){
    ['buy','hold','sell'].forEach(function(k,i){
      var el=document.getElementById('bar-'+k);
      if(el)el.style.width=([buy,hold,sell][i]/n*100)+'%';
    });
    var bb=document.getElementById('bar-b3'),bn=document.getElementById('bar-nyse');
    if(bb)bb.style.width=(b3c/n*100)+'%';
    if(bn)bn.style.width=(nyc/n*100)+'%';
  }
}

function updateMarketStats(){
  var u=dedup(allSignals);
  var b3=u.filter(function(x){return x.market_type==='B3';});
  var ny=u.filter(function(x){return x.market_type==='NYSE';});
  function avg(a,k){return a.length?(a.reduce(function(s,b){return s+(parseFloat(b[k])||0);},0)/a.length).toFixed(1):'--';}
  function cnt(a,sig){return a.filter(function(x){return x.signal===sig;}).length;}
  set('mkt-b3-buy',cnt(b3,'COMPRA'));set('mkt-b3-sell',cnt(b3,'VENDA'));
  set('mkt-b3-rsi',avg(b3,'rsi'));set('mkt-b3-score',avg(b3,'score'));
  set('mkt-ny-buy',cnt(ny,'COMPRA'));set('mkt-ny-sell',cnt(ny,'VENDA'));
  set('mkt-ny-rsi',avg(ny,'rsi'));set('mkt-ny-score',avg(ny,'score'));
  set('mkt-all-total',u.length);
  set('mkt-all-ratio',cnt(u,'COMPRA')+'/'+cnt(u,'VENDA'));
  set('mkt-all-rsi',avg(u,'rsi'));set('mkt-all-score',avg(u,'score'));
  // Crypto card stats
  var cry=u.filter(function(x){return x.market_type==='CRYPTO';});
  set('mkt-cry-buy',cnt(cry,'COMPRA'));set('mkt-cry-sell',cnt(cry,'VENDA'));
  set('mkt-cry-rsi',avg(cry,'rsi'));set('mkt-cry-score',avg(cry,'score'));
  // Render tables
  renderMktTable('mkt-crypto-body', cry, 'CRYPTO');
  renderMktTable('mkt-b3-body', u.filter(function(x){return x.market_type==='B3';}), 'B3');
  renderMktTable('mkt-nyse-body', u.filter(function(x){return x.market_type==='NYSE'||x.market_type==='NASDAQ';}), 'NYSE');
}

function renderMktTable(tbId, list, mkt){
  var tb=document.getElementById(tbId);if(!tb)return;
  if(!list.length){
    if(mkt!=='CRYPTO'){tb.innerHTML='<tr><td colspan="8" style="padding:20px;text-align:center;color:var(--text3)">Mercado fechado — dados no próximo pregão</td></tr>';}
    return;
  }
  tb.innerHTML=list.map(function(s){
    var p=parseFloat(s.price||0);
    var pr=mkt==='B3'?'R$'+p.toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2}):'$'+p.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:4});
    var ch=parseFloat(s.change_24h||0);
    var chColor=ch>0?'#2ecc71':ch<0?'#e74c3c':'#8fa3be';
    var chStr=(ch>0?'+':'')+ch.toFixed(2)+'%';
    var trend=ch>1?'▲':ch<-1?'▼':'▸';
    var sig=s.signal==='COMPRA'?'<span style="color:#2ecc71;font-weight:600">COMPRA</span>':s.signal==='VENDA'?'<span style="color:#e74c3c;font-weight:600">VENDA</span>':'<span style="color:#8fa3be">MANTER</span>';
    var sc_=s.score||0;
    return '<tr style="border-bottom:1px solid var(--line)">'
      +'<td style="padding:8px 14px;font-weight:600">'+s.symbol+'<span style="color:var(--text3);font-size:11px;margin-left:6px">'+( s.name||s.market_type||'' )+'</span></td>'
      +'<td style="padding:8px 14px;text-align:right;font-family:monospace">'+pr+'</td>'
      +'<td style="padding:8px 14px;text-align:right;color:'+chColor+';font-family:monospace">'+trend+' '+chStr+'</td>'
      +'<td style="padding:8px 14px;text-align:right;color:'+(parseFloat(s.rsi||50)<30?'#2ecc71':parseFloat(s.rsi||50)>70?'#e74c3c':'#8fa3be')+';font-family:monospace">'+parseFloat(s.rsi||0).toFixed(1)+'</td>'
      +'<td style="padding:8px 14px;text-align:right;color:'+sc(sc_)+';font-family:monospace;font-weight:600">'+sc_+'</td>'
      +'<td style="padding:8px 14px;text-align:center">'+sig+'</td>'
      +'<td style="padding:8px 14px;text-align:right;color:var(--text2);font-family:monospace">$'+parseFloat(s.ema9||0).toFixed(2)+'</td>'
      +'<td style="padding:8px 14px;text-align:right;color:var(--text3);font-family:monospace">$'+parseFloat(s.ema21||0).toFixed(2)+'</td>'
      +'</tr>';
  }).join('');
}

function updatePortfolioPage(s){
  var arbi=s.arbi_book||{};
  var arbiInitial=arbi.initial_capital||3000000;
  // Capital inicial TOTAL (stocks + crypto + arbi)
  var initialCap=s.initial_capital||8000000; // backend já inclui arbi (3.5M+1.5M+3M=8M)
  // [v10.14] total_portfolio_value JÁ inclui arbi no backend — não somar novamente
  // [v10.14] total_portfolio_value já inclui arbi — fallback também não duplica
  // [v10.14] total_portfolio_value = stocks+crypto+arbi (backend já inclui tudo)
  // NUNCA somar arbi.portfolio_value aqui — seria dupla contagem → $12M
  var totalVal=s.total_portfolio_value||(s.core_portfolio_value||0);
  // Sanity check: se alguém mexeu e voltou o código antigo, isso previne $12M
  // var totalVal deve ser ~8.5M, NÃO ~12M
  var gainPct=initialCap>0?((totalVal-initialCap)/initialCap*100):0;
  var gainSign=gainPct>=0?'+':'';
  // [v10.14] s.closed_pnl e s.open_pnl já incluem arbi no backend
  var arbiTotalPnl=(arbi.closed_pnl||0)+(arbi.open_pnl||0);
  var totalClosedPnl=s.closed_pnl||0;   // já inclui arbi
  var totalOpenPnl=s.open_pnl||0;       // já inclui arbi
  // [v10.14] s.daily/weekly/monthly/annual já incluem arbi no backend
  var dailyPnl=s.daily_pnl_total||s.daily_pnl||0;  // [v10.14] inclui posições abertas
  var weeklyPnl=s.weekly_pnl||0;
  var monthlyPnl=s.monthly_pnl||0;
  var annualPnl=s.annual_pnl||0;

  set('port-initial','$'+fmtNum(initialCap));
  set('port-total-val','$'+fmtNum(totalVal));

  var gainEl=document.getElementById('port-gain');
  if(gainEl){
    gainEl.textContent=gainSign+gainPct.toFixed(2)+'% vs capital inicial';
    gainEl.style.color=gainPct>=0?'#2ecc71':'#e74c3c';
  }

  var freeCap=(s.current_capital||0)+(arbi.capital||0);
  var invested=totalVal-freeCap;
  set('port-capital','$'+fmtNum(freeCap));
  set('port-invested','$'+fmtNum(Math.max(0,invested)));
  set('port-open-pnl',fmtBig(totalOpenPnl),pc(totalOpenPnl));
  set('port-closed-pnl',fmtBig(totalClosedPnl),pc(totalClosedPnl));

  // Períodos (stocks + crypto + arbi)
  set('port-daily-pnl',fmtBig(dailyPnl),pc(dailyPnl));
  set('port-weekly-pnl',fmtBig(weeklyPnl),pc(weeklyPnl));
  set('port-monthly-pnl',fmtBig(monthlyPnl),pc(monthlyPnl));
  set('port-annual-pnl',fmtBig(annualPnl),pc(annualPnl));

  // Trade stats
  var wrVal=(s.win_rate||0).toFixed(1)+'%';
  var wrEl=document.getElementById('port-winrate');
  if(wrEl){wrEl.textContent=wrVal;wrEl.style.color=(s.win_rate||0)>=50?'#2ecc71':'#e74c3c';}
  set('port-open',s.open_trades||0);
  set('port-closed',s.closed_trades||0);
  set('port-winning',s.winning_trades||0);

  // Categorias — Stocks
  var stocksCap=s.stocks_portfolio_value||(s.stocks_capital||0);  // [v10.14] portfolio total, não cash livre
  var stocksPnl=s.stocks_closed_pnl||0;
  var stocksWr=s.stocks_win_rate||0;
  var sRetPct=s.stocks_return_pct||0;
  var sRod=s.stocks_return_on_deployed||0;
  set('port-stocks-cap','$'+fmtNum(stocksCap));
  set('port-stocks-pnl',fmtBig(stocksPnl),pc(stocksPnl));
  set('port-stocks-daily',fmtBig(s.stocks_daily_pnl||0),pc(s.stocks_daily_pnl||0));
  set('port-stocks-monthly',fmtBig(s.stocks_monthly_pnl||0),pc(s.stocks_monthly_pnl||0));
  var swEl=document.getElementById('port-stocks-wr');
  if(swEl){swEl.textContent=stocksWr.toFixed(1)+'% WR';swEl.style.color=stocksWr>=50?'var(--gold2)':'#e74c3c';}
  var sRetEl=document.getElementById('port-stocks-ret');
  if(sRetEl){sRetEl.textContent=(sRetPct>=0?'+':'')+sRetPct.toFixed(2)+'% cap.inicial';sRetEl.style.color=sRetPct>=0?'#2ecc71':'#e74c3c';}
  var sRodEl=document.getElementById('port-stocks-rod');
  if(sRodEl){sRodEl.textContent=(sRod>=0?'+':'')+sRod.toFixed(2)+'% empregado';sRodEl.style.color=sRod>=0?'#2ecc71':'#e74c3c';}

  // Categorias — Crypto
  var cryptoCap=s.crypto_portfolio_value||(s.crypto_capital||0);  // [v10.14] portfolio total, não cash livre
  var cryptoPnl=s.crypto_closed_pnl||0;
  var cryptoWr=s.crypto_win_rate||0;
  var cRetPct=s.crypto_return_pct||0;
  var cRod=s.crypto_return_on_deployed||0;
  set('port-crypto-cap','$'+fmtNum(cryptoCap));
  set('port-crypto-pnl',fmtBig(cryptoPnl),pc(cryptoPnl));
  set('port-crypto-daily',fmtBig(s.crypto_daily_pnl||0),pc(s.crypto_daily_pnl||0));
  set('port-crypto-monthly',fmtBig(s.crypto_monthly_pnl||0),pc(s.crypto_monthly_pnl||0));
  var cwEl=document.getElementById('port-crypto-wr');
  if(cwEl){cwEl.textContent=cryptoWr.toFixed(1)+'% WR';cwEl.style.color=cryptoWr>=50?'#c77dff':'#e74c3c';}
  var cRetEl=document.getElementById('port-crypto-ret');
  if(cRetEl){cRetEl.textContent=(cRetPct>=0?'+':'')+cRetPct.toFixed(2)+'% cap.inicial';cRetEl.style.color=cRetPct>=0?'#2ecc71':'#e74c3c';}
  var cRodEl=document.getElementById('port-crypto-rod');
  if(cRodEl){cRodEl.textContent=(cRod>=0?'+':'')+cRod.toFixed(2)+'% empregado';cRodEl.style.color=cRod>=0?'#2ecc71':'#e74c3c';}

  // Categorias — Arbi
  var arbiPnl=(arbi.closed_pnl||0)+(arbi.open_pnl||0);
  var arbiDailyPnl=arbi.daily_pnl||0;
  var arbiMonthlyPnl=arbi.monthly_pnl||0;
  var arbiWr=arbi.win_rate||0;
  var arbiRetPct=arbi.gain_percent||0;
  var arbiRod=arbi.return_on_deployed||0;
  set('port-arbi-cap','$'+fmtNum(arbi.portfolio_value||arbiInitial||3000000));
  set('port-arbi-pnl',fmtBig(arbiPnl),pc(arbiPnl));
  set('port-arbi-daily',fmtBig(arbiDailyPnl),pc(arbiDailyPnl));
  set('port-arbi-monthly',fmtBig(arbiMonthlyPnl),pc(arbiMonthlyPnl));
  var awEl=document.getElementById('port-arbi-wr');
  if(awEl){awEl.textContent=arbiWr.toFixed(1)+'% WR';awEl.style.color=arbiWr>=50?'#00cec9':'#e74c3c';}
  var aRetEl=document.getElementById('port-arbi-ret');
  if(aRetEl){aRetEl.textContent=(arbiRetPct>=0?'+':'')+arbiRetPct.toFixed(2)+'% cap.inicial';aRetEl.style.color=arbiRetPct>=0?'#2ecc71':'#e74c3c';}
  var aRodEl=document.getElementById('port-arbi-rod');
  if(aRodEl){aRodEl.textContent=(arbiRod>=0?'+':'')+arbiRod.toFixed(2)+'% empregado';aRodEl.style.color=arbiRod>=0?'#2ecc71':'#e74c3c';}

  // Gráficos do Portfolio
  if(window.Chart){
    var gridCol='rgba(255,255,255,0.05)'; var tickCol='#8fa3be';
    // Alocação de Capital (donut)
    var gc1=document.getElementById('port-chart-alloc');
    if(gc1){ if(gc1._ch)gc1._ch.destroy();
      var sCap=s.stocks_portfolio_value||0;
      var cCap=s.crypto_portfolio_value||0;
      var aCap=arbi.portfolio_value||0;  // correto — valor real do portfólio arbi
      gc1._ch=new Chart(gc1,{type:'doughnut',data:{
        labels:['Stocks $'+fmtNum(sCap),'Crypto $'+fmtNum(cCap),'Arbi $'+fmtNum(aCap)],
        datasets:[{data:[sCap,cCap,aCap],backgroundColor:['rgba(74,158,255,0.8)','rgba(199,125,255,0.8)','rgba(0,206,201,0.8)'],borderWidth:0,hoverOffset:8}]},
        options:{responsive:true,maintainAspectRatio:false,cutout:'55%',
          plugins:{legend:{position:'right',labels:{color:tickCol,font:{size:10},boxWidth:12,padding:10}}}}});
    }
    // P&L por categoria (barras)
    var gc2=document.getElementById('port-chart-pnl');
    if(gc2){ if(gc2._ch)gc2._ch.destroy();
      var sPnlV=s.stocks_closed_pnl||0;
      var cPnlV=s.crypto_closed_pnl||0;
      var aPnlV=(arbi.closed_pnl||0)+(arbi.open_pnl||0);
      gc2._ch=new Chart(gc2,{type:'bar',data:{
        labels:['Stocks','Crypto','Arbitragem'],
        datasets:[{data:[sPnlV,cPnlV,aPnlV],
          backgroundColor:[sPnlV>=0?'rgba(74,158,255,0.8)':'rgba(231,76,60,0.7)',
            cPnlV>=0?'rgba(199,125,255,0.8)':'rgba(231,76,60,0.7)',
            aPnlV>=0?'rgba(0,206,201,0.8)':'rgba(231,76,60,0.7)'],borderRadius:6}]},
        options:{responsive:true,maintainAspectRatio:false,
          plugins:{legend:{display:false}},
          scales:{x:{ticks:{color:tickCol,font:{size:11}},grid:{display:false}},
            y:{ticks:{color:tickCol,font:{size:9},callback:function(v){return(v>=0?'+$':'-$')+Math.abs(v/1000).toFixed(0)+'K';}},grid:{color:gridCol}}}}});
    }
  }
}

async function updateAnalytics(){
  // Busca dados de learning
  var ls={}, lp={}, lf={}, sh={};
  try{ var r=await apiFetch(API_BASE+'/learning/status'); ls=await r.json(); }catch(e){}
  try{ var r=await apiFetch(API_BASE+'/learning/patterns'); lp=await r.json(); }catch(e){}
  try{ var r=await apiFetch(API_BASE+'/learning/factors'); lf=await r.json(); }catch(e){}
  try{ var r=await apiFetch(API_BASE+'/shadow/status'); sh=await r.json(); }catch(e){}

  // KPIs
  set('ana-patterns', (ls.patterns_above_min_samples||0).toLocaleString());
  set('ana-patterns-sub', (ls.total_patterns||0).toLocaleString()+' total · min '+( ls.min_samples_threshold||10)+' samples');
  set('ana-events', (ls.total_signal_events||0).toLocaleString());
  set('ana-factors', (ls.total_factor_rows||0).toLocaleString());
  set('ana-version', ls.learning_version||'—');
  var lu=ls.last_learning_update||''; set('ana-last-update', lu?lu.substring(0,16).replace('T',' '):'nunca');

  // Shadow win rate com cor
  var swr=sh.shadow_win_rate_pct||0;
  var swrEl=document.getElementById('ana-shadow-wr');
  if(swrEl){ swrEl.textContent=swr.toFixed(1)+'%'; swrEl.style.color=swr>=50?'#2ecc71':'#e74c3c'; }

  // Shadow wins/losses/pending
  var bst=sh.by_status||{};
  set('ana-shadow-wins', (bst.WIN||0).toLocaleString());
  set('ana-shadow-losses', (bst.LOSS||0).toLocaleString());
  set('ana-shadow-pending', (bst.PENDING||0).toLocaleString());

  // Motivos shadow
  var reasons=sh.by_reason||[];
  var rEl=document.getElementById('ana-shadow-reasons');
  if(rEl && reasons.length){
    var top=reasons.slice(0,4); var maxN=top[0].n||1;
    rEl.innerHTML=top.map(function(r){
      var pct=Math.round(r.n/maxN*100);
      var lbl=r.not_executed_reason.replace(/_/g,' ');
      return '<div style="margin-bottom:8px">'
        +'<div style="display:flex;justify-content:space-between;font-size:9px;font-family:monospace;margin-bottom:3px">'
        +'<span style="color:var(--text2)">'+lbl+'</span>'
        +'<span style="color:var(--text3)">'+r.n.toLocaleString()+'</span></div>'
        +'<div style="height:4px;background:var(--navy3)"><div style="height:100%;width:'+pct+'%;background:var(--gold2)"></div></div>'
        +'</div>';
    }).join('');
  }

  // Top patterns table
  var pats=(lp.patterns||[]).slice(0,8);
  var pb=document.getElementById('ana-patterns-body');
  if(pb && pats.length){
    pb.innerHTML=pats.map(function(p){
      var wr=p.total_samples>0?Math.round(p.wins/p.total_samples*100):0;
      var wrColor=wr>=70?'#2ecc71':wr>=50?'#f39c12':'#e74c3c';
      var expColor=p.expectancy>=0?'#2ecc71':'#e74c3c';
      return '<tr style="border-bottom:1px solid var(--line)">'
        +'<td style="padding:5px 8px;font-family:monospace;font-size:9px;color:var(--text3)">'+p.feature_hash.substring(0,8)+'</td>'
        +'<td style="padding:5px 8px;text-align:right;font-size:10px">'+p.total_samples+'</td>'
        +'<td style="padding:5px 8px;text-align:right;font-size:10px;color:'+wrColor+'">'+wr+'%</td>'
        +'<td style="padding:5px 8px;text-align:right;font-family:monospace;font-size:10px;color:'+expColor+'">'+(p.expectancy>=0?'+':'')+p.expectancy.toFixed(3)+'</td>'
        +'<td style="padding:5px 8px;text-align:right;font-family:monospace;font-size:10px;color:var(--gold2)">'+p.confidence_weight.toFixed(3)+'</td>'
        +'</tr>';
    }).join('');
  }

  // Top & bottom factors
  function renderFactors(el, factors, color){
    if(!el||!factors||!factors.length) return;
    el.innerHTML=factors.slice(0,5).map(function(f){
      var exp=f.expectancy||0;
      var bar=Math.min(Math.abs(exp)*200,100);
      return '<div style="margin-bottom:10px">'
        +'<div style="display:flex;justify-content:space-between;font-size:9px;font-family:monospace;margin-bottom:3px">'
        +'<span style="color:var(--text2)">'+f.factor_type+': <span style="color:var(--white)">'+f.factor_value+'</span></span>'
        +'<span style="color:'+color+'">'+(exp>=0?'+':'')+exp.toFixed(3)+'</span></div>'
        +'<div style="height:4px;background:var(--navy3)"><div style="height:100%;width:'+bar+'%;background:'+color+'"></div></div>'
        +'<div style="font-size:8px;color:var(--text3);margin-top:2px">'+f.total_samples+' amostras · wins: '+f.wins+' · EWMA: '+(f.ewma_pnl_pct||0).toFixed(3)+'%</div>'
        +'</div>';
    }).join('');
  }
  renderFactors(document.getElementById('ana-top-factors'), lf.top_factors, '#2ecc71');
  renderFactors(document.getElementById('ana-bottom-factors'), lf.bottom_factors, '#e74c3c');

  // Bar charts de sinais, scores e RSI a partir dos sinais
  var u=dedup(allSignals), n=u.length; if(!n) return;
  function barChart(elId, data, colors){
    var el=document.getElementById(elId); if(!el) return;
    var max=Math.max.apply(null,data.map(function(d){return d.v;}));
    el.innerHTML=data.map(function(d){
      var pct=max>0?Math.round(d.v/max*100):0;
      var vpct=n>0?Math.round(d.v/n*100):0;
      return '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">'
        +'<span style="width:50px;font-size:9px;color:var(--text3);font-family:monospace">'+d.l+'</span>'
        +'<div style="flex:1;height:8px;background:var(--navy3);border-radius:2px">'
        +'<div style="height:100%;width:'+pct+'%;background:'+(d.c||'var(--gold)')+';border-radius:2px"></div></div>'
        +'<span style="width:30px;font-size:9px;text-align:right;color:var(--text2)">'+vpct+'%</span>'
        +'</div>';
    }).join('');
  }
  // Sinais
  var buy=u.filter(function(x){return x.signal==='COMPRA';}).length;
  var sell=u.filter(function(x){return x.signal==='VENDA';}).length;
  var hold=u.filter(function(x){return x.signal==='MANTER';}).length;
  barChart('ana-sig-bars',[{l:'COMPRA',v:buy,c:'#2ecc71'},{l:'VENDA',v:sell,c:'#e74c3c'},{l:'MANTER',v:hold,c:'#8fa3be'}]);
  // Scores
  var sb=[0,0,0,0,0];
  u.forEach(function(x){var s=x.score||0; sb[Math.min(Math.floor(s/20),4)]++;});
  barChart('ana-score-bars',[
    {l:'0-20',v:sb[0],c:'#e74c3c'},{l:'20-40',v:sb[1],c:'#e67e22'},
    {l:'40-60',v:sb[2],c:'#f39c12'},{l:'60-80',v:sb[3],c:'#2ecc71'},{l:'80-100',v:sb[4],c:'#00cec9'}
  ]);
  // RSI
  var rb=[0,0,0,0,0];
  u.forEach(function(x){var r=parseFloat(x.rsi||50); if(r<30)rb[0]++; else if(r<40)rb[1]++; else if(r<60)rb[2]++; else if(r<70)rb[3]++; else rb[4]++;});
  barChart('ana-rsi-bars',[
    {l:'<30 OS',v:rb[0],c:'#2ecc71'},{l:'30-40',v:rb[1],c:'#00cec9'},
    {l:'40-60',v:rb[2],c:'#8fa3be'},{l:'60-70',v:rb[3],c:'#e67e22'},{l:'>70 OB',v:rb[4],c:'#e74c3c'}
  ]);
}

function renderTable(sigs){
  var list=currentFilter==='ALL'?sigs:sigs.filter(function(x){
    if(currentFilter==='COMPRA')return x.signal==='COMPRA';
    if(currentFilter==='VENDA')return x.signal==='VENDA';
    if(currentFilter==='MANTER')return x.signal==='MANTER';
    if(currentFilter==='B3')return x.market_type==='B3';
    if(currentFilter==='NYSE')return x.market_type==='NYSE'||x.market_type==='NASDAQ';
    if(currentFilter==='CRYPTO')return x.market_type==='CRYPTO';
    return true;
  });
  // [v2] Aplicar ordenação se houver (Item 2)
  if(currentSort && currentSort.key) list = _applySortToList(list, currentSort.key, currentSort.dir);
  var tb=document.getElementById('signals-body');if(!tb)return;
  if(!list.length){tb.innerHTML='<tr><td colspan="10" style="padding:30px;text-align:center;color:var(--text3)">Sem sinais</td></tr>';return;}
  tb.innerHTML=list.map(function(s){
    var sp=s.signal==='COMPRA'?'<span style="color:#2ecc71;font-weight:600">COMPRA</span>':s.signal==='VENDA'?'<span style="color:#e74c3c;font-weight:600">VENDA</span>':'<span style="color:#8fa3be">MANTER</span>';
    var p=parseFloat(s.price||0);
    var isB3=s.market_type==='B3';
    var cur=isB3?'R$':'$';
    var pr=cur+p.toLocaleString(isB3?'pt-BR':'en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
    var ch=parseFloat(s.change_pct||s.change_24h||0);
    var chColor=ch>0.01?'#22C55E':ch<-0.01?'#EF4444':'#64748B';
    var chArrow=ch>0.01?'▲':ch<-0.01?'▼':'─';
    var chBg=ch>0.01?'rgba(34,197,94,0.1)':ch<-0.01?'rgba(239,68,68,0.1)':'transparent';
    var chStr=chArrow+' '+(ch>0?'+':'')+ch.toFixed(2)+'%';
    var sc_=s.score||0;
    var rsiV=parseFloat(s.rsi||0);
    var rsiC=rsiV<30?'#22C55E':rsiV>70?'#EF4444':'#94A3B8';
    var ema=function(v){return cur+parseFloat(v||0).toLocaleString(isB3?'pt-BR':'en-US',{minimumFractionDigits:2,maximumFractionDigits:2});};
    var td='<td style="padding:8px;text-align:right;font-family:monospace;font-size:12px;';
    return '<tr style="border-bottom:1px solid var(--line)">'
      +'<td style="padding:8px 14px;font-weight:600;font-size:13px">'+s.symbol+'<span style="color:var(--text3);font-size:10px;margin-left:5px">'+( isB3?'B3':'US' )+'</span></td>'
      +td+'color:var(--text1)">'+pr+'</td>'
      +td+'color:'+chColor+';background:'+chBg+';border-radius:3px;font-weight:600;font-size:11px;padding:6px 8px">'+chStr+'</td>'
      +'<td style="padding:8px;text-align:center">'+sp+'</td>'
      +td+'color:'+sc(sc_)+';font-weight:700">'+sc_+'</td>'
      +td+'color:'+rsiC+';font-weight:600">'+rsiV.toFixed(1)+'</td>'
      +td+'color:var(--text2)">'+ema(s.ema9)+'</td>'
      +td+'color:var(--text2)">'+ema(s.ema21)+'</td>'
      +td+'color:var(--text2)">'+ema(s.ema50)+'</td>'
      +td+'color:var(--text3);font-size:10px" class="hide-mobile">'+fmtT(s.created_at)+'</td>'
      +'</tr>';
  }).join('');
}

function renderOpps(sigs){
  // Ordenar por score × learning_confidence — as melhores oportunidades reais
  function oppScore(x){ return (x.score||0) * (0.5 + (x.learning_confidence||0)*0.5); }
  var all=dedup(sigs);
  var buys=all.filter(function(x){return x.signal==='COMPRA'&&(x.score||0)>=70;})
    .sort(function(a,b){return oppScore(b)-oppScore(a);}).slice(0,6);
  var sells=all.filter(function(x){return x.signal==='VENDA'&&(x.score||0)>=70;})
    .sort(function(a,b){return oppScore(b)-oppScore(a);}).slice(0,6);
  var buyEl=document.getElementById('opp-buy');
  var sellEl=document.getElementById('opp-sell');
  function card(s,isBuy){
    var p=parseFloat(s.price||0);
    var isB3=s.market_type==='B3';
    var pr=isB3?'R$'+p.toFixed(2):'$'+p.toFixed(2);
    var dir=isBuy?'<span style="background:#1a3a2a;color:#2ecc71;padding:2px 6px;border-radius:3px;font-size:10px;font-family:monospace;font-weight:700">LONG</span>'
                 :'<span style="background:#3a1a1a;color:#e74c3c;padding:2px 6px;border-radius:3px;font-size:10px;font-family:monospace;font-weight:700">SHORT</span>';
    // TP +8% / SL -3%
    var tp=isBuy?p*1.08:p*0.92;
    var sl=isBuy?p*0.97:p*1.03;
    var tpStr=isB3?'R$'+tp.toFixed(2):'$'+tp.toFixed(2);
    var slStr=isB3?'R$'+sl.toFixed(2):'$'+sl.toFixed(2);
    return '<div style="padding:12px 14px;border-bottom:1px solid var(--line)">'
      +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">'
      +'<span style="font-weight:600;font-size:14px">'+s.symbol+'</span>'
      +dir
      +'<span style="color:var(--text2);font-size:11px">'+s.market_type+'</span>'
      +'<span style="font-family:monospace;font-weight:600">'+pr+'</span>'
      +'<span style="color:'+sc(s.score||0)+';font-weight:700;font-family:monospace">'+(s.score||0)+'</span>'
      +'</div>'
      +'<div style="display:flex;gap:12px;font-size:11px;font-family:monospace;flex-wrap:wrap">'
      +'<span style="color:var(--text3)">RSI <span style="color:'+(parseFloat(s.rsi||50)<30?'#2ecc71':parseFloat(s.rsi||50)>70?'#e74c3c':'#8fa3be')+'">'+parseFloat(s.rsi||0).toFixed(1)+'</span></span>'
      +'<span style="color:var(--text3)">EMA9 <span style="color:var(--text2)">'+( isB3?'R$':'$')+parseFloat(s.ema9||0).toFixed(2)+'</span></span>'
      +((s.learning_confidence||0)>0?'<span style="color:#f39c12">Conf <span style="color:var(--white)">'+((s.learning_confidence||0)*100).toFixed(0)+'%</span></span>':'')
      +'<span style="color:#2ecc71">TP '+tpStr+'</span>'
      +'<span style="color:#e74c3c">SL '+slStr+'</span>'
      +'</div>'
      +'</div>';
  }
  if(buyEl){buyEl.innerHTML=buys.length?buys.map(function(s){return card(s,true);}).join(''):'<div style="padding:20px;text-align:center;color:var(--text3)">Nenhuma oportunidade acima de 70</div>';}
  if(sellEl){sellEl.innerHTML=sells.length?sells.map(function(s){return card(s,false);}).join(''):'<div style="padding:20px;text-align:center;color:var(--text3)">Nenhum alerta de venda</div>';}
}

function renderOpenTrades(trades){
  var tb=document.getElementById('open-trades-body');if(!tb)return;
  if(!trades.length){tb.innerHTML='<tr><td colspan="9" style="padding:30px;text-align:center;color:var(--text3)">Nenhuma posição aberta</td></tr>';return;}
  tb.innerHTML=trades.map(function(t){
    var dir=t.direction==='LONG'?'<span style="color:#2ecc71">LONG</span>':'<span style="color:#e74c3c">SHORT</span>';
    var pnl=t.pnl||0,pct=t.pnl_pct||0;
    var qty=parseFloat(t.quantity||0);
    var qtyStr=qty>1000?qty.toFixed(0):qty>1?qty.toFixed(3):qty.toFixed(6);
    var posVal=parseFloat(t.position_value||0);
    var isCrypto=t.market==='CRYPTO'||t.asset_type==='crypto';
    var ep=parseFloat(t.entry_price||0);
    var epStr=isCrypto&&ep<1?'$'+ep.toFixed(4):'$'+ep.toFixed(2);
    return '<tr style="border-bottom:1px solid var(--line)">'
      +'<td style="padding:10px 14px;font-weight:600">'+t.symbol+'<br><span style="color:var(--text3);font-size:10px">'+( t.market||'')+'</span></td>'
      +'<td style="padding:10px 14px">'+dir+'</td>'
      +'<td style="padding:10px 14px;text-align:right;font-family:monospace">'+epStr+'</td>'
      +'<td style="padding:10px 14px;text-align:right;font-family:monospace" id="lp-'+t.id+'">$'+parseFloat(t.current_price||t.entry_price||0).toFixed(isCrypto&&ep<1?4:2)+'</td>'
      +'<td style="padding:10px 14px;text-align:right;color:var(--text2);font-family:monospace">'+qtyStr+'</td>'
      +'<td style="padding:10px 14px;text-align:right;font-family:monospace">$'+fmtNum(posVal)+'</td>'
      +'<td style="padding:10px 14px;text-align:right;color:'+pc(pnl)+';font-family:monospace;font-weight:600" id="lpnl-'+t.id+'">'+(pnl>=0?'+':'')+pnl.toFixed(2)+'</td>'
      +'<td style="padding:10px 14px;text-align:right;color:'+pc(pct)+';font-family:monospace" id="lppct-'+t.id+'">'+(pct>=0?'+':'')+pct.toFixed(2)+'%</td>'
      +'<td style="padding:10px 14px;text-align:right;color:var(--text3);font-size:11px">'+fmtT(t.opened_at)+'</td>'
      +'</tr>';
  }).join('');
}

var _allHistTrades=[];var _histTab='all';
function switchHistTab(tab,el){
  _histTab=tab;
  document.querySelectorAll('[id^="htab-"]').forEach(function(b){b.style.background='transparent';b.style.color='var(--text2)';});
  if(el){el.style.background='var(--blue)';el.style.color='var(--white)';}
  _renderHistoryTable(_allHistTrades);
}
function _catOfTrade(t){
  var at=(t.asset_type||'').toLowerCase();
  var mkt=(t.market||'').toUpperCase();
  if(at==='crypto'||mkt==='CRYPTO') return 'crypto';
  if(at==='arbi'||mkt==='ARBI') return 'arbi';
  return 'stocks';
}
function _renderHistoryTable(trades){
  var tb=document.getElementById('history-body');if(!tb)return;
  var rm={'TAKE_PROFIT':'TP','TRAILING_STOP':'Trail','STOP_LOSS':'SL','TIMEOUT':'Timeout','MARKET_CLOSE':'Mkt Close'};
  var list=_histTab==='all'?trades:trades.filter(function(t){return _catOfTrade(t)===_histTab;});
  if(!list.length){tb.innerHTML='<tr><td colspan="9" style="padding:30px;text-align:center;color:var(--text3)">Sem trades nesta categoria</td></tr>';return;}
  tb.innerHTML=list.map(function(t){
    var pnl=t.pnl||0,pct=t.pnl_pct||0;
    var isArbi=(t.asset_type||'').toLowerCase()==='arbi';
    var dir=t.direction||'--';
    var dirColor=dir==='LONG'?'#2ecc71':'#e74c3c';
    var symCell, mktCell, dirCell, entryCell, exitCell;
    if(isArbi){
      var la=t._arbi_leg_a||'', lb=t._arbi_leg_b||'';
      var ma=t._arbi_mkt_a||'', mb=t._arbi_mkt_b||'';
      symCell='<td style="padding:10px 14px;font-weight:600">'+t.symbol+'<div style="font-size:9px;color:var(--text3);font-family:monospace">'+la+'/'+lb+'</div></td>';
      mktCell='<td style="padding:10px 14px;color:#f39c12;font-size:11px">ARBI<div style="font-size:9px;color:var(--text3)">'+ma+'/'+mb+'</div></td>';
      dirCell='<td style="padding:10px 14px;color:#f39c12;font-size:11px;font-weight:600">SPREAD</td>';
      entryCell='<td style="padding:10px 14px;text-align:right;font-family:monospace">'+parseFloat(t.entry_price||0).toFixed(3)+'%</td>';
      exitCell='<td style="padding:10px 14px;text-align:right;font-family:monospace">'+parseFloat(t.exit_price||0).toFixed(3)+'%</td>';
    } else {
      symCell='<td style="padding:10px 14px;font-weight:600">'+t.symbol+'</td>';
      mktCell='<td style="padding:10px 14px;color:var(--text3);font-size:11px">'+(t.market||'')+'</td>';
      dirCell='<td style="padding:10px 14px;color:'+dirColor+';font-size:11px;font-weight:600">'+dir+'</td>';
      entryCell='<td style="padding:10px 14px;text-align:right;font-family:monospace">$'+parseFloat(t.entry_price||0).toFixed(2)+'</td>';
      exitCell='<td style="padding:10px 14px;text-align:right;font-family:monospace">$'+parseFloat(t.exit_price||0).toFixed(2)+'</td>';
    }
    return '<tr style="border-bottom:1px solid var(--line)'+(isArbi?';background:rgba(243,156,18,0.04)':'')+'">'
      +symCell+mktCell+dirCell+entryCell+exitCell
      +'<td style="padding:10px 14px;text-align:right;color:'+pc(pnl)+';font-family:monospace;font-weight:600">'+(pnl>=0?'+':'')+fmtBig(pnl)+'</td>'
      +'<td style="padding:10px 14px;text-align:right;color:'+pc(pct)+';font-family:monospace">'+(pct>=0?'+':'')+pct.toFixed(2)+'%</td>'
      +'<td style="padding:10px 14px;color:var(--text3);font-size:11px">'+( rm[t.close_reason]||t.close_reason||'--')+'</td>'
      +'<td style="padding:10px 14px;color:var(--text3);font-size:11px">'+fmtDT(t.closed_at||t.opened_at)+'</td>'
      +'</tr>';
  }).join('');
}
function _calcCatStats(trades,cat){
  var list=trades.filter(function(t){return _catOfTrade(t)===cat;});
  var wins=list.filter(function(t){return (t.pnl||0)>0;});
  var totalPnl=list.reduce(function(a,t){return a+(t.pnl||0);},0);
  var totalInvested=list.reduce(function(a,t){return a+(t.position_value||0);},0);
  var wonInvested=wins.reduce(function(a,t){return a+(t.position_value||0);},0);
  var wr=list.length?((wins.length/list.length)*100):0;
  var valWr=totalInvested>0?((wonInvested/totalInvested)*100):0;
  return {pnl:totalPnl,wr:wr,count:list.length,valWr:valWr};
}
function renderHistory(trades,s){
  _allHistTrades=trades||[];
  _renderHistoryTable(_allHistTrades);
  var wr=s.win_rate||0;
  set('hist-winrate',wr.toFixed(1)+'%');
  set('hist-total',(s.closed_trades||0)+' trades');
  set('hist-closed-pnl',fmtBig(s.closed_pnl||0),pc(s.closed_pnl||0));
  var totalPnl=(s.total_pnl||0);
  var initCap=(s.initial_capital||8000000);
  var pctCap=initCap>0?((totalPnl/initCap)*100):0;
  set('hist-best',fmtBig(totalPnl),pc(totalPnl));
  set('hist-pct-capital',(pctCap>=0?'+':'')+pctCap.toFixed(2)+'%',pc(pctCap));
  // Stats por categoria do backend
  function setEl(id,val,color){var e=document.getElementById(id);if(e){e.textContent=val;if(color)e.style.color=color;}}
  function pcolor(v){return v>=0?'#2ecc71':'#e74c3c';}
  if(s){
    // STOCKS
    var stkStats=_calcCatStats(_allHistTrades,'stocks');
    setEl('hist-stocks-pnl',fmtBig(s.stocks_closed_pnl||stkStats.pnl),pcolor(s.stocks_closed_pnl||stkStats.pnl));
    var sWr=s.stocks_win_rate||stkStats.wr;
    setEl('hist-stocks-wr',sWr.toFixed(1)+'%',sWr>=50?'#2ecc71':'#e74c3c');
    setEl('hist-stocks-daily',fmtBig(s.stocks_daily_pnl||0),pcolor(s.stocks_daily_pnl||0));
    setEl('hist-stocks-monthly',fmtBig(s.stocks_monthly_pnl||0),pcolor(s.stocks_monthly_pnl||0));
    setEl('hist-stocks-annual',fmtBig(s.stocks_annual_pnl||0),pcolor(s.stocks_annual_pnl||0));
    setEl('hist-stocks-count',(s.stocks_closed_trades||stkStats.count)+' trades');
    // CRYPTO
    var cryStats=_calcCatStats(_allHistTrades,'crypto');
    setEl('hist-crypto-pnl',fmtBig(s.crypto_closed_pnl||cryStats.pnl),pcolor(s.crypto_closed_pnl||cryStats.pnl));
    var cWr=s.crypto_win_rate||cryStats.wr;
    setEl('hist-crypto-wr',cWr.toFixed(1)+'%',cWr>=50?'#2ecc71':'#e74c3c');
    setEl('hist-crypto-daily',fmtBig(s.crypto_daily_pnl||0),pcolor(s.crypto_daily_pnl||0));
    setEl('hist-crypto-monthly',fmtBig(s.crypto_monthly_pnl||0),pcolor(s.crypto_monthly_pnl||0));
    setEl('hist-crypto-annual',fmtBig(s.crypto_annual_pnl||0),pcolor(s.crypto_annual_pnl||0));
    setEl('hist-crypto-count',(s.crypto_closed_trades||cryStats.count)+' trades');
    // ARBI
    var ab=s.arbi_book||{};
    setEl('hist-arbi-pnl',fmtBig(ab.total_pnl||0),pcolor(ab.total_pnl||0));
    setEl('hist-arbi-wr',(ab.win_rate||0).toFixed(1)+'%',(ab.win_rate||0)>=50?'#2ecc71':'#e74c3c');
    setEl('hist-arbi-daily',fmtBig(ab.daily_pnl||0),pcolor(ab.daily_pnl||0));
    setEl('hist-arbi-monthly',fmtBig(ab.monthly_pnl||0),pcolor(ab.monthly_pnl||0));
    setEl('hist-arbi-annual',fmtBig(ab.annual_pnl||0),pcolor(ab.annual_pnl||0));
    setEl('hist-arbi-count',(ab.closed_trades||0)+' trades');
  }
}

function filterSignals(f,el){
  currentFilter=f;
  document.querySelectorAll('.filter-btn').forEach(function(b){b.classList.remove('active');});
  if(el)el.classList.add('active');
  renderTable(allSignals);
}

/* ═══ [v2] SORT de colunas (Item 2) ═══ */
var currentSort = { key: null, dir: 'desc' };
function sortSignals(key){
  if(currentSort.key === key){
    currentSort.dir = currentSort.dir === 'asc' ? 'desc' : 'asc';
  } else {
    currentSort.key = key;
    currentSort.dir = 'desc';
  }
  // Atualizar classes visuais das setas
  document.querySelectorAll('.sig-th').forEach(function(th){
    var k = th.getAttribute('data-sort-key');
    th.classList.remove('sort-asc','sort-desc','sort-none');
    if(k === key) th.classList.add(currentSort.dir === 'asc' ? 'sort-asc' : 'sort-desc');
    else th.classList.add('sort-none');
  });
  renderTable(allSignals);
}

function _applySortToList(list, key, dir){
  if(!key) return list;
  var mul = dir === 'asc' ? 1 : -1;
  return list.slice().sort(function(a, b){
    var va = a[key], vb = b[key];
    if(va == null && vb == null) return 0;
    if(va == null) return 1;
    if(vb == null) return -1;
    if(typeof va === 'number' && typeof vb === 'number') return (va - vb) * mul;
    return String(va).localeCompare(String(vb), 'pt-BR', {numeric:true}) * mul;
  });
}

function showPage(page,el){
  document.querySelectorAll('.page').forEach(function(p){p.classList.remove('active');});
  document.querySelectorAll('.nav-item').forEach(function(n){n.classList.remove('active');});
  var pg=document.getElementById('page-'+page);
  if(pg)pg.classList.add('active');
  if(el)el.classList.add('active');
  if(page==='arbitrage')loadArbitrage();
  if(page==='watchlist')loadWatchlist();
  if(page==='stocks-perf')loadStocksPerf();
  if(page==='crypto-perf')loadCryptoPerf();
  if(page==='reports'&&!_currentReport){setTimeout(function(){loadReport('daily',document.getElementById('rpt-btn-daily'));},100);}
  if(page==='performance')loadPerformance();
  if(page==='settings')loadSettings();
  if(page==='network'){loadNetworkIntelligence();}
}

async function loadArbitrage(){
  try{
    var r=await apiFetch(API_BASE+'/arbitrage/trades');
    var d=await r.json();
    set('arbi-open-count',d.open_count||0);
    set('arbi-closed-count',d.closed_count||0);
    var arbiClosedPnl=d.closed_pnl||0;
    var arbiOpenPnlV=d.open_pnl||0;
    var arbiTotalPnl=d.total_pnl||0;
    var arbiCap=d.capital||0;
    var arbiInitCap=d.initial_capital||3000000;
    // portfolio_value = capital inicial + total P&L (closed + open)
    var arbiPortVal=arbiInitCap+arbiTotalPnl;
    var arbiGainPct=arbiInitCap>0?(arbiTotalPnl/arbiInitCap*100):0;
    // investido = capital inicial - capital livre
    var arbiInvested=Math.max(0,arbiInitCap-arbiCap);
    set('arbi-initial-cap','$'+fmtNum(arbiInitCap));
    set('arbi-portfolio-val','$'+fmtNum(arbiPortVal));
    var arbiGainEl=document.getElementById('arbi-gain-pct');
    if(arbiGainEl){arbiGainEl.textContent=(arbiGainPct>=0?'+':'')+arbiGainPct.toFixed(2)+'% vs capital inicial';arbiGainEl.style.color=arbiGainPct>=0?'#2ecc71':'#e74c3c';}
    set('arbi-capital','$'+fmtNum(arbiCap));
    set('arbi-invested','$'+fmtNum(arbiInvested));
    set('arbi-open-pnl-val',fmtBig(arbiOpenPnlV),pc(arbiOpenPnlV));
    set('arbi-total-pnl',fmtBig(arbiClosedPnl),pc(arbiClosedPnl));
    var wrEl=document.getElementById('arbi-winrate');
    if(wrEl){wrEl.textContent=(d.win_rate||0).toFixed(1)+'%';wrEl.style.color=(d.win_rate||0)>=50?'#2ecc71':'#e74c3c';}
    // daily/monthly vem do statsData (arbi_book)
    var ab=statsData&&statsData.arbi_book||{};
    set('arbi-daily-pnl',fmtBig(ab.daily_pnl||0),pc(ab.daily_pnl||0));
    set('arbi-monthly-pnl',fmtBig(ab.monthly_pnl||0),pc(ab.monthly_pnl||0));
    var ob=document.getElementById('arbi-open-body');
    var ot=d.open_trades||[];
    if(ob){
      if(!ot.length){ob.innerHTML='<tr><td colspan="6" style="padding:30px;text-align:center;color:var(--text3)">Nenhuma posição aberta</td></tr>';}
      else{
        ob.innerHTML=ot.map(function(t){
          var pnl=t.pnl||0;
          var buyLeg=t.buy_leg||t.leg_a||'—';
          var buyMkt=t.buy_mkt||t.mkt_a||'—';
          var sellLeg=t.short_leg||t.leg_b||'—';
          var sellMkt=t.short_mkt||t.mkt_b||'—';
          var spIn=parseFloat(t.entry_spread||0).toFixed(2);
          var spOut=parseFloat(t.current_spread||0).toFixed(2);
          var spColor=pnl>=0?'#2ecc71':'#e74c3c';
          return '<tr style="border-bottom:1px solid var(--line)">'
            +'<td style="padding:10px 14px;font-weight:600">'+t.name+'</td>'
            +'<td style="padding:10px 14px"><span style="color:#2ecc71;font-weight:600;font-size:11px">COMPRA</span> <span style="font-family:monospace;font-size:11px">'+buyLeg+'</span> <span style="color:var(--text3);font-size:10px;background:var(--navy2);padding:1px 5px;border-radius:2px">'+buyMkt+'</span></td>'
            +'<td style="padding:10px 14px"><span style="color:#e74c3c;font-weight:600;font-size:11px">VENDA</span> <span style="font-family:monospace;font-size:11px">'+sellLeg+'</span> <span style="color:var(--text3);font-size:10px;background:var(--navy2);padding:1px 5px;border-radius:2px">'+sellMkt+'</span></td>'
            +'<td style="padding:10px 14px;text-align:right;font-family:monospace"><span style="color:var(--text3)">'+spIn+'%</span> → <span style="color:'+spColor+'">'+spOut+'%</span></td>'
            +'<td style="padding:10px 14px;text-align:right;color:'+pc(pnl)+';font-weight:600;font-family:monospace">'+(pnl>=0?'+':'')+fmtBig(pnl)+'</td>'
            +'<td style="padding:10px 14px;text-align:right;font-family:monospace">$'+fmtNum(t.position_size||0)+'</td>'
            +'<td style="padding:10px 14px;text-align:right;color:var(--text3);font-size:11px">'+fmtDT(t.opened_at)+'</td>'
            +'</tr>';
        }).join('');
      }
    }
    var r2=await apiFetch(API_BASE+'/arbitrage/spreads');
    var d2=await r2.json();
    var opps=d2.opportunities||[];
    var aob=document.getElementById('arbi-opps-body');
    if(aob){
      if(!opps.length){aob.innerHTML='<tr><td colspan="6" style="padding:20px;text-align:center;color:var(--text3)">Nenhuma oportunidade acima do threshold</td></tr>';}
      else{
        aob.innerHTML=opps.map(function(s){
            var dirA=s.direction==='LONG_A'?'<span style="color:#2ecc71;font-weight:600">COMPRA</span>':'<span style="color:#e74c3c;font-weight:600">VENDA</span>';
            var dirB=s.direction==='LONG_A'?'<span style="color:#e74c3c;font-weight:600">VENDA</span>':'<span style="color:#2ecc71;font-weight:600">COMPRA</span>';
            var pa=parseFloat(s.price_a_usd||s.price_a||0).toFixed(2);
            var pb=parseFloat(s.price_b_usd||s.price_b||0).toFixed(2);
            var spColor=s.abs_spread>=3?'#e74c3c':s.abs_spread>=1.5?'#f39c12':'#8fa3be';
            return '<tr style="border-bottom:1px solid var(--line)">'
            +'<td style="padding:10px 14px;font-weight:600">'+s.name+'</td>'
            +'<td style="padding:10px 14px;color:var(--text2);font-size:11px">'+s.leg_a+' '+dirA+' <span style="color:var(--text3)">$'+pa+'</span></td>'
            +'<td style="padding:10px 14px;color:var(--text2);font-size:11px">'+s.leg_b+' '+dirB+' <span style="color:var(--text3)">$'+pb+'</span></td>'
            +'<td style="padding:10px 14px;text-align:right;color:'+spColor+';font-weight:600">'+parseFloat(s.spread_pct||0).toFixed(2)+'%</td>'
            +'<td style="padding:10px 14px;text-align:center;color:var(--text3);font-size:11px">'+s.mkt_a+'/'+s.mkt_b+'</td>'
            +'<td style="padding:10px 14px;text-align:right;color:var(--text3);font-size:11px">'+parseFloat(s.fx_rate||0).toFixed(3)+'</td>'
            +'</tr>';
        }).join('');
      }
    }
  // Arbi Charts
  try {
    var ct = d.closed_trades||[];
    if (ct.length && window.Chart) {
      // Acumulado por data
      var arbiByDate = {};
      ct.forEach(function(t){
        var dt=(t.closed_at||'').slice(0,10);
        if(!arbiByDate[dt]) arbiByDate[dt]={pnl:0};
        arbiByDate[dt].pnl+=t.pnl||0;
      });
      var arbiDates=Object.keys(arbiByDate).sort();
      var arbiCumPnl=[]; var arbiAcc=0;
      arbiDates.forEach(function(d){ arbiAcc+=arbiByDate[d].pnl; arbiCumPnl.push(+(arbiAcc/1000).toFixed(1)); });
      _mc('arbi-chart-cum',{type:'line',data:{labels:arbiDates.map(function(d){return d.slice(5);}),datasets:[{data:arbiCumPnl,borderColor:'#00cec9',backgroundColor:'rgba(0,206,201,0.1)',fill:true,tension:0.3,pointRadius:3,borderWidth:2}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:'#8fa3be',font:{size:8}},grid:{color:'rgba(255,255,255,0.05)'}},y:{ticks:{color:'#8fa3be',font:{size:9},callback:function(v){return(v>=0?'+':'')+v.toFixed(0)+'K';}},grid:{color:'rgba(255,255,255,0.05)'}}}}});
      // Motivos
      var arbiReasons={};
      ct.forEach(function(t){ var r=t.close_reason||'?'; arbiReasons[r]=(arbiReasons[r]||0)+1; });
      var rKeys=Object.keys(arbiReasons); var rCols={'TAKE_PROFIT':'#1D9E75','STOP_LOSS':'#e74c3c','TIMEOUT':'#4a9eff','MARKET_CLOSE':'#9b59b6','TRAILING_STOP':'#f39c12'};
      _mc('arbi-chart-reasons',{type:'doughnut',data:{labels:rKeys.map(function(r){return r.replace('_',' ')+' ('+arbiReasons[r]+')';}),datasets:[{data:rKeys.map(function(r){return arbiReasons[r];}),backgroundColor:rKeys.map(function(r){return rCols[r]||'#8fa3be';}),borderWidth:0,hoverOffset:6}]},options:{responsive:true,maintainAspectRatio:false,cutout:'55%',plugins:{legend:{position:'right',labels:{color:'#8fa3be',font:{size:8},boxWidth:8,padding:6}}}}});
      // Pares
      var arbiPairs={};
      ct.forEach(function(t){ var p=t.name||t.pair_id||'?'; if(!arbiPairs[p]) arbiPairs[p]=0; arbiPairs[p]+=(t.pnl||0); });
      var pairArr=Object.keys(arbiPairs).map(function(k){return{n:k,v:arbiPairs[k]};}).sort(function(a,b){return b.v-a.v;}).slice(0,8);
      _mc('arbi-chart-pairs',{type:'bar',data:{labels:pairArr.map(function(p){return p.n;}),datasets:[{data:pairArr.map(function(p){return+(p.v/1000).toFixed(1);}),backgroundColor:pairArr.map(function(p){return p.v>=0?'rgba(0,206,201,0.7)':'rgba(231,76,60,0.7)';}),borderRadius:3}]},options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:'#8fa3be',font:{size:9},callback:function(v){return(v>=0?'+':'')+v.toFixed(0)+'K';}},grid:{color:'rgba(255,255,255,0.05)'}},y:{ticks:{color:'#8fa3be',font:{size:9}},grid:{display:false}}}}});
      // Tabela fechadas
      var acb=document.getElementById('arbi-closed-body');
      if(acb){ acb.innerHTML=ct.slice(0,50).map(function(t){ var pc=t.pnl>=0?'#2ecc71':'#e74c3c'; var rCols2={'TAKE_PROFIT':'#2ecc71','STOP_LOSS':'#e74c3c','TIMEOUT':'#8fa3be','MARKET_CLOSE':'#4a9eff','TRAILING_STOP':'#f39c12'}; return '<tr style="border-bottom:1px solid rgba(255,255,255,0.05)">'+'<td style="padding:6px 12px;font-weight:600">'+( t.name||t.pair_id||'—')+'</td>'+'<td style="padding:6px 12px;text-align:right;font-family:monospace;font-size:11px">'+parseFloat(t.entry_spread||0).toFixed(2)+'%</td>'+'<td style="padding:6px 12px;text-align:right;font-family:monospace;font-size:11px">'+parseFloat(t.current_spread||0).toFixed(2)+'%</td>'+'<td style="padding:6px 12px;text-align:right;color:'+pc+';font-weight:600">'+_fmtK(t.pnl)+'</td>'+'<td style="padding:6px 12px;text-align:right;color:'+pc+';font-size:11px">'+(t.pnl_pct>=0?'+':'')+parseFloat(t.pnl_pct||0).toFixed(2)+'%</td>'+'<td style="padding:6px 12px;text-align:center;font-size:10px;color:'+(rCols2[t.close_reason]||'#8fa3be')+'">'+(t.close_reason||'—').replace('_',' ')+'</td>'+'<td style="padding:6px 12px;text-align:right;color:#8fa3be;font-size:10px">'+(t.opened_at||'').slice(5,16)+'</td>'+'</tr>'; }).join('');}
    }
  } catch(arbiChartErr) { console.warn('Arbi charts:', arbiChartErr); }
    makeSortable('arbi-closed-table');
  }catch(e){console.error('loadArbitrage error:',e);}
}

// ── Watchlist ──────────────────────────────────────────
var wlData=[];
async function loadWatchlist(){
  try{
    var r=await apiFetch(API_BASE+'/watchlist');
    var d=await r.json();
    wlData=d.symbols||[];
    renderWatchlist();
    for(var i=0;i<wlData.length;i++){
      await loadWlQuote(wlData[i]);
    }
  }catch(e){console.error('loadWatchlist error:',e);}
}

async function loadWlQuote(w){
  try{
    var r=await apiFetch(API_BASE+'/watchlist/quote?symbol='+encodeURIComponent(w.symbol)+'&market='+encodeURIComponent(w.market));
    var q=await r.json();
    var row=document.getElementById('wl-row-'+w.symbol);
    if(!row)return;
    var price=q.price||0,chg=q.change_pct||0;
    row.querySelector('.wl-price').textContent=(q.currency==='BRL'?'R$':'$')+parseFloat(price).toFixed(2);
    var cv=row.querySelector('.wl-chg');
    cv.textContent=(chg>=0?'+':'')+parseFloat(chg).toFixed(2)+'%';
    cv.style.color=pc(chg);
    row.querySelector('.wl-vol').textContent=fmtNum(q.volume||0);
  }catch(e){}
}

function renderWatchlist(){
  var tb=document.getElementById('wl-body');if(!tb)return;
  if(!wlData.length){
    tb.innerHTML='<tr><td colspan="6" style="padding:40px;text-align:center;color:var(--text3);font-size:11px">WATCHLIST VAZIA — ADICIONE ACIMA</td></tr>';
    return;
  }
  tb.innerHTML=wlData.map(function(w){
    return '<tr id="wl-row-'+w.symbol+'" style="border-bottom:1px solid var(--line)">'
      +'<td style="padding:10px 14px;font-weight:600">'+w.symbol+'</td>'
      +'<td style="padding:10px 14px;color:var(--text2)">'+w.market+'</td>'
      +'<td style="padding:10px 14px;text-align:right" class="wl-price">...</td>'
      +'<td style="padding:10px 14px;text-align:right" class="wl-chg">...</td>'
      +'<td style="padding:10px 14px;text-align:right" class="wl-vol">...</td>'
      +'<td style="padding:10px 14px;text-align:center">'
        +'<button data-s="'+w.symbol+'" onclick="wlRemove(this)" style="background:rgba(231,76,60,.15);border:1px solid rgba(231,76,60,.3);color:#e74c3c;padding:4px 12px;font-size:10px;cursor:pointer">×</button>'
      +'</td>'
      +'</tr>';
  }).join('');
}

async function wlAdd(){
  var sym=(document.getElementById('wl-input').value||'').toUpperCase().trim();
  var mkt=document.getElementById('wl-mkt').value||'NYSE';
  if(!sym)return;
  try{
    await apiFetch(API_BASE+'/watchlist/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:sym,market:mkt})});
    document.getElementById('wl-input').value='';
    await loadWatchlist();
  }catch(e){alert('Erro ao adicionar: '+e);}
}

async function wlRemove(btn){
  var sym=btn.getAttribute('data-s');
  try{
    await apiFetch(API_BASE+'/watchlist/remove',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:sym})});
    await loadWatchlist();
  }catch(e){}
}


// === NETWORK INTELLIGENCE ===
var _netLoaded=false;
async function loadNetworkIntelligence(force){
  if(_netLoaded&&!force)return; _netLoaded=true;
  var BASE=API_BASE;
  try{
    var resp=await apiFetch(BASE+'/sync/export');
    var r=await resp.json();
    document.getElementById('net-l-patterns').textContent=r.learning.total_patterns||'0';
    document.getElementById('net-l-signals').textContent=(r.hot_signals||[]).length;
    var mkArr=Object.values(r.market_stats||{});
    var avgWr=mkArr.length?Math.round(mkArr.reduce(function(s,m){return s+m.win_rate},0)/mkArr.length):0;
    document.getElementById('net-l-wr').textContent=avgWr+'%';
    document.getElementById('net-l-conf').textContent=r.learning.avg_confidence||'—';
    var signals=r.hot_signals||[];
    var html=signals.length===0?'<div style="text-align:center;padding:20px;color:var(--text3)">Nenhum sinal quente</div>'
      :'<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:10px">'+signals.map(function(s){
        var c=s.action==='BUY'?'#39ff14':'#ff4444';
        return '<div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.07);border-radius:6px;padding:12px">'
          +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">'
          +'<span style="font-weight:600;color:var(--white);font-size:12px">'+s.symbol+'</span>'
          +'<span style="font-size:10px;font-weight:600;color:'+c+'">'+s.action+'</span></div>'
          +'<div style="font-size:11px;color:var(--gold2)">Score: '+Math.round(s.score)+'</div></div>';
      }).join('')+'</div>';
    document.getElementById('net-local-signals').innerHTML=html;
    document.getElementById('net-last-sync').textContent='Última sincronização: '+new Date().toLocaleTimeString('pt-BR');
    // Buscar dados do peer Manus via Railway /sync/peer-data (que consulta e retorna snapshot)
    try{
      var peResp=await apiFetch(API_BASE+'/sync/peer-data');
      if(peResp.ok){var pe=await peResp.json();
        var r2=pe.peer;
        if(r2&&r2.learning){
          document.getElementById('net-r-dot').style.background='#39ff14';
          document.getElementById('net-r-patterns').textContent=r2.learning.total_patterns||'0';
          document.getElementById('net-r-signals').textContent=(r2.hot_signals||[]).length;
          var mkArr2=Object.values(r2.market_stats||{});
          var wr2=mkArr2.length?Math.round(mkArr2.reduce(function(s,m){return s+m.win_rate},0)/mkArr2.length):0;
          document.getElementById('net-r-wr').textContent=wr2+'%';
          document.getElementById('net-r-conf').textContent=r2.learning?r2.learning.avg_confidence||'—':'—';
          // Salvar snapshot do peer no backend local
          apiFetch(API_BASE+'/sync/import',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(r2)});
        } else {
          // peer offline — mostrar latência se disponível
          if(pe.peer_error) document.getElementById('net-r-dot').style.background='#ff4444';
        }
      }
    }catch(e2){}
  }catch(e){console.error('Network sync error:',e);}
}

/* ═══ [v2] FX USD/EUR realtime (Item 8) ═══ */
var _fxPrev = {};
async function updateFxChips(){
  try {
    var r = await fetch(API_BASE + '/api/fx-rates');
    if(!r.ok) return;
    var d = await r.json();
    var fx = (d && d.fx) || {};
    var usd = fx.USDBRL, eur = fx.EURBRL;
    var html = '';
    if(usd){
      var dir = _fxPrev.usd ? (usd > _fxPrev.usd ? 'up' : (usd < _fxPrev.usd ? 'down' : '')) : '';
      html += '<div class="fx-chip '+dir+'"><span class="fx-label">USD</span><span class="fx-value">R$ '+usd.toFixed(4)+'</span></div>';
      _fxPrev.usd = usd;
    }
    if(eur){
      var dir = _fxPrev.eur ? (eur > _fxPrev.eur ? 'up' : (eur < _fxPrev.eur ? 'down' : '')) : '';
      html += '<div class="fx-chip '+dir+'"><span class="fx-label">EUR</span><span class="fx-value">R$ '+eur.toFixed(4)+'</span></div>';
      _fxPrev.eur = eur;
    }
    var el = document.getElementById('fx-rates');
    if(el) el.innerHTML = html;
  } catch(e) {}
}

loadAll();
updateFxChips();                           // [v2] FX inicial
setInterval(loadAll,10*1000);
setInterval(loadLivePrices,3000);
setInterval(updateFxChips, 30000);         // [v2] FX a cada 30s

