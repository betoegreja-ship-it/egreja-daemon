"""
Phase 4b: Arbitrage Module
Extracted from api_server.py (lines ~6125-6937)
Uses context-passing pattern for global state management.
"""

import time
import threading
import requests
from datetime import datetime
from collections import defaultdict

# ═══════════════════════════════════════════════════════════════
# Module-level globals (local to arbitrage.py)
# ═══════════════════════════════════════════════════════════════
_arbi_learning_cache = {}  # pair_id → {zone → {n, wins, pnl}}
_arbi_learning_lock = threading.Lock()
_arbi_pair_stats = {}  # {pair_id: {wins_low: int, n_low: int, wins_high: int, n_high: int, ...}}

# ═══════════════════════════════════════════════════════════════
# ARBI Pair Configuration and Constants
# ═══════════════════════════════════════════════════════════════

ARBI_PAIRS = [
    # PETR4-PBR REATIVADO — alta liquidez, importante para mercado real
    # Proteções: min_spread 10% (zona HIGH WR67%), max_pos $30K fixo, sanity spread >20%
    # Bug anterior (-$896K) foi por posição $1M + preço PBR=0 → corrigido
    {'id':'PETR4-PBR', 'leg_a':'PETR4.SA','leg_b':'PBR', 'mkt_a':'B3','mkt_b':'NYSE',
     'fx':'USDBRL','name':'Petrobras','ratio_a':2,'ratio_b':1},
    {'id':'ITUB4-ITUB',  'leg_a':'ITUB4.SA', 'leg_b':'ITUB',   'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Itaú',        'ratio_a':1,'ratio_b':1},
    {'id':'BBDC4-BBD',   'leg_a':'BBDC4.SA', 'leg_b':'BBD',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Bradesco',    'ratio_a':1,'ratio_b':1},
    {'id':'ABEV3-ABEV',  'leg_a':'ABEV3.SA', 'leg_b':'ABEV',   'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Ambev',       'ratio_a':1,'ratio_b':1},
    # Embraer (EMBR3/ERJ) removida — ERJ sem cobertura de preço disponível
    {'id':'GGBR4-GGB',   'leg_a':'GGBR4.SA', 'leg_b':'GGB',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Gerdau',      'ratio_a':1,'ratio_b':1},
    {'id':'CSNA3-SID',   'leg_a':'CSNA3.SA', 'leg_b':'SID',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'CSN',         'ratio_a':1,'ratio_b':1},
    {'id':'CMIG4-CIG',   'leg_a':'CMIG4.SA', 'leg_b':'CIG',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Cemig',       'ratio_a':1,'ratio_b':1},
    # Copel (CPLE6/ELP) removida — ELP ADR sem cobertura de preço disponível
    {'id':'BP-BP.L',     'leg_a':'BP',       'leg_b':'BP.L',   'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'BP',          'ratio_a':1,'ratio_b':6},
    {'id':'SHEL-SHEL.L', 'leg_a':'SHEL',     'leg_b':'SHEL.L', 'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'Shell',       'ratio_a':1,'ratio_b':2},
    {'id':'AZN-AZN.L',   'leg_a':'AZN',      'leg_b':'AZN.L',  'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'AstraZeneca', 'ratio_a':1,'ratio_b':1},
    {'id':'GSK-GSK.L',   'leg_a':'GSK',      'leg_b':'GSK.L',  'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'GSK',         'ratio_a':1,'ratio_b':2},
    {'id':'HSBC-HSBA.L', 'leg_a':'HSBC',     'leg_b':'HSBA.L', 'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'HSBC',        'ratio_a':1,'ratio_b':5},
    # [v10.9] HKEX pares removidos — NYSE e HKEX não têm sobreposição de horário (gap de 6h)
    # HKEX fecha 08:00 UTC, NYSE abre 14:30 UTC → jamais executariam. 0 trades em todo o histórico.
    # Removidos: Tencent, Alibaba, HSBC HK, China Mobile, Ping An

    # ── B3/NYSE novos ─────────────────────────────────────────────────────────
    # Ratios confirmados via preços reais: spread ≈ 0% quando mercados eficientes
    # pa = (preco_BRL / USDBRL) × ratio_a  |  pb = preco_USD × ratio_b
    {'id':'SUZB3-SUZ',  'leg_a':'SUZB3.SA','leg_b':'SUZ',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Suzano',      'ratio_a':1,'ratio_b':1},
    {'id':'SBSP3-SBS',  'leg_a':'SBSP3.SA','leg_b':'SBS',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Sabesp',      'ratio_a':1,'ratio_b':1},
    {'id':'UGPA3-UGP',  'leg_a':'UGPA3.SA','leg_b':'UGP',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Ultrapar',    'ratio_a':1,'ratio_b':1},

    # ── NYSE/TSX (Canadá) — REMOVIDOS: mercado eficiente demais ─────────────
    # Spreads históricos: 0.00-0.09% — nunca atingem o limiar de 2.0% para abrir.
    # 7 pares (RBC, TD, Shopify, Suncor, CNQ, ENB, BNS) removidos em 20/03/2026.

    # ── NYSE/LSE adicionais — overlap 2h (14:30-16:30 UTC) ───────────────────
    # pa = preco_USD × ratio_a  |  pb = (preco_GBp / 100) × GBPUSD × ratio_b
    # ATENÇÃO: LSE cotações em GBp (pence), não £ — divisão por 100 obrigatória
    # Rio Tinto: 1 ADR NYSE = 1 ação LSE (ratio confirmado 0.996 ≈ 1:1)
    # Diageo: 1 ADR NYSE = 4 ações LSE (ratio confirmado 3.973 ≈ 4:1)
    {'id':'RIO-RIO.L',  'leg_a':'RIO',     'leg_b':'RIO.L',  'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'Rio Tinto',  'ratio_a':1,'ratio_b':1},
    {'id':'UL-ULVR.L',  'leg_a':'UL',      'leg_b':'ULVR.L', 'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'Unilever',   'ratio_a':1,'ratio_b':1},
    {'id':'DEO-DGE.L',  'leg_a':'DEO',     'leg_b':'DGE.L',  'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'Diageo',     'ratio_a':1,'ratio_b':4},
    {'id':'BTI-BATS.L', 'leg_a':'BTI',     'leg_b':'BATS.L', 'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'BAT',        'ratio_a':1,'ratio_b':1},

    # ── NYSE/EURONEXT — overlap 2h (14:30-16:30 UTC) ─────────────────────────
    # pa = preco_USD × ratio_a  |  pb = preco_EUR × EURUSD × ratio_b
    # Ratios confirmados ≈ 1:1. Spread estrutural de ~5-7% é REAL (custo ADR + bid-ask)
    {'id':'ASML-ASML.AS','leg_a':'ASML',   'leg_b':'ASML.AS','mkt_a':'NYSE','mkt_b':'EURONEXT','fx':'EURUSD','name':'ASML',       'ratio_a':1,'ratio_b':1},
    {'id':'TTE-TTE.PA',  'leg_a':'TTE',    'leg_b':'TTE.PA', 'mkt_a':'NYSE','mkt_b':'EURONEXT','fx':'EURUSD','name':'TotalEnergies','ratio_a':1,'ratio_b':1},
    {'id':'SAP-SAP.DE',  'leg_a':'SAP',    'leg_b':'SAP.DE', 'mkt_a':'NYSE','mkt_b':'XETRA',  'fx':'EURUSD','name':'SAP',         'ratio_a':1,'ratio_b':1},
    # LVMH: 1 ADR LVMUY (NYSE) = 0.2 ação MC.PA → ratio_a=5 para paridade com 1 ação LVMH
    # Verificado: LVMUY=$105 × 5 = $525 vs MC.PA=€458 × 1.1555 = $529 → spread -0.71% ✅
    {'id':'LVMUY-MC.PA', 'leg_a':'LVMUY',  'leg_b':'MC.PA',  'mkt_a':'NYSE','mkt_b':'EURONEXT','fx':'EURUSD','name':'LVMH',        'ratio_a':5,'ratio_b':1},

    # ── B3/NYSE adicionais ────────────────────────────────────────────────────
    # TIMS3/TIMB: ratio 5:1 verificado — 1 ADR TIMB = 5 ações TIMS3
    # Verificado: TIMS3=R$26.28 ÷ 5.2552 × 5 = $25.00 vs TIMB=$24.81 → spread +0.78% ✅
    {'id':'TIMS3-TIMB',  'leg_a':'TIMS3.SA','leg_b':'TIMB',  'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'TIM Brasil',     'ratio_a':5,'ratio_b':1},
    # BRF (BRFS3/BRFS) removida — ticker sem cobertura de preço disponível
]


def _spread_zone(abs_spread: float, pair_id: str = '') -> str:
    """Classifica o spread em zona para análise de padrão."""
    if abs_spread >= 12.0:  return 'EXTREME'   # muito alto — possível erro
    if abs_spread >= 10.0:  return 'HIGH'       # zona de reversão
    if abs_spread >=  9.0:  return 'MID'        # zona incerta
    if abs_spread >=  7.0:  return 'LOW'        # zona desfavorável
    return 'MINIMAL'                            # spread pequeno — não opera


def run_arbi_pattern_learning(ctx):
    """[v10.14] Aprende padrões de spread por par a partir das trades históricas.

    Analisa a tabela arbi_trades e descobre:
    - Qual zona de spread tem melhor WR para cada par
    - Ajusta ARBI_PAIR_CONFIG dinamicamente
    - Loga descobertas para auditoria

    Dimensões analisadas:
    pair_id × spread_zone × weekday × hour_utc
    """
    log = ctx['log']
    get_db = ctx['get_db']
    state_lock = ctx['state_lock']
    arbi_closed = ctx['arbi_closed']
    ARBI_PAIR_CONFIG = ctx['ARBI_PAIR_CONFIG']
    ARBI_MIN_SPREAD = ctx['ARBI_MIN_SPREAD']
    ARBI_TP_SPREAD = ctx['ARBI_TP_SPREAD']
    ARBI_SL_PCT = ctx['ARBI_SL_PCT']

    conn = get_db()
    if not conn: return
    try:
        cursor = conn.cursor(dictionary=True)
        # Buscar todas as trades de arbi fechadas
        # [v10.14-FIX] Combinar memória (mais atualizado) + DB (histórico completo)
        cursor.execute("""
            SELECT pair_id, name, entry_spread, current_spread, pnl, pnl_pct,
                   status, close_reason, opened_at, closed_at, direction,
                   position_size
            FROM arbi_trades
            WHERE status='CLOSED' AND pnl IS NOT NULL
            ORDER BY closed_at DESC
            LIMIT 500
        """)
        db_rows = cursor.fetchall()
        cursor.close(); conn.close()

        # Adicionar trades da memória que podem não estar no DB ainda
        mem_rows = []
        with state_lock:
            for t in arbi_closed:
                mem_rows.append({
                    'pair_id':      t.get('pair_id','?'),
                    'name':         t.get('name','?'),
                    'entry_spread': t.get('entry_spread', 0),
                    'current_spread': t.get('current_spread', 0),
                    'pnl':          t.get('pnl', 0),
                    'pnl_pct':      t.get('pnl_pct', 0),
                    'status':       'CLOSED',
                    'close_reason': t.get('close_reason', ''),
                    'opened_at':    t.get('opened_at', ''),
                    'closed_at':    t.get('closed_at', ''),
                    'direction':    t.get('direction', ''),
                    'position_size': t.get('position_size', 0),
                })

        # Deduplicar por pair_id + opened_at (memória tem prioridade)
        db_ids = {(r.get('pair_id',''), str(r.get('opened_at',''))[:16]) for r in mem_rows}
        db_only = [r for r in db_rows if (r.get('pair_id',''), str(r.get('opened_at',''))[:16]) not in db_ids]
        rows = mem_rows + db_only

        if not rows:
            return

        # Agrupar por par e zona de spread
        by_pair_zone = defaultdict(lambda: defaultdict(lambda: {
            'n':0,'wins':0,'pnl':0.0,'avg_duration_h':0.0,'stops':0
        }))
        by_pair_weekday = defaultdict(lambda: defaultdict(lambda: {'n':0,'wins':0,'pnl':0.0}))

        for row in rows:
            pair   = row.get('pair_id','?')
            spread = abs(float(row.get('entry_spread') or 0))
            pnl    = float(row.get('pnl') or 0)
            zone   = _spread_zone(spread, pair)
            win    = 1 if pnl > 0 else 0
            reason = row.get('close_reason','')

            # Por zona
            z = by_pair_zone[pair][zone]
            z['n']    += 1
            z['wins'] += win
            z['pnl']  += pnl
            if reason == 'STOP_LOSS': z['stops'] += 1

            # Por dia da semana
            try:
                dt = datetime.fromisoformat(str(row['opened_at']).replace('Z',''))
                wd = dt.strftime('%A')  # Monday, Tuesday...
                d = by_pair_weekday[pair][wd]
                d['n'] += 1; d['wins'] += win; d['pnl'] += pnl
            except: pass

        # Atualizar cache e ARBI_PAIR_CONFIG
        discoveries = []
        with _arbi_learning_lock:
            _arbi_learning_cache.clear()
            for pair, zones in by_pair_zone.items():
                _arbi_learning_cache[pair] = {}
                best_zone = None; best_wr = 0; best_pnl = -999999

                for zone, stats in zones.items():
                    if stats['n'] == 0: continue
                    wr  = stats['wins'] / stats['n'] * 100
                    avg = stats['pnl']  / stats['n']
                    stop_rate = stats['stops'] / stats['n'] * 100
                    _arbi_learning_cache[pair][zone] = {
                        'n': stats['n'], 'wr': round(wr,1),
                        'avg_pnl': round(avg,0), 'stop_rate': round(stop_rate,1)
                    }
                    # Zona com WR ≥ 60% e avg positivo é candidata a best
                    if wr >= 60 and avg > best_pnl and stats['n'] >= 3:
                        best_wr   = wr
                        best_pnl  = avg
                        best_zone = zone

                # Ajustar ARBI_PAIR_CONFIG baseado na melhor zona encontrada
                zone_to_threshold = {
                    'EXTREME': 12.0, 'HIGH': 10.0, 'MID': 9.0,
                    'LOW': 7.0, 'MINIMAL': ARBI_MIN_SPREAD
                }
                if best_zone:
                    new_min = zone_to_threshold.get(best_zone, ARBI_MIN_SPREAD)
                    if pair in ARBI_PAIR_CONFIG:
                        old_min = ARBI_PAIR_CONFIG[pair].get('min_spread', ARBI_MIN_SPREAD)
                        if abs(new_min - old_min) >= 0.5:  # só atualiza se mudança significativa
                            ARBI_PAIR_CONFIG[pair]['min_spread'] = new_min
                            discoveries.append(
                                f"{pair}: min_spread {old_min:.1f}%→{new_min:.1f}% "
                                f"(best_zone={best_zone}, WR={best_wr:.0f}%, avg=${best_pnl:.0f})"
                            )
                    else:
                        # Par novo — criar config dinâmico
                        ARBI_PAIR_CONFIG[pair] = {
                            'min_spread':   new_min,
                            'tp_spread':    ARBI_TP_SPREAD,
                            'sl_pct':       ARBI_SL_PCT,
                            'max_spread':   15.0,
                            'last_10_wr':   best_wr,
                            'learn_window': 10,
                            'note':         f'auto-learned: best_zone={best_zone}',
                        }
                        discoveries.append(
                            f"{pair}: NOVO config — min_spread={new_min:.1f}% "
                            f"(best_zone={best_zone}, WR={best_wr:.0f}%)"
                        )

        # Log das descobertas
        log.info(f'[ArbiLearning] Analisados {len(rows)} trades de {len(by_pair_zone)} pares')
        for d in discoveries:
            log.info(f'[ArbiLearning] AJUSTE: {d}')

        # Log do estado atual por par
        for pair, zones in _arbi_learning_cache.items():
            summary = ' | '.join(
                f"{z}: WR={s['wr']:.0f}% n={s['n']} avg=${s['avg_pnl']:.0f}"
                for z, s in sorted(zones.items())
                if s['n'] >= 2
            )
            if summary:
                log.info(f'[ArbiLearning] {pair}: {summary}')

    except Exception as e:
        log.error(f'run_arbi_pattern_learning: {e}')
    finally:
        try:
            if cursor: cursor.close()
            if conn: conn.close()
        except: pass


def _arbi_pair_learning(ctx, pair_id, recent_trades):
    """[v10.14] Aprende o threshold ideal para cada par baseado nos últimos trades.
    Ajusta min_spread dinamicamente: se WR cai, sobe o threshold de entrada.
    """
    log = ctx['log']
    ARBI_PAIR_CONFIG = ctx['ARBI_PAIR_CONFIG']
    ARBI_MIN_SPREAD = ctx['ARBI_MIN_SPREAD']

    cfg = ARBI_PAIR_CONFIG.get(pair_id, ARBI_PAIR_CONFIG['_default'])
    if not recent_trades or len(recent_trades) < 3:
        return cfg

    # Agrupar por zona de spread de entrada
    zones = {'high': [], 'mid': [], 'low': []}
    for t in recent_trades[-cfg.get('learn_window', 10):]:
        abs_spread = abs(float(t.get('entry_spread', 0)))
        pnl = float(t.get('pnl', 0))
        if abs_spread >= 10.0:   zones['high'].append(pnl)
        elif abs_spread >= 9.0:  zones['mid'].append(pnl)
        else:                    zones['low'].append(pnl)

    # Calcular WR por zona
    def wr(lst): return sum(1 for p in lst if p > 0) / len(lst) * 100 if lst else 0
    wr_high = wr(zones['high'])
    wr_mid  = wr(zones['mid'])
    wr_low  = wr(zones['low'])

    # Ajustar threshold: usar a menor zona com WR ≥ 60%
    new_min = cfg.get('min_spread', ARBI_MIN_SPREAD)
    if wr_high >= 60 and len(zones['high']) >= 3:
        new_min = 10.0  # zona alta funciona
    if wr_mid  >= 60 and len(zones['mid'])  >= 3:
        new_min = 9.0   # zona média também funciona → relaxar
    if wr_low  < 40 and len(zones['low'])   >= 3:
        new_min = max(new_min, 9.5)  # zona baixa ruim → endurecer

    # Atualizar config em memória
    if pair_id in ARBI_PAIR_CONFIG:
        ARBI_PAIR_CONFIG[pair_id]['min_spread'] = new_min
        ARBI_PAIR_CONFIG[pair_id]['last_10_wr'] = wr_high
        log.info(f'[ARBI-LEARN] {pair_id}: min_spread={new_min:.1f}% WR_high={wr_high:.0f}% WR_mid={wr_mid:.0f}% WR_low={wr_low:.0f}%')

    return ARBI_PAIR_CONFIG.get(pair_id, cfg)


def _fetch_arbi_price(ctx, symbol: str) -> float:
    """[v10.4][v10.6-P4] Preço para arbitragem com ADR fallback para legs B3.
    Cadência: Binance (crypto) → Polygon (US + ADR de B3) → brapi (B3) → FMP → Yahoo.
    """
    fx_rates = ctx['fx_rates']
    BRAPI_TOKEN = ctx.get('BRAPI_TOKEN')
    POLYGON_API_KEY = ctx.get('POLYGON_API_KEY')
    FMP_API_KEY = ctx.get('FMP_API_KEY')
    B3_TO_ADR = ctx['B3_TO_ADR']
    STOCK_SYMBOLS_B3 = ctx['STOCK_SYMBOLS_B3']
    CRYPTO_SYMBOLS = ctx['CRYPTO_SYMBOLS']
    _fetch_brapi_stock = ctx['_fetch_brapi_stock']
    _fetch_polygon_stock = ctx['_fetch_polygon_stock']
    log = ctx['log']

    display = symbol.replace('.SA', '')
    is_b3_sym = symbol.endswith('.SA') or display in {s.replace('.SA','') for s in STOCK_SYMBOLS_B3}

    # Binance para crypto
    if symbol.endswith('USDT') or symbol in CRYPTO_SYMBOLS:
        try:
            r = requests.get('https://api.binance.com/api/v3/ticker/price',
                             params={'symbol': symbol}, timeout=5)
            if r.status_code == 200:
                p = float(r.json().get('price', 0))
                if p > 0: return p
        except: pass

    # brapi primário para B3 (snapshot, usa cache)
    if is_b3_sym and BRAPI_TOKEN:
        result, _ = _fetch_brapi_stock(display)
        if result and result.get('price', 0) > 0:
            return result['price']

    # [v10.6-P4] Para B3 sem brapi: tentar ADR via Polygon com conversão USD→BRL
    if is_b3_sym and POLYGON_API_KEY:
        adr_sym = B3_TO_ADR.get(display)
        if adr_sym:
            result, _ = _fetch_polygon_stock(adr_sym)
            if result and result.get('price', 0) > 0:
                usd_brl = fx_rates.get('USDBRL', 5.8)
                return round(result['price'] * usd_brl, 2)

    # Polygon para equity US direto
    if not is_b3_sym and POLYGON_API_KEY:
        result, _ = _fetch_polygon_stock(display)
        if result and result.get('price', 0) > 0:
            return result['price']

    # FMP fallback universal
    if FMP_API_KEY:
        try:
            r = requests.get(
                f'https://financialmodelingprep.com/api/v3/quote/{display}',
                params={'apikey': FMP_API_KEY}, timeout=6)
            if r.status_code == 200:
                d = r.json()
                if d and isinstance(d, list):
                    p = float(d[0].get('price') or 0)
                    if p > 0: return p
        except: pass

    # Yahoo último recurso
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d',
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=6)
        if r.status_code == 200:
            return r.json()['chart']['result'][0]['meta'].get('regularMarketPrice', 0)
    except: pass
    return 0


def arbi_learn_from_closed(ctx, pair_id, entry_spread, pnl, direction):
    """[v10.14] Registra resultado de trade fechada para aprendizado de thresholds."""
    global _arbi_pair_stats
    ARBI_LEARN_MIN_SAMPLES = ctx.get('ARBI_LEARN_MIN_SAMPLES', 5)

    with _arbi_learning_lock:
        if pair_id not in _arbi_pair_stats:
            _arbi_pair_stats[pair_id] = {
                'n': 0, 'pnl': 0.0,
                'spread_buckets': {},  # {bucket: {n, wins, pnl}}
                'low_threshold': None, 'high_threshold': None,
                'no_entry_low': None,  'no_entry_high': None,
                'last_updated': None,
            }
        st = _arbi_pair_stats[pair_id]
        st['n']   += 1
        st['pnl'] += pnl

        # Classificar em buckets de 0.5%
        abs_s = abs(entry_spread)
        bucket = round(abs_s * 2) / 2  # arredonda para 0.5%
        if bucket not in st['spread_buckets']:
            st['spread_buckets'][bucket] = {'n': 0, 'wins': 0, 'pnl': 0.0}
        bk = st['spread_buckets'][bucket]
        bk['n']   += 1
        bk['pnl'] += pnl
        if pnl > 0: bk['wins'] += 1

        # Recalcular thresholds se tiver amostras suficientes
        _recalc_arbi_thresholds(ctx, pair_id, st)


def _recalc_arbi_thresholds(ctx, pair_id, st):
    """[v10.14] Recalcula thresholds ótimos baseado no histórico de cada bucket."""
    log = ctx['log']
    ARBI_LEARN_MIN_SAMPLES = ctx.get('ARBI_LEARN_MIN_SAMPLES', 5)

    buckets = st['spread_buckets']
    if not buckets: return

    # Para cada bucket com >= MIN_SAMPLES, calcular WR e P&L/trade
    bucket_stats = []
    for b, v in sorted(buckets.items()):
        if v['n'] >= ARBI_LEARN_MIN_SAMPLES:
            wr   = v['wins'] / v['n'] * 100
            avg  = v['pnl']  / v['n']
            bucket_stats.append((b, v['n'], wr, avg))

    if len(bucket_stats) < 3: return  # precisa de mais dados

    # Estratégia de aprendizado:
    # Buckets onde LONG_A tem WR < 40% → candidatos para LONG_B (spread baixo aumenta)
    # Buckets onde LONG_A tem WR > 60% → manter LONG_A (spread alto diminui)
    # Zona de transição = no-entry zone

    # Encontrar threshold de forma automática
    bad_buckets  = [b for b, n, wr, avg in bucket_stats if wr < 45]
    good_buckets = [b for b, n, wr, avg in bucket_stats if wr > 60]

    if bad_buckets and good_buckets:
        new_low  = max(bad_buckets)   # limite superior dos buckets ruins (usar LONG_B abaixo disto)
        new_high = min(good_buckets)  # limite inferior dos buckets bons (usar LONG_A acima disto)
        no_low   = new_low
        no_high  = new_high

        # Só atualizar se mudou significativamente (>0.5%)
        if (st['low_threshold'] is None or
            abs(st['low_threshold'] - new_low) >= 0.5 or
            abs(st['high_threshold'] - new_high) >= 0.5):

            old_low  = st['low_threshold']
            old_high = st['high_threshold']
            st['low_threshold']  = new_low
            st['high_threshold'] = new_high
            st['no_entry_low']   = no_low
            st['no_entry_high']  = no_high
            st['last_updated']   = datetime.utcnow().isoformat()

            if old_low is not None:
                log.info(f'[ArbiLearn] {pair_id}: threshold atualizado '
                         f'low {old_low}→{new_low}% high {old_high}→{new_high}% '
                         f'(buckets: {len(bucket_stats)})')
            else:
                log.info(f'[ArbiLearn] {pair_id}: threshold inicial detectado '
                         f'low={new_low}% high={new_high}%')


def arbi_get_smart_direction(ctx, pair, abs_spread):
    """[v10.14] Retorna direção correta para par com smart_direction=True."""
    pair_id = pair.get('id', '')

    # Verificar se temos thresholds aprendidos para este par
    with _arbi_learning_lock:
        learned = _arbi_pair_stats.get(pair_id, {})

    # Usar thresholds aprendidos se disponíveis, senão usar config do par
    low_thr  = learned.get('low_threshold')  or pair.get('spread_low_threshold', 9.0)
    high_thr = learned.get('high_threshold') or pair.get('spread_high_threshold', 10.0)
    no_low   = learned.get('no_entry_low')   or pair.get('no_entry_zone', (9.0, 10.0))[0]
    no_high  = learned.get('no_entry_high')  or pair.get('no_entry_zone', (9.0, 10.0))[1]

    # Zona ambígua → não entrar
    if no_low <= abs_spread <= no_high:
        return None, 'no_entry_zone'

    if abs_spread < low_thr:
        return 'LONG_B', f'smart_low<{low_thr:.1f}%'
    elif abs_spread > high_thr:
        return 'LONG_A', f'smart_high>{high_thr:.1f}%'
    else:
        return None, 'no_entry_zone'


def calc_spread(ctx, pair):
    """Calcula spread entre pares com tratamento de FX e ratios."""
    try:
        fx_rates = ctx['fx_rates']
        log = ctx['log']
        market_open_for = ctx['market_open_for']
        ARBI_MIN_SPREAD = ctx['ARBI_MIN_SPREAD']
        ARBI_MAX_SPREAD = ctx['ARBI_MAX_SPREAD']
        ARBI_POS_SIZE = ctx['ARBI_POS_SIZE']
        _fetch_arbi_price_fn = ctx['_fetch_arbi_price']

        pa_raw=_fetch_arbi_price_fn(pair['leg_a']); pb_raw=_fetch_arbi_price_fn(pair['leg_b'])
        if pa_raw<=0 or pb_raw<=0: return None
        fx=pair['fx']; ra=pair.get('ratio_a',1); rb=pair.get('ratio_b',1)
        if fx=='USDBRL':
            # [v10.14-Audit] Fórmula unificada correta:
            # pa_norm = ação local em USD (sem ratio)
            # pb_norm = ADR × ratio_b (quantidade de ações locais por ADR)
            # spread  = pa_norm / pb_norm - 1
            rate=fx_rates.get('USDBRL',5.8)
            pa = pa_raw/rate            # local (BRL→USD), sem ratio
            pb = pb_raw * rb            # ADR × ratio_b (normaliza para mesmo denominador)
            # Normalizar ambos para preço por ação local
            pa = pa / 1                 # já é por 1 ação local
            pb = pb / ra                # pb / ratio_a = preço por 1 ação local via ADR
        elif fx=='GBPUSD':
            # [v10.14-Audit] pa_norm = NYSE em USD (sem ratio, pois ratio_a=1)
            # pb_norm = LSE em USD × ratio_b (ações LSE por 1 NYSE) / ratio_a
            rate=fx_rates.get('GBPUSD',1.27)
            pa = pa_raw                     # NYSE já em USD
            pb = (pb_raw/100*rate)*rb/ra    # LSE: GBp→£→USD, normalizado pelo ratio
        elif fx=='HKDUSD':
            rate=fx_rates.get('HKDUSD',7.8)
            pa = pa_raw; pb = (pb_raw/rate)*rb/ra
        elif fx=='CADUSD':
            rate=fx_rates.get('CADUSD',0.735)
            pa = pa_raw; pb = (pb_raw*rate)*rb/ra
        elif fx=='EURUSD':
            # [v10.14-Audit] pa_norm = NYSE em USD, pb_norm = EUR→USD × ratio_b / ratio_a
            rate=fx_rates.get('EURUSD',1.085)
            pa = pa_raw                     # NYSE já em USD
            pb = (pb_raw*rate)*rb/ra        # EUR→USD, normalizado pelo ratio
        else:
            pa=pa_raw*ra; pb=pb_raw*rb
        if pb<=0: return None
        # [v10.14-Audit] Spread normalizado por ratio — corrige exibição de ADR
        # spread_norm = (price_a_normalized / price_b_normalized - 1) × 100
        # price_a_norm = pa_raw_usd / ratio_a → preço por unidade econômica
        # price_b_norm = pb_raw_usd / ratio_b → preço por unidade econômica
        # [v10.14-Audit] pa e pb já estão normalizados pela nova fórmula acima
        # pa = preço da leg_a por unidade econômica (1 ação local ou equivalente)
        # pb = preço da leg_b normalizado ao equivalente da leg_a
        pa_norm = pa; pb_norm = pb
        spread_pct = ((pa_norm - pb_norm) / pb_norm) * 100 if pb_norm > 0 else 0
        # [v10.14-SANITY] Rejeitar spreads impossíveis — dado corrompido (99% é impossível)
        if abs(spread_pct) > 30.0:
            log.warning(f'[ARBI-SANITY] {pair["id"]}: spread={spread_pct:.2f}% IMPOSSÍVEL (pa={pa:.4f} pb={pb:.4f}) — ignorado')
            return None

        # [v10.14-SMART] Direção adaptativa por faixa de spread
        # Para pares com smart_direction=True, a direção depende do nível do spread
        abs_sp = abs(spread_pct)
        smart_dir = pair.get('smart_direction', False)
        low_thr   = pair.get('spread_low_threshold', 0)
        high_thr  = pair.get('spread_high_threshold', 999)
        no_entry  = pair.get('no_entry_zone', None)

        if smart_dir and no_entry and no_entry[0] <= abs_sp <= no_entry[1]:
            # Zona ambígua — não gerar sinal
            return None  # calc_spread retorna None = sem oportunidade

        if smart_dir:
            forced_direction, _reason = arbi_get_smart_direction(ctx, pair, abs_sp)
        else:
            forced_direction = None

        now_ts = datetime.utcnow().isoformat()
        # Timestamp do preço (simplificado — usar updated_at do cache)
        price_ts_a = now_ts; price_ts_b = now_ts

        # Bid/Ask estimado — simulação conservadora: spread bid/ask típico por mercado
        # B3: ~0.05% | NYSE: ~0.02% | LSE/EURONEXT: ~0.03%
        def _ba_spread(mkt):
            return {'B3':0.0005,'NYSE':0.0002,'LSE':0.0003,'EURONEXT':0.0003}.get(mkt,0.0003)
        bid_a = round(pa_raw * (1 - _ba_spread(pair['mkt_a'])/2), 4)
        ask_a = round(pa_raw * (1 + _ba_spread(pair['mkt_a'])/2), 4)
        bid_b = round(pb_raw * (1 - _ba_spread(pair['mkt_b'])/2), 4)
        ask_b = round(pb_raw * (1 + _ba_spread(pair['mkt_b'])/2), 4)
        spread_bps_a = round(_ba_spread(pair['mkt_a']) * 10000, 1)
        spread_bps_b = round(_ba_spread(pair['mkt_b']) * 10000, 1)

        # Quantidade estimada (position_size / entry_price em USD)
        _pos = ARBI_POS_SIZE
        qty_a = round(_pos / pa, 0) if pa > 0 else 0
        qty_b = round(_pos / pb, 0) if pb > 0 else 0

        return {'pair_id':pair['id'],'name':pair['name'],'leg_a':pair['leg_a'],'leg_b':pair['leg_b'],
            'mkt_a':pair['mkt_a'],'mkt_b':pair['mkt_b'],
            # Preços raw e normalizados
            'price_a':round(pa_raw,4),'price_b':round(pb_raw,4),
            'price_a_usd':round(pa,4),'price_b_usd':round(pb,4),   # já normalizado por ratio
            'price_a_raw_usd':round(pa_raw if fx=='USDBRL' else pa_raw, 4),
            'price_b_raw_usd':round(pb_raw, 4),
            # Bid/Ask simulado
            'bid_a':bid_a,'ask_a':ask_a,'bid_b':bid_b,'ask_b':ask_b,
            'spread_bps_a':spread_bps_a,'spread_bps_b':spread_bps_b,
            'price_source_a':'last','price_source_b':'last',
            # Timestamps
            'signal_ts_a':price_ts_a,'signal_ts_b':price_ts_b,'delta_ts_ms':0,
            # Spread calculado corretamente
            'spread_pct':round(spread_pct,4),
            'spread_pct_display':round(spread_pct,2),
            'abs_spread':round(abs(spread_pct),2),
            'entry_spread_normalized':round(spread_pct,4),
            # Ratio e FX
            'fx_rate':fx_rates.get(fx,0),'fx_pair':fx,'fx_ts':now_ts,
            'ratio_a':ra,'ratio_b':rb,'ratio_source':'pair_config',
            # Quantidade
            'qty_a_est':int(qty_a),'qty_b_est':int(qty_b),
            # Flags
            'opportunity':(ARBI_MIN_SPREAD<=abs(spread_pct)<=ARBI_MAX_SPREAD) and
                           (not smart_dir or forced_direction is not None),
            'direction': forced_direction if forced_direction else ('LONG_A' if spread_pct<0 else 'LONG_B'),
            'smart_direction': smart_dir,
            'spread_zone': 'LOW' if (smart_dir and abs_sp < low_thr) else ('HIGH' if (smart_dir and abs_sp > high_thr) else 'MID'),
            'markets_open':market_open_for(pair['mkt_a']) and market_open_for(pair['mkt_b']),
            'updated_at':now_ts}
    except Exception as e:
        log.error(f'Spread {pair.get("id", "?")}: {e}');
        return None


def arbi_scan_loop(ctx):
    """Main arbitrage scanning loop."""
    log = ctx['log']
    state_lock = ctx['state_lock']
    beat = ctx['beat']
    fetch_fx_rates = ctx['fetch_fx_rates']
    arbi_spreads = ctx['arbi_spreads']
    arbi_capital = ctx['arbi_capital']
    ARBI_PAIR_CONFIG = ctx['ARBI_PAIR_CONFIG']
    ARBI_MIN_SPREAD = ctx['ARBI_MIN_SPREAD']
    ARBI_MAX_SPREAD = ctx['ARBI_MAX_SPREAD']
    arbi_open = ctx['arbi_open']
    market_open_for = ctx['market_open_for']
    arbi_capital_ref = ctx['arbi_capital_ref']  # mutable reference
    check_risk_arbi = ctx['check_risk_arbi']
    gen_id = ctx['gen_id']
    ARBI_CAPITAL = ctx['ARBI_CAPITAL']
    ARBI_POS_SIZE = ctx['ARBI_POS_SIZE']
    audit = ctx['audit']
    enqueue_persist = ctx['enqueue_persist']
    send_whatsapp = ctx['send_whatsapp']
    ledger_record = ctx['ledger_record']

    while True:
        beat('arbi_scan_loop')
        try:
            fetch_fx_rates()
            for pair in ARBI_PAIRS:
                beat('arbi_scan_loop')
                spread=calc_spread(ctx, pair)
                if not spread:
                    time.sleep(1); continue

                with state_lock: arbi_spreads[pair['id']]=spread

                # [v10.14] Threshold dinâmico por par
                _pair_cfg = ARBI_PAIR_CONFIG.get(pair['id'], ARBI_PAIR_CONFIG['_default'])
                _min_sp   = _pair_cfg['min_spread'] if _pair_cfg['min_spread'] else ARBI_MIN_SPREAD
                _max_sp   = _pair_cfg.get('max_spread', ARBI_MAX_SPREAD)
                spread['opportunity'] = _min_sp <= abs(spread.get('spread_pct',0)) <= _max_sp

                if abs(spread.get('spread_pct',0)) > _max_sp:
                    log.warning(f'[ARBI-SANITY] {pair["id"]} spread {spread["spread_pct"]:+.2f}% acima do teto {ARBI_MAX_SPREAD}% — possível preço inválido, ignorando')
                if not spread['opportunity'] or not spread['markets_open']:
                    time.sleep(1.5); continue

                # [v10.11] Position size dinâmico = portfolio_arbi / 3 (cresce com lucros)
                with state_lock:
                    _arbi_pnl_total = sum(t.get('pnl',0) for t in arbi_open) + sum(t.get('pnl',0) for t in ctx['arbi_closed'])
                    # [v10.14] Portfolio arbi REAL = capital livre + posições abertas + todo P&L
                    # Isso faz os ganhos acumulados participarem das novas posições
                    _committed_arbi = sum(t.get('position_size',0) for t in arbi_open)
                    _arbi_port_val  = max(
                        arbi_capital_ref[0] + _committed_arbi,           # capital livre + comprometido
                        ARBI_CAPITAL + _arbi_pnl_total,           # inicial + pnl total
                        ARBI_CAPITAL)                             # mínimo = capital inicial
                # [v10.14-FIX] Respeitar max_pos por par (protege contra spreads voláteis)
                _pair_max_pos = ARBI_PAIR_CONFIG.get(pair['id'], {}).get('max_pos', None)
                _dynamic_pos = max(round(_arbi_port_val / 3), ARBI_POS_SIZE)
                if _pair_max_pos:
                    _dynamic_pos = min(_dynamic_pos, _pair_max_pos)
                risk_ok,risk_reason,approved_size=check_risk_arbi(pair['id'],_dynamic_pos)
                if not risk_ok:
                    if 'KILL_SWITCH' in risk_reason: break
                    time.sleep(1.5); continue

                bl=pair['leg_a'] if spread['direction']=='LONG_A' else pair['leg_b']
                sl=pair['leg_b'] if spread['direction']=='LONG_A' else pair['leg_a']
                bm=pair['mkt_a'] if spread['direction']=='LONG_A' else pair['mkt_b']
                sm=pair['mkt_b'] if spread['direction']=='LONG_A' else pair['mkt_a']
                trade_id=gen_id('ARB'); opened=False; pos=0

                with state_lock:
                    if any(t['pair_id']==pair['id'] for t in arbi_open): pass
                    elif not (market_open_for(pair['mkt_a']) and market_open_for(pair['mkt_b'])): pass
                    elif approved_size<=0 or arbi_capital_ref[0]<=0: pass
                    else:
                        pos=min(approved_size,arbi_capital_ref[0]); arbi_capital_ref[0]-=pos
                        # [v10.20] Ledger: RESERVE arbi
                        ledger_record('arbi', 'RESERVE', pair['name'], round(pos, 2), arbi_capital_ref[0], trade_id)
                        _entry_ts = datetime.utcnow().isoformat()
                        # [v10.14-Audit] Preço de entrada por lado
                        # LONG_A: compra leg_a (ask) e vende leg_b (bid)
                        _entry_price_a = spread.get('ask_a' if spread['direction']=='LONG_A' else 'bid_a', spread.get('price_a',0))
                        _entry_price_b = spread.get('bid_b' if spread['direction']=='LONG_A' else 'ask_b', spread.get('price_b',0))
                        # Custo de câmbio estimado (B3↔NYSE: 0.10% do volume)
                        _fx_cost = round(pos * 0.001, 2) if pair.get('fx','') == 'USDBRL' else 0
                        # Fee ARBI via BTG Day Trade — emolumentos B3 ~0.010% round trip
                        _fee_b3 = round(pos * 0.0001, 2) if 'B3' in [pair['mkt_a'], pair['mkt_b']] else round(pos * 0.0002, 2)
                        # Slippage estimado (posição / ADV proxy × fator)
                        _slippage_a_bps = round(min(pos / 5e6 * 10, 5), 2)  # estimativa conservadora
                        _slippage_b_bps = round(min(pos / 5e6 * 10, 5), 2)
                        _slippage_cost  = round(pos * (_slippage_a_bps + _slippage_b_bps) / 10000, 2)
                        # [v10.23] Custo de aluguel de ações (stock lending) para a perna short
                        # Taxas anuais típicas: B3 blue chips ~2% a.a., NYSE ADRs ~0.5% a.a.
                        # Custo = (position_size/2) × taxa_anual × (timeout_h / 8760)
                        _lending_rates = {'B3': 0.020, 'NYSE': 0.005, 'LSE': 0.008, 'EURONEXT': 0.008}  # % anual
                        _short_mkt = pair['mkt_b'] if spread['direction']=='LONG_A' else pair['mkt_a']
                        _lending_rate = _lending_rates.get(_short_mkt, 0.010)
                        ARBI_TIMEOUT_H = ctx['ARBI_TIMEOUT_H']
                        _lending_cost = round((pos / 2) * _lending_rate * (ARBI_TIMEOUT_H / 8760), 2)
                        _qty_a = spread.get('qty_a_est', 0)
                        _qty_b = spread.get('qty_b_est', 0)
                        trade={'id':trade_id,'pair_id':pair['id'],'name':pair['name'],
                            'leg_a':pair['leg_a'],'leg_b':pair['leg_b'],
                            'mkt_a':pair['mkt_a'],'mkt_b':pair['mkt_b'],
                            'direction':spread['direction'],'buy_leg':bl,'buy_mkt':bm,
                            'short_leg':sl,'short_mkt':sm,
                            # Spreads — raw e normalizado
                            'entry_spread':spread.get('entry_spread_normalized', spread['spread_pct']),
                            'entry_spread_raw':spread['spread_pct'],
                            'current_spread':spread['spread_pct'],
                            'position_size':round(pos,2),
                            'pnl':0,'pnl_pct':0,'peak_pnl_pct':0,
                            'fx_rate':spread['fx_rate'],'fx_ts':spread.get('fx_ts',_entry_ts),
                            # Timestamps Sprint 1
                            'entry_ts':_entry_ts,
                            'signal_ts_a':spread.get('signal_ts_a',_entry_ts),
                            'signal_ts_b':spread.get('signal_ts_b',_entry_ts),
                            'delta_ts_between_legs_ms':spread.get('delta_ts_ms',0),
                            # Preços por perna
                            'price_a_entry':_entry_price_a,
                            'price_b_entry':_entry_price_b,
                            'price_a_usd_norm':spread.get('price_a_usd',0),
                            'price_b_usd_norm':spread.get('price_b_usd',0),
                            'bid_a':spread.get('bid_a',0),'ask_a':spread.get('ask_a',0),
                            'bid_b':spread.get('bid_b',0),'ask_b':spread.get('ask_b',0),
                            'price_source_a':spread.get('price_source_a','last'),
                            'price_source_b':spread.get('price_source_b','last'),
                            # Quantidade
                            'qty_a':_qty_a,'qty_b':_qty_b,
                            'ratio_a':pair.get('ratio_a',1),'ratio_b':pair.get('ratio_b',1),
                            # Custos detalhados Sprint 1
                            'broker_fee_a':0,'broker_fee_b':0,  # BTG Day Trade ZERO
                            'exchange_fee_a':round(_fee_b3/2,2),'exchange_fee_b':round(_fee_b3/2,2),
                            'fx_cost':_fx_cost,
                            'slippage_cost_a':round(_slippage_cost/2,2),
                            'slippage_cost_b':round(_slippage_cost/2,2),
                            'slippage_bps_total':round(_slippage_a_bps+_slippage_b_bps,2),
                            'lending_cost':_lending_cost,
                            'lending_rate_annual':_lending_rate,
                            'total_cost_estimated':round(_fee_b3 + _fx_cost + _slippage_cost + _lending_cost,2),
                            # Audit flags
                            'audit_flag':'valid',
                            'simulation_model_version':'v2.0',
                            'fee_model_version':'v1.0',
                            'slippage_model_version':'v1.0',
                            'opened_at':_entry_ts,'status':'OPEN','asset_type':'arbitrage'}
                        arbi_open.append(trade); opened=True

                if opened:
                    audit('ARBI_OPENED',{'id':trade_id,'pair':pair['id'],'spread':spread['abs_spread']})
                    enqueue_persist('arbi',trade)
                    send_whatsapp(f"ARBI: {pair['name']} spread {spread['abs_spread']:.2f}% ${pos:,.0f}")

                time.sleep(1.5)
        except Exception as e: log.error(f'arbi_scan: {e}')

        beat('arbi_scan_loop')
        time.sleep(300)
        beat('arbi_scan_loop')


def arbi_monitor_loop(ctx):
    """Monitor open arbitrage trades and close when conditions are met."""
    log = ctx['log']
    state_lock = ctx['state_lock']
    beat = ctx['beat']
    arbi_open = ctx['arbi_open']
    arbi_closed = ctx['arbi_closed']
    arbi_spreads = ctx['arbi_spreads']
    arbi_capital_ref = ctx['arbi_capital_ref']
    ARBI_TP_SPREAD = ctx['ARBI_TP_SPREAD']
    ARBI_SL_PCT = ctx['ARBI_SL_PCT']
    ARBI_TIMEOUT_H = ctx['ARBI_TIMEOUT_H']
    market_open_for = ctx['market_open_for']
    audit = ctx['audit']
    enqueue_persist = ctx['enqueue_persist']
    ledger_record = ctx['ledger_record']
    is_momentum_positive = ctx['is_momentum_positive']
    risk_manager = ctx.get('risk_manager')
    perf_stats = ctx.get('perf_stats')
    market_regime = ctx['market_regime']

    while True:
        beat('arbi_monitor_loop')
        time.sleep(60)
        try:
            closed_trades=[]
            with state_lock:
                now=datetime.utcnow(); to_close=[]
                for trade in arbi_open:
                    age_h=(now-datetime.fromisoformat(trade['opened_at'])).total_seconds()/3600
                    sd=arbi_spreads.get(trade['pair_id'])
                    if sd:
                        trade['current_spread']=sd['spread_pct']
                        ea=abs(float(trade['entry_spread'])); ca=abs(float(trade['current_spread']))
                        trade['pnl_pct']=round(ea-ca,4)
                        # [v10.14-FIX] Sanity check: spread > 20% = preço inválido
                        if abs(float(trade.get('current_spread', 0))) > 20.0:
                            log.warning(f"[ARBI-SANITY] {trade.get('pair_id')} spread={trade.get('current_spread')} INVÁLIDO")
                            trade['current_spread'] = trade.get('entry_spread', 0)
                            trade['pnl_pct'] = 0.0
                            trade['pnl'] = 0.0
                            continue
                    trade['pnl']=round(trade['pnl_pct']/100*float(trade['position_size']),2)
                    trade['peak_pnl_pct']=round(max(trade.get('peak_pnl_pct',0),trade['pnl_pct']),2)
                    peak=trade['peak_pnl_pct']
                    h=trade.setdefault('pnl_history',[]); h.append(trade['pnl_pct'])
                    if len(h)>5: h.pop(0)
                    reason=None
                    mkt_a=trade.get('mkt_a',''); mkt_b=trade.get('mkt_b','')
                    both_open=(market_open_for(mkt_a) and market_open_for(mkt_b))
                    if abs(trade.get('current_spread',99))<=ARBI_TP_SPREAD:  reason='TAKE_PROFIT'
                    elif peak>=2.0 and trade['pnl_pct']<=peak-1.0:           reason='TRAILING_STOP'
                    elif trade['pnl_pct']<=-ARBI_SL_PCT:                     reason='STOP_LOSS'
                    elif not both_open and age_h>=0.5:                       reason='MARKET_CLOSE'
                    elif age_h>=ARBI_TIMEOUT_H:
                        ext=trade.get('extensions',0)
                        if is_momentum_positive(trade) and ext<3: trade['extensions']=ext+1
                        else: reason='TIMEOUT'
                    if reason:
                        # [v10.20] Ledger: RELEASE + PNL_CREDIT arbi (ordem contábil correta)
                        arbi_capital_ref[0] += trade['position_size']
                        ledger_record('arbi', 'RELEASE', trade.get('name', trade.get('pair_id', '')),
                                      trade['position_size'], arbi_capital_ref[0], trade['id'])
                        arbi_capital_ref[0] += trade['pnl']
                        if trade['pnl'] != 0:
                            ledger_record('arbi', 'PNL_CREDIT', trade.get('name', trade.get('pair_id', '')),
                                          trade['pnl'], arbi_capital_ref[0], trade['id'])
                        # [v10.22] Record to institutional modules
                        if risk_manager:
                            risk_manager.record_trade_result('arbi', trade.get('pair_id', ''), trade['pnl'], trade['position_size'], arbi_capital_ref[0])
                        if perf_stats:
                            perf_stats.record_trade({
                                'strategy': 'arbi', 'symbol': trade.get('pair_id', ''),
                                'pnl': trade['pnl'], 'pnl_pct': trade.get('pnl_pct', 0),
                                'entry_price': trade.get('leg1_entry', 0), 'exit_price': trade.get('leg1_exit', 0),
                                'opened_at': trade.get('opened_at', now.isoformat()), 'closed_at': now.isoformat(),
                                'confidence': 0, 'exit_type': reason, 'asset_type': 'arbi',
                                'regime': market_regime.get('mode', 'UNKNOWN'),
                            })
                        c=dict(trade); c.update({'closed_at':now.isoformat(),'close_reason':reason,'status':'CLOSED'})
                        arbi_closed.insert(0,c)
                        # [v10.9] Sem limite em memória — histórico completo
                        to_close.append(trade['id']); closed_trades.append(c)
                arbi_open[:] = [t for t in arbi_open if t['id'] not in to_close]

            for c in closed_trades:
                audit('ARBI_CLOSED',{'id':c['id'],'pair':c['pair_id'],'pnl':c['pnl'],'reason':c['close_reason']})
                # [v10.14] Aprendizado por par — ajusta threshold após cada fechamento
                _pair_recent = [t for t in list(arbi_closed)[:20] if t.get('pair_id')==c['pair_id']]
                if len(_pair_recent) >= 3:
                    _arbi_pair_learning(ctx, c['pair_id'], _pair_recent)
                enqueue_persist('arbi',c)
                # [v10.14] Alimentar o sistema de aprendizado de thresholds
                try:
                    arbi_learn_from_closed(
                        ctx,
                        pair_id=c.get('pair_id',''),
                        entry_spread=float(c.get('entry_spread',0)),
                        pnl=float(c.get('pnl',0)),
                        direction=c.get('direction','LONG_A')
                    )
                except Exception as _le: log.warning(f'arbi_learn: {_le}')
        except Exception as e: log.error(f'arbi_monitor: {e}')


def arbi_learning_loop(ctx):
    """[v10.14] Loop de aprendizado de arbi — roda a cada 30 minutos."""
    beat = ctx['beat']
    log = ctx['log']

    time.sleep(120)  # aguardar startup
    beat('arbi_learning_loop')
    run_arbi_pattern_learning(ctx)
    while True:
        beat('arbi_learning_loop')
        time.sleep(5 * 60)   # [v10.14] a cada 5 minutos — detecta padrões rápido
        beat('arbi_learning_loop')
        try:
            run_arbi_pattern_learning(ctx)
        except Exception as e:
            log.error(f'arbi_learning_loop: {e}')


def build_arbitrage_ctx(g):
    """
    Build context dict from globals for arbitrage module.
    Pass the result to all arbitrage functions via ctx parameter.
    """
    return {
        # Logging and utilities
        'log': g.get('log'),
        'beat': g.get('beat'),
        'audit': g.get('audit'),
        'gen_id': g.get('gen_id'),
        'send_whatsapp': g.get('send_whatsapp'),
        'enqueue_persist': g.get('enqueue_persist'),
        'ledger_record': g.get('ledger_record'),

        # Database
        'get_db': g.get('get_db'),

        # State management
        'state_lock': g.get('state_lock'),
        'arbi_open': g.get('arbi_open'),
        'arbi_closed': g.get('arbi_closed'),
        'arbi_spreads': g.get('arbi_spreads'),
        'arbi_capital_ref': g.get('arbi_capital_ref', [0]),  # mutable list
        'market_regime': g.get('market_regime'),

        # Market data
        'stock_prices': g.get('stock_prices'),
        'crypto_prices': g.get('crypto_prices'),
        'fx_rates': g.get('fx_rates'),

        # Functions
        'market_open_for': g.get('market_open_for'),
        'fetch_fx_rates': g.get('fetch_fx_rates'),
        'check_risk_arbi': g.get('check_risk_arbi'),
        'is_momentum_positive': g.get('is_momentum_positive'),
        '_fetch_single_stock': g.get('_fetch_single_stock'),
        '_fetch_binance_ticker': g.get('_fetch_binance_ticker'),
        '_fetch_brapi_stock': g.get('_fetch_brapi_stock'),
        '_fetch_polygon_stock': g.get('_fetch_polygon_stock'),
        '_fetch_arbi_price': lambda sym: _fetch_arbi_price(g, sym),

        # Symbols and mappings
        'STOCK_SYMBOLS_B3': g.get('STOCK_SYMBOLS_B3', set()),
        'CRYPTO_SYMBOLS': g.get('CRYPTO_SYMBOLS', set()),
        'B3_TO_ADR': g.get('B3_TO_ADR', {}),

        # Constants
        'ARBI_MIN_SPREAD': g.get('ARBI_MIN_SPREAD', 2.0),
        'ARBI_MAX_SPREAD': g.get('ARBI_MAX_SPREAD', 15.0),
        'ARBI_TP_SPREAD': g.get('ARBI_TP_SPREAD', 0.5),
        'ARBI_SL_PCT': g.get('ARBI_SL_PCT', 2.0),
        'ARBI_POS_SIZE': g.get('ARBI_POS_SIZE', 10000),
        'ARBI_CAPITAL': g.get('ARBI_CAPITAL', 100000),
        'ARBI_TIMEOUT_H': g.get('ARBI_TIMEOUT_H', 24),
        'ARBI_PAIR_CONFIG': g.get('ARBI_PAIR_CONFIG', {'_default': {}}),
        'ARBI_LEARN_MIN_SAMPLES': g.get('ARBI_LEARN_MIN_SAMPLES', 5),

        # API Keys
        'BRAPI_TOKEN': g.get('BRAPI_TOKEN'),
        'POLYGON_API_KEY': g.get('POLYGON_API_KEY'),
        'FMP_API_KEY': g.get('FMP_API_KEY'),

        # Institutional modules
        'risk_manager': g.get('risk_manager'),
        'perf_stats': g.get('perf_stats'),
    }
