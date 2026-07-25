# -*- coding: utf-8 -*-
"""[25-jul-2026, decisao Beto — "limonada da Arbi" v2, pos-revisao GPT+Grok] LONG-LEG HARVEST.

A Arbi de 2 pernas era fantasma, mas a INFORMACAO de direcao do spread e real.
Revisao GPT+Grok apontou 2 furos que esta v2 corrige:
  1. Fechar a perna com a Arbi herda a saida do spread (logica fantasma).
     -> book DIRECTIONAL_EXIT: saida PROPRIA (stop 1.5 ATR, trailing 1 ATR,
        timeout 90min), simulada nos candles intraday REAIS da perna no fecho.
  2. Perna unica perde o hedge (beta/gap/cambio/noticia).
     -> book LIQUID_LEG_HEDGED: perna liquida - beta*indice (IBOV/SPY).

Todos os books sao registros VIRTUAIS do MESMO sinal (nada toca producao).
Guarda os retornos CRUS (long, short, indice, directional) por trade; cada
"book" e uma view derivada. Congelado: LONGLEG_FILTER_VERSION=2026-07-25.

  ALL              long da perna barata em TODA Arbi (rede aberta, benchmark)
  FILTERED_6       6 pares (WR>=60% backtest) + |spread| 0.8-2.0%
  FILTERED_10      10 pares (P&L>0) + PETR4-PBR.A + janela
  SHORT_ALL        short da perna cara (mede a outra metade da intuicao)
  SMART_LEG        long/short conforme Market Pulse confirma; senao NO_TRADE
  LIQUID_LEG_HEDGED  perna liquida - beta*indice (neutraliza mercado)
  DIRECTIONAL_EXIT saida propria (stop/trailing/timeout) na perna long
"""
import os, json, logging, urllib.request
from datetime import datetime, timezone

import pymysql

log = logging.getLogger('egreja.longleg')

FILTER_VERSION = '2026-07-25'
GOOD6 = {'SBSP3-SBS', 'GGBR4-GGB', 'SAP-SAP.DE', 'ASML-ASML.AS', 'ITUB4-ITUB', 'UGPA3-UGP'}
GOOD10 = GOOD6 | {'CSNA3-SID', 'CMIG4-CIG', 'PETR4-PBR', 'PETR4-PBR.A'}
SPREAD_LO, SPREAD_HI = 0.8, 2.0
# saida direcional (congelada)
STOP_ATR = 1.5
TRAIL_ACT_ATR = 1.0
TIMEOUT_MIN = 90
HEDGE_BETA = 1.0   # v1 fixo; refinar por par depois


def _conn():
    return pymysql.connect(
        host=os.environ['MYSQLHOST'], user=os.environ['MYSQLUSER'],
        password=os.environ['MYSQLPASSWORD'], database=os.environ['MYSQLDATABASE'],
        port=int(os.environ.get('MYSQLPORT', 3306)), autocommit=True)


_ready = {'v': False}


def create_tables():
    if _ready['v']:
        return
    c = _conn(); cur = c.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS longleg_harvest (
        id BIGINT AUTO_INCREMENT PRIMARY KEY, arbi_id VARCHAR(40) UNIQUE,
        pair VARCHAR(16), long_leg VARCHAR(20), long_mkt VARCHAR(10),
        short_leg VARCHAR(20), short_mkt VARCHAR(10), direction VARCHAR(8),
        entry_spread_abs DECIMAL(8,4), spread_band VARCHAR(12),
        pulse_at_open VARCHAR(10), notional_usd DECIMAL(14,2),
        in_f6 TINYINT, in_f10 TINYINT,
        long_px_entry DECIMAL(18,6), short_px_entry DECIMAL(18,6), fx_entry DECIMAL(12,6),
        status VARCHAR(8) DEFAULT 'OPEN', opened_at DATETIME, closed_at DATETIME,
        long_ret_pct DECIMAL(10,4), short_ret_pct DECIMAL(10,4),
        index_ret_pct DECIMAL(10,4), hedged_ret_pct DECIMAL(10,4),
        dir_exit_ret_pct DECIMAL(10,4), dir_exit_reason VARCHAR(16),
        mfe_pct DECIMAL(10,4), mae_pct DECIMAL(10,4),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX ix_pair (pair), INDEX ix_status (status), INDEX ix_band (spread_band)
        ) CHARACTER SET utf8mb4""")
    c.close(); _ready['v'] = True


def _band(a):
    if a < 0.5: return 'a<0.5'
    if a < 0.8: return 'b0.5-0.8'
    if a < 1.2: return 'c0.8-1.2'
    if a < 2.0: return 'd1.2-2.0'
    if a < 3.0: return 'e2.0-3.0'
    return 'f>3.0'


def _usd(p, fx, mkt):
    p = float(p or 0); fx = float(fx or 1)
    if p <= 0: return None
    if mkt == 'B3': return p / fx if fx else None
    if mkt == 'NYSE': return p
    return p * fx


def _ysym(leg, mkt):
    leg = str(leg or '')
    if mkt == 'B3': return (leg if leg.endswith('.SA') else leg + '.SA')
    return leg  # NYSE/LSE/EUR ja vem com sufixo quando aplicavel


def _idx_sym(mkt):
    return '^BVSP' if mkt == 'B3' else 'SPY'


def _yahoo_bars(sym, t0, t1):
    """barras 5m [(ts,o,h,l,c), ...] entre t0 e t1 (epoch). Fail-open -> []."""
    try:
        url = (f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}'
               f'?period1={int(t0)-3600}&period2={int(t1)+300}&interval=5m')
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=8) as r:
            d = json.load(r)
        res = d['chart']['result'][0]; ts = res['timestamp']; q = res['indicators']['quote'][0]
        out = []
        for i, t in enumerate(ts):
            if q['close'][i] is None: continue
            if t0 <= t <= t1 + 300:
                out.append((t, q['open'][i], q['high'][i], q['low'][i], q['close'][i]))
        return out
    except Exception as e:
        log.debug(f'[LONGLEG] bars {sym}: {e}')
        return []


def _ret_between(sym, t0, t1):
    """retorno % de sym entre t0 e t1 (usa close das barras 5m)."""
    b = _yahoo_bars(sym, t0, t1)
    if len(b) < 2: return None
    return (b[-1][4] / b[0][4] - 1.0) * 100


def _simulate_directional(bars, entry_px):
    """Simula saida propria LONG na perna (stop 1.5ATR, trailing 1ATR, timeout 90min).
    Retorna (ret_pct, reason, mfe_pct, mae_pct)."""
    if len(bars) < 3 or not entry_px:
        return None, 'NO_BARS', None, None
    # ATR simples nas barras
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i][2], bars[i][3], bars[i-1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = (sum(trs) / len(trs)) if trs else entry_px * 0.005
    if atr <= 0: atr = entry_px * 0.005
    stop = entry_px - STOP_ATR * atr
    peak = entry_px; trailing = False
    mfe = mae = 0.0
    t0 = bars[0][0]
    for (t, o, h, l, cl) in bars:
        mfe = max(mfe, (h / entry_px - 1) * 100)
        mae = min(mae, (l / entry_px - 1) * 100)
        # stop hit intrabar?
        if l <= stop:
            return (stop / entry_px - 1) * 100, ('TRAIL_STOP' if trailing else 'STOP'), round(mfe, 4), round(mae, 4)
        # ativa trailing apos +1 ATR
        if h >= entry_px + TRAIL_ACT_ATR * atr:
            trailing = True
        if trailing:
            peak = max(peak, h)
            stop = max(stop, peak - STOP_ATR * atr)
        # timeout
        if (t - t0) / 60 >= TIMEOUT_MIN:
            return (cl / entry_px - 1) * 100, 'TIMEOUT', round(mfe, 4), round(mae, 4)
    # fim das barras (fechou junto com a Arbi)
    return (bars[-1][4] / entry_px - 1) * 100, 'WINDOW_END', round(mfe, 4), round(mae, 4)


def on_arbi_open(trade, pulse=None):
    """Snapshot na abertura. pulse: 'RISK_ON'|'RISK_OFF'|'NEUTRAL' se disponivel."""
    try:
        create_tables()
        pair = trade.get('pair_id') or trade.get('id')
        direction = str(trade.get('direction', 'LONG_A')).upper()
        if direction == 'LONG_A':
            ll, lm, lpx = trade.get('leg_a'), trade.get('mkt_a'), trade.get('price_a_entry')
            sl, sm, spx = trade.get('leg_b'), trade.get('mkt_b'), trade.get('price_b_entry')
        else:
            ll, lm, lpx = trade.get('leg_b'), trade.get('mkt_b'), trade.get('price_b_entry')
            sl, sm, spx = trade.get('leg_a'), trade.get('mkt_a'), trade.get('price_a_entry')
        abss = abs(float(trade.get('entry_spread') or trade.get('entry_spread_raw') or 0))
        fx = trade.get('fx_a_entry') or trade.get('fx_rate_entry') or trade.get('fx_rate') or 1
        notional = float(trade.get('position_size') or 0)
        inw = SPREAD_LO <= abss <= SPREAD_HI
        c = _conn(); cur = c.cursor()
        cur.execute("""INSERT IGNORE INTO longleg_harvest (arbi_id,pair,long_leg,long_mkt,
            short_leg,short_mkt,direction,entry_spread_abs,spread_band,pulse_at_open,
            notional_usd,in_f6,in_f10,long_px_entry,short_px_entry,fx_entry,status,opened_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s)""",
            (trade.get('id'), pair, ll, lm, sl, sm, direction, round(abss, 4), _band(abss),
             str(pulse or 'NA'), notional, 1 if (pair in GOOD6 and inw) else 0,
             1 if (pair in GOOD10 and inw) else 0, lpx, spx, fx,
             datetime.now(timezone.utc).replace(tzinfo=None)))
        c.close()
    except Exception as e:
        log.debug(f'[LONGLEG] open: {e}')


def on_arbi_close(trade):
    """No fecho: retornos crus (long/short/indice) + saida direcional simulada."""
    try:
        create_tables()
        c = _conn(); cur = c.cursor()
        cur.execute("""SELECT long_leg,long_mkt,short_leg,short_mkt,direction,
            long_px_entry,short_px_entry,fx_entry,opened_at FROM longleg_harvest
            WHERE arbi_id=%s AND status='OPEN'""", (trade.get('id'),))
        row = cur.fetchone()
        if not row:
            c.close(); return
        ll, lm, sl, sm, direction, lpe, spe, fxe, oat = row
        # precos de saida (do dict da Arbi)
        if direction == 'LONG_A':
            lpx1, spx1 = trade.get('price_a_exit'), trade.get('price_b_exit')
        else:
            lpx1, spx1 = trade.get('price_b_exit'), trade.get('price_a_exit')
        fx1 = trade.get('fx_a_exit') or trade.get('fx_rate_exit') or trade.get('fx_rate') or fxe
        # retorno da perna long (USD-normalizado, como no backtest)
        u0 = _usd(lpe, fxe, lm); u1 = _usd(lpx1, fx1, lm)
        long_ret = (u1 / u0 - 1) * 100 if (u0 and u1) else None
        # retorno da perna short (USD-normalizado)
        su0 = _usd(spe, fxe, sm); su1 = _usd(spx1, fx1, sm)
        short_ret = (su1 / su0 - 1) * 100 if (su0 and su1) else None
        # janela de tempo
        try:
            t0 = int(oat.replace(tzinfo=timezone.utc).timestamp())
            t1 = int(datetime.fromisoformat(str(trade.get('closed_at'))[:19]).replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            t0 = t1 = None
        # indice do mercado da perna long (hedge, retorno LOCAL)
        idx_ret = None; hedged = None
        if t0 and t1 and t1 > t0:
            idx_ret = _ret_between(_idx_sym(lm), t0, t1)
            # retorno LOCAL da perna (sem FX) p/ hedge same-market
            loc = (float(lpx1) / float(lpe) - 1) * 100 if lpe and lpx1 else None
            if loc is not None and idx_ret is not None:
                hedged = loc - HEDGE_BETA * idx_ret
        # saida direcional simulada nos candles reais da perna (preco LOCAL)
        dir_ret = dir_reason = mfe = mae = None
        if t0 and t1:
            bars = _yahoo_bars(_ysym(ll, lm), t0, t1 + TIMEOUT_MIN * 60)
            dir_ret, dir_reason, mfe, mae = _simulate_directional(bars, float(lpe) if lpe else None)
        cur.execute("""UPDATE longleg_harvest SET status='CLOSED', closed_at=%s,
            long_ret_pct=%s, short_ret_pct=%s, index_ret_pct=%s, hedged_ret_pct=%s,
            dir_exit_ret_pct=%s, dir_exit_reason=%s, mfe_pct=%s, mae_pct=%s
            WHERE arbi_id=%s AND status='OPEN'""",
            (datetime.now(timezone.utc).replace(tzinfo=None),
             None if long_ret is None else round(long_ret, 4),
             None if short_ret is None else round(short_ret, 4),
             None if idx_ret is None else round(idx_ret, 4),
             None if hedged is None else round(hedged, 4),
             None if dir_ret is None else round(dir_ret, 4), dir_reason, mfe, mae,
             trade.get('id')))
        c.close()
    except Exception as e:
        log.debug(f'[LONGLEG] close: {e}')


def summary():
    create_tables()
    c = _conn(); cur = c.cursor(pymysql.cursors.DictCursor)
    cur.execute("SELECT * FROM longleg_harvest WHERE status='CLOSED'")
    rows = cur.fetchall(); c.close()

    def stat(vals):
        vals = [v for v in vals if v is not None]
        n = len(vals)
        if not n: return {'n': 0}
        wins = sum(1 for v in vals if v > 0)
        return {'n': n, 'wr': round(100 * wins / n, 1),
                'ret_med': round(sum(vals) / n, 3),
                'total_ret': round(sum(vals), 2)}
    books = {}
    books['ALL'] = stat([float(r['long_ret_pct']) for r in rows if r['long_ret_pct'] is not None])
    books['FILTERED_6'] = stat([float(r['long_ret_pct']) for r in rows if r['in_f6'] and r['long_ret_pct'] is not None])
    books['FILTERED_10'] = stat([float(r['long_ret_pct']) for r in rows if r['in_f10'] and r['long_ret_pct'] is not None])
    books['SHORT_ALL'] = stat([-float(r['short_ret_pct']) for r in rows if r['short_ret_pct'] is not None])
    # SMART: long se pulse RISK_ON; short se RISK_OFF; senao nao entra
    smart = []
    for r in rows:
        p = str(r.get('pulse_at_open') or '')
        if p == 'RISK_ON' and r['long_ret_pct'] is not None: smart.append(float(r['long_ret_pct']))
        elif p == 'RISK_OFF' and r['short_ret_pct'] is not None: smart.append(-float(r['short_ret_pct']))
    books['SMART_LEG'] = stat(smart)
    books['LIQUID_LEG_HEDGED'] = stat([float(r['hedged_ret_pct']) for r in rows if r['hedged_ret_pct'] is not None])
    books['DIRECTIONAL_EXIT'] = stat([float(r['dir_exit_ret_pct']) for r in rows if r['dir_exit_ret_pct'] is not None])

    # por par (long)
    bypair = {}
    for r in rows:
        if r['long_ret_pct'] is None: continue
        d = bypair.setdefault(r['pair'], [])
        d.append(float(r['long_ret_pct']))
    por_par = [{'pair': k, **stat(v)} for k, v in sorted(bypair.items(), key=lambda x: -sum(x[1]))]
    # por faixa de spread (long)
    byband = {}
    for r in rows:
        if r['long_ret_pct'] is None: continue
        byband.setdefault(r['spread_band'], []).append(float(r['long_ret_pct']))
    por_faixa = {k: stat(v) for k, v in sorted(byband.items())}
    return {'version': FILTER_VERSION, 'books': books, 'por_par': por_par,
            'por_faixa_spread': por_faixa,
            'total_registrados': len(rows)}
