# -*- coding: utf-8 -*-
"""[25-jul-2026, decisao Beto — "limonada da Arbi" v3, pos-revisao 3x GPT+Grok] LONG-LEG HARVEST.

O spread da Arbi (2 pernas fantasma) pode conter INFORMACAO direcional real.
7 books SHADOW derivados do MESMO sinal (parent = arbi_id), nada toca producao.

v3 corrige os 4 bugs de correcao que GPT+Grok apontaram:
  1. DIRECTIONAL_EXIT DEFERIDO — no fecho da Arbi os candles dos 90min ainda nao
     existem. Marca PENDING; finalize_directional() simula depois que a janela
     de 90min ja passou (bars reais disponiveis). Independente do fecho da Arbi.
  2. ATR CONGELADO EX-ANTE — calculado so com barras ANTES da entrada (sem look-ahead).
  3. BETA DO HEDGE EX-ANTE — 60 pregoes ate a entrada, congelado (nao 1.0 fixo).
  4. AMBIGUIDADE NO CANDLE — politica conservadora congelada: se stop e trailing
     no mesmo candle, STOP primeiro. Flag exit_ambiguous.

Books:
  ALL / FILTERED_6 / FILTERED_10  — perna long (barata)
  SHORT_ALL (teorico)             — perna cara (short); executabilidade a modelar
  SMART_LEG                       — long/short conforme Market Pulse (snapshot na entrada)
  LIQUID_LEG_HEDGED               — perna liquida - beta_exante*indice (IBOV/SPY)
  DIRECTIONAL_EXIT                — saida propria (stop 1.5ATR / trailing 1ATR / timeout 90min)

Congelado: LONGLEG_FILTER_VERSION=2026-07-25. Nao retocar no meio do teste.
"""
import os, json, logging, urllib.request, statistics
from datetime import datetime, timezone

import pymysql

log = logging.getLogger('egreja.longleg')

FILTER_VERSION = '2026-07-25'
GOOD6 = {'SBSP3-SBS', 'GGBR4-GGB', 'SAP-SAP.DE', 'ASML-ASML.AS', 'ITUB4-ITUB', 'UGPA3-UGP'}
GOOD10 = GOOD6 | {'CSNA3-SID', 'CMIG4-CIG', 'PETR4-PBR', 'PETR4-PBR.A'}
SPREAD_LO, SPREAD_HI = 0.8, 2.0
STOP_ATR = 1.5
TRAIL_ACT_ATR = 1.0
TIMEOUT_MIN = 90
BETA_WINDOW_D = 60


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
        opened_epoch BIGINT,
        long_ret_pct DECIMAL(10,4), short_ret_pct DECIMAL(10,4),
        index_ret_pct DECIMAL(10,4), hedged_ret_pct DECIMAL(10,4),
        hedge_beta DECIMAL(8,4), hedged_beta_ret_pct DECIMAL(10,4),
        dir_status VARCHAR(8) DEFAULT 'PENDING',
        dir_exit_ret_pct DECIMAL(10,4), dir_exit_reason VARCHAR(16),
        exit_ambiguous TINYINT DEFAULT 0, atr_pct_entry DECIMAL(10,4),
        matched_ctrl_ret_pct DECIMAL(10,4), matched_ctrl_n INT DEFAULT 0,
        proc_claimed_at DATETIME NULL,
        mfe_pct DECIMAL(10,4), mae_pct DECIMAL(10,4),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX ix_pair (pair), INDEX ix_status (status),
        INDEX ix_dir (dir_status), INDEX ix_band (spread_band)) CHARACTER SET utf8mb4""")
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
    return (leg if leg.endswith('.SA') else leg + '.SA') if mkt == 'B3' else leg


def _idx_sym(mkt):
    return '^BVSP' if mkt == 'B3' else 'SPY'


def _yahoo_bars(sym, t0, t1, interval='5m'):
    try:
        url = (f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}'
               f'?period1={int(t0)}&period2={int(t1)}&interval={interval}')
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=8) as r:
            d = json.load(r)
        res = d['chart']['result'][0]; ts = res['timestamp']; q = res['indicators']['quote'][0]
        return [(t, q['open'][i], q['high'][i], q['low'][i], q['close'][i])
                for i, t in enumerate(ts) if q['close'][i] is not None]
    except Exception as e:
        log.debug(f'[LONGLEG] bars {sym}: {e}')
        return []


def _daily(sym, t0, t1):
    b = _yahoo_bars(sym, t0, t1, '1d')
    return [(t, cl) for (t, o, h, l, cl) in b]


def _ret_between(sym, t0, t1):
    b = _yahoo_bars(sym, t0 - 600, t1 + 300)
    b = [x for x in b if t0 <= x[0] <= t1 + 300]
    if len(b) < 2: return None
    return (b[-1][4] / b[0][4] - 1.0) * 100


def _ex_ante_beta(leg_sym, idx_sym, entry_epoch):
    """Beta de 60 pregoes ATE (exclusive) a data de entrada. Frozen. Fallback 1.0."""
    try:
        t1 = entry_epoch - 86400  # ate o dia anterior (ex-ante)
        t0 = t1 - BETA_WINDOW_D * 2 * 86400
        dl = dict(_daily(leg_sym, t0, t1)); di = dict(_daily(idx_sym, t0, t1))
        days = sorted(set(dl) & set(di))[-BETA_WINDOW_D:]
        if len(days) < 20: return 1.0
        rl = [dl[days[i]] / dl[days[i-1]] - 1 for i in range(1, len(days))]
        ri = [di[days[i]] / di[days[i-1]] - 1 for i in range(1, len(days))]
        vi = sum(x*x for x in ri)
        if vi <= 0: return 1.0
        cov = sum(a*b for a, b in zip(rl, ri))
        beta = cov / vi
        return max(0.0, min(2.5, beta))  # sanidade
    except Exception:
        return 1.0


def _simulate_directional(pre_bars, win_bars, entry_px, session_end_epoch=None):
    """ATR ex-ante (pre_bars, ANTES da entrada). Simula LONG na janela (win_bars).
    [v4 GPT] Gap atraves do stop: se o candle ABRE cruzado, sai no open (conservador).
    [v4 GPT] Timeout perto do fechamento: encerra no ultimo preco antes da sessao
    fechar (MARKET_CLOSE). Ambiguidade intrabar: stop primeiro (conservador)+flag.
    Retorna (ret,reason,mfe,mae,atr_pct,ambig)."""
    if not entry_px or len(win_bars) < 2:
        return None, 'NO_BARS', None, None, None, 0
    trs = []
    for i in range(1, len(pre_bars)):
        h, l, pc = pre_bars[i][2], pre_bars[i][3], pre_bars[i-1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = statistics.mean(trs) if trs else entry_px * 0.005
    if atr <= 0: atr = entry_px * 0.005
    atr_pct = atr / entry_px * 100
    stop = entry_px - STOP_ATR * atr
    peak = entry_px; trailing = False; mfe = mae = 0.0; ambig = 0
    t0 = win_bars[0][0]
    for (t, o, h, l, cl) in win_bars:
        # [v4] fechamento de sessao: encerra no ultimo executavel
        if session_end_epoch and t >= session_end_epoch:
            return (o / entry_px - 1) * 100, 'MARKET_CLOSE', round(mfe, 4), round(mae, 4), round(atr_pct, 4), ambig
        mfe = max(mfe, (h / entry_px - 1) * 100)
        mae = min(mae, (l / entry_px - 1) * 100)
        # [v4] GAP atraves do stop: candle ABRE ja abaixo do stop -> sai no open (pior)
        if o <= stop:
            return (o / entry_px - 1) * 100, ('STOP_GAP'), round(mfe, 4), round(mae, 4), round(atr_pct, 4), ambig
        hit_stop = l <= stop
        act_trail = h >= entry_px + TRAIL_ACT_ATR * atr
        if hit_stop and act_trail:
            ambig = 1  # ambiguo -> STOP primeiro (conservador)
        if hit_stop:
            return (stop / entry_px - 1) * 100, ('TRAIL_STOP' if trailing else 'STOP'), \
                   round(mfe, 4), round(mae, 4), round(atr_pct, 4), ambig
        if act_trail:
            trailing = True
        if trailing:
            peak = max(peak, h)
            stop = max(stop, peak - STOP_ATR * atr)
        if (t - t0) / 60 >= TIMEOUT_MIN:
            return (cl / entry_px - 1) * 100, 'TIMEOUT', round(mfe, 4), round(mae, 4), round(atr_pct, 4), ambig
    return (win_bars[-1][4] / entry_px - 1) * 100, 'WINDOW_END', round(mfe, 4), round(mae, 4), round(atr_pct, 4), ambig


def on_arbi_open(trade, pulse=None):
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
        now = datetime.now(timezone.utc)
        c = _conn(); cur = c.cursor()
        cur.execute("""INSERT IGNORE INTO longleg_harvest (arbi_id,pair,long_leg,long_mkt,
            short_leg,short_mkt,direction,entry_spread_abs,spread_band,pulse_at_open,
            notional_usd,in_f6,in_f10,long_px_entry,short_px_entry,fx_entry,status,opened_at,opened_epoch)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s,%s)""",
            (trade.get('id'), pair, ll, lm, sl, sm, direction, round(abss, 4), _band(abss),
             str(pulse or 'NA'), notional, 1 if (pair in GOOD6 and inw) else 0,
             1 if (pair in GOOD10 and inw) else 0, lpx, spx, fx,
             now.replace(tzinfo=None), int(now.timestamp())))
        c.close()
    except Exception as e:
        log.debug(f'[LONGLEG] open: {e}')


def on_arbi_close(trade):
    """No fecho: retornos crus (long/short/indice/hedge beta=1). Directional fica PENDING."""
    try:
        create_tables()
        c = _conn(); cur = c.cursor()
        cur.execute("""SELECT long_mkt,short_mkt,direction,long_px_entry,short_px_entry,
            fx_entry,opened_epoch FROM longleg_harvest WHERE arbi_id=%s AND status='OPEN'""",
            (trade.get('id'),))
        row = cur.fetchone()
        if not row:
            c.close(); return
        lm, sm, direction, lpe, spe, fxe, oepoch = row
        if direction == 'LONG_A':
            lpx1, spx1 = trade.get('price_a_exit'), trade.get('price_b_exit')
        else:
            lpx1, spx1 = trade.get('price_b_exit'), trade.get('price_a_exit')
        fx1 = trade.get('fx_a_exit') or trade.get('fx_rate_exit') or trade.get('fx_rate') or fxe
        u0 = _usd(lpe, fxe, lm); u1 = _usd(lpx1, fx1, lm)
        long_ret = (u1 / u0 - 1) * 100 if (u0 and u1) else None
        su0 = _usd(spe, fxe, sm); su1 = _usd(spx1, fx1, sm)
        short_ret = (su1 / su0 - 1) * 100 if (su0 and su1) else None
        try:
            t1 = int(datetime.fromisoformat(str(trade.get('closed_at'))[:19]).replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            t1 = None
        idx_ret = hedged = None
        if oepoch and t1 and t1 > oepoch:
            idx_ret = _ret_between(_idx_sym(lm), oepoch, t1)
            loc = (float(lpx1) / float(lpe) - 1) * 100 if lpe and lpx1 else None
            if loc is not None and idx_ret is not None:
                hedged = loc - 1.0 * idx_ret  # beta=1 cru; hedged_beta refinado no finalize
        cur.execute("""UPDATE longleg_harvest SET status='CLOSED', closed_at=%s,
            long_ret_pct=%s, short_ret_pct=%s, index_ret_pct=%s, hedged_ret_pct=%s
            WHERE arbi_id=%s AND status='OPEN'""",
            (datetime.now(timezone.utc).replace(tzinfo=None),
             None if long_ret is None else round(long_ret, 4),
             None if short_ret is None else round(short_ret, 4),
             None if idx_ret is None else round(idx_ret, 4),
             None if hedged is None else round(hedged, 4), trade.get('id')))
        c.close()
        # directional NAO e finalizado aqui (candles do futuro ainda nao existem);
        # o scheduler (arbi_monitor -> finalize_directional) resolve quando a janela passa.
    except Exception as e:
        log.debug(f'[LONGLEG] close: {e}')


def finalize_directional(limit=20):
    """Finaliza DIRECTIONAL_EXIT + beta ex-ante + MATCHED_CONTROL dos trades cuja
    janela de 90min JA passou. IDEMPOTENTE: claim atomico por linha (WHERE
    dir_status='PENDING' + rowcount==1). So no scheduler (nao no endpoint). Fail-open."""
    try:
        create_tables()
        cutoff = int(datetime.now(timezone.utc).timestamp()) - (TIMEOUT_MIN + 5) * 60
        c = _conn(); cur = c.cursor()
        # [v5 GPT] recupera PROC orfaos (crash no meio): >15min volta p/ PENDING
        cur.execute("""UPDATE longleg_harvest SET dir_status='PENDING'
            WHERE dir_status='PROC' AND proc_claimed_at < (NOW() - INTERVAL 15 MINUTE)""")
        cur.execute("""SELECT arbi_id,long_leg,long_mkt,long_px_entry,opened_epoch,index_ret_pct
            FROM longleg_harvest WHERE dir_status='PENDING' AND status='CLOSED'
            AND opened_epoch IS NOT NULL AND opened_epoch < %s
            ORDER BY opened_epoch LIMIT %s""", (cutoff, limit))
        rows = cur.fetchall(); done = 0
        for arbi_id, ll, lm, lpe, oe, idx_ret in rows:
            # CLAIM atomico + timestamp (idempotencia + recuperacao de orfao)
            cur.execute("UPDATE longleg_harvest SET dir_status='PROC', proc_claimed_at=NOW() WHERE arbi_id=%s AND dir_status='PENDING'", (arbi_id,))
            if cur.rowcount != 1:
                continue
            ysym = _ysym(ll, lm); entry_px = float(lpe) if lpe else None
            pre = _yahoo_bars(ysym, oe - 4000, oe)                       # ATR ex-ante
            day_bars = _yahoo_bars(ysym, oe - 6*3600, oe + 8*3600)       # dia amplo (sessao + janela)
            # fim de sessao = ultimo bar do MESMO dia calendario da entrada
            ed = datetime.utcfromtimestamp(oe).strftime('%Y-%m-%d')
            same_day = [b for b in day_bars if datetime.utcfromtimestamp(b[0]).strftime('%Y-%m-%d') == ed]
            session_end = same_day[-1][0] if same_day else None
            win = [b for b in day_bars if oe <= b[0] <= oe + (TIMEOUT_MIN + 5) * 60]
            dr, rsn, mfe, mae, atrp, amb = _simulate_directional(pre, win, entry_px, session_end)
            beta = _ex_ante_beta(ysym, _idx_sym(lm), oe)
            loc = (win[-1][4] / entry_px - 1) * 100 if (win and entry_px) else None
            hb = (loc - beta * float(idx_ret)) if (loc is not None and idx_ret is not None) else None
            # [v5 GPT] MATCHED_CONTROL PAREADO: ate 5 controles no MESMO ativo/dia,
            # MESMO bloco de horario (+-30min do sinal), ATR parecido (0.5x-2x),
            # MESMA logica de saida. Compara o sinal com a MEDIANA dos controles.
            # Seed deterministica (reproduzivel, nao cherry-pick). atr do sinal = atrp.
            mc = None; mc_n = 0
            try:
                import random
                sig_hm = oe % 86400  # segundos no dia (proxy do horario)
                elig = []
                for b in same_day:
                    ce = b[0]
                    if ce == oe: continue
                    if ce > (session_end or oe) - TIMEOUT_MIN * 60: continue  # precisa janela cheia
                    if abs((ce % 86400) - sig_hm) > 1800: continue           # +-30min do sinal
                    elig.append(b)
                rnd = random.Random(hash(('mc', arbi_id, '2026-07-25')) & 0xffffffff)
                rnd.shuffle(elig)
                ctrl_rets = []
                for cb in elig[:5]:
                    cepoch = cb[0]; cpx = cb[4]
                    cpre = [x for x in day_bars if x[0] < cepoch][-12:]
                    cwin = [x for x in day_bars if cepoch <= x[0] <= cepoch + (TIMEOUT_MIN + 5) * 60]
                    cr, _, _, _, catr, _ = _simulate_directional(cpre, cwin, cpx, session_end)
                    # ATR parecido: descarta controle com vol muito diferente do sinal
                    if cr is None or (atrp and catr and not (0.5 * atrp <= catr <= 2.0 * atrp)):
                        continue
                    ctrl_rets.append(cr)
                if ctrl_rets:
                    mc = statistics.median(ctrl_rets); mc_n = len(ctrl_rets)
            except Exception:
                pass
            cur.execute("""UPDATE longleg_harvest SET dir_status='DONE',
                dir_exit_ret_pct=%s, dir_exit_reason=%s, mfe_pct=%s, mae_pct=%s,
                atr_pct_entry=%s, exit_ambiguous=%s, hedge_beta=%s, hedged_beta_ret_pct=%s,
                matched_ctrl_ret_pct=%s, matched_ctrl_n=%s WHERE arbi_id=%s""",
                (None if dr is None else round(dr, 4), rsn, mfe, mae, atrp, amb,
                 round(beta, 4), None if hb is None else round(hb, 4),
                 None if mc is None else round(mc, 4), mc_n, arbi_id))
            done += 1
        c.close()
        if done:
            log.info(f'[LONGLEG] finalize_directional: {done} finalizados (+matched control)')
    except Exception as e:
        log.debug(f'[LONGLEG] finalize: {e}')


def summary():
    # [v4 GPT] READ-ONLY: nao finaliza aqui (isso e do scheduler/monitor).
    create_tables()
    c = _conn(); cur = c.cursor(pymysql.cursors.DictCursor)
    cur.execute("SELECT * FROM longleg_harvest WHERE status='CLOSED'")
    rows = cur.fetchall(); c.close()

    def stat(vals):
        vals = [v for v in vals if v is not None]
        n = len(vals)
        if not n: return {'n': 0}
        wins = [v for v in vals if v > 0]; los = [v for v in vals if v <= 0]
        gm = statistics.mean(wins) if wins else 0; lm = statistics.mean(los) if los else 0
        p = len(wins) / n
        return {'n': n, 'wr': round(100 * p, 1), 'ret_med': round(statistics.mean(vals), 3),
                'mediana': round(statistics.median(vals), 3),
                'expectancy': round(p * gm + (1 - p) * lm, 3), 'total_ret': round(sum(vals), 2)}
    b = {}
    b['ALL'] = stat([r['long_ret_pct'] for r in rows])
    b['FILTERED_6'] = stat([r['long_ret_pct'] for r in rows if r['in_f6']])
    b['FILTERED_10'] = stat([r['long_ret_pct'] for r in rows if r['in_f10']])
    b['SHORT_ALL_teorico'] = stat([(-r['short_ret_pct']) if r['short_ret_pct'] is not None else None for r in rows])
    smart = []
    for r in rows:
        p = str(r.get('pulse_at_open') or '')
        if p == 'RISK_ON' and r['long_ret_pct'] is not None: smart.append(r['long_ret_pct'])
        elif p == 'RISK_OFF' and r['short_ret_pct'] is not None: smart.append(-r['short_ret_pct'])
    b['SMART_LEG'] = stat(smart)
    b['LIQUID_LEG_BETA_REDUCED'] = stat([r['hedged_beta_ret_pct'] for r in rows])
    b['DIRECTIONAL_EXIT'] = stat([r['dir_exit_ret_pct'] for r in rows if r['dir_status'] == 'DONE'])
    b['MATCHED_CONTROL'] = stat([r['matched_ctrl_ret_pct'] for r in rows if r['dir_status'] == 'DONE'])
    # o teste decisivo: directional (sinal) SUPERA o matched control (aleatorio)?
    _dir = [float(r['dir_exit_ret_pct']) for r in rows if r['dir_status'] == 'DONE' and r['dir_exit_ret_pct'] is not None]
    _mc = [float(r['matched_ctrl_ret_pct']) for r in rows if r['dir_status'] == 'DONE' and r['matched_ctrl_ret_pct'] is not None]
    b['_LIFT_dir_vs_matched'] = round((statistics.mean(_dir) - statistics.mean(_mc)), 4) if (_dir and _mc) else None

    bypair = {}
    for r in rows:
        if r['long_ret_pct'] is None: continue
        bypair.setdefault(r['pair'], []).append(r['long_ret_pct'])
    por_par = [{'pair': k, **stat(v)} for k, v in sorted(bypair.items(), key=lambda x: -sum(x[1]))]
    byband = {}
    for r in rows:
        if r['long_ret_pct'] is None: continue
        byband.setdefault(r['spread_band'], []).append(r['long_ret_pct'])
    por_faixa = {k: stat(v) for k, v in sorted(byband.items())}
    dir_pend = sum(1 for r in rows if r['dir_status'] == 'PENDING')
    return {'version': FILTER_VERSION, 'books': b, 'por_par': por_par,
            'por_faixa_spread': por_faixa, 'total_registrados': len(rows),
            'directional_pendentes': dir_pend,
            'nota': 'SHORT=teorico (borrow a modelar); hedge beta ex-ante 60d; ATR ex-ante; dir 90min deferido'}
