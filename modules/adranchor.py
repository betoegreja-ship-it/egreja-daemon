# -*- coding: utf-8 -*-
"""[24-jul-2026, decisao Beto] ADRANCHOR — os 2 ensinamentos reais da Arbi.

A auditoria forense provou que o 'spread' da Arbi era lead-lag, nao arbitragem.
O estudo de atribuicao (405 trades) quantificou DOIS sinais direcionais reais:

  1. ARBI-ANCHOR (lado B3): quando a acao B3 desvia >=1% do valor justo
     implicito pelo ADR (ADR x USDBRL), ela tende a voltar. Acerto historico:
     SBSP3 100%, ITUB4 90%, GGBR4 74%, ABEV3 71%, BBDC4 64%; faixa 2-4% = 72%.
     -> vira VIES para o motor direcional B3 (anchor_bias()).

  2. ADR-CATCHUP (lado NYSE): nos ADRs menos eficientes (CIG, SID, TIMB, UGP)
     e o ADR que corre atras do preco fresco da B3. -> book direcional no ADR,
     UMA perna so, executavel HOJE no IB paper (sem aluguel, sem casamento).

PROTECOES (licoes da auditoria, na ordem em que ja nos morderam):
  a) GATE DE SIMULTANEIDADE: as 3 cotacoes (B3, ADR, USDBRL) precisam ser
     frescas (<=ADRA_QUOTE_MAX_AGE_S) e sincronizadas entre si
     (<=ADRA_SYNC_TOL_S). Sem isso nao ha sinal.
  b) GUARDA DE CAMBIO [preocupacao explicita do Beto 24/jul]: o desvio e
     recalculado com o USDBRL mediano de ~30min atras. Se com o cambio antigo
     o desvio encolhe (<60%) ou troca de sinal, o "desvio" e movimento de
     CAMBIO, nao da acao -> sinal descartado e gravado como FX_DRIVEN.
  c) RAZAO ESTRUTURAL k por mediana rolante de 3 dias (nao confia em ratio
     cadastrado — licao PETR4-PBR). Aquecimento: 60 amostras antes de operar.
  d) PERSISTENCIA: 2 ciclos consecutivos para entrada.
  e) Janela: so com B3 e NYSE abertas simultaneamente (13:35-19:50 UTC).

Book shadow proprio (adranchor_*). P&L bruto sem taxas (diretriz Beto).
Execucao real no IB paper via modules.ib_exec (uma perna, ADR NYSE).
"""
import os, json, time, logging
from datetime import datetime, timezone
from collections import deque

import requests
import pymysql

log = logging.getLogger('egreja.adranchor')

# B3 local -> ADR NYSE. ANCHOR = sinal vira vies p/ motor B3.
ANCHOR_PAIRS = {  # b3_sym: adr
    'SBSP3': 'SBS', 'ITUB4': 'ITUB', 'ABEV3': 'ABEV', 'GGBR4': 'GGB', 'BBDC4': 'BBD',
}
# CATCHUP = ADR corre atras da B3 -> trade direcional NO ADR (executa IB).
CATCHUP_PAIRS = {
    'CMIG4': 'CIG', 'CSNA3': 'SID', 'TIMS3': 'TIMB', 'UGPA3': 'UGP',
}
ALL_PAIRS = {**ANCHOR_PAIRS, **CATCHUP_PAIRS}


def _f(name, d):
    try: return float(os.environ.get(name, d))
    except Exception: return float(d)


def _conn():
    return pymysql.connect(
        host=os.environ['MYSQLHOST'], user=os.environ['MYSQLUSER'],
        password=os.environ['MYSQLPASSWORD'], database=os.environ['MYSQLDATABASE'],
        port=int(os.environ.get('MYSQLPORT', 3306)), autocommit=True)


def create_tables():
    c = _conn(); cur = c.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS adranchor_signals (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        ts DATETIME, pair VARCHAR(12), kind VARCHAR(8),
        dev_pct DECIMAL(8,4), dev_oldfx_pct DECIMAL(8,4), fx DECIMAL(10,4),
        fx_driven TINYINT, quote_age_s INT, quote_lag_s INT,
        side VARCHAR(6), actioned TINYINT DEFAULT 0,
        INDEX ix_ts (ts), INDEX ix_pair (pair)) CHARACTER SET utf8mb4""")
    cur.execute("""CREATE TABLE IF NOT EXISTS adranchor_trades (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        adr VARCHAR(8), b3_sym VARCHAR(8), direction VARCHAR(6),
        status VARCHAR(8) DEFAULT 'OPEN',
        opened_at DATETIME, closed_at DATETIME,
        entry_dev_pct DECIMAL(8,4), exit_dev_pct DECIMAL(8,4),
        adr_entry DECIMAL(12,4), adr_exit DECIMAL(12,4),
        b3_entry DECIMAL(12,4), b3_exit DECIMAL(12,4),
        fx_entry DECIMAL(10,4), fx_exit DECIMAL(10,4),
        leg_usd DECIMAL(12,2), pnl_gross DECIMAL(12,2), pnl_pct DECIMAL(8,4),
        close_reason VARCHAR(24),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX ix_status (status)) CHARACTER SET utf8mb4""")
    cur.execute("""CREATE TABLE IF NOT EXISTS adranchor_meta (
        k VARCHAR(64) PRIMARY KEY, v MEDIUMTEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) CHARACTER SET utf8mb4""")
    c.close()


def _quote(sym):
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}',
            params={'range': '1d', 'interval': '5m'},
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        m = r.json()['chart']['result'][0]['meta']
        return float(m['regularMarketPrice']), int(m['regularMarketTime'])
    except Exception as e:
        log.debug(f'[ADRA] quote {sym}: {e}')
        return None, None


def _median(xs):
    s = sorted(xs); n = len(s)
    return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])


class _State:
    def __init__(self):
        self.k_samples = {}          # pair -> [(ts, k)]
        self.fx_hist = deque(maxlen=200)   # (ts, usdbrl)
        self.pending = {}            # adr -> (side, ts)
        self.bias = {}               # b3_sym -> (side, dev, ts)  p/ motor B3
        self.loaded = False

    def load(self, cur):
        cur.execute("SELECT v FROM adranchor_meta WHERE k='k_samples'")
        row = cur.fetchone()
        if row:
            try:
                raw = json.loads(row[0])
                self.k_samples = {p: [(int(t), float(x)) for t, x in v] for p, v in raw.items()}
            except Exception: pass
        self.loaded = True

    def save(self, cur):
        cur.execute("INSERT INTO adranchor_meta (k,v) VALUES ('k_samples',%s) "
                    "ON DUPLICATE KEY UPDATE v=VALUES(v)", (json.dumps(self.k_samples),))


_st = _State()


def anchor_bias(b3_symbol):
    """API para o motor B3: ('LONG'|'SHORT', dev_pct) se ha vies fresco (<=10min).
    Fail-open: qualquer problema -> None (nunca bloqueia por erro)."""
    try:
        sym = str(b3_symbol).upper().replace('.SA', '')
        ent = _st.bias.get(sym)
        if not ent: return None
        side, dev, ts = ent
        if time.time() - ts > 600: return None
        return (side, dev)
    except Exception:
        return None


def _fx_old(now_e):
    """USDBRL mediano de 25-40min atras (guarda de cambio)."""
    xs = [v for t, v in _st.fx_hist if now_e - 2400 <= t <= now_e - 1500]
    if not xs:
        xs = [v for t, v in _st.fx_hist if t <= now_e - 600]  # fallback >=10min
    return _median(xs) if xs else None


def _in_window(now):
    if now.weekday() >= 5: return False
    hm = now.hour * 60 + now.minute
    return (13 * 60 + 35) <= hm <= (19 * 60 + 50)


def scan_cycle():
    max_age = _f('ADRA_QUOTE_MAX_AGE_S', 90)
    sync_tol = _f('ADRA_SYNC_TOL_S', 25)
    min_dev = _f('ADRA_MIN_DEV', 1.0)          # % — faixa com 61-72% de acerto
    exit_dev = _f('ADRA_EXIT_DEV', 0.25)
    timeout_m = _f('ADRA_TIMEOUT_MIN', 150)
    leg_usd = _f('ADRA_LEG_USD', 10000)
    max_open = int(_f('ADRA_MAX_OPEN', 3))

    now = datetime.now(timezone.utc)
    now_e = int(now.timestamp())
    window = _in_window(now)

    c = _conn(); cur = c.cursor()
    if not _st.loaded: _st.load(cur)

    fx, tfx = _quote('BRL=X')
    if fx and now_e - tfx <= 300:
        _st.fx_hist.append((tfx, fx))
    fx_ok = bool(fx) and (now_e - (tfx or 0) <= max_age * 2)

    cur.execute("SELECT id,adr,b3_sym,direction,opened_at,adr_entry,b3_entry,fx_entry,leg_usd "
                "FROM adranchor_trades WHERE status='OPEN'")
    open_by_adr = {r[1]: r for r in cur.fetchall()}
    n_open = len(open_by_adr)

    for b3s, adr in ALL_PAIRS.items():
        try:
            pb3, tb3 = _quote(f'{b3s}.SA')
            padr, tadr = _quote(adr)
            if not (pb3 and padr and fx_ok): continue
            age = max(now_e - tb3, now_e - tadr)
            lag = abs(tb3 - tadr)
            synced = (age <= max_age) and (lag <= sync_tol)

            if synced:
                samples = _st.k_samples.setdefault(b3s, [])
                samples.append((now_e, pb3 / (padr * fx)))
                cutoff = now_e - 3 * 86400
                _st.k_samples[b3s] = [(t, x) for t, x in samples if t >= cutoff][-2000:]
            samples = _st.k_samples.get(b3s, [])
            if len(samples) < 60:
                continue  # aquecimento
            k = _median([x for _, x in samples])
            if not synced:
                dev = None
            else:
                dev = 100.0 * (pb3 / (k * padr * fx) - 1.0)

            kind = 'ANCHOR' if b3s in ANCHOR_PAIRS else 'CATCH'

            # ---- gestao de posicao CATCHUP aberta ----
            if adr in open_by_adr:
                (tid, _, _, direction, opened_at, adr0, b30, fx0, leg0) = open_by_adr[adr]
                held = (now.replace(tzinfo=None) - opened_at).total_seconds() / 60
                reason = None
                if dev is not None and abs(dev) <= exit_dev: reason = 'CONVERGED'
                elif held >= timeout_m: reason = 'TIMEOUT'
                elif not window: reason = 'WINDOW_END'
                if reason:
                    adr0 = float(adr0); leg0 = float(leg0)
                    r = padr / adr0 - 1.0
                    pnl = leg0 * r if direction == 'LONG' else -leg0 * r
                    cur.execute("""UPDATE adranchor_trades SET status='CLOSED',closed_at=%s,
                        exit_dev_pct=%s,adr_exit=%s,b3_exit=%s,fx_exit=%s,
                        pnl_gross=%s,pnl_pct=%s,close_reason=%s WHERE id=%s""",
                        (now.replace(tzinfo=None), None if dev is None else round(dev, 4),
                         padr, pb3, fx, round(pnl, 2), round(100 * pnl / leg0, 4), reason, tid))
                    n_open -= 1
                    log.warning(f'[ADRA] CLOSE {adr} {direction} {reason} pnl=${pnl:,.0f} ({held:.0f}min)')
                    try:
                        from modules.ib_exec import exec_on_close
                        exec_on_close({'id': f'ADRC-{tid}', 'symbol': adr,
                                       'direction': direction, 'current_price': padr})
                    except Exception as _e:
                        log.error(f'[ADRA] ib close: {_e}')
                continue

            if dev is None:
                _st.pending.pop(adr, None); continue

            # ---- GUARDA DE CAMBIO (Beto, 24/jul) ----
            fxo = _fx_old(now_e)
            dev_oldfx = 100.0 * (pb3 / (k * padr * fxo) - 1.0) if fxo else None
            fx_driven = (dev_oldfx is not None and
                         (dev_oldfx * dev <= 0 or abs(dev_oldfx) < 0.6 * abs(dev)))

            # registra sinal relevante (auditoria continua, mesmo sem acao)
            if abs(dev) >= 0.8 * min_dev:
                side = None
                if b3s in ANCHOR_PAIRS:
                    side = 'LONG' if dev <= -min_dev else ('SHORT' if dev >= min_dev else None)
                else:
                    side = 'LONG' if dev >= min_dev else ('SHORT' if dev <= -min_dev else None)
                cur.execute("""INSERT INTO adranchor_signals (ts,pair,kind,dev_pct,dev_oldfx_pct,
                    fx,fx_driven,quote_age_s,quote_lag_s,side) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (now.replace(tzinfo=None), b3s, kind, round(dev, 4),
                     None if dev_oldfx is None else round(dev_oldfx, 4),
                     fx, 1 if fx_driven else 0, int(age), int(lag), side))

            if fx_driven:
                _st.bias.pop(b3s, None); _st.pending.pop(adr, None)
                continue

            # ---- ANCHOR: alimenta vies p/ motor B3 ----
            if b3s in ANCHOR_PAIRS:
                if dev <= -min_dev:   _st.bias[b3s] = ('LONG', round(dev, 3), now_e)
                elif dev >= min_dev:  _st.bias[b3s] = ('SHORT', round(dev, 3), now_e)
                else:                 _st.bias.pop(b3s, None)
                continue

            # ---- CATCHUP: trade direcional no ADR (executa IB paper) ----
            if not window or n_open >= max_open:
                _st.pending.pop(adr, None); continue
            if abs(dev) < min_dev:
                _st.pending.pop(adr, None); continue
            # dev>0: B3 rica vs ADR -> ADR barato -> LONG ADR. dev<0 -> SHORT ADR.
            side = 'LONG' if dev > 0 else 'SHORT'
            prev = _st.pending.get(adr)
            if not prev or prev[0] != side or now_e - prev[1] > 240:
                _st.pending[adr] = (side, now_e); continue
            _st.pending.pop(adr, None)
            cur.execute("""INSERT INTO adranchor_trades (adr,b3_sym,direction,status,opened_at,
                entry_dev_pct,adr_entry,b3_entry,fx_entry,leg_usd)
                VALUES (%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s)""",
                (adr, b3s, side, now.replace(tzinfo=None), round(dev, 4), padr, pb3, fx, leg_usd))
            tid = cur.lastrowid
            cur.execute("UPDATE adranchor_signals SET actioned=1 WHERE pair=%s ORDER BY id DESC LIMIT 1", (b3s,))
            n_open += 1
            log.warning(f'[ADRA] OPEN {adr} {side} dev={dev:+.2f}% (fx_guard ok, '
                        f'lag={lag}s age={age}s) ${leg_usd:,.0f}')
            try:
                from modules.ib_exec import exec_on_open
                exec_on_open({'id': f'ADRC-{tid}', 'symbol': adr,
                              'direction': side, 'current_price': padr})
            except Exception as _e:
                log.error(f'[ADRA] ib open: {_e}')
        except Exception as e:
            log.error(f'[ADRA] {b3s}: {e}')

    try: _st.save(cur)
    except Exception: pass
    c.close()


def adranchor_loop(beat_fn=None):
    if os.environ.get('ADRANCHOR_ENABLED', 'true').lower() == 'false':
        log.info('[ADRA] desabilitado via env'); return
    try:
        create_tables()
        log.info(f'[ADRA] ADRANCHOR iniciado — {len(ANCHOR_PAIRS)} anchor (vies B3) + '
                 f'{len(CATCHUP_PAIRS)} catchup (ADR no IB), fx-guard ativo')
    except Exception as e:
        log.error(f'[ADRA] setup: {e}'); return
    while True:
        try:
            if beat_fn: beat_fn('adranchor_loop')
            now = datetime.now(timezone.utc)
            hm = now.hour * 60 + now.minute
            if now.weekday() < 5 and (13 * 60 + 30) <= hm <= (20 * 60 + 5):
                scan_cycle()
                time.sleep(90)
            else:
                time.sleep(300)
        except Exception as e:
            log.error(f'[ADRA] loop: {e}')
            time.sleep(120)
