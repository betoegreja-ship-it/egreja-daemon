# -*- coding: utf-8 -*-
"""[24-jul-2026, decisao Beto pos-auditoria forense] ARBIX — Arbi v2 em SHADOW.

Motor de arbitragem cross-listed do universo IB (NYSE <-> LSE/EURONEXT/XETRA)
reconstruido com as licoes da auditoria forense de 24/jul:

  1. GATE DE SIMULTANEIDADE: so mede spread quando as DUAS pernas tem cotacao
     fresca (idade max ARBIX_QUOTE_MAX_AGE_S) e sincronizada entre si
     (defasagem max ARBIX_SYNC_TOL_S). Sem isso, nao ha sinal — era a origem
     dos +US$3,3M fantasmas do livro antigo (spread = preco vivo vs quote velha).
  2. PERSISTENCIA: o spread precisa aparecer em 2 ciclos consecutivos (>=60s)
     para virar entrada — elimina print isolado / ruido de tick.
  3. SIZING REAL: US$150k por perna (ARBIX_LEG_USD), nao US$1,3M.
  4. RAZAO ESTRUTURAL k por mediana rolante de amostras sincronizadas (3 dias);
     par novo so opera depois de >=60 amostras (aquecimento).
  5. JANELA: so opera no overlap Europa x NYSE (13:31-15:25 UTC). Fecha tudo
     forcado no fim da janela (sem posicao overnight com perna morta).
  6. TICK FILTER: par de preco baixo (ex.: LYG ~US$4) exige spread >= 2 ticks.

Universo: os 13 pares ja testados no IB + 4 aprovados no screening de 24/jul
(NGG-NG.L, RELX-REL.L, PUK-PRU.L, LYG-LLOY.L).

SHADOW puro: capital paper proprio, tabelas arbix_shadow_*. NAO toca no livro
Arbi de producao. Espelho IB opcional via ARBIX_IB_EXEC (default false, para
nao duplicar com o hook da Arbi de producao que ja esta em teste).

P&L bruto, sem deducao de taxas (diretriz do Beto). Custos ficam registrados
em colunas informativas.
"""
import os, json, time, logging, threading
from datetime import datetime, timezone

import requests
import pymysql

log = logging.getLogger('egreja.arbix')

# (pair_id, leg_us, leg_eu, moeda, regiao)
#   moeda : GBX (pence Londres), EUR (Euronext/Madri), CAD (dolar canadense)
#   regiao: EU  = majors, janela overlap Europa x NY, perna US$150k
#           EU2 = 2a linha europeia, mesma janela, perna US$100k
#           CA  = dual-listing canadense TSX<->NYSE, sessao CHEIA, perna US$100k
PAIRS = [
    ('BP-BP.L', 'BP', 'BP.L', 'GBX', 'EU'), ('SHEL-SHEL.L', 'SHEL', 'SHEL.L', 'GBX', 'EU'),
    ('AZN-AZN.L', 'AZN', 'AZN.L', 'GBX', 'EU'), ('GSK-GSK.L', 'GSK', 'GSK.L', 'GBX', 'EU'),
    ('HSBC-HSBA.L', 'HSBC', 'HSBA.L', 'GBX', 'EU'), ('RIO-RIO.L', 'RIO', 'RIO.L', 'GBX', 'EU'),
    ('UL-ULVR.L', 'UL', 'ULVR.L', 'GBX', 'EU'), ('DEO-DGE.L', 'DEO', 'DGE.L', 'GBX', 'EU'),
    ('BTI-BATS.L', 'BTI', 'BATS.L', 'GBX', 'EU'), ('ASML-ASML.AS', 'ASML', 'ASML.AS', 'EUR', 'EU'),
    ('TTE-TTE.PA', 'TTE', 'TTE.PA', 'EUR', 'EU'), ('SAP-SAP.DE', 'SAP', 'SAP.DE', 'EUR', 'EU'),
    ('LVMUY-MC.PA', 'LVMUY', 'MC.PA', 'EUR', 'EU'),
    # aprovados no screening 24/jul (episodios reais >=0.3% em 60d)
    ('NGG-NG.L', 'NGG', 'NG.L', 'GBX', 'EU'), ('RELX-REL.L', 'RELX', 'REL.L', 'GBX', 'EU'),
    ('PUK-PRU.L', 'PUK', 'PRU.L', 'GBX', 'EU'), ('LYG-LLOY.L', 'LYG', 'LLOY.L', 'GBX', 'EU'),
    # ═══ [28-jul-2026, decisao Beto] 2a LINHA — onde o edge cross-listed vive ═══
    # Canadenses TSX<->NYSE (sessao cheia, 1 FX CAD/USD, mid-caps de mineracao):
    ('CCJ-CCO.TO',  'CCJ',  'CCO.TO', 'CAD', 'CA'), ('BTG-BTO.TO',  'BTG',  'BTO.TO', 'CAD', 'CA'),
    ('PAAS-PAAS.TO','PAAS', 'PAAS.TO','CAD', 'CA'), ('HBM-HBM.TO',  'HBM',  'HBM.TO', 'CAD', 'CA'),
    ('EGO-ELD.TO',  'EGO',  'ELD.TO', 'CAD', 'CA'), ('IAG-IMG.TO',  'IAG',  'IMG.TO', 'CAD', 'CA'),
    ('NGD-NGD.TO',  'NGD',  'NGD.TO', 'CAD', 'CA'), ('SAND-SSL.TO', 'SAND', 'SSL.TO', 'CAD', 'CA'),
    # ADRs europeus de 2a linha (razao ADR absorvida pelo k aprendido):
    ('AEG-AGN.AS',  'AEG',  'AGN.AS', 'EUR', 'EU2'), ('TEF-TEF.MC',  'TEF',  'TEF.MC', 'EUR', 'EU2'),
    ('RYAAY-RYA.IR','RYAAY','RYA.IR', 'EUR', 'EU2'), ('SNN-SN.L',    'SNN',  'SN.L',   'GBX', 'EU2'),
    ('WPP-WPP.L',   'WPP',  'WPP.L',  'GBX', 'EU2'), ('NWG-NWG.L',   'NWG',  'NWG.L',  'GBX', 'EU2'),
]
FXSYM = {'GBX': 'GBPUSD=X', 'EUR': 'EURUSD=X', 'CAD': 'CADUSD=X'}


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
    cur.execute("""CREATE TABLE IF NOT EXISTS arbix_shadow_trades (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        pair VARCHAR(16) NOT NULL, direction VARCHAR(8) NOT NULL,
        status VARCHAR(8) NOT NULL DEFAULT 'OPEN',
        opened_at DATETIME, closed_at DATETIME,
        entry_spread_pct DECIMAL(8,4), exit_spread_pct DECIMAL(8,4),
        k_ratio DECIMAL(14,6),
        price_us_entry DECIMAL(14,4), price_eu_entry DECIMAL(14,4),
        price_us_exit DECIMAL(14,4), price_eu_exit DECIMAL(14,4),
        fx_entry DECIMAL(12,6), fx_exit DECIMAL(12,6),
        quote_lag_ms_entry INT, quote_age_s_entry INT,
        leg_usd DECIMAL(12,2), pnl_gross DECIMAL(12,2), pnl_pct DECIMAL(8,4),
        est_costs DECIMAL(12,2), close_reason VARCHAR(24),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX ix_pair_status (pair,status)) CHARACTER SET utf8mb4""")
    cur.execute("""CREATE TABLE IF NOT EXISTS arbix_shadow_meta (
        k VARCHAR(64) PRIMARY KEY, v MEDIUMTEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) CHARACTER SET utf8mb4""")
    c.close()


def _quote(sym):
    """Cotacao viva via chart meta (sem crumb): (price, epoch_ts) ou (None,None)."""
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}',
            params={'range': '1d', 'interval': '5m'},
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        m = r.json()['chart']['result'][0]['meta']
        return float(m['regularMarketPrice']), int(m['regularMarketTime'])
    except Exception as e:
        log.debug(f'[ARBIX] quote {sym}: {e}')
        return None, None


class _State:
    def __init__(self):
        self.k_samples = {}     # pair -> [(ts, ratio)]
        self.pending = {}       # pair -> (signal_dir, spread, ciclo_ts)  persistencia
        self.loaded = False

    def load(self, cur):
        cur.execute("SELECT v FROM arbix_shadow_meta WHERE k='k_samples'")
        row = cur.fetchone()
        if row:
            try:
                raw = json.loads(row[0])
                self.k_samples = {p: [(int(t), float(x)) for t, x in v] for p, v in raw.items()}
            except Exception: pass
        self.loaded = True

    def save(self, cur):
        cur.execute("INSERT INTO arbix_shadow_meta (k,v) VALUES ('k_samples',%s) "
                    "ON DUPLICATE KEY UPDATE v=VALUES(v)",
                    (json.dumps(self.k_samples),))


_st = _State()


def _median(xs):
    s = sorted(xs); n = len(s)
    return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])


def _in_window(now, region='EU'):
    """Janela de negociacao por regiao (dias uteis, UTC).
    EU/EU2: overlap Europa x NYSE 13:31-15:25. CA: sessao cheia NYSE/TSX 13:31-19:55."""
    if now.weekday() >= 5: return False
    hm = now.hour * 60 + now.minute
    if region == 'CA':
        return (13 * 60 + 31) <= hm <= (19 * 60 + 55)   # 9:31-15:55 ET
    return (13 * 60 + 31) <= hm <= (15 * 60 + 25)


def scan_cycle():
    sync_tol = _f('ARBIX_SYNC_TOL_S', 20)
    max_age = _f('ARBIX_QUOTE_MAX_AGE_S', 90)
    min_spr = _f('ARBIX_MIN_SPREAD', 0.30)
    exit_spr = _f('ARBIX_EXIT_SPREAD', 0.10)
    timeout_m = _f('ARBIX_TIMEOUT_MIN', 240)
    leg_major = _f('ARBIX_LEG_USD', 150000)       # majors EU
    leg_2l = _f('ARBIX_LEG_USD_2L', 100000)       # 2a linha (CA/EU2) — teto US$100k
    max_open = int(_f('ARBIX_MAX_OPEN', 4))

    now = datetime.now(timezone.utc)
    c = _conn(); cur = c.cursor()
    if not _st.loaded: _st.load(cur)
    cur.execute("SELECT id,pair,direction,opened_at,entry_spread_pct,k_ratio,"
                "price_us_entry,price_eu_entry,fx_entry,leg_usd FROM arbix_shadow_trades WHERE status='OPEN'")
    open_rows = {r[1]: r for r in cur.fetchall()}

    fx_cache = {}
    for cur_code, fxs in FXSYM.items():
        fx_cache[cur_code] = _quote(fxs)

    n_open = len(open_rows)
    for pid, us, eu, code, region in PAIRS:
        try:
            window = _in_window(now, region)               # janela por regiao
            leg_usd = leg_2l if region in ('CA', 'EU2') else leg_major
            pu, tu = _quote(us)
            pe_raw, te = _quote(eu)
            fx, tf = fx_cache[code]
            if not (pu and pe_raw and fx): continue
            now_e = int(now.timestamp())
            age = max(now_e - tu, now_e - te)
            lag = abs(tu - te)
            pe_usd = pe_raw * fx / (100.0 if code == 'GBX' else 1.0)

            # ===== GATE DE SIMULTANEIDADE =====
            synced = (age <= max_age) and (lag <= sync_tol)

            if synced:
                # alimenta razao estrutural k (mesmo fora de janela de trade)
                samples = _st.k_samples.setdefault(pid, [])
                samples.append((now_e, pu / pe_usd))
                cutoff = now_e - 3 * 86400
                _st.k_samples[pid] = [(t, x) for t, x in samples if t >= cutoff][-2000:]

            samples = _st.k_samples.get(pid, [])
            if len(samples) < 60:
                continue  # aquecimento: ainda calibrando k
            k = _median([x for _, x in samples])
            spread = 100.0 * (pu / (k * pe_usd) - 1.0) if synced else None

            # ---- gestao de posicao aberta ----
            if pid in open_rows:
                (tid, _, direction, opened_at, es, k0, pu0, pe0, fx0, leg0) = open_rows[pid]
                held_min = (now.replace(tzinfo=None) - opened_at).total_seconds() / 60
                reason = None
                if spread is not None and abs(spread) <= exit_spr: reason = 'CONVERGED'
                elif held_min >= timeout_m: reason = 'TIMEOUT'
                elif not window: reason = 'WINDOW_END'
                if reason:
                    pu0, pe0, fx0, k0, leg0 = map(float, (pu0, pe0, fx0, k0, leg0))
                    pe0_usd = pe0 * fx0 / (100.0 if code == 'GBX' else 1.0)
                    r_us = pu / pu0 - 1.0
                    r_eu = pe_usd / pe0_usd - 1.0
                    pnl = leg0 * (r_us - r_eu) if direction == 'LONG_US' else leg0 * (r_eu - r_us)
                    est_cost = leg0 * 2 * 0.0007  # ~7bps/perna ida+volta, informativo
                    cur.execute("""UPDATE arbix_shadow_trades SET status='CLOSED',closed_at=%s,
                        exit_spread_pct=%s,price_us_exit=%s,price_eu_exit=%s,fx_exit=%s,
                        pnl_gross=%s,pnl_pct=%s,est_costs=%s,close_reason=%s WHERE id=%s""",
                        (now.replace(tzinfo=None), None if spread is None else round(spread, 4),
                         pu, pe_raw, fx, round(pnl, 2), round(100 * pnl / leg0, 4),
                         round(est_cost, 2), reason, tid))
                    n_open -= 1
                    log.warning(f'[ARBIX] CLOSE {pid} {direction} {reason} '
                                f'spread={spread if spread is None else round(spread,3)}% pnl=${pnl:,.0f} ({held_min:.0f}min)')
                continue

            # ---- entrada (so em janela, sincronizado, com persistencia) ----
            if not (window and synced) or n_open >= max_open:
                _st.pending.pop(pid, None); continue
            # tick filter para preco baixo: exige >= 2 ticks de dislocacao
            tick_pct = 100.0 * 0.02 / pu  # 2 ticks de $0.01 sobre o preco US
            thr = max(min_spr, tick_pct)
            if abs(spread) < thr:
                _st.pending.pop(pid, None); continue
            sig = 'LONG_EU' if spread > 0 else 'LONG_US'  # compra a perna barata
            prev = _st.pending.get(pid)
            if not prev or prev[0] != sig or now_e - prev[2] > 200:
                _st.pending[pid] = (sig, spread, now_e)  # 1o ciclo: arma
                continue
            # 2o ciclo consecutivo com o mesmo sinal -> ENTRADA
            _st.pending.pop(pid, None)
            cur.execute("""INSERT INTO arbix_shadow_trades (pair,direction,status,opened_at,
                entry_spread_pct,k_ratio,price_us_entry,price_eu_entry,fx_entry,
                quote_lag_ms_entry,quote_age_s_entry,leg_usd)
                VALUES (%s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (pid, 'LONG_US' if sig == 'LONG_US' else 'LONG_EU', now.replace(tzinfo=None),
                 round(spread, 4), round(k, 6), pu, pe_raw, fx,
                 int(lag * 1000), int(age), leg_usd))
            n_open += 1
            log.warning(f'[ARBIX] OPEN {pid} {sig} spread={spread:+.3f}% '
                        f'(lag={lag}s age={age}s k={k:.4f}) ${leg_usd:,.0f}/perna')
            # espelho IB opcional (default OFF para nao duplicar com Arbi producao)
            if os.environ.get('ARBIX_IB_EXEC', 'false').lower() == 'true':
                try:
                    from modules.ib_exec import exec_arbi
                    mkt_b = 'TSX' if code == 'CAD' else ('LSE' if code == 'GBX' else 'EURONEXT')
                    pair_dict = {'id': f'ARBIX-{pid}', 'leg_a': us, 'leg_b': eu,
                                 'mkt_a': 'NYSE', 'mkt_b': mkt_b, 'ratio_a': 1, 'ratio_b': 1}
                    exec_arbi(pair_dict, 'LONG_A' if sig == 'LONG_US' else 'LONG_B',
                              pu, pe_usd, 'OPEN', ref_id=f'ARBIX-{pid}-{now_e}')
                except Exception as _e:
                    log.error(f'[ARBIX] ib exec: {_e}')
        except Exception as e:
            log.error(f'[ARBIX] {pid}: {e}')

    try: _st.save(cur)
    except Exception: pass
    c.close()


def arbix_shadow_loop(beat_fn=None):
    if os.environ.get('ARBIX_ENABLED', 'true').lower() == 'false':
        log.info('[ARBIX] desabilitado via env'); return
    try:
        create_tables()
        log.info(f'[ARBIX] Arbi v2 shadow iniciado — {len(PAIRS)} pares cross-listed IB, '
                 f'gate de simultaneidade ativo')
    except Exception as e:
        log.error(f'[ARBIX] setup: {e}'); return
    while True:
        try:
            if beat_fn: beat_fn('arbix_shadow_loop')
            now = datetime.now(timezone.utc)
            # roda ciclo na uniao das janelas (EU overlap + sessao cheia CA).
            # A janela por-par decide quem realmente opera em cada horario.
            hm = now.hour * 60 + now.minute
            if now.weekday() < 5 and (13 * 60 + 25) <= hm <= (20 * 60 + 10):
                scan_cycle()
                time.sleep(75)
            else:
                time.sleep(300)
        except Exception as e:
            log.error(f'[ARBIX] loop: {e}')
            time.sleep(120)
