# -*- coding: utf-8 -*-
"""[22-jul-2026] MOTOR CROSS-ASSET RELATIVE VALUE — MODO SHADOW (paper).

Irmao diario do crypto_rv_shadow: mesma logica de valor relativo (neutro a
direcao) aplicada a COMMODITIES (ETFs) e CAMBIO (forex), via Polygon. Barras
diarias. Nao aposta direcao — aposta que o spread entre dois ativos
correlacionados volta a media.

Pares aprovados no screen de edge de 22-jul-2026 (RV z 2.0/0.5/3.5, diario):
  COMMODITIES: GLD-GDX (ouro vs mineradoras, +17.8% WR100), CPER-XME (cobre vs
               metais, +8.6% WR71), WEAT-CORN (trigo vs milho, +10.0% WR67)
  FX         : EUR-GBP (EURUSD vs GBPUSD, +3.3% WR86)
Reprovados descartados de proposito (GLD-SLV, USO-XLE, AUD-NZD, etc.).

SHADOW: nao envia ordem, nao toca capital real, nao mexe em nenhum outro
motor. Tabelas proprias crossasset_rv_shadow_*. SEM taxa (regra Beto).

Envs:
  CROSSASSET_RV_SHADOW_ENABLED (true)  CROSSASSET_RV_SHADOW_CAPITAL (1000000)
  CROSSASSET_RV_POSITION_PCT   (50)    CROSSASSET_RV_FEE_BPS        (0)
  CROSSASSET_RV_BETA_WINDOW    (120)   CROSSASSET_RV_Z_WINDOW       (60)
  CROSSASSET_RV_Z_ENTRY (2.0)  CROSSASSET_RV_Z_EXIT (0.5)
  CROSSASSET_RV_Z_STOP  (3.5)  CROSSASSET_RV_TIMEOUT_DAYS (30)
"""
import os, math, time, logging
from datetime import datetime, timedelta, timezone

import requests
import pymysql

log = logging.getLogger('egreja.crossasset.rv.shadow')

# A=numerador, B=denominador. 'kind' so documenta a classe.
PAIRS = {
    'GLD-GDX':   {'a': 'GLD',       'b': 'GDX',       'book': 'METAIS',    'kind': 'commodity'},
    'CPER-XME':  {'a': 'CPER',      'b': 'XME',       'book': 'METAIS',    'kind': 'commodity'},
    'WEAT-CORN': {'a': 'WEAT',      'b': 'CORN',      'book': 'AGRO',      'kind': 'commodity'},
    'EUR-GBP':   {'a': 'C:EURUSD',  'b': 'C:GBPUSD',  'book': 'FX_MAJORS', 'kind': 'fx'},
    # ═══ [28-jul-2026, decisao Beto] Candidatos novos direto no shadow ═══
    # Metal-vs-mineradoras (analogos do GLD-GDX vencedor) + energia/agro.
    # Provam o edge ao vivo; sem backtest previo, sem risco (shadow puro).
    'SLV-SIL':   {'a': 'SLV',       'b': 'SIL',       'book': 'METAIS',    'kind': 'commodity'},
    'GDX-GDXJ':  {'a': 'GDX',       'b': 'GDXJ',      'book': 'METAIS',    'kind': 'commodity'},
    'PPLT-PALL': {'a': 'PPLT',      'b': 'PALL',      'book': 'METAIS',    'kind': 'commodity'},
    'URA-URNM':  {'a': 'URA',       'b': 'URNM',      'book': 'ENERGIA',   'kind': 'commodity'},
    'XLE-XOP':   {'a': 'XLE',       'b': 'XOP',       'book': 'ENERGIA',   'kind': 'commodity'},
    'SOYB-CORN': {'a': 'SOYB',      'b': 'CORN',      'book': 'AGRO',      'kind': 'commodity'},
    # ═══ [03-ago-2026, expansao Beto] Aprovados em screening quantitativo ═══
    # Regua: 2 anos diarios, beta 120d, z 2.0/0.4, custo 20bps RT;
    # aprovacao = WR>=55% E PnL>0. Reprovados: IWM-SPY, KWEB-FXI, XLU-TLT,
    # DBA-CORN, AUD-NZD (nao readicionar sem novo estudo).
    'GLD-SLV':   {'a': 'GLD',       'b': 'SLV',       'book': 'METAIS',    'kind': 'commodity'},  # ratio ouro/prata: WR 83.3
    'USO-XLE':   {'a': 'USO',       'b': 'XLE',       'book': 'ENERGIA',   'kind': 'commodity'},  # petroleo vs acoes energia: WR 77.8
    'OIH-XLE':   {'a': 'OIH',       'b': 'XLE',       'book': 'ENERGIA',   'kind': 'commodity'},  # oil services vs energia: WR 75
    'EWZ-ILF':   {'a': 'EWZ',       'b': 'ILF',       'book': 'EM_LATAM',  'kind': 'equity'},     # Brasil vs LatAm: corr .94, WR 87.5
    'COPX-CPER': {'a': 'COPX',      'b': 'CPER',      'book': 'METAIS',    'kind': 'commodity'},  # mineradoras cobre vs cobre: WR 71.4
    'EUR-CHF':   {'a': 'C:EURUSD',  'b': 'C:CHFUSD',  'book': 'FX_MAJORS', 'kind': 'fx'},         # bloco europeu: WR 100 (n=6)
    # [03-ago] Adicao com CONVICCAO do Kimi: unico par com arbitragem de
    # substituicao genuina (nao correlacao historica). TLT=20y+, IEF=7-10y,
    # ambos Treasury dos EUA: o spread e o SLOPE da curva de duration, com
    # beta teorico ~2.3 vindo de cash flows (nao de correlacao). Drivers
    # independentes dos clusters atuais (premio de termo, demanda de pensoes
    # por duration, hedge de convexity de MBS). Poucos sinais/ano, mas cada um
    # de alta conviccao. Reversao mais lenta => timeout maior que acoes.
    'TLT-IEF':   {'a': 'TLT',       'b': 'IEF',       'book': 'RATES',     'kind': 'bond'},
}


def _env_f(name, default):
    try:
        return float(os.environ.get(name, default))
    except Exception:
        return float(default)


def _conn():
    return pymysql.connect(
        host=os.environ['MYSQLHOST'], user=os.environ['MYSQLUSER'],
        password=os.environ['MYSQLPASSWORD'], database=os.environ['MYSQLDATABASE'],
        port=int(os.environ.get('MYSQLPORT', 3306)), autocommit=True)


def create_tables():
    ddl_trades = """CREATE TABLE IF NOT EXISTS crossasset_rv_shadow_trades (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        pair VARCHAR(20) NOT NULL, book VARCHAR(16) NOT NULL, kind VARCHAR(12),
        direction VARCHAR(8) NOT NULL, status VARCHAR(8) NOT NULL DEFAULT 'OPEN',
        opened_day DATE NOT NULL, closed_day DATE NULL,
        entry_z DECIMAL(8,3), exit_z DECIMAL(8,3),
        entry_spread DECIMAL(14,7), exit_spread DECIMAL(14,7),
        beta_entry DECIMAL(10,4),
        price_a_entry DECIMAL(18,6), price_b_entry DECIMAL(18,6),
        price_a_exit DECIMAL(18,6), price_b_exit DECIMAL(18,6),
        notional_a DECIMAL(16,2), notional_b DECIMAL(16,2),
        pnl_gross DECIMAL(16,2), fees DECIMAL(16,2), pnl_net DECIMAL(16,2),
        pnl_pct DECIMAL(10,4),
        close_reason VARCHAR(24), days_held INT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_pair_status (pair, status)) CHARACTER SET utf8mb4"""
    ddl_snap = """CREATE TABLE IF NOT EXISTS crossasset_rv_shadow_snapshots (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        day DATE NOT NULL, pair VARCHAR(20) NOT NULL, book VARCHAR(16),
        beta DECIMAL(10,4), spread DECIMAL(14,7), z DECIMAL(8,3),
        price_a DECIMAL(18,6), price_b DECIMAL(18,6),
        signal_hyp VARCHAR(16), position_open TINYINT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_day_pair (day, pair)) CHARACTER SET utf8mb4"""
    ddl_meta = """CREATE TABLE IF NOT EXISTS crossasset_rv_shadow_meta (
        k VARCHAR(64) PRIMARY KEY, v TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) CHARACTER SET utf8mb4"""
    c = _conn()
    try:
        cur = c.cursor()
        for ddl in (ddl_trades, ddl_snap, ddl_meta):
            cur.execute(ddl)
        # ═══ [07-ago-2026 | decisao Beto] MARCACAO A MERCADO 5min + PROTECAO
        # Aqui o problema era ainda maior que no crypto: este book so reavaliava
        # UMA VEZ POR DIA (barra diaria). Uma posicao podia marcar +4% e devolver
        # tudo sem ninguem ver. Agora marca a cada beat; a protecao apenas
        # REGISTRA o que faria — o book atual continua sendo o controle.
        for col, ddl_col in (
            ('mtm_pnl_pct', 'DECIMAL(10,4) NULL'),
            ('mtm_peak_pct', 'DECIMAL(10,4) NULL'),
            ('mtm_min_pct', 'DECIMAL(10,4) NULL'),
            ('mtm_marks', 'INT DEFAULT 0'),
            ('mtm_updated_at', 'DATETIME NULL'),
            ('prot_armed_at', 'DATETIME NULL'),
            ('prot_closed_at', 'DATETIME NULL'),
            ('prot_pnl_pct', 'DECIMAL(10,4) NULL'),
            ('prot_reason', 'VARCHAR(24) NULL'),
            ('prot2_closed_at', 'DATETIME NULL'),
            ('prot2_pnl_pct', 'DECIMAL(10,4) NULL'),
            ('prot2_reason', 'VARCHAR(24) NULL'),
        ):
            try:
                cur.execute(f"ALTER TABLE crossasset_rv_shadow_trades ADD COLUMN {col} {ddl_col}")
                log.info(f'[CROSSASSET-RV] coluna {col} criada')
            except Exception:
                pass
    finally:
        c.close()


def _last_prices(tickers):
    """Preco corrente dos tickers (ETF/acao e forex) na Polygon, para a
    marcacao a mercado. Snapshot em lote p/ acoes; forex em chamada propria.
    Retorna {ticker: preco}. Fail-open: o que nao vier, nao marca."""
    key = os.environ.get('POLYGON_API_KEY', '')
    if not key:
        return {}
    out = {}
    acoes = [t for t in tickers if not t.startswith('C:')]
    fx = [t for t in tickers if t.startswith('C:')]
    try:
        if acoes:
            r = requests.get(
                'https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers',
                params={'tickers': ','.join(acoes), 'apiKey': key}, timeout=12)
            for t in ((r.json() or {}).get('tickers') or []):
                p = ((t.get('lastTrade') or {}).get('p')
                     or (t.get('day') or {}).get('c')
                     or (t.get('prevDay') or {}).get('c'))
                if t.get('ticker') and p:
                    out[t['ticker']] = float(p)
    except Exception as e:
        log.debug(f'[CROSSASSET-RV] snapshot acoes: {e}')
    for t in fx:
        try:
            par = t[2:]
            r = requests.get(
                f'https://api.polygon.io/v1/last_quote/currency/{par[:3]}/{par[3:]}',
                params={'apiKey': key}, timeout=10)
            j = r.json() or {}
            bid = (j.get('last') or {}).get('bid')
            ask = (j.get('last') or {}).get('ask')
            if bid and ask:
                out[t] = (float(bid) + float(ask)) / 2.0
        except Exception as e:
            log.debug(f'[CROSSASSET-RV] fx {t}: {e}')
    return out


def mark_open_positions():
    """[07-ago | Beto] Igual ao crypto RV: marca a mercado as posicoes abertas
    a cada beat e avalia as duas variantes de protecao de lucro, SEM fechar
    nada. Envs compartilhadas com o crypto (RV_PROTECT_*)."""
    if os.environ.get('RV_PROTECT_ENABLED', 'true').lower() == 'false':
        return None
    arm = _env_f('RV_PROTECT_ARM_PCT', 1.5)
    floor = _env_f('RV_PROTECT_FLOOR_PCT', 0.0)
    give = _env_f('RV_PROTECT_GIVEBACK_PCT', 1.5)

    c = _conn()
    try:
        cur = c.cursor()
        cur.execute("SELECT id, pair, direction, entry_spread, beta_entry, "
                    "notional_a, mtm_peak_pct, mtm_min_pct, mtm_marks, "
                    "prot_armed_at, prot_closed_at, prot2_closed_at "
                    "FROM crossasset_rv_shadow_trades WHERE status='OPEN'")
        rows = cur.fetchall()
        if not rows:
            return 0
        tick = set()
        for r in rows:
            cfg = PAIRS.get(r[1])
            if cfg:
                tick.add(cfg['a']); tick.add(cfg['b'])
        px = _last_prices(tick)
        if not px:
            return 0

        agora = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        n = 0
        for (tid, pair, direction, espread, beta_e, na,
             peak, vale, marks, armed_at, prot_cl, prot2_cl) in rows:
            cfg = PAIRS.get(pair)
            if not cfg or cfg['a'] not in px or cfg['b'] not in px:
                continue
            pa, pb = px[cfg['a']], px[cfg['b']]
            if pa <= 0 or pb <= 0 or not na:
                continue
            sx = math.log(pa) - float(beta_e) * math.log(pb)
            sign = -1.0 if direction == 'SHORT_A' else 1.0
            pnl_pct = sign * (sx - float(espread)) * 100.0

            peak = max(float(peak) if peak is not None else pnl_pct, pnl_pct)
            vale = min(float(vale) if vale is not None else pnl_pct, pnl_pct)
            sets = ["mtm_pnl_pct=%s", "mtm_peak_pct=%s", "mtm_min_pct=%s",
                    "mtm_marks=%s", "mtm_updated_at=%s"]
            vals = [round(pnl_pct, 4), round(peak, 4), round(vale, 4),
                    int(marks or 0) + 1, agora]

            if not armed_at and pnl_pct >= arm:
                sets.append("prot_armed_at=%s"); vals.append(agora)
                armed_at = agora
                log.info(f'[XRV-PROT] #{tid} {pair}: ARMADA em {pnl_pct:+.2f}%')

            if armed_at:
                if not prot_cl and pnl_pct <= floor:
                    sets += ["prot_closed_at=%s", "prot_pnl_pct=%s", "prot_reason=%s"]
                    vals += [agora, round(pnl_pct, 4), 'PROTECAO_PISO']
                    log.info(f'[XRV-PROT] #{tid} {pair}: PROT-1 fecharia em '
                             f'{pnl_pct:+.2f}% (pico {peak:+.2f}%)')
                if not prot2_cl and (peak - pnl_pct) >= give:
                    sets += ["prot2_closed_at=%s", "prot2_pnl_pct=%s", "prot2_reason=%s"]
                    vals += [agora, round(pnl_pct, 4), 'PROTECAO_DEVOLUCAO']
                    log.info(f'[XRV-PROT] #{tid} {pair}: PROT-2 fecharia em '
                             f'{pnl_pct:+.2f}%')

            vals.append(tid)
            cur.execute(f"UPDATE crossasset_rv_shadow_trades SET {', '.join(sets)} "
                        f"WHERE id=%s", vals)
            n += 1
        return n
    except Exception as e:
        log.error(f'[CROSSASSET-RV] marcacao falhou: {e}')
        return None
    finally:
        c.close()


def _meta_get(cur, k, default=None):
    cur.execute("SELECT v FROM crossasset_rv_shadow_meta WHERE k=%s", (k,))
    r = cur.fetchone()
    return r[0] if r else default


def _meta_set(cur, k, v):
    cur.execute("INSERT INTO crossasset_rv_shadow_meta (k,v) VALUES (%s,%s) "
                "ON DUPLICATE KEY UPDATE v=VALUES(v)", (k, str(v)))


def _fetch_daily(ticker, days=460):
    """Fecha diarios do Polygon: {date_str: close}. Aceita ETF ('GLD') e
    forex ('C:EURUSD')."""
    end = datetime.utcnow().strftime('%Y-%m-%d')
    start = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')
    url = (f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/'
           f'{start}/{end}')
    for _ in range(3):
        try:
            rc = requests.get(url, params={
                'apiKey': os.environ.get('POLYGON_API_KEY', ''),
                'adjusted': 'true', 'sort': 'asc', 'limit': 500}, timeout=15)
            if rc.status_code == 200 and rc.json().get('results'):
                return {datetime.fromtimestamp(b['t'] / 1000, tz=timezone.utc)
                        .strftime('%Y-%m-%d'): float(b['c'])
                        for b in rc.json()['results']}
        except Exception as e:
            log.debug(f'[CROSSASSET-RV] fetch {ticker}: {e}')
        time.sleep(2)
    return None


def _ols_beta(la, lb):
    n = len(la)
    mb = sum(lb) / n
    ma = sum(la) / n
    cov = sum((lb[i] - mb) * (la[i] - ma) for i in range(n))
    var = sum((x - mb) ** 2 for x in lb) or 1e-12
    return cov / var


def _compute_pair(da, db_, beta_window, z_window):
    days = sorted(set(da) & set(db_))
    if len(days) < z_window + 5:
        return None
    w = days[-int(beta_window):] if len(days) >= beta_window else days
    la = [math.log(da[d]) for d in w]
    lb = [math.log(db_[d]) for d in w]
    beta = _ols_beta(la, lb)
    spread = [math.log(da[d]) - beta * math.log(db_[d]) for d in days]
    hist = spread[-(z_window + 1):-1]
    m = sum(hist) / len(hist)
    sd = math.sqrt(sum((x - m) ** 2 for x in hist) / len(hist)) or 1e-9
    z = (spread[-1] - m) / sd
    d_last = days[-1]
    return {'day': d_last, 'beta': round(beta, 4), 'spread': spread[-1],
            'z': z, 'price_a': da[d_last], 'price_b': db_[d_last],
            'n_days': len(days)}


def scan_once():
    """Um ciclo. Processa apenas quando surge um dia novo (dedup)."""
    z_entry = _env_f('CROSSASSET_RV_Z_ENTRY', 2.0)
    z_exit = _env_f('CROSSASSET_RV_Z_EXIT', 0.5)
    z_stop = _env_f('CROSSASSET_RV_Z_STOP', 3.5)
    timeout_d = int(_env_f('CROSSASSET_RV_TIMEOUT_DAYS', 30))
    capital = _env_f('CROSSASSET_RV_SHADOW_CAPITAL', 1000000)
    pos_pct = _env_f('CROSSASSET_RV_POSITION_PCT', 50)
    cost_bps = _env_f('CROSSASSET_RV_FEE_BPS', 0)  # regra Beto: sem taxa
    beta_window = int(_env_f('CROSSASSET_RV_BETA_WINDOW', 120))
    z_window = int(_env_f('CROSSASSET_RV_Z_WINDOW', 60))

    tickers = sorted({t for p in PAIRS.values() for t in (p['a'], p['b'])})
    series = {}
    for t in tickers:
        s = _fetch_daily(t)
        if s:
            series[t] = s
        time.sleep(0.2)
    missing = [t for t in tickers if t not in series]
    if missing:
        # [28-jul] Antes abortava o scan INTEIRO se qualquer ticker faltasse — um
        # par novo com ticker ruim derrubaria os que funcionam. Agora so pula os
        # pares afetados e segue com o resto (fail-safe granular).
        log.warning(f'[CROSSASSET-RV] sem dados para {missing} — pares afetados serao pulados')
    if len(series) < 2:
        log.warning('[CROSSASSET-RV] dados insuficientes (<2 tickers) — scan abortado')
        return None

    results = {}
    for pair, cfg in PAIRS.items():
        if cfg['a'] not in series or cfg['b'] not in series:
            continue  # par sem um dos lados — pula, nao aborta o scan
        r = _compute_pair(series[cfg['a']], series[cfg['b']], beta_window, z_window)
        if r:
            results[pair] = r
    if not results:
        return None
    day = max(r['day'] for r in results.values())

    c = _conn()
    try:
        cur = c.cursor()
        last = _meta_get(cur, 'last_scan_day')
        if last and str(last) >= day:
            return None  # sem dia novo

        cur.execute("SELECT id, pair, direction, opened_day, entry_z, entry_spread, "
                    "beta_entry, price_a_entry, price_b_entry, notional_a, notional_b, "
                    "days_held FROM crossasset_rv_shadow_trades WHERE status='OPEN'")
        open_rows = cur.fetchall()
        open_by_pair = {r[1]: r for r in open_rows}

        n_closed = n_opened = 0
        open_now = set(open_by_pair)

        for pair, r in sorted(results.items()):
            cfg = PAIRS[pair]
            book, kind = cfg['book'], cfg['kind']
            z, spread = r['z'], r['spread']
            sig = ('ENTRY_SHORT_A' if z >= z_entry else
                   ('ENTRY_LONG_A' if z <= -z_entry else
                    ('EXIT_ZONE' if abs(z) <= z_exit else 'HOLD')))
            if abs(z) >= z_stop:
                sig = 'STOP_ZONE'

            if pair in open_by_pair:
                (tid, _, direction, opened_d, ez, espread, beta_e, pa_e, pb_e,
                 na, nb, held) = open_by_pair[pair]
                held = int(held or 0) + 1
                reason = None
                if abs(z) <= z_exit:
                    reason = 'CONVERGED'
                elif abs(z) >= z_stop:
                    reason = 'STOP_REGIME'
                elif held >= timeout_d:
                    reason = 'TIMEOUT'
                if reason:
                    spread_e_beta = float(espread)
                    spread_x_beta = (math.log(r['price_a']) -
                                     float(beta_e) * math.log(r['price_b']))
                    dlt = spread_x_beta - spread_e_beta
                    sign = -1.0 if direction == 'SHORT_A' else 1.0
                    pnl_gross = sign * dlt * float(na)
                    fees = (cost_bps / 10000.0) * (float(na) + abs(float(nb))) * 2
                    pnl_net = pnl_gross - fees
                    pnl_pct = (pnl_net / float(na) * 100.0) if float(na) else 0.0
                    cur.execute(
                        "UPDATE crossasset_rv_shadow_trades SET status='CLOSED', "
                        "closed_day=%s, exit_z=%s, exit_spread=%s, price_a_exit=%s, "
                        "price_b_exit=%s, pnl_gross=%s, fees=%s, pnl_net=%s, pnl_pct=%s, "
                        "close_reason=%s, days_held=%s WHERE id=%s",
                        (day, round(z, 3), round(spread_x_beta, 7), r['price_a'],
                         r['price_b'], round(pnl_gross, 2), round(fees, 2),
                         round(pnl_net, 2), round(pnl_pct, 4), reason, held, tid))
                    n_closed += 1
                    del open_by_pair[pair]
                    open_now.discard(pair)
                    log.info(f'[CROSSASSET-RV] CLOSE {pair} {direction} {reason} '
                             f'z={z:+.2f} pnl=${pnl_net:.2f} ({pnl_pct:+.2f}%, {held}d)')
                    # [03-ago, decisao Beto] espelha o fechamento no IB paper.
                    # Fail-open: corretora fora do ar nao afeta o book shadow.
                    try:
                        from modules.ib_exec import exec_crossasset
                        exec_crossasset(pair, cfg.get('a'), cfg.get('b'), direction,
                                        r['price_a'], r['price_b'],
                                        float(na or 0), float(nb or 0),
                                        'CLOSE', ref_id=str(tid))
                    except Exception as _xe:
                        log.debug(f'[CROSSASSET-RV] exec CLOSE {pair}: {_xe}')
                else:
                    cur.execute("UPDATE crossasset_rv_shadow_trades SET days_held=%s "
                                "WHERE id=%s", (held, tid))

            elif sig in ('ENTRY_SHORT_A', 'ENTRY_LONG_A') and abs(z) < z_stop:
                na = capital * pos_pct / 100.0
                nb = na * abs(r['beta'])
                direction = 'SHORT_A' if sig == 'ENTRY_SHORT_A' else 'LONG_A'
                cur.execute(
                    "INSERT INTO crossasset_rv_shadow_trades (pair, book, kind, direction, "
                    "status, opened_day, entry_z, entry_spread, beta_entry, "
                    "price_a_entry, price_b_entry, notional_a, notional_b) "
                    "VALUES (%s,%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s,%s)",
                    (pair, book, kind, direction, day, round(z, 3),
                     round(spread, 7), r['beta'], r['price_a'], r['price_b'],
                     round(na, 2), round(nb, 2)))
                n_opened += 1
                open_now.add(pair)
                log.info(f'[CROSSASSET-RV] OPEN {pair} {direction} z={z:+.2f} '
                         f'beta={r["beta"]} notional=${na:.0f}/${nb:.0f}')
                # [03-ago, decisao Beto] espelha a abertura no IB paper (2 pernas,
                # ETFs SMART/USD, tif=OPG). Pares de FX sao registrados como
                # SKIPPED_FX (exigem IDEALPRO). Fail-open.
                try:
                    from modules.ib_exec import exec_crossasset
                    exec_crossasset(pair, cfg.get('a'), cfg.get('b'), direction,
                                    r['price_a'], r['price_b'], na, nb,
                                    'OPEN', ref_id=str(cur.lastrowid))
                except Exception as _xe:
                    log.debug(f'[CROSSASSET-RV] exec OPEN {pair}: {_xe}')

            cur.execute(
                "INSERT IGNORE INTO crossasset_rv_shadow_snapshots (day, pair, book, "
                "beta, spread, z, price_a, price_b, signal_hyp, position_open) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (day, pair, book, r['beta'], round(spread, 7), round(z, 3),
                 r['price_a'], r['price_b'], sig, 1 if pair in open_now else 0))

        _meta_set(cur, 'last_scan_day', day)
        _meta_set(cur, 'last_scan_at', datetime.utcnow().isoformat())
        summary = {'day': day, 'pairs': len(results),
                   'opened': n_opened, 'closed': n_closed,
                   'open_total': len(open_now)}
        log.info(f'[CROSSASSET-RV] scan {day}: {len(results)} par(es), '
                 f'{n_opened} abertura(s), {n_closed} fechamento(s), '
                 f'{summary["open_total"]} aberta(s)')
        return summary
    finally:
        c.close()


def crossasset_rv_shadow_loop(beat_fn=None):
    """Loop: tenta scan a cada 30min; scan_once deduplica por dia."""
    if os.environ.get('CROSSASSET_RV_SHADOW_ENABLED', 'true').lower() == 'false':
        log.info('[CROSSASSET-RV] shadow desabilitado via env')
        return
    try:
        create_tables()
        log.info(f'[CROSSASSET-RV] motor shadow iniciado — {len(PAIRS)} par(es) '
                 f'commodities+FX (book ${_env_f("CROSSASSET_RV_SHADOW_CAPITAL", 1000000):,.0f})')
    except Exception as e:
        log.error(f'[CROSSASSET-RV] falha ao criar tabelas: {e}')
        return
    while True:
        try:
            if beat_fn:
                beat_fn('crossasset_rv_shadow_loop')
            # [07-ago | Beto] marcacao a mercado a cada beat. Antes este book
            # so olhava as posicoes UMA VEZ POR DIA. Fail-open.
            try:
                mark_open_positions()
            except Exception as _me:
                log.error(f'[CROSSASSET-RV] marcacao: {_me}')
            scan_once()
        except Exception as e:
            log.error(f'[CROSSASSET-RV] erro no loop: {e}')
        time.sleep(300)  # [07-ago] 30min -> 5min: marcacao a mercado das
        # posicoes abertas. scan_once continua deduplicando por dia, entao a
        # logica de entrada/saida por z segue exatamente igual (1x/dia).
