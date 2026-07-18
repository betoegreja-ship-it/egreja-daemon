# -*- coding: utf-8 -*-
"""[P2 10-jul-2026] MOTOR US PAIRS — MODO SHADOW (paper, capital separado).

Motor SEPARADO do pairs_engine B3. Opera os pares NYSE aprovados no
screening OOS de 06/jul/2026 (formacao ate dez/25, validacao jan-jul/26).

Universo (14 pares):
  CORE_FIN  : WFC-COF, SCHW-COF, JPM-SCHW, WFC-SCHW          (operaveis)
  CORE_SECT : CI-ELV, KLAC-STX, ADI-STX, COP-OXY, MAR-DAL,
              UNP-UPS                                          (operaveis)
  WATCH     : NOW-PYPL, SHOP-COF, BLK-CCL, MDLZ-AMT           (so observa)

Regras (aprovadas na revisao de 10/jul com o Beto):
  - Entrada |z| >= 2.0 (SHORT_A se z>=+2, LONG_A se z<=-2)
  - Saida   |z| <= 0.4 (CONVERGED)
  - Stop    |z| >= 3.5 (STOP_REGIME — regime pode ter quebrado)
  - Timeout 25 pregoes
  - Max 2 pares abertos por papel
  - Beta REAVALIADO diariamente (OLS log-price, janela 120 pregoes)
  - z sobre janela 60 pregoes (identico ao uspairs_watch.py)
  - Capital paper USD separado; custos fee+slippage por perna
  - Cadencia DIARIA: roda apos o fechamento NYSE (bar diario Polygon)

SHADOW: nao envia ordem, nao toca capital real. Tabelas proprias
(uspairs_shadow_*). Aprendizado proprio futuro — NAO usa brain_* nem
pairs_engine B3 (sem contaminacao cross-strategy).

Envs:
  USPAIRS_SHADOW_ENABLED   (true)   USPAIRS_SHADOW_CAPITAL (100000 USD)
  USPAIRS_POSITION_PCT     (10)     USPAIRS_FEE_BPS        (2 por perna/lado)
  USPAIRS_SLIP_BPS         (3)      USPAIRS_BETA_WINDOW    (120)
  USPAIRS_Z_ENTRY (2.0) | USPAIRS_Z_EXIT (0.4) | USPAIRS_Z_STOP (3.5)
  USPAIRS_TIMEOUT_PREGOES  (25)     USPAIRS_MAX_PER_SYMBOL (2)
"""
import os, json, math, time, logging, threading
from datetime import datetime, timedelta, timezone

import requests
import pymysql

log = logging.getLogger('egreja.uspairs.shadow')

PAIRS = {
    'WFC-COF':  'CORE_FIN', 'SCHW-COF': 'CORE_FIN', 'JPM-SCHW': 'CORE_FIN',
    'WFC-SCHW': 'CORE_FIN',
    'CI-ELV':   'CORE_SECT', 'KLAC-STX': 'CORE_SECT', 'ADI-STX': 'CORE_SECT',
    'COP-OXY':  'CORE_SECT', 'MAR-DAL':  'CORE_SECT', 'UNP-UPS':  'CORE_SECT',
    'NOW-PYPL': 'WATCH', 'SHOP-COF': 'WATCH', 'BLK-CCL': 'WATCH', 'MDLZ-AMT': 'WATCH',
}
OPERABLE = {p for p, b in PAIRS.items() if b != 'WATCH'}
TICKERS = sorted({t for p in PAIRS for t in p.split('-')})


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
    ddl_trades = """CREATE TABLE IF NOT EXISTS uspairs_shadow_trades (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        pair VARCHAR(16) NOT NULL, book VARCHAR(16) NOT NULL,
        direction VARCHAR(8) NOT NULL, status VARCHAR(8) NOT NULL DEFAULT 'OPEN',
        opened_pregao DATE NOT NULL, closed_pregao DATE NULL,
        entry_z DECIMAL(8,3), exit_z DECIMAL(8,3),
        entry_spread DECIMAL(12,5), exit_spread DECIMAL(12,5),
        beta_entry DECIMAL(10,4),
        price_a_entry DECIMAL(12,4), price_b_entry DECIMAL(12,4),
        price_a_exit DECIMAL(12,4), price_b_exit DECIMAL(12,4),
        notional_a DECIMAL(14,2), notional_b DECIMAL(14,2),
        pnl_gross DECIMAL(14,2), fees DECIMAL(14,2), pnl_net DECIMAL(14,2),
        close_reason VARCHAR(24), pregoes_held INT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_pair_status (pair, status)) CHARACTER SET utf8mb4"""
    ddl_snap = """CREATE TABLE IF NOT EXISTS uspairs_shadow_snapshots (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        pregao DATE NOT NULL, pair VARCHAR(16) NOT NULL, book VARCHAR(16),
        beta DECIMAL(10,4), spread DECIMAL(12,5), z DECIMAL(8,3),
        price_a DECIMAL(12,4), price_b DECIMAL(12,4),
        signal_hyp VARCHAR(16), position_open TINYINT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_pregao_pair (pregao, pair)) CHARACTER SET utf8mb4"""
    ddl_meta = """CREATE TABLE IF NOT EXISTS uspairs_shadow_meta (
        k VARCHAR(64) PRIMARY KEY, v TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) CHARACTER SET utf8mb4"""
    c = _conn()
    try:
        cur = c.cursor()
        for ddl in (ddl_trades, ddl_snap, ddl_meta):
            cur.execute(ddl)
    finally:
        c.close()


def _meta_get(cur, k, default=None):
    cur.execute("SELECT v FROM uspairs_shadow_meta WHERE k=%s", (k,))
    r = cur.fetchone()
    return r[0] if r else default


def _meta_set(cur, k, v):
    cur.execute("INSERT INTO uspairs_shadow_meta (k,v) VALUES (%s,%s) "
                "ON DUPLICATE KEY UPDATE v=VALUES(v)", (k, str(v)))


def _fetch_daily(ticker, days=210):
    """Fecha diarios ajustados do Polygon: [(date, close), ...]."""
    end = datetime.utcnow().strftime('%Y-%m-%d')
    start = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')
    url = (f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/'
           f'{start}/{end}')
    for _ in range(3):
        try:
            rc = requests.get(url, params={
                'apiKey': os.environ.get('POLYGON_API_KEY', ''),
                'adjusted': 'true', 'sort': 'asc', 'limit': 260}, timeout=15)
            if rc.status_code == 200 and rc.json().get('results'):
                return [(datetime.fromtimestamp(b['t'] / 1000, tz=timezone.utc)
                         .strftime('%Y-%m-%d'), float(b['c']))
                        for b in rc.json()['results']]
        except Exception as e:
            log.debug(f'[USPAIRS] fetch {ticker}: {e}')
        time.sleep(2)
    return None


def _ols_beta(la, lb):
    """OLS log(A) = alpha + beta*log(B)."""
    n = len(la)
    mb = sum(lb) / n
    ma = sum(la) / n
    cov = sum((lb[i] - mb) * (la[i] - ma) for i in range(n))
    var = sum((x - mb) ** 2 for x in lb) or 1e-12
    return cov / var


def _compute_pair(series_a, series_b, beta_window, z_window=60):
    """Beta rolling + spread + z do ultimo pregao comum."""
    da, db_ = dict(series_a), dict(series_b)
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
    return {'pregao': d_last, 'beta': round(beta, 4), 'spread': spread[-1],
            'z': z, 'price_a': da[d_last], 'price_b': db_[d_last],
            'n_pregoes': len(days)}


def scan_once():
    """Um ciclo diario completo. Retorna resumo ou None se sem pregao novo."""
    z_entry = _env_f('USPAIRS_Z_ENTRY', 2.0)
    z_exit = _env_f('USPAIRS_Z_EXIT', 0.4)
    z_stop = _env_f('USPAIRS_Z_STOP', 3.5)
    timeout_p = int(_env_f('USPAIRS_TIMEOUT_PREGOES', 25))
    max_per_sym = int(_env_f('USPAIRS_MAX_PER_SYMBOL', 2))
    capital = _env_f('USPAIRS_SHADOW_CAPITAL', 100000)
    pos_pct = _env_f('USPAIRS_POSITION_PCT', 10)
    cost_bps = _env_f('USPAIRS_FEE_BPS', 2) + _env_f('USPAIRS_SLIP_BPS', 3)
    beta_window = int(_env_f('USPAIRS_BETA_WINDOW', 120))

    series = {}
    for t in TICKERS:
        s = _fetch_daily(t)
        if s:
            series[t] = s
        time.sleep(0.12)
    missing = [t for t in TICKERS if t not in series]
    if missing:
        log.warning(f'[USPAIRS] sem dados para {missing} — scan abortado (fail-safe)')
        return None

    results = {}
    for pair in PAIRS:
        a, b = pair.split('-')
        r = _compute_pair(series[a], series[b], beta_window)
        if r:
            results[pair] = r
    if not results:
        return None
    pregao = max(r['pregao'] for r in results.values())

    c = _conn()
    try:
        cur = c.cursor()
        last = _meta_get(cur, 'last_scan_pregao')
        if last and str(last) >= pregao:
            return None  # sem pregao novo

        cur.execute("SELECT id, pair, direction, opened_pregao, entry_z, entry_spread, "
                    "beta_entry, price_a_entry, price_b_entry, notional_a, notional_b, "
                    "pregoes_held FROM uspairs_shadow_trades WHERE status='OPEN'")
        open_rows = cur.fetchall()
        open_by_pair = {r[1]: r for r in open_rows}
        sym_count = {}
        for r in open_rows:
            for s in r[1].split('-'):
                sym_count[s] = sym_count.get(s, 0) + 1

        n_closed = n_opened = 0
        open_now = set(open_by_pair)
        reserved = sum(float(r[9] or 0) + abs(float(r[10] or 0)) for r in open_rows)

        for pair, r in sorted(results.items()):
            book = PAIRS[pair]
            z, spread = r['z'], r['spread']
            sig = ('ENTRY_SHORT_A' if z >= z_entry else
                   ('ENTRY_LONG_A' if z <= -z_entry else
                    ('EXIT_ZONE' if abs(z) <= z_exit else 'HOLD')))
            if abs(z) >= z_stop:
                sig = 'STOP_ZONE'

            # ── fechar posicao aberta?
            if pair in open_by_pair:
                (tid, _, direction, opened_pg, ez, espread, beta_e, pa_e, pb_e,
                 na, nb, held) = open_by_pair[pair]
                held = int(held or 0) + 1
                reason = None
                if abs(z) <= z_exit:
                    reason = 'CONVERGED'
                elif abs(z) >= z_stop:
                    reason = 'STOP_REGIME'
                elif held >= timeout_p:
                    reason = 'TIMEOUT'
                if reason:
                    # PnL no espaco log-spread com o beta DE ENTRADA (posicao real)
                    spread_e_beta = float(espread)
                    spread_x_beta = (math.log(r['price_a']) -
                                     float(beta_e) * math.log(r['price_b']))
                    dlt = spread_x_beta - spread_e_beta
                    sign = -1.0 if direction == 'SHORT_A' else 1.0
                    pnl_gross = sign * dlt * float(na)
                    fees = (cost_bps / 10000.0) * (float(na) + abs(float(nb))) * 2
                    pnl_net = pnl_gross - fees
                    cur.execute(
                        "UPDATE uspairs_shadow_trades SET status='CLOSED', "
                        "closed_pregao=%s, exit_z=%s, exit_spread=%s, price_a_exit=%s, "
                        "price_b_exit=%s, pnl_gross=%s, fees=%s, pnl_net=%s, "
                        "close_reason=%s, pregoes_held=%s WHERE id=%s",
                        (pregao, round(z, 3), round(spread_x_beta, 5), r['price_a'],
                         r['price_b'], round(pnl_gross, 2), round(fees, 2),
                         round(pnl_net, 2), reason, held, tid))
                    n_closed += 1
                    reserved -= float(na) + abs(float(nb))
                    for s in pair.split('-'):
                        sym_count[s] = max(0, sym_count.get(s, 1) - 1)
                    del open_by_pair[pair]
                    open_now.discard(pair)
                    log.info(f'[USPAIRS] CLOSE {pair} {direction} {reason} '
                             f'z={z:+.2f} pnl_net=${pnl_net:.2f} ({held} pregoes)')
                else:
                    cur.execute("UPDATE uspairs_shadow_trades SET pregoes_held=%s "
                                "WHERE id=%s", (held, tid))

            # ── abrir nova? (so CORE, sem posicao no par)
            elif (pair in OPERABLE and sig in ('ENTRY_SHORT_A', 'ENTRY_LONG_A')
                  and abs(z) < z_stop):
                a, b = pair.split('-')
                if sym_count.get(a, 0) >= max_per_sym or sym_count.get(b, 0) >= max_per_sym:
                    log.info(f'[USPAIRS] {pair}: entrada vetada — max {max_per_sym} '
                             f'pares por papel')
                else:
                    na = capital * pos_pct / 100.0
                    nb = na * abs(r['beta'])
                    if reserved + na + nb > capital:
                        log.info(f'[USPAIRS] {pair}: entrada vetada — capital paper '
                                 f'esgotado (reservado ${reserved:.0f})')
                    else:
                        direction = 'SHORT_A' if sig == 'ENTRY_SHORT_A' else 'LONG_A'
                        cur.execute(
                            "INSERT INTO uspairs_shadow_trades (pair, book, direction, "
                            "status, opened_pregao, entry_z, entry_spread, beta_entry, "
                            "price_a_entry, price_b_entry, notional_a, notional_b) "
                            "VALUES (%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s,%s)",
                            (pair, book, direction, pregao, round(z, 3),
                             round(spread, 5), r['beta'], r['price_a'], r['price_b'],
                             round(na, 2), round(nb, 2)))
                        n_opened += 1
                        open_now.add(pair)
                        reserved += na + nb
                        for s in pair.split('-'):
                            sym_count[s] = sym_count.get(s, 0) + 1
                        log.info(f'[USPAIRS] OPEN {pair} {direction} z={z:+.2f} '
                                 f'beta={r["beta"]} notional=${na:.0f}/${nb:.0f}')

            cur.execute(
                "INSERT IGNORE INTO uspairs_shadow_snapshots (pregao, pair, book, "
                "beta, spread, z, price_a, price_b, signal_hyp, position_open) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (r['pregao'], pair, book, r['beta'], round(spread, 5), round(z, 3),
                 r['price_a'], r['price_b'], sig, 1 if pair in open_now else 0))

        _meta_set(cur, 'last_scan_pregao', pregao)
        _meta_set(cur, 'last_scan_at', datetime.utcnow().isoformat())
        summary = {'pregao': pregao, 'pairs': len(results),
                   'opened': n_opened, 'closed': n_closed,
                   'open_total': len(open_now)}
        log.info(f'[USPAIRS] scan {pregao}: {len(results)} pares, '
                 f'{n_opened} aberturas, {n_closed} fechamentos, '
                 f'{summary["open_total"]} posicoes abertas')
        return summary
    finally:
        c.close()


def uspairs_shadow_loop(beat_fn=None):
    """Loop diario: tenta scan apos 21:05 UTC em dias uteis (bar diario
    Polygon disponivel pos-fechamento). Deduplicado por last_scan_pregao."""
    if os.environ.get('USPAIRS_SHADOW_ENABLED', 'true').lower() == 'false':
        log.info('[USPAIRS] shadow desabilitado via env')
        return
    try:
        create_tables()
        log.info(f'[USPAIRS] motor shadow iniciado — {len(PAIRS)} pares '
                 f'({len(OPERABLE)} operaveis + {len(PAIRS)-len(OPERABLE)} watch)')
    except Exception as e:
        log.error(f'[USPAIRS] falha ao criar tabelas: {e}')
        return
    while True:
        try:
            if beat_fn:
                beat_fn('uspairs_shadow_loop')
            now = datetime.utcnow()
            # janela de scan: dias uteis 21:05-23:55 UTC (pos-fechamento NYSE)
            # + sabado de manha como catch-up de sexta
            in_window = ((now.weekday() < 5 and now.hour >= 21) or
                         (now.weekday() == 5 and now.hour < 12))
            if in_window:
                scan_once()
        except Exception as e:
            log.error(f'[USPAIRS] erro no loop: {e}')
        time.sleep(900)  # 15min — scan_once deduplica por pregao
