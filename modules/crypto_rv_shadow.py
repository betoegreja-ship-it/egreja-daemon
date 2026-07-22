# -*- coding: utf-8 -*-
"""[22-jul-2026] MOTOR CRYPTO RELATIVE VALUE — MODO SHADOW (paper, book separado).

Motor SEPARADO do motor direcional de cripto. Opera valor relativo entre
criptos altamente correlacionadas (comeca com ETH-BTC), na mesma logica da
Arbi/US-Pairs: nao aposta em direcao, aposta que o spread entre os dois
ativos volta a media. Neutro a mercado.

Par inicial (A=ETH, B=BTC):
  - Beta OLS log-price (janela 120 barras de 4h)
  - z do spread sobre janela 60 barras
  - Entrada |z| >= 2.0  (SHORT_A se z>=+2 -> ETH caro: vende ETH/compra BTC;
                          LONG_A  se z<=-2 -> ETH barato: compra ETH/vende BTC)
  - Saida   |z| <= 0.5  (CONVERGED)
  - Stop    |z| >= 3.5  (STOP_REGIME)
  - Timeout 42 barras (~7 dias)
  - Barras de 4h (mercado 24/7). Dados: mirror publico da Binance.

Calibracao (180d ETHBTC + varredura 4h/6h/12h/1d, 22-jul-2026):
  edge robusto e POSITIVO em todos os timeframes; 4h/janela60/z2.0 ~1.3
  trade/semana, market-neutral, +12.6% de spread no periodo testado.

SHADOW: nao envia ordem, nao toca capital real, nao mexe na Arbi nem no
motor direcional. Tabelas proprias (crypto_rv_shadow_*). SEM taxa
(regra Beto: nao descontar taxa de nenhuma estrategia).

Envs:
  CRYPTO_RV_SHADOW_ENABLED (true)  CRYPTO_RV_SHADOW_CAPITAL (1000000)
  CRYPTO_RV_POSITION_PCT   (50)    CRYPTO_RV_FEE_BPS        (0)
  CRYPTO_RV_INTERVAL       (4h)    CRYPTO_RV_BETA_WINDOW    (120)
  CRYPTO_RV_Z_WINDOW       (60)    CRYPTO_RV_Z_ENTRY (2.0)
  CRYPTO_RV_Z_EXIT (0.5)  CRYPTO_RV_Z_STOP (3.5)  CRYPTO_RV_TIMEOUT_BARS (42)
"""
import os, math, time, logging
from datetime import datetime

import requests
import pymysql

log = logging.getLogger('egreja.crypto.rv.shadow')

# A-B: A e o "numerador", B o "denominador". Pares aprovados no screen de
# edge de 22-jul-2026 (RV z-score 4h, janela 60, entrada 2.0). Somente pares
# com soma de spread POSITIVA e WR>=55% entraram — os reprovados (LTC-BCH,
# NEAR-SOL, SOL-BNB, XRP-XLM, etc.) foram descartados de proposito.
PAIRS = {
    'ETH-BTC':  {'a': 'ETHUSDT',  'b': 'BTCUSDT',  'book': 'CORE_MAJORS'},   # +7.7% WR62
    'BNB-BTC':  {'a': 'BNBUSDT',  'b': 'BTCUSDT',  'book': 'CORE_MAJORS'},   # +15.4% WR64
    'UNI-AAVE': {'a': 'UNIUSDT',  'b': 'AAVEUSDT', 'book': 'DEFI'},          # +12.8% WR55
}

_BINANCE_HOSTS = [
    'https://data-api.binance.vision',
    'https://api.binance.com',
]


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
    ddl_trades = """CREATE TABLE IF NOT EXISTS crypto_rv_shadow_trades (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        pair VARCHAR(16) NOT NULL, book VARCHAR(16) NOT NULL,
        direction VARCHAR(8) NOT NULL, status VARCHAR(8) NOT NULL DEFAULT 'OPEN',
        opened_bar DATETIME NOT NULL, closed_bar DATETIME NULL,
        entry_z DECIMAL(8,3), exit_z DECIMAL(8,3),
        entry_spread DECIMAL(14,7), exit_spread DECIMAL(14,7),
        beta_entry DECIMAL(10,4),
        price_a_entry DECIMAL(18,6), price_b_entry DECIMAL(18,6),
        price_a_exit DECIMAL(18,6), price_b_exit DECIMAL(18,6),
        notional_a DECIMAL(16,2), notional_b DECIMAL(16,2),
        pnl_gross DECIMAL(16,2), fees DECIMAL(16,2), pnl_net DECIMAL(16,2),
        pnl_pct DECIMAL(10,4),
        close_reason VARCHAR(24), bars_held INT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_pair_status (pair, status)) CHARACTER SET utf8mb4"""
    ddl_snap = """CREATE TABLE IF NOT EXISTS crypto_rv_shadow_snapshots (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        bar DATETIME NOT NULL, pair VARCHAR(16) NOT NULL, book VARCHAR(16),
        beta DECIMAL(10,4), spread DECIMAL(14,7), z DECIMAL(8,3),
        price_a DECIMAL(18,6), price_b DECIMAL(18,6),
        signal_hyp VARCHAR(16), position_open TINYINT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_bar_pair (bar, pair)) CHARACTER SET utf8mb4"""
    ddl_meta = """CREATE TABLE IF NOT EXISTS crypto_rv_shadow_meta (
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
    cur.execute("SELECT v FROM crypto_rv_shadow_meta WHERE k=%s", (k,))
    r = cur.fetchone()
    return r[0] if r else default


def _meta_set(cur, k, v):
    cur.execute("INSERT INTO crypto_rv_shadow_meta (k,v) VALUES (%s,%s) "
                "ON DUPLICATE KEY UPDATE v=VALUES(v)", (k, str(v)))


def _fetch_klines(symbol, interval, limit=300):
    """Klines fechados: [(open_time_ms, close_float), ...]. Descarta a barra
    em formacao (ultimo elemento). Tenta mirrors publicos da Binance."""
    for host in _BINANCE_HOSTS:
        try:
            rc = requests.get(f'{host}/api/v3/klines',
                              params={'symbol': symbol, 'interval': interval,
                                      'limit': limit},
                              headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
            if rc.status_code == 200:
                k = rc.json()
                if isinstance(k, list) and len(k) > 2:
                    return [(int(x[0]), float(x[4])) for x in k[:-1]]  # dropa forming
        except Exception as e:
            log.debug(f'[CRYPTO-RV] fetch {symbol}@{host}: {e}')
    return None


def _ols_beta(la, lb):
    n = len(la)
    mb = sum(lb) / n
    ma = sum(la) / n
    cov = sum((lb[i] - mb) * (la[i] - ma) for i in range(n))
    var = sum((x - mb) ** 2 for x in lb) or 1e-12
    return cov / var


def _compute_pair(series_a, series_b, beta_window, z_window):
    """series_*: [(open_time_ms, close)]. Beta rolling + spread + z da ultima
    barra fechada comum."""
    da, db_ = dict(series_a), dict(series_b)
    bars = sorted(set(da) & set(db_))
    if len(bars) < z_window + 5:
        return None
    w = bars[-int(beta_window):] if len(bars) >= beta_window else bars
    la = [math.log(da[t]) for t in w]
    lb = [math.log(db_[t]) for t in w]
    beta = _ols_beta(la, lb)
    spread = [math.log(da[t]) - beta * math.log(db_[t]) for t in bars]
    hist = spread[-(z_window + 1):-1]
    m = sum(hist) / len(hist)
    sd = math.sqrt(sum((x - m) ** 2 for x in hist) / len(hist)) or 1e-9
    z = (spread[-1] - m) / sd
    t_last = bars[-1]
    return {'bar_ms': t_last, 'beta': round(beta, 4), 'spread': spread[-1],
            'z': z, 'price_a': da[t_last], 'price_b': db_[t_last],
            'n_bars': len(bars)}


def scan_once():
    """Um ciclo. Processa apenas quando surge uma barra 4h nova (dedup)."""
    interval = os.environ.get('CRYPTO_RV_INTERVAL', '4h')
    z_entry = _env_f('CRYPTO_RV_Z_ENTRY', 2.0)
    z_exit = _env_f('CRYPTO_RV_Z_EXIT', 0.5)
    z_stop = _env_f('CRYPTO_RV_Z_STOP', 3.5)
    timeout_b = int(_env_f('CRYPTO_RV_TIMEOUT_BARS', 42))
    capital = _env_f('CRYPTO_RV_SHADOW_CAPITAL', 1000000)
    pos_pct = _env_f('CRYPTO_RV_POSITION_PCT', 50)
    cost_bps = _env_f('CRYPTO_RV_FEE_BPS', 0)  # regra Beto: sem taxa
    beta_window = int(_env_f('CRYPTO_RV_BETA_WINDOW', 120))
    z_window = int(_env_f('CRYPTO_RV_Z_WINDOW', 60))

    # coleta series de todos os simbolos envolvidos
    symbols = sorted({s for p in PAIRS.values() for s in (p['a'], p['b'])})
    series = {}
    for sym in symbols:
        s = _fetch_klines(sym, interval, limit=max(beta_window, z_window) + 60)
        if s:
            series[sym] = s
        time.sleep(0.1)
    missing = [s for s in symbols if s not in series]
    if missing:
        log.warning(f'[CRYPTO-RV] sem dados para {missing} — scan abortado (fail-safe)')
        return None

    results = {}
    for pair, cfg in PAIRS.items():
        r = _compute_pair(series[cfg['a']], series[cfg['b']], beta_window, z_window)
        if r:
            results[pair] = r
    if not results:
        return None
    bar_ms = max(r['bar_ms'] for r in results.values())
    bar_dt = datetime.utcfromtimestamp(bar_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

    c = _conn()
    try:
        cur = c.cursor()
        last = _meta_get(cur, 'last_scan_bar_ms')
        if last and int(last) >= bar_ms:
            return None  # sem barra nova

        cur.execute("SELECT id, pair, direction, opened_bar, entry_z, entry_spread, "
                    "beta_entry, price_a_entry, price_b_entry, notional_a, notional_b, "
                    "bars_held FROM crypto_rv_shadow_trades WHERE status='OPEN'")
        open_rows = cur.fetchall()
        open_by_pair = {r[1]: r for r in open_rows}

        n_closed = n_opened = 0
        open_now = set(open_by_pair)

        for pair, r in sorted(results.items()):
            cfg = PAIRS[pair]
            book = cfg['book']
            z, spread = r['z'], r['spread']
            sig = ('ENTRY_SHORT_A' if z >= z_entry else
                   ('ENTRY_LONG_A' if z <= -z_entry else
                    ('EXIT_ZONE' if abs(z) <= z_exit else 'HOLD')))
            if abs(z) >= z_stop:
                sig = 'STOP_ZONE'

            # ── fechar posicao aberta?
            if pair in open_by_pair:
                (tid, _, direction, opened_b, ez, espread, beta_e, pa_e, pb_e,
                 na, nb, held) = open_by_pair[pair]
                held = int(held or 0) + 1
                reason = None
                if abs(z) <= z_exit:
                    reason = 'CONVERGED'
                elif abs(z) >= z_stop:
                    reason = 'STOP_REGIME'
                elif held >= timeout_b:
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
                        "UPDATE crypto_rv_shadow_trades SET status='CLOSED', "
                        "closed_bar=%s, exit_z=%s, exit_spread=%s, price_a_exit=%s, "
                        "price_b_exit=%s, pnl_gross=%s, fees=%s, pnl_net=%s, pnl_pct=%s, "
                        "close_reason=%s, bars_held=%s WHERE id=%s",
                        (bar_dt, round(z, 3), round(spread_x_beta, 7), r['price_a'],
                         r['price_b'], round(pnl_gross, 2), round(fees, 2),
                         round(pnl_net, 2), round(pnl_pct, 4), reason, held, tid))
                    n_closed += 1
                    del open_by_pair[pair]
                    open_now.discard(pair)
                    log.info(f'[CRYPTO-RV] CLOSE {pair} {direction} {reason} '
                             f'z={z:+.2f} pnl=${pnl_net:.2f} ({pnl_pct:+.2f}%, {held} barras)')
                else:
                    cur.execute("UPDATE crypto_rv_shadow_trades SET bars_held=%s "
                                "WHERE id=%s", (held, tid))

            # ── abrir nova? (sem posicao no par)
            elif sig in ('ENTRY_SHORT_A', 'ENTRY_LONG_A') and abs(z) < z_stop:
                na = capital * pos_pct / 100.0
                nb = na * abs(r['beta'])
                direction = 'SHORT_A' if sig == 'ENTRY_SHORT_A' else 'LONG_A'
                cur.execute(
                    "INSERT INTO crypto_rv_shadow_trades (pair, book, direction, "
                    "status, opened_bar, entry_z, entry_spread, beta_entry, "
                    "price_a_entry, price_b_entry, notional_a, notional_b) "
                    "VALUES (%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s,%s)",
                    (pair, book, direction, bar_dt, round(z, 3),
                     round(spread, 7), r['beta'], r['price_a'], r['price_b'],
                     round(na, 2), round(nb, 2)))
                n_opened += 1
                open_now.add(pair)
                log.info(f'[CRYPTO-RV] OPEN {pair} {direction} z={z:+.2f} '
                         f'beta={r["beta"]} notional=${na:.0f}/${nb:.0f}')

            cur.execute(
                "INSERT IGNORE INTO crypto_rv_shadow_snapshots (bar, pair, book, "
                "beta, spread, z, price_a, price_b, signal_hyp, position_open) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (bar_dt, pair, book, r['beta'], round(spread, 7), round(z, 3),
                 r['price_a'], r['price_b'], sig, 1 if pair in open_now else 0))

        _meta_set(cur, 'last_scan_bar_ms', bar_ms)
        _meta_set(cur, 'last_scan_bar', bar_dt)
        _meta_set(cur, 'last_scan_at', datetime.utcnow().isoformat())
        summary = {'bar': bar_dt, 'pairs': len(results),
                   'opened': n_opened, 'closed': n_closed,
                   'open_total': len(open_now)}
        log.info(f'[CRYPTO-RV] scan {bar_dt}: {len(results)} par(es), '
                 f'{n_opened} abertura(s), {n_closed} fechamento(s), '
                 f'{summary["open_total"]} aberta(s)')
        return summary
    finally:
        c.close()


def crypto_rv_shadow_loop(beat_fn=None):
    """Loop 24/7: tenta scan a cada 5min; scan_once deduplica por barra 4h."""
    if os.environ.get('CRYPTO_RV_SHADOW_ENABLED', 'true').lower() == 'false':
        log.info('[CRYPTO-RV] shadow desabilitado via env')
        return
    try:
        create_tables()
        log.info(f'[CRYPTO-RV] motor shadow iniciado — {len(PAIRS)} par(es) '
                 f'(book nocional ${_env_f("CRYPTO_RV_SHADOW_CAPITAL", 1000000):,.0f})')
    except Exception as e:
        log.error(f'[CRYPTO-RV] falha ao criar tabelas: {e}')
        return
    while True:
        try:
            if beat_fn:
                beat_fn('crypto_rv_shadow_loop')
            scan_once()
        except Exception as e:
            log.error(f'[CRYPTO-RV] erro no loop: {e}')
        time.sleep(300)  # 5min — scan_once deduplica por barra
