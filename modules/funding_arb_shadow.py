# -*- coding: utf-8 -*-
"""[25-jul-2026, decisao Beto] FUNDING-ARB — arbitragem de taxa de financiamento
delta-neutro, em SHADOW. A ideia nova pos-auditoria: rendimento NEUTRO a mercado.

  Posicao delta-neutro: LONG spot + SHORT perpetuo do mesmo ativo.
  - O movimento de preco se cancela (delta ~0, sem aposta de direcao).
  - Coleta-se a TAXA DE FINANCIAMENTO que os alavancados comprados pagam
    (funding positivo = multidao comprada paga quem esta short no perp).

  Entra quando funding >= FUNDING_ARB_ENTRY (%/8h) — so vale a pena se o
  funding cobre as taxas de execucao das 4 pernas (entrada+saida spot+perp).
  Sai quando funding cai abaixo de FUNDING_ARB_EXIT ou VIRA NEGATIVO (aí
  voce PAGARIA, entao fecha). Funding e pago a cada 8h (00/08/16 UTC).

HONESTIDADE: nao e ganho grande em %. E rendimento consistente, baixo Sharpe-
risco. Registra funding BRUTO coletado E liquido de taxas SEPARADOS — aqui a
taxa e o que decide o edge, entao nao pode ser escondida (diferente das
outras estrategias paper). Book proprio, shadow, nao toca producao.
"""
import os, time, json, logging
from datetime import datetime, timezone

import requests
import pymysql

log = logging.getLogger('egreja.fundarb')

# universo: majors + as coins que ja operamos (perp liquido na Binance)
UNIVERSE = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'DOGE', 'AVAX', 'LINK',
            'DOT', 'LTC', 'BCH', 'NEAR', 'ATOM', 'UNI', 'APT', 'ARB', 'TRX']

# fees estimadas (round-trip nas 2 pernas): spot taker ~0.075% + perp taker ~0.045%
# entrada e saida -> ~ (0.075+0.045)*2 = 0.24% do notional. Conservador.
_FEE_ROUNDTRIP_PCT = 0.24


def _f(n, d):
    try: return float(os.environ.get(n, d))
    except Exception: return float(d)


def _conn():
    return pymysql.connect(
        host=os.environ['MYSQLHOST'], user=os.environ['MYSQLUSER'],
        password=os.environ['MYSQLPASSWORD'], database=os.environ['MYSQLDATABASE'],
        port=int(os.environ.get('MYSQLPORT', 3306)), autocommit=True)


def create_tables():
    c = _conn(); cur = c.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS funding_arb_shadow (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(12), status VARCHAR(8) DEFAULT 'OPEN',
        opened_at DATETIME, closed_at DATETIME,
        funding_entry_pct DECIMAL(10,5), funding_exit_pct DECIMAL(10,5),
        notional_usd DECIMAL(14,2),
        intervals_held INT DEFAULT 0,
        funding_collected_usd DECIMAL(14,4) DEFAULT 0,
        est_fees_usd DECIMAL(14,4) DEFAULT 0,
        net_usd DECIMAL(14,4) DEFAULT 0,
        annualized_pct DECIMAL(10,3),
        close_reason VARCHAR(24),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX ix_sym_status (symbol, status)) CHARACTER SET utf8mb4""")
    cur.execute("""CREATE TABLE IF NOT EXISTS funding_arb_meta (
        k VARCHAR(64) PRIMARY KEY, v TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) CHARACTER SET utf8mb4""")
    c.close()


def _all_funding():
    """funding_pct_8h de TODOS os perps num call (fapi premiumIndex sem symbol)."""
    try:
        r = requests.get('https://fapi.binance.com/fapi/v1/premiumIndex', timeout=10)
        out = {}
        for it in r.json():
            sym = it.get('symbol', '')
            if sym.endswith('USDT'):
                out[sym[:-4]] = float(it.get('lastFundingRate') or 0) * 100  # %/8h
        return out
    except Exception as e:
        log.debug(f'[FUNDARB] funding fetch: {e}')
        return {}


# funding e liquidado nesses horarios UTC
_SETTLE_HOURS = (0, 8, 16)


def scan_cycle():
    if os.environ.get('FUNDING_ARB_ENABLED', 'true').lower() == 'false':
        return
    entry = _f('FUNDING_ARB_ENTRY', 0.02)   # %/8h (~22%/ano) — cobre as taxas
    exitt = _f('FUNDING_ARB_EXIT', 0.005)   # %/8h — abaixo disso, fecha
    leg_usd = _f('FUNDING_ARB_LEG_USD', 50000)
    max_open = int(_f('FUNDING_ARB_MAX_OPEN', 8))
    now = datetime.now(timezone.utc)

    fund = _all_funding()
    if not fund:
        return
    c = _conn(); cur = c.cursor()
    create_tables()
    cur.execute("SELECT id,symbol,opened_at,funding_entry_pct,notional_usd,intervals_held,"
                "funding_collected_usd,est_fees_usd FROM funding_arb_shadow WHERE status='OPEN'")
    open_pos = {r[1]: r for r in cur.fetchall()}
    n_open = len(open_pos)

    # ultimo settle passado (para saber se acumula funding neste ciclo)
    _last_settle_key = now.strftime('%Y-%m-%d') + f"-{max(h for h in _SETTLE_HOURS if h <= now.hour) if now.hour>=0 else 0}"
    cur.execute("SELECT v FROM funding_arb_meta WHERE k='last_settle_key'")
    row = cur.fetchone()
    settle_now = (row is None) or (row[0] != _last_settle_key)
    just_settled = now.hour in _SETTLE_HOURS

    for sym in UNIVERSE:
        fr = fund.get(sym)
        if fr is None:
            continue
        # ---- posicao aberta: acumula funding no settle e checa saida ----
        if sym in open_pos:
            (pid, _, oat, fe, notional, ivh, coll, fees) = open_pos[sym]
            notional = float(notional); coll = float(coll); fees = float(fees); ivh = int(ivh or 0)
            # acumula um intervalo de funding a cada settle novo (delta-neutro
            # -> P&L de preco ~0; o retorno e o funding coletado)
            if just_settled and settle_now:
                add = notional * (fr / 100.0)   # funding do intervalo
                coll += add; ivh += 1
                cur.execute("UPDATE funding_arb_shadow SET funding_collected_usd=%s,"
                            "intervals_held=%s, net_usd=%s WHERE id=%s",
                            (round(coll, 4), ivh, round(coll - fees, 4), pid))
            reason = None
            if fr < exitt: reason = 'FUNDING_LOW'
            if fr < 0: reason = 'FUNDING_NEGATIVE'
            if ivh >= int(_f('FUNDING_ARB_MAX_INTERVALS', 90)): reason = 'MAX_HOLD'  # ~30 dias
            if reason:
                net = coll - fees
                days = max((now - oat.replace(tzinfo=timezone.utc)).total_seconds() / 86400, 0.01)
                ann = (net / notional) / days * 365 * 100 if notional else 0
                cur.execute("""UPDATE funding_arb_shadow SET status='CLOSED', closed_at=%s,
                    funding_exit_pct=%s, net_usd=%s, annualized_pct=%s, close_reason=%s
                    WHERE id=%s""",
                    (now.replace(tzinfo=None), round(fr, 5), round(net, 4), round(ann, 3), reason, pid))
                n_open -= 1
                log.info(f'[FUNDARB] CLOSE {sym} {reason} funding=${coll:.2f} fees=${fees:.2f} '
                         f'net=${net:.2f} ({ivh}x8h, ~{ann:.1f}%/ano)')
            continue

        # ---- entrada: funding atrativo, delta-neutro ----
        if n_open >= max_open or fr < entry:
            continue
        fees0 = leg_usd * _FEE_ROUNDTRIP_PCT / 100.0
        cur.execute("""INSERT INTO funding_arb_shadow (symbol,status,opened_at,
            funding_entry_pct,notional_usd,intervals_held,funding_collected_usd,est_fees_usd,net_usd)
            VALUES (%s,'OPEN',%s,%s,%s,0,0,%s,%s)""",
            (sym, now.replace(tzinfo=None), round(fr, 5), leg_usd, round(fees0, 4), round(-fees0, 4)))
        n_open += 1
        log.info(f'[FUNDARB] OPEN {sym} funding={fr:+.4f}%/8h (~{fr*3*365:.0f}%/ano bruto) '
                 f'delta-neutro ${leg_usd:,.0f} (fees est ${fees0:.2f})')

    if just_settled and settle_now:
        cur.execute("INSERT INTO funding_arb_meta (k,v) VALUES ('last_settle_key',%s) "
                    "ON DUPLICATE KEY UPDATE v=VALUES(v)", (_last_settle_key,))
    c.close()


def funding_arb_loop(beat_fn=None):
    if os.environ.get('FUNDING_ARB_ENABLED', 'true').lower() == 'false':
        log.info('[FUNDARB] desabilitado via env'); return
    try:
        create_tables()
        log.info(f'[FUNDARB] Funding-Arb shadow iniciado — {len(UNIVERSE)} perps, '
                 f'delta-neutro, coleta funding')
    except Exception as e:
        log.error(f'[FUNDARB] setup: {e}'); return
    while True:
        try:
            if beat_fn: beat_fn('funding_arb_loop')
            scan_cycle()
        except Exception as e:
            log.error(f'[FUNDARB] loop: {e}')
        time.sleep(900)  # 15min; funding acumula a cada 8h
