# -*- coding: utf-8 -*-
"""[25-jul-2026, decisao Beto — "limonada da Arbi"] LONG-LEG HARVEST.

A Arbi de 2 pernas era fantasma. Mas a INFORMACAO de direcao dela e real:
comprar so a PERNA LONG de cada Arbi, como trade direcional simples, mostrou
edge no backtest de 405 trades (WR 56% na rede aberta; 81% na filtrada).

Aqui rodam 3 books em SHADOW, alimentados por TODA abertura de Arbi. O mesmo
trade cai em 1, 2 ou 3 books conforme os filtros. Mede-se por par.

  ALL          — compra a perna long de TODA Arbi (rede aberta). Benchmark.
  FILTERED_6   — 6 pares (WR>=60% no backtest) + |spread| em [0.8%, 2.0%].
  FILTERED_10  — 10 pares (P&L>0 no backtest, inclui PETR4-PBR.A) + janela.

Registro no ABRIR (snapshot imutavel da entrada) e no FECHAR da Arbi
(usa o preco REAL da perna no exit — validado na auditoria forense).
P&L direcional da perna long apenas. Book shadow, nao toca producao nem capital.
"""
import os, logging
from datetime import datetime, timezone

import pymysql

log = logging.getLogger('egreja.longleg')

# 6 pares com WR>=60% na perna long (backtest 5 meses)
GOOD6 = {'SBSP3-SBS', 'GGBR4-GGB', 'SAP-SAP.DE', 'ASML-ASML.AS', 'ITUB4-ITUB', 'UGPA3-UGP'}
# 10 pares com P&L>0 na perna long + o par novo corrigido da Petro (PETR4-PBR.A)
GOOD10 = GOOD6 | {'CSNA3-SID', 'CMIG4-CIG', 'PETR4-PBR', 'PETR4-PBR.A'}
SPREAD_LO, SPREAD_HI = 0.8, 2.0   # janela de |spread| de entrada (%)


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
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        arbi_id VARCHAR(40) UNIQUE, pair VARCHAR(16),
        long_leg VARCHAR(20), long_mkt VARCHAR(10),
        book_all TINYINT DEFAULT 1, book_f6 TINYINT DEFAULT 0, book_f10 TINYINT DEFAULT 0,
        entry_spread_abs DECIMAL(8,4), direction VARCHAR(8),
        leg_price_entry DECIMAL(18,6), fx_entry DECIMAL(12,6),
        notional_usd DECIMAL(14,2), status VARCHAR(8) DEFAULT 'OPEN',
        opened_at DATETIME, closed_at DATETIME,
        leg_price_exit DECIMAL(18,6), fx_exit DECIMAL(12,6),
        ret_pct DECIMAL(10,4), pnl_usd DECIMAL(14,2), win TINYINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX ix_pair (pair), INDEX ix_status (status)) CHARACTER SET utf8mb4""")
    c.close()
    _ready['v'] = True


def _usd(p, fx, mkt):
    p = float(p or 0); fx = float(fx or 1)
    if p <= 0:
        return None
    if mkt == 'B3':
        return p / fx if fx else None
    if mkt == 'NYSE':
        return p
    return p * fx


def on_arbi_open(trade):
    """Snapshot da perna long na abertura da Arbi. Fail-open."""
    try:
        create_tables()
        pair = trade.get('pair_id') or trade.get('id')
        direction = str(trade.get('direction', 'LONG_A')).upper()
        # perna long = a que foi comprada
        if direction == 'LONG_A':
            leg, mkt, px = trade.get('leg_a'), trade.get('mkt_a'), trade.get('price_a_entry')
        else:
            leg, mkt, px = trade.get('leg_b'), trade.get('mkt_b'), trade.get('price_b_entry')
        abss = abs(float(trade.get('entry_spread') or trade.get('entry_spread_raw') or 0))
        fx = trade.get('fx_a_entry') or trade.get('fx_rate_entry') or trade.get('fx_rate') or 1
        notional = float(trade.get('position_size') or 0)
        in_window = SPREAD_LO <= abss <= SPREAD_HI
        f6 = 1 if (pair in GOOD6 and in_window) else 0
        f10 = 1 if (pair in GOOD10 and in_window) else 0
        c = _conn(); cur = c.cursor()
        cur.execute("""INSERT IGNORE INTO longleg_harvest (arbi_id,pair,long_leg,long_mkt,
            book_all,book_f6,book_f10,entry_spread_abs,direction,leg_price_entry,fx_entry,
            notional_usd,status,opened_at)
            VALUES (%s,%s,%s,%s,1,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s)""",
            (trade.get('id'), pair, leg, mkt, f6, f10, round(abss, 4), direction,
             px, fx, notional, datetime.now(timezone.utc).replace(tzinfo=None)))
        c.close()
    except Exception as e:
        log.debug(f'[LONGLEG] open: {e}')


def on_arbi_close(trade):
    """Fecha a perna long usando o preco REAL da perna no exit. Fail-open."""
    try:
        create_tables()
        direction = str(trade.get('direction', 'LONG_A')).upper()
        if direction == 'LONG_A':
            mkt, px1 = trade.get('mkt_a'), trade.get('price_a_exit')
        else:
            mkt, px1 = trade.get('mkt_b'), trade.get('price_b_exit')
        fx1 = trade.get('fx_a_exit') or trade.get('fx_rate_exit') or trade.get('fx_rate') or 1
        c = _conn(); cur = c.cursor()
        cur.execute("""SELECT leg_price_entry, fx_entry, long_mkt, notional_usd
            FROM longleg_harvest WHERE arbi_id=%s AND status='OPEN'""", (trade.get('id'),))
        row = cur.fetchone()
        if not row:
            c.close(); return
        pe, fxe, lmkt, notional = row
        u0 = _usd(pe, fxe, lmkt); u1 = _usd(px1, fx1, lmkt)
        if not u0 or not u1:
            c.close(); return
        ret = u1 / u0 - 1.0
        pnl = float(notional or 0) * ret
        cur.execute("""UPDATE longleg_harvest SET status='CLOSED', closed_at=%s,
            leg_price_exit=%s, fx_exit=%s, ret_pct=%s, pnl_usd=%s, win=%s
            WHERE arbi_id=%s AND status='OPEN'""",
            (datetime.now(timezone.utc).replace(tzinfo=None), px1, fx1,
             round(ret * 100, 4), round(pnl, 2), 1 if ret > 0 else 0, trade.get('id')))
        c.close()
    except Exception as e:
        log.debug(f'[LONGLEG] close: {e}')


def summary():
    """Resumo dos 3 books para o endpoint."""
    create_tables()
    c = _conn(); cur = c.cursor(pymysql.cursors.DictCursor)
    out = {}
    for book, col in (('ALL', 'book_all'), ('FILTERED_6', 'book_f6'), ('FILTERED_10', 'book_f10')):
        cur.execute(f"""SELECT COUNT(*) n, SUM(status='CLOSED') fechados,
            ROUND(100*SUM(win=1)/NULLIF(SUM(status='CLOSED'),0),1) wr,
            ROUND(AVG(CASE WHEN status='CLOSED' THEN ret_pct END),3) ret_med,
            ROUND(SUM(pnl_usd),0) pnl FROM longleg_harvest WHERE {col}=1""")
        out[book] = cur.fetchone()
    cur.execute("""SELECT pair, COUNT(*) n, SUM(status='CLOSED') fech,
        ROUND(100*SUM(win=1)/NULLIF(SUM(status='CLOSED'),0),1) wr, ROUND(SUM(pnl_usd),0) pnl
        FROM longleg_harvest GROUP BY pair ORDER BY pnl DESC""")
    out['por_par'] = list(cur.fetchall())
    c.close()
    return out
