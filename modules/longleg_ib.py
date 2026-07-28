# -*- coding: utf-8 -*-
"""[28-jul-2026, decisao Beto] LONG-LEG IB — execucao VIVA da perna long da limonada.

Book SEPARADO (tag LL-), em paralelo a Arbi (nao mexe na Arbi). Quando a Arbi abre um
par cuja perna LONG (barata) e IB-reachable (nao-B3), compra a long no IB paper e
gerencia com risco PROPRIO: stop 1.5xATR, trailing 1xATR, timeout 90min. Saida propria
(book DIRECTIONAL_EXIT), nao espelha o fecho da Arbi.

Diferenca do longleg_harvest (shadow): aquele MEDE (simula depois); este EXECUTA de
verdade no IB paper e mede o fill real. Fail-open total: erro nunca derruba producao.
"""
import os, time, logging
from datetime import datetime, timezone

log = logging.getLogger('egreja.longleg_ib')

STOP_ATR = 1.5
TRAIL_ACT_ATR = 1.0
TIMEOUT_MIN = 90
BETA = None  # nao ha hedge aqui; e direcional puro


def _conn():
    from modules.binance_exec import _conn as bc
    return bc()


_ready = {'v': False}


def create_tables():
    if _ready['v']:
        return
    try:
        c = _conn(); cur = c.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS ll_ib_positions (
            id BIGINT AUTO_INCREMENT PRIMARY KEY, arbi_id VARCHAR(48) UNIQUE,
            pair VARCHAR(20), symbol VARCHAR(20), market VARCHAR(10),
            qty INT, entry_px DECIMAL(18,6), atr_abs DECIMAL(18,6),
            stop_px DECIMAL(18,6), peak_px DECIMAL(18,6), trailing TINYINT DEFAULT 0,
            notional_usd DECIMAL(16,2), opened_at DATETIME, opened_epoch BIGINT,
            open_status VARCHAR(12), status VARCHAR(10) DEFAULT 'OPEN',
            exit_px DECIMAL(18,6), exit_reason VARCHAR(16), pnl DECIMAL(16,4),
            ret_pct DECIMAL(10,4), closed_at DATETIME, claimed_at DATETIME NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX ix_status (status)) CHARACTER SET utf8mb4""")
        c.close(); _ready['v'] = True
    except Exception as e:
        log.debug(f'[LL-IB] create_tables: {e}')


def _atr_abs(pre_bars, px):
    """ATR absoluto (em preco) das barras ANTES da entrada."""
    if not pre_bars or len(pre_bars) < 2:
        return px * 0.005 if px else None
    trs = []
    for i in range(1, len(pre_bars)):
        h, l, pc = pre_bars[i][2], pre_bars[i][3], pre_bars[i-1][4]
        if None in (h, l, pc):
            continue
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not trs:
        return px * 0.005 if px else None
    import statistics
    a = statistics.mean(trs)
    return a if a > 0 else (px * 0.005 if px else None)


def _last_px(ysym):
    """Ultimo close 5m do Yahoo (preco corrente aproximado)."""
    try:
        from modules.longleg_harvest import _yahoo_bars
        now = int(time.time())
        bars = _yahoo_bars(ysym, now - 3 * 3600, now + 300)
        return bars[-1][4] if bars else None
    except Exception:
        return None


def on_arbi_open(trade):
    """Compra a perna LONG no IB paper (se IB-reachable) e registra a posicao viva."""
    try:
        if os.environ.get('IB_LONGLEG_ENABLED', 'true').lower() == 'false':
            return
        create_tables()
        from modules import ib_exec
        from modules.longleg_harvest import _yahoo_bars, _ysym
        direction = str(trade.get('direction', 'LONG_A')).upper()
        if direction == 'LONG_A':
            ll, lm, lpx = trade.get('leg_a'), trade.get('mkt_a'), trade.get('price_a_entry')
        else:
            ll, lm, lpx = trade.get('leg_b'), trade.get('mkt_b'), trade.get('price_b_entry')
        if not ll or not lm:
            return
        if not ib_exec.ib_reachable(lm, ll):
            return  # perna long em B3 -> IB nao alcanca
        arbi_id = str(trade.get('id') or trade.get('pair_id') or f'{ll}-{int(time.time())}')
        arbi_id = ('LL-' + arbi_id)[:48]
        # ja existe? (idempotente)
        c = _conn(); cur = c.cursor()
        cur.execute("SELECT 1 FROM ll_ib_positions WHERE arbi_id=%s", (arbi_id,))
        if cur.fetchone():
            c.close(); return
        entry_px = float(lpx) if lpx else None
        oe = int(time.time())
        ysym = _ysym(ll, lm)
        pre = _yahoo_bars(ysym, oe - 4000, oe)
        if not entry_px:
            entry_px = pre[-1][4] if pre else None
        if not entry_px:
            c.close(); return
        atr = _atr_abs(pre, entry_px)
        stop = entry_px - STOP_ATR * atr if atr else entry_px * 0.985
        # sizing: notional fixo (paper), capado pelo teto de liquidez do IB
        notional = min(ib_exec._f('IB_LONGLEG_USD', 50000), ib_exec._f('IB_EXEC_MAX_USD', 150000))
        qty = max(1, int(notional / entry_px))
        fill, status = ib_exec.exec_longleg(ll, lm, 'BUY', qty, arbi_id, price_ref=entry_px)
        eff_entry = float(fill) if fill else entry_px
        cur.execute("""INSERT INTO ll_ib_positions
            (arbi_id,pair,symbol,market,qty,entry_px,atr_abs,stop_px,peak_px,trailing,
             notional_usd,opened_at,opened_epoch,open_status,status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,NOW(),%s,%s,'OPEN')""",
            (arbi_id, trade.get('pair_id') or trade.get('id'), ll, lm, qty,
             round(eff_entry, 6), round(atr, 6) if atr else None, round(stop, 6),
             round(eff_entry, 6), round(qty * eff_entry, 2), oe, status))
        c.close()
        log.warning(f'[LL-IB] ABRIU {ll}({lm}) {qty}sh @ {eff_entry} stop={stop:.4f} ({status})')
    except Exception as e:
        log.debug(f'[LL-IB] on_arbi_open: {e}')


def on_arbi_close(trade):
    """[decisao Beto] SAIDA PELA ARBI (principal): quando a Arbi fecha o par, vende a
    perna long no IB paper. O trail/saida-propria fica so em shadow (longleg_harvest)."""
    try:
        if os.environ.get('IB_LONGLEG_ENABLED', 'true').lower() == 'false':
            return
        create_tables()
        from modules import ib_exec
        from modules.longleg_harvest import _ysym
        arbi_id = ('LL-' + str(trade.get('id') or trade.get('pair_id') or ''))[:48]
        if arbi_id == 'LL-':
            return
        c = _conn(); cur = c.cursor()
        cur.execute("""UPDATE ll_ib_positions SET status='CLOSING', claimed_at=NOW()
            WHERE arbi_id=%s AND status='OPEN'""", (arbi_id,))
        if cur.rowcount != 1:
            c.close(); return
        cur.execute("SELECT symbol,market,qty,entry_px FROM ll_ib_positions WHERE arbi_id=%s", (arbi_id,))
        row = cur.fetchone(); c.close()
        if not row:
            return
        sym, mkt, qty, entry_px = row[0], row[1], int(row[2] or 0), float(row[3] or 0)
        direction = str(trade.get('direction', 'LONG_A')).upper()
        if direction == 'LONG_A':
            exit_ref = trade.get('price_a_exit') or trade.get('price_a')
        else:
            exit_ref = trade.get('price_b_exit') or trade.get('price_b')
        exit_ref = float(exit_ref) if exit_ref else _last_px(_ysym(sym, mkt))
        if not exit_ref:
            c = _conn(); cur = c.cursor()
            cur.execute("UPDATE ll_ib_positions SET status='OPEN' WHERE arbi_id=%s", (arbi_id,))
            c.close(); return  # sem preco -> volta OPEN, safety fecha depois
        fill, stt = ib_exec.exec_longleg(sym, mkt, 'SELL', qty, arbi_id, price_ref=exit_ref)
        exit_px = float(fill) if fill else exit_ref
        pnl = (exit_px - entry_px) * qty
        ret = (exit_px / entry_px - 1) * 100 if entry_px else 0
        c = _conn(); cur = c.cursor()
        cur.execute("""UPDATE ll_ib_positions SET status='CLOSED', exit_px=%s,
            exit_reason='ARBI_CLOSE', pnl=%s, ret_pct=%s, closed_at=NOW() WHERE arbi_id=%s""",
            (round(exit_px, 6), round(pnl, 4), round(ret, 4), arbi_id))
        c.close()
        log.warning(f'[LL-IB] FECHOU (Arbi) {sym} @ {exit_px} pnl={pnl:+.2f} ({stt})')
    except Exception as e:
        log.debug(f'[LL-IB] on_arbi_close: {e}')


def monitor(limit=40):
    """[decisao Beto] Saida principal = fecho da Arbi (on_arbi_close). Aqui e so REDE DE
    SEGURANCA: recupera claim orfao e fecha posicoes presas ha muito tempo (Arbi que
    nunca fechou / hook perdido). Stop/trail fica so em shadow (longleg_harvest)."""
    try:
        if os.environ.get('IB_LONGLEG_ENABLED', 'true').lower() == 'false':
            return
        create_tables()
        from modules import ib_exec
        from modules.longleg_harvest import _ysym
        safety_min = int(ib_exec._f('IB_LONGLEG_SAFETY_MIN', 4320))  # 3 dias
        c = _conn(); cur = c.cursor()
        cur.execute("""UPDATE ll_ib_positions SET status='OPEN'
            WHERE status='CLOSING' AND claimed_at < (NOW() - INTERVAL 10 MINUTE)""")
        cur.execute("""SELECT arbi_id,symbol,market,qty,entry_px FROM ll_ib_positions
            WHERE status='OPEN' AND opened_epoch < %s ORDER BY opened_epoch LIMIT %s""",
            (int(time.time()) - safety_min * 60, limit))
        rows = cur.fetchall(); c.close()
        for (arbi_id, sym, mkt, qty, entry_px) in rows:
            cp = _last_px(_ysym(sym, mkt))
            if cp is None:
                continue
            c = _conn(); cur = c.cursor()
            cur.execute("""UPDATE ll_ib_positions SET status='CLOSING', claimed_at=NOW()
                WHERE arbi_id=%s AND status='OPEN'""", (arbi_id,))
            if cur.rowcount != 1:
                c.close(); continue
            c.close()
            fill, stt = ib_exec.exec_longleg(sym, mkt, 'SELL', int(qty), arbi_id, price_ref=cp)
            exit_px = float(fill) if fill else cp
            entry_px = float(entry_px or 0)
            pnl = (exit_px - entry_px) * int(qty)
            ret = (exit_px / entry_px - 1) * 100 if entry_px else 0
            c = _conn(); cur = c.cursor()
            cur.execute("""UPDATE ll_ib_positions SET status='CLOSED', exit_px=%s,
                exit_reason='SAFETY_TIMEOUT', pnl=%s, ret_pct=%s, closed_at=NOW() WHERE arbi_id=%s""",
                (round(exit_px, 6), round(pnl, 4), round(ret, 4), arbi_id))
            c.close()
            log.warning(f'[LL-IB] FECHOU (safety) {sym} @ {exit_px} pnl={pnl:+.2f}')
    except Exception as e:
        log.debug(f'[LL-IB] monitor: {e}')


def summary():
    try:
        create_tables()
        import pymysql
        c = _conn(); cur = c.cursor(pymysql.cursors.DictCursor)
        cur.execute("SELECT * FROM ll_ib_positions ORDER BY id DESC")
        rows = cur.fetchall(); c.close()
        closed = [r for r in rows if r['status'] == 'CLOSED' and r['pnl'] is not None]
        openp = [r for r in rows if r['status'] in ('OPEN', 'CLOSING')]
        wr = round(100 * sum(1 for r in closed if r['pnl'] > 0) / len(closed), 1) if closed else 0
        tot = round(sum(float(r['pnl']) for r in closed), 2)
        rets = [float(r['ret_pct']) for r in closed if r['ret_pct'] is not None]
        import statistics
        return {
            'book': 'LONG-LEG IB (saida propria: stop1.5ATR/trail1ATR/timeout90)',
            'abertas': len(openp), 'fechadas': len(closed),
            'WR_pct': wr, 'pnl_total': tot,
            'ret_medio_pct': round(statistics.mean(rets), 3) if rets else None,
            'ret_mediana_pct': round(statistics.median(rets), 3) if rets else None,
            'ultimas': [{'symbol': r['symbol'], 'reason': r['exit_reason'],
                         'pnl': float(r['pnl']) if r['pnl'] is not None else None,
                         'ret_pct': float(r['ret_pct']) if r['ret_pct'] is not None else None,
                         'status': r['status']} for r in rows[:12]],
            'nota': 'Book proprio (tag LL-) em paralelo a Arbi; so pernas long IB-reachable (nao-B3). Paper.'}
    except Exception as e:
        return {'error': str(e)}
