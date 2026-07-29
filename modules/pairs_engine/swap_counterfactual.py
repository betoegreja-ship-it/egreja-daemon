"""
═══════════════════════════════════════════════════════════════════════════
SWAP COUNTERFACTUAL  [29-jul-2026, teste do vies apontado pelo GPT]
═══════════════════════════════════════════════════════════════════════════
Hipotese a testar: o WR de 83% do SWAPPED_OUT pode ser artefato de
sobrevivencia — vencedoras saem por swap, perdedoras ficam e viram
stop/timeout. Sem contrafactual, nao sabemos se o swap ADICIONA valor.

Mecanica: toda trade live fechada por SWAPPED_OUT gera uma copia virtual
que CONTINUA com as regras originais (converge |z|<=z_exit, stop |z|>=z_stop,
timeout 15d do open original). No fim comparamos:
  pnl_swap (realizado no swap)  vs  pnl_contrafactual (se tivesse segurado)
  delta = pnl_swap - pnl_cf  → swap adicionou valor se delta medio > 0.

Shadow puro: tabela propria, nao toca em nada.
"""
import os, time, logging
from datetime import datetime

log = logging.getLogger('egreja.pairs.swapcf')

CF_ENABLED = os.environ.get('PAIRS_SWAPCF_ENABLED', 'true').lower() != 'false'
Z_EXIT = float(os.environ.get('PAIRS_SWAPCF_Z_EXIT', 0.4))
Z_STOP = float(os.environ.get('PAIRS_SWAPCF_Z_STOP', 3.5))
TIMEOUT_D = float(os.environ.get('PAIRS_SWAPCF_TIMEOUT_D', 15.0))

_open = {}      # pair_id -> cf dict (1 por par; swaps consecutivos raros)
_booted = False


def _conn():
    try:
        from .persistence import _get_conn
        return _get_conn()
    except Exception as e:
        log.debug(f'[SWAPCF] conn: {e}')
        return None


def _ensure_table(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS pairs_swap_counterfactual (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        live_trade_id VARCHAR(40), pair_id VARCHAR(24), direction VARCHAR(8),
        entry_z DOUBLE, price_a_entry DOUBLE, price_b_entry DOUBLE,
        qty_a INT, qty_b INT, position_size DOUBLE,
        pnl_swap DOUBLE, swapped_at DATETIME, orig_opened_at DATETIME,
        status VARCHAR(8) DEFAULT 'OPEN', cf_close_reason VARCHAR(16),
        cf_closed_at DATETIME, cf_exit_z DOUBLE, pnl_cf DOUBLE, delta DOUBLE,
        updated_at DATETIME, INDEX ix_pair (pair_id), INDEX ix_status (status))""")


def _boot():
    global _booted
    if _booted: return
    _booted = True
    c = _conn()
    if not c: return
    try:
        cur = c.cursor(); _ensure_table(cur)
        cur.execute("""SELECT id,pair_id,direction,entry_z,price_a_entry,price_b_entry,
                       qty_a,qty_b,position_size,pnl_swap,UNIX_TIMESTAMP(orig_opened_at)
                       FROM pairs_swap_counterfactual WHERE status='OPEN'""")
        for r in cur.fetchall():
            _open[r[1]] = {'row_id': r[0], 'pair_id': r[1], 'direction': r[2], 'entry_z': r[3],
                           'price_a_entry': r[4], 'price_b_entry': r[5], 'qty_a': r[6],
                           'qty_b': r[7], 'position_size': r[8], 'pnl_swap': r[9],
                           'orig_epoch': r[10] or time.time()}
        cur.close(); c.commit(); c.close()
        if _open: log.info(f'[SWAPCF] boot: {len(_open)} contrafactuais abertos')
    except Exception as e:
        log.debug(f'[SWAPCF] boot: {e}')
        try: c.close()
        except Exception: pass


def feed(trade):
    """Chamado quando uma trade live fecha por SWAPPED_OUT."""
    if not CF_ENABLED or not trade: return
    _boot()
    pid = trade.get('pair_id')
    if not pid or pid in _open: return
    try:
        opened = trade.get('opened_at')
        orig_epoch = time.time()
        if opened:
            try:
                orig_epoch = datetime.fromisoformat(str(opened).replace('Z', '')).timestamp()
            except Exception: pass
        cf = {'pair_id': pid, 'direction': trade.get('direction'),
              'entry_z': float(trade.get('entry_z') or 0),
              'price_a_entry': float(trade.get('price_a_entry') or 0),
              'price_b_entry': float(trade.get('price_b_entry') or 0),
              'qty_a': int(trade.get('qty_a') or 0), 'qty_b': int(trade.get('qty_b') or 0),
              'position_size': float(trade.get('position_size') or 0),
              'pnl_swap': float(trade.get('pnl') or 0), 'orig_epoch': orig_epoch}
        if not (cf['price_a_entry'] and cf['qty_a']): return
        c = _conn()
        if not c: return
        cur = c.cursor(); _ensure_table(cur)
        cur.execute("""INSERT INTO pairs_swap_counterfactual
            (live_trade_id,pair_id,direction,entry_z,price_a_entry,price_b_entry,
             qty_a,qty_b,position_size,pnl_swap,swapped_at,orig_opened_at,status,updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),FROM_UNIXTIME(%s),'OPEN',NOW())""",
            (trade.get('id'), pid, cf['direction'], cf['entry_z'], cf['price_a_entry'],
             cf['price_b_entry'], cf['qty_a'], cf['qty_b'], cf['position_size'],
             cf['pnl_swap'], int(orig_epoch)))
        cf['row_id'] = cur.lastrowid
        c.commit(); cur.close(); c.close()
        _open[pid] = cf
        log.info(f'[SWAPCF] armado: {pid} (swap pnl=R${cf["pnl_swap"]:,.0f}) — '
                 f'copia virtual continua ate saida original')
    except Exception as e:
        log.debug(f'[SWAPCF] feed: {e}')


def monitor(signal):
    """Chamado a cada scan com o signal do par (mesmo do motor live)."""
    if not CF_ENABLED or not signal: return
    _boot()
    pid = signal.get('pair_id')
    cf = _open.get(pid)
    if not cf: return
    z = signal.get('z_score')
    pa, pb = signal.get('price_a'), signal.get('price_b')
    if z is None or not pa or not pb: return
    age_d = (time.time() - cf['orig_epoch']) / 86400.0
    reason = None
    if abs(z) <= Z_EXIT: reason = 'CONVERGED'
    elif abs(z) >= Z_STOP: reason = 'STOP_LOSS'
    elif age_d > TIMEOUT_D: reason = 'TIMEOUT'
    if not reason: return
    if cf['direction'] == 'SHORT_A':
        pnl_cf = cf['qty_a'] * (cf['price_a_entry'] - pa) + cf['qty_b'] * (pb - cf['price_b_entry'])
    else:
        pnl_cf = cf['qty_a'] * (pa - cf['price_a_entry']) + cf['qty_b'] * (cf['price_b_entry'] - pb)
    delta = cf['pnl_swap'] - pnl_cf
    c = _conn()
    if not c: return
    try:
        cur = c.cursor()
        cur.execute("""UPDATE pairs_swap_counterfactual SET status='CLOSED',
            cf_close_reason=%s, cf_closed_at=NOW(), cf_exit_z=%s, pnl_cf=%s, delta=%s,
            updated_at=NOW() WHERE id=%s""",
            (reason, round(z, 4), round(pnl_cf, 2), round(delta, 2), cf['row_id']))
        c.commit(); cur.close(); c.close()
        _open.pop(pid, None)
        log.info(f'[SWAPCF] fechado {pid} cf={reason} pnl_cf=R${pnl_cf:,.0f} vs '
                 f'swap=R${cf["pnl_swap"]:,.0f} → delta={delta:+,.0f} '
                 f'({"swap ADICIONOU" if delta > 0 else "swap PERDEU"} valor)')
    except Exception as e:
        log.debug(f'[SWAPCF] close: {e}')
        try: c.close()
        except Exception: pass


def summary():
    _boot()
    out = {'enabled': CF_ENABLED, 'abertos': len(_open), 'fechados': 0,
           'swap_adicionou_valor_n': 0, 'delta_total': 0.0, 'delta_medio': None,
           'leitura': 'delta>0 = fechar por swap foi melhor que segurar'}
    c = _conn()
    if not c: return out
    try:
        cur = c.cursor(); _ensure_table(cur)
        cur.execute("""SELECT COUNT(*), SUM(delta>0), SUM(delta), AVG(delta)
                       FROM pairs_swap_counterfactual WHERE status='CLOSED'""")
        n, w, tot, avg = cur.fetchone()
        out['fechados'] = int(n or 0)
        out['swap_adicionou_valor_n'] = int(w or 0)
        out['delta_total'] = round(float(tot or 0), 2)
        out['delta_medio'] = round(float(avg), 2) if avg is not None else None
        cur.close(); c.commit(); c.close()
    except Exception as e:
        log.debug(f'[SWAPCF] summary: {e}')
        try: c.close()
        except Exception: pass
    return out
