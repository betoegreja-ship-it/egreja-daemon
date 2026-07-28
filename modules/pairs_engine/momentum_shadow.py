"""
═══════════════════════════════════════════════════════════════════════════
PAIRS MOMENTUM SHADOW  —  [28-jul-2026, decisao Beto]
═══════════════════════════════════════════════════════════════════════════
Hipotese do Beto: pares com WR 0% (o spread TENDE em vez de reverter) dariam
lucro se a gente VIRASSE o lado (seguir a tendencia em vez de fadear).

Este modulo NAO opera book real. Sempre que uma entrada de fade e BLOQUEADA
pelos novos freios (regime BROKEN/RANDOM_WALK, cooldown, no-deeper), ele abre
uma trade VIRTUAL momentum com a direcao INVERTIDA e acompanha o resultado
hipotetico ate o fim. Assim medimos, com dado real, se "virar o lado" nesses
pares vira lucro de verdade — sem arriscar 1 centavo.

Direcao:
  fade LONG_A  (z<0, aposta que sobe)  -> momentum MOM_SHORT_A (aposta que cai mais)
  fade SHORT_A (z>0, aposta que cai)   -> momentum MOM_LONG_A  (aposta que sobe mais)

Saida (baseada em z, agnostica a sinal):
  moved_away = |z_now| - |z_entry|
   >= TARGET_DZ  -> MOM_TARGET      (tendencia continuou; momentum funcionou)
   <= -STOP_DZ   -> MOM_REVERT_STOP (spread reverteu; momentum falhou)
   hold > MAX_H  -> MOM_TIMEOUT
O P&L e sempre calculado dos PRECOS reais — o close_reason e so diagnostico.
"""
import os, time, uuid, logging
from datetime import datetime

log = logging.getLogger('egreja.pairs.momentum_shadow')

MOM_ENABLED       = os.environ.get('PAIRS_MOMENTUM_SHADOW', 'true').lower() != 'false'
MOM_NOTIONAL      = float(os.environ.get('PAIRS_MOM_NOTIONAL', 100000))   # R$ nominal virtual
MOM_TARGET_DZ     = float(os.environ.get('PAIRS_MOM_TARGET_DZ', 1.0))     # profit: diverge +1.0
MOM_STOP_DZ       = float(os.environ.get('PAIRS_MOM_STOP_DZ', 0.75))      # stop: reverte 0.75
MOM_MAX_HOLD_H    = float(os.environ.get('PAIRS_MOM_MAX_HOLD_H', 72))
MOM_REOPEN_COOL_H = float(os.environ.get('PAIRS_MOM_REOPEN_COOL_H', 6))
MOM_PARTIAL_TTL_S = float(os.environ.get('PAIRS_MOM_PARTIAL_TTL_S', 120)) # throttle DB partial

_open = {}            # {pair_id: trade_dict}  (em memoria)
_last_close = {}      # {pair_id: epoch}       (cooldown de reabertura)
_last_partial = {}    # {id: epoch}            (throttle update DB)
_booted = False


def _conn():
    try:
        from .persistence import _get_conn
        return _get_conn()
    except Exception as e:
        log.debug(f'[MOM] conn: {e}')
        return None


def _ensure_table(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS pairs_momentum_shadow (
        id VARCHAR(40) PRIMARY KEY,
        pair_id VARCHAR(48), direction VARCHAR(16), block_reason VARCHAR(160),
        entry_z DOUBLE, entry_spread DOUBLE,
        price_a_entry DOUBLE, price_b_entry DOUBLE,
        qty_a INT, qty_b INT, notional DOUBLE, hedge_ratio DOUBLE,
        opened_at DATETIME, status VARCHAR(10),
        current_z DOUBLE, pnl DOUBLE, pnl_pct DOUBLE,
        exit_z DOUBLE, exit_spread DOUBLE,
        price_a_exit DOUBLE, price_b_exit DOUBLE,
        close_reason VARCHAR(20), closed_at DATETIME, updated_at DATETIME,
        INDEX ix_pair (pair_id), INDEX ix_status (status)
    )""")


def _boot_load():
    """Carrega shadows abertas do DB pra memoria (sobrevive restart)."""
    global _booted
    if _booted:
        return
    _booted = True
    c = _conn()
    if not c:
        return
    try:
        cur = c.cursor()
        _ensure_table(cur)
        cur.execute("""SELECT id,pair_id,direction,entry_z,price_a_entry,price_b_entry,
                              qty_a,qty_b,notional,hedge_ratio,UNIX_TIMESTAMP(opened_at)
                       FROM pairs_momentum_shadow WHERE status='OPEN'""")
        for r in cur.fetchall():
            _open[r[1]] = {'id': r[0], 'pair_id': r[1], 'direction': r[2], 'entry_z': r[3],
                           'price_a_entry': r[4], 'price_b_entry': r[5], 'qty_a': r[6],
                           'qty_b': r[7], 'notional': r[8], 'hedge_ratio': r[9],
                           'opened_epoch': r[10] or time.time()}
        cur.close()
        c.commit(); c.close()
        if _open:
            log.info(f'[MOM] boot: {len(_open)} shadows momentum abertas recarregadas')
    except Exception as e:
        log.debug(f'[MOM] boot load: {e}')
        try: c.close()
        except: pass


def feed(signal, block_reason=''):
    """Abre uma shadow momentum (direcao invertida) quando um fade real e bloqueado."""
    if not MOM_ENABLED or not signal:
        return
    _boot_load()
    pid = signal.get('pair_id')
    if not pid or pid in _open:
        return
    # cooldown de reabertura (mesma divergencia persistente)
    if time.time() - _last_close.get(pid, 0) < MOM_REOPEN_COOL_H * 3600:
        return
    fade_dir = signal.get('direction')
    if fade_dir not in ('LONG_A', 'SHORT_A'):
        return
    mom_dir = 'MOM_SHORT_A' if fade_dir == 'LONG_A' else 'MOM_LONG_A'
    pa = signal.get('price_a'); pb = signal.get('price_b')
    if not pa or not pb:
        return
    hedge = abs(signal.get('hedge_ratio', 1.0) or 1.0)
    qty_a = int(MOM_NOTIONAL / 2 / max(pa, 0.01))
    qty_b = int(qty_a * hedge)
    if qty_a <= 0:
        return
    tid = f'MOM-{uuid.uuid4().hex[:12]}'
    tr = {'id': tid, 'pair_id': pid, 'direction': mom_dir, 'block_reason': (block_reason or '')[:158],
          'entry_z': signal.get('z_score'), 'entry_spread': signal.get('spread_current'),
          'price_a_entry': pa, 'price_b_entry': pb, 'qty_a': qty_a, 'qty_b': qty_b,
          'notional': MOM_NOTIONAL, 'hedge_ratio': hedge, 'opened_epoch': time.time()}
    _open[pid] = tr
    c = _conn()
    if c:
        try:
            cur = c.cursor(); _ensure_table(cur)
            cur.execute("""INSERT INTO pairs_momentum_shadow
                (id,pair_id,direction,block_reason,entry_z,entry_spread,price_a_entry,
                 price_b_entry,qty_a,qty_b,notional,hedge_ratio,opened_at,status,
                 current_z,pnl,pnl_pct,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),'OPEN',%s,0,0,NOW())""",
                (tid, pid, mom_dir, tr['block_reason'], tr['entry_z'], tr['entry_spread'],
                 pa, pb, qty_a, qty_b, MOM_NOTIONAL, hedge, tr['entry_z']))
            cur.close(); c.commit(); c.close()
        except Exception as e:
            log.debug(f'[MOM] insert {pid}: {e}')
            try: c.close()
            except: pass
    log.info(f'[MOM] OPEN {tid} {pid} {mom_dir} z={tr["entry_z"]:.2f} '
             f'(fade bloqueado: {block_reason})')


def _pnl(tr, pa, pb):
    qa = tr['qty_a']; qb = tr['qty_b']
    if tr['direction'] == 'MOM_SHORT_A':   # short A, long B
        return qa * (tr['price_a_entry'] - pa) + qb * (pb - tr['price_b_entry'])
    else:                                   # MOM_LONG_A: long A, short B
        return qa * (pa - tr['price_a_entry']) + qb * (tr['price_b_entry'] - pb)


def monitor(signal):
    """A cada scan: atualiza P&L parcial da shadow do par e avalia saida."""
    if not MOM_ENABLED or not signal:
        return
    pid = signal.get('pair_id')
    tr = _open.get(pid)
    if not tr:
        return
    pa = signal.get('price_a'); pb = signal.get('price_b')
    z_now = signal.get('z_score')
    if pa is None or pb is None or z_now is None:
        return
    pnl = _pnl(tr, pa, pb)
    pnl_pct = 100 * pnl / max(tr['notional'], 1)
    tr['pnl'] = round(pnl, 2); tr['current_z'] = z_now

    moved_away = abs(z_now) - abs(tr.get('entry_z') or 0)
    age_h = (time.time() - tr.get('opened_epoch', time.time())) / 3600.0
    reason = None
    if moved_away >= MOM_TARGET_DZ:
        reason = 'MOM_TARGET'
    elif moved_away <= -MOM_STOP_DZ:
        reason = 'MOM_REVERT_STOP'
    elif age_h > MOM_MAX_HOLD_H:
        reason = 'MOM_TIMEOUT'

    c = _conn()
    if not c:
        return
    try:
        cur = c.cursor()
        if reason:
            cur.execute("""UPDATE pairs_momentum_shadow SET status='CLOSED',current_z=%s,
                pnl=%s,pnl_pct=%s,exit_z=%s,exit_spread=%s,price_a_exit=%s,price_b_exit=%s,
                close_reason=%s,closed_at=NOW(),updated_at=NOW() WHERE id=%s""",
                (z_now, round(pnl, 2), round(pnl_pct, 4), z_now, signal.get('spread_current'),
                 pa, pb, reason, tr['id']))
            cur.close(); c.commit(); c.close()
            _open.pop(pid, None)
            _last_close[pid] = time.time()
            log.info(f'[MOM] CLOSE {tr["id"]} {pid} {reason} pnl=R${pnl:,.0f} '
                     f'({pnl_pct:+.2f}%) z_entry={tr.get("entry_z"):.2f}->{z_now:.2f}')
        else:
            # throttle update parcial no DB (memoria ja esta live)
            if time.time() - _last_partial.get(tr['id'], 0) >= MOM_PARTIAL_TTL_S:
                _last_partial[tr['id']] = time.time()
                cur.execute("""UPDATE pairs_momentum_shadow SET current_z=%s,pnl=%s,
                    pnl_pct=%s,updated_at=NOW() WHERE id=%s""",
                    (z_now, round(pnl, 2), round(pnl_pct, 4), tr['id']))
                cur.close(); c.commit(); c.close()
            else:
                cur.close(); c.close()
    except Exception as e:
        log.debug(f'[MOM] monitor {pid}: {e}')
        try: c.close()
        except: pass


def summary():
    """Resumo pro dashboard/endpoint shadow."""
    _boot_load()
    out = {'enabled': MOM_ENABLED, 'abertas': len(_open), 'fechadas': 0,
           'wins': 0, 'pnl_fechado': 0.0, 'pnl_parcial_aberto': 0.0,
           'por_reason': {}, 'trades': []}
    for tr in _open.values():
        out['pnl_parcial_aberto'] += float(tr.get('pnl', 0) or 0)
        out['trades'].append({'id': tr['id'], 'pair_id': tr['pair_id'],
            'dir': tr['direction'], 'status': 'OPEN', 'entry_z': tr.get('entry_z'),
            'current_z': tr.get('current_z'), 'parcial': round(float(tr.get('pnl', 0) or 0), 2)})
    c = _conn()
    if c:
        try:
            cur = c.cursor()
            _ensure_table(cur)
            cur.execute("""SELECT COUNT(*),SUM(pnl>0),SUM(pnl) FROM pairs_momentum_shadow
                           WHERE status='CLOSED'""")
            n, w, pnl = cur.fetchone()
            out['fechadas'] = int(n or 0); out['wins'] = int(w or 0)
            out['pnl_fechado'] = round(float(pnl or 0), 2)
            cur.execute("""SELECT close_reason,COUNT(*),ROUND(SUM(pnl),0)
                           FROM pairs_momentum_shadow WHERE status='CLOSED'
                           GROUP BY close_reason""")
            for r in cur.fetchall():
                out['por_reason'][r[0]] = {'n': int(r[1]), 'pnl': float(r[2] or 0)}
            cur.close(); c.close()
        except Exception as e:
            log.debug(f'[MOM] summary: {e}')
            try: c.close()
            except: pass
    out['wr'] = round(out['wins'] / out['fechadas'] * 100, 1) if out['fechadas'] else None
    return out
