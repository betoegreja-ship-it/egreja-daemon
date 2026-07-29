"""
═══════════════════════════════════════════════════════════════════════════
PAIRS B3 v2 — DOIS BOOKS SHADOW  [29-jul-2026, decisao Beto pos-conselho]
═══════════════════════════════════════════════════════════════════════════
Implementa o desenho consolidado dos 3 conselheiros. 100% shadow (paper
virtual, tabelas proprias). NAO toca no book pairs live nem no Arbi.

Universo: tiers NUCLEO + PROVACAO do formation engine (funil vivo).

BOOK A (spec Kimi):
  entrada |z| 2.00-2.50 · z NAO-ALARGANDO ha 3 leituras · sem 2 primeiras
  horas do pregao (>=12:00 BRT) · 1 trade por episodio (rearme |z|<1.0)
BOOK B (spec GPT):
  entrada |z| 1.90-2.25 · CONFIRMACAO: |z| recuou do pico do episodio
  (pico > entrada minima e |z_now| < pico - 0.05 e recuando) · >=11:00 BRT

Saidas (ambos, consenso):
  TARGET_Z     |z| <= 0.50
  PRICE_STOP   |z| >= 4.00 (fora do universo de validade da tese)
  THESIS_STOP  ADF-120d do residuo degrada (t > -2.57 em 2 checks diarios)
  TIMEOUT_HL   idade > min(3 x half-life, 5 pregoes) — sai a mercado

Riscos: notional virtual R$100k fixo (sem multiplicador de conviccao),
max 4 posicoes/book, nenhuma perna compartilhada entre posicoes do book.
"""
import os, time, math, uuid, logging
from datetime import datetime, timezone, timedelta
from collections import deque

log = logging.getLogger('egreja.pairs.v2')

V2_ENABLED = os.environ.get('PAIRS_V2_ENABLED', 'true').lower() != 'false'
NOTIONAL = float(os.environ.get('PAIRS_V2_NOTIONAL', 100000))
MAX_OPEN = int(os.environ.get('PAIRS_V2_MAX_OPEN', 4))
Z_WINDOW = int(os.environ.get('PAIRS_V2_Z_WINDOW', 60))
REARM_Z = float(os.environ.get('PAIRS_V2_REARM_Z', 1.0))
TARGET_Z = float(os.environ.get('PAIRS_V2_TARGET_Z', 0.5))
PRICE_STOP_Z = float(os.environ.get('PAIRS_V2_PRICE_STOP_Z', 4.0))
TIMEOUT_CAP_D = float(os.environ.get('PAIRS_V2_TIMEOUT_CAP_D', 7.0))   # 5 pregoes ~ 7 corridos
THESIS_T = float(os.environ.get('PAIRS_V2_THESIS_T', -2.57))            # ~p0.10 DF
SCAN_SLEEP_S = int(os.environ.get('PAIRS_V2_SCAN_SLEEP_S', 120))

BOOKS = {
    'A': {'z_lo': 2.00, 'z_hi': 2.50, 'start_brt': 12.0, 'end_brt': 16.5, 'confirm': 'no_widen'},
    'B': {'z_lo': 1.90, 'z_hi': 2.25, 'start_brt': 11.0, 'end_brt': 16.5, 'confirm': 'pullback'},
}

# estado em memoria
_open = {'A': {}, 'B': {}}          # book -> {pair_id: trade}
_episode = {'A': {}, 'B': {}}       # book -> {pair_id: {'armed','peak','traded'}}
_zhist = {}                          # pair_id -> deque[(ts, z)]
_thesis = {}                         # pair_id -> {'day','bad_days','last_t'}
_booted = False


def _conn():
    try:
        from .persistence import _get_conn
        return _get_conn()
    except Exception as e:
        log.debug(f'[V2] conn: {e}')
        return None


def _ensure_table(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS pairs_v2_shadow_trades (
        id VARCHAR(40) PRIMARY KEY, book VARCHAR(2), pair_id VARCHAR(24), tier VARCHAR(10),
        direction VARCHAR(8), status VARCHAR(8) DEFAULT 'OPEN',
        entry_z DOUBLE, exit_z DOUBLE, hedge_beta DOUBLE, half_life_d DOUBLE,
        price_a_entry DOUBLE, price_b_entry DOUBLE, price_a_exit DOUBLE, price_b_exit DOUBLE,
        qty_a INT, qty_b INT, notional DOUBLE,
        opened_at DATETIME, closed_at DATETIME, close_reason VARCHAR(16),
        current_z DOUBLE, pnl DOUBLE, pnl_pct DOUBLE, updated_at DATETIME,
        INDEX ix_book (book), INDEX ix_pair (pair_id), INDEX ix_status (status))""")


def _boot():
    global _booted
    if _booted: return
    _booted = True
    c = _conn()
    if not c: return
    try:
        cur = c.cursor(); _ensure_table(cur)
        cur.execute("""SELECT id,book,pair_id,tier,direction,entry_z,hedge_beta,half_life_d,
                       price_a_entry,price_b_entry,qty_a,qty_b,notional,
                       UNIX_TIMESTAMP(opened_at) FROM pairs_v2_shadow_trades WHERE status='OPEN'""")
        for r in cur.fetchall():
            tr = {'id': r[0], 'book': r[1], 'pair_id': r[2], 'tier': r[3], 'direction': r[4],
                  'entry_z': r[5], 'hedge_beta': r[6], 'half_life_d': r[7],
                  'price_a_entry': r[8], 'price_b_entry': r[9], 'qty_a': r[10], 'qty_b': r[11],
                  'notional': r[12], 'opened_epoch': r[13] or time.time(), 'pnl': 0.0}
            if r[1] in _open:
                _open[r[1]][r[2]] = tr
                # episodio: marca como ja negociado ate rearme
                _episode[r[1]][r[2]] = {'armed': False, 'peak': abs(r[5] or 0), 'traded': True}
        cur.close(); c.commit(); c.close()
        n = sum(len(v) for v in _open.values())
        if n: log.info(f'[V2] boot: {n} posicoes shadow recarregadas')
    except Exception as e:
        log.debug(f'[V2] boot: {e}')
        try: c.close()
        except Exception: pass


def _brt_hour(now_utc):
    return (now_utc.hour - 3) % 24 + now_utc.minute / 60.0


def _market_open(now_utc):
    if now_utc.weekday() >= 5: return False
    h = _brt_hour(now_utc)
    return 10.0 <= h <= 17.0


def _signal(pair_id, meta, quotes):
    """z ao vivo do par: historico diario + quote atual, janela Z_WINDOW."""
    from .zscore import calc_spread_series, calc_zscore_stats, calc_hedge_ratio
    from .scanner import _refresh_history
    qa, qb = quotes.get(meta['leg_a']), quotes.get(meta['leg_b'])
    if not qa or not qb: return None
    ha, hb = _refresh_history(meta['leg_a']), _refresh_history(meta['leg_b'])
    if len(ha) < Z_WINDOW or len(hb) < Z_WINDOW: return None
    da = {h['date']: h['close'] for h in ha}
    db_ = {h['date']: h['close'] for h in hb}
    common = sorted(set(da) & set(db_))
    if len(common) < Z_WINDOW: return None
    pa = [da[d] for d in common] + [qa['price']]
    pb = [db_[d] for d in common] + [qb['price']]
    spread = calc_spread_series(pa, pb, method='log_ratio')
    stats = calc_zscore_stats(spread, window=Z_WINDOW)
    if stats.get('z') is None: return None
    beta = calc_hedge_ratio(pa, pb, window=Z_WINDOW) or meta.get('hedge_beta', 1.0)
    return {'z': stats['z'], 'price_a': qa['price'], 'price_b': qb['price'],
            'hedge': abs(beta), 'resid_hist': spread}


def _widening_streak(pair_id, z):
    """Atualiza historico de z e retorna nº de leituras consecutivas com |z| crescendo."""
    dq = _zhist.setdefault(pair_id, deque(maxlen=8))
    dq.append((time.time(), z))
    vals = [abs(x[1]) for x in dq]
    streak = 0
    for i in range(len(vals) - 1, 0, -1):
        if vals[i] > vals[i - 1] + 1e-9: streak += 1
        else: break
    return streak


def _episode_update(book, pair_id, absz, z_lo):
    """Maquina de episodio: arma quando |z| cruza z_lo vindo de baixo do rearme;
    reseta quando |z| < REARM_Z. 1 trade por episodio."""
    ep = _episode[book].setdefault(pair_id, {'armed': False, 'peak': 0.0, 'traded': False})
    if absz < REARM_Z:
        ep.update({'armed': True, 'peak': 0.0, 'traded': False})
    if ep['armed'] and absz > ep['peak']:
        ep['peak'] = absz
    return ep


def _pnl(tr, pa, pb):
    if tr['direction'] == 'SHORT_A':
        return tr['qty_a'] * (tr['price_a_entry'] - pa) + tr['qty_b'] * (pb - tr['price_b_entry'])
    return tr['qty_a'] * (pa - tr['price_a_entry']) + tr['qty_b'] * (tr['price_b_entry'] - pb)


def _thesis_check(pair_id, resid_hist):
    """1x/dia: ADF-120 do residuo. 2 dias ruins consecutivos -> True (stop de tese)."""
    from .formation import _adf_t
    today = datetime.utcnow().strftime('%Y-%m-%d')
    st = _thesis.setdefault(pair_id, {'day': None, 'bad_days': 0, 'last_t': None})
    if st['day'] == today:
        return st['bad_days'] >= 2
    st['day'] = today
    try:
        t = _adf_t(resid_hist[-120:]) if len(resid_hist) >= 60 else -9
        st['last_t'] = round(t, 3)
        st['bad_days'] = st['bad_days'] + 1 if t > THESIS_T else 0
    except Exception:
        pass
    return st['bad_days'] >= 2


def _persist_open(tr):
    c = _conn()
    if not c: return
    try:
        cur = c.cursor(); _ensure_table(cur)
        cur.execute("""INSERT INTO pairs_v2_shadow_trades
            (id,book,pair_id,tier,direction,status,entry_z,hedge_beta,half_life_d,
             price_a_entry,price_b_entry,qty_a,qty_b,notional,opened_at,current_z,pnl,pnl_pct,updated_at)
            VALUES (%s,%s,%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,0,0,NOW())""",
            (tr['id'], tr['book'], tr['pair_id'], tr['tier'], tr['direction'], tr['entry_z'],
             tr['hedge_beta'], tr['half_life_d'], tr['price_a_entry'], tr['price_b_entry'],
             tr['qty_a'], tr['qty_b'], tr['notional'], tr['entry_z']))
        c.commit(); cur.close(); c.close()
    except Exception as e:
        log.debug(f'[V2] persist open: {e}')
        try: c.close()
        except Exception: pass


def _persist_close(tr, z, pa, pb, reason, pnl, pnl_pct):
    c = _conn()
    if not c: return
    try:
        cur = c.cursor()
        cur.execute("""UPDATE pairs_v2_shadow_trades SET status='CLOSED',exit_z=%s,
            price_a_exit=%s,price_b_exit=%s,close_reason=%s,closed_at=NOW(),
            current_z=%s,pnl=%s,pnl_pct=%s,updated_at=NOW() WHERE id=%s""",
            (z, pa, pb, reason, z, round(pnl, 2), round(pnl_pct, 4), tr['id']))
        c.commit(); cur.close(); c.close()
    except Exception as e:
        log.debug(f'[V2] persist close: {e}')
        try: c.close()
        except Exception: pass


def _persist_partial(tr, z, pnl, pnl_pct):
    if time.time() - tr.get('_last_upd', 0) < 300: return
    tr['_last_upd'] = time.time()
    c = _conn()
    if not c: return
    try:
        cur = c.cursor()
        cur.execute("UPDATE pairs_v2_shadow_trades SET current_z=%s,pnl=%s,pnl_pct=%s,"
                    "updated_at=NOW() WHERE id=%s", (z, round(pnl, 2), round(pnl_pct, 4), tr['id']))
        c.commit(); cur.close(); c.close()
    except Exception as e:
        log.debug(f'[V2] persist partial: {e}')
        try: c.close()
        except Exception: pass


def _try_enter(book, cfg, pair_id, meta, sig, now_utc):
    ep = _episode[book].get(pair_id, {})
    absz = abs(sig['z'])
    if pair_id in _open[book]: return
    if not ep.get('armed') or ep.get('traded'): return
    if not (cfg['z_lo'] <= absz <= cfg['z_hi']): return
    h = _brt_hour(now_utc)
    if not (cfg['start_brt'] <= h <= cfg['end_brt']): return
    if len(_open[book]) >= MAX_OPEN: return
    # nenhuma perna compartilhada no book (cluster de fator comum)
    legs = {meta['leg_a'], meta['leg_b']}
    for tr in _open[book].values():
        m2 = tr.get('_legs') or set()
        if legs & m2: return
    # confirmacao por book
    if cfg['confirm'] == 'no_widen':
        if _widening_streak(pair_id, sig['z']) >= 3: return
    else:  # pullback (GPT)
        dq = _zhist.get(pair_id) or deque()
        prev = abs(dq[-2][1]) if len(dq) >= 2 else absz
        if not (ep.get('peak', 0) >= cfg['z_lo'] and absz <= ep['peak'] - 0.05 and absz < prev):
            return

    direction = 'SHORT_A' if sig['z'] > 0 else 'LONG_A'
    qty_a = int(NOTIONAL / 2 / max(sig['price_a'], 0.01))
    qty_b = int(qty_a * sig['hedge'])
    if qty_a <= 0 or qty_b <= 0: return
    tr = {'id': f'V2{book}-{uuid.uuid4().hex[:10]}', 'book': book, 'pair_id': pair_id,
          'tier': meta['tier'], 'direction': direction, 'entry_z': round(sig['z'], 4),
          'hedge_beta': round(sig['hedge'], 4), 'half_life_d': meta.get('half_life_d', 10.0),
          'price_a_entry': sig['price_a'], 'price_b_entry': sig['price_b'],
          'qty_a': qty_a, 'qty_b': qty_b, 'notional': NOTIONAL,
          'opened_epoch': time.time(), 'pnl': 0.0, '_legs': legs}
    _open[book][pair_id] = tr
    ep['traded'] = True
    _persist_open(tr)
    log.info(f'[V2-{book}] OPEN {tr["id"]} {pair_id} {direction} z={sig["z"]:+.2f} '
             f'tier={meta["tier"]} hl={tr["half_life_d"]:.0f}d')


def _manage(book, pair_id, meta, sig):
    tr = _open[book].get(pair_id)
    if not tr: return
    z = sig['z']; absz = abs(z)
    pnl = _pnl(tr, sig['price_a'], sig['price_b'])
    pnl_pct = 100 * pnl / max(tr['notional'], 1)
    tr['pnl'] = round(pnl, 2)
    age_d = (time.time() - tr['opened_epoch']) / 86400.0
    hl = float(tr.get('half_life_d') or 10.0)
    timeout_d = min(3 * hl, TIMEOUT_CAP_D)

    reason = None
    if absz <= TARGET_Z: reason = 'TARGET_Z'
    elif absz >= PRICE_STOP_Z: reason = 'PRICE_STOP'
    elif age_d > timeout_d: reason = 'TIMEOUT_HL'
    elif _thesis_check(pair_id, sig.get('resid_hist') or []): reason = 'THESIS_STOP'

    if reason:
        _persist_close(tr, round(z, 4), sig['price_a'], sig['price_b'], reason, pnl, pnl_pct)
        _open[book].pop(pair_id, None)
        log.info(f'[V2-{book}] CLOSE {tr["id"]} {pair_id} {reason} pnl=R${pnl:,.0f} '
                 f'({pnl_pct:+.2f}%) z={tr["entry_z"]:+.2f}->{z:+.2f} idade={age_d:.1f}d')
    else:
        _persist_partial(tr, round(z, 4), pnl, pnl_pct)


def pairs_v2_loop(beat_fn=None):
    if not V2_ENABLED:
        log.info('[V2] desabilitado via env'); return
    from .data_fetcher import fetch_pair_quotes_bulk
    from .formation import current_tiers
    log.info('[V2] books shadow A (Kimi 2.0-2.5 no-widen) e B (GPT 1.9-2.25 pullback) iniciando')
    time.sleep(180)
    _boot()
    while True:
        try:
            if beat_fn:
                try: beat_fn('pairs_v2_loop')
                except Exception: pass
            now = datetime.now(timezone.utc)
            if not _market_open(now):
                time.sleep(300); continue
            tiers = current_tiers()
            tradable = {pid: m for pid, m in tiers.items() if m['tier'] in ('NUCLEO', 'PROVACAO')}
            # posicoes abertas de pares que sairam do funil tambem precisam de gestao
            for book in BOOKS:
                for pid in list(_open[book].keys()):
                    if pid not in tradable and pid in tiers:
                        tradable[pid] = tiers[pid]
                    elif pid not in tiers:
                        # par sumiu da formacao: stop de tese imediato no proximo sinal
                        tradable[pid] = {'tier': 'FORA', 'leg_a': pid.split('-')[0],
                                         'leg_b': pid.split('-')[1], 'half_life_d': 10.0,
                                         'hedge_beta': 1.0}
            if not tradable:
                time.sleep(SCAN_SLEEP_S); continue
            syms = sorted({s for m in tradable.values() for s in (m['leg_a'], m['leg_b'])})
            quotes = fetch_pair_quotes_bulk(syms) or {}
            for pid, meta in tradable.items():
                sig = _signal(pid, meta, quotes)
                if not sig: continue
                absz = abs(sig['z'])
                for book, cfg in BOOKS.items():
                    _episode_update(book, pid, absz, cfg['z_lo'])
                    _manage(book, pid, meta, sig)
                    if meta['tier'] in ('NUCLEO', 'PROVACAO'):
                        _try_enter(book, cfg, pid, meta, sig, now)
        except Exception as e:
            log.error(f'[V2] loop: {e}')
            import traceback; traceback.print_exc()
        time.sleep(SCAN_SLEEP_S)


def summary():
    _boot()
    out = {'enabled': V2_ENABLED, 'books': {}}
    c = _conn()
    stats = {}
    if c:
        try:
            cur = c.cursor(); _ensure_table(cur)
            cur.execute("""SELECT book, COUNT(*), SUM(status='OPEN'), SUM(status='CLOSED'),
                           SUM(CASE WHEN status='CLOSED' AND pnl>0 THEN 1 ELSE 0 END),
                           SUM(CASE WHEN status='CLOSED' THEN pnl ELSE 0 END)
                           FROM pairs_v2_shadow_trades GROUP BY book""")
            for b, n, no, nc, w, pnl in cur.fetchall():
                stats[b] = {'n': int(n or 0), 'abertas': int(no or 0), 'fechadas': int(nc or 0),
                            'wins': int(w or 0), 'pnl_fechado': round(float(pnl or 0), 2)}
            cur.close(); c.commit(); c.close()
        except Exception as e:
            log.debug(f'[V2] summary: {e}')
            try: c.close()
            except Exception: pass
    for book, cfg in BOOKS.items():
        s = stats.get(book, {'n': 0, 'abertas': 0, 'fechadas': 0, 'wins': 0, 'pnl_fechado': 0.0})
        s['wr'] = round(s['wins'] / s['fechadas'] * 100, 1) if s['fechadas'] else None
        s['regra'] = (f"|z| {cfg['z_lo']}-{cfg['z_hi']} + "
                      f"{'z nao-alargando (Kimi)' if cfg['confirm']=='no_widen' else 'confirmacao de recuo (GPT)'}")
        s['parcial_abertas'] = round(sum(float(t.get('pnl', 0) or 0)
                                         for t in _open.get(book, {}).values()), 2)
        s['trades_abertas'] = [{'pair': t['pair_id'], 'dir': t['direction'],
                                'entry_z': t['entry_z'], 'parcial': t.get('pnl', 0)}
                               for t in _open.get(book, {}).values()]
        out['books'][book] = s
    try:
        from .formation import summary as fsum
        out['formation'] = fsum()
    except Exception:
        pass
    return out
