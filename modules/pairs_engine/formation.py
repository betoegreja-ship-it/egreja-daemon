"""
═══════════════════════════════════════════════════════════════════════════
PAIRS FORMATION ENGINE — universo VIVO  [29-jul-2026, decisao Beto pos-conselho]
═══════════════════════════════════════════════════════════════════════════
Consolida o consenso GPT + Grok + Kimi: o problema nº 1 da Pairs B3 era o
universo morto. Este motor separa FORMACAO (janelas longas, semanal) de
MONITORAMENTO (60d horario, que continua no learning worker, mas que tem
pouco poder estatistico — por isso quase tudo la vira RANDOM_WALK).

Pipeline semanal (domingo ou no boot se nunca rodou):
  1. universo liquido B3 (~57 tickers) → historico diario (DB + brapi)
  2. corr de retornos 120d ≥ 0.60 → pre-filtro (limita custo computacional)
  3. Engle-Granger (regressao log-log) + ADF no residuo em 252d
  4. ESTABILIDADE POR METADES (corte do Kimi): ADF nas 2 metades do periodo
  5. half-life do residuo (AR1)

Tiers CONGELADOS (nao mudar sem ata):
  NUCLEO  : t252 ≤ -3.37 (~p<0.05 EG) E ambas metades t ≤ -2.86 E HL 3-45d
  PROVACAO: t252 ≤ -3.37 E HL 3-45d (falha estabilidade)
  MONITOR : t252 ≤ -3.07 (~p<0.10)
  FORA    : resto

Seeds (rastreio Kimi 29-jul, dados reais 2y): usados apenas se a formacao
ainda nao rodou — a primeira rodada substitui pelos numeros da casa.
Shadow-only: quem consome os tiers e o v2_shadow. NAO toca no book live.
"""
import os, time, math, logging, itertools
from datetime import datetime

log = logging.getLogger('egreja.pairs.formation')

FORMATION_INTERVAL_S = int(os.environ.get('PAIRS_FORMATION_INTERVAL_S', 6 * 86400))  # ~semanal
UNIVERSE = [s.strip().upper() for s in os.environ.get('PAIRS_FORMATION_UNIVERSE',
    'ABEV3,ALOS3,ASAI3,B3SA3,BBAS3,BBDC3,BBDC4,BBSE3,BPAC11,BRAP4,BRFS3,'
    'CMIG3,CMIG4,CPLE6,CSAN3,CSNA3,CYRE3,EGIE3,ELET3,ELET6,EMBR3,ENEV3,ENGI11,'
    'EQTL3,GGBR4,GOAU4,HAPV3,HYPE3,ITSA4,ITUB4,KLBN11,LREN3,MULT3,PETR3,PETR4,'
    'PRIO3,RADL3,RAIL3,RDOR3,RENT3,SANB11,SBSP3,SAPR11,SUZB3,TAEE11,TIMS3,'
    'TOTS3,UGPA3,USIM5,VALE3,VBBR3,VIVT3,WEGE3').split(',') if s.strip()]
MIN_CORR = float(os.environ.get('PAIRS_FORMATION_MIN_CORR', 0.60))
MAX_CANDIDATES = int(os.environ.get('PAIRS_FORMATION_MAX_CANDIDATES', 180))
T_FULL = -3.37       # ~p 0.05 Engle-Granger (2 vars, constante)
T_MONITOR = -3.07    # ~p 0.10
T_HALF = -2.86       # barra mais frouxa p/ metades (n~126)
HL_MIN, HL_MAX = 3.0, 45.0

# Seeds do rastreio independente do Kimi (29-jul-2026, 54 tickers, 2 anos)
SEED_TIERS = {
    'ENGI11-EQTL3': 'NUCLEO', 'ENGI11-MULT3': 'NUCLEO',
    'EQTL3-EGIE3': 'PROVACAO', 'PETR4-PRIO3': 'PROVACAO', 'PETR3-PRIO3': 'PROVACAO',
    'GGBR4-GOAU4': 'PROVACAO', 'ENGI11-EGIE3': 'PROVACAO',
}

_tier_cache = {'ts': 0, 'tiers': {}, 'meta': {}}


def _conn():
    try:
        from .persistence import _get_conn
        return _get_conn()
    except Exception as e:
        log.debug(f'[FORM] conn: {e}')
        return None


def _ensure_tables(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS pairs_formation_runs (
        id BIGINT AUTO_INCREMENT PRIMARY KEY, started_at DATETIME, finished_at DATETIME,
        n_tickers INT, n_candidates INT, n_nucleo INT, n_provacao INT, n_monitor INT,
        note VARCHAR(200))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS pairs_formation (
        id BIGINT AUTO_INCREMENT PRIMARY KEY, run_id BIGINT, ts DATETIME,
        pair_id VARCHAR(24), leg_a VARCHAR(12), leg_b VARCHAR(12),
        corr120 DOUBLE, t120 DOUBLE, t180 DOUBLE, t252 DOUBLE,
        t_half1 DOUBLE, t_half2 DOUBLE, half_life_d DOUBLE, hedge_beta DOUBLE,
        tier VARCHAR(10), INDEX ix_run (run_id), INDEX ix_pair (pair_id))""")


def _returns(prices):
    return [math.log(prices[i] / prices[i - 1]) for i in range(1, len(prices))
            if prices[i - 1] > 0 and prices[i] > 0]


def _corr(xs, ys):
    n = min(len(xs), len(ys))
    if n < 30: return 0.0
    xs, ys = xs[-n:], ys[-n:]
    mx, my = sum(xs) / n, sum(ys) / n
    cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    vx = sum((x - mx) ** 2 for x in xs); vy = sum((y - my) ** 2 for y in ys)
    d = math.sqrt(vx * vy)
    return cov / d if d > 1e-12 else 0.0


def _eg_residual(la, lb):
    """Regressao EG log(A) = a + b*log(B); retorna (residuos, beta)."""
    n = len(la)
    mb = sum(lb) / n; ma = sum(la) / n
    cov = sum((lb[i] - mb) * (la[i] - ma) for i in range(n))
    var = sum((x - mb) ** 2 for x in lb)
    beta = cov / var if var > 1e-12 else 1.0
    alpha = ma - beta * mb
    return [la[i] - alpha - beta * lb[i] for i in range(n)], beta


def _adf_t(resid):
    """ADF sem lags (DF): d_e[t] = rho*e[t-1] + err; t-stat de rho."""
    n = len(resid)
    if n < 40: return 0.0
    de = [resid[i] - resid[i - 1] for i in range(1, n)]
    lag = resid[:-1]
    m = len(de)
    sxy = sum(lag[i] * de[i] for i in range(m)); sxx = sum(x * x for x in lag)
    if sxx < 1e-12: return 0.0
    rho = sxy / sxx
    sse = sum((de[i] - rho * lag[i]) ** 2 for i in range(m))
    s2 = sse / max(m - 1, 1)
    se = math.sqrt(s2 / sxx) if sxx > 0 else 1.0
    return rho / se if se > 0 else 0.0


def _half_life_d(resid):
    n = len(resid)
    if n < 40: return 999.0
    de = [resid[i] - resid[i - 1] for i in range(1, n)]
    lag = resid[:-1]
    m = len(de)
    sxy = sum(lag[i] * de[i] for i in range(m)); sxx = sum(x * x for x in lag)
    if sxx < 1e-12: return 999.0
    rho = sxy / sxx
    if rho >= 0: return 999.0
    hl = -math.log(2) / math.log(1 + rho) if (1 + rho) > 0 else 999.0
    return min(hl, 999.0)


def run_formation(beat_fn=None):
    """Uma rodada completa. Retorna dict resumo."""
    from . import persistence as _persist
    from .data_fetcher import fetch_pair_history
    t0 = datetime.utcnow()
    logs = {}

    # 1. historico por ticker (DB primeiro; fetch se insuficiente)
    hist = {}
    for sym in UNIVERSE:
        if beat_fn:
            try: beat_fn('pairs_formation_loop')
            except Exception: pass
        try:
            bars = _persist.load_history_from_db(sym, days=300)
            if len(bars) < 200:
                fresh = fetch_pair_history(sym, days=300)
                if fresh:
                    try: _persist.bulk_upsert_daily_bars(sym, fresh, source=fresh[0].get('source', 'brapi'))
                    except Exception: pass
                    merged = {b['date']: b for b in bars}
                    for b in fresh: merged[b['date']] = b
                    bars = sorted(merged.values(), key=lambda x: x['date'])
                time.sleep(0.25)
            if len(bars) >= 150:
                hist[sym] = bars
        except Exception as e:
            log.debug(f'[FORM] hist {sym}: {e}')
    log.info(f'[FORM] historico ok para {len(hist)}/{len(UNIVERSE)} tickers')

    # 2. pre-filtro por correlacao de retornos 120d
    rets = {}
    for s, bars in hist.items():
        px = [float(b['close']) for b in bars[-121:]]
        rets[s] = _returns(px)
    cands = []
    for a, b in itertools.combinations(sorted(hist.keys()), 2):
        c = _corr(rets.get(a, []), rets.get(b, []))
        if c >= MIN_CORR:
            cands.append((a, b, c))
    cands.sort(key=lambda x: -x[2])
    cands = cands[:MAX_CANDIDATES]
    log.info(f'[FORM] {len(cands)} pares candidatos (corr120 >= {MIN_CORR})')

    # 3-5. EG + ADF multi-janela + metades + half-life
    rows = []
    for a, b, c in cands:
        if beat_fn:
            try: beat_fn('pairs_formation_loop')
            except Exception: pass
        try:
            da = {x['date']: float(x['close']) for x in hist[a]}
            db_ = {x['date']: float(x['close']) for x in hist[b]}
            common = sorted(set(da) & set(db_))
            if len(common) < 150: continue
            common = common[-252:]
            la = [math.log(da[d]) for d in common]
            lb = [math.log(db_[d]) for d in common]
            n = len(la)

            def _t(win):
                if n < win * 0.8: return None
                r, _ = _eg_residual(la[-win:] if n >= win else la, lb[-win:] if n >= win else lb)
                return round(_adf_t(r), 3)

            t120, t180 = _t(120), _t(180)
            r252, beta = _eg_residual(la, lb)
            t252 = round(_adf_t(r252), 3)
            half = n // 2
            r1, _ = _eg_residual(la[:half], lb[:half])
            r2, _ = _eg_residual(la[half:], lb[half:])
            th1, th2 = round(_adf_t(r1), 3), round(_adf_t(r2), 3)
            hl = round(_half_life_d(r252), 1)

            stable = th1 <= T_HALF and th2 <= T_HALF
            hl_ok = HL_MIN <= hl <= HL_MAX
            if t252 <= T_FULL and stable and hl_ok: tier = 'NUCLEO'
            elif t252 <= T_FULL and hl_ok: tier = 'PROVACAO'
            elif t252 <= T_MONITOR: tier = 'MONITOR'
            else: tier = 'FORA'
            rows.append({'pair_id': f'{a}-{b}', 'leg_a': a, 'leg_b': b, 'corr120': round(c, 3),
                         't120': t120, 't180': t180, 't252': t252, 't_half1': th1, 't_half2': th2,
                         'half_life_d': hl, 'hedge_beta': round(beta, 4), 'tier': tier})
        except Exception as e:
            log.debug(f'[FORM] {a}-{b}: {e}')

    n_nu = sum(1 for r in rows if r['tier'] == 'NUCLEO')
    n_pr = sum(1 for r in rows if r['tier'] == 'PROVACAO')
    n_mo = sum(1 for r in rows if r['tier'] == 'MONITOR')

    # persistir
    c = _conn()
    run_id = None
    if c:
        try:
            cur = c.cursor(); _ensure_tables(cur)
            cur.execute("""INSERT INTO pairs_formation_runs
                (started_at, finished_at, n_tickers, n_candidates, n_nucleo, n_provacao, n_monitor, note)
                VALUES (%s,NOW(),%s,%s,%s,%s,%s,%s)""",
                (t0, len(hist), len(cands), n_nu, n_pr, n_mo, 'weekly formation'))
            run_id = cur.lastrowid
            for r in rows:
                if r['tier'] == 'FORA': continue  # so grava o funil util
                cur.execute("""INSERT INTO pairs_formation
                    (run_id, ts, pair_id, leg_a, leg_b, corr120, t120, t180, t252,
                     t_half1, t_half2, half_life_d, hedge_beta, tier)
                    VALUES (%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (run_id, r['pair_id'], r['leg_a'], r['leg_b'], r['corr120'], r['t120'],
                     r['t180'], r['t252'], r['t_half1'], r['t_half2'], r['half_life_d'],
                     r['hedge_beta'], r['tier']))
            c.commit(); cur.close(); c.close()
        except Exception as e:
            log.warning(f'[FORM] persist: {e}')
            try: c.close()
            except Exception: pass

    _tier_cache['ts'] = 0  # invalida cache
    top = sorted([r for r in rows if r['tier'] in ('NUCLEO', 'PROVACAO')], key=lambda x: x['t252'])
    log.info(f'[FORM] run#{run_id}: {n_nu} NUCLEO, {n_pr} PROVACAO, {n_mo} MONITOR | '
             f'top: {[(r["pair_id"], r["tier"], r["t252"], r["half_life_d"]) for r in top[:8]]}')
    return {'run_id': run_id, 'nucleo': n_nu, 'provacao': n_pr, 'monitor': n_mo,
            'candidates': len(cands), 'tickers': len(hist)}


def current_tiers():
    """{pair_id: {'tier','half_life_d','hedge_beta','leg_a','leg_b','t252'}} da ultima rodada.
    Fallback: seeds do Kimi enquanto a formacao nunca rodou. Cache 30min."""
    if time.time() - _tier_cache['ts'] < 1800 and _tier_cache['tiers']:
        return _tier_cache['tiers']
    out = {}
    c = _conn()
    if c:
        try:
            cur = c.cursor(); _ensure_tables(cur)
            cur.execute("SELECT MAX(run_id) FROM pairs_formation")
            row = cur.fetchone()
            rid = row[0] if row else None
            if rid:
                cur.execute("""SELECT pair_id, leg_a, leg_b, tier, half_life_d, hedge_beta, t252
                               FROM pairs_formation WHERE run_id=%s""", (rid,))
                for pid, a, b, tier, hl, beta, t in cur.fetchall():
                    out[pid] = {'tier': tier, 'leg_a': a, 'leg_b': b,
                                'half_life_d': float(hl or 10), 'hedge_beta': float(beta or 1),
                                't252': float(t or 0)}
            cur.close(); c.commit(); c.close()
        except Exception as e:
            log.debug(f'[FORM] current_tiers: {e}')
            try: c.close()
            except Exception: pass
    if not out:  # seeds Kimi (fallback ate a 1a rodada)
        for pid, tier in SEED_TIERS.items():
            a, b = pid.split('-')
            out[pid] = {'tier': tier, 'leg_a': a, 'leg_b': b, 'half_life_d': 10.0,
                        'hedge_beta': 1.0, 't252': None, 'seed': True}
    _tier_cache.update({'ts': time.time(), 'tiers': out})
    return out


def _last_run_age_s():
    c = _conn()
    if not c: return 1e12
    try:
        cur = c.cursor(); _ensure_tables(cur)
        cur.execute("SELECT MAX(finished_at) FROM pairs_formation_runs")
        row = cur.fetchone(); cur.close(); c.commit(); c.close()
        if row and row[0]:
            return (datetime.utcnow() - row[0]).total_seconds()
    except Exception:
        try: c.close()
        except Exception: pass
    return 1e12


def pairs_formation_loop(beat_fn=None):
    log.info(f'[FORM] worker iniciando | intervalo={FORMATION_INTERVAL_S}s '
             f'| universo={len(UNIVERSE)} tickers')
    time.sleep(240)  # deixa o app estabilizar
    while True:
        try:
            if beat_fn:
                try: beat_fn('pairs_formation_loop')
                except Exception: pass
            if _last_run_age_s() >= FORMATION_INTERVAL_S:
                log.info('[FORM] iniciando rodada de formacao...')
                run_formation(beat_fn=beat_fn)
        except Exception as e:
            log.error(f'[FORM] loop: {e}')
            import traceback; traceback.print_exc()
        time.sleep(21600)  # checa a cada 6h


def summary():
    tiers = current_tiers()
    out = {'pares': [], 'n_nucleo': 0, 'n_provacao': 0, 'n_monitor': 0, 'fonte': 'formation'}
    for pid, t in sorted(tiers.items(), key=lambda kv: (kv[1]['tier'], kv[0])):
        if t.get('seed'): out['fonte'] = 'seeds_kimi (formacao ainda nao rodou)'
        if t['tier'] == 'NUCLEO': out['n_nucleo'] += 1
        elif t['tier'] == 'PROVACAO': out['n_provacao'] += 1
        elif t['tier'] == 'MONITOR': out['n_monitor'] += 1
        out['pares'].append({'pair': pid, 'tier': t['tier'], 'hl_d': t.get('half_life_d'),
                             't252': t.get('t252')})
    return out
