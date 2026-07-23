# -*- coding: utf-8 -*-
"""[22-jul-2026, decisao Beto] STOCKS INTEL — Fases A+B+C da mesa quant de acoes.

Motivo (medido em 22/07): correlacao do nosso score tecnico com o retorno
do dia = -0.07 (ZERO). Acao tem ancora economica que o candle nao ve.

  FASE B — INSTRUMENT MASTER + CORPORATE ACTIONS
    Cadastro central (nome, setor, moeda, CIK/ISIN) + proventos/splits.
    Fontes: Polygon reference (US: details/dividends/splits),
            brapi (B3: summaryProfile + cashDividends com JCP).
    Trava: entrada em dia de ex-provento/split nao vira sinal (gap falso).

  FASE A — FACTOR ENGINE (shadow)
    Scores cross-sectional por mercado: QUALITY (ROE, margem, alavancagem,
    crescimento), VALUE (earnings yield, book yield), MOMENTUM (retorno
    12m/3m). Percentil 0-100 dentro do mercado. Fontes: brapi financialData/
    defaultKeyStatistics (B3); Polygon financials + aggs (US).
    SHADOW: scores vao para features_json das trades + /debug/factors.
    NAO seleciona nem bloqueia nada ainda.

  FASE C — EARNINGS INTEL (shadow)
    FMP stable earnings-calendar (estimativa + realizado): surpresa de EPS
    e receita. Tag EARNINGS_TAILWIND / EARNINGS_HEADWIND por N dias apos o
    resultado. Vai para features + log; nao bloqueia.

Loop unico (core-only), TTLs proprios por camada. Tudo fail-open.
"""
import os, time, json, logging, threading
from datetime import datetime, timedelta, date

import requests
import pymysql

log = logging.getLogger('egreja.stocks.intel')

# caches em memoria (leitura rapida pelos gates/features)
_master = {}        # sym -> {name, sector, market, ...}
_ca_by_sym = {}     # sym -> [ {type, ex_date, amount, label}, ... ] (proximos/recentes)
_factors = {}       # sym -> {quality, value, momentum, composite, updated}
_earnings = {}      # sym -> {date, eps_est, eps_act, rev_est, rev_act, surprise_pct, tag, tag_until}
_state = {'last_master': 0, 'last_ca': 0, 'last_factors': 0, 'last_earn': 0}


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
    ddls = [
        """CREATE TABLE IF NOT EXISTS instrument_master (
            symbol VARCHAR(16) PRIMARY KEY, market VARCHAR(8), name VARCHAR(120),
            sector VARCHAR(80), industry VARCHAR(120), currency VARCHAR(8),
            cik VARCHAR(16), isin VARCHAR(16), shares_out BIGINT,
            adr_of VARCHAR(16), meta_json TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) CHARACTER SET utf8mb4""",
        """CREATE TABLE IF NOT EXISTS corporate_actions (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(16) NOT NULL, market VARCHAR(8), type VARCHAR(16),
            ex_date DATE, pay_date DATE, amount DECIMAL(16,6), ratio VARCHAR(16),
            label VARCHAR(32), source VARCHAR(16),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_ca (symbol, type, ex_date, label)
            ) CHARACTER SET utf8mb4""",
        """CREATE TABLE IF NOT EXISTS factor_scores (
            symbol VARCHAR(16) PRIMARY KEY, market VARCHAR(8),
            quality DECIMAL(6,2), value_ DECIMAL(6,2), momentum DECIMAL(6,2),
            composite DECIMAL(6,2), raw_json TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) CHARACTER SET utf8mb4""",
        """CREATE TABLE IF NOT EXISTS earnings_events (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(16) NOT NULL, report_date DATE,
            eps_est DECIMAL(16,4), eps_act DECIMAL(16,4),
            rev_est DECIMAL(20,0), rev_act DECIMAL(20,0),
            surprise_pct DECIMAL(10,2), tag VARCHAR(24),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_earn (symbol, report_date)
            ) CHARACTER SET utf8mb4""",
    ]
    c = _conn()
    try:
        cur = c.cursor()
        for d in ddls:
            cur.execute(d)
    finally:
        c.close()


def _jget(url, params=None, timeout=15):
    r = requests.get(url, params=params or {}, headers={'User-Agent': 'Mozilla/5.0'},
                     timeout=timeout)
    if r.status_code == 200:
        return r.json()
    return None


def _is_b3(sym):
    import re
    return bool(re.match(r'^[A-Z][A-Z0-9]{3}[0-9]{1,2}$', sym))


# ═══════════════ FASE B: master + corporate actions ═══════════════

def refresh_b3(symbols):
    """brapi: 1 chamada por papel traz perfil + fundamentos + dividendos."""
    tok = os.environ.get('BRAPI_TOKEN') or os.environ.get('BRAPI_KEY') or ''
    out_fund = {}
    c = _conn(); cur = c.cursor()
    try:
        for sym in symbols:
            try:
                d = _jget(f'https://brapi.dev/api/quote/{sym}',
                          {'modules': 'summaryProfile,financialData,defaultKeyStatistics',
                           'dividends': 'true', 'token': tok})
                if not d or not d.get('results'):
                    continue
                q = d['results'][0]
                prof = q.get('summaryProfile') or {}
                fd = q.get('financialData') or {}
                ks = q.get('defaultKeyStatistics') or {}
                _master[sym] = {'market': 'B3', 'name': q.get('longName') or q.get('shortName'),
                                'sector': prof.get('sector'), 'industry': prof.get('industry'),
                                'currency': q.get('currency') or 'BRL'}
                cur.execute("""INSERT INTO instrument_master (symbol,market,name,sector,industry,currency)
                    VALUES (%s,'B3',%s,%s,%s,%s) ON DUPLICATE KEY UPDATE name=VALUES(name),
                    sector=VALUES(sector), industry=VALUES(industry)""",
                    (sym, _master[sym]['name'], _master[sym]['sector'],
                     _master[sym]['industry'], _master[sym]['currency']))
                # corporate actions (dividendos + JCP)
                cas = []
                for dv in ((q.get('dividendsData') or {}).get('cashDividends') or [])[:12]:
                    exd = (dv.get('lastDatePrior') or '')[:10] or None
                    pay = (dv.get('paymentDate') or '')[:10] or None
                    if not exd and not pay:
                        continue
                    cas.append({'type': 'DIVIDEND', 'ex_date': exd, 'pay_date': pay,
                                'amount': dv.get('rate'), 'label': (dv.get('label') or '')[:30]})
                    cur.execute("""INSERT IGNORE INTO corporate_actions
                        (symbol,market,type,ex_date,pay_date,amount,label,source)
                        VALUES (%s,'B3','DIVIDEND',%s,%s,%s,%s,'brapi')""",
                        (sym, exd, pay, dv.get('rate'), (dv.get('label') or '')[:30]))
                _ca_by_sym[sym] = cas
                # fundamentos p/ factor engine
                out_fund[sym] = {
                    'roe': fd.get('returnOnEquity'), 'margin': fd.get('operatingMargins'),
                    'debt_eq': fd.get('debtToEquity'), 'growth': fd.get('earningsGrowth'),
                    'pe': ks.get('trailingPE'), 'pb': ks.get('priceToBook'),
                    'ev_ebitda': ks.get('enterpriseToEbitda'),
                    'mom_12m': ks.get('52WeekChange'), 'dy': ks.get('dividendYield'),
                }
                time.sleep(0.12)
            except Exception as e:
                log.debug(f'[INTEL-B3] {sym}: {e}')
    finally:
        c.close()
    return out_fund


def refresh_us(symbols):
    """Polygon: details + dividends + splits + financials + momentum (aggs)."""
    pk = os.environ.get('POLYGON_API_KEY', '')
    out_fund = {}
    c = _conn(); cur = c.cursor()
    try:
        for sym in symbols:
            try:
                det = (_jget(f'https://api.polygon.io/v3/reference/tickers/{sym}',
                             {'apiKey': pk}) or {}).get('results') or {}
                _master[sym] = {'market': 'NYSE', 'name': det.get('name'),
                                'sector': det.get('sic_description'), 'industry': None,
                                'currency': 'USD', 'cik': det.get('cik'),
                                'shares_out': det.get('weighted_shares_outstanding')}
                cur.execute("""INSERT INTO instrument_master (symbol,market,name,sector,currency,cik,shares_out)
                    VALUES (%s,'NYSE',%s,%s,'USD',%s,%s) ON DUPLICATE KEY UPDATE
                    name=VALUES(name), sector=VALUES(sector), cik=VALUES(cik),
                    shares_out=VALUES(shares_out)""",
                    (sym, det.get('name'), det.get('sic_description'), det.get('cik'),
                     det.get('weighted_shares_outstanding')))
                cas = []
                dv = (_jget('https://api.polygon.io/v3/reference/dividends',
                            {'ticker': sym, 'limit': 4, 'apiKey': pk}) or {}).get('results') or []
                for x in dv:
                    cas.append({'type': 'DIVIDEND', 'ex_date': x.get('ex_dividend_date'),
                                'pay_date': x.get('pay_date'), 'amount': x.get('cash_amount'),
                                'label': 'CASH'})
                    cur.execute("""INSERT IGNORE INTO corporate_actions
                        (symbol,market,type,ex_date,pay_date,amount,label,source)
                        VALUES (%s,'NYSE','DIVIDEND',%s,%s,%s,'CASH','polygon')""",
                        (sym, x.get('ex_dividend_date'), x.get('pay_date'), x.get('cash_amount')))
                sp = (_jget('https://api.polygon.io/v3/reference/splits',
                            {'ticker': sym, 'limit': 2, 'apiKey': pk}) or {}).get('results') or []
                for x in sp:
                    cas.append({'type': 'SPLIT', 'ex_date': x.get('execution_date'),
                                'ratio': f"{x.get('split_from')}:{x.get('split_to')}", 'label': 'SPLIT'})
                    cur.execute("""INSERT IGNORE INTO corporate_actions
                        (symbol,market,type,ex_date,ratio,label,source)
                        VALUES (%s,'NYSE','SPLIT',%s,%s,'SPLIT','polygon')""",
                        (sym, x.get('execution_date'), f"{x.get('split_from')}:{x.get('split_to')}"))
                _ca_by_sym[sym] = cas
                # fundamentos (XBRL) + momentum
                fin = (_jget('https://api.polygon.io/vX/reference/financials',
                             {'ticker': sym, 'limit': 1, 'apiKey': pk}) or {}).get('results') or []
                roe = margin = debt_eq = None
                ep = None
                if fin:
                    f = fin[0].get('financials') or {}
                    # anualizar SO se o filing e trimestral (annual: x1)
                    _mult = 4 if fin[0].get('timeframe') == 'quarterly' else 1
                    inc = f.get('income_statement') or {}
                    bal = f.get('balance_sheet') or {}
                    ni = ((inc.get('net_income_loss') or {}).get('value'))
                    rev = ((inc.get('revenues') or {}).get('value'))
                    eq = ((bal.get('equity') or {}).get('value'))
                    liab = ((bal.get('liabilities') or {}).get('value'))
                    if ni is not None and eq: roe = ni * _mult / eq
                    if ni is not None and rev: margin = ni / rev
                    if liab is not None and eq: debt_eq = liab / eq
                    sh = det.get('weighted_shares_outstanding')
                    if ni is not None and sh: ep = ni * _mult / sh
                mom = None; px = None
                ag = (_jget(f'https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day/'
                            f'{(date.today()-timedelta(days=380)).isoformat()}/{date.today().isoformat()}',
                            {'adjusted': 'true', 'sort': 'asc', 'limit': 300, 'apiKey': pk}) or {}).get('results') or []
                if len(ag) > 30:
                    px = ag[-1]['c']
                    mom = (px / ag[0]['c'] - 1)
                pe = (px / ep) if (px and ep and ep > 0) else None
                out_fund[sym] = {'roe': roe, 'margin': margin, 'debt_eq': debt_eq,
                                 'growth': None, 'pe': pe, 'pb': None, 'ev_ebitda': None,
                                 'mom_12m': mom, 'dy': None}
                time.sleep(0.15)
            except Exception as e:
                log.debug(f'[INTEL-US] {sym}: {e}')
    finally:
        c.close()
    return out_fund


# ═══════════════ FASE A: factor engine ═══════════════

def _pct_rank(vals, v, invert=False):
    """percentil de v dentro de vals (0-100); invert p/ 'menor e melhor'."""
    vv = [x for x in vals if x is not None]
    if v is None or len(vv) < 5:
        return None
    below = sum(1 for x in vv if x < v)
    p = below / len(vv) * 100
    return round(100 - p if invert else p, 1)


def compute_factors(fund_by_sym, market):
    """Percentis cross-sectional dentro do mercado."""
    syms = list(fund_by_sym)
    cols = {k: [fund_by_sym[s].get(k) for s in syms]
            for k in ('roe', 'margin', 'debt_eq', 'growth', 'pe', 'pb', 'ev_ebitda', 'mom_12m', 'dy')}
    c = _conn(); cur = c.cursor()
    try:
        for s in syms:
            f = fund_by_sym[s]
            q_parts = [_pct_rank(cols['roe'], f.get('roe')),
                       _pct_rank(cols['margin'], f.get('margin')),
                       _pct_rank(cols['debt_eq'], f.get('debt_eq'), invert=True),
                       _pct_rank(cols['growth'], f.get('growth'))]
            v_parts = [_pct_rank(cols['pe'], f.get('pe'), invert=True),
                       _pct_rank(cols['pb'], f.get('pb'), invert=True),
                       _pct_rank(cols['ev_ebitda'], f.get('ev_ebitda'), invert=True),
                       _pct_rank(cols['dy'], f.get('dy'))]
            m_parts = [_pct_rank(cols['mom_12m'], f.get('mom_12m'))]
            def avg(parts):
                pp = [x for x in parts if x is not None]
                return round(sum(pp) / len(pp), 1) if pp else None
            qual, val, mom = avg(q_parts), avg(v_parts), avg(m_parts)
            comp = avg([qual, val, mom])
            _factors[s] = {'market': market, 'quality': qual, 'value': val,
                           'momentum': mom, 'composite': comp, 'updated': time.time()}
            cur.execute("""INSERT INTO factor_scores (symbol,market,quality,value_,momentum,composite,raw_json)
                VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE quality=VALUES(quality),
                value_=VALUES(value_), momentum=VALUES(momentum), composite=VALUES(composite),
                raw_json=VALUES(raw_json)""",
                (s, market, qual, val, mom, comp, json.dumps(f, default=str)))
    finally:
        c.close()
    log.info(f'[FACTOR] {market}: {len(syms)} papeis ranqueados '
             f'(cobertura quality={sum(1 for s in syms if _factors.get(s,{}).get("quality") is not None)})')


# ═══════════════ FASE C: earnings intel ═══════════════

def refresh_earnings(universe_us):
    """FMP stable earnings-calendar: -10d..+7d; surpresa + tag."""
    key = os.environ.get('FMP_API_KEY') or os.environ.get('FMP_KEY') or ''
    if not key:
        return
    frm = (date.today() - timedelta(days=10)).isoformat()
    to = (date.today() + timedelta(days=7)).isoformat()
    rows = _jget('https://financialmodelingprep.com/stable/earnings-calendar',
                 {'from': frm, 'to': to, 'apikey': key}) or []
    uni = set(universe_us)
    tag_days = int(_env_f('EARNINGS_TAG_DAYS', 3))
    c = _conn(); cur = c.cursor()
    try:
        n = 0
        for r in rows:
            sym = (r.get('symbol') or '').upper()
            if sym not in uni:
                continue
            eps_e, eps_a = r.get('epsEstimated'), r.get('epsActual')
            rev_e, rev_a = r.get('revenueEstimated'), r.get('revenueActual')
            surprise = None; tag = None
            if eps_a is not None and eps_e:
                try:
                    surprise = round((float(eps_a) - float(eps_e)) / abs(float(eps_e)) * 100, 2)
                    if surprise >= _env_f('EARNINGS_SURPRISE_POS', 2.0):
                        tag = 'EARNINGS_TAILWIND'
                    elif surprise <= -_env_f('EARNINGS_SURPRISE_NEG', 2.0):
                        tag = 'EARNINGS_HEADWIND'
                except Exception:
                    pass
            rep_d = (r.get('date') or '')[:10]
            if tag and rep_d:
                try:
                    until = (datetime.strptime(rep_d, '%Y-%m-%d') + timedelta(days=tag_days))
                    if until >= datetime.utcnow():
                        _earnings[sym] = {'date': rep_d, 'surprise_pct': surprise,
                                          'tag': tag, 'tag_until': until.isoformat()}
                except Exception:
                    pass
            cur.execute("""INSERT INTO earnings_events (symbol,report_date,eps_est,eps_act,
                rev_est,rev_act,surprise_pct,tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE eps_act=VALUES(eps_act), rev_act=VALUES(rev_act),
                surprise_pct=VALUES(surprise_pct), tag=VALUES(tag)""",
                (sym, rep_d or None, eps_e, eps_a, rev_e, rev_a, surprise, tag))
            n += 1
        log.info(f'[EARNINGS-INTEL] {n} eventos do universo ({len(_earnings)} com tag ativa)')
    finally:
        c.close()


# ═══════════════ API publica (gates/features) ═══════════════

def get_factor_scores(sym):
    return _factors.get(sym)


def earnings_tag(sym):
    e = _earnings.get(sym)
    if not e:
        return None
    try:
        if datetime.fromisoformat(e['tag_until']) < datetime.utcnow():
            return None
    except Exception:
        pass
    return e


def ca_event_today(sym):
    """Evento corporativo com ex-date HOJE (ou ontem p/ B3 'com') => gap falso.
    Retorna dict do evento ou None."""
    today = date.today().isoformat()
    yest = (date.today() - timedelta(days=1)).isoformat()
    for ev in _ca_by_sym.get(sym, []):
        exd = ev.get('ex_date') or ''
        if ev['type'] == 'SPLIT' and exd == today:
            return ev
        if ev['type'] == 'DIVIDEND' and (exd == today or (exd == yest and _is_b3(sym))):
            return ev
    return None


def snapshot():
    top = sorted([(s, f) for s, f in _factors.items() if f.get('composite') is not None],
                 key=lambda x: -x[1]['composite'])
    return {
        'master': len(_master), 'factors': len(_factors),
        'ca_symbols': len(_ca_by_sym), 'earnings_tags': {s: e for s, e in _earnings.items()},
        'top10': [(s, f['composite'], f['market']) for s, f in top[:10]],
        'bottom10': [(s, f['composite'], f['market']) for s, f in top[-10:]],
        'last_refresh': {k: (datetime.utcfromtimestamp(v).isoformat() if v else None)
                         for k, v in _state.items()},
    }


# ═══════════════ loop ═══════════════

def stocks_intel_loop(universe_fn, beat_fn=None):
    """universe_fn() -> lista de simbolos (B3+NYSE) do universo ativo."""
    if os.environ.get('STOCKS_INTEL_ENABLED', 'true').lower() == 'false':
        log.info('[STOCKS-INTEL] desabilitado via env')
        return
    try:
        create_tables()
        log.info('[STOCKS-INTEL] motor iniciado (master+CA+factors+earnings)')
    except Exception as e:
        log.error(f'[STOCKS-INTEL] tabelas: {e}')
        return
    while True:
        try:
            if beat_fn:
                beat_fn('stocks_intel_loop')
            uni = [s.upper() for s in (universe_fn() or [])]
            b3 = [s for s in uni if _is_b3(s)]
            us = [s for s in uni if not _is_b3(s)]
            now = time.time()
            # fundamentos/CA/master: a cada STOCKS_INTEL_FUND_TTL_H (12h)
            if now - _state['last_factors'] > _env_f('STOCKS_INTEL_FUND_TTL_H', 12) * 3600:
                if b3:
                    fb = refresh_b3(b3)
                    if fb:
                        compute_factors(fb, 'B3')
                if us:
                    fu = refresh_us(us)
                    if fu:
                        compute_factors(fu, 'NYSE')
                _state['last_factors'] = _state['last_master'] = _state['last_ca'] = now
            # earnings: a cada STOCKS_INTEL_EARN_TTL_H (4h)
            if us and now - _state['last_earn'] > _env_f('STOCKS_INTEL_EARN_TTL_H', 4) * 3600:
                refresh_earnings(us)
                _state['last_earn'] = now
        except Exception as e:
            log.error(f'[STOCKS-INTEL] loop: {e}')
        time.sleep(600)
