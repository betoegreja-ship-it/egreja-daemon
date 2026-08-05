"""
═══════════════════════════════════════════════════════════════════════════
FX FEED — cambio real-time institucional  [29-jul-2026, ordem Beto]
═══════════════════════════════════════════════════════════════════════════
Prioridade USDBRL:
  1. CEDRO dolar futuro (tick real-time). 'DOLFUT'/'WDOFUT' continuo pode nao
     existir no plano — tentamos tambem os CONTRATOS reais (DOL<letra><aa>,
     WDO<letra><aa>) do mes da frente, assinando-os no socket na primeira vez.
     Ajuste de BASIS vs spot comercial (EWMA, clamp 0..0.6%).
  2. Spot comercial: BRAPI (token, quota folgada, cache 120s)
                     -> AwesomeAPI (cache 600s + backoff 30min em HTTP 429).
  3. frankfurter.dev (ECB diario) — marcado STALE.
Demais pares (EURUSD/GBPUSD/CADUSD/HKDUSD): BRAPI -> AwesomeAPI -> ECB.
ZERO Yahoo em qualquer etapa.
"""
import os, time, logging, requests
from datetime import date

log = logging.getLogger('egreja.fx')

_MONTH_CODES = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
_awes = {'ts': 0, 'data': {}, 'backoff_until': 0}
_brapi = {'ts': 0, 'data': {}}
_basis = {'val': None, 'ts': 0}
_dol_subscribed = False
_profit_dol = {'sub_ts': 0, 'px_ts': 0, 'cache': None}


def profit_dollar():
    """[05-ago | decisao Beto] DOL futuro da B3 via ProfitDLL (bridge do VPS).
    PRIMARIA do USDBRL na semana de teste: tick com timestamp (age_s), entao
    cambio parado e DETECTAVEL — exatamente o que faltou em 09/13/20-jul e
    28-29/jul (9 de 9 trades fx_suspect, -54.898 nos pares DOL).
    Retorna (preco, simbolo, idade_tick_s, stale) como cedro_dollar. Fail-open.
    Desligar: FX_USE_PROFIT=false."""
    if os.environ.get('FX_USE_PROFIT', 'true').lower() == 'false':
        return None, None, None, False
    url = (os.environ.get('PROFIT_BRIDGE_URL', '') or '').rstrip('/')
    tok = os.environ.get('PROFIT_BRIDGE_TOKEN', '')
    if not url:
        return None, None, None, False
    now = time.time()
    try:
        # garante a assinatura dos contratos DOL/WDO (bolsa F) a cada 10min
        if now - _profit_dol['sub_ts'] > 600:
            for s in _dollar_contract_candidates():
                try:
                    requests.post(url + '/subscribe', json={'symbol': s, 'bolsa': 'F'},
                                  headers={'X-Bridge-Token': tok}, timeout=3)
                except Exception:
                    pass
            _profit_dol['sub_ts'] = now
        if now - _profit_dol['px_ts'] > 2 or _profit_dol['cache'] is None:
            r = requests.get(url + '/quotes', headers={'X-Bridge-Token': tok}, timeout=2.5)
            j = r.json() or {}
            _profit_dol['cache'] = j.get('quotes') if isinstance(j.get('quotes'), dict) else j
            _profit_dol['px_ts'] = now
        best = None
        for s, q in (_profit_dol['cache'] or {}).items():
            if not (s.startswith('DOL') or s.startswith('WDO')):
                continue
            px = q.get('price') or q.get('last') or q.get('close')
            if not px:
                continue
            age = q.get('age_s')
            if age is not None:
                age = float(age) + (now - _profit_dol['px_ts'])
            if best is None or (age if age is not None else 9e9) < (best[2] if best[2] is not None else 9e9):
                best = (float(px), s, age)
        if best:
            px, sym, age = best
            if px > 100:          # DOL cota em pontos (R$/US$1000): 5.113,5 -> 5,1135
                px = px / 1000.0
            max_age = float(os.environ.get('FX_DOL_MAX_TICK_AGE_S', 120))
            stale = bool(_bmf_open() and age is not None and age > max_age)
            return px, f'PROFIT_{sym}', (round(age, 1) if age is not None else None), stale
    except Exception as e:
        log.debug(f'[FX] profit_dollar: {e}')
    return None, None, None, False


def _dollar_contract_candidates():
    """['DOLFUT','WDOFUT','DOLQ26','WDOQ26','DOLU26','WDOU26'] p/ hoje."""
    t = date.today()
    out = ['DOLFUT', 'WDOFUT']
    for k in (1, 2):   # frente = mes+1 (DOL vence no 1o dia util do mes)
        mth = (t.month - 1 + k) % 12
        yy = (t.year + ((t.month - 1 + k) // 12)) % 100
        out += [f'DOL{_MONTH_CODES[mth]}{yy:02d}', f'WDO{_MONTH_CODES[mth]}{yy:02d}']
    return out


from datetime import datetime, timezone


def _bmf_open():
    """Janela de negociacao do DOL futuro (aprox 9:00-18:10 BRT = 12:00-21:10 UTC, dias uteis)."""
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return False
    hm = now.hour * 60 + now.minute
    return (12 * 60) <= hm <= (21 * 60 + 10)


def _tick_age_s(q):
    """Idade do TICK (nao do nosso fetch), a partir do _updated_at do quote Cedro."""
    ts = (q or {}).get('_updated_at')
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace('Z', ''))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return None


def cedro_dollar(cedro):
    """Preco do dolar via Cedro. Retorna (preco, simbolo, idade_tick_s, stale_bool).
    stale=True quando estamos em horario de pregao MAS o tick esta velho (Cedro travou)."""
    global _dol_subscribed
    if not cedro or not getattr(cedro, 'enabled', False):
        return None, None, None, False
    cands = _dollar_contract_candidates()
    try:
        if not _dol_subscribed:
            cedro.subscribe(cands)
            _dol_subscribed = True
            time.sleep(1.0)
    except Exception as e:
        log.debug(f'[FX] subscribe dol: {e}')
    max_age = float(os.environ.get('FX_DOL_MAX_TICK_AGE_S', 120))
    for sym in cands:
        try:
            q = cedro.get_quote(sym, wait_ms=700) if hasattr(cedro, 'get_quote') else None
            p = (q or {}).get('price') if q else cedro.get_price(sym, wait_ms=700)
            if p and float(p) > 0:
                p = float(p)
                if p > 1000: p = p / 1000.0
                if 3.0 < p < 9.0:
                    age = _tick_age_s(q)
                    stale = bool(_bmf_open() and age is not None and age > max_age)
                    return p, sym, (round(age, 1) if age is not None else None), stale
        except Exception:
            continue
    return None, None, None, False


def _brapi_rates():
    """Spot via brapi (token). {'USDBRL':x,'EURUSD':y,...} cache 120s."""
    now = time.time()
    if now - _brapi['ts'] < 120 and _brapi['data']:
        return _brapi['data']
    tok = os.environ.get('BRAPI_TOKEN', '')
    if not tok: return _brapi['data']
    try:
        r = requests.get('https://brapi.dev/api/v2/currency',
                         params={'currency': 'USD-BRL,EUR-USD,GBP-USD,USD-CAD,USD-HKD',
                                 'token': tok}, timeout=8)
        if r.status_code == 200:
            out = {}
            for c in (r.json() or {}).get('currency', []) or []:
                try:
                    key = (c.get('fromCurrency', '') + c.get('toCurrency', '')).upper()
                    px = float(c.get('bidPrice') or c.get('askPrice') or 0)
                    if px > 0: out[key] = px
                except Exception:
                    continue
            if out:
                _brapi.update({'ts': now, 'data': out})
    except Exception as e:
        log.debug(f'[FX] brapi: {e}')
    return _brapi['data']


def _awesome_rates():
    """Spot via AwesomeAPI. Cache 600s; backoff 30min em 429 (QuotaExceeded)."""
    now = time.time()
    if now < _awes['backoff_until']: return _awes['data']
    if now - _awes['ts'] < 600 and _awes['data']: return _awes['data']
    try:
        r = requests.get('https://economia.awesomeapi.com.br/last/'
                         'USD-BRL,EUR-USD,GBP-USD,USD-CAD,USD-HKD',
                         headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        if r.status_code == 429:
            _awes['backoff_until'] = now + 1800
            log.warning('[FX] AwesomeAPI 429 — backoff 30min')
            return _awes['data']
        if r.status_code == 200:
            out = {}
            for k, v in (r.json() or {}).items():
                try:
                    bid, ask = float(v.get('bid', 0)), float(v.get('ask', 0))
                    mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else (bid or ask)
                    if mid > 0: out[k] = mid
                except Exception:
                    continue
            if out: _awes.update({'ts': now, 'data': out})
    except Exception as e:
        log.debug(f'[FX] awesome: {e}')
    return _awes['data']


def get_rates(cedro=None):
    """Retorna (rates: dict, meta: dict). Nunca levanta excecao."""
    now = time.time()
    rates, meta = {}, {}

    br = _brapi_rates()
    aw = _awesome_rates()
    spot = br.get('USDBRL') or aw.get('USDBRL')
    spot_src = 'BRAPI' if br.get('USDBRL') else ('AWESOMEAPI' if aw.get('USDBRL') else None)

    # [05-ago] ProfitDLL primeiro (tick com idade); Cedro vira backup do DOL
    dol, dol_sym, tick_age, dol_stale = profit_dollar()
    if not dol:
        dol, dol_sym, tick_age, dol_stale = cedro_dollar(cedro)
        if dol and dol_sym and not str(dol_sym).startswith('CEDRO_'):
            dol_sym = f'CEDRO_{dol_sym}'
    # GUARD: em pregao, se o tick do DOL estiver velho (Cedro travou), NAO usa o
    # futuro congelado — cai pro spot e marca STALE. Fora de pregao, o congelamento
    # e esperado (futuro nao negocia) e usamos o ultimo com fonte CEDRO_CLOSE.
    if dol and dol_stale and spot:
        rates['USDBRL'] = round(spot, 4)
        meta['USDBRL'] = {'source': f'{spot_src}_spot(DOL_STALE_{int(tick_age)}s)', 'ts': now,
                          'dol_tick_age_s': tick_age, 'stale': True}
        log.warning(f'[FX] DOL {dol_sym} tick velho ({tick_age}s) EM PREGAO — usando spot {spot_src}')
    elif dol and spot:
        b = max(0.0, min(dol - spot, 0.006 * dol))   # clamp 0..0.6%
        _basis['val'] = b if _basis['val'] is None else 0.7 * _basis['val'] + 0.3 * b
        _basis['ts'] = now
        rates['USDBRL'] = round(dol - _basis['val'], 4)
        _closed = '' if _bmf_open() else '_CLOSE'
        meta['USDBRL'] = {'source': f'{dol_sym}{_closed}+basis({spot_src})', 'ts': now,
                          'dolfut': round(dol, 4), 'basis': round(_basis['val'], 4),
                          'dol_tick_age_s': tick_age}
    elif dol and _basis['val'] is not None:
        rates['USDBRL'] = round(dol - _basis['val'], 4)
        _closed = '' if _bmf_open() else '_CLOSE'
        meta['USDBRL'] = {'source': f'{dol_sym}{_closed}+basis_cache', 'ts': now,
                          'dolfut': round(dol, 4), 'basis': round(_basis['val'], 4),
                          'dol_tick_age_s': tick_age}
    elif dol:
        rates['USDBRL'] = round(dol, 4)
        meta['USDBRL'] = {'source': f'{dol_sym}_raw', 'ts': now, 'dolfut': round(dol, 4),
                          'dol_tick_age_s': tick_age}
    elif spot:
        rates['USDBRL'] = round(spot, 4)
        meta['USDBRL'] = {'source': f'{spot_src}_spot', 'ts': now}

    # demais pares (brapi > awesome), mantendo formatos historicos do fx_rates
    def _pick(*keys):
        for src_name, src in (('BRAPI', br), ('AWESOMEAPI', aw)):
            for k in keys:
                if src.get(k): return src_name, src[k]
        return None, None
    s, v = _pick('EURUSD');  0
    if v: rates['EURUSD'] = round(v, 4); meta['EURUSD'] = {'source': s, 'ts': now}
    s, v = _pick('GBPUSD')
    if v: rates['GBPUSD'] = round(v, 4); meta['GBPUSD'] = {'source': s, 'ts': now}
    s, v = _pick('USDCAD')
    if v and v > 0: rates['CADUSD'] = round(1.0 / v, 4); meta['CADUSD'] = {'source': s, 'ts': now}
    s, v = _pick('USDHKD')
    if v: rates['HKDUSD'] = round(v, 4); meta['HKDUSD'] = {'source': s, 'ts': now}  # USD/HKD

    if rates.get('USDBRL'):
        return rates, meta

    # ultimo recurso: ECB diario
    try:
        r = requests.get('https://api.frankfurter.dev/v1/latest',
                         params={'base': 'USD', 'symbols': 'BRL,GBP,HKD,CAD,EUR'}, timeout=8)
        if r.status_code == 200:
            rr = r.json().get('rates', {})
            if rr.get('BRL'): rates['USDBRL'] = round(rr['BRL'], 4)
            if rr.get('GBP'): rates['GBPUSD'] = round(1.0 / rr['GBP'], 4)
            if rr.get('HKD'): rates['HKDUSD'] = round(rr['HKD'], 4)
            if rr.get('CAD'): rates['CADUSD'] = round(1.0 / rr['CAD'], 4)
            if rr.get('EUR'): rates['EURUSD'] = round(1.0 / rr['EUR'], 4)
            for k in rates: meta.setdefault(k, {'source': 'ECB_DIARIO_STALE', 'ts': now})
            log.warning('[FX] todas as fontes real-time falharam — ECB diario STALE')
    except Exception as e:
        log.warning(f'[FX] frankfurter.dev: {e}')
    return rates, meta
