# -*- coding: utf-8 -*-
"""[22-jul-2026, decisao Beto] MARKET DATA ENGINE — derivativos de cripto.

Motor 1 dos "5 motores": dados ANTECIPADORES (funding rate, open interest)
em vez de preco (retrovisor). Estudo causa-raiz de 22/07 provou que o motor
direcional opera com o vies de ontem e compra topo em dia de pump
(-$1.830/dia em 60 dias de BTC em alta). Funding esticado + OI inflado =
mercado lotado de comprado alavancado = topo perto — sinal que o candle
so mostra depois.

Fontes (fallback em cadeia, todas publicas, sem chave):
  1. Binance Futures (fapi)   — funding + OI
  2. Bybit v5 (linear)        — funding + OI num call
  3. OKX v5 (swap)            — funding; OI em call separado

Regimes:
  LONGS_LOTADOS   funding >= CRYPTO_DERIVS_FUNDING_HOT (0.03%/8h)
  SHORTS_LOTADOS  funding <= -CRYPTO_DERIVS_FUNDING_COLD (0.02%/8h)
  DELEVERAGING    OI caiu >= CRYPTO_DERIVS_OI_DUMP_PCT (5%) em 24h
  NEUTRO          resto

MODO SHADOW (default): so loga o que VETARIA, nao bloqueia nada.
CRYPTO_DERIVS_MODE=enforce passa a bloquear (so com aprovacao do Beto).
"""
import os, time, logging
import requests

log = logging.getLogger('egreja.crypto.derivs')

_cache = {'ts': 0, 'data': None}


def _env_f(name, default):
    try:
        return float(os.environ.get(name, default))
    except Exception:
        return float(default)


def _binance():
    """Funding atual + OI atual e 24h atras (fapi)."""
    base = 'https://fapi.binance.com'
    r = requests.get(f'{base}/fapi/v1/premiumIndex', params={'symbol': 'BTCUSDT'},
                     headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
    if r.status_code != 200:
        return None
    funding = float(r.json()['lastFundingRate']) * 100  # % por 8h
    oi_now = oi_24h = None
    try:
        r2 = requests.get(f'{base}/futures/data/openInterestHist',
                          params={'symbol': 'BTCUSDT', 'period': '1h', 'limit': 25},
                          headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        if r2.status_code == 200:
            h = r2.json()
            if isinstance(h, list) and len(h) >= 2:
                oi_now = float(h[-1]['sumOpenInterest'])
                oi_24h = float(h[0]['sumOpenInterest'])
    except Exception:
        pass
    return {'source': 'binance', 'funding_pct_8h': funding, 'oi_now': oi_now, 'oi_24h_ago': oi_24h}


def _bybit():
    r = requests.get('https://api.bybit.com/v5/market/tickers',
                     params={'category': 'linear', 'symbol': 'BTCUSDT'},
                     headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
    if r.status_code != 200:
        return None
    d = r.json()
    if d.get('retCode') != 0 or not d.get('result', {}).get('list'):
        return None
    t = d['result']['list'][0]
    funding = float(t.get('fundingRate') or 0) * 100
    oi_now = float(t.get('openInterest') or 0) or None
    oi_24h = None
    try:
        r2 = requests.get('https://api.bybit.com/v5/market/open-interest',
                          params={'category': 'linear', 'symbol': 'BTCUSDT',
                                  'intervalTime': '1h', 'limit': 24},
                          headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        if r2.status_code == 200:
            lst = r2.json().get('result', {}).get('list') or []
            if len(lst) >= 2:
                # bybit retorna desc (mais novo primeiro)
                oi_24h = float(lst[-1]['openInterest'])
                if oi_now is None:
                    oi_now = float(lst[0]['openInterest'])
    except Exception:
        pass
    return {'source': 'bybit', 'funding_pct_8h': funding, 'oi_now': oi_now, 'oi_24h_ago': oi_24h}


def _okx():
    r = requests.get('https://www.okx.com/api/v5/public/funding-rate',
                     params={'instId': 'BTC-USDT-SWAP'},
                     headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
    if r.status_code != 200:
        return None
    d = r.json()
    if d.get('code') != '0' or not d.get('data'):
        return None
    funding = float(d['data'][0]['fundingRate']) * 100
    return {'source': 'okx', 'funding_pct_8h': funding, 'oi_now': None, 'oi_24h_ago': None}


def get_derivs_snapshot(force=False):
    """Snapshot BTC: funding, OI 24h, regime. Cache CRYPTO_DERIVS_TTL_S (600s).
    None se nenhuma fonte respondeu (fail-open: quem chama segue sem o dado)."""
    ttl = _env_f('CRYPTO_DERIVS_TTL_S', 600)
    if not force and time.time() - _cache['ts'] < ttl and _cache['data']:
        return _cache['data']
    raw = None
    for fn in (_binance, _bybit, _okx):
        try:
            raw = fn()
            if raw and raw.get('funding_pct_8h') is not None:
                break
        except Exception as e:
            log.debug(f'[CRYPTO-DERIVS] {fn.__name__}: {e}')
            raw = None
    if not raw:
        log.warning('[CRYPTO-DERIVS] nenhuma fonte respondeu (binance/bybit/okx)')
        return None
    funding = raw['funding_pct_8h']
    oi_chg = None
    if raw.get('oi_now') and raw.get('oi_24h_ago'):
        oi_chg = (raw['oi_now'] / raw['oi_24h_ago'] - 1) * 100
    hot = _env_f('CRYPTO_DERIVS_FUNDING_HOT', 0.03)
    cold = _env_f('CRYPTO_DERIVS_FUNDING_COLD', 0.02)
    oi_dump = _env_f('CRYPTO_DERIVS_OI_DUMP_PCT', 5.0)
    if oi_chg is not None and oi_chg <= -oi_dump:
        regime = 'DELEVERAGING'
    elif funding >= hot:
        regime = 'LONGS_LOTADOS'
    elif funding <= -cold:
        regime = 'SHORTS_LOTADOS'
    else:
        regime = 'NEUTRO'
    data = {'source': raw['source'], 'funding_pct_8h': round(funding, 4),
            'oi_chg_24h_pct': round(oi_chg, 2) if oi_chg is not None else None,
            'regime': regime, 'ts': time.time()}
    _cache.update({'ts': time.time(), 'data': data})
    log.info(f"[CRYPTO-DERIVS] {regime} | funding {funding:+.4f}%/8h | "
             f"OI 24h {f'{oi_chg:+.1f}%' if oi_chg is not None else '?'} | fonte {raw['source']}")
    return data


def shadow_advice(direction):
    """Conselho do motor de derivativos p/ uma entrada. Retorna (would_veto,
    motivo). Em modo shadow o chamador SO LOGA; em enforce, bloqueia."""
    d = get_derivs_snapshot()
    if not d:
        return False, 'sem dados'
    r = d['regime']
    if direction == 'LONG' and r == 'LONGS_LOTADOS':
        return True, f"funding {d['funding_pct_8h']:+.3f}%/8h — comprados lotados/alavancados (topo perto)"
    if direction == 'SHORT' and r == 'SHORTS_LOTADOS':
        return True, f"funding {d['funding_pct_8h']:+.3f}%/8h — vendidos lotados (squeeze perto)"
    if direction == 'LONG' and r == 'DELEVERAGING':
        return True, f"OI {d['oi_chg_24h_pct']:+.1f}% em 24h — desalavancagem em curso"
    return False, r
