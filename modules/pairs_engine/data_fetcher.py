"""Data fetcher para pairs trade B3.

Fontes (em ordem de preferencia):
1. BRAPI    — gratis, simples, suficiente pra historico diario
2. Cedro    — datafeed proprietario (real-time + historico denso)
3. Polygon  — fallback US-listed only
"""
import os
import requests
import logging
import time
from typing import List, Dict, Optional

log = logging.getLogger('egreja.pairs')

BRAPI_TOKEN = os.environ.get('BRAPI_TOKEN', '').strip()
BRAPI_BASE = 'https://brapi.dev/api'

# Cache simples de quotes (5s TTL)
_quote_cache: Dict[str, tuple] = {}
_QUOTE_TTL_S = 5


def fetch_pair_history_brapi(symbol: str, days: int = 60) -> List[Dict]:
    """Historico diario via BRAPI (range max 1y free, 5y/10y/max planos pagos)."""
    try:
        if days <= 30: range_ = '1mo'
        elif days <= 90: range_ = '3mo'
        elif days <= 180: range_ = '6mo'
        elif days <= 365: range_ = '1y'
        elif days <= 730: range_ = '2y'
        elif days <= 1825: range_ = '5y'
        else: range_ = 'max'
        params = {'range': range_, 'interval': '1d'}
        if BRAPI_TOKEN:
            params['token'] = BRAPI_TOKEN
        r = requests.get(f'{BRAPI_BASE}/quote/{symbol}', params=params, timeout=25)
        if r.status_code != 200:
            log.warning(f'[BRAPI] {symbol}: HTTP {r.status_code} range={range_}')
            return []
        data = r.json().get('results', [{}])[0]
        hist = data.get('historicalDataPrice', []) or []
        from datetime import datetime as _dt
        out = []
        for h in hist[-days:]:
            ts = h.get('date'); close = h.get('close')
            if not ts or not close or close <= 0:
                continue
            try:
                dt = _dt.utcfromtimestamp(int(ts)).strftime('%Y-%m-%d')
                out.append({'date': dt, 'close': float(close),
                           'open': float(h.get('open', close) or close),
                           'high': float(h.get('high', close) or close),
                           'low': float(h.get('low', close) or close),
                           'volume': int(h.get('volume', 0) or 0),
                           'source': 'brapi'})
            except Exception: continue
        return out
    except Exception as e:
        log.warning(f'[BRAPI] fetch_pair_history {symbol}: {e}')
        return []


def fetch_pair_history_cedro(symbol: str, days: int = 60) -> List[Dict]:
    """Historico diario via Cedro Socket (datafeed pago, max ~10y)."""
    try:
        from modules.cedro_socket_provider import get_cedro
        cedro = get_cedro()
        if not cedro:
            return []
        # Cedro retorna lista cronologica (antigo -> recente)
        candles = cedro.get_candles_history(symbol, period='D', n_candles=days, wait_ms=8000)
        if not candles:
            return []
        out = []
        for c in candles:
            dt_str = c.get('datetime') or c.get('date')
            if not dt_str: continue
            # datetime do Cedro: 'YYYYMMDDHHMM' -> 'YYYY-MM-DD'
            try:
                if len(str(dt_str)) >= 8:
                    s = str(dt_str)
                    date_iso = f'{s[0:4]}-{s[4:6]}-{s[6:8]}'
                else:
                    date_iso = str(dt_str)
            except Exception: continue
            close = c.get('close') or c.get('c')
            if not close or float(close) <= 0:
                continue
            out.append({
                'date': date_iso,
                'open': float(c.get('open', close) or close),
                'high': float(c.get('high', close) or close),
                'low': float(c.get('low', close) or close),
                'close': float(close),
                'volume': int(c.get('volume', 0) or 0),
                'source': 'cedro',
            })
        return out
    except Exception as e:
        log.debug(f'[CEDRO] fetch_pair_history {symbol}: {e}')
        return []


def fetch_pair_history(symbol: str, days: int = 60, prefer_cedro: bool = False) -> List[Dict]:
    """Historico fusion BRAPI + Cedro. Cedro tem mais densidade quando disponivel.

    Strategy:
    - Default: BRAPI primary (mais estavel), Cedro fallback se BRAPI < 80% target
    - prefer_cedro=True: Cedro primary (usado em backfill onde queremos densidade)
    """
    if prefer_cedro:
        out = fetch_pair_history_cedro(symbol, days=days)
        if len(out) >= days * 0.5:
            return out
        # fallback BRAPI
        return fetch_pair_history_brapi(symbol, days=days)
    # default: BRAPI primary
    out = fetch_pair_history_brapi(symbol, days=days)
    if len(out) >= days * 0.5:
        return out
    log.info(f'[FUSION] {symbol}: BRAPI returned {len(out)}/{days}, tentando Cedro...')
    cedro_out = fetch_pair_history_cedro(symbol, days=days)
    if len(cedro_out) > len(out):
        return cedro_out
    return out


def fetch_pair_quote(symbol: str) -> Optional[Dict]:
    """Pega quote atual (last + bid/ask) via BRAPI.
    Cacheado por 5s para evitar rate limit.

    Returns:
        {'symbol': 'ITUB4', 'price': 40.49, 'bid': 40.48, 'ask': 40.50, 'volume': 1234567}
        None em erro.
    """
    now = time.time()
    cached = _quote_cache.get(symbol)
    if cached and (now - cached[0]) < _QUOTE_TTL_S:
        return cached[1]
    try:
        params = {}
        if BRAPI_TOKEN:
            params['token'] = BRAPI_TOKEN
        r = requests.get(f'{BRAPI_BASE}/quote/{symbol}', params=params, timeout=6)
        if r.status_code != 200:
            return None
        data = r.json().get('results', [{}])[0]
        price = float(data.get('regularMarketPrice') or 0)
        if price <= 0:
            return None
        result = {
            'symbol': symbol,
            'price': price,
            'bid': float(data.get('bid') or price),
            'ask': float(data.get('ask') or price),
            'volume': int(data.get('regularMarketVolume') or 0),
            'change_pct': float(data.get('regularMarketChangePercent') or 0),
        }
        _quote_cache[symbol] = (now, result)
        return result
    except Exception as e:
        log.debug(f'[BRAPI] fetch_pair_quote {symbol}: {e}')
        return None


_BRAPI_BULK_CHUNK = 20   # limite plano free BRAPI

def fetch_pair_quotes_bulk(symbols: List[str]) -> Dict[str, Dict]:
    """Pega quotes de varios simbolos via BRAPI em chunks de 20 (limite free plan)."""
    if not symbols:
        return {}
    out = {}
    # Quebrar em chunks de 20 (limite BRAPI free)
    chunks = [symbols[i:i + _BRAPI_BULK_CHUNK] for i in range(0, len(symbols), _BRAPI_BULK_CHUNK)]
    for chunk in chunks:
        try:
            params = {}
            if BRAPI_TOKEN:
                params['token'] = BRAPI_TOKEN
            syms_csv = ','.join(chunk)
            r = requests.get(f'{BRAPI_BASE}/quote/{syms_csv}', params=params, timeout=25)
            if r.status_code != 200:
                log.warning(f'[BRAPI] bulk chunk ({len(chunk)} syms) HTTP {r.status_code}')
                continue
            for d in r.json().get('results', []) or []:
                sym = d.get('symbol')
                price = float(d.get('regularMarketPrice') or 0)
                if not sym or price <= 0:
                    continue
                out[sym] = {
                    'symbol': sym, 'price': price,
                    'bid': float(d.get('bid') or price),
                    'ask': float(d.get('ask') or price),
                    'volume': int(d.get('regularMarketVolume') or 0),
                    'change_pct': float(d.get('regularMarketChangePercent') or 0),
                }
        except Exception as e:
            log.warning(f'[BRAPI] bulk chunk: {e}')
            continue
    return out
