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


def fetch_pair_history(symbol: str, days: int = 60) -> List[Dict]:
    """Pega historico diario de fechamento via BRAPI.

    Args:
        symbol: ticker B3 (ex: 'ITUB4')
        days: numero de dias (60 default)

    Returns:
        Lista de dicts [{'date': '2026-06-24', 'close': 40.49}, ...]
        Lista vazia em caso de erro.
    """
    try:
        range_ = '3mo' if days <= 90 else '6mo' if days <= 180 else '1y'
        params = {'range': range_, 'interval': '1d'}
        if BRAPI_TOKEN:
            params['token'] = BRAPI_TOKEN
        r = requests.get(f'{BRAPI_BASE}/quote/{symbol}', params=params, timeout=10)
        if r.status_code != 200:
            log.warning(f'[BRAPI] {symbol}: HTTP {r.status_code}')
            return []
        data = r.json().get('results', [{}])[0]
        hist = data.get('historicalDataPrice', []) or []
        # Filtrar so close > 0 e ultimos `days` dias
        from datetime import datetime
        out = []
        for h in hist[-days:]:
            ts = h.get('date')
            close = h.get('close')
            if not ts or not close or close <= 0:
                continue
            try:
                dt = datetime.utcfromtimestamp(int(ts)).strftime('%Y-%m-%d')
                out.append({'date': dt, 'close': float(close),
                           'high': float(h.get('high', close) or close),
                           'low': float(h.get('low', close) or close),
                           'volume': int(h.get('volume', 0) or 0)})
            except Exception:
                continue
        return out
    except Exception as e:
        log.warning(f'[BRAPI] fetch_pair_history {symbol}: {e}')
        return []


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


def fetch_pair_quotes_bulk(symbols: List[str]) -> Dict[str, Dict]:
    """Pega quotes de varios simbolos numa unica chamada BRAPI."""
    if not symbols:
        return {}
    out = {}
    try:
        params = {}
        if BRAPI_TOKEN:
            params['token'] = BRAPI_TOKEN
        # BRAPI aceita lista CSV
        syms_csv = ','.join(symbols)
        r = requests.get(f'{BRAPI_BASE}/quote/{syms_csv}', params=params, timeout=10)
        if r.status_code != 200:
            log.warning(f'[BRAPI] bulk quote HTTP {r.status_code}')
            return {}
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
        return out
    except Exception as e:
        log.warning(f'[BRAPI] bulk quote: {e}')
        return {}
