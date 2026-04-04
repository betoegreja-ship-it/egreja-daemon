"""[v10.4+] Pure stock fetching functions — parameterized, no global state.

This module contains:
  - EMA/RSI calculation functions (pure math)
  - Candle cache management functions
  - Individual stock fetchers (Polygon, brapi, FMP, Yahoo)
  - Score calculation functions

All functions receive their data/config as parameters. This module does NOT
access global variables like stock_prices or fx_rates — those are passed in.

To use in api_server.py:
  from modules.stock_fetcher import _ema, _rsi, _fetch_polygon_stock, _fetch_single_stock
"""

import time
import requests
from datetime import datetime, timedelta

# ═══════════════════════════════════════════════════════════════
# EMA AND RSI — Pure Mathematical Functions
# ═══════════════════════════════════════════════════════════════

def _ema(closes, period):
    """[v10.4] Exponential Moving Average.

    Pure mathematical function — no side effects.

    Args:
        closes (list): List of closing prices
        period (int): EMA period

    Returns:
        float: EMA value
    """
    if len(closes) < period:
        return closes[-1] if closes else 0
    k = 2.0 / (period + 1)
    ema = closes[0]
    for c in closes[1:]:
        ema = c * k + ema * (1 - k)
    return ema


def _rsi(closes, period=14):
    """[v10.4] Relative Strength Index.

    Pure mathematical function — no side effects.

    Args:
        closes (list): List of closing prices
        period (int): RSI period (default 14)

    Returns:
        float: RSI value (0-100)
    """
    if len(closes) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, period + 1):
        d = closes[-period + i] - closes[-period + i - 1]
        gains.append(d if d > 0 else 0)
        losses.append(abs(d) if d < 0 else 0)
    ag = sum(gains) / period
    al = sum(losses) / period
    if al == 0:
        return 100.0
    return round(100 - 100 / (1 + ag / al), 1)


def _calc_atr(closes: list, highs: list = None, lows: list = None, period: int = 14) -> float:
    """[v10.4] ATR simplificado. Se highs/lows não disponíveis, usa desvio de closes.

    Pure mathematical function — no side effects.

    Args:
        closes (list): List of closing prices
        highs (list): List of high prices (optional)
        lows (list): List of low prices (optional)
        period (int): ATR period (default 14)

    Returns:
        float: Average True Range
    """
    if len(closes) < 2:
        return 0.0
    if highs and lows and len(highs) == len(closes):
        trs = []
        for i in range(1, min(period + 1, len(closes))):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i - 1])
            lc = abs(lows[i] - closes[i - 1])
            trs.append(max(hl, hc, lc))
        return sum(trs) / len(trs) if trs else 0.0
    # Fallback: desvio médio absoluto dos closes
    n = min(period, len(closes))
    diffs = [abs(closes[i] - closes[i - 1]) for i in range(1, n + 1)]
    return sum(diffs) / len(diffs) if diffs else 0.0


# ═══════════════════════════════════════════════════════════════
# CANDLE CACHING — Parameterized (receives cache dict as param)
# ═══════════════════════════════════════════════════════════════

def _get_cached_candles(sym: str, cache_dict: dict, cache_lock, ttl_min: int = None) -> dict:
    """Retorna candles do cache se frescos, None caso contrário.

    Args:
        sym (str): Symbol identifier (e.g., 'polygon:AAPL', 'brapi:PETR4')
        cache_dict (dict): The cache dictionary to read from
        cache_lock: Threading lock for cache access
        ttl_min (int): TTL customizado em minutos. None usa CANDLES_CACHE_MIN padrão

    Returns:
        dict: Cached candles data if fresh, None otherwise
    """
    CANDLES_CACHE_MIN = 10  # Default from api_server
    ttl = (ttl_min if ttl_min is not None else CANDLES_CACHE_MIN) * 60
    with cache_lock:
        entry = cache_dict.get(sym)
    if entry and (time.time() - entry['ts']) < ttl:
        return entry['data']
    return None


def _set_cached_candles(sym: str, data: dict, cache_dict: dict, cache_lock):
    """Armazena candles em cache com timestamp.

    Args:
        sym (str): Symbol identifier
        data (dict): Candles data to cache
        cache_dict (dict): The cache dictionary to write to
        cache_lock: Threading lock for cache access
    """
    with cache_lock:
        cache_dict[sym] = {'data': data, 'ts': time.time()}


# ═══════════════════════════════════════════════════════════════
# POLYGON STOCK FETCHER
# ═══════════════════════════════════════════════════════════════

def _fetch_polygon_stock(ticker: str, api_key: str, cache_dict: dict, cache_lock, log=None) -> tuple:
    """[v10.4][v10.5-5] Polygon.io: snapshot de preço sempre fresco.
    Candles históricos (EMA/RSI/ATR/Volume) buscados só se cache > CANDLES_CACHE_MIN min.
    Reduz chamadas de API de ~4/min para ~1/10min por símbolo.

    Args:
        ticker (str): Stock ticker (e.g., 'AAPL', 'PETR4.SA')
        api_key (str): Polygon.io API key
        cache_dict (dict): Candle cache dictionary
        cache_lock: Threading lock for cache
        log: Logger object (optional)

    Returns:
        tuple: (result_dict | None, latency_ms)
    """
    t0 = time.time()
    try:
        # Snapshot para preço atual — sempre fresco
        r = requests.get(
            f'https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}',
            params={'apiKey': api_key}, timeout=8)
        lat = (time.time() - t0) * 1000
        if r.status_code != 200:
            return None, lat
        snap = r.json().get('ticker', {})
        day = snap.get('day', {})
        prev_day = snap.get('prevDay', {})
        price = float(day.get('c') or snap.get('lastTrade', {}).get('p') or 0)
        prev = float(prev_day.get('c') or 0)
        if price <= 0:
            return None, lat

        market = 'NYSE' if not ticker.endswith('.SA') else 'B3'

        # [v10.5-5] Tentar cache de candles primeiro
        cached = _get_cached_candles(f'polygon:{ticker}', cache_dict, cache_lock)
        if cached:
            result = dict(cached)
            result['price'] = price
            result['prev'] = prev
            result['change_pct'] = round((price / prev - 1) * 100, 2) if prev > 0 else 0
            result['updated_at'] = datetime.utcnow().isoformat()
            result['source'] = 'Polygon-snapshot'
            vol_today = float(day.get('v') or 0)
            if vol_today > 0 and cached.get('_avg_vol20', 0) > 0:
                result['volume_ratio'] = round(vol_today / cached['_avg_vol20'], 3)
            return result, lat

        # Cache frio: buscar candles históricos
        end_date = datetime.utcnow().strftime('%Y-%m-%d')
        start_date = (datetime.utcnow() - timedelta(days=90)).strftime('%Y-%m-%d')
        rc = requests.get(
            f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}',
            params={'apiKey': api_key, 'adjusted': 'true', 'sort': 'asc', 'limit': 90},
            timeout=8)
        closes = []
        highs = []
        lows = []
        volumes = []
        if rc.status_code == 200:
            bars = rc.json().get('results', [])
            closes = [b['c'] for b in bars if b.get('c')]
            highs = [b['h'] for b in bars if b.get('h')]
            lows = [b['l'] for b in bars if b.get('l')]
            volumes = [b['v'] for b in bars if b.get('v')]

        n = len(closes)
        ema9 = _ema(closes, 9) if n >= 9 else price
        ema21 = _ema(closes, 21) if n >= 21 else price
        ema50 = _ema(closes, 50) if n >= 50 else price
        rsi = _rsi(closes) if n >= 15 else 50.0
        atr = _calc_atr(closes, highs, lows, 14) if n >= 15 else 0.0
        atr_pct = round((atr / price) * 100, 3) if price > 0 and atr > 0 else 0.0
        vol_today = float(day.get('v') or volumes[-1] if volumes else 0)
        avg_vol20 = sum(volumes[-20:]) / len(volumes[-20:]) if len(volumes) >= 20 else 0
        vol_ratio = round(vol_today / avg_vol20, 3) if avg_vol20 > 0 else 0.0

        result = {
            'price': price, 'prev': prev,
            'change_pct': round((price / prev - 1) * 100, 2) if prev > 0 else 0,
            'ema9': round(ema9, 4), 'ema21': round(ema21, 4), 'ema50': round(ema50, 4),
            'rsi': round(rsi, 1), 'atr_pct': atr_pct, 'volume_ratio': vol_ratio,
            'ema9_real': n >= 9, 'ema21_real': n >= 21, 'ema50_real': n >= 50, 'rsi_real': n >= 15,
            'candles_available': n, 'market': market,
            '_avg_vol20': avg_vol20,   # guardado no cache para atualizar vol_ratio no snapshot
            'source': 'Polygon', 'updated_at': datetime.utcnow().isoformat()
        }
        _set_cached_candles(f'polygon:{ticker}', result, cache_dict, cache_lock)
        return result, lat
    except Exception as e:
        lat = (time.time() - t0) * 1000
        if log:
            log.debug(f'Polygon {ticker}: {e}')
        return None, lat


# ═══════════════════════════════════════════════════════════════
# BRAPI STOCK FETCHERS
# ═══════════════════════════════════════════════════════════════

def _fetch_brapi_stock(ticker: str, brapi_token: str, cache_dict: dict, cache_lock, log=None) -> tuple:
    """[v10.6-P0-1] Wrapper fino sobre _fetch_brapi_batch para retrocompatibilidade.
    Chamado por _fetch_single_stock() e _fetch_arbi_price() quando BRAPI_TOKEN existe.
    Retorna (result_dict | None, latency_ms).

    Args:
        ticker (str): Stock ticker (B3 format)
        brapi_token (str): brapi.dev API token
        cache_dict (dict): Candle cache dictionary
        cache_lock: Threading lock for cache
        log: Logger object (optional)

    Returns:
        tuple: (result_dict | None, latency_ms)
    """
    t0 = time.time()
    res = _fetch_brapi_batch([ticker], brapi_token, cache_dict, cache_lock, log)
    lat = (time.time() - t0) * 1000
    data = res.get(ticker)
    return (data, lat) if data else (None, lat)


def _fetch_brapi_batch(tickers: list, brapi_token: str, cache_dict: dict, cache_lock, log=None) -> dict:
    """[v10.6-P1] Busca até 20 ativos B3 em uma única chamada brapi.
    Retorna dict {ticker: result_dict}.

    Isso reduz chamadas de brapi de N req/loop para ceil(N/20) req/loop,
    e de ~2.5M/mês para ~130k/mês com candles cacheados por CANDLES_CACHE_MIN.

    Args:
        tickers (list): List of B3 tickers to fetch (without .SA)
        brapi_token (str): brapi.dev API token
        cache_dict (dict): Candle cache dictionary
        cache_lock: Threading lock for cache
        log: Logger object (optional)

    Returns:
        dict: {ticker: result_dict} with price and technical indicators
    """
    if not tickers or not brapi_token:
        return {}
    results = {}
    # Separar os que precisam de histórico dos que só precisam de snapshot
    cold = [t for t in tickers if _get_cached_candles(f'brapi:{t}', cache_dict, cache_lock) is None]
    warm = [t for t in tickers if t not in cold]

    headers = {'Authorization': f'Bearer {brapi_token}'}

    # ── Warm: batch snapshot, sem histórico ─────────────────────────────────
    for i in range(0, len(warm), 20):
        chunk = warm[i:i + 20]
        t0 = time.time()
        try:
            r = requests.get(
                f'https://brapi.dev/api/quote/{",".join(chunk)}',
                headers=headers, timeout=8)
            lat = (time.time() - t0) * 1000
            if r.status_code != 200:
                continue
            for q in r.json().get('results', []):
                sym = q.get('symbol', '').replace('.SA', '')
                price = float(q.get('regularMarketPrice') or 0)
                prev = float(q.get('regularMarketPreviousClose') or 0)
                if price <= 0:
                    continue
                cached = _get_cached_candles(f'brapi:{sym}', cache_dict, cache_lock)
                if cached:
                    entry = dict(cached)
                    entry['price'] = price
                    entry['prev'] = prev
                    entry['change_pct'] = round((price / prev - 1) * 100, 2) if prev > 0 else 0
                    entry['updated_at'] = datetime.utcnow().isoformat()
                    entry['source'] = 'brapi-batch-snapshot'
                    results[sym] = entry
        except Exception as e:
            if log:
                log.warning(f'brapi batch snapshot chunk {chunk}: {e}')

    # ── Cold: batch com histórico, chunks de 10 (range=3mo é mais pesado) ────
    for i in range(0, len(cold), 10):
        chunk = cold[i:i + 10]
        t0 = time.time()
        try:
            r = requests.get(
                f'https://brapi.dev/api/quote/{",".join(chunk)}',
                params={'range': '3mo', 'interval': '1d', 'fundamental': 'false'},
                headers=headers, timeout=12)
            lat = (time.time() - t0) * 1000
            if r.status_code != 200:
                continue
            for q in r.json().get('results', []):
                sym = q.get('symbol', '').replace('.SA', '')
                price = float(q.get('regularMarketPrice') or 0)
                prev = float(q.get('regularMarketPreviousClose') or 0)
                if price <= 0:
                    continue

                hist = q.get('historicalDataPrice', [])
                closes = [c['close'] for c in hist if c.get('close')]
                highs = [c['high'] for c in hist if c.get('high')]
                lows = [c['low'] for c in hist if c.get('low')]
                volumes = [c['volume'] for c in hist if c.get('volume')]
                n = len(closes)
                ema9 = _ema(closes, 9) if n >= 9 else price
                ema21 = _ema(closes, 21) if n >= 21 else price
                ema50 = _ema(closes, 50) if n >= 50 else price
                rsi = _rsi(closes) if n >= 15 else 50.0
                atr = _calc_atr(closes, highs, lows, 14) if n >= 15 else 0.0
                atr_pct = round((atr / price) * 100, 3) if price > 0 and atr > 0 else 0.0
                vol_today = float(q.get('regularMarketVolume') or 0)
                avg_vol20 = sum(volumes[-20:]) / len(volumes[-20:]) if len(volumes) >= 20 else 0
                vol_ratio = round(vol_today / avg_vol20, 3) if avg_vol20 > 0 else 0.0

                entry = {
                    'price': price, 'prev': prev,
                    'change_pct': round((price / prev - 1) * 100, 2) if prev > 0 else 0,
                    'ema9': round(ema9, 4), 'ema21': round(ema21, 4), 'ema50': round(ema50, 4),
                    'rsi': round(rsi, 1), 'atr_pct': atr_pct, 'volume_ratio': vol_ratio,
                    'ema9_real': n >= 9, 'ema21_real': n >= 21,
                    'ema50_real': n >= 50, 'rsi_real': n >= 15,
                    'candles_available': n, 'market': 'B3',
                    'source': 'brapi-batch-cold', 'updated_at': datetime.utcnow().isoformat()
                }
                _set_cached_candles(f'brapi:{sym}', entry, cache_dict, cache_lock)
                results[sym] = entry
        except Exception as e:
            if log:
                log.warning(f'brapi batch cold chunk {chunk}: {e}')

    return results


# ═══════════════════════════════════════════════════════════════
# SINGLE STOCK FETCHER — Multi-source Fallback Chain
# ═══════════════════════════════════════════════════════════════

def _fetch_single_stock(sym: str, config: dict, cache_dict: dict, cache_lock, log=None) -> tuple:
    """[v10.4] Camada de dados: Polygon (US) → brapi (B3) → FMP → Yahoo.
    Sempre retorna atr_pct e volume_ratio quando disponível.

    Args:
        sym (str): Stock symbol (e.g., 'AAPL', 'PETR4')
        config (dict): Configuration dict with:
            - 'polygon_api_key': Polygon.io key (optional)
            - 'brapi_token': brapi.dev token (optional)
            - 'fmp_api_key': FMP key (optional)
            - 'fx_rates': dict with FX rates (e.g., {'USDBRL': 5.8})
            - 'b3_to_adr': dict mapping B3 to ADR symbols
            - 'stock_symbols_b3': list of B3 symbols
        cache_dict (dict): Candle cache
        cache_lock: Threading lock
        log: Logger (optional)

    Returns:
        tuple: (result_dict | None, latency_ms)
    """
    polygon_key = config.get('polygon_api_key', '')
    brapi_token = config.get('brapi_token', '')
    fmp_key = config.get('fmp_api_key', '')
    fx_rates = config.get('fx_rates', {'USDBRL': 5.8})
    b3_to_adr = config.get('b3_to_adr', {})
    stock_symbols_b3 = config.get('stock_symbols_b3', [])

    is_b3 = sym.endswith('.SA') or any(sym == s.replace('.SA', '') for s in stock_symbols_b3)
    display = sym.replace('.SA', '')

    # 1. brapi para B3
    if is_b3 and brapi_token:
        result, lat = _fetch_brapi_stock(display, brapi_token, cache_dict, cache_lock, log)
        if result:
            return result, lat

    # 2. Polygon para US (e ADR de B3 quando brapi indisponível)
    if polygon_key:
        if not is_b3:
            result, lat = _fetch_polygon_stock(display, polygon_key, cache_dict, cache_lock, log)
            if result:
                return result, lat
        else:
            # [v10.5-1] ADR map real — não tentar ticker B3 diretamente no Polygon
            adr_sym = b3_to_adr.get(display)
            if adr_sym:
                result, lat = _fetch_polygon_stock(adr_sym, polygon_key, cache_dict, cache_lock, log)
                if result and result.get('price', 0) > 0:
                    # Converter preço USD → BRL usando fx_rates
                    usd_brl = fx_rates.get('USDBRL', 5.8)
                    price_brl = round(result['price'] * usd_brl, 2)
                    result['price'] = price_brl
                    result['prev'] = round(result.get('prev', 0) * usd_brl, 2)
                    result['ema9'] = round(result.get('ema9', 0) * usd_brl, 4)
                    result['ema21'] = round(result.get('ema21', 0) * usd_brl, 4)
                    result['ema50'] = round(result.get('ema50', 0) * usd_brl, 4)
                    result['market'] = 'B3'
                    result['source'] = f'Polygon-ADR({adr_sym})'
                    return result, lat
            # Sem ADR mapeado: não tentar Polygon com ticker B3 — vai retornar 404

    # 3. FMP fallback
    if fmp_key:
        try:
            t0 = time.time()
            r = requests.get(
                f'https://financialmodelingprep.com/api/v3/quote/{display}',
                params={'apikey': fmp_key}, timeout=8)
            lat = (time.time() - t0) * 1000
            if r.status_code == 200:
                d = r.json()
                if d and isinstance(d, list):
                    q = d[0]
                    price = float(q.get('price') or 0)
                    prev = float(q.get('previousClose') or 0)
                    if price > 0:
                        result = {
                            'price': price, 'prev': prev,
                            'change_pct': round(float(q.get('changesPercentage') or 0), 2),
                            'ema9': price, 'ema21': price, 'ema50': price,
                            'rsi': 50.0, 'atr_pct': 0.0, 'volume_ratio': 0.0,
                            'ema9_real': False, 'ema21_real': False, 'ema50_real': False, 'rsi_real': False,
                            'candles_available': 0, 'market': 'B3' if is_b3 else 'NYSE',
                            'source': 'FMP', 'updated_at': datetime.utcnow().isoformat()
                        }
                        return result, lat
        except Exception as e:
            if log:
                log.debug(f'FMP fallback {display}: {e}')

    # 4. Yahoo Finance último recurso
    t0 = time.time()
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=3mo',
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        lat = (time.time() - t0) * 1000
        if r.status_code != 200:
            return None, lat
        data = r.json()['chart']['result'][0]
        meta = data['meta']
        price = float(meta.get('regularMarketPrice') or 0)
        prev = float(meta.get('chartPreviousClose') or 0)
        if price <= 0:
            return None, lat
        closes = [c for c in data.get('indicators', {}).get('quote', [{}])[0].get('close', []) if c]
        n = len(closes)
        ema9 = _ema(closes, 9) if n >= 9 else price
        ema21 = _ema(closes, 21) if n >= 21 else price
        ema50 = _ema(closes, 50) if n >= 50 else price
        rsi = _rsi(closes) if n >= 15 else 50.0
        atr = _calc_atr(closes, [], [], 14)
        atr_pct = round((atr / price) * 100, 3) if price > 0 and atr > 0 else 0.0
        result = {
            'price': price, 'prev': prev,
            'change_pct': round((price / prev - 1) * 100, 2) if prev > 0 else 0,
            'ema9': round(ema9, 4), 'ema21': round(ema21, 4), 'ema50': round(ema50, 4),
            'rsi': round(rsi, 1), 'atr_pct': atr_pct, 'volume_ratio': 0.0,
            'ema9_real': n >= 9, 'ema21_real': n >= 21, 'ema50_real': n >= 50, 'rsi_real': n >= 15,
            'candles_available': n, 'market': 'B3' if sym.endswith('.SA') else 'NYSE',
            'source': 'Yahoo', 'updated_at': datetime.utcnow().isoformat()
        }
        return result, lat
    except Exception as e:
        lat = (time.time() - t0) * 1000
        if log:
            log.debug(f'Yahoo fallback {sym}: {e}')
        return None, lat
