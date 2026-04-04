"""[v10.4+] Pure crypto fetching and analysis functions — parameterized, no global state.

This module contains:
  - Binance ticker and klines fetchers
  - Crypto composite score calculation
  - Market regime updates
  - PnL and momentum analysis functions
  - FX rate fetching

All functions receive their data/config as parameters. This module does NOT
access global variables like crypto_prices or market_regime — those are passed in
or returned as results.

To use in api_server.py:
  from modules.crypto_fetcher import (_fetch_binance_ticker, _fetch_binance_klines,
                                       _crypto_composite_score, calc_period_pnl,
                                       is_momentum_positive, fetch_fx_rates)
"""

import time
import requests
from datetime import datetime, timedelta


# ═══════════════════════════════════════════════════════════════
# BINANCE FETCHERS
# ═══════════════════════════════════════════════════════════════

def _fetch_binance_ticker(symbol: str, log=None) -> dict:
    """[v10.4] Binance 24h ticker — preço, volume, change_pct, high, low.
    Endpoint público, sem API key. Latência típica < 80ms.

    Args:
        symbol (str): Binance symbol (e.g., 'BTCUSDT')
        log: Logger object (optional)

    Returns:
        dict: Ticker data with price, prev, change_pct, high_24h, low_24h, vol_24h, etc.
    """
    try:
        r = requests.get(
            f'https://api.binance.com/api/v3/ticker/24hr',
            params={'symbol': symbol}, timeout=6)
        if r.status_code != 200:
            return {}
        d = r.json()
        return {
            'price':      float(d.get('lastPrice') or 0),
            'prev':       float(d.get('prevClosePrice') or 0),
            'change_pct': float(d.get('priceChangePercent') or 0),
            'high_24h':   float(d.get('highPrice') or 0),
            'low_24h':    float(d.get('lowPrice') or 0),
            'vol_24h':    float(d.get('volume') or 0),       # volume em base coin
            'vol_quote':  float(d.get('quoteVolume') or 0),  # volume em USDT
            'n_trades':   int(d.get('count') or 0),
        }
    except Exception as e:
        if log:
            log.debug(f'Binance ticker {symbol}: {e}')
        return {}


def _fetch_binance_klines(symbol: str, period: int = 20, log=None) -> dict:
    """[v10.4][v10.5-2] Binance klines diárias para ATR e volume médio.
    Usa b[7] (quoteAssetVolume, em USDT) — compatível com vol_quote do allTickers.
    b[5] é volume em moeda base (BTC, ETH…) — não comparável com quoteVolume.

    Args:
        symbol (str): Binance symbol (e.g., 'BTCUSDT')
        period (int): Number of candles to fetch (default 20)
        log: Logger object (optional)

    Returns:
        dict: {'closes': [...], 'highs': [...], 'lows': [...], 'volumes': [...]}
    """
    try:
        r = requests.get(
            'https://api.binance.com/api/v3/klines',
            params={'symbol': symbol, 'interval': '1d', 'limit': period + 2},
            timeout=6)
        if r.status_code != 200:
            return {}
        bars = r.json()
        closes = [float(b[4]) for b in bars]   # close
        highs = [float(b[2]) for b in bars]    # high
        lows = [float(b[3]) for b in bars]     # low
        volumes = [float(b[7]) for b in bars]  # [v10.5-2] quoteAssetVolume (USDT)
        return {'closes': closes, 'highs': highs, 'lows': lows, 'volumes': volumes}
    except Exception as e:
        if log:
            log.debug(f'Binance klines {symbol}: {e}')
        return {}


# ═══════════════════════════════════════════════════════════════
# CRYPTO SCORE CALCULATION
# ═══════════════════════════════════════════════════════════════

def _crypto_composite_score(ticker: dict, klines: dict, direction: str) -> int:
    """[v10.4] Score composto multi-fator para crypto.
    Substitui 'score = 50 + int(abs(change_24h)*5)' que ignorava volume e ATR.

    Fatores (todos normalizados para 0-100, depois ponderados):
    - change_pct_24h   (65%): força do movimento (dominante para crypto)
    - volume_ratio     (10%): volume hoje vs média 20d — confirma movimento
    - atr_position     (15%): preço vs range do dia (high/low) — direcionalidade
    - momentum_quality (10%): número de trades normalizado — liquidez

    Args:
        ticker (dict): Ticker data with price, change_pct, high_24h, low_24h, vol_quote, n_trades
        klines (dict): Klines data with closes, highs, lows, volumes
        direction (str): 'LONG' or 'SHORT'

    Returns:
        int: Composite score (5-95)
    """
    change = ticker.get('change_pct', 0)
    high_24 = ticker.get('high_24h', 0)
    low_24 = ticker.get('low_24h', 0)
    price = ticker.get('price', 0)
    vol_24 = ticker.get('vol_quote', 0)
    n_tr = ticker.get('n_trades', 0)

    closes = klines.get('closes', [])
    highs_k = klines.get('highs', [])
    lows_k = klines.get('lows', [])
    vols_k = klines.get('volumes', [])

    # Fator 1: change_pct (capped em ±15%)
    change_capped = max(-15.0, min(15.0, change))
    change_factor = (change_capped + 15) / 30 * 100  # 0-100

    # Fator 2: volume ratio vs média 20d
    avg_vol20 = sum(vols_k[-20:]) / len(vols_k[-20:]) if len(vols_k) >= 20 else 0
    vol_ratio = vol_24 / avg_vol20 if avg_vol20 > 0 else 1.0
    # [v10.14-FIX] Escala corrigida: 0.5→25 | 1.0→50 | 1.5→65 | 2.0→80 | 3.0→100
    # Vol normal (1x) = 50 (neutro), não 25 (que penalizava desnecessariamente)
    vol_factor = min(100, max(0, (vol_ratio - 0.5) / 2.5 * 100))

    # Fator 3: posição no range do dia (0=low, 100=high)
    day_range = high_24 - low_24
    if day_range > 0 and price > 0:
        range_pos = ((price - low_24) / day_range) * 100
    else:
        range_pos = 50.0

    # Fator 4: liquidez (n_trades normalizado — >100k = max)
    liq_factor = min(100, (n_tr / 100_000) * 100) if n_tr > 0 else 50.0

    # [v10.14-FIX] Pesos: change=65% (dominante), vol=10%, range=15%, liq=10%
    # Crypto: o movimento de preço é o sinal mais confiável — volume é confirmação secundária
    raw = (0.65 * change_factor + 0.10 * vol_factor +
           0.15 * range_pos + 0.10 * liq_factor)
    composite = max(5, min(95, int(raw)))

    # Para SHORT: inverter (score baixo = sinal de venda forte)
    if direction == 'SHORT':
        composite = 100 - composite
    return composite


# ═══════════════════════════════════════════════════════════════
# MARKET REGIME UPDATES
# ═══════════════════════════════════════════════════════════════

def _update_market_regime(crypto_momentum: dict) -> dict:
    """[v10.4] Classifica regime de mercado baseado em momentum das cryptos.

    Args:
        crypto_momentum (dict): {symbol: change_pct_24h, ...}

    Returns:
        dict: {'mode': 'HIGH_VOL|TRENDING|RANGING', 'volatility': 'HIGH|NORMAL|LOW', 'avg_change_pct': float}
    """
    if not crypto_momentum:
        return {'mode': 'RANGING', 'volatility': 'LOW', 'avg_change_pct': 0.0,
                'updated_at': datetime.utcnow().isoformat()}

    vals = list(crypto_momentum.values())
    n = len(vals)
    trending = sum(1 for v in vals if abs(v) > 2.0)
    high_vol = sum(1 for v in vals if abs(v) > 8.0)  # [v10.24.1] era 5.0 — crypto volátil por natureza, 5% é normal
    mode = 'HIGH_VOL' if high_vol / n > 0.4 else ('TRENDING' if trending / n > 0.6 else 'RANGING')
    avg = sum(abs(v) for v in vals) / n
    vol = 'HIGH' if avg > 4 else ('LOW' if avg < 1 else 'NORMAL')
    return {
        'mode': mode,
        'volatility': vol,
        'avg_change_pct': round(avg, 2),
        'updated_at': datetime.utcnow().isoformat()
    }


# ═══════════════════════════════════════════════════════════════
# PNL AND MOMENTUM ANALYSIS
# ═══════════════════════════════════════════════════════════════

def calc_period_pnl(trades: list, days: int) -> float:
    """[v10.4] Calcula PnL total dos últimos N dias.

    Args:
        trades (list): List of trade dicts with 'pnl' and 'closed_at' fields
        days (int): Number of days to consider

    Returns:
        float: Total PnL for the period
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    return round(sum(t.get('pnl', 0) for t in trades if t.get('closed_at', '') >= cutoff), 2)


def is_momentum_positive(trade: dict) -> bool:
    """[v10.4] Verifica se o trade tem momentum positivo (3 últimos PnL% crescendo + não muito negativo).

    Args:
        trade (dict): Trade dict with 'pnl_history' and 'pnl_pct' fields

    Returns:
        bool: True if momentum is positive and trade isn't deeply underwater
    """
    h = trade.get('pnl_history', [])
    return len(h) >= 3 and h[-1] > h[-2] > h[-3] and trade.get('pnl_pct', 0) > -1.5


# ═══════════════════════════════════════════════════════════════
# FX RATES FETCHING
# ═══════════════════════════════════════════════════════════════

def fetch_fx_rates(fx_rates_dict: dict, log=None):
    """[v10.4] frankfurter.app primário (ECB data, free, sem key, sem limite) → Yahoo fallback.
    frankfurter.app é mantido pelo Frankfurter open-source project, dados do Banco Central Europeu.
    USDBRL, GBPUSD, HKDUSD. Atualizado a cada ciclo do arbi_scan_loop (~6min).

    Updates fx_rates_dict in-place with new FX rates.

    Args:
        fx_rates_dict (dict): Dictionary to update with FX rates (will be modified in-place)
        log: Logger object (optional)
    """
    try:
        # frankfurter.app: base USD, retorna quantas unidades de cada moeda = 1 USD
        r = requests.get(
            'https://api.frankfurter.app/latest',
            params={'from': 'USD', 'to': 'BRL,GBP,HKD,CAD,EUR'}, timeout=8)  # [v10.9] +CAD,EUR
        if r.status_code == 200:
            rates = r.json().get('rates', {})
            if rates.get('BRL', 0) > 0:
                fx_rates_dict['USDBRL'] = round(rates['BRL'], 4)
            if rates.get('GBP', 0) > 0:
                # frankfurter retorna USD→GBP (ex: 0.79); queremos GBPUSD (ex: 1.27)
                fx_rates_dict['GBPUSD'] = round(1.0 / rates['GBP'], 4)
            if rates.get('HKD', 0) > 0:
                fx_rates_dict['HKDUSD'] = round(rates['HKD'], 4)
            if rates.get('CAD', 0) > 0:
                # USD→CAD (ex: 1.36); queremos CADUSD = 1 CAD em USD (ex: 0.735)
                fx_rates_dict['CADUSD'] = round(1.0 / rates['CAD'], 4)
            if rates.get('EUR', 0) > 0:
                # USD→EUR (ex: 0.92); queremos EURUSD = 1 EUR em USD (ex: 1.085)
                fx_rates_dict['EURUSD'] = round(1.0 / rates['EUR'], 4)
            if log:
                log.info(f'FX (frankfurter.app/ECB): {fx_rates_dict}')
            return
    except Exception as e:
        if log:
            log.warning(f'frankfurter.app: {e}')

    # Yahoo fallback
    pairs = {
        'USDBRL': 'BRL=X',
        'GBPUSD': 'GBPUSD=X',
        'HKDUSD': 'HKD=X',
        'CADUSD': 'CAD=X',
        'EURUSD': 'EURUSD=X'
    }
    for key, sym in pairs.items():
        try:
            r = requests.get(
                f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=1d',
                headers={'User-Agent': 'Mozilla/5.0'}, timeout=6)
            if r.status_code == 200:
                price = r.json()['chart']['result'][0]['meta'].get('regularMarketPrice', 0)
                if price > 0:
                    fx_rates_dict[key] = price
        except Exception:
            pass
    if log:
        log.info(f'FX (Yahoo fallback): {fx_rates_dict}')
