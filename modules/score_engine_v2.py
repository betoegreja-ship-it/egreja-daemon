"""
score_engine_v2.py — Multi-indicator scoring engine (v2)

Substitui o score simplificado de api_server.py (5 fatores para stocks,
4 fatores não-técnicos para crypto) por um modelo de 12+ indicadores
técnicos convergentes + camada adaptativa de learning.

Inspirado na arquitetura de 9 indicadores documentada em:
Egreja_Analise_Tecnica_Decisoes_Trading.docx (Manus v6.2)

Mas vai além — adiciona:
- VWAP deviation (institucional)
- OBV trend (volume acumulado)
- Ichimoku cloud signal
- Supertrend
- Learning adjustment via factor_stats (já existe no daemon)

MODO DE OPERAÇÃO:
  Shadow: calcula v2 e grava em coluna score_v2, mas v1 continua decidindo.
  Live:   v2 decide. Controlado via env var USE_SCORE_V2.

AUTOR: Refatoração Claude Opus 4.7 + Beto Egreja — 17/abr/2026
"""
from __future__ import annotations
import math
from typing import List, Dict, Tuple, Optional


# ═══════════════════════════════════════════════════════════════════
# 1) INDICADORES TÉCNICOS — implementações puras em numpy/python
# ═══════════════════════════════════════════════════════════════════

def _sma(values: List[float], period: int) -> Optional[float]:
    """Simple Moving Average."""
    if len(values) < period: return None
    return sum(values[-period:]) / period


def _ema(values: List[float], period: int) -> Optional[float]:
    """Exponential Moving Average."""
    if len(values) < period: return None
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period  # seed SMA
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
    return ema


def _ema_series(values: List[float], period: int) -> List[Optional[float]]:
    """EMA completa (útil para MACD histogram)."""
    if len(values) < period: return [None] * len(values)
    out = [None] * (period - 1)
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    out.append(ema)
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
        out.append(ema)
    return out


def rsi(closes: List[float], period: int = 14) -> Optional[float]:
    """Relative Strength Index.
    Fórmula Wilder: RS = avg_gain / avg_loss; RSI = 100 - 100/(1+RS)
    """
    if len(closes) < period + 1: return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i-1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    # Wilder smoothing
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0: return 100.0
    rs = avg_g / avg_l
    return 100 - (100 / (1 + rs))


def macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Optional[Dict]:
    """MACD line, signal line, histogram.
    Retorna dict {line, signal, hist, hist_prev} para detectar cruzamentos.
    """
    if len(closes) < slow + signal: return None
    ema_fast = _ema_series(closes, fast)
    ema_slow = _ema_series(closes, slow)
    macd_line = []
    for f, s in zip(ema_fast, ema_slow):
        if f is None or s is None: macd_line.append(None)
        else: macd_line.append(f - s)
    valid = [v for v in macd_line if v is not None]
    if len(valid) < signal: return None
    signal_line = _ema_series(valid, signal)
    # Alinhar
    pad = len(macd_line) - len(signal_line)
    sig_full = [None] * pad + signal_line
    line_now = macd_line[-1]
    sig_now = sig_full[-1]
    if line_now is None or sig_now is None: return None
    hist = line_now - sig_now
    # histórico do histogram
    hist_prev = None
    if macd_line[-2] is not None and sig_full[-2] is not None:
        hist_prev = macd_line[-2] - sig_full[-2]
    return {'line': line_now, 'signal': sig_now, 'hist': hist, 'hist_prev': hist_prev}


def bollinger(closes: List[float], period: int = 20, stdev: float = 2.0) -> Optional[Dict]:
    """Bollinger Bands. Retorna upper, middle, lower, %B, bandwidth."""
    if len(closes) < period: return None
    window = closes[-period:]
    mid = sum(window) / period
    var = sum((x - mid) ** 2 for x in window) / period
    sd = math.sqrt(var)
    upper = mid + stdev * sd
    lower = mid - stdev * sd
    price = closes[-1]
    width = upper - lower
    pct_b = (price - lower) / width if width > 0 else 0.5
    bandwidth = width / mid if mid > 0 else 0  # volatilidade relativa
    return {'upper': upper, 'mid': mid, 'lower': lower,
            'pct_b': pct_b, 'bandwidth': bandwidth}


def adx(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[Dict]:
    """Average Directional Index. Retorna ADX, +DI, -DI."""
    n = len(closes)
    if n < period * 2: return None
    tr_list, plus_dm, minus_dm = [], [], []
    for i in range(1, n):
        high_diff = highs[i] - highs[i-1]
        low_diff = lows[i-1] - lows[i]
        pdm = high_diff if (high_diff > low_diff and high_diff > 0) else 0
        mdm = low_diff if (low_diff > high_diff and low_diff > 0) else 0
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i-1]),
                 abs(lows[i] - closes[i-1]))
        tr_list.append(tr); plus_dm.append(pdm); minus_dm.append(mdm)
    if len(tr_list) < period: return None
    # Wilder smoothing
    atr = sum(tr_list[:period])
    plus_smooth = sum(plus_dm[:period])
    minus_smooth = sum(minus_dm[:period])
    dx_list = []
    for i in range(period, len(tr_list)):
        atr = atr - atr/period + tr_list[i]
        plus_smooth = plus_smooth - plus_smooth/period + plus_dm[i]
        minus_smooth = minus_smooth - minus_smooth/period + minus_dm[i]
        plus_di = 100 * plus_smooth / atr if atr > 0 else 0
        minus_di = 100 * minus_smooth / atr if atr > 0 else 0
        den = plus_di + minus_di
        dx = 100 * abs(plus_di - minus_di) / den if den > 0 else 0
        dx_list.append(dx)
    if len(dx_list) < period: return None
    adx_val = sum(dx_list[-period:]) / period
    plus_di_final = 100 * plus_smooth / atr if atr > 0 else 0
    minus_di_final = 100 * minus_smooth / atr if atr > 0 else 0
    return {'adx': adx_val, 'plus_di': plus_di_final, 'minus_di': minus_di_final}


def cci(highs: List[float], lows: List[float], closes: List[float], period: int = 20) -> Optional[float]:
    """Commodity Channel Index. CCI = (TP - SMA_TP) / (0.015 * MeanDev)."""
    if len(closes) < period: return None
    tp = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(len(closes))]
    window = tp[-period:]
    sma_tp = sum(window) / period
    mean_dev = sum(abs(x - sma_tp) for x in window) / period
    if mean_dev == 0: return 0.0
    return (tp[-1] - sma_tp) / (0.015 * mean_dev)


def stochastic(highs: List[float], lows: List[float], closes: List[float],
               k_period: int = 14, d_period: int = 3) -> Optional[Dict]:
    """Oscilador Estocástico %K e %D."""
    if len(closes) < k_period + d_period: return None
    k_list = []
    for i in range(k_period - 1, len(closes)):
        window_h = max(highs[i - k_period + 1:i + 1])
        window_l = min(lows[i - k_period + 1:i + 1])
        rng = window_h - window_l
        k = 100 * (closes[i] - window_l) / rng if rng > 0 else 50
        k_list.append(k)
    if len(k_list) < d_period: return None
    d = sum(k_list[-d_period:]) / d_period
    return {'k': k_list[-1], 'd': d, 'k_prev': k_list[-2] if len(k_list) > 1 else None}


def williams_r(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
    """Williams %R. Vai de 0 (topo) a -100 (fundo)."""
    if len(closes) < period: return None
    window_h = max(highs[-period:])
    window_l = min(lows[-period:])
    rng = window_h - window_l
    if rng == 0: return -50.0
    return (window_h - closes[-1]) / rng * -100


def atr_percent(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
    """ATR como % do preço atual."""
    if len(closes) < period + 1: return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i-1]),
                 abs(lows[i] - closes[i-1]))
        trs.append(tr)
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return (atr / closes[-1]) * 100 if closes[-1] > 0 else None


def vwap(highs: List[float], lows: List[float], closes: List[float],
         volumes: List[float]) -> Optional[Dict]:
    """VWAP + desvio atual do preço em relação ao VWAP (em %)."""
    if not volumes or len(volumes) != len(closes): return None
    tp = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(len(closes))]
    num = sum(t * v for t, v in zip(tp, volumes))
    den = sum(volumes)
    if den == 0: return None
    vwap_val = num / den
    price = closes[-1]
    deviation_pct = (price - vwap_val) / vwap_val * 100 if vwap_val > 0 else 0
    return {'vwap': vwap_val, 'deviation_pct': deviation_pct}


def obv(closes: List[float], volumes: List[float]) -> Optional[Dict]:
    """On-Balance Volume + tendência (subindo/descendo)."""
    if len(closes) < 3 or len(volumes) != len(closes): return None
    obv_val = 0
    obv_series = [0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i-1]: obv_val += volumes[i]
        elif closes[i] < closes[i-1]: obv_val -= volumes[i]
        obv_series.append(obv_val)
    # Tendência do OBV nos últimos 5 períodos
    recent = obv_series[-5:] if len(obv_series) >= 5 else obv_series
    if len(recent) < 2: return {'obv': obv_val, 'trend': 'FLAT'}
    slope = (recent[-1] - recent[0]) / max(abs(recent[0]), 1)
    trend = 'UP' if slope > 0.02 else ('DOWN' if slope < -0.02 else 'FLAT')
    return {'obv': obv_val, 'trend': trend, 'slope': slope}


def supertrend(highs: List[float], lows: List[float], closes: List[float],
               period: int = 10, multiplier: float = 3.0) -> Optional[Dict]:
    """Supertrend — indicador de trend-following baseado em ATR."""
    atr_pct = atr_percent(highs, lows, closes, period)
    if atr_pct is None: return None
    # ATR absoluto
    atr_abs = atr_pct / 100 * closes[-1]
    hl2 = (highs[-1] + lows[-1]) / 2
    upper = hl2 + multiplier * atr_abs
    lower = hl2 - multiplier * atr_abs
    price = closes[-1]
    # Direção simples: preço acima = uptrend
    direction = 'UP' if price > lower else 'DOWN'
    return {'upper': upper, 'lower': lower, 'direction': direction,
            'distance_pct': (price - (lower if direction == 'UP' else upper)) / price * 100}


def ema_cross(closes: List[float]) -> Optional[Dict]:
    """Retorna posição das EMAs 9/21/50 (alinhamento de tendência)."""
    ema9 = _ema(closes, 9); ema21 = _ema(closes, 21); ema50 = _ema(closes, 50)
    if ema9 is None or ema21 is None: return None
    price = closes[-1]
    # Alinhamento forte: price > ema9 > ema21 > ema50
    if ema50 is not None:
        if price > ema9 > ema21 > ema50: alignment = 'STRONG_BULL'
        elif price < ema9 < ema21 < ema50: alignment = 'STRONG_BEAR'
        elif ema9 > ema21: alignment = 'BULL'
        elif ema9 < ema21: alignment = 'BEAR'
        else: alignment = 'NEUTRAL'
    else:
        alignment = 'BULL' if ema9 > ema21 else 'BEAR'
    # Distância price vs ema9 (em %)
    dist_pct = (price - ema9) / ema9 * 100 if ema9 > 0 else 0
    return {'ema9': ema9, 'ema21': ema21, 'ema50': ema50,
            'alignment': alignment, 'distance_pct': dist_pct}


def volume_strength(volumes: List[float], lookback: int = 20) -> Optional[Dict]:
    """Volume ratio vs média de 20 períodos + tendência."""
    if len(volumes) < lookback + 1: return None
    avg = sum(volumes[-lookback-1:-1]) / lookback  # exclui o atual
    current = volumes[-1]
    ratio = current / avg if avg > 0 else 1.0
    # Tendência nos últimos 5
    if len(volumes) >= 5:
        recent_avg = sum(volumes[-5:]) / 5
        trend = 'UP' if recent_avg > avg * 1.1 else ('DOWN' if recent_avg < avg * 0.9 else 'FLAT')
    else:
        trend = 'FLAT'
    return {'ratio': ratio, 'trend': trend, 'current': current, 'avg20': avg}


def ichimoku_signal(highs: List[float], lows: List[float], closes: List[float]) -> Optional[Dict]:
    """Ichimoku Kinko Hyo — conversion line, base line, cloud thickness."""
    if len(closes) < 52: return None
    # Tenkan-sen (9-period) = (9-period high + 9-period low) / 2
    tenkan = (max(highs[-9:]) + min(lows[-9:])) / 2
    # Kijun-sen (26-period)
    kijun = (max(highs[-26:]) + min(lows[-26:])) / 2
    # Senkou A = (Tenkan + Kijun) / 2
    senkou_a = (tenkan + kijun) / 2
    # Senkou B (52-period high/low médio)
    senkou_b = (max(highs[-52:]) + min(lows[-52:])) / 2
    price = closes[-1]
    # Sinal: preço acima das nuvens = forte bull
    cloud_top = max(senkou_a, senkou_b)
    cloud_bottom = min(senkou_a, senkou_b)
    if price > cloud_top: signal = 'BULL'
    elif price < cloud_bottom: signal = 'BEAR'
    else: signal = 'NEUTRAL'
    return {'tenkan': tenkan, 'kijun': kijun, 'cloud_top': cloud_top,
            'cloud_bottom': cloud_bottom, 'signal': signal,
            'tenkan_above_kijun': tenkan > kijun}


# ═══════════════════════════════════════════════════════════════════
# 2) SCORING COMPOSTO — v2
# ═══════════════════════════════════════════════════════════════════
#
# Filosofia: cada indicador vota independentemente COMPRA/VENDA/NEUTRO
# Score final = convergência ponderada dos votos
# Pesos baseados em Manus v6.2 + extensões
#
# IMPORTANTE: o score resultante é SIMÉTRICO em torno de 50:
#   - Score 100 = convergência total COMPRA
#   - Score 50  = neutro (indicadores discordam)
#   - Score 0   = convergência total VENDA
#
# Isso mantém compatibilidade com api_server.py:5921:
#   signal = 'COMPRA' if score >= MIN_SCORE_AUTO else 'VENDA' if score <= (100-MIN_SCORE_AUTO)

# Pesos (total = 100, ajuste final)
WEIGHTS = {
    'rsi':         12,   # oversold/overbought
    'macd':        14,   # momentum/trend (mais alto que Manus: valida bem)
    'bollinger':   10,   # volatilidade estatística
    'adx':         10,   # força da tendência
    'cci':          7,
    'ema_cross':   12,   # tendência de curto/médio prazo (sólida)
    'stoch':        7,
    'williams':     6,
    'atr':          5,   # filtro de volatilidade
    'vwap':         7,   # institucional
    'obv':          4,   # volume acumulado
    'supertrend':   4,   # confirmação tendência
    'ichimoku':     2,   # fraco mas complementar
}
# Soma = 100 exato


def _vote_rsi(val: Optional[float]) -> float:
    """Vota +1 (compra), -1 (venda), ou fracionário."""
    if val is None: return 0.0
    if val < 25: return 1.0           # extremo oversold
    if val < 35: return 0.7
    if val < 45: return 0.3
    if val > 75: return -1.0          # extremo overbought
    if val > 65: return -0.7
    if val > 55: return -0.3
    return 0.0


def _vote_macd(m: Optional[Dict]) -> float:
    """MACD: vota baseado em histograma e cruzamento."""
    if m is None: return 0.0
    hist = m['hist']; hist_prev = m.get('hist_prev')
    line = m['line']
    # Cruzamento recente (hist mudou de sinal)
    crossing = False
    if hist_prev is not None:
        crossing = (hist > 0 and hist_prev <= 0) or (hist < 0 and hist_prev >= 0)
    base = 1.0 if hist > 0 else (-1.0 if hist < 0 else 0.0)
    # Amplifica se hist está crescendo (momentum)
    if hist_prev is not None:
        if hist > 0 and hist > hist_prev: base = min(1.0, base * 1.2)
        elif hist < 0 and hist < hist_prev: base = max(-1.0, base * 1.2)
    return base


def _vote_bollinger(b: Optional[Dict]) -> float:
    """%B < 0 = oversold (compra), > 1 = overbought (venda)."""
    if b is None: return 0.0
    pct_b = b['pct_b']
    if pct_b < 0.0: return 1.0
    if pct_b < 0.2: return 0.6
    if pct_b < 0.4: return 0.2
    if pct_b > 1.0: return -1.0
    if pct_b > 0.8: return -0.6
    if pct_b > 0.6: return -0.2
    return 0.0


def _vote_adx(a: Optional[Dict]) -> float:
    """ADX: direção vem dos DIs, força do ADX.
    ADX<20: mercado lateral → voto fraco
    ADX>25: tendência forte → voto amplificado
    """
    if a is None: return 0.0
    adx_v = a['adx']; pd = a['plus_di']; md = a['minus_di']
    # Strength multiplier
    strength = 0.3 if adx_v < 20 else (0.6 if adx_v < 25 else (1.0 if adx_v < 40 else 0.85))
    # Direction: +DI > -DI = bull
    if pd > md * 1.3: return strength
    if md > pd * 1.3: return -strength
    return 0.0  # muito próximos, sem direção clara


def _vote_cci(val: Optional[float]) -> float:
    if val is None: return 0.0
    if val < -200: return 1.0
    if val < -100: return 0.7
    if val < -50:  return 0.3
    if val > 200:  return -1.0
    if val > 100:  return -0.7
    if val > 50:   return -0.3
    return 0.0


def _vote_stoch(s: Optional[Dict]) -> float:
    """Stochastic: <20 oversold, >80 overbought. Cruzamento %K/%D reforça."""
    if s is None: return 0.0
    k, d = s['k'], s['d']
    base = 0.0
    if k < 20 and d < 20: base = 1.0
    elif k < 30: base = 0.5
    elif k > 80 and d > 80: base = -1.0
    elif k > 70: base = -0.5
    # Cruzamento: k subindo sobre d = reforça compra
    k_prev = s.get('k_prev')
    if k_prev is not None and base > 0 and k > d and k_prev <= d: base = min(1.0, base * 1.3)
    elif k_prev is not None and base < 0 and k < d and k_prev >= d: base = max(-1.0, base * 1.3)
    return base


def _vote_williams(val: Optional[float]) -> float:
    """Williams %R: vai de 0 a -100. -80 oversold, -20 overbought."""
    if val is None: return 0.0
    if val < -85: return 1.0
    if val < -70: return 0.5
    if val > -15: return -1.0
    if val > -30: return -0.5
    return 0.0


def _vote_atr(val: Optional[float]) -> float:
    """ATR como filtro: volatilidade normal bom, extrema ruim.
    ATR é filtro de RISCO, não direção. Vota 0 (neutro) ou -1 (muito volátil).
    """
    if val is None: return 0.0
    if val > 6.0: return -0.7   # volatilidade extrema, reduzir confiança
    if val > 4.0: return -0.3
    if 0.5 < val < 2.5: return 0.2  # faixa saudável
    return 0.0


def _vote_vwap(v: Optional[Dict]) -> float:
    """VWAP: preço acima = institucional comprando."""
    if v is None: return 0.0
    dev = v['deviation_pct']
    if dev > 3.0: return 0.5  # acima do VWAP — bullish, mas não extremo
    if dev > 1.0: return 0.3
    if dev > 0:   return 0.1
    if dev < -3.0: return -0.5
    if dev < -1.0: return -0.3
    if dev < 0:    return -0.1
    return 0.0


def _vote_obv(o: Optional[Dict]) -> float:
    """OBV trend: acumulação vs distribuição."""
    if o is None: return 0.0
    trend = o['trend']
    if trend == 'UP': return 0.7
    if trend == 'DOWN': return -0.7
    return 0.0


def _vote_supertrend(s: Optional[Dict]) -> float:
    if s is None: return 0.0
    return 0.8 if s['direction'] == 'UP' else -0.8


def _vote_ema(e: Optional[Dict]) -> float:
    """EMA alignment: vota baseado em stack."""
    if e is None: return 0.0
    a = e['alignment']
    if a == 'STRONG_BULL': return 1.0
    if a == 'BULL':        return 0.6
    if a == 'STRONG_BEAR': return -1.0
    if a == 'BEAR':        return -0.6
    return 0.0


def _vote_ichimoku(ich: Optional[Dict]) -> float:
    if ich is None: return 0.0
    s = ich['signal']
    base = 1.0 if s == 'BULL' else (-1.0 if s == 'BEAR' else 0.0)
    # Reforça se tenkan > kijun (ou vice-versa)
    if base > 0 and ich['tenkan_above_kijun']: base = 1.0
    elif base < 0 and not ich['tenkan_above_kijun']: base = -1.0
    return base



# ═══════════════════════════════════════════════════════════════════
# [FIX 29/abr/2026 HOTFIX] Compatibilidade NAMES V3 ↔ factor_stats
# Bug: V3 usa STRONG_BULL, factor_stats armazena BULLISH_STACK → 0 matches
# Estas funcoes traduzem antes de consultar factor_stats_cache.
# ═══════════════════════════════════════════════════════════════════

def _translate_ema_alignment_for_fs(v3_alignment):
    """V3 (STRONG_BULL/BULL/NEUTRAL/BEAR/STRONG_BEAR) -> factor_stats names."""
    return {
        'STRONG_BULL': 'BULLISH_STACK',
        'BULL':        'BULLISH_CROSS',
        'NEUTRAL':     'MIXED',
        'BEAR':        'BEARISH_CROSS',
        'STRONG_BEAR': 'BEARISH_STACK',
    }.get(v3_alignment, v3_alignment)


def _translate_rsi_bucket_for_fs(v3_bucket):
    """V3 (LOW/HIGH) -> factor_stats (WEAK/STRONG)."""
    return {
        'LOW':  'WEAK',
        'HIGH': 'STRONG',
    }.get(v3_bucket, v3_bucket)


# ═══════════════════════════════════════════════════════════════════
# [PATTERN_CALIBRATION 29/abr/2026] Combos LETAIS e GOLDEN identificados
# empiricamente em 3548 trades stock CLOSED (n>=30, |avg_pnl|>$50).
# ═══════════════════════════════════════════════════════════════════
LETHAL_COMBOS_LONG = {
    ('RANGING',  'BULLISH_CROSS', 'VERY_LOW'),
    ('TRENDING', 'BULLISH_CROSS', 'VERY_LOW'),
    ('RANGING',  'BULLISH_CROSS', 'LOW'),
    ('TRENDING', 'BULLISH_STACK', 'VERY_LOW'),
}
GOLDEN_COMBOS_LONG = {
    ('TRENDING', 'BULLISH_CROSS', 'NORMAL'): 15,
    ('RANGING',  'BULLISH_STACK', 'NORMAL'): 12,
    ('RANGING',  'BULLISH_CROSS', 'NORMAL'): 10,
    ('TRENDING', 'BULLISH_CROSS', 'LOW'):    8,
    ('TRENDING', 'BULLISH_STACK', 'NORMAL'): 8,
    ('RANGING',  'BULLISH_STACK', 'HIGH'):   8,
}
BAD_HOURS_B3_LONG_UTC = {14, 17}

# ═══════════════════════════════════════════════════════════════════
# [CRYPTO_CALIBRATION 29/abr/2026] Combos identificados em 3003 trades CLOSED
# ═══════════════════════════════════════════════════════════════════

# Horas TOXICAS UTC em crypto (avg <-150/trade, n>=85):
#  3h UTC (00 BRT): -$153/trade (final do dia US)
# 13h UTC (10 BRT): -$158/trade (sobreposicao US/EU abrindo)
# 15h UTC (12 BRT): -$192/trade (algos meio-dia US)
# 19h UTC (16 BRT): -$188/trade (close US chegando)
# 20h UTC (17 BRT): -$173/trade
BAD_HOURS_CRYPTO_UTC = {3, 13, 15, 19, 20}

# Horas DOURADAS UTC (avg >+50/trade, WR >55%):
#  5h UTC ( 2 BRT): +$122/trade WR66.5% (Asia ativa, EU acordando)
#  6h UTC ( 3 BRT): +$58 WR58%
#  7h UTC ( 4 BRT): +$74 WR58.3%
#  8h UTC ( 5 BRT): +$21 WR63.9%
GOLDEN_HOURS_CRYPTO_UTC = {5, 6, 7, 8}

# Combos LETAIS (forçar score reduzido / bloquear):
LETHAL_COMBOS_CRYPTO_LONG = {
    # (regime, ema_alignment, volume_bucket): bloquear LONG
    ('TRENDING', 'BULLISH_STACK', 'SURGE'),       # chase de pump em trend
    ('TRENDING', 'BULLISH_CROSS', 'SURGE'),       # FOMO entry
}

LETHAL_COMBOS_CRYPTO_SHORT = {
    # SHORT em RANGING crypto perde -$70/trade
    ('RANGING',  'BEARISH_STACK', 'NORMAL'),
    ('RANGING',  'BEARISH_CROSS', 'NORMAL'),
    ('RANGING',  'MIXED',         'NORMAL'),
    ('RANGING',  'MIXED',         'LOW'),
}

# Combos GOLDEN (bonus +5 a +12):
GOLDEN_COMBOS_CRYPTO_LONG = {
    # (regime, ema, volume): bonus
    ('RANGING',  'MIXED',         'LOW'):      8,   # +$55 avg
    ('RANGING',  'BULLISH_STACK', 'LOW'):      10,  # entrada calma em range
    ('RANGING',  'BULLISH_CROSS', 'NORMAL'):   8,
    # Volatility LOW LONG funciona em crypto (calmaria) +$51
    # mas nao em ATR LOW (mercado morto) -$79 — distincao important
}

GOLDEN_COMBOS_CRYPTO_SHORT = {
    # SHORT crypto so funciona em momentum negativo claro
    ('TRENDING', 'BEARISH_STACK', 'HIGH'):    8,
    ('TRENDING', 'BEARISH_STACK', 'SURGE'):   10,  # capitulation pump-fade
}


# ═══════════════════════════════════════════════════════════════════
# 3) COMPUTE — função principal que o daemon chama
# ═══════════════════════════════════════════════════════════════════

def compute_score_v2(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    volumes: Optional[List[float]] = None,
    *,
    factor_stats_cache: Optional[Dict] = None,
    pattern_stats_cache: Optional[Dict] = None,
    temporal_adj: float = 0.0,  # ajuste temporal passado pelo daemon
) -> Dict:
    """Calcula o score v2 integrando 13 indicadores técnicos + learning.

    Args:
        closes, highs, lows: séries de preço (mesma ordem, mais recente no fim)
        volumes: opcional (VWAP/OBV ficam off se None)
        factor_stats_cache: dict {(factor_name, bucket): {...}} do learning engine
        pattern_stats_cache: dict para bloqueio por padrão ruim
        temporal_adj: ajuste pré-calculado de hora/dia (fornecido pelo daemon)

    Returns:
        {
          'score': int 0-100,
          'signal': 'COMPRA'|'VENDA'|'MANTER',
          'strength': float 0-1,
          'votes': dict com cada indicador,
          'blocked': bool,
          'block_reason': str,
          'diagnostic': dict para debug/learning
        }
    """
    n = len(closes)
    if n < 20:
        return {'score': 50, 'signal': 'MANTER', 'strength': 0.0,
                'votes': {}, 'blocked': True, 'block_reason': 'INSUFFICIENT_DATA',
                'diagnostic': {'n_bars': n, 'need': 20}}

    # ── Calcular todos os indicadores ──
    rsi_val = rsi(closes, 14)
    macd_val = macd(closes)
    boll_val = bollinger(closes)
    adx_val = adx(highs, lows, closes)
    cci_val = cci(highs, lows, closes)
    stoch_val = stochastic(highs, lows, closes)
    williams_val = williams_r(highs, lows, closes)
    atr_val = atr_percent(highs, lows, closes)
    ema_val = ema_cross(closes)
    super_val = supertrend(highs, lows, closes)
    ich_val = ichimoku_signal(highs, lows, closes) if n >= 52 else None

    vwap_val = None; obv_val = None; vol_val = None
    if volumes and len(volumes) == len(closes):
        vwap_val = vwap(highs, lows, closes, volumes)
        obv_val = obv(closes, volumes)
        vol_val = volume_strength(volumes)

    # ── Votos individuais ──
    votes = {
        'rsi':        _vote_rsi(rsi_val),
        'macd':       _vote_macd(macd_val),
        'bollinger':  _vote_bollinger(boll_val),
        'adx':        _vote_adx(adx_val),
        'cci':        _vote_cci(cci_val),
        'stoch':      _vote_stoch(stoch_val),
        'williams':   _vote_williams(williams_val),
        'atr':        _vote_atr(atr_val),
        'ema_cross':  _vote_ema(ema_val),
        'vwap':       _vote_vwap(vwap_val),
        'obv':        _vote_obv(obv_val),
        'supertrend': _vote_supertrend(super_val),
        'ichimoku':   _vote_ichimoku(ich_val),
    }

    # ── Score ponderado ──
    total_weight = 0
    weighted_sum = 0.0
    for key, vote in votes.items():
        w = WEIGHTS.get(key, 0)
        # Se indicador retornou None (dados insuficientes), não pondera
        if key == 'ichimoku' and ich_val is None: continue
        if key == 'vwap' and vwap_val is None: continue
        if key == 'obv' and obv_val is None: continue
        weighted_sum += vote * w
        total_weight += w
    # Normalizar considerando só os indicadores que votaram
    if total_weight == 0:
        raw_score = 50.0
    else:
        # weighted_sum varia de -total_weight a +total_weight
        # Normalizar para 0-100 com 50 = neutro
        normalized = weighted_sum / total_weight  # [-1, +1]
        raw_score = 50 + normalized * 50


    # ── Camada adaptativa: learning via factor_stats_cache ──
    # Aproveita o learning engine que o daemon já mantém
    learning_adj = 0.0
    if factor_stats_cache:
        # RSI bucket
        rsi_b = 'OVERSOLD' if rsi_val and rsi_val < 30 else (
                'OVERBOUGHT' if rsi_val and rsi_val > 70 else (
                'LOW' if rsi_val and rsi_val < 45 else (
                'HIGH' if rsi_val and rsi_val > 55 else 'NEUTRAL')))
        fs = factor_stats_cache.get(('rsi_bucket', rsi_b), {})
        if fs.get('total_samples', 0) >= 10:
            learning_adj += float(fs.get('confidence_weight', 0)) * 8

        # EMA alignment (usa string exata)
        if ema_val:
            fs = factor_stats_cache.get(('ema_alignment', ema_val['alignment']), {})
            if fs.get('total_samples', 0) >= 10:
                learning_adj += float(fs.get('confidence_weight', 0)) * 8

        # Volatility
        vol_b = 'LOW' if atr_val and atr_val < 1 else (
                'HIGH' if atr_val and atr_val > 3 else 'NORMAL')
        fs = factor_stats_cache.get(('volatility_bucket', vol_b), {})
        if fs.get('total_samples', 0) >= 10:
            learning_adj += float(fs.get('confidence_weight', 0)) * 5

        # Direction
        direction = 'LONG' if raw_score > 50 else 'SHORT'
        fs = factor_stats_cache.get(('direction', direction), {})
        if fs.get('total_samples', 0) >= 5:
            learning_adj += float(fs.get('confidence_weight', 0)) * 4

    raw_score += learning_adj

    # ── Ajuste temporal (fornecido pelo daemon) ──
    raw_score += temporal_adj

    # ── Clamp ──
    final_score = int(max(0, min(100, raw_score)))

    # ── Sinal baseado em thresholds ──
    # Tradicional: signal = 'COMPRA' se score >= min_score
    # v2 mais conservador: precisa convergência mínima
    convergence = abs(weighted_sum / total_weight) if total_weight > 0 else 0
    strength = convergence  # [0, 1]


    # ── Bloqueio por pattern stats ruim ──
    blocked = False
    block_reason = ''
    if pattern_stats_cache:
        for key, ps in list(pattern_stats_cache.items())[:200]:
            n_samples = ps.get('total_samples', 0)
            wins = ps.get('wins', 0)
            if n_samples >= 30 and wins / n_samples < 0.40:
                ewma_hit = ps.get('ewma_hit_rate', 1.0)
                if ewma_hit < 0.45:
                    blocked = True
                    block_reason = f'PATTERN_BLOCK_{key}'
                    break

    # ── Signal ──
    if final_score >= 70 and strength >= 0.25:
        signal = 'COMPRA'
    elif final_score <= 30 and strength >= 0.25:
        signal = 'VENDA'
    else:
        signal = 'MANTER'

    return {
        'score': final_score,
        'signal': signal,
        'strength': round(strength, 3),
        'votes': {k: round(v, 3) for k, v in votes.items()},
        'blocked': blocked,
        'block_reason': block_reason,
        'diagnostic': {
            'rsi': rsi_val,
            'macd_hist': macd_val['hist'] if macd_val else None,
            'bb_pct_b': boll_val['pct_b'] if boll_val else None,
            'adx': adx_val['adx'] if adx_val else None,
            'cci': cci_val,
            'stoch_k': stoch_val['k'] if stoch_val else None,
            'williams': williams_val,
            'atr_pct': atr_val,
            'ema_alignment': ema_val['alignment'] if ema_val else None,
            'vwap_dev': vwap_val['deviation_pct'] if vwap_val else None,
            'obv_trend': obv_val['trend'] if obv_val else None,
            'supertrend_dir': super_val['direction'] if super_val else None,
            'ichimoku_sig': ich_val['signal'] if ich_val else None,
            'n_bars': n,
            'total_weight_used': total_weight,
            'weighted_sum': round(weighted_sum, 3),
            'learning_adj': round(learning_adj, 2),
            'temporal_adj': round(temporal_adj, 2),
            'raw_score_pre_clamp': round(raw_score, 2),
            'convergence': round(convergence, 3),
        }
    }



# ═══════════════════════════════════════════════════════════════════
# 4) REGIME-AWARE SCORING — v3
# ═══════════════════════════════════════════════════════════════════
#
# DESCOBERTA dos testes da v2:
# Em uptrend forte, osciladores (RSI, Stoch, CCI, Williams) votam
# "overbought → VENDER", cancelando os trend-followers (MACD, EMA, ADX).
# Isso dá score 57 em uptrend claro.
#
# Solução: **regime-aware weighting**.
# - ADX > 25 → TRENDING: osciladores perdem peso, trend-followers ganham
# - ADX < 20 → RANGING: osciladores ganham peso (mean-reversion funciona)
# - 20-25   → MIXED: pesos intermediários
#
# Também: osciladores em regime TRENDING deixam de votar "reversão"
# e passam a votar "timing" (extremos só aceleram o sinal, não invertem).


def _detect_regime(adx_val: Optional[Dict], atr_pct_val: Optional[float],
                   trend_dir: int = 0, volume_bucket: Optional[str] = None) -> str:
    """Detecta regime de mercado: TRENDING / MIXED / RANGING / CHOPPY.

    [FIX 29/abr/2026] Bug histórico: ADX>=25 era classificado TRENDING sem
    confirmar direção nem volume. Em movimentos artificiais (abertura B3,
    spike de leilão), ADX dispara mas sem trend real → V3 dava score 95
    e perdia tudo. Agora exige trend_dir confirmado E volume não-baixo.
    """
    if adx_val is None: return 'UNKNOWN'
    adx_v = adx_val['adx']
    atr_p = atr_pct_val or 0
    # ATR muito alto = mercado caótico
    if atr_p > 5.0: return 'CHOPPY'
    # Volume muito baixo = sem convicção, não pode ser TRENDING
    if volume_bucket in ('LOW', 'VERY_LOW'):
        if adx_v >= 25: return 'MIXED'  # rebaixar TRENDING -> MIXED
        return 'RANGING'
    # ADX alto exige trend_dir confirmado
    if adx_v >= 25 and trend_dir != 0: return 'TRENDING'
    if adx_v >= 25: return 'MIXED'  # ADX forte mas sem direção clara
    if adx_v >= 20: return 'MIXED'
    return 'RANGING'


# ═══════════════════════════════════════════════════════════════════
# Pesos regime-aware SEPARADOS POR ASSET TYPE
# [SEPARACAO 29/abr/2026] Stocks e crypto sao mercados COMPLETAMENTE
# diferentes — 24/7 vs horario fixo, volatilidade base 5x maior, etc.
# Aplicar pesos calibrados com 3548 trades stock em crypto era ERRO.
# ═══════════════════════════════════════════════════════════════════

# ─── PESOS STOCK (calibrados com 3548 trades stock CLOSED) ──────────
# Em TRENDING + LONG stock, dados mostram:
#   - Volume é #1 preditivo (NORMAL +$342/trade)
#   - EMA BEARISH_CROSS LONG = +$801 (reversao em trend)
#   - Osciladores perdem peso em trend
WEIGHTS_TRENDING_STOCK = {
    'ema_cross':   22, 'macd': 14, 'adx': 10, 'supertrend': 8,
    'ichimoku':    4,  'obv':  10, 'vwap': 15,
    'rsi':         3,  'stoch': 2, 'williams': 1, 'cci': 1, 'bollinger': 4,
    'atr':         6,
}

WEIGHTS_RANGING_STOCK = {
    # Em range + LONG stock, ATR EXTREME +$128, Volume NORMAL +$94
    'rsi':         12, 'stoch': 10, 'williams': 6, 'cci': 8, 'bollinger': 12,
    'ema_cross':   8,  'macd': 6,  'adx': 4, 'supertrend': 3,
    'ichimoku':    2,  'obv':  6,  'vwap': 12,
    'atr':         9,
}

# ─── PESOS CRYPTO (calibrados 29/abr — analise de 3003 trades CLOSED) ────
# DESCOBERTAS empiricas do historico crypto:
#   1. TRENDING+LONG = catastrofe (-$68/trade) — crypto reverte tendencias
#   2. RANGING+SHORT = catastrofe (-$70/trade) — crypto sai de range pra cima
#   3. ATR LOW + LONG = ruim (-$79) — mercado morto sangra
#   4. Volume SURGE + LONG = ruim (-$57) — chase de pump
#   5. Volume LOW + LONG em RANGING = bom (+$55) — entrada calma
#   6. Madrugada UTC (5-8h = 2-5 BRT) = OURO — WR 62%, +$73/trade
# Crypto e CONTRARIAN — fade extremos, mean-reversion domina.

WEIGHTS_TRENDING_CRYPTO = {
    # [v2 — 29/abr/2026 + relatorio Sofia 12480 sinais]
    # Sofia confirmou: Bollinger WR 66.7%, CCI 65.1%, Stoch 63.9%, RSI 62%
    # MACD WR 51%, EMA WR 51%, ADX 100% NEUTRO em crypto.
    # Nossos dados confirmam: ADX NULL em 100% das 3003 trades, ADX_value missing.
    'bollinger':  12,                  # ↑ Sofia: WR 66.7% — melhor preditor crypto
    'cci':        10,                  # ↑ Sofia: WR 65.1%
    'stoch':      10,                  # ↑ Sofia: WR 63.9%
    'williams':    8,                  # ↑ Sofia: WR 63.9%
    'rsi':        12,                  # Sofia: WR 62.0% — mantem
    'ema_cross':  10,                  # ↓ Sofia: WR 51.1% — reduzido
    'macd':        8,                  # ↓ Sofia: WR 51.0% — reduzido
    'supertrend':  6,
    'obv':         6,
    'vwap':        8,
    'ichimoku':    4,
    'adx':         0,                  # ★ ZERADO — Sofia: 100% neutro, nossos: 0/3003
    'atr':         6,
}

WEIGHTS_RANGING_CRYPTO = {
    # Em RANGING crypto, osciladores DOMINAM ainda mais
    'bollinger':  16,                  # mean reversion classica em range
    'rsi':        18,
    'cci':        12,
    'stoch':      14,
    'williams':   10,
    'ema_cross':   4,
    'macd':        4,
    'supertrend':  3,
    'obv':         4,
    'vwap':        6,
    'ichimoku':    2,
    'adx':         0,                  # ★ ZERADO
    'atr':         7,
}

# ─── MIXED e CHOPPY: iguais para ambos (pouca evidencia para diferenciar) ─
WEIGHTS_MIXED = {
    'rsi':         10, 'stoch': 7, 'williams': 5, 'cci': 6, 'bollinger': 9,
    'ema_cross':   12, 'macd': 11, 'adx': 9, 'supertrend': 6,
    'ichimoku':    4,  'obv':  4,  'vwap': 7,
    'atr':         10,
}

WEIGHTS_CHOPPY = {
    'rsi':         6,  'stoch': 5, 'williams': 4, 'cci': 5, 'bollinger': 6,
    'ema_cross':   6,  'macd': 6,  'adx': 5, 'supertrend': 4,
    'ichimoku':    2,  'obv':  3,  'vwap': 4,
    'atr':         18,
    'volume_trend': 26,
}

# Backwards compat: WEIGHTS_TRENDING/_RANGING genericos
# (codigo legacy que nao passa asset_type cai aqui — ainda usa stock-style)
WEIGHTS_TRENDING = WEIGHTS_TRENDING_STOCK
WEIGHTS_RANGING  = WEIGHTS_RANGING_STOCK

WEIGHTS_BY_REGIME = {
    'TRENDING': WEIGHTS_TRENDING,
    'RANGING':  WEIGHTS_RANGING,
    'MIXED':    WEIGHTS_MIXED,
    'CHOPPY':   WEIGHTS_CHOPPY,
    'UNKNOWN':  WEIGHTS_MIXED,
}

WEIGHTS_BY_REGIME_STOCK = {
    'TRENDING': WEIGHTS_TRENDING_STOCK,
    'RANGING':  WEIGHTS_RANGING_STOCK,
    'MIXED':    WEIGHTS_MIXED,
    'CHOPPY':   WEIGHTS_CHOPPY,
    'UNKNOWN':  WEIGHTS_MIXED,
}

WEIGHTS_BY_REGIME_CRYPTO = {
    'TRENDING': WEIGHTS_TRENDING_CRYPTO,
    'RANGING':  WEIGHTS_RANGING_CRYPTO,
    'MIXED':    WEIGHTS_MIXED,
    'CHOPPY':   WEIGHTS_CHOPPY,
    'UNKNOWN':  WEIGHTS_MIXED,
}


def get_weights_by_regime(regime: str, asset_type: str = 'stock') -> dict:
    """Retorna pesos apropriados por (regime, asset_type).
    asset_type: 'stock' (default), 'crypto'. ARBI nao usa V3."""
    if asset_type == 'crypto':
        return WEIGHTS_BY_REGIME_CRYPTO.get(regime, WEIGHTS_MIXED)
    return WEIGHTS_BY_REGIME_STOCK.get(regime, WEIGHTS_MIXED)



def _vote_rsi_trending(val: Optional[float], trend_direction: int) -> float:
    """RSI em regime TRENDING: alinhamento com tendência dominante.

    Em UPTREND forte: RSI alto = força continuando (vota a favor);
                      RSI muito baixo (pullback) = entrada oportuna.
    Em DOWNTREND forte: RSI baixo = continuação da queda (vota VENDA);
                        RSI muito alto (pullback) = entrada de SHORT.
    NUNCA vota contra a tendência em regime TRENDING forte.
    """
    if val is None: return 0.0
    if trend_direction > 0:  # uptrend
        if val < 30: return 0.8      # pullback em uptrend = entrada
        if val < 45: return 0.5
        if val < 60: return 0.3      # saudável
        if val < 75: return 0.2
        return -0.1                   # extremo — apenas cautela leve
    if trend_direction < 0:  # downtrend
        if val > 70: return -0.8     # pullback em downtrend = entrada SHORT
        if val > 55: return -0.5
        if val > 40: return -0.3
        if val > 25: return -0.2
        return 0.1                    # extremo oversold — só cautela leve
    return _vote_rsi(val)


def _vote_stoch_trending(s: Optional[Dict], trend_direction: int) -> float:
    if s is None: return 0.0
    k = s['k']
    if trend_direction > 0:
        if k < 20: return 0.7       # oversold em uptrend = entrada
        if k < 50: return 0.4
        if k < 80: return 0.2
        return -0.1
    if trend_direction < 0:
        if k > 80: return -0.7      # overbought em downtrend = entrada SHORT
        if k > 50: return -0.4
        if k > 20: return -0.2
        return 0.1
    return _vote_stoch(s)


def _vote_cci_trending(val: Optional[float], trend_direction: int) -> float:
    if val is None: return 0.0
    if trend_direction > 0:
        if val < -100: return 0.7
        if val < 0:    return 0.3
        if val < 100:  return 0.2
        return -0.1
    if trend_direction < 0:
        if val > 100:  return -0.7
        if val > 0:    return -0.3
        if val > -100: return -0.2
        return 0.1
    return _vote_cci(val)


def _vote_williams_trending(val: Optional[float], trend_direction: int) -> float:
    if val is None: return 0.0
    if trend_direction > 0:
        if val < -85: return 0.7
        if val < -50: return 0.3
        return 0.1
    if trend_direction < 0:
        if val > -15: return -0.7
        if val > -50: return -0.3
        return -0.1
    return _vote_williams(val)


def _vote_bollinger_trending(b: Optional[Dict], trend_direction: int) -> float:
    """Em trending, tocar banda da direção = força."""
    if b is None: return 0.0
    pct_b = b['pct_b']
    if trend_direction > 0:
        if pct_b < 0.2:  return 0.7   # pullback
        if pct_b < 0.5:  return 0.3
        if pct_b < 0.9:  return 0.2
        return 0.1                    # rompendo = força
    if trend_direction < 0:
        if pct_b > 0.8:  return -0.7  # pullback para venda
        if pct_b > 0.5:  return -0.3
        if pct_b > 0.1:  return -0.2
        return -0.1
    return _vote_bollinger(b)


def _vote_macd_trending(m: Optional[Dict], trend_direction: int) -> float:
    """MACD em trending: alinhamento com tendência dominante.
    Pullback de curto prazo (hist pequeno contra-tendência) é ignorado.
    """
    if m is None: return 0.0
    hist = m['hist']
    line = m['line']
    if trend_direction > 0:
        # Uptrend: linha MACD acima de zero é o que importa, não hist
        if line > 0 and hist > 0: return 1.0      # momentum forte
        if line > 0 and hist < 0: return 0.4      # pullback em uptrend
        if line < 0 and hist > 0: return 0.2      # recuperando
        return -0.3                                 # linha abaixo = fraco
    if trend_direction < 0:
        if line < 0 and hist < 0: return -1.0
        if line < 0 and hist > 0: return -0.4     # pullback em downtrend
        if line > 0 and hist < 0: return -0.2
        return 0.3
    return _vote_macd(m)


def _vote_supertrend_trending(s: Optional[Dict], trend_direction: int) -> float:
    """Supertrend em trending: só vota contra se claramente contradiz a tendência.
    Em downtrend forte, Supertrend temporariamente UP (pullback) não muda nada.
    """
    if s is None: return 0.0
    if trend_direction > 0:
        return 0.8 if s['direction'] == 'UP' else 0.0  # contra apenas neutraliza
    if trend_direction < 0:
        return -0.8 if s['direction'] == 'DOWN' else 0.0
    return 0.8 if s['direction'] == 'UP' else -0.8



def _detect_trend_direction(ema_val, macd_val, adx_val, super_val) -> int:
    """Retorna +1 (uptrend), -1 (downtrend), 0 (lateral).

    EMA alignment e ADX/DIs são indicadores de tendência LONGA (peso 2).
    MACD hist e Supertrend são curtos (peso 1) — podem virar em pullbacks.
    """
    votes = 0.0
    n = 0.0
    if ema_val:
        a = ema_val['alignment']
        if a == 'STRONG_BULL':   votes += 3; n += 3
        elif a == 'BULL':        votes += 2; n += 2
        elif a == 'STRONG_BEAR': votes -= 3; n += 3
        elif a == 'BEAR':        votes -= 2; n += 2
    if adx_val and adx_val['adx'] > 20:
        # ADX com força: direção pelos DIs, peso 3
        pd, md = adx_val['plus_di'], adx_val['minus_di']
        if pd > md * 1.5:   votes += 3; n += 3
        elif md > pd * 1.5: votes -= 3; n += 3
        elif pd > md:       votes += 1; n += 1
        elif md > pd:       votes -= 1; n += 1
    if macd_val and macd_val['hist'] is not None:
        if macd_val['hist'] > 0: votes += 1
        elif macd_val['hist'] < 0: votes -= 1
        n += 1
    if super_val:
        if super_val['direction'] == 'UP': votes += 1
        else: votes -= 1
        n += 1
    if n == 0: return 0
    ratio = votes / n
    if ratio >= 0.40: return 1
    if ratio <= -0.40: return -1
    return 0


def compute_score_v3(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    volumes: Optional[List[float]] = None,
    *,
    factor_stats_cache: Optional[Dict] = None,
    pattern_stats_cache: Optional[Dict] = None,
    temporal_adj: float = 0.0,
    asset_type: str = 'stock',  # [SEPARACAO 29/abr] 'stock' ou 'crypto' — usa pesos diferentes
) -> Dict:
    """Score regime-aware — v3.

    Pipeline:
      1. Calcular todos os indicadores
      2. Detectar regime (TRENDING/RANGING/MIXED/CHOPPY)
      3. Detectar direção da tendência (UP/DOWN/NEUTRAL)
      4. Escolher função de voto conforme regime
      5. Aplicar pesos regime-aware
      6. Learning adj + temporal adj
      7. Threshold de convergência
    """
    n = len(closes)
    if n < 20:
        return {'score': 50, 'signal': 'MANTER', 'strength': 0.0,
                'regime': 'UNKNOWN', 'trend_direction': 0, 'votes': {},
                'blocked': True, 'block_reason': 'INSUFFICIENT_DATA',
                'diagnostic': {'n_bars': n}}

    # ── Calcular indicadores ──
    rsi_val = rsi(closes, 14)
    macd_val = macd(closes)
    boll_val = bollinger(closes)
    adx_val = adx(highs, lows, closes)
    cci_val = cci(highs, lows, closes)
    stoch_val = stochastic(highs, lows, closes)
    williams_val = williams_r(highs, lows, closes)
    atr_val = atr_percent(highs, lows, closes)
    ema_val = ema_cross(closes)
    super_val = supertrend(highs, lows, closes)
    ich_val = ichimoku_signal(highs, lows, closes) if n >= 52 else None
    vwap_val = None; obv_val = None; vol_val = None
    if volumes and len(volumes) == len(closes):
        vwap_val = vwap(highs, lows, closes, volumes)
        obv_val = obv(closes, volumes)
        vol_val = volume_strength(volumes)


    # ── Detectar regime e direção ──
    # [FIX 29/abr] Calcular trend_dir + volume bucket ANTES do regime
    trend_dir = _detect_trend_direction(ema_val, macd_val, adx_val, super_val)
    _vol_b_pre = 'LOW' if (atr_val is not None and atr_val < 1) else (
                 'HIGH' if (atr_val is not None and atr_val > 3) else 'NORMAL')
    # Volume real se disponível — tentar inferir bucket por OBV trend ou volumes input
    _vbucket = None
    try:
        if volumes and len(volumes) >= 10:
            recent_vol = sum(volumes[-5:]) / 5
            avg_vol = sum(volumes[-20:]) / max(1, len(volumes[-20:]))
            if avg_vol > 0:
                ratio = recent_vol / avg_vol
                if ratio < 0.3: _vbucket = 'VERY_LOW'
                elif ratio < 0.6: _vbucket = 'LOW'
                elif ratio < 1.5: _vbucket = 'NORMAL'
                else: _vbucket = 'HIGH'
    except Exception:
        _vbucket = None
    regime = _detect_regime(adx_val, atr_val, trend_dir, _vbucket)

    # ── Votos regime-aware ──
    # Em TRENDING ou MIXED (com tendência clara), osciladores viram "timing"
    if regime in ('TRENDING', 'MIXED') and trend_dir != 0:
        votes = {
            'rsi':        _vote_rsi_trending(rsi_val, trend_dir),
            'stoch':      _vote_stoch_trending(stoch_val, trend_dir),
            'cci':        _vote_cci_trending(cci_val, trend_dir),
            'williams':   _vote_williams_trending(williams_val, trend_dir),
            'bollinger':  _vote_bollinger_trending(boll_val, trend_dir),
            'macd':       _vote_macd_trending(macd_val, trend_dir),
            'adx':        _vote_adx(adx_val),
            'ema_cross':  _vote_ema(ema_val),
            'supertrend': _vote_supertrend_trending(super_val, trend_dir),
            'ichimoku':   _vote_ichimoku(ich_val),
            'atr':        _vote_atr(atr_val),
            'vwap':       _vote_vwap(vwap_val),
            'obv':        _vote_obv(obv_val),
        }
    else:
        # RANGING / CHOPPY / sem tendência clara: votos clássicos
        votes = {
            'rsi':        _vote_rsi(rsi_val),
            'stoch':      _vote_stoch(stoch_val),
            'cci':        _vote_cci(cci_val),
            'williams':   _vote_williams(williams_val),
            'bollinger':  _vote_bollinger(boll_val),
            'macd':       _vote_macd(macd_val),
            'adx':        _vote_adx(adx_val),
            'ema_cross':  _vote_ema(ema_val),
            'supertrend': _vote_supertrend(super_val),
            'ichimoku':   _vote_ichimoku(ich_val),
            'atr':        _vote_atr(atr_val),
            'vwap':       _vote_vwap(vwap_val),
            'obv':        _vote_obv(obv_val),
        }

    # Adicionar volume_trend se CHOPPY
    if vol_val:
        vt = 0.5 if vol_val['trend'] == 'UP' else (-0.5 if vol_val['trend'] == 'DOWN' else 0.0)
        votes['volume_trend'] = vt

    # ── Pesos regime-aware ──
    weights = get_weights_by_regime(regime, asset_type=asset_type)

    # ── Score ponderado ──
    total_weight = 0
    weighted_sum = 0.0
    for key, vote in votes.items():
        w = weights.get(key, 0)
        if w == 0: continue
        if key == 'ichimoku' and ich_val is None: continue
        if key == 'vwap' and vwap_val is None: continue
        if key == 'obv' and obv_val is None: continue
        if key == 'volume_trend' and vol_val is None: continue
        weighted_sum += vote * w
        total_weight += w

    if total_weight == 0:
        raw_score = 50.0
    else:
        normalized = weighted_sum / total_weight
        raw_score = 50 + normalized * 50


    # ── Learning adj (via factor_stats_cache do daemon) ──
    # [FIX 29/abr/2026] Tradução de NAMES + uso de ewma_pnl_pct (info real)
    # em vez de confidence_weight (capado em 0.40 por bug de incrementação).
    learning_adj = 0.0
    if factor_stats_cache:
        # RSI — traduz LOW/HIGH -> WEAK/STRONG
        rsi_b = 'OVERSOLD' if rsi_val and rsi_val < 30 else (
                'OVERBOUGHT' if rsi_val and rsi_val > 70 else (
                'LOW' if rsi_val and rsi_val < 45 else (
                'HIGH' if rsi_val and rsi_val > 55 else 'NEUTRAL')))
        rsi_b_fs = _translate_rsi_bucket_for_fs(rsi_b)
        fs = factor_stats_cache.get(('rsi_bucket', rsi_b_fs), {})
        if fs.get('total_samples', 0) >= 30:
            ewma = float(fs.get('ewma_pnl_pct', 0))
            learning_adj += max(-12, min(12, ewma * 16))
        # EMA — traduz STRONG_BULL -> BULLISH_STACK
        if ema_val:
            ema_align_fs = _translate_ema_alignment_for_fs(ema_val['alignment'])
            fs = factor_stats_cache.get(('ema_alignment', ema_align_fs), {})
            if fs.get('total_samples', 0) >= 30:
                ewma = float(fs.get('ewma_pnl_pct', 0))
                learning_adj += max(-12, min(12, ewma * 16))
        # Volatility — factor_stats usa atr_bucket, não volatility_bucket
        vol_b = 'LOW' if atr_val and atr_val < 1 else (
                'HIGH' if atr_val and atr_val > 3 else 'NORMAL')
        fs = factor_stats_cache.get(('atr_bucket', vol_b), {})
        if fs.get('total_samples', 0) >= 30:
            ewma = float(fs.get('ewma_pnl_pct', 0))
            learning_adj += max(-8, min(8, ewma * 10))
        # Regime
        fs = factor_stats_cache.get(('regime_mode', regime), {})
        if fs.get('total_samples', 0) >= 30:
            ewma = float(fs.get('ewma_pnl_pct', 0))
            learning_adj += max(-6, min(6, ewma * 8))

    # ═══════════════════════════════════════════════════════════════════
    # [PATTERN_CALIBRATION 29/abr/2026] Padrões confirmados em trades reais.
    # Stock: 3548 trades. Crypto: 3003 trades. Cada um tem regras proprias.
    # ═══════════════════════════════════════════════════════════════════
    pattern_adj = 0.0
    pattern_notes = []
    direction_inferred = 'LONG' if raw_score >= 50 else 'SHORT'
    # Inicializar combos (usado depois em raw_score += combo_adj)
    combo_adj = 0
    combo_block = False
    combo_notes = []

    # CRYPTO-specific patterns (asset_type='crypto')
    if asset_type == 'crypto':
        # [v2 29/abr] Operacao 24/7 mantida. Logica:
        # GOLDEN_HOURS = forca maxima (+8)
        # BAD_HOURS LONG: -8 padrao, mas se trend_dir>0 confirmado: apenas -3
        #   (deixa entrar LONG em BAD_HOUR se mercado realmente em alta)
        # BAD_HOURS SHORT: -8 mantido
        try:
            from datetime import datetime
            _h_now_utc = datetime.utcnow().hour
            if _h_now_utc in GOLDEN_HOURS_CRYPTO_UTC:
                pattern_adj += 8  # ↑ era 5 — forca maxima nas horas boas
                pattern_notes.append(f'GOLDEN_HOUR_CRYPTO_{_h_now_utc}+8')
            elif _h_now_utc in BAD_HOURS_CRYPTO_UTC:
                # LONG em hora ruim: so se uptrend confirmado
                if direction_inferred == 'LONG' and trend_dir > 0:
                    # ema BULLISH? cross ou stack
                    ema_align = ema_val.get('alignment', '') if ema_val else ''
                    if ema_align in ('STRONG_BULL', 'BULL'):
                        pattern_adj -= 3  # penalty leve — uptrend confirmado
                        pattern_notes.append(f'BAD_HOUR_CRYPTO_LONG_UPTREND_{_h_now_utc}-3')
                    else:
                        pattern_adj -= 8  # sem uptrend = penalty cheio
                        pattern_notes.append(f'BAD_HOUR_CRYPTO_LONG_NO_UPTREND_{_h_now_utc}-8')
                else:
                    # SHORT em hora ruim: penalty cheio
                    pattern_adj -= 8
                    pattern_notes.append(f'BAD_HOUR_CRYPTO_{_h_now_utc}-8')
        except Exception:
            pass

        if direction_inferred == 'LONG':
            # ATR LOW + LONG = mercado morto, sangra (-$79/trade em crypto)
            atr_b = 'VERY_LOW' if atr_val and atr_val < 0.5 else (
                    'LOW' if atr_val and atr_val < 1.0 else (
                    'NORMAL' if atr_val and atr_val < 2.0 else (
                    'HIGH' if atr_val and atr_val < 3.5 else 'EXTREME')))
            if atr_b in ('VERY_LOW', 'LOW'):
                pattern_adj -= 6
                pattern_notes.append(f'ATR_DEAD_CRYPTO_LONG-6')

            # TRENDING + LONG em crypto = -$68 (crypto reverte trends)
            if regime == 'TRENDING':
                pattern_adj -= 5
                pattern_notes.append('TRENDING_LONG_CRYPTO-5')

            # Volume SURGE + LONG = chase pump (-$57)
            # Inferir SURGE: ratio recent5/avg20 > 2.5
            if volumes and len(volumes) >= 20:
                try:
                    r5 = sum(volumes[-5:])/5
                    a20 = sum(volumes[-20:])/20
                    if a20 > 0 and r5/a20 > 2.5:
                        pattern_adj -= 5
                        pattern_notes.append('VOL_SURGE_LONG_CRYPTO-5')
                except Exception:
                    pass

        else:  # SHORT crypto
            # SHORT em RANGING = -$70/trade (crypto sai de range pra cima)
            if regime == 'RANGING':
                pattern_adj -= 6
                pattern_notes.append('RANGING_SHORT_CRYPTO-6')

        pattern_adj = max(-15, min(15, pattern_adj))
        # CRYPTO termina aqui — nao aplica regras stock

    # ─── STOCK-specific patterns (asset_type='stock' ou nao especificado) ─
    if asset_type != 'crypto':
      # Detectar volume bucket pelo input
      vol_bucket_real = None
      if volumes and len(volumes) >= 20:
          try:
              recent5 = sum(volumes[-5:]) / 5
              avg20 = sum(volumes[-20:]) / 20
              if avg20 > 0:
                  ratio = recent5 / avg20
                  if ratio < 0.3: vol_bucket_real = 'VERY_LOW'
                  elif ratio < 0.6: vol_bucket_real = 'LOW'
                  elif ratio < 1.5: vol_bucket_real = 'NORMAL'
                  elif ratio < 2.5: vol_bucket_real = 'HIGH'
                  else: vol_bucket_real = 'VERY_HIGH'
          except Exception:
              pass

      # Detectar atr bucket
      atr_bucket_real = None
      if atr_val is not None:
          if atr_val < 0.5: atr_bucket_real = 'VERY_LOW'
          elif atr_val < 1.0: atr_bucket_real = 'LOW'
          elif atr_val < 2.0: atr_bucket_real = 'NORMAL'
          elif atr_val < 3.5: atr_bucket_real = 'HIGH'
          else: atr_bucket_real = 'EXTREME'

      if direction_inferred == 'LONG':
          # ✓ BULLISH (LONG) calibrações
          # Volume NORMAL + LONG = melhor bucket (avg +$169, n=347)
          if vol_bucket_real == 'NORMAL':
              pattern_adj += 6
              pattern_notes.append('VOL_NORMAL_LONG+6')
          # Volume LOW/VERY_LOW + LONG = sinal fraco (sem convicção)
          elif vol_bucket_real in ('LOW', 'VERY_LOW'):
              pattern_adj -= 8
              pattern_notes.append('VOL_LOW_LONG-8')

          # BEARISH_CROSS + LONG = reversão de fundo (n=128, WR 57%, avg +$201) — counter-intuitivo!
          if ema_val and ema_val.get('alignment') == 'BEAR':  # V3: BEAR == factor_stats BEARISH_CROSS
              pattern_adj += 5
              pattern_notes.append('EMA_BEAR_REV+5')

          # Volatility LOW + LONG = mercado morto, perde (avg -$90, n=159)
          if atr_bucket_real in ('VERY_LOW', 'LOW') and vol_bucket_real not in ('NORMAL', 'HIGH'):
              pattern_adj -= 6
              pattern_notes.append('ATR_LOW_NO_VOL-6')

          # ATR HIGH + LONG = volátil, perde (avg -$30, n=474)
          if atr_bucket_real == 'HIGH':
              pattern_adj -= 3
              pattern_notes.append('ATR_HIGH_LONG-3')

          # TRENDING confirmado + LONG = bom (n=684, +$42k)
          if regime == 'TRENDING' and trend_dir > 0:
              pattern_adj += 4
              pattern_notes.append('TRENDING_UP+4')
          # TRENDING sem volume confirmado = NÃO é trend real
          elif regime == 'TRENDING' and vol_bucket_real in ('LOW', 'VERY_LOW'):
              pattern_adj -= 10
              pattern_notes.append('FAKE_TRENDING-10')

      else:  # SHORT
          # ✗ SHORT calibrações — MAIORIA é negativa, manter SHORT em stocks geralmente bloqueado
          # RSI STRONG + SHORT = perda (avg -$106, n=370)
          rsi_check = 'OVERSOLD' if rsi_val and rsi_val < 30 else (
                      'OVERBOUGHT' if rsi_val and rsi_val > 70 else (
                      'WEAK' if rsi_val and rsi_val < 45 else (
                      'STRONG' if rsi_val and rsi_val > 55 else 'NEUTRAL')))
          if rsi_check == 'STRONG':
              pattern_adj -= 6
              pattern_notes.append('RSI_STRONG_SHORT-6')
          # BEARISH_STACK + SHORT = sinal "obvio" mas perde (avg -$58, n=643)
          if ema_val and ema_val.get('alignment') == 'STRONG_BEAR':
              pattern_adj -= 4
              pattern_notes.append('EMA_BEAR_STACK_SHORT-4')
          # HIGH_VOL regime + SHORT = catástrofe (avg -$250, n=62)
          if regime == 'CHOPPY':  # CHOPPY no V3 ~= HIGH_VOL no factor_stats
              pattern_adj -= 8
              pattern_notes.append('CHOPPY_SHORT-8')

      # ═══════════════════════════════════════════════════════════════════
      # [COMBO_RULES 29/abr] Combos especificos baseados em 3548 trades reais
      # ═══════════════════════════════════════════════════════════════════
      combo_adj = 0
      combo_block = False
      combo_notes = []

      ema_fs = None
      if ema_val:
          ema_fs = _translate_ema_alignment_for_fs(ema_val.get('alignment', ''))

      if direction_inferred == 'LONG' and ema_fs and vol_bucket_real:
          combo_key = (regime, ema_fs, vol_bucket_real)
          if combo_key in LETHAL_COMBOS_LONG:
              combo_block = True
              combo_notes.append(f'LETHAL_{combo_key[0]}_{combo_key[1]}_{combo_key[2]}')
          elif combo_key in GOLDEN_COMBOS_LONG:
              bonus = GOLDEN_COMBOS_LONG[combo_key]
              combo_adj += bonus
              combo_notes.append(f'GOLDEN_{combo_key[0]}_{combo_key[1]}_{combo_key[2]}+{bonus}')

      # Aplicar pattern_adj com clamp
      pattern_adj = max(-15, min(15, pattern_adj))

    raw_score += learning_adj
    raw_score += temporal_adj
    raw_score += pattern_adj
    raw_score += combo_adj

    # Hard block para combos letais — score 49 (não passa em threshold 70)
    if combo_block:
        raw_score = min(raw_score, 49)

    final_score = int(max(0, min(100, raw_score)))

    # ── Convergence strength ──
    convergence = abs(weighted_sum / total_weight) if total_weight > 0 else 0

    # ── Pattern block ──
    blocked = False
    block_reason = ''
    if pattern_stats_cache:
        for key, ps in list(pattern_stats_cache.items())[:200]:
            n_samples = ps.get('total_samples', 0)
            wins = ps.get('wins', 0)
            if n_samples >= 30 and wins / n_samples < 0.40:
                ewma_hit = ps.get('ewma_hit_rate', 1.0)
                if ewma_hit < 0.45:
                    blocked = True
                    block_reason = f'PATTERN_BLOCK_{key}'
                    break

    # ── Signal ──
    # v3: threshold varia conforme regime
    # CHOPPY: mais restrito (só emite se score forte E convergência alta)
    # TRENDING com trend claro: mais permissivo
    if regime == 'CHOPPY':
        threshold_long = 78; threshold_short = 22; min_conv = 0.35
    elif regime == 'TRENDING' and trend_dir != 0:
        threshold_long = 65; threshold_short = 35; min_conv = 0.20
    elif regime == 'RANGING':
        threshold_long = 72; threshold_short = 28; min_conv = 0.25
    else:
        threshold_long = 70; threshold_short = 30; min_conv = 0.25

    if final_score >= threshold_long and convergence >= min_conv:
        signal = 'COMPRA'
    elif final_score <= threshold_short and convergence >= min_conv:
        signal = 'VENDA'
    else:
        signal = 'MANTER'


    return {
        'score': final_score,
        'signal': signal,
        'strength': round(convergence, 3),
        'regime': regime,
        'trend_direction': trend_dir,
        'votes': {k: round(v, 3) for k, v in votes.items()},
        'blocked': blocked,
        'block_reason': block_reason,
        'diagnostic': {
            'rsi': rsi_val,
            'macd_hist': macd_val['hist'] if macd_val else None,
            'bb_pct_b': boll_val['pct_b'] if boll_val else None,
            'adx': adx_val['adx'] if adx_val else None,
            'cci': cci_val,
            'stoch_k': stoch_val['k'] if stoch_val else None,
            'williams': williams_val,
            'atr_pct': atr_val,
            'ema_alignment': ema_val['alignment'] if ema_val else None,
            'vwap_dev': vwap_val['deviation_pct'] if vwap_val else None,
            'obv_trend': obv_val['trend'] if obv_val else None,
            'supertrend_dir': super_val['direction'] if super_val else None,
            'ichimoku_sig': ich_val['signal'] if ich_val else None,
            'n_bars': n,
            'total_weight_used': total_weight,
            'weighted_sum': round(weighted_sum, 3),
            'pattern_adj': round(pattern_adj, 2),
            'pattern_notes': pattern_notes,
            'combo_adj': combo_adj,
            'combo_notes': combo_notes,
            'combo_block': combo_block,
            'learning_adj': round(learning_adj, 2),
            'temporal_adj': round(temporal_adj, 2),
            'raw_score_pre_clamp': round(raw_score, 2),
            'convergence': round(convergence, 3),
            'threshold_long': threshold_long,
            'threshold_short': threshold_short,
        }
    }
