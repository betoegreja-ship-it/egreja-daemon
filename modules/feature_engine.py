"""
[v10.24] Feature Engineering Module
Feature bucketing and extraction for learning engine.
Pure functions - no mutable state, no global dependencies.
"""

import hashlib
from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# BUCKETING FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def _score_bucket(score: float) -> str:
    """Bucket a signal score (0-100) into 5 levels."""
    if score <= 29:
        return 'VERY_LOW'
    if score <= 49:
        return 'LOW'
    if score <= 69:
        return 'NEUTRAL'
    if score <= 84:
        return 'HIGH'
    return 'VERY_HIGH'

def _rsi_bucket(rsi: float) -> str:
    """Bucket RSI (0-100) into 5 levels."""
    if rsi < 30:
        return 'OVERSOLD'
    if rsi < 45:
        return 'WEAK'
    if rsi < 55:
        return 'NEUTRAL'
    if rsi < 70:
        return 'STRONG'
    return 'OVERBOUGHT'

def _ema_alignment(ema9: float, ema21: float, ema50: float, price: float) -> str:
    """Determine EMA alignment pattern."""
    if price > ema9 > ema21 > ema50:
        return 'BULLISH_STACK'
    if price < ema9 < ema21 < ema50:
        return 'BEARISH_STACK'
    if ema9 > ema21:
        return 'BULLISH_CROSS'
    if ema9 < ema21:
        return 'BEARISH_CROSS'
    return 'MIXED'

def _change_pct_bucket(change_pct: float) -> str:
    """Bucket price change percentage."""
    a = abs(change_pct)
    if a < 0.5:
        return 'FLAT'
    if a < 1.5:
        return 'SMALL'
    if a < 3.0:
        return 'MEDIUM'
    if a < 6.0:
        return 'LARGE'
    return 'EXTREME'

def _volatility_bucket(regime_volatility: str) -> str:
    """Normalize regime volatility level."""
    return regime_volatility or 'NORMAL'

def _time_bucket(dt: datetime) -> str:
    """Bucket time of day into 7 periods."""
    h = dt.hour
    if h < 6:
        return 'OVERNIGHT'
    if h < 10:
        return 'PRE_MARKET'
    if h < 12:
        return 'MORNING'
    if h < 14:
        return 'MIDDAY'
    if h < 17:
        return 'AFTERNOON'
    if h < 20:
        return 'EVENING'
    return 'NIGHT'

def _data_quality_bucket(dq_score: float) -> str:
    """Bucket data quality score (0-100) into 3 levels."""
    if dq_score >= 90:
        return 'HIGH'
    if dq_score >= 60:
        return 'MEDIUM'
    return 'LOW'

def _atr_bucket(atr_pct: float) -> str:
    """[v10.4] Bucket ATR as % of price — volatility real, não só regime de crypto."""
    if atr_pct <= 0:
        return 'UNKNOWN'
    if atr_pct < 0.5:
        return 'VERY_LOW'
    if atr_pct < 1.5:
        return 'LOW'
    if atr_pct < 3.0:
        return 'NORMAL'
    if atr_pct < 6.0:
        return 'HIGH'
    return 'EXTREME'

def _volume_bucket(vol_ratio: float) -> str:
    """[v10.4] Bucket volume ratio (volume_atual / volume_médio_20d).
    >1.5 = volume above average (confirms movement); <0.7 = weak volume."""
    if vol_ratio <= 0:
        return 'UNKNOWN'
    if vol_ratio < 0.5:
        return 'VERY_LOW'
    if vol_ratio < 0.8:
        return 'LOW'
    if vol_ratio < 1.3:
        return 'NORMAL'
    if vol_ratio < 2.0:
        return 'HIGH'
    return 'SURGE'

# ═══════════════════════════════════════════════════════════════
# ATR CALCULATION
# ═══════════════════════════════════════════════════════════════

def _calc_atr(closes: list, highs: list = None, lows: list = None, period: int = 14) -> float:
    """[v10.4] ATR simplificado. Se highs/lows não disponíveis, usa desvio de closes."""
    if len(closes) < 2:
        return 0.0
    if highs and lows and len(highs) == len(closes):
        trs = []
        for i in range(1, min(period + 1, len(closes))):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            trs.append(max(hl, hc, lc))
        return sum(trs) / len(trs) if trs else 0.0
    # Fallback: desvio médio absoluto dos closes
    n = min(period, len(closes))
    diffs = [abs(closes[i] - closes[i-1]) for i in range(1, n + 1)]
    return sum(diffs) / len(diffs) if diffs else 0.0

# ═══════════════════════════════════════════════════════════════
# FEATURE EXTRACTION
# ═══════════════════════════════════════════════════════════════

def extract_features(sig: dict, regime: dict, dq_score: float, now: datetime) -> dict:
    """[L-1][v10.4] Extrai features canônicas de um sinal para learning.
    Inclui atr_bucket e volume_bucket para espaço de padrões mais discriminativo.

    Args:
        sig: Signal dict with score, rsi, ema9/21/50, price, change_pct, signal, asset_type, market_type, atr_pct, volume_ratio
        regime: Regime dict with volatility and mode
        dq_score: Data quality score (0-100)
        now: Current datetime (used for time_bucket, weekday)

    Returns:
        dict with bucketed features for learning
    """
    score     = float(sig.get('score', 50) or 50)
    rsi       = float(sig.get('rsi', 50) or 50)
    ema9      = float(sig.get('ema9', 0) or 0)
    ema21     = float(sig.get('ema21', 0) or 0)
    ema50     = float(sig.get('ema50', 0) or 0)
    price     = float(sig.get('price', 0) or 0)
    change    = float(sig.get('change_pct', sig.get('change_24h', 0)) or 0)
    direction = 'LONG' if sig.get('signal') == 'COMPRA' else ('SHORT' if sig.get('signal') == 'VENDA' else 'NEUTRAL')
    asset_t   = sig.get('asset_type', 'stock')
    mkt       = sig.get('market_type', 'NYSE')

    # [v10.4] ATR e volume — vindos do price_dict ou do sig_enriched
    atr_pct    = float(sig.get('atr_pct', 0) or 0)
    vol_ratio  = float(sig.get('volume_ratio', 0) or 0)

    return {
        'score_bucket':      _score_bucket(score),
        'rsi_bucket':        _rsi_bucket(rsi),
        'ema_alignment':     _ema_alignment(ema9, ema21, ema50, price),
        'change_pct_bucket': _change_pct_bucket(change),
        'volatility_bucket': _volatility_bucket(regime.get('volatility', 'NORMAL')),
        'regime_mode':       regime.get('mode', 'UNKNOWN'),
        'time_bucket':       _time_bucket(now),
        'weekday':           now.weekday(),   # 0=segunda
        'asset_type':        asset_t,
        'market_type':       mkt,
        'direction':         direction,
        'dq_bucket':         _data_quality_bucket(dq_score),
        'atr_bucket':        _atr_bucket(atr_pct),       # [v10.4]
        'volume_bucket':     _volume_bucket(vol_ratio),  # [v10.4]
    }

# ═══════════════════════════════════════════════════════════════
# FEATURE HASHING
# ═══════════════════════════════════════════════════════════════

def make_feature_hash(features: dict) -> str:
    """[L-1][v10.4] Hash canônico determinístico — espaço ampliado com atr, volume e weekday.
    weekday distingue comportamento segunda-feira (gap open) de quarta/quinta (fluxo normal).

    Args:
        features: Feature dict from extract_features()

    Returns:
        16-char MD5 hash hex string
    """
    canonical = '|'.join([
        features.get('score_bucket', ''),
        features.get('rsi_bucket', ''),
        features.get('ema_alignment', ''),
        features.get('volatility_bucket', ''),
        features.get('regime_mode', ''),
        features.get('time_bucket', ''),
        features.get('asset_type', ''),
        features.get('direction', ''),
        features.get('atr_bucket', ''),       # [v10.4]
        features.get('volume_bucket', ''),    # [v10.4]
        str(features.get('weekday', '')),     # [v10.4]
    ])
    return hashlib.md5(canonical.encode()).hexdigest()[:16]
