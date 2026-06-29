"""Adaptive Bridge — conecta Advisor V4 ao Adaptive Learning Brain.

Fornece 2 funções rápidas (com cache) pro Advisor V4 consumir:
  1. get_pattern_verdict(pattern_hash, asset_type)
     Retorna 'GOLD'/'GREEN'/'YELLOW'/'RED'/'GREY'/None (cache)
     Advisor pode usar pra vetar (RED) ou boostar (GOLD).

  2. get_confidence_penalty(asset_type, confidence_value)
     Retorna int: penalty a aplicar no score (negativo) ou 0 se ok.
     Baseado no que confidence_calibrator descobriu.

Design:
- Cache TTL 1h — queries só rodam de hora em hora, zero overhead no hot path
- Bypass se derivatives/arbi (respeita isolation)
- Retorna defaults neutros se DB indisponível ou tabelas vazias
"""
from __future__ import annotations
import time
import threading
from typing import Any, Dict, Optional, Tuple
from .isolation import should_bypass_adaptive_learning


_CACHE_TTL_SEC = 3600  # 1 hora
_pattern_cache: Dict[Tuple[str, str, str], Tuple[float, Optional[str]]] = {}
_conf_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_cache_lock = threading.Lock()


def _market_scope(asset_type: str, market_type: Optional[str] = None) -> str:
    if asset_type in ('stock', 'stocks') and market_type in ('B3', 'NYSE', 'NASDAQ'):
        return f'stock:{market_type}'
    return asset_type


def get_pattern_verdict(db_fn, log, pattern_hash, asset_type, market_type: Optional[str] = None):
    """Retorna 'GOLD'|'GREEN'|'YELLOW'|'RED'|'GREY'|None pra (hash, asset_type)."""
    if should_bypass_adaptive_learning(asset_type):
        return None
    if not pattern_hash:
        return None

    scope = _market_scope(asset_type, market_type)
    key = (pattern_hash, asset_type, scope)
    now = time.time()
    with _cache_lock:
        cached = _pattern_cache.get(key)
        if cached and (now - cached[0]) < _CACHE_TTL_SEC:
            return cached[1]

    verdict = None
    conn = None
    try:
        conn = db_fn()
        if not conn:
            return None
        c = conn.cursor()
        # pattern_hash ja inclui market_type no feature_hash canonico. Tentamos
        # primeiro o escopo market-specific, depois o asset_type legado.
        c.execute("""SELECT actionability FROM learning_pattern_intelligence
                     WHERE pattern_hash = %s AND asset_type IN (%s, %s)
                     ORDER BY id DESC LIMIT 1""",
                  (pattern_hash, scope, asset_type))
        r = c.fetchone()
        if r:
            verdict = r[0]
    except Exception as e:
        log.debug(f'[ADAPTIVE-BRIDGE] pattern_verdict: {e}')
    finally:
        if conn:
            try: conn.close()
            except Exception: pass

    with _cache_lock:
        _pattern_cache[key] = (now, verdict)
    return verdict


def get_confidence_penalty(db_fn, log, asset_type, confidence_value, market_type: Optional[str] = None):
    """Retorna int penalty pra score baseado em calibração da confidence band."""
    if should_bypass_adaptive_learning(asset_type):
        return 0
    if confidence_value is None:
        return 0

    scope = _market_scope(asset_type, market_type)
    now = time.time()
    with _cache_lock:
        cached = _conf_cache.get(scope)
        if cached and (now - cached[0]) < _CACHE_TTL_SEC:
            return _apply_penalty(cached[1], confidence_value)

    bands = {}
    conn = None
    try:
        conn = db_fn()
        if not conn:
            return 0
        c = conn.cursor(dictionary=True)
        c.execute("""SELECT band_lower, band_upper, inversion_flag,
                            recommended_dead_zone, sample_size, total_pnl
                     FROM learning_confidence_calibration
                     WHERE asset_type = %s
                     ORDER BY id DESC LIMIT 20""", (scope,))
        rows = c.fetchall()
        if not rows and scope != asset_type and asset_type in ('stock', 'stocks'):
            # Fail-safe: nao aplicar penalidade agregada de stock em B3/NYSE.
            # Sem calibracao market-specific, o voto deve ser neutro.
            rows = []
        seen = set()
        for r in rows:
            k = (float(r['band_lower']), float(r['band_upper']))
            if k in seen:
                continue
            seen.add(k)
            bands[f"{k[0]}-{k[1]}"] = r
    except Exception as e:
        log.debug(f'[ADAPTIVE-BRIDGE] confidence_penalty: {e}')
    finally:
        if conn:
            try: conn.close()
            except Exception: pass

    with _cache_lock:
        _conf_cache[scope] = (now, bands)
    return _apply_penalty(bands, confidence_value)


def _apply_penalty(bands, confidence_value):
    for key, r in bands.items():
        lo = float(r['band_lower'])
        hi = float(r['band_upper'])
        if lo <= confidence_value < hi:
            n = int(r['sample_size'])
            if n < 30:
                return 0
            if int(r['inversion_flag']) == 1 and confidence_value >= 70:
                return -15
            if int(r['recommended_dead_zone']) == 1:
                return -10
            total_pnl = float(r.get('total_pnl') or 0)
            if total_pnl < -20000 and n >= 100:
                return -5
            return 0
    return 0


def clear_cache():
    with _cache_lock:
        _pattern_cache.clear()
        _conf_cache.clear()
