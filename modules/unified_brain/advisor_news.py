"""
Brain Advisor V4 — Voto de News

Tier 1 (gratuito): consulta Polygon News API (já temos POLYGON_API_KEY)
e conta menções do símbolo nas últimas 24h, com sentimento básico.

Tier 2 (futuro): LLM para análise semântica. Desativado por enquanto.

IMPORTANTE:
- Cache de 15 min por símbolo — não pode consultar Polygon a cada trade
- Se falhar a API, retorna voto neutro (não trava o advisor)
"""
from __future__ import annotations
import os
import time
from typing import Dict, Any, Optional
from .advisor_common import get_cache, DEFAULT_NEUTRAL_VOTE


NEWS_CACHE_TTL = 900   # 15 min
POLYGON_KEY = os.environ.get('POLYGON_API_KEY', '').strip()


# Mapa B3 → ADR para buscar news em Polygon (B3 não tem news em Polygon)
B3_TO_ADR = {
    'PETR4': 'PBR', 'VALE3': 'VALE', 'ITUB4': 'ITUB',
    'BBDC4': 'BBD', 'ABEV3': 'ABEV', 'BBAS3': None,
    'B3SA3': None, 'BOVA11': None,
}


def _normalize_news_symbol(symbol: str, asset_type: str) -> Optional[str]:
    """Converte símbolo pra formato aceito por Polygon News.
    Retorna None se não for possível buscar news pra esse asset."""
    if asset_type == 'stock':
        # NYSE: usa direto
        # B3: tenta ADR correspondente
        sym = symbol.upper().strip()
        if sym in B3_TO_ADR:
            return B3_TO_ADR[sym]
        return sym
    elif asset_type == 'crypto':
        # Polygon não cobre crypto news bem; pula
        return None
    return None


def news_vote(db_fn, log, *,
              symbol: str, asset_type: str,
              direction: Optional[str] = None) -> Dict[str, Any]:
    """Voto 0..1. Alto = news favorecem a direction proposta.

    Tier 1 heurística simples:
    - Conta N news nas últimas 24h
    - Se tem muitas news (>5): voto levemente baixo (incerteza)
    - Se tem poucas (0-2): voto neutro-alto
    - Sem LLM, não fazemos sentiment real
    """
    result = {
        'vote': DEFAULT_NEUTRAL_VOTE,
        'n_news': 0,
        'reason': 'neutral_default',
    }

    news_sym = _normalize_news_symbol(symbol, asset_type)
    if not news_sym:
        # Crypto ou símbolo sem ADR mapeado: neutro
        result['reason'] = 'no_news_source'
        return result

    if not POLYGON_KEY:
        result['reason'] = 'no_polygon_key'
        return result

    cache = get_cache()
    key = f'news:{news_sym}'
    cached = cache.get(key)
    if cached is not None:
        return cached

    # Consulta Polygon News API
    try:
        import urllib.request
        import urllib.parse
        import json as _json
        url = ('https://api.polygon.io/v2/reference/news'
               + '?' + urllib.parse.urlencode({
                   'ticker': news_sym,
                   'limit': 10,
                   'order': 'desc',
                   'apiKey': POLYGON_KEY,
               }))
        req = urllib.request.Request(url, headers={'User-Agent': 'egreja-advisor/1.0'})
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            data = _json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        log.debug(f'[ADVISOR:news] polygon err {news_sym}: {e}')
        result['reason'] = 'api_error'
        # Cacheia resultado neutro pra não bombardear Polygon
        cache.set(key, result)
        return result

    items = data.get('results', []) or []
    # Filtra só news das últimas 24h
    from datetime import datetime, timedelta
    cutoff = datetime.utcnow() - timedelta(hours=24)
    recent = []
    for it in items:
        try:
            pub = it.get('published_utc', '')
            if pub:
                # Polygon retorna ISO 8601
                dt = datetime.fromisoformat(pub.replace('Z', '+00:00')).replace(tzinfo=None)
                if dt > cutoff:
                    recent.append(it)
        except Exception:
            continue

    n_news = len(recent)

    # Heurística Tier 1:
    # 0 news → vote neutro-alto (0.65) — sem ruído de notícia
    # 1-3 news → vote alto (0.75) — fluxo normal
    # 4-7 news → vote neutro (0.50) — algo rolando, pode ser incerteza
    # 8+ news → vote baixo (0.35) — muita notícia, alta volatilidade
    if n_news == 0:
        vote = 0.65
    elif n_news <= 3:
        vote = 0.70
    elif n_news <= 7:
        vote = 0.50
    else:
        vote = 0.35

    result.update({
        'vote': round(vote, 3),
        'n_news': n_news,
        'reason': f'n_news_{n_news}',
    })
    cache.set(key, result)
    return result

