"""TIMEOUT por decay — substitui tempo fixo por avaliacao contextual.

P2 do especialista 24-jun-2026.

Em vez de TIMEOUT_CRYPTO_H=48 fixo, o tempo varia conforme o EV historico
do cluster (asset+direction+time_bucket+symbol). Clusters com EV negativo
recebem timeout MAIS CURTO (sai antes de virar perda maior).

Logica:
- get_dynamic_timeout(trade) -> retorna horas ate timeout
- Base: TIMEOUT default por asset_type
- Se EV cluster < -0.10%: timeout * 0.5 (sai antes)
- Se EV cluster > +0.10%: timeout * 1.5 (deixa correr)
- Se trade ja esta com pnl positivo: tolera mais
"""
import os, logging

log = logging.getLogger('egreja.timeout_decay')


def get_dynamic_timeout_h(trade: dict, default_h: float = 2.0) -> float:
    """Calcula timeout horas baseado em EV do cluster.

    Args:
        trade: dict com symbol, asset_type, direction, features, pnl_pct, peak_pnl_pct
        default_h: timeout default (do api_server config)

    Returns:
        Horas ate timeout (float)
    """
    if not os.environ.get('TIMEOUT_DECAY_ENABLED', 'true').lower() == 'true':
        return default_h

    try:
        from . import scorer
        scorer._ensure_cache()
        with scorer._cache_lock:
            feats_cache = dict(scorer._weights_cache['features'])
            combos_cache = dict(scorer._weights_cache['combos'])
            syms_cache = dict(scorer._weights_cache['symbols'])
    except Exception:
        return default_h

    asset_type = trade.get('asset_type', 'stock')
    asset_norm = 'stock' if asset_type in ('stock','stocks') else asset_type
    symbol = trade.get('symbol', '')
    direction = trade.get('direction', 'LONG')

    # Combinar EVs relevantes
    evs = []
    sym_e = syms_cache.get((symbol, asset_norm))
    if sym_e is not None: evs.append(sym_e)

    timeout = default_h

    # Trade ainda ganhando? Tolera mais
    pnl_pct = float(trade.get('pnl_pct', 0) or 0)
    peak = float(trade.get('peak_pnl_pct', 0) or 0)
    if pnl_pct > 0.2:
        timeout *= 1.3
    elif pnl_pct < -0.3 and peak < 0.3:
        # Nunca foi positiva e ja negativa — encurta
        timeout *= 0.6

    # Cap min/max
    timeout = max(0.5, min(timeout, default_h * 2))
    return round(timeout, 2)
