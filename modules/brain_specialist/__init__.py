"""Brain Specialist - 3 cerebros independentes (B3, NYSE, CRYPTO)."""
import logging
log = logging.getLogger('egreja.brain_specialist')

MARKETS = ('B3', 'NYSE', 'CRYPTO')


def detect_market(symbol: str, asset_type: str = None) -> str:
    """Detecta market a partir do symbol/asset_type."""
    if not symbol:
        return 'NYSE'
    s = symbol.upper().strip()
    at = (asset_type or '').lower()
    if at in ('crypto', 'cryptocurrency'):
        return 'CRYPTO'
    if s.endswith(('USDT', 'USDC', 'USD', 'BUSD')):
        return 'CRYPTO'
    crypto_prefixes = ('BTC', 'ETH', 'SOL', 'ADA', 'DOGE', 'XRP', 'MATIC', 'DOT', 'AVAX',
                       'LINK', 'UNI', 'ATOM', 'LTC', 'BCH', 'TRX', 'BNB', 'FIL', 'APT',
                       'NEAR', 'ARB', 'OP', 'SUI', 'TON', 'INJ', 'FET', 'RNDR', 'TIA')
    if any(s.startswith(p) for p in crypto_prefixes) and len(s) <= 8 and s not in ('FILE', 'TIAA'):
        return 'CRYPTO'
    if s[-1].isdigit() and at != 'crypto':
        return 'B3'
    return 'NYSE'


__all__ = ['MARKETS', 'detect_market', 'log']
