"""
[v10.24] Market Calendar Module
Market holidays, timezones, and market status functions.
Pure functions - no mutable state.
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

# ═══════════════════════════════════════════════════════════════
# CALENDÁRIO DE FERIADOS 2025-2027
# ═══════════════════════════════════════════════════════════════
NYSE_HOLIDAYS = {
    date(2025,1,1), date(2025,1,20), date(2025,2,17), date(2025,4,18),
    date(2025,5,26), date(2025,6,19), date(2025,7,4), date(2025,9,1),
    date(2025,11,27), date(2025,12,25),
    date(2026,1,1), date(2026,1,19), date(2026,2,16), date(2026,4,3),
    date(2026,5,25), date(2026,6,19), date(2026,7,3), date(2026,9,7),
    date(2026,11,26), date(2026,12,25),
    date(2027,1,1), date(2027,1,18), date(2027,2,15), date(2027,3,26),
    date(2027,5,31), date(2027,6,18), date(2027,7,5), date(2027,9,6),
    date(2027,11,25), date(2027,12,24),
}

B3_HOLIDAYS = {
    date(2025,1,1), date(2025,3,3), date(2025,3,4), date(2025,4,18),
    date(2025,4,21), date(2025,5,1), date(2025,6,19), date(2025,9,7),
    date(2025,10,12), date(2025,11,2), date(2025,11,15), date(2025,11,20), date(2025,12,25),
    date(2026,1,1), date(2026,2,16), date(2026,2,17), date(2026,4,3),
    date(2026,4,21), date(2026,5,1), date(2026,6,4), date(2026,9,7),
    date(2026,10,12), date(2026,11,2), date(2026,11,15), date(2026,11,20), date(2026,12,25),
    date(2027,1,1), date(2027,2,8), date(2027,2,9), date(2027,3,26),
    date(2027,4,21), date(2027,5,1), date(2027,5,27), date(2027,9,7),
    date(2027,10,12), date(2027,11,2), date(2027,11,15), date(2027,11,20), date(2027,12,25),
}

LSE_HOLIDAYS = {
    date(2025,1,1), date(2025,4,18), date(2025,4,21), date(2025,5,5),
    date(2025,5,26), date(2025,8,25), date(2025,12,25), date(2025,12,26),
    date(2026,1,1), date(2026,4,3), date(2026,4,6), date(2026,5,4),
    date(2026,5,25), date(2026,8,31), date(2026,12,25), date(2026,12,28),
    date(2027,1,1), date(2027,3,26), date(2027,3,29), date(2027,5,3),
    date(2027,5,31), date(2027,8,30), date(2027,12,27), date(2027,12,28),
}

HKEX_HOLIDAYS = {
    date(2025,1,1), date(2025,1,29), date(2025,1,30), date(2025,1,31),
    date(2025,4,4), date(2025,4,18), date(2025,4,21), date(2025,5,1),
    date(2025,5,5), date(2025,6,2), date(2025,7,1), date(2025,10,1),
    date(2025,10,2), date(2025,10,7), date(2025,12,25), date(2025,12,26),
    date(2026,1,1), date(2026,2,17), date(2026,2,18), date(2026,2,19),
    date(2026,4,3), date(2026,4,6), date(2026,4,7), date(2026,5,1),
    date(2026,5,25), date(2026,6,19), date(2026,7,1), date(2026,10,1),
    date(2026,10,2), date(2026,12,25),
}

# ═══════════════════════════════════════════════════════════════
# TIMEZONES
# ═══════════════════════════════════════════════════════════════
TZ_SAO_PAULO = ZoneInfo('America/Sao_Paulo')
TZ_NEW_YORK  = ZoneInfo('America/New_York')
TZ_LONDON    = ZoneInfo('Europe/London')
TZ_HK        = ZoneInfo('Asia/Hong_Kong')

# ═══════════════════════════════════════════════════════════════
# MARKET STATUS FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def is_b3_open() -> bool:
    """Check if B3 (São Paulo) stock exchange is open."""
    now = datetime.now(TZ_SAO_PAULO)
    if now.weekday() >= 5 or now.date() in B3_HOLIDAYS:
        return False
    h = now.hour + now.minute / 60.0
    return 10.0 <= h < 17.0

def is_nyse_open() -> bool:
    """Check if NYSE/NASDAQ (New York) stock exchange is open."""
    now = datetime.now(TZ_NEW_YORK)
    if now.weekday() >= 5 or now.date() in NYSE_HOLIDAYS:
        return False
    h = now.hour + now.minute / 60.0
    return 9.5 <= h < 16.0

def is_lse_open() -> bool:
    """Check if LSE (London) stock exchange is open."""
    now = datetime.now(TZ_LONDON)
    if now.weekday() >= 5 or now.date() in LSE_HOLIDAYS:
        return False
    h = now.hour + now.minute / 60.0
    return 8.0 <= h < 16.5

def is_hkex_open() -> bool:
    """Check if HKEX (Hong Kong) stock exchange is open."""
    now = datetime.now(TZ_HK)
    if now.weekday() >= 5 or now.date() in HKEX_HOLIDAYS:
        return False
    h = now.hour + now.minute / 60.0
    return (9.5 <= h < 12.0) or (13.0 <= h < 16.0)

def is_tsx_open() -> bool:
    """Check if TSX (Toronto) stock exchange is open.
    TSX abre 09:30-16:00 ET = mesmo horário da NYSE
    """
    now = datetime.now(TZ_NEW_YORK)
    if now.weekday() >= 5:
        return False
    h = now.hour + now.minute / 60.0
    return 9.5 <= h < 16.0

def is_euronext_open() -> bool:
    """Check if Euronext/XETRA (continental Europe) exchange is open.
    Euronext/XETRA 09:00-17:30 CET = 08:00-16:30 UTC (aprox)
    """
    try:
        TZ_CET = ZoneInfo('Europe/Paris')
    except Exception:
        TZ_CET = TZ_LONDON
    now = datetime.now(TZ_CET)
    if now.weekday() >= 5:
        return False
    h = now.hour + now.minute / 60.0
    return 9.0 <= h < 17.5

def market_open_for(mkt: str) -> bool:
    """Check if a specific market is open.

    Args:
        mkt: Market type (CRYPTO, B3, NYSE, NASDAQ, US, LSE, HKEX, TSX, EURONEXT, XETRA, XAMS)

    Returns:
        True if market is open, False otherwise
    """
    if mkt == 'CRYPTO':
        return True
    if mkt == 'B3':
        return is_b3_open()
    if mkt in ('NYSE', 'NASDAQ', 'US'):
        return is_nyse_open()
    if mkt == 'LSE':
        return is_lse_open()
    if mkt == 'HKEX':
        return is_hkex_open()
    if mkt == 'TSX':
        return is_tsx_open()
    if mkt in ('EURONEXT', 'XETRA', 'XAMS'):
        return is_euronext_open()
    return False
