"""
Futures Adapter — B3 & international futures helpers.

Utilities for WIN/WDO/IND/DOL ticker parsing, DI1 PU <-> rate conversion,
DV01 calculation, carry-cost pricing, and canonical multiplier/tick lookups.

This module does NOT fetch market data; it transforms/normalises values
coming from the ProviderManager for use inside strategy scan loops.

All monetary values in BRL unless otherwise stated.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, Dict, Tuple, Any
import math
import re


# ─────────────────────────────────────────────────────────────────────────
# B3 Futures Calendar — month codes
# ─────────────────────────────────────────────────────────────────────────
# B3 uses the same letter convention as CME:
#   F=Jan G=Feb H=Mar J=Apr K=May M=Jun N=Jul Q=Aug U=Sep V=Oct X=Nov Z=Dec
B3_MONTH_CODE_TO_NUM: Dict[str, int] = {
    'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
    'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12,
}
B3_MONTH_NUM_TO_CODE: Dict[int, str] = {v: k for k, v in B3_MONTH_CODE_TO_NUM.items()}


# ─────────────────────────────────────────────────────────────────────────
# Contract specs (multiplier = BRL per point; tick_size in points)
# ─────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ContractSpec:
    root: str
    name: str
    multiplier: float   # BRL per point (or per rate-point for DI)
    tick_size: float
    instrument_type: str  # 'future', 'spot', 'swap'

CONTRACT_SPECS: Dict[str, ContractSpec] = {
    # Índice Bovespa futures
    'WIN': ContractSpec('WIN', 'Mini Ibovespa Futuro', 0.20, 5.0,  'future'),
    'IND': ContractSpec('IND', 'Ibovespa Cheio Futuro', 1.00, 5.0,  'future'),
    # Dólar futures
    'WDO': ContractSpec('WDO', 'Mini Dólar Futuro',      10.0, 0.5,  'future'),
    'DOL': ContractSpec('DOL', 'Dólar Cheio Futuro',     50.0, 0.5,  'future'),
    # Juros — DI1 uses different convention (see di1_pu_from_rate)
    'DI1': ContractSpec('DI1', 'Taxa DI1 Futuro',       100_000.0, 0.001, 'future'),
    # Commodities
    'BGI': ContractSpec('BGI', 'Boi Gordo Futuro',       330.0, 0.05, 'future'),
    'CCM': ContractSpec('CCM', 'Milho Futuro',           450.0, 0.01, 'future'),
    'ICF': ContractSpec('ICF', 'Café Arábica Futuro',    100.0, 0.05, 'future'),
    'SJC': ContractSpec('SJC', 'Soja CME Financeiro',     27.0, 0.05, 'future'),
    # International (reference only)
    'ES':  ContractSpec('ES',  'E-mini S&P 500',          50.0, 0.25, 'future'),
    'NQ':  ContractSpec('NQ',  'E-mini Nasdaq-100',       20.0, 0.25, 'future'),
}


def get_spec(root: str) -> Optional[ContractSpec]:
    return CONTRACT_SPECS.get((root or '').upper())


# ─────────────────────────────────────────────────────────────────────────
# Ticker parsing — e.g. WINZ26, DOLJ26, DI1F27, INDG27
# ─────────────────────────────────────────────────────────────────────────
_TICKER_RE = re.compile(r'^([A-Z]{2,3})([FGHJKMNQUVXZ])(\d{1,2})$', re.IGNORECASE)


@dataclass(frozen=True)
class ParsedTicker:
    root: str
    month_code: str
    month: int
    year: int           # full 4-digit year
    ticker: str         # original
    @property
    def expiry_ym(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def parse_future_ticker(ticker: str, pivot_year: Optional[int] = None) -> Optional[ParsedTicker]:
    """Parse B3 future ticker like 'WINZ26' -> (root=WIN, month=12, year=2026)."""
    if not ticker:
        return None
    m = _TICKER_RE.match(ticker.strip().upper())
    if not m:
        return None
    root, code, yr = m.group(1), m.group(2).upper(), m.group(3)
    mon = B3_MONTH_CODE_TO_NUM.get(code)
    if not mon:
        return None
    # 2-digit year heuristic: years <= pivot+10 are 20xx, else 19xx
    y = int(yr)
    if len(yr) == 2:
        pivot = pivot_year or (datetime.now().year % 100)
        # default sliding window: years 00..pivot+10 = 2000s, else 1900s
        century = 2000 if y <= (pivot + 10) else 1900
        y = century + y
    return ParsedTicker(root=root, month_code=code, month=mon, year=y, ticker=ticker.upper())


def build_future_ticker(root: str, year: int, month: int) -> str:
    """Build ticker like WIN + 2026 + 12 -> 'WINZ26'."""
    code = B3_MONTH_NUM_TO_CODE.get(int(month))
    if not code:
        raise ValueError(f"invalid month: {month}")
    return f"{root.upper()}{code}{int(year) % 100:02d}"


# ─────────────────────────────────────────────────────────────────────────
# Business-day (DU) helpers
# ─────────────────────────────────────────────────────────────────────────
def business_days_between(d1: date, d2: date) -> int:
    """Rough DU count — excludes Sat/Sun. Does not subtract B3 holidays.
    For DI1 pricing, swap to exchange calendar in production."""
    if d2 < d1:
        d1, d2 = d2, d1
    days = (d2 - d1).days
    if days == 0:
        return 0
    # count weekends precisely
    import datetime as _d
    cnt = 0
    cur = d1
    step = _d.timedelta(days=1)
    while cur < d2:
        if cur.weekday() < 5:
            cnt += 1
        cur += step
    return cnt


def du_until(expiry: date | datetime, today: Optional[date] = None) -> int:
    """Business days from today to expiry."""
    if isinstance(expiry, datetime):
        expiry = expiry.date()
    today = today or date.today()
    return business_days_between(today, expiry)


# ─────────────────────────────────────────────────────────────────────────
# DI1 — PU (unit price) ↔ annual rate (252-day convention)
# ─────────────────────────────────────────────────────────────────────────
DI1_FACE_VALUE = 100_000.0   # R$

def di1_pu_from_rate(rate_annual: float, du: int) -> float:
    """Return DI1 PU given an annual rate (decimal or %) and business days to expiry.

    Convention: PU = 100000 / (1+r)^(DU/252)   where r is annual rate in decimal.
    """
    if du < 0:
        return 0.0
    r = rate_annual / 100.0 if rate_annual > 1 else float(rate_annual)
    return DI1_FACE_VALUE / ((1.0 + r) ** (du / 252.0))


def di1_rate_from_pu(pu: float, du: int) -> float:
    """Invert DI1 PU -> annual rate (decimal)."""
    if pu <= 0 or du <= 0:
        return 0.0
    return (DI1_FACE_VALUE / pu) ** (252.0 / du) - 1.0


def di1_dv01(pu: float, du: int, notional_contracts: int = 1) -> float:
    """Sensitivity of PU to a 1bp change in annual rate.

    ∂PU/∂r = -DU/252 * PU / (1+r) ; DV01 ≈ |∂PU/∂r| * 0.0001  (per contract).
    """
    if pu <= 0 or du <= 0:
        return 0.0
    r = di1_rate_from_pu(pu, du)
    dpu_dr = -(du / 252.0) * pu / (1.0 + r)
    return abs(dpu_dr) * 0.0001 * notional_contracts


# ─────────────────────────────────────────────────────────────────────────
# Basis / carry — spot-futures fair value (equity index)
# ─────────────────────────────────────────────────────────────────────────
def fair_future_price(
    spot: float,
    cdi_annual: float,
    days_to_expiry_calendar: int,
    dividend_yield_annual: float = 0.0,
    day_count: int = 252,
) -> float:
    """Cost-of-carry fair price for equity index future.

    F* = S * (1 + (r - q))^(t/day_count)    (continuous-to-discrete approximation).
    For B3 equity futures we use 252-day convention with DU but calendar-day input
    is accepted (caller should pass du when known).
    """
    if spot <= 0 or days_to_expiry_calendar <= 0:
        return spot
    r = cdi_annual / 100.0 if cdi_annual > 1 else float(cdi_annual)
    q = dividend_yield_annual / 100.0 if dividend_yield_annual > 1 else float(dividend_yield_annual)
    return spot * ((1.0 + (r - q)) ** (days_to_expiry_calendar / day_count))


def basis_bps(market_future: float, fair_future: float) -> float:
    """Deviation of market vs. fair in basis points of fair."""
    if fair_future <= 0:
        return 0.0
    return (market_future - fair_future) / fair_future * 10_000.0


# ─────────────────────────────────────────────────────────────────────────
# FX hedge sizing — for INTERLISTED_HEDGED
# ─────────────────────────────────────────────────────────────────────────
def wdo_hedge_contracts(usd_notional: float, usdbrl_spot: float = 0.0) -> int:
    """Number of WDO contracts to hedge a USD notional exposure.

    WDO multiplier = USD 10 per contract. 1 contract = USD 10,000 notional.
    Returns integer rounded to nearest (min 1 if notional>0).
    """
    if usd_notional <= 0:
        return 0
    wdo_unit_usd = 10_000.0   # notional per WDO contract
    qty = usd_notional / wdo_unit_usd
    return max(1, int(round(qty)))


def dol_hedge_contracts(usd_notional: float) -> int:
    """Number of DOL (cheio) contracts. 1 DOL = USD 50,000 notional."""
    if usd_notional <= 0:
        return 0
    return max(1, int(round(usd_notional / 50_000.0)))


# ─────────────────────────────────────────────────────────────────────────
# Dividend yield — lookup by symbol (fallback table)
# ─────────────────────────────────────────────────────────────────────────
# Conservative trailing-12m yields as of April 2026 for Ibovespa index proxies.
# Overridden if dividend_service provides forward dividends.
DEFAULT_DIV_YIELD_ANNUAL: Dict[str, float] = {
    'IBOV':   0.060,   # ~6%
    'BOVA11': 0.060,
    'SMAL11': 0.045,
    'IVVB11': 0.015,
    'PETR4':  0.090,
    'VALE3':  0.080,
    'ITUB4':  0.055,
    'BBDC4':  0.055,
    'BBAS3':  0.080,
    'ABEV3':  0.030,
    'B3SA3':  0.025,
}

def default_div_yield(symbol: str) -> float:
    return DEFAULT_DIV_YIELD_ANNUAL.get((symbol or '').upper(), 0.0)


# ─────────────────────────────────────────────────────────────────────────
# WIN/IND notional sizing
# ─────────────────────────────────────────────────────────────────────────
def win_notional_per_contract(ibov_points: float) -> float:
    """1 WIN = 0.20 R$ per point. Notional = 0.20 * index_points."""
    return 0.20 * max(0.0, ibov_points)


def ind_notional_per_contract(ibov_points: float) -> float:
    """1 IND = 1.00 R$ per point."""
    return 1.00 * max(0.0, ibov_points)


def contracts_for_notional(target_notional_brl: float, future_price: float, root: str) -> int:
    """Translate a target BRL notional into a contract count for WIN/IND/WDO/DOL."""
    spec = get_spec(root)
    if not spec or future_price <= 0:
        return 0
    per_contract = spec.multiplier * future_price
    if per_contract <= 0:
        return 0
    qty = target_notional_brl / per_contract
    return max(1, int(round(qty)))


# ─────────────────────────────────────────────────────────────────────────
# Audit helpers
# ─────────────────────────────────────────────────────────────────────────
def fair_value_snapshot(
    *,
    spot: float,
    cdi: float,
    dy: float,
    du: int,
    market_price: float,
) -> Dict[str, Any]:
    """Snapshot of fair-value inputs for persistence in strategy_master_trades.fair_value_inputs."""
    fair = fair_future_price(spot, cdi, du, dy)
    return {
        'spot': round(spot, 4),
        'cdi_annual_pct': round(cdi if cdi > 1 else cdi * 100, 4),
        'div_yield_annual_pct': round(dy if dy > 1 else dy * 100, 4),
        'days_to_expiry': int(du),
        'fair_future': round(fair, 4),
        'market_future': round(market_price, 4),
        'deviation_bps': round(basis_bps(market_price, fair), 2),
        'generated_at': datetime.utcnow().isoformat(),
    }
