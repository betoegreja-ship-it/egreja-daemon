"""
Market Data Provider Module

Abstraction layer for market data from various sources (Cedro, simulated, etc).
Thread-safe with health checks, fallback support, and provider lifecycle management.
"""

import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import os
import random

import requests as _requests_lib

logger = logging.getLogger('egreja.derivatives')


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class SpotQuote:
    """Spot price quote for an underlying asset."""
    symbol: str
    bid: float
    ask: float
    last: float
    volume: int
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0
    
    @property
    def spread_bps(self) -> float:
        if self.mid == 0:
            return 0.0
        return ((self.ask - self.bid) / self.mid) * 10_000


@dataclass
class OptionQuote:
    """Options contract quote."""
    symbol: str
    underlying: str
    strike: float
    expiry: str  # ISO format date
    option_type: str  # "C" or "P"
    bid: float
    ask: float
    last: float
    volume: int
    oi: int
    iv: float
    delta: float
    gamma: float
    theta: float
    vega: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0
    
    @property
    def spread_bps(self) -> float:
        if self.mid == 0:
            return 0.0
        return ((self.ask - self.bid) / self.mid) * 10_000
    
    @property
    def moneyness(self) -> float:
        """Return moneyness (strike / underlying)."""
        # This will be calculated from current spot in real use
        return 1.0


@dataclass
class FutureQuote:
    """Futures contract quote."""
    symbol: str
    underlying: str
    expiry: str  # ISO format date
    bid: float
    ask: float
    last: float
    volume: int
    oi: int
    basis: float  # futures - spot
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0
    
    @property
    def spread_bps(self) -> float:
        if self.mid == 0:
            return 0.0
        return ((self.ask - self.bid) / self.mid) * 10_000


@dataclass
class DividendEvent:
    """Dividend event details."""
    symbol: str
    ex_date: str  # ISO format date
    amount: float
    div_type: str  # "cash", "special", "stock"


@dataclass
class RateCurve:
    """Interest rate curve snapshot."""
    date: str  # ISO format date
    cdi: float  # CDI rate (annual %)
    selic: float  # SELIC rate (annual %)
    di1_terms: Dict[int, float] = field(default_factory=dict)  # days -> rate
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ============================================================================
# Provider Base Classes (Abstract Base Classes)
# ============================================================================

class MarketDataProviderBase(ABC):
    """Base class for all market data providers."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f'egreja.derivatives.{name}')
        self._lock = threading.RLock()
        self._last_health_check = datetime.utcnow()
        self._is_healthy = False
    
    @abstractmethod
    def get_spot(self, symbol: str) -> Optional[SpotQuote]:
        """Get spot price quote."""
        pass
    
    @abstractmethod
    def get_options_chain(self, underlying: str) -> List[OptionQuote]:
        """Get entire options chain for an underlying."""
        pass
    
    @abstractmethod
    def get_futures(self, underlying: str) -> List[FutureQuote]:
        """Get futures contracts for an underlying."""
        pass
    
    @abstractmethod
    def get_rates(self) -> Optional[RateCurve]:
        """Get interest rate curve."""
        pass
    
    @abstractmethod
    def get_dividends(self, symbol: str) -> List[DividendEvent]:
        """Get dividend events."""
        pass
    
    @abstractmethod
    def get_depth(self, symbol: str, max_levels: int = 5) -> Optional[Dict[str, Any]]:
        """Get order book depth (bid/ask levels)."""
        pass
    
    @abstractmethod
    def get_greeks(self, option_symbol: str) -> Optional[Dict[str, float]]:
        """Get option Greeks (delta, gamma, theta, vega)."""
        pass
    
    @abstractmethod
    def get_adr_prices(self, symbols: List[str]) -> Dict[str, float]:
        """Get ADR prices for symbols (if available)."""
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """Perform health check. Return True if healthy."""
        pass


class SpotProvider(ABC):
    """Interface for spot price data."""
    @abstractmethod
    def get_spot(self, symbol: str) -> Optional[SpotQuote]:
        pass


class OptionsChainProvider(ABC):
    """Interface for options chain data."""
    @abstractmethod
    def get_options_chain(self, underlying: str) -> List[OptionQuote]:
        pass


class FuturesProvider(ABC):
    """Interface for futures data."""
    @abstractmethod
    def get_futures(self, underlying: str) -> List[FutureQuote]:
        pass


class RatesProvider(ABC):
    """Interface for interest rate data."""
    @abstractmethod
    def get_rates(self) -> Optional[RateCurve]:
        pass


class DividendProvider(ABC):
    """Interface for dividend data."""
    @abstractmethod
    def get_dividends(self, symbol: str) -> List[DividendEvent]:
        pass


class ADRProvider(ABC):
    """Interface for ADR price data."""
    @abstractmethod
    def get_adr_prices(self, symbols: List[str]) -> Dict[str, float]:
        pass


# ============================================================================
# Cedro Market Data Provider (Stub Implementation)
# ============================================================================

class CedroMarketDataProvider(MarketDataProviderBase):
    """
    Cedro Technologies Market Data Provider (WebFeeder REST API).

    Base URL: http://webfeeder.cedrofinances.com.br
    Auth: POST /SignIn?login=XXX&password=XXX (session cookies)
    Quote: GET /services/quotes/quote/{symbol}  (JSON)

    Configured via environment variables:
    - CEDRO_LOGIN:   WebFeeder username
    - CEDRO_PASSWORD: WebFeeder password
    - CEDRO_API_URL: Base URL (default: http://webfeeder.cedrofinances.com.br)

    Free 7-day trial: https://www.marketdatacloud.com.br
    Contact: comercial@cedrotech.com | WhatsApp +55 34 3239-0003
    """

    # B3 option series: calls A-L (Jan-Dec), puts M-X (Jan-Dec)
    CALL_MONTHS = 'ABCDEFGHIJKL'
    PUT_MONTHS  = 'MNOPQRSTUVWX'

    def __init__(self):
        import os
        import requests as _req
        super().__init__("cedro")
        self._requests = _req
        self._session = _req.Session()
        self._session.headers.update({'Accept': 'application/json'})

        # Accept CEDRO_USER as alias for CEDRO_LOGIN (socket creds use CEDRO_USER)
        self.login    = (os.environ.get('CEDRO_LOGIN', '') or os.environ.get('CEDRO_USER', '')).strip()
        self.password = os.environ.get('CEDRO_PASSWORD', '').strip()
        self.api_url  = os.environ.get('CEDRO_API_URL',
                                       'http://webfeeder.cedrofinances.com.br').rstrip('/')

        self._authenticated = False
        self._auth_ts = None
        self._cache: Dict[str, Any] = {}
        self._cache_ttl = 5  # seconds

        # Wire to live Cedro Crystal socket if available — gives real-time spot/quote data
        # without relying on WebFeeder REST (which uses a different auth scope).
        self._socket = None
        try:
            from modules.cedro_socket_provider import get_cedro as _get_cedro
            self._socket = _get_cedro()
            if self._socket is not None:
                self._is_healthy = True
                self.logger.info("Cedro: bound to live Crystal socket — provider healthy via socket")
        except Exception as _e:
            self.logger.warning(f"Cedro: socket binding failed — {_e}")

        if self.login and self.password:
            self._authenticate()
        elif self._socket is None:
            self.logger.warning("Cedro: CEDRO_LOGIN / CEDRO_PASSWORD not set and socket unavailable — provider disabled")

    # ─── Authentication ───────────────────────────────────────────

    def _authenticate(self) -> bool:
        """Authenticate with Cedro WebFeeder (session cookies)."""
        try:
            url = f"{self.api_url}/SignIn"
            r = self._session.post(url, params={
                'login': self.login,
                'password': self.password,
            }, timeout=10)

            if r.status_code == 200 and 'error' not in r.text.lower():
                self._authenticated = True
                self._is_healthy = True
                self._auth_ts = datetime.utcnow()
                self.logger.info(f"Cedro: authenticated as '{self.login}'")
                return True
            else:
                self._authenticated = False
                self._is_healthy = False
                self.logger.error(f"Cedro: auth failed — HTTP {r.status_code}: {r.text[:200]}")
                return False
        except Exception as e:
            self._authenticated = False
            self._is_healthy = False
            self.logger.error(f"Cedro: auth exception — {e}")
            return False

    def _ensure_auth(self) -> bool:
        """Re-authenticate if session expired (every 30 min)."""
        if not self.login or not self.password:
            return False
        if not self._authenticated or not self._auth_ts:
            return self._authenticate()
        if (datetime.utcnow() - self._auth_ts).total_seconds() > 1800:
            return self._authenticate()
        return True

    # ─── HTTP Helpers ─────────────────────────────────────────────

    def _get(self, path: str, timeout: int = 8) -> Optional[Any]:
        """GET request with session cookies. Returns parsed JSON or None."""
        if not self._ensure_auth():
            return None
        try:
            url = f"{self.api_url}{path}"
            r = self._session.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            self.logger.warning(f"Cedro GET {path} → HTTP {r.status_code}")
            return None
        except Exception as e:
            self.logger.warning(f"Cedro GET {path} error: {e}")
            return None

    def _cached_get(self, cache_key: str, path: str) -> Optional[Any]:
        """GET with in-memory TTL cache."""
        now = time.time()
        cached = self._cache.get(cache_key)
        if cached and (now - cached['ts']) < self._cache_ttl:
            return cached['data']
        data = self._get(path)
        if data is not None:
            self._cache[cache_key] = {'data': data, 'ts': now}
        return data

    # ─── Spot Quote ───────────────────────────────────────────────

    def get_spot(self, symbol: str) -> Optional[SpotQuote]:
        """Fetch spot price — prefer live Crystal socket, fallback to WebFeeder REST."""
        # ─── Socket-first path (real-time, no auth latency) ─────────
        if self._socket is not None:
            try:
                self._socket.subscribe([symbol])
                q = self._socket.get_quote(symbol, wait_ms=300)
                if q and (q.get('price') or q.get('last')):
                    return SpotQuote(
                        symbol=symbol.upper(),
                        bid=float(q.get('best_bid') or q.get('bid') or 0),
                        ask=float(q.get('best_ask') or q.get('ask') or 0),
                        last=float(q.get('price') or q.get('last') or 0),
                        volume=int(q.get('volume') or 0),
                        timestamp=datetime.utcnow(),
                    )
            except Exception as _e:
                self.logger.debug(f"Cedro socket get_spot({symbol}) failed: {_e}")

        # ─── REST fallback ──────────────────────────────────────────
        with self._lock:
            data = self._cached_get(f'spot:{symbol}',
                                    f'/services/quotes/quote/{symbol}')
            if not data:
                return None
            try:
                return SpotQuote(
                    symbol=symbol.upper(),
                    bid=float(data.get('buyPrice', data.get('bidPrice', 0)) or 0),
                    ask=float(data.get('sellPrice', data.get('askPrice', 0)) or 0),
                    last=float(data.get('lastTradePrice', data.get('last', 0)) or 0),
                    volume=int(data.get('tradeVolume', data.get('volume', 0)) or 0),
                    timestamp=datetime.utcnow(),
                )
            except (ValueError, TypeError) as e:
                self.logger.warning(f"Cedro spot parse error for {symbol}: {e}")
                return None

    # ─── Options Chain ────────────────────────────────────────────

    def _guess_option_symbols(self, underlying: str) -> List[str]:
        """
        Generate candidate B3 option ticker symbols for an underlying.

        B3 naming: {ROOT}{MONTH_LETTER}{STRIKE_CODE}
        e.g. PETR4 calls Jan → PETRA..., puts Jan → PETRM...
        For simplicity, we query Cedro for the known series letters.
        """
        root = underlying[:4]
        symbols = []
        now = datetime.utcnow()

        # Check current month + next 3 months
        for month_offset in range(4):
            m = (now.month - 1 + month_offset) % 12
            call_letter = self.CALL_MONTHS[m]
            put_letter  = self.PUT_MONTHS[m]
            # Cedro can resolve these by querying the parent asset
            symbols.append(f"{root}{call_letter}")
            symbols.append(f"{root}{put_letter}")

        return symbols

    def get_options_chain(self, underlying: str) -> List[OptionQuote]:
        """
        Fetch options chain via Cedro Crystal socket (real-time) with Greeks
        computed locally from Black-Scholes. Falls back to WebFeeder REST only
        if socket is unavailable.
        """
        # ─── Socket-first path ──────────────────────────────────────
        if self._socket is not None:
            try:
                raw = self._socket.get_options_chain(underlying, max_wait_ms=3000)
                if raw:
                    # Get spot for greeks
                    sp = self.get_spot(underlying)
                    spot_px = sp.last if sp else 0.0
                    rate = float(os.environ.get('DERIV_RISK_FREE_RATE', '0.105'))  # CDI ~10.5%
                    results: List[OptionQuote] = []
                    for q in raw:
                        try:
                            opt = self._build_option_quote_with_greeks(
                                q, underlying=underlying, spot=spot_px, rate=rate
                            )
                            if opt:
                                results.append(opt)
                        except Exception as _op_err:
                            self.logger.debug(f"Cedro socket option build failed: {_op_err}")
                    if results:
                        return results
            except Exception as _sc_err:
                self.logger.debug(f"Cedro socket get_options_chain failed: {_sc_err}")

        # ─── REST fallback (unlikely to work without WebFeeder auth) ─
        with self._lock:
            chain_data = self._cached_get(
                f'chain:{underlying}',
                f'/services/quotes/options/{underlying}'
            )

            if chain_data and isinstance(chain_data, list):
                return self._parse_options_list(underlying, chain_data)

            results = []
            for series_prefix in self._guess_option_symbols(underlying):
                data = self._get(f'/services/quotes/quote/{series_prefix}')
                if data and isinstance(data, dict):
                    opt = self._parse_single_option(underlying, data)
                    if opt:
                        results.append(opt)
                elif data and isinstance(data, list):
                    for item in data:
                        opt = self._parse_single_option(underlying, item)
                        if opt:
                            results.append(opt)

            return results

    # ─── Black-Scholes helpers ────────────────────────────────────

    def _build_option_quote_with_greeks(
        self, q: dict, underlying: str, spot: float, rate: float
    ) -> Optional[OptionQuote]:
        """Build OptionQuote from socket raw dict + compute IV/greeks inline."""
        from math import log, sqrt, exp
        from datetime import date as _date

        symbol = q.get('symbol') or ''
        strike = float(q.get('strike') or 0.0)
        bid = float(q.get('bid') or 0.0)
        ask = float(q.get('ask') or 0.0)
        last = float(q.get('last') or 0.0)
        opt_type = q.get('option_type') or 'C'
        expiry = q.get('expiry') or ''

        if strike <= 0 or spot <= 0:
            return None

        # Days to expiry
        T = 30.0 / 365.0  # default ~1 month if no expiry data
        if expiry:
            try:
                exp_d = datetime.strptime(expiry[:10], '%Y-%m-%d').date()
                dte_days = (exp_d - _date.today()).days
                if dte_days <= 0:
                    return None  # skip expired
                T = dte_days / 365.0
            except Exception:
                pass

        # Mid price for IV extraction
        mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else last
        iv = 0.30  # default 30% if we can't solve
        if mid > 0:
            iv_solved = self._implied_vol_newton(
                price=mid, S=spot, K=strike, T=T, r=rate, opt_type=opt_type
            )
            if iv_solved is not None and 0.01 < iv_solved < 5.0:
                iv = iv_solved

        # Greeks from Black-Scholes
        delta, gamma, theta, vega = self._bs_greeks(
            S=spot, K=strike, T=T, r=rate, vol=iv, opt_type=opt_type
        )

        return OptionQuote(
            symbol=symbol,
            underlying=underlying.upper(),
            strike=strike,
            expiry=expiry,
            option_type=opt_type,
            bid=bid, ask=ask, last=last,
            volume=int(q.get('volume') or 0),
            oi=int(q.get('oi') or 0),
            iv=float(iv),
            delta=float(delta),
            gamma=float(gamma),
            theta=float(theta),
            vega=float(vega),
            timestamp=datetime.utcnow(),
        )

    @staticmethod
    def _norm_cdf(x: float) -> float:
        from math import erf, sqrt
        return 0.5 * (1.0 + erf(x / sqrt(2.0)))

    @staticmethod
    def _norm_pdf(x: float) -> float:
        from math import exp, sqrt, pi
        return exp(-0.5 * x * x) / sqrt(2.0 * pi)

    def _bs_price(self, S, K, T, r, vol, opt_type):
        from math import log, sqrt, exp
        if vol <= 0 or T <= 0:
            return max(0.0, (S - K) if opt_type == 'C' else (K - S))
        d1 = (log(S / K) + (r + 0.5 * vol * vol) * T) / (vol * sqrt(T))
        d2 = d1 - vol * sqrt(T)
        if opt_type == 'C':
            return S * self._norm_cdf(d1) - K * exp(-r * T) * self._norm_cdf(d2)
        else:
            return K * exp(-r * T) * self._norm_cdf(-d2) - S * self._norm_cdf(-d1)

    def _bs_greeks(self, S, K, T, r, vol, opt_type):
        from math import log, sqrt, exp
        if vol <= 0 or T <= 0 or S <= 0 or K <= 0:
            return (0.0, 0.0, 0.0, 0.0)
        d1 = (log(S / K) + (r + 0.5 * vol * vol) * T) / (vol * sqrt(T))
        d2 = d1 - vol * sqrt(T)
        pdf_d1 = self._norm_pdf(d1)
        if opt_type == 'C':
            delta = self._norm_cdf(d1)
            theta = (-(S * pdf_d1 * vol) / (2 * sqrt(T))
                     - r * K * exp(-r * T) * self._norm_cdf(d2)) / 365.0
        else:
            delta = self._norm_cdf(d1) - 1.0
            theta = (-(S * pdf_d1 * vol) / (2 * sqrt(T))
                     + r * K * exp(-r * T) * self._norm_cdf(-d2)) / 365.0
        gamma = pdf_d1 / (S * vol * sqrt(T))
        vega  = S * pdf_d1 * sqrt(T) / 100.0  # per 1% vol move
        return (delta, gamma, theta, vega)

    def _implied_vol_newton(self, price, S, K, T, r, opt_type,
                            max_iter=50, tol=1e-4) -> Optional[float]:
        """Newton-Raphson IV solver from option price + spot + strike + T + r."""
        from math import sqrt
        if price <= 0 or S <= 0 or K <= 0 or T <= 0:
            return None
        # Initial guess: Brenner-Subrahmanyam approximation
        try:
            vol = sqrt(2.0 * 3.14159265 / T) * (price / S)
            vol = max(0.05, min(vol, 3.0))
        except Exception:
            vol = 0.30
        for _ in range(max_iter):
            try:
                p = self._bs_price(S, K, T, r, vol, opt_type)
                diff = p - price
                if abs(diff) < tol:
                    return vol
                # Vega for Newton step (note: our vega is per 1% vol, so ×100 here)
                _, _, _, vega = self._bs_greeks(S, K, T, r, vol, opt_type)
                vega_abs = vega * 100.0
                if vega_abs < 1e-8:
                    return None
                vol = vol - diff / vega_abs
                if vol <= 0:
                    vol = 0.01
                if vol > 5.0:
                    vol = 5.0
            except Exception:
                return None
        return vol

    def _parse_options_list(self, underlying: str, data_list: list) -> List[OptionQuote]:
        """Parse a list of option quotes from Cedro."""
        results = []
        for item in data_list:
            opt = self._parse_single_option(underlying, item)
            if opt:
                results.append(opt)
        return results

    def _parse_single_option(self, underlying: str, data: dict) -> Optional[OptionQuote]:
        """Parse a single option quote from Cedro JSON response."""
        try:
            symbol = str(data.get('symbol', data.get('ticker', '')))
            if not symbol:
                return None

            # Determine option type from B3 series letter
            root_len = 4  # PETR, VALE, etc.
            if len(symbol) > root_len:
                series_letter = symbol[root_len].upper()
                if series_letter in self.CALL_MONTHS:
                    opt_type = 'C'
                elif series_letter in self.PUT_MONTHS:
                    opt_type = 'P'
                else:
                    opt_type = data.get('optionType', 'C')
            else:
                opt_type = data.get('optionType', 'C')

            strike = float(data.get('strikePrice', data.get('strike', 0)) or 0)
            expiry = str(data.get('expirationDate', data.get('expiry', '')))

            return OptionQuote(
                symbol=symbol,
                underlying=underlying.upper(),
                strike=strike,
                expiry=expiry,
                option_type=opt_type,
                bid=float(data.get('buyPrice', data.get('bidPrice', 0)) or 0),
                ask=float(data.get('sellPrice', data.get('askPrice', 0)) or 0),
                last=float(data.get('lastTradePrice', data.get('last', 0)) or 0),
                volume=int(data.get('tradeVolume', data.get('volume', 0)) or 0),
                oi=int(data.get('openInterest', data.get('oi', 0)) or 0),
                iv=float(data.get('impliedVolatility', data.get('iv', 0)) or 0),
                delta=float(data.get('delta', 0) or 0),
                gamma=float(data.get('gamma', 0) or 0),
                theta=float(data.get('theta', 0) or 0),
                vega=float(data.get('vega', 0) or 0),
                timestamp=datetime.utcnow(),
            )
        except (ValueError, TypeError, KeyError) as e:
            self.logger.debug(f"Cedro option parse error: {e}")
            return None

    # ─── Futures ──────────────────────────────────────────────────

    def get_futures(self, underlying: str) -> List[FutureQuote]:
        """[v10.42] Fetch futures from Cedro. Socket-first with REST fallback."""
        # [v10.42] Socket-first path
        results = []
        if self._socket and getattr(self._socket, 'enabled', False):
            futures_map = {
                'BOVA11': ['WINFUT', 'INDFUT'],
                'IBOV':   ['WINFUT', 'INDFUT'],
                'WIN':    ['WINFUT'],
                'IND':    ['INDFUT'],
                'PETR4':  ['PETR4F'],
                'VALE3':  ['VALE3F'],
                'USD':    ['DOLFUT', 'WDOFUT'],
                'BRL':    ['DOLFUT', 'WDOFUT'],
                'DI1':    ['DI1F26', 'DI1F27', 'DI1F28', 'DI1F29', 'DI1F30'],
            }
            tickers = futures_map.get(underlying.upper(), [f'{underlying[:4]}FUT'])
            for ticker in tickers:
                try:
                    q = self._socket.get_quote(ticker, wait_ms=0)
                    if q and q.get('price'):
                        price = float(q.get('price') or 0)
                        bid = float(q.get('best_bid') or 0) or price
                        ask = float(q.get('best_ask') or 0) or price
                        if price > 0:
                            spot_price = 0.0
                            if underlying.upper() not in ('WIN','IND','IBOV'):
                                sq = self._socket.get_quote(underlying, wait_ms=0)
                                if sq and sq.get('price'):
                                    spot_price = float(sq['price'])
                            results.append(FutureQuote(
                                symbol=ticker, underlying=underlying.upper(),
                                expiry=self._derive_expiry_from_ticker(ticker),
                                bid=bid, ask=ask, last=price,
                                volume=int(q.get('volume') or 0), oi=0,
                                basis=price - spot_price if spot_price > 0 else 0,
                                timestamp=datetime.utcnow(),
                            ))
                except Exception:
                    pass
                try:
                    self._socket.subscribe([ticker])
                except Exception:
                    pass
            if results:
                return results
        with self._lock:
            # Map underlying to B3 futures root
            futures_map = {
                'BOVA11': ['WINFUT', 'INDFUT'],
                'IBOV':   ['WINFUT', 'INDFUT'],
                'WIN':    ['WINFUT'],
                'IND':    ['INDFUT'],
                'PETR4':  ['PETR4F'],
                'VALE3':  ['VALE3F'],
                'USD':    ['DOLFUT', 'WDOFUT'],
                'BRL':    ['DOLFUT', 'WDOFUT'],
                'DI1':    ['DI1F26', 'DI1F27', 'DI1F28', 'DI1F29', 'DI1F30'],  # [v10.42] DI curve
            }

            tickers = futures_map.get(underlying.upper(), [f'{underlying[:4]}FUT'])
            results = []

            for ticker in tickers:
                data = self._cached_get(f'fut:{ticker}',
                                        f'/services/quotes/quote/{ticker}')
                if not data:
                    continue
                try:
                    spot_data = self._cached_get(f'spot:{underlying}',
                                                 f'/services/quotes/quote/{underlying}')
                    spot_price = float(spot_data.get('lastTradePrice', 0) or 0) if spot_data else 0
                    last_price = float(data.get('lastTradePrice', data.get('last', 0)) or 0)

                    results.append(FutureQuote(
                        symbol=ticker,
                        underlying=underlying.upper(),
                        expiry=str(data.get('expirationDate', data.get('maturityDate', ''))),
                        bid=float(data.get('buyPrice', data.get('bidPrice', 0)) or 0),
                        ask=float(data.get('sellPrice', data.get('askPrice', 0)) or 0),
                        last=last_price,
                        volume=int(data.get('tradeVolume', data.get('volume', 0)) or 0),
                        oi=int(data.get('openInterest', 0) or 0),
                        basis=last_price - spot_price,
                        timestamp=datetime.utcnow(),
                    ))
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"Cedro futures parse error for {ticker}: {e}")

            return results

    # ─── Rates ────────────────────────────────────────────────────

    def get_rates(self) -> Optional[RateCurve]:
        """Fetch rate curve — CDI and SELIC from Cedro."""
        with self._lock:
            # Try CDI indicator
            cdi_data = self._cached_get('rate:cdi', '/services/quotes/quote/CDI')
            selic_data = self._cached_get('rate:selic', '/services/quotes/quote/SELIC')

            cdi_rate = 14.90  # fallback
            selic_rate = 14.75

            if cdi_data:
                try:
                    cdi_rate = float(cdi_data.get('lastTradePrice',
                                    cdi_data.get('last', cdi_rate)) or cdi_rate)
                except (ValueError, TypeError):
                    pass

            if selic_data:
                try:
                    selic_rate = float(selic_data.get('lastTradePrice',
                                      selic_data.get('last', selic_rate)) or selic_rate)
                except (ValueError, TypeError):
                    pass

            # DI1 futures curve (main tenors)
            di1_terms = {}
            for months in [1, 3, 6, 12, 24]:
                # DI1 futures use F, G, H, J, K, M, N, Q, U, V, X, Z month codes
                # This is simplified — real implementation maps to actual DI1 tickers
                pass

            return RateCurve(
                date=datetime.utcnow().strftime('%Y-%m-%d'),
                cdi=cdi_rate,
                selic=selic_rate,
                di1_terms=di1_terms,
                timestamp=datetime.utcnow(),
            )

    # ─── Dividends ────────────────────────────────────────────────

    def get_dividends(self, symbol: str) -> List[DividendEvent]:
        """Fetch dividend events from Cedro."""
        with self._lock:
            data = self._cached_get(f'div:{symbol}',
                                    f'/services/quotes/dividends/{symbol}')
            if not data:
                return []

            results = []
            items = data if isinstance(data, list) else [data]
            for item in items:
                try:
                    results.append(DividendEvent(
                        symbol=symbol.upper(),
                        ex_date=str(item.get('exDate', item.get('ex_date', ''))),
                        amount=float(item.get('amount', item.get('value', 0)) or 0),
                        div_type=str(item.get('type', 'cash')),
                    ))
                except (ValueError, TypeError, KeyError):
                    continue
            return results

    # ─── Order Book ───────────────────────────────────────────────

    def get_depth(self, symbol: str, max_levels: int = 5) -> Optional[Dict[str, Any]]:
        """Fetch order book depth from Cedro."""
        with self._lock:
            data = self._cached_get(f'book:{symbol}',
                                    f'/services/quotes/book/{symbol}')
            if not data:
                return None

            bids = []
            asks = []
            try:
                for entry in (data.get('bids', data.get('buyOffers', [])) or [])[:max_levels]:
                    bids.append({
                        'price': float(entry.get('price', 0)),
                        'qty': int(entry.get('quantity', entry.get('qty', 0))),
                    })
                for entry in (data.get('asks', data.get('sellOffers', [])) or [])[:max_levels]:
                    asks.append({
                        'price': float(entry.get('price', 0)),
                        'qty': int(entry.get('quantity', entry.get('qty', 0))),
                    })
            except (ValueError, TypeError):
                pass

            return {'bids': bids, 'asks': asks, 'symbol': symbol, 'levels': max_levels}

    # ─── Greeks ───────────────────────────────────────────────────

    def get_greeks(self, option_symbol: str) -> Optional[Dict[str, float]]:
        """Fetch option Greeks from Cedro (if available in quote data)."""
        with self._lock:
            data = self._cached_get(f'greeks:{option_symbol}',
                                    f'/services/quotes/quote/{option_symbol}')
            if not data:
                return None

            try:
                return {
                    'delta': float(data.get('delta', 0) or 0),
                    'gamma': float(data.get('gamma', 0) or 0),
                    'theta': float(data.get('theta', 0) or 0),
                    'vega':  float(data.get('vega', 0) or 0),
                    'iv':    float(data.get('impliedVolatility', data.get('iv', 0)) or 0),
                }
            except (ValueError, TypeError):
                return None

    # ─── ADR Prices ───────────────────────────────────────────────

    def get_adr_prices(self, symbols: List[str]) -> Dict[str, float]:
        """Fetch ADR prices. These are NYSE-listed so may need different feed."""
        with self._lock:
            result = {}
            adr_map = {
                'PBR': 'PBR',    # Petrobras ADR
                'VALE': 'VALE',  # Vale ADR
                'ITUB': 'ITUB',  # Itaú ADR
                'BBD': 'BBD',    # Bradesco ADR
                'ABEV': 'ABEV',  # Ambev ADR
            }
            for sym in symbols:
                adr_ticker = adr_map.get(sym.upper(), sym)
                data = self._cached_get(f'adr:{adr_ticker}',
                                        f'/services/quotes/quote/{adr_ticker}')
                if data:
                    try:
                        result[sym] = float(data.get('lastTradePrice',
                                           data.get('last', 0)) or 0)
                    except (ValueError, TypeError):
                        pass
            return result

    # ─── Price History ────────────────────────────────────────────

    def get_price_history(self, symbol: str, lookback_days: int = 60) -> Optional[list]:
        """Fetch daily candle history from Cedro."""
        with self._lock:
            data = self._cached_get(
                f'hist:{symbol}:{lookback_days}',
                f'/services/candle/daily/{symbol}/{lookback_days}'
            )
            if not data:
                return None

            items = data if isinstance(data, list) else data.get('candles', [])
            results = []
            for item in items:
                try:
                    results.append({
                        'date': str(item.get('date', item.get('tradeDate', ''))),
                        'open': float(item.get('open', item.get('openPrice', 0)) or 0),
                        'high': float(item.get('high', item.get('highPrice', 0)) or 0),
                        'low': float(item.get('low', item.get('lowPrice', 0)) or 0),
                        'close': float(item.get('close', item.get('closePrice', 0)) or 0),
                        'volume': int(item.get('volume', item.get('tradeVolume', 0)) or 0),
                    })
                except (ValueError, TypeError):
                    continue
            return results if results else None

    # ─── Health Check ─────────────────────────────────────────────

    def health_check(self) -> bool:
        """Check Cedro connectivity — socket-first, then REST WebFeeder."""
        # ─── Socket-first: if Crystal socket is live, we are healthy ─
        if self._socket is not None:
            try:
                stats = getattr(self._socket, 'stats', lambda: {})() or {}
                connected = bool(stats.get('connected', True))
                if connected:
                    self._is_healthy = True
                    self._last_health_check = datetime.utcnow()
                    return True
            except Exception:
                # Socket exists but stats failed — still consider healthy since we bound successfully
                self._is_healthy = True
                self._last_health_check = datetime.utcnow()
                return True

        # ─── REST fallback ──────────────────────────────────────────
        with self._lock:
            if not self.login or not self.password:
                self._is_healthy = False
                return False

            try:
                if not self._ensure_auth():
                    return False

                # Quick check: fetch PETR4 (most liquid B3 stock)
                r = self._session.get(
                    f"{self.api_url}/services/quotes/quote/PETR4",
                    timeout=5
                )
                if r.status_code == 200:
                    data = r.json()
                    if data and (data.get('lastTradePrice') or data.get('last')):
                        self._is_healthy = True
                        self._last_health_check = datetime.utcnow()
                        return True

                self._is_healthy = False
                return False
            except Exception as e:
                self.logger.warning(f"Cedro health check failed: {e}")
                self._is_healthy = False
                return False


# ============================================================================
# OpLab Market Data Provider (Options, Greeks, IV, Book, Rates)
# ============================================================================

class OpLabMarketDataProvider(MarketDataProviderBase):
    """
    OpLab Market Data Provider — REST API.

    Primary provider for B3 derivatives: options chain, Greeks, IV, order book,
    interest rates, Black-Scholes, and historical data.

    API Docs: https://apidocs.oplab.com.br
    Auth: Access-Token header on every request.

    Env vars:
    - OPLAB_ACCESS_TOKEN: API access token (required)
    - OPLAB_API_URL: Base URL (default: https://api.oplab.com.br/v3)

    Complementary data from other providers already in the system:
    - ADR prices (NYSE) → Polygon.io via _fetch_polygon_stock()
    - Dividends → BRAPI via brapi.dev
    """

    # B3 option series: calls A-L (Jan-Dec), puts M-X (Jan-Dec)
    CALL_MONTHS = 'ABCDEFGHIJKL'
    PUT_MONTHS  = 'MNOPQRSTUVWX'

    # B3 → NYSE ADR mapping (mirrors B3_TO_ADR in api_server.py)
    B3_TO_ADR = {
        'PETR4': 'PBR', 'PETR3': 'PBR-A',
        'VALE3': 'VALE',
        'ITUB4': 'ITUB', 'ITUB3': 'ITUB',
        'BBDC4': 'BBD',  'BBDC3': 'BBD',
        'ABEV3': 'ABEV',
        'EMBR3': 'ERJ',
    }

    def __init__(self):
        super().__init__("oplab")
        self._token = os.environ.get('OPLAB_ACCESS_TOKEN', '').strip()
        self._api_url = os.environ.get(
            'OPLAB_API_URL', 'https://api.oplab.com.br/v3'
        ).rstrip('/')

        self._session = _requests_lib.Session()
        self._session.headers.update({
            'Accept': 'application/json',
            'Access-Token': self._token,
        })

        self._cache: Dict[str, Any] = {}
        self._cache_ttl = 5  # seconds

        # BRAPI for dividends
        self._brapi_token = os.environ.get('BRAPI_TOKEN', '').strip()

        # Polygon for ADR
        self._polygon_key = os.environ.get('POLYGON_API_KEY', '').strip()

        if self._token:
            self._is_healthy = True
            self.logger.info("OpLab: provider initialized (Access-Token set)")
        else:
            self._is_healthy = False
            self.logger.warning("OpLab: OPLAB_ACCESS_TOKEN not set — provider disabled")

    # ─── HTTP Helpers ─────────────────────────────────────────────

    def _get(self, path: str, params: dict = None, timeout: int = 10) -> Optional[Any]:
        """GET request to OpLab API."""
        if not self._token:
            return None
        try:
            url = f"{self._api_url}{path}"
            r = self._session.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 401:
                self.logger.error("OpLab: 401 Unauthorized — check OPLAB_ACCESS_TOKEN")
                self._is_healthy = False
            elif r.status_code == 403:
                self.logger.error(
                    f"OpLab: 403 FORBIDDEN on {path} — token recognized but account lacks access. "
                    "Likely expired plan or disabled account. Renew at https://oplab.com.br"
                )
                self._is_healthy = False
            elif r.status_code == 429:
                self.logger.warning("OpLab: rate limited (429) — backing off")
                time.sleep(1)
            else:
                self.logger.warning(f"OpLab GET {path} → HTTP {r.status_code}")
            return None
        except Exception as e:
            self.logger.warning(f"OpLab GET {path} error: {e}")
            return None

    def _cached_get(self, cache_key: str, path: str, params: dict = None) -> Optional[Any]:
        """GET with in-memory TTL cache."""
        now = time.time()
        cached = self._cache.get(cache_key)
        if cached and (now - cached['ts']) < self._cache_ttl:
            return cached['data']
        data = self._get(path, params=params)
        if data is not None:
            self._cache[cache_key] = {'data': data, 'ts': now}
        return data

    # ─── Spot Quote ───────────────────────────────────────────────

    def get_spot(self, symbol: str) -> Optional[SpotQuote]:
        """Fetch spot price from OpLab /market/stocks/{symbol}.

        Real response fields: open, high, low, close, volume, financial_volume,
        bid, ask, bid_volume, ask_volume, variation, time, previous_close,
        has_options, iv_current, beta_ibov, sector, etc.
        """
        with self._lock:
            data = self._cached_get(f'spot:{symbol}', f'/market/stocks/{symbol}')
            if not data:
                return None
            try:
                bid = float(data.get('bid', 0) or 0)
                ask = float(data.get('ask', 0) or 0)
                close = float(data.get('close', 0) or 0)
                # If bid/ask are 0 (market closed), use close as fallback
                if bid == 0:
                    bid = close
                if ask == 0:
                    ask = close

                return SpotQuote(
                    symbol=symbol.upper(),
                    bid=bid,
                    ask=ask,
                    last=close,
                    volume=int(data.get('volume', 0) or 0),
                    timestamp=datetime.utcnow(),
                )
            except (ValueError, TypeError) as e:
                self.logger.warning(f"OpLab spot parse error for {symbol}: {e}")
                return None

    # ─── Options Chain ────────────────────────────────────────────

    def _compute_greeks_bs(self, spot: float, strike: float,
                           due_date: str, rate: float,
                           opt_type: str, price: float) -> Dict[str, float]:
        """Call OpLab Black-Scholes endpoint to get Greeks and IV for one option."""
        try:
            params = {
                'spot': spot, 'strike': strike, 'due_date': due_date,
                'rate': rate, 'type': opt_type, 'price': price,
            }
            data = self._get('/market/options/bs', params=params, timeout=5)
            if data and data.get('delta'):
                return {
                    'delta': float(data.get('delta', 0) or 0),
                    'gamma': float(data.get('gamma', 0) or 0),
                    'theta': float(data.get('theta', 0) or 0),
                    'vega':  float(data.get('vega', 0) or 0),
                    'rho':   float(data.get('rho', 0) or 0),
                    'iv':    float(data.get('volatility', 0) or 0),
                }
        except Exception:
            pass
        return {}

    def get_options_chain(self, underlying: str) -> List[OptionQuote]:
        """
        Fetch options chain from OpLab /market/options/{underlying}.

        Real response fields per option: symbol, name, open, high, low, close,
        volume, financial_volume, bid, ask, bid_volume, ask_volume, category
        (CALL/PUT), due_date, maturity_type, strike, contract_size, spot_price,
        days_to_maturity, market_maker, variation, strike_eod.

        Note: Greeks are NOT returned inline. They are calculated by our local
        GreeksCalculator or via the /market/options/bs endpoint.
        """
        with self._lock:
            data = self._cached_get(
                f'chain:{underlying}',
                f'/market/options/{underlying}'
            )
            if not data:
                return []

            results = []
            items = data if isinstance(data, list) else data.get('options', data.get('data', []))

            for item in items:
                try:
                    symbol = str(item.get('symbol', ''))
                    if not symbol:
                        continue

                    # category is "CALL" or "PUT" in OpLab
                    cat = str(item.get('category', item.get('type', ''))).upper()
                    if cat in ('CALL', 'C'):
                        opt_type = 'C'
                    elif cat in ('PUT', 'P'):
                        opt_type = 'P'
                    else:
                        root_len = 4
                        if len(symbol) > root_len:
                            letter = symbol[root_len].upper()
                            opt_type = 'C' if letter in self.CALL_MONTHS else 'P'
                        else:
                            opt_type = 'C'

                    bid = float(item.get('bid', 0) or 0)
                    ask = float(item.get('ask', 0) or 0)
                    close = float(item.get('close', 0) or 0)
                    mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else close

                    results.append(OptionQuote(
                        symbol=symbol,
                        underlying=underlying.upper(),
                        strike=float(item.get('strike', 0) or 0),
                        expiry=str(item.get('due_date', '')),
                        option_type=opt_type,
                        bid=bid,
                        ask=ask,
                        last=close,
                        volume=int(item.get('volume', 0) or 0),
                        oi=int(item.get('open_interest', 0) or 0),
                        # Greeks not in chain response — set 0, local calc fills them
                        iv=0.0,
                        delta=0.0,
                        gamma=0.0,
                        theta=0.0,
                        vega=0.0,
                        timestamp=datetime.utcnow(),
                    ))
                except (ValueError, TypeError, KeyError) as e:
                    self.logger.debug(f"OpLab option parse error: {e}")
                    continue

            self.logger.info(f"OpLab: fetched {len(results)} options for {underlying}")
            return results

    # ─── Futures ──────────────────────────────────────────────────

    def get_futures(self, underlying: str) -> List[FutureQuote]:
        """
        Fetch futures from OpLab via /market/instruments.

        B3 futures: WIN, IND, DOL, WDO + underlying-specific.
        OpLab treats futures as instruments — query by symbol prefix.
        """
        with self._lock:
            # Map underlying to B3 futures tickers
            futures_map = {
                'BOVA11': ['WIN', 'IND'],
                'IBOV':   ['WIN', 'IND'],
                'PETR4':  ['PETR4'],
                'VALE3':  ['VALE3'],
                'USD':    ['DOL', 'WDO'],
                'BRL':    ['DOL', 'WDO'],
            }
            prefixes = futures_map.get(underlying.upper(), [underlying[:4]])
            results = []

            for prefix in prefixes:
                # Try fetching the instrument series
                data = self._cached_get(
                    f'fut:{prefix}',
                    f'/market/instruments/series/{prefix}'
                )

                if not data:
                    # Fallback: search instruments
                    data = self._cached_get(
                        f'fut_search:{prefix}',
                        '/market/instruments/search',
                        params={'q': prefix, 'type': 'FUTURE'}
                    )

                if not data:
                    continue

                items = data if isinstance(data, list) else data.get('data', [])
                spot_quote = self.get_spot(underlying)
                spot_price = spot_quote.mid if spot_quote else 0

                for item in items:
                    try:
                        sym = str(item.get('symbol', ''))
                        last_price = float(item.get('close', item.get('last', 0)) or 0)

                        results.append(FutureQuote(
                            symbol=sym,
                            underlying=underlying.upper(),
                            expiry=str(item.get('due_date', item.get('maturity_date', ''))),
                            bid=float(item.get('bid', 0) or 0),
                            ask=float(item.get('ask', 0) or 0),
                            last=last_price,
                            volume=int(item.get('volume', 0) or 0),
                            oi=int(item.get('open_interest', 0) or 0),
                            basis=last_price - spot_price if spot_price else 0,
                            timestamp=datetime.utcnow(),
                        ))
                    except (ValueError, TypeError) as e:
                        self.logger.debug(f"OpLab futures parse error for {prefix}: {e}")
                        continue

            if results:
                return results

            # ── Fallback: BRAPI futures (WINFUT, INDFUT, DOLFUT, WDOFUT) ──
            return self._get_futures_brapi(underlying, prefixes)

    def _get_futures_brapi(self, underlying: str, oplab_prefixes: list) -> List[FutureQuote]:
        """Fallback: try BRAPI for B3 futures tickers like WINFUT, INDFUT."""
        if not self._brapi_token:
            return []

        # Map underlying/prefix to BRAPI futures ticker(s)
        _brapi_futures_map = {
            'WIN': ['WINFUT'],
            'IND': ['INDFUT'],
            'DOL': ['DOLFUT'],
            'WDO': ['WDOFUT'],
            'BOVA11': ['WINFUT', 'INDFUT'],
            'IBOV': ['WINFUT', 'INDFUT'],
        }

        brapi_tickers = set()
        for prefix in oplab_prefixes:
            for t in _brapi_futures_map.get(prefix, []):
                brapi_tickers.add(t)
        # Also try direct underlying mapping
        for t in _brapi_futures_map.get(underlying.upper(), []):
            brapi_tickers.add(t)

        if not brapi_tickers:
            self.logger.debug(f"BRAPI futures: no mapping for {underlying} / prefixes={oplab_prefixes}")
            return []

        results = []
        ticker_str = ','.join(sorted(brapi_tickers))

        try:
            url = f"https://brapi.dev/api/quote/{ticker_str}"
            r = _requests_lib.get(url, params={
                'token': self._brapi_token,
            }, timeout=10)

            if r.status_code != 200:
                self.logger.debug(f"BRAPI futures: status {r.status_code} for {ticker_str}")
                return []

            data = r.json()
            if data.get('error'):
                self.logger.debug(f"BRAPI futures error for {ticker_str}: {data.get('message','?')[:80]}")
                return []

            spot_quote = self.get_spot(underlying)
            spot_price = spot_quote.mid if spot_quote else 0

            for q in data.get('results', []):
                try:
                    sym = str(q.get('symbol', ''))
                    price = float(q.get('regularMarketPrice', 0) or 0)
                    if price <= 0:
                        continue

                    bid = float(q.get('regularMarketDayLow', price) or price)
                    ask = float(q.get('regularMarketDayHigh', price) or price)

                    # Estimate expiry: B3 index futures expire 3rd Wed of even months
                    import calendar
                    now = datetime.utcnow()
                    month = now.month
                    year = now.year
                    if month % 2 != 0:
                        month += 1
                    if month > 12:
                        month = 2
                        year += 1
                    cal = calendar.Calendar()
                    wednesdays = [d for d in cal.itermonthdays2(year, month) if d[0] > 0 and d[1] == 2]
                    expiry_day = wednesdays[2][0] if len(wednesdays) >= 3 else 15
                    expiry_str = f"{year}-{month:02d}-{expiry_day:02d}"

                    results.append(FutureQuote(
                        symbol=sym,
                        underlying=underlying.upper(),
                        expiry=expiry_str,
                        bid=bid,
                        ask=ask,
                        last=price,
                        volume=int(q.get('regularMarketVolume', 0) or 0),
                        oi=0,
                        basis=price - spot_price if spot_price else 0,
                        timestamp=datetime.utcnow(),
                    ))
                    self.logger.info(f"BRAPI futures fallback: {sym} price={price} for {underlying}")

                except (ValueError, TypeError) as e:
                    self.logger.debug(f"BRAPI futures parse error: {e}")
                    continue

        except Exception as e:
            self.logger.warning(f"BRAPI futures fallback error for {underlying}: {e}")

        return results

    # ─── Rates ────────────────────────────────────────────────────

    def get_rates(self) -> Optional[RateCurve]:
        """Fetch interest rate curve from OpLab /market/interest_rates.

        Real response: list of 2 items:
        [{"name": "Taxa DI", "value": 14.65}, {"name": "Taxa Selic", "value": 14.65}]
        """
        with self._lock:
            data = self._cached_get('rates:all', '/market/interest_rates')

            cdi_rate = 14.90  # sensible defaults
            selic_rate = 14.75
            di1_terms = {}

            if data and isinstance(data, list):
                for item in data:
                    try:
                        name = str(item.get('name', '')).upper()
                        rate_val = float(item.get('value', 0) or 0)
                        if rate_val <= 0:
                            continue
                        if 'DI' in name and 'SELIC' not in name:
                            cdi_rate = rate_val
                        elif 'SELIC' in name:
                            selic_rate = rate_val
                    except (ValueError, TypeError):
                        continue

            return RateCurve(
                date=datetime.utcnow().strftime('%Y-%m-%d'),
                cdi=cdi_rate,
                selic=selic_rate,
                di1_terms=di1_terms,
                timestamp=datetime.utcnow(),
            )

    # ─── Dividends (via BRAPI) ────────────────────────────────────

    def get_dividends(self, symbol: str) -> List[DividendEvent]:
        """
        Fetch dividends via BRAPI (brapi.dev) — already subscribed.

        OpLab does not provide dividend data, so we use the existing BRAPI
        integration for this.
        """
        with self._lock:
            if not self._brapi_token:
                self.logger.debug("OpLab.get_dividends: BRAPI_TOKEN not set")
                return []

            try:
                url = f"https://brapi.dev/api/quote/{symbol}"
                r = _requests_lib.get(url, params={
                    'token': self._brapi_token,
                    'dividends': 'true',
                    'range': '6mo',
                }, timeout=10)

                if r.status_code != 200:
                    return []

                data = r.json()
                results_list = data.get('results', [])
                if not results_list:
                    return []

                dividends = results_list[0].get('dividendsData', {}).get('cashDividends', [])
                result = []
                for div in dividends:
                    try:
                        result.append(DividendEvent(
                            symbol=symbol.upper(),
                            ex_date=str(div.get('exDividendDate', '')),
                            amount=float(div.get('rate', div.get('value', 0)) or 0),
                            div_type='cash' if div.get('label', '').upper() != 'JCP' else 'jcp',
                        ))
                    except (ValueError, TypeError, KeyError):
                        continue

                return result
            except Exception as e:
                self.logger.warning(f"OpLab.get_dividends (BRAPI) error: {e}")
                return []

    # ─── Order Book ───────────────────────────────────────────────

    def get_depth(self, symbol: str, max_levels: int = 5) -> Optional[Dict[str, Any]]:
        """Fetch order book from OpLab /market/options/details/{symbol} or /market/stocks/{symbol}."""
        with self._lock:
            # Try options detail endpoint (has book data)
            data = self._cached_get(f'book:{symbol}', f'/market/options/details/{symbol}')

            if not data:
                # Fallback: stock endpoint
                data = self._cached_get(f'stock_book:{symbol}', f'/market/stocks/{symbol}')

            if not data:
                return None

            bids = []
            asks = []
            try:
                # OpLab may return book as nested arrays or objects
                book_data = data.get('book', data.get('depth', data))

                bid_list = book_data.get('bids', book_data.get('buy', [])) or []
                ask_list = book_data.get('asks', book_data.get('sell', [])) or []

                for entry in bid_list[:max_levels]:
                    if isinstance(entry, dict):
                        bids.append({
                            'price': float(entry.get('price', 0)),
                            'qty': int(entry.get('quantity', entry.get('qty', entry.get('amount', 0)))),
                        })
                    elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                        bids.append({'price': float(entry[0]), 'qty': int(entry[1])})

                for entry in ask_list[:max_levels]:
                    if isinstance(entry, dict):
                        asks.append({
                            'price': float(entry.get('price', 0)),
                            'qty': int(entry.get('quantity', entry.get('qty', entry.get('amount', 0)))),
                        })
                    elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                        asks.append({'price': float(entry[0]), 'qty': int(entry[1])})
            except (ValueError, TypeError, AttributeError):
                pass

            if not bids and not asks:
                # Minimal book from bid/ask in quote
                bid_price = float(data.get('bid', 0) or 0)
                ask_price = float(data.get('ask', 0) or 0)
                if bid_price > 0:
                    bids = [{'price': bid_price, 'qty': int(data.get('bid_volume', 100))}]
                if ask_price > 0:
                    asks = [{'price': ask_price, 'qty': int(data.get('ask_volume', 100))}]

            return {'bids': bids, 'asks': asks, 'symbol': symbol, 'levels': max_levels}

    # ─── Greeks ───────────────────────────────────────────────────

    def get_greeks(self, option_symbol: str) -> Optional[Dict[str, float]]:
        """Fetch Greeks from OpLab /market/options/details/{symbol}."""
        with self._lock:
            data = self._cached_get(
                f'greeks:{option_symbol}',
                f'/market/options/details/{option_symbol}'
            )
            if not data:
                return None
            try:
                return {
                    'delta': float(data.get('delta', 0) or 0),
                    'gamma': float(data.get('gamma', 0) or 0),
                    'theta': float(data.get('theta', 0) or 0),
                    'vega':  float(data.get('vega', 0) or 0),
                    'iv':    float(data.get('implied_volatility', data.get('iv', 0)) or 0),
                    'rho':   float(data.get('rho', 0) or 0),
                }
            except (ValueError, TypeError):
                return None

    # ─── ADR Prices (via Polygon) ─────────────────────────────────

    def get_adr_prices(self, symbols: List[str]) -> Dict[str, float]:
        """
        Fetch ADR prices using Polygon.io — already integrated in the system.

        Maps B3 tickers to NYSE ADRs and queries Polygon snapshot endpoint.
        """
        with self._lock:
            if not self._polygon_key:
                self.logger.debug("OpLab.get_adr_prices: POLYGON_API_KEY not set")
                return {}

            result = {}
            for sym in symbols:
                adr_ticker = self.B3_TO_ADR.get(sym.upper(), sym)
                try:
                    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{adr_ticker}"
                    r = _requests_lib.get(url, params={'apiKey': self._polygon_key}, timeout=8)
                    if r.status_code == 200:
                        data = r.json()
                        ticker_data = data.get('ticker', {})
                        day_data = ticker_data.get('day', {})
                        price = float(day_data.get('c', day_data.get('vw', 0)) or 0)
                        if price > 0:
                            result[sym] = price
                except Exception as e:
                    self.logger.debug(f"OpLab.get_adr_prices Polygon error for {adr_ticker}: {e}")
                    continue

            return result

    # ─── Price History ────────────────────────────────────────────

    def get_price_history(self, symbol: str, lookback_days: int = 60) -> Optional[list]:
        """
        Fetch historical daily candles from OpLab /market/historical/{symbol}/1d.

        Requires from/to date params. Real response:
        {"symbol":"PETR4","resolution":"1d","data":[
            {"time":1772420400000,"open":41.3,"low":40.52,"high":41.53,
             "close":41.13,"volume":83817000,"fvolume":3434243282}, ...
        ]}

        Returns list of dicts with date, open, high, low, close, volume.
        """
        with self._lock:
            from_date = (datetime.utcnow() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            to_date = datetime.utcnow().strftime('%Y-%m-%d')

            data = self._cached_get(
                f'hist:{symbol}:{lookback_days}',
                f'/market/historical/{symbol}/1d',
                params={'from': from_date, 'to': to_date}
            )
            if not data:
                return None

            # Response is {"symbol":...,"data":[...]} or direct list
            items = data.get('data', []) if isinstance(data, dict) else data
            if not isinstance(items, list):
                return None

            results = []
            for item in items:
                try:
                    # time is epoch milliseconds
                    ts = item.get('time', 0)
                    if isinstance(ts, (int, float)) and ts > 1_000_000_000_000:
                        date_str = datetime.utcfromtimestamp(ts / 1000).strftime('%Y-%m-%d')
                    else:
                        date_str = str(ts)

                    results.append({
                        'date': date_str,
                        'open': float(item.get('open', 0) or 0),
                        'high': float(item.get('high', 0) or 0),
                        'low': float(item.get('low', 0) or 0),
                        'close': float(item.get('close', 0) or 0),
                        'volume': int(item.get('volume', 0) or 0),
                    })
                except (ValueError, TypeError):
                    continue

            if results:
                return results

            # ── Fallback: BRAPI historical ──────────────────────────
            return self._get_price_history_brapi(symbol, lookback_days)

    def _get_price_history_brapi(self, symbol: str, lookback_days: int = 60) -> Optional[list]:
        """Fallback: fetch historical prices from BRAPI when OpLab returns nothing."""
        if not self._brapi_token:
            return None
        try:
            # Map lookback to BRAPI range string
            if lookback_days <= 30:
                brapi_range = '1mo'
            elif lookback_days <= 90:
                brapi_range = '3mo'
            elif lookback_days <= 180:
                brapi_range = '6mo'
            else:
                brapi_range = '1y'

            url = f"https://brapi.dev/api/quote/{symbol}"
            r = _requests_lib.get(url, params={
                'token': self._brapi_token,
                'range': brapi_range,
                'interval': '1d',
                'fundamental': 'false',
            }, timeout=12)

            if r.status_code != 200:
                return None

            data = r.json()
            results_list = data.get('results', [])
            if not results_list:
                return None

            hist = results_list[0].get('historicalDataPrice', [])
            if not hist:
                return None

            results = []
            for item in hist:
                try:
                    close_val = float(item.get('close', 0) or 0)
                    if close_val <= 0:
                        continue
                    ts = item.get('date', 0)
                    date_str = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d') if isinstance(ts, (int, float)) and ts > 0 else '?'
                    results.append({
                        'date': date_str,
                        'open': float(item.get('open', 0) or 0),
                        'high': float(item.get('high', 0) or 0),
                        'low': float(item.get('low', 0) or 0),
                        'close': close_val,
                        'volume': int(item.get('volume', 0) or 0),
                    })
                except (ValueError, TypeError):
                    continue

            self.logger.info(f"OpLab.get_price_history: BRAPI fallback returned {len(results)} candles for {symbol}")
            return results if results else None
        except Exception as e:
            self.logger.warning(f"OpLab._get_price_history_brapi error for {symbol}: {e}")
            return None

    # ─── Health Check ─────────────────────────────────────────────

    def health_check(self) -> bool:
        """Validate OpLab connectivity by fetching market status."""
        with self._lock:
            if not self._token:
                self._is_healthy = False
                return False
            try:
                r = self._session.get(
                    f"{self._api_url}/market/status",
                    timeout=5
                )
                if r.status_code == 200:
                    self._is_healthy = True
                    self._last_health_check = datetime.utcnow()
                    return True

                # Fallback: try fetching PETR4 options
                r2 = self._session.get(
                    f"{self._api_url}/market/stocks/PETR4",
                    timeout=5
                )
                if r2.status_code == 200:
                    self._is_healthy = True
                    self._last_health_check = datetime.utcnow()
                    return True

                self._is_healthy = False
                return False
            except Exception as e:
                self.logger.warning(f"OpLab health check failed: {e}")
                self._is_healthy = False
                return False


# ============================================================================
# Simulated Market Data Provider (for testing/paper trading)
# ============================================================================

class SimulatedMarketDataProvider(MarketDataProviderBase):
    """
    Simulated market data provider for testing and paper trading.
    
    Generates realistic synthetic data based on known spot prices.
    """
    
    def __init__(self, base_spots: Optional[Dict[str, float]] = None):
        super().__init__("simulated")
        self._base_spots = base_spots or {
            "PETR4": 28.50,
            "VALE3": 55.20,
            "BOVA11": 82.15,
            "ITUB4": 26.85,
            "BBDC4": 12.40,
            "BBAS3": 38.90,
            "ABEV3": 14.25,
            "B3SA3": 9.75,
        }
        self._spot_cache: Dict[str, SpotQuote] = {}
        self._cache_ts = time.time()
        self._is_healthy = True
        self.logger.info(f"SimulatedMarketDataProvider initialized with {len(self._base_spots)} spots")
    
    def _get_simulated_spot(self, symbol: str) -> SpotQuote:
        """Generate simulated spot quote with realistic noise."""
        base = self._base_spots.get(symbol, 50.0)
        noise = random.gauss(0, base * 0.002)  # 0.2% std dev
        mid = base + noise
        spread = mid * 0.001  # 0.1% spread
        
        return SpotQuote(
            symbol=symbol,
            bid=mid - spread / 2,
            ask=mid + spread / 2,
            last=mid,
            volume=random.randint(100_000, 1_000_000),
        )
    
    def get_spot(self, symbol: str) -> Optional[SpotQuote]:
        """Get simulated spot quote."""
        with self._lock:
            if symbol not in self._base_spots:
                self.logger.debug(f"Simulated: Symbol {symbol} not in base spots")
                return None
            return self._get_simulated_spot(symbol)
    
    def get_options_chain(self, underlying: str) -> List[OptionQuote]:
        """Generate simulated options chain."""
        with self._lock:
            spot_quote = self.get_spot(underlying)
            if not spot_quote:
                return []
            
            spot = spot_quote.mid
            chain = []
            
            # Generate ATM, ITM, OTM calls and puts for multiple expirations
            for days_out in [10, 20, 40]:
                for strike_pct in [0.98, 1.0, 1.02]:
                    strike = spot * strike_pct
                    for opt_type in ["C", "P"]:
                        expiry = (datetime.utcnow() + timedelta(days=days_out)).isoformat()
                        
                        # Simplified greeks calculation
                        if opt_type == "C":
                            delta = 0.5 if strike_pct == 1.0 else (0.7 if strike_pct < 1.0 else 0.3)
                            mid_premium = max(spot - strike, 0) + 1.0
                        else:
                            delta = -0.5 if strike_pct == 1.0 else (-0.3 if strike_pct < 1.0 else -0.7)
                            mid_premium = max(strike - spot, 0) + 1.0
                        
                        quote = OptionQuote(
                            symbol=f"{underlying}{expiry[:7]}{opt_type}{int(strike)}",
                            underlying=underlying,
                            strike=strike,
                            expiry=expiry,
                            option_type=opt_type,
                            bid=mid_premium * 0.99,
                            ask=mid_premium * 1.01,
                            last=mid_premium,
                            volume=random.randint(100, 5000),
                            oi=random.randint(500, 50000),
                            iv=0.25 + random.gauss(0, 0.02),
                            delta=delta,
                            gamma=0.01,
                            theta=-0.01,
                            vega=0.05,
                        )
                        chain.append(quote)
            
            return chain
    
    def get_futures(self, underlying: str) -> List[FutureQuote]:
        """Generate simulated futures quotes."""
        with self._lock:
            spot_quote = self.get_spot(underlying)
            if not spot_quote:
                return []
            
            spot = spot_quote.mid
            futures = []
            
            for days_out in [30, 60, 120]:
                expiry = (datetime.utcnow() + timedelta(days=days_out)).isoformat()
                fut_price = spot * (1 + 0.0001 * days_out)  # Slight contango
                
                quote = FutureQuote(
                    symbol=f"{underlying}_F{expiry[:7]}",
                    underlying=underlying,
                    expiry=expiry,
                    bid=fut_price * 0.9995,
                    ask=fut_price * 1.0005,
                    last=fut_price,
                    volume=random.randint(1000, 100000),
                    oi=random.randint(10000, 500000),
                    basis=fut_price - spot,
                )
                futures.append(quote)
            
            return futures
    
    def get_rates(self) -> Optional[RateCurve]:
        """Return simulated rate curve."""
        with self._lock:
            cdi = 14.90
            selic = 14.75
            
            return RateCurve(
                date=datetime.utcnow().date().isoformat(),
                cdi=cdi,
                selic=selic,
                di1_terms={
                    30: cdi * 0.98,
                    60: cdi * 0.97,
                    90: cdi * 0.96,
                    180: cdi * 0.94,
                    360: cdi * 0.92,
                },
            )
    
    def get_dividends(self, symbol: str) -> List[DividendEvent]:
        """Return simulated dividends."""
        with self._lock:
            if symbol not in self._base_spots:
                return []
            
            # Return some simulated dividend events
            return [
                DividendEvent(
                    symbol=symbol,
                    ex_date=(datetime.utcnow() + timedelta(days=30)).isoformat(),
                    amount=0.50,
                    div_type="cash",
                )
            ]
    
    def get_depth(self, symbol: str, max_levels: int = 5) -> Optional[Dict[str, Any]]:
        """Return simulated order book depth."""
        with self._lock:
            spot_quote = self.get_spot(symbol)
            if not spot_quote:
                return None
            
            bid_price = spot_quote.bid
            ask_price = spot_quote.ask
            
            bids = [
                {"price": bid_price - i * 0.01, "size": 1000 - i * 100}
                for i in range(max_levels)
            ]
            asks = [
                {"price": ask_price + i * 0.01, "size": 1000 - i * 100}
                for i in range(max_levels)
            ]
            
            return {"bids": bids, "asks": asks, "timestamp": datetime.utcnow().isoformat()}
    
    def get_greeks(self, option_symbol: str) -> Optional[Dict[str, float]]:
        """Return simulated Greeks."""
        with self._lock:
            return {
                "delta": random.uniform(-1, 1),
                "gamma": random.uniform(0, 0.1),
                "theta": random.uniform(-0.1, 0),
                "vega": random.uniform(0, 0.2),
            }
    
    def get_adr_prices(self, symbols: List[str]) -> Dict[str, float]:
        """Return simulated ADR prices."""
        with self._lock:
            result = {}
            for symbol in symbols:
                if symbol in self._base_spots:
                    result[symbol] = self._base_spots[symbol] * 0.20  # Rough conversion
            return result
    
    def get_price_history(self, symbol: str, lookback_days: int = 60) -> Optional[List[float]]:
        """Generate simulated price history for vol calculations."""
        import math
        base = self._base_spots.get(symbol)
        if not base:
            return None
        prices = []
        price = base * 0.95  # Start slightly below current
        daily_vol = 0.02  # ~32% annualized
        for i in range(lookback_days):
            drift = 0.0002  # Small upward drift
            shock = (hash(f"{symbol}_{i}") % 1000 - 500) / 500.0 * daily_vol
            price *= (1 + drift + shock)
            prices.append(round(price, 2))
        return prices

    def health_check(self) -> bool:
        """Simulated provider is always healthy."""
        with self._lock:
            self._is_healthy = True
            self._last_health_check = datetime.utcnow()
            return True


# ============================================================================
# Provider Manager (Singleton)
# ============================================================================

class ProviderManager:
    """
    Singleton manager for market data providers.
    
    Manages provider lifecycle, fallback chains, and health checks.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.logger = logging.getLogger('egreja.derivatives.provider_manager')
        self._providers: Dict[str, MarketDataProviderBase] = {}
        self._primary_provider = None
        self._fallback_chain: List[str] = []
        self._health_checks_enabled = True
        self._lock = threading.RLock()
        self._initialized = True
        self.logger.info("ProviderManager initialized")
    
    def register_provider(
        self, name: str, provider: MarketDataProviderBase, is_primary: bool = False
    ):
        """Register a market data provider."""
        with self._lock:
            self._providers[name] = provider
            if is_primary:
                self._primary_provider = name
                self._fallback_chain.insert(0, name)
            else:
                if name not in self._fallback_chain:
                    self._fallback_chain.append(name)
            
            self.logger.info(
                f"Registered provider '{name}' (primary={is_primary})"
            )
    
    def get_provider(self, name: str) -> Optional[MarketDataProviderBase]:
        """Get a provider by name."""
        with self._lock:
            return self._providers.get(name)
    
    def get_active_provider(self) -> Optional[MarketDataProviderBase]:
        """Get the first healthy provider from fallback chain."""
        with self._lock:
            for name in self._fallback_chain:
                provider = self._providers.get(name)
                if provider and provider.health_check():
                    return provider
            
            self.logger.warning("No healthy providers available")
            return None
    
    def list_providers(self) -> List[str]:
        """List all registered provider names."""
        with self._lock:
            return list(self._providers.keys())
    
    def health_check_all(self) -> Dict[str, bool]:
        """Perform health check on all providers."""
        with self._lock:
            results = {}
            for name, provider in self._providers.items():
                results[name] = provider.health_check()
            
            healthy_count = sum(1 for v in results.values() if v)
            self.logger.info(
                f"Health check: {healthy_count}/{len(results)} providers healthy"
            )
            return results
    
    def set_primary_provider(self, name: str) -> bool:
        """Set primary provider (moves to front of fallback chain)."""
        with self._lock:
            if name not in self._providers:
                self.logger.error(f"Provider '{name}' not registered")
                return False
            
            self._primary_provider = name
            if name in self._fallback_chain:
                self._fallback_chain.remove(name)
            self._fallback_chain.insert(0, name)
            self.logger.info(f"Set primary provider to '{name}'")
            return True
    
    def get_fallback_chain(self) -> List[str]:
        """Get the current fallback chain."""
        with self._lock:
            return list(self._fallback_chain)

    # ─── Delegation methods for strategy scan loops ───

    def _resolve(self) -> Optional[MarketDataProviderBase]:
        """Resolve the active provider (fallback chain first, then _active attr)."""
        p = self.get_active_provider()
        if p:
            return p
        # Fallback: direct _active attribute (set during init)
        return getattr(self, '_active', None)

    def get_spot(self, symbol: str) -> Optional[SpotQuote]:
        p = self._resolve()
        result = p.get_spot(symbol) if p else None
        if result:
            return result

        # ── Fallback for special symbols not on B3/OpLab ────────────
        return self._get_spot_fallback(symbol)

    def _get_spot_fallback(self, symbol: str) -> Optional[SpotQuote]:
        """Fallback spot quotes for FX and ADR symbols via BRAPI/Polygon."""
        import logging
        _log = logging.getLogger('providers.fallback')

        # FX: USDBRL via BRAPI currency endpoint
        if symbol.upper() in ('USDBRL', 'USD/BRL', 'BRL=X'):
            brapi_token = os.environ.get('BRAPI_TOKEN', '').strip()
            if not brapi_token:
                return None
            try:
                r = _requests_lib.get(
                    'https://brapi.dev/api/v2/currency',
                    params={'currency': 'USD-BRL', 'token': brapi_token},
                    timeout=8,
                )
                if r.status_code == 200:
                    data = r.json()
                    currencies = data.get('currency', [])
                    if currencies:
                        c = currencies[0]
                        bid = float(c.get('bidPrice', 0) or 0)
                        ask = float(c.get('askPrice', 0) or 0)
                        if bid > 0 and ask > 0:
                            _log.info(f"BRAPI FX fallback: USDBRL bid={bid} ask={ask}")
                            return SpotQuote(
                                symbol='USDBRL', bid=bid, ask=ask,
                                last=(bid + ask) / 2, volume=0,
                                timestamp=datetime.utcnow(),
                            )
            except Exception as e:
                _log.debug(f"BRAPI FX fallback error: {e}")
            return None

        # ADR: NYSE stocks via Polygon.io
        polygon_key = os.environ.get('POLYGON_API_KEY', '').strip()
        # Known ADR tickers (not on B3)
        _adr_tickers = {'PBR', 'PBR-A', 'VALE', 'ITUB', 'BBD', 'ABEV', 'ERJ', 'EWZ'}
        if symbol.upper() in _adr_tickers and polygon_key:
            try:
                url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol.upper()}"
                r = _requests_lib.get(url, params={'apiKey': polygon_key}, timeout=8)
                if r.status_code == 200:
                    data = r.json()
                    ticker_data = data.get('ticker', {})
                    day = ticker_data.get('day', {})
                    last_price = float(day.get('c', day.get('vw', 0)) or 0)
                    if last_price > 0:
                        _log.info(f"Polygon ADR fallback: {symbol} price={last_price}")
                        return SpotQuote(
                            symbol=symbol.upper(), bid=last_price, ask=last_price,
                            last=last_price, volume=int(day.get('v', 0) or 0),
                            timestamp=datetime.utcnow(),
                        )
            except Exception as e:
                _log.debug(f"Polygon ADR fallback error for {symbol}: {e}")
            return None

        return None

    def get_option_chain(self, underlying: str, option_type: str = None, expiry=None, min_dte: int = 0) -> Optional[dict]:
        """Get option chain keyed by strike for the NEAREST valid expiry (or a specific one).
        Previously collapsed all expiries into one dict, losing ~80% of strikes.
        """
        p = self._resolve()
        if not p:
            return None
        chain = p.get_options_chain(underlying)
        if not chain:
            return None
        _type_map = {'CALL': 'C', 'PUT': 'P', 'C': 'C', 'P': 'P'}
        norm_type = _type_map.get((option_type or '').upper()) if option_type else None
        # Group by expiry
        import datetime as _dt
        by_expiry = {}
        for quote in chain:
            if norm_type and hasattr(quote, 'option_type'):
                qt = _type_map.get((quote.option_type or '').upper())
                if qt != norm_type:
                    continue
            exp = getattr(quote, 'expiry', None)
            by_expiry.setdefault(exp, {})[quote.strike] = quote
        if not by_expiry:
            return None
        # Choose expiry
        today = _dt.date.today()
        def _to_date(e):
            if isinstance(e, _dt.datetime): return e.date()
            if isinstance(e, _dt.date): return e
            try: return _dt.datetime.strptime(str(e)[:10], '%Y-%m-%d').date()
            except Exception: return None
        if expiry is not None:
            tgt = _to_date(expiry)
            if tgt and tgt in [_to_date(k) for k in by_expiry]:
                for k in by_expiry:
                    if _to_date(k) == tgt:
                        return by_expiry[k] or None
        valid = []
        _min_d = today + _dt.timedelta(days=int(min_dte)) if min_dte else today
        for k in by_expiry:
            d = _to_date(k)
            if d and d >= _min_d:
                valid.append((d, k))
        # If min_dte filter empty, fall back to any >= today
        if not valid and min_dte:
            for k in by_expiry:
                d = _to_date(k)
                if d and d >= today:
                    valid.append((d, k))
        if not valid:
            # Fallback: merge everything (legacy behavior)
            merged = {}
            for d in by_expiry.values():
                merged.update(d)
            return merged or None
        valid.sort()
        return by_expiry[valid[0][1]] or None

    def get_future(self, underlying: str, tenor_offset: int = 0) -> Optional[FutureQuote]:
        """Get future quote for given underlying. tenor_offset=0 is nearest."""
        p = self._resolve()
        if not p:
            return None
        futures = p.get_futures(underlying)
        if not futures:
            return None
        # Sort by expiry and pick by offset
        sorted_f = sorted(futures, key=lambda f: f.expiry)
        if tenor_offset < len(sorted_f):
            return sorted_f[tenor_offset]
        return None

    def get_price_history(self, symbol: str, lookback_days: int = 60) -> Optional[list]:
        """[v10.42] Get historical prices. Provider first, then synthetic fallback."""
        p = self._resolve()
        if p and hasattr(p, 'get_price_history'):
            result = p.get_price_history(symbol, lookback_days)
            if result and len(result) >= 20:
                return result
        # [v10.42] Synthetic fallback from current spot for Vol Arb
        spot_quote = self.get_spot(symbol)
        if not spot_quote or spot_quote.mid <= 0:
            return None
        base = spot_quote.mid
        prices = []
        price = base * 0.95
        daily_vol = 0.02
        for i in range(lookback_days):
            drift = 0.0002
            shock = (hash(f"{symbol}_{i}") % 1000 - 500) / 500.0 * daily_vol
            price *= (1 + drift + shock)
            prices.append(round(price, 2))
        return prices if len(prices) >= 20 else None

    def get_provider_health(self, name: str) -> Optional[Dict[str, Any]]:
        """Get health status for a named provider (used by dashboard)."""
        with self._lock:
            provider = self._providers.get(name)
            if not provider:
                return None
            try:
                healthy = provider.health_check()
                last_check = getattr(provider, '_last_health_check', None)
                return {
                    'status': 'healthy' if healthy else 'unhealthy',
                    'last_check': last_check.isoformat() if last_check else None,
                }
            except Exception as e:
                return {'status': f'error: {e}', 'last_check': None}


# Global singleton instance helper
_provider_manager = None


def get_provider_manager() -> ProviderManager:
    """Get the global ProviderManager instance."""
    global _provider_manager
    if _provider_manager is None:
        _provider_manager = ProviderManager()
    return _provider_manager
