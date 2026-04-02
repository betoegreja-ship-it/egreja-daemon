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
import random

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
    Cedro market data provider (stub).
    
    Configured via environment variables:
    - CEDRO_API_KEY: API authentication key
    - CEDRO_API_URL: Base URL for REST endpoints
    - CEDRO_WS_URL: WebSocket URL for real-time data
    """
    
    def __init__(self):
        super().__init__("cedro")
        self.api_key = None  # Would be set from env
        self.api_url = None  # Would be set from env
        self.ws_url = None  # Would be set from env
        self._ws_client = None  # WebSocket connection
        self.logger.info("CedroMarketDataProvider initialized (not yet configured)")
    
    def get_spot(self, symbol: str) -> Optional[SpotQuote]:
        """Fetch spot price from Cedro."""
        with self._lock:
            if not self._is_healthy:
                self.logger.debug(f"Cedro not healthy, cannot fetch spot for {symbol}")
                return None
            
            self.logger.debug(f"Cedro: Would fetch spot for {symbol} via REST")
            # In real implementation: return self._call_rest_api(f"/spot/{symbol}")
            return None
    
    def get_options_chain(self, underlying: str) -> List[OptionQuote]:
        """Fetch options chain from Cedro."""
        with self._lock:
            if not self._is_healthy:
                self.logger.debug(f"Cedro not healthy, cannot fetch chain for {underlying}")
                return []
            
            self.logger.debug(f"Cedro: Would fetch options chain for {underlying} via REST")
            # In real implementation: return self._call_rest_api(f"/options/{underlying}")
            return []
    
    def get_futures(self, underlying: str) -> List[FutureQuote]:
        """Fetch futures from Cedro."""
        with self._lock:
            if not self._is_healthy:
                self.logger.debug(f"Cedro not healthy, cannot fetch futures for {underlying}")
                return []
            
            self.logger.debug(f"Cedro: Would fetch futures for {underlying} via REST")
            return []
    
    def get_rates(self) -> Optional[RateCurve]:
        """Fetch rate curve from Cedro."""
        with self._lock:
            if not self._is_healthy:
                self.logger.debug("Cedro not healthy, cannot fetch rates")
                return None
            
            self.logger.debug("Cedro: Would fetch rates via REST")
            return None
    
    def get_dividends(self, symbol: str) -> List[DividendEvent]:
        """Fetch dividend events from Cedro."""
        with self._lock:
            if not self._is_healthy:
                self.logger.debug(f"Cedro not healthy, cannot fetch dividends for {symbol}")
                return []
            
            self.logger.debug(f"Cedro: Would fetch dividends for {symbol} via REST")
            return []
    
    def get_depth(self, symbol: str, max_levels: int = 5) -> Optional[Dict[str, Any]]:
        """Fetch order book depth from Cedro."""
        with self._lock:
            if not self._is_healthy:
                return None
            
            self.logger.debug(f"Cedro: Would fetch depth for {symbol} via WebSocket")
            return None
    
    def get_greeks(self, option_symbol: str) -> Optional[Dict[str, float]]:
        """Fetch option Greeks from Cedro."""
        with self._lock:
            if not self._is_healthy:
                return None
            
            self.logger.debug(f"Cedro: Would fetch greeks for {option_symbol} via REST")
            return None
    
    def get_adr_prices(self, symbols: List[str]) -> Dict[str, float]:
        """Fetch ADR prices from Cedro."""
        with self._lock:
            if not self._is_healthy:
                return {}
            
            self.logger.debug(f"Cedro: Would fetch ADR prices for {symbols}")
            return {}
    
    def health_check(self) -> bool:
        """Check Cedro connectivity."""
        with self._lock:
            # In real implementation, attempt to reach Cedro
            self._is_healthy = False
            self.logger.warning("Cedro not configured or unreachable")
            self._last_health_check = datetime.utcnow()
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


# Global singleton instance helper
_provider_manager = None


def get_provider_manager() -> ProviderManager:
    """Get the global ProviderManager instance."""
    global _provider_manager
    if _provider_manager is None:
        _provider_manager = ProviderManager()
    return _provider_manager
