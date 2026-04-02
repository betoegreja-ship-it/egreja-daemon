"""
Egreja Derivatives Trading Module

Provides comprehensive infrastructure for derivatives trading across multiple strategies:
- PCP (Put-Call Parity)
- FST (Futures Spread Trading)
- Roll Arbitrage
- ETF Basket
- Skew Arbitrage
- Interlisted
- Dividend Arbitrage
- Volatility Arbitrage

Thread-safe, fully instrumented for production use.
"""

import logging

# Configure module logger
logger = logging.getLogger('egreja.derivatives')

# Import public classes from submodules
from .config import (
    DerivativesConfig,
    PCPConfig,
    FSTConfig,
    LiquidityScore,
    ActiveStatus,
    B3Fees,
    PromotionDemotionRules,
    get_config,
    reload_config,
)

from .providers import (
    MarketDataProviderBase,
    SpotProvider,
    OptionsChainProvider,
    FuturesProvider,
    RatesProvider,
    DividendProvider,
    ADRProvider,
    OptionQuote,
    FutureQuote,
    SpotQuote,
    DividendEvent,
    RateCurve,
    CedroMarketDataProvider,
    SimulatedMarketDataProvider,
    ProviderManager,
    get_provider_manager,
)

__all__ = [
    # Config classes
    "DerivativesConfig",
    "PCPConfig",
    "FSTConfig",
    "LiquidityScore",
    "ActiveStatus",
    "B3Fees",
    "PromotionDemotionRules",
    "get_config",
    "reload_config",
    # Provider base classes
    "MarketDataProviderBase",
    "SpotProvider",
    "OptionsChainProvider",
    "FuturesProvider",
    "RatesProvider",
    "DividendProvider",
    "ADRProvider",
    # Data classes
    "OptionQuote",
    "FutureQuote",
    "SpotQuote",
    "DividendEvent",
    "RateCurve",
    # Implementations
    "CedroMarketDataProvider",
    "SimulatedMarketDataProvider",
    "ProviderManager",
    "get_provider_manager",
]

__version__ = "1.0.0"
