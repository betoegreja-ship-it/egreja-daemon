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

Infrastructure (Phase 1):
  - config.py: Environment-driven configuration, strategy params
  - providers.py: Market data providers (OpLab, Cedro, Simulated)
  - services.py: Greeks calculator, IV engine, caches, calibration
  - liquidity.py: 7-dimension liquidity scoring, promotion engine
  - schema.py: 13 MySQL tables for derivatives infrastructure
  - endpoints.py: 17 Flask API endpoints

Autonomous Execution (Phase 2):
  - capital.py: Capital allocation, daily loss limits, reconciliation
  - position_sizing.py: Dynamic sizing (Kelly + liquidity + Greeks margin)
  - deriv_execution.py: Multi-leg order execution with legging risk
  - monitoring.py: Real-time P&L (Greeks-based), exit triggers
  - learning.py: Adaptive confidence calibration, slippage prediction
  - strategies.py: 8 scan loops with autonomous execution pipeline

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

__version__ = "2.0.0-autonomous"
