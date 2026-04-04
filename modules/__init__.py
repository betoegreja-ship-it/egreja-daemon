"""Egreja Daemon Pure Modules — Phase 1-3 Modularization

This package contains pure, parameterized modules extracted from api_server.py:

Phase 1 (pure math & config):
  - trading_config: Environment variables, constants, symbol lists
  - market_calendar: Holidays, timezones, market open functions
  - feature_engine: Feature extraction, bucketing, ATR
  - fees: Binance/B3/NYSE fee calculations
  - learning_engine: Pattern/factor stats, confidence, risk multiplier

Phase 2 (data persistence):
  - database: MySQL connection pool management
  - signal_tracking: Signal events, attribution, outcomes, shadow decisions
  - ledger: Capital ledger, reconciliation, calibration persistence

Phase 3 (data fetchers):
  - stock_fetcher: Stock price fetching (Polygon, brapi, FMP, Yahoo)
  - crypto_fetcher: Crypto price fetching and analysis (Binance)

Phase 4 (execution & arbitrage):
  - execution: Trade monitoring, stock execution worker, crypto auto-trading
  - arbitrage: Arbi scan/monitor/learning loops, spread calculation

Phase 5 (API routes):
  - api_routes: All 78 Flask route handlers as a Blueprint

Phase 6 (derivatives autonomous execution):
  - derivatives/capital: Capital allocation, daily loss limits, reconciliation
  - derivatives/position_sizing: Dynamic sizing (Kelly + liquidity + Greeks margin)
  - derivatives/deriv_execution: Multi-leg order execution with legging risk
  - derivatives/monitoring: Real-time P&L (Greeks-based), exit triggers
  - derivatives/learning: Adaptive confidence calibration, slippage prediction

All modules are designed to be:
  - PURE: No global state access
  - PARAMETERIZED: All dependencies passed as parameters
  - TESTABLE: Can be unit tested independently
  - THREAD-SAFE: Use locks for shared resources
"""

__version__ = "1.0.0-phase6"
