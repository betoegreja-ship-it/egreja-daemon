"""
Egreja Long Horizon AI Investment Module

Proprietary long-term investment analysis engine with:
  - Proprietary 0-100 score across 7 dimensions
  - Explainable investment theses in Portuguese
  - 3 model portfolios (Quality Brasil, Dividendos + Proteção, Brasil + EUA)
  - Backtest results vs Ibovespa and CDI benchmarks
  - Capital tracking (R$ 7,000,000 initial)
  - Win rate and P&L tracking

Architecture:
  - schema.py: 10 MySQL tables for assets, scores, theses, portfolios, backtests
  - scoring_engine.py: 7-dimension proprietary scoring algorithm with conviction levels
  - thesis_engine.py: Portuguese investment theses with hedge suggestions
  - portfolio_engine.py: Three model portfolios with realistic allocations
  - backtest_engine.py: 12-month backtest results with monthly attribution
  - endpoints.py: 13 Flask API endpoints for dashboard integration

MVP Assets (8 stocks):
  - PETR4 (Petrobras)
  - VALE3 (Vale)
  - ITUB4 (Itaú Unibanco)
  - BBDC4 (Bradesco)
  - BBAS3 (Banco do Brasil)
  - ABEV3 (Ambev)
  - B3SA3 (B3 - Bolsa de Valores)
  - BOVA11 (Bovespa ETF)

All data is demo/realistic, not connected to real data providers (OpLab, BRAPI, Polygon).
Designed for dashboard visualization and strategy analysis.
"""

import logging

logger = logging.getLogger('egreja.long_horizon')

# Import public functions
from .scoring_engine import (
    generate_demo_scores,
    rank_assets,
    get_conviction_color,
    DIMENSION_WEIGHTS,
)

from .thesis_engine import (
    generate_thesis_for_ticker,
)

from .portfolio_engine import (
    get_model_portfolios,
    get_all_portfolios_summary,
    calculate_portfolio_positions,
    compare_portfolios,
    INITIAL_CAPITAL,
)

from .backtest_engine import (
    get_all_backtest_results,
    generate_backtest_results,
)

from .schema import (
    create_long_horizon_tables,
)

from .endpoints import (
    create_long_horizon_blueprint,
)

__all__ = [
    # Scoring
    'generate_demo_scores',
    'rank_assets',
    'get_conviction_color',
    'DIMENSION_WEIGHTS',
    # Theses
    'generate_thesis_for_ticker',
    # Portfolios
    'get_model_portfolios',
    'get_all_portfolios_summary',
    'calculate_portfolio_positions',
    'compare_portfolios',
    'INITIAL_CAPITAL',
    # Backtests
    'get_all_backtest_results',
    'generate_backtest_results',
    # Schema
    'create_long_horizon_tables',
    # Endpoints
    'create_long_horizon_blueprint',
]

__version__ = '1.0.0'
