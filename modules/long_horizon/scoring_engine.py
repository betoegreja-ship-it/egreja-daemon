"""
Scoring Engine for Long Horizon AI Module.

Calculates proprietary 0-100 scores across 7 dimensions for equity assets.
Generates realistic demo scores based on fundamental analysis.

Score Distribution:
  - 85-100: Conviction Buy (Strong conviction)
  - 70-84: Buy/Accumulate (Positive outlook)
  - 55-69: Neutral (Mixed signals)
  - 40-54: Caution (Elevated risks)
  - <40: Avoid (Not recommended)
"""

import logging
from datetime import datetime, date
from decimal import Decimal
import json

logger = logging.getLogger(__name__)

# Dimension Weights (sum = 100%)
DIMENSION_WEIGHTS = {
    'business_quality': 0.25,      # ROE, ROIC, margins, growth, cash, leverage
    'valuation': 0.20,             # P/E, P/B, EV/EBITDA, discount vs history
    'market_strength': 0.15,       # momentum, trend, liquidity, drawdown
    'macro_factors': 0.10,         # interest rates, FX sensitivity, sector exposure
    'options_signal': 0.10,        # IV, skew, risk reversal
    'structural_risk': 0.10,       # volatility, governance, regulatory risk
    'data_reliability': 0.10,      # completeness, consistency
}


def get_realistic_asset_scores():
    """
    Generate realistic demo scores for MVP assets.
    Scores are based on real-world knowledge of these Brazilian companies.

    Returns:
        dict: {ticker: {dimension: score, ...}}
    """
    return {
        'PETR4': {  # Petrobras - Oil & Gas Giant
            'business_quality': 72,      # Strong cash generation, dividends, but cyclical
            'valuation': 78,             # Attractive P/E, discount to peers
            'market_strength': 75,       # Good momentum, high liquidity
            'macro_factors': 60,         # FX sensitive, commodity prices volatile
            'options_signal': 65,        # Moderate IV, normal skew
            'structural_risk': 70,       # Government influence, regulatory
            'data_reliability': 88,      # Excellent data quality
        },
        'VALE3': {  # Vale - Commodities/Mining
            'business_quality': 75,      # Strong ROIC, world-class assets
            'valuation': 72,             # Reasonable P/B, depends on commodity cycle
            'market_strength': 68,       # Good liquidity, commodity-driven momentum
            'macro_factors': 55,         # Very sensitive to global commodity prices, China
            'options_signal': 62,        # Higher IV due to commodity volatility
            'structural_risk': 75,       # Good governance but cycle/regulatory risk
            'data_reliability': 87,      # Good data quality
        },
        'ITUB4': {  # Itaú Unibanco - Banking
            'business_quality': 82,      # Strong ROE, efficient operations, diversified
            'valuation': 75,             # Reasonable P/E for quality bank
            'market_strength': 76,       # Stable, good technical setup
            'macro_factors': 68,         # Interest rate sensitive but good hedging
            'options_signal': 70,        # Normal IV, stable skew
            'structural_risk': 78,       # Good governance, regulated sector
            'data_reliability': 90,      # Excellent data quality
        },
        'BBDC4': {  # Banco Bradesco - Banking
            'business_quality': 80,      # Strong fundamentals, consistent profitability
            'valuation': 74,             # Good value, lower cost base
            'market_strength': 73,       # Stable dividend payer, good technicals
            'macro_factors': 67,         # Interest rate sensitive
            'options_signal': 68,        # Normal IV
            'structural_risk': 76,       # Good governance, systemic importance
            'data_reliability': 89,      # Excellent data quality
        },
        'BBAS3': {  # Banco do Brasil - Banking (State-owned)
            'business_quality': 73,      # Solid fundamentals, but less efficient
            'valuation': 70,             # Reasonable, discount to private banks
            'market_strength': 68,       # Good dividend, subject to government policy
            'macro_factors': 62,         # Government risk, policy uncertainty
            'options_signal': 63,        # Moderate IV, higher skew due to policy risk
            'structural_risk': 65,       # Government influence, regulatory pressure
            'data_reliability': 85,      # Good data quality
        },
        'ABEV3': {  # Ambev - Beverages/Consumer
            'business_quality': 79,      # Strong brands, consistent margins, dividend
            'valuation': 76,             # Reasonable P/E, earnings stable
            'market_strength': 74,       # Good momentum, market leader
            'macro_factors': 70,         # Lower macro sensitivity (defensive)
            'options_signal': 72,        # Normal IV, stable signal
            'structural_risk': 77,       # Strong moat, lower governance risk
            'data_reliability': 88,      # Excellent data quality
        },
        'B3SA3': {  # B3 - Exchange Operator
            'business_quality': 78,      # High margins, recurring revenue, solid growth
            'valuation': 72,             # Fair P/E for quality
            'market_strength': 71,       # Stable with portfolio diversification
            'macro_factors': 73,         # Low macro sensitivity (structural revenue)
            'options_signal': 70,        # Normal IV, stable
            'structural_risk': 80,       # Regulated monopoly, excellent governance
            'data_reliability': 89,      # Excellent data quality
        },
        'BOVA11': {  # Bovespa Index ETF
            'business_quality': 72,      # Diversified portfolio (Ibovespa constituents)
            'valuation': 70,             # Market valuation
            'market_strength': 72,       # Mirrors market trend
            'macro_factors': 65,         # Brazil macro exposure (diversified)
            'options_signal': 68,        # Market IV level
            'structural_risk': 72,       # Diversification lowers risk
            'data_reliability': 90,      # Excellent data quality (ETF)
        },
    }


def calculate_total_score(dimension_scores: dict) -> tuple:
    """
    Calculate total score from 7 dimension scores.

    Args:
        dimension_scores: {dimension_name: score, ...}

    Returns:
        (total_score, conviction_level)
    """
    # Weighted average of all dimensions
    total = sum(
        dimension_scores.get(dim, 0) * DIMENSION_WEIGHTS.get(dim, 0)
        for dim in DIMENSION_WEIGHTS.keys()
    )

    # Determine conviction level
    if total >= 85:
        conviction = "Conviction Buy"
    elif total >= 70:
        conviction = "Buy/Accumulate"
    elif total >= 55:
        conviction = "Neutral"
    elif total >= 40:
        conviction = "Caution"
    else:
        conviction = "Avoid"

    return round(total, 2), conviction


def generate_demo_scores(score_date: date = None) -> dict:
    """
    Generate demo scores for all MVP assets.

    Args:
        score_date: Date for scores (defaults to today)

    Returns:
        {ticker: {
            'total_score': float,
            'conviction': str,
            'dimension_scores': {dim: score, ...},
            'subscores': json
        }}
    """
    if score_date is None:
        score_date = date.today()

    asset_scores = get_realistic_asset_scores()
    results = {}

    for ticker, dimensions in asset_scores.items():
        total_score, conviction = calculate_total_score(dimensions)

        results[ticker] = {
            'ticker': ticker,
            'score_date': score_date.isoformat(),
            'total_score': float(total_score),
            'conviction': conviction,
            'business_quality': float(dimensions['business_quality']),
            'valuation': float(dimensions['valuation']),
            'market_strength': float(dimensions['market_strength']),
            'macro_factors': float(dimensions['macro_factors']),
            'options_signal': float(dimensions['options_signal']),
            'structural_risk': float(dimensions['structural_risk']),
            'data_reliability': float(dimensions['data_reliability']),
            'subscores': json.dumps(dimensions),
            'model_version': 'v1.0',
        }

    return results


def get_conviction_color(conviction: str) -> str:
    """Get color code for conviction level (for UI)."""
    colors = {
        'Conviction Buy': '#27ae60',    # Green
        'Buy/Accumulate': '#3498db',   # Blue
        'Neutral': '#f39c12',           # Orange
        'Caution': '#e74c3c',           # Red
        'Avoid': '#c0392b',             # Dark Red
    }
    return colors.get(conviction, '#95a5a6')


def rank_assets(scores: dict) -> list:
    """
    Rank assets by total score.

    Args:
        scores: {ticker: score_data, ...}

    Returns:
        List of (ticker, score) sorted by score descending
    """
    ranked = sorted(
        [(t, s['total_score']) for t, s in scores.items()],
        key=lambda x: x[1],
        reverse=True
    )
    return ranked


if __name__ == '__main__':
    # Demo: Generate and print scores
    scores = generate_demo_scores()
    print("\n=== Long Horizon Scoring Engine - Demo Scores ===\n")
    for ticker, data in scores.items():
        print(f"{ticker}: {data['total_score']:.2f} ({data['conviction']})")
    print("\n")
