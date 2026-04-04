"""
Scoring Engine for Long Horizon AI Module.

Calculates proprietary 0-100 scores across 7 dimensions for equity assets.
Generates realistic demo scores OR scores from real data normalized from all providers.

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
from typing import Dict, Any, Optional, Tuple

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


# ─── Real Data Scoring ───────────────────────────────────────────────────

def score_from_real_data(profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Score from a unified asset profile (real data from data_ingestion).

    Args:
        profile: Unified profile dict with 7 dimensions

    Returns:
        {
            'ticker': str,
            'score_date': str,
            'total_score': float,
            'conviction': str,
            'business_quality': float,
            'valuation': float,
            'market_strength': float,
            'macro_factors': float,
            'options_signal': float,
            'structural_risk': float,
            'data_reliability': float,
            'subscores': json,
            'model_version': str,
            'data_source': 'real',
        }
    """
    # Extract dimension data from profile
    quality_score = _score_business_quality(profile.get('quality', {}))
    valuation_score = _score_valuation(profile.get('valuation', {}))
    market_score = _score_market_strength(profile.get('market', {}))
    macro_score = _score_macro_factors(profile.get('macro', {}))
    options_score = _score_options_signal(profile.get('options_signal', {}))
    risk_score = _score_structural_risk(profile.get('risk', {}))
    dq_score = _score_data_reliability(profile.get('data_quality', {}))

    dimension_scores = {
        'business_quality': quality_score,
        'valuation': valuation_score,
        'market_strength': market_score,
        'macro_factors': macro_score,
        'options_signal': options_score,
        'structural_risk': risk_score,
        'data_reliability': dq_score,
    }

    total_score, conviction = calculate_total_score(dimension_scores)

    return {
        'ticker': profile.get('ticker', ''),
        'score_date': date.today().isoformat(),
        'total_score': float(total_score),
        'conviction': conviction,
        'business_quality': float(quality_score),
        'valuation': float(valuation_score),
        'market_strength': float(market_score),
        'macro_factors': float(macro_score),
        'options_signal': float(options_score),
        'structural_risk': float(risk_score),
        'data_reliability': float(dq_score),
        'subscores': json.dumps(dimension_scores),
        'model_version': 'v2.0-realdata',
        'data_source': 'real',
        'timestamp': datetime.utcnow().isoformat(),
    }


# ─── Individual Dimension Scorers ────────────────────────────────────────

def _score_business_quality(quality: Dict[str, Any]) -> float:
    """
    Score business quality (ROE, ROIC, margins, growth, cash, leverage).

    Excellent: ROE > 15%, ROIC > 12%, margins stable/growing, negative net debt
    Good: ROE > 10%, ROIC > 8%, healthy margins, moderate debt
    Fair: ROE > 5%, ROIC > 4%, squeezed margins, higher debt
    Poor: ROE < 5%, negative ROIC, declining margins, heavy debt
    """
    score = 50  # Start at neutral

    # ROE scoring (0-25 points)
    roe = quality.get('roe')
    if roe is not None:
        if roe > 0.20:
            score += 25
        elif roe > 0.15:
            score += 22
        elif roe > 0.10:
            score += 18
        elif roe > 0.05:
            score += 10
        elif roe > 0:
            score += 5
        else:
            score -= 10

    # ROIC scoring (0-15 points)
    roic = quality.get('roic')
    if roic is not None:
        if roic > 0.12:
            score += 15
        elif roic > 0.08:
            score += 10
        elif roic > 0.04:
            score += 5
        else:
            score -= 5

    # Net margin scoring (0-10 points)
    net_margin = quality.get('net_margin')
    if net_margin is not None:
        if net_margin > 0.20:
            score += 10
        elif net_margin > 0.10:
            score += 7
        elif net_margin > 0.05:
            score += 3
        elif net_margin > 0:
            score += 1
        else:
            score -= 10

    # Growth scoring (0-15 points)
    growth = quality.get('revenue_growth')
    if growth is not None:
        if growth > 0.15:
            score += 15
        elif growth > 0.10:
            score += 10
        elif growth > 0.05:
            score += 5
        elif growth > 0:
            score += 2
        else:
            score -= 5

    # Debt scoring (0-10 points)
    de = quality.get('debt_to_equity')
    if de is not None:
        if de < 0.5:
            score += 10
        elif de < 1.0:
            score += 7
        elif de < 1.5:
            score += 3
        else:
            score -= 5

    return max(0, min(100, score))


def _score_valuation(valuation: Dict[str, Any]) -> float:
    """
    Score valuation (P/E, P/B, EV/EBITDA, discount vs history).

    Cheap: P/E < 8, P/B < 1, >30% discount to history
    Fair: P/E 8-12, P/B 1-1.5, 0-30% discount
    Expensive: P/E 12-16, P/B 1.5-2, -10-0% discount
    Very Expensive: P/E > 16, P/B > 2, <-10% discount
    """
    score = 50

    # P/E scoring (0-30 points)
    pe = valuation.get('pe')
    if pe and pe > 0:
        if pe < 8:
            score += 30
        elif pe < 12:
            score += 20
        elif pe < 16:
            score += 10
        elif pe < 20:
            score += 2
        else:
            score -= 10

    # P/B scoring (0-20 points)
    pb = valuation.get('pb')
    if pb and pb > 0:
        if pb < 1.0:
            score += 20
        elif pb < 1.5:
            score += 12
        elif pb < 2.0:
            score += 5
        else:
            score -= 5

    # Discount vs history (0-20 points)
    discount = valuation.get('discount_vs_history')
    if discount is not None:
        if discount > 0.30:
            score += 20
        elif discount > 0.20:
            score += 15
        elif discount > 0.10:
            score += 10
        elif discount > 0:
            score += 5
        elif discount > -0.10:
            score += 0
        else:
            score -= 10

    # Dividend yield (0-10 points)
    div_yield = valuation.get('dividend_yield')
    if div_yield and div_yield > 0:
        if div_yield > 0.10:
            score += 10
        elif div_yield > 0.06:
            score += 7
        elif div_yield > 0.03:
            score += 4
        else:
            score += 1

    return max(0, min(100, score))


def _score_market_strength(market: Dict[str, Any]) -> float:
    """
    Score market strength (momentum, trend, liquidity, drawdown).

    Bullish: >15% 12m momentum, above 200-SMA, high volume, RSI 50-70
    Neutral: 0-15% momentum, around 200-SMA, normal volume, RSI 40-60
    Bearish: <0% momentum, below 200-SMA, declining volume, RSI <40
    """
    score = 50

    # 12-month momentum (0-30 points)
    momentum = market.get('momentum_12m')
    if momentum is not None:
        if momentum > 0.30:
            score += 30
        elif momentum > 0.15:
            score += 20
        elif momentum > 0.05:
            score += 10
        elif momentum > -0.05:
            score += 0
        elif momentum > -0.20:
            score -= 10
        else:
            score -= 20

    # Technical setup: price vs SMA (0-20 points)
    price = market.get('price')
    sma200 = market.get('sma_200')
    if price and sma200 and sma200 > 0:
        price_to_sma = price / sma200
        if price_to_sma > 1.10:
            score += 20
        elif price_to_sma > 1.00:
            score += 10
        elif price_to_sma > 0.90:
            score += 0
        else:
            score -= 15

    # RSI (0-20 points)
    rsi = market.get('rsi_14')
    if rsi is not None:
        if 50 <= rsi <= 70:
            score += 15
        elif 40 <= rsi < 50:
            score += 5
        elif 70 < rsi <= 80:
            score += 8
        elif 30 <= rsi < 40:
            score -= 5
        else:
            score -= 15

    # Volume (0-10 points)
    avg_vol = market.get('avg_volume_3m')
    if avg_vol and avg_vol > 1e6:  # > 1M shares
        score += 10
    elif avg_vol and avg_vol > 1e5:
        score += 5

    return max(0, min(100, score))


def _score_macro_factors(macro: Dict[str, Any]) -> float:
    """
    Score macro factors (interest rates, FX, sector exposure).

    Tailwinds: Rates declining, FX favorable, sector strong
    Neutral: Rates stable, FX stable, sector average
    Headwinds: Rates rising, FX unfavorable, sector weak
    """
    score = 50

    # Interest rates (Selic)
    selic = macro.get('selic')
    if selic is not None:
        if selic < 0.08:
            score += 15  # Supportive for equities
        elif selic < 0.10:
            score += 8
        elif selic < 0.12:
            score += 0
        elif selic < 0.14:
            score -= 8
        else:
            score -= 15

    # Inflation (IPCA)
    ipca = macro.get('ipca')
    if ipca is not None:
        if ipca < 0.03:
            score += 10
        elif ipca < 0.05:
            score += 5
        elif ipca < 0.08:
            score += 0
        else:
            score -= 10

    # Sector sensitivity (estimated)
    selic_sens = macro.get('sector_sensitivity_selic', 0)
    if selic_sens < -0.3:  # Negative = benefits from lower rates
        score += 10
    elif selic_sens < 0:
        score += 5

    return max(0, min(100, score))


def _score_options_signal(options: Dict[str, Any]) -> float:
    """
    Score options market signals (IV, skew, risk reversal, put/call).

    Bullish: Low IV percentile, negative skew, low put/call, negative RR
    Neutral: Mid IV, normal skew, balanced put/call
    Bearish: High IV, positive skew, high put/call, positive RR
    """
    score = 50

    # IV percentile (0-25 points)
    iv_pct = options.get('iv_percentile')
    if iv_pct is not None:
        if iv_pct < 30:
            score += 25  # Low IV = complacency opportunity
        elif iv_pct < 50:
            score += 12
        elif iv_pct < 70:
            score += 0
        else:
            score -= 15  # High IV = fear/stress

    # Skew (0-20 points) - negative skew = bullish
    skew = options.get('skew_25d')
    if skew is not None:
        if skew < -0.10:
            score += 20
        elif skew < -0.05:
            score += 10
        elif skew < 0.05:
            score += 0
        elif skew < 0.10:
            score -= 8
        else:
            score -= 15

    # Put/Call ratio (0-20 points) - low ratio = bullish
    pc_ratio = options.get('put_call_ratio')
    if pc_ratio is not None:
        if pc_ratio < 0.8:
            score += 20
        elif pc_ratio < 1.0:
            score += 10
        elif pc_ratio < 1.2:
            score += 0
        else:
            score -= 10

    # Hedge cost (0-15 points) - high cost = expensive = bearish
    hedge = options.get('hedge_cost_3m_10pct')
    if hedge is not None:
        if hedge < 0.02:
            score += 15  # Cheap insurance
        elif hedge < 0.03:
            score += 8
        elif hedge < 0.05:
            score += 0
        else:
            score -= 10

    return max(0, min(100, score))


def _score_structural_risk(risk: Dict[str, Any]) -> float:
    """
    Score structural risk (volatility, governance, regulatory).

    Low Risk: HV < 20%, good governance, low regulatory
    Moderate: HV 20-40%, normal governance
    High Risk: HV > 40%, governance concerns, high regulatory
    """
    score = 50

    # Historical volatility (0-30 points)
    hv = risk.get('volatility_30d')
    if hv is not None:
        if hv < 0.15:
            score += 30
        elif hv < 0.25:
            score += 20
        elif hv < 0.40:
            score += 5
        else:
            score -= 15

    # Governance risk (0-20 points)
    gov_risk = risk.get('governance_risk', 'moderate')
    if gov_risk == 'low':
        score += 20
    elif gov_risk == 'moderate':
        score += 10
    elif gov_risk == 'high':
        score -= 10
    else:
        score -= 20

    # Regulatory risk (0-20 points)
    reg_risk = risk.get('regulatory_risk', 'moderate')
    if reg_risk == 'low':
        score += 20
    elif reg_risk == 'moderate':
        score += 10
    elif reg_risk == 'high':
        score -= 10
    else:
        score -= 20

    # State ownership (0-10 points) - generally negative
    state_owned = risk.get('state_owned', False)
    if state_owned:
        score -= 10
    else:
        score += 5

    return max(0, min(100, score))


def _score_data_reliability(dq: Dict[str, Any]) -> float:
    """
    Score data reliability (provider availability, completeness, consistency).
    """
    score = 50

    # Provider availability (0-30 points)
    brapi_avail = dq.get('brapi_available', False)
    oplab_avail = dq.get('oplab_available', False)
    polygon_avail = dq.get('polygon_available', False)

    providers = sum([brapi_avail, oplab_avail, polygon_avail])
    if providers == 3:
        score += 30
    elif providers == 2:
        score += 15
    elif providers == 1:
        score += 5
    else:
        score -= 30

    # Fundamentals completeness (0-25 points)
    fund_complete = dq.get('fundamentals_complete', 0)
    score += fund_complete * 25

    # Options data completeness (0-25 points)
    opts_complete = dq.get('options_data_complete', 0)
    score += opts_complete * 25

    # Price consistency (0-20 points)
    price_consistency = dq.get('price_consistency', 0.9)
    if price_consistency > 0.99:
        score += 20
    elif price_consistency > 0.95:
        score += 12
    elif price_consistency > 0.90:
        score += 5
    else:
        score -= 10

    return max(0, min(100, score))


if __name__ == '__main__':
    # Demo: Generate and print scores
    scores = generate_demo_scores()
    print("\n=== Long Horizon Scoring Engine - Demo Scores ===\n")
    for ticker, data in scores.items():
        print(f"{ticker}: {data['total_score']:.2f} ({data['conviction']})")
    print("\n")
