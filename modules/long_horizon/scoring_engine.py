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
    Generate realistic demo scores for all 87 assets (8 MVP + 79 additional).
    Scores are based on real-world knowledge of companies across B3 (Brazil) and US markets.

    MVP assets (existing, well-tuned) remain unchanged.
    New assets scored across 7 dimensions based on sector fundamentals.

    Returns:
        dict: {ticker: {dimension: score, ...}}
    """
    return {
        # ═══════════════════════════════════════════════════════════════════
        # MVP ASSETS (8) - EXISTING, WELL-TUNED SCORES
        # ═══════════════════════════════════════════════════════════════════
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

        # ═══════════════════════════════════════════════════════════════════
        # B3 ADDITIONAL STOCKS (47)
        # ═══════════════════════════════════════════════════════════════════

        # ─── B3 Blue Chips: Manufacturing, Industrial ───
        'WEGE3': {  # WEG - Electric Motors & Equipment
            'business_quality': 78,      # Strong ROE, efficient operations, global expansion
            'valuation': 72,             # Fair P/E, solid earnings growth
            'market_strength': 74,       # Good momentum, industrial leader
            'macro_factors': 65,         # Some industrial cycle exposure
            'options_signal': 68,        # Moderate IV, stable signal
            'structural_risk': 76,       # Good governance, lower regulatory risk
            'data_reliability': 87,      # Good data quality
        },
        'SUZB3': {  # Suzano - Pulp & Paper (Blue Chip)
            'business_quality': 76,      # Strong cash generation, integrated operations
            'valuation': 74,             # Reasonable P/E, commodity exposure
            'market_strength': 72,       # Good technical setup, cyclical
            'macro_factors': 58,         # Commodity prices, global demand sensitive
            'options_signal': 64,        # Higher IV due to commodity cycle
            'structural_risk': 74,       # Good governance, environmental concerns
            'data_reliability': 86,      # Good data quality
        },
        'EMBR3': {  # Embraer - Aerospace & Defense
            'business_quality': 78,      # Strong engineering, global presence, orders
            'valuation': 71,             # Reasonable P/E, cyclical
            'market_strength': 73,       # Stable momentum, cyclical
            'macro_factors': 62,         # Global economic cycle, FX sensitive
            'options_signal': 66,        # Moderate IV
            'structural_risk': 75,       # Good governance, export dependent
            'data_reliability': 85,      # Good data quality
        },

        # ─── B3 Utilities: Defensive, Dividend-Paying ───
        'CMIG4': {  # Cemig - Electricity Distribution (Utility)
            'business_quality': 70,      # Regulated utility, stable cash flows
            'valuation': 72,             # Fair P/E for utility
            'market_strength': 68,       # Stable, low volatility
            'macro_factors': 72,         # Defensive (low economic sensitivity)
            'options_signal': 66,        # Low IV, stable
            'structural_risk': 74,       # Regulated, government influence risk
            'data_reliability': 86,      # Good data quality
        },
        'CPLE6': {  # Copel - Electricity Distribution (Utility)
            'business_quality': 68,      # Regulated utility, stable operations
            'valuation': 70,             # Fair utility valuation
            'market_strength': 66,       # Stable, defensive
            'macro_factors': 73,         # Defensive sector
            'options_signal': 65,        # Low IV
            'structural_risk': 72,       # Regulated utility, government risk
            'data_reliability': 84,      # Good data quality
        },
        'EQTL3': {  # Equatorial Energia - Electricity Distribution
            'business_quality': 67,      # Regulated utility, growing footprint
            'valuation': 71,             # Fair utility P/E
            'market_strength': 67,       # Stable dividend payer
            'macro_factors': 72,         # Defensive
            'options_signal': 64,        # Low IV
            'structural_risk': 71,       # Regulated, government involvement
            'data_reliability': 83,      # Adequate data quality
        },
        'TAEE11': {  # Taesa - Transmission (Utility)
            'business_quality': 72,      # Regulated transmission, stable cash
            'valuation': 73,             # Fair utility valuation
            'market_strength': 69,       # Stable, low volatility
            'macro_factors': 73,         # Defensive, essential service
            'options_signal': 65,        # Low IV, stable
            'structural_risk': 75,       # Regulated, government risk
            'data_reliability': 85,      # Good data quality
        },
        'CPFE3': {  # CPFL Energia - Electricity Distribution
            'business_quality': 70,      # Regulated utility, regional player
            'valuation': 71,             # Fair P/E
            'market_strength': 67,       # Stable, defensive
            'macro_factors': 72,         # Defensive sector
            'options_signal': 64,        # Low IV
            'structural_risk': 72,       # Regulated utility, government risk
            'data_reliability': 84,      # Good data quality
        },
        'EGIE3': {  # EDP Energias - Electricity Distribution
            'business_quality': 69,      # Regulated utility, Portuguese-owned
            'valuation': 70,             # Fair utility P/E
            'market_strength': 66,       # Stable, defensive
            'macro_factors': 72,         # Defensive, essential
            'options_signal': 64,        # Low IV
            'structural_risk': 71,       # Regulated, FX exposure (Euro parent)
            'data_reliability': 83,      # Adequate data quality
        },
        'ENEV3': {  # Eneva - Energy Generation
            'business_quality': 65,      # Energy generation, thermal & renewable mix
            'valuation': 68,             # Reasonable P/E
            'market_strength': 65,       # Moderate volatility
            'macro_factors': 70,         # Some defensive characteristics
            'options_signal': 63,        # Moderate IV
            'structural_risk': 70,       # Regulatory & commodity exposure
            'data_reliability': 82,      # Adequate data quality
        },
        'SBSP3': {  # Sabesp - Water & Sanitation (Utility)
            'business_quality': 71,      # Water utility, essential service
            'valuation': 72,             # Fair utility valuation
            'market_strength': 68,       # Stable, defensive
            'macro_factors': 73,         # Defensive, essential service
            'options_signal': 65,        # Low IV
            'structural_risk': 73,       # Regulated, government influence
            'data_reliability': 85,      # Good data quality
        },
        'AESB3': {  # AES Brasil - Electricity Distribution
            'business_quality': 68,      # Regulated utility, growth in renewables
            'valuation': 70,             # Fair utility P/E
            'market_strength': 66,       # Stable, defensive
            'macro_factors': 72,         # Defensive, essential
            'options_signal': 64,        # Low IV
            'structural_risk': 71,       # Regulated, government risk
            'data_reliability': 83,      # Adequate data quality
        },

        # ─── B3 Banks (Additional) ───
        'BPAC11': {  # Banco Bradesco (preferred) - Banking
            'business_quality': 79,      # Strong bank fundamentals
            'valuation': 74,             # Good value for bank
            'market_strength': 72,       # Stable, good dividend
            'macro_factors': 67,         # Interest rate sensitive
            'options_signal': 68,        # Normal IV
            'structural_risk': 75,       # Good governance, systemic
            'data_reliability': 88,      # Good data quality
        },
        'BBDC3': {  # Banco Bradesco (common) - Banking
            'business_quality': 79,      # Strong fundamentals
            'valuation': 74,             # Good value
            'market_strength': 72,       # Stable, dividend payer
            'macro_factors': 67,         # Interest rate sensitive
            'options_signal': 68,        # Normal IV
            'structural_risk': 75,       # Good governance
            'data_reliability': 88,      # Good data quality
        },
        'BBSE3': {  # Banco Santander - Banking
            'business_quality': 76,      # Large bank, Spanish-owned, solid ROE
            'valuation': 72,             # Fair P/E for bank
            'market_strength': 70,       # Stable, good technical
            'macro_factors': 66,         # Interest rate & FX sensitive
            'options_signal': 67,        # Normal IV
            'structural_risk': 74,       # Good governance, parent support
            'data_reliability': 87,      # Good data quality
        },
        'CXSE3': {  # Caixa Seguridade - Insurance/Banking Services
            'business_quality': 72,      # Insurance subsidiary, growing
            'valuation': 70,             # Fair valuation
            'market_strength': 68,       # Moderate momentum
            'macro_factors': 66,         # Economic cycle sensitive
            'options_signal': 65,        # Moderate IV
            'structural_risk': 72,       # Government backing (Caixa)
            'data_reliability': 84,      # Good data quality
        },

        # ─── B3 Healthcare ───
        'RADL3': {  # Raia Drogasil - Pharmacy Retail
            'business_quality': 75,      # Strong market position, consolidation
            'valuation': 73,             # Fair P/E for retail pharmacy
            'market_strength': 71,       # Good momentum, consolidation story
            'macro_factors': 69,         # Defensive (healthcare demand)
            'options_signal': 67,        # Moderate IV
            'structural_risk': 74,       # Good governance, growth through M&A
            'data_reliability': 86,      # Good data quality
        },
        'HAPV3': {  # Hapvida - Healthcare Provider
            'business_quality': 68,      # Healthcare services, growing consolidation
            'valuation': 65,             # Reasonable P/E
            'market_strength': 67,       # Moderate momentum
            'macro_factors': 68,         # Defensive sector
            'options_signal': 64,        # Moderate IV
            'structural_risk': 70,       # Regulatory & M&A risks
            'data_reliability': 82,      # Adequate data quality
        },
        'HYPE3': {  # Hypera - Pharmaceuticals
            'business_quality': 72,      # Pharmaceutical company, stable revenue
            'valuation': 70,             # Fair P/E
            'market_strength': 69,       # Stable, defensive
            'macro_factors': 69,         # Defensive healthcare
            'options_signal': 66,        # Moderate IV
            'structural_risk': 73,       # Regulatory & competition risks
            'data_reliability': 85,      # Good data quality
        },

        # ─── B3 Food & Beverages ───
        'BRFS3': {  # BRF - Food (Chicken, Pork)
            'business_quality': 65,      # Food production, volatile commodity input
            'valuation': 68,             # Fair but cyclical
            'market_strength': 66,       # Moderate momentum, cyclical
            'macro_factors': 60,         # Commodity & FX sensitive
            'options_signal': 62,        # Higher IV, volatile
            'structural_risk': 68,       # Regulatory & commodity risk
            'data_reliability': 84,      # Good data quality
        },
        'MRFG3': {  # Marfrig - Food (Beef)
            'business_quality': 62,      # Commodity-heavy, volatile
            'valuation': 65,             # Low P/E, volatile valuation
            'market_strength': 63,       # Volatile momentum
            'macro_factors': 55,         # Very commodity & FX sensitive
            'options_signal': 60,        # Higher IV
            'structural_risk': 65,       # Commodity & regulatory risk
            'data_reliability': 82,      # Adequate data quality
        },
        'JBSS3': {  # JBS - Food (Diversified Meat)
            'business_quality': 63,      # Diversified food, commodity exposure
            'valuation': 66,             # Cyclical valuation
            'market_strength': 64,       # Volatile, cyclical
            'macro_factors': 56,         # Commodity & FX sensitive
            'options_signal': 61,        # Higher IV
            'structural_risk': 66,       # Commodity, regulatory, governance concerns
            'data_reliability': 83,      # Adequate data quality
        },
        'MDIA3': {  # M. Dias Branco - Food (Biscuits/Pasta)
            'business_quality': 66,      # Consumer staples, stable brands
            'valuation': 68,             # Fair P/E
            'market_strength': 65,       # Moderate momentum
            'macro_factors': 65,         # Lower macro sensitivity
            'options_signal': 63,        # Moderate IV
            'structural_risk': 69,       # Regulatory & competition
            'data_reliability': 83,      # Adequate data quality
        },

        # ─── B3 Oil & Fuel Distribution ───
        'VBBR3': {  # Vibra Energia - Fuel Distribution
            'business_quality': 68,      # Fuel logistics, stable cash flow
            'valuation': 71,             # Fair P/E
            'market_strength': 69,       # Stable momentum
            'macro_factors': 64,         # Commodity & FX sensitive
            'options_signal': 65,        # Moderate IV
            'structural_risk': 70,       # Regulatory & competition
            'data_reliability': 84,      # Good data quality
        },
        'UGPA3': {  # Ultrapar - Fuel Distribution & Logistics
            'business_quality': 71,      # Diversified (fuel, chemicals, logistics)
            'valuation': 70,             # Fair P/E
            'market_strength': 70,       # Good momentum
            'macro_factors': 65,         # Economic cycle sensitive
            'options_signal': 66,        # Moderate IV
            'structural_risk': 72,       # Good governance, competition risk
            'data_reliability': 85,      # Good data quality
        },
        'CSAN3': {  # Cosan - Fuel & Sugar/Ethanol
            'business_quality': 70,      # Diversified energy & commodities
            'valuation': 69,             # Fair P/E
            'market_strength': 69,       # Moderate momentum
            'macro_factors': 58,         # Sugar, ethanol, commodity cycle
            'options_signal': 63,        # Moderate IV
            'structural_risk': 70,       # Commodity & regulatory risk
            'data_reliability': 84,      # Good data quality
        },
        'PRIO3': {  # Petrorio - Oil & Gas Exploration
            'business_quality': 66,      # Oil exploration, execution risk
            'valuation': 65,             # Volatile valuation
            'market_strength': 64,       # Volatile momentum
            'macro_factors': 52,         # Very commodity & oil price sensitive
            'options_signal': 60,        # Higher IV
            'structural_risk': 65,       # Commodity & operational risk
            'data_reliability': 80,      # Adequate data quality
        },
        'RECV3': {  # Recôncavo Energia - Oil Exploration
            'business_quality': 63,      # Oil exploration, high risk
            'valuation': 62,             # Volatile
            'market_strength': 61,       # Volatile momentum
            'macro_factors': 50,         # Very oil price sensitive
            'options_signal': 58,        # Higher IV
            'structural_risk': 62,       # High operational & commodity risk
            'data_reliability': 78,      # Adequate data quality
        },

        # ─── B3 Commodities: Metals, Mining ───
        'GGBR4': {  # Gerdau - Steel
            'business_quality': 68,      # Steel producer, commodity cyclical
            'valuation': 66,             # Low P/E, cyclical
            'market_strength': 65,       # Cyclical momentum
            'macro_factors': 55,         # Very cycle & commodity sensitive
            'options_signal': 61,        # Higher IV
            'structural_risk': 68,       # Commodity & regulatory risk
            'data_reliability': 84,      # Good data quality
        },
        'CSNA3': {  # CSN - Steel
            'business_quality': 65,      # Steel, commodity producer
            'valuation': 64,             # Low P/E, volatile
            'market_strength': 62,       # Volatile momentum
            'macro_factors': 52,         # Very commodity sensitive
            'options_signal': 59,        # Higher IV
            'structural_risk': 66,       # Commodity & leverage risk
            'data_reliability': 83,      # Adequate data quality
        },
        'GOAU4': {  # Gerdau Preferred - Steel
            'business_quality': 68,      # Steel (preferred), commodity
            'valuation': 66,             # Low P/E, cyclical
            'market_strength': 65,       # Cyclical momentum
            'macro_factors': 55,         # Commodity sensitive
            'options_signal': 61,        # Higher IV
            'structural_risk': 68,       # Commodity & regulatory
            'data_reliability': 84,      # Good data quality
        },
        'USIM5': {  # Usiminas - Steel
            'business_quality': 64,      # Steel producer, volatile
            'valuation': 63,             # Low P/E, highly cyclical
            'market_strength': 62,       # Volatile momentum
            'macro_factors': 50,         # Very commodity sensitive
            'options_signal': 58,        # Higher IV
            'structural_risk': 65,       # Commodity & leverage risk
            'data_reliability': 82,      # Adequate data quality
        },
        'CMIN3': {  # Cosan Mineração - Mining
            'business_quality': 62,      # Mining operations, commodity exposure
            'valuation': 61,             # Volatile valuation
            'market_strength': 60,       # Volatile momentum
            'macro_factors': 48,         # Very commodity & China cycle
            'options_signal': 57,        # Higher IV
            'structural_risk': 63,       # High commodity & operational risk
            'data_reliability': 80,      # Adequate data quality
        },
        'KLBN11': {  # Klabin - Pulp & Paper
            'business_quality': 69,      # Integrated pulp/paper, commodity
            'valuation': 68,             # Fair P/E, cyclical
            'market_strength': 67,       # Cyclical momentum
            'macro_factors': 56,         # Commodity & global demand
            'options_signal': 62,        # Higher IV
            'structural_risk': 70,       # Commodity & environmental risk
            'data_reliability': 84,      # Good data quality
        },

        # ─── B3 Retail & Consumer ───
        'LREN3': {  # Lojas Renner - Department Stores
            'business_quality': 62,      # Retail, consumer cycle sensitive
            'valuation': 64,             # Fair P/E, cyclical
            'market_strength': 62,       # Moderate momentum, volatile
            'macro_factors': 60,         # Economic cycle sensitive
            'options_signal': 61,        # Moderate IV
            'structural_risk': 66,       # Retail & economic cycle risk
            'data_reliability': 83,      # Adequate data quality
        },
        'MGLU3': {  # Magazine Luiza - E-commerce & Retail
            'business_quality': 50,      # High-growth but risky, execution dependent
            'valuation': 55,             # Expensive relative to current profitability
            'market_strength': 58,       # Volatile momentum, growth story
            'macro_factors': 58,         # Economic cycle sensitive
            'options_signal': 60,        # Higher IV
            'structural_risk': 55,       # High leverage & operational risk
            'data_reliability': 82,      # Adequate data quality
        },
        'CASH3': {  # Casas Bahia - Retail
            'business_quality': 55,      # Retail, economic cycle sensitive
            'valuation': 58,             # Fair to expensive
            'market_strength': 56,       # Volatile momentum
            'macro_factors': 58,         # Economic cycle sensitive
            'options_signal': 59,        # Moderate IV
            'structural_risk': 60,       # Retail & economic risk
            'data_reliability': 81,      # Adequate data quality
        },
        'TOTS3': {  # Totvs - Software (Retail Systems)
            'business_quality': 70,      # Software company, recurring revenue
            'valuation': 68,             # Fair P/E for SaaS
            'market_strength': 69,       # Good momentum, growth story
            'macro_factors': 65,         # Lower macro sensitivity
            'options_signal': 66,        # Moderate IV
            'structural_risk': 71,       # Good governance, tech risk
            'data_reliability': 84,      # Good data quality
        },
        'AMER3': {  # Americanas - Retail & E-commerce
            'business_quality': 48,      # Retail/e-commerce, restructuring
            'valuation': 52,             # Distressed valuation
            'market_strength': 50,       # Volatile, turnaround story
            'macro_factors': 58,         # Economic cycle sensitive
            'options_signal': 57,        # Higher IV
            'structural_risk': 52,       # High execution & financial risk
            'data_reliability': 80,      # Adequate data quality
        },

        # ─── B3 Tech & Software ───
        'SMFT3': {  # Smartfit - Fitness Clubs
            'business_quality': 58,      # Fitness services, cyclical
            'valuation': 62,             # Fair P/E
            'market_strength': 60,       # Moderate momentum
            'macro_factors': 62,         # Economic cycle sensitive
            'options_signal': 60,        # Moderate IV
            'structural_risk': 63,       # Operational & economic risk
            'data_reliability': 81,      # Adequate data quality
        },
        'MULT3': {  # Multiplan - Shopping Centers
            'business_quality': 64,      # Shopping centers, stable revenue
            'valuation': 66,             # Fair P/E
            'market_strength': 65,       # Stable momentum
            'macro_factors': 62,         # Economic cycle sensitive
            'options_signal': 62,        # Moderate IV
            'structural_risk': 67,       # Real estate & economic risk
            'data_reliability': 83,      # Adequate data quality
        },
        'ALOS3': {  # Aliansce - Shopping Centers
            'business_quality': 62,      # Shopping centers, leverage
            'valuation': 64,             # Fair P/E
            'market_strength': 62,       # Moderate momentum
            'macro_factors': 60,         # Economic cycle sensitive
            'options_signal': 61,        # Moderate IV
            'structural_risk': 63,       # Leverage & economic risk
            'data_reliability': 82,      # Adequate data quality
        },

        # ─── B3 Telecommunications ───
        'VIVT3': {  # Vivo - Telecom
            'business_quality': 72,      # Incumbent telecom, cash generation
            'valuation': 70,             # Fair P/E
            'market_strength': 69,       # Stable momentum
            'macro_factors': 68,         # Lower macro sensitivity
            'options_signal': 67,        # Moderate IV
            'structural_risk': 73,       # Regulatory & competition risk
            'data_reliability': 86,      # Good data quality
        },

        # ─── B3 Education ───
        'COGN3': {  # Cogna Educação - Education
            'business_quality': 48,      # Education, vulnerable to cycles
            'valuation': 55,             # Distressed valuation
            'market_strength': 52,       # Volatile momentum
            'macro_factors': 58,         # Economic cycle sensitive
            'options_signal': 58,        # Higher IV
            'structural_risk': 55,       # Regulatory & cycle risk
            'data_reliability': 80,      # Adequate data quality
        },
        'YDUQ3': {  # Yduqs - Education
            'business_quality': 50,      # Education services, cyclical
            'valuation': 54,             # Volatile valuation
            'market_strength': 51,       # Volatile momentum
            'macro_factors': 57,         # Economic cycle sensitive
            'options_signal': 57,        # Higher IV
            'structural_risk': 54,       # Regulatory, cycle, leverage risk
            'data_reliability': 79,      # Adequate data quality
        },

        # ─── B3 Logistics & Transport ───
        'AZUL4': {  # Azul - Airlines
            'business_quality': 55,      # Airlines, volatile, capital intensive
            'valuation': 58,             # Fair P/E but cyclical
            'market_strength': 56,       # Volatile momentum
            'macro_factors': 55,         # FX & economic cycle sensitive
            'options_signal': 59,        # Higher IV
            'structural_risk': 60,       # Fuel, FX, operational risk
            'data_reliability': 82,      # Adequate data quality
        },
        'CCRO3': {  # CCR - Infrastructure/Toll Roads
            'business_quality': 66,      # Toll roads, stable cash flow
            'valuation': 67,             # Fair P/E
            'market_strength': 66,       # Stable momentum
            'macro_factors': 64,         # Economic cycle sensitive
            'options_signal': 64,        # Moderate IV
            'structural_risk': 68,       # Regulatory & concession risk
            'data_reliability': 83,      # Adequate data quality
        },

        # ─── B3 Consumer Products ───
        'RENT3': {  # Natura & Co - Cosmetics & Personal Care
            'business_quality': 64,      # Cosmetics, brand-driven, global
            'valuation': 66,             # Fair P/E
            'market_strength': 65,       # Moderate momentum
            'macro_factors': 62,         # Economic cycle sensitive
            'options_signal': 63,        # Moderate IV
            'structural_risk': 67,       # Regulatory & competition risk
            'data_reliability': 83,      # Adequate data quality
        },
        'ALPA4': {  # Alpargatas - Footwear & Apparel
            'business_quality': 62,      # Consumer goods, brand-driven
            'valuation': 64,             # Fair P/E
            'market_strength': 63,       # Moderate momentum
            'macro_factors': 61,         # Economic cycle sensitive
            'options_signal': 62,        # Moderate IV
            'structural_risk': 65,       # Competition & economic risk
            'data_reliability': 82,      # Adequate data quality
        },
        'POMO4': {  # Poloplast - Plastic Products
            'business_quality': 58,      # Plastic products, cyclical
            'valuation': 61,             # Fair P/E
            'market_strength': 59,       # Moderate momentum
            'macro_factors': 58,         # Commodity & cycle sensitive
            'options_signal': 60,        # Moderate IV
            'structural_risk': 62,       # Commodity & industrial risk
            'data_reliability': 80,      # Adequate data quality
        },
        'NTCO3': {  # Natura Cosméticos (standalone) - Cosmetics
            'business_quality': 63,      # Cosmetics, brand business
            'valuation': 65,             # Fair P/E
            'market_strength': 64,       # Moderate momentum
            'macro_factors': 61,         # Economic cycle sensitive
            'options_signal': 62,        # Moderate IV
            'structural_risk': 66,       # Competition & regulatory risk
            'data_reliability': 82,      # Adequate data quality
        },
        'RDOR3': {  # Rede D'Or - Healthcare/Hospitals
            'business_quality': 70,      # Hospital operator, growing, profitable
            'valuation': 68,             # Fair P/E
            'market_strength': 69,       # Good momentum, consolidation
            'macro_factors': 68,         # Defensive (healthcare)
            'options_signal': 66,        # Moderate IV
            'structural_risk': 71,       # Good governance, regulatory risk
            'data_reliability': 84,      # Good data quality
        },

        # ═══════════════════════════════════════════════════════════════════
        # US ADDITIONAL STOCKS (40)
        # ═══════════════════════════════════════════════════════════════════

        # ─── US Mega-Cap Tech ───
        'AAPL': {  # Apple
            'business_quality': 89,      # Exceptional ecosystem, margins, cash generation
            'valuation': 70,             # Fair to slightly expensive
            'market_strength': 78,       # Strong momentum, technical leadership
            'macro_factors': 70,         # Global exposure, FX sensitive
            'options_signal': 68,        # Low to moderate IV
            'structural_risk': 85,       # Excellent governance, brand moat
            'data_reliability': 92,      # Excellent data quality
        },
        'MSFT': {  # Microsoft
            'business_quality': 90,      # Excellent quality, recurring cloud revenue
            'valuation': 72,             # Fair to slightly expensive
            'market_strength': 80,       # Strong momentum, cloud leadership
            'macro_factors': 71,         # Global, enterprise-focused
            'options_signal': 69,        # Low to moderate IV
            'structural_risk': 86,       # Excellent governance, strong moat
            'data_reliability': 92,      # Excellent data quality
        },
        'GOOGL': {  # Alphabet/Google
            'business_quality': 88,      # Excellent quality, diverse revenue (ads, cloud)
            'valuation': 71,             # Fair to slightly expensive
            'market_strength': 77,       # Strong momentum, AI leadership
            'macro_factors': 70,         # Global advertising exposure
            'options_signal': 68,        # Moderate IV
            'structural_risk': 84,       # Excellent governance, moat
            'data_reliability': 92,      # Excellent data quality
        },
        'AMZN': {  # Amazon
            'business_quality': 86,      # Excellent quality, AWS dominant, AWS margins
            'valuation': 68,             # Fair valuation
            'market_strength': 76,       # Strong momentum, leadership
            'macro_factors': 69,         # Global consumer & enterprise
            'options_signal': 67,        # Moderate IV
            'structural_risk': 82,       # Strong governance, moat
            'data_reliability': 92,      # Excellent data quality
        },
        'META': {  # Meta Platforms (Facebook/Instagram)
            'business_quality': 85,      # Strong quality, improving margins, AI focus
            'valuation': 65,             # Fair valuation, AI re-rating
            'market_strength': 75,       # Strong momentum, efficiency gains
            'macro_factors': 68,         # Digital advertising exposure
            'options_signal': 66,        # Moderate IV
            'structural_risk': 80,       # Regulatory risks, content moderation
            'data_reliability': 92,      # Excellent data quality
        },
        'NVDA': {  # Nvidia
            'business_quality': 90,      # Exceptional quality, GPU/AI leadership
            'valuation': 60,             # Expensive relative to history
            'market_strength': 85,       # Exceptional momentum, AI leader
            'macro_factors': 68,         # Tech cycle & AI capex sensitive
            'options_signal': 64,        # Low IV (complacency)
            'structural_risk': 78,       # Strong moat, tech concentration risk
            'data_reliability': 92,      # Excellent data quality
        },

        # ─── US Large Cap Tech ───
        'TSLA': {  # Tesla
            'business_quality': 70,      # Good quality, but execution risk
            'valuation': 50,             # Expensive relative to earnings
            'market_strength': 72,       # Strong momentum but volatile
            'macro_factors': 62,         # EV cycle, macro sensitive
            'options_signal': 62,        # Moderate IV, high skew
            'structural_risk': 65,       # Execution & competitive risk, governance
            'data_reliability': 90,      # Excellent data quality
        },
        'NFLX': {  # Netflix
            'business_quality': 78,      # Good quality, recurring subscription revenue
            'valuation': 70,             # Fair P/E
            'market_strength': 74,       # Good momentum, content leadership
            'macro_factors': 70,         # Global, but advertising macro exposure
            'options_signal': 68,        # Moderate IV
            'structural_risk': 76,       # Content risk, competition increasing
            'data_reliability': 91,      # Excellent data quality
        },

        # ─── US Semiconductors & Chip Design ───
        'AMD': {  # Advanced Micro Devices
            'business_quality': 78,      # Good quality, competitive with Intel
            'valuation': 68,             # Fair P/E
            'market_strength': 74,       # Good momentum, AI exposure
            'macro_factors': 66,         # Chip cycle, China exposure
            'options_signal': 66,        # Moderate IV
            'structural_risk': 74,       # Cyclical, competitive risk
            'data_reliability': 91,      # Excellent data quality
        },
        'INTC': {  # Intel
            'business_quality': 72,      # Decent quality, but losing share
            'valuation': 62,             # Moderate P/E, turnaround
            'market_strength': 62,       # Weak momentum, turnaround story
            'macro_factors': 64,         # Chip cycle sensitive
            'options_signal': 62,        # Moderate IV
            'structural_risk': 70,       # Competitive & execution risk
            'data_reliability': 91,      # Excellent data quality
        },
        'TSM': {  # Taiwan Semiconductor
            'business_quality': 88,      # Exceptional quality, TSMC world-class
            'valuation': 72,             # Fair P/E for quality
            'market_strength': 76,       # Strong momentum, supply constraints
            'macro_factors': 62,         # China geopolitical risk, US exposure
            'options_signal': 67,        # Moderate IV
            'structural_risk': 75,       # Geopolitical (Taiwan), competitive
            'data_reliability': 91,      # Excellent data quality
        },
        'AVGO': {  # Broadcom
            'business_quality': 85,      # Excellent quality, broadband & data center
            'valuation': 72,             # Fair P/E
            'market_strength': 76,       # Strong momentum, infrastructure
            'macro_factors': 67,         # Tech cycle, infrastructure cycle
            'options_signal': 67,        # Moderate IV
            'structural_risk': 80,       # Good moat, diversified
            'data_reliability': 91,      # Excellent data quality
        },
        'MU': {  # Micron Technology
            'business_quality': 76,      # Good quality, memory supplier
            'valuation': 65,             # Fair valuation, cyclical
            'market_strength': 71,       # Moderate momentum, cyclical
            'macro_factors': 65,         # Memory cycle, China exposure
            'options_signal': 64,        # Moderate IV
            'structural_risk': 72,       # Cyclical, commodity-like
            'data_reliability': 90,      # Good data quality
        },
        'ARM': {  # Arm Holdings
            'business_quality': 82,      # Excellent quality, IP provider
            'valuation': 70,             # Fair P/E for growth
            'market_strength': 73,       # Good momentum, mobile/AI
            'macro_factors': 67,         # Tech cycle sensitive
            'options_signal': 66,        # Moderate IV
            'structural_risk': 78,       # Good moat, IPO-related execution
            'data_reliability': 89,      # Good data quality
        },
        'SMCI': {  # Super Micro Computer
            'business_quality': 75,      # Good quality, AI server leader
            'valuation': 68,             # Fair P/E, growth
            'market_strength': 77,       # Strong momentum, AI capex
            'macro_factors': 66,         # Tech cycle, AI sensitive
            'options_signal': 65,        # Moderate IV
            'structural_risk': 72,       # Execution & supply chain risk
            'data_reliability': 88,      # Adequate data quality
        },

        # ─── US Banks & Financial Services ───
        'JPM': {  # JP Morgan
            'business_quality': 81,      # Excellent quality, diversified bank
            'valuation': 74,             # Fair P/E for quality
            'market_strength': 75,       # Good momentum, stable
            'macro_factors': 68,         # Interest rate sensitive
            'options_signal': 69,        # Normal IV
            'structural_risk': 82,       # Excellent governance, systemically important
            'data_reliability': 92,      # Excellent data quality
        },
        'BAC': {  # Bank of America
            'business_quality': 78,      # Good quality, diversified bank
            'valuation': 72,             # Fair P/E
            'market_strength': 73,       # Good momentum, stable
            'macro_factors': 67,         # Interest rate sensitive
            'options_signal': 68,        # Normal IV
            'structural_risk': 79,       # Excellent governance, systemically important
            'data_reliability': 92,      # Excellent data quality
        },
        'GS': {  # Goldman Sachs
            'business_quality': 77,      # Good quality, investment banking focus
            'valuation': 70,             # Fair P/E
            'market_strength': 71,       # Moderate momentum, cyclical
            'macro_factors': 65,         # Capital markets & economic cycle
            'options_signal': 67,        # Moderate IV
            'structural_risk': 77,       # Governance, capital markets risk
            'data_reliability': 91,      # Excellent data quality
        },
        'MS': {  # Morgan Stanley
            'business_quality': 78,      # Good quality, wealth & investment banking
            'valuation': 71,             # Fair P/E
            'market_strength': 72,       # Moderate momentum
            'macro_factors': 66,         # Capital markets cycle
            'options_signal': 67,        # Moderate IV
            'structural_risk': 78,       # Governance, capital markets risk
            'data_reliability': 91,      # Excellent data quality
        },

        # ─── US Payment Networks ───
        'V': {  # Visa
            'business_quality': 89,      # Exceptional quality, recurring revenue network
            'valuation': 73,             # Fair P/E for quality
            'market_strength': 77,       # Strong momentum, stable
            'macro_factors': 72,         # Economic growth sensitive but global
            'options_signal': 70,        # Low to moderate IV
            'structural_risk': 87,       # Excellent moat, governance
            'data_reliability': 92,      # Excellent data quality
        },
        'MA': {  # Mastercard
            'business_quality': 88,      # Exceptional quality, recurring revenue network
            'valuation': 72,             # Fair P/E for quality
            'market_strength': 76,       # Strong momentum, stable
            'macro_factors': 71,         # Economic growth sensitive, global
            'options_signal': 69,        # Low to moderate IV
            'structural_risk': 86,       # Excellent moat, governance
            'data_reliability': 92,      # Excellent data quality
        },

        # ─── US Healthcare ───
        'JNJ': {  # Johnson & Johnson
            'business_quality': 86,      # Excellent quality, diversified healthcare
            'valuation': 73,             # Fair P/E
            'market_strength': 74,       # Stable momentum, defensive
            'macro_factors': 75,         # Defensive, lower macro sensitivity
            'options_signal': 71,        # Low IV, stable
            'structural_risk': 84,       # Excellent governance, moat
            'data_reliability': 92,      # Excellent data quality
        },
        'PFE': {  # Pfizer
            'business_quality': 78,      # Good quality, pharma + vaccines
            'valuation': 70,             # Fair P/E
            'market_strength': 69,       # Moderate momentum, stable
            'macro_factors': 73,         # Somewhat defensive
            'options_signal': 68,        # Low to moderate IV
            'structural_risk': 80,       # Good governance, regulatory risk
            'data_reliability': 92,      # Excellent data quality
        },
        'UNH': {  # UnitedHealth
            'business_quality': 84,      # Excellent quality, healthcare services
            'valuation': 71,             # Fair P/E
            'market_strength': 75,       # Strong momentum, growth
            'macro_factors': 72,         # Somewhat defensive, regulatory
            'options_signal': 69,        # Moderate IV
            'structural_risk': 80,       # Good governance, regulatory risk
            'data_reliability': 92,      # Excellent data quality
        },
        'LLY': {  # Eli Lilly
            'business_quality': 85,      # Excellent quality, pharma, GLP-1 strength
            'valuation': 72,             # Fair P/E for growth
            'market_strength': 79,       # Strong momentum, GLP-1 leadership
            'macro_factors': 73,         # Somewhat defensive
            'options_signal': 70,        # Low IV
            'structural_risk': 82,       # Good governance, patent risk
            'data_reliability': 92,      # Excellent data quality
        },

        # ─── US Energy ───
        'XOM': {  # ExxonMobil
            'business_quality': 78,      # Good quality, integrated oil & gas
            'valuation': 75,             # Fair P/E, attractive
            'market_strength': 74,       # Good momentum, commodity
            'macro_factors': 60,         # Very commodity & geopolitical sensitive
            'options_signal': 64,        # Moderate IV
            'structural_risk': 72,       # Commodity & regulatory risk
            'data_reliability': 91,      # Excellent data quality
        },
        'CVX': {  # Chevron
            'business_quality': 76,      # Good quality, integrated oil & gas
            'valuation': 74,             # Fair P/E, attractive
            'market_strength': 72,       # Good momentum, commodity
            'macro_factors': 59,         # Very commodity & geopolitical sensitive
            'options_signal': 63,        # Moderate IV
            'structural_risk': 71,       # Commodity & regulatory risk
            'data_reliability': 91,      # Excellent data quality
        },
        'COP': {  # ConocoPhillips
            'business_quality': 75,      # Good quality, upstream focused
            'valuation': 72,             # Fair P/E
            'market_strength': 71,       # Good momentum, commodity
            'macro_factors': 58,         # Very commodity sensitive
            'options_signal': 62,        # Moderate IV
            'structural_risk': 70,       # Commodity & regulatory risk
            'data_reliability': 91,      # Excellent data quality
        },

        # ─── US Media & Entertainment ───
        'DIS': {  # Disney
            'business_quality': 68,      # Good quality, but streaming losses
            'valuation': 68,             # Fair P/E
            'market_strength': 70,       # Moderate momentum, streaming stabilizing
            'macro_factors': 67,         # Consumer & economic cycle
            'options_signal': 66,        # Moderate IV
            'structural_risk': 71,       # Content & streaming execution risk
            'data_reliability': 92,      # Excellent data quality
        },

        # ─── US Transportation & Mobility ───
        'UBER': {  # Uber
            'business_quality': 70,      # Good quality improving, approaching profitability
            'valuation': 68,             # Fair P/E, growth
            'market_strength': 74,       # Strong momentum, platform growth
            'macro_factors': 66,         # Economic cycle, regulatory
            'options_signal': 66,        # Moderate IV
            'structural_risk': 72,       # Regulatory & competitive risk
            'data_reliability': 91,      # Excellent data quality
        },
        'LYFT': {  # Lyft
            'business_quality': 55,      # Weak quality, losses, niche player
            'valuation': 60,             # Fair P/E but unprofitable
            'market_strength': 59,       # Moderate momentum, Uber's shadow
            'macro_factors': 63,         # Economic cycle sensitive
            'options_signal': 62,        # Moderate IV
            'structural_risk': 60,       # Regulatory & competitive risk (vs Uber)
            'data_reliability': 90,      # Good data quality
        },

        # ─── US Consumer Cyclical ───
        'TGT': {  # Target
            'business_quality': 70,      # Good quality, retailer, inventory discipline
            'valuation': 70,             # Fair P/E
            'market_strength': 70,       # Good momentum, e-commerce growth
            'macro_factors': 64,         # Economic cycle sensitive
            'options_signal': 66,        # Moderate IV
            'structural_risk': 71,       # Retail & economic cycle risk
            'data_reliability': 91,      # Excellent data quality
        },

        # ─── US Digital & Streaming ───
        'SPOT': {  # Spotify
            'business_quality': 65,      # Fair quality, improving margins, growth
            'valuation': 65,             # Fair P/E
            'market_strength': 72,       # Good momentum, subscriber growth
            'macro_factors': 67,         # Economic cycle, advertising exposure
            'options_signal': 65,        # Moderate IV
            'structural_risk': 69,       # Competition & music licensing risk
            'data_reliability': 90,      # Good data quality
        },
        'COIN': {  # Coinbase
            'business_quality': 58,      # Fair quality, volatile, regulatory uncertain
            'valuation': 62,             # Fair P/E but cycle dependent
            'market_strength': 66,       # Moderate momentum, crypto cycle
            'macro_factors': 55,         # Very macro (rates, risk appetite)
            'options_signal': 61,        # Higher IV
            'structural_risk': 60,       # Regulatory & crypto risk
            'data_reliability': 89,      # Good data quality
        },

        # ─── US Index ETFs ───
        'SPY': {  # S&P 500 ETF
            'business_quality': 74,      # Diversified portfolio (500 stocks)
            'valuation': 72,             # Market valuation
            'market_strength': 74,       # Mirrors market trend
            'macro_factors': 68,         # US macro exposure (diversified)
            'options_signal': 70,        # Market IV level
            'structural_risk': 76,       # Diversification lowers risk
            'data_reliability': 92,      # Excellent data quality (ETF)
        },
        'QQQ': {  # Nasdaq 100 ETF (Tech-heavy)
            'business_quality': 76,      # Tech-focused portfolio
            'valuation': 70,             # Fair P/E relative to growth
            'market_strength': 77,       # Strong tech momentum
            'macro_factors': 66,         # Tech cycle & rates sensitive
            'options_signal': 69,        # Moderate IV
            'structural_risk': 74,       # Tech concentration risk
            'data_reliability': 92,      # Excellent data quality
        },
        'IWM': {  # Russell 2000 (Small Cap)
            'business_quality': 72,      # Diversified small cap
            'valuation': 68,             # Fair small cap valuation
            'market_strength': 71,       # Moderate momentum
            'macro_factors': 65,         # Economic cycle sensitive
            'options_signal': 68,        # Market IV level
            'structural_risk': 72,       # Small cap dispersion risk
            'data_reliability': 91,      # Excellent data quality
        },

        # ─── US SaaS & Enterprise Software ───
        'ADBE': {  # Adobe
            'business_quality': 82,      # Excellent quality, recurring SaaS
            'valuation': 72,             # Fair P/E for growth
            'market_strength': 74,       # Good momentum, creative/doc dominance
            'macro_factors': 68,         # Some economic cycle sensitivity
            'options_signal': 68,        # Moderate IV
            'structural_risk': 79,       # Good moat, competition emerging
            'data_reliability': 91,      # Excellent data quality
        },
        'CRM': {  # Salesforce
            'business_quality': 80,      # Excellent quality, CRM leader
            'valuation': 70,             # Fair P/E
            'market_strength': 72,       # Good momentum, AI focus
            'macro_factors': 67,         # Some enterprise cycle sensitivity
            'options_signal': 67,        # Moderate IV
            'structural_risk': 78,       # Good governance, competition
            'data_reliability': 91,      # Excellent data quality
        },
        'NOW': {  # ServiceNow
            'business_quality': 83,      # Excellent quality, workflow platform
            'valuation': 71,             # Fair P/E for growth
            'market_strength': 75,       # Strong momentum, enterprise AI
            'macro_factors': 67,         # Some enterprise cycle sensitivity
            'options_signal': 68,        # Moderate IV
            'structural_risk': 80,       # Strong moat, platform risk
            'data_reliability': 91,      # Excellent data quality
        },
        'ORCL': {  # Oracle
            'business_quality': 82,      # Excellent quality, database & cloud
            'valuation': 72,             # Fair P/E
            'market_strength': 74,       # Good momentum, cloud growth
            'macro_factors': 68,         # Some enterprise cycle sensitivity
            'options_signal': 68,        # Moderate IV
            'structural_risk': 80,       # Strong moat, mature company
            'data_reliability': 92,      # Excellent data quality
        },
        'SNOW': {  # Snowflake
            'business_quality': 78,      # Good quality, high growth SaaS
            'valuation': 68,             # Fair P/E for growth
            'market_strength': 72,       # Good momentum, data cloud
            'macro_factors': 66,         # Enterprise cycle sensitive
            'options_signal': 67,        # Moderate IV
            'structural_risk': 75,       # Execution & competition risk
            'data_reliability': 90,      # Good data quality
        },

        # ─── US E-commerce & Marketplaces ───
        'SHOP': {  # Shopify
            'business_quality': 77,      # Good quality, e-commerce platform
            'valuation': 69,             # Fair P/E
            'market_strength': 73,       # Good momentum, SMB focus
            'macro_factors': 66,         # Economic cycle & e-commerce sensitive
            'options_signal': 67,        # Moderate IV
            'structural_risk': 76,       # Competition from larger platforms
            'data_reliability': 91,      # Excellent data quality
        },
        'MELI': {  # MercadoLibre
            'business_quality': 75,      # Good quality, LATAM e-commerce leader
            'valuation': 68,             # Fair P/E for growth
            'market_strength': 72,       # Good momentum, LATAM growth
            'macro_factors': 64,         # LATAM macro & FX exposure
            'options_signal': 66,        # Moderate IV
            'structural_risk': 72,       # LATAM regulatory & competition
            'data_reliability': 90,      # Good data quality
        },
        'HOOD': {  # Robinhood
            'business_quality': 65,      # Fair quality, improving profitability
            'valuation': 64,             # Fair P/E, cyclical
            'market_strength': 68,       # Moderate momentum, crypto/trading cycle
            'macro_factors': 62,         # Rates, volatility, sentiment sensitive
            'options_signal': 64,        # Moderate IV
            'structural_risk': 67,       # Regulatory & volatility dependent
            'data_reliability': 89,      # Good data quality
        },
        'HUBS': {  # HubSpot
            'business_quality': 79,      # Good quality, CRM platform growth
            'valuation': 70,             # Fair P/E
            'market_strength': 73,       # Good momentum, SMB & enterprise
            'macro_factors': 67,         # Some enterprise cycle sensitivity
            'options_signal': 68,        # Moderate IV
            'structural_risk': 77,       # Competition, but strong market position
            'data_reliability': 90,      # Good data quality
        },

        # ─── US Global/Chinese Exposure ───
        'TCOM': {  # Trip.com
            'business_quality': 73,      # Good quality, online travel
            'valuation': 66,             # Fair P/E
            'market_strength': 70,       # Moderate momentum, China recovery
            'macro_factors': 58,         # China regulatory & macro sensitive
            'options_signal': 63,        # Moderate to higher IV
            'structural_risk': 68,       # China regulatory & geopolitical risk
            'data_reliability': 88,      # Good data quality
        },
        'BABA': {  # Alibaba
            'business_quality': 62,      # Fair quality but China regulatory risk
            'valuation': 60,             # Fair P/E but execution uncertain
            'market_strength': 62,       # Moderate momentum, recovery story
            'macro_factors': 54,         # China macro & regulatory sensitive
            'options_signal': 61,        # Higher IV due to China risk
            'structural_risk': 60,       # China regulatory & governance risk
            'data_reliability': 87,      # Adequate data quality
        },
        'TME': {  # Tencent Music
            'business_quality': 55,      # Fair quality, streaming competition
            'valuation': 58,             # Fair P/E
            'market_strength': 58,       # Moderate momentum, China exposure
            'macro_factors': 54,         # China macro & regulatory risk
            'options_signal': 60,        # Higher IV
            'structural_risk': 58,       # China regulatory & Tencent ownership
            'data_reliability': 86,      # Adequate data quality
        },

        # ─── US Alternative Investment/Growth ───
        'PLTR': {  # Palantir
            'business_quality': 72,      # Good quality, data analytics, growing
            'valuation': 65,             # Fair P/E, growth story
            'market_strength': 72,       # Good momentum, government contracts
            'macro_factors': 66,         # Some defense cycle exposure
            'options_signal': 66,        # Moderate IV
            'structural_risk': 72,       # Governance (shares structure), execution
            'data_reliability': 89,      # Good data quality
        },
        'OKLO': {  # Oklo (Nuclear Energy)
            'business_quality': 45,      # Early stage, pre-revenue concept
            'valuation': 55,             # Speculative valuation
            'market_strength': 62,       # High momentum, speculative
            'macro_factors': 68,         # Energy transition tailwind
            'options_signal': 62,        # Higher IV
            'structural_risk': 42,       # Very high execution & technology risk
            'data_reliability': 75,      # Limited data quality (new company)
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
