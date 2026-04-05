"""
Portfolio Engine for Long Horizon AI Module.

Manages three model portfolios:
  1. Quality Brasil - Focus on blue-chip quality stocks
  2. Dividendos + Proteção - High dividend yield with protection
  3. Brasil + EUA - Geographic diversification

Initial capital: R$ 7,000,000
Tracks: allocations, P&L per position, total value, benchmark returns
"""

import logging
from datetime import datetime, date
from decimal import Decimal
import json

logger = logging.getLogger(__name__)

# Initial capital in BRL
INITIAL_CAPITAL = 7_000_000


def get_model_portfolios() -> dict:
    """
    Define three model portfolios with strategic allocations.

    Returns:
        {
            portfolio_name: {
                'description': str,
                'target_return': float,
                'risk_level': str,
                'allocations': {ticker: weight, ...}
            }
        }
    """
    return {
        'Quality Brasil': {
            'description': 'Blue-chip quality stocks focusing on operational excellence, '
                          'cash generation, and competitive moats',
            'target_return': 10.5,  # Annual %
            'risk_level': 'Moderate',
            'allocations': {
                'PETR4': 0.12,
                'VALE3': 0.12,
                'ITUB4': 0.18,
                'BBDC4': 0.08,
                'ABEV3': 0.12,
                'WEGE3': 0.15,   # Added: WEG - quality engineering
                'SUZB3': 0.05,   # Added: Suzano - pulp & paper
                'EMBR3': 0.05,   # Added: Embraer - aerospace
                'B3SA3': 0.08,   # Exchange operator
                'BRFS3': 0.05,   # Added: BRF - protein production
            }
        },
        'Dividendos + Proteção': {
            'description': 'High dividend yield stocks with focus on recurring income '
                          'and downside protection',
            'target_return': 9.0,
            'risk_level': 'Conservative',
            'allocations': {
                'PETR4': 0.20,    # High dividend yield
                'VALE3': 0.15,    # Cyclical but defensive
                'ITUB4': 0.12,    # Stable income
                'BBAS3': 0.12,    # Government support
                'BBDC4': 0.08,    # Defensive banking
                'ABEV3': 0.12,    # Dividend growth
                'TAEE11': 0.10,   # Added: Taesa - energy transmission
                'VIVT3': 0.05,    # Added: Vivo - telecom dividend
                'EGIE3': 0.05,    # Added: EGE - energy utility
                'RADL3': 0.01,    # Diagnostics
            }
        },
        'Brasil + EUA': {
            'description': 'Geographic diversification with exposure to Brazil and US tech/mega-cap',
            'target_return': 12.0,
            'risk_level': 'Moderate-Aggressive',
            'allocations': {
                'PETR4': 0.08,
                'VALE3': 0.08,
                'ITUB4': 0.08,
                'BBDC4': 0.05,
                'ABEV3': 0.05,
                'BOVA11': 0.11,   # Brazil ETF for diversification
                'AAPL': 0.12,     # Apple (US)
                'MSFT': 0.12,     # Microsoft (US)
                'NVDA': 0.05,     # Added: NVIDIA - AI/semiconductors
                'META': 0.05,     # Added: Meta - social/AI
                'JPM': 0.05,      # Added: JP Morgan - US banking
                'GOOGL': 0.08,    # Google (US)
                'AMZN': 0.08,     # Amazon (US)
            }
        }
    }


def get_realistic_asset_prices() -> dict:
    """
    Get realistic current prices for all assets (April 2026 estimates in BRL/USD).

    Returns:
        {ticker: {'price': float, 'currency': str}, ...}
    """
    # Exchange rate: 1 USD = 5.15 BRL (April 2026 estimate)
    usd_to_brl = 5.15

    return {
        # Brazilian stocks (BRL) - 57 stocks
        'PETR4': {'price': 27.50, 'currency': 'BRL'},
        'VALE3': {'price': 59.20, 'currency': 'BRL'},
        'ITUB4': {'price': 29.80, 'currency': 'BRL'},
        'BBDC4': {'price': 18.90, 'currency': 'BRL'},
        'BBAS3': {'price': 35.40, 'currency': 'BRL'},
        'ABEV3': {'price': 14.50, 'currency': 'BRL'},
        'WEGE3': {'price': 32.15, 'currency': 'BRL'},
        'RENT3': {'price': 8.75, 'currency': 'BRL'},
        'LREN3': {'price': 22.50, 'currency': 'BRL'},
        'SUZB3': {'price': 11.20, 'currency': 'BRL'},
        'GGBR4': {'price': 28.40, 'currency': 'BRL'},
        'EMBR3': {'price': 19.30, 'currency': 'BRL'},
        'CSNA3': {'price': 15.80, 'currency': 'BRL'},
        'CMIG4': {'price': 12.40, 'currency': 'BRL'},
        'CPLE6': {'price': 30.70, 'currency': 'BRL'},
        'VIVT3': {'price': 38.50, 'currency': 'BRL'},
        'SBSP3': {'price': 45.20, 'currency': 'BRL'},
        'CSAN3': {'price': 16.90, 'currency': 'BRL'},
        'GOAU4': {'price': 7.65, 'currency': 'BRL'},
        'USIM5': {'price': 13.45, 'currency': 'BRL'},
        'BPAC11': {'price': 26.30, 'currency': 'BRL'},
        'RADL3': {'price': 41.80, 'currency': 'BRL'},
        'PRIO3': {'price': 24.60, 'currency': 'BRL'},
        'BRFS3': {'price': 18.75, 'currency': 'BRL'},
        'MRFG3': {'price': 12.15, 'currency': 'BRL'},
        'JBSS3': {'price': 28.90, 'currency': 'BRL'},
        'EGIE3': {'price': 21.50, 'currency': 'BRL'},
        'CMIN3': {'price': 10.30, 'currency': 'BRL'},
        'AESB3': {'price': 27.60, 'currency': 'BRL'},
        'BBDC3': {'price': 20.75, 'currency': 'BRL'},
        'BBSE3': {'price': 32.40, 'currency': 'BRL'},
        'ALOS3': {'price': 6.95, 'currency': 'BRL'},
        'MULT3': {'price': 13.20, 'currency': 'BRL'},
        'SMFT3': {'price': 16.45, 'currency': 'BRL'},
        'EQTL3': {'price': 34.70, 'currency': 'BRL'},
        'TAEE11': {'price': 35.90, 'currency': 'BRL'},
        'ENEV3': {'price': 8.40, 'currency': 'BRL'},
        'CPFE3': {'price': 28.15, 'currency': 'BRL'},
        'CXSE3': {'price': 14.70, 'currency': 'BRL'},
        'VBBR3': {'price': 19.60, 'currency': 'BRL'},
        'UGPA3': {'price': 25.85, 'currency': 'BRL'},
        'KLBN11': {'price': 11.75, 'currency': 'BRL'},
        'TOTS3': {'price': 9.30, 'currency': 'BRL'},
        'MGLU3': {'price': 7.10, 'currency': 'BRL'},
        'CASH3': {'price': 42.30, 'currency': 'BRL'},
        'HAPV3': {'price': 23.50, 'currency': 'BRL'},
        'RDOR3': {'price': 31.20, 'currency': 'BRL'},
        'HYPE3': {'price': 5.85, 'currency': 'BRL'},
        'COGN3': {'price': 3.40, 'currency': 'BRL'},
        'YDUQ3': {'price': 18.95, 'currency': 'BRL'},
        'NTCO3': {'price': 29.45, 'currency': 'BRL'},
        'AZUL4': {'price': 22.70, 'currency': 'BRL'},
        'CCRO3': {'price': 24.15, 'currency': 'BRL'},
        'MDIA3': {'price': 8.60, 'currency': 'BRL'},
        'ALPA4': {'price': 19.30, 'currency': 'BRL'},
        'POMO4': {'price': 27.85, 'currency': 'BRL'},
        'AMER3': {'price': 10.20, 'currency': 'BRL'},
        'RECV3': {'price': 15.45, 'currency': 'BRL'},
        'B3SA3': {'price': 12.10, 'currency': 'BRL'},
        'BOVA11': {'price': 102.30, 'currency': 'BRL'},

        # US stocks (USD converted to BRL) - 51 stocks
        'AAPL': {'price': 175.00 * usd_to_brl, 'currency': 'BRL'},
        'MSFT': {'price': 420.00 * usd_to_brl, 'currency': 'BRL'},
        'NVDA': {'price': 875.00 * usd_to_brl, 'currency': 'BRL'},
        'AMZN': {'price': 180.00 * usd_to_brl, 'currency': 'BRL'},
        'GOOGL': {'price': 140.00 * usd_to_brl, 'currency': 'BRL'},
        'META': {'price': 495.00 * usd_to_brl, 'currency': 'BRL'},
        'TSLA': {'price': 245.00 * usd_to_brl, 'currency': 'BRL'},
        'NFLX': {'price': 285.00 * usd_to_brl, 'currency': 'BRL'},
        'AMD': {'price': 165.00 * usd_to_brl, 'currency': 'BRL'},
        'INTC': {'price': 42.00 * usd_to_brl, 'currency': 'BRL'},
        'JPM': {'price': 195.00 * usd_to_brl, 'currency': 'BRL'},
        'BAC': {'price': 38.50 * usd_to_brl, 'currency': 'BRL'},
        'GS': {'price': 445.00 * usd_to_brl, 'currency': 'BRL'},
        'MS': {'price': 375.00 * usd_to_brl, 'currency': 'BRL'},
        'V': {'price': 265.00 * usd_to_brl, 'currency': 'BRL'},
        'MA': {'price': 475.00 * usd_to_brl, 'currency': 'BRL'},
        'JNJ': {'price': 155.00 * usd_to_brl, 'currency': 'BRL'},
        'PFE': {'price': 28.75 * usd_to_brl, 'currency': 'BRL'},
        'UNH': {'price': 515.00 * usd_to_brl, 'currency': 'BRL'},
        'XOM': {'price': 115.00 * usd_to_brl, 'currency': 'BRL'},
        'CVX': {'price': 165.00 * usd_to_brl, 'currency': 'BRL'},
        'COP': {'price': 65.00 * usd_to_brl, 'currency': 'BRL'},
        'DIS': {'price': 95.00 * usd_to_brl, 'currency': 'BRL'},
        'UBER': {'price': 72.50 * usd_to_brl, 'currency': 'BRL'},
        'LYFT': {'price': 18.50 * usd_to_brl, 'currency': 'BRL'},
        'SPOT': {'price': 335.00 * usd_to_brl, 'currency': 'BRL'},
        'COIN': {'price': 145.00 * usd_to_brl, 'currency': 'BRL'},
        'SPY': {'price': 585.00 * usd_to_brl, 'currency': 'BRL'},
        'QQQ': {'price': 410.00 * usd_to_brl, 'currency': 'BRL'},
        'IWM': {'price': 195.00 * usd_to_brl, 'currency': 'BRL'},
        'TSM': {'price': 160.00 * usd_to_brl, 'currency': 'BRL'},
        'AVGO': {'price': 145.00 * usd_to_brl, 'currency': 'BRL'},
        'MU': {'price': 110.00 * usd_to_brl, 'currency': 'BRL'},
        'ARM': {'price': 165.00 * usd_to_brl, 'currency': 'BRL'},
        'SMCI': {'price': 18.50 * usd_to_brl, 'currency': 'BRL'},
        'ADBE': {'price': 625.00 * usd_to_brl, 'currency': 'BRL'},
        'CRM': {'price': 315.00 * usd_to_brl, 'currency': 'BRL'},
        'NOW': {'price': 695.00 * usd_to_brl, 'currency': 'BRL'},
        'ORCL': {'price': 145.00 * usd_to_brl, 'currency': 'BRL'},
        'SNOW': {'price': 145.00 * usd_to_brl, 'currency': 'BRL'},
        'SHOP': {'price': 72.50 * usd_to_brl, 'currency': 'BRL'},
        'MELI': {'price': 1850.00 * usd_to_brl, 'currency': 'BRL'},
        'HOOD': {'price': 23.50 * usd_to_brl, 'currency': 'BRL'},
        'HUBS': {'price': 575.00 * usd_to_brl, 'currency': 'BRL'},
        'TCOM': {'price': 18.25 * usd_to_brl, 'currency': 'BRL'},
        'BABA': {'price': 95.00 * usd_to_brl, 'currency': 'BRL'},
        'LLY': {'price': 815.00 * usd_to_brl, 'currency': 'BRL'},
        'TME': {'price': 6.50 * usd_to_brl, 'currency': 'BRL'},
        'PLTR': {'price': 42.50 * usd_to_brl, 'currency': 'BRL'},
        'OKLO': {'price': 14.85 * usd_to_brl, 'currency': 'BRL'},
        'TGT': {'price': 85.00 * usd_to_brl, 'currency': 'BRL'},
    }


def calculate_portfolio_positions(portfolio_name: str, initial_capital: float = INITIAL_CAPITAL) -> dict:
    """
    Calculate portfolio positions for a given model portfolio.

    Args:
        portfolio_name: Name of model portfolio
        initial_capital: Initial capital in BRL

    Returns:
        {
            'portfolio_name': str,
            'total_capital': float,
            'positions': [
                {
                    'ticker': str,
                    'weight': float,
                    'entry_price': float,
                    'quantity': float,
                    'position_value': float,
                    'current_price': float,
                    'pnl': float,
                    'pnl_pct': float
                },
                ...
            ],
            'total_position_value': float,
            'total_pnl': float,
            'total_pnl_pct': float
        }
    """
    portfolios = get_model_portfolios()
    if portfolio_name not in portfolios:
        return None

    portfolio = portfolios[portfolio_name]
    prices = get_realistic_asset_prices()

    positions = []
    total_position_value = 0
    total_pnl = 0

    for ticker, weight in portfolio['allocations'].items():
        # Allocate capital
        allocation_value = initial_capital * weight

        # Get current price
        current_price = prices.get(ticker, {}).get('price', 0)
        if current_price == 0:
            continue

        # Calculate quantity
        quantity = allocation_value / current_price

        # Simulate entry price (10-30 days ago, with some variance)
        # For demo, assume 2-5% variance from current price
        import random
        variance = random.uniform(-0.05, 0.03)  # -5% to +3% range
        entry_price = current_price / (1 + variance)

        # Calculate position value and P&L
        position_value = quantity * current_price
        position_pnl = position_value - (quantity * entry_price)
        position_pnl_pct = (position_pnl / (quantity * entry_price)) * 100 if entry_price > 0 else 0

        positions.append({
            'ticker': ticker,
            'weight': weight,
            'entry_price': round(entry_price, 2),
            'current_price': round(current_price, 2),
            'quantity': round(quantity, 4),
            'position_value': round(position_value, 2),
            'pnl': round(position_pnl, 2),
            'pnl_pct': round(position_pnl_pct, 4),
            'entry_date': (datetime.now() - __import__('datetime').timedelta(days=20)).strftime('%Y-%m-%d'),
        })

        total_position_value += position_value
        total_pnl += position_pnl

    total_pnl_pct = (total_pnl / (initial_capital if initial_capital > 0 else 1)) * 100

    return {
        'portfolio_name': portfolio_name,
        'description': portfolio['description'],
        'target_return': portfolio['target_return'],
        'risk_level': portfolio['risk_level'],
        'total_capital': initial_capital,
        'positions': positions,
        'position_count': len(positions),
        'total_position_value': round(total_position_value, 2),
        'total_pnl': round(total_pnl, 2),
        'total_pnl_pct': round(total_pnl_pct, 4),
        'cash_reserve': round(initial_capital - total_position_value, 2),
        'investment_ratio': round((total_position_value / initial_capital) * 100, 2),
        'as_of_date': date.today().isoformat(),
    }


def get_all_portfolios_summary(initial_capital: float = INITIAL_CAPITAL) -> dict:
    """
    Get summary of all model portfolios.

    Returns:
        {portfolio_name: summary_data, ...}
    """
    portfolios = get_model_portfolios()
    results = {}

    for portfolio_name in portfolios.keys():
        results[portfolio_name] = calculate_portfolio_positions(portfolio_name, initial_capital)

    return results


def compare_portfolios(initial_capital: float = INITIAL_CAPITAL) -> dict:
    """
    Compare characteristics and performance across model portfolios.

    Returns:
        Comparison metrics across all three portfolios
    """
    all_portfolios = get_all_portfolios_summary(initial_capital)

    comparison = {
        'initial_capital': initial_capital,
        'as_of_date': date.today().isoformat(),
        'portfolios': {}
    }

    for pname, pdata in all_portfolios.items():
        if pdata:
            comparison['portfolios'][pname] = {
                'risk_level': pdata['risk_level'],
                'target_return': pdata['target_return'],
                'total_value': pdata['total_position_value'],
                'total_pnl': pdata['total_pnl'],
                'total_pnl_pct': pdata['total_pnl_pct'],
                'position_count': pdata['position_count'],
                'cash_reserve': pdata['cash_reserve'],
            }

    return comparison


if __name__ == '__main__':
    # Demo: Generate and print portfolio summaries
    print("\n=== Long Horizon Model Portfolios ===\n")
    portfolios = get_all_portfolios_summary()
    for pname, pdata in portfolios.items():
        print(f"\n{pname}")
        print(f"{'='*60}")
        print(f"Risk Level: {pdata['risk_level']} | Target Return: {pdata['target_return']}% p.a.")
        print(f"Total Value: R$ {pdata['total_position_value']:,.2f}")
        print(f"Total P&L: R$ {pdata['total_pnl']:,.2f} ({pdata['total_pnl_pct']:.2f}%)")
        print(f"\nTop Positions:")
        for pos in sorted(pdata['positions'], key=lambda x: x['position_value'], reverse=True)[:5]:
            print(f"  {pos['ticker']}: {pos['weight']*100:.1f}% | "
                  f"R$ {pos['position_value']:,.2f} | P&L: {pos['pnl_pct']:.2f}%")
