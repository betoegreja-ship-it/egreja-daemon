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
                'PETR4': 0.15,
                'VALE3': 0.15,
                'ITUB4': 0.20,
                'BBDC4': 0.10,
                'ABEV3': 0.15,
                'B3SA3': 0.10,
                'BOVA11': 0.15,
            }
        },
        'Dividendos + Proteção': {
            'description': 'High dividend yield stocks with focus on recurring income '
                          'and downside protection',
            'target_return': 9.0,
            'risk_level': 'Conservative',
            'allocations': {
                'PETR4': 0.25,    # High dividend yield
                'VALE3': 0.20,    # Cyclical but defensive
                'ITUB4': 0.15,    # Stable income
                'BBAS3': 0.15,    # Government support
                'BBDC4': 0.10,    # Defensive banking
                'ABEV3': 0.15,    # Dividend growth
            }
        },
        'Brasil + EUA': {
            'description': 'Geographic diversification with exposure to Brazil and US tech/mega-cap',
            'target_return': 12.0,
            'risk_level': 'Moderate-Aggressive',
            'allocations': {
                'PETR4': 0.10,
                'VALE3': 0.10,
                'ITUB4': 0.10,
                'BOVA11': 0.20,   # Brazil ETF for diversification
                'AAPL': 0.15,     # Apple (US)
                'MSFT': 0.15,     # Microsoft (US)
                'GOOGL': 0.10,    # Google (US)
                'AMZN': 0.10,     # Amazon (US)
            }
        }
    }


def get_realistic_asset_prices() -> dict:
    """
    Get realistic current prices for all assets (demo data in BRL/USD).

    Returns:
        {ticker: {'price': float, 'currency': str}, ...}
    """
    return {
        # Brazilian stocks (BRL)
        'PETR4': {'price': 27.50, 'currency': 'BRL'},
        'VALE3': {'price': 59.20, 'currency': 'BRL'},
        'ITUB4': {'price': 29.80, 'currency': 'BRL'},
        'BBDC4': {'price': 18.90, 'currency': 'BRL'},
        'BBAS3': {'price': 35.40, 'currency': 'BRL'},
        'ABEV3': {'price': 14.50, 'currency': 'BRL'},
        'B3SA3': {'price': 12.10, 'currency': 'BRL'},
        'BOVA11': {'price': 102.30, 'currency': 'BRL'},
        # US stocks (USD, converted to BRL with 5.15 rate for demo)
        'AAPL': {'price': 175.00 * 5.15, 'currency': 'BRL'},    # ~901.25 BRL
        'MSFT': {'price': 420.00 * 5.15, 'currency': 'BRL'},    # ~2,163 BRL
        'GOOGL': {'price': 140.00 * 5.15, 'currency': 'BRL'},   # ~721 BRL
        'AMZN': {'price': 180.00 * 5.15, 'currency': 'BRL'},    # ~927 BRL
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
