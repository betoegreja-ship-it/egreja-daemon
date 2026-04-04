"""
Polygon Provider for US Market Data

Connects to https://api.polygon.io for comprehensive US market data:
- Stock aggregates (OHLCV) for ADRs
- Real-time snapshots
- Options data for US-listed options
- Financial statements (income, balance sheet, cash flow)
- Dividend and split history
- Technical indicators
- News and related companies

Auth: apiKey query parameter
Env: POLYGON_API_KEY
"""

import logging
import os
import time
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from threading import Lock

logger = logging.getLogger(__name__)


class PolygonProvider:
    """
    Polygon Complete Connector - US Equities & Options

    URL: https://api.polygon.io
    Auth: apiKey={POLYGON_API_KEY}
    """

    BASE_URL = "https://api.polygon.io"

    def __init__(self):
        """Initialize Polygon provider with API key."""
        self._api_key = os.environ.get('POLYGON_API_KEY', '').strip()
        self._session = requests.Session()

        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_lock = Lock()
        self._cache_ttl = 300  # 5 minutes

        self._is_healthy = bool(self._api_key)

        if self._is_healthy:
            logger.info("Polygon provider initialized with API key")
        else:
            logger.warning("Polygon provider disabled (no API key)")

    # ─── Health & Status ──────────────────────────────────────────────

    def is_healthy(self) -> bool:
        """Check if provider is healthy and initialized."""
        return self._is_healthy

    def health_check(self) -> Dict[str, Any]:
        """Perform health check on Polygon service."""
        if not self._is_healthy:
            return {'healthy': False, 'reason': 'No API key'}

        try:
            # Try a simple snapshot fetch
            r = self._session.get(
                f"{self.BASE_URL}/v2/snapshot/locale/us/markets/stocks/tickers/AAPL",
                params={'apiKey': self._api_key},
                timeout=8
            )
            if r.status_code == 200:
                return {
                    'healthy': True,
                    'timestamp': datetime.utcnow().isoformat(),
                    'response_time_ms': r.elapsed.total_seconds() * 1000,
                }
            else:
                logger.warning(f"Polygon health check returned {r.status_code}")
                return {'healthy': False, 'status_code': r.status_code}
        except Exception as e:
            logger.warning(f"Polygon health check failed: {e}")
            return {'healthy': False, 'error': str(e)}

    # ─── HTTP Helpers ─────────────────────────────────────────────────

    def _get(self, path: str, params: dict = None, timeout: int = 10) -> Optional[Any]:
        """GET request to Polygon API with caching."""
        if not self._is_healthy:
            return None

        if params is None:
            params = {}
        params['apiKey'] = self._api_key

        # Check cache
        cache_key = f"{path}:{str(sorted(params.items()))}"
        with self._cache_lock:
            cached = self._cache.get(cache_key)
            if cached and (time.time() - cached['ts']) < self._cache_ttl:
                return cached['data']

        try:
            url = f"{self.BASE_URL}{path}"
            r = self._session.get(url, params=params, timeout=timeout)

            if r.status_code == 200:
                data = r.json()
                # Cache result
                with self._cache_lock:
                    self._cache[cache_key] = {
                        'data': data,
                        'ts': time.time(),
                    }
                return data
            else:
                logger.warning(f"Polygon GET {path}: HTTP {r.status_code}")
            return None

        except Exception as e:
            logger.warning(f"Polygon GET {path} error: {e}")
            return None

    # ─── Snapshot ─────────────────────────────────────────────────────

    def get_snapshot(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get real-time snapshot for a ticker.

        Returns:
            {
                'ticker': str,
                'price': float,
                'change': float,
                'changePercent': float,
                'volume': float,
                'vwap': float,
                'open': float,
                'high': float,
                'low': float,
                'previousClose': float,
                'timestamp': str,
            }
        """
        if not self._is_healthy:
            return None

        try:
            data = self._get(f"/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}")
            if not data or not data.get('results'):
                return None

            result = data['results'][0]
            session = result.get('intraday', {})
            prev = result.get('prevDay', {})

            return {
                'ticker': ticker,
                'price': result.get('lastPrice', 0),
                'change': session.get('c', 0) - prev.get('c', 0) if 'c' in session and 'c' in prev else 0,
                'changePercent': (
                    ((session.get('c', 0) - prev.get('c', 0)) / prev.get('c', 1) * 100)
                    if 'c' in session and 'c' in prev else 0
                ),
                'volume': session.get('v', 0),
                'vwap': session.get('vw', 0),
                'open': session.get('o', 0),
                'high': session.get('h', 0),
                'low': session.get('l', 0),
                'previousClose': prev.get('c', 0),
                'timestamp': datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.warning(f"Polygon get_snapshot {ticker}: {e}")
            return None

    # ─── Aggregates (OHLCV) ───────────────────────────────────────────

    def get_aggregates(self, ticker: str, timespan: str = 'day', range_days: int = 365) -> Optional[List[Dict[str, Any]]]:
        """
        Get historical aggregates (OHLCV data).

        Args:
            ticker: Stock ticker
            timespan: 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year'
            range_days: How many days of history (approximate based on timespan)

        Returns:
            List of candles: [{date, open, high, low, close, volume, vwap}, ...]
        """
        if not self._is_healthy:
            return None

        try:
            # Calculate multiplier based on days
            multiplier = 1
            if timespan == 'week':
                multiplier = 1
            elif timespan == 'month':
                multiplier = 1
            else:
                multiplier = 1

            end_date = datetime.utcnow().strftime('%Y-%m-%d')
            start_date = (datetime.utcnow() - timedelta(days=range_days)).strftime('%Y-%m-%d')

            data = self._get(
                f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}",
                params={'sort': 'asc'}
            )

            if not data or not data.get('results'):
                return None

            candles = []
            for bar in data['results']:
                candles.append({
                    'timestamp': bar.get('t', 0),
                    'date': datetime.fromtimestamp(bar.get('t', 0) / 1000).strftime('%Y-%m-%d'),
                    'open': bar.get('o', 0),
                    'high': bar.get('h', 0),
                    'low': bar.get('l', 0),
                    'close': bar.get('c', 0),
                    'volume': bar.get('v', 0),
                    'vwap': bar.get('vw', 0),
                })

            return candles

        except Exception as e:
            logger.warning(f"Polygon get_aggregates {ticker}: {e}")
            return None

    # ─── Options Chain ────────────────────────────────────────────────

    def get_options_chain(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get available options contracts for a ticker.

        Returns:
            {
                'ticker': str,
                'timestamp': str,
                'contracts': [
                    {
                        'contractType': 'call' or 'put',
                        'expirationDate': 'YYYYMMDD',
                        'strikePrice': float,
                    }, ...
                ]
            }
        """
        if not self._is_healthy:
            return None

        try:
            data = self._get(f"/v3/reference/options/contracts", params={'underlying_ticker': ticker})

            if not data or not data.get('results'):
                return None

            contracts = []
            for contract in data['results']:
                contracts.append({
                    'contractType': 'call' if contract.get('contract_type') == 'call' else 'put',
                    'expirationDate': contract.get('expiration_date', ''),
                    'strikePrice': float(contract.get('strike_price', 0)),
                })

            return {
                'ticker': ticker,
                'timestamp': datetime.utcnow().isoformat(),
                'contracts': contracts,
            }

        except Exception as e:
            logger.warning(f"Polygon get_options_chain {ticker}: {e}")
            return None

    # ─── Options Snapshot ─────────────────────────────────────────────

    def get_options_snapshot(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get options chain snapshot with Greeks and IV.

        Returns:
            {
                'ticker': str,
                'timestamp': str,
                'options': [
                    {
                        'contractType': str,
                        'strikePrice': float,
                        'expirationDate': str,
                        'lastPrice': float,
                        'bidPrice': float,
                        'askPrice': float,
                        'impliedVolatility': float,
                        'openInterest': int,
                        'delta': float,
                        'gamma': float,
                        'theta': float,
                        'vega': float,
                    }, ...
                ]
            }
        """
        if not self._is_healthy:
            return None

        try:
            data = self._get(f"/v3/snapshot/options/{ticker}")

            if not data or not data.get('results'):
                return None

            options = []
            for opt in data['results']:
                options.append({
                    'contractType': 'call' if opt.get('option_type') == 'call' else 'put',
                    'strikePrice': float(opt.get('strike_price', 0)),
                    'expirationDate': opt.get('expiration_date', ''),
                    'lastPrice': float(opt.get('last_price', 0)),
                    'bidPrice': float(opt.get('bid', 0)),
                    'askPrice': float(opt.get('ask', 0)),
                    'impliedVolatility': float(opt.get('implied_volatility', 0)),
                    'openInterest': int(opt.get('open_interest', 0)),
                    'delta': float(opt.get('delta', 0)),
                    'gamma': float(opt.get('gamma', 0)),
                    'theta': float(opt.get('theta', 0)),
                    'vega': float(opt.get('vega', 0)),
                    'breakEvenPrice': float(opt.get('break_even_price', 0)),
                })

            return {
                'ticker': ticker,
                'timestamp': datetime.utcnow().isoformat(),
                'options': options,
            }

        except Exception as e:
            logger.warning(f"Polygon get_options_snapshot {ticker}: {e}")
            return None

    # ─── Financial Statements ─────────────────────────────────────────

    def get_financials(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get latest financial statements.

        Returns:
            {
                'ticker': str,
                'timestamp': str,
                'incomeStatement': {
                    'revenues': float,
                    'costOfRevenue': float,
                    'grossProfit': float,
                    'operatingIncome': float,
                    'netIncome': float,
                    'eps': float,
                },
                'balanceSheet': {
                    'totalAssets': float,
                    'totalLiabilities': float,
                    'equity': float,
                    'currentAssets': float,
                    'currentLiabilities': float,
                    'longTermDebt': float,
                },
                'cashFlow': {
                    'operatingCashflow': float,
                    'investingCashflow': float,
                    'financingCashflow': float,
                    'freeCashflow': float,
                    'dividendsPaid': float,
                },
            }
        """
        if not self._is_healthy:
            return None

        try:
            data = self._get(f"/vX/reference/financials", params={'ticker': ticker})

            if not data or not data.get('results'):
                return None

            # Get most recent financial data
            results = data['results'][0] if data['results'] else {}

            income = results.get('financials', {}).get('income_statement', {})
            balance = results.get('financials', {}).get('balance_sheet', {})
            cf = results.get('financials', {}).get('cash_flow_statement', {})

            return {
                'ticker': ticker,
                'timestamp': datetime.utcnow().isoformat(),
                'incomeStatement': {
                    'revenues': float(income.get('revenues', 0)) if income.get('revenues') else None,
                    'costOfRevenue': float(income.get('cost_of_revenue', 0)) if income.get('cost_of_revenue') else None,
                    'grossProfit': float(income.get('gross_profit', 0)) if income.get('gross_profit') else None,
                    'operatingIncome': float(income.get('operating_income', 0)) if income.get('operating_income') else None,
                    'netIncome': float(income.get('net_income', 0)) if income.get('net_income') else None,
                    'eps': float(income.get('earnings_per_share', 0)) if income.get('earnings_per_share') else None,
                },
                'balanceSheet': {
                    'totalAssets': float(balance.get('total_assets', 0)) if balance.get('total_assets') else None,
                    'totalLiabilities': float(balance.get('total_liabilities', 0)) if balance.get('total_liabilities') else None,
                    'equity': float(balance.get('total_equity', 0)) if balance.get('total_equity') else None,
                    'currentAssets': float(balance.get('current_assets', 0)) if balance.get('current_assets') else None,
                    'currentLiabilities': float(balance.get('current_liabilities', 0)) if balance.get('current_liabilities') else None,
                    'longTermDebt': float(balance.get('long_term_debt', 0)) if balance.get('long_term_debt') else None,
                },
                'cashFlow': {
                    'operatingCashflow': float(cf.get('operating_cash_flow', 0)) if cf.get('operating_cash_flow') else None,
                    'investingCashflow': float(cf.get('investing_cash_flow', 0)) if cf.get('investing_cash_flow') else None,
                    'financingCashflow': float(cf.get('financing_cash_flow', 0)) if cf.get('financing_cash_flow') else None,
                    'freeCashflow': float(cf.get('free_cash_flow', 0)) if cf.get('free_cash_flow') else None,
                    'dividendsPaid': float(cf.get('dividends_paid', 0)) if cf.get('dividends_paid') else None,
                },
            }

        except Exception as e:
            logger.warning(f"Polygon get_financials {ticker}: {e}")
            return None

    # ─── Dividends ────────────────────────────────────────────────────

    def get_dividends(self, ticker: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get dividend history.

        Returns:
            [{
                'exDate': str,
                'payDate': str,
                'recordDate': str,
                'declaredDate': str,
                'amount': float,
                'currency': str,
            }, ...]
        """
        if not self._is_healthy:
            return None

        try:
            data = self._get(f"/v3/reference/dividends", params={'ticker': ticker})

            if not data or not data.get('results'):
                return None

            dividends = []
            for div in data['results']:
                dividends.append({
                    'exDate': div.get('ex_dividend_date', ''),
                    'payDate': div.get('pay_date', ''),
                    'recordDate': div.get('record_date', ''),
                    'declaredDate': div.get('declaration_date', ''),
                    'amount': float(div.get('dividend_per_share', 0)),
                    'currency': div.get('currency', 'USD'),
                })

            return sorted(dividends, key=lambda x: x['exDate'], reverse=True)

        except Exception as e:
            logger.warning(f"Polygon get_dividends {ticker}: {e}")
            return None

    # ─── Stock Splits ─────────────────────────────────────────────────

    def get_splits(self, ticker: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get stock split history.

        Returns:
            [{
                'exDate': str,
                'splitFrom': float,
                'splitTo': float,
            }, ...]
        """
        if not self._is_healthy:
            return None

        try:
            data = self._get(f"/v3/reference/splits", params={'ticker': ticker})

            if not data or not data.get('results'):
                return None

            splits = []
            for split in data['results']:
                splits.append({
                    'exDate': split.get('execution_date', ''),
                    'splitFrom': float(split.get('split_from', 1)),
                    'splitTo': float(split.get('split_to', 1)),
                })

            return sorted(splits, key=lambda x: x['exDate'], reverse=True)

        except Exception as e:
            logger.warning(f"Polygon get_splits {ticker}: {e}")
            return None

    # ─── Technical Indicators ─────────────────────────────────────────

    def get_technical_indicators(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get technical indicators (SMA, EMA, MACD, RSI).

        Returns:
            {
                'ticker': str,
                'timestamp': str,
                'sma_50': float,
                'sma_200': float,
                'ema_12': float,
                'ema_26': float,
                'rsi_14': float,
                'macd': {
                    'value': float,
                    'signal': float,
                    'histogram': float,
                },
            }
        """
        if not self._is_healthy:
            return None

        try:
            # SMA
            sma50 = self._get(
                f"/v1/indicators/sma/{ticker}",
                params={'timespan': 'day', 'adjusted': True, 'window': 50}
            )
            sma200 = self._get(
                f"/v1/indicators/sma/{ticker}",
                params={'timespan': 'day', 'adjusted': True, 'window': 200}
            )
            rsi = self._get(
                f"/v1/indicators/rsi/{ticker}",
                params={'timespan': 'day', 'adjusted': True, 'window': 14}
            )
            macd = self._get(
                f"/v1/indicators/macd/{ticker}",
                params={'timespan': 'day', 'adjusted': True}
            )

            result = {
                'ticker': ticker,
                'timestamp': datetime.utcnow().isoformat(),
                'sma_50': None,
                'sma_200': None,
                'rsi_14': None,
                'macd': {},
            }

            if sma50 and sma50.get('results'):
                result['sma_50'] = sma50['results'][-1].get('value')
            if sma200 and sma200.get('results'):
                result['sma_200'] = sma200['results'][-1].get('value')
            if rsi and rsi.get('results'):
                result['rsi_14'] = rsi['results'][-1].get('value')
            if macd and macd.get('results'):
                last_macd = macd['results'][-1]
                result['macd'] = {
                    'value': last_macd.get('value'),
                    'signal': last_macd.get('signal'),
                    'histogram': last_macd.get('histogram'),
                }

            return result

        except Exception as e:
            logger.warning(f"Polygon get_technical_indicators {ticker}: {e}")
            return None

    # ─── News ────────────────────────────────────────────────────────

    def get_news(self, ticker: str, limit: int = 10) -> Optional[List[Dict[str, Any]]]:
        """
        Get recent news articles.

        Returns:
            [{
                'title': str,
                'description': str,
                'publishedUtc': str,
                'url': str,
                'source': str,
            }, ...]
        """
        if not self._is_healthy:
            return None

        try:
            data = self._get(
                f"/v2/reference/news",
                params={'query': ticker, 'limit': limit}
            )

            if not data or not data.get('results'):
                return None

            news = []
            for article in data['results'][:limit]:
                news.append({
                    'title': article.get('title', ''),
                    'description': article.get('description', '')[:200],
                    'publishedUtc': article.get('published_utc', ''),
                    'url': article.get('article_url', ''),
                    'source': article.get('publisher', {}).get('name', ''),
                })

            return news

        except Exception as e:
            logger.warning(f"Polygon get_news {ticker}: {e}")
            return None

    # ─── Related Companies ────────────────────────────────────────────

    def get_related(self, ticker: str) -> Optional[List[str]]:
        """
        Get list of related company tickers.

        Returns:
            [ticker1, ticker2, ...] (up to 20)
        """
        if not self._is_healthy:
            return None

        try:
            data = self._get(f"/v1/related-companies/{ticker}")

            if not data or not data.get('results'):
                return None

            tickers = [r.get('ticker') for r in data['results'][:20] if r.get('ticker')]
            return tickers

        except Exception as e:
            logger.warning(f"Polygon get_related {ticker}: {e}")
            return None

    # ─── Ticker Details ───────────────────────────────────────────────

    def get_ticker_details(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed ticker information.

        Returns:
            {
                'ticker': str,
                'name': str,
                'market': str,
                'marketCap': float,
                'locale': str,
                'currencyName': str,
                'cik': str,
                'filingType': str,
                'sicCode': str,
                'sicDescription': str,
            }
        """
        if not self._is_healthy:
            return None

        try:
            data = self._get(f"/v3/reference/tickers/{ticker}")

            if not data or not data.get('results'):
                return None

            result = data['results']

            return {
                'ticker': ticker,
                'name': result.get('name', ''),
                'market': result.get('market', ''),
                'marketCap': float(result.get('market_cap', 0)) if result.get('market_cap') else None,
                'locale': result.get('locale', ''),
                'currencyName': result.get('currency_name', ''),
                'cik': result.get('cik', ''),
                'filingType': result.get('type', ''),
                'sicCode': result.get('sic_code', ''),
                'sicDescription': result.get('sic_description', ''),
                'primaryExchange': result.get('primary_exchange', ''),
            }

        except Exception as e:
            logger.warning(f"Polygon get_ticker_details {ticker}: {e}")
            return None
