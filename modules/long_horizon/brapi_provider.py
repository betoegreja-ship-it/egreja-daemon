"""
BRAPI Provider for Brazilian Stock Data

Connects to https://brapi.dev/api for comprehensive Brazilian market data:
- Stock quotes with fundamentals, historical data
- Dividend history
- Economic indicators (Selic, IPCA, IGP-M, CDI)

Auth: Bearer token via Authorization header
Env: BRAPI_TOKEN
"""

import logging
import os
import time
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from threading import Lock

logger = logging.getLogger(__name__)


class BRAPIProvider:
    """
    BRAPI Complete Connector - Brazilian Equities & Macro

    URL: https://brapi.dev/api
    Auth: Authorization: Bearer {BRAPI_TOKEN}
    """

    BASE_URL = "https://brapi.dev/api"

    def __init__(self):
        """Initialize BRAPI provider with API token."""
        self._token = os.environ.get('BRAPI_TOKEN', '').strip()
        self._session = requests.Session()
        if self._token:
            self._session.headers.update({
                'Authorization': f'Bearer {self._token}',
            })

        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_lock = Lock()
        self._cache_ttl = 300  # 5 minutes

        self._is_healthy = bool(self._token)

        if self._is_healthy:
            logger.info("BRAPI provider initialized with token")
        else:
            logger.warning("BRAPI provider disabled (no token)")

    # ─── Health & Status ──────────────────────────────────────────────

    def is_healthy(self) -> bool:
        """Check if provider is healthy and initialized."""
        return self._is_healthy

    def health_check(self) -> Dict[str, Any]:
        """Perform health check on BRAPI service."""
        if not self._is_healthy:
            return {'healthy': False, 'reason': 'No API token'}

        try:
            # Try a simple quote fetch
            r = self._session.get(
                f"{self.BASE_URL}/quote/PETR4",
                timeout=8
            )
            if r.status_code == 200:
                return {
                    'healthy': True,
                    'timestamp': datetime.utcnow().isoformat(),
                    'response_time_ms': r.elapsed.total_seconds() * 1000,
                }
            else:
                logger.warning(f"BRAPI health check returned {r.status_code}")
                self._is_healthy = False
                return {'healthy': False, 'status_code': r.status_code}
        except Exception as e:
            logger.warning(f"BRAPI health check failed: {e}")
            self._is_healthy = False
            return {'healthy': False, 'error': str(e)}

    # ─── Quote Data ───────────────────────────────────────────────────

    def get_quote(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get current quote for a ticker.

        Returns:
            {
                'ticker': str,
                'name': str,
                'price': float,
                'previousClose': float,
                'change': float,
                'changePercent': float,
                'volume': float,
                'avgVolume3m': float,
                'avgVolume10d': float,
                '50dayAverage': float,
                '200dayAverage': float,
                '52weekHigh': float,
                '52weekLow': float,
                'marketCap': float,
                'enterpriseValue': float,
                'pe': float,
                'eps': float,
                'timestamp': str,
            }
        """
        if not self._is_healthy:
            return None

        try:
            url = f"{self.BASE_URL}/quote/{ticker}"
            r = self._session.get(url, timeout=8)
            if r.status_code != 200:
                logger.warning(f"BRAPI quote {ticker}: HTTP {r.status_code}")
                return None

            data = r.json()
            results = data.get('results', [])
            if not results:
                return None

            q = results[0]

            return {
                'ticker': ticker,
                'name': q.get('longName', ''),
                'price': float(q.get('regularMarketPrice', 0)),
                'previousClose': float(q.get('regularMarketPreviousClose', 0)),
                'change': float(q.get('regularMarketChange', 0)),
                'changePercent': float(q.get('regularMarketChangePercent', 0)),
                'volume': float(q.get('regularMarketVolume', 0)),
                'avgVolume3m': float(q.get('averageDailyVolume3Month', 0)),
                'avgVolume10d': float(q.get('averageDailyVolume10Day', 0)),
                '50dayAverage': float(q.get('fiftyDayAverage', 0)),
                '200dayAverage': float(q.get('twoHundredDayAverage', 0)),
                '52weekHigh': float(q.get('fiftyTwoWeekHigh', 0)),
                '52weekLow': float(q.get('fiftyTwoWeekLow', 0)),
                'marketCap': float(q.get('marketCap', 0)),
                'enterpriseValue': float(q.get('enterpriseValue', 0)),
                'pe': float(q.get('priceEarnings', 0)) if q.get('priceEarnings') else None,
                'eps': float(q.get('earningsPerShare', 0)) if q.get('earningsPerShare') else None,
                'timestamp': datetime.utcnow().isoformat(),
            }
        except Exception as e:
            logger.warning(f"BRAPI get_quote {ticker}: {e}")
            return None

    def get_batch_quotes(self, tickers: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get quotes for up to 20 tickers in a single call.

        Returns: {ticker: quote_dict, ...}
        """
        if not self._is_healthy or not tickers:
            return {}

        results = {}

        # Process in chunks of 20 (BRAPI limit)
        for i in range(0, len(tickers), 20):
            chunk = tickers[i:i+20]
            try:
                url = f"{self.BASE_URL}/quote/{','.join(chunk)}"
                r = self._session.get(url, timeout=12)
                if r.status_code != 200:
                    logger.warning(f"BRAPI batch quotes: HTTP {r.status_code}")
                    continue

                data = r.json()
                for q in data.get('results', []):
                    ticker = q.get('symbol', '').replace('.SA', '')
                    if ticker:
                        results[ticker] = {
                            'ticker': ticker,
                            'name': q.get('longName', ''),
                            'price': float(q.get('regularMarketPrice', 0)),
                            'previousClose': float(q.get('regularMarketPreviousClose', 0)),
                            'change': float(q.get('regularMarketChange', 0)),
                            'changePercent': float(q.get('regularMarketChangePercent', 0)),
                            'volume': float(q.get('regularMarketVolume', 0)),
                        }
            except Exception as e:
                logger.warning(f"BRAPI batch quotes chunk {chunk}: {e}")

        return results

    # ─── Fundamentals ─────────────────────────────────────────────────

    def get_fundamentals(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive fundamentals and financial data.

        Returns:
            {
                'sector': str,
                'industry': str,
                'description': str,
                'employees': int,
                'roe': float,
                'roic': float,
                'returnOnAssets': float,
                'grossMargin': float,
                'operatingMargin': float,
                'netMargin': float,
                'currentPrice': float,
                'targetMeanPrice': float,
                'recommendations': {mean, count},
                'pe': float,
                'pb': float,
                'priceSales': float,
                'pegRatio': float,
                'evRevenue': float,
                'evEbitda': float,
                'beta': float,
                'debtToEquity': float,
                'currentRatio': float,
                'quickRatio': float,
                'totalAssets': float,
                'totalLiabilities': float,
                'stockholdersEquity': float,
                'totalRevenue': float,
                'grossProfit': float,
                'ebit': float,
                'netIncome': float,
                'operatingCashflow': float,
                'freeCashflow': float,
                'dividendRate': float,
                'dividendYield': float,
                'payoutRatio': float,
                'timestamp': str,
            }
        """
        if not self._is_healthy:
            return None

        try:
            params = {
                'modules': ','.join([
                    'summaryProfile',
                    'financialData',
                    'defaultKeyStatistics',
                    'balanceSheetHistory',
                    'incomeStatementHistory',
                    'cashflowStatementHistory',
                ])
            }
            url = f"{self.BASE_URL}/quote/{ticker}"
            r = self._session.get(url, params=params, timeout=12)

            if r.status_code != 200:
                logger.warning(f"BRAPI fundamentals {ticker}: HTTP {r.status_code}")
                return None

            data = r.json()
            results = data.get('results', [])
            if not results:
                return None

            q = results[0]
            profile = q.get('summaryProfile', {})
            fin = q.get('financialData', {})
            stats = q.get('defaultKeyStatistics', {})
            balance = q.get('balanceSheetHistory', {})
            income = q.get('incomeStatementHistory', {})
            cashflow = q.get('cashflowStatementHistory', {})

            # Extract most recent balance sheet
            bal_data = {}
            if balance and isinstance(balance, dict):
                bal_hist = balance.get('balanceSheetStatements', [])
                if bal_hist:
                    bal_data = bal_hist[0]

            # Extract most recent income statement
            inc_data = {}
            if income and isinstance(income, dict):
                inc_hist = income.get('incomeStatementHistory', [])
                if inc_hist:
                    inc_data = inc_hist[0]

            # Extract most recent cashflow
            cf_data = {}
            if cashflow and isinstance(cashflow, dict):
                cf_hist = cashflow.get('cashflowStatements', [])
                if cf_hist:
                    cf_data = cf_hist[0]

            return {
                'ticker': ticker,
                'sector': profile.get('sector'),
                'industry': profile.get('industry'),
                'description': profile.get('longBusinessSummary', '')[:500],
                'employees': profile.get('fullTimeEmployees'),
                'roe': float(fin.get('returnOnEquity', 0)) if fin.get('returnOnEquity') else None,
                'roic': float(stats.get('returnOnCapital', 0)) if stats.get('returnOnCapital') else None,
                'returnOnAssets': float(fin.get('returnOnAssets', 0)) if fin.get('returnOnAssets') else None,
                'grossMargin': float(fin.get('grossMargins', 0)) if fin.get('grossMargins') else None,
                'operatingMargin': float(fin.get('operatingMargins', 0)) if fin.get('operatingMargins') else None,
                'netMargin': float(fin.get('profitMargins', 0)) if fin.get('profitMargins') else None,
                'currentPrice': float(fin.get('currentPrice', 0)) if fin.get('currentPrice') else None,
                'targetMeanPrice': float(fin.get('targetMeanPrice', 0)) if fin.get('targetMeanPrice') else None,
                'recommendationMean': float(fin.get('recommendationMean', 0)) if fin.get('recommendationMean') else None,
                'recommendationCount': fin.get('numberOfAnalystOpinions'),
                'pe': float(stats.get('trailingPE', 0)) if stats.get('trailingPE') else None,
                'pb': float(stats.get('priceToBook', 0)) if stats.get('priceToBook') else None,
                'priceSales': float(stats.get('priceToSalesTrailing12Months', 0)) if stats.get('priceToSalesTrailing12Months') else None,
                'pegRatio': float(stats.get('pegRatio', 0)) if stats.get('pegRatio') else None,
                'evRevenue': float(stats.get('enterpriseToRevenue', 0)) if stats.get('enterpriseToRevenue') else None,
                'evEbitda': float(stats.get('enterpriseToEbitda', 0)) if stats.get('enterpriseToEbitda') else None,
                'beta': float(stats.get('beta', 0)) if stats.get('beta') else None,
                'debtToEquity': float(fin.get('debtToEquity', 0)) if fin.get('debtToEquity') else None,
                'currentRatio': float(fin.get('currentRatio', 0)) if fin.get('currentRatio') else None,
                'quickRatio': float(fin.get('quickRatio', 0)) if fin.get('quickRatio') else None,
                'totalAssets': float(bal_data.get('totalAssets', 0)) if bal_data.get('totalAssets') else None,
                'totalLiabilities': float(bal_data.get('totalLiabilities', 0)) if bal_data.get('totalLiabilities') else None,
                'stockholdersEquity': float(bal_data.get('stockholdersEquity', 0)) if bal_data.get('stockholdersEquity') else None,
                'totalRevenue': float(inc_data.get('totalRevenue', 0)) if inc_data.get('totalRevenue') else None,
                'grossProfit': float(inc_data.get('grossProfit', 0)) if inc_data.get('grossProfit') else None,
                'ebit': float(inc_data.get('ebit', 0)) if inc_data.get('ebit') else None,
                'netIncome': float(inc_data.get('netIncome', 0)) if inc_data.get('netIncome') else None,
                'operatingCashflow': float(cf_data.get('operatingCashflow', 0)) if cf_data.get('operatingCashflow') else None,
                'freeCashflow': float(cf_data.get('freeCashflow', 0)) if cf_data.get('freeCashflow') else None,
                'dividendRate': float(stats.get('trailingAnnualDividendRate', 0)) if stats.get('trailingAnnualDividendRate') else None,
                'dividendYield': float(stats.get('trailingAnnualDividendYield', 0)) if stats.get('trailingAnnualDividendYield') else None,
                'payoutRatio': float(stats.get('payoutRatio', 0)) if stats.get('payoutRatio') else None,
                'revenueGrowth': float(fin.get('revenueGrowth', 0)) if fin.get('revenueGrowth') else None,
                'earningsGrowth': float(fin.get('earningsGrowth', 0)) if fin.get('earningsGrowth') else None,
                'sharesOutstanding': float(stats.get('sharesOutstanding', 0)) if stats.get('sharesOutstanding') else None,
                'timestamp': datetime.utcnow().isoformat(),
            }
        except Exception as e:
            logger.warning(f"BRAPI get_fundamentals {ticker}: {e}")
            return None

    # ─── Historical Data ──────────────────────────────────────────────

    def get_historical(self, ticker: str, range: str = '5y', interval: str = '1mo') -> Optional[List[Dict[str, Any]]]:
        """
        Get historical OHLCV data.

        Args:
            ticker: Stock ticker
            range: '1mo', '3mo', '6mo', '1y', '5y'
            interval: '1d', '1wk', '1mo'

        Returns:
            List of candles:
            [{
                'date': str,
                'open': float,
                'high': float,
                'low': float,
                'close': float,
                'volume': float,
            }, ...]
        """
        if not self._is_healthy:
            return None

        try:
            params = {
                'range': range,
                'interval': interval,
                'fundamental': 'false',
            }
            url = f"{self.BASE_URL}/quote/{ticker}"
            r = self._session.get(url, params=params, timeout=12)

            if r.status_code != 200:
                logger.warning(f"BRAPI historical {ticker}: HTTP {r.status_code}")
                return None

            data = r.json()
            results = data.get('results', [])
            if not results:
                return None

            hist_data = results[0].get('historicalDataPrice', [])
            candles = []

            for h in hist_data:
                candles.append({
                    'date': h.get('date', ''),
                    'open': float(h.get('open', 0)),
                    'high': float(h.get('high', 0)),
                    'low': float(h.get('low', 0)),
                    'close': float(h.get('close', 0)),
                    'volume': float(h.get('volume', 0)),
                })

            return candles

        except Exception as e:
            logger.warning(f"BRAPI get_historical {ticker}: {e}")
            return None

    # ─── Dividends ────────────────────────────────────────────────────

    def get_dividends(self, ticker: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get complete dividend history.

        Returns:
            [{
                'exDate': str (YYYY-MM-DD),
                'paymentDate': str,
                'amount': float,
                'type': str ('dividend', 'JSCP', etc),
            }, ...]
        """
        if not self._is_healthy:
            return None

        try:
            params = {'dividends': 'true'}
            url = f"{self.BASE_URL}/quote/{ticker}"
            r = self._session.get(url, params=params, timeout=8)

            if r.status_code != 200:
                logger.warning(f"BRAPI dividends {ticker}: HTTP {r.status_code}")
                return None

            data = r.json()
            results = data.get('results', [])
            if not results:
                return None

            divs = results[0].get('dividends', [])
            dividends = []

            for d in divs:
                dividends.append({
                    'exDate': d.get('exDate', ''),
                    'paymentDate': d.get('paymentDate', ''),
                    'amount': float(d.get('amount', 0)),
                    'type': d.get('type', 'dividend'),
                })

            return sorted(dividends, key=lambda x: x['exDate'], reverse=True)

        except Exception as e:
            logger.warning(f"BRAPI get_dividends {ticker}: {e}")
            return None

    # ─── Economic Indicators ──────────────────────────────────────────

    def get_economic_indicators(self) -> Optional[Dict[str, Any]]:
        """
        Get Brazilian economic indicators (Selic, IPCA, CDI, etc).

        Returns:
            {
                'selic': float,
                'selic_target': float,
                'ipca': float,
                'igpm': float,
                'cdi': float,
                'usd_brl': float,
                'timestamp': str,
            }
        """
        if not self._is_healthy:
            return None

        try:
            # Try to get from BRAPI if available
            # Note: BRAPI may not have all economic indicators - this is a template
            # In production, might need separate economic API calls
            url = f"{self.BASE_URL}/quote/PETR4"
            r = self._session.get(url, timeout=8)

            if r.status_code == 200:
                # Extract any economic data if present in response
                data = r.json()
                # BRAPI may include macro data in response
                # Fallback to defaults if not available
                pass

            # Return default structure (would be populated by actual data)
            return {
                'selic': None,
                'selic_target': None,
                'ipca': None,
                'igpm': None,
                'cdi': None,
                'usd_brl': None,
                'timestamp': datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.warning(f"BRAPI get_economic_indicators: {e}")
            return None
