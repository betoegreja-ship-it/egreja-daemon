"""
Long Horizon Data Ingestion Module

Master coordinator for collecting data from 3 providers:
1. BRAPI (Brazilian market fundamentals, dividends, economic indicators)
2. OpLab (B3 options data, Greeks, volatility)
3. Polygon (US market data for ADRs and cross-validation)

Handles parallel collection, normalization, merging, and quality metrics.
Degrades gracefully if one provider fails.
"""

import logging
import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any
from threading import Lock
import json

from .brapi_provider import BRAPIProvider
from .oplab_provider import OpLabProvider
from .polygon_provider import PolygonProvider
from .normalizer import DataNormalizer

logger = logging.getLogger(__name__)


class LongHorizonDataCollector:
    """
    Master coordinator for long-horizon data collection.

    Collects from all 3 providers in parallel, normalizes, merges,
    and returns unified asset profiles for scoring.
    """

    # B3 stocks (47 major tickers on B3)
    B3_TICKERS = [
        'PETR4', 'VALE3', 'ITUB4', 'BBDC4', 'ABEV3', 'WEGE3', 'RENT3', 'LREN3',
        'SUZB3', 'GGBR4', 'EMBR3', 'CSNA3', 'CMIG4', 'CPLE6', 'BBAS3', 'VIVT3',
        'SBSP3', 'CSAN3', 'GOAU4', 'USIM5', 'BPAC11', 'RADL3', 'PRIO3', 'BRFS3',
        'MRFG3', 'JBSS3', 'EGIE3', 'CMIN3', 'AESB3', 'BBDC3', 'BBSE3', 'ALOS3',
        'MULT3', 'SMFT3', 'EQTL3', 'TAEE11', 'ENEV3', 'CPFE3', 'CXSE3', 'VBBR3',
        'UGPA3', 'KLBN11', 'TOTS3', 'MGLU3', 'CASH3', 'HAPV3', 'RDOR3', 'HYPE3',
        'COGN3', 'YDUQ3', 'NTCO3', 'AZUL4', 'CCRO3', 'MDIA3', 'ALPA4', 'POMO4',
        'AMER3', 'RECV3'
    ]

    # US stocks (40 major tickers)
    US_TICKERS = [
        'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NFLX',
        'AMD', 'INTC', 'JPM', 'BAC', 'GS', 'MS', 'V', 'MA',
        'JNJ', 'PFE', 'UNH', 'XOM', 'CVX', 'COP', 'DIS', 'UBER',
        'LYFT', 'SPOT', 'COIN', 'SPY', 'QQQ', 'IWM', 'TSM', 'AVGO',
        'MU', 'ARM', 'SMCI', 'ADBE', 'CRM', 'NOW', 'ORCL', 'SNOW',
        'SHOP', 'MELI', 'HOOD', 'HUBS', 'TCOM', 'BABA', 'LLY', 'TME',
        'PLTR', 'OKLO', 'TGT'
    ]

    # Combined universe
    UNIVERSE = B3_TICKERS + US_TICKERS

    # B3 to ADR mapping (expanded)
    B3_TO_ADR = {
        'PETR4': 'PBR',
        'VALE3': 'VALE',
        'ITUB4': 'ITUB',
        'BBDC4': 'BBD',
        'ABEV3': 'ABEV',
        'WEGE3': None,
        'RENT3': None,
        'LREN3': None,
        'SUZB3': 'SUZ',
        'GGBR4': 'GGB',
        'EMBR3': 'ERJ',
        'CSNA3': None,
        'CMIG4': 'CIG',
        'CPLE6': None,
        'BBAS3': 'BDORY',  # OTC
        'VIVT3': None,
        'SBSP3': None,
        'CSAN3': None,
        'GOAU4': None,
        'USIM5': None,
        'BPAC11': None,
        'RADL3': None,
        'PRIO3': None,
        'BRFS3': 'BRFS',
        'MRFG3': None,
        'JBSS3': 'JBSAY',  # OTC
        'EGIE3': None,
        'CMIN3': None,
        'AESB3': None,
        'BBDC3': None,
        'BBSE3': None,
        'ALOS3': None,
        'MULT3': None,
        'SMFT3': None,
        'EQTL3': None,
        'TAEE11': None,
        'ENEV3': None,
        'CPFE3': None,
        'CXSE3': None,
        'VBBR3': None,
        'UGPA3': None,
        'KLBN11': None,
        'TOTS3': None,
        'MGLU3': None,
        'CASH3': None,
        'HAPV3': None,
        'RDOR3': None,
        'HYPE3': None,
        'COGN3': None,
        'YDUQ3': None,
        'NTCO3': None,
        'AZUL4': 'AZUL',
        'CCRO3': None,
        'MDIA3': None,
        'ALPA4': None,
        'POMO4': None,
        'AMER3': None,
        'RECV3': None,
    }

    def __init__(self, max_workers: int = 3, timeout: int = 30):
        """
        Initialize data collector with all providers.

        Args:
            max_workers: Number of parallel threads for provider calls
            timeout: Timeout in seconds for each provider call
        """
        self.max_workers = max_workers
        self.timeout = timeout

        # Initialize providers
        self.brapi = BRAPIProvider()
        self.oplab = OpLabProvider()
        self.polygon = PolygonProvider()

        # Normalizer
        self.normalizer = DataNormalizer()

        # Cache and locks
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_lock = Lock()
        self._cache_ttl = 300  # 5 minutes

        # Data quality metrics
        self.quality_metrics = {
            'last_collection': None,
            'collection_duration': 0,
            'providers_available': {},
            'assets_by_completeness': {},
        }

        logger.info("LongHorizonDataCollector initialized")

    # ─── Public API ──────────────────────────────────────────────────

    def collect_all(self, ticker: str) -> Dict[str, Any]:
        """
        Collect ALL available data for a single ticker from ALL providers.

        Calls BRAPI, OpLab, and Polygon in parallel. Returns unified profile
        with ALL available information, gracefully handling partial failures.

        Args:
            ticker: Stock ticker (e.g., 'PETR4', 'ABEV3')

        Returns:
            Unified asset profile dict with all data organized by dimension
        """
        # Check cache first
        cached = self._get_cached(ticker)
        if cached:
            logger.debug(f"Returning cached data for {ticker}")
            return cached

        logger.info(f"Starting data collection for {ticker}")
        t0 = time.time()

        # Collect from all providers in parallel
        results = self._collect_parallel(ticker)

        duration = time.time() - t0
        logger.info(f"Data collection for {ticker} completed in {duration:.2f}s")

        # Normalize and merge
        unified_profile = self.normalizer.build_unified_asset_profile(
            ticker=ticker,
            brapi_data=results.get('brapi'),
            oplab_data=results.get('oplab'),
            polygon_data=results.get('polygon'),
        )

        # Calculate quality score
        unified_profile['data_quality']['quality_score'] = (
            self.normalizer.calculate_data_quality_score(unified_profile)
        )

        # Cache
        self._cache_set(ticker, unified_profile)

        return unified_profile

    def collect_universe(self) -> Dict[str, Dict[str, Any]]:
        """
        Collect data for all UNIVERSE assets in parallel.

        Returns:
            {ticker: unified_profile_dict, ...}
        """
        logger.info(f"Collecting data for all {len(self.UNIVERSE)} universe assets")
        t0 = time.time()

        results = {}

        # Collect all tickers in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.collect_all, ticker): ticker
                for ticker in self.UNIVERSE
            }

            for future in as_completed(futures, timeout=self.timeout * 3):
                ticker = futures[future]
                try:
                    profile = future.result(timeout=self.timeout)
                    results[ticker] = profile
                    logger.debug(f"✓ {ticker}")
                except Exception as e:
                    logger.error(f"✗ {ticker}: {e}")
                    results[ticker] = None

        duration = time.time() - t0
        success_count = sum(1 for r in results.values() if r is not None)

        logger.info(
            f"Universe collection complete: {success_count}/{len(self.UNIVERSE)} "
            f"in {duration:.2f}s"
        )

        # Update quality metrics
        self.quality_metrics['last_collection'] = datetime.utcnow().isoformat()
        self.quality_metrics['collection_duration'] = duration
        self.quality_metrics['assets_by_completeness'] = {
            ticker: (
                results[ticker].get('data_quality', {}).get('quality_score', 0)
                if results[ticker] else 0
            )
            for ticker in self.UNIVERSE
        }

        return results

    def get_quality_metrics(self) -> Dict[str, Any]:
        """Return data collection quality metrics."""
        return dict(self.quality_metrics)

    def health_check(self) -> Dict[str, Any]:
        """
        Check health of all providers and system.

        Returns:
            {
                'timestamp': ISO string,
                'healthy': bool,
                'providers': {
                    'brapi': bool,
                    'oplab': bool,
                    'polygon': bool,
                },
                'status': str,
            }
        """
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'healthy': (
                self.brapi.is_healthy() or
                self.oplab.is_healthy() or
                self.polygon.is_healthy()
            ),
            'providers': {
                'brapi': self.brapi.is_healthy(),
                'oplab': self.oplab.is_healthy(),
                'polygon': self.polygon.is_healthy(),
            },
            'status': 'OK' if (
                self.brapi.is_healthy() or
                self.oplab.is_healthy() or
                self.polygon.is_healthy()
            ) else 'NO_PROVIDERS_AVAILABLE',
        }

    # ─── Private Helpers ──────────────────────────────────────────────

    def _collect_parallel(self, ticker: str) -> Dict[str, Optional[Dict]]:
        """
        Collect from all 3 providers in parallel for a ticker.

        Route B3 tickers to BRAPI+OpLab, US tickers to Polygon.

        Returns: {
            'brapi': result or None,
            'oplab': result or None,
            'polygon': result or None,
        }
        """
        results = {}

        # Determine if this is a B3 or US ticker
        is_b3_ticker = ticker in self.B3_TICKERS
        is_us_ticker = ticker in self.US_TICKERS

        with ThreadPoolExecutor(max_workers=3) as executor:
            # Submit jobs based on ticker type
            futures = {}

            if is_b3_ticker:
                # B3 tickers use BRAPI and OpLab
                futures['brapi'] = executor.submit(self._fetch_brapi, ticker)
                futures['oplab'] = executor.submit(self._fetch_oplab, ticker)
            elif is_us_ticker:
                # US tickers use Polygon
                futures['polygon'] = executor.submit(self._fetch_polygon, ticker)

            # Wait for completion
            for provider, future in futures.items():
                try:
                    results[provider] = future.result(timeout=self.timeout)
                except Exception as e:
                    logger.warning(f"{provider} fetch failed for {ticker}: {e}")
                    results[provider] = None

        return results

    def _fetch_brapi(self, ticker: str) -> Optional[Dict]:
        """Fetch from BRAPI provider."""
        try:
            if not self.brapi.is_healthy():
                return None

            # Collect quote, fundamentals, historical, dividends, economic data
            return {
                'quote': self.brapi.get_quote(ticker),
                'fundamentals': self.brapi.get_fundamentals(ticker),
                'historical': self.brapi.get_historical(ticker, range='5y'),
                'dividends': self.brapi.get_dividends(ticker),
                'economic_indicators': self.brapi.get_economic_indicators(),
            }
        except Exception as e:
            logger.warning(f"BRAPI fetch error for {ticker}: {e}")
            return None

    def _fetch_oplab(self, ticker: str) -> Optional[Dict]:
        """Fetch from OpLab provider."""
        try:
            if not self.oplab.is_healthy():
                return None

            # Collect options, greeks, IV, volatility data
            return {
                'options_chain': self.oplab.get_options_chain(ticker),
                'greeks_summary': self.oplab.get_greeks_summary(ticker),
                'iv_surface': self.oplab.get_iv_surface(ticker),
                'skew': self.oplab.get_skew(ticker),
                'risk_reversal': self.oplab.get_risk_reversal(ticker),
                'hedge_cost': self.oplab.get_hedge_cost(ticker),
                'put_call_ratio': self.oplab.get_put_call_ratio(ticker),
                'historical_volatility': self.oplab.get_historical_volatility(ticker),
                'interest_rates': self.oplab.get_interest_rates(),
            }
        except Exception as e:
            logger.warning(f"OpLab fetch error for {ticker}: {e}")
            return None

    def _fetch_polygon(self, ticker: str) -> Optional[Dict]:
        """Fetch from Polygon provider."""
        try:
            if not self.polygon.is_healthy():
                return None

            # For B3 assets, try to fetch ADR equivalent
            adr_ticker = self.B3_TO_ADR.get(ticker)
            fetch_ticker = adr_ticker if adr_ticker else ticker

            return {
                'snapshot': self.polygon.get_snapshot(fetch_ticker),
                'aggregates': self.polygon.get_aggregates(fetch_ticker, timespan='day'),
                'options_chain': self.polygon.get_options_chain(fetch_ticker),
                'options_snapshot': self.polygon.get_options_snapshot(fetch_ticker),
                'financials': self.polygon.get_financials(fetch_ticker),
                'dividends': self.polygon.get_dividends(fetch_ticker),
                'splits': self.polygon.get_splits(fetch_ticker),
                'technical_indicators': self.polygon.get_technical_indicators(fetch_ticker),
                'news': self.polygon.get_news(fetch_ticker, limit=5),
                'related': self.polygon.get_related(fetch_ticker),
                'ticker_details': self.polygon.get_ticker_details(fetch_ticker),
            }
        except Exception as e:
            logger.warning(f"Polygon fetch error for {ticker}: {e}")
            return None

    def _get_cached(self, ticker: str) -> Optional[Dict]:
        """Get cached profile if available and not expired."""
        with self._cache_lock:
            cached = self._cache.get(ticker)
            if cached:
                age = time.time() - cached['ts']
                if age < self._cache_ttl:
                    return cached['data']
                else:
                    del self._cache[ticker]
        return None

    def _cache_set(self, ticker: str, data: Dict) -> None:
        """Cache profile with timestamp."""
        with self._cache_lock:
            self._cache[ticker] = {
                'data': data,
                'ts': time.time(),
            }

    def clear_cache(self, ticker: str = None) -> None:
        """Clear cache for specific ticker or all."""
        with self._cache_lock:
            if ticker:
                self._cache.pop(ticker, None)
            else:
                self._cache.clear()
        logger.info(f"Cache cleared for {ticker or 'all tickers'}")


# Module-level functions for easy access
_default_collector = None

def get_collector() -> LongHorizonDataCollector:
    """Get or create default collector instance."""
    global _default_collector
    if _default_collector is None:
        _default_collector = LongHorizonDataCollector()
    return _default_collector

def collect_all(ticker: str) -> Dict[str, Any]:
    """Collect all data for ticker."""
    return get_collector().collect_all(ticker)

def collect_universe() -> Dict[str, Dict[str, Any]]:
    """Collect all universe assets."""
    return get_collector().collect_universe()

def health_check() -> Dict[str, Any]:
    """Check system health."""
    return get_collector().health_check()
