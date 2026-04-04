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

    # MVP assets: 6 Brazilian stocks + Banco do Brasil + BOVA11 ETF
    MVP_ASSETS = [
        'PETR4', 'VALE3', 'ITUB4', 'BBDC4', 'BBAS3', 'ABEV3', 'B3SA3', 'BOVA11'
    ]

    # B3 to ADR mapping
    B3_TO_ADR = {
        'PETR4': 'PBR',
        'VALE3': 'VALE',
        'ITUB4': 'ITUB',
        'BBDC4': 'BBD',
        'BBAS3': None,      # No direct ADR
        'ABEV3': 'ABEV',
        'B3SA3': None,      # No ADR
        'BOVA11': None,     # ETF - no ADR equivalent
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
        Collect data for all MVP assets in parallel.

        Returns:
            {ticker: unified_profile_dict, ...}
        """
        logger.info(f"Collecting data for all {len(self.MVP_ASSETS)} MVP assets")
        t0 = time.time()

        results = {}

        # Collect all tickers in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.collect_all, ticker): ticker
                for ticker in self.MVP_ASSETS
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
            f"Universe collection complete: {success_count}/{len(self.MVP_ASSETS)} "
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
            for ticker in self.MVP_ASSETS
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

        Returns: {
            'brapi': result or None,
            'oplab': result or None,
            'polygon': result or None,
        }
        """
        results = {}

        with ThreadPoolExecutor(max_workers=3) as executor:
            # Submit all jobs
            futures = {
                'brapi': executor.submit(self._fetch_brapi, ticker),
                'oplab': executor.submit(self._fetch_oplab, ticker),
                'polygon': executor.submit(self._fetch_polygon, ticker),
            }

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
    """Collect all MVP assets."""
    return get_collector().collect_universe()

def health_check() -> Dict[str, Any]:
    """Check system health."""
    return get_collector().health_check()
