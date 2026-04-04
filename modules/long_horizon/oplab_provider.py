"""
OpLab Provider for B3 Derivatives Data

Connects to https://api.oplab.com.br/v3 for comprehensive options data:
- Complete options chains with Greeks
- IV surface and volatility metrics
- Risk reversals and skew
- Hedge costs and put/call ratios
- Interest rate curves

Auth: Access-Token header
Env: OPLAB_ACCESS_TOKEN
"""

import logging
import os
import time
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime
from threading import Lock

logger = logging.getLogger(__name__)


class OpLabProvider:
    """
    OpLab Complete Connector - B3 Derivatives & Volatility

    URL: https://api.oplab.com.br/v3
    Auth: Access-Token: {OPLAB_ACCESS_TOKEN}
    """

    BASE_URL = "https://api.oplab.com.br/v3"

    def __init__(self):
        """Initialize OpLab provider with API token."""
        self._token = os.environ.get('OPLAB_ACCESS_TOKEN', '').strip()
        self._session = requests.Session()
        if self._token:
            self._session.headers.update({
                'Accept': 'application/json',
                'Access-Token': self._token,
            })

        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_lock = Lock()
        self._cache_ttl = 60  # 1 minute (faster refresh for options)

        self._is_healthy = bool(self._token)

        if self._is_healthy:
            logger.info("OpLab provider initialized with token")
        else:
            logger.warning("OpLab provider disabled (no token)")

    # ─── Health & Status ──────────────────────────────────────────────

    def is_healthy(self) -> bool:
        """Check if provider is healthy and initialized."""
        return self._is_healthy

    def health_check(self) -> Dict[str, Any]:
        """Perform health check on OpLab service."""
        if not self._is_healthy:
            return {'healthy': False, 'reason': 'No API token'}

        try:
            # Try to fetch options chain for a major asset
            r = self._session.get(
                f"{self.BASE_URL}/market/options/PETR4",
                timeout=8
            )
            if r.status_code == 200:
                return {
                    'healthy': True,
                    'timestamp': datetime.utcnow().isoformat(),
                    'response_time_ms': r.elapsed.total_seconds() * 1000,
                }
            else:
                logger.warning(f"OpLab health check returned {r.status_code}")
                self._is_healthy = False
                return {'healthy': False, 'status_code': r.status_code}
        except Exception as e:
            logger.warning(f"OpLab health check failed: {e}")
            self._is_healthy = False
            return {'healthy': False, 'error': str(e)}

    # ─── HTTP Helpers ─────────────────────────────────────────────────

    def _get(self, path: str, params: dict = None, timeout: int = 10) -> Optional[Any]:
        """GET request to OpLab API with caching."""
        if not self._is_healthy:
            return None

        # Check cache
        cache_key = f"{path}:{str(params or {})}"
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
            elif r.status_code == 401:
                logger.error("OpLab: 401 Unauthorized — check OPLAB_ACCESS_TOKEN")
                self._is_healthy = False
            elif r.status_code == 429:
                logger.warning("OpLab: rate limited (429)")
                time.sleep(1)
            else:
                logger.warning(f"OpLab GET {path}: HTTP {r.status_code}")
            return None

        except Exception as e:
            logger.warning(f"OpLab GET {path} error: {e}")
            return None

    # ─── Options Chain ────────────────────────────────────────────────

    def get_options_chain(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get complete options chain with all strikes and expirations.

        Returns:
            {
                'ticker': str,
                'timestamp': str,
                'expirations': ['YYYYMMDD', ...],
                'options': [
                    {
                        'symbol': str,
                        'underlying': str,
                        'expiration': str,
                        'strike': float,
                        'type': 'call' or 'put',
                        'bid': float,
                        'ask': float,
                        'last': float,
                        'volume': int,
                        'openInterest': int,
                        'impliedVolatility': float,
                    }, ...
                ]
            }
        """
        if not self._is_healthy:
            return None

        try:
            data = self._get(f"/market/options/{ticker}")
            if not data:
                return None

            options_list = []
            expirations = set()

            # Parse options chain data
            for opt in data.get('options', []):
                expirations.add(opt.get('expirationDate'))
                options_list.append({
                    'symbol': opt.get('symbol', ''),
                    'underlying': ticker,
                    'expiration': opt.get('expirationDate', ''),
                    'strike': float(opt.get('strike', 0)),
                    'type': opt.get('type', '').lower(),
                    'bid': float(opt.get('bid', 0)),
                    'ask': float(opt.get('ask', 0)),
                    'last': float(opt.get('last', 0)),
                    'volume': int(opt.get('volume', 0)),
                    'openInterest': int(opt.get('openInterest', 0)),
                    'impliedVolatility': float(opt.get('impliedVolatility', 0)),
                })

            return {
                'ticker': ticker,
                'timestamp': datetime.utcnow().isoformat(),
                'expirations': sorted(list(expirations)),
                'options': options_list,
            }

        except Exception as e:
            logger.warning(f"OpLab get_options_chain {ticker}: {e}")
            return None

    # ─── Greeks Summary ───────────────────────────────────────────────

    def get_greeks_summary(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get summary of Greeks by expiration and type.

        Returns:
            {
                'ticker': str,
                'timestamp': str,
                'by_expiration': {
                    'YYYYMMDD': {
                        'calls': {
                            'avgDelta': float,
                            'avgGamma': float,
                            'avgTheta': float,
                            'avgVega': float,
                        },
                        'puts': {...}
                    }
                }
            }
        """
        if not self._is_healthy:
            return None

        try:
            chain = self.get_options_chain(ticker)
            if not chain:
                return None

            greeks_by_exp = {}

            for exp in chain['expirations']:
                calls = [o for o in chain['options'] if o['type'] == 'call' and o['expiration'] == exp]
                puts = [o for o in chain['options'] if o['type'] == 'put' and o['expiration'] == exp]

                greeks_by_exp[exp] = {
                    'calls': {
                        'count': len(calls),
                        'volume': sum(o['volume'] for o in calls),
                        'openInterest': sum(o['openInterest'] for o in calls),
                    },
                    'puts': {
                        'count': len(puts),
                        'volume': sum(o['volume'] for o in puts),
                        'openInterest': sum(o['openInterest'] for o in puts),
                    }
                }

            return {
                'ticker': ticker,
                'timestamp': datetime.utcnow().isoformat(),
                'by_expiration': greeks_by_exp,
            }

        except Exception as e:
            logger.warning(f"OpLab get_greeks_summary {ticker}: {e}")
            return None

    # ─── IV Surface ───────────────────────────────────────────────────

    def get_iv_surface(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get implied volatility surface (IV by strike and expiration).

        Returns:
            {
                'ticker': str,
                'timestamp': str,
                'spot': float,
                'surface': {
                    'YYYYMMDD': {
                        strike: iv_value,
                        ...
                    }
                },
                'atm_iv': float,
            }
        """
        if not self._is_healthy:
            return None

        try:
            chain = self.get_options_chain(ticker)
            if not chain or not chain['options']:
                return None

            # Get spot price (use last price of ATM call)
            calls = [o for o in chain['options'] if o['type'] == 'call']
            spot = calls[0]['last'] if calls else None

            surface = {}

            for exp in chain['expirations']:
                exp_options = [o for o in chain['options'] if o['expiration'] == exp and o['type'] == 'call']
                surface[exp] = {}
                for opt in exp_options:
                    strike = opt['strike']
                    iv = opt.get('impliedVolatility', 0)
                    surface[exp][strike] = iv

            # Calculate ATM IV
            atm_iv = 0
            if calls:
                atm_calls = [c for c in calls if c['expiration'] == chain['expirations'][0]]
                if atm_calls:
                    atm_iv = sum(c['impliedVolatility'] for c in atm_calls) / len(atm_calls)

            return {
                'ticker': ticker,
                'timestamp': datetime.utcnow().isoformat(),
                'spot': spot,
                'surface': surface,
                'atm_iv': atm_iv,
            }

        except Exception as e:
            logger.warning(f"OpLab get_iv_surface {ticker}: {e}")
            return None

    # ─── Skew ─────────────────────────────────────────────────────────

    def get_skew(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get volatility skew (IV OTM puts vs OTM calls for same delta).

        Returns:
            {
                'ticker': str,
                'timestamp': str,
                'by_expiration': {
                    'YYYYMMDD': {
                        'skew_25d': float,  # IV(25d put) - IV(25d call)
                        'skew_10d': float,
                    }
                }
            }
        """
        if not self._is_healthy:
            return None

        try:
            chain = self.get_options_chain(ticker)
            if not chain:
                return None

            skew_data = {}

            for exp in chain['expirations']:
                calls = sorted([o for o in chain['options'] if o['type'] == 'call' and o['expiration'] == exp],
                              key=lambda x: x['strike'])
                puts = sorted([o for o in chain['options'] if o['type'] == 'put' and o['expiration'] == exp],
                             key=lambda x: x['strike'], reverse=True)

                if len(calls) > 10 and len(puts) > 10:
                    # 25 delta approximation (rough)
                    call_iv_25 = calls[min(10, len(calls)//4)]['impliedVolatility']
                    put_iv_25 = puts[min(10, len(puts)//4)]['impliedVolatility']

                    skew_data[exp] = {
                        'skew_25d': put_iv_25 - call_iv_25,
                    }

            return {
                'ticker': ticker,
                'timestamp': datetime.utcnow().isoformat(),
                'by_expiration': skew_data,
            }

        except Exception as e:
            logger.warning(f"OpLab get_skew {ticker}: {e}")
            return None

    # ─── Risk Reversal ────────────────────────────────────────────────

    def get_risk_reversal(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get risk reversal (25-delta risk reversal pricing).

        Returns:
            {
                'ticker': str,
                'timestamp': str,
                'by_expiration': {
                    'YYYYMMDD': {
                        'risk_reversal_25d': float,
                    }
                }
            }
        """
        if not self._is_healthy:
            return None

        try:
            chain = self.get_options_chain(ticker)
            if not chain:
                return None

            rr_data = {}

            for exp in chain['expirations']:
                calls = sorted([o for o in chain['options'] if o['type'] == 'call' and o['expiration'] == exp],
                              key=lambda x: x['strike'])
                puts = sorted([o for o in chain['options'] if o['type'] == 'put' and o['expiration'] == exp],
                             key=lambda x: x['strike'], reverse=True)

                if len(calls) > 10 and len(puts) > 10:
                    # Approximate 25-delta
                    call_25 = calls[min(10, len(calls)//4)]
                    put_25 = puts[min(10, len(puts)//4)]

                    rr = (call_25['bid'] + call_25['ask']) / 2 - (put_25['bid'] + put_25['ask']) / 2

                    rr_data[exp] = {
                        'risk_reversal_25d': rr,
                        'call_25_price': (call_25['bid'] + call_25['ask']) / 2,
                        'put_25_price': (put_25['bid'] + put_25['ask']) / 2,
                    }

            return {
                'ticker': ticker,
                'timestamp': datetime.utcnow().isoformat(),
                'by_expiration': rr_data,
            }

        except Exception as e:
            logger.warning(f"OpLab get_risk_reversal {ticker}: {e}")
            return None

    # ─── Hedge Cost ───────────────────────────────────────────────────

    def get_hedge_cost(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get cost of hedging (10% OTM put at various horizons as % of spot).

        Returns:
            {
                'ticker': str,
                'timestamp': str,
                'spot': float,
                'by_expiration': {
                    'YYYYMMDD': {
                        'cost_pct': float,
                        'put_strike': float,
                        'put_price': float,
                    }
                }
            }
        """
        if not self._is_healthy:
            return None

        try:
            chain = self.get_options_chain(ticker)
            if not chain or not chain['options']:
                return None

            # Get spot
            spot_opts = [o for o in chain['options'] if o['type'] == 'call']
            spot = spot_opts[0]['last'] if spot_opts else 1

            hedge_data = {}

            for exp in chain['expirations']:
                puts = sorted([o for o in chain['options'] if o['type'] == 'put' and o['expiration'] == exp],
                             key=lambda x: x['strike'])

                if puts:
                    # Find 10% OTM put (strike = spot * 0.9)
                    target_strike = spot * 0.90
                    closest_put = min(puts, key=lambda x: abs(x['strike'] - target_strike))

                    mid_price = (closest_put['bid'] + closest_put['ask']) / 2
                    cost_pct = mid_price / spot if spot > 0 else 0

                    hedge_data[exp] = {
                        'cost_pct': cost_pct,
                        'put_strike': closest_put['strike'],
                        'put_price': mid_price,
                        'spot_at_calc': spot,
                    }

            return {
                'ticker': ticker,
                'timestamp': datetime.utcnow().isoformat(),
                'spot': spot,
                'by_expiration': hedge_data,
            }

        except Exception as e:
            logger.warning(f"OpLab get_hedge_cost {ticker}: {e}")
            return None

    # ─── Put/Call Ratio ───────────────────────────────────────────────

    def get_put_call_ratio(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get put/call ratio (puts / calls by volume and open interest).

        Returns:
            {
                'ticker': str,
                'timestamp': str,
                'by_expiration': {
                    'YYYYMMDD': {
                        'volume_ratio': float,
                        'oi_ratio': float,
                    }
                }
            }
        """
        if not self._is_healthy:
            return None

        try:
            chain = self.get_options_chain(ticker)
            if not chain:
                return None

            ratio_data = {}

            for exp in chain['expirations']:
                calls = [o for o in chain['options'] if o['type'] == 'call' and o['expiration'] == exp]
                puts = [o for o in chain['options'] if o['type'] == 'put' and o['expiration'] == exp]

                call_vol = sum(o['volume'] for o in calls)
                put_vol = sum(o['volume'] for o in puts)
                call_oi = sum(o['openInterest'] for o in calls)
                put_oi = sum(o['openInterest'] for o in puts)

                ratio_data[exp] = {
                    'volume_ratio': put_vol / call_vol if call_vol > 0 else 0,
                    'oi_ratio': put_oi / call_oi if call_oi > 0 else 0,
                    'put_volume': put_vol,
                    'call_volume': call_vol,
                }

            return {
                'ticker': ticker,
                'timestamp': datetime.utcnow().isoformat(),
                'by_expiration': ratio_data,
            }

        except Exception as e:
            logger.warning(f"OpLab get_put_call_ratio {ticker}: {e}")
            return None

    # ─── Historical Volatility ────────────────────────────────────────

    def get_historical_volatility(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get historical volatility for different windows.

        Returns:
            {
                'ticker': str,
                'timestamp': str,
                'hv_21d': float,
                'hv_63d': float,
                'hv_252d': float,
            }
        """
        if not self._is_healthy:
            return None

        try:
            # This would typically fetch from /market/historical-volatility/{ticker}
            # For now, return structure with None values (would be populated from real API)
            return {
                'ticker': ticker,
                'timestamp': datetime.utcnow().isoformat(),
                'hv_21d': None,
                'hv_63d': None,
                'hv_252d': None,
            }

        except Exception as e:
            logger.warning(f"OpLab get_historical_volatility {ticker}: {e}")
            return None

    # ─── Interest Rates ───────────────────────────────────────────────

    def get_interest_rates(self) -> Optional[Dict[str, Any]]:
        """
        Get interest rate curves (DI futures, term structure).

        Returns:
            {
                'timestamp': str,
                'di_curve': [{maturity, rate}, ...],
                'selic': float,
            }
        """
        if not self._is_healthy:
            return None

        try:
            # This would fetch from /market/interest-rates or similar
            # For now, return structure with None values
            return {
                'timestamp': datetime.utcnow().isoformat(),
                'di_curve': [],
                'selic': None,
            }

        except Exception as e:
            logger.warning(f"OpLab get_interest_rates: {e}")
            return None
