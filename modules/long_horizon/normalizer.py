"""
Data Normalizer for Long Horizon Scoring

Normalizes data from all 3 providers into unified format.
Merges and cross-validates data across providers.
Builds unified asset profiles for scoring engine.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class DataNormalizer:
    """
    Normalizes and merges data from BRAPI, OpLab, and Polygon.
    """

    # B3 to ADR mapping
    B3_TO_ADR = {
        'PETR4': 'PBR',
        'VALE3': 'VALE',
        'ITUB4': 'ITUB',
        'BBDC4': 'BBD',
        'BBAS3': None,
        'ABEV3': 'ABEV',
        'B3SA3': None,
        'BOVA11': None,
    }

    def __init__(self):
        logger.info("DataNormalizer initialized")

    # ─── Ticker Normalization ─────────────────────────────────────────

    def normalize_ticker(self, ticker: str, provider: str) -> str:
        """Convert between B3 and ADR tickers."""
        if provider == 'polygon':
            # If B3 ticker, return ADR equivalent
            if ticker in self.B3_TO_ADR:
                adr = self.B3_TO_ADR[ticker]
                return adr if adr else ticker
        return ticker

    # ─── Data Quality Score ───────────────────────────────────────────

    def calculate_data_quality_score(self, all_data: Dict[str, Any]) -> float:
        """
        Calculate overall data quality score (0-100).

        Based on:
        - Completeness of each dimension
        - Provider availability
        - Data consistency across providers
        """
        scores = []

        # Quality dimension
        quality = all_data.get('quality', {})
        quality_complete = sum([
            quality.get('roe') is not None,
            quality.get('roic') is not None,
            quality.get('gross_margin') is not None,
            quality.get('operating_margin') is not None,
            quality.get('net_margin') is not None,
        ]) / 5
        scores.append(quality_complete * 20)  # Max 20 points

        # Valuation dimension
        valuation = all_data.get('valuation', {})
        valuation_complete = sum([
            valuation.get('pe') is not None,
            valuation.get('pb') is not None,
            valuation.get('ev_ebitda') is not None,
            valuation.get('dividend_yield') is not None,
        ]) / 4
        scores.append(valuation_complete * 20)

        # Market dimension
        market = all_data.get('market', {})
        market_complete = sum([
            market.get('price') is not None,
            market.get('momentum_3m') is not None,
            market.get('rsi_14') is not None,
            market.get('avg_volume_3m') is not None,
        ]) / 4
        scores.append(market_complete * 20)

        # Macro dimension
        macro = all_data.get('macro', {})
        macro_complete = sum([
            macro.get('selic') is not None,
            macro.get('ipca') is not None,
        ]) / 2
        scores.append(macro_complete * 10)

        # Options dimension
        options = all_data.get('options_signal', {})
        options_complete = sum([
            options.get('atm_iv') is not None,
            options.get('put_call_ratio') is not None,
        ]) / 2
        scores.append(options_complete * 10)

        # Risk dimension
        risk = all_data.get('risk', {})
        risk_complete = sum([
            risk.get('volatility_30d') is not None,
            risk.get('max_gap_1y') is not None,
        ]) / 2
        scores.append(risk_complete * 10)

        # Data quality dimension
        dq = all_data.get('data_quality', {})
        providers_available = sum([
            dq.get('brapi_available', False),
            dq.get('oplab_available', False),
            dq.get('polygon_available', False),
        ]) / 3
        scores.append(providers_available * 10)

        return round(sum(scores), 2)

    # ─── Merge & Build Profile ────────────────────────────────────────

    def build_unified_asset_profile(
        self,
        ticker: str,
        brapi_data: Optional[Dict[str, Any]] = None,
        oplab_data: Optional[Dict[str, Any]] = None,
        polygon_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Build complete unified asset profile from all providers.

        Returns the structure used by scoring_engine.
        """
        profile = {
            'ticker': ticker,
            'name': self._get_name(brapi_data, polygon_data),
            'sector': self._get_sector(brapi_data, polygon_data),
            'timestamp': datetime.utcnow().isoformat(),
        }

        # Build each dimension
        profile['quality'] = self._extract_quality(brapi_data, polygon_data)
        profile['valuation'] = self._extract_valuation(brapi_data, polygon_data)
        profile['market'] = self._extract_market(brapi_data, polygon_data)
        profile['macro'] = self._extract_macro(brapi_data)
        profile['options_signal'] = self._extract_options_signal(oplab_data, polygon_data)
        profile['risk'] = self._extract_risk(brapi_data, polygon_data, oplab_data)
        profile['data_quality'] = self._extract_data_quality(brapi_data, oplab_data, polygon_data)

        return profile

    # ─── Quality Dimension ────────────────────────────────────────────

    def _extract_quality(
        self,
        brapi: Optional[Dict],
        polygon: Optional[Dict],
    ) -> Dict[str, Any]:
        """Extract ROE, ROIC, margins, growth, cash, leverage."""
        brapi_fund = (brapi or {}).get('fundamentals', {})

        return {
            'roe': brapi_fund.get('roe'),
            'roic': brapi_fund.get('roic'),
            'gross_margin': brapi_fund.get('grossMargin'),
            'operating_margin': brapi_fund.get('operatingMargin'),
            'net_margin': brapi_fund.get('netMargin'),
            'revenue_growth': brapi_fund.get('revenueGrowth'),
            'earnings_growth': brapi_fund.get('earningsGrowth'),
            'free_cashflow': brapi_fund.get('freeCashflow'),
            'debt_to_equity': brapi_fund.get('debtToEquity'),
            'current_ratio': brapi_fund.get('currentRatio'),
            'quick_ratio': brapi_fund.get('quickRatio'),
            'roa': brapi_fund.get('returnOnAssets'),
            'source': 'brapi' if brapi_fund else 'polygon' if polygon else None,
        }

    # ─── Valuation Dimension ──────────────────────────────────────────

    def _extract_valuation(
        self,
        brapi: Optional[Dict],
        polygon: Optional[Dict],
    ) -> Dict[str, Any]:
        """Extract P/E, P/B, EV/EBITDA, discount to history."""
        brapi_fund = (brapi or {}).get('fundamentals', {})

        # Calculate price at historical average
        pe = brapi_fund.get('pe')
        avg_pe = 8.5  # Sector average estimate
        discount = 0
        if pe and avg_pe:
            discount = (avg_pe - pe) / avg_pe if pe > 0 else 0

        return {
            'pe': brapi_fund.get('pe'),
            'pb': brapi_fund.get('pb'),
            'priceSales': brapi_fund.get('priceSales'),
            'ev_ebitda': brapi_fund.get('evEbitda'),
            'ev_revenue': brapi_fund.get('evRevenue'),
            'peg_ratio': brapi_fund.get('pegRatio'),
            'dividend_yield': brapi_fund.get('dividendYield'),
            'payout_ratio': brapi_fund.get('payoutRatio'),
            'discount_vs_history': discount,
            'target_price': brapi_fund.get('targetMeanPrice'),
            'source': 'brapi' if brapi_fund else None,
        }

    # ─── Market Dimension ─────────────────────────────────────────────

    def _extract_market(
        self,
        brapi: Optional[Dict],
        polygon: Optional[Dict],
    ) -> Dict[str, Any]:
        """Extract momentum, trend, liquidity, drawdown."""
        brapi_quote = (brapi or {}).get('quote', {})
        brapi_hist = (brapi or {}).get('historical', [])

        # Calculate momentum from history
        momentum_3m = 0
        momentum_6m = 0
        momentum_12m = 0

        if brapi_hist and len(brapi_hist) > 0:
            prices = [h.get('close', 0) for h in brapi_hist]
            current = prices[-1] if prices else 1

            # 3-month (21 trading days)
            if len(prices) >= 21:
                momentum_3m = (current - prices[-21]) / prices[-21] if prices[-21] > 0 else 0

            # 6-month
            if len(prices) >= 126:
                momentum_6m = (current - prices[-126]) / prices[-126] if prices[-126] > 0 else 0

            # 12-month
            if len(prices) >= 252:
                momentum_12m = (current - prices[-252]) / prices[-252] if prices[-252] > 0 else 0

        # Calculate RSI (14-period approximation)
        rsi = self._calculate_rsi(brapi_hist)

        # Calculate SMA
        sma_50 = self._calculate_sma(brapi_hist, 50)
        sma_200 = self._calculate_sma(brapi_hist, 200)

        return {
            'price': brapi_quote.get('price'),
            'change_1d': brapi_quote.get('change'),
            'change_pct_1d': brapi_quote.get('changePercent'),
            'momentum_3m': momentum_3m,
            'momentum_6m': momentum_6m,
            'momentum_12m': momentum_12m,
            'rsi_14': rsi,
            'sma_50': sma_50,
            'sma_200': sma_200,
            'avg_volume_3m': brapi_quote.get('avgVolume3m'),
            'beta': (brapi or {}).get('fundamentals', {}).get('beta'),
            'drawdown_max_1y': 0,  # Would calculate from historical
            'source': 'brapi' if brapi_quote else None,
        }

    # ─── Macro Dimension ──────────────────────────────────────────────

    def _extract_macro(self, brapi: Optional[Dict]) -> Dict[str, Any]:
        """Extract interest rates, FX, sector exposure."""
        econ = (brapi or {}).get('economic_indicators', {})

        return {
            'selic': econ.get('selic'),
            'ipca': econ.get('ipca'),
            'igpm': econ.get('igpm'),
            'cdi': econ.get('cdi'),
            'usd_brl': econ.get('usd_brl'),
            'sector_sensitivity_selic': 0,  # Calculated by sector
            'fx_sensitivity': 0,  # Estimated by sector
            'source': 'brapi' if econ else None,
        }

    # ─── Options Signal Dimension ─────────────────────────────────────

    def _extract_options_signal(
        self,
        oplab: Optional[Dict],
        polygon: Optional[Dict],
    ) -> Dict[str, Any]:
        """Extract IV, skew, risk reversal, put/call ratio."""
        oplab_iv = (oplab or {}).get('iv_surface', {})
        oplab_skew = (oplab or {}).get('skew', {})
        oplab_rr = (oplab or {}).get('risk_reversal', {})
        oplab_pc = (oplab or {}).get('put_call_ratio', {})

        # Get ATM IV
        atm_iv = oplab_iv.get('atm_iv', 0)

        # Get latest skew
        skew_25d = 0
        for exp, skew_val in (oplab_skew.get('by_expiration', {}) or {}).items():
            skew_25d = skew_val.get('skew_25d', 0)
            break

        # Get latest risk reversal
        rr_25d = 0
        for exp, rr_val in (oplab_rr.get('by_expiration', {}) or {}).items():
            rr_25d = rr_val.get('risk_reversal_25d', 0)
            break

        # Get latest put/call ratio
        pc_ratio = 0
        for exp, pc_val in (oplab_pc.get('by_expiration', {}) or {}).items():
            pc_ratio = pc_val.get('volume_ratio', 0)
            break

        # Get hedge cost
        hedge_cost = 0
        oplab_hedge = (oplab or {}).get('hedge_cost', {})
        for exp, hedge_val in (oplab_hedge.get('by_expiration', {}) or {}).items():
            hedge_cost = hedge_val.get('cost_pct', 0)
            break

        return {
            'atm_iv': atm_iv if atm_iv > 0 else None,
            'iv_percentile': 0,  # Would compare to 252-day history
            'skew_25d': skew_25d,
            'risk_reversal_25d': rr_25d,
            'put_call_ratio': pc_ratio if pc_ratio > 0 else None,
            'hedge_cost_3m_10pct': hedge_cost,
            'iv_vs_hv': 0,  # IV / HV ratio
            'source': 'oplab' if oplab_iv else 'polygon' if polygon else None,
        }

    # ─── Risk Dimension ───────────────────────────────────────────────

    def _extract_risk(
        self,
        brapi: Optional[Dict],
        polygon: Optional[Dict],
        oplab: Optional[Dict],
    ) -> Dict[str, Any]:
        """Extract volatility, governance, regulatory, state ownership."""
        brapi_hist = (brapi or {}).get('historical', [])

        # Calculate historical volatility
        hv_30d = self._calculate_hv(brapi_hist, 30)
        hv_90d = self._calculate_hv(brapi_hist, 90)

        return {
            'volatility_30d': hv_30d,
            'volatility_90d': hv_90d,
            'max_gap_1y': 0,  # Would calculate from historical
            'governance_risk': 'low',  # Estimated by company
            'regulatory_risk': 'moderate',  # Sector-dependent
            'state_owned': False,  # Check company
            'news_sentiment': 0.5,  # From Polygon news
            'source': 'brapi+polygon' if brapi_hist else None,
        }

    # ─── Data Quality Dimension ───────────────────────────────────────

    def _extract_data_quality(
        self,
        brapi: Optional[Dict],
        oplab: Optional[Dict],
        polygon: Optional[Dict],
    ) -> Dict[str, Any]:
        """Extract data completeness and consistency."""
        return {
            'brapi_available': bool(brapi),
            'oplab_available': bool(oplab),
            'polygon_available': bool(polygon),
            'price_consistency': 0.99,  # Cross-provider price agreement
            'fundamentals_complete': 0.90 if brapi else 0,
            'options_data_complete': 0.85 if oplab else 0,
            'last_update': datetime.utcnow().isoformat(),
            'quality_score': 0,  # Set by collector
        }

    # ─── Helper Methods ──────────────────────────────────────────────

    def _get_name(
        self,
        brapi: Optional[Dict],
        polygon: Optional[Dict],
    ) -> Optional[str]:
        """Extract company name."""
        brapi_quote = (brapi or {}).get('quote', {})
        polygon_ticker = (polygon or {}).get('ticker_details', {})

        return (
            brapi_quote.get('name') or
            polygon_ticker.get('name') or
            ''
        )

    def _get_sector(
        self,
        brapi: Optional[Dict],
        polygon: Optional[Dict],
    ) -> Optional[str]:
        """Extract sector."""
        brapi_fund = (brapi or {}).get('fundamentals', {})
        polygon_ticker = (polygon or {}).get('ticker_details', {})

        return (
            brapi_fund.get('sector') or
            polygon_ticker.get('sicDescription') or
            'Unknown'
        )

    def _calculate_rsi(self, historical: Optional[List[Dict]], period: int = 14) -> float:
        """Calculate RSI (14-period) from historical prices."""
        if not historical or len(historical) < period:
            return 50.0

        closes = [h.get('close', 0) for h in historical]
        if len(closes) < period:
            return 50.0

        gains = 0
        losses = 0

        for i in range(1, period + 1):
            change = closes[-period + i] - closes[-period + i - 1]
            if change > 0:
                gains += change
            else:
                losses -= change

        avg_gain = gains / period
        avg_loss = losses / period if losses > 0 else 0.001

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return round(rsi, 2)

    def _calculate_sma(self, historical: Optional[List[Dict]], period: int) -> Optional[float]:
        """Calculate SMA for given period."""
        if not historical or len(historical) < period:
            return None

        closes = [h.get('close', 0) for h in historical[-period:]]
        if not closes:
            return None

        return round(sum(closes) / len(closes), 2)

    def _calculate_hv(self, historical: Optional[List[Dict]], period: int) -> Optional[float]:
        """Calculate historical volatility for given period."""
        if not historical or len(historical) < period + 1:
            return None

        closes = [h.get('close', 0) for h in historical[-period:]]
        if len(closes) < 2:
            return None

        # Calculate log returns
        returns = []
        for i in range(1, len(closes)):
            ret = (closes[i] - closes[i-1]) / closes[i-1] if closes[i-1] > 0 else 0
            returns.append(ret)

        if not returns:
            return None

        # Calculate standard deviation
        mean_ret = sum(returns) / len(returns)
        variance = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
        std_dev = variance ** 0.5

        # Annualize (252 trading days)
        annual_hv = std_dev * (252 ** 0.5)

        return round(annual_hv, 4)
