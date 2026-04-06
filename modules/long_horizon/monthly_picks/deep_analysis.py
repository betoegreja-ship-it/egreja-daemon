"""
Monthly Picks — Deep Analysis.

Performs in-depth analysis on the top N candidates
by consuming Long Horizon thesis engine, risk signals,
options data, and market regime.
"""

import logging
from typing import List, Dict, Optional, Callable

logger = logging.getLogger('egreja.monthly_picks.deep_analysis')


class DeepAnalyzer:
    """
    Deep analysis on top 10 candidates before final selection.

    Consumes Long Horizon:
      - thesis_engine.generate_thesis_for_ticker()
      - scoring_engine subscores
      - risk signals from lh_alerts
      - market regime data
    """

    def __init__(self, db_fn: Callable, config, log=None):
        self.db_fn = db_fn
        self.config = config
        self.log = log or logger

    def analyze_candidates(self, candidates: List[Dict]) -> List[Dict]:
        """
        Perform deep analysis on each candidate.
        Enriches candidate dict with thesis, risk flags, and adjusted score.
        """
        self.log.info(f'[MP DeepAnalysis] Analyzing {len(candidates)} candidates')
        enriched = []

        for c in candidates:
            ticker = c['ticker']
            try:
                analysis = self._analyze_one(c)
                enriched.append(analysis)
            except Exception as e:
                self.log.warning(f'[MP DeepAnalysis] Error on {ticker}: {e}')
                # Still include, just without deep analysis
                c['deep_analysis'] = {'error': str(e)}
                c['analysis_score'] = c.get('total_score', 0)
                enriched.append(c)

        # Re-sort by analysis_score
        enriched.sort(key=lambda x: x.get('analysis_score', 0), reverse=True)
        return enriched

    def _analyze_one(self, candidate: Dict) -> Dict:
        """Deep analysis for a single candidate."""
        ticker = candidate['ticker']
        scores = {
            'total_score': candidate.get('total_score', 0),
            'business_quality': candidate.get('business_quality', 0),
            'valuation': candidate.get('valuation', 0),
            'market_strength': candidate.get('market_strength', 0),
        }

        # 1. Get thesis from Long Horizon
        thesis = self._get_thesis(ticker, scores)

        # 2. Check for active risk alerts
        risk_flags = self._get_risk_flags(ticker)

        # 3. Get options signal details
        options_data = self._get_options_context(ticker)

        # 4. Compute adjusted analysis score
        base_score = float(candidate.get('total_score', 0))
        adjustment = 0.0

        # Risk penalty
        if risk_flags:
            high_risks = [r for r in risk_flags if r.get('severity') == 'high']
            adjustment -= len(high_risks) * 3.0
            medium_risks = [r for r in risk_flags if r.get('severity') == 'medium']
            adjustment -= len(medium_risks) * 1.5

        # Thesis conviction bonus
        thesis_conv = thesis.get('conviction_level', 50) if thesis else 50
        if thesis_conv > 75:
            adjustment += 2.0
        elif thesis_conv < 40:
            adjustment -= 2.0

        # Options bullish signal bonus
        if options_data.get('bullish_signal'):
            adjustment += 1.5

        analysis_score = max(0, min(100, base_score + adjustment))

        # 5. Build enriched candidate
        enriched = {**candidate}
        enriched['thesis'] = thesis
        enriched['risk_flags'] = risk_flags
        enriched['options_context'] = options_data
        enriched['analysis_score'] = round(analysis_score, 2)
        enriched['score_adjustment'] = round(adjustment, 2)
        enriched['deep_analysis'] = {
            'thesis_conviction': thesis.get('conviction_level') if thesis else None,
            'risk_count': len(risk_flags),
            'high_risk_count': len([r for r in risk_flags if r.get('severity') == 'high']),
            'options_bullish': options_data.get('bullish_signal', False),
            'recommended_horizon': thesis.get('recommended_horizon') if thesis else None,
            'hedge_suggestion': thesis.get('hedge_suggestion') if thesis else None,
        }

        return enriched

    def _get_thesis(self, ticker: str, scores: dict) -> Optional[Dict]:
        """Get thesis from Long Horizon thesis engine."""
        try:
            from modules.long_horizon.thesis_engine import generate_thesis_for_ticker
            thesis = generate_thesis_for_ticker(ticker, scores)
            return thesis
        except ImportError:
            self.log.debug(f'[MP DeepAnalysis] thesis_engine not available')
            return None
        except Exception as e:
            self.log.warning(f'[MP DeepAnalysis] Thesis error for {ticker}: {e}')
            return None

    def _get_risk_flags(self, ticker: str) -> List[Dict]:
        """Check active alerts from lh_alerts for this ticker."""
        conn = None
        try:
            conn = self.db_fn()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT a.alert_type, a.message, a.severity
                FROM lh_alerts a
                JOIN lh_assets ast ON a.asset_id = ast.asset_id
                WHERE ast.ticker = %s
                AND a.resolved = FALSE
                ORDER BY a.severity DESC
            """, (ticker,))
            return cursor.fetchall() or []
        except Exception as e:
            self.log.debug(f'[MP DeepAnalysis] Risk flags query error: {e}')
            return []
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _get_options_context(self, ticker: str) -> Dict:
        """
        Get options context from Long Horizon scores.
        The options_signal dimension captures IV, skew, risk reversal.
        """
        conn = None
        try:
            conn = self.db_fn()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT s.options_signal, s.subscores
                FROM lh_scores s
                JOIN lh_assets a ON s.asset_id = a.asset_id
                WHERE a.ticker = %s
                ORDER BY s.score_date DESC LIMIT 1
            """, (ticker,))
            row = cursor.fetchone()
            if not row:
                return {'bullish_signal': False}

            options_score = float(row.get('options_signal', 50))
            return {
                'options_score': options_score,
                'bullish_signal': options_score > 65,
                'bearish_signal': options_score < 35,
            }
        except Exception as e:
            self.log.debug(f'[MP DeepAnalysis] Options context error: {e}')
            return {'bullish_signal': False}
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
