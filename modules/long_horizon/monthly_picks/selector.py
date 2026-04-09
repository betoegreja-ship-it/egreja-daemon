"""
Monthly Picks — Selector (consumes Long Horizon core + Discovery Engine).

Ranks candidates from BOTH universes:
  - Core: 111 ativos já monitorados (via lh_scores)
  - Expanded: ativos novos descobertos pelo Discovery Engine

Fluxo:
  candidate_expansion.expand_candidates() → top 10 finalists
  → deep_analysis → portfolio_rules → final 3 picks

Não recalcula scores — consome o que já existe.
"""

import logging
import datetime
from typing import List, Dict, Optional, Callable

logger = logging.getLogger('egreja.monthly_picks.selector')


class CandidateSelector:
    """
    Selects top candidates from BOTH universes:
      - Core: lh_scores / scoring_engine (already computed)
      - Expanded: discovery_engine / lh_expanded_universe

    Uses CandidateExpansionLayer to merge and rank both sources.
    """

    def __init__(self, db_fn: Callable, config, log=None):
        self.db_fn = db_fn
        self.config = config
        self.log = log or logger

    # ── Main entry point ───────────────────────────────────

    def select_candidates(self, n: int = 10) -> List[Dict]:
        """
        Get top N candidates from BOTH universes (core + expanded).

        Pipeline:
          1. Try CandidateExpansionLayer (merges core + discovery)
          2. Fallback to core-only if expansion not available
          3. Apply minimum filters
          4. Rank by composite score
          5. Return top N
        """
        self.log.info(f'[MP Selector] Selecting top {n} candidates '
                      f'from core + expanded universe')

        # 1. Try the full expansion pipeline (core + discovery)
        raw_candidates = self._fetch_from_expansion_layer(n)

        # 2. Fallback: core-only via lh_scores
        if not raw_candidates:
            self.log.info('[MP Selector] Expansion layer not available, '
                          'falling back to core universe')
            raw_candidates = self._fetch_latest_scores()

        # 3. Fallback: scoring engine directly
        if not raw_candidates:
            self.log.warning('[MP Selector] No DB scores, '
                             'falling back to scoring_engine')
            raw_candidates = self._fetch_from_scoring_engine()

        if not raw_candidates:
            self.log.error('[MP Selector] No candidates available at all')
            return []

        # 4. Apply minimum filters
        filtered = self._apply_filters(raw_candidates)
        self.log.info(f'[MP Selector] {len(raw_candidates)} universe → '
                      f'{len(filtered)} passed filters')

        # 5. Rank by composite score (conviction × inverse risk)
        ranked = self._rank_candidates(filtered)

        # 6. Return top N
        top_n = ranked[:n]
        self.log.info(f'[MP Selector] Top {len(top_n)} candidates selected')
        for i, c in enumerate(top_n):
            source = c.get('source', 'core')
            self.log.info(f'  #{i+1} {c["ticker"]} ({source}) — '
                          f'score={c.get("total_score", 0):.1f} '
                          f'conviction={c.get("conviction","?")}')

        return top_n

    def _fetch_from_expansion_layer(self, n: int) -> List[Dict]:
        """Use CandidateExpansionLayer to get merged candidates."""
        try:
            from ..candidate_expansion import CandidateExpansionLayer
            from ..universe_manager import UniverseManager

            um = UniverseManager(self.db_fn, self.log)
            expansion = CandidateExpansionLayer(
                db_fn=self.db_fn,
                universe_manager=um,
                log=self.log,
            )
            candidates = expansion.expand_candidates(
                max_semifinalists=20,
                max_finalists=n,
            )
            if candidates:
                self.log.info(f'[MP Selector] Got {len(candidates)} from '
                              f'expansion layer')
            return candidates
        except ImportError:
            self.log.debug('[MP Selector] Expansion layer not available')
            return []
        except Exception as e:
            self.log.warning(f'[MP Selector] Expansion error: {e}')
            return []

    # ── Data sources ───────────────────────────────────────

    def _fetch_latest_scores(self) -> List[Dict]:
        """Read latest scores from lh_scores table (Long Horizon core)."""
        conn = None
        try:
            conn = self.db_fn()
            cursor = conn.cursor(dictionary=True)
            # Get latest score_date per asset, then fetch all scores for that date
            cursor.execute("""
                SELECT s.*, a.ticker, a.name, a.sector, a.market, a.asset_type
                FROM lh_scores s
                JOIN lh_assets a ON s.asset_id = a.asset_id
                WHERE a.active = TRUE
                AND s.score_date = (
                    SELECT MAX(score_date) FROM lh_scores
                    WHERE asset_id = s.asset_id
                )
                ORDER BY s.total_score DESC
            """)
            rows = cursor.fetchall()
            return [self._row_to_candidate(r) for r in rows] if rows else []
        except Exception as e:
            self.log.warning(f'[MP Selector] DB fetch error: {e}')
            return []
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _fetch_from_scoring_engine(self) -> List[Dict]:
        """
        [v10.27] Collect REAL data from Polygon/BRAPI/Oplab, score, return candidates.
        Falls back to demo scores only if all providers fail.
        """
        # Try real data first
        try:
            from modules.long_horizon.data_ingestion import LongHorizonDataCollector
            from modules.long_horizon.scoring_engine import score_from_real_data

            collector = LongHorizonDataCollector()
            self.log.info('[MP Selector] Collecting real data from providers...')

            candidates = []
            failed = 0
            # [v10.27b] Limit to top 30 B3 + top 20 US for sync speed
            universe = collector.B3_TICKERS[:30] + collector.US_TICKERS[:20]

            # Collect and score each ticker
            for ticker in universe:
                try:
                    profile = collector.collect_all(ticker)
                    if not profile:
                        failed += 1
                        continue

                    scored = score_from_real_data(profile)
                    if not scored or scored.get('total_score', 0) < 30:
                        continue

                    candidates.append({
                        'ticker': scored['ticker'],
                        'total_score': scored.get('total_score', 0),
                        'business_quality': scored.get('business_quality', 0),
                        'valuation': scored.get('valuation', 0),
                        'market_strength': scored.get('market_strength', 0),
                        'macro_factors': scored.get('macro_factors', 0),
                        'options_signal': scored.get('options_signal', 0),
                        'structural_risk': scored.get('structural_risk', 0),
                        'data_reliability': scored.get('data_reliability', 0),
                        'conviction': scored.get('conviction', 'Neutral'),
                        'sector': profile.get('sector', 'Unknown'),
                        'market': profile.get('market', 'Unknown'),
                        'name': profile.get('name', ticker),
                        'source': 'real_data',
                    })
                except Exception as e:
                    self.log.debug(f'[MP Selector] {ticker} real data error: {e}')
                    failed += 1

            self.log.info(f'[MP Selector] Real data: {len(candidates)} scored, '
                          f'{failed} failed out of {len(universe)}')

            if candidates:
                # Also persist scores to lh_scores for future use
                self._persist_scores(candidates)
                return candidates

        except Exception as e:
            self.log.warning(f'[MP Selector] Real data collection error: {e}')

        # Fallback to demo scores
        self.log.warning('[MP Selector] Falling back to demo scores')
        try:
            from modules.long_horizon.scoring_engine import (
                generate_demo_scores, rank_assets
            )
            scores = generate_demo_scores()
            if not scores:
                return []

            candidates = []
            for ticker, data in scores.items():
                candidates.append({
                    'ticker': ticker,
                    'total_score': data.get('total_score', 0),
                    'business_quality': data.get('business_quality', 0),
                    'valuation': data.get('valuation', 0),
                    'market_strength': data.get('market_strength', 0),
                    'macro_factors': data.get('macro_factors', 0),
                    'options_signal': data.get('options_signal', 0),
                    'structural_risk': data.get('structural_risk', 0),
                    'data_reliability': data.get('data_reliability', 0),
                    'conviction': data.get('conviction', 'Neutral'),
                    'sector': data.get('sector', 'Unknown'),
                    'market': data.get('market', 'Unknown'),
                    'name': data.get('name', ticker),
                    'source': 'demo',
                })
            return candidates
        except Exception as e:
            self.log.warning(f'[MP Selector] demo fallback error: {e}')
            return []

    def _persist_scores(self, candidates: List[Dict]):
        """[v10.27] Persist real scores to lh_scores table for future scans."""
        conn = None
        try:
            conn = self.db_fn()
            cursor = conn.cursor()
            today = __import__('datetime').date.today().isoformat()

            for c in candidates:
                ticker = c['ticker']
                # Get or create asset_id
                cursor.execute(
                    "SELECT asset_id FROM lh_assets WHERE ticker = %s",
                    (ticker,)
                )
                row = cursor.fetchone()
                if row:
                    asset_id = row[0]
                else:
                    cursor.execute(
                        "INSERT INTO lh_assets (ticker, name, sector, market, active) "
                        "VALUES (%s, %s, %s, %s, TRUE)",
                        (ticker, c.get('name', ticker),
                         c.get('sector', 'Unknown'),
                         c.get('market', 'Unknown'))
                    )
                    asset_id = cursor.lastrowid

                # Upsert score
                cursor.execute("""
                    INSERT INTO lh_scores
                        (asset_id, score_date, total_score, conviction,
                         business_quality, valuation, market_strength,
                         macro_factors, options_signal, structural_risk,
                         data_reliability, model_version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        total_score = VALUES(total_score),
                        conviction = VALUES(conviction),
                        business_quality = VALUES(business_quality),
                        valuation = VALUES(valuation),
                        market_strength = VALUES(market_strength),
                        macro_factors = VALUES(macro_factors),
                        options_signal = VALUES(options_signal),
                        structural_risk = VALUES(structural_risk),
                        data_reliability = VALUES(data_reliability),
                        model_version = VALUES(model_version)
                """, (
                    asset_id, today,
                    c.get('total_score', 0), c.get('conviction', 'Neutral'),
                    c.get('business_quality', 0), c.get('valuation', 0),
                    c.get('market_strength', 0), c.get('macro_factors', 0),
                    c.get('options_signal', 0), c.get('structural_risk', 0),
                    c.get('data_reliability', 0), 'v2.0-realdata',
                ))

            conn.commit()
            self.log.info(f'[MP Selector] Persisted {len(candidates)} scores to lh_scores')
        except Exception as e:
            self.log.warning(f'[MP Selector] Score persistence error: {e}')
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # ── Filters ────────────────────────────────────────────

    def _apply_filters(self, candidates: List[Dict]) -> List[Dict]:
        """Apply minimum entry criteria."""
        cfg = self.config
        filtered = []

        for c in candidates:
            score = float(c.get('total_score', 0))
            quality = float(c.get('data_reliability', 0))

            # Min score for entry
            if score < cfg.min_score_entry:
                continue

            # Min data quality
            if quality < cfg.min_data_quality:
                continue

            # Avoid if conviction is too low
            conviction = c.get('conviction', '')
            if conviction in ('Avoid', 'Caution') and cfg.avoid_open_risk_triggers:
                continue

            filtered.append(c)

        return filtered

    # ── Ranking ────────────────────────────────────────────

    def _rank_candidates(self, candidates: List[Dict]) -> List[Dict]:
        """
        Rank by composite score: conviction × (1 - structural_risk_normalized).
        This prioritizes high conviction with low risk.
        """
        cfg = self.config

        for c in candidates:
            score = float(c.get('total_score', 0))
            risk = float(c.get('structural_risk', 50))
            # Normalize risk to 0-1 (higher = riskier)
            risk_norm = min(max(risk / 100.0, 0), 1)
            # Composite: weighted blend
            composite = (cfg.conviction_weight * score +
                         cfg.risk_weight * (100 - risk))
            c['composite_score'] = round(composite, 2)

        candidates.sort(key=lambda x: x.get('composite_score', 0), reverse=True)
        return candidates

    # ── Helpers ─────────────────────────────────────────────

    def _row_to_candidate(self, row: dict) -> dict:
        """Convert a DB row to candidate dict."""
        return {
            'ticker': row.get('ticker', ''),
            'asset_id': row.get('asset_id'),
            'name': row.get('name', ''),
            'total_score': float(row.get('total_score', 0)),
            'business_quality': float(row.get('business_quality', 0)),
            'valuation': float(row.get('valuation', 0)),
            'market_strength': float(row.get('market_strength', 0)),
            'macro_factors': float(row.get('macro_factors', 0)),
            'options_signal': float(row.get('options_signal', 0)),
            'structural_risk': float(row.get('structural_risk', 0)),
            'data_reliability': float(row.get('data_reliability', 0)),
            'conviction': row.get('conviction', 'Neutral'),
            'sector': row.get('sector', 'Unknown'),
            'market': row.get('market', 'Unknown'),
            'score_date': row.get('score_date'),
        }
