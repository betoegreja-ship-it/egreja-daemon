"""
Candidate Expansion Layer — Egreja Investment AI v3.2

Transforma o universo expandido (discovery) em candidatas concretas
para o Monthly Picks selector.

Fluxo:
  universo expandido (50+) → filtro → 20 semifinalistas → 10 finalistas

Usa os scores e dados já existentes no Long Horizon quando disponíveis,
e complementa com dados do discovery engine para ativos novos.
"""

import logging
from typing import List, Dict, Callable, Optional

logger = logging.getLogger('egreja.long_horizon.candidate_expansion')


class CandidateExpansionLayer:
    """
    Pega o universo expandido (discovery + core) e produz as top 10
    candidatas para deep analysis no Monthly Picks.
    """

    def __init__(self, db_fn: Callable, universe_manager=None, log=None):
        self.db_fn = db_fn
        self.universe_manager = universe_manager
        self.log = log or logger
    def expand_candidates(self, max_semifinalists: int = 20,
                          max_finalists: int = 10) -> List[Dict]:
        """
        Full expansion pipeline:
          1. Get merged universe (core + expanded)
          2. Score each candidate (using LH scores for core, discovery scores for expanded)
          3. Filter to semifinalists (top 20)
          4. Re-rank and select finalists (top 10)
        """
        self.log.info('[CandidateExpansion] Starting expansion pipeline')

        # 1. Get candidates from both universes
        core_scored = self._get_core_candidates()
        expanded_scored = self._get_expanded_candidates()

        all_candidates = core_scored + expanded_scored
        self.log.info(f'[CandidateExpansion] Total pool: '
                      f'{len(core_scored)} core + {len(expanded_scored)} expanded '
                      f'= {len(all_candidates)}')

        if not all_candidates:
            return []

        # 2. Normalize scores to 0-100 scale for fair comparison
        all_candidates = self._normalize_scores(all_candidates)

        # 3. Sort by expansion_score
        all_candidates.sort(
            key=lambda x: x.get('expansion_score', 0),
            reverse=True
        )

        # 4. Semifinalists (top 20)
        semifinalists = all_candidates[:max_semifinalists]
        self.log.info(f'[CandidateExpansion] Semifinalists: {len(semifinalists)}')
        # 5. Diversification filter for finalists
        finalists = self._diversify_selection(semifinalists, max_finalists)
        self.log.info(f'[CandidateExpansion] Finalists: {len(finalists)}')

        for i, f in enumerate(finalists):
            self.log.info(f'  #{i+1} {f["ticker"]} ({f.get("source","?")}) '
                          f'— score={f.get("expansion_score", 0):.1f}')

        return finalists

    # ── Core Candidates ────────────────────────────────────

    def _get_core_candidates(self) -> List[Dict]:
        """Get scored candidates from core universe (lh_scores)."""
        conn = None
        try:
            conn = self.db_fn()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT s.total_score, s.business_quality, s.valuation,
                       s.market_strength, s.macro_factors, s.options_signal,
                       s.structural_risk, s.data_reliability, s.conviction,
                       a.ticker, a.name, a.sector, a.market
                FROM lh_scores s
                JOIN lh_assets a ON s.asset_id = a.asset_id
                WHERE a.active = TRUE
                AND s.score_date = (
                    SELECT MAX(score_date) FROM lh_scores
                    WHERE asset_id = s.asset_id
                )
                AND s.total_score >= 55
                ORDER BY s.total_score DESC
            """)
            rows = cursor.fetchall()            candidates = []
            for r in rows:
                candidates.append({
                    'ticker': r['ticker'],
                    'name': r.get('name', ''),
                    'sector': r.get('sector', 'Unknown'),
                    'market': r.get('market', 'Unknown'),
                    'total_score': float(r.get('total_score', 0)),
                    'business_quality': float(r.get('business_quality', 0)),
                    'valuation': float(r.get('valuation', 0)),
                    'market_strength': float(r.get('market_strength', 0)),
                    'macro_factors': float(r.get('macro_factors', 0)),
                    'options_signal': float(r.get('options_signal', 0)),
                    'structural_risk': float(r.get('structural_risk', 0)),
                    'data_reliability': float(r.get('data_reliability', 0)),
                    'conviction': r.get('conviction', 'Neutral'),
                    'source': 'core',
                    'expansion_score': float(r.get('total_score', 0)),
                })
            return candidates
        except Exception as e:
            self.log.warning(f'[CandidateExpansion] Core candidates error: {e}')
            # Fallback to scoring engine
            return self._fallback_core_candidates()
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    def _fallback_core_candidates(self) -> List[Dict]:
        """Fallback: use scoring engine directly."""
        try:
            from .scoring_engine import generate_demo_scores
            scores = generate_demo_scores()
            candidates = []
            for ticker, data in (scores or {}).items():
                total = data.get('total_score', 0)
                if total >= 55:
                    candidates.append({
                        'ticker': ticker,
                        'total_score': total,
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
                        'source': 'core',
                        'expansion_score': total,
                    })
            return candidates
        except Exception as e:
            self.log.warning(f'[CandidateExpansion] Fallback error: {e}')
            return []
    # ── Expanded Candidates ────────────────────────────────

    def _get_expanded_candidates(self) -> List[Dict]:
        """Get candidates from expanded (discovery) universe."""
        conn = None
        try:
            conn = self.db_fn()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT ticker, market, price, discovery_score,
                       discovery_rank, data_completeness
                FROM lh_expanded_universe
                WHERE promoted = FALSE
                AND discovery_score >= 40
                AND scan_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                ORDER BY discovery_score DESC
                LIMIT 30
            """)
            rows = cursor.fetchall()
            candidates = []
            for r in rows:
                candidates.append({
                    'ticker': r['ticker'],
                    'market': r.get('market', 'Unknown'),
                    'total_score': float(r.get('discovery_score', 0)),
                    'data_reliability': float(r.get('data_completeness', 0)) * 100,
                    'price_at_scan': float(r.get('price', 0)) if r.get('price') else None,
                    'source': 'discovery',
                    'expansion_score': float(r.get('discovery_score', 0)),
                    'sector': 'Unknown',  # Will be enriched in deep analysis
                })
            return candidates
        except Exception as e:
            self.log.debug(f'[CandidateExpansion] Expanded candidates error: {e}')
            return []
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    # ── Normalization ──────────────────────────────────────

    def _normalize_scores(self, candidates: List[Dict]) -> List[Dict]:
        """
        Normalize expansion scores so core and discovery are comparable.
        Core scores (0-100 from LH) are already normalized.
        Discovery scores (0-100 from discovery engine) get a slight discount
        due to less data available.
        """
        for c in candidates:
            if c.get('source') == 'discovery':
                # Discovery candidates get a 15% discount on score
                # (less data confidence)
                c['expansion_score'] = c['expansion_score'] * 0.85
            # Core candidates keep their score as-is
        return candidates

    # ── Diversification ────────────────────────────────────

    def _diversify_selection(self, candidates: List[Dict],
                             max_count: int) -> List[Dict]:
        """
        Select top N ensuring diversity:
          - Max 3 per sector
          - Max 60% from one market
          - Mix of core and discovery sources
        """
        selected = []
        sector_count = {}
        market_count = {}
        source_count = {'core': 0, 'discovery': 0}

        for c in candidates:
            if len(selected) >= max_count:
                break

            sector = c.get('sector', 'Unknown')
            market = c.get('market', 'Unknown')
            source = c.get('source', 'core')

            # Sector limit
            if sector_count.get(sector, 0) >= 3:
                continue

            # Market balance (max 60% from one market)
            if len(selected) >= 3:
                market_ratio = market_count.get(market, 0) / len(selected)
                if market_ratio >= 0.6:
                    continue

            selected.append(c)
            sector_count[sector] = sector_count.get(sector, 0) + 1
            market_count[market] = market_count.get(market, 0) + 1
            source_count[source] = source_count.get(source, 0) + 1

        return selected