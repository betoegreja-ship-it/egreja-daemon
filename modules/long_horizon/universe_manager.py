"""
Universe Manager — Egreja Investment AI v3.2

Gerencia os dois universos do Long Horizon:
  Universo A (Core) — 111 ativos já monitorados com score e tese
  Universo B (Expanded) — ativos descobertos pelo Discovery Engine

Funcionalidades:
  - Lista universo core
  - Lista universo expandido
  - Promove ativo externo → watchlist oficial
  - Merge universos para o Monthly Picks selector
"""

import logging
import datetime
from typing import List, Dict, Set, Callable, Optional

logger = logging.getLogger('egreja.long_horizon.universe_manager')


class UniverseManager:
    """
    Gerencia universo core + expandido.
    O Monthly Picks selector consulta este manager para obter
    os candidatos de ambos os universos.
    """

    def __init__(self, db_fn: Callable, log=None):
        self.db_fn = db_fn
        self.log = log or logger

    # ── Core Universe ──────────────────────────────────────

    def get_core_tickers(self) -> List[Dict]:
        """Get all active tickers from lh_assets (core universe)."""
        conn = None
        try:
            conn = self.db_fn()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT ticker, name, sector, market, asset_type
                FROM lh_assets
                WHERE active = TRUE
                ORDER BY ticker
            """)
            return cursor.fetchall() or []
        except Exception as e:
            self.log.warning(f'[Universe] Core tickers query error: {e}')
            return []
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def get_core_ticker_set(self) -> Set[str]:
        """Get just the ticker symbols from core."""
        tickers = self.get_core_tickers()
        return {t['ticker'] for t in tickers}

    # ── Expanded Universe ──────────────────────────────────

    def get_expanded_tickers(self, min_score: float = 30.0,
                             limit: int = 50) -> List[Dict]:
        """Get discovery candidates from expanded universe."""
        conn = None
        try:
            conn = self.db_fn()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT ticker, market, price, discovery_score,
                       discovery_rank, data_completeness, scan_date
                FROM lh_expanded_universe
                WHERE discovery_score >= %s
                AND promoted = FALSE
                AND scan_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                ORDER BY discovery_score DESC
                LIMIT %s
            """, (min_score, limit))
            return cursor.fetchall() or []
        except Exception as e:
            self.log.debug(f'[Universe] Expanded query error: {e}')
            return []
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # ── Merged Universe ────────────────────────────────────

    def get_merged_universe(self, include_expanded: bool = True) -> Dict:
        """
        Get unified view of both universes.
        Returns dict with core and expanded lists.
        """
        core = self.get_core_tickers()
        expanded = self.get_expanded_tickers() if include_expanded else []

        return {
            'core': core,
            'expanded': expanded,
            'core_count': len(core),
            'expanded_count': len(expanded),
            'total': len(core) + len(expanded),
        }

    # ── Promotion ──────────────────────────────────────────

    def promote_to_core(self, ticker: str, name: str = None,
                        sector: str = None, market: str = None,
                        asset_type: str = 'stock') -> bool:
        """
        Promote a discovery ticker to the official core watchlist.
        Inserts into lh_assets and marks as promoted in expanded universe.
        """
        conn = None
        try:
            conn = self.db_fn()
            cursor = conn.cursor()

            # Check if already in core
            cursor.execute(
                "SELECT asset_id FROM lh_assets WHERE ticker = %s",
                (ticker,)
            )
            if cursor.fetchone():
                self.log.info(f'[Universe] {ticker} already in core')
                return True

            # Insert into lh_assets
            cursor.execute("""
                INSERT INTO lh_assets (ticker, name, sector, market, asset_type, active)
                VALUES (%s, %s, %s, %s, %s, TRUE)
            """, (ticker, name or ticker, sector, market, asset_type))

            # Mark as promoted in expanded universe
            cursor.execute("""
                UPDATE lh_expanded_universe SET promoted = TRUE
                WHERE ticker = %s
            """, (ticker,))

            conn.commit()
            self.log.info(f'[Universe] Promoted {ticker} to core watchlist')
            return True

        except Exception as e:
            if conn:
                conn.rollback()
            self.log.error(f'[Universe] Promotion error for {ticker}: {e}')
            return False
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # ── Stats ──────────────────────────────────────────────

    def get_universe_stats(self) -> Dict:
        """Get statistics about both universes."""
        core = self.get_core_tickers()
        expanded = self.get_expanded_tickers(min_score=0, limit=999)

        # Sector distribution in core
        core_sectors = {}
        for t in core:
            s = t.get('sector', 'Unknown')
            core_sectors[s] = core_sectors.get(s, 0) + 1

        # Market distribution
        core_markets = {}
        for t in core:
            m = t.get('market', 'Unknown')
            core_markets[m] = core_markets.get(m, 0) + 1

        expanded_markets = {}
        for t in expanded:
            m = t.get('market', 'Unknown')
            expanded_markets[m] = expanded_markets.get(m, 0) + 1

        return {
            'core_total': len(core),
            'expanded_total': len(expanded),
            'core_sectors': core_sectors,
            'core_markets': core_markets,
            'expanded_markets': expanded_markets,
            'avg_expanded_score': (
                sum(float(t.get('discovery_score', 0)) for t in expanded) / len(expanded)
                if expanded else 0
            ),
        }
