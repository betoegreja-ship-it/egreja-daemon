"""
Discovery Engine — Egreja Investment AI v3.2

Procura ativos NOVOS fora da watchlist atual do Long Horizon.
Usa dados dos 3 providers (BRAPI, Polygon, OpLab) para encontrar
oportunidades que ainda não estão no universo monitorado.

Critérios de discovery:
  - Liquidez mínima
  - Cobertura mínima de dados
  - Momentum anormal
  - Valuation chamativo
  - Divergência frente a pares
  - Sinais de opções
  - Sinais macro/setoriais
  - Upgrades/deterioração relevante

Fluxo:
  providers → discovery scan → rank candidates → expanded universe
"""

import logging
import datetime
import time
from typing import List, Dict, Optional, Callable, Set

logger = logging.getLogger('egreja.long_horizon.discovery')

# ──────────────────────────────────────────────────────────────
# UNIVERSO EXPANDIDO DE BUSCA — Ativos potenciais fora do core
# ──────────────────────────────────────────────────────────────

# Brasil: ações B3 fora do universo core do Long Horizon
DISCOVERY_BR_TICKERS = [
    # Utilities / Energia
    'CPFE3', 'ENGI11', 'EQTL3', 'CMIG4', 'CPLE6', 'TRPL4', 'AURE3',
    # Bancos / Financeiras fora do core
    'SANB11', 'BPAC11', 'CIEL3', 'BBSE3', 'SULA11', 'IRBR3',
    # Varejo
    'MGLU3', 'VIIA3', 'LREN3', 'ARZZ3', 'PETZ3', 'SOMA3', 'GUAR3',
    # Saúde
    'HAPV3', 'RDOR3', 'FLRY3', 'DASA3', 'QUAL3',
    # Tecnologia BR
    'TOTS3', 'LWSA3', 'CASH3', 'MLAS3', 'POSI3',
    # Construção / Real Estate
    'CYRE3', 'EZTC3', 'MRVE3', 'EVEN3', 'TRIS3', 'DIRR3',    # Alimentos / Agro
    'MDIA3', 'SMTO3', 'SLCE3', 'CAML3', 'BEEF3',
    # Siderurgia / Mineração
    'CSNA3', 'GGBR4', 'USIM5', 'GOAU4',
    # Petroquímica
    'BRKM5', 'UNIP6',
    # Papel / Celulose
    'KLBN11', 'RANI3',
    # Telecomunicações
    'TIMS3',
    # Transporte / Logística
    'CCRO3', 'ECOR3', 'RAIL3', 'STBP3',
    # Seguradoras / Holdings
    'PSSA3', 'BRSR6',
    # ETFs BR
    'SMAL11', 'IVVB11', 'DIVO11', 'PIBB11', 'HASH11',
]

# EUA: ações fora do universo core de 40+ tickers
DISCOVERY_US_TICKERS = [
    # Growth / Tech
    'SNOW', 'PLTR', 'NET', 'DDOG', 'CRWD', 'ZS', 'MDB', 'PANW',
    # Semis
    'MRVL', 'ON', 'KLAC', 'LRCX', 'NXPI',
    # Biotech / Pharma
    'REGN', 'VRTX', 'GILD', 'ISRG', 'DXCM', 'ILMN',
    # Finance
    'SCHW', 'BLK', 'ICE', 'CME', 'SPGI',
    # Consumer
    'SBUX', 'ABNB', 'BKNG', 'LULU', 'TJX', 'ROST',
    # Industrial
    'CAT', 'DE', 'GE', 'HON', 'MMM', 'EMR',
    # Energy
    'SLB', 'EOG', 'PXD', 'DVN', 'OXY',
    # REITs
    'PLD', 'AMT', 'CCI', 'EQIX', 'SPG',
    # ETFs
    'XLF', 'XLE', 'XLK', 'ARKK', 'SOXX', 'VGT', 'SMH',
]

# Filtros mínimos para discovery
DISCOVERY_FILTERS = {
    'min_market_cap_br':  500_000_000,     # R$ 500M
    'min_market_cap_us':  2_000_000_000,   # US$ 2B
    'min_avg_volume_br':  500_000,          # 500K shares/day
    'min_avg_volume_us':  1_000_000,        # 1M shares/day
    'min_data_completeness': 0.6,           # 60% dos campos preenchidos
    'min_history_days':   252,              # ~1 year
    'exclude_penny_stocks_br': 2.0,         # < R$2
    'exclude_penny_stocks_us': 5.0,         # < US$5
}

class DiscoveryEngine:
    """
    Procura ativos novos fora da watchlist atual do Long Horizon.
    Usa providers existentes (BRAPI, Polygon, OpLab) como fontes.
    """

    def __init__(self, db_fn: Callable, log=None,
                 brapi_provider=None, polygon_provider=None,
                 oplab_provider=None):
        self.db_fn = db_fn
        self.log = log or logger
        self.brapi = brapi_provider
        self.polygon = polygon_provider
        self.oplab = oplab_provider

    # ── Main Discovery Flow ────────────────────────────────

    def run_discovery(self) -> Dict:
        """
        Full discovery scan:
          1. Get current core watchlist (to exclude)
          2. Scan BR candidates via BRAPI
          3. Scan US candidates via Polygon
          4. Score & rank discovery candidates
          5. Store in expanded universe
        """
        self.log.info('[Discovery] === Running Discovery Scan ===')
        start = time.time()

        # 1. Get core tickers to exclude
        core_tickers = self._get_core_tickers()
        self.log.info(f'[Discovery] Core watchlist: {len(core_tickers)} tickers')

        # 2. Build discovery universe (excluding core)
        br_candidates = [t for t in DISCOVERY_BR_TICKERS if t not in core_tickers]
        us_candidates = [t for t in DISCOVERY_US_TICKERS if t not in core_tickers]

        self.log.info(f'[Discovery] Discovery universe: '
                      f'{len(br_candidates)} BR + {len(us_candidates)} US')

        # 3. Scan each candidate for basic data
        all_candidates = []
        all_candidates.extend(self._scan_br_candidates(br_candidates))
        all_candidates.extend(self._scan_us_candidates(us_candidates))

        # 4. Apply minimum filters
        filtered = self._apply_discovery_filters(all_candidates)
        self.log.info(f'[Discovery] {len(all_candidates)} scanned → '
                      f'{len(filtered)} passed filters')

        # 5. Rank by discovery score
        ranked = self._rank_discovery(filtered)

        # 6. Store in universe_expanded table
        stored = self._store_expanded_universe(ranked)

        duration = round(time.time() - start, 2)
        result = {
            'status': 'completed',
            'scanned': len(all_candidates),
            'filtered': len(filtered),
            'new_candidates': stored,
            'duration_sec': duration,
            'timestamp': datetime.datetime.now().isoformat(),
        }
        self.log.info(f'[Discovery] Complete: {result}')
        return result
    # ── Core Tickers ───────────────────────────────────────

    def _get_core_tickers(self) -> Set[str]:
        """Get tickers already in lh_assets (Long Horizon core)."""
        conn = None
        try:
            conn = self.db_fn()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT ticker FROM lh_assets WHERE active = TRUE")
            rows = cursor.fetchall()
            return {r['ticker'] for r in rows} if rows else set()
        except Exception as e:
            self.log.warning(f'[Discovery] Core tickers query error: {e}')
            # Fallback: use known universe from data_ingestion
            try:
                from .data_ingestion import LongHorizonDataCollector
                collector = LongHorizonDataCollector()
                return set(collector.B3_TICKERS + collector.US_TICKERS)
            except Exception:
                return set()
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # ── BR Scanning ────────────────────────────────────────

    def _scan_br_candidates(self, tickers: List[str]) -> List[Dict]:
        """Scan BR candidates using BRAPI provider."""
        candidates = []

        if self.brapi and hasattr(self.brapi, 'get_quote'):
            for ticker in tickers:
                try:
                    quote = self.brapi.get_quote(ticker)
                    if quote:
                        candidates.append(self._build_candidate(
                            ticker=ticker,
                            market='B3',
                            data=quote,
                        ))
                except Exception as e:
                    self.log.debug(f'[Discovery] BRAPI error for {ticker}: {e}')
        else:
            # No provider available — build minimal candidates
            for ticker in tickers:
                candidates.append({
                    'ticker': ticker,
                    'market': 'B3',
                    'price': None,
                    'volume': None,
                    'market_cap': None,
                    'data_completeness': 0.1,
                    'discovery_score': 0,
                })

        return candidates
    def _scan_us_candidates(self, tickers: List[str]) -> List[Dict]:
        """Scan US candidates using Polygon provider."""
        candidates = []

        if self.polygon and hasattr(self.polygon, 'get_snapshot'):
            for ticker in tickers:
                try:
                    snapshot = self.polygon.get_snapshot(ticker)
                    if snapshot:
                        candidates.append(self._build_candidate(
                            ticker=ticker,
                            market='NYSE',
                            data=snapshot,
                        ))
                except Exception as e:
                    self.log.debug(f'[Discovery] Polygon error for {ticker}: {e}')
        else:
            for ticker in tickers:
                candidates.append({
                    'ticker': ticker,
                    'market': 'NYSE',
                    'price': None,
                    'volume': None,
                    'market_cap': None,
                    'data_completeness': 0.1,
                    'discovery_score': 0,
                })

        return candidates

    def _build_candidate(self, ticker: str, market: str, data: dict) -> Dict:
        """Build a discovery candidate from provider data."""
        price = data.get('regularMarketPrice') or data.get('close') or data.get('price')
        volume = data.get('regularMarketVolume') or data.get('volume')
        market_cap = data.get('marketCap')

        # Data completeness: how many fields are populated
        fields = [price, volume, market_cap,
                  data.get('pe'), data.get('eps'),
                  data.get('beta'), data.get('dividendYield')]
        completeness = sum(1 for f in fields if f is not None) / len(fields)

        # Basic discovery score based on momentum & valuation signals
        discovery_score = self._compute_discovery_score(data, completeness)

        return {
            'ticker': ticker,
            'market': market,
            'price': price,
            'volume': volume,
            'market_cap': market_cap,
            'pe': data.get('pe') or data.get('priceEarnings'),
            'dividend_yield': data.get('dividendYield'),
            'beta': data.get('beta'),
            'change_pct': data.get('regularMarketChangePercent') or data.get('todaysChangePerc'),
            'data_completeness': round(completeness, 2),
            'discovery_score': round(discovery_score, 2),
            'raw_data': data,
        }
    # ── Discovery Score ────────────────────────────────────

    def _compute_discovery_score(self, data: dict, completeness: float) -> float:
        """
        Compute a preliminary discovery score (0-100) based on:
          - Data completeness (20%)
          - Valuation attractiveness (30%)
          - Momentum signals (25%)
          - Liquidity (25%)
        """
        score = 0.0

        # Data completeness (0-20)
        score += completeness * 20

        # Valuation (0-30)
        pe = data.get('pe') or data.get('priceEarnings')
        if pe and pe > 0:
            if pe < 10:
                score += 30
            elif pe < 15:
                score += 25
            elif pe < 20:
                score += 15
            elif pe < 30:
                score += 8
            else:
                score += 3

        # Momentum (0-25)
        change = data.get('regularMarketChangePercent') or data.get('todaysChangePerc') or 0
        if isinstance(change, (int, float)):
            if change > 5:
                score += 20  # Strong positive momentum
            elif change > 2:
                score += 15
            elif change > 0:
                score += 10
            elif change > -2:
                score += 5

        # Liquidity (0-25)
        volume = data.get('regularMarketVolume') or data.get('volume') or 0
        if volume > 5_000_000:
            score += 25
        elif volume > 1_000_000:
            score += 20
        elif volume > 500_000:
            score += 15
        elif volume > 100_000:
            score += 10
        elif volume > 50_000:
            score += 5

        return min(100, score)
    # ── Filters ────────────────────────────────────────────

    def _apply_discovery_filters(self, candidates: List[Dict]) -> List[Dict]:
        """Apply minimum discovery filters."""
        filtered = []
        for c in candidates:
            # Skip if no price data at all
            if c.get('price') is None:
                # Keep with low score if no provider
                if c.get('data_completeness', 0) < DISCOVERY_FILTERS['min_data_completeness']:
                    continue

            # Penny stock filter
            price = c.get('price') or 0
            if c['market'] == 'B3' and 0 < price < DISCOVERY_FILTERS['exclude_penny_stocks_br']:
                continue
            if c['market'] == 'NYSE' and 0 < price < DISCOVERY_FILTERS['exclude_penny_stocks_us']:
                continue

            # Volume filter (if we have volume data)
            volume = c.get('volume') or 0
            if c['market'] == 'B3' and volume > 0 and volume < DISCOVERY_FILTERS['min_avg_volume_br']:
                continue
            if c['market'] == 'NYSE' and volume > 0 and volume < DISCOVERY_FILTERS['min_avg_volume_us']:
                continue

            filtered.append(c)

        return filtered

    # ── Ranking ────────────────────────────────────────────

    def _rank_discovery(self, candidates: List[Dict]) -> List[Dict]:
        """Rank discovery candidates by discovery_score."""
        candidates.sort(key=lambda x: x.get('discovery_score', 0), reverse=True)
        for i, c in enumerate(candidates):
            c['discovery_rank'] = i + 1
        return candidates

    # ── Storage ────────────────────────────────────────────

    def _store_expanded_universe(self, candidates: List[Dict]) -> int:
        """Store top candidates in expanded universe table."""
        conn = None
        stored = 0
        try:
            conn = self.db_fn()
            cursor = conn.cursor()

            # Create table if needed
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS lh_expanded_universe (
                id            BIGINT AUTO_INCREMENT PRIMARY KEY,
                ticker        VARCHAR(16) NOT NULL,
                market        VARCHAR(32),
                price         DECIMAL(12,4),
                volume        BIGINT,
                market_cap    DECIMAL(20,2),
                discovery_score DECIMAL(5,2),
                discovery_rank  INT,
                data_completeness DECIMAL(5,2),
                scan_date     DATE NOT NULL,
                promoted      BOOLEAN DEFAULT FALSE,
                created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uk_ticker_date (ticker, scan_date),
                INDEX idx_score (discovery_score),
                INDEX idx_scan_date (scan_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            today = datetime.date.today()
            for c in candidates[:50]:  # Store top 50
                try:
                    cursor.execute("""
                    INSERT INTO lh_expanded_universe
                        (ticker, market, price, volume, market_cap,
                         discovery_score, discovery_rank, data_completeness, scan_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        price=%s, volume=%s, discovery_score=%s,
                        discovery_rank=%s, data_completeness=%s
                    """, (
                        c['ticker'], c['market'], c.get('price'),
                        c.get('volume'), c.get('market_cap'),
                        c['discovery_score'], c.get('discovery_rank'),
                        c['data_completeness'], today,
                        c.get('price'), c.get('volume'),
                        c['discovery_score'], c.get('discovery_rank'),
                        c['data_completeness'],
                    ))
                    stored += 1
                except Exception as e:
                    self.log.debug(f'[Discovery] Store error for {c["ticker"]}: {e}')

            conn.commit()
        except Exception as e:
            self.log.warning(f'[Discovery] Storage error: {e}')
            if conn:
                conn.rollback()
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        return stored

    # ── Status ─────────────────────────────────────────────

    def get_status(self) -> Dict:
        """Get discovery engine status."""
        conn = None
        try:
            conn = self.db_fn()
            cursor = conn.cursor(dictionary=True)

            cursor.execute("""
                SELECT COUNT(*) as total,
                       MAX(scan_date) as last_scan,
                       AVG(discovery_score) as avg_score
                FROM lh_expanded_universe
            """)
            row = cursor.fetchone()

            cursor.execute("""
                SELECT ticker, market, discovery_score, discovery_rank
                FROM lh_expanded_universe
                WHERE scan_date = (SELECT MAX(scan_date) FROM lh_expanded_universe)
                ORDER BY discovery_score DESC LIMIT 10
            """)
            top10 = cursor.fetchall()

            return {
                'discovery': 'active',
                'total_in_expanded': row.get('total', 0) if row else 0,
                'last_scan': str(row.get('last_scan')) if row and row.get('last_scan') else None,
                'avg_score': float(row['avg_score']) if row and row.get('avg_score') else None,
                'top_10': top10 or [],
                'br_universe_size': len(DISCOVERY_BR_TICKERS),
                'us_universe_size': len(DISCOVERY_US_TICKERS),
            }
        except Exception as e:
            return {'discovery': 'error', 'message': str(e)}
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass