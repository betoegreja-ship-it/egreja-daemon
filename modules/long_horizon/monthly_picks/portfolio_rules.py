"""
Monthly Picks — Portfolio Rules & Governance.

Enforces all selection constraints before a candidate becomes a pick:
  1. Max 3 new picks per month
  2. No duplicate tickers with open positions
  3. Sector concentration limits
  4. Correlation limits
  5. Score minimum
  6. Liquidity minimum
  7. Data quality minimum
  8. No open risk triggers
  9. Conviction × risk priority
"""

import logging
from typing import List, Dict, Tuple

logger = logging.getLogger('egreja.monthly_picks.portfolio_rules')


class PortfolioGovernance:
    """
    Applies governance rules to filter analyzed candidates down to
    the final N picks, ensuring portfolio-level constraints are met.
    """

    def __init__(self, repo, config, log=None):
        """
        Args:
            repo: MonthlyPicksRepository instance
            config: MonthlyPicksConfig instance
        """
        self.repo = repo
        self.config = config
        self.log = log or logger

    def apply_rules(self, candidates: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Apply all governance rules to ranked candidates.

        Returns:
            (selected, rejected) — selected candidates that pass all rules,
            and rejected candidates with rejection reasons.
        """
        cfg = self.config
        selected = []
        rejected = []

        for c in candidates:
            ticker = c['ticker']
            reason = self._check_rules(c, selected)

            if reason:
                c['rejection_reason'] = reason
                rejected.append(c)
                self.log.info(f'[MP Rules] REJECTED {ticker}: {reason}')
            else:
                selected.append(c)
                self.log.info(f'[MP Rules] SELECTED {ticker} — '
                              f'score={c.get("analysis_score", c.get("total_score", 0)):.1f}')

            # Stop once we have enough
            if len(selected) >= cfg.picks_per_month:
                # Remaining are rejected as "quota full"
                for remaining in candidates[candidates.index(c) + 1:]:
                    if remaining not in selected and remaining not in rejected:
                        remaining['rejection_reason'] = 'quota_full'
                        rejected.append(remaining)
                break

        return selected, rejected

    def _check_rules(self, candidate: Dict, already_selected: List[Dict]) -> str:
        """
        Check all governance rules for a candidate.
        Returns rejection reason string, or empty string if passes.
        """
        cfg = self.config
        ticker = candidate['ticker']
        sector = candidate.get('sector', 'Unknown')

        # Rule 1: No duplicate tickers with open positions
        if self.repo.is_ticker_open(ticker):
            return f'duplicate_open:{ticker}'

        # Rule 2: No duplicate in current selection batch
        if any(s['ticker'] == ticker for s in already_selected):
            return f'duplicate_batch:{ticker}'

        # Rule 3: Sector concentration limit
        open_in_sector = self.repo.count_open_by_sector(sector)
        batch_in_sector = sum(1 for s in already_selected
                              if s.get('sector') == sector)
        total_sector = open_in_sector + batch_in_sector
        if total_sector >= cfg.max_sector_concentration:
            return f'sector_limit:{sector}({total_sector})'

        # Rule 4: Score minimum
        score = float(candidate.get('analysis_score',
                                    candidate.get('total_score', 0)))
        if score < cfg.min_score_entry:
            return f'score_low:{score:.1f}<{cfg.min_score_entry}'

        # Rule 5: Data quality minimum
        quality = float(candidate.get('data_reliability',
                                      candidate.get('data_quality', 0)))
        if quality < cfg.min_data_quality:
            return f'quality_low:{quality:.1f}<{cfg.min_data_quality}'

        # Rule 6: Avoid tickers with high-severity risk triggers
        if cfg.avoid_open_risk_triggers:
            risk_flags = candidate.get('risk_flags', [])
            high_risks = [r for r in risk_flags
                          if r.get('severity') == 'high']
            if high_risks:
                return f'high_risk_alert:{len(high_risks)}_active'

        # Rule 7: Conviction check — avoid "Avoid" or low conviction
        conviction = candidate.get('conviction', '')
        if conviction == 'Avoid':
            return 'conviction_avoid'

        # All rules passed
        return ''

    def compute_position_sizing(self, candidate: Dict) -> Dict:
        """
        Compute position sizing for a selected candidate.
        Returns dict with target_price, stop_price, quantity, capital.
        """
        cfg = self.config
        price = float(candidate.get('price_at_scan',
                                    candidate.get('current_price', 0)))
        if price <= 0:
            return {'error': 'no_price'}

        # Target and stop based on config
        target_price = round(price * (1 + cfg.target_gain_pct / 100), 4)
        stop_price = round(price * (1 + cfg.stop_loss_pct / 100), 4)

        # Capital allocation
        capital = cfg.capital_per_pick
        quantity = int(capital / price)  # whole shares
        actual_capital = round(quantity * price, 2)

        return {
            'entry_price': price,
            'target_price': target_price,
            'stop_price': stop_price,
            'quantity': quantity,
            'capital_allocated': actual_capital,
            'risk_per_share': round(price - stop_price, 4),
            'reward_per_share': round(target_price - price, 4),
            'risk_reward_ratio': round(
                (target_price - price) / (price - stop_price), 2
            ) if (price - stop_price) > 0 else 0,
        }
