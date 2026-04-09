"""
Monthly Picks — Review Engine.

Weekly review of all open positions.
Checks 6 exit triggers + thesis_broken + market regime.
Returns action recommendation for each position.
"""

import logging
import datetime
from typing import Dict, Optional, Callable, List

from .config import ExitReason, ReviewAction

logger = logging.getLogger('egreja.monthly_picks.review_engine')


class ReviewEngine:
    """
    Evaluates each open position against exit triggers and
    current market conditions. Produces an action recommendation.
    """

    def __init__(self, db_fn: Callable, repo, config, log=None):
        self.db_fn = db_fn
        self.repo = repo
        self.config = config
        self.log = log or logger

    def review_position(self, position: Dict) -> Dict:
        """
        Review a single open position.
        Returns dict with action, reason, exit_reason, and updated metrics.
        """
        ticker = position['ticker']
        pos_id = position['position_id']
        entry_price = float(position.get('entry_price', 0))
        entry_score = float(position.get('entry_score', 0))

        # 1. Get current price & score
        # [v10.27m] Check for override price from daily_check (bypasses all providers)
        if '_override_price' in position and position['_override_price'] > 0:
            current_price = float(position['_override_price'])
        else:
            current_price = self._get_current_price(ticker, entry_price)
        current_score = self._get_current_score(ticker, entry_score)
        prev_score = float(position.get('current_score', entry_score))

        # 2. Compute metrics
        if entry_price > 0:
            pnl_pct = round((current_price - entry_price) / entry_price * 100, 4)
        else:
            pnl_pct = 0.0

        peak_price = max(float(position.get('peak_price', entry_price)), current_price)
        max_gain = round((peak_price - entry_price) / entry_price * 100, 4) if entry_price > 0 else 0
        max_loss = min(float(position.get('max_loss_pct', 0)), pnl_pct)

        # Weeks held
        entry_date = position.get('entry_date')
        weeks_held = self._compute_weeks_held(entry_date)

        quantity = float(position.get('quantity', 0))
        pnl_value = round((current_price - entry_price) * quantity, 2)

        # 3. Update position in DB
        self.repo.update_position_price(
            position_id=pos_id,
            current_price=current_price,
            current_score=current_score,
            pnl_pct=pnl_pct,
            pnl_value=pnl_value,
            peak_price=peak_price,
            max_gain=max_gain,
            max_loss=max_loss,
            weeks_held=weeks_held,
        )

        # 4. Check exit triggers
        exit_trigger = self._check_exit_triggers(
            position, current_price, current_score,
            entry_price, entry_score, peak_price,
            pnl_pct, weeks_held,
        )

        # 5. Determine action
        if exit_trigger:
            action = ReviewAction.CLOSE.value
            reason = exit_trigger['reason']
            exit_reason = exit_trigger['exit_reason']
        else:
            # Assess confidence
            confidence = self._compute_review_confidence(
                current_score, prev_score, pnl_pct, weeks_held)

            if confidence < 40:
                action = ReviewAction.MONITOR.value
                reason = f'Low confidence ({confidence:.0f}) — closer watch'
            else:
                action = ReviewAction.HOLD.value
                reason = f'Confidence {confidence:.0f} — hold'

            exit_reason = None

        # 6. Get market regime
        market_regime = self._get_market_regime()

        # 7. Record review
        review_conf = self._compute_review_confidence(
            current_score, prev_score, pnl_pct, weeks_held)

        self.repo.insert_review(
            position_id=pos_id,
            ticker=ticker,
            current_price=current_price,
            current_score=current_score,
            prev_score=prev_score,
            pnl_pct=pnl_pct,
            weeks_held=weeks_held,
            action=action,
            reason=reason,
            market_regime=market_regime,
            confidence=review_conf,
        )

        self.repo.update_review_info(pos_id, datetime.date.today(), review_conf)

        result = {
            'position_id': pos_id,
            'ticker': ticker,
            'action': action,
            'reason': reason,
            'exit_reason': exit_reason,
            'current_price': current_price,
            'current_score': current_score,
            'pnl_pct': pnl_pct,
            'pnl_value': pnl_value,
            'weeks_held': weeks_held,
            'review_confidence': review_conf,
        }

        self.log.info(f'[MP Review] {ticker}: {action} — {reason} '
                      f'(PnL={pnl_pct:+.2f}%, score={current_score:.1f})')
        return result

    # ── EXIT TRIGGERS ──────────────────────────────────────

    def _check_exit_triggers(self, position: Dict, current_price: float,
                             current_score: float, entry_price: float,
                             entry_score: float, peak_price: float,
                             pnl_pct: float, weeks_held: int) -> Optional[Dict]:
        """
        Check all 6 exit triggers. Returns first one that fires,
        or None if position should hold.
        """
        cfg = self.config

        # 1. TARGET HIT: +15%
        if pnl_pct >= cfg.target_gain_pct:
            return {
                'exit_reason': ExitReason.TARGET_HIT.value,
                'reason': f'Target hit: {pnl_pct:+.2f}% >= {cfg.target_gain_pct}%',
            }

        # 2. STOP LOSS: -8%
        if pnl_pct <= cfg.stop_loss_pct:
            return {
                'exit_reason': ExitReason.STOP_LOSS.value,
                'reason': f'Stop loss: {pnl_pct:+.2f}% <= {cfg.stop_loss_pct}%',
            }

        # 3. TRAILING STOP: fell 5% from peak
        if peak_price > 0 and current_price > 0:
            drop_from_peak = (peak_price - current_price) / peak_price * 100
            if drop_from_peak >= cfg.trailing_stop_pct and pnl_pct > 0:
                return {
                    'exit_reason': ExitReason.TRAILING_STOP.value,
                    'reason': f'Trailing stop: fell {drop_from_peak:.2f}% from peak '
                              f'(threshold: {cfg.trailing_stop_pct}%)',
                }

        # 4. TIMEOUT: 9 months
        max_weeks = cfg.max_hold_months * 4.33
        if weeks_held >= max_weeks:
            return {
                'exit_reason': ExitReason.TIMEOUT.value,
                'reason': f'Timeout: {weeks_held} weeks held '
                          f'(max: {cfg.max_hold_months} months)',
            }

        # 5. SCORE LOW: current score < 50
        if current_score < cfg.min_score_keep:
            return {
                'exit_reason': ExitReason.SCORE_LOW.value,
                'reason': f'Score too low: {current_score:.1f} < {cfg.min_score_keep}',
            }

        # 6. SCORE DROP: score fell 15+ points from entry
        score_drop = entry_score - current_score
        if score_drop >= cfg.score_drop_threshold:
            return {
                'exit_reason': ExitReason.SCORE_DROP.value,
                'reason': f'Score dropped {score_drop:.1f} pts from entry '
                          f'(threshold: {cfg.score_drop_threshold})',
            }

        return None

    # ── HELPERS ─────────────────────────────────────────────

    def _get_current_price(self, ticker: str, fallback: float) -> float:
        """Get latest price - priority: global cache > BRAPI > Polygon > DB > fallback.
        [v10.27i] Global stock_prices cache is the most reliable real-time source.
        """
        # 0) [v10.27i] Global stock_prices cache (updated every ~60s by api_server)
        try:
            import sys as _sys
            _main = _sys.modules.get('__main__')
            if _main and hasattr(_main, 'stock_prices'):
                _sp = _main.stock_prices
                _pd = _sp.get(ticker) or _sp.get(ticker + '.SA') or {}
                if isinstance(_pd, dict):
                    _price = _pd.get('regularMarketPrice') or _pd.get('price') or _pd.get('c')
                    if _price and float(_price) > 0:
                        return float(_price)
                elif isinstance(_pd, (int, float)) and float(_pd) > 0:
                    return float(_pd)
        except Exception:
            pass
        # 1) BRAPI live quote
        try:
            from modules.long_horizon.brapi_provider import BRAPIProvider
            q = BRAPIProvider().get_quote(ticker)
            if q and float(q.get('price', 0) or 0) > 0:
                return float(q['price'])
        except Exception:
            pass
        # 2) Polygon (US)
        try:
            from modules.long_horizon.polygon_provider import PolygonProvider
            q = PolygonProvider().get_quote(ticker)
            if q and float(q.get('price', 0) or 0) > 0:
                return float(q['price'])
        except Exception:
            pass
        # 3) lh_assets.last_price snapshot
        try:
            conn = self.db_fn()
            cur = conn.cursor(dictionary=True)
            cur.execute("SELECT last_price FROM lh_assets WHERE ticker=%s", (ticker,))
            row = cur.fetchone()
            conn.close()
            if row and row.get('last_price') and float(row['last_price']) > 0:
                return float(row['last_price'])
        except Exception:
            pass
        self.log.warning(f'[MP Review] No real price for {ticker}, fallback={fallback}')
        return fallback

    def _get_current_score(self, ticker: str, fallback: float) -> float:
        """Get latest score from lh_scores."""
        conn = None
        try:
            conn = self.db_fn()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT s.total_score FROM lh_scores s
                JOIN lh_assets a ON s.asset_id = a.asset_id
                WHERE a.ticker = %s
                ORDER BY s.score_date DESC LIMIT 1
            """, (ticker,))
            row = cursor.fetchone()
            return float(row['total_score']) if row else fallback
        except Exception:
            return fallback
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _compute_weeks_held(self, entry_date) -> int:
        if not entry_date:
            return 0
        try:
            if isinstance(entry_date, str):
                ed = datetime.date.fromisoformat(entry_date)
            else:
                ed = entry_date
            delta = (datetime.date.today() - ed).days
            return max(0, delta // 7)
        except Exception:
            return 0

    def _compute_review_confidence(self, current_score: float,
                                   prev_score: float, pnl_pct: float,
                                   weeks_held: int) -> float:
        """
        Compute a confidence score (0-100) for the hold decision.
        Higher = more confident the position should stay open.
        """
        conf = 50.0  # base

        # Score level contribution
        if current_score >= 75:
            conf += 15
        elif current_score >= 60:
            conf += 5
        elif current_score < 50:
            conf -= 15

        # Score trend
        if prev_score and prev_score > 0:
            change = current_score - prev_score
            if change > 3:
                conf += 10
            elif change < -5:
                conf -= 10

        # PnL contribution
        if pnl_pct > 10:
            conf += 10
        elif pnl_pct > 5:
            conf += 5
        elif pnl_pct < -5:
            conf -= 10

        # Time decay — less confident as time goes on
        if weeks_held > 30:
            conf -= 10
        elif weeks_held > 20:
            conf -= 5

        return max(0, min(100, conf))

    def _get_market_regime(self) -> str:
        """Simple market regime detection (placeholder)."""
        # TODO: integrate with actual market regime from LH
        return 'normal'
