"""
Monthly Picks — Lifecycle Manager.

Orchestrates the full lifecycle:
  1. Monthly scan → select → analyze → pick → open positions
  2. Weekly review → hold/reduce/close
  3. Exit trigger evaluation
  4. Performance computation per cohort
"""

import time
import logging
import datetime
from typing import Optional, Callable, List, Dict

from .config import (
    MonthlyPicksConfig, SleeveStatus, PositionStatus,
    ExitReason, ReviewAction, get_config,
)
from .repositories import MonthlyPicksRepository
from .selector import CandidateSelector
from .deep_analysis import DeepAnalyzer
from .portfolio_rules import PortfolioGovernance
from .review_engine import ReviewEngine

logger = logging.getLogger('egreja.monthly_picks.lifecycle')


class MonthlyPicksLifecycle:
    """
    Central orchestrator for the Monthly Picks sleeve.
    Coordinates selector, analyzer, governance, review, and learning.
    """

    def __init__(self, db_fn: Callable, config: MonthlyPicksConfig = None,
                 learning_bridge=None, log=None):
        self.db_fn = db_fn
        self.config = config or get_config()
        self.log = log or logger
        self.learning_bridge = learning_bridge

        # Compose sub-components
        self.repo = MonthlyPicksRepository(db_fn, self.log)
        self.selector = CandidateSelector(db_fn, self.config, self.log)
        self.analyzer = DeepAnalyzer(db_fn, self.config, self.log)
        self.governance = PortfolioGovernance(self.repo, self.config, self.log)
        self.review_engine = ReviewEngine(db_fn, self.repo, self.config, self.log)

    # ── MONTHLY SCAN ───────────────────────────────────────

    def run_monthly_scan(self, force: bool = False) -> Dict:
        """
        Full monthly scan pipeline:
          1. Check sleeve status
          2. Select top N candidates from LH universe
          3. Deep analysis
          4. Apply governance rules
          5. Open positions for selected picks
          6. Record everything
          7. Notify learning bridge
        """
        start = time.time()
        now = datetime.datetime.now()
        scan_month = now.strftime('%Y-%m')
        sleeve_status = self.repo.get_sleeve_status() or self.config.initial_status

        self.log.info(f'[MP Lifecycle] === Monthly Scan {scan_month} === '
                      f'(sleeve: {sleeve_status})')

        # Check if already ran this month
        existing = self.repo.get_scan_run(scan_month)
        if not force and existing and existing.get('picks_made', 0) > 0:
            self.log.info(f'[MP Lifecycle] Scan already done for {scan_month}')
            return {'status': 'already_done', 'scan_month': scan_month}

        # 1. Select candidates
        candidates = self.selector.select_candidates(
            n=self.config.candidates_per_scan
        )

        if not candidates:
            self.log.warning('[MP Lifecycle] No candidates found')
            return {'status': 'no_candidates', 'scan_month': scan_month}

        # 2. Create scan run record
        run_id = self.repo.insert_scan_run(
            scan_month=scan_month,
            sleeve_status=sleeve_status,
            universe_size=len(candidates),
            model_version='v3.2',
            rule_set='default',
        )

        # 3. Deep analysis
        analyzed = self.analyzer.analyze_candidates(candidates)

        # 4. Record all candidates
        candidate_ids = {}
        for i, c in enumerate(analyzed):
            cid = self.repo.insert_candidate(
                run_id=run_id,
                ticker=c['ticker'],
                rank=i + 1,
                scores=c,
                thesis_summary=(c.get('thesis', {}) or {}).get('thesis_text', '')[:500],
                deep_analysis=c.get('deep_analysis'),
                price=c.get('price_at_scan'),
                sector=c.get('sector'),
                liquidity=c.get('liquidity_score'),
                quality=c.get('data_reliability'),
            )
            candidate_ids[c['ticker']] = cid

        # 5. Apply governance rules
        selected, rejected = self.governance.apply_rules(analyzed)

        # Mark selected/rejected in DB
        for s in selected:
            cid = candidate_ids.get(s['ticker'])
            if cid:
                self.repo.mark_candidate_selected(cid)

        for r in rejected:
            cid = candidate_ids.get(r['ticker'])
            if cid:
                self.repo.mark_candidate_rejected(
                    cid, r.get('rejection_reason', 'filtered'))

        # 5b. [v10.27h] Get prices from api_server global cache
        for s in selected:
            if not s.get('price_at_scan'):
                ticker = s['ticker']
                try:
                    import sys as _sys
                    _main = _sys.modules.get('__main__')
                    if _main and hasattr(_main, 'stock_prices'):
                        _sp = _main.stock_prices
                        # Try ticker directly, then with .SA suffix for B3
                        _pd = _sp.get(ticker) or _sp.get(ticker + '.SA') or {}
                        if isinstance(_pd, dict):
                            _price = _pd.get('regularMarketPrice') or _pd.get('price') or _pd.get('c')
                            if _price:
                                s['price_at_scan'] = float(_price)
                        elif isinstance(_pd, (int, float)):
                            s['price_at_scan'] = float(_pd)
                    # Fallback: try crypto_prices
                    if not s.get('price_at_scan') and _main and hasattr(_main, 'crypto_prices'):
                        _cp = _main.crypto_prices
                        _cpd = _cp.get(ticker) or {}
                        if isinstance(_cpd, dict) and _cpd.get('price'):
                            s['price_at_scan'] = float(_cpd['price'])
                    if s.get('price_at_scan'):
                        self.log.info(f'[MP Lifecycle] Price for {ticker}: {s["price_at_scan"]}')
                    else:
                        self.log.warning(f'[MP Lifecycle] No price for {ticker} in cache')
                except Exception as _pe:
                    self.log.debug(f'[MP Lifecycle] Price fetch for {ticker}: {_pe}')

        # 6. Open positions for selected picks
        positions_opened = []
        for s in selected:
            sizing = self.governance.compute_position_sizing(s)
            if 'error' in sizing:
                self.log.warning(f'[MP Lifecycle] Sizing error for {s["ticker"]}: '
                                 f'{sizing["error"]}')
                continue

            pos_id = self.repo.insert_position(
                candidate_id=candidate_ids.get(s['ticker']),
                run_id=run_id,
                pick_month=scan_month,
                ticker=s['ticker'],
                sector=s.get('sector', 'Unknown'),
                entry_price=sizing['entry_price'],
                entry_score=float(s.get('analysis_score', s.get('total_score', 0))),
                entry_conviction=s.get('conviction', 'Neutral'),
                target_price=sizing['target_price'],
                stop_price=sizing['stop_price'],
                capital=sizing['capital_allocated'],
                quantity=sizing['quantity'],
                sleeve_status=sleeve_status,
            )

            # Record BUY action
            self.repo.insert_action(
                position_id=pos_id,
                action_type='buy',
                ticker=s['ticker'],
                quantity=sizing['quantity'],
                price=sizing['entry_price'],
                trigger_type='monthly_scan',
                trigger_detail=f'Score {s.get("analysis_score", 0):.1f}, '
                               f'Rank #{analyzed.index(s) + 1}',
                sleeve_status=sleeve_status,
            )

            positions_opened.append({
                'position_id': pos_id,
                'ticker': s['ticker'],
                'entry_price': sizing['entry_price'],
                'score': s.get('analysis_score', s.get('total_score', 0)),
            })

            # Notify learning bridge
            if self.learning_bridge:
                self.learning_bridge.emit_opened(s, sizing, sleeve_status)

        # Notify rejections to learning bridge
        if self.learning_bridge:
            for r in rejected:
                self.learning_bridge.emit_rejected(
                    r, r.get('rejection_reason', 'unknown'))

        # 7. Update scan run
        duration = round(time.time() - start, 2)
        self.repo.update_scan_run(
            run_id=run_id,
            candidates_found=len(analyzed),
            picks_made=len(positions_opened),
            duration_sec=duration,
            notes=f'Selected: {[p["ticker"] for p in positions_opened]}',
        )

        result = {
            'status': 'completed',
            'scan_month': scan_month,
            'sleeve_status': sleeve_status,
            'candidates_analyzed': len(analyzed),
            'picks_made': len(positions_opened),
            'positions': positions_opened,
            'rejected': len(rejected),
            'duration_sec': duration,
        }
        self.log.info(f'[MP Lifecycle] Scan complete: {result}')
        return result

    # ── WEEKLY REVIEW ──────────────────────────────────────

    def run_weekly_review(self) -> Dict:
        """
        Review all open positions:
          1. Update prices and scores
          2. Check exit triggers
          3. Record reviews
          4. Close positions that hit triggers
          5. Compute cohort performance
        """
        self.log.info('[MP Lifecycle] === Weekly Review ===')
        positions = self.repo.get_open_positions()

        if not positions:
            self.log.info('[MP Lifecycle] No open positions to review')
            return {'status': 'no_positions', 'reviewed': 0}

        results = []
        closed_count = 0

        for pos in positions:
            review_result = self.review_engine.review_position(pos)
            results.append(review_result)

            if review_result.get('action') == ReviewAction.CLOSE.value:
                self._close_position(pos, review_result)
                closed_count += 1
            elif review_result.get('action') == ReviewAction.REDUCE.value:
                # TODO: implement partial reduce
                self.log.info(f'[MP Lifecycle] REDUCE signal for '
                              f'{pos["ticker"]} — not yet implemented')

        # Update cohort performance
        self._update_performance()

        result = {
            'status': 'completed',
            'reviewed': len(results),
            'closed': closed_count,
            'details': results,
        }
        self.log.info(f'[MP Lifecycle] Review complete: '
                      f'{len(results)} reviewed, {closed_count} closed')
        return result

    def _close_position(self, position: Dict, review_result: Dict):
        """Close a position based on review/trigger."""
        pos_id = position['position_id']
        ticker = position['ticker']
        close_price = float(review_result.get('current_price', 0))
        entry_price = float(position.get('entry_price', 0))

        if entry_price > 0 and close_price > 0:
            pnl_pct = round((close_price - entry_price) / entry_price * 100, 4)
            quantity = float(position.get('quantity', 0))
            pnl_value = round((close_price - entry_price) * quantity, 2)
        else:
            pnl_pct = 0
            pnl_value = 0

        close_reason = review_result.get('exit_reason', 'review_close')
        thesis_broken = close_reason in (
            ExitReason.THESIS_BROKEN.value,
            ExitReason.SCORE_DROP.value,
            ExitReason.SCORE_LOW.value,
        )

        self.repo.close_position(
            position_id=pos_id,
            close_price=close_price,
            close_reason=close_reason,
            pnl_pct=pnl_pct,
            pnl_value=pnl_value,
            thesis_broken=thesis_broken,
        )

        # Record CLOSE action
        self.repo.insert_action(
            position_id=pos_id,
            action_type='close',
            ticker=ticker,
            quantity=float(position.get('quantity', 0)),
            price=close_price,
            trigger_type=close_reason,
            trigger_detail=review_result.get('reason', ''),
            sleeve_status=position.get('sleeve_status'),
            pnl_realized=pnl_value,
        )

        # Notify learning bridge
        if self.learning_bridge:
            self.learning_bridge.emit_closed(
                position, close_reason, pnl_pct, pnl_value)

        self.log.info(f'[MP Lifecycle] CLOSED {ticker} — '
                      f'reason={close_reason}, PnL={pnl_pct:+.2f}%')

    # ── FORCE CLOSE ────────────────────────────────────────

    def force_close(self, position_id: int, reason: str = 'human_override',
                    close_price: float = None) -> Dict:
        """Force-close a position (human override)."""
        pos = self.repo.get_position(position_id)
        if not pos:
            return {'error': 'position_not_found'}
        if pos['status'] == 'closed':
            return {'error': 'already_closed'}

        price = close_price or float(pos.get('current_price', 0))
        entry_price = float(pos.get('entry_price', 0))
        pnl_pct = round((price - entry_price) / entry_price * 100, 4) if entry_price > 0 else 0
        quantity = float(pos.get('quantity', 0))
        pnl_value = round((price - entry_price) * quantity, 2)

        self.repo.close_position(
            position_id=position_id,
            close_price=price,
            close_reason=reason,
            pnl_pct=pnl_pct,
            pnl_value=pnl_value,
            human_override=True,
        )

        self.repo.insert_action(
            position_id=position_id,
            action_type='close',
            ticker=pos['ticker'],
            quantity=quantity,
            price=price,
            trigger_type='human_override',
            trigger_detail=reason,
            pnl_realized=pnl_value,
        )

        if self.learning_bridge:
            self.learning_bridge.emit_closed(pos, reason, pnl_pct, pnl_value)

        return {
            'status': 'closed',
            'ticker': pos['ticker'],
            'pnl_pct': pnl_pct,
            'pnl_value': pnl_value,
        }

    # ── PERFORMANCE ────────────────────────────────────────

    def _update_performance(self):
        """Compute and store performance metrics for each cohort."""
        conn = None
        try:
            conn = self.db_fn()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT DISTINCT pick_month FROM mp_positions
                ORDER BY pick_month DESC LIMIT 12
            """)
            months = [r['pick_month'] for r in cursor.fetchall()]
        except Exception:
            return
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        for month in months:
            self._compute_cohort_performance(month)

    def _compute_cohort_performance(self, cohort_month: str):
        """Compute aggregated metrics for a monthly cohort."""
        positions = self.repo.get_positions_by_month(cohort_month)
        if not positions:
            return

        total = len(positions)
        closed = [p for p in positions if p['status'] == 'closed']
        opens = [p for p in positions if p['status'] != 'closed']

        wins = [p for p in closed if float(p.get('pnl_pct', 0)) > 0]
        losses = [p for p in closed if float(p.get('pnl_pct', 0)) <= 0]

        # Exit reason counts
        exit_counts = {}
        for p in closed:
            reason = p.get('close_reason', 'unknown')
            exit_counts[reason] = exit_counts.get(reason, 0) + 1

        # Average metrics
        returns = [float(p.get('pnl_pct', 0)) for p in closed] if closed else [0]
        entry_scores = [float(p.get('entry_score', 0)) for p in positions if p.get('entry_score')]
        exit_scores = [float(p.get('current_score', 0)) for p in closed if p.get('current_score')]

        # Hold days
        hold_days = []
        for p in closed:
            if p.get('entry_date') and p.get('close_date'):
                try:
                    ed = datetime.date.fromisoformat(str(p['entry_date']))
                    cd = datetime.date.fromisoformat(str(p['close_date']))
                    hold_days.append((cd - ed).days)
                except Exception:
                    pass

        # Sector breakdown
        sector_counts = {}
        for p in positions:
            s = p.get('sector', 'Unknown')
            sector_counts[s] = sector_counts.get(s, 0) + 1

        metrics = {
            'total_picks': total,
            'open_picks': len(opens),
            'closed_picks': len(closed),
            'win_count': len(wins),
            'loss_count': len(losses),
            'win_rate': round(len(wins) / len(closed), 4) if closed else None,
            'total_return_pct': round(sum(returns), 4),
            'avg_return_pct': round(sum(returns) / len(returns), 4) if returns else None,
            'avg_hold_days': round(sum(hold_days) / len(hold_days), 2) if hold_days else None,
            'exits_by_target': exit_counts.get('target_hit', 0),
            'exits_by_stop': exit_counts.get('stop_loss', 0),
            'exits_by_trailing': exit_counts.get('trailing_stop', 0),
            'exits_by_timeout': exit_counts.get('timeout', 0),
            'exits_by_thesis': exit_counts.get('thesis_broken', 0) + exit_counts.get('score_drop', 0),
            'exits_by_score': exit_counts.get('score_low', 0),
            'exits_by_human': exit_counts.get('human_override', 0),
            'avg_entry_score': round(sum(entry_scores) / len(entry_scores), 2) if entry_scores else None,
            'avg_exit_score': round(sum(exit_scores) / len(exit_scores), 2) if exit_scores else None,
            'max_drawdown_pct': min(returns) if returns else None,
            'sector_breakdown': sector_counts,
            'edge_stability': None,  # TODO: compute rolling edge stability
        }

        self.repo.upsert_performance(cohort_month, metrics)

    # ── DAILY POSITION CHECK ──────────────────────────────

    def run_daily_check(self) -> Dict:
        """
        [v10.27i] Lightweight daily position monitor.
        Updates prices from live cache, checks exit triggers, closes if needed.
        Does NOT do deep analysis or re-scoring (that's the weekly review).
        """
        self.log.info('[MP Lifecycle] Starting daily position check')
        start = time.time()

        # Get open positions
        open_positions = self.repo.get_open_positions()
        if not open_positions:
            return {'status': 'ok', 'positions_checked': 0, 'closed': []}

        closed = []
        updated = 0

        for pos in open_positions:
            try:
                result = self.review_engine.review_position(pos)

                if result.get('action') == 'close' and result.get('exit_reason'):
                    # Close the position
                    self.repo.close_position(
                        position_id=pos['position_id'],
                        close_price=result['current_price'],
                        close_reason=result['exit_reason'],
                    )

                    # Record SELL action
                    self.repo.insert_action(
                        position_id=pos['position_id'],
                        action_type='sell',
                        ticker=pos['ticker'],
                        quantity=float(pos.get('quantity', 0)),
                        price=result['current_price'],
                        trigger_type='daily_monitor',
                        trigger_detail=result.get('reason', ''),
                        sleeve_status=pos.get('sleeve_status', 'paper_full'),
                    )

                    closed.append({
                        'ticker': pos['ticker'],
                        'exit_reason': result['exit_reason'],
                        'pnl_pct': result.get('pnl_pct', 0),
                        'reason': result.get('reason', ''),
                    })

                    # Notify brain
                    if self.learning_bridge:
                        try:
                            self.learning_bridge.on_position_closed(
                                pos, result['current_price'],
                                result['exit_reason'],
                                result.get('pnl_pct', 0),
                            )
                        except Exception:
                            pass

                    self.log.info(f'[MP Daily] CLOSED {pos["ticker"]}: '
                                 f'{result["exit_reason"]} '
                                 f'(PnL={result.get("pnl_pct", 0):+.2f}%)')
                else:
                    updated += 1

            except Exception as e:
                self.log.warning(f'[MP Daily] Error checking {pos["ticker"]}: {e}')

        duration = time.time() - start
        self.log.info(f'[MP Daily] Check done: {len(open_positions)} checked, '
                      f'{updated} updated, {len(closed)} closed '
                      f'({duration:.1f}s)')

        return {
            'status': 'ok',
            'positions_checked': len(open_positions),
            'updated': updated,
            'closed': closed,
            'duration_sec': round(duration, 2),
        }

    # ── DASHBOARD ──────────────────────────────────────────

    def get_dashboard(self) -> Dict:
        """Return comprehensive dashboard data."""
        open_positions = self.repo.get_open_positions()
        recent_scans = self.repo.get_scan_runs(limit=6)
        performance = self.repo.get_performance(limit=6)
        sleeve_status = self.repo.get_sleeve_status()

        # Summary stats
        total_allocated = sum(float(p.get('capital_allocated', 0))
                              for p in open_positions)
        total_pnl = sum(float(p.get('pnl_value', 0))
                        for p in open_positions)

        return {
            'sleeve_status': sleeve_status,
            'open_positions': len(open_positions),
            'total_allocated': round(total_allocated, 2),
            'unrealized_pnl': round(total_pnl, 2),
            'positions': open_positions,
            'recent_scans': recent_scans,
            'performance': performance,
        }
