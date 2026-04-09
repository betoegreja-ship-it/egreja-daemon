"""
Monthly Picks — Scheduler Hooks.

Lightweight hooks for the existing scheduler system.
No heavy permanent threads — orchestration only.

Hooks:
  - monthly_scan_hook()      — called on 1st business day of month
  - daily_monitor_hook()     — called every day during market hours [v10.27i]
  - weekly_review_hook()     — called every Monday morning (full review)
  - discovery_hook()         — called weekly to refresh expanded universe
  - rescore_universe_hook()  — called weekly to refresh scores from real data [v10.27i]
"""

import logging
import datetime
import time
from typing import Callable, Optional

logger = logging.getLogger('egreja.monthly_picks.scheduler_hooks')


def monthly_scan_hook(db_fn: Callable, log=None,
                      brain_lesson_fn: Callable = None):
    """
    Hook for monthly scan — called by existing scheduler on 1st of month.
    Creates lifecycle, runs scan, returns result dict.
    """
    log = log or logger
    log.info('[MP Scheduler] Monthly scan hook triggered')

    try:
        from .lifecycle import MonthlyPicksLifecycle
        from .learning_bridge import LearningBridge
        from .config import get_config

        bridge = LearningBridge(
            brain_lesson_fn=brain_lesson_fn,
            log=log,
        )
        lifecycle = MonthlyPicksLifecycle(
            db_fn=db_fn,
            config=get_config(),
            learning_bridge=bridge,
            log=log,
        )
        result = lifecycle.run_monthly_scan()
        log.info(f'[MP Scheduler] Monthly scan completed: '
                 f'{result.get("picks_made", 0)} picks made')
        return result

    except Exception as e:
        log.error(f'[MP Scheduler] Monthly scan hook error: {e}')
        return {'status': 'error', 'message': str(e)}


def daily_monitor_hook(db_fn: Callable, log=None,
                       brain_lesson_fn: Callable = None):
    """
    [v10.27i] Daily position monitor — updates prices, checks exit triggers.
    Runs every 2 hours during market hours. Lightweight: uses cached prices.
    """
    log = log or logger
    log.info('[MP Scheduler] Daily monitor hook triggered')

    try:
        from .lifecycle import MonthlyPicksLifecycle
        from .learning_bridge import LearningBridge
        from .config import get_config

        bridge = LearningBridge(
            brain_lesson_fn=brain_lesson_fn,
            log=log,
        )
        lifecycle = MonthlyPicksLifecycle(
            db_fn=db_fn,
            config=get_config(),
            learning_bridge=bridge,
            log=log,
        )
        result = lifecycle.run_daily_check()
        closed_count = len(result.get('closed', []))
        log.info(f'[MP Scheduler] Daily monitor: '
                 f'{result.get("positions_checked", 0)} checked, '
                 f'{closed_count} closed')
        return result

    except Exception as e:
        log.error(f'[MP Scheduler] Daily monitor error: {e}')
        return {'status': 'error', 'message': str(e)}


def weekly_review_hook(db_fn: Callable, log=None,
                       brain_lesson_fn: Callable = None):
    """
    Hook for weekly review — called by existing scheduler every Monday.
    Creates lifecycle, runs review, returns result dict.
    """
    log = log or logger
    log.info('[MP Scheduler] Weekly review hook triggered')

    try:
        from .lifecycle import MonthlyPicksLifecycle
        from .learning_bridge import LearningBridge
        from .config import get_config

        bridge = LearningBridge(
            brain_lesson_fn=brain_lesson_fn,
            log=log,
        )
        lifecycle = MonthlyPicksLifecycle(
            db_fn=db_fn,
            config=get_config(),
            learning_bridge=bridge,
            log=log,
        )
        result = lifecycle.run_weekly_review()
        log.info(f'[MP Scheduler] Weekly review completed: '
                 f'{result.get("reviewed", 0)} reviewed, '
                 f'{result.get("closed", 0)} closed')
        return result

    except Exception as e:
        log.error(f'[MP Scheduler] Weekly review hook error: {e}')
        return {'status': 'error', 'message': str(e)}


def rescore_universe_hook(db_fn: Callable, log=None):
    """
    [v10.27i] Refresh scores from real provider data (BRAPI, Polygon, OpLab).
    Called weekly (e.g. Sunday night) to have fresh scores for the week.
    """
    log = log or logger
    log.info('[MP Scheduler] Rescore universe hook triggered')

    try:
        from modules.long_horizon.data_ingestion import LongHorizonDataCollector
        from modules.long_horizon.scoring_engine import score_from_real_data

        collector = LongHorizonDataCollector(max_workers=5, timeout=20)
        profiles = collector.collect_universe()

        scored = 0
        failed = 0
        for ticker, profile in profiles.items():
            if profile is None:
                failed += 1
                continue
            try:
                scores = score_from_real_data(profile)
                if scores and scores.get('total_score', 0) > 0:
                    _persist_score(db_fn, scores, log)
                    scored += 1
            except Exception as e:
                log.debug(f'[Rescore] {ticker}: {e}')
                failed += 1

        log.info(f'[MP Scheduler] Rescore complete: {scored} scored, {failed} failed')
        return {'status': 'ok', 'scored': scored, 'failed': failed}

    except Exception as e:
        log.error(f'[MP Scheduler] Rescore error: {e}')
        return {'status': 'error', 'message': str(e)}


def _persist_score(db_fn, scores, log):
    """Persist a single score dict to lh_scores + lh_assets."""
    conn = None
    try:
        conn = db_fn()
        cur = conn.cursor(dictionary=True)

        ticker = scores['ticker']

        # Upsert lh_assets
        cur.execute("""
            INSERT INTO lh_assets (ticker, name, asset_type, market, active)
            VALUES (%s, %s, 'stock', %s, TRUE)
            ON DUPLICATE KEY UPDATE active=TRUE
        """, (ticker, ticker, 'B3' if len(ticker) >= 5 else 'US'))

        cur.execute("SELECT asset_id FROM lh_assets WHERE ticker=%s", (ticker,))
        row = cur.fetchone()
        if not row:
            return
        asset_id = row['asset_id']

        # Upsert lh_scores
        cur.execute("""
            INSERT INTO lh_scores (
                asset_id, score_date, total_score, conviction,
                business_quality, valuation, market_strength,
                macro_factors, options_signal, structural_risk,
                data_reliability, model_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                total_score=VALUES(total_score),
                conviction=VALUES(conviction),
                business_quality=VALUES(business_quality),
                valuation=VALUES(valuation),
                market_strength=VALUES(market_strength),
                macro_factors=VALUES(macro_factors),
                options_signal=VALUES(options_signal),
                structural_risk=VALUES(structural_risk),
                data_reliability=VALUES(data_reliability),
                model_version=VALUES(model_version)
        """, (
            asset_id, scores['score_date'], scores['total_score'],
            scores['conviction'], scores['business_quality'],
            scores['valuation'], scores['market_strength'],
            scores['macro_factors'], scores['options_signal'],
            scores['structural_risk'], scores['data_reliability'],
            scores.get('model_version', 'v2.0-realdata'),
        ))
        conn.commit()
    except Exception as e:
        try:
            log.debug(f'[Rescore] persist error: {e}')
        except Exception:
            pass
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def discovery_hook(db_fn: Callable, log=None):
    """
    Hook for discovery engine — called weekly to refresh expanded universe.
    Optional: only runs if discovery_engine is available.
    """
    log = log or logger

    try:
        from ..discovery_engine import DiscoveryEngine
        engine = DiscoveryEngine(db_fn=db_fn, log=log)
        result = engine.run_discovery()
        log.info(f'[MP Scheduler] Discovery completed: '
                 f'{result.get("new_candidates", 0)} new candidates')
        return result
    except ImportError:
        log.debug('[MP Scheduler] Discovery engine not available yet')
        return {'status': 'not_available'}
    except Exception as e:
        log.warning(f'[MP Scheduler] Discovery hook error: {e}')
        return {'status': 'error', 'message': str(e)}


# ──────────────────────────────────────────────────────────────
# LIGHTWEIGHT WORKER
# ──────────────────────────────────────────────────────────────

def _last_scan_month(db_fn, log) -> str:
    """Return 'YYYY-MM' of most recent scan, or '' if none."""
    try:
        conn = db_fn()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT MAX(scan_date) AS last FROM mp_scan_runs")
        row = cur.fetchone()
        conn.close()
        if row and row.get('last'):
            return str(row['last'])[:7]
    except Exception as e:
        try:
            log.warning(f'[MP Worker] last_scan_month err: {e}')
        except Exception:
            pass
    return ''


def _last_review_date(db_fn, log) -> str:
    try:
        conn = db_fn()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT MAX(review_date) AS last FROM mp_reviews")
        row = cur.fetchone()
        conn.close()
        if row and row.get('last'):
            return str(row['last'])[:10]
    except Exception:
        pass
    return ''


def monthly_picks_worker(db_fn: Callable, log=None,
                         brain_lesson_fn: Callable = None):
    """
    [v10.27i] Enhanced orchestration worker with daily monitoring.

    Schedule:
      - Every 2 hours (market hours 9-18 BRT): daily position monitor
      - 1st-5th of month, 9-10 AM: monthly scan
      - Monday 9-10 AM: full weekly review (deep analysis)
      - Sunday 22-23: rescore universe from real provider data
      - Wednesday 10-11: discovery of new stocks
    """
    log = log or logger
    log.info('[MP Worker] Starting enhanced orchestration worker (v10.27i)')

    # Startup delay
    time.sleep(30)

    # Startup catch-up: scan if missing this month
    try:
        now = datetime.datetime.now()
        cur_month = now.strftime('%Y-%m')
        last_scan = _last_scan_month(db_fn, log)
        if last_scan != cur_month:
            log.info(f'[MP Worker] Catch-up: no scan for {cur_month} '
                     f'(last={last_scan}) — running now')
            monthly_scan_hook(db_fn, log, brain_lesson_fn)
    except Exception as e:
        log.error(f'[MP Worker] Startup catch-up error: {e}')

    last_daily_check = ''  # track 'YYYY-MM-DD HH' to avoid re-running

    while True:
        try:
            now = datetime.datetime.now()
            day = now.day
            weekday = now.strftime('%A').lower()
            hour = now.hour
            now_key = now.strftime('%Y-%m-%d %H')

            # ── Daily position monitor: every 2h during market hours ──
            if 9 <= hour <= 18 and now_key != last_daily_check:
                log.info('[MP Worker] Running daily position monitor')
                daily_monitor_hook(db_fn, log, brain_lesson_fn)
                last_daily_check = now_key

            # ── Monthly scan: 1st-5th, 9-10 AM ──
            if 1 <= day <= 5 and 9 <= hour <= 10:
                cur_month = now.strftime('%Y-%m')
                last_scan = _last_scan_month(db_fn, log)
                if last_scan != cur_month:
                    log.info('[MP Worker] Triggering monthly scan')
                    monthly_scan_hook(db_fn, log, brain_lesson_fn)

            # ── Weekly review: Monday, 9-10 AM ──
            if weekday == 'monday' and 9 <= hour <= 10:
                last_rev = _last_review_date(db_fn, log)
                today_str = now.strftime('%Y-%m-%d')
                if last_rev != today_str:
                    log.info('[MP Worker] Triggering weekly review')
                    weekly_review_hook(db_fn, log, brain_lesson_fn)

            # ── Rescore universe: Sunday 22-23 ──
            if weekday == 'sunday' and 22 <= hour <= 23:
                log.info('[MP Worker] Triggering universe rescore')
                rescore_universe_hook(db_fn, log)

            # ── Discovery: Wednesday, 10-11 AM ──
            if weekday == 'wednesday' and 10 <= hour <= 11:
                log.info('[MP Worker] Triggering discovery')
                discovery_hook(db_fn, log)

        except Exception as e:
            log.error(f'[MP Worker] Error: {e}')

        time.sleep(3600)  # Check every hour
