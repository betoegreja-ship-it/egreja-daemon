"""
Monthly Picks — Scheduler Hooks.

Lightweight hooks for the existing scheduler system.
No heavy permanent threads — orchestration only.

Hooks:
  - monthly_scan_hook()  — called on 1st business day of month
  - weekly_review_hook() — called every Monday morning
  - discovery_hook()     — called weekly to refresh expanded universe
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
# LIGHTWEIGHT WORKER (optional — only if justified)
# ──────────────────────────────────────────────────────────────

def monthly_picks_worker(db_fn: Callable, log=None,
                         brain_lesson_fn: Callable = None):
    """
    Lightweight orchestration worker.
    Only checks schedule and delegates to hooks — no heavy computation.
    Runs every hour, checks if it's time to scan or review.

    This is the function registered in start_background_threads() if needed.
    """
    log = log or logger
    log.info('[MP Worker] Starting lightweight orchestration worker')

    # Startup delay
    time.sleep(120)

    while True:
        try:
            now = datetime.datetime.now()
            day = now.day
            weekday = now.strftime('%A').lower()
            hour = now.hour

            # Monthly scan: 1st-5th of month, between 9-10 AM
            if 1 <= day <= 5 and 9 <= hour <= 10:
                log.info('[MP Worker] Triggering monthly scan')
                monthly_scan_hook(db_fn, log, brain_lesson_fn)

            # Weekly review: Monday, between 9-10 AM
            if weekday == 'monday' and 9 <= hour <= 10:
                log.info('[MP Worker] Triggering weekly review')
                weekly_review_hook(db_fn, log, brain_lesson_fn)

            # Discovery: Wednesday, between 10-11 AM
            if weekday == 'wednesday' and 10 <= hour <= 11:
                log.info('[MP Worker] Triggering discovery')
                discovery_hook(db_fn, log)

        except Exception as e:
            log.error(f'[MP Worker] Error: {e}')

        time.sleep(3600)  # Check every hour
