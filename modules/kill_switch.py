"""
External Kill Switch Module for Egreja Investment AI v10.22

Self-contained kill switch system backed by MySQL for persistence across
process restarts and deployments. NO imports from api_server.py.

Provides:
- ExternalKillSwitch: Thread-safe kill switch with DB-backed state
- KillSwitchMiddleware: Pre-trade validation checks
"""

import os
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, List, Any
from enum import Enum

logger = logging.getLogger(__name__)


class KillSwitchScope(Enum):
    """Valid kill switch scopes."""
    GLOBAL = 'global'
    STOCKS = 'stocks'
    CRYPTO = 'crypto'
    ARBI = 'arbi'


class KillSwitchAction(Enum):
    """Kill switch log actions."""
    ACTIVATE = 'ACTIVATE'
    DEACTIVATE = 'DEACTIVATE'
    AUTO_DEACTIVATE = 'AUTO_DEACTIVATE'
    AUTO_ACTIVATE = 'AUTO_ACTIVATE'


class ExternalKillSwitch:
    """
    MySQL-backed kill switch that survives process restarts and deployments.

    Caches state to reduce DB load while maintaining real-time responsiveness
    to database changes. Thread-safe with per-scope locking.

    State stored in:
    - kill_switch_state: Current state of each scope
    - kill_switch_log: Immutable audit log
    """

    # Configuration from environment
    CHECK_INTERVAL_S = int(os.getenv('KILL_SWITCH_CHECK_INTERVAL_S', '30'))

    def __init__(self):
        """Initialize kill switch manager."""
        self._cache_lock = threading.RLock()
        self._scope_locks = {scope: threading.RLock() for scope in KillSwitchScope}

        # Cache: {scope: (is_active, reason, cached_at)}
        self._cache = {}
        self._last_cache_time = {}

        logger.info(
            f"ExternalKillSwitch initialized with CHECK_INTERVAL_S={self.CHECK_INTERVAL_S}s"
        )

    def init_table(self, get_db_func) -> bool:
        """
        Create kill switch tables if they don't exist.

        Args:
            get_db_func: Callable that returns a DB connection

        Returns:
            bool: True if successful, False on error
        """
        try:
            db = get_db_func()
            cursor = db.cursor()

            # Main state table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS kill_switch_state (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    key VARCHAR(32) NOT NULL UNIQUE,
                    active BOOLEAN NOT NULL DEFAULT FALSE,
                    activated_by VARCHAR(255),
                    activated_at TIMESTAMP,
                    reason TEXT,
                    auto_resume_at TIMESTAMP NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_key (key),
                    INDEX idx_active (active),
                    INDEX idx_auto_resume (auto_resume_at)
                )
            """)

            # Immutable audit log
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS kill_switch_log (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    scope VARCHAR(32) NOT NULL,
                    action VARCHAR(32) NOT NULL,
                    by VARCHAR(255),
                    reason TEXT,
                    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_scope (scope),
                    INDEX idx_action (action),
                    INDEX idx_ts (ts)
                )
            """)

            # Initialize all scopes if not present
            for scope in KillSwitchScope:
                cursor.execute("""
                    INSERT IGNORE INTO kill_switch_state (key, active, activated_by)
                    VALUES (%s, FALSE, NULL)
                """, (scope.value,))

            db.commit()
            logger.info("Kill switch tables initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize kill switch tables: {e}", exc_info=True)
            return False
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()

    def activate(
        self,
        scope: str,
        reason: str,
        activated_by: str,
        auto_resume_minutes: Optional[int] = None,
        get_db_func=None
    ) -> bool:
        """
        Activate kill switch for a scope.

        Args:
            scope: 'global' | 'stocks' | 'crypto' | 'arbi'
            reason: Human-readable reason for activation
            activated_by: Email or 'SYSTEM'
            auto_resume_minutes: Optional auto-resume after N minutes
            get_db_func: Callable that returns a DB connection

        Returns:
            bool: True if successful, False on error
        """
        if scope not in [s.value for s in KillSwitchScope]:
            logger.error(f"Invalid kill switch scope: {scope}")
            return False

        try:
            db = get_db_func()
            cursor = db.cursor()

            now = datetime.utcnow()
            auto_resume_at = None
            if auto_resume_minutes:
                auto_resume_at = now + timedelta(minutes=auto_resume_minutes)

            # Update state
            cursor.execute("""
                UPDATE kill_switch_state
                SET active = TRUE,
                    activated_by = %s,
                    activated_at = %s,
                    reason = %s,
                    auto_resume_at = %s
                WHERE key = %s
            """, (activated_by, now, reason, auto_resume_at, scope))

            # Log action
            cursor.execute("""
                INSERT INTO kill_switch_log (scope, action, by, reason)
                VALUES (%s, %s, %s, %s)
            """, (scope, KillSwitchAction.ACTIVATE.value, activated_by, reason))

            db.commit()

            # Invalidate cache
            with self._cache_lock:
                if scope in self._cache:
                    del self._cache[scope]
                if scope in self._last_cache_time:
                    del self._last_cache_time[scope]

            logger.warning(
                f"Kill switch activated for {scope} by {activated_by}: {reason}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to activate kill switch for {scope}: {e}", exc_info=True)
            return False
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()

    def deactivate(
        self,
        scope: str,
        deactivated_by: str,
        get_db_func=None
    ) -> bool:
        """
        Deactivate kill switch for a scope.

        Args:
            scope: 'global' | 'stocks' | 'crypto' | 'arbi'
            deactivated_by: Email or 'SYSTEM' of who deactivated it
            get_db_func: Callable that returns a DB connection

        Returns:
            bool: True if successful, False on error
        """
        if scope not in [s.value for s in KillSwitchScope]:
            logger.error(f"Invalid kill switch scope: {scope}")
            return False

        try:
            db = get_db_func()
            cursor = db.cursor()

            # Update state
            cursor.execute("""
                UPDATE kill_switch_state
                SET active = FALSE,
                    activated_by = NULL,
                    activated_at = NULL,
                    reason = NULL,
                    auto_resume_at = NULL
                WHERE key = %s
            """, (scope,))

            # Log action
            cursor.execute("""
                INSERT INTO kill_switch_log (scope, action, by)
                VALUES (%s, %s, %s)
            """, (scope, KillSwitchAction.DEACTIVATE.value, deactivated_by))

            db.commit()

            # Invalidate cache
            with self._cache_lock:
                if scope in self._cache:
                    del self._cache[scope]
                if scope in self._last_cache_time:
                    del self._last_cache_time[scope]

            logger.info(f"Kill switch deactivated for {scope} by {deactivated_by}")
            return True

        except Exception as e:
            logger.error(f"Failed to deactivate kill switch for {scope}: {e}", exc_info=True)
            return False
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()

    def is_active(
        self,
        scope: Optional[str] = None,
        get_db_func=None
    ) -> Tuple[bool, str]:
        """
        Check if kill switch is active for a scope.

        Caches result for CHECK_INTERVAL_S to avoid DB spam.
        Automatically deactivates if auto_resume_at has passed.

        Args:
            scope: 'global' | 'stocks' | 'crypto' | 'arbi' (default: 'global')
            get_db_func: Callable that returns a DB connection

        Returns:
            Tuple[bool, str]: (is_active, reason)
        """
        if scope is None:
            scope = KillSwitchScope.GLOBAL.value

        if scope not in [s.value for s in KillSwitchScope]:
            logger.error(f"Invalid kill switch scope: {scope}")
            return False, "Invalid scope"

        # Check cache first
        with self._cache_lock:
            now = datetime.utcnow()
            cache_key = scope

            if (cache_key in self._cache and
                cache_key in self._last_cache_time and
                (now - self._last_cache_time[cache_key]).total_seconds() < self.CHECK_INTERVAL_S):
                return self._cache[cache_key]

        try:
            db = get_db_func()
            cursor = db.cursor()

            cursor.execute("""
                SELECT active, reason, auto_resume_at
                FROM kill_switch_state
                WHERE key = %s
            """, (scope,))

            row = cursor.fetchone()

            if not row:
                logger.warning(f"Kill switch state not found for scope: {scope}")
                return False, "State not found"

            active, reason, auto_resume_at = row

            # Check auto-resume
            now = datetime.utcnow()
            if active and auto_resume_at and auto_resume_at <= now:
                logger.info(
                    f"Auto-resuming kill switch for {scope} "
                    f"(auto_resume_at={auto_resume_at})"
                )
                cursor.execute("""
                    UPDATE kill_switch_state
                    SET active = FALSE,
                        activated_by = NULL,
                        activated_at = NULL,
                        reason = NULL,
                        auto_resume_at = NULL
                    WHERE key = %s
                """, (scope,))

                cursor.execute("""
                    INSERT INTO kill_switch_log (scope, action, by)
                    VALUES (%s, %s, %s)
                """, (scope, KillSwitchAction.AUTO_DEACTIVATE.value, 'SYSTEM'))

                db.commit()

                active = False
                reason = None

            result = (active, reason or "")

            # Update cache
            with self._cache_lock:
                self._cache[scope] = result
                self._last_cache_time[scope] = now

            return result

        except Exception as e:
            logger.error(f"Failed to check kill switch for {scope}: {e}", exc_info=True)
            return False, f"Error checking kill switch: {e}"
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()

    def check_all(self, get_db_func) -> Dict[str, Dict[str, Any]]:
        """
        Check status of all kill switch scopes.

        Args:
            get_db_func: Callable that returns a DB connection

        Returns:
            dict: {scope: {active, reason, activated_by, activated_at, auto_resume_at}}
        """
        result = {}

        try:
            db = get_db_func()
            cursor = db.cursor()

            cursor.execute("""
                SELECT key, active, reason, activated_by, activated_at, auto_resume_at
                FROM kill_switch_state
                ORDER BY key
            """)

            for row in cursor.fetchall():
                key, active, reason, activated_by, activated_at, auto_resume_at = row
                result[key] = {
                    'active': active,
                    'reason': reason,
                    'activated_by': activated_by,
                    'activated_at': activated_at,
                    'auto_resume_at': auto_resume_at
                }

            return result

        except Exception as e:
            logger.error(f"Failed to check all kill switches: {e}", exc_info=True)
            return {}
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()

    def get_history(
        self,
        limit: int = 50,
        get_db_func=None
    ) -> List[Dict[str, Any]]:
        """
        Get kill switch audit log history.

        Args:
            limit: Max number of log entries to return
            get_db_func: Callable that returns a DB connection

        Returns:
            list[dict]: Log entries in reverse chronological order
        """
        result = []

        try:
            db = get_db_func()
            cursor = db.cursor()

            cursor.execute("""
                SELECT id, scope, action, by, reason, ts
                FROM kill_switch_log
                ORDER BY ts DESC
                LIMIT %s
            """, (limit,))

            for row in cursor.fetchall():
                result.append({
                    'id': row[0],
                    'scope': row[1],
                    'action': row[2],
                    'by': row[3],
                    'reason': row[4],
                    'ts': row[5]
                })

            return result

        except Exception as e:
            logger.error(f"Failed to get kill switch history: {e}", exc_info=True)
            return []
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()

    def auto_activate_on_risk_breach(
        self,
        breaches: List[str],
        get_db_func
    ) -> bool:
        """
        Automatically activate global kill switch on risk manager breach.

        Called by risk manager when limits are exceeded. Activates global
        kill switch with 60-minute auto-resume.

        Args:
            breaches: List of breach descriptions (e.g., ['Max daily loss exceeded'])
            get_db_func: Callable that returns a DB connection

        Returns:
            bool: True if successful, False on error
        """
        reason = f"RISK BREACH: {'; '.join(breaches)}"

        logger.critical(f"Risk breach detected: {reason}")

        return self.activate(
            scope=KillSwitchScope.GLOBAL.value,
            reason=reason,
            activated_by='SYSTEM',
            auto_resume_minutes=60,
            get_db_func=get_db_func
        )


class KillSwitchMiddleware:
    """
    Pre-trade validation middleware.

    Checks kill switch status before allowing trades to execute.
    """

    def __init__(self, kill_switch: ExternalKillSwitch):
        """
        Initialize middleware.

        Args:
            kill_switch: ExternalKillSwitch instance
        """
        self.kill_switch = kill_switch

    def check_before_trade(
        self,
        strategy: str,
        get_db_func
    ) -> Tuple[bool, str]:
        """
        Check kill switch before executing a trade.

        Validates:
        1. Global kill switch (blocks all trades)
        2. Strategy-specific kill switch (blocks only that strategy type)

        Args:
            strategy: Strategy type ('stocks', 'crypto', 'arbi')
            get_db_func: Callable that returns a DB connection

        Returns:
            Tuple[bool, str]: (allowed, reason)
        """
        # Check global kill switch
        is_active, reason = self.kill_switch.is_active(
            scope=KillSwitchScope.GLOBAL.value,
            get_db_func=get_db_func
        )

        if is_active:
            msg = f"Global kill switch active: {reason}"
            logger.warning(f"Trade blocked: {msg}")
            return False, msg

        # Check strategy-specific kill switch
        if strategy in [s.value for s in KillSwitchScope]:
            is_active, reason = self.kill_switch.is_active(
                scope=strategy,
                get_db_func=get_db_func
            )

            if is_active:
                msg = f"{strategy.capitalize()} kill switch active: {reason}"
                logger.warning(f"Trade blocked: {msg}")
                return False, msg

        return True, "Kill switch check passed"
