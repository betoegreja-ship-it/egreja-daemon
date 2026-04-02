"""
Operational Metrics Collector for Egreja Investment AI v10.23

Tracks system health metrics over time for soak testing and operational monitoring:
- Memory RSS, CPU usage
- Worker cycle timing
- Endpoint latency
- MySQL query timing
- Reconciliation drift history
- Circuit breaker events
- Restart counter
- Progressive alert system

Self-contained module with NO imports from api_server.py.
Thread-safe implementation using threading.Lock.

Author: Egreja Investment AI
Version: 10.23
"""

import os
import time
import logging
import threading
import resource
from datetime import datetime, timedelta
from collections import deque, defaultdict
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Progressive alert levels."""
    OK = 'OK'
    WARNING = 'WARNING'
    CRITICAL = 'CRITICAL'
    FREEZE = 'FREEZE'


class OpsMetricsCollector:
    """
    Operational metrics collector for soak testing and monitoring.

    Collects time-series data on system health indicators including
    memory, latency, drift, and circuit breakers. Provides progressive
    alerting when metrics exceed thresholds.
    """

    # Default thresholds (configurable via env vars)
    MEM_WARNING_MB = float(os.getenv('OPS_MEM_WARNING_MB', '512'))
    MEM_CRITICAL_MB = float(os.getenv('OPS_MEM_CRITICAL_MB', '1024'))
    DRIFT_WARNING = float(os.getenv('OPS_DRIFT_WARNING', '100'))
    DRIFT_CRITICAL = float(os.getenv('OPS_DRIFT_CRITICAL', '500'))
    DRIFT_FREEZE = float(os.getenv('OPS_DRIFT_FREEZE', '1000'))
    LATENCY_WARNING_MS = float(os.getenv('OPS_LATENCY_WARNING_MS', '2000'))
    LATENCY_CRITICAL_MS = float(os.getenv('OPS_LATENCY_CRITICAL_MS', '5000'))
    MAX_HISTORY = int(os.getenv('OPS_MAX_HISTORY', '2880'))  # 2 days at 1-min intervals

    def __init__(self):
        """Initialize metrics collector with empty buffers."""
        self._lock = threading.RLock()
        self._boot_time = time.time()
        self._restart_count = 0

        # Time-series buffers (deques with max length)
        self._memory_history: deque = deque(maxlen=self.MAX_HISTORY)
        self._worker_timing: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.MAX_HISTORY))
        self._endpoint_latency: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.MAX_HISTORY))
        self._db_query_timing: deque = deque(maxlen=self.MAX_HISTORY)
        self._drift_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.MAX_HISTORY))
        self._circuit_breaker_events: deque = deque(maxlen=1000)
        self._alerts: deque = deque(maxlen=500)

        # Current state
        self._active_alerts: Dict[str, AlertLevel] = {}

        logger.info("OpsMetricsCollector initialized")

    # ── Memory Tracking ──────────────────────────────────────────────────

    def record_memory(self) -> Dict[str, float]:
        """
        Record current memory usage.

        Returns:
            Dict with rss_mb, vms_mb, timestamp
        """
        with self._lock:
            try:
                usage = resource.getrusage(resource.RUSAGE_SELF)
                rss_mb = usage.ru_maxrss / 1024  # macOS: bytes, Linux: KB
                # On Linux, ru_maxrss is in KB
                if rss_mb > 100000:
                    rss_mb = usage.ru_maxrss / (1024 * 1024)
            except Exception:
                rss_mb = 0.0

            snapshot = {
                'rss_mb': round(rss_mb, 2),
                'ts': time.time(),
                'ts_iso': datetime.utcnow().isoformat()
            }
            self._memory_history.append(snapshot)

            # Check alert
            if rss_mb > self.MEM_CRITICAL_MB:
                self._raise_alert('memory', AlertLevel.CRITICAL,
                                  f'Memory RSS {rss_mb:.0f}MB exceeds {self.MEM_CRITICAL_MB}MB')
            elif rss_mb > self.MEM_WARNING_MB:
                self._raise_alert('memory', AlertLevel.WARNING,
                                  f'Memory RSS {rss_mb:.0f}MB exceeds {self.MEM_WARNING_MB}MB')
            else:
                self._clear_alert('memory')

            return snapshot

    def get_memory_trend(self, last_n: int = 60) -> Dict[str, Any]:
        """
        Get memory trend over last N samples.

        Returns:
            Dict with current, min, max, avg, growth_rate_mb_per_hour
        """
        with self._lock:
            if not self._memory_history:
                return {'current': 0, 'min': 0, 'max': 0, 'avg': 0,
                        'growth_rate_mb_per_hour': 0, 'samples': 0}

            recent = list(self._memory_history)[-last_n:]
            rss_values = [s['rss_mb'] for s in recent]

            # Calculate growth rate
            growth_rate = 0.0
            if len(recent) >= 2:
                time_span_hours = (recent[-1]['ts'] - recent[0]['ts']) / 3600
                if time_span_hours > 0:
                    growth_rate = (rss_values[-1] - rss_values[0]) / time_span_hours

            return {
                'current': rss_values[-1] if rss_values else 0,
                'min': min(rss_values) if rss_values else 0,
                'max': max(rss_values) if rss_values else 0,
                'avg': round(sum(rss_values) / len(rss_values), 2) if rss_values else 0,
                'growth_rate_mb_per_hour': round(growth_rate, 4),
                'samples': len(recent),
                'leak_suspected': growth_rate > 5.0  # >5 MB/hour
            }

    # ── Worker Timing ────────────────────────────────────────────────────

    def record_worker_cycle(self, worker_name: str, duration_s: float,
                            trades_executed: int = 0) -> None:
        """
        Record a worker cycle completion.

        Args:
            worker_name: e.g. 'stock_execution_worker', 'auto_trade_crypto', 'arbi_scan_loop'
            duration_s: Cycle duration in seconds
            trades_executed: Number of trades executed in this cycle
        """
        with self._lock:
            self._worker_timing[worker_name].append({
                'duration_s': round(duration_s, 3),
                'trades': trades_executed,
                'ts': time.time()
            })

    def get_worker_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get timing statistics for all workers."""
        with self._lock:
            result = {}
            for name, timings in self._worker_timing.items():
                if not timings:
                    continue
                durations = [t['duration_s'] for t in timings]
                trades = [t['trades'] for t in timings]
                last_ts = timings[-1]['ts']
                result[name] = {
                    'cycles': len(durations),
                    'avg_duration_s': round(sum(durations) / len(durations), 3),
                    'max_duration_s': round(max(durations), 3),
                    'min_duration_s': round(min(durations), 3),
                    'total_trades': sum(trades),
                    'last_cycle_ago_s': round(time.time() - last_ts, 1),
                    'stale': (time.time() - last_ts) > 300  # >5 min since last cycle
                }
            return result

    # ── Endpoint Latency ─────────────────────────────────────────────────

    def record_endpoint_latency(self, endpoint: str, latency_ms: float,
                                 status_code: int = 200) -> None:
        """Record an API endpoint response time."""
        with self._lock:
            self._endpoint_latency[endpoint].append({
                'latency_ms': round(latency_ms, 2),
                'status': status_code,
                'ts': time.time()
            })

            if latency_ms > self.LATENCY_CRITICAL_MS:
                self._raise_alert(f'latency_{endpoint}', AlertLevel.CRITICAL,
                                  f'{endpoint} latency {latency_ms:.0f}ms')
            elif latency_ms > self.LATENCY_WARNING_MS:
                self._raise_alert(f'latency_{endpoint}', AlertLevel.WARNING,
                                  f'{endpoint} latency {latency_ms:.0f}ms')

    def get_endpoint_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get latency statistics for all endpoints."""
        with self._lock:
            result = {}
            for endpoint, records in self._endpoint_latency.items():
                if not records:
                    continue
                latencies = [r['latency_ms'] for r in records]
                errors = sum(1 for r in records if r['status'] >= 400)
                result[endpoint] = {
                    'requests': len(latencies),
                    'avg_ms': round(sum(latencies) / len(latencies), 2),
                    'p95_ms': round(sorted(latencies)[int(len(latencies) * 0.95)], 2) if len(latencies) >= 20 else round(max(latencies), 2),
                    'max_ms': round(max(latencies), 2),
                    'error_count': errors,
                    'error_rate': round(errors / len(latencies), 4) if latencies else 0
                }
            return result

    # ── DB Query Timing ──────────────────────────────────────────────────

    def record_db_query(self, query_type: str, duration_ms: float,
                        success: bool = True) -> None:
        """Record a database query timing."""
        with self._lock:
            self._db_query_timing.append({
                'type': query_type,
                'duration_ms': round(duration_ms, 2),
                'success': success,
                'ts': time.time()
            })

    def get_db_stats(self) -> Dict[str, Any]:
        """Get database query statistics."""
        with self._lock:
            if not self._db_query_timing:
                return {'total_queries': 0}

            recent = list(self._db_query_timing)
            durations = [q['duration_ms'] for q in recent]
            failures = sum(1 for q in recent if not q['success'])

            by_type = defaultdict(list)
            for q in recent:
                by_type[q['type']].append(q['duration_ms'])

            return {
                'total_queries': len(recent),
                'avg_ms': round(sum(durations) / len(durations), 2),
                'max_ms': round(max(durations), 2),
                'failure_count': failures,
                'failure_rate': round(failures / len(recent), 4),
                'by_type': {
                    t: {'count': len(ds), 'avg_ms': round(sum(ds) / len(ds), 2)}
                    for t, ds in by_type.items()
                }
            }

    # ── Reconciliation Drift History ─────────────────────────────────────

    def record_drift(self, strategy: str, formula_capital: float,
                     ledger_capital: float, db_capital: Optional[float] = None) -> None:
        """
        Record a reconciliation drift measurement.

        Args:
            strategy: 'stocks', 'crypto', 'arbi'
            formula_capital: Capital calculated by formula
            ledger_capital: Capital from ledger replay
            db_capital: Capital from MySQL (if available)
        """
        with self._lock:
            drift_formula_ledger = abs(formula_capital - ledger_capital)
            drift_formula_db = abs(formula_capital - db_capital) if db_capital is not None else None

            snapshot = {
                'formula': round(formula_capital, 2),
                'ledger': round(ledger_capital, 2),
                'db': round(db_capital, 2) if db_capital is not None else None,
                'drift_formula_ledger': round(drift_formula_ledger, 2),
                'drift_formula_db': round(drift_formula_db, 2) if drift_formula_db is not None else None,
                'ts': time.time(),
                'ts_iso': datetime.utcnow().isoformat()
            }
            self._drift_history[strategy].append(snapshot)

            # Progressive alerts
            max_drift = drift_formula_ledger
            if drift_formula_db is not None:
                max_drift = max(max_drift, drift_formula_db)

            alert_key = f'drift_{strategy}'
            if max_drift > self.DRIFT_FREEZE:
                self._raise_alert(alert_key, AlertLevel.FREEZE,
                                  f'{strategy} drift ${max_drift:,.2f} exceeds FREEZE threshold')
            elif max_drift > self.DRIFT_CRITICAL:
                self._raise_alert(alert_key, AlertLevel.CRITICAL,
                                  f'{strategy} drift ${max_drift:,.2f} exceeds CRITICAL threshold')
            elif max_drift > self.DRIFT_WARNING:
                self._raise_alert(alert_key, AlertLevel.WARNING,
                                  f'{strategy} drift ${max_drift:,.2f} exceeds WARNING threshold')
            else:
                self._clear_alert(alert_key)

    def get_drift_report(self) -> Dict[str, Dict[str, Any]]:
        """
        Get comprehensive drift report by strategy.

        Returns:
            Dict with current drift, trend, max historical, alert status
        """
        with self._lock:
            result = {}
            for strategy, history in self._drift_history.items():
                if not history:
                    continue

                drifts = [h['drift_formula_ledger'] for h in history]
                latest = history[-1]

                # Trend: compare last 10 vs previous 10
                trend = 'stable'
                if len(drifts) >= 20:
                    recent_avg = sum(drifts[-10:]) / 10
                    previous_avg = sum(drifts[-20:-10]) / 10
                    if recent_avg > previous_avg * 1.5:
                        trend = 'worsening'
                    elif recent_avg < previous_avg * 0.5:
                        trend = 'improving'

                alert_key = f'drift_{strategy}'
                result[strategy] = {
                    'current_drift': latest['drift_formula_ledger'],
                    'current_formula': latest['formula'],
                    'current_ledger': latest['ledger'],
                    'current_db': latest.get('db'),
                    'max_drift': max(drifts),
                    'avg_drift': round(sum(drifts) / len(drifts), 2),
                    'samples': len(drifts),
                    'trend': trend,
                    'alert_level': self._active_alerts.get(alert_key, AlertLevel.OK).value,
                    'last_check': latest['ts_iso']
                }
            return result

    # ── Circuit Breaker Events ───────────────────────────────────────────

    def record_circuit_breaker_event(self, source: str, old_state: str,
                                      new_state: str, reason: str = '') -> None:
        """Record a circuit breaker state transition."""
        with self._lock:
            self._circuit_breaker_events.append({
                'source': source,
                'from': old_state,
                'to': new_state,
                'reason': reason,
                'ts': time.time(),
                'ts_iso': datetime.utcnow().isoformat()
            })

            if new_state == 'OPEN':
                self._raise_alert(f'cb_{source}', AlertLevel.CRITICAL,
                                  f'Circuit breaker OPEN for {source}: {reason}')
            elif new_state == 'CLOSED':
                self._clear_alert(f'cb_{source}')

    def get_circuit_breaker_history(self, last_n: int = 50) -> List[Dict[str, Any]]:
        """Get recent circuit breaker events."""
        with self._lock:
            return list(self._circuit_breaker_events)[-last_n:]

    # ── Restart Tracking ─────────────────────────────────────────────────

    def record_restart(self) -> None:
        """Record a system restart."""
        with self._lock:
            self._restart_count += 1
            self._boot_time = time.time()
            logger.warning(f"System restart recorded. Total restarts: {self._restart_count}")

    # ── Alert System ─────────────────────────────────────────────────────

    def _raise_alert(self, key: str, level: AlertLevel, message: str) -> None:
        """Raise or escalate an alert (must be called with lock held)."""
        current = self._active_alerts.get(key)
        if current != level:
            self._active_alerts[key] = level
            self._alerts.append({
                'key': key,
                'level': level.value,
                'message': message,
                'ts': time.time(),
                'ts_iso': datetime.utcnow().isoformat()
            })
            if level in (AlertLevel.CRITICAL, AlertLevel.FREEZE):
                logger.critical(f"ALERT [{level.value}] {key}: {message}")
            elif level == AlertLevel.WARNING:
                logger.warning(f"ALERT [{level.value}] {key}: {message}")

    def _clear_alert(self, key: str) -> None:
        """Clear an alert (must be called with lock held)."""
        if key in self._active_alerts:
            old = self._active_alerts.pop(key)
            self._alerts.append({
                'key': key,
                'level': 'CLEARED',
                'message': f'Cleared from {old.value}',
                'ts': time.time(),
                'ts_iso': datetime.utcnow().isoformat()
            })

    def get_active_alerts(self) -> Dict[str, str]:
        """Get all active alerts."""
        with self._lock:
            return {k: v.value for k, v in self._active_alerts.items()}

    def get_alert_history(self, last_n: int = 50) -> List[Dict[str, Any]]:
        """Get recent alert history."""
        with self._lock:
            return list(self._alerts)[-last_n:]

    # ── Daily Audit Report ───────────────────────────────────────────────

    def generate_daily_audit(self) -> Dict[str, Any]:
        """
        Generate comprehensive daily audit report.

        Combines all metrics into a single report suitable for
        daily review during soak testing.
        """
        with self._lock:
            uptime_s = time.time() - self._boot_time
            uptime_hours = uptime_s / 3600

            return {
                'generated_at': datetime.utcnow().isoformat(),
                'uptime_hours': round(uptime_hours, 2),
                'restart_count': self._restart_count,
                'memory': self.get_memory_trend(),
                'workers': self.get_worker_stats(),
                'endpoints': self.get_endpoint_stats(),
                'database': self.get_db_stats(),
                'drift': self.get_drift_report(),
                'circuit_breakers': {
                    'recent_events': len(self._circuit_breaker_events),
                    'events': self.get_circuit_breaker_history(10)
                },
                'alerts': {
                    'active': self.get_active_alerts(),
                    'active_count': len(self._active_alerts),
                    'critical_count': sum(1 for v in self._active_alerts.values()
                                         if v in (AlertLevel.CRITICAL, AlertLevel.FREEZE)),
                    'recent': self.get_alert_history(10)
                },
                'health_score': self._compute_health_score()
            }

    def _compute_health_score(self) -> Dict[str, Any]:
        """
        Compute overall system health score (0-100).

        Factors: memory stability, drift, worker freshness,
        circuit breakers, active alerts.
        """
        score = 100
        deductions = []

        # Memory leak
        mem_trend = self.get_memory_trend()
        if mem_trend.get('leak_suspected'):
            score -= 20
            deductions.append('Memory leak suspected (-20)')
        elif mem_trend.get('growth_rate_mb_per_hour', 0) > 2.0:
            score -= 10
            deductions.append('High memory growth (-10)')

        # Drift
        for strategy, drift_data in self.get_drift_report().items():
            if drift_data['alert_level'] == 'FREEZE':
                score -= 30
                deductions.append(f'{strategy} drift FREEZE (-30)')
            elif drift_data['alert_level'] == 'CRITICAL':
                score -= 15
                deductions.append(f'{strategy} drift CRITICAL (-15)')
            elif drift_data['alert_level'] == 'WARNING':
                score -= 5
                deductions.append(f'{strategy} drift WARNING (-5)')

        # Stale workers
        for name, stats in self.get_worker_stats().items():
            if stats.get('stale'):
                score -= 10
                deductions.append(f'Worker {name} stale (-10)')

        # Active critical alerts
        critical_count = sum(1 for v in self._active_alerts.values()
                             if v in (AlertLevel.CRITICAL, AlertLevel.FREEZE))
        if critical_count > 0:
            score -= critical_count * 10
            deductions.append(f'{critical_count} critical alerts (-{critical_count * 10})')

        score = max(0, score)
        grade = 'HEALTHY' if score >= 80 else 'DEGRADED' if score >= 50 else 'UNHEALTHY'

        return {
            'score': score,
            'grade': grade,
            'deductions': deductions
        }

    # ── Full Status ──────────────────────────────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        """Get compact status for /ops endpoint."""
        with self._lock:
            return {
                'uptime_s': round(time.time() - self._boot_time, 1),
                'restart_count': self._restart_count,
                'memory_rss_mb': self._memory_history[-1]['rss_mb'] if self._memory_history else 0,
                'active_alerts': len(self._active_alerts),
                'critical_alerts': sum(1 for v in self._active_alerts.values()
                                       if v in (AlertLevel.CRITICAL, AlertLevel.FREEZE)),
                'health_score': self._compute_health_score()['score'],
                'health_grade': self._compute_health_score()['grade'],
            }


__all__ = [
    'AlertLevel',
    'OpsMetricsCollector',
]
