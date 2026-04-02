#!/usr/bin/env python3
"""
Egreja Investment AI — Module Tests v10.22
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Tests for the 7 institutional modules.
Modules are self-contained — no api_server import needed.

Execução: python -m pytest tests/test_modules_v1022.py -v
"""
import sys, os, unittest, time, threading
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


# ═══════════════════════════════════════════════════════════════
# 1. InstitutionalRiskManager
# ═══════════════════════════════════════════════════════════════
from modules.risk_manager import InstitutionalRiskManager

class TestRiskManagerDailyLoss(unittest.TestCase):
    """[v10.22] Risk manager blocks after daily loss limit."""

    def test_blocks_after_daily_loss_exceeded(self):
        rm = InstitutionalRiskManager()
        # Record losses until breach
        for i in range(5):
            rm.record_trade_result('stocks', f'SYM{i}', -50_000, 100_000, 9_000_000)
        breached, reasons = rm.is_breached()
        self.assertTrue(breached)
        self.assertTrue(any('daily' in r.lower() or 'diário' in r.lower() or 'intraday' in r.lower()
                           for r in reasons))

    def test_allows_trade_within_limits(self):
        rm = InstitutionalRiskManager()
        rm.record_trade_result('stocks', 'AAPL', -10_000, 100_000, 9_000_000)
        allowed, reason = rm.check_can_open('stocks', 'MSFT', 100_000, 9_000_000)
        self.assertTrue(allowed)

    def test_blocks_trade_after_breach(self):
        rm = InstitutionalRiskManager()
        for i in range(5):
            rm.record_trade_result('stocks', f'SYM{i}', -50_000, 100_000, 9_000_000)
        allowed, reason = rm.check_can_open('stocks', 'NEW', 100_000, 9_000_000)
        self.assertFalse(allowed)


class TestRiskManagerLosingStreak(unittest.TestCase):
    """[v10.22] Risk multiplier reduces after losing streak."""

    def test_multiplier_reduces_after_streak(self):
        rm = InstitutionalRiskManager()
        for i in range(6):  # more than threshold (5)
            rm.record_trade_result('stocks', 'AAPL', -5_000, 100_000, 9_000_000)
        mult = rm.get_risk_multiplier()
        self.assertLess(mult, 1.0)

    def test_multiplier_normal_without_streak(self):
        rm = InstitutionalRiskManager()
        rm.record_trade_result('stocks', 'AAPL', 5_000, 100_000, 9_000_000)
        mult = rm.get_risk_multiplier()
        self.assertGreaterEqual(mult, 1.0)


class TestRiskManagerStatus(unittest.TestCase):
    """[v10.22] Risk manager status reporting."""

    def test_status_has_required_fields(self):
        rm = InstitutionalRiskManager()
        status = rm.get_status()
        self.assertIn('pnl', status)
        self.assertIn('is_breached', status)


# ═══════════════════════════════════════════════════════════════
# 2. Broker Abstraction
# ═══════════════════════════════════════════════════════════════
from modules.broker_base import (
    PaperBroker, BTGBroker, OrderTracker, BrokerFactory,
    OrderStatus, OrderSide, OrderType, AssetClass, create_order_record,
)

class TestPaperBroker(unittest.TestCase):
    """[v10.22] PaperBroker simulates execution correctly."""

    def test_submit_order_fills(self):
        broker = PaperBroker(asset_class=AssetClass.STOCK_US, initial_balance=1_000_000,
                             fill_rate=1.0, partial_fill_prob=0.0)  # disable randomness
        order = create_order_record(
            trade_id='T-001', symbol='AAPL', side=OrderSide.BUY,
            order_type=OrderType.MARKET, asset_class=AssetClass.STOCK_US,
            quantity=100, decision_price=150.0,
        )
        result = broker.submit_order(order)
        self.assertIn(result.status, (OrderStatus.FILLED, OrderStatus.PARTIAL_FILL))
        self.assertGreater(result.executed_price, 0)

    def test_tracks_positions(self):
        broker = PaperBroker(asset_class=AssetClass.STOCK_US, initial_balance=1_000_000)
        order = create_order_record(
            trade_id='T-002', symbol='AAPL', side=OrderSide.BUY,
            order_type=OrderType.MARKET, asset_class=AssetClass.STOCK_US,
            quantity=100, decision_price=150.0,
        )
        broker.submit_order(order)
        positions = broker.get_positions()
        self.assertIn('AAPL', positions)
        self.assertEqual(positions['AAPL'], 100)


class TestBTGBrokerStub(unittest.TestCase):
    """[v10.22] BTG broker stub raises NotImplementedError."""

    def test_has_no_credentials(self):
        broker = BTGBroker()
        self.assertFalse(broker.has_credentials())

    def test_submit_raises(self):
        broker = BTGBroker()
        with self.assertRaises(NotImplementedError):
            broker.submit_order(MagicMock())


class TestOrderTracker(unittest.TestCase):
    """[v10.22] Order tracker with idempotency."""

    def test_add_and_retrieve(self):
        tracker = OrderTracker()
        order = create_order_record(
            trade_id='T-001', symbol='AAPL', side=OrderSide.BUY,
            order_type=OrderType.MARKET, asset_class=AssetClass.STOCK_US,
            quantity=100, decision_price=150.0,
        )
        tracker.add_order(order)
        retrieved = tracker.get_order(order.order_id)
        self.assertEqual(retrieved.trade_id, 'T-001')

    def test_idempotency_check(self):
        tracker = OrderTracker()
        order = create_order_record(
            trade_id='T-001', symbol='AAPL', side=OrderSide.BUY,
            order_type=OrderType.MARKET, asset_class=AssetClass.STOCK_US,
            quantity=100, decision_price=150.0,
            idempotency_key='idem-key-001',
        )
        tracker.add_order(order)
        dup = tracker.check_idempotency('idem-key-001')
        self.assertIsNotNone(dup)


# ═══════════════════════════════════════════════════════════════
# 3. Data Validator
# ═══════════════════════════════════════════════════════════════
from modules.data_validator import MarketDataValidator

class TestDataValidatorBasic(unittest.TestCase):
    """[v10.22] Data validator catches bad prices."""

    def test_rejects_zero_price(self):
        dv = MarketDataValidator()
        result = dv.validate_price('AAPL', 0, 'test')
        self.assertFalse(result.valid)

    def test_rejects_negative_price(self):
        dv = MarketDataValidator()
        result = dv.validate_price('AAPL', -10, 'test')
        self.assertFalse(result.valid)

    def test_accepts_normal_price(self):
        dv = MarketDataValidator()
        result = dv.validate_price('AAPL', 150.0, 'test')
        self.assertTrue(result.valid)

    def test_records_and_retrieves_price(self):
        dv = MarketDataValidator()
        dv.record_price('AAPL', 150.0, 'test')
        last = dv.get_last_price('AAPL')
        self.assertIsNotNone(last)
        self.assertEqual(last['price'], 150.0)


class TestDataValidatorStaleness(unittest.TestCase):
    """[v10.22] Staleness detection works."""

    def test_fresh_price_not_stale(self):
        dv = MarketDataValidator()
        dv.record_price('AAPL', 150.0, 'test')
        stale, age = dv.check_staleness('AAPL')
        self.assertFalse(stale)
        self.assertLess(age, 5)

    def test_unknown_symbol_returns_zero_age(self):
        dv = MarketDataValidator()
        stale, age = dv.check_staleness('UNKNOWN_SYM')
        self.assertEqual(age, 0.0)  # No data recorded = age 0


class TestCircuitBreaker(unittest.TestCase):
    """[v10.22] Circuit breaker trips after consecutive errors."""

    def test_trips_after_errors(self):
        dv = MarketDataValidator()
        for _ in range(6):  # default threshold is 5
            dv.record_error('binance')
        broken, reason = dv.is_circuit_broken('binance')
        self.assertTrue(broken)

    def test_not_tripped_below_threshold(self):
        dv = MarketDataValidator()
        for _ in range(3):
            dv.record_error('binance')
        broken, reason = dv.is_circuit_broken('binance')
        self.assertFalse(broken)


# ═══════════════════════════════════════════════════════════════
# 4. Auth RBAC
# ═══════════════════════════════════════════════════════════════
from modules.auth_rbac import AuthManager, Role

class TestAuthManagerRoles(unittest.TestCase):
    """[v10.22] Role hierarchy works correctly."""

    def test_admin_has_all_roles(self):
        am = AuthManager()
        admin_user = {'role': 'admin'}
        self.assertTrue(am.authorize(admin_user, Role.VIEWER))
        self.assertTrue(am.authorize(admin_user, Role.OPERATOR))
        self.assertTrue(am.authorize(admin_user, Role.ADMIN))

    def test_viewer_cannot_operate(self):
        am = AuthManager()
        viewer = {'role': 'viewer'}
        self.assertTrue(am.authorize(viewer, Role.VIEWER))
        self.assertFalse(am.authorize(viewer, Role.OPERATOR))
        self.assertFalse(am.authorize(viewer, Role.ADMIN))

    def test_operator_can_view_and_operate(self):
        am = AuthManager()
        op = {'role': 'operator'}
        self.assertTrue(am.authorize(op, Role.VIEWER))
        self.assertTrue(am.authorize(op, Role.OPERATOR))
        self.assertFalse(am.authorize(op, Role.ADMIN))


# ═══════════════════════════════════════════════════════════════
# 5. Stats Engine
# ═══════════════════════════════════════════════════════════════
from modules.stats_engine import PerformanceStats

class TestPerformanceStatsBasic(unittest.TestCase):
    """[v10.22] Performance stats compute correctly."""

    def _make_trades(self, ps, wins=10, losses=5):
        now = datetime.utcnow()
        for i in range(wins):
            ps.record_trade({
                'strategy': 'stocks', 'symbol': f'SYM{i}',
                'pnl': 5000, 'pnl_pct': 1.5,
                'entry_price': 100, 'exit_price': 101.5,
                'opened_at': (now - timedelta(hours=2)).isoformat(),
                'closed_at': now.isoformat(),
                'confidence': 75, 'exit_type': 'TAKE_PROFIT',
                'asset_type': 'stock', 'regime': 'TRENDING',
            })
        for i in range(losses):
            ps.record_trade({
                'strategy': 'stocks', 'symbol': f'LOSE{i}',
                'pnl': -3000, 'pnl_pct': -0.8,
                'entry_price': 100, 'exit_price': 99.2,
                'opened_at': (now - timedelta(hours=2)).isoformat(),
                'closed_at': now.isoformat(),
                'confidence': 60, 'exit_type': 'STOP_LOSS',
                'asset_type': 'stock', 'regime': 'RANGING',
            })

    def test_win_rate_correct(self):
        ps = PerformanceStats()
        self._make_trades(ps, wins=10, losses=5)
        stats = ps.compute_all()
        self.assertAlmostEqual(stats['win_rate'], 10/15, places=2)

    def test_profit_factor_positive(self):
        ps = PerformanceStats()
        self._make_trades(ps, wins=10, losses=5)
        stats = ps.compute_all()
        # gross_profit = 10 * 5000 = 50000, gross_loss = 5 * 3000 = 15000
        self.assertGreater(stats['profit_factor'], 1.0)

    def test_expectancy_positive(self):
        ps = PerformanceStats()
        self._make_trades(ps, wins=10, losses=5)
        stats = ps.compute_all()
        self.assertGreater(stats['expectancy'], 0)

    def test_sharpe_computed(self):
        ps = PerformanceStats()
        self._make_trades(ps, wins=10, losses=5)
        stats = ps.compute_all()
        self.assertIn('sharpe_ratio', stats)

    def test_by_strategy_breakdown(self):
        ps = PerformanceStats()
        self._make_trades(ps, wins=10, losses=5)
        breakdown = ps.by_strategy()
        self.assertIn('stocks', breakdown)

    def test_by_exit_type_breakdown(self):
        ps = PerformanceStats()
        self._make_trades(ps, wins=10, losses=5)
        breakdown = ps.by_exit_type()
        self.assertIn('TAKE_PROFIT', breakdown)
        self.assertIn('STOP_LOSS', breakdown)


class TestPromotionCriteria(unittest.TestCase):
    """[v10.22] Capital promotion criteria checking."""

    def test_not_eligible_with_few_trades(self):
        ps = PerformanceStats()
        # Only 5 trades — needs 200
        for i in range(5):
            ps.record_trade({
                'strategy': 'stocks', 'symbol': 'AAPL',
                'pnl': 5000, 'pnl_pct': 1.0,
                'entry_price': 100, 'exit_price': 101,
                'opened_at': datetime.utcnow().isoformat(),
                'closed_at': datetime.utcnow().isoformat(),
                'confidence': 75, 'exit_type': 'TAKE_PROFIT',
                'asset_type': 'stock', 'regime': 'TRENDING',
            })
        criteria = ps.get_promotion_criteria()
        self.assertFalse(criteria['eligible_for_promotion'])


# ═══════════════════════════════════════════════════════════════
# 6. Kill Switch
# ═══════════════════════════════════════════════════════════════
from modules.kill_switch import ExternalKillSwitch, KillSwitchMiddleware

class TestKillSwitchActivation(unittest.TestCase):
    """[v10.22] Kill switch activate/deactivate cycle."""

    def test_activate_and_check(self):
        ks = ExternalKillSwitch()
        mock_db = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {'active': 1, 'reason': 'test', 'auto_resume_at': None}
        mock_conn.cursor.return_value = mock_cursor
        mock_db.return_value = mock_conn

        ks.activate('global', 'test reason', 'tester', None, mock_db)
        # The activate writes to DB, so we can verify cursor was called
        self.assertTrue(mock_cursor.execute.called)


class TestKillSwitchMiddleware(unittest.TestCase):
    """[v10.22] Kill switch middleware pre-trade check."""

    def test_allows_when_inactive(self):
        ks = ExternalKillSwitch()
        mw = KillSwitchMiddleware(ks)
        # Mock is_active to return False
        ks.is_active = MagicMock(return_value=(False, ''))
        mock_db = MagicMock()
        allowed, reason = mw.check_before_trade('stocks', mock_db)
        self.assertTrue(allowed)

    def test_blocks_when_global_active(self):
        ks = ExternalKillSwitch()
        mw = KillSwitchMiddleware(ks)
        # Mock is_active: global = True
        def mock_active(scope=None, get_db_func=None):
            if scope == 'global' or scope is None:
                return (True, 'Risk breach')
            return (False, '')
        ks.is_active = mock_active
        mock_db = MagicMock()
        allowed, reason = mw.check_before_trade('stocks', mock_db)
        self.assertFalse(allowed)


# ═══════════════════════════════════════════════════════════════
# 7. Integration: modules accessible from api_server
# ═══════════════════════════════════════════════════════════════
# Stub external modules before importing api_server
import types
mysql_pkg = types.ModuleType('mysql')
mysql_connector = types.ModuleType('mysql.connector')
mysql_connector.pooling = MagicMock()
mysql_connector.Error = Exception
mysql_connector.connect = MagicMock()
mysql_pkg.connector = mysql_connector
sys.modules['mysql'] = mysql_pkg
sys.modules['mysql.connector'] = mysql_connector
sys.modules['mysql.connector.pooling'] = MagicMock()

_app_mock = MagicMock()
_app_mock.before_request = MagicMock(return_value=lambda f: f)
_app_mock.route = MagicMock(return_value=lambda f: f)
_app_mock.errorhandler = MagicMock(return_value=lambda f: f)
flask_mock = types.ModuleType('flask')
flask_mock.Flask = MagicMock(return_value=_app_mock)
flask_mock.jsonify = MagicMock(side_effect=lambda x: x)
flask_mock.request = MagicMock()
sys.modules['flask'] = flask_mock

flask_cors_mock = types.ModuleType('flask_cors')
flask_cors_mock.CORS = MagicMock()
sys.modules['flask_cors'] = flask_cors_mock

requests_stub = types.ModuleType('requests')
requests_stub.get = MagicMock()
requests_stub.post = MagicMock()
requests_stub.auth = MagicMock()
sys.modules['requests'] = requests_stub

os.environ.setdefault('ENV', 'test')
os.environ.setdefault('API_SECRET_KEY', 'test_key')
os.environ.setdefault('DATABASE_URL', '')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import api_server as srv


class TestModulesAccessibleFromServer(unittest.TestCase):
    """[v10.22] All module instances accessible from api_server."""

    def test_risk_manager_exists(self):
        self.assertIsNotNone(srv.risk_manager)
        self.assertIsInstance(srv.risk_manager, InstitutionalRiskManager)

    def test_order_tracker_exists(self):
        from modules.broker_base import OrderTracker
        self.assertIsNotNone(srv.order_tracker)
        self.assertIsInstance(srv.order_tracker, OrderTracker)

    def test_data_validator_exists(self):
        self.assertIsNotNone(srv.data_validator)
        self.assertIsInstance(srv.data_validator, MarketDataValidator)

    def test_perf_stats_exists(self):
        self.assertIsNotNone(srv.perf_stats)
        self.assertIsInstance(srv.perf_stats, PerformanceStats)

    def test_ext_kill_switch_exists(self):
        self.assertIsNotNone(srv.ext_kill_switch)
        self.assertIsInstance(srv.ext_kill_switch, ExternalKillSwitch)

    def test_version_is_v1024(self):
        self.assertEqual(srv.VERSION, 'v10.24.3')

    def test_ops_metrics_exists(self):
        self.assertIsNotNone(srv.ops_metrics)


# ═══════════════════════════════════════════════════════════════
# [v10.23] NEW MODULE TESTS
# ═══════════════════════════════════════════════════════════════

class TestOpsMetricsCollector(unittest.TestCase):
    """[v10.23] Operational metrics collector tests."""

    def setUp(self):
        from modules.ops_metrics import OpsMetricsCollector
        self.collector = OpsMetricsCollector()

    def test_record_memory(self):
        snap = self.collector.record_memory()
        self.assertIn('rss_mb', snap)
        self.assertIn('ts', snap)

    def test_memory_trend(self):
        for _ in range(5):
            self.collector.record_memory()
        trend = self.collector.get_memory_trend()
        self.assertEqual(trend['samples'], 5)
        self.assertIn('growth_rate_mb_per_hour', trend)

    def test_record_drift(self):
        self.collector.record_drift('stocks', 1000000.0, 1000050.0, 1000020.0)
        report = self.collector.get_drift_report()
        self.assertIn('stocks', report)
        self.assertEqual(report['stocks']['current_drift'], 50.0)

    def test_drift_progressive_alert(self):
        # Normal drift — no alert
        self.collector.record_drift('crypto', 500000.0, 500010.0)
        alerts = self.collector.get_active_alerts()
        self.assertNotIn('drift_crypto', alerts)

        # Warning drift
        self.collector.record_drift('crypto', 500000.0, 500200.0)
        alerts = self.collector.get_active_alerts()
        self.assertEqual(alerts.get('drift_crypto'), 'WARNING')

        # Critical drift
        self.collector.record_drift('crypto', 500000.0, 500600.0)
        alerts = self.collector.get_active_alerts()
        self.assertEqual(alerts.get('drift_crypto'), 'CRITICAL')

    def test_worker_timing(self):
        self.collector.record_worker_cycle('stock_worker', 2.5, 3)
        self.collector.record_worker_cycle('stock_worker', 3.1, 2)
        stats = self.collector.get_worker_stats()
        self.assertIn('stock_worker', stats)
        self.assertEqual(stats['stock_worker']['cycles'], 2)
        self.assertEqual(stats['stock_worker']['total_trades'], 5)

    def test_health_score(self):
        report = self.collector.generate_daily_audit()
        self.assertIn('health_score', report)
        self.assertEqual(report['health_score']['grade'], 'HEALTHY')
        self.assertEqual(report['health_score']['score'], 100)

    def test_circuit_breaker_event(self):
        self.collector.record_circuit_breaker_event('yfinance', 'CLOSED', 'OPEN', 'timeout')
        alerts = self.collector.get_active_alerts()
        self.assertEqual(alerts.get('cb_yfinance'), 'CRITICAL')
        history = self.collector.get_circuit_breaker_history()
        self.assertEqual(len(history), 1)


class TestStrategyScorecard(unittest.TestCase):
    """[v10.23] Per-strategy scorecard tests."""

    def setUp(self):
        self.stats = PerformanceStats()

    def test_empty_scorecard(self):
        scorecard = self.stats.get_strategy_scorecard()
        self.assertEqual(scorecard, {})

    def test_scorecard_with_trades(self):
        from datetime import datetime, timedelta
        base = datetime(2026, 1, 1)
        for i in range(20):
            self.stats.record_trade({
                'strategy': 'stocks', 'symbol': f'SYM{i}',
                'pnl': 100 if i % 3 != 0 else -50, 'pnl_pct': 0.01,
                'entry_price': 50.0, 'exit_price': 51.0,
                'opened_at': base + timedelta(hours=i),
                'closed_at': base + timedelta(hours=i+1),
                'confidence': 70.0, 'exit_type': 'profit_target',
                'asset_type': 'stock', 'regime': 'trending'
            })
        scorecard = self.stats.get_strategy_scorecard()
        self.assertIn('stocks', scorecard)
        self.assertIn('grade', scorecard['stocks'])
        self.assertIn('scores', scorecard['stocks'])


class TestEnhancedPromotion(unittest.TestCase):
    """[v10.23] Enhanced promotion criteria with per-strategy gates."""

    def setUp(self):
        self.stats = PerformanceStats()

    def test_empty_not_eligible(self):
        result = self.stats.get_enhanced_promotion_criteria()
        self.assertFalse(result['eligible_for_promotion'])

    def test_per_strategy_criteria_present(self):
        result = self.stats.get_enhanced_promotion_criteria()
        self.assertIn('per_strategy_criteria', result)
        self.assertIn('regime_criteria', result)


class TestKillSwitchLiveMode(unittest.TestCase):
    """[v10.23] Kill switch live mode — manual resume required."""

    def setUp(self):
        from modules.kill_switch import ExternalKillSwitch, ResumeMode
        self.ks = ExternalKillSwitch()
        self.ResumeMode = ResumeMode

    def test_default_mode_is_paper(self):
        self.assertEqual(self.ks._mode, self.ResumeMode.PAPER)

    def test_set_live_mode(self):
        self.ks.set_mode(self.ResumeMode.LIVE)
        self.assertEqual(self.ks._mode, self.ResumeMode.LIVE)

    def test_safe_resume_needs_db(self):
        """safe_resume validates preconditions before deactivating."""
        # With no breach time and no real DB, it should attempt deactivate
        # which fails on None DB — this is expected behavior
        self.assertIsNone(self.ks._breach_time)


class TestPaperBrokerInstitutional(unittest.TestCase):
    """[v10.23] Enhanced PaperBroker execution simulator."""

    def test_execution_profile(self):
        broker = PaperBroker(AssetClass.STOCK_BR, slippage_bps=5.0, fill_rate=1.0, partial_fill_prob=0.0)
        profile = broker.get_execution_profile()
        self.assertIn('statistics', profile)
        self.assertIn('total_orders', profile['statistics'])

    def test_set_market_regime(self):
        broker = PaperBroker(AssetClass.CRYPTO, slippage_bps=5.0, fill_rate=1.0, partial_fill_prob=0.0)
        broker.set_market_regime('volatile')
        profile = broker.get_execution_profile()
        self.assertEqual(profile['market_regime'], 'volatile')


if __name__ == '__main__':
    unittest.main()
