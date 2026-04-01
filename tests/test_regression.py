#!/usr/bin/env python3
"""
Egreja Investment AI — Regression + Integration Tests v10.20
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Testes de regressão REAIS + testes de integração.
Import do api_server.py FALHA ALTO se quebrado (não silencia com SkipTest).
DB e enqueue_persist mockados; lógica de negócio testada de verdade.

Execução: python -m pytest tests/test_regression.py -v
"""
import sys, os, types, unittest, threading
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, PropertyMock

# ── Stub external modules before importing api_server ────────────────────
# mysql (parent package + connector sub-module)
mysql_pkg = types.ModuleType('mysql')
mysql_connector = types.ModuleType('mysql.connector')
mysql_connector.pooling = MagicMock()
mysql_connector.Error = Exception
mysql_connector.connect = MagicMock()
mysql_pkg.connector = mysql_connector
sys.modules['mysql'] = mysql_pkg
sys.modules['mysql.connector'] = mysql_connector
sys.modules['mysql.connector.pooling'] = MagicMock()

# Flask
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

# requests
requests_stub = types.ModuleType('requests')
requests_stub.get = MagicMock()
requests_stub.post = MagicMock()
requests_stub.auth = MagicMock()
sys.modules['requests'] = requests_stub

# Set env vars before import
os.environ.setdefault('ENV', 'test')
os.environ.setdefault('API_SECRET_KEY', 'test_key')
os.environ.setdefault('DATABASE_URL', '')

# ── Import api_server — FAIL HARD if broken ──────────────────────────────
# [v10.20] Se o import quebrar, a suíte inteira fica vermelha. Sem SkipTest silencioso.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import api_server as srv


# ═══════════════════════════════════════════════════════════════
# 1. is_trade_flat — real function call
# ═══════════════════════════════════════════════════════════════
class TestIsTradeFlatReal(unittest.TestCase):
    """Bug: capital preso em trades estagnadas. Fix: v10.17 is_trade_flat."""

    def test_flat_stock_triggers_after_min_age(self):
        """Stock trade com 60min e variação 0.1% → flat."""
        now = datetime.utcnow()
        trade = {
            'opened_at': (now - timedelta(minutes=60)).isoformat(),
            'pnl_pct': 0.1,
            'peak_pnl_pct': 0.2,
            'pnl_history': [0.1, 0.12, 0.1],
            'asset_type': 'stock',
        }
        self.assertTrue(srv.is_trade_flat(trade, now))

    def test_flat_stock_too_young(self):
        """Stock trade com 20min → NÃO flat (min age = 45min)."""
        now = datetime.utcnow()
        trade = {
            'opened_at': (now - timedelta(minutes=20)).isoformat(),
            'pnl_pct': 0.05,
            'peak_pnl_pct': 0.1,
            'pnl_history': [0.05, 0.06, 0.05],
            'asset_type': 'stock',
        }
        self.assertFalse(srv.is_trade_flat(trade, now))

    def test_crypto_min_hold_15min(self):
        """Crypto trade com 10min → NÃO flat (min hold = 15min)."""
        now = datetime.utcnow()
        trade = {
            'opened_at': (now - timedelta(minutes=10)).isoformat(),
            'pnl_pct': 0.05,
            'peak_pnl_pct': 0.1,
            'pnl_history': [0.05, 0.06, 0.05],
            'asset_type': 'crypto',
        }
        self.assertFalse(srv.is_trade_flat(trade, now))

    def test_crypto_flat_after_min_hold(self):
        """Crypto trade com 50min e variação 0.1% → flat."""
        now = datetime.utcnow()
        trade = {
            'opened_at': (now - timedelta(minutes=50)).isoformat(),
            'pnl_pct': 0.08,
            'peak_pnl_pct': 0.15,
            'pnl_history': [0.08, 0.09, 0.08],
            'asset_type': 'crypto',
        }
        self.assertTrue(srv.is_trade_flat(trade, now))

    def test_not_flat_if_peak_high(self):
        """Trade que teve momentum alto (peak >0.5%) → NÃO flat."""
        now = datetime.utcnow()
        trade = {
            'opened_at': (now - timedelta(minutes=60)).isoformat(),
            'pnl_pct': 0.1,
            'peak_pnl_pct': 1.5,  # teve momentum significativo
            'pnl_history': [0.1, 0.12, 0.1],
            'asset_type': 'stock',
        }
        self.assertFalse(srv.is_trade_flat(trade, now))


# ═══════════════════════════════════════════════════════════════
# 2. check_directional_exposure — real function call
# ═══════════════════════════════════════════════════════════════
class TestCheckDirectionalExposureReal(unittest.TestCase):
    """Bug: sem limite, 100% posições podiam ser LONG. Fix: v10.17."""

    def test_blocks_excess_longs(self):
        """80% LONG → bloqueia novo LONG."""
        # Temporarily replace stocks_open
        original = srv.stocks_open[:]
        try:
            srv.stocks_open[:] = [{'direction': 'LONG', 'symbol': f'S{i}'} for i in range(8)] + \
                                  [{'direction': 'SHORT', 'symbol': f'S{i}'} for i in range(8, 10)]
            blocked, reason, stats = srv.check_directional_exposure('LONG', 'stocks')
            self.assertTrue(blocked, f'Should block LONG at 80%. reason={reason}')
            self.assertIn('DIRECTIONAL_LIMIT', reason)
        finally:
            srv.stocks_open[:] = original

    def test_allows_balanced_direction(self):
        """50% LONG → permite novo LONG."""
        original = srv.stocks_open[:]
        try:
            srv.stocks_open[:] = [{'direction': 'LONG', 'symbol': f'S{i}'} for i in range(5)] + \
                                  [{'direction': 'SHORT', 'symbol': f'S{i}'} for i in range(5, 10)]
            blocked, reason, stats = srv.check_directional_exposure('LONG', 'stocks')
            self.assertFalse(blocked, f'Should allow LONG at 50%. reason={reason}')
        finally:
            srv.stocks_open[:] = original

    def test_ignores_small_set(self):
        """Com <3 trades, NÃO bloqueia (sem sentido estatístico)."""
        original = srv.stocks_open[:]
        try:
            srv.stocks_open[:] = [{'direction': 'LONG', 'symbol': 'A'}, {'direction': 'LONG', 'symbol': 'B'}]
            blocked, reason, stats = srv.check_directional_exposure('LONG', 'stocks')
            self.assertFalse(blocked, 'Should not block with < 3 positions')
        finally:
            srv.stocks_open[:] = original


# ═══════════════════════════════════════════════════════════════
# 3. get_dynamic_timeout_h — real function call
# ═══════════════════════════════════════════════════════════════
class TestGetDynamicTimeoutReal(unittest.TestCase):
    """Bug: timeout fixo não reflete duração real. Fix: v10.17."""

    def test_returns_default_without_history(self):
        """Sem histórico → retorna default."""
        timeout = srv.get_dynamic_timeout_h('UNKNOWN_SYM', 3.0)
        self.assertEqual(timeout, 3.0)

    def test_uses_history_when_available(self):
        """Com ≥5 trades de histórico, usa avg * mult."""
        original = srv._symbol_avg_duration.copy()
        try:
            srv._symbol_avg_duration['TESTETH'] = {'sum_h': 10.0, 'n': 5, 'avg_h': 2.0}
            timeout = srv.get_dynamic_timeout_h('TESTETH', 4.0)
            # Expected: 2.0 * 1.3 = 2.6, clamped between 1.5 and 6.0
            expected = max(srv.DYNAMIC_TIMEOUT_MIN_H,
                           min(2.0 * srv.DYNAMIC_TIMEOUT_MULT, srv.DYNAMIC_TIMEOUT_MAX_H))
            self.assertAlmostEqual(timeout, expected, places=2)
        finally:
            srv._symbol_avg_duration.clear()
            srv._symbol_avg_duration.update(original)

    def test_clamps_to_min(self):
        """Avg muito curto → clamp ao mínimo."""
        original = srv._symbol_avg_duration.copy()
        try:
            srv._symbol_avg_duration['FASTSYM'] = {'sum_h': 2.5, 'n': 5, 'avg_h': 0.5}
            timeout = srv.get_dynamic_timeout_h('FASTSYM', 4.0)
            self.assertEqual(timeout, srv.DYNAMIC_TIMEOUT_MIN_H)
        finally:
            srv._symbol_avg_duration.clear()
            srv._symbol_avg_duration.update(original)

    def test_clamps_to_max(self):
        """Avg muito longo → clamp ao máximo."""
        original = srv._symbol_avg_duration.copy()
        try:
            srv._symbol_avg_duration['SLOWSYM'] = {'sum_h': 50.0, 'n': 5, 'avg_h': 10.0}
            timeout = srv.get_dynamic_timeout_h('SLOWSYM', 4.0)
            self.assertEqual(timeout, srv.DYNAMIC_TIMEOUT_MAX_H)
        finally:
            srv._symbol_avg_duration.clear()
            srv._symbol_avg_duration.update(original)


# ═══════════════════════════════════════════════════════════════
# 4. check_crypto_conviction — real function call
# ═══════════════════════════════════════════════════════════════
class TestCheckCryptoConvictionReal(unittest.TestCase):
    """v10.18: Conviction filter para crypto."""

    def test_blocks_low_conviction_small_move(self):
        """Conf<58 e change<3% → bloqueado."""
        ok, reason = srv.check_crypto_conviction({'final_confidence': 52}, 1.5, 'ETH')
        self.assertFalse(ok)
        self.assertIn('conviction_low', reason)

    def test_allows_low_conviction_big_move(self):
        """Conf<58 mas change≥3% → permitido."""
        ok, reason = srv.check_crypto_conviction({'final_confidence': 52}, 4.5, 'ETH')
        self.assertTrue(ok)

    def test_allows_high_conviction(self):
        """Conf≥58 → sempre permitido."""
        ok, reason = srv.check_crypto_conviction({'final_confidence': 65}, 0.5, 'ETH')
        self.assertTrue(ok)


# ═══════════════════════════════════════════════════════════════
# 5. _reconcile_strategy — real function call
# ═══════════════════════════════════════════════════════════════
class TestReconcileStrategyReal(unittest.TestCase):
    """v10.18: Reconciliação por fórmula detecta desvios."""

    def test_perfect_balance(self):
        """Sem desvio → ok=True, delta≈0."""
        open_trades = [{'position_value': 200_000}, {'position_value': 300_000}]
        closed_trades = [{'pnl': 5_000}, {'pnl': -2_000}]
        # capital = initial + 3000 - 500000 = initial - 497000
        initial = 9_000_000
        committed = 500_000
        realized = 3_000
        memory = initial + realized - committed  # 8_503_000
        r = srv._reconcile_strategy('stocks', memory, initial, open_trades, closed_trades)
        self.assertTrue(r['ok'])
        self.assertAlmostEqual(r['delta'], 0, places=0)

    def test_detects_drift(self):
        """Com desvio artificial → delta > 0."""
        open_trades = [{'position_value': 200_000}]
        closed_trades = [{'pnl': 10_000}]
        initial = 9_000_000
        # Correct: 9M + 10K - 200K = 8_810_000
        memory = 8_860_000  # $50K a mais que o correto
        r = srv._reconcile_strategy('stocks', memory, initial, open_trades, closed_trades)
        self.assertAlmostEqual(r['delta'], 50_000, places=0)
        self.assertGreater(r['delta_pct'], 0)

    def test_arbi_reconciliation(self):
        """Reconciliação de arbi usa position_size."""
        open_trades = [{'position_size': 100_000}]
        closed_trades = [{'pnl': 5_000}]
        initial = 4_500_000
        memory = initial + 5_000 - 100_000  # 4_405_000
        r = srv._reconcile_strategy_arbi(memory, initial, open_trades, closed_trades)
        self.assertTrue(r['ok'])
        self.assertAlmostEqual(r['delta'], 0, places=0)
        self.assertEqual(r['strategy'], 'arbi')


# ═══════════════════════════════════════════════════════════════
# 6. _reconcile_via_ledger — real function call
# ═══════════════════════════════════════════════════════════════
class TestReconcileViaLedgerReal(unittest.TestCase):
    """v10.19: Reconciliação via replay do ledger."""

    def test_empty_ledger_ok(self):
        """Sem eventos → ok=True."""
        # Ensure empty
        original = srv._capital_ledger[:]
        try:
            srv._capital_ledger.clear()
            r = srv._reconcile_via_ledger('stocks', 9_000_000, 9_000_000)
            self.assertTrue(r['ok'])
            self.assertEqual(r['ledger_events'], 0)
        finally:
            srv._capital_ledger[:] = original

    def test_ledger_replay_matches(self):
        """Replay de RESERVE+RELEASE+PNL_CREDIT deve bater com memória."""
        original = srv._capital_ledger[:]
        try:
            srv._capital_ledger.clear()
            initial = 9_000_000
            # Trade: reserve 200K, release 200K, PNL +5K
            srv._capital_ledger.extend([
                {'strategy': 'stocks', 'event': 'RESERVE', 'amount': 200_000},
                {'strategy': 'stocks', 'event': 'RELEASE', 'amount': 200_000},
                {'strategy': 'stocks', 'event': 'PNL_CREDIT', 'amount': 5_000},
            ])
            # Expected balance: 9M - 200K + 200K + 5K = 9_005_000
            memory = 9_005_000
            r = srv._reconcile_via_ledger('stocks', initial, memory)
            self.assertTrue(r['ok'])
            self.assertAlmostEqual(r['delta'], 0, places=0)
            self.assertEqual(r['ledger_events'], 3)
        finally:
            srv._capital_ledger[:] = original

    def test_ledger_detects_drift(self):
        """Se memória diverge do replay, delta > 0."""
        original = srv._capital_ledger[:]
        try:
            srv._capital_ledger.clear()
            initial = 1_000_000
            srv._capital_ledger.extend([
                {'strategy': 'crypto', 'event': 'RESERVE', 'amount': 100_000},
                {'strategy': 'crypto', 'event': 'RELEASE', 'amount': 100_000},
                {'strategy': 'crypto', 'event': 'PNL_CREDIT', 'amount': -3_000},
            ])
            # Expected: 1M - 100K + 100K - 3K = 997_000
            memory = 1_000_000  # +3K drift (didn't subtract PnL loss)
            r = srv._reconcile_via_ledger('crypto', initial, memory)
            self.assertAlmostEqual(r['delta'], 3_000, places=0)
            self.assertGreater(r['delta_pct'], 0)
        finally:
            srv._capital_ledger[:] = original


# ═══════════════════════════════════════════════════════════════
# 7. ledger_record — real function with mocked enqueue
# ═══════════════════════════════════════════════════════════════
class TestLedgerRecordReal(unittest.TestCase):
    """v10.18: ledger_record deve registrar em memória e enfileirar."""


    @patch.object(srv, 'enqueue_persist')
    def test_ledger_record_appends_and_enqueues(self, mock_enqueue):
        """ledger_record adiciona ao _capital_ledger e chama enqueue_persist."""
        original_len = len(srv._capital_ledger)
        srv.ledger_record('stocks', 'RESERVE', 'AAPL', 150_000, 2_850_000, 'STK-test')
        self.assertEqual(len(srv._capital_ledger), original_len + 1)
        evt = srv._capital_ledger[-1]
        self.assertEqual(evt['strategy'], 'stocks')
        self.assertEqual(evt['event'], 'RESERVE')
        self.assertEqual(evt['symbol'], 'AAPL')
        self.assertEqual(evt['amount'], 150_000)
        self.assertEqual(evt['balance_after'], 2_850_000)
        self.assertEqual(evt['trade_id'], 'STK-test')
        mock_enqueue.assert_called_once()
        call_args = mock_enqueue.call_args
        self.assertEqual(call_args[0][0], 'ledger_event')


    @patch.object(srv, 'enqueue_persist')
    def test_ledger_trims_at_5000(self, mock_enqueue):
        """Ledger deve podar para 3000 quando passa de 5000."""
        original = srv._capital_ledger[:]
        try:
            srv._capital_ledger[:] = [{'strategy': 'x', 'event': 'RESERVE', 'amount': 1}] * 5001
            srv.ledger_record('stocks', 'RESERVE', 'TEST', 1, 1, 'T-1')
            self.assertLessEqual(len(srv._capital_ledger), 3001)
        finally:
            srv._capital_ledger[:] = original


# ═══════════════════════════════════════════════════════════════
# 8. get_regime_multiplier — real function call
# ═══════════════════════════════════════════════════════════════
class TestGetRegimeMultiplierReal(unittest.TestCase):
    """v10.17: Regime-aware sizing."""

    def test_high_vol_regime(self):
        """HIGH_VOL → size 0.6x, SL 1.5x."""
        original = dict(srv.market_regime)
        try:
            srv.market_regime['mode'] = 'HIGH_VOL'
            srv.market_regime['volatility'] = 'HIGH'
            size_m, sl_m, reason = srv.get_regime_multiplier()
            self.assertAlmostEqual(size_m, 0.6)
            self.assertAlmostEqual(sl_m, 1.5)
        finally:
            srv.market_regime.update(original)

    def test_trending_regime(self):
        """TRENDING → size 1.2x, SL 1.3x."""
        original = dict(srv.market_regime)
        try:
            srv.market_regime['mode'] = 'TRENDING'
            srv.market_regime['volatility'] = 'NORMAL'
            size_m, sl_m, reason = srv.get_regime_multiplier()
            self.assertAlmostEqual(size_m, 1.2)
            self.assertAlmostEqual(sl_m, 1.3)
        finally:
            srv.market_regime.update(original)

    def test_ranging_regime(self):
        """RANGING → size 0.8x, SL 0.85x."""
        original = dict(srv.market_regime)
        try:
            srv.market_regime['mode'] = 'RANGING'
            srv.market_regime['volatility'] = 'NORMAL'
            size_m, sl_m, reason = srv.get_regime_multiplier()
            self.assertAlmostEqual(size_m, 0.8)
            self.assertAlmostEqual(sl_m, 0.85)
        finally:
            srv.market_regime.update(original)

    def test_normal_regime(self):
        """NORMAL/UNKNOWN → size 1.0x, SL 1.0x."""
        original = dict(srv.market_regime)
        try:
            srv.market_regime['mode'] = 'NORMAL'
            srv.market_regime['volatility'] = 'NORMAL'
            size_m, sl_m, reason = srv.get_regime_multiplier()
            self.assertAlmostEqual(size_m, 1.0)
            self.assertAlmostEqual(sl_m, 1.0)
        finally:
            srv.market_regime.update(original)


# ═══════════════════════════════════════════════════════════════
# 9. persist_calibration / load_calibration — with mocked DB
# ═══════════════════════════════════════════════════════════════
class TestCalibrationPersistenceReal(unittest.TestCase):
    """v10.18: Calibração persiste e restaura via MySQL."""


    @patch.object(srv, 'get_db')
    def test_persist_calibration_writes_bands(self, mock_get_db):
        """persist_calibration escreve as 3 bands no DB."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        # Force past interval
        srv._last_calibration_persist = 0
        srv.persist_calibration()

        # Should have 3 execute calls (HIGH, MEDIUM, LOW)
        self.assertEqual(mock_cursor.execute.call_count, 3)
        mock_conn.commit.assert_called_once()


    @patch.object(srv, 'get_db')
    def test_load_calibration_restores_bands(self, mock_get_db):
        """load_calibration restaura valores do DB."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {'band': 'HIGH', 'wins': 42, 'losses': 8, 'total': 50, 'sum_pnl_pct': 3.5},
            {'band': 'LOW', 'wins': 10, 'losses': 25, 'total': 35, 'sum_pnl_pct': -2.1},
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        # Reset
        original = {k: dict(v) for k, v in srv._calibration_tracker.items()}
        try:
            for band in srv._calibration_tracker:
                srv._calibration_tracker[band] = {'wins': 0, 'losses': 0, 'total': 0, 'sum_pnl_pct': 0.0}
            srv.load_calibration()
            self.assertEqual(srv._calibration_tracker['HIGH']['wins'], 42)
            self.assertEqual(srv._calibration_tracker['HIGH']['total'], 50)
            self.assertEqual(srv._calibration_tracker['LOW']['losses'], 25)
            self.assertAlmostEqual(srv._calibration_tracker['LOW']['sum_pnl_pct'], -2.1)
        finally:
            for k, v in original.items():
                srv._calibration_tracker[k] = v


# ═══════════════════════════════════════════════════════════════
# 10. track_calibration — real function call
# ═══════════════════════════════════════════════════════════════
class TestTrackCalibrationReal(unittest.TestCase):
    """v10.15: track_calibration atualiza contagem em memória."""

    def test_tracks_winning_trade(self):
        """Trade com pnl_pct > 0.1 → wins++."""
        original = {k: dict(v) for k, v in srv._calibration_tracker.items()}
        try:
            before_wins = srv._calibration_tracker['HIGH']['wins']
            before_total = srv._calibration_tracker['HIGH']['total']
            trade = {'_confidence_band': 'HIGH', 'pnl_pct': 1.5}
            srv.track_calibration(trade)
            self.assertEqual(srv._calibration_tracker['HIGH']['wins'], before_wins + 1)
            self.assertEqual(srv._calibration_tracker['HIGH']['total'], before_total + 1)
        finally:
            for k, v in original.items():
                srv._calibration_tracker[k] = v

    def test_tracks_losing_trade(self):
        """Trade com pnl_pct < -0.1 → losses++."""
        original = {k: dict(v) for k, v in srv._calibration_tracker.items()}
        try:
            before_losses = srv._calibration_tracker['MEDIUM']['losses']
            trade = {'_confidence_band': 'MEDIUM', 'pnl_pct': -0.5}
            srv.track_calibration(trade)
            self.assertEqual(srv._calibration_tracker['MEDIUM']['losses'], before_losses + 1)
        finally:
            for k, v in original.items():
                srv._calibration_tracker[k] = v


# ═══════════════════════════════════════════════════════════════
# 11. _db_save_ledger_event exists and is callable
# ═══════════════════════════════════════════════════════════════
class TestDbSaveLedgerEventExists(unittest.TestCase):
    """Critical bug (v10.18): _db_save_ledger_event was missing → persistence crash."""

    def test_function_exists(self):
        """_db_save_ledger_event deve existir como callable."""
        self.assertTrue(hasattr(srv, '_db_save_ledger_event'))
        self.assertTrue(callable(srv._db_save_ledger_event))


    @patch.object(srv, 'get_db')
    def test_function_handles_event(self, mock_get_db):
        """_db_save_ledger_event insere no MySQL sem crash."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        evt = {'ts': '2026-04-01T12:00:00', 'strategy': 'stocks',
               'event': 'RESERVE', 'symbol': 'AAPL', 'amount': 100_000,
               'balance_after': 8_900_000, 'trade_id': 'STK-test'}
        # Should not raise
        srv._db_save_ledger_event(evt)
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()


# ═══════════════════════════════════════════════════════════════
# INTEGRATION TESTS — open → close → ledger → reconcile
# ═══════════════════════════════════════════════════════════════
class TestIntegrationStockTradeCycle(unittest.TestCase):
    """[v10.20] Integration: simula ciclo completo de trade de stocks."""

    @patch.object(srv, 'enqueue_persist')
    def test_full_stock_cycle(self, mock_enqueue):
        """Open stock → ledger RESERVE → close → ledger RELEASE+PNL → reconcile OK."""
        # Save state
        orig_capital = srv.stocks_capital
        orig_open = srv.stocks_open[:]
        orig_closed = srv.stocks_closed[:]
        orig_ledger = srv._capital_ledger[:]
        try:
            # Reset
            srv.stocks_capital = 3_000_000
            srv.stocks_open.clear()
            srv.stocks_closed.clear()
            srv._capital_ledger.clear()

            # === OPEN: simulate stock trade open ===
            trade_id = 'STK-integ-001'
            symbol = 'AAPL'
            position_value = 200_000
            entry_price = 200.0
            qty = 1000

            srv.stocks_capital -= position_value
            srv.ledger_record('stocks', 'RESERVE', symbol, position_value,
                              srv.stocks_capital, trade_id)

            trade = {
                'id': trade_id, 'symbol': symbol, 'market': 'NYSE',
                'asset_type': 'stock', 'direction': 'LONG',
                'entry_price': entry_price, 'current_price': entry_price,
                'quantity': qty, 'position_value': position_value,
                'pnl': 0, 'pnl_pct': 0, 'peak_pnl_pct': 0,
                'opened_at': datetime.utcnow().isoformat(), 'status': 'OPEN',
            }
            srv.stocks_open.append(trade)

            # Verify after open
            self.assertEqual(srv.stocks_capital, 2_800_000)
            self.assertEqual(len(srv.stocks_open), 1)
            self.assertEqual(len(srv._capital_ledger), 1)
            self.assertEqual(srv._capital_ledger[-1]['event'], 'RESERVE')

            # === CLOSE: simulate trade close with profit ===
            exit_price = 205.0
            pnl = round((exit_price - entry_price) * qty, 2)  # +5000
            trade['pnl'] = pnl
            trade['pnl_pct'] = round((exit_price / entry_price - 1) * 100, 2)

            # Replicate monitor_trades ledger logic (v10.19 order)
            srv.stocks_capital += position_value
            srv.ledger_record('stocks', 'RELEASE', symbol, position_value,
                              srv.stocks_capital, trade_id)
            srv.stocks_capital += pnl
            srv.ledger_record('stocks', 'PNL_CREDIT', symbol, pnl,
                              srv.stocks_capital, trade_id)

            closed_trade = dict(trade)
            closed_trade.update({
                'exit_price': exit_price, 'status': 'CLOSED',
                'close_reason': 'TRAILING_STOP',
                'closed_at': datetime.utcnow().isoformat(),
            })
            srv.stocks_closed.insert(0, closed_trade)
            srv.stocks_open.clear()

            # Verify after close
            self.assertEqual(srv.stocks_capital, 3_005_000)  # 3M + 5K profit
            self.assertEqual(len(srv.stocks_open), 0)
            self.assertEqual(len(srv.stocks_closed), 1)
            self.assertEqual(len(srv._capital_ledger), 3)  # RESERVE + RELEASE + PNL_CREDIT

            # === RECONCILE: camada 1 (fórmula) ===
            r1 = srv._reconcile_strategy('stocks', srv.stocks_capital, 3_000_000,
                                         list(srv.stocks_open), list(srv.stocks_closed))
            self.assertTrue(r1['ok'], f'Formula recon failed: delta={r1["delta"]}')
            self.assertAlmostEqual(r1['delta'], 0, places=0)

            # === RECONCILE: camada 2 (ledger replay) ===
            r2 = srv._reconcile_via_ledger('stocks', 3_000_000, srv.stocks_capital)
            self.assertTrue(r2['ok'], f'Ledger recon failed: delta={r2["delta"]}')
            self.assertAlmostEqual(r2['delta'], 0, places=0)
            self.assertEqual(r2['ledger_events'], 3)

            # === Verify ledger order is contábil ===
            events = srv._capital_ledger
            self.assertEqual(events[0]['event'], 'RESERVE')
            self.assertEqual(events[1]['event'], 'RELEASE')
            self.assertEqual(events[2]['event'], 'PNL_CREDIT')
            # RELEASE balance should be pre-PnL (capital + position but not + pnl)
            self.assertEqual(events[1]['balance_after'], 3_000_000)  # margin returned, before PnL
            self.assertEqual(events[2]['balance_after'], 3_005_000)  # after PnL credit

        finally:
            srv.stocks_capital = orig_capital
            srv.stocks_open[:] = orig_open
            srv.stocks_closed[:] = orig_closed
            srv._capital_ledger[:] = orig_ledger


class TestIntegrationCryptoTradeCycle(unittest.TestCase):
    """[v10.20] Integration: simula ciclo completo de trade de crypto."""

    @patch.object(srv, 'enqueue_persist')
    def test_full_crypto_cycle_with_loss(self, mock_enqueue):
        """Open crypto → ledger RESERVE → close with loss → RELEASE+PNL → reconcile OK."""
        orig_capital = srv.crypto_capital
        orig_open = srv.crypto_open[:]
        orig_closed = srv.crypto_closed[:]
        orig_ledger = srv._capital_ledger[:]
        try:
            srv.crypto_capital = 1_000_000
            srv.crypto_open.clear()
            srv.crypto_closed.clear()
            srv._capital_ledger.clear()

            trade_id = 'CRY-integ-001'
            symbol = 'ETH'
            approved_size = 150_000
            entry_price = 3000.0
            qty = approved_size / entry_price  # 50

            # OPEN
            srv.crypto_capital -= approved_size
            srv.ledger_record('crypto', 'RESERVE', symbol, approved_size,
                              srv.crypto_capital, trade_id)
            trade = {
                'id': trade_id, 'symbol': symbol, 'market': 'CRYPTO',
                'asset_type': 'crypto', 'direction': 'LONG',
                'entry_price': entry_price, 'current_price': entry_price,
                'quantity': round(qty, 6), 'position_value': round(approved_size, 2),
                'pnl': 0, 'pnl_pct': 0, 'peak_pnl_pct': 0,
                'opened_at': datetime.utcnow().isoformat(), 'status': 'OPEN',
            }
            srv.crypto_open.append(trade)
            self.assertEqual(srv.crypto_capital, 850_000)

            # CLOSE with loss
            exit_price = 2940.0  # -2%
            pnl = round((exit_price - entry_price) * qty, 2)  # -3000
            trade['pnl'] = pnl

            srv.crypto_capital += approved_size
            srv.ledger_record('crypto', 'RELEASE', symbol, approved_size,
                              srv.crypto_capital, trade_id)
            srv.crypto_capital += pnl
            srv.ledger_record('crypto', 'PNL_CREDIT', symbol, pnl,
                              srv.crypto_capital, trade_id)
            srv.crypto_open.clear()
            closed_trade = dict(trade)
            closed_trade['status'] = 'CLOSED'
            closed_trade['close_reason'] = 'STOP_LOSS'
            closed_trade['closed_at'] = datetime.utcnow().isoformat()
            srv.crypto_closed.insert(0, closed_trade)

            # Verify capital
            self.assertEqual(srv.crypto_capital, 1_000_000 + pnl)  # 997_000

            # Reconcile camada 1
            r1 = srv._reconcile_strategy('crypto', srv.crypto_capital, 1_000_000,
                                         list(srv.crypto_open), list(srv.crypto_closed))
            self.assertTrue(r1['ok'])
            self.assertAlmostEqual(r1['delta'], 0, places=0)

            # Reconcile camada 2
            r2 = srv._reconcile_via_ledger('crypto', 1_000_000, srv.crypto_capital)
            self.assertTrue(r2['ok'])
            self.assertAlmostEqual(r2['delta'], 0, places=0)
        finally:
            srv.crypto_capital = orig_capital
            srv.crypto_open[:] = orig_open
            srv.crypto_closed[:] = orig_closed
            srv._capital_ledger[:] = orig_ledger


class TestIntegrationArbiTradeCycle(unittest.TestCase):
    """[v10.20] Integration: simula ciclo completo de trade de arbitragem."""

    @patch.object(srv, 'enqueue_persist')
    def test_full_arbi_cycle(self, mock_enqueue):
        """Open arbi → ledger RESERVE → close → RELEASE+PNL → reconcile formula + ledger OK."""
        orig_capital = srv.arbi_capital
        orig_open = srv.arbi_open[:]
        orig_closed = srv.arbi_closed[:]
        orig_ledger = srv._capital_ledger[:]
        try:
            srv.arbi_capital = 4_500_000
            srv.arbi_open.clear()
            srv.arbi_closed.clear()
            srv._capital_ledger.clear()

            trade_id = 'ARB-integ-001'
            name = 'VALE3-VALE'
            pos = 100_000

            # OPEN
            srv.arbi_capital -= pos
            srv.ledger_record('arbi', 'RESERVE', name, pos, srv.arbi_capital, trade_id)
            trade = {
                'id': trade_id, 'pair_id': 'VALE3-VALE', 'name': name,
                'position_size': pos, 'pnl': 0, 'pnl_pct': 0,
                'opened_at': datetime.utcnow().isoformat(), 'status': 'OPEN',
            }
            srv.arbi_open.append(trade)
            self.assertEqual(srv.arbi_capital, 4_400_000)

            # CLOSE with profit
            pnl = 2_500
            trade['pnl'] = pnl
            srv.arbi_capital += pos
            srv.ledger_record('arbi', 'RELEASE', name, pos, srv.arbi_capital, trade_id)
            srv.arbi_capital += pnl
            srv.ledger_record('arbi', 'PNL_CREDIT', name, pnl, srv.arbi_capital, trade_id)
            srv.arbi_open.clear()
            closed_trade = dict(trade)
            closed_trade['status'] = 'CLOSED'
            closed_trade['close_reason'] = 'TARGET'
            closed_trade['closed_at'] = datetime.utcnow().isoformat()
            srv.arbi_closed.insert(0, closed_trade)

            self.assertEqual(srv.arbi_capital, 4_502_500)

            # Reconcile formula (arbi-specific)
            r1 = srv._reconcile_strategy_arbi(srv.arbi_capital, 4_500_000,
                                              list(srv.arbi_open), list(srv.arbi_closed))
            self.assertTrue(r1['ok'])
            self.assertAlmostEqual(r1['delta'], 0, places=0)
            self.assertEqual(r1['strategy'], 'arbi')

            # Reconcile ledger
            r2 = srv._reconcile_via_ledger('arbi', 4_500_000, srv.arbi_capital)
            self.assertTrue(r2['ok'])
            self.assertAlmostEqual(r2['delta'], 0, places=0)
            self.assertEqual(r2['ledger_events'], 3)
        finally:
            srv.arbi_capital = orig_capital
            srv.arbi_open[:] = orig_open
            srv.arbi_closed[:] = orig_closed
            srv._capital_ledger[:] = orig_ledger


class TestIntegrationLedgerFromDB(unittest.TestCase):
    """[v10.20] Integration: reconciliação via ledger carrega do MySQL quando memória vazia."""

    @patch.object(srv, 'enqueue_persist')
    @patch.object(srv, 'get_db')
    def test_ledger_falls_back_to_mysql(self, mock_get_db, mock_enqueue):
        """Com ledger vazio em memória, _reconcile_via_ledger busca do MySQL."""
        orig_ledger = srv._capital_ledger[:]
        try:
            srv._capital_ledger.clear()

            # Mock DB: retorna 3 eventos (simula pós-deploy)
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [
                {'event': 'RESERVE', 'amount': 200_000},
                {'event': 'RELEASE', 'amount': 200_000},
                {'event': 'PNL_CREDIT', 'amount': 8_000},
            ]
            mock_conn.cursor.return_value = mock_cursor
            mock_get_db.return_value = mock_conn

            initial = 9_000_000
            # Expected from DB: 9M - 200K + 200K + 8K = 9_008_000
            memory = 9_008_000
            r = srv._reconcile_via_ledger('stocks', initial, memory)

            self.assertTrue(r['ok'])
            self.assertAlmostEqual(r['delta'], 0, places=0)
            self.assertEqual(r['ledger_events'], 3)
            self.assertEqual(r['source'], 'mysql')  # loaded from DB, not memory
        finally:
            srv._capital_ledger[:] = orig_ledger


if __name__ == '__main__':
    unittest.main()
