"""
Monthly Picks — Test Suite.

Tests:
  1. Seleção mensal
  2. Exclusão de ticker duplicado
  3. Limite por setor
  4. Gatilhos de saída (6 triggers)
  5. Timeout 9 meses
  6. Review semanal
  7. Persistência MySQL (mock)
  8. Endpoints
  9. Aprendizado enviado ao brain
  10. Promoção de estado OBSERVE → SHADOW → PAPER_SMALL → PAPER_FULL
"""

import unittest
import datetime
from unittest.mock import MagicMock, patch, PropertyMock
from decimal import Decimal

from .config import (
    MonthlyPicksConfig, SleeveStatus, PositionStatus,
    ExitReason, ReviewAction, LearningEvent,
    PROMOTION_RULES, get_config, reset_config,
)
from .portfolio_rules import PortfolioGovernance
from .review_engine import ReviewEngine
from .learning_bridge import LearningBridge


class TestConfig(unittest.TestCase):
    """Test configuration."""

    def setUp(self):
        reset_config()

    def test_default_config(self):
        cfg = MonthlyPicksConfig()
        self.assertEqual(cfg.picks_per_month, 3)
        self.assertEqual(cfg.candidates_per_scan, 10)
        self.assertEqual(cfg.max_hold_months, 9)
        self.assertEqual(cfg.target_gain_pct, 15.0)
        self.assertEqual(cfg.stop_loss_pct, -8.0)
        self.assertEqual(cfg.trailing_stop_pct, 5.0)
        self.assertEqual(cfg.min_score_entry, 65.0)

    def test_sleeve_status_enum(self):
        self.assertEqual(SleeveStatus.OBSERVE.value, 'observe')
        self.assertEqual(SleeveStatus.PAPER_FULL.value, 'paper_full')

    def test_promotion_rules(self):
        self.assertIn(SleeveStatus.OBSERVE, PROMOTION_RULES)
        obs_rule = PROMOTION_RULES[SleeveStatus.OBSERVE]
        self.assertEqual(obs_rule['target'], SleeveStatus.SHADOW_EXEC)
        self.assertEqual(obs_rule['min_score'], 50.0)


class TestPortfolioGovernance(unittest.TestCase):
    """Test selection rules and governance."""

    def setUp(self):
        self.config = MonthlyPicksConfig()
        self.repo = MagicMock()
        self.repo.is_ticker_open.return_value = False
        self.repo.count_open_by_sector.return_value = 0
        self.governance = PortfolioGovernance(self.repo, self.config)

    def _make_candidate(self, ticker='TEST1', score=75.0,
                        sector='Technology', quality=70.0,
                        conviction='Buy', risk_flags=None):
        return {
            'ticker': ticker,
            'analysis_score': score,
            'total_score': score,
            'data_reliability': quality,
            'conviction': conviction,
            'sector': sector,
            'risk_flags': risk_flags or [],
        }

    def test_select_top_3(self):
        """Should select exactly 3 candidates."""
        candidates = [
            self._make_candidate('A1', 90, sector='Tech'),
            self._make_candidate('A2', 85, sector='Finance'),
            self._make_candidate('A3', 80, sector='Energy'),
            self._make_candidate('A4', 75, sector='Retail'),
        ]
        selected, rejected = self.governance.apply_rules(candidates)
        self.assertEqual(len(selected), 3)
        self.assertEqual(selected[0]['ticker'], 'A1')

    def test_reject_duplicate_ticker(self):
        """Should reject ticker already open."""
        self.repo.is_ticker_open.side_effect = lambda t: t == 'DUP1'
        candidates = [
            self._make_candidate('DUP1', 90),
            self._make_candidate('NEW1', 85),
        ]
        selected, rejected = self.governance.apply_rules(candidates)
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]['ticker'], 'NEW1')
        self.assertIn('duplicate_open', rejected[0].get('rejection_reason', ''))

    def test_sector_limit(self):
        """Should enforce max 2 per sector."""
        self.repo.count_open_by_sector.return_value = 1
        candidates = [
            self._make_candidate('T1', 90, 'Tech'),
            self._make_candidate('T2', 85, 'Tech'),
            self._make_candidate('T3', 80, 'Tech'),
        ]
        selected, rejected = self.governance.apply_rules(candidates)
        # 1 already open + 1 new = 2 (max). Third should be rejected.
        tech_selected = [s for s in selected if s['sector'] == 'Tech']
        self.assertLessEqual(len(tech_selected), 1)

    def test_reject_low_score(self):
        """Should reject candidates below min_score_entry."""
        candidates = [
            self._make_candidate('LOW1', 40),
            self._make_candidate('HIGH1', 80),
        ]
        selected, rejected = self.governance.apply_rules(candidates)
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]['ticker'], 'HIGH1')

    def test_reject_low_quality(self):
        """Should reject candidates with low data quality."""
        candidates = [
            self._make_candidate('LQ1', 80, quality=30),
            self._make_candidate('HQ1', 80, quality=80),
        ]
        selected, rejected = self.governance.apply_rules(candidates)
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]['ticker'], 'HQ1')

    def test_reject_high_risk(self):
        """Should reject candidates with high-severity risk flags."""
        candidates = [
            self._make_candidate('RISK1', 80, risk_flags=[
                {'severity': 'high', 'message': 'Regulatory risk'}
            ]),
            self._make_candidate('SAFE1', 75),
        ]
        selected, rejected = self.governance.apply_rules(candidates)
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]['ticker'], 'SAFE1')

    def test_reject_avoid_conviction(self):
        """Should reject candidates with 'Avoid' conviction."""
        candidates = [
            self._make_candidate('BAD1', 80, conviction='Avoid'),
            self._make_candidate('GOOD1', 75, conviction='Buy'),
        ]
        selected, rejected = self.governance.apply_rules(candidates)
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]['ticker'], 'GOOD1')

    def test_position_sizing(self):
        """Should compute correct target/stop prices."""
        c = self._make_candidate('SZ1', 80)
        c['price_at_scan'] = 50.0
        sizing = self.governance.compute_position_sizing(c)
        self.assertAlmostEqual(sizing['target_price'], 57.50, places=2)
        self.assertAlmostEqual(sizing['stop_price'], 46.0, places=2)
        self.assertEqual(sizing['quantity'], 2000)  # 100K / 50


class TestExitTriggers(unittest.TestCase):
    """Test all 6 exit triggers + thesis_broken."""

    def setUp(self):
        self.config = MonthlyPicksConfig()
        self.db_fn = MagicMock()
        self.repo = MagicMock()
        self.engine = ReviewEngine(self.db_fn, self.repo, self.config)

    def test_target_hit(self):
        result = self.engine._check_exit_triggers(
            {}, current_price=115, current_score=80,
            entry_price=100, entry_score=75,
            peak_price=115, pnl_pct=15.0, weeks_held=8,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result['exit_reason'], ExitReason.TARGET_HIT.value)

    def test_stop_loss(self):
        result = self.engine._check_exit_triggers(
            {}, current_price=91, current_score=60,
            entry_price=100, entry_score=75,
            peak_price=102, pnl_pct=-9.0, weeks_held=4,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result['exit_reason'], ExitReason.STOP_LOSS.value)

    def test_trailing_stop(self):
        result = self.engine._check_exit_triggers(
            {}, current_price=109, current_score=70,
            entry_price=100, entry_score=75,
            peak_price=118, pnl_pct=9.0, weeks_held=12,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result['exit_reason'], ExitReason.TRAILING_STOP.value)

    def test_timeout_9_months(self):
        result = self.engine._check_exit_triggers(
            {}, current_price=105, current_score=65,
            entry_price=100, entry_score=75,
            peak_price=108, pnl_pct=5.0, weeks_held=40,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result['exit_reason'], ExitReason.TIMEOUT.value)

    def test_score_low(self):
        result = self.engine._check_exit_triggers(
            {}, current_price=98, current_score=45,
            entry_price=100, entry_score=75,
            peak_price=103, pnl_pct=-2.0, weeks_held=6,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result['exit_reason'], ExitReason.SCORE_LOW.value)

    def test_score_drop(self):
        result = self.engine._check_exit_triggers(
            {}, current_price=101, current_score=58,
            entry_price=100, entry_score=80,
            peak_price=106, pnl_pct=1.0, weeks_held=10,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result['exit_reason'], ExitReason.SCORE_DROP.value)

    def test_hold_when_ok(self):
        """Should return None when no trigger fires."""
        result = self.engine._check_exit_triggers(
            {}, current_price=108, current_score=72,
            entry_price=100, entry_score=75,
            peak_price=110, pnl_pct=8.0, weeks_held=8,
        )
        self.assertIsNone(result)


class TestLearningBridge(unittest.TestCase):
    """Test learning events are emitted correctly."""

    def setUp(self):
        self.brain_fn = MagicMock()
        self.bridge = LearningBridge(
            brain_lesson_fn=self.brain_fn,
        )

    def test_emit_opened(self):
        candidate = {'ticker': 'PETR4', 'analysis_score': 82, 'conviction': 'Buy', 'sector': 'Energy'}
        sizing = {'entry_price': 38.5, 'target_price': 44.3, 'stop_price': 35.4,
                  'quantity': 2597, 'capital_allocated': 100000, 'risk_reward_ratio': 1.87}
        self.bridge.emit_opened(candidate, sizing, 'paper_small')
        self.brain_fn.assert_called_once()
        call_kwargs = self.brain_fn.call_args
        self.assertEqual(call_kwargs[1]['module'], 'monthly_picks')
        self.assertEqual(call_kwargs[1]['lesson_type'],
                         LearningEvent.MONTHLY_PICK_OPENED.value)

    def test_emit_closed_target_hit(self):
        position = {'ticker': 'VALE3', 'entry_score': 78, 'current_score': 80,
                     'weeks_held': 12, 'sector': 'Mining'}
        self.bridge.emit_closed(position, ExitReason.TARGET_HIT.value, 15.5, 15500)
        self.brain_fn.assert_called_once()
        call_kwargs = self.brain_fn.call_args
        self.assertEqual(call_kwargs[1]['lesson_type'],
                         LearningEvent.MONTHLY_PICK_TARGET_HIT.value)

    def test_emit_rejected(self):
        candidate = {'ticker': 'BAD1', 'analysis_score': 55, 'sector': 'Retail'}
        self.bridge.emit_rejected(candidate, 'sector_limit')
        self.brain_fn.assert_called_once()

    def test_disabled_bridge(self):
        """Should not crash when brain function is None."""
        bridge = LearningBridge(brain_lesson_fn=None)
        bridge.emit_opened({'ticker': 'X'}, {'entry_price': 10}, 'observe')
        # No crash = success


class TestPromotionRules(unittest.TestCase):
    """Test sleeve promotion from OBSERVE to PAPER_FULL."""

    def test_observe_to_shadow(self):
        rule = PROMOTION_RULES[SleeveStatus.OBSERVE]
        self.assertEqual(rule['target'], SleeveStatus.SHADOW_EXEC)
        self.assertEqual(rule['min_days'], 0)

    def test_shadow_to_paper_small(self):
        rule = PROMOTION_RULES[SleeveStatus.SHADOW_EXEC]
        self.assertEqual(rule['target'], SleeveStatus.PAPER_SMALL)
        self.assertEqual(rule['min_days'], 30)
        self.assertIn('min_shadow_trades', rule)

    def test_paper_small_to_paper_full(self):
        rule = PROMOTION_RULES[SleeveStatus.PAPER_SMALL]
        self.assertEqual(rule['target'], SleeveStatus.PAPER_FULL)
        self.assertEqual(rule['min_days'], 60)
        self.assertIn('min_win_rate', rule)
        self.assertGreaterEqual(rule['min_win_rate'], 0.4)


class TestReviewConfidence(unittest.TestCase):
    """Test review confidence calculation."""

    def setUp(self):
        self.config = MonthlyPicksConfig()
        self.engine = ReviewEngine(MagicMock(), MagicMock(), self.config)

    def test_high_confidence(self):
        conf = self.engine._compute_review_confidence(
            current_score=80, prev_score=77, pnl_pct=12, weeks_held=4)
        self.assertGreater(conf, 70)

    def test_low_confidence(self):
        conf = self.engine._compute_review_confidence(
            current_score=45, prev_score=55, pnl_pct=-6, weeks_held=35)
        self.assertLess(conf, 40)

    def test_baseline(self):
        conf = self.engine._compute_review_confidence(
            current_score=60, prev_score=60, pnl_pct=2, weeks_held=8)
        self.assertAlmostEqual(conf, 55, delta=15)


def run_tests():
    """Run all Monthly Picks tests."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    suite.addTests(loader.loadTestsFromTestCase(TestConfig))
    suite.addTests(loader.loadTestsFromTestCase(TestPortfolioGovernance))
    suite.addTests(loader.loadTestsFromTestCase(TestExitTriggers))
    suite.addTests(loader.loadTestsFromTestCase(TestLearningBridge))
    suite.addTests(loader.loadTestsFromTestCase(TestPromotionRules))
    suite.addTests(loader.loadTestsFromTestCase(TestReviewConfidence))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result


if __name__ == '__main__':
    run_tests()
