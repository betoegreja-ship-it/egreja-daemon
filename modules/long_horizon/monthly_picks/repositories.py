"""
Monthly Picks — Database Schema & Repository Layer.

8 tables with clear separation:
  1. mp_config           – global config key/value
  2. mp_scan_runs        – one row per monthly scan execution
  3. mp_candidates       – top 10 analyzed each month
  4. mp_positions        – chosen positions + full lifecycle
  5. mp_reviews          – weekly review records
  6. mp_actions          – buy / hold / reduce / close actions log
  7. mp_performance      – aggregated metrics by monthly cohort
  8. mp_model_versions   – score/thesis/rule versions used
"""

import json
import logging
import datetime
from typing import Any, Optional, List, Dict
from decimal import Decimal

logger = logging.getLogger('egreja.monthly_picks.repositories')


# ──────────────────────────────────────────────────────────────
# SCHEMA CREATION
# ──────────────────────────────────────────────────────────────

def create_monthly_picks_tables(connection: Any) -> None:
    """Create all 8 Monthly Picks tables. Idempotent."""
    cursor = connection.cursor()
    try:

        # 1. mp_config — global key/value config
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS mp_config (
            config_key    VARCHAR(64) PRIMARY KEY,
            config_value  TEXT,
            updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 2. mp_scan_runs — one row per monthly scan execution
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS mp_scan_runs (
            run_id          BIGINT AUTO_INCREMENT PRIMARY KEY,
            scan_month      VARCHAR(7) NOT NULL,
            scan_date       DATETIME NOT NULL,
            sleeve_status   VARCHAR(32) NOT NULL,
            universe_size   INT,
            candidates_found INT,
            picks_made      INT,
            model_version   VARCHAR(32),
            rule_set        VARCHAR(32),
            duration_sec    DECIMAL(8,2),
            notes           TEXT,
            created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_scan_month (scan_month),
            INDEX idx_scan_date (scan_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 3. mp_candidates — top 10 analyzed per scan
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS mp_candidates (
            candidate_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
            run_id            BIGINT NOT NULL,
            ticker            VARCHAR(16) NOT NULL,
            rank_position     INT NOT NULL,
            total_score       DECIMAL(5,2),
            business_quality  DECIMAL(5,2),
            valuation         DECIMAL(5,2),
            market_strength   DECIMAL(5,2),
            macro_factors     DECIMAL(5,2),
            options_signal    DECIMAL(5,2),
            structural_risk   DECIMAL(5,2),
            data_reliability  DECIMAL(5,2),
            conviction        VARCHAR(32),
            sector            VARCHAR(64),
            thesis_summary    TEXT,
            deep_analysis     JSON,
            price_at_scan     DECIMAL(12,4),
            liquidity_score   DECIMAL(5,2),
            data_quality      DECIMAL(5,2),
            selected          BOOLEAN DEFAULT FALSE,
            rejection_reason  VARCHAR(255),
            created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (run_id) REFERENCES mp_scan_runs(run_id),
            INDEX idx_run_id (run_id),
            INDEX idx_ticker (ticker),
            INDEX idx_rank (rank_position),
            INDEX idx_selected (selected)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 4. mp_positions — chosen picks + full lifecycle
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS mp_positions (
            position_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
            candidate_id    BIGINT,
            run_id          BIGINT NOT NULL,
            pick_month      VARCHAR(7) NOT NULL,
            ticker          VARCHAR(16) NOT NULL,
            sector          VARCHAR(64),
            entry_date      DATE NOT NULL,
            entry_price     DECIMAL(12,4) NOT NULL,
            entry_score     DECIMAL(5,2),
            entry_conviction VARCHAR(32),
            target_price    DECIMAL(12,4),
            stop_price      DECIMAL(12,4),
            current_price   DECIMAL(12,4),
            peak_price      DECIMAL(12,4),
            current_score   DECIMAL(5,2),
            pnl_pct         DECIMAL(8,4) DEFAULT 0,
            pnl_value       DECIMAL(14,2) DEFAULT 0,
            capital_allocated DECIMAL(16,2),
            quantity         DECIMAL(18,4),
            status          VARCHAR(32) DEFAULT 'open',
            sleeve_status   VARCHAR(32),
            close_date      DATE,
            close_price     DECIMAL(12,4),
            close_reason    VARCHAR(64),
            thesis_broken   BOOLEAN DEFAULT FALSE,
            human_override  BOOLEAN DEFAULT FALSE,
            max_gain_pct    DECIMAL(8,4) DEFAULT 0,
            max_loss_pct    DECIMAL(8,4) DEFAULT 0,
            weeks_held      INT DEFAULT 0,
            last_review     DATE,
            review_confidence DECIMAL(5,2),
            created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (run_id) REFERENCES mp_scan_runs(run_id),
            INDEX idx_run_id (run_id),
            INDEX idx_ticker (ticker),
            INDEX idx_pick_month (pick_month),
            INDEX idx_status (status),
            INDEX idx_sector (sector),
            INDEX idx_close_reason (close_reason)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 5. mp_reviews — weekly review records
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS mp_reviews (
            review_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
            position_id     BIGINT NOT NULL,
            review_date     DATE NOT NULL,
            ticker          VARCHAR(16) NOT NULL,
            current_price   DECIMAL(12,4),
            current_score   DECIMAL(5,2),
            prev_score      DECIMAL(5,2),
            score_change    DECIMAL(5,2),
            pnl_pct         DECIMAL(8,4),
            weeks_held      INT,
            action          VARCHAR(32) NOT NULL,
            reason          TEXT,
            market_regime   VARCHAR(32),
            risk_flags      JSON,
            review_confidence DECIMAL(5,2),
            created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (position_id) REFERENCES mp_positions(position_id),
            INDEX idx_position_id (position_id),
            INDEX idx_review_date (review_date),
            INDEX idx_ticker (ticker),
            INDEX idx_action (action)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 6. mp_actions — every buy/hold/reduce/close action
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS mp_actions (
            action_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
            position_id     BIGINT,
            action_type     VARCHAR(32) NOT NULL,
            ticker          VARCHAR(16) NOT NULL,
            quantity         DECIMAL(18,4),
            price           DECIMAL(12,4),
            action_date     DATETIME NOT NULL,
            trigger_type    VARCHAR(64),
            trigger_detail  TEXT,
            sleeve_status   VARCHAR(32),
            pnl_realized    DECIMAL(14,2),
            created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_position_id (position_id),
            INDEX idx_action_type (action_type),
            INDEX idx_ticker (ticker),
            INDEX idx_action_date (action_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 7. mp_performance — aggregated metrics by monthly cohort
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS mp_performance (
            perf_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
            cohort_month    VARCHAR(7) NOT NULL,
            total_picks     INT,
            open_picks      INT,
            closed_picks    INT,
            win_count       INT,
            loss_count      INT,
            win_rate        DECIMAL(5,4),
            total_return_pct DECIMAL(8,4),
            avg_return_pct  DECIMAL(8,4),
            avg_hold_days   DECIMAL(8,2),
            exits_by_target INT DEFAULT 0,
            exits_by_stop   INT DEFAULT 0,
            exits_by_trailing INT DEFAULT 0,
            exits_by_timeout INT DEFAULT 0,
            exits_by_thesis INT DEFAULT 0,
            exits_by_score  INT DEFAULT 0,
            exits_by_human  INT DEFAULT 0,
            avg_entry_score DECIMAL(5,2),
            avg_exit_score  DECIMAL(5,2),
            max_drawdown_pct DECIMAL(8,4),
            sector_breakdown JSON,
            edge_stability  DECIMAL(5,4),
            computed_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_cohort (cohort_month),
            INDEX idx_cohort_month (cohort_month)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 8. mp_model_versions — versions of score/thesis/rules used
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS mp_model_versions (
            version_id      INT AUTO_INCREMENT PRIMARY KEY,
            version_tag     VARCHAR(32) NOT NULL,
            score_version   VARCHAR(32),
            thesis_version  VARCHAR(32),
            rule_set        VARCHAR(32),
            config_snapshot JSON,
            created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
            notes           TEXT,
            UNIQUE KEY uk_version_tag (version_tag),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        connection.commit()
        logger.info('[MP] All 8 Monthly Picks tables created/verified')

    except Exception as e:
        connection.rollback()
        logger.error(f'[MP] Error creating tables: {e}')
        raise
    finally:
        cursor.close()


# ──────────────────────────────────────────────────────────────
# REPOSITORY CLASS
# ──────────────────────────────────────────────────────────────

def _serialize(val):
    """Convert Decimal/datetime to JSON-safe types."""
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.isoformat()
    return val


def _serialize_row(row: dict) -> dict:
    if not row:
        return row
    return {k: _serialize(v) for k, v in row.items()}


def _serialize_rows(rows: list) -> list:
    return [_serialize_row(r) for r in rows]


class MonthlyPicksRepository:
    """
    Data access layer for the Monthly Picks sleeve.
    All DB operations go through here — no raw SQL in business logic.
    """

    def __init__(self, db_fn, log=None):
        self.db_fn = db_fn
        self.log = log or logger

    def _conn(self):
        return self.db_fn()

    def _query(self, sql: str, params=None, fetch='all') -> Any:
        conn = None
        try:
            conn = self._conn()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql, params or ())
            if fetch == 'one':
                result = cursor.fetchone()
                return _serialize_row(result) if result else None
            rows = cursor.fetchall()
            return _serialize_rows(rows)
        except Exception as e:
            self.log.warning(f'[MP] Query error: {e}')
            return None if fetch == 'one' else []
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _execute(self, sql: str, params=None) -> Optional[int]:
        conn = None
        try:
            conn = self._conn()
            cursor = conn.cursor()
            cursor.execute(sql, params or ())
            conn.commit()
            return cursor.lastrowid
        except Exception as e:
            if conn:
                conn.rollback()
            self.log.error(f'[MP] Execute error: {e}')
            return None
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # ── Config ─────────────────────────────────────────────

    def get_config(self, key: str, default=None) -> Optional[str]:
        row = self._query(
            "SELECT config_value FROM mp_config WHERE config_key = %s",
            (key,), fetch='one'
        )
        return row['config_value'] if row else default

    def set_config(self, key: str, value: str):
        self._execute(
            "REPLACE INTO mp_config (config_key, config_value) VALUES (%s, %s)",
            (key, str(value))
        )

    def get_sleeve_status(self) -> str:
        return self.get_config('sleeve_status', 'observe')

    def set_sleeve_status(self, status: str):
        self.set_config('sleeve_status', status)

    # ── Scan Runs ──────────────────────────────────────────

    def insert_scan_run(self, scan_month: str, sleeve_status: str,
                        universe_size: int, model_version: str = None,
                        rule_set: str = None) -> Optional[int]:
        return self._execute(
            """INSERT INTO mp_scan_runs
               (scan_month, scan_date, sleeve_status, universe_size,
                model_version, rule_set)
               VALUES (%s, NOW(), %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE scan_date=NOW(), universe_size=%s""",
            (scan_month, sleeve_status, universe_size, model_version,
             rule_set, universe_size)
        )

    def update_scan_run(self, run_id: int, candidates_found: int,
                        picks_made: int, duration_sec: float,
                        notes: str = None):
        self._execute(
            """UPDATE mp_scan_runs
               SET candidates_found=%s, picks_made=%s, duration_sec=%s, notes=%s
               WHERE run_id=%s""",
            (candidates_found, picks_made, round(duration_sec, 2), notes, run_id)
        )

    def get_scan_run(self, scan_month: str) -> Optional[Dict]:
        return self._query(
            "SELECT * FROM mp_scan_runs WHERE scan_month = %s",
            (scan_month,), fetch='one'
        )

    def get_scan_runs(self, limit: int = 12) -> List[Dict]:
        return self._query(
            "SELECT * FROM mp_scan_runs ORDER BY scan_date DESC LIMIT %s",
            (limit,)
        )

    # ── Candidates ─────────────────────────────────────────

    def insert_candidate(self, run_id: int, ticker: str, rank: int,
                         scores: dict, thesis_summary: str = None,
                         deep_analysis: dict = None,
                         price: float = None, sector: str = None,
                         liquidity: float = None, quality: float = None) -> Optional[int]:
        return self._execute(
            """INSERT INTO mp_candidates
               (run_id, ticker, rank_position, total_score,
                business_quality, valuation, market_strength, macro_factors,
                options_signal, structural_risk, data_reliability,
                conviction, sector, thesis_summary, deep_analysis,
                price_at_scan, liquidity_score, data_quality)
               VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s,%s)""",
            (run_id, ticker, rank,
             scores.get('total_score'),
             scores.get('business_quality'),
             scores.get('valuation'),
             scores.get('market_strength'),
             scores.get('macro_factors'),
             scores.get('options_signal'),
             scores.get('structural_risk'),
             scores.get('data_reliability'),
             scores.get('conviction'),
             sector, thesis_summary,
             json.dumps(deep_analysis) if deep_analysis else None,
             price, liquidity, quality)
        )

    def mark_candidate_selected(self, candidate_id: int):
        self._execute(
            "UPDATE mp_candidates SET selected=TRUE WHERE candidate_id=%s",
            (candidate_id,)
        )

    def mark_candidate_rejected(self, candidate_id: int, reason: str):
        self._execute(
            "UPDATE mp_candidates SET selected=FALSE, rejection_reason=%s WHERE candidate_id=%s",
            (reason, candidate_id)
        )

    def get_candidates(self, run_id: int) -> List[Dict]:
        return self._query(
            """SELECT * FROM mp_candidates
               WHERE run_id=%s ORDER BY rank_position""",
            (run_id,)
        )

    # ── Positions ──────────────────────────────────────────

    def insert_position(self, candidate_id: int, run_id: int,
                        pick_month: str, ticker: str, sector: str,
                        entry_price: float, entry_score: float,
                        entry_conviction: str, target_price: float,
                        stop_price: float, capital: float,
                        quantity: float, sleeve_status: str) -> Optional[int]:
        return self._execute(
            """INSERT INTO mp_positions
               (candidate_id, run_id, pick_month, ticker, sector,
                entry_date, entry_price, entry_score, entry_conviction,
                target_price, stop_price, current_price, peak_price,
                current_score, capital_allocated, quantity,
                status, sleeve_status)
               VALUES (%s,%s,%s,%s,%s, CURDATE(),%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s)""",
            (candidate_id, run_id, pick_month, ticker, sector,
             entry_price, entry_score, entry_conviction,
             target_price, stop_price, entry_price, entry_price,
             entry_score, capital, quantity,
             'shadow' if sleeve_status in ('observe', 'shadow_exec') else 'open',
             sleeve_status)
        )

    def get_open_positions(self) -> List[Dict]:
        return self._query(
            """SELECT * FROM mp_positions
               WHERE status IN ('open', 'shadow', 'reduced')
               ORDER BY entry_date""")

    def get_position(self, position_id: int) -> Optional[Dict]:
        return self._query(
            "SELECT * FROM mp_positions WHERE position_id=%s",
            (position_id,), fetch='one'
        )

    def get_positions_by_month(self, pick_month: str) -> List[Dict]:
        return self._query(
            "SELECT * FROM mp_positions WHERE pick_month=%s ORDER BY entry_date",
            (pick_month,)
        )

    def get_closed_positions(self, limit: int = 50) -> List[Dict]:
        return self._query(
            """SELECT * FROM mp_positions
               WHERE status='closed'
               ORDER BY close_date DESC LIMIT %s""",
            (limit,)
        )

    def update_position_price(self, position_id: int, current_price: float,
                              current_score: float, pnl_pct: float,
                              pnl_value: float, peak_price: float,
                              max_gain: float, max_loss: float,
                              weeks_held: int):
        self._execute(
            """UPDATE mp_positions SET
               current_price=%s, current_score=%s, pnl_pct=%s, pnl_value=%s,
               peak_price=%s, max_gain_pct=%s, max_loss_pct=%s, weeks_held=%s,
               updated_at=NOW()
               WHERE position_id=%s""",
            (current_price, current_score, pnl_pct, pnl_value,
             peak_price, max_gain, max_loss, weeks_held, position_id)
        )

    def close_position(self, position_id: int, close_price: float,
                       close_reason: str, pnl_pct: float, pnl_value: float,
                       thesis_broken: bool = False, human_override: bool = False):
        self._execute(
            """UPDATE mp_positions SET
               status='closed', close_date=CURDATE(), close_price=%s,
               close_reason=%s, pnl_pct=%s, pnl_value=%s,
               thesis_broken=%s, human_override=%s, current_price=%s,
               updated_at=NOW()
               WHERE position_id=%s""",
            (close_price, close_reason, pnl_pct, pnl_value,
             thesis_broken, human_override, close_price, position_id)
        )

    def update_review_info(self, position_id: int, review_date,
                           review_confidence: float):
        self._execute(
            """UPDATE mp_positions SET last_review=%s, review_confidence=%s
               WHERE position_id=%s""",
            (review_date, review_confidence, position_id)
        )

    def is_ticker_open(self, ticker: str) -> bool:
        row = self._query(
            """SELECT COUNT(*) as cnt FROM mp_positions
               WHERE ticker=%s AND status IN ('open','shadow','reduced')""",
            (ticker,), fetch='one'
        )
        return (row or {}).get('cnt', 0) > 0

    def count_open_by_sector(self, sector: str) -> int:
        row = self._query(
            """SELECT COUNT(*) as cnt FROM mp_positions
               WHERE sector=%s AND status IN ('open','shadow','reduced')""",
            (sector,), fetch='one'
        )
        return (row or {}).get('cnt', 0)

    # ── Reviews ────────────────────────────────────────────

    def insert_review(self, position_id: int, ticker: str,
                      current_price: float, current_score: float,
                      prev_score: float, pnl_pct: float,
                      weeks_held: int, action: str, reason: str,
                      market_regime: str = None, risk_flags: dict = None,
                      confidence: float = None) -> Optional[int]:
        return self._execute(
            """INSERT INTO mp_reviews
               (position_id, review_date, ticker, current_price,
                current_score, prev_score, score_change, pnl_pct,
                weeks_held, action, reason, market_regime, risk_flags,
                review_confidence)
               VALUES (%s, CURDATE(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (position_id, ticker, current_price, current_score,
             prev_score, round(current_score - prev_score, 2) if prev_score else 0,
             pnl_pct, weeks_held, action, reason, market_regime,
             json.dumps(risk_flags) if risk_flags else None, confidence)
        )

    def get_reviews_for_position(self, position_id: int) -> List[Dict]:
        return self._query(
            "SELECT * FROM mp_reviews WHERE position_id=%s ORDER BY review_date",
            (position_id,)
        )

    def get_recent_reviews(self, limit: int = 30) -> List[Dict]:
        return self._query(
            "SELECT * FROM mp_reviews ORDER BY review_date DESC LIMIT %s",
            (limit,)
        )

    # ── Actions ────────────────────────────────────────────

    def insert_action(self, position_id: int, action_type: str,
                      ticker: str, quantity: float = None,
                      price: float = None, trigger_type: str = None,
                      trigger_detail: str = None, sleeve_status: str = None,
                      pnl_realized: float = None) -> Optional[int]:
        return self._execute(
            """INSERT INTO mp_actions
               (position_id, action_type, ticker, quantity, price,
                action_date, trigger_type, trigger_detail,
                sleeve_status, pnl_realized)
               VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s)""",
            (position_id, action_type, ticker, quantity, price,
             trigger_type, trigger_detail, sleeve_status, pnl_realized)
        )

    def get_actions(self, position_id: int = None, limit: int = 50) -> List[Dict]:
        if position_id:
            return self._query(
                "SELECT * FROM mp_actions WHERE position_id=%s ORDER BY action_date DESC",
                (position_id,)
            )
        return self._query(
            "SELECT * FROM mp_actions ORDER BY action_date DESC LIMIT %s",
            (limit,)
        )

    # ── Performance ────────────────────────────────────────

    def upsert_performance(self, cohort_month: str, metrics: dict):
        self._execute(
            """REPLACE INTO mp_performance
               (cohort_month, total_picks, open_picks, closed_picks,
                win_count, loss_count, win_rate, total_return_pct,
                avg_return_pct, avg_hold_days,
                exits_by_target, exits_by_stop, exits_by_trailing,
                exits_by_timeout, exits_by_thesis, exits_by_score,
                exits_by_human, avg_entry_score, avg_exit_score,
                max_drawdown_pct, sector_breakdown, edge_stability)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (cohort_month,
             metrics.get('total_picks', 0),
             metrics.get('open_picks', 0),
             metrics.get('closed_picks', 0),
             metrics.get('win_count', 0),
             metrics.get('loss_count', 0),
             metrics.get('win_rate'),
             metrics.get('total_return_pct'),
             metrics.get('avg_return_pct'),
             metrics.get('avg_hold_days'),
             metrics.get('exits_by_target', 0),
             metrics.get('exits_by_stop', 0),
             metrics.get('exits_by_trailing', 0),
             metrics.get('exits_by_timeout', 0),
             metrics.get('exits_by_thesis', 0),
             metrics.get('exits_by_score', 0),
             metrics.get('exits_by_human', 0),
             metrics.get('avg_entry_score'),
             metrics.get('avg_exit_score'),
             metrics.get('max_drawdown_pct'),
             json.dumps(metrics.get('sector_breakdown')) if metrics.get('sector_breakdown') else None,
             metrics.get('edge_stability'))
        )

    def get_performance(self, limit: int = 12) -> List[Dict]:
        return self._query(
            "SELECT * FROM mp_performance ORDER BY cohort_month DESC LIMIT %s",
            (limit,)
        )

    # ── Model Versions ─────────────────────────────────────

    def insert_model_version(self, version_tag: str, score_version: str = None,
                             thesis_version: str = None, rule_set: str = None,
                             config_snapshot: dict = None, notes: str = None):
        self._execute(
            """REPLACE INTO mp_model_versions
               (version_tag, score_version, thesis_version, rule_set,
                config_snapshot, notes)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (version_tag, score_version, thesis_version, rule_set,
             json.dumps(config_snapshot) if config_snapshot else None, notes)
        )
