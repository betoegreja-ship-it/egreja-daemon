"""
Unified Brain - Database Schema

8 tables tracking the learning, pattern detection, and intelligence generated
by the unified learning engine across all 5 Egreja modules.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def create_unified_brain_tables(connection: Any) -> None:
    """
    Create all unified brain infrastructure tables.

    Tables:
    1. brain_lessons - Individual lessons learned (45+)
    2. brain_patterns - Cross-domain patterns detected (12+)
    3. brain_correlations - Asset/strategy correlations (8+)
    4. brain_decisions - Decision recommendations (15+)
    5. brain_metrics - Daily aggregated metrics (180+)
    6. brain_regime - Market regime detection
    7. brain_daily_digest - Daily intelligence reports
    8. brain_evolution - Brain improvement tracking

    Args:
        connection: Database connection object (MySQL compatible)
    """
    cursor = connection.cursor()

    try:
        # 1. Brain Lessons - Every lesson learned
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS brain_lessons (
            lesson_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            module VARCHAR(32) NOT NULL,
            strategy VARCHAR(64),
            lesson_type VARCHAR(64) NOT NULL,
            description TEXT NOT NULL,
            impact_score DECIMAL(5, 2),
            confidence DECIMAL(5, 2),
            data_json JSON,
            learned_at DATETIME NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_module (module),
            INDEX idx_lesson_type (lesson_type),
            INDEX idx_confidence (confidence),
            INDEX idx_learned_at (learned_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 2. Brain Patterns - Cross-domain patterns
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS brain_patterns (
            pattern_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            pattern_type VARCHAR(64) NOT NULL,
            description TEXT NOT NULL,
            modules_involved JSON,
            correlation DECIMAL(6, 4),
            confidence DECIMAL(5, 2),
            occurrences INT DEFAULT 1,
            first_seen DATE NOT NULL,
            last_seen DATE NOT NULL,
            active BOOLEAN DEFAULT TRUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_pattern_type (pattern_type),
            INDEX idx_confidence (confidence),
            INDEX idx_active (active),
            INDEX idx_last_seen (last_seen)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 3. Brain Correlations - Asset and strategy correlations
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS brain_correlations (
            correlation_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            asset_a VARCHAR(32) NOT NULL,
            asset_b VARCHAR(32) NOT NULL,
            module_a VARCHAR(32) NOT NULL,
            module_b VARCHAR(32) NOT NULL,
            correlation_coeff DECIMAL(6, 4),
            timeframe VARCHAR(32),
            sample_size INT,
            reliability DECIMAL(5, 2),
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_pair (asset_a, asset_b, module_a, module_b),
            INDEX idx_asset_a (asset_a),
            INDEX idx_asset_b (asset_b),
            INDEX idx_correlation (correlation_coeff)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 4. Brain Decisions - AI recommendations
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS brain_decisions (
            decision_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            decision_type VARCHAR(64) NOT NULL,
            module VARCHAR(32),
            recommendation TEXT NOT NULL,
            reasoning TEXT,
            factors_json JSON,
            confidence DECIMAL(5, 2),
            outcome VARCHAR(32),
            decided_at DATETIME NOT NULL,
            resolved_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_decision_type (decision_type),
            INDEX idx_module (module),
            INDEX idx_confidence (confidence),
            INDEX idx_decided_at (decided_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 5. Brain Metrics - Daily aggregated metrics
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS brain_metrics (
            metric_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL,
            module VARCHAR(32) NOT NULL,
            metric_name VARCHAR(64) NOT NULL,
            value DECIMAL(12, 4),
            trend VARCHAR(32),
            context_json JSON,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_metric (date, module, metric_name),
            INDEX idx_date (date),
            INDEX idx_module (module),
            INDEX idx_metric_name (metric_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 6. Brain Regime - Market regime detection
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS brain_regime (
            regime_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL,
            regime_type VARCHAR(32) NOT NULL,
            confidence DECIMAL(5, 2),
            indicators_json JSON,
            duration_days INT,
            module_signals JSON,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_date (date),
            INDEX idx_date (date),
            INDEX idx_regime_type (regime_type),
            INDEX idx_confidence (confidence)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 7. Brain Daily Digest - Daily intelligence reports
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS brain_daily_digest (
            digest_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            digest_json JSON,
            key_insights JSON,
            alerts JSON,
            recommendations JSON,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_date (date),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 8. Brain Evolution - Track improvement over time
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS brain_evolution (
            evolution_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            total_lessons INT DEFAULT 0,
            accuracy_pct DECIMAL(5, 2),
            patterns_active INT DEFAULT 0,
            decisions_correct INT DEFAULT 0,
            decisions_total INT DEFAULT 0,
            evolution_score DECIMAL(5, 2),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_date (date),
            INDEX idx_evolution_score (evolution_score)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        connection.commit()
        logger.info("All unified_brain tables created successfully (8 tables)")

    except Exception as e:
        connection.rollback()
        logger.error(f"Error creating unified_brain tables: {e}")
        raise
    finally:
        cursor.close()
