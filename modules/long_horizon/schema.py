"""
SQL Schema Definition for Long Horizon AI Investment Module.

Creates all necessary tables for asset scoring, thesis tracking, portfolio management,
backtesting, and capital monitoring.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def create_long_horizon_tables(connection: Any) -> None:
    """
    Create all long horizon investment infrastructure tables.

    Args:
        connection: Database connection object (MySQL compatible)
    """
    cursor = connection.cursor()

    try:
        # 1. Assets Master Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lh_assets (
            asset_id INT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(16) NOT NULL UNIQUE,
            name VARCHAR(255) NOT NULL,
            sector VARCHAR(64),
            market VARCHAR(32),
            asset_type VARCHAR(32),
            active BOOLEAN DEFAULT TRUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_ticker (ticker),
            INDEX idx_sector (sector),
            INDEX idx_active (active)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 2. Asset Scores - Proprietary score with 7 dimensions
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lh_scores (
            score_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            asset_id INT NOT NULL,
            score_date DATE NOT NULL,
            total_score DECIMAL(5, 2),
            conviction VARCHAR(32),
            business_quality DECIMAL(5, 2),
            valuation DECIMAL(5, 2),
            market_strength DECIMAL(5, 2),
            macro_factors DECIMAL(5, 2),
            options_signal DECIMAL(5, 2),
            structural_risk DECIMAL(5, 2),
            data_reliability DECIMAL(5, 2),
            subscores JSON,
            model_version VARCHAR(32),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (asset_id) REFERENCES lh_assets(asset_id),
            UNIQUE KEY unique_asset_date (asset_id, score_date),
            INDEX idx_asset_id (asset_id),
            INDEX idx_score_date (score_date),
            INDEX idx_total_score (total_score),
            INDEX idx_conviction (conviction)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 3. Investment Theses - Explainable investment reasoning
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lh_theses (
            thesis_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            asset_id INT NOT NULL,
            thesis_date DATE NOT NULL,
            thesis_text LONGTEXT,
            key_drivers JSON,
            risks LONGTEXT,
            hedge_suggestion VARCHAR(255),
            recommended_horizon VARCHAR(32),
            conviction_level DECIMAL(5, 2),
            model_version VARCHAR(32),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (asset_id) REFERENCES lh_assets(asset_id),
            INDEX idx_asset_id (asset_id),
            INDEX idx_thesis_date (thesis_date),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 4. Portfolio Master Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lh_portfolios (
            portfolio_id INT AUTO_INCREMENT PRIMARY KEY,
            portfolio_name VARCHAR(64) NOT NULL,
            description VARCHAR(255),
            target_return DECIMAL(5, 2),
            risk_level VARCHAR(32),
            initial_capital DECIMAL(16, 2),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            rebalance_date DATE,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_portfolio_name (portfolio_name),
            INDEX idx_portfolio_name (portfolio_name),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 5. Portfolio Positions
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lh_portfolio_positions (
            position_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            portfolio_id INT NOT NULL,
            ticker VARCHAR(16) NOT NULL,
            weight DECIMAL(5, 2),
            entry_price DECIMAL(12, 4),
            current_price DECIMAL(12, 4),
            quantity DECIMAL(18, 4),
            position_value DECIMAL(16, 2),
            pnl DECIMAL(16, 2),
            pnl_pct DECIMAL(8, 4),
            entry_date DATE,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (portfolio_id) REFERENCES lh_portfolios(portfolio_id),
            INDEX idx_portfolio_id (portfolio_id),
            INDEX idx_ticker (ticker),
            INDEX idx_updated_at (updated_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 6. Backtest Results
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lh_backtests (
            backtest_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            portfolio_id INT NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            total_return DECIMAL(8, 4),
            annualized_return DECIMAL(8, 4),
            benchmark_return DECIMAL(8, 4),
            outperformance DECIMAL(8, 4),
            sharpe_ratio DECIMAL(8, 4),
            max_drawdown DECIMAL(8, 4),
            win_rate DECIMAL(5, 4),
            profit_factor DECIMAL(8, 4),
            trades_count INT,
            monthly_returns JSON,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (portfolio_id) REFERENCES lh_portfolios(portfolio_id),
            INDEX idx_portfolio_id (portfolio_id),
            INDEX idx_start_date (start_date),
            INDEX idx_end_date (end_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 7. Capital Tracking
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lh_capital (
            capital_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL,
            initial_capital DECIMAL(16, 2),
            current_value DECIMAL(16, 2),
            daily_pnl DECIMAL(14, 2),
            monthly_pnl DECIMAL(14, 2),
            annual_pnl DECIMAL(14, 2),
            allocated DECIMAL(16, 2),
            reserve DECIMAL(16, 2),
            return_pct DECIMAL(8, 4),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_date (date),
            INDEX idx_date (date),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 8. Trades Log
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lh_trades (
            trade_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(16) NOT NULL,
            portfolio VARCHAR(64),
            action VARCHAR(16),
            quantity DECIMAL(18, 4),
            price DECIMAL(12, 4),
            trade_date DATETIME NOT NULL,
            pnl_realized DECIMAL(14, 2),
            pnl_pct DECIMAL(8, 4),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_ticker (ticker),
            INDEX idx_portfolio (portfolio),
            INDEX idx_trade_date (trade_date),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 9. Model Versions
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lh_model_versions (
            version_id INT AUTO_INCREMENT PRIMARY KEY,
            version VARCHAR(32) NOT NULL UNIQUE,
            dimension_weights JSON,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            notes VARCHAR(255),
            INDEX idx_version (version)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 10. Alerts
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lh_alerts (
            alert_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            asset_id INT,
            alert_type VARCHAR(64),
            message VARCHAR(255),
            severity VARCHAR(32),
            resolved BOOLEAN DEFAULT FALSE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            resolved_at DATETIME,
            FOREIGN KEY (asset_id) REFERENCES lh_assets(asset_id),
            INDEX idx_asset_id (asset_id),
            INDEX idx_alert_type (alert_type),
            INDEX idx_severity (severity),
            INDEX idx_resolved (resolved),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        connection.commit()
        logger.info("All long_horizon tables created successfully (10 tables)")

    except Exception as e:
        connection.rollback()
        logger.error(f"Error creating long_horizon tables: {e}")
        raise
    finally:
        cursor.close()
