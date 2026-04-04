"""
SQL Schema Definition for Derivatives Trading Infrastructure.

Creates all necessary tables for strategy tracking, market data snapshots,
calibration, liquidity monitoring, and execution metrics.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def create_derivatives_tables(connection: Any) -> None:
    """
    Create all derivatives trading infrastructure tables.
    
    Args:
        connection: Database connection object (MySQL compatible)
    """
    cursor = connection.cursor()
    
    try:
        # 1. Strategy Master Trades - Core trade records
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_master_trades (
            trade_id VARCHAR(64) PRIMARY KEY,
            strategy_type VARCHAR(32) NOT NULL,
            symbol VARCHAR(16) NOT NULL,
            underlying VARCHAR(16),
            strike DECIMAL(10, 2),
            expiry VARCHAR(8),
            direction VARCHAR(16),
            structure_type VARCHAR(32),
            expected_edge DECIMAL(12, 4),
            realized_edge DECIMAL(12, 4),
            expected_cost DECIMAL(12, 4),
            executed_cost DECIMAL(12, 4),
            slippage DECIMAL(12, 4),
            latency_ms INT,
            pnl DECIMAL(14, 2),
            pnl_pct DECIMAL(8, 4),
            rejection_reason VARCHAR(255),
            risk_approval_id VARCHAR(64),
            calibration_ref VARCHAR(64),
            liquidity_score DECIMAL(5, 2),
            active_status VARCHAR(32),
            opened_at DATETIME,
            closed_at DATETIME,
            status VARCHAR(16),
            close_reason VARCHAR(255),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_strategy_symbol (strategy_type, symbol),
            INDEX idx_expiry (expiry),
            INDEX idx_status (status),
            INDEX idx_opened_at (opened_at),
            INDEX idx_strategy_type (strategy_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 2. Strategy Trade Legs - Individual legs of multi-leg trades
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_trade_legs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            trade_id VARCHAR(64) NOT NULL,
            leg_type VARCHAR(32),
            symbol VARCHAR(16) NOT NULL,
            qty INT,
            side VARCHAR(8),
            intended_price DECIMAL(10, 4),
            executed_price DECIMAL(10, 4),
            fill_status VARCHAR(16),
            slippage DECIMAL(10, 4),
            latency_ms INT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (trade_id) REFERENCES strategy_master_trades(trade_id),
            INDEX idx_trade_id (trade_id),
            INDEX idx_symbol (symbol),
            INDEX idx_timestamp (timestamp)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 3. Strategy Opportunities Log - Decision audit trail
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_opportunities_log (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            strategy_type VARCHAR(32) NOT NULL,
            symbol VARCHAR(16) NOT NULL,
            strike DECIMAL(10, 2),
            expiry VARCHAR(8),
            opportunity_type VARCHAR(32),
            expected_edge_bps DECIMAL(8, 2),
            cost_estimate DECIMAL(12, 4),
            liquidity_score DECIMAL(5, 2),
            decision VARCHAR(16),
            rejection_reason VARCHAR(255),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_strategy_symbol (strategy_type, symbol),
            INDEX idx_timestamp (timestamp),
            INDEX idx_decision (decision),
            INDEX idx_expiry (expiry)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 4. Calibration Data - Statistical calibration windows
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS calibration_data (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            strategy_type VARCHAR(32) NOT NULL,
            symbol VARCHAR(16) NOT NULL,
            expiry VARCHAR(8),
            metric_name VARCHAR(64) NOT NULL,
            mean_val DECIMAL(14, 6),
            std_val DECIMAL(14, 6),
            p5 DECIMAL(14, 6),
            p95 DECIMAL(14, 6),
            sample_count INT,
            window_start DATETIME,
            window_end DATETIME,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_strategy_metric (strategy_type, metric_name),
            INDEX idx_symbol_expiry (symbol, expiry),
            INDEX idx_updated_at (updated_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 5. Options Snapshots - Market snapshots for options
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS options_snapshots (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(16) NOT NULL,
            underlying VARCHAR(16),
            strike DECIMAL(10, 2),
            expiry VARCHAR(8),
            option_type VARCHAR(8),
            bid DECIMAL(10, 4),
            ask DECIMAL(10, 4),
            last DECIMAL(10, 4),
            volume INT,
            oi INT,
            iv DECIMAL(8, 6),
            delta DECIMAL(6, 4),
            gamma DECIMAL(8, 6),
            theta DECIMAL(8, 6),
            vega DECIMAL(8, 6),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_symbol_strike_expiry (symbol, strike, expiry),
            INDEX idx_timestamp (timestamp),
            INDEX idx_expiry (expiry),
            INDEX idx_option_type (option_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 6. Futures Snapshots - Market snapshots for futures
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS futures_snapshots (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(16) NOT NULL,
            underlying VARCHAR(16),
            expiry VARCHAR(8),
            bid DECIMAL(10, 4),
            ask DECIMAL(10, 4),
            last DECIMAL(10, 4),
            volume INT,
            oi INT,
            basis DECIMAL(10, 4),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_symbol_expiry (symbol, expiry),
            INDEX idx_timestamp (timestamp),
            INDEX idx_underlying (underlying)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 7. Greeks Snapshots - Options Greeks over time
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS greeks_snapshots (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(16) NOT NULL,
            underlying VARCHAR(16),
            strike DECIMAL(10, 2),
            expiry VARCHAR(8),
            iv DECIMAL(8, 6),
            delta DECIMAL(6, 4),
            gamma DECIMAL(8, 6),
            theta DECIMAL(8, 6),
            vega DECIMAL(8, 6),
            rho DECIMAL(8, 6),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_symbol_strike_expiry (symbol, strike, expiry),
            INDEX idx_timestamp (timestamp)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 8. Dividend Events - Corporate actions
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dividend_events (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(16) NOT NULL,
            ex_date DATE,
            record_date DATE,
            payment_date DATE,
            amount DECIMAL(10, 4),
            div_type VARCHAR(32),
            status VARCHAR(16),
            source VARCHAR(64),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_symbol_ex_date (symbol, ex_date),
            INDEX idx_ex_date (ex_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 9. Strategy Liquidity Monitor - Real-time liquidity tracking
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_liquidity_monitor (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(16) NOT NULL,
            strategy_type VARCHAR(32) NOT NULL,
            expiry VARCHAR(8),
            strike DECIMAL(10, 2),
            spread_score DECIMAL(5, 2),
            depth_score DECIMAL(5, 2),
            oi_volume_score DECIMAL(5, 2),
            exec_plausibility_score DECIMAL(5, 2),
            persistence_score DECIMAL(5, 2),
            exit_liquidity_score DECIMAL(5, 2),
            data_quality_score DECIMAL(5, 2),
            total_score DECIMAL(5, 2),
            tier VARCHAR(32),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_symbol_strategy (symbol, strategy_type),
            INDEX idx_tier (tier),
            INDEX idx_timestamp (timestamp),
            INDEX idx_expiry_strike (expiry, strike)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 10. Strategy Scorecards - Performance metrics per period
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_scorecards (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            strategy_type VARCHAR(32) NOT NULL,
            symbol VARCHAR(16) NOT NULL,
            period VARCHAR(32),
            opportunities_seen INT,
            opportunities_approved INT,
            opportunities_rejected INT,
            trades_executed INT,
            legging_incidents INT,
            edge_realized_mean DECIMAL(8, 4),
            edge_expected_mean DECIMAL(8, 4),
            slippage_mean DECIMAL(10, 4),
            latency_mean DECIMAL(10, 2),
            pnl_total DECIMAL(14, 2),
            sharpe DECIMAL(8, 4),
            profit_factor DECIMAL(8, 4),
            max_drawdown DECIMAL(8, 4),
            fill_ratio DECIMAL(5, 4),
            multi_leg_completion_rate DECIMAL(5, 4),
            data_quality_incidents INT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_strategy_period (strategy_type, period),
            INDEX idx_symbol (symbol),
            INDEX idx_timestamp (timestamp)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 11. Execution Plausibility Log - Multi-leg execution feasibility
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS execution_plausibility_log (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            strategy_type VARCHAR(32) NOT NULL,
            symbol VARCHAR(16) NOT NULL,
            structure_type VARCHAR(32),
            expected_price DECIMAL(12, 4),
            executable_price DECIMAL(12, 4),
            slippage_estimate DECIMAL(10, 4),
            depth_sufficient BOOLEAN,
            multi_leg_feasible BOOLEAN,
            result VARCHAR(32),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_strategy_symbol (strategy_type, symbol),
            INDEX idx_result (result),
            INDEX idx_timestamp (timestamp)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 12. Liquidity Score History - Score evolution tracking
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS liquidity_score_history (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(16) NOT NULL,
            strategy_type VARCHAR(32) NOT NULL,
            expiry VARCHAR(8),
            strike DECIMAL(10, 2),
            total_score DECIMAL(5, 2),
            tier VARCHAR(32),
            spread_score DECIMAL(5, 2),
            depth_score DECIMAL(5, 2),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_symbol_strategy (symbol, strategy_type),
            INDEX idx_timestamp (timestamp),
            INDEX idx_expiry (expiry)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 13. Active Status Registry - Current execution status per (asset, strategy)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS active_status_registry (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(16) NOT NULL,
            strategy_type VARCHAR(32) NOT NULL,
            current_status VARCHAR(32),
            prev_status VARCHAR(32),
            days_in_status INT,
            liquidity_score_avg DECIMAL(5, 2),
            last_promotion DATETIME,
            last_demotion DATETIME,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_symbol_strategy (symbol, strategy_type),
            INDEX idx_current_status (current_status),
            INDEX idx_updated_at (updated_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 14. Daily Capital Summary - End-of-day capital snapshots
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS derivatives_daily_capital (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            trade_date DATE NOT NULL,
            total_capital DECIMAL(16, 2),
            allocated DECIMAL(16, 2),
            daily_pnl DECIMAL(14, 2),
            trades_count INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_trade_date (trade_date),
            INDEX idx_trade_date (trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 15. Monthly P&L Aggregation
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS derivatives_monthly_pnl (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            year_month VARCHAR(7) NOT NULL,
            strategy_type VARCHAR(32),
            symbol VARCHAR(16),
            total_pnl DECIMAL(16, 2),
            trade_count INT,
            win_count INT,
            loss_count INT,
            win_rate DECIMAL(5, 4),
            avg_edge_realized DECIMAL(10, 4),
            avg_slippage DECIMAL(10, 4),
            sharpe DECIMAL(8, 4),
            profit_factor DECIMAL(8, 4),
            max_drawdown DECIMAL(14, 2),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_month_strat_sym (year_month, strategy_type, symbol),
            INDEX idx_year_month (year_month),
            INDEX idx_strategy (strategy_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 16. Annual P&L Aggregation
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS derivatives_annual_pnl (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            year INT NOT NULL,
            strategy_type VARCHAR(32),
            symbol VARCHAR(16),
            total_pnl DECIMAL(16, 2),
            trade_count INT,
            win_count INT,
            loss_count INT,
            win_rate DECIMAL(5, 4),
            sharpe DECIMAL(8, 4),
            profit_factor DECIMAL(8, 4),
            max_drawdown DECIMAL(14, 2),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_year_strat_sym (year, strategy_type, symbol),
            INDEX idx_year (year),
            INDEX idx_strategy (strategy_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 17. Learning Outcomes - Detailed learning data per trade
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS derivatives_learning_outcomes (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            trade_id VARCHAR(64) NOT NULL,
            strategy_type VARCHAR(32) NOT NULL,
            symbol VARCHAR(16) NOT NULL,
            structure_type VARCHAR(32),
            expected_edge DECIMAL(12, 4),
            realized_pnl DECIMAL(14, 2),
            slippage_total DECIMAL(10, 4),
            latency_avg_ms DECIMAL(10, 2),
            time_in_trade_hours DECIMAL(10, 2),
            close_reason VARCHAR(255),
            liquidity_score DECIMAL(5, 2),
            active_status VARCHAR(32),
            legs_count INT,
            legging_incidents INT,
            confidence_at_entry DECIMAL(5, 4),
            confidence_adj_after DECIMAL(5, 4),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_trade_id (trade_id),
            INDEX idx_strategy_symbol (strategy_type, symbol),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        connection.commit()
        logger.info("All derivatives tables created successfully (17 tables)")
        
    except Exception as e:
        connection.rollback()
        logger.error(f"Error creating derivatives tables: {e}")
        raise
    finally:
        cursor.close()
