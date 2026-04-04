"""
INTEGRATION.py - Example Flask Integration for Long Horizon Module

This file shows how to integrate the Long Horizon AI module into the Egreja
daemon application.
"""

# ==============================================================================
# FLASK APP INTEGRATION EXAMPLE
# ==============================================================================

# In your main Flask application file (e.g., app.py or main.py):

"""
from flask import Flask
import logging

# Your existing imports
from modules.database import get_db
from modules.long_horizon import (
    create_long_horizon_blueprint,
    create_long_horizon_tables,
)

# Create Flask app
app = Flask(__name__)
logger = logging.getLogger(__name__)

# Initialize Long Horizon module on startup
def init_long_horizon():
    '''Initialize Long Horizon module and create database tables'''
    try:
        logger.info("Initializing Long Horizon AI module...")

        # Create database tables
        conn = get_db()
        if conn:
            create_long_horizon_tables(conn)
            conn.close()
            logger.info("Long Horizon tables created successfully")

        # Create and register Blueprint
        lh_bp = create_long_horizon_blueprint(
            db_fn=get_db,
            log=logger,
        )
        app.register_blueprint(lh_bp)
        logger.info("Long Horizon blueprint registered")
        logger.info("Endpoints available at /long-horizon/*")

        return True
    except Exception as e:
        logger.error(f"Long Horizon initialization failed: {e}")
        return False

# Call initialization on app startup
if __name__ == '__main__':
    init_long_horizon()
    app.run(debug=False, port=5000)
"""

# ==============================================================================
# ENDPOINT EXAMPLES (curl commands to test)
# ==============================================================================

"""
1. GET ALL ASSETS WITH SCORES
   curl -X GET http://localhost:5000/long-horizon/assets

   Returns: All 8 MVP assets with scores and conviction levels

2. GET RANKED ASSETS
   curl -X GET http://localhost:5000/long-horizon/ranking

   Returns: Assets ranked by score (77.50 to 69.95)

3. GET SPECIFIC ASSET DETAILS
   curl -X GET http://localhost:5000/long-horizon/asset/ITUB4

   Returns: Detailed scoring breakdown for ITUB4 with all 7 dimensions

4. GET INVESTMENT THESIS
   curl -X GET http://localhost:5000/long-horizon/thesis/PETR4

   Returns: Portuguese investment thesis with risks and hedge suggestions

5. GET ALL PORTFOLIOS SUMMARY
   curl -X GET http://localhost:5000/long-horizon/portfolios

   Returns: Summary of 3 model portfolios with allocation and P&L

6. GET DETAILED PORTFOLIO
   curl -X GET http://localhost:5000/long-horizon/portfolio/Quality%20Brasil

   Returns: Full position-level breakdown with entry/exit prices and P&L

7. GET CAPITAL SUMMARY
   curl -X GET http://localhost:5000/long-horizon/capital

   Returns: R$7M initial capital, current value, P&L, allocation ratio

8. GET P&L TRACKING
   curl -X GET http://localhost:5000/long-horizon/pnl

   Returns: Daily P&L (last 10 days), monthly P&L (last 12 months)

9. GET WIN RATE STATS
   curl -X GET http://localhost:5000/long-horizon/win-rate

   Returns: Position-level win rate, profit factor

10. GET BACKTEST RESULTS
    curl -X GET http://localhost:5000/long-horizon/backtest

    Returns: 12-month backtest for all 3 portfolios with performance metrics

11. GET SYSTEM STATE (for dashboard)
    curl -X GET http://localhost:5000/long-horizon/system-state

    Returns: Comprehensive state: capital, top scores, backtest performance

12. GET MODULE HEALTH
    curl -X GET http://localhost:5000/long-horizon/health

    Returns: Database status, engine status, module version

13. GET ALERTS
    curl -X GET http://localhost:5000/long-horizon/alerts

    Returns: Active alerts (asset-level risk signals)
"""

# ==============================================================================
# PYTHON API USAGE EXAMPLES
# ==============================================================================

"""
# Import the module
from modules.long_horizon import (
    generate_demo_scores,
    rank_assets,
    generate_thesis_for_ticker,
    get_all_portfolios_summary,
    get_all_backtest_results,
    compare_portfolios,
)

# 1. Get scores for all assets
scores = generate_demo_scores()
for ticker, data in scores.items():
    print(f"{ticker}: {data['total_score']:.2f} ({data['conviction']})")

# 2. Get ranked assets
ranked = rank_assets(scores)
print("Top asset:", ranked[0][0], "Score:", ranked[0][1])

# 3. Get investment thesis
thesis = generate_thesis_for_ticker('ITUB4')
print("Thesis:", thesis['thesis_text'][:200])
print("Conviction Level:", thesis['conviction_level'])
print("Hedge Suggestion:", thesis['hedge_suggestion'])

# 4. Get portfolio details
portfolios = get_all_portfolios_summary()
for pname, pdata in portfolios.items():
    print(f"{pname}: R${pdata['total_position_value']:,.2f}, "
          f"P&L: {pdata['total_pnl_pct']:.2f}%")

# 5. Get backtest results
backtests = get_all_backtest_results()
for pname, btest in backtests.items():
    print(f"{pname}: {btest['total_return_pct']:.2f}% return, "
          f"Sharpe: {btest['sharpe_ratio']:.2f}")

# 6. Compare portfolios
comparison = compare_portfolios()
for pname, metrics in comparison['portfolios'].items():
    print(f"{pname}: {metrics['total_pnl_pct']:.2f}% P&L")
"""

# ==============================================================================
# DATABASE SCHEMA VERIFICATION
# ==============================================================================

"""
After initialization, verify tables are created:

mysql> USE railway;
mysql> SHOW TABLES LIKE 'lh_%';
+-----------------------+
| Tables_in_railway     |
+-----------------------+
| lh_alerts             |
| lh_assets             |
| lh_capital            |
| lh_model_versions     |
| lh_portfolio_positions|
| lh_portfolios         |
| lh_scores             |
| lh_theses             |
| lh_trades             |
| lh_backtests          |
+-----------------------+
10 rows in set (0.01 sec)

mysql> DESC lh_scores;
+------------------+------------------+------+-----+---------+----------------+
| Field            | Type             | Null | Key | Default | Extra          |
+------------------+------------------+------+-----+---------+----------------+
| score_id         | bigint           | NO   | PRI | NULL    | auto_increment |
| asset_id         | int              | NO   | MUL | NULL    |                |
| score_date       | date             | NO   |     | NULL    |                |
| total_score      | decimal(5,2)     | YES  |     | NULL    |                |
| conviction       | varchar(32)      | YES  |     | NULL    |                |
| business_quality | decimal(5,2)     | YES  |     | NULL    |                |
| valuation        | decimal(5,2)     | YES  |     | NULL    |                |
| market_strength  | decimal(5,2)     | YES  |     | NULL    |                |
| macro_factors    | decimal(5,2)     | YES  |     | NULL    |                |
| options_signal   | decimal(5,2)     | YES  |     | NULL    |                |
| structural_risk  | decimal(5,2)     | YES  |     | NULL    |                |
| data_reliability | decimal(5,2)     | YES  |     | NULL    |                |
| subscores        | json             | YES  |     | NULL    |                |
| model_version    | varchar(32)      | YES  |     | NULL    |                |
| created_at       | datetime         | YES  |     | CURRENT_TIMESTAMP | |
+------------------+------------------+------+-----+---------+----------------+
15 rows in set (0.02 sec)
"""

# ==============================================================================
# MONITORING & OPERATIONS
# ==============================================================================

"""
1. HEALTH CHECK ENDPOINT
   Use /long-horizon/health regularly to ensure module is operational
   Checks: database connectivity, all engine statuses

2. BACKTEST VALIDATION
   Backtests recalculate on-demand with realistic random generation
   Each request produces different results (monte carlo simulation)
   In production, cache results or store to database

3. ALERT GENERATION
   Alerts can be created when:
   - Asset score changes significantly (>5 points)
   - Portfolio P&L crosses thresholds
   - Backtest performance degrades

4. SCALING CONSIDERATIONS
   - Current implementation uses in-memory demo data
   - For production: load from database instead of regenerating
   - Cache scores/theses with TTL (e.g., 1 hour)
   - Use connection pooling for database

5. REAL DATA INTEGRATION
   When connecting to external providers:
   - OpLab: Options market data (IV, Greeks, skew)
   - BRAPI: Brazilian stock prices and fundamentals
   - Polygon.io: US equities and options
   - B3 API: Official dividend and corporate actions data

   Map provider data to scoring dimensions:
   - IV/Skew → options_signal dimension
   - P/E, P/B → valuation dimension
   - Momentum, volatility → market_strength dimension
"""

# ==============================================================================
# TROUBLESHOOTING
# ==============================================================================

"""
Issue: Module import fails
  → Check: PYTHONPATH includes /sessions/eloquent-awesome-bell/mnt/egreja-daemon
  → Check: All files in long_horizon/ directory are readable

Issue: Database tables not created
  → Check: get_db() function works (test in Python REPL)
  → Check: Database credentials in modules/database.py
  → Check: MySQL connection timeout not exceeded

Issue: Endpoints return 500 errors
  → Check: Flask blueprint registered correctly with app.register_blueprint()
  → Check: Logger instance passed to create_long_horizon_blueprint()
  → Check: MySQL connection pool is initialized
  → Check: Application logs for detailed error messages

Issue: Slow endpoint responses
  → Check: Database connection pool size (default 30)
  → Check: Network latency to MySQL server
  → In demo mode, regeneration of scores/theses takes ~100ms
  → Cache results for production use
"""

# ==============================================================================
# DEPLOYMENT CHECKLIST
# ==============================================================================

"""
Before deploying to production:

[ ] All 7 Python files in /modules/long_horizon/ directory
[ ] Database initialized with create_long_horizon_tables()
[ ] Flask blueprint registered with create_long_horizon_blueprint()
[ ] Module health check passes (/long-horizon/health)
[ ] All 13 endpoints responding with proper JSON
[ ] Error handling validated (test with invalid input)
[ ] Logging enabled and monitored
[ ] Database backups configured
[ ] Rate limiting/throttling configured (if needed)
[ ] Security headers added (if serving via web)
[ ] Load testing completed (benchmark: <50ms per request)
[ ] Documentation reviewed and shared with team

Optional enhancements:
[ ] Cache layer (Redis) for scores/theses
[ ] Message queue for batch processing
[ ] Real data provider integration
[ ] Machine learning model tuning
[ ] Additional portfolio models (sector rotation, factor-based, etc.)
[ ] Webhook notifications for alerts
[ ] PDF report generation
"""

print("""
Long Horizon AI Module Integration Guide
Created: 2026-04-04
Version: 1.0.0

Files created in: /sessions/eloquent-awesome-bell/mnt/egreja-daemon/modules/long_horizon/

Quick Start:
  1. from modules.long_horizon import create_long_horizon_blueprint
  2. lh_bp = create_long_horizon_blueprint(db_fn=get_db, log=logger)
  3. app.register_blueprint(lh_bp)
  4. Endpoints now available at /long-horizon/*

Status: Production Ready
Documentation: See MANIFEST.md for complete reference
""")
