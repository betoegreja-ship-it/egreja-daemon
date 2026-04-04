"""
Derivatives Strategy Endpoints
Flask Blueprint with 17 endpoints for strategy monitoring and management
Compatible with MySQL (mysql.connector) - uses %s placeholders, cursor(dictionary=True)
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
import traceback


def create_strategies_blueprint(db_fn, log, provider_mgr, services_dict):
    """
    Factory function to create Flask Blueprint with dependencies.
    db_fn: callable returning a mysql.connector connection (from pool).
    """
    strategies_bp = Blueprint('strategies', __name__, url_prefix='/strategies')

    # ============== HELPER FUNCTIONS ==============

    def get_db():
        return db_fn()

    def _safe_query(query, params=None, fetch='all'):
        """Execute a read query safely, always closing the connection."""
        conn = None
        try:
            conn = get_db()
            if conn is None:
                return []
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, params or ())
            if fetch == 'one':
                return cursor.fetchone()
            return cursor.fetchall() or []
        except Exception as e:
            log.warning(f"_safe_query error: {e}")
            return [] if fetch == 'all' else None
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _safe_write(query, params=None):
        """Execute a write query safely, committing and closing."""
        conn = None
        try:
            conn = get_db()
            if conn is None:
                return False
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            conn.commit()
            return True
        except Exception as e:
            log.warning(f"_safe_write error: {e}")
            return False
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def query_opportunities(strategy_type=None, symbol=None, limit=100, offset=0,
                           start_date=None, end_date=None):
        """Generic opportunity query"""
        query = "SELECT * FROM strategy_opportunities_log WHERE 1=1"
        params = []

        if strategy_type:
            query += " AND strategy_type = %s"
            params.append(strategy_type)
        if symbol:
            query += " AND symbol = %s"
            params.append(symbol)
        if start_date:
            query += " AND timestamp >= %s"
            params.append(start_date)
        if end_date:
            query += " AND timestamp <= %s"
            params.append(end_date)

        query += " ORDER BY timestamp DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        return _safe_query(query, params)

    def _serialize_row(row):
        """Convert datetime/decimal fields to JSON-serializable types."""
        if not row:
            return row
        out = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                out[k] = v.isoformat()
            elif hasattr(v, '__float__'):  # Decimal
                out[k] = float(v)
            else:
                out[k] = v
        return out

    def _serialize_rows(rows):
        return [_serialize_row(r) for r in rows] if rows else []

    # ============== STRATEGY ENDPOINTS ==============

    @strategies_bp.route('/pcp', methods=['GET'])
    def get_pcp():
        """GET /strategies/pcp - PCP trades and opportunities"""
        try:
            limit = request.args.get('limit', 50, type=int)
            offset = request.args.get('offset', 0, type=int)
            symbol = request.args.get('symbol')

            opps = query_opportunities('PCP', symbol, limit, offset)

            return jsonify({
                'strategy': 'PCP',
                'opportunities_count': len(opps),
                'opportunities': _serialize_rows(opps),
                'pagination': {'limit': limit, 'offset': offset}
            }), 200
        except Exception as e:
            log.error(f"GET /pcp error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/fst', methods=['GET'])
    def get_fst():
        """GET /strategies/fst - Futuro Sintetico Triangular"""
        try:
            limit = request.args.get('limit', 50, type=int)
            offset = request.args.get('offset', 0, type=int)
            symbol = request.args.get('symbol')

            opps = query_opportunities('FST', symbol, limit, offset)

            return jsonify({
                'strategy': 'FST',
                'opportunities_count': len(opps),
                'opportunities': _serialize_rows(opps),
                'pagination': {'limit': limit, 'offset': offset}
            }), 200
        except Exception as e:
            log.error(f"GET /fst error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/roll-arb', methods=['GET'])
    def get_roll_arb():
        """GET /strategies/roll-arb - Roll Arbitrage"""
        try:
            limit = request.args.get('limit', 50, type=int)
            offset = request.args.get('offset', 0, type=int)
            symbol = request.args.get('symbol')

            opps = query_opportunities('ROLL_ARB', symbol, limit, offset)

            return jsonify({
                'strategy': 'ROLL_ARB',
                'opportunities_count': len(opps),
                'opportunities': _serialize_rows(opps),
                'pagination': {'limit': limit, 'offset': offset}
            }), 200
        except Exception as e:
            log.error(f"GET /roll-arb error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/etf-basket', methods=['GET'])
    def get_etf_basket():
        """GET /strategies/etf-basket - ETF vs Basket"""
        try:
            limit = request.args.get('limit', 50, type=int)
            offset = request.args.get('offset', 0, type=int)
            symbol = request.args.get('symbol', 'BOVA11')

            opps = query_opportunities('ETF_BASKET', symbol, limit, offset)

            return jsonify({
                'strategy': 'ETF_BASKET',
                'opportunities_count': len(opps),
                'opportunities': _serialize_rows(opps),
                'pagination': {'limit': limit, 'offset': offset}
            }), 200
        except Exception as e:
            log.error(f"GET /etf-basket error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/skew', methods=['GET'])
    def get_skew():
        """GET /strategies/skew - Volatility Skew"""
        try:
            limit = request.args.get('limit', 50, type=int)
            offset = request.args.get('offset', 0, type=int)
            symbol = request.args.get('symbol')

            opps = query_opportunities('SKEW_ARB', symbol, limit, offset)

            return jsonify({
                'strategy': 'SKEW_ARB',
                'opportunities_count': len(opps),
                'opportunities': _serialize_rows(opps),
                'pagination': {'limit': limit, 'offset': offset}
            }), 200
        except Exception as e:
            log.error(f"GET /skew error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/interlisted', methods=['GET'])
    def get_interlisted():
        """GET /strategies/interlisted - Inter-Listed ADR/B3"""
        try:
            limit = request.args.get('limit', 50, type=int)
            offset = request.args.get('offset', 0, type=int)
            symbol = request.args.get('symbol')

            opps = query_opportunities('INTERLISTED', symbol, limit, offset)

            return jsonify({
                'strategy': 'INTERLISTED',
                'opportunities_count': len(opps),
                'opportunities': _serialize_rows(opps),
                'pagination': {'limit': limit, 'offset': offset}
            }), 200
        except Exception as e:
            log.error(f"GET /interlisted error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/dividend', methods=['GET'])
    def get_dividend():
        """GET /strategies/dividend - Dividend Arbitrage"""
        try:
            limit = request.args.get('limit', 50, type=int)
            offset = request.args.get('offset', 0, type=int)
            symbol = request.args.get('symbol')

            opps = query_opportunities('DIVIDEND_ARB', symbol, limit, offset)

            return jsonify({
                'strategy': 'DIVIDEND_ARB',
                'opportunities_count': len(opps),
                'opportunities': _serialize_rows(opps),
                'pagination': {'limit': limit, 'offset': offset}
            }), 200
        except Exception as e:
            log.error(f"GET /dividend error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/vol-arb', methods=['GET'])
    def get_vol_arb():
        """GET /strategies/vol-arb - Volatility Arbitrage"""
        try:
            limit = request.args.get('limit', 50, type=int)
            offset = request.args.get('offset', 0, type=int)
            symbol = request.args.get('symbol')

            opps = query_opportunities('VOL_ARB', symbol, limit, offset)

            return jsonify({
                'strategy': 'VOL_ARB',
                'opportunities_count': len(opps),
                'opportunities': _serialize_rows(opps),
                'pagination': {'limit': limit, 'offset': offset}
            }), 200
        except Exception as e:
            log.error(f"GET /vol-arb error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    # ============== AGGREGATE & MONITORING ENDPOINTS ==============

    @strategies_bp.route('/opportunities', methods=['GET'])
    def get_all_opportunities():
        """GET /strategies/opportunities - All opportunities across strategies with filters"""
        try:
            strategy = request.args.get('strategy_type')
            symbol = request.args.get('symbol')
            limit = request.args.get('limit', 100, type=int)
            offset = request.args.get('offset', 0, type=int)
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')

            opps = query_opportunities(strategy, symbol, limit, offset, start_date, end_date)

            # Group by strategy
            by_strategy = {}
            for opp in opps:
                strat = opp.get('strategy_type', 'UNKNOWN')
                if strat not in by_strategy:
                    by_strategy[strat] = []
                by_strategy[strat].append(_serialize_row(opp))

            return jsonify({
                'total_opportunities': len(opps),
                'by_strategy': by_strategy,
                'pagination': {'limit': limit, 'offset': offset},
                'filters': {
                    'strategy_type': strategy,
                    'symbol': symbol,
                    'start_date': start_date,
                    'end_date': end_date
                }
            }), 200
        except Exception as e:
            log.error(f"GET /opportunities error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/calibration', methods=['GET'])
    def get_calibration():
        """GET /strategies/calibration - Calibration data per strategy"""
        try:
            strategy = request.args.get('strategy_type')
            symbol = request.args.get('symbol')

            query = "SELECT * FROM calibration_data WHERE 1=1"
            params = []

            if strategy:
                query += " AND strategy_type = %s"
                params.append(strategy)
            if symbol:
                query += " AND symbol = %s"
                params.append(symbol)

            query += " ORDER BY updated_at DESC LIMIT 100"

            rows = _safe_query(query, params)

            return jsonify({
                'calibration_records': len(rows),
                'records': _serialize_rows(rows),
                'filters': {'strategy_type': strategy, 'symbol': symbol}
            }), 200
        except Exception as e:
            log.error(f"GET /calibration error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/liquidity', methods=['GET'])
    def get_liquidity():
        """GET /strategies/liquidity - Liquidity monitoring data"""
        try:
            symbol = request.args.get('symbol')

            query = "SELECT * FROM strategy_liquidity_monitor WHERE 1=1"
            params = []

            if symbol:
                query += " AND symbol = %s"
                params.append(symbol)

            query += " ORDER BY timestamp DESC LIMIT 100"

            rows = _safe_query(query, params)

            return jsonify({
                'liquidity_records': len(rows),
                'records': _serialize_rows(rows),
                'filters': {'symbol': symbol}
            }), 200
        except Exception as e:
            log.error(f"GET /liquidity error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/scorecard', methods=['GET'])
    def get_scorecard():
        """GET /strategies/scorecard - Scorecards per strategy/asset"""
        try:
            strategy = request.args.get('strategy_type')
            symbol = request.args.get('symbol')

            query = "SELECT * FROM strategy_scorecards WHERE 1=1"
            params = []

            if strategy:
                query += " AND strategy_type = %s"
                params.append(strategy)
            if symbol:
                query += " AND symbol = %s"
                params.append(symbol)

            query += " ORDER BY timestamp DESC LIMIT 100"

            rows = _safe_query(query, params)

            return jsonify({
                'scorecard_count': len(rows),
                'scorecards': _serialize_rows(rows),
                'filters': {'strategy_type': strategy, 'symbol': symbol}
            }), 200
        except Exception as e:
            log.error(f"GET /scorecard error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/health', methods=['GET'])
    def get_health():
        """GET /strategies/health - Health of all strategy loops"""
        try:
            strategies = [
                'pcp_scan_loop',
                'fst_scan_loop',
                'roll_arb_scan_loop',
                'etf_basket_scan_loop',
                'skew_arb_scan_loop',
                'interlisted_scan_loop',
                'dividend_arb_scan_loop',
                'vol_arb_scan_loop'
            ]

            health_status = {}
            now = datetime.utcnow()

            for strat in strategies:
                row = _safe_query(
                    """
                    SELECT MAX(timestamp) as last_heartbeat, COUNT(*) as opp_count
                    FROM strategy_opportunities_log
                    WHERE strategy_type = %s
                    AND timestamp > NOW() - INTERVAL 1 HOUR
                    """,
                    (strat,),
                    fetch='one'
                )

                last_hb = row.get('last_heartbeat') if row else None
                opp_count = row.get('opp_count', 0) if row else 0

                is_healthy = True
                last_hb_str = None
                if not last_hb:
                    is_healthy = False
                else:
                    if isinstance(last_hb, str):
                        last_hb_time = datetime.fromisoformat(last_hb)
                    else:
                        last_hb_time = last_hb  # already datetime from MySQL
                    last_hb_str = last_hb_time.isoformat()
                    if (now - last_hb_time).total_seconds() > 300:
                        is_healthy = False

                health_status[strat] = {
                    'healthy': is_healthy,
                    'last_heartbeat': last_hb_str,
                    'opportunities_1h': opp_count
                }

            all_healthy = all(h['healthy'] for h in health_status.values())

            # Include capital info from config
            try:
                from modules.derivatives.config import get_config
                cfg = get_config()
                capital_info = {
                    'initial_capital': cfg.initial_capital,
                    'max_daily_loss_global': cfg.max_daily_loss_global,
                    'mode': cfg.derivatives_mode,
                }
            except Exception:
                capital_info = {}

            return jsonify({
                'overall_health': 'HEALTHY' if all_healthy else 'DEGRADED',
                'strategies': health_status,
                'capital': capital_info,
                'timestamp': datetime.utcnow().isoformat()
            }), 200
        except Exception as e:
            log.error(f"GET /health error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/capital', methods=['GET'])
    def get_capital():
        """GET /strategies/capital - Derivatives capital allocation and usage"""
        try:
            from modules.derivatives.config import get_config
            cfg = get_config()

            # Sum max_notional across all strategies
            total_allocated = 0
            strategy_allocations = {}

            for strat_name in cfg.active_strategies:
                strat_cfg = cfg.get_strategy_config(strat_name)
                notional = strat_cfg.get('max_notional', 0)
                total_allocated += notional
                strategy_allocations[strat_name] = {
                    'max_notional': notional,
                    'max_daily_loss': strat_cfg.get('max_daily_loss', 0),
                    'enabled': strat_cfg.get('enabled', True),
                    'max_positions': strat_cfg.get('max_positions', 0),
                }

            return jsonify({
                'initial_capital': cfg.initial_capital,
                'total_allocated_notional': total_allocated,
                'available_reserve': max(0, cfg.initial_capital - total_allocated),
                'utilization_pct': round(total_allocated / cfg.initial_capital * 100, 1) if cfg.initial_capital > 0 else 0,
                'max_daily_loss_global': cfg.max_daily_loss_global,
                'mode': cfg.derivatives_mode,
                'strategies': strategy_allocations,
                'timestamp': datetime.utcnow().isoformat()
            }), 200
        except Exception as e:
            log.error(f"GET /capital error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/status', methods=['GET'])
    def get_status():
        """GET /strategies/status - Active status per (asset, strategy)"""
        try:
            symbol = request.args.get('symbol')

            query = "SELECT * FROM active_status_registry WHERE 1=1"
            params = []

            if symbol:
                query += " AND symbol = %s"
                params.append(symbol)

            query += " ORDER BY symbol ASC"

            rows = _safe_query(query, params)

            return jsonify({
                'status_count': len(rows),
                'statuses': _serialize_rows(rows),
                'filters': {'symbol': symbol}
            }), 200
        except Exception as e:
            log.error(f"GET /status error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/promotion', methods=['POST'])
    def post_promotion():
        """POST /strategies/promotion - Manual promote/demote an asset/strategy"""
        try:
            data = request.get_json() or {}
            symbol = data.get('symbol')
            strategy = data.get('strategy')
            action = data.get('action')  # 'promote' or 'demote'

            if not all([symbol, strategy, action]):
                return jsonify({'error': 'Missing symbol, strategy, or action'}), 400

            if action not in ['promote', 'demote']:
                return jsonify({'error': 'action must be promote or demote'}), 400

            # Get current status
            row = _safe_query(
                "SELECT active_status FROM active_status_registry WHERE symbol = %s AND strategy_type = %s",
                (symbol, strategy),
                fetch='one'
            )
            current_status = row.get('active_status', 0) if row else 0

            # Adjust status
            if action == 'promote':
                new_status = min(current_status + 1, 3)  # Max PAPER_FULL = 3
            else:
                new_status = max(current_status - 1, 0)  # Min OBSERVE = 0

            # Upsert
            _safe_write(
                """
                INSERT INTO active_status_registry (symbol, strategy_type, active_status, updated_at)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE active_status = VALUES(active_status), updated_at = VALUES(updated_at)
                """,
                (symbol, strategy, new_status, datetime.utcnow())
            )

            status_labels = {0: 'OBSERVE', 1: 'SHADOW_EXEC', 2: 'PAPER_SMALL', 3: 'PAPER_FULL'}

            log.info(f"Promotion: {symbol}/{strategy} {status_labels.get(current_status)} -> {status_labels.get(new_status)}")

            return jsonify({
                'symbol': symbol,
                'strategy': strategy,
                'previous_status': status_labels.get(current_status),
                'new_status': status_labels.get(new_status),
                'action': action
            }), 200
        except Exception as e:
            log.error(f"POST /promotion error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/liquidity-score', methods=['GET'])
    def get_liquidity_score():
        """GET /strategies/liquidity-score - Current liquidity scores"""
        try:
            symbol = request.args.get('symbol')

            liquidity_engine = services_dict.get('liquidity_engine')
            if not liquidity_engine:
                # Fallback: read from DB
                query = """
                    SELECT symbol, overall_score, timestamp
                    FROM liquidity_score_history
                    WHERE timestamp = (SELECT MAX(timestamp) FROM liquidity_score_history lsh2 WHERE lsh2.symbol = liquidity_score_history.symbol)
                """
                params = []
                if symbol:
                    query = """
                        SELECT symbol, overall_score, timestamp
                        FROM liquidity_score_history
                        WHERE symbol = %s
                        ORDER BY timestamp DESC LIMIT 1
                    """
                    params = [symbol]

                rows = _safe_query(query, params)
                scores = {r['symbol']: float(r.get('overall_score', 0)) for r in rows}

                return jsonify({
                    'liquidity_scores': scores,
                    'source': 'database',
                    'timestamp': datetime.utcnow().isoformat()
                }), 200

            scores = {}
            if symbol:
                score = liquidity_engine.get_liquidity_score(symbol)
                scores[symbol] = score if score is not None else 0
            else:
                try:
                    from modules.derivatives.config import get_config
                    cfg = get_config()
                    universe = cfg.UNIVERSE_TIER_A if hasattr(cfg, 'UNIVERSE_TIER_A') else [
                        'PETR4', 'VALE3', 'BOVA11', 'ITUB4', 'BBDC4', 'BBAS3', 'ABEV3', 'B3SA3'
                    ]
                except Exception:
                    universe = ['PETR4', 'VALE3', 'BOVA11', 'ITUB4', 'BBDC4', 'BBAS3', 'ABEV3', 'B3SA3']

                for asset in universe:
                    score = liquidity_engine.get_liquidity_score(asset)
                    scores[asset] = score if score is not None else 0

            return jsonify({
                'liquidity_scores': scores,
                'source': 'engine',
                'timestamp': datetime.utcnow().isoformat()
            }), 200
        except Exception as e:
            log.error(f"GET /liquidity-score error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/execution-plausibility', methods=['GET'])
    def get_execution_plausibility():
        """GET /strategies/execution-plausibility - Execution plausibility logs"""
        try:
            symbol = request.args.get('symbol')
            strategy = request.args.get('strategy_type')
            limit = request.args.get('limit', 100, type=int)
            offset = request.args.get('offset', 0, type=int)

            query = "SELECT * FROM execution_plausibility_log WHERE 1=1"
            params = []

            if symbol:
                query += " AND symbol = %s"
                params.append(symbol)
            if strategy:
                query += " AND strategy_type = %s"
                params.append(strategy)

            query += " ORDER BY timestamp DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])

            rows = _safe_query(query, params)

            return jsonify({
                'execution_plausibility_count': len(rows),
                'logs': _serialize_rows(rows),
                'pagination': {'limit': limit, 'offset': offset},
                'filters': {'symbol': symbol, 'strategy_type': strategy}
            }), 200
        except Exception as e:
            log.error(f"GET /execution-plausibility error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    # ============== P&L ENDPOINTS ==============

    @strategies_bp.route('/pnl-daily', methods=['GET'])
    def get_pnl_daily():
        """GET /strategies/pnl-daily?start_date=2024-01-01&end_date=2024-12-31&strategy=pcp"""
        try:
            start_date = request.args.get('start_date', '')
            end_date = request.args.get('end_date', '')
            strategy = request.args.get('strategy', '')

            query = """
                SELECT DATE(closed_at) as trade_date, strategy_type,
                       COUNT(*) as trade_count,
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses,
                       SUM(pnl) as total_pnl,
                       AVG(pnl) as avg_pnl,
                       MIN(pnl) as worst_trade,
                       MAX(pnl) as best_trade
                FROM strategy_master_trades
                WHERE status = 'CLOSED' AND closed_at IS NOT NULL
            """
            params = []
            if start_date:
                query += " AND closed_at >= %s"
                params.append(start_date)
            if end_date:
                query += " AND closed_at <= %s"
                params.append(end_date + ' 23:59:59')
            if strategy:
                query += " AND strategy_type = %s"
                params.append(strategy)

            query += " GROUP BY DATE(closed_at), strategy_type ORDER BY trade_date DESC"
            rows = _safe_query(query, params)

            # Calculate running totals
            cumulative_pnl = 0
            for row in reversed(rows):
                cumulative_pnl += float(row.get('total_pnl', 0) or 0)
                row['cumulative_pnl'] = cumulative_pnl
                tc = row.get('trade_count', 0) or 1
                row['win_rate'] = float(row.get('wins', 0) or 0) / tc

            return jsonify({
                'period': 'daily',
                'count': len(rows),
                'data': _serialize_rows(rows),
                'filters': {'start_date': start_date, 'end_date': end_date, 'strategy': strategy},
            }), 200
        except Exception as e:
            log.error(f"GET /pnl-daily error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/pnl-monthly', methods=['GET'])
    def get_pnl_monthly():
        """GET /strategies/pnl-monthly?year=2024&strategy=pcp"""
        try:
            year = request.args.get('year', '')
            strategy = request.args.get('strategy', '')

            query = """
                SELECT DATE_FORMAT(closed_at, '%%Y-%%m') as year_month,
                       strategy_type,
                       COUNT(*) as trade_count,
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses,
                       SUM(pnl) as total_pnl,
                       AVG(pnl) as avg_pnl,
                       MIN(pnl) as worst_trade,
                       MAX(pnl) as best_trade,
                       AVG(expected_edge) as avg_expected_edge,
                       AVG(slippage) as avg_slippage
                FROM strategy_master_trades
                WHERE status = 'CLOSED' AND closed_at IS NOT NULL
            """
            params = []
            if year:
                query += " AND YEAR(closed_at) = %s"
                params.append(int(year))
            if strategy:
                query += " AND strategy_type = %s"
                params.append(strategy)

            query += " GROUP BY year_month, strategy_type ORDER BY year_month DESC"
            rows = _safe_query(query, params)

            for row in rows:
                tc = row.get('trade_count', 0) or 1
                row['win_rate'] = float(row.get('wins', 0) or 0) / tc
                wins_pnl = float(row.get('best_trade', 0) or 0)
                losses_pnl = abs(float(row.get('worst_trade', 0) or 0))
                row['profit_factor'] = wins_pnl / losses_pnl if losses_pnl > 0 else 0

            return jsonify({
                'period': 'monthly',
                'count': len(rows),
                'data': _serialize_rows(rows),
                'filters': {'year': year, 'strategy': strategy},
            }), 200
        except Exception as e:
            log.error(f"GET /pnl-monthly error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/pnl-annual', methods=['GET'])
    def get_pnl_annual():
        """GET /strategies/pnl-annual?strategy=pcp"""
        try:
            strategy = request.args.get('strategy', '')

            query = """
                SELECT YEAR(closed_at) as year, strategy_type,
                       COUNT(*) as trade_count,
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses,
                       SUM(pnl) as total_pnl,
                       AVG(pnl) as avg_pnl,
                       MIN(pnl) as worst_trade,
                       MAX(pnl) as best_trade
                FROM strategy_master_trades
                WHERE status = 'CLOSED' AND closed_at IS NOT NULL
            """
            params = []
            if strategy:
                query += " AND strategy_type = %s"
                params.append(strategy)

            query += " GROUP BY year, strategy_type ORDER BY year DESC"
            rows = _safe_query(query, params)

            for row in rows:
                tc = row.get('trade_count', 0) or 1
                row['win_rate'] = float(row.get('wins', 0) or 0) / tc

            return jsonify({
                'period': 'annual',
                'count': len(rows),
                'data': _serialize_rows(rows),
                'filters': {'strategy': strategy},
            }), 200
        except Exception as e:
            log.error(f"GET /pnl-annual error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    # ============== WIN RATE ENDPOINT ==============

    @strategies_bp.route('/win-rate', methods=['GET'])
    def get_win_rate():
        """GET /strategies/win-rate?strategy=pcp&symbol=PETR4&period=30"""
        try:
            strategy = request.args.get('strategy', '')
            symbol = request.args.get('symbol', '')
            period_days = int(request.args.get('period', 90))

            query = """
                SELECT strategy_type, symbol,
                       COUNT(*) as total_trades,
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses,
                       SUM(pnl) as total_pnl,
                       AVG(pnl) as avg_pnl,
                       AVG(expected_edge) as avg_edge_expected,
                       AVG(CASE WHEN pnl > 0 THEN pnl ELSE NULL END) as avg_win,
                       AVG(CASE WHEN pnl < 0 THEN pnl ELSE NULL END) as avg_loss
                FROM strategy_master_trades
                WHERE status = 'CLOSED'
                  AND closed_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
            """
            params = [period_days]
            if strategy:
                query += " AND strategy_type = %s"
                params.append(strategy)
            if symbol:
                query += " AND symbol = %s"
                params.append(symbol)

            query += " GROUP BY strategy_type, symbol ORDER BY total_trades DESC"
            rows = _safe_query(query, params)

            for row in rows:
                tc = row.get('total_trades', 0) or 1
                row['win_rate'] = float(row.get('wins', 0) or 0) / tc
                avg_w = float(row.get('avg_win', 0) or 0)
                avg_l = abs(float(row.get('avg_loss', 0) or -1))
                row['reward_risk_ratio'] = avg_w / avg_l if avg_l > 0 else 0
                row['expectancy'] = (
                    row['win_rate'] * avg_w - (1 - row['win_rate']) * avg_l
                )

            return jsonify({
                'period_days': period_days,
                'count': len(rows),
                'data': _serialize_rows(rows),
                'filters': {'strategy': strategy, 'symbol': symbol},
            }), 200
        except Exception as e:
            log.error(f"GET /win-rate error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    # ============== LEARNING & POSITIONS ENDPOINTS ==============

    @strategies_bp.route('/learning', methods=['GET'])
    def get_learning_stats():
        """GET /strategies/learning?strategy=pcp"""
        try:
            strategy = request.args.get('strategy', '')

            # Try to get learning engine from services_dict
            learner = services_dict.get('deriv_learner')
            if not learner:
                return jsonify({'error': 'Learning engine not initialized'}), 503

            summary = learner.get_learning_summary()
            stats = learner.get_strategy_stats(strategy or None)

            # Serialize stats
            stats_data = {}
            for key, s in stats.items():
                stats_data[key] = {
                    'trade_count': s.trade_count,
                    'win_count': s.win_count,
                    'loss_count': s.loss_count,
                    'win_rate': round(s.win_rate, 4),
                    'total_pnl': round(s.total_pnl, 2),
                    'sharpe': round(s.sharpe, 4),
                    'profit_factor': round(s.profit_factor, 4),
                    'max_drawdown': round(s.max_drawdown, 2),
                    'confidence_adjustment': round(s.confidence_adjustment, 4),
                    'avg_slippage': round(s.avg_slippage, 4),
                    'avg_latency_ms': round(s.avg_latency_ms, 2),
                    'edge_accuracy': round(s.edge_accuracy, 4),
                    'legging_rate': round(s.legging_rate, 4),
                    'last_updated': s.last_updated.isoformat() if s.last_updated else None,
                }

            return jsonify({
                'summary': summary,
                'strategies': stats_data,
                'filters': {'strategy': strategy},
            }), 200
        except Exception as e:
            log.error(f"GET /learning error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/positions', methods=['GET'])
    def get_active_positions():
        """GET /strategies/positions?strategy=pcp"""
        try:
            strategy = request.args.get('strategy', '')

            monitor = services_dict.get('deriv_monitor')
            exec_engine = services_dict.get('deriv_execution')

            if not exec_engine:
                return jsonify({'error': 'Execution engine not initialized'}), 503

            trades = exec_engine.get_active_trades(strategy or None)
            positions = []

            for trade in trades:
                pos = {
                    'trade_id': trade.trade_id,
                    'strategy': trade.strategy,
                    'symbol': trade.symbol,
                    'structure_type': trade.structure_type,
                    'notional': trade.notional,
                    'expected_edge': trade.expected_edge,
                    'status': trade.status.value,
                    'opened_at': trade.opened_at.isoformat() if trade.opened_at else None,
                    'legs_count': len(trade.legs),
                    'liquidity_score': trade.liquidity_score,
                    'active_status': trade.active_status,
                }

                # Add snapshot data if monitor available
                if monitor:
                    snap = monitor.get_position_snapshot(trade.trade_id)
                    if snap:
                        pos['unrealized_pnl'] = snap.unrealized_pnl
                        pos['unrealized_pnl_pct'] = snap.unrealized_pnl_pct
                        pos['delta'] = snap.delta
                        pos['gamma'] = snap.gamma
                        pos['theta'] = snap.theta
                        pos['vega'] = snap.vega
                        pos['time_in_trade_hours'] = snap.time_in_trade_hours
                        pos['days_to_expiry'] = snap.days_to_expiry

                positions.append(pos)

            return jsonify({
                'count': len(positions),
                'positions': positions,
                'filters': {'strategy': strategy},
            }), 200
        except Exception as e:
            log.error(f"GET /positions error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/trades-closed', methods=['GET'])
    def get_closed_trades():
        """GET /strategies/trades-closed?strategy=pcp&limit=100"""
        try:
            strategy = request.args.get('strategy', '')
            symbol = request.args.get('symbol', '')
            limit = int(request.args.get('limit', 100))
            offset = int(request.args.get('offset', 0))

            query = """
                SELECT trade_id, strategy_type, symbol, structure_type,
                       expected_edge, pnl, slippage, latency_ms,
                       opened_at, closed_at, close_reason,
                       liquidity_score, active_status
                FROM strategy_master_trades
                WHERE status = 'CLOSED'
            """
            params = []
            if strategy:
                query += " AND strategy_type = %s"
                params.append(strategy)
            if symbol:
                query += " AND symbol = %s"
                params.append(symbol)

            query += " ORDER BY closed_at DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            rows = _safe_query(query, params)

            return jsonify({
                'count': len(rows),
                'trades': _serialize_rows(rows),
                'pagination': {'limit': limit, 'offset': offset},
            }), 200
        except Exception as e:
            log.error(f"GET /trades-closed error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/exit-signals', methods=['GET'])
    def get_exit_signals():
        """GET /strategies/exit-signals"""
        try:
            monitor = services_dict.get('deriv_monitor')
            if not monitor:
                return jsonify({'error': 'Monitor not initialized'}), 503

            signals = monitor.get_pending_exits()
            stats = monitor.get_stats()

            return jsonify({
                'count': len(signals),
                'signals': [
                    {
                        'trade_id': s.trade_id,
                        'trigger': s.trigger,
                        'reason': s.reason,
                        'urgency': s.urgency,
                        'estimated_pnl': s.estimated_pnl,
                        'timestamp': s.timestamp.isoformat(),
                    }
                    for s in signals
                ],
                'stats': stats,
            }), 200
        except Exception as e:
            log.error(f"GET /exit-signals error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    # ============== SYSTEM STATE ENDPOINT ==============

    @strategies_bp.route('/system-state', methods=['GET'])
    def get_system_state():
        """
        GET /strategies/system-state
        Returns comprehensive system state for UI rendering:
        - market_open / market_phase
        - per-strategy scan loop heartbeats, calibration progress, tier status
        - provider health
        - capital snapshot
        - kill switch state
        - learning engine summary
        - smart status descriptions for the UI
        """
        try:
            from datetime import time as dtime
            now = datetime.utcnow()
            # B3 market hours: 10:00-17:00 BRT = 13:00-20:00 UTC
            brt_offset = timedelta(hours=-3)
            brt_now = now + brt_offset
            brt_time = brt_now.time()
            is_weekday = brt_now.weekday() < 5

            market_open = False
            market_phase = 'CLOSED'
            if is_weekday:
                if dtime(9, 45) <= brt_time < dtime(10, 0):
                    market_phase = 'PRE_MARKET'
                elif dtime(10, 0) <= brt_time < dtime(17, 0):
                    market_open = True
                    market_phase = 'OPEN'
                elif dtime(17, 0) <= brt_time < dtime(17, 30):
                    market_phase = 'AFTER_MARKET'
                elif dtime(17, 30) <= brt_time < dtime(18, 0):
                    market_phase = 'CLOSING'
                else:
                    market_phase = 'CLOSED'
            else:
                market_phase = 'WEEKEND'

            # --- Per-strategy state ---
            strategy_keys = ['PCP', 'FST', 'ROLL_ARB', 'ETF_BASKET', 'SKEW_ARB', 'INTERLISTED', 'DIVIDEND_ARB', 'VOL_ARB']
            scan_loop_map = {
                'PCP': 'pcp_scan_loop', 'FST': 'fst_scan_loop', 'ROLL_ARB': 'roll_arb_scan_loop',
                'ETF_BASKET': 'etf_basket_scan_loop', 'SKEW_ARB': 'skew_arb_scan_loop',
                'INTERLISTED': 'interlisted_scan_loop', 'DIVIDEND_ARB': 'dividend_arb_scan_loop',
                'VOL_ARB': 'vol_arb_scan_loop'
            }
            strategies_state = {}

            for sk in strategy_keys:
                loop_name = scan_loop_map[sk]

                # Heartbeat: last opportunity seen
                hb_row = _safe_query(
                    """SELECT MAX(timestamp) as last_hb, COUNT(*) as opp_count
                       FROM strategy_opportunities_log
                       WHERE strategy_type = %s AND timestamp > NOW() - INTERVAL 1 HOUR""",
                    (loop_name,), fetch='one'
                )
                # Also check with strategy key directly
                if not hb_row or not hb_row.get('last_hb'):
                    hb_row = _safe_query(
                        """SELECT MAX(timestamp) as last_hb, COUNT(*) as opp_count
                           FROM strategy_opportunities_log
                           WHERE strategy_type = %s AND timestamp > NOW() - INTERVAL 1 HOUR""",
                        (sk,), fetch='one'
                    )

                last_hb = hb_row.get('last_hb') if hb_row else None
                opp_count = hb_row.get('opp_count', 0) if hb_row else 0

                # Calibration: count calibration records
                cal_row = _safe_query(
                    """SELECT COUNT(*) as cal_count, MAX(updated_at) as last_cal
                       FROM calibration_data
                       WHERE strategy_type = %s""",
                    (sk,), fetch='one'
                )
                cal_count = cal_row.get('cal_count', 0) if cal_row else 0
                last_cal = cal_row.get('last_cal') if cal_row else None

                # Active status registry: best tier for this strategy
                tier_row = _safe_query(
                    """SELECT current_status, symbol, days_in_status, liquidity_score_avg
                       FROM active_status_registry
                       WHERE strategy_type = %s ORDER BY current_status DESC LIMIT 1""",
                    (sk,), fetch='one'
                )
                # Also check with loop name
                if not tier_row:
                    tier_row = _safe_query(
                        """SELECT current_status, symbol, days_in_status, liquidity_score_avg
                           FROM active_status_registry
                           WHERE strategy_type = %s ORDER BY current_status DESC LIMIT 1""",
                        (loop_name,), fetch='one'
                    )

                best_tier = tier_row.get('current_status', 'OBSERVE') if tier_row else 'OBSERVE'
                tier_symbol = tier_row.get('symbol') if tier_row else None
                days_in_status = tier_row.get('days_in_status', 0) if tier_row else 0

                # Trades: recent activity
                trades_row = _safe_query(
                    """SELECT COUNT(*) as total, SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) as open_count,
                              SUM(pnl) as total_pnl
                       FROM strategy_master_trades
                       WHERE strategy_type = %s""",
                    (sk,), fetch='one'
                )
                total_trades = trades_row.get('total', 0) if trades_row else 0
                open_trades = trades_row.get('open_count', 0) if trades_row else 0
                total_pnl = float(trades_row.get('total_pnl', 0) or 0) if trades_row else 0.0

                # Determine scan loop health
                loop_healthy = False
                seconds_since_hb = None
                if last_hb:
                    hb_time = last_hb if isinstance(last_hb, datetime) else datetime.fromisoformat(str(last_hb))
                    seconds_since_hb = (now - hb_time).total_seconds()
                    loop_healthy = seconds_since_hb < 300

                # Smart status description
                if not market_open and market_phase in ('CLOSED', 'WEEKEND'):
                    smart_status = 'Mercado fechado — aguardando abertura'
                    smart_icon = 'moon'
                elif not loop_healthy and market_open:
                    smart_status = 'Scan loop sem heartbeat — verificar'
                    smart_icon = 'alert'
                elif cal_count == 0:
                    smart_status = 'Calibrando — coletando dados iniciais'
                    smart_icon = 'calibrating'
                elif opp_count == 0 and market_open:
                    smart_status = 'Monitorando — sem oportunidades no momento'
                    smart_icon = 'scanning'
                elif opp_count > 0 and open_trades > 0:
                    smart_status = f'Operando — {open_trades} posição(ões) aberta(s)'
                    smart_icon = 'active'
                elif opp_count > 0:
                    smart_status = f'{opp_count} oportunidade(s) detectada(s)'
                    smart_icon = 'opportunity'
                else:
                    smart_status = 'Standby — sem sinal'
                    smart_icon = 'idle'

                strategies_state[sk] = {
                    'loop_healthy': loop_healthy,
                    'last_heartbeat': last_hb.isoformat() if isinstance(last_hb, datetime) else str(last_hb) if last_hb else None,
                    'seconds_since_heartbeat': round(seconds_since_hb, 0) if seconds_since_hb else None,
                    'opportunities_1h': opp_count,
                    'calibration_records': cal_count,
                    'last_calibration': last_cal.isoformat() if isinstance(last_cal, datetime) else str(last_cal) if last_cal else None,
                    'best_tier': best_tier,
                    'tier_symbol': tier_symbol,
                    'days_in_tier': days_in_status,
                    'total_trades': total_trades,
                    'open_trades': open_trades,
                    'total_pnl': total_pnl,
                    'smart_status': smart_status,
                    'smart_icon': smart_icon,
                }

            # --- Provider health ---
            providers_health = {}
            if provider_mgr:
                try:
                    for pname in ['brapi', 'oplab', 'polygon']:
                        try:
                            ph = provider_mgr.get_provider_health(pname) if hasattr(provider_mgr, 'get_provider_health') else None
                            if ph:
                                providers_health[pname] = ph
                            else:
                                providers_health[pname] = {'status': 'unknown', 'last_check': None}
                        except Exception:
                            providers_health[pname] = {'status': 'unknown', 'last_check': None}
                except Exception:
                    pass

            # --- Capital snapshot ---
            capital_data = {}
            capital_mgr = services_dict.get('capital_manager')
            if capital_mgr:
                try:
                    snap = capital_mgr.get_snapshot()
                    capital_data = {
                        'total_capital': snap.total_capital,
                        'allocated': snap.allocated,
                        'available': snap.available,
                        'daily_pnl': snap.daily_pnl,
                        'daily_loss_remaining': snap.daily_loss_remaining,
                        'positions_count': snap.positions_count,
                    }
                except Exception:
                    pass

            # --- Kill switch / config ---
            kill_switch = False
            mode = 'PAPER'
            try:
                from modules.derivatives.config import get_config
                cfg = get_config()
                kill_switch = getattr(cfg, 'kill_switch', False)
                mode = getattr(cfg, 'derivatives_mode', 'PAPER')
            except Exception:
                pass

            # --- Learning summary ---
            learning_data = {}
            learner = services_dict.get('deriv_learner')
            if learner:
                try:
                    learning_data = learner.get_learning_summary()
                except Exception:
                    pass

            # --- DB health ---
            db_healthy = False
            try:
                test_row = _safe_query("SELECT 1 as ok", fetch='one')
                db_healthy = test_row is not None and test_row.get('ok') == 1
            except Exception:
                pass

            # --- Overall smart status ---
            healthy_count = sum(1 for s in strategies_state.values() if s['loop_healthy'])
            if kill_switch:
                overall_smart = 'Kill switch ativado — execução suspensa'
                overall_icon = 'kill'
            elif not market_open:
                overall_smart = f'Mercado {market_phase.lower()} — sistema em standby'
                overall_icon = 'standby'
            elif healthy_count == 8:
                overall_smart = 'Todas as estratégias operacionais'
                overall_icon = 'healthy'
            elif healthy_count > 0:
                overall_smart = f'{healthy_count}/8 estratégias ativas — verificar degradadas'
                overall_icon = 'degraded'
            else:
                overall_smart = 'Nenhum scan loop ativo — verificar sistema'
                overall_icon = 'critical'

            return jsonify({
                'market': {
                    'open': market_open,
                    'phase': market_phase,
                    'brt_time': brt_now.strftime('%H:%M:%S'),
                    'brt_date': brt_now.strftime('%Y-%m-%d'),
                    'is_weekday': is_weekday,
                },
                'overall': {
                    'smart_status': overall_smart,
                    'smart_icon': overall_icon,
                    'healthy_strategies': healthy_count,
                    'total_strategies': 8,
                    'kill_switch': kill_switch,
                    'mode': mode,
                    'db_healthy': db_healthy,
                },
                'strategies': strategies_state,
                'providers': providers_health,
                'capital': capital_data,
                'learning': learning_data,
                'timestamp': now.isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /system-state error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @strategies_bp.route('/capital-summary', methods=['GET'])
    def get_capital_summary():
        """GET /strategies/capital-summary - Detailed capital state"""
        try:
            capital_mgr = services_dict.get('capital_manager')
            if not capital_mgr:
                return jsonify({'error': 'Capital manager not initialized'}), 503

            snapshot = capital_mgr.get_snapshot()
            strategy_summary = capital_mgr.get_strategy_summary()

            return jsonify({
                'snapshot': {
                    'total_capital': snapshot.total_capital,
                    'allocated': snapshot.allocated,
                    'available': snapshot.available,
                    'daily_pnl': snapshot.daily_pnl,
                    'daily_loss_remaining': snapshot.daily_loss_remaining,
                    'positions_count': snapshot.positions_count,
                    'timestamp': snapshot.timestamp.isoformat(),
                },
                'strategies': strategy_summary,
            }), 200
        except Exception as e:
            log.error(f"GET /capital-summary error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    return strategies_bp
