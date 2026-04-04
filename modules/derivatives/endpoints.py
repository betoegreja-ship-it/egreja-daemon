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

    return strategies_bp
