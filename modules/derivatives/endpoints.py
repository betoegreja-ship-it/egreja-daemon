"""
Derivatives Strategy Endpoints
Flask Blueprint with 17 endpoints for strategy monitoring and management
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
import traceback


def create_strategies_blueprint(db_fn, log, provider_mgr, services_dict):
    """
    Factory function to create Flask Blueprint with dependencies
    """
    strategies_bp = Blueprint('strategies', __name__, url_prefix='/strategies')
    
    # ============== HELPER FUNCTIONS ==============
    
    def get_db():
        return db_fn()
    
    def query_opportunities(strategy_type=None, symbol=None, limit=100, offset=0, 
                           start_date=None, end_date=None):
        """Generic opportunity query"""
        db = get_db()
        query = "SELECT * FROM strategy_opportunities_log WHERE 1=1"
        params = []
        
        if strategy_type:
            query += " AND strategy = ?"
            params.append(strategy_type)
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        if start_date:
            query += " AND created_at >= ?"
            params.append(start_date)
        if end_date:
            query += " AND created_at <= ?"
            params.append(end_date)
        
        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        try:
            cursor = db.execute(query, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows] if rows else []
        except Exception as e:
            log.warning(f"Query error: {e}")
            return []
    
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
                'opportunities': opps,
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
                'opportunities': opps,
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
                'opportunities': opps,
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
                'opportunities': opps,
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
                'opportunities': opps,
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
                'opportunities': opps,
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
                'opportunities': opps,
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
                'opportunities': opps,
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
                strat = opp.get('strategy', 'UNKNOWN')
                if strat not in by_strategy:
                    by_strategy[strat] = []
                by_strategy[strat].append(opp)
            
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
            db = get_db()
            strategy = request.args.get('strategy_type')
            symbol = request.args.get('symbol')
            
            query = "SELECT * FROM strategy_calibration_log WHERE 1=1"
            params = []
            
            if strategy:
                query += " AND strategy = ?"
                params.append(strategy)
            if symbol:
                query += " AND symbol = ?"
                params.append(symbol)
            
            query += " ORDER BY created_at DESC LIMIT 100"
            
            cursor = db.execute(query, params)
            rows = cursor.fetchall()
            calibration = [dict(row) for row in rows] if rows else []
            
            return jsonify({
                'calibration_records': len(calibration),
                'records': calibration,
                'filters': {'strategy_type': strategy, 'symbol': symbol}
            }), 200
        except Exception as e:
            log.error(f"GET /calibration error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500
    
    @strategies_bp.route('/liquidity', methods=['GET'])
    def get_liquidity():
        """GET /strategies/liquidity - Liquidity monitoring data"""
        try:
            db = get_db()
            symbol = request.args.get('symbol')
            
            query = "SELECT * FROM liquidity_monitoring WHERE 1=1"
            params = []
            
            if symbol:
                query += " AND symbol = ?"
                params.append(symbol)
            
            query += " ORDER BY created_at DESC LIMIT 100"
            
            cursor = db.execute(query, params)
            rows = cursor.fetchall()
            liquidity = [dict(row) for row in rows] if rows else []
            
            return jsonify({
                'liquidity_records': len(liquidity),
                'records': liquidity,
                'filters': {'symbol': symbol}
            }), 200
        except Exception as e:
            log.error(f"GET /liquidity error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500
    
    @strategies_bp.route('/scorecard', methods=['GET'])
    def get_scorecard():
        """GET /strategies/scorecard - Scorecards per strategy/asset"""
        try:
            db = get_db()
            strategy = request.args.get('strategy_type')
            symbol = request.args.get('symbol')
            
            query = "SELECT * FROM strategy_scorecard WHERE 1=1"
            params = []
            
            if strategy:
                query += " AND strategy = ?"
                params.append(strategy)
            if symbol:
                query += " AND symbol = ?"
                params.append(symbol)
            
            query += " ORDER BY updated_at DESC LIMIT 100"
            
            cursor = db.execute(query, params)
            rows = cursor.fetchall()
            scorecards = [dict(row) for row in rows] if rows else []
            
            return jsonify({
                'scorecard_count': len(scorecards),
                'scorecards': scorecards,
                'filters': {'strategy_type': strategy, 'symbol': symbol}
            }), 200
        except Exception as e:
            log.error(f"GET /scorecard error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500
    
    @strategies_bp.route('/health', methods=['GET'])
    def get_health():
        """GET /strategies/health - Health of all strategy loops"""
        try:
            db = get_db()
            
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
                query = """
                SELECT MAX(created_at) as last_heartbeat, COUNT(*) as opp_count
                FROM strategy_opportunities_log
                WHERE strategy = ?
                AND created_at > datetime('now', '-1 hour')
                """
                cursor = db.execute(query, (strat,))
                row = cursor.fetchone()
                
                last_hb = row[0] if row else None
                opp_count = row[1] if row else 0
                
                is_healthy = True
                if not last_hb:
                    is_healthy = False
                else:
                    last_hb_time = datetime.fromisoformat(last_hb)
                    if (now - last_hb_time).total_seconds() > 300:  # 5 min threshold
                        is_healthy = False
                
                health_status[strat] = {
                    'healthy': is_healthy,
                    'last_heartbeat': last_hb,
                    'opportunities_1h': opp_count
                }
            
            all_healthy = all(h['healthy'] for h in health_status.values())
            
            return jsonify({
                'overall_health': 'HEALTHY' if all_healthy else 'DEGRADED',
                'strategies': health_status,
                'timestamp': datetime.utcnow().isoformat()
            }), 200
        except Exception as e:
            log.error(f"GET /health error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500
    
    @strategies_bp.route('/status', methods=['GET'])
    def get_status():
        """GET /strategies/status - Active status per (asset, strategy)"""
        try:
            db = get_db()
            symbol = request.args.get('symbol')
            
            query = "SELECT * FROM active_status_registry WHERE 1=1"
            params = []
            
            if symbol:
                query += " AND symbol = ?"
                params.append(symbol)
            
            query += " ORDER BY symbol ASC"
            
            cursor = db.execute(query, params)
            rows = cursor.fetchall()
            statuses = [dict(row) for row in rows] if rows else []
            
            return jsonify({
                'status_count': len(statuses),
                'statuses': statuses,
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
            
            db = get_db()
            
            # Get current status
            cursor = db.execute(
                "SELECT active_status FROM active_status_registry WHERE symbol = ? AND strategy = ?",
                (symbol, strategy)
            )
            row = cursor.fetchone()
            current_status = row[0] if row else 0
            
            # Adjust status
            new_status = current_status
            if action == 'promote':
                new_status = min(current_status + 1, 3)  # Max PAPER_FULL = 3
            else:
                new_status = max(current_status - 1, 0)  # Min OBSERVE = 0
            
            # Update database
            db.execute(
                """
                INSERT OR REPLACE INTO active_status_registry 
                (symbol, strategy, active_status, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (symbol, strategy, new_status, datetime.utcnow().isoformat())
            )
            db.commit()
            
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
                return jsonify({'error': 'Liquidity engine not available'}), 503
            
            scores = {}
            
            if symbol:
                score = liquidity_engine.get_liquidity_score(symbol)
                scores[symbol] = score if score is not None else 0
            else:
                from config import UNIVERSE_TIER_A
                for asset in UNIVERSE_TIER_A:
                    score = liquidity_engine.get_liquidity_score(asset)
                    scores[asset] = score if score is not None else 0
            
            return jsonify({
                'liquidity_scores': scores,
                'timestamp': datetime.utcnow().isoformat()
            }), 200
        except Exception as e:
            log.error(f"GET /liquidity-score error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500
    
    @strategies_bp.route('/execution-plausibility', methods=['GET'])
    def get_execution_plausibility():
        """GET /strategies/execution-plausibility - Execution plausibility logs"""
        try:
            db = get_db()
            symbol = request.args.get('symbol')
            strategy = request.args.get('strategy_type')
            limit = request.args.get('limit', 100, type=int)
            offset = request.args.get('offset', 0, type=int)
            
            query = "SELECT * FROM execution_plausibility_log WHERE 1=1"
            params = []
            
            if symbol:
                query += " AND symbol = ?"
                params.append(symbol)
            if strategy:
                query += " AND strategy = ?"
                params.append(strategy)
            
            query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            cursor = db.execute(query, params)
            rows = cursor.fetchall()
            logs = [dict(row) for row in rows] if rows else []
            
            return jsonify({
                'execution_plausibility_count': len(logs),
                'logs': logs,
                'pagination': {'limit': limit, 'offset': offset},
                'filters': {'symbol': symbol, 'strategy_type': strategy}
            }), 200
        except Exception as e:
            log.error(f"GET /execution-plausibility error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500
    
    return strategies_bp
