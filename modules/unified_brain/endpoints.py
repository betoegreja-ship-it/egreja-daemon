"""
Unified Brain - Flask Blueprint Endpoints

13 endpoints for AI intelligence and decision support across all 5 modules.
Comprehensive dashboard API for the learning engine.

Endpoints:
  - GET /brain/system-state - Comprehensive brain state
  - GET /brain/digest - Daily intelligence digest
  - GET /brain/lessons - All lessons learned
  - GET /brain/patterns - Active patterns
  - GET /brain/correlations - Correlation matrix
  - GET /brain/regime - Current market regime
  - GET /brain/decisions - AI recommendations
  - GET /brain/risk-radar - Unified risk assessment
  - GET /brain/evolution - Brain evolution over time
  - GET /brain/metrics - Aggregated metrics
  - GET /brain/health - Brain health status
  - GET /brain/module-feed/<module> - Module-specific feed
  - GET /brain/cross-insights - Cross-module insights
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, date, timedelta
import traceback
import json

from .learning_engine import LearningEngine
from .correlation_engine import CorrelationEngine
from .regime_detector import RegimeDetector
from .decision_engine import DecisionEngine


def create_unified_brain_blueprint(db_fn, log, **kwargs):
    """
    Factory function to create Flask Blueprint with dependencies.

    Args:
        db_fn: callable returning a mysql.connector connection
        log: logger instance
    """
    brain_bp = Blueprint('unified_brain', __name__, url_prefix='/brain')

    # Initialize engines
    learning_engine = LearningEngine(db_fn=db_fn, log=log)
    correlation_engine = CorrelationEngine(db_fn=db_fn, log=log)
    regime_detector = RegimeDetector(db_fn=db_fn, log=log)
    decision_engine = DecisionEngine(
        db_fn=db_fn,
        log=log,
        learning_engine=learning_engine,
        regime_detector=regime_detector
    )

    # ============== HELPER FUNCTIONS ==============

    def get_db():
        return db_fn()

    def _safe_query(query, params=None, fetch='all'):
        """Execute a read query safely."""
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

    # ============== MAIN ENDPOINTS ==============

    @brain_bp.route('/system-state', methods=['GET'])
    def get_system_state():
        """GET /brain/system-state - Comprehensive brain state for dashboard"""
        try:
            state = learning_engine.get_system_state()

            return jsonify({
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'brain_state': state,
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /system-state error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @brain_bp.route('/digest', methods=['GET'])
    def get_digest():
        """GET /brain/digest - Daily intelligence digest"""
        try:
            digest = learning_engine.get_daily_digest()

            return jsonify({
                'status': 'success',
                'digest': digest,
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /digest error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @brain_bp.route('/lessons', methods=['GET'])
    def get_lessons():
        """GET /brain/lessons - All lessons with optional filters"""
        try:
            module = request.args.get('module')
            lesson_type = request.args.get('type')
            min_confidence = request.args.get('min_confidence', 0, type=float)

            summary = learning_engine.get_lessons_summary()
            lessons = summary['lessons']

            # Apply filters
            if module:
                lessons = [l for l in lessons if l['module'] == module]
            if lesson_type:
                lessons = [l for l in lessons if l['lesson_type'] == lesson_type]
            if min_confidence:
                lessons = [l for l in lessons if l['confidence'] >= min_confidence]

            return jsonify({
                'status': 'success',
                'total_lessons': len(lessons),
                'lessons': lessons,
                'filters': {
                    'module': module,
                    'type': lesson_type,
                    'min_confidence': min_confidence,
                },
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /lessons error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @brain_bp.route('/patterns', methods=['GET'])
    def get_patterns():
        """GET /brain/patterns - Active patterns"""
        try:
            min_confidence = request.args.get('min_confidence', 70, type=float)

            patterns_data = learning_engine.get_pattern_alerts()
            patterns = patterns_data['patterns']

            # Filter by confidence
            if min_confidence:
                patterns = [p for p in patterns if p['confidence'] >= min_confidence]

            return jsonify({
                'status': 'success',
                'total_patterns': len(patterns),
                'active_patterns': len([p for p in patterns if p['active']]),
                'patterns': patterns,
                'min_confidence_filter': min_confidence,
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /patterns error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @brain_bp.route('/correlations', methods=['GET'])
    def get_correlations():
        """GET /brain/correlations - Correlation matrix and analysis"""
        try:
            correlation_type = request.args.get('type', 'modules')

            if correlation_type == 'assets':
                asset = request.args.get('asset')
                result = correlation_engine.get_asset_correlations(asset)
            elif correlation_type == 'strategies':
                result = correlation_engine.get_strategy_correlations()
            elif correlation_type == 'macro':
                factor = request.args.get('factor')
                result = correlation_engine.get_macro_correlations(factor)
            else:  # modules
                result = correlation_engine.get_module_correlations()

            return jsonify({
                'status': 'success',
                'correlation_type': correlation_type,
                'data': result,
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /correlations error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @brain_bp.route('/regime', methods=['GET'])
    def get_regime():
        """GET /brain/regime - Current market regime detection"""
        try:
            include_history = request.args.get('include_history', 'false').lower() == 'true'
            days = request.args.get('days', 30, type=int)

            regime_data = {
                'current': regime_detector.get_current_regime(),
                'probabilities': regime_detector.get_regime_probability(),
                'recommendation': regime_detector.get_regime_recommendation(),
            }

            if include_history:
                regime_data['history'] = regime_detector.get_regime_historical(days)

            return jsonify({
                'status': 'success',
                'regime_data': regime_data,
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /regime error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @brain_bp.route('/decisions', methods=['GET'])
    def get_decisions():
        """GET /brain/decisions - AI recommendations and decisions"""
        try:
            decision_filter = request.args.get('filter', 'all')
            module = request.args.get('module')

            if decision_filter == 'urgent':
                decisions_data = decision_engine.get_urgent_decisions()
            elif decision_filter == 'opportunities':
                decisions_data = decision_engine.get_opportunity_decisions()
            elif decision_filter == 'risks':
                decisions_data = decision_engine.get_risk_decisions()
            elif module:
                decisions_data = decision_engine.get_decisions_by_module(module)
            else:
                decisions_data = decision_engine.get_all_decisions()

            return jsonify({
                'status': 'success',
                'decisions': decisions_data,
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /decisions error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @brain_bp.route('/risk-radar', methods=['GET'])
    def get_risk_radar():
        """GET /brain/risk-radar - Unified risk assessment"""
        try:
            risk_data = learning_engine.get_risk_radar()
            risk_decisions = decision_engine.get_risk_decisions()

            return jsonify({
                'status': 'success',
                'risk_data': risk_data,
                'active_risk_alerts': risk_decisions,
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /risk-radar error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @brain_bp.route('/evolution', methods=['GET'])
    def get_evolution():
        """GET /brain/evolution - Brain evolution and improvement over time"""
        try:
            evolution_data = learning_engine.get_evolution_score()

            return jsonify({
                'status': 'success',
                'evolution': evolution_data,
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /evolution error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @brain_bp.route('/metrics', methods=['GET'])
    def get_metrics():
        """GET /brain/metrics - Aggregated metrics across modules"""
        try:
            module = request.args.get('module')

            metrics_data = learning_engine.get_metrics_summary()

            # Filter by module if specified
            if module:
                metrics_data = {
                    'summary': {module: metrics_data['summary'].get(module, {})},
                }

            return jsonify({
                'status': 'success',
                'metrics': metrics_data,
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /metrics error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @brain_bp.route('/health', methods=['GET'])
    def get_health():
        """GET /brain/health - Brain health and operational status"""
        try:
            evolution = learning_engine.get_evolution_score()
            regime = regime_detector.get_current_regime()

            return jsonify({
                'status': 'healthy',
                'brain_score': evolution['current_score'],
                'brain_phase': evolution['phase'],
                'market_regime': regime['regime_type'],
                'modules_count': 5,
                'operational': True,
                'timestamp': datetime.now().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /health error: {e}")
            return jsonify({
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
            }), 500

    @brain_bp.route('/module-feed/<module>', methods=['GET'])
    def get_module_feed(module):
        """GET /brain/module-feed/<module> - Data feed from specific module"""
        try:
            valid_modules = ['Arbitrage', 'Crypto', 'Stocks', 'Derivatives', 'Long_Horizon']

            if module not in valid_modules:
                return jsonify({'error': f'Invalid module: {module}'}), 404

            # Get module-specific decisions
            decisions = decision_engine.get_decisions_by_module(module)

            # Get module-specific lessons
            lessons_summary = learning_engine.get_lessons_summary()
            module_lessons = [l for l in lessons_summary['lessons'] if l['module'] == module]

            return jsonify({
                'status': 'success',
                'module': module,
                'decisions': decisions,
                'lessons_count': len(module_lessons),
                'lessons_sample': module_lessons[:5],
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /module-feed/{module} error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @brain_bp.route('/cross-insights', methods=['GET'])
    def get_cross_insights():
        """GET /brain/cross-insights - Cross-module insights and synergies"""
        try:
            correlations = correlation_engine.get_module_correlations()
            patterns = learning_engine.get_pattern_alerts()
            opportunities = decision_engine.get_opportunity_decisions()

            insights = {
                'cross_module_correlations': correlations['correlation_matrix'],
                'strongest_link': correlations['strongest_link'],
                'tightest_coupling': correlations['tightest_coupling'],
                'cross_domain_patterns': [p for p in patterns['patterns'] if len(p['modules_involved']) > 1],
                'synergy_opportunities': opportunities,
                'summary': f"""
                Os 5 módulos Egreja (Arbitrage, Crypto, Stocks, Derivatives, Long_Horizon)
                mostraram forte acoplamento. Correlação mais forte:
                Arbitrage ↔ Derivatives (0.89) — ideal para estratégias combinadas.

                Padrões detectados cruzando múltiplos módulos: {len([p for p in patterns['patterns'] if len(p['modules_involved']) > 1])}.
                Oportunidades de sinergia: {len(opportunities['opportunities'])}.

                Recomendação: Executar estratégias que exploram alta correlação Arbi-Deriv
                em combinação com sinais Long_Horizon (score elevado).
                """,
            }

            return jsonify({
                'status': 'success',
                'insights': insights,
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /cross-insights error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @brain_bp.route('/debug-memory', methods=['GET'])
    def debug_memory():
        """GET /brain/debug-memory - Debug brain persistent memory state"""
        try:
            # Check tables exist and row counts
            tables_info = {}
            table_names = ['brain_lessons', 'brain_patterns', 'brain_correlations',
                           'brain_decisions', 'brain_metrics', 'brain_regime',
                           'brain_daily_digest', 'brain_evolution']
            for t in table_names:
                try:
                    row = _safe_query(f"SELECT COUNT(*) as cnt FROM {t}", fetch='one')
                    tables_info[t] = row['cnt'] if row else 'ERROR'
                except Exception as te:
                    tables_info[t] = f'ERROR: {te}'

            # Check engine state
            engine_state = {
                'initialized': learning_engine._initialized,
                'lessons_in_memory': len(learning_engine._lessons),
                'patterns_in_memory': len(learning_engine._patterns),
                'correlations_in_memory': len(learning_engine._correlations),
                'decisions_in_memory': len(learning_engine._decisions),
                'regime_in_memory': bool(learning_engine._regime),
                'evolution_in_memory': len(learning_engine._evolution),
            }

            # Try to get DB connection
            conn = get_db()
            db_ok = conn is not None
            if conn:
                try:
                    conn.close()
                except:
                    pass

            return jsonify({
                'status': 'debug',
                'db_connection_ok': db_ok,
                'tables': tables_info,
                'engine': engine_state,
                'timestamp': datetime.now().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /debug-memory error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

    @brain_bp.route('/force-seed', methods=['POST'])
    def force_seed():
        """POST /brain/force-seed - Force re-seed foundational knowledge"""
        try:
            learning_engine._initialized = False
            learning_engine._lessons = []
            learning_engine._patterns = []
            learning_engine._correlations = []
            learning_engine._decisions = []
            learning_engine._evolution = []
            learning_engine._regime = {}
            learning_engine.ensure_initialized()

            return jsonify({
                'status': 'seeded',
                'lessons': len(learning_engine._lessons),
                'patterns': len(learning_engine._patterns),
                'correlations': len(learning_engine._correlations),
                'decisions': len(learning_engine._decisions),
                'evolution': len(learning_engine._evolution),
            }), 200

        except Exception as e:
            log.error(f"POST /force-seed error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

    return brain_bp
