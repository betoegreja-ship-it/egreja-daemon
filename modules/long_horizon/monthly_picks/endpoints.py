"""
Monthly Picks — REST Endpoints.

Blueprint modular — registrado via factory function.
Nenhuma lógica de negócio aqui, apenas roteamento e serialização.

Endpoints sob /monthly-picks/:
  GET  /dashboard       — overview completo
  POST /scan            — dispara scan mensal
  POST /review          — dispara review semanal
  GET  /positions        — posições abertas
  GET  /positions/closed — posições encerradas
  GET  /candidates       — candidatos do último scan
  GET  /scans            — histórico de scans
  GET  /reviews          — reviews recentes
  GET  /performance      — métricas por coorte
  POST /close/<id>       — force close
  GET  /config           — config atual
  POST /config           — atualizar config
  GET  /health           — health check
  GET  /discovery/status — status do discovery engine
"""

import logging
from flask import Blueprint, jsonify, request

logger = logging.getLogger('egreja.monthly_picks.endpoints')


def create_monthly_picks_blueprint(db_fn, log=None, **kwargs) -> Blueprint:
    """
    Factory function — same pattern as Long Horizon & Derivatives.
    Receives db_fn and optional services via kwargs.
    """
    bp = Blueprint('monthly_picks', __name__)
    log = log or logger

    # Lazy init — lifecycle created on first use
    _lifecycle = [None]

    def _get_lifecycle():
        if _lifecycle[0] is None:
            from .lifecycle import MonthlyPicksLifecycle
            from .learning_bridge import LearningBridge
            from .config import get_config

            # Try to get brain functions from kwargs or globals
            brain_lesson_fn = kwargs.get('brain_lesson_fn')
            brain_decision_fn = kwargs.get('brain_decision_fn')

            bridge = LearningBridge(
                brain_lesson_fn=brain_lesson_fn,
                brain_decision_fn=brain_decision_fn,
                log=log,
            )

            _lifecycle[0] = MonthlyPicksLifecycle(
                db_fn=db_fn,
                config=get_config(),
                learning_bridge=bridge,
                log=log,
            )
        return _lifecycle[0]

    # ── Dashboard ──────────────────────────────────────────

    @bp.route('/dashboard', methods=['GET'])
    def dashboard():
        try:
            lc = _get_lifecycle()
            data = lc.get_dashboard()
            return jsonify({'status': 'ok', **data}), 200
        except Exception as e:
            log.error(f'[MP API] /dashboard error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    # ── Monthly Scan ───────────────────────────────────────

    @bp.route('/scan', methods=['POST'])
    def run_scan():
        try:
            lc = _get_lifecycle()
            force = request.args.get('force', '').lower() in ('1', 'true', 'yes')
            result = lc.run_monthly_scan(force=force)
            return jsonify(result), 200
        except Exception as e:
            log.error(f'[MP API] /scan error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    @bp.route('/reset-month', methods=['POST'])
    def reset_month():
        try:
            lc = _get_lifecycle()
            conn = db_fn()
            cur = conn.cursor()
            cur.execute('DELETE FROM mp_actions')
            cur.execute('DELETE FROM mp_reviews')
            cur.execute('DELETE FROM mp_positions')
            cur.execute('DELETE FROM mp_candidates')
            cur.execute('DELETE FROM mp_scan_runs')
            conn.commit()
            conn.close()
            return jsonify({'status': 'ok', 'message': 'All MP data cleared for re-scan'}), 200
        except Exception as e:
            log.error(f'[MP API] /reset-month error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    # ── Weekly Review ──────────────────────────────────────

    @bp.route('/review', methods=['POST'])
    def run_review():
        try:
            lc = _get_lifecycle()
            result = lc.run_weekly_review()
            return jsonify(result), 200
        except Exception as e:
            log.error(f'[MP API] /review error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    # ── Daily Check [v10.27i] ─────────────────────────────

    @bp.route('/daily-check', methods=['POST'])
    def daily_check():
        try:
            lc = _get_lifecycle()
            result = lc.run_daily_check()
            return jsonify(result), 200
        except Exception as e:
            log.error(f'[MP API] /daily-check error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    # ── Rescore Universe [v10.27i] ────────────────────────

    @bp.route('/rescore', methods=['POST'])
    def rescore():
        try:
            from .scheduler_hooks import rescore_universe_hook
            result = rescore_universe_hook(db_fn, log)
            return jsonify(result), 200
        except Exception as e:
            log.error(f'[MP API] /rescore error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    # ── Positions ──────────────────────────────────────────

    @bp.route('/positions', methods=['GET'])
    def get_positions():
        try:
            lc = _get_lifecycle()
            positions = lc.repo.get_open_positions()
            return jsonify({
                'status': 'ok',
                'count': len(positions),
                'positions': positions,
            }), 200
        except Exception as e:
            log.error(f'[MP API] /positions error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    @bp.route('/positions/closed', methods=['GET'])
    def get_closed_positions():
        try:
            lc = _get_lifecycle()
            limit = request.args.get('limit', 50, type=int)
            positions = lc.repo.get_closed_positions(limit=limit)
            return jsonify({
                'status': 'ok',
                'count': len(positions),
                'positions': positions,
            }), 200
        except Exception as e:
            log.error(f'[MP API] /positions/closed error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    # ── Candidates ─────────────────────────────────────────

    @bp.route('/candidates', methods=['GET'])
    def get_candidates():
        try:
            lc = _get_lifecycle()
            scans = lc.repo.get_scan_runs(limit=1)
            if not scans:
                return jsonify({'status': 'ok', 'candidates': []}), 200
            run_id = scans[0]['run_id']
            candidates = lc.repo.get_candidates(run_id)
            return jsonify({
                'status': 'ok',
                'scan_month': scans[0].get('scan_month'),
                'count': len(candidates),
                'candidates': candidates,
            }), 200
        except Exception as e:
            log.error(f'[MP API] /candidates error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    # ── Scan History ───────────────────────────────────────

    @bp.route('/scans', methods=['GET'])
    def get_scans():
        try:
            lc = _get_lifecycle()
            limit = request.args.get('limit', 12, type=int)
            scans = lc.repo.get_scan_runs(limit=limit)
            return jsonify({'status': 'ok', 'scans': scans}), 200
        except Exception as e:
            log.error(f'[MP API] /scans error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    # ── Reviews ────────────────────────────────────────────

    @bp.route('/reviews', methods=['GET'])
    def get_reviews():
        try:
            lc = _get_lifecycle()
            limit = request.args.get('limit', 30, type=int)
            reviews = lc.repo.get_recent_reviews(limit=limit)
            return jsonify({'status': 'ok', 'reviews': reviews}), 200
        except Exception as e:
            log.error(f'[MP API] /reviews error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    # ── Performance ────────────────────────────────────────

    @bp.route('/performance', methods=['GET'])
    def get_performance():
        try:
            lc = _get_lifecycle()
            perf = lc.repo.get_performance()
            return jsonify({'status': 'ok', 'performance': perf}), 200
        except Exception as e:
            log.error(f'[MP API] /performance error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    # ── Force Close ────────────────────────────────────────

    @bp.route('/close/<int:position_id>', methods=['POST'])
    def force_close(position_id):
        try:
            lc = _get_lifecycle()
            body = request.get_json(silent=True) or {}
            reason = body.get('reason', 'human_override')
            price = body.get('close_price')
            result = lc.force_close(position_id, reason=reason,
                                     close_price=price)
            status_code = 200 if 'error' not in result else 400
            return jsonify(result), status_code
        except Exception as e:
            log.error(f'[MP API] /close/{position_id} error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    # ── Config ─────────────────────────────────────────────

    @bp.route('/config', methods=['GET'])
    def get_config_endpoint():
        try:
            from .config import get_config
            cfg = get_config()
            return jsonify({
                'status': 'ok',
                'config': {
                    k: v for k, v in cfg.__dict__.items()
                    if not k.startswith('_')
                },
            }), 200
        except Exception as e:
            log.error(f'[MP API] /config GET error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    @bp.route('/config', methods=['POST'])
    def update_config():
        try:
            lc = _get_lifecycle()
            body = request.get_json(silent=True) or {}
            for key, value in body.items():
                lc.repo.set_config(key, str(value))
            return jsonify({'status': 'ok', 'updated': list(body.keys())}), 200
        except Exception as e:
            log.error(f'[MP API] /config POST error: {e}')
            return jsonify({'status': 'error', 'message': str(e)}), 500

    # ── Health ─────────────────────────────────────────────

    @bp.route('/debug-price-trace/<ticker>', methods=['GET'])
    def debug_price_trace(ticker):
        import sys as _sys
        trace = []
        # Step 0: Global cache
        _main = _sys.modules.get('__main__')
        sp = getattr(_main, 'stock_prices', {}) if _main else {}
        direct = sp.get(ticker)
        with_sa = sp.get(ticker + '.SA')
        if direct:
            if isinstance(direct, dict):
                p = direct.get('regularMarketPrice') or direct.get('price') or direct.get('c')
                trace.append({'step': 'global_cache_direct', 'found': True, 'price': p, 'type': type(p).__name__ if p else 'None'})
            else:
                trace.append({'step': 'global_cache_direct', 'found': True, 'price': direct, 'type': type(direct).__name__})
        elif with_sa:
            if isinstance(with_sa, dict):
                p = with_sa.get('regularMarketPrice') or with_sa.get('price') or with_sa.get('c')
                trace.append({'step': 'global_cache_sa', 'found': True, 'price': p})
            else:
                trace.append({'step': 'global_cache_sa', 'found': True, 'price': with_sa})
        else:
            trace.append({'step': 'global_cache', 'found': False})
        # Step 1: BRAPI
        try:
            from modules.long_horizon.brapi_provider import BRAPIProvider
            q = BRAPIProvider().get_quote(ticker)
            trace.append({'step': 'brapi', 'found': bool(q), 'price': q.get('price') if q else None})
        except Exception as e:
            trace.append({'step': 'brapi', 'error': str(e)})
        # Step 2: Polygon
        try:
            from modules.long_horizon.polygon_provider import PolygonProvider
            q = PolygonProvider().get_quote(ticker)
            trace.append({'step': 'polygon', 'found': bool(q), 'price': q.get('price') if q else None})
        except Exception as e:
            trace.append({'step': 'polygon', 'error': str(e)})
        return jsonify({'ticker': ticker, 'trace': trace}), 200

    @bp.route('/debug-review/<int:pos_id>', methods=['POST'])
    def debug_review(pos_id):
        try:
            lc = _get_lifecycle()
            positions = lc.repo.get_open_positions()
            pos = None
            for p in positions:
                if p['position_id'] == pos_id:
                    pos = p
                    break
            if not pos:
                return jsonify({'error': 'position not found', 'pos_id': pos_id}), 404
            result = lc.review_engine.review_position(pos)
            return jsonify({'status': 'ok', 'result': result}), 200
        except Exception as e:
            import traceback
            return jsonify({'status': 'error', 'message': str(e), 'traceback': traceback.format_exc()}), 500

    @bp.route('/debug-prices', methods=['GET'])
    def debug_prices():
        import sys as _sys
        tickers = request.args.get('tickers', '').split(',')
        if not tickers or not tickers[0]:
            tickers = ['NVDA','LLY','ITUB4','ABEV3','UNH','ADBE','BBDC4','AVGO']
        result = {}
        _main = _sys.modules.get('__main__')
        sp = getattr(_main, 'stock_prices', {}) if _main else {}
        for t in tickers:
            t = t.strip()
            direct = sp.get(t)
            with_sa = sp.get(t + '.SA')
            if direct:
                if isinstance(direct, dict):
                    result[t] = {'source': 'direct', 'price': direct.get('regularMarketPrice') or direct.get('price') or direct.get('c'), 'keys': list(direct.keys())[:10]}
                else:
                    result[t] = {'source': 'direct', 'value': direct}
            elif with_sa:
                if isinstance(with_sa, dict):
                    result[t] = {'source': t+'.SA', 'price': with_sa.get('regularMarketPrice') or with_sa.get('price') or with_sa.get('c'), 'keys': list(with_sa.keys())[:10]}
                else:
                    result[t] = {'source': t+'.SA', 'value': with_sa}
            else:
                result[t] = {'source': 'NOT_FOUND', 'tried': [t, t+'.SA']}
        result['_cache_size'] = len(sp)
        result['_sample_keys'] = list(sp.keys())[:20]
        return jsonify(result), 200

    @bp.route('/health', methods=['GET'])
    def health():
        try:
            lc = _get_lifecycle()
            sleeve_status = lc.repo.get_sleeve_status()
            open_count = len(lc.repo.get_open_positions())
            return jsonify({
                'status': 'ok',
                'module': 'monthly_picks',
                'version': 'v3.2',
                'sleeve_status': sleeve_status,
                'open_positions': open_count,
            }), 200
        except Exception as e:
            return jsonify({
                'status': 'degraded',
                'module': 'monthly_picks',
                'error': str(e),
            }), 200

    # ── Discovery Status ───────────────────────────────────

    @bp.route('/discovery/status', methods=['GET'])
    def discovery_status():
        """Status of the discovery engine (expanded universe)."""
        try:
            from ..discovery_engine import DiscoveryEngine
            engine = DiscoveryEngine(db_fn=db_fn, log=log)
            status = engine.get_status()
            return jsonify({'status': 'ok', **status}), 200
        except ImportError:
            return jsonify({
                'status': 'ok',
                'discovery': 'not_available',
                'message': 'Discovery engine not yet deployed',
            }), 200
        except Exception as e:
            log.warning(f'[MP API] /discovery/status error: {e}')
            return jsonify({'status': 'ok', 'discovery': 'error',
                            'message': str(e)}), 200

    return bp
