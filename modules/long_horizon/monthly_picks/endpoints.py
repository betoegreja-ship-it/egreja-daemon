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
import threading
import time as _time
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
            result = lc.run_monthly_scan()
            return jsonify(result), 200
        except Exception as e:
            log.error(f'[MP API] /scan error: {e}')
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

    # ── [v10.27] Data Ingestion (background) ─────────────
    _ingest_state = {'running': False, 'progress': 0, 'total': 0,
                     'scored': 0, 'failed': 0, 'last_ticker': '',
                     'started_at': None, 'completed_at': None, 'error': None}

    def _run_ingest_background():
        """Collect real data from providers, score, persist to lh_scores."""
        import datetime as _dt
        _ingest_state.update({
            'running': True, 'progress': 0, 'scored': 0, 'failed': 0,
            'error': None, 'started_at': _dt.datetime.now().isoformat(),
            'completed_at': None,
        })
        try:
            from modules.long_horizon.data_ingestion import LongHorizonDataCollector
            from modules.long_horizon.scoring_engine import score_from_real_data

            collector = LongHorizonDataCollector()
            universe = collector.UNIVERSE
            _ingest_state['total'] = len(universe)
            log.info(f'[MP Ingest] Starting data collection for {len(universe)} tickers')

            scored_list = []

            for i, ticker in enumerate(universe):
                _ingest_state['progress'] = i + 1
                _ingest_state['last_ticker'] = ticker
                try:
                    profile = collector.collect_all(ticker)
                    if not profile:
                        _ingest_state['failed'] += 1
                        continue

                    scored = score_from_real_data(profile)
                    if scored and scored.get('total_score', 0) > 20:
                        scored['sector'] = profile.get('sector', 'Unknown')
                        scored['market'] = profile.get('market', 'Unknown')
                        scored['name'] = profile.get('name', ticker)
                        scored_list.append(scored)
                        _ingest_state['scored'] += 1
                except Exception as e:
                    _ingest_state['failed'] += 1
                    log.debug(f'[MP Ingest] {ticker}: {e}')

            # Persist all scores to DB
            if scored_list:
                _persist_all_scores(scored_list)

            _ingest_state['completed_at'] = _dt.datetime.now().isoformat()
            log.info(f'[MP Ingest] Done: {len(scored_list)} scored, '
                     f'{_ingest_state["failed"]} failed')

        except Exception as e:
            _ingest_state['error'] = str(e)
            log.error(f'[MP Ingest] Fatal error: {e}')
        finally:
            _ingest_state['running'] = False

    def _persist_all_scores(scored_list):
        """Write scored data to lh_assets + lh_scores tables."""
        import datetime as _dt
        conn = None
        try:
            conn = db_fn()
            cursor = conn.cursor()
            today = _dt.date.today().isoformat()

            for s in scored_list:
                ticker = s['ticker']
                # Upsert asset
                cursor.execute(
                    "INSERT INTO lh_assets (ticker, name, sector, market, active) "
                    "VALUES (%s, %s, %s, %s, TRUE) "
                    "ON DUPLICATE KEY UPDATE name=VALUES(name), sector=VALUES(sector), "
                    "market=VALUES(market), active=TRUE",
                    (ticker, s.get('name', ticker),
                     s.get('sector', 'Unknown'),
                     s.get('market', 'Unknown'))
                )
                cursor.execute("SELECT asset_id FROM lh_assets WHERE ticker=%s", (ticker,))
                asset_id = cursor.fetchone()[0]

                # Upsert score
                cursor.execute("""
                    INSERT INTO lh_scores
                        (asset_id, score_date, total_score, conviction,
                         business_quality, valuation, market_strength,
                         macro_factors, options_signal, structural_risk,
                         data_reliability, model_version)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        total_score=VALUES(total_score), conviction=VALUES(conviction),
                        business_quality=VALUES(business_quality), valuation=VALUES(valuation),
                        market_strength=VALUES(market_strength), macro_factors=VALUES(macro_factors),
                        options_signal=VALUES(options_signal), structural_risk=VALUES(structural_risk),
                        data_reliability=VALUES(data_reliability), model_version=VALUES(model_version)
                """, (
                    asset_id, today,
                    s.get('total_score', 0), s.get('conviction', 'Neutral'),
                    s.get('business_quality', 0), s.get('valuation', 0),
                    s.get('market_strength', 0), s.get('macro_factors', 0),
                    s.get('options_signal', 0), s.get('structural_risk', 0),
                    s.get('data_reliability', 0), 'v2.0-realdata',
                ))

            conn.commit()
            log.info(f'[MP Ingest] Persisted {len(scored_list)} scores to lh_scores')
        except Exception as e:
            log.error(f'[MP Ingest] DB persist error: {e}')
        finally:
            if conn:
                try: conn.close()
                except: pass

    @bp.route('/ingest', methods=['POST'])
    def start_ingest():
        """Start background data ingestion from all providers."""
        if _ingest_state['running']:
            return jsonify({
                'status': 'already_running',
                'progress': _ingest_state['progress'],
                'total': _ingest_state['total'],
                'last_ticker': _ingest_state['last_ticker'],
            }), 200

        t = threading.Thread(target=_run_ingest_background, daemon=True)
        t.start()
        return jsonify({'status': 'started', 'total': 109}), 202

    @bp.route('/ingest/status', methods=['GET'])
    def ingest_status():
        """Check data ingestion progress."""
        return jsonify({
            'status': 'ok',
            **_ingest_state,
        }), 200

    # ── [v10.27] Async Scan ───────────────────────────────

    _scan_state = {'running': False, 'result': None, 'error': None,
                   'started_at': None, 'completed_at': None}

    def _run_scan_background():
        """Run monthly scan in background thread."""
        import datetime as _dt
        _scan_state.update({
            'running': True, 'result': None, 'error': None,
            'started_at': _dt.datetime.now().isoformat(),
            'completed_at': None,
        })
        try:
            lc = _get_lifecycle()
            result = lc.run_monthly_scan()
            _scan_state['result'] = result
            _scan_state['completed_at'] = _dt.datetime.now().isoformat()
            log.info(f'[MP Scan Async] Done: {result}')
        except Exception as e:
            _scan_state['error'] = str(e)
            log.error(f'[MP Scan Async] Error: {e}')
        finally:
            _scan_state['running'] = False

    @bp.route('/scan-async', methods=['POST'])
    def run_scan_async():
        """Start scan in background thread (returns immediately)."""
        if _scan_state['running']:
            return jsonify({'status': 'already_running'}), 200

        t = threading.Thread(target=_run_scan_background, daemon=True)
        t.start()
        return jsonify({'status': 'scan_started'}), 202

    @bp.route('/scan/status', methods=['GET'])
    def scan_status():
        """Check async scan progress."""
        return jsonify({
            'status': 'ok',
            **_scan_state,
        }), 200

    return bp
