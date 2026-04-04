"""
[Phase 5] API Routes Blueprint
Extracted from api_server.py lines 7168-9664
All ~78 Flask route handlers as a Blueprint.

CONTEXT REQUIREMENTS:
The context provider must return a dict with ALL of the following keys:

  State & Locks:
    - stocks_open, stocks_closed (lists of trade dicts)
    - crypto_open, crypto_closed (lists of trade dicts)
    - arbi_trades (list of arbitrage trade dicts)
    - stocks_capital, crypto_capital, arbi_capital (floats)
    - state_lock (threading.Lock for state access)
    - market_regime (str: 'bull', 'bear', 'sideways')
    - trading_suspended (bool)
    - kill_switch_active (bool)
    - arbi_kill_switch (bool)

  Prices & Quotes:
    - stock_prices, crypto_prices (dicts mapping symbol -> price data)
    - watchlist_symbols (list of watchlist dicts)
    - watchlist_lock (threading.Lock)
    - stock_prices_lock, crypto_prices_lock (threading.Lock)

  Functions (callables):
    - get_db() -> MySQL connection or None
    - audit(event: str, data: dict) -> None
    - beat(name: str) -> None (thread heartbeat)
    - log (logger instance, e.g., logging.Logger)
    - test_db() -> bool
    - calc_fee(position_value: float, market: str, asset_type: str) -> float
    - apply_fee_to_trade(trade: dict) -> dict
    - require_auth (decorator function for auth checking)
    - check_risk(position_value: float, asset_type: str, market: str) -> bool

  Constants:
    - API_SECRET_KEY (str)
    - INITIAL_CAPITAL_STOCKS, INITIAL_CAPITAL_CRYPTO, ARBI_CAPITAL (floats)
    - RISK_KILL_SWITCH, ARBI_KILL_SWITCH (bools)
    - ALERTS_ENABLED (bool)
    - VERSION (str)
    - THREAD_HEARTBEAT_TIMEOUT (dict: {thread_name: timeout_seconds})
    - DEFAULT_HB_TIMEOUT (float)
    - URGENT_QUEUE_WARN, URGENT_QUEUE_CRIT (ints for queue size thresholds)

  Queues & Thread Status:
    - urgent_queue, alert_queue (queue.Queue instances)
    - thread_health, thread_heartbeat, thread_restart_count (dicts)

  Managers & Modules (institutional):
    - risk_manager (has get_status() method)
    - ext_kill_switch (has check_all(get_db) method)
    - order_tracker (has get_reconciliation_status(), get_slippage_stats())
    - data_validator (has get_data_quality_status())
    - perf_stats (has get_full_report())
    - auth_manager (has auth_mode, admin_email attributes, verify())
    - learning_manager, learner_state, learner_feature_scaling

  Internal State:
    - _daily_dd_stocks, _daily_dd_crypto (floats for drawdown)
    - _symbol_blacklist (dict)
    - _read_degraded() -> bool/dict (function)
    - _ledger_lock, _capital_ledger (threading.Lock, list of ledger entries)
    - _last_reconciliation, _last_calibration_persist (float timestamps or None)
    - _reconciliation_log, _capital_ledger (lists)
    - _boot_time (float timestamp)
"""
from flask import Blueprint, request, jsonify, Response, send_from_directory
from datetime import datetime, timedelta
from functools import wraps
import time
import logging
import json
import threading
import os
from decimal import Decimal

# Blueprint definition
api_bp = Blueprint('api_routes', __name__)

# Global context provider (set by init_routes)
_ctx_provider = None

def require_auth(f):
    """[FIX-1] Decorador de documentação — autenticação real feita pelo before_request.
    Mantido para clareza semântica: marcar explicitamente rotas que exigem auth."""
    @wraps(f)
    def decorated(*args, **kwargs):
        return f(*args, **kwargs)
    return decorated

def init_routes(ctx_provider_fn):
    """Initialize the routes with a context provider function.

    Injects all shared state into this module's namespace so route functions
    can access them as bare names (e.g. stock_prices, get_db, etc.).
    For mutable containers (dicts, lists), references are shared with the caller.
    For scalars that change (stocks_capital etc.), routes should use _get_ctx().

    Args:
        ctx_provider_fn: A callable that returns a dict with all required global state.
                        See module docstring for required keys.
    """
    global _ctx_provider
    _ctx_provider = ctx_provider_fn
    # Inject all shared references into module namespace
    # This allows route code to use bare names (stock_prices, get_db, etc.)
    ctx = ctx_provider_fn()
    g = globals()
    for key, val in ctx.items():
        g[key] = val

def _get_ctx():
    """Get the current context dict with all global state.

    Returns:
        dict: All global state required by the routes.

    Raises:
        RuntimeError: If routes not initialized.
    """
    if _ctx_provider is None:
        raise RuntimeError("Routes not initialized: call init_routes(provider_fn) first")
    return _ctx_provider()


@api_bp.route('/watchlist/quote')
def watchlist_quote():
    ctx = _get_ctx()
    symbol=request.args.get('symbol','').upper().strip()
    market=request.args.get('market','US').upper()
    if not symbol: return jsonify({'error':'symbol required'}),400
    cached=stock_prices.get(symbol)
    if cached and cached.get('price',0)>0:
        return jsonify({'symbol':symbol,'name':symbol,'price':cached['price'],'change':0,
            'change_pct':cached.get('change_pct',0),'rsi':cached.get('rsi',50),
            'ema9':cached.get('ema9',0),'ema21':cached.get('ema21',0),
            'ema9_real':cached.get('ema9_real',False),'ema50_real':cached.get('ema50_real',False),
            'candles':cached.get('candles_available',0),
            'atr_pct':cached.get('atr_pct',0.0),             # [v10.4]
            'volume_ratio':cached.get('volume_ratio',0.0),   # [v10.4]
            'source':cached.get('source','cache'),
            'currency':'BRL' if market=='B3' else 'USD','market':market})
    # [v10.4] Usar camada unificada Polygon→brapi→FMP→Yahoo
    try:
        sym_fetch = symbol+'.SA' if market=='B3' else symbol
        result, _ = _fetch_single_stock(sym_fetch)
        if result and result.get('price',0)>0:
            price=result['price']; prev=result.get('prev',0)
            return jsonify({'symbol':symbol,'name':symbol,
                'price':price,'change':round(price-prev,4) if prev>0 else 0,
                'change_pct':result.get('change_pct',0),
                'rsi':result.get('rsi',50),'ema9':result.get('ema9',0),'ema21':result.get('ema21',0),
                'ema9_real':result.get('ema9_real',False),'ema50_real':result.get('ema50_real',False),
                'candles':result.get('candles_available',0),
                'atr_pct':result.get('atr_pct',0.0),
                'volume_ratio':result.get('volume_ratio',0.0),
                'source':result.get('source','live'),
                'currency':'BRL' if market=='B3' else 'USD','market':market})
        return jsonify({'error':'price unavailable'}),400
    except Exception as e: return jsonify({'error':str(e)}),500

@api_bp.route('/watchlist/add', methods=['POST'])
def watchlist_add():
    ctx = _get_ctx()
    data=request.get_json() or {}
    symbol=data.get('symbol','').upper().strip(); market=data.get('market','US').upper()
    if not symbol: return jsonify({'error':'symbol required'}),400
    with watchlist_lock:
        if any(w['symbol']==symbol for w in watchlist_symbols):
            return jsonify({'ok':True,'total':len(watchlist_symbols),'msg':'already exists'})
        conn=get_db()
        if conn:
            try:
                cursor=conn.cursor()
                cursor.execute("INSERT IGNORE INTO watchlist (symbol,market) VALUES (%s,%s)",(symbol,market))
                conn.commit(); cursor.close(); conn.close()
            except Exception as e: log.error(f'Watchlist add DB: {e}')
        watchlist_symbols.append({'symbol':symbol,'market':market,'addedAt':datetime.utcnow().isoformat()})
    return jsonify({'ok':True,'total':len(watchlist_symbols)})

@api_bp.route('/watchlist/remove', methods=['POST'])
def watchlist_remove():
    ctx = _get_ctx()
    global watchlist_symbols
    data=request.get_json() or {}; symbol=data.get('symbol','').upper().strip()
    with watchlist_lock:
        conn=get_db()
        if conn:
            try:
                cursor=conn.cursor()
                cursor.execute("DELETE FROM watchlist WHERE symbol=%s",(symbol,))
                conn.commit(); cursor.close(); conn.close()
            except Exception as e: log.error(f'Watchlist remove DB: {e}')
        watchlist_symbols=[w for w in watchlist_symbols if w['symbol']!=symbol]
    return jsonify({'ok':True,'total':len(watchlist_symbols)})

@api_bp.route('/watchlist')
def watchlist_get():
    ctx = _get_ctx()
    with watchlist_lock: syms=list(watchlist_symbols)
    return jsonify({'symbols':syms,'total':len(syms)})

# ═══════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════
@api_bp.route('/admin/fix_corrupted_arbi_trade', methods=['POST'])
def fix_corrupted_arbi_trade():
    """[v10.14] Corrige trade arbi com dado corrompido (spread impossível)."""
    global arbi_capital
    data = request.json or {}
    trade_id = data.get('trade_id','')
    correct_pnl = float(data.get('correct_pnl', 0))
    if not trade_id:
        return jsonify({'error': 'trade_id required'}), 400
    conn = get_db()
    if not conn: return jsonify({'error': 'db error'}), 500
    try:
        c = conn.cursor(dictionary=True)
        c.execute("SELECT * FROM arbi_trades WHERE id=%s", (trade_id,))
        row = c.fetchone()
        if not row: return jsonify({'error': 'trade not found'}), 404
        old_pnl = float(row.get('pnl',0))
        old_spread = float(row.get('current_spread',0))
        # Corrigir no banco
        c.execute("UPDATE arbi_trades SET pnl=%s, pnl_pct=%s, current_spread=%s, close_reason=%s WHERE id=%s",
                  (correct_pnl, correct_pnl/float(row.get('position_size',1))*100, row.get('entry_spread',0),
                   'CORRUPTED_DATA_FIXED', trade_id))
        conn.commit()
        # Ajustar arbi_capital em memória
        with state_lock:
            adjustment = correct_pnl - old_pnl
            arbi_capital += adjustment
        log.warning(f'[ADMIN] Trade {trade_id} corrigida: pnl {old_pnl:+.0f}→{correct_pnl:+.0f} spread {old_spread:.2f}%→{row.get("entry_spread",0):.2f}% adj_capital={adjustment:+.0f}')
        return jsonify({'ok': True, 'old_pnl': old_pnl, 'new_pnl': correct_pnl, 'capital_adjustment': adjustment})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        try: conn.close()
        except: pass

# ═══ [v10.26] DERIVATIVES DASHBOARD (standalone HTML) ═══
@api_bp.route('/derivatives')
def derivatives_dashboard():
    """Serve the standalone derivatives trading dashboard."""
    ctx = _get_ctx()
    return send_from_directory('static', 'derivatives.html')

@api_bp.route('/health')
def health():
    ctx = _get_ctx()
    with state_lock: open_count=len(stocks_open)+len(crypto_open)
    now=time.time()
    hb_status={}
    for k,t in thread_health.items():
        timeout=THREAD_HEARTBEAT_TIMEOUT.get(k,DEFAULT_HB_TIMEOUT)
        hb_age=round(now-thread_heartbeat.get(k,now),1)
        hb_status[k]={'alive':t.is_alive(),'hb_age_s':hb_age,'timeout_s':timeout,
            'frozen': hb_age>timeout,'restarts':thread_restart_count.get(k,0)}
    return jsonify({
        'status':'ok','db':'connected' if test_db() else 'unavailable',
        'open_trades':open_count,'kill_switch':RISK_KILL_SWITCH,'arbi_kill_switch':ARBI_KILL_SWITCH,
        'strategy_dd': {'stocks': _daily_dd_stocks, 'crypto': _daily_dd_crypto},
        'blacklisted_symbols': list(_symbol_blacklist.keys()),
        'market_regime':market_regime,'alerts':ALERTS_ENABLED,
        'stock_prices_cached':len(stock_prices),'crypto_prices_cached':len(crypto_prices),
        'persist_queue_size':urgent_queue.qsize(),
        'persist_queue_warn':URGENT_QUEUE_WARN,'persist_queue_crit':URGENT_QUEUE_CRIT,
        'alert_queue_size':alert_queue.qsize(),
        'degraded': _read_degraded(),   # [V91-5]
        'learning_degraded': LEARNING_DEGRADED,   # [L-10]
        'threads':hb_status,'timestamp':datetime.utcnow().isoformat()
    })

@api_bp.route('/ops')
@require_auth
def ops_dashboard():
    """[v10.21] Dashboard operacional — reconciliação, ledger, calibração."""
    now = time.time()
    # Ledger stats
    with _ledger_lock:
        ledger_total = len(_capital_ledger)
        ledger_by_strat = {}
        ledger_by_event = {}
        for e in _capital_ledger:
            s = e.get('strategy', '?')
            ev = e.get('event', '?')
            ledger_by_strat[s] = ledger_by_strat.get(s, 0) + 1
            ledger_by_event[ev] = ledger_by_event.get(ev, 0) + 1
        last_ledger = _capital_ledger[-1] if _capital_ledger else None
    # Reconciliation
    last_recon_age = round(now - _last_reconciliation, 1) if _last_reconciliation else None
    recent_recon = _reconciliation_log[-10:] if _reconciliation_log else []
    recon_alerts = [r for r in _reconciliation_log[-30:] if r.get('alert')]
    # Calibration
    last_cal_age = round(now - _last_calibration_persist, 1) if _last_calibration_persist else None
    # Capital snapshot
    capital = {
        'stocks': round(stocks_capital, 2),
        'crypto': round(crypto_capital, 2),
        'arbi': round(arbi_capital, 2),
    }
    return jsonify({
        'version': VERSION,
        'uptime_s': round(now - _boot_time, 1),
        'capital': capital,
        'ledger': {
            'total_events': ledger_total,
            'by_strategy': ledger_by_strat,
            'by_event': ledger_by_event,
            'last_event': last_ledger,
        },
        'reconciliation': {
            'last_run_age_s': last_recon_age,
            'interval_s': RECONCILIATION_INTERVAL_S,
            'recent': recent_recon,
            'alerts_last_30': recon_alerts,
        },
        'calibration': {
            'last_persist_age_s': last_cal_age,
            'interval_s': CALIBRATION_PERSIST_INTERVAL,
        },
        # [v10.22] Institutional modules status
        'risk': risk_manager.get_status(),
        'kill_switch': ext_kill_switch.check_all(get_db),
        'broker': {
            'order_tracker': order_tracker.get_reconciliation_status(),
            'slippage': order_tracker.get_slippage_stats(),
        },
        'data_quality': data_validator.get_data_quality_status(),
        'performance': perf_stats.get_full_report(),
        'auth': {'mode': auth_manager.auth_mode, 'admin': auth_manager.admin_email},
        # [v10.23] Operational metrics
        'ops_health': ops_metrics.get_status(),
        'scorecard': perf_stats.get_strategy_scorecard() if hasattr(perf_stats, 'get_strategy_scorecard') else {},
        'drift_report': ops_metrics.get_drift_report(),
        'active_alerts': ops_metrics.get_active_alerts(),
        'timestamp': datetime.utcnow().isoformat(),
    })

# ── [v10.22] Kill Switch endpoints ───────────────────────────────────
@api_bp.route('/kill-switch/activate', methods=['POST'])
@require_auth
def kill_switch_activate():
    """[v10.22] Activate kill switch via API."""
    ctx = _get_ctx()
    data = request.get_json() or {}
    scope = data.get('scope', 'global')
    reason = data.get('reason', 'Manual activation via API')
    auto_resume = data.get('auto_resume_minutes')
    ext_kill_switch.activate(scope, reason, 'API', auto_resume, get_db)
    audit_logger.log_action('API', 'KILL_SWITCH', f'Activated {scope}: {reason}', get_db)
    return jsonify({'ok': True, 'scope': scope, 'reason': reason})

@api_bp.route('/kill-switch/deactivate', methods=['POST'])
@require_auth
def kill_switch_deactivate():
    """[v10.22] Deactivate kill switch via API."""
    data = request.get_json() or {}
    scope = data.get('scope', 'global')
    ext_kill_switch.deactivate(scope, 'API', get_db)
    audit_logger.log_action('API', 'KILL_SWITCH', f'Deactivated {scope}', get_db)
    return jsonify({'ok': True, 'scope': scope})

@api_bp.route('/kill-switch/status')
@require_auth
def kill_switch_status():
    """[v10.22] Kill switch status."""
    ctx = _get_ctx()
    return jsonify(ext_kill_switch.check_all(get_db))

# ── [v10.22] Risk endpoints ─────────────────────────────────────────
@api_bp.route('/risk/institutional')
@require_auth
def risk_institutional():
    """[v10.22] Institutional risk status."""
    return jsonify(risk_manager.get_status())

# ── [v10.22] Performance endpoints ──────────────────────────────────
@api_bp.route('/stats/report')
@require_auth
def stats_report():
    """[v10.22] Full performance report."""
    ctx = _get_ctx()
    return jsonify(perf_stats.get_full_report())

@api_bp.route('/stats/promotion')
@require_auth
def stats_promotion():
    """[v10.22] Capital promotion criteria check."""
    return jsonify(perf_stats.get_promotion_criteria())

# ── [v10.22] RBAC endpoints ────────────────────────────────────────
@api_bp.route('/admin/users', methods=['GET'])
@require_auth
def admin_list_users():
    """[v10.22] List all RBAC users."""
    ctx = _get_ctx()
    return jsonify(auth_manager.list_users(get_db))

@api_bp.route('/admin/users', methods=['POST'])
@require_auth
def admin_add_user():
    """[v10.22] Add a new user (admin only)."""
    data = request.get_json() or {}
    email = data.get('email', '')
    role = data.get('role', 'viewer')
    ok, msg = auth_manager.add_user(email, role, 'admin', get_db)
    if ok:
        audit_logger.log_action('admin', 'USER_ADD', f'{email} as {role}', get_db)
    return jsonify({'ok': ok, 'message': msg})

@api_bp.route('/admin/audit')
@require_auth
def admin_audit():
    """[v10.22] View audit log."""
    ctx = _get_ctx()
    limit = request.args.get('limit', 100, type=int)
    return jsonify(audit_logger.get_recent(limit, get_db))

# ── [v10.22] Data quality endpoint ──────────────────────────────────
@api_bp.route('/data/quality')
@require_auth
def data_quality_v1022():
    """[v10.22] Market data quality status."""
    return jsonify(data_validator.get_data_quality_status())

# ── [v10.23] Enhanced endpoints ──────────────────────────────────────

@api_bp.route('/stats/scorecard')
@require_auth
def stats_scorecard_v1023():
    """[v10.23] Per-strategy scorecard with traffic light."""
    ctx = _get_ctx()
    try:
        return jsonify(perf_stats.get_strategy_scorecard())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/stats/promotion/enhanced')
@require_auth
def stats_promotion_enhanced_v1023():
    """[v10.23] Enhanced promotion criteria with per-strategy + regime gates."""
    try:
        return jsonify(perf_stats.get_enhanced_promotion_criteria())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/ops/metrics')
@require_auth
def ops_metrics_v1023():
    """[v10.23] Operational metrics — memory, drift, workers, alerts."""
    ctx = _get_ctx()
    try:
        ops_metrics.record_memory()
        return jsonify(ops_metrics.get_status())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/ops/audit')
@require_auth
def ops_daily_audit_v1023():
    """[v10.23] Full daily audit report for soak testing."""
    try:
        ops_metrics.record_memory()
        return jsonify(ops_metrics.generate_daily_audit())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/ops/drift')
@require_auth
def ops_drift_report_v1023():
    """[v10.23] Reconciliation drift history with progressive alerts."""
    ctx = _get_ctx()
    try:
        return jsonify(ops_metrics.get_drift_report())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/ops/alerts')
@require_auth
def ops_alerts_v1023():
    """[v10.23] Active alerts + recent history."""
    try:
        return jsonify({
            'active': ops_metrics.get_active_alerts(),
            'history': ops_metrics.get_alert_history(50)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/kill-switch/safe-resume', methods=['POST'])
@require_auth
def kill_switch_safe_resume_v1023():
    """[v10.23] Safe resume with pre-checks (live mode)."""
    ctx = _get_ctx()
    try:
        data = request.get_json(silent=True) or {}
        scope = data.get('scope', 'global')
        resumed_by = data.get('resumed_by', 'api')
        success, reason = ext_kill_switch.safe_resume(
            scope=scope,
            resumed_by=resumed_by,
            get_db_func=get_db,
            data_validator=data_validator,
            risk_manager=risk_manager
        )
        return jsonify({'success': success, 'reason': reason})
    except Exception as e:
        return jsonify({'success': False, 'reason': str(e)}), 500

@api_bp.route('/broker/execution-profile')
@require_auth
def broker_execution_profile_v1023():
    """[v10.23] PaperBroker execution simulator statistics."""
    try:
        from modules.broker_base import BrokerFactory, AssetClass
        profiles = {}
        for ac in AssetClass:
            broker = BrokerFactory.get_broker(ac)
            if hasattr(broker, 'get_execution_profile'):
                profiles[ac.value] = broker.get_execution_profile()
        return jsonify(profiles)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ═══ [v10.26] WEB DASHBOARD (standalone HTML) ═══
@api_bp.route('/')
def index():
    """Serve main web dashboard. API info moved to /api/info."""
    ctx = _get_ctx()
    return send_from_directory('static', 'index.html')

@api_bp.route('/api/info')
def api_info():
    """API service info (previously served at /)."""
    return jsonify({
        'service':'Egreja Investment AI','version':'10.26.0','status':'online',
        'kill_switch':RISK_KILL_SWITCH,'arbi_kill_switch':ARBI_KILL_SWITCH,
        'market_regime':market_regime.get('mode','UNKNOWN'),
        'market_status':{'b3':is_b3_open(),'nyse':is_nyse_open(),'lse':is_lse_open(),'hkex':is_hkex_open(),'crypto':True},
        'deploy_mode':'single-process',
        'degraded': _read_degraded()['active'],   # [V91-5] flag rápida
    })

@api_bp.route('/degraded')
def degraded_route():
    """[V9-3][V91-5] Estado degradado do sistema — público."""
    ctx = _get_ctx()
    return jsonify({
        **_read_degraded(),
        'learning_degraded':   LEARNING_DEGRADED,   # [L-10]
        'learning_errors':     learning_errors,
        'queue_warn_threshold': URGENT_QUEUE_WARN,
        'queue_crit_threshold': URGENT_QUEUE_CRIT,
        'timestamp': datetime.utcnow().isoformat(),
    })

@api_bp.route('/debug')
def debug():
    ctx = _get_ctx()
    now=time.time()
    return jsonify({
        'db_status':'connected' if test_db() else 'unavailable',
        'stock_prices_cached':len(stock_prices),'crypto_prices_cached':len(crypto_prices),
        'alerts_enabled':ALERTS_ENABLED,'kill_switch':RISK_KILL_SWITCH,'arbi_kill_switch':ARBI_KILL_SWITCH,
        'market_regime':market_regime,
        'degraded': _read_degraded(),   # [V91-5]
        'risk_limits':{'max_open':MAX_OPEN_POSITIONS,'max_daily_dd_pct':MAX_DAILY_DRAWDOWN_PCT,
            'max_weekly_dd_pct':MAX_WEEKLY_DRAWDOWN_PCT,'max_risk_per_trade_pct':MAX_RISK_PER_TRADE_PCT,
            'signal_max_age_min':SIGNAL_MAX_AGE_MIN,'cooldown_sec':SYMBOL_COOLDOWN_SEC,
            'max_positions_stocks':MAX_POSITIONS_STOCKS,'max_positions_crypto':MAX_POSITIONS_CRYPTO},
        'arbi_limits':{'max_positions':ARBI_MAX_POSITIONS,'min_spread':ARBI_MIN_SPREAD,
            'tp_spread':ARBI_TP_SPREAD,'sl_pct':ARBI_SL_PCT,'timeout_h':ARBI_TIMEOUT_H},
        'queue_limits':{'warn':URGENT_QUEUE_WARN,'crit':URGENT_QUEUE_CRIT,
            'current':urgent_queue.qsize()},
        'market_status':{'b3':is_b3_open(),'nyse':is_nyse_open(),'lse':is_lse_open(),'hkex':is_hkex_open()},
        'threads':{k:{'alive':t.is_alive(),'hb_age_s':round(now-thread_heartbeat.get(k,now),1),
            'timeout_s':THREAD_HEARTBEAT_TIMEOUT.get(k,DEFAULT_HB_TIMEOUT),
            'restarts':thread_restart_count.get(k,0)}
            for k,t in thread_health.items()},
        'env':{k:os.environ.get(k,'NOT SET') for k in ['MYSQLHOST','MYSQLPORT','MYSQLDATABASE','PORT','ENV','WEB_CONCURRENCY']}
    })

@api_bp.route('/signals')
def signals():
    ctx = _get_ctx()
    conn=get_db()
    if not conn: return jsonify({'error':'Database unavailable'}),503
    try:
        cursor=conn.cursor(dictionary=True)
        cutoff=(datetime.utcnow()-timedelta(minutes=SIGNAL_MAX_AGE_MIN)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('SELECT * FROM market_signals WHERE created_at>=%s ORDER BY ABS(score-50) DESC LIMIT 500',(cutoff,))
        rows=cursor.fetchall(); cursor.close(); conn.close()
        for row in rows:
            for k,v in row.items():
                if isinstance(v,datetime): row[k]=v.isoformat()
            row['asset_type']='stock'
        with state_lock:
            open_stock_syms  = {t['symbol'] for t in stocks_open}
            open_crypto_syms = {t['symbol'] for t in crypto_open}
            sp_snap          = dict(stock_prices)
        for sig in rows:
            sig['trade_open']=sig['symbol'] in open_stock_syms
            sig['market_open']=market_open_for(sig.get('market_type',''))
            cached=sp_snap.get(sig['symbol'])
            if cached:
                sig['price']=cached['price']; sig['rsi']=cached.get('rsi',sig.get('rsi',50))
                sig['ema9']=cached.get('ema9',sig.get('ema9',0)); sig['ema21']=cached.get('ema21',sig.get('ema21',0))
                sig['ema50']=cached.get('ema50',sig.get('ema50',0))
                sig['ema50_real']=cached.get('ema50_real',False); sig['rsi_real']=cached.get('rsi_real',False)

        # [v10.9] Gerar stock_signals diretamente do stock_prices em memória
        # (como crypto faz com crypto_prices) — independente do market_signals no banco.
        # Garante que ações aparecem sempre no dashboard (mercado aberto ou fechado).
        stock_signals_from_mem = []
        b3_open  = is_b3_open()
        nyse_open= is_nyse_open()
        syms_in_rows = {r['symbol'] for r in rows}
        now_iso = datetime.utcnow().isoformat()
        for sym, pd in sp_snap.items():
            if not pd or pd.get('price', 0) <= 0: continue
            if sym in syms_in_rows: continue  # já veio do banco, não duplicar
            mkt_type = 'B3' if any(sym == s.replace('.SA','') for s in STOCK_SYMBOLS_B3) else 'NYSE'
            mkt_open = b3_open if mkt_type == 'B3' else nyse_open
            rsi  = pd.get('rsi', 50) or 50
            ema9 = pd.get('ema9', 0)  or 0
            ema21= pd.get('ema21',0)  or 0
            ema50= pd.get('ema50',0)  or 0
            # [v10.14] Score composto igual ao worker — não apenas RSI+EMA simples
            score = 50
            if   rsi < 30: score += 25
            elif rsi < 40: score += 15
            elif rsi < 50: score += 5
            elif rsi > 70: score -= 25
            elif rsi > 60: score -= 15
            elif rsi > 50: score -= 5
            ema50 = pd.get('ema50', 0) or 0
            if ema9 > 0 and ema21 > 0:
                if ema9 > ema21:
                    score += 12
                    if ema50 > 0 and ema21 > ema50: score += 8
                else:
                    score -= 12
                    if ema50 > 0 and ema21 < ema50: score -= 8
            vol_ratio = pd.get('volume_ratio', 0) or 0
            if vol_ratio > 1.5: score += 8
            elif vol_ratio < 0.5: score -= 5
            atr_pct = pd.get('atr_pct', 0) or 0
            if 0 < atr_pct < 1.5: score += 5
            elif atr_pct > 4.0: score -= 10
            price_sig = pd.get('price', 0) or 0
            if price_sig > 0 and ema9 > 0:
                if price_sig > ema9 * 1.01: score += 7
                elif price_sig < ema9 * 0.99: score -= 7
            score = max(0, min(100, score))
            signal = 'COMPRA' if score >= MIN_SCORE_AUTO else ('VENDA' if score <= (100-MIN_SCORE_AUTO) else 'MANTER')
            stock_signals_from_mem.append({
                'symbol': sym, 'price': pd.get('price', 0),
                'signal': signal, 'score': score,
                'market_type': mkt_type, 'asset_type': 'stock',
                'name': sym, 'rsi': round(rsi, 1),
                'ema9': round(ema9, 4), 'ema21': round(ema21, 4), 'ema50': round(ema50, 4),
                'ema50_real': pd.get('ema50_real', False),
                'rsi_real': pd.get('rsi_real', False),
                'change_24h': pd.get('change_24h', 0),
                'atr_pct': pd.get('atr_pct', 0),
                'vol_ratio': pd.get('volume_ratio', 0),
                'created_at': now_iso,
                'trade_open': sym in open_stock_syms,
                'market_open': mkt_open,
            })
        rows = rows + stock_signals_from_mem
        crypto_signals=[]
        for sym in CRYPTO_SYMBOLS:
            display=sym.replace('USDT',''); price=crypto_prices.get(sym,0)
            if price<=0: continue
            change_24h=crypto_momentum.get(sym,0); strength=abs(change_24h)
            if strength < 0.5:
                score = 50; signal = 'MANTER'
            else:
                direction_str = 'LONG' if change_24h > 0 else 'SHORT'
                # [v10.5-3] Usar _crypto_composite_score real — mesmo motor da execução
                ticker_data = crypto_tickers.get(sym, {})
                kline_cache_key = f'klines:{sym}'
                # [v10.6.2-Fix4] Mesma fonte única de klines — _candles_cache TTL=60min
                klines_data = _get_cached_candles(kline_cache_key, ttl_min=60) or {}
                if ticker_data and klines_data:
                    score = _crypto_composite_score(ticker_data, klines_data, direction_str)
                else:
                    # fallback se klines ainda não carregadas (startup)
                    base  = min(50 + int(strength * 5), 95)
                    score = base if change_24h > 0 else (100 - base)
                signal = 'COMPRA' if score >= MIN_SCORE_AUTO_CRYPTO else ('VENDA' if score <= (100 - MIN_SCORE_AUTO_CRYPTO) else 'MANTER')  # [v10.24-FIX] era 70/30 hardcoded — deve usar mesmo threshold do motor

            crypto_signals.append({
                'symbol':display,'price':price,'signal':signal,'score':score,
                'market_type':'CRYPTO','asset_type':'crypto',
                'name':CRYPTO_NAMES.get(sym,display),'rsi':round(max(10,min(90,50+change_24h*3)),1),
                'change_24h':round(change_24h,2),'ema50_real':False,'rsi_real':False,
                'atr_pct':   crypto_tickers.get(sym,{}).get('atr_pct', 0.0),      # [v10.5-3]
                'vol_ratio': crypto_tickers.get(sym,{}).get('vol_ratio', 0.0),     # [v10.5-3]
                'created_at':datetime.utcnow().isoformat(),'trade_open':display in open_crypto_syms
            })
        # [v10.8] Quando mercado está fechado e não há sinais recentes no DB,
        # gerar sinais off-hours a partir do stock_prices em memória (atualizado 1x/30min)
        # para que Markets/Overview continuem mostrando cotações.
        if not rows and not (is_b3_open() or is_nyse_open()):
            with state_lock:
                sp_snap = dict(stock_prices)
            now_iso = datetime.utcnow().isoformat()
            for sym, pd in sp_snap.items():
                if not pd or pd.get('price', 0) <= 0: continue
                # Determinar mercado pelo símbolo
                mkt_type = 'B3' if any(sym == s.replace('.SA','') for s in STOCK_SYMBOLS_B3) else 'NYSE'
                rows.append({
                    'symbol': sym, 'price': pd.get('price', 0),
                    'signal': 'MANTER', 'score': 50,
                    'market_type': mkt_type, 'asset_type': 'stock',
                    'name': sym, 'rsi': pd.get('rsi', 50),
                    'ema9': pd.get('ema9', 0), 'ema21': pd.get('ema21', 0),
                    'ema50': pd.get('ema50', 0), 'ema50_real': pd.get('ema50_real', False),
                    'rsi_real': pd.get('rsi_real', False),
                    'change_24h': pd.get('change_24h', 0),
                    'atr_pct': pd.get('atr_pct', 0),
                    'vol_ratio': pd.get('volume_ratio', 0),
                    'created_at': now_iso, 'trade_open': sym in open_stock_syms,
                    'market_open': False,
                })

        all_signals=rows+crypto_signals
        return jsonify({'status':'OK','timestamp':datetime.utcnow().isoformat(),
            'total':len(all_signals),'stocks_count':len(rows),'crypto_count':len(crypto_signals),
            'market_status':{'b3':is_b3_open(),'nyse':is_nyse_open(),'crypto':True},
            'market_regime':market_regime,'signals':all_signals})
    except Exception as e: return jsonify({'error':str(e)}),500

@api_bp.route('/prices/live')
def prices_live():
    ctx = _get_ctx()
    with state_lock:
        trades=[{'id':t['id'],'symbol':t['symbol'],
            'current_price':t.get('current_price',t.get('entry_price',0)),
            'pnl':t.get('pnl',0),'pnl_pct':t.get('pnl_pct',0),
            'peak_pnl_pct':t.get('peak_pnl_pct',0),'direction':t.get('direction','LONG')}
            for t in stocks_open+crypto_open]
        crypto_snap={k.replace('USDT',''):v for k,v in crypto_prices.items()}
    return jsonify({'timestamp':datetime.utcnow().isoformat(),'trades':trades,'crypto_prices':crypto_snap})

@api_bp.route('/trades/open')
def trades_open():
    ctx = _get_ctx()
    with state_lock: data=stocks_open+crypto_open
    return jsonify({'trades':data,'total':len(data)})

@api_bp.route('/trades/closed')
def trades_closed():
    ctx = _get_ctx()
    with state_lock:
        data=sorted(stocks_closed+crypto_closed,key=lambda x:x.get('closed_at',''),reverse=True)
    return jsonify({'trades':data,'total':len(data)})

@api_bp.route('/trades')
def trades():
    ctx = _get_ctx()
    with state_lock: all_t=stocks_open+crypto_open+stocks_closed+crypto_closed  # [v10.9] sem limite
    return jsonify({'trades':all_t,'total':len(all_t)})


def _get_db_trade_stats():
    """[v10.11] Stats agregadas de TODAS as trades do banco."""
    try:
        conn = get_db()
        if not conn: return {}
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(pnl) as total_pnl,
                SUM(CASE WHEN asset_type='stock' THEN pnl ELSE 0 END) as stocks_pnl,
                SUM(CASE WHEN asset_type='crypto' THEN pnl ELSE 0 END) as crypto_pnl,
                SUM(CASE WHEN asset_type='stock' AND pnl>0 THEN 1 ELSE 0 END) as stocks_wins,
                SUM(CASE WHEN asset_type='stock' THEN 1 ELSE 0 END) as stocks_total,
                SUM(CASE WHEN asset_type='crypto' AND pnl>0 THEN 1 ELSE 0 END) as crypto_wins,
                SUM(CASE WHEN asset_type='crypto' THEN 1 ELSE 0 END) as crypto_total,
                MAX(pnl) as best_trade, MIN(pnl) as worst_trade,
                SUM(CASE WHEN DATE(closed_at)=CURDATE() THEN pnl ELSE 0 END) as daily_pnl,
                SUM(CASE WHEN closed_at >= DATE_SUB(NOW(),INTERVAL 7 DAY) THEN pnl ELSE 0 END) as weekly_pnl,
                SUM(CASE WHEN closed_at >= DATE_SUB(NOW(),INTERVAL 30 DAY) THEN pnl ELSE 0 END) as monthly_pnl,
                SUM(CASE WHEN closed_at >= DATE_SUB(NOW(),INTERVAL 365 DAY) THEN pnl ELSE 0 END) as annual_pnl,
                SUM(CASE WHEN asset_type='stock' AND DATE(closed_at)=CURDATE() THEN pnl ELSE 0 END) as stocks_daily,
                SUM(CASE WHEN asset_type='stock' AND closed_at >= DATE_SUB(NOW(),INTERVAL 30 DAY) THEN pnl ELSE 0 END) as stocks_monthly,
                SUM(CASE WHEN asset_type='stock' AND closed_at >= DATE_SUB(NOW(),INTERVAL 365 DAY) THEN pnl ELSE 0 END) as stocks_annual,
                SUM(CASE WHEN asset_type='crypto' AND DATE(closed_at)=CURDATE() THEN pnl ELSE 0 END) as crypto_daily,
                SUM(CASE WHEN asset_type='crypto' AND closed_at >= DATE_SUB(NOW(),INTERVAL 30 DAY) THEN pnl ELSE 0 END) as crypto_monthly,
                SUM(CASE WHEN asset_type='crypto' AND closed_at >= DATE_SUB(NOW(),INTERVAL 365 DAY) THEN pnl ELSE 0 END) as crypto_annual,
                SUM(CASE WHEN asset_type='stock' THEN position_value ELSE 0 END) as stocks_deployed,
                SUM(CASE WHEN asset_type='crypto' THEN position_value ELSE 0 END) as crypto_deployed
            FROM trades WHERE status='CLOSED'
        """)
        row = cursor.fetchone()
        base = {k: float(v or 0) for k, v in row.items()}
        # 2a query — arbi (cursor ainda aberto)
        cursor.execute("SELECT SUM(position_size) as d, COUNT(*) as n FROM arbi_trades WHERE status='CLOSED'")
        ar = cursor.fetchone()
        base['arbi_deployed'] = float(ar.get('d') or 0)
        base['arbi_count_db'] = int(ar.get('n') or 0)
        cursor.close(); conn.close()
        return base
    except Exception as e:
        log.error(f'_get_db_trade_stats: {e}')
        return {}

@api_bp.route('/stats')
def stats():
    ctx = _get_ctx()
    # [v10.11] Stats de closed trades vêm do banco — nunca limitadas por memória
    db_st = _get_db_trade_stats()
    with state_lock:
        s_op=sum(t.get('pnl',0) for t in stocks_open)
        # [v10.14-FIX-CRITICO] position_value + pnl funciona para LONG e SHORT
        # current_price*qty era ERRADO para SHORT: quando short lucra (preço cai),
        # current_price*qty caía e portfolio diminuía — o inverso do correto!
        # Fórmula nova: para LONG: pos+pnl = entry*qty+(current-entry)*qty = current*qty ✓
        #               para SHORT: pos+pnl = entry*qty+(entry-current)*qty ✓
        s_val=sum(float(t.get('position_value',0))+float(t.get('pnl',0)) for t in stocks_open)
        c_op=sum(t.get('pnl',0) for t in crypto_open)
        c_val=sum(float(t.get('position_value',0))+float(t.get('pnl',0)) for t in crypto_open)
        a_op=sum(t.get('pnl',0) for t in arbi_open); a_cl=sum(t.get('pnl',0) for t in arbi_closed)
        a_win=sum(1 for t in arbi_closed if t.get('pnl',0)>0)
        a_d=calc_period_pnl(list(arbi_closed),1); a_w=calc_period_pnl(list(arbi_closed),7)
        a_m=calc_period_pnl(list(arbi_closed),30); a_y=calc_period_pnl(list(arbi_closed),365)
        sc=stocks_capital; cc=crypto_capital; ac=arbi_capital
    s_cl=db_st.get('stocks_pnl',0); c_cl=db_st.get('crypto_pnl',0)
    s_win=int(db_st.get('stocks_wins',0)); c_win=int(db_st.get('crypto_wins',0))
    d_pnl=db_st.get('daily_pnl',0); w_pnl=db_st.get('weekly_pnl',0)
    m_pnl=db_st.get('monthly_pnl',0); y_pnl=db_st.get('annual_pnl',0)
    st=sc+s_val; ct=cc+c_val
    core_total=round(st+ct,2); arbi_total=round(ARBI_CAPITAL+a_cl+a_op,2)
    grand_total=round(core_total+arbi_total,2)  # [v10.14] stocks+crypto+arbi
    initial_global=INITIAL_CAPITAL_STOCKS+INITIAL_CAPITAL_CRYPTO+ARBI_CAPITAL
    # [v10.14-FIX] P&L derivado do grand_total — garante consistência matemática
    # Evita discrepância entre portfolio value e total_pnl reportado
    _true_total_pnl  = round(grand_total - initial_global, 2)
    _true_open_pnl   = round(s_op + c_op + a_op, 2)
    # [v10.14] closed_pnl = DB + arbi_closed — NÃO derivar de open para evitar efeito contrário
    _true_closed_pnl = round(s_cl + c_cl + a_cl, 2)
    # [v10.14-FIX] Win rate e trades incluem arbi
    _arbi_wins = sum(1 for t in arbi_closed if t.get('pnl',0) > 0)
    total_cl_n = int(db_st.get('total', len(stocks_closed)+len(crypto_closed))) + len(arbi_closed)
    total_win  = int(db_st.get('wins', s_win+c_win)) + _arbi_wins
    return jsonify({
        # ─── GLOBAL (stocks + crypto) — arbi NÃO entra aqui ────
        'initial_capital':initial_global,
        'core_portfolio_value':core_total,        # stocks+crypto apenas
        'total_portfolio_value':grand_total,       # [v10.14] stocks+crypto+arbi (correto para display)
        'open_positions_value':round(s_val+c_val,2),'current_capital':round(sc+cc,2),
        'total_pnl':_true_total_pnl,      # [v10.14] = grand_total - initial (matematicamente exato)
        'open_pnl':_true_open_pnl,         # stocks+crypto+arbi open
        'closed_pnl':_true_closed_pnl,     # total - open (consistente com portfolio)
        'gain_percent':round(_true_total_pnl/initial_global*100,2),
        'open_trades':len(stocks_open)+len(crypto_open)+len(arbi_open),
        'closed_trades':total_cl_n,'winning_trades':total_win,
        'win_rate':round(total_win/total_cl_n*100,1) if total_cl_n>0 else 0,
        # [v10.14] Períodos incluem arbi
        'daily_pnl':  round(d_pnl + calc_period_pnl(list(arbi_closed),1),2),
        'daily_pnl_closed': round(d_pnl + calc_period_pnl(list(arbi_closed),1),2),  # só fechadas
        'daily_pnl_total':  round(d_pnl + calc_period_pnl(list(arbi_closed),1) + s_op + c_op + a_op,2),  # fechadas+abertas
        'weekly_pnl': round(w_pnl + calc_period_pnl(list(arbi_closed),7),2),
        'monthly_pnl':round(m_pnl + calc_period_pnl(list(arbi_closed),30),2),
        'annual_pnl': round(y_pnl + calc_period_pnl(list(arbi_closed),365),2),
        'daily_gain_pct':  round((d_pnl + calc_period_pnl(list(arbi_closed),1))/initial_global*100,3),
        'monthly_gain_pct':round((m_pnl + calc_period_pnl(list(arbi_closed),30))/initial_global*100,2),
        'annual_gain_pct': round((y_pnl + calc_period_pnl(list(arbi_closed),365))/initial_global*100,2),
        'best_trade':round(db_st.get('best_trade',0),2),'worst_trade':round(db_st.get('worst_trade',0),2),
        # ─── STOCKS ─────────────────────────────────────────────
        'stocks_capital':round(sc,2),'stocks_portfolio_value':round(st,2),
        'stocks_open_pnl':round(s_op,2),
        'stocks_closed_pnl':round(s_cl,2),  # [v10.14] do banco (pnl gross fechadas)
        'stocks_fees_total':round(sum(t.get('fee_estimated',0) for t in stocks_closed),2),
        'stocks_pnl_net':round(sum(t.get('pnl_net',t.get('pnl',0)) for t in stocks_closed),2),
        'stocks_open_trades':len(stocks_open),
        'stocks_closed_trades':int(db_st.get('stocks_total',len(stocks_closed))),
        'stocks_win_rate':round(db_st.get('stocks_wins',0)/db_st.get('stocks_total',1)*100,1) if db_st.get('stocks_total',0)>0 else 0,
        'stocks_annual_pnl':round(db_st.get('stocks_annual',0),2),
        'stocks_daily_pnl':round(db_st.get('stocks_daily',0),2),
        'stocks_monthly_pnl':round(db_st.get('stocks_monthly',0),2),
        'stocks_deployed':round(db_st.get('stocks_deployed',0),2),
        'stocks_return_pct':round(db_st.get('stocks_pnl',0)/INITIAL_CAPITAL_STOCKS*100,2) if INITIAL_CAPITAL_STOCKS>0 else 0,
        'stocks_return_on_deployed':round(db_st.get('stocks_pnl',0)/db_st.get('stocks_deployed',1)*100,2) if db_st.get('stocks_deployed',0)>0 else 0,
        'stocks_annual_return_pct':round(db_st.get('stocks_annual',0)/INITIAL_CAPITAL_STOCKS*100,2) if INITIAL_CAPITAL_STOCKS>0 else 0,
        # ─── CRYPTO ─────────────────────────────────────────────
        'crypto_capital':round(cc,2),'crypto_portfolio_value':round(ct,2),
        'crypto_open_pnl':round(c_op,2),
        'crypto_closed_pnl':round(c_cl,2),  # [v10.14] do banco (pnl gross fechadas)
        'crypto_fees_total':round(sum(t.get('fee_estimated',0) for t in crypto_closed),2),
        'crypto_pnl_net':round(sum(t.get('pnl_net',t.get('pnl',0)) for t in crypto_closed),2),
        'crypto_open_trades':len(crypto_open),
        'crypto_closed_trades':int(db_st.get('crypto_total',len(crypto_closed))),
        'crypto_win_rate':round(db_st.get('crypto_wins',0)/db_st.get('crypto_total',1)*100,1) if db_st.get('crypto_total',0)>0 else 0,
        'crypto_annual_pnl':round(db_st.get('crypto_annual',0),2),
        'crypto_daily_pnl':round(db_st.get('crypto_daily',0),2),
        'crypto_monthly_pnl':round(db_st.get('crypto_monthly',0),2),
        'crypto_deployed':round(db_st.get('crypto_deployed',0),2),
        'crypto_return_pct':round(db_st.get('crypto_pnl',0)/INITIAL_CAPITAL_CRYPTO*100,2) if INITIAL_CAPITAL_CRYPTO>0 else 0,
        'crypto_return_on_deployed':round(db_st.get('crypto_pnl',0)/db_st.get('crypto_deployed',1)*100,2) if db_st.get('crypto_deployed',0)>0 else 0,
        'crypto_annual_return_pct':round(db_st.get('crypto_annual',0)/INITIAL_CAPITAL_CRYPTO*100,2) if INITIAL_CAPITAL_CRYPTO>0 else 0,
        # ─── ARBI (SEGREGADO) ───────────────────────────────────
        'arbi_book': {
            'segregated': True,
            'note': 'Arbi capital is separate — not included in core_portfolio_value',
            'capital': round(ac,2), 'initial_capital': ARBI_CAPITAL,
            'portfolio_value': arbi_total,
            'open_pnl': round(a_op,2), 'closed_pnl': round(a_cl,2),
            'total_pnl': round(a_op+a_cl,2),
            'gain_percent': round((arbi_total-ARBI_CAPITAL)/ARBI_CAPITAL*100,2),
            'open_trades': len(arbi_open), 'closed_trades': len(arbi_closed),
            'closed_trades_db': int(db_st.get('arbi_count_db', len(arbi_closed))),
            'deployed_capital': round(db_st.get('arbi_deployed',0),2),
            'return_on_deployed': round((a_cl+a_op)/db_st.get('arbi_deployed',1)*100,2) if db_st.get('arbi_deployed',0)>0 else 0,
            'winning_trades': a_win,
            'win_rate': round(a_win/len(arbi_closed)*100,1) if arbi_closed else 0,
            'kill_switch': ARBI_KILL_SWITCH,
            'daily_pnl': round(a_d,2), 'weekly_pnl': round(a_w,2),
            'monthly_pnl': round(a_m,2), 'annual_pnl': round(a_y,2),
        },
        'assets_monitored':len(ALL_STOCK_SYMBOLS)+len(CRYPTO_SYMBOLS),
        'kill_switch':RISK_KILL_SWITCH,'market_regime':market_regime,
        'alerts_enabled':ALERTS_ENABLED,
        'market_status':{'b3':is_b3_open(),'nyse':is_nyse_open(),'crypto':True},
        'updated_at':datetime.utcnow().isoformat()
    })

@api_bp.route('/audit')
def audit_route():
    ctx = _get_ctx()
    # [V9-4] cached_recent_only: true — deixa explícito que é cache parcial (últimos 200 do DB + runtime)
    with audit_lock: data=list(reversed(audit_log))[:100]
    return jsonify({'events':data,'total':len(audit_log),
        'cached_recent_only': True,
        'note': 'In-memory cache (last ~200 from DB + runtime). Full history in audit_events table.'})

@api_bp.route('/risk')
def risk_status():
    ctx = _get_ctx()
    with state_lock:
        open_c=len(stocks_open)+len(crypto_open)
        d=calc_period_pnl(stocks_closed+crypto_closed,1)
        w=calc_period_pnl(stocks_closed+crypto_closed,7)
    total_cap=INITIAL_CAPITAL_STOCKS+INITIAL_CAPITAL_CRYPTO
    return jsonify({
        'kill_switch':RISK_KILL_SWITCH,'arbi_kill_switch':ARBI_KILL_SWITCH,
        'limits':{'max_open':MAX_OPEN_POSITIONS,'max_same_symbol':MAX_SAME_SYMBOL,
            'max_daily_dd_pct':MAX_DAILY_DRAWDOWN_PCT,'max_weekly_dd_pct':MAX_WEEKLY_DRAWDOWN_PCT,
            'max_risk_per_trade_pct':MAX_RISK_PER_TRADE_PCT,
            'signal_max_age_min':SIGNAL_MAX_AGE_MIN,'cooldown_sec':SYMBOL_COOLDOWN_SEC},
        'current':{'open_positions':open_c,
            'daily_pnl':d,'daily_dd_pct':round(abs(min(d,0))/total_cap*100,3),
            'weekly_pnl':w,'weekly_dd_pct':round(abs(min(w,0))/total_cap*100,3)},
        'arbi_book':{'capital':arbi_capital,'open':len(arbi_open),
            'max_positions':ARBI_MAX_POSITIONS,'kill_switch':ARBI_KILL_SWITCH,
            'note':'segregated book — own risk limits, separate kill switch'}
    })

@api_bp.route('/risk/reset_kill_switch', methods=['POST'])
def reset_kill_switch():
    ctx = _get_ctx()
    global RISK_KILL_SWITCH
    data=request.get_json() or {}
    if data.get('confirm')!='RESET': return jsonify({'error':'Send {"confirm":"RESET"}'}),400
    RISK_KILL_SWITCH=False
    # Limpar cache de sinais bloqueados por kill_switch — permite reavaliação imediata
    ks_reasons = {'kill_switch','KILL_SWITCH_ACTIVE','KILL_SWITCH','ARBI_KILL_SWITCH'}
    cleared = 0
    with learning_lock:
        keys_to_del = [k for k,v in processed_signal_ids.items() if v.get('reason') in ks_reasons]
        for k in keys_to_del:
            del processed_signal_ids[k]
            cleared += 1
    audit('KILL_SWITCH_RESET',{'by':'manual_api','cache_cleared':cleared})
    log.info(f'[KS] Kill switch reset — {cleared} sinais liberados do cache')
    return jsonify({'ok':True,'kill_switch':False,'signals_released':cleared})

@api_bp.route('/trades/correct', methods=['POST'])
@require_auth
def correct_trade():
    """[v10.8] Corrigir exit_price/pnl de trade fechada com preço errado (bug de API)."""
    data = request.get_json() or {}
    trade_id = data.get('trade_id')
    new_exit  = float(data.get('exit_price', 0))
    reason    = data.get('reason', 'price_correction')
    if not trade_id or new_exit <= 0:
        return jsonify({'error': 'Informe trade_id e exit_price > 0'}), 400
    conn = get_db()
    if not conn: return jsonify({'error': 'DB unavailable'}), 503
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute('SELECT * FROM trades WHERE id=%s AND status=%s', (trade_id, 'CLOSED'))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return jsonify({'error': f'Trade {trade_id} não encontrada ou não fechada'}), 404
        entry  = float(row.get('entry_price') or 0)
        pos_v  = float(row.get('position_value') or 0)
        direction = row.get('direction', 'LONG')
        if entry <= 0 or pos_v <= 0:
            cur.close(); conn.close()
            return jsonify({'error': 'entry_price ou position_value inválidos na trade'}), 400
        qty    = pos_v / entry
        if direction == 'LONG':
            new_pnl = (new_exit - entry) * qty
        else:
            new_pnl = (entry - new_exit) * qty
        new_pnl_pct = (new_pnl / pos_v) * 100
        old_exit = float(row.get('exit_price') or row.get('current_price') or 0)
        old_pnl  = float(row.get('pnl') or 0)
        cur.execute(
            'UPDATE trades SET exit_price=%s, current_price=%s, pnl=%s, pnl_pct=%s, close_reason=%s WHERE id=%s',
            (new_exit, new_exit, round(new_pnl, 4), round(new_pnl_pct, 4), reason, trade_id)
        )
        conn.commit(); cur.close(); conn.close()
        # Atualizar em memória também
        for lst in (stocks_closed, crypto_closed):
            for t in lst:
                if t.get('id') == trade_id:
                    t['exit_price'] = new_exit; t['current_price'] = new_exit
                    t['pnl'] = round(new_pnl, 4); t['pnl_pct'] = round(new_pnl_pct, 4)
                    t['close_reason'] = reason
        audit('TRADE_CORRECTED', {'id': trade_id, 'old_exit': old_exit, 'new_exit': new_exit,
                                   'old_pnl': old_pnl, 'new_pnl': round(new_pnl, 4)})
        log.info(f'[CORRECTION] {trade_id}: exit {old_exit}→{new_exit}, pnl {old_pnl:+.2f}→{new_pnl:+.2f}')
        return jsonify({'ok': True, 'trade_id': trade_id,
                        'old_exit': old_exit, 'new_exit': new_exit,
                        'old_pnl': old_pnl, 'new_pnl': round(new_pnl, 4),
                        'new_pnl_pct': round(new_pnl_pct, 4)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/trades/void', methods=['POST'])
@require_auth
def void_trade():
    """[v10.9] Anular trades fechadas com erro — remove da memória e marca VOID no banco.
    Devolve o capital como se a trade nunca tivesse existido (pnl=0).
    Uso: POST /trades/void  body: {"trade_ids": ["STK-xxx","STK-yyy"], "reason":"loop_error"}
    """
    ctx = _get_ctx()
    data = request.get_json() or {}
    trade_ids = data.get('trade_ids', [])
    reason    = data.get('reason', 'void_error')
    if not trade_ids:
        return jsonify({'error': 'Informe trade_ids (lista)'}), 400

    conn = get_db()
    if not conn: return jsonify({'error': 'DB unavailable'}), 503

    results = []
    try:
        cur = conn.cursor(dictionary=True)
        for tid in trade_ids:
            cur.execute('SELECT * FROM trades WHERE id=%s AND status=%s', (tid, 'CLOSED'))
            row = cur.fetchone()
            if not row:
                results.append({'id': tid, 'status': 'not_found'})
                continue
            # Marcar como VOID no banco
            cur.execute(
                "UPDATE trades SET status='VOID', close_reason=%s WHERE id=%s",
                (f'VOID:{reason}', tid)
            )
            # Remover da memória e devolver capital
            with state_lock:
                global stocks_capital, crypto_capital
                orig_pnl = float(row.get('pnl') or 0)
                pos_v    = float(row.get('position_value') or 0)
                asset_type = row.get('asset_type', 'stock')
                # Reverte: descapitaliza o pnl que foi somado ao capital quando fechou
                # Ao fechar: capital += pos_v + pnl. Para anular: capital -= pos_v (devolve apenas pos_v)
                # Na prática: capital += (pos_v + 0) - (pos_v + pnl) = -pnl
                if asset_type == 'crypto':
                    crypto_capital -= orig_pnl  # se pnl negativo, devolve capital
                    crypto_closed[:] = [t for t in crypto_closed if t.get('id') != tid]
                else:
                    stocks_capital -= orig_pnl
                    stocks_closed[:] = [t for t in stocks_closed if t.get('id') != tid]
            results.append({'id': tid, 'status': 'voided', 'pnl_reversed': round(orig_pnl, 2)})
        conn.commit(); cur.close(); conn.close()
        audit('TRADES_VOIDED', {'count': len([r for r in results if r['status']=='voided']), 'reason': reason, 'ids': trade_ids})
        total_reversed = sum(r.get('pnl_reversed',0) for r in results if r['status']=='voided')
        return jsonify({'ok': True, 'results': results, 'total_pnl_reversed': round(total_reversed, 2)})
    except Exception as e:
        conn.rollback(); conn.close()
        return jsonify({'error': str(e)}), 500

@api_bp.route('/debug/drawdown')
@require_auth
def debug_drawdown():
    """Mostra exatamente o que check_risk vê para drawdown."""
    from datetime import timedelta
    with state_lock:
        s_closed = list(stocks_closed); c_closed = list(crypto_closed)
    total_cap = INITIAL_CAPITAL_STOCKS + INITIAL_CAPITAL_CRYPTO
    cutoff_d = (datetime.utcnow()-timedelta(days=1)).isoformat()
    cutoff_w = (datetime.utcnow()-timedelta(days=7)).isoformat()
    daily_losses = [t for t in s_closed+c_closed if t.get('closed_at','')>=cutoff_d and t.get('pnl',0)<0]
    weekly_losses= [t for t in s_closed+c_closed if t.get('closed_at','')>=cutoff_w and t.get('pnl',0)<0]
    daily_loss   = sum(t.get('pnl',0) for t in daily_losses)
    weekly_loss  = sum(t.get('pnl',0) for t in weekly_losses)
    dd_d = abs(daily_loss)/total_cap*100
    dd_w = abs(weekly_loss)/total_cap*100
    top5 = sorted(daily_losses, key=lambda t: t.get('pnl',0))[:5]
    return jsonify({
        'kill_switch': RISK_KILL_SWITCH,
        'in_memory': {'stocks_closed': len(s_closed), 'crypto_closed': len(c_closed)},
        'daily': {'loss': round(daily_loss,2), 'dd_pct': round(dd_d,4), 'limit': MAX_DAILY_DRAWDOWN_PCT, 'count': len(daily_losses)},
        'weekly': {'loss': round(weekly_loss,2), 'dd_pct': round(dd_w,4), 'limit': MAX_WEEKLY_DRAWDOWN_PCT, 'count': len(weekly_losses)},
        'top5_daily_losses': [{'id':t.get('id'),'sym':t.get('symbol'),'pnl':t.get('pnl'),'at':t.get('closed_at','')} for t in top5],
        'would_trigger': dd_d >= MAX_DAILY_DRAWDOWN_PCT or dd_w >= MAX_WEEKLY_DRAWDOWN_PCT,
    })


@api_bp.route('/db/audit')
def db_audit():
    """[v10.11] Auditoria direta do banco — conta TODAS as trades sem limite de memória."""
    ctx = _get_ctx()
    try:
        conn = get_db()
        if not conn: return jsonify({'error':'db unavailable'}), 500
        cursor = conn.cursor(dictionary=True)
        
        # Contagem total por tipo e status
        cursor.execute("SELECT asset_type, status, COUNT(*) as n FROM trades GROUP BY asset_type, status ORDER BY asset_type, status")
        by_type = cursor.fetchall()
        
        # Total geral
        cursor.execute("SELECT COUNT(*) as total FROM trades")
        total = cursor.fetchone()['total']
        
        # Primeira e última trade
        cursor.execute("SELECT MIN(opened_at) as primeira, MAX(closed_at) as ultima FROM trades")
        dates = cursor.fetchone()
        
        # Por mês
        cursor.execute("""SELECT DATE_FORMAT(closed_at,'%Y-%m') as mes, asset_type, COUNT(*) as n, 
            SUM(pnl) as pnl, SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins
            FROM trades WHERE status='CLOSED' 
            GROUP BY mes, asset_type ORDER BY mes, asset_type""")
        by_month = cursor.fetchall()
        
        # Arbi
        cursor.execute("SELECT COUNT(*) as total FROM arbi_trades")
        arbi_total = cursor.fetchone()['total']
        cursor.execute("SELECT status, COUNT(*) as n FROM arbi_trades GROUP BY status")
        arbi_by_status = cursor.fetchall()
        
        cursor.close(); conn.close()
        return jsonify({
            'trades_by_type_status': [{**r, 'n': int(r['n'])} for r in by_type],
            'total_trades': int(total),
            'date_range': {k: str(v) for k,v in dates.items()},
            'by_month': [{**r, 'n': int(r['n']), 'wins': int(r['wins']), 
                          'pnl': float(r['pnl'] or 0)} for r in by_month],
            'arbi_total': int(arbi_total),
            'arbi_by_status': [{**r, 'n': int(r['n'])} for r in arbi_by_status],
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/performance/stocks')
def performance_stocks():
    """[v10.11] Dados detalhados de performance histórica de stocks."""
    try:
        conn = get_db()
        if not conn: return jsonify({'error':'db unavailable'}), 500
        cursor = conn.cursor(dictionary=True)
        # Diário
        cursor.execute("""
            ctx = _get_ctx()
            SELECT DATE(closed_at) as dt,
                COUNT(*) as n, SUM(pnl) as pnl,
                SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins,
                SUM(position_value) as deployed,
                AVG(TIMESTAMPDIFF(MINUTE,opened_at,closed_at))/60 as avg_dur_h
            FROM trades WHERE status='CLOSED' AND asset_type='stock'
            GROUP BY DATE(closed_at) ORDER BY dt""")
        daily = [{**r,'dt':str(r['dt']),'pnl':float(r['pnl'] or 0),'deployed':float(r['deployed'] or 0),
                  'n':int(r['n']),'wins':int(r['wins']),'avg_dur_h':round(float(r['avg_dur_h'] or 0),2)} for r in cursor.fetchall()]
        # Por símbolo
        cursor.execute("""
            SELECT symbol, market, COUNT(*) as n, SUM(pnl) as pnl,
                SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins,
                MAX(pnl) as best, MIN(pnl) as worst, AVG(pnl) as avg_pnl,
                SUM(position_value) as deployed
            FROM trades WHERE status='CLOSED' AND asset_type='stock'
            GROUP BY symbol, market ORDER BY pnl DESC""")
        by_sym = [{**r,'pnl':float(r['pnl'] or 0),'wins':int(r['wins']),'n':int(r['n']),
                   'best':float(r['best'] or 0),'worst':float(r['worst'] or 0),
                   'avg_pnl':float(r['avg_pnl'] or 0),'deployed':float(r['deployed'] or 0)} for r in cursor.fetchall()]
        # Por motivo de fechamento
        cursor.execute("""
            SELECT close_reason, COUNT(*) as n, SUM(pnl) as pnl,
                SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins
            FROM trades WHERE status='CLOSED' AND asset_type='stock'
            GROUP BY close_reason ORDER BY n DESC""")
        by_reason = [{**r,'pnl':float(r['pnl'] or 0),'n':int(r['n']),'wins':int(r['wins'])} for r in cursor.fetchall()]
        # Global
        cursor.execute("""SELECT COUNT(*) as n, SUM(pnl) as pnl, SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins,
            MAX(pnl) as best, MIN(pnl) as worst, AVG(pnl) as avg_pnl, SUM(position_value) as deployed,
            AVG(TIMESTAMPDIFF(MINUTE,opened_at,closed_at))/60 as avg_dur_h
            FROM trades WHERE status='CLOSED' AND asset_type='stock'""")
        glb = cursor.fetchone()
        cursor.close(); conn.close()
        with state_lock:
            open_t  = list(stocks_open)
            _fees   = round(sum(t.get('fee_estimated',0) for t in stocks_closed), 2)
            _pnl_net= round(sum(t.get('pnl_net', t.get('pnl',0)) for t in stocks_closed), 2)
        glb_d = {k:float(v or 0) if isinstance(v,(int,float,type(None))) else str(v) for k,v in glb.items()}
        glb_d['fee_estimated_total'] = _fees
        glb_d['pnl_net_total']       = _pnl_net
        return jsonify({
            'global': glb_d,
            'daily': daily, 'by_symbol': by_sym, 'by_reason': by_reason,
            'open_trades': len(open_t),
            'initial_capital': INITIAL_CAPITAL_STOCKS,
        })
    except Exception as e: return jsonify({'error':str(e)}), 500

@api_bp.route('/performance/crypto')
def performance_crypto():
    """[v10.11] Dados detalhados de performance histórica de crypto."""
    try:
        conn = get_db()
        if not conn: return jsonify({'error':'db unavailable'}), 500
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            ctx = _get_ctx()
            SELECT DATE(closed_at) as dt,
                COUNT(*) as n, SUM(pnl) as pnl,
                SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins,
                SUM(position_value) as deployed,
                AVG(TIMESTAMPDIFF(MINUTE,opened_at,closed_at))/60 as avg_dur_h
            FROM trades WHERE status='CLOSED' AND asset_type='crypto'
            GROUP BY DATE(closed_at) ORDER BY dt""")
        daily = [{**r,'dt':str(r['dt']),'pnl':float(r['pnl'] or 0),'deployed':float(r['deployed'] or 0),
                  'n':int(r['n']),'wins':int(r['wins']),'avg_dur_h':round(float(r['avg_dur_h'] or 0),2)} for r in cursor.fetchall()]
        cursor.execute("""
            SELECT symbol, COUNT(*) as n, SUM(pnl) as pnl,
                SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins,
                MAX(pnl) as best, MIN(pnl) as worst, AVG(pnl) as avg_pnl,
                SUM(position_value) as deployed
            FROM trades WHERE status='CLOSED' AND asset_type='crypto'
            GROUP BY symbol ORDER BY pnl DESC""")
        by_sym = [{**r,'pnl':float(r['pnl'] or 0),'wins':int(r['wins']),'n':int(r['n']),
                   'best':float(r['best'] or 0),'worst':float(r['worst'] or 0),
                   'avg_pnl':float(r['avg_pnl'] or 0),'deployed':float(r['deployed'] or 0)} for r in cursor.fetchall()]
        cursor.execute("""
            SELECT close_reason, COUNT(*) as n, SUM(pnl) as pnl,
                SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins
            FROM trades WHERE status='CLOSED' AND asset_type='crypto'
            GROUP BY close_reason ORDER BY n DESC""")
        by_reason = [{**r,'pnl':float(r['pnl'] or 0),'n':int(r['n']),'wins':int(r['wins'])} for r in cursor.fetchall()]
        cursor.execute("""SELECT COUNT(*) as n, SUM(pnl) as pnl, SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins,
            MAX(pnl) as best, MIN(pnl) as worst, AVG(pnl) as avg_pnl, SUM(position_value) as deployed,
            AVG(TIMESTAMPDIFF(MINUTE,opened_at,closed_at))/60 as avg_dur_h
            FROM trades WHERE status='CLOSED' AND asset_type='crypto'""")
        glb = cursor.fetchone()
        cursor.close(); conn.close()
        with state_lock:
            open_t   = list(crypto_open)
            _fees_c  = round(sum(t.get('fee_estimated',0) for t in crypto_closed), 2)
            _net_c   = round(sum(t.get('pnl_net', t.get('pnl',0)) for t in crypto_closed), 2)
        glb_cd = {k:float(v or 0) if isinstance(v,(int,float,type(None))) else str(v) for k,v in glb.items()}
        glb_cd['fee_estimated_total'] = _fees_c
        glb_cd['pnl_net_total']       = _net_c
        return jsonify({
            'global': glb_cd,
            'daily': daily, 'by_symbol': by_sym, 'by_reason': by_reason,
            'open_trades': len(open_t),
            'initial_capital': INITIAL_CAPITAL_CRYPTO,
        })
    except Exception as e: return jsonify({'error':str(e)}), 500

@api_bp.route('/risk/reset_arbi_kill_switch', methods=['POST'])
def reset_arbi_kill_switch():
    ctx = _get_ctx()
    global ARBI_KILL_SWITCH
    data=request.get_json() or {}
    if data.get('confirm')!='RESET': return jsonify({'error':'Send {"confirm":"RESET"}'}),400
    ARBI_KILL_SWITCH=False; audit('ARBI_KILL_SWITCH_RESET',{'by':'manual_api'})
    return jsonify({'ok':True,'arbi_kill_switch':False})

@api_bp.route('/settings', methods=['GET','POST'])
def settings_endpoint():
    ctx = _get_ctx()
    global RISK_KILL_SWITCH, KILL_SWITCH_USD, STOCK_TP_PCT, STOCK_SL_PCT
    global TRAILING_FLOOR_PCT, TRAILING_TRIGGER_PCT, TIMEOUT_B3_H, TIMEOUT_CRYPTO_H
    global TIMEOUT_NYSE_H, MIN_SCORE_AUTO, DEFAULT_POSITION_SIZE
    global MAX_POSITIONS_STOCKS, MAX_POSITIONS_CRYPTO, MAX_POSITIONS_NYSE
    if request.method == 'POST':
        d = request.get_json() or {}
        if 'kill_switch_active' in d: RISK_KILL_SWITCH = bool(d['kill_switch_active'])
        if 'kill_switch_usd' in d: KILL_SWITCH_USD = float(d['kill_switch_usd'])
        if 'stock_tp_pct' in d: STOCK_TP_PCT = float(d['stock_tp_pct'])
        if 'stock_sl_pct' in d: STOCK_SL_PCT = float(d['stock_sl_pct'])
        if 'trailing_floor_pct' in d: TRAILING_FLOOR_PCT = float(d['trailing_floor_pct'])
        if 'trailing_trigger_pct' in d: TRAILING_TRIGGER_PCT = float(d['trailing_trigger_pct'])
        if 'timeout_b3_h' in d: TIMEOUT_B3_H = float(d['timeout_b3_h'])
        if 'timeout_crypto_h' in d: TIMEOUT_CRYPTO_H = float(d['timeout_crypto_h'])
        if 'timeout_nyse_h' in d: TIMEOUT_NYSE_H = float(d['timeout_nyse_h'])
        if 'min_score_auto' in d: MIN_SCORE_AUTO = int(d['min_score_auto'])
        if 'default_position_size' in d: DEFAULT_POSITION_SIZE = float(d['default_position_size'])
        if 'max_positions_b3' in d: MAX_POSITIONS_STOCKS = int(d['max_positions_b3'])
        if 'max_positions_crypto' in d: MAX_POSITIONS_CRYPTO = int(d['max_positions_crypto'])
        if 'max_positions_nyse' in d: MAX_POSITIONS_NYSE = int(d['max_positions_nyse'])
        if 'arbi_capital_add' in d:  # adiciona capital ao pool arbi (positivo=deposita, negativo=retira)
            global arbi_capital
            with state_lock:
                arbi_capital = max(0, arbi_capital + float(d['arbi_capital_add']))
            log.info(f'ARBI CAPITAL ajustado em {d["arbi_capital_add"]:+,.0f} → novo total: {arbi_capital:,.0f}')
            # Persistir no banco
            try:
                _ca = get_db()
                if _ca:
                    _cr2 = _ca.cursor()
                    _cr2.execute("CREATE TABLE IF NOT EXISTS runtime_settings (key_name VARCHAR(60) PRIMARY KEY, value_float DOUBLE, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP) ENGINE=InnoDB")
                    _cr2.execute("INSERT INTO runtime_settings (key_name,value_float) VALUES ('ARBI_CAPITAL_TOTAL',%s) ON DUPLICATE KEY UPDATE value_float=%s,updated_at=NOW()", (arbi_capital+sum(t.get('position_size',0) for t in arbi_open), arbi_capital+sum(t.get('position_size',0) for t in arbi_open)))
                    _ca.commit(); _cr2.close(); _ca.close()
            except Exception as _ea: log.error(f'arbi capital persist: {_ea}')
        audit('SETTINGS_UPDATED', d)
        # [v10.9] Persistir settings no banco — sobrevive restarts
        try:
            _cs = get_db()
            if _cs:
                _cr = _cs.cursor()
                _cr.execute("CREATE TABLE IF NOT EXISTS runtime_settings (key_name VARCHAR(60) PRIMARY KEY, value_float DOUBLE, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB")
                for _k, _v in [('MIN_SCORE_AUTO', MIN_SCORE_AUTO), ('TRAILING_TRIGGER_PCT', TRAILING_TRIGGER_PCT), ('STOCK_SL_PCT', STOCK_SL_PCT)]:
                    _cr.execute("INSERT INTO runtime_settings (key_name,value_float) VALUES (%s,%s) ON DUPLICATE KEY UPDATE value_float=%s,updated_at=NOW()", (_k,_v,_v))
                _cs.commit(); _cr.close(); _cs.close()
        except Exception as _e: log.error(f'settings persist: {_e}')
    return jsonify({
        'kill_switch_active': RISK_KILL_SWITCH,
        'kill_switch_usd': KILL_SWITCH_USD,
        'stock_tp_pct': STOCK_TP_PCT,
        'stock_sl_pct': STOCK_SL_PCT,
        'trailing_floor_pct': TRAILING_FLOOR_PCT,
        'trailing_trigger_pct': TRAILING_TRIGGER_PCT,
        'timeout_b3_h': TIMEOUT_B3_H,
        'timeout_crypto_h': TIMEOUT_CRYPTO_H,
        'timeout_nyse_h': TIMEOUT_NYSE_H,
        'min_score_auto': MIN_SCORE_AUTO,
        'default_position_size': DEFAULT_POSITION_SIZE,
        'max_positions_b3': MAX_POSITIONS_STOCKS,
        'max_positions_crypto': MAX_POSITIONS_CRYPTO,
        'max_positions_nyse': MAX_POSITIONS_NYSE,
    })

@api_bp.route('/alerts/test')
def alerts_test():
    ctx = _get_ctx()
    ok=_send_whatsapp_direct(f"Egreja AI v10.7.0 test {datetime.now().strftime('%d/%m %H:%M')}")
    return jsonify({'sent':ok,'enabled':ALERTS_ENABLED})

@api_bp.route('/arbitrage/learning')
def arbi_learning_status():
    """[v10.14] Estado do aprendizado de thresholds por par."""""
    if request.headers.get('X-API-Key') != API_SECRET_KEY:
        return jsonify({'error':'unauthorized'}),401
    with _arbi_learning_lock:
        stats = dict(_arbi_pair_stats)
    result = []
    for pair_id, st in stats.items():
        buckets = [{'spread':b,'n':v['n'],'wr':round(v['wins']/v['n']*100,1),
                    'avg_pnl':round(v['pnl']/v['n'],0)}
                   for b,v in sorted(st['spread_buckets'].items()) if v['n']>=1]
        result.append({
            'pair_id': pair_id,
            'n_trades': st['n'],
            'total_pnl': round(st['pnl'],2),
            'low_threshold': st['low_threshold'],
            'high_threshold': st['high_threshold'],
            'no_entry_zone': [st['no_entry_low'], st['no_entry_high']],
            'last_updated': st['last_updated'],
            'buckets': buckets,
            'status': 'aprendendo' if st['low_threshold'] is None else 'ativo'
        })
    return jsonify({'pairs': result, 'total_pairs_learning': len(result)})

@api_bp.route('/admin/arbi-kill-switch/reset', methods=['POST'])
@require_auth
def arbi_kill_switch_reset():
    """[v10.14] Reseta o ARBI_KILL_SWITCH manualmente."""
    ctx = _get_ctx()
    global ARBI_KILL_SWITCH
    ARBI_KILL_SWITCH = False
    log.info('[ADMIN] ARBI_KILL_SWITCH resetado manualmente')
    return jsonify({'ok': True, 'arbi_kill_switch': ARBI_KILL_SWITCH})

@api_bp.route('/arbitrage/spreads')
def arbi_spreads_route():
    ctx = _get_ctx()
    with state_lock: spreads=list(arbi_spreads.values())
    spreads.sort(key=lambda x:x['abs_spread'],reverse=True)
    return jsonify({'spreads':spreads,'opportunities':[s for s in spreads if s['opportunity']],
        'total_pairs':len(ARBI_PAIRS),'monitored':len(spreads),'fx_rates':fx_rates,
        'arbi_kill_switch':ARBI_KILL_SWITCH,'updated_at':datetime.utcnow().isoformat()})

@api_bp.route('/arbitrage/force-close', methods=['POST'])
def arbi_force_close():
    """[v10.14] Fechar trade arbi manualmente — remove da memória e fecha no banco."""
    global arbi_capital
    data = request.json or {}
    trade_id = data.get('trade_id', '')
    if not trade_id:
        return jsonify({'error': 'trade_id required'}), 400
    with state_lock:
        trade = next((t for t in arbi_open if t.get('id') == trade_id), None)
        if not trade:
            return jsonify({'error': 'trade not found in open list'}), 404
        # Fechar em memória
        pnl = float(trade.get('pnl', 0) or 0)
        pos = float(trade.get('position_size', 0) or 0)
        arbi_capital += pos + pnl
        closed = dict(trade)
        closed.update({'status': 'CLOSED', 'close_reason': 'MANUAL_CLOSE', 
                       'closed_at': datetime.utcnow().isoformat(), 'pnl': pnl})
        arbi_closed.insert(0, closed)
        arbi_open[:] = [t for t in arbi_open if t['id'] != trade_id]
    # Fechar no banco
    conn = get_db()
    if conn:
        try:
            c = conn.cursor()
            c.execute("UPDATE arbi_trades SET status='CLOSED', close_reason='MANUAL_CLOSE', closed_at=NOW() WHERE id=%s", (trade_id,))
            conn.commit()
        except Exception as e:
            log.error(f'arbi_force_close db: {e}')
        finally:
            try: conn.close()
            except: pass
    audit('ARBI_CLOSED', {'id': trade_id, 'pair': trade.get('pair_id'), 'pnl': pnl, 'reason': 'MANUAL_CLOSE'})
    log.info(f'[MANUAL] Arbi trade {trade_id} fechada: pos=${pos:,.0f} pnl={pnl:>+,.0f} capital_adj={pos+pnl:>+,.0f}')
    return jsonify({'ok': True, 'trade_id': trade_id, 'pnl': pnl, 'position': pos, 'capital_returned': pos + pnl})

@api_bp.route('/arbitrage/purge', methods=['POST'])
@require_auth
def arbitrage_purge():
    """[v10.9] Deletar/corrigir trade arbi problemática do banco e memória."""
    ctx = _get_ctx()
    data = request.get_json() or {}
    trade_id = data.get('trade_id')
    confirm  = data.get('confirm','')
    new_pnl  = data.get('new_pnl', None)  # se fornecido, corrige o pnl em vez de deletar
    if not trade_id or confirm != 'PURGE':
        return jsonify({'error': 'Informe trade_id e confirm=PURGE'}), 400
    conn = get_db()
    if not conn: return jsonify({'error': 'DB unavailable'}), 503
    deleted = 0; corrected = False; old_pnl = 0
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute('SELECT * FROM arbi_trades WHERE id=%s', (trade_id,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return jsonify({'error': f'{trade_id} não encontrado em arbi_trades'}), 404
        old_pnl = float(row.get('pnl') or 0)
        if new_pnl is not None:
            # Corrigir PnL em vez de deletar
            cur.execute('UPDATE arbi_trades SET pnl=%s, pnl_pct=%s WHERE id=%s',
                       (float(new_pnl), round(float(new_pnl)/float(row.get('position_size',1000000))*100,4), trade_id))
            conn.commit(); corrected = True
        else:
            cur.execute('DELETE FROM arbi_trades WHERE id=%s', (trade_id,))
            conn.commit(); deleted = 1
        cur.close(); conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    # Atualizar memória
    global arbi_capital
    with state_lock:
        if new_pnl is not None:
            # Corrigir em memória
            for t in arbi_closed:
                if t.get('id') == trade_id:
                    diff = float(new_pnl) - old_pnl
                    t['pnl'] = float(new_pnl)
                    arbi_capital += diff
                    break
        else:
            before = len(arbi_closed)
            arbi_closed[:] = [t for t in arbi_closed if t.get('id') != trade_id]
            if len(arbi_closed) < before:
                arbi_capital += old_pnl  # devolver o PnL ao capital
    audit('ARBI_PURGE', {'id': trade_id, 'old_pnl': old_pnl, 'new_pnl': new_pnl, 'deleted': deleted})
    return jsonify({'ok': True, 'trade_id': trade_id, 'old_pnl': old_pnl,
                   'deleted': deleted, 'corrected': corrected, 'new_pnl': new_pnl})

@api_bp.route('/arbitrage/fix-trade', methods=['POST'])
def arbi_fix_trade():
    """[v10.14] Corrigir P&L de trade arbi com dado inválido."""
    d = request.get_json(force=True)
    trade_id = d.get('trade_id')
    new_pnl   = float(d.get('pnl', 0))
    reason    = d.get('reason', 'data_correction')
    if not trade_id:
        return jsonify({'error': 'trade_id required'}), 400
    conn = get_db()
    if not conn: return jsonify({'error': 'db'}), 500
    try:
        cur = conn.cursor()
        pos_size = 0
        cur.execute("SELECT position_size, pnl FROM arbi_trades WHERE id=%s", (trade_id,))
        row = cur.fetchone()
        if not row: return jsonify({'error': 'not found'}), 404
        old_pnl  = float(row[1] or 0) if row else 0
        pos_size = float(row[0] or 1) if row else 1
        new_pnl_pct = round(new_pnl / pos_size * 100, 4)
        cur.execute("UPDATE arbi_trades SET pnl=%s, pnl_pct=%s WHERE id=%s",
                    (new_pnl, new_pnl_pct, trade_id))
        conn.commit()
        # Corrigir capital em memória
        global arbi_capital
        diff = new_pnl - old_pnl
        with state_lock:
            arbi_capital += diff  # devolver diferença ao capital
        log.info(f'[ARBI-FIX] {trade_id}: pnl {old_pnl:+,.0f} → {new_pnl:+,.0f} (diff={diff:+,.0f}) reason={reason}')
        return jsonify({'ok': True, 'old_pnl': old_pnl, 'new_pnl': new_pnl, 'diff': diff, 'capital_adj': diff})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        try: cur.close(); conn.close()
        except: pass

@api_bp.route('/arbitrage/trades')
def arbi_trades_route():
    with state_lock:
        open_t=list(arbi_open); closed_t=list(arbi_closed); cap=arbi_capital
        c_pnl=sum(t.get('pnl',0) for t in arbi_closed); o_pnl=sum(t.get('pnl',0) for t in arbi_open)
        winners=sum(1 for t in arbi_closed if t.get('pnl',0)>0)
    return jsonify({'open_trades':open_t,'closed_trades':closed_t,'capital':round(cap,2),
        'initial_capital':ARBI_CAPITAL,'open_pnl':round(o_pnl,2),
        'closed_pnl':round(c_pnl,2),'total_pnl':round(o_pnl+c_pnl,2),
        'win_rate':round(winners/len(arbi_closed)*100,1) if arbi_closed else 0,
        'open_count':len(open_t),'closed_count':len(arbi_closed),'kill_switch':ARBI_KILL_SWITCH,
        'book':'SEGREGATED — own risk limits, separate capital',
        'parameters':{'min_spread':ARBI_MIN_SPREAD,'tp_spread':ARBI_TP_SPREAD,
            'sl_pct':ARBI_SL_PCT,'timeout_h':ARBI_TIMEOUT_H,
            'position_size':ARBI_POS_SIZE,'max_positions':ARBI_MAX_POSITIONS}})

@api_bp.route('/orders')
def orders_route():
    limit=min(int(request.args.get('limit',50)),500)
    status=request.args.get('status','')
    with orders_lock: data=list(reversed(orders_log))
    filtered=[o for o in data if not status or o.get('status')==status]
    # [V9-4] cached_recent_only: deixa explícito que é cache parcial
    return jsonify({'orders':filtered[:limit],'total':len(orders_log),
        'cached_recent_only': True,
        'note': 'In-memory cache (last ~500 from DB + runtime). Full history in orders table.'})

@api_bp.route('/portfolio/snapshots')
def portfolio_snapshots():
    conn=get_db()
    if not conn: return jsonify({'error':'DB unavailable'}),503
    try:
        limit=min(int(request.args.get('limit',100)),1000)
        cursor=conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM portfolio_snapshots ORDER BY ts DESC LIMIT %s",(limit,))
        rows=cursor.fetchall(); cursor.close(); conn.close()
        for r in rows:
            for k,v in r.items():
                if isinstance(v,datetime): r[k]=v.isoformat()
        return jsonify({'snapshots':rows,'total':len(rows)})
    except Exception as e: return jsonify({'error':str(e)}),500

@api_bp.route('/data/quality')
def data_quality_route():
    with dq_lock: dq=dict(data_quality)
    stale=[s for s in dq.values() if s.get('stale')]
    low_quality=[s for s in dq.values() if s.get('quality',100)<60]
    return jsonify({'symbols':list(dq.values()),'total':len(dq),
        'stale_count':len(stale),'low_quality_count':len(low_quality),
        'stale_symbols':[s['symbol'] for s in stale],
        'timestamp':datetime.utcnow().isoformat()})

# ═══════════════════════════════════════════════════════════════
# [L] ENDPOINTS DE LEARNING & INSIGHT ENGINE
# ═══════════════════════════════════════════════════════════════

@api_bp.route('/learning/status')
@require_auth
def learning_status():
    """[L][FIX-6] Status geral do Learning Engine com métricas de calibração."""
    ctx = _get_ctx()
    with learning_lock:
        n_patterns = len(pattern_stats_cache)
        n_factors  = len(factor_stats_cache)
        patterns_above_min = sum(1 for p in pattern_stats_cache.values()
                                 if p.get('total_samples', 0) >= LEARNING_MIN_SAMPLES)

    # [FIX-6] Calcular métricas de calibração a partir do banco (últimos 500 sinais)
    calib = {'avg_confidence_winners': None, 'avg_confidence_losers': None,
             'confidence_band_stats': {}, 'total_attributed': 0,
             'realtime_calibration': dict(_calibration_tracker)}
    try:
        conn = get_db()
        if conn:
            c = conn.cursor(dictionary=True)
            c.execute("""SELECT outcome_status, confidence_band,
                                AVG(learning_confidence) as avg_conf,
                                COUNT(*) as n
                         FROM signal_events
                         WHERE outcome_status IS NOT NULL
                         AND learning_confidence IS NOT NULL
                         GROUP BY outcome_status, confidence_band
                         LIMIT 100""")
            rows = c.fetchall()
            wins_conf  = []; losses_conf = []; band_agg: dict = {}
            for r in rows:
                status = r.get('outcome_status', ''); band = r.get('confidence_band', '')
                avg_c  = float(r.get('avg_conf') or 0); n = int(r.get('n', 0))
                if status == 'WIN':   wins_conf.append((avg_c, n))
                if status == 'LOSS':  losses_conf.append((avg_c, n))
                if band not in band_agg: band_agg[band] = {'wins':0,'losses':0,'flat':0,'total':0}
                band_agg[band][status.lower() if status in ('WIN','LOSS','FLAT') else 'flat'] += n
                band_agg[band]['total'] += n
            # Média ponderada
            def _wavg(lst):
                if not lst: return None
                total_n = sum(n for _, n in lst)
                return round(sum(c * n for c, n in lst) / total_n, 1) if total_n else None
            calib['avg_confidence_winners'] = _wavg(wins_conf)
            calib['avg_confidence_losers']  = _wavg(losses_conf)
            # Win rate por banda
            for band, agg in band_agg.items():
                t = agg['total']
                calib['confidence_band_stats'][band] = {
                    'total': t,
                    'win_rate': round(agg['wins'] / t * 100, 1) if t else None,
                    'wins': agg['wins'], 'losses': agg['losses'],
                }
            c.execute("SELECT COUNT(*) as n FROM signal_events WHERE trade_id IS NOT NULL")
            row = c.fetchone(); calib['total_attributed'] = row['n'] if row else 0
            c.close(); conn.close()
    except Exception as e:
        log.debug(f'learning_status calibration: {e}')

    return jsonify({
        'learning_version':           LEARNING_VERSION,
        'enabled':                    LEARNING_ENABLED,
        'degraded':                   LEARNING_DEGRADED,
        'learning_errors':            learning_errors,
        'total_signal_events':        signal_events_count,
        'total_patterns':             n_patterns,
        'total_factor_rows':          n_factors,
        'patterns_above_min_samples': patterns_above_min,
        'last_learning_update':       last_learning_update,
        'min_samples_threshold':      LEARNING_MIN_SAMPLES,
        'ewma_alpha':                 LEARNING_EWMA_ALPHA,
        'risk_mult_range':            [RISK_MULT_MIN, RISK_MULT_MAX],
        'shadow_eval_window_min':     SHADOW_EVAL_WINDOW_MIN,
        # [FIX-6] calibração
        'calibration':                calib,
        'timestamp':                  datetime.utcnow().isoformat(),
    })

@api_bp.route('/learning/arbi')
def arbi_learning_status_v2():
    """[v10.14] Estado do aprendizado de arbi por par."""
    with _arbi_learning_lock:
        cache = dict(_arbi_learning_cache)
    result = {}
    for pair, zones in cache.items():
        cfg = ARBI_PAIR_CONFIG.get(pair, ARBI_PAIR_CONFIG.get('_default', {}))
        result[pair] = {
            'current_min_spread': cfg.get('min_spread', ARBI_MIN_SPREAD),
            'zones': zones,
            'best_zone': max(zones.items(), key=lambda x: x[1].get('wr',0) if x[1].get('n',0)>=3 else 0)[0] if zones else None,
        }
    return jsonify({'pairs': result, 'total_pairs': len(result),
                    'last_run': _last_discovery_run})

@api_bp.route('/learning/composite')
def learning_composite():
    """[v10.13] Retorna padrões compostos descobertos automaticamente."""
    ctx = _get_ctx()
    try:
        with _pattern_discovery_lock:
            pats = list(_composite_patterns.values())
        # Ordenar: primeiro os mais confiáveis, depois os mais perigosos
        reliable = sorted([p for p in pats if p.get('reliable')],
                         key=lambda x: -x['total_samples'])
        blocked  = sorted([p for p in pats if p.get('blocked')],
                         key=lambda x: x['total_samples'], reverse=True)
        interesting = sorted([p for p in pats if not p.get('reliable') and not p.get('blocked')
                              and p['total_samples'] >= 15],
                            key=lambda x: abs(x.get('score_adj',0)), reverse=True)
        return jsonify({
            'total': len(pats),
            'reliable_count': len(reliable),
            'blocked_count': len(blocked),
            'reliable': reliable[:20],
            'blocked': blocked[:20],
            'interesting': interesting[:30],
            'last_discovery_run': _last_discovery_run,
            'dimensions_tracked': len(DISCOVERY_DIMENSIONS),
            'combo_2d': len(DISCOVERY_COMBOS_2D),
            'combo_3d': len(DISCOVERY_COMBOS_3D),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/learning/patterns')
@require_auth
def learning_patterns():
    """[L-3][FIX-7] Padrões com filtros funcionando e métricas reais."""
    min_samp   = int(request.args.get('min_samples', LEARNING_MIN_SAMPLES))
    sort_by    = request.args.get('sort_by', 'confidence_weight')
    limit      = int(request.args.get('limit', 50))
    # Filtros por símbolo/asset_type/market_type cruzam com signal_events no banco
    symbol     = request.args.get('symbol', '').upper()
    asset_type = request.args.get('asset_type', '')
    market_type= request.args.get('market_type', '')

    with learning_lock:
        rows = [dict(v) for v in pattern_stats_cache.values()
                if v.get('total_samples', 0) >= min_samp]
    for r in rows: r.pop('_ewma_hit', None)

    # Se filtros contextuais foram pedidos, cruzar com signal_events no banco
    if symbol or asset_type or market_type:
        try:
            conn = get_db()
            if conn:
                c = conn.cursor(dictionary=True)
                where = ["1=1"]
                params = []
                if symbol:      where.append("symbol=%s");      params.append(symbol)
                if asset_type:  where.append("asset_type=%s");  params.append(asset_type)
                if market_type: where.append("market_type=%s"); params.append(market_type)
                c.execute(f"SELECT DISTINCT feature_hash FROM signal_events WHERE {' AND '.join(where)}", params)
                valid_hashes = {r2['feature_hash'] for r2 in c.fetchall()}
                c.close(); conn.close()
                rows = [r for r in rows if r.get('feature_hash') in valid_hashes]
        except Exception as e:
            log.debug(f'learning_patterns filter: {e}')

    # Ordenação segura
    if sort_by not in ('confidence_weight','total_samples','expectancy','ewma_pnl_pct','wins','losses'):
        sort_by = 'confidence_weight'
    rows.sort(key=lambda x: x.get(sort_by, 0), reverse=True)

    return jsonify({
        'patterns':    rows[:limit],
        'total_count': len(rows),
        'min_samples': min_samp,
        'sort_by':     sort_by,
        'filters':     {'symbol': symbol, 'asset_type': asset_type, 'market_type': market_type},
        'timestamp':   datetime.utcnow().isoformat(),
    })

@api_bp.route('/learning/factors')
@require_auth
def learning_factors():
    """[L-4] Lista fatores com melhor e pior performance histórica."""
    ctx = _get_ctx()
    factor_type = request.args.get('factor_type', '')
    min_samp    = int(request.args.get('min_samples', 5))

    with learning_lock:
        rows = [dict(v) for k, v in factor_stats_cache.items()
                if v.get('total_samples', 0) >= min_samp
                and (not factor_type or v.get('factor_type') == factor_type)]

    for r in rows:
        r.pop('_ewma_hit', None)

    rows.sort(key=lambda x: x.get('confidence_weight', 0), reverse=True)
    top    = rows[:20]
    bottom = sorted(rows, key=lambda x: x.get('confidence_weight', 0))[:10]

    return jsonify({
        'top_factors':    top,
        'bottom_factors': bottom,
        'total_count':    len(rows),
        'timestamp':      datetime.utcnow().isoformat(),
    })

@api_bp.route('/learning/insights')
@require_auth
def learning_insights():
    """[L-6] Insights do sistema baseados no histórico."""
    factors   = get_top_factors(n_best=10, n_worst=5)
    with learning_lock:
        # Padrões com alta confiança mas poucos dados
        fragile = [dict(v) for v in pattern_stats_cache.values()
                   if v.get('confidence_weight', 0) > 0.3
                   and v.get('total_samples', 0) < LEARNING_MIN_SAMPLES * 2]
        # Padrões deteriorando: ewma_pnl recente pior que avg
        deteriorating = [dict(v) for v in pattern_stats_cache.values()
                         if v.get('ewma_pnl_pct', 0) < v.get('avg_pnl_pct', 0) - 0.5
                         and v.get('total_samples', 0) >= LEARNING_MIN_SAMPLES]
        # Top padrões
        top_patterns = sorted(pattern_stats_cache.values(),
                               key=lambda x: x.get('confidence_weight', 0), reverse=True)[:5]

    return jsonify({
        'top_positive_factors':  factors['top_positive'],
        'top_negative_factors':  factors['top_negative'],
        'fragile_patterns':      fragile[:10],
        'deteriorating_patterns':deteriorating[:10],
        'top_patterns':          [dict(p) for p in top_patterns],
        'total_signal_events':   signal_events_count,
        'learning_degraded':     LEARNING_DEGRADED,
        'timestamp':             datetime.utcnow().isoformat(),
    })

@api_bp.route('/signals/enriched')
@require_auth
def signals_enriched():
    """[L-5/L-6] Sinais enriquecidos com learning_confidence e insight."""
    ctx = _get_ctx()
    conn = get_db()
    if not conn:
        return jsonify({'error': 'DB unavailable'}), 503
    try:
        cursor = conn.cursor(dictionary=True)
        cutoff = (datetime.utcnow() - timedelta(minutes=SIGNAL_MAX_AGE_MIN)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('SELECT * FROM market_signals WHERE created_at>=%s ORDER BY score DESC LIMIT 50', (cutoff,))
        raw_signals = cursor.fetchall(); cursor.close(); conn.close()
        for r in raw_signals:
            for k, v in r.items():
                if isinstance(v, datetime): r[k] = v.isoformat()
            r['asset_type'] = 'stock'
            cached = stock_prices.get(r['symbol'])
            if cached:
                r['price']  = cached['price']
                r['rsi']    = cached.get('rsi', r.get('rsi', 50))
                r['ema9']   = cached.get('ema9', r.get('ema9', 0))
                r['ema21']  = cached.get('ema21', r.get('ema21', 0))
                r['ema50']  = cached.get('ema50', r.get('ema50', 0))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    result = []
    now_dt = datetime.utcnow()
    factors = get_top_factors(n_best=3, n_worst=2)

    for sig in raw_signals:
        try:
            sym       = sig.get('symbol', '')
            dq_score  = get_dq_score(sym)
            sig_e     = dict(sig)
            features  = extract_features(sig_e, dict(market_regime), dq_score, now_dt)
            features['_dq_score'] = dq_score
            feat_hash = make_feature_hash(features)
            conf      = calc_learning_confidence(sig_e, features, feat_hash)
            insight   = generate_insight(sig_e, features, feat_hash, conf)
            risk_mult = get_risk_multiplier(conf)

            result.append({
                'symbol':               sym,
                'signal':               sig.get('signal'),
                'raw_score':            sig.get('score'),
                'price':                sig.get('price'),
                'learning_confidence':  conf.get('final_confidence'),
                'confidence_band':      conf.get('confidence_band'),
                'pattern_samples':      conf.get('pattern_samples', 0),
                'insight_summary':      insight,
                'top_positive_factors': factors['top_positive'][:3],
                'top_negative_factors': factors['top_negative'][:2],
                'recommended_risk_multiplier': risk_mult,
                'recommended_action':  ('OPERAR' if conf.get('confidence_band') == 'HIGH' else
                                        'CAUTELA' if conf.get('confidence_band') == 'MEDIUM' else
                                        'EVITAR'),
                'feature_hash':         feat_hash,
                'confidence_breakdown': conf,
            })
        except Exception as e:
            log.debug(f'signals_enriched {sig.get("symbol")}: {e}')

    return jsonify({
        'signals':   result,
        'count':     len(result),
        'timestamp': datetime.utcnow().isoformat(),
        'cached_recent_only': True,
    })

@api_bp.route('/shadow/status')
@require_auth
def shadow_status():
    """[L-8][FIX-7] Resumo do shadow learning com métricas reais."""
    conn = get_db()
    if not conn:
        return jsonify({'error': 'DB unavailable', 'timestamp': datetime.utcnow().isoformat()})
    try:
        c = conn.cursor(dictionary=True)
        c.execute("SELECT COUNT(*) as total FROM shadow_decisions")
        total = (c.fetchone() or {}).get('total', 0)
        c.execute("SELECT COUNT(*) as pending FROM shadow_decisions WHERE evaluation_status='PENDING'")
        pending = (c.fetchone() or {}).get('pending', 0)
        c.execute("""SELECT evaluation_status, COUNT(*) as n
                     FROM shadow_decisions GROUP BY evaluation_status""")
        by_status = {r['evaluation_status']: r['n'] for r in c.fetchall()}
        c.execute("""SELECT not_executed_reason, COUNT(*) as n
                     FROM shadow_decisions GROUP BY not_executed_reason ORDER BY n DESC LIMIT 10""")
        by_reason = c.fetchall()
        # Shadow win rate (avaliadas)
        evaluated = total - pending
        wins = by_status.get('WIN', 0); losses = by_status.get('LOSS', 0)
        shadow_win_rate = round(wins / evaluated * 100, 1) if evaluated > 0 else None
        # Média de pnl_pct hipotético
        c.execute("""SELECT AVG(hypothetical_pnl_pct) as avg_pnl
                     FROM shadow_decisions WHERE evaluation_status != 'PENDING'""")
        avg_row = c.fetchone()
        avg_hyp_pnl = round(float(avg_row['avg_pnl']), 4) if avg_row and avg_row['avg_pnl'] else None
        c.close(); conn.close()
        return jsonify({
            'total_shadow_decisions': total,
            'pending_evaluation':     pending,
            'evaluated':              evaluated,
            'shadow_win_rate_pct':    shadow_win_rate,
            'avg_hypothetical_pnl_pct': avg_hyp_pnl,
            'by_status':              by_status,
            'by_reason':              by_reason,
            'eval_window_min':        SHADOW_EVAL_WINDOW_MIN,
            'learning_enabled':       LEARNING_ENABLED,
            'timestamp':              datetime.utcnow().isoformat(),
        })
    except Exception as e:
        return jsonify({'error': str(e), 'timestamp': datetime.utcnow().isoformat()})

# Adicionar rotas de learning a PUBLIC_ROUTES (somente status básico)
# PUBLIC_ROUTES is injected by init_routes() — defer until integration
try:
    PUBLIC_ROUTES.add('/learning/status')
except NameError:
    pass  # Will be added when init_routes() is called

# ═══════════════════════════════════════════════════════════════
# [SYNC] NETWORK INTELLIGENCE — TROCA ENTRE SISTEMAS EGREJA
# ═══════════════════════════════════════════════════════════════

SYNC_VERSION = "1.0"
SYNC_PEER_URL = os.environ.get('SYNC_PEER_URL', 'https://manus.up.railway.app')  # URL do sistema Manus

@api_bp.route('/sync/export')
def sync_export():
    """Exporta inteligência aprendida para troca entre sistemas Egreja. [PUBLIC — sem auth]"""
    ctx = _get_ctx()
    try:
        # 1. Padrões aprendidos (top 50 por confiança)
        with learning_lock:
            patterns_raw = dict(pattern_stats_cache)

        top_patterns = []
        for key, p in patterns_raw.items():
            if p.get('total_samples', 0) >= LEARNING_MIN_SAMPLES:
                wr = p.get('win_rate', 0)
                top_patterns.append({
                    'key':          key,
                    'win_rate':     round(wr, 1),
                    'avg_pnl':      round(p.get('avg_pnl', 0), 2),
                    'total_samples':p.get('total_samples', 0),
                    'confidence':   round(p.get('confidence', 0), 1),
                })
        top_patterns.sort(key=lambda x: x['confidence'], reverse=True)
        top_patterns = top_patterns[:50]

        # 2. Win rate por mercado
        market_stats = {}
        try:
            conn = get_db()
            if conn:
                c = conn.cursor(dictionary=True)
                c.execute("""
                    SELECT market,
                           COUNT(*) as total,
                           SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                           AVG(pnl_pct) as avg_pnl_pct,
                           SUM(pnl) as total_pnl
                    FROM trades
                    WHERE status='CLOSED' AND closed_at >= NOW() - INTERVAL 30 DAY
                    GROUP BY market
                """)
                for r in c.fetchall():
                    mkt = r.get('market', 'UNKNOWN')
                    t = int(r.get('total', 0))
                    market_stats[mkt] = {
                        'total_trades': t,
                        'win_rate':     round(int(r.get('wins', 0)) / t * 100, 1) if t else 0,
                        'avg_pnl_pct':  round(float(r.get('avg_pnl_pct') or 0), 2),
                        'total_pnl':    round(float(r.get('total_pnl') or 0), 2),
                    }
                c.close(); conn.close()
        except Exception as e:
            log.debug(f'sync_export market_stats: {e}')

        # 3. Top sinais ativos agora (score alto)
        hot_signals = []
        try:
            conn = get_db()
            if conn:
                c = conn.cursor(dictionary=True)
                c.execute("""
                    SELECT symbol, market, action, score, learning_confidence, created_at
                    FROM signal_events
                    WHERE created_at >= NOW() - INTERVAL 2 HOUR
                    AND learning_confidence >= 60
                    ORDER BY learning_confidence DESC
                    LIMIT 20
                """)
                for r in c.fetchall():
                    hot_signals.append({
                        'symbol':     r.get('symbol'),
                        'market':     r.get('market'),
                        'action':     r.get('action'),
                        'score':      float(r.get('score') or 0),
                        'confidence': float(r.get('learning_confidence') or 0),
                        'age_min':    round((datetime.utcnow() - r['created_at']).total_seconds() / 60, 1) if r.get('created_at') else None,
                    })
                c.close(); conn.close()
        except Exception as e:
            log.debug(f'sync_export hot_signals: {e}')

        # 4. Score de aprendizado global
        total_patterns = len(top_patterns)
        avg_conf = round(sum(p['confidence'] for p in top_patterns) / total_patterns, 1) if total_patterns else 0
        best_market = max(market_stats, key=lambda m: market_stats[m]['win_rate'], default=None)

        return jsonify({
            'system':         'egreja-railway',
            'sync_version':   SYNC_VERSION,
            'exported_at':    datetime.utcnow().isoformat() + 'Z',
            'learning': {
                'total_patterns':   total_patterns,
                'avg_confidence':   avg_conf,
                'learning_enabled': LEARNING_ENABLED,
            },
            'top_patterns':   top_patterns,
            'market_stats':   market_stats,
            'hot_signals':    hot_signals,
            'best_market':    best_market,
            'portfolio': {
                'initial_capital': INITIAL_CAPITAL_STOCKS + INITIAL_CAPITAL_CRYPTO,
                'stocks_capital':  INITIAL_CAPITAL_STOCKS,
                'crypto_capital':  INITIAL_CAPITAL_CRYPTO,
                'arbi_capital':    ARBI_CAPITAL,
            }
        })
    except Exception as e:
        log.error(f'sync_export error: {e}')
        return jsonify({'error': str(e)}), 500


@api_bp.route('/sync/import', methods=['POST'])
def sync_import():
    """Recebe inteligência de outro sistema Egreja. [PUBLIC — sem auth]"""
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({'error': 'No data'}), 400

        source_system = data.get('system', 'unknown')
        exported_at   = data.get('exported_at', '')
        hot_signals   = data.get('hot_signals', [])
        market_stats  = data.get('market_stats', {})
        top_patterns  = data.get('top_patterns', [])

        # Salva snapshot na DB para auditoria e uso pelo sistema
        conn = get_db()
        if conn:
            c = conn.cursor()
            c.execute("""
                ctx = _get_ctx()
                CREATE TABLE IF NOT EXISTS sync_snapshots (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    source_system VARCHAR(100),
                    exported_at DATETIME,
                    received_at DATETIME DEFAULT NOW(),
                    hot_signals_count INT,
                    top_patterns_count INT,
                    payload JSON
                )
            """)
            import json as _json
            c.execute("""
                INSERT INTO sync_snapshots
                    (source_system, exported_at, hot_signals_count, top_patterns_count, payload)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                source_system,
                exported_at[:19].replace('T', ' ') if exported_at else None,
                len(hot_signals),
                len(top_patterns),
                _json.dumps(data)[:65000],
            ))
            conn.commit(); c.close(); conn.close()

        log.info(f'[SYNC] Received from {source_system}: {len(hot_signals)} signals, {len(top_patterns)} patterns')

        return jsonify({
            'status':   'ok',
            'received': {
                'source':           source_system,
                'hot_signals':      len(hot_signals),
                'patterns':         len(top_patterns),
                'market_snapshots': len(market_stats),
            },
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        })
    except Exception as e:
        log.error(f'sync_import error: {e}')
        return jsonify({'error': str(e)}), 500


@api_bp.route('/sync/peer-data')
@require_auth
def sync_peer_data():
    """Busca dados do sistema parceiro (egreja.com) e retorna inteligência cruzada."""
    try:
        import requests as _req

        peer_url = SYNC_PEER_URL.rstrip('/')
        peer_data = None
        peer_error = None

        # Tenta buscar export do peer
        try:
            r = _req.get(
                f'{peer_url}/sync/export',
                headers={'X-API-Key': API_SECRET_KEY},
                timeout=8
            )
            if r.status_code == 200:
                peer_data = r.json()
        except Exception as e:
            peer_error = str(e)

        # Export local para comparação
        local_export = None
        try:
            with learning_lock:
                n_patterns = len(pattern_stats_cache)
            local_export = {
                'system': 'egreja-railway',
                'total_patterns': n_patterns,
            }
        except Exception:
            pass

        return jsonify({
            'local':      local_export,
            'peer':       peer_data,
            'peer_url':   peer_url,
            'peer_error': peer_error,
            'synced_at':  datetime.utcnow().isoformat() + 'Z',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/sync/status')
@require_auth
def sync_status():
    """Verifica se o peer está online e retorna resumo de conectividade."""
    ctx = _get_ctx()
    try:
        import requests as _req
        peer_url = SYNC_PEER_URL.rstrip('/')
        online = False; latency_ms = None; peer_info = {}

        try:
            import time
            t0 = time.time()
            r = _req.get(f'{peer_url}/health', timeout=5)
            latency_ms = round((time.time() - t0) * 1000)
            online = r.status_code == 200
            if online:
                try: peer_info = r.json()
                except: pass
        except Exception as e:
            peer_info = {'error': str(e)}

        # Último sync recebido
        last_sync = None
        try:
            conn = get_db()
            if conn:
                c = conn.cursor(dictionary=True)
                c.execute("SELECT source_system, received_at, hot_signals_count FROM sync_snapshots ORDER BY received_at DESC LIMIT 1")
                row = c.fetchone()
                if row:
                    last_sync = {
                        'source':     row['source_system'],
                        'received_at': row['received_at'].isoformat() if row['received_at'] else None,
                        'signals':    row['hot_signals_count'],
                    }
                c.close(); conn.close()
        except Exception:
            pass

        return jsonify({
            'peer_url':   peer_url,
            'peer_online':online,
            'latency_ms': latency_ms,
            'peer_info':  peer_info,
            'last_sync':  last_sync,
            'timestamp':  datetime.utcnow().isoformat() + 'Z',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════
# RELATÓRIOS — /reports/daily  /reports/weekly  /reports/monthly
# ═══════════════════════════════════════════════════════════════

def _build_report(period_days, label):
    """Gera dict estruturado do relatório para um período."""
    cutoff = (datetime.utcnow() - timedelta(days=period_days)).isoformat()
    with state_lock:
        s_cl = list(stocks_closed); c_cl = list(crypto_closed); a_cl = list(arbi_closed)
        s_op = list(stocks_open);   c_op = list(crypto_open);   a_op = list(arbi_open)
        sc = stocks_capital; cc = crypto_capital; ac = arbi_capital

    def period_trades(lst): return [t for t in lst if t.get('closed_at','') >= cutoff]
    def _stats(lst):
        wins   = [t for t in lst if t.get('pnl',0) > 0]
        losses = [t for t in lst if t.get('pnl',0) < 0]
        total_pnl = sum(t.get('pnl',0) for t in lst)
        by_sym = {}
        for t in lst:
            sym = t.get('symbol') or t.get('name','?')
            by_sym.setdefault(sym, {'pnl':0,'count':0})
            by_sym[sym]['pnl'] += t.get('pnl',0); by_sym[sym]['count'] += 1
        top5 = sorted(by_sym.items(), key=lambda x: x[1]['pnl'], reverse=True)[:5]
        bot5 = sorted(by_sym.items(), key=lambda x: x[1]['pnl'])[:5]
        return {
            'count': len(lst), 'wins': len(wins), 'losses': len(losses),
            'win_rate': round(len(wins)/len(lst)*100,1) if lst else 0,
            'total_pnl': round(total_pnl, 2),
            'avg_pnl': round(total_pnl/len(lst),2) if lst else 0,
            'best_trade': round(max((t.get('pnl',0) for t in lst), default=0),2),
            'worst_trade': round(min((t.get('pnl',0) for t in lst), default=0),2),
            'top5_symbols': [{'symbol':k,'pnl':round(v['pnl'],2),'count':v['count']} for k,v in top5 if v['pnl']>0],
            'bot5_symbols': [{'symbol':k,'pnl':round(v['pnl'],2),'count':v['count']} for k,v in bot5 if v['pnl']<0],
        }

    s_trades = period_trades(s_cl); c_trades = period_trades(c_cl); a_trades = period_trades(a_cl)
    all_trades = s_trades + c_trades
    total_pnl_sc   = sum(t.get('pnl',0) for t in all_trades)
    total_pnl_arbi = sum(t.get('pnl',0) for t in a_trades)
    initial_sc     = INITIAL_CAPITAL_STOCKS + INITIAL_CAPITAL_CRYPTO
    open_pnl_sc    = sum(t.get('pnl',0) for t in s_op+c_op)
    open_pnl_arbi  = sum(t.get('pnl',0) for t in a_op)

    return {
        'period': label, 'period_days': period_days,
        'generated_at': datetime.utcnow().isoformat(),
        'portfolio': {
            'initial_capital': initial_sc,
            'closed_pnl': round(total_pnl_sc, 2),
            'open_pnl': round(open_pnl_sc, 2),
            'total_pnl': round(total_pnl_sc + open_pnl_sc, 2),
            'return_pct': round((total_pnl_sc + open_pnl_sc) / initial_sc * 100, 3) if initial_sc else 0,
        },
        'arbi': {
            'initial_capital': ARBI_CAPITAL,
            'closed_pnl': round(total_pnl_arbi, 2),
            'open_pnl': round(open_pnl_arbi, 2),
            'total_pnl': round(total_pnl_arbi + open_pnl_arbi, 2),
            'return_pct': round((total_pnl_arbi + open_pnl_arbi) / ARBI_CAPITAL * 100, 3) if ARBI_CAPITAL else 0,
        },
        'stocks': _stats(s_trades),
        'crypto': _stats(c_trades),
        'arbi_detail': _stats(a_trades),
        'combined': {
            'count': len(s_trades)+len(c_trades)+len(a_trades),
            'total_pnl': round(total_pnl_sc + total_pnl_arbi, 2),
            'win_rate': round(sum(1 for t in all_trades+a_trades if t.get('pnl',0)>0) /
                              max(len(all_trades+a_trades),1) * 100, 1),
        },
        'open_positions': {
            'stocks': len(s_op), 'crypto': len(c_op), 'arbi': len(a_op),
            'open_pnl_total': round(open_pnl_sc + open_pnl_arbi, 2),
        },
    }

def _whatsapp_report(rpt):
    """Formata e envia relatorio por WhatsApp."""
    p  = rpt['period']
    pf = rpt['portfolio']; ar = rpt['arbi']
    sc_r = rpt['stocks']; cr_r = rpt['crypto']; ab = rpt['arbi_detail']
    cmb  = rpt['combined']
    g = lambda v: 'GANHO' if v>=0 else 'PERDA'
    msg = (
        f"EGREJA AI - Relatorio {p}\n"
        f"\nSTOCKS + CRYPTO\n"
        f"PnL fechado: {g(pf['closed_pnl'])} ${pf['closed_pnl']:+,.0f}\n"
        f"PnL aberto:  {g(pf['open_pnl'])} ${pf['open_pnl']:+,.0f}\n"
        f"Retorno: {pf['return_pct']:+.2f}%\n"
        f"  Stocks ({sc_r['count']} trades, WR {sc_r['win_rate']:.0f}%): ${sc_r['total_pnl']:+,.0f}\n"
        f"  Crypto ({cr_r['count']} trades, WR {cr_r['win_rate']:.0f}%): ${cr_r['total_pnl']:+,.0f}\n"
        f"\nARBI (pool separado)\n"
        f"PnL: {g(ar['closed_pnl'])} ${ar['closed_pnl']:+,.0f} ({ar['return_pct']:+.2f}%)\n"
        f"Trades: {ab['count']} | WR: {ab['win_rate']:.0f}%\n"
        f"\nTOTAL GERAL\n"
        f"PnL combinado: {g(cmb['total_pnl'])} ${cmb['total_pnl']:+,.0f}\n"
        f"Win Rate global: {cmb['win_rate']:.0f}% ({cmb['count']} trades)"
    )
    if sc_r.get('top5_symbols'):
        tops = ' | '.join(f"{x['symbol']} +${x['pnl']:,.0f}" for x in sc_r['top5_symbols'][:3])
        msg += f"\nTop Stocks: {tops}"
    if cr_r.get('top5_symbols'):
        tops = ' | '.join(f"{x['symbol']} +${x['pnl']:,.0f}" for x in cr_r['top5_symbols'][:3])
        msg += f"\nTop Crypto: {tops}"
    send_whatsapp(msg)

@api_bp.route('/risk/block_symbol', methods=['POST'])
@require_auth
def block_symbol_route():
    """Bloqueia ou desbloqueia um símbolo manualmente.
    POST body: {"symbol": "RAIZ4", "action": "block"|"unblock"}
    """
    ctx = _get_ctx()
    data = request.get_json() or {}
    sym    = data.get('symbol','').upper().strip()
    action = data.get('action','block')
    if not sym: return jsonify({'error': 'symbol obrigatório'}), 400
    if action == 'block':
        symbol_blocked.add(sym)
        symbol_cooldown[sym] = time.time() + 86400 - SYMBOL_COOLDOWN_SEC
        audit('SYMBOL_BLOCKED', {'symbol': sym})
        log.warning(f'SYMBOL_BLOCKED manual: {sym}')
        # [v10.9] Persistir no banco para sobreviver restarts
        try:
            _conn = get_db()
            if _conn:
                _cur = _conn.cursor()
                _cur.execute(
                    "INSERT INTO symbol_blocked_persistent (symbol, reason) VALUES (%s,%s) "
                    "ON DUPLICATE KEY UPDATE blocked_at=NOW(), reason=VALUES(reason)",
                    (sym, 'manual_block')
                )
                _conn.commit(); _cur.close(); _conn.close()
        except Exception as _e: log.error(f'block_symbol persist: {_e}')
        return jsonify({'ok': True, 'symbol': sym, 'status': 'blocked', 'persisted': True})
    else:
        symbol_blocked.discard(sym)
        symbol_cooldown.pop(sym, None)
        audit('SYMBOL_UNBLOCKED', {'symbol': sym})
        log.info(f'SYMBOL_UNBLOCKED: {sym}')
        try:
            _conn = get_db()
            if _conn:
                _cur = _conn.cursor()
                _cur.execute("DELETE FROM symbol_blocked_persistent WHERE symbol=%s", (sym,))
                _conn.commit(); _cur.close(); _conn.close()
        except Exception as _e: log.error(f'unblock persist: {_e}')
        return jsonify({'ok': True, 'symbol': sym, 'status': 'unblocked'})

@api_bp.route('/risk/blocked_symbols')
@require_auth
def blocked_symbols_route():
    """Lista símbolos bloqueados e cooldowns ativos.""";
    now = time.time()
    blocked = [{'symbol': s, 'reason': 'manual_block'} for s in sorted(symbol_blocked)]
    in_cooldown = [
        {'symbol': s, 'remaining_s': int(ts - now), 'sl_count': symbol_sl_count.get(s,0)}
        for s, ts in symbol_cooldown.items()
        if ts > now and s not in symbol_blocked
    ]
    in_cooldown.sort(key=lambda x: -x['remaining_s'])
    return jsonify({'blocked': blocked, 'in_cooldown': in_cooldown[:20]})


@api_bp.route('/trades/purge', methods=['POST'])
@require_auth
def purge_trades():
    """[v10.9] Deletar trades permanentemente do banco e da memória.
    Remove também trades VOID do mesmo símbolo.
    Body: {"symbol": "RAIZ4"} para deletar todas de um símbolo
          {"trade_ids": ["STK-xxx",...]} para deletar IDs específicos
    ATENÇÃO: operação irreversível.
    """
    ctx = _get_ctx()
    data = request.get_json() or {}
    sym    = data.get('symbol','').upper().strip()
    ids_in = data.get('trade_ids', [])
    confirm = data.get('confirm','')

    if confirm != 'PURGE':
        return jsonify({'error': 'Adicione "confirm":"PURGE" para confirmar'}), 400

    conn = get_db()
    if not conn: return jsonify({'error': 'DB unavailable'}), 503

    deleted_ids = []
    pnl_removed = 0.0
    try:
        cur = conn.cursor(dictionary=True)

        if sym:
            # Deletar todas as trades do símbolo (CLOSED e VOID)
            cur.execute('SELECT id, pnl, asset_type, status FROM trades WHERE symbol=%s', (sym,))
            rows = cur.fetchall()
            for r in rows:
                cur.execute('DELETE FROM trades WHERE id=%s', (r['id'],))
                if r.get('status') == 'CLOSED':
                    pnl_removed += float(r.get('pnl') or 0)
                deleted_ids.append(r['id'])
            log.warning(f'PURGE: {len(deleted_ids)} trades de {sym} deletadas do banco')
        elif ids_in:
            for tid in ids_in:
                cur.execute('SELECT id, pnl, asset_type, status FROM trades WHERE id=%s', (tid,))
                r = cur.fetchone()
                if r:
                    cur.execute('DELETE FROM trades WHERE id=%s', (tid,))
                    if r.get('status') == 'CLOSED':
                        pnl_removed += float(r.get('pnl') or 0)
                    deleted_ids.append(tid)
        else:
            cur.close(); conn.close()
            return jsonify({'error': 'Informe symbol ou trade_ids'}), 400

        conn.commit(); cur.close(); conn.close()

        # Remover da memória + reverter capital
        with state_lock:
            global stocks_capital, crypto_capital
            before_s = len(stocks_closed); before_c = len(crypto_closed)
            if sym:
                stocks_closed[:] = [t for t in stocks_closed if t.get('symbol') != sym]
                crypto_closed[:] = [t for t in crypto_closed if t.get('symbol') != sym]
                stocks_open[:]   = [t for t in stocks_open   if t.get('symbol') != sym]
                crypto_open[:]   = [t for t in crypto_open   if t.get('symbol') != sym]
            else:
                stocks_closed[:] = [t for t in stocks_closed if t.get('id') not in deleted_ids]
                crypto_closed[:] = [t for t in crypto_closed if t.get('id') not in deleted_ids]
            # Reverter PnL: as trades removidas tinham pnl já somado ao capital
            # Desfazer: capital -= pnl (pnl negativo → capital sobe)
            stocks_capital -= pnl_removed  # pnl_removed negativo → capital aumenta
            removed_s = before_s - len(stocks_closed)
            removed_c = before_c - len(crypto_closed)

        audit('TRADES_PURGED', {'symbol': sym or 'multiple', 'count': len(deleted_ids),
                                  'pnl_removed': round(pnl_removed,2)})
        log.warning(f'PURGE completo: {len(deleted_ids)} trades | pnl_removed={pnl_removed:+,.2f}')
        return jsonify({
            'ok': True, 'deleted': len(deleted_ids), 'ids': deleted_ids,
            'pnl_removed': round(pnl_removed, 2),
            'note': 'Operação irreversível — trades removidas permanentemente do banco'
        })
    except Exception as e:
        conn.rollback(); conn.close()
        return jsonify({'error': str(e)}), 500

@api_bp.route('/reports/daily')
@require_auth
def report_daily():
    ctx = _get_ctx()
    return jsonify(_build_report(1, 'DIARIO'))

@api_bp.route('/reports/weekly')
@require_auth
def report_weekly():
    ctx = _get_ctx()
    return jsonify(_build_report(7, 'SEMANAL'))

@api_bp.route('/reports/monthly')
@require_auth
def report_monthly():
    ctx = _get_ctx()
    return jsonify(_build_report(30, 'MENSAL'))

@api_bp.route('/reports/send/<period>', methods=['POST'])
@require_auth
def report_send(period):
    """Envia relatorio por WhatsApp: POST /reports/send/daily|weekly|monthly"""
    days = {'daily':1,'weekly':7,'monthly':30}.get(period)
    if not days: return jsonify({'error':'period deve ser daily, weekly ou monthly'}), 400
    label = {'daily':'DIARIO','weekly':'SEMANAL','monthly':'MENSAL'}[period]
    rpt = _build_report(days, label)
    _whatsapp_report(rpt)
    return jsonify({'ok': True, 'period': period, 'report': rpt})

def _report_scheduler():
    """Scheduler automatico: diario 20h BRT, semanal sextas, mensal ultimo dia."""
    ctx = _get_ctx()
    import pytz, calendar as _cal
    brt = pytz.timezone('America/Sao_Paulo')
    sent = set()
    while True:
        try:
            beat('report_scheduler')
            now_brt = datetime.now(brt)
            key_d  = now_brt.strftime('%Y-%m-%d')
            hour   = now_brt.hour; wd = now_brt.weekday()
            last_d = _cal.monthrange(now_brt.year, now_brt.month)[1]
            if hour == 20:
                if key_d not in sent:
                    sent.add(key_d)
                    try: _whatsapp_report(_build_report(1,'DIARIO')); log.info('REPORT: diario enviado')
                    except Exception as e: log.error(f'REPORT diario: {e}')
                if wd == 4 and (key_d+'-W') not in sent:
                    sent.add(key_d+'-W')
                    try: _whatsapp_report(_build_report(7,'SEMANAL')); log.info('REPORT: semanal enviado')
                    except Exception as e: log.error(f'REPORT semanal: {e}')
                if now_brt.day == last_d and (key_d+'-M') not in sent:
                    sent.add(key_d+'-M')
                    try: _whatsapp_report(_build_report(30,'MENSAL')); log.info('REPORT: mensal enviado')
                    except Exception as e: log.error(f'REPORT mensal: {e}')
            if len(sent) > 200: sent.clear()
        except Exception as e: log.error(f'_report_scheduler: {e}')
        time.sleep(60)


# ═══════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════
# [v10.14] BROKERAGE FEE SIMULATION — taxas reais por mercado
# Deduzidas automaticamente no fechamento de cada trade
# P&L BRUTO inalterado (usado para lógica de trading)
# P&L LÍQUIDO = gross - fee (usado para reporting e aprendizado)
# ═══════════════════════════════════════════════════════════════════

# [v10.28] calc_fee and apply_fee_to_trade are injected by init_routes() from api_server.py
# They will be available as module-level names after init_routes() is called.


@api_bp.route('/admin/db-cleanup', methods=['POST'])
def admin_db_cleanup():
    """[v10.14] Limpa tabelas cheias, adiciona colunas e libera espaço."""
    if request.headers.get('X-API-Key') != API_SECRET_KEY:
        return jsonify({'error': 'unauthorized'}), 401
    conn = get_db()
    if not conn: return jsonify({'error': 'db unavailable'}), 500
    results = {}
    try:
        c = conn.cursor()
        # TRUNCATE nas tabelas auxiliares — dados não são críticos
        for tbl in ['signal_events', 'shadow_decisions', 'learning_audit', 'audit_events']:
            try:
                c.execute(f'SELECT COUNT(*) FROM {tbl}')
                before = c.fetchone()[0]
                c.execute(f'TRUNCATE TABLE {tbl}')
                conn.commit()
                results[tbl] = f'truncated {before} rows'
            except Exception as e:
                results[tbl] = f'error: {e}'
        # orders — manter últimos 500
        try:
            c.execute("DELETE FROM orders WHERE id NOT IN (SELECT id FROM (SELECT id FROM orders ORDER BY created_at DESC LIMIT 500) t)")
            conn.commit()
            results['orders'] = f'kept last 500 (deleted {c.rowcount})'
        except Exception as e:
            results['orders'] = f'error: {e}'
        # OPTIMIZE para liberar espaço físico
        for tbl in ['signal_events', 'shadow_decisions', 'learning_audit']:
            try: c.execute(f'OPTIMIZE TABLE {tbl}'); conn.commit()
            except: pass
        # Adicionar colunas de fee (idempotente — ignora se já existem)
        for sql in [
            "ALTER TABLE trades ADD COLUMN fee_estimated DECIMAL(10,2) NULL DEFAULT 0",
            "ALTER TABLE trades ADD COLUMN pnl_net DECIMAL(10,2) NULL",
            "ALTER TABLE trades ADD COLUMN pnl_gross DECIMAL(10,2) NULL",
        ]:
            try: c.execute(sql); conn.commit(); results.setdefault('cols','') ; results['cols'] += ' OK'
            except: pass  # coluna já existe
        # Tamanho atual
        c.execute("""SELECT table_name, table_rows,
            ROUND((data_length+index_length)/1024/1024,1) as mb
            FROM information_schema.tables WHERE table_schema=DATABASE()
            ORDER BY (data_length+index_length) DESC LIMIT 12""")
        results['table_sizes'] = [{'t': r[0], 'rows': r[1], 'mb': float(r[2] or 0)} for r in c.fetchall()]
        total_mb = sum(x['mb'] for x in results['table_sizes'])
        results['total_mb'] = total_mb
        c.close(); conn.close()
        return jsonify({'ok': True, 'results': results})
    except Exception as e:
        return jsonify({'error': str(e)}), 500



def create_api_blueprint(ctx_provider_fn):
    """Create and configure the API blueprint with the given context provider.
    
    Args:
        ctx_provider_fn: A callable that returns a dict with all required global state
        
    Returns:
        The configured api_bp Blueprint ready to be registered with the Flask app
    """
    init_routes(ctx_provider_fn)
    return api_bp
