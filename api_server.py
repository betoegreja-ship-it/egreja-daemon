#!/usr/bin/env python3
"""
Egreja Investment AI — API Server v10.22.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
v10.6.4 → v10.7.0: Polling inteligente + 6 cirurgias (dedup stocks+crypto, dead code, klines unificado, signal_id real)

FONTES DE DADOS (em ordem de prioridade)
  Stocks US / NYSE:  Polygon.io REST (candles + snapshot) → FMP → Yahoo
  Stocks B3:         brapi.dev (especializado) → Polygon → FMP → Yahoo
  Crypto:            Binance REST público (allTickers bulk) → FMP → Yahoo
  FX (USDBRL etc.):  frankfurter.app (ECB, free) → Yahoo
  Arbi legs:         camada unificada _fetch_arbi_price() (Binance/Polygon/brapi/FMP/Yahoo)

  Env vars novas:
    POLYGON_API_KEY  — obrigatório para stocks US com qualidade máxima
    BRAPI_TOKEN      — recomendado para B3 (sem token: modo free com rate limit)
  Env vars mantidas:
    FMP_API_KEY      — fallback secundário (ainda útil)

FEATURES ENRIQUECIDAS  [v10.4]
  atr_bucket    — ATR como % do preço (VERY_LOW/LOW/NORMAL/HIGH/EXTREME)
                  calculado com high/low reais quando disponíveis (Polygon, brapi, Binance klines)
                  distingue ativo em compressão de ativo em expansão de volatilidade

  volume_bucket — ratio volume_hoje / média_20d (VERY_LOW/LOW/NORMAL/HIGH/SURGE)
                  confirma ou invalida o movimento; volume fraco = sinal suspeito
                  disponível via Polygon, brapi e Binance klines

  weekday       — agora faz parte do make_feature_hash()
                  segunda-feira (gap open) e sexta-feira (liquidez reduzida) têm
                  padrões distintos que o learning vai capturar naturalmente

SCORE COMPOSTO CRYPTO  [v10.4]
  Substitui: score = 50 + int(abs(change_24h) * 5)   ← ignorava volume e ATR
  Novo: _crypto_composite_score() — 4 fatores ponderados:
    40% change_pct_24h  — força direcional (capped ±15%)
    30% volume_ratio    — volume USDT hoje vs média 20d (confirma movimento)
    20% range_position  — posição do preço no high/low do dia (direcionalidade intraday)
    10% liquidez        — n_trades normalizado (evita altcoins ilíquidas)
  Klines Binance são cacheadas por 1 hora por símbolo (sem impacto em rate limit)

DEDUPLICAÇÃO CRYPTO  [v10.4, melhoria sobre v10.3.4]
  Substituiu: ms_key = f"CRY:{sym}:{direction}:{score}:{int(price)}"
              ↑ instável em altcoins com preço < 1 USDT
  Novo:       ms_key = f"CRY:{sym}:{direction}:{int(time.time()/90)}"
              chave muda a cada janela de 90s — exatamente o ciclo do loop
              sem falsos positivos por variação de centavo em DOGE/XRP etc.

Herdado e preservado da v10.3.4 (F1..F5) e ancestrais.
"""







import decimal   # [v10.7] movido do interior de funções para o nível de módulo
import re        # [adaptive-v1] pattern matching B3 ticker classification
import os, sys, time, queue, json, uuid, threading, itertools, requests, logging, hashlib, math
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import mysql.connector

# [v10.22] Garantir que o diretório do script está no sys.path (para imports de modules/)
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

# ── [v10.23] Institutional modules ─────────────────────────────────────
try:
    from modules.risk_manager import InstitutionalRiskManager
    from modules.broker_base import PaperBroker, BTGBroker, BinanceBroker, NYSEBroker, OrderTracker, BrokerFactory, OrderStatus, AssetClass, create_order_record
    from modules.data_validator import MarketDataValidator
    from modules.auth_rbac import AuthManager, AuditLogger, Role
    from modules.stats_engine import PerformanceStats
    from modules.kill_switch import ExternalKillSwitch, KillSwitchMiddleware, ResumeMode
    from modules.ops_metrics import OpsMetricsCollector, AlertLevel
    # [v10.25] Derivatives module imports
    from modules.derivatives.config import get_config as get_deriv_config, DerivativesConfig, ActiveStatus
    from modules.derivatives.schema import create_derivatives_tables
    from modules.derivatives.providers import ProviderManager, SimulatedMarketDataProvider, CedroMarketDataProvider, OpLabMarketDataProvider
    from modules.derivatives.services import (OptionsChainCache, FuturesChainCache, DividendEventService,
        RatesCurveService, GreeksCalculator, CalibrationService, StrategyScorecard, StructuredOrderExecutor,
        NAVCalculatorService, ImpliedVolEngine)
    from modules.derivatives.liquidity import LiquidityScoreEngine, PromotionEngine, ActiveStatusRegistry
    from modules.derivatives.strategies import (pcp_scan_loop, fst_scan_loop, roll_arb_scan_loop,
        etf_basket_scan_loop, skew_arb_scan_loop, interlisted_scan_loop, dividend_arb_scan_loop, vol_arb_scan_loop,
        ibov_basis_scan_loop, di_calendar_scan_loop, interlisted_hedged_scan_loop)
    from modules.derivatives.endpoints import create_strategies_blueprint
    # [v10.35] Execution / capital / monitoring / sizing / learning — paper trading end-to-end
    from modules.derivatives.capital import DerivativesCapitalManager
    from modules.derivatives.deriv_execution import (DerivativesExecutionEngine, DerivativesTrade,
        TradeLeg, TradeStatus, LegStatus)
    from modules.derivatives.monitoring import DerivativesMonitor
    from modules.derivatives.position_sizing import DerivativesPositionSizer
    from modules.derivatives.learning import DerivativesLearningEngine
    # [v10.36] Top Opportunities daily audit (EOD 17:30 BRT snapshot + endpoint)
    from modules.derivatives.top_opps_audit import (
        create_top_opps_audit_table,
        snapshot_top_opportunities,
        query_top_opps_audit,
    )
    _MODULES_LOADED = True
except Exception as _mod_err:
    import traceback as _tb
    print(f'[v10.22] WARNING: Failed to load institutional modules: {_mod_err}', flush=True)
    _tb.print_exc()
    _MODULES_LOADED = False
    # Stubs so the rest of the code doesn't crash
    class _Stub:
        def __getattr__(self, name): return lambda *a, **kw: None
    InstitutionalRiskManager = type('InstitutionalRiskManager', (), {'__init__': lambda s: None, 'record_trade_result': lambda *a,**k: None, 'check_can_open': lambda *a,**k: (True,''), 'get_risk_multiplier': lambda s: 1.0, 'is_breached': lambda s: (False,[]), 'get_status': lambda s: {}})
    OrderTracker = type('OrderTracker', (), {'__init__': lambda s: None, 'get_reconciliation_status': lambda s: {}, 'get_slippage_stats': lambda s: {}})
    MarketDataValidator = type('MarketDataValidator', (), {'__init__': lambda s: None, 'record_price': lambda *a,**k: None, 'validate_price': lambda *a,**k: None, 'get_data_quality_status': lambda s: {}})
    AuthManager = type('AuthManager', (), {'__init__': lambda s: None, 'init_users_table': lambda *a,**k: None, 'auth_mode': 'api_key', 'admin_email': '', 'list_users': lambda *a,**k: []})
    AuditLogger = type('AuditLogger', (), {'__init__': lambda s: None, 'log_action': lambda *a,**k: None, 'get_recent': lambda *a,**k: []})
    class Role: VIEWER='viewer'; OPERATOR='operator'; ADMIN='admin'
    PerformanceStats = type('PerformanceStats', (), {'__init__': lambda s: None, 'record_trade': lambda *a,**k: None, 'get_full_report': lambda s: {}, 'get_promotion_criteria': lambda s: {}})
    ExternalKillSwitch = type('ExternalKillSwitch', (), {'__init__': lambda s: None, 'init_table': lambda *a,**k: None, 'check_all': lambda *a,**k: {}, 'auto_activate_on_risk_breach': lambda *a,**k: None})
    class KillSwitchMiddleware:
        def __init__(self, ks=None): pass
        def check_before_trade(self, *a, **k): return (True, '')
    class ResumeMode:
        PAPER='paper'; SHADOW='shadow'; LIVE='live'
    OpsMetricsCollector = type('OpsMetricsCollector', (), {'__init__': lambda s: None, 'record_memory': lambda s: {}, 'record_drift': lambda *a,**k: None, 'record_worker_cycle': lambda *a,**k: None, 'record_endpoint_latency': lambda *a,**k: None, 'record_circuit_breaker_event': lambda *a,**k: None, 'get_status': lambda s: {}, 'generate_daily_audit': lambda s: {}, 'get_drift_report': lambda s: {}, 'get_active_alerts': lambda s: {}})
    class AlertLevel:
        OK='OK'; WARNING='WARNING'; CRITICAL='CRITICAL'; FREEZE='FREEZE'
    # [v10.25] Derivatives stubs (fallback if modules not installed)
    class DerivativesConfig:
        STRATEGIES = []
    def get_deriv_config(): return DerivativesConfig()
    class ActiveStatus:
        OBSERVE='OBSERVE'; SHADOW_EXEC='SHADOW_EXEC'; PAPER_SMALL='PAPER_SMALL'; PAPER_FULL='PAPER_FULL'; DISABLED='DISABLED'
    def create_derivatives_tables(c): pass
    def create_top_opps_audit_table(c): pass
    def snapshot_top_opportunities(*a, **kw): return {'captured': 0, 'inserted': 0, 'errors': ['module_not_loaded']}
    def query_top_opps_audit(*a, **kw): return {'rows': [], 'error': 'module_not_loaded'}
    class SimulatedMarketDataProvider: pass
    class ProviderManager:
        def __init__(self): pass
        def get_provider(self): return None
    class LiquidityScoreEngine: pass
    class PromotionEngine: pass
    class ActiveStatusRegistry: pass
    OptionsChainCache = FuturesChainCache = DividendEventService = RatesCurveService = None
    GreeksCalculator = CalibrationService = StrategyScorecard = StructuredOrderExecutor = None
    NAVCalculatorService = ImpliedVolEngine = None
    def create_strategies_blueprint(**kw):
        from flask import Blueprint; return Blueprint('strategies', __name__)
    pcp_scan_loop = fst_scan_loop = roll_arb_scan_loop = etf_basket_scan_loop = None
    skew_arb_scan_loop = interlisted_scan_loop = dividend_arb_scan_loop = vol_arb_scan_loop = None
    ibov_basis_scan_loop = di_calendar_scan_loop = interlisted_hedged_scan_loop = None
    # [v10.35] Stubs for paper trading engine (fallback)
    DerivativesCapitalManager = None
    DerivativesExecutionEngine = None
    DerivativesMonitor = None
    DerivativesPositionSizer = None
    DerivativesLearningEngine = None
    DerivativesTrade = TradeLeg = None
    class TradeStatus:
        OPEN='OPEN'; CLOSED='CLOSED'; FAILED='FAILED'
    class LegStatus:
        FILLED='FILLED'; PENDING='PENDING'

# ── [v10.28] Pure business logic modules ───────────────────────────────
try:
    from modules.trading_config import (
        INITIAL_CAPITAL_STOCKS, INITIAL_CAPITAL_CRYPTO,
        MAX_POSITION_STOCKS, MAX_POSITION_CRYPTO, CRYPTO_MAX_POSITION_BY_SYM,
        MAX_CAPITAL_PCT_STOCKS, MAX_CAPITAL_PCT_CRYPTO,
        MAX_POSITIONS_STOCKS, MAX_POSITIONS_CRYPTO, MAX_POSITIONS_NYSE,
        FMP_API_KEY as _TC_FMP_API_KEY, POLYGON_API_KEY as _TC_POLYGON_API_KEY,
        BRAPI_TOKEN as _TC_BRAPI_TOKEN, API_SECRET_KEY as _TC_API_SECRET_KEY,
        B3_TO_ADR, B3_ADR_SYMBOLS, CRYPTO_SYMBOLS, CRYPTO_NAMES,
        ENV as _TC_ENV
    )
    from modules.market_calendar import (
        is_b3_open, is_nyse_open, is_lse_open, is_hkex_open,
        is_tsx_open, is_euronext_open, market_open_for,
        NYSE_HOLIDAYS, B3_HOLIDAYS, LSE_HOLIDAYS, HKEX_HOLIDAYS,
        TZ_SAO_PAULO as _TZ_SP, TZ_NEW_YORK as _TZ_NY, TZ_LONDON as _TZ_LN, TZ_HK as _TZ_HK
    )
    from modules.feature_engine import (
        extract_features, make_feature_hash,
        _score_bucket, _rsi_bucket, _ema_alignment, _change_pct_bucket,
        _volatility_bucket, _time_bucket, _data_quality_bucket,
        _atr_bucket, _volume_bucket, _calc_atr
    )
    from modules.fees import (
        calc_fee as _module_calc_fee, apply_fee_to_trade as _module_apply_fee_to_trade,
        get_fees, _binance_rt, BINANCE_VIP_TIER, USE_BNB_DISCOUNT
    )
    from modules.learning_engine import (
        calc_learning_confidence, get_risk_multiplier
    )
    from modules.database import (
        db_config, get_db, test_db, _get_pool
    )
    from modules.signal_tracking import (
        record_signal_event, update_signal_attribution, update_signal_outcome,
        record_shadow_decision, _db_save_signal_event, _db_update_signal_attribution,
        _db_update_signal_outcome, _db_upsert_pattern_stats, _db_upsert_factor_stats,
        _db_save_shadow_decision, _db_log_learning_audit
    )
    from modules.ledger import (
        ledger_record, run_reconciliation, persist_calibration, load_calibration,
        _reconcile_strategy, _reconcile_strategy_arbi, _replay_ledger_events,
        _load_ledger_from_db, _record_baseline_if_needed, _reconcile_via_ledger
    )
    from modules.stock_fetcher import (
        _ema as _mod_ema, _rsi as _mod_rsi, _calc_atr as _mod_calc_atr,
        _get_cached_candles as _mod_get_cached_candles,
        _set_cached_candles as _mod_set_cached_candles,
        _fetch_polygon_stock as _mod_fetch_polygon_stock,
        _fetch_brapi_stock as _mod_fetch_brapi_stock,
        _fetch_brapi_batch as _mod_fetch_brapi_batch,
        _fetch_single_stock as _mod_fetch_single_stock,
    )
    from modules.crypto_fetcher import (
        _fetch_binance_ticker as _mod_fetch_binance_ticker,
        _fetch_binance_klines as _mod_fetch_binance_klines,
        _crypto_composite_score as _mod_crypto_composite_score,
        _update_market_regime as _mod_update_market_regime,
        calc_period_pnl as _mod_calc_period_pnl,
        is_momentum_positive as _mod_is_momentum_positive,
        fetch_fx_rates as _mod_fetch_fx_rates,
    )
    from modules.execution import (
        monitor_trades as _mod_monitor_trades,
        stock_execution_worker as _mod_stock_execution_worker,
        auto_trade_crypto as _mod_auto_trade_crypto,
        build_execution_ctx as _build_execution_ctx,
    )
    from modules.arbitrage import (
        arbi_scan_loop as _mod_arbi_scan_loop,
        arbi_monitor_loop as _mod_arbi_monitor_loop,
        arbi_learning_loop as _mod_arbi_learning_loop,
        calc_spread as _mod_calc_spread,
        run_arbi_pattern_learning as _mod_run_arbi_pattern_learning,
        build_arbitrage_ctx as _build_arbitrage_ctx,
    )
    from modules.api_routes import (
        api_bp as _mod_api_bp,
        init_routes as _mod_init_routes,
        create_api_blueprint as _mod_create_api_blueprint,
    )
    # Note: log is not yet initialized, so use print. It will be logged after logging.basicConfig
    print('[v10.28] All modules loaded: Phase1(5) + Phase2(3) + Phase3(2) + Phase4(2) + Phase5(1) = 13 modules', flush=True)
    _PURE_MODULES_LOADED = True
except Exception as _pm_err:
    import traceback as _pm_tb
    print(f'[v10.28] WARNING: Failed to load pure modules: {_pm_err}', flush=True)
    _pm_tb.print_exc()
    _PURE_MODULES_LOADED = False

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger('egreja')

app = Flask(__name__)

# ═══ [v10.31] CEDRO SOCKET PROVIDER — real-time streaming quotes ═══
# Primary source for B3 quotes (stocks + indices + futures). BRAPI/OpLab used as fallback.
_cedro_socket = None
try:
    from modules.cedro_socket_provider import get_cedro as _get_cedro_socket, is_enabled as _cedro_enabled
    if _cedro_enabled():
        _cedro_socket = _get_cedro_socket()
        log.info('[v10.31] CedroSocketProvider STARTED (primary for B3 real-time quotes)')
    else:
        log.info('[v10.31] CedroSocketProvider dormant (CEDRO_USER/CEDRO_PASSWORD not set)')
except Exception as _cedro_e:
    log.warning(f'[v10.31] CedroSocketProvider init failed: {_cedro_e}')
    _cedro_socket = None

def _fetch_cedro_stock(display: str) -> tuple:
    """[v10.31] Cedro socket primary for B3 stocks/indices.
    Merges real-time price/high/low/volume/fundamentals with cached candle-based EMAs/RSI/ATR.
    Returns (result_dict | None, latency_ms)."""
    if not _cedro_socket or not _cedro_socket.enabled:
        return None, 0.0
    t0 = time.time()
    q = _cedro_socket.get_quote(display, wait_ms=1200)
    lat = (time.time() - t0) * 1000
    if not q or not q.get('price'):
        return None, lat
    price = float(q.get('price') or 0)
    prev = float(q.get('prev_close') or 0) or price
    # Start from last BRAPI candle cache if exists (for EMAs/RSI/ATR)
    cached = _get_cached_candles(f'brapi:{display}') if '_get_cached_candles' in globals() else None
    if cached:
        entry = dict(cached)
        entry['price'] = price
        entry['prev'] = prev
        entry['change_pct'] = q.get('variation_pct') if q.get('variation_pct') is not None else (round((price/prev-1)*100, 2) if prev > 0 else 0)
        entry['source'] = 'cedro-socket+brapi-candles'
    else:
        entry = {
            'price': price, 'prev': prev,
            'change_pct': q.get('variation_pct') if q.get('variation_pct') is not None else (round((price/prev-1)*100, 2) if prev > 0 else 0),
            'ema9': price, 'ema21': price, 'ema50': price,
            'rsi': 50.0, 'atr_pct': 0.0, 'volume_ratio': 0.0,
            'ema9_real': False, 'ema21_real': False, 'ema50_real': False, 'rsi_real': False,
            'candles_available': 0, 'market': 'B3',
            'source': 'cedro-socket',
        }
    # Enrich with Cedro-exclusive real-time fields
    entry.update({
        'day_high': q.get('day_high'),
        'day_low': q.get('day_low'),
        'day_open': q.get('day_open'),
        'week_high': q.get('week_high'),
        'week_low': q.get('week_low'),
        'month_high': q.get('month_high'),
        'month_low': q.get('month_low'),
        'year_high': q.get('year_high'),
        'year_low': q.get('year_low'),
        'var_week_pct': q.get('var_week_pct'),
        'var_month_pct': q.get('var_month_pct'),
        'var_year_pct': q.get('var_year_pct'),
        'volume_financial': q.get('volume_financial'),
        'best_bid': q.get('best_bid'),
        'best_ask': q.get('best_ask'),
        'market_cap': q.get('market_cap'),
        'sector_code': q.get('sector_code'),
        'subsector_code': q.get('subsector_code'),
        'segment_code': q.get('segment_code'),
        'trading_phase': q.get('trading_phase'),
        'updated_at': datetime.utcnow().isoformat(),
    })
    try:
        record_data_quality(display, 'cedro-socket', lat, True)
    except Exception:
        pass
    return entry, lat

# ═══ [v10.27] DERIVATIVES MODULE INITIALIZATION ═══
# Provider chain: OpLab (primary) → Cedro (backup) → Simulated (fallback)
_deriv_config = get_deriv_config()
_deriv_provider_mgr = ProviderManager()

# 1. OpLab — primary provider for derivatives (options, Greeks, IV, book, rates)
#    ADR prices via Polygon, dividends via BRAPI (both already configured)
try:
    _oplab_provider = OpLabMarketDataProvider()
    _deriv_provider_mgr.register_provider('oplab', _oplab_provider, is_primary=True)
    if _oplab_provider._token:
        log.info('[v10.27] Derivatives: OpLabMarketDataProvider ACTIVE (primary — options/Greeks/IV/book)')
    else:
        log.info('[v10.27] Derivatives: OpLab registered but OPLAB_ACCESS_TOKEN not set')
except Exception as e:
    log.warning(f'[v10.27] OpLab provider init: {e}')

# 2. Cedro — backup provider (activate by setting CEDRO_LOGIN + CEDRO_PASSWORD)
try:
    _cedro_provider = CedroMarketDataProvider()
    _deriv_provider_mgr.register_provider('cedro', _cedro_provider, is_primary=False)
    if _cedro_provider._authenticated:
        log.info('[v10.27] Derivatives: CedroMarketDataProvider registered (backup — active)')
    else:
        log.info('[v10.27] Derivatives: Cedro registered (backup — dormant, no credentials)')
except Exception as e:
    log.warning(f'[v10.27] Cedro provider init: {e}')

# 3. Simulated — always available as last-resort fallback
try:
    _sim_provider = SimulatedMarketDataProvider()
    _deriv_provider_mgr.register_provider('simulated', _sim_provider, is_primary=False)
    _deriv_provider_mgr._active = _sim_provider  # direct fallback attr
    log.info('[v10.27] Derivatives: SimulatedMarketDataProvider registered (fallback/paper)')
except Exception as e:
    log.warning(f'[v10.27] Simulated provider init: {e}')

_deriv_services = {}
try:
    _deriv_services['options_cache'] = OptionsChainCache() if OptionsChainCache else None
    _deriv_services['futures_cache'] = FuturesChainCache() if FuturesChainCache else None
    _deriv_services['dividend_svc'] = DividendEventService() if DividendEventService else None
    _deriv_services['rates_svc'] = RatesCurveService() if RatesCurveService else None
    _rates_svc = _deriv_services.get('rates_svc')
    _deriv_services['greeks_calc'] = GreeksCalculator(_rates_svc) if GreeksCalculator and _rates_svc else None
    _deriv_services['calibration_svc'] = CalibrationService() if CalibrationService else None
    _deriv_services['scorecard_svc'] = StrategyScorecard() if StrategyScorecard else None
    _deriv_services['order_executor'] = StructuredOrderExecutor() if StructuredOrderExecutor else None
    _deriv_services['nav_calc'] = NAVCalculatorService() if NAVCalculatorService else None
    _greeks = _deriv_services.get('greeks_calc')
    _deriv_services['iv_engine'] = ImpliedVolEngine(_greeks) if ImpliedVolEngine and _greeks else None
    _deriv_services['liquidity_engine'] = LiquidityScoreEngine() if LiquidityScoreEngine else None
    _deriv_services['promotion_engine'] = PromotionEngine() if PromotionEngine else None
    _deriv_services['status_registry'] = ActiveStatusRegistry() if ActiveStatusRegistry else None
    # [FIX] strategies.py reads this under 'active_status_registry' — alias for compatibility
    _deriv_services['active_status_registry'] = _deriv_services['status_registry']
    # [v10.35] Aliases so all strategies.py scan loops can find services by their expected keys
    _deriv_services['greeks_calculator'] = _deriv_services.get('greeks_calc')
    _deriv_services['rates_curve'] = _deriv_services.get('rates_svc')
    _deriv_services['dividend_service'] = _deriv_services.get('dividend_svc')
    _deriv_services['nav_calculator'] = _deriv_services.get('nav_calc')
    # [FIX] Seed all tier-A assets × 8 strategies → PAPER_SMALL (paper mode, no real $)
    try:
        _sr = _deriv_services['status_registry']
        if _sr is not None:
            from modules.derivatives.liquidity import ExecutionTier as _ET
            _tier_a = ['PETR4','VALE3','ITUB4','BBDC4','BBAS3','ABEV3','B3SA3','BOVA11']
            _strats = ['PCP','FST','ROLL_ARB','ETF_BASKET','SKEW_ARB','INTERLISTED','DIVIDEND_ARB','VOL_ARB',
                        'IBOV_BASIS','DI_CALENDAR','INTERLISTED_HEDGED']  # [v10.42] added 3 missing
            # [v10.42] Futures assets need tiers too
            _futures_assets = ['WIN','IND','DI1','WINFUT','INDFUT']
            _tier_a = list(set(_tier_a + _futures_assets))
            for _a in _tier_a:
                for _s in _strats:
                    _sr.set_status(_a, _s, _ET.PAPER_SMALL, reason='bootstrap_seed')
            log.info(f'[FIX] Seeded {len(_tier_a)*len(_strats)} (asset,strategy) pairs to PAPER_SMALL')
    except Exception as _seed_err:
        log.warning(f'[FIX] registry seed failed: {_seed_err}')
    log.info(f'[v10.25] Derivatives services initialized: {len([v for v in _deriv_services.values() if v])} active')
except Exception as e:
    log.warning(f'[v10.25] Derivatives services init: {e}')

# ═══ [v10.35] DERIVATIVES PAPER TRADING ENGINE ═══
# Instantiate execution / capital / monitoring / sizing / learning so paper trades
# actually open, mark-to-market, and close with realized PnL for the 2-month track record.
_deriv_cap_mgr = None
_deriv_exec = None
_deriv_monitor = None
_deriv_sizer = None
_deriv_learner = None
try:
    # get_db is redefined later in the file (line ~722). Use a lambda to defer resolution.
    def _resolve_get_db():
        try:
            return get_db()
        except Exception:
            return None

    if DerivativesCapitalManager is not None:
        _deriv_cap_mgr = DerivativesCapitalManager(_deriv_config, get_db_fn=_resolve_get_db)
        _deriv_services['capital_manager'] = _deriv_cap_mgr
        # Alias some strategies may look for
        _deriv_services['deriv_capital'] = _deriv_cap_mgr

    if DerivativesPositionSizer is not None:
        _deriv_sizer = DerivativesPositionSizer(_deriv_config)
        _deriv_services['deriv_sizer'] = _deriv_sizer

    if DerivativesLearningEngine is not None:
        _deriv_learner = DerivativesLearningEngine(_deriv_config, get_db_fn=_resolve_get_db)
        _deriv_services['deriv_learner'] = _deriv_learner

    # [v10.38] Wire the unified brain learning engine into derivatives services
    try:
        from modules.unified_brain.learning_engine import LearningEngine as _UnifiedBrain
        if 'unified_brain' not in _deriv_services:
            _deriv_services['unified_brain'] = _UnifiedBrain(
                db_fn=_resolve_get_db,
                log=log,
            )
            log.info('[v10.38] unified_brain wired into derivatives services')
    except Exception as _brain_err:
        log.debug(f'[v10.38] unified_brain not available: {_brain_err}')

    if DerivativesExecutionEngine is not None and _deriv_cap_mgr is not None:
        _deriv_exec = DerivativesExecutionEngine(_deriv_config, _deriv_cap_mgr, get_db_fn=_resolve_get_db)
        _deriv_services['deriv_execution'] = _deriv_exec

    _greeks_calc = _deriv_services.get('greeks_calc')
    if DerivativesMonitor is not None and _deriv_exec is not None and _greeks_calc is not None:
        _deriv_monitor = DerivativesMonitor(
            execution_engine=_deriv_exec,
            greeks_calculator=_greeks_calc,
            provider_mgr=_deriv_provider_mgr,
            learning_engine=_deriv_learner,
            get_db_fn=_resolve_get_db,
        )
        _deriv_services['deriv_monitor'] = _deriv_monitor

    log.info(
        f'[v10.35] Paper engine: cap_mgr={bool(_deriv_cap_mgr)}, sizer={bool(_deriv_sizer)}, '
        f'learner={bool(_deriv_learner)}, exec={bool(_deriv_exec)}, monitor={bool(_deriv_monitor)}'
    )
except Exception as _eng_err:
    log.warning(f'[v10.35] Paper engine init failed: {_eng_err}')
    import traceback as _tb2
    _tb2.print_exc()
# ═══ END DERIVATIVES INIT ═══

# [v10.25] Register derivatives strategies blueprint
try:
    _strategies_bp = create_strategies_blueprint(
        db_fn=get_db,
        log=log,
        provider_mgr=globals().get('_deriv_provider_mgr'),
        services_dict=globals().get('_deriv_services', {}),
    )
    app.register_blueprint(_strategies_bp, url_prefix='/strategies')
    log.info('[v10.25] Derivatives strategies blueprint registered at /strategies/*')
except Exception as e:
    log.warning(f'[v10.25] Strategies blueprint registration: {e}')

# ── [v2.1] Long Horizon AI Blueprint ─────────────────────────────────
_lh_load_error = None
try:
    from modules.long_horizon.endpoints import create_long_horizon_blueprint
    _lh_bp = create_long_horizon_blueprint(
        db_fn=get_db,
        log=log,
    )
    app.register_blueprint(_lh_bp, url_prefix='/long-horizon')
    log.info('[v2.1] Long Horizon AI blueprint registered at /long-horizon/*')
except Exception as e:
    import traceback as _tb
    _lh_load_error = _tb.format_exc()
    log.warning(f'[v2.1] Long Horizon AI blueprint registration FAILED:\n{_lh_load_error}')

# ── [v3.2] Monthly Picks Sleeve (modular) ─────────────────────────────
_mp_load_error = None
try:
    from modules.long_horizon.monthly_picks.endpoints import create_monthly_picks_blueprint
    from modules.long_horizon.monthly_picks.repositories import MonthlyPicksRepository
    _mp_bp = create_monthly_picks_blueprint(
        db_fn=get_db,
        log=log,
        brain_lesson_fn=enqueue_brain_lesson,
    )
    app.register_blueprint(_mp_bp, url_prefix='/monthly-picks')
    log.info('[v3.2] Monthly Picks sleeve registered at /monthly-picks/* (modular)')
except Exception as e:
    import traceback as _tb
    _mp_load_error = _tb.format_exc()
    log.warning(f'[v3.2] Monthly Picks sleeve registration FAILED:\n{_mp_load_error}')

# ── [v2.2] Unified Brain (Intelligence Engine) Blueprint ──────────────
_brain_load_error = None
try:
    from modules.unified_brain.endpoints import create_unified_brain_blueprint
    from modules.unified_brain.schema import create_unified_brain_tables
    _brain_bp = create_unified_brain_blueprint(
        db_fn=get_db,
        log=log,
    )
    app.register_blueprint(_brain_bp, url_prefix='/brain')
    log.info('[v2.2] Unified Brain blueprint registered at /brain/*')
except Exception as e:
    import traceback as _tb
    _brain_load_error = _tb.format_exc()
    log.warning(f'[v2.2] Unified Brain blueprint registration FAILED:\n{_brain_load_error}')

CORS(app)

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
VERSION = 'v10.43'
_boot_time = time.time()

# ── [v10.23] Module instances ──────────────────────────────────────────
risk_manager = InstitutionalRiskManager()
order_tracker = OrderTracker()
data_validator = MarketDataValidator()
auth_manager = AuthManager()
audit_logger = AuditLogger()
perf_stats = PerformanceStats()
ext_kill_switch = ExternalKillSwitch()
kill_switch_middleware = KillSwitchMiddleware(ext_kill_switch)
ops_metrics = OpsMetricsCollector()

ENV = os.environ.get('ENV', 'dev').lower()

# [C-3] Single-process enforcement
try:
    _raw_workers = os.environ.get('WEB_CONCURRENCY', os.environ.get('GUNICORN_WORKERS', '1'))
    GUNICORN_WORKERS = int(str(_raw_workers).strip().strip('"').strip("'").split()[0])
except (ValueError, IndexError):
    log.warning(f'[C-3] Could not parse GUNICORN_WORKERS from env ({_raw_workers!r}), defaulting to 1')
    GUNICORN_WORKERS = 1
if GUNICORN_WORKERS > 1:
    raise RuntimeError(
        f'[C-3] This system uses in-process state (global lists + threads). '
        f'Running with {GUNICORN_WORKERS} workers would create parallel universes. '
        f'Set WEB_CONCURRENCY=1 or GUNICORN_WORKERS=1.')

INITIAL_CAPITAL_STOCKS = float(os.environ.get('INITIAL_CAPITAL_STOCKS', 9_000_000))
INITIAL_CAPITAL_CRYPTO = float(os.environ.get('INITIAL_CAPITAL_CRYPTO', 1_000_000))
MAX_POSITION_STOCKS    = float(os.environ.get('MAX_POSITION_STOCKS', 350_000))  # [v10.14] proporcional ao capital atual
MAX_POSITION_CRYPTO    = float(os.environ.get('MAX_POSITION_CRYPTO', 500_000))  # [v10.25] máximo global
# [v10.14] Posição máxima por símbolo — BTC e ETH são as âncoras de capital
CRYPTO_MAX_POSITION_BY_SYM = {
    'ETHUSDT':  float(os.environ.get('MAX_POS_ETH',  500_000)),  # [v10.24.4] 33% — melhor histórico WR 55%
    'BTCUSDT':  float(os.environ.get('MAX_POS_BTC',  300_000)),  # 20% — referência de mercado
    'ARBUSDT':  float(os.environ.get('MAX_POS_ARB',  200_000)),  # [v10.24.4] 13% — segundo melhor P&L
    'NEARUSDT': float(os.environ.get('MAX_POS_NEAR', 200_000)),  # 13% — terceiro WR 55%
    'BNBUSDT':  float(os.environ.get('MAX_POS_BNB',  200_000)),  # [v10.24.4] 13% — exchange coin estável
}

FMP_API_KEY      = os.environ.get('FMP_API_KEY', '')        # mantido como fallback terciário
POLYGON_API_KEY  = os.environ.get('POLYGON_API_KEY', '')    # primário para stocks US/NYSE
BRAPI_TOKEN      = os.environ.get('BRAPI_TOKEN', '')        # primário para stocks B3
# Binance: endpoints públicos sem key | frankfurter.app: BCE, free, sem key
API_SECRET_KEY = os.environ.get('API_SECRET_KEY', '')

if ENV == 'production' and not API_SECRET_KEY:
    raise RuntimeError('[P0-3] API_SECRET_KEY is REQUIRED in production.')
if not POLYGON_API_KEY:
    log.warning('POLYGON_API_KEY not set — stocks US/NYSE usarão fallback FMP→Yahoo')
if not BRAPI_TOKEN:
    log.info('BRAPI_TOKEN não configurado — B3 usará mapa ADR/Polygon como proxy')

# [v10.5-1] Mapa explícito B3 ticker → ADR no NYSE/NASDAQ para Polygon como fallback.
# Apenas os ADRs mais líquidos e com cobertura confiável no Polygon.
B3_TO_ADR = {
    'PETR4': 'PBR',   'PETR3': 'PBR-A',
    'VALE3': 'VALE',
    'ITUB4': 'ITUB',  'ITUB3': 'ITUB',
    'BBDC4': 'BBD',   'BBDC3': 'BBD',
    'ABEV3': 'ABEV',
    'EMBR3': 'ERJ',
    'PBR':   'PBR',   # já é ADR — passthrough
    'VALE':  'VALE',
}
# ADRs têm preço em USD; converter para BRL usando fx_rates['USDBRL']
B3_ADR_SYMBOLS = set(B3_TO_ADR.values())
if not FMP_API_KEY and not POLYGON_API_KEY:
    log.warning('Nenhuma API key configurada — usando Yahoo Finance (não recomendado em produção)')

PUBLIC_ROUTES = {'/', '/health', '/degraded', '/sync/export', '/sync/import'}

TWILIO_SID     = os.environ.get('TWILIO_ACCOUNT_SID', '')
TWILIO_TOKEN   = os.environ.get('TWILIO_AUTH_TOKEN', '')
TWILIO_FROM    = os.environ.get('TWILIO_WHATSAPP_FROM', 'whatsapp:+14155238886')
TWILIO_TO      = os.environ.get('TWILIO_WHATSAPP_TO', '')
ALERTS_ENABLED = bool(TWILIO_SID and TWILIO_TOKEN and TWILIO_TO)
ALERT_MIN_SCORE = int(os.environ.get('ALERT_MIN_SCORE', 80))

MAX_CAPITAL_PCT_STOCKS   = float(os.environ.get('MAX_CAPITAL_PCT_STOCKS', 100.0))  # [v10.14] 100% do capital
MAX_CAPITAL_PCT_CRYPTO   = float(os.environ.get('MAX_CAPITAL_PCT_CRYPTO', 100.0))  # [v10.14] 100% do capital
MAX_POSITIONS_STOCKS     = 60  # [v10.14] 60 posições simultâneas (env var ignorada)
MAX_POSITIONS_CRYPTO     = int(os.environ.get('MAX_POSITIONS_CRYPTO', 20)) # [v10.47] 20 simultâneas — v3 discrimina, não deixar dinheiro parado
MAX_POSITIONS_NYSE       = int(os.environ.get('MAX_POSITIONS_NYSE', 20)) # [v10.47]

# Settings ajustaveis em runtime (via /settings POST)
KILL_SWITCH_USD          = float(os.environ.get('KILL_SWITCH_USD', 30000))
STOCK_TP_PCT             = float(os.environ.get('STOCK_TP_PCT', 2.0))
STOCK_SL_PCT             = float(os.environ.get('STOCK_SL_PCT', 2.0))
TRAILING_FLOOR_PCT       = float(os.environ.get('TRAILING_FLOOR_PCT', 0.3))
TRAILING_TRIGGER_PCT     = float(os.environ.get('TRAILING_TRIGGER_PCT', 1.5))
TIMEOUT_B3_H             = float(os.environ.get('TIMEOUT_B3_H', 5))
TIMEOUT_CRYPTO_H         = float(os.environ.get('TIMEOUT_CRYPTO_H', 48))
TIMEOUT_NYSE_H           = float(os.environ.get('TIMEOUT_NYSE_H', 7))
MIN_SCORE_AUTO           = int(os.environ.get('MIN_SCORE_AUTO', 70))
# [v10.47] Sistema APENAS V3 — sem fallback para v1
# Emergency stop: se setado true, pula todos os loops de trading (sistema morto)
V3_EMERGENCY_STOP        = os.environ.get('V3_EMERGENCY_STOP', 'false').lower() == 'true'
MIN_SCORE_AUTO_CRYPTO    = int(os.environ.get('MIN_SCORE_AUTO_CRYPTO', 55))  # [v10.15] crypto threshold 55 (era 48) — reduz over-trading
DEFAULT_POSITION_SIZE    = float(os.environ.get('DEFAULT_POSITION_SIZE', 100000))

# [v10.47] Position sizing escalado por score (dinheiro parado é prejuízo)
# Quanto maior confiança do score v3, maior o capital alocado
# Baseline 1.0x para score neutro, máximo 2.0x em score 85+
SCORE_SIZING_ENABLED     = os.environ.get('SCORE_SIZING_ENABLED', 'true').lower() != 'false'
SCORE_SIZING_MAX_MULT    = float(os.environ.get('SCORE_SIZING_MAX_MULT', 2.0))

def get_score_sizing_mult(score: int) -> float:
    """Retorna multiplicador de sizing baseado no score [0-100].

    - Score 85-100: 2.0x (alta confiança, alocar mais)
    - Score 75-84:  1.6x
    - Score 65-74:  1.3x
    - Score 50-64:  1.0x (baseline)
    - Score <50:    0.8x (LOW/SHORT — ainda passa, mas menor)

    Para SHORTs (score baixo indica venda forte), o multiplicador
    também cresce — usamos abs(50-score) como magnitude.
    """
    if not SCORE_SIZING_ENABLED:
        return 1.0
    mag = abs(score - 50)  # distância do neutro
    if   mag >= 35: return min(SCORE_SIZING_MAX_MULT, 2.0)   # score 85+ ou ≤15
    elif mag >= 25: return 1.6                                # 75-84 ou 16-25
    elif mag >= 15: return 1.3                                # 65-74 ou 26-35
    elif mag >= 5:  return 1.0                                # 55-64 ou 36-45
    else:           return 0.8                                # 46-54 (zona morta)


# Arbitragem — livro segregado
ARBI_CAPITAL         = float(os.environ.get('ARBI_CAPITAL', 4_500_000))  # [v10.9] aumentado de 3M para 4.5M
ARBI_MIN_SPREAD      = float(os.environ.get('ARBI_MIN_SPREAD', 0.8))    # [v10.14] BTG: era 2.0%
ARBI_MAX_SPREAD      = float(os.environ.get('ARBI_MAX_SPREAD', 15.0))  # [v10.9] teto: spread >15% = estrutural/preço inválido
ARBI_TP_SPREAD       = float(os.environ.get('ARBI_TP_SPREAD',  0.20))   # [v10.14] BTG: era 0.5%
ARBI_SL_PCT          = float(os.environ.get('ARBI_SL_PCT',    0.80))   # [v10.14] BTG: era 1.5%
ARBI_TIMEOUT_H       = float(os.environ.get('ARBI_TIMEOUT_H',  48))    # [v10.14] BTG: era 72h
ARBI_POS_SIZE        = float(os.environ.get('ARBI_POS_SIZE', 50_000))
ARBI_MAX_POSITIONS   = int(os.environ.get('ARBI_MAX_POSITIONS', 8))
ARBI_MAX_DAILY_LOSS  = float(os.environ.get('ARBI_MAX_DAILY_LOSS_PCT', 1.5))
ARBI_KILL_SWITCH     = False

# Risco global
MAX_OPEN_POSITIONS      = 65  # [v10.14] 60 stocks + 5 crypto (hardcoded)
MAX_DAILY_DRAWDOWN_PCT  = float(os.environ.get('MAX_DAILY_DRAWDOWN_PCT', 2.0))
MAX_WEEKLY_DRAWDOWN_PCT = float(os.environ.get('MAX_WEEKLY_DRAWDOWN_PCT', 5.0))
MAX_POSITION_SAME_MKT   = int(os.environ.get('MAX_POSITION_SAME_MKT', 20))  # [v10.47]
MAX_SAME_SYMBOL         = int(os.environ.get('MAX_SAME_SYMBOL', 1))
MAX_RISK_PER_TRADE_PCT  = float(os.environ.get('MAX_RISK_PER_TRADE_PCT', 1.5))
RISK_KILL_SWITCH        = False

SIGNAL_MAX_AGE_MIN  = int(os.environ.get('SIGNAL_MAX_AGE_MIN', 30))
SYMBOL_COOLDOWN_SEC = int(os.environ.get('SYMBOL_COOLDOWN_SEC', 300))

# [V9-2] Limites de proteção da fila crítica
URGENT_QUEUE_WARN = int(os.environ.get('URGENT_QUEUE_WARN', 1000))
URGENT_QUEUE_CRIT = int(os.environ.get('URGENT_QUEUE_CRIT', 5000))
_queue_alert_last = 0   # throttle de alerta da fila

# [C-1] Heartbeat timeout POR THREAD (segundos)
THREAD_HEARTBEAT_TIMEOUT = {
    'stock_price_loop':       600,   # [v10.9] 10min — loop off-hours tem sleep 5x60s
    'crypto_price_loop':      120,   # [v10.9] 2min — aumentado
    'monitor_trades':         60,    # [v10.9] 60s — era 30s, muitas trades podem demorar
    'auto_trade_crypto':      300,   # [v10.9] 5min
    'stock_execution_worker': 300,   # [v10.9] 5min — era 150s, loop de 60s + processamento
    'arbi_scan_loop':         600,
    'arbi_monitor_loop':      180,
    'snapshot_loop':          600,
    'persistence_worker':     60,    # [v10.9] 60s
    'alert_worker':           60,
    'watchdog':               90,
    'shadow_evaluator_loop':  1800,  # 30 min
    'network_sync_loop':      150,   # [v10.9] 2.5min
    'report_scheduler':       150,   # acorda a cada 60s — 30 sleeps de 60s mas beat intermediário,
    # [v10.25] Derivatives scan loops
    'pcp_scan_loop': 300,
    'fst_scan_loop': 300,
    'roll_arb_scan_loop': 300,
    'etf_basket_scan_loop': 300,
    'skew_arb_scan_loop': 300,
    'interlisted_scan_loop': 300,
    'dividend_arb_scan_loop': 600,
    'vol_arb_scan_loop': 300,
    # [v10.38] New futures-arbitrage strategies
    'ibov_basis_scan_loop': 300,
    'di_calendar_scan_loop': 300,
    'interlisted_hedged_scan_loop': 300,
    # [v10.35] Derivatives paper monitoring loop (beats every ~15s)
    'deriv_monitor_loop': 120,
    # [v10.52] Workers de cadencia longa — heartbeats raros por design
    'brain_hourly_reminder': 3900,   # roda 1x/hora + beat a cada minuto
    'monthly_picks_worker':  7200,   # 2h — loop external com logica mensal/semanal
    'arbi_learning_loop':    600,    # 10min — sleep de 5min entre passes
    'pattern_discovery':     7200,   # [v10.13] minera a cada 6h
    # [v11] Portfolio Accounting — shadow comparator (60s interval + margem)
    'portfolio_shadow_comparator': 180,
}
DEFAULT_HB_TIMEOUT = int(os.environ.get('DEFAULT_HB_TIMEOUT', 120))
WATCHDOG_RESET_STABLE_H = float(os.environ.get('WATCHDOG_RESET_STABLE_H', 6.0))

# ── Learning Engine config ────────────────────────────────────────
LEARNING_VERSION       = '10.17.0'
LEARNING_MIN_SAMPLES   = int(os.environ.get('LEARNING_MIN_SAMPLES', 10))   # amostras mínimas para confiar no histórico
LEARNING_EWMA_ALPHA    = float(os.environ.get('LEARNING_EWMA_ALPHA', 0.15)) # recência (0=ignore histórico, 1=só recente)
RISK_MULT_MIN          = float(os.environ.get('RISK_MULT_MIN', 0.30))       # [L-9][v10.15] multiplicador mínimo (era 0.50)
RISK_MULT_MAX          = float(os.environ.get('RISK_MULT_MAX', 1.50))       # [L-9][v10.15] multiplicador máximo (era 1.15)
# [v10.9-Learning] Dead zone de confiança: faixa onde o histórico mostra performance negativa
# Dados de 10 dias: faixa 55-64 = 38-44% WR, -$53K em perdas. Não executar.
LEARNING_DEAD_ZONE_LOW  = float(os.environ.get('LEARNING_DEAD_ZONE_LOW',  58.0))  # [v10.24] era 55 — estreitado para não colidir com conviction=52
LEARNING_DEAD_ZONE_HIGH = float(os.environ.get('LEARNING_DEAD_ZONE_HIGH', 63.0))  # [v10.24] era 65 — dead zone mais cirúrgica [58,63)
SHADOW_TRACK_REASONS   = {'confidence_low','market_closed','risk_blocked','symbol_open','kill_switch','cooldown','capital'}
# ── [v10.16] Daily drawdown per strategy ──────────────────────────────────
DAILY_DD_STOCKS_PCT   = float(os.environ.get('DAILY_DD_STOCKS_PCT', 1.5))   # max 1.5% drawdown diário em stocks
DAILY_DD_CRYPTO_PCT   = float(os.environ.get('DAILY_DD_CRYPTO_PCT', 2.0))   # max 2.0% drawdown diário em crypto (mais volátil)
# ── [v10.16] Auto-blacklist: suspende símbolo com histórico ruim ──────────
BLACKLIST_MIN_TRADES    = int(os.environ.get('BLACKLIST_MIN_TRADES', 9999))   # [v10.47] DESLIGADO — v3 discrimina por score, não por histórico por símbolo
BLACKLIST_MAX_AVG_PNL   = float(os.environ.get('BLACKLIST_MAX_AVG_PNL', -40)) # avg PnL < -$40 = blacklist
BLACKLIST_MAX_WR        = float(os.environ.get('BLACKLIST_MAX_WR', 0))        # [v10.47] 0 = nunca blacklist (v3 decide)
BLACKLIST_REVIEW_H      = float(os.environ.get('BLACKLIST_REVIEW_H', 24))     # reavaliar a cada 24h
# ── [v10.16] ATR-based adaptive stop-loss ─────────────────────────────────
ATR_SL_MULTIPLIER_STOCK  = float(os.environ.get('ATR_SL_MULTIPLIER_STOCK', 2.5))  # SL = ATR * 2.5 para stocks
ATR_SL_MULTIPLIER_CRYPTO = float(os.environ.get('ATR_SL_MULTIPLIER_CRYPTO', 2.0)) # SL = ATR * 2.0 para crypto
ATR_SL_MIN_PCT           = float(os.environ.get('ATR_SL_MIN_PCT', 0.8))           # SL mínimo 0.8%
ATR_SL_MAX_PCT           = float(os.environ.get('ATR_SL_MAX_PCT', 4.0))           # SL máximo 4.0%
# ── [v10.16] Inactivity alert ─────────────────────────────────────────────
INACTIVITY_ALERT_H_STOCKS = float(os.environ.get('INACTIVITY_ALERT_H_STOCKS', 4))  # alertar se 0 trades stocks em 4h (horário de mercado)
INACTIVITY_ALERT_H_CRYPTO = float(os.environ.get('INACTIVITY_ALERT_H_CRYPTO', 8))  # alertar se 0 trades crypto em 8h
# ── [v10.17] Flat Exit — fecha trades estagnadas ──────────────────────────
FLAT_EXIT_MIN_AGE_MIN     = float(os.environ.get('FLAT_EXIT_MIN_AGE_MIN', 45))     # minutos mínimos antes de avaliar flat
FLAT_EXIT_MAX_VARIATION   = float(os.environ.get('FLAT_EXIT_MAX_VARIATION', 0.30))  # variação máxima (%) para considerar flat
# ── [v10.17] Trailing stop triggers (mais baixos) ────────────────────────
TRAILING_PEAK_STOCKS      = float(os.environ.get('TRAILING_PEAK_STOCKS', 1.0))     # era 1.5% — ativa trailing mais cedo
TRAILING_DROP_STOCKS      = float(os.environ.get('TRAILING_DROP_STOCKS', 0.4))     # era 0.5% — retração menor
TRAILING_PEAK_CRYPTO      = float(os.environ.get('TRAILING_PEAK_CRYPTO', 1.5))     # era 2.0% — ativa trailing mais cedo
TRAILING_DROP_CRYPTO      = float(os.environ.get('TRAILING_DROP_CRYPTO', 0.7))     # era 1.0% — retração menor
# ── [v10.17] Directional exposure limit ───────────────────────────────────
MAX_DIRECTIONAL_PCT       = float(os.environ.get('MAX_DIRECTIONAL_PCT', 70))       # max 70% das posições na mesma direção (stocks)
MAX_DIRECTIONAL_PCT_CRYPTO = float(os.environ.get('MAX_DIRECTIONAL_PCT_CRYPTO', 100))  # [v10.24.4] crypto: 100% — mercado correlacionado, diversificação é entre moedas
# ── [v10.17] Dynamic timeout ─────────────────────────────────────────────
DYNAMIC_TIMEOUT_ENABLED   = os.environ.get('DYNAMIC_TIMEOUT_ENABLED', 'true').lower() != 'false'
DYNAMIC_TIMEOUT_MULT      = float(os.environ.get('DYNAMIC_TIMEOUT_MULT', 1.3))    # timeout = avg_dur * 1.3
DYNAMIC_TIMEOUT_MIN_H     = float(os.environ.get('DYNAMIC_TIMEOUT_MIN_H', 1.5))   # timeout mínimo 1.5h
DYNAMIC_TIMEOUT_MAX_H     = float(os.environ.get('DYNAMIC_TIMEOUT_MAX_H', 6.0))   # timeout máximo 6h
# ── [v10.18] Calibration persistence ─────────────────────────────────────
CALIBRATION_PERSIST_INTERVAL = int(os.environ.get('CALIBRATION_PERSIST_INTERVAL', 300))  # 5 min
# ── [v10.18] Reconciliation ─────────────────────────────────────────────
RECONCILIATION_INTERVAL_S    = int(os.environ.get('RECONCILIATION_INTERVAL_S', 600))     # 10 min
RECONCILIATION_ALERT_PCT     = float(os.environ.get('RECONCILIATION_ALERT_PCT', 2.0))    # alertar se >2% desvio (camada 1: formula)
# [v10.52] Threshold dedicado para camada 2 (ledger replay). Ledger foi
# introduzido em v10.20 — trades anteriores NAO foram retroativamente
# registrados no ledger, entao replay sempre diverge da memoria por
# historico incompleto (falso positivo). Default 100% = nao alerta
# enquanto nao tiver ferramenta de rehidratacao. Camada 1 continua
# estrita em 2% para alertas reais.
RECONCILIATION_LEDGER_ALERT_PCT = float(os.environ.get('RECONCILIATION_LEDGER_ALERT_PCT', 100.0))
# ── [v10.18] Crypto conviction filter ───────────────────────────────────
CRYPTO_MIN_CONVICTION        = float(os.environ.get('CRYPTO_MIN_CONVICTION', 52))        # [v10.24] era 58 — muito restritivo, bloqueava quase tudo em mercado lateral
CRYPTO_MIN_HOLD_MIN          = float(os.environ.get('CRYPTO_MIN_HOLD_MIN', 180))         # [v10.46.6] 15→180min — era muito agressivo, matava trades boas do v3
LEARNING_ENABLED       = os.environ.get('LEARNING_ENABLED', 'true').lower() != 'false'

CRYPTO_SYMBOLS = [
    # [v10.47] Universo de 20 cryptos (expansão via v3 regime-aware)
    # Filosofia: v3 decide COMPRA ou VENDA por símbolo — downtrend vira SHORT.
    # MAX_POSITIONS_CRYPTO=20 | Blacklist OFF | Score sizing 2.0x em score 85+
    'BTCUSDT',   # Bitcoin — referência de mercado
    'ETHUSDT',   # Ethereum — histórico melhor WR 55%
    'BNBUSDT',   # BNB — exchange coin
    'SOLUSDT',   # Solana — V3 SHORT WR 91%
    'XRPUSDT',   # XRP — alta liquidez
    'ADAUSDT',   # Cardano
    'DOGEUSDT',  # Dogecoin — meme coin
    'AVAXUSDT',  # Avalanche
    'TRXUSDT',   # TRON — V3 LONG WR 85%
    'DOTUSDT',   # Polkadot
    'LINKUSDT',  # Chainlink
    # 'MATICUSDT', # Polygon — REMOVIDO 2026-04-20: bug de preco recorrente
    #                (MATIC foi renomeado para POL pela Polygon Labs; Binance/
    #                 Polygon mostram precos inconsistentes). Trade CRY-ec2fd...
    #                teve P&L +$55K impossivel. Ver ops_void_trade + blacklist.
    'LTCUSDT',   # Litecoin
    'UNIUSDT',   # Uniswap
    'ATOMUSDT',  # Cosmos
    'XLMUSDT',   # Stellar
    'BCHUSDT',   # Bitcoin Cash
    'NEARUSDT',  # NEAR
    'APTUSDT',   # Aptos
    'ARBUSDT',   # Arbitrum
]
CRYPTO_NAMES = {
    'BTCUSDT':'Bitcoin','ETHUSDT':'Ethereum','BNBUSDT':'BNB','SOLUSDT':'Solana',
    'XRPUSDT':'XRP','ADAUSDT':'Cardano','DOGEUSDT':'Dogecoin','AVAXUSDT':'Avalanche',
    'TRXUSDT':'TRON','DOTUSDT':'Polkadot','LINKUSDT':'Chainlink','MATICUSDT':'Polygon',
    'LTCUSDT':'Litecoin','UNIUSDT':'Uniswap','ATOMUSDT':'Cosmos','XLMUSDT':'Stellar',
    'BCHUSDT':'Bitcoin Cash','NEARUSDT':'NEAR','APTUSDT':'Aptos','ARBUSDT':'Arbitrum'
}

# ═══════════════════════════════════════════════════════════════
# MYSQL
# ═══════════════════════════════════════════════════════════════
db_config = {
    'host':     os.environ.get('MYSQLHOST', 'mysql.railway.internal'),
    'port':     int(os.environ.get('MYSQLPORT', 3306)),
    'user':     os.environ.get('MYSQLUSER', 'root'),
    'password': os.environ.get('MYSQLPASSWORD', ''),
    'database': os.environ.get('MYSQLDATABASE', 'railway'),
    'autocommit': True, 'connection_timeout': 10
}

# [v10.7-Fix1] Connection pool — elimina overhead de connect/disconnect por operação.
# Pool de 10 conexões: suficiente para persistence_worker + shadow_evaluator + workers simultâneos.
# Sem pool: cada get_db() abre uma TCP connection nova (~5-50ms), Railway tem limite baixo.
_db_pool = None
_db_pool_lock = threading.Lock()

def _get_pool():
    """Inicializa o pool na primeira chamada (lazy) e retorna a instância."""
    global _db_pool
    if _db_pool is not None:
        return _db_pool
    with _db_pool_lock:
        if _db_pool is not None:
            return _db_pool
        try:
            from mysql.connector.pooling import MySQLConnectionPool
            pool_cfg = dict(db_config)
            pool_cfg.pop('autocommit', None)   # pooling não aceita autocommit no config
            pool_cfg.pop('connection_timeout', None)
            # mysql.connector pool size tem HARD CAP = 32
            _pool_sz = min(int(os.environ.get('MYSQL_POOL_SIZE', 32)), 32)
            _db_pool = MySQLConnectionPool(
                pool_name='egreja', pool_size=_pool_sz,
                pool_reset_session=False,  # [28abr] mais leve, evita reset overhead
                autocommit=True, connection_timeout=10,
                **pool_cfg)
            log.info(f'[v10.7] MySQL connection pool inicializado (size={_pool_sz})')
        except Exception as e:
            log.error(f'MySQL pool init: {e}')
    return _db_pool

def get_db():
    """Retorna uma conexão do pool. Caller é responsável por chamar .close()
    (que devolve a conexão ao pool, não fecha a TCP connection).
    Em caso de falha no pool, faz fallback para conexão direta.
    """
    pool = _get_pool()
    if pool:
        try:
            return pool.get_connection()
        except Exception as e:
            log.warning(f'Pool get_connection: {e} — tentando conexão direta')
    # Fallback direto (ex.: pool esgotado ou erro de inicialização)
    try:
        return mysql.connector.connect(**db_config)
    except Exception as e:
        log.error(f'MySQL fallback connect: {e}')
        return None

def test_db():
    c = get_db()
    if c: c.close(); return True
    return False

# ═══════════════════════════════════════════════════════════════
# STATE + LOCKS
# ═══════════════════════════════════════════════════════════════
stocks_capital = INITIAL_CAPITAL_STOCKS
crypto_capital = INITIAL_CAPITAL_CRYPTO
arbi_capital   = ARBI_CAPITAL

stocks_open  = []; stocks_closed  = []
crypto_open  = []; crypto_closed  = []
arbi_open    = []; arbi_closed    = []

# [v10.7-Fix3] Cap em listas de trades fechados.
# Sem cap, após 6 meses com 20-30 trades/dia = 3.000-5.000 entradas em memória.
# check_risk() itera s_closed+c_closed em cada sinal → O(n) no caminho crítico.
# 500 entradas cobre >7 dias de histórico para drawdown (janela máxima = 7d).
MAX_CLOSED_HISTORY = int(os.environ.get('MAX_CLOSED_HISTORY', 0))  # 0 = sem limite

state_lock       = threading.Lock()
orders_lock      = threading.Lock()
audit_lock       = threading.Lock()
dq_lock          = threading.Lock()
degraded_lock    = threading.Lock()   # [V91-5] protege DEGRADED_MODE
learning_lock    = threading.Lock()   # [L-3] protege caches de learning em memória

stock_prices    = {}
crypto_prices   = {}
crypto_momentum = {}
crypto_tickers  = {}   # [v10.4] dados extras Binance: high_24h, low_24h, vol_quote, n_trades
market_regime   = {'mode':'UNKNOWN','volatility':'NORMAL','avg_change_pct':0,'updated_at':''}
arbi_spreads    = {}
fx_rates        = {}

symbol_cooldown = {}
_trailing_stop_cooldown = {}  # sym → timestamp do último trailing stop
symbol_sl_count  = {}   # [v10.9-CircuitBreaker] conta stop losses consecutivos por símbolo
symbol_blocked   = set()  # [v10.9] símbolos bloqueados manualmente (admin)
# Cooldown exponencial: 2 SLs→10min, 3→30min, 4+→2h
SYMBOL_SL_COOLDOWNS = {1: 300, 2: 600, 3: 1800, 4: 7200}
alerted_signals = {}
alerted_trades  = {}

orders_log  = []
audit_log   = []
data_quality= {}

# ── [L-3/L-4] Caches de learning em memória (protegidos por learning_lock) ──
# Espelham as tabelas pattern_stats e factor_stats para evitar I/O no caminho crítico
pattern_stats_cache: dict = {}   # feature_hash → stats dict
factor_stats_cache:  dict = {}   # (factor_type, factor_value) → stats dict
signal_events_count: int  = 0
last_learning_update: str = ''
learning_errors:     int  = 0
LEARNING_DEGRADED:   bool = False

# [P0-2] Deduplicação: rastreia IDs de market_signals já processados nesta sessão.
# Formato: {market_signal_db_id: signal_event_id}
# LRU manual: descarta metade quando ultrapassa MAX_PROCESSED_SIGNALS_CACHE.
# Evita que o mesmo sinal de origem gere vários signal_events e shadow_decisions
# enquanto permanecer dentro da janela SIGNAL_MAX_AGE_MIN.
MAX_PROCESSED_SIGNALS_CACHE = 2000
processed_signal_ids: dict = {}   # market_signal_id → signal_event_id

thread_health        = {}
thread_fns           = {}
thread_restart_count = {}
thread_last_restart  = {}
thread_heartbeat     = {}

# ── [v10.16] State: daily drawdown per strategy ──────────────────────────
_daily_dd_stocks = {'date': '', 'pnl': 0.0, 'blocked': False}
_daily_dd_crypto = {'date': '', 'pnl': 0.0, 'blocked': False}
# ── [v10.16] Auto-blacklist state ─────────────────────────────────────────
_symbol_blacklist: dict = {}       # symbol → {'reason': str, 'until': float(timestamp), 'stats': dict}
_blacklist_last_eval: float = 0.0  # timestamp da última avaliação
# ── [v10.16] Inactivity tracking ──────────────────────────────────────────
_last_trade_opened = {'stocks': 0.0, 'crypto': 0.0}  # timestamp do último trade aberto
_inactivity_alerted = {'stocks': False, 'crypto': False}
# ── [v10.17] Symbol duration tracker (avg hours per symbol) ───────────────
_symbol_avg_duration: dict = {}  # symbol → {'sum_h': float, 'n': int, 'avg_h': float}
# ── [v10.18] Capital ledger ──────────────────────────────────────────────
_capital_ledger: list = []        # [{ts, strategy, event, symbol, amount, balance_after, trade_id}]
_ledger_lock = threading.Lock()
# ── [v10.18] Calibration & reconciliation persistence ────────────────────
_last_calibration_persist: float = 0.0
_last_reconciliation: float = 0.0
_reconciliation_log: list = []    # últimas N reconciliações

def gen_id(prefix='TRD'):
    return f"{prefix}-{uuid.uuid4().hex[:12]}"

def beat(name):
    thread_heartbeat[name] = time.time()

# ═══════════════════════════════════════════════════════════════
# [V9-3] DEGRADED MODE
# ═══════════════════════════════════════════════════════════════
DEGRADED_MODE = {
    'active':     False,
    'reasons':    [],
    'since':      None,
    'queue_size': 0,
}

def _check_degraded():
    """[V9-3][V91-5] Atualiza estado degradado com lock próprio."""
    reasons = []
    qsize = urgent_queue.qsize()

    if qsize >= URGENT_QUEUE_CRIT:
        reasons.append(f'QUEUE_CRITICAL:{qsize}')
    elif qsize >= URGENT_QUEUE_WARN:
        reasons.append(f'QUEUE_HIGH:{qsize}')

    with dq_lock:
        dq_snap = list(data_quality.values())
    if dq_snap:
        stale_n = sum(1 for s in dq_snap if s.get('stale'))
        if stale_n / len(dq_snap) > 0.5:
            reasons.append(f'FEED_STALE:{stale_n}/{len(dq_snap)}')

    now = time.time()
    recent_restarts = sum(
        1 for name in thread_restart_count
        if thread_restart_count.get(name, 0) > 0
        and (now - thread_last_restart.get(name, 0)) < 6 * 3600
    )
    if recent_restarts > 0:
        reasons.append(f'THREAD_RESTARTS:{recent_restarts}')

    if RISK_KILL_SWITCH:
        reasons.append('KILL_SWITCH_ACTIVE')
    if ARBI_KILL_SWITCH:
        reasons.append('ARBI_KILL_SWITCH_ACTIVE')

    active = len(reasons) > 0

    with degraded_lock:   # [V91-5]
        was_active = DEGRADED_MODE['active']
        if active and not was_active:
            DEGRADED_MODE['since'] = datetime.utcnow().isoformat()
        elif not active:
            DEGRADED_MODE['since'] = None
        DEGRADED_MODE['active']     = active
        DEGRADED_MODE['reasons']    = reasons
        DEGRADED_MODE['queue_size'] = qsize

def _read_degraded():
    """[V91-5] Leitura thread-safe de DEGRADED_MODE."""
    with degraded_lock:
        return dict(DEGRADED_MODE)

# ═══════════════════════════════════════════════════════════════
# [BUG-1] QUEUES — PriorityQueue com (priority, seq, item)
#   Sem counter monotônico, dois dicts com mesma priority causam
#   TypeError: '<' not supported between instances of 'dict' and 'dict'
# ═══════════════════════════════════════════════════════════════
_urgent_seq   = itertools.count()      # [BUG-1] contador monotônico — nunca compara dicts
persist_queue = queue.Queue()          # mantido para compat, não usado diretamente
alert_queue   = queue.Queue(maxsize=500)
urgent_queue  = queue.PriorityQueue() # (priority, seq, item) — sem risco de comparar dicts

PERSIST_PRIORITY = {
    'trade':                1,
    'order':                1,
    'audit':                2,
    'arbi':                 1,
    'cooldown':             3,
    'snapshot':             4,
    'signal_event':         3,
    'signal_attribution':   2,   # [FIX-2] vínculo trade↔sinal — prioridade alta
    'signal_outcome':       2,
    'pattern_stats':        4,
    'factor_stats':         4,
    'shadow_decision':      5,
}

def enqueue_persist(kind, data=None, **kwargs):
    """[BUG-1] Enfileira para persistência. Nunca descarta. Nunca compara dicts."""
    item = {'kind': kind}
    if data is not None: item['data'] = data
    item.update(kwargs)
    priority = PERSIST_PRIORITY.get(kind, 5)
    # [BUG-1] seq garante ordem FIFO dentro de mesma prioridade e NUNCA compara dicts
    urgent_queue.put((priority, next(_urgent_seq), item))

_whatsapp_dedup_cache: dict = {}  # fingerprint -> last_sent_ts
_WHATSAPP_DEDUP_WINDOW_S = int(os.environ.get('WHATSAPP_DEDUP_WINDOW_S', '1800'))  # 30 min

def send_whatsapp(message):
    """Enfileira alerta WhatsApp com de-duplicacao por fingerprint.

    Evita spam de alertas identicos (ex: 'CRITICO: thread starvation! N
    threads ativos' disparando a cada ciclo do watchdog). Msgs com mesmo
    texto base (normalizando numeros) sao silenciadas por
    WHATSAPP_DEDUP_WINDOW_S segundos (default 1800s = 30 min).

    Alertas legitimamente NOVOS (com texto diferente) passam normalmente.
    """
    try:
        import re as _re
        # Fingerprint = primeiros 100 chars com numeros normalizados para 'N'.
        # Isso agrupa 'stocks_ledger delta=-2,095,833' e
        # 'stocks_ledger delta=-2,095,999' como mesma mensagem.
        fp = _re.sub(r'[-+]?\d[\d,._]*', 'N', message[:100])
        now = time.time()
        last = _whatsapp_dedup_cache.get(fp, 0)
        if now - last < _WHATSAPP_DEDUP_WINDOW_S:
            # Silenciado — ja enviado recentemente. Log apenas.
            log.info(f'[whatsapp-dedup] silenced (last {int(now-last)}s ago): {message[:60]}')
            return
        _whatsapp_dedup_cache[fp] = now
        # Limpa entradas antigas para nao crescer indefinidamente
        if len(_whatsapp_dedup_cache) > 500:
            cutoff = now - _WHATSAPP_DEDUP_WINDOW_S
            for k in [k for k, ts in _whatsapp_dedup_cache.items() if ts < cutoff]:
                _whatsapp_dedup_cache.pop(k, None)

        alert_queue.put_nowait({'kind': 'whatsapp', 'message': message})
    except queue.Full:
        log.warning(f'alert_queue full — alert dropped: {message[:60]}')

def _send_whatsapp_direct(message):
    if not ALERTS_ENABLED:
        log.info(f'[ALERT disabled] {message[:80]}'); return False
    try:
        r = requests.post(
            f'https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json',
            auth=(TWILIO_SID, TWILIO_TOKEN),
            data={'From': TWILIO_FROM, 'To': TWILIO_TO, 'Body': message}, timeout=10)
        return r.status_code == 201
    except Exception as e:
        log.error(f'WhatsApp direct: {e}'); return False

def persistence_worker():
    """[BUG-1] Desempacota (priority, seq, item) — nunca mais TypeError."""
    global _queue_alert_last
    while True:
        beat('persistence_worker')
        try:
            # [BUG-1] agora são 3 elementos
            priority, seq, task = urgent_queue.get(timeout=5)
            kind = task.get('kind')
            if kind == 'trade':          _db_save_trade(task['data']); _backup_trade_to_file(task['data'])
            elif kind == 'arbi':         _db_save_arbi_trade(task['data'])
            elif kind == 'audit':        _db_insert_audit(task['data'])
            elif kind == 'order':        _db_save_order(task['data'])
            elif kind == 'snapshot':     _db_save_snapshot(task['data'])
            elif kind == 'cooldown':     _db_save_cooldown(task['symbol'], task['ts'])
            elif kind == 'signal_event':       _db_save_signal_event(task['data'])
            elif kind == 'signal_attribution': _db_update_signal_attribution(task['data'])
            elif kind == 'signal_outcome':     _db_update_signal_outcome(task['data'])
            elif kind == 'pattern_stats':      _db_upsert_pattern_stats(task['data'])
            elif kind == 'factor_stats':       _db_upsert_factor_stats(task['data'])
            elif kind == 'shadow_decision':    _db_save_shadow_decision(task['data'])
            elif kind == 'ledger_event':       _db_save_ledger_event(task['data'])
            elif kind == 'brain_lesson':       _brain_persist_lesson(task['data'])
            elif kind == 'brain_decision':     _brain_persist_decision(task['data'])
            elif kind == 'brain_pattern':      _brain_persist_pattern(task['data'])
            elif kind == 'brain_regime':       _brain_persist_regime(task['data'])
            urgent_queue.task_done()

            # [V9-2] Monitorar crescimento da fila após cada processamento
            qsize = urgent_queue.qsize()
            now   = time.time()
            if qsize >= URGENT_QUEUE_CRIT and now - _queue_alert_last > 300:
                _queue_alert_last = now
                log.critical(f'[V9-2] urgent_queue CRÍTICA: {qsize} itens — DB pode estar lento/travado')
                send_whatsapp(f'CRÍTICO: fila de persistência com {qsize} itens. Verificar banco de dados.')
            elif qsize >= URGENT_QUEUE_WARN:
                log.warning(f'[V9-2] urgent_queue alta: {qsize} itens')

        except queue.Empty:
            pass
        except Exception as e:
            log.error(f'persistence_worker: {e}')

def alert_worker():
    while True:
        beat('alert_worker')
        try:
            task = alert_queue.get(timeout=5)
            if task.get('kind') == 'whatsapp':
                _send_whatsapp_direct(task['message'])
            alert_queue.task_done()
        except queue.Empty:
            pass
        except Exception as e:
            log.error(f'alert_worker: {e}')

# ═══════════════════════════════════════════════════════════════
# AUTH MIDDLEWARE
# ═══════════════════════════════════════════════════════════════
@app.before_request
def auth_check():
    if request.method == 'OPTIONS':
        return None
    if not API_SECRET_KEY:
        return None
    if request.path in PUBLIC_ROUTES or request.path.startswith('/health') or request.path.startswith('/strategies') or request.path.startswith('/brain') or request.path.startswith('/long-horizon') or request.path.startswith('/signals') or request.path.startswith('/stats') or request.path.startswith('/trades') or request.path.startswith('/arbitrage') or request.path.startswith('/prices') or request.path.startswith('/performance') or request.path.startswith('/reports') or request.path.startswith('/static/') or request.path in ('/derivatives', '/api/info', '/api/modules-debug', '/api/ticker-tape', '/api/fx-rates', '/ticker-tape.js'):
        return None
    key = request.headers.get('X-API-Key', '').strip()
    if key != API_SECRET_KEY:
        log.warning(f'Unauthorized: {request.remote_addr} {request.path}')
        return jsonify({'error': 'Unauthorized — X-API-Key required'}), 401

def require_auth(f):
    """[FIX-1] Decorador de documentação — autenticação real feita pelo before_request.
    Mantido para clareza semântica: marcar explicitamente rotas que exigem auth."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        return f(*args, **kwargs)
    return decorated

# ═══════════════════════════════════════════════════════════════
# AUDIT
# ═══════════════════════════════════════════════════════════════
def audit(event, data):
    entry = {'timestamp': datetime.utcnow().isoformat(), 'event': event, **data}
    with audit_lock:
        audit_log.append(entry)
        if len(audit_log) > 1000: audit_log.pop(0)
    log.info(f'[AUDIT] {event} | {data}')
    enqueue_persist('audit', entry)

def _db_insert_audit(entry):
    conn = get_db()
    if not conn: return
    cursor=None
    try:
        cursor    = conn.cursor()
        event     = entry.get('event', '')
        entity_id = str(entry.get('id') or entry.get('pair') or entry.get('symbol', ''))
        payload   = json.dumps({k:v for k,v in entry.items() if k not in ('event','timestamp')})
        cursor.execute(
            "INSERT INTO audit_events (event_type, entity_type, entity_id, payload_json) "
            "VALUES (%s, %s, %s, %s)",
            (event, event.split('_')[0].lower(), entity_id, payload))
        conn.commit()
    except Exception as e:
        log.error(f'_db_insert_audit: {e}')
    finally:
        try:
            if cursor: cursor.close()
        except: pass
        try:
            if conn: conn.close()
        except: pass

# ═══════════════════════════════════════════════════════════════
# CALENDÁRIO DE FERIADOS 2025-2027
# ═══════════════════════════════════════════════════════════════
# [v10.28] Holidays from market_calendar module — reassign with underscores for backward compatibility
if _PURE_MODULES_LOADED:
    _NYSE_HOLIDAYS = NYSE_HOLIDAYS
    _B3_HOLIDAYS = B3_HOLIDAYS
    _LSE_HOLIDAYS = LSE_HOLIDAYS
    _HKEX_HOLIDAYS = HKEX_HOLIDAYS
else:
    _NYSE_HOLIDAYS = {
        date(2025,1,1), date(2025,1,20), date(2025,2,17), date(2025,4,18),
        date(2025,5,26), date(2025,6,19), date(2025,7,4), date(2025,9,1),
        date(2025,11,27), date(2025,12,25),
        date(2026,1,1), date(2026,1,19), date(2026,2,16), date(2026,4,3),
        date(2026,5,25), date(2026,6,19), date(2026,7,3), date(2026,9,7),
        date(2026,11,26), date(2026,12,25),
        date(2027,1,1), date(2027,1,18), date(2027,2,15), date(2027,3,26),
        date(2027,5,31), date(2027,6,18), date(2027,7,5), date(2027,9,6),
        date(2027,11,25), date(2027,12,24),
    }
    _B3_HOLIDAYS = {
        date(2025,1,1), date(2025,3,3), date(2025,3,4), date(2025,4,18),
        date(2025,4,21), date(2025,5,1), date(2025,6,19), date(2025,9,7),
        date(2025,10,12), date(2025,11,2), date(2025,11,15), date(2025,11,20), date(2025,12,25),
        date(2026,1,1), date(2026,2,16), date(2026,2,17), date(2026,4,3),
        date(2026,4,21), date(2026,5,1), date(2026,6,4), date(2026,9,7),
        date(2026,10,12), date(2026,11,2), date(2026,11,15), date(2026,11,20), date(2026,12,25),
        date(2027,1,1), date(2027,2,8), date(2027,2,9), date(2027,3,26),
        date(2027,4,21), date(2027,5,1), date(2027,5,27), date(2027,9,7),
        date(2027,10,12), date(2027,11,2), date(2027,11,15), date(2027,11,20), date(2027,12,25),
    }
    _LSE_HOLIDAYS = {
        date(2025,1,1), date(2025,4,18), date(2025,4,21), date(2025,5,5),
        date(2025,5,26), date(2025,8,25), date(2025,12,25), date(2025,12,26),
        date(2026,1,1), date(2026,4,3), date(2026,4,6), date(2026,5,4),
        date(2026,5,25), date(2026,8,31), date(2026,12,25), date(2026,12,28),
        date(2027,1,1), date(2027,3,26), date(2027,3,29), date(2027,5,3),
        date(2027,5,31), date(2027,8,30), date(2027,12,27), date(2027,12,28),
    }
    _HKEX_HOLIDAYS = {
        date(2025,1,1), date(2025,1,29), date(2025,1,30), date(2025,1,31),
        date(2025,4,4), date(2025,4,18), date(2025,4,21), date(2025,5,1),
        date(2025,5,5), date(2025,6,2), date(2025,7,1), date(2025,10,1),
        date(2025,10,2), date(2025,10,7), date(2025,12,25), date(2025,12,26),
        date(2026,1,1), date(2026,2,17), date(2026,2,18), date(2026,2,19),
        date(2026,4,3), date(2026,4,6), date(2026,4,7), date(2026,5,1),
        date(2026,5,25), date(2026,6,19), date(2026,7,1), date(2026,10,1),
        date(2026,10,2), date(2026,12,25),
    }

# [v10.28] Timezones from market_calendar module — reassign for backward compatibility
# Module imports them as _TZ_SP, _TZ_NY, _TZ_LN, _TZ_HK to avoid conflicts
if _PURE_MODULES_LOADED:
    TZ_SAO_PAULO = _TZ_SP
    TZ_NEW_YORK  = _TZ_NY
    TZ_LONDON    = _TZ_LN
    TZ_HK        = _TZ_HK
else:
    TZ_SAO_PAULO = ZoneInfo('America/Sao_Paulo')
    TZ_NEW_YORK  = ZoneInfo('America/New_York')
    TZ_LONDON    = ZoneInfo('Europe/London')
    TZ_HK        = ZoneInfo('Asia/Hong_Kong')

# [v10.28] Market open functions imported from market_calendar module
# The module provides: is_b3_open, is_nyse_open, is_lse_open, is_hkex_open,
# is_tsx_open, is_euronext_open, market_open_for
# These are imported at the top if _PURE_MODULES_LOADED=True
# If modules failed to load, define fallback implementations:
if not _PURE_MODULES_LOADED:
    def is_b3_open():
        now = datetime.now(TZ_SAO_PAULO)
        if now.weekday()>=5 or now.date() in _B3_HOLIDAYS: return False
        h = now.hour + now.minute/60.0; return 10.0<=h<17.0

    def is_nyse_open():
        now = datetime.now(TZ_NEW_YORK)
        if now.weekday()>=5 or now.date() in _NYSE_HOLIDAYS: return False
        h = now.hour + now.minute/60.0; return 9.5<=h<16.0

    def is_lse_open():
        now = datetime.now(TZ_LONDON)
        if now.weekday()>=5 or now.date() in _LSE_HOLIDAYS: return False
        h = now.hour + now.minute/60.0; return 8.0<=h<16.5

    def is_hkex_open():
        now = datetime.now(TZ_HK)
        if now.weekday()>=5 or now.date() in _HKEX_HOLIDAYS: return False
        h = now.hour + now.minute/60.0; return (9.5<=h<12.0) or (13.0<=h<16.0)

    def is_tsx_open():
        # TSX (Toronto) abre 09:30-16:00 ET = mesmo horário da NYSE
        now = datetime.now(TZ_NEW_YORK)
        if now.weekday()>=5: return False
        h = now.hour + now.minute/60.0; return 9.5<=h<16.0

    def is_euronext_open():
        # Euronext/XETRA 09:00-17:30 CET = 08:00-16:30 UTC (aprox)
        try:
            from zoneinfo import ZoneInfo as _ZI
            TZ_CET = _ZI('Europe/Paris')
        except: TZ_CET = TZ_LONDON
        now = datetime.now(TZ_CET)
        if now.weekday()>=5: return False
        h = now.hour + now.minute/60.0; return 9.0<=h<17.5

    def market_open_for(mkt):
        if mkt=='CRYPTO':                        return True
        if mkt=='B3':                            return is_b3_open()
        if mkt in ('NYSE','NASDAQ','US'):        return is_nyse_open()
        if mkt=='LSE':                           return is_lse_open()
        if mkt=='HKEX':                          return is_hkex_open()
        if mkt=='TSX':                           return is_tsx_open()
        if mkt in ('EURONEXT','XETRA','XAMS'):  return is_euronext_open()
        return False

# ═══════════════════════════════════════════════════════════════
# RISK ENGINE
# ═══════════════════════════════════════════════════════════════
def check_risk(symbol, market_type, position_value, strategy='stocks'):
    global RISK_KILL_SWITCH
    # [v10.14] Auto-reset se kill foi de outro dia e drawdown atual está ok
    if RISK_KILL_SWITCH:
        _auto_reset_kill_switch_if_safe()
    if RISK_KILL_SWITCH: return False, 'KILL_SWITCH_ACTIVE', 0

    with state_lock:
        all_open = stocks_open + crypto_open
        s_open   = list(stocks_open); c_open = list(crypto_open)
        s_closed = list(stocks_closed); c_closed = list(crypto_closed)
        sc = stocks_capital; cc = crypto_capital

    if len(all_open) >= MAX_OPEN_POSITIONS:
        return False, f'MAX_OPEN_POSITIONS ({len(all_open)}/{MAX_OPEN_POSITIONS})', 0

    # [v10.14] Bloqueio DIA INTEIRO após TRAILING_STOP — re-entradas devolvem 36.6% dos ganhos
    _ts_cooldown_ts = _trailing_stop_cooldown.get(symbol, 0)
    if _ts_cooldown_ts:
        from datetime import timezone
        _ts_date = datetime.utcfromtimestamp(_ts_cooldown_ts).date()
        _today   = datetime.utcnow().date()
        if _ts_date == _today:  # mesmo dia UTC → bloquear
            return False, f'TRAILING_STOP_DAY_BLOCK (trailing stop hoje, aguardar amanhã)', 0

    if strategy == 'stocks':
        sc_count = sum(1 for t in s_open if t.get('asset_type')=='stock')
        if sc_count >= MAX_POSITIONS_STOCKS:
            return False, f'MAX_POSITIONS_STOCKS ({sc_count}/{MAX_POSITIONS_STOCKS})', 0
        committed = sum(t.get('position_value',0) for t in s_open)
        # [v10.14] Capital check: usar portfolio TOTAL (livre + comprometido) = inclui ganhos
        # stocks_capital = capital livre | committed = valor das posições abertas
        # portfolio_total = stocks_capital + committed (reflete ganhos acumulados)
        _stocks_portfolio_total = stocks_capital + committed
        _effective_cap_s = max(_stocks_portfolio_total, INITIAL_CAPITAL_STOCKS)
        if committed+position_value > _effective_cap_s*MAX_CAPITAL_PCT_STOCKS/100:
            return False, 'STOCKS_CAPITAL_LIMIT', 0
        free_cap = sc; max_pos = MAX_POSITION_STOCKS
    elif strategy == 'crypto':
        cc_count = sum(1 for t in c_open if t.get('asset_type')=='crypto')
        if cc_count >= MAX_POSITIONS_CRYPTO:
            return False, f'MAX_POSITIONS_CRYPTO ({cc_count}/{MAX_POSITIONS_CRYPTO})', 0
        committed = sum(t.get('position_value',0) for t in c_open)
        # [v10.14] Crypto capital check: portfolio total inclui ganhos
        _crypto_portfolio_total = crypto_capital + committed
        _effective_cap_c = max(_crypto_portfolio_total, INITIAL_CAPITAL_CRYPTO)
        if committed+position_value > _effective_cap_c*MAX_CAPITAL_PCT_CRYPTO/100:
            return False, 'CRYPTO_CAPITAL_LIMIT', 0
        free_cap = cc; max_pos = CRYPTO_MAX_POSITION_BY_SYM.get(symbol, MAX_POSITION_CRYPTO)
    else:
        free_cap = sc; max_pos = MAX_POSITION_STOCKS

    if sum(1 for t in all_open if t.get('symbol')==symbol) >= MAX_SAME_SYMBOL:
        return False, f'SYMBOL_ALREADY_OPEN ({symbol})', 0
    if symbol in symbol_blocked:
        return False, f'SYMBOL_BLOCKED (manual)', 0
    if time.time()-symbol_cooldown.get(symbol,0) < SYMBOL_COOLDOWN_SEC:
        secs = int(SYMBOL_COOLDOWN_SEC-(time.time()-symbol_cooldown.get(symbol,0)))
        return False, f'SYMBOL_COOLDOWN (+{secs}s)', 0
    if sum(1 for t in all_open if t.get('market')==market_type) >= MAX_POSITION_SAME_MKT:
        return False, f'MAX_POSITION_SAME_MKT ({market_type})', 0
    if position_value > free_cap:
        return False, f'INSUFFICIENT_CAPITAL (free=${free_cap:.0f})', 0

    total_cap = INITIAL_CAPITAL_STOCKS+INITIAL_CAPITAL_CRYPTO
    max_risk  = total_cap*MAX_RISK_PER_TRADE_PCT/100
    approved  = min(position_value, max_risk, max_pos, free_cap)

    # [v10.9-Fix] Drawdown calculado por PnL LÍQUIDO do período (não perdas brutas).
    # Perdas brutas penalizam sistemas ativos indevidamente — um sistema com 500 trades/semana
    # acumula perdas brutas enormes mesmo sendo lucrativo.
    # PnL líquido = realidade econômica real do portfólio.
    cutoff_d = (datetime.utcnow()-timedelta(days=1)).isoformat()
    daily_net = sum(t.get('pnl',0) for t in s_closed+c_closed
        if t.get('closed_at','')>=cutoff_d)
    dd_d = abs(min(daily_net, 0))/total_cap*100   # só conta se negativo
    if dd_d >= MAX_DAILY_DRAWDOWN_PCT:
        _trigger_kill_switch(dd_d,'daily'); return False,f'DAILY_DRAWDOWN ({dd_d:.2f}%)',0

    cutoff_w = (datetime.utcnow()-timedelta(days=7)).isoformat()
    weekly_net = sum(t.get('pnl',0) for t in s_closed+c_closed
        if t.get('closed_at','')>=cutoff_w)
    dd_w = abs(min(weekly_net, 0))/total_cap*100   # só conta se negativo
    if dd_w >= MAX_WEEKLY_DRAWDOWN_PCT:
        _trigger_kill_switch(dd_w,'weekly'); return False,f'WEEKLY_DRAWDOWN ({dd_w:.2f}%)',0

    return True, 'OK', round(approved, 2)

def check_risk_arbi(pair_id, position_value):
    global ARBI_KILL_SWITCH
    if ARBI_KILL_SWITCH: return False, 'ARBI_KILL_SWITCH', 0
    with state_lock:
        open_count=len(arbi_open); cap=arbi_capital; a_closed=list(arbi_closed)
    if open_count >= ARBI_MAX_POSITIONS:
        return False, f'ARBI_MAX_POSITIONS ({open_count}/{ARBI_MAX_POSITIONS})', 0
    if any(t.get('pair_id')==pair_id for t in arbi_open):
        return False, f'ARBI_PAIR_OPEN ({pair_id})', 0
    if position_value > cap:
        return False, f'ARBI_INSUFFICIENT_CAPITAL ({cap:.0f})', 0
    cutoff=(datetime.utcnow()-timedelta(days=1)).isoformat()
    daily_loss=sum(t.get('pnl',0) for t in a_closed
        if t.get('closed_at','')>=cutoff and t.get('pnl',0)<0)
    # [FIX-28] Usar capital total real (cap + em trades) como base do drawdown
    # ARBI_CAPITAL constante pode ser a env var antiga (500K); usar variável real
    _arbi_cap_total = max(cap + sum(t.get('position_size',0) for t in arbi_open), ARBI_CAPITAL, 1)
    dd=abs(daily_loss)/_arbi_cap_total*100
    if dd>=ARBI_MAX_DAILY_LOSS:
        if not ARBI_KILL_SWITCH:
            ARBI_KILL_SWITCH=True; send_whatsapp(f'ARBI KILL SWITCH: drawdown {dd:.2f}% (base ${_arbi_cap_total:,.0f})')
        return False, f'ARBI_DAILY_DRAWDOWN ({dd:.2f}%)', 0
    # [v10.14] Auto-reset: drawdown voltou abaixo do threshold
    if ARBI_KILL_SWITCH and dd < ARBI_MAX_DAILY_LOSS * 0.7:
        ARBI_KILL_SWITCH = False
        log.info(f'[ARBI] Kill switch auto-resetado: drawdown={dd:.2f}% < {ARBI_MAX_DAILY_LOSS*0.7:.2f}%')
    return True, 'OK', round(min(position_value, ARBI_POS_SIZE, cap), 2)

def _trigger_kill_switch(dd_pct, period):
    global RISK_KILL_SWITCH
    RISK_KILL_SWITCH = True
    audit('KILL_SWITCH_ACTIVATED',{'drawdown_pct':round(dd_pct,2),'period':period})

def _auto_reset_kill_switch_if_safe():
    """[v10.14] Auto-reset do kill_switch no início de um novo dia se drawdown < limite."""
    global RISK_KILL_SWITCH
    if not RISK_KILL_SWITCH: return
    try:
        now = datetime.utcnow()
        # Só resetar entre 13:00-13:30 UTC (abertura NYSE) ou 13:00 UTC (qualquer mercado)
        # Resetar se o drawdown diário zerou (novo dia)
        with state_lock:
            all_closed = list(stocks_closed) + list(crypto_closed)
        today = now.date()
        daily_loss = sum(t.get('pnl',0) for t in all_closed
                         if t.get('closed_at','')[:10] == str(today) and t.get('pnl',0) < 0)
        total_cap = INITIAL_CAPITAL_STOCKS + INITIAL_CAPITAL_CRYPTO
        dd_today = abs(daily_loss) / total_cap * 100 if total_cap > 0 else 0
        # Se drawdown de HOJE está ok → reset automático (kill foi do dia anterior)
        if dd_today < MAX_DAILY_DRAWDOWN_PCT * 0.5:  # margem de 50% do limite
            RISK_KILL_SWITCH = False
            log.info(f'[KS] Auto-reset: drawdown hoje={dd_today:.2f}% < limite/2={MAX_DAILY_DRAWDOWN_PCT*0.5:.1f}%')
            audit('KILL_SWITCH_AUTO_RESET', {'dd_today': round(dd_today,2), 'threshold': MAX_DAILY_DRAWDOWN_PCT})
    except Exception as e:
        log.warning(f'auto_reset_ks: {e}')
    send_whatsapp(f'KILL SWITCH ATIVADO — drawdown {period}: {dd_pct:.2f}%')

def _second_validation(symbol, market_type, strategy):
    """Segunda validação leve DENTRO do state_lock"""
    global RISK_KILL_SWITCH
    if RISK_KILL_SWITCH: return False, 'KILL_SWITCH'
    all_open = stocks_open+crypto_open
    if len(all_open) >= MAX_OPEN_POSITIONS: return False,'MAX_OPEN_POSITIONS'
    if any(t.get('symbol')==symbol for t in all_open): return False,'SYMBOL_DUPLICATE'
    if time.time()-symbol_cooldown.get(symbol,0)<SYMBOL_COOLDOWN_SEC: return False,'COOLDOWN'
    if sum(1 for t in all_open if t.get('market')==market_type)>=MAX_POSITION_SAME_MKT:
        return False,'MAX_SAME_MKT'
    if strategy=='stocks':
        if len(stocks_open)>=MAX_POSITIONS_STOCKS: return False,'MAX_POSITIONS_STOCKS'
        if stocks_capital<=0: return False,'NO_CAPITAL_STOCKS'
    elif strategy=='crypto':
        if len(crypto_open)>=MAX_POSITIONS_CRYPTO: return False,'MAX_POSITIONS_CRYPTO'
        if crypto_capital<=0: return False,'NO_CAPITAL_CRYPTO'
    return True,'OK'

def alert_signal(signal):
    key=signal.get('symbol',''); now=time.time()
    if now-alerted_signals.get(key,0)<3600: return
    alerted_signals[key]=now
    send_whatsapp(f"Egreja AI | {key} ({signal.get('market_type','')}) Score:{signal.get('score',0)}/100 {signal.get('signal','')} ${signal.get('price',0):,.2f}")

def alert_trade_closed(trade):
    key=trade.get('id','')
    if key in alerted_trades: return
    alerted_trades[key]=True
    pnl=trade.get('pnl',0); result='OK' if pnl>=0 else 'LOSS'
    send_whatsapp(f"Trade {result} | {trade.get('symbol','')} | {trade.get('close_reason','')} | {'+'if pnl>=0 else ''}{pnl:,.2f} ({trade.get('pnl_pct',0):+.2f}%)")

    # [v3.0] Brain learns from every closed trade
    try:
        sym = trade.get('symbol', '')
        pnl_pct = trade.get('pnl_pct', 0)
        reason = trade.get('close_reason', '')
        signal = trade.get('signal_type', '')
        asset_type = trade.get('asset_type', 'stock')
        module = 'Crypto' if asset_type == 'crypto' else 'Stocks'

        if pnl >= 0:
            desc = f'{sym}: trade POSITIVO +{pnl_pct:.1f}% (sinal {signal}, saida {reason}) — padrao validado'
            impact = min(6.0 + pnl_pct * 0.3, 9.5)
        else:
            desc = f'{sym}: trade NEGATIVO {pnl_pct:.1f}% (sinal {signal}, saida {reason}) — revisar parametros'
            impact = min(6.0 + abs(pnl_pct) * 0.4, 9.5)

        enqueue_brain_lesson(
            module=module,
            description=desc,
            lesson_type='Trade_Outcome',
            impact_score=round(impact, 1),
            confidence=min(70 + abs(pnl_pct) * 2, 95),
            strategy=signal,
            data_json={'symbol': sym, 'pnl': pnl, 'pnl_pct': pnl_pct,
                       'close_reason': reason, 'signal': signal, 'trade_id': key}
        )
    except Exception as _be:
        log.warning(f'[Brain] Trade learning hook error: {_be}')

# ═══════════════════════════════════════════════════════════════
# ORDERS
# ═══════════════════════════════════════════════════════════════
def create_order(trade_id, symbol, side, order_type, qty, price, strategy='stocks', notes='',
                 order_id_override=None):
    """[V91-1] Aceita order_id_override para que trade e ordem compartilhem ID pré-gerado."""
    order = {
        'order_id':   order_id_override or gen_id('ORD'), 'trade_id': trade_id,
        'symbol':     symbol, 'side': side, 'order_type': order_type,
        'qty': qty, 'limit_price': price, 'stop_price': None, 'strategy': strategy,
        'status': 'NEW', 'status_history': [{'status':'NEW','ts':datetime.utcnow().isoformat()}],
        'sent_at': None, 'filled_at': None, 'fill_price': None,
        'fill_qty': 0, 'slippage': 0, 'fee': 0, 'notes': notes,
        'created_at': datetime.utcnow().isoformat(), 'updated_at': datetime.utcnow().isoformat(),
    }
    with orders_lock:
        orders_log.append(order)
        if len(orders_log) > 2000: orders_log.pop(0)
    enqueue_persist('order', order)
    return order

def update_order_status(order, new_status, fill_price=None, fill_qty=None):
    """[V91-4] Protegido por orders_lock — evita leitura de estado intermediário."""
    with orders_lock:
        order['status'] = new_status; order['updated_at'] = datetime.utcnow().isoformat()
        order['status_history'].append({'status':new_status,'ts':order['updated_at']})
        if new_status=='SENT': order['sent_at']=order['updated_at']
        if new_status in ('FILLED','PARTIALLY_FILLED') and fill_price:
            order['fill_price']=fill_price; order['fill_qty']=fill_qty or order['qty']
            order['filled_at']=order['updated_at']
            order['slippage']=round(abs(fill_price-order['limit_price'])/order['limit_price']*100,4)
    enqueue_persist('order', order)
    return order

def _db_save_order(order):
    conn=get_db()
    if not conn: return
    cursor=None
    try:
        cursor=conn.cursor()
        # [V91-2] status_history_json persiste a trilha completa da máquina de estados
        status_history_json = json.dumps(order.get('status_history', []))
        cursor.execute("""INSERT INTO orders (
            order_id,trade_id,symbol,side,order_type,qty,limit_price,stop_price,
            strategy,status,fill_price,fill_qty,slippage,fee,notes,
            sent_at,filled_at,created_at,updated_at,status_history_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE status=VALUES(status),fill_price=VALUES(fill_price),
            fill_qty=VALUES(fill_qty),slippage=VALUES(slippage),
            filled_at=VALUES(filled_at),updated_at=VALUES(updated_at),
            status_history_json=VALUES(status_history_json)""",
            (order.get('order_id'),order.get('trade_id'),order.get('symbol'),
             order.get('side'),order.get('order_type'),order.get('qty'),
             order.get('limit_price'),order.get('stop_price'),order.get('strategy'),
             order.get('status'),order.get('fill_price'),order.get('fill_qty'),
             order.get('slippage',0),order.get('fee',0),order.get('notes',''),
             order.get('sent_at'),order.get('filled_at'),
             order.get('created_at'),order.get('updated_at'),
             status_history_json))
        conn.commit()
    except Exception as e: log.error(f'db_save_order: {e}')
    finally:
        try:
            if cursor: cursor.close()
        except: pass
        try:
            if conn: conn.close()
        except: pass

# ═══════════════════════════════════════════════════════════════
# PORTFOLIO SNAPSHOT
# ═══════════════════════════════════════════════════════════════
def take_portfolio_snapshot():
    with state_lock:
        _now_utc = datetime.utcnow()
        snap = {
            'timestamp':        _now_utc.strftime('%Y-%m-%d %H:%M:%S'),
            'ts_epoch':         int(_now_utc.timestamp()),
            'stocks_capital':   round(stocks_capital,2),
            'crypto_capital':   round(crypto_capital,2),
            'arbi_capital':     round(arbi_capital,2),
            'stocks_open_pnl':  round(sum(t.get('pnl',0) for t in stocks_open),2),
            'crypto_open_pnl':  round(sum(t.get('pnl',0) for t in crypto_open),2),
            'arbi_open_pnl':    round(sum(t.get('pnl',0) for t in arbi_open),2),
            'total_open_pnl':   round(sum(t.get('pnl',0) for t in stocks_open+crypto_open+arbi_open),2),
            'open_positions':   len(stocks_open)+len(crypto_open),
            'arbi_positions':   len(arbi_open),
            'kill_switch':      int(RISK_KILL_SWITCH),
            'arbi_kill_switch': int(ARBI_KILL_SWITCH),
            'market_regime':    market_regime.get('mode','UNKNOWN'),
        }
    enqueue_persist('snapshot', snap)

def _db_save_snapshot(snap):
    conn=get_db()
    if not conn: return
    cursor=None
    try:
        cursor=conn.cursor()
        # [FIX 17abr] ts é BIGINT (unix epoch), snapshot_at é TIMESTAMP.
        # Antes estava gravando string formatada em 'ts' → truncated desde 12/mar.
        cursor.execute("""INSERT INTO portfolio_snapshots (
            ts,snapshot_at,stocks_capital,crypto_capital,arbi_capital,
            stocks_open_pnl,crypto_open_pnl,arbi_open_pnl,total_open_pnl,
            open_positions,arbi_positions,kill_switch,arbi_kill_switch,market_regime)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (snap.get('ts_epoch') or int(time.time()), snap['timestamp'],
             snap['stocks_capital'],snap['crypto_capital'],snap['arbi_capital'],
             snap['stocks_open_pnl'],snap['crypto_open_pnl'],snap['arbi_open_pnl'],
             snap['total_open_pnl'],snap['open_positions'],snap['arbi_positions'],
             snap['kill_switch'],snap['arbi_kill_switch'],snap['market_regime']))
        conn.commit()
    except Exception as e: log.error(f'db_save_snapshot: {e}')
    finally:
        try:
            if cursor: cursor.close()
        except: pass
        try:
            if conn: conn.close()
        except: pass

def snapshot_loop():
    while True:
        beat('snapshot_loop')
        time.sleep(300)
        beat('snapshot_loop')
        try:
            take_portfolio_snapshot()
        except Exception as e: log.error(f'snapshot_loop: {e}')

# ═══════════════════════════════════════════════════════════════
# QUALIDADE DO DADO
# ═══════════════════════════════════════════════════════════════
def record_data_quality(symbol, source, latency_ms, price_valid):
    score = 100
    if not price_valid:     score -= 50
    if latency_ms > 30_000: score -= 20
    if latency_ms > 60_000: score -= 30
    with dq_lock:
        data_quality[symbol] = {
            'symbol': symbol, 'source': source,
            'fetch_at': datetime.utcnow().isoformat(),
            'latency_ms': round(latency_ms,1),
            'quality': max(0,score), 'stale': latency_ms>60_000,
            'price_valid': price_valid,
        }

# ═══════════════════════════════════════════════════════════════════
# [v10.13] TEMPORAL & CROSS-MARKET LEARNING ENGINE
# Aprende padrões de hora×dia, correlações stocks→crypto e FX
# ═══════════════════════════════════════════════════════════════════

# Scores temporais de STOCKS por hora UTC (derivados de 534 trades reais)
STOCKS_HOUR_SCORE = {
    13: +6,   # 13h UTC = 10h BRT: abertura B3, WR 60.6%, +$292/trade
    14: +2,   # 14h UTC = 11h BRT: WR 54.3%
    15: -5,   # 15h UTC = 12h BRT: WR 45.9%, instável pós-abertura
    16: +6,   # 16h UTC = 13h BRT: WR 64.7%, NYSE estável
    17: +2,   # 17h UTC = 14h BRT: WR 54.1%
    18: +6,   # 18h UTC = 15h BRT: WR 59.4%, B3 69% WR
    19: -5,   # 19h UTC = 16h BRT: WR 41.4%, fechamento B3 caótico
}

# Score por dia da semana para STOCKS (0=Seg, 6=Dom)
STOCKS_DOW_SCORE = {
    0: 0,    # Segunda: 50.5% WR — neutro
    1: +2,   # Terça:   51.6% WR — levemente positivo
    2: +12,  # Quarta:  60.5% WR — MELHOR DIA, +$32.5K total
    3: -10,  # Quinta:  44.1% WR — PIOR DIA, SL 27%, -$14.4K
    4: +4,   # Sexta:   54.7% WR — ligeiramente positivo
    5: 0,    # Sábado:  mercado fechado
    6: 0,    # Domingo: mercado fechado
}

# Penalidade específica B3 na quinta (volatilidade de anúncios/opções)
STOCKS_B3_QUINTA_PENALTY = -15  # B3 quinta: WR 34.8%, média -$490/trade

# Janelas prioritárias stocks (WR>=70%, n>=5) — boost adicional
STOCKS_PRIORITY_WINDOWS = {
    (1, 13): +15,  # Terça 13h: WR 69%, n=36
    (2, 13): +15,  # Quarta 13h: WR 72%, n=50
    (2, 18): +12,  # Quarta 18h: WR 78%, n=23
    (0, 18): +12,  # Segunda 18h: WR 70%, n=20
    (2, 17): +10,  # Quarta 17h: WR 71%, n=21
    (4, 16): +10,  # Sexta 16h: WR 67%, n=21
}

# Janelas a bloquear em stocks (WR<35%, n>=5)
STOCKS_BLOCKED_WINDOWS = {
    (3, 17): 'Qui17h_WR11pct',   # PIOR: WR 11%, n=9, média -$645
    (1, 18): 'Ter18h_WR33pct',   # WR 33%, n=18
    (0, 19): 'Seg19h_WR33pct',   # WR 33%, n=18
    (2, 19): 'Qua19h_WR25pct',   # WR 25%, n=16
}

# Scores temporais de crypto por hora UTC# Scores temporais de crypto por hora UTC (derivados de 1.183 trades reais)
# Positivo = favorável, Negativo = desfavorável
CRYPTO_HOUR_SCORE = {
    0: 0, 1: -8, 2: +6, 3: -8, 4: +6, 5: +12, 6: +12, 7: +12,
    8: 0, 9: -15, 10: 0, 11: +6, 12: +6, 13: 0, 14: -8, 15: -15,
    16: -8, 17: -8, 18: -15, 19: -15, 20: -15, 21: -15, 22: -8, 23: 0
}

# [v10.29] Score por dia da semana para crypto (0=Seg, 6=Dom)
# Dados Manus confirmam: Quarta 0% WR, Sexta 8.7% WR, Domingo 100% WR
CRYPTO_DOW_SCORE = {
    0: +8,   # Segunda: 53.7% WR Egreja | Manus: gap opportunities → reforçado
    1: -10,  # Terça:   43.9% WR — pior dia Egreja, -$37.9K total
    2: -25,  # [v10.29] Quarta: 40.3% WR Egreja | Manus: 0% WR → hard penalty (era -8)
    3: +2,   # Quinta:  48.7% WR — neutro-positivo
    4: -20,  # [v10.29] Sexta: 42.0% WR Egreja | Manus: 8.7% WR → heavy penalty (era -8)
    5: -6,   # Sábado:  43.3% WR
    6: +20,  # [v10.29] Domingo: 64.7% WR Egreja | Manus: 100% WR → max boost (era +15)
}

# [v10.15] CRYPTO_BLOCKED_WINDOWS agora é DINÂMICO — calculado a partir do factor_stats real.
# A tabela estática foi removida. A função _is_crypto_window_blocked() consulta
# factor_stats em tempo real e só bloqueia se houver evidência forte (WR<25%, n>=15).
CRYPTO_BLOCKED_WINDOWS = {}  # vazio — substituído por _is_crypto_window_blocked()

def _is_crypto_window_blocked(weekday: int, hour_utc: int) -> tuple:
    """[v10.15] Bloqueio dinâmico baseado em factor_stats real.
    Só bloqueia se weekday E hour_utc AMBOS indicarem performance ruim.
    Retorna (blocked: bool, reason: str).
    """
    DOW_NAMES = {0:'Seg',1:'Ter',2:'Qua',3:'Qui',4:'Sex',5:'Sab',6:'Dom'}
    with learning_lock:
        # Checar weekday factor
        wd_fs = factor_stats_cache.get(('weekday', str(weekday)), {})
        wd_n = wd_fs.get('total_samples', 0)
        wd_exp = wd_fs.get('expectancy', 0)
        wd_wr = (wd_fs.get('wins', 0) / wd_n * 100) if wd_n > 0 else 50
        # Checar time_bucket factor (mapear hour → bucket)
        if hour_utc < 6:    tb = 'OVERNIGHT'
        elif hour_utc < 10: tb = 'PRE_MARKET'
        elif hour_utc < 12: tb = 'MORNING'
        elif hour_utc < 14: tb = 'MIDDAY'
        elif hour_utc < 17: tb = 'AFTERNOON'
        elif hour_utc < 20: tb = 'EVENING'
        else:               tb = 'NIGHT'
        tb_fs = factor_stats_cache.get(('time_bucket', tb), {})
        tb_n = tb_fs.get('total_samples', 0)
        tb_exp = tb_fs.get('expectancy', 0)
        tb_wr = (tb_fs.get('wins', 0) / tb_n * 100) if tb_n > 0 else 50
    # Só bloqueia se AMBOS fatores são ruins com amostras suficientes
    wd_bad = wd_n >= 15 and wd_wr < 25 and wd_exp < -0.1
    tb_bad = tb_n >= 15 and tb_wr < 25 and tb_exp < -0.1
    if wd_bad and tb_bad:
        reason = f"{DOW_NAMES.get(weekday,'?')}{hour_utc}h_WR{wd_wr:.0f}pct_n{wd_n}_DYNAMIC"
        return True, reason
    return False, ''


# Janelas prioritárias (WR>=80%, n>=5) — score boost adicional
CRYPTO_PRIORITY_WINDOWS = {
    (0, 17): +20, (3, 7): +20, (4, 8): +20, (6, 12): +20,
    (6, 0): +18,  (3, 4): +18, (3, 13): +16, (2, 11): +16,
    (1, 5): +14,  (2, 8): +14, (0, 6): +14,  (3, 7): +14,
}

# Estado cross-market em memória (atualizado pelo trading loop)
_cross_market_state = {
    'stocks_wr_today': 50.0,      # WR de stocks hoje
    'stocks_pnl_today': 0.0,      # PnL de stocks hoje
    'stocks_n_today': 0,          # Nº de trades de stocks hoje
    'stocks_wr_yesterday': 50.0,  # WR de stocks ontem
    'stocks_pnl_yesterday': 0.0,
    'stocks_n_yesterday': 0,
    'usdbrl_change_pct': 0.0,     # variação % do USD/BRL hoje
    'btc_change_24h': 0.0,        # variação % BTC 24h (proxy de sentimento crypto)
    'last_update': '',
}

def update_cross_market_state(new_vals: dict):
    """Atualiza estado cross-market para uso no score de crypto."""
    global _cross_market_state
    _cross_market_state.update(new_vals)
    _cross_market_state['last_update'] = datetime.utcnow().isoformat()

def get_temporal_stock_score(hour_utc: int, weekday: int, market: str = 'NYSE') -> tuple:
    """
    [v10.13] Retorna (score_adj, blocked, reason) para stocks.
    Baseado em análise de 534 trades reais por dia/hora/mercado.
    """
    window_key = (weekday, hour_utc)

    # Bloquear janelas ruins
    if window_key in STOCKS_BLOCKED_WINDOWS:
        return 0, True, STOCKS_BLOCKED_WINDOWS[window_key]

    hour_adj = STOCKS_HOUR_SCORE.get(hour_utc, 0)
    dow_adj  = STOCKS_DOW_SCORE.get(weekday, 0)

    # Penalidade específica B3 na quinta
    b3_quinta_adj = 0
    if weekday == 3 and market == 'B3':
        b3_quinta_adj = STOCKS_B3_QUINTA_PENALTY

    # Boost janelas prioritárias
    priority_boost = STOCKS_PRIORITY_WINDOWS.get(window_key, 0)

    total_adj = hour_adj + dow_adj + b3_quinta_adj + priority_boost
    reason = f"h{hour_utc}UTC_d{weekday}_mkt{market}_adj{total_adj:+d}"
    return total_adj, False, reason

def get_temporal_crypto_score(hour_utc: int, weekday: int) -> tuple:
    """
    [v10.13] Retorna (score_adj, blocked, reason) baseado em padrões temporais.
    score_adj: pontos a adicionar/subtrair do score base
    blocked: True se a janela deve ser completamente bloqueada
    reason: string descritiva para logging
    """
    # [v10.15] Bloqueio dinâmico baseado em factor_stats real (substituiu tabela estática)
    _dyn_blocked, _dyn_reason = _is_crypto_window_blocked(weekday, hour_utc)
    if _dyn_blocked:
        return 0, True, _dyn_reason
    
    # Score hora + dia
    hour_adj = CRYPTO_HOUR_SCORE.get(hour_utc, 0)
    dow_adj  = CRYPTO_DOW_SCORE.get(weekday, 0)
    
    # Boost para janelas prioritárias
    window_key = (weekday, hour_utc)
    priority_boost = CRYPTO_PRIORITY_WINDOWS.get(window_key, 0)
    
    total_adj = hour_adj + dow_adj + priority_boost
    reason = f"h{hour_utc}UTC_d{weekday}_adj{total_adj:+d}"
    return total_adj, False, reason

def get_cross_market_crypto_adj() -> int:
    """
    [v10.13] Ajuste de score baseado em correlação stocks→crypto.
    Quando stocks estão ruins no mesmo dia → crypto penalizado.
    Quando stocks estão neutros → crypto favorecido (menor correlação).
    """
    s = _cross_market_state
    today_wr = s.get('stocks_wr_today', 50.0)
    today_n  = s.get('stocks_n_today', 0)
    yest_wr  = s.get('stocks_wr_yesterday', 50.0)
    yest_n   = s.get('stocks_n_yesterday', 0)
    
    adj = 0
    # Correlação mesmo dia (dados reais: stocks ruins → crypto WR 35.8%)
    if today_n >= 5:
        if today_wr < 40:
            adj -= 12  # stocks muito ruins hoje → crypto penalizado
        elif today_wr < 48:
            adj -= 6   # stocks ruins → leve penalidade
        elif 48 <= today_wr <= 55:
            adj += 4   # stocks neutros → crypto levemente favorecido
        # stocks bons não ajudam crypto (dados mostram correlação inversa)
    
    # Correlação D-1: stocks ruins ontem → crypto amanhã também fraco
    if yest_n >= 5 and yest_wr < 45:
        adj -= 5
    
    # FX: USD/BRL subindo = dólar forte = cripto geralmente sobe (safe haven)
    usdbrl_chg = s.get('usdbrl_change_pct', 0.0)
    if usdbrl_chg > 1.5:   adj += 4  # dólar forte → cripto pode subir
    elif usdbrl_chg < -1.5: adj -= 3  # dólar fraco → menos fluxo para cripto
    
    # [v10.29] BTC sentimento reforçado (Manus usa BTC como proxy cross-market)
    # BTC caindo > 3% = altcoins em risco severo → penalidade forte
    btc_chg = s.get('btc_change_24h', 0.0)
    if btc_chg > 5.0:    adj += 8   # BTC em forte alta → mercado muito bullish
    elif btc_chg > 3.0:  adj += 5   # BTC em alta → mercado bullish
    elif btc_chg < -5.0: adj -= 15  # [v10.29] BTC em queda forte → altcoins em perigo severo (era -8)
    elif btc_chg < -3.0: adj -= 10  # [v10.29] BTC em queda → mercado bearish (era -8)
    elif btc_chg < -1.5: adj -= 4   # [v10.29] BTC levemente negativo → cautela

    return max(-25, min(+20, adj))  # [v10.29] cap expandido ±25/+20 pts (era ±20/+15)


# ═══════════════════════════════════════════════════════════════
# [L-1] FEATURE ENGINEERING — extração determinística e bucketing
# ═══════════════════════════════════════════════════════════════

# [v10.28] Feature engineering functions imported from feature_engine module
# These helper functions and extract_features are provided by modules.feature_engine
# If modules failed to load, define fallback implementations:
if not _PURE_MODULES_LOADED:
    def _score_bucket(score: float) -> str:
        if score <= 29:   return 'VERY_LOW'
        if score <= 49:   return 'LOW'
        if score <= 69:   return 'NEUTRAL'
        if score <= 84:   return 'HIGH'
        return 'VERY_HIGH'

    def _rsi_bucket(rsi: float) -> str:
        if rsi < 30:    return 'OVERSOLD'
        if rsi < 45:    return 'WEAK'
        if rsi < 55:    return 'NEUTRAL'
        if rsi < 70:    return 'STRONG'
        return 'OVERBOUGHT'

    def _ema_alignment(ema9: float, ema21: float, ema50: float, price: float) -> str:
        """Alinhamento das EMAs em relação ao preço e entre si."""
        if price > ema9 > ema21 > ema50:  return 'BULLISH_STACK'
        if price < ema9 < ema21 < ema50:  return 'BEARISH_STACK'
        if ema9 > ema21:                   return 'BULLISH_CROSS'
        if ema9 < ema21:                   return 'BEARISH_CROSS'
        return 'MIXED'

    def _change_pct_bucket(change_pct: float) -> str:
        a = abs(change_pct)
        if a < 0.5:   return 'FLAT'
        if a < 1.5:   return 'SMALL'
        if a < 3.0:   return 'MEDIUM'
        if a < 6.0:   return 'LARGE'
        return 'EXTREME'

    def _volatility_bucket(regime_volatility: str) -> str:
        return regime_volatility or 'NORMAL'

    def _time_bucket(dt: datetime) -> str:
        h = dt.hour
        if h < 6:    return 'OVERNIGHT'
        if h < 10:   return 'PRE_MARKET'
        if h < 12:   return 'MORNING'
        if h < 14:   return 'MIDDAY'
        if h < 17:   return 'AFTERNOON'
        if h < 20:   return 'EVENING'
        return 'NIGHT'

    def _data_quality_bucket(dq_score: float) -> str:
        if dq_score >= 90: return 'HIGH'
        if dq_score >= 60: return 'MEDIUM'
        return 'LOW'

    def _atr_bucket(atr_pct: float) -> str:
        """[v10.4] ATR como % do preço — volatility real, não só regime de crypto."""
        if atr_pct <= 0:    return 'UNKNOWN'
        if atr_pct < 0.5:   return 'VERY_LOW'
        if atr_pct < 1.5:   return 'LOW'
        if atr_pct < 3.0:   return 'NORMAL'
        if atr_pct < 6.0:   return 'HIGH'
        return 'EXTREME'

    def _volume_bucket(vol_ratio: float) -> str:
        """[v10.4] Ratio volume_atual / volume_médio_20d.
        >1.5 = volume acima da média (confirma movimento); <0.7 = volume fraco."""
        if vol_ratio <= 0:   return 'UNKNOWN'
        if vol_ratio < 0.5:  return 'VERY_LOW'
        if vol_ratio < 0.8:  return 'LOW'
        if vol_ratio < 1.3:  return 'NORMAL'
        if vol_ratio < 2.0:  return 'HIGH'
        return 'SURGE'

    def _calc_atr(closes: list, highs: list = None, lows: list = None, period: int = 14) -> float:
        """[v10.4] ATR simplificado. Se highs/lows não disponíveis, usa desvio de closes."""
        if len(closes) < 2: return 0.0
        if highs and lows and len(highs) == len(closes):
            trs = []
            for i in range(1, min(period + 1, len(closes))):
                hl = highs[i] - lows[i]
                hc = abs(highs[i] - closes[i-1])
                lc = abs(lows[i] - closes[i-1])
                trs.append(max(hl, hc, lc))
            return sum(trs) / len(trs) if trs else 0.0
        # Fallback: desvio médio absoluto dos closes
        n = min(period, len(closes))
        diffs = [abs(closes[i] - closes[i-1]) for i in range(1, n + 1)]
        return sum(diffs) / len(diffs) if diffs else 0.0

    def extract_features(sig: dict, regime: dict, dq_score: float, now: datetime) -> dict:
        """[L-1][v10.4] Extrai features canônicas de um sinal para learning.
        Inclui atr_bucket e volume_bucket para espaço de padrões mais discriminativo.
        """
        score     = float(sig.get('score', 50) or 50)
        rsi       = float(sig.get('rsi', 50) or 50)
        ema9      = float(sig.get('ema9', 0) or 0)
        ema21     = float(sig.get('ema21', 0) or 0)
        ema50     = float(sig.get('ema50', 0) or 0)
        price     = float(sig.get('price', 0) or 0)
        change    = float(sig.get('change_pct', sig.get('change_24h', 0)) or 0)
        direction = 'LONG' if sig.get('signal') == 'COMPRA' else ('SHORT' if sig.get('signal') == 'VENDA' else 'NEUTRAL')
        asset_t   = sig.get('asset_type', 'stock')
        mkt       = sig.get('market_type', 'NYSE')

        # [v10.4] ATR e volume — vindos do price_dict ou do sig_enriched
        atr_pct    = float(sig.get('atr_pct', 0) or 0)
        vol_ratio  = float(sig.get('volume_ratio', 0) or 0)

        return {
            'score_bucket':     _score_bucket(score),
            'rsi_bucket':       _rsi_bucket(rsi),
            'ema_alignment':    _ema_alignment(ema9, ema21, ema50, price),
            'change_pct_bucket':_change_pct_bucket(change),
            'volatility_bucket':_volatility_bucket(regime.get('volatility', 'NORMAL')),
            'regime_mode':      regime.get('mode', 'UNKNOWN'),
            'time_bucket':      _time_bucket(now),
            'weekday':          now.weekday(),   # 0=segunda
            'asset_type':       asset_t,
            'market_type':      mkt,
            'direction':        direction,
            'dq_bucket':        _data_quality_bucket(dq_score),
            'atr_bucket':       _atr_bucket(atr_pct),       # [v10.4] volatility real por ativo
            'volume_bucket':    _volume_bucket(vol_ratio),  # [v10.4] confirmação por volume
        }

    def make_feature_hash(features: dict) -> str:
        """[L-1][v10.4] Hash canônico determinístico — espaço ampliado com atr, volume e weekday.
        weekday distingue comportamento segunda-feira (gap open) de quarta/quinta (fluxo normal).
        """
        canonical = '|'.join([
            features.get('score_bucket', ''),
            features.get('rsi_bucket', ''),
            features.get('ema_alignment', ''),
            features.get('volatility_bucket', ''),
            features.get('regime_mode', ''),
            features.get('time_bucket', ''),
            features.get('asset_type', ''),
            features.get('direction', ''),
            features.get('atr_bucket', ''),       # [v10.4]
            features.get('volume_bucket', ''),    # [v10.4]
            str(features.get('weekday', '')),     # [v10.4]
        ])
        return hashlib.md5(canonical.encode()).hexdigest()[:16]

def get_dq_score(symbol: str) -> float:
    """Retorna data quality score do símbolo ou 50 se desconhecido."""
    with dq_lock:
        dq = data_quality.get(symbol.upper(), {})
    return float(dq.get('quality', 50))

# ═══════════════════════════════════════════════════════════════
# [L-3/L-4] PATTERN & FACTOR STATS — estruturas e helpers
# ═══════════════════════════════════════════════════════════════

def _empty_pattern_stats(feature_hash: str) -> dict:
    return {
        'feature_hash': feature_hash,
        'total_samples': 0, 'wins': 0, 'losses': 0, 'flat_count': 0,
        'avg_pnl': 0.0, 'avg_pnl_pct': 0.0,
        'ewma_pnl_pct': 0.0, 'ewma_hit_rate': 0.5,
        'expectancy': 0.0, 'downside_score': 0.0,
        'max_loss_seen': 0.0, 'confidence_weight': 0.0,
        'last_seen_at': '', 'updated_at': '',
    }

def _empty_factor_stats(factor_type: str, factor_value: str) -> dict:
    return {
        'factor_type': factor_type, 'factor_value': factor_value,
        'total_samples': 0, 'wins': 0, 'losses': 0,
        'avg_pnl_pct': 0.0, 'ewma_pnl_pct': 0.0,
        'expectancy': 0.0, 'downside_score': 0.0,
        'confidence_weight': 0.0,
        'last_seen_at': '', 'updated_at': '',
    }

def _update_ewma(current: float, new_value: float, alpha: float) -> float:
    return alpha * new_value + (1 - alpha) * current

def _calc_confidence_weight(total_samples: int, ewma_hit_rate: float,
                             expectancy: float, downside_score: float) -> float:
    """[L-3] Peso de confiança: aumenta com amostras, penaliza downside."""
    # Fator de amostras: sobe suavemente até N>=30
    sample_factor = min(total_samples / max(LEARNING_MIN_SAMPLES * 3, 30), 1.0)
    # Fator de hit_rate normalizado (0.5 = neutro)
    hit_factor    = max(0.0, (ewma_hit_rate - 0.5) * 2)
    # Fator de expectancy (normalizado para [-1, 1])
    exp_factor    = max(-1.0, min(1.0, expectancy / 3.0))
    # Penalidade de downside
    down_penalty  = min(downside_score / 5.0, 1.0)

    raw = sample_factor * (0.4 + 0.3 * hit_factor + 0.3 * exp_factor) - 0.2 * down_penalty
    return max(-1.0, min(1.0, round(raw, 4)))

def update_pattern_stats(feature_hash: str, pnl: float, pnl_pct: float) -> dict:
    """[L-3] Atualiza pattern_stats em memória de forma incremental."""
    global last_learning_update
    alpha = LEARNING_EWMA_ALPHA
    now_s = datetime.utcnow().isoformat()
    with learning_lock:
        s = pattern_stats_cache.get(feature_hash) or _empty_pattern_stats(feature_hash)
        s['total_samples'] += 1
        if pnl_pct > 0.1:    s['wins'] += 1
        elif pnl_pct < -0.1: s['losses'] += 1
        else:                  s['flat_count'] += 1

        n = s['total_samples']
        # Média simples incremental (Welford)
        s['avg_pnl']     += (pnl - s['avg_pnl']) / n
        s['avg_pnl_pct'] += (pnl_pct - s['avg_pnl_pct']) / n
        # EWMA para recência
        s['ewma_pnl_pct']  = _update_ewma(s['ewma_pnl_pct'],  pnl_pct, alpha)
        hit = 1.0 if pnl_pct > 0.1 else 0.0
        s['ewma_hit_rate'] = _update_ewma(s['ewma_hit_rate'], hit, alpha)
        # Expectancy = win_rate * avg_win - loss_rate * avg_loss (simplificado)
        wins   = s['wins']; losses = s['losses']
        s['expectancy'] = round(s['ewma_hit_rate'] * max(s['avg_pnl_pct'], 0)
                                - (1 - s['ewma_hit_rate']) * abs(min(s['avg_pnl_pct'], 0)), 4)
        # Downside: frequência de perdas grandes
        if pnl_pct < s['max_loss_seen']: s['max_loss_seen'] = round(pnl_pct, 4)
        loss_rate = losses / n if n > 0 else 0
        s['downside_score'] = round(loss_rate * abs(min(s['avg_pnl_pct'], 0)) * 10, 4)
        s['confidence_weight'] = _calc_confidence_weight(
            n, s['ewma_hit_rate'], s['expectancy'], s['downside_score'])
        s['last_seen_at'] = now_s; s['updated_at'] = now_s
        pattern_stats_cache[feature_hash] = s
        last_learning_update = now_s
    return dict(s)

def update_factor_stats(features: dict, pnl: float, pnl_pct: float):
    """[L-4] Atualiza factor_stats incrementalmente para cada fator do sinal."""
    alpha   = LEARNING_EWMA_ALPHA
    now_s   = datetime.utcnow().isoformat()
    factors = [
        ('score_bucket',      features.get('score_bucket', '')),
        ('rsi_bucket',        features.get('rsi_bucket', '')),
        ('ema_alignment',     features.get('ema_alignment', '')),
        ('volatility_bucket', features.get('volatility_bucket', '')),
        ('regime_mode',       features.get('regime_mode', '')),
        ('time_bucket',       features.get('time_bucket', '')),
        ('weekday',           str(features.get('weekday', ''))),
        ('asset_type',        features.get('asset_type', '')),
        ('market_type',       features.get('market_type', '')),
        ('direction',         features.get('direction', '')),
        ('dq_bucket',         features.get('dq_bucket', '')),
        ('atr_bucket',        features.get('atr_bucket', '')),      # [v10.4]
        ('volume_bucket',     features.get('volume_bucket', '')),   # [v10.4]
    ]
    with learning_lock:
        for ftype, fval in factors:
            if not fval: continue
            key = (ftype, fval)
            s   = factor_stats_cache.get(key) or _empty_factor_stats(ftype, fval)
            s['total_samples'] += 1
            n = s['total_samples']
            if pnl_pct > 0.1:    s['wins'] += 1
            elif pnl_pct < -0.1: s['losses'] += 1
            s['avg_pnl_pct']   += (pnl_pct - s['avg_pnl_pct']) / n
            s['ewma_pnl_pct']   = _update_ewma(s['ewma_pnl_pct'], pnl_pct, alpha)
            hit = 1.0 if pnl_pct > 0.1 else 0.0
            hit_rate = _update_ewma(s.get('_ewma_hit', 0.5), hit, alpha)
            s['_ewma_hit'] = hit_rate
            s['expectancy'] = round(hit_rate * max(s['avg_pnl_pct'], 0)
                                    - (1 - hit_rate) * abs(min(s['avg_pnl_pct'], 0)), 4)
            loss_rate = s['losses'] / n if n > 0 else 0
            s['downside_score'] = round(loss_rate * abs(min(s['avg_pnl_pct'], 0)) * 10, 4)
            s['confidence_weight'] = _calc_confidence_weight(
                n, hit_rate, s['expectancy'], s['downside_score'])
            s['last_seen_at'] = now_s; s['updated_at'] = now_s
            factor_stats_cache[key] = s

# ═══════════════════════════════════════════════════════════════
# [L-5] CONFIDENCE ENGINE — aprendizado explicável
# ═══════════════════════════════════════════════════════════════

def calc_learning_confidence(sig: dict, features: dict, feature_hash: str) -> dict:
    """
    [L-5][P0-2] Calcula learning_confidence para um sinal.
    Retorna dict com breakdown completo — nada de caixa-preta.

    IMPORTANTE: base normaliza pela FORÇA do sinal, não pelo valor bruto.
    Score 85 (compra forte) e score 15 (venda forte) têm mesma força base = 0.70.
    Evita viés estrutural contra shorts.
    """
    if not LEARNING_ENABLED:
        direction = features.get('direction', 'LONG') if features else 'LONG'
        return _neutral_confidence(sig.get('score', 50), direction)

    raw_score   = float(sig.get('score', 50) or 50)
    dq_score    = float(features.get('_dq_score', 50))
    regime_mode = features.get('regime_mode', 'UNKNOWN')
    direction   = features.get('direction', 'LONG')

    # ── [P0-2] Base: força relativa ao lado do sinal ──────────────
    # score 50 = neutro → força 0; score 100 ou 0 → força máxima
    # LONG:  score alto é bom   (ex. 85 → força = (85-50)/50 = 0.70)
    # SHORT: score baixo é bom  (ex. 15 → força = (50-15)/50 = 0.70)
    if direction == 'SHORT':
        signal_strength = (50 - raw_score) / 50.0   # scores baixos = curto forte
    else:
        signal_strength = (raw_score - 50) / 50.0   # scores altos = longo forte
    # Normalizar para [0, 1] — força negativa tratada como neutro (50%)
    base = max(0.0, min(1.0, 0.5 + signal_strength * 0.5))

    # ── Histórico do padrão ───────────────────────────────────
    with learning_lock:
        ps = dict(pattern_stats_cache.get(feature_hash, {}))
    p_samples = ps.get('total_samples', 0)
    p_cw      = ps.get('confidence_weight', 0.0)
    p_exp     = ps.get('expectancy', 0.0)

    # Shrinkage: peso do padrão cresce com amostras
    p_weight  = min(p_samples / max(LEARNING_MIN_SAMPLES * 3, 30), 1.0)
    pattern_score = 0.5 + 0.5 * p_cw   # mapeia [-1,1] → [0,1]

    # ── Fatores individuais ───────────────────────────────────
    relevant = ['score_bucket','rsi_bucket','ema_alignment','regime_mode','direction']
    factor_scores = []
    with learning_lock:
        for ftype in relevant:
            fval = features.get(ftype, '')
            if not fval: continue
            fs = factor_stats_cache.get((ftype, fval), {})
            if fs.get('total_samples', 0) >= 5:
                factor_scores.append(0.5 + 0.5 * fs.get('confidence_weight', 0.0))
    factor_score = (sum(factor_scores) / len(factor_scores)) if factor_scores else 0.5

    # ── Ajuste de qualidade do dado ───────────────────────────
    dq_adj = (dq_score / 100.0 - 0.5) * 0.2   # ±0.1 no máximo

    # ── Ajuste de regime ─────────────────────────────────────
    regime_adj = 0.0
    if regime_mode == 'HIGH_VOL':   regime_adj = -0.08
    elif regime_mode == 'TRENDING': regime_adj =  0.04

    # ── Penalidade por amostra pequena ───────────────────────
    sample_penalty = max(0.0, 0.15 * (1 - p_weight))

    # ── Composição final ─────────────────────────────────────
    # Peso: base 40%, padrão 30% (ajustado por shrinkage), fatores 20%, ajustes 10%
    if p_weight > 0:
        blended = (0.40 * base
                   + 0.30 * (p_weight * pattern_score + (1 - p_weight) * base)
                   + 0.20 * factor_score
                   + 0.10 * base)    # fallback parcial
    else:
        blended = 0.65 * base + 0.35 * factor_score

    final_raw   = blended + dq_adj + regime_adj - sample_penalty
    final_conf  = max(0.0, min(1.0, final_raw))
    final_score = round(final_conf * 100, 1)

    band = ('HIGH'   if final_score >= 65 else
            'MEDIUM' if final_score >= 40 else 'LOW')

    return {
        'final_confidence': final_score,
        'confidence_band':  band,
        'base_score':       round(base * 100, 1),
        'pattern_score':    round(pattern_score * 100, 1) if p_weight > 0 else None,
        'pattern_samples':  p_samples,
        'factor_score':     round(factor_score * 100, 1),
        'data_quality_adj': round(dq_adj * 100, 1),
        'regime_adj':       round(regime_adj * 100, 1),
        'sample_penalty':   round(sample_penalty * 100, 1),
        'feature_hash':     feature_hash,
    }

def _neutral_confidence(raw_score: float, direction: str = 'LONG') -> dict:
    """[P0-2][P6] Fallback — normaliza pela força do lado do sinal.
    Banda é calculada dinamicamente (não fixo MEDIUM).
    """
    if direction == 'SHORT':
        strength = max(0.0, (50 - raw_score) / 50.0)
    else:
        strength = max(0.0, (raw_score - 50) / 50.0)
    final = round(50 + strength * 50, 1)
    # [P6] Banda dinâmica: não travar em MEDIUM quando confiança for alta/baixa
    if   final >= 70: band = 'HIGH'
    elif final <= 40: band = 'LOW'
    else:             band = 'MEDIUM'
    return {
        'final_confidence': final,
        'confidence_band':  band,
        'base_score':       round(final, 1),
        'pattern_score':    None, 'pattern_samples': 0,
        'factor_score':     50.0, 'data_quality_adj': 0.0,
        'regime_adj':       0.0,  'sample_penalty':   0.0,
        'feature_hash':     '',
    }

# ═══════════════════════════════════════════════════════════════
# [L-6] INSIGHT ENGINE — explicação humano-legível
# ═══════════════════════════════════════════════════════════════

def generate_insight(sig: dict, features: dict, feature_hash: str, conf: dict) -> str:
    """[L-6] Gera insight_summary explicável para o sinal."""
    if not LEARNING_ENABLED or LEARNING_DEGRADED:
        return f"Score bruto: {sig.get('score', 50)}/100. Learning indisponível."

    parts = []
    band   = conf.get('confidence_band', 'MEDIUM')
    fc     = conf.get('final_confidence', 50)
    p_samp = conf.get('pattern_samples', 0)

    # Avaliação geral
    if band == 'HIGH':
        parts.append(f"Alta confiança ({fc:.0f}/100)")
    elif band == 'MEDIUM':
        parts.append(f"Confiança média ({fc:.0f}/100)")
    else:
        parts.append(f"Baixa confiança ({fc:.0f}/100)")

    # Histórico do padrão
    with learning_lock:
        ps = dict(pattern_stats_cache.get(feature_hash, {}))
    if p_samp >= LEARNING_MIN_SAMPLES:
        wr    = round(ps.get('ewma_hit_rate', 0.5) * 100)
        exp   = ps.get('expectancy', 0.0)
        sign  = "positiva" if exp >= 0 else "negativa"
        parts.append(f"padrão semelhante teve win rate de {wr}% em {p_samp} amostras (expectancy {sign})")
    elif p_samp > 0:
        parts.append(f"baixa amostra ({p_samp} trades); confiança reduzida apesar do score")
    else:
        parts.append("sem histórico para este padrão ainda")

    # Fatores positivos e negativos
    regime = features.get('regime_mode', '')
    if regime == 'HIGH_VOL':
        parts.append("atenção: regime HIGH_VOL reduz confiança histórica")
    elif regime == 'TRENDING':
        parts.append("regime TRENDING favorável para este setup")

    dq_adj = conf.get('data_quality_adj', 0)
    if dq_adj < -5:
        parts.append("qualidade do dado fraca — sinal com cautela")

    ema = features.get('ema_alignment', '')
    if ema in ('BULLISH_STACK', 'BEARISH_STACK'):
        with learning_lock:
            ema_fs = factor_stats_cache.get(('ema_alignment', ema), {})
        if ema_fs.get('total_samples', 0) >= 5 and ema_fs.get('confidence_weight', 0) > 0.1:
            parts.append(f"{ema} historicamente favorável neste contexto")

    sp = conf.get('sample_penalty', 0)
    if sp > 8:
        parts.append(f"penalização por amostra insuficiente ({sp:.0f}pts)")

    return '. '.join(parts) + '.'

def get_risk_multiplier(conf: dict) -> float:
    """[L-9][v10.15] Multiplica size do position — agora contínuo, não discreto.
    Usa final_confidence (0-100) para interpolar linearmente entre MIN e MAX.
    conf=50 → mult=1.0 (neutro); conf=100 → MAX; conf=0 → MIN.
    """
    fc = float(conf.get('final_confidence', 50) or 50)
    # Normalizar: 50=neutro(1.0), 100=MAX, 0=MIN
    if fc >= 50:
        # 50→100 mapeia para 1.0→RISK_MULT_MAX
        t = (fc - 50) / 50.0  # 0→1
        mult = 1.0 + t * (RISK_MULT_MAX - 1.0)
    else:
        # 0→50 mapeia para RISK_MULT_MIN→1.0
        t = fc / 50.0  # 0→1
        mult = RISK_MULT_MIN + t * (1.0 - RISK_MULT_MIN)
    return round(max(RISK_MULT_MIN, min(RISK_MULT_MAX, mult)), 3)


def should_trade_ml(features: dict, conf: dict, asset_type: str = 'stock') -> tuple:
    """[v10.15] ML gate — consulta pattern_stats e factor_stats para decidir se deve operar.
    Retorna (should_trade: bool, reason: str, ml_score: float).
    ml_score: -1.0 (forte rejeição) a +1.0 (forte aprovação), 0=neutro.
    """
    if not LEARNING_ENABLED or LEARNING_DEGRADED:
        return True, 'learning_disabled', 0.0

    fc = float(conf.get('final_confidence', 50) or 50)
    feat_hash = conf.get('feature_hash', '')

    # 1. Checar pattern histórico
    pattern_score = 0.0
    with learning_lock:
        ps = pattern_stats_cache.get(feat_hash, {})
    if ps.get('total_samples', 0) >= 15:
        p_exp = ps.get('expectancy', 0)
        p_wr = ps.get('wins', 0) / max(ps['total_samples'], 1) * 100
        # Rejeitar padrões consistentemente perdedores
        if p_exp < -0.15 and p_wr < 40:
            return False, f'ML_PATTERN_REJECT(exp={p_exp:.3f},wr={p_wr:.0f}%,n={ps["total_samples"]})', -0.8
        pattern_score = min(max(p_exp * 2, -1.0), 1.0)

    # 2. Checar fatores críticos
    bad_factors = 0; good_factors = 0
    critical_factors = ['atr_bucket', 'volatility_bucket', 'regime_mode', 'volume_bucket', 'weekday']
    with learning_lock:
        for ftype in critical_factors:
            fval = str(features.get(ftype, ''))
            if not fval: continue
            fs = factor_stats_cache.get((ftype, fval), {})
            if fs.get('total_samples', 0) < 10: continue
            f_exp = fs.get('expectancy', 0)
            f_cw = fs.get('confidence_weight', 0.5)
            if f_exp < -0.1 and f_cw < 0.35:
                bad_factors += 1
            elif f_exp > 0.05 and f_cw > 0.45:
                good_factors += 1

    # Rejeitar se 3+ fatores críticos são negativos e nenhum é positivo
    if bad_factors >= 3 and good_factors == 0:
        return False, f'ML_FACTORS_REJECT(bad={bad_factors},good={good_factors})', -0.6

    # 3. Confiança muito baixa = rejeitar
    if fc < 30 and asset_type == 'crypto':
        return False, f'ML_LOW_CONF(fc={fc:.1f})', -0.5

    # Calcular ml_score geral
    conf_score = (fc - 50) / 50.0  # -1 a +1
    ml_score = 0.4 * conf_score + 0.4 * pattern_score + 0.2 * ((good_factors - bad_factors) / max(len(critical_factors), 1))
    return True, 'ML_OK', round(ml_score, 3)

def get_top_factors(n_best: int = 5, n_worst: int = 5) -> dict:
    """[L-6] Retorna fatores com melhor e pior performance histórica."""
    with learning_lock:
        items = [(k, dict(v)) for k, v in factor_stats_cache.items()
                 if v.get('total_samples', 0) >= LEARNING_MIN_SAMPLES]
    items.sort(key=lambda x: x[1].get('confidence_weight', 0), reverse=True)
    def _fmt(entry):
        k, v = entry
        return {'factor_type': k[0], 'factor_value': k[1],
                'samples': v['total_samples'], 'cw': round(v['confidence_weight'], 3),
                'expectancy': round(v.get('expectancy', 0), 4),
                'ewma_pnl_pct': round(v.get('ewma_pnl_pct', 0), 4)}
    return {
        'top_positive': [_fmt(i) for i in items[:n_best]],
        'top_negative': [_fmt(i) for i in reversed(items[-n_worst:]) if items],
    }


# [v10.15] Calibração contínua — tracking em memória de confidence vs outcome
_calibration_tracker = {
    'HIGH': {'wins': 0, 'losses': 0, 'total': 0, 'sum_pnl_pct': 0.0},
    'MEDIUM': {'wins': 0, 'losses': 0, 'total': 0, 'sum_pnl_pct': 0.0},
    'LOW': {'wins': 0, 'losses': 0, 'total': 0, 'sum_pnl_pct': 0.0},
}

def track_calibration(trade: dict):
    """[v10.15] Atualiza calibração in-memory quando trade fecha."""
    band = trade.get('_confidence_band') or trade.get('confidence_band', 'MEDIUM')
    pnl_pct = float(trade.get('pnl_pct', 0) or 0)
    if band not in _calibration_tracker: band = 'MEDIUM'
    ct = _calibration_tracker[band]
    ct['total'] += 1
    ct['sum_pnl_pct'] += pnl_pct
    if pnl_pct > 0.1:   ct['wins'] += 1
    elif pnl_pct < -0.1: ct['losses'] += 1
    # Log a cada 50 trades para visibilidade
    total_all = sum(b['total'] for b in _calibration_tracker.values())
    if total_all > 0 and total_all % 50 == 0:
        for b_name, b_data in _calibration_tracker.items():
            if b_data['total'] > 0:
                wr = b_data['wins'] / b_data['total'] * 100
                avg = b_data['sum_pnl_pct'] / b_data['total']
                log.info(f'[ML-CALIB] {b_name}: WR={wr:.1f}% avg_pnl={avg:.3f}% n={b_data["total"]}')

# ── [v10.18] Capital Ledger ──────────────────────────────────────────────
def ledger_record(strategy: str, event: str, symbol: str, amount: float,
                  balance_after: float, trade_id: str = ''):
    """[v10.18] Registra evento no capital ledger (memória + DB assíncrono).
    Eventos: RESERVE | RELEASE | PNL_CREDIT | BASELINE (v10.21)"""
    # [v10.21] Idempotência: proteger contra duplicata de RESERVE/RELEASE no mesmo trade
    if event in ('RESERVE', 'RELEASE') and trade_id:
        with _ledger_lock:
            recent = _capital_ledger[-50:] if len(_capital_ledger) > 50 else _capital_ledger
            for prev in reversed(recent):
                if (prev.get('trade_id') == trade_id and prev.get('event') == event
                        and prev.get('strategy') == strategy):
                    log.warning(f'[LEDGER-DEDUP] Skipping duplicate {event} for {trade_id}')
                    return  # já registrado — pular
    evt = {
        'ts': datetime.utcnow().isoformat(),
        'strategy': strategy,
        'event': event,
        'symbol': symbol,
        'amount': round(amount, 2),
        'balance_after': round(balance_after, 2),
        'trade_id': trade_id,
    }
    with _ledger_lock:
        _capital_ledger.append(evt)
        if len(_capital_ledger) > 5000:
            _capital_ledger[:] = _capital_ledger[-3000:]
    enqueue_persist('ledger_event', evt)

# ── [v10.18] Reconciliation Engine ──────────────────────────────────────
def _reconcile_strategy(name: str, memory_capital: float, initial: float,
                        open_trades: list, closed_trades: list) -> dict:
    """[v10.18] Reconcilia capital de uma estratégia."""
    committed = sum(t.get('position_value', 0) for t in open_trades)
    realized_pnl = sum(float(t.get('pnl', 0) or 0) for t in closed_trades)
    calculated = initial + realized_pnl - committed
    delta = memory_capital - calculated
    delta_pct = abs(delta) / max(initial, 1) * 100
    return {
        'strategy': name,
        'memory_capital': round(memory_capital, 2),
        'calculated_capital': round(calculated, 2),
        'committed': round(committed, 2),
        'realized_pnl': round(realized_pnl, 2),
        'delta': round(delta, 2),
        'delta_pct': round(delta_pct, 4),
        'ok': delta_pct < RECONCILIATION_ALERT_PCT,
        'ts': datetime.utcnow().isoformat(),
    }

def _reconcile_strategy_arbi(memory_capital: float, initial: float,
                             open_trades: list, closed_trades: list) -> dict:
    """[v10.19] Reconcilia capital de arbitragem (usa position_size em vez de position_value)."""
    committed = sum(t.get('position_size', 0) for t in open_trades)
    realized_pnl = sum(float(t.get('pnl', 0) or 0) for t in closed_trades)
    calculated = initial + realized_pnl - committed
    delta = memory_capital - calculated
    delta_pct = abs(delta) / max(initial, 1) * 100
    return {
        'strategy': 'arbi',
        'memory_capital': round(memory_capital, 2),
        'calculated_capital': round(calculated, 2),
        'committed': round(committed, 2),
        'realized_pnl': round(realized_pnl, 2),
        'delta': round(delta, 2),
        'delta_pct': round(delta_pct, 4),
        'ok': delta_pct < RECONCILIATION_ALERT_PCT,
        'ts': datetime.utcnow().isoformat(),
    }

def _record_baseline_if_needed():
    """[v10.21] Registra evento BASELINE no ledger para cada estratégia que nunca teve um.
    O BASELINE marca a data de corte contábil — eventos anteriores são drift pré-ledger.
    Chamado no boot, uma vez."""
    for strat, cap_var, initial in [
        ('stocks', stocks_capital, INITIAL_CAPITAL_STOCKS),
        ('crypto', crypto_capital, INITIAL_CAPITAL_CRYPTO),
        ('arbi', arbi_capital, ARBI_CAPITAL),
    ]:
        # Verificar se já existe um BASELINE no DB
        conn = get_db()
        if not conn: continue
        has_baseline = False
        try:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM capital_ledger WHERE strategy=%s AND event='BASELINE'",
                      (strat,))
            count = c.fetchone()[0]
            has_baseline = count > 0
            c.close(); conn.close()
        except Exception as e:
            log.debug(f'baseline check {strat}: {e}')
            try: conn.close()
            except: pass
        if not has_baseline:
            # Registrar baseline: amount = capital atual no momento do corte
            # balance_after = capital atual (o ponto de partida do ledger)
            ledger_record(strat, 'BASELINE', 'SYSTEM', cap_var, cap_var, 'BASELINE')
            log.info(f'[LEDGER-BASELINE] {strat}: registered baseline at ${cap_var:,.0f} '
                     f'(initial=${initial:,.0f}, drift=${cap_var - initial:,.0f})')

def _replay_ledger_events(events: list, initial: float) -> float:
    """[v10.20/v10.21] Replay de eventos de ledger. Se há BASELINE, usa como ponto de partida."""
    # [v10.21] Se existe evento BASELINE, começar dali (ignora initial)
    baseline_balance = None
    baseline_idx = -1
    for i, evt in enumerate(events):
        if evt.get('event') == 'BASELINE':
            baseline_balance = float(evt.get('amount', 0))
            baseline_idx = i
    if baseline_balance is not None:
        balance = baseline_balance
        events = events[baseline_idx + 1:]  # replay só pós-baseline
    else:
        balance = initial
    for evt in events:
        ev_type = evt.get('event', '')
        amount = float(evt.get('amount', 0))
        if ev_type == 'RESERVE':
            balance -= amount
        elif ev_type in ('RELEASE', 'PNL_CREDIT'):
            balance += amount
    return balance

def _load_ledger_from_db(strategy: str) -> list:
    """[v10.20] Carrega eventos do capital_ledger do MySQL para uma estratégia."""
    conn = get_db()
    if not conn: return []
    try:
        c = conn.cursor(dictionary=True)
        c.execute("SELECT event, amount FROM capital_ledger WHERE strategy=%s ORDER BY id ASC",
                  (strategy,))
        rows = c.fetchall()
        c.close(); conn.close()
        return [{'event': r['event'], 'amount': float(r['amount'])} for r in rows]
    except Exception as e:
        log.error(f'_load_ledger_from_db({strategy}): {e}')
        try: conn.close()
        except: pass
        return []

def _reconcile_via_ledger(strategy: str, initial: float, memory_capital: float) -> dict:
    """[v10.20] Reconciliação via replay do ledger — segunda camada de verificação.
    Camada 2a: replay da memória (rápido, cobre runtime).
    Camada 2b: replay do MySQL (lento, cobre pós-deploy — usa DB quando memória vazia)."""
    with _ledger_lock:
        mem_events = [e for e in _capital_ledger if e.get('strategy') == strategy]
    # Se temos eventos em memória, usar (mais rápido e preciso para drift runtime)
    # Se não temos (pós-deploy), carregar do MySQL para reconciliação histórica
    if mem_events:
        events = mem_events
        source = 'memory'
    else:
        events = _load_ledger_from_db(strategy)
        source = 'mysql'
    if not events:
        return {'strategy': f'{strategy}_ledger', 'ledger_events': 0, 'ok': True,
                'memory_capital': round(memory_capital, 2), 'ledger_capital': round(initial, 2),
                'delta': 0, 'delta_pct': 0, 'source': 'none',
                'ts': datetime.utcnow().isoformat()}
    balance = _replay_ledger_events(events, initial)
    delta = memory_capital - balance
    delta_pct = abs(delta) / max(initial, 1) * 100
    return {
        'strategy': f'{strategy}_ledger',
        'memory_capital': round(memory_capital, 2),
        'ledger_capital': round(balance, 2),
        'ledger_events': len(events),
        'source': source,
        'delta': round(delta, 2),
        'delta_pct': round(delta_pct, 4),
        # [v10.52] Usa threshold dedicado da camada 2 (ledger). Menos
        # estrito que camada 1 para evitar falsos positivos de historico
        # pre-v10.20 nao rehidratado.
        'ok': delta_pct < RECONCILIATION_LEDGER_ALERT_PCT,
        'ts': datetime.utcnow().isoformat(),
    }

def run_reconciliation():
    """[v10.18/v10.19] Roda reconciliação de capital — chamado pelo watchdog a cada 10min.
    Camada 1: fórmula (initial + pnl - committed).
    Camada 2: replay do ledger (initial + eventos).
    Inclui stocks, crypto e arbi."""
    global _last_reconciliation
    now = time.time()
    if now - _last_reconciliation < RECONCILIATION_INTERVAL_S:
        return
    _last_reconciliation = now
    try:
        with state_lock:
            r_stocks = _reconcile_strategy('stocks', stocks_capital, INITIAL_CAPITAL_STOCKS,
                                           list(stocks_open), list(stocks_closed))
            r_crypto = _reconcile_strategy('crypto', crypto_capital, INITIAL_CAPITAL_CRYPTO,
                                           list(crypto_open), list(crypto_closed))
            r_arbi = _reconcile_strategy_arbi(arbi_capital, ARBI_CAPITAL,
                                              list(arbi_open), list(arbi_closed))
            # [v10.20] Camada 2: reconciliação via ledger (memória ou MySQL)
            r_stocks_ldg = _reconcile_via_ledger('stocks', INITIAL_CAPITAL_STOCKS, stocks_capital)
            r_crypto_ldg = _reconcile_via_ledger('crypto', INITIAL_CAPITAL_CRYPTO, crypto_capital)
            r_arbi_ldg   = _reconcile_via_ledger('arbi', ARBI_CAPITAL, arbi_capital)
        for r in [r_stocks, r_crypto, r_arbi]:
            _reconciliation_log.append(r)
            if not r['ok']:
                msg = (f'[RECON-ALERT] {r["strategy"]}: delta=${r["delta"]:,.0f} '
                       f'({r["delta_pct"]:.2f}%) mem=${r["memory_capital"]:,.0f} '
                       f'calc=${r["calculated_capital"]:,.0f}')
                log.warning(msg)
                send_whatsapp(msg)
            else:
                log.info(f'[RECON-OK] {r["strategy"]}: delta=${r["delta"]:,.0f} ({r["delta_pct"]:.2f}%)')
        # [v10.20] Camada 2: ledger-based reconciliation (stocks + crypto + arbi)
        for r_ldg in [r_stocks_ldg, r_crypto_ldg, r_arbi_ldg]:
            _reconciliation_log.append(r_ldg)
            if not r_ldg['ok']:
                msg = (f'[RECON-LEDGER-ALERT] {r_ldg["strategy"]}: delta=${r_ldg["delta"]:,.0f} '
                       f'({r_ldg["delta_pct"]:.2f}%) events={r_ldg["ledger_events"]}')
                log.warning(msg)
                send_whatsapp(msg)
            elif r_ldg.get('ledger_events', 0) > 0:
                log.info(f'[RECON-LEDGER-OK] {r_ldg["strategy"]}: delta=${r_ldg["delta"]:,.0f} events={r_ldg["ledger_events"]}')
        if len(_reconciliation_log) > 300:
            _reconciliation_log[:] = _reconciliation_log[-150:]
        # Persistir no DB
        conn = get_db()
        if conn:
            try:
                c = conn.cursor()
                for r in [r_stocks, r_crypto, r_arbi]:
                    c.execute("""INSERT INTO reconciliation_log
                        (ts, strategy, memory_capital, calculated_capital, committed,
                         realized_pnl, delta, delta_pct, ok)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (r['ts'], r['strategy'], r['memory_capital'], r['calculated_capital'],
                         r['committed'], r['realized_pnl'], r['delta'], r['delta_pct'],
                         1 if r['ok'] else 0))
                # [v10.20] Ledger reconciliation — persiste delta e contagem de eventos
                for r_ldg in [r_stocks_ldg, r_crypto_ldg, r_arbi_ldg]:
                    c.execute("""INSERT INTO reconciliation_log
                        (ts, strategy, memory_capital, calculated_capital, committed,
                         realized_pnl, delta, delta_pct, ok)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (r_ldg['ts'], r_ldg['strategy'], r_ldg['memory_capital'],
                         r_ldg.get('ledger_capital', 0), 0, 0,
                         r_ldg['delta'], r_ldg['delta_pct'], 1 if r_ldg['ok'] else 0))
                conn.commit(); c.close(); conn.close()
            except Exception as e:
                log.error(f'reconciliation persist: {e}')
                try: conn.close()
                except: pass
    except Exception as e:
        log.error(f'run_reconciliation: {e}')

# ── [v10.18] Calibration Persistence ────────────────────────────────────
def persist_calibration():
    """[v10.18] Salva _calibration_tracker no MySQL a cada 5min."""
    global _last_calibration_persist
    now = time.time()
    if now - _last_calibration_persist < CALIBRATION_PERSIST_INTERVAL:
        return
    _last_calibration_persist = now
    conn = get_db()
    if not conn: return
    cursor=None
    try:
        c = conn.cursor()
        for band, data in _calibration_tracker.items():
            c.execute("""INSERT INTO calibration_tracker (band, wins, losses, total, sum_pnl_pct, updated_at)
                VALUES (%s,%s,%s,%s,%s,NOW())
                ON DUPLICATE KEY UPDATE
                wins=VALUES(wins), losses=VALUES(losses), total=VALUES(total),
                sum_pnl_pct=VALUES(sum_pnl_pct), updated_at=NOW()""",
                (band, data['wins'], data['losses'], data['total'], data['sum_pnl_pct']))
        conn.commit(); c.close(); conn.close()
        log.debug('[CALIB-PERSIST] calibration_tracker saved to MySQL')
    except Exception as e:
        log.error(f'persist_calibration: {e}')
        try: conn.close()
        except: pass

def load_calibration():
    """[v10.18] Carrega _calibration_tracker do MySQL no boot — sobrevive a deploys."""
    conn = get_db()
    if not conn: return
    cursor=None
    try:
        c = conn.cursor(dictionary=True)
        c.execute("SELECT band, wins, losses, total, sum_pnl_pct FROM calibration_tracker")
        loaded = 0
        for row in c.fetchall():
            band = row['band']
            if band in _calibration_tracker:
                _calibration_tracker[band]['wins'] = int(row['wins'])
                _calibration_tracker[band]['losses'] = int(row['losses'])
                _calibration_tracker[band]['total'] = int(row['total'])
                _calibration_tracker[band]['sum_pnl_pct'] = float(row['sum_pnl_pct'])
                loaded += 1
        c.close(); conn.close()
        if loaded > 0:
            log.info(f'[CALIB-LOAD] Loaded {loaded} bands from MySQL: '
                     + ', '.join(f'{b}={d["total"]}t' for b, d in _calibration_tracker.items() if d['total'] > 0))
    except Exception as e:
        log.error(f'load_calibration: {e}')
        try: conn.close()
        except: pass

# ── [v10.18] Crypto Conviction Filter ───────────────────────────────────
def check_crypto_conviction(conf_c: dict, change_24h: float, display: str) -> tuple:
    """[v10.18] Filtro de convicção para crypto — bloqueia trades com confiança baixa
    e movimento insuficiente. Retorna (ok: bool, reason: str)."""
    final_conf = conf_c.get('final_confidence', 50)
    if final_conf < CRYPTO_MIN_CONVICTION and abs(change_24h) < 2.0:  # [v10.24] era 3.0 — 2% já é movimento suficiente para validar
        return False, f'conviction_low conf={final_conf:.1f} change={change_24h:.1f}%'
    return True, ''

# ── [v10.16] Daily Drawdown per Strategy ──────────────────────────────────
def check_strategy_daily_dd(strategy: str) -> tuple:
    """[v10.16] Verifica se a estratégia atingiu o drawdown diário.
    Retorna (blocked: bool, reason: str).
    """
    today = datetime.utcnow().strftime('%Y-%m-%d')
    if strategy == 'stocks':
        dd_state = _daily_dd_stocks
        limit = DAILY_DD_STOCKS_PCT
        cap = INITIAL_CAPITAL_STOCKS
        closed_list = stocks_closed
    elif strategy == 'crypto':
        dd_state = _daily_dd_crypto
        limit = DAILY_DD_CRYPTO_PCT
        cap = INITIAL_CAPITAL_CRYPTO
        closed_list = crypto_closed
    else:
        return False, 'OK'
    # Reset diário
    if dd_state['date'] != today:
        dd_state['date'] = today
        dd_state['pnl'] = 0.0
        dd_state['blocked'] = False
    # Calcular PnL do dia
    daily_pnl = sum(t.get('pnl', 0) for t in closed_list
                     if (t.get('closed_at', '') or '')[:10] == today)
    dd_state['pnl'] = daily_pnl
    dd_pct = abs(min(daily_pnl, 0)) / max(cap, 1) * 100
    if dd_pct >= limit:
        dd_state['blocked'] = True
        return True, f'STRATEGY_DAILY_DD_{strategy.upper()} ({dd_pct:.2f}%/{limit:.1f}%)'
    dd_state['blocked'] = False
    return False, 'OK'


# ── [v10.16] Auto-blacklist de símbolos perdedores ────────────────────────
def evaluate_symbol_blacklist():
    """[v10.16] Avalia performance por símbolo e blacklista perdedores.
    Roda periodicamente (a cada BLACKLIST_REVIEW_H).
    """
    global _blacklist_last_eval
    now = time.time()
    if now - _blacklist_last_eval < BLACKLIST_REVIEW_H * 3600:
        return  # ainda não é hora de reavaliar
    _blacklist_last_eval = now
    with state_lock:
        all_closed = list(stocks_closed) + list(crypto_closed)
    # Agrupar por símbolo
    by_sym = {}
    for t in all_closed:
        s = t.get('symbol', '')
        if not s: continue
        if s not in by_sym:
            by_sym[s] = {'n': 0, 'wins': 0, 'total_pnl': 0.0, 'asset_type': t.get('asset_type', '')}
        by_sym[s]['n'] += 1
        by_sym[s]['total_pnl'] += float(t.get('pnl', 0) or 0)
        if float(t.get('pnl', 0) or 0) > 0:
            by_sym[s]['wins'] += 1
    # Avaliar
    newly_blocked = 0
    newly_unblocked = 0
    for sym, stats in by_sym.items():
        if stats['n'] < BLACKLIST_MIN_TRADES:
            continue
        avg_pnl = stats['total_pnl'] / stats['n']
        wr = stats['wins'] / stats['n'] * 100
        should_block = avg_pnl < BLACKLIST_MAX_AVG_PNL and wr < BLACKLIST_MAX_WR
        if should_block:
            if sym not in _symbol_blacklist:
                newly_blocked += 1
            _symbol_blacklist[sym] = {
                'reason': f'avg_pnl=${avg_pnl:.0f} wr={wr:.0f}% n={stats["n"]}',
                'until': now + BLACKLIST_REVIEW_H * 3600,
                'stats': {'avg_pnl': round(avg_pnl, 2), 'wr': round(wr, 1), 'n': stats['n']},
            }
        elif sym in _symbol_blacklist:
            # Símbolo melhorou — desbloquear
            del _symbol_blacklist[sym]
            newly_unblocked += 1
    # Limpar blacklists expirados
    expired = [s for s, v in _symbol_blacklist.items() if v['until'] < now]
    for s in expired:
        del _symbol_blacklist[s]
    if newly_blocked or newly_unblocked:
        log.info(f'[BLACKLIST] Avaliação: +{newly_blocked} bloqueados, -{newly_unblocked} desbloqueados, total={len(_symbol_blacklist)}')
        for sym, info in _symbol_blacklist.items():
            log.info(f'  [BLACKLIST] {sym}: {info["reason"]}')


def is_symbol_blacklisted(symbol: str) -> tuple:
    """[v10.16] Retorna (blocked: bool, reason: str)."""
    info = _symbol_blacklist.get(symbol)
    if info and info['until'] > time.time():
        return True, f'SYMBOL_BLACKLISTED ({info["reason"]})'
    return False, 'OK'


def get_symbol_wr_sizing_mult(symbol: str, asset_type: str = 'crypto') -> float:
    """[v10.29] Dynamic position sizing multiplier based on per-symbol WR.
    Inspired by Manus report: allocate more to high-WR assets, less to low-WR.
    Returns multiplier: 0.6x to 1.3x (default 1.0 if insufficient data).

    Rules:
    - WR > 58%: 1.3x (proven winner)
    - WR > 53%: 1.15x (above average)
    - WR 48-53%: 1.0x (neutral)
    - WR 45-48%: 0.8x (underperformer)
    - WR < 45%: 0.6x (poor performer — near blacklist territory)
    - Minimum 10 closed trades required for adjustment.
    """
    with state_lock:
        closed_list = list(crypto_closed) if asset_type == 'crypto' else list(stocks_closed)
    # Count wins/losses for this symbol
    n = 0; wins = 0
    for t in closed_list:
        if t.get('symbol', '') == symbol:
            n += 1
            if float(t.get('pnl', 0) or 0) > 0:
                wins += 1
    if n < 10:
        return 1.0  # insufficient data
    wr = wins / n * 100
    if wr > 58:   return 1.30
    if wr > 53:   return 1.15
    if wr >= 48:  return 1.00
    if wr >= 45:  return 0.80
    return 0.60


# ── [v10.16] ATR-based adaptive stop-loss ─────────────────────────────────
def get_adaptive_sl_pct(trade: dict) -> float:
    """[v10.16] Calcula stop-loss adaptativo baseado no ATR do ativo.
    Retorna o SL como percentual positivo (ex: 2.5 = -2.5%).
    """
    atr_pct = float(trade.get('_atr_pct', 0) or trade.get('atr_pct', 0) or 0)
    asset_type = trade.get('asset_type', 'stock')
    if atr_pct <= 0:
        # Sem ATR: usar default fixo
        return 2.0 if asset_type == 'stock' else 2.0
    mult = ATR_SL_MULTIPLIER_STOCK if asset_type == 'stock' else ATR_SL_MULTIPLIER_CRYPTO
    sl_pct = atr_pct * mult
    # Clampar entre mínimo e máximo
    sl_pct = max(ATR_SL_MIN_PCT, min(ATR_SL_MAX_PCT, sl_pct))
    return round(sl_pct, 2)


# ── [v10.16] Score Snapshot para auditoria de trades ──────────────────────
def make_score_snapshot(sig: dict, features: dict, conf: dict, extras: dict = None) -> dict:
    """[v10.16] Cria snapshot completo dos componentes de score para auditoria."""
    snap = {
        'score': sig.get('score'),
        'rsi': sig.get('rsi'),
        'ema9': sig.get('ema9'),
        'ema21': sig.get('ema21'),
        'ema50': sig.get('ema50'),
        'atr_pct': sig.get('atr_pct'),
        'volume_ratio': sig.get('volume_ratio'),
        'direction': features.get('direction'),
        'regime_mode': features.get('regime_mode'),
        'volatility_bucket': features.get('volatility_bucket'),
        'atr_bucket': features.get('atr_bucket'),
        'volume_bucket': features.get('volume_bucket'),
        'weekday': features.get('weekday'),
        'time_bucket': features.get('time_bucket'),
        'rsi_bucket': features.get('rsi_bucket'),
        'ema_alignment': features.get('ema_alignment'),
        'final_confidence': conf.get('final_confidence'),
        'risk_multiplier': conf.get('risk_multiplier') if isinstance(conf, dict) else None,
        'timestamp': datetime.utcnow().isoformat(),
    }
    if extras:
        snap.update(extras)
    return {k: v for k, v in snap.items() if v is not None}


# ── [v10.16] Inactivity Alert ─────────────────────────────────────────────
def check_inactivity_alert():
    """[v10.16] Verifica se houve inatividade anormal e alerta."""
    now = time.time()
    # Stocks: só checar durante horário de mercado
    if market_open_for('NYSE') or market_open_for('B3'):
        last_stk = _last_trade_opened.get('stocks', now)
        hours_since_stk = (now - last_stk) / 3600 if last_stk > 0 else 0
        if hours_since_stk >= INACTIVITY_ALERT_H_STOCKS and not _inactivity_alerted.get('stocks'):
            _inactivity_alerted['stocks'] = True
            log.warning(f'[INACTIVITY] STOCKS: {hours_since_stk:.1f}h sem novas trades (limite={INACTIVITY_ALERT_H_STOCKS}h)')
            try:
                if ALERTS_ENABLED:
                    _send_whatsapp_direct(f'⚠️ INATIVIDADE: {hours_since_stk:.1f}h sem trades de stocks')
            except: pass
        elif hours_since_stk < INACTIVITY_ALERT_H_STOCKS:
            _inactivity_alerted['stocks'] = False
    # Crypto: 24/7
    last_cry = _last_trade_opened.get('crypto', now)
    hours_since_cry = (now - last_cry) / 3600 if last_cry > 0 else 0
    if hours_since_cry >= INACTIVITY_ALERT_H_CRYPTO and not _inactivity_alerted.get('crypto'):
        _inactivity_alerted['crypto'] = True
        log.warning(f'[INACTIVITY] CRYPTO: {hours_since_cry:.1f}h sem novas trades (limite={INACTIVITY_ALERT_H_CRYPTO}h)')
        try:
            if ALERTS_ENABLED:
                _send_whatsapp_direct(f'⚠️ INATIVIDADE: {hours_since_cry:.1f}h sem trades de crypto')
        except: pass
    elif hours_since_cry < INACTIVITY_ALERT_H_CRYPTO:
        _inactivity_alerted['crypto'] = False


# ── [v10.17] Flat Exit — detecta trades estagnadas e fecha para liberar capital ──
def is_trade_flat(trade: dict, now: datetime) -> bool:
    """[v10.17] Retorna True se a trade está flat (variação < threshold por tempo mínimo).
    Condições:
    - Idade > FLAT_EXIT_MIN_AGE_MIN
    - |pnl_pct| < FLAT_EXIT_MAX_VARIATION (quase zero)
    - Peak nunca passou de 0.5% (nunca teve momentum real)
    - Últimos 3 pontos de pnl_history ~iguais (sem tendência)
    [v10.46.6] NUNCA aplica flat exit em trade com tese V3 válida
    (regime=TRENDING + signal COMPRA/VENDA). Essas têm momentum esperado
    pela arquitetura e devem esperar stop/take_profit normal.
    [v10.46.7] Ampliado: qualquer signal_v2 decisivo (COMPRA/VENDA)
    tem tese v3 válida — v3 só emite signal quando há convergência
    mínima. Trades sem convicção ficam com signal_v2=MANTER.
    """
    # [v10.46.7] Pular flat exit se trade tem tese v3 decisiva
    signal_v2 = trade.get('signal_v2')
    if signal_v2 in ('COMPRA', 'VENDA'):
        return False  # tese v3 — deixar stop/TP/timeout decidir

    age_min = (now - datetime.fromisoformat(trade['opened_at'])).total_seconds() / 60
    # [v10.18] Min hold time for crypto — evitar flat exit prematuro
    _min_age = CRYPTO_MIN_HOLD_MIN if trade.get('asset_type') == 'crypto' else FLAT_EXIT_MIN_AGE_MIN
    if age_min < _min_age:
        return False
    pnl_pct = abs(float(trade.get('pnl_pct', 0) or 0))
    peak = abs(float(trade.get('peak_pnl_pct', 0) or 0))
    if pnl_pct > FLAT_EXIT_MAX_VARIATION:
        return False
    if peak > 0.5:  # teve momentum significativo em algum momento
        return False
    # Verificar se pnl_history mostra estagnação
    h = trade.get('pnl_history', [])
    if len(h) >= 3:
        recent = h[-3:]
        spread = max(recent) - min(recent)
        if spread > 0.3:  # houve movimento recente
            return False
    return True


# ── [v10.17] Directional Exposure Check ───────────────────────────────────
def check_v3_reversal(trade: dict, asset_type: str = 'crypto') -> tuple:
    """[v10.48] Reavalia v3 em trade aberta.
    Retorna (should_close: bool, new_regime: str, new_signal: str, reason_detail: str)

    Critérios para fechar (reversão de tese):
    - Signal virou MANTER (convicção se perdeu)
    - Signal virou oposto do da abertura (LONG com signal VENDA ou SHORT com signal COMPRA)
    - Regime saiu de TRENDING para RANGING/CHOPPY (tendência acabou)

    Se dados insuficientes ou v3 falha, retorna (False, None, None, 'data_insufficient') —
    não fecha em dúvida.
    """
    opened_signal = trade.get('signal_v2')
    opened_regime = trade.get('regime_v2')
    direction = trade.get('direction', 'LONG')
    if not opened_signal or opened_signal == 'MANTER':
        return False, None, None, 'no_v3_thesis'  # trade não tem tese v3, não reavaliar

    try:
        from modules.score_engine_v2 import compute_score_v3
        # Puxar dados de mercado atuais
        if asset_type == 'crypto':
            sym = trade['symbol'] + 'USDT'
            klines = _get_cached_candles(f'klines:{sym}', ttl_min=5) or {}  # [v10.49] 60→5min
            if not klines or len(klines.get('closes', [])) < 30:
                return False, None, None, 'crypto_no_klines'
            r = compute_score_v3(klines['closes'], klines.get('highs', []),
                                 klines.get('lows', []), klines.get('volumes', []),
                                 factor_stats_cache=factor_stats_cache,
                                 pattern_stats_cache=pattern_stats_cache)
        else:  # stock
            sym = trade['symbol']
            # [2026-04-20 deadlock fix] state_lock removido: esta função é
            # chamada apenas por monitor_trades (api_server.py:5811, :5910),
            # que já detém state_lock. threading.Lock() não é reentrante —
            # re-adquirir travava a thread. dict.get() é atômico no CPython.
            pd_data = stock_prices.get(sym, {})
            c = pd_data.get('closes_series', [])
            h = pd_data.get('highs_series', [])
            l = pd_data.get('lows_series', [])
            v = pd_data.get('volumes_series', [])
            if len(c) < 30:
                return False, None, None, 'stock_no_series'
            r = compute_score_v3(c, h, l, v,
                                 factor_stats_cache=factor_stats_cache,
                                 pattern_stats_cache=pattern_stats_cache)

        new_regime = r.get('regime')
        new_signal = r.get('signal')

        # Critério 1: signal virou MANTER (convicção se perdeu)
        if new_signal == 'MANTER':
            return True, new_regime, new_signal, 'signal_to_hold'

        # Critério 2: signal virou oposto (COMPRA→VENDA ou VENDA→COMPRA)
        # LONG aberto com signal=COMPRA: sair se virou VENDA (tendência inverteu)
        # SHORT aberto com signal=VENDA: sair se virou COMPRA
        if direction == 'LONG' and new_signal == 'VENDA':
            return True, new_regime, new_signal, 'signal_reversed_long_to_venda'
        if direction == 'SHORT' and new_signal == 'COMPRA':
            return True, new_regime, new_signal, 'signal_reversed_short_to_compra'

        # Critério 3: regime saiu de TRENDING (tendência acabou)
        if opened_regime == 'TRENDING' and new_regime != 'TRENDING':
            return True, new_regime, new_signal, f'trend_ended_{opened_regime}_to_{new_regime}'

        return False, new_regime, new_signal, 'thesis_valid'
    except Exception as e:
        log.debug(f"check_v3_reversal {trade.get('symbol')}: {e}")
        return False, None, None, f'exception:{str(e)[:40]}'


def check_directional_exposure(direction: str, strategy: str = 'stocks') -> tuple:
    """[v10.17] Verifica se abrir mais uma posição nesta direção excede o limite.
    Retorna (blocked: bool, reason: str, stats: dict).
    """
    with state_lock:
        if strategy == 'stocks':
            open_list = list(stocks_open)
        elif strategy == 'crypto':
            open_list = list(crypto_open)
        else:
            open_list = list(stocks_open) + list(crypto_open)
    total = len(open_list)
    if total < 3:  # com menos de 3 posições, não faz sentido limitar
        return False, 'OK', {'total': total, 'long': 0, 'short': 0}
    longs = sum(1 for t in open_list if t.get('direction') == 'LONG')
    shorts = sum(1 for t in open_list if t.get('direction') == 'SHORT')
    # Calcular percentual da direção pedida
    same_dir = longs if direction == 'LONG' else shorts
    pct = (same_dir / total) * 100 if total > 0 else 0
    stats = {'total': total, 'long': longs, 'short': shorts, 'same_dir_pct': round(pct, 1)}
    _dir_limit = MAX_DIRECTIONAL_PCT_CRYPTO if strategy == 'crypto' else MAX_DIRECTIONAL_PCT  # [v10.24.4]
    if pct > _dir_limit:  # [v10.24.5] > not >= so 100% limit allows all-same-direction
        return True, f'DIRECTIONAL_LIMIT ({direction}={same_dir}/{total}={pct:.0f}%>{_dir_limit:.0f}%)', stats
    return False, 'OK', stats


# ── [v10.17] Regime-aware sizing multiplier ───────────────────────────────
def get_regime_multiplier() -> tuple:
    """[v10.17] Retorna (size_mult, sl_mult, reason) baseado no regime de mercado.
    RANGING:  size menor (0.8x), SL mais tight (0.85x) — mean-reversion
    TRENDING: size maior (1.2x), SL mais largo (1.3x) — momentum
    HIGH_VOL: size menor (0.6x), SL mais largo (1.5x) — proteção
    NORMAL:   1.0x, 1.0x
    """
    mode = market_regime.get('mode', 'UNKNOWN')
    vol = market_regime.get('volatility', 'NORMAL')
    if mode == 'HIGH_VOL':
        return 0.6, 1.5, f'regime={mode} vol={vol}'
    elif mode == 'TRENDING':
        if vol == 'HIGH':
            return 1.0, 1.3, f'regime={mode} vol={vol}'
        return 1.2, 1.3, f'regime={mode} vol={vol}'
    elif mode == 'RANGING':
        return 0.8, 0.85, f'regime={mode} vol={vol}'
    return 1.0, 1.0, f'regime={mode} vol={vol}'


# ── [v10.17] Dynamic timeout per symbol ───────────────────────────────────
def get_dynamic_timeout_h(symbol: str, default_h: float) -> float:
    """[v10.17] Retorna timeout em horas baseado no histórico do símbolo.
    Se o símbolo tem histórico, usa avg_dur * DYNAMIC_TIMEOUT_MULT.
    Se não, usa o default.
    """
    if not DYNAMIC_TIMEOUT_ENABLED:
        return default_h
    stats = _symbol_avg_duration.get(symbol)
    if stats and stats.get('n', 0) >= 5:  # mínimo 5 trades para confiar
        avg_h = stats['avg_h']
        timeout = avg_h * DYNAMIC_TIMEOUT_MULT
        return max(DYNAMIC_TIMEOUT_MIN_H, min(DYNAMIC_TIMEOUT_MAX_H, timeout))
    return default_h


def update_symbol_duration(symbol: str, duration_h: float):
    """[v10.17] Atualiza a duração média do símbolo (EWMA)."""
    if symbol not in _symbol_avg_duration:
        _symbol_avg_duration[symbol] = {'sum_h': 0.0, 'n': 0, 'avg_h': duration_h}
    s = _symbol_avg_duration[symbol]
    s['sum_h'] += duration_h
    s['n'] += 1
    # EWMA com alpha=0.2 para reagir a mudanças recentes
    alpha = 0.2
    s['avg_h'] = round(alpha * duration_h + (1 - alpha) * s['avg_h'], 2)


# ── [v10.16] Trace ID por ciclo de worker ─────────────────────────────────
def gen_trace_id(worker_name: str) -> str:
    """[v10.16] Gera trace ID único para cada ciclo de worker."""
    return f'{worker_name[:3].upper()}-{uuid.uuid4().hex[:8]}'


# ── [v10.16] Settings Validation on Boot ──────────────────────────────────
def validate_settings_on_boot():
    """[v10.16] Valida todas as configurações críticas na inicialização.
    Loga config efetiva e aborta se houver conflito.
    """
    errors = []
    warnings = []
    # Validações de range
    if MAX_POSITIONS_CRYPTO < 1 or MAX_POSITIONS_CRYPTO > 20:
        errors.append(f'MAX_POSITIONS_CRYPTO={MAX_POSITIONS_CRYPTO} fora de range [1,20]')
    if MAX_POSITIONS_STOCKS < 1 or MAX_POSITIONS_STOCKS > 100:
        errors.append(f'MAX_POSITIONS_STOCKS={MAX_POSITIONS_STOCKS} fora de range [1,100]')
    if MIN_SCORE_AUTO < 50 or MIN_SCORE_AUTO > 95:
        errors.append(f'MIN_SCORE_AUTO={MIN_SCORE_AUTO} fora de range [50,95]')
    if MIN_SCORE_AUTO_CRYPTO < 40 or MIN_SCORE_AUTO_CRYPTO > 90:
        errors.append(f'MIN_SCORE_AUTO_CRYPTO={MIN_SCORE_AUTO_CRYPTO} fora de range [40,90]')
    if RISK_MULT_MIN >= RISK_MULT_MAX:
        errors.append(f'RISK_MULT_MIN={RISK_MULT_MIN} >= RISK_MULT_MAX={RISK_MULT_MAX}')
    if RISK_MULT_MIN < 0.1 or RISK_MULT_MAX > 3.0:
        errors.append(f'Risk multiplier range [{RISK_MULT_MIN},{RISK_MULT_MAX}] fora de limites seguros [0.1,3.0]')
    if MAX_DAILY_DRAWDOWN_PCT > 10:
        warnings.append(f'MAX_DAILY_DRAWDOWN_PCT={MAX_DAILY_DRAWDOWN_PCT}% é alto (>10%)')
    if KILL_SWITCH_USD < 1000:
        warnings.append(f'KILL_SWITCH_USD=${KILL_SWITCH_USD} é muito baixo')
    if STOCK_SL_PCT > 5 or STOCK_SL_PCT < 0.5:
        warnings.append(f'STOCK_SL_PCT={STOCK_SL_PCT}% fora de range recomendado [0.5,5.0]')
    if DAILY_DD_STOCKS_PCT > MAX_DAILY_DRAWDOWN_PCT:
        warnings.append(f'DAILY_DD_STOCKS_PCT={DAILY_DD_STOCKS_PCT}% > MAX_DAILY_DRAWDOWN_PCT={MAX_DAILY_DRAWDOWN_PCT}%')
    # Conflitos lógicos
    if MAX_OPEN_POSITIONS < MAX_POSITIONS_STOCKS:
        errors.append(f'MAX_OPEN_POSITIONS={MAX_OPEN_POSITIONS} < MAX_POSITIONS_STOCKS={MAX_POSITIONS_STOCKS}')
    # Log efetivo
    log.info('═══════════════════════════════════════════════════════════')
    log.info(f'[BOOT] Settings Validation v10.16')
    log.info(f'  Capital: Stocks ${INITIAL_CAPITAL_STOCKS:,.0f} | Crypto ${INITIAL_CAPITAL_CRYPTO:,.0f} | Arbi ${ARBI_CAPITAL:,.0f}')
    log.info(f'  Positions: Stocks max={MAX_POSITIONS_STOCKS} | Crypto max={MAX_POSITIONS_CRYPTO} | Global max={MAX_OPEN_POSITIONS}')
    log.info(f'  Thresholds: Stocks min_score={MIN_SCORE_AUTO} | Crypto min_score={MIN_SCORE_AUTO_CRYPTO}')
    log.info(f'  Risk: mult=[{RISK_MULT_MIN},{RISK_MULT_MAX}] | daily_dd={MAX_DAILY_DRAWDOWN_PCT}% | kill=${KILL_SWITCH_USD:,.0f}')
    log.info(f'  Strategy DD: stocks={DAILY_DD_STOCKS_PCT}% | crypto={DAILY_DD_CRYPTO_PCT}%')
    log.info(f'  SL/TP: stock_sl={STOCK_SL_PCT}% stock_tp={STOCK_TP_PCT}% | ATR mult stock={ATR_SL_MULTIPLIER_STOCK} crypto={ATR_SL_MULTIPLIER_CRYPTO}')
    log.info(f'  Blacklist: min_trades={BLACKLIST_MIN_TRADES} max_avg_pnl=${BLACKLIST_MAX_AVG_PNL} max_wr={BLACKLIST_MAX_WR}%')
    log.info(f'  Learning: enabled={LEARNING_ENABLED} ewma={LEARNING_EWMA_ALPHA} dead_zone=[{LEARNING_DEAD_ZONE_LOW},{LEARNING_DEAD_ZONE_HIGH}]')
    log.info(f'  Shadow: eval_window={SHADOW_EVAL_WINDOW_MIN}min')
    log.info(f'  Inactivity: stocks={INACTIVITY_ALERT_H_STOCKS}h crypto={INACTIVITY_ALERT_H_CRYPTO}h')
    log.info(f'  [v10.17] Flat exit: age>{FLAT_EXIT_MIN_AGE_MIN}min var<{FLAT_EXIT_MAX_VARIATION}%')
    log.info(f'  [v10.17] Trailing: stocks peak={TRAILING_PEAK_STOCKS}% drop={TRAILING_DROP_STOCKS}% | crypto peak={TRAILING_PEAK_CRYPTO}% drop={TRAILING_DROP_CRYPTO}%')
    log.info(f'  [v10.17] Directional limit: {MAX_DIRECTIONAL_PCT}%')
    log.info(f'  [v10.17] Dynamic timeout: enabled={DYNAMIC_TIMEOUT_ENABLED} mult={DYNAMIC_TIMEOUT_MULT} range=[{DYNAMIC_TIMEOUT_MIN_H},{DYNAMIC_TIMEOUT_MAX_H}]h')
    log.info(f'  [v10.17] Regime: {market_regime.get("mode","?")} vol={market_regime.get("volatility","?")}')

    log.info('═══════════════════════════════════════════════════════════')
    for w in warnings:
        log.warning(f'[BOOT-WARN] {w}')
    if errors:
        for e in errors:
            log.error(f'[BOOT-ERROR] {e}')
        if ENV == 'production':
            raise RuntimeError(f'[v10.16] Settings validation failed: {errors}')
        else:
            log.warning('[BOOT] Settings errors detected but ENV != production — continuing')


# ═══════════════════════════════════════════════════════════════
# [L-2] SIGNAL MEMORY — snapshot de cada sinal no DB
# ═══════════════════════════════════════════════════════════════

def record_signal_event(sig: dict, features: dict, feature_hash: str,
                         conf: dict, insight: str,
                         trade_id: str = None, order_id: str = None,
                         source_type: str = 'stock_signal_db',
                         existing_signal_id: str = None,
                         origin_signal_key: str = None) -> str:
    """[L-2][FIX-2][S2] Registra evento de sinal. Retorna signal_id.
    origin_signal_key: chave de origem do registro em market_signals (para dedup persistida).
    Se existing_signal_id for passado, faz UPDATE ao invés de INSERT.
    """
    global signal_events_count, LEARNING_DEGRADED
    if not LEARNING_ENABLED: return ''
    try:
        signal_id  = existing_signal_id or gen_id('SIG')
        dq_score   = features.get('_dq_score', 50)
        payload    = {k: v for k, v in sig.items()
                      if k not in ('payload_json',) and not isinstance(v, (list, dict))}
        payload.update(features)

        event = {
            'signal_id':               signal_id,
            'feature_hash':            feature_hash,
            'symbol':                  sig.get('symbol', ''),
            'asset_type':              sig.get('asset_type', 'stock'),
            'market_type':             sig.get('market_type', ''),
            'signal':                  sig.get('signal', ''),
            'raw_score':               float(sig.get('score', 50) or 50),
            'learning_confidence':     conf.get('final_confidence', 50),
            'confidence_band':         conf.get('confidence_band', 'MEDIUM'),
            'price':                   float(sig.get('price', 0) or 0),
            'signal_created_at':       datetime.utcnow().isoformat(),
            'market_regime_mode':      features.get('regime_mode', ''),
            'market_regime_volatility':features.get('volatility_bucket', ''),
            'market_open':             bool(sig.get('market_open', False)),
            'trade_open':              bool(sig.get('trade_open', False)),
            'rsi':                     float(sig.get('rsi', 50) or 50),
            'ema9':                    float(sig.get('ema9', 0) or 0),
            'ema21':                   float(sig.get('ema21', 0) or 0),
            'ema50':                   float(sig.get('ema50', 0) or 0),
            'rsi_bucket':              features.get('rsi_bucket', ''),
            'score_bucket':            features.get('score_bucket', ''),
            'change_pct_bucket':       features.get('change_pct_bucket', ''),
            'ema_alignment':           features.get('ema_alignment', ''),
            'volatility_bucket':       features.get('volatility_bucket', ''),
            'weekday':                 features.get('weekday', 0),
            'time_bucket':             features.get('time_bucket', ''),
            'data_quality_score':      dq_score,
            'source_type':             source_type,
            'payload_json':            json.dumps(payload, default=str),
            'insight_summary':         insight,
            'learning_version':        LEARNING_VERSION,
            'origin_signal_key':       origin_signal_key,   # [S2] chave de dedup persistida
            'trade_id':                trade_id,
            'order_id':                order_id,
            'outcome_status':          None,
            'outcome_pnl':             None,
            'outcome_pnl_pct':         None,
            'outcome_close_reason':    None,
            'updated_at':              datetime.utcnow().isoformat(),
        }
        # [v10.3.2-P0-1] Se for reavaliação (existing_signal_id vem do cache), gravar SÍNCRONO
        # para garantir que o signal_id retornado é o real do banco — usado em seguida pelo
        # update_signal_attribution() para vincular trade_id/order_id corretamente.
        if existing_signal_id:
            confirmed_id = _db_save_signal_event(event)
            LEARNING_DEGRADED = False
            if not confirmed_id:
                # [v10.3.3-F4] Banco falhou ou oscilou — logar explicitamente.
                # Retornar existing_signal_id (o tentado) para não deixar trade sem referência,
                # mas sinalizar que a atribuição pode estar inconsistente.
                log.warning(f'record_signal_event: banco não confirmou signal_id {existing_signal_id} '
                            f'(origin_key={origin_signal_key}). Atribuição pode estar inconsistente.')
                return existing_signal_id
            return confirmed_id
        enqueue_persist('signal_event', event)
        # [P0-3] Não incrementar aqui — o contador é gerenciado por _db_save_signal_event
        # via ROW_COUNT, que distingue insert real de ON DUPLICATE KEY UPDATE.
        LEARNING_DEGRADED = False
        return signal_id
    except Exception as e:
        log.error(f'record_signal_event: {e}')
        return ''

def update_signal_attribution(signal_id: str, trade_id: str, order_id: str):
    """[FIX-2][v10.15] Vincula trade_id/order_id ao signal_event — crítico para calibração."""
    if not LEARNING_ENABLED or not signal_id: return
    log.info(f'[ML-ATTRIB] {signal_id} → trade={trade_id} order={order_id}')
    try:
        update = {
            'signal_id': signal_id,
            'trade_id':  trade_id,
            'order_id':  order_id,
            'updated_at':datetime.utcnow().isoformat(),
        }
        enqueue_persist('signal_attribution', update)
    except Exception as e:
        log.error(f'update_signal_attribution: {e}')

def update_signal_outcome(signal_id: str, trade_id: str, order_id: str,
                           pnl: float, pnl_pct: float, close_reason: str):
    """[L-7] Vincula outcome de trade ao evento de sinal original."""
    if not LEARNING_ENABLED or not signal_id: return
    try:
        update = {
            'signal_id':           signal_id,
            'trade_id':            trade_id,
            'order_id':            order_id,
            'outcome_status':      'WIN' if pnl_pct > 0.1 else ('LOSS' if pnl_pct < -0.1 else 'FLAT'),
            'outcome_pnl':         round(pnl, 4),
            'outcome_pnl_pct':     round(pnl_pct, 4),
            'outcome_close_reason':close_reason,
            'updated_at':          datetime.utcnow().isoformat(),
        }
        enqueue_persist('signal_outcome', update)
    except Exception as e:
        log.error(f'update_signal_outcome: {e}')

# ═══════════════════════════════════════════════════════════════
# [L-8] SHADOW LEARNING — decisões hipotéticas
# ═══════════════════════════════════════════════════════════════

def record_shadow_decision(signal_id: str, sig: dict, reason: str):
    """[L-8] Registra sinal observado mas não executado."""
    if not LEARNING_ENABLED: return
    try:
        shadow = {
            'shadow_id':         gen_id('SHD'),
            'signal_id':         signal_id,
            'symbol':            sig.get('symbol', ''),
            'signal':            sig.get('signal', ''),
            'price_at_signal':   float(sig.get('price', 0) or 0),
            'not_executed_reason':reason,
            'hypothetical_entry':float(sig.get('price', 0) or 0),
            'evaluation_status': 'PENDING',
            'created_at':        datetime.utcnow().isoformat(),
            'payload_json':      json.dumps({'score': sig.get('score'), 'reason': reason}, default=str),
        }
        enqueue_persist('shadow_decision', shadow)
    except Exception as e:
        log.error(f'record_shadow_decision: {e}')

# ═══════════════════════════════════════════════════════════════
# [L-2/L-3] PERSIST HELPERS para learning tables
# ═══════════════════════════════════════════════════════════════

def _db_save_signal_event(event: dict):
    """[L-2][P0-3] Persiste signal_event.
    ROW_COUNT() = 1 → insert real → incrementa signal_events_count.
    ROW_COUNT() = 2 → ON DUPLICATE KEY UPDATE → não incrementa (já contado).
    """
    global signal_events_count
    conn = get_db()
    if not conn: return
    cursor=None
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO signal_events (
            signal_id, feature_hash, symbol, asset_type, market_type, signal_type, raw_score,
            learning_confidence, confidence_band, price, signal_created_at,
            market_regime_mode, market_regime_volatility, market_open, trade_open,
            rsi, ema9, ema21, ema50, rsi_bucket, score_bucket, change_pct_bucket,
            ema_alignment, volatility_bucket, weekday, time_bucket, data_quality_score,
            source_type, payload_json, insight_summary, learning_version,
            origin_signal_key,
            trade_id, order_id, outcome_status, outcome_pnl, outcome_pnl_pct,
            outcome_close_reason, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                trade_id=COALESCE(VALUES(trade_id), trade_id),
                order_id=COALESCE(VALUES(order_id), order_id),
                learning_confidence=VALUES(learning_confidence),
                confidence_band=VALUES(confidence_band),
                insight_summary=VALUES(insight_summary),
                payload_json=VALUES(payload_json),
                updated_at=VALUES(updated_at)""",
            (event['signal_id'], event['feature_hash'], event['symbol'],
             event['asset_type'], event['market_type'], event.get('signal',''),
             event['raw_score'], event['learning_confidence'], event['confidence_band'],
             event['price'], event['signal_created_at'], event['market_regime_mode'],
             event['market_regime_volatility'], event['market_open'], event['trade_open'],
             event['rsi'], event['ema9'], event['ema21'], event['ema50'],
             event['rsi_bucket'], event['score_bucket'], event['change_pct_bucket'],
             event['ema_alignment'], event['volatility_bucket'], event['weekday'],
             event['time_bucket'], event['data_quality_score'], event['source_type'],
             event['payload_json'], event['insight_summary'], event['learning_version'],
             event.get('origin_signal_key'),
             event['trade_id'], event['order_id'], event['outcome_status'],
             event['outcome_pnl'], event['outcome_pnl_pct'],
             event['outcome_close_reason'], event['updated_at']))
        # [P0-3] ROW_COUNT=1 → insert real; ROW_COUNT=2 → duplicate key update (signal_id PK)
        # ROW_COUNT=0 pode acontecer quando o ON DUPLICATE KEY não altera nenhuma coluna (valores iguais)
        row_count = c.rowcount
        # [v10.3.2-P0-1] Se houve conflito por origin_signal_key (UNIQUE), o sinal_id real
        # pode ser diferente do que foi tentado inserir. Buscar o ID real do banco.
        real_signal_id = event['signal_id']
        # [v10.3.3-F1] cursor com dictionary=True para acessar row['signal_id'] sem KeyError/TypeError
        if row_count != 1 and event.get('origin_signal_key'):
            try:
                c2 = conn.cursor(dictionary=True)
                c2.execute("SELECT signal_id FROM signal_events WHERE origin_signal_key=%s LIMIT 1",
                           (event['origin_signal_key'],))
                row = c2.fetchone()
                c2.close()
                if row:
                    real_signal_id = row['signal_id']
            except Exception: pass
        conn.commit(); c.close(); conn.close()
        if row_count == 1:
            with learning_lock:
                signal_events_count += 1
        return real_signal_id
    except Exception as e:
        log.error(f'_db_save_signal_event: {e}')

def _db_update_signal_attribution(upd: dict):
    """[FIX-2] Vincula trade_id/order_id ao signal_event existente."""
    conn = get_db()
    if not conn: return
    cursor=None
    try:
        c = conn.cursor()
        c.execute("""UPDATE signal_events
                     SET trade_id=%s, order_id=%s, updated_at=%s
                     WHERE signal_id=%s""",
                  (upd['trade_id'], upd['order_id'], upd['updated_at'], upd['signal_id']))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        log.error(f'_db_update_signal_attribution: {e}')

def _db_update_signal_outcome(upd: dict):
    conn = get_db()
    if not conn: return
    cursor=None
    try:
        c = conn.cursor()
        c.execute("""UPDATE signal_events SET
            trade_id=%s, order_id=%s, outcome_status=%s, outcome_pnl=%s,
            outcome_pnl_pct=%s, outcome_close_reason=%s, updated_at=%s
            WHERE signal_id=%s""",
            (upd['trade_id'], upd['order_id'], upd['outcome_status'],
             upd['outcome_pnl'], upd['outcome_pnl_pct'],
             upd['outcome_close_reason'], upd['updated_at'],
             upd['signal_id']))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        log.error(f'_db_update_signal_outcome: {e}')

def _db_upsert_pattern_stats(s: dict):
    conn = get_db()
    if not conn: return
    cursor=None
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO pattern_stats (
            feature_hash, total_samples, wins, losses, flat_count,
            avg_pnl, avg_pnl_pct, ewma_pnl_pct, ewma_hit_rate,
            expectancy, downside_score, max_loss_seen, confidence_weight,
            last_seen_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
            total_samples=VALUES(total_samples), wins=VALUES(wins),
            losses=VALUES(losses), flat_count=VALUES(flat_count),
            avg_pnl=VALUES(avg_pnl), avg_pnl_pct=VALUES(avg_pnl_pct),
            ewma_pnl_pct=VALUES(ewma_pnl_pct), ewma_hit_rate=VALUES(ewma_hit_rate),
            expectancy=VALUES(expectancy), downside_score=VALUES(downside_score),
            max_loss_seen=VALUES(max_loss_seen), confidence_weight=VALUES(confidence_weight),
            last_seen_at=VALUES(last_seen_at), updated_at=VALUES(updated_at)""",
            (s['feature_hash'], s['total_samples'], s['wins'], s['losses'], s['flat_count'],
             s['avg_pnl'], s['avg_pnl_pct'], s['ewma_pnl_pct'], s['ewma_hit_rate'],
             s['expectancy'], s['downside_score'], s['max_loss_seen'], s['confidence_weight'],
             s['last_seen_at'], s['updated_at']))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        log.error(f'_db_upsert_pattern_stats: {e}')

def _db_upsert_factor_stats(s: dict):
    conn = get_db()
    if not conn: return
    cursor=None
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO factor_stats (
            factor_type, factor_value, total_samples, wins, losses,
            avg_pnl_pct, ewma_pnl_pct, expectancy, downside_score,
            confidence_weight, last_seen_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
            total_samples=VALUES(total_samples), wins=VALUES(wins),
            losses=VALUES(losses), avg_pnl_pct=VALUES(avg_pnl_pct),
            ewma_pnl_pct=VALUES(ewma_pnl_pct), expectancy=VALUES(expectancy),
            downside_score=VALUES(downside_score), confidence_weight=VALUES(confidence_weight),
            last_seen_at=VALUES(last_seen_at), updated_at=VALUES(updated_at)""",
            (s['factor_type'], s['factor_value'], s['total_samples'], s['wins'], s['losses'],
             s['avg_pnl_pct'], s['ewma_pnl_pct'], s['expectancy'], s['downside_score'],
             s['confidence_weight'], s['last_seen_at'], s['updated_at']))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        log.error(f'_db_upsert_factor_stats: {e}')

def _db_save_ledger_event(evt: dict):
    """[v10.18] Persiste evento do capital ledger no MySQL."""
    conn = get_db()
    if not conn: return
    cursor=None
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO capital_ledger (ts, strategy, event, symbol, amount, balance_after, trade_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (evt.get('ts'), evt.get('strategy'), evt.get('event'),
             evt.get('symbol',''), evt.get('amount',0), evt.get('balance_after',0),
             evt.get('trade_id','')))
        conn.commit(); c.close()
    except Exception as e:
        log.error(f'_db_save_ledger_event: {e}')
    finally:
        conn.close()

def _db_save_shadow_decision(shadow: dict):
    conn = get_db()
    if not conn: return
    cursor=None
    try:
        c = conn.cursor()
        c.execute("""INSERT IGNORE INTO shadow_decisions (
            shadow_id, signal_id, symbol, signal_type, price_at_signal,
            not_executed_reason, hypothetical_entry, evaluation_status,
            created_at, payload_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (shadow['shadow_id'], shadow['signal_id'], shadow['symbol'],
             shadow.get('signal_type', shadow.get('signal','')), shadow['price_at_signal'],
             shadow['not_executed_reason'], shadow['hypothetical_entry'],
             shadow['evaluation_status'], shadow['created_at'], shadow['payload_json']))
        c.close()
    except Exception as e:
        log.error(f'_db_save_shadow_decision: {e}')
    finally:
        conn.close()   # [v10.7] sempre devolve ao pool

# =========================================================
# [v3.0] Brain Persistence Helpers — brain never forgets
# =========================================================

def _get_brain_engine():
    """Get the brain learning engine from the registered blueprint."""
    try:
        bp = app.blueprints.get('unified_brain')
        if bp and hasattr(bp, 'deferred_functions'):
            # Engine is on the blueprint module scope; access via the factory's closure
            pass
        # Fallback: create a quick engine instance sharing the same DB
        from modules.unified_brain.learning_engine import LearningEngine
        if not hasattr(_get_brain_engine, '_instance'):
            _get_brain_engine._instance = LearningEngine(
                db_fn=lambda: get_db(),
                log=log
            )
        return _get_brain_engine._instance
    except Exception as e:
        log.error(f'[Brain] _get_brain_engine: {e}')
        return None

def _brain_persist_lesson(data: dict):
    """Persist a brain lesson via the learning engine."""
    engine = _get_brain_engine()
    if engine:
        engine.persist_lesson(
            module=data.get('module', 'Unknown'),
            lesson_type=data.get('lesson_type', 'Trade_Outcome'),
            description=data.get('description', ''),
            impact_score=data.get('impact_score', 7.0),
            confidence=data.get('confidence', 70.0),
            strategy=data.get('strategy', ''),
            data_json=data.get('data_json', {})
        )

def _brain_persist_decision(data: dict):
    """Persist a brain decision via the learning engine."""
    engine = _get_brain_engine()
    if engine:
        engine.persist_decision(
            decision_type=data.get('decision_type', 'MONITOR'),
            module=data.get('module', 'Unknown'),
            recommendation=data.get('recommendation', ''),
            reasoning=data.get('reasoning', ''),
            confidence=data.get('confidence', 70.0),
            factors=data.get('factors', {})
        )

def _brain_persist_pattern(data: dict):
    """Persist a brain pattern via the learning engine."""
    engine = _get_brain_engine()
    if engine:
        engine.persist_pattern(
            pattern_type=data.get('pattern_type', 'Unknown'),
            description=data.get('description', ''),
            modules_involved=data.get('modules_involved', []),
            correlation=data.get('correlation', 0.7),
            confidence=data.get('confidence', 75.0)
        )

def _brain_persist_regime(data: dict):
    """Persist market regime change via the learning engine."""
    engine = _get_brain_engine()
    if engine:
        engine.persist_regime(
            regime_type=data.get('regime_type', 'UNKNOWN'),
            confidence=data.get('confidence', 80.0),
            indicators=data.get('indicators', {}),
            duration_days=data.get('duration_days', 1),
            module_signals=data.get('module_signals', {})
        )

def enqueue_brain_lesson(module: str, description: str, **kwargs):
    """Helper to enqueue a brain lesson for persistence (non-blocking)."""
    try:
        data = {'module': module, 'description': description}
        data.update(kwargs)
        urgent_queue.put((3, next(_urgent_seq), {'kind': 'brain_lesson', 'data': data}))
    except Exception as e:
        log.error(f'enqueue_brain_lesson: {e}')

def enqueue_brain_decision(decision_type: str, module: str, recommendation: str, **kwargs):
    """Helper to enqueue a brain decision for persistence (non-blocking)."""
    try:
        data = {'decision_type': decision_type, 'module': module, 'recommendation': recommendation}
        data.update(kwargs)
        urgent_queue.put((3, next(_urgent_seq), {'kind': 'brain_decision', 'data': data}))
    except Exception as e:
        log.error(f'enqueue_brain_decision: {e}')

# =========================================================

def _db_log_learning_audit(event_type: str, entity_id: str, payload: dict):
    conn = get_db()
    if not conn: return
    cursor=None
    try:
        c = conn.cursor()
        c.execute("INSERT INTO learning_audit (event_type, entity_id, payload_json) VALUES (%s,%s,%s)",
                  (event_type, entity_id, json.dumps(payload, default=str)))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        log.error(f'_db_log_learning_audit: {e}')

# ═══════════════════════════════════════════════════════════════
# [L-3] LEARNING UPDATE — chamado quando trade fecha
# ═══════════════════════════════════════════════════════════════

def _update_cross_market_from_stocks():
    """[v10.13] Atualiza estado cross-market baseado em trades de stocks do dia."""
    try:
        from datetime import date
        today = date.today().isoformat()
        yesterday = (date.today() - __import__('datetime').timedelta(days=1)).isoformat()
        with state_lock:
            today_trades   = [t for t in stocks_closed if (t.get('closed_at','') or '')[:10] == today]
            yest_trades    = [t for t in stocks_closed if (t.get('closed_at','') or '')[:10] == yesterday]
        def calc(trades_list):
            n=len(trades_list); wins=sum(1 for t in trades_list if float(t.get('pnl',0) or 0)>0)
            pnl=sum(float(t.get('pnl',0) or 0) for t in trades_list)
            return n, wins/n*100 if n else 50.0, pnl
        tn,twr,tpnl = calc(today_trades)
        yn,ywr,ypnl = calc(yest_trades)
        # BTC como proxy de sentimento crypto
        btc_chg = 0.0
        with state_lock:
            # [v10.14-FIX] crypto_prices contém float, não dict — usar crypto_momentum
            btc_chg = float(crypto_momentum.get('BTCUSDT', 0) or 0)
        # USDBRL
        usdbrl_chg = 0.0
        try:
            import requests
            r = requests.get('https://frankfurter.app/latest?from=USD&to=BRL', timeout=3)
            if r.ok:
                rate = r.json().get('rates',{}).get('BRL', 0)
                if rate > 0:
                    # Comparar com valor anterior em memória
                    prev = _cross_market_state.get('_usdbrl_prev', rate)
                    usdbrl_chg = (rate - prev) / prev * 100
                    _cross_market_state['_usdbrl_prev'] = rate
                    _cross_market_state['_usdbrl_rate'] = rate
        except: pass
        update_cross_market_state({
            'stocks_wr_today': twr, 'stocks_pnl_today': tpnl, 'stocks_n_today': tn,
            'stocks_wr_yesterday': ywr, 'stocks_pnl_yesterday': ypnl, 'stocks_n_yesterday': yn,
            'usdbrl_change_pct': usdbrl_chg, 'btc_change_24h': btc_chg,
        })
        log.debug(f"CROSS_MKT: stocks_today WR={twr:.1f}% n={tn} | BTC_chg={btc_chg:+.1f}% | USDBRL={usdbrl_chg:+.1f}%")
    except Exception as e:
        log.warning(f"_update_cross_market_from_stocks: {e}")

def process_trade_outcome(trade: dict):
    """[L-7][FIX-4] Processa fechamento de trade e atualiza aprendizado.
    Se _features não estiver em memória (pós-restart), reconstrói do features_json salvo no banco.
    """
    global LEARNING_DEGRADED, learning_errors
    if not LEARNING_ENABLED: return
    try:
        pnl       = trade.get('pnl', 0)
        pnl_pct   = trade.get('pnl_pct', 0)
        sig_id    = trade.get('signal_id', '')
        feat_hash = trade.get('feature_hash', '')

        # [FIX-4] Reconstituir features — preferência: memória → features_json do trade → None
        features = trade.get('_features')
        if not features and trade.get('features_json'):
            try:
                features = json.loads(trade['features_json'])
                log.debug(f'process_trade_outcome: features reconstituídas do features_json ({trade.get("id")})')
            except Exception as e:
                log.warning(f'process_trade_outcome: falha ao parse features_json: {e}')
                features = None

        if feat_hash:
            ps = update_pattern_stats(feat_hash, pnl, pnl_pct)
            enqueue_persist('pattern_stats', ps)

        if features:
            update_factor_stats(features, pnl, pnl_pct)
            # [v10.5-4] Persistir TODOS os fatores — antes só 6 eram salvos no banco.
            # atr_bucket, volume_bucket, time_bucket, weekday, asset_type, market_type, dq_bucket
            # agora persistem junto. Shadow learning e restart usam cache completo.
            ALL_FACTOR_KEYS = [
                'score_bucket', 'rsi_bucket', 'ema_alignment',
                'volatility_bucket', 'regime_mode', 'direction',
                'atr_bucket', 'volume_bucket',          # [v10.5-4]
                'time_bucket', 'weekday',               # [v10.5-4]
                'asset_type', 'market_type', 'dq_bucket',  # [v10.5-4]
            ]
            with learning_lock:
                for ftype in ALL_FACTOR_KEYS:
                    fval = str(features.get(ftype, ''))
                    if fval:
                        fs_copy = dict(factor_stats_cache.get((ftype, fval), {}))
                        if fs_copy:
                            enqueue_persist('factor_stats', fs_copy)
        elif feat_hash:
            log.debug(f'process_trade_outcome: sem features para {trade.get("id")} — só pattern_stats atualizado')

        if sig_id:
            update_signal_outcome(sig_id, trade.get('id',''), trade.get('order_id',''),
                                   pnl, pnl_pct, trade.get('close_reason',''))
        # [v10.15] Calibração contínua
        try: track_calibration(trade)
        except: pass

        # [v10.13] Atualizar padrões compostos descobertos automaticamente
        if features:
            try:
                update_composite_pattern(features, pnl, pnl_pct)
            except Exception as ce:
                log.debug(f"update_composite_pattern: {ce}")

        LEARNING_DEGRADED = False
        learning_errors = max(0, learning_errors - 1)   # decrementa ao sucesso
    except Exception as e:
        log.error(f'process_trade_outcome: {e}')
        learning_errors += 1
        if learning_errors >= 5:
            LEARNING_DEGRADED = True
            log.warning('Learning engine em modo degradado após 5 erros consecutivos')

# ═══════════════════════════════════════════════════════════════
# [v10.13] PATTERN DISCOVERY ENGINE — mineração automática
# Cruza todas as dimensões capturadas, descobre padrões novos
# e retroalimenta o score com confiança estatística
# ═══════════════════════════════════════════════════════════════

# Cache de padrões compostos descobertos automaticamente
_composite_patterns: dict = {}   # chave_composta → stats
_pattern_discovery_lock = threading.Lock()
_last_discovery_run: str = ''

# Dimensões disponíveis para cruzamento — cresce com novos dados
DISCOVERY_DIMENSIONS = [
    # Primárias — já capturadas no feature_hash atual
    'score_bucket', 'rsi_bucket', 'ema_alignment', 'volatility_bucket',
    'regime_mode', 'time_bucket', 'weekday', 'asset_type', 'market_type',
    'atr_bucket', 'volume_bucket', 'direction', 'dq_bucket',
    # Temporais — extraídas do horário de abertura
    'hour_utc', 'is_market_open_hour',
    # Cross-market — estado dos outros mercados no momento do sinal
    'stocks_regime',      # bom/ruim/neutro baseado em WR do dia
    'crypto_regime',      # idem
    'btc_trend',          # BTC subindo/caindo
    'usdbrl_trend',       # dólar subindo/caindo
    # Símbolo — padrões por ativo específico
    'symbol_bucket',      # top_performer / normal / underperformer
    # [v10.14] Arbi — dimensões específicas de arbitragem
    'arbi_pair',          # pair_id específico (PETR4-PBR, CSN, etc.)
    'arbi_spread_zone',   # HIGH/MID/LOW/MINIMAL baseado em spread de entrada
    'arbi_direction',     # LONG_A / LONG_B
    # [v10.14] Comportamentais — padrões que o sistema deve aprender sozinho
    'reentry_after_trailing',  # YES/NO — re-entrada após trailing stop no mesmo dia
    'same_day_count',          # 1st/2nd/3rd+ — quantas vezes o símbolo foi operado hoje
    'close_reason_prev',       # TRAILING_STOP/STOP_LOSS/TIMEOUT — motivo da trade anterior
]

# Combinações de 2 e 3 dimensões a explorar automaticamente
DISCOVERY_COMBOS_2D = [
    ('weekday', 'time_bucket'),
    ('weekday', 'market_type'),
    ('weekday', 'rsi_bucket'),
    ('weekday', 'ema_alignment'),
    ('weekday', 'volatility_bucket'),
    ('time_bucket', 'market_type'),
    ('time_bucket', 'rsi_bucket'),
    ('time_bucket', 'ema_alignment'),
    ('time_bucket', 'volatility_bucket'),
    ('market_type', 'rsi_bucket'),
    ('market_type', 'ema_alignment'),
    ('market_type', 'volatility_bucket'),
    ('rsi_bucket', 'ema_alignment'),
    ('rsi_bucket', 'atr_bucket'),
    ('rsi_bucket', 'volume_bucket'),
    ('ema_alignment', 'volatility_bucket'),
    ('ema_alignment', 'atr_bucket'),
    ('score_bucket', 'rsi_bucket'),
    ('score_bucket', 'ema_alignment'),
    ('score_bucket', 'volatility_bucket'),
    ('asset_type', 'weekday'),
    ('asset_type', 'time_bucket'),
    ('direction', 'weekday'),
    ('direction', 'time_bucket'),
    ('reentry_after_trailing', 'direction'),
    ('reentry_after_trailing', 'weekday'),
    ('same_day_count', 'direction'),
    ('close_reason_prev', 'direction'),
    # [v10.14] Combos comportamentais — detecta padrões que destroem P&L
    ('reentry_after_trailing', 'direction'),
    ('reentry_after_trailing', 'asset_type'),
    ('same_day_count', 'direction'),
    ('same_day_count', 'close_reason_prev'),
    ('close_reason_prev', 'direction'),
    ('close_reason_prev', 'time_bucket'),
    ('direction', 'volatility_bucket'),
    ('stocks_regime', 'asset_type'),      # cross-market
    ('btc_trend', 'asset_type'),          # BTC → crypto/stocks
    ('usdbrl_trend', 'market_type'),      # FX → B3/NYSE
]

DISCOVERY_COMBOS_3D = [
    ('weekday', 'time_bucket', 'market_type'),
    ('weekday', 'time_bucket', 'rsi_bucket'),
    ('weekday', 'rsi_bucket', 'ema_alignment'),
    ('weekday', 'market_type', 'volatility_bucket'),
    ('time_bucket', 'rsi_bucket', 'ema_alignment'),
    ('time_bucket', 'market_type', 'rsi_bucket'),
    ('rsi_bucket', 'ema_alignment', 'volatility_bucket'),
    ('score_bucket', 'rsi_bucket', 'ema_alignment'),
    ('asset_type', 'weekday', 'time_bucket'),
    ('asset_type', 'weekday', 'volatility_bucket'),
    ('direction', 'weekday', 'volatility_bucket'),
    ('market_type', 'weekday', 'rsi_bucket'),
    ('stocks_regime', 'weekday', 'asset_type'),   # 3D cross-market
    ('btc_trend', 'weekday', 'asset_type'),
]

def _enrich_features_for_discovery(features: dict) -> dict:
    """[v10.13] Adiciona dimensões cross-market e temporais ao feature dict para mineração."""
    enriched = dict(features)
    try:
        # Hora UTC real
        now = datetime.utcnow()
        enriched['hour_utc'] = str(now.hour)
        enriched['is_market_open_hour'] = 'YES' if 13 <= now.hour <= 19 else 'NO'
        # Estado dos mercados cruzados
        cm = _cross_market_state
        sw = cm.get('stocks_wr_today', 50.0)
        enriched['stocks_regime'] = 'BAD' if sw < 45 else ('GOOD' if sw >= 58 else 'NEUTRAL')
        btc = cm.get('btc_change_24h', 0.0)
        enriched['btc_trend'] = 'UP' if btc > 2 else ('DOWN' if btc < -2 else 'FLAT')
        fx = cm.get('usdbrl_change_pct', 0.0)
        enriched['usdbrl_trend'] = 'UP' if fx > 0.5 else ('DOWN' if fx < -0.5 else 'FLAT')
        # Regime de crypto (baseado em WR do dia se disponível)
        # Simplificado: usa btc_trend como proxy
        enriched['crypto_regime'] = enriched['btc_trend']
    except: pass
    return enriched

def _make_composite_key(features: dict, dims: tuple) -> str:
    """Gera chave composta para combinação de dimensões."""
    parts = [f"{d}={features.get(d, '?')}" for d in dims]
    return '|'.join(parts)

def _empty_composite_stats(key: str, dims: tuple) -> dict:
    return {
        'key': key, 'dims': '×'.join(dims),
        'total_samples': 0, 'wins': 0, 'losses': 0,
        'avg_pnl_pct': 0.0, 'ewma_hit_rate': 0.5,
        'expectancy': 0.0, 'confidence_weight': 0.0,
        'score_adj': 0,   # ajuste de score calculado
        'reliable': False, 'blocked': False,
        'last_seen': '', 'updated': '',
    }

def update_composite_pattern(features: dict, pnl: float, pnl_pct: float):
    """[v10.13] Atualiza todos os padrões compostos relevantes para este trade."""
    if not LEARNING_ENABLED: return
    enriched = _enrich_features_for_discovery(features)
    win = 1 if pnl > 0 else 0
    alpha = 0.15
    now_iso = datetime.utcnow().isoformat()

    with _pattern_discovery_lock:
        all_combos = DISCOVERY_COMBOS_2D + DISCOVERY_COMBOS_3D
        for dims in all_combos:
            # Verificar se todas as dimensões estão disponíveis
            if not all(enriched.get(d) for d in dims): continue
            key = _make_composite_key(enriched, dims)
            s = _composite_patterns.get(key) or _empty_composite_stats(key, dims)

            s['total_samples'] += 1
            s['wins'] += win
            s['losses'] += (1 - win)
            s['avg_pnl_pct'] = (s['avg_pnl_pct'] * (s['total_samples']-1) + pnl_pct) / s['total_samples']
            s['ewma_hit_rate'] = alpha * win + (1 - alpha) * s['ewma_hit_rate']
            # Expectancy = E[pnl_pct] ajustado pelo EWMA
            s['expectancy'] = round(s['ewma_hit_rate'] * s['avg_pnl_pct'] - (1-s['ewma_hit_rate']) * abs(s['avg_pnl_pct']), 4)
            n = s['total_samples']
            wr = s['wins'] / n
            # confidence_weight: [-1, +1] — positivo = padrão confiável
            s['confidence_weight'] = round(2 * wr - 1, 4)
            # Score adjustment: quanto este padrão deve ajustar o score (±15 pts)
            if n >= 15:
                if wr >= 0.75: s['score_adj'] = +15
                elif wr >= 0.65: s['score_adj'] = +10
                elif wr >= 0.58: s['score_adj'] = +5
                elif wr >= 0.48: s['score_adj'] = 0
                elif wr >= 0.38: s['score_adj'] = -8
                else: s['score_adj'] = -15
            # Marcar como confiável ou a bloquear
            s['reliable'] = (n >= 20 and wr >= 0.70 and s['ewma_hit_rate'] >= 0.65)
            s['blocked']  = (n >= 20 and wr < 0.38 and s['ewma_hit_rate'] < 0.45)
            s['last_seen'] = now_iso
            s['updated'] = now_iso
            _composite_patterns[key] = s

def get_composite_score_adj(features: dict) -> tuple:
    """
    [v10.13] Consulta todos os padrões compostos e retorna o ajuste de score agregado.
    Retorna (score_adj, blocked, best_pattern_key).
    Prioriza padrões com mais amostras e maior confiança.
    """
    if not _composite_patterns or not LEARNING_ENABLED:
        return 0, False, ''

    enriched = _enrich_features_for_discovery(features)
    adj_total = 0
    blocked = False
    best_key = ''
    best_n = 0
    count = 0

    with _pattern_discovery_lock:
        all_combos = DISCOVERY_COMBOS_2D + DISCOVERY_COMBOS_3D
        for dims in all_combos:
            if not all(enriched.get(d) for d in dims): continue
            key = _make_composite_key(enriched, dims)
            s = _composite_patterns.get(key)
            if not s or s['total_samples'] < 10: continue

            # Bloquear se este padrão é consistentemente ruim
            if s['blocked']:
                blocked = True
                best_key = key
                break

            # Peso pelo número de amostras (mais amostras = mais confiável)
            weight = min(s['total_samples'] / 50.0, 1.0)
            adj_total += s['score_adj'] * weight
            count += 1
            if s['total_samples'] > best_n:
                best_n = s['total_samples']
                best_key = key

    if count > 0:
        # Média ponderada, limitada a ±20 pts
        adj_total = max(-20, min(+20, int(adj_total / count)))

    return adj_total, blocked, best_key

def run_pattern_discovery():
    """
    [v10.13] Mineração periódica de padrões do banco — roda em background.
    Analisa trades históricas e alimenta _composite_patterns com dados passados.
    """
    global _last_discovery_run
    if not LEARNING_ENABLED: return
    try:
        beat('pattern_discovery')  # heartbeat antes de começar
        conn = get_db()
        if not conn: return
        cursor = conn.cursor(dictionary=True)
        # Buscar trades fechadas com features_json
        # Stocks e crypto
        cursor.execute("""
            SELECT t.pnl, t.pnl_pct, t.features_json, t.asset_type, t.market,
                   t.opened_at, t.closed_at, t.close_reason, t.symbol,
                   t.score, NULL as pair_id, NULL as entry_spread
            FROM trades t
            WHERE t.status='CLOSED' AND t.pnl IS NOT NULL
            ORDER BY t.closed_at DESC
            LIMIT 200000
        """)
        rows = list(cursor.fetchall())

        # [v10.14-FIX] Incluir arbi_trades com features sintéticas
        try:
            cursor.execute("""
                SELECT pnl, pnl_pct, NULL as features_json, 'arbitrage' as asset_type,
                       'ARBI' as market, opened_at, closed_at, close_reason,
                       pair_id as symbol, NULL as score, pair_id, entry_spread
                FROM arbi_trades
                WHERE status='CLOSED' AND pnl IS NOT NULL
                ORDER BY closed_at DESC LIMIT 500
            """)
            arbi_rows = cursor.fetchall()
            rows.extend(arbi_rows)
        except: pass
        cursor.close(); conn.close()

        processed = 0
        beat('pattern_discovery')  # heartbeat após query

        # [v10.14] Pré-processar mapa de trades por símbolo/dia para features comportamentais
        from collections import defaultdict as _dd
        sym_day_map = _dd(list)
        for _r in rows:
            _sk = (_r.get('symbol',''), str(_r.get('opened_at',''))[:10])
            sym_day_map[_sk].append(_r)

        for row in rows:
            try:
                features = {}
                if row.get('features_json'):
                    features = json.loads(row['features_json'])
                if not features:
                    # Reconstruir features básicas do que temos
                    opened = row.get('opened_at')
                    dt = datetime.fromisoformat(str(opened).replace('Z','')) if opened else datetime.utcnow()
                    features = {
                        'asset_type': row.get('asset_type','stock'),
                        'market_type': row.get('market','NYSE'),
                        'weekday': str(dt.weekday()),
                        'hour_utc': str(dt.hour),
                        'time_bucket': _time_bucket(dt),
                        'direction': 'LONG',  # maioria é LONG
                    }

                # [v10.14] Features específicas de arbi
                if row.get('asset_type') == 'arbitrage' or row.get('market') == 'ARBI':
                    pair_id     = row.get('pair_id') or row.get('symbol', '?')
                    entry_sp    = abs(float(row.get('entry_spread') or 0))
                    features['arbi_pair']        = pair_id
                    features['arbi_spread_zone'] = _spread_zone(entry_sp, pair_id)
                    features['arbi_direction']   = features.get('direction', 'LONG_A')
                    features['asset_type']       = 'arbitrage'
                    features['market_type']      = 'ARBI'
                # Garantir weekday e hour como strings
                if 'weekday' in features:
                    features['weekday'] = str(features['weekday'])
                if 'hour_utc' not in features and row.get('opened_at'):
                    dt = datetime.fromisoformat(str(row['opened_at']).replace('Z',''))
                    features['hour_utc'] = str(dt.hour)
                    features['is_market_open_hour'] = 'YES' if 13 <= dt.hour <= 19 else 'NO'
                # [v10.14] Copiar features comportamentais do sig para features se presentes
                for _bfeat in ('reentry_after_trailing', 'same_day_count', 'close_reason_prev'):
                    if sig.get(_bfeat): features[_bfeat] = sig[_bfeat]
                # Adicionar cross-market (histórico simplificado)
                features['stocks_regime'] = 'NEUTRAL'
                features['btc_trend'] = 'FLAT'
                features['usdbrl_trend'] = 'FLAT'
                pnl = float(row.get('pnl', 0) or 0)
                pnl_pct = float(row.get('pnl_pct', 0) or 0)

                # [v10.14] Features comportamentais — detectar padrões de re-entrada e sequência
                sym_row   = row.get('symbol', '')
                dt_row    = str(row.get('opened_at', ''))[:10]
                sym_key   = (sym_row, dt_row)
                _sym_day_trades = sym_day_map.get(sym_key, [])
                # Ordenar por horário para encontrar trades anteriores
                _before = [t for t in _sym_day_trades if str(t.get('opened_at','')) < str(row.get('opened_at',''))]
                _prev_reason = _before[-1].get('close_reason','') if _before else ''
                _reentry     = 'YES' if _prev_reason == 'TRAILING_STOP' else 'NO'
                _same_count  = str(min(len(_before) + 1, 3)) if len(_before) < 3 else '3+'
                features['reentry_after_trailing'] = _reentry
                features['same_day_count']         = _same_count
                features['close_reason_prev']      = _prev_reason or 'NONE'

                update_composite_pattern(features, pnl, pnl_pct)
                processed += 1
            except: pass

        beat('pattern_discovery')  # heartbeat após processar
        _last_discovery_run = datetime.utcnow().isoformat()
        log.info(f"[PatternDiscovery] Processadas {processed}/{len(rows)} trades → {len(_composite_patterns)} padrões compostos")

        # Identificar e logar os mais confiáveis
        with _pattern_discovery_lock:
            reliable = [(k,v) for k,v in _composite_patterns.items()
                       if v.get('reliable') and v['total_samples'] >= 20]
            reliable.sort(key=lambda x: -x[1]['total_samples'])
            for k,v in reliable[:5]:
                wr = v['wins']/v['total_samples']*100
                log.info(f"[PatternDiscovery] RELIABLE: {v['dims']} WR={wr:.0f}% n={v['total_samples']} adj={v['score_adj']:+d}")

    except Exception as e:
        log.error(f"run_pattern_discovery: {e}")

def pattern_discovery_loop():
    """[v10.13] Loop de background — mineração de padrões a cada 6 horas."""
    # Aguardar startup batendo coração para não ser marcado FROZEN
    for _ in range(18):  # 3 minutos em intervals de 10s
        beat('pattern_discovery')
        time.sleep(10)
    try: run_pattern_discovery()
    except: pass
    while True:
        # Dormir em pequenos incrementos batendo coração — evita FROZEN
        for _ in range(int(6*3600/30)):  # 6h em pedaços de 30s
            beat('pattern_discovery')
            time.sleep(30)
        beat('pattern_discovery')
        try: run_pattern_discovery()
        except Exception as e: log.error(f'pattern_discovery_loop: {e}')


# ═══════════════════════════════════════════════════════════════
# [L-3] INIT LEARNING — carrega stats do banco no startup
# ═══════════════════════════════════════════════════════════════

def init_learning_cache():
    """[L-3] Carrega pattern_stats e factor_stats do banco para memória."""
    global signal_events_count, last_learning_update, learning_errors, LEARNING_DEGRADED
    if not LEARNING_ENABLED: return
    conn = get_db()
    if not conn: return
    cursor=None
    try:
        c = conn.cursor(dictionary=True)

        # pattern_stats
        c.execute("SELECT * FROM pattern_stats")
        with learning_lock:
            for r in c.fetchall():
                ps = {k: float(v) if isinstance(v, decimal.Decimal) else
                         (v.isoformat() if isinstance(v, datetime) else v)
                      for k, v in r.items()}
                pattern_stats_cache[ps['feature_hash']] = ps

        # factor_stats
        c.execute("SELECT * FROM factor_stats")
        with learning_lock:
            for r in c.fetchall():
                fs = {k: float(v) if isinstance(v, decimal.Decimal) else
                         (v.isoformat() if isinstance(v, datetime) else v)
                      for k, v in r.items()}
                key = (fs['factor_type'], fs['factor_value'])
                factor_stats_cache[key] = fs

        # Contagem de signal_events
        c.execute("SELECT COUNT(*) as n FROM signal_events")
        row = c.fetchone()
        signal_events_count = row['n'] if row else 0

        c.close(); conn.close()
        learning_errors = 0; LEARNING_DEGRADED = False
        log.info(f'Learning cache: {len(pattern_stats_cache)} padrões | '
                 f'{len(factor_stats_cache)} fatores | {signal_events_count} signal_events')
    except Exception as e:
        log.error(f'init_learning_cache: {e}')
        LEARNING_DEGRADED = True

# ═══════════════════════════════════════════════════════════════
# [FIX-5] SHADOW EVALUATOR LOOP — avalia decisões PENDING
# ═══════════════════════════════════════════════════════════════
SHADOW_EVAL_WINDOW_MIN = int(os.environ.get('SHADOW_EVAL_WINDOW_MIN', 30))   # [v10.15] minutos até avaliar (era 60)

def shadow_evaluator_loop():
    """[FIX-5] Avalia shadow_decisions PENDING após janela configurável.
    Busca o preço atual do ativo, calcula hypothetical_pnl e fecha a decisão.
    Também atualiza aprendizado shadow (pattern_stats com peso reduzido).
    """
    while True:
        beat('shadow_evaluator_loop')
        time.sleep(120)   # [v10.15] verifica a cada 2 min (era 10 min)
        beat('shadow_evaluator_loop')
        if not LEARNING_ENABLED or LEARNING_DEGRADED: continue
        try:
            conn = get_db()
            if not conn: continue
            cutoff = (datetime.utcnow() - timedelta(minutes=SHADOW_EVAL_WINDOW_MIN)).strftime('%Y-%m-%d %H:%M:%S')
            c = conn.cursor(dictionary=True)
            c.execute("""SELECT * FROM shadow_decisions
                         WHERE evaluation_status='PENDING'
                         AND created_at <= %s
                         LIMIT 50""", (cutoff,))
            pending = c.fetchall(); c.close(); conn.close()
            if not pending: continue

            evaluated = 0
            for dec in pending:
                # [v10.7-Fix4] Uma única conexão por decision, fechada em finally.
                # Antes: conn2 + conn3 = até 3 conexões simultâneas × 50 decisions = 150 conexões.
                # Agora: 1 conexão reaproveitada, fechamento garantido mesmo em erro.
                dec_conn = get_db()
                if not dec_conn: continue
                try:
                    sym   = dec.get('symbol', '')
                    sig   = dec.get('signal', 'COMPRA')
                    entry = float(dec.get('hypothetical_entry', 0) or 0)
                    if entry <= 0: continue

                    # Preço atual — lê de memória, sem I/O
                    current_price = None
                    p = stock_prices.get(sym + '.SA') or stock_prices.get(sym)
                    if p: current_price = p.get('price')
                    if not current_price:
                        crypto_sym = sym + 'USDT'
                        if crypto_sym in crypto_prices:
                            current_price = crypto_prices[crypto_sym]
                    if not current_price or current_price <= 0: continue

                    # PnL hipotético — [P4] coerente para long e short
                    if sig == 'COMPRA':
                        hyp_pnl_pct = (current_price - entry) / entry * 100
                        hyp_pnl     = round(current_price - entry, 4)
                    else:
                        hyp_pnl_pct = (entry - current_price) / entry * 100
                        hyp_pnl     = round(entry - current_price, 4)

                    status = 'WIN' if hyp_pnl_pct > 0.1 else ('LOSS' if hyp_pnl_pct < -0.1 else 'FLAT')

                    # UPDATE shadow_decision
                    cx = dec_conn.cursor()
                    cx.execute("""UPDATE shadow_decisions SET
                        hypothetical_exit=%s, hypothetical_pnl=%s,
                        hypothetical_pnl_pct=%s, evaluation_status=%s,
                        evaluated_at=%s WHERE shadow_id=%s""",
                        (current_price, round(hyp_pnl, 4), round(hyp_pnl_pct, 4),
                         status, datetime.utcnow().isoformat(), dec['shadow_id']))
                    dec_conn.commit()  # [v10.15] commit explícito — sem isso o UPDATE nunca persiste!
                    cx.close()

                    # Buscar feature_hash via signal_events — mesma conexão
                    try:
                        cx2 = dec_conn.cursor(dictionary=True)
                        cx2.execute("SELECT feature_hash, payload_json FROM signal_events WHERE signal_id=%s",
                                    (dec.get('signal_id'),))
                        se_row = cx2.fetchone(); cx2.close()
                        if se_row and se_row.get('feature_hash'):
                            fhash = se_row['feature_hash']
                            shadow_pnl_pct = round(hyp_pnl_pct * 0.5, 4)
                            shadow_pnl     = round(hyp_pnl * 0.5, 4)
                            ps = update_pattern_stats(fhash, shadow_pnl, shadow_pnl_pct)
                            enqueue_persist('pattern_stats', ps)
                            try:
                                payload = json.loads(se_row.get('payload_json') or '{}')
                                shadow_features = {
                                    'score_bucket':     payload.get('score_bucket',''),
                                    'rsi_bucket':       payload.get('rsi_bucket',''),
                                    'ema_alignment':    payload.get('ema_alignment',''),
                                    'volatility_bucket':payload.get('volatility_bucket',''),
                                    'regime_mode':      payload.get('regime_mode',''),
                                    'direction':        payload.get('direction',''),
                                    'time_bucket':      payload.get('time_bucket',''),
                                    'weekday':          str(payload.get('weekday','')),
                                    'asset_type':       payload.get('asset_type',''),
                                    'market_type':      payload.get('market_type',''),
                                    'dq_bucket':        payload.get('dq_bucket',''),
                                }
                                # [FIX 29/abr/2026] Bloquear shadow FLAT (sem PnL real) de poluir factor_stats.
                                # Bug histórico: 22.591 shadow FLATs (market_closed) inflaram total_samples ~13x
                                # cada trade real, capando confidence_weight em 0.40 e cegando o cérebro V3.
                                shadow_status = status if 'status' in dir() or 'status' in locals() else None
                                _is_flat = (shadow_status == 'FLAT') or (abs(shadow_pnl_pct) < 0.1)
                                _no_real_event = (dec.get('not_executed_reason') == 'market_closed')
                                if any(shadow_features.values()) and not (_is_flat and _no_real_event):
                                    update_factor_stats(shadow_features, shadow_pnl, shadow_pnl_pct)
                                    alpha_keys = list(shadow_features.keys())
                                    with learning_lock:
                                        for ftype in alpha_keys:
                                            fval = shadow_features.get(ftype, '')
                                            if fval:
                                                fs_copy = dict(factor_stats_cache.get((ftype, fval), {}))
                                                if fs_copy:
                                                    enqueue_persist('factor_stats', fs_copy)
                            except Exception as ef:
                                log.debug(f'shadow factor_stats {dec.get("shadow_id")}: {ef}')
                            _db_log_learning_audit('SHADOW_OUTCOME', dec['shadow_id'], {
                                'feature_hash': fhash, 'status': status,
                                'hyp_pnl_pct': round(hyp_pnl_pct, 4),
                                'shadow_weight': 0.5,
                            })
                    except Exception as e2:
                        log.debug(f'shadow learning update {dec.get("shadow_id")}: {e2}')

                    evaluated += 1
                except Exception as e:
                    log.debug(f'shadow_evaluator item {dec.get("shadow_id")}: {e}')
                finally:
                    dec_conn.close()   # [v10.7-Fix4] sempre devolve ao pool

            if evaluated:
                log.info(f'Shadow evaluator: {evaluated} decisões avaliadas')
        except Exception as e:
            log.error(f'shadow_evaluator_loop: {e}')

# ═══════════════════════════════════════════════════════════════
# Registrar handlers no persistence_worker para novos tipos
# ═══════════════════════════════════════════════════════════════
# (feito inline no persistence_worker existente via extend)

# Antes de init_all_tables:
# ═══════════════════════════════════════════════════════════════
def init_all_tables():
    conn=get_db()
    if not conn: log.error('init_all_tables: no DB'); return
    try:
        cursor=conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS trades (
            id VARCHAR(40) PRIMARY KEY, symbol VARCHAR(20), market VARCHAR(10),
            asset_type VARCHAR(15), direction VARCHAR(5),
            entry_price DECIMAL(18,6), exit_price DECIMAL(18,6), current_price DECIMAL(18,6),
            quantity DECIMAL(20,6), position_value DECIMAL(18,2),
            pnl DECIMAL(18,2) DEFAULT 0, pnl_pct DECIMAL(10,4) DEFAULT 0,
            peak_pnl_pct DECIMAL(10,4) DEFAULT 0, score INT, signal_type VARCHAR(10),
            status VARCHAR(10) DEFAULT 'OPEN', close_reason VARCHAR(20),
            from_watchlist TINYINT(1) DEFAULT 0, order_id VARCHAR(40),
            opened_at DATETIME, closed_at DATETIME, extensions INT DEFAULT 0,
            signal_id VARCHAR(40) NULL, feature_hash VARCHAR(20) NULL,
            learning_confidence DECIMAL(6,2) NULL, insight_summary TEXT NULL,
            learning_version VARCHAR(10) NULL, features_json LONGTEXT NULL)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS arbi_trades (
            id VARCHAR(40) PRIMARY KEY, pair_id VARCHAR(40), name VARCHAR(40),
            leg_a VARCHAR(20), leg_b VARCHAR(20), mkt_a VARCHAR(10), mkt_b VARCHAR(10),
            direction VARCHAR(10), buy_leg VARCHAR(20), buy_mkt VARCHAR(10),
            short_leg VARCHAR(20), short_mkt VARCHAR(10),
            entry_spread DECIMAL(10,4), entry_spread_raw DECIMAL(10,4), current_spread DECIMAL(10,4),
            entry_spread_normalized DECIMAL(10,4), position_size DECIMAL(18,2),
            price_a_entry DECIMAL(18,4), price_b_entry DECIMAL(18,4),
            price_a_usd_norm DECIMAL(18,4), price_b_usd_norm DECIMAL(18,4),
            bid_a DECIMAL(18,4), ask_a DECIMAL(18,4), bid_b DECIMAL(18,4), ask_b DECIMAL(18,4),
            qty_a INT, qty_b INT,
            entry_ts DATETIME, signal_ts_a DATETIME, signal_ts_b DATETIME,
            delta_ts_between_legs_ms INT DEFAULT 0,
            fx_cost DECIMAL(10,2) DEFAULT 0, slippage_cost_a DECIMAL(10,2) DEFAULT 0,
            slippage_cost_b DECIMAL(10,2) DEFAULT 0, slippage_bps_total DECIMAL(8,2) DEFAULT 0,
            exchange_fee_a DECIMAL(10,2) DEFAULT 0, exchange_fee_b DECIMAL(10,2) DEFAULT 0,
            total_cost_estimated DECIMAL(10,2) DEFAULT 0,
            audit_flag VARCHAR(30) DEFAULT 'valid',
            simulation_model_version VARCHAR(10),
            pnl DECIMAL(18,2) DEFAULT 0, pnl_pct DECIMAL(10,4) DEFAULT 0,
            peak_pnl_pct DECIMAL(10,4) DEFAULT 0, fx_rate DECIMAL(10,4),
            status VARCHAR(10) DEFAULT 'OPEN', close_reason VARCHAR(20),
            opened_at DATETIME, closed_at DATETIME, extensions INT DEFAULT 0)""")
        # [v10.23] Colunas de custo de aluguel
        for _col_sql in [
            "ALTER TABLE arbi_trades ADD COLUMN lending_cost DECIMAL(10,2) DEFAULT 0",
            "ALTER TABLE arbi_trades ADD COLUMN lending_rate_annual DECIMAL(6,4) DEFAULT 0",
        ]:
            try: cursor.execute(_col_sql); conn.commit()
            except: pass  # coluna já existe
        cursor.execute("""CREATE TABLE IF NOT EXISTS audit_events (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            event_type VARCHAR(50), entity_type VARCHAR(30), entity_id VARCHAR(50),
            payload_json TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_created (created_at), INDEX idx_event (event_type))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS watchlist (
            symbol VARCHAR(30) PRIMARY KEY, market VARCHAR(10) NOT NULL,
            added_at DATETIME DEFAULT CURRENT_TIMESTAMP)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(40) PRIMARY KEY, trade_id VARCHAR(40),
            symbol VARCHAR(20), side VARCHAR(5), order_type VARCHAR(10),
            qty DECIMAL(20,6), limit_price DECIMAL(18,6), stop_price DECIMAL(18,6),
            strategy VARCHAR(20), status VARCHAR(20) DEFAULT 'NEW',
            fill_price DECIMAL(18,6), fill_qty DECIMAL(20,6),
            slippage DECIMAL(10,4) DEFAULT 0, fee DECIMAL(10,4) DEFAULT 0,
            notes VARCHAR(200), sent_at DATETIME, filled_at DATETIME,
            status_history_json TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_symbol (symbol), INDEX idx_status (status))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS portfolio_snapshots (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ts DATETIME, stocks_capital DECIMAL(18,2), crypto_capital DECIMAL(18,2),
            arbi_capital DECIMAL(18,2), stocks_open_pnl DECIMAL(18,2),
            crypto_open_pnl DECIMAL(18,2), arbi_open_pnl DECIMAL(18,2),
            total_open_pnl DECIMAL(18,2), open_positions INT, arbi_positions INT,
            kill_switch TINYINT(1), arbi_kill_switch TINYINT(1), market_regime VARCHAR(20),
            INDEX idx_ts (ts))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS symbol_blocked_persistent (
            symbol VARCHAR(20) PRIMARY KEY,
            blocked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            reason VARCHAR(200)
        ) ENGINE=InnoDB""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS symbol_cooldowns (
            symbol VARCHAR(30) PRIMARY KEY, last_close_at DATETIME,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)""")

        # ── [L-2] Signal Events ───────────────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS signal_events (
            signal_id VARCHAR(40) PRIMARY KEY,
            feature_hash VARCHAR(20), symbol VARCHAR(20), asset_type VARCHAR(15),
            market_type VARCHAR(10), signal_type VARCHAR(10), raw_score DECIMAL(6,2),
            learning_confidence DECIMAL(6,2), confidence_band VARCHAR(10),
            price DECIMAL(18,6), signal_created_at DATETIME,
            market_regime_mode VARCHAR(20), market_regime_volatility VARCHAR(10),
            market_open TINYINT(1), trade_open TINYINT(1),
            rsi DECIMAL(6,2), ema9 DECIMAL(18,6), ema21 DECIMAL(18,6), ema50 DECIMAL(18,6),
            rsi_bucket VARCHAR(15), score_bucket VARCHAR(15), change_pct_bucket VARCHAR(10),
            ema_alignment VARCHAR(20), volatility_bucket VARCHAR(10),
            weekday TINYINT, time_bucket VARCHAR(15), data_quality_score DECIMAL(5,2),
            source_type VARCHAR(30), payload_json TEXT, insight_summary TEXT,
            learning_version VARCHAR(10),
            trade_id VARCHAR(40) NULL, order_id VARCHAR(40) NULL,
            outcome_status VARCHAR(10) NULL, outcome_pnl DECIMAL(18,4) NULL,
            outcome_pnl_pct DECIMAL(10,4) NULL, outcome_close_reason VARCHAR(20) NULL,
            origin_signal_key VARCHAR(120) NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_sig_symbol (symbol), INDEX idx_sig_hash (feature_hash),
            INDEX idx_sig_created (signal_created_at),
            UNIQUE KEY uq_origin_signal_key (origin_signal_key))""")

        # ── [L-3] Pattern Stats ───────────────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS pattern_stats (
            feature_hash VARCHAR(20) PRIMARY KEY,
            total_samples INT DEFAULT 0, wins INT DEFAULT 0,
            losses INT DEFAULT 0, flat_count INT DEFAULT 0,
            avg_pnl DECIMAL(18,4) DEFAULT 0, avg_pnl_pct DECIMAL(10,4) DEFAULT 0,
            ewma_pnl_pct DECIMAL(10,4) DEFAULT 0, ewma_hit_rate DECIMAL(6,4) DEFAULT 0.5,
            expectancy DECIMAL(10,4) DEFAULT 0, downside_score DECIMAL(10,4) DEFAULT 0,
            max_loss_seen DECIMAL(10,4) DEFAULT 0, confidence_weight DECIMAL(6,4) DEFAULT 0,
            last_seen_at DATETIME, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)""")

        # ── [L-4] Factor Stats ────────────────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS factor_stats (
            factor_type VARCHAR(30), factor_value VARCHAR(30),
            total_samples INT DEFAULT 0, wins INT DEFAULT 0, losses INT DEFAULT 0,
            avg_pnl_pct DECIMAL(10,4) DEFAULT 0, ewma_pnl_pct DECIMAL(10,4) DEFAULT 0,
            expectancy DECIMAL(10,4) DEFAULT 0, downside_score DECIMAL(10,4) DEFAULT 0,
            confidence_weight DECIMAL(6,4) DEFAULT 0,
            last_seen_at DATETIME, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (factor_type, factor_value))""")

        # ── [L-8] Shadow Decisions ────────────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS shadow_decisions (
            shadow_id VARCHAR(40) PRIMARY KEY,
            signal_id VARCHAR(40), symbol VARCHAR(20), signal_type VARCHAR(10),
            price_at_signal DECIMAL(18,6), not_executed_reason VARCHAR(30),
            hypothetical_entry DECIMAL(18,6), hypothetical_exit DECIMAL(18,6) NULL,
            hypothetical_pnl DECIMAL(18,4) NULL, hypothetical_pnl_pct DECIMAL(10,4) NULL,
            evaluation_status VARCHAR(15) DEFAULT 'PENDING',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            evaluated_at DATETIME NULL, payload_json TEXT,
            INDEX idx_shd_signal (signal_id), INDEX idx_shd_symbol (symbol))""")

        # ── Learning Audit ────────────────────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS learning_audit (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            event_type VARCHAR(50), entity_id VARCHAR(50),
            payload_json TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_la_event (event_type))""")

        # ── [v10.18] Calibration Tracker ─────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS calibration_tracker (
            band VARCHAR(20) PRIMARY KEY,
            wins INT DEFAULT 0, losses INT DEFAULT 0, total INT DEFAULT 0,
            sum_pnl_pct DECIMAL(12,4) DEFAULT 0,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)""")

        # ── [v10.18] Capital Ledger ──────────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS capital_ledger (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ts DATETIME, strategy VARCHAR(20), event VARCHAR(20),
            symbol VARCHAR(20), amount DECIMAL(14,2), balance_after DECIMAL(14,2),
            trade_id VARCHAR(40),
            INDEX idx_ledger_strategy (strategy),
            INDEX idx_ledger_ts (ts))""")

        # ── [v10.18] Reconciliation Log ──────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS reconciliation_log (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ts DATETIME, strategy VARCHAR(20),
            memory_capital DECIMAL(14,2), calculated_capital DECIMAL(14,2),
            committed DECIMAL(14,2), realized_pnl DECIMAL(14,2),
            delta DECIMAL(14,2), delta_pct DECIMAL(8,4), ok TINYINT DEFAULT 1,
            INDEX idx_recon_ts (ts))""")

        # ── [v10.22] UNIQUE constraint on capital_ledger ─────────────
        try:
            cursor.execute("""CREATE UNIQUE INDEX uq_ledger_event
                ON capital_ledger (strategy, trade_id, event)""")
        except Exception as _e:
            if 'Duplicate' not in str(_e) and 'exists' not in str(_e).lower():
                log.debug(f'Migration uq_ledger: {_e}')

        # ── [v10.22] RBAC Users ──────────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS rbac_users (
            email VARCHAR(120) PRIMARY KEY,
            role VARCHAR(20) NOT NULL DEFAULT 'viewer',
            api_key_hash VARCHAR(128),
            created_by VARCHAR(120),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_access DATETIME,
            is_active TINYINT(1) DEFAULT 1,
            INDEX idx_rbac_role (role))""")

        # ── [v10.22] Audit Log (immutable) ───────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS audit_log (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP,
            email VARCHAR(120),
            action VARCHAR(50),
            detail TEXT,
            ip_address VARCHAR(45),
            INDEX idx_audit_ts (ts),
            INDEX idx_audit_email (email))""")

        # ── [v10.22] Kill Switch State ───────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS kill_switch_state (
            scope VARCHAR(20) PRIMARY KEY,
            active TINYINT(1) DEFAULT 0,
            activated_by VARCHAR(120),
            activated_at DATETIME,
            reason TEXT,
            auto_resume_at DATETIME NULL)""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS kill_switch_log (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP,
            scope VARCHAR(20),
            action VARCHAR(30),
            activated_by VARCHAR(120),
            reason TEXT,
            INDEX idx_ks_ts (ts))""")

        # ── [v10.22] Order Tracking ──────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS order_tracking (
            order_id VARCHAR(60) PRIMARY KEY,
            trade_id VARCHAR(40),
            symbol VARCHAR(20),
            side VARCHAR(5),
            order_type VARCHAR(15),
            asset_class VARCHAR(15),
            quantity DECIMAL(20,6),
            decision_price DECIMAL(18,6),
            sent_price DECIMAL(18,6),
            executed_price DECIMAL(18,6),
            average_price DECIMAL(18,6),
            slippage DECIMAL(10,4),
            latency_ms INT,
            status VARCHAR(20),
            fees_estimated DECIMAL(10,4),
            fees_actual DECIMAL(10,4),
            idempotency_key VARCHAR(80),
            retry_count INT DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_ot_trade (trade_id),
            INDEX idx_ot_symbol (symbol),
            UNIQUE KEY uq_idempotency (idempotency_key))""")

        # ── [v10.22] Performance Stats Snapshots ─────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS stats_snapshots (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP,
            strategy VARCHAR(20),
            total_trades INT,
            win_rate DECIMAL(6,4),
            profit_factor DECIMAL(10,4),
            sharpe DECIMAL(10,4),
            sortino DECIMAL(10,4),
            max_drawdown_pct DECIMAL(10,4),
            expectancy DECIMAL(10,4),
            edge_stability DECIMAL(6,2),
            payload_json LONGTEXT,
            INDEX idx_ss_ts (ts),
            INDEX idx_ss_strat (strategy))""")

        # ── [v10.22] Risk Events ─────────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS risk_events (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP,
            event_type VARCHAR(50),
            strategy VARCHAR(20),
            detail TEXT,
            INDEX idx_re_ts (ts))""")

        # ── Migração: adicionar colunas de learning nas tabelas existentes ─
        for col_sql in [
            "ALTER TABLE trades ADD COLUMN signal_id           VARCHAR(40)  NULL",
            "ALTER TABLE trades ADD COLUMN feature_hash        VARCHAR(20)  NULL",
            "ALTER TABLE trades ADD COLUMN learning_confidence DECIMAL(6,2) NULL",
            "ALTER TABLE trades ADD COLUMN fee_estimated DECIMAL(10,2) NULL DEFAULT 0",
            "CREATE TABLE IF NOT EXISTS symbol_blocked_persistent (symbol VARCHAR(20) PRIMARY KEY, blocked_until DATETIME, reason VARCHAR(100), created_at DATETIME DEFAULT NOW())",
            "ALTER TABLE trades ADD COLUMN pnl_gross DECIMAL(10,2) NULL",
            "ALTER TABLE trades ADD COLUMN insight_summary     TEXT         NULL",
            "ALTER TABLE trades ADD COLUMN learning_version    VARCHAR(10)  NULL",
            "ALTER TABLE trades ADD COLUMN features_json       LONGTEXT     NULL",  # [P0-1]
            # [S2] Deduplicação persistida — chave de origem do sinal de mercado
            "ALTER TABLE signal_events ADD COLUMN origin_signal_key VARCHAR(120) NULL",
        ]:
            try: cursor.execute(col_sql)
            except Exception as e:
                if 'Duplicate column' not in str(e): log.debug(f'Migration: {e}')
        # [S2] Índice UNIQUE para origin_signal_key em bancos existentes
        # Ignora erro se já existir (Duplicate key name)
        try:
            cursor.execute("""ALTER TABLE signal_events
                ADD UNIQUE KEY uq_origin_signal_key (origin_signal_key)""")
        except Exception as e:
            if 'Duplicate key name' not in str(e) and 'already exists' not in str(e).lower():
                log.debug(f'Migration uq_origin: {e}')
        # [v10.25] Derivatives tables
        try:
            create_derivatives_tables(conn)
            log.info('[v10.25] Derivatives tables created/verified')
        except Exception as e:
            log.warning(f'[v10.25] Derivatives tables: {e}')
        # [v10.38] Audit columns for strategy_master_trades (instrument_type, audit trail)
        for _col_sql in [
            "ALTER TABLE strategy_master_trades ADD COLUMN instrument_type VARCHAR(16) DEFAULT 'option'",
            "ALTER TABLE strategy_master_trades ADD COLUMN theoretical_price DECIMAL(14,4) NULL",
            "ALTER TABLE strategy_master_trades ADD COLUMN deviation_bps DECIMAL(8,2) NULL",
            "ALTER TABLE strategy_master_trades ADD COLUMN brain_confidence DECIMAL(5,4) NULL",
            "ALTER TABLE strategy_master_trades ADD COLUMN brain_adjustment DECIMAL(5,4) NULL",
            "ALTER TABLE strategy_master_trades ADD COLUMN borrow_fee_estimate DECIMAL(12,4) NULL DEFAULT 0",
            "ALTER TABLE strategy_master_trades ADD COLUMN hedge_trade_id VARCHAR(64) NULL",
            "ALTER TABLE strategy_master_trades ADD COLUMN fair_value_inputs TEXT NULL",
            "ALTER TABLE strategy_master_trades ADD COLUMN audit_notes TEXT NULL",
        ]:
            try: cursor.execute(_col_sql); conn.commit()
            except Exception as e:
                if 'Duplicate column' not in str(e):
                    log.debug(f'[v10.38] strategy_master_trades migration: {e}')
        # [v10.36] Top opportunities daily audit table
        try:
            create_top_opps_audit_table(conn)
            log.info('[v10.36] top_opportunities_audit table created/verified')
        except Exception as e:
            log.warning(f'[v10.36] top_opportunities_audit table: {e}')
        conn.commit()
        log.info('All tables created/verified')
    except Exception as e: log.error(f'init_all_tables: {e}')
    finally:
        try:
            if cursor: cursor.close()
        except: pass
        try:
            if conn: conn.close()
        except: pass

def _row_to_trade(r):
    t = {}
    for k,v in r.items():
        if isinstance(v,datetime): t[k]=v.isoformat()
        elif isinstance(v,decimal.Decimal): t[k]=float(v)
        else: t[k]=v
    t.setdefault('pnl_history',[]); t.setdefault('peak_pnl_pct',0); t.setdefault('extensions',0)
    return t

def init_trades_tables():
    global stocks_capital, crypto_capital, arbi_capital
    conn=get_db()
    if not conn: return
    cursor=None
    try:
        cursor=conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM trades WHERE status='OPEN'")
        for r in cursor.fetchall():
            t=_row_to_trade(r)
            # [FIX-4] Restaurar _features para factor learning funcionar após restart
            if t.get('features_json'):
                try: t['_features'] = json.loads(t['features_json'])
                except: pass
            if t['asset_type']=='stock': stocks_open.append(t); stocks_capital-=t['position_value']
            elif t['asset_type']=='crypto': crypto_open.append(t); crypto_capital-=t['position_value']
        cursor.execute("SELECT * FROM trades WHERE status='CLOSED' ORDER BY closed_at DESC")  # [v10.9] sem limite
        for r in cursor.fetchall():
            t=_row_to_trade(r)
            if t['asset_type']=='stock':
                if not t.get('fee_estimated') and t.get('status')=='CLOSED':
                    apply_fee_to_trade(t)  # [v10.14] fee retroativo
                stocks_closed.append(t)
                stocks_capital += float(t.get('pnl') or 0)  # [FIX-27]
            elif t['asset_type']=='crypto':
                if not t.get('fee_estimated') and t.get('status')=='CLOSED':
                    apply_fee_to_trade(t)  # [v10.14] fee retroativo
                crypto_closed.append(t)
                crypto_capital += float(t.get('pnl') or 0)  # [FIX-27]
        cursor.execute("SELECT * FROM arbi_trades WHERE status='OPEN'")
        for r in cursor.fetchall():
            t=_row_to_trade(r); arbi_open.append(t); arbi_capital-=t['position_size']
        cursor.execute("SELECT * FROM arbi_trades WHERE status='CLOSED' ORDER BY closed_at DESC")  # [v10.9] sem limite
        for r in cursor.fetchall():
            at=_row_to_trade(r)
            if not at.get('fee_estimated'):
                at['market']='ARBI'; apply_fee_to_trade(at)
            arbi_closed.append(at)
            # [v10.52-FIX] Arbi capital nao estava recebendo PnL realizado
            # ao recarregar do DB — linhas 4722 (stocks) e 4727 (crypto)
            # faziam isso, mas arbi foi esquecido. Por isso RECON-ALERT
            # disparava com memory=$3M (baseline) vs calc=$3M+realized_pnl.
            # Ajusta capital para refletir historico de trades fechados.
            arbi_capital += float(at.get('pnl') or 0)
        # [v10.9] Carregar runtime settings do banco
        try:
            global MIN_SCORE_AUTO, TRAILING_TRIGGER_PCT, STOCK_SL_PCT  # [FIX] declarar globals antes de atribuir
            cursor.execute("CREATE TABLE IF NOT EXISTS runtime_settings (key_name VARCHAR(60) PRIMARY KEY, value_float DOUBLE, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB")
            cursor.execute("SELECT key_name, value_float FROM runtime_settings")
            for _r in cursor.fetchall():
                _k, _v = _r['key_name'], float(_r['value_float'])
                if _k == 'MIN_SCORE_AUTO':        MIN_SCORE_AUTO = int(_v)
                elif _k == 'TRAILING_TRIGGER_PCT': TRAILING_TRIGGER_PCT = _v
                elif _k == 'STOCK_SL_PCT':         STOCK_SL_PCT = _v
                log.info(f'STARTUP settings: {_k}={_v}')
        except Exception as _e: log.warning(f'runtime_settings load: {_e}')
        cursor.execute("SELECT symbol FROM symbol_blocked_persistent")
        for r in cursor.fetchall():
            symbol_blocked.add(r['symbol'])
            log.info(f'STARTUP: {r["symbol"]} carregado como BLOCKED (persistido)')
        cursor.execute("SELECT symbol, last_close_at FROM symbol_cooldowns")
        for r in cursor.fetchall():
            if r.get('last_close_at'):
                symbol_cooldown[r['symbol']]=r['last_close_at'].timestamp()
        cursor.execute("SELECT * FROM orders ORDER BY created_at DESC LIMIT 500")
        with orders_lock:
            for r in cursor.fetchall():
                row = _row_to_trade(r)
                # [V91-2] Restaurar status_history do JSON salvo no banco
                if row.get('status_history_json'):
                    try: row['status_history'] = json.loads(row['status_history_json'])
                    except: row.setdefault('status_history', [])
                else:
                    row.setdefault('status_history', [])
                orders_log.append(row)
        cursor.execute("SELECT * FROM audit_events ORDER BY created_at DESC LIMIT 200")
        with audit_lock:
            for r in cursor.fetchall():
                try:
                    payload=json.loads(r.get('payload_json') or '{}')
                    entry={'timestamp':r['created_at'].isoformat() if r.get('created_at') else '',
                           'event':r.get('event_type','')}
                    entry.update(payload); audit_log.append(entry)
                except: pass
        cursor.close(); conn.close()
        log.info(f'Loaded: {len(stocks_open)}s/{len(crypto_open)}c/{len(arbi_open)}a open | '
                 f'{len(orders_log)} orders | {len(audit_log)} audit | {len(symbol_cooldown)} cooldowns')
        _restore_missing_trades_from_backup()   # [v10.11] verifica backup após carregar do banco
    except Exception as e: log.error(f'init_trades_tables: {e}')


import os as _os, json as _json, threading as _bk_lock
_BACKUP_FILE = '/tmp/egreja_trades_backup.json'
_backup_lock = threading.Lock()

def _backup_trade_to_file(trade: dict):
    """[v10.11] Backup local JSON — proteção dupla contra perda de dados em deploy."""
    try:
        with _backup_lock:
            existing = []
            if _os.path.exists(_BACKUP_FILE):
                try:
                    with open(_BACKUP_FILE, 'r') as f:
                        existing = _json.load(f)
                except: existing = []
            # Evita duplicatas pelo ID
            ids = {t.get('id') for t in existing}
            if trade.get('id') not in ids:
                existing.append({k: str(v) if hasattr(v,'isoformat') else v 
                                 for k,v in trade.items() 
                                 if k not in ('_features','pnl_history')})
                with open(_BACKUP_FILE, 'w') as f:
                    _json.dump(existing, f)
    except Exception as e:
        log.warning(f'[BACKUP] falha no backup local: {e}')

def _restore_missing_trades_from_backup():
    """[v10.11] No startup, verifica se há trades no backup que não estão na memória."""
    try:
        if not _os.path.exists(_BACKUP_FILE): return
        with open(_BACKUP_FILE, 'r') as f:
            backup = _json.load(f)
        db_ids = {t.get('id') for t in stocks_closed + crypto_closed}
        missing = [t for t in backup if t.get('id') not in db_ids and t.get('status')=='CLOSED']
        if missing:
            log.warning(f'[BACKUP] {len(missing)} trades no backup não encontradas na memória — verificar banco')
            for t in missing[:5]:
                log.warning(f'  Missing: {t.get("id")} {t.get("symbol")} {t.get("closed_at")}')
    except Exception as e:
        log.warning(f'[BACKUP] erro ao verificar backup: {e}')

def _db_save_trade(trade):
    conn=get_db()
    if not conn: return
    cursor=None
    try:
        cursor=conn.cursor(); t=trade
        # [FIX-4] features_json serializado para sobreviver restart
        features_json = None
        if t.get('_features'):
            try: features_json = json.dumps({k: v for k, v in t['_features'].items()
                                              if not k.startswith('_')}, default=str)
            except: pass
        cursor.execute("""INSERT INTO trades (id,symbol,market,asset_type,direction,
            entry_price,exit_price,current_price,quantity,position_value,
            pnl,pnl_pct,peak_pnl_pct,score,`signal`,status,close_reason,
            from_watchlist,order_id,opened_at,closed_at,extensions,
            signal_id,feature_hash,learning_confidence,insight_summary,learning_version,features_json,
            score_v2,regime_v2,signal_v2)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE current_price=VALUES(current_price),pnl=VALUES(pnl),
            pnl_pct=VALUES(pnl_pct),peak_pnl_pct=VALUES(peak_pnl_pct),
            status=VALUES(status),close_reason=VALUES(close_reason),
            exit_price=VALUES(exit_price),closed_at=VALUES(closed_at),extensions=VALUES(extensions)""",
            (t.get('id'),t.get('symbol'),t.get('market'),t.get('asset_type'),t.get('direction'),
             t.get('entry_price'),t.get('exit_price'),t.get('current_price'),
             t.get('quantity'),t.get('position_value'),t.get('pnl',0),t.get('pnl_pct',0),
             t.get('peak_pnl_pct',0),t.get('score'),t.get('signal',''),t.get('status','OPEN'),
             t.get('close_reason'),1 if t.get('from_watchlist') else 0,
             t.get('order_id'),t.get('opened_at'),t.get('closed_at'),t.get('extensions',0),
             t.get('signal_id'),t.get('feature_hash'),t.get('learning_confidence'),
             t.get('insight_summary'),t.get('learning_version'),features_json,
             t.get('score_v2'),t.get('regime_v2'),t.get('signal_v2')))
        conn.commit()
    except Exception as e: log.error(f'db_save_trade: {e}')
    finally:
        try:
            if cursor: cursor.close()
        except: pass
        try:
            if conn: conn.close()
        except: pass

def _db_save_arbi_trade(trade):
    conn=get_db()
    if not conn: return
    cursor=None
    try:
        cursor=conn.cursor(); t=trade
        cursor.execute("""INSERT INTO arbi_trades (id,pair_id,name,leg_a,leg_b,mkt_a,mkt_b,
            direction,buy_leg,buy_mkt,short_leg,short_mkt,entry_spread,current_spread,
            position_size,pnl,pnl_pct,peak_pnl_pct,fx_rate,status,close_reason,opened_at,closed_at,extensions,
            fx_cost,slippage_cost_a,slippage_cost_b,exchange_fee_a,exchange_fee_b,
            lending_cost,lending_rate_annual,total_cost_estimated)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE current_spread=VALUES(current_spread),pnl=VALUES(pnl),
            pnl_pct=VALUES(pnl_pct),peak_pnl_pct=VALUES(peak_pnl_pct),
            status=VALUES(status),close_reason=VALUES(close_reason),
            closed_at=VALUES(closed_at),extensions=VALUES(extensions)""",
            (t.get('id'),t.get('pair_id'),t.get('name'),t.get('leg_a'),t.get('leg_b'),
             t.get('mkt_a'),t.get('mkt_b'),t.get('direction'),t.get('buy_leg'),t.get('buy_mkt'),
             t.get('short_leg'),t.get('short_mkt'),t.get('entry_spread'),t.get('current_spread'),
             t.get('position_size'),t.get('pnl',0),t.get('pnl_pct',0),t.get('peak_pnl_pct',0),
             t.get('fx_rate'),t.get('status','OPEN'),t.get('close_reason'),
             t.get('opened_at'),t.get('closed_at'),t.get('extensions',0),
             t.get('fx_cost',0),t.get('slippage_cost_a',0),t.get('slippage_cost_b',0),
             t.get('exchange_fee_a',0),t.get('exchange_fee_b',0),
             t.get('lending_cost',0),t.get('lending_rate_annual',0),t.get('total_cost_estimated',0)))
        conn.commit()
    except Exception as e: log.error(f'db_save_arbi_trade: {e}')
    finally:
        try:
            if cursor: cursor.close()
        except: pass
        try:
            if conn: conn.close()
        except: pass

def _db_save_cooldown(symbol, ts):
    conn=get_db()
    if not conn: return
    cursor=None
    try:
        cursor=conn.cursor()
        dt=datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("INSERT INTO symbol_cooldowns (symbol,last_close_at) VALUES (%s,%s) "
                       "ON DUPLICATE KEY UPDATE last_close_at=%s,updated_at=NOW()",(symbol,dt,dt))
        conn.commit()
    except Exception as e: log.error(f'db_save_cooldown: {e}')
    finally:
        try:
            if cursor: cursor.close()
        except: pass
        try:
            if conn: conn.close()
        except: pass

# ═══════════════════════════════════════════════════════════════
# STOCK PRICE FEED — range=3mo para indicadores reais
# ═══════════════════════════════════════════════════════════════
STOCK_SYMBOLS_B3 = [
    # ── Blue chips originais ──────────────────────────────
    'PETR4.SA','VALE3.SA','ITUB4.SA','BBDC4.SA','ABEV3.SA','WEGE3.SA',
    'RENT3.SA','LREN3.SA','SUZB3.SA','GGBR4.SA','EMBR3.SA','CSNA3.SA',
    'CMIG4.SA','CPLE6.SA','BBAS3.SA','VIVT3.SA','SBSP3.SA','CSAN3.SA',
    'GOAU4.SA','USIM5.SA','BPAC11.SA','RADL3.SA','PRIO3.SA',
    'BRFS3.SA','MRFG3.SA','JBSS3.SA','EGIE3.SA','CMIN3.SA','AESB3.SA',
    # ── Expansão v10.14 — watchlist + curadoria ──────────
    'BBDC3.SA','BBSE3.SA',             # Bradesco ON + BB Seguridade
    'ALOS3.SA','MULT3.SA','SMFT3.SA',  # shoppings + Smart Fit
    'EQTL3.SA','TAEE11.SA','ENEV3.SA', # energia elétrica
    'CPFE3.SA','CXSE3.SA',             # CPFL + Caixa Seguros
    'VBBR3.SA','UGPA3.SA',             # Vibra + Ultrapar (combustíveis)
    'KLBN11.SA',                        # Klabin (papel/celulose)
    'TOTS3.SA','MGLU3.SA','CASH3.SA',  # tech/varejo BR
    'HAPV3.SA','RDOR3.SA','HYPE3.SA',  # saúde
    'COGN3.SA','YDUQ3.SA',             # educação (alta volatilidade)
    'NTCO3.SA',                         # Natura/Grupo Boticário
    'AZUL4.SA',                         # Azul Airlines
    'CCRO3.SA',                         # CCR concessões
    'MDIA3.SA','ALPA4.SA','POMO4.SA',  # consumo
    'AMER3.SA','RECV3.SA',             # Americanas + PetroRecôncavo
]
STOCK_SYMBOLS_US = [
    # ── Blue chips originais ──────────────────────────────
    'AAPL','MSFT','NVDA','AMZN','GOOGL','META','TSLA','NFLX','AMD','INTC',
    'JPM','BAC','GS','MS','V','MA','JNJ','PFE','UNH','XOM','CVX','COP',
    'DIS','UBER','LYFT','SPOT','COIN','SPY','QQQ','IWM',
    # ── Expansão v10.14 — watchlist + curadoria ──────────
    'TSM','AVGO','MU','ARM','SMCI',    # semicondutores
    'ADBE','CRM','NOW','ORCL','SNOW',  # enterprise cloud
    'SHOP','MELI','HOOD','HUBS','TCOM',# e-commerce / fintech
    'BABA',                             # China tech
    'LLY','TME',                        # pharma + Tencent Music
    'PLTR','OKLO','TGT',               # watchlist usuário
]
ALL_STOCK_SYMBOLS = STOCK_SYMBOLS_B3 + STOCK_SYMBOLS_US

# [v10.31] Warm Cedro socket cache — subscribe all B3 symbols + indices/futures
try:
    if _cedro_socket and _cedro_socket.enabled:
        _b3_clean = [s.replace('.SA', '') for s in STOCK_SYMBOLS_B3]
        _extras = ['IBOV', 'WINFUT', 'INDFUT', 'DOLFUT', 'WDOFUT',
                   'DI1F26', 'DI1F27', 'DI1F28', 'DI1F29', 'DI1F30',  # [v10.42] DI futures for DI_CALENDAR
                   'PETR4F', 'VALE3F']  # [v10.42] synthetic single-stock futures
        _cedro_socket.subscribe(_b3_clean + _extras)
        log.info(f'[v10.31] Cedro socket pre-subscribed {len(_b3_clean)+len(_extras)} symbols')
except Exception as _cedro_sub_err:
    log.warning(f'[v10.31] Cedro pre-subscribe failed: {_cedro_sub_err}')

# [v10.28] Phase 3: _ema and _rsi are pure math — use module versions when available
if _PURE_MODULES_LOADED:
    _ema = _mod_ema
    _rsi = _mod_rsi
else:
    def _ema(closes, period):
        if len(closes) < period: return closes[-1] if closes else 0
        k=2.0/(period+1); ema=closes[0]
        for c in closes[1:]: ema=c*k+ema*(1-k)
        return ema

    def _rsi(closes, period=14):
        if len(closes) < period+1: return 50.0
        gains=[]; losses=[]
        for i in range(1,period+1):
            d=closes[-period+i]-closes[-period+i-1]
            gains.append(d if d>0 else 0); losses.append(abs(d) if d<0 else 0)
        ag=sum(gains)/period; al=sum(losses)/period
        if al==0: return 100.0
        return round(100-100/(1+ag/al),1)

# [v10.5-5] Cache de candles/indicadores para não refetchar histórico a cada loop.
# Preço snapshot: sempre fresco. Candles/EMA/RSI/ATR/Volume: cache de CANDLES_CACHE_MIN minutos.
CANDLES_CACHE_MIN  = int(os.environ.get('CANDLES_CACHE_MIN', 10))
_candles_cache: dict = {}   # sym → {'data': result_dict, 'ts': float}
_candles_lock = threading.Lock()

def _get_cached_candles(sym: str, ttl_min: int = None) -> dict:
    """Retorna candles do cache se frescos, None caso contrário.
    ttl_min: TTL customizado. None usa CANDLES_CACHE_MIN (padrão 10min).
    Klines diários de crypto usam ttl_min=60 para não sobrecarregar Binance.
    """
    ttl = (ttl_min if ttl_min is not None else CANDLES_CACHE_MIN) * 60
    with _candles_lock:
        entry = _candles_cache.get(sym)
    if entry and (time.time() - entry['ts']) < ttl:
        return entry['data']
    return None

def _set_cached_candles(sym: str, data: dict):
    with _candles_lock:
        _candles_cache[sym] = {'data': data, 'ts': time.time()}
def _fetch_polygon_stock(ticker: str) -> tuple:
    """[v10.4][v10.5-5] Polygon.io: snapshot de preço sempre fresco.
    Candles históricos (EMA/RSI/ATR/Volume) buscados só se cache > CANDLES_CACHE_MIN min.
    Reduz chamadas de API de ~4/min para ~1/10min por símbolo.
    """
    t0 = time.time()
    try:
        # Snapshot para preço atual — sempre fresco
        r = requests.get(
            f'https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}',
            params={'apiKey': POLYGON_API_KEY}, timeout=8)
        lat = (time.time() - t0) * 1000
        if r.status_code != 200: return None, lat
        snap = r.json().get('ticker', {})
        day  = snap.get('day', {}); prev_day = snap.get('prevDay', {})
        price = float(day.get('c') or snap.get('lastTrade', {}).get('p') or 0)
        prev  = float(prev_day.get('c') or 0)
        if price <= 0: return None, lat

        market = 'NYSE' if not ticker.endswith('.SA') else 'B3'

        # [v10.5-5] Tentar cache de candles primeiro
        cached = _get_cached_candles(f'polygon:{ticker}')
        if cached:
            result = dict(cached)
            result['price']      = price
            result['prev']       = prev
            result['change_pct'] = round((price / prev - 1) * 100, 2) if prev > 0 else 0
            result['updated_at'] = datetime.utcnow().isoformat()
            result['source']     = 'Polygon-snapshot'
            vol_today = float(day.get('v') or 0)
            if vol_today > 0 and cached.get('_avg_vol20', 0) > 0:
                result['volume_ratio'] = round(vol_today / cached['_avg_vol20'], 3)
            record_data_quality(ticker.replace('.SA', ''), 'Polygon', lat, True)
            return result, lat

        # Cache frio: buscar candles históricos
        end_date   = datetime.utcnow().strftime('%Y-%m-%d')
        start_date = (datetime.utcnow() - timedelta(days=90)).strftime('%Y-%m-%d')
        rc = requests.get(
            f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}',
            params={'apiKey': POLYGON_API_KEY, 'adjusted': 'true', 'sort': 'asc', 'limit': 90},
            timeout=8)
        closes = []; highs = []; lows = []; volumes = []
        if rc.status_code == 200:
            bars = rc.json().get('results', [])
            closes  = [b['c'] for b in bars if b.get('c')]
            highs   = [b['h'] for b in bars if b.get('h')]
            lows    = [b['l'] for b in bars if b.get('l')]
            volumes = [b['v'] for b in bars if b.get('v')]

        n = len(closes)
        ema9  = _ema(closes, 9)  if n >= 9  else price
        ema21 = _ema(closes, 21) if n >= 21 else price
        ema50 = _ema(closes, 50) if n >= 50 else price
        rsi   = _rsi(closes)     if n >= 15 else 50.0
        atr   = _calc_atr(closes, highs, lows, 14) if n >= 15 else 0.0
        atr_pct = round((atr / price) * 100, 3) if price > 0 and atr > 0 else 0.0
        vol_today = float(day.get('v') or volumes[-1] if volumes else 0)
        avg_vol20 = sum(volumes[-20:]) / len(volumes[-20:]) if len(volumes) >= 20 else 0
        vol_ratio = round(vol_today / avg_vol20, 3) if avg_vol20 > 0 else 0.0

        result = {
            'price': price, 'prev': prev,
            'change_pct': round((price / prev - 1) * 100, 2) if prev > 0 else 0,
            'ema9': round(ema9, 4), 'ema21': round(ema21, 4), 'ema50': round(ema50, 4),
            'rsi': round(rsi, 1), 'atr_pct': atr_pct, 'volume_ratio': vol_ratio,
            'ema9_real': n >= 9, 'ema21_real': n >= 21, 'ema50_real': n >= 50, 'rsi_real': n >= 15,
            'candles_available': n, 'market': market,
            '_avg_vol20': avg_vol20,   # guardado no cache para atualizar vol_ratio no snapshot
            # [v10.48-HOTFIX] expor séries para compute_score_v3 (mesma razão do brapi)
            'closes_series': closes, 'highs_series': highs,
            'lows_series': lows, 'volumes_series': volumes,
            'source': 'Polygon', 'updated_at': datetime.utcnow().isoformat()
        }
        _set_cached_candles(f'polygon:{ticker}', result)
        record_data_quality(ticker.replace('.SA', ''), 'Polygon', lat, True)
        return result, lat
    except Exception as e:
        lat = (time.time() - t0) * 1000
        log.debug(f'Polygon {ticker}: {e}')
        record_data_quality(ticker.replace('.SA', ''), 'Polygon', lat, False)
        return None, lat

def _fetch_brapi_stock(ticker: str) -> tuple:
    """[v10.6-P0-1] Wrapper fino sobre _fetch_brapi_batch para retrocompatibilidade.
    Chamado por _fetch_single_stock() e _fetch_arbi_price() quando BRAPI_TOKEN existe.
    Retorna (result_dict | None, latency_ms).
    """
    t0 = time.time()
    res = _fetch_brapi_batch([ticker])
    lat = (time.time() - t0) * 1000
    data = res.get(ticker)
    return (data, lat) if data else (None, lat)


def _fetch_brapi_batch(tickers: list) -> dict:
    """[v10.6-P1] Busca até 20 ativos B3 em uma única chamada brapi.
    Retorna dict {ticker: result_dict}.

    Isso reduz chamadas de brapi de N req/loop para ceil(N/20) req/loop,
    e de ~2.5M/mês para ~130k/mês com candles cacheados por CANDLES_CACHE_MIN.
    """
    if not tickers or not BRAPI_TOKEN:
        return {}
    results = {}
    # Separar os que precisam de histórico dos que só precisam de snapshot
    cold = [t for t in tickers if _get_cached_candles(f'brapi:{t}') is None]
    warm = [t for t in tickers if t not in cold]

    headers = {'Authorization': f'Bearer {BRAPI_TOKEN}'}

    # ── Warm: batch snapshot, sem histórico ─────────────────────────────────
    for i in range(0, len(warm), 20):
        chunk = warm[i:i+20]
        t0 = time.time()
        try:
            r = requests.get(
                f'https://brapi.dev/api/quote/{",".join(chunk)}',
                headers=headers, timeout=8)
            lat = (time.time() - t0) * 1000
            if r.status_code != 200: continue
            for q in r.json().get('results', []):
                sym   = q.get('symbol', '').replace('.SA', '')
                price = float(q.get('regularMarketPrice') or 0)
                prev  = float(q.get('regularMarketPreviousClose') or 0)
                if price <= 0: continue
                cached = _get_cached_candles(f'brapi:{sym}')
                if cached:
                    entry = dict(cached)
                    entry['price']      = price
                    entry['prev']       = prev
                    entry['change_pct'] = round((price / prev - 1) * 100, 2) if prev > 0 else 0
                    entry['updated_at'] = datetime.utcnow().isoformat()
                    entry['source']     = 'brapi-batch-snapshot'
                    results[sym] = entry
                    record_data_quality(sym, 'brapi', lat, True)
        except Exception as e:
            log.warning(f'brapi batch snapshot chunk {chunk}: {e}')

    # ── Cold: batch com histórico, chunks de 10 (range=3mo é mais pesado) ────
    for i in range(0, len(cold), 10):
        chunk = cold[i:i+10]
        t0 = time.time()
        try:
            r = requests.get(
                f'https://brapi.dev/api/quote/{",".join(chunk)}',
                params={'range': '3mo', 'interval': '1d', 'fundamental': 'false'},
                headers=headers, timeout=12)
            lat = (time.time() - t0) * 1000
            if r.status_code != 200: continue
            for q in r.json().get('results', []):
                sym   = q.get('symbol', '').replace('.SA', '')
                price = float(q.get('regularMarketPrice') or 0)
                prev  = float(q.get('regularMarketPreviousClose') or 0)
                if price <= 0: continue

                hist  = q.get('historicalDataPrice', [])
                closes  = [c['close']  for c in hist if c.get('close')]
                highs   = [c['high']   for c in hist if c.get('high')]
                lows    = [c['low']    for c in hist if c.get('low')]
                volumes = [c['volume'] for c in hist if c.get('volume')]
                n = len(closes)
                ema9  = _ema(closes, 9)  if n >= 9  else price
                ema21 = _ema(closes, 21) if n >= 21 else price
                ema50 = _ema(closes, 50) if n >= 50 else price
                rsi   = _rsi(closes)     if n >= 15 else 50.0
                atr   = _calc_atr(closes, highs, lows, 14) if n >= 15 else 0.0
                atr_pct   = round((atr / price) * 100, 3) if price > 0 and atr > 0 else 0.0
                vol_today = float(q.get('regularMarketVolume') or 0)
                avg_vol20 = sum(volumes[-20:]) / len(volumes[-20:]) if len(volumes) >= 20 else 0
                vol_ratio = round(vol_today / avg_vol20, 3) if avg_vol20 > 0 else 0.0

                entry = {
                    'price': price, 'prev': prev,
                    'change_pct': round((price / prev - 1) * 100, 2) if prev > 0 else 0,
                    'ema9': round(ema9, 4), 'ema21': round(ema21, 4), 'ema50': round(ema50, 4),
                    'rsi': round(rsi, 1), 'atr_pct': atr_pct, 'volume_ratio': vol_ratio,
                    'ema9_real': n >= 9, 'ema21_real': n >= 21,
                    'ema50_real': n >= 50, 'rsi_real': n >= 15,
                    'candles_available': n, 'market': 'B3',
                    # [v10.48-HOTFIX] expor séries para compute_score_v3
                    # Sem estas chaves, V3_STOCK_SKIP (log.debug invisível) bloqueia TODAS
                    # as aberturas de stocks desde v10.47 (SISTEMA APENAS V3).
                    'closes_series': closes, 'highs_series': highs,
                    'lows_series': lows, 'volumes_series': volumes,
                    'source': 'brapi-batch-cold', 'updated_at': datetime.utcnow().isoformat()
                }
                _set_cached_candles(f'brapi:{sym}', entry)
                results[sym] = entry
                record_data_quality(sym, 'brapi', lat, True)
        except Exception as e:
            log.warning(f'brapi batch cold chunk {chunk}: {e}')

    return results


def _fetch_single_stock(sym: str) -> tuple:
    """[v10.4] Camada de dados: Polygon (US) → brapi (B3) → FMP → Yahoo.
    Sempre retorna atr_pct e volume_ratio quando disponível.
    """
    is_b3 = sym.endswith('.SA') or bool(re.match(r'^[A-Z]{4}[0-9]+$', sym))
    display = sym.replace('.SA', '')

    # 0. [v10.31] Cedro socket (real-time, primary for B3)
    if is_b3 and _cedro_socket and _cedro_socket.enabled:
        result, lat = _fetch_cedro_stock(display)
        if result:
            return result, lat

    # 1. brapi para B3
    if is_b3 and BRAPI_TOKEN:
        result, lat = _fetch_brapi_stock(display)
        if result: return result, lat

    # 2. Polygon para US (e ADR de B3 quando brapi indisponível)
    if POLYGON_API_KEY:
        if not is_b3:
            result, lat = _fetch_polygon_stock(display)
            if result: return result, lat
        else:
            # [v10.5-1] ADR map real — não tentar ticker B3 diretamente no Polygon
            adr_sym = B3_TO_ADR.get(display)
            if adr_sym:
                result, lat = _fetch_polygon_stock(adr_sym)
                if result and result.get('price', 0) > 0:
                    # Converter preço USD → BRL usando fx_rates
                    usd_brl = fx_rates.get('USDBRL', 5.8)
                    price_brl = round(result['price'] * usd_brl, 2)
                    result['price'] = price_brl
                    result['prev']  = round(result.get('prev', 0) * usd_brl, 2)
                    result['ema9']  = round(result.get('ema9', 0) * usd_brl, 4)
                    result['ema21'] = round(result.get('ema21', 0) * usd_brl, 4)
                    result['ema50'] = round(result.get('ema50', 0) * usd_brl, 4)
                    result['market'] = 'B3'
                    result['source'] = f'Polygon-ADR({adr_sym})'
                    return result, lat
            # Sem ADR mapeado: não tentar Polygon com ticker B3 — vai retornar 404

    # 3. FMP fallback
    if FMP_API_KEY:
        try:
            t0 = time.time()
            r = requests.get(
                f'https://financialmodelingprep.com/api/v3/quote/{display}',
                params={'apikey': FMP_API_KEY}, timeout=8)
            lat = (time.time() - t0) * 1000
            if r.status_code == 200:
                d = r.json()
                if d and isinstance(d, list):
                    q = d[0]; price = float(q.get('price') or 0); prev = float(q.get('previousClose') or 0)
                    if price > 0:
                        result = {
                            'price': price, 'prev': prev,
                            'change_pct': round(float(q.get('changesPercentage') or 0), 2),
                            'ema9': price, 'ema21': price, 'ema50': price,
                            'rsi': 50.0, 'atr_pct': 0.0, 'volume_ratio': 0.0,
                            'ema9_real': False, 'ema21_real': False, 'ema50_real': False, 'rsi_real': False,
                            'candles_available': 0, 'market': 'B3' if is_b3 else 'NYSE',
                            'source': 'FMP', 'updated_at': datetime.utcnow().isoformat()
                        }
                        record_data_quality(display, 'FMP', lat, True)
                        return result, lat
        except Exception as e:
            log.debug(f'FMP fallback {display}: {e}')

    # 4. Yahoo Finance último recurso
    t0 = time.time()
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=3mo',
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        lat = (time.time() - t0) * 1000
        if r.status_code != 200: return None, lat
        data   = r.json()['chart']['result'][0]
        meta   = data['meta']
        price  = float(meta.get('regularMarketPrice') or 0)
        prev   = float(meta.get('chartPreviousClose') or 0)
        if price <= 0: return None, lat
        closes = [c for c in data.get('indicators', {}).get('quote', [{}])[0].get('close', []) if c]
        n = len(closes)
        ema9  = _ema(closes, 9)  if n >= 9  else price
        ema21 = _ema(closes, 21) if n >= 21 else price
        ema50 = _ema(closes, 50) if n >= 50 else price
        rsi   = _rsi(closes)     if n >= 15 else 50.0
        atr   = _calc_atr(closes, [], [], 14)
        atr_pct = round((atr / price) * 100, 3) if price > 0 and atr > 0 else 0.0
        result = {
            'price': price, 'prev': prev,
            'change_pct': round((price / prev - 1) * 100, 2) if prev > 0 else 0,
            'ema9': round(ema9, 4), 'ema21': round(ema21, 4), 'ema50': round(ema50, 4),
            'rsi': round(rsi, 1), 'atr_pct': atr_pct, 'volume_ratio': 0.0,
            'ema9_real': n >= 9, 'ema21_real': n >= 21, 'ema50_real': n >= 50, 'rsi_real': n >= 15,
            'candles_available': n, 'market': 'B3' if sym.endswith('.SA') else 'NYSE',
            'source': 'Yahoo', 'updated_at': datetime.utcnow().isoformat()
        }
        record_data_quality(display, 'Yahoo-3mo', lat, True)
        return result, lat
    except Exception as e:
        lat = (time.time() - t0) * 1000
        log.debug(f'Yahoo fallback {sym}: {e}')
        record_data_quality(display, 'Yahoo-3mo', lat, False)
        return None, lat

def fetch_stock_prices():
    """[v10.6-P2+P3] Polling com cadência inteligente por tipo de ativo e status do pregão.

    Regras de cadência reais (implementadas):
    ┌─────────────────────────┬──────────────────┬──────────────────────────┐
    │ Situação                │ B3               │ US/NYSE                  │
    ├─────────────────────────┼──────────────────┼──────────────────────────┤
    │ Posição aberta          │ ~30s (todo loop) │ ~30s (todo loop)         │
    │ Watchlist, pregão aberto│ ~60s (throttle)  │ ~30s (todo loop)         │
    │ Fora do pregão          │ 1x/30min         │ 1x/30min                 │
    └─────────────────────────┴──────────────────┴──────────────────────────┘

    Conta real de requisições com 30 B3 + 30 US:
    - Posições B3 (todo loop a 30s, 7h/dia × 22d): 2 batches × 30s = ~29.000/mês brapi max
      (em geral bem menos — apenas os ativos em posição, não todos os 30)
    - Watchlist B3 (60s throttle, 7h/dia × 22d):   2 batches × 60s = ~14.500/mês brapi max
    - Fora do pregão:                               ~800/mês brapi
    - Total máximo brapi: ~45.000/mês — 9% do plano Pro (500k).
    """
    with state_lock:
        open_syms_all = {t['symbol'] for t in stocks_open}

    b3_open   = is_b3_open()
    nyse_open = is_nyse_open()

    b3_symbols_display  = [s.replace('.SA', '') for s in STOCK_SYMBOLS_B3]
    us_symbols          = list(STOCK_SYMBOLS_US)
    with watchlist_lock:
        wl_snap = list(watchlist_symbols)
    for w in wl_snap:
        wsym = w['symbol'].upper().replace('.SA','')
        wmkt = w.get('market','US').upper()
        if wmkt == 'B3' and wsym not in b3_symbols_display:
            b3_symbols_display.append(wsym)
        elif wmkt != 'B3' and wmkt != 'CRYPTO' and wsym not in us_symbols:
            us_symbols.append(wsym)

    # ── B3 ───────────────────────────────────────────────────────────────────
    b3_open_positions = [s for s in b3_symbols_display if s in open_syms_all]
    b3_watchlist      = [s for s in b3_symbols_display if s not in open_syms_all]

    now_ts = time.time()
    B3_OFF_HOURS_INTERVAL  = 30 * 60   # 30 min fora do pregão
    B3_WATCHLIST_PREGAO_IV = 60        # [v10.6-P1-3] 60s entre updates da watchlist durante pregão

    if BRAPI_TOKEN:
        # Posições abertas B3: todo loop durante pregão
        if b3_open_positions and b3_open:
            batch_result = _fetch_brapi_batch(b3_open_positions)
            with state_lock:
                for sym, data in batch_result.items():
                    stock_prices[sym] = data

        if b3_watchlist:
            last_wl_ts = getattr(fetch_stock_prices, '_last_b3_watchlist_ts', 0)
            if b3_open:
                # [v10.6-P1-3] Throttle real: watchlist só a cada 60s mesmo com pregão aberto
                should_update_watchlist = (now_ts - last_wl_ts) >= B3_WATCHLIST_PREGAO_IV
            else:
                should_update_watchlist = (now_ts - last_wl_ts) >= B3_OFF_HOURS_INTERVAL

            if should_update_watchlist:
                batch_result = _fetch_brapi_batch(b3_watchlist)
                with state_lock:
                    for sym, data in batch_result.items():
                        stock_prices[sym] = data
                fetch_stock_prices._last_b3_watchlist_ts = now_ts

        # Posições abertas B3 fora do pregão: 1x/30min para monitorar gap de abertura
        if b3_open_positions and not b3_open:
            last_b3_pos = getattr(fetch_stock_prices, '_last_b3_pos_offhours_ts', 0)
            if now_ts - last_b3_pos > B3_OFF_HOURS_INTERVAL:
                batch_result = _fetch_brapi_batch(b3_open_positions)
                with state_lock:
                    for sym, data in batch_result.items():
                        stock_prices[sym] = data
                fetch_stock_prices._last_b3_pos_offhours_ts = now_ts

    else:
        # Sem brapi: fallback individual (Polygon ADR → FMP → Yahoo)
        if b3_open or (now_ts - getattr(fetch_stock_prices, '_last_b3_fallback_ts', 0) > B3_OFF_HOURS_INTERVAL):
            for sym in b3_symbols_display:
                result, _ = _fetch_single_stock(sym)
                if result:
                    with state_lock: stock_prices[sym] = result
                time.sleep(0.3)
            fetch_stock_prices._last_b3_fallback_ts = now_ts

    # ── US/NYSE ──────────────────────────────────────────────────────────────
    # Posições abertas: sempre atualizar (podem estar em extended hours)
    us_open_positions = [s for s in us_symbols if s in open_syms_all]
    us_watchlist      = [s for s in us_symbols if s not in open_syms_all]

    US_OFF_HOURS_INTERVAL = 30 * 60

    for sym in us_open_positions:
        result, _ = _fetch_single_stock(sym)
        if result:
            with state_lock: stock_prices[sym] = result
        time.sleep(0.15)

    # Watchlist US: normal no pregão, 1x/30min fora
    if nyse_open or (now_ts - getattr(fetch_stock_prices, '_last_us_watchlist_ts', 0) > US_OFF_HOURS_INTERVAL):
        for sym in us_watchlist:
            if sym in open_syms_all: continue
            result, _ = _fetch_single_stock(sym)
            if result:
                with state_lock: stock_prices[sym] = result
            time.sleep(0.20)
        fetch_stock_prices._last_us_watchlist_ts = now_ts


def stock_price_loop():
    """[v10.6-P3] Loop com sleep adaptativo: mais curto no pregão, mais longo fora.
    [v10.6-P0-2] Beat incremental a cada 60s durante sleep fora do pregão para
    não disparar watchdog (timeout=420s). Sleep total fora do pregão: 300s = 5 beats de 60s.
    """
    while True:
        beat('stock_price_loop')
        try:
            fetch_stock_prices()
        except Exception as e:
            log.error(f'stock_price_loop: {e}')
        beat('stock_price_loop')
        # [v10.6-P3] Cadência adaptativa: 30s durante pregão, 5min fora
        if is_b3_open() or is_nyse_open():
            time.sleep(30)
        else:
            # [v10.6-P0-2] Sleep fragmentado: 5×60s com beat intermediário
            # Watchdog timeout = 420s; fragmentos de 60s garantem < 420s entre beats
            for _ in range(5):
                time.sleep(60)
                beat('stock_price_loop')

# ═══════════════════════════════════════════════════════════════
# CRYPTO PRICES — v10.4: Binance REST primário + score composto
# ═══════════════════════════════════════════════════════════════
FMP_CRYPTO_SYMBOLS=[s.replace('USDT','USD') for s in CRYPTO_SYMBOLS]
FMP_TO_INTERNAL={s:s.replace('USD','USDT') for s in FMP_CRYPTO_SYMBOLS}

def _fetch_binance_ticker(symbol: str) -> dict:
    """[v10.4] Binance 24h ticker — preço, volume, change_pct, high, low.
    Endpoint público, sem API key. Latência típica < 80ms.
    """
    try:
        r = requests.get(
            f'https://api.binance.com/api/v3/ticker/24hr',
            params={'symbol': symbol}, timeout=6)
        if r.status_code != 200: return {}
        d = r.json()
        return {
            'price':      float(d.get('lastPrice') or 0),
            'prev':       float(d.get('prevClosePrice') or 0),
            'change_pct': float(d.get('priceChangePercent') or 0),
            'high_24h':   float(d.get('highPrice') or 0),
            'low_24h':    float(d.get('lowPrice') or 0),
            'vol_24h':    float(d.get('volume') or 0),       # volume em base coin
            'vol_quote':  float(d.get('quoteVolume') or 0),  # volume em USDT
            'n_trades':   int(d.get('count') or 0),
        }
    except Exception as e:
        log.debug(f'Binance ticker {symbol}: {e}')
        return {}

def _fetch_binance_klines(symbol: str, period: int = 20) -> dict:
    """[v10.4][v10.5-2] Binance klines para ATR e volume médio.
    [v10.50] interval='1h' em vez de '1d' — trades duram horas, não meses.
    Score_engine_v2 calcula sobre estes candles; com 1d a visão era de 3 meses.
    Usa b[7] (quoteAssetVolume, em USDT) — compatível com vol_quote do allTickers.
    b[5] é volume em moeda base (BTC, ETH…) — não comparável com quoteVolume.
    """
    try:
        r = requests.get(
            'https://api.binance.com/api/v3/klines',
            params={'symbol': symbol, 'interval': '1h', 'limit': period + 2},
            timeout=6)
        if r.status_code != 200: return {}
        bars = r.json()
        closes  = [float(b[4]) for b in bars]   # close
        highs   = [float(b[2]) for b in bars]   # high
        lows    = [float(b[3]) for b in bars]   # low
        volumes = [float(b[7]) for b in bars]   # [v10.5-2] quoteAssetVolume (USDT) — era b[5] (base)
        return {'closes': closes, 'highs': highs, 'lows': lows, 'volumes': volumes}
    except Exception as e:
        log.debug(f'Binance klines {symbol}: {e}')
        return {}

def _crypto_composite_score(ticker: dict, klines: dict, direction: str) -> int:
    """[v10.4] Score composto multi-fator para crypto.
    Substitui 'score = 50 + int(abs(change_24h)*5)' que ignorava volume e ATR.

    Fatores (todos normalizados para 0-100, depois ponderados):
    - change_pct_24h   (40%): força do movimento
    - volume_ratio     (30%): volume hoje vs média 20d — confirma movimento
    - atr_position     (20%): preço vs range do dia (high/low) — direcionalidade
    - momentum_quality (10%): número de trades normalizado — liquidez
    """
    change  = ticker.get('change_pct', 0)
    high_24 = ticker.get('high_24h', 0)
    low_24  = ticker.get('low_24h', 0)
    price   = ticker.get('price', 0)
    vol_24  = ticker.get('vol_quote', 0)
    n_tr    = ticker.get('n_trades', 0)

    closes  = klines.get('closes', [])
    highs_k = klines.get('highs', [])
    lows_k  = klines.get('lows', [])
    vols_k  = klines.get('volumes', [])

    # Fator 1: change_pct (capped em ±15%)
    change_capped = max(-15.0, min(15.0, change))
    change_factor = (change_capped + 15) / 30 * 100  # 0-100

    # Fator 2: volume ratio vs média 20d
    avg_vol20 = sum(vols_k[-20:]) / len(vols_k[-20:]) if len(vols_k) >= 20 else 0
    vol_ratio = vol_24 / avg_vol20 if avg_vol20 > 0 else 1.0
    # [v10.14-FIX] Escala corrigida: 0.5→25 | 1.0→50 | 1.5→65 | 2.0→80 | 3.0→100
    # Vol normal (1x) = 50 (neutro), não 25 (que penalizava desnecessariamente)
    vol_factor = min(100, max(0, (vol_ratio - 0.5) / 2.5 * 100))

    # Fator 3: posição no range do dia (0=low, 100=high)
    day_range = high_24 - low_24
    if day_range > 0 and price > 0:
        range_pos = ((price - low_24) / day_range) * 100
    else:
        range_pos = 50.0

    # Fator 4: liquidez (n_trades normalizado — >100k = max)
    liq_factor = min(100, (n_tr / 100_000) * 100) if n_tr > 0 else 50.0

    # [v10.14-FIX] Pesos: change=65% (dominante), vol=10%, range=15%, liq=10%
    # Crypto: o movimento de preço é o sinal mais confiável — volume é confirmação secundária
    raw = (0.65 * change_factor + 0.10 * vol_factor +
           0.15 * range_pos     + 0.10 * liq_factor)
    composite = max(5, min(95, int(raw)))

    # Para SHORT: inverter (score baixo = sinal de venda forte)
    if direction == 'SHORT':
        composite = 100 - composite
    return composite

def fetch_crypto_prices():
    """[v10.4] Binance REST primário → FMP fallback → Yahoo último recurso."""
    fetched_via_binance = False

    # 1. Binance — endpoint público, sem rate limit em bulk para poucos símbolos
    if True:  # sempre tenta Binance
        try:
            t0 = time.time()
            # allTickers em uma chamada só para eficiência
            r_all = requests.get('https://api.binance.com/api/v3/ticker/24hr', timeout=8)
            lat_bulk = (time.time() - t0) * 1000
            if r_all.status_code == 200:
                all_tickers = {d['symbol']: d for d in r_all.json()}
                for sym in CRYPTO_SYMBOLS:
                    t_data = all_tickers.get(sym, {})
                    if not t_data: continue
                    price   = float(t_data.get('lastPrice') or 0)
                    change  = float(t_data.get('priceChangePercent') or 0)
                    if price <= 0: continue
                    # [v10.9-SanityFix] Rejeitar preço suspeito: se difere >90% do anterior, é bug da API
                    prev_price = crypto_prices.get(sym, 0)
                    if prev_price > 0 and price < prev_price * 0.10:
                        log.warning(f'PRICE_SANITY: {sym} rejected price={price:.6f} (prev={prev_price:.4f}, drop>90%)')
                        continue
                    with state_lock:
                        crypto_prices[sym] = price
                        crypto_momentum[sym] = round(change, 3)
                        # Guardar dados extras para score composto
                        crypto_tickers[sym] = {
                            'price': price, 'change_pct': change,
                            'high_24h': float(t_data.get('highPrice') or 0),
                            'low_24h':  float(t_data.get('lowPrice') or 0),
                            'vol_quote': float(t_data.get('quoteVolume') or 0),
                            'n_trades': int(t_data.get('count') or 0),
                        }
                    record_data_quality(sym.replace('USDT',''), 'Binance', lat_bulk, True)
                fetched_via_binance = True

                # [v10.6-P1-5] Enriquecer crypto_tickers com atr_pct e vol_ratio reais via klines
                # Feito APÓS o bulk para não bloquear a atualização de preço.
                # Usa cache _candles_cache para evitar excesso de chamadas.
                for sym in CRYPTO_SYMBOLS:
                    cached_klines = _get_cached_candles(f'klines:{sym}', ttl_min=5)  # [v10.49] 60→5min
                    # [v10.46] 22→100 barras para score_engine_v2 (Ichimoku precisa 52+, RSI 14+)
                    if cached_klines is None or len(cached_klines.get('closes', [])) < 60:
                        klines = _fetch_binance_klines(sym, 100)
                        if klines:
                            _set_cached_candles(f'klines:{sym}', klines)
                    else:
                        klines = cached_klines

                    if not klines:
                        continue

                    closes  = klines.get('closes', [])
                    highs_k = klines.get('highs', [])
                    lows_k  = klines.get('lows', [])
                    vols_k  = klines.get('volumes', [])
                    n = len(closes)

                    with state_lock:
                        tk = crypto_tickers.get(sym, {})
                        price_tk = tk.get('price', 0)

                    if price_tk <= 0 or n < 2:
                        continue

                    atr     = _calc_atr(closes, highs_k, lows_k, 14) if n >= 15 else 0.0
                    atr_pct = round((atr / price_tk) * 100, 3) if atr > 0 else 0.0
                    avg_vol  = sum(vols_k[-20:]) / len(vols_k[-20:]) if len(vols_k) >= 20 else 0
                    vol_24h  = tk.get('vol_quote', 0)
                    vol_ratio = round(vol_24h / avg_vol, 3) if avg_vol > 0 else 0.0

                    with state_lock:
                        if sym in crypto_tickers:
                            crypto_tickers[sym]['atr_pct']   = atr_pct
                            crypto_tickers[sym]['vol_ratio']  = vol_ratio
            else:
                log.warning(f'Binance allTickers HTTP {r_all.status_code}')
        except Exception as e:
            log.warning(f'Binance bulk fetch: {e}')

    if fetched_via_binance: return

    # 2. FMP fallback
    if FMP_API_KEY:
        try:
            t0 = time.time()
            r = requests.get(
                f'https://financialmodelingprep.com/api/v3/quote/{",".join(FMP_CRYPTO_SYMBOLS)}',
                params={'apikey': FMP_API_KEY}, timeout=10)
            lat = (time.time() - t0) * 1000
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and data:
                    with state_lock:
                        for a in data:
                            internal = FMP_TO_INTERNAL.get(a.get('symbol', ''))
                            price = float(a.get('price') or 0); chg = float(a.get('changesPercentage') or 0)
                            if internal and price > 0:
                                crypto_prices[internal] = price; crypto_momentum[internal] = chg
                                record_data_quality(internal.replace('USDT', ''), 'FMP', lat, True)
                    return
        except Exception as e: log.warning(f'FMP crypto: {e}')

    # 3. Yahoo último recurso
    try:
        for sym in CRYPTO_SYMBOLS:
            t0 = time.time(); display = sym.replace('USDT', '') + '-USD'
            r = requests.get(
                f'https://query1.finance.yahoo.com/v8/finance/chart/{display}?interval=1d&range=1d',
                headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            lat = (time.time() - t0) * 1000
            if r.status_code == 200:
                meta = r.json()['chart']['result'][0]['meta']
                price = float(meta.get('regularMarketPrice') or 0)
                prev  = float(meta.get('chartPreviousClose') or 0)
                if price > 0:
                    with state_lock:
                        crypto_prices[sym] = price
                        crypto_momentum[sym] = round((price / prev - 1) * 100, 2) if prev > 0 else 0
                    record_data_quality(sym.replace('USDT', ''), 'Yahoo', lat, True)
            time.sleep(0.3)
    except Exception as e: log.error(f'Yahoo crypto: {e}')

def crypto_price_loop():
    while True:
        beat('crypto_price_loop')
        try: fetch_crypto_prices(); _update_market_regime()
        except Exception as e: log.error(f'crypto_price_loop: {e}')
        time.sleep(10)

def _update_market_regime():
    global market_regime
    with state_lock: mom=dict(crypto_momentum)
    if not mom: return
    vals=list(mom.values()); n=len(vals)
    trending=sum(1 for v in vals if abs(v)>2.0); high_vol=sum(1 for v in vals if abs(v)>8.0)  # [v10.24.1] era 5.0 — crypto volátil por natureza, 5% é normal
    mode='HIGH_VOL' if high_vol/n>0.4 else ('TRENDING' if trending/n>0.6 else 'RANGING')
    avg=sum(abs(v) for v in vals)/n
    vol='HIGH' if avg>4 else ('LOW' if avg<1 else 'NORMAL')
    market_regime={'mode':mode,'volatility':vol,'avg_change_pct':round(avg,2),'updated_at':datetime.utcnow().isoformat()}

    # [v3.0] Brain persists regime changes
    try:
        urgent_queue.put((4, next(_urgent_seq), {'kind': 'brain_regime', 'data': {
            'regime_type': mode,
            'confidence': 85 if n > 3 else 60,
            'indicators': {'volatility': vol, 'avg_change_pct': round(avg, 2),
                           'trending_pct': round(trending / max(n, 1), 2),
                           'high_vol_pct': round(high_vol / max(n, 1), 2)},
            'duration_days': 1,
            'module_signals': {'Crypto': mode, 'Stocks': 'monitoring', 'Arbitrage': 'active'}
        }}))
    except Exception:
        pass

def calc_period_pnl(trades, days):
    cutoff=(datetime.utcnow()-timedelta(days=days)).isoformat()
    return round(sum(t.get('pnl',0) for t in trades if t.get('closed_at','')>=cutoff),2)

def is_momentum_positive(trade):
    h=trade.get('pnl_history',[]); return len(h)>=3 and h[-1]>h[-2]>h[-3] and trade['pnl_pct']>-1.5

# ═══════════════════════════════════════════════════════════════
# FX RATES
# ═══════════════════════════════════════════════════════════════
def fetch_fx_rates():
    """[v10.4] frankfurter.app primário (ECB data, free, sem key, sem limite) → Yahoo fallback.
    frankfurter.app é mantido pelo Frankfurter open-source project, dados do Banco Central Europeu.
    USDBRL, GBPUSD, HKDUSD. Atualizado a cada ciclo do arbi_scan_loop (~6min).
    """
    try:
        # frankfurter.app: base USD, retorna quantas unidades de cada moeda = 1 USD
        r = requests.get(
            'https://api.frankfurter.app/latest',
            params={'from': 'USD', 'to': 'BRL,GBP,HKD,CAD,EUR'}, timeout=8)  # [v10.9] +CAD,EUR
        if r.status_code == 200:
            rates = r.json().get('rates', {})
            if rates.get('BRL', 0) > 0:
                fx_rates['USDBRL'] = round(rates['BRL'], 4)
            if rates.get('GBP', 0) > 0:
                # frankfurter retorna USD→GBP (ex: 0.79); queremos GBPUSD (ex: 1.27)
                fx_rates['GBPUSD'] = round(1.0 / rates['GBP'], 4)
            if rates.get('HKD', 0) > 0:
                fx_rates['HKDUSD'] = round(rates['HKD'], 4)
            if rates.get('CAD', 0) > 0:
                # USD→CAD (ex: 1.36); queremos CADUSD = 1 CAD em USD (ex: 0.735)
                fx_rates['CADUSD'] = round(1.0 / rates['CAD'], 4)
            if rates.get('EUR', 0) > 0:
                # USD→EUR (ex: 0.92); queremos EURUSD = 1 EUR em USD (ex: 1.085)
                fx_rates['EURUSD'] = round(1.0 / rates['EUR'], 4)
            log.info(f'FX (frankfurter.app/ECB): {fx_rates}')
            return
    except Exception as e:
        log.warning(f'frankfurter.app: {e}')
    # Yahoo fallback
    pairs = {'USDBRL': 'BRL=X', 'GBPUSD': 'GBPUSD=X', 'HKDUSD': 'HKD=X', 'CADUSD': 'CAD=X', 'EURUSD': 'EURUSD=X'}  # [v10.9-arbi]
    for key, sym in pairs.items():
        try:
            r = requests.get(
                f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=1d',
                headers={'User-Agent': 'Mozilla/5.0'}, timeout=6)
            if r.status_code == 200:
                price = r.json()['chart']['result'][0]['meta'].get('regularMarketPrice', 0)
                if price > 0: fx_rates[key] = price
        except: pass
    log.info(f'FX (Yahoo fallback): {fx_rates}')

# ═══════════════════════════════════════════════════════════════
# MONITOR TRADES
# ═══════════════════════════════════════════════════════════════
def monitor_trades():
    global stocks_capital, crypto_capital
    while True:
        beat('monitor_trades')
        time.sleep(5)
        try:
            closed_stocks=[]; closed_cryptos=[]
            with state_lock:
                now=datetime.utcnow(); to_close=[]
                for trade in stocks_open:
                    sym=trade['symbol']; pd=stock_prices.get(sym)
                    price=pd['price'] if pd else trade.get('current_price',trade['entry_price'])
                    trade['current_price']=price
                    age_h=(now-datetime.fromisoformat(trade['opened_at'])).total_seconds()/3600
                    if trade.get('direction')=='SHORT':
                        trade['pnl']=round((trade['entry_price']-price)*trade['quantity'],2)
                        trade['pnl_pct']=round((trade['entry_price']/price-1)*100,2) if price>0 else 0
                    else:
                        trade['pnl']=round((price-trade['entry_price'])*trade['quantity'],2)
                        trade['pnl_pct']=round((price/trade['entry_price']-1)*100,2)
                    h=trade.setdefault('pnl_history',[]); h.append(trade['pnl_pct'])
                    if len(h)>5: h.pop(0)
                    trade['peak_pnl_pct']=round(max(trade.get('peak_pnl_pct',0),trade['pnl_pct']),2)
                    peak=trade['peak_pnl_pct']; mkt=trade.get('market',''); reason=None
                    _adaptive_sl_s = get_adaptive_sl_pct(trade)  # [v10.16] ATR-based
                    _regime_sm, _regime_sl_m, _regime_r = get_regime_multiplier()  # [v10.17]
                    _eff_sl_s = round(_adaptive_sl_s * _regime_sl_m, 2)  # [v10.17] SL ajustado por regime
                    _timeout_s = get_dynamic_timeout_h(sym, 2.0)  # [v10.17] timeout dinâmico (default 2h)
                    # [v10.48] Trade com tese v3 válida ignora TIMEOUT — só fecha se tendência reverter
                    _has_v3_thesis = trade.get('signal_v2') in ('COMPRA', 'VENDA')

                    # ═══ EXIT ADVISOR V4 — stocks ═══════════════════════
                    # Consulta advisor ANTES do chain de exit do motor.
                    # Hierarquia: hard trailing/stop do motor > ADVISOR > resto do chain.
                    # Em shadow mode: só grava, NUNCA aplica.
                    _exit_adv_stk = None
                    try:
                        from modules.unified_brain.advisor_exit import evaluate_exit as _exadv_s
                        from modules.unified_brain.advisor_shadow import log_exit_decision as _exlog_s
                        _hold_min = int((now - datetime.fromisoformat(trade['opened_at'].replace('Z',''))).total_seconds() / 60)
                        _exit_adv_stk = _exadv_s(
                            get_db, log,
                            trade_id=trade.get('id','?'),
                            symbol=sym, asset_type='stock',
                            strategy='day_trade',
                            entry_price=trade.get('entry_price'),
                            current_price=price,
                            current_pnl=trade.get('pnl', 0),
                            current_pnl_pct=trade['pnl_pct'],
                            peak_pnl_pct=peak,
                            holding_minutes=_hold_min,
                            score_v3_entry=trade.get('score_v2'),
                            score_v3_current=None,
                            regime_v3_entry=trade.get('regime_v2'),
                            regime_v3_current=None,
                            direction=trade.get('direction'),
                            portfolio_state={'open_positions': len(stocks_open)})
                        if _exit_adv_stk and not _exit_adv_stk.get('bypassed'):
                            try:
                                _exlog_s(get_db, log,
                                          trade_id=trade.get('id','?'),
                                          symbol=sym, asset_type='stock', strategy='day_trade',
                                          entry_price=trade.get('entry_price'), current_price=price,
                                          current_pnl=trade.get('pnl',0), current_pnl_pct=trade['pnl_pct'],
                                          peak_pnl_pct=peak, holding_minutes=_hold_min,
                                          score_v3_current=None, regime_v3_current=None,
                                          decision=_exit_adv_stk, motor_action=None, motor_applied=False)
                            except Exception: pass
                            # Só age se NÃO em shadow
                            if not _exit_adv_stk.get('shadow'):
                                _ea_action = _exit_adv_stk.get('action')
                                if _ea_action == 'close':
                                    reason = 'ADVISOR_CLOSE'
                                    log.info(f"[EXIT-ADV] {sym}: CLOSE {_exit_adv_stk.get('reason','')}")
                    except Exception as _ea_e_s:
                        log.debug(f'[EXIT-ADV] stock {sym}: {_ea_e_s}')
                    # ═══ FIM EXIT ADVISOR ════════════════════════════════

                    if reason is None and peak>=TRAILING_PEAK_STOCKS and trade['pnl_pct']<=peak-TRAILING_DROP_STOCKS:
                        reason='TRAILING_STOP'  # [v10.17] triggers: peak≥1.0%, drop≥0.4% (era 1.5/0.5)

                    # ═══ [adaptive-v1] EARLY STOP STOCK ═══════════════════
                    # Corta trades stock afundando ANTES de virar STOP_LOSS catastrao.
                    # So ativa se peak < 0.4 (trade nunca foi lucrativa — trailing cuida do resto).
                    # Env vars: EARLY_STOP_STOCK_ENABLED/PCT, EARLY_ALERT_STOCK_PCT
                    if reason is None and os.environ.get('EARLY_STOP_STOCK_ENABLED','true').lower()!='false':
                        _early_stop_pct_s = float(os.environ.get('EARLY_STOP_STOCK_PCT', -0.6))
                        _early_alert_pct_s = float(os.environ.get('EARLY_ALERT_STOCK_PCT', -0.4))
                        _peak_pos_s = float(trade.get('peak_pnl_pct', 0) or 0)
                        _min_pnl_s = trade.setdefault('min_pnl_pct', trade['pnl_pct'])
                        trade['min_pnl_pct'] = round(min(_min_pnl_s, trade['pnl_pct']), 2)
                        if _peak_pos_s < 0.4:
                            if trade['pnl_pct'] <= _early_stop_pct_s:
                                reason = 'EARLY_STOP'
                                log.info(f"[EARLY-STOP] {trade['symbol']}(stock): pnl={trade['pnl_pct']:+.2f}% "
                                         f"peak={_peak_pos_s:+.2f}% — cortando antes de STOP_LOSS")
                            elif trade['pnl_pct'] <= _early_alert_pct_s:
                                if not trade.get('_early_alerted'):
                                    trade['_early_alerted'] = True
                                    log.info(f"[EARLY-ALERT] {trade['symbol']}(stock): pnl={trade['pnl_pct']:+.2f}% "
                                             f"peak={_peak_pos_s:+.2f}% — zona alerta")
                    if reason is None and trade['pnl_pct']<=-_eff_sl_s:
                        reason='STOP_LOSS'  # [v10.17] ATR × regime (era fixo -2.0%)
                    elif _has_v3_thesis and market_open_for(mkt):
                        # [v10.49-fix] V3_REVERSAL só pode disparar se mercado estiver aberto.
                        # Sem esse guard, trades fechavam com preço stale pre-pregão.
                        _should_close, _new_reg, _new_sig, _rdet = check_v3_reversal(trade, 'stock')
                        if _should_close:
                            reason='V3_REVERSAL'
                            trade['_v3_reversal_detail'] = _rdet
                            trade['_v3_new_regime'] = _new_reg
                            trade['_v3_new_signal'] = _new_sig
                            log.info(f"V3_REVERSAL {trade['symbol']}: {trade.get('regime_v2')}/{trade.get('signal_v2')} → {_new_reg}/{_new_sig} ({_rdet})")
                    elif is_trade_flat(trade, now):
                        reason='FLAT_EXIT'  # [v10.17] trade estagnada — liberar capital
                    elif age_h>=_timeout_s:
                        ext=trade.get('extensions',0)
                        if is_momentum_positive(trade) and ext<3: trade['extensions']=ext+1
                        else:                                      reason='TIMEOUT'
                    elif not market_open_for(mkt) and age_h>0.5:   reason='MARKET_CLOSE'
                    if reason:
                        # [v10.7-Fix2] Devolução de capital correta para LONG e SHORT:
                        # Debitado na abertura: position_value = entry_price * qty
                        # Retornado no fechamento: position_value + pnl
                        #   LONG:  pnl = (exit - entry) * qty  → retorna exit_price * qty   ✓
                        #   SHORT: pnl = (entry - exit) * qty  → retorna collateral + ganho ✓
                        # NÃO usar exit_price * qty para SHORT (seria capital incorreto)
                        # [v10.18] Ledger: RELEASE margin first, then PNL_CREDIT
                        # Saldo contábil correto: RELEASE devolve margem, PNL_CREDIT ajusta lucro/prejuízo
                        stocks_capital += trade['position_value']
                        ledger_record('stocks', 'RELEASE', trade['symbol'],
                                      trade['position_value'], stocks_capital, trade['id'])
                        stocks_capital += trade['pnl']
                        if trade['pnl'] != 0:
                            ledger_record('stocks', 'PNL_CREDIT', trade['symbol'],
                                          trade['pnl'], stocks_capital, trade['id'])
                        # [v11-hook] dual-write
                        _v11_on_trade_close('stocks', trade['id'],
                                             float(trade['position_value']),
                                             float(trade['pnl'] or 0),
                                             fees=float(trade.get('fees', 0) or 0))
                        # [v10.22] Record to institutional modules
                        risk_manager.record_trade_result('stocks', trade['symbol'], trade['pnl'], trade['position_value'], stocks_capital)
                        perf_stats.record_trade({
                            'strategy': 'stocks', 'symbol': trade['symbol'],
                            'pnl': trade['pnl'], 'pnl_pct': trade['pnl_pct'],
                            'entry_price': trade['entry_price'], 'exit_price': price,
                            'opened_at': trade['opened_at'], 'closed_at': now.isoformat(),
                            'confidence': trade.get('learning_confidence', 0),
                            'exit_type': reason, 'asset_type': 'stock',
                            'regime': market_regime.get('mode', 'UNKNOWN'),
                        })
                        # [v10.9-CB] Circuit breaker: SL consecutivo aumenta cooldown
                        if reason == 'STOP_LOSS':
                            symbol_sl_count[sym] = symbol_sl_count.get(sym, 0) + 1
                        else:
                            symbol_sl_count[sym] = 0  # reset ao fechar sem SL
                        _cd = SYMBOL_SL_COOLDOWNS.get(min(symbol_sl_count.get(sym,1),4), 300)
                        symbol_cooldown[sym] = time.time() + (_cd - SYMBOL_COOLDOWN_SEC)  # offset extra
                        c=dict(trade); c.update({'exit_price':price,'closed_at':now.isoformat(),'close_reason':reason,'status':'CLOSED'})
                        try:
                            apply_fee_to_trade(c)  # [v10.14] fee simulado
                        except Exception as _fe:
                            log.debug(f"apply_fee_to_trade stock: {_fe}")
                        stocks_closed.insert(0,c)
                        # [v10.9] Sem limite em memória — histórico completo
                        to_close.append(trade['id']); closed_stocks.append(c)
                stocks_open[:] = [t for t in stocks_open if t['id'] not in to_close]

                to_close_c=[]
                for trade in crypto_open:
                    sym=trade['symbol']+'USDT'
                    # [v10.8-Fix] Nunca usar price=0 ou price suspeito (< 5% do entry)
                    _raw_price = crypto_prices.get(sym, 0)
                    _entry = trade.get('entry_price', 0)
                    # Sanity check: preço válido deve ser > 0 e > 5% do entry_price
                    # Preços tipo 0.0001 quando entry=0.92 são bugs da API Binance
                    _price_sane = (_raw_price > 0 and
                                   (_entry <= 0 or _raw_price >= _entry * 0.05))
                    price = _raw_price if _price_sane else trade.get('current_price', _entry)
                    if price <= 0: price = _entry if _entry > 0 else 1  # último fallback
                    age_h=(now-datetime.fromisoformat(trade['opened_at'])).total_seconds()/3600
                    trade['current_price']=price
                    if trade.get('direction')=='SHORT':
                        trade['pnl']=round((trade['entry_price']-price)*trade['quantity'],2)
                        trade['pnl_pct']=round((trade['entry_price']/price-1)*100,2) if price>0 else 0
                    else:
                        trade['pnl']=round((price-trade['entry_price'])*trade['quantity'],2)
                        trade['pnl_pct']=round((price/trade['entry_price']-1)*100,2)
                    h=trade.setdefault('pnl_history',[]); h.append(trade['pnl_pct'])
                    if len(h)>5: h.pop(0)
                    trade['peak_pnl_pct']=round(max(trade.get('peak_pnl_pct',0),trade['pnl_pct']),2)
                    peak=trade['peak_pnl_pct']; reason=None
                    _adaptive_sl_c = get_adaptive_sl_pct(trade)  # [v10.16] ATR-based
                    _regime_cm, _regime_csl_m, _regime_cr = get_regime_multiplier()  # [v10.17]
                    _eff_sl_c = round(_adaptive_sl_c * _regime_csl_m, 2)  # [v10.17] SL ajustado por regime
                    _timeout_c = get_dynamic_timeout_h(trade['symbol'], 4.0)  # [v10.17] timeout dinâmico (default 4h)
                    # [v10.48] Trade com tese v3 válida ignora TIMEOUT — só fecha se tendência reverter
                    _has_v3_thesis = trade.get('signal_v2') in ('COMPRA', 'VENDA')

                    # ═══ EXIT ADVISOR V4 — crypto ═══════════════════════
                    # REGRA INTOCÁVEL: advisor NUNCA fecha crypto em zona trailing.
                    # (enforced em advisor_exit.is_crypto_in_trailing_protection)
                    # Trailing do motor V3 continua sendo o dono da captura de lucro.
                    _exit_adv_cry = None
                    try:
                        from modules.unified_brain.advisor_exit import evaluate_exit as _exadv_c
                        from modules.unified_brain.advisor_shadow import log_exit_decision as _exlog_c
                        _hold_min_c = int((now - datetime.fromisoformat(trade['opened_at'].replace('Z',''))).total_seconds() / 60)
                        _exit_adv_cry = _exadv_c(
                            get_db, log,
                            trade_id=trade.get('id','?'),
                            symbol=trade['symbol'], asset_type='crypto',
                            strategy='day_trade',
                            entry_price=trade.get('entry_price'),
                            current_price=price,
                            current_pnl=trade.get('pnl', 0),
                            current_pnl_pct=trade['pnl_pct'],
                            peak_pnl_pct=peak,
                            holding_minutes=_hold_min_c,
                            score_v3_entry=trade.get('score_v2'),
                            score_v3_current=None,
                            regime_v3_entry=trade.get('regime_v2'),
                            regime_v3_current=None,
                            direction=trade.get('direction'),
                            portfolio_state={'open_positions': len(crypto_open)})
                        if _exit_adv_cry and not _exit_adv_cry.get('bypassed'):
                            try:
                                _exlog_c(get_db, log,
                                          trade_id=trade.get('id','?'),
                                          symbol=trade['symbol'], asset_type='crypto', strategy='day_trade',
                                          entry_price=trade.get('entry_price'), current_price=price,
                                          current_pnl=trade.get('pnl',0), current_pnl_pct=trade['pnl_pct'],
                                          peak_pnl_pct=peak, holding_minutes=_hold_min_c,
                                          score_v3_current=None, regime_v3_current=None,
                                          decision=_exit_adv_cry, motor_action=None, motor_applied=False)
                            except Exception: pass
                            if not _exit_adv_cry.get('shadow'):
                                _ea_action = _exit_adv_cry.get('action')
                                # close só é possível se advisor_exit permitir (regra intocável garante que NÃO é close em trailing)
                                if _ea_action == 'close':
                                    reason = 'ADVISOR_CLOSE'
                                    log.info(f"[EXIT-ADV] {trade['symbol']}: CLOSE {_exit_adv_cry.get('reason','')}")
                    except Exception as _ea_e_c:
                        log.debug(f'[EXIT-ADV] crypto {trade["symbol"]}: {_ea_e_c}')
                    # ═══ FIM EXIT ADVISOR ════════════════════════════════

                    if reason is None and peak>=TRAILING_PEAK_CRYPTO and trade['pnl_pct']<=peak-TRAILING_DROP_CRYPTO:
                        reason='TRAILING_STOP'  # [v10.17] triggers: peak≥1.5%, drop≥0.7% (era 2.0/1.0)

                    # ═══ [adaptive-v1] EARLY STOP CRYPTO ═══════════════════
                    # Corta trades que estão afundando ANTES de virar STOP_LOSS catastrão.
                    # Só ativa se peak < 0.4% (trade nunca foi lucrativa — trailing cuida do resto).
                    # Env vars:
                    #   EARLY_STOP_CRYPTO_ENABLED (default true)
                    #   EARLY_STOP_CRYPTO_PCT     (default -0.6)
                    #   EARLY_ALERT_CRYPTO_PCT    (default -0.4)  — só loga, não fecha
                    if reason is None and os.environ.get('EARLY_STOP_CRYPTO_ENABLED','true').lower()!='false':
                        _early_stop_pct = float(os.environ.get('EARLY_STOP_CRYPTO_PCT', -0.6))
                        _early_alert_pct = float(os.environ.get('EARLY_ALERT_CRYPTO_PCT', -0.4))
                        _peak_pos = float(trade.get('peak_pnl_pct', 0) or 0)
                        _min_pnl = trade.setdefault('min_pnl_pct', trade['pnl_pct'])
                        trade['min_pnl_pct'] = round(min(_min_pnl, trade['pnl_pct']), 2)
                        # Só considera stop se peak NUNCA passou de 0.4 (senão trailing cuida)
                        if _peak_pos < 0.4:
                            if trade['pnl_pct'] <= _early_stop_pct:
                                reason = 'EARLY_STOP'
                                log.info(f"[EARLY-STOP] {trade['symbol']}: pnl={trade['pnl_pct']:+.2f}% "
                                         f"peak={_peak_pos:+.2f}% — cortando antes de virar STOP_LOSS")
                            elif trade['pnl_pct'] <= _early_alert_pct:
                                # Throttle: alerta 1x por trade (usa _early_alerted flag)
                                if not trade.get('_early_alerted'):
                                    trade['_early_alerted'] = True
                                    log.info(f"[EARLY-ALERT] {trade['symbol']}: pnl={trade['pnl_pct']:+.2f}% "
                                             f"peak={_peak_pos:+.2f}% — zona de alerta (-0.4%)")
                    if reason is None and trade['pnl_pct']<=-_eff_sl_c:
                        reason='STOP_LOSS'  # [v10.17] ATR × regime
                    elif _has_v3_thesis and os.environ.get('V3_REVERSAL_CRYPTO_ENABLED', 'true').lower() != 'false':
                        # [v10.48] Trade com tese v3: só fecha se v3 reverter
                        # [adaptive-v1] Guard: V3_REVERSAL crypto pode ser desabilitado via env var
                        _should_close, _new_reg, _new_sig, _rdet = check_v3_reversal(trade, 'crypto')
                        if _should_close:
                            reason='V3_REVERSAL'
                            trade['_v3_reversal_detail'] = _rdet
                            trade['_v3_new_regime'] = _new_reg
                            trade['_v3_new_signal'] = _new_sig
                            log.info(f"V3_REVERSAL {trade['symbol']}: {trade.get('regime_v2')}/{trade.get('signal_v2')} → {_new_reg}/{_new_sig} ({_rdet})")
                    elif is_trade_flat(trade, now):
                        reason='FLAT_EXIT'  # [v10.17] trade estagnada — liberar capital
                    elif age_h>=_timeout_c:
                        ext=trade.get('extensions',0)
                        if is_momentum_positive(trade) and ext<3: trade['extensions']=ext+1
                        else:                                      reason='TIMEOUT'
                    if reason:
                        # [v10.18] Ledger: RELEASE margin first, then PNL_CREDIT
                        crypto_capital += trade['position_value']
                        # [v11-hook] dual-write crypto close
                        _v11_on_trade_close('crypto', trade['id'],
                                             float(trade['position_value']),
                                             float(trade['pnl'] or 0),
                                             fees=float(trade.get('fees', 0) or 0))
                        ledger_record('crypto', 'RELEASE', trade['symbol'],
                                      trade['position_value'], crypto_capital, trade['id'])
                        crypto_capital += trade['pnl']
                        if trade['pnl'] != 0:
                            ledger_record('crypto', 'PNL_CREDIT', trade['symbol'],
                                          trade['pnl'], crypto_capital, trade['id'])
                        # [v10.22] Record to institutional modules
                        risk_manager.record_trade_result('crypto', trade['symbol'], trade['pnl'], trade['position_value'], crypto_capital)
                        perf_stats.record_trade({
                            'strategy': 'crypto', 'symbol': trade['symbol'],
                            'pnl': trade['pnl'], 'pnl_pct': trade['pnl_pct'],
                            'entry_price': trade['entry_price'], 'exit_price': price,
                            'opened_at': trade['opened_at'], 'closed_at': now.isoformat(),
                            'confidence': trade.get('learning_confidence', 0),
                            'exit_type': reason, 'asset_type': 'crypto',
                            'regime': market_regime.get('mode', 'UNKNOWN'),
                        })
                        symbol_cooldown[trade['symbol']]=time.time()
                        c=dict(trade); c.update({'exit_price':price,'closed_at':now.isoformat(),'close_reason':reason,'status':'CLOSED'})
                        try:
                            apply_fee_to_trade(c)  # [v10.14] fee simulado
                        except Exception as _fe:
                            log.debug(f"apply_fee_to_trade crypto: {_fe}")
                        crypto_closed.insert(0,c)
                        # [v10.9] Sem limite em memória — histórico completo
                        to_close_c.append(trade['id']); closed_cryptos.append(c)
                crypto_open[:] = [t for t in crypto_open if t['id'] not in to_close_c]

            for c in closed_stocks:
                # [v10.17] Track duration for dynamic timeout
                try:
                    _dur_h = (datetime.fromisoformat(c['closed_at']) - datetime.fromisoformat(c['opened_at'])).total_seconds() / 3600
                    update_symbol_duration(c['symbol'], _dur_h)
                except: pass
                audit('TRADE_CLOSED',{'id':c['id'],'symbol':c['symbol'],'pnl':c['pnl'],'reason':c['close_reason']})
                enqueue_persist('trade',c)
                enqueue_persist('cooldown',symbol=c['symbol'],ts=symbol_cooldown.get(c['symbol'],time.time()))
                alert_trade_closed(c)
                # [v10.14] Cooldown 2h após TRAILING_STOP lucrativo — evita re-entrada que devolve ganhos
                # Análise: 26/55 re-entradas perderam $17.5K (36.6% dos ganhos do trailing devolvidos)
                if c.get('close_reason') == 'TRAILING_STOP' and float(c.get('pnl',0)) > 0:
                    _trailing_stop_cooldown[c['symbol']] = time.time()
                    log.info(f'[TRAIL-COOLDOWN] {c["symbol"]}: 2h cooldown após TRAILING_STOP +${float(c.get("pnl",0)):,.0f}')
                # [L-7] Aprender com o resultado do trade
                process_trade_outcome(c)
                # [v10.13] Atualizar estado cross-market após fechar trade de stock
                if c.get('asset_type') == 'stock':
                    try: _update_cross_market_from_stocks()
                    except: pass
            for c in closed_cryptos:
                # [v10.17] Track duration for dynamic timeout
                try:
                    _dur_h = (datetime.fromisoformat(c['closed_at']) - datetime.fromisoformat(c['opened_at'])).total_seconds() / 3600
                    update_symbol_duration(c['symbol'], _dur_h)
                except: pass
                audit('TRADE_CLOSED',{'id':c['id'],'symbol':c['symbol'],'pnl':c['pnl'],'reason':c['close_reason']})
                enqueue_persist('trade',c)
                enqueue_persist('cooldown',symbol=c['symbol'],ts=symbol_cooldown.get(c['symbol'],time.time()))
                alert_trade_closed(c)
                # [L-7] Aprender com o resultado do trade
                process_trade_outcome(c)
                # [v10.13] Atualizar estado cross-market após fechar trade de stock
                if c.get('asset_type') == 'stock':
                    try: _update_cross_market_from_stocks()
                    except: pass
            beat('monitor_trades')  # [v10.9] beat após processamento — evita FROZEN com muitas trades
        except Exception as e: log.error(f'monitor_trades: {e}')

# ═══════════════════════════════════════════════════════════════
# [V9-1] STOCK EXECUTION WORKER — create_order FORA do state_lock
# ═══════════════════════════════════════════════════════════════
def stock_execution_worker():
    global stocks_capital
    while True:
        beat('stock_execution_worker')
        time.sleep(60)
        beat('stock_execution_worker')
        # [v10.47] Emergency stop: pula tudo se env var setada
        if V3_EMERGENCY_STOP:
            log.info('[STOCK-LOOP] V3_EMERGENCY_STOP ativo — pulando iteração')
            continue
        try:
            # [v10.9] Gerar sinais inline do stock_prices em memória
            # (não depende mais do market_signals no banco — igual ao crypto)
            with state_lock:
                sp_snap = dict(stock_prices)
            now_iso = datetime.utcnow().isoformat()
            rows = []
            for sym, pd_data in sp_snap.items():
                if not pd_data or pd_data.get('price', 0) <= 0: continue
                mkt_type = 'B3' if re.match(r'^[A-Z]{4}[0-9]+$', sym) else 'NYSE'  # [adaptive-v1] pattern match
                rsi  = pd_data.get('rsi', 50) or 50
                ema9 = pd_data.get('ema9', 0)  or 0
                ema21= pd_data.get('ema21',0)  or 0
                # [v10.11] Score composto multi-fator — mais discriminante que RSI+EMA simples
                score = 50
                # RSI (30 pontos): oversold/overbought com gradação
                if   rsi < 30: score += 25    # extremo oversold — forte COMPRA
                elif rsi < 40: score += 15    # oversold
                elif rsi < 50: score += 5     # neutro-baixo
                elif rsi > 70: score -= 25    # extremo overbought — forte VENDA
                elif rsi > 60: score -= 15    # overbought
                elif rsi > 50: score -= 5     # neutro-alto
                # EMA cross (25 pontos): alinhamento de tendência
                ema50 = pd_data.get('ema50', 0) or 0
                if ema9 > 0 and ema21 > 0:
                    if ema9 > ema21:
                        score += 12           # EMA9 > EMA21: tendência de alta
                        if ema50 > 0 and ema21 > ema50: score += 8  # EMA21 > EMA50: tendência forte
                    else:
                        score -= 12           # EMA9 < EMA21: tendência de baixa
                        if ema50 > 0 and ema21 < ema50: score -= 8  # EMA21 < EMA50: tendência fraca
                # Volume (10 pontos): confirma movimento
                vol_ratio = pd_data.get('volume_ratio', 0) or 0
                if vol_ratio > 1.5: score += 8    # volume acima da média — confirma
                elif vol_ratio < 0.5: score -= 5  # volume baixo — sinal fraco
                # ATR / Volatilidade (10 pontos): filtrar alta volatilidade
                atr_pct = pd_data.get('atr_pct', 0) or 0
                if 0 < atr_pct < 1.5: score += 5  # volatilidade saudável
                elif atr_pct > 4.0: score -= 10   # volatilidade excessiva — risco alto
                # Preço vs EMA9 (10 pontos): preço acima/abaixo da média rápida
                price = pd_data.get('price', 0) or 0
                if price > 0 and ema9 > 0:
                    if price > ema9 * 1.01: score += 7   # preço acima EMA9 — momentum
                    elif price < ema9 * 0.99: score -= 7  # preço abaixo EMA9 — fraqueza
                score = max(0, min(100, score))
                # [v10.12] Score dinâmico por learning — mais fatores, maior impacto
                _rsi_bkt = 'OVERSOLD' if rsi<30 else ('LOW' if rsi<45 else ('OVERBOUGHT' if rsi>75 else ('HIGH' if rsi>65 else 'NEUTRAL')))
                _ema_align  = 'BULLISH_STACK' if (ema9>ema21 and ema21>ema50 and ema50>0) else ('BEARISH_STACK' if (ema9<ema21 and ema21<ema50 and ema50>0) else ('BULLISH' if ema9>ema21 else 'BEARISH'))
                _vol_bucket = 'LOW' if (vol_ratio>0 and vol_ratio<0.8) else ('HIGH' if vol_ratio>1.8 else 'NORMAL')
                _atr_bucket = 'EXTREME' if atr_pct>4 else ('HIGH' if atr_pct>2.5 else ('LOW' if atr_pct<0.8 else 'NORMAL'))
                _direction  = 'LONG' if score>50 else 'SHORT'
                _score_adj  = 0
                _pattern_blocked = False
                with learning_lock:
                    # Fator RSI (±12 pts)
                    _fs_rsi = factor_stats_cache.get(('rsi_bucket', _rsi_bucket), {})
                    if _fs_rsi.get('total_samples',0) >= 10:
                        _score_adj += int(_fs_rsi.get('confidence_weight',0) * 12)
                    # Fator EMA alignment (±12 pts)
                    _fs_ema = factor_stats_cache.get(('ema_alignment', _ema_align), {})
                    if _fs_ema.get('total_samples',0) >= 10:
                        _score_adj += int(_fs_ema.get('confidence_weight',0) * 12)
                    # Fator volatilidade (±8 pts)
                    _fs_vol = factor_stats_cache.get(('volatility_bucket', _vol_bucket), {})
                    if _fs_vol.get('total_samples',0) >= 10:
                        _score_adj += int(_fs_vol.get('confidence_weight',0) * 8)
                    # Fator ATR (±10 pts — ATR extremo penaliza muito)
                    _fs_atr = factor_stats_cache.get(('atr_bucket', _atr_bucket), {})
                    if _fs_atr.get('total_samples',0) >= 10:
                        _score_adj += int(_fs_atr.get('confidence_weight',0) * 10)
                    # Fator direção (±6 pts)
                    _fs_dir = factor_stats_cache.get(('direction', _direction), {})
                    if _fs_dir.get('total_samples',0) >= 5:
                        _score_adj += int(_fs_dir.get('confidence_weight',0) * 6)
                    # Bloqueio: padrão com WR<40% e n≥30 — não executar mesmo se score OK
                    for _ph, _ps in list(pattern_stats_cache.items())[:200]:
                        _pn = _ps.get('total_samples',0)
                        _pw = _ps.get('wins',0)
                        if _pn >= 30 and _pw/_pn < 0.40 and _ps.get('ewma_hit_rate',1) < 0.45:
                            _pattern_blocked = True
                            break
                score = max(0, min(100, score + _score_adj))
                # [v10.14-FIX] _pattern_blocked: checar por DIREÇÃO
                # LONG fraco: score < 75 → bloquear
                # SHORT fraco: score > 25 → bloquear (SHORTs fortes têm score BAIXO)
                _pre_dir = 'LONG' if score > 50 else 'SHORT'
                _is_weak_long  = (_pre_dir == 'LONG'  and score < MIN_SCORE_AUTO + 5)
                _is_weak_short = (_pre_dir == 'SHORT' and score > (100 - MIN_SCORE_AUTO - 5))
                if _pattern_blocked and (_is_weak_long or _is_weak_short):
                    continue  # bloquear sinal fraco de padrão ruim
                # [v10.13] Ajuste temporal para stocks
                _now_s = datetime.utcnow()
                # [adaptive-v1] B3 pattern CODE+NUMBER
                _mkt_type = 'B3' if re.match(r'^[A-Z]{4}[0-9]+$', sym) else 'NYSE'
                _st_adj, _st_blocked, _st_reason = get_temporal_stock_score(_now_s.hour, _now_s.weekday(), _mkt_type)
                # [v10.14-FIX] Temporal block NÃO bloqueia SHORTs — foi calibrado só com LONGs
                # SHORTs têm comportamento diferente em janelas ruins para LONGs
                _pre_temporal_dir = 'SHORT' if score <= 50 else 'LONG'
                if _st_blocked and _pre_temporal_dir == 'LONG':
                    log.debug(f"STOCK_TEMPORAL_BLOCK: {sym} — {_st_reason}")
                    continue
                elif _st_blocked and _pre_temporal_dir == 'SHORT':
                    _st_adj = -5  # penaliza leve mas não bloqueia SHORT
                _score_before_t = score
                # [v10.14-FIX] Temporal: LONGS beneficiam de hora boa (score+)
                # SHORTs TAMBÉM devem beneficiar — mas score de SHORT é BAIXO
                # Bom horário com adj positivo → ajuda LONG mas PREJUDICA SHORT
                # Fix: para direção SHORT (score < 50), inverter o sinal do ajuste
                _pre_dir_t = 'LONG' if score > 50 else ('SHORT' if score < 50 else 'NEUTRAL')
                if _pre_dir_t == 'SHORT' and _st_adj > 0:
                    # Hora boa ajuda SHORT TAMBÉM → subtrair (mantém score baixo)
                    _st_adj_effective = -_st_adj
                elif _pre_dir_t == 'SHORT' and _st_adj < 0:
                    # Hora ruim → penalizar SHORT (subir score tira da zona SHORT)
                    _st_adj_effective = -_st_adj  # penalidade vira boost (sai da zona)
                else:
                    _st_adj_effective = _st_adj
                score = max(0, min(100, score + _st_adj_effective))
                # [v10.13] Padrões compostos descobertos automaticamente
                _feats_disc = {'score_bucket': _score_bucket(score), 'rsi_bucket': _rsi_bkt,
                               'ema_alignment': 'BULLISH' if ema9>ema21 else 'BEARISH',
                               'weekday': str(_now_s.weekday()), 'hour_utc': str(_now_s.hour),
                               'time_bucket': _time_bucket(_now_s), 'market_type': _mkt_type,
                               'asset_type': 'stock', 'direction': 'LONG' if score>50 else 'SHORT',
                               'volatility_bucket': 'LOW' if atr_pct<1 else ('HIGH' if atr_pct>3 else 'NORMAL'),
                               'atr_bucket': 'EXTREME' if atr_pct>4 else ('HIGH' if atr_pct>2.5 else 'NORMAL'),
                               'volume_bucket': 'HIGH' if vol_ratio>1.5 else ('LOW' if vol_ratio<0.5 else 'NORMAL')}
                try:
                    _disc_adj, _disc_blocked, _disc_key = get_composite_score_adj(_feats_disc)
                except Exception as _ge:
                    _disc_adj, _disc_blocked, _disc_key = 0, False, ''
                if _disc_blocked:
                    log.debug(f"COMPOSITE_BLOCK stock {sym}: {_disc_key}")
                    continue
                if _disc_adj != 0:
                    score = max(0, min(100, score + _disc_adj))
                    log.debug(f"COMPOSITE_ADJ stock {sym}: {_disc_adj:+d} via {_disc_key}")
                if abs(_st_adj) >= 5:
                    log.debug(f"STOCK_SCORE_ADJ: {sym} {_score_before_t}→{score} ({_st_reason})")
                # [v10.47] SISTEMA APENAS V3 — score v1 acima é ignorado, só v3 decide
                # Se v3 falhar para este símbolo, skip (não abrir trade)
                regime_v2_val = None; signal_v2_val = None
                try:
                    from modules.score_engine_v2 import compute_score_v3 as _csv3
                    _c = pd_data.get('closes_series', [])
                    _h = pd_data.get('highs_series', [])
                    _l = pd_data.get('lows_series', [])
                    _v = pd_data.get('volumes_series', [])
                    if len(_c) < 30 or len(_h) != len(_c) or len(_l) != len(_c):
                        log.debug(f"V3_STOCK_SKIP {sym}: séries insuficientes (closes={len(_c)})")
                        continue
                    _r = _csv3(_c, _h, _l, _v,
                               factor_stats_cache=factor_stats_cache,
                               pattern_stats_cache=pattern_stats_cache,
                               temporal_adj=float(_st_adj_effective or 0))
                    score = _r['score']
                    regime_v2_val = _r['regime']
                    signal_v2_val = _r['signal']
                    log.info(f"V3_STOCK {sym}: score={score} regime={regime_v2_val} signal={signal_v2_val}")
                except Exception as _e:
                    log.warning(f"V3_STOCK_FAIL {sym}: {_e} — símbolo pulado")
                    continue
                signal_val = 'COMPRA' if score >= MIN_SCORE_AUTO else ('VENDA' if score <= (100-MIN_SCORE_AUTO) else 'MANTER')  # [v10.24-FIX] was MIN_SCORE_AUTO_CRYPTO
                rows.append({
                    'symbol': sym, 'price': pd_data.get('price', 0),
                    'score': score, 'signal': signal_val,
                    'score_v2': score,  # [v10.47] sempre v3
                    'regime_v2': regime_v2_val,
                    'signal_v2': signal_v2_val,
                    'market_type': mkt_type, 'asset_type': 'stock',
                    'rsi': rsi, 'ema9': ema9, 'ema21': ema21,
                    'ema50': pd_data.get('ema50', 0) or 0,
                    'atr_pct': pd_data.get('atr_pct', 0),
                    'volume_ratio': pd_data.get('volume_ratio', 0),
                    'created_at': now_iso,
                    'id': None,
                })

            for sig in rows:
                score=sig.get('score',0); mkt=sig.get('market_type','')
                signal_val=sig.get('signal',''); sym=sig.get('symbol','')
                price=sig.get('price', 0)
                if price<=0: continue
                # [v10.12] Threshold variável por confiança do padrão
                _eff_min = MIN_SCORE_AUTO
                with learning_lock:
                    _best_pat = max(pattern_stats_cache.values(), 
                                   key=lambda p: p.get('wins',0)/max(p.get('total_samples',1),1),
                                   default={})
                    if _best_pat.get('total_samples',0)>=50:
                        _pat_wr = _best_pat.get('wins',0)/_best_pat['total_samples']
                        if _pat_wr >= 0.80: _eff_min = max(65, MIN_SCORE_AUTO-5)   # padrão muito confiável
                        elif _pat_wr >= 0.70: _eff_min = MIN_SCORE_AUTO            # padrão ok
                is_long=score>=_eff_min and signal_val=='COMPRA'
                is_short=score<=(100-_eff_min) and signal_val=='VENDA'
                # [adaptive-v1] Guard: bloquear SHORT stocks se env var dizer
                _allow_short = os.environ.get('ALLOW_SHORT_STOCKS', 'true').lower() != 'false'
                if is_short and not _allow_short:
                    log.info(f'[SHORT-BLOCK] {sym}: ALLOW_SHORT_STOCKS=false — trade vetada')
                    is_short = False
                if is_short:
                    log.info(f'[SHORT-DBG] {sym} score={score} _eff_min={_eff_min} is_short={is_short} signal_val={signal_val}')
                if not (is_long or is_short): continue

                # [BAD-HOURS-BLOCK 29/abr/2026] Filtro de horarios catastroficos B3 LONG.
                # Backtested em 3548 trades historicas:
                #  10:30-11:00 BRT (UTC 13:30-14): n=202, WR 42.4%, avg -$155, total -$31k
                #  14:00-15:00 BRT (UTC 17): n=211, WR 39.1%, avg -$86, total -$18k
                # Filtro reduz n=2533 (-1015 trades) e ganha +$33k vs baseline.
                if is_long and mkt_type == 'B3':
                    from datetime import datetime
                    _now_utc = datetime.utcnow()
                    _h_utc = _now_utc.hour
                    _m_utc = _now_utc.minute
                    if (_h_utc == 13 and _m_utc >= 30) or _h_utc == 17:
                        log.info(f'[BAD-HOURS-BLOCK] {sym} LONG B3 bloqueado: UTC {_h_utc:02d}:{_m_utc:02d} (BRT {(_h_utc-3)%24:02d}:{_m_utc:02d}) historicamente catastrofico')
                        continue

                # [v10.9-TrendFilter] Bloquear LONGs em ações com queda >5% nos últimos 5 preços
                # Previne loops como RAIZ4 — ação em tendência de queda não deve receber COMPRA
                if is_long:
                    _hist = pd_data.get('price_history') or []
                    if len(_hist) >= 5:
                        _p_now  = pd_data.get('price', 0)
                        _p_5ago = _hist[-5] if len(_hist) >= 5 else _hist[0]
                        if _p_5ago > 0 and (_p_now - _p_5ago) / _p_5ago * 100 < -5.0:
                            log.debug(f'TREND_FILTER: {sym} LONG bloqueado — queda {(_p_now-_p_5ago)/_p_5ago*100:.1f}% em 5 períodos')
                            continue

                # ── Deduplicação + política de re-avaliação ─────────────────────────────────
                # Motivos PERMANENTES: nunca re-avaliar dentro da mesma janela de sinal.
                # [v10.3.2-P0-3] 'executed' agora é permanente — evita reprocessar sinal que já virou trade.
                PERMANENT_REASONS = {'kill_switch', 'symbol_duplicate', 'executed'}
                # Motivos TEMPORÁRIOS: re-avaliar se o contexto que causou o bloqueio mudou.
                # Mapeamento reason → função de checagem (True = ainda bloqueado).
                # [v10.9] Para sinais inline (id=None), janela de 60s para dedup
                _sig_time_window = int(time.time() / 60)
                ms_key = str(sig.get('id') or f"{sym}:{score}:{_sig_time_window}")
                origin_key = ms_key[:120]
                with learning_lock:
                    cached = processed_signal_ids.get(ms_key)

                # [v10.3.2-P0-1] _sig_pre_id preservado do cache; gen_id() SÓ para sinal novo.
                # A linha que sobrescrevia o valor do cache foi removida.
                if cached:
                    if cached['reason'] in PERMANENT_REASONS:
                        continue
                    reason_was = cached['reason']
                    _sig_pre_id = cached['sig_id']   # reusar ID existente — não gerar novo
                    # Verificar se o contexto ainda bloqueia
                    # Mapeamento cobre TODAS as strings reais devolvidas por check_risk()
                    still_blocked = False
                    if reason_was == 'market_closed':
                        still_blocked = not market_open_for(mkt)
                    elif reason_was in ('SYMBOL_COOLDOWN', 'cooldown', 'COOLDOWN'):
                        # [v10.3.2-P0-2] float timestamp + SYMBOL_COOLDOWN_SEC
                        still_blocked = (time.time() - symbol_cooldown.get(sym, 0)) < SYMBOL_COOLDOWN_SEC
                    elif reason_was in ('INSUFFICIENT_CAPITAL', 'capital',
                                        'STOCKS_CAPITAL_LIMIT', 'CRYPTO_CAPITAL_LIMIT', 'NO_CAPITAL_CRYPTO'):
                        # [v10.3.4-F3] Replica a lógica REAL do check_risk():
                        # STOCKS_CAPITAL_LIMIT: committed + desired > INITIAL * MAX_CAPITAL_PCT/100
                        # Não basta olhar stocks_capital livre — precisa checar capital comprometido.
                        if reason_was in ('STOCKS_CAPITAL_LIMIT', 'capital', 'INSUFFICIENT_CAPITAL'):
                            committed_s = sum(t.get('position_value', 0) for t in stocks_open)
                            score_factor_tmp = min(abs(score - 50) / 50.0, 1.0)
                            conf_tmp = calc_learning_confidence(
                                {'symbol': sym, 'asset_type': 'stock', 'market_type': mkt, 'score': score},
                                {}, '')
                            rm_tmp = get_risk_multiplier(conf_tmp)
                            desired_tmp = min(stocks_capital * (0.08 + score_factor_tmp * 0.07) * rm_tmp,
                                              MAX_POSITION_STOCKS)
                            cap_limit = INITIAL_CAPITAL_STOCKS * MAX_CAPITAL_PCT_STOCKS / 100
                            still_blocked = (committed_s + desired_tmp) > cap_limit
                        else:
                            # [v10.5-6] CRYPTO_CAPITAL_LIMIT no stock_execution_worker:
                            # approved_size não existe neste escopo (é variável do auto_trade_crypto).
                            # Usar desired_pos calculado para a posição corrente como proxy.
                            committed_c = sum(t.get('position_value', 0) for t in crypto_open)
                            cap_limit_c = INITIAL_CAPITAL_CRYPTO * MAX_CAPITAL_PCT_CRYPTO / 100
                            score_factor_c = min(abs(score - 50) / 50.0, 1.0)
                            desired_c = min(crypto_capital * (0.05 + score_factor_c * 0.05),
                                            MAX_POSITION_CRYPTO)
                            still_blocked = (committed_c + desired_c) > cap_limit_c
                    elif reason_was.startswith('MAX_OPEN_POSITIONS'):
                        # [v10.3.3-F2] Bloqueado por limite global — checar se abrimos menos
                        still_blocked = len(stocks_open) + len(crypto_open) >= MAX_OPEN_POSITIONS
                    elif reason_was.startswith('MAX_POSITIONS_STOCKS'):
                        still_blocked = len(stocks_open) >= MAX_POSITIONS_STOCKS
                    elif reason_was.startswith('MAX_POSITIONS_CRYPTO'):
                        still_blocked = len(crypto_open) >= MAX_POSITIONS_CRYPTO
                    elif reason_was.startswith('MAX_POSITION_SAME_MKT'):
                        # [v10.3.4-F4] Constante sempre existe — definida no topo do arquivo
                        mkt_count = sum(1 for t in stocks_open if t.get('market') == mkt)
                        still_blocked = mkt_count >= MAX_POSITION_SAME_MKT
                    elif reason_was.startswith('SYMBOL_ALREADY_OPEN'):
                        still_blocked = sym in {t['symbol'] for t in stocks_open + crypto_open}
                    # [v10.3.4-F5] DRAWDOWN ativa kill_switch internamente — tratar como permanente
                    # já no primeiro evento (mesmo que RISK_KILL_SWITCH ainda não fosse True no split()[0])
                    elif reason_was in ('KILL_SWITCH_ACTIVE', 'KILL_SWITCH', 'ARBI_KILL_SWITCH') \
                            or reason_was.startswith(('DAILY_DRAWDOWN', 'WEEKLY_DRAWDOWN')):
                        still_blocked = True
                    else:
                        # Motivo desconhecido ou temporário genérico → tentar de novo
                        still_blocked = False
                    if still_blocked:
                        continue
                    # Contexto mudou — reavaliação usando o signal_id já existente no banco
                else:
                    _sig_pre_id = gen_id('SIG')   # sinal novo: gerar ID agora
                    # Registrar no cache com LRU
                    with learning_lock:
                        if len(processed_signal_ids) >= MAX_PROCESSED_SIGNALS_CACHE:
                            keys_to_drop = list(processed_signal_ids.keys())[:MAX_PROCESSED_SIGNALS_CACHE // 2]
                            for k in keys_to_drop: del processed_signal_ids[k]
                        processed_signal_ids[ms_key] = {'sig_id': _sig_pre_id, 'reason': 'processing'}

                direction='LONG' if is_long else 'SHORT'
                score_factor=min(abs(score-50)/50.0,1.0)

                # [L-1/L-5] Extrair features e calcular confidence para TODOS os sinais acionáveis
                now_dt   = datetime.utcnow()
                dq_score = get_dq_score(sym)
                mkt_open = market_open_for(mkt)
                price_dict = stock_prices.get(sym, {})
                sig_enriched = dict(sig)
                # [v10.14] Features comportamentais para aprendizado
                _sym_trades_today = [t for t in list(stocks_closed)
                                     if t.get('symbol')==sym
                                     and (t.get('closed_at','') or '')[:10] == datetime.utcnow().strftime('%Y-%m-%d')]
                _last_close_reason = _sym_trades_today[0].get('close_reason','NONE') if _sym_trades_today else 'NONE'
                _had_trailing_today = any(t.get('close_reason')=='TRAILING_STOP' for t in _sym_trades_today)
                _same_day_count_str = '1st' if len(_sym_trades_today)==0 else ('2nd' if len(_sym_trades_today)==1 else '3rd+')
                sig_enriched.update({
                    'price':        price,
                    'asset_type':   'stock',
                    'market_open':  mkt_open,
                    'trade_open':   sym in {t['symbol'] for t in stocks_open},
                    'atr_pct':      price_dict.get('atr_pct', 0.0),       # [v10.4]
                    'volume_ratio': price_dict.get('volume_ratio', 0.0),   # [v10.4]
                    # [v10.14] Comportamentais
                    'reentry_after_trailing': 'YES' if _had_trailing_today else 'NO',
                    'same_day_count':         _same_day_count_str,
                    'close_reason_prev':      _last_close_reason,
                })
                features = extract_features(sig_enriched, dict(market_regime), dq_score, now_dt)
                features['_dq_score'] = dq_score
                feat_hash = make_feature_hash(features)
                conf      = calc_learning_confidence(sig_enriched, features, feat_hash)
                insight   = generate_insight(sig_enriched, features, feat_hash, conf)
                risk_mult = get_risk_multiplier(conf)

                # [v10.9-DeadZone] Bloquear faixa de confiança com performance historicamente negativa
                # Dados mostram: 55-64 = 38-44% WR e -$53K em perdas. Faixa 40-54 e 65+ OK.
                # [v10.14-FIX] Dead zone NÃO se aplica a SHORTs puros — foi calibrada só com LONGs
                # SHORTs têm WR 53.8% histórico — penalizá-los com dead zone é um bug estrutural
                _lc = conf.get('final_confidence', 50)
                _is_short_signal = (direction == 'SHORT')
                if _is_short_signal:
                    log.info(f'[SHORT-DBG2] {sym} conf={_lc:.1f} dead_zone={LEARNING_DEAD_ZONE_LOW}-{LEARNING_DEAD_ZONE_HIGH} skip_dz={_is_short_signal}')
                if not _is_short_signal and LEARNING_DEAD_ZONE_LOW <= _lc < LEARNING_DEAD_ZONE_HIGH:
                    _confirmed_sig_id = record_signal_event(sig_enriched, features, feat_hash, conf, insight,
                                        source_type='stock_signal_db', existing_signal_id=_sig_pre_id,
                                        origin_signal_key=origin_key)
                    record_shadow_decision(_confirmed_sig_id, sig_enriched, 'learning_dead_zone')
                    _cache_reason('learning_dead_zone')
                    continue


                # Filtros de execução — gravar signal_event + shadow antes de qualquer continue/break
                # [v10.6.3-Fix1] _confirmed_sig_id: começa com _sig_pre_id e é atualizado para o ID
                # real que o banco confirma via ON DUPLICATE KEY em record_signal_event().
                # Sem isso, o cache pode guardar o ID tentado em vez do ID persistido, causando
                # shadow_decisions ligados ao ID errado — simétrico ao fix de crypto em v10.6.2.
                _confirmed_sig_id = _sig_pre_id

                def _cache_reason(reason: str):
                    with learning_lock:
                        processed_signal_ids[ms_key] = {'sig_id': _confirmed_sig_id, 'reason': reason}

                if not mkt_open:
                    _confirmed_sig_id = record_signal_event(sig_enriched, features, feat_hash, conf, insight,
                                        source_type='stock_signal_db',
                                        existing_signal_id=_sig_pre_id,
                                        origin_signal_key=origin_key)
                    record_shadow_decision(_confirmed_sig_id, sig_enriched, 'market_closed')
                    _cache_reason('market_closed')
                    continue

                # [v10.11] Posição maior para reduzir capital parado — 15 posições × $200K+ = $3M investido
                # [v10.14] Posição baseada no portfolio TOTAL de stocks (não só capital livre)
                # stocks_capital = capital livre (pode ser pequeno quando cheio de trades)
                # Usar: MAX_POSITION_STOCKS como teto real, e floor de $50K
                _stocks_port_total = s.get('stocks_portfolio_value', INITIAL_CAPITAL_STOCKS) if False else                     max(stocks_capital + sum(t.get('position_value',0) for t in stocks_open), INITIAL_CAPITAL_STOCKS)
                _regime_size_m, _regime_sl_tmp, _regime_info = get_regime_multiplier()  # [v10.17]
                _pos_target = _stocks_port_total / MAX_POSITIONS_STOCKS * (0.8 + score_factor * 0.4)
                # [v10.47] Score sizing multiplier — mais capital em scores altos
                _score_mult = get_score_sizing_mult(score)
                desired_pos = min(max(_pos_target * risk_mult * _regime_size_m * _score_mult, 50_000), MAX_POSITION_STOCKS)
                if _score_mult != 1.0:
                    log.info(f'[STK-SCORE-SIZE] {sym}: score={score} mult={_score_mult:.1f}x desired={desired_pos:,.0f}')
                # [v10.16] Strategy daily drawdown check
                _dd_blocked_s, _dd_reason_s = check_strategy_daily_dd('stocks')
                if _dd_blocked_s:
                    log.info(f'[STK-DD-BLOCK] {sym}: {_dd_reason_s}')
                    break  # não adianta checar mais sinais
                # [v10.16] Auto-blacklist check
                _bl_blocked_s, _bl_reason_s = is_symbol_blacklisted(sym)
                if _bl_blocked_s:
                    log.info(f'[STK-BL-BLOCK] {sym}: {_bl_reason_s}')
                    continue
                # [v10.17] Directional exposure check
                _dir_blocked_s, _dir_reason_s, _dir_stats_s = check_directional_exposure(direction, 'stocks')
                if _dir_blocked_s:
                    log.info(f'[STK-DIR-BLOCK] {sym}: {_dir_reason_s}')
                    continue
                # [v10.15] ML Gate para stocks — bloqueia se padrões/fatores indicam perda
                _ml_ok_s, _ml_reason_s, _ml_score_s = should_trade_ml(
                    features, conf, asset_type='stock')
                if not _ml_ok_s:
                    log.info(f'[STK-ML-BLOCK] {sym}: {_ml_reason_s} score={score}')
                    record_shadow_decision(_confirmed_sig_id, sig_enriched, _ml_reason_s)
                    with learning_lock:
                        processed_signal_ids[ms_key] = {'sig_id': _confirmed_sig_id, 'reason': _ml_reason_s}
                    continue
                risk_ok,risk_reason,approved_size=check_risk(sym,mkt,desired_pos,'stocks')
                if not risk_ok:
                    # [v10.3.4-F1] Preservar o motivo REAL do bloqueio, não colapsar em 'risk_blocked'
                    real_reason = risk_reason.split()[0] if risk_reason else 'risk_blocked'
                    # [v10.3.4-F5] DAILY/WEEKLY_DRAWDOWN dispara kill_switch internamente —
                    # tratar como permanente já no primeiro evento, sem esperar KILL_SWITCH_ACTIVE
                    is_permanent_risk = ('KILL_SWITCH' in risk_reason
                                         or risk_reason.startswith(('DAILY_DRAWDOWN', 'WEEKLY_DRAWDOWN')))
                    _confirmed_sig_id = record_signal_event(sig_enriched, features, feat_hash, conf, insight,
                                        source_type='stock_signal_db',
                                        existing_signal_id=_sig_pre_id,
                                        origin_signal_key=origin_key)
                    record_shadow_decision(_confirmed_sig_id, sig_enriched, real_reason)
                    _cache_reason('kill_switch' if is_permanent_risk else real_reason)
                    log.info(f'Risk-1 {sym}: {risk_reason} (dir={direction})')
                    if is_permanent_risk: break
                    continue
                qty=int(approved_size/price)
                if qty<=0: continue

                # [V91-1] Gerar IDs ANTES do lock — trade já nasce com identidade formal
                # [L-2] Registrar signal_event com intenção de executar
                trade = None; pre_trade_id = gen_id('STK'); pre_order_id = gen_id('ORD')
                order_side = 'BUY' if direction=='LONG' else 'SELL'
                # [v10.3.2-P0-1] signal_id = retorno real do banco (via ON DUPLICATE KEY, pode ser o antigo)
                # [v10.6.3-Fix1] Atualizar _confirmed_sig_id para que _cache_reason use o ID correto
                signal_id  = record_signal_event(
                    sig_enriched, features, feat_hash, conf, insight,
                    source_type='stock_signal_db',
                    existing_signal_id=_sig_pre_id,
                    origin_signal_key=origin_key)
                _confirmed_sig_id = signal_id
                _cache_reason('executed')

                with state_lock:
                    # [v10.22] Pre-trade risk + kill switch check
                    ks_ok, ks_reason = kill_switch_middleware.check_before_trade('stocks', get_db)
                    if not ks_ok:
                        log.warning(f'[KILL-SWITCH] Stock blocked: {ks_reason}')
                        continue
                    # [v10.51] Portfolio TOTAL para stocks também
                    _total_stocks_portfolio = stocks_capital + sum(t.get('position_value', 0) for t in stocks_open)
                    _total_stocks_portfolio = max(_total_stocks_portfolio, INITIAL_CAPITAL_STOCKS)
                    risk_ok, risk_reason = risk_manager.check_can_open('stocks', sym, price*qty, _total_stocks_portfolio)
                    if not risk_ok:
                        log.warning(f'[RISK-BLOCK] Stock {sym}: {risk_reason}')
                        continue

                    # ═══ BRAIN ADVISOR V4 — Entry (stocks) ═══════════════
                    # IA propõe, motor decide. Shadow mode por padrão.
                    _adv_decision_stk = None
                    _adv_qty_stk = qty
                    try:
                        from modules.unified_brain.advisor_entry import evaluate_entry as _adv_eval_e
                        _adv_decision_stk = _adv_eval_e(
                            get_db, log,
                            symbol=sym, asset_type='stock',
                            strategy='day_trade',
                            score_v3=sig.get('score_v2') or score,
                            regime_v3=sig.get('regime_v2'),
                            direction=direction, atr_pct=sig.get('atr_pct', 0),
                            market_type=mkt,
                            hour_of_day=datetime.utcnow().hour,
                            weekday=datetime.utcnow().weekday(),
                            portfolio_state={'open_positions': len(stocks_open),
                                             'max_positions': 20},
                            feature_hash=feat_hash,
                            learning_confidence=conf.get('final_confidence'))
                        if _adv_decision_stk and not _adv_decision_stk.get('bypassed'):
                            # SEMPRE logar a decisão (shadow ou não, block ou não)
                            try:
                                from modules.unified_brain.advisor_shadow import log_entry_decision as _adv_log_e
                                _would_block_active = (not _adv_decision_stk.get('shadow')
                                                        and not _adv_decision_stk.get('approve'))
                                _adv_log_e(get_db, log,
                                          trade_id=pre_trade_id,
                                          symbol=sym, asset_type='stock', strategy='day_trade',
                                          market_type=mkt, direction=direction,
                                          score_v3=sig.get('score_v2') or score,
                                          regime_v3=sig.get('regime_v2'),
                                          atr_pct=sig.get('atr_pct', 0),
                                          hour_of_day=datetime.utcnow().hour,
                                          weekday=datetime.utcnow().weekday(),
                                          decision=_adv_decision_stk,
                                          motor_opened=(not _would_block_active),
                                          motor_size_used=None)
                            except Exception: pass
                            # Aplicar BLOCK apenas se NÃO shadow
                            if not _adv_decision_stk.get('shadow') and not _adv_decision_stk.get('approve'):
                                log.info(f"[ADVISOR-ENTRY] {sym}: BLOCK {_adv_decision_stk.get('reason','')}")
                                record_shadow_decision(signal_id, sig_enriched,
                                    f"advisor_block_{_adv_decision_stk.get('reason','')}"[:60])
                                continue
                            # Aplicar size_multiplier se NÃO shadow
                            if not _adv_decision_stk.get('shadow'):
                                _sm = float(_adv_decision_stk.get('size_multiplier', 1.0))
                                _adv_qty_stk = max(1, int(qty * _sm))
                    except Exception as _adv_e_stk:
                        log.debug(f'[ADVISOR-ENTRY] stock {sym}: {_adv_e_stk}')
                        _adv_decision_stk = None
                    # ═══ FIM BRAIN ADVISOR ════════════════════════════════

                    ok2,reason2=_second_validation(sym,mkt,'stocks')
                    if ok2 and stocks_capital>=price*_adv_qty_stk:
                        qty = _adv_qty_stk  # advisor pode ter ajustado qty
                        stocks_capital -= price*qty
                        # [v10.18] Ledger: RESERVE
                        # [v11-hook] dual-write stocks open — trade_id calc antes do INSERT
                        try:
                            _v11_open_tmp_id = locals().get('new_trade_id') or locals().get('trade_id') or sym + ':' + str(int(time.time()))
                            _v11_on_trade_open('stocks', _v11_open_tmp_id,
                                                float(price * qty),
                                                metadata={'symbol': sym})
                        except Exception: pass
                        ledger_record('stocks', 'RESERVE', sym,
                                      round(price*qty, 2), stocks_capital, pre_trade_id)
                        # [V91-1] order_id já está no trade dentro do lock
                        trade = {
                            'id':pre_trade_id,'symbol':sym,'market':mkt,'asset_type':'stock',
                            'direction':direction,'entry_price':price,'current_price':price,
                            'quantity':qty,'position_value':round(price*qty,2),
                            'pnl':0,'pnl_pct':0,'peak_pnl_pct':0,'score':score,
                            'signal':signal_val,'order_id':pre_order_id,
                            'opened_at':datetime.utcnow().isoformat(),'status':'OPEN',
                            # [L-7] campos de attribution
                            'signal_id':           signal_id,
                            'feature_hash':        feat_hash,
                            'learning_confidence': conf.get('final_confidence'),
                            'insight_summary':     insight,
                            'learning_version':    LEARNING_VERSION,
                            '_features':           features,
                            # [v10.16] Score snapshot + ATR para SL adaptativo
                            '_score_snapshot':     make_score_snapshot(sig_enriched, features, conf),
                            '_atr_pct':            sig.get('atr_pct', 0),
                            # [v10.46] Score v3 fields
                            'score_v2':            sig.get('score_v2'),
                            'regime_v2':           sig.get('regime_v2'),
                            'signal_v2':           sig.get('signal_v2'),
                        }
                        stocks_open.append(trade)
                    else:
                        log.info(f'Risk-2 {sym}: {reason2 if not ok2 else "insufficient_capital"}')
                        # [L-8] Shadow: registrar sinal bloqueado no segundo nível
                        block_reason2 = reason2 if not ok2 else 'capital'
                        record_shadow_decision(signal_id, sig_enriched, block_reason2)
                        # [S3] symbol_duplicate é permanente para esta janela
                        if 'DUPLICATE' in (reason2 or '').upper():
                            with learning_lock:
                                processed_signal_ids[ms_key] = {'sig_id': signal_id, 'reason': 'symbol_duplicate'}
                        else:
                            with learning_lock:
                                processed_signal_ids[ms_key] = {'sig_id': signal_id, 'reason': block_reason2}

                if trade is None: continue

                # [FIX-2] Vincular trade_id e order_id ao signal_event imediatamente
                update_signal_attribution(signal_id, pre_trade_id, pre_order_id)

                # [V91-1] Fora do lock: criar ordem com o ID já definido, depois atualizar status
                order = create_order(pre_trade_id, sym, order_side, 'MARKET', qty, price, 'stocks',
                                     order_id_override=pre_order_id)
                update_order_status(order,'VALIDATED')
                update_order_status(order,'SENT')
                update_order_status(order,'FILLED',price,qty)

                audit('TRADE_OPENED',{'id':pre_trade_id,'symbol':sym,'direction':direction,'score':score,'pos':round(price*qty)})
                enqueue_persist('trade',trade)
                if score>=ALERT_MIN_SCORE: alert_signal(dict(sig))
                _last_trade_opened['stocks'] = time.time()  # [v10.16] inactivity tracking
                log.info(f'STK {sym} {direction} qty={qty} score={score}')
        except Exception as e:
            import traceback as _tb
            log.error(f'stock_execution_worker: {e}\n{_tb.format_exc()[:800]}')

# ═══════════════════════════════════════════════════════════════
# [V9-1] CRYPTO AUTO-TRADE — create_order FORA do state_lock
# ═══════════════════════════════════════════════════════════════
def auto_trade_crypto():
    global crypto_capital
    while True:
        beat('auto_trade_crypto')
        time.sleep(90)
        beat('auto_trade_crypto')
        # [v10.47] Emergency stop: pula tudo se env var setada
        if V3_EMERGENCY_STOP:
            log.info('[CRYPTO-LOOP] V3_EMERGENCY_STOP ativo — pulando iteração')
            continue
        try:
            if market_regime.get('mode')=='HIGH_VOL':
                log.info('[CRYPTO] HIGH_VOL regime — sizing reduced 0.6x via get_regime_multiplier')  # [v10.24.1] não bloquear mais — sizing já é reduzido
            log.info(f'[CRYPTO-LOOP] precos={len(crypto_prices)} momentum={len(crypto_momentum)} regime={market_regime.get("mode")}')
            for sym in CRYPTO_SYMBOLS:
                display=sym.replace('USDT',''); price=crypto_prices.get(sym,0)
                change_24h=crypto_momentum.get(sym,0)
                if price<=0 or abs(change_24h)<0.3:  # [v10.24] era 0.5 — muito restritivo para mercado lateral
                    log.info(f'[CRYPTO-SKIP] {display}: price={price:.2f} change={change_24h:.2f}%')
                    continue
                direction='LONG' if change_24h>0 else 'SHORT'

                # [v10.4] Score composto multi-fator (substitui change_24h * 5)
                ticker_data = crypto_tickers.get(sym, {})
                if ticker_data:
                    # [v10.6.2-Fix4] Cache unificado: usa _candles_cache com TTL=60min (klines diários).
                    # Elimina o segundo cache privado auto_trade_crypto._klines_cache — fonte única.
                    kline_cache_key = f'klines:{sym}'
                    klines_data = _get_cached_candles(kline_cache_key, ttl_min=5) or {}  # [v10.49] 60→5min
                    if not klines_data:
                        # [v10.46] Aumentado 22→100 barras para suportar score_engine_v2 (Ichimoku precisa 52+)
                        klines_data = _fetch_binance_klines(sym, 100)
                        if klines_data:
                            _set_cached_candles(kline_cache_key, klines_data)
                    # [v10.47] SISTEMA APENAS V3 — crypto score vem direto do v3
                    closes_k = klines_data.get('closes', [])
                    highs_k  = klines_data.get('highs', [])
                    lows_k   = klines_data.get('lows', [])
                    vols_k   = klines_data.get('volumes', [])
                    regime_v2_c = None; signal_v2_c = None
                    try:
                        from modules.score_engine_v2 import compute_score_v3 as _csv3c
                        if len(closes_k) < 30:
                            log.debug(f"V3_CRYPTO_SKIP {sym}: closes={len(closes_k)} < 30")
                            continue
                        _rc = _csv3c(closes_k, highs_k, lows_k, vols_k,
                                     factor_stats_cache=factor_stats_cache,
                                     pattern_stats_cache=pattern_stats_cache)
                        score = _rc['score']
                        regime_v2_c = _rc['regime']
                        signal_v2_c = _rc['signal']
                        log.info(f"V3_CRYPTO {sym}: score={score} regime={regime_v2_c} sig={signal_v2_c}")
                    except Exception as _e:
                        log.warning(f"V3_CRYPTO_FAIL {sym}: {_e} — símbolo pulado")
                        continue
                    atr_c    = _calc_atr(closes_k, highs_k, lows_k, 14) if len(closes_k) >= 15 else 0.0
                    atr_pct_c = round((atr_c / price) * 100, 3) if price > 0 and atr_c > 0 else 0.0
                    avg_vol20_c = sum(vols_k[-20:]) / len(vols_k[-20:]) if len(vols_k) >= 20 else 0
                    vol_ratio_c = round(ticker_data.get('vol_quote', 0) / avg_vol20_c, 3) if avg_vol20_c > 0 else 0.0
                else:
                    # Fallback sem dados Binance
                    score = min(50 + int(abs(change_24h) * 5), 95)
                    if direction == 'SHORT': score = 100 - score
                    atr_pct_c = 0.0; vol_ratio_c = 0.0

                # [v10.13] Ajuste temporal + cross-market no score de crypto
                _now_c = datetime.utcnow()
                _t_adj, _t_blocked, _t_reason = get_temporal_crypto_score(_now_c.hour, _now_c.weekday())
                if _t_blocked:
                    log.info(f"[CRYPTO-TBLOCK] {display}: {_t_reason}")
                    continue
                _cm_adj = get_cross_market_crypto_adj()
                _score_before = score
                # [v10.29] Limitar penalidade temporal — caps ajustados para Quarta/Sexta
                # Se crypto se move >3%, o mercado está em tendência — limitar mas não ignorar
                _raw_change = float(ticker_data.get('change_pct', 0))
                _strong_signal = abs(_raw_change) > 3.0  # [v10.29] era 2.0 — mais restritivo para override temporal
                if _strong_signal and (_t_adj + _cm_adj) < 0:
                    # Sinal forte: penalidade máxima -12 (era -8) — mesmo sinais fortes respeitam dias ruins
                    _capped_t = max(_t_adj + _cm_adj, -12)
                    score = max(0, min(100, score + _capped_t))
                else:
                    # [v10.29] Sinal normal: penalidade máxima -20 (era -12) — Quarta/Sexta pesam mais
                    _total_t = _t_adj + _cm_adj
                    _capped_t = max(_total_t, -20) if _total_t < 0 else _total_t
                    score = max(0, min(100, score + _capped_t))
                # [v10.13] Padrões compostos
                _rsi_c = float(ticker_data.get('rsi',50) or 50)
                _feats_disc_c = {'score_bucket': _score_bucket(score), 'rsi_bucket': _rsi_bucket(_rsi_c),
                                 'weekday': str(_now_c.weekday()), 'hour_utc': str(_now_c.hour),
                                 'time_bucket': _time_bucket(_now_c), 'asset_type': 'crypto',
                                 'market_type': 'CRYPTO', 'direction': direction,
                                 'volatility_bucket': 'LOW' if atr_pct_c<1 else ('HIGH' if atr_pct_c>3 else 'NORMAL'),
                                 'btc_trend': _cross_market_state.get('btc_change_24h',0)>2 and 'UP' or (_cross_market_state.get('btc_change_24h',0)<-2 and 'DOWN' or 'FLAT'),
                                 'stocks_regime': 'BAD' if _cross_market_state.get('stocks_wr_today',50)<45 else ('GOOD' if _cross_market_state.get('stocks_wr_today',50)>=58 else 'NEUTRAL')}
                try:
                    _disc_adj_c, _disc_blocked_c, _disc_key_c = get_composite_score_adj(_feats_disc_c)
                except Exception as _ge:
                    _disc_adj_c, _disc_blocked_c, _disc_key_c = 0, False, ''
                if _disc_blocked_c:
                    log.info(f"[CRYPTO-CBLOCK] {display}: {_disc_key_c}")
                    continue
                if _disc_adj_c != 0:
                    score = max(0, min(100, score + _disc_adj_c))
                if abs(_t_adj + _cm_adj) >= 5:
                    log.info(f"[CRYPTO-SADJ] {display}: {_score_before}→{score} (t={_t_adj:+d} cm={_cm_adj:+d} disc={_disc_adj_c:+d})")

                # [v10.16] Strategy daily drawdown check
                _dd_blocked_c, _dd_reason_c = check_strategy_daily_dd('crypto')
                if _dd_blocked_c:
                    log.info(f'[CRYPTO-DD-BLOCK] {display}: {_dd_reason_c}')
                    break
                # [v10.16] Auto-blacklist check
                _bl_blocked_c, _bl_reason_c = is_symbol_blacklisted(display)
                if _bl_blocked_c:
                    log.info(f'[CRYPTO-BL-BLOCK] {display}: {_bl_reason_c}')
                    continue
                # [v10.17] Directional exposure check
                _dir_blocked_c, _dir_reason_c, _dir_stats_c = check_directional_exposure(direction, 'crypto')
                if _dir_blocked_c:
                    log.info(f'[CRYPTO-DIR-BLOCK] {display}: {_dir_reason_c}')
                    continue
                # [v10.24.2-FIX] _crypto_composite_score() já inverte o score para SHORT
                # (linha 4650: composite = 100 - composite). Portanto score ALTO = sinal
                # forte para AMBAS as direções. Threshold único: score >= MIN_SCORE_AUTO_CRYPTO.
                _entry_ok = score >= MIN_SCORE_AUTO_CRYPTO
                if not _entry_ok:
                    log.info(f'[CRYPTO-THRESHOLD] {display}: score={score} dir={direction} threshold={MIN_SCORE_AUTO_CRYPTO} -> BLOCKED')
                    continue

                score_factor=min(abs(score-50)/50.0,1.0)

                # [v10.4-F2-dedup] Chave por janela de tempo de 90s — não por preço (instável em altcoins)
                time_window = int(time.time() / 90)   # muda a cada ciclo do loop
                ms_key_c = f"CRY:{display}:{direction}:{time_window}"
                origin_key_c = ms_key_c[:120]
                with learning_lock:
                    cached_c = processed_signal_ids.get(ms_key_c)

                if cached_c and cached_c['reason'] in ('executed', 'kill_switch'):
                    continue

                _sig_pre_id_c = cached_c['sig_id'] if cached_c else gen_id('SIG')
                if not cached_c:
                    with learning_lock:
                        if len(processed_signal_ids) >= MAX_PROCESSED_SIGNALS_CACHE:
                            keys_to_drop = list(processed_signal_ids.keys())[:MAX_PROCESSED_SIGNALS_CACHE // 2]
                            for k in keys_to_drop: del processed_signal_ids[k]
                        processed_signal_ids[ms_key_c] = {'sig_id': _sig_pre_id_c, 'reason': 'processing'}

                # [FIX-3][v10.4] Calcular features com ATR e volume_ratio
                now_dt_c   = datetime.utcnow()
                dq_score_c = get_dq_score(display)
                sig_enriched_c = {
                    'symbol': display, 'asset_type': 'crypto', 'market_type': 'CRYPTO',
                    'signal': 'COMPRA' if direction == 'LONG' else 'VENDA',
                    'score': score, 'price': price, 'rsi': 50,
                    'atr_pct': atr_pct_c,         # [v10.4]
                    'volume_ratio': vol_ratio_c,   # [v10.4]
                }
                features_c  = extract_features(sig_enriched_c, dict(market_regime), dq_score_c, now_dt_c)
                features_c['_dq_score'] = dq_score_c
                feat_hash_c = make_feature_hash(features_c)
                conf_c      = calc_learning_confidence(sig_enriched_c, features_c, feat_hash_c)
                insight_c   = generate_insight(sig_enriched_c, features_c, feat_hash_c, conf_c)
                risk_mult_c = get_risk_multiplier(conf_c)

                # [v10.18] Conviction filter — confiança mínima + movimento mínimo
                _conv_ok, _conv_reason = check_crypto_conviction(conf_c, change_24h, display)
                if not _conv_ok:
                    log.info(f'[CRYPTO-CONV-BLOCK] {display}: {_conv_reason}')
                    _csig_conv = record_signal_event(sig_enriched_c, features_c, feat_hash_c, conf_c, insight_c,
                                        source_type='crypto_signal', existing_signal_id=_sig_pre_id_c,
                                        origin_signal_key=origin_key_c)
                    record_shadow_decision(_csig_conv, sig_enriched_c, 'conviction_low')
                    with learning_lock: processed_signal_ids[ms_key_c] = {'sig_id': _csig_conv, 'reason': 'conviction_low'}
                    continue

                # [v10.15] ML Gate — consulta padrões e fatores (movido para após conf_c existir)
                _ml_ok, _ml_reason, _ml_score = should_trade_ml(
                    features_c, conf_c, asset_type='crypto')
                if not _ml_ok:
                    log.info(f'[CRYPTO-ML-BLOCK] {display}: {_ml_reason} score={score}')
                    _csig_ml = record_signal_event(sig_enriched_c, features_c, feat_hash_c, conf_c, insight_c,
                                        source_type='crypto_signal', existing_signal_id=_sig_pre_id_c,
                                        origin_signal_key=origin_key_c)
                    record_shadow_decision(_csig_ml, sig_enriched_c, _ml_reason)
                    with learning_lock: processed_signal_ids[ms_key_c] = {'sig_id': _csig_ml, 'reason': _ml_reason}
                    continue

                # [v10.9-DeadZone] Dead zone crypto — skip se movimento ≥ 2.5% (sinal real)
                _lc_c = conf_c.get('final_confidence', 50)
                _raw_change_c = float(ticker_data.get('change_pct', 0) if ticker_data else change_24h)
                _skip_dz_c = abs(_raw_change_c) >= 2.5 or abs(change_24h) >= 2.5
                if not _skip_dz_c and LEARNING_DEAD_ZONE_LOW <= _lc_c < LEARNING_DEAD_ZONE_HIGH:
                    log.info(f'[CRYPTO-DZ] {display}: conf={_lc_c:.1f} change={_raw_change_c:.1f}% → dead_zone BLOCK')
                    _csig_id = record_signal_event(sig_enriched_c, features_c, feat_hash_c, conf_c, insight_c,
                                        source_type='crypto_signal', existing_signal_id=_sig_pre_id_c,
                                        origin_signal_key=origin_key_c)
                    record_shadow_decision(_csig_id, sig_enriched_c, 'learning_dead_zone')
                    with learning_lock: processed_signal_ids[ms_key_c] = {'sig_id': _csig_id, 'reason': 'learning_dead_zone'}
                    continue

                # [v10.29] Crypto sizing — capital distribuído entre MAX_POSITIONS_CRYPTO slots
                # + dynamic WR-based sizing multiplier (Manus insight)
                _sym_max = CRYPTO_MAX_POSITION_BY_SYM.get(sym, MAX_POSITION_CRYPTO)
                # [v10.14] Posição baseada no portfolio TOTAL de crypto
                _crypto_port_total = max(
                    crypto_capital + sum(t.get('position_value',0) for t in crypto_open),
                    INITIAL_CAPITAL_CRYPTO)
                _regime_csize_m, _regime_csl_tmp, _regime_cinfo = get_regime_multiplier()  # [v10.17]
                # [v10.28] Crypto regime floor: crypto é naturalmente volátil, regime
                # HIGH_VOL não deve penalizar tanto — floor 0.75x (era 0.6x sem floor)
                _regime_csize_m = max(_regime_csize_m, 0.75)
                # [v10.28] Score factor mais agressivo para crypto: 0.80 base (era 0.70)
                _crypto_pos_target = _crypto_port_total / MAX_POSITIONS_CRYPTO * (0.80 + score_factor * 0.20)
                # [v10.28] Risk mult floor para crypto: mínimo 0.6 (era 0.3 global)
                _risk_mult_crypto = max(risk_mult_c, 0.6)
                # [v10.29] Dynamic WR-based sizing: allocate more to proven winners
                _wr_sizing_mult = get_symbol_wr_sizing_mult(display, 'crypto')
                # [v10.47] Score sizing multiplier — mais capital em scores altos
                _score_mult_c = get_score_sizing_mult(score)
                # [v10.28] Mínimo por posição: 15% do slot (era 50K fixo)
                _min_crypto_pos = max(80_000, _crypto_port_total / MAX_POSITIONS_CRYPTO * 0.12)
                desired_pos = min(max(_crypto_pos_target * _risk_mult_crypto * _regime_csize_m * _wr_sizing_mult * _score_mult_c, _min_crypto_pos), _sym_max)
                if _wr_sizing_mult != 1.0 or _score_mult_c != 1.0:
                    log.info(f'[CRYPTO-SIZE] {display}: wr_mult={_wr_sizing_mult:.2f} score_mult={_score_mult_c:.1f}x score={score} desired={desired_pos:,.0f}')
                risk_ok,risk_reason,approved_size=check_risk(display,'CRYPTO',desired_pos,'crypto')

                if not risk_ok:
                    # [v10.3.3-F3] Motivo real preservado
                    real_reason_c = risk_reason.split()[0] if risk_reason else 'risk_blocked'
                    is_perm_c = 'KILL_SWITCH' in risk_reason or 'DRAWDOWN' in risk_reason
                    # [v10.6.4] Capturar ID real confirmado pelo banco — mesmo padrão do fix de stocks.
                    # record_signal_event pode retornar ID diferente de _sig_pre_id_c por ON DUPLICATE KEY.
                    confirmed_sig_id_c = record_signal_event(
                        sig_enriched_c, features_c, feat_hash_c, conf_c, insight_c,
                        source_type='crypto_derived',
                        existing_signal_id=_sig_pre_id_c,
                        origin_signal_key=origin_key_c)
                    record_shadow_decision(confirmed_sig_id_c, sig_enriched_c,
                                           'kill_switch' if is_perm_c else real_reason_c)
                    with learning_lock:
                        processed_signal_ids[ms_key_c] = {
                            'sig_id': confirmed_sig_id_c,
                            'reason': 'kill_switch' if is_perm_c else real_reason_c}
                    if is_perm_c: break
                    continue
                if approved_size<=0: continue

                # [V91-1] Gerar IDs ANTES do lock
                pre_trade_id = gen_id('CRY'); pre_order_id = gen_id('ORD')
                order_side   = 'BUY' if direction=='LONG' else 'SELL'
                trade = None; qty = 0
                # [v10.3.4-F1] existing_signal_id → síncrono → sig_id_c confirmado antes do attribution
                sig_id_c = record_signal_event(
                    sig_enriched_c, features_c, feat_hash_c, conf_c, insight_c,
                    source_type='crypto_derived',
                    existing_signal_id=_sig_pre_id_c,
                    origin_signal_key=origin_key_c)
                with learning_lock:
                    processed_signal_ids[ms_key_c] = {'sig_id': sig_id_c, 'reason': 'executed'}

                with state_lock:
                    # [v10.22] Pre-trade risk + kill switch check
                    ks_ok, ks_reason = kill_switch_middleware.check_before_trade('crypto', get_db)
                    if not ks_ok:
                        log.warning(f'[KILL-SWITCH] Crypto blocked: {ks_reason}')
                        continue
                    # [v10.51] Passar portfolio TOTAL (livre + alocado), não só o livre.
                    # Bug: conforme mais trades abrem, capital livre diminui → concentração
                    # aparente de uma trade nova cresce artificialmente. Ex: 13 abertas × 75k = 975k
                    # alocado; livre = 472k; trade 84k / 472k = 17.86% > 15% limite.
                    # Correto: 84k / 1.500k (portfolio total) = 5.6% (dentro do limite).
                    _total_crypto_portfolio = crypto_capital + sum(t.get('position_value', 0) for t in crypto_open)
                    _total_crypto_portfolio = max(_total_crypto_portfolio, INITIAL_CAPITAL_CRYPTO)
                    risk_ok_pre, risk_reason_pre = risk_manager.check_can_open('crypto', display, approved_size, _total_crypto_portfolio)
                    if not risk_ok_pre:
                        log.warning(f'[RISK-BLOCK] Crypto {display}: {risk_reason_pre}')
                        continue

                    # ═══ BRAIN ADVISOR V4 — Entry (crypto) ═══════════════
                    # IA propõe, motor decide. Shadow mode por padrão.
                    # REGRA INTOCÁVEL: advisor NUNCA altera TRAILING_PEAK_CRYPTO nem TRAILING_DROP_CRYPTO.
                    _adv_decision_cry = None
                    _adv_size_cry = approved_size
                    try:
                        from modules.unified_brain.advisor_entry import evaluate_entry as _adv_eval_c
                        _adv_decision_cry = _adv_eval_c(
                            get_db, log,
                            symbol=display, asset_type='crypto',
                            strategy='day_trade',
                            score_v3=score,
                            regime_v3=locals().get('regime_v2_c'),
                            direction=direction, atr_pct=atr_pct_c,
                            market_type='CRYPTO',
                            hour_of_day=datetime.utcnow().hour,
                            weekday=datetime.utcnow().weekday(),
                            portfolio_state={'open_positions': len(crypto_open),
                                             'max_positions': MAX_POSITIONS_CRYPTO},
                            feature_hash=feat_hash_c,
                            learning_confidence=conf_c.get('final_confidence'))
                        if _adv_decision_cry and not _adv_decision_cry.get('bypassed'):
                            # SEMPRE logar a decisão (shadow ou não, block ou não)
                            try:
                                from modules.unified_brain.advisor_shadow import log_entry_decision as _adv_log_c
                                _would_block_c = (not _adv_decision_cry.get('shadow')
                                                  and not _adv_decision_cry.get('approve'))
                                _adv_log_c(get_db, log,
                                          trade_id=pre_trade_id,
                                          symbol=display, asset_type='crypto', strategy='day_trade',
                                          market_type='CRYPTO', direction=direction,
                                          score_v3=score,
                                          regime_v3=locals().get('regime_v2_c'),
                                          atr_pct=atr_pct_c,
                                          hour_of_day=datetime.utcnow().hour,
                                          weekday=datetime.utcnow().weekday(),
                                          decision=_adv_decision_cry,
                                          motor_opened=(not _would_block_c),
                                          motor_size_used=None)
                            except Exception: pass
                            # Aplicar BLOCK apenas se NÃO shadow
                            if not _adv_decision_cry.get('shadow') and not _adv_decision_cry.get('approve'):
                                log.info(f"[ADVISOR-ENTRY] {display}: BLOCK {_adv_decision_cry.get('reason','')}")
                                record_shadow_decision(sig_id_c, sig_enriched_c,
                                    f"advisor_block_{_adv_decision_cry.get('reason','')}"[:60])
                                continue
                            # Aplicar size_multiplier se NÃO shadow
                            if not _adv_decision_cry.get('shadow'):
                                _sm = float(_adv_decision_cry.get('size_multiplier', 1.0))
                                _adv_size_cry = max(10.0, approved_size * _sm)
                    except Exception as _adv_e_cry:
                        log.debug(f'[ADVISOR-ENTRY] crypto {display}: {_adv_e_cry}')
                        _adv_decision_cry = None
                    # ═══ FIM BRAIN ADVISOR ════════════════════════════════

                    ok2,reason2=_second_validation(display,'CRYPTO','crypto')
                    if ok2 and crypto_capital>=_adv_size_cry:
                        approved_size = _adv_size_cry  # advisor pode ter ajustado size
                        qty=approved_size/price; crypto_capital-=approved_size
                        # [v10.18] Ledger: RESERVE
                        # [v11-hook] dual-write crypto open
                        try:
                            _v11_open_tmp_id = locals().get('new_trade_id') or locals().get('trade_id') or display + ':' + str(int(time.time()))
                            _v11_on_trade_open('crypto', _v11_open_tmp_id,
                                                float(pos),
                                                metadata={'symbol': display})
                        except Exception: pass
                        ledger_record('crypto', 'RESERVE', display,
                                      round(approved_size, 2), crypto_capital, pre_trade_id)
                        trade={
                            'id':pre_trade_id,'symbol':display,'market':'CRYPTO','asset_type':'crypto',
                            'direction':direction,'entry_price':price,'current_price':price,
                            'quantity':round(qty,6),'position_value':round(approved_size,2),
                            'pnl':0,'pnl_pct':0,'peak_pnl_pct':0,'score':score,
                            'signal':'COMPRA' if direction=='LONG' else 'VENDA',
                            'order_id':pre_order_id,
                            'opened_at':datetime.utcnow().isoformat(),'status':'OPEN',
                            'signal_id':           sig_id_c,
                            'feature_hash':        feat_hash_c,
                            'learning_confidence': conf_c.get('final_confidence'),
                            'insight_summary':     insight_c,
                            'learning_version':    LEARNING_VERSION,
                            '_features':           features_c,
                            # [v10.16] Score snapshot + ATR para SL adaptativo
                            '_score_snapshot':     make_score_snapshot(sig_enriched_c, features_c, conf_c),
                            '_atr_pct':            atr_pct_c,
                            # [v10.47] Score v3 fields — sempre v3
                            'score_v2':            score,
                            'regime_v2':           locals().get('regime_v2_c'),
                            'signal_v2':           locals().get('signal_v2_c'),
                        }
                        crypto_open.append(trade)
                    else:
                        # [v10.6.2-Fix1] 2ª validação falhou — sobrescrever 'executed' com o motivo real.
                        # Sem isso, o sinal fica marcado como 'executed' no dedup mesmo sem abrir trade,
                        # impedindo reavaliação futura. Padrão simétrico ao bloco stocks (linhas 3285-3290).
                        _c_block2 = reason2 if not ok2 else 'capital'
                        log.info(f'Crypto Risk-2 {display}: {_c_block2}')
                        record_shadow_decision(sig_id_c, sig_enriched_c, _c_block2)
                        is_perm_c = 'DUPLICATE' in (_c_block2 or '').upper()
                        with learning_lock:
                            processed_signal_ids[ms_key_c] = {
                                'sig_id': sig_id_c,
                                'reason': 'symbol_duplicate' if is_perm_c else _c_block2
                            }

                if trade is None: continue

                # [FIX-2] Vincular trade_id e order_id ao signal_event imediatamente
                update_signal_attribution(sig_id_c, pre_trade_id, pre_order_id)

                # [V91-1] Fora do lock: criar ordem com ID pré-definido
                order=create_order(pre_trade_id,display,order_side,'MARKET',round(qty,6),price,'crypto',
                                   order_id_override=pre_order_id)
                update_order_status(order,'VALIDATED')
                update_order_status(order,'SENT')
                update_order_status(order,'FILLED',price,round(qty,6))

                _last_trade_opened['crypto'] = time.time()  # [v10.16] inactivity tracking
                audit('TRADE_OPENED',{'id':pre_trade_id,'symbol':display,'direction':direction,'score':score})
                enqueue_persist('trade',trade)
        except Exception as e:
            import traceback
            log.error(f'auto_trade_crypto: {e}\n{traceback.format_exc()}')

# ═══════════════════════════════════════════════════════════════
# ARBI ENGINE
# ═══════════════════════════════════════════════════════════════
# [v10.14] Thresholds dinâmicos por par — baseados em auditoria histórica
# Substitui o ARBI_MIN_SPREAD global para pares com comportamento específico
# Aprendido automaticamente via _arbi_pair_learning()
ARBI_PAIR_CONFIG = {
    # PETR4-PBR: spread estrutural crônico -9% a -11%
    # Auditoria 24 trades: zona ≥10%=WR80%, zona <9%=WR30%
    # Só entra com spread ≥ 10% para capturar reversão à média
    'PETR4-PBR': {
        'min_spread':   10.0,   # threshold de entrada (dinâmico — aprende)
        'tp_spread':     0.8,   # TP: 0.8% de convergência desde entrada
        'sl_pct':        1.2,   # SL: 1.2% de divergência (mais largo que global)
        'max_spread':   13.0,   # spread acima disso = dado errado, ignorar
        # max_pos removido — usa posição dinâmica igual aos outros pares (~portfolio/3)
        'last_10_wr':   80.0,
        'learn_window':  10,
        'note': 'spread estrutural -9% a -11%, só operar reversão extrema',
    },
    # TIMS3-TIMB: ratio 5:1, poucas trades — usar padrão conservador
    'TIMS3-TIMB': {
        'min_spread':    1.5,
        'tp_spread':     0.3,
        'sl_pct':        1.0,
        'max_spread':   10.0,
        'last_10_wr':  100.0,
        'learn_window':  10,
        'note': 'ratio 5:1, spread pequeno, operar conservador',
    },
    # Default para todos os outros pares — usar parâmetros globais
    '_default': {
        'min_spread': None,   # usa ARBI_MIN_SPREAD global
        'tp_spread':  None,   # usa ARBI_TP_SPREAD global
        'sl_pct':     None,   # usa ARBI_SL_PCT global
        'max_spread': 15.0,
        'last_10_wr': None,
        'learn_window': 10,
        'note': 'parâmetros globais',
    },
}


# ═══════════════════════════════════════════════════════════════
# [v10.14] ARBI PATTERN LEARNING — módulo autônomo de aprendizado
# Analisa arbi_trades e ajusta parâmetros por par automaticamente
# ═══════════════════════════════════════════════════════════════

_arbi_learning_cache = {}  # pair_id → {zone → {n, wins, pnl}}
_arbi_learning_lock  = threading.Lock()

def _spread_zone(abs_spread: float, pair_id: str = '') -> str:
    """Classifica o spread em zona para análise de padrão."""
    if abs_spread >= 12.0:  return 'EXTREME'   # muito alto — possível erro
    if abs_spread >= 10.0:  return 'HIGH'       # zona de reversão
    if abs_spread >=  9.0:  return 'MID'        # zona incerta
    if abs_spread >=  7.0:  return 'LOW'        # zona desfavorável
    return 'MINIMAL'                            # spread pequeno — não opera

def run_arbi_pattern_learning():
    """[v10.14] Aprende padrões de spread por par a partir das trades históricas.
    
    Analisa a tabela arbi_trades e descobre:
    - Qual zona de spread tem melhor WR para cada par
    - Ajusta ARBI_PAIR_CONFIG dinamicamente
    - Loga descobertas para auditoria
    
    Dimensões analisadas:
    pair_id × spread_zone × weekday × hour_utc
    """
    conn = get_db()
    if not conn: return
    cursor=None
    try:
        cursor = conn.cursor(dictionary=True)
        # Buscar todas as trades de arbi fechadas
        # [v10.14-FIX] Combinar memória (mais atualizado) + DB (histórico completo)
        cursor.execute("""
            SELECT pair_id, name, entry_spread, current_spread, pnl, pnl_pct,
                   status, close_reason, opened_at, closed_at, direction,
                   position_size
            FROM arbi_trades
            WHERE status='CLOSED' AND pnl IS NOT NULL
            ORDER BY closed_at DESC
            LIMIT 500
        """)
        db_rows = cursor.fetchall()
        cursor.close(); conn.close()

        # Adicionar trades da memória que podem não estar no DB ainda
        mem_rows = []
        with state_lock:
            for t in arbi_closed:
                mem_rows.append({
                    'pair_id':      t.get('pair_id','?'),
                    'name':         t.get('name','?'),
                    'entry_spread': t.get('entry_spread', 0),
                    'current_spread': t.get('current_spread', 0),
                    'pnl':          t.get('pnl', 0),
                    'pnl_pct':      t.get('pnl_pct', 0),
                    'status':       'CLOSED',
                    'close_reason': t.get('close_reason', ''),
                    'opened_at':    t.get('opened_at', ''),
                    'closed_at':    t.get('closed_at', ''),
                    'direction':    t.get('direction', ''),
                    'position_size': t.get('position_size', 0),
                })

        # Deduplicar por pair_id + opened_at (memória tem prioridade)
        db_ids = {(r.get('pair_id',''), str(r.get('opened_at',''))[:16]) for r in mem_rows}
        db_only = [r for r in db_rows if (r.get('pair_id',''), str(r.get('opened_at',''))[:16]) not in db_ids]
        rows = mem_rows + db_only

        if not rows:
            return

        # Agrupar por par e zona de spread
        from collections import defaultdict
        by_pair_zone = defaultdict(lambda: defaultdict(lambda: {
            'n':0,'wins':0,'pnl':0.0,'avg_duration_h':0.0,'stops':0
        }))
        by_pair_weekday = defaultdict(lambda: defaultdict(lambda: {'n':0,'wins':0,'pnl':0.0}))

        for row in rows:
            pair   = row.get('pair_id','?')
            spread = abs(float(row.get('entry_spread') or 0))
            pnl    = float(row.get('pnl') or 0)
            zone   = _spread_zone(spread, pair)
            win    = 1 if pnl > 0 else 0
            reason = row.get('close_reason','')

            # Por zona
            z = by_pair_zone[pair][zone]
            z['n']    += 1
            z['wins'] += win
            z['pnl']  += pnl
            if reason == 'STOP_LOSS': z['stops'] += 1

            # Por dia da semana
            try:
                dt = datetime.fromisoformat(str(row['opened_at']).replace('Z',''))
                wd = dt.strftime('%A')  # Monday, Tuesday...
                d = by_pair_weekday[pair][wd]
                d['n'] += 1; d['wins'] += win; d['pnl'] += pnl
            except: pass

        # Atualizar cache e ARBI_PAIR_CONFIG
        discoveries = []
        with _arbi_learning_lock:
            _arbi_learning_cache.clear()
            for pair, zones in by_pair_zone.items():
                _arbi_learning_cache[pair] = {}
                best_zone = None; best_wr = 0; best_pnl = -999999

                for zone, stats in zones.items():
                    if stats['n'] == 0: continue
                    wr  = stats['wins'] / stats['n'] * 100
                    avg = stats['pnl']  / stats['n']
                    stop_rate = stats['stops'] / stats['n'] * 100
                    _arbi_learning_cache[pair][zone] = {
                        'n': stats['n'], 'wr': round(wr,1),
                        'avg_pnl': round(avg,0), 'stop_rate': round(stop_rate,1)
                    }
                    # Zona com WR ≥ 60% e avg positivo é candidata a best
                    if wr >= 60 and avg > best_pnl and stats['n'] >= 3:
                        best_wr   = wr
                        best_pnl  = avg
                        best_zone = zone

                # Ajustar ARBI_PAIR_CONFIG baseado na melhor zona encontrada
                zone_to_threshold = {
                    'EXTREME': 12.0, 'HIGH': 10.0, 'MID': 9.0,
                    'LOW': 7.0, 'MINIMAL': ARBI_MIN_SPREAD
                }
                if best_zone:
                    new_min = zone_to_threshold.get(best_zone, ARBI_MIN_SPREAD)
                    if pair in ARBI_PAIR_CONFIG:
                        old_min = ARBI_PAIR_CONFIG[pair].get('min_spread', ARBI_MIN_SPREAD)
                        if abs(new_min - old_min) >= 0.5:  # só atualiza se mudança significativa
                            ARBI_PAIR_CONFIG[pair]['min_spread'] = new_min
                            discoveries.append(
                                f"{pair}: min_spread {old_min:.1f}%→{new_min:.1f}% "
                                f"(best_zone={best_zone}, WR={best_wr:.0f}%, avg=${best_pnl:.0f})"
                            )
                    else:
                        # Par novo — criar config dinâmico
                        ARBI_PAIR_CONFIG[pair] = {
                            'min_spread':   new_min,
                            'tp_spread':    ARBI_TP_SPREAD,
                            'sl_pct':       ARBI_SL_PCT,
                            'max_spread':   15.0,
                            'last_10_wr':   best_wr,
                            'learn_window': 10,
                            'note':         f'auto-learned: best_zone={best_zone}',
                        }
                        discoveries.append(
                            f"{pair}: NOVO config — min_spread={new_min:.1f}% "
                            f"(best_zone={best_zone}, WR={best_wr:.0f}%)"
                        )

        # Log das descobertas
        log.info(f'[ArbiLearning] Analisados {len(rows)} trades de {len(by_pair_zone)} pares')
        for d in discoveries:
            log.info(f'[ArbiLearning] AJUSTE: {d}')

        # Log do estado atual por par
        for pair, zones in _arbi_learning_cache.items():
            summary = ' | '.join(
                f"{z}: WR={s['wr']:.0f}% n={s['n']} avg=${s['avg_pnl']:.0f}"
                for z, s in sorted(zones.items())
                if s['n'] >= 2
            )
            if summary:
                log.info(f'[ArbiLearning] {pair}: {summary}')

    except Exception as e:
        log.error(f'run_arbi_pattern_learning: {e}')
    finally:
        try:
            if cursor: cursor.close()
            if conn: conn.close()
        except: pass

def arbi_learning_loop():
    """[v10.14] Loop de aprendizado de arbi — roda a cada 5 minutos.

    [v10.52] Startup sleep 120s e gap de 5min entre passes eram maiores
    que o heartbeat timeout (120s default) — watchdog considerava
    frozen. Agora sleep e quebrado em chunks de 30s com beat em cada,
    mantendo vigilancia continua.
    """
    # Startup delay em chunks com beat
    for _ in range(4):  # 4 x 30s = 120s total
        beat('arbi_learning_loop')
        time.sleep(30)
    beat('arbi_learning_loop')
    try:
        run_arbi_pattern_learning()
    except Exception as e:
        log.error(f'arbi_learning_loop initial: {e}')

    while True:
        # Sleep 5min em chunks de 30s com beat — watchdog vigilante
        for _ in range(10):  # 10 x 30s = 300s = 5min
            beat('arbi_learning_loop')
            time.sleep(30)
        beat('arbi_learning_loop')
        try:
            run_arbi_pattern_learning()
        except Exception as e:
            log.error(f'arbi_learning_loop: {e}')


def _arbi_pair_learning(pair_id, recent_trades):
    """[v10.14] Aprende o threshold ideal para cada par baseado nos últimos trades.
    Ajusta min_spread dinamicamente: se WR cai, sobe o threshold de entrada.
    """
    cfg = ARBI_PAIR_CONFIG.get(pair_id, ARBI_PAIR_CONFIG['_default'])
    if not recent_trades or len(recent_trades) < 3:
        return cfg
    
    # Agrupar por zona de spread de entrada
    zones = {'high': [], 'mid': [], 'low': []}
    for t in recent_trades[-cfg.get('learn_window', 10):]:
        abs_spread = abs(float(t.get('entry_spread', 0)))
        pnl = float(t.get('pnl', 0))
        if abs_spread >= 10.0:   zones['high'].append(pnl)
        elif abs_spread >= 9.0:  zones['mid'].append(pnl)
        else:                    zones['low'].append(pnl)
    
    # Calcular WR por zona
    def wr(lst): return sum(1 for p in lst if p > 0) / len(lst) * 100 if lst else 0
    wr_high = wr(zones['high'])
    wr_mid  = wr(zones['mid'])
    wr_low  = wr(zones['low'])
    
    # Ajustar threshold: usar a menor zona com WR ≥ 60%
    new_min = cfg.get('min_spread', ARBI_MIN_SPREAD)
    if wr_high >= 60 and len(zones['high']) >= 3:
        new_min = 10.0  # zona alta funciona
    if wr_mid  >= 60 and len(zones['mid'])  >= 3:
        new_min = 9.0   # zona média também funciona → relaxar
    if wr_low  < 40 and len(zones['low'])   >= 3:
        new_min = max(new_min, 9.5)  # zona baixa ruim → endurecer
    
    # Atualizar config em memória
    if pair_id in ARBI_PAIR_CONFIG:
        ARBI_PAIR_CONFIG[pair_id]['min_spread'] = new_min
        ARBI_PAIR_CONFIG[pair_id]['last_10_wr'] = wr_high
        log.info(f'[ARBI-LEARN] {pair_id}: min_spread={new_min:.1f}% WR_high={wr_high:.0f}% WR_mid={wr_mid:.0f}% WR_low={wr_low:.0f}%')
    
    return ARBI_PAIR_CONFIG.get(pair_id, cfg)

ARBI_PAIRS = [
    # PETR4-PBR REATIVADO — alta liquidez, importante para mercado real
    # Proteções: min_spread 10% (zona HIGH WR67%), max_pos $30K fixo, sanity spread >20%
    # Bug anterior (-$896K) foi por posição $1M + preço PBR=0 → corrigido
    {'id':'PETR4-PBR', 'leg_a':'PETR4.SA','leg_b':'PBR', 'mkt_a':'B3','mkt_b':'NYSE',
     'fx':'USDBRL','name':'Petrobras','ratio_a':2,'ratio_b':1},
    {'id':'ITUB4-ITUB',  'leg_a':'ITUB4.SA', 'leg_b':'ITUB',   'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Itaú',        'ratio_a':1,'ratio_b':1},
    {'id':'BBDC4-BBD',   'leg_a':'BBDC4.SA', 'leg_b':'BBD',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Bradesco',    'ratio_a':1,'ratio_b':1},
    {'id':'ABEV3-ABEV',  'leg_a':'ABEV3.SA', 'leg_b':'ABEV',   'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Ambev',       'ratio_a':1,'ratio_b':1},
    # Embraer (EMBR3/ERJ) removida — ERJ sem cobertura de preço disponível
    {'id':'GGBR4-GGB',   'leg_a':'GGBR4.SA', 'leg_b':'GGB',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Gerdau',      'ratio_a':1,'ratio_b':1},
    {'id':'CSNA3-SID',   'leg_a':'CSNA3.SA', 'leg_b':'SID',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'CSN',         'ratio_a':1,'ratio_b':1},
    {'id':'CMIG4-CIG',   'leg_a':'CMIG4.SA', 'leg_b':'CIG',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Cemig',       'ratio_a':1,'ratio_b':1},
    # Copel (CPLE6/ELP) removida — ELP ADR sem cobertura de preço disponível
    {'id':'BP-BP.L',     'leg_a':'BP',       'leg_b':'BP.L',   'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'BP',          'ratio_a':1,'ratio_b':6},
    {'id':'SHEL-SHEL.L', 'leg_a':'SHEL',     'leg_b':'SHEL.L', 'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'Shell',       'ratio_a':1,'ratio_b':2},
    {'id':'AZN-AZN.L',   'leg_a':'AZN',      'leg_b':'AZN.L',  'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'AstraZeneca', 'ratio_a':1,'ratio_b':1},
    {'id':'GSK-GSK.L',   'leg_a':'GSK',      'leg_b':'GSK.L',  'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'GSK',         'ratio_a':1,'ratio_b':2},
    {'id':'HSBC-HSBA.L', 'leg_a':'HSBC',     'leg_b':'HSBA.L', 'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'HSBC',        'ratio_a':1,'ratio_b':5},
    # [v10.9] HKEX pares removidos — NYSE e HKEX não têm sobreposição de horário (gap de 6h)
    # HKEX fecha 08:00 UTC, NYSE abre 14:30 UTC → jamais executariam. 0 trades em todo o histórico.
    # Removidos: Tencent, Alibaba, HSBC HK, China Mobile, Ping An

    # ── B3/NYSE novos ─────────────────────────────────────────────────────────
    # Ratios confirmados via preços reais: spread ≈ 0% quando mercados eficientes
    # pa = (preco_BRL / USDBRL) × ratio_a  |  pb = preco_USD × ratio_b
    {'id':'SUZB3-SUZ',  'leg_a':'SUZB3.SA','leg_b':'SUZ',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Suzano',      'ratio_a':1,'ratio_b':1},
    {'id':'SBSP3-SBS',  'leg_a':'SBSP3.SA','leg_b':'SBS',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Sabesp',      'ratio_a':1,'ratio_b':1},
    {'id':'UGPA3-UGP',  'leg_a':'UGPA3.SA','leg_b':'UGP',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Ultrapar',    'ratio_a':1,'ratio_b':1},

    # ── NYSE/TSX (Canadá) — REMOVIDOS: mercado eficiente demais ─────────────
    # Spreads históricos: 0.00-0.09% — nunca atingem o limiar de 2.0% para abrir.
    # 7 pares (RBC, TD, Shopify, Suncor, CNQ, ENB, BNS) removidos em 20/03/2026.

    # ── NYSE/LSE adicionais — overlap 2h (14:30-16:30 UTC) ───────────────────
    # pa = preco_USD × ratio_a  |  pb = (preco_GBp / 100) × GBPUSD × ratio_b
    # ATENÇÃO: LSE cotações em GBp (pence), não £ — divisão por 100 obrigatória
    # Rio Tinto: 1 ADR NYSE = 1 ação LSE (ratio confirmado 0.996 ≈ 1:1)
    # Diageo: 1 ADR NYSE = 4 ações LSE (ratio confirmado 3.973 ≈ 4:1)
    {'id':'RIO-RIO.L',  'leg_a':'RIO',     'leg_b':'RIO.L',  'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'Rio Tinto',  'ratio_a':1,'ratio_b':1},
    {'id':'UL-ULVR.L',  'leg_a':'UL',      'leg_b':'ULVR.L', 'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'Unilever',   'ratio_a':1,'ratio_b':1},
    {'id':'DEO-DGE.L',  'leg_a':'DEO',     'leg_b':'DGE.L',  'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'Diageo',     'ratio_a':1,'ratio_b':4},
    {'id':'BTI-BATS.L', 'leg_a':'BTI',     'leg_b':'BATS.L', 'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'BAT',        'ratio_a':1,'ratio_b':1},

    # ── NYSE/EURONEXT — overlap 2h (14:30-16:30 UTC) ─────────────────────────
    # pa = preco_USD × ratio_a  |  pb = preco_EUR × EURUSD × ratio_b
    # Ratios confirmados ≈ 1:1. Spread estrutural de ~5-7% é REAL (custo ADR + bid-ask)
    {'id':'ASML-ASML.AS','leg_a':'ASML',   'leg_b':'ASML.AS','mkt_a':'NYSE','mkt_b':'EURONEXT','fx':'EURUSD','name':'ASML',       'ratio_a':1,'ratio_b':1},
    {'id':'TTE-TTE.PA',  'leg_a':'TTE',    'leg_b':'TTE.PA', 'mkt_a':'NYSE','mkt_b':'EURONEXT','fx':'EURUSD','name':'TotalEnergies','ratio_a':1,'ratio_b':1},
    {'id':'SAP-SAP.DE',  'leg_a':'SAP',    'leg_b':'SAP.DE', 'mkt_a':'NYSE','mkt_b':'XETRA',  'fx':'EURUSD','name':'SAP',         'ratio_a':1,'ratio_b':1},
    # LVMH: 1 ADR LVMUY (NYSE) = 0.2 ação MC.PA → ratio_a=5 para paridade com 1 ação LVMH
    # Verificado: LVMUY=$105 × 5 = $525 vs MC.PA=€458 × 1.1555 = $529 → spread -0.71% ✅
    {'id':'LVMUY-MC.PA', 'leg_a':'LVMUY',  'leg_b':'MC.PA',  'mkt_a':'NYSE','mkt_b':'EURONEXT','fx':'EURUSD','name':'LVMH',        'ratio_a':5,'ratio_b':1},



    # ── B3/NYSE adicionais ────────────────────────────────────────────────────
    # TIMS3/TIMB: ratio 5:1 verificado — 1 ADR TIMB = 5 ações TIMS3
    # Verificado: TIMS3=R$26.28 ÷ 5.2552 × 5 = $25.00 vs TIMB=$24.81 → spread +0.78% ✅
    {'id':'TIMS3-TIMB',  'leg_a':'TIMS3.SA','leg_b':'TIMB',  'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'TIM Brasil',     'ratio_a':5,'ratio_b':1},
    # BRF (BRFS3/BRFS) removida — ticker sem cobertura de preço disponível
]

def _fetch_arbi_price(symbol: str) -> float:
    """[v10.4][v10.6-P4] Preço para arbitragem com ADR fallback para legs B3.
    Cadência: Binance (crypto) → Polygon (US + ADR de B3) → brapi (B3) → FMP → Yahoo.
    """
    display = symbol.replace('.SA', '')
    is_b3_sym = symbol.endswith('.SA') or display in {s.replace('.SA','') for s in STOCK_SYMBOLS_B3}

    # Binance para crypto
    if symbol.endswith('USDT') or symbol in CRYPTO_SYMBOLS:
        try:
            r = requests.get('https://api.binance.com/api/v3/ticker/price',
                             params={'symbol': symbol}, timeout=5)
            if r.status_code == 200:
                p = float(r.json().get('price', 0))
                if p > 0: return p
        except: pass

    # [v10.31] Cedro socket primário para B3 (real-time streaming)
    if is_b3_sym and _cedro_socket and _cedro_socket.enabled:
        p = _cedro_socket.get_price(display, wait_ms=800)
        if p and p > 0:
            return float(p)

    # brapi primário para B3 (snapshot, usa cache)
    if is_b3_sym and BRAPI_TOKEN:
        result, _ = _fetch_brapi_stock(display)
        if result and result.get('price', 0) > 0:
            return result['price']

    # [v10.6-P4] Para B3 sem brapi: tentar ADR via Polygon com conversão USD→BRL
    if is_b3_sym and POLYGON_API_KEY:
        adr_sym = B3_TO_ADR.get(display)
        if adr_sym:
            result, _ = _fetch_polygon_stock(adr_sym)
            if result and result.get('price', 0) > 0:
                usd_brl = fx_rates.get('USDBRL', 5.8)
                return round(result['price'] * usd_brl, 2)

    # Polygon para equity US direto
    if not is_b3_sym and POLYGON_API_KEY:
        result, _ = _fetch_polygon_stock(display)
        if result and result.get('price', 0) > 0:
            return result['price']

    # FMP fallback universal
    if FMP_API_KEY:
        try:
            r = requests.get(
                f'https://financialmodelingprep.com/api/v3/quote/{display}',
                params={'apikey': FMP_API_KEY}, timeout=6)
            if r.status_code == 200:
                d = r.json()
                if d and isinstance(d, list):
                    p = float(d[0].get('price') or 0)
                    if p > 0: return p
        except: pass

    # Yahoo último recurso
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d',
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=6)
        if r.status_code == 200:
            return r.json()['chart']['result'][0]['meta'].get('regularMarketPrice', 0)
    except: pass
    return 0


# ═══════════════════════════════════════════════════════════════
# [v10.14] ARBI SMART LEARNING — Aprendizado de thresholds por par
# Analisa trades fechadas e ajusta os parâmetros dinamicamente
# ═══════════════════════════════════════════════════════════════

_arbi_pair_stats = {}  # {pair_id: {wins_low: int, n_low: int, wins_high: int, n_high: int, ...}}
_arbi_learning_lock = threading.Lock()
ARBI_LEARN_MIN_SAMPLES = 5  # mínimo de amostras para ajustar thresholds

def arbi_learn_from_closed(pair_id, entry_spread, pnl, direction):
    """[v10.14] Registra resultado de trade fechada para aprendizado de thresholds."""
    global _arbi_pair_stats
    with _arbi_learning_lock:
        if pair_id not in _arbi_pair_stats:
            _arbi_pair_stats[pair_id] = {
                'n': 0, 'pnl': 0.0,
                'spread_buckets': {},  # {bucket: {n, wins, pnl}}
                'low_threshold': None, 'high_threshold': None,
                'no_entry_low': None,  'no_entry_high': None,
                'last_updated': None,
            }
        st = _arbi_pair_stats[pair_id]
        st['n']   += 1
        st['pnl'] += pnl

        # Classificar em buckets de 0.5%
        abs_s = abs(entry_spread)
        bucket = round(abs_s * 2) / 2  # arredonda para 0.5%
        if bucket not in st['spread_buckets']:
            st['spread_buckets'][bucket] = {'n': 0, 'wins': 0, 'pnl': 0.0}
        bk = st['spread_buckets'][bucket]
        bk['n']   += 1
        bk['pnl'] += pnl
        if pnl > 0: bk['wins'] += 1

        # Recalcular thresholds se tiver amostras suficientes
        _recalc_arbi_thresholds(pair_id, st)

def _recalc_arbi_thresholds(pair_id, st):
    """[v10.14] Recalcula thresholds ótimos baseado no histórico de cada bucket."""
    buckets = st['spread_buckets']
    if not buckets: return

    # Para cada bucket com >= MIN_SAMPLES, calcular WR e P&L/trade
    bucket_stats = []
    for b, v in sorted(buckets.items()):
        if v['n'] >= ARBI_LEARN_MIN_SAMPLES:
            wr   = v['wins'] / v['n'] * 100
            avg  = v['pnl']  / v['n']
            bucket_stats.append((b, v['n'], wr, avg))

    if len(bucket_stats) < 3: return  # precisa de mais dados

    # Estratégia de aprendizado:
    # Buckets onde LONG_A tem WR < 40% → candidatos para LONG_B (spread baixo aumenta)
    # Buckets onde LONG_A tem WR > 60% → manter LONG_A (spread alto diminui)
    # Zona de transição = no-entry zone

    # Encontrar threshold de forma automática
    bad_buckets  = [b for b, n, wr, avg in bucket_stats if wr < 45]
    good_buckets = [b for b, n, wr, avg in bucket_stats if wr > 60]

    if bad_buckets and good_buckets:
        new_low  = max(bad_buckets)   # limite superior dos buckets ruins (usar LONG_B abaixo disto)
        new_high = min(good_buckets)  # limite inferior dos buckets bons (usar LONG_A acima disto)
        no_low   = new_low
        no_high  = new_high

        # Só atualizar se mudou significativamente (>0.5%)
        if (st['low_threshold'] is None or
            abs(st['low_threshold'] - new_low) >= 0.5 or
            abs(st['high_threshold'] - new_high) >= 0.5):

            old_low  = st['low_threshold']
            old_high = st['high_threshold']
            st['low_threshold']  = new_low
            st['high_threshold'] = new_high
            st['no_entry_low']   = no_low
            st['no_entry_high']  = no_high
            st['last_updated']   = datetime.utcnow().isoformat()

            if old_low is not None:
                log.info(f'[ArbiLearn] {pair_id}: threshold atualizado '
                         f'low {old_low}→{new_low}% high {old_high}→{new_high}% '
                         f'(buckets: {len(bucket_stats)})')
            else:
                log.info(f'[ArbiLearn] {pair_id}: threshold inicial detectado '
                         f'low={new_low}% high={new_high}%')

def arbi_get_smart_direction(pair, abs_spread):
    """[v10.14] Retorna direção correta para par com smart_direction=True."""
    pair_id = pair.get('id', '')

    # Verificar se temos thresholds aprendidos para este par
    with _arbi_learning_lock:
        learned = _arbi_pair_stats.get(pair_id, {})

    # Usar thresholds aprendidos se disponíveis, senão usar config do par
    low_thr  = learned.get('low_threshold')  or pair.get('spread_low_threshold', 9.0)
    high_thr = learned.get('high_threshold') or pair.get('spread_high_threshold', 10.0)
    no_low   = learned.get('no_entry_low')   or pair.get('no_entry_zone', (9.0, 10.0))[0]
    no_high  = learned.get('no_entry_high')  or pair.get('no_entry_zone', (9.0, 10.0))[1]

    # Zona ambígua → não entrar
    if no_low <= abs_spread <= no_high:
        return None, 'no_entry_zone'

    if abs_spread < low_thr:
        return 'LONG_B', f'smart_low<{low_thr:.1f}%'
    elif abs_spread > high_thr:
        return 'LONG_A', f'smart_high>{high_thr:.1f}%'
    else:
        return None, 'no_entry_zone'


def calc_spread(pair):
    try:
        pa_raw=_fetch_arbi_price(pair['leg_a']); pb_raw=_fetch_arbi_price(pair['leg_b'])
        if pa_raw<=0 or pb_raw<=0: return None
        fx=pair['fx']; ra=pair.get('ratio_a',1); rb=pair.get('ratio_b',1)
        if fx=='USDBRL':
            # [v10.14-Audit] Fórmula unificada correta:
            # pa_norm = ação local em USD (sem ratio)
            # pb_norm = ADR × ratio_b (quantidade de ações locais por ADR)
            # spread  = pa_norm / pb_norm - 1
            rate=fx_rates.get('USDBRL',5.8)
            pa = pa_raw/rate            # local (BRL→USD), sem ratio
            pb = pb_raw * rb            # ADR × ratio_b (normaliza para mesmo denominador)
            # Normalizar ambos para preço por ação local
            pa = pa / 1                 # já é por 1 ação local
            pb = pb / ra                # pb / ratio_a = preço por 1 ação local via ADR
        elif fx=='GBPUSD':
            # [v10.14-Audit] pa_norm = NYSE em USD (sem ratio, pois ratio_a=1)
            # pb_norm = LSE em USD × ratio_b (ações LSE por 1 NYSE) / ratio_a
            rate=fx_rates.get('GBPUSD',1.27)
            pa = pa_raw                     # NYSE já em USD
            pb = (pb_raw/100*rate)*rb/ra    # LSE: GBp→£→USD, normalizado pelo ratio
        elif fx=='HKDUSD':
            rate=fx_rates.get('HKDUSD',7.8)
            pa = pa_raw; pb = (pb_raw/rate)*rb/ra
        elif fx=='CADUSD':
            rate=fx_rates.get('CADUSD',0.735)
            pa = pa_raw; pb = (pb_raw*rate)*rb/ra
        elif fx=='EURUSD':
            # [v10.14-Audit] pa_norm = NYSE em USD, pb_norm = EUR→USD × ratio_b / ratio_a
            rate=fx_rates.get('EURUSD',1.085)
            pa = pa_raw                     # NYSE já em USD
            pb = (pb_raw*rate)*rb/ra        # EUR→USD, normalizado pelo ratio
        else:
            pa=pa_raw*ra; pb=pb_raw*rb
        if pb<=0: return None
        # [v10.14-Audit] Spread normalizado por ratio — corrige exibição de ADR
        # spread_norm = (price_a_normalized / price_b_normalized - 1) × 100
        # price_a_norm = pa_raw_usd / ratio_a → preço por unidade econômica
        # price_b_norm = pb_raw_usd / ratio_b → preço por unidade econômica
        # [v10.14-Audit] pa e pb já estão normalizados pela nova fórmula acima
        # pa = preço da leg_a por unidade econômica (1 ação local ou equivalente)
        # pb = preço da leg_b normalizado ao equivalente da leg_a
        pa_norm = pa; pb_norm = pb
        spread_pct = ((pa_norm - pb_norm) / pb_norm) * 100 if pb_norm > 0 else 0
        # [v10.14-SANITY] Rejeitar spreads impossíveis — dado corrompido (99% é impossível)
        if abs(spread_pct) > 30.0:
            log.warning(f'[ARBI-SANITY] {pair["id"]}: spread={spread_pct:.2f}% IMPOSSÍVEL (pa={pa:.4f} pb={pb:.4f}) — ignorado')
            return None

        # [v10.14-SMART] Direção adaptativa por faixa de spread
        # Para pares com smart_direction=True, a direção depende do nível do spread
        abs_sp = abs(spread_pct)
        smart_dir = pair.get('smart_direction', False)
        low_thr   = pair.get('spread_low_threshold', 0)
        high_thr  = pair.get('spread_high_threshold', 999)
        no_entry  = pair.get('no_entry_zone', None)

        if smart_dir and no_entry and no_entry[0] <= abs_sp <= no_entry[1]:
            # Zona ambígua — não gerar sinal
            return None  # calc_spread retorna None = sem oportunidade

        if smart_dir:
            forced_direction, _reason = arbi_get_smart_direction(pair, abs_sp)
        else:
            forced_direction = None

        now_ts = datetime.utcnow().isoformat()
        # Timestamp do preço (simplificado — usar updated_at do cache)
        price_ts_a = now_ts; price_ts_b = now_ts

        # Bid/Ask estimado — simulação conservadora: spread bid/ask típico por mercado
        # B3: ~0.05% | NYSE: ~0.02% | LSE/EURONEXT: ~0.03%
        def _ba_spread(mkt):
            return {'B3':0.0005,'NYSE':0.0002,'LSE':0.0003,'EURONEXT':0.0003}.get(mkt,0.0003)
        bid_a = round(pa_raw * (1 - _ba_spread(pair['mkt_a'])/2), 4)
        ask_a = round(pa_raw * (1 + _ba_spread(pair['mkt_a'])/2), 4)
        bid_b = round(pb_raw * (1 - _ba_spread(pair['mkt_b'])/2), 4)
        ask_b = round(pb_raw * (1 + _ba_spread(pair['mkt_b'])/2), 4)
        spread_bps_a = round(_ba_spread(pair['mkt_a']) * 10000, 1)
        spread_bps_b = round(_ba_spread(pair['mkt_b']) * 10000, 1)

        # Quantidade estimada (position_size / entry_price em USD)
        _pos = ARBI_POS_SIZE
        qty_a = round(_pos / pa, 0) if pa > 0 else 0
        qty_b = round(_pos / pb, 0) if pb > 0 else 0

        return {'pair_id':pair['id'],'name':pair['name'],'leg_a':pair['leg_a'],'leg_b':pair['leg_b'],
            'mkt_a':pair['mkt_a'],'mkt_b':pair['mkt_b'],
            # Preços raw e normalizados
            'price_a':round(pa_raw,4),'price_b':round(pb_raw,4),
            'price_a_usd':round(pa,4),'price_b_usd':round(pb,4),   # já normalizado por ratio
            'price_a_raw_usd':round(pa_raw if fx=='USDBRL' else pa_raw, 4),
            'price_b_raw_usd':round(pb_raw, 4),
            # Bid/Ask simulado
            'bid_a':bid_a,'ask_a':ask_a,'bid_b':bid_b,'ask_b':ask_b,
            'spread_bps_a':spread_bps_a,'spread_bps_b':spread_bps_b,
            'price_source_a':'last','price_source_b':'last',
            # Timestamps
            'signal_ts_a':price_ts_a,'signal_ts_b':price_ts_b,'delta_ts_ms':0,
            # Spread calculado corretamente
            'spread_pct':round(spread_pct,4),
            'spread_pct_display':round(spread_pct,2),
            'abs_spread':round(abs(spread_pct),2),
            'entry_spread_normalized':round(spread_pct,4),
            # Ratio e FX
            'fx_rate':fx_rates.get(fx,0),'fx_pair':fx,'fx_ts':now_ts,
            'ratio_a':ra,'ratio_b':rb,'ratio_source':'pair_config',
            # Quantidade
            'qty_a_est':int(qty_a),'qty_b_est':int(qty_b),
            # Flags
            'opportunity':(ARBI_MIN_SPREAD<=abs(spread_pct)<=ARBI_MAX_SPREAD) and
                           (not smart_dir or forced_direction is not None),
            'direction': forced_direction if forced_direction else ('LONG_A' if spread_pct<0 else 'LONG_B'),
            'smart_direction': smart_dir,
            'spread_zone': 'LOW' if (smart_dir and abs_sp < low_thr) else ('HIGH' if (smart_dir and abs_sp > high_thr) else 'MID'),
            'markets_open':market_open_for(pair['mkt_a']) and market_open_for(pair['mkt_b']),
            'updated_at':now_ts}
    except Exception as e: log.error(f'Spread {pair["id"]}: {e}'); return None

def arbi_scan_loop():
    global arbi_capital
    while True:
        beat('arbi_scan_loop')
        try:
            fetch_fx_rates()
            for pair in ARBI_PAIRS:
                beat('arbi_scan_loop')
                spread=calc_spread(pair)
                if not spread:
                    time.sleep(1); continue

                with state_lock: arbi_spreads[pair['id']]=spread

                # [v10.14] Threshold dinâmico por par
                _pair_cfg = ARBI_PAIR_CONFIG.get(pair['id'], ARBI_PAIR_CONFIG['_default'])
                _min_sp   = _pair_cfg['min_spread'] if _pair_cfg['min_spread'] else ARBI_MIN_SPREAD
                _max_sp   = _pair_cfg.get('max_spread', ARBI_MAX_SPREAD)
                spread['opportunity'] = _min_sp <= abs(spread.get('spread_pct',0)) <= _max_sp

                if abs(spread.get('spread_pct',0)) > _max_sp:
                    log.warning(f'[ARBI-SANITY] {pair["id"]} spread {spread["spread_pct"]:+.2f}% acima do teto {ARBI_MAX_SPREAD}% — possível preço inválido, ignorando')
                if not spread['opportunity'] or not spread['markets_open']:
                    time.sleep(1.5); continue

                # [v10.11] Position size dinâmico = portfolio_arbi / 3 (cresce com lucros)
                with state_lock:
                    _arbi_pnl_total = sum(t.get('pnl',0) for t in arbi_open) + sum(t.get('pnl',0) for t in arbi_closed)
                    # [v10.14] Portfolio arbi REAL = capital livre + posições abertas + todo P&L
                    # Isso faz os ganhos acumulados participarem das novas posições
                    _committed_arbi = sum(t.get('position_size',0) for t in arbi_open)
                    _arbi_port_val  = max(
                        arbi_capital + _committed_arbi,           # capital livre + comprometido
                        ARBI_CAPITAL + _arbi_pnl_total,           # inicial + pnl total
                        ARBI_CAPITAL)                             # mínimo = capital inicial
                # [v10.14-FIX] Respeitar max_pos por par (protege contra spreads voláteis)
                _pair_max_pos = ARBI_PAIR_CONFIG.get(pair['id'], {}).get('max_pos', None)
                _dynamic_pos = max(round(_arbi_port_val / 3), ARBI_POS_SIZE)
                if _pair_max_pos:
                    _dynamic_pos = min(_dynamic_pos, _pair_max_pos)
                risk_ok,risk_reason,approved_size=check_risk_arbi(pair['id'],_dynamic_pos)
                if not risk_ok:
                    if 'KILL_SWITCH' in risk_reason: break
                    time.sleep(1.5); continue

                bl=pair['leg_a'] if spread['direction']=='LONG_A' else pair['leg_b']
                sl=pair['leg_b'] if spread['direction']=='LONG_A' else pair['leg_a']
                bm=pair['mkt_a'] if spread['direction']=='LONG_A' else pair['mkt_b']
                sm=pair['mkt_b'] if spread['direction']=='LONG_A' else pair['mkt_a']
                trade_id=gen_id('ARB'); opened=False; pos=0

                with state_lock:
                    if any(t['pair_id']==pair['id'] for t in arbi_open): pass
                    elif not (market_open_for(pair['mkt_a']) and market_open_for(pair['mkt_b'])): pass
                    elif approved_size<=0 or arbi_capital<=0: pass
                    else:
                        pos=min(approved_size,arbi_capital); arbi_capital-=pos
                        # [v10.20] Ledger: RESERVE arbi
                        ledger_record('arbi', 'RESERVE', pair['name'], round(pos, 2), arbi_capital, trade_id)
                        # [v11-hook] dual-write arbi open
                        _v11_on_trade_open('arbi', trade_id, float(pos),
                                            metadata={'pair': pair.get('name', '')})
                        _entry_ts = datetime.utcnow().isoformat()
                        # [v10.14-Audit] Preço de entrada por lado
                        # LONG_A: compra leg_a (ask) e vende leg_b (bid)
                        _entry_price_a = spread.get('ask_a' if spread['direction']=='LONG_A' else 'bid_a', spread.get('price_a',0))
                        _entry_price_b = spread.get('bid_b' if spread['direction']=='LONG_A' else 'ask_b', spread.get('price_b',0))
                        # Custo de câmbio estimado (B3↔NYSE: 0.10% do volume)
                        _fx_cost = round(pos * 0.001, 2) if pair.get('fx','') == 'USDBRL' else 0
                        # Fee ARBI via BTG Day Trade — emolumentos B3 ~0.010% round trip
                        _fee_b3 = round(pos * 0.0001, 2) if 'B3' in [pair['mkt_a'], pair['mkt_b']] else round(pos * 0.0002, 2)
                        # Slippage estimado (posição / ADV proxy × fator)
                        _slippage_a_bps = round(min(pos / 5e6 * 10, 5), 2)  # estimativa conservadora
                        _slippage_b_bps = round(min(pos / 5e6 * 10, 5), 2)
                        _slippage_cost  = round(pos * (_slippage_a_bps + _slippage_b_bps) / 10000, 2)
                        # [v10.23] Custo de aluguel de ações (stock lending) para a perna short
                        # Taxas anuais típicas: B3 blue chips ~2% a.a., NYSE ADRs ~0.5% a.a.
                        # Custo = (position_size/2) × taxa_anual × (timeout_h / 8760)
                        _lending_rates = {'B3': 0.020, 'NYSE': 0.005, 'LSE': 0.008, 'EURONEXT': 0.008}  # % anual
                        _short_mkt = pair['mkt_b'] if spread['direction']=='LONG_A' else pair['mkt_a']
                        _lending_rate = _lending_rates.get(_short_mkt, 0.010)
                        _lending_cost = round((pos / 2) * _lending_rate * (ARBI_TIMEOUT_H / 8760), 2)
                        _qty_a = spread.get('qty_a_est', 0)
                        _qty_b = spread.get('qty_b_est', 0)
                        trade={'id':trade_id,'pair_id':pair['id'],'name':pair['name'],
                            'leg_a':pair['leg_a'],'leg_b':pair['leg_b'],
                            'mkt_a':pair['mkt_a'],'mkt_b':pair['mkt_b'],
                            'direction':spread['direction'],'buy_leg':bl,'buy_mkt':bm,
                            'short_leg':sl,'short_mkt':sm,
                            # Spreads — raw e normalizado
                            'entry_spread':spread.get('entry_spread_normalized', spread['spread_pct']),
                            'entry_spread_raw':spread['spread_pct'],
                            'current_spread':spread['spread_pct'],
                            'position_size':round(pos,2),
                            'pnl':0,'pnl_pct':0,'peak_pnl_pct':0,
                            'fx_rate':spread['fx_rate'],'fx_ts':spread.get('fx_ts',_entry_ts),
                            # Timestamps Sprint 1
                            'entry_ts':_entry_ts,
                            'signal_ts_a':spread.get('signal_ts_a',_entry_ts),
                            'signal_ts_b':spread.get('signal_ts_b',_entry_ts),
                            'delta_ts_between_legs_ms':spread.get('delta_ts_ms',0),
                            # Preços por perna
                            'price_a_entry':_entry_price_a,
                            'price_b_entry':_entry_price_b,
                            'price_a_usd_norm':spread.get('price_a_usd',0),
                            'price_b_usd_norm':spread.get('price_b_usd',0),
                            'bid_a':spread.get('bid_a',0),'ask_a':spread.get('ask_a',0),
                            'bid_b':spread.get('bid_b',0),'ask_b':spread.get('ask_b',0),
                            'price_source_a':spread.get('price_source_a','last'),
                            'price_source_b':spread.get('price_source_b','last'),
                            # Quantidade
                            'qty_a':_qty_a,'qty_b':_qty_b,
                            'ratio_a':pair.get('ratio_a',1),'ratio_b':pair.get('ratio_b',1),
                            # Custos detalhados Sprint 1
                            'broker_fee_a':0,'broker_fee_b':0,  # BTG Day Trade ZERO
                            'exchange_fee_a':round(_fee_b3/2,2),'exchange_fee_b':round(_fee_b3/2,2),
                            'fx_cost':_fx_cost,
                            'slippage_cost_a':round(_slippage_cost/2,2),
                            'slippage_cost_b':round(_slippage_cost/2,2),
                            'slippage_bps_total':round(_slippage_a_bps+_slippage_b_bps,2),
                            'lending_cost':_lending_cost,
                            'lending_rate_annual':_lending_rate,
                            'total_cost_estimated':round(_fee_b3 + _fx_cost + _slippage_cost + _lending_cost,2),
                            # Audit flags
                            'audit_flag':'valid',
                            'simulation_model_version':'v2.0',
                            'fee_model_version':'v1.0',
                            'slippage_model_version':'v1.0',
                            'opened_at':_entry_ts,'status':'OPEN','asset_type':'arbitrage'}
                        arbi_open.append(trade); opened=True

                if opened:
                    audit('ARBI_OPENED',{'id':trade_id,'pair':pair['id'],'spread':spread['abs_spread']})
                    enqueue_persist('arbi',trade)
                    send_whatsapp(f"ARBI: {pair['name']} spread {spread['abs_spread']:.2f}% ${pos:,.0f}")

                time.sleep(1.5)
        except Exception as e: log.error(f'arbi_scan: {e}')

        beat('arbi_scan_loop')
        time.sleep(300)
        beat('arbi_scan_loop')

def arbi_monitor_loop():
    global arbi_capital
    while True:
        beat('arbi_monitor_loop')
        time.sleep(60)
        try:
            closed_trades=[]
            with state_lock:
                now=datetime.utcnow(); to_close=[]
                for trade in arbi_open:
                    age_h=(now-datetime.fromisoformat(trade['opened_at'])).total_seconds()/3600
                    sd=arbi_spreads.get(trade['pair_id'])
                    if sd:
                        trade['current_spread']=sd['spread_pct']
                        ea=abs(float(trade['entry_spread'])); ca=abs(float(trade['current_spread']))
                        trade['pnl_pct']=round(ea-ca,4)
                        # [v10.14-FIX] Sanity check: spread > 20% = preço inválido
                        if abs(float(trade.get('current_spread', 0))) > 20.0:
                            log.warning(f"[ARBI-SANITY] {trade.get('pair_id')} spread={trade.get('current_spread')} INVÁLIDO")
                            trade['current_spread'] = trade.get('entry_spread', 0)
                            trade['pnl_pct'] = 0.0
                            trade['pnl'] = 0.0
                            continue
                    trade['pnl']=round(trade['pnl_pct']/100*float(trade['position_size']),2)
                    trade['peak_pnl_pct']=round(max(trade.get('peak_pnl_pct',0),trade['pnl_pct']),2)
                    peak=trade['peak_pnl_pct']
                    h=trade.setdefault('pnl_history',[]); h.append(trade['pnl_pct'])
                    if len(h)>5: h.pop(0)
                    reason=None
                    mkt_a=trade.get('mkt_a',''); mkt_b=trade.get('mkt_b','')
                    both_open=(market_open_for(mkt_a) and market_open_for(mkt_b))
                    if abs(trade.get('current_spread',99))<=ARBI_TP_SPREAD:  reason='TAKE_PROFIT'
                    elif peak>=2.0 and trade['pnl_pct']<=peak-1.0:           reason='TRAILING_STOP'
                    elif trade['pnl_pct']<=-ARBI_SL_PCT:                     reason='STOP_LOSS'
                    elif not both_open and age_h>=0.5:                       reason='MARKET_CLOSE'
                    elif age_h>=ARBI_TIMEOUT_H:
                        ext=trade.get('extensions',0)
                        if is_momentum_positive(trade) and ext<3: trade['extensions']=ext+1
                        else: reason='TIMEOUT'
                    if reason:
                        # [v10.20] Ledger: RELEASE + PNL_CREDIT arbi (ordem contábil correta)
                        arbi_capital += trade['position_size']
                        # [v11-hook] dual-write arbi close
                        _v11_on_trade_close('arbi', trade['id'],
                                             float(trade.get('position_size', 0) or 0),
                                             float(trade.get('pnl', 0) or 0),
                                             fees=float(trade.get('fees', 0) or 0))
                        ledger_record('arbi', 'RELEASE', trade.get('name', trade.get('pair_id', '')),
                                      trade['position_size'], arbi_capital, trade['id'])
                        arbi_capital += trade['pnl']
                        if trade['pnl'] != 0:
                            ledger_record('arbi', 'PNL_CREDIT', trade.get('name', trade.get('pair_id', '')),
                                          trade['pnl'], arbi_capital, trade['id'])
                        # [v10.22] Record to institutional modules
                        risk_manager.record_trade_result('arbi', trade.get('pair_id', ''), trade['pnl'], trade['position_size'], arbi_capital)
                        perf_stats.record_trade({
                            'strategy': 'arbi', 'symbol': trade.get('pair_id', ''),
                            'pnl': trade['pnl'], 'pnl_pct': trade.get('pnl_pct', 0),
                            'entry_price': trade.get('leg1_entry', 0), 'exit_price': trade.get('leg1_exit', 0),
                            'opened_at': trade.get('opened_at', now.isoformat()), 'closed_at': now.isoformat(),
                            'confidence': 0, 'exit_type': reason, 'asset_type': 'arbi',
                            'regime': market_regime.get('mode', 'UNKNOWN'),
                        })
                        c=dict(trade); c.update({'closed_at':now.isoformat(),'close_reason':reason,'status':'CLOSED'})
                        arbi_closed.insert(0,c)
                        # [v10.9] Sem limite em memória — histórico completo
                        to_close.append(trade['id']); closed_trades.append(c)
                arbi_open[:] = [t for t in arbi_open if t['id'] not in to_close]

            for c in closed_trades:
                audit('ARBI_CLOSED',{'id':c['id'],'pair':c['pair_id'],'pnl':c['pnl'],'reason':c['close_reason']})
                # [v10.14] Aprendizado por par — ajusta threshold após cada fechamento
                _pair_recent = [t for t in list(arbi_closed)[:20] if t.get('pair_id')==c['pair_id']]
                if len(_pair_recent) >= 3:
                    _arbi_pair_learning(c['pair_id'], _pair_recent)
                enqueue_persist('arbi',c)
                # [v10.14] Alimentar o sistema de aprendizado de thresholds
                try:
                    arbi_learn_from_closed(
                        pair_id=c.get('pair_id',''),
                        entry_spread=float(c.get('entry_spread',0)),
                        pnl=float(c.get('pnl',0)),
                        direction=c.get('direction','LONG_A')
                    )
                except Exception as _le: log.warning(f'arbi_learn: {_le}')
        except Exception as e: log.error(f'arbi_monitor: {e}')

# ═══════════════════════════════════════════════════════════════
# [C-1] WATCHDOG — timeout por thread + [V9-3] _check_degraded
# ═══════════════════════════════════════════════════════════════
def watchdog():
    while True:
        beat('watchdog')
        # [v10.16] Periodic checks
        try:
            evaluate_symbol_blacklist()
            check_inactivity_alert()
            persist_calibration()    # [v10.18] salvar calibração a cada 5min
            run_reconciliation()     # [v10.18] reconciliar capital a cada 10min

            # [v10.22] Institutional risk check
            try:
                breached, reasons = risk_manager.is_breached()
                if breached:
                    log.warning(f'[RISK-BREACH] {reasons}')
                    ext_kill_switch.auto_activate_on_risk_breach(reasons, get_db)
            except Exception as _risk_e:
                log.debug(f'watchdog risk check: {_risk_e}')

            # [v10.22] Market data quality check
            try:
                with state_lock:
                    for sym, pd in stock_prices.items():
                        if pd and pd.get('price', 0) > 0:
                            data_validator.record_price(sym, pd['price'], 'stock_poller')
                    for sym, pd in crypto_prices.items():
                        if pd and pd.get('price', 0) > 0:
                            data_validator.record_price(sym, pd['price'], 'crypto_poller')
            except Exception as _dv_e:
                log.debug(f'watchdog data validator: {_dv_e}')
        except Exception as _wdg_e:
            log.debug(f'watchdog v10.16+ checks: {_wdg_e}')
        time.sleep(30)
        now=time.time()

        # [V9-3] Atualizar modo degradado a cada ciclo do watchdog
        try: _check_degraded()
        except Exception as e: log.error(f'watchdog _check_degraded: {e}')

        # [V91-3] Alerta de fila crítica direto no watchdog — não depende do persistence_worker
        qsize = urgent_queue.qsize()
        if qsize >= URGENT_QUEUE_CRIT:
            global _queue_alert_last
            if now - _queue_alert_last > 300:
                _queue_alert_last = now
                log.critical(f'[V91-3] WATCHDOG: urgent_queue CRÍTICA {qsize} itens — DB pode estar travado')
                send_whatsapp(f'CRÍTICO (watchdog): fila de persistência com {qsize} itens. Verificar banco.')

        for name, t in list(thread_health.items()):
            if name=='watchdog': continue
            alive   = t.is_alive()
            hb      = thread_heartbeat.get(name, now)
            timeout = THREAD_HEARTBEAT_TIMEOUT.get(name, DEFAULT_HB_TIMEOUT)
            hb_ok   = (now-hb) < timeout

            if alive and hb_ok:
                last_restart=thread_last_restart.get(name,0)
                if (thread_restart_count.get(name,0)>0 and last_restart>0 and
                        (now-last_restart)/3600 >= WATCHDOG_RESET_STABLE_H):
                    old=thread_restart_count[name]; thread_restart_count[name]=0
                    log.info(f'WATCHDOG: {name} stable {WATCHDOG_RESET_STABLE_H}h — reset count (was {old})')
                continue

            problem='DEAD' if not alive else f'FROZEN (no beat for {now-hb:.0f}s, timeout={timeout}s)'
            count=thread_restart_count.get(name,0)
            log.error(f'WATCHDOG: {name} {problem} (restart #{count+1})')

            if count>=5:  # [v10.9] era 3x — aumentado para reduzir falsos positivos
                log.critical(f'WATCHDOG: {name} failed 5x — activating kill switch')
                global RISK_KILL_SWITCH
                RISK_KILL_SWITCH=True
                send_whatsapp(f'CRITICO: thread {name} falhou 3x ({problem}). Kill switch ativado.')
                thread_restart_count[name]=0
                continue

            fn=thread_fns.get(name)
            if fn:
                try:
                    # [v10.52] Thread limit — era 45 (falso alarme).
                    # O daemon tem ~32 workers legitimos (21 main + 11
                    # derivatives). Flask/waitress + Cedro socket reader +
                    # pool MySQL + threads de IO somam ~15 extras.
                    # Total saudavel ~47. Limite de 45 disparava alerta
                    # em estado NORMAL do sistema. Comentario antigo
                    # 'padrao Python = 50' estava errado — Python nao tem
                    # limite inerente, Linux permite 4000+ threads/processo.
                    # Novo limite: 150 (acomoda 3x saudavel, ainda bloqueia
                    # runaway legitimo). Configuravel via env var.
                    import threading as _th
                    active = _th.active_count()
                    thread_limit = int(os.environ.get('WATCHDOG_THREAD_LIMIT', '150'))
                    if active > thread_limit:
                        # Lista threads ativas para diagnostico
                        try:
                            names = sorted([t.name for t in _th.enumerate() if t.is_alive()])
                            names_sample = ', '.join(names[:10]) + (f', +{len(names)-10} mais' if len(names) > 10 else '')
                        except Exception:
                            names_sample = '(erro listando)'
                        log.error(f'WATCHDOG: {name} NAO reiniciada — threads ativos={active}/{thread_limit} (limite). Amostra: {names_sample}')
                        send_whatsapp(f'CRITICO: thread starvation! {active}/{thread_limit} threads ativos. Reiniciar o serviço.')
                        # Marcar heartbeat para não tentar de novo em loop
                        thread_heartbeat[name] = time.time()
                    else:
                        new_t=threading.Thread(target=fn,daemon=True,name=f'wdg-{name}'); new_t.start()
                        thread_health[name]=new_t
                        thread_restart_count[name]=count+1
                        thread_last_restart[name]=now
                        thread_heartbeat[name]=now
                        log.warning(f'WATCHDOG: {name} restarted (attempt {count+1}), threads={active}')
                        send_whatsapp(f'ALERTA: {name} ({problem}) reiniciada (tentativa {count+1})')
                except Exception as e: log.error(f'WATCHDOG restart {name}: {e}')

def network_sync_loop():
    """Faz PUSH periódico dos dados do Railway para o Manus a cada 30 minutos."""
    import requests as _req
    # Aguarda startup com beats para não ser detectado como FROZEN pelo watchdog
    for _ in range(6):
        beat('network_sync_loop')
        time.sleep(10)
    while True:
        beat('network_sync_loop')
        try:
            peer_url = SYNC_PEER_URL.rstrip('/')
            # Gerar export local
            with learning_lock:
                patterns_raw = dict(pattern_stats_cache)
            with state_lock:
                all_cl = list(stocks_closed) + list(crypto_closed)
            total_patterns = len(patterns_raw)
            avg_conf = round(sum(p.get('confidence',0) for p in patterns_raw.values()) / max(len(patterns_raw),1), 2) if patterns_raw else 0.0
            hot_signals = []
            for sym, p in list(patterns_raw.items())[:20]:
                if p.get('confidence',0) >= 0.6:
                    hot_signals.append({'symbol': sym, 'action': p.get('best_action','BUY'), 'score': p.get('confidence',0)*100, 'market': p.get('market','UNKNOWN')})
            market_stats = {}
            for mkt in ['B3','CRYPTO','NYSE']:
                mkt_trades = [t for t in all_cl if t.get('asset_type','').upper()==mkt or (mkt=='B3' and t.get('asset_type','')=='stock' and not t.get('symbol','').endswith('USDT'))]
                if mkt_trades:
                    wins = sum(1 for t in mkt_trades if t.get('pnl',0)>0)
                    market_stats[mkt] = {'total_trades': len(mkt_trades), 'win_rate': round(wins/len(mkt_trades)*100,1), 'total_pnl': round(sum(t.get('pnl',0) for t in mkt_trades),2), 'avg_pnl_pct': round(sum(t.get('pnl_pct',0) for t in mkt_trades)/len(mkt_trades),2)}
            payload = {
                'system': 'egreja-railway',
                'exported_at': datetime.utcnow().isoformat() + 'Z',
                'learning': {'total_patterns': total_patterns, 'avg_confidence': avg_conf, 'learning_enabled': LEARNING_ENABLED,
                                  'composite_patterns': len(_composite_patterns),
                                  'composite_reliable': sum(1 for v in _composite_patterns.values() if v.get('reliable')),
                                  'composite_blocked_windows': sum(1 for v in _composite_patterns.values() if v.get('blocked')),
                                  'last_discovery_run': _last_discovery_run},
                'hot_signals': hot_signals,
                'market_stats': market_stats,
                'portfolio': {'initial_capital': INITIAL_CAPITAL_STOCKS + INITIAL_CAPITAL_CRYPTO, 'stocks_capital': INITIAL_CAPITAL_STOCKS, 'crypto_capital': INITIAL_CAPITAL_CRYPTO}
            }
            r = _req.post(f'{peer_url}/sync/import', json=payload, timeout=10)
            if r.status_code == 200:
                log.info(f'[NETWORK] ✅ Push para Manus OK: {len(hot_signals)} sinais, {total_patterns} padrões')
            else:
                log.warning(f'[NETWORK] Push para Manus status={r.status_code}')
        except Exception as e:
            log.debug(f'[NETWORK] Push para Manus falhou: {e}')
        # Sleep de 30 min com beats a cada 60s para o watchdog não matar a thread
        for _ in range(30):
            beat('network_sync_loop')
            time.sleep(60)


# ═══ [v10.35] Derivatives boot reconciliation + monitoring loop ═══
def _rehydrate_derivatives_state():
    """
    Rehydrate DerivativesExecutionEngine.active_trades and
    DerivativesCapitalManager.active_allocations from DB after every restart.

    Without this, the engine loses all knowledge of previously opened paper trades
    when Railway restarts the process, so the monitor never generates exits and
    PnL never gets marked. Called once during start_background_threads().
    """
    from datetime import datetime as _dt_fix
    if _deriv_exec is None or _deriv_cap_mgr is None:
        log.warning('[v10.35] Rehydrate skipped — execution/capital not initialized')
        return

    conn = None
    try:
        conn = get_db()
        if conn is None:
            log.warning('[v10.35] Rehydrate skipped — no DB connection')
            return

        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """SELECT trade_id, strategy_type, symbol, strike, expiry,
                      structure_type, expected_edge, notional,
                      liquidity_score, active_status, opened_at, status
               FROM strategy_master_trades
               WHERE status = 'OPEN'
               ORDER BY opened_at ASC
               LIMIT 2000"""
        )
        trade_rows = cursor.fetchall()

        # Index legs per trade_id
        legs_by_trade = {}
        if trade_rows:
            tids = [r['trade_id'] for r in trade_rows]
            placeholders = ','.join(['%s'] * len(tids))
            cursor.execute(
                f"""SELECT trade_id, leg_type, symbol, qty, side,
                           intended_price, executed_price, fill_status,
                           slippage, latency_ms, timestamp
                    FROM strategy_trade_legs
                    WHERE trade_id IN ({placeholders})""",
                tids,
            )
            for row in cursor.fetchall():
                legs_by_trade.setdefault(row['trade_id'], []).append(row)
        cursor.close()

        restored_trades = 0
        restored_capital = 0
        for row in trade_rows:
            try:
                # Rebuild TradeLeg objects
                trade_legs = []
                for i, lrow in enumerate(legs_by_trade.get(row['trade_id'], [])):
                    try:
                        lstatus = LegStatus(lrow['fill_status'])
                    except Exception:
                        lstatus = LegStatus.FILLED
                    leg = TradeLeg(
                        leg_id=f"{row['trade_id']}-L{i}",
                        leg_type=lrow['leg_type'],
                        symbol=lrow['symbol'],
                        qty=int(lrow['qty'] or 0),
                        side=lrow['side'],
                        intended_price=float(lrow['intended_price'] or 0),
                        executed_price=float(lrow['executed_price'] or 0),
                        slippage=float(lrow['slippage'] or 0),
                        latency_ms=int(lrow['latency_ms'] or 0),
                        status=lstatus,
                        timestamp=lrow['timestamp'] or _dt_fix.utcnow(),
                    )
                    trade_legs.append(leg)

                trade = DerivativesTrade(
                    trade_id=row['trade_id'],
                    strategy=(row['strategy_type'] or '').lower(),
                    symbol=row['symbol'] or '',
                    structure_type=row['structure_type'] or '',
                    strike=float(row['strike'] or 0),
                    expiry=row['expiry'] or '',
                    expected_edge=float(row['expected_edge'] or 0),
                    notional=float(row['notional'] or 0),
                    legs=trade_legs,
                    status=TradeStatus.OPEN,
                    opened_at=row['opened_at'] or _dt_fix.utcnow(),
                    liquidity_score=float(row['liquidity_score'] or 0),
                    active_status=row['active_status'] or '',
                )

                _deriv_exec.active_trades[trade.trade_id] = trade
                restored_trades += 1

                # Rebuild capital allocation (margin ~15% of notional)
                try:
                    margin_amt = float(row['notional'] or 0) * 0.15
                    from modules.derivatives.capital import AllocationRecord as _AR
                    rec = _AR(
                        trade_id=trade.trade_id,
                        strategy=trade.strategy,
                        symbol=trade.symbol,
                        amount=margin_amt,
                        direction='ALLOCATE',
                        timestamp=trade.opened_at,
                    )
                    _deriv_cap_mgr.active_allocations[trade.trade_id] = rec
                    _deriv_cap_mgr.allocated += margin_amt
                    _deriv_cap_mgr.strategy_allocated[trade.strategy] += margin_amt
                    restored_capital += 1
                except Exception as _cap_err:
                    log.debug(f'[v10.35] Capital restore failed for {trade.trade_id}: {_cap_err}')
            except Exception as _trow_err:
                log.debug(f"[v10.35] Trade restore failed for {row.get('trade_id')}: {_trow_err}")

        log.info(
            f'[v10.35] Rehydrated {restored_trades} open trades into exec.active_trades, '
            f'{restored_capital} into capital.active_allocations '
            f'(allocated=R${_deriv_cap_mgr.allocated:,.0f})'
        )
    except Exception as e:
        log.warning(f'[v10.35] Rehydrate error: {e}')
        import traceback as _tb
        _tb.print_exc()
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def deriv_monitor_loop():
    """Derivatives monitoring background loop — mark-to-market + exit triggers."""
    loop_name = 'deriv_monitor_loop'
    if _deriv_monitor is None:
        log.warning(f'[v10.35] {loop_name} skipped — monitor not initialized')
        # Keep thread alive but idle so watchdog doesn't churn
        while True:
            beat(loop_name)
            time.sleep(60)
    try:
        _deriv_monitor.monitoring_loop(beat, log)
    except Exception as e:
        log.error(f'[v10.35] {loop_name} crashed: {e}')
        import traceback; traceback.print_exc()


def start_background_threads():
    # [v10.35] Rehydrate paper trades from DB BEFORE threads start so monitor sees them
    try:
        _rehydrate_derivatives_state()
    except Exception as _rh_err:
        log.warning(f'[v10.35] Rehydrate call failed: {_rh_err}')

    defs = {
        'stock_price_loop':       stock_price_loop,
        'crypto_price_loop':      crypto_price_loop,
        'monitor_trades':         monitor_trades,
        'auto_trade_crypto':      auto_trade_crypto,
        'stock_execution_worker': stock_execution_worker,
        'arbi_scan_loop':         arbi_scan_loop,
        'arbi_monitor_loop':      arbi_monitor_loop,
        'snapshot_loop':          snapshot_loop,
        'persistence_worker':     persistence_worker,
        'alert_worker':           alert_worker,
        'pattern_discovery':      pattern_discovery_loop,   # [v10.13] mineração periódica
        'arbi_learning_loop':     arbi_learning_loop,        # [v10.14] aprendizado específico de arbi
        'watchdog':               watchdog,
        'shadow_evaluator_loop':  shadow_evaluator_loop,   # [FIX-5]
        'network_sync_loop':      network_sync_loop,       # [NETWORK] push periódico para Manus
        'report_scheduler':       _report_scheduler,        # relatórios automáticos
        'brain_hourly_reminder':  _brain_hourly_reminder,   # [v2.2] lembrete horário do Unified Brain
        'monthly_picks_worker':   _monthly_picks_worker,    # [v3.2] stock picker mensal + review semanal (modular)
        'deriv_monitor_loop':     deriv_monitor_loop,        # [v10.35] paper MTM + exits (stop/target/expiry)
    }
    # [v10.25] Derivatives strategy scan loops (paper/shadow mode)
    _deriv_loop_args = dict(
        beat_fn=beat, get_db_fn=get_db, log=log,
        provider_mgr=globals().get('_deriv_provider_mgr'),
        services_dict=globals().get('_deriv_services', {}),
        risk_check_fn=lambda strat, sym, notional: (True, 'OK'),  # paper mode: always approve
        audit_fn=lambda evt, data: log.debug(f'[DERIV-AUDIT] {evt}: {data}'),
    )
    _deriv_loops = {
        'pcp_scan_loop': pcp_scan_loop,
        'fst_scan_loop': fst_scan_loop,
        'roll_arb_scan_loop': roll_arb_scan_loop,
        'etf_basket_scan_loop': etf_basket_scan_loop,
        'skew_arb_scan_loop': skew_arb_scan_loop,
        'interlisted_scan_loop': interlisted_scan_loop,
        'dividend_arb_scan_loop': dividend_arb_scan_loop,
        'vol_arb_scan_loop': vol_arb_scan_loop,
        # [v10.38] New arbitrage strategies
        'ibov_basis_scan_loop': ibov_basis_scan_loop,
        'di_calendar_scan_loop': di_calendar_scan_loop,
        'interlisted_hedged_scan_loop': interlisted_hedged_scan_loop,
    }
    for dname, dfn in _deriv_loops.items():
        if dfn is None:
            log.warning(f'[v10.25] {dname} not available (stub mode)')
            continue
        def _make_deriv_wrapper(fn, name, args):
            def wrapper():
                try:
                    fn(**args)
                except Exception as e:
                    log.error(f'[v10.25] {name} crashed: {e}')
                    import traceback; traceback.print_exc()
            return wrapper
        _wrapped = _make_deriv_wrapper(dfn, dname, _deriv_loop_args)
        defs[dname] = _wrapped
    now=time.time()
    for name,fn in defs.items():
        thread_fns[name]=fn; thread_restart_count[name]=0
        thread_last_restart[name]=0; thread_heartbeat[name]=now
        t=threading.Thread(target=fn,daemon=True); t.start()
        thread_health[name]=t
        log.info(f'Thread started: {name} (hb_timeout={THREAD_HEARTBEAT_TIMEOUT.get(name,DEFAULT_HB_TIMEOUT)}s)')

# ═══════════════════════════════════════════════════════════════
# WATCHLIST
# ═══════════════════════════════════════════════════════════════
watchlist_symbols=[]
watchlist_lock=threading.Lock()

def init_watchlist_table():
    global watchlist_symbols
    conn=get_db()
    if not conn: return
    cursor=None
    try:
        cursor=conn.cursor(dictionary=True)
        cursor.execute("SELECT symbol, market, added_at FROM watchlist")
        watchlist_symbols=[{'symbol':r['symbol'],'market':r['market'],
            'addedAt':r['added_at'].isoformat() if r['added_at'] else ''} for r in cursor.fetchall()]
        cursor.close(); conn.close()
        log.info(f'Watchlist: {len(watchlist_symbols)} loaded')
    except Exception as e: log.error(f'Watchlist init: {e}')

@app.route('/watchlist/quote')
def watchlist_quote():
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

@app.route('/watchlist/add', methods=['POST'])
def watchlist_add():
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
                conn.commit()
            except Exception as e: log.error(f'Watchlist add DB: {e}')
            finally:
                try:
                    if cursor: cursor.close()
                except: pass
                try:
                    if conn: conn.close()
                except: pass
        watchlist_symbols.append({'symbol':symbol,'market':market,'addedAt':datetime.utcnow().isoformat()})
    return jsonify({'ok':True,'total':len(watchlist_symbols)})

@app.route('/watchlist/remove', methods=['POST'])
def watchlist_remove():
    global watchlist_symbols
    data=request.get_json() or {}; symbol=data.get('symbol','').upper().strip()
    with watchlist_lock:
        conn=get_db()
        if conn:
            try:
                cursor=conn.cursor()
                cursor.execute("DELETE FROM watchlist WHERE symbol=%s",(symbol,))
                conn.commit()
            except Exception as e: log.error(f'Watchlist remove DB: {e}')
            finally:
                try:
                    if cursor: cursor.close()
                except: pass
                try:
                    if conn: conn.close()
                except: pass
        watchlist_symbols=[w for w in watchlist_symbols if w['symbol']!=symbol]
    return jsonify({'ok':True,'total':len(watchlist_symbols)})

@app.route('/watchlist')
def watchlist_get():
    with watchlist_lock: syms=list(watchlist_symbols)
    return jsonify({'symbols':syms,'total':len(syms)})

# ═══════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════
@app.route('/admin/fix_corrupted_arbi_trade', methods=['POST'])
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
@app.route('/derivatives')
def derivatives_dashboard():
    """Serve the standalone derivatives trading dashboard."""
    return send_from_directory('static', 'derivatives.html')

@app.route('/cedro/health')
def cedro_health():
    """[v10.31] Cedro socket provider status."""
    if not _cedro_socket:
        return jsonify({'enabled': False, 'reason': 'provider_not_loaded'}), 503
    return jsonify(_cedro_socket.healthcheck())


@app.route('/cedro/analysis/<symbol>')
def cedro_analysis(symbol):
    """[v10.31] Full Cedro real-time analysis for a symbol:
    price, prev, day/week/month/year high-low, market cap, sector, bid/ask, volume, variations."""
    if not _cedro_socket or not _cedro_socket.enabled:
        return jsonify({'error': 'cedro_disabled'}), 503
    data = _cedro_socket.get_analysis(symbol)
    if not data.get('price'):
        return jsonify({'error': 'no_data', 'symbol': symbol.upper()}), 404
    return jsonify(data)


@app.route('/cedro/quotes')
def cedro_quotes():
    """[v10.31] Bulk Cedro snapshots. Query: ?symbols=PETR4,VALE3,ITUB4"""
    if not _cedro_socket or not _cedro_socket.enabled:
        return jsonify({'error': 'cedro_disabled'}), 503
    raw = request.args.get('symbols', '')
    syms = [s.strip().upper() for s in raw.split(',') if s.strip()]
    if not syms:
        return jsonify({'error': 'no_symbols'}), 400
    batch = _cedro_socket.get_batch(syms, wait_ms=2000)
    return jsonify({
        'count': len(batch),
        'quotes': {k: _cedro_socket.get_analysis(k) for k in batch.keys()},
    })


@app.route('/health')
def health():
    # [hotfix] timeout defensivo: se scan loop segura lock, não trava HTTP
    open_count = -1
    if state_lock.acquire(timeout=2):
        try: open_count = len(stocks_open) + len(crypto_open)
        finally: state_lock.release()
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

@app.route('/ops')
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
@app.route('/kill-switch/activate', methods=['POST'])
@require_auth
def kill_switch_activate():
    """[v10.22] Activate kill switch via API."""
    data = request.get_json() or {}
    scope = data.get('scope', 'global')
    reason = data.get('reason', 'Manual activation via API')
    auto_resume = data.get('auto_resume_minutes')
    ext_kill_switch.activate(scope, reason, 'API', auto_resume, get_db)
    audit_logger.log_action('API', 'KILL_SWITCH', f'Activated {scope}: {reason}', get_db)
    return jsonify({'ok': True, 'scope': scope, 'reason': reason})

@app.route('/kill-switch/deactivate', methods=['POST'])
@require_auth
def kill_switch_deactivate():
    """[v10.22] Deactivate kill switch via API."""
    data = request.get_json() or {}
    scope = data.get('scope', 'global')
    ext_kill_switch.deactivate(scope, 'API', get_db)
    audit_logger.log_action('API', 'KILL_SWITCH', f'Deactivated {scope}', get_db)
    return jsonify({'ok': True, 'scope': scope})

@app.route('/kill-switch/status')
@require_auth
def kill_switch_status():
    """[v10.22] Kill switch status."""
    return jsonify(ext_kill_switch.check_all(get_db))

# ── [v10.22] Risk endpoints ─────────────────────────────────────────
@app.route('/risk/institutional')
@require_auth
def risk_institutional():
    """[v10.22] Institutional risk status."""
    return jsonify(risk_manager.get_status())

# ── [v10.22] Performance endpoints ──────────────────────────────────
@app.route('/stats/report')
@require_auth
def stats_report():
    """[v10.22] Full performance report."""
    return jsonify(perf_stats.get_full_report())

@app.route('/stats/promotion')
@require_auth
def stats_promotion():
    """[v10.22] Capital promotion criteria check."""
    return jsonify(perf_stats.get_promotion_criteria())

# ── [v10.22] RBAC endpoints ────────────────────────────────────────
@app.route('/admin/users', methods=['GET'])
@require_auth
def admin_list_users():
    """[v10.22] List all RBAC users."""
    return jsonify(auth_manager.list_users(get_db))

@app.route('/admin/users', methods=['POST'])
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

@app.route('/admin/audit')
@require_auth
def admin_audit():
    """[v10.22] View audit log."""
    limit = request.args.get('limit', 100, type=int)
    return jsonify(audit_logger.get_recent(limit, get_db))

# ── [v10.22] Data quality endpoint ──────────────────────────────────


def _fetch_yahoo_history_cached(yahoo_symbol, days=60):
    """Busca histórico diário do Yahoo Finance para um símbolo (IBOV=^BVSP, SPY=SPY).
    Cacheia em memória por 1h para não martelar o Yahoo.
    Retorna dict {iso_date: close_price} ou {} se falhar.
    """
    import requests, time as _t, json as _j
    cache_key = f'_yahoo_hist_{yahoo_symbol}_{days}'
    g = globals()
    if cache_key in g:
        ts, data = g[cache_key]
        if _t.time() - ts < 3600:
            return data
    try:
        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}?interval=1d&range={days}d'
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        if r.status_code != 200: return {}
        j = r.json()
        result = (j.get('chart') or {}).get('result') or []
        if not result: return {}
        timestamps = result[0].get('timestamp') or []
        indicators = result[0].get('indicators') or {}
        quote = (indicators.get('quote') or [{}])[0]
        closes = quote.get('close') or []
        from datetime import datetime as _dt
        out = {}
        for ts, cl in zip(timestamps, closes):
            if cl is None: continue
            d = _dt.utcfromtimestamp(ts).strftime('%Y-%m-%d')
            out[d] = float(cl)
        g[cache_key] = (_t.time(), out)
        return out
    except Exception as e:
        log.debug(f'yahoo history {yahoo_symbol}: {e}')
        return {}


@app.route('/performance/benchmark')
def benchmark_comparison():
    """[v10.44] Benchmark comparison — system returns vs IBOV/SPY com alpha/beta real.

    Query params:
      days (int, default=30, max=180) - janela temporal
      market (str: 'B3', 'NYSE', 'CRYPTO', 'ALL', default='ALL')

    Retorna:
      summary: retornos acumulados + vs benchmarks
      series: lista de pontos {date, system_cum_pct, ibov_cum_pct, spy_cum_pct, alpha_day}
      alpha_beta: por mercado (alpha, beta, sharpe, max_dd, wins/losses)
    """
    conn = get_db()
    if not conn: return jsonify({'error': 'DB unavailable'}), 503
    try:
        days = min(int(request.args.get('days', 30)), 180)
        market_filter = request.args.get('market', 'ALL').upper()
        cursor = conn.cursor(dictionary=True)

        where_market = "" if market_filter == 'ALL' else "AND market = %s"
        params = (days,) if market_filter == 'ALL' else (days, market_filter)
        cursor.execute(f"""
            SELECT DATE(closed_at) AS dia, market,
                   SUM(pnl) AS pnl_dia,
                   SUM(position_value) AS capital_dia,
                   SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins,
                   COUNT(*) AS trades
            FROM trades
            WHERE status='closed'
              AND closed_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
              AND pnl IS NOT NULL
              {where_market}
            GROUP BY DATE(closed_at), market
            ORDER BY dia ASC
        """, params)
        daily_rows = cursor.fetchall()

        from collections import defaultdict
        daily_pnl = defaultdict(float)
        daily_cap = defaultdict(float)
        daily_trades = defaultdict(int)
        daily_wins = defaultdict(int)
        market_daily = defaultdict(lambda: defaultdict(lambda: {'pnl': 0, 'cap': 0, 'n': 0, 'w': 0}))

        for r in daily_rows:
            d = r['dia'].isoformat() if r['dia'] else None
            if not d: continue
            pnl_d = float(r['pnl_dia'] or 0)
            cap_d = float(r['capital_dia'] or 0)
            trades_d = int(r['trades'] or 0)
            wins_d = int(r['wins'] or 0)
            daily_pnl[d] += pnl_d
            daily_cap[d] += cap_d
            daily_trades[d] += trades_d
            daily_wins[d] += wins_d
            md = market_daily[r['market']][d]
            md['pnl'] += pnl_d; md['cap'] += cap_d
            md['n'] += trades_d; md['w'] += wins_d

        sorted_dates = sorted(daily_pnl.keys())
        if not sorted_dates:
            return jsonify({'error': 'no_data', 'days': days, 'market': market_filter}), 404

        # Buscar IBOV e SPY
        ibov_hist = _fetch_yahoo_history_cached('^BVSP', days=days + 10)
        spy_hist = _fetch_yahoo_history_cached('SPY', days=days + 10)

        def _cum_return_from_hist(hist, dates):
            """Calcula retorno acumulado a partir de dict {date: close}."""
            if not hist or not dates: return [0] * len(dates)
            first_date = None
            for d in sorted(hist.keys()):
                if d >= dates[0]:
                    first_date = d; break
            if not first_date: return [0] * len(dates)
            base = hist[first_date]
            out = []
            last_price = base
            for d in dates:
                if d in hist:
                    last_price = hist[d]
                ret = (last_price / base - 1) * 100
                out.append(round(ret, 4))
            return out

        ibov_cum = _cum_return_from_hist(ibov_hist, sorted_dates)
        spy_cum = _cum_return_from_hist(spy_hist, sorted_dates)

        # Retorno do sistema
        series = []
        cum_system = 0.0
        for i, d in enumerate(sorted_dates):
            cap = daily_cap.get(d, 0)
            pnl = daily_pnl.get(d, 0)
            ret_day = (pnl / cap * 100) if cap > 0 else 0
            cum_system += ret_day
            series.append({
                'date': d,
                'system_daily_pct': round(ret_day, 4),
                'system_cum_pct': round(cum_system, 4),
                'ibov_cum_pct': ibov_cum[i] if i < len(ibov_cum) else 0,
                'spy_cum_pct': spy_cum[i] if i < len(spy_cum) else 0,
                'alpha_vs_ibov': round(cum_system - (ibov_cum[i] if i < len(ibov_cum) else 0), 4),
                'alpha_vs_spy': round(cum_system - (spy_cum[i] if i < len(spy_cum) else 0), 4),
                'trades': daily_trades.get(d, 0),
                'wins': daily_wins.get(d, 0),
                'pnl': round(pnl, 2),
                'capital': round(cap, 2),
            })

        # Alpha/Beta por mercado
        def _compute_stats(date_to_market, benchmark_cum, dates):
            """Calcula retornos diários do mercado e compara com benchmark."""
            sys_rets = []
            bench_rets = [benchmark_cum[i] - (benchmark_cum[i-1] if i > 0 else 0) for i in range(len(benchmark_cum))]
            for i, d in enumerate(dates):
                mdata = date_to_market.get(d, {'pnl': 0, 'cap': 0})
                r = (mdata['pnl'] / mdata['cap'] * 100) if mdata['cap'] > 0 else 0
                sys_rets.append(r)
            n = len(sys_rets)
            if n < 2:
                return None
            mean_s = sum(sys_rets) / n
            mean_b = sum(bench_rets) / n
            var_s = sum((r - mean_s) ** 2 for r in sys_rets) / n
            var_b = sum((r - mean_b) ** 2 for r in bench_rets) / n
            cov = sum((sys_rets[i] - mean_s) * (bench_rets[i] - mean_b) for i in range(n)) / n
            beta = (cov / var_b) if var_b > 0 else 0
            alpha = mean_s - beta * mean_b  # daily alpha
            std_s = var_s ** 0.5
            sharpe = (mean_s / std_s * (252 ** 0.5)) if std_s > 0 else 0  # annualized
            cum_s = sum(sys_rets)
            cum_b = sum(bench_rets)
            peak = 0; max_dd = 0; cum = 0
            for r in sys_rets:
                cum += r
                if cum > peak: peak = cum
                dd = peak - cum
                if dd > max_dd: max_dd = dd
            total_trades = sum(market_daily[mkt][d]['n'] for d in dates for mkt in market_daily if d in market_daily[mkt])
            return {
                'alpha_daily': round(alpha, 4),
                'alpha_annualized_pct': round(alpha * 252, 2),
                'beta': round(beta, 3),
                'sharpe_annualized': round(sharpe, 3),
                'max_drawdown_pct': round(max_dd, 2),
                'cum_return_pct': round(cum_s, 2),
                'benchmark_cum_pct': round(cum_b, 2),
                'alpha_absolute_pct': round(cum_s - cum_b, 2),
            }

        alpha_beta = {}
        for mkt, bench_cum in [('B3', ibov_cum), ('NYSE', spy_cum)]:
            if mkt in market_daily:
                stats = _compute_stats(market_daily[mkt], bench_cum, sorted_dates)
                if stats:
                    n_mkt = sum(market_daily[mkt][d]['n'] for d in sorted_dates if d in market_daily[mkt])
                    w_mkt = sum(market_daily[mkt][d]['w'] for d in sorted_dates if d in market_daily[mkt])
                    stats['trades'] = n_mkt
                    stats['wins'] = w_mkt
                    stats['win_rate_pct'] = round(w_mkt / n_mkt * 100, 2) if n_mkt > 0 else 0
                    stats['benchmark'] = 'IBOV' if mkt == 'B3' else 'SPY'
                    alpha_beta[mkt] = stats

        # Crypto (sem benchmark padrão, apenas métricas absolutas)
        if 'CRYPTO' in market_daily:
            crypto_rets = []
            for d in sorted_dates:
                mdata = market_daily['CRYPTO'].get(d, {'pnl': 0, 'cap': 0})
                crypto_rets.append((mdata['pnl'] / mdata['cap'] * 100) if mdata['cap'] > 0 else 0)
            n = len(crypto_rets)
            if n > 1:
                mean = sum(crypto_rets) / n
                std = (sum((r - mean) ** 2 for r in crypto_rets) / n) ** 0.5
                n_crypto = sum(market_daily['CRYPTO'][d]['n'] for d in sorted_dates if d in market_daily['CRYPTO'])
                w_crypto = sum(market_daily['CRYPTO'][d]['w'] for d in sorted_dates if d in market_daily['CRYPTO'])
                alpha_beta['CRYPTO'] = {
                    'cum_return_pct': round(sum(crypto_rets), 2),
                    'sharpe_annualized': round((mean / std * (252 ** 0.5)) if std > 0 else 0, 3),
                    'trades': n_crypto,
                    'wins': w_crypto,
                    'win_rate_pct': round(w_crypto / n_crypto * 100, 2) if n_crypto > 0 else 0,
                    'benchmark': 'none',
                }

        summary = {
            'period_days': days,
            'market_filter': market_filter,
            'start_date': sorted_dates[0],
            'end_date': sorted_dates[-1],
            'total_trades': sum(daily_trades.values()),
            'total_wins': sum(daily_wins.values()),
            'win_rate_pct': round(sum(daily_wins.values()) / sum(daily_trades.values()) * 100, 2) if sum(daily_trades.values()) > 0 else 0,
            'total_pnl': round(sum(daily_pnl.values()), 2),
            'system_cum_return_pct': round(cum_system, 4),
            'ibov_cum_return_pct': ibov_cum[-1] if ibov_cum else None,
            'spy_cum_return_pct': spy_cum[-1] if spy_cum else None,
            'alpha_vs_ibov_pct': round(cum_system - (ibov_cum[-1] if ibov_cum else 0), 2),
            'alpha_vs_spy_pct': round(cum_system - (spy_cum[-1] if spy_cum else 0), 2),
            'trading_days': len(sorted_dates),
        }

        cursor.close(); conn.close()
        return jsonify({
            'status': 'success',
            'summary': summary,
            'series': series,
            'alpha_beta': alpha_beta,
        }), 200
    except Exception as e:
        import traceback as _tb
        log.error(f'/performance/benchmark: {e}\n{_tb.format_exc()}')
        return jsonify({'error': str(e)}), 500


@app.route('/data/quality')
@require_auth
def data_quality_v1022():
    """[v10.22] Market data quality status."""
    return jsonify(data_validator.get_data_quality_status())

# ── [v10.23] Enhanced endpoints ──────────────────────────────────────

@app.route('/stats/scorecard')
@require_auth
def stats_scorecard_v1023():
    """[v10.23] Per-strategy scorecard with traffic light."""
    try:
        return jsonify(perf_stats.get_strategy_scorecard())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/stats/promotion/enhanced')
@require_auth
def stats_promotion_enhanced_v1023():
    """[v10.23] Enhanced promotion criteria with per-strategy + regime gates."""
    try:
        return jsonify(perf_stats.get_enhanced_promotion_criteria())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/ops/metrics')
@require_auth
def ops_metrics_v1023():
    """[v10.23] Operational metrics — memory, drift, workers, alerts."""
    try:
        ops_metrics.record_memory()
        return jsonify(ops_metrics.get_status())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/ops/audit')
@require_auth
def ops_daily_audit_v1023():
    """[v10.23] Full daily audit report for soak testing."""
    try:
        ops_metrics.record_memory()
        return jsonify(ops_metrics.generate_daily_audit())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/ops/drift')
@require_auth
def ops_drift_report_v1023():
    """[v10.23] Reconciliation drift history with progressive alerts."""
    try:
        return jsonify(ops_metrics.get_drift_report())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/ops/alerts')
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

# ── [v10.52] Proxy endpoints para o Egreja Brain ─────────────────
# Frontend nao chama o brain direto (evita expor BRAIN_API_KEY no
# browser). Proxy roda aqui com a key server-side.
_BRAIN_URL = os.environ.get('BRAIN_URL', '').rstrip('/')
_BRAIN_API_KEY = os.environ.get('BRAIN_API_KEY', '')

def _brain_request(method: str, path: str, **kwargs):
    """Chama o brain com auth, retorna (status_code, json_body)."""
    if not _BRAIN_URL or not _BRAIN_API_KEY:
        return 503, {'error': 'brain not configured (BRAIN_URL/BRAIN_API_KEY)'}
    url = f'{_BRAIN_URL}{path}'
    headers = kwargs.pop('headers', {}) or {}
    headers['X-API-Key'] = _BRAIN_API_KEY
    try:
        r = requests.request(method, url, headers=headers, timeout=15, **kwargs)
        try: body = r.json()
        except Exception: body = {'raw': r.text[:500]}
        return r.status_code, body
    except Exception as e:
        return 502, {'error': f'{type(e).__name__}: {e}'}


@app.route('/api/brain/proposals', methods=['GET'])
def brain_proposals_list():
    """Lista propostas do Nightly Analyst (passthrough do brain)."""
    status_arg = request.args.get('status', 'pending')
    limit = request.args.get('limit', '50')
    code, body = _brain_request('GET', f'/llm/proposals?status={status_arg}&limit={limit}')
    return jsonify(body), code


@app.route('/api/brain/proposals/<int:pid>/approve', methods=['POST'])
@require_auth
def brain_proposal_approve(pid):
    data = request.get_json(silent=True) or {}
    code, body = _brain_request('POST', f'/llm/proposals/{pid}/approve', json=data)
    return jsonify(body), code


@app.route('/api/brain/proposals/<int:pid>/reject', methods=['POST'])
@require_auth
def brain_proposal_reject(pid):
    data = request.get_json(silent=True) or {}
    code, body = _brain_request('POST', f'/llm/proposals/{pid}/reject', json=data)
    return jsonify(body), code


@app.route('/api/brain/nightly/run', methods=['POST'])
@require_auth
def brain_nightly_run():
    """Dispara Nightly Analyst manualmente (passthrough)."""
    code, body = _brain_request('POST', '/llm/nightly/run')
    return jsonify(body), code


@app.route('/api/brain/status', methods=['GET'])
def brain_status():
    """Status dos workers do brain (passthrough)."""
    code, body = _brain_request('GET', '/llm/status')
    return jsonify(body), code


# ═══════════════════════════════════════════════════════════════════
# [v11 PORTFOLIO ACCOUNTING] Fase 1/2 — SHADOW MODE
# ═══════════════════════════════════════════════════════════════════

_v11_engine = None
_v11_shadow_thread = None


def _portfolio_v11_boot():
    """Migration + boot do PortfolioEngine em shadow. Idempotente.

    Não flipa nada no caminho crítico. Apenas materializa o estado v11
    em memória e inicia thread de comparação com variáveis globais.
    """
    global _v11_engine, _v11_shadow_thread

    # 1. Migration idempotente (CREATE IF NOT EXISTS + ALTER IF NOT EXISTS)
    try:
        _run_v11_migration_if_needed()
    except Exception as e:
        log.warning(f'[v11] migration skipped/failed: {e}')
        return

    # 2. Boot engine
    from modules.portfolio import PortfolioEngine, ConfigLoader
    from modules.portfolio.shadow_comparator import ShadowComparator
    engine = PortfolioEngine.instance()
    config_loader = ConfigLoader(db_fn=get_db, ttl_s=60)
    engine.boot(db_fn=get_db, config_loader=config_loader)
    _v11_engine = engine
    log.info('[v11] PortfolioEngine booted in SHADOW mode')

    # 3. Registrar thread de comparação com globals legacy
    def _legacy_state_snapshot():
        # Lê as variáveis globais do módulo
        return {
            'stocks': stocks_capital,
            'crypto': crypto_capital,
            'arbi':   arbi_capital,
        }

    comparator = ShadowComparator(
        db_fn=get_db,
        legacy_state_fn=_legacy_state_snapshot,
        interval_s=int(os.environ.get('V11_SHADOW_INTERVAL_S', '60')),
        alert_threshold_usd=float(os.environ.get('V11_ALERT_THRESHOLD_USD', '10.0')),
        warn_threshold_usd=float(os.environ.get('V11_WARN_THRESHOLD_USD', '0.50')),
        beat_fn=beat,
    )
    _v11_shadow_thread = threading.Thread(
        target=comparator.run_forever,
        daemon=True,
        name='v11-shadow-comparator',
    )
    _v11_shadow_thread.start()
    thread_health['portfolio_shadow_comparator'] = _v11_shadow_thread
    thread_heartbeat['portfolio_shadow_comparator'] = time.time()
    thread_fns['portfolio_shadow_comparator'] = comparator.run_forever
    log.info('[v11] ShadowComparator thread started')


_V11_MIGRATIONS_DIR = 'migrations'
_V11_MIGRATIONS_FILES = [
    '011_portfolio_v11.sql',
    '012_portfolio_v11_configs_real.sql',
    '013_crypto_refinement.sql',
]


def _run_v11_migration_if_needed():
    """Aplica todas as migrations v11 em ordem. Cada uma idempotente.

    Migration 011 cria tabelas/colunas.
    Migration 012 atualiza configs para capital_fraction nas 3 strats.
    """
    import os as _os
    conn = get_db()
    if conn is None:
        return
    try:
        # Roda TODAS as migrations em ordem. Cada UPDATE/INSERT é
        # idempotente ou tolerante a já-existe.
        for migration_file in _V11_MIGRATIONS_FILES:
            migration_path = _os.path.join(
                _os.path.dirname(__file__), _V11_MIGRATIONS_DIR, migration_file
            )
            if not _os.path.exists(migration_path):
                log.error(f'[v11] migration file não encontrado: {migration_path}')
                continue
            _apply_sql_file(conn, migration_path, migration_file)
        return
    finally:
        try: conn.close()
        except Exception: pass


def _apply_sql_file(conn, migration_path: str, label: str) -> None:
    """Aplica um arquivo SQL statement-por-statement com tolerância a
    erros idempotentes (duplicate/already-exists)."""
    import os as _os
    try:
        with open(migration_path) as f:
            sql_text_raw = f.read()

        # Remove comentários (linhas iniciadas com --) ANTES de splitar — caso
        # contrário ';' dentro de texto em português quebra o parser.
        sql_lines = []
        for ln in sql_text_raw.split('\n'):
            stripped = ln.lstrip()
            if stripped.startswith('--'):
                continue
            sql_lines.append(ln)
        sql_text = '\n'.join(sql_lines)
        cur = conn.cursor()
        stmts = [s.strip() for s in sql_text.split(';') if s.strip()]
        applied = 0
        skipped = 0
        for stmt in stmts:
            try:
                cur.execute(stmt)
                applied += 1
            except Exception as se:
                msg = str(se).lower()
                if any(k in msg for k in ('duplicate', 'already exists',
                                           'check that column', 'check that key')):
                    skipped += 1
                    continue
                if 'if not exists' in stmt.lower():
                    stmt_fallback = stmt.replace('IF NOT EXISTS ', '').replace('if not exists ', '')
                    try:
                        cur.execute(stmt_fallback)
                        applied += 1
                        continue
                    except Exception as se2:
                        msg2 = str(se2).lower()
                        if any(k in msg2 for k in ('duplicate', 'already exists',
                                                    'check that column', 'check that key')):
                            skipped += 1
                            continue
                        log.warning(f'[v11] {label} fallback falhou: {se2}')
                        continue
                log.warning(f'[v11] {label} stmt falhou: {str(se)[:200]}')
        conn.commit()
        cur.close()
        log.info(f'[v11] {label}: {applied} aplicados, {skipped} idempotentes')
    except Exception as e:
        log.error(f'[v11] {label} erro geral: {e}')


# ── Endpoints read-only (shadow, não exigem auth pra facilitar dash) ──

@app.route('/portfolio/state', methods=['GET'])
def portfolio_v11_state():
    """Estado canônico de todas as 3 strategies (shadow mode)."""
    if _v11_engine is None or not _v11_engine.booted:
        return jsonify({'error': 'engine not booted', 'v11_active': False}), 503
    out = {}
    for strat in ('stocks', 'crypto', 'arbi'):
        try:
            s = _v11_engine.get_state(strat)
            out[strat] = s.to_dict()
        except Exception as e:
            out[strat] = {'error': str(e)}
    return jsonify({
        'v11_active': _v11_engine.active,
        'strategies': out,
    })


@app.route('/portfolio/state/<strategy>', methods=['GET'])
def portfolio_v11_state_one(strategy):
    if _v11_engine is None or not _v11_engine.booted:
        return jsonify({'error': 'engine not booted'}), 503
    if strategy not in ('stocks', 'crypto', 'arbi'):
        return jsonify({'error': 'strategy inválida'}), 400
    try:
        s = _v11_engine.get_state(strategy)
        return jsonify(s.to_dict())
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/portfolio/integrity', methods=['GET'])
def portfolio_v11_integrity():
    """Compara replay(ledger) vs canonical em memória."""
    if _v11_engine is None or not _v11_engine.booted:
        return jsonify({'error': 'engine not booted'}), 503
    return jsonify(_v11_engine.integrity_check())


@app.route('/portfolio/rebuild/<strategy>', methods=['POST'])
@require_auth
def portfolio_v11_rebuild(strategy):
    """Força rebuild do snapshot de uma strategy a partir do ledger."""
    if _v11_engine is None:
        return jsonify({'error': 'engine not booted'}), 503
    try:
        new_state = _v11_engine.rebuild(strategy)
        return jsonify({'rebuilt': True, 'state': new_state.to_dict()})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/portfolio/config/<strategy>', methods=['GET'])
def portfolio_v11_config(strategy):
    if _v11_engine is None:
        return jsonify({'error': 'engine not booted'}), 503
    try:
        cfg = _v11_engine.config_loader.get(strategy)
        return jsonify({
            'strategy': cfg.strategy,
            'initial_capital': float(cfg.initial_capital),
            'risk_per_trade_pct': float(cfg.risk_per_trade_pct),
            'max_gross_exposure_pct': float(cfg.max_gross_exposure_pct),
            'configured_max_positions': cfg.configured_max_positions,
            'min_capital_per_trade': float(cfg.min_capital_per_trade),
            'position_hard_cap': float(cfg.position_hard_cap) if cfg.position_hard_cap else None,
            'sizing_mode': cfg.sizing_mode,
            'drawdown_hard_stop_pct': float(cfg.drawdown_hard_stop_pct),
            'kill_switch_active': cfg.kill_switch_active,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 404


# ═══════════════════════════════════════════════════════════════════
# [v11] Hooks dual-write para integrar com fluxo de trades existente
# ═══════════════════════════════════════════════════════════════════
# Estes hooks sao chamados pelos pontos de open/close trades no daemon
# (ainda nao integrados automaticamente — caller deve chamar
# explicitamente onde fizer sentido).
#
# Modo: shadow-plus (default) = engine recebe eventos em paralelo ao
#       fluxo legacy. Globals legacy continuam fonte autoritativa no
#       caminho critico. Engine acumula estado correto, detectavel via
#       /portfolio/integrity.
#
# Quando PORTFOLIO_ENGINE_ACTIVE=true: engine vira fonte autoritativa
# (mas esta primeira versao mantem dual-write como salvaguarda).

def _v11_enabled() -> bool:
    """Engine ativo = hooks gravam no ledger v11.

    [kill-switch 2026-04-20] Reducer conta RESERVE legacy + TRADE_OPEN_RESERVE
    v11 como eventos separados (dual-count). Desligado via env var ate
    reducer ser corrigido para dedup por trade_id. Default: FALSE (no-op).
    """
    if _v11_engine is None or not _v11_engine.booted:
        return False
    return os.environ.get('V11_DUAL_WRITE', 'false').lower() == 'true'  # kill-switch v11 dual-write


def _v11_on_trade_open(strategy: str, trade_id: str, reserved: float,
                       metadata: dict = None) -> None:
    """Hook: trade abriu — registra TRADE_OPEN_RESERVE no ledger v11.
    Idempotente. Nao afeta globals legacy."""
    if not _v11_enabled():
        return
    try:
        from modules.portfolio import (
            InsufficientCapitalError, DuplicateIdempotencyError
        )
        try:
            _v11_engine.reserve_on_open(
                strategy=strategy, trade_id=str(trade_id),
                reserve_amount=reserved, metadata=metadata or {}
            )
        except DuplicateIdempotencyError:
            pass  # já registrado, OK
        except InsufficientCapitalError as e:
            # Interessante: caminho legacy abriu mas engine diz que
            # nao deveria. Apenas loga — nao bloqueia legacy.
            log.warning(f'[v11-hook] {strategy}/{trade_id} engine rejeitaria: {e}')
    except Exception as e:
        log.error(f'[v11-hook] on_open {strategy}/{trade_id}: {e}')


def _v11_on_trade_close(strategy: str, trade_id: str, reserved: float,
                        realized_pnl: float, fees: float = 0,
                        metadata: dict = None) -> None:
    """Hook: trade fechou — libera reserva + credita PnL + deduz fees.
    Idempotente. Nao afeta globals legacy."""
    if not _v11_enabled():
        return
    try:
        _v11_engine.release_and_realize(
            strategy=strategy, trade_id=str(trade_id),
            reserved_amount=reserved, realized_pnl=realized_pnl,
            fees_total=fees, metadata=metadata or {}
        )
    except Exception as e:
        log.error(f'[v11-hook] on_close {strategy}/{trade_id}: {e}')


# Expose hooks como módulo-level para chamada fora do file
v11_on_trade_open = _v11_on_trade_open
v11_on_trade_close = _v11_on_trade_close


# ═══════════════════════════════════════════════════════════════════
# [/v11]
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
# [v11-ops] Void trade + blacklist persistente
# ═══════════════════════════════════════════════════════════════════

@app.route('/ops/void-trade/<trade_id>', methods=['POST'])
@require_auth
def ops_void_trade(trade_id: str):
    """Reverte uma trade fechada (ex: MATIC com PnL corrompido).

    Abordagem institucional (auditável, não destrutiva):
      1. NÃO deleta a row — mantém trilha original
      2. Marca closed_reason='VOIDED' + status mantém CLOSED
      3. Zera pnl/pnl_pct no trades row
      4. Registra MANUAL_ADJUSTMENT no capital_ledger com valor oposto
         ao pnl original (REVERTE o impacto no capital)
      5. Atualiza variável global correspondente (stocks/crypto/arbi)
      6. Hook v11: também grava MANUAL_ADJUSTMENT no engine se ativo

    Body JSON (opcional):
      { "reason": "descrição livre do motivo" }

    Retorna JSON com resumo do void.
    """
    payload = request.get_json(silent=True) or {}
    reason = (payload.get('reason') or 'void via endpoint')[:200]

    conn = get_db()
    if conn is None:
        return jsonify({'error': 'db unavailable'}), 503

    try:
        cur = conn.cursor(dictionary=True)

        # 1) Localizar trade (trades ou arbi_trades)
        cur.execute("SELECT * FROM trades WHERE id=%s", (trade_id,))
        row = cur.fetchone()
        table = 'trades'
        asset_type = None
        if not row:
            cur.execute("SELECT * FROM arbi_trades WHERE id=%s", (trade_id,))
            row = cur.fetchone()
            table = 'arbi_trades'
        if not row:
            cur.close()
            return jsonify({'error': f'trade {trade_id} não encontrada'}), 404

        if row.get('status') != 'CLOSED':
            cur.close()
            return jsonify({
                'error': f'trade {trade_id} não está CLOSED (status={row.get("status")})',
                'hint': 'void só aplicável em trades já fechadas'
            }), 400

        existing_reason = row.get('close_reason') or row.get('closed_reason') or ''
        if 'VOID' in str(existing_reason).upper():
            cur.close()
            return jsonify({'already_voided': True, 'trade_id': trade_id}), 200

        symbol = row.get('symbol') or row.get('pair_id') or '?'
        orig_pnl = float(row.get('pnl') or 0)
        position_value_field = ('position_size' if table == 'arbi_trades'
                                else 'position_value')
        position_value = float(row.get(position_value_field) or 0)

        # Determinar strategy (para ledger + portfolio engine)
        if table == 'arbi_trades':
            strategy = 'arbi'
        else:
            asset_type = row.get('asset_type') or 'stock'
            strategy = 'crypto' if asset_type == 'crypto' else 'stocks'

        # 2) Atualizar trades row — zera pnl, marca VOIDED
        #    Usar close_reason (trades) ou closed_reason (arbi_trades)
        reason_col = 'close_reason' if table == 'trades' else 'closed_reason'
        # Checar se a coluna existe
        cur2 = conn.cursor()
        cur2.execute(f"SHOW COLUMNS FROM {table} LIKE %s", (reason_col,))
        has_col = cur2.fetchone() is not None
        if not has_col:
            # fallback (apenas stocks antigas podem não ter)
            reason_col = 'close_reason'
        # Detalha motivo vai para o ledger (metadata_json); aqui so marker
        # curto porque close_reason é VARCHAR(20) em trades antigas
        cur2.execute(
            f"""UPDATE {table}
                SET pnl=0, pnl_pct=0,
                    {reason_col}=%s
                WHERE id=%s""",
            ('VOIDED', trade_id)
        )
        cur2.close()

        # 3) Registrar MANUAL_ADJUSTMENT no capital_ledger que REVERTE
        #    o PnL original. Se trade tinha pnl=+55K, adjustment=-55K.
        #    Note: event 'MANUAL_ADJUSTMENT' é amount SIGNED (convenção v11).
        adj_amount = -orig_pnl  # reverter impacto original
        cur3 = conn.cursor()
        cur3.execute(
            """INSERT INTO capital_ledger
               (ts, strategy, event, symbol, amount, balance_after,
                trade_id, idempotency_key, metadata_json, created_by)
               VALUES (NOW(3), %s, 'MANUAL_ADJUSTMENT', %s, %s, NULL,
                       %s, %s, %s, 'void-trade-endpoint')""",
            (strategy, symbol, adj_amount, trade_id,
             f'VOID:{strategy}:{trade_id}',
             json.dumps({
                 'reason': reason,
                 'original_pnl': orig_pnl,
                 'original_position_value': position_value,
                 'voided_at': datetime.utcnow().isoformat(),
             }))
        )
        cur3.close()

        # 4) Atualizar global legacy (stocks_capital / crypto_capital / arbi_capital)
        global stocks_capital, crypto_capital, arbi_capital
        if strategy == 'stocks':
            with state_lock:
                stocks_capital += adj_amount  # adj é negativo se reverter ganho
        elif strategy == 'crypto':
            with state_lock:
                crypto_capital += adj_amount
        elif strategy == 'arbi':
            with state_lock:
                arbi_capital += adj_amount

        # 5) Hook v11 — se engine ativo, sincronizar mirror
        try:
            if _v11_engine is not None and _v11_engine.booted:
                _v11_engine.apply_event(
                    strategy=strategy,
                    event_type='MANUAL_ADJUSTMENT',
                    amount=adj_amount,
                    trade_id=trade_id,
                    symbol=symbol,
                    metadata={'reason': reason, 'void_of_trade': trade_id},
                    idempotency_key=f'VOID:{strategy}:{trade_id}',
                )
        except Exception as ve:
            log.warning(f'[void-trade] v11 sync falhou (legacy OK): {ve}')

        conn.commit()
        cur.close()
        log.warning(
            f'[VOID-TRADE] {strategy}/{symbol} id={trade_id}: '
            f'pnl_revertido={orig_pnl:+.2f} adj={adj_amount:+.2f} reason="{reason}"'
        )
        send_whatsapp(
            f'⚠ Trade VOIDED: {strategy}/{symbol} id={trade_id}\n'
            f'P&L revertido: ${orig_pnl:+,.2f}\n'
            f'Motivo: {reason}'
        )

        return jsonify({
            'voided': True,
            'trade_id': trade_id,
            'strategy': strategy,
            'symbol': symbol,
            'original_pnl': orig_pnl,
            'adjustment_applied': adj_amount,
            'reason': reason,
        })

    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        log.error(f'void-trade error: {e}')
        import traceback; log.error(traceback.format_exc())
        return jsonify({'error': f'{type(e).__name__}: {e}'}), 500
    finally:
        try: conn.close()
        except Exception: pass


@app.route('/ops/blacklist-symbol', methods=['POST'])
@require_auth
def ops_blacklist_symbol():
    """Adiciona símbolo ao symbol_blocked_persistent (bloqueio permanente).

    Body JSON: { "symbol": "MATICUSDT", "reason": "motivo" }
    Idempotente (INSERT IGNORE).
    """
    payload = request.get_json(silent=True) or {}
    symbol = (payload.get('symbol') or '').upper().strip()
    reason = (payload.get('reason') or 'manual blacklist')[:200]
    if not symbol:
        return jsonify({'error': 'symbol obrigatório no body JSON'}), 400

    conn = get_db()
    if conn is None:
        return jsonify({'error': 'db unavailable'}), 503
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT IGNORE INTO symbol_blocked_persistent
               (symbol, reason) VALUES (%s, %s)""",
            (symbol, reason)
        )
        affected = cur.rowcount
        conn.commit()
        cur.close()
        log.warning(f'[BLACKLIST] {symbol} bloqueado permanentemente: {reason}')
        return jsonify({
            'symbol': symbol,
            'blacklisted': True,
            'reason': reason,
            'was_new': bool(affected),
        })
    except Exception as e:
        return jsonify({'error': f'{type(e).__name__}: {e}'}), 500
    finally:
        try: conn.close()
        except Exception: pass


@app.route('/ops/blacklist-symbol/<symbol>', methods=['DELETE'])
@require_auth
def ops_unblacklist_symbol(symbol: str):
    """Remove símbolo da blacklist persistente."""
    conn = get_db()
    if conn is None:
        return jsonify({'error': 'db unavailable'}), 503
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM symbol_blocked_persistent WHERE symbol=%s",
                    (symbol.upper(),))
        affected = cur.rowcount
        conn.commit()
        cur.close()
        return jsonify({'symbol': symbol.upper(), 'removed': bool(affected)})
    finally:
        try: conn.close()
        except Exception: pass


@app.route('/ops/rehydrate-ledger', methods=['POST'])
@require_auth
def ops_rehydrate_ledger():
    """[v10.52] Rehidrata capital_ledger retroativamente a partir das tabelas
    de trades. Resolve o problema onde ledger foi introduzido em v10.20 e
    trades anteriores nao foram instrumentados — replay divergia da memoria.

    Query params:
      commit=1       : commita; sem isso roda em dry-run (default)
      strategy=X     : stocks|crypto|arbi|all (default all)

    Algoritmo por strategy:
      1. Ler trades ordenados por opened_at
      2. BASELINE no timestamp do primeiro trade com amount=initial
      3. Para cada trade OPEN: RESERVE em opened_at
      4. Para cada trade CLOSED: RESERVE em opened_at + RELEASE + PNL_CREDIT em closed_at
      5. Validar: balance replay deve bater com memory_capital (tolerancia 0.01%)
      6. Se commit=1 e validacao passa: DELETE ledger rows dessa strategy + INSERT eventos novos
      7. Limpa _capital_ledger em memoria (eventos runtime pos-deploy) para nao duplicar
    """
    commit_mode = request.args.get('commit', '0') == '1'
    strategy_filter = request.args.get('strategy', 'all').lower()

    if strategy_filter not in ('all', 'stocks', 'crypto', 'arbi'):
        return jsonify({'error': 'strategy deve ser all|stocks|crypto|arbi'}), 400

    conn = get_db()
    if not conn:
        return jsonify({'error': 'db unavailable'}), 503

    strategies = []
    if strategy_filter in ('all', 'stocks'):
        strategies.append(('stocks', INITIAL_CAPITAL_STOCKS, stocks_capital))
    if strategy_filter in ('all', 'crypto'):
        strategies.append(('crypto', INITIAL_CAPITAL_CRYPTO, crypto_capital))
    if strategy_filter in ('all', 'arbi'):
        strategies.append(('arbi', ARBI_CAPITAL, arbi_capital))

    results = {}
    with state_lock:
        for strat_name, initial, memory_cap in strategies:
            try:
                results[strat_name] = _rehydrate_ledger_one(
                    conn, strat_name, initial, memory_cap, commit_mode)
            except Exception as e:
                results[strat_name] = {'error': f'{type(e).__name__}: {e}'}
                log.error(f'rehydrate {strat_name}: {e}')
                import traceback; log.error(traceback.format_exc())

    return jsonify({
        'commit_mode': commit_mode,
        'hint': 'Use ?commit=1 para aplicar. Sem isso, apenas simula (dry-run).',
        'strategies': results,
    })


def _rehydrate_ledger_one(conn, strategy: str, initial: float,
                           memory_capital: float, commit_mode: bool) -> dict:
    """Rehidrata ledger de uma strategy. Retorna resumo com validacao."""
    # Determina tabela e coluna de tamanho
    if strategy == 'arbi':
        table = 'arbi_trades'
        size_col = 'position_size'
        asset_filter = ''
    elif strategy == 'stocks':
        table = 'trades'
        size_col = 'position_value'
        asset_filter = "WHERE asset_type='stock'"
    elif strategy == 'crypto':
        table = 'trades'
        size_col = 'position_value'
        asset_filter = "WHERE asset_type='crypto'"
    else:
        return {'error': f'strategy invalida: {strategy}'}

    cur = conn.cursor(dictionary=True)
    cur.execute(
        f"SELECT id, status, opened_at, closed_at, {size_col} AS amt, pnl "
        f"FROM {table} {asset_filter} "
        f"ORDER BY opened_at ASC, id ASC"
    )
    trades = cur.fetchall()
    cur.close()

    if not trades:
        return {'strategy': strategy, 'trades': 0, 'events_generated': 0,
                'message': 'sem trades — ledger fica vazio'}

    # Construir eventos
    events = []
    balance = float(initial)
    first_ts = trades[0]['opened_at']

    # BASELINE (idempotencia: trade_id='BASELINE', unique constraint pega)
    events.append({
        'ts': first_ts, 'event': 'BASELINE', 'symbol': 'SYSTEM',
        'amount': balance, 'balance_after': balance, 'trade_id': 'BASELINE'
    })

    for t in trades:
        amt = float(t.get('amt') or 0)
        pnl = float(t.get('pnl') or 0)
        tid = str(t.get('id', ''))
        opened_at = t.get('opened_at')
        closed_at = t.get('closed_at')
        status = (t.get('status') or '').upper()

        # RESERVE na abertura
        balance -= amt
        events.append({
            'ts': opened_at, 'event': 'RESERVE', 'symbol': tid[:20],
            'amount': amt, 'balance_after': balance, 'trade_id': tid
        })

        # Se fechado: RELEASE + PNL_CREDIT
        if status == 'CLOSED' and closed_at is not None:
            balance += amt
            events.append({
                'ts': closed_at, 'event': 'RELEASE', 'symbol': tid[:20],
                'amount': amt, 'balance_after': balance, 'trade_id': tid
            })
            if pnl != 0:
                balance += pnl
                events.append({
                    'ts': closed_at, 'event': 'PNL_CREDIT', 'symbol': tid[:20],
                    'amount': pnl, 'balance_after': balance, 'trade_id': tid
                })

    # Validacao: balance deve bater com memory_capital (tolerancia absoluta)
    delta = memory_capital - balance
    delta_pct = abs(delta) / max(abs(initial), 1) * 100
    ok = abs(delta) < 1.0  # 1 USD de tolerancia (arredondamento)

    summary = {
        'strategy': strategy,
        'initial': round(float(initial), 2),
        'trades_scanned': len(trades),
        'events_generated': len(events),
        'replayed_balance': round(balance, 2),
        'memory_capital': round(float(memory_capital), 2),
        'delta': round(delta, 2),
        'delta_pct': round(delta_pct, 4),
        'ok': ok,
    }

    if not commit_mode:
        summary['action'] = 'DRY_RUN'
        return summary

    if not ok:
        summary['action'] = 'ABORTED'
        summary['reason'] = (
            f'replay balance ({balance:.2f}) nao bate com memory '
            f'({memory_capital:.2f}); delta={delta:.2f}. '
            f'Nao foi commitado — capital em memoria pode ter drift real.'
        )
        return summary

    # Commit: DELETE + INSERT batch
    cur2 = conn.cursor()
    try:
        cur2.execute("DELETE FROM capital_ledger WHERE strategy=%s", (strategy,))
        deleted = cur2.rowcount
        for e in events:
            cur2.execute(
                """INSERT INTO capital_ledger
                   (ts, strategy, event, symbol, amount, balance_after, trade_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (e['ts'], strategy, e['event'], e['symbol'],
                 e['amount'], e['balance_after'], e['trade_id'])
            )
        conn.commit()
        # Limpar memoria do strategy — proximo replay vai do DB
        with _ledger_lock:
            _capital_ledger[:] = [ev for ev in _capital_ledger
                                   if ev.get('strategy') != strategy]
        summary['action'] = 'COMMITTED'
        summary['events_deleted'] = deleted
    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        summary['action'] = 'FAILED'
        summary['error'] = f'{type(e).__name__}: {e}'
    finally:
        cur2.close()

    return summary


@app.route('/kill-switch/safe-resume', methods=['POST'])
@require_auth
def kill_switch_safe_resume_v1023():
    """[v10.23] Safe resume with pre-checks (live mode)."""
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

@app.route('/broker/execution-profile')
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
@app.route('/')
def index():
    """Serve main web dashboard. API info moved to /api/info."""
    return send_from_directory('static', 'index.html')

@app.route('/ticker-tape.js')
def ticker_tape_js():
    """Serve ticker tape JS (loaded by index.html)."""
    resp = send_from_directory('static', 'ticker-tape.js')
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp

@app.route('/api/info')
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

@app.route('/api/modules-debug')
def modules_debug():
    """Debug: check if blueprints loaded successfully."""
    bp_names = [bp.name for bp in app.iter_blueprints()]
    return jsonify({
        'registered_blueprints': bp_names,
        'long_horizon_loaded': 'long_horizon' in bp_names,
        'brain_loaded': 'brain' in bp_names or 'unified_brain' in bp_names,
        'lh_error': _lh_load_error,
        'brain_error': _brain_load_error,
    })

@app.route('/api/fx-rates')
def fx_rates_endpoint():
    """[v2] Public endpoint: cotações de câmbio para exibição no header do dashboard."""
    # fx_rates é populado por fetch_fx_rates() (arbi_scan_loop, a cada ~6min)
    # Valores: USDBRL (BRL por USD), EURUSD (USD por EUR), GBPUSD, HKDUSD, CADUSD
    out = dict(fx_rates) if isinstance(fx_rates, dict) else {}
    # Derivar EURBRL para conveniência do frontend (EUR em BRL)
    if out.get('EURUSD') and out.get('USDBRL'):
        out['EURBRL'] = round(out['EURUSD'] * out['USDBRL'], 4)
    if out.get('GBPUSD') and out.get('USDBRL'):
        out['GBPBRL'] = round(out['GBPUSD'] * out['USDBRL'], 4)
    return jsonify({'fx': out, 'ts': datetime.utcnow().isoformat()})


@app.route('/api/ticker-tape')
def ticker_tape():
    """Public endpoint: all stock + crypto prices for real-time ticker tape display."""
    items = []
    # Stocks
    for sym, data in dict(stock_prices).items():
        if not isinstance(data, dict):
            continue
        ticker = sym.replace('.SA', '')
        items.append({
            't': ticker,
            'p': round(data.get('price', 0), 2),
            'c': round(data.get('change_pct', 0), 2),
            'm': data.get('market', 'B3'),
            'cur': 'BRL' if data.get('market') == 'B3' else 'USD',
        })
    # Crypto
    for sym, price in dict(crypto_prices).items():
        if not isinstance(price, (int, float)):
            continue
        ticker = sym.replace('USDT', '')
        chg = crypto_momentum.get(sym, 0)
        items.append({
            't': ticker,
            'p': round(price, 2),
            'c': round(chg, 2) if isinstance(chg, (int, float)) else 0,
            'm': 'CRYPTO',
            'cur': 'USD',
        })
    # Sort: B3 first, then US, then Crypto — alphabetically within each
    order = {'B3': 0, 'NYSE': 1, 'CRYPTO': 2}
    items.sort(key=lambda x: (order.get(x['m'], 9), x['t']))
    return jsonify({'items': items, 'count': len(items), 'ts': datetime.utcnow().isoformat()})

@app.route('/degraded')
def degraded_route():
    """[V9-3][V91-5] Estado degradado do sistema — público."""
    return jsonify({
        **_read_degraded(),
        'learning_degraded':   LEARNING_DEGRADED,   # [L-10]
        'learning_errors':     learning_errors,
        'queue_warn_threshold': URGENT_QUEUE_WARN,
        'queue_crit_threshold': URGENT_QUEUE_CRIT,
        'timestamp': datetime.utcnow().isoformat(),
    })

@app.route('/debug')
def debug():
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

@app.route('/signals')
def signals():
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
        # [hotfix] state_lock com timeout 2s — se travado, segue sem trade_open flags
        open_stock_syms = set()
        open_crypto_syms = set()
        sp_snap = {}
        if state_lock.acquire(timeout=2):
            try:
                open_stock_syms  = {t['symbol'] for t in stocks_open}
                open_crypto_syms = {t['symbol'] for t in crypto_open}
                sp_snap          = dict(stock_prices)
            finally: state_lock.release()
        else:
            log.warning('[/signals] state_lock timeout — servindo sem trade_open flags')
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
            mkt_type = 'B3' if re.match(r'^[A-Z]{4}[0-9]+$', sym) else 'NYSE'  # [adaptive-v1] pattern match
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
                klines_data = _get_cached_candles(kline_cache_key, ttl_min=5) or {}  # [v10.49] 60→5min
                # [v10.47] SISTEMA APENAS V3
                if klines_data and len(klines_data.get('closes', [])) >= 30:
                    try:
                        from modules.score_engine_v2 import compute_score_v3 as _csv3e
                        _re = _csv3e(klines_data['closes'], klines_data.get('highs', []),
                                    klines_data.get('lows', []), klines_data.get('volumes', []))
                        score = _re['score']
                    except Exception:
                        # v3 falhou — usar neutro (signal MANTER)
                        score = 50
                else:
                    # fallback se klines ainda não carregadas (startup)
                    score = 50
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
                mkt_type = 'B3' if re.match(r'^[A-Z]{4}[0-9]+$', sym) else 'NYSE'  # [adaptive-v1] pattern match
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

@app.route('/prices/live')
def prices_live():
    # [hotfix] state_lock com timeout 2s
    trades = []
    crypto_snap = {}
    if state_lock.acquire(timeout=2):
        try:
            trades=[{'id':t['id'],'symbol':t['symbol'],
                'current_price':t.get('current_price',t.get('entry_price',0)),
                'pnl':t.get('pnl',0),'pnl_pct':t.get('pnl_pct',0),
                'peak_pnl_pct':t.get('peak_pnl_pct',0),'direction':t.get('direction','LONG')}
                for t in stocks_open+crypto_open]
            crypto_snap={k.replace('USDT',''):v for k,v in crypto_prices.items()}
        finally: state_lock.release()
    else:
        log.warning('[/prices/live] state_lock timeout')
    return jsonify({'timestamp':datetime.utcnow().isoformat(),'trades':trades,'crypto_prices':crypto_snap})

@app.route('/trades/open')
def trades_open():
    # [hotfix] timeout 2s
    data = []
    if state_lock.acquire(timeout=2):
        try: data = stocks_open + crypto_open
        finally: state_lock.release()
    return jsonify({'trades':data,'total':len(data)})

@app.route('/trades/closed')
def trades_closed():
    # [hotfix] timeout 2s
    data = []
    if state_lock.acquire(timeout=2):
        try:
            data=sorted(stocks_closed+crypto_closed,key=lambda x:x.get('closed_at',''),reverse=True)
        finally: state_lock.release()
    return jsonify({'trades':data,'total':len(data)})

@app.route('/trades')
def trades():
    # [hotfix] timeout 2s
    all_t = []
    if state_lock.acquire(timeout=2):
        try: all_t = stocks_open+crypto_open+stocks_closed+crypto_closed
        finally: state_lock.release()
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

@app.route('/stats')
def stats():
    # [v10.11] Stats de closed trades vêm do banco — nunca limitadas por memória
    db_st = _get_db_trade_stats()
    # [hotfix] state_lock com timeout 2s
    s_op = s_val = c_op = c_val = a_op = a_cl = 0
    a_win = 0
    a_d = a_w = a_m = a_y = 0
    sc = cc = ac = 0
    if state_lock.acquire(timeout=2):
        try:
            s_op=sum(t.get('pnl',0) for t in stocks_open)
            s_val=sum(float(t.get('position_value',0))+float(t.get('pnl',0)) for t in stocks_open)
            c_op=sum(t.get('pnl',0) for t in crypto_open)
            c_val=sum(float(t.get('position_value',0))+float(t.get('pnl',0)) for t in crypto_open)
            a_op=sum(t.get('pnl',0) for t in arbi_open); a_cl=sum(t.get('pnl',0) for t in arbi_closed)
            a_win=sum(1 for t in arbi_closed if t.get('pnl',0)>0)
            a_d=calc_period_pnl(list(arbi_closed),1); a_w=calc_period_pnl(list(arbi_closed),7)
            a_m=calc_period_pnl(list(arbi_closed),30); a_y=calc_period_pnl(list(arbi_closed),365)
            sc=stocks_capital; cc=crypto_capital; ac=arbi_capital
        finally: state_lock.release()
    else:
        log.warning('[/stats] state_lock timeout — servindo dados parciais')
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

@app.route('/audit')
def audit_route():
    # [V9-4] cached_recent_only: true — deixa explícito que é cache parcial (últimos 200 do DB + runtime)
    with audit_lock: data=list(reversed(audit_log))[:100]
    return jsonify({'events':data,'total':len(audit_log),
        'cached_recent_only': True,
        'note': 'In-memory cache (last ~200 from DB + runtime). Full history in audit_events table.'})

@app.route('/risk')
def risk_status():
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

@app.route('/risk/reset_kill_switch', methods=['POST'])
def reset_kill_switch():
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

@app.route('/trades/correct', methods=['POST'])
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

@app.route('/trades/void', methods=['POST'])
@require_auth
def void_trade():
    """[v10.9] Anular trades fechadas com erro — remove da memória e marca VOID no banco.
    Devolve o capital como se a trade nunca tivesse existido (pnl=0).
    Uso: POST /trades/void  body: {"trade_ids": ["STK-xxx","STK-yyy"], "reason":"loop_error"}
    """
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

@app.route('/debug/drawdown')
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


@app.route('/db/audit')
def db_audit():
    """[v10.11] Auditoria direta do banco — conta TODAS as trades sem limite de memória."""
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


@app.route('/performance/stocks')
def performance_stocks():
    """[v10.11] Dados detalhados de performance histórica de stocks."""
    try:
        conn = get_db()
        if not conn: return jsonify({'error':'db unavailable'}), 500
        cursor = conn.cursor(dictionary=True)
        # Diário
        cursor.execute("""
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

@app.route('/performance/crypto')
def performance_crypto():
    """[v10.11] Dados detalhados de performance histórica de crypto."""
    try:
        conn = get_db()
        if not conn: return jsonify({'error':'db unavailable'}), 500
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
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

@app.route('/risk/reset_arbi_kill_switch', methods=['POST'])
def reset_arbi_kill_switch():
    global ARBI_KILL_SWITCH
    data=request.get_json() or {}
    if data.get('confirm')!='RESET': return jsonify({'error':'Send {"confirm":"RESET"}'}),400
    ARBI_KILL_SWITCH=False; audit('ARBI_KILL_SWITCH_RESET',{'by':'manual_api'})
    return jsonify({'ok':True,'arbi_kill_switch':False})

@app.route('/settings', methods=['GET','POST'])
def settings_endpoint():
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

@app.route('/alerts/test')
def alerts_test():
    ok=_send_whatsapp_direct(f"Egreja AI v10.7.0 test {datetime.now().strftime('%d/%m %H:%M')}")
    return jsonify({'sent':ok,'enabled':ALERTS_ENABLED})

@app.route('/arbitrage/learning')
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

@app.route('/admin/arbi-kill-switch/reset', methods=['POST'])
@require_auth
def arbi_kill_switch_reset():
    """[v10.14] Reseta o ARBI_KILL_SWITCH manualmente."""
    global ARBI_KILL_SWITCH
    ARBI_KILL_SWITCH = False
    log.info('[ADMIN] ARBI_KILL_SWITCH resetado manualmente')
    return jsonify({'ok': True, 'arbi_kill_switch': ARBI_KILL_SWITCH})

@app.route('/arbitrage/spreads')
def arbi_spreads_route():
    with state_lock: spreads=list(arbi_spreads.values())
    spreads.sort(key=lambda x:x['abs_spread'],reverse=True)
    return jsonify({'spreads':spreads,'opportunities':[s for s in spreads if s['opportunity']],
        'total_pairs':len(ARBI_PAIRS),'monitored':len(spreads),'fx_rates':fx_rates,
        'arbi_kill_switch':ARBI_KILL_SWITCH,'updated_at':datetime.utcnow().isoformat()})

@app.route('/arbitrage/force-close', methods=['POST'])
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

@app.route('/arbitrage/purge', methods=['POST'])
@require_auth
def arbitrage_purge():
    """[v10.9] Deletar/corrigir trade arbi problemática do banco e memória."""
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

@app.route('/arbitrage/fix-trade', methods=['POST'])
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

@app.route('/arbitrage/trades')
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

@app.route('/orders')
def orders_route():
    limit=min(int(request.args.get('limit',50)),500)
    status=request.args.get('status','')
    with orders_lock: data=list(reversed(orders_log))
    filtered=[o for o in data if not status or o.get('status')==status]
    # [V9-4] cached_recent_only: deixa explícito que é cache parcial
    return jsonify({'orders':filtered[:limit],'total':len(orders_log),
        'cached_recent_only': True,
        'note': 'In-memory cache (last ~500 from DB + runtime). Full history in orders table.'})

@app.route('/portfolio/snapshots')
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

@app.route('/data/quality')
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

@app.route('/learning/status')
@require_auth
def learning_status():
    """[L][FIX-6] Status geral do Learning Engine com métricas de calibração."""
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

@app.route('/learning/arbi')
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

@app.route('/learning/composite')
def learning_composite():
    """[v10.13] Retorna padrões compostos descobertos automaticamente."""
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

@app.route('/learning/patterns')
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

@app.route('/learning/factors')
@require_auth
def learning_factors():
    """[L-4] Lista fatores com melhor e pior performance histórica."""
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

@app.route('/learning/insights')
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

@app.route('/signals/enriched')
@require_auth
def signals_enriched():
    """[L-5/L-6] Sinais enriquecidos com learning_confidence e insight."""
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

@app.route('/shadow/status')
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
PUBLIC_ROUTES.add('/learning/status')

# ═══════════════════════════════════════════════════════════════
# [SYNC] NETWORK INTELLIGENCE — TROCA ENTRE SISTEMAS EGREJA
# ═══════════════════════════════════════════════════════════════

SYNC_VERSION = "1.0"
SYNC_PEER_URL = os.environ.get('SYNC_PEER_URL', 'https://manus.up.railway.app')  # URL do sistema Manus

@app.route('/sync/export')
def sync_export():
    """Exporta inteligência aprendida para troca entre sistemas Egreja. [PUBLIC — sem auth]"""
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


@app.route('/sync/import', methods=['POST'])
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


@app.route('/sync/peer-data')
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


@app.route('/sync/status')
@require_auth
def sync_status():
    """Verifica se o peer está online e retorna resumo de conectividade."""
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

@app.route('/risk/block_symbol', methods=['POST'])
@require_auth
def block_symbol_route():
    """Bloqueia ou desbloqueia um símbolo manualmente.
    POST body: {"symbol": "RAIZ4", "action": "block"|"unblock"}
    """
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

@app.route('/risk/blocked_symbols')
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


@app.route('/trades/purge', methods=['POST'])
@require_auth
def purge_trades():
    """[v10.9] Deletar trades permanentemente do banco e da memória.
    Remove também trades VOID do mesmo símbolo.
    Body: {"symbol": "RAIZ4"} para deletar todas de um símbolo
          {"trade_ids": ["STK-xxx",...]} para deletar IDs específicos
    ATENÇÃO: operação irreversível.
    """
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

@app.route('/reports/daily')
@require_auth
def report_daily():
    return jsonify(_build_report(1, 'DIARIO'))

@app.route('/reports/weekly')
@require_auth
def report_weekly():
    return jsonify(_build_report(7, 'SEMANAL'))

@app.route('/reports/monthly')
@require_auth
def report_monthly():
    return jsonify(_build_report(30, 'MENSAL'))

@app.route('/reports/send/<period>', methods=['POST'])
@require_auth
def report_send(period):
    """Envia relatorio por WhatsApp: POST /reports/send/daily|weekly|monthly"""
    days = {'daily':1,'weekly':7,'monthly':30}.get(period)
    if not days: return jsonify({'error':'period deve ser daily, weekly ou monthly'}), 400
    label = {'daily':'DIARIO','weekly':'SEMANAL','monthly':'MENSAL'}[period]
    rpt = _build_report(days, label)
    _whatsapp_report(rpt)
    return jsonify({'ok': True, 'period': period, 'report': rpt})

def _monthly_picks_worker():
    """[v3.2] Monthly Picks Sleeve — scan mensal + review semanal + discovery.
    Delega para scheduler_hooks.monthly_picks_worker() (modular).

    [v10.52] O worker externo nao chama beat(). Rodamos um thread
    batedor paralelo que sinaliza heartbeat a cada 60s, evitando
    falso-frozen no watchdog. O worker externo em si continua
    gerenciando seu proprio schedule interno.
    """
    import threading as _th

    # Thread batedor: bate heartbeat continuamente enquanto o worker
    # externo roda. Daemon=True para nao segurar shutdown.
    _stop = _th.Event()
    def _heartbeat_pinger():
        while not _stop.is_set():
            try: beat('monthly_picks_worker')
            except Exception: pass
            _stop.wait(60)
    pinger = _th.Thread(target=_heartbeat_pinger,
                        daemon=True,
                        name='mp-pinger')
    pinger.start()

    try:
        from modules.long_horizon.monthly_picks.scheduler_hooks import monthly_picks_worker
        monthly_picks_worker(
            db_fn=get_db,
            log=log,
            brain_lesson_fn=enqueue_brain_lesson,
        )
    except Exception as e:
        log.error(f'[MonthlyPicks] Worker import/start error: {e}')
        import traceback
        log.error(traceback.format_exc())
    finally:
        _stop.set()


def _brain_hourly_reminder():
    """PRINCÍPIO FUNDAMENTAL — Lembrete horário do Unified Brain.
    Roda a cada hora para garantir que os princípios NUNCA são esquecidos."""
    CORE_PRINCIPLES = """
    ╔═══════════════════════════════════════════════════════════════════════╗
    ║           EGREJA UNIFIED BRAIN — PRINCÍPIOS FUNDAMENTAIS            ║
    ╠═══════════════════════════════════════════════════════════════════════╣
    ║                                                                     ║
    ║  1. NUNCA INVENTAR DADOS OU FATOS — toda informação deve ser        ║
    ║     verificável e rastreável até a fonte original.                   ║
    ║                                                                     ║
    ║  2. NUNCA MENTIR — integridade absoluta em todos os outputs.        ║
    ║     Se não sabe, diz que não sabe. Se é incerto, diz a confiança.  ║
    ║                                                                     ║
    ║  3. TODOS OS MÓDULOS CONECTADOS — Arbi, Crypto, Stocks,            ║
    ║     Derivativos e Long Horizon DEVEM alimentar o motor de           ║
    ║     aprendizado continuamente.                                      ║
    ║                                                                     ║
    ║  4. APRENDIZADO CONTÍNUO — cada trade, cada dado, cada padrão      ║
    ║     deve ser absorvido. O sistema fica mais inteligente a cada dia. ║
    ║                                                                     ║
    ║  5. INTELIGÊNCIA CROSS-DOMAIN — ligar TODOS os fatos e fatores     ║
    ║     entre módulos. Correlações, regimes, timing, risco unificado.  ║
    ║                                                                     ║
    ║  6. CORAÇÃO DO SISTEMA — este motor é a parte mais importante.     ║
    ║     Toda decisão passa por aqui. Cada dia mais potente.            ║
    ║                                                                     ║
    ║  7. MÉTRICAS EMERGENTES — criar parâmetros e métricas que se       ║
    ║     formam a partir dos dados, não inventados.                      ║
    ║                                                                     ║
    ╚═══════════════════════════════════════════════════════════════════════╝
    """
    import pytz
    brt = pytz.timezone('America/Sao_Paulo')
    last_hour = -1
    while True:
        # [v10.52] Faltava beat — watchdog considerava worker frozen apos
        # 120s (default HB timeout) mesmo o worker estando vivo, apenas
        # dormindo entre lembretes horarios.
        beat('brain_hourly_reminder')
        try:
            now = datetime.now(brt)
            if now.hour != last_hour:
                last_hour = now.hour
                log.info(f'[BRAIN] ═══ LEMBRETE HORÁRIO ({now.strftime("%H:%M BRT")}) ═══')
                log.info(CORE_PRINCIPLES)
                log.info(f'[BRAIN] Módulos ativos: Arbi | Crypto | Stocks | Derivativos | Long Horizon')
                log.info(f'[BRAIN] Status: Motor de aprendizado ATIVO. Integridade: ABSOLUTA.')
                log.info(f'[BRAIN] Próximo lembrete: {(now.hour+1)%24}:00 BRT')
                log.info(f'[BRAIN] ═══════════════════════════════════════════════════════')
        except Exception as e:
            log.error(f'[BRAIN] hourly reminder error: {e}')
        time.sleep(60)

def _report_scheduler():
    """Scheduler automatico: diario 20h BRT, semanal sextas, mensal ultimo dia.
    [v10.36] Também dispara o snapshot EOD de top opportunities às 17:30 BRT (seg-sex)."""
    import pytz, calendar as _cal
    brt = pytz.timezone('America/Sao_Paulo')
    sent = set()
    while True:
        try:
            beat('report_scheduler')
            now_brt = datetime.now(brt)
            key_d  = now_brt.strftime('%Y-%m-%d')
            hour   = now_brt.hour; minute = now_brt.minute; wd = now_brt.weekday()
            last_d = _cal.monthrange(now_brt.year, now_brt.month)[1]
            # [v10.36] EOD Top Opportunities snapshot — 17:30 BRT, seg-sex
            if wd < 5 and hour == 17 and minute >= 30:
                topk = key_d + '-TOPOPPS'
                if topk not in sent:
                    sent.add(topk)
                    try:
                        _res = snapshot_top_opportunities(get_db, log=log,
                                                         snapshot_date=now_brt.date(),
                                                         top_n=10)
                        log.info(f'[v10.36] top-opps snapshot: {_res}')
                    except Exception as _e:
                        log.error(f'[v10.36] top-opps snapshot error: {_e}')
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

# [v10.28] Fees from modules.fees — reassign with adapter functions for backward compatibility
if _PURE_MODULES_LOADED:
    # Use module exports (already imported above)
    # Module provides: calc_fee, apply_fee_to_trade, get_fees, _binance_rt, BINANCE_VIP_TIER, USE_BNB_DISCOUNT
    # Create wrapper functions that ignore the new optional parameters when called from api_server
    _module_calc_fee_orig = _module_calc_fee
    _module_apply_fee_orig = _module_apply_fee_to_trade
    # Create a static FEES dict for backward compatibility
    FEES = get_fees()  # Computed once at startup

    def calc_fee(position_value: float, market: str, asset_type: str = 'stock') -> float:
        """[v10.28] Wrapper around modules.fees.calc_fee for backward compatibility."""
        return _module_calc_fee_orig(position_value, market, asset_type)

    def apply_fee_to_trade(trade: dict) -> dict:
        """[v10.28] Wrapper around modules.fees.apply_fee_to_trade for backward compatibility."""
        return _module_apply_fee_orig(trade)
else:
    # Fallback: define fees locally if modules not loaded
    # Taxas round-trip (entrada + saída) por mercado
    # [v10.14] ARBI agora via BTG Pactual (não Binance)
    # BTG Day Trade: corretagem ZERO + emolumentos B3 ~0.010% round trip
    # vs Binance 0.200% — economia de $2.261 por trade!
    # [v10.14] Corretagem real por mercado — verificado em março/2026
    # B3 + NYSE + Arbi: BTG Pactual Day Trade (corretagem ZERO, só emolumentos)
    # Crypto: Binance Spot — taxas reais por VIP tier
    #   VIP 0:       0.100% maker + 0.100% taker = 0.200% rt
    #   VIP 0+BNB:   0.075% + 0.075% = 0.150% rt
    #   VIP 3:       0.042% + 0.060% = 0.102% rt   (elegível: vol>$20M/30d)
    #   VIP 3+BNB:   0.0315% + 0.045% = 0.0765% rt ← TAXA REAL com nosso volume
    BINANCE_VIP_TIER   = int(os.environ.get('BINANCE_VIP_TIER', 3))
    USE_BNB_DISCOUNT   = bool(os.environ.get('USE_BNB_DISCOUNT', 'true').lower() == 'true')
    BROKER             = 'BTG'   # B3, NYSE, Arbi via BTG | Crypto via Binance

    # Tabela maker/taker Binance por VIP tier (valores por LADO, sem BNB)
    _BINANCE_FEES = {0:(0.0010,0.0010), 1:(0.0009,0.0010), 2:(0.0008,0.0010),
                     3:(0.00042,0.0006), 4:(0.0002,0.0004), 5:(0.00012,0.0003)}
    def _binance_rt() -> float:
        m,t = _BINANCE_FEES.get(BINANCE_VIP_TIER, (0.001,0.001))
        if USE_BNB_DISCOUNT: m,t = m*0.75, t*0.75
        return round(m+t, 6)   # round trip = maker+taker (compra taker + venda taker)

    FEES = {
        'B3':    0.00030,   # BTG Day Trade: ZERO corretagem + emolumentos B3 (era 0.195% XP)
        'NYSE':  0.00020,   # BTG US: ~0.020% rt spread+SEC (era 0.012% IBKR)
        'CRYPTO':_binance_rt(),  # Binance VIP3+BNB = 0.0765% rt (era 0.200%)
        'ARBI':  0.00010,   # BTG Day Trade: ZERO corretagem + emolumentos ~0.010% rt
    }

    def calc_fee(position_value: float, market: str, asset_type: str = 'stock') -> float:
        """[v10.14] Calcula taxa estimada de corretagem para uma operação round-trip.
        FEES já incorpora BNB discount via _binance_rt() — não precisa de FEES_BNB separado.
        """
        pv = abs(float(position_value or 0))
        if asset_type == 'stock':
            rate = FEES.get(market, FEES['NYSE'])
        elif asset_type == 'crypto':
            rate = FEES['CRYPTO']   # já calculado com VIP tier + BNB por _binance_rt()
        else:                       # arbi — BTG Day Trade: emolumentos B3 ~0.010% rt
            # Já capturado em total_cost_estimated na abertura — não duplicar
            # Retornar só emolumentos mínimos para registro
            rate = FEES['ARBI']  # 0.010%
        return round(pv * rate, 2)

    def apply_fee_to_trade(trade: dict) -> dict:
        """
        [v10.14] Calcula e registra a taxa estimada de corretagem.
        IMPORTANTE: pnl e pnl_pct NÃO são alterados — permanecem como bruto.
        O sistema interno (capital, learning, WR, SL, TP) usa sempre o bruto.
        Campos adicionados para exibição no frontend:
          - trade['pnl_gross']    = cópia do pnl bruto (igual a pnl)
          - trade['fee_estimated'] = taxa calculada
          - trade['pnl_net']      = pnl - fee (só para display)
          - trade['pnl_net_pct']  = pnl_net / position_value × 100
        """
        if trade.get('_fee_applied'):
            return trade
        pv    = float(trade.get('position_value', 0) or trade.get('position_size', 0) or 0)
        mkt   = trade.get('market', 'NYSE')
        atype = trade.get('asset_type', 'stock')
        # Para arbi: usar total_cost_estimated que já tem fee+slippage+fx_cost
        if atype in ('arbitrage','arbi') or mkt == 'ARBI':
            fee = float(trade.get('total_cost_estimated', 0) or 0)
            if fee == 0:  # fallback para trades antigas sem o campo
                fee = calc_fee(pv, mkt, atype)
        else:
            fee = calc_fee(pv, mkt, atype)
        gross = float(trade.get('pnl', 0) or 0)
        net   = round(gross - fee, 2)
        net_pct = round(net / pv * 100, 4) if pv > 0 else 0
        # NUNCA sobrescreve pnl ou pnl_pct — lógica interna usa bruto
        trade['pnl_gross']     = gross   # igual a pnl (bruto)
        trade['fee_estimated'] = fee
        trade['pnl_net']       = net     # líquido = só para display
        trade['pnl_net_pct']   = net_pct
        trade['_fee_applied']  = True
        return trade


@app.route('/admin/db-cleanup', methods=['POST'])
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


# ENTRY POINT
# ═══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    port=int(os.environ.get('PORT',3001))
    log.info(f'━━━ Egreja Investment AI v10.22.0 | {ENV.upper()} | port {port} | single-process ━━━')
    log.info(f'FMP: {"SET" if FMP_API_KEY else "NOT SET"} | Auth: {"ENABLED" if API_SECRET_KEY else "DISABLED (dev)"} | Alerts: {"ON" if ALERTS_ENABLED else "OFF"}')
    validate_settings_on_boot()  # [v10.16]
    load_calibration()  # [v10.18] restaurar calibração persistida
    log.info(f'Stocks ${INITIAL_CAPITAL_STOCKS/1e6:.0f}M | Crypto ${INITIAL_CAPITAL_CRYPTO/1e6:.0f}M | Arbi ${ARBI_CAPITAL/1e3:.0f}K (SEGREGATED)')
    log.info(f'Queue thresholds: WARN={URGENT_QUEUE_WARN} / CRIT={URGENT_QUEUE_CRIT}')

    log.info('Init...')
    # [v10.14] Liberar disco ANTES de init_all_tables — DROP TABLE libera espaço imediatamente
    # (TRUNCATE não libera espaço InnoDB, DROP TABLE sim)
    try:
        _sc = get_db()
        if _sc:
            _cur = _sc.cursor()
            dropped = []
            # DROP tabelas auxiliares não críticas (serão recriadas por init_all_tables)
            for _t in ['signal_events','shadow_decisions','learning_audit','audit_events']:
                try:
                    _cur.execute(f'DROP TABLE IF EXISTS {_t}'); _sc.commit()
                    dropped.append(_t)
                except: pass
            # orders — limpar antigas (manter tabela)
            try:
                _cur.execute("DELETE FROM orders WHERE created_at < DATE_SUB(NOW(), INTERVAL 3 DAY)"); _sc.commit()
            except: pass
            # pattern_stats — limpar entradas fracas
            try:
                _cur.execute("DELETE FROM pattern_stats WHERE total_samples < 2"); _sc.commit()
            except: pass
            _cur.close(); _sc.close()
            log.info(f'Pre-init disk freed (DROP): {dropped}')
    except Exception as _e:
        log.warning(f'Pre-init cleanup error: {_e}')
    init_all_tables()

    # [v3.0] Create unified brain tables (persistent memory — the brain never forgets)
    try:
        _brain_conn = get_db()
        if _brain_conn:
            create_unified_brain_tables(_brain_conn)
            _brain_conn.close()
            log.info('[v3.0] Unified Brain tables created/verified (8 tables — persistent memory)')
    except Exception as _be:
        log.warning(f'[v3.0] Brain tables creation warning: {_be}')

    # [v4.0] Brain Advisor V4 schema + workers (shadow mode padrão)
    try:
        from modules.unified_brain.advisor_shadow import (
            ensure_advisor_schema as _adv_schema,
            start_resolution_worker as _adv_reso_w,
        )
        from modules.unified_brain.advisor_metrics import start_metrics_worker as _adv_met_w
        if _adv_schema(get_db, log):
            log.info('[v4.0] Brain Advisor V4 schema created/verified (3 tables)')
            _adv_reso_w(get_db, log, interval_sec=3600)
            _adv_met_w(get_db, log, interval_sec=3600*6)
            log.info('[v4.0] Brain Advisor workers started (resolution + metrics)')
    except Exception as _av4e:
        log.warning(f'[v4.0] Brain Advisor V4 init warning: {_av4e}')

    # [v10.27] Long Horizon tables (lh_assets, lh_scores, etc.)
    try:
        _lh_conn = get_db()
        if _lh_conn:
            from modules.long_horizon.schema import create_long_horizon_tables
            create_long_horizon_tables(_lh_conn)
            _lh_conn.close()
            log.info('[v10.27] Long Horizon tables created/verified (10 tables)')
    except Exception as _lhe:
        log.warning(f'[v10.27] Long Horizon tables warning: {_lhe}')

    # [v3.2] Monthly Picks tables (8 tables — modular sleeve)
    try:
        _mp_conn = get_db()
        if _mp_conn:
            from modules.long_horizon.monthly_picks.repositories import create_monthly_picks_tables
            create_monthly_picks_tables(_mp_conn)
            _mp_conn.close()
            log.info('[v3.2] Monthly Picks tables created/verified (8 tables — modular sleeve)')
    except Exception as _mpe:
        log.warning(f'[v3.2] Monthly Picks tables warning: {_mpe}')

    fetch_fx_rates()          # [v10.6-P1-4] FX carregado ANTES de stock — ADR usa USDBRL
    fetch_crypto_prices()
    fetch_stock_prices()
    init_watchlist_table()
    init_trades_tables()
    _record_baseline_if_needed()  # [v10.21] registra BASELINE formal para strategies sem histórico no ledger

    # [v11 Portfolio Accounting — Fase 1/2 SHADOW] -----------------------
    # Roda migration idempotente + boota engine. SEM afetar caminho crítico.
    # Flag PORTFOLIO_ENGINE_ACTIVE continua False até Fase 3.
    try:
        _portfolio_v11_boot()
    except Exception as _e:
        log.error(f'[v11] boot falhou (shadow mode, não afeta critical path): {_e}')

    # [v10.22] Initialize institutional modules
    ext_kill_switch.init_table(get_db)
    auth_manager.init_users_table(get_db)
    audit_logger.init_table(get_db) if hasattr(audit_logger, 'init_table') else None
    log.info('[v10.22] Institutional modules initialized: risk, broker, data_validator, auth, stats, kill_switch')

    init_learning_cache()   # [L-3] carrega histórico de aprendizado em memória
    _update_market_regime()
    take_portfolio_snapshot()
    _check_degraded()

    # [v3.0] Initialize brain persistent memory and update evolution score
    try:
        engine = _get_brain_engine()
        if engine:
            engine.update_evolution()
            log.info(f'[v3.0] Brain memory initialized: {len(engine._lessons)} lessons, '
                     f'{len(engine._patterns)} patterns, {len(engine._correlations)} correlations, '
                     f'{len(engine._decisions)} decisions — persistent MySQL')
    except Exception as _be:
        log.warning(f'[v3.0] Brain init warning: {_be}')

    log.info('Init complete.')

    start_background_threads()
    # Single-process: use gunicorn com --workers=1 em produção
    # gunicorn -w 1 -b 0.0.0.0:$PORT api_server:app
    app.run(host='0.0.0.0', port=port, debug=False)
