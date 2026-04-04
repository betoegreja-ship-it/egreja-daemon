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
        etf_basket_scan_loop, skew_arb_scan_loop, interlisted_scan_loop, dividend_arb_scan_loop, vol_arb_scan_loop)
    from modules.derivatives.endpoints import create_strategies_blueprint
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
    log.info(f'[v10.25] Derivatives services initialized: {len([v for v in _deriv_services.values() if v])} active')
except Exception as e:
    log.warning(f'[v10.25] Derivatives services init: {e}')
# ═══ END DERIVATIVES INIT ═══

# [v10.25] Register derivatives strategies blueprint
try:
    _strategies_bp = create_strategies_blueprint(
        db_fn=lambda: get_db() if 'get_db' in dir() else None,
        log=log if 'log' in dir() else logging.getLogger('egreja'),
        provider_mgr=globals().get('_deriv_provider_mgr'),
        services_dict=globals().get('_deriv_services', {}),
    )
    app.register_blueprint(_strategies_bp, url_prefix='/strategies')
    log.info('[v10.25] Derivatives strategies blueprint registered at /strategies/*')
except Exception as e:
    log.warning(f'[v10.25] Strategies blueprint registration: {e}')
CORS(app)

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
VERSION = 'v10.24.5'
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
MAX_POSITIONS_CRYPTO     = int(os.environ.get('MAX_POSITIONS_CRYPTO', 5))  # [v10.24.4] 5 símbolos — usar todo o capital nas 5 moedas
MAX_POSITIONS_NYSE       = int(os.environ.get('MAX_POSITIONS_NYSE', 10))

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
MIN_SCORE_AUTO_CRYPTO    = int(os.environ.get('MIN_SCORE_AUTO_CRYPTO', 55))  # [v10.15] crypto threshold 55 (era 48) — reduz over-trading
DEFAULT_POSITION_SIZE    = float(os.environ.get('DEFAULT_POSITION_SIZE', 100000))

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
MAX_POSITION_SAME_MKT   = int(os.environ.get('MAX_POSITION_SAME_MKT', 10))
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
BLACKLIST_MIN_TRADES    = int(os.environ.get('BLACKLIST_MIN_TRADES', 20))     # mínimo de trades para avaliar
BLACKLIST_MAX_AVG_PNL   = float(os.environ.get('BLACKLIST_MAX_AVG_PNL', -40)) # avg PnL < -$40 = blacklist
BLACKLIST_MAX_WR        = float(os.environ.get('BLACKLIST_MAX_WR', 42))       # WR < 42% = blacklist
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
RECONCILIATION_ALERT_PCT     = float(os.environ.get('RECONCILIATION_ALERT_PCT', 2.0))    # alertar se >2% desvio
# ── [v10.18] Crypto conviction filter ───────────────────────────────────
CRYPTO_MIN_CONVICTION        = float(os.environ.get('CRYPTO_MIN_CONVICTION', 52))        # [v10.24] era 58 — muito restritivo, bloqueava quase tudo em mercado lateral
CRYPTO_MIN_HOLD_MIN          = float(os.environ.get('CRYPTO_MIN_HOLD_MIN', 15))          # hold mínimo (min) para flat exit
LEARNING_ENABLED       = os.environ.get('LEARNING_ENABLED', 'true').lower() != 'false'

CRYPTO_SYMBOLS = [
    # [v10.14] Corte cirúrgico para 5 melhores por P&L real (análise 30/03/2026)
    # REMOVIDOS por P&L negativo acumulado:
    # ADAUSDT  -$5.515 (WR 48%), AVAXUSDT -$3.885 (WR 49%), SOLUSDT -$3.583 (WR 47%)
    # DOGEUSDT -$3.037 (WR 54%!), XRPUSDT -$1.243, DOTUSDT -$5.897, UNIUSDT -$427
    # LTCUSDT -$240, APTUSDT -$297, MATICUSDT -N/A, TRXUSDT -$1.372
    # MANTIDOS (únicos lucrativos + neutros):
    'ETHUSDT',   # +$5.723  WR 55% — melhor de todos
    'ARBUSDT',   # +$2.455  WR 54% — segundo melhor
    'NEARUSDT',  # +$964    WR 55% — terceiro
    'BTCUSDT',   # +$242    WR 53% — quase neutro, referência de mercado
    'BNBUSDT',   # +$191    WR 50% — quase neutro, exchange coin estável
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
            _db_pool = MySQLConnectionPool(
                pool_name='egreja', pool_size=30,
                autocommit=True, connection_timeout=10,
                **pool_cfg)
            log.info('[v10.7] MySQL connection pool inicializado (size=30)')
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

def send_whatsapp(message):
    try:
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
    if request.path in PUBLIC_ROUTES or request.path.startswith('/health') or request.path.startswith('/strategies') or request.path in ('/derivatives', '/api/info'):
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
    try:
        cursor    = conn.cursor()
        event     = entry.get('event', '')
        entity_id = str(entry.get('id') or entry.get('pair') or entry.get('symbol', ''))
        payload   = json.dumps({k:v for k,v in entry.items() if k not in ('event','timestamp')})
        cursor.execute(
            "INSERT INTO audit_events (event_type, entity_type, entity_id, payload_json) "
            "VALUES (%s, %s, %s, %s)",
            (event, event.split('_')[0].lower(), entity_id, payload))
        conn.commit(); cursor.close(); conn.close()
    except Exception as e:
        log.error(f'_db_insert_audit: {e}')

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
        conn.commit(); cursor.close(); conn.close()
    except Exception as e: log.error(f'db_save_order: {e}')

# ═══════════════════════════════════════════════════════════════
# PORTFOLIO SNAPSHOT
# ═══════════════════════════════════════════════════════════════
def take_portfolio_snapshot():
    with state_lock:
        snap = {
            'timestamp':        datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
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
    try:
        cursor=conn.cursor()
        cursor.execute("""INSERT INTO portfolio_snapshots (
            ts,stocks_capital,crypto_capital,arbi_capital,
            stocks_open_pnl,crypto_open_pnl,arbi_open_pnl,total_open_pnl,
            open_positions,arbi_positions,kill_switch,arbi_kill_switch,market_regime)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (snap['timestamp'],snap['stocks_capital'],snap['crypto_capital'],snap['arbi_capital'],
             snap['stocks_open_pnl'],snap['crypto_open_pnl'],snap['arbi_open_pnl'],
             snap['total_open_pnl'],snap['open_positions'],snap['arbi_positions'],
             snap['kill_switch'],snap['arbi_kill_switch'],snap['market_regime']))
        conn.commit(); cursor.close(); conn.close()
    except Exception as e: log.error(f'db_save_snapshot: {e}')

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

# Score por dia da semana para crypto (0=Seg, 6=Dom)
CRYPTO_DOW_SCORE = {
    0: +4,   # Segunda: 53.7% WR
    1: -10,  # Terça:   43.9% WR — pior dia, -$37.9K total
    2: -8,   # Quarta:  40.3% WR
    3: +2,   # Quinta:  48.7% WR — neutro-positivo
    4: -8,   # Sexta:   42.0% WR
    5: -6,   # Sábado:  43.3% WR
    6: +15,  # Domingo: 64.7% WR — melhor dia, +$12.7K total
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
    
    # BTC sentimento (proxy geral de mercado cripto)
    btc_chg = s.get('btc_change_24h', 0.0)
    if btc_chg > 3.0:   adj += 5   # BTC em alta → mercado bullish
    elif btc_chg < -3.0: adj -= 8  # BTC em queda → mercado bearish (penaliza mais)
    
    return max(-20, min(+15, adj))  # cap ±20 pts


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
        'ok': delta_pct < RECONCILIATION_ALERT_PCT,
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
    """
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

def _db_log_learning_audit(event_type: str, entity_id: str, payload: dict):
    conn = get_db()
    if not conn: return
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
                                if any(shadow_features.values()):
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
        conn.commit(); cursor.close(); conn.close()
        log.info('All tables created/verified')
    except Exception as e: log.error(f'init_all_tables: {e}')

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
            signal_id,feature_hash,learning_confidence,insight_summary,learning_version,features_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
             t.get('insight_summary'),t.get('learning_version'),features_json))
        conn.commit(); cursor.close(); conn.close()
    except Exception as e: log.error(f'db_save_trade: {e}')

def _db_save_arbi_trade(trade):
    conn=get_db()
    if not conn: return
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
        conn.commit(); cursor.close(); conn.close()
    except Exception as e: log.error(f'db_save_arbi_trade: {e}')

def _db_save_cooldown(symbol, ts):
    conn=get_db()
    if not conn: return
    try:
        cursor=conn.cursor()
        dt=datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("INSERT INTO symbol_cooldowns (symbol,last_close_at) VALUES (%s,%s) "
                       "ON DUPLICATE KEY UPDATE last_close_at=%s,updated_at=NOW()",(symbol,dt,dt))
        conn.commit(); cursor.close(); conn.close()
    except Exception as e: log.error(f'db_save_cooldown: {e}')

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
    is_b3 = sym.endswith('.SA') or any(sym == s.replace('.SA','') for s in STOCK_SYMBOLS_B3)
    display = sym.replace('.SA', '')

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
    """[v10.4][v10.5-2] Binance klines diárias para ATR e volume médio.
    Usa b[7] (quoteAssetVolume, em USDT) — compatível com vol_quote do allTickers.
    b[5] é volume em moeda base (BTC, ETH…) — não comparável com quoteVolume.
    """
    try:
        r = requests.get(
            'https://api.binance.com/api/v3/klines',
            params={'symbol': symbol, 'interval': '1d', 'limit': period + 2},
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
                    cached_klines = _get_cached_candles(f'klines:{sym}', ttl_min=60)  # [v10.6.3-Fix2]
                    if cached_klines is None:
                        klines = _fetch_binance_klines(sym, 22)
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
# ═══════════════════════════════════════════════════════════════
# [v10.28] EXECUTION — served via modules/execution.py
# monitor_trades, stock_execution_worker, auto_trade_crypto
# ═══════════════════════════════════════════════════════════════
def monitor_trades():
    """[v10.28] Trade monitor — delegates to modules/execution.py"""
    _mod_monitor_trades(_build_execution_ctx(globals()))

def stock_execution_worker():
    """[v10.28] Stock execution — delegates to modules/execution.py"""
    _mod_stock_execution_worker(_build_execution_ctx(globals()))

def auto_trade_crypto():
    """[v10.28] Crypto execution — delegates to modules/execution.py"""
    _mod_auto_trade_crypto(_build_execution_ctx(globals()))

# ═══════════════════════════════════════════════════════════════
# [v10.28] ARBITRAGE — served via modules/arbitrage.py
# ═══════════════════════════════════════════════════════════════
def arbi_scan_loop():
    """[v10.28] Arbi scanner — delegates to modules/arbitrage.py"""
    _mod_arbi_scan_loop(_build_arbitrage_ctx(globals()))

def arbi_monitor_loop():
    """[v10.28] Arbi monitor — delegates to modules/arbitrage.py"""
    _mod_arbi_monitor_loop(_build_arbitrage_ctx(globals()))

def arbi_learning_loop():
    """[v10.28] Arbi learning — delegates to modules/arbitrage.py"""
    _mod_arbi_learning_loop(_build_arbitrage_ctx(globals()))

def calc_spread(sym_a, sym_b, direction='LONG_A'):
    """[v10.28] Spread calculator — delegates to modules/arbitrage.py"""
    return _mod_calc_spread(_build_arbitrage_ctx(globals()), sym_a, sym_b, direction)

def run_arbi_pattern_learning():
    """[v10.28] Arbi pattern learning — delegates to modules/arbitrage.py"""
    _mod_run_arbi_pattern_learning(_build_arbitrage_ctx(globals()))

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
                    # [v10.14-FIX] Verificar limite de threads antes de criar novo
                    import threading as _th
                    active = _th.active_count()
                    if active > 45:  # limite seguro (padrão Python = 50)
                        log.error(f'WATCHDOG: {name} NÃO reiniciada — threads ativos={active} (limite atingido)')
                        send_whatsapp(f'CRITICO: thread starvation! {active} threads ativos. Reiniciar o serviço.')
                        # Marcar heartbeat para não tentar de novo em loop
                        thread_heartbeat[name] = time.time()
                    else:
                        new_t=threading.Thread(target=fn,daemon=True); new_t.start()
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


def start_background_threads():
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
    try:
        cursor=conn.cursor(dictionary=True)
        cursor.execute("SELECT symbol, market, added_at FROM watchlist")
        watchlist_symbols=[{'symbol':r['symbol'],'market':r['market'],
            'addedAt':r['added_at'].isoformat() if r['added_at'] else ''} for r in cursor.fetchall()]
        cursor.close(); conn.close()
        log.info(f'Watchlist: {len(watchlist_symbols)} loaded')
    except Exception as e: log.error(f'Watchlist init: {e}')
# ═══════════════════════════════════════════════════════════════
# [v10.28] API ROUTES — served via Blueprint (modules/api_routes.py)
# 78 routes extracted — see modules/api_routes.py for all route handlers
# ═══════════════════════════════════════════════════════════════
if _PURE_MODULES_LOADED:
    def _build_routes_ctx():
        """Build context dict for API routes Blueprint."""
        return {k: v for k, v in globals().items()}
    _mod_init_routes(_build_routes_ctx())
    app.register_blueprint(_mod_api_bp)
    log.info('[v10.28] API routes: 78 routes registered via Blueprint')
else:
    log.warning('[v10.28] API routes Blueprint not available — some routes will be missing!')
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
    fetch_fx_rates()          # [v10.6-P1-4] FX carregado ANTES de stock — ADR usa USDBRL
    fetch_crypto_prices()
    fetch_stock_prices()
    init_watchlist_table()
    init_trades_tables()
    _record_baseline_if_needed()  # [v10.21] registra BASELINE formal para strategies sem histórico no ledger

    # [v10.22] Initialize institutional modules
    ext_kill_switch.init_table(get_db)
    auth_manager.init_users_table(get_db)
    audit_logger.init_table(get_db) if hasattr(audit_logger, 'init_table') else None
    log.info('[v10.22] Institutional modules initialized: risk, broker, data_validator, auth, stats, kill_switch')

    init_learning_cache()   # [L-3] carrega histórico de aprendizado em memória
    _update_market_regime()
    take_portfolio_snapshot()
    _check_degraded()
    log.info('Init complete.')

    start_background_threads()
    # Single-process: use gunicorn com --workers=1 em produção
    # gunicorn -w 1 -b 0.0.0.0:$PORT api_server:app
    app.run(host='0.0.0.0', port=port, debug=False)
