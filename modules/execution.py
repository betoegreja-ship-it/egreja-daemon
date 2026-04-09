"""
=============================================================================
PHASE 4a: EXECUTION MODULE [v10.26] — Trade Monitoring & Execution Workers
=============================================================================

This module contains the core trading execution logic extracted from api_server.py:
  - monitor_trades()         : Monitors open trades, applies SL/TP/trailing/timeout exits
  - stock_execution_worker() : Main stock signal processing and trade entry loop
  - auto_trade_crypto()      : Main crypto signal processing and trade entry loop

CONTEXT PASSING PATTERN:
All functions accept a `ctx` parameter (dict) containing references to global state
and supporting functions. This allows the module to be executed independently while
maintaining access to the main application state.

Key ctx dict keys (build with build_execution_ctx(globals_dict)):
  - state_lock, stocks_open, crypto_open, stock_prices, crypto_prices
  - stocks_capital, crypto_capital, stocks_closed, crypto_closed
  - market_regime, trading_suspended, _last_trade_opened, processed_signal_ids
  - log, and all referenced helper functions

Author: Claude Agent (Anthropic)
Version: Phase 4a
"""

import time
import threading
import requests
import collections
from datetime import datetime, timedelta

# ── [v10.26] Batch entry limiter ──────────────────────────────
_entry_timestamps = {'stocks': collections.deque(maxlen=200), 'crypto': collections.deque(maxlen=200)}
_entry_lock = threading.Lock()

def _check_batch_limit(asset_type, max_per_min):
    """[v10.26] Returns True if within batch limit, False if throttled."""
    now = time.time()
    with _entry_lock:
        dq = _entry_timestamps[asset_type]
        # Remove entries older than 60s
        while dq and dq[0] < now - 60:
            dq.popleft()
        if len(dq) >= max_per_min:
            return False
        dq.append(now)
        return True

# ── [v10.26] Market reversal detection ────────────────────────
_reversal_cache = {}
_reversal_cache_ts = {}

def detect_market_reversal(ctx, symbol, price_data, asset_type='stock'):
    """
    [v10.26] Multi-indicator reversal detection:
    - RSI exiting overbought/oversold
    - EMA fast/slow crossover
    - MACD histogram divergence
    - Volume spike
    Returns: (is_reversal, direction_hint, signals_count, detail)
      direction_hint: 'BULLISH_REVERSAL' or 'BEARISH_REVERSAL' or None
    """
    global _reversal_cache, _reversal_cache_ts
    cache_key = f"{symbol}:{asset_type}"
    now = time.time()
    # Cache 30s to avoid recomputing every tick
    if cache_key in _reversal_cache and now - _reversal_cache_ts.get(cache_key, 0) < 30:
        return _reversal_cache[cache_key]

    signals_bull = 0
    signals_bear = 0
    detail = []

    rsi = price_data.get('rsi', 50) or 50
    ema9 = price_data.get('ema9', 0) or 0
    ema21 = price_data.get('ema21', 0) or 0
    ema50 = price_data.get('ema50', 0) or 0
    vol_ratio = price_data.get('volume_ratio', 0) or 0
    price_hist = price_data.get('price_history', []) or []

    # 1) RSI reversal
    RSI_OB = ctx.get('REVERSAL_RSI_OB', 72)
    RSI_OS = ctx.get('REVERSAL_RSI_OS', 28)
    if rsi > RSI_OB:
        signals_bear += 1
        detail.append(f'RSI_OB={rsi:.0f}')
    elif rsi < RSI_OS:
        signals_bull += 1
        detail.append(f'RSI_OS={rsi:.0f}')

    # 2) EMA crossover (detect fresh cross using price vs EMAs)
    if ema9 > 0 and ema21 > 0:
        cross_pct = (ema9 - ema21) / ema21 * 100 if ema21 > 0 else 0
        if 0 < cross_pct < 0.3:  # just crossed bullish (within 0.3%)
            signals_bull += 1
            detail.append(f'EMA_BULL_CROSS={cross_pct:.2f}%')
        elif -0.3 < cross_pct < 0:  # just crossed bearish
            signals_bear += 1
            detail.append(f'EMA_BEAR_CROSS={cross_pct:.2f}%')

    # 3) Price momentum divergence (proxy for MACD)
    if len(price_hist) >= 5:
        recent = price_hist[-3:]
        older = price_hist[-5:-3]
        if recent and older:
            recent_avg = sum(recent) / len(recent)
            older_avg = sum(older) / len(older)
            if older_avg > 0:
                mom_change = (recent_avg - older_avg) / older_avg * 100
                if mom_change > 0.5 and rsi < 45:  # price rising but RSI low = bullish divergence
                    signals_bull += 1
                    detail.append(f'MACD_DIV_BULL={mom_change:.2f}%')
                elif mom_change < -0.5 and rsi > 55:  # price falling but RSI high = bearish divergence
                    signals_bear += 1
                    detail.append(f'MACD_DIV_BEAR={mom_change:.2f}%')

    # 4) Volume spike
    SPIKE_MULT = ctx.get('REVERSAL_VOLUME_SPIKE_MULT', 2.0)
    if vol_ratio >= SPIKE_MULT:
        # Volume spike: amplifies existing signal direction
        if signals_bull > signals_bear:
            signals_bull += 1
            detail.append(f'VOL_SPIKE_BULL={vol_ratio:.1f}x')
        elif signals_bear > signals_bull:
            signals_bear += 1
            detail.append(f'VOL_SPIKE_BEAR={vol_ratio:.1f}x')
        else:
            detail.append(f'VOL_SPIKE_NEUTRAL={vol_ratio:.1f}x')

    # 5) Regime check (trend exhaustion)
    if ema9 > 0 and ema50 > 0:
        trend_stretch = abs(ema9 - ema50) / ema50 * 100 if ema50 > 0 else 0
        if trend_stretch > 5.0:  # EMA9 stretched >5% from EMA50 = exhaustion risk
            if ema9 > ema50:
                signals_bear += 1
                detail.append(f'TREND_EXHAUST_BEAR={trend_stretch:.1f}%')
            else:
                signals_bull += 1
                detail.append(f'TREND_EXHAUST_BULL={trend_stretch:.1f}%')

    MIN_SIGNALS = ctx.get('REVERSAL_MIN_SIGNALS', 2)
    is_reversal = False
    direction_hint = None
    total_signals = max(signals_bull, signals_bear)

    if signals_bull >= MIN_SIGNALS and signals_bull > signals_bear:
        is_reversal = True
        direction_hint = 'BULLISH_REVERSAL'
    elif signals_bear >= MIN_SIGNALS and signals_bear > signals_bull:
        is_reversal = True
        direction_hint = 'BEARISH_REVERSAL'

    result = (is_reversal, direction_hint, total_signals, '|'.join(detail))
    _reversal_cache[cache_key] = result
    _reversal_cache_ts[cache_key] = now
    return result


# ── [v10.26] External data confirmation ───────────────────────
_confirm_cache = {}
_confirm_cache_ts = {}

def _polygon_confirm(ctx, symbol, direction):
    """[v10.26] Check Polygon snapshot for direction confirmation."""
    try:
        api_key = ctx.get('POLYGON_API_KEY', '')
        if not api_key:
            return True, 'no_key'  # pass-through if no key
        cache_key = f"poly:{symbol}"
        now = time.time()
        if cache_key in _confirm_cache and now - _confirm_cache_ts.get(cache_key, 0) < 120:
            cached = _confirm_cache[cache_key]
        else:
            url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}?apiKey={api_key}"
            r = requests.get(url, timeout=ctx.get('CONFIRM_TIMEOUT_S', 3.0))
            if r.status_code != 200:
                return True, f'api_err_{r.status_code}'
            data = r.json()
            ticker = data.get('ticker', {})
            day = ticker.get('day', {})
            cached = {
                'change_pct': day.get('todaysChangePerc', 0),
                'volume': day.get('volume', 0),
                'vwap': day.get('vwap', 0),
                'close': day.get('close', 0),
            }
            _confirm_cache[cache_key] = cached
            _confirm_cache_ts[cache_key] = now

        change = cached.get('change_pct', 0) or 0
        # Confirm: LONG needs positive or neutral momentum, SHORT needs negative or neutral
        if direction == 'LONG' and change < -3.0:
            return False, f'polygon_against_long(chg={change:.1f}%)'
        if direction == 'SHORT' and change > 3.0:
            return False, f'polygon_against_short(chg={change:.1f}%)'
        return True, f'polygon_ok(chg={change:.1f}%)'
    except Exception as e:
        return True, f'polygon_err({str(e)[:40]})'


def _brapi_confirm(ctx, symbol, direction):
    """[v10.26] Check BRAPI for B3 stock confirmation."""
    try:
        token = ctx.get('BRAPI_TOKEN', '')
        if not token:
            return True, 'no_key'
        cache_key = f"brapi:{symbol}"
        now = time.time()
        if cache_key in _confirm_cache and now - _confirm_cache_ts.get(cache_key, 0) < 120:
            cached = _confirm_cache[cache_key]
        else:
            url = f"https://brapi.dev/api/quote/{symbol}?token={token}"
            r = requests.get(url, timeout=ctx.get('CONFIRM_TIMEOUT_S', 3.0))
            if r.status_code != 200:
                return True, f'api_err_{r.status_code}'
            data = r.json()
            results = data.get('results', [{}])
            if not results:
                return True, 'no_data'
            cached = results[0]
            _confirm_cache[cache_key] = cached
            _confirm_cache_ts[cache_key] = now

        change = cached.get('regularMarketChangePercent', 0) or 0
        if direction == 'LONG' and change < -3.0:
            return False, f'brapi_against_long(chg={change:.1f}%)'
        if direction == 'SHORT' and change > 3.0:
            return False, f'brapi_against_short(chg={change:.1f}%)'
        return True, f'brapi_ok(chg={change:.1f}%)'
    except Exception as e:
        return True, f'brapi_err({str(e)[:40]})'


def check_external_confirmation(ctx, symbol, direction, market_type):
    """[v10.26] Run external confirmation filter. Returns (ok, reason)."""
    if not ctx.get('POLYGON_CONFIRM_ENABLED') and not ctx.get('BRAPI_CONFIRM_ENABLED'):
        return True, 'disabled'

    confirmations = 0
    rejections = 0
    details = []

    if market_type == 'NYSE' and ctx.get('POLYGON_CONFIRM_ENABLED'):
        ok, reason = _polygon_confirm(ctx, symbol, direction)
        details.append(reason)
        if ok: confirmations += 1
        else: rejections += 1

    if market_type == 'B3' and ctx.get('BRAPI_CONFIRM_ENABLED'):
        ok, reason = _brapi_confirm(ctx, symbol, direction)
        details.append(reason)
        if ok: confirmations += 1
        else: rejections += 1

    min_agree = ctx.get('CONFIRM_MIN_AGREEMENT', 1)
    if rejections > 0 and confirmations < min_agree:
        return False, f'REJECTED({"|".join(details)})'
    return True, f'CONFIRMED({"|".join(details)})'



def build_execution_ctx(g):
    """
    Build the execution context dict from a globals() dict.

    Args:
        g: globals() dict from api_server.py

    Returns:
        ctx: dict with all required state and functions
    """
    ctx = {
        # State locks & collections
        'state_lock':              g.get('state_lock'),
        'learning_lock':           g.get('learning_lock'),
        'stocks_open':             g.get('stocks_open'),
        'crypto_open':             g.get('crypto_open'),
        'stock_prices':            g.get('stock_prices'),
        'crypto_prices':           g.get('crypto_prices'),
        'stocks_capital':          g.get('stocks_capital'),
        'crypto_capital':          g.get('crypto_capital'),
        'stocks_closed':           g.get('stocks_closed'),
        'crypto_closed':           g.get('crypto_closed'),
        'market_regime':           g.get('market_regime'),
        'trading_suspended':       g.get('trading_suspended'),
        '_last_trade_opened':      g.get('_last_trade_opened'),
        'processed_signal_ids':    g.get('processed_signal_ids'),
        'symbol_cooldown':         g.get('symbol_cooldown'),
        'symbol_sl_count':         g.get('symbol_sl_count'),
        '_trailing_stop_cooldown': g.get('_trailing_stop_cooldown'),
        'crypto_momentum':         g.get('crypto_momentum'),
        'crypto_prices':           g.get('crypto_prices'),
        'crypto_tickers':          g.get('crypto_tickers'),
        'arbi_closed':             g.get('arbi_closed'),
        'factor_stats_cache':      g.get('factor_stats_cache'),
        'pattern_stats_cache':     g.get('pattern_stats_cache'),
        '_cross_market_state':     g.get('_cross_market_state'),
        '_candles_cache':          g.get('_candles_cache'),

        # Logging
        'log':                     g.get('log'),

        # Helper functions - Core execution
        'beat':                    g.get('beat'),
        'get_adaptive_sl_pct':     g.get('get_adaptive_sl_pct'),
        'get_regime_multiplier':   g.get('get_regime_multiplier'),
        'is_trade_flat':           g.get('is_trade_flat'),
        'get_dynamic_timeout_h':   g.get('get_dynamic_timeout_h'),
        'market_open_for':         g.get('market_open_for'),
        'is_momentum_positive':    g.get('is_momentum_positive'),
        'apply_fee_to_trade':      g.get('apply_fee_to_trade'),
        'ledger_record':           g.get('ledger_record'),

        # Helper functions - Risk & Signals
        'check_risk':              g.get('check_risk'),
        'should_trade_ml':         g.get('should_trade_ml'),
        'record_signal_event':     g.get('record_signal_event'),
        'update_signal_outcome':   g.get('update_signal_outcome'),
        'update_signal_attribution': g.get('update_signal_attribution'),
        'record_shadow_decision':  g.get('record_shadow_decision'),
        'audit':                   g.get('audit'),
        'enqueue_persist':         g.get('enqueue_persist'),
        'alert_trade_closed':      g.get('alert_trade_closed'),
        'alert_signal':            g.get('alert_signal'),

        # Helper functions - Learning & Features
        'extract_features':        g.get('extract_features'),
        'make_feature_hash':       g.get('make_feature_hash'),
        'calc_learning_confidence': g.get('calc_learning_confidence'),
        'generate_insight':        g.get('generate_insight'),
        'get_risk_multiplier':     g.get('get_risk_multiplier'),
        'get_dq_score':            g.get('get_dq_score'),
        'make_score_snapshot':     g.get('make_score_snapshot'),
        'process_trade_outcome':   g.get('process_trade_outcome'),
        'update_symbol_duration':  g.get('update_symbol_duration'),
        'gen_id':                  g.get('gen_id'),
        'get_temporal_stock_score': g.get('get_temporal_stock_score'),
        'get_composite_score_adj':  g.get('get_composite_score_adj'),
        '_score_bucket':           g.get('_score_bucket'),
        '_time_bucket':            g.get('_time_bucket'),
        'get_temporal_crypto_score': g.get('get_temporal_crypto_score'),
        'get_cross_market_crypto_adj': g.get('get_cross_market_crypto_adj'),
        '_fetch_binance_klines':   g.get('_fetch_binance_klines'),
        '_get_cached_candles':     g.get('_get_cached_candles'),
        '_set_cached_candles':     g.get('_set_cached_candles'),
        '_calc_atr':               g.get('_calc_atr'),
        '_crypto_composite_score': g.get('_crypto_composite_score'),
        '_second_validation':      g.get('_second_validation'),
        'create_order':            g.get('create_order'),
        'update_order_status':     g.get('update_order_status'),
        'check_strategy_daily_dd': g.get('check_strategy_daily_dd'),
        'is_symbol_blacklisted':   g.get('is_symbol_blacklisted'),
        'check_directional_exposure': g.get('check_directional_exposure'),
        'get_db':                  g.get('get_db'),
        'kill_switch_middleware':  g.get('kill_switch_middleware'),
        'risk_manager':            g.get('risk_manager'),
        'perf_stats':              g.get('perf_stats'),
        'check_crypto_conviction': g.get('check_crypto_conviction'),
        '_update_cross_market_from_stocks': g.get('_update_cross_market_from_stocks'),
        '_rsi_bucket':             g.get('_rsi_bucket'),

        # Constants
        'TRAILING_PEAK_STOCKS':    g.get('TRAILING_PEAK_STOCKS'),
        'TRAILING_DROP_STOCKS':    g.get('TRAILING_DROP_STOCKS'),
        'TRAILING_PEAK_CRYPTO':    g.get('TRAILING_PEAK_CRYPTO'),
        'TRAILING_DROP_CRYPTO':    g.get('TRAILING_DROP_CRYPTO'),
        'SYMBOL_SL_COOLDOWNS':     g.get('SYMBOL_SL_COOLDOWNS'),
        'SYMBOL_COOLDOWN_SEC':     g.get('SYMBOL_COOLDOWN_SEC'),
        'MIN_SCORE_AUTO':          g.get('MIN_SCORE_AUTO'),
        'MIN_SCORE_AUTO_CRYPTO':   g.get('MIN_SCORE_AUTO_CRYPTO'),
        'MAX_POSITIONS_STOCKS':    g.get('MAX_POSITIONS_STOCKS'),
        'MAX_POSITIONS_CRYPTO':    g.get('MAX_POSITIONS_CRYPTO'),
        'MAX_OPEN_POSITIONS':      g.get('MAX_OPEN_POSITIONS'),
        'MAX_POSITION_STOCKS':     g.get('MAX_POSITION_STOCKS'),
        'MAX_POSITION_CRYPTO':     g.get('MAX_POSITION_CRYPTO'),
        'MAX_POSITION_SAME_MKT':   g.get('MAX_POSITION_SAME_MKT'),
        'INITIAL_CAPITAL_STOCKS':  g.get('INITIAL_CAPITAL_STOCKS'),
        'INITIAL_CAPITAL_CRYPTO':  g.get('INITIAL_CAPITAL_CRYPTO'),
        'MAX_CAPITAL_PCT_STOCKS':  g.get('MAX_CAPITAL_PCT_STOCKS'),
        'MAX_CAPITAL_PCT_CRYPTO':  g.get('MAX_CAPITAL_PCT_CRYPTO'),
        'STOCK_SYMBOLS_B3':        g.get('STOCK_SYMBOLS_B3'),
        'CRYPTO_SYMBOLS':          g.get('CRYPTO_SYMBOLS'),
        'CRYPTO_MAX_POSITION_BY_SYM': g.get('CRYPTO_MAX_POSITION_BY_SYM'),
        'LEARNING_DEAD_ZONE_LOW':  g.get('LEARNING_DEAD_ZONE_LOW'),
        'LEARNING_DEAD_ZONE_HIGH': g.get('LEARNING_DEAD_ZONE_HIGH'),
        'LEARNING_VERSION':        g.get('LEARNING_VERSION'),
        'ALERT_MIN_SCORE':         g.get('ALERT_MIN_SCORE'),
        'MAX_PROCESSED_SIGNALS_CACHE': g.get('MAX_PROCESSED_SIGNALS_CACHE'),

        # [v10.26] Monthly Picks owns all LONG capital
        'MONTHLY_PICKS_OWNS_LONG':       g.get('MONTHLY_PICKS_OWNS_LONG', True),

        # [v10.26] New constants
        'MAX_ENTRIES_PER_MINUTE_STOCKS':  g.get('MAX_ENTRIES_PER_MINUTE_STOCKS', 5),
        'MAX_ENTRIES_PER_MINUTE_CRYPTO':  g.get('MAX_ENTRIES_PER_MINUTE_CRYPTO', 3),
        'REVERSAL_RSI_OB':               g.get('REVERSAL_RSI_OB', 72),
        'REVERSAL_RSI_OS':               g.get('REVERSAL_RSI_OS', 28),
        'REVERSAL_VOLUME_SPIKE_MULT':    g.get('REVERSAL_VOLUME_SPIKE_MULT', 2.0),
        'REVERSAL_MIN_SIGNALS':          g.get('REVERSAL_MIN_SIGNALS', 2),
        'REVERSAL_BLOCK_COUNTER_TREND':  g.get('REVERSAL_BLOCK_COUNTER_TREND', True),
        'REVERSAL_CLOSE_LOSING':         g.get('REVERSAL_CLOSE_LOSING', True),
        'POLYGON_API_KEY':               g.get('POLYGON_API_KEY', ''),
        'BRAPI_TOKEN':                   g.get('BRAPI_TOKEN', ''),
        'POLYGON_CONFIRM_ENABLED':       g.get('POLYGON_CONFIRM_ENABLED', True),
        'OPLAB_CONFIRM_ENABLED':         g.get('OPLAB_CONFIRM_ENABLED', True),
        'BRAPI_CONFIRM_ENABLED':         g.get('BRAPI_CONFIRM_ENABLED', True),
        'CONFIRM_TIMEOUT_S':             g.get('CONFIRM_TIMEOUT_S', 3.0),
        'CONFIRM_MIN_AGREEMENT':         g.get('CONFIRM_MIN_AGREEMENT', 1),
    }
    return ctx


# ═══════════════════════════════════════════════════════════════
# [v10.X] MONITOR TRADES — Líneas 5091-5281 from api_server.py
# ═══════════════════════════════════════════════════════════════
def monitor_trades(ctx):
    """
    Monitor open trades and apply exit logic (SL/TP/trailing/timeout/market_close).

    Processes both stock and crypto trades, calculating P&L, applying adaptive stop losses,
    trailing stops, flat exits, and timeout closures. Updates capital allocation and
    records closed trades for learning.

    Args:
        ctx: Execution context dict (from build_execution_ctx)
    """
    while True:
        ctx['beat']('monitor_trades')
        time.sleep(5)
        try:
            closed_stocks=[]; closed_cryptos=[]
            with ctx['state_lock']:
                now=datetime.utcnow(); to_close=[]
                for trade in ctx['stocks_open']:
                    sym=trade['symbol']; pd=ctx['stock_prices'].get(sym)
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
                    _adaptive_sl_s = ctx['get_adaptive_sl_pct'](trade)
                    _regime_sm, _regime_sl_m, _regime_r = ctx['get_regime_multiplier']()
                    _eff_sl_s = round(_adaptive_sl_s * _regime_sl_m, 2)
                    _timeout_s = ctx['get_dynamic_timeout_h'](sym, 2.0)
                    if peak>=ctx['TRAILING_PEAK_STOCKS'] and trade['pnl_pct']<=peak-ctx['TRAILING_DROP_STOCKS']:
                        reason='TRAILING_STOP'
                    elif trade['pnl_pct']<=-_eff_sl_s:
                        reason='STOP_LOSS'
                    elif ctx['is_trade_flat'](trade, now):
                        reason='FLAT_EXIT'
                    elif age_h>=_timeout_s:
                        ext=trade.get('extensions',0)
                        if ctx['is_momentum_positive'](trade) and ext<3: trade['extensions']=ext+1
                        else:                                      reason='TIMEOUT'
                    elif not ctx['market_open_for'](mkt) and age_h>0.5:   reason='MARKET_CLOSE'
                    if reason:
                        ctx['stocks_capital'] += trade['position_value']
                        ctx['ledger_record']('stocks', 'RELEASE', trade['symbol'],
                                      trade['position_value'], ctx['stocks_capital'], trade['id'])
                        ctx['stocks_capital'] += trade['pnl']
                        if trade['pnl'] != 0:
                            ctx['ledger_record']('stocks', 'PNL_CREDIT', trade['symbol'],
                                          trade['pnl'], ctx['stocks_capital'], trade['id'])
                        ctx['risk_manager'].record_trade_result('stocks', trade['symbol'], trade['pnl'], trade['position_value'], ctx['stocks_capital'])
                        ctx['perf_stats'].record_trade({
                            'strategy': 'stocks', 'symbol': trade['symbol'],
                            'pnl': trade['pnl'], 'pnl_pct': trade['pnl_pct'],
                            'entry_price': trade['entry_price'], 'exit_price': price,
                            'opened_at': trade['opened_at'], 'closed_at': now.isoformat(),
                            'confidence': trade.get('learning_confidence', 0),
                            'exit_type': reason, 'asset_type': 'stock',
                            'regime': ctx['market_regime'].get('mode', 'UNKNOWN'),
                        })
                        if reason == 'STOP_LOSS':
                            ctx['symbol_sl_count'][sym] = ctx['symbol_sl_count'].get(sym, 0) + 1
                        else:
                            ctx['symbol_sl_count'][sym] = 0
                        _cd = ctx['SYMBOL_SL_COOLDOWNS'].get(min(ctx['symbol_sl_count'].get(sym,1),4), 300)
                        ctx['symbol_cooldown'][sym] = time.time() + (_cd - ctx['SYMBOL_COOLDOWN_SEC'])
                        c=dict(trade); c.update({'exit_price':price,'closed_at':now.isoformat(),'close_reason':reason,'status':'CLOSED'})
                        try:
                            ctx['apply_fee_to_trade'](c)
                        except Exception as _fe:
                            ctx['log'].debug(f"apply_fee_to_trade stock: {_fe}")
                        ctx['stocks_closed'].insert(0,c)
                        to_close.append(trade['id']); closed_stocks.append(c)
                ctx['stocks_open'][:] = [t for t in ctx['stocks_open'] if t['id'] not in to_close]

                to_close_c=[]
                for trade in ctx['crypto_open']:
                    sym=trade['symbol']+'USDT'
                    _raw_price = ctx['crypto_prices'].get(sym, 0)
                    _entry = trade.get('entry_price', 0)
                    _price_sane = (_raw_price > 0 and
                                   (_entry <= 0 or _raw_price >= _entry * 0.05))
                    price = _raw_price if _price_sane else trade.get('current_price', _entry)
                    if price <= 0: price = _entry if _entry > 0 else 1
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
                    _adaptive_sl_c = ctx['get_adaptive_sl_pct'](trade)
                    _regime_cm, _regime_csl_m, _regime_cr = ctx['get_regime_multiplier']()
                    _eff_sl_c = round(_adaptive_sl_c * _regime_csl_m, 2)
                    _timeout_c = ctx['get_dynamic_timeout_h'](trade['symbol'], 4.0)
                    if peak>=ctx['TRAILING_PEAK_CRYPTO'] and trade['pnl_pct']<=peak-ctx['TRAILING_DROP_CRYPTO']:
                        reason='TRAILING_STOP'
                    elif trade['pnl_pct']<=-_eff_sl_c:
                        reason='STOP_LOSS'
                    elif ctx['is_trade_flat'](trade, now):
                        reason='FLAT_EXIT'
                    elif age_h>=_timeout_c:
                        ext=trade.get('extensions',0)
                        if ctx['is_momentum_positive'](trade) and ext<3: trade['extensions']=ext+1
                        else:                                      reason='TIMEOUT'
                    if reason:
                        ctx['crypto_capital'] += trade['position_value']
                        ctx['ledger_record']('crypto', 'RELEASE', trade['symbol'],
                                      trade['position_value'], ctx['crypto_capital'], trade['id'])
                        ctx['crypto_capital'] += trade['pnl']
                        if trade['pnl'] != 0:
                            ctx['ledger_record']('crypto', 'PNL_CREDIT', trade['symbol'],
                                          trade['pnl'], ctx['crypto_capital'], trade['id'])
                        ctx['risk_manager'].record_trade_result('crypto', trade['symbol'], trade['pnl'], trade['position_value'], ctx['crypto_capital'])
                        ctx['perf_stats'].record_trade({
                            'strategy': 'crypto', 'symbol': trade['symbol'],
                            'pnl': trade['pnl'], 'pnl_pct': trade['pnl_pct'],
                            'entry_price': trade['entry_price'], 'exit_price': price,
                            'opened_at': trade['opened_at'], 'closed_at': now.isoformat(),
                            'confidence': trade.get('learning_confidence', 0),
                            'exit_type': reason, 'asset_type': 'crypto',
                            'regime': ctx['market_regime'].get('mode', 'UNKNOWN'),
                        })
                        ctx['symbol_cooldown'][trade['symbol']]=time.time()
                        c=dict(trade); c.update({'exit_price':price,'closed_at':now.isoformat(),'close_reason':reason,'status':'CLOSED'})
                        try:
                            ctx['apply_fee_to_trade'](c)
                        except Exception as _fe:
                            ctx['log'].debug(f"apply_fee_to_trade crypto: {_fe}")
                        ctx['crypto_closed'].insert(0,c)
                        to_close_c.append(trade['id']); closed_cryptos.append(c)
                ctx['crypto_open'][:] = [t for t in ctx['crypto_open'] if t['id'] not in to_close_c]

            for c in closed_stocks:
                try:
                    _dur_h = (datetime.fromisoformat(c['closed_at']) - datetime.fromisoformat(c['opened_at'])).total_seconds() / 3600
                    ctx['update_symbol_duration'](c['symbol'], _dur_h)
                except: pass
                ctx['audit']('TRADE_CLOSED',{'id':c['id'],'symbol':c['symbol'],'pnl':c['pnl'],'reason':c['close_reason']})
                ctx['enqueue_persist']('trade',c)
                ctx['enqueue_persist']('cooldown',symbol=c['symbol'],ts=ctx['symbol_cooldown'].get(c['symbol'],time.time()))
                ctx['alert_trade_closed'](c)
                if c.get('close_reason') == 'TRAILING_STOP' and float(c.get('pnl',0)) > 0:
                    ctx['_trailing_stop_cooldown'][c['symbol']] = time.time()
                    ctx['log'].info(f'[TRAIL-COOLDOWN] {c["symbol"]}: 2h cooldown após TRAILING_STOP +${float(c.get("pnl",0)):,.0f}')
                ctx['process_trade_outcome'](c)
                if c.get('asset_type') == 'stock':
                    try: ctx['_update_cross_market_from_stocks']()
                    except: pass
            for c in closed_cryptos:
                try:
                    _dur_h = (datetime.fromisoformat(c['closed_at']) - datetime.fromisoformat(c['opened_at'])).total_seconds() / 3600
                    ctx['update_symbol_duration'](c['symbol'], _dur_h)
                except: pass
                ctx['audit']('TRADE_CLOSED',{'id':c['id'],'symbol':c['symbol'],'pnl':c['pnl'],'reason':c['close_reason']})
                ctx['enqueue_persist']('trade',c)
                ctx['enqueue_persist']('cooldown',symbol=c['symbol'],ts=ctx['symbol_cooldown'].get(c['symbol'],time.time()))
                ctx['alert_trade_closed'](c)
                ctx['process_trade_outcome'](c)
                if c.get('asset_type') == 'stock':
                    try: ctx['_update_cross_market_from_stocks']()
                    except: pass
            ctx['beat']('monitor_trades')
        except Exception as e: ctx['log'].error(f'monitor_trades: {e}')


# ═══════════════════════════════════════════════════════════════
# [V9-1] STOCK EXECUTION WORKER — Líneas 5286-5770 from api_server.py
# ═══════════════════════════════════════════════════════════════
def stock_execution_worker(ctx):
    """
    Stock signal processing and trade entry loop.

    Generates stock trading signals from price data, applies scoring and learning-based
    confidence filters, and executes LONG/SHORT trades based on risk management rules.

    Args:
        ctx: Execution context dict (from build_execution_ctx)
    """
    while True:
        ctx['beat']('stock_execution_worker')
        time.sleep(60)
        ctx['beat']('stock_execution_worker')
        try:
            with ctx['state_lock']:
                sp_snap = dict(ctx['stock_prices'])
            now_iso = datetime.utcnow().isoformat()
            rows = []
            for sym, pd_data in sp_snap.items():
                if not pd_data or pd_data.get('price', 0) <= 0: continue
                mkt_type = 'B3' if any(sym == s.replace('.SA','') for s in ctx['STOCK_SYMBOLS_B3']) else 'NYSE'
                rsi  = pd_data.get('rsi', 50) or 50
                ema9 = pd_data.get('ema9', 0)  or 0
                ema21= pd_data.get('ema21',0)  or 0
                score = 50
                if   rsi < 30: score += 25
                elif rsi < 40: score += 15
                elif rsi < 50: score += 5
                elif rsi > 70: score -= 25
                elif rsi > 60: score -= 15
                elif rsi > 50: score -= 5
                ema50 = pd_data.get('ema50', 0) or 0
                if ema9 > 0 and ema21 > 0:
                    if ema9 > ema21:
                        score += 12
                        if ema50 > 0 and ema21 > ema50: score += 8
                    else:
                        score -= 12
                        if ema50 > 0 and ema21 < ema50: score -= 8
                vol_ratio = pd_data.get('volume_ratio', 0) or 0
                if vol_ratio > 1.5: score += 8
                elif vol_ratio < 0.5: score -= 5
                atr_pct = pd_data.get('atr_pct', 0) or 0
                if 0 < atr_pct < 1.5: score += 5
                elif atr_pct > 4.0: score -= 10
                price = pd_data.get('price', 0) or 0
                if price > 0 and ema9 > 0:
                    if price > ema9 * 1.01: score += 7
                    elif price < ema9 * 0.99: score -= 7
                score = max(0, min(100, score))
                _rsi_bkt = 'OVERSOLD' if rsi<30 else ('LOW' if rsi<45 else ('OVERBOUGHT' if rsi>75 else ('HIGH' if rsi>65 else 'NEUTRAL')))
                _ema_align  = 'BULLISH_STACK' if (ema9>ema21 and ema21>ema50 and ema50>0) else ('BEARISH_STACK' if (ema9<ema21 and ema21<ema50 and ema50>0) else ('BULLISH' if ema9>ema21 else 'BEARISH'))
                _vol_bucket = 'LOW' if (vol_ratio>0 and vol_ratio<0.8) else ('HIGH' if vol_ratio>1.8 else 'NORMAL')
                _atr_bucket = 'EXTREME' if atr_pct>4 else ('HIGH' if atr_pct>2.5 else ('LOW' if atr_pct<0.8 else 'NORMAL'))
                _direction  = 'LONG' if score>50 else 'SHORT'
                _score_adj  = 0
                _pattern_blocked = False
                with ctx['learning_lock']:
                    _fs_rsi = ctx['factor_stats_cache'].get(('rsi_bucket', _rsi_bkt), {})
                    if _fs_rsi.get('total_samples',0) >= 10:
                        _score_adj += int(_fs_rsi.get('confidence_weight',0) * 12)
                    _fs_ema = ctx['factor_stats_cache'].get(('ema_alignment', _ema_align), {})
                    if _fs_ema.get('total_samples',0) >= 10:
                        _score_adj += int(_fs_ema.get('confidence_weight',0) * 12)
                    _fs_vol = ctx['factor_stats_cache'].get(('volatility_bucket', _vol_bucket), {})
                    if _fs_vol.get('total_samples',0) >= 10:
                        _score_adj += int(_fs_vol.get('confidence_weight',0) * 8)
                    _fs_atr = ctx['factor_stats_cache'].get(('atr_bucket', _atr_bucket), {})
                    if _fs_atr.get('total_samples',0) >= 10:
                        _score_adj += int(_fs_atr.get('confidence_weight',0) * 10)
                    _fs_dir = ctx['factor_stats_cache'].get(('direction', _direction), {})
                    if _fs_dir.get('total_samples',0) >= 5:
                        _score_adj += int(_fs_dir.get('confidence_weight',0) * 6)
                    for _ph, _ps in list(ctx['pattern_stats_cache'].items())[:200]:
                        _pn = _ps.get('total_samples',0)
                        _pw = _ps.get('wins',0)
                        if _pn >= 30 and _pw/_pn < 0.40 and _ps.get('ewma_hit_rate',1) < 0.45:
                            _pattern_blocked = True
                            break
                score = max(0, min(100, score + _score_adj))
                _pre_dir = 'LONG' if score > 50 else 'SHORT'
                _is_weak_long  = (_pre_dir == 'LONG'  and score < ctx['MIN_SCORE_AUTO'] + 5)
                _is_weak_short = (_pre_dir == 'SHORT' and score > (100 - ctx['MIN_SCORE_AUTO'] - 5))
                if _pattern_blocked and (_is_weak_long or _is_weak_short):
                    continue
                _now_s = datetime.utcnow()
                _mkt_type = 'B3' if any(sym == s.replace('.SA','') for s in ctx['STOCK_SYMBOLS_B3']) else 'NYSE'
                _st_adj, _st_blocked, _st_reason = ctx['get_temporal_stock_score'](_now_s.hour, _now_s.weekday(), _mkt_type)
                _pre_temporal_dir = 'SHORT' if score <= 50 else 'LONG'
                if _st_blocked and _pre_temporal_dir == 'LONG':
                    ctx['log'].debug(f"STOCK_TEMPORAL_BLOCK: {sym} — {_st_reason}")
                    continue
                elif _st_blocked and _pre_temporal_dir == 'SHORT':
                    _st_adj = -5
                _score_before_t = score
                _pre_dir_t = 'LONG' if score > 50 else ('SHORT' if score < 50 else 'NEUTRAL')
                if _pre_dir_t == 'SHORT' and _st_adj > 0:
                    _st_adj_effective = -_st_adj
                elif _pre_dir_t == 'SHORT' and _st_adj < 0:
                    _st_adj_effective = -_st_adj
                else:
                    _st_adj_effective = _st_adj
                score = max(0, min(100, score + _st_adj_effective))
                _feats_disc = {'score_bucket': ctx['_score_bucket'](score), 'rsi_bucket': _rsi_bkt,
                               'ema_alignment': 'BULLISH' if ema9>ema21 else 'BEARISH',
                               'weekday': str(_now_s.weekday()), 'hour_utc': str(_now_s.hour),
                               'time_bucket': ctx['_time_bucket'](_now_s), 'market_type': _mkt_type,
                               'asset_type': 'stock', 'direction': 'LONG' if score>50 else 'SHORT',
                               'volatility_bucket': 'LOW' if atr_pct<1 else ('HIGH' if atr_pct>3 else 'NORMAL'),
                               'atr_bucket': 'EXTREME' if atr_pct>4 else ('HIGH' if atr_pct>2.5 else 'NORMAL'),
                               'volume_bucket': 'HIGH' if vol_ratio>1.5 else ('LOW' if vol_ratio<0.5 else 'NORMAL')}
                try:
                    _disc_adj, _disc_blocked, _disc_key = ctx['get_composite_score_adj'](_feats_disc)
                except Exception as _ge:
                    _disc_adj, _disc_blocked, _disc_key = 0, False, ''
                if _disc_blocked:
                    ctx['log'].debug(f"COMPOSITE_BLOCK stock {sym}: {_disc_key}")
                    continue
                if _disc_adj != 0:
                    score = max(0, min(100, score + _disc_adj))
                    ctx['log'].debug(f"COMPOSITE_ADJ stock {sym}: {_disc_adj:+d} via {_disc_key}")
                if abs(_st_adj) >= 5:
                    ctx['log'].debug(f"STOCK_SCORE_ADJ: {sym} {_score_before_t}→{score} ({_st_reason})")
                signal_val = 'COMPRA' if score >= ctx['MIN_SCORE_AUTO'] else ('VENDA' if score <= (100-ctx['MIN_SCORE_AUTO']) else 'MANTER')
                rows.append({
                    'symbol': sym, 'price': pd_data.get('price', 0),
                    'score': score, 'signal': signal_val,
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
                _eff_min = ctx['MIN_SCORE_AUTO']
                with ctx['learning_lock']:
                    _best_pat = max(ctx['pattern_stats_cache'].values(),
                                   key=lambda p: p.get('wins',0)/max(p.get('total_samples',1),1),
                                   default={})
                    if _best_pat.get('total_samples',0)>=50:
                        _pat_wr = _best_pat.get('wins',0)/_best_pat['total_samples']
                        if _pat_wr >= 0.80: _eff_min = max(65, ctx['MIN_SCORE_AUTO']-5)
                        elif _pat_wr >= 0.70: _eff_min = ctx['MIN_SCORE_AUTO']
                is_long=score>=_eff_min and signal_val=='COMPRA'
                is_short=score<=(100-_eff_min) and signal_val=='VENDA'
                # [v10.26] LONG capital goes to Monthly Picks — block auto LONG stocks
                if is_long and ctx.get('MONTHLY_PICKS_OWNS_LONG', True):
                    continue
                if is_short:
                    ctx['log'].info(f'[SHORT-DBG] {sym} score={score} _eff_min={_eff_min} is_short={is_short} signal_val={signal_val}')
                if not (is_long or is_short): continue

                if is_long:
                    _hist = pd_data.get('price_history') or []
                    if len(_hist) >= 5:
                        _p_now  = pd_data.get('price', 0)
                        _p_5ago = _hist[-5] if len(_hist) >= 5 else _hist[0]
                        if _p_5ago > 0 and (_p_now - _p_5ago) / _p_5ago * 100 < -5.0:
                            ctx['log'].debug(f'TREND_FILTER: {sym} LONG bloqueado — queda {(_p_now-_p_5ago)/_p_5ago*100:.1f}% em 5 períodos')
                            continue

                # [v10.26] Market reversal detection
                _pd_for_rev = sp_snap.get(sym, {})
                _is_rev, _rev_dir, _rev_count, _rev_detail = detect_market_reversal(ctx, sym, _pd_for_rev, 'stock')
                if _is_rev and ctx.get('REVERSAL_BLOCK_COUNTER_TREND', True):
                    if _rev_dir == 'BEARISH_REVERSAL' and is_long:
                        ctx['log'].info(f'[REVERSAL-BLOCK] {sym} LONG blocked: {_rev_detail} ({_rev_count} signals)')
                        continue
                    if _rev_dir == 'BULLISH_REVERSAL' and is_short:
                        ctx['log'].info(f'[REVERSAL-BLOCK] {sym} SHORT blocked: {_rev_detail} ({_rev_count} signals)')
                        continue

                # [v10.26] External confirmation (Polygon / BRAPI)
                direction = 'LONG' if is_long else 'SHORT'
                _ext_ok, _ext_reason = check_external_confirmation(ctx, sym, direction, mkt)
                if not _ext_ok:
                    ctx['log'].info(f'[CONFIRM-BLOCK] {sym} {direction}: {_ext_reason}')
                    continue

                PERMANENT_REASONS = {'kill_switch', 'symbol_duplicate', 'executed'}
                _sig_time_window = int(time.time() / 60)
                ms_key = str(sig.get('id') or f"{sym}:{score}:{_sig_time_window}")
                origin_key = ms_key[:120]
                with ctx['learning_lock']:
                    cached = ctx['processed_signal_ids'].get(ms_key)

                if cached:
                    if cached['reason'] in PERMANENT_REASONS:
                        continue
                    reason_was = cached['reason']
                    _sig_pre_id = cached['sig_id']
                    still_blocked = False
                    if reason_was == 'market_closed':
                        still_blocked = not ctx['market_open_for'](mkt)
                    elif reason_was in ('SYMBOL_COOLDOWN', 'cooldown', 'COOLDOWN'):
                        still_blocked = (time.time() - ctx['symbol_cooldown'].get(sym, 0)) < ctx['SYMBOL_COOLDOWN_SEC']
                    elif reason_was in ('INSUFFICIENT_CAPITAL', 'capital',
                                        'STOCKS_CAPITAL_LIMIT', 'CRYPTO_CAPITAL_LIMIT', 'NO_CAPITAL_CRYPTO'):
                        if reason_was in ('STOCKS_CAPITAL_LIMIT', 'capital', 'INSUFFICIENT_CAPITAL'):
                            committed_s = sum(t.get('position_value', 0) for t in ctx['stocks_open'])
                            score_factor_tmp = min(abs(score - 50) / 50.0, 1.0)
                            conf_tmp = ctx['calc_learning_confidence'](
                                {'symbol': sym, 'asset_type': 'stock', 'market_type': mkt, 'score': score},
                                {}, '')
                            rm_tmp = ctx['get_risk_multiplier'](conf_tmp)
                            desired_tmp = min(ctx['stocks_capital'] * (0.08 + score_factor_tmp * 0.07) * rm_tmp,
                                              ctx['MAX_POSITION_STOCKS'])
                            cap_limit = ctx['INITIAL_CAPITAL_STOCKS'] * ctx['MAX_CAPITAL_PCT_STOCKS'] / 100
                            still_blocked = (committed_s + desired_tmp) > cap_limit
                        else:
                            committed_c = sum(t.get('position_value', 0) for t in ctx['crypto_open'])
                            cap_limit_c = ctx['INITIAL_CAPITAL_CRYPTO'] * ctx['MAX_CAPITAL_PCT_CRYPTO'] / 100
                            score_factor_c = min(abs(score - 50) / 50.0, 1.0)
                            desired_c = min(ctx['crypto_capital'] * (0.05 + score_factor_c * 0.05),
                                            ctx['MAX_POSITION_CRYPTO'])
                            still_blocked = (committed_c + desired_c) > cap_limit_c
                    elif reason_was.startswith('MAX_OPEN_POSITIONS'):
                        still_blocked = len(ctx['stocks_open']) + len(ctx['crypto_open']) >= ctx['MAX_OPEN_POSITIONS']
                    elif reason_was.startswith('MAX_POSITIONS_STOCKS'):
                        still_blocked = len(ctx['stocks_open']) >= ctx['MAX_POSITIONS_STOCKS']
                    elif reason_was.startswith('MAX_POSITIONS_CRYPTO'):
                        still_blocked = len(ctx['crypto_open']) >= ctx['MAX_POSITIONS_CRYPTO']
                    elif reason_was.startswith('MAX_POSITION_SAME_MKT'):
                        mkt_count = sum(1 for t in ctx['stocks_open'] if t.get('market') == mkt)
                        still_blocked = mkt_count >= ctx['MAX_POSITION_SAME_MKT']
                    elif reason_was.startswith('SYMBOL_ALREADY_OPEN'):
                        still_blocked = sym in {t['symbol'] for t in ctx['stocks_open'] + ctx['crypto_open']}
                    elif reason_was in ('KILL_SWITCH_ACTIVE', 'KILL_SWITCH', 'ARBI_KILL_SWITCH') \
                            or reason_was.startswith(('DAILY_DRAWDOWN', 'WEEKLY_DRAWDOWN')):
                        still_blocked = True
                    else:
                        still_blocked = False
                    if still_blocked:
                        continue
                else:
                    _sig_pre_id = ctx['gen_id']('SIG')
                    with ctx['learning_lock']:
                        if len(ctx['processed_signal_ids']) >= ctx['MAX_PROCESSED_SIGNALS_CACHE']:
                            keys_to_drop = list(ctx['processed_signal_ids'].keys())[:ctx['MAX_PROCESSED_SIGNALS_CACHE'] // 2]
                            for k in keys_to_drop: del ctx['processed_signal_ids'][k]
                        ctx['processed_signal_ids'][ms_key] = {'sig_id': _sig_pre_id, 'reason': 'processing'}

                direction='LONG' if is_long else 'SHORT'
                score_factor=min(abs(score-50)/50.0,1.0)

                now_dt   = datetime.utcnow()
                dq_score = ctx['get_dq_score'](sym)
                mkt_open = ctx['market_open_for'](mkt)
                price_dict = ctx['stock_prices'].get(sym, {})
                sig_enriched = dict(sig)
                _sym_trades_today = [t for t in list(ctx['stocks_closed'])
                                     if t.get('symbol')==sym
                                     and (t.get('closed_at','') or '')[:10] == datetime.utcnow().strftime('%Y-%m-%d')]
                _last_close_reason = _sym_trades_today[0].get('close_reason','NONE') if _sym_trades_today else 'NONE'
                _had_trailing_today = any(t.get('close_reason')=='TRAILING_STOP' for t in _sym_trades_today)
                _same_day_count_str = '1st' if len(_sym_trades_today)==0 else ('2nd' if len(_sym_trades_today)==1 else '3rd+')
                sig_enriched.update({
                    'price':        price,
                    'asset_type':   'stock',
                    'market_open':  mkt_open,
                    'trade_open':   sym in {t['symbol'] for t in ctx['stocks_open']},
                    'atr_pct':      price_dict.get('atr_pct', 0.0),
                    'volume_ratio': price_dict.get('volume_ratio', 0.0),
                    'reentry_after_trailing': 'YES' if _had_trailing_today else 'NO',
                    'same_day_count':         _same_day_count_str,
                    'close_reason_prev':      _last_close_reason,
                })
                features = ctx['extract_features'](sig_enriched, dict(ctx['market_regime']), dq_score, now_dt)
                features['_dq_score'] = dq_score
                feat_hash = ctx['make_feature_hash'](features)
                conf      = ctx['calc_learning_confidence'](sig_enriched, features, feat_hash)
                insight   = ctx['generate_insight'](sig_enriched, features, feat_hash, conf)
                risk_mult = ctx['get_risk_multiplier'](conf)

                _lc = conf.get('final_confidence', 50)
                _is_short_signal = (direction == 'SHORT')
                if _is_short_signal:
                    ctx['log'].info(f'[SHORT-DBG2] {sym} conf={_lc:.1f} dead_zone={ctx["LEARNING_DEAD_ZONE_LOW"]}-{ctx["LEARNING_DEAD_ZONE_HIGH"]} skip_dz={_is_short_signal}')
                if not _is_short_signal and ctx['LEARNING_DEAD_ZONE_LOW'] <= _lc < ctx['LEARNING_DEAD_ZONE_HIGH']:
                    _confirmed_sig_id = ctx['record_signal_event'](sig_enriched, features, feat_hash, conf, insight,
                                        source_type='stock_signal_db', existing_signal_id=_sig_pre_id,
                                        origin_signal_key=origin_key)
                    ctx['record_shadow_decision'](_confirmed_sig_id, sig_enriched, 'learning_dead_zone')
                    with ctx['learning_lock']:
                        ctx['processed_signal_ids'][ms_key] = {'sig_id': _confirmed_sig_id, 'reason': 'learning_dead_zone'}
                    continue

                _confirmed_sig_id = _sig_pre_id

                def _cache_reason(reason: str):
                    with ctx['learning_lock']:
                        ctx['processed_signal_ids'][ms_key] = {'sig_id': _confirmed_sig_id, 'reason': reason}

                if not mkt_open:
                    _confirmed_sig_id = ctx['record_signal_event'](sig_enriched, features, feat_hash, conf, insight,
                                        source_type='stock_signal_db',
                                        existing_signal_id=_sig_pre_id,
                                        origin_signal_key=origin_key)
                    ctx['record_shadow_decision'](_confirmed_sig_id, sig_enriched, 'market_closed')
                    _cache_reason('market_closed')
                    continue

                _stocks_port_total = max(ctx['stocks_capital'] + sum(t.get('position_value',0) for t in ctx['stocks_open']), ctx['INITIAL_CAPITAL_STOCKS'])
                _regime_size_m, _regime_sl_tmp, _regime_info = ctx['get_regime_multiplier']()
                _pos_target = _stocks_port_total / ctx['MAX_POSITIONS_STOCKS'] * (0.8 + score_factor * 0.4)
                desired_pos = min(max(_pos_target * risk_mult * _regime_size_m, 50_000), ctx['MAX_POSITION_STOCKS'])
                _dd_blocked_s, _dd_reason_s = ctx['check_strategy_daily_dd']('stocks')
                if _dd_blocked_s:
                    ctx['log'].info(f'[STK-DD-BLOCK] {sym}: {_dd_reason_s}')
                    break
                _bl_blocked_s, _bl_reason_s = ctx['is_symbol_blacklisted'](sym)
                if _bl_blocked_s:
                    ctx['log'].info(f'[STK-BL-BLOCK] {sym}: {_bl_reason_s}')
                    continue
                _dir_blocked_s, _dir_reason_s, _dir_stats_s = ctx['check_directional_exposure'](direction, 'stocks')
                if _dir_blocked_s:
                    ctx['log'].info(f'[STK-DIR-BLOCK] {sym}: {_dir_reason_s}')
                    continue
                _ml_ok_s, _ml_reason_s, _ml_score_s = ctx['should_trade_ml'](
                    features, conf, asset_type='stock')
                if not _ml_ok_s:
                    ctx['log'].info(f'[STK-ML-BLOCK] {sym}: {_ml_reason_s} score={score}')
                    ctx['record_shadow_decision'](_confirmed_sig_id, sig_enriched, _ml_reason_s)
                    with ctx['learning_lock']:
                        ctx['processed_signal_ids'][ms_key] = {'sig_id': _confirmed_sig_id, 'reason': _ml_reason_s}
                    continue
                risk_ok,risk_reason,approved_size=ctx['check_risk'](sym,mkt,desired_pos,'stocks')
                if not risk_ok:
                    real_reason = risk_reason.split()[0] if risk_reason else 'risk_blocked'
                    is_permanent_risk = ('KILL_SWITCH' in risk_reason
                                         or risk_reason.startswith(('DAILY_DRAWDOWN', 'WEEKLY_DRAWDOWN')))
                    _confirmed_sig_id = ctx['record_signal_event'](sig_enriched, features, feat_hash, conf, insight,
                                        source_type='stock_signal_db',
                                        existing_signal_id=_sig_pre_id,
                                        origin_signal_key=origin_key)
                    ctx['record_shadow_decision'](_confirmed_sig_id, sig_enriched, real_reason)
                    _cache_reason('kill_switch' if is_permanent_risk else real_reason)
                    ctx['log'].info(f'Risk-1 {sym}: {risk_reason} (dir={direction})')
                    if is_permanent_risk: break
                    continue
                qty=int(approved_size/price)
                if qty<=0: continue

                trade = None; pre_trade_id = ctx['gen_id']('STK'); pre_order_id = ctx['gen_id']('ORD')
                order_side = 'BUY' if direction=='LONG' else 'SELL'
                signal_id  = ctx['record_signal_event'](
                    sig_enriched, features, feat_hash, conf, insight,
                    source_type='stock_signal_db',
                    existing_signal_id=_sig_pre_id,
                    origin_signal_key=origin_key)
                _confirmed_sig_id = signal_id
                _cache_reason('executed')

                with ctx['state_lock']:
                    ks_ok, ks_reason = ctx['kill_switch_middleware'].check_before_trade('stocks', ctx['get_db'])
                    if not ks_ok:
                        ctx['log'].warning(f'[KILL-SWITCH] Stock blocked: {ks_reason}')
                        continue
                    risk_ok, risk_reason = ctx['risk_manager'].check_can_open('stocks', sym, price*qty, ctx['stocks_capital'])
                    if not risk_ok:
                        ctx['log'].warning(f'[RISK-BLOCK] Stock {sym}: {risk_reason}')
                        continue

                    ok2,reason2=ctx['_second_validation'](sym,mkt,'stocks')
                    if ok2 and ctx['stocks_capital']>=price*qty:
                        ctx['stocks_capital'] -= price*qty
                        ctx['ledger_record']('stocks', 'RESERVE', sym,
                                      round(price*qty, 2), ctx['stocks_capital'], pre_trade_id)
                        trade = {
                            'id':pre_trade_id,'symbol':sym,'market':mkt,'asset_type':'stock',
                            'direction':direction,'entry_price':price,'current_price':price,
                            'quantity':qty,'position_value':round(price*qty,2),
                            'pnl':0,'pnl_pct':0,'peak_pnl_pct':0,'score':score,
                            'signal':signal_val,'order_id':pre_order_id,
                            'opened_at':datetime.utcnow().isoformat(),'status':'OPEN',
                            'signal_id':           signal_id,
                            'feature_hash':        feat_hash,
                            'learning_confidence': conf.get('final_confidence'),
                            'insight_summary':     insight,
                            'learning_version':    ctx['LEARNING_VERSION'],
                            '_features':           features,
                            '_score_snapshot':     ctx['make_score_snapshot'](sig_enriched, features, conf),
                            '_atr_pct':            sig.get('atr_pct', 0),
                        }
                        ctx['stocks_open'].append(trade)
                    else:
                        ctx['log'].info(f'Risk-2 {sym}: {reason2 if not ok2 else "insufficient_capital"}')
                        block_reason2 = reason2 if not ok2 else 'capital'
                        ctx['record_shadow_decision'](signal_id, sig_enriched, block_reason2)
                        if 'DUPLICATE' in (reason2 or '').upper():
                            with ctx['learning_lock']:
                                ctx['processed_signal_ids'][ms_key] = {'sig_id': signal_id, 'reason': 'symbol_duplicate'}
                        else:
                            with ctx['learning_lock']:
                                ctx['processed_signal_ids'][ms_key] = {'sig_id': signal_id, 'reason': block_reason2}

                if trade is None: continue

                ctx['update_signal_attribution'](signal_id, pre_trade_id, pre_order_id)

                order = ctx['create_order'](pre_trade_id, sym, order_side, 'MARKET', qty, price, 'stocks',
                                     order_id_override=pre_order_id)
                ctx['update_order_status'](order,'VALIDATED')
                ctx['update_order_status'](order,'SENT')
                ctx['update_order_status'](order,'FILLED',price,qty)

                ctx['audit']('TRADE_OPENED',{'id':pre_trade_id,'symbol':sym,'direction':direction,'score':score,'pos':round(price*qty)})
                ctx['enqueue_persist']('trade',trade)
                if score>=ctx['ALERT_MIN_SCORE']: ctx['alert_signal'](dict(sig))
                ctx['_last_trade_opened']['stocks'] = time.time()
                ctx['log'].info(f'STK {sym} {direction} qty={qty} score={score}')
        except Exception as e:
            import traceback as _tb
            ctx['log'].error(f'stock_execution_worker: {e}\n{_tb.format_exc()[:800]}')


# ═══════════════════════════════════════════════════════════════
# [V9-1] CRYPTO AUTO-TRADE — Líneas 5774-6077 from api_server.py
# ═══════════════════════════════════════════════════════════════
def auto_trade_crypto(ctx):
    """
    Crypto signal processing and trade entry loop.

    Generates crypto trading signals from ticker and kline data, applies learning-based
    confidence filters, and executes LONG/SHORT trades with regime and conviction checks.

    Args:
        ctx: Execution context dict (from build_execution_ctx)
    """
    while True:
        ctx['beat']('auto_trade_crypto')
        time.sleep(90)
        ctx['beat']('auto_trade_crypto')
        try:
            if ctx['market_regime'].get('mode')=='HIGH_VOL':
                ctx['log'].info('[CRYPTO] HIGH_VOL regime — sizing reduced 0.6x via get_regime_multiplier')
            ctx['log'].info(f'[CRYPTO-LOOP] precos={len(ctx["crypto_prices"])} momentum={len(ctx["crypto_momentum"])} regime={ctx["market_regime"].get("mode")}')
            for sym in ctx['CRYPTO_SYMBOLS']:
                display=sym.replace('USDT',''); price=ctx['crypto_prices'].get(sym,0)
                change_24h=ctx['crypto_momentum'].get(sym,0)
                if price<=0 or abs(change_24h)<0.3:
                    ctx['log'].info(f'[CRYPTO-SKIP] {display}: price={price:.2f} change={change_24h:.2f}%')
                    continue
                direction='LONG' if change_24h>0 else 'SHORT'

                ticker_data = ctx['crypto_tickers'].get(sym, {})
                if ticker_data:
                    kline_cache_key = f'klines:{sym}'
                    klines_data = ctx['_get_cached_candles'](kline_cache_key, ttl_min=60) or {}
                    if not klines_data:
                        klines_data = ctx['_fetch_binance_klines'](sym, 22)
                        if klines_data:
                            ctx['_set_cached_candles'](kline_cache_key, klines_data)
                    score = ctx['_crypto_composite_score'](ticker_data, klines_data, direction)
                    closes_k = klines_data.get('closes', [])
                    highs_k  = klines_data.get('highs', [])
                    lows_k   = klines_data.get('lows', [])
                    vols_k   = klines_data.get('volumes', [])
                    atr_c    = ctx['_calc_atr'](closes_k, highs_k, lows_k, 14) if len(closes_k) >= 15 else 0.0
                    atr_pct_c = round((atr_c / price) * 100, 3) if price > 0 and atr_c > 0 else 0.0
                    avg_vol20_c = sum(vols_k[-20:]) / len(vols_k[-20:]) if len(vols_k) >= 20 else 0
                    vol_ratio_c = round(ticker_data.get('vol_quote', 0) / avg_vol20_c, 3) if avg_vol20_c > 0 else 0.0
                else:
                    score = min(50 + int(abs(change_24h) * 5), 95)
                    if direction == 'SHORT': score = 100 - score
                    atr_pct_c = 0.0; vol_ratio_c = 0.0

                _now_c = datetime.utcnow()
                _t_adj, _t_blocked, _t_reason = ctx['get_temporal_crypto_score'](_now_c.hour, _now_c.weekday())
                if _t_blocked:
                    ctx['log'].info(f"[CRYPTO-TBLOCK] {display}: {_t_reason}")
                    continue
                _cm_adj = ctx['get_cross_market_crypto_adj']()
                _score_before = score
                _raw_change = float(ticker_data.get('change_pct', 0))
                _strong_signal = abs(_raw_change) > 2.0
                if _strong_signal and (_t_adj + _cm_adj) < 0:
                    _capped_t = max(_t_adj + _cm_adj, -8)
                    score = max(0, min(100, score + _capped_t))
                else:
                    _total_t = _t_adj + _cm_adj
                    _capped_t = max(_total_t, -12) if _total_t < 0 else _total_t
                    score = max(0, min(100, score + _capped_t))
                _rsi_c = float(ticker_data.get('rsi',50) or 50)
                _feats_disc_c = {'score_bucket': ctx['_score_bucket'](score), 'rsi_bucket': ctx['_rsi_bucket'](_rsi_c),
                                 'weekday': str(_now_c.weekday()), 'hour_utc': str(_now_c.hour),
                                 'time_bucket': ctx['_time_bucket'](_now_c), 'asset_type': 'crypto',
                                 'market_type': 'CRYPTO', 'direction': direction,
                                 'volatility_bucket': 'LOW' if atr_pct_c<1 else ('HIGH' if atr_pct_c>3 else 'NORMAL'),
                                 'btc_trend': ctx['_cross_market_state'].get('btc_change_24h',0)>2 and 'UP' or (ctx['_cross_market_state'].get('btc_change_24h',0)<-2 and 'DOWN' or 'FLAT'),
                                 'stocks_regime': 'BAD' if ctx['_cross_market_state'].get('stocks_wr_today',50)<45 else ('GOOD' if ctx['_cross_market_state'].get('stocks_wr_today',50)>=58 else 'NEUTRAL')}
                try:
                    _disc_adj_c, _disc_blocked_c, _disc_key_c = ctx['get_composite_score_adj'](_feats_disc_c)
                except Exception as _ge:
                    _disc_adj_c, _disc_blocked_c, _disc_key_c = 0, False, ''
                if _disc_blocked_c:
                    ctx['log'].info(f"[CRYPTO-CBLOCK] {display}: {_disc_key_c}")
                    continue
                if _disc_adj_c != 0:
                    score = max(0, min(100, score + _disc_adj_c))
                if abs(_t_adj + _cm_adj) >= 5:
                    ctx['log'].info(f"[CRYPTO-SADJ] {display}: {_score_before}→{score} (t={_t_adj:+d} cm={_cm_adj:+d} disc={_disc_adj_c:+d})")

                _dd_blocked_c, _dd_reason_c = ctx['check_strategy_daily_dd']('crypto')
                if _dd_blocked_c:
                    ctx['log'].info(f'[CRYPTO-DD-BLOCK] {display}: {_dd_reason_c}')
                    break
                _bl_blocked_c, _bl_reason_c = ctx['is_symbol_blacklisted'](display)
                if _bl_blocked_c:
                    ctx['log'].info(f'[CRYPTO-BL-BLOCK] {display}: {_bl_reason_c}')
                    continue
                _dir_blocked_c, _dir_reason_c, _dir_stats_c = ctx['check_directional_exposure'](direction, 'crypto')
                if _dir_blocked_c:
                    ctx['log'].info(f'[CRYPTO-DIR-BLOCK] {display}: {_dir_reason_c}')
                    continue
                _entry_ok = score >= ctx['MIN_SCORE_AUTO_CRYPTO']
                if not _entry_ok:
                    ctx['log'].info(f'[CRYPTO-THRESHOLD] {display}: score={score} dir={direction} threshold={ctx["MIN_SCORE_AUTO_CRYPTO"]} -> BLOCKED')
                    continue

                # [v10.26] Market reversal detection for crypto
                _cpd = {'rsi': 50, 'ema9': 0, 'ema21': 0, 'ema50': 0,
                         'volume_ratio': vol_ratio_c, 'price_history': []}
                _crev, _crev_dir, _crev_cnt, _crev_det = detect_market_reversal(ctx, display, _cpd, 'crypto')
                if _crev and ctx.get('REVERSAL_BLOCK_COUNTER_TREND', True):
                    if _crev_dir == 'BEARISH_REVERSAL' and direction == 'LONG':
                        ctx['log'].info(f'[REVERSAL-BLOCK] crypto {display} LONG blocked: {_crev_det}')
                        continue
                    if _crev_dir == 'BULLISH_REVERSAL' and direction == 'SHORT':
                        ctx['log'].info(f'[REVERSAL-BLOCK] crypto {display} SHORT blocked: {_crev_det}')
                        continue

                score_factor=min(abs(score-50)/50.0,1.0)

                time_window = int(time.time() / 90)
                ms_key_c = f"CRY:{display}:{direction}:{time_window}"
                origin_key_c = ms_key_c[:120]
                with ctx['learning_lock']:
                    cached_c = ctx['processed_signal_ids'].get(ms_key_c)

                if cached_c and cached_c['reason'] in ('executed', 'kill_switch'):
                    continue

                _sig_pre_id_c = cached_c['sig_id'] if cached_c else ctx['gen_id']('SIG')
                if not cached_c:
                    with ctx['learning_lock']:
                        if len(ctx['processed_signal_ids']) >= ctx['MAX_PROCESSED_SIGNALS_CACHE']:
                            keys_to_drop = list(ctx['processed_signal_ids'].keys())[:ctx['MAX_PROCESSED_SIGNALS_CACHE'] // 2]
                            for k in keys_to_drop: del ctx['processed_signal_ids'][k]
                        ctx['processed_signal_ids'][ms_key_c] = {'sig_id': _sig_pre_id_c, 'reason': 'processing'}

                now_dt_c   = datetime.utcnow()
                dq_score_c = ctx['get_dq_score'](display)
                sig_enriched_c = {
                    'symbol': display, 'asset_type': 'crypto', 'market_type': 'CRYPTO',
                    'signal': 'COMPRA' if direction == 'LONG' else 'VENDA',
                    'score': score, 'price': price, 'rsi': 50,
                    'atr_pct': atr_pct_c,
                    'volume_ratio': vol_ratio_c,
                }
                features_c  = ctx['extract_features'](sig_enriched_c, dict(ctx['market_regime']), dq_score_c, now_dt_c)
                features_c['_dq_score'] = dq_score_c
                feat_hash_c = ctx['make_feature_hash'](features_c)
                conf_c      = ctx['calc_learning_confidence'](sig_enriched_c, features_c, feat_hash_c)
                insight_c   = ctx['generate_insight'](sig_enriched_c, features_c, feat_hash_c, conf_c)
                risk_mult_c = ctx['get_risk_multiplier'](conf_c)

                _conv_ok, _conv_reason = ctx['check_crypto_conviction'](conf_c, change_24h, display)
                if not _conv_ok:
                    ctx['log'].info(f'[CRYPTO-CONV-BLOCK] {display}: {_conv_reason}')
                    _csig_conv = ctx['record_signal_event'](sig_enriched_c, features_c, feat_hash_c, conf_c, insight_c,
                                        source_type='crypto_signal', existing_signal_id=_sig_pre_id_c,
                                        origin_signal_key=origin_key_c)
                    ctx['record_shadow_decision'](_csig_conv, sig_enriched_c, 'conviction_low')
                    with ctx['learning_lock']: ctx['processed_signal_ids'][ms_key_c] = {'sig_id': _csig_conv, 'reason': 'conviction_low'}
                    continue

                _ml_ok, _ml_reason, _ml_score = ctx['should_trade_ml'](
                    features_c, conf_c, asset_type='crypto')
                if not _ml_ok:
                    ctx['log'].info(f'[CRYPTO-ML-BLOCK] {display}: {_ml_reason} score={score}')
                    _csig_ml = ctx['record_signal_event'](sig_enriched_c, features_c, feat_hash_c, conf_c, insight_c,
                                        source_type='crypto_signal', existing_signal_id=_sig_pre_id_c,
                                        origin_signal_key=origin_key_c)
                    ctx['record_shadow_decision'](_csig_ml, sig_enriched_c, _ml_reason)
                    with ctx['learning_lock']: ctx['processed_signal_ids'][ms_key_c] = {'sig_id': _csig_ml, 'reason': _ml_reason}
                    continue

                _lc_c = conf_c.get('final_confidence', 50)
                _raw_change_c = float(ticker_data.get('change_pct', 0) if ticker_data else change_24h)
                _skip_dz_c = abs(_raw_change_c) >= 2.5 or abs(change_24h) >= 2.5
                if not _skip_dz_c and ctx['LEARNING_DEAD_ZONE_LOW'] <= _lc_c < ctx['LEARNING_DEAD_ZONE_HIGH']:
                    ctx['log'].info(f'[CRYPTO-DZ] {display}: conf={_lc_c:.1f} change={_raw_change_c:.1f}% → dead_zone BLOCK')
                    _csig_id = ctx['record_signal_event'](sig_enriched_c, features_c, feat_hash_c, conf_c, insight_c,
                                        source_type='crypto_signal', existing_signal_id=_sig_pre_id_c,
                                        origin_signal_key=origin_key_c)
                    ctx['record_shadow_decision'](_csig_id, sig_enriched_c, 'learning_dead_zone')
                    with ctx['learning_lock']: ctx['processed_signal_ids'][ms_key_c] = {'sig_id': _csig_id, 'reason': 'learning_dead_zone'}
                    continue

                _sym_max = ctx['CRYPTO_MAX_POSITION_BY_SYM'].get(sym, ctx['MAX_POSITION_CRYPTO'])
                _crypto_port_total = max(
                    ctx['crypto_capital'] + sum(t.get('position_value',0) for t in ctx['crypto_open']),
                    ctx['INITIAL_CAPITAL_CRYPTO'])
                _regime_csize_m, _regime_csl_tmp, _regime_cinfo = ctx['get_regime_multiplier']()
                _regime_csize_m = max(_regime_csize_m, 0.75)
                _crypto_pos_target = _crypto_port_total / ctx['MAX_POSITIONS_CRYPTO'] * (0.80 + score_factor * 0.20)
                _risk_mult_crypto = max(risk_mult_c, 0.6)
                _min_crypto_pos = max(100_000, _crypto_port_total / ctx['MAX_POSITIONS_CRYPTO'] * 0.15)
                desired_pos = min(max(_crypto_pos_target * _risk_mult_crypto * _regime_csize_m, _min_crypto_pos), _sym_max)
                risk_ok,risk_reason,approved_size=ctx['check_risk'](display,'CRYPTO',desired_pos,'crypto')

                if not risk_ok:
                    real_reason_c = risk_reason.split()[0] if risk_reason else 'risk_blocked'
                    is_perm_c = 'KILL_SWITCH' in risk_reason or 'DRAWDOWN' in risk_reason
                    confirmed_sig_id_c = ctx['record_signal_event'](
                        sig_enriched_c, features_c, feat_hash_c, conf_c, insight_c,
                        source_type='crypto_derived',
                        existing_signal_id=_sig_pre_id_c,
                        origin_signal_key=origin_key_c)
                    ctx['record_shadow_decision'](confirmed_sig_id_c, sig_enriched_c,
                                           'kill_switch' if is_perm_c else real_reason_c)
                    with ctx['learning_lock']:
                        ctx['processed_signal_ids'][ms_key_c] = {
                            'sig_id': confirmed_sig_id_c,
                            'reason': 'kill_switch' if is_perm_c else real_reason_c}
                    if is_perm_c: break
                    continue
                if approved_size<=0: continue

                pre_trade_id = ctx['gen_id']('CRY'); pre_order_id = ctx['gen_id']('ORD')
                order_side   = 'BUY' if direction=='LONG' else 'SELL'
                trade = None; qty = 0
                sig_id_c = ctx['record_signal_event'](
                    sig_enriched_c, features_c, feat_hash_c, conf_c, insight_c,
                    source_type='crypto_derived',
                    existing_signal_id=_sig_pre_id_c,
                    origin_signal_key=origin_key_c)
                with ctx['learning_lock']:
                    ctx['processed_signal_ids'][ms_key_c] = {'sig_id': sig_id_c, 'reason': 'executed'}

                with ctx['state_lock']:
                    ks_ok, ks_reason = ctx['kill_switch_middleware'].check_before_trade('crypto', ctx['get_db'])
                    if not ks_ok:
                        ctx['log'].warning(f'[KILL-SWITCH] Crypto blocked: {ks_reason}')
                        continue
                    risk_ok_pre, risk_reason_pre = ctx['risk_manager'].check_can_open('crypto', display, approved_size, ctx['crypto_capital'])
                    if not risk_ok_pre:
                        ctx['log'].warning(f'[RISK-BLOCK] Crypto {display}: {risk_reason_pre}')
                        continue

                    ok2,reason2=ctx['_second_validation'](display,'CRYPTO','crypto')
                    if ok2 and ctx['crypto_capital']>=approved_size:
                        qty=approved_size/price; ctx['crypto_capital']-=approved_size
                        ctx['ledger_record']('crypto', 'RESERVE', display,
                                      round(approved_size, 2), ctx['crypto_capital'], pre_trade_id)
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
                            'learning_version':    ctx['LEARNING_VERSION'],
                            '_features':           features_c,
                            '_score_snapshot':     ctx['make_score_snapshot'](sig_enriched_c, features_c, conf_c),
                            '_atr_pct':            atr_pct_c,
                        }
                        ctx['crypto_open'].append(trade)
                    else:
                        _c_block2 = reason2 if not ok2 else 'capital'
                        ctx['log'].info(f'Crypto Risk-2 {display}: {_c_block2}')
                        ctx['record_shadow_decision'](sig_id_c, sig_enriched_c, _c_block2)
                        is_perm_c = 'DUPLICATE' in (_c_block2 or '').upper()
                        with ctx['learning_lock']:
                            ctx['processed_signal_ids'][ms_key_c] = {
                                'sig_id': sig_id_c,
                                'reason': 'symbol_duplicate' if is_perm_c else _c_block2
                            }

                if trade is None: continue

                ctx['update_signal_attribution'](sig_id_c, pre_trade_id, pre_order_id)

                order=ctx['create_order'](pre_trade_id,display,order_side,'MARKET',round(qty,6),price,'crypto',
                                   order_id_override=pre_order_id)
                ctx['update_order_status'](order,'VALIDATED')
                ctx['update_order_status'](order,'SENT')
                ctx['update_order_status'](order,'FILLED',price,round(qty,6))

                ctx['_last_trade_opened']['crypto'] = time.time()
                ctx['audit']('TRADE_OPENED',{'id':pre_trade_id,'symbol':display,'direction':direction,'score':score})
                ctx['enqueue_persist']('trade',trade)
        except Exception as e:
            import traceback
            ctx['log'].error(f'auto_trade_crypto: {e}\n{traceback.format_exc()}')
