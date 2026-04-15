"""
Derivatives Strategy Scan Loops
11 derivative strategies:
  Options: PCP, FST, Roll Arb, ETF Basket, Skew, Dividend, Vol Arb
  Equity-pair: InterListed, InterListed Hedged (+FX)
  Futures: IBOV Basis, DI Calendar
MySQL-compatible: uses %s placeholders, cursor(dictionary=True), conn.close()
"""

import json
import time
import traceback
from datetime import datetime, timedelta
import statistics

from . import futures_adapter as fadapter


def _parse_expiry(expiry_val):
    """Parse expiry from string or datetime to datetime object.
    Handles ISO strings, date strings, and datetime objects."""
    if isinstance(expiry_val, datetime):
        return expiry_val
    if isinstance(expiry_val, str):
        for fmt in ('%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%Y%m%d'):
            try:
                return datetime.strptime(expiry_val.split('+')[0].split('Z')[0], fmt)
            except ValueError:
                continue
    return None


def _days_to_expiry(expiry_val):
    """Return days to expiry as int, or -1 if unparseable."""
    dt = _parse_expiry(expiry_val)
    if dt is None:
        return -1
    return (dt - datetime.now()).days


def _expiry_str(expiry_val, fmt='%Y%m%d'):
    """Format expiry as string, handling both str and datetime inputs."""
    dt = _parse_expiry(expiry_val)
    if dt is None:
        return str(expiry_val) if expiry_val else ''
    return dt.strftime(fmt)


def _expiry_date_str(expiry_val):
    """Format expiry as date string for logging."""
    dt = _parse_expiry(expiry_val)
    if dt is None:
        return str(expiry_val) if expiry_val else '?'
    return str(dt.date())


# Global diagnostic dict — read by /strategies/loop-diag endpoint
_scan_loop_diag = {}
_pcp_calibration = {}

# Exec diagnostic ring buffer — read by /strategies/exec-diagnostics
from collections import deque
_exec_diag_buffer = deque(maxlen=50)

def _exec_diag(entry: dict):
    """Append a timestamped diagnostic entry to the ring buffer."""
    entry['ts'] = datetime.now().isoformat()
    _exec_diag_buffer.append(entry)

def get_exec_diagnostics():
    """Return the last N exec diagnostic entries (newest first)."""
    return list(reversed(_exec_diag_buffer))


def _pick_nearest_strike(chain, target):
    """Return the quote in chain whose strike is closest to target. chain is {strike: quote}."""
    if not chain:
        return None, None
    try:
        best = min(chain.keys(), key=lambda k: abs(float(k) - float(target)))
    except Exception:
        return None, None
    return best, chain.get(best)


def _get_config():
    """Lazy-load derivatives config."""
    try:
        from modules.derivatives.config import get_config
        return get_config()
    except Exception:
        # Fallback defaults
        class _FallbackCfg:
            universe_tier_a = ['PETR4', 'VALE3', 'BOVA11', 'ITUB4', 'BBDC4', 'BBAS3', 'ABEV3', 'B3SA3']
            universe_tier_b = ['PETR4', 'VALE3', 'BOVA11', 'ITUB4']
            selic_rate = 14.75
            cdi_rate = 14.90
        return _FallbackCfg()


def _persist_audit_fields(get_db_fn, log, trade_id, payload):
    """Update strategy_master_trades with audit columns (v10.38).

    payload keys (all optional): instrument_type, theoretical_price, deviation_bps,
    brain_confidence, brain_adjustment, borrow_fee_estimate, hedge_trade_id,
    fair_value_inputs (dict -> JSON), audit_notes.
    """
    if not trade_id or not payload:
        return
    conn = None
    try:
        conn = get_db_fn()
        if conn is None:
            return
        cursor = conn.cursor()
        cols, vals = [], []
        _allowed = {
            'instrument_type', 'theoretical_price', 'deviation_bps',
            'brain_confidence', 'brain_adjustment', 'borrow_fee_estimate',
            'hedge_trade_id', 'fair_value_inputs', 'audit_notes',
        }
        for k, v in payload.items():
            if k not in _allowed or v is None:
                continue
            if k == 'fair_value_inputs' and isinstance(v, (dict, list)):
                v = json.dumps(v, default=str)
            cols.append(f"{k} = %s")
            vals.append(v)
        if not cols:
            return
        vals.append(trade_id)
        cursor.execute(
            f"UPDATE strategy_master_trades SET {', '.join(cols)} WHERE trade_id = %s",
            tuple(vals),
        )
        conn.commit()
        cursor.close()
    except Exception as e:
        log.warning(f"Audit persist error for {trade_id}: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _try_autonomous_execution(
    log, services_dict, strategy, symbol, structure_type,
    edge_magnitude, notional_estimate, strike=0.0, expiry='',
    legs=None, spot_price=0.0, liquidity_score=0.0, active_status_str='',
    audit_payload=None, get_db_fn=None,
):
    """
    Unified autonomous execution pipeline for all derivatives strategies.

    Pipeline: sizing → capital check → execute → (monitoring/learning automatic)

    Args:
        log: Logger
        services_dict: Dict with 'deriv_execution', 'deriv_sizer', 'deriv_learner',
                       'active_status_registry', 'capital_manager'
        strategy: Strategy name (e.g., 'pcp')
        symbol: Underlying symbol
        structure_type: Trade structure type
        edge_magnitude: Expected edge in R$
        notional_estimate: Estimated notional
        strike: Strike price
        expiry: Expiry string (YYYYMMDD)
        legs: List of leg dicts [{leg_type, symbol, qty, side, intended_price}]
        spot_price: Current spot
        liquidity_score: Current liquidity score
        active_status_str: Current tier string

    Returns:
        True if execution attempted, False otherwise
    """
    diag = {'strategy': strategy, 'symbol': symbol, 'structure': structure_type, 'steps': []}
    try:
        execution_engine = services_dict.get('deriv_execution')
        sizer = services_dict.get('deriv_sizer')
        capital_mgr = services_dict.get('capital_manager')
        learner = services_dict.get('deriv_learner')

        eng_type = type(execution_engine).__name__ if execution_engine else None
        has_execute = hasattr(execution_engine, 'execute_trade') if execution_engine else False
        diag['steps'].append(f"INIT: engine={eng_type}(has_execute_trade={has_execute}) sizer={bool(sizer)} cap={bool(capital_mgr)}")

        if not execution_engine or not sizer or not capital_mgr:
            log.info(f"[EXEC-SKIP] {strategy}/{symbol}: engine={eng_type} sizer={bool(sizer)} cap={bool(capital_mgr)}")
            diag['result'] = 'SKIP_MISSING_DEPS'
            _exec_diag(diag)
            return False

        if not has_execute:
            log.error(f"[EXEC-SKIP] {strategy}/{symbol}: engine {eng_type} has NO execute_trade method!")
            diag['result'] = f'SKIP_NO_EXECUTE_METHOD_ON_{eng_type}'
            _exec_diag(diag)
            return False

        # Get capital snapshot
        cap_snap = capital_mgr.get_snapshot()
        diag['steps'].append(f"EXEC-1: cap_avail=R${cap_snap.available:,.0f} daily_loss_rem=R${cap_snap.daily_loss_remaining:,.0f}")
        log.info(f"[EXEC-1] {strategy}/{symbol}: cap_avail=R${cap_snap.available:,.0f} daily_loss_rem=R${cap_snap.daily_loss_remaining:,.0f}")

        # Check if trading is allowed
        allowed, reason = capital_mgr.is_trading_allowed(strategy)
        if not allowed:
            log.info(f"[EXEC-2-BLOCKED] {strategy}/{symbol}: {reason}")
            diag['result'] = f'BLOCKED_TRADING_NOT_ALLOWED: {reason}'
            _exec_diag(diag)
            return False
        diag['steps'].append('EXEC-2: trading_allowed=True')

        # Get confidence adjustment from learning engine
        confidence = 0.65  # base confidence
        if learner:
            try:
                adj = learner.get_confidence_adjustment(strategy, symbol)
                confidence *= adj
            except Exception as _le:
                log.debug(f"[EXEC] learner adj error: {_le}")

        # Compute position size
        edge_bps = (edge_magnitude / notional_estimate * 10_000) if notional_estimate > 0 else 0
        diag['steps'].append(f"EXEC-3: edge_mag={edge_magnitude:.6f} notional_est={notional_estimate:.0f} edge_bps={edge_bps:.4f} conf={confidence:.3f} tier={active_status_str}")
        log.info(f"[EXEC-3] {strategy}/{symbol}: edge_mag={edge_magnitude:.6f} notional_est={notional_estimate:.0f} edge_bps={edge_bps:.4f} conf={confidence:.3f} tier={active_status_str}")
        sizing = sizer.compute_size(
            strategy=strategy,
            symbol=symbol,
            edge_bps=edge_bps,
            confidence=confidence,
            liquidity_tier=active_status_str or 'PAPER_FULL',
            capital_available=cap_snap.available,
            daily_loss_remaining=cap_snap.daily_loss_remaining,
            spot_price=spot_price,
        )
        diag['steps'].append(f"EXEC-4: sizing.notional=R${sizing.notional:,.0f} contracts={sizing.contracts} reason={sizing.reason}")
        log.info(f"[EXEC-4] {strategy}/{symbol}: sizing.notional=R${sizing.notional:,.0f} contracts={sizing.contracts} reason={sizing.reason}")

        if sizing.notional <= 0:
            diag['result'] = f'REJECTED_ZERO_NOTIONAL: reason={sizing.reason}'
            _exec_diag(diag)
            return False

        # Build legs if not provided
        if not legs:
            legs = [{
                'leg_type': 'STOCK',
                'symbol': symbol,
                'qty': sizing.contracts or 1,
                'side': 'BUY',
                'intended_price': spot_price,
            }]

        # Scale leg quantities by sizing
        if sizing.contracts > 0:
            for leg in legs:
                if leg.get('qty', 0) == 0:
                    leg['qty'] = sizing.contracts

        diag['steps'].append(f"EXEC-5: {len(legs)} legs, notional=R${sizing.notional:,.0f}")
        log.info(f"[EXEC-5] {strategy}/{symbol}: calling execute_trade with {len(legs)} legs, notional=R${sizing.notional:,.0f}")

        # Execute
        trade = execution_engine.execute_trade(
            strategy=strategy,
            symbol=symbol,
            structure_type=structure_type,
            legs=legs,
            expected_edge=edge_magnitude,
            notional=sizing.notional,
            strike=strike,
            expiry=expiry,
            liquidity_score=liquidity_score,
            active_status=active_status_str,
        )

        diag['steps'].append(f"EXEC-6: trade_id={trade.trade_id} status={trade.status.value} notional=R${sizing.notional:,.0f}")
        diag['result'] = f'EXECUTED: {trade.trade_id} / {trade.status.value}'
        _exec_diag(diag)
        log.info(
            f"[EXEC-6-DONE] {strategy}/{symbol}: "
            f"trade_id={trade.trade_id}, status={trade.status.value}, "
            f"notional=R${sizing.notional:,.0f}"
        )

        # v10.38 — persist audit fields (instrument_type, fair value, brain state)
        if audit_payload and get_db_fn:
            try:
                ap = dict(audit_payload)
                ap.setdefault('brain_confidence', round(confidence, 4))
                _persist_audit_fields(get_db_fn, log, trade.trade_id, ap)
            except Exception as _ae:
                log.debug(f"[EXEC-AUDIT] {strategy}/{symbol}: {_ae}")

        # v10.38 — feed decision to unified brain learning engine
        brain = services_dict.get('unified_brain')
        if brain and hasattr(brain, 'persist_decision'):
            try:
                brain.persist_decision(
                    decision_type='EXECUTE_TRADE',
                    module=f'derivatives.{strategy}',
                    recommendation=f'{structure_type} on {symbol}',
                    reasoning=(
                        f'edge_bps={edge_bps:.1f}, confidence={confidence:.3f}, '
                        f'notional=R${sizing.notional:,.0f}'
                    ),
                    confidence=min(100.0, max(0.0, confidence * 100.0)),
                    factors={
                        'trade_id': trade.trade_id,
                        'strategy': strategy,
                        'symbol': symbol,
                        'structure_type': structure_type,
                        'edge_bps': round(edge_bps, 2),
                        'notional': float(sizing.notional),
                        'liquidity_score': float(liquidity_score or 0),
                        'active_status': active_status_str,
                    },
                )
            except Exception as _be:
                log.debug(f"[EXEC-BRAIN] {strategy}/{symbol}: {_be}")

        return trade.trade_id if trade else True

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        diag['result'] = f'EXCEPTION: {e}'
        diag['traceback'] = tb
        _exec_diag(diag)
        log.error(f"[EXEC-ERROR] {strategy}/{symbol}: {e}\n{tb}")
        return False


def _try_autonomous_exec_generic(
    log, services_dict, strategy, symbol, structure_type,
    edge_magnitude, notional_estimate, spot_price,
    strike=0.0, expiry='', legs=None,
):
    """
    Convenience wrapper for non-PCP strategies.
    Checks tier from active_status_registry, then delegates to _try_autonomous_execution.
    """
    try:
        active_status_reg = services_dict.get('active_status_registry')
        tier_str = 'OBSERVE'
        if active_status_reg:
            tier_obj = active_status_reg.get_status(symbol, strategy.upper())
            tier_str = tier_obj.value if tier_obj else 'OBSERVE'

        if tier_str not in ('PAPER_FULL', 'PAPER_SMALL'):
            return False

        return _try_autonomous_execution(
            log, services_dict,
            strategy=strategy, symbol=symbol,
            structure_type=structure_type,
            edge_magnitude=edge_magnitude,
            notional_estimate=notional_estimate,
            strike=strike, expiry=expiry,
            legs=legs, spot_price=spot_price,
            liquidity_score=0.0,
            active_status_str=tier_str,
        )
    except Exception as e:
        log.warning(f"Generic exec wrapper error {strategy}/{symbol}: {e}")
        return False


def _b3_fees():
    """Return default B3 fee schedule."""
    return {
        'option_buy': 0.001, 'option_sell': 0.001,
        'stock_buy': 0.0005, 'stock_sell': 0.0005,
        'etf_arb': 0.003, 'interlisted': 0.005,
    }


_CAL_SAMPLES = {}

def _feed_calibration_sample(get_db_fn, log, strategy_type, symbol, edge_value, expiry=None):
    """Feed edge sample into rolling buffer and persist calibration every 5 samples."""
    try:
        if edge_value is None:
            return
        key = (strategy_type, symbol, 'edge_bps', expiry)
        buf = _CAL_SAMPLES.setdefault(key, [])
        buf.append(float(edge_value))
        if len(buf) > 100:
            buf.pop(0)
        if len(buf) >= 3 and len(buf) % 5 == 0:
            _upsert_calibration(get_db_fn, log, strategy_type, symbol, 'edge_bps', list(buf), expiry=expiry)
    except Exception as e:
        log.warning(f"_feed_calibration_sample error ({strategy_type}/{symbol}): {e}")

def _safe_insert_opportunity(get_db_fn, log, strategy_type, symbol, edge_value,
                              strike=None, expiry=None, opportunity_type=None,
                              liquidity_score=None, cost_estimate=None,
                              decision=None, rejection_reason=None):
    """Insert an opportunity into strategy_opportunities_log using MySQL."""
    _feed_calibration_sample(get_db_fn, log, strategy_type, symbol, edge_value, expiry=expiry)
    conn = None
    try:
        conn = get_db_fn()
        if conn is None:
            return
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO strategy_opportunities_log
            (strategy_type, symbol, strike, expiry, opportunity_type, expected_edge_bps,
             cost_estimate, liquidity_score, decision, rejection_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (strategy_type, symbol, strike, expiry, opportunity_type, edge_value,
             cost_estimate, liquidity_score, decision, rejection_reason)
        )
        conn.commit()
        cursor.close()
    except Exception as e:
        log.warning(f"Insert opportunity error ({strategy_type}/{symbol}): {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _upsert_calibration(get_db_fn, log, strategy_type, symbol, metric_name,
                        samples, expiry=None):
    """Persist calibration stats (mean/std/p5/p95/count) to calibration_data table."""
    if not samples or len(samples) < 2:
        return
    conn = None
    try:
        mean_v = float(statistics.mean(samples))
        std_v = float(statistics.stdev(samples)) if len(samples) > 1 else 0.0
        sorted_s = sorted(samples)
        n = len(sorted_s)
        p5 = float(sorted_s[max(0, int(0.05 * n))])
        p95 = float(sorted_s[min(n - 1, int(0.95 * n))])
        conn = get_db_fn()
        if conn is None:
            return
        cursor = conn.cursor()
        cursor.execute(
            """DELETE FROM calibration_data
               WHERE strategy_type=%s AND symbol=%s AND metric_name=%s
                 AND (expiry=%s OR (expiry IS NULL AND %s IS NULL))""",
            (strategy_type, symbol, metric_name, expiry, expiry)
        )
        cursor.execute(
            """INSERT INTO calibration_data
               (strategy_type, symbol, expiry, metric_name, mean_val, std_val,
                p5, p95, sample_count, window_start, window_end)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (strategy_type, symbol, expiry, metric_name, mean_v, std_v, p5, p95,
             n, datetime.now() - timedelta(days=1), datetime.now())
        )
        conn.commit()
        cursor.close()
    except Exception as e:
        log.warning(f"Upsert calibration error ({strategy_type}/{symbol}/{metric_name}): {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _compute_liq_score(services_dict, asset, strategy, expiry, strike, opt_q=None):
    """Safe wrapper: compute liquidity composite via LiquidityScoreEngine.compute_score."""
    try:
        # [FORENSIC] Score heuristico baseado em bid/ask/volume - o engine completo exige 
        # market_data que nao temos no scan loop (book depth, slippage, etc) e devolvia ~25 
        # pra tudo, jogando todos os assets em OBSERVE e bloqueando execucao em PAPER mode.
        if opt_q is not None:
            _bid = getattr(opt_q, 'bid', 0) or 0
            _ask = getattr(opt_q, 'ask', 0) or 0
            _vol = getattr(opt_q, 'volume', 0) or 0
            _oi  = getattr(opt_q, 'open_interest', 0) or 0
            if _bid > 0 and _ask > 0 and _ask > _bid:
                _spread_bps = ((_ask - _bid) / ((_ask + _bid) / 2)) * 10000
                _spread_score = max(0, 100 - _spread_bps / 5)  # 500bps spread = 0, 0bps = 100
                _vol_score = min(100, _vol / 10) if _vol else 50
                _oi_score = min(100, _oi / 100) if _oi else 50
                _h_score = 0.5 * _spread_score + 0.25 * _vol_score + 0.25 * _oi_score
                return round(max(15, min(100, _h_score)), 1)
        engine = services_dict.get('liquidity_engine') if services_dict else None
        if engine is None or not hasattr(engine, 'compute_score'):
            return None
        md = {}
        if opt_q is not None:
            md['bid'] = getattr(opt_q, 'bid', 0) or 0
            md['ask'] = getattr(opt_q, 'ask', 0) or 0
            md['last'] = getattr(opt_q, 'mid', (md.get('bid', 0) + md.get('ask', 0)) / 2 if md.get('bid') and md.get('ask') else 0)
            md['volume'] = getattr(opt_q, 'volume', 0) or 0
            md['open_interest'] = getattr(opt_q, 'open_interest', 0) or 0
        result = engine.compute_score(
            asset=asset, strategy=strategy,
            expiry=str(expiry or ''), strike=float(strike or 0),
            window='MID', market_data=md,
        )
        score = getattr(result, 'score', None)
        return float(score) if score is not None else None
    except Exception:
        return None


def pcp_scan_loop(beat_fn, get_db_fn, log, provider_mgr, services_dict, risk_check_fn, audit_fn):
    """
    Put-Call Parity Arbitrage Scan Loop
    B3: Calls AMERICAN, Puts EUROPEAN
    Band: S - K <= C - P <= S - PV(K) + PV(D)
    """
    loop_name = "pcp_scan_loop"

    # Startup sleep with beats
    for _ in range(5):
        beat_fn(loop_name)
        time.sleep(2)

    while True:
        try:
            beat_fn(loop_name)

            cfg = _get_config()
            fees = _b3_fees()
            eligible_assets = list(set(cfg.universe_tier_a + cfg.universe_tier_b))
            cdi_rate = cfg.cdi_rate / 100.0 if cfg.cdi_rate > 1 else cfg.cdi_rate

            opportunities_found = 0
            _diag = {'assets_checked': 0, 'spot_ok': 0, 'chains_ok': 0, 'strikes_checked': 0,
                     'liq_pass': 0, 'dte_pass': 0, 'opportunities': 0, 'errors': [], 'ts': str(datetime.now()),
                     'edge_samples': [], 'max_conv': None, 'max_rev': None}

            for asset in eligible_assets:
                try:
                    _diag['assets_checked'] += 1
                    # Get market data
                    spot_quote = provider_mgr.get_spot(asset)
                    if not spot_quote or spot_quote.bid is None:
                        continue

                    _diag['spot_ok'] += 1
                    spot_price = spot_quote.mid
                    spot_ask = spot_quote.ask
                    spot_bid = spot_quote.bid

                    # Get option chains (calls american, puts european)
                    # [FIX] min_dte=15 para pular weeklies ilíquidas (puts bid=0)
                    import os as _os3
                    _pcp_min_dte = int(_os3.environ.get('PCP_MIN_DTE', '15'))
                    call_chain = provider_mgr.get_option_chain(asset, option_type='CALL', min_dte=_pcp_min_dte)
                    put_chain = provider_mgr.get_option_chain(asset, option_type='PUT', min_dte=_pcp_min_dte)

                    if not call_chain or not put_chain:
                        continue
                    _diag['chains_ok'] += 1

                    # Extract service objects
                    greeks_calc = services_dict.get('greeks_calculator')
                    rates_service = services_dict.get('rates_curve')

                    for strike in sorted(set(call_chain.keys()) & set(put_chain.keys())):
                        call_quote = call_chain.get(strike)
                        put_quote = put_chain.get(strike)

                        if not call_quote or not put_quote:
                            continue

                        # Check liquidity filters
                        call_spread = call_quote.ask - call_quote.bid if call_quote.ask else None
                        put_spread = put_quote.ask - put_quote.bid if put_quote.ask else None

                        if not call_spread or not put_spread or call_spread > 0.02 * spot_price:
                            continue
                        _diag['liq_pass'] += 1

                        # Calculate PV(K) using CDI rate
                        days_to_expiry = _days_to_expiry(call_quote.expiry)
                        if days_to_expiry <= 0:
                            continue
                        _diag['dte_pass'] += 1
                        _diag['strikes_checked'] += 1

                        discount_factor = 1.0 / ((1 + cdi_rate) ** (days_to_expiry / 252))
                        pv_strike = strike * discount_factor

                        # Calculate dividend adjustment
                        div_adj = 0.0
                        dividend_service = services_dict.get('dividend_service')
                        _expiry_dt = _parse_expiry(call_quote.expiry)
                        if dividend_service and _expiry_dt:
                            try:
                                divs = dividend_service.get_expected_dividends(asset, datetime.now(), _expiry_dt)
                                div_adj = sum(d.get('amount', 0) for d in divs) if divs else 0.0
                            except Exception:
                                div_adj = 0.0

                        # Calculate transaction costs
                        call_cost = call_quote.ask * fees.get('option_buy', 0.001)
                        put_cost = put_quote.bid * fees.get('option_sell', 0.001)
                        stock_cost = spot_ask * fees.get('stock_buy', 0.0005)
                        import os as _os2
                        safety_factor = float(_os2.environ.get('PCP_SAFETY_FACTOR', '1.0'))
                        total_costs = (call_cost + put_cost + stock_cost) * safety_factor

                        # Check conversion edge: C_bid - P_ask - S_ask + PV(K) + div_adj
                        conversion_edge = (
                            call_quote.bid - put_quote.ask - spot_ask + pv_strike + div_adj - total_costs
                        )

                        # Check reversal edge: S_bid - C_ask + P_bid - PV(K) - div_adj
                        reversal_edge = (
                            spot_bid - call_quote.ask + put_quote.bid - pv_strike - div_adj - total_costs
                        )
                        # [FIX] Zero put bid means not executable — invalidate reversal edge
                        if (put_quote.bid or 0) <= 0:
                            reversal_edge = -9.99
                        # [FIX] Zero call bid means conversion not executable
                        if (call_quote.bid or 0) <= 0:
                            conversion_edge = -9.99
                        # [DIAG] Capture edge sample for visibility
                        if _diag['max_conv'] is None or conversion_edge > _diag['max_conv']:
                            _diag['max_conv'] = round(conversion_edge, 4)
                        if _diag['max_rev'] is None or reversal_edge > _diag['max_rev']:
                            _diag['max_rev'] = round(reversal_edge, 4)
                        if len(_diag['edge_samples']) < 8:
                            _diag['edge_samples'].append({
                                'asset': asset, 'K': float(strike), 'dte': int(days_to_expiry),
                                'cbid': round(call_quote.bid or 0, 4), 'cask': round(call_quote.ask or 0, 4),
                                'pbid': round(put_quote.bid or 0, 4), 'pask': round(put_quote.ask or 0, 4),
                                'sbid': round(spot_bid, 4), 'sask': round(spot_ask, 4),
                                'pvK': round(pv_strike, 4), 'div': round(div_adj, 4),
                                'cost': round(total_costs, 4),
                                'conv': round(conversion_edge, 4), 'rev': round(reversal_edge, 4),
                            })

                        # Collect calibration sample: observed C - P vs theoretical (S - PV(K) + div)
                        try:
                            parity_resid = (call_quote.mid - put_quote.mid) - (spot_price - pv_strike + div_adj)
                            _pcp_cal_key = f"{asset}_{_expiry_str(call_quote.expiry)}"
                            _pcp_calibration.setdefault(_pcp_cal_key, []).append(parity_resid)
                            if len(_pcp_calibration[_pcp_cal_key]) > 60:
                                _pcp_calibration[_pcp_cal_key] = _pcp_calibration[_pcp_cal_key][-60:]
                            if len(_pcp_calibration[_pcp_cal_key]) % 5 == 0:
                                _upsert_calibration(
                                    get_db_fn, log, 'PCP', asset, 'parity_residual',
                                    _pcp_calibration[_pcp_cal_key],
                                    expiry=_expiry_str(call_quote.expiry)
                                )
                        except Exception:
                            pass

                        # Compute liquidity composite + cost estimate regardless of edge
                        _liq_score = _compute_liq_score(
                            services_dict, asset, 'PCP',
                            _expiry_str(call_quote.expiry), strike, opt_q=call_quote
                        )
                        _cost_est = float(total_costs) if total_costs is not None else None

                        if conversion_edge > 0 or reversal_edge > 0:
                            edge_type = "CONVERSION" if conversion_edge > reversal_edge else "REVERSAL"
                            edge_magnitude = max(conversion_edge, reversal_edge)

                            log.info(
                                f"PCP {edge_type} opportunity: {asset} K={strike} "
                                f"expiry={_expiry_date_str(call_quote.expiry)} edge={edge_magnitude:.4f} "
                                f"liq={_liq_score} cost={_cost_est}"
                            )

                            opportunities_found += 1
                            _diag['opportunities'] += 1

                            _decision = 'CANDIDATE'
                            _rej = None
                            # [FORENSIC] era hardcoded 40. Única opportunity tinha score 25.3
                            # → rejeição perpétua. Movido p/ env var (default 15, conservador
                            # suficiente para permitir execução em papel com BOVA11 ~25).
                            import os as _os
                            _min_liq = float(_os.environ.get('DERIV_MIN_LIQUIDITY', 15))
                            if _liq_score is not None and _liq_score < _min_liq:
                                _decision = 'REJECTED'
                                _rej = f'liquidity_too_low({_liq_score:.1f}<{_min_liq})'
                            elif _cost_est is not None and edge_magnitude < _cost_est:
                                # [FORENSIC] Em PAPER mode, logamos mas nao rejeitamos; queremos ver execucao funcionar
                                if cfg.derivatives_mode == 'PAPER':
                                    _rej = f'edge_below_cost(warn)_{edge_magnitude:.4f}<{_cost_est:.4f}'
                                else:
                                    _decision = 'REJECTED'
                                    _rej = 'edge_below_cost'

                            _safe_insert_opportunity(
                                get_db_fn, log, 'PCP', asset, edge_magnitude,
                                strike=strike,
                                expiry=_expiry_str(call_quote.expiry),
                                opportunity_type=edge_type,
                                liquidity_score=_liq_score,
                                cost_estimate=_cost_est,
                                decision=_decision,
                                rejection_reason=_rej,
                            )

                            # Autonomous execution via new pipeline
                            active_status_reg = services_dict.get('active_status_registry')
                            tier_str = ''
                            if active_status_reg:
                                tier_obj = active_status_reg.get_status(asset, 'PCP')
                                tier_str = tier_obj.value if tier_obj else 'OBSERVE'

                            # Per-asset cap: max 2 open PCP positions per underlying
                            _MAX_PCP_PER_ASSET = 2
                            _cap_mgr = services_dict.get('capital_manager')
                            if _cap_mgr:
                                _asset_count = sum(
                                    1 for a in _cap_mgr.active_allocations.values()
                                    if a.strategy == 'pcp' and a.symbol == asset
                                )
                                if _asset_count >= _MAX_PCP_PER_ASSET:
                                    log.debug(f"PCP {asset}: per-asset cap reached ({_asset_count}/{_MAX_PCP_PER_ASSET})")
                                    continue

                            if tier_str in ('PAPER_FULL', 'PAPER_SMALL'):
                                # Build multi-leg order
                                expiry_str = _expiry_str(call_quote.expiry)
                                if edge_type == 'CONVERSION':
                                    legs = [
                                        {'leg_type': 'CALL', 'symbol': f'{asset}_C{strike}', 'qty': 1, 'side': 'SELL', 'intended_price': call_quote.bid},
                                        {'leg_type': 'PUT', 'symbol': f'{asset}_P{strike}', 'qty': 1, 'side': 'BUY', 'intended_price': put_quote.ask},
                                        {'leg_type': 'STOCK', 'symbol': asset, 'qty': 100, 'side': 'BUY', 'intended_price': spot_ask},
                                    ]
                                else:  # REVERSAL
                                    legs = [
                                        {'leg_type': 'CALL', 'symbol': f'{asset}_C{strike}', 'qty': 1, 'side': 'BUY', 'intended_price': call_quote.ask},
                                        {'leg_type': 'PUT', 'symbol': f'{asset}_P{strike}', 'qty': 1, 'side': 'SELL', 'intended_price': put_quote.bid},
                                        {'leg_type': 'STOCK', 'symbol': asset, 'qty': 100, 'side': 'SELL', 'intended_price': spot_bid},
                                    ]

                                _try_autonomous_execution(
                                    log, services_dict,
                                    strategy='pcp', symbol=asset,
                                    structure_type=edge_type,
                                    edge_magnitude=edge_magnitude,
                                    notional_estimate=spot_price * 100,
                                    strike=strike, expiry=expiry_str,
                                    legs=legs, spot_price=spot_price,
                                    liquidity_score=0.0,
                                    active_status_str=tier_str,
                                )

                except Exception as e:
                    log.warning(f"PCP scan error for {asset}: {e}")
                    _diag['errors'].append(f"{asset}: {str(e)[:80]}")

            _scan_loop_diag['pcp'] = _diag
            if opportunities_found > 0:
                log.info(f"PCP scan completed: {opportunities_found} opportunities")

        except Exception as e:
            log.error(f"PCP loop error: {e}\n{traceback.format_exc()}")

        time.sleep(45)


def fst_scan_loop(beat_fn, get_db_fn, log, provider_mgr, services_dict, risk_check_fn, audit_fn):
    """
    Futuro Sintetico Triangular Arbitrage
    Synthetic future = C - P + PV(K) - div_adj
    """
    loop_name = "fst_scan_loop"
    calibration_data = {}

    for _ in range(5):
        beat_fn(loop_name)
        time.sleep(2)

    while True:
        try:
            beat_fn(loop_name)

            cfg = _get_config()
            cdi_rate = cfg.cdi_rate / 100.0 if cfg.cdi_rate > 1 else cfg.cdi_rate

            for asset in cfg.universe_tier_a:
                try:
                    asset_key = f"fst_{asset}"
                    # Initialize calibration timestamp on first encounter
                    if f"{asset_key}_created" not in calibration_data:
                        calibration_data[f"{asset_key}_created"] = datetime.now()
                        log.info(f"FST {asset}: starting calibration from now")
                    created_at = calibration_data[f"{asset_key}_created"]
                    days_running = (datetime.now() - created_at).days

                    in_calibration = days_running < 1  # reduced from 5 for faster activation

                    # Get spot price
                    spot_quote = provider_mgr.get_spot(asset)
                    if not spot_quote:
                        _scan_loop_diag.setdefault('fst_data', {})[asset] = 'no_spot'
                        continue

                    spot_price = spot_quote.mid

                    # Get listed future and options
                    future_quote = provider_mgr.get_future(asset)
                    call_chain = provider_mgr.get_option_chain(asset, option_type='CALL')
                    put_chain = provider_mgr.get_option_chain(asset, option_type='PUT')

                    if not future_quote or not call_chain or not put_chain:
                        _scan_loop_diag.setdefault('fst_data', {})[asset] = f'missing: fut={bool(future_quote)} calls={bool(call_chain)} puts={bool(put_chain)}'
                        continue
                    _scan_loop_diag.setdefault('fst_data', {})[asset] = f'ok: fut={future_quote.symbol} calls={len(call_chain)} puts={len(put_chain)}'

                    # Calculate synthetic future for nearest maturity
                    future_expiry = future_quote.expiry
                    _fst_k, call_quote = _pick_nearest_strike(call_chain, spot_price)
                    put_quote = put_chain.get(_fst_k) if _fst_k is not None else None
                    _fst_strike_used = _fst_k if _fst_k is not None else _fst_strike_used

                    if not call_quote or not put_quote:
                        continue

                    days_to_expiry = _days_to_expiry(future_expiry)
                    if days_to_expiry <= 0:
                        continue

                    discount_factor = 1.0 / ((1 + cdi_rate) ** (days_to_expiry / 252))
                    pv_strike = _fst_strike_used * discount_factor

                    synthetic_future = call_quote.mid - put_quote.mid + pv_strike

                    # Calculate carry theoretical
                    dividend_service = services_dict.get('dividend_service')
                    div_sum = 0.0
                    if dividend_service:
                        try:
                            _fut_exp = _parse_expiry(future_expiry) if isinstance(future_expiry, str) else future_expiry
                            if _fut_exp:
                                divs = dividend_service.get_expected_dividends(asset, datetime.now(), _fut_exp)
                                div_sum = sum(d.get('amount', 0) for d in divs) if divs else 0.0
                        except Exception:
                            div_sum = 0.0

                    carry_theoretical = spot_price * ((1 + cdi_rate) ** (days_to_expiry / 252)) - div_sum

                    # Calculate spreads
                    spread_a = future_quote.mid - synthetic_future
                    spread_c = synthetic_future - carry_theoretical

                    # Store in calibration
                    if asset_key not in calibration_data:
                        calibration_data[asset_key] = []

                    calibration_data[asset_key].append(spread_a)
                    if len(calibration_data[asset_key]) > 20:
                        calibration_data[asset_key].pop(0)

                    # Persist calibration stats to DB every few samples so the
                    # dashboard shows non-zero calibration_records.
                    if len(calibration_data[asset_key]) >= 3 and len(calibration_data[asset_key]) % 3 == 0:
                        _upsert_calibration(
                            get_db_fn, log, 'FST', asset, 'spread_a',
                            calibration_data[asset_key],
                            expiry=_expiry_str(future_expiry)
                        )

                    if in_calibration:
                        log.info(f"FST {asset}: calibration day {days_running}/5, samples={len(calibration_data[asset_key])}, spread_a={spread_a:.4f}")
                        beat_fn(loop_name)
                        continue

                    # Check for opportunity (only after calibration phase)
                    if not in_calibration and len(calibration_data[asset_key]) > 5:
                        mean_spread = statistics.mean(calibration_data[asset_key])
                        stdev_spread = statistics.stdev(calibration_data[asset_key]) if len(calibration_data[asset_key]) > 1 else 0

                        if abs(spread_a - mean_spread) > 2 * stdev_spread and abs(spread_c - mean_spread) > stdev_spread:
                            log.info(
                                f"FST {asset} opportunity: spread_a={spread_a:.4f} "
                                f"(mean={mean_spread:.4f}), spread_c={spread_c:.4f}"
                            )

                            _fst_liq = _compute_liq_score(
                                services_dict, asset, 'FST',
                                _expiry_str(future_expiry), _fst_strike_used,
                                opt_q=call_quote
                            )
                            _fst_cost = float(abs(spread_a) * 0.002)  # 20 bps est on spread notional
                            _fst_decision = 'CANDIDATE'
                            _fst_rej = None
                            # [FORENSIC] env var, mesma justificativa do PCP
                            import os as _os
                            _min_liq = float(_os.environ.get('DERIV_MIN_LIQUIDITY', 15))
                            if _fst_liq is not None and _fst_liq < _min_liq:
                                _fst_decision = 'REJECTED'
                                _fst_rej = f'liquidity_too_low({_fst_liq:.1f}<{_min_liq})'
                            _safe_insert_opportunity(
                                get_db_fn, log, 'FST', asset, spread_a,
                                strike=_fst_strike_used,
                                expiry=_expiry_str(future_expiry),
                                opportunity_type='SPREAD_DIVERGENCE',
                                liquidity_score=_fst_liq,
                                cost_estimate=_fst_cost,
                                decision=_fst_decision,
                                rejection_reason=_fst_rej,
                            )

                            # Autonomous execution
                            _try_autonomous_exec_generic(
                                log, services_dict, 'fst', asset,
                                'SPREAD_DIVERGENCE', abs(spread_a),
                                spot_price * 100, spot_price,
                                strike=_fst_strike_used,
                                legs=[
                                    {'leg_type': 'FUTURE', 'symbol': f'{asset}_FUT', 'qty': 1,
                                     'side': 'SELL' if spread_a > 0 else 'BUY',
                                     'intended_price': future_quote.mid},
                                    {'leg_type': 'CALL', 'symbol': f'{asset}_C{_fst_strike_used}', 'qty': 1,
                                     'side': 'BUY' if spread_a > 0 else 'SELL',
                                     'intended_price': call_quote.mid},
                                    {'leg_type': 'PUT', 'symbol': f'{asset}_P{_fst_strike_used}', 'qty': 1,
                                     'side': 'SELL' if spread_a > 0 else 'BUY',
                                     'intended_price': put_quote.mid},
                                ],
                            )

                    # Check liquidity fallback
                    if future_quote.volume and future_quote.volume < 100:
                        log.warning(f"FST {asset}: low future liquidity, considering BOVA11/WIN pivot")

                except Exception as e:
                    log.warning(f"FST scan error for {asset}: {e}")

            _scan_loop_diag['fst'] = {'ts': str(datetime.now()), 'status': 'ran',
                'iteration': _scan_loop_diag.get('fst',{}).get('iteration',0)+1}

        except Exception as e:
            log.error(f"FST loop error: {e}\n{traceback.format_exc()}")
            _scan_loop_diag['fst'] = {'ts': str(datetime.now()), 'error': str(e)[:120]}

        time.sleep(50)


def roll_arb_scan_loop(beat_fn, get_db_fn, log, provider_mgr, services_dict, risk_check_fn, audit_fn):
    """
    Roll Arbitrage: Compare F2-F1 vs theoretical carry between expiries
    Focus: WIN/IND index futures
    """
    loop_name = "roll_arb_scan_loop"

    for _ in range(5):
        beat_fn(loop_name)
        time.sleep(2)

    while True:
        try:
            beat_fn(loop_name)

            cfg = _get_config()
            cdi_rate = cfg.cdi_rate / 100.0 if cfg.cdi_rate > 1 else cfg.cdi_rate
            roll_assets = ['WIN', 'IND']

            for asset in roll_assets:
                try:
                    # Get two consecutive futures
                    future_1 = provider_mgr.get_future(asset, tenor_offset=0)
                    future_2 = provider_mgr.get_future(asset, tenor_offset=1)

                    if not future_1 or not future_2:
                        _scan_loop_diag.setdefault('roll_data', {})[asset] = f'missing: f1={bool(future_1)} f2={bool(future_2)}'
                        continue
                    _scan_loop_diag.setdefault('roll_data', {})[asset] = f'ok: f1={future_1.symbol} f2={future_2.symbol}'

                    # Calculate roll cost
                    roll_cost_realized = future_2.mid - future_1.mid

                    # Calculate theoretical carry
                    days_f1 = _days_to_expiry(future_1.expiry)
                    days_f2 = _days_to_expiry(future_2.expiry)
                    days_between = days_f2 - days_f1

                    carry_theoretical = future_1.mid * cdi_rate * (days_between / 252)

                    # Check threshold
                    roll_mispricing = roll_cost_realized - carry_theoretical
                    threshold = carry_theoretical * 0.05  # 5% threshold

                    if abs(roll_mispricing) > threshold:
                        direction = "CHEAP" if roll_mispricing < 0 else "EXPENSIVE"
                        log.info(
                            f"Roll Arb {asset}: {direction} F2-F1={roll_cost_realized:.2f} "
                            f"vs carry={carry_theoretical:.2f}"
                        )

                        _safe_insert_opportunity(
                            get_db_fn, log, 'ROLL_ARB', asset, roll_mispricing,
                            opportunity_type=direction
                        )

                        # Autonomous execution
                        _try_autonomous_exec_generic(
                            log, services_dict, 'roll_arb', asset,
                            direction, abs(roll_mispricing),
                            future_1.mid * 5, future_1.mid,
                            legs=[
                                {'leg_type': 'FUTURE', 'symbol': f'{asset}_F1', 'qty': 1,
                                 'side': 'SELL' if direction == 'EXPENSIVE' else 'BUY',
                                 'intended_price': future_1.mid},
                                {'leg_type': 'FUTURE', 'symbol': f'{asset}_F2', 'qty': 1,
                                 'side': 'BUY' if direction == 'EXPENSIVE' else 'SELL',
                                 'intended_price': future_2.mid},
                            ],
                        )

                except Exception as e:
                    log.warning(f"Roll arb error for {asset}: {e}")

            _scan_loop_diag['roll_arb'] = {'ts': str(datetime.now()), 'status': 'ran',
                'iteration': _scan_loop_diag.get('roll_arb',{}).get('iteration',0)+1}

        except Exception as e:
            log.error(f"Roll Arb loop error: {e}\n{traceback.format_exc()}")
            _scan_loop_diag['roll_arb'] = {'ts': str(datetime.now()), 'error': str(e)[:120]}

        time.sleep(60)


def etf_basket_scan_loop(beat_fn, get_db_fn, log, provider_mgr, services_dict, risk_check_fn, audit_fn):
    """
    ETF vs Basket: BOVA11 NAV vs price
    """
    loop_name = "etf_basket_scan_loop"

    for _ in range(5):
        beat_fn(loop_name)
        time.sleep(2)

    while True:
        try:
            beat_fn(loop_name)

            fees = _b3_fees()
            etf_ticker = 'BOVA11'

            try:
                # Get ETF price
                etf_quote = provider_mgr.get_spot(etf_ticker)
                if not etf_quote:
                    log.warning(f"ETF Basket: no quote for {etf_ticker}")
                    time.sleep(60)
                    continue

                etf_price = etf_quote.mid

                # Calculate NAV from components
                nav_calc = services_dict.get('nav_calculator')
                _scan_loop_diag.setdefault('etf_data', {})['nav_calc'] = bool(nav_calc)
                _scan_loop_diag['etf_data']['etf_price'] = etf_price
                if nav_calc:
                    # [FORENSIC] calculate_nav exige component_prices; sem basket registrado, skip silencioso
                    try:
                        nav_value = nav_calc.calculate_nav(etf_ticker, {})
                    except TypeError:
                        nav_value = 0

                    if nav_value:
                        divergence = (etf_price - nav_value) / nav_value
                        divergence_pct = divergence * 100

                        cost_threshold = fees.get('etf_arb', 0.003)

                        if abs(divergence_pct) > cost_threshold * 100:
                            direction = "PREMIUM" if divergence > 0 else "DISCOUNT"
                            log.info(
                                f"ETF Basket {etf_ticker}: {direction} "
                                f"price={etf_price:.2f} NAV={nav_value:.2f} "
                                f"div={divergence_pct:.3f}%"
                            )

                            _safe_insert_opportunity(
                                get_db_fn, log, 'ETF_BASKET', etf_ticker, divergence,
                                opportunity_type=direction
                            )

                            # Autonomous execution
                            _try_autonomous_exec_generic(
                                log, services_dict, 'etf_basket', etf_ticker,
                                direction, abs(divergence) * etf_price,
                                etf_price * 1000, etf_price,
                                legs=[
                                    {'leg_type': 'STOCK', 'symbol': etf_ticker, 'qty': 1000,
                                     'side': 'SELL' if direction == 'PREMIUM' else 'BUY',
                                     'intended_price': etf_price},
                                ],
                            )

                # Optional: BOVA11 vs EWZ with FX hedge
                try:
                    ewz_quote = provider_mgr.get_spot('EWZ')
                    usdbrl_quote = provider_mgr.get_spot('USDBRL')

                    if ewz_quote and usdbrl_quote:
                        ewz_brl_equivalent = ewz_quote.mid * usdbrl_quote.mid
                        basis = etf_price - ewz_brl_equivalent

                        if abs(basis) > ewz_quote.mid * 0.02:
                            log.info(f"ETF Basket: BOVA11 vs EWZ basis={basis:.2f}")

                except Exception:
                    pass

            except Exception as e:
                log.warning(f"ETF Basket scan error: {e}")

            _scan_loop_diag['etf_basket'] = {'ts': str(datetime.now()), 'status': 'ran',
                'iteration': _scan_loop_diag.get('etf_basket',{}).get('iteration',0)+1}

        except Exception as e:
            log.error(f"ETF Basket loop error: {e}\n{traceback.format_exc()}")
            _scan_loop_diag['etf_basket'] = {'ts': str(datetime.now()), 'error': str(e)[:120]}

        time.sleep(55)


def skew_arb_scan_loop(beat_fn, get_db_fn, log, provider_mgr, services_dict, risk_check_fn, audit_fn):
    """
    Volatility Skew Arbitrage: Track IV difference OTM put vs OTM call
    """
    loop_name = "skew_arb_scan_loop"
    skew_history = {}

    for _ in range(5):
        beat_fn(loop_name)
        time.sleep(2)

    while True:
        try:
            beat_fn(loop_name)

            cfg = _get_config()

            for asset in cfg.universe_tier_a:
                try:
                    spot_quote = provider_mgr.get_spot(asset)
                    if not spot_quote:
                        continue

                    spot_price = spot_quote.mid

                    # Get OTM puts and calls
                    call_chain = provider_mgr.get_option_chain(asset, option_type='CALL')
                    put_chain = provider_mgr.get_option_chain(asset, option_type='PUT')

                    if not call_chain or not put_chain:
                        continue

                    # Pick 1-delta OTM call and put
                    otm_call_strike, _skew_call_q = _pick_nearest_strike(call_chain, spot_price * 1.05)
                    otm_put_strike, _skew_put_q = _pick_nearest_strike(put_chain, spot_price * 0.95)
                    if otm_call_strike is None or otm_put_strike is None:
                        continue

                    call_quote = call_chain.get(otm_call_strike)
                    put_quote = put_chain.get(otm_put_strike)

                    if not call_quote or not put_quote:
                        continue

                    # Extract IV
                    greeks_calc = services_dict.get('greeks_calculator')
                    _scan_loop_diag.setdefault('skew_data', {})[asset] = f'chains_ok, greeks_calc={bool(greeks_calc)}'
                    if greeks_calc:
                        _call_dte = _days_to_expiry(call_quote.expiry)
                        _put_dte = _days_to_expiry(put_quote.expiry)
                        if _call_dte <= 0 or _put_dte <= 0:
                            continue
                        try:
                            call_iv = greeks_calc.calculate_iv_newton_raphson(
                                'call', spot_price, otm_call_strike, _call_dte, call_quote.mid
                            )
                            put_iv = greeks_calc.calculate_iv_newton_raphson(
                                'put', spot_price, otm_put_strike, _put_dte, put_quote.mid
                            )
                        except Exception:
                            continue

                        # Calculate risk reversal
                        rr = put_iv - call_iv

                        asset_key = f"skew_{asset}"
                        if asset_key not in skew_history:
                            skew_history[asset_key] = []

                        skew_history[asset_key].append(rr)
                        if len(skew_history[asset_key]) > 30:
                            skew_history[asset_key].pop(0)

                        # Check z-score
                        if len(skew_history[asset_key]) > 10:
                            mean_rr = statistics.mean(skew_history[asset_key])
                            stdev_rr = statistics.stdev(skew_history[asset_key])
                            z_score = (rr - mean_rr) / stdev_rr if stdev_rr > 0 else 0

                            if abs(z_score) > 2.0:
                                direction = "SKEW_HIGH_PUT" if rr > mean_rr else "SKEW_HIGH_CALL"
                                log.info(
                                    f"Skew {asset}: {direction} RR={rr:.4f} "
                                    f"z-score={z_score:.2f}"
                                )

                                _safe_insert_opportunity(
                                    get_db_fn, log, 'SKEW_ARB', asset, z_score,
                                    opportunity_type=direction
                                )

                                # Autonomous execution: risk reversal trade
                                if direction == 'SKEW_HIGH_PUT':
                                    skew_legs = [
                                        {'leg_type': 'PUT', 'symbol': f'{asset}_P{otm_put_strike}', 'qty': 1,
                                         'side': 'SELL', 'intended_price': put_quote.mid},
                                        {'leg_type': 'CALL', 'symbol': f'{asset}_C{otm_call_strike}', 'qty': 1,
                                         'side': 'BUY', 'intended_price': call_quote.mid},
                                    ]
                                else:
                                    skew_legs = [
                                        {'leg_type': 'PUT', 'symbol': f'{asset}_P{otm_put_strike}', 'qty': 1,
                                         'side': 'BUY', 'intended_price': put_quote.mid},
                                        {'leg_type': 'CALL', 'symbol': f'{asset}_C{otm_call_strike}', 'qty': 1,
                                         'side': 'SELL', 'intended_price': call_quote.mid},
                                    ]

                                _try_autonomous_exec_generic(
                                    log, services_dict, 'skew_arb', asset,
                                    direction, abs(z_score) * spot_price * 0.01,
                                    spot_price * 100, spot_price,
                                    strike=otm_call_strike, legs=skew_legs,
                                )

                except Exception as e:
                    log.warning(f"Skew scan error for {asset}: {e}")

            _scan_loop_diag['skew_arb'] = {'ts': str(datetime.now()), 'status': 'ran',
                'iteration': _scan_loop_diag.get('skew_arb',{}).get('iteration',0)+1}

        except Exception as e:
            log.error(f"Skew loop error: {e}\n{traceback.format_exc()}")
            _scan_loop_diag['skew_arb'] = {'ts': str(datetime.now()), 'error': str(e)[:120]}

        time.sleep(52)


def interlisted_scan_loop(beat_fn, get_db_fn, log, provider_mgr, services_dict, risk_check_fn, audit_fn):
    """
    Inter-Listed ADR/B3: PETR4 vs PBR, VALE3 vs VALE with FX hedge
    """
    loop_name = "interlisted_scan_loop"

    for _ in range(5):
        beat_fn(loop_name)
        time.sleep(2)

    while True:
        try:
            beat_fn(loop_name)

            fees = _b3_fees()
            pairs = [('PETR4', 'PBR'), ('VALE3', 'VALE')]

            for b3_ticker, adr_ticker in pairs:
                try:
                    b3_quote = provider_mgr.get_spot(b3_ticker)
                    adr_quote = provider_mgr.get_spot(adr_ticker)
                    usdbrl_quote = provider_mgr.get_spot('USDBRL')

                    if not b3_quote or not adr_quote or not usdbrl_quote:
                        _scan_loop_diag.setdefault('inter_data', {})[b3_ticker] = f'missing: b3={bool(b3_quote)} adr={bool(adr_quote)} fx={bool(usdbrl_quote)}'
                        continue

                    _scan_loop_diag.setdefault('inter_data', {})[b3_ticker] = f'ok: b3={b3_quote.mid:.2f} adr={adr_quote.mid:.2f} fx={usdbrl_quote.mid:.4f}'
                    # Calculate basis
                    adr_in_brl = adr_quote.mid * usdbrl_quote.mid
                    basis = b3_quote.mid - adr_in_brl
                    basis_pct = basis / adr_in_brl

                    threshold = fees.get('interlisted', 0.005)

                    if abs(basis_pct) > threshold:
                        direction = "B3_RICH" if basis > 0 else "ADR_RICH"
                        log.info(
                            f"InterListed {b3_ticker}/{adr_ticker}: {direction} "
                            f"basis={basis:.2f} ({basis_pct*100:.3f}%)"
                        )

                        _safe_insert_opportunity(
                            get_db_fn, log, 'INTERLISTED',
                            f"{b3_ticker}/{adr_ticker}", basis_pct,
                            opportunity_type=direction
                        )

                        # Autonomous execution
                        _try_autonomous_exec_generic(
                            log, services_dict, 'interlisted', b3_ticker,
                            direction, abs(basis),
                            b3_quote.mid * 100, b3_quote.mid,
                            legs=[
                                {'leg_type': 'STOCK', 'symbol': b3_ticker, 'qty': 100,
                                 'side': 'SELL' if direction == 'B3_RICH' else 'BUY',
                                 'intended_price': b3_quote.mid},
                                {'leg_type': 'STOCK', 'symbol': adr_ticker, 'qty': 100,
                                 'side': 'BUY' if direction == 'B3_RICH' else 'SELL',
                                 'intended_price': adr_quote.mid},
                            ],
                        )

                except Exception as e:
                    log.warning(f"InterListed scan error for {b3_ticker}/{adr_ticker}: {e}")

            _scan_loop_diag['interlisted'] = {'ts': str(datetime.now()), 'status': 'ran',
                'iteration': _scan_loop_diag.get('interlisted',{}).get('iteration',0)+1}

        except Exception as e:
            log.error(f"InterListed loop error: {e}\n{traceback.format_exc()}")
            _scan_loop_diag['interlisted'] = {'ts': str(datetime.now()), 'error': str(e)[:120]}

        time.sleep(58)


def dividend_arb_scan_loop(beat_fn, get_db_fn, log, provider_mgr, services_dict, risk_check_fn, audit_fn):
    """
    Dividend Arbitrage: Monitor dividend calendar, calculate buy+put vs dividend capture
    """
    loop_name = "dividend_arb_scan_loop"

    for _ in range(5):
        beat_fn(loop_name)
        time.sleep(2)

    while True:
        try:
            beat_fn(loop_name)

            cfg = _get_config()
            fees = _b3_fees()
            dividend_service = services_dict.get('dividend_service')
            _scan_loop_diag.setdefault('div_data', {})['dividend_service'] = bool(dividend_service)

            if not dividend_service:
                _scan_loop_diag['div_data']['blocked'] = 'no_dividend_service'
                time.sleep(60)
                continue

            for asset in cfg.universe_tier_a:
                try:
                    spot_quote = provider_mgr.get_spot(asset)
                    if not spot_quote:
                        continue

                    spot_price = spot_quote.mid

                    # Get upcoming dividends (next 60 days)
                    try:
                        upcoming_divs = dividend_service.get_expected_dividends(
                            asset, datetime.now(), datetime.now() + timedelta(days=60)
                        )
                    except Exception:
                        upcoming_divs = []

                    for dividend in (upcoming_divs or []):
                        _ex_date = dividend.get('ex_date') if isinstance(dividend, dict) else getattr(dividend, 'ex_date', None)
                        if not _ex_date:
                            continue
                        _ex_dt = _parse_expiry(_ex_date) if isinstance(_ex_date, str) else _ex_date
                        if not _ex_dt:
                            continue
                        days_to_ex = (_ex_dt - datetime.now()).days
                        if days_to_ex <= 0 or days_to_ex > 60:
                            continue

                        # Get put hedge cost
                        put_chain = provider_mgr.get_option_chain(asset, option_type='PUT')
                        if not put_chain:
                            continue

                        strike, _div_put_q = _pick_nearest_strike(put_chain, spot_price)
                        if strike is None: continue
                        put_quote = put_chain.get(strike)
                        if not put_quote:
                            continue

                        put_cost = put_quote.mid

                        # Calculate net edge
                        buy_cost = spot_price * fees.get('stock_buy', 0.0005)
                        put_hedge_cost = put_cost * fees.get('option_buy', 0.001)

                        _div_amount = dividend.get('amount', 0) if isinstance(dividend, dict) else getattr(dividend, 'amount', 0)
                        net_edge = _div_amount - buy_cost - put_hedge_cost

                        if net_edge > 0:
                            log.info(
                                f"Dividend Arb {asset}: ex-date={_ex_dt} "
                                f"dividend={_div_amount:.2f} net_edge={net_edge:.4f}"
                            )

                            _safe_insert_opportunity(
                                get_db_fn, log, 'DIVIDEND_ARB', asset, net_edge,
                                opportunity_type='DIVIDEND_CAPTURE'
                            )

                            # Autonomous execution: buy stock + buy put hedge
                            _try_autonomous_exec_generic(
                                log, services_dict, 'dividend_arb', asset,
                                'DIVIDEND_CAPTURE', net_edge,
                                spot_price * 100, spot_price,
                                strike=strike,
                                legs=[
                                    {'leg_type': 'STOCK', 'symbol': asset, 'qty': 100,
                                     'side': 'BUY', 'intended_price': spot_price},
                                    {'leg_type': 'PUT', 'symbol': f'{asset}_P{strike}', 'qty': 1,
                                     'side': 'BUY', 'intended_price': put_cost},
                                ],
                            )

                except Exception as e:
                    log.warning(f"Dividend arb error for {asset}: {e}")

            _scan_loop_diag['dividend_arb'] = {'ts': str(datetime.now()), 'status': 'ran',
                'iteration': _scan_loop_diag.get('dividend_arb',{}).get('iteration',0)+1}

        except Exception as e:
            log.error(f"Dividend Arb loop error: {e}\n{traceback.format_exc()}")
            _scan_loop_diag['dividend_arb'] = {'ts': str(datetime.now()), 'error': str(e)[:120]}

        time.sleep(48)


def vol_arb_scan_loop(beat_fn, get_db_fn, log, provider_mgr, services_dict, risk_check_fn, audit_fn):
    """
    Volatility Arbitrage: realized vol vs IV, with delta-hedge cost estimation
    """
    loop_name = "vol_arb_scan_loop"
    realized_vol_history = {}

    for _ in range(5):
        beat_fn(loop_name)
        time.sleep(2)

    while True:
        try:
            beat_fn(loop_name)

            cfg = _get_config()
            fees = _b3_fees()

            for asset in cfg.universe_tier_a:
                try:
                    # Get price history for realized vol
                    spot_prices = provider_mgr.get_price_history(asset, lookback_days=60)
                    if not spot_prices or len(spot_prices) < 20:
                        _scan_loop_diag.setdefault('vol_data', {})[asset] = f'no_history: got={len(spot_prices) if spot_prices else 0}'
                        continue
                    # [FORENSIC] providers devolvem list[dict] com chave close; extrair floats
                    if spot_prices and isinstance(spot_prices[0], dict):
                        spot_prices = [float(p.get("close") or p.get("c") or 0) for p in spot_prices]
                        spot_prices = [p for p in spot_prices if p > 0]
                        if len(spot_prices) < 20:
                            continue

                    # Calculate 20-day and 60-day realized vol
                    returns_20 = [
                        (spot_prices[i] - spot_prices[i-1]) / spot_prices[i-1]
                        for i in range(max(1, len(spot_prices)-20), len(spot_prices))
                    ]
                    returns_60 = [
                        (spot_prices[i] - spot_prices[i-1]) / spot_prices[i-1]
                        for i in range(1, len(spot_prices))
                    ]

                    if returns_20 and returns_60:
                        rv_20 = statistics.stdev(returns_20) * (252 ** 0.5)
                        rv_60 = statistics.stdev(returns_60) * (252 ** 0.5)

                        # Get current IV
                        call_chain = provider_mgr.get_option_chain(asset, option_type='CALL')
                        if not call_chain:
                            continue

                        spot_quote = provider_mgr.get_spot(asset)
                        if not spot_quote:
                            continue

                        spot_price = spot_quote.mid
                        _vol_k, call_quote = _pick_nearest_strike(call_chain, spot_price)
                        _vol_strike_used = _vol_k if _vol_k is not None else _vol_strike_used

                        if not call_quote:
                            continue

                        greeks_calc = services_dict.get('greeks_calculator')
                        if greeks_calc:
                            _vol_dte = _days_to_expiry(call_quote.expiry)
                            if _vol_dte <= 0:
                                continue
                            try:
                                iv = greeks_calc.calculate_iv_newton_raphson(
                                    'call', spot_price, _vol_strike_used, _vol_dte, call_quote.mid
                                )
                            except Exception:
                                continue

                            # Calculate IV/RV ratios
                            iv_rv_20 = iv / rv_20 if rv_20 > 0 else 0
                            iv_rv_60 = iv / rv_60 if rv_60 > 0 else 0

                            # Estimate delta-hedge cost
                            try:
                                greeks_data = greeks_calc.calculate_greeks('call', spot_price, _vol_strike_used, _vol_dte, iv)
                                delta = greeks_data.delta if hasattr(greeks_data, 'delta') else 0.5
                            except Exception:
                                delta = 0.5

                            hedge_cost_pct = delta * 2 * fees.get('stock_buy', 0.0005)

                            # Signal opportunity
                            if iv > rv_60 * 1.5:  # IV significantly above RV
                                log.info(
                                    f"Vol Arb {asset}: SELL (IV high) IV={iv:.2%} RV60={rv_60:.2%} "
                                    f"hedge_cost={hedge_cost_pct:.3%}"
                                )

                                _safe_insert_opportunity(
                                    get_db_fn, log, 'VOL_ARB', asset, iv_rv_60 - 1,
                                    opportunity_type='IV_HIGH_SELL'
                                )

                                # Autonomous execution: sell straddle
                                _try_autonomous_exec_generic(
                                    log, services_dict, 'vol_arb', asset,
                                    'IV_HIGH_SELL', (iv - rv_60) * spot_price,
                                    spot_price * 100, spot_price,
                                    strike=_vol_strike_used,
                                    legs=[
                                        {'leg_type': 'CALL', 'symbol': f'{asset}_C{_vol_strike_used}', 'qty': 1,
                                         'side': 'SELL', 'intended_price': call_quote.mid},
                                        {'leg_type': 'STOCK', 'symbol': asset,
                                         'qty': int(abs(delta) * 100) or 50,
                                         'side': 'BUY', 'intended_price': spot_price},
                                    ],
                                )

                            elif iv < rv_60 * 0.8:  # IV significantly below RV
                                log.info(
                                    f"Vol Arb {asset}: BUY (IV low) IV={iv:.2%} RV60={rv_60:.2%} "
                                    f"hedge_cost={hedge_cost_pct:.3%}"
                                )

                                _safe_insert_opportunity(
                                    get_db_fn, log, 'VOL_ARB', asset, 1 - iv_rv_60,
                                    opportunity_type='IV_LOW_BUY'
                                )

                                # Autonomous execution: buy straddle
                                _try_autonomous_exec_generic(
                                    log, services_dict, 'vol_arb', asset,
                                    'IV_LOW_BUY', (rv_60 - iv) * spot_price,
                                    spot_price * 100, spot_price,
                                    strike=_vol_strike_used,
                                    legs=[
                                        {'leg_type': 'CALL', 'symbol': f'{asset}_C{_vol_strike_used}', 'qty': 1,
                                         'side': 'BUY', 'intended_price': call_quote.mid},
                                        {'leg_type': 'STOCK', 'symbol': asset,
                                         'qty': int(abs(delta) * 100) or 50,
                                         'side': 'SELL', 'intended_price': spot_price},
                                    ],
                                )

                except Exception as e:
                    log.warning(f"Vol arb error for {asset}: {e}")

            _scan_loop_diag['vol_arb'] = {'ts': str(datetime.now()), 'status': 'ran'}

        except Exception as e:
            log.error(f"Vol Arb loop error: {e}\n{traceback.format_exc()}")
            _scan_loop_diag['vol_arb'] = {'ts': str(datetime.now()), 'error': str(e)[:120]}

        time.sleep(56)
def ibov_basis_scan_loop(beat_fn, get_db_fn, log, provider_mgr, services_dict, risk_check_fn, audit_fn):
    """IBOV_BASIS — spot-future arbitrage on Ibovespa.

    Compares market WIN/IND futures price vs cost-of-carry fair value
    derived from BOVA11 (or index proxy) spot, CDI rate and dividend yield.

    Direction:
      * BASIS_RICH  : market future > fair + threshold  → SELL future + BUY spot
      * BASIS_CHEAP : market future < fair - threshold  → BUY  future + SELL (short) spot
    """
    loop_name = "ibov_basis_scan_loop"
    strategy = 'ibov_basis'
    strategy_upper = 'IBOV_BASIS'
    proxy_spot_symbol = 'BOVA11'         # B3 Ibovespa ETF (liquid proxy)
    future_underlying = 'IBOV'
    _basis_window = {}                    # {ym: [bps residuals]} for calibration band

    for _ in range(5):
        beat_fn(loop_name); time.sleep(2)

    while True:
        try:
            beat_fn(loop_name)
            cfg = _get_config()
            fees = _b3_fees()
            cdi_rate = cfg.cdi_rate / 100.0 if cfg.cdi_rate > 1 else cfg.cdi_rate
            threshold_bps = 15.0         # minimum deviation to flag an opportunity

            _diag = {'ts': str(datetime.now()), 'checks': 0, 'opportunities': 0, 'errors': []}

            try:
                spot_quote = provider_mgr.get_spot(proxy_spot_symbol)
            except Exception as e:
                log.warning(f"IBOV_BASIS spot fetch error: {e}")
                spot_quote = None

            if not spot_quote or not spot_quote.bid:
                _scan_loop_diag['ibov_basis'] = {**_diag, 'status': 'no_spot'}
                time.sleep(60); continue

            spot_price = spot_quote.mid
            spot_ask = spot_quote.ask
            spot_bid = spot_quote.bid

            # Dividend yield — default table or from dividend service
            dy = fadapter.default_div_yield(proxy_spot_symbol) if fadapter else 0.06
            try:
                dsvc = services_dict.get('dividend_service')
                if dsvc and hasattr(dsvc, 'get_forward_yield'):
                    dy_override = dsvc.get_forward_yield(proxy_spot_symbol)
                    if dy_override is not None:
                        dy = float(dy_override)
            except Exception:
                pass

            # Iterate over nearest + next future tenors
            for tenor_offset in (0, 1):
                try:
                    f_quote = provider_mgr.get_future(future_underlying, tenor_offset=tenor_offset)
                except Exception:
                    f_quote = None
                if not f_quote or not f_quote.bid:
                    continue

                _diag['checks'] += 1
                market_future = f_quote.mid
                if market_future <= 0:
                    continue

                expiry_str = _expiry_str(f_quote.expiry)
                du = fadapter.du_until(_parse_expiry(f_quote.expiry) or datetime.now()) if fadapter else 21
                if du <= 0:
                    continue

                fair = fadapter.fair_future_price(spot_price, cdi_rate, du, dy) if fadapter \
                       else spot_price * ((1 + (cdi_rate - dy)) ** (du / 252.0))
                dev_bps = fadapter.basis_bps(market_future, fair) if fadapter else \
                          (market_future - fair) / fair * 10_000.0

                # Rolling calibration band
                ym = expiry_str[:6] if expiry_str else 'nx'
                _basis_window.setdefault(ym, []).append(dev_bps)
                if len(_basis_window[ym]) > 120:
                    _basis_window[ym] = _basis_window[ym][-120:]
                if len(_basis_window[ym]) % 10 == 0:
                    _upsert_calibration(get_db_fn, log, strategy_upper,
                                        future_underlying, 'basis_bps',
                                        _basis_window[ym], expiry=expiry_str)

                # Cost estimate — round-trip on both legs
                futures_fee_bps = 5.0      # B3 futures emoluments ~5 bps round-trip
                etf_fee_bps = fees.get('stock_buy', 0.0005) * 2 * 10_000  # ~10 bps round-trip
                slippage_bps = 8.0
                total_cost_bps = futures_fee_bps + etf_fee_bps + slippage_bps

                net_edge_bps = abs(dev_bps) - total_cost_bps
                notional_estimate = spot_price * 100  # index points * basic multiplier

                if abs(dev_bps) < threshold_bps:
                    continue

                direction = 'BASIS_RICH' if dev_bps > 0 else 'BASIS_CHEAP'
                _diag['opportunities'] += 1

                log.info(
                    f"IBOV_BASIS {direction} tenor={tenor_offset} expiry={expiry_str} "
                    f"fair={fair:.1f} mkt={market_future:.1f} dev={dev_bps:.1f}bps "
                    f"net={net_edge_bps:.1f}bps cdi={cdi_rate:.4f} dy={dy:.3f} du={du}"
                )

                _safe_insert_opportunity(
                    get_db_fn, log, strategy_upper, future_underlying,
                    net_edge_bps,
                    expiry=expiry_str,
                    opportunity_type=direction,
                    cost_estimate=total_cost_bps,
                    decision='CANDIDATE' if net_edge_bps > 0 else 'REJECTED',
                    rejection_reason=None if net_edge_bps > 0 else 'edge_below_cost',
                )

                if net_edge_bps <= 0:
                    continue

                # Tier gate
                active_status_reg = services_dict.get('active_status_registry')
                tier_str = 'OBSERVE'
                if active_status_reg:
                    tier_obj = active_status_reg.get_status(future_underlying, strategy_upper)
                    tier_str = tier_obj.value if tier_obj else 'OBSERVE'
                if tier_str not in ('PAPER_FULL', 'PAPER_SMALL'):
                    continue

                # Confidence from learner
                conf_adj = _get_confidence_adj(services_dict, strategy, future_underlying)

                # Build legs — WIN contracts vs BOVA11 spot
                if direction == 'BASIS_RICH':
                    legs = [
                        {'leg_type': 'FUTURE', 'symbol': f'WIN{expiry_str[:6]}',
                         'qty': 1, 'side': 'SELL', 'intended_price': f_quote.bid},
                        {'leg_type': 'STOCK', 'symbol': proxy_spot_symbol,
                         'qty': 100, 'side': 'BUY', 'intended_price': spot_ask},
                    ]
                else:
                    legs = [
                        {'leg_type': 'FUTURE', 'symbol': f'WIN{expiry_str[:6]}',
                         'qty': 1, 'side': 'BUY', 'intended_price': f_quote.ask},
                        {'leg_type': 'STOCK', 'symbol': proxy_spot_symbol,
                         'qty': 100, 'side': 'SELL', 'intended_price': spot_bid},
                    ]

                audit = {
                    'instrument_type': 'future',
                    'theoretical_price': round(fair, 4),
                    'deviation_bps': round(dev_bps, 2),
                    'fair_value_inputs': fadapter.fair_value_snapshot(
                        spot=spot_price, cdi=cdi_rate, dy=dy, du=du,
                        market_price=market_future,
                    ) if fadapter else {
                        'spot': spot_price, 'cdi': cdi_rate, 'dy': dy,
                        'du': du, 'market_future': market_future, 'fair': fair,
                    },
                    'audit_notes': (
                        f'IBOV_BASIS {direction}: mkt={market_future:.2f} fair={fair:.2f} '
                        f'dev={dev_bps:.1f}bps net={net_edge_bps:.1f}bps'
                    ),
                }

                _try_autonomous_execution(
                    log, services_dict,
                    strategy=strategy, symbol=future_underlying,
                    structure_type=direction,
                    edge_magnitude=abs(market_future - fair),
                    notional_estimate=notional_estimate,
                    strike=0.0, expiry=expiry_str,
                    legs=legs, spot_price=spot_price,
                    liquidity_score=0.0,
                    active_status_str=tier_str,
                    audit_payload=audit,
                    get_db_fn=get_db_fn,
                )

            _scan_loop_diag['ibov_basis'] = {**_diag, 'status': 'ran'}

        except Exception as e:
            log.error(f"IBOV_BASIS loop error: {e}\n{traceback.format_exc()}")
            _scan_loop_diag['ibov_basis'] = {'ts': str(datetime.now()), 'error': str(e)[:120]}

        time.sleep(55)


def di_calendar_scan_loop(beat_fn, get_db_fn, log, provider_mgr, services_dict, risk_check_fn, audit_fn):
    """DI_CALENDAR — calendar spread between two DI1 vertices.

    Compares short-tenor DI1 (t1) vs long-tenor DI1 (t2). The implied forward
    rate between them is compared to the Anbima curve spot forward; when the
    deviation exceeds a calibrated band, the loop opens a DV01-neutral spread.

    Direction:
      * FWD_HIGH : implied forward > fair + band  → RECEIVE t1, PAY t2
      * FWD_LOW  : implied forward < fair - band  → PAY t1, RECEIVE t2
    """
    loop_name = "di_calendar_scan_loop"
    strategy = 'di_calendar'
    strategy_upper = 'DI_CALENDAR'
    _di_cal_window = {}   # {pair_label: [fwd_dev_bps samples]}

    for _ in range(5):
        beat_fn(loop_name); time.sleep(2)

    while True:
        try:
            beat_fn(loop_name)
            _diag = {'ts': str(datetime.now()), 'pairs_checked': 0, 'opportunities': 0, 'errors': []}

            # Fetch DI1 futures curve via provider manager
            try:
                p = provider_mgr._resolve() if hasattr(provider_mgr, '_resolve') else None
                futures_list = p.get_futures('DI1') if p else []
            except Exception as e:
                log.warning(f"DI_CALENDAR get_futures error: {e}")
                futures_list = []

            if not futures_list or len(futures_list) < 2:
                _scan_loop_diag['di_calendar'] = {**_diag, 'status': 'insufficient_curve'}
                time.sleep(60); continue

            today = datetime.now().date()

            # Sort by expiry ascending, drop expired
            valid = []
            for fq in futures_list:
                dt = _parse_expiry(fq.expiry)
                if dt and dt.date() > today and (fq.mid or 0) > 0:
                    valid.append((dt.date(), fq))
            valid.sort(key=lambda t: t[0])
            if len(valid) < 2:
                _scan_loop_diag['di_calendar'] = {**_diag, 'status': 'insufficient_curve'}
                time.sleep(60); continue

            for i in range(len(valid) - 1):
                try:
                    d1, fq1 = valid[i]
                    d2, fq2 = valid[i + 1]
                    _diag['pairs_checked'] += 1

                    # Convert PU to rate (fq.mid expected to already be a rate if provider gives it;
                    # keep both paths to tolerate provider conventions).
                    mid1 = float(fq1.mid)
                    mid2 = float(fq2.mid)
                    r1 = mid1 if mid1 < 1.0 else (mid1 / 100.0 if mid1 < 100.0 else
                          (fadapter.di1_rate_from_pu(mid1, fadapter.du_until(d1)) if fadapter else 0.0))
                    r2 = mid2 if mid2 < 1.0 else (mid2 / 100.0 if mid2 < 100.0 else
                          (fadapter.di1_rate_from_pu(mid2, fadapter.du_until(d2)) if fadapter else 0.0))
                    if r1 <= 0 or r2 <= 0:
                        continue

                    du1 = fadapter.du_until(d1) if fadapter else max(1, (d1 - today).days)
                    du2 = fadapter.du_until(d2) if fadapter else max(2, (d2 - today).days)
                    if du2 <= du1:
                        continue

                    # Implied forward rate between t1 and t2
                    implied_fwd = ((1 + r2) ** (du2 / 252.0) /
                                   (1 + r1) ** (du1 / 252.0)) ** (252.0 / (du2 - du1)) - 1.0

                    # Fair forward = use rates-curve service if available, else r2 (rough anchor)
                    rates_service = services_dict.get('rates_curve')
                    fair_fwd = None
                    if rates_service and hasattr(rates_service, 'forward_rate'):
                        try:
                            fair_fwd = float(rates_service.forward_rate(du1, du2))
                        except Exception:
                            fair_fwd = None
                    if fair_fwd is None:
                        # Naive anchor: midpoint interpolation of spot rates
                        fair_fwd = (r1 * du1 + r2 * du2) / (du1 + du2)

                    dev_bps = (implied_fwd - fair_fwd) * 10_000.0

                    pair_label = f"{_expiry_str(d1)[:6]}_{_expiry_str(d2)[:6]}"
                    _di_cal_window.setdefault(pair_label, []).append(dev_bps)
                    if len(_di_cal_window[pair_label]) > 120:
                        _di_cal_window[pair_label] = _di_cal_window[pair_label][-120:]
                    if len(_di_cal_window[pair_label]) % 10 == 0:
                        _upsert_calibration(get_db_fn, log, strategy_upper,
                                            pair_label, 'fwd_dev_bps',
                                            _di_cal_window[pair_label])

                    # Band = max(20 bps, 1.5 * rolling std)
                    samples = _di_cal_window[pair_label]
                    std_bps = statistics.stdev(samples) if len(samples) > 5 else 20.0
                    band_bps = max(20.0, 1.5 * std_bps)

                    if abs(dev_bps) < band_bps:
                        continue

                    direction = 'FWD_HIGH' if dev_bps > 0 else 'FWD_LOW'
                    _diag['opportunities'] += 1

                    log.info(
                        f"DI_CALENDAR {direction} pair={pair_label} "
                        f"fwd={implied_fwd:.4%} fair={fair_fwd:.4%} dev={dev_bps:.1f}bps "
                        f"band={band_bps:.1f}bps du1={du1} du2={du2}"
                    )

                    # Costs — DI1 exchange fee ~2 bps per leg + slippage ~4 bps
                    total_cost_bps = 2 * (2.0 + 4.0)
                    net_edge_bps = abs(dev_bps) - total_cost_bps
                    notional_estimate = 100_000.0  # one DI1 PU ~R$100k per contract

                    _safe_insert_opportunity(
                        get_db_fn, log, strategy_upper, f'DI1_{pair_label}',
                        net_edge_bps,
                        expiry=_expiry_str(d2),
                        opportunity_type=direction,
                        cost_estimate=total_cost_bps,
                        decision='CANDIDATE' if net_edge_bps > 0 else 'REJECTED',
                        rejection_reason=None if net_edge_bps > 0 else 'edge_below_cost',
                    )

                    if net_edge_bps <= 0:
                        continue

                    # Tier check
                    active_status_reg = services_dict.get('active_status_registry')
                    tier_str = 'OBSERVE'
                    if active_status_reg:
                        tier_obj = active_status_reg.get_status(f'DI1_{pair_label}', strategy_upper)
                        tier_str = tier_obj.value if tier_obj else 'OBSERVE'
                    if tier_str not in ('PAPER_FULL', 'PAPER_SMALL'):
                        continue

                    # DV01-neutral sizing: qty2 = qty1 * dv01_1 / dv01_2
                    dv01_1 = fadapter.di1_dv01(mid1 if mid1 > 100 else fadapter.di1_pu_from_rate(r1, du1), du1) \
                             if fadapter else 1.0
                    dv01_2 = fadapter.di1_dv01(mid2 if mid2 > 100 else fadapter.di1_pu_from_rate(r2, du2), du2) \
                             if fadapter else 1.0
                    qty1 = 10
                    qty2 = max(1, int(round(qty1 * dv01_1 / max(dv01_2, 1e-6))))

                    if direction == 'FWD_HIGH':
                        # Receive t1 (buy PU), pay t2 (sell PU)
                        legs = [
                            {'leg_type': 'FUTURE', 'symbol': f'DI1{_expiry_str(d1)[:6]}',
                             'qty': qty1, 'side': 'BUY', 'intended_price': float(fq1.ask or fq1.mid)},
                            {'leg_type': 'FUTURE', 'symbol': f'DI1{_expiry_str(d2)[:6]}',
                             'qty': qty2, 'side': 'SELL', 'intended_price': float(fq2.bid or fq2.mid)},
                        ]
                    else:
                        legs = [
                            {'leg_type': 'FUTURE', 'symbol': f'DI1{_expiry_str(d1)[:6]}',
                             'qty': qty1, 'side': 'SELL', 'intended_price': float(fq1.bid or fq1.mid)},
                            {'leg_type': 'FUTURE', 'symbol': f'DI1{_expiry_str(d2)[:6]}',
                             'qty': qty2, 'side': 'BUY', 'intended_price': float(fq2.ask or fq2.mid)},
                        ]

                    audit = {
                        'instrument_type': 'future',
                        'theoretical_price': round(fair_fwd * 100, 4),
                        'deviation_bps': round(dev_bps, 2),
                        'fair_value_inputs': {
                            'r1_annual': round(r1, 6), 'r2_annual': round(r2, 6),
                            'du1': du1, 'du2': du2,
                            'implied_forward': round(implied_fwd, 6),
                            'fair_forward': round(fair_fwd, 6),
                            'band_bps': round(band_bps, 2),
                            'dv01_1': round(dv01_1, 4), 'dv01_2': round(dv01_2, 4),
                            'qty1': qty1, 'qty2': qty2,
                            'generated_at': datetime.utcnow().isoformat(),
                        },
                        'audit_notes': (
                            f'DI_CALENDAR {direction} pair={pair_label} '
                            f'fwd={implied_fwd:.4%} fair={fair_fwd:.4%} '
                            f'dev={dev_bps:.1f}bps net={net_edge_bps:.1f}bps'
                        ),
                    }

                    _try_autonomous_execution(
                        log, services_dict,
                        strategy=strategy, symbol=f'DI1_{pair_label}',
                        structure_type=direction,
                        edge_magnitude=abs(dev_bps) * notional_estimate / 10_000.0,
                        notional_estimate=notional_estimate * (qty1 + qty2),
                        strike=0.0, expiry=_expiry_str(d2),
                        legs=legs, spot_price=float(mid1),
                        liquidity_score=0.0,
                        active_status_str=tier_str,
                        audit_payload=audit,
                        get_db_fn=get_db_fn,
                    )

                except Exception as inner:
                    _diag['errors'].append(str(inner)[:100])
                    log.warning(f"DI_CALENDAR pair error: {inner}")

            _scan_loop_diag['di_calendar'] = {**_diag, 'status': 'ran'}

        except Exception as e:
            log.error(f"DI_CALENDAR loop error: {e}\n{traceback.format_exc()}")
            _scan_loop_diag['di_calendar'] = {'ts': str(datetime.now()), 'error': str(e)[:120]}

        time.sleep(60)


def interlisted_hedged_scan_loop(beat_fn, get_db_fn, log, provider_mgr, services_dict, risk_check_fn, audit_fn):
    """INTERLISTED_HEDGED — ADR/B3 arbitrage with explicit FX hedge via WDO.

    Extends the classic INTERLISTED logic by adding a WDO (mini dólar) hedge
    leg sized to the USD notional exposure of the ADR leg. This removes the
    FX P&L drift that the unhedged pair carries between legging and closeout.
    """
    loop_name = "interlisted_hedged_scan_loop"
    strategy = 'interlisted_hedged'
    strategy_upper = 'INTERLISTED_HEDGED'

    for _ in range(5):
        beat_fn(loop_name); time.sleep(2)

    while True:
        try:
            beat_fn(loop_name)
            fees = _b3_fees()
            pairs = [('PETR4', 'PBR'), ('VALE3', 'VALE'), ('ITUB4', 'ITUB')]
            _diag = {'ts': str(datetime.now()), 'pairs_checked': 0, 'opportunities': 0, 'errors': []}

            usdbrl_quote = provider_mgr.get_spot('USDBRL')
            if not usdbrl_quote or not usdbrl_quote.bid:
                _scan_loop_diag['interlisted_hedged'] = {**_diag, 'status': 'no_fx'}
                time.sleep(60); continue

            fx_mid = usdbrl_quote.mid
            fx_ask = usdbrl_quote.ask
            fx_bid = usdbrl_quote.bid

            # WDO for FX hedge sizing
            wdo_future = None
            try:
                wdo_future = provider_mgr.get_future('USDBRL', tenor_offset=0) or \
                             provider_mgr.get_future('USD', tenor_offset=0)
            except Exception:
                wdo_future = None

            for b3_ticker, adr_ticker in pairs:
                try:
                    _diag['pairs_checked'] += 1
                    b3_quote = provider_mgr.get_spot(b3_ticker)
                    adr_quote = provider_mgr.get_spot(adr_ticker)
                    if not b3_quote or not adr_quote:
                        continue

                    adr_in_brl = adr_quote.mid * fx_mid
                    basis = b3_quote.mid - adr_in_brl
                    basis_pct = basis / adr_in_brl if adr_in_brl > 0 else 0.0

                    threshold = fees.get('interlisted', 0.005)
                    if abs(basis_pct) < threshold:
                        continue

                    direction = 'B3_RICH' if basis > 0 else 'ADR_RICH'
                    _diag['opportunities'] += 1

                    log.info(
                        f"INTERLISTED_HEDGED {b3_ticker}/{adr_ticker}: {direction} "
                        f"basis={basis:.2f} ({basis_pct*100:.3f}%) fx={fx_mid:.4f}"
                    )

                    _safe_insert_opportunity(
                        get_db_fn, log, strategy_upper,
                        f"{b3_ticker}/{adr_ticker}", basis_pct * 10_000,
                        opportunity_type=direction,
                        cost_estimate=threshold * 10_000,
                        decision='CANDIDATE',
                    )

                    active_status_reg = services_dict.get('active_status_registry')
                    tier_str = 'OBSERVE'
                    if active_status_reg:
                        tier_obj = active_status_reg.get_status(b3_ticker, strategy_upper)
                        tier_str = tier_obj.value if tier_obj else 'OBSERVE'
                    if tier_str not in ('PAPER_FULL', 'PAPER_SMALL'):
                        continue

                    # Sizing: main equity legs at 100 shares
                    qty = 100
                    usd_notional = adr_quote.mid * qty
                    wdo_qty = fadapter.wdo_hedge_contracts(usd_notional) if fadapter else 1

                    # Build 3-leg structure
                    if direction == 'B3_RICH':
                        # Sell B3, buy ADR → we are short BRL, long USD on the equity side.
                        # Hedge: SELL WDO to neutralize USD long (convert USD→BRL forward).
                        legs = [
                            {'leg_type': 'STOCK', 'symbol': b3_ticker, 'qty': qty,
                             'side': 'SELL', 'intended_price': b3_quote.bid},
                            {'leg_type': 'STOCK', 'symbol': adr_ticker, 'qty': qty,
                             'side': 'BUY', 'intended_price': adr_quote.ask},
                            {'leg_type': 'FUTURE', 'symbol': 'WDO', 'qty': wdo_qty,
                             'side': 'SELL',
                             'intended_price': float(wdo_future.bid) if wdo_future and wdo_future.bid else fx_bid * 1000},
                        ]
                    else:  # ADR_RICH
                        legs = [
                            {'leg_type': 'STOCK', 'symbol': b3_ticker, 'qty': qty,
                             'side': 'BUY', 'intended_price': b3_quote.ask},
                            {'leg_type': 'STOCK', 'symbol': adr_ticker, 'qty': qty,
                             'side': 'SELL', 'intended_price': adr_quote.bid},
                            {'leg_type': 'FUTURE', 'symbol': 'WDO', 'qty': wdo_qty,
                             'side': 'BUY',
                             'intended_price': float(wdo_future.ask) if wdo_future and wdo_future.ask else fx_ask * 1000},
                        ]

                    fair_adr_in_brl = adr_quote.mid * fx_mid
                    audit = {
                        'instrument_type': 'spot',   # main legs are equity; FX leg flagged via notes
                        'theoretical_price': round(fair_adr_in_brl, 4),
                        'deviation_bps': round(basis_pct * 10_000, 2),
                        'fair_value_inputs': {
                            'b3_mid': b3_quote.mid, 'adr_mid': adr_quote.mid,
                            'usdbrl': fx_mid, 'fair_brl': fair_adr_in_brl,
                            'basis': round(basis, 4), 'basis_pct': round(basis_pct, 6),
                            'qty': qty, 'wdo_qty': wdo_qty, 'usd_notional': usd_notional,
                            'generated_at': datetime.utcnow().isoformat(),
                        },
                        'audit_notes': (
                            f'INTERLISTED_HEDGED {direction} {b3_ticker}/{adr_ticker} '
                            f'basis={basis_pct*100:.3f}% wdo_hedge={wdo_qty} qty={qty}'
                        ),
                    }

                    _try_autonomous_execution(
                        log, services_dict,
                        strategy=strategy, symbol=b3_ticker,
                        structure_type=direction,
                        edge_magnitude=abs(basis),
                        notional_estimate=b3_quote.mid * qty,
                        strike=0.0, expiry='',
                        legs=legs, spot_price=b3_quote.mid,
                        liquidity_score=0.0,
                        active_status_str=tier_str,
                        audit_payload=audit,
                        get_db_fn=get_db_fn,
                    )

                except Exception as e:
                    _diag['errors'].append(f'{b3_ticker}:{str(e)[:80]}')
                    log.warning(f"INTERLISTED_HEDGED scan error for {b3_ticker}/{adr_ticker}: {e}")

            _scan_loop_diag['interlisted_hedged'] = {**_diag, 'status': 'ran'}

        except Exception as e:
            log.error(f"INTERLISTED_HEDGED loop error: {e}\n{traceback.format_exc()}")
            _scan_loop_diag['interlisted_hedged'] = {'ts': str(datetime.now()), 'error': str(e)[:120]}

        time.sleep(58)
