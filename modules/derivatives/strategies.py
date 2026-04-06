"""
Derivatives Strategy Scan Loops
8 derivative strategies: PCP, FST, Roll Arb, ETF Basket, Skew, InterListed, Dividend, Vol Arb
MySQL-compatible: uses %s placeholders, cursor(dictionary=True), conn.close()
"""

import time
import traceback
from datetime import datetime, timedelta
import statistics


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


def _try_autonomous_execution(
    log, services_dict, strategy, symbol, structure_type,
    edge_magnitude, notional_estimate, strike=0.0, expiry='',
    legs=None, spot_price=0.0, liquidity_score=0.0, active_status_str='',
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
    try:
        execution_engine = services_dict.get('deriv_execution')
        sizer = services_dict.get('deriv_sizer')
        capital_mgr = services_dict.get('capital_manager')
        learner = services_dict.get('deriv_learner')

        if not execution_engine or not sizer or not capital_mgr:
            # Modules not yet initialized — fall back to logging only
            return False

        # Get capital snapshot
        cap_snap = capital_mgr.get_snapshot()

        # Check if trading is allowed
        allowed, reason = capital_mgr.is_trading_allowed(strategy)
        if not allowed:
            log.info(f"Trading blocked for {strategy}: {reason}")
            return False

        # Get confidence adjustment from learning engine
        confidence = 0.65  # base confidence
        if learner:
            adj = learner.get_confidence_adjustment(strategy, symbol)
            confidence *= adj

        # Compute position size
        edge_bps = (edge_magnitude / notional_estimate * 10_000) if notional_estimate > 0 else 0
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

        if sizing.notional <= 0:
            log.debug(f"Sizing rejected {strategy}/{symbol}: {sizing.reason}")
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

        log.info(
            f"Autonomous execution {strategy}/{symbol}: "
            f"trade_id={trade.trade_id}, status={trade.status.value}, "
            f"notional=R${sizing.notional:,.0f}"
        )
        return True

    except Exception as e:
        log.warning(f"Autonomous execution error {strategy}/{symbol}: {e}")
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


def _safe_insert_opportunity(get_db_fn, log, strategy_type, symbol, edge_value,
                              strike=None, expiry=None, opportunity_type=None):
    """Insert an opportunity into strategy_opportunities_log using MySQL."""
    conn = None
    try:
        conn = get_db_fn()
        if conn is None:
            return
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO strategy_opportunities_log
            (strategy_type, symbol, strike, expiry, opportunity_type, expected_edge_bps)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (strategy_type, symbol, strike, expiry, opportunity_type, edge_value)
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
                     'liq_pass': 0, 'dte_pass': 0, 'opportunities': 0, 'errors': [], 'ts': str(datetime.now())}

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
                    call_chain = provider_mgr.get_option_chain(asset, option_type='CALL')
                    put_chain = provider_mgr.get_option_chain(asset, option_type='PUT')

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
                        safety_factor = 1.75
                        total_costs = (call_cost + put_cost + stock_cost) * safety_factor

                        # Check conversion edge: C_bid - P_ask - S_ask + PV(K) + div_adj
                        conversion_edge = (
                            call_quote.bid - put_quote.ask - spot_ask + pv_strike + div_adj - total_costs
                        )

                        # Check reversal edge: S_bid - C_ask + P_bid - PV(K) - div_adj
                        reversal_edge = (
                            spot_bid - call_quote.ask + put_quote.bid - pv_strike - div_adj - total_costs
                        )

                        if conversion_edge > 0 or reversal_edge > 0:
                            edge_type = "CONVERSION" if conversion_edge > reversal_edge else "REVERSAL"
                            edge_magnitude = max(conversion_edge, reversal_edge)

                            log.info(
                                f"PCP {edge_type} opportunity: {asset} K={strike} "
                                f"expiry={_expiry_date_str(call_quote.expiry)} edge={edge_magnitude:.4f}"
                            )

                            opportunities_found += 1
                            _diag['opportunities'] += 1

                            _safe_insert_opportunity(
                                get_db_fn, log, 'PCP', asset, edge_magnitude,
                                strike=strike,
                                expiry=_expiry_str(call_quote.expiry),
                                opportunity_type=edge_type
                            )

                            # Autonomous execution via new pipeline
                            active_status_reg = services_dict.get('active_status_registry')
                            tier_str = ''
                            if active_status_reg:
                                tier_obj = active_status_reg.get_status(asset, 'PCP')
                                tier_str = tier_obj.value if tier_obj else 'OBSERVE'

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

                    in_calibration = days_running < 5

                    # Get spot price
                    spot_quote = provider_mgr.get_spot(asset)
                    if not spot_quote:
                        continue

                    spot_price = spot_quote.mid

                    # Get listed future and options
                    future_quote = provider_mgr.get_future(asset)
                    call_chain = provider_mgr.get_option_chain(asset, option_type='CALL')
                    put_chain = provider_mgr.get_option_chain(asset, option_type='PUT')

                    if not future_quote or not call_chain or not put_chain:
                        continue

                    # Calculate synthetic future for nearest maturity
                    future_expiry = future_quote.expiry
                    call_quote = call_chain.get(int(spot_price))
                    put_quote = put_chain.get(int(spot_price))

                    if not call_quote or not put_quote:
                        continue

                    days_to_expiry = _days_to_expiry(future_expiry)
                    if days_to_expiry <= 0:
                        continue

                    discount_factor = 1.0 / ((1 + cdi_rate) ** (days_to_expiry / 252))
                    pv_strike = int(spot_price) * discount_factor

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

                            _safe_insert_opportunity(
                                get_db_fn, log, 'FST', asset, spread_a,
                                opportunity_type='SPREAD_DIVERGENCE'
                            )

                            # Autonomous execution
                            _try_autonomous_exec_generic(
                                log, services_dict, 'fst', asset,
                                'SPREAD_DIVERGENCE', abs(spread_a),
                                spot_price * 100, spot_price,
                                strike=int(spot_price),
                                legs=[
                                    {'leg_type': 'FUTURE', 'symbol': f'{asset}_FUT', 'qty': 1,
                                     'side': 'SELL' if spread_a > 0 else 'BUY',
                                     'intended_price': future_quote.mid},
                                    {'leg_type': 'CALL', 'symbol': f'{asset}_C{int(spot_price)}', 'qty': 1,
                                     'side': 'BUY' if spread_a > 0 else 'SELL',
                                     'intended_price': call_quote.mid},
                                    {'leg_type': 'PUT', 'symbol': f'{asset}_P{int(spot_price)}', 'qty': 1,
                                     'side': 'SELL' if spread_a > 0 else 'BUY',
                                     'intended_price': put_quote.mid},
                                ],
                            )

                    # Check liquidity fallback
                    if future_quote.volume and future_quote.volume < 100:
                        log.warning(f"FST {asset}: low future liquidity, considering BOVA11/WIN pivot")

                except Exception as e:
                    log.warning(f"FST scan error for {asset}: {e}")

            _scan_loop_diag['fst'] = {'ts': str(datetime.now()), 'status': 'ran'}

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
                        continue

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

            _scan_loop_diag['roll_arb'] = {'ts': str(datetime.now()), 'status': 'ran'}

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
                if nav_calc:
                    nav_value = nav_calc.calculate_nav(etf_ticker)

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

            _scan_loop_diag['etf_basket'] = {'ts': str(datetime.now()), 'status': 'ran'}

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
                    otm_call_strike = int(spot_price * 1.05)
                    otm_put_strike = int(spot_price * 0.95)

                    call_quote = call_chain.get(otm_call_strike)
                    put_quote = put_chain.get(otm_put_strike)

                    if not call_quote or not put_quote:
                        continue

                    # Extract IV
                    greeks_calc = services_dict.get('greeks_calculator')
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

            _scan_loop_diag['skew_arb'] = {'ts': str(datetime.now()), 'status': 'ran'}

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
                        continue

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

            _scan_loop_diag['interlisted'] = {'ts': str(datetime.now()), 'status': 'ran'}

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

            if not dividend_service:
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

                        strike = int(spot_price)
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

            _scan_loop_diag['dividend_arb'] = {'ts': str(datetime.now()), 'status': 'ran'}

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
                        call_quote = call_chain.get(int(spot_price))

                        if not call_quote:
                            continue

                        greeks_calc = services_dict.get('greeks_calculator')
                        if greeks_calc:
                            _vol_dte = _days_to_expiry(call_quote.expiry)
                            if _vol_dte <= 0:
                                continue
                            try:
                                iv = greeks_calc.calculate_iv_newton_raphson(
                                    'call', spot_price, int(spot_price), _vol_dte, call_quote.mid
                                )
                            except Exception:
                                continue

                            # Calculate IV/RV ratios
                            iv_rv_20 = iv / rv_20 if rv_20 > 0 else 0
                            iv_rv_60 = iv / rv_60 if rv_60 > 0 else 0

                            # Estimate delta-hedge cost
                            try:
                                greeks_data = greeks_calc.calculate_greeks('call', spot_price, int(spot_price), _vol_dte, iv)
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
                                    strike=int(spot_price),
                                    legs=[
                                        {'leg_type': 'CALL', 'symbol': f'{asset}_C{int(spot_price)}', 'qty': 1,
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
                                    strike=int(spot_price),
                                    legs=[
                                        {'leg_type': 'CALL', 'symbol': f'{asset}_C{int(spot_price)}', 'qty': 1,
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
