"""
Derivatives Strategy Scan Loops
8 derivative strategies: PCP, FST, Roll Arb, ETF Basket, Skew, InterListed, Dividend, Vol Arb
"""

import time
import traceback
from datetime import datetime, timedelta
import statistics


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
            db = get_db_fn()
            
            # Get eligible assets from universe
            from config import UNIVERSE_TIER_A, UNIVERSE_TIER_B, B3_FEES, SELIC_RATE, CDI_RATE
            eligible_assets = UNIVERSE_TIER_A + UNIVERSE_TIER_B
            
            opportunities_found = 0
            
            for asset in eligible_assets:
                try:
                    # Get market data
                    spot_quote = provider_mgr.get_spot(asset)
                    if not spot_quote or spot_quote.bid is None:
                        continue
                    
                    spot_price = spot_quote.mid
                    spot_ask = spot_quote.ask
                    spot_bid = spot_quote.bid
                    
                    # Get option chains (calls american, puts european)
                    call_chain = provider_mgr.get_option_chain(asset, option_type='CALL')
                    put_chain = provider_mgr.get_option_chain(asset, option_type='PUT')
                    
                    if not call_chain or not put_chain:
                        continue
                    
                    # Extract service objects
                    nav_calc = services_dict.get('nav_calculator')
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
                        
                        # Calculate PV(K) using CDI rate
                        days_to_expiry = (call_quote.expiry - datetime.now()).days
                        if days_to_expiry <= 0:
                            continue
                        
                        discount_factor = 1.0 / ((1 + CDI_RATE) ** (days_to_expiry / 252))
                        pv_strike = strike * discount_factor
                        
                        # Calculate dividend adjustment
                        div_adj = 0.0
                        dividend_service = services_dict.get('dividend_service')
                        if dividend_service:
                            divs = dividend_service.get_dividend_stream(asset, call_quote.expiry)
                            div_adj = sum(d.amount for d in divs if d.ex_date <= call_quote.expiry)
                        
                        # Calculate transaction costs
                        call_cost = call_quote.ask * B3_FEES.get('option_buy', 0.001)
                        put_cost = put_quote.bid * B3_FEES.get('option_sell', 0.001)
                        stock_cost = spot_ask * B3_FEES.get('stock_buy', 0.0005)
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
                                f"expiry={call_quote.expiry.date()} edge={edge_magnitude:.4f}"
                            )
                            
                            opportunities_found += 1
                            
                            # Persist to database
                            db.execute(
                                """
                                INSERT OR REPLACE INTO strategy_opportunities_log 
                                (strategy, symbol, strike, expiry, edge_type, edge_value, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                """,
                                ("PCP", asset, strike, call_quote.expiry.isoformat(), 
                                 edge_type, edge_magnitude, datetime.utcnow().isoformat())
                            )
                            db.commit()
                            
                            # Execute if eligible
                            active_status = services_dict.get('active_status_registry')
                            if active_status and active_status.get_status(asset, 'PCP') >= 2:  # PAPER_SMALL+
                                executor = services_dict.get('executor')
                                if executor:
                                    try:
                                        executor.execute_pcp(
                                            asset, strike, call_quote.expiry, 
                                            edge_type, edge_magnitude
                                        )
                                    except Exception as e:
                                        log.warning(f"PCP execution failed: {e}")
                
                except Exception as e:
                    log.warning(f"PCP scan error for {asset}: {e}")
            
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
            db = get_db_fn()
            
            from config import UNIVERSE_TIER_A, CDI_RATE
            
            for asset in UNIVERSE_TIER_A:
                try:
                    asset_key = f"fst_{asset}"
                    created_at = calibration_data.get(f"{asset_key}_created", datetime.now())
                    days_running = (datetime.now() - created_at).days
                    
                    if days_running < 5:
                        log.info(f"FST {asset}: calibration phase ({days_running}/5 days)")
                        beat_fn(loop_name)
                        time.sleep(10)
                        continue
                    
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
                    
                    days_to_expiry = (future_expiry - datetime.now()).days
                    if days_to_expiry <= 0:
                        continue
                    
                    discount_factor = 1.0 / ((1 + CDI_RATE) ** (days_to_expiry / 252))
                    pv_strike = int(spot_price) * discount_factor
                    
                    synthetic_future = call_quote.mid - put_quote.mid + pv_strike
                    
                    # Calculate carry theoretical
                    dividend_service = services_dict.get('dividend_service')
                    div_sum = 0.0
                    if dividend_service:
                        divs = dividend_service.get_dividend_stream(asset, future_expiry)
                        div_sum = sum(d.amount for d in divs if d.ex_date <= future_expiry)
                    
                    carry_theoretical = spot_price * ((1 + CDI_RATE) ** (days_to_expiry / 252)) - div_sum
                    
                    # Calculate spreads
                    spread_a = future_quote.mid - synthetic_future
                    spread_c = synthetic_future - carry_theoretical
                    
                    # Store in calibration
                    if asset_key not in calibration_data:
                        calibration_data[asset_key] = []
                    
                    calibration_data[asset_key].append(spread_a)
                    if len(calibration_data[asset_key]) > 20:
                        calibration_data[asset_key].pop(0)
                    
                    # Check for opportunity
                    if len(calibration_data[asset_key]) > 5:
                        mean_spread = statistics.mean(calibration_data[asset_key])
                        stdev_spread = statistics.stdev(calibration_data[asset_key]) if len(calibration_data[asset_key]) > 1 else 0
                        
                        if abs(spread_a - mean_spread) > 2 * stdev_spread and abs(spread_c - mean_spread) > stdev_spread:
                            log.info(
                                f"FST {asset} opportunity: spread_a={spread_a:.4f} "
                                f"(mean={mean_spread:.4f}), spread_c={spread_c:.4f}"
                            )
                            
                            db.execute(
                                """
                                INSERT INTO strategy_opportunities_log 
                                (strategy, symbol, edge_value, created_at)
                                VALUES (?, ?, ?, ?)
                                """,
                                ("FST", asset, spread_a, datetime.utcnow().isoformat())
                            )
                            db.commit()
                    
                    # Check liquidity fallback
                    if future_quote.volume and future_quote.volume < 100:
                        log.warning(f"FST {asset}: low future liquidity, considering BOVA11/WIN pivot")
                
                except Exception as e:
                    log.warning(f"FST scan error for {asset}: {e}")
            
        except Exception as e:
            log.error(f"FST loop error: {e}\n{traceback.format_exc()}")
        
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
            db = get_db_fn()
            
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
                    days_f1 = (future_1.expiry - datetime.now()).days
                    days_f2 = (future_2.expiry - datetime.now()).days
                    days_between = days_f2 - days_f1
                    
                    from config import CDI_RATE
                    carry_theoretical = future_1.mid * CDI_RATE * (days_between / 252)
                    
                    # Check threshold
                    roll_mispricing = roll_cost_realized - carry_theoretical
                    threshold = carry_theoretical * 0.05  # 5% threshold
                    
                    if abs(roll_mispricing) > threshold:
                        direction = "CHEAP" if roll_mispricing < 0 else "EXPENSIVE"
                        log.info(
                            f"Roll Arb {asset}: {direction} F2-F1={roll_cost_realized:.2f} "
                            f"vs carry={carry_theoretical:.2f}"
                        )
                        
                        db.execute(
                            """
                            INSERT INTO strategy_opportunities_log 
                            (strategy, symbol, edge_value, created_at)
                            VALUES (?, ?, ?, ?)
                            """,
                            ("ROLL_ARB", asset, roll_mispricing, datetime.utcnow().isoformat())
                        )
                        db.commit()
                
                except Exception as e:
                    log.warning(f"Roll arb error for {asset}: {e}")
        
        except Exception as e:
            log.error(f"Roll Arb loop error: {e}\n{traceback.format_exc()}")
        
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
            db = get_db_fn()
            
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
                        
                        from config import B3_FEES
                        cost_threshold = B3_FEES.get('etf_arb', 0.003)
                        
                        if abs(divergence_pct) > cost_threshold * 100:
                            direction = "PREMIUM" if divergence > 0 else "DISCOUNT"
                            log.info(
                                f"ETF Basket {etf_ticker}: {direction} "
                                f"price={etf_price:.2f} NAV={nav_value:.2f} "
                                f"div={divergence_pct:.3f}%"
                            )
                            
                            db.execute(
                                """
                                INSERT INTO strategy_opportunities_log 
                                (strategy, symbol, edge_value, created_at)
                                VALUES (?, ?, ?, ?)
                                """,
                                ("ETF_BASKET", etf_ticker, divergence, datetime.utcnow().isoformat())
                            )
                            db.commit()
                
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
        
        except Exception as e:
            log.error(f"ETF Basket loop error: {e}\n{traceback.format_exc()}")
        
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
            db = get_db_fn()
            
            from config import UNIVERSE_TIER_A
            
            for asset in UNIVERSE_TIER_A:
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
                        call_iv = greeks_calc.implied_vol(
                            spot_price, otm_call_strike, call_quote.mid,
                            (call_quote.expiry - datetime.now()).days / 365, 'call'
                        )
                        put_iv = greeks_calc.implied_vol(
                            spot_price, otm_put_strike, put_quote.mid,
                            (put_quote.expiry - datetime.now()).days / 365, 'put'
                        )
                        
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
                                
                                db.execute(
                                    """
                                    INSERT INTO strategy_opportunities_log 
                                    (strategy, symbol, edge_value, created_at)
                                    VALUES (?, ?, ?, ?)
                                    """,
                                    ("SKEW_ARB", asset, z_score, datetime.utcnow().isoformat())
                                )
                                db.commit()
                
                except Exception as e:
                    log.warning(f"Skew scan error for {asset}: {e}")
        
        except Exception as e:
            log.error(f"Skew loop error: {e}\n{traceback.format_exc()}")
        
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
            db = get_db_fn()
            
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
                    
                    from config import B3_FEES
                    threshold = B3_FEES.get('interlisted', 0.005)
                    
                    if abs(basis_pct) > threshold:
                        direction = "B3_RICH" if basis > 0 else "ADR_RICH"
                        log.info(
                            f"InterListed {b3_ticker}/{adr_ticker}: {direction} "
                            f"basis={basis:.2f} ({basis_pct*100:.3f}%)"
                        )
                        
                        db.execute(
                            """
                            INSERT INTO strategy_opportunities_log 
                            (strategy, symbol, edge_value, created_at)
                            VALUES (?, ?, ?, ?)
                            """,
                            ("INTERLISTED", f"{b3_ticker}/{adr_ticker}", basis_pct, 
                             datetime.utcnow().isoformat())
                        )
                        db.commit()
                
                except Exception as e:
                    log.warning(f"InterListed scan error for {b3_ticker}/{adr_ticker}: {e}")
        
        except Exception as e:
            log.error(f"InterListed loop error: {e}\n{traceback.format_exc()}")
        
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
            db = get_db_fn()
            
            from config import UNIVERSE_TIER_A, B3_FEES
            dividend_service = services_dict.get('dividend_service')
            
            if not dividend_service:
                time.sleep(60)
                continue
            
            for asset in UNIVERSE_TIER_A:
                try:
                    spot_quote = provider_mgr.get_spot(asset)
                    if not spot_quote:
                        continue
                    
                    spot_price = spot_quote.mid
                    
                    # Get upcoming dividends (next 60 days)
                    upcoming_divs = dividend_service.get_dividend_stream(
                        asset, 
                        datetime.now() + timedelta(days=60)
                    )
                    
                    for dividend in upcoming_divs:
                        days_to_ex = (dividend.ex_date - datetime.now()).days
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
                        buy_cost = spot_price * B3_FEES.get('stock_buy', 0.0005)
                        put_hedge_cost = put_cost * B3_FEES.get('option_buy', 0.001)
                        
                        net_edge = dividend.amount - buy_cost - put_hedge_cost
                        
                        if net_edge > 0:
                            log.info(
                                f"Dividend Arb {asset}: ex-date={dividend.ex_date.date()} "
                                f"dividend={dividend.amount:.2f} net_edge={net_edge:.4f}"
                            )
                            
                            db.execute(
                                """
                                INSERT INTO strategy_opportunities_log 
                                (strategy, symbol, edge_value, created_at)
                                VALUES (?, ?, ?, ?)
                                """,
                                ("DIVIDEND_ARB", asset, net_edge, datetime.utcnow().isoformat())
                            )
                            db.commit()
                
                except Exception as e:
                    log.warning(f"Dividend arb error for {asset}: {e}")
        
        except Exception as e:
            log.error(f"Dividend Arb loop error: {e}\n{traceback.format_exc()}")
        
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
            db = get_db_fn()
            
            from config import UNIVERSE_TIER_A
            
            for asset in UNIVERSE_TIER_A:
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
                            iv = greeks_calc.implied_vol(
                                spot_price, int(spot_price), call_quote.mid,
                                (call_quote.expiry - datetime.now()).days / 365, 'call'
                            )
                            
                            # Calculate IV/RV ratios
                            iv_rv_20 = iv / rv_20 if rv_20 > 0 else 0
                            iv_rv_60 = iv / rv_60 if rv_60 > 0 else 0
                            
                            # Estimate delta-hedge cost
                            delta = greeks_calc.get_delta(
                                spot_price, int(spot_price), iv,
                                (call_quote.expiry - datetime.now()).days / 365, 'call'
                            )
                            
                            from config import B3_FEES
                            hedge_cost_pct = delta * 2 * B3_FEES.get('stock_buy', 0.0005)
                            
                            # Signal opportunity
                            if iv > rv_60 * 1.5:  # IV significantly above RV
                                log.info(
                                    f"Vol Arb {asset}: SELL (IV high) IV={iv:.2%} RV60={rv_60:.2%} "
                                    f"hedge_cost={hedge_cost_pct:.3%}"
                                )
                                
                                db.execute(
                                    """
                                    INSERT INTO strategy_opportunities_log 
                                    (strategy, symbol, edge_value, created_at)
                                    VALUES (?, ?, ?, ?)
                                    """,
                                    ("VOL_ARB", asset, iv_rv_60 - 1, datetime.utcnow().isoformat())
                                )
                                db.commit()
                            
                            elif iv < rv_60 * 0.8:  # IV significantly below RV
                                log.info(
                                    f"Vol Arb {asset}: BUY (IV low) IV={iv:.2%} RV60={rv_60:.2%} "
                                    f"hedge_cost={hedge_cost_pct:.3%}"
                                )
                                
                                db.execute(
                                    """
                                    INSERT INTO strategy_opportunities_log 
                                    (strategy, symbol, edge_value, created_at)
                                    VALUES (?, ?, ?, ?)
                                    """,
                                    ("VOL_ARB", asset, 1 - iv_rv_60, datetime.utcnow().isoformat())
                                )
                                db.commit()
                
                except Exception as e:
                    log.warning(f"Vol arb error for {asset}: {e}")
        
        except Exception as e:
            log.error(f"Vol Arb loop error: {e}\n{traceback.format_exc()}")
        
        time.sleep(56)
