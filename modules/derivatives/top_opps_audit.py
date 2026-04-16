"""
Top Opportunities Daily Audit — [v10.36]
─────────────────────────────────────────
Captures, once per trading day (17:30 BRT EOD), the top-N opportunities surfaced
by the derivatives engine together with their complete execution lifecycle
(entry, exit, greeks, legs, slippage, liquidity) so that the paper track record
can be audited end-to-end by investors.

Source of "Top Opportunities":
    strategy_opportunities_log → GET /strategies/opportunities?limit=200
    (rendered by static/derivatives.html → renderOpportunitiesTable)

Cross-joined with:
    strategy_master_trades          (paper trade header + realized P&L + slippage)
    strategy_trade_legs             (per-leg fills)
    strategy_liquidity_monitor      (7-factor liquidity breakdown)
    options_snapshots / greeks_snapshots (bid/ask/oi/volume/greeks at entry & exit)
    derivatives_learning_outcomes   (confidence, time-in-trade, close reason)

Output:
    top_opportunities_audit         (one row per opportunity per day)
    GET /strategies/top-opps/daily?date=YYYY-MM-DD   (endpoint)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# SCHEMA
# ═══════════════════════════════════════════════════════════════════════════

def create_top_opps_audit_table(connection: Any) -> None:
    """Create the top_opportunities_audit table (idempotent)."""
    cursor = connection.cursor()
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_opportunities_audit (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            snapshot_date DATE NOT NULL,
            snapshot_ts DATETIME NOT NULL,
            rank_position INT NOT NULL,
            opportunity_id BIGINT,
            strategy_type VARCHAR(32) NOT NULL,
            symbol VARCHAR(16) NOT NULL,
            underlying VARCHAR(16),
            strike DECIMAL(10, 2),
            expiry VARCHAR(8),
            opportunity_type VARCHAR(32),
            decision VARCHAR(16),
            rejection_reason VARCHAR(255),
            expected_edge_bps DECIMAL(10, 2),
            cost_estimate DECIMAL(14, 4),
            -- Paper trade linkage (may be NULL if no trade was opened)
            trade_id VARCHAR(64),
            trade_status VARCHAR(16),
            active_status VARCHAR(32),
            direction VARCHAR(16),
            structure_type VARCHAR(32),
            notional DECIMAL(16, 2),
            opened_at DATETIME,
            closed_at DATETIME,
            time_in_trade_hours DECIMAL(10, 2),
            close_reason VARCHAR(255),
            -- P&L and execution quality
            expected_edge DECIMAL(12, 4),
            realized_edge DECIMAL(12, 4),
            expected_cost DECIMAL(12, 4),
            executed_cost DECIMAL(12, 4),
            slippage DECIMAL(12, 4),
            latency_ms INT,
            pnl DECIMAL(14, 2),
            pnl_pct DECIMAL(8, 4),
            legs_count INT,
            legging_incidents INT,
            -- Market microstructure at snapshot
            spot_price DECIMAL(12, 4),
            bid DECIMAL(10, 4),
            ask DECIMAL(10, 4),
            mid DECIMAL(10, 4),
            spread_bps DECIMAL(10, 2),
            oi INT,
            volume INT,
            iv DECIMAL(8, 6),
            -- Greeks at entry and exit (JSON blob with full set)
            greeks_entry JSON,
            greeks_exit JSON,
            -- Legs detail (JSON array)
            legs JSON,
            -- Liquidity (7-factor breakdown + tier)
            liquidity_score DECIMAL(5, 2),
            liquidity_tier VARCHAR(32),
            liquidity_breakdown JSON,
            -- Learning engine
            confidence_at_entry DECIMAL(5, 4),
            confidence_adj_after DECIMAL(5, 4),
            -- Bookkeeping
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_snapshot_rank (snapshot_date, strategy_type, symbol, expiry, strike, opportunity_type),
            INDEX idx_snapshot_date (snapshot_date),
            INDEX idx_strategy_symbol (strategy_type, symbol),
            INDEX idx_trade_id (trade_id),
            INDEX idx_snapshot_ts (snapshot_ts)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        connection.commit()
        logger.info('[v10.36] top_opportunities_audit table created/verified')
    except Exception as e:
        connection.rollback()
        logger.error(f'[v10.36] create_top_opps_audit_table error: {e}')
        raise
    finally:
        cursor.close()


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def _safe_json(v: Any) -> str:
    try:
        return json.dumps(v, default=str)
    except Exception:
        return 'null'


def _compute_spread_bps(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is None or ask is None:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return round(((ask - bid) / mid) * 10000.0, 2)


def _select_top_opps(cursor, snapshot_date: date, top_n: int) -> List[Dict[str, Any]]:
    """
    Top-N opportunities of the day:
      - prefer APPROVED / highest expected_edge_bps
      - tie-break by liquidity_score
      - fallback to most recent if edge is null
    """
    start = datetime.combine(snapshot_date, datetime.min.time())
    end = datetime.combine(snapshot_date, datetime.max.time())
    cursor.execute("""
        SELECT id, strategy_type, symbol, strike, expiry,
               opportunity_type, expected_edge_bps, cost_estimate,
               liquidity_score, decision, rejection_reason, timestamp
        FROM strategy_opportunities_log
        WHERE timestamp BETWEEN %s AND %s
        ORDER BY
            CASE WHEN decision = 'APPROVED' THEN 0 ELSE 1 END,
            COALESCE(expected_edge_bps, 0) DESC,
            COALESCE(liquidity_score, 0) DESC,
            timestamp DESC
        LIMIT %s
    """, (start, end, top_n))
    return cursor.fetchall() or []


def _find_paper_trade(cursor, opp: Dict[str, Any], snapshot_date: date) -> Optional[Dict[str, Any]]:
    """Find the paper trade that matches an opportunity (if any was opened)."""
    start = datetime.combine(snapshot_date, datetime.min.time())
    end = datetime.combine(snapshot_date, datetime.max.time())
    cursor.execute("""
        SELECT * FROM strategy_master_trades
        WHERE strategy_type = %s AND symbol = %s
          AND (strike <=> %s) AND (expiry <=> %s)
          AND opened_at BETWEEN %s AND %s
        ORDER BY opened_at DESC
        LIMIT 1
    """, (
        opp.get('strategy_type'), opp.get('symbol'),
        opp.get('strike'), opp.get('expiry'),
        start, end,
    ))
    return cursor.fetchone()


def _fetch_legs(cursor, trade_id: str) -> List[Dict[str, Any]]:
    cursor.execute("""
        SELECT leg_type, symbol, qty, side, intended_price, executed_price,
               fill_status, slippage, latency_ms, timestamp
        FROM strategy_trade_legs
        WHERE trade_id = %s
        ORDER BY timestamp ASC
    """, (trade_id,))
    return cursor.fetchall() or []


def _fetch_latest_greeks(cursor, symbol: str, strike: Optional[float], expiry: Optional[str],
                        before_ts: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    if before_ts:
        cursor.execute("""
            SELECT iv, delta, gamma, theta, vega, rho, timestamp
            FROM greeks_snapshots
            WHERE symbol = %s AND (strike <=> %s) AND (expiry <=> %s)
              AND timestamp <= %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol, strike, expiry, before_ts))
    else:
        cursor.execute("""
            SELECT iv, delta, gamma, theta, vega, rho, timestamp
            FROM greeks_snapshots
            WHERE symbol = %s AND (strike <=> %s) AND (expiry <=> %s)
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol, strike, expiry))
    return cursor.fetchone()


def _fetch_latest_option_snap(cursor, symbol: str, strike: Optional[float],
                              expiry: Optional[str]) -> Optional[Dict[str, Any]]:
    cursor.execute("""
        SELECT bid, ask, last, volume, oi, iv, delta, gamma, theta, vega, timestamp
        FROM options_snapshots
        WHERE symbol = %s AND (strike <=> %s) AND (expiry <=> %s)
        ORDER BY timestamp DESC
        LIMIT 1
    """, (symbol, strike, expiry))
    return cursor.fetchone()


def _fetch_liquidity(cursor, symbol: str, strategy: str) -> Optional[Dict[str, Any]]:
    cursor.execute("""
        SELECT spread_score, depth_score, oi_volume_score, exec_plausibility_score,
               persistence_score, exit_liquidity_score, data_quality_score,
               total_score, tier, timestamp
        FROM strategy_liquidity_monitor
        WHERE symbol = %s AND strategy_type = %s
        ORDER BY timestamp DESC
        LIMIT 1
    """, (symbol, strategy))
    return cursor.fetchone()


def _fetch_learning_outcome(cursor, trade_id: str) -> Optional[Dict[str, Any]]:
    cursor.execute("""
        SELECT confidence_at_entry, confidence_adj_after, time_in_trade_hours
        FROM derivatives_learning_outcomes
        WHERE trade_id = %s
        ORDER BY created_at DESC
        LIMIT 1
    """, (trade_id,))
    return cursor.fetchone()


# ═══════════════════════════════════════════════════════════════════════════
# SNAPSHOT
# ═══════════════════════════════════════════════════════════════════════════

def snapshot_top_opportunities(
    db_fn,
    log=None,
    snapshot_date: Optional[date] = None,
    top_n: int = 10,
) -> Dict[str, Any]:
    """
    Capture the top-N opportunities of `snapshot_date` along with their
    execution lifecycle. Idempotent (REPLACE INTO on the unique key).

    Returns dict with {date, captured, inserted, errors}.
    """
    _log = log or logger
    if snapshot_date is None:
        try:
            import pytz
            snapshot_date = datetime.now(pytz.timezone('America/Sao_Paulo')).date()
        except Exception:
            snapshot_date = datetime.utcnow().date()

    result = {
        'date': snapshot_date.isoformat(),
        'captured': 0,
        'inserted': 0,
        'errors': [],
    }

    conn = None
    try:
        conn = db_fn()
        if conn is None:
            result['errors'].append('no_db_connection')
            return result

        cursor = conn.cursor(dictionary=True)
        snapshot_ts = datetime.now()

        opps = _select_top_opps(cursor, snapshot_date, top_n)
        result['captured'] = len(opps)
        _log.info(f'[top-opps-audit] {snapshot_date} — {len(opps)} opps captured')

        write_cursor = conn.cursor()

        for rank, opp in enumerate(opps, start=1):
            try:
                trade = _find_paper_trade(cursor, opp, snapshot_date)
                trade_id = trade['trade_id'] if trade else None

                legs = _fetch_legs(cursor, trade_id) if trade_id else []

                greeks_entry = None
                greeks_exit = None
                if trade:
                    greeks_entry = _fetch_latest_greeks(
                        cursor, opp.get('symbol'), opp.get('strike'),
                        opp.get('expiry'), trade.get('opened_at'))
                    if trade.get('closed_at'):
                        greeks_exit = _fetch_latest_greeks(
                            cursor, opp.get('symbol'), opp.get('strike'),
                            opp.get('expiry'), trade.get('closed_at'))

                opt_snap = _fetch_latest_option_snap(
                    cursor, opp.get('symbol'), opp.get('strike'), opp.get('expiry'))

                liq = _fetch_liquidity(
                    cursor, opp.get('symbol'), opp.get('strategy_type'))

                learning = None
                if trade_id:
                    learning = _fetch_learning_outcome(cursor, trade_id)

                bid = _to_float(opt_snap.get('bid')) if opt_snap else None
                ask = _to_float(opt_snap.get('ask')) if opt_snap else None
                mid = (bid + ask) / 2.0 if (bid is not None and ask is not None) else None
                spread_bps = _compute_spread_bps(bid, ask)

                liquidity_breakdown = None
                liquidity_tier = None
                liquidity_score_val = _to_float(opp.get('liquidity_score'))
                if liq:
                    liquidity_breakdown = {
                        'spread_score': _to_float(liq.get('spread_score')),
                        'depth_score': _to_float(liq.get('depth_score')),
                        'oi_volume_score': _to_float(liq.get('oi_volume_score')),
                        'exec_plausibility_score': _to_float(liq.get('exec_plausibility_score')),
                        'persistence_score': _to_float(liq.get('persistence_score')),
                        'exit_liquidity_score': _to_float(liq.get('exit_liquidity_score')),
                        'data_quality_score': _to_float(liq.get('data_quality_score')),
                        'total_score': _to_float(liq.get('total_score')),
                        'captured_at': _iso(liq.get('timestamp')),
                    }
                    liquidity_tier = liq.get('tier')
                    if liquidity_score_val is None:
                        liquidity_score_val = _to_float(liq.get('total_score'))

                # time in trade
                tit = None
                if trade and trade.get('opened_at') and trade.get('closed_at'):
                    try:
                        delta = trade['closed_at'] - trade['opened_at']
                        tit = round(delta.total_seconds() / 3600.0, 2)
                    except Exception:
                        tit = None
                elif learning:
                    tit = _to_float(learning.get('time_in_trade_hours'))

                legs_json = _safe_json([
                    {
                        'leg_type': l.get('leg_type'),
                        'symbol': l.get('symbol'),
                        'qty': l.get('qty'),
                        'side': l.get('side'),
                        'intended_price': _to_float(l.get('intended_price')),
                        'executed_price': _to_float(l.get('executed_price')),
                        'fill_status': l.get('fill_status'),
                        'slippage': _to_float(l.get('slippage')),
                        'latency_ms': l.get('latency_ms'),
                        'timestamp': _iso(l.get('timestamp')),
                    } for l in legs
                ])

                greeks_entry_json = _safe_json({
                    'iv': _to_float(greeks_entry.get('iv')) if greeks_entry else None,
                    'delta': _to_float(greeks_entry.get('delta')) if greeks_entry else None,
                    'gamma': _to_float(greeks_entry.get('gamma')) if greeks_entry else None,
                    'theta': _to_float(greeks_entry.get('theta')) if greeks_entry else None,
                    'vega': _to_float(greeks_entry.get('vega')) if greeks_entry else None,
                    'rho': _to_float(greeks_entry.get('rho')) if greeks_entry else None,
                    'timestamp': _iso(greeks_entry.get('timestamp')) if greeks_entry else None,
                }) if greeks_entry else 'null'

                greeks_exit_json = _safe_json({
                    'iv': _to_float(greeks_exit.get('iv')) if greeks_exit else None,
                    'delta': _to_float(greeks_exit.get('delta')) if greeks_exit else None,
                    'gamma': _to_float(greeks_exit.get('gamma')) if greeks_exit else None,
                    'theta': _to_float(greeks_exit.get('theta')) if greeks_exit else None,
                    'vega': _to_float(greeks_exit.get('vega')) if greeks_exit else None,
                    'rho': _to_float(greeks_exit.get('rho')) if greeks_exit else None,
                    'timestamp': _iso(greeks_exit.get('timestamp')) if greeks_exit else None,
                }) if greeks_exit else 'null'

                liquidity_json = _safe_json(liquidity_breakdown) if liquidity_breakdown else 'null'

                # Insert / replace
                write_cursor.execute("""
                    REPLACE INTO top_opportunities_audit (
                        snapshot_date, snapshot_ts, rank_position,
                        opportunity_id, strategy_type, symbol, underlying,
                        strike, expiry, opportunity_type, decision, rejection_reason,
                        expected_edge_bps, cost_estimate,
                        trade_id, trade_status, active_status, direction, structure_type,
                        notional, opened_at, closed_at, time_in_trade_hours, close_reason,
                        expected_edge, realized_edge, expected_cost, executed_cost,
                        slippage, latency_ms, pnl, pnl_pct, legs_count, legging_incidents,
                        spot_price, bid, ask, mid, spread_bps, oi, volume, iv,
                        greeks_entry, greeks_exit, legs,
                        liquidity_score, liquidity_tier, liquidity_breakdown,
                        confidence_at_entry, confidence_adj_after
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s
                    )
                """, (
                    snapshot_date, snapshot_ts, rank,
                    opp.get('id'), opp.get('strategy_type'), opp.get('symbol'),
                    (trade or {}).get('underlying'),
                    _to_float(opp.get('strike')), opp.get('expiry'),
                    opp.get('opportunity_type'), opp.get('decision'),
                    opp.get('rejection_reason'),
                    _to_float(opp.get('expected_edge_bps')),
                    _to_float(opp.get('cost_estimate')),
                    trade_id,
                    (trade or {}).get('status'),
                    (trade or {}).get('active_status'),
                    (trade or {}).get('direction'),
                    (trade or {}).get('structure_type'),
                    None,  # notional (not in strategy_master_trades columns)
                    (trade or {}).get('opened_at'),
                    (trade or {}).get('closed_at'),
                    tit,
                    (trade or {}).get('close_reason'),
                    _to_float((trade or {}).get('expected_edge')),
                    _to_float((trade or {}).get('realized_edge')),
                    _to_float((trade or {}).get('expected_cost')),
                    _to_float((trade or {}).get('executed_cost')),
                    _to_float((trade or {}).get('slippage')),
                    _to_int((trade or {}).get('latency_ms')),
                    _to_float((trade or {}).get('pnl')),
                    _to_float((trade or {}).get('pnl_pct')),
                    len(legs) if legs else None,
                    None,  # legging_incidents not tracked in master row; available on trade dataclass only
                    None,  # spot_price (not recorded per-opp)
                    bid, ask, mid, spread_bps,
                    _to_int(opt_snap.get('oi')) if opt_snap else None,
                    _to_int(opt_snap.get('volume')) if opt_snap else None,
                    _to_float(opt_snap.get('iv')) if opt_snap else None,
                    greeks_entry_json, greeks_exit_json, legs_json,
                    liquidity_score_val, liquidity_tier, liquidity_json,
                    _to_float(learning.get('confidence_at_entry')) if learning else None,
                    _to_float(learning.get('confidence_adj_after')) if learning else None,
                ))
                result['inserted'] += 1

            except Exception as row_err:
                msg = f"rank={rank} sym={opp.get('symbol')} err={row_err}"
                result['errors'].append(msg)
                _log.warning(f'[top-opps-audit] row error: {msg}')

        conn.commit()
        write_cursor.close()
        cursor.close()
        _log.info(
            f'[top-opps-audit] {snapshot_date} — inserted {result["inserted"]}/{result["captured"]} rows'
        )
        return result

    except Exception as e:
        _log.error(f'[top-opps-audit] snapshot error: {e}')
        result['errors'].append(str(e))
        return result
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════════
# QUERY
# ═══════════════════════════════════════════════════════════════════════════

def query_top_opps_audit(db_fn, date_str: Optional[str] = None,
                         days: int = 1, log=None) -> Dict[str, Any]:
    """
    Query the audit table for a single date (YYYY-MM-DD) or the last `days` days.
    Returns a JSON-serializable dict.
    """
    _log = log or logger
    conn = None
    try:
        conn = db_fn()
        if conn is None:
            return {'error': 'no_db_connection', 'rows': []}
        cursor = conn.cursor(dictionary=True)

        if date_str:
            cursor.execute("""
                SELECT * FROM top_opportunities_audit
                WHERE snapshot_date = %s
                ORDER BY rank_position ASC
            """, (date_str,))
        else:
            cutoff = date.today() - timedelta(days=max(0, days - 1))
            cursor.execute("""
                SELECT * FROM top_opportunities_audit
                WHERE snapshot_date >= %s
                ORDER BY snapshot_date DESC, rank_position ASC
            """, (cutoff,))

        rows = cursor.fetchall() or []
        cursor.close()

        out_rows = []
        for r in rows:
            rr = {}
            for k, v in r.items():
                if isinstance(v, (datetime, date)):
                    rr[k] = v.isoformat()
                elif hasattr(v, '__float__'):
                    rr[k] = float(v)
                elif isinstance(v, (bytes, bytearray)):
                    try:
                        rr[k] = json.loads(v.decode('utf-8'))
                    except Exception:
                        rr[k] = v.decode('utf-8', errors='replace')
                elif isinstance(v, str) and k in ('greeks_entry', 'greeks_exit', 'legs',
                                                   'liquidity_breakdown'):
                    try:
                        rr[k] = json.loads(v)
                    except Exception:
                        rr[k] = v
                else:
                    rr[k] = v
            out_rows.append(rr)

        return {
            'date': date_str,
            'days': None if date_str else days,
            'row_count': len(out_rows),
            'rows': out_rows,
        }
    except Exception as e:
        _log.error(f'[top-opps-audit] query error: {e}')
        return {'error': str(e), 'rows': []}
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
