"""
[v10.28] Capital ledger and reconciliation module.

Manages capital tracking, ledger persistence, and reconciliation logic.
All database operations are parameterized through db_fn callback.

Key functions:
  - ledger_record(strategy, event, symbol, amount, balance_after, trade_id, ...)
  - run_reconciliation(db_fn, state_getter, initial_capitals, ...)
  - persist_calibration(db_fn, calibration_tracker)
  - load_calibration(db_fn, calibration_tracker)

Pure/internal functions:
  - _reconcile_strategy(name, memory_capital, initial, open_trades, closed_trades)
  - _reconcile_strategy_arbi(memory_capital, initial, open_trades, closed_trades)
  - _replay_ledger_events(events, initial)
  - _load_ledger_from_db(strategy, db_fn)
  - _record_baseline_if_needed(db_fn, strategy_data_dict)
  - _reconcile_via_ledger(strategy, initial, memory_capital, db_fn)
"""

import threading
import logging
from datetime import datetime

log = logging.getLogger(__name__)


# ── [v10.18] CAPITAL LEDGER ──────────────────────────────────────────────

def ledger_record(strategy: str, event: str, symbol: str, amount: float,
                  balance_after: float, trade_id: str = '',
                  db_fn=None, enqueue_fn=None, ledger_lock=None,
                  ledger_list=None):
    """[v10.18] Registra evento no capital ledger (memória + DB assíncrono).
    Eventos: RESERVE | RELEASE | PNL_CREDIT | BASELINE (v10.21)

    Args:
        strategy: 'stocks' | 'crypto' | 'arbi'
        event: 'RESERVE' | 'RELEASE' | 'PNL_CREDIT' | 'BASELINE'
        symbol: Asset symbol or 'SYSTEM'
        amount: Absolute amount
        balance_after: Capital balance after event
        trade_id: Optional trade identifier
        db_fn: Callback to get database connection
        enqueue_fn: Callback to enqueue async persistence
        ledger_lock: Threading lock for ledger list
        ledger_list: In-memory ledger list (mutable, passed as list in container)
    """
    # [v10.21] Idempotência: proteger contra duplicata de RESERVE/RELEASE no mesmo trade
    if event in ('RESERVE', 'RELEASE') and trade_id and ledger_lock is not None and ledger_list is not None:
        with ledger_lock:
            recent = ledger_list[-50:] if len(ledger_list) > 50 else ledger_list
            for prev in reversed(recent):
                if (prev.get('trade_id') == trade_id and prev.get('event') == event
                        and prev.get('strategy') == strategy):
                    log.warning(f'[LEDGER-DEDUP] Skipping duplicate {event} for {trade_id}')
                    return

    evt = {
        'ts': datetime.utcnow().isoformat(),
        'strategy': strategy,
        'event': event,
        'symbol': symbol,
        'amount': round(amount, 2),
        'balance_after': round(balance_after, 2),
        'trade_id': trade_id,
    }

    # Record in memory
    if ledger_lock is not None and ledger_list is not None:
        with ledger_lock:
            ledger_list.append(evt)
            if len(ledger_list) > 5000:
                ledger_list[:] = ledger_list[-3000:]

    # Queue for persistence
    if enqueue_fn:
        enqueue_fn('ledger_event', evt)
    else:
        _db_save_ledger_event(evt, db_fn)


# ── [v10.18] Reconciliation Engine ──────────────────────────────────────

def _reconcile_strategy(name: str, memory_capital: float, initial: float,
                        open_trades: list, closed_trades: list) -> dict:
    """[v10.18] Reconcilia capital de uma estratégia (stocks/crypto).
    Pure function — no database access.
    """
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
        'ok': delta_pct < 2.0,  # Alert threshold — caller should parameterize
        'ts': datetime.utcnow().isoformat(),
    }


def _reconcile_strategy_arbi(memory_capital: float, initial: float,
                             open_trades: list, closed_trades: list) -> dict:
    """[v10.19] Reconcilia capital de arbitragem (usa position_size em vez de position_value).
    Pure function — no database access.
    """
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
        'ok': delta_pct < 2.0,  # Alert threshold
        'ts': datetime.utcnow().isoformat(),
    }


def _replay_ledger_events(events: list, initial: float) -> float:
    """[v10.20/v10.21] Replay de eventos de ledger. Pure function.
    Se há BASELINE, usa como ponto de partida.
    """
    baseline_balance = None
    baseline_idx = -1
    for i, evt in enumerate(events):
        if evt.get('event') == 'BASELINE':
            baseline_balance = float(evt.get('amount', 0))
            baseline_idx = i

    if baseline_balance is not None:
        balance = baseline_balance
        events = events[baseline_idx + 1:]
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


def _load_ledger_from_db(strategy: str, db_fn=None) -> list:
    """[v10.20] Carrega eventos do capital_ledger do MySQL para uma estratégia."""
    if not db_fn:
        return []
    conn = db_fn()
    if not conn:
        return []
    try:
        c = conn.cursor(dictionary=True)
        c.execute("SELECT event, amount FROM capital_ledger WHERE strategy=%s ORDER BY id ASC",
                  (strategy,))
        rows = c.fetchall()
        c.close()
        conn.close()
        return [{'event': r['event'], 'amount': float(r['amount'])} for r in rows]
    except Exception as e:
        log.error(f'_load_ledger_from_db({strategy}): {e}')
        try:
            conn.close()
        except:
            pass
        return []


def _record_baseline_if_needed(strategy: str, capital_value: float, initial_capital: float,
                               db_fn=None, enqueue_fn=None, ledger_lock=None, ledger_list=None):
    """[v10.21] Registra evento BASELINE no ledger para uma estratégia que nunca teve um.
    O BASELINE marca a data de corte contábil — eventos anteriores são drift pré-ledger.
    Chamado no boot, uma vez por estratégia.
    """
    if not db_fn:
        return

    conn = db_fn()
    if not conn:
        return

    has_baseline = False
    try:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM capital_ledger WHERE strategy=%s AND event='BASELINE'",
                  (strategy,))
        count = c.fetchone()[0]
        has_baseline = count > 0
        c.close()
        conn.close()
    except Exception as e:
        log.debug(f'baseline check {strategy}: {e}')
        try:
            conn.close()
        except:
            pass

    if not has_baseline:
        # Registrar baseline: amount = capital atual no momento do corte
        ledger_record(strategy, 'BASELINE', 'SYSTEM', capital_value, capital_value,
                     'BASELINE', db_fn=db_fn, enqueue_fn=enqueue_fn,
                     ledger_lock=ledger_lock, ledger_list=ledger_list)
        drift = capital_value - initial_capital
        log.info(f'[LEDGER-BASELINE] {strategy}: registered baseline at ${capital_value:,.0f} '
                f'(initial=${initial_capital:,.0f}, drift=${drift:,.0f})')


def _reconcile_via_ledger(strategy: str, initial: float, memory_capital: float,
                          db_fn=None, ledger_lock=None, ledger_list=None) -> dict:
    """[v10.20] Reconciliação via replay do ledger — segunda camada de verificação.
    Camada 2a: replay da memória (rápido, cobre runtime).
    Camada 2b: replay do MySQL (lento, cobre pós-deploy).
    Pure calculation after loading events.
    """
    events = []

    # Try memory first
    if ledger_lock is not None and ledger_list is not None:
        with ledger_lock:
            events = [e for e in ledger_list if e.get('strategy') == strategy]
        source = 'memory'
    else:
        events = _load_ledger_from_db(strategy, db_fn)
        source = 'mysql'

    if not events:
        return {
            'strategy': f'{strategy}_ledger',
            'ledger_events': 0,
            'ok': True,
            'memory_capital': round(memory_capital, 2),
            'ledger_capital': round(initial, 2),
            'delta': 0,
            'delta_pct': 0,
            'source': 'none',
            'ts': datetime.utcnow().isoformat()
        }

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
        'ok': delta_pct < 2.0,  # Alert threshold — caller should parameterize
        'ts': datetime.utcnow().isoformat(),
    }


def run_reconciliation(db_fn=None, state_lock=None,
                      get_stocks=None, get_crypto=None, get_arbi=None,
                      stocks_capital=None, crypto_capital=None, arbi_capital=None,
                      initial_stocks=None, initial_crypto=None, initial_arbi=None,
                      reconciliation_log=None, last_reconciliation_ts=None,
                      send_whatsapp_fn=None, alert_threshold=2.0, interval_s=600):
    """[v10.18/v10.19] Roda reconciliação de capital — chamado pelo watchdog a cada 10min.
    Camada 1: fórmula (initial + pnl - committed).
    Camada 2: replay do ledger (initial + eventos).
    Inclui stocks, crypto e arbi.

    Args:
        db_fn: Callback to get database connection
        state_lock: Threading lock for atomic state read
        get_stocks, get_crypto, get_arbi: Callables returning (open_list, closed_list) tuples
        stocks_capital, crypto_capital, arbi_capital: Current capital values (float)
        initial_stocks, initial_crypto, initial_arbi: Initial capital values
        reconciliation_log: Mutable list for logging results
        last_reconciliation_ts: Dict with 'ts' key for throttling
        send_whatsapp_fn: Callback for sending alerts
        alert_threshold: Delta percentage threshold for alerts
        interval_s: Throttle interval in seconds
    """
    import time
    now = time.time()
    if last_reconciliation_ts and (now - last_reconciliation_ts.get('ts', 0)) < interval_s:
        return
    if last_reconciliation_ts:
        last_reconciliation_ts['ts'] = now

    try:
        if state_lock:
            with state_lock:
                s_open, s_closed = get_stocks() if get_stocks else ([], [])
                c_open, c_closed = get_crypto() if get_crypto else ([], [])
                a_open, a_closed = get_arbi() if get_arbi else ([], [])
        else:
            s_open, s_closed = get_stocks() if get_stocks else ([], [])
            c_open, c_closed = get_crypto() if get_crypto else ([], [])
            a_open, a_closed = get_arbi() if get_arbi else ([], [])

        # Layer 1: Formula-based reconciliation
        r_stocks = _reconcile_strategy('stocks', stocks_capital or 0, initial_stocks or 0, s_open, s_closed)
        r_crypto = _reconcile_strategy('crypto', crypto_capital or 0, initial_crypto or 0, c_open, c_closed)
        r_arbi = _reconcile_strategy_arbi(arbi_capital or 0, initial_arbi or 0, a_open, a_closed)

        # Layer 2: Ledger-based reconciliation
        r_stocks_ldg = _reconcile_via_ledger('stocks', initial_stocks or 0, stocks_capital or 0, db_fn)
        r_crypto_ldg = _reconcile_via_ledger('crypto', initial_crypto or 0, crypto_capital or 0, db_fn)
        r_arbi_ldg = _reconcile_via_ledger('arbi', initial_arbi or 0, arbi_capital or 0, db_fn)

        # Log and alert
        if reconciliation_log is not None:
            for r in [r_stocks, r_crypto, r_arbi]:
                reconciliation_log.append(r)
                if not r['ok']:
                    msg = (f'[RECON-ALERT] {r["strategy"]}: delta=${r["delta"]:,.0f} '
                          f'({r["delta_pct"]:.2f}%) mem=${r["memory_capital"]:,.0f} '
                          f'calc=${r["calculated_capital"]:,.0f}')
                    log.warning(msg)
                    if send_whatsapp_fn:
                        send_whatsapp_fn(msg)
                else:
                    log.info(f'[RECON-OK] {r["strategy"]}: delta=${r["delta"]:,.0f} ({r["delta_pct"]:.2f}%)')

            for r_ldg in [r_stocks_ldg, r_crypto_ldg, r_arbi_ldg]:
                reconciliation_log.append(r_ldg)
                if not r_ldg['ok']:
                    msg = (f'[RECON-LEDGER-ALERT] {r_ldg["strategy"]}: delta=${r_ldg["delta"]:,.0f} '
                          f'({r_ldg["delta_pct"]:.2f}%) events={r_ldg["ledger_events"]}')
                    log.warning(msg)
                    if send_whatsapp_fn:
                        send_whatsapp_fn(msg)
                elif r_ldg.get('ledger_events', 0) > 0:
                    log.info(f'[RECON-LEDGER-OK] {r_ldg["strategy"]}: delta=${r_ldg["delta"]:,.0f} events={r_ldg["ledger_events"]}')

            # Cap log
            if len(reconciliation_log) > 300:
                reconciliation_log[:] = reconciliation_log[-150:]

        # Persist Layer 1 results
        if db_fn:
            conn = db_fn()
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
                    # Layer 2 persistence
                    for r_ldg in [r_stocks_ldg, r_crypto_ldg, r_arbi_ldg]:
                        c.execute("""INSERT INTO reconciliation_log
                            (ts, strategy, memory_capital, calculated_capital, committed,
                             realized_pnl, delta, delta_pct, ok)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (r_ldg['ts'], r_ldg['strategy'], r_ldg['memory_capital'],
                             r_ldg.get('ledger_capital', 0), 0, 0,
                             r_ldg['delta'], r_ldg['delta_pct'], 1 if r_ldg['ok'] else 0))
                    conn.commit()
                    c.close()
                    conn.close()
                except Exception as e:
                    log.error(f'reconciliation persist: {e}')
                    try:
                        conn.close()
                    except:
                        pass
    except Exception as e:
        log.error(f'run_reconciliation: {e}')


# ── [v10.18] Calibration Persistence ────────────────────────────────────

def persist_calibration(db_fn=None, calibration_tracker=None, last_persist_ts=None,
                       interval_s=300):
    """[v10.18] Salva _calibration_tracker no MySQL a cada 5min."""
    import time
    now = time.time()
    if last_persist_ts and (now - last_persist_ts.get('ts', 0)) < interval_s:
        return
    if last_persist_ts:
        last_persist_ts['ts'] = now

    if not db_fn or not calibration_tracker:
        return

    conn = db_fn()
    if not conn:
        return

    try:
        c = conn.cursor()
        for band, data in calibration_tracker.items():
            c.execute("""INSERT INTO calibration_tracker (band, wins, losses, total, sum_pnl_pct, updated_at)
                VALUES (%s,%s,%s,%s,%s,NOW())
                ON DUPLICATE KEY UPDATE
                wins=VALUES(wins), losses=VALUES(losses), total=VALUES(total),
                sum_pnl_pct=VALUES(sum_pnl_pct), updated_at=NOW()""",
                (band, data['wins'], data['losses'], data['total'], data['sum_pnl_pct']))
        conn.commit()
        c.close()
        conn.close()
        log.debug('[CALIB-PERSIST] calibration_tracker saved to MySQL')
    except Exception as e:
        log.error(f'persist_calibration: {e}')
        try:
            conn.close()
        except:
            pass


def load_calibration(db_fn=None, calibration_tracker=None):
    """[v10.18] Carrega _calibration_tracker do MySQL no boot — sobrevive a deploys."""
    if not db_fn or not calibration_tracker:
        return

    conn = db_fn()
    if not conn:
        return

    try:
        c = conn.cursor(dictionary=True)
        c.execute("SELECT band, wins, losses, total, sum_pnl_pct FROM calibration_tracker")
        loaded = 0
        for row in c.fetchall():
            band = row['band']
            if band in calibration_tracker:
                calibration_tracker[band]['wins'] = int(row['wins'])
                calibration_tracker[band]['losses'] = int(row['losses'])
                calibration_tracker[band]['total'] = int(row['total'])
                calibration_tracker[band]['sum_pnl_pct'] = float(row['sum_pnl_pct'])
                loaded += 1
        c.close()
        conn.close()
        if loaded > 0:
            log.info(f'[CALIB-LOAD] Loaded {loaded} bands from MySQL: '
                    + ', '.join(f'{b}={d["total"]}t' for b, d in calibration_tracker.items() if d['total'] > 0))
    except Exception as e:
        log.error(f'load_calibration: {e}')
        try:
            conn.close()
        except:
            pass


# ── Internal helpers ────────────────────────────────────────────────────────

def _db_save_ledger_event(evt: dict, db_fn=None):
    """[v10.18] Persiste evento do capital ledger no MySQL."""
    if not db_fn:
        return
    conn = db_fn()
    if not conn:
        return
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO capital_ledger (ts, strategy, event, symbol, amount, balance_after, trade_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (evt.get('ts'), evt.get('strategy'), evt.get('event'),
             evt.get('symbol', ''), evt.get('amount', 0), evt.get('balance_after', 0),
             evt.get('trade_id', '')))
        conn.commit()
        c.close()
    except Exception as e:
        log.error(f'_db_save_ledger_event: {e}')
    finally:
        try:
            conn.close()
        except:
            pass
