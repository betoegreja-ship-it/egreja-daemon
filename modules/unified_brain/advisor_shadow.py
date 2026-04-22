"""
Brain Advisor V4 — Shadow logger

Persiste TODA decisão do advisor (shadow ou ativa) nas tabelas:
- brain_shadow_entry_advisor
- brain_shadow_exit_advisor

Também:
- cria tabelas se não existem (idempotente)
- worker de resolução ex-post: liga shadow ↔ outcome real da trade
"""
from __future__ import annotations
import json
import os
import threading
import time
from typing import Dict, Any, Optional
from datetime import datetime, timedelta


SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'advisor_schema.sql')
_schema_lock = threading.Lock()
_schema_done = False


def ensure_advisor_schema(db_fn, log) -> bool:
    """Cria as 3 tabelas do advisor se ainda não existem.
    Idempotente — seguro chamar em cada startup."""
    global _schema_done
    with _schema_lock:
        if _schema_done:
            return True
        conn = None
        try:
            conn = db_fn()
            if not conn:
                log.warning('[ADVISOR] ensure_schema: sem DB')
                return False
            with open(SCHEMA_PATH, 'r') as f:
                sql_text = f.read()
            # Remove comentários line-by-line ANTES de split(';')
            # (senão '-- ...' no topo faz toda a statement virar comentário)
            clean_lines = []
            for line in sql_text.split('\n'):
                stripped = line.strip()
                if stripped.startswith('--') or not stripped:
                    continue
                clean_lines.append(line)
            clean_sql = '\n'.join(clean_lines)
            statements = [s.strip() for s in clean_sql.split(';') if s.strip()]
            c = conn.cursor()
            created = 0
            for stmt in statements:
                try:
                    c.execute(stmt)
                    created += 1
                except Exception as e:
                    log.debug(f'[ADVISOR] schema stmt skip: {e}')
            conn.commit()
            c.close()
            _schema_done = True
            log.info(f'[ADVISOR] schema OK ({created} statements)')
            return True
        except Exception as e:
            log.error(f'[ADVISOR] ensure_schema err: {e}')
            return False
        finally:
            try:
                if conn: conn.close()
            except Exception:
                pass


def log_entry_decision(db_fn, log, *,
                       symbol: str, asset_type: str, strategy: Optional[str],
                       market_type: Optional[str], direction: Optional[str],
                       score_v3: Optional[int], regime_v3: Optional[str],
                       atr_pct: Optional[float], hour_of_day: Optional[int],
                       weekday: Optional[int],
                       decision: Dict[str, Any],
                       motor_opened: bool, motor_size_used: Optional[int],
                       trade_id: Optional[str] = None) -> Optional[int]:
    """Grava 1 linha em brain_shadow_entry_advisor.
    Retorna id inserido (ou None se falhou)."""
    conn = None
    try:
        conn = db_fn()
        if not conn:
            return None
        c = conn.cursor()
        c.execute("""
            INSERT INTO brain_shadow_entry_advisor
              (symbol, asset_type, strategy, market_type, direction,
               score_v3, regime_v3, atr_pct, hour_of_day, weekday,
               would_action, would_size_mult, would_score_delta, would_threshold_delta,
               aggregate_score, votes_json, reason, shadow_mode,
               motor_opened, motor_size_used, trade_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            symbol, asset_type, strategy, market_type, direction,
            score_v3, regime_v3, atr_pct, hour_of_day, weekday,
            decision.get('action', 'pass'),
            float(decision.get('size_multiplier', 1.0)),
            int(decision.get('score_delta', 0)),
            int(decision.get('threshold_delta', 0)),
            float(decision.get('aggregate_score', 0.5)),
            json.dumps(decision.get('votes', {})),
            (decision.get('reason') or '')[:250],
            1 if decision.get('shadow', True) else 0,
            1 if motor_opened else 0,
            motor_size_used,
            trade_id,
        ))
        inserted_id = c.lastrowid
        conn.commit()
        c.close()
        return inserted_id
    except Exception as e:
        log.debug(f'[ADVISOR] log_entry err: {e}')
        return None
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass


def log_exit_decision(db_fn, log, *,
                      trade_id: str, symbol: str, asset_type: str,
                      strategy: Optional[str],
                      entry_price, current_price, current_pnl, current_pnl_pct,
                      peak_pnl_pct, holding_minutes,
                      score_v3_current, regime_v3_current,
                      decision: Dict[str, Any],
                      motor_action: Optional[str] = None,
                      motor_applied: bool = False) -> Optional[int]:
    """Grava 1 linha em brain_shadow_exit_advisor."""
    conn = None
    try:
        conn = db_fn()
        if not conn:
            return None
        c = conn.cursor()
        c.execute("""
            INSERT INTO brain_shadow_exit_advisor
              (trade_id, symbol, asset_type, strategy,
               entry_price, current_price, current_pnl, current_pnl_pct,
               peak_pnl_pct, holding_minutes,
               score_v3_current, regime_v3_current,
               would_action, would_size_reduction_pct, would_stop_adjustment_pct,
               confidence, aggregate_score, votes_json, reason, shadow_mode,
               motor_action, motor_applied)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            trade_id, symbol, asset_type, strategy,
            entry_price, current_price, current_pnl, current_pnl_pct,
            peak_pnl_pct, holding_minutes,
            score_v3_current, regime_v3_current,
            decision.get('action', 'hold'),
            float(decision.get('size_reduction_pct', 0.0)),
            float(decision.get('stop_adjustment_pct', 0.0)),
            float(decision.get('confidence', 0.5)),
            float(decision.get('aggregate_score', 0.5)),
            json.dumps(decision.get('votes', {})),
            (decision.get('reason') or '')[:250],
            1 if decision.get('shadow', True) else 0,
            motor_action,
            1 if motor_applied else 0,
        ))
        inserted_id = c.lastrowid
        conn.commit()
        c.close()
        return inserted_id
    except Exception as e:
        log.debug(f'[ADVISOR] log_exit err: {e}')
        return None
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass



def resolve_outcomes(db_fn, log, lookback_hours: int = 168) -> Dict[str, int]:
    """Worker de reconciliação ex-post.

    Para cada shadow record não-resolvido, tenta achar trade correspondente
    pela (symbol + timestamp próximo) ou trade_id direto, e preenche:
      - actual_pnl, actual_pnl_pct, actual_hold_minutes, actual_close_reason
      - final_pnl, final_pnl_pct, final_close_reason (exit shadow)
      - resolved_at

    Chamado periodicamente (1x/hora) por thread dedicada.
    """
    stats = {'entry_resolved': 0, 'exit_resolved': 0}
    conn = None
    try:
        conn = db_fn()
        if not conn:
            return stats
        c = conn.cursor(dictionary=True)

        # 1) Entry shadow: resolver por trade_id quando preenchido
        c.execute("""
            UPDATE brain_shadow_entry_advisor sa
            JOIN trades t ON sa.trade_id = t.trade_id
            SET sa.actual_pnl = t.pnl,
                sa.actual_pnl_pct = t.pnl_pct,
                sa.actual_hold_minutes = TIMESTAMPDIFF(MINUTE, t.opened_at, t.closed_at),
                sa.actual_close_reason = t.close_reason,
                sa.resolved_at = NOW()
            WHERE sa.resolved_at IS NULL
              AND sa.trade_id IS NOT NULL
              AND t.status = 'CLOSED'
              AND sa.created_at > NOW() - INTERVAL %s HOUR
        """, (lookback_hours,))
        stats['entry_resolved'] = c.rowcount or 0

        # 2) Exit shadow: resolver por trade_id
        c.execute("""
            UPDATE brain_shadow_exit_advisor sa
            JOIN trades t ON sa.trade_id = t.trade_id
            SET sa.final_pnl = t.pnl,
                sa.final_pnl_pct = t.pnl_pct,
                sa.final_close_reason = t.close_reason,
                sa.resolved_at = NOW()
            WHERE sa.resolved_at IS NULL
              AND t.status = 'CLOSED'
              AND sa.created_at > NOW() - INTERVAL %s HOUR
        """, (lookback_hours,))
        stats['exit_resolved'] = c.rowcount or 0

        conn.commit()
        c.close()
        if stats['entry_resolved'] + stats['exit_resolved'] > 0:
            log.info(f"[ADVISOR] resolved: entry={stats['entry_resolved']} exit={stats['exit_resolved']}")
        return stats
    except Exception as e:
        log.debug(f'[ADVISOR] resolve err: {e}')
        return stats
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass


def start_resolution_worker(db_fn, log, interval_sec: int = 3600):
    """Inicia thread que chama resolve_outcomes() periodicamente."""
    def _loop():
        log.info('[ADVISOR] resolution worker started')
        time.sleep(60)  # aguarda startup estabilizar
        while True:
            try:
                resolve_outcomes(db_fn, log)
            except Exception as e:
                log.error(f'[ADVISOR] resolution loop err: {e}')
            time.sleep(interval_sec)
    t = threading.Thread(target=_loop, name='advisor_resolution', daemon=True)
    t.start()
    return t

