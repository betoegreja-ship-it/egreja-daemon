"""
Brain Advisor V4 — Métricas agregadas

Worker diário que calcula métricas contrafactuais:
- Quanto advisor teria economizado em vetos?
- Quanto teria adicionado em boosts?
- WR / PF com vs sem advisor?

Popula brain_advisor_metrics.
"""
from __future__ import annotations
import threading
import time
from typing import Dict, Any, Optional
from datetime import datetime, date, timedelta


def compute_daily_metrics(db_fn, log, target_date: Optional[date] = None) -> Dict[str, Any]:
    """Calcula métricas do advisor para uma data. Default = ontem."""
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    stats = {'entry_stock': {}, 'entry_crypto': {},
             'exit_stock': {}, 'exit_crypto': {}}

    conn = None
    try:
        conn = db_fn()
        if not conn:
            return stats
        c = conn.cursor(dictionary=True)

        # Entry metrics por asset_type
        for asset_type in ('stock', 'crypto'):
            c.execute("""
                SELECT 
                  COUNT(*) as n_total,
                  SUM(CASE WHEN would_action='block'  THEN 1 ELSE 0 END) as n_block,
                  SUM(CASE WHEN would_action='reduce' THEN 1 ELSE 0 END) as n_reduce,
                  SUM(CASE WHEN would_action='pass'   THEN 1 ELSE 0 END) as n_pass,
                  SUM(CASE WHEN would_action='boost'  THEN 1 ELSE 0 END) as n_boost,
                  SUM(CASE WHEN would_action='block'  AND actual_pnl IS NOT NULL
                          THEN -actual_pnl ELSE 0 END) as pnl_saved_by_block,
                  SUM(CASE WHEN would_action='boost'  AND actual_pnl IS NOT NULL
                          THEN actual_pnl * 0.25 ELSE 0 END) as pnl_added_by_boost
                FROM brain_shadow_entry_advisor
                WHERE asset_type=%s AND DATE(created_at)=%s
            """, (asset_type, target_date))
            r = c.fetchone() or {}
            stats[f'entry_{asset_type}'] = {k: float(v or 0) if v is not None else 0
                                              for k, v in r.items()}

            # Exit metrics
            c.execute("""
                SELECT 
                  COUNT(*) as n_total,
                  SUM(CASE WHEN would_action='close'        THEN 1 ELSE 0 END) as n_close,
                  SUM(CASE WHEN would_action='reduce'       THEN 1 ELSE 0 END) as n_reduce,
                  SUM(CASE WHEN would_action='hold'         THEN 1 ELSE 0 END) as n_hold,
                  SUM(CASE WHEN would_action='tighten_stop' THEN 1 ELSE 0 END) as n_tighten_stop
                FROM brain_shadow_exit_advisor
                WHERE asset_type=%s AND DATE(created_at)=%s
            """, (asset_type, target_date))
            r2 = c.fetchone() or {}
            stats[f'exit_{asset_type}'] = {k: float(v or 0) if v is not None else 0
                                             for k, v in r2.items()}

        # Persistir em brain_advisor_metrics
        for asset_type in ('stock', 'crypto'):
            for kind in ('entry', 'exit'):
                st = stats[f'{kind}_{asset_type}']
                if not st or (st.get('n_total', 0) == 0):
                    continue
                c.execute("""
                    INSERT INTO brain_advisor_metrics
                      (metric_date, asset_type, advisor_kind,
                       n_decisions, n_block, n_reduce, n_pass, n_boost,
                       n_hold, n_close, n_tighten_stop,
                       pnl_saved_by_block, pnl_added_by_boost)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                       n_decisions=VALUES(n_decisions),
                       n_block=VALUES(n_block),
                       n_reduce=VALUES(n_reduce),
                       n_pass=VALUES(n_pass),
                       n_boost=VALUES(n_boost),
                       n_hold=VALUES(n_hold),
                       n_close=VALUES(n_close),
                       n_tighten_stop=VALUES(n_tighten_stop),
                       pnl_saved_by_block=VALUES(pnl_saved_by_block),
                       pnl_added_by_boost=VALUES(pnl_added_by_boost)
                """, (
                    target_date, asset_type, kind,
                    int(st.get('n_total', 0)),
                    int(st.get('n_block', 0)),
                    int(st.get('n_reduce', 0)),
                    int(st.get('n_pass', 0)),
                    int(st.get('n_boost', 0)),
                    int(st.get('n_hold', 0)),
                    int(st.get('n_close', 0)),
                    int(st.get('n_tighten_stop', 0)),
                    float(st.get('pnl_saved_by_block', 0)),
                    float(st.get('pnl_added_by_boost', 0)),
                ))
        conn.commit()
        c.close()
        log.info(f'[ADVISOR:metrics] daily metrics saved for {target_date}')
        return stats
    except Exception as e:
        log.error(f'[ADVISOR:metrics] err: {e}')
        return stats
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass


def start_metrics_worker(db_fn, log, interval_sec: int = 3600 * 6):
    """Worker que calcula métricas a cada 6h (2-4x por dia)."""
    def _loop():
        log.info('[ADVISOR:metrics] worker started')
        time.sleep(120)  # aguarda startup
        while True:
            try:
                compute_daily_metrics(db_fn, log)
            except Exception as e:
                log.error(f'[ADVISOR:metrics] loop err: {e}')
            time.sleep(interval_sec)
    t = threading.Thread(target=_loop, name='advisor_metrics', daemon=True)
    t.start()
    return t

