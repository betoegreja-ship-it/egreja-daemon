"""Pattern Intelligence — descobre padrões úteis vs tóxicos.

Função: build_pattern_intelligence(db_fn, log, asset_type) -> List[dict]

Pra cada feature_hash com N>=20 samples, calcula métricas contextualizadas
por (direction, regime, close_reason) e classifica em:
GOLD / GREEN / YELLOW / RED / GREY (ver stats_helpers.classify_actionability).

Escreve resultado em learning_pattern_intelligence.
"""
from __future__ import annotations
import uuid
from typing import Any, Callable, Dict, List
from .isolation import should_bypass_adaptive_learning
from .stats_helpers import (
    profit_factor, classify_actionability, wilson_ci,
)


def build_pattern_intelligence(
    db_fn: Callable,
    log,
    asset_type: str,
    lookback_days: int = 45,
    min_samples: int = 20,
    persist: bool = True,
    run_id: str = None,
) -> List[Dict[str, Any]]:
    """Retorna lista de patterns analisados + persiste em learning_pattern_intelligence.

    Cada item: {
      pattern_hash, asset_type, direction, regime,
      sample_size, win_rate, profit_factor, avg_pnl_pct, total_pnl,
      stop_loss_rate, trailing_rate, timeout_rate, reversal_rate,
      actionability, wr_ci_low, wr_ci_high, tags
    }
    """
    if should_bypass_adaptive_learning(asset_type):
        return []

    run_id = run_id or f"PI-{uuid.uuid4().hex[:12]}"
    conn = None
    try:
        conn = db_fn()
        if not conn:
            return []
        c = conn.cursor(dictionary=True)

        # Pegar feature_hashes com samples suficientes
        c.execute(f"""
            SELECT feature_hash,
                   COUNT(*) AS n,
                   SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins,
                   SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) AS losses,
                   SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) AS wins_sum,
                   SUM(CASE WHEN pnl < 0 THEN pnl ELSE 0 END) AS losses_sum,
                   AVG(pnl_pct) AS avg_pnl_pct,
                   SUM(pnl) AS total_pnl,
                   SUM(CASE WHEN close_reason = 'STOP_LOSS' THEN 1 ELSE 0 END) AS n_stop,
                   SUM(CASE WHEN close_reason = 'TRAILING_STOP' THEN 1 ELSE 0 END) AS n_trail,
                   SUM(CASE WHEN close_reason = 'TIMEOUT' THEN 1 ELSE 0 END) AS n_timeout,
                   SUM(CASE WHEN close_reason = 'V3_REVERSAL' THEN 1 ELSE 0 END) AS n_rev,
                   direction
            FROM trades
            WHERE asset_type = %s AND status = 'CLOSED'
              AND feature_hash IS NOT NULL
              AND closed_at > NOW() - INTERVAL {int(lookback_days)} DAY
            GROUP BY feature_hash, direction
            HAVING n >= {int(min_samples)}
            ORDER BY n DESC
            LIMIT 500
        """, (asset_type,))

        results = []
        for row in c.fetchall():
            n = int(row['n'])
            w = int(row['wins'] or 0)
            l = int(row['losses'] or 0)
            wr = w / max(w + l, 1) if (w + l) else 0.0
            pf = profit_factor(
                float(row['wins_sum'] or 0),
                float(row['losses_sum'] or 0),
            )
            n_stop = int(row['n_stop'] or 0)
            n_trail = int(row['n_trail'] or 0)
            n_to = int(row['n_timeout'] or 0)
            n_rev = int(row['n_rev'] or 0)
            wr_low, wr_high = wilson_ci(w, w + l) if (w + l) else (0.0, 0.0)
            actionability = classify_actionability(n, wr, pf)

            tags = []
            if n_stop / n >= 0.30: tags.append('high_stop_rate')
            if n_trail / n >= 0.40: tags.append('high_trailing_rate')
            if n_to / n >= 0.30: tags.append('high_timeout_rate')
            if n_rev / n >= 0.20: tags.append('high_reversal_rate')
            if wr_high - wr_low > 0.30: tags.append('low_confidence_ci')

            item = {
                'run_id': run_id,
                'pattern_hash': row['feature_hash'],
                'asset_type': asset_type,
                'direction': row.get('direction'),
                'regime': None,
                'sample_size': n,
                'win_rate': round(wr, 4),
                'profit_factor': round(pf, 4) if pf is not None else None,
                'avg_pnl_pct': round(float(row['avg_pnl_pct'] or 0), 4),
                'total_pnl': round(float(row['total_pnl'] or 0), 2),
                'stop_loss_rate': round(n_stop / n, 4),
                'trailing_rate': round(n_trail / n, 4),
                'timeout_rate': round(n_to / n, 4),
                'reversal_rate': round(n_rev / n, 4),
                'wr_ci_low': round(wr_low, 4),
                'wr_ci_high': round(wr_high, 4),
                'actionability': actionability,
                'tags': tags,
            }
            results.append(item)

        if persist and results:
            _persist(conn, results, log)

        return results
    except Exception as e:
        log.warning(f'[ADAPTIVE] build_pattern_intelligence erro: {e}')
        return []
    finally:
        if conn:
            try: conn.close()
            except Exception: pass


def _persist(conn, items: List[Dict], log) -> None:
    import json
    c = conn.cursor()
    for it in items:
        try:
            c.execute("""INSERT INTO learning_pattern_intelligence
                (run_id, pattern_hash, asset_type, direction, regime,
                 sample_size, win_rate, profit_factor, avg_pnl_pct, total_pnl,
                 stop_loss_rate, trailing_rate, timeout_rate, reversal_rate,
                 actionability, tags_json)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                it['run_id'], it['pattern_hash'], it['asset_type'],
                it.get('direction'), it.get('regime'),
                it['sample_size'], it['win_rate'], it.get('profit_factor'),
                it['avg_pnl_pct'], it['total_pnl'],
                it['stop_loss_rate'], it['trailing_rate'],
                it['timeout_rate'], it['reversal_rate'],
                it['actionability'],
                json.dumps(it.get('tags', [])),
            ))
        except Exception as e:
            log.debug(f'[ADAPTIVE] persist pattern skip: {e}')
    conn.commit()
