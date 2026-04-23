"""Outcome Pathology — identifica patologias de saída.

Função: diagnose_outcome_pathologies(db_fn, log, asset_type) -> dict

Responde perguntas como:
- STOP_LOSS está comendo muito? (pct perda, largura média)
- TIMEOUT está perdendo? (trades flat que viraram timeout negativo)
- V3_REVERSAL está errando? (WR por close_reason)
- TRAILING_STOP está capturando o ganho? (quais padrões chegam em trailing)
- Tem SHORT desproporcionalmente ruim?
"""
from __future__ import annotations
from typing import Any, Callable, Dict
from .isolation import should_bypass_adaptive_learning


def diagnose_outcome_pathologies(
    db_fn: Callable,
    log,
    asset_type: str,
    lookback_days: int = 30,
) -> Dict[str, Any]:
    if should_bypass_adaptive_learning(asset_type):
        return {'bypassed': True, 'asset_type': asset_type}
    conn = None
    try:
        conn = db_fn()
        if not conn:
            return {'error': 'no_db'}
        c = conn.cursor(dictionary=True)

        # Por close_reason agregado
        c.execute(f"""
            SELECT COALESCE(close_reason,'(none)') AS close_reason,
                   COUNT(*) AS n,
                   SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS w,
                   SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) AS l,
                   AVG(pnl_pct) AS avg_pct,
                   SUM(pnl) AS total_pnl,
                   MIN(pnl_pct) AS worst_pct,
                   MAX(pnl_pct) AS best_pct
            FROM trades
            WHERE asset_type = %s AND status = 'CLOSED'
              AND closed_at > NOW() - INTERVAL {int(lookback_days)} DAY
            GROUP BY close_reason
            ORDER BY total_pnl ASC
        """, (asset_type,))
        by_reason = c.fetchall()

        # Por direction
        c.execute(f"""
            SELECT direction, COUNT(*) AS n,
                   SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS w,
                   SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) AS l,
                   AVG(pnl_pct) AS avg_pct,
                   SUM(pnl) AS total_pnl
            FROM trades
            WHERE asset_type = %s AND status = 'CLOSED'
              AND closed_at > NOW() - INTERVAL {int(lookback_days)} DAY
            GROUP BY direction
        """, (asset_type,))
        by_direction = c.fetchall()

        # Identificar patologias
        pathologies = []
        for row in by_reason:
            reason = row['close_reason']
            n = int(row['n'])
            total = float(row['total_pnl'] or 0)
            w = int(row['w'] or 0)
            l = int(row['l'] or 0)
            wr = w / max(w + l, 1)
            avg = float(row['avg_pct'] or 0)

            if reason == 'STOP_LOSS' and n >= 10 and total < 0:
                pathologies.append({
                    'severity': 'HIGH' if total < -20000 else 'MEDIUM',
                    'type': 'STOP_LOSS_CATASTRAO',
                    'asset_type': asset_type,
                    'n': n, 'wr': round(wr, 3),
                    'total_pnl': round(total, 2), 'avg_pnl_pct': round(avg, 3),
                    'hint': 'Apertar ATR_SL_MULTIPLIER',
                })
            if reason == 'V3_REVERSAL' and n >= 20 and wr < 0.35:
                pathologies.append({
                    'severity': 'HIGH',
                    'type': 'V3_REVERSAL_RUIM',
                    'asset_type': asset_type,
                    'n': n, 'wr': round(wr, 3), 'total_pnl': round(total, 2),
                    'hint': ('Desligar V3_REVERSAL crypto'
                             if asset_type == 'crypto'
                             else 'Revisar thresholds V3_REVERSAL'),
                })
            if reason == 'TIMEOUT' and n >= 50 and total < -5000:
                pathologies.append({
                    'severity': 'MEDIUM',
                    'type': 'TIMEOUT_DEFICITARIO',
                    'asset_type': asset_type,
                    'n': n, 'total_pnl': round(total, 2),
                    'hint': 'Timeout extendendo trades perdedoras — reduzir ou fechar antes',
                })
            if reason == 'TRAILING_STOP' and wr >= 0.90 and n >= 20:
                pathologies.append({
                    'severity': 'OPPORTUNITY',
                    'type': 'TRAILING_SAUDAVEL',
                    'asset_type': asset_type,
                    'n': n, 'wr': round(wr, 3), 'total_pnl': round(total, 2),
                    'hint': 'Facilitar trades chegarem em trailing (stop mais apertado + tighten_stop)',
                })

        # Patologia de direction
        if by_direction:
            long_ = next((r for r in by_direction if r['direction'] == 'LONG'), None)
            short_ = next((r for r in by_direction if r['direction'] == 'SHORT'), None)
            if short_ and long_:
                short_pnl = float(short_['total_pnl'] or 0)
                long_pnl = float(long_['total_pnl'] or 0)
                if short_pnl < -10000 and long_pnl > 0:
                    pathologies.append({
                        'severity': 'HIGH',
                        'type': 'SHORT_DEFICITARIO',
                        'asset_type': asset_type,
                        'long_pnl': round(long_pnl, 2),
                        'short_pnl': round(short_pnl, 2),
                        'hint': ('Bloquear SHORT stocks' if asset_type == 'stock'
                                 else 'Filtrar SHORT crypto mais restritivo'),
                    })

        return {
            'bypassed': False,
            'asset_type': asset_type,
            'lookback_days': lookback_days,
            'by_close_reason': by_reason,
            'by_direction': by_direction,
            'pathologies': pathologies,
        }
    except Exception as e:
        log.warning(f'[ADAPTIVE] diagnose_outcome_pathologies erro: {e}')
        return {'error': str(e)}
    finally:
        if conn:
            try: conn.close()
            except Exception: pass
