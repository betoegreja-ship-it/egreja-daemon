"""Regime Effects — mede performance por regime de mercado.

Função: analyze_regime_effects(db_fn, log, asset_type) -> dict

Responde: em qual regime (TRENDING/RANGING/CHOPPY) o sistema ganha/perde mais?
Útil pra sugerir filtros contextuais.
"""
from __future__ import annotations
from typing import Any, Callable, Dict
from .isolation import should_bypass_adaptive_learning


def analyze_regime_effects(
    db_fn: Callable, log, asset_type: str, lookback_days: int = 30,
) -> Dict[str, Any]:
    if should_bypass_adaptive_learning(asset_type):
        return {'bypassed': True}
    conn = None
    try:
        conn = db_fn()
        if not conn:
            return {'error': 'no_db'}
        c = conn.cursor(dictionary=True)
        # signal_events tem market_regime_mode
        c.execute(f"""
            SELECT se.market_regime_mode AS regime, t.direction,
                   COUNT(*) AS n,
                   SUM(CASE WHEN t.pnl > 0 THEN 1 ELSE 0 END) AS w,
                   SUM(CASE WHEN t.pnl < 0 THEN 1 ELSE 0 END) AS l,
                   AVG(t.pnl_pct) AS avg_pct,
                   SUM(t.pnl) AS total_pnl
            FROM trades t
            INNER JOIN signal_events se ON se.trade_id = t.id
            WHERE t.asset_type = %s AND t.status='CLOSED'
              AND t.closed_at > NOW() - INTERVAL {int(lookback_days)} DAY
              AND se.market_regime_mode IS NOT NULL
            GROUP BY se.market_regime_mode, t.direction
            ORDER BY total_pnl DESC
        """, (asset_type,))
        rows = c.fetchall()

        # Ranking por regime (independente de direction)
        regime_totals = {}
        for r in rows:
            reg = r['regime'] or 'UNKNOWN'
            regime_totals.setdefault(reg, {'n': 0, 'pnl': 0.0, 'w': 0, 'l': 0})
            regime_totals[reg]['n'] += int(r['n'])
            regime_totals[reg]['pnl'] += float(r['total_pnl'] or 0)
            regime_totals[reg]['w'] += int(r['w'] or 0)
            regime_totals[reg]['l'] += int(r['l'] or 0)
        ranking = sorted(
            [{'regime': k, **v, 'wr': v['w']/max(v['w']+v['l'],1)}
             for k,v in regime_totals.items()],
            key=lambda x: x['pnl'], reverse=True,
        )

        insights = []
        for r in ranking:
            if r['n'] >= 50 and r['pnl'] < -5000:
                insights.append({
                    'type': 'REGIME_DEFICITARIO',
                    'regime': r['regime'],
                    'n': r['n'], 'total_pnl': round(r['pnl'], 2),
                    'hint': f"Reduzir exposição em regime {r['regime']}",
                })
            if r['n'] >= 50 and r['pnl'] > 10000 and r['wr'] > 0.55:
                insights.append({
                    'type': 'REGIME_FAVORAVEL',
                    'regime': r['regime'],
                    'n': r['n'], 'wr': round(r['wr'], 3),
                    'total_pnl': round(r['pnl'], 2),
                    'hint': f"Priorizar trades em regime {r['regime']}",
                })

        return {
            'bypassed': False, 'asset_type': asset_type,
            'by_regime_direction': rows, 'ranking': ranking,
            'insights': insights,
        }
    except Exception as e:
        log.warning(f'[ADAPTIVE] regime_effects erro: {e}')
        return {'error': str(e)}
    finally:
        if conn:
            try: conn.close()
            except Exception: pass
