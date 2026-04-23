"""Policy Simulator — estima impacto de uma proposta ANTES de aplicar.

Função: simulate_policy_change(db_fn, log, proposal) -> dict

Usa histórico real pra responder: "se essa política estivesse vigente
nos últimos 30d, quantas trades teriam sido afetadas? qual o PnL delta?"
"""
from __future__ import annotations
from typing import Any, Callable, Dict


def simulate_policy_change(db_fn: Callable, log, proposal: Dict[str, Any]) -> Dict[str, Any]:
    """Simula proposta específica. Cada tipo de proposta tem simulador próprio."""
    pt = proposal.get('proposal_type')
    scope = proposal.get('target_scope', '')
    conn = None
    try:
        conn = db_fn()
        if not conn: return {'error': 'no_db'}
        c = conn.cursor(dictionary=True)

        # ─── Simular FEATURE_FLAG ALLOW_SHORT_STOCKS=false ───
        if proposal.get('proposed_value') == 'ALLOW_SHORT_STOCKS=false':
            c.execute("""
                SELECT COUNT(*) AS n, SUM(pnl) AS total_pnl,
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS w,
                       SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) AS l
                FROM trades WHERE asset_type='stock' AND direction='SHORT'
                AND status='CLOSED' AND closed_at > NOW() - INTERVAL 30 DAY
            """)
            r = c.fetchone()
            return {
                'trades_afetadas': int(r['n']),
                'pnl_removido': float(r['total_pnl'] or 0),
                'wins_removidos': int(r['w'] or 0),
                'losses_removidos': int(r['l'] or 0),
                'pnl_liquido_estimado': -float(r['total_pnl'] or 0),
                'analise': ('Bloquear SHORT remove todas essas trades. '
                            f'PnL histórico: ${r["total_pnl"] or 0:,.2f}'),
            }

        # ─── Simular V3_REVERSAL_CRYPTO_ENABLED=false ───
        if proposal.get('proposed_value') == 'V3_REVERSAL_CRYPTO_ENABLED=false':
            c.execute("""
                SELECT COUNT(*) AS n, SUM(pnl) AS total_pnl,
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS w
                FROM trades WHERE asset_type='crypto'
                AND close_reason='V3_REVERSAL' AND status='CLOSED'
                AND closed_at > NOW() - INTERVAL 30 DAY
            """)
            r = c.fetchone()
            return {
                'trades_que_teriam_continuado': int(r['n']),
                'pnl_evitado_negativo': float(r['total_pnl'] or 0),
                'analise': 'Essas trades teriam esperado TIMEOUT/TRAILING/STOP em vez de V3_REVERSAL',
            }

        # ─── ATR_SL_MULTIPLIER reduzido (aproximação) ───
        if 'ATR_SL_MULTIPLIER' in (proposal.get('proposed_value') or ''):
            # Aproximação: trades que bateram STOP e perderam >1.5% seriam fechadas antes
            at = 'stock' if 'STOCK' in proposal.get('proposed_value','') else 'crypto'
            c.execute(f"""
                SELECT COUNT(*) AS n, SUM(pnl_pct) AS sum_pct, SUM(pnl) AS total
                FROM trades WHERE asset_type=%s AND close_reason='STOP_LOSS'
                AND status='CLOSED' AND pnl_pct < -1.5
                AND closed_at > NOW() - INTERVAL 30 DAY
            """, (at,))
            r = c.fetchone()
            return {
                'trades_afetadas': int(r['n']),
                'pnl_pct_medio_atual': float(r['sum_pct'] or 0) / max(int(r['n']), 1),
                'analise': (f'Aproximadamente {r["n"]} trades teriam sido cortadas '
                            f'mais cedo. Perda estimada seria ~50% menor.'),
                'pnl_economizado_estimado': abs(float(r['total'] or 0)) * 0.4,
            }

        return {'analise': 'Simulação genérica — nenhuma regra específica pra esse proposal_type'}
    except Exception as e:
        log.warning(f'[ADAPTIVE] simulate erro: {e}')
        return {'error': str(e)}
    finally:
        if conn:
            try: conn.close()
            except Exception: pass
