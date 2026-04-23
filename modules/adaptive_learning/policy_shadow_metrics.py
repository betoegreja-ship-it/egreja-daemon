"""Policy Shadow Metrics — mede o que teria acontecido com cada proposta.

Worker/função que roda periodicamente (ex: a cada hora) e pra cada proposta
em status 'proposed' ou 'approved' (não ainda rolled_out) calcula métricas
do "mundo paralelo" e registra em learning_policy_outcomes.

Uso: confirmar se proposta vale a pena antes de rollout real.
"""
from __future__ import annotations
import json
from typing import Any, Callable, Dict
from .policy_simulator import simulate_policy_change


def measure_shadow_impact(
    db_fn: Callable, log, proposal: Dict[str, Any],
) -> bool:
    """Mede impacto shadow e grava em learning_policy_outcomes.
    Retorna True se conseguiu medir.
    """
    proposal_id = proposal.get('id')
    if not proposal_id:
        return False

    sim = simulate_policy_change(db_fn, log, proposal)
    if sim.get('error'):
        return False

    conn = None
    try:
        conn = db_fn()
        if not conn: return False
        c = conn.cursor()
        for metric_name, val in sim.items():
            if metric_name == 'analise':
                continue
            if isinstance(val, (int, float)):
                try:
                    c.execute("""INSERT INTO learning_policy_outcomes
                        (proposal_id, metric_name, metric_value, metric_unit, metadata_json)
                        VALUES (%s,%s,%s,%s,%s)""", (
                        int(proposal_id), metric_name[:50], float(val), '',
                        json.dumps({'analise': sim.get('analise','')}),
                    ))
                except Exception as e:
                    log.debug(f'[ADAPTIVE] shadow metric skip: {e}')
        conn.commit()
        return True
    except Exception as e:
        log.warning(f'[ADAPTIVE] shadow_impact erro: {e}')
        return False
    finally:
        if conn:
            try: conn.close()
            except Exception: pass
