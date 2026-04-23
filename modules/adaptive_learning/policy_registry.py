"""Policy Registry — gravação e ciclo de vida de propostas.

Responsabilidades:
- register_proposal(): grava em learning_policy_proposals
- list_proposals(status=...)
- approve_proposal(id, by, note)
- rollout_proposal(id)
- rollback_proposal(id, reason)

Estados: proposed -> approved -> rolled_out -> rolled_back
"""
from __future__ import annotations
import json
from typing import Any, Callable, Dict, List, Optional


def register_proposal(
    db_fn: Callable, log,
    proposal: Dict[str, Any],
    run_id: str = 'manual',
) -> Optional[int]:
    """Grava proposta e retorna id inserido."""
    conn = None
    try:
        conn = db_fn()
        if not conn: return None
        c = conn.cursor()
        c.execute("""INSERT INTO learning_policy_proposals
            (run_id, proposal_type, target_scope, current_value, proposed_value,
             rationale, evidence_json, expected_impact_json,
             risk_level, confidence_score, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'proposed')""", (
            run_id, proposal.get('proposal_type'),
            proposal.get('target_scope'),
            proposal.get('current_value'),
            proposal.get('proposed_value'),
            proposal.get('rationale'),
            json.dumps(proposal.get('evidence_json', {})),
            json.dumps(proposal.get('expected_impact_json', {})),
            proposal.get('risk_level', 'medio'),
            proposal.get('confidence_score'),
        ))
        new_id = c.lastrowid
        conn.commit()
        return new_id
    except Exception as e:
        log.warning(f'[ADAPTIVE] register_proposal erro: {e}')
        return None
    finally:
        if conn:
            try: conn.close()
            except Exception: pass


def list_proposals(db_fn: Callable, log, status: Optional[str] = None,
                   limit: int = 50) -> List[Dict]:
    conn = None
    try:
        conn = db_fn()
        if not conn: return []
        c = conn.cursor(dictionary=True)
        if status:
            c.execute("""SELECT * FROM learning_policy_proposals
                         WHERE status=%s ORDER BY id DESC LIMIT %s""",
                      (status, int(limit)))
        else:
            c.execute("""SELECT * FROM learning_policy_proposals
                         ORDER BY id DESC LIMIT %s""", (int(limit),))
        return c.fetchall()
    except Exception as e:
        log.warning(f'[ADAPTIVE] list_proposals erro: {e}')
        return []
    finally:
        if conn:
            try: conn.close()
            except Exception: pass


def approve_proposal(db_fn: Callable, log, proposal_id: int,
                     approved_by: str, note: str = '') -> bool:
    conn = None
    try:
        conn = db_fn()
        if not conn: return False
        c = conn.cursor()
        c.execute("""UPDATE learning_policy_proposals
            SET status='approved', approved_at=NOW(), approved_by=%s, approval_note=%s
            WHERE id=%s AND status='proposed'""",
            (approved_by, note, int(proposal_id)))
        conn.commit()
        return c.rowcount > 0
    except Exception as e:
        log.warning(f'[ADAPTIVE] approve erro: {e}')
        return False
    finally:
        if conn:
            try: conn.close()
            except Exception: pass


def rollout_proposal(db_fn: Callable, log, proposal_id: int) -> bool:
    """Marca proposta como rolled_out (aplicação EFETIVA é manual via Railway env var)."""
    conn = None
    try:
        conn = db_fn()
        if not conn: return False
        c = conn.cursor()
        c.execute("""UPDATE learning_policy_proposals
            SET status='rolled_out', rolled_out_at=NOW()
            WHERE id=%s AND status='approved'""", (int(proposal_id),))
        conn.commit()
        return c.rowcount > 0
    except Exception as e:
        log.warning(f'[ADAPTIVE] rollout erro: {e}')
        return False
    finally:
        if conn:
            try: conn.close()
            except Exception: pass


def rollback_proposal(db_fn: Callable, log, proposal_id: int, reason: str) -> bool:
    conn = None
    try:
        conn = db_fn()
        if not conn: return False
        c = conn.cursor()
        c.execute("""UPDATE learning_policy_proposals
            SET status='rolled_back', rolled_back_at=NOW(),
                approval_note = CONCAT(COALESCE(approval_note,''), ' | ROLLBACK: ', %s)
            WHERE id=%s""", (reason, int(proposal_id)))
        conn.commit()
        return c.rowcount > 0
    except Exception as e:
        log.warning(f'[ADAPTIVE] rollback erro: {e}')
        return False
    finally:
        if conn:
            try: conn.close()
            except Exception: pass
