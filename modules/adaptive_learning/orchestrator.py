"""Orchestrator do Adaptive Learning Brain — pipeline completo.

Função: run_full_analysis(db_fn, log, asset_type) -> dict
    1. analyze_learning_state      (visão geral)
    2. calibrate_confidence        (bandas)
    3. build_pattern_intelligence  (padrões)
    4. diagnose_outcome_pathologies
    5. analyze_regime_effects
    6. generate_policy_proposals   (junta tudo e propõe)
    7. register + simulate shadow  (gravar e medir)
"""
from __future__ import annotations
import uuid
from typing import Any, Callable, Dict, List
from .isolation import should_bypass_adaptive_learning
from .learning_interpreter import analyze_learning_state
from .confidence_calibrator import calibrate_confidence
from .pattern_intelligence import build_pattern_intelligence
from .outcome_pathology import diagnose_outcome_pathologies
from .regime_effects import analyze_regime_effects
from .policy_update_engine import generate_policy_proposals
from .policy_simulator import simulate_policy_change
from .policy_registry import register_proposal


def run_full_analysis(
    db_fn: Callable, log, asset_type: str,
    lookback_days: int = 30,
    register_proposals: bool = True,
    simulate: bool = True,
) -> Dict[str, Any]:
    """Roda pipeline completo. Retorna dicionário com cada seção +
    lista de proposals gerados com simulação.
    """
    if should_bypass_adaptive_learning(asset_type):
        return {'bypassed': True, 'asset_type': asset_type}

    run_id = f"FULL-{asset_type[:3].upper()}-{uuid.uuid4().hex[:10]}"
    report = {
        'run_id': run_id,
        'asset_type': asset_type,
        'lookback_days': lookback_days,
    }

    try:
        report['health'] = analyze_learning_state(db_fn, log, asset_type, lookback_days)
        report['confidence'] = calibrate_confidence(
            db_fn, log, asset_type, lookback_days, run_id=run_id)
        report['patterns_count'] = len(build_pattern_intelligence(
            db_fn, log, asset_type, lookback_days=45, run_id=run_id))
        report['pathology'] = diagnose_outcome_pathologies(
            db_fn, log, asset_type, lookback_days)
        report['regime'] = analyze_regime_effects(db_fn, log, asset_type, lookback_days)

        # Gerar propostas baseadas nos relatórios
        proposals_raw = generate_policy_proposals(asset_type, report)

        # Simular e registrar
        proposals_out = []
        for p in proposals_raw:
            entry = dict(p)
            if simulate:
                entry['simulation'] = simulate_policy_change(db_fn, log, p)
            if register_proposals:
                pid = register_proposal(db_fn, log, p, run_id=run_id)
                entry['proposal_id'] = pid
            proposals_out.append(entry)
        report['proposals'] = proposals_out
        return report
    except Exception as e:
        log.warning(f'[ADAPTIVE] run_full_analysis erro: {e}')
        report['error'] = str(e)
        return report


def ensure_schema(db_fn: Callable, log) -> bool:
    """Cria as 4 tabelas do Adaptive Learning. Idempotente."""
    import os
    path = os.path.join(os.path.dirname(__file__), 'schema.sql')
    conn = None
    try:
        conn = db_fn()
        if not conn:
            return False
        with open(path, 'r') as f:
            sql_text = f.read()
        # Remove comentários line-by-line ANTES de split(';')
        clean_lines = []
        for line in sql_text.split('\n'):
            s = line.strip()
            if s.startswith('--') or not s:
                continue
            clean_lines.append(line)
        clean_sql = '\n'.join(clean_lines)
        statements = [s.strip() for s in clean_sql.split(';') if s.strip()]
        c = conn.cursor()
        for stmt in statements:
            try:
                c.execute(stmt)
            except Exception as e:
                log.debug(f'[ADAPTIVE] schema stmt skip: {e}')
        conn.commit()
        log.info(f'[ADAPTIVE] schema OK ({len(statements)} statements)')
        return True
    except Exception as e:
        log.warning(f'[ADAPTIVE] ensure_schema erro: {e}')
        return False
    finally:
        if conn:
            try: conn.close()
            except Exception: pass
