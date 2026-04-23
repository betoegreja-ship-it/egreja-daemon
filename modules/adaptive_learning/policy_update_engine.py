"""Policy Update Engine — transforma diagnóstico em propostas de política.

Função: generate_policy_proposals(asset_type, report) -> List[dict]

Consome o output de learning_interpreter + pattern_intelligence +
confidence_calibrator + outcome_pathology + regime_effects e gera
propostas concretas com:
  - proposal_type, target_scope
  - current_value, proposed_value
  - risk_level (baixo/medio/alto)
  - rationale, evidence_json, expected_impact_json

NÃO aplica nada. Só propõe. Gravação é via policy_registry.
"""
from __future__ import annotations
from typing import Any, Dict, List
from .isolation import should_bypass_adaptive_learning


def generate_policy_proposals(
    asset_type: str,
    report: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Gera propostas baseadas no relatório consolidado.

    Args:
        asset_type: 'stock' ou 'crypto'
        report: dict com chaves 'confidence', 'pathology', 'regime', 'health'
    """
    if should_bypass_adaptive_learning(asset_type):
        return []

    proposals: List[Dict[str, Any]] = []

    conf = report.get('confidence', {})
    path = report.get('pathology', {})
    hlth = report.get('health', {})

    # ─── Proposta 1: dead_zone de confidence ───────────────────
    dz = conf.get('dead_zone_suggested')
    if dz and conf.get('inverted') is False:
        proposals.append({
            'proposal_type': 'ENV_VAR_UPDATE',
            'target_scope': f'learning.{asset_type}',
            'current_value': 'LEARNING_DEAD_ZONE_LOW=58 / HIGH=63',
            'proposed_value': f'LEARNING_DEAD_ZONE_LOW={dz[0]} / HIGH={dz[1]}',
            'rationale': (f'Bandas {dz[0]}-{dz[1]} são deficitárias em {asset_type}. '
                          f'Dead zone atual (58-63) é estreita demais.'),
            'evidence_json': {'confidence_bands': conf.get('bands', [])},
            'expected_impact_json': {
                'trades_bloqueadas_estimadas': 'a simular',
                'pnl_protegido_estimado': 'a simular',
            },
            'risk_level': 'baixo',
            'confidence_score': 75.0,
        })

    # ─── Proposta 2: inversão de confidence crypto ─────────────
    if conf.get('inverted'):
        proposals.append({
            'proposal_type': 'SCORE_DELTA',
            'target_scope': f'advisor_v4.entry.{asset_type}',
            'current_value': 'score_delta=0 para banda extrema',
            'proposed_value': 'score_delta=-10 para confidence>=80 em crypto (inversão detectada)',
            'rationale': f'Confidence está INVERTIDA em {asset_type}: bandas altas dão PnL pior.',
            'evidence_json': {'bands': conf.get('bands', [])},
            'expected_impact_json': {
                'estrategia': 'penalizar score quando confidence alta em crypto',
            },
            'risk_level': 'medio',
            'confidence_score': 65.0,
        })

    # ─── Proposta 3: STOP_LOSS catastrão ───────────────────────
    for p in path.get('pathologies', []):
        t = p.get('type')
        if t == 'STOP_LOSS_CATASTRAO':
            cur_mult = '2.5' if asset_type == 'stock' else '2.0'
            new_mult = '1.5' if asset_type == 'stock' else '1.3'
            proposals.append({
                'proposal_type': 'ENV_VAR_UPDATE',
                'target_scope': f'monitor.stop_loss.{asset_type}',
                'current_value': f'ATR_SL_MULTIPLIER_{asset_type.upper()}={cur_mult}',
                'proposed_value': f'ATR_SL_MULTIPLIER_{asset_type.upper()}={new_mult}',
                'rationale': (f'STOP_LOSS em {asset_type}: {p["n"]} trades, WR {p["wr"]*100:.1f}%, '
                              f'PnL ${p["total_pnl"]:,.2f}. Reduzir largura do stop para sair antes.'),
                'evidence_json': p,
                'expected_impact_json': {
                    'estrategia': 'stop mais apertado → perda menor por stop → trailing começa antes',
                },
                'risk_level': 'medio',
                'confidence_score': 70.0,
            })
        elif t == 'SHORT_DEFICITARIO' and asset_type == 'stock':
            proposals.append({
                'proposal_type': 'FEATURE_FLAG',
                'target_scope': 'motor.short.stocks',
                'current_value': 'ALLOW_SHORT_STOCKS=true',
                'proposed_value': 'ALLOW_SHORT_STOCKS=false',
                'rationale': (f'SHORT stocks: PnL ${p["short_pnl"]:,.2f} (LONG ${p["long_pnl"]:,.2f}). '
                              'Estruturalmente deficitário.'),
                'evidence_json': p,
                'expected_impact_json': {'pnl_protegido': abs(p['short_pnl'])},
                'risk_level': 'medio',
                'confidence_score': 85.0,
            })
        elif t == 'V3_REVERSAL_RUIM' and asset_type == 'crypto':
            proposals.append({
                'proposal_type': 'FEATURE_FLAG',
                'target_scope': 'monitor.v3_reversal.crypto',
                'current_value': 'V3_REVERSAL_CRYPTO_ENABLED=true',
                'proposed_value': 'V3_REVERSAL_CRYPTO_ENABLED=false',
                'rationale': f'V3_REVERSAL crypto: {p["n"]} trades, WR {p["wr"]*100:.1f}%, PnL ${p["total_pnl"]:,.2f}',
                'evidence_json': p,
                'expected_impact_json': {'pnl_protegido': abs(p['total_pnl'])},
                'risk_level': 'medio',
                'confidence_score': 80.0,
            })

    # ─── Proposta 4: thresholds do Advisor Exit ────────────────
    if any(p.get('type') == 'STOP_LOSS_CATASTRAO'
           for p in path.get('pathologies', [])):
        proposals.append({
            'proposal_type': 'ENV_VAR_UPDATE',
            'target_scope': 'advisor_v4.exit',
            'current_value': 'ADVISOR_EXIT_CLOSE_MIN=0.80',
            'proposed_value': 'ADVISOR_EXIT_CLOSE_MIN=0.60',
            'rationale': ('Exit Advisor nunca atinge 0.80 → nunca fecha. '
                          'Baixar threshold para ele conseguir agir antes do STOP_LOSS.'),
            'evidence_json': {
                'trigger': 'STOP_LOSS_CATASTRAO detectado',
                'sugestao_secundaria': 'EXIT_TIGHTEN_MIN=0.45',
            },
            'expected_impact_json': {
                'estrategia': 'advisor pode fechar antes de bater stop_loss',
            },
            'risk_level': 'medio',
            'confidence_score': 70.0,
        })

    return proposals
