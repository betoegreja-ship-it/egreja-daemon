"""Adaptive Learning Brain v1 — Egreja Investment AI.

Sistema que LÊ profundamente o histórico acumulado (trades, padrões,
confidence, outcomes) e transforma em propostas de política.

NÃO executa decisões diretamente. NÃO usa LLM. NÃO mexe em derivatives/arbi.

Entry points públicos:
    run_full_analysis(asset_type) -> dict  # pipeline completo
    should_bypass_adaptive_learning(asset_type, strategy) -> bool

Módulos:
    learning_interpreter    : visão consolidada
    pattern_intelligence    : padrões úteis vs tóxicos
    confidence_calibrator   : calibra bandas de confidence
    outcome_pathology       : patologias de saída
    regime_effects          : efeitos de regime
    policy_update_engine    : gera propostas
    policy_simulator        : simula impacto
    policy_registry         : registra proposals/rollouts
    policy_shadow_metrics   : mede o que teria acontecido
"""

from .isolation import should_bypass_adaptive_learning

__all__ = ['should_bypass_adaptive_learning']
__version__ = '1.0.0'
