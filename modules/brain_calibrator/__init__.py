"""Brain Calibrator — sistema auto-evolutivo de aprendizado.

Lê os trades históricos a cada hora, descobre quais features predizem
PnL real, e ajusta o score do brain antes da decisão de entry.

NUNCA DELETA — o conhecimento acumulado é o ativo do sistema.

Arquitetura:
1. learner.recalibrate_brain()    — roda a cada 1h, atualiza pesos
2. scorer.apply_calibration(...)  — chama no fluxo de entry
3. worker.calibrator_loop()       — thread infinita
4. schema.py                       — tabelas MySQL (IF NOT EXISTS)
"""
from .schema import create_calibrator_tables
from .learner import recalibrate_brain, get_baseline_winrate
from .scorer import apply_calibration, get_active_weights, score_breakdown
from .worker import calibrator_loop

__all__ = [
    'create_calibrator_tables',
    'recalibrate_brain', 'get_baseline_winrate',
    'apply_calibration', 'get_active_weights', 'score_breakdown',
    'calibrator_loop',
]
