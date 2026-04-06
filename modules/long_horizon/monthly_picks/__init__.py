"""
Monthly Picks Sleeve — Egreja Investment AI v3.2

Sleeve modular do Long Horizon que seleciona e gerencia posições
de curto/médio prazo (3–9 meses) com governança, maturidade e aprendizado.

Arquitetura:
  Long Horizon Core  →  calcula score, tese, risco, qualidade
  Monthly Picks Sleeve →  escolhe, entra, revisa, sai e aprende

Fluxo:
  1. Todo 1º dia útil do mês: scan → top 10 → deep analysis → escolhe 3
  2. Toda segunda: review semanal de posições abertas
  3. Auto-close por 6 gatilhos + thesis_broken + human_override
  4. Aprendizado via learning_bridge (eventos estruturados → Brain)

Níveis de maturidade (mesmos do derivativos):
  OBSERVE → SHADOW_EXEC → PAPER_SMALL → PAPER_FULL
"""

from .config import MonthlyPicksConfig, SleeveStatus, get_config
from .repositories import MonthlyPicksRepository, create_monthly_picks_tables
from .endpoints import create_monthly_picks_blueprint

__all__ = [
    'MonthlyPicksConfig',
    'SleeveStatus',
    'get_config',
    'MonthlyPicksRepository',
    'create_monthly_picks_tables',
    'create_monthly_picks_blueprint',
]
