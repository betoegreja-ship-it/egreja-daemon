"""Utilidades estatísticas usadas por todo o Adaptive Learning.

Sem dependência em scipy — implementações simples mas corretas.
"""
from __future__ import annotations
import math
from typing import List, Optional, Tuple


def wilson_ci(wins: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    """Wilson score confidence interval para win rate.
    z=1.96 -> 95% CI. Retorna (low, high) no intervalo [0, 1].
    Útil pra saber se WR=50% com n=10 é confiável ou não.
    """
    if n <= 0:
        return (0.0, 0.0)
    p = wins / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / denom
    return (max(0.0, center - half), min(1.0, center + half))


def min_sample_for_significance(min_effect: float = 0.05) -> int:
    """Nº mínimo de samples pra detectar efeito de `min_effect` com 95% conf.
    Default: 30 samples = detecta 5% de diferença em WR com poder razoável.
    """
    if min_effect >= 0.15:
        return 15
    if min_effect >= 0.10:
        return 30
    if min_effect >= 0.05:
        return 100
    return 200


def profit_factor(wins_sum: float, losses_sum: float) -> Optional[float]:
    """Profit Factor = |sum(ganhos)| / |sum(perdas)|.
    PF > 1.0 = lucrativo. PF > 1.5 = bom. PF > 2.0 = excelente.
    Retorna None se não houver perdas (indefinido) ou tudo zerado.
    """
    if losses_sum is None or abs(losses_sum) < 1e-9:
        return None
    if wins_sum is None:
        return 0.0
    return abs(wins_sum) / abs(losses_sum)


def expectancy(win_rate: float, avg_win: float, avg_loss: float) -> float:
    """Expectancy = (WR * avg_win) - ((1-WR) * |avg_loss|).
    Em R$, é quanto a estratégia espera ganhar por trade.
    """
    return (win_rate * avg_win) - ((1 - win_rate) * abs(avg_loss))


def is_confidence_inverted(band_stats: List[dict], pnl_key: str = 'total_pnl') -> bool:
    """Detecta se bandas de confidence estão invertidas.
    Input: lista ordenada por banda (baixa -> alta). Cada dict tem total_pnl.
    Retorna True se bandas mais altas têm PnL PIOR que bandas mais baixas.
    Critério: correlação de Spearman < -0.3 entre banda e PnL.
    """
    if len(band_stats) < 3:
        return False
    ranks_band = list(range(len(band_stats)))
    pnls = [float(b.get(pnl_key, 0) or 0) for b in band_stats]
    ranks_pnl = _ranks(pnls)
    n = len(ranks_band)
    d_sq_sum = sum((ranks_band[i] - ranks_pnl[i]) ** 2 for i in range(n))
    spearman = 1 - (6 * d_sq_sum / (n * (n * n - 1)))
    return spearman < -0.3


def _ranks(vals: List[float]) -> List[float]:
    sorted_pairs = sorted(enumerate(vals), key=lambda x: x[1])
    ranks = [0.0] * len(vals)
    for rank, (original_idx, _) in enumerate(sorted_pairs):
        ranks[original_idx] = float(rank)
    return ranks


def classify_actionability(
    sample_size: int,
    win_rate: float,
    profit_factor_val: Optional[float],
    stability: Optional[float] = None,
) -> str:
    """Classifica quão acionável é um padrão.
    - GOLD: >=100 samples, WR>=65%, PF>=1.5, estável
    - GREEN: >=50 samples, WR>=55%, PF>=1.2
    - YELLOW: 20-50 samples, WR entre 45-55% (precisa mais dados)
    - RED: qualquer tamanho com WR<=40% ou PF<=0.7 (tóxico)
    - GREY: samples<20 ou dados ruins
    """
    if sample_size < 20:
        return 'GREY'
    if win_rate <= 0.40 or (profit_factor_val is not None and profit_factor_val <= 0.7):
        return 'RED'
    if (sample_size >= 100 and win_rate >= 0.65
            and profit_factor_val is not None and profit_factor_val >= 1.5
            and (stability is None or stability >= 0.6)):
        return 'GOLD'
    if sample_size >= 50 and win_rate >= 0.55 and (
            profit_factor_val is None or profit_factor_val >= 1.2):
        return 'GREEN'
    if 20 <= sample_size < 50 and 0.45 <= win_rate <= 0.55:
        return 'YELLOW'
    return 'GREY'
