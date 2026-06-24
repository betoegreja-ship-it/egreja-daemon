"""Calculo de z-score e hedge ratio dinamico para pairs trade."""
import math
from typing import List, Tuple, Optional


def calc_spread_series(prices_a: List[float], prices_b: List[float],
                       method: str = 'log_ratio') -> List[float]:
    """Calcula serie temporal do spread entre 2 ativos.

    Methods:
    - 'log_ratio':  ln(a/b)  — bom pra mean reversion estatistica
    - 'price_diff': a - b    — bom pra classes ON/PN da mesma empresa
    - 'pct_diff':   (a/b - 1) * 100  — em percentual

    Returns:
        Lista de floats. Mesmo comprimento que entradas.
    """
    if len(prices_a) != len(prices_b):
        raise ValueError(f'series length mismatch: {len(prices_a)} vs {len(prices_b)}')
    out = []
    for a, b in zip(prices_a, prices_b):
        if a <= 0 or b <= 0:
            out.append(0.0)
            continue
        if method == 'log_ratio':
            out.append(math.log(a / b))
        elif method == 'price_diff':
            out.append(a - b)
        elif method == 'pct_diff':
            out.append((a / b - 1) * 100)
        else:
            out.append(math.log(a / b))
    return out


def calc_zscore(spread_series: List[float], window: int = 60) -> Optional[float]:
    """Calcula z-score atual baseado em janela rolling.

    z = (spread_atual - media_rolling) / desvio_padrao_rolling

    Args:
        spread_series: serie temporal do spread, mais recente = ultimo
        window: tamanho da janela rolling (default 60 dias)

    Returns:
        z-score atual (float), ou None se serie insuficiente
    """
    if len(spread_series) < window or window < 5:
        return None
    recent = spread_series[-window:]
    n = len(recent)
    mean = sum(recent) / n
    variance = sum((x - mean) ** 2 for x in recent) / max(n - 1, 1)
    stdev = math.sqrt(variance)
    if stdev < 1e-9:
        return 0.0
    current = spread_series[-1]
    return (current - mean) / stdev


def calc_zscore_stats(spread_series: List[float], window: int = 60) -> dict:
    """Versao expandida: retorna z + estatisticas da janela.

    Returns:
        {'z': float, 'mean': float, 'stdev': float, 'current': float,
         'min_w': float, 'max_w': float, 'n_samples': int}
    """
    if len(spread_series) < window or window < 5:
        return {'z': None, 'mean': None, 'stdev': None, 'current': None,
                'n_samples': len(spread_series)}
    recent = spread_series[-window:]
    n = len(recent)
    mean = sum(recent) / n
    variance = sum((x - mean) ** 2 for x in recent) / max(n - 1, 1)
    stdev = math.sqrt(variance)
    current = spread_series[-1]
    z = (current - mean) / stdev if stdev > 1e-9 else 0.0
    return {
        'z': round(z, 4), 'mean': round(mean, 6), 'stdev': round(stdev, 6),
        'current': round(current, 6),
        'min_w': round(min(recent), 6), 'max_w': round(max(recent), 6),
        'n_samples': n,
    }


def calc_hedge_ratio(prices_a: List[float], prices_b: List[float],
                     window: int = 60) -> Optional[float]:
    """Calcula hedge ratio dinamico via regressao linear (OLS).

    Modelo: prices_a = beta * prices_b + alpha
    Retorna beta (coeficiente angular). Eh quantas unidades de B encurtar pra
    cada unidade de A comprada.

    Returns:
        beta (float), ou None se serie insuficiente
    """
    if len(prices_a) != len(prices_b):
        return None
    if len(prices_a) < window or window < 5:
        return None
    a = prices_a[-window:]
    b = prices_b[-window:]
    n = len(a)
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    numerator = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(n))
    denominator = sum((b[i] - mean_b) ** 2 for i in range(n))
    if denominator < 1e-9:
        return 1.0
    return round(numerator / denominator, 4)


def calc_correlation(prices_a: List[float], prices_b: List[float],
                     window: int = 60) -> Optional[float]:
    """Correlacao de Pearson entre as duas series.

    Returns: float entre -1 e 1, ou None.
    """
    if len(prices_a) != len(prices_b):
        return None
    if len(prices_a) < window or window < 5:
        return None
    a = prices_a[-window:]
    b = prices_b[-window:]
    n = len(a)
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    cov = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(n)) / max(n - 1, 1)
    var_a = sum((x - mean_a) ** 2 for x in a) / max(n - 1, 1)
    var_b = sum((x - mean_b) ** 2 for x in b) / max(n - 1, 1)
    denom = math.sqrt(var_a * var_b)
    if denom < 1e-9:
        return 0.0
    return round(cov / denom, 4)
