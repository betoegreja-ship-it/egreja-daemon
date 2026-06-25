"""Pairs Engine — Stat Arbi B3 (Pairs Trading)
==============================================

Detecta oportunidades de arbitragem estatistica entre pares correlacionados
listados na B3. Diferente do arbi cross-listed (puro, fungivel), aqui usamos
mean-reversion em z-score do spread historico.

Tipos de pares suportados:
1. Holding x Operacional   (ITUB4/ITSA4, VALE3/BRAP4)
2. Classes ON/PN           (PETR4/PETR3, BBDC4/BBDC3, GGBR4/GGBR3)
3. Setoriais correlacionados (SUZB3/KLBN11, CPFE3/CMIG4, etc)

Metodologia:
- z-score rolling: (spread_atual - media_60d) / desvio_60d
- Entry: |z| > 2.0 (anomalia 2 desvios padrao)
- Exit:  |z| < 0.5 (mean reversion proximo de zero)
- Stop:  |z| > 3.5 (regime shift potencial)

Capital: separado do arbi puro. Paper mode obrigatorio nos primeiros 60 dias.
"""

from .config import PAIRS_CONFIG, PAIRS_LIST
from .data_fetcher import fetch_pair_history, fetch_pair_quote
from .zscore import calc_zscore, calc_hedge_ratio
from .scanner import pairs_scan_loop, calc_pair_signal
from .learning import recalibrate_pair, generate_insights
from .learning_worker import pairs_learning_loop

__all__ = [
    'PAIRS_CONFIG', 'PAIRS_LIST',
    'fetch_pair_history', 'fetch_pair_quote',
    'calc_zscore', 'calc_hedge_ratio',
    'pairs_scan_loop', 'calc_pair_signal',
    'recalibrate_pair', 'generate_insights', 'pairs_learning_loop',
]
