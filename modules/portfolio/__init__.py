"""
Portfolio Accounting v11 — capital vivo por estratégia.

Fonte de verdade: capital_ledger (MySQL, append-only).
Mirror em memória: PortfolioEngine singleton.

Uso canônico:
    from modules.portfolio import PortfolioEngine, SizingEngine, LimitsEngine

    engine = PortfolioEngine.instance()
    engine.boot(db_fn=get_db)

    state = engine.get_state('stocks')
    print(state.gross_equity, state.free_capital, state.max_positions_allowed)

    size_result = SizingEngine.calc_position_size('stocks',
                                                   stop_distance_pct=0.02,
                                                   score=78)
    can_open = LimitsEngine.can_open('stocks',
                                      proposed_size=size_result['size'])

Este módulo é SHADOW-only na Fase 1/2 — não afeta o caminho crítico do
daemon até PORTFOLIO_ENGINE_ACTIVE=true (Fase 3+).
"""
from modules.portfolio.events import EventType
from modules.portfolio.state import StrategyCapitalState
from modules.portfolio.config_loader import StrategyConfig, ConfigLoader
from modules.portfolio.reducer import apply_event_to_state
from modules.portfolio.portfolio_engine import (
    PortfolioEngine,
    InsufficientCapitalError,
    DuplicateIdempotencyError,
    IntegrityViolationError,
)
from modules.portfolio.sizing_engine import SizingEngine
from modules.portfolio.limits_engine import LimitsEngine

__all__ = [
    'EventType',
    'StrategyCapitalState',
    'StrategyConfig',
    'ConfigLoader',
    'apply_event_to_state',
    'PortfolioEngine',
    'SizingEngine',
    'LimitsEngine',
    'InsufficientCapitalError',
    'DuplicateIdempotencyError',
    'IntegrityViolationError',
]

__version__ = '11.0.0-shadow'
