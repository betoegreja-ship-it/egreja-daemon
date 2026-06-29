"""Egreja Meta-Brain.

Read-only operational awareness layer. It observes the trading books and
returns health, capital gates and briefings. It never opens, closes or mutates
trades.
"""

from .analyzer import (
    build_meta_status,
    build_briefing,
    build_market_edge,
    build_error_report,
    build_capital_gate,
    build_patterns,
    build_recommendations,
    build_data_sources,
    build_intelligence,
    answer_question,
)

__all__ = [
    'build_meta_status',
    'build_briefing',
    'build_market_edge',
    'build_error_report',
    'build_capital_gate',
    'build_patterns',
    'build_recommendations',
    'build_data_sources',
    'build_intelligence',
    'answer_question',
]
