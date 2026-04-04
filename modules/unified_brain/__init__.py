"""
Unified Brain - AI Learning Engine Module

The heart of the Egreja system. Connects and learns from all 5 modules:
- Arbitrage
- Crypto
- Stocks
- Derivatives (8 strategies)
- Long Horizon

Exports:
- create_unified_brain_tables: Initialize database schema
- create_unified_brain_blueprint: Create Flask blueprint
- LearningEngine: Core intelligence module
"""

from .schema import create_unified_brain_tables
from .endpoints import create_unified_brain_blueprint

__all__ = [
    'create_unified_brain_tables',
    'create_unified_brain_blueprint',
]
