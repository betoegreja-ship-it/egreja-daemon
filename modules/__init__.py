"""
Egreja Investment AI — Modular Architecture v10.23
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Institutional-grade modules extracted from the monolith.
Each module is self-contained with no circular dependencies.
"""

from modules.risk_manager import InstitutionalRiskManager
from modules.broker_base import (
    AbstractBroker, PaperBroker, BTGBroker, BinanceBroker, NYSEBroker,
    OrderTracker, BrokerFactory, OrderStatus, OrderSide, OrderType, AssetClass,
    create_order_record,
)
from modules.data_validator import MarketDataValidator, PriceAnomalyDetector, HistoricalSnapshot
from modules.auth_rbac import AuthManager, AuditLogger, Role
from modules.stats_engine import PerformanceStats
from modules.kill_switch import ExternalKillSwitch, KillSwitchMiddleware, ResumeMode
from modules.ops_metrics import OpsMetricsCollector, AlertLevel

__all__ = [
    'InstitutionalRiskManager',
    'AbstractBroker', 'PaperBroker', 'BTGBroker', 'BinanceBroker', 'NYSEBroker',
    'OrderTracker', 'BrokerFactory', 'OrderStatus', 'OrderSide', 'OrderType', 'AssetClass',
    'create_order_record',
    'MarketDataValidator', 'PriceAnomalyDetector', 'HistoricalSnapshot',
    'AuthManager', 'AuditLogger', 'Role',
    'PerformanceStats',
    'ExternalKillSwitch', 'KillSwitchMiddleware', 'ResumeMode',
    'OpsMetricsCollector', 'AlertLevel',
]

__version__ = '10.23.0'
