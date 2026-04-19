"""
ConfigLoader — lê strategy_configs do MySQL com cache TTL.
Auto-refresh a cada 60s para permitir ajuste via UPDATE sem redeploy.
"""
import logging
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Optional

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class StrategyConfig:
    strategy: str
    initial_capital: Decimal
    risk_per_trade_pct: Decimal
    max_gross_exposure_pct: Decimal
    configured_max_positions: int
    min_capital_per_trade: Decimal
    position_hard_cap: Optional[Decimal]
    sizing_mode: str
    capital_compounding_enabled: bool
    drawdown_hard_stop_pct: Decimal
    drawdown_soft_warn_pct: Decimal
    kill_switch_active: bool
    kill_switch_reason: Optional[str]


class ConfigLoader:
    _instance = None
    _cls_lock = threading.Lock()

    def __new__(cls, *a, **kw):
        if cls._instance is None:
            with cls._cls_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, db_fn: Callable = None, ttl_s: int = 60):
        if self._initialized:
            return
        self.db_fn = db_fn
        self.ttl_s = ttl_s
        self._cache: dict[str, StrategyConfig] = {}
        self._last_load: float = 0
        self._lock = threading.RLock()
        self._initialized = True

    def set_db_fn(self, db_fn: Callable) -> None:
        self.db_fn = db_fn

    def get(self, strategy: str) -> StrategyConfig:
        with self._lock:
            now = time.time()
            if now - self._last_load > self.ttl_s or strategy not in self._cache:
                self._reload()
            cfg = self._cache.get(strategy)
            if cfg is None:
                raise KeyError(f'strategy_configs sem registro para {strategy}')
            return cfg

    def all(self) -> dict[str, StrategyConfig]:
        with self._lock:
            if time.time() - self._last_load > self.ttl_s:
                self._reload()
            return dict(self._cache)

    def _reload(self) -> None:
        if self.db_fn is None:
            raise RuntimeError('ConfigLoader sem db_fn — chame set_db_fn() antes')
        conn = self.db_fn()
        if conn is None:
            raise RuntimeError('db_fn retornou None')
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute("""SELECT * FROM strategy_configs""")
            rows = cur.fetchall()
            cur.close()
        finally:
            try: conn.close()
            except Exception: pass

        new_cache = {}
        for r in rows:
            new_cache[r['strategy']] = StrategyConfig(
                strategy=r['strategy'],
                initial_capital=Decimal(str(r['initial_capital'])),
                risk_per_trade_pct=Decimal(str(r['risk_per_trade_pct'])),
                max_gross_exposure_pct=Decimal(str(r['max_gross_exposure_pct'])),
                configured_max_positions=int(r['configured_max_positions']),
                min_capital_per_trade=Decimal(str(r['min_capital_per_trade'])),
                position_hard_cap=(Decimal(str(r['position_hard_cap']))
                                   if r.get('position_hard_cap') is not None else None),
                sizing_mode=r.get('sizing_mode') or 'risk_based',
                capital_compounding_enabled=bool(r.get('capital_compounding_enabled', 1)),
                drawdown_hard_stop_pct=Decimal(str(r.get('drawdown_hard_stop_pct') or '0.25')),
                drawdown_soft_warn_pct=Decimal(str(r.get('drawdown_soft_warn_pct') or '0.15')),
                kill_switch_active=bool(r.get('kill_switch_active', 0)),
                kill_switch_reason=r.get('kill_switch_reason'),
            )
        self._cache = new_cache
        self._last_load = time.time()
        log.info(f'[ConfigLoader] reload: {len(new_cache)} strategies')

    def update(self, strategy: str, updates: dict, updated_by: str = 'api') -> None:
        """Atualiza campos da config (exige lista de campos permitidos)."""
        allowed = {
            'risk_per_trade_pct', 'max_gross_exposure_pct',
            'configured_max_positions', 'min_capital_per_trade',
            'position_hard_cap', 'sizing_mode',
            'capital_compounding_enabled',
            'drawdown_hard_stop_pct', 'drawdown_soft_warn_pct',
            'kill_switch_active', 'kill_switch_reason',
        }
        clean = {k: v for k, v in updates.items() if k in allowed}
        if not clean:
            raise ValueError(f'Nenhum campo atualizável em {list(updates.keys())}')
        conn = self.db_fn()
        try:
            cur = conn.cursor()
            sets = ', '.join(f'{k}=%s' for k in clean.keys())
            values = list(clean.values()) + [updated_by, strategy]
            cur.execute(
                f"UPDATE strategy_configs SET {sets}, updated_by=%s "
                f"WHERE strategy=%s",
                values
            )
            conn.commit()
            cur.close()
        finally:
            try: conn.close()
            except Exception: pass
        with self._lock:
            self._last_load = 0  # força reload no próximo get
