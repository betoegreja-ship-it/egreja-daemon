"""
StrategyCapitalState — dataclass imutável representando o estado canônico
de uma estratégia em um instante. Retornado por PortfolioEngine.get_state().

IMUTABILIDADE: frozen=True. Callers NUNCA mutam. Nova versão após
apply_event é produzida via dataclass.replace(...).
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime
from decimal import Decimal
from typing import Optional


@dataclass(frozen=True)
class StrategyCapitalState:
    # Identificação
    strategy: str  # 'stocks' | 'crypto' | 'arbi'

    # ─── Base contábil (alimentada pelo ledger) ─────────────────────────
    initial_capital: Decimal
    net_deposits: Decimal
    realized_pnl: Decimal
    reserved_capital: Decimal  # soma das reservas ativas (trades OPEN)

    # ─── Derivados calculados ───────────────────────────────────────────
    gross_equity: Decimal             # initial + net_deposits + realized_pnl
    free_capital: Decimal             # gross_equity - reserved_capital

    # ─── Exposição ──────────────────────────────────────────────────────
    current_gross_exposure: Decimal   # soma de position_value/size das abertas
    max_gross_exposure: Decimal       # gross_equity * max_gross_exposure_pct
    available_exposure: Decimal       # max_gross - current

    # ─── Capacidade operacional ─────────────────────────────────────────
    operational_buying_power: Decimal  # min(free_capital, available_exposure)
    open_positions_count: int
    max_positions_allowed: int

    # ─── Opcional (calculado sob demanda por workers específicos) ──────
    unrealized_pnl: Optional[Decimal] = None

    # ─── Metadata ───────────────────────────────────────────────────────
    version: int = 0          # incrementa a cada apply_event
    as_of: Optional[datetime] = None
    last_event_id: Optional[int] = None  # id da última row do ledger aplicada

    # ─── Helpers ────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Serializa para JSON-friendly (Decimal → float, datetime → iso)."""
        d = {}
        for k, v in asdict(self).items():
            if isinstance(v, Decimal):
                d[k] = float(v)
            elif isinstance(v, datetime):
                d[k] = v.isoformat()
            else:
                d[k] = v
        return d

    def exposure_utilization_pct(self) -> float:
        """0-100: quanto da exposição máxima está usada."""
        if self.max_gross_exposure <= 0:
            return 0.0
        return float(self.current_gross_exposure / self.max_gross_exposure) * 100

    def drawdown_from_initial_pct(self) -> float:
        """Drawdown relativo ao initial_capital (realized only)."""
        base = self.initial_capital + self.net_deposits
        if base <= 0:
            return 0.0
        pnl_pct = float(self.realized_pnl / base)
        return max(0.0, -pnl_pct)  # só reporta se negativo

    def integrity_check(self) -> tuple[bool, str]:
        """Sanity internal: free + reserved == gross_equity?"""
        computed_gross = self.initial_capital + self.net_deposits + self.realized_pnl
        if abs(computed_gross - self.gross_equity) > Decimal('0.01'):
            return False, f'gross_equity inconsistente: {computed_gross} vs {self.gross_equity}'
        computed_free = self.gross_equity - self.reserved_capital
        if abs(computed_free - self.free_capital) > Decimal('0.01'):
            return False, f'free_capital inconsistente: {computed_free} vs {self.free_capital}'
        if self.reserved_capital < -Decimal('0.01'):
            return False, f'reserved_capital negativo: {self.reserved_capital}'
        return True, 'ok'


def empty_state(strategy: str, initial_capital: Decimal) -> StrategyCapitalState:
    """Cria state inicial (pre-BASELINE)."""
    zero = Decimal('0')
    return StrategyCapitalState(
        strategy=strategy,
        initial_capital=initial_capital,
        net_deposits=zero,
        realized_pnl=zero,
        reserved_capital=zero,
        gross_equity=initial_capital,
        free_capital=initial_capital,
        current_gross_exposure=zero,
        max_gross_exposure=zero,
        available_exposure=zero,
        operational_buying_power=zero,
        open_positions_count=0,
        max_positions_allowed=0,
        version=0,
        as_of=datetime.utcnow(),
        last_event_id=None,
    )
