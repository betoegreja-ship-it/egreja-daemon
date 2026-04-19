"""
LimitsEngine — decide se uma nova posição pode ser aberta.
Lê do PortfolioEngine + ConfigLoader. Funções puras.
"""
import logging
from decimal import Decimal
from typing import Optional

from modules.portfolio.portfolio_engine import PortfolioEngine

log = logging.getLogger(__name__)


class LimitsEngine:

    @staticmethod
    def can_open(
        strategy: str,
        proposed_size: Optional[Decimal] = None,
    ) -> dict:
        """
        Retorna:
          {
            'can_open': bool,
            'strategy': str,
            'checks': [ (name: str, passed: bool, detail: str) ],
            'state_snapshot': dict,   (serializado)
          }
        Checks roda TODOS (não short-circuit) para diagnóstico completo.
        """
        engine = PortfolioEngine.instance()
        state = engine.get_state(strategy)
        cfg = engine.config_loader.get(strategy)

        checks = []
        blocked = False

        # 1. Kill switch
        if cfg.kill_switch_active:
            checks.append(('kill_switch', False,
                           f'ativo: {cfg.kill_switch_reason or ""}'))
            blocked = True
        else:
            checks.append(('kill_switch', True, 'ok'))

        # 2. Max positions
        if state.open_positions_count >= state.max_positions_allowed:
            checks.append(('max_positions', False,
                           f'{state.open_positions_count}/{state.max_positions_allowed}'))
            blocked = True
        else:
            checks.append(('max_positions', True,
                           f'{state.open_positions_count}/{state.max_positions_allowed}'))

        # 3. Buying power mínimo
        if state.operational_buying_power < cfg.min_capital_per_trade:
            checks.append(('buying_power', False,
                           f'obp={float(state.operational_buying_power):.0f} '
                           f'< min={float(cfg.min_capital_per_trade):.0f}'))
            blocked = True
        else:
            checks.append(('buying_power', True,
                           f'{float(state.operational_buying_power):.0f}'))

        # 4. Drawdown hard stop
        dd_pct = state.drawdown_from_initial_pct()
        if Decimal(str(dd_pct)) >= cfg.drawdown_hard_stop_pct:
            checks.append(('drawdown_hard', False,
                           f'{dd_pct:.2%} >= {float(cfg.drawdown_hard_stop_pct):.2%}'))
            blocked = True
        else:
            checks.append(('drawdown_hard', True, f'{dd_pct:.2%}'))

        # 5. Size proposto vs free_capital (se fornecido)
        if proposed_size is not None:
            ps = Decimal(str(proposed_size))
            if ps > state.free_capital:
                checks.append(('proposed_size_vs_free', False,
                               f'proposed={float(ps):.0f} > free={float(state.free_capital):.0f}'))
                blocked = True
            elif ps > state.available_exposure:
                checks.append(('proposed_size_vs_exposure', False,
                               f'proposed={float(ps):.0f} > avail_exp={float(state.available_exposure):.0f}'))
                blocked = True
            else:
                checks.append(('proposed_size', True, f'{float(ps):.0f} ok'))

        # 6. Integrity check interno
        ok_int, msg = state.integrity_check()
        if not ok_int:
            checks.append(('state_integrity', False, msg))
            blocked = True
        else:
            checks.append(('state_integrity', True, 'ok'))

        return {
            'can_open': not blocked,
            'strategy': strategy,
            'checks': checks,
            'state_snapshot': state.to_dict(),
        }

    @staticmethod
    def max_positions_allowed(strategy: str) -> int:
        """Shortcut para leitura pontual."""
        engine = PortfolioEngine.instance()
        return engine.get_state(strategy).max_positions_allowed

    @staticmethod
    def operational_buying_power(strategy: str) -> Decimal:
        engine = PortfolioEngine.instance()
        return engine.get_state(strategy).operational_buying_power
