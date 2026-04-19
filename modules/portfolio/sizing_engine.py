"""
SizingEngine — calcula tamanho de posição a partir do state canônico.
Funções puras que lêem do PortfolioEngine + ConfigLoader.

Clamps em ordem:
  1. raw = risk_amount / stop_distance_pct
  2. aplicar score_multiplier (confidence)
  3. min(size, free_capital)
  4. min(size, available_exposure)
  5. min(size, position_hard_cap)
  6. min(size, 20% do gross_equity) — safety global
  7. se size < min_capital_per_trade → bloqueado
"""
import logging
from decimal import Decimal

from modules.portfolio.config_loader import ConfigLoader
from modules.portfolio.portfolio_engine import PortfolioEngine

log = logging.getLogger(__name__)


class SizingEngine:
    """Interface stateless. Métodos estáticos (não precisa instância)."""

    @staticmethod
    def _score_multiplier(score: int) -> Decimal:
        """Multiplicador por confidence. 0..100."""
        if score >= 85: return Decimal('1.50')
        if score >= 75: return Decimal('1.25')
        if score >= 65: return Decimal('1.00')
        if score >= 55: return Decimal('0.80')
        return Decimal('0.50')

    @staticmethod
    def calc_position_size(
        strategy: str,
        stop_distance_pct: float,
        score: int = 70,
        *,
        override_risk_pct: float = None,
    ) -> dict:
        """
        Retorna:
          {
            'size': Decimal (0 se bloqueado),
            'raw_size': Decimal,
            'risk_amount': Decimal,
            'score_multiplier': Decimal,
            'blocked': bool,
            'reason': str | None,
            'clamps_applied': list[str],
            'state_version': int,
            'strategy': str,
          }
        """
        engine = PortfolioEngine.instance()
        state = engine.get_state(strategy)
        cfg = engine.config_loader.get(strategy)

        if cfg.kill_switch_active:
            return {
                'size': Decimal('0'), 'blocked': True,
                'reason': f'kill_switch: {cfg.kill_switch_reason or "active"}',
                'strategy': strategy, 'state_version': state.version,
            }

        stop_pct_dec = Decimal(str(max(stop_distance_pct, 0.001)))
        risk_pct = Decimal(str(override_risk_pct)) if override_risk_pct else cfg.risk_per_trade_pct

        # Base
        risk_amount = state.gross_equity * risk_pct
        raw_size = risk_amount / stop_pct_dec

        # Score multiplier
        score_mult = SizingEngine._score_multiplier(score)
        size = raw_size * score_mult

        clamps_applied = []

        # Clamp 1: free_capital
        if size > state.free_capital:
            clamps_applied.append(f'free_capital:{float(state.free_capital):.0f}')
            size = state.free_capital

        # Clamp 2: available_exposure
        if size > state.available_exposure:
            clamps_applied.append(f'available_exposure:{float(state.available_exposure):.0f}')
            size = state.available_exposure

        # Clamp 3: position_hard_cap
        if cfg.position_hard_cap and size > cfg.position_hard_cap:
            clamps_applied.append(f'hard_cap:{float(cfg.position_hard_cap):.0f}')
            size = cfg.position_hard_cap

        # Clamp 4: safety global (20% do equity por trade)
        safety_cap = state.gross_equity * Decimal('0.20')
        if size > safety_cap:
            clamps_applied.append(f'global_20pct:{float(safety_cap):.0f}')
            size = safety_cap

        # Clamp 5: min threshold
        if size < cfg.min_capital_per_trade:
            return {
                'size': Decimal('0'),
                'raw_size': round(raw_size, 2),
                'risk_amount': round(risk_amount, 2),
                'score_multiplier': float(score_mult),
                'blocked': True,
                'reason': f'below_min:{float(cfg.min_capital_per_trade):.0f}',
                'clamps_applied': clamps_applied,
                'state_version': state.version,
                'strategy': strategy,
            }

        # Sanity: nunca negativo
        if size < 0:
            size = Decimal('0')

        return {
            'size': round(size, 2),
            'raw_size': round(raw_size, 2),
            'risk_amount': round(risk_amount, 2),
            'score_multiplier': float(score_mult),
            'blocked': False,
            'reason': None,
            'clamps_applied': clamps_applied,
            'state_version': state.version,
            'strategy': strategy,
        }
