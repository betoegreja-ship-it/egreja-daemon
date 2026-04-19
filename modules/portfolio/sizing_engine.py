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
        stop_distance_pct: float = 0.02,
        score: int = 70,
        *,
        override_risk_pct: float = None,
    ) -> dict:
        """
        Retorna dict com size calculado + metadata.

        Modo é determinado por cfg.sizing_mode:
          - 'risk_based'       (default): size = risk_amount / stop_distance_pct
          - 'capital_fraction': size = free_capital / slots_restantes
                                (usa 100% do capital dividido entre slots livres)
          - 'external'         : retorna raw=0, delega para caller
                                (arbi pode usar ARBI_PAIR_CONFIG próprio)
        """
        engine = PortfolioEngine.instance()
        state = engine.get_state(strategy)
        cfg = engine.config_loader.get(strategy)

        if cfg.kill_switch_active:
            return {
                'size': Decimal('0'), 'blocked': True,
                'reason': f'kill_switch: {cfg.kill_switch_reason or "active"}',
                'strategy': strategy, 'state_version': state.version,
                'sizing_mode': cfg.sizing_mode,
            }

        clamps_applied = []
        risk_amount = None
        score_mult = Decimal('1.0')
        raw_size = Decimal('0')

        # ─── Branch: qual modo de sizing? ──────────────────────────────
        if cfg.sizing_mode == 'capital_fraction':
            # Modo: divide capital livre pelos slots restantes.
            # Ex: 3 slots max, 1 aberto, free=$2.6M → size = $2.6M / 2 = $1.3M
            # Após abrir, free cai para $1.3M com 2 slots ocupados e 1 livre
            # → próximo size = $1.3M (consistente, 100% usado).
            slots_remaining = cfg.configured_max_positions - state.open_positions_count
            if slots_remaining <= 0:
                return {
                    'size': Decimal('0'), 'blocked': True,
                    'reason': f'no_slots_available (positions {state.open_positions_count}/{cfg.configured_max_positions})',
                    'clamps_applied': [], 'strategy': strategy,
                    'state_version': state.version, 'sizing_mode': cfg.sizing_mode,
                }
            # Raw = todo o capital livre / slots livres
            raw_size = state.free_capital / Decimal(slots_remaining)
            size = raw_size
            # Sem score_multiplier neste modo — sizing é de capacidade,
            # não de convicção. (Se quiser bump por convicção, mude para
            # risk_based ou ajuste risk_per_trade_pct para baixo.)

        elif cfg.sizing_mode == 'external':
            # Arbi (ou outra estratégia) decide sizing fora. Engine só
            # valida na hora de reservar. Retornamos 0 com flag clara.
            return {
                'size': Decimal('0'),
                'raw_size': Decimal('0'),
                'blocked': False,  # não é bloqueio — é delegação
                'reason': 'sizing_externo — caller decide tamanho',
                'clamps_applied': [], 'strategy': strategy,
                'state_version': state.version, 'sizing_mode': cfg.sizing_mode,
                'free_capital': float(state.free_capital),
                'operational_buying_power': float(state.operational_buying_power),
            }

        else:  # 'risk_based' (default)
            stop_pct_dec = Decimal(str(max(stop_distance_pct, 0.001)))
            risk_pct = (Decimal(str(override_risk_pct))
                        if override_risk_pct else cfg.risk_per_trade_pct)
            risk_amount = state.gross_equity * risk_pct
            raw_size = risk_amount / stop_pct_dec
            # Score multiplier só no modo risk_based
            score_mult = SizingEngine._score_multiplier(score)
            size = raw_size * score_mult

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

        # Clamp 4: safety global 20% do equity — SÓ em risk_based.
        # No capital_fraction o objetivo é JUSTAMENTE permitir fracoes
        # maiores (arbi 33%, crypto 5%, stocks 4%). No external, o
        # caller decide — tambem nao cabe este clamp.
        if cfg.sizing_mode == 'risk_based':
            safety_cap = state.gross_equity * Decimal('0.20')
            if size > safety_cap:
                clamps_applied.append(f'global_20pct:{float(safety_cap):.0f}')
                size = safety_cap

        # Clamp 5: min threshold
        if size < cfg.min_capital_per_trade:
            return {
                'size': Decimal('0'),
                'raw_size': round(raw_size, 2),
                'risk_amount': round(risk_amount, 2) if risk_amount is not None else None,
                'score_multiplier': float(score_mult),
                'blocked': True,
                'reason': f'below_min:{float(cfg.min_capital_per_trade):.0f}',
                'clamps_applied': clamps_applied,
                'state_version': state.version,
                'strategy': strategy,
                'sizing_mode': cfg.sizing_mode,
            }

        # Sanity: nunca negativo
        if size < 0:
            size = Decimal('0')

        return {
            'size': round(size, 2),
            'raw_size': round(raw_size, 2),
            'risk_amount': round(risk_amount, 2) if risk_amount is not None else None,
            'score_multiplier': float(score_mult),
            'blocked': False,
            'reason': None,
            'clamps_applied': clamps_applied,
            'state_version': state.version,
            'strategy': strategy,
            'sizing_mode': cfg.sizing_mode,
        }
