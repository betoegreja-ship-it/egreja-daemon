"""
Brain Advisor V4 — Exit Advisor

Avalia trades ABERTAS e sugere: HOLD / REDUCE / CLOSE / TIGHTEN_STOP.

REGRA INTOCÁVEL (crypto trailing):
- Quando crypto já está em zona de trailing protection
  (peak_pnl_pct >= TRAILING_PEAK_CRYPTO),
  Exit Advisor NUNCA sugere CLOSE. Só pode TIGHTEN_STOP.
- Isso garante que a regra do motor V3 seja respeitada
  (captura do lucro pelo trailing dispara naturalmente).

FILOSOFIA:
- IA PROPÕE. Motor decide e executa.
- Em shadow mode: action sempre 'hold' efetivo (só grava o que FARIA).
- Bypass absoluto para derivatives.
"""
from __future__ import annotations
from datetime import datetime
from typing import Dict, Any, Optional
from .advisor_common import (
    should_bypass_ai, advisor_exit_enabled, advisor_exit_shadow,
    advisor_exit_actions, exit_weights,
    EXIT_CLOSE_MIN, EXIT_REDUCE_MIN, EXIT_ATTN_MIN,
    DEFAULT_NEUTRAL_VOTE, is_crypto_in_trailing_protection,
)
from .advisor_similarity import exit_similarity_outcome
from .advisor_risk import risk_exit_vote
from .advisor_news import news_vote


def _pnl_protection_vote(*, current_pnl_pct: float, peak_pnl_pct: float,
                         asset_type: str) -> Dict[str, Any]:
    """Voto: quanto mais longe do peak, mais alto (proteger lucro)."""
    if peak_pnl_pct <= 0:
        return {'vote': 0.3, 'reason': 'no_peak'}

    drawdown_from_peak = peak_pnl_pct - current_pnl_pct

    # Stocks: drawdown de 0.5pp já preocupa
    # Crypto: respeita trailing (só preocupa além do TRAILING_DROP_CRYPTO)
    if asset_type == 'crypto':
        # Se já estamos na zona protegida, motor V3 já cuida
        if is_crypto_in_trailing_protection(current_pnl_pct, peak_pnl_pct):
            return {'vote': 0.25, 'reason': 'trailing_zone_motor_handles'}
        # Fora da zona de trailing ativo, drawdown forte pode sinalizar fim
        if drawdown_from_peak > 1.5:
            return {'vote': 0.70, 'reason': f'deep_dd_{drawdown_from_peak:.2f}'}
        return {'vote': 0.35, 'reason': f'dd_{drawdown_from_peak:.2f}'}

    # Stocks
    if drawdown_from_peak > 1.5:
        return {'vote': 0.85, 'reason': f'deep_dd_{drawdown_from_peak:.2f}'}
    if drawdown_from_peak > 0.8:
        return {'vote': 0.65, 'reason': f'mid_dd_{drawdown_from_peak:.2f}'}
    if drawdown_from_peak > 0.4:
        return {'vote': 0.50, 'reason': f'small_dd_{drawdown_from_peak:.2f}'}
    return {'vote': 0.25, 'reason': f'tight_{drawdown_from_peak:.2f}'}


def _regime_deterioration_vote(*, regime_at_entry: Optional[str],
                                regime_current: Optional[str],
                                score_at_entry: Optional[int],
                                score_current: Optional[int]) -> Dict[str, Any]:
    """Voto: quanto o contexto piorou desde abertura."""
    if not regime_at_entry or not regime_current:
        return {'vote': DEFAULT_NEUTRAL_VOTE, 'reason': 'no_regime_data'}

    regime_changed = (regime_at_entry != regime_current)
    # Score V3 degradou (tava 75 na entrada, agora 50)
    score_delta = (score_current or 50) - (score_at_entry or 50)

    if regime_changed and score_delta < -15:
        return {'vote': 0.85, 'reason': f'regime+score_bad_d{score_delta}'}
    if regime_changed:
        return {'vote': 0.65, 'reason': 'regime_changed'}
    if score_delta < -20:
        return {'vote': 0.75, 'reason': f'score_dropped_{score_delta}'}
    if score_delta < -10:
        return {'vote': 0.55, 'reason': f'score_weak_{score_delta}'}
    return {'vote': 0.25, 'reason': 'stable'}


def _time_decay_vote(*, holding_minutes: int, asset_type: str,
                     current_pnl_pct: float) -> Dict[str, Any]:
    """Voto: quanto tempo já na posição sem evolução."""
    hours = holding_minutes / 60.0

    # Stocks: 5h é limite natural; crypto: 24h
    limit_h = 5.0 if asset_type == 'stock' else 24.0

    if hours < limit_h * 0.5:
        return {'vote': 0.20, 'reason': f'early_{hours:.1f}h'}
    if hours < limit_h:
        if abs(current_pnl_pct) < 0.3:
            return {'vote': 0.55, 'reason': f'stale_{hours:.1f}h_flat'}
        return {'vote': 0.35, 'reason': f'mid_{hours:.1f}h'}
    if hours < limit_h * 1.5:
        return {'vote': 0.70, 'reason': f'late_{hours:.1f}h'}
    return {'vote': 0.85, 'reason': f'overstay_{hours:.1f}h'}


def _bypass_exit() -> Dict[str, Any]:
    return {
        'action': 'hold',
        'confidence': 0.5,
        'size_reduction_pct': 0.0,
        'stop_adjustment_pct': 0.0,
        'reason': 'bypass',
        'votes': {},
        'aggregate_score': 0.0,
        'shadow': True,
        'bypassed': True,
    }



def evaluate_exit(db_fn, log, *,
                  trade_id: str,
                  symbol: str,
                  asset_type: str,
                  strategy: Optional[str] = None,
                  entry_price: Optional[float] = None,
                  current_price: Optional[float] = None,
                  current_pnl: Optional[float] = None,
                  current_pnl_pct: Optional[float] = None,
                  peak_pnl_pct: Optional[float] = None,
                  holding_minutes: Optional[int] = None,
                  score_v3_entry: Optional[int] = None,
                  score_v3_current: Optional[int] = None,
                  regime_v3_entry: Optional[str] = None,
                  regime_v3_current: Optional[str] = None,
                  direction: Optional[str] = None,
                  portfolio_state: Optional[Dict] = None) -> Dict[str, Any]:
    """Avalia exit de uma trade aberta. Retorna decisão estruturada."""
    # 1) Bypass absoluto
    if should_bypass_ai(asset_type, strategy):
        d = _bypass_exit()
        d['reason'] = f'bypass_deriv_or_disabled_{asset_type}'
        return d

    # 2) Exit advisor desligado
    if not advisor_exit_enabled():
        d = _bypass_exit()
        d['reason'] = 'exit_disabled'
        return d

    cur_pnl_pct = float(current_pnl_pct or 0.0)
    peak = float(peak_pnl_pct or cur_pnl_pct)
    hold_min = int(holding_minutes or 0)

    # 3) Coletar 5 votos
    try:
        v_pnl = _pnl_protection_vote(current_pnl_pct=cur_pnl_pct,
                                      peak_pnl_pct=peak,
                                      asset_type=asset_type)
        v_regime = _regime_deterioration_vote(regime_at_entry=regime_v3_entry,
                                               regime_current=regime_v3_current,
                                               score_at_entry=score_v3_entry,
                                               score_current=score_v3_current)
        v_news = news_vote(db_fn, log, symbol=symbol, asset_type=asset_type,
                            direction=direction)
        # Para exit, invertemos news_vote: muitas news → queremos sair
        v_news_exit = {'vote': round(1.0 - v_news['vote'], 3),
                       'reason': 'inv_' + v_news.get('reason', '')}
        v_time = _time_decay_vote(holding_minutes=hold_min,
                                   asset_type=asset_type,
                                   current_pnl_pct=cur_pnl_pct)
        v_risk = risk_exit_vote(db_fn, log, asset_type=asset_type,
                                 current_pnl=float(current_pnl or 0),
                                 portfolio_state=portfolio_state)
    except Exception as e:
        log.error(f'[ADVISOR:exit] vote err {trade_id}: {e}')
        d = _bypass_exit()
        d['reason'] = 'vote_err'
        return d

    votes = {
        'pnl_protection':       v_pnl['vote'],
        'regime_deterioration': v_regime['vote'],
        'news_exit':            v_news_exit['vote'],
        'time_decay':           v_time['vote'],
        'risk_exit':            v_risk['vote'],
    }

    # 4) Combinação ponderada
    w = exit_weights()
    total_w = sum(w.values())
    agg = sum(votes[k] * w.get(k, 0) for k in votes) / max(total_w, 0.001)
    agg = max(0.0, min(1.0, agg))

    # 5) Hierarquia de decisão
    # REGRA INTOCÁVEL: crypto em trailing zone → nunca CLOSE, só TIGHTEN_STOP
    crypto_in_trail = (asset_type == 'crypto' and
                       is_crypto_in_trailing_protection(cur_pnl_pct, peak))

    if agg > EXIT_CLOSE_MIN:
        if crypto_in_trail:
            # Força tighten_stop em vez de close (motor V3 termina o trabalho)
            action = 'tighten_stop'
            size_reduction_pct = 0.0
            stop_adjustment_pct = -0.3  # aperta 0.3pp
            reason = f'agg{agg:.2f}_crypto_trail_tighten_only'
        else:
            action = 'close'
            size_reduction_pct = 100.0
            stop_adjustment_pct = 0.0
            reason = f'agg{agg:.2f}_close'
    elif agg >= EXIT_REDUCE_MIN:
        if cur_pnl_pct > 0.5:
            # Em lucro: apertar stop
            action = 'tighten_stop'
            size_reduction_pct = 0.0
            stop_adjustment_pct = -0.2
            reason = f'agg{agg:.2f}_tighten'
        else:
            # Reduzir metade
            action = 'reduce'
            size_reduction_pct = 50.0
            stop_adjustment_pct = 0.0
            reason = f'agg{agg:.2f}_reduce50'
    elif agg >= EXIT_ATTN_MIN:
        action = 'hold'
        size_reduction_pct = 0.0
        stop_adjustment_pct = 0.0
        reason = f'agg{agg:.2f}_hold_attn'
    else:
        action = 'hold'
        size_reduction_pct = 0.0
        stop_adjustment_pct = 0.0
        reason = f'agg{agg:.2f}_hold'

    shadow = advisor_exit_shadow()

    # 6) Filtrar ações ativas (se não-shadow)
    if not shadow:
        active = advisor_exit_actions()
        if action == 'close' and 'close' not in active:
            action = 'hold'
            size_reduction_pct = 0.0
            reason = f'would_close_but_inactive_{reason}'
        elif action == 'reduce' and 'reduce' not in active:
            action = 'hold'
            size_reduction_pct = 0.0
            reason = f'would_reduce_but_inactive_{reason}'
        elif action == 'tighten_stop' and 'tighten_stop' not in active:
            action = 'hold'
            stop_adjustment_pct = 0.0
            reason = f'would_tighten_but_inactive_{reason}'

    return {
        'action': action,
        'confidence': round(agg, 3),
        'size_reduction_pct': round(size_reduction_pct, 2),
        'stop_adjustment_pct': round(stop_adjustment_pct, 2),
        'reason': reason[:250],
        'votes': votes,
        'aggregate_score': round(agg, 3),
        'shadow': shadow,
        'bypassed': False,
    }

