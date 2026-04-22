"""
Brain Advisor V4 — Entry Advisor

Ponto de entrada único. Recebe contexto de uma possível trade,
consulta 5 votos em paralelo, combina, e retorna decisão.

Saída:
{
  "approve": bool,
  "action": "pass|block|reduce|boost",
  "score_delta": int,
  "threshold_delta": int,
  "size_multiplier": float,
  "reason": str,
  "votes": {...},
  "aggregate_score": float,
  "shadow": bool,
}

FILOSOFIA:
- IA PROPÕE. Motor decide e executa.
- Em shadow mode: approve sempre True, size_mult sempre 1.0
  (só 'action' e 'votes' são registrados para análise ex-post)
- Bypass absoluto para derivatives (should_bypass_ai)
"""
from __future__ import annotations
from datetime import datetime
from typing import Dict, Any, Optional
from .advisor_common import (
    should_bypass_ai, advisor_entry_enabled, advisor_entry_shadow,
    advisor_entry_actions, entry_weights,
    ENTRY_BLOCK_MAX, ENTRY_REDUCE_MAX, ENTRY_PASS_MAX,
    DEFAULT_NEUTRAL_VOTE,
)
from .advisor_similarity import similarity_vote
from .advisor_risk import risk_vote
from .advisor_news import news_vote


def _regime_vote(db_fn, log, *, asset_type: str, regime: Optional[str],
                 direction: Optional[str]) -> Dict[str, Any]:
    """Voto 3: performance do asset_type nesse regime+direction nos últimos 30d."""
    if not regime:
        return {'vote': DEFAULT_NEUTRAL_VOTE, 'reason': 'no_regime'}

    conn = None
    try:
        conn = db_fn()
        if not conn:
            return {'vote': DEFAULT_NEUTRAL_VOTE, 'reason': 'no_db'}
        c = conn.cursor(dictionary=True)
        sql_parts = ["asset_type=%s", "status='CLOSED'",
                     "regime_v2=%s", "closed_at > NOW() - INTERVAL 30 DAY"]
        params = [asset_type, regime]
        if direction in ('LONG', 'SHORT'):
            sql_parts.append("direction=%s")
            params.append(direction)
        sql = f"""SELECT COUNT(*) as n,
                  SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins,
                  AVG(pnl_pct) as avg_pnl_pct
                  FROM trades WHERE {' AND '.join(sql_parts)}"""
        c.execute(sql, tuple(params))
        r = c.fetchone()
        c.close()
    except Exception as e:
        log.debug(f'[ADVISOR:regime] err: {e}')
        return {'vote': DEFAULT_NEUTRAL_VOTE, 'reason': 'err'}
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

    if not r or (r['n'] or 0) < 5:
        return {'vote': 0.55, 'reason': 'thin_data'}
    n = int(r['n'])
    wins = int(r['wins'] or 0)
    wr = wins / n
    avg_pnl = float(r['avg_pnl_pct'] or 0)
    vote = min(1.0, max(0.0, wr * 1.15))
    if avg_pnl < -0.5:
        vote = max(0.0, vote - 0.1)
    return {
        'vote': round(vote, 3),
        'n_samples': n,
        'wr': round(wr, 3),
        'reason': f'n{n}_wr{int(wr*100)}',
    }


def _calendar_vote(*, asset_type: str, hour_of_day: Optional[int],
                   weekday: Optional[int]) -> Dict[str, Any]:
    """Voto 4: timing (hora do dia, weekend risk)."""
    now = datetime.now()
    h = hour_of_day if hour_of_day is not None else now.hour
    wd = weekday if weekday is not None else now.weekday()

    # Stocks B3/NYSE: últimas 2h do pregão são arriscadas (exec nova perto close)
    if asset_type == 'stock':
        if h >= 15:  # stock trading perto close
            return {'vote': 0.35, 'reason': f'late_session_h{h}'}
        if wd == 4 and h >= 14:  # sexta tarde → weekend risk
            return {'vote': 0.45, 'reason': 'friday_afternoon'}
        if wd == 0 and h < 11:  # segunda manhã → gap risk
            return {'vote': 0.55, 'reason': 'monday_open'}
        return {'vote': 0.80, 'reason': f'normal_h{h}_wd{wd}'}

    # Crypto: 24/7, mas fins de semana têm menos volume
    if asset_type == 'crypto':
        if wd in (5, 6):
            return {'vote': 0.60, 'reason': 'weekend'}
        return {'vote': 0.80, 'reason': f'normal_h{h}_wd{wd}'}

    return {'vote': DEFAULT_NEUTRAL_VOTE, 'reason': 'unknown_asset'}



def _bypass_decision(reason: str) -> Dict[str, Any]:
    """Decisão neutra que o motor interpreta como PASS sem interferência."""
    return {
        'approve': True,
        'action': 'pass',
        'score_delta': 0,
        'threshold_delta': 0,
        'size_multiplier': 1.0,
        'reason': f'bypass_{reason}',
        'votes': {},
        'aggregate_score': DEFAULT_NEUTRAL_VOTE,
        'shadow': True,
        'bypassed': True,
    }


def evaluate_entry(db_fn, log, *,
                   symbol: str,
                   asset_type: str,
                   strategy: Optional[str] = None,
                   score_v3: Optional[int] = None,
                   regime_v3: Optional[str] = None,
                   direction: Optional[str] = None,
                   atr_pct: Optional[float] = None,
                   market_type: Optional[str] = None,
                   hour_of_day: Optional[int] = None,
                   weekday: Optional[int] = None,
                   portfolio_state: Optional[Dict] = None,
                   pattern_stats: Optional[Dict] = None,
                   factor_stats: Optional[Dict] = None) -> Dict[str, Any]:
    """Avalia uma potencial entrada. Retorna decisão estruturada.

    Se advisor desligado OU derivatives → decisão neutra 'pass' (bypass).
    """
    # 1) Bypass absoluto
    if should_bypass_ai(asset_type, strategy):
        return _bypass_decision(f'deriv_or_disabled_{asset_type}')

    # 2) Advisor desligado globalmente
    if not advisor_entry_enabled():
        return _bypass_decision('entry_disabled')

    # 3) Coletar os 5 votos
    try:
        v_sim = similarity_vote(db_fn, log, symbol=symbol, asset_type=asset_type,
                                 regime=regime_v3, direction=direction)
        v_risk = risk_vote(db_fn, log, asset_type=asset_type,
                            portfolio_state=portfolio_state)
        v_regime = _regime_vote(db_fn, log, asset_type=asset_type,
                                regime=regime_v3, direction=direction)
        v_cal = _calendar_vote(asset_type=asset_type,
                               hour_of_day=hour_of_day, weekday=weekday)
        v_news = news_vote(db_fn, log, symbol=symbol, asset_type=asset_type,
                            direction=direction)
    except Exception as e:
        log.error(f'[ADVISOR:entry] vote collection err {symbol}: {e}')
        return _bypass_decision('vote_error')

    votes = {
        'similarity': v_sim['vote'],
        'risk':       v_risk['vote'],
        'regime':     v_regime['vote'],
        'calendar':   v_cal['vote'],
        'news':       v_news['vote'],
    }
    reasons = {
        'similarity': v_sim.get('reason', ''),
        'risk':       v_risk.get('reason', ''),
        'regime':     v_regime.get('reason', ''),
        'calendar':   v_cal.get('reason', ''),
        'news':       v_news.get('reason', ''),
    }

    # 4) Combinação ponderada
    w = entry_weights()
    total_w = sum(w.values())
    if total_w <= 0:
        return _bypass_decision('zero_weights')
    agg = sum(votes[k] * w.get(k, 0) for k in votes) / total_w
    agg = max(0.0, min(1.0, agg))

    # 5) Decisão
    if agg < ENTRY_BLOCK_MAX:
        action = 'block'
        size_mult = 0.0
        score_delta = 0
        threshold_delta = 0
    elif agg < ENTRY_REDUCE_MAX:
        action = 'reduce'
        size_mult = 0.6
        score_delta = -2   # força score um pouco menor
        threshold_delta = 3 # sobe threshold em 3 pts → mais seletivo
    elif agg < ENTRY_PASS_MAX:
        action = 'pass'
        size_mult = 1.0
        score_delta = 0
        threshold_delta = 0
    else:
        action = 'boost'
        size_mult = 1.25
        score_delta = 2
        threshold_delta = 0

    reason = f"agg{agg:.2f}_sim{votes['similarity']}_risk{votes['risk']}"
    shadow = advisor_entry_shadow()

    # 6) Aplicar filtro de ações ativas (quando não-shadow)
    approve = True
    if not shadow:
        active_actions = advisor_entry_actions()
        if action == 'block' and 'block' not in active_actions:
            # BLOCK não ativo ainda: registra mas deixa passar
            approve = True
            size_mult = 1.0
            reason = f'would_block_but_inactive_{reason}'
        elif action == 'reduce' and 'reduce' not in active_actions:
            size_mult = 1.0
            reason = f'would_reduce_but_inactive_{reason}'
        elif action == 'boost' and 'boost' not in active_actions:
            size_mult = 1.0
            reason = f'would_boost_but_inactive_{reason}'
        else:
            approve = (action != 'block')

    return {
        'approve': approve,
        'action': action,
        'score_delta': score_delta,
        'threshold_delta': threshold_delta,
        'size_multiplier': round(size_mult, 3),
        'reason': reason[:250],
        'votes': votes,
        'vote_reasons': reasons,
        'aggregate_score': round(agg, 3),
        'shadow': shadow,
        'bypassed': False,
    }

