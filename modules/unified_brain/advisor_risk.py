"""
Brain Advisor V4 — Voto de Risco Consolidado

Avalia risco consolidado do dia atual:
- PnL do dia por asset_type
- Número de stops recentes
- Concentração (muitas posições abertas no mesmo lado)
- Kill switch state
"""
from __future__ import annotations
from typing import Dict, Any, Optional
from .advisor_common import get_cache, DEFAULT_NEUTRAL_VOTE


def risk_vote(db_fn, log, *,
              asset_type: str,
              portfolio_state: Optional[Dict] = None) -> Dict[str, Any]:
    """Voto 0..1 — ALTO = seguro abrir trade, BAIXO = dia ruim, pare.

    Retorna {
      'vote': 0..1,
      'day_pnl': float,
      'n_stops_today': int,
      'reason': str
    }
    """
    cache = get_cache()
    key = f'risk:{asset_type}'
    cached = cache.get(key)
    if cached is not None:
        return cached

    result = {
        'vote': DEFAULT_NEUTRAL_VOTE,
        'day_pnl': 0.0,
        'n_stops_today': 0,
        'reason': 'no_data',
    }

    conn = None
    try:
        conn = db_fn()
        if not conn:
            return result
        c = conn.cursor(dictionary=True)
        c.execute("""
            SELECT 
              COUNT(*) as n_closed,
              SUM(CASE WHEN close_reason LIKE 'STOP_LOSS%%' THEN 1 ELSE 0 END) as n_stops,
              COALESCE(SUM(pnl), 0) as day_pnl,
              SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as n_losses,
              SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as n_wins
            FROM trades 
            WHERE asset_type=%s AND status='CLOSED'
              AND closed_at > CURDATE()
        """, (asset_type,))
        r = c.fetchone()
        c.close()
    except Exception as e:
        log.debug(f'[ADVISOR:risk] err: {e}')
        return result
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

    if not r or (r['n_closed'] or 0) == 0:
        # Dia sem trades fechadas ainda — voto neutro-otimista
        result.update({'vote': 0.75, 'reason': 'day_start'})
        cache.set(key, result)
        return result

    day_pnl = float(r['day_pnl'] or 0)
    n_stops = int(r['n_stops'] or 0)
    n_wins = int(r['n_wins'] or 0)
    n_losses = int(r['n_losses'] or 0)
    n_total = int(r['n_closed'] or 0)

    # Thresholds por asset_type (magnitudes diferentes)
    if asset_type == 'stock':
        big_loss = 10000.0     # perdeu $10k no dia → alerta
        catastrophic = 25000.0  # perdeu $25k → stop total
    else:  # crypto
        big_loss = 3000.0
        catastrophic = 8000.0

    # Heurística de voto
    if day_pnl <= -catastrophic or n_stops >= 8:
        vote = 0.05
        reason = f'catastrophic_pnl{day_pnl:+.0f}_stops{n_stops}'
    elif day_pnl <= -big_loss or n_stops >= 5:
        vote = 0.25
        reason = f'bad_day_pnl{day_pnl:+.0f}_stops{n_stops}'
    elif day_pnl < 0 and n_losses > n_wins * 2:
        vote = 0.45
        reason = f'losing_streak_{n_wins}w{n_losses}l'
    elif day_pnl >= 5000:
        vote = 0.90
        reason = f'good_day_pnl{day_pnl:+.0f}'
    else:
        vote = 0.70
        reason = f'normal_pnl{day_pnl:+.0f}'

    # Penalidade extra por portfolio cheio (se disponível)
    if portfolio_state:
        try:
            n_open = int(portfolio_state.get('open_positions', 0))
            max_pos = int(portfolio_state.get('max_positions', 20))
            if n_open >= max_pos * 0.9:
                vote = max(0.1, vote - 0.15)
                reason += '_near_full'
        except Exception:
            pass

    result.update({
        'vote': round(vote, 3),
        'day_pnl': round(day_pnl, 2),
        'n_stops_today': n_stops,
        'reason': reason,
    })
    cache.set(key, result)
    return result


def risk_exit_vote(db_fn, log, *,
                   asset_type: str,
                   current_pnl: float,
                   portfolio_state: Optional[Dict] = None) -> Dict[str, Any]:
    """Para Exit Advisor: voto ALTO = feche agora (risco alto).
    Simétrico inverso do risk_vote do Entry.
    """
    # Inverte a semântica: se dia está ruim E a trade atual está positiva,
    # sinal pra proteger lucro (fechar)
    entry = risk_vote(db_fn, log, asset_type=asset_type,
                      portfolio_state=portfolio_state)
    entry_vote = entry['vote']

    # Se dia está ruim (entry_vote baixo) E trade em lucro → exit_vote alto
    if entry_vote < 0.40 and current_pnl > 0:
        exit_vote = 0.75
        reason = f'protect_profit_bad_day'
    elif entry_vote < 0.25:
        # Dia muito ruim: exit_vote alto independente de pnl atual
        exit_vote = 0.70
        reason = f'bad_day_close_all'
    else:
        exit_vote = 1.0 - entry_vote
        reason = entry['reason']

    return {
        'vote': round(exit_vote, 3),
        'day_pnl': entry['day_pnl'],
        'n_stops_today': entry['n_stops_today'],
        'reason': reason,
    }

