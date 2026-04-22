"""
Brain Advisor V4 — Voto de Similaridade

Consulta últimas N trades com mesmo (symbol, asset_type, regime, direction)
nos últimos 30 dias e retorna voto 0..1 baseado em WR e PnL médio.

Exit:
Consulta similaridade de trades que fecharam com close_reason específico
para avaliar qual exit_action seria melhor.
"""
from __future__ import annotations
from typing import Dict, Any, Optional
from .advisor_common import get_cache, DEFAULT_NEUTRAL_VOTE


LOOKBACK_DAYS = 30
MIN_EVIDENCE = 5


def similarity_vote(db_fn, log, *,
                    symbol: str, asset_type: str,
                    regime: Optional[str], direction: Optional[str]) -> Dict[str, Any]:
    """Voto 0..1 baseado em histórico de trades similares.

    Retorna {
      'vote': 0.0..1.0,
      'n_samples': int,
      'wr': 0.0..1.0,
      'avg_pnl_pct': float,
      'reason': str
    }
    """
    cache = get_cache()
    key = f'sim:{symbol}:{asset_type}:{regime or "any"}:{direction or "any"}'
    cached = cache.get(key)
    if cached is not None:
        return cached

    result = {
        'vote': DEFAULT_NEUTRAL_VOTE,
        'n_samples': 0,
        'wr': 0.0,
        'avg_pnl_pct': 0.0,
        'reason': 'no_data',
    }

    conn = None
    try:
        conn = db_fn()
        if not conn:
            return result
        c = conn.cursor(dictionary=True)

        # Filtros adaptativos: se regime/direction não bate, cai para broader
        where_parts = ["symbol=%s", "asset_type=%s", "status='CLOSED'",
                       f"closed_at > NOW() - INTERVAL {LOOKBACK_DAYS} DAY"]
        params = [symbol, asset_type]
        if direction in ('LONG', 'SHORT'):
            where_parts.append("direction=%s")
            params.append(direction)

        sql = f"""SELECT pnl_pct, pnl FROM trades
                  WHERE {' AND '.join(where_parts)}
                  ORDER BY closed_at DESC LIMIT 20"""
        c.execute(sql, tuple(params))
        rows = c.fetchall()
        c.close()
    except Exception as e:
        log.debug(f'[ADVISOR:sim] query err {symbol}: {e}')
        return result
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

    if not rows:
        result['reason'] = 'no_history'
        cache.set(key, result)
        return result

    n = len(rows)
    wins = sum(1 for r in rows if float(r['pnl'] or 0) > 0)
    wr = wins / n if n else 0.0
    avg_pnl_pct = sum(float(r['pnl_pct'] or 0) for r in rows) / n

    if n < MIN_EVIDENCE:
        # Pouca evidência: voto neutro-otimista
        vote = 0.55
        reason = f'thin_data_n{n}_wr{int(wr*100)}'
    else:
        # Mapeamento WR → vote com leve atenuação nas pontas
        # WR 30% → vote 0.30 | WR 50% → vote 0.55 | WR 70% → vote 0.80
        vote = min(1.0, max(0.0, wr * 1.15))
        # Bônus/penalidade por pnl médio
        if avg_pnl_pct > 0.5:
            vote = min(1.0, vote + 0.05)
        elif avg_pnl_pct < -0.5:
            vote = max(0.0, vote - 0.05)
        reason = f'n{n}_wr{int(wr*100)}_avg{avg_pnl_pct:+.2f}'

    result.update({
        'vote': round(vote, 3),
        'n_samples': n,
        'wr': round(wr, 3),
        'avg_pnl_pct': round(avg_pnl_pct, 3),
        'reason': reason,
    })
    cache.set(key, result)
    return result


def exit_similarity_outcome(db_fn, log, *,
                            symbol: str, asset_type: str,
                            current_pnl_pct: float,
                            holding_minutes: int) -> Dict[str, Any]:
    """Para exits: consulta trades similares que chegaram em pnl semelhante.
    Quantos continuaram subindo? Quantos reverteram?

    Retorna dict com probabilidade de reversão.
    """
    result = {
        'vote': DEFAULT_NEUTRAL_VOTE,
        'n_samples': 0,
        'reversal_prob': 0.5,
        'reason': 'no_data',
    }
    conn = None
    try:
        conn = db_fn()
        if not conn:
            return result
        c = conn.cursor(dictionary=True)
        # Trades do mesmo símbolo que atingiram pnl_pct >= current em histórico
        c.execute("""
            SELECT peak_pnl_pct, pnl_pct
            FROM trades
            WHERE symbol=%s AND asset_type=%s AND status='CLOSED'
              AND peak_pnl_pct >= %s
              AND closed_at > NOW() - INTERVAL 30 DAY
            ORDER BY closed_at DESC LIMIT 20
        """, (symbol, asset_type, float(current_pnl_pct) - 0.2))
        rows = c.fetchall()
        c.close()
    except Exception as e:
        log.debug(f'[ADVISOR:exit_sim] err: {e}')
        return result
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

    if len(rows) < MIN_EVIDENCE:
        return result

    # Quantas reverteram (final < peak - 0.3pp)
    reversals = sum(1 for r in rows
                    if float(r['pnl_pct'] or 0) < float(r['peak_pnl_pct'] or 0) - 0.3)
    rev_prob = reversals / len(rows)
    # Se prob reversão alta → vote alto = advisor quer sair
    result.update({
        'vote': round(rev_prob, 3),
        'n_samples': len(rows),
        'reversal_prob': round(rev_prob, 3),
        'reason': f'n{len(rows)}_revprob{int(rev_prob*100)}',
    })
    return result

