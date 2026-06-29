"""Meta-Brain analyzer.

This module is intentionally read-only. It can inspect tables and in-memory
snapshots passed by api_server, but it must never write to DB or call trading
execution paths.
"""

from __future__ import annotations

import json
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
from math import sqrt
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple


MARKETS = ('B3', 'NYSE', 'CRYPTO')
MARKET_WHERE = {
    'B3': "asset_type='stock' AND market='B3'",
    'NYSE': "asset_type='stock' AND market IN ('NYSE','NASDAQ')",
    'CRYPTO': "asset_type='crypto'",
}
SIGNAL_WHERE = {
    'B3': "asset_type='stock' AND market_type='B3'",
    'NYSE': "asset_type='stock' AND market_type IN ('NYSE','NASDAQ')",
    'CRYPTO': "asset_type='crypto'",
}

FEATURE_KEYS = (
    'direction',
    'score_bucket',
    'rsi_bucket',
    'ema_alignment',
    'change_pct_bucket',
    'volatility_bucket',
    'time_bucket',
    'atr_bucket',
    'volume_bucket',
    'weekday',
)

FEATURE_COMBOS = (
    ('direction', 'time_bucket'),
    ('direction', 'score_bucket'),
    ('score_bucket', 'rsi_bucket'),
    ('rsi_bucket', 'ema_alignment'),
    ('change_pct_bucket', 'atr_bucket'),
    ('volume_bucket', 'time_bucket'),
    ('ema_alignment', 'volume_bucket'),
)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    return value


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _safe_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        data = json.loads(value)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _fetch_one(cur, sql: str, params: Tuple[Any, ...] = ()) -> Dict[str, Any]:
    cur.execute(sql, params)
    return cur.fetchone() or {}


def _fetch_all(cur, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    cur.execute(sql, params)
    return list(cur.fetchall() or [])


def _pearson(rows: Iterable[Dict[str, Any]], x_key: str = 'score', y_key: str = 'pnl_pct') -> Optional[float]:
    pairs = []
    for row in rows:
        x = row.get(x_key)
        y = row.get(y_key)
        if x is None or y is None:
            continue
        pairs.append((_num(x), _num(y)))
    if len(pairs) < 20:
        return None
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    cov = sum((x - mx) * (y - my) for x, y in pairs)
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    denom = sqrt(vx * vy)
    if denom <= 0:
        return None
    return round(cov / denom, 4)


def _duration_minutes(row: Dict[str, Any]) -> Optional[float]:
    opened = row.get('opened_at')
    closed = row.get('closed_at')
    if not opened or not closed:
        return None
    if isinstance(opened, str):
        try:
            opened = datetime.fromisoformat(opened.replace('Z', '+00:00'))
        except Exception:
            return None
    if isinstance(closed, str):
        try:
            closed = datetime.fromisoformat(closed.replace('Z', '+00:00'))
        except Exception:
            return None
    try:
        minutes = (closed - opened).total_seconds() / 60.0
        return minutes if minutes >= 0 else None
    except Exception:
        return None


def _recent_trade_rows(cur, market: str, lookback_days: int, limit: int = 1200) -> List[Dict[str, Any]]:
    where = MARKET_WHERE[market]
    rows = _fetch_all(cur, f"""
        SELECT id, symbol, market, asset_type, direction, score,
               learning_confidence, pnl, pnl_pct, peak_pnl_pct, close_reason,
               opened_at, closed_at, features_json
        FROM trades
        WHERE status='CLOSED'
          AND {where}
          AND COALESCE(close_reason,'') NOT LIKE 'VOID%%'
          AND closed_at >= NOW() - INTERVAL %s DAY
        ORDER BY closed_at DESC
        LIMIT {int(limit)}
    """, (lookback_days,))
    for row in rows:
        features = _safe_json(row.get('features_json'))
        row['_features'] = features
        row['_duration_min'] = _duration_minutes(row)
    return rows


def _feature_value(row: Dict[str, Any], key: str) -> str:
    if key == 'direction':
        return str(row.get('direction') or row.get('_features', {}).get('direction') or 'UNKNOWN')
    value = row.get('_features', {}).get(key)
    if value is None and key == 'weekday':
        closed = row.get('closed_at')
        if hasattr(closed, 'weekday'):
            value = closed.weekday()
    if value is None or value == '':
        return 'UNKNOWN'
    return str(value)


def _bucket_stats(rows: Iterable[Dict[str, Any]], dimensions: Tuple[str, ...], min_samples: int) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        'n': 0,
        'wins': 0,
        'pnl': 0.0,
        'pnl_pct': 0.0,
        'fast_losses': 0,
        'stop_like': 0,
        'symbols': Counter(),
    })
    for row in rows:
        values = tuple(_feature_value(row, dim) for dim in dimensions)
        if any(v == 'UNKNOWN' for v in values):
            continue
        key = '|'.join(values)
        b = buckets[key]
        pnl = _num(row.get('pnl'))
        pnl_pct = _num(row.get('pnl_pct'))
        b['n'] += 1
        b['wins'] += 1 if pnl > 0 else 0
        b['pnl'] += pnl
        b['pnl_pct'] += pnl_pct
        if pnl < 0 and row.get('_duration_min') is not None and row['_duration_min'] <= 2:
            b['fast_losses'] += 1
        reason = str(row.get('close_reason') or '')
        if reason.startswith('STOP_LOSS') or reason in ('EARLY_STOP', 'V3_REVERSAL'):
            b['stop_like'] += 1
        if row.get('symbol'):
            b['symbols'][str(row['symbol'])] += 1

    out = []
    for value, b in buckets.items():
        n = b['n']
        if n < min_samples:
            continue
        win_rate = b['wins'] / n * 100.0
        avg_pnl = b['pnl'] / n
        avg_pnl_pct = b['pnl_pct'] / n
        out.append({
            'dimensions': list(dimensions),
            'value': value,
            'n': n,
            'win_rate': round(win_rate, 2),
            'pnl': round(b['pnl'], 2),
            'avg_pnl': round(avg_pnl, 2),
            'avg_pnl_pct': round(avg_pnl_pct, 4),
            'stop_like_rate': round(b['stop_like'] / n * 100.0, 2),
            'fast_loss_rate': round(b['fast_losses'] / n * 100.0, 2),
            'dominant_symbols': [s for s, _ in b['symbols'].most_common(3)],
        })
    return out


def _pattern_profile(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if len(rows) < 12:
        return {
            'sample': len(rows),
            'best_patterns': [],
            'worst_patterns': [],
            'warning': 'amostra_insuficiente',
        }
    min_samples = 4 if len(rows) < 80 else 8
    patterns: List[Dict[str, Any]] = []
    for key in FEATURE_KEYS:
        patterns.extend(_bucket_stats(rows, (key,), min_samples))
    for combo in FEATURE_COMBOS:
        patterns.extend(_bucket_stats(rows, combo, min_samples))

    for item in patterns:
        wr_edge = (item['win_rate'] - 50.0) * 0.12
        pnl_edge = max(-15.0, min(15.0, item['avg_pnl_pct'] * 25.0))
        risk_drag = item['stop_like_rate'] * 0.08 + item['fast_loss_rate'] * 0.12
        item['pattern_score'] = round(wr_edge + pnl_edge - risk_drag, 2)

    best = sorted(
        [p for p in patterns if p['pnl'] > 0],
        key=lambda p: (p['pattern_score'], p['pnl'], p['n']),
        reverse=True,
    )[:8]
    worst = sorted(
        [p for p in patterns if p['pnl'] < 0],
        key=lambda p: (p['pattern_score'], p['pnl'], -p['n']),
    )[:8]
    return {
        'sample': len(rows),
        'min_samples': min_samples,
        'best_patterns': best,
        'worst_patterns': worst,
    }


def _window_trend(cur, market: str) -> Dict[str, Any]:
    where = MARKET_WHERE[market]
    row = _fetch_one(cur, f"""
        SELECT
          SUM(CASE WHEN closed_at >= NOW() - INTERVAL 7 DAY THEN 1 ELSE 0 END) AS n7,
          SUM(CASE WHEN closed_at >= NOW() - INTERVAL 7 DAY AND pnl > 0 THEN 1 ELSE 0 END) AS w7,
          COALESCE(SUM(CASE WHEN closed_at >= NOW() - INTERVAL 7 DAY THEN pnl ELSE 0 END),0) AS pnl7,
          SUM(CASE WHEN closed_at >= NOW() - INTERVAL 30 DAY THEN 1 ELSE 0 END) AS n30,
          SUM(CASE WHEN closed_at >= NOW() - INTERVAL 30 DAY AND pnl > 0 THEN 1 ELSE 0 END) AS w30,
          COALESCE(SUM(CASE WHEN closed_at >= NOW() - INTERVAL 30 DAY THEN pnl ELSE 0 END),0) AS pnl30
        FROM trades
        WHERE status='CLOSED'
          AND {where}
          AND COALESCE(close_reason,'') NOT LIKE 'VOID%%'
          AND closed_at >= NOW() - INTERVAL 30 DAY
    """)
    n7 = _int(row.get('n7'))
    w7 = _int(row.get('w7'))
    n30 = _int(row.get('n30'))
    w30 = _int(row.get('w30'))
    wr7 = (w7 / n7 * 100.0) if n7 else 0.0
    wr30 = (w30 / n30 * 100.0) if n30 else 0.0
    pnl7 = _num(row.get('pnl7'))
    pnl30 = _num(row.get('pnl30'))
    weekly_run_rate = pnl7 * (30.0 / 7.0) if n7 else 0.0
    delta_wr = wr7 - wr30
    delta_pnl = weekly_run_rate - pnl30
    if n7 < 5:
        state = 'LOW_SAMPLE'
    elif delta_wr >= 6 and delta_pnl > 0:
        state = 'IMPROVING'
    elif delta_wr <= -6 and delta_pnl < 0:
        state = 'DETERIORATING'
    else:
        state = 'STABLE'
    return {
        'state': state,
        'n_7d': n7,
        'win_rate_7d': round(wr7, 2),
        'pnl_7d': round(pnl7, 2),
        'n_30d': n30,
        'win_rate_30d': round(wr30, 2),
        'pnl_30d': round(pnl30, 2),
        'delta_wr_7d_vs_30d': round(delta_wr, 2),
        'delta_pnl_run_rate': round(delta_pnl, 2),
    }


def _signal_quality(cur, market: str) -> Dict[str, Any]:
    where = SIGNAL_WHERE[market]
    rows = _fetch_all(cur, f"""
        SELECT
          signal_type,
          confidence_band,
          COUNT(*) AS n,
          COALESCE(AVG(raw_score),0) AS avg_score,
          COALESCE(AVG(learning_confidence),0) AS avg_conf,
          SUM(CASE WHEN outcome_status='WIN' OR outcome_pnl > 0 THEN 1 ELSE 0 END) AS wins,
          SUM(CASE WHEN outcome_status='LOSS' OR outcome_pnl < 0 THEN 1 ELSE 0 END) AS losses,
          COALESCE(SUM(outcome_pnl),0) AS pnl
        FROM signal_events
        WHERE {where}
          AND signal_created_at >= NOW() - INTERVAL 30 DAY
        GROUP BY signal_type, confidence_band
        ORDER BY n DESC
        LIMIT 12
    """)
    out = []
    for r in rows:
        wins = _int(r.get('wins'))
        losses = _int(r.get('losses'))
        resolved = wins + losses
        out.append({
            'signal_type': r.get('signal_type') or 'UNKNOWN',
            'confidence_band': r.get('confidence_band') or 'UNKNOWN',
            'n': _int(r.get('n')),
            'resolved': resolved,
            'resolved_win_rate': round((wins / resolved * 100.0) if resolved else 0.0, 2),
            'avg_score': round(_num(r.get('avg_score')), 2),
            'avg_confidence': round(_num(r.get('avg_conf')), 2),
            'pnl': round(_num(r.get('pnl')), 2),
        })
    bad = [
        r for r in out
        if r['resolved'] >= 5 and r['resolved_win_rate'] < 42 and r['pnl'] < 0
    ]
    return {
        'segments': out,
        'weak_segments': bad[:5],
    }


def _advisor_shadow(cur, market: str) -> Dict[str, Any]:
    asset_type = 'crypto' if market == 'CRYPTO' else 'stock'
    if market == 'NYSE':
        market_filter = "AND (market_type IN ('NYSE','NASDAQ') OR market_type IS NULL)"
        params = (asset_type,)
    else:
        signal_market = 'crypto' if market == 'CRYPTO' else market
        market_filter = "AND (market_type=%s OR market_type IS NULL)"
        params = (asset_type, signal_market)
    try:
        entry = _fetch_all(cur, f"""
            SELECT would_action,
                   COUNT(*) AS n,
                   SUM(CASE WHEN actual_pnl > 0 THEN 1 ELSE 0 END) AS wins,
                   COALESCE(SUM(actual_pnl),0) AS pnl,
                   COALESCE(AVG(aggregate_score),0) AS avg_advisor_score
            FROM brain_shadow_entry_advisor
            WHERE asset_type=%s
              {market_filter}
              AND created_at >= NOW() - INTERVAL 30 DAY
            GROUP BY would_action
            ORDER BY n DESC
        """, params)
        return {
            'available': True,
            'entry_actions': [_jsonable(r) for r in entry],
        }
    except Exception as exc:
        return {
            'available': False,
            'note': f'advisor_shadow_unavailable:{type(exc).__name__}',
        }


def _market_summary(cur, market: str, lookback_days: int) -> Dict[str, Any]:
    where = MARKET_WHERE[market]
    params = (lookback_days,)
    row = _fetch_one(cur, f"""
        SELECT
          COUNT(*) AS n,
          SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins,
          SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) AS losses,
          COALESCE(SUM(pnl),0) AS total_pnl,
          COALESCE(AVG(pnl),0) AS avg_pnl,
          COALESCE(AVG(pnl_pct),0) AS avg_pnl_pct,
          COALESCE(MAX(pnl),0) AS best_pnl,
          COALESCE(MIN(pnl),0) AS worst_pnl,
          SUM(CASE WHEN COALESCE(close_reason,'') IN ('EARLY_STOP','STOP_LOSS','V3_REVERSAL')
                    OR COALESCE(close_reason,'') LIKE 'STOP_LOSS%%'
                   THEN 1 ELSE 0 END) AS stop_like,
          SUM(CASE WHEN pnl < 0
                    AND opened_at IS NOT NULL AND closed_at IS NOT NULL
                    AND TIMESTAMPDIFF(SECOND, opened_at, closed_at) BETWEEN 0 AND 120
                   THEN 1 ELSE 0 END) AS fast_losses
        FROM trades
        WHERE status='CLOSED'
          AND {where}
          AND COALESCE(close_reason,'') NOT LIKE 'VOID%%'
          AND closed_at >= NOW() - INTERVAL %s DAY
    """, params)

    n = _int(row.get('n'))
    wins = _int(row.get('wins'))
    losses = _int(row.get('losses'))
    stop_like = _int(row.get('stop_like'))
    fast_losses = _int(row.get('fast_losses'))
    win_rate = (wins / n * 100.0) if n else 0.0
    stop_rate = (stop_like / n * 100.0) if n else 0.0
    fast_loss_rate = (fast_losses / n * 100.0) if n else 0.0

    open_row = _fetch_one(cur, f"""
        SELECT COUNT(*) AS n_open, COALESCE(SUM(position_value),0) AS exposure
        FROM trades
        WHERE status='OPEN' AND {where}
    """)

    recent_rows = _fetch_all(cur, f"""
        SELECT score, pnl_pct
        FROM trades
        WHERE status='CLOSED'
          AND {where}
          AND score IS NOT NULL AND pnl_pct IS NOT NULL
          AND closed_at >= NOW() - INTERVAL %s DAY
        ORDER BY closed_at DESC LIMIT 500
    """, params)
    score_corr = _pearson(recent_rows)

    reasons = _fetch_all(cur, f"""
        SELECT COALESCE(close_reason,'UNKNOWN') AS close_reason,
               COUNT(*) AS n,
               COALESCE(SUM(pnl),0) AS pnl
        FROM trades
        WHERE status='CLOSED'
          AND {where}
          AND closed_at >= NOW() - INTERVAL %s DAY
        GROUP BY COALESCE(close_reason,'UNKNOWN')
        ORDER BY n DESC LIMIT 5
    """, params)

    worst_symbols = _fetch_all(cur, f"""
        SELECT symbol,
               COUNT(*) AS n,
               COALESCE(SUM(pnl),0) AS pnl,
               SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins
        FROM trades
        WHERE status='CLOSED'
          AND {where}
          AND closed_at >= NOW() - INTERVAL %s DAY
        GROUP BY symbol
        HAVING n >= 3
        ORDER BY pnl ASC LIMIT 5
    """, params)

    best_symbols = _fetch_all(cur, f"""
        SELECT symbol,
               COUNT(*) AS n,
               COALESCE(SUM(pnl),0) AS pnl,
               SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins
        FROM trades
        WHERE status='CLOSED'
          AND {where}
          AND closed_at >= NOW() - INTERVAL %s DAY
        GROUP BY symbol
        HAVING n >= 3
        ORDER BY pnl DESC LIMIT 5
    """, params)

    signal_where = SIGNAL_WHERE[market]
    signals_24h = _fetch_one(cur, f"""
        SELECT COUNT(*) AS n
        FROM signal_events
        WHERE {signal_where}
          AND signal_created_at >= NOW() - INTERVAL 24 HOUR
    """)

    calib = _fetch_one(cur, """
        SELECT n_trades_used, baseline_wr, calibration_quality, avg_adj_pts, run_ts
        FROM brain_specialist_calibration_history
        WHERE market=%s
        ORDER BY run_ts DESC LIMIT 1
    """, (market,))

    recent_trade_rows = _recent_trade_rows(cur, market, lookback_days)
    trend = _window_trend(cur, market)
    patterns = _pattern_profile(recent_trade_rows)
    signal_quality = _signal_quality(cur, market)
    advisor = _advisor_shadow(cur, market)

    health = _health_score(
        n=n,
        win_rate=win_rate,
        total_pnl=_num(row.get('total_pnl')),
        stop_rate=stop_rate,
        fast_loss_rate=fast_loss_rate,
        score_corr=score_corr,
        calibration=calib,
    )

    return {
        'market': market,
        'lookback_days': lookback_days,
        'trades': {
            'closed': n,
            'wins': wins,
            'losses': losses,
            'win_rate': round(win_rate, 2),
            'total_pnl': round(_num(row.get('total_pnl')), 2),
            'avg_pnl': round(_num(row.get('avg_pnl')), 2),
            'avg_pnl_pct': round(_num(row.get('avg_pnl_pct')), 4),
            'best_pnl': round(_num(row.get('best_pnl')), 2),
            'worst_pnl': round(_num(row.get('worst_pnl')), 2),
            'stop_like_rate': round(stop_rate, 2),
            'fast_loss_rate': round(fast_loss_rate, 2),
            'open_positions': _int(open_row.get('n_open')),
            'open_exposure': round(_num(open_row.get('exposure')), 2),
        },
        'signals_24h': _int(signals_24h.get('n')),
        'score_pnl_corr': score_corr,
        'health': health,
        'capital_gate': _capital_gate_for_market(health, n, win_rate, _num(row.get('total_pnl')), stop_rate, score_corr),
        'trend': trend,
        'pattern_profile': patterns,
        'signal_quality': signal_quality,
        'advisor_shadow': advisor,
        'insights': _market_insights(market, health, trend, patterns, signal_quality),
        'top_close_reasons': [_jsonable(r) for r in reasons],
        'best_symbols': [_symbol_row(r) for r in best_symbols],
        'worst_symbols': [_symbol_row(r) for r in worst_symbols],
        'last_calibration': _jsonable(calib),
    }


def _market_insights(
    market: str,
    health: Dict[str, Any],
    trend: Dict[str, Any],
    patterns: Dict[str, Any],
    signal_quality: Dict[str, Any],
) -> List[Dict[str, Any]]:
    insights: List[Dict[str, Any]] = []
    state = health.get('state')
    if state in ('DEFENSIVE', 'PAUSE'):
        insights.append({
            'severity': 'HIGH' if state == 'PAUSE' else 'MEDIUM',
            'type': 'market_health',
            'title': f'{market} em modo {state}',
            'detail': ', '.join(health.get('reasons') or []) or 'score_operacional_baixo',
        })
    if trend.get('state') == 'DETERIORATING':
        insights.append({
            'severity': 'HIGH',
            'type': 'trend',
            'title': f'{market} perdeu qualidade nos ultimos 7 dias',
            'detail': f"WR 7d {trend.get('win_rate_7d')}% vs 30d {trend.get('win_rate_30d')}%",
        })
    elif trend.get('state') == 'IMPROVING':
        insights.append({
            'severity': 'LOW',
            'type': 'trend',
            'title': f'{market} esta melhorando',
            'detail': f"run-rate PnL 7d acima da janela de 30d em {trend.get('delta_pnl_run_rate')}",
        })
    worst = (patterns.get('worst_patterns') or [])[:2]
    for p in worst:
        insights.append({
            'severity': 'MEDIUM',
            'type': 'pattern_loss',
            'title': f"Padrao negativo em {market}",
            'detail': f"{' x '.join(p['dimensions'])}={p['value']} | n={p['n']} | WR={p['win_rate']}% | PnL={p['pnl']}",
        })
    for seg in (signal_quality.get('weak_segments') or [])[:2]:
        insights.append({
            'severity': 'MEDIUM',
            'type': 'signal_quality',
            'title': f"Sinal fraco em {market}",
            'detail': f"{seg['signal_type']}/{seg['confidence_band']} | resolvido={seg['resolved']} | WR={seg['resolved_win_rate']}%",
        })
    if not insights:
        insights.append({
            'severity': 'LOW',
            'type': 'ok',
            'title': f'{market} sem anomalia critica',
            'detail': 'nenhum bloqueio operacional sugerido pelo Meta-Brain',
        })
    return insights[:6]


def _symbol_row(row: Dict[str, Any]) -> Dict[str, Any]:
    n = _int(row.get('n'))
    wins = _int(row.get('wins'))
    return {
        'symbol': row.get('symbol'),
        'n': n,
        'pnl': round(_num(row.get('pnl')), 2),
        'win_rate': round((wins / n * 100.0) if n else 0.0, 2),
    }


def _health_score(
    *,
    n: int,
    win_rate: float,
    total_pnl: float,
    stop_rate: float,
    fast_loss_rate: float,
    score_corr: Optional[float],
    calibration: Dict[str, Any],
) -> Dict[str, Any]:
    score = 100.0
    reasons = []

    if n < 10:
        score -= 35
        reasons.append('amostra_muito_pequena')
    elif n < 30:
        score -= 15
        reasons.append('amostra_pequena')

    if win_rate < 35:
        score -= 30
        reasons.append('win_rate_critico')
    elif win_rate < 42:
        score -= 18
        reasons.append('win_rate_fraco')
    elif win_rate >= 52:
        score += 5
        reasons.append('win_rate_saudavel')

    if total_pnl < 0:
        score -= 15
        reasons.append('pnl_negativo')
    elif total_pnl > 0 and n >= 20:
        score += 5
        reasons.append('pnl_positivo')

    if stop_rate > 60:
        score -= 25
        reasons.append('stop_rate_extremo')
    elif stop_rate > 45:
        score -= 15
        reasons.append('stop_rate_alto')

    if fast_loss_rate > 20:
        score -= 15
        reasons.append('perdas_rapidas_repetidas')

    if score_corr is not None:
        if score_corr < -0.05:
            score -= 15
            reasons.append('score_contra_pnl')
        elif score_corr > 0.10:
            score += 8
            reasons.append('score_alinhado_com_pnl')
    else:
        score -= 5
        reasons.append('correlacao_score_insuficiente')

    if not calibration:
        score -= 10
        reasons.append('sem_calibracao_specialist')

    score = max(0.0, min(100.0, score))
    if score >= 75:
        state = 'HEALTHY'
    elif score >= 55:
        state = 'WATCH'
    elif score >= 35:
        state = 'DEFENSIVE'
    else:
        state = 'PAUSE'

    return {
        'score': round(score, 1),
        'state': state,
        'reasons': reasons,
    }


def _capital_gate_for_market(
    health: Dict[str, Any],
    n: int,
    win_rate: float,
    total_pnl: float,
    stop_rate: float,
    score_corr: Optional[float],
) -> Dict[str, Any]:
    state = health.get('state')
    if n < 30:
        return {
            'decision': 'HOLD_PAPER',
            'capital_multiplier': 0.0,
            'reason': 'amostra_insuficiente_para_capital_real',
        }
    if state == 'HEALTHY' and win_rate >= 50 and total_pnl > 0 and stop_rate < 40 and (score_corr is None or score_corr >= 0):
        return {
            'decision': 'ALLOW_SMALL_INCREASE',
            'capital_multiplier': 1.10,
            'reason': 'mercado_saudavel_com_pnl_positivo',
        }
    if state in ('HEALTHY', 'WATCH'):
        return {
            'decision': 'KEEP_OR_SMALL_SIZE',
            'capital_multiplier': 1.0 if state == 'HEALTHY' else 0.70,
            'reason': 'operar_com_controle',
        }
    if state == 'DEFENSIVE':
        return {
            'decision': 'REDUCE_SIZE',
            'capital_multiplier': 0.35,
            'reason': 'mercado_em_defesa',
        }
    return {
        'decision': 'PAUSE_NEW_ENTRIES',
        'capital_multiplier': 0.0,
        'reason': 'sem_edge_operacional_suficiente',
    }


def _arbi_monitor(cur) -> Dict[str, Any]:
    try:
        rows = _fetch_all(cur, """
            SELECT status, COUNT(*) AS n, COALESCE(SUM(pnl),0) AS pnl
            FROM arbi_trades
            GROUP BY status
        """)
        return {
            'monitor_only': True,
            'segregated': True,
            'note': 'Meta-Brain only observes Arbi; it does not score, block or mutate Arbi.',
            'by_status': [_jsonable(r) for r in rows],
        }
    except Exception as exc:
        return {
            'monitor_only': True,
            'segregated': True,
            'available': False,
            'note': f'Arbi monitor skipped: {type(exc).__name__}',
        }


def _data_sources(provider_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    ctx = provider_context or {}
    providers = ctx.get('providers') or {}
    dq = ctx.get('data_quality') or {}
    cache = ctx.get('market_cache') or {}
    score = 100.0
    issues = []

    polygon = providers.get('polygon') or {}
    brapi = providers.get('brapi') or {}
    cedro = providers.get('cedro') or {}

    if not polygon.get('configured'):
        score -= 18
        issues.append('polygon_key_missing')
    elif not polygon.get('connected') and polygon.get('ws_loaded'):
        score -= 8
        issues.append('polygon_ws_not_connected')

    if not brapi.get('configured'):
        score -= 18
        issues.append('brapi_token_missing')

    if not cedro.get('configured'):
        score -= 18
        issues.append('cedro_credentials_missing')
    elif not cedro.get('connected'):
        score -= 8
        issues.append('cedro_socket_not_connected')

    stale = _int(dq.get('stale_count'))
    total = _int(dq.get('observations'))
    if total and stale / max(total, 1) > 0.25:
        score -= 12
        issues.append('data_quality_stale_high')

    avg_quality = dq.get('avg_quality')
    if avg_quality is not None and _num(avg_quality) < 75:
        score -= 12
        issues.append('avg_data_quality_low')

    b3_brapi = _int((cache.get('source_counts') or {}).get('brapi', 0))
    b3_cedro = _int((cache.get('source_counts') or {}).get('cedro-socket', 0))
    if brapi.get('configured') and cedro.get('configured') and b3_brapi + b3_cedro == 0:
        score -= 6
        issues.append('b3_sources_not_seen_in_cache')

    score = max(0.0, min(100.0, score))
    if score >= 85:
        state = 'RICH'
    elif score >= 65:
        state = 'GOOD'
    elif score >= 40:
        state = 'DEGRADED'
    else:
        state = 'POOR'

    return {
        'state': state,
        'score': round(score, 1),
        'issues': issues,
        'providers': providers,
        'data_quality': dq,
        'market_cache': cache,
        'brain_access': {
            'polygon': 'NYSE/NASDAQ real-time, snapshots, historical candles, news',
            'brapi': 'B3 quotes, candles, fundamentals, dividends, macro/fx fallbacks',
            'cedro': 'B3 real-time socket, depth/derivatives support, historical candles',
        },
        'policy': 'read_only_context_for_meta_brain_no_trade_execution',
    }


def _safe_count(cur, table: str, where: str = '', params: Tuple[Any, ...] = ()) -> Optional[int]:
    try:
        sql = f"SELECT COUNT(*) AS n FROM {table}"
        if where:
            sql += f" WHERE {where}"
        return _int(_fetch_one(cur, sql, params).get('n'))
    except Exception:
        return None


def _system_memory(cur) -> Dict[str, Any]:
    signal_count = _safe_count(cur, 'signal_events')
    trades_count = _safe_count(cur, 'trades')
    closed_count = _safe_count(cur, 'trades', "status='CLOSED'")
    arbi_count = _safe_count(cur, 'arbi_trades')
    feature_count = _safe_count(cur, 'brain_feature_weights')
    combo_count = _safe_count(cur, 'brain_combo_weights')
    symbol_count = _safe_count(cur, 'brain_symbol_stats')
    advisor_entry = _safe_count(cur, 'brain_shadow_entry_advisor')
    advisor_exit = _safe_count(cur, 'brain_shadow_exit_advisor')

    total_memory = sum(x or 0 for x in (
        signal_count, trades_count, arbi_count, feature_count, combo_count,
        symbol_count, advisor_entry, advisor_exit,
    ))
    maturity = 20.0
    if (closed_count or 0) >= 1000:
        maturity += 20
    elif (closed_count or 0) >= 300:
        maturity += 12
    if (signal_count or 0) >= 5000:
        maturity += 18
    elif (signal_count or 0) >= 1000:
        maturity += 10
    if (combo_count or 0) >= 50:
        maturity += 12
    if (symbol_count or 0) >= 20:
        maturity += 10
    if (advisor_entry or 0) + (advisor_exit or 0) >= 200:
        maturity += 12
    if arbi_count is not None:
        maturity += 8

    if maturity >= 80:
        phase = 'institutional_candidate'
    elif maturity >= 60:
        phase = 'advanced_research'
    elif maturity >= 40:
        phase = 'learning_system'
    else:
        phase = 'early_learning'

    return {
        'phase': phase,
        'maturity_score': round(min(100.0, maturity), 1),
        'total_memory_rows': total_memory,
        'signal_events': signal_count,
        'trades_total': trades_count,
        'trades_closed': closed_count,
        'arbi_rows_monitor_only': arbi_count,
        'feature_weights': feature_count,
        'combo_weights': combo_count,
        'symbol_stats': symbol_count,
        'advisor_entry_shadow': advisor_entry,
        'advisor_exit_shadow': advisor_exit,
    }


def _global_alerts(markets: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    alerts: List[Dict[str, Any]] = []
    for market, data in markets.items():
        health = data.get('health', {})
        trades = data.get('trades', {})
        trend = data.get('trend', {})
        gate = data.get('capital_gate', {})
        if gate.get('decision') == 'PAUSE_NEW_ENTRIES':
            alerts.append({
                'severity': 'CRITICAL',
                'market': market,
                'title': f'{market} sem permissao para novas entradas',
                'detail': gate.get('reason'),
            })
        elif health.get('state') == 'DEFENSIVE':
            alerts.append({
                'severity': 'HIGH',
                'market': market,
                'title': f'{market} requer reducao defensiva',
                'detail': ', '.join(health.get('reasons') or []),
            })
        if trend.get('state') == 'DETERIORATING':
            alerts.append({
                'severity': 'HIGH',
                'market': market,
                'title': f'{market} deteriorando na janela curta',
                'detail': f"WR 7d {trend.get('win_rate_7d')}% contra 30d {trend.get('win_rate_30d')}%",
            })
        if _num(trades.get('fast_loss_rate')) > 15:
            alerts.append({
                'severity': 'MEDIUM',
                'market': market,
                'title': f'{market} com perdas muito rapidas',
                'detail': f"{trades.get('fast_loss_rate')}% das perdas fecharam em ate 2 minutos",
            })
    order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
    alerts.sort(key=lambda a: order.get(a.get('severity'), 9))
    return alerts[:10]


def _data_source_alerts(data_sources: Dict[str, Any]) -> List[Dict[str, Any]]:
    alerts = []
    if not data_sources:
        return alerts
    state = data_sources.get('state')
    if state in ('DEGRADED', 'POOR'):
        alerts.append({
            'severity': 'HIGH' if state == 'POOR' else 'MEDIUM',
            'market': 'DATA',
            'title': f"Fontes de dados em estado {state}",
            'detail': ', '.join(data_sources.get('issues') or []),
        })
    for issue in data_sources.get('issues') or []:
        if issue.endswith('_missing'):
            alerts.append({
                'severity': 'HIGH',
                'market': 'DATA',
                'title': issue,
                'detail': 'Configurar credencial para enriquecer o Meta-Brain.',
            })
    return alerts[:5]


def _council(markets: Dict[str, Dict[str, Any]], memory: Dict[str, Any]) -> List[Dict[str, Any]]:
    council = []
    edge = _edge_from_markets(markets)
    best = edge[0] if edge else {}
    worst = edge[-1] if edge else {}
    council.append({
        'role': 'Risk Officer',
        'vote': 'protect_capital' if any(m['health']['state'] in ('DEFENSIVE', 'PAUSE') for m in markets.values()) else 'normal_risk',
        'message': 'Reduzir mercados em defesa antes de pensar em escala.' if any(m['health']['state'] in ('DEFENSIVE', 'PAUSE') for m in markets.values()) else 'Risco operacional dentro do aceitavel para paper/controlado.',
    })
    council.append({
        'role': 'Quant Research',
        'vote': 'mine_patterns',
        'message': f"Melhor edge atual: {best.get('market', '-')}. Pior edge: {worst.get('market', '-')}. Priorizar padroes bons e cortar clusters negativos.",
    })
    council.append({
        'role': 'Data Quality',
        'vote': 'expand_memory' if _num(memory.get('maturity_score')) < 75 else 'memory_strong',
        'message': f"Fase {memory.get('phase')} com score de maturidade {memory.get('maturity_score')}.",
    })
    council.append({
        'role': 'Arbi Guardian',
        'vote': 'observe_only',
        'message': 'Arbi permanece segregada: monitorar resultado, sem bloquear ou alterar motor.',
    })
    return council


def _meta_recommendations(status: Dict[str, Any]) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    for item in status.get('market_edge', []):
        market = item.get('market')
        data = status['markets'].get(market, {})
        gate = data.get('capital_gate', {})
        trend = data.get('trend', {})
        if gate.get('decision') == 'ALLOW_SMALL_INCREASE':
            recs.append({
                'priority': 'P2',
                'market': market,
                'action': 'validar_aumento_pequeno',
                'detail': f"{market} tem edge positivo; aumento somente controlado e auditado.",
            })
        elif gate.get('decision') in ('REDUCE_SIZE', 'PAUSE_NEW_ENTRIES'):
            recs.append({
                'priority': 'P0' if gate.get('decision') == 'PAUSE_NEW_ENTRIES' else 'P1',
                'market': market,
                'action': gate.get('decision').lower(),
                'detail': gate.get('reason'),
            })
        if trend.get('state') == 'DETERIORATING':
            recs.append({
                'priority': 'P1',
                'market': market,
                'action': 'recalibrar_janela_curta',
                'detail': f"Janela de 7 dias pior que 30 dias; revisar thresholds recentes.",
            })
        worst = (data.get('pattern_profile') or {}).get('worst_patterns') or []
        if worst:
            p = worst[0]
            recs.append({
                'priority': 'P1',
                'market': market,
                'action': 'penalizar_padrao_negativo',
                'detail': f"{' x '.join(p['dimensions'])}={p['value']} | WR {p['win_rate']}% | PnL {p['pnl']}",
            })
    recs.append({
        'priority': 'P0',
        'market': 'ARBI',
        'action': 'manter_monitor_only',
        'detail': 'Nao conectar Meta-Brain a execucao da Arbi nesta fase.',
    })
    data_sources = status.get('data_sources') or {}
    for issue in data_sources.get('issues') or []:
        if issue in ('polygon_key_missing', 'brapi_token_missing', 'cedro_credentials_missing'):
            recs.append({
                'priority': 'P1',
                'market': 'DATA',
                'action': 'configurar_fonte',
                'detail': issue,
            })
        elif issue in ('polygon_ws_not_connected', 'cedro_socket_not_connected', 'data_quality_stale_high'):
            recs.append({
                'priority': 'P1',
                'market': 'DATA',
                'action': 'verificar_fonte_tempo_real',
                'detail': issue,
            })
    prio = {'P0': 0, 'P1': 1, 'P2': 2, 'P3': 3}
    recs.sort(key=lambda r: prio.get(r.get('priority'), 9))
    return recs[:12]


def build_meta_status(
    db_fn: Callable[[], Any],
    lookback_days: int = 30,
    provider_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build full operational awareness snapshot."""
    lookback_days = max(1, min(int(lookback_days or 30), 120))
    conn = db_fn()
    if not conn:
        return {'ok': False, 'error': 'no_db'}
    try:
        cur = conn.cursor(dictionary=True)
        markets = {m: _market_summary(cur, m, lookback_days) for m in MARKETS}
        arbi = _arbi_monitor(cur)
        memory = _system_memory(cur)
        cur.close()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    gates = {m: markets[m]['capital_gate'] for m in MARKETS}
    ranked = sorted(markets.values(), key=lambda x: x['health']['score'], reverse=True)
    pause_count = sum(1 for m in markets.values() if m['health']['state'] == 'PAUSE')
    healthy_count = sum(1 for m in markets.values() if m['health']['state'] == 'HEALTHY')
    data_sources = _data_sources(provider_context)

    if pause_count >= 2:
        global_decision = 'NO_CAPITAL_INCREASE'
    elif healthy_count >= 2 and pause_count == 0:
        global_decision = 'ALLOW_CONTROLLED_SCALE'
    elif ranked[0]['health']['state'] in ('HEALTHY', 'WATCH'):
        global_decision = 'SELECTIVE_ONLY'
    else:
        global_decision = 'DEFENSIVE_MODE'

    status = {
        'ok': True,
        'meta_brain': {
            'version': 'meta-brain-0.2',
            'mode': 'monitor_only',
            'writes_to_db': False,
            'can_execute_trades': False,
            'arbi_policy': 'observe_only_never_mutate',
        },
        'timestamp': datetime.utcnow().isoformat(),
        'lookback_days': lookback_days,
        'global_state': {
            'decision': global_decision,
            'best_market': ranked[0]['market'] if ranked else None,
            'worst_market': ranked[-1]['market'] if ranked else None,
            'healthy_markets': healthy_count,
            'paused_markets': pause_count,
        },
        'memory': memory,
        'data_sources': data_sources,
        'markets': markets,
        'capital_gates': gates,
        'market_edge': _edge_from_markets(markets),
        'arbi_monitor_only': arbi,
    }
    status['alerts'] = _data_source_alerts(data_sources) + _global_alerts(markets)
    status['council'] = _council(markets, memory)
    status['recommendations'] = _meta_recommendations(status)
    return status


def _edge_from_markets(markets: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    edge = []
    for market, data in markets.items():
        trades = data.get('trades', {})
        health = data.get('health', {})
        pnl = _num(trades.get('total_pnl'))
        win_rate = _num(trades.get('win_rate'))
        stop_rate = _num(trades.get('stop_like_rate'))
        corr = data.get('score_pnl_corr')
        edge_score = _num(health.get('score')) + max(-10, min(10, pnl / 10000.0)) + (win_rate - 45) * 0.2 - stop_rate * 0.1
        if corr is not None:
            edge_score += corr * 20
        edge.append({
            'market': market,
            'edge_score': round(edge_score, 2),
            'health_state': health.get('state'),
            'health_score': health.get('score'),
            'win_rate': round(win_rate, 2),
            'total_pnl': round(pnl, 2),
            'stop_like_rate': round(stop_rate, 2),
            'score_pnl_corr': corr,
            'capital_gate': data.get('capital_gate', {}).get('decision'),
        })
    return sorted(edge, key=lambda x: x['edge_score'], reverse=True)


def build_market_edge(
    db_fn: Callable[[], Any],
    lookback_days: int = 30,
    provider_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    status = build_meta_status(db_fn, lookback_days=lookback_days, provider_context=provider_context)
    if not status.get('ok'):
        return status
    return {
        'ok': True,
        'timestamp': status['timestamp'],
        'lookback_days': status['lookback_days'],
        'market_edge': status['market_edge'],
        'best_market': status['global_state']['best_market'],
        'global_decision': status['global_state']['decision'],
    }


def build_error_report(
    db_fn: Callable[[], Any],
    lookback_days: int = 30,
    provider_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    status = build_meta_status(db_fn, lookback_days=lookback_days, provider_context=provider_context)
    if not status.get('ok'):
        return status
    errors = []
    for market, data in status['markets'].items():
        for symbol in data.get('worst_symbols', []):
            errors.append({
                'market': market,
                'symbol': symbol['symbol'],
                'pnl': symbol['pnl'],
                'n': symbol['n'],
                'win_rate': symbol['win_rate'],
            })
    errors.sort(key=lambda x: x['pnl'])
    return {
        'ok': True,
        'timestamp': status['timestamp'],
        'lookback_days': status['lookback_days'],
        'top_errors': errors[:12],
        'market_states': {
            m: status['markets'][m]['health'] for m in MARKETS
        },
    }


def build_capital_gate(
    db_fn: Callable[[], Any],
    lookback_days: int = 30,
    provider_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    status = build_meta_status(db_fn, lookback_days=lookback_days, provider_context=provider_context)
    if not status.get('ok'):
        return status
    return {
        'ok': True,
        'timestamp': status['timestamp'],
        'global_state': status['global_state'],
        'capital_gates': status['capital_gates'],
        'rule': 'Meta-Brain never increases capital directly; it only recommends gates.',
    }


def build_patterns(
    db_fn: Callable[[], Any],
    lookback_days: int = 30,
    provider_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    status = build_meta_status(db_fn, lookback_days=lookback_days, provider_context=provider_context)
    if not status.get('ok'):
        return status
    markets = status.get('markets', {})
    return {
        'ok': True,
        'timestamp': status['timestamp'],
        'lookback_days': status['lookback_days'],
        'patterns': {
            m: markets[m].get('pattern_profile', {}) for m in MARKETS
        },
        'weak_signal_segments': {
            m: markets[m].get('signal_quality', {}).get('weak_segments', []) for m in MARKETS
        },
    }


def build_recommendations(
    db_fn: Callable[[], Any],
    lookback_days: int = 30,
    provider_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    status = build_meta_status(db_fn, lookback_days=lookback_days, provider_context=provider_context)
    if not status.get('ok'):
        return status
    return {
        'ok': True,
        'timestamp': status['timestamp'],
        'lookback_days': status['lookback_days'],
        'global_state': status['global_state'],
        'alerts': status.get('alerts', []),
        'council': status.get('council', []),
        'recommendations': status.get('recommendations', []),
    }


def build_data_sources(provider_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        'ok': True,
        'timestamp': datetime.utcnow().isoformat(),
        'data_sources': _data_sources(provider_context),
    }


def build_intelligence(
    db_fn: Callable[[], Any],
    lookback_days: int = 30,
    provider_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    status = build_meta_status(db_fn, lookback_days=lookback_days, provider_context=provider_context)
    if not status.get('ok'):
        return status
    return {
        'ok': True,
        'timestamp': status['timestamp'],
        'lookback_days': status['lookback_days'],
        'meta_brain': status['meta_brain'],
        'global_state': status['global_state'],
        'memory': status.get('memory', {}),
        'data_sources': status.get('data_sources', {}),
        'market_edge': status.get('market_edge', []),
        'capital_gates': status.get('capital_gates', {}),
        'alerts': status.get('alerts', []),
        'council': status.get('council', []),
        'recommendations': status.get('recommendations', []),
        'briefing': _briefing_text(status),
        'arbi_monitor_only': status.get('arbi_monitor_only', {}),
        'markets': {
            m: {
                'trades': status['markets'][m].get('trades', {}),
                'health': status['markets'][m].get('health', {}),
                'trend': status['markets'][m].get('trend', {}),
                'capital_gate': status['markets'][m].get('capital_gate', {}),
                'score_pnl_corr': status['markets'][m].get('score_pnl_corr'),
                'signals_24h': status['markets'][m].get('signals_24h'),
                'best_symbols': status['markets'][m].get('best_symbols', []),
                'worst_symbols': status['markets'][m].get('worst_symbols', []),
                'insights': status['markets'][m].get('insights', []),
                'pattern_profile': status['markets'][m].get('pattern_profile', {}),
                'signal_quality': status['markets'][m].get('signal_quality', {}),
                'advisor_shadow': status['markets'][m].get('advisor_shadow', {}),
            }
            for m in MARKETS
        },
    }


def _briefing_text(status: Dict[str, Any]) -> str:
    lines = []
    gs = status['global_state']
    lines.append(f"Meta-Brain: {gs['decision']}. Melhor mercado: {gs['best_market']}. Pior mercado: {gs['worst_market']}.")
    mem = status.get('memory') or {}
    lines.append(f"Memoria: fase {mem.get('phase')} com maturidade {mem.get('maturity_score')} e {mem.get('total_memory_rows')} linhas observadas.")
    ds = status.get('data_sources') or {}
    if ds:
        lines.append(f"Dados: estado {ds.get('state')} com score {ds.get('score')}; fontes Polygon/BRAPI/Cedro em modo contexto read-only.")
    for item in status['market_edge']:
        lines.append(
            f"{item['market']}: {item['health_state']} "
            f"(saude {item['health_score']}, WR {item['win_rate']}%, "
            f"PnL {item['total_pnl']}, gate {item['capital_gate']})."
        )
    for alert in status.get('alerts', [])[:3]:
        lines.append(f"Alerta {alert.get('severity')}: {alert.get('market')} - {alert.get('title')}.")
    if status.get('arbi_monitor_only'):
        lines.append("Arbi: monitorada apenas como book segregado; Meta-Brain nao interfere.")
    return "\n".join(lines)


def build_briefing(
    db_fn: Callable[[], Any],
    lookback_days: int = 30,
    provider_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    status = build_meta_status(db_fn, lookback_days=lookback_days, provider_context=provider_context)
    if not status.get('ok'):
        return status
    return {
        'ok': True,
        'timestamp': status['timestamp'],
        'lookback_days': status['lookback_days'],
        'briefing': _briefing_text(status),
        'global_state': status['global_state'],
        'market_edge': status['market_edge'],
        'alerts': status.get('alerts', []),
        'recommendations': status.get('recommendations', []),
    }


def answer_question(
    db_fn: Callable[[], Any],
    question: str,
    lookback_days: int = 30,
    provider_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    q = (question or '').strip()
    ql = unicodedata.normalize('NFKD', q.lower()).encode('ascii', 'ignore').decode('ascii')
    status = build_meta_status(db_fn, lookback_days=lookback_days, provider_context=provider_context)
    if not status.get('ok'):
        return status

    if not q:
        answer = "Pergunte sobre operar, capital, mercado com edge, erros, B3, NYSE, cripto ou Arbi."
    elif any(w in ql for w in ('padrao', 'cluster', 'combina', 'feature')):
        parts = []
        for market in MARKETS:
            prof = status['markets'][market].get('pattern_profile', {})
            best = (prof.get('best_patterns') or [])[:1]
            worst = (prof.get('worst_patterns') or [])[:1]
            if best:
                p = best[0]
                parts.append(f"{market} melhor: {' x '.join(p['dimensions'])}={p['value']} com WR {p['win_rate']}% e PnL {p['pnl']}.")
            if worst:
                p = worst[0]
                parts.append(f"{market} pior: {' x '.join(p['dimensions'])}={p['value']} com WR {p['win_rate']}% e PnL {p['pnl']}.")
        answer = " ".join(parts) if parts else "Ainda nao ha amostra suficiente para padroes robustos."
    elif any(w in ql for w in ('recomend', 'fazer agora', 'prioridade', 'corrigir')):
        recs = status.get('recommendations', [])[:5]
        answer = "Prioridades: " + " ".join(
            f"{r['priority']} {r['market']}: {r['action']} ({r['detail']})." for r in recs
        )
    elif any(w in ql for w in ('memoria', 'maturidade', 'evolucao')):
        mem = status.get('memory') or {}
        answer = (
            f"O Meta-Brain esta na fase {mem.get('phase')} com maturidade {mem.get('maturity_score')}/100. "
            f"Memoria observada: {mem.get('total_memory_rows')} linhas, "
            f"{mem.get('trades_closed')} trades fechadas, {mem.get('signal_events')} sinais, "
            f"{mem.get('combo_weights')} pesos de combinacao e {mem.get('symbol_stats')} estatisticas por ativo."
        )
    elif any(w in ql for w in ('polygon', 'brapi', 'cedro', 'fonte', 'fontes', 'dados', 'data')):
        ds = status.get('data_sources') or {}
        providers = ds.get('providers') or {}
        parts = [f"Estado das fontes: {ds.get('state')} score {ds.get('score')}/100."]
        for name in ('polygon', 'brapi', 'cedro'):
            p = providers.get(name) or {}
            parts.append(
                f"{name.upper()}: configurado={bool(p.get('configured'))}, "
                f"conectado={bool(p.get('connected'))}, uso={p.get('role','contexto')}"
            )
        if ds.get('issues'):
            parts.append("Pontos de atencao: " + ", ".join(ds.get('issues') or []))
        parts.append("A politica atual e read-only: enriquecer diagnostico e recomendacao, sem executar trades.")
        answer = " ".join(parts)
    elif any(w in ql for w in ('capital', 'aumentar', 'reduzir', 'real')):
        gate = build_capital_gate(db_fn, lookback_days, provider_context=provider_context)
        parts = [f"Decisao global: {gate['global_state']['decision']}."]
        for market, info in gate['capital_gates'].items():
            parts.append(f"{market}: {info['decision']} ({info['reason']}).")
        answer = " ".join(parts)
    elif any(w in ql for w in ('edge', 'melhor mercado', 'qual mercado', 'mercado melhor')):
        top = status['market_edge'][0]
        answer = (
            f"O melhor edge agora e {top['market']}: estado {top['health_state']}, "
            f"score {top['health_score']}, WR {top['win_rate']}%, PnL {top['total_pnl']}."
        )
    elif any(w in ql for w in ('erro', 'perda', 'perdendo', 'pior')):
        errors = build_error_report(db_fn, lookback_days, provider_context=provider_context)['top_errors'][:5]
        if not errors:
            answer = "Nao encontrei erros relevantes na janela analisada."
        else:
            answer = "Principais erros: " + "; ".join(
                f"{e['market']} {e['symbol']} PnL {e['pnl']} WR {e['win_rate']}%" for e in errors
            )
    elif 'arbi' in ql or 'arbitragem' in ql:
        answer = "A Arbi esta em modo monitor-only no Meta-Brain. Eu observo o book segregado, mas nao bloqueio, altero ou executo nada na Arbi."
    elif 'advisor' in ql or 'shadow' in ql:
        parts = []
        for market in MARKETS:
            adv = status['markets'][market].get('advisor_shadow', {})
            if not adv.get('available'):
                parts.append(f"{market}: shadow advisor indisponivel ou sem tabela.")
            else:
                actions = adv.get('entry_actions') or []
                txt = ', '.join(f"{a.get('would_action')} n={a.get('n')} pnl={a.get('pnl')}" for a in actions[:4])
                parts.append(f"{market}: {txt or 'sem decisoes recentes'}.")
        answer = " ".join(parts)
    elif 'cripto' in ql or 'crypto' in ql:
        answer = _market_answer(status, 'CRYPTO')
    elif 'b3' in ql:
        answer = _market_answer(status, 'B3')
    elif 'nyse' in ql or 'eua' in ql or 'stocks' in ql:
        answer = _market_answer(status, 'NYSE')
    elif any(w in ql for w in ('operar', 'hoje', 'saudavel')):
        answer = build_briefing(db_fn, lookback_days, provider_context=provider_context)['briefing']
    else:
        answer = build_briefing(db_fn, lookback_days, provider_context=provider_context)['briefing']

    return {
        'ok': True,
        'question': q,
        'answer': answer,
        'timestamp': status['timestamp'],
        'lookback_days': status['lookback_days'],
        'source': 'meta_brain_mvp_read_only',
    }


def _market_answer(status: Dict[str, Any], market: str) -> str:
    data = status['markets'][market]
    trades = data['trades']
    gate = data['capital_gate']
    health = data['health']
    return (
        f"{market}: estado {health['state']} com saude {health['score']}. "
        f"WR {trades['win_rate']}%, PnL {trades['total_pnl']}, "
        f"stop-like {trades['stop_like_rate']}%, gate {gate['decision']} ({gate['reason']})."
    )
