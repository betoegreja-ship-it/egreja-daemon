"""Learning Interpreter — visão consolidada do estado de aprendizado.

Função principal: analyze_learning_state(db_fn, log, asset_type) -> dict

Lê trades, signal_events, pattern_stats e fornece um "retrato" do estado:
- quantidade de dados
- performance agregada por período
- quais áreas têm dados suficientes
- quais têm "aprendizado cego" (muitos samples, pouca inteligência)
"""
from __future__ import annotations
from typing import Any, Callable, Dict, Optional
from .isolation import should_bypass_adaptive_learning


def analyze_learning_state(
    db_fn: Callable,
    log,
    asset_type: str,
    lookback_days: int = 30,
) -> Dict[str, Any]:
    """Retrato consolidado do aprendizado pra um asset_type.

    Args:
        db_fn: função que retorna conexão MySQL (get_db do api_server)
        log: logger
        asset_type: 'stock' ou 'crypto'
        lookback_days: janela de análise (padrão 30d)

    Returns:
        {
          "bypassed": bool,
          "asset_type": str,
          "lookback_days": int,
          "trades": {...},        # volume/resultado
          "learning": {...},      # uso de learning_confidence
          "patterns": {...},      # patterns catalogados
          "health": {...},        # saúde do aprendizado
          "red_flags": [...],     # alertas
        }
    """
    if should_bypass_adaptive_learning(asset_type):
        return {'bypassed': True, 'asset_type': asset_type}

    conn = None
    try:
        conn = db_fn()
        if not conn:
            log.warning('[ADAPTIVE] analyze_learning_state: sem DB')
            return {'bypassed': False, 'error': 'no_db'}

        report: Dict[str, Any] = {
            'bypassed': False,
            'asset_type': asset_type,
            'lookback_days': lookback_days,
            'red_flags': [],
        }
        c = conn.cursor(dictionary=True)

        # === Bloco 1: Trades agregado ===
        c.execute(f"""
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins,
                   SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) AS losses,
                   SUM(pnl) AS total_pnl,
                   AVG(pnl_pct) AS avg_pnl_pct
            FROM trades
            WHERE asset_type = %s AND status = 'CLOSED'
              AND closed_at > NOW() - INTERVAL {int(lookback_days)} DAY
        """, (asset_type,))
        r = c.fetchone() or {}
        n = int(r.get('n') or 0)
        w = int(r.get('wins') or 0)
        l = int(r.get('losses') or 0)
        wr = w / max(w + l, 1) if (w + l) else 0.0
        report['trades'] = {
            'n': n, 'wins': w, 'losses': l,
            'win_rate': round(wr, 4),
            'total_pnl': float(r.get('total_pnl') or 0),
            'avg_pnl_pct': float(r.get('avg_pnl_pct') or 0),
        }
        if n < 50:
            report['red_flags'].append(f'poucos_dados_{asset_type}:{n}_trades')

        # === Bloco 2: Uso de learning_confidence ===
        c.execute(f"""
            SELECT COUNT(*) AS total,
                   COUNT(learning_confidence) AS com_conf,
                   AVG(learning_confidence) AS avg_conf
            FROM trades
            WHERE asset_type = %s AND status = 'CLOSED'
              AND closed_at > NOW() - INTERVAL {int(lookback_days)} DAY
        """, (asset_type,))
        r2 = c.fetchone() or {}
        total = int(r2.get('total') or 0)
        com = int(r2.get('com_conf') or 0)
        pct_com = com / max(total, 1)
        report['learning'] = {
            'trades_total': total,
            'trades_com_learning_confidence': com,
            'pct_com_confidence': round(pct_com, 4),
            'avg_confidence': float(r2.get('avg_conf') or 0),
        }
        if pct_com < 0.90 and total > 50:
            report['red_flags'].append(
                f'learning_confidence_faltando:{round(pct_com*100,1)}%_preenchido')

        # === Bloco 3: pattern_stats (quantos conhecemos) ===
        c.execute("""SELECT COUNT(*) AS n FROM pattern_stats
                     WHERE total_samples >= 10""")
        r3 = c.fetchone() or {}
        report['patterns'] = {
            'padroes_com_10plus_samples': int(r3.get('n') or 0),
        }

        # === Bloco 4: Saúde ===
        trades = report['trades']
        health_score = 0
        if trades['n'] >= 200: health_score += 25
        if trades['win_rate'] >= 0.50: health_score += 25
        if trades['total_pnl'] > 0: health_score += 25
        if report['learning']['pct_com_confidence'] >= 0.95: health_score += 25
        report['health'] = {
            'score': health_score,
            'label': _health_label(health_score),
        }

        return report
    except Exception as e:
        log.warning(f'[ADAPTIVE] analyze_learning_state erro: {e}')
        return {'bypassed': False, 'error': str(e)}
    finally:
        if conn:
            try: conn.close()
            except Exception: pass


def _health_label(score: int) -> str:
    if score >= 75: return 'SAUDAVEL'
    if score >= 50: return 'ATENCAO'
    if score >= 25: return 'DEGRADADO'
    return 'CRITICO'
