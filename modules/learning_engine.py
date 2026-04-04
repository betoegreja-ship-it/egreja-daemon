"""
[v10.24] Learning Engine Module
Pattern & factor statistics, confidence calculation, and risk multipliers.
Functions accept cache dicts and config as parameters - no mutable global state in module.
"""

from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# EMPTY STRUCTURE FACTORIES
# ═══════════════════════════════════════════════════════════════

def empty_pattern_stats(feature_hash: str) -> dict:
    """Factory for empty pattern stats structure."""
    return {
        'feature_hash': feature_hash,
        'total_samples': 0, 'wins': 0, 'losses': 0, 'flat_count': 0,
        'avg_pnl': 0.0, 'avg_pnl_pct': 0.0,
        'ewma_pnl_pct': 0.0, 'ewma_hit_rate': 0.5,
        'expectancy': 0.0, 'downside_score': 0.0,
        'max_loss_seen': 0.0, 'confidence_weight': 0.0,
        'last_seen_at': '', 'updated_at': '',
    }

def empty_factor_stats(factor_type: str, factor_value: str) -> dict:
    """Factory for empty factor stats structure."""
    return {
        'factor_type': factor_type, 'factor_value': factor_value,
        'total_samples': 0, 'wins': 0, 'losses': 0,
        'avg_pnl_pct': 0.0, 'ewma_pnl_pct': 0.0,
        'expectancy': 0.0, 'downside_score': 0.0,
        'confidence_weight': 0.0,
        'last_seen_at': '', 'updated_at': '',
    }

# ═══════════════════════════════════════════════════════════════
# EWMA & CONFIDENCE WEIGHT HELPERS
# ═══════════════════════════════════════════════════════════════

def _update_ewma(current: float, new_value: float, alpha: float) -> float:
    """Update exponential weighted moving average."""
    return alpha * new_value + (1 - alpha) * current

def _calc_confidence_weight(total_samples: int, ewma_hit_rate: float,
                            expectancy: float, downside_score: float,
                            learning_min_samples: int = 10) -> float:
    """[L-3] Peso de confiança: aumenta com amostras, penaliza downside.

    Args:
        total_samples: Total number of samples for this pattern
        ewma_hit_rate: Exponential weighted moving average hit rate [0, 1]
        expectancy: Expected value per trade
        downside_score: Downside risk score
        learning_min_samples: Minimum samples threshold (from config)

    Returns:
        Confidence weight in range [-1.0, 1.0]
    """
    # Fator de amostras: sobe suavemente até N>=30
    sample_factor = min(total_samples / max(learning_min_samples * 3, 30), 1.0)
    # Fator de hit_rate normalizado (0.5 = neutro)
    hit_factor    = max(0.0, (ewma_hit_rate - 0.5) * 2)
    # Fator de expectancy (normalizado para [-1, 1])
    exp_factor    = max(-1.0, min(1.0, expectancy / 3.0))
    # Penalidade de downside
    down_penalty  = min(downside_score / 5.0, 1.0)

    raw = sample_factor * (0.4 + 0.3 * hit_factor + 0.3 * exp_factor) - 0.2 * down_penalty
    return max(-1.0, min(1.0, round(raw, 4)))

# ═══════════════════════════════════════════════════════════════
# PATTERN & FACTOR UPDATE FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def update_pattern_stats(feature_hash: str, pnl: float, pnl_pct: float,
                        pattern_cache: dict, learning_ewma_alpha: float,
                        learning_min_samples: int = 10) -> dict:
    """[L-3] Atualiza pattern_stats de forma incremental.

    Args:
        feature_hash: Hash of feature combination
        pnl: Profit/loss in currency
        pnl_pct: Profit/loss as percentage
        pattern_cache: Mutable dict to store pattern stats (passed by reference)
        learning_ewma_alpha: EWMA alpha parameter (from config)
        learning_min_samples: Minimum samples for confidence (from config)

    Returns:
        Updated pattern stats dict
    """
    alpha = learning_ewma_alpha
    now_s = datetime.utcnow().isoformat()

    s = pattern_cache.get(feature_hash) or empty_pattern_stats(feature_hash)
    s['total_samples'] += 1
    if pnl_pct > 0.1:
        s['wins'] += 1
    elif pnl_pct < -0.1:
        s['losses'] += 1
    else:
        s['flat_count'] += 1

    n = s['total_samples']
    # Média simples incremental (Welford)
    s['avg_pnl']     += (pnl - s['avg_pnl']) / n
    s['avg_pnl_pct'] += (pnl_pct - s['avg_pnl_pct']) / n
    # EWMA para recência
    s['ewma_pnl_pct']  = _update_ewma(s['ewma_pnl_pct'],  pnl_pct, alpha)
    hit = 1.0 if pnl_pct > 0.1 else 0.0
    s['ewma_hit_rate'] = _update_ewma(s['ewma_hit_rate'], hit, alpha)
    # Expectancy = win_rate * avg_win - loss_rate * avg_loss (simplificado)
    wins   = s['wins']
    losses = s['losses']
    s['expectancy'] = round(s['ewma_hit_rate'] * max(s['avg_pnl_pct'], 0)
                            - (1 - s['ewma_hit_rate']) * abs(min(s['avg_pnl_pct'], 0)), 4)
    # Downside: frequência de perdas grandes
    if pnl_pct < s['max_loss_seen']:
        s['max_loss_seen'] = round(pnl_pct, 4)
    loss_rate = losses / n if n > 0 else 0
    s['downside_score'] = round(loss_rate * abs(min(s['avg_pnl_pct'], 0)) * 10, 4)
    s['confidence_weight'] = _calc_confidence_weight(
        n, s['ewma_hit_rate'], s['expectancy'], s['downside_score'], learning_min_samples)
    s['last_seen_at'] = now_s
    s['updated_at'] = now_s
    pattern_cache[feature_hash] = s
    return dict(s)

def update_factor_stats(features: dict, pnl: float, pnl_pct: float,
                       factor_cache: dict, learning_ewma_alpha: float,
                       learning_min_samples: int = 10):
    """[L-4] Atualiza factor_stats incrementalmente para cada fator do sinal.

    Args:
        features: Feature dict from extract_features()
        pnl: Profit/loss in currency
        pnl_pct: Profit/loss as percentage
        factor_cache: Mutable dict to store factor stats (passed by reference)
        learning_ewma_alpha: EWMA alpha parameter (from config)
        learning_min_samples: Minimum samples for confidence (from config)
    """
    alpha   = learning_ewma_alpha
    now_s   = datetime.utcnow().isoformat()
    factors = [
        ('score_bucket',      features.get('score_bucket', '')),
        ('rsi_bucket',        features.get('rsi_bucket', '')),
        ('ema_alignment',     features.get('ema_alignment', '')),
        ('volatility_bucket', features.get('volatility_bucket', '')),
        ('regime_mode',       features.get('regime_mode', '')),
        ('time_bucket',       features.get('time_bucket', '')),
        ('weekday',           str(features.get('weekday', ''))),
        ('asset_type',        features.get('asset_type', '')),
        ('market_type',       features.get('market_type', '')),
        ('direction',         features.get('direction', '')),
        ('dq_bucket',         features.get('dq_bucket', '')),
        ('atr_bucket',        features.get('atr_bucket', '')),      # [v10.4]
        ('volume_bucket',     features.get('volume_bucket', '')),   # [v10.4]
    ]

    for ftype, fval in factors:
        if not fval:
            continue
        key = (ftype, fval)
        s   = factor_cache.get(key) or empty_factor_stats(ftype, fval)
        s['total_samples'] += 1
        n = s['total_samples']
        if pnl_pct > 0.1:
            s['wins'] += 1
        elif pnl_pct < -0.1:
            s['losses'] += 1
        s['avg_pnl_pct']   += (pnl_pct - s['avg_pnl_pct']) / n
        s['ewma_pnl_pct']   = _update_ewma(s['ewma_pnl_pct'], pnl_pct, alpha)
        hit = 1.0 if pnl_pct > 0.1 else 0.0
        hit_rate = _update_ewma(s.get('_ewma_hit', 0.5), hit, alpha)
        s['_ewma_hit'] = hit_rate
        s['expectancy'] = round(hit_rate * max(s['avg_pnl_pct'], 0)
                                - (1 - hit_rate) * abs(min(s['avg_pnl_pct'], 0)), 4)
        loss_rate = s['losses'] / n if n > 0 else 0
        s['downside_score'] = round(loss_rate * abs(min(s['avg_pnl_pct'], 0)) * 10, 4)
        s['confidence_weight'] = _calc_confidence_weight(
            n, hit_rate, s['expectancy'], s['downside_score'], learning_min_samples)
        s['last_seen_at'] = now_s
        s['updated_at'] = now_s
        factor_cache[key] = s

# ═══════════════════════════════════════════════════════════════
# CONFIDENCE CALCULATION
# ═══════════════════════════════════════════════════════════════

def calc_learning_confidence(sig: dict, features: dict, feature_hash: str,
                            pattern_cache: dict, factor_cache: dict,
                            learning_enabled: bool, learning_min_samples: int = 10) -> dict:
    """[L-5][P0-2] Calcula learning_confidence para um sinal.
    Retorna dict com breakdown completo — nada de caixa-preta.

    IMPORTANTE: base normaliza pela FORÇA do sinal, não pelo valor bruto.
    Score 85 (compra forte) e score 15 (venda forte) têm mesma força base = 0.70.
    Evita viés estrutural contra shorts.

    Args:
        sig: Signal dict with score
        features: Feature dict from extract_features()
        feature_hash: Hash from make_feature_hash()
        pattern_cache: Pattern stats cache dict
        factor_cache: Factor stats cache dict
        learning_enabled: Whether learning is enabled (from config)
        learning_min_samples: Minimum samples threshold (from config)

    Returns:
        dict with confidence breakdown and final score
    """
    if not learning_enabled:
        direction = features.get('direction', 'LONG') if features else 'LONG'
        return _neutral_confidence(sig.get('score', 50), direction)

    raw_score   = float(sig.get('score', 50) or 50)
    dq_score    = float(features.get('_dq_score', 50))
    regime_mode = features.get('regime_mode', 'UNKNOWN')
    direction   = features.get('direction', 'LONG')

    # ── [P0-2] Base: força relativa ao lado do sinal ──────────────
    # score 50 = neutro → força 0; score 100 ou 0 → força máxima
    # LONG:  score alto é bom   (ex. 85 → força = (85-50)/50 = 0.70)
    # SHORT: score baixo é bom  (ex. 15 → força = (50-15)/50 = 0.70)
    if direction == 'SHORT':
        signal_strength = (50 - raw_score) / 50.0   # scores baixos = curto forte
    else:
        signal_strength = (raw_score - 50) / 50.0   # scores altos = longo forte
    # Normalizar para [0, 1] — força negativa tratada como neutro (50%)
    base = max(0.0, min(1.0, 0.5 + signal_strength * 0.5))

    # ── Histórico do padrão ───────────────────────────────────
    ps = dict(pattern_cache.get(feature_hash, {}))
    p_samples = ps.get('total_samples', 0)
    p_cw      = ps.get('confidence_weight', 0.0)
    p_exp     = ps.get('expectancy', 0.0)

    # Shrinkage: peso do padrão cresce com amostras
    p_weight  = min(p_samples / max(learning_min_samples * 3, 30), 1.0)
    pattern_score = 0.5 + 0.5 * p_cw   # mapeia [-1,1] → [0,1]

    # ── Fatores individuais ───────────────────────────────────
    relevant = ['score_bucket', 'rsi_bucket', 'ema_alignment', 'regime_mode', 'direction']
    factor_scores = []
    for ftype in relevant:
        fval = features.get(ftype, '')
        if not fval:
            continue
        fs = factor_cache.get((ftype, fval), {})
        if fs.get('total_samples', 0) >= 5:
            factor_scores.append(0.5 + 0.5 * fs.get('confidence_weight', 0.0))
    factor_score = (sum(factor_scores) / len(factor_scores)) if factor_scores else 0.5

    # ── Ajuste de qualidade do dado ───────────────────────────
    dq_adj = (dq_score / 100.0 - 0.5) * 0.2   # ±0.1 no máximo

    # ── Ajuste de regime ─────────────────────────────────────
    regime_adj = 0.0
    if regime_mode == 'HIGH_VOL':
        regime_adj = -0.08
    elif regime_mode == 'TRENDING':
        regime_adj =  0.04

    # ── Penalidade por amostra pequena ───────────────────────
    sample_penalty = max(0.0, 0.15 * (1 - p_weight))

    # ── Composição final ─────────────────────────────────────
    # Peso: base 40%, padrão 30% (ajustado por shrinkage), fatores 20%, ajustes 10%
    if p_weight > 0:
        blended = (0.40 * base
                   + 0.30 * (p_weight * pattern_score + (1 - p_weight) * base)
                   + 0.20 * factor_score
                   + 0.10 * base)    # fallback parcial
    else:
        blended = 0.65 * base + 0.35 * factor_score

    final_raw   = blended + dq_adj + regime_adj - sample_penalty
    final_conf  = max(0.0, min(1.0, final_raw))
    final_score = round(final_conf * 100, 1)

    band = ('HIGH'   if final_score >= 65 else
            'MEDIUM' if final_score >= 40 else 'LOW')

    return {
        'final_confidence': final_score,
        'confidence_band':  band,
        'base_score':       round(base * 100, 1),
        'pattern_score':    round(pattern_score * 100, 1) if p_weight > 0 else None,
        'pattern_samples':  p_samples,
        'factor_score':     round(factor_score * 100, 1),
        'data_quality_adj': round(dq_adj * 100, 1),
        'regime_adj':       round(regime_adj * 100, 1),
        'sample_penalty':   round(sample_penalty * 100, 1),
        'feature_hash':     feature_hash,
    }

def _neutral_confidence(raw_score: float, direction: str = 'LONG') -> dict:
    """[P0-2][P6] Fallback — normaliza pela força do lado do sinal.
    Banda é calculada dinamicamente (não fixo MEDIUM).
    """
    if direction == 'SHORT':
        strength = max(0.0, (50 - raw_score) / 50.0)
    else:
        strength = max(0.0, (raw_score - 50) / 50.0)
    final = round(50 + strength * 50, 1)
    # [P6] Banda dinâmica: não travar em MEDIUM quando confiança for alta/baixa
    if   final >= 70:
        band = 'HIGH'
    elif final <= 40:
        band = 'LOW'
    else:
        band = 'MEDIUM'
    return {
        'final_confidence': final,
        'confidence_band':  band,
        'base_score':       round(final, 1),
        'pattern_score':    None, 'pattern_samples': 0,
        'factor_score':     50.0, 'data_quality_adj': 0.0,
        'regime_adj':       0.0,  'sample_penalty':   0.0,
        'feature_hash':     '',
    }

def get_risk_multiplier(conf: dict, risk_mult_min: float = 0.30, risk_mult_max: float = 1.50) -> float:
    """[L-9][v10.15] Multiplica size do position — agora contínuo, não discreto.
    Usa final_confidence (0-100) para interpolar linearmente entre MIN e MAX.
    conf=50 → mult=1.0 (neutro); conf=100 → MAX; conf=0 → MIN.

    Args:
        conf: Confidence dict from calc_learning_confidence()
        risk_mult_min: Minimum risk multiplier (from config)
        risk_mult_max: Maximum risk multiplier (from config)

    Returns:
        Risk multiplier in range [risk_mult_min, risk_mult_max]
    """
    fc = float(conf.get('final_confidence', 50) or 50)
    # Normalizar: 50=neutro(1.0), 100=MAX, 0=MIN
    if fc >= 50:
        # 50→100 mapeia para 1.0→RISK_MULT_MAX
        t = (fc - 50) / 50.0  # 0→1
        mult = 1.0 + t * (risk_mult_max - 1.0)
    else:
        # 0→50 mapeia para RISK_MULT_MIN→1.0
        t = fc / 50.0  # 0→1
        mult = risk_mult_min + t * (1.0 - risk_mult_min)
    return round(max(risk_mult_min, min(risk_mult_max, mult)), 3)

def should_trade_ml(features: dict, conf: dict, pattern_cache: dict, factor_cache: dict,
                   learning_enabled: bool, learning_degraded: bool = False,
                   asset_type: str = 'stock') -> tuple:
    """[v10.15] ML gate — consulta pattern_stats e factor_stats para decidir se deve operar.
    Retorna (should_trade: bool, reason: str, ml_score: float).
    ml_score: -1.0 (forte rejeição) a +1.0 (forte aprovação), 0=neutro.

    Args:
        features: Feature dict from extract_features()
        conf: Confidence dict from calc_learning_confidence()
        pattern_cache: Pattern stats cache dict
        factor_cache: Factor stats cache dict
        learning_enabled: Whether learning is enabled
        learning_degraded: Whether learning is in degraded mode
        asset_type: Asset type (stock, crypto)

    Returns:
        tuple (should_trade, reason, ml_score)
    """
    if not learning_enabled or learning_degraded:
        return True, 'learning_disabled', 0.0

    fc = float(conf.get('final_confidence', 50) or 50)
    feat_hash = conf.get('feature_hash', '')

    # 1. Checar pattern histórico
    pattern_score = 0.0
    ps = pattern_cache.get(feat_hash, {})
    if ps.get('total_samples', 0) >= 15:
        p_exp = ps.get('expectancy', 0)
        p_wr = ps.get('wins', 0) / max(ps['total_samples'], 1) * 100
        # Rejeitar padrões consistentemente perdedores
        if p_exp < -0.15 and p_wr < 40:
            return False, f'ML_PATTERN_REJECT(exp={p_exp:.3f},wr={p_wr:.0f}%,n={ps["total_samples"]})', -0.8
        pattern_score = min(max(p_exp * 2, -1.0), 1.0)

    # 2. Checar fatores críticos
    bad_factors = 0
    good_factors = 0
    critical_factors = ['atr_bucket', 'volatility_bucket', 'regime_mode', 'volume_bucket', 'weekday']
    for ftype in critical_factors:
        fval = str(features.get(ftype, ''))
        if not fval:
            continue
        fs = factor_cache.get((ftype, fval), {})
        if fs.get('total_samples', 0) < 10:
            continue
        f_exp = fs.get('expectancy', 0)
        f_cw = fs.get('confidence_weight', 0.5)
        if f_exp < -0.1 and f_cw < 0.35:
            bad_factors += 1
        elif f_exp > 0.05 and f_cw > 0.45:
            good_factors += 1

    # Rejeitar se 3+ fatores críticos são negativos e nenhum é positivo
    if bad_factors >= 3 and good_factors == 0:
        return False, f'ML_FACTORS_REJECT(bad={bad_factors},good={good_factors})', -0.6

    # 3. Confiança muito baixa = rejeitar (crypto-specific)
    if fc < 30 and asset_type == 'crypto':
        return False, f'ML_LOW_CONF(fc={fc:.1f})', -0.5

    # Calcular ml_score geral
    conf_score = (fc - 50) / 50.0  # -1 a +1
    ml_score = 0.4 * conf_score + 0.4 * pattern_score + 0.2 * ((good_factors - bad_factors) / max(len(critical_factors), 1))
    return True, 'ML_OK', round(ml_score, 3)

def get_top_factors(factor_cache: dict, learning_min_samples: int = 10,
                   n_best: int = 5, n_worst: int = 5) -> dict:
    """[L-6] Retorna fatores com melhor e pior performance histórica.

    Args:
        factor_cache: Factor stats cache dict
        learning_min_samples: Minimum samples threshold (from config)
        n_best: Number of best factors to return
        n_worst: Number of worst factors to return

    Returns:
        dict with top_positive and top_negative factor lists
    """
    items = [(k, dict(v)) for k, v in factor_cache.items()
             if v.get('total_samples', 0) >= learning_min_samples]
    items.sort(key=lambda x: x[1].get('confidence_weight', 0), reverse=True)

    def _fmt(entry):
        k, v = entry
        return {'factor_type': k[0], 'factor_value': k[1],
                'samples': v['total_samples'], 'cw': round(v['confidence_weight'], 3),
                'expectancy': round(v.get('expectancy', 0), 4),
                'ewma_pnl_pct': round(v.get('ewma_pnl_pct', 0), 4)}

    return {
        'top_positive': [_fmt(i) for i in items[:n_best]],
        'top_negative': [_fmt(i) for i in reversed(items[-n_worst:]) if items],
    }
