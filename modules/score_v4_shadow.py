# -*- coding: utf-8 -*-
"""[P1 24-jul-2026, decisao Beto + revisao externa] SCORE V4 — SHADOW PURO.

Especificacao: score_v4_spec_2026-07-24 (3 rodadas de revisao independente).
NADA aqui altera decisao, sizing, stop ou saida. Log-only.

Pilares (as 4 correcoes obrigatorias da revisao final):
  1. direction_score_v4 (bullish/bearish do MERCADO) SEPARADO de
     trade_quality_v4 (alinhamento com o TRADE). Decis/AUC/expectativa usam
     trade_quality_v4 — sem isso, shorts perfeitos seriam avaliados invertidos.
  2. ADX = modulador de confianca da familia de tendencia (nao vota direcao).
     +DI/-DI ficam logados brutos.
  3. Indicador ausente -> renormaliza peso da familia (coverage), nunca voto 0.
     total_coverage < 0.70 -> score_v4_valid=False.
  4. strength = conviccao x concordancia x cobertura (3 metricas separadas).

Snapshot IMUTAVEL na decisao (votos, familias, pesos, regime, hash config).
No fechamento apenas ACRESCENTA outcome + contrafactuais (nunca sobrescreve).
VOIDED/REJECTED -> pnl NULL (nunca zero).
Contrafactuais em coluna: capped 0.85-1.15, flat notional 100k, flat risk-ATR,
score_v3_sanitized (sem supertrend/atr direcional).
Quarentena NYSE+TRENDING: so classificacao (regime_status/shadow_no_trade).
"""
import os, json, hashlib, logging
from datetime import datetime, timezone

import pymysql

log = logging.getLogger('egreja.scorev4')

WEIGHTS_VERSION = 'v4_2026-07-24'
SCHEMA_VERSION = 'score_log_v4_1'

# pesos de FAMILIA por mercado/regime. MIXED/CHOPPY = fallback explicito
# (regime_weights_status=FALLBACK; nao usar p/ promover regras de regime).
REGIME_WEIGHTS = {
    'B3': {'TRENDING': (0.60, 0.35, 0.05), 'RANGING': (0.30, 0.25, 0.45),
           'MIXED': (1/3, 1/3, 1/3), 'CHOPPY': (1/3, 1/3, 1/3)},
    'NYSE': {'TRENDING': (0.45, 0.40, 0.15), 'RANGING': (0.30, 0.40, 0.30),
             'MIXED': (1/3, 1/3, 1/3), 'CHOPPY': (1/3, 1/3, 1/3)},
    'CRYPTO': {'TRENDING': (0.35, 0.40, 0.25), 'RANGING': (0.35, 0.40, 0.25),
               'MIXED': (1/3, 1/3, 1/3), 'CHOPPY': (1/3, 1/3, 1/3)},
}
TREND_W = {'macd': 0.35, 'ema_cross': 0.35, 'ichimoku': 0.30}   # ADX modula, nao vota
FLOW_W = {'obv': 0.55, 'vwap': 0.45}
OSC_W = {'rsi': 0.25, 'cci': 0.25, 'bollinger': 0.30, 'stoch': 0.20}  # williams REMOVIDO

_cfg_blob = json.dumps({'w': REGIME_WEIGHTS, 't': TREND_W, 'f': FLOW_W,
                        'o': OSC_W, 'v': WEIGHTS_VERSION}, sort_keys=True)
CONFIG_HASH = hashlib.sha256(_cfg_blob.encode()).hexdigest()[:16]


def _clip(x, lo, hi):
    return max(lo, min(hi, x))


def weighted_available_mean(votes, weights):
    """Media ponderada SO dos indicadores presentes; renormaliza pesos.
    Retorna (family_score|None, coverage 0-1)."""
    num = 0.0; den = 0.0
    for name, w in weights.items():
        v = votes.get(name)
        if v is None:
            continue
        num += w * _clip(float(v), -1.0, 1.0)
        den += abs(w)
    total = sum(abs(w) for w in weights.values())
    if den == 0.0:
        return None, 0.0
    return num / den, den / total


def compute_v4(votes, diag, market, regime, direction):
    """Calcula o V4 a partir dos MESMOS votos/diagnostico do v3 da decisao.
    Retorna dict completo (familias, coverage, direction/quality, strength)."""
    market = (market or 'NYSE').upper()
    regime = (regime or 'MIXED').upper()
    if regime not in ('TRENDING', 'RANGING', 'MIXED', 'CHOPPY'):
        regime = 'MIXED'
    weights_status = 'CALIBRATED' if regime in ('TRENDING', 'RANGING') else 'FALLBACK'

    trend_core, trend_cov = weighted_available_mean(votes, TREND_W)
    flow, flow_cov = weighted_available_mean(votes, FLOW_W)
    osc, osc_cov = weighted_available_mean(votes, OSC_W)

    # ADX como MODULADOR de confianca da tendencia (nunca direcao)
    adx_val = diag.get('adx')
    adx_conf = _clip(((adx_val or 0) - 18.0) / 22.0, 0.0, 1.0) if adx_val is not None else 0.5
    trend = trend_core * (0.50 + 0.50 * adx_conf) if trend_core is not None else None

    w_t, w_f, w_o = REGIME_WEIGHTS.get(market, REGIME_WEIGHTS['NYSE'])[regime] \
        if regime in REGIME_WEIGHTS.get(market, {}) else REGIME_WEIGHTS['NYSE']['MIXED']

    fams = [(w_t, trend, trend_cov), (w_f, flow, flow_cov), (w_o, osc, osc_cov)]
    avail = [(w, v) for w, v, _ in fams if v is not None]
    total_cov = sum(w * c for w, _, c in fams) / (w_t + w_f + w_o)
    valid = total_cov >= 0.70 and bool(avail)

    if not avail:
        return dict(valid=False, total_coverage=0.0, weights_status=weights_status,
                    direction_raw=None, direction_score=None, alignment_raw=None,
                    trade_quality=None, trend_family=None, flow_family=flow,
                    osc_family=osc, trend_coverage=trend_cov, flow_coverage=flow_cov,
                    osc_coverage=osc_cov, conviction=None, agreement=None,
                    strength=None, adx_confidence=adx_conf,
                    w_trend=w_t, w_flow=w_f, w_osc=w_o)

    wsum = sum(w for w, _ in avail)
    direction_raw = sum(w * v for w, v in avail) / wsum
    direction_score = (direction_raw + 1.0) * 50.0
    dsign = 1.0 if str(direction).upper() == 'LONG' else -1.0
    alignment_raw = direction_raw * dsign
    trade_quality = (alignment_raw + 1.0) * 50.0

    conviction = abs(direction_raw)
    disagreement = sum(w * abs(v - direction_raw) for w, v in avail) / (2.0 * wsum)
    agreement = _clip(1.0 - disagreement, 0.0, 1.0)
    strength = conviction * agreement * total_cov

    return dict(valid=valid, total_coverage=round(total_cov, 4),
                weights_status=weights_status,
                direction_raw=round(direction_raw, 4),
                direction_score=round(direction_score, 2),
                alignment_raw=round(alignment_raw, 4),
                trade_quality=round(trade_quality, 2),
                trend_family=None if trend is None else round(trend, 4),
                flow_family=None if flow is None else round(flow, 4),
                osc_family=None if osc is None else round(osc, 4),
                trend_coverage=round(trend_cov, 3), flow_coverage=round(flow_cov, 3),
                osc_coverage=round(osc_cov, 3),
                conviction=round(conviction, 4), agreement=round(agreement, 4),
                strength=round(strength, 4), adx_confidence=round(adx_conf, 3),
                w_trend=w_t, w_flow=w_f, w_osc=w_o)


def score_v3_sanitized(votes, regime, asset_type):
    """v3 recalculado SEM supertrend e SEM atr direcional (custo do bug)."""
    try:
        from modules.score_engine_v2 import get_weights_by_regime
        w = dict(get_weights_by_regime(regime or 'MIXED', asset_type or 'stock'))
    except Exception:
        w = {'rsi': 12, 'macd': 14, 'bollinger': 10, 'adx': 10, 'cci': 7,
             'ema_cross': 12, 'stoch': 7, 'williams': 6, 'vwap': 7,
             'obv': 4, 'ichimoku': 2}
    w.pop('supertrend', None); w.pop('atr', None)
    num = 0.0; den = 0.0
    for k, wt in w.items():
        v = votes.get(k)
        if v is None:
            continue
        num += wt * _clip(float(v), -1, 1); den += wt
    if den == 0:
        return None
    return round(50 + (num / den) * 50, 2)


# ═══════════════ persistencia ═══════════════

def _conn():
    return pymysql.connect(
        host=os.environ['MYSQLHOST'], user=os.environ['MYSQLUSER'],
        password=os.environ['MYSQLPASSWORD'], database=os.environ['MYSQLDATABASE'],
        port=int(os.environ.get('MYSQLPORT', 3306)), autocommit=True)


_table_ok = {'v': False}


def create_table():
    if _table_ok['v']:
        return
    c = _conn(); cur = c.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS score_log_v4 (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        schema_version VARCHAR(20) DEFAULT 'score_log_v4_1',
        book_trade_id VARCHAR(80) UNIQUE,
        trade_id VARCHAR(40), signal_id VARCHAR(60), strategy_id VARCHAR(40),
        symbol VARCHAR(16), market VARCHAR(8), currency VARCHAR(6),
        direction VARCHAR(6), venue VARCHAR(16), data_provider VARCHAR(16),
        bar_timeframe VARCHAR(6),
        trade_status VARCHAR(16) DEFAULT 'OPEN',
        rejection_reason VARCHAR(64) NULL, void_reason VARCHAR(64) NULL,
        decision_timestamp DATETIME(3), execution_timestamp DATETIME(3) NULL,
        regime_raw VARCHAR(10), regime_smoothed VARCHAR(10),
        regime_status VARCHAR(12) DEFAULT 'NORMAL',
        regime_weights_status VARCHAR(12),
        score_live DECIMAL(6,2), score_v3_original DECIMAL(6,2),
        score_v3_sanitized DECIMAL(6,2),
        direction_raw_v4 DECIMAL(7,4), direction_score_v4 DECIMAL(6,2),
        alignment_raw_v4 DECIMAL(7,4), trade_quality_v4 DECIMAL(6,2),
        strength_v4 DECIMAL(6,4), conviction_v4 DECIMAL(6,4),
        agreement_v4 DECIMAL(6,4), total_coverage DECIMAL(5,3),
        score_v4_valid TINYINT DEFAULT 1,
        shadow_no_trade TINYINT DEFAULT 0,
        size_mult_original DECIMAL(5,2), size_live DECIMAL(14,2),
        atr_pct_entry DECIMAL(8,4),
        pnl_real DECIMAL(14,2) NULL, pnl_pct DECIMAL(9,4) NULL,
        pnl_if_capped DECIMAL(14,2) NULL,
        pnl_if_flat_notional DECIMAL(14,2) NULL,
        pnl_if_flat_atr DECIMAL(14,2) NULL,
        mfe DECIMAL(9,4) NULL, mae DECIMAL(9,4) NULL,
        hold_min INT NULL, close_reason VARCHAR(40) NULL,
        audit_ok TINYINT DEFAULT 1,
        weights_version VARCHAR(24), config_hash VARCHAR(20),
        votes_json TEXT, families_json TEXT, audit_errors_json TEXT,
        counterfactuals_json TEXT, feature_snapshot_json MEDIUMTEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        closed_logged_at TIMESTAMP NULL,
        INDEX ix_market (market), INDEX ix_strategy (strategy_id),
        INDEX ix_quality (trade_quality_v4), INDEX ix_status (trade_status),
        INDEX ix_decision (decision_timestamp), INDEX ix_trade (trade_id)
        ) CHARACTER SET utf8mb4""")
    c.close()
    _table_ok['v'] = True


CURR = {'B3': 'BRL', 'NYSE': 'USD', 'CRYPTO': 'USDT'}


def log_decision(trade_id, signal_id, strategy_id, symbol, market, direction,
                 v3_result, score_live, regime, size_mult, size_live,
                 atr_pct, data_provider='INTERNAL', venue='PAPER',
                 bar_timeframe='mixed', trade_status='OPEN',
                 rejection_reason=None):
    """SNAPSHOT IMUTAVEL na decisao. Fail-open: erro nunca afeta o trade."""
    try:
        create_table()
        votes = dict(v3_result.get('votes') or {})
        diag = dict(v3_result.get('diagnostic') or {})
        market = (market or '').upper()
        v4 = compute_v4(votes, diag, market, regime, direction)
        sanitized = score_v3_sanitized(votes, regime,
                                       'crypto' if market == 'CRYPTO' else 'stock')
        audit_errors = []
        if not strategy_id:
            audit_errors.append('MISSING_STRATEGY_ID')
        if not v4['valid']:
            audit_errors.append('LOW_COVERAGE')
        for k, v in votes.items():
            if v is not None and not (-1.0 <= float(v) <= 1.0):
                audit_errors.append(f'VOTE_OUT_OF_RANGE_{k}')
        quarantine = (market == 'NYSE' and str(regime).upper() == 'TRENDING')
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        snap = {'votes': votes, 'diag': {k: diag.get(k) for k in
                ('rsi', 'adx', 'atr_pct', 'bb_pct_b', 'macd_hist', 'cci',
                 'stoch_k', 'williams', 'ema_alignment', 'vwap_dev',
                 'obv_trend', 'plus_di', 'minus_di', 'n_bars')},
                'regime': regime, 'weights': [v4['w_trend'], v4['w_flow'], v4['w_osc']],
                'adx_confidence': v4['adx_confidence']}
        snap_json = json.dumps(snap, default=str)
        c = _conn(); cur = c.cursor()
        cur.execute("""INSERT IGNORE INTO score_log_v4 (schema_version,book_trade_id,
            trade_id,signal_id,strategy_id,symbol,market,currency,direction,venue,
            data_provider,bar_timeframe,trade_status,rejection_reason,
            decision_timestamp,regime_raw,regime_smoothed,regime_status,
            regime_weights_status,score_live,score_v3_original,score_v3_sanitized,
            direction_raw_v4,direction_score_v4,alignment_raw_v4,trade_quality_v4,
            strength_v4,conviction_v4,agreement_v4,total_coverage,score_v4_valid,
            shadow_no_trade,size_mult_original,size_live,atr_pct_entry,audit_ok,
            weights_version,config_hash,votes_json,families_json,audit_errors_json,
            feature_snapshot_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (SCHEMA_VERSION, f'{trade_id or signal_id}:V4_SHADOW',
             trade_id, signal_id, strategy_id, symbol, market, CURR.get(market, 'OTHER'),
             str(direction).upper(), venue, data_provider, bar_timeframe,
             trade_status, rejection_reason,
             now, regime, regime, 'QUARANTINE' if quarantine else 'NORMAL',
             v4['weights_status'], score_live, v3_result.get('score'), sanitized,
             v4['direction_raw'], v4['direction_score'], v4['alignment_raw'],
             v4['trade_quality'], v4['strength'], v4['conviction'], v4['agreement'],
             v4['total_coverage'], 1 if v4['valid'] else 0,
             1 if quarantine else 0, size_mult, size_live, atr_pct,
             0 if audit_errors else 1, WEIGHTS_VERSION, CONFIG_HASH,
             json.dumps(votes), json.dumps({'trend': v4['trend_family'],
                'flow': v4['flow_family'], 'osc': v4['osc_family'],
                'trend_cov': v4['trend_coverage'], 'flow_cov': v4['flow_coverage'],
                'osc_cov': v4['osc_coverage']}),
             json.dumps(audit_errors), snap_json))
        c.close()
    except Exception as e:
        log.debug(f'[V4] log_decision: {e}')


def log_close(trade_id, pnl, pnl_pct, close_reason, hold_min=None,
              voided=False, void_reason=None):
    """Acrescenta outcome + contrafactuais. VOIDED -> pnl NULL, nunca zero."""
    try:
        create_table()
        c = _conn(); cur = c.cursor()
        cur.execute("""SELECT size_mult_original,size_live,atr_pct_entry,market
            FROM score_log_v4 WHERE trade_id=%s AND trade_status IN ('OPEN','SIGNALLED')
            ORDER BY id DESC LIMIT 1""", (trade_id,))
        row = cur.fetchone()
        if not row:
            c.close(); return
        mult0, size_live, atr_pct, market = (float(row[0] or 1), float(row[1] or 0),
                                             float(row[2] or 0), row[3])
        if voided:
            cur.execute("""UPDATE score_log_v4 SET trade_status='VOIDED',
                void_reason=%s, close_reason=%s, closed_logged_at=NOW()
                WHERE trade_id=%s AND trade_status IN ('OPEN','SIGNALLED')""",
                (void_reason, close_reason, trade_id))
            c.close(); return
        pnl = float(pnl or 0); pnl_pct = float(pnl_pct or 0)
        # contrafactuais: pnl escala linearmente com notional (mesma entrada/saida)
        capped_mult = _clip(mult0, 0.85, 1.15)
        pnl_if_capped = pnl * (capped_mult / mult0) if mult0 else None
        pnl_if_flat_notional = (pnl_pct / 100.0) * 100000.0
        # flat risk-ATR: risco fixo $1000, stop=1.5*ATR
        stop_pct = max(0.05, (atr_pct or 1.0) * 1.5)
        notional_flat_atr = min(1000.0 / (stop_pct / 100.0), 500000.0)
        pnl_if_flat_atr = (pnl_pct / 100.0) * notional_flat_atr
        cur.execute("""UPDATE score_log_v4 SET trade_status='CLOSED',
            pnl_real=%s, pnl_pct=%s, pnl_if_capped=%s, pnl_if_flat_notional=%s,
            pnl_if_flat_atr=%s, hold_min=%s, close_reason=%s,
            counterfactuals_json=%s, closed_logged_at=NOW()
            WHERE trade_id=%s AND trade_status IN ('OPEN','SIGNALLED')""",
            (round(pnl, 2), round(pnl_pct, 4),
             None if pnl_if_capped is None else round(pnl_if_capped, 2),
             round(pnl_if_flat_notional, 2), round(pnl_if_flat_atr, 2),
             hold_min, close_reason,
             json.dumps({'capped_mult': capped_mult, 'orig_mult': mult0,
                         'notional_flat_atr': round(notional_flat_atr, 0),
                         'stop_pct_used': round(stop_pct, 3),
                         # [revisao 24-jul] moedas NUNCA misturadas: risco e
                         # notional contrafactuais sao na moeda do BOOK do
                         # mercado (B3=BRL-book, NYSE=USD, CRYPTO=USDT).
                         # Comparacao entre mercados: usar pnl_pct/r-multiple.
                         'risk_ccy': CURR.get(market, 'OTHER'),
                         'risk_budget_book_ccy': 1000.0,
                         'flat_notional_book_ccy': 100000.0,
                         'nota': 'aproximacao analitica: sem lote-padrao/custos; livro executavel = P2'}),
             trade_id))
        c.close()
    except Exception as e:
        log.debug(f'[V4] log_close: {e}')
