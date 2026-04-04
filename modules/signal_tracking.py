"""
[v10.28] Signal tracking and learning persistence module.

Handles recording, updating, and persisting signal events and outcomes.
All database operations are parameterized through db_fn callback.

Key functions:
  - record_signal_event(sig, features, feature_hash, conf, insight, ...)
  - update_signal_attribution(signal_id, trade_id, order_id, db_fn)
  - update_signal_outcome(signal_id, trade_id, order_id, pnl, pnl_pct, close_reason, db_fn)
  - record_shadow_decision(signal_id, sig, reason, db_fn)

Helper persist functions (internal):
  - _db_save_signal_event(event, db_fn)
  - _db_update_signal_attribution(upd, db_fn)
  - _db_update_signal_outcome(upd, db_fn)
  - _db_upsert_pattern_stats(s, db_fn)
  - _db_upsert_factor_stats(s, db_fn)
  - _db_save_shadow_decision(shadow, db_fn)
  - _db_log_learning_audit(event_type, entity_id, payload, db_fn)
"""

import json
import logging
from datetime import datetime

log = logging.getLogger(__name__)


def gen_id(prefix: str) -> str:
    """Gera ID único com prefixo (ex. 'SIG', 'SHD')."""
    import uuid
    return f'{prefix}-{uuid.uuid4().hex[:8]}'


def record_signal_event(sig: dict, features: dict, feature_hash: str,
                         conf: dict, insight: str,
                         trade_id: str = None, order_id: str = None,
                         source_type: str = 'stock_signal_db',
                         existing_signal_id: str = None,
                         origin_signal_key: str = None,
                         db_fn=None, enqueue_fn=None, learning_lock=None,
                         signal_events_count_container=None) -> str:
    """[L-2][FIX-2][S2] Registra evento de sinal. Retorna signal_id.
    origin_signal_key: chave de origem do registro em market_signals (para dedup persistida).
    Se existing_signal_id for passado, faz UPDATE ao invés de INSERT.

    Args:
        sig, features, feature_hash, conf, insight: Signal data
        trade_id, order_id, source_type: Optional trade linkage
        existing_signal_id: If provided, updates existing signal instead of creating new one
        origin_signal_key: Dedup key for persistent deduplication
        db_fn: Callback to get database connection (unused here, enqueue_fn is primary path)
        enqueue_fn: Callback to enqueue async persistence
        learning_lock: Threading lock for atomic counter updates
        signal_events_count_container: Dict-like object with 'count' key for atomic increments
    """
    # Note: This function primarily enqueues async persistence.
    # The actual DB save happens via _db_save_signal_event called by persistence worker.
    try:
        signal_id = existing_signal_id or gen_id('SIG')
        dq_score = features.get('_dq_score', 50)
        payload = {k: v for k, v in sig.items()
                   if k not in ('payload_json',) and not isinstance(v, (list, dict))}
        payload.update(features)

        event = {
            'signal_id': signal_id,
            'feature_hash': feature_hash,
            'symbol': sig.get('symbol', ''),
            'asset_type': sig.get('asset_type', 'stock'),
            'market_type': sig.get('market_type', ''),
            'signal': sig.get('signal', ''),
            'raw_score': float(sig.get('score', 50) or 50),
            'learning_confidence': conf.get('final_confidence', 50),
            'confidence_band': conf.get('confidence_band', 'MEDIUM'),
            'price': float(sig.get('price', 0) or 0),
            'signal_created_at': datetime.utcnow().isoformat(),
            'market_regime_mode': features.get('regime_mode', ''),
            'market_regime_volatility': features.get('volatility_bucket', ''),
            'market_open': bool(sig.get('market_open', False)),
            'trade_open': bool(sig.get('trade_open', False)),
            'rsi': float(sig.get('rsi', 50) or 50),
            'ema9': float(sig.get('ema9', 0) or 0),
            'ema21': float(sig.get('ema21', 0) or 0),
            'ema50': float(sig.get('ema50', 0) or 0),
            'rsi_bucket': features.get('rsi_bucket', ''),
            'score_bucket': features.get('score_bucket', ''),
            'change_pct_bucket': features.get('change_pct_bucket', ''),
            'ema_alignment': features.get('ema_alignment', ''),
            'volatility_bucket': features.get('volatility_bucket', ''),
            'weekday': features.get('weekday', 0),
            'time_bucket': features.get('time_bucket', ''),
            'data_quality_score': dq_score,
            'source_type': source_type,
            'payload_json': json.dumps(payload, default=str),
            'insight_summary': insight,
            'learning_version': '1.0',  # Placeholder — caller should provide
            'origin_signal_key': origin_signal_key,
            'trade_id': trade_id,
            'order_id': order_id,
            'outcome_status': None,
            'outcome_pnl': None,
            'outcome_pnl_pct': None,
            'outcome_close_reason': None,
            'updated_at': datetime.utcnow().isoformat(),
        }

        # [v10.3.2-P0-1] Se for reavaliação (existing_signal_id vem do cache), gravar SÍNCRONO
        if existing_signal_id:
            confirmed_id = _db_save_signal_event(event, db_fn)
            if not confirmed_id:
                log.warning(f'record_signal_event: banco não confirmou signal_id {existing_signal_id} '
                           f'(origin_key={origin_signal_key}). Atribuição pode estar inconsistente.')
                return existing_signal_id
            return confirmed_id

        # Enqueue async persistence
        if enqueue_fn:
            enqueue_fn('signal_event', event)
        return signal_id
    except Exception as e:
        log.error(f'record_signal_event: {e}')
        return ''


def update_signal_attribution(signal_id: str, trade_id: str, order_id: str,
                              db_fn=None, enqueue_fn=None):
    """[FIX-2][v10.15] Vincula trade_id/order_id ao signal_event — crítico para calibração."""
    if not signal_id:
        return
    log.info(f'[ML-ATTRIB] {signal_id} → trade={trade_id} order={order_id}')
    try:
        update = {
            'signal_id': signal_id,
            'trade_id': trade_id,
            'order_id': order_id,
            'updated_at': datetime.utcnow().isoformat(),
        }
        if enqueue_fn:
            enqueue_fn('signal_attribution', update)
        else:
            _db_update_signal_attribution(update, db_fn)
    except Exception as e:
        log.error(f'update_signal_attribution: {e}')


def update_signal_outcome(signal_id: str, trade_id: str, order_id: str,
                          pnl: float, pnl_pct: float, close_reason: str,
                          db_fn=None, enqueue_fn=None):
    """[L-7] Vincula outcome de trade ao evento de sinal original."""
    if not signal_id:
        return
    try:
        update = {
            'signal_id': signal_id,
            'trade_id': trade_id,
            'order_id': order_id,
            'outcome_status': 'WIN' if pnl_pct > 0.1 else ('LOSS' if pnl_pct < -0.1 else 'FLAT'),
            'outcome_pnl': round(pnl, 4),
            'outcome_pnl_pct': round(pnl_pct, 4),
            'outcome_close_reason': close_reason,
            'updated_at': datetime.utcnow().isoformat(),
        }
        if enqueue_fn:
            enqueue_fn('signal_outcome', update)
        else:
            _db_update_signal_outcome(update, db_fn)
    except Exception as e:
        log.error(f'update_signal_outcome: {e}')


def record_shadow_decision(signal_id: str, sig: dict, reason: str,
                          db_fn=None, enqueue_fn=None):
    """[L-8] Registra sinal observado mas não executado."""
    try:
        shadow = {
            'shadow_id': gen_id('SHD'),
            'signal_id': signal_id,
            'symbol': sig.get('symbol', ''),
            'signal': sig.get('signal', ''),
            'price_at_signal': float(sig.get('price', 0) or 0),
            'not_executed_reason': reason,
            'hypothetical_entry': float(sig.get('price', 0) or 0),
            'evaluation_status': 'PENDING',
            'created_at': datetime.utcnow().isoformat(),
            'payload_json': json.dumps({'score': sig.get('score'), 'reason': reason}, default=str),
        }
        if enqueue_fn:
            enqueue_fn('shadow_decision', shadow)
        else:
            _db_save_shadow_decision(shadow, db_fn)
    except Exception as e:
        log.error(f'record_shadow_decision: {e}')


# ═══════════════════════════════════════════════════════════════
# PERSIST HELPERS — internal database operations
# ═══════════════════════════════════════════════════════════════

def _db_save_signal_event(event: dict, db_fn=None):
    """[L-2][P0-3] Persiste signal_event.
    ROW_COUNT() = 1 → insert real.
    ROW_COUNT() = 2 → ON DUPLICATE KEY UPDATE.
    Returns the actual signal_id from the database.
    """
    if not db_fn:
        return event.get('signal_id')
    conn = db_fn()
    if not conn:
        return None
    try:
        c = conn.cursor(dictionary=True)
        c.execute("""INSERT INTO signal_events (
            signal_id, feature_hash, symbol, asset_type, market_type, signal_type, raw_score,
            learning_confidence, confidence_band, price, signal_created_at,
            market_regime_mode, market_regime_volatility, market_open, trade_open,
            rsi, ema9, ema21, ema50, rsi_bucket, score_bucket, change_pct_bucket,
            ema_alignment, volatility_bucket, weekday, time_bucket, data_quality_score,
            source_type, payload_json, insight_summary, learning_version,
            origin_signal_key,
            trade_id, order_id, outcome_status, outcome_pnl, outcome_pnl_pct,
            outcome_close_reason, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                trade_id=COALESCE(VALUES(trade_id), trade_id),
                order_id=COALESCE(VALUES(order_id), order_id),
                learning_confidence=VALUES(learning_confidence),
                confidence_band=VALUES(confidence_band),
                insight_summary=VALUES(insight_summary),
                payload_json=VALUES(payload_json),
                updated_at=VALUES(updated_at)""",
            (event['signal_id'], event['feature_hash'], event['symbol'],
             event['asset_type'], event['market_type'], event.get('signal', ''),
             event['raw_score'], event['learning_confidence'], event['confidence_band'],
             event['price'], event['signal_created_at'], event['market_regime_mode'],
             event['market_regime_volatility'], event['market_open'], event['trade_open'],
             event['rsi'], event['ema9'], event['ema21'], event['ema50'],
             event['rsi_bucket'], event['score_bucket'], event['change_pct_bucket'],
             event['ema_alignment'], event['volatility_bucket'], event['weekday'],
             event['time_bucket'], event['data_quality_score'], event['source_type'],
             event['payload_json'], event['insight_summary'], event.get('learning_version', '1.0'),
             event.get('origin_signal_key'),
             event['trade_id'], event['order_id'], event['outcome_status'],
             event['outcome_pnl'], event['outcome_pnl_pct'],
             event['outcome_close_reason'], event['updated_at']))
        row_count = c.rowcount
        real_signal_id = event['signal_id']

        # [v10.3.3-F1] Se houve conflito por origin_signal_key, buscar ID real
        if row_count != 1 and event.get('origin_signal_key'):
            try:
                c2 = conn.cursor(dictionary=True)
                c2.execute("SELECT signal_id FROM signal_events WHERE origin_signal_key=%s LIMIT 1",
                           (event['origin_signal_key'],))
                row = c2.fetchone()
                c2.close()
                if row:
                    real_signal_id = row['signal_id']
            except Exception:
                pass
        conn.commit()
        c.close()
        conn.close()
        return real_signal_id
    except Exception as e:
        log.error(f'_db_save_signal_event: {e}')
        try:
            conn.close()
        except:
            pass
        return None


def _db_update_signal_attribution(upd: dict, db_fn=None):
    """[FIX-2] Vincula trade_id/order_id ao signal_event existente."""
    if not db_fn:
        return
    conn = db_fn()
    if not conn:
        return
    try:
        c = conn.cursor()
        c.execute("""UPDATE signal_events
                     SET trade_id=%s, order_id=%s, updated_at=%s
                     WHERE signal_id=%s""",
                  (upd['trade_id'], upd['order_id'], upd['updated_at'], upd['signal_id']))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log.error(f'_db_update_signal_attribution: {e}')
        try:
            conn.close()
        except:
            pass


def _db_update_signal_outcome(upd: dict, db_fn=None):
    """[L-7] Atualiza outcome de signal no banco."""
    if not db_fn:
        return
    conn = db_fn()
    if not conn:
        return
    try:
        c = conn.cursor()
        c.execute("""UPDATE signal_events SET
            trade_id=%s, order_id=%s, outcome_status=%s, outcome_pnl=%s,
            outcome_pnl_pct=%s, outcome_close_reason=%s, updated_at=%s
            WHERE signal_id=%s""",
            (upd['trade_id'], upd['order_id'], upd['outcome_status'],
             upd['outcome_pnl'], upd['outcome_pnl_pct'],
             upd['outcome_close_reason'], upd['updated_at'],
             upd['signal_id']))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log.error(f'_db_update_signal_outcome: {e}')
        try:
            conn.close()
        except:
            pass


def _db_upsert_pattern_stats(s: dict, db_fn=None):
    """[L-3] Upsert pattern statistics."""
    if not db_fn:
        return
    conn = db_fn()
    if not conn:
        return
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO pattern_stats (
            feature_hash, total_samples, wins, losses, flat_count,
            avg_pnl, avg_pnl_pct, ewma_pnl_pct, ewma_hit_rate,
            expectancy, downside_score, max_loss_seen, confidence_weight,
            last_seen_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
            total_samples=VALUES(total_samples), wins=VALUES(wins),
            losses=VALUES(losses), flat_count=VALUES(flat_count),
            avg_pnl=VALUES(avg_pnl), avg_pnl_pct=VALUES(avg_pnl_pct),
            ewma_pnl_pct=VALUES(ewma_pnl_pct), ewma_hit_rate=VALUES(ewma_hit_rate),
            expectancy=VALUES(expectancy), downside_score=VALUES(downside_score),
            max_loss_seen=VALUES(max_loss_seen), confidence_weight=VALUES(confidence_weight),
            last_seen_at=VALUES(last_seen_at), updated_at=VALUES(updated_at)""",
            (s['feature_hash'], s['total_samples'], s['wins'], s['losses'], s['flat_count'],
             s['avg_pnl'], s['avg_pnl_pct'], s['ewma_pnl_pct'], s['ewma_hit_rate'],
             s['expectancy'], s['downside_score'], s['max_loss_seen'], s['confidence_weight'],
             s['last_seen_at'], s['updated_at']))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log.error(f'_db_upsert_pattern_stats: {e}')
        try:
            conn.close()
        except:
            pass


def _db_upsert_factor_stats(s: dict, db_fn=None):
    """[L-3] Upsert factor statistics."""
    if not db_fn:
        return
    conn = db_fn()
    if not conn:
        return
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO factor_stats (
            factor_type, factor_value, total_samples, wins, losses,
            avg_pnl_pct, ewma_pnl_pct, expectancy, downside_score,
            confidence_weight, last_seen_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
            total_samples=VALUES(total_samples), wins=VALUES(wins),
            losses=VALUES(losses), avg_pnl_pct=VALUES(avg_pnl_pct),
            ewma_pnl_pct=VALUES(ewma_pnl_pct), expectancy=VALUES(expectancy),
            downside_score=VALUES(downside_score), confidence_weight=VALUES(confidence_weight),
            last_seen_at=VALUES(last_seen_at), updated_at=VALUES(updated_at)""",
            (s['factor_type'], s['factor_value'], s['total_samples'], s['wins'], s['losses'],
             s['avg_pnl_pct'], s['ewma_pnl_pct'], s['expectancy'], s['downside_score'],
             s['confidence_weight'], s['last_seen_at'], s['updated_at']))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log.error(f'_db_upsert_factor_stats: {e}')
        try:
            conn.close()
        except:
            pass


def _db_save_shadow_decision(shadow: dict, db_fn=None):
    """[L-8] Persiste shadow decision no MySQL."""
    if not db_fn:
        return
    conn = db_fn()
    if not conn:
        return
    try:
        c = conn.cursor()
        c.execute("""INSERT IGNORE INTO shadow_decisions (
            shadow_id, signal_id, symbol, signal_type, price_at_signal,
            not_executed_reason, hypothetical_entry, evaluation_status,
            created_at, payload_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (shadow['shadow_id'], shadow['signal_id'], shadow['symbol'],
             shadow.get('signal_type', shadow.get('signal', '')), shadow['price_at_signal'],
             shadow['not_executed_reason'], shadow['hypothetical_entry'],
             shadow['evaluation_status'], shadow['created_at'], shadow['payload_json']))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log.error(f'_db_save_shadow_decision: {e}')
        try:
            conn.close()
        except:
            pass


def _db_log_learning_audit(event_type: str, entity_id: str, payload: dict, db_fn=None):
    """[L-4] Log learning audit events."""
    if not db_fn:
        return
    conn = db_fn()
    if not conn:
        return
    try:
        c = conn.cursor()
        c.execute("INSERT INTO learning_audit (event_type, entity_id, payload_json) VALUES (%s,%s,%s)",
                  (event_type, entity_id, json.dumps(payload, default=str)))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log.error(f'_db_log_learning_audit: {e}')
        try:
            conn.close()
        except:
            pass
