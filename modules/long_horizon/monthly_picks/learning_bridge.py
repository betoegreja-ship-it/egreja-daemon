"""
Monthly Picks — Learning Bridge.

Single point of integration with the Unified Brain.
Publishes structured events — no scattered brain calls in business logic.

Events:
  - MONTHLY_PICK_OPENED
  - MONTHLY_PICK_REJECTED
  - MONTHLY_PICK_CLOSED
  - MONTHLY_PICK_THESIS_BROKEN
  - MONTHLY_PICK_TIMEOUT
  - MONTHLY_PICK_TARGET_HIT
  - MONTHLY_PICK_STOP_LOSS
  - MONTHLY_PICK_REVIEW_HOLD
  - MONTHLY_PICK_REVIEW_REDUCE
"""

import json
import logging
from typing import Dict, Optional, Callable

from .config import LearningEvent, ExitReason

logger = logging.getLogger('egreja.monthly_picks.learning_bridge')


class LearningBridge:
    """
    Bridge between Monthly Picks and the Unified Brain.

    All learning events flow through here as structured payloads.
    The bridge calls enqueue_brain_lesson() or enqueue_brain_decision()
    from the api_server — passed in as callables at init.
    """

    def __init__(self, brain_lesson_fn: Callable = None,
                 brain_decision_fn: Callable = None,
                 log=None):
        """
        Args:
            brain_lesson_fn: callable like enqueue_brain_lesson(module, description, **kw)
            brain_decision_fn: callable like enqueue_brain_decision(type, module, rec, **kw)
        """
        self.brain_lesson_fn = brain_lesson_fn
        self.brain_decision_fn = brain_decision_fn
        self.log = log or logger
        self._enabled = brain_lesson_fn is not None

    # ── Public API ─────────────────────────────────────────

    def emit_opened(self, candidate: Dict, sizing: Dict, sleeve_status: str):
        """Emit MONTHLY_PICK_OPENED event."""
        ticker = candidate.get('ticker', '?')
        score = candidate.get('analysis_score', candidate.get('total_score', 0))

        self._emit_lesson(
            event=LearningEvent.MONTHLY_PICK_OPENED,
            description=(
                f'Monthly Pick aberto: {ticker} com score {score:.1f}, '
                f'preço entrada R${sizing.get("entry_price", 0):.2f}, '
                f'alvo R${sizing.get("target_price", 0):.2f}, '
                f'stop R${sizing.get("stop_price", 0):.2f}. '
                f'Capital alocado: R${sizing.get("capital_allocated", 0):,.0f}. '
                f'Sleeve status: {sleeve_status}.'
            ),
            data={
                'ticker': ticker,
                'score': score,
                'conviction': candidate.get('conviction'),
                'sector': candidate.get('sector'),
                'entry_price': sizing.get('entry_price'),
                'target_price': sizing.get('target_price'),
                'stop_price': sizing.get('stop_price'),
                'quantity': sizing.get('quantity'),
                'capital': sizing.get('capital_allocated'),
                'risk_reward': sizing.get('risk_reward_ratio'),
                'sleeve_status': sleeve_status,
            },
            impact=60,
            confidence=min(50 + score * 0.4, 90),
        )

    def emit_rejected(self, candidate: Dict, reason: str):
        """Emit MONTHLY_PICK_REJECTED event."""
        ticker = candidate.get('ticker', '?')
        score = candidate.get('analysis_score', candidate.get('total_score', 0))

        self._emit_lesson(
            event=LearningEvent.MONTHLY_PICK_REJECTED,
            description=(
                f'Monthly Pick rejeitado: {ticker} (score {score:.1f}). '
                f'Motivo: {reason}. '
                f'Aprender: por que esta ação foi filtrada e se a decisão foi correta.'
            ),
            data={
                'ticker': ticker,
                'score': score,
                'rejection_reason': reason,
                'sector': candidate.get('sector'),
            },
            impact=30,
            confidence=70,
        )

    def emit_closed(self, position: Dict, close_reason: str,
                    pnl_pct: float, pnl_value: float):
        """Emit appropriate close event based on reason."""
        ticker = position.get('ticker', '?')

        # Map close reason to specific learning event
        event_map = {
            ExitReason.TARGET_HIT.value: LearningEvent.MONTHLY_PICK_TARGET_HIT,
            ExitReason.STOP_LOSS.value: LearningEvent.MONTHLY_PICK_STOP_LOSS,
            ExitReason.TIMEOUT.value: LearningEvent.MONTHLY_PICK_TIMEOUT,
            ExitReason.THESIS_BROKEN.value: LearningEvent.MONTHLY_PICK_THESIS_BROKEN,
            ExitReason.SCORE_DROP.value: LearningEvent.MONTHLY_PICK_THESIS_BROKEN,
            ExitReason.SCORE_LOW.value: LearningEvent.MONTHLY_PICK_THESIS_BROKEN,
        }
        event = event_map.get(close_reason, LearningEvent.MONTHLY_PICK_CLOSED)

        entry_score = float(position.get('entry_score', 0))
        current_score = float(position.get('current_score', 0))
        weeks_held = int(position.get('weeks_held', 0))

        self._emit_lesson(
            event=event,
            description=(
                f'Monthly Pick encerrado: {ticker}. '
                f'Motivo: {close_reason}. '
                f'PnL: {pnl_pct:+.2f}% (R${pnl_value:+,.0f}). '
                f'Score entrada: {entry_score:.1f}, score saída: {current_score:.1f}. '
                f'Duração: {weeks_held} semanas. '
                f'Aprender: avaliar se a entrada foi boa, se o timing de saída '
                f'foi adequado, e como melhorar a seleção futura.'
            ),
            data={
                'ticker': ticker,
                'close_reason': close_reason,
                'pnl_pct': pnl_pct,
                'pnl_value': pnl_value,
                'entry_score': entry_score,
                'exit_score': current_score,
                'score_delta': round(current_score - entry_score, 2),
                'weeks_held': weeks_held,
                'sector': position.get('sector'),
                'entry_price': position.get('entry_price'),
                'close_price': position.get('close_price') or position.get('current_price'),
                'was_profitable': pnl_pct > 0,
            },
            impact=max(40, min(90, abs(pnl_pct) * 4)),
            confidence=min(60 + abs(pnl_pct) * 2, 95),
        )

    def emit_review(self, position: Dict, action: str,
                    review_confidence: float):
        """Emit review event (hold/reduce)."""
        ticker = position.get('ticker', '?')

        if action == 'reduce':
            event = LearningEvent.MONTHLY_PICK_REVIEW_REDUCE
        else:
            event = LearningEvent.MONTHLY_PICK_REVIEW_HOLD

        self._emit_lesson(
            event=event,
            description=(
                f'Monthly Pick review: {ticker} → {action}. '
                f'Confiança: {review_confidence:.0f}. '
                f'PnL atual: {position.get("pnl_pct", 0):+.2f}%.'
            ),
            data={
                'ticker': ticker,
                'action': action,
                'review_confidence': review_confidence,
                'pnl_pct': position.get('pnl_pct'),
                'current_score': position.get('current_score'),
                'weeks_held': position.get('weeks_held'),
            },
            impact=20,
            confidence=review_confidence,
        )

    # ── Internal ───────────────────────────────────────────

    def _emit_lesson(self, event: LearningEvent, description: str,
                     data: dict, impact: float = 50,
                     confidence: float = 70):
        """Send structured lesson to the brain."""
        if not self._enabled:
            self.log.debug(f'[MP Learning] (disabled) {event.value}: {description[:80]}')
            return

        try:
            self.brain_lesson_fn(
                module='monthly_picks',
                description=description,
                lesson_type=event.value,
                impact_score=round(impact, 1),
                confidence=round(confidence, 1),
                strategy='monthly_picks',
                data_json=data,
            )
            self.log.debug(f'[MP Learning] Emitted {event.value} for '
                           f'{data.get("ticker", "?")}')
        except Exception as e:
            self.log.warning(f'[MP Learning] Failed to emit {event.value}: {e}')
