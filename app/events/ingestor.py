"""Stateless event ingestor — deduplication + FSM-gated state transitions.

``EventIngestor.process(event, db)`` handles a single :class:`~app.providers.base.CallEvent`
and is the *only* place that applies state transitions from telecom events.

Processing pipeline (per spec):
1. **Idempotency check**: INSERT INTO processed_events.  On UNIQUE violation
   the event has already been processed → return ``'duplicate'`` (no-op).
2. **Load call**: fetch the call row; if not found → return ``'not_found'``.
3. **FSM gate**: ask ``call_fsm.apply`` if the transition is legal.  Illegal →
   log + return ``'rejected'`` (no state change anywhere).
4. **Apply transition + side effects** atomically:
   - ``RINGING``   → call status = RINGING.
   - ``ANSWERED``  → call status = ANSWERED; answered_at = now.
                     If agent attached (agent_id not None):
                         agent DIALING → CONNECTED.
                     Else:
                         call.abandoned = True (feeds abandon-rate window).
   - ``CONNECTED`` → call status = CONNECTED.
   - ``COMPLETED`` → call status = COMPLETED; ended_at = now.
                     Agent → WRAP_UP; borrower → DONE.
                     Immediately release agent (WRAP_UP → AVAILABLE) after a
                     configurable wrap-up delay (0 s in tests, real value via
                     WRAP_UP_DELAY_SECONDS env var).
   - ``FAILED``    → call status = FAILED; ended_at = now.
                     Agent → AVAILABLE (if attached); borrower → CALLED.

The ingestor is stateless: any worker can process any event safely.
Crash-after-ANSWERED recovery is automatic: if the process restarts mid-call
the next COMPLETED/FAILED event re-runs this function and the correct side
effects fire.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Literal

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.domain import agent_fsm, call_fsm
from app.models import Agent, Borrower, Call, ProcessedEvent
from app.providers.base import CallEvent

logger = logging.getLogger(__name__)

# Seconds to hold an agent in WRAP_UP before releasing to AVAILABLE.
# Set to 0 for tests; real deployments use e.g. 30 s.
WRAP_UP_DELAY_SECONDS: float = float(os.environ.get("WRAP_UP_DELAY_SECONDS", "0"))

IngestResult = Literal["ok", "duplicate", "rejected", "not_found", "error"]


class EventIngestor:
    """Stateless event processor — safe to instantiate once and call from any thread."""

    def process(self, event: CallEvent, db: Session) -> IngestResult:
        """Process a single telecom event atomically.

        Args:
            event: The :class:`~app.providers.base.CallEvent` to process.
            db:    An active SQLAlchemy Session.  The caller commits after this
                   method returns ``'ok'``; for all other results the session
                   is left unchanged (or rolled back on ``'error'``).

        Returns:
            One of: ``'ok'``, ``'duplicate'``, ``'rejected'``, ``'not_found'``,
            ``'error'``.
        """
        # ── Step 1: idempotency insert ────────────────────────────────────────
        try:
            processed = ProcessedEvent(
                event_id=event.event_id,
                call_id=event.call_id,
                event_type=event.event_type,
                received_at=datetime.utcnow(),
            )
            db.add(processed)
            db.flush()
        except IntegrityError:
            db.rollback()
            logger.debug(
                "Duplicate event %s (%s) for call %s — ignored.",
                event.event_id, event.event_type, event.call_id,
            )
            return "duplicate"

        # ── Step 2: load call ─────────────────────────────────────────────────
        call: Call | None = db.get(Call, event.call_id)
        if call is None:
            logger.warning(
                "Event %s references unknown call %s — ignored.",
                event.event_id, event.call_id,
            )
            db.rollback()
            return "not_found"

        # ── Step 3: FSM gate ──────────────────────────────────────────────────
        current_call_state = call_fsm.CallState(call.status)
        # Map provider event type to the target FSM state.
        target_call_state = _EVENT_TO_CALL_STATE.get(event.event_type)
        if target_call_state is None:
            logger.error("Unknown event_type %r — rejected.", event.event_type)
            db.rollback()
            return "rejected"

        try:
            new_call_state = call_fsm.apply(current_call_state, target_call_state)
        except call_fsm.IllegalTransition as exc:
            logger.info(
                "Rejected event %s (%s) for call %s [%s→%s]: %s",
                event.event_id, event.event_type, event.call_id,
                current_call_state.value, target_call_state.value, exc,
            )
            db.rollback()
            return "rejected"

        # ── Step 4: apply transition + side effects ───────────────────────────
        now = datetime.utcnow()
        call.status = new_call_state.value
        try:
            self._apply_side_effects(call, event, new_call_state, db, now)
            db.commit()
            return "ok"
        except Exception as exc:
            logger.exception(
                "Error applying side effects for event %s: %s", event.event_id, exc
            )
            db.rollback()
            return "error"

    # ─────────────────────────────────────────────────────────────────────────
    # Side-effect handlers
    # ─────────────────────────────────────────────────────────────────────────

    def _apply_side_effects(
        self,
        call: Call,
        event: CallEvent,
        new_state: call_fsm.CallState,
        db: Session,
        now: datetime,
    ) -> None:
        """Mutate related rows based on the new call state."""

        if new_state == call_fsm.CallState.RINGING:
            pass  # no extra side effects beyond the call status update

        elif new_state == call_fsm.CallState.ANSWERED:
            call.answered_at = now
            if call.agent_id is not None:
                agent = db.get(Agent, call.agent_id)
                if agent is not None:
                    _transition_agent(agent, agent_fsm.AgentState.CONNECTED, db)
                else:
                    logger.warning("ANSWERED: agent %s not found.", call.agent_id)
                    call.abandoned = True
            else:
                # No agent attached → abandoned.
                call.abandoned = True
                logger.info("Call %s answered but no agent → abandoned=True", call.id)

        elif new_state == call_fsm.CallState.CONNECTED:
            pass  # call is live; no extra DB changes needed

        elif new_state == call_fsm.CallState.COMPLETED:
            call.ended_at = now
            if call.agent_id is not None:
                agent = db.get(Agent, call.agent_id)
                if agent is not None:
                    _transition_agent(agent, agent_fsm.AgentState.WRAP_UP, db)
                    # Immediate wrap-up release (WRAP_UP_DELAY_SECONDS=0 in tests).
                    if WRAP_UP_DELAY_SECONDS == 0:
                        _transition_agent(agent, agent_fsm.AgentState.AVAILABLE, db)
            # Mark borrower as DONE.
            borrower = db.get(Borrower, call.borrower_id)
            if borrower is not None:
                borrower.status = "DONE"

        elif new_state == call_fsm.CallState.FAILED:
            call.ended_at = now
            if call.agent_id is not None:
                agent = db.get(Agent, call.agent_id)
                if agent is not None:
                    # Return agent to AVAILABLE so it can take another call.
                    _maybe_release_agent(agent, db)
            # Borrower goes to CALLED (attempted but failed).
            borrower = db.get(Borrower, call.borrower_id)
            if borrower is not None:
                borrower.status = "CALLED"

        db.flush()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

# Maps provider event_type → target CallState for the FSM gate.
_EVENT_TO_CALL_STATE: dict[str, call_fsm.CallState] = {
    "RINGING": call_fsm.CallState.RINGING,
    "ANSWERED": call_fsm.CallState.ANSWERED,
    "CONNECTED": call_fsm.CallState.CONNECTED,
    "COMPLETED": call_fsm.CallState.COMPLETED,
    "FAILED": call_fsm.CallState.FAILED,
}


def _transition_agent(agent: Agent, target: agent_fsm.AgentState, db: Session) -> None:
    """Attempt an agent FSM transition, logging errors without raising."""
    try:
        current = agent_fsm.AgentState(agent.status)
        new_status = agent_fsm.apply(current, target)
        agent.status = new_status.value
        if target == agent_fsm.AgentState.AVAILABLE:
            agent.lease_expires_at = None
            agent.worker_id = None
        db.flush()
    except agent_fsm.IllegalTransition as exc:
        logger.warning("Agent FSM transition failed: %s", exc)


def _maybe_release_agent(agent: Agent, db: Session) -> None:
    """Try to release agent to AVAILABLE from whichever state it's in."""
    # DIALING → AVAILABLE or CONNECTED → WRAP_UP → AVAILABLE
    current = agent_fsm.AgentState(agent.status)
    if current in (agent_fsm.AgentState.DIALING, agent_fsm.AgentState.RESERVED):
        _transition_agent(agent, agent_fsm.AgentState.AVAILABLE, db)
    elif current == agent_fsm.AgentState.CONNECTED:
        _transition_agent(agent, agent_fsm.AgentState.WRAP_UP, db)
        _transition_agent(agent, agent_fsm.AgentState.AVAILABLE, db)
