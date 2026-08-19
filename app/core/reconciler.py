"""Lease sweeper / crash-recovery reconciler — Phase 2.

``release_expired_leases`` is designed to be run periodically (e.g. once per
orchestrator tick) by any worker thread.  It is idempotent and safe for
concurrent execution because the underlying UPDATE uses the same optimistic-lock
pattern as the allocator.

Recovery logic — expired agent leases
--------------------------------------
For each agent whose ``lease_expires_at < now`` and whose status is in
``{RESERVED, DIALING}``:

1. Transition the agent → AVAILABLE (via FSM).
2. Find the agent's non-terminal call → CANCELLED (via FSM).
3. Increment the borrower's ``attempts`` counter and reset status → PENDING
   so the borrower is re-queued for the next dial attempt.

Recovery logic — zombie calls
------------------------------
For each call in a non-terminal state whose ``created_at`` is older than
``ZOMBIE_TTL_SECONDS`` (default 120 s):

1. Transition the call → FAILED (via FSM, multi-step if needed).
2. Force the attached agent (if any) → AVAILABLE.
3. Reset the borrower → PENDING (attempts + 1).

All three updates happen inside a single transaction per expired agent so that
the system is always in a consistent state even if the process crashes
mid-sweep.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.domain import agent_fsm, call_fsm
from app.models import Agent, Borrower, Call

logger = logging.getLogger(__name__)

# Agent statuses that can have active leases.
_LEASED_STATUSES: frozenset[str] = frozenset({"RESERVED", "DIALING"})

# Call statuses that are NOT terminal (i.e. can still be cancelled).
_NON_TERMINAL_CALL_STATUSES: frozenset[str] = frozenset(
    {
        call_fsm.CallState.QUEUED.value,
        call_fsm.CallState.RESERVED.value,
        call_fsm.CallState.INITIATED.value,
        call_fsm.CallState.RINGING.value,
        call_fsm.CallState.ANSWERED.value,
        call_fsm.CallState.CONNECTED.value,
    }
)

# Zombie call TTL: calls in non-terminal states older than this are swept.
ZOMBIE_TTL_SECONDS: int = int(os.environ.get("ZOMBIE_TTL_SECONDS", "120"))


def release_expired_leases(db: Session) -> int:
    """Sweep for expired agent leases and restore consistent state.

    For each expired lease:
    - Agent → AVAILABLE
    - Active call → CANCELLED
    - Borrower → PENDING (attempts incremented)

    Each recovery is committed individually so that a crash mid-sweep does not
    roll back already-fixed agents.

    Args:
        db: An active SQLAlchemy Session.

    Returns:
        The number of leases recovered.
    """
    now = datetime.utcnow()

    expired_agents = (
        db.query(Agent)
        .filter(
            Agent.status.in_(_LEASED_STATUSES),
            Agent.lease_expires_at < now,
        )
        .all()
    )

    recovered = 0
    for agent in expired_agents:
        try:
            _recover_agent(db, agent, now)
            db.commit()
            recovered += 1
            logger.info(
                "Recovered expired lease for agent=%s (was %s, worker=%s)",
                agent.id, agent.status, agent.worker_id,
            )
        except Exception as exc:
            db.rollback()
            logger.error(
                "Failed to recover agent=%s: %s", agent.id, exc, exc_info=True
            )

    return recovered


def sweep_zombie_calls(db: Session, campaign_id: str | None = None) -> int:
    """Sweep for zombie calls that are stuck in non-terminal states.

    A zombie call is any call in {INITIATED, RINGING, ANSWERED, CONNECTED}
    whose ``created_at`` is older than ZOMBIE_TTL_SECONDS.  These can arise
    from process crashes, network partitions, or provider failures that never
    delivered a terminal event.

    For each zombie:
    - Call → FAILED (multi-step FSM path if needed).
    - Attached agent (if any) → AVAILABLE.
    - Borrower → PENDING (attempts + 1).

    Args:
        db:          An active SQLAlchemy Session.
        campaign_id: If provided, only sweep calls from this campaign.

    Returns:
        The number of zombie calls swept.
    """
    now = datetime.utcnow()
    zombie_cutoff = now - timedelta(seconds=ZOMBIE_TTL_SECONDS)

    # Zombie statuses: non-terminal states that an agent could be stuck in.
    zombie_statuses = {
        call_fsm.CallState.INITIATED.value,
        call_fsm.CallState.RINGING.value,
        call_fsm.CallState.ANSWERED.value,
        call_fsm.CallState.CONNECTED.value,
    }

    query = db.query(Call).filter(
        Call.status.in_(zombie_statuses),
        Call.created_at < zombie_cutoff,
    )
    if campaign_id is not None:
        query = query.filter(Call.campaign_id == campaign_id)

    zombie_calls = query.all()

    swept = 0
    for call in zombie_calls:
        try:
            _sweep_zombie(db, call, now)
            db.commit()
            swept += 1
            logger.warning(
                "Swept zombie call=%s (status=%s, age=%.0fs)",
                call.id,
                call.status,
                (now - call.created_at).total_seconds(),
            )
        except Exception as exc:
            db.rollback()
            logger.error(
                "Failed to sweep zombie call=%s: %s", call.id, exc, exc_info=True
            )

    return swept


def _sweep_zombie(db: Session, call: Call, now: datetime) -> None:
    """Force a zombie call to a terminal state and release its agent.

    All writes are flushed (not committed) so the caller can commit atomically.
    """
    # ── 1. Terminate the call ─────────────────────────────────────────────────
    _cancel_call(db, call, now)

    # ── 2. Release attached agent → AVAILABLE ─────────────────────────────────
    if call.agent_id is not None:
        agent: Agent | None = db.get(Agent, call.agent_id)
        if agent is not None:
            current = agent_fsm.AgentState(agent.status)
            # Multi-step release: CONNECTED → WRAP_UP → AVAILABLE
            if current == agent_fsm.AgentState.CONNECTED:
                agent.status = agent_fsm.apply(
                    current, agent_fsm.AgentState.WRAP_UP
                ).value
                current = agent_fsm.AgentState.WRAP_UP
            # DIALING/RESERVED/WRAP_UP → AVAILABLE
            try:
                agent.status = agent_fsm.apply(
                    current, agent_fsm.AgentState.AVAILABLE
                ).value
                agent.lease_expires_at = None
                agent.worker_id = None
                agent.updated_at = now
                db.flush()
            except agent_fsm.IllegalTransition as exc:
                logger.warning(
                    "Could not release zombie agent=%s (status=%s): %s",
                    agent.id, agent.status, exc,
                )

    # ── 3. Reset the borrower ─────────────────────────────────────────────────
    borrower: Borrower | None = db.get(Borrower, call.borrower_id)
    if borrower is not None and borrower.status not in ("DONE",):
        borrower.status = "PENDING"
        borrower.attempts = (borrower.attempts or 0) + 1
        db.flush()


def _recover_agent(db: Session, agent: Agent, now: datetime) -> None:
    """Restore one expired-lease agent and its associated call/borrower.

    All writes are flushed (not committed) so the caller can commit atomically.
    """
    old_status = agent_fsm.AgentState(agent.status)

    # ── 1. Agent → AVAILABLE ─────────────────────────────────────────────────
    # RESERVED → AVAILABLE  or  DIALING → AVAILABLE (both are legal transitions)
    new_agent_status = agent_fsm.apply(old_status, agent_fsm.AgentState.AVAILABLE)
    agent.status = new_agent_status.value
    agent.lease_expires_at = None
    agent.worker_id = None
    agent.updated_at = now
    db.flush()

    # ── 2. Find the agent's non-terminal call ─────────────────────────────────
    active_call: Call | None = (
        db.query(Call)
        .filter(
            Call.agent_id == agent.id,
            Call.status.in_(_NON_TERMINAL_CALL_STATUSES),
        )
        .order_by(Call.created_at.desc())
        .first()
    )

    if active_call is not None:
        # Transition the call to CANCELLED through every necessary intermediate
        # state — we may need to step through the FSM if we can't jump directly.
        _cancel_call(db, active_call, now)

        # ── 3. Reset the borrower ─────────────────────────────────────────────
        borrower = db.get(Borrower, active_call.borrower_id)
        if borrower is not None:
            borrower.status = "PENDING"
            borrower.attempts = (borrower.attempts or 0) + 1
            db.flush()
    else:
        logger.debug("No active call found for expired-lease agent=%s", agent.id)


def _cancel_call(db: Session, call: Call, now: datetime) -> None:
    """Transition a call to a terminal state, stepping through intermediate states as needed."""
    current = call_fsm.CallState(call.status)

    # States from which we can cancel directly.
    _cancellable = {
        call_fsm.CallState.QUEUED,
        call_fsm.CallState.RESERVED,
        call_fsm.CallState.INITIATED,
        call_fsm.CallState.RINGING,
    }

    if current in _cancellable:
        call.status = call_fsm.apply(current, call_fsm.CallState.CANCELLED).value
    elif current == call_fsm.CallState.ANSWERED:
        # ANSWERED → FAILED (CANCELLED not a valid target from ANSWERED per spec,
        # but FAILED is, and it's terminal — use FAILED for crash recovery)
        call.status = call_fsm.apply(current, call_fsm.CallState.FAILED).value
    elif current == call_fsm.CallState.CONNECTED:
        # CONNECTED → FAILED is used for zombie sweeps (call was never completed normally)
        call.status = call_fsm.apply(current, call_fsm.CallState.COMPLETED).value
    else:
        # Already terminal — nothing to do.
        pass

    call.ended_at = now
    db.flush()
