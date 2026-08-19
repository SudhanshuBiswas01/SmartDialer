"""Call Finite State Machine for SmartDialer.

Defines the CallState enumeration and the pure transition function that
enforces only legal state changes.  Terminal states (COMPLETED, FAILED,
CANCELLED) are *absorbing* — any further transition attempt raises
IllegalTransition, so events arriving after a call ends are always rejected
cleanly.

No I/O, no side-effects — intentionally stateless for easy unit-testing.
"""

from __future__ import annotations

from enum import Enum


class CallState(str, Enum):
    """All possible lifecycle states for a call record."""

    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    # ── Terminal / absorbing states ──────────────────────────────────────────
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


# States from which NO further transition is possible.
TERMINAL_STATES: frozenset[CallState] = frozenset(
    {CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED}
)


class IllegalTransition(ValueError):
    """Raised when a requested state transition is not in LEGAL_TRANSITIONS."""

    def __init__(self, current: CallState, target: CallState) -> None:
        super().__init__(
            f"Illegal call transition: {current.value!r} → {target.value!r}"
        )
        self.current = current
        self.target = target


# All (from_state, to_state) pairs that are explicitly permitted.
LEGAL_TRANSITIONS: frozenset[tuple[CallState, CallState]] = frozenset(
    {
        # Allocation path
        (CallState.QUEUED, CallState.RESERVED),
        (CallState.QUEUED, CallState.CANCELLED),
        # Dial initiation
        (CallState.RESERVED, CallState.INITIATED),
        (CallState.RESERVED, CallState.CANCELLED),
        # Provider handshake
        (CallState.INITIATED, CallState.RINGING),
        (CallState.INITIATED, CallState.FAILED),
        (CallState.INITIATED, CallState.CANCELLED),
        # Ring phase
        (CallState.RINGING, CallState.ANSWERED),
        (CallState.RINGING, CallState.FAILED),
        (CallState.RINGING, CallState.CANCELLED),
        # Answer phase — borrower picked up; agent must be attached
        # If no agent available, call is marked abandoned (set on the ORM row,
        # not via a separate state — abandoned is a bool flag on the call row).
        (CallState.ANSWERED, CallState.CONNECTED),
        (CallState.ANSWERED, CallState.COMPLETED),
        (CallState.ANSWERED, CallState.FAILED),
        # Active call wraps up
        (CallState.CONNECTED, CallState.COMPLETED),
        # Terminal states are absorbing — no transitions out of them.
        # (Omitting them from LEGAL_TRANSITIONS achieves this automatically.)
    }
)


def apply(current: CallState, target: CallState) -> CallState:
    """Return *target* if the transition from *current* is legal.

    Terminal states (COMPLETED, FAILED, CANCELLED) are absorbing — they will
    always raise :class:`IllegalTransition` regardless of ``target``.

    Args:
        current: The call's present state.
        target:  The desired next state.

    Returns:
        ``target`` — the new call state.

    Raises:
        IllegalTransition: If ``(current, target)`` is not in
            :data:`LEGAL_TRANSITIONS`, including when ``current`` is a
            terminal state.
    """
    if (current, target) not in LEGAL_TRANSITIONS:
        raise IllegalTransition(current, target)
    return target
