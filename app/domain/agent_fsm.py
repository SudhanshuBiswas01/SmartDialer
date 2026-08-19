"""Agent Finite State Machine for SmartDialer.

Defines the AgentState enumeration and the pure transition function that
enforces only legal state changes.  No I/O, no side-effects — this module
is intentionally stateless so it can be unit-tested without a DB.
"""

from __future__ import annotations

from enum import Enum


class AgentState(str, Enum):
    """All possible lifecycle states for a call-center agent."""

    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"


class IllegalTransition(ValueError):
    """Raised when a requested state transition is not in LEGAL_TRANSITIONS."""

    def __init__(self, current: AgentState, target: AgentState) -> None:
        super().__init__(
            f"Illegal agent transition: {current.value!r} → {target.value!r}"
        )
        self.current = current
        self.target = target


# All (from_state, to_state) pairs that are explicitly permitted.
# Every other combination is illegal and will raise IllegalTransition.
LEGAL_TRANSITIONS: frozenset[tuple[AgentState, AgentState]] = frozenset(
    {
        # Agent logs in
        (AgentState.OFFLINE, AgentState.AVAILABLE),
        # Agent goes idle from available (e.g. supervisor-initiated pause / logout)
        (AgentState.AVAILABLE, AgentState.RESERVED),
        (AgentState.AVAILABLE, AgentState.PAUSED),
        (AgentState.AVAILABLE, AgentState.OFFLINE),
        # Allocator reserves the agent, then initiates the dial
        (AgentState.RESERVED, AgentState.DIALING),
        # Lease expired / dial abandoned before initiation → release
        (AgentState.RESERVED, AgentState.AVAILABLE),
        # Call initiation succeeded → wait for answer; or failed → release
        (AgentState.DIALING, AgentState.CONNECTED),
        (AgentState.DIALING, AgentState.AVAILABLE),
        # Call answered → agent wraps up after it ends
        (AgentState.CONNECTED, AgentState.WRAP_UP),
        # Wrap-up complete
        (AgentState.WRAP_UP, AgentState.AVAILABLE),
        (AgentState.WRAP_UP, AgentState.PAUSED),
        (AgentState.WRAP_UP, AgentState.OFFLINE),
        # Supervisor-controlled pause/resume/logout
        (AgentState.PAUSED, AgentState.AVAILABLE),
        (AgentState.PAUSED, AgentState.OFFLINE),
    }
)


def apply(current: AgentState, target: AgentState) -> AgentState:
    """Return *target* if the transition from *current* is legal.

    Args:
        current: The agent's present state.
        target:  The desired next state.

    Returns:
        ``target`` — the new agent state.

    Raises:
        IllegalTransition: If ``(current, target)`` is not in
            :data:`LEGAL_TRANSITIONS`.
    """
    if (current, target) not in LEGAL_TRANSITIONS:
        raise IllegalTransition(current, target)
    return target
