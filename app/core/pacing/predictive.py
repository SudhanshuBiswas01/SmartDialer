"""Predictive pacing strategy — Phase 3.

Uses historical answer-rate and talk-time EWMAs to predict how many calls to
dial *now* so that an agent becomes free just as each borrower answers.

Formula (from spec)
--------------------
    dials = max(0, floor((A + F_soon) * k / max(p_hat, 0.05)) - R)

Where:
    A       = available_agents
    F_soon  = agents expected to complete their call before the next answer
    k       = aggressiveness coefficient in [0.1, 1.0]; AIMD-managed
    p_hat   = EWMA answer rate (floor 0.05 prevents division-by-near-zero)
    R       = currently ringing calls

AIMD tuning (managed by SafetyController, state stored on this object):
    - On any abandon event : k ← max(0.1, k / 2)    (multiplicative decrease)
    - On each clean tick   : k ← min(1.0, k + 0.05) (additive increase)

The SafetyController reads and writes ``self.k`` and passes the updated value
into the Snapshot so the audit log always reflects the actual k used.

Design constraint: no imports from app.core.allocator.
"""

from __future__ import annotations

import math

from app.core.pacing.base import PacingStrategy, Snapshot

# Bounds for the aggressiveness coefficient.
K_MIN: float = 0.1
K_MAX: float = 1.0

# EWMA alpha for answer-rate and talk-time smoothing (higher = faster response).
EWMA_ALPHA: float = 0.3

# Minimum answer rate to use in the denominator (prevents division by ~0).
P_HAT_FLOOR: float = 0.05


class PredictiveStrategy:
    """Predictive pacing using EWMA answer-rate and AIMD aggressiveness.

    Implements :class:`~app.core.pacing.base.PacingStrategy`.

    State:
        k: Aggressiveness coefficient, updated by SafetyController on each tick.
    """

    def __init__(self, initial_k: float = 1.0) -> None:
        """Initialise with a starting aggressiveness coefficient.

        Args:
            initial_k: Starting k value in [K_MIN, K_MAX].
        """
        self.k: float = max(K_MIN, min(K_MAX, initial_k))

    def propose(self, snapshot: Snapshot) -> int:
        """Calculate the predicted number of dials for this tick.

        The snapshot's ``k`` field is used (it is set by the SafetyController
        before calling propose, so the audit trail always logs the actual k).

        Args:
            snapshot: Current system snapshot with k, ewma_answer_rate, etc.

        Returns:
            Proposed number of dials (≥ 0).
        """
        p_hat = max(snapshot.ewma_answer_rate, P_HAT_FLOOR)
        k = max(K_MIN, min(K_MAX, snapshot.k))

        numerator = (snapshot.available_agents + snapshot.frees_soon) * k
        dials = math.floor(numerator / p_hat) - snapshot.ringing_calls
        return max(0, dials)

    # ─────────────────────────────────────────────────────────────────────────
    # AIMD helpers (called by SafetyController)
    # ─────────────────────────────────────────────────────────────────────────

    def on_abandon(self) -> float:
        """Halve k on any abandon event (multiplicative decrease).

        Returns:
            New k value after adjustment.
        """
        self.k = max(K_MIN, self.k / 2.0)
        return self.k

    def on_clean_tick(self) -> float:
        """Increment k by 0.05 on a clean tick (additive increase).

        Returns:
            New k value after adjustment.
        """
        self.k = min(K_MAX, self.k + 0.05)
        return self.k
