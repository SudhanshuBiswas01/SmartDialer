"""Progressive pacing strategy — Phase 3.

The simplest, most conservative mode: never dial more calls than there are
AVAILABLE agents.  This guarantees an abandon rate of 0% because every
answered call has an agent ready to handle it.

This mode is:
- Used as the initial/safe fallback mode.
- Forced by the SafetyController when the rolling abandon rate exceeds 3%.
- Always correct even under extreme load or model uncertainty.

Design constraint: no imports from app.core.allocator.
"""

from __future__ import annotations

from app.core.pacing.base import PacingStrategy, Snapshot


class ProgressiveStrategy:
    """Strict 1:1 pacing — propose at most ``available_agents`` dials.

    Implements :class:`~app.core.pacing.base.PacingStrategy`.
    """

    def propose(self, snapshot: Snapshot) -> int:
        """Return ``snapshot.available_agents`` — one potential dial per free agent.

        Args:
            snapshot: Current system snapshot.

        Returns:
            Number of available agents (≥ 0).
        """
        return max(0, snapshot.available_agents)
