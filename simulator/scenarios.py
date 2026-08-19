"""Simulator scenario definitions — Phase 5.

Configurations for A/B/C/D scenarios requested in the spec.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Scenario:
    """A specific test configuration for the simulator."""
    name: str
    answer_rate: float
    talk_time_mean: float
    description: str


SCENARIOS = {
    "A": Scenario(
        name="A",
        answer_rate=0.20,
        talk_time_mean=120.0,
        description="Low answer rate (20%), long talk time (120s)",
    ),
    "B": Scenario(
        name="B",
        answer_rate=0.50,
        talk_time_mean=90.0,
        description="Medium answer rate (50%), medium talk time (90s)",
    ),
    "C": Scenario(
        name="C",
        answer_rate=0.70,
        talk_time_mean=180.0,
        description="High answer rate (70%), very long talk time (180s)",
    ),
    "D": Scenario(
        name="D",
        answer_rate=0.70,  # starts at 70%, drifts down to 10% mid-run
        talk_time_mean=90.0,
        description="Drifting: answer rate drops from 70% to 10% mid-run",
    ),
}
