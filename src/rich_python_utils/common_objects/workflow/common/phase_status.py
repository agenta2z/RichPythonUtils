"""PhaseStatus — lifecycle status for workflow phases and SOP states.

StrEnum so values are strings: PhaseStatus.IDLE == "idle" is True.
All existing string comparisons (if status == "running") continue to
work unchanged — drop-in replacement for raw status strings.
"""

from enum import StrEnum


class PhaseStatus(StrEnum):
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    ERROR = "error"
    PAUSED = "paused"
