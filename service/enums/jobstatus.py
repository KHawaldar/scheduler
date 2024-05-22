"""Job Status."""

from enum import Enum


class JobStatus(Enum):
    """Job status attributes."""
    JOB_COMPLETED: str = "JOB_COMPLETED"
    JOB_IN_PROGRESS: str = "JOB_IN_PROGRESS"
    JOB_ABORTED: str = "JOB_ABORTED"