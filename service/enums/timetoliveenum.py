"""List of time to live attributes."""

from enum import Enum


class TimeToLive(Enum):
    """Times to live attributes."""
    ONE_DAY_TTL: int = 24 * 3600
    SEVEN_DAY_TTL: int = 7 * 24 * 3600
    ONE_YEAR_TTL: int = 366 * 24 * 3600