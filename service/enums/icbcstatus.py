"""ICBC service Status."""

from enum import Enum


class IcbcStatus(Enum):
    """Icbc status attributes."""
    ACTIVE: str = "ACTIVE"
    SILENT: str = "SILENT"