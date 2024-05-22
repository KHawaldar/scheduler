"""Date utils Module.
This module contains some utility functions that can help other components
of the service.
"""
from datetime import datetime


class DateUtils:
    """DateUtils."""

    @staticmethod
    def get_current_time() -> datetime:
        """It gives the datetime .
        Args:
        Returns:
          datetime
        """
        return datetime.today().now()

    @staticmethod
    def get_datetime_in_iso_format() -> str:
        """It gives the datetime in iso format.
        Args:
        Returns:
            str: given date in iso format
        """
        return datetime.today().isoformat()