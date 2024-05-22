"""Error messages.
This class define list of error messages.
"""


class ErrorMessage:
    """Error messages."""
    ICBC_CALCULATION_ERROR1 = "icbc_calculation_kpis is null"
    TOTAL_RECALL_AUDITOR_FAILS_COUNT_ERROR1 = "total recall or auditor_fails_count is None"
    TOTAL_RECALL_AUDITOR_FAILS_COUNT_ERROR2 = "total recall or auditor_fails_count not calculated"
    INPUT_MISSING_TOTAL_RECALL_OR_AUDITOR_FAILS_CNT_VALUE_ERROR1 = \
        "some inputs are missing, it may be total_recall/auditor_fails_count"
    KEY_ERROR1 = "all keys are not populated, something wrong in query"