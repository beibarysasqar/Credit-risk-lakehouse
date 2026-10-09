"""Synthetic calendar mapping of the Home Credit day and month offsets.

The source has no calendar dates. Every derived date goes through these two helpers, which
anchor all offsets on ``ANCHOR_DATE``; the resulting dates are synthetic.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

from credit_risk.common.config import ANCHOR_DATE


def to_calendar_date(days: Column) -> Column:
    """Map a ``DAYS_*`` offset (days relative to the application date) to a synthetic date.

    Fractional offsets are floored; a null offset gives a null date.
    """
    return F.date_add(F.lit(ANCHOR_DATE), F.floor(days).cast("int"))


def to_calendar_month(months: Column) -> Column:
    """Map a ``MONTHS_BALANCE`` offset to the first day of the synthetic calendar month."""
    return F.trunc(F.add_months(F.lit(ANCHOR_DATE), months.cast("int")), "month")
