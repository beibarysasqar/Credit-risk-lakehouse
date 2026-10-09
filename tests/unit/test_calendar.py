from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from credit_risk.common.config import ANCHOR_DATE
from credit_risk.transformations.calendar import to_calendar_date, to_calendar_month


def test_anchor_is_first_day_of_month() -> None:
    assert ANCHOR_DATE.day == 1


def test_to_calendar_date(spark: SparkSession) -> None:
    offsets = spark.createDataFrame(
        [(0.0,), (-1.0,), (-121.0,), (-0.5,), (31.0,), (None,)], "d DOUBLE"
    )

    result = offsets.select("d", to_calendar_date(F.col("d")).alias("date"))

    expected = spark.createDataFrame(
        [
            (0.0, date(2018, 5, 1)),
            (-1.0, date(2018, 4, 30)),
            # Crosses a year boundary.
            (-121.0, date(2017, 12, 31)),
            # Fractional offsets are floored, not truncated towards the anchor.
            (-0.5, date(2018, 4, 30)),
            (31.0, date(2018, 6, 1)),
            (None, None),
        ],
        "d DOUBLE, date DATE",
    )
    assertDataFrameEqual(result, expected)


def test_to_calendar_month(spark: SparkSession) -> None:
    offsets = spark.createDataFrame([(0,), (-1,), (-4,), (-5,), (-96,), (None,)], "m INT")

    result = offsets.select("m", to_calendar_month(F.col("m")).alias("month"))

    expected = spark.createDataFrame(
        [
            (0, date(2018, 5, 1)),
            (-1, date(2018, 4, 1)),
            (-4, date(2018, 1, 1)),
            (-5, date(2017, 12, 1)),
            (-96, date(2010, 5, 1)),
            (None, None),
        ],
        "m INT, month DATE",
    )
    assertDataFrameEqual(result, expected)
