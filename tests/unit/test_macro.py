from collections.abc import Callable
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.testing import assertDataFrameEqual

from credit_risk.common.expectations import SILVER_RULES
from credit_risk.transformations.macro import build_macro_month, clean_macro_observations
from credit_risk.transformations.quality import quarantine_rows

Table = Callable[[str], DataFrame]

BRONZE_SCHEMA = (
    "KEY STRING, FREQ STRING, TIME_PERIOD STRING, OBS_VALUE DOUBLE, OBS_STATUS STRING, "
    "_rescued_data STRING, _ingested_at TIMESTAMP, _source_file STRING"
)
OBSERVATION_SCHEMA = "series STRING, period_month DATE, obs_value DOUBLE"


def month(year: int, number: int) -> date:
    return date(year, number, 1)


def test_clean_macro_observations(spark: SparkSession, prepared: Table) -> None:
    result = prepared("macro_observation")

    assert result.columns[:3] == ["series", "period_month", "obs_value"]
    assert result.count() == 21
    expected = spark.createDataFrame(
        [
            ("euribor_3m", month(2018, 5), -0.3252273, "FM.M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA"),
            ("hicp_yoy", month(2018, 5), 2.0, "ICP.M.U2.N.000000.4.ANR"),
            ("unemployment_rate", month(2018, 5), 8.3, "LFSI.M.I9.S.UNEHRT.TOTAL0.15_74.T"),
        ],
        "series STRING, period_month DATE, obs_value DOUBLE, series_key STRING",
    )
    assertDataFrameEqual(
        result.filter("time_period = '2018-05'").select(*expected.columns), expected
    )
    assert quarantine_rows(result, SILVER_RULES["macro_observation"].drop).count() == 0


def test_clean_macro_observations_keeps_the_latest_revision(spark: SparkSession) -> None:
    first, refetch = datetime(2026, 1, 1), datetime(2026, 2, 1)
    rows = [
        ("K", "M", "2018-04", 1.2, "A", None, first, "/ecb/hicp_yoy/hicp_yoy_20260101.csv"),
        ("K", "M", "2018-05", 1.9, "P", None, first, "/ecb/hicp_yoy/hicp_yoy_20260101.csv"),
        # Revised by the ECB and fetched again.
        ("K", "M", "2018-05", 2.0, "A", None, refetch, "/ecb/hicp_yoy/hicp_yoy_20260201.csv"),
    ]

    result = clean_macro_observations({"hicp_yoy": spark.createDataFrame(rows, BRONZE_SCHEMA)})

    expected = spark.createDataFrame(
        [("hicp_yoy", month(2018, 4), 1.2, "A"), ("hicp_yoy", month(2018, 5), 2.0, "A")],
        f"{OBSERVATION_SCHEMA}, obs_status STRING",
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)


def test_unparseable_observations_are_quarantined(spark: SparkSession) -> None:
    at, file = datetime(2026, 1, 1), "/ecb/hicp_yoy/hicp_yoy_20260101.csv"
    rows = [
        ("K", "M", "2018-05", 2.0, "A", None, at, file),
        # Not a monthly period, a daily period, a missing value.
        ("K", "Q", "2018-Q1", 1.0, "A", None, at, file),
        ("K", "D", "2018-05-15", 1.0, "A", None, at, file),
        ("K", "M", "2018-06", None, "L", None, at, file),
    ]

    result = clean_macro_observations({"hicp_yoy": spark.createDataFrame(rows, BRONZE_SCHEMA)})
    quarantine = quarantine_rows(result, SILVER_RULES["macro_observation"].drop)

    assert {row.time_period: row._failed_rules for row in quarantine.collect()} == {
        "2018-Q1": ["period_month_parsed"],
        "2018-05-15": ["period_month_parsed"],
        "2018-06": ["obs_value_present"],
    }


def test_build_macro_month_from_fixtures(spark: SparkSession, derived: Table) -> None:
    result = derived("macro_month")

    assert result.columns == [
        "month",
        "macro_hicp_yoy",
        "macro_unemployment_rate",
        "macro_euribor_3m",
    ]
    # November 2017 (first Euribor) to July 2018 (last unemployment rate, lag 2).
    assert result.count() == 9
    expected = spark.createDataFrame(
        [
            (month(2017, 11), None, None, -0.329),
            # January sees HICP of December and the unemployment rate of November.
            (month(2018, 1), 1.3, 8.8, -0.3284545),
            (month(2018, 5), 1.2, 8.5, -0.3252273),
            # Nothing newer is published: the last known values are carried forward.
            (month(2018, 7), 2.0, 8.3, -0.3252273),
        ],
        "month DATE, macro_hicp_yoy DOUBLE, macro_unemployment_rate DOUBLE, "
        "macro_euribor_3m DOUBLE",
    )
    months = [row.month for row in expected.collect()]
    assertDataFrameEqual(result.filter(result.month.isin(months)), expected)


def test_macro_month_respects_the_publication_lag(spark: SparkSession) -> None:
    """Look-ahead guard: an observation is invisible before reference month + lag."""
    observations = spark.createDataFrame(
        [("a", month(2018, 3), 1.0), ("a", month(2018, 4), 2.0), ("b", month(2018, 4), 9.0)],
        OBSERVATION_SCHEMA,
    )

    result = build_macro_month(observations, {"a": 1, "b": 0})

    expected = spark.createDataFrame(
        [
            # "b" (lag 0) is known in its own month, "a" of April only in May.
            (month(2018, 4), 1.0, 9.0),
            (month(2018, 5), 2.0, 9.0),
        ],
        "month DATE, macro_a DOUBLE, macro_b DOUBLE",
    )
    assertDataFrameEqual(result, expected)


def test_macro_month_fills_gaps_and_ignores_unpinned_series(spark: SparkSession) -> None:
    observations = spark.createDataFrame(
        [
            ("a", month(2017, 12), 1.0),
            # No observation for January and February.
            ("a", month(2018, 3), 4.0),
            ("other", month(2018, 1), 99.0),
        ],
        OBSERVATION_SCHEMA,
    )

    result = build_macro_month(observations, {"a": 0})

    expected = spark.createDataFrame(
        [
            (month(2017, 12), 1.0),
            (month(2018, 1), 1.0),
            (month(2018, 2), 1.0),
            (month(2018, 3), 4.0),
        ],
        "month DATE, macro_a DOUBLE",
    )
    assertDataFrameEqual(result, expected)


def test_macro_month_has_no_look_ahead(spark: SparkSession) -> None:
    """Adding later observations must not change the months already published."""
    past = [("a", month(2018, 1), 1.0), ("a", month(2018, 2), 2.0)]
    future = [("a", month(2018, 3), 30.0), ("a", month(2018, 4), 40.0)]

    def history(rows: list[tuple]) -> DataFrame:
        result = build_macro_month(spark.createDataFrame(rows, OBSERVATION_SCHEMA), {"a": 1})
        return result.filter(result.month <= month(2018, 3))

    assert history(past).count() == 2
    assertDataFrameEqual(history(past + future), history(past))
