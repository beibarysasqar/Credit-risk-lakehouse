from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from credit_risk.transformations.silver import (
    cast_columns,
    dedup_latest,
    flag_parent_exists,
    nullify_sentinel,
    nullify_tokens,
    standardize_columns,
)


def test_standardize_columns(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, 2.0, "f")], "SK_ID_PREV INT, AMT_RECIVABLE DOUBLE, _source_file STRING"
    )

    result = standardize_columns(df, {"amt_recivable": "amt_receivable"})

    assert result.columns == ["sk_id_prev", "amt_receivable", "_source_file"]


def test_nullify_tokens_only_touches_business_strings(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, "XNA", "XAP", "XNA"), (2, "Cash", None, "/landing/a.csv"), (3, "X", "xna", "f")],
        "id INT, a STRING, b STRING, _source_file STRING",
    )

    result = nullify_tokens(df)

    expected = spark.createDataFrame(
        [(1, None, None, "XNA"), (2, "Cash", None, "/landing/a.csv"), (3, "X", "xna", "f")],
        "id INT, a STRING, b STRING, _source_file STRING",
    )
    assertDataFrameEqual(result, expected)


def test_nullify_sentinel(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, 365243.0), (2, -637.0), (3, None)], "id INT, days DOUBLE")

    result = nullify_sentinel(df, "days", 365243, "days_anomaly")

    expected = spark.createDataFrame(
        [(1, None, True), (2, -637.0, False), (3, None, False)],
        "id INT, days DOUBLE, days_anomaly BOOLEAN",
    )
    assertDataFrameEqual(result, expected)


def test_cast_columns(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1.0, 418058.145, "a")], "n DOUBLE, amt DOUBLE, s STRING")

    result = cast_columns(df, {"n": "INT", "amt": "DECIMAL(18,3)"})

    assert result.schema.simpleString() == "struct<n:int,amt:decimal(18,3),s:string>"


def test_dedup_latest_keeps_most_recent_row_per_key(spark: SparkSession) -> None:
    old, new = datetime(2026, 1, 1), datetime(2026, 2, 1)
    df = spark.createDataFrame(
        [
            (1, "old", old, "b.csv"),
            (1, "new", new, "a.csv"),
            # Same load time: the later file name wins.
            (2, "first-file", new, "a.csv"),
            (2, "second-file", new, "b.csv"),
            (3, "single", old, "a.csv"),
            # Exact duplicates collapse into one row.
            (4, "same", old, "a.csv"),
            (4, "same", old, "a.csv"),
        ],
        "id INT, v STRING, _ingested_at TIMESTAMP, _source_file STRING",
    )

    result = dedup_latest(df, ["id"])

    expected = spark.createDataFrame(
        [
            (1, "new", new, "a.csv"),
            (2, "second-file", new, "b.csv"),
            (3, "single", old, "a.csv"),
            (4, "same", old, "a.csv"),
        ],
        df.schema,
    )
    assertDataFrameEqual(result, expected)


def test_dedup_latest_breaks_full_ties_deterministically(spark: SparkSession) -> None:
    ts = datetime(2026, 1, 1)
    rows = [(1, "a", ts, "f.csv"), (1, "b", ts, "f.csv"), (1, "c", ts, "f.csv")]
    schema = "id INT, v STRING, _ingested_at TIMESTAMP, _source_file STRING"

    forward = dedup_latest(spark.createDataFrame(rows, schema), ["id"])
    backward = dedup_latest(spark.createDataFrame(rows[::-1], schema).repartition(3), ["id"])

    assertDataFrameEqual(forward, backward)


def test_flag_parent_exists_does_not_fan_out(spark: SparkSession) -> None:
    child = spark.createDataFrame([(1, "a"), (2, "b"), (None, "c")], "k INT, v STRING")
    parent = spark.createDataFrame([(1,), (1,), (3,)], "k INT")

    result = flag_parent_exists(child, parent, "k", "has_parent")

    expected = spark.createDataFrame(
        [(1, "a", True), (2, "b", False), (None, "c", False)], "k INT, v STRING, has_parent BOOLEAN"
    )
    assertDataFrameEqual(result, expected)
