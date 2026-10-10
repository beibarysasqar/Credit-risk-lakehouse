from datetime import UTC, date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.testing import assertDataFrameEqual

from credit_risk.transformations.history import (
    build_snapshot_manifest,
    build_validation_dataset,
    scd2_as_of,
)

SCD2_SCHEMA = (
    "sk_id_curr BIGINT, is_train BOOLEAN, target INT, amt_income_total INT, "
    "_ingested_at TIMESTAMP, _source_file STRING, __START_AT TIMESTAMP, __END_AT TIMESTAMP"
)
FIRST_DELIVERY = datetime(2026, 1, 1, tzinfo=UTC)
CORRECTION = datetime(2026, 2, 1, tzinfo=UTC)
AS_OF_MONTH = date(2018, 4, 1)


def application_scd2(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            # Client 1 was corrected on the second delivery: two versions.
            (1, True, 1, 100, FIRST_DELIVERY, "a.csv", FIRST_DELIVERY, CORRECTION),
            (1, True, 1, 110, CORRECTION, "b.csv", CORRECTION, None),
            (2, False, None, 200, FIRST_DELIVERY, "a.csv", FIRST_DELIVERY, None),
            # Client 3 became known only with the second delivery.
            (3, True, 0, 300, CORRECTION, "b.csv", CORRECTION, None),
        ],
        SCD2_SCHEMA,
    )


def incomes_as_of(spark: SparkSession, ts: datetime) -> dict[int, int]:
    rows = scd2_as_of(application_scd2(spark), ts).collect()
    assert len({row.sk_id_curr for row in rows}) == len(rows)
    return {row.sk_id_curr: int(row.amt_income_total) for row in rows}


def test_scd2_as_of_start_is_inclusive_and_end_is_exclusive(spark: SparkSession) -> None:
    assert incomes_as_of(spark, datetime(2025, 12, 31, tzinfo=UTC)) == {}
    assert incomes_as_of(spark, FIRST_DELIVERY) == {1: 100, 2: 200}
    assert incomes_as_of(spark, datetime(2026, 1, 31, 23, 59, 59, tzinfo=UTC)) == {1: 100, 2: 200}
    # At the boundary the closed version is already gone and the new one is in force.
    assert incomes_as_of(spark, CORRECTION) == {1: 110, 2: 200, 3: 300}


def client_month(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            (1, date(2018, 3, 1), 0, False),
            (1, AS_OF_MONTH, 95, True),
            # Look-ahead bait: a later month must never reach the snapshot.
            (1, date(2018, 5, 1), 125, True),
            (2, AS_OF_MONTH, 0, False),
            (3, AS_OF_MONTH, 10, False),
            # No history row at all: the client is kept with null attributes.
            (4, AS_OF_MONTH, 0, False),
            (5, date(2018, 5, 1), 0, False),
        ],
        "sk_id_curr BIGINT, month DATE, max_dpd INT, default_flag BOOLEAN",
    )


DATASET_SCHEMA = (
    "snapshot_id STRING, as_of_month DATE, snapshot_ts TIMESTAMP, sk_id_curr BIGINT, "
    "max_dpd INT, default_flag BOOLEAN, is_train BOOLEAN, target INT, "
    "amt_income_total INT"
)


def test_validation_dataset_is_point_in_time(spark: SparkSession) -> None:
    # Taken between the deliveries: the correction and client 3 are not known yet.
    ts = datetime(2026, 1, 15, tzinfo=UTC)

    result = build_validation_dataset(
        client_month(spark), application_scd2(spark), "s1", AS_OF_MONTH, ts
    )

    assert result.columns == [field.split()[0] for field in DATASET_SCHEMA.split(", ")]
    expected = spark.createDataFrame(
        [
            ("s1", AS_OF_MONTH, ts, 1, 95, True, True, 1, 100),
            ("s1", AS_OF_MONTH, ts, 2, 0, False, False, None, 200),
            ("s1", AS_OF_MONTH, ts, 3, 10, False, None, None, None),
            ("s1", AS_OF_MONTH, ts, 4, 0, False, None, None, None),
        ],
        DATASET_SCHEMA,
    )
    assertDataFrameEqual(result, expected)


def test_validation_dataset_uses_the_version_valid_at_snapshot_time(spark: SparkSession) -> None:
    result = build_validation_dataset(
        client_month(spark), application_scd2(spark), "s2", AS_OF_MONTH, CORRECTION
    )

    incomes = {row.sk_id_curr: row.amt_income_total for row in result.collect()}
    assert incomes == {1: 110, 2: 200, 3: 300, 4: None}


def test_snapshot_manifest_records_latest_versions(spark: SparkSession) -> None:
    history_schema = "version BIGINT, timestamp TIMESTAMP, operation STRING"
    histories = {
        "history.validation_dataset": spark.createDataFrame(
            [(0, FIRST_DELIVERY, "CREATE TABLE"), (1, CORRECTION, "WRITE")], history_schema
        ),
        "history.application_scd2": spark.createDataFrame(
            [(7, CORRECTION, "STREAMING UPDATE"), (6, FIRST_DELIVERY, "STREAMING UPDATE")],
            history_schema,
        ),
    }
    dataset = spark.createDataFrame(
        [("s1", 1), ("s1", 2), ("s0", 1)], "snapshot_id STRING, sk_id_curr BIGINT"
    )
    ts = datetime(2026, 2, 2, tzinfo=UTC)

    result = build_snapshot_manifest(histories, dataset, "s1", AS_OF_MONTH, ts)

    expected = spark.createDataFrame(
        [
            ("s1", AS_OF_MONTH, ts, "history.validation_dataset", 1, CORRECTION, 2),
            ("s1", AS_OF_MONTH, ts, "history.application_scd2", 7, CORRECTION, 2),
        ],
        "snapshot_id STRING, as_of_month DATE, snapshot_ts TIMESTAMP, table_name STRING, "
        "table_version BIGINT, table_version_ts TIMESTAMP, n_dataset_rows BIGINT",
    )
    assertDataFrameEqual(result, expected)
