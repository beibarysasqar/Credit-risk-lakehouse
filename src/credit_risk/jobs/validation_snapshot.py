"""Write a reproducible model-validation snapshot.

Appends the as-of-month slice to ``history.validation_dataset`` and the table versions it was
built from to ``history.validation_snapshot``. Both are ordinary Delta tables owned by this
job, so a snapshot can be read back with ``VERSION AS OF``.

Usage (wheel entry point ``validation_snapshot``):
    validation_snapshot --catalog <catalog> --as-of-month 2018-04-01 --snapshot-id <id>
"""

import argparse
from datetime import UTC, date, datetime

from pyspark.sql import DataFrame, SparkSession

from credit_risk.common.config import (
    HISTORY_ENTITIES,
    VALIDATION_DATASET_TABLE,
    VALIDATION_SNAPSHOT_TABLE,
)
from credit_risk.transformations.history import build_snapshot_manifest, build_validation_dataset


def _first_day_of_month(value: str) -> date:
    month = date.fromisoformat(value)
    if month.day != 1:
        raise argparse.ArgumentTypeError(f"{value!r} is not the first day of a month")
    return month


def _append(spark: SparkSession, df: DataFrame, table: str, cluster_by: list[str]) -> None:
    if spark.catalog.tableExists(table):
        df.writeTo(table).append()
    else:
        df.writeTo(table).using("delta").clusterBy(*cluster_by).create()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--as-of-month", type=_first_day_of_month, required=True)
    parser.add_argument("--snapshot-id", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    snapshot_ts = datetime.now(UTC)
    dataset_table = f"{args.catalog}.{VALIDATION_DATASET_TABLE}"
    manifest_table = f"{args.catalog}.{VALIDATION_SNAPSHOT_TABLE}"

    dataset = build_validation_dataset(
        spark.read.table(f"{args.catalog}.gold.client_month"),
        spark.read.table(f"{args.catalog}.history.application_scd2"),
        args.snapshot_id,
        args.as_of_month,
        snapshot_ts,
    )
    _append(spark, dataset, dataset_table, ["snapshot_id", "sk_id_curr"])

    # Read after the append, so the dataset version recorded is the one holding this snapshot.
    versioned = [VALIDATION_DATASET_TABLE, *(f"history.{e}_scd2" for e in HISTORY_ENTITIES)]
    manifest = build_snapshot_manifest(
        {table: spark.sql(f"DESCRIBE HISTORY {args.catalog}.{table}") for table in versioned},
        spark.read.table(dataset_table),
        args.snapshot_id,
        args.as_of_month,
        snapshot_ts,
    )
    _append(spark, manifest, manifest_table, ["snapshot_id"])

    spark.read.table(manifest_table).filter(f"snapshot_id = '{args.snapshot_id}'").show(
        truncate=False
    )


if __name__ == "__main__":
    main()
