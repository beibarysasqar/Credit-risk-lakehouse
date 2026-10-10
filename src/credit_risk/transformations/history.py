"""History transformations: point-in-time reads of SCD2 tables and model-validation snapshots.

The validity interval of an SCD2 row (``__START_AT`` inclusive, ``__END_AT`` exclusive, null
while the version is current) is ingestion time: when the lakehouse learned the value, not the
synthetic calendar of ``transformations/calendar.py``.
"""

from datetime import date, datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SCD2_START = "__START_AT"
SCD2_END = "__END_AT"


def scd2_as_of(scd2: DataFrame, ts: datetime) -> DataFrame:
    """Return the versions of an SCD2 table that were valid at ``ts``.

    Grain: one row per key of the SCD2 table (keys unknown at ``ts`` are absent). A version is
    valid from ``__START_AT`` (inclusive) to ``__END_AT`` (exclusive).
    """
    at = F.lit(ts).cast("timestamp")
    return scd2.filter(
        (F.col(SCD2_START) <= at) & (F.col(SCD2_END).isNull() | (F.col(SCD2_END) > at))
    )


def build_validation_dataset(
    client_month: DataFrame,
    application_scd2: DataFrame,
    snapshot_id: str,
    as_of_month: date,
    snapshot_ts: datetime,
) -> DataFrame:
    """Build the model-validation slice of one snapshot.

    Grain: one row per ``snapshot_id`` x ``sk_id_curr`` (key): the ``gold.client_month`` row of
    ``as_of_month`` with the application attributes valid at ``snapshot_ts``. Later months and
    versions that started after ``snapshot_ts`` are never read. ``target`` is the label (null
    for test clients), not a feature. Ingestion metadata and the SCD2 interval are dropped.
    """
    months = client_month.filter(F.col("month") == F.lit(as_of_month)).drop("month")
    attributes = scd2_as_of(application_scd2, snapshot_ts)
    attributes = attributes.drop(*[name for name in attributes.columns if name.startswith("_")])
    snapshot = {
        "snapshot_id": F.lit(snapshot_id),
        "as_of_month": F.lit(as_of_month),
        "snapshot_ts": F.lit(snapshot_ts).cast("timestamp"),
    }
    joined = months.join(attributes, on="sk_id_curr", how="left")
    return joined.select(
        *[column.alias(name) for name, column in snapshot.items()],
        "sk_id_curr",
        *[name for name in joined.columns if name != "sk_id_curr"],
    )


def build_snapshot_manifest(
    histories: dict[str, DataFrame],
    dataset: DataFrame,
    snapshot_id: str,
    as_of_month: date,
    snapshot_ts: datetime,
) -> DataFrame:
    """Record the table versions a snapshot was built from.

    Grain: one row per ``snapshot_id`` x ``table_name`` (key). ``histories`` maps a table name
    to its ``DESCRIBE HISTORY`` output; ``table_version`` is the latest version in it and
    ``table_version_ts`` its commit time. ``n_dataset_rows`` is the number of rows of this
    snapshot in ``dataset`` (the validation dataset), repeated on every row.
    """
    versions = reduce(
        DataFrame.unionByName,
        [
            history.agg(
                F.max("version").alias("table_version"),
                F.max_by("timestamp", "version").alias("table_version_ts"),
            ).withColumn("table_name", F.lit(table_name))
            for table_name, history in histories.items()
        ],
    )
    n_rows = dataset.filter(F.col("snapshot_id") == snapshot_id).agg(
        F.count("*").alias("n_dataset_rows")
    )
    return versions.crossJoin(n_rows).select(
        F.lit(snapshot_id).alias("snapshot_id"),
        F.lit(as_of_month).alias("as_of_month"),
        F.lit(snapshot_ts).cast("timestamp").alias("snapshot_ts"),
        "table_name",
        "table_version",
        "table_version_ts",
        "n_dataset_rows",
    )
