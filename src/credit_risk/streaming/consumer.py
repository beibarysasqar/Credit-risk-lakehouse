"""Consumer of the payment stream: JSON micro-files -> ``bronze.payment_events``.

Runs as an incremental batch: ``availableNow`` is the only trigger supported on serverless. The
checkpoint remembers the files already ingested, so every run appends the new files only.
"""

from pyspark.sql import DataFrame, SparkSession

from credit_risk.common.config import PAYMENT_STREAM_ENTITY
from credit_risk.common.schemas import schema_hints
from credit_risk.transformations.bronze import add_ingestion_metadata


def read_payment_events(spark: SparkSession, path: str, schema_location: str) -> DataFrame:
    """Read the micro-files under ``path`` as a stream with Auto Loader.

    Grain: one row per event. Columns are typed like ``bronze.installments_payments``; anything
    that does not fit goes to ``_rescued_data``. Outside a pipeline Auto Loader needs its own
    schema location.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", schema_hints(PAYMENT_STREAM_ENTITY))
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .load(path)
    )


def write_payment_events(events: DataFrame, table: str, checkpoint: str) -> None:
    """Append the available ``events`` plus ingestion metadata to ``table`` and stop.

    Grain: unchanged, one row per event. ``events`` must be a stream read from files.
    """
    query = (
        add_ingestion_metadata(events)
        .writeStream.trigger(availableNow=True)
        .option("checkpointLocation", checkpoint)
        .toTable(table)
    )
    query.awaitTermination()
