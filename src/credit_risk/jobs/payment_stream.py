"""Ingest the payment stream micro-files that have landed since the previous run.

Appends to ``bronze.payment_events``, an ordinary Delta table owned by this job.

Usage (wheel entry point ``payment_stream``):
    payment_stream --catalog <catalog>
"""

import argparse

from pyspark.sql import SparkSession

from credit_risk.common.config import (
    PAYMENT_EVENTS_TABLE,
    payment_stream_checkpoint_dir,
    payment_stream_events_dir,
    payment_stream_schema_dir,
)
from credit_risk.streaming.consumer import read_payment_events, write_payment_events


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    table = f"{args.catalog}.{PAYMENT_EVENTS_TABLE}"
    events = read_payment_events(
        spark, payment_stream_events_dir(args.catalog), payment_stream_schema_dir(args.catalog)
    )
    write_payment_events(events, table, payment_stream_checkpoint_dir(args.catalog))

    spark.read.table(table).groupBy("_source_file").count().orderBy("_source_file").show(
        100, truncate=False
    )


if __name__ == "__main__":
    main()
