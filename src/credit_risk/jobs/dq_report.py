"""Publish the expectation results of the pipeline for data-quality reporting.

Merges the counters of every update in the pipeline event log into
``monitoring.expectation_result``, an ordinary Delta table owned by this job. A rerun adds only
the updates that have happened since.

Usage (wheel entry point ``dq_report``):
    dq_report --catalog <catalog>
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from credit_risk.common.config import (
    EVENT_LOG_ANCHOR_TABLE,
    EXPECTATION_RESULT_KEY,
    EXPECTATION_RESULT_TABLE,
)
from credit_risk.transformations.quality import expectation_results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    table = f"{args.catalog}.{EXPECTATION_RESULT_TABLE}"
    events = spark.sql(f"SELECT * FROM event_log(TABLE({args.catalog}.{EVENT_LOG_ANCHOR_TABLE}))")
    results = expectation_results(events)

    if spark.catalog.tableExists(table):
        # mergeInto has no alias for the target: its columns are qualified by the table name.
        same_key = " AND ".join(
            f"{table}.{column} <=> s.{column}" for column in EXPECTATION_RESULT_KEY
        )
        (
            results.alias("s")
            .mergeInto(table, F.expr(same_key))
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .merge()
        )
    else:
        results.writeTo(table).using("delta").clusterBy("update_id").create()

    # Violations of the latest update, as a run summary.
    spark.sql(
        f"""
        SELECT dataset, expectation, passed_records, failed_records, pass_rate
        FROM {table}
        WHERE update_ts = (SELECT max(update_ts) FROM {table}) AND failed_records > 0
        ORDER BY dataset, expectation
        """
    ).show(100, truncate=False)


if __name__ == "__main__":
    main()
