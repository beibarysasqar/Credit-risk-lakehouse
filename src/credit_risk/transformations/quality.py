"""Data-quality helpers: quarantine of Silver rows and expectation results of the pipeline."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# details.flow_progress.data_quality.expectations of a pipeline event, as reported by Lakeflow.
_EXPECTATIONS_PATH = "$.flow_progress.data_quality.expectations"
_EXPECTATIONS_TYPE = (
    "array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>"
)


def quarantine_rows(df: DataFrame, rules: dict[str, str]) -> DataFrame:
    """Return the rows that break at least one of ``rules`` (expectation name -> SQL condition).

    Grain: unchanged, a subset of the input rows; the complement of what ``expect_or_drop``
    keeps for the same rules. A rule that evaluates to null counts as broken. Adds
    ``_failed_rules``, the sorted names of the broken rules.
    """
    failed = [
        F.when(~F.coalesce(F.expr(condition), F.lit(False)), F.lit(name))
        for name, condition in sorted(rules.items())
    ]
    return df.withColumn("_failed_rules", F.array_compact(F.array(*failed))).filter(
        F.size("_failed_rules") > 0
    )


def expectation_results(events: DataFrame) -> DataFrame:
    """Return the expectation counters of every pipeline update found in the event log.

    Grain: one row per ``update_id`` x ``dataset`` x ``expectation`` (the key). ``events`` is the
    pipeline event log (``origin.update_id``, ``timestamp``, ``event_type``, ``details`` as a JSON
    string). A flow reports its counters in one or more ``flow_progress`` events, which are
    summed. ``update_ts`` is the first event of the update; ``pass_rate`` is null when no record
    was evaluated. ``dataset`` is kept as reported: a fully qualified table name, or the bare name
    of a pipeline-private view.
    """
    expectations = F.from_json(F.get_json_object("details", _EXPECTATIONS_PATH), _EXPECTATIONS_TYPE)
    evaluated = F.col("passed_records") + F.col("failed_records")
    return (
        events.select(
            F.col("origin.update_id").alias("update_id"), "timestamp", "event_type", "details"
        )
        .filter(F.col("update_id").isNotNull())
        .withColumn("update_ts", F.min("timestamp").over(Window.partitionBy("update_id")))
        .filter(F.col("event_type") == "flow_progress")
        .select("update_id", "update_ts", F.explode(expectations).alias("e"))
        .groupBy(
            "update_id",
            F.col("e.dataset").alias("dataset"),
            F.col("e.name").alias("expectation"),
        )
        .agg(
            F.first("update_ts").alias("update_ts"),
            F.sum("e.passed_records").alias("passed_records"),
            F.sum("e.failed_records").alias("failed_records"),
        )
        .withColumn("pass_rate", F.when(evaluated > 0, F.col("passed_records") / evaluated))
    )
