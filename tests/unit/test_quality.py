import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from credit_risk.transformations.quality import expectation_results, quarantine_rows

RULES = {"b_positive": "b > 0", "a_known": "a IN ('x', 'y')"}


def test_quarantine_rows_keeps_only_broken_rows(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, "x", 5), (2, "z", 5), (3, "x", -1), (4, "z", 0), (5, None, 5), (6, "y", None)],
        "id INT, a STRING, b INT",
    )

    result = quarantine_rows(df, RULES)

    expected = spark.createDataFrame(
        [
            (2, "z", 5, ["a_known"]),
            (3, "x", -1, ["b_positive"]),
            (4, "z", 0, ["a_known", "b_positive"]),
            # A rule that evaluates to null counts as broken.
            (5, None, 5, ["a_known"]),
            (6, "y", None, ["b_positive"]),
        ],
        "id INT, a STRING, b INT, _failed_rules ARRAY<STRING>",
    )
    assertDataFrameEqual(result, expected)


def _event(update_id: str | None, ts: str, event_type: str, expectations: list | None) -> tuple:
    details: dict = {"flow_progress": {"status": "COMPLETED"}}
    if expectations is not None:
        details["flow_progress"]["data_quality"] = {
            "dropped_records": 0,
            "expectations": [
                {"name": name, "dataset": dataset, "passed_records": ok, "failed_records": bad}
                for dataset, name, ok, bad in expectations
            ],
        }
    if event_type != "flow_progress":
        details = {event_type: {}}
    return ((update_id,), datetime.fromisoformat(ts), event_type, json.dumps(details))


EVENT_LOG_SCHEMA = (
    "origin STRUCT<update_id: STRING>, timestamp TIMESTAMP, event_type STRING, details STRING"
)
RESULT_SCHEMA = (
    "update_id STRING, dataset STRING, expectation STRING, update_ts TIMESTAMP, "
    "passed_records BIGINT, failed_records BIGINT, pass_rate DOUBLE"
)


def test_expectation_results_sums_the_progress_events_of_an_update(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            # Maintenance events carry no update and are ignored.
            (_event(None, "2026-01-01T00:00:00", "create_update", None)),
            (_event("u1", "2026-01-02T10:00:00", "create_update", None)),
            # A flow that only reports metrics.
            (_event("u1", "2026-01-02T10:01:00", "flow_progress", None)),
            # A streaming flow reports per micro-batch: the counters add up.
            (_event("u1", "2026-01-02T10:02:00", "flow_progress", [("c.s.t", "key", 6, 0)])),
            (
                _event(
                    "u1",
                    "2026-01-02T10:03:00",
                    "flow_progress",
                    [("c.s.t", "key", 2, 0), ("c.s.t", "plausible", 1, 3)],
                )
            ),
            # The same expectation name on another dataset stays separate.
            (_event("u1", "2026-01-02T10:04:00", "flow_progress", [("changes", "key", 5, 0)])),
            # Nothing evaluated: the pass rate is unknown, not 0 or 1.
            (_event("u1", "2026-01-02T10:05:00", "flow_progress", [("c.s.e", "key", 0, 0)])),
        ],
        EVENT_LOG_SCHEMA,
    )

    result = expectation_results(events)

    update_ts = datetime(2026, 1, 2, 10, 0)
    expected = spark.createDataFrame(
        [
            ("u1", "c.s.t", "key", update_ts, 8, 0, 1.0),
            ("u1", "c.s.t", "plausible", update_ts, 1, 3, 0.25),
            ("u1", "changes", "key", update_ts, 5, 0, 1.0),
            ("u1", "c.s.e", "key", update_ts, 0, 0, None),
        ],
        RESULT_SCHEMA,
    )
    assertDataFrameEqual(result, expected)


def test_expectation_results_keeps_updates_apart(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            (_event("u1", "2026-01-02T10:00:00", "create_update", None)),
            (_event("u1", "2026-01-02T10:02:00", "flow_progress", [("c.s.t", "key", 6, 1)])),
            (_event("u2", "2026-01-03T09:00:00", "create_update", None)),
            (_event("u2", "2026-01-03T09:05:00", "flow_progress", [("c.s.t", "key", 7, 0)])),
        ],
        EVENT_LOG_SCHEMA,
    )

    result = expectation_results(events)

    expected = spark.createDataFrame(
        [
            ("u1", "c.s.t", "key", datetime(2026, 1, 2, 10, 0), 6, 1, 6 / 7),
            ("u2", "c.s.t", "key", datetime(2026, 1, 3, 9, 0), 7, 0, 1.0),
        ],
        RESULT_SCHEMA,
    )
    assertDataFrameEqual(result, expected)
