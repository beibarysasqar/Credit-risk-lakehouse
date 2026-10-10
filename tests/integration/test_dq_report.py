import pytest

from credit_risk.common.config import EXPECTATION_RESULT_KEY, EXPECTATION_RESULT_TABLE

pytestmark = pytest.mark.integration

KEY = ", ".join(EXPECTATION_RESULT_KEY)


def test_expectation_result_key_is_unique_and_complete(sql) -> None:
    [[rows, keys, incomplete]] = sql(
        f"""
        SELECT
          count(*),
          count(DISTINCT {KEY}),
          count_if(update_ts IS NULL OR passed_records IS NULL OR failed_records IS NULL)
        FROM {EXPECTATION_RESULT_TABLE}
        """
    )
    assert int(rows) > 0
    assert rows == keys
    assert int(incomplete) == 0


def test_pass_rate_follows_the_counters(sql) -> None:
    [[wrong]] = sql(
        f"""
        SELECT count(*) FROM {EXPECTATION_RESULT_TABLE}
        WHERE NOT (pass_rate <=> try_divide(passed_records, passed_records + failed_records))
        """
    )
    assert int(wrong) == 0


def test_latest_update_matches_the_event_log(sql, failed_expectations) -> None:
    """The report of the latest pipeline update equals the counters read from the event log.

    Run ``dq_report_job`` after the pipeline update for this to hold.
    """
    rows = sql(
        f"""
        SELECT dataset, expectation, failed_records FROM {EXPECTATION_RESULT_TABLE}
        WHERE update_ts = (SELECT max(update_ts) FROM {EXPECTATION_RESULT_TABLE})
        """
    )
    reported = {(dataset.split(".")[-1], name): int(bad) for dataset, name, bad in rows}
    assert reported == failed_expectations
