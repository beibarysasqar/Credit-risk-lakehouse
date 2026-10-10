import pytest

from credit_risk.common.config import ECB_PUBLICATION_LAGS, ECB_SERIES

pytestmark = pytest.mark.integration

MACRO_COLUMNS = [f"macro_{series}" for series in ECB_SERIES]
ALL_PRESENT = " AND ".join(f"{column} IS NOT NULL" for column in MACRO_COLUMNS)


def test_every_series_is_a_gapless_monthly_history(sql) -> None:
    rows = sql(
        """
        SELECT series, count(*), months_between(max(period_month), min(period_month)) + 1,
               min(period_month)
        FROM silver.macro_observation GROUP BY series
        """
    )
    assert {row[0] for row in rows} == set(ECB_SERIES)
    for series, observations, months, first_month in rows:
        assert int(observations) == int(float(months)), series
        assert first_month == "2009-01-01", series


def test_macro_month_has_one_row_per_consecutive_month(sql) -> None:
    [[rows, months, span]] = sql(
        "SELECT count(*), count(DISTINCT month), months_between(max(month), min(month)) + 1 "
        "FROM gold.macro_month"
    )
    assert int(rows) == int(months) == int(float(span))


@pytest.mark.parametrize("series", list(ECB_SERIES))
def test_macro_month_shifts_each_series_by_its_publication_lag(series: str, sql) -> None:
    """Month M carries the observation of M - lag, never a later one (no look-ahead)."""
    lag = ECB_PUBLICATION_LAGS[series]
    [[compared, mismatches]] = sql(
        f"""
        SELECT count(*), count_if(NOT m.macro_{series} <=> o.obs_value)
        FROM silver.macro_observation o
        JOIN gold.macro_month m ON m.month = add_months(o.period_month, {lag})
        WHERE o.series = '{series}'
        """
    )
    assert int(compared) > 100
    assert int(mismatches) == 0


def test_client_month_carries_the_macro_values_of_its_month(sql) -> None:
    same = " AND ".join(f"c.{column} <=> m.{column}" for column in MACRO_COLUMNS)
    [[rows, covered, matching]] = sql(
        f"""
        SELECT count(*), count_if({ALL_PRESENT.replace("macro_", "c.macro_")}), count_if({same})
        FROM gold.client_month c LEFT JOIN gold.macro_month m USING (month)
        """
    )
    assert int(matching) == int(rows)
    # The ECB history starts before the first client month, so every month is covered.
    assert int(covered) == int(rows)


def test_macro_warnings_of_latest_update(failed_expectations) -> None:
    assert failed_expectations["client_month", "macro_columns_present"] == 0
    # 2009-01 and 2009-02: the unemployment rate (lag 2) is not published yet.
    assert failed_expectations["macro_month", "macro_values_present"] == 2
