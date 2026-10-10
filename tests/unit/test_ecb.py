from datetime import date
from pathlib import Path

import pytest

from credit_risk.common.config import ECB_SERIES
from credit_risk.ingestion.ecb import plan_landing, series_url, validate_csv

FIXTURES = Path(__file__).parents[1] / "fixtures" / "ecb"


def test_series_url_uses_the_data_api() -> None:
    assert series_url("hicp_yoy", start_period="2017-11") == (
        "https://data-api.ecb.europa.eu/service/data/ICP/M.U2.N.000000.4.ANR"
        "?format=csvdata&startPeriod=2017-11"
    )
    assert all("sdw-wsrest" not in series_url(series) for series in ECB_SERIES)


def test_plan_landing_dates_every_series_file() -> None:
    plan = {item.series: item for item in plan_landing("some_catalog", date(2026, 10, 10))}

    assert plan.keys() == ECB_SERIES.keys()
    assert plan["euribor_3m"].volume_path == (
        "/Volumes/some_catalog/raw/landing/ecb/euribor_3m/euribor_3m_20261010.csv"
    )
    assert plan["euribor_3m"].url == series_url("euribor_3m")


@pytest.mark.parametrize("series", list(ECB_SERIES))
def test_validate_csv_accepts_the_api_response(series: str) -> None:
    validate_csv(series, (FIXTURES / f"{series}.csv").read_bytes())


@pytest.mark.parametrize(
    ("body", "message"),
    [
        # An unknown or empty selection is answered with HTTP 200 and an empty body.
        (b"", "lacks columns KEY, TIME_PERIOD, OBS_VALUE, OBS_STATUS"),
        (b"<html>error</html>", "lacks columns"),
        (b"KEY,TIME_PERIOD,OBS_VALUE,OBS_STATUS\n", "no observations"),
    ],
)
def test_validate_csv_rejects_a_response_without_data(body: bytes, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        validate_csv("hicp_yoy", body)
