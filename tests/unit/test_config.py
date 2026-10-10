import re

import pytest

from credit_risk.common.config import (
    ECB_PUBLICATION_LAGS,
    ECB_SERIES,
    HISTORY_ENTITIES,
    HISTORY_SEQUENCE_COLUMN,
    HOME_CREDIT_TABLES,
    INGESTION_METADATA_COLUMNS,
    SILVER_ENTITIES,
    ecb_landing_dir,
    landing_dir,
)


def test_home_credit_entities_are_snake_case() -> None:
    assert len(HOME_CREDIT_TABLES) == 8
    assert all(re.fullmatch(r"[a-z][a-z0-9_]*", entity) for entity in HOME_CREDIT_TABLES)
    assert HOME_CREDIT_TABLES["pos_cash_balance"] == "POS_CASH_balance.csv"


def test_landing_dir_uses_the_given_catalog() -> None:
    assert landing_dir("some_catalog", "bureau") == (
        "/Volumes/some_catalog/raw/landing/home_credit/bureau/"
    )


def test_landing_dir_rejects_unknown_entity() -> None:
    with pytest.raises(KeyError):
        landing_dir("some_catalog", "POS_CASH_balance")


def test_ecb_series_are_monthly_with_a_lag() -> None:
    assert all(re.fullmatch(r"[a-z][a-z0-9_]*", series) for series in ECB_SERIES)
    # The lag logic and the period parsing assume monthly series.
    assert all(spec.key.startswith("M.") for spec in ECB_SERIES.values())
    assert ECB_PUBLICATION_LAGS == {"hicp_yoy": 1, "unemployment_rate": 2, "euribor_3m": 0}


def test_ecb_landing_dir() -> None:
    assert ecb_landing_dir("some_catalog", "hicp_yoy") == (
        "/Volumes/some_catalog/raw/landing/ecb/hicp_yoy/"
    )
    with pytest.raises(KeyError):
        ecb_landing_dir("some_catalog", "gdp")


def test_history_entities_are_silver_entities_sequenced_by_ingestion_time() -> None:
    assert HISTORY_ENTITIES == {"application": "sk_id_curr", "previous_application": "sk_id_prev"}
    assert HISTORY_ENTITIES.keys() <= set(SILVER_ENTITIES)
    assert HISTORY_SEQUENCE_COLUMN in INGESTION_METADATA_COLUMNS
