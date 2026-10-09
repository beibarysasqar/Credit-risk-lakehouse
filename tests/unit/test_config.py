import re

import pytest

from credit_risk.common.config import HOME_CREDIT_TABLES, landing_dir


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
