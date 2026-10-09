import re
from collections.abc import Callable

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from credit_risk.common.config import SILVER_ENTITIES
from credit_risk.common.expectations import DERIVED_RULES, SILVER_RULES


def test_every_silver_entity_has_rules() -> None:
    assert tuple(SILVER_RULES) == SILVER_ENTITIES
    for rules in SILVER_RULES.values():
        assert rules.fail and rules.drop and rules.warn


@pytest.mark.parametrize("entity", SILVER_ENTITIES)
def test_rule_names_are_snake_case_and_unique(entity: str) -> None:
    rules = SILVER_RULES[entity]
    names = [*rules.fail, *rules.drop, *rules.warn]
    assert all(re.fullmatch(r"[a-z][a-z0-9_]*", name) for name in names)
    assert len(names) == len(set(names))


@pytest.mark.parametrize("entity", SILVER_ENTITIES)
def test_rules_evaluate_on_prepared_data(entity: str, prepared: Callable[[str], DataFrame]) -> None:
    rules = SILVER_RULES[entity]
    df = prepared(entity)

    def count_where(condition: str) -> int:
        return df.filter(F.expr(condition)).count()

    # Every rule resolves against the Silver columns.
    for condition in rules.warn.values():
        count_where(condition)
    # Keys of the fixtures are clean, so nothing would stop the pipeline.
    for condition in rules.fail.values():
        assert count_where(f"NOT coalesce({condition}, false)") == 0
    # A drop rule is never null: a row is either kept or quarantined.
    for condition in rules.drop.values():
        assert count_where(f"({condition}) IS NULL") == 0


@pytest.mark.parametrize("entity", list(DERIVED_RULES))
def test_derived_rules_evaluate_on_derived_data(
    entity: str, derived: Callable[[str], DataFrame]
) -> None:
    rules = DERIVED_RULES[entity]
    df = derived(entity)
    names = [*rules.fail, *rules.warn]
    assert all(re.fullmatch(r"[a-z][a-z0-9_]*", name) for name in names)
    assert len(names) == len(set(names))
    # Derived tables have no quarantine: their inputs are already clean.
    assert not rules.drop

    for condition in rules.warn.values():
        df.filter(F.expr(condition)).count()
    for condition in rules.fail.values():
        assert df.filter(F.expr(f"NOT coalesce({condition}, false)")).count() == 0
