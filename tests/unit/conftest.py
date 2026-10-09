"""Fixtures that build Bronze-like and Silver-prepared DataFrames from the CSV fixtures."""

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from credit_risk.common.expectations import SILVER_RULES
from credit_risk.transformations import silver

FIXTURES = Path(__file__).parents[1] / "fixtures" / "home_credit"


@pytest.fixture(scope="session")
def bronze(spark: SparkSession) -> Callable[[str], DataFrame]:
    """Load a fixture CSV the way Bronze delivers it: inferred types plus metadata columns."""

    def load(entity: str) -> DataFrame:
        source = spark.read.csv(str(FIXTURES / f"{entity}.csv"), header=True, inferSchema=True)
        return source.withColumns(
            {
                "_rescued_data": F.lit(None).cast("string"),
                "_ingested_at": F.lit(datetime(2026, 1, 1)),
                "_source_file": F.lit(f"/landing/home_credit/{entity}/{entity}.csv"),
            }
        )

    return load


@pytest.fixture(scope="session")
def prepared(bronze: Callable[[str], DataFrame]) -> Callable[[str], DataFrame]:
    """Build the cleaned (pre-expectation) DataFrame of a Silver entity from the fixtures."""

    def build(entity: str) -> DataFrame:
        if entity == "application":
            return silver.clean_application(bronze("application_train"), bronze("application_test"))
        if entity == "bureau":
            return silver.clean_bureau(bronze("bureau"))
        if entity == "bureau_balance":
            return silver.clean_bureau_balance(bronze("bureau_balance"), build("bureau"))
        if entity == "previous_application":
            return silver.clean_previous_application(bronze("previous_application"))
        clean = getattr(silver, f"clean_{entity}")
        return clean(bronze(entity), build("previous_application"))

    return build


@pytest.fixture(scope="session")
def silver_table(prepared: Callable[[str], DataFrame]) -> Callable[[str], DataFrame]:
    """Build a Silver table as published: the prepared rows that pass every drop rule."""

    def build(entity: str) -> DataFrame:
        df = prepared(entity)
        for condition in SILVER_RULES[entity].drop.values():
            df = df.filter(F.expr(condition))
        return df

    return build
