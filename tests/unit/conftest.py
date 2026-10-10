"""Fixtures that build Bronze-like and Silver-prepared DataFrames from the CSV fixtures."""

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from credit_risk.common.config import ECB_PUBLICATION_LAGS, ECB_SERIES
from credit_risk.common.expectations import SILVER_RULES
from credit_risk.transformations import dpd, gold, macro, silver

FIXTURES = Path(__file__).parents[1] / "fixtures" / "home_credit"
ECB_FIXTURES = Path(__file__).parents[1] / "fixtures" / "ecb"


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
def bronze_ecb(spark: SparkSession) -> Callable[[str], DataFrame]:
    """Load an ECB fixture CSV the way Bronze delivers it: hinted types plus metadata columns."""

    def load(series: str) -> DataFrame:
        source = spark.read.csv(str(ECB_FIXTURES / f"{series}.csv"), header=True)
        return source.withColumns(
            {
                "OBS_VALUE": F.col("OBS_VALUE").cast("double"),
                "_rescued_data": F.lit(None).cast("string"),
                "_ingested_at": F.lit(datetime(2026, 1, 1)),
                "_source_file": F.lit(f"/landing/ecb/{series}/{series}_20260101.csv"),
            }
        )

    return load


@pytest.fixture(scope="session")
def prepared(
    bronze: Callable[[str], DataFrame], bronze_ecb: Callable[[str], DataFrame]
) -> Callable[[str], DataFrame]:
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
        if entity == "macro_observation":
            return macro.clean_macro_observations({s: bronze_ecb(s) for s in ECB_SERIES})
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


@pytest.fixture(scope="session")
def derived(silver_table: Callable[[str], DataFrame]) -> Callable[[str], DataFrame]:
    """Build a table derived from Silver (installments, contract / client / macro month)."""

    def build(entity: str) -> DataFrame:
        if entity == "installments":
            return dpd.aggregate_installments(silver_table("installments_payments"))
        if entity == "contract_month":
            return gold.build_contract_month(
                silver_table("pos_cash_balance"),
                silver_table("credit_card_balance"),
                dpd.installment_dpd_by_month(build("installments")),
                silver_table("previous_application"),
            )
        if entity == "macro_month":
            return macro.build_macro_month(silver_table("macro_observation"), ECB_PUBLICATION_LAGS)
        bureau_month = gold.bureau_client_month(
            silver_table("bureau_balance"), silver_table("bureau")
        )
        return gold.build_client_month(build("contract_month"), bureau_month)

    return build
