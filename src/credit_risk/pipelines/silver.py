"""Silver layer: cleaned Home Credit tables with expectations and quarantine tables."""

from collections.abc import Callable

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from credit_risk.common.config import SILVER_ENTITIES
from credit_risk.common.expectations import SILVER_RULES
from credit_risk.transformations import silver
from credit_risk.transformations.quality import quarantine_rows

spark = SparkSession.getActiveSession()


def _bronze(entity: str) -> DataFrame:
    return spark.read.table(f"bronze.{entity}")


def _silver(entity: str) -> DataFrame:
    return spark.read.table(f"silver.{entity}")


# Silver entity -> (cleaned DataFrame builder, liquid clustering columns).
SILVER_TABLES: dict[str, tuple[Callable[[], DataFrame], list[str]]] = {
    "application": (
        lambda: silver.clean_application(_bronze("application_train"), _bronze("application_test")),
        ["sk_id_curr"],
    ),
    "bureau": (lambda: silver.clean_bureau(_bronze("bureau")), ["sk_id_curr"]),
    "bureau_balance": (
        lambda: silver.clean_bureau_balance(_bronze("bureau_balance"), _silver("bureau")),
        ["sk_id_bureau", "month"],
    ),
    "previous_application": (
        lambda: silver.clean_previous_application(_bronze("previous_application")),
        ["sk_id_curr"],
    ),
    "pos_cash_balance": (
        lambda: silver.clean_pos_cash_balance(
            _bronze("pos_cash_balance"), _silver("previous_application")
        ),
        ["sk_id_curr", "month"],
    ),
    "credit_card_balance": (
        lambda: silver.clean_credit_card_balance(
            _bronze("credit_card_balance"), _silver("previous_application")
        ),
        ["sk_id_curr", "month"],
    ),
    "installments_payments": (
        lambda: silver.clean_installments_payments(
            _bronze("installments_payments"), _silver("previous_application")
        ),
        ["sk_id_curr", "sk_id_prev"],
    ),
}

assert tuple(SILVER_TABLES) == SILVER_ENTITIES


def _define_silver_table(
    entity: str, build: Callable[[], DataFrame], cluster_by: list[str]
) -> None:
    rules = SILVER_RULES[entity]
    prepared = f"silver_{entity}_prepared"

    @dp.temporary_view(name=prepared)
    def _prepared() -> DataFrame:
        return build()

    @dp.materialized_view(
        name=f"silver.{entity}",
        comment=f"Cleaned Home Credit {entity}: typed, snake_case, sentinel-free, deduplicated.",
        cluster_by=cluster_by,
    )
    @dp.expect_all_or_fail(rules.fail)
    @dp.expect_all_or_drop(rules.drop)
    @dp.expect_all(rules.warn)
    def _clean() -> DataFrame:
        return spark.read.table(prepared)

    @dp.materialized_view(
        name=f"silver.{entity}_quarantine",
        comment=f"Rows of {entity} dropped by the Silver expectations, with the broken rules.",
    )
    def _quarantine() -> DataFrame:
        return quarantine_rows(spark.read.table(prepared), rules.drop)


for _entity, (_build, _cluster_by) in SILVER_TABLES.items():
    _define_silver_table(_entity, _build, _cluster_by)
