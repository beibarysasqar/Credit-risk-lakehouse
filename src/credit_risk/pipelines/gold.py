"""Gold layer: monthly DPD, default flag and exposure per contract and per client."""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from credit_risk.common.expectations import DERIVED_RULES
from credit_risk.transformations import gold
from credit_risk.transformations.dpd import installment_dpd_by_month

spark = SparkSession.getActiveSession()


def _silver(entity: str) -> DataFrame:
    return spark.read.table(f"silver.{entity}")


@dp.materialized_view(
    name="gold.contract_month",
    comment="Previous Home Credit loans by month: source and computed DPD, activity, exposure.",
    cluster_by=["sk_id_curr", "month"],
)
@dp.expect_all_or_fail(DERIVED_RULES["contract_month"].fail)
@dp.expect_all(DERIVED_RULES["contract_month"].warn)
def _contract_month() -> DataFrame:
    return gold.build_contract_month(
        _silver("pos_cash_balance"),
        _silver("credit_card_balance"),
        installment_dpd_by_month(_silver("installments")),
        _silver("previous_application"),
    )


@dp.materialized_view(
    name="gold.client_month",
    comment="Clients by month: max DPD, 90+ DPD default flag, exposure and bureau statuses.",
    cluster_by=["sk_id_curr", "month"],
)
@dp.expect_all_or_fail(DERIVED_RULES["client_month"].fail)
@dp.expect_all(DERIVED_RULES["client_month"].warn)
def _client_month() -> DataFrame:
    return gold.build_client_month(
        spark.read.table("gold.contract_month"),
        gold.bureau_client_month(_silver("bureau_balance"), _silver("bureau")),
    )
