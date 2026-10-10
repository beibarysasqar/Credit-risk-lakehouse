"""Gold transformations: monthly DPD, default flag and exposure per contract and per client.

Inputs are Silver DataFrames. The default definition (90+ DPD) is documented in
``docs/default_definition.md``; all months are synthetic (see ``transformations/calendar.py``).
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from credit_risk.common.config import (
    ACTIVE_CONTRACT_STATUSES,
    AMOUNT_TYPE,
    BUREAU_DEFAULT_BUCKET,
    DEFAULT_DPD_THRESHOLD,
)
from credit_risk.transformations.dpd import dpd_bucket

CONTRACT_TYPE_POS_CASH = "pos_cash"
CONTRACT_TYPE_CREDIT_CARD = "credit_card"


def _balance_rows(
    balance: DataFrame, contract_type: str, cnt_instalment_future: Column, amt_balance: Column
) -> DataFrame:
    """Reduce a monthly balance table to the columns shared by POS/cash loans and cards."""
    return balance.select(
        "sk_id_prev",
        "sk_id_curr",
        "month",
        F.lit(contract_type).alias("contract_type"),
        "name_contract_status",
        F.col("sk_dpd").alias("source_dpd"),
        F.col("sk_dpd_def").alias("source_dpd_def"),
        cnt_instalment_future.alias("cnt_instalment_future"),
        amt_balance.alias("amt_balance"),
        "has_previous_application",
    )


def build_contract_month(
    pos_cash_balance: DataFrame,
    credit_card_balance: DataFrame,
    installment_dpd: DataFrame,
    previous_application: DataFrame,
) -> DataFrame:
    """Combine the source DPD of the balance tables with the computed installment DPD.

    Grain: one row per ``sk_id_prev`` x ``month`` (key), for every month with a balance row or
    with an installment due or overdue. ``dpd`` is the source ``sk_dpd`` when a balance row
    exists and the computed ``installment_dpd`` otherwise; the source value is never
    overwritten, both are kept for reconciliation and ``dpd_source`` names the one used.
    ``is_active``: the balance status is an open one, or the month has only installments.
    ``exposure_amount``: ``amt_balance`` for cards; for POS/cash loans the estimate
    ``cnt_instalment_future`` x ``amt_annuity`` of the previous application (null for orphan
    contracts and for months without a balance row).
    """
    balances = _balance_rows(
        pos_cash_balance,
        CONTRACT_TYPE_POS_CASH,
        cnt_instalment_future=F.col("cnt_instalment_future"),
        amt_balance=F.lit(None).cast(AMOUNT_TYPE),
    ).unionByName(
        _balance_rows(
            credit_card_balance,
            CONTRACT_TYPE_CREDIT_CARD,
            cnt_instalment_future=F.lit(None).cast("int"),
            amt_balance=F.col("amt_balance"),
        )
    )
    annuities = previous_application.select("sk_id_prev", "amt_annuity")
    computed = installment_dpd.select(
        "sk_id_prev",
        "month",
        F.col("sk_id_curr").alias("_installment_sk_id_curr"),
        "installment_dpd",
        F.col("has_previous_application").alias("_installment_has_previous_application"),
    )
    has_balance = F.col("contract_type").isNotNull()
    estimated_exposure = F.col("cnt_instalment_future") * F.col("amt_annuity")
    return (
        balances.join(computed, on=["sk_id_prev", "month"], how="full")
        .join(annuities, on="sk_id_prev", how="left")
        .select(
            "sk_id_prev",
            F.coalesce("sk_id_curr", "_installment_sk_id_curr").alias("sk_id_curr"),
            "month",
            "contract_type",
            "name_contract_status",
            "source_dpd",
            "source_dpd_def",
            "installment_dpd",
            F.coalesce("source_dpd", "installment_dpd").alias("dpd"),
            F.when(F.col("source_dpd").isNotNull(), "balance")
            .when(F.col("installment_dpd").isNotNull(), "installments")
            .alias("dpd_source"),
            F.when(
                has_balance,
                F.coalesce(
                    F.col("name_contract_status").isin(*ACTIVE_CONTRACT_STATUSES), F.lit(False)
                ),
            )
            .otherwise(F.lit(True))
            .alias("is_active"),
            F.when(F.col("contract_type") == CONTRACT_TYPE_CREDIT_CARD, F.col("amt_balance"))
            .when(F.col("contract_type") == CONTRACT_TYPE_POS_CASH, estimated_exposure)
            .cast(AMOUNT_TYPE)
            .alias("exposure_amount"),
            F.coalesce("has_previous_application", "_installment_has_previous_application").alias(
                "has_previous_application"
            ),
        )
    )


def bureau_client_month(bureau_balance: DataFrame, bureau: DataFrame) -> DataFrame:
    """Aggregate the monthly credit bureau statuses to the client.

    Grain: one row per ``sk_id_curr`` x ``month`` (key). Bureau statuses are ordinal buckets,
    not day counts, so they stay in their own columns: ``bureau_max_dpd_bucket`` (null when no
    contract has a known open status), ``bureau_default_flag`` (bucket >= 4, i.e. 91+ days) and
    ``n_bureau_active_contracts`` (contracts with status 0..5; closed and unknown are not
    counted). Orphan statuses (no ``bureau`` row, hence no client) are left out.
    """
    clients = bureau.select("sk_id_bureau", "sk_id_curr")
    return (
        bureau_balance.join(clients, on="sk_id_bureau", how="inner")
        .groupBy("sk_id_curr", "month")
        .agg(
            F.max("dpd_bucket").alias("bureau_max_dpd_bucket"),
            F.count("dpd_bucket").cast("int").alias("n_bureau_active_contracts"),
        )
        .withColumn(
            "bureau_default_flag",
            F.coalesce(F.col("bureau_max_dpd_bucket") >= BUREAU_DEFAULT_BUCKET, F.lit(False)),
        )
    )


def build_client_month(
    contract_month: DataFrame, bureau_month: DataFrame, macro_month: DataFrame
) -> DataFrame:
    """Build the client-month table with the 90+ DPD default flag.

    Grain: one row per ``sk_id_curr`` x ``month`` (key), for every month in which the client
    has a Home Credit contract row or a credit bureau status. ``max_dpd`` is the maximum DPD
    over the client's Home Credit contracts (null without such a contract in the month);
    ``default_flag`` = ``max_dpd`` >= 90. ``default_start_month`` is the first month of the
    current uninterrupted default episode (null outside default); a month without default or
    without any row ends the episode. It only looks backwards, like every other column: month M
    uses data up to M. ``exposure_amount`` sums the active contracts. Bureau columns come from
    ``bureau_client_month`` and do not feed ``default_flag``. The ``macro_*`` columns of
    ``macro_month`` (one row per ``month``, see ``transformations/macro.py``) are attached by
    month and are null for months it does not cover.
    """
    is_active = F.col("is_active")
    contracts = contract_month.groupBy("sk_id_curr", "month").agg(
        F.max("dpd").alias("max_dpd"),
        F.max("source_dpd_def").alias("max_dpd_def"),
        F.sum(F.when(is_active, F.col("exposure_amount"))).cast(AMOUNT_TYPE).alias("exposure"),
        F.count_if(is_active).cast("int").alias("n_active"),
        F.count("*").cast("int").alias("n_contracts"),
    )
    df = contracts.join(bureau_month, on=["sk_id_curr", "month"], how="full").withColumn(
        "default_flag", F.coalesce(F.col("max_dpd") >= DEFAULT_DPD_THRESHOLD, F.lit(False))
    )

    history = Window.partitionBy("sk_id_curr").orderBy("month")
    continues_episode = F.lag("default_flag").over(history) & (
        F.lag("month").over(history) == F.add_months("month", -1)
    )
    # Window functions cannot be nested, so the episode starts are materialized first.
    df = df.withColumn(
        "_episode_start",
        F.when(
            F.col("default_flag") & ~F.coalesce(continues_episode, F.lit(False)), F.col("month")
        ),
    )
    current_episode_start = F.last("_episode_start", ignorenulls=True).over(
        history.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    client_month = df.select(
        "sk_id_curr",
        "month",
        "max_dpd",
        "max_dpd_def",
        dpd_bucket(F.col("max_dpd")).alias("dpd_bucket"),
        "default_flag",
        F.when(F.col("default_flag"), current_episode_start).alias("default_start_month"),
        F.col("exposure").alias("exposure_amount"),
        F.coalesce("n_active", F.lit(0)).alias("n_active_contracts"),
        F.coalesce("n_contracts", F.lit(0)).alias("n_contracts"),
        "bureau_max_dpd_bucket",
        F.coalesce("bureau_default_flag", F.lit(False)).alias("bureau_default_flag"),
        F.coalesce("n_bureau_active_contracts", F.lit(0)).alias("n_bureau_active_contracts"),
    )
    macro_columns = [name for name in macro_month.columns if name != "month"]
    return client_month.join(macro_month, on="month", how="left").select(
        *client_month.columns, *macro_columns
    )
