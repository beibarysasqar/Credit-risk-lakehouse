"""Days past due (DPD) of the previous Home Credit loans, computed from the installment payments.

All dates are synthetic (see ``transformations/calendar.py``). DPD of a month uses only payments
dated up to the end of that month, so the result is point-in-time correct by construction.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from credit_risk.common.config import AMOUNT_TYPE, DPD_BUCKETS, OBSERVATION_END_OFFSET_DAYS
from credit_risk.transformations.calendar import to_calendar_date

INSTALMENT_KEY = ["sk_id_prev", "num_instalment_number", "num_instalment_version"]


def aggregate_installments(payments: DataFrame) -> DataFrame:
    """Collapse the partial payments of ``silver.installments_payments`` into installments.

    Grain: one row per ``sk_id_prev`` x ``num_instalment_number`` x ``num_instalment_version``
    (key). ``paid_in_full_date`` is the first payment date on which the running sum of payments
    covers ``amt_instalment`` (null while it does not); ``dpd_at_settlement`` is the number of
    days between the due date and that date, never negative. ``is_paid_in_full`` is also true
    for an installment with nothing to pay.
    """
    paid_so_far = F.sum("amt_payment").over(
        Window.partitionBy(*INSTALMENT_KEY).orderBy("entry_payment_date")
    )
    covered = paid_so_far >= F.col("amt_instalment")
    installments = (
        payments.withColumn("_covered_on", F.when(covered, F.col("entry_payment_date")))
        .groupBy(*INSTALMENT_KEY)
        .agg(
            F.min("sk_id_curr").alias("sk_id_curr"),
            F.min("days_instalment").alias("days_instalment"),
            F.min("instalment_date").alias("instalment_date"),
            F.max("amt_instalment").alias("amt_instalment"),
            F.coalesce(F.sum("amt_payment"), F.lit(0)).cast(AMOUNT_TYPE).alias("amt_paid"),
            F.count("entry_payment_date").cast("int").alias("n_payments"),
            F.min("entry_payment_date").alias("first_payment_date"),
            F.max("entry_payment_date").alias("last_payment_date"),
            F.min("_covered_on").alias("paid_in_full_date"),
            F.bool_or("has_previous_application").alias("has_previous_application"),
        )
    )
    days_late = F.datediff(F.col("paid_in_full_date"), F.col("instalment_date"))
    return installments.withColumns(
        {
            "is_paid_in_full": F.coalesce(
                F.col("amt_paid") >= F.col("amt_instalment"), F.lit(False)
            ),
            # greatest() skips nulls, so an unsettled installment needs the explicit guard.
            "dpd_at_settlement": F.when(days_late.isNotNull(), F.greatest(days_late, F.lit(0))),
        }
    ).select(
        "sk_id_prev",
        "sk_id_curr",
        "num_instalment_number",
        "num_instalment_version",
        "days_instalment",
        "instalment_date",
        "amt_instalment",
        "amt_paid",
        "n_payments",
        "first_payment_date",
        "last_payment_date",
        "paid_in_full_date",
        "is_paid_in_full",
        "dpd_at_settlement",
        "has_previous_application",
    )


def installment_dpd_by_month(installments: DataFrame) -> DataFrame:
    """Compute the maximum installment DPD of every contract in every month.

    Grain: one row per ``sk_id_prev`` x ``month`` (key), for every month in which an installment
    of the contract falls due or is still overdue. An installment is overdue from its due date
    until ``paid_in_full_date``, or until the last observed day when it was never paid in full.
    In a month it counts with the DPD reached on the last day of the month, or on the settlement
    day if that comes first; ``installment_dpd`` is the maximum over the installments.
    """
    due = F.col("instalment_date")
    observation_end = to_calendar_date(F.lit(OBSERVATION_END_OFFSET_DAYS))
    overdue_end = F.greatest(
        due,
        F.when(F.col("is_paid_in_full"), F.coalesce(F.col("paid_in_full_date"), due)).otherwise(
            observation_end
        ),
    )
    months = F.sequence(
        F.trunc(due, "month"), F.trunc(overdue_end, "month"), F.expr("interval 1 month")
    )
    dpd = F.datediff(F.least(F.last_day("month"), F.col("_overdue_end")), due)
    return (
        installments.withColumn("_overdue_end", overdue_end)
        .withColumn("month", F.explode(months))
        .groupBy("sk_id_prev", "month")
        .agg(
            F.min("sk_id_curr").alias("sk_id_curr"),
            F.max(dpd).alias("installment_dpd"),
            F.bool_or("has_previous_application").alias("has_previous_application"),
        )
    )


def dpd_bucket(dpd: Column) -> Column:
    """Map a DPD day count to its bucket label (``0``, ``1-29``, ..., ``90+``); null stays null."""
    bucket = F.when(dpd.isNull(), F.lit(None).cast("string"))
    for lower_edge, label in reversed(DPD_BUCKETS):
        bucket = bucket.when(dpd >= lower_edge, F.lit(label))
    return bucket
