from collections.abc import Callable
from datetime import date, timedelta
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from credit_risk.common.config import ANCHOR_DATE
from credit_risk.transformations.dpd import (
    aggregate_installments,
    dpd_bucket,
    installment_dpd_by_month,
)

SilverTable = Callable[[str], DataFrame]

PAYMENTS_SCHEMA = (
    "sk_id_prev BIGINT, sk_id_curr BIGINT, num_instalment_number INT, "
    "num_instalment_version INT, instalment_date DATE, entry_payment_date DATE, "
    "amt_instalment DECIMAL(18,3), amt_payment DECIMAL(18,3)"
)
MONTHLY_SCHEMA = "sk_id_prev BIGINT, month DATE, installment_dpd INT"


def day(offset: int) -> date:
    return ANCHOR_DATE + timedelta(days=offset)


def payments(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """Build payments shaped like ``silver.installments_payments`` (amounts given as strings)."""
    typed = [
        (*row[:6], Decimal(row[6]), None if row[7] is None else Decimal(row[7])) for row in rows
    ]
    return spark.createDataFrame(typed, PAYMENTS_SCHEMA).withColumns(
        {
            "days_instalment": F.datediff("instalment_date", F.lit(ANCHOR_DATE)),
            "has_previous_application": F.lit(True),
        }
    )


def monthly(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    return installment_dpd_by_month(aggregate_installments(payments(spark, rows))).select(
        "sk_id_prev", "month", "installment_dpd"
    )


def test_aggregate_installments(spark: SparkSession, silver_table: SilverTable) -> None:
    # The row without an installment number is quarantined in Silver and never gets here.
    result = aggregate_installments(silver_table("installments_payments"))

    expected = spark.createDataFrame(
        [
            # Paid a day early.
            (1001, 1, 1, day(-120), Decimal("1000"), 1, day(-121), day(-121), True, 0, True),
            # Paid in two parts: settled by the second payment, 89 days late.
            (1001, 2, 1, day(-90), Decimal("1000"), 2, day(-90), day(-1), True, 89, True),
            # Never paid.
            (1001, 3, 1, day(-60), Decimal("0"), 0, None, None, False, None, True),
            (9999, 1, 0, day(-30), Decimal("500"), 1, day(-30), day(-30), True, 0, False),
        ],
        "sk_id_prev BIGINT, num_instalment_number INT, num_instalment_version INT, "
        "instalment_date DATE, amt_paid DECIMAL(18,3), n_payments INT, first_payment_date DATE, "
        "paid_in_full_date DATE, is_paid_in_full BOOLEAN, dpd_at_settlement INT, "
        "has_previous_application BOOLEAN",
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)


def test_aggregate_installments_edge_cases(spark: SparkSession) -> None:
    rows = [
        # Overpaid by the first payment; the later payment does not move the settlement date.
        (1, 10, 1, 1, date(2018, 1, 10), date(2018, 1, 12), "1000", "1200"),
        (1, 10, 1, 1, date(2018, 1, 10), date(2018, 2, 1), "1000", "50"),
        # Two payments on the same day cover the installment together.
        (1, 10, 2, 1, date(2018, 2, 10), date(2018, 2, 15), "1000", "500"),
        (1, 10, 2, 1, date(2018, 2, 10), date(2018, 2, 15), "1000", "500.000"),
        # Underpaid: never settled.
        (1, 10, 3, 1, date(2018, 3, 10), date(2018, 3, 10), "1000", "999.999"),
        # A second version of installment 3 is a separate installment.
        (1, 10, 3, 2, date(2018, 3, 20), date(2018, 3, 20), "700", "700"),
        # Nothing to pay and no payment: settled, without a date.
        (1, 10, 4, 1, date(2018, 4, 10), None, "0", None),
    ]

    result = aggregate_installments(payments(spark, rows))

    expected = spark.createDataFrame(
        [
            (1, 1, Decimal("1250"), 2, date(2018, 1, 12), True, 2),
            (2, 1, Decimal("1000"), 2, date(2018, 2, 15), True, 5),
            (3, 1, Decimal("999.999"), 1, None, False, None),
            (3, 2, Decimal("700"), 1, date(2018, 3, 20), True, 0),
            (4, 1, Decimal("0"), 0, None, True, None),
        ],
        "num_instalment_number INT, num_instalment_version INT, amt_paid DECIMAL(18,3), "
        "n_payments INT, paid_in_full_date DATE, is_paid_in_full BOOLEAN, dpd_at_settlement INT",
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)


def test_installment_dpd_by_month_on_fixture(
    spark: SparkSession, silver_table: SilverTable
) -> None:
    result = installment_dpd_by_month(aggregate_installments(silver_table("installments_payments")))

    expected = spark.createDataFrame(
        [
            (1001, 100001, date(2018, 1, 1), 0, True),
            # Installment 2 (due Jan 31) stays open until Apr 30; installment 3 (due Mar 2,
            # never paid) is younger, so the oldest unpaid one sets the contract DPD.
            (1001, 100001, date(2018, 2, 1), 28, True),
            (1001, 100001, date(2018, 3, 1), 59, True),
            (1001, 100001, date(2018, 4, 1), 89, True),
            (9999, 100001, date(2018, 4, 1), 0, False),
        ],
        "sk_id_prev BIGINT, sk_id_curr BIGINT, month DATE, installment_dpd INT, "
        "has_previous_application BOOLEAN",
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)


def test_month_end_dpd_89_vs_90(spark: SparkSession) -> None:
    rows = [
        # Due Jan 1, never paid: 89 days on Mar 31, default only from April.
        (1, 10, 1, 1, date(2018, 1, 1), None, "100", None),
        # Due one day earlier: 90 days on Mar 31.
        (2, 10, 1, 1, date(2017, 12, 31), None, "100", None),
    ]

    result = monthly(spark, rows)

    expected = spark.createDataFrame(
        [
            (1, date(2018, 1, 1), 30),
            (1, date(2018, 2, 1), 58),
            (1, date(2018, 3, 1), 89),
            # Open installments are overdue up to the last observed day, Apr 30.
            (1, date(2018, 4, 1), 119),
            (2, date(2017, 12, 1), 0),
            (2, date(2018, 1, 1), 31),
            (2, date(2018, 2, 1), 59),
            (2, date(2018, 3, 1), 90),
            (2, date(2018, 4, 1), 120),
        ],
        MONTHLY_SCHEMA,
    )
    assertDataFrameEqual(result, expected)


def test_settlement_closes_the_overdue_interval(spark: SparkSession) -> None:
    rows = [
        # Due Jan 20, settled Mar 5: the settlement month counts up to the settlement day.
        (1, 10, 1, 1, date(2018, 1, 20), date(2018, 3, 5), "100", "100"),
        # Paid early: only the due month, no DPD.
        (1, 10, 2, 1, date(2018, 4, 20), date(2018, 3, 30), "100", "100"),
    ]

    result = monthly(spark, rows)

    expected = spark.createDataFrame(
        [
            (1, date(2018, 1, 1), 11),
            (1, date(2018, 2, 1), 39),
            (1, date(2018, 3, 1), 44),
            (1, date(2018, 4, 1), 0),
        ],
        MONTHLY_SCHEMA,
    )
    assertDataFrameEqual(result, expected)


def test_no_look_ahead(spark: SparkSession) -> None:
    """A month must not change when a payment made after it is moved or removed."""

    def history(settled_on: date | None) -> DataFrame:
        paid = None if settled_on is None else "100"
        rows = [(1, 10, 1, 1, date(2018, 1, 20), settled_on, "100", paid)]
        return monthly(spark, rows).filter(F.col("month") <= date(2018, 2, 1))

    expected = spark.createDataFrame(
        [(1, date(2018, 1, 1), 11), (1, date(2018, 2, 1), 39)], MONTHLY_SCHEMA
    )
    for settled_on in (date(2018, 3, 1), date(2018, 4, 25), None):
        assertDataFrameEqual(history(settled_on), expected)


def test_dpd_bucket(spark: SparkSession) -> None:
    days = [0, 1, 29, 30, 59, 60, 89, 90, 4231, None]
    df = spark.createDataFrame([(value,) for value in days], "dpd INT")

    result = df.select("dpd", dpd_bucket(F.col("dpd")).alias("bucket"))

    labels = ["0", "1-29", "1-29", "30-59", "30-59", "60-89", "60-89", "90+", "90+", None]
    expected = spark.createDataFrame(list(zip(days, labels, strict=True)), "dpd INT, bucket STRING")
    assertDataFrameEqual(result, expected)
