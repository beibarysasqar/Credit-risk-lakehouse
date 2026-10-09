from collections.abc import Callable
from datetime import date
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from credit_risk.common.expectations import DERIVED_RULES
from credit_risk.transformations.gold import (
    build_client_month,
    bureau_client_month,
)

Table = Callable[[str], DataFrame]

JAN, FEB, MAR, APR, MAY = (date(2018, month, 1) for month in range(1, 6))

CONTRACT_MONTH_SCHEMA = (
    "sk_id_prev BIGINT, sk_id_curr BIGINT, month DATE, dpd INT, source_dpd_def INT, "
    "is_active BOOLEAN, exposure_amount DECIMAL(18,3)"
)
BUREAU_MONTH_SCHEMA = (
    "sk_id_curr BIGINT, month DATE, bureau_max_dpd_bucket INT, "
    "n_bureau_active_contracts INT, bureau_default_flag BOOLEAN"
)


def contract_months(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """Build ``gold.contract_month`` rows from (sk_id_prev, sk_id_curr, month, dpd)."""
    return spark.createDataFrame([(*row, None, True, None) for row in rows], CONTRACT_MONTH_SCHEMA)


def no_bureau(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([], BUREAU_MONTH_SCHEMA)


def test_build_contract_month(spark: SparkSession, derived: Table) -> None:
    result = derived("contract_month")

    expected = spark.createDataFrame(
        [
            # Installments only (no balance row): computed DPD is used, no exposure.
            (1001, 100001, JAN, None, None, 0, 0, "installments", True, None, True),
            # The balance status is unknown (XNA), so the contract does not count as active.
            (1001, 100001, FEB, "pos_cash", 0, 28, 0, "balance", False, Decimal("20765.16"), True),
            # Source DPD wins over the computed one; both are kept for reconciliation.
            (1001, 100001, MAR, "pos_cash", 95, 59, 95, "balance", True, Decimal("20765.16"), True),
            # 11 future installments x annuity 1730.43.
            (1001, 100001, APR, "pos_cash", 0, 89, 0, "balance", True, Decimal("19034.73"), True),
            # Card exposure is the source balance, even when the card is overpaid.
            (
                1003,
                100002,
                MAR,
                "credit_card",
                90,
                None,
                90,
                "balance",
                True,
                Decimal("-45.675"),
                True,
            ),
            (1003, 100002, APR, "credit_card", 0, None, 0, "balance", True, Decimal("56970"), True),
            # Orphan contract: no previous application, hence no annuity and no exposure.
            (9999, 100001, APR, "pos_cash", 0, 0, 0, "balance", True, None, False),
        ],
        "sk_id_prev BIGINT, sk_id_curr BIGINT, month DATE, contract_type STRING, source_dpd INT, "
        "installment_dpd INT, dpd INT, dpd_source STRING, is_active BOOLEAN, "
        "exposure_amount DECIMAL(18,3), has_previous_application BOOLEAN",
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)

    # The reconciliation rule catches the month where source and computed DPD disagree on 90+.
    mismatch = DERIVED_RULES["contract_month"].warn["installment_dpd_default_matches_source"]
    assert [row.month for row in result.filter(f"NOT ({mismatch})").collect()] == [MAR]


def test_bureau_client_month(spark: SparkSession, silver_table: Table) -> None:
    result = bureau_client_month(silver_table("bureau_balance"), silver_table("bureau"))

    expected = spark.createDataFrame(
        [
            (100001, MAR, 1, 1, False),
            # Bucket 5 on one contract, bucket 0 on the other.
            (100001, APR, 5, 2, True),
            # Closed and unknown statuses: no bucket, no active contract.
            (100001, MAY, None, 0, False),
        ],
        BUREAU_MONTH_SCHEMA,
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)


def test_bureau_default_starts_at_bucket_4(spark: SparkSession) -> None:
    statuses = spark.createDataFrame(
        [(1, JAN, 3), (2, JAN, 4)], "sk_id_bureau BIGINT, month DATE, dpd_bucket INT"
    )
    bureau = spark.createDataFrame([(1, 10), (2, 20)], "sk_id_bureau BIGINT, sk_id_curr BIGINT")

    result = bureau_client_month(statuses, bureau)

    flags = {row.sk_id_curr: row.bureau_default_flag for row in result.collect()}
    assert flags == {10: False, 20: True}


def test_build_client_month(spark: SparkSession, derived: Table) -> None:
    result = derived("client_month")

    expected = spark.createDataFrame(
        [
            (100001, JAN, 0, None, "0", False, None, None, 1, 1, None, False, 0),
            # Only inactive contract rows: no active exposure.
            (100001, FEB, 0, 0, "0", False, None, None, 0, 1, None, False, 0),
            (100001, MAR, 95, 95, "90+", True, MAR, Decimal("20765.16"), 1, 1, 1, False, 1),
            # Cured in April; the bureau default does not set default_flag.
            (100001, APR, 0, 0, "0", False, None, Decimal("19034.73"), 2, 2, 5, True, 2),
            # Bureau status only: no Home Credit contract row in the month.
            (100001, MAY, None, None, None, False, None, None, 0, 0, None, False, 0),
            (100002, MAR, 90, 89, "90+", True, MAR, Decimal("-45.675"), 1, 1, None, False, 0),
            (100002, APR, 0, 0, "0", False, None, Decimal("56970"), 1, 1, None, False, 0),
        ],
        "sk_id_curr BIGINT, month DATE, max_dpd INT, max_dpd_def INT, dpd_bucket STRING, "
        "default_flag BOOLEAN, default_start_month DATE, exposure_amount DECIMAL(18,3), "
        "n_active_contracts INT, n_contracts INT, bureau_max_dpd_bucket INT, "
        "bureau_default_flag BOOLEAN, n_bureau_active_contracts INT",
    )
    assert result.columns == expected.columns
    assertDataFrameEqual(result, expected)


def test_max_dpd_is_taken_over_contracts_at_the_89_90_boundary(spark: SparkSession) -> None:
    rows = [(1, 10, JAN, 89), (2, 10, JAN, 30), (3, 20, JAN, 89), (4, 20, JAN, 90)]

    result = build_client_month(contract_months(spark, rows), no_bureau(spark))

    expected = spark.createDataFrame(
        [(10, 89, "60-89", False, 2), (20, 90, "90+", True, 2)],
        "sk_id_curr BIGINT, max_dpd INT, dpd_bucket STRING, default_flag BOOLEAN, n_contracts INT",
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)


def test_default_episodes(spark: SparkSession) -> None:
    dec = date(2017, 12, 1)
    jun, aug = date(2018, 6, 1), date(2018, 8, 1)
    rows = [
        # Episode across a year boundary, cure, re-default, then a missing month (July)
        # that also ends the episode.
        (1, 10, dec, 95),
        (1, 10, JAN, 126),
        (1, 10, FEB, 0),
        (1, 10, MAR, 100),
        (1, 10, APR, 130),
        (1, 10, MAY, 161),
        (1, 10, jun, 191),
        (1, 10, aug, 252),
    ]

    result = build_client_month(contract_months(spark, rows), no_bureau(spark))

    expected = spark.createDataFrame(
        [
            (dec, True, dec),
            (JAN, True, dec),
            (FEB, False, None),
            (MAR, True, MAR),
            (APR, True, MAR),
            (MAY, True, MAR),
            (jun, True, MAR),
            (aug, True, aug),
        ],
        "month DATE, default_flag BOOLEAN, default_start_month DATE",
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)


def test_client_month_has_no_look_ahead(spark: SparkSession) -> None:
    """Changing what happens after a month must not change that month."""
    past = [(1, 10, JAN, 95), (1, 10, FEB, 126)]
    futures = ([], [(1, 10, MAR, 0)], [(1, 10, MAR, 154), (2, 10, APR, 400)])

    def history(future: list[tuple]) -> DataFrame:
        result = build_client_month(contract_months(spark, past + future), no_bureau(spark))
        return result.filter(F.col("month") <= FEB)

    baseline = history(futures[0])
    assert baseline.count() == 2
    for future in futures[1:]:
        assertDataFrameEqual(history(future), baseline)
