from collections.abc import Callable
from datetime import date, timedelta
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.testing import assertDataFrameEqual

from credit_risk.common.config import ANCHOR_DATE
from credit_risk.common.expectations import SILVER_RULES
from credit_risk.transformations.quality import quarantine_rows

Prepared = Callable[[str], DataFrame]


def day(offset: int) -> date:
    return ANCHOR_DATE + timedelta(days=offset)


def test_clean_application(spark: SparkSession, prepared: Prepared) -> None:
    result = prepared("application")

    assert result.columns[:3] == ["sk_id_curr", "is_train", "target"]
    expected = spark.createDataFrame(
        [
            (100001, True, 1, "M", "Business Entity Type 3", -637, False, Decimal("24700.5"), 1),
            # Duplicate source row collapsed; XNA and the 365243 sentinel become null.
            (100002, True, 0, None, None, None, True, Decimal("6750"), 3),
            # Test rows carry no target.
            (100003, False, None, "F", "Kindergarten", -2329, False, Decimal("20560.5"), 2),
        ],
        "sk_id_curr BIGINT, is_train BOOLEAN, target INT, code_gender STRING, "
        "organization_type STRING, days_employed INT, days_employed_anomaly BOOLEAN, "
        "amt_annuity DECIMAL(18,3), cnt_fam_members INT",
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)
    assert quarantine_rows(result, SILVER_RULES["application"].drop).count() == 0


def test_clean_bureau(spark: SparkSession, prepared: Prepared) -> None:
    result = prepared("bureau")

    expected = spark.createDataFrame(
        [
            (5001, -153, day(-497), day(-153), day(-153), Decimal("0"), None),
            # Float noise of the source is rounded to 3 decimals; far-future end date is kept.
            (5002, 31199, day(-31), day(31199), None, Decimal("171342"), None),
        ],
        "sk_id_bureau BIGINT, days_credit_enddate INT, credit_date DATE, credit_end_date DATE, "
        "credit_end_fact_date DATE, amt_credit_sum_debt DECIMAL(18,3), amt_annuity DECIMAL(18,3)",
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)


def test_clean_bureau_balance(spark: SparkSession, prepared: Prepared) -> None:
    result = prepared("bureau_balance")

    schema = (
        "sk_id_bureau BIGINT, months_balance INT, status STRING, month DATE, dpd_bucket INT, "
        "is_closed BOOLEAN, is_unknown BOOLEAN, has_bureau_record BOOLEAN"
    )
    expected = spark.createDataFrame(
        [
            (5001, 0, "C", date(2018, 5, 1), None, True, False, True),
            (5001, -1, "0", date(2018, 4, 1), 0, False, False, True),
            (5001, -2, "1", date(2018, 3, 1), 1, False, False, True),
            (5002, 0, "X", date(2018, 5, 1), None, False, True, True),
            (5002, -1, "5", date(2018, 4, 1), 5, False, False, True),
            (5002, -2, "9", date(2018, 3, 1), None, False, False, True),
            (5002, 1, "0", date(2018, 6, 1), 0, False, False, True),
            # Contract missing from bureau.
            (5999, 0, "0", date(2018, 5, 1), 0, False, False, False),
        ],
        schema,
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)

    quarantined = quarantine_rows(result, SILVER_RULES["bureau_balance"].drop)
    assertDataFrameEqual(
        quarantined.select("sk_id_bureau", "months_balance", "_failed_rules"),
        spark.createDataFrame(
            [(5002, -2, ["status_in_domain"]), (5002, 1, ["months_balance_not_positive"])],
            "sk_id_bureau BIGINT, months_balance INT, _failed_rules ARRAY<STRING>",
        ),
    )


def test_clean_previous_application(spark: SparkSession, prepared: Prepared) -> None:
    result = prepared("previous_application")

    expected = spark.createDataFrame(
        [
            (1001, None, None, True, -37, False, 35, False, 12, Decimal("1730.43"), day(-73)),
            (1002, None, None, False, None, False, None, True, None, None, day(-14)),
            (1003, None, -290, False, None, True, None, True, 0, Decimal("2250"), day(-300)),
        ],
        "sk_id_prev BIGINT, name_cash_loan_purpose STRING, days_first_drawing INT, "
        "days_first_drawing_anomaly BOOLEAN, days_termination INT, "
        "days_termination_anomaly BOOLEAN, sellerplace_area INT, "
        "sellerplace_area_anomaly BOOLEAN, cnt_payment INT, amt_annuity DECIMAL(18,3), "
        "decision_date DATE",
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)
    assert result.filter("code_reject_reason IS NOT NULL").count() == 0


def test_clean_pos_cash_balance(spark: SparkSession, prepared: Prepared) -> None:
    result = prepared("pos_cash_balance")

    expected = spark.createDataFrame(
        [
            (1001, -1, date(2018, 4, 1), "Active", 0, 0, 12, True),
            # Source DPD values are kept as delivered.
            (1001, -2, date(2018, 3, 1), "Active", 95, 95, 12, True),
            (1001, -3, date(2018, 2, 1), None, 0, 0, 12, True),
            (1001, -4, date(2018, 1, 1), "Active", -5, 0, 12, True),
            (9999, -1, date(2018, 4, 1), "Active", 0, 0, 6, False),
        ],
        "sk_id_prev BIGINT, months_balance INT, month DATE, name_contract_status STRING, "
        "sk_dpd INT, sk_dpd_def INT, cnt_instalment INT, has_previous_application BOOLEAN",
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)

    quarantined = quarantine_rows(result, SILVER_RULES["pos_cash_balance"].drop)
    assert [row.months_balance for row in quarantined.collect()] == [-4]


def test_clean_credit_card_balance(spark: SparkSession, prepared: Prepared) -> None:
    result = prepared("credit_card_balance")

    assert "amt_recivable" not in result.columns
    expected = spark.createDataFrame(
        [
            (1003, -1, date(2018, 4, 1), Decimal("56970"), Decimal("1700.325"), 0, 0, 0, True),
            (1003, -2, date(2018, 3, 1), Decimal("-45.675"), Decimal("0"), None, 90, 89, True),
        ],
        "sk_id_prev BIGINT, months_balance INT, month DATE, amt_receivable DECIMAL(18,3), "
        "amt_inst_min_regularity DECIMAL(18,3), cnt_drawings_atm_current INT, sk_dpd INT, "
        "sk_dpd_def INT, has_previous_application BOOLEAN",
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)


def test_clean_installments_payments(spark: SparkSession, prepared: Prepared) -> None:
    result = prepared("installments_payments")

    expected = spark.createDataFrame(
        [
            # The exact duplicate of this source row is removed.
            (1001, 1, 1, -121, day(-120), day(-121), Decimal("1000"), True),
            # Partial payments of one installment stay separate rows.
            (1001, 1, 2, -90, day(-90), day(-90), Decimal("400"), True),
            (1001, 1, 2, -1, day(-90), day(-1), Decimal("600"), True),
            # Unpaid installment.
            (1001, 1, 3, None, day(-60), None, None, True),
            (1001, 1, None, -10, day(-10), day(-10), Decimal("5"), True),
            (9999, 0, 1, -30, day(-30), day(-30), Decimal("500"), False),
        ],
        "sk_id_prev BIGINT, num_instalment_version INT, num_instalment_number INT, "
        "days_entry_payment INT, instalment_date DATE, entry_payment_date DATE, "
        "amt_payment DECIMAL(18,3), has_previous_application BOOLEAN",
    )
    assertDataFrameEqual(result.select(*expected.columns), expected)

    quarantined = quarantine_rows(result, SILVER_RULES["installments_payments"].drop)
    assert [row._failed_rules for row in quarantined.collect()] == [["instalment_identified"]]
