import pytest

from credit_risk.common.expectations import DERIVED_RULES

pytestmark = pytest.mark.integration

KEYS = {
    "silver.installments": "sk_id_prev, num_instalment_number, num_instalment_version",
    "gold.contract_month": "sk_id_prev, month",
    "gold.client_month": "sk_id_curr, month",
}

# Rows with SK_DPD >= 90 in the balance tables (profiled on the source files).
SOURCE_DEFAULT_ROWS = {"pos_cash": 119_274, "credit_card": 48_377}


@pytest.mark.parametrize("table", list(KEYS))
def test_key_is_unique(table: str, sql) -> None:
    [[duplicates]] = sql(
        f"SELECT count(*) FROM (SELECT 1 FROM {table} GROUP BY {KEYS[table]} HAVING count(*) > 1)"
    )
    assert int(duplicates) == 0


@pytest.mark.parametrize("table", list(KEYS))
def test_every_client_has_an_application(table: str, sql) -> None:
    [[orphans]] = sql(
        f"SELECT count(*) FROM {table} c LEFT ANTI JOIN silver.application a "
        "ON c.sk_id_curr = a.sk_id_curr"
    )
    assert int(orphans) == 0


def test_installments_cover_all_payments(sql) -> None:
    [[installments, payments, paid, paid_source]] = sql(
        """
        SELECT
          (SELECT count(*) FROM silver.installments),
          (SELECT sum(n_payments) FROM silver.installments),
          (SELECT sum(amt_paid) FROM silver.installments),
          (SELECT sum(amt_payment) FROM silver.installments_payments)
        """
    )
    assert int(installments) == 12_951_918
    assert paid == paid_source
    [[payments_source]] = sql("SELECT count(days_entry_payment) FROM silver.installments_payments")
    assert int(payments) == int(payments_source)


def test_contract_month_keeps_every_balance_row_and_its_source_dpd(sql) -> None:
    rows = sql(
        """
        SELECT contract_type, count(*), count_if(source_dpd >= 90), count_if(dpd <> source_dpd)
        FROM gold.contract_month WHERE contract_type IS NOT NULL GROUP BY ALL
        """
    )
    [[pos_rows, card_rows]] = sql(
        "SELECT (SELECT count(*) FROM silver.pos_cash_balance), "
        "(SELECT count(*) FROM silver.credit_card_balance)"
    )
    by_type = {contract_type: list(map(int, counts)) for contract_type, *counts in rows}
    assert by_type == {
        "pos_cash": [int(pos_rows), SOURCE_DEFAULT_ROWS["pos_cash"], 0],
        "credit_card": [int(card_rows), SOURCE_DEFAULT_ROWS["credit_card"], 0],
    }


def test_client_month_covers_exactly_its_sources(sql) -> None:
    [[client_months, source_months]] = sql(
        """
        SELECT
          (SELECT count(*) FROM gold.client_month),
          (SELECT count(*) FROM (
             SELECT sk_id_curr, month FROM gold.contract_month
             UNION
             SELECT b.sk_id_curr, s.month FROM silver.bureau_balance s
             JOIN silver.bureau b ON s.sk_id_bureau = b.sk_id_bureau))
        """
    )
    assert int(client_months) == int(source_months)


def test_default_flag_follows_the_definition(sql) -> None:
    [[wrong_flag, wrong_start, wrong_max, defaults]] = sql(
        """
        WITH expected AS (
          SELECT sk_id_curr, month, max(dpd) AS max_dpd FROM gold.contract_month GROUP BY ALL
        )
        SELECT
          count_if(c.default_flag <> coalesce(c.max_dpd >= 90, false)),
          count_if(c.default_flag <> (c.default_start_month IS NOT NULL)
                   OR c.default_start_month > c.month),
          count_if(NOT (c.max_dpd <=> e.max_dpd)),
          count_if(c.default_flag)
        FROM gold.client_month c LEFT JOIN expected e USING (sk_id_curr, month)
        """
    )
    assert [int(wrong_flag), int(wrong_start), int(wrong_max)] == [0, 0, 0]
    assert int(defaults) > 0


def test_default_start_month_is_the_start_of_the_episode(sql) -> None:
    """The month before an episode start is not in default; inside an episode it is."""
    [[bad_starts, bad_continuations]] = sql(
        """
        SELECT
          count_if(c.default_start_month = c.month AND p.default_flag),
          count_if(c.default_start_month < c.month
                   AND NOT (p.default_flag AND p.default_start_month <=> c.default_start_month))
        FROM gold.client_month c LEFT JOIN gold.client_month p
          ON p.sk_id_curr = c.sk_id_curr AND p.month = add_months(c.month, -1)
        WHERE c.default_flag
        """
    )
    assert [int(bad_starts), int(bad_continuations)] == [0, 0]


def test_expectations_of_latest_update(failed_expectations) -> None:
    """Every rule was evaluated in the latest update and no fail rule was broken."""
    for entity, rules in DERIVED_RULES.items():
        for name in [*rules.fail, *rules.warn]:
            assert (entity, name) in failed_expectations, f"{entity}.{name} missing from the log"
        for name in rules.fail:
            assert failed_expectations[entity, name] == 0
