import pytest

from credit_risk.common.config import SILVER_ENTITIES
from credit_risk.common.expectations import SILVER_RULES

pytestmark = pytest.mark.integration

BRONZE_SOURCES = {entity: [entity] for entity in SILVER_ENTITIES} | {
    "application": ["application_train", "application_test"]
}

KEYS = {
    "application": "sk_id_curr",
    "bureau": "sk_id_bureau",
    "bureau_balance": "sk_id_bureau, months_balance",
    "previous_application": "sk_id_prev",
    "pos_cash_balance": "sk_id_prev, months_balance",
    "credit_card_balance": "sk_id_prev, months_balance",
    # No key narrower than the whole source row: partial payments.
    "installments_payments": (
        "sk_id_prev, sk_id_curr, num_instalment_version, num_instalment_number, "
        "days_instalment, days_entry_payment, amt_instalment, amt_payment"
    ),
}

# Rows whose parent contract is missing from the Kaggle sample (profiled on the source files).
ORPHAN_ROWS = {
    "bureau_balance": ("has_bureau_record", 3_120_184),
    "pos_cash_balance": ("has_previous_application", 340_561),
    "credit_card_balance": ("has_previous_application", 1_082_816),
    "installments_payments": ("has_previous_application", 1_250_826),
}

PREVIOUS_APPLICATION_SENTINELS = {
    "days_first_drawing_anomaly": 934_444,
    "days_first_due_anomaly": 40_645,
    "days_last_due_1st_version_anomaly": 93_864,
    "days_last_due_anomaly": 211_221,
    "days_termination_anomaly": 225_913,
    "sellerplace_area_anomaly": 762_675,
}

TOKEN_COLUMNS = {
    "application": ["code_gender", "organization_type"],
    "previous_application": ["name_cash_loan_purpose", "code_reject_reason", "name_contract_type"],
    "pos_cash_balance": ["name_contract_status"],
}


@pytest.mark.parametrize("entity", SILVER_ENTITIES)
def test_silver_and_quarantine_cover_bronze(entity: str, sql) -> None:
    bronze = " + ".join(f"(SELECT count(*) FROM bronze.{e})" for e in BRONZE_SOURCES[entity])
    [[bronze_rows, silver_rows, quarantined, rescued]] = sql(
        f"""
        SELECT
          {bronze},
          (SELECT count(*) FROM silver.{entity}),
          (SELECT count(*) FROM silver.{entity}_quarantine),
          (SELECT count(_rescued_data) FROM silver.{entity})
        """
    )
    assert int(silver_rows) + int(quarantined) == int(bronze_rows)
    assert int(rescued) == 0


@pytest.mark.parametrize("entity", SILVER_ENTITIES)
def test_silver_key_is_unique(entity: str, sql) -> None:
    [[duplicates]] = sql(
        f"SELECT count(*) FROM (SELECT 1 FROM silver.{entity} GROUP BY {KEYS[entity]} "
        "HAVING count(*) > 1)"
    )
    assert int(duplicates) == 0


CHILDREN_OF_APPLICATION = [
    "bureau",
    "previous_application",
    "pos_cash_balance",
    "credit_card_balance",
    "installments_payments",
]


@pytest.mark.parametrize("entity", CHILDREN_OF_APPLICATION)
def test_every_client_has_an_application(entity: str, sql) -> None:
    [[orphans]] = sql(
        f"SELECT count(*) FROM silver.{entity} c LEFT ANTI JOIN silver.application a "
        "ON c.sk_id_curr = a.sk_id_curr"
    )
    assert int(orphans) == 0


def test_application_target_and_sentinels(sql) -> None:
    [[train_rows, test_targets, default_rate, employed_anomalies, employed_sentinels]] = sql(
        """
        SELECT
          count_if(is_train),
          count_if(NOT is_train AND target IS NOT NULL),
          avg(CASE WHEN is_train THEN target END),
          count_if(days_employed_anomaly),
          count_if(days_employed = 365243)
        FROM silver.application
        """
    )
    assert int(train_rows) == 307_511
    assert int(test_targets) == 0
    assert float(default_rate) == pytest.approx(0.0807, abs=0.0005)
    assert int(employed_anomalies) == 64_648
    assert int(employed_sentinels) == 0


def test_previous_application_sentinels(sql) -> None:
    flags = ", ".join(f"count_if({flag})" for flag in PREVIOUS_APPLICATION_SENTINELS)
    [counts] = sql(f"SELECT {flags} FROM silver.previous_application")
    assert dict(zip(PREVIOUS_APPLICATION_SENTINELS, map(int, counts), strict=True)) == (
        PREVIOUS_APPLICATION_SENTINELS
    )


@pytest.mark.parametrize("entity", list(TOKEN_COLUMNS))
def test_no_xna_xap_tokens_left(entity: str, sql) -> None:
    checks = ", ".join(f"count_if({c} IN ('XNA', 'XAP'))" for c in TOKEN_COLUMNS[entity])
    [counts] = sql(f"SELECT {checks} FROM silver.{entity}")
    assert [int(count) for count in counts] == [0] * len(TOKEN_COLUMNS[entity])


@pytest.mark.parametrize("entity", list(ORPHAN_ROWS))
def test_orphan_contracts_are_flagged(entity: str, sql) -> None:
    flag, expected = ORPHAN_ROWS[entity]
    [[orphans]] = sql(f"SELECT count_if(NOT {flag}) FROM silver.{entity}")
    assert int(orphans) == expected


def test_bureau_balance_status_mapping(sql) -> None:
    rows = sql(
        "SELECT status, dpd_bucket, is_closed, is_unknown, count(*) FROM silver.bureau_balance "
        "GROUP BY ALL ORDER BY status"
    )
    assert [row[:4] for row in rows] == [
        *[[str(bucket), str(bucket), "false", "false"] for bucket in range(6)],
        ["C", None, "true", "false"],
        ["X", None, "false", "true"],
    ]


def test_expectations_of_latest_update(failed_expectations) -> None:
    """Every rule was evaluated in the latest update and no fail rule was broken."""
    for entity, rules in SILVER_RULES.items():
        for name in [*rules.fail, *rules.drop, *rules.warn]:
            assert (entity, name) in failed_expectations, f"{entity}.{name} missing from the log"
        for name in rules.fail:
            assert failed_expectations[entity, name] == 0
