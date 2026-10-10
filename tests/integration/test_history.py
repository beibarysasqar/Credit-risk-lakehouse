import pytest

from credit_risk.common.config import (
    HISTORY_ENTITIES,
    VALIDATION_DATASET_TABLE,
    VALIDATION_SNAPSHOT_TABLE,
)

pytestmark = pytest.mark.integration

# A few tracked attributes per entity, compared between Silver and the open SCD2 version.
TRACKED_SAMPLE = {
    "application": ["is_train", "target", "amt_income_total", "name_family_status"],
    "previous_application": ["sk_id_curr", "amt_credit", "name_contract_status"],
}
# Bronze rows delivered by ingestion/demo_correction.py.
CORRECTED_CLIENTS = (
    "SELECT DISTINCT SK_ID_CURR AS sk_id_curr FROM bronze.application_train "
    "WHERE contains(_source_file, '_correction_')"
)


@pytest.mark.parametrize("entity", list(HISTORY_ENTITIES))
def test_every_key_has_exactly_one_open_version(entity: str, sql) -> None:
    key = HISTORY_ENTITIES[entity]
    [[broken]] = sql(
        f"SELECT count(*) FROM (SELECT {key} FROM history.{entity}_scd2 GROUP BY {key} "
        "HAVING count_if(__END_AT IS NULL) <> 1)"
    )
    assert int(broken) == 0


@pytest.mark.parametrize("entity", list(HISTORY_ENTITIES))
def test_versions_of_a_key_are_contiguous(entity: str, sql) -> None:
    key = HISTORY_ENTITIES[entity]
    [[broken]] = sql(
        f"""
        SELECT count(*) FROM (
          SELECT __START_AT, __END_AT,
                 lead(__START_AT) OVER (PARTITION BY {key} ORDER BY __START_AT) AS next_start
          FROM history.{entity}_scd2
        ) WHERE NOT (__END_AT <=> next_start) OR __END_AT <= __START_AT
        """
    )
    assert int(broken) == 0


@pytest.mark.parametrize("entity", list(HISTORY_ENTITIES))
def test_open_versions_equal_silver(entity: str, sql) -> None:
    key = HISTORY_ENTITIES[entity]
    same = " AND ".join(f"s.{column} <=> h.{column}" for column in TRACKED_SAMPLE[entity])
    [[silver_rows, open_rows, matching]] = sql(
        f"""
        SELECT
          (SELECT count(*) FROM silver.{entity}),
          (SELECT count(*) FROM history.{entity}_scd2 WHERE __END_AT IS NULL),
          (SELECT count(*) FROM silver.{entity} s JOIN history.{entity}_scd2 h
             ON s.{key} = h.{key} AND h.__END_AT IS NULL AND {same})
        """
    )
    assert int(silver_rows) == int(open_rows) == int(matching)


def test_only_corrected_clients_have_a_second_version(sql) -> None:
    [[corrected, unexpected, missing, wrong_income]] = sql(
        f"""
        WITH corrected AS ({CORRECTED_CLIENTS}),
        versions AS (
          SELECT sk_id_curr, count(*) AS n,
                 max_by(amt_income_total, __START_AT) AS income_now,
                 min_by(amt_income_total, __START_AT) AS income_before
          FROM history.application_scd2 GROUP BY sk_id_curr
        )
        SELECT
          (SELECT count(*) FROM corrected),
          (SELECT count(*) FROM versions LEFT ANTI JOIN corrected USING (sk_id_curr) WHERE n > 1),
          (SELECT count(*) FROM corrected JOIN versions USING (sk_id_curr) WHERE n <> 2),
          (SELECT count(*) FROM corrected JOIN versions USING (sk_id_curr)
             WHERE income_now <> round(income_before * 1.1, 3))
        """
    )
    assert int(corrected) > 0
    assert (int(unexpected), int(missing), int(wrong_income)) == (0, 0, 0)
    [[multi_version_contracts]] = sql(
        "SELECT count(*) FROM (SELECT 1 FROM history.previous_application_scd2 "
        "GROUP BY sk_id_prev HAVING count(*) > 1)"
    )
    assert int(multi_version_contracts) == 0


def test_validation_dataset_key_is_unique(sql) -> None:
    [[duplicates]] = sql(
        f"SELECT count(*) FROM (SELECT 1 FROM {VALIDATION_DATASET_TABLE} "
        "GROUP BY snapshot_id, sk_id_curr HAVING count(*) > 1)"
    )
    assert int(duplicates) == 0


def test_validation_dataset_holds_the_as_of_month_of_every_client(sql) -> None:
    rows = sql(
        f"""
        SELECT d.snapshot_id, d.n, d.n_months,
               (SELECT count(*) FROM gold.client_month c WHERE c.month = d.as_of_month)
        FROM (
          SELECT snapshot_id, any_value(as_of_month) AS as_of_month, count(*) AS n,
                 count(DISTINCT as_of_month) AS n_months
          FROM {VALIDATION_DATASET_TABLE} GROUP BY snapshot_id
        ) d
        """
    )
    assert rows
    for snapshot_id, n, n_months, client_month_rows in rows:
        assert int(n_months) == 1, snapshot_id
        assert int(n) == int(client_month_rows), snapshot_id


def test_validation_dataset_attributes_are_point_in_time(sql) -> None:
    # Fails on look-ahead: every snapshot must carry the version valid at its snapshot_ts,
    # also when a later delivery has replaced it since.
    [[joined, mismatches, later_versions]] = sql(
        f"""
        SELECT count(h.sk_id_curr),
               count_if(h.sk_id_curr IS NOT NULL AND NOT (
                 d.amt_income_total <=> h.amt_income_total
                 AND d.name_family_status <=> h.name_family_status)),
               count_if(h.__START_AT > d.snapshot_ts)
        FROM {VALIDATION_DATASET_TABLE} d
        LEFT JOIN history.application_scd2 h
          ON d.sk_id_curr = h.sk_id_curr AND h.__START_AT <= d.snapshot_ts
          AND (h.__END_AT IS NULL OR h.__END_AT > d.snapshot_ts)
        """
    )
    assert int(joined) > 0
    assert (int(mismatches), int(later_versions)) == (0, 0)


def test_snapshot_is_reproducible_with_time_travel(sql) -> None:
    manifest = sql(
        f"SELECT snapshot_id, table_name, table_version, n_dataset_rows "
        f"FROM {VALIDATION_SNAPSHOT_TABLE}"
    )
    by_snapshot: dict[str, set[str]] = {}
    for snapshot_id, table_name, version, n_dataset_rows in manifest:
        by_snapshot.setdefault(snapshot_id, set()).add(table_name)
        assert version is not None, (snapshot_id, table_name)
        if table_name == VALIDATION_DATASET_TABLE:
            [[rows]] = sql(
                f"SELECT count(*) FROM {VALIDATION_DATASET_TABLE} VERSION AS OF {version} "
                f"WHERE snapshot_id = '{snapshot_id}'"
            )
            assert int(rows) == int(n_dataset_rows) > 0, snapshot_id
    expected = {VALIDATION_DATASET_TABLE, *(f"history.{e}_scd2" for e in HISTORY_ENTITIES)}
    assert by_snapshot
    assert all(tables == expected for tables in by_snapshot.values())
