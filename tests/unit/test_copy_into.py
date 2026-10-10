from credit_risk.common.config import (
    COPY_INTO_DEMO_ENTITY,
    COPY_INTO_DEMO_TABLE,
    HOME_CREDIT_TABLES,
    landing_dir,
)
from credit_risk.ingestion.copy_into import copy_into_statement, create_table_statement


def test_demo_table_is_outside_the_medallion_schemas() -> None:
    assert COPY_INTO_DEMO_ENTITY in HOME_CREDIT_TABLES
    assert COPY_INTO_DEMO_TABLE == "sandbox.bureau_copy_into"


def test_create_table_statement_is_idempotent_and_schemaless() -> None:
    assert create_table_statement("c.sandbox.t") == "CREATE TABLE IF NOT EXISTS c.sandbox.t"


def test_copy_into_statement_reads_the_landing_dir_with_schema_merge() -> None:
    statement = copy_into_statement("c.sandbox.t", landing_dir("c", "bureau"))

    assert statement.startswith(
        "COPY INTO c.sandbox.t FROM '/Volumes/c/raw/landing/home_credit/bureau/' FILEFORMAT = CSV"
    )
    assert "'header' = 'true'" in statement
    assert "'inferSchema' = 'true'" in statement
    assert "COPY_OPTIONS ('mergeSchema' = 'true')" in statement
