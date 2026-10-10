import pytest

from credit_risk.common.config import HOME_CREDIT_TABLES, landing_dir

pytestmark = pytest.mark.integration

# Data rows of the Kaggle "Home Credit Default Risk" files (line count minus header).
SOURCE_ROW_COUNTS = {
    "application_train": 307_511,
    "application_test": 48_744,
    "bureau": 1_716_428,
    "bureau_balance": 27_299_925,
    "previous_application": 1_670_214,
    "pos_cash_balance": 10_001_358,
    "credit_card_balance": 3_840_312,
    "installments_payments": 13_605_401,
}


def test_row_counts_cover_every_entity() -> None:
    assert SOURCE_ROW_COUNTS.keys() == HOME_CREDIT_TABLES.keys()


@pytest.mark.parametrize("entity", list(HOME_CREDIT_TABLES))
def test_bronze_table_matches_source(entity: str, sql, catalog: str) -> None:
    # Only the rows of the Kaggle file are counted: later deliveries into the same directory
    # (the demo correction of application_train) add rows on top.
    [[rows, rescued, missing_metadata, foreign_files]] = sql(
        f"""
        SELECT
          count_if(endswith(_source_file, '/{HOME_CREDIT_TABLES[entity]}')),
          count(_rescued_data),
          count_if(_ingested_at IS NULL OR _source_file IS NULL),
          count_if(NOT contains(_source_file, '{landing_dir(catalog, entity)}'))
        FROM bronze.{entity}
        """
    )
    assert int(rows) == SOURCE_ROW_COUNTS[entity]
    assert int(rescued) == 0
    assert int(missing_metadata) == 0
    assert int(foreign_files) == 0


def test_target_exists_only_in_train(sql) -> None:
    # The schema also holds the pipeline's internal __materialization_* tables.
    entities = ", ".join(f"'{entity}'" for entity in HOME_CREDIT_TABLES)
    tables = sql(
        "SELECT table_name FROM information_schema.columns "
        f"WHERE table_schema = 'bronze' AND column_name = 'TARGET' AND table_name IN ({entities})"
    )
    assert tables == [["application_train"]]
