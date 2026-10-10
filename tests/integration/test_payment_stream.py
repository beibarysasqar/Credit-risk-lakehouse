import pytest

from credit_risk.common.config import (
    HOME_CREDIT_TABLES,
    PAYMENT_EVENTS_TABLE,
    PAYMENT_STREAM_ENTITY,
    PAYMENT_STREAM_SAMPLE_MODULUS,
    payment_stream_events_dir,
)
from credit_risk.common.schemas import SCHEMA_HINTS

pytestmark = pytest.mark.integration

COLUMNS = ", ".join(SCHEMA_HINTS[PAYMENT_STREAM_ENTITY])
STREAM = f"SELECT {COLUMNS} FROM {PAYMENT_EVENTS_TABLE}"
# The same payments as delivered by the Kaggle file: the batch path of the replayed contracts.
BATCH = (
    f"SELECT {COLUMNS} FROM bronze.{PAYMENT_STREAM_ENTITY} "
    f"WHERE endswith(_source_file, '/{HOME_CREDIT_TABLES[PAYMENT_STREAM_ENTITY]}') "
    f"AND SK_ID_PREV % {PAYMENT_STREAM_SAMPLE_MODULUS} = 0"
)


def test_payment_events_are_delivered_with_metadata(sql, catalog: str) -> None:
    [[rows, rescued, missing_metadata, foreign_files, outside_sample]] = sql(
        f"""
        SELECT
          count(*),
          count(_rescued_data),
          count_if(_ingested_at IS NULL OR _source_file IS NULL),
          count_if(NOT startswith(_source_file, '{payment_stream_events_dir(catalog)}')),
          count_if(SK_ID_PREV % {PAYMENT_STREAM_SAMPLE_MODULUS} <> 0)
        FROM {PAYMENT_EVENTS_TABLE}
        """
    )
    assert int(rows) > 0
    assert (int(rescued), int(missing_metadata), int(foreign_files), int(outside_sample)) == (
        0,
        0,
        0,
        0,
    )


def test_stream_has_no_event_missing_from_the_batch(sql) -> None:
    # Compared as multisets, so a micro-file ingested twice would show up here.
    [[extra]] = sql(f"SELECT count(*) FROM ({STREAM} EXCEPT ALL {BATCH})")
    assert int(extra) == 0


def test_stream_has_every_batch_payment_of_the_replayed_contracts(sql) -> None:
    # Holds once every micro-file of the generator has landed and the consumer has run.
    [[lost]] = sql(f"SELECT count(*) FROM ({BATCH} EXCEPT ALL {STREAM})")
    assert int(lost) == 0
