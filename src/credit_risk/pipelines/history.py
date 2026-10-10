"""History layer: SCD2 tables of client and contract attributes, built with AUTO CDC.

Silver is made of materialized views, which cannot be streamed, so the change feed is the
Bronze stream cleaned with the same row-level functions and the same fail / drop rules.
"""

from collections.abc import Callable

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from credit_risk.common.config import (
    HISTORY_ENTITIES,
    HISTORY_SEQUENCE_COLUMN,
    INGESTION_METADATA_COLUMNS,
)
from credit_risk.common.expectations import SILVER_RULES
from credit_risk.transformations import silver

spark = SparkSession.getActiveSession()


def _bronze_stream(entity: str) -> DataFrame:
    return spark.readStream.table(f"bronze.{entity}")


# History entity -> builder of its change feed (one row per delivered source row).
HISTORY_CHANGES: dict[str, Callable[[], DataFrame]] = {
    "application": lambda: silver.prepare_application(
        _bronze_stream("application_train"), _bronze_stream("application_test")
    ),
    "previous_application": lambda: silver.prepare_previous_application(
        _bronze_stream("previous_application")
    ),
}

assert HISTORY_CHANGES.keys() == HISTORY_ENTITIES.keys()


def _define_history_table(entity: str, key: str, build: Callable[[], DataFrame]) -> None:
    rules = SILVER_RULES[entity]
    changes = f"history_{entity}_changes"
    target = f"history.{entity}_scd2"

    @dp.temporary_view(name=changes)
    @dp.expect_all_or_fail(rules.fail)
    @dp.expect_all_or_drop(rules.drop)
    def _changes() -> DataFrame:
        return build()

    dp.create_streaming_table(
        name=target,
        comment=(
            f"SCD2 history of {entity}: one row per {key} and version; __START_AT / __END_AT "
            "are ingestion time."
        ),
        cluster_by=[key],
    )
    dp.create_auto_cdc_flow(
        target=target,
        source=changes,
        keys=[key],
        sequence_by=HISTORY_SEQUENCE_COLUMN,
        stored_as_scd_type=2,
        track_history_except_column_list=list(INGESTION_METADATA_COLUMNS),
    )


for _entity, _key in HISTORY_ENTITIES.items():
    _define_history_table(_entity, _key, HISTORY_CHANGES[_entity])
