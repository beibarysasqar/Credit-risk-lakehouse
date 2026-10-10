import csv
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from credit_risk.common.config import PAYMENT_STREAM_ENTITY
from credit_risk.common.expectations import SILVER_RULES
from credit_risk.common.schemas import SCHEMA_HINTS, schema_hints
from credit_risk.streaming.consumer import write_payment_events
from credit_risk.streaming.generator import (
    Event,
    batch_file_name,
    order_events,
    render_batch,
    split_batches,
    to_event,
)
from credit_risk.transformations import dpd, silver

FIXTURE = Path(__file__).parents[1] / "fixtures" / "home_credit" / f"{PAYMENT_STREAM_ENTITY}.csv"
SOURCE_COLUMNS = list(SCHEMA_HINTS[PAYMENT_STREAM_ENTITY])


@dataclass(frozen=True)
class StreamEnv:
    """A landing directory, a target table and a checkpoint of one test."""

    spark: SparkSession
    events_dir: Path
    table: str
    checkpoint: Path

    def land(self, index: int, events: list[Event]) -> None:
        (self.events_dir / batch_file_name(index)).write_text(render_batch(events))

    def consume(self) -> DataFrame:
        # Auto Loader exists only on Databricks: a plain JSON file stream with the same types
        # and the rescued-data column stands in for read_payment_events.
        events = (
            self.spark.readStream.schema(schema_hints(PAYMENT_STREAM_ENTITY))
            .json(str(self.events_dir))
            .withColumn("_rescued_data", F.lit(None).cast("string"))
        )
        write_payment_events(events, self.table, str(self.checkpoint))
        return self.spark.read.table(self.table)


@pytest.fixture
def env(spark: SparkSession, tmp_path: Path) -> Iterator[StreamEnv]:
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    table = f"payment_events_{abs(hash(str(tmp_path)))}"
    yield StreamEnv(spark, events_dir, table, tmp_path / "_checkpoint")
    spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.fixture(scope="module")
def batches() -> list[list[Event]]:
    with FIXTURE.open(newline="") as source:
        reader = csv.reader(source)
        header = next(reader)
        events = [to_event(header, row) for row in reader]
    return split_batches(order_events(events), 3)


def test_stream_delivers_the_source_rows_with_metadata(
    env: StreamEnv, batches: list[list[Event]], spark: SparkSession
) -> None:
    for index, batch in enumerate(batches):
        env.land(index, batch)

    result = env.consume()

    assert result.columns == [*SOURCE_COLUMNS, "_rescued_data", "_ingested_at", "_source_file"]
    # Compared as a multiset: the fixture holds an exact duplicate of a payment.
    source = spark.read.csv(str(FIXTURE), header=True, schema=schema_hints(PAYMENT_STREAM_ENTITY))
    assertDataFrameEqual(result.select(*SOURCE_COLUMNS), source)
    assert result.filter("_ingested_at IS NULL OR _source_file IS NULL").isEmpty()
    assert result.select("_source_file").distinct().count() == len(batches)


def test_rerun_ingests_only_new_files(env: StreamEnv, batches: list[list[Event]]) -> None:
    assert len(batches) >= 2
    env.land(0, batches[0])
    assert env.consume().count() == len(batches[0])

    # Nothing new has landed: the checkpoint keeps the first file from being read again.
    assert env.consume().count() == len(batches[0])

    env.land(1, batches[1])
    result = env.consume()
    assert result.count() == len(batches[0]) + len(batches[1])
    assert result.filter(F.col("_source_file").endswith(batch_file_name(1))).count() == len(
        batches[1]
    )


def test_stream_reproduces_the_batch_installments(
    env: StreamEnv,
    batches: list[list[Event]],
    silver_table: Callable[[str], DataFrame],
    derived: Callable[[str], DataFrame],
) -> None:
    for index, batch in enumerate(batches):
        env.land(index, batch)

    cleaned = silver.clean_installments_payments(
        env.consume(), silver_table("previous_application")
    )
    for condition in SILVER_RULES[PAYMENT_STREAM_ENTITY].drop.values():
        cleaned = cleaned.filter(F.expr(condition))

    assertDataFrameEqual(dpd.aggregate_installments(cleaned), derived("installments"))
