"""Replay a sample of ``installments_payments`` as JSON micro-files into the landing volume.

The Kaggle file is static, so there is no real payment stream. This helper replays the payments
of a deterministic sample of contracts in payment order, a few hundred events per file; the
consumer job then ingests them incrementally. Events keep the source column names and offsets:
no calendar date is derived here.

Usage:
    uv run python -m credit_risk.streaming.generator --source-dir <dir> --catalog <catalog>
"""

import argparse
import csv
import json
import tempfile
from pathlib import Path

from credit_risk.common.config import (
    HOME_CREDIT_TABLES,
    PAYMENT_STREAM_ENTITY,
    PAYMENT_STREAM_SAMPLE_MODULUS,
    payment_stream_events_dir,
)
from credit_risk.common.schemas import SCHEMA_HINTS
from credit_risk.ingestion.kaggle_landing import LandingFile, upload

Event = dict[str, int | float | None]

KEY_COLUMN = "SK_ID_PREV"
PAYMENT_DAY_COLUMN = "DAYS_ENTRY_PAYMENT"
DUE_DAY_COLUMN = "DAYS_INSTALMENT"
_INTEGER_TYPES = ("BIGINT", "INT")


def is_replayed(sk_id_prev: int) -> bool:
    """Tell whether the payments of contract ``sk_id_prev`` belong to the replayed sample."""
    return sk_id_prev % PAYMENT_STREAM_SAMPLE_MODULUS == 0


def to_event(header: list[str], row: list[str]) -> Event:
    """Convert one source CSV row into an event typed like the Bronze table.

    Grain: one event per source row (key: none, a payment has no identifier). Integers are
    written as integers although the file stores some of them as floats (``"6.0"``); an empty
    value becomes null.
    """
    event: Event = {}
    for column, value in zip(header, row, strict=True):
        if value == "":
            event[column] = None
        elif SCHEMA_HINTS[PAYMENT_STREAM_ENTITY][column] in _INTEGER_TYPES:
            event[column] = int(float(value))
        else:
            event[column] = float(value)
    return event


def _replay_position(event: Event) -> tuple:
    # An installment that was never paid is replayed on its due day; offsets are never positive.
    day = event[PAYMENT_DAY_COLUMN]
    if day is None:
        day = event[DUE_DAY_COLUMN]
    tie_break = tuple((value is None, value or 0) for value in event.values())
    return (day is None, day or 0, tie_break)


def order_events(events: list[Event]) -> list[Event]:
    """Return ``events`` in replay order: by payment day, then by every column (total order)."""
    return sorted(events, key=_replay_position)


def split_batches(events: list[Event], size: int) -> list[list[Event]]:
    """Split ``events`` into consecutive micro-batches of at most ``size`` events."""
    if size < 1:
        raise ValueError(f"Batch size must be positive, got {size}")
    return [events[start : start + size] for start in range(0, len(events), size)]


def batch_file_name(index: int) -> str:
    """Return the name of the micro-file of batch ``index`` (0-based)."""
    return f"payments_{index:05d}.json"


def render_batch(events: list[Event]) -> str:
    """Render a micro-batch as JSON Lines, one event per line."""
    return "".join(json.dumps(event) + "\n" for event in events)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--events-per-file", type=int, default=200)
    parser.add_argument(
        "--max-batches", type=int, default=None, help="Land only the first N micro-files"
    )
    parser.add_argument("--profile", default=None, help="Profile in ~/.databrickscfg")
    args = parser.parse_args()

    with (args.source_dir / HOME_CREDIT_TABLES[PAYMENT_STREAM_ENTITY]).open(newline="") as source:
        reader = csv.reader(source)
        header = next(reader)
        key = header.index(KEY_COLUMN)
        events = [to_event(header, row) for row in reader if is_replayed(int(row[key]))]

    batches = split_batches(order_events(events), args.events_per_file)
    print(f"{len(events)} events in {len(batches)} micro-files")
    with tempfile.TemporaryDirectory() as directory:
        plan = []
        for index, batch in enumerate(batches[: args.max_batches]):
            local_path = Path(directory) / batch_file_name(index)
            local_path.write_text(render_batch(batch), newline="\n")
            plan.append(
                LandingFile(
                    entity=PAYMENT_STREAM_ENTITY,
                    local_path=local_path,
                    volume_path=f"{payment_stream_events_dir(args.catalog)}{local_path.name}",
                )
            )
        upload(plan, profile=args.profile)


if __name__ == "__main__":
    main()
