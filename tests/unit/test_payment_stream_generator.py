import json

import pytest

from credit_risk.common.config import PAYMENT_STREAM_SAMPLE_MODULUS
from credit_risk.streaming.generator import (
    batch_file_name,
    is_replayed,
    order_events,
    render_batch,
    split_batches,
    to_event,
)

HEADER = [
    "SK_ID_PREV",
    "SK_ID_CURR",
    "NUM_INSTALMENT_VERSION",
    "NUM_INSTALMENT_NUMBER",
    "DAYS_INSTALMENT",
    "DAYS_ENTRY_PAYMENT",
    "AMT_INSTALMENT",
    "AMT_PAYMENT",
]


def _event(number: int, due: float, paid: float | None) -> dict:
    return to_event(
        HEADER,
        [
            "1001",
            "100001",
            "1.0",
            str(number),
            str(due),
            "" if paid is None else str(paid),
            "5",
            "",
        ],
    )


def test_to_event_types_values_like_bronze() -> None:
    event = to_event(HEADER, ["1054186", "161674", "1.0", "6", "-1180.0", "", "6948.36", ""])

    assert event == {
        "SK_ID_PREV": 1054186,
        "SK_ID_CURR": 161674,
        "NUM_INSTALMENT_VERSION": 1.0,
        "NUM_INSTALMENT_NUMBER": 6,
        "DAYS_INSTALMENT": -1180.0,
        # An installment that was never paid has no payment day and no payment amount.
        "DAYS_ENTRY_PAYMENT": None,
        "AMT_INSTALMENT": 6948.36,
        "AMT_PAYMENT": None,
    }
    assert isinstance(event["SK_ID_PREV"], int) and isinstance(event["NUM_INSTALMENT_NUMBER"], int)
    assert isinstance(event["NUM_INSTALMENT_VERSION"], float)


def test_to_event_rejects_a_row_of_another_width() -> None:
    with pytest.raises(ValueError):
        to_event(HEADER, ["1054186", "161674"])


def test_is_replayed_selects_the_sample_contracts() -> None:
    assert is_replayed(PAYMENT_STREAM_SAMPLE_MODULUS * 3)
    assert not is_replayed(PAYMENT_STREAM_SAMPLE_MODULUS * 3 + 1)


def test_order_events_replays_by_payment_day_and_unpaid_on_the_due_day() -> None:
    paid_late = _event(1, due=-90.0, paid=-1.0)
    paid_early = _event(2, due=-60.0, paid=-121.0)
    unpaid = _event(3, due=-30.0, paid=None)
    tie = _event(4, due=-10.0, paid=-30.0)

    ordered = order_events([paid_late, tie, unpaid, paid_early])

    assert ordered == [paid_early, unpaid, tie, paid_late]
    # The order is total: it does not depend on the input order.
    assert order_events([tie, paid_early, paid_late, unpaid]) == ordered


def test_split_batches_keeps_the_order_and_the_remainder() -> None:
    events = [_event(number, due=-10.0, paid=-10.0) for number in range(5)]

    batches = split_batches(events, 2)

    assert [len(batch) for batch in batches] == [2, 2, 1]
    assert [event for batch in batches for event in batch] == events
    assert split_batches([], 2) == []
    with pytest.raises(ValueError):
        split_batches(events, 0)


def test_batch_file_names_sort_in_replay_order() -> None:
    assert batch_file_name(0) == "payments_00000.json"
    assert batch_file_name(9) < batch_file_name(10)


def test_render_batch_is_json_lines() -> None:
    events = [_event(1, due=-90.0, paid=-1.0), _event(2, due=-60.0, paid=None)]

    lines = render_batch(events).splitlines()

    assert [json.loads(line) for line in lines] == events
    assert render_batch(events).endswith("\n")
