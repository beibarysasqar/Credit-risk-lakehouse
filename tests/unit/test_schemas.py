from credit_risk.common.config import HOME_CREDIT_TABLES
from credit_risk.common.schemas import SCHEMA_HINTS, schema_hints


def test_every_entity_pins_its_keys_as_bigint() -> None:
    for entity in HOME_CREDIT_TABLES:
        keys = {c: t for c, t in SCHEMA_HINTS[entity].items() if c.startswith("SK_ID_")}
        assert keys, entity
        assert set(keys.values()) == {"BIGINT"}, entity


def test_target_is_hinted_only_for_train() -> None:
    assert [e for e, hints in SCHEMA_HINTS.items() if "TARGET" in hints] == ["application_train"]


def test_schema_hints_rendering() -> None:
    assert schema_hints("bureau_balance") == (
        "SK_ID_BUREAU BIGINT, MONTHS_BALANCE INT, STATUS STRING"
    )
