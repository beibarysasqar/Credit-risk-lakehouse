import pytest

from credit_risk.common.config import (
    COPY_INTO_DEMO_ENTITY,
    COPY_INTO_DEMO_TABLE,
    HOME_CREDIT_TABLES,
)
from credit_risk.common.schemas import SCHEMA_HINTS

pytestmark = pytest.mark.integration

# The Kaggle file as ingested by Auto Loader (the demo correction touches other entities only).
BRONZE = (
    f"bronze.{COPY_INTO_DEMO_ENTITY} "
    f"WHERE endswith(_source_file, '/{HOME_CREDIT_TABLES[COPY_INTO_DEMO_ENTITY]}')"
)


def test_copy_into_loads_the_file_exactly_once(sql) -> None:
    """Equal row counts also after reruns of the job: COPY INTO skips files it has loaded."""
    [[copied]] = sql(f"SELECT count(*) FROM {COPY_INTO_DEMO_TABLE}")
    [[ingested]] = sql(f"SELECT count(*) FROM {BRONZE}")
    assert int(copied) > 0
    assert copied == ingested


def test_copy_into_delivers_the_same_keys_and_amounts(sql) -> None:
    columns = ", ".join(SCHEMA_HINTS[COPY_INTO_DEMO_ENTITY])
    typed = ", ".join(
        f"CAST({column} AS {type_}) AS {column}"
        for column, type_ in SCHEMA_HINTS[COPY_INTO_DEMO_ENTITY].items()
    )
    [[difference]] = sql(
        f"""
        SELECT count(*) FROM (
          SELECT {typed} FROM {COPY_INTO_DEMO_TABLE}
          EXCEPT ALL
          SELECT {columns} FROM {BRONZE}
        )
        """
    )
    assert int(difference) == 0
