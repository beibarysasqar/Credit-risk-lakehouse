"""Silver transformations: typed, snake_case, sentinel-free and deduplicated Home Credit tables.

Inputs are Bronze DataFrames (source column names plus ``_rescued_data``, ``_ingested_at``,
``_source_file``). Rows are never filtered here: row-level garbage is separated by the
expectations in ``common/expectations.py``.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from credit_risk.common.config import (
    AMOUNT_TYPE,
    NULL_TOKENS,
)
from credit_risk.common.schemas import SCHEMA_HINTS

_METADATA_COLUMNS = ("_rescued_data", "_ingested_at", "_source_file")


def standardize_columns(df: DataFrame, renames: dict[str, str] | None = None) -> DataFrame:
    """Lowercase every column name, then apply ``renames`` (lowercase name -> new name).

    Grain: unchanged.
    """
    renames = renames or {}
    return df.toDF(*[renames.get(name.lower(), name.lower()) for name in df.columns])


def nullify_tokens(df: DataFrame) -> DataFrame:
    """Replace the ``XNA`` / ``XAP`` tokens with null in every business string column.

    Grain: unchanged.
    """
    columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType) and field.name not in _METADATA_COLUMNS
    ]
    return df.withColumns(
        {name: F.when(~F.col(name).isin(*NULL_TOKENS), F.col(name)) for name in columns}
    )


def nullify_sentinel(df: DataFrame, column: str, sentinel: int, flag: str) -> DataFrame:
    """Replace ``sentinel`` in ``column`` with null and record it in the boolean ``flag`` column.

    Grain: unchanged. ``flag`` is false when the source value is null.
    """
    is_sentinel = F.coalesce(F.col(column) == sentinel, F.lit(False))
    return df.withColumns({flag: is_sentinel, column: F.when(~is_sentinel, F.col(column))})


def cast_columns(df: DataFrame, types: dict[str, str]) -> DataFrame:
    """Cast the given columns (name -> Spark SQL type); every other column is left as is.

    Grain: unchanged.
    """
    return df.withColumns({name: F.col(name).cast(dtype) for name, dtype in types.items()})


def dedup_latest(df: DataFrame, keys: list[str]) -> DataFrame:
    """Keep one row per ``keys``: the most recently ingested one.

    Grain: one row per ``keys``. Ordering is total and therefore deterministic:
    ``_ingested_at`` desc, ``_source_file`` desc, then a hash of the whole row.
    """
    window = Window.partitionBy(*keys).orderBy(
        F.col("_ingested_at").desc(),
        F.col("_source_file").desc(),
        F.xxhash64(*df.columns).desc(),
    )
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def flag_parent_exists(df: DataFrame, parent: DataFrame, key: str, flag: str) -> DataFrame:
    """Add the boolean ``flag``: does the row's ``key`` exist in ``parent``?

    Grain: unchanged (``parent`` is reduced to distinct keys before the join).
    """
    parent_keys = parent.select(key).distinct().withColumn(flag, F.lit(True))
    return df.join(parent_keys, on=key, how="left").withColumn(
        flag, F.coalesce(F.col(flag), F.lit(False))
    )


def _amount_types(entity: str) -> dict[str, str]:
    """Silver type of every money column of a Bronze entity (the columns pinned in Bronze)."""
    return {
        column.lower(): AMOUNT_TYPE for column in SCHEMA_HINTS[entity] if column.startswith("AMT_")
    }


def _int_types(*columns: str) -> dict[str, str]:
    return dict.fromkeys(columns, "INT")
