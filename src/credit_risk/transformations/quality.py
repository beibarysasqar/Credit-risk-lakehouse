"""Data-quality helpers shared by the Silver tables."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def quarantine_rows(df: DataFrame, rules: dict[str, str]) -> DataFrame:
    """Return the rows that break at least one of ``rules`` (expectation name -> SQL condition).

    Grain: unchanged, a subset of the input rows; the complement of what ``expect_or_drop``
    keeps for the same rules. A rule that evaluates to null counts as broken. Adds
    ``_failed_rules``, the sorted names of the broken rules.
    """
    failed = [
        F.when(~F.coalesce(F.expr(condition), F.lit(False)), F.lit(name))
        for name, condition in sorted(rules.items())
    ]
    return df.withColumn("_failed_rules", F.array_compact(F.array(*failed))).filter(
        F.size("_failed_rules") > 0
    )
