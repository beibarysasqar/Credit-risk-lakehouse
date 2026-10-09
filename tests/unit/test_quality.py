from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from credit_risk.transformations.quality import quarantine_rows

RULES = {"b_positive": "b > 0", "a_known": "a IN ('x', 'y')"}


def test_quarantine_rows_keeps_only_broken_rows(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, "x", 5), (2, "z", 5), (3, "x", -1), (4, "z", 0), (5, None, 5), (6, "y", None)],
        "id INT, a STRING, b INT",
    )

    result = quarantine_rows(df, RULES)

    expected = spark.createDataFrame(
        [
            (2, "z", 5, ["a_known"]),
            (3, "x", -1, ["b_positive"]),
            (4, "z", 0, ["a_known", "b_positive"]),
            # A rule that evaluates to null counts as broken.
            (5, None, 5, ["a_known"]),
            (6, "y", None, ["b_positive"]),
        ],
        "id INT, a STRING, b INT, _failed_rules ARRAY<STRING>",
    )
    assertDataFrameEqual(result, expected)
