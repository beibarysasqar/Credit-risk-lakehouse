from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual


def test_delta_round_trip(spark: SparkSession, tmp_path: Path) -> None:
    expected = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, value STRING")
    path = str(tmp_path / "delta_table")

    expected.write.format("delta").save(path)

    assertDataFrameEqual(spark.read.format("delta").load(path), expected)
