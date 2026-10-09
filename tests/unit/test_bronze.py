from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from credit_risk.transformations.bronze import add_ingestion_metadata

FIXTURE = Path(__file__).parents[1] / "fixtures" / "home_credit" / "bureau_balance.csv"


def test_add_ingestion_metadata(spark: SparkSession) -> None:
    source = spark.read.csv(str(FIXTURE), header=True)

    result = add_ingestion_metadata(source)

    assert result.columns == [*source.columns, "_ingested_at", "_source_file"]
    assert result.schema["_ingested_at"].dataType.typeName() == "timestamp"
    assertDataFrameEqual(result.select(*source.columns), source)
    # Every row carries a load timestamp and the path of the file it came from.
    assertDataFrameEqual(
        result.select(
            F.col("_ingested_at").isNotNull().alias("has_ts"),
            F.col("_source_file").endswith("/home_credit/bureau_balance.csv").alias("has_file"),
        ).distinct(),
        spark.createDataFrame([(True, True)], "has_ts BOOLEAN, has_file BOOLEAN"),
    )
