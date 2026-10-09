"""Bronze layer: Auto Loader ingestion of the Home Credit CSV files from the landing volume."""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from credit_risk.common.config import HOME_CREDIT_TABLES, landing_dir
from credit_risk.common.schemas import schema_hints
from credit_risk.transformations.bronze import add_ingestion_metadata

spark = SparkSession.getActiveSession()
catalog = spark.conf.get("catalog")


def _define_bronze_table(entity: str, source_file: str) -> None:
    # Schema location and checkpoint are managed by the pipeline, so they are not set here.
    @dp.table(
        name=f"bronze.{entity}",
        comment=f"Home Credit {source_file} as delivered, plus ingestion metadata.",
    )
    def _bronze() -> DataFrame:
        return add_ingestion_metadata(
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaHints", schema_hints(entity))
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("rescuedDataColumn", "_rescued_data")
            .load(landing_dir(catalog, entity))
        )


for _entity, _source_file in HOME_CREDIT_TABLES.items():
    _define_bronze_table(_entity, _source_file)
