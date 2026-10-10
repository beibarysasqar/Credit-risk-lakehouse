"""Bronze layer: Auto Loader ingestion of the Home Credit and ECB files from the landing volume."""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from credit_risk.common.config import (
    ECB_SERIES,
    HOME_CREDIT_TABLES,
    ecb_landing_dir,
    landing_dir,
)
from credit_risk.common.schemas import ecb_schema_hints, schema_hints
from credit_risk.transformations.bronze import add_ingestion_metadata

spark = SparkSession.getActiveSession()
catalog = spark.conf.get("catalog")


def _define_bronze_table(table: str, path: str, hints: str, comment: str) -> None:
    # Schema location and checkpoint are managed by the pipeline, so they are not set here.
    @dp.table(name=f"bronze.{table}", comment=comment)
    def _bronze() -> DataFrame:
        return add_ingestion_metadata(
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaHints", hints)
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("rescuedDataColumn", "_rescued_data")
            .load(path)
        )


for _entity, _source_file in HOME_CREDIT_TABLES.items():
    _define_bronze_table(
        _entity,
        landing_dir(catalog, _entity),
        schema_hints(_entity),
        f"Home Credit {_source_file} as delivered, plus ingestion metadata.",
    )

for _series, _spec in ECB_SERIES.items():
    _define_bronze_table(
        f"ecb_{_series}",
        ecb_landing_dir(catalog, _series),
        ecb_schema_hints(),
        f"ECB series {_spec.flow}/{_spec.key} as delivered, plus ingestion metadata.",
    )
