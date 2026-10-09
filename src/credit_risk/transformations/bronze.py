"""Bronze transformations: source rows as delivered plus ingestion metadata."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Add ``_ingested_at`` and ``_source_file`` to a DataFrame read from files.

    Grain: unchanged, one row per source file row. No keys are enforced in Bronze.
    The input must come from a file source, which exposes the hidden ``_metadata`` column.
    """
    return df.withColumns(
        {
            "_ingested_at": F.current_timestamp(),
            "_source_file": F.col("_metadata.file_path"),
        }
    )
