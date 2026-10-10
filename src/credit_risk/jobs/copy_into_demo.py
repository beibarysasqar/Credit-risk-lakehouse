"""Load one Home Credit table with COPY INTO, for the comparison with Auto Loader.

Fills ``sandbox.bureau_copy_into`` from the same landing files as ``bronze.bureau``. A rerun
loads nothing: COPY INTO skips the files it has already loaded. See docs/perf_notes.md.

Usage (wheel entry point ``copy_into_demo``):
    copy_into_demo --catalog <catalog>
"""

import argparse

from pyspark.sql import SparkSession

from credit_risk.common.config import COPY_INTO_DEMO_ENTITY, COPY_INTO_DEMO_TABLE, landing_dir
from credit_risk.ingestion.copy_into import copy_into_statement, create_table_statement


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    table = f"{args.catalog}.{COPY_INTO_DEMO_TABLE}"
    spark.sql(create_table_statement(table))
    spark.sql(copy_into_statement(table, landing_dir(args.catalog, COPY_INTO_DEMO_ENTITY))).show(
        truncate=False
    )


if __name__ == "__main__":
    main()
