"""Shared pytest fixtures."""

import os
import subprocess
from collections.abc import Iterator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def _ensure_java_home() -> None:
    """Point JAVA_HOME at Homebrew's keg-only openjdk@17 when nothing else is configured."""
    if os.environ.get("JAVA_HOME"):
        return
    try:
        prefix = subprocess.run(
            ["brew", "--prefix", "openjdk@17"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return
    if os.path.isdir(prefix):
        os.environ["JAVA_HOME"] = prefix


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    """Local Spark session with Delta Lake enabled, shared by the whole test session."""
    _ensure_java_home()
    warehouse_dir = tmp_path_factory.mktemp("spark-warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("credit-risk-unit-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
