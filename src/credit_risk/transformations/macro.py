"""Macro transformations: ECB series cleaned to observations and published by month.

Unlike the Home Credit offsets, ``period_month`` of an observation is a real calendar month.
It meets the synthetic months of Gold only in the join of ``gold.client_month``; Home Credit's
country is not disclosed, so that join is illustrative.
"""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from credit_risk.common.config import MACRO_COLUMN_PREFIX
from credit_risk.transformations.silver import dedup_latest, standardize_columns

_SOURCE_COLUMNS = ("KEY", "TIME_PERIOD", "OBS_VALUE", "OBS_STATUS")
_METADATA_COLUMNS = ("_rescued_data", "_ingested_at", "_source_file")


def clean_macro_observations(observations: dict[str, DataFrame]) -> DataFrame:
    """Union the Bronze ECB tables (series name -> DataFrame) into typed observations.

    Grain: one row per ``series`` x ``time_period`` (key); a refetched, revised observation
    replaces the older one. ``period_month`` is the first day of the reference month, null when
    ``time_period`` is not ``YYYY-MM``; ``obs_value`` is null when it is not a number. Such rows
    are left to the drop expectations.
    """
    frames = [
        standardize_columns(
            df.select(*_SOURCE_COLUMNS, *_METADATA_COLUMNS), renames={"key": "series_key"}
        ).withColumn("series", F.lit(series))
        for series, df in observations.items()
    ]
    df = reduce(DataFrame.unionByName, frames)
    time_period = F.col("time_period").cast("string")
    df = df.withColumns(
        {
            "time_period": time_period,
            "period_month": F.try_to_timestamp(time_period, F.lit("yyyy-MM")).cast("date"),
            "obs_value": F.col("obs_value").try_cast("double"),
        }
    )
    df = dedup_latest(df, ["series", "time_period"])
    return df.select(
        "series",
        "period_month",
        "obs_value",
        "obs_status",
        "time_period",
        "series_key",
        *_METADATA_COLUMNS,
    )


def build_macro_month(observations: DataFrame, lags: dict[str, int]) -> DataFrame:
    """Publish the macro series by month, each with its publication lag.

    Grain: one row per ``month`` (key), for every month from the first to the last month in
    which any observation is available. ``lags`` maps a series to its publication lag in
    months; other series are ignored. Column ``macro_<series>`` of month M is the latest
    observation whose reference month + lag is not after M, so month M never sees a value
    published later (point-in-time). It is null until the first observation is available.
    """
    lag = F.create_map(*[F.lit(part) for item in lags.items() for part in item])[F.col("series")]
    published = observations.filter(F.col("series").isin(*lags)).select(
        "series",
        "period_month",
        "obs_value",
        F.add_months("period_month", lag).alias("available_month"),
    )
    months = published.agg(
        F.min("available_month").alias("first_month"),
        F.max("available_month").alias("last_month"),
    ).select(
        F.explode(F.sequence("first_month", "last_month", F.expr("interval 1 month"))).alias(
            "month"
        )
    )
    # A handful of months times a handful of series: the non-equi join stays tiny.
    by_month = (
        months.join(published, F.col("available_month") <= F.col("month"))
        .groupBy("month")
        .pivot("series", list(lags))
        .agg(F.max_by("obs_value", "period_month"))
    )
    return by_month.select(
        "month", *[F.col(series).alias(f"{MACRO_COLUMN_PREFIX}{series}") for series in lags]
    )
