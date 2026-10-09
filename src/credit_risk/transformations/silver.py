"""Silver transformations: typed, snake_case, sentinel-free and deduplicated Home Credit tables.

Inputs are Bronze DataFrames (source column names plus ``_rescued_data``, ``_ingested_at``,
``_source_file``). Rows are never filtered here: row-level garbage is separated by the
expectations in ``common/expectations.py``.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from credit_risk.common.config import (
    AMOUNT_TYPE,
    DAYS_SENTINEL,
    NULL_TOKENS,
    SELLERPLACE_AREA_SENTINEL,
)
from credit_risk.common.schemas import SCHEMA_HINTS
from credit_risk.transformations.calendar import to_calendar_date, to_calendar_month

_METADATA_COLUMNS = ("_rescued_data", "_ingested_at", "_source_file")


def standardize_columns(df: DataFrame, renames: dict[str, str] | None = None) -> DataFrame:
    """Lowercase every column name, then apply ``renames`` (lowercase name -> new name).

    Grain: unchanged.
    """
    renames = renames or {}
    return df.toDF(*[renames.get(name.lower(), name.lower()) for name in df.columns])


def nullify_tokens(df: DataFrame) -> DataFrame:
    """Replace the ``XNA`` / ``XAP`` tokens with null in every business string column.

    Grain: unchanged.
    """
    columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType) and field.name not in _METADATA_COLUMNS
    ]
    return df.withColumns(
        {name: F.when(~F.col(name).isin(*NULL_TOKENS), F.col(name)) for name in columns}
    )


def nullify_sentinel(df: DataFrame, column: str, sentinel: int, flag: str) -> DataFrame:
    """Replace ``sentinel`` in ``column`` with null and record it in the boolean ``flag`` column.

    Grain: unchanged. ``flag`` is false when the source value is null.
    """
    is_sentinel = F.coalesce(F.col(column) == sentinel, F.lit(False))
    return df.withColumns({flag: is_sentinel, column: F.when(~is_sentinel, F.col(column))})


def cast_columns(df: DataFrame, types: dict[str, str]) -> DataFrame:
    """Cast the given columns (name -> Spark SQL type); every other column is left as is.

    Grain: unchanged.
    """
    return df.withColumns({name: F.col(name).cast(dtype) for name, dtype in types.items()})


def dedup_latest(df: DataFrame, keys: list[str]) -> DataFrame:
    """Keep one row per ``keys``: the most recently ingested one.

    Grain: one row per ``keys``. Ordering is total and therefore deterministic:
    ``_ingested_at`` desc, ``_source_file`` desc, then a hash of the whole row.
    """
    window = Window.partitionBy(*keys).orderBy(
        F.col("_ingested_at").desc(),
        F.col("_source_file").desc(),
        F.xxhash64(*df.columns).desc(),
    )
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def flag_parent_exists(df: DataFrame, parent: DataFrame, key: str, flag: str) -> DataFrame:
    """Add the boolean ``flag``: does the row's ``key`` exist in ``parent``?

    Grain: unchanged (``parent`` is reduced to distinct keys before the join).
    """
    parent_keys = parent.select(key).distinct().withColumn(flag, F.lit(True))
    return df.join(parent_keys, on=key, how="left").withColumn(
        flag, F.coalesce(F.col(flag), F.lit(False))
    )


def _amount_types(entity: str) -> dict[str, str]:
    """Silver type of every money column of a Bronze entity (the columns pinned in Bronze)."""
    return {
        column.lower(): AMOUNT_TYPE for column in SCHEMA_HINTS[entity] if column.startswith("AMT_")
    }


def _int_types(*columns: str) -> dict[str, str]:
    return dict.fromkeys(columns, "INT")


def clean_application(train: DataFrame, test: DataFrame) -> DataFrame:
    """Merge ``application_train`` and ``application_test`` into one cleaned table.

    Grain: one row per ``sk_id_curr`` (key). ``is_train`` tells the two sources apart;
    ``target`` exists only for train rows and is null (never imputed) for test rows.
    ``days_employed`` = 365243 becomes null with ``days_employed_anomaly`` set.
    """
    df = standardize_columns(
        train.withColumn("is_train", F.lit(True)).unionByName(
            test.withColumn("is_train", F.lit(False)), allowMissingColumns=True
        )
    )
    df = nullify_tokens(df)
    df = nullify_sentinel(df, "days_employed", DAYS_SENTINEL, "days_employed_anomaly")
    df = cast_columns(
        df,
        {
            "sk_id_curr": "BIGINT",
            **_amount_types("application_train"),
            **_int_types(
                "target",
                "cnt_children",
                "cnt_fam_members",
                "days_birth",
                "days_employed",
                "days_id_publish",
            ),
        },
    )
    leading = ["sk_id_curr", "is_train", "target"]
    df = df.select(*leading, *[name for name in df.columns if name not in leading])
    return dedup_latest(df, ["sk_id_curr"])


def clean_bureau(df: DataFrame) -> DataFrame:
    """Clean the credit bureau loans.

    Grain: one row per ``sk_id_bureau`` (key); ``sk_id_curr`` references the application.
    Adds the synthetic dates ``credit_date``, ``credit_end_date``, ``credit_end_fact_date``
    and ``credit_update_date``.
    """
    df = nullify_tokens(standardize_columns(df))
    df = cast_columns(
        df,
        {
            "sk_id_curr": "BIGINT",
            "sk_id_bureau": "BIGINT",
            **_amount_types("bureau"),
            **_int_types(
                "days_credit",
                "credit_day_overdue",
                "days_credit_enddate",
                "days_enddate_fact",
                "days_credit_update",
                "cnt_credit_prolong",
            ),
        },
    )
    df = df.withColumns(
        {
            "credit_date": to_calendar_date(F.col("days_credit")),
            "credit_end_date": to_calendar_date(F.col("days_credit_enddate")),
            "credit_end_fact_date": to_calendar_date(F.col("days_enddate_fact")),
            "credit_update_date": to_calendar_date(F.col("days_credit_update")),
        }
    )
    return dedup_latest(df, ["sk_id_bureau"])


def clean_bureau_balance(df: DataFrame, bureau: DataFrame) -> DataFrame:
    """Clean the monthly credit bureau statuses.

    Grain: one row per ``sk_id_bureau`` x ``months_balance`` (key). ``bureau`` is the Silver
    bureau table. ``status`` is mapped to the ordinal ``dpd_bucket`` (0 = no DPD, 1..5 = DPD
    buckets, null for C / X) plus ``is_closed`` and ``is_unknown``; no day counts are invented.
    ``month`` is the synthetic calendar month, ``has_bureau_record`` flags orphan contracts.
    """
    df = standardize_columns(df)
    df = cast_columns(df, {"sk_id_bureau": "BIGINT", "months_balance": "INT", "status": "STRING"})
    status = F.col("status")
    df = df.withColumns(
        {
            "month": to_calendar_month(F.col("months_balance")),
            "dpd_bucket": F.when(status.rlike("^[0-5]$"), status.try_cast("int")),
            "is_closed": F.coalesce(status == "C", F.lit(False)),
            "is_unknown": F.coalesce(status == "X", F.lit(False)),
        }
    )
    df = dedup_latest(df, ["sk_id_bureau", "months_balance"])
    return flag_parent_exists(df, bureau, "sk_id_bureau", "has_bureau_record")


def clean_previous_application(df: DataFrame) -> DataFrame:
    """Clean the previous Home Credit applications.

    Grain: one row per ``sk_id_prev`` (key); ``sk_id_curr`` references the application.
    The 365243 sentinel in the five ``days_*`` schedule columns and the -1 sentinel in
    ``sellerplace_area`` become null with a ``<column>_anomaly`` flag each.
    ``decision_date`` is the synthetic date of ``days_decision``.
    """
    df = nullify_tokens(standardize_columns(df))
    sentinel_days = [
        "days_first_drawing",
        "days_first_due",
        "days_last_due_1st_version",
        "days_last_due",
        "days_termination",
    ]
    for column in sentinel_days:
        df = nullify_sentinel(df, column, DAYS_SENTINEL, f"{column}_anomaly")
    df = nullify_sentinel(
        df, "sellerplace_area", SELLERPLACE_AREA_SENTINEL, "sellerplace_area_anomaly"
    )
    df = cast_columns(
        df,
        {
            "sk_id_prev": "BIGINT",
            "sk_id_curr": "BIGINT",
            **_amount_types("previous_application"),
            **_int_types(
                "days_decision",
                *sentinel_days,
                "sellerplace_area",
                "cnt_payment",
                "hour_appr_process_start",
                "nflag_last_appl_in_day",
                "nflag_insured_on_approval",
            ),
        },
    )
    df = df.withColumn("decision_date", to_calendar_date(F.col("days_decision")))
    return dedup_latest(df, ["sk_id_prev"])


def clean_pos_cash_balance(df: DataFrame, previous_application: DataFrame) -> DataFrame:
    """Clean the monthly balances of previous POS and cash loans.

    Grain: one row per ``sk_id_prev`` x ``months_balance`` (key). ``previous_application`` is
    the Silver table. ``sk_dpd`` / ``sk_dpd_def`` keep the source values. ``month`` is the
    synthetic calendar month, ``has_previous_application`` flags orphan contracts.
    """
    df = nullify_tokens(standardize_columns(df))
    df = cast_columns(
        df,
        {
            "sk_id_prev": "BIGINT",
            "sk_id_curr": "BIGINT",
            **_int_types(
                "months_balance", "cnt_instalment", "cnt_instalment_future", "sk_dpd", "sk_dpd_def"
            ),
        },
    )
    df = df.withColumn("month", to_calendar_month(F.col("months_balance")))
    df = dedup_latest(df, ["sk_id_prev", "months_balance"])
    return flag_parent_exists(df, previous_application, "sk_id_prev", "has_previous_application")


def clean_credit_card_balance(df: DataFrame, previous_application: DataFrame) -> DataFrame:
    """Clean the monthly balances of previous credit cards.

    Grain: one row per ``sk_id_prev`` x ``months_balance`` (key). ``previous_application`` is
    the Silver table. The source typo ``AMT_RECIVABLE`` is renamed to ``amt_receivable``;
    ``sk_dpd`` / ``sk_dpd_def`` keep the source values. ``month`` is the synthetic calendar
    month, ``has_previous_application`` flags orphan contracts.
    """
    df = nullify_tokens(standardize_columns(df, {"amt_recivable": "amt_receivable"}))
    amounts = {
        ("amt_receivable" if column == "amt_recivable" else column): dtype
        for column, dtype in _amount_types("credit_card_balance").items()
    }
    df = cast_columns(
        df,
        {
            "sk_id_prev": "BIGINT",
            "sk_id_curr": "BIGINT",
            **amounts,
            **_int_types(
                "months_balance",
                "cnt_drawings_atm_current",
                "cnt_drawings_current",
                "cnt_drawings_other_current",
                "cnt_drawings_pos_current",
                "cnt_instalment_mature_cum",
                "sk_dpd",
                "sk_dpd_def",
            ),
        },
    )
    df = df.withColumn("month", to_calendar_month(F.col("months_balance")))
    df = dedup_latest(df, ["sk_id_prev", "months_balance"])
    return flag_parent_exists(df, previous_application, "sk_id_prev", "has_previous_application")


def clean_installments_payments(df: DataFrame, previous_application: DataFrame) -> DataFrame:
    """Clean the payments made on previous loans.

    Grain: one row per payment; an installment (``sk_id_prev``, ``num_instalment_number``,
    ``num_instalment_version``) has several rows when it was paid in parts, so there is no
    narrower key and only exact duplicates of a whole source row are removed. Payments are not
    aggregated here. ``previous_application`` is the Silver table. Adds the synthetic
    ``instalment_date`` / ``entry_payment_date`` and ``has_previous_application``.
    """
    df = standardize_columns(df)
    business_columns = [name for name in df.columns if name not in _METADATA_COLUMNS]
    df = cast_columns(
        df,
        {
            "sk_id_prev": "BIGINT",
            "sk_id_curr": "BIGINT",
            **_amount_types("installments_payments"),
            **_int_types(
                "num_instalment_version",
                "num_instalment_number",
                "days_instalment",
                "days_entry_payment",
            ),
        },
    )
    df = df.withColumns(
        {
            "instalment_date": to_calendar_date(F.col("days_instalment")),
            "entry_payment_date": to_calendar_date(F.col("days_entry_payment")),
        }
    )
    df = dedup_latest(df, business_columns)
    return flag_parent_exists(df, previous_application, "sk_id_prev", "has_previous_application")
