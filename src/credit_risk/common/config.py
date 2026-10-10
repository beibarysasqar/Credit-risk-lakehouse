"""Project-wide constants and path helpers.

The catalog name is never hardcoded here: it flows from the bundle variable ``catalog``
into pipeline/job parameters and is passed to these helpers explicitly.
"""

from dataclasses import dataclass
from datetime import date

LANDING_SCHEMA = "raw"
LANDING_VOLUME = "landing"
HOME_CREDIT_SOURCE = "home_credit"

# Bronze entity name -> Kaggle source file name.
HOME_CREDIT_TABLES: dict[str, str] = {
    "application_train": "application_train.csv",
    "application_test": "application_test.csv",
    "bureau": "bureau.csv",
    "bureau_balance": "bureau_balance.csv",
    "previous_application": "previous_application.csv",
    "pos_cash_balance": "POS_CASH_balance.csv",
    "credit_card_balance": "credit_card_balance.csv",
    "installments_payments": "installments_payments.csv",
}


@dataclass(frozen=True)
class EcbSeries:
    """One pinned ECB Data Portal series: dataflow, series key and publication lag."""

    flow: str
    key: str
    # Months between the reference month and the month in which the value is known. A value
    # of period P is used from month P + lag on (point-in-time), see docs/data_model.md.
    publication_lag_months: int


ECB_SOURCE = "ecb"
# The old sdw-wsrest host is dead; this is the only supported endpoint.
ECB_API_URL = "https://data-api.ecb.europa.eu/service/data"
# Client months start in 2010-05 (offset -96 from the anchor); the margin covers the lags.
ECB_START_PERIOD = "2009-01"
# Euro-area monthly series. Home Credit's country is not disclosed: the macro join is illustrative.
ECB_SERIES: dict[str, EcbSeries] = {
    # HICP overall index, annual rate of change; the final figure comes mid next month.
    "hicp_yoy": EcbSeries("ICP", "M.U2.N.000000.4.ANR", publication_lag_months=1),
    # Unemployment rate, age 15-74, seasonally adjusted; released about a month after the period.
    "unemployment_rate": EcbSeries(
        "LFSI", "M.I9.S.UNEHRT.TOTAL0.15_74.T", publication_lag_months=2
    ),
    # Euribor 3-month, average of the month; complete on the last day of the month itself.
    "euribor_3m": EcbSeries("FM", "M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA", publication_lag_months=0),
}
ECB_PUBLICATION_LAGS: dict[str, int] = {
    name: series.publication_lag_months for name, series in ECB_SERIES.items()
}
# Prefix of the macro columns in Gold: macro_<series>.
MACRO_COLUMN_PREFIX = "macro_"

# Silver entity names. application_train and application_test are merged into "application";
# macro_observation holds the ECB series.
SILVER_ENTITIES: tuple[str, ...] = (
    "application",
    "bureau",
    "bureau_balance",
    "previous_application",
    "pos_cash_balance",
    "credit_card_balance",
    "installments_payments",
    "macro_observation",
)

# SCD2 history tables history.<entity>_scd2: Silver entity -> business key.
HISTORY_ENTITIES: dict[str, str] = {
    "application": "sk_id_curr",
    "previous_application": "sk_id_prev",
}
# Ingestion metadata of a Bronze row. In the history tables it is carried along but not tracked:
# a row delivered again with unchanged attributes does not open a new version.
INGESTION_METADATA_COLUMNS: tuple[str, ...] = ("_rescued_data", "_ingested_at", "_source_file")
# Column that orders the deliveries of one key; it becomes __START_AT / __END_AT.
HISTORY_SEQUENCE_COLUMN = "_ingested_at"

# The source has no calendar dates: DAYS_* and MONTHS_BALANCE are offsets from the application
# date. They are mapped to the calendar through this single SYNTHETIC anchor (every application
# is treated as filed on this day). Only transformations/calendar.py may use it.
ANCHOR_DATE = date(2018, 5, 1)

# "Infinity" placeholder used by the source in DAYS_* columns (about 1000 years).
DAYS_SENTINEL = 365243
# previous_application.SELLERPLACE_AREA placeholder for an unknown selling area.
SELLERPLACE_AREA_SENTINEL = -1
# Source tokens for "not available" / "not applicable" in categorical columns.
NULL_TOKENS: tuple[str, ...] = ("XNA", "XAP")

# Source amounts carry up to 3 decimal places.
AMOUNT_TYPE = "DECIMAL(18,3)"

# Default definition: 90+ days past due (DPD component of CRR Art. 178 only), see
# docs/default_definition.md.
DEFAULT_DPD_THRESHOLD = 90
# DPD buckets as (lower edge in days, label), ascending.
DPD_BUCKETS: tuple[tuple[int, str], ...] = (
    (0, "0"),
    (1, "1-29"),
    (30, "30-59"),
    (60, "60-89"),
    (DEFAULT_DPD_THRESHOLD, "90+"),
)
# bureau_balance buckets are ordinal: 3 = 61-90 days, 4 = 91-120, 5 = 120+ or written off.
# The first bucket that lies entirely beyond 90 days is the bureau proxy of the default.
BUREAU_DEFAULT_BUCKET = 4
# Monthly balance statuses (POS_CASH_balance / credit_card_balance) of a contract that is open.
ACTIVE_CONTRACT_STATUSES: tuple[str, ...] = ("Active", "Signed", "Demand")
# Last observed day as an offset from the application date: the source ends at offset -1.
# Installments still unpaid on that day are overdue up to it.
OBSERVATION_END_OFFSET_DAYS = -1


def landing_root(catalog: str) -> str:
    """Return the root path of the landing volume of ``catalog``."""
    return f"/Volumes/{catalog}/{LANDING_SCHEMA}/{LANDING_VOLUME}"


def landing_dir(catalog: str, entity: str) -> str:
    """Return the landing directory of a Home Credit entity, one directory per table."""
    if entity not in HOME_CREDIT_TABLES:
        raise KeyError(f"Unknown Home Credit entity: {entity!r}")
    return f"{landing_root(catalog)}/{HOME_CREDIT_SOURCE}/{entity}/"


def ecb_landing_dir(catalog: str, series: str) -> str:
    """Return the landing directory of an ECB series, one directory per series."""
    if series not in ECB_SERIES:
        raise KeyError(f"Unknown ECB series: {series!r}")
    return f"{landing_root(catalog)}/{ECB_SOURCE}/{series}/"
