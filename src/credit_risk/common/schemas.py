"""Auto Loader schema hints for the Home Credit source files.

Only keys, offsets that are part of a key or drive DPD logic, and money amounts are pinned;
every other column is inferred. Amounts stay DOUBLE in Bronze to mirror the source files,
decimal typing is decided in Silver.
"""

from credit_risk.common.config import HOME_CREDIT_TABLES

_APPLICATION_HINTS: dict[str, str] = {
    "SK_ID_CURR": "BIGINT",
    "AMT_INCOME_TOTAL": "DOUBLE",
    "AMT_CREDIT": "DOUBLE",
    "AMT_ANNUITY": "DOUBLE",
    "AMT_GOODS_PRICE": "DOUBLE",
}

SCHEMA_HINTS: dict[str, dict[str, str]] = {
    "application_train": {**_APPLICATION_HINTS, "TARGET": "INT"},
    "application_test": _APPLICATION_HINTS,
    "bureau": {
        "SK_ID_CURR": "BIGINT",
        "SK_ID_BUREAU": "BIGINT",
        "AMT_CREDIT_MAX_OVERDUE": "DOUBLE",
        "AMT_CREDIT_SUM": "DOUBLE",
        "AMT_CREDIT_SUM_DEBT": "DOUBLE",
        "AMT_CREDIT_SUM_LIMIT": "DOUBLE",
        "AMT_CREDIT_SUM_OVERDUE": "DOUBLE",
        "AMT_ANNUITY": "DOUBLE",
    },
    "bureau_balance": {
        "SK_ID_BUREAU": "BIGINT",
        "MONTHS_BALANCE": "INT",
        # Mixes digits with C / X, must never be inferred as a number.
        "STATUS": "STRING",
    },
    "previous_application": {
        "SK_ID_PREV": "BIGINT",
        "SK_ID_CURR": "BIGINT",
        "AMT_ANNUITY": "DOUBLE",
        "AMT_APPLICATION": "DOUBLE",
        "AMT_CREDIT": "DOUBLE",
        "AMT_DOWN_PAYMENT": "DOUBLE",
        "AMT_GOODS_PRICE": "DOUBLE",
    },
    "pos_cash_balance": {
        "SK_ID_PREV": "BIGINT",
        "SK_ID_CURR": "BIGINT",
        "MONTHS_BALANCE": "INT",
        "SK_DPD": "INT",
        "SK_DPD_DEF": "INT",
    },
    "credit_card_balance": {
        "SK_ID_PREV": "BIGINT",
        "SK_ID_CURR": "BIGINT",
        "MONTHS_BALANCE": "INT",
        "AMT_BALANCE": "DOUBLE",
        "AMT_CREDIT_LIMIT_ACTUAL": "DOUBLE",
        "AMT_DRAWINGS_ATM_CURRENT": "DOUBLE",
        "AMT_DRAWINGS_CURRENT": "DOUBLE",
        "AMT_DRAWINGS_OTHER_CURRENT": "DOUBLE",
        "AMT_DRAWINGS_POS_CURRENT": "DOUBLE",
        "AMT_INST_MIN_REGULARITY": "DOUBLE",
        "AMT_PAYMENT_CURRENT": "DOUBLE",
        "AMT_PAYMENT_TOTAL_CURRENT": "DOUBLE",
        "AMT_RECEIVABLE_PRINCIPAL": "DOUBLE",
        # Misspelled in the source file.
        "AMT_RECIVABLE": "DOUBLE",
        "AMT_TOTAL_RECEIVABLE": "DOUBLE",
        "SK_DPD": "INT",
        "SK_DPD_DEF": "INT",
    },
    "installments_payments": {
        "SK_ID_PREV": "BIGINT",
        "SK_ID_CURR": "BIGINT",
        # Stored as floats in the source file (e.g. "1.0", "-1180.0").
        "NUM_INSTALMENT_VERSION": "DOUBLE",
        "NUM_INSTALMENT_NUMBER": "INT",
        "DAYS_INSTALMENT": "DOUBLE",
        "DAYS_ENTRY_PAYMENT": "DOUBLE",
        "AMT_INSTALMENT": "DOUBLE",
        "AMT_PAYMENT": "DOUBLE",
    },
}

assert SCHEMA_HINTS.keys() == HOME_CREDIT_TABLES.keys()


def schema_hints(entity: str) -> str:
    """Render the ``cloudFiles.schemaHints`` value of an entity, e.g. ``"A BIGINT, B DOUBLE"``."""
    return ", ".join(f"{column} {dtype}" for column, dtype in SCHEMA_HINTS[entity].items())
