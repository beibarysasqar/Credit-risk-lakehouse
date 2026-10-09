"""Project-wide constants and path helpers.

The catalog name is never hardcoded here: it flows from the bundle variable ``catalog``
into pipeline/job parameters and is passed to these helpers explicitly.
"""

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


def landing_root(catalog: str) -> str:
    """Return the root path of the landing volume of ``catalog``."""
    return f"/Volumes/{catalog}/{LANDING_SCHEMA}/{LANDING_VOLUME}"


def landing_dir(catalog: str, entity: str) -> str:
    """Return the landing directory of a Home Credit entity, one directory per table."""
    if entity not in HOME_CREDIT_TABLES:
        raise KeyError(f"Unknown Home Credit entity: {entity!r}")
    return f"{landing_root(catalog)}/{HOME_CREDIT_SOURCE}/{entity}/"
