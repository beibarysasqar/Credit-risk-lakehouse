from datetime import date

from credit_risk.ingestion.demo_correction import build_correction, correction_file_name

HEADER = ["SK_ID_CURR", "TARGET", "AMT_INCOME_TOTAL", "NAME_FAMILY_STATUS", "AMT_CREDIT"]


def test_build_correction_changes_only_income_and_family_status() -> None:
    rows = [
        ["100002", "1", "202500.0", "Single / not married", "406597.5"],
        ["100003", "0", "270000.0", "Married", "1293502.5"],
        ["100004", "0", "", "Widow", "135000.0"],
    ]

    assert build_correction(HEADER, rows) == [
        ["100002", "1", "222750.000", "Married", "406597.5"],
        ["100003", "0", "297000.000", "Single / not married", "1293502.5"],
        # A missing income stays missing.
        ["100004", "0", "", "Married", "135000.0"],
    ]
    # The input is not modified in place.
    assert rows[0][2] == "202500.0"


def test_correction_file_name_is_dated() -> None:
    assert correction_file_name(date(2026, 10, 10)) == "application_train_correction_20261010.csv"
