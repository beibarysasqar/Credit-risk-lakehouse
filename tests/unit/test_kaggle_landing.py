from pathlib import Path

import pytest

from credit_risk.common.config import HOME_CREDIT_TABLES
from credit_risk.ingestion.kaggle_landing import plan_landing


def _touch_all(source_dir: Path) -> None:
    for file_name in HOME_CREDIT_TABLES.values():
        (source_dir / file_name).write_text("header\n")


def test_plan_landing_maps_each_file_to_its_entity_dir(tmp_path: Path) -> None:
    _touch_all(tmp_path)
    (tmp_path / "sample_submission.csv").write_text("ignored\n")

    plan = {item.entity: item for item in plan_landing(tmp_path, "some_catalog")}

    assert plan.keys() == HOME_CREDIT_TABLES.keys()
    assert plan["pos_cash_balance"].local_path == tmp_path / "POS_CASH_balance.csv"
    assert plan["pos_cash_balance"].volume_path == (
        "/Volumes/some_catalog/raw/landing/home_credit/pos_cash_balance/POS_CASH_balance.csv"
    )


def test_plan_landing_lists_all_missing_files(tmp_path: Path) -> None:
    _touch_all(tmp_path)
    (tmp_path / "bureau.csv").unlink()
    (tmp_path / "installments_payments.csv").unlink()

    with pytest.raises(FileNotFoundError, match="bureau.csv, installments_payments.csv"):
        plan_landing(tmp_path, "some_catalog")
