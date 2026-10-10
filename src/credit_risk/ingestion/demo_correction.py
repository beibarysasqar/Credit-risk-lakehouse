"""Land a small correction delivery of ``application_train`` to demonstrate the SCD2 history.

The Kaggle files are static, so every client would keep a single version forever. This helper
re-delivers the first clients of the local ``application_train.csv`` with changed attributes;
the pipeline then closes their current version and opens a new one. The values are made up.

Usage:
    uv run python -m credit_risk.ingestion.demo_correction --source-dir <dir> --catalog <catalog>
"""

import argparse
import csv
import tempfile
from datetime import date
from decimal import Decimal
from itertools import islice
from pathlib import Path

from credit_risk.common.config import HOME_CREDIT_TABLES, landing_dir
from credit_risk.ingestion.kaggle_landing import LandingFile, upload

ENTITY = "application_train"
INCOME_COLUMN = "AMT_INCOME_TOTAL"
FAMILY_STATUS_COLUMN = "NAME_FAMILY_STATUS"
INCOME_FACTOR = Decimal("1.1")
MARRIED = "Married"
NOT_MARRIED = "Single / not married"


def build_correction(header: list[str], rows: list[list[str]]) -> list[list[str]]:
    """Return ``rows`` with a 10% higher income and a flipped family status.

    Every other column, including ``SK_ID_CURR`` and ``TARGET``, is returned as delivered.
    """
    income = header.index(INCOME_COLUMN)
    family_status = header.index(FAMILY_STATUS_COLUMN)
    corrected = []
    for row in rows:
        row = list(row)
        if row[income]:
            row[income] = str((Decimal(row[income]) * INCOME_FACTOR).quantize(Decimal("0.001")))
        row[family_status] = NOT_MARRIED if row[family_status] == MARRIED else MARRIED
        corrected.append(row)
    return corrected


def correction_file_name(delivery_date: date) -> str:
    """Return the name of the correction file delivered on ``delivery_date``."""
    return f"{ENTITY}_correction_{delivery_date:%Y%m%d}.csv"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--clients", type=int, default=5, help="Number of clients to correct")
    parser.add_argument("--profile", default=None, help="Profile in ~/.databrickscfg")
    args = parser.parse_args()

    with (args.source_dir / HOME_CREDIT_TABLES[ENTITY]).open(newline="") as source:
        reader = csv.reader(source)
        header = next(reader)
        rows = list(islice(reader, args.clients))

    file_name = correction_file_name(date.today())
    with tempfile.TemporaryDirectory() as directory:
        local_path = Path(directory) / file_name
        with local_path.open("w", newline="") as target:
            csv.writer(target).writerows([header, *build_correction(header, rows)])
        item = LandingFile(
            entity=ENTITY,
            local_path=local_path,
            volume_path=f"{landing_dir(args.catalog, ENTITY)}{file_name}",
        )
        upload([item], profile=args.profile)


if __name__ == "__main__":
    main()
