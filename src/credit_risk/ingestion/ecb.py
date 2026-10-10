"""Fetch the pinned ECB series and upload them into the landing volume.

Runs locally, so it does not depend on outbound access from serverless compute.

Usage:
    uv run python -m credit_risk.ingestion.ecb --catalog <catalog>
"""

import argparse
import csv
import io
from dataclasses import dataclass
from datetime import date

from credit_risk.common.config import (
    ECB_API_URL,
    ECB_SERIES,
    ECB_START_PERIOD,
    ecb_landing_dir,
)
from credit_risk.common.schemas import ECB_SCHEMA_HINTS

_TIMEOUT_SECONDS = 60


@dataclass(frozen=True)
class EcbFile:
    """One series to fetch and its destination in the landing volume."""

    series: str
    url: str
    volume_path: str


def series_url(series: str, start_period: str = ECB_START_PERIOD) -> str:
    """Return the csvdata URL of a pinned series, from ``start_period`` (``YYYY-MM``) on."""
    spec = ECB_SERIES[series]
    return f"{ECB_API_URL}/{spec.flow}/{spec.key}?format=csvdata&startPeriod={start_period}"


def plan_landing(catalog: str, fetch_date: date) -> list[EcbFile]:
    """Map every pinned series to its URL and landing path.

    The file name carries the fetch date: the ECB revises history, so a later fetch lands as a
    new file next to the old one and Silver keeps the most recently ingested observation.
    """
    return [
        EcbFile(
            series=series,
            url=series_url(series),
            volume_path=f"{ecb_landing_dir(catalog, series)}{series}_{fetch_date:%Y%m%d}.csv",
        )
        for series in ECB_SERIES
    ]


def validate_csv(series: str, body: bytes) -> None:
    """Raise ValueError unless ``body`` is a csvdata response with at least one observation.

    The API answers an unknown or empty selection with an empty body and HTTP 200, so the
    status code alone is not enough.
    """
    rows = csv.reader(io.StringIO(body.decode("utf-8")))
    header = next(rows, [])
    missing = [column for column in ECB_SCHEMA_HINTS if column not in header]
    if missing:
        raise ValueError(f"ECB series {series}: response lacks columns {', '.join(missing)}")
    if next(rows, None) is None:
        raise ValueError(f"ECB series {series}: response has no observations")


def fetch(item: EcbFile) -> bytes:
    """Download one series and validate it; HTTP errors are raised."""
    # Ships with databricks-sdk and brings its own CA bundle: urllib relies on the system
    # certificates, which a python.org build on macOS does not have.
    import requests

    response = requests.get(item.url, timeout=_TIMEOUT_SECONDS)
    response.raise_for_status()
    validate_csv(item.series, response.content)
    return response.content


def upload(plan: list[EcbFile], profile: str | None = None) -> None:
    """Fetch and upload planned files; a file already landed with the same size is skipped.

    Existing files are never overwritten or deleted: a size mismatch is an error.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound

    client = WorkspaceClient(profile=profile)
    for item in plan:
        body = fetch(item)
        try:
            remote_size = client.files.get_metadata(item.volume_path).content_length
        except NotFound:
            remote_size = None
        if remote_size == len(body):
            print(f"skip    {item.volume_path} (already landed)")
            continue
        if remote_size is not None:
            raise RuntimeError(
                f"{item.volume_path} exists with size {remote_size}, fetched {len(body)} bytes"
            )
        print(f"upload  {item.series} -> {item.volume_path} ({len(body) / 1e3:.0f} kB)")
        client.files.upload(item.volume_path, io.BytesIO(body), overwrite=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--profile", default=None, help="Profile in ~/.databrickscfg")
    args = parser.parse_args()
    upload(plan_landing(args.catalog, date.today()), profile=args.profile)


if __name__ == "__main__":
    main()
