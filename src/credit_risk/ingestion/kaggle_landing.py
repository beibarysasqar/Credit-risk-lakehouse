"""Upload locally downloaded Home Credit CSV files into the landing volume.

Usage:
    uv run python -m credit_risk.ingestion.kaggle_landing --source-dir <dir> --catalog <catalog>
"""

import argparse
from dataclasses import dataclass
from pathlib import Path

from credit_risk.common.config import HOME_CREDIT_TABLES, landing_dir


@dataclass(frozen=True)
class LandingFile:
    """One source file and its destination in the landing volume."""

    entity: str
    local_path: Path
    volume_path: str


def plan_landing(source_dir: Path, catalog: str) -> list[LandingFile]:
    """Map every Home Credit source file in ``source_dir`` to its landing path.

    Raises FileNotFoundError naming all missing files, so a partial download is never landed.
    """
    plan = [
        LandingFile(
            entity=entity,
            local_path=source_dir / file_name,
            volume_path=f"{landing_dir(catalog, entity)}{file_name}",
        )
        for entity, file_name in HOME_CREDIT_TABLES.items()
    ]
    missing = [item.local_path.name for item in plan if not item.local_path.is_file()]
    if missing:
        raise FileNotFoundError(f"Missing in {source_dir}: {', '.join(missing)}")
    return plan


def upload(plan: list[LandingFile], profile: str | None = None) -> None:
    """Upload planned files; files already landed with the same size are skipped.

    Existing files are never overwritten or deleted: a size mismatch is an error.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound

    client = WorkspaceClient(profile=profile)
    for item in plan:
        local_size = item.local_path.stat().st_size
        try:
            remote_size = client.files.get_metadata(item.volume_path).content_length
        except NotFound:
            remote_size = None
        if remote_size == local_size:
            print(f"skip    {item.volume_path} (already landed)")
            continue
        if remote_size is not None:
            raise RuntimeError(
                f"{item.volume_path} exists with size {remote_size}, local file has {local_size}"
            )
        print(f"upload  {item.local_path.name} -> {item.volume_path} ({local_size / 1e6:.0f} MB)")
        with item.local_path.open("rb") as contents:
            client.files.upload(item.volume_path, contents, overwrite=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--profile", default=None, help="Profile in ~/.databrickscfg")
    args = parser.parse_args()
    upload(plan_landing(args.source_dir, args.catalog), profile=args.profile)


if __name__ == "__main__":
    main()
