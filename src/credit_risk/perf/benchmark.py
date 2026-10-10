"""Measure table layouts on copies of ``gold.contract_month``: none, liquid, partitioned, Z-ORDER.

Runs locally against the SQL warehouse. ``build`` (re)creates the copies in the ``sandbox``
schema, ``run`` executes a fixed set of queries on each copy and prints the Query History
metrics as a Markdown table for docs/perf_notes.md.

Usage:
    uv run python -m credit_risk.perf.benchmark --catalog <catalog> [--step build|run|all]
"""

import argparse
import statistics
import time
import uuid
from dataclasses import dataclass

SOURCE_TABLE = "gold.contract_month"
TARGET_PREFIX = "sandbox.contract_month_"
LAYOUT_KEYS = "sk_id_curr, month"
# The source is about 145 MB: with the default file size every copy is 1-4 files and there is
# nothing to skip. Small files on every copy make the layouts comparable.
FILE_SIZE_PROPERTY = "TBLPROPERTIES ('delta.targetFileSize' = '8mb')"
# Files written per copy before OPTIMIZE (OPTIMIZE does not split files that are too large).
FILES = 16


@dataclass(frozen=True)
class Layout:
    """A physical layout of the copy: the CREATE TABLE clause and the OPTIMIZE suffix."""

    create_clause: str
    optimize_suffix: str = ""


LAYOUTS: dict[str, Layout] = {
    "unclustered": Layout(""),
    "liquid": Layout(f"CLUSTER BY ({LAYOUT_KEYS})"),
    "partitioned": Layout("PARTITIONED BY (month)"),
    "zorder": Layout("", f"ZORDER BY ({LAYOUT_KEYS})"),
}

# Query name -> SQL with {table} (the copy), {application} and {client_id} placeholders.
QUERIES: dict[str, str] = {
    # One client: selective on the first layout key.
    "client_lookup": "SELECT * FROM {table} WHERE sk_id_curr = {client_id}",
    # One quarter out of eight years: selective on the second layout key.
    "month_range": (
        "SELECT count(*), sum(exposure_amount) FROM {table} "
        "WHERE month BETWEEN DATE'2017-01-01' AND DATE'2017-03-01'"
    ),
    # Join on the client key, no filter on the copy.
    "client_join": (
        "SELECT a.name_contract_type, count(*), max(c.dpd) FROM {table} c "
        "JOIN {application} a ON a.sk_id_curr = c.sk_id_curr GROUP BY a.name_contract_type"
    ),
    # Full scan with an aggregation by month.
    "month_aggregate": "SELECT month, count(*), count_if(dpd >= 90) FROM {table} GROUP BY month",
}

# Query History metrics reported per run; the first one is the duration.
METRICS: tuple[str, ...] = (
    "total_time_ms",
    "read_files_count",
    "pruned_files_count",
    "read_bytes",
    "network_sent_bytes",
)


def target_table(catalog: str, layout: str) -> str:
    """Return the name of the copy of the source table with ``layout``."""
    return f"{catalog}.{TARGET_PREFIX}{layout}"


def build_statements(catalog: str, layout: str) -> list[str]:
    """Return the statements that (re)create and compact the copy with ``layout``.

    Every copy is written from the same scattered rows (hash of ``sk_id_prev``), so none of them
    inherits the clustering of the source, with the same target file size, and is compacted with
    OPTIMIZE, so they differ only in layout.
    """
    spec = LAYOUTS[layout]
    table = target_table(catalog, layout)
    create = " ".join(
        part
        for part in (
            f"CREATE OR REPLACE TABLE {table}",
            spec.create_clause,
            FILE_SIZE_PROPERTY,
            f"AS SELECT /*+ REPARTITION({FILES}, sk_id_prev) */ * FROM {catalog}.{SOURCE_TABLE}",
        )
        if part
    )
    return [create, " ".join(part for part in (f"OPTIMIZE {table}", spec.optimize_suffix) if part)]


def query_statement(catalog: str, layout: str, query: str, client_id: int, run_id: str) -> str:
    """Return one benchmark query on the copy with ``layout``.

    ``run_id`` is returned as an extra column: the result differs per run, so the result cache
    of the warehouse cannot answer it (a comment or an always-true predicate does not prevent
    a cache hit).
    """
    sql = QUERIES[query].format(
        table=target_table(catalog, layout),
        application=f"{catalog}.silver.application",
        client_id=client_id,
    )
    return f"SELECT *, '{run_id}' AS run_id FROM ({sql})"


@dataclass(frozen=True)
class Run:
    """Metrics of one execution of ``query`` on the copy with ``layout``."""

    layout: str
    query: str
    metrics: dict[str, int]


def summarize(runs: list[Run]) -> list[tuple[str, str, dict[str, int]]]:
    """Return the median of every metric per (query, layout), in QUERIES / LAYOUTS order."""
    summary = []
    for query in QUERIES:
        for layout in LAYOUTS:
            group = [run.metrics for run in runs if (run.layout, run.query) == (layout, query)]
            if group:
                medians = {m: int(statistics.median(g[m] for g in group)) for m in METRICS}
                summary.append((query, layout, medians))
    return summary


def render_markdown(summary: list[tuple[str, str, dict[str, int]]]) -> str:
    """Render the summary as a Markdown table, one row per (query, layout)."""
    lines = [
        "| Query | Layout | " + " | ".join(METRICS) + " |",
        "|---|---|" + "---:|" * len(METRICS),
    ]
    for query, layout, medians in summary:
        values = " | ".join(f"{medians[m]:,}".replace(",", " ") for m in METRICS)
        lines.append(f"| `{query}` | {layout} | {values} |")
    return "\n".join(lines)


class Warehouse:
    """Statement execution and Query History metrics on the first SQL warehouse."""

    def __init__(self) -> None:
        # Imported here: the SDK is a local dev dependency and is not part of the wheel.
        from databricks.sdk import WorkspaceClient

        self._client = WorkspaceClient()
        self._warehouse_id = next(iter(self._client.warehouses.list())).id

    def execute(self, statement: str) -> tuple[str, list[dict[str, str | None]]]:
        """Run ``statement`` to completion; return its statement id and rows by column name."""
        from databricks.sdk.service.sql import StatementState

        response = self._client.statement_execution.execute_statement(
            statement=statement, warehouse_id=self._warehouse_id, wait_timeout="50s"
        )
        while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(2)
            response = self._client.statement_execution.get_statement(response.statement_id)
        if response.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(f"{statement[:80]}: {response.status.error}")
        columns = [column.name for column in response.manifest.schema.columns or []]
        rows = response.result.data_array if response.result else None
        return response.statement_id, [dict(zip(columns, row, strict=True)) for row in rows or []]

    def metrics(self, statement_ids: list[str]) -> dict[str, dict[str, int]]:
        """Return the final Query History metrics of ``statement_ids`` (they arrive with a lag)."""
        from databricks.sdk.service.sql import QueryFilter

        for _ in range(30):
            found = {}
            for start in range(0, len(statement_ids), 50):
                response = self._client.query_history.list(
                    filter_by=QueryFilter(statement_ids=statement_ids[start : start + 50]),
                    include_metrics=True,
                    max_results=100,
                )
                found |= {q.query_id: q for q in response.res or []}
            if len(found) == len(statement_ids) and all(
                q.is_final and q.metrics for q in found.values()
            ):
                cached = [q.query_id for q in found.values() if q.metrics.result_from_cache]
                if cached:
                    raise RuntimeError(f"Answered from the result cache: {cached}")
                return {
                    query_id: {m: getattr(q.metrics, m) or 0 for m in METRICS}
                    for query_id, q in found.items()
                }
            time.sleep(5)
        raise TimeoutError("Query History metrics are not final yet")


def build(warehouse: Warehouse, catalog: str) -> None:
    """(Re)create every copy and print its file count and size."""
    for layout in LAYOUTS:
        for statement in build_statements(catalog, layout):
            warehouse.execute(statement)
        _, [detail] = warehouse.execute(f"DESCRIBE DETAIL {target_table(catalog, layout)}")
        print(f"{layout}: {detail['numFiles']} files, {detail['sizeInBytes']} bytes")


def run(warehouse: Warehouse, catalog: str, runs: int) -> str:
    """Execute every query ``runs`` times on every copy; return the Markdown summary."""
    _, [client] = warehouse.execute(
        f"SELECT percentile_approx(sk_id_curr, 0.5) AS id FROM {catalog}.{SOURCE_TABLE}"
    )
    client_id = int(client["id"])
    executed: dict[str, tuple[str, str]] = {}
    # Layouts alternate inside a round, so no layout gets all its runs on a colder cache.
    for _ in range(runs):
        for query in QUERIES:
            for layout in LAYOUTS:
                statement = query_statement(
                    catalog, layout, query, int(client_id), uuid.uuid4().hex
                )
                statement_id, _ = warehouse.execute(statement)
                executed[statement_id] = (layout, query)
    metrics = warehouse.metrics(list(executed))
    return render_markdown(
        summarize(
            [Run(*executed[statement_id], metrics[statement_id]) for statement_id in executed]
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--step", choices=("build", "run", "all"), default="all")
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()

    warehouse = Warehouse()
    if args.step in ("build", "all"):
        build(warehouse, args.catalog)
    if args.step in ("run", "all"):
        print(run(warehouse, args.catalog, args.runs))


if __name__ == "__main__":
    main()
