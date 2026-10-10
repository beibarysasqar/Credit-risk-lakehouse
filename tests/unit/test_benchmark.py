from credit_risk.perf.benchmark import (
    LAYOUTS,
    METRICS,
    QUERIES,
    Run,
    build_statements,
    query_statement,
    render_markdown,
    summarize,
    target_table,
)


def _metrics(duration: int, files: int) -> dict[str, int]:
    return dict.fromkeys(METRICS, 0) | {"total_time_ms": duration, "read_files_count": files}


def test_every_layout_is_built_from_the_same_scattered_rows_and_compacted() -> None:
    source = (
        "TBLPROPERTIES ('delta.targetFileSize' = '8mb') "
        "AS SELECT /*+ REPARTITION(16, sk_id_prev) */ * FROM c.gold.contract_month"
    )

    assert build_statements("c", "unclustered") == [
        f"CREATE OR REPLACE TABLE c.sandbox.contract_month_unclustered {source}",
        "OPTIMIZE c.sandbox.contract_month_unclustered",
    ]
    assert build_statements("c", "liquid") == [
        "CREATE OR REPLACE TABLE c.sandbox.contract_month_liquid "
        f"CLUSTER BY (sk_id_curr, month) {source}",
        "OPTIMIZE c.sandbox.contract_month_liquid",
    ]
    assert build_statements("c", "partitioned") == [
        "CREATE OR REPLACE TABLE c.sandbox.contract_month_partitioned "
        f"PARTITIONED BY (month) {source}",
        "OPTIMIZE c.sandbox.contract_month_partitioned",
    ]
    assert build_statements("c", "zorder") == [
        f"CREATE OR REPLACE TABLE c.sandbox.contract_month_zorder {source}",
        "OPTIMIZE c.sandbox.contract_month_zorder ZORDER BY (sk_id_curr, month)",
    ]


def test_copies_live_in_the_sandbox_schema() -> None:
    assert all(target_table("c", layout).startswith("c.sandbox.") for layout in LAYOUTS)


def test_query_statement_targets_the_copy_and_is_unique_per_run() -> None:
    first = query_statement("c", "liquid", "client_lookup", 42, "run1")
    second = query_statement("c", "liquid", "client_lookup", 42, "run2")

    assert first == (
        "SELECT *, 'run1' AS run_id "
        "FROM (SELECT * FROM c.sandbox.contract_month_liquid WHERE sk_id_curr = 42)"
    )
    assert first != second
    assert "c.silver.application" in query_statement("c", "zorder", "client_join", 42, "run1")


def test_every_query_reads_only_the_copy_under_test() -> None:
    for query in QUERIES:
        statement = query_statement("c", "partitioned", query, 42, "run1")
        assert "c.sandbox.contract_month_partitioned" in statement
        assert "gold.contract_month" not in statement


def test_summarize_takes_the_median_so_a_cold_first_run_does_not_dominate() -> None:
    runs = [
        Run("liquid", "client_lookup", _metrics(900, 2)),
        Run("liquid", "client_lookup", _metrics(100, 2)),
        Run("liquid", "client_lookup", _metrics(120, 2)),
        Run("unclustered", "client_lookup", _metrics(300, 8)),
        Run("liquid", "month_range", _metrics(50, 4)),
    ]

    summary = summarize(runs)

    assert [(query, layout) for query, layout, _ in summary] == [
        ("client_lookup", "unclustered"),
        ("client_lookup", "liquid"),
        ("month_range", "liquid"),
    ]
    assert summary[1][2]["total_time_ms"] == 120
    assert summary[1][2]["read_files_count"] == 2


def test_render_markdown_has_one_row_per_query_and_layout() -> None:
    table = render_markdown(summarize([Run("zorder", "month_range", _metrics(1234, 5))]))

    header, separator, row = table.splitlines()
    assert header.startswith("| Query | Layout | total_time_ms | read_files_count |")
    assert separator.count("---:") == len(METRICS)
    assert row.startswith("| `month_range` | zorder | 1 234 | 5 |")
