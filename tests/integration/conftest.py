"""Fixtures for tests that query the deployed dev catalog through a SQL warehouse."""

import os
from collections.abc import Callable

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

SqlRunner = Callable[[str], list[list[str | None]]]


@pytest.fixture(scope="session")
def catalog() -> str:
    return os.environ.get("CREDIT_RISK_CATALOG", "credit_risk_dev")


@pytest.fixture(scope="session")
def sql(catalog: str) -> SqlRunner:
    """Run a statement on the first available warehouse and return rows as lists of strings."""
    client = WorkspaceClient()
    warehouse_id = (
        os.environ.get("DATABRICKS_WAREHOUSE_ID") or next(iter(client.warehouses.list())).id
    )

    def run(statement: str) -> list[list[str | None]]:
        response = client.statement_execution.execute_statement(
            statement=statement, warehouse_id=warehouse_id, catalog=catalog, wait_timeout="50s"
        )
        while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
            response = client.statement_execution.get_statement(response.statement_id)
        assert response.status.state == StatementState.SUCCEEDED, response.status.error
        return response.result.data_array or []

    return run


@pytest.fixture(scope="session")
def failed_expectations(sql: SqlRunner, catalog: str) -> dict[tuple[str, str], int]:
    """Failed records per (table, expectation name) in the latest pipeline update."""
    log = f"event_log(TABLE({catalog}.silver.application))"
    rows = sql(
        f"""
        WITH latest AS (
          SELECT origin.update_id AS update_id FROM {log}
          WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1
        ),
        expectations AS (
          SELECT explode(from_json(
            details:flow_progress.data_quality.expectations,
            'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'
          )) AS e
          FROM {log}
          WHERE event_type = 'flow_progress' AND origin.update_id = (SELECT update_id FROM latest)
        )
        SELECT e.dataset, e.name, sum(e.passed_records), sum(e.failed_records)
        FROM expectations GROUP BY ALL
        """
    )
    return {(dataset.split(".")[-1], name): int(bad) for dataset, name, _, bad in rows}
