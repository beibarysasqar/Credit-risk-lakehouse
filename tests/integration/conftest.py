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
