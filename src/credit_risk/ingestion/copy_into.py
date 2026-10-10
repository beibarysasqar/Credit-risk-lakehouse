"""COPY INTO statements of the comparison demo; Auto Loader is the project's ingestion path."""


def create_table_statement(table: str) -> str:
    """Return the statement that creates ``table`` without columns, for COPY INTO to fill."""
    return f"CREATE TABLE IF NOT EXISTS {table}"


def copy_into_statement(table: str, path: str) -> str:
    """Return the COPY INTO statement that loads the CSV files under ``path`` into ``table``.

    The schema is inferred from the files and merged into the empty table. COPY INTO keeps track
    of the files it has loaded, so a rerun loads only new ones.
    """
    return (
        f"COPY INTO {table} FROM '{path}' "
        "FILEFORMAT = CSV "
        "FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true') "
        "COPY_OPTIONS ('mergeSchema' = 'true')"
    )
