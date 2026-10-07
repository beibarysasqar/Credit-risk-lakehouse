# CLAUDE.md — Credit Risk Lakehouse

Databricks Free Edition · PySpark · Delta · Lakeflow Declarative Pipelines · Unity Catalog · Asset Bundles.
Goal: a bank-grade medallion lakehouse on Home Credit Default Risk + ECB macro data, with an SCD2 history base for model validation.

Communicate with me in Russian; code, comments, commit messages and docs in English.

---

## Environment

- macOS (Apple Silicon), PyCharm, Python **3.12**, dependency manager **uv**. Interpreter = `.venv/bin/python`.
- Local Spark needs Java 17: `brew install openjdk@17`. The formula is keg-only, so `/usr/libexec/java_home` does not see it; use `JAVA_HOME=$(brew --prefix openjdk@17)`. `tests/conftest.py` sets this automatically when `JAVA_HOME` is unset.
- Pin `pyspark` and `delta-spark` to the Spark major/minor of the serverless environment (check `spark.version` in the workspace before bumping).
- **Never install `databricks-connect` into `.venv`** — it conflicts with `pyspark`. If interactive remote debugging is needed, use a separate `.venv-connect`.
- Databricks CLI auth: profile `DEFAULT` in `~/.databrickscfg`. Never read, print or commit tokens.

## Commands

```bash
uv sync                                   # install deps
uv run ruff check . && uv run ruff format --check .
uv run pytest tests/unit -q               # fast, local Spark
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run <job_or_pipeline_key> -t dev
databricks bundle run <pipeline_key> -t dev --refresh <table>   # partial refresh
uv run pytest tests/integration -q        # asserts against dev catalog via SQL
```

Definition of done for any change: ruff clean → unit tests pass → `bundle validate` passes → deployed and run on `dev` if pipeline/job code changed.

## Repository layout

```
databricks.yml              # bundle root, targets dev / prod
resources/                  # *.yml: pipelines, jobs (one resource per file)
src/credit_risk/
  common/                   # config.py (catalog, anchor date, ECB series), schemas.py
  transformations/          # PURE functions: DataFrame in → DataFrame out
  pipelines/                # thin Lakeflow wrappers: bronze.py, silver.py, gold.py, history.py
  ingestion/                # ecb.py, kaggle landing helpers
  streaming/                # payment-stream generator + consumer
tests/unit/  tests/integration/  tests/fixtures/   # tiny hand-made CSV/JSON fixtures
infra/terraform/
docs/                       # default_definition.md, data_model.md, perf_notes.md
```

Raw Kaggle files and any data samples larger than fixtures are **never** committed (`data/`, `*.csv` outside `tests/fixtures` are gitignored).

## Core code rules

- Business logic lives only in `transformations/`. Pipeline files contain decorators, table names, expectations and a call into a transformation — nothing else. This is what makes the logic unit-testable locally.
- Lakeflow API: `from pyspark import pipelines as dp`. Do not mix with legacy `import dlt` in the same codebase.
- No `collect()`, `toPandas()`, `count()` for control flow, Python UDFs, or row loops in pipeline code. Use built-in functions; if a UDF is unavoidable, use a pandas UDF and justify it in a comment.
- Serverless restrictions: no `sparkContext`/RDD API, no `.cache()/.persist()`, no custom Spark confs beyond the supported list, no DBFS root paths. All files live in UC Volumes: `/Volumes/<catalog>/raw/landing/...`.
- Catalog name is never hardcoded: bundle variable `catalog` → pipeline/job parameter → `config.py`. Targets: `dev` → `credit_risk_dev`, `prod` → `credit_risk_prod`.
- Table names: `<layer>.<entity>` in snake_case, e.g. `silver.installments_payments`, `gold.client_month`. History tables: `history.<entity>_scd2`.
- Every function in `transformations/` has type hints, a docstring stating grain (one row per …) and keys, and at least one unit test.

## Data domain — Home Credit specifics (do not "fix" without asking)

- Tables: `application_train/test`, `bureau`, `bureau_balance`, `previous_application`, `POS_CASH_balance`, `credit_card_balance`, `installments_payments`. Keys: `SK_ID_CURR` (client/application), `SK_ID_PREV` (previous Home Credit loan), `SK_ID_BUREAU` (credit bureau loan).
- `TARGET` exists only in `application_train`. Never union train and test without an explicit `is_train` flag; never impute or leak `TARGET` into features.
- **There are no calendar dates.** All `DAYS_*` are negative offsets from the application date, `MONTHS_BALANCE` are offsets from it in months. Calendar mapping uses a single synthetic `ANCHOR_DATE` in `config.py`; every derived date must go through one helper (`to_calendar_date`, `to_calendar_month`). Document that the mapping is synthetic.
- `DAYS_EMPLOYED = 365243` is a sentinel → null + flag column `days_employed_anomaly`. Same pattern for other sentinels discovered in profiling; `XNA`/`XAP` → null in Silver.
- `installments_payments` contains multiple rows per installment (partial payments). Aggregate per (`SK_ID_PREV`, `NUM_INSTALMENT_NUMBER`, `NUM_INSTALMENT_VERSION`) before computing DPD.
- `bureau_balance.STATUS`: `0` = no DPD, `1..5` = DPD buckets (5 = 120+ or written off), `C` = closed, `X` = unknown. Map to ordinal bucket + `is_closed` + `is_unknown`, not to fake day counts.
- `POS_CASH_balance` / `credit_card_balance` already carry `SK_DPD` and `SK_DPD_DEF`; use the source values and reconcile against computed installment DPD in a data-quality check, don't overwrite.
- Macro data (ECB) is joined by calendar month via the anchor mapping. Home Credit's country is not disclosed, so the macro join is illustrative — say so in `docs/data_model.md`.

## Gold: `gold.client_month`

- Grain: one row per `SK_ID_CURR` × `month`. Uniqueness of this key is an integration test.
- Columns at minimum: `max_dpd`, `dpd_bucket`, `default_flag`, `default_start_month`, `exposure_amount`, `n_active_contracts`, macro columns.
- **Default definition = 90+ DPD** on any contract in the month (DPD component of CRR Art. 178 only; unlikeliness-to-pay and materiality thresholds are out of scope). Any change to the definition requires updating `docs/default_definition.md` and the tests in the same commit.
- Point-in-time correctness: features for month M use only data with offset ≤ M. Add a test that would fail on look-ahead.

## History base (SCD2 + time travel)

- SCD2 via Lakeflow AUTO CDC (`dp.create_auto_cdc_flow`, `stored_as_scd_type=2`) for client/contract attributes; columns `__START_AT`/`__END_AT` are the validity interval. Use MERGE in a job only if AUTO CDC cannot express the case.
- Time travel is **not** the history store: VACUUM removes old versions. SCD2 tables are the durable history; time travel is for audit/rollback demos and reproducing a model-validation snapshot (`VERSION AS OF` recorded in the validation job output).
- Model-validation snapshots: a job writes `history.validation_snapshot` with `snapshot_id`, source table versions and as-of month, so any snapshot is reproducible.

## Data quality (expectations)

- Bronze: no expectations, keep `_rescued_data`, add `_ingested_at`, `_source_file` (from `_metadata.file_path`).
- Silver: key not null / referential integrity → `expect_or_fail` on keys, `expect_or_drop` with a quarantine table `silver.<entity>_quarantine` for row-level garbage, `expect` (warn) for business plausibility (e.g. `AMT_CREDIT > 0`).
- Expectation names are snake_case and stable — they are queried from the event log for DQ reporting.
- Dedup in Silver is deterministic: explicit key + ordering column; never `dropDuplicates()` without a subset.

## Ingestion

- Kaggle CSVs land in the Volume `raw/landing/home_credit/<table>/`. Auto Loader with `cloudFiles.schemaLocation` in the same Volume, schema hints for keys and amounts; COPY INTO is used only in the comparison demo job.
- ECB: `https://data-api.ecb.europa.eu/service/data/{flow}/{key}?format=csvdata`. Old `sdw-wsrest` URLs are dead — never use them. Series keys are pinned in `config.py`. If outbound access from serverless is blocked, fetch locally via `ingestion/ecb.py` and upload to the Volume; don't silently skip.
- Streaming: generator replays `installments_payments` as JSON micro-files into a Volume; consumer uses Structured Streaming with `trigger(availableNow=True)` only (the only trigger supported on serverless) and a checkpoint in the Volume.

## Performance

- Dataset is ~GB scale: **do not partition** tables by default. Use liquid clustering (`CLUSTER BY`) on join/filter keys (`SK_ID_CURR`, `month`). Partitioning and Z-ORDER exist only as a measured experiment in `docs/perf_notes.md`.
- Serverless has no classic Spark UI — analyse with Query Profile and `EXPLAIN FORMATTED`. Every performance claim in docs has before/after numbers (duration, files read, shuffle bytes).
- Prefer broadcast hints only after checking the plan; don't sprinkle them.

## Testing

- Unit: session-scoped local Spark fixture with Delta configured, `spark.sql.shuffle.partitions=2`, UI disabled. Compare DataFrames with `pyspark.testing.assertDataFrameEqual`. Fixtures are tiny and hand-crafted to hit edge cases (partial payments, sentinels, `X`/`C` statuses, month boundaries at DPD 89/90).
- Integration: run against `credit_risk_dev` after `bundle run`; check row counts vs source, key uniqueness, expectation pass rates from the event log, default rate in train ≈ 8.07%.
- Bug fix = failing test first, then the fix.

## CI/CD

- GitHub Actions: on PR → ruff, unit tests, `bundle validate -t dev`; on merge to `main` → `bundle deploy -t prod`. Secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN` in repo secrets only.
- Ownership split: **Terraform** owns catalogs, schemas, volumes, grants; **bundles** own pipelines and jobs. Never define the same resource in both.
- Free Edition uses Default Storage: `databricks catalogs create` (REST) fails with "Metastore storage root URL does not exist"; `CREATE CATALOG` via SQL on the serverless warehouse works. `credit_risk_dev` was bootstrapped that way in phase 0 and must be `terraform import`ed, not recreated.
- Conventional Commits (`feat:`, `fix:`, `refactor:`, `test:`, `ci:`, `docs:`). One logical change per commit.

## Workspace permissions for Claude

Claude may validate, deploy and run anything in the **dev** target without asking. Ask first before:
- any action on the `prod` target or `credit_risk_prod` catalog;
- `--full-refresh` of a pipeline or rerunning full ingestion (Free Edition compute quota is limited);
- `DROP`, `TRUNCATE`, `DELETE`, `VACUUM`, `RESTORE`, deleting Volume files or checkpoints;
- changing grants or Terraform state (`terraform apply`).

After every run report: run URL, status, duration, and failed expectations if any.

## How to work with me
- Start each phase in plan mode: propose the plan and file list, wait for my OK, then implement.
- After implementing, run the relevant commands yourself and fix errors before reporting back.
- Never print, log or commit API keys. If a key is missing, stop and tell me which env var to set.
- Keep changes small; suggest a conventional commit message at the end of each step.
- If an API response differs from what this file says, trust the real response and update this file.