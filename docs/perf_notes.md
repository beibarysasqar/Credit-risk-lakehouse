# Performance notes

Everything here was measured on the dev workspace (Databricks Free Edition, serverless SQL
warehouse `2X-Small`, Photon) on 2026-10-11. Serverless has no classic Spark UI: the numbers come
from the Query History API (the data behind Query Profile) and from `EXPLAIN FORMATTED`.

## Table layout: liquid clustering vs partitioning vs Z-ORDER

**Question.** `CLAUDE.md` says: do not partition, use liquid clustering on (`sk_id_curr`, `month`).
Does that hold on the largest Gold table?

**Setup.** `uv run python -m credit_risk.perf.benchmark --catalog <catalog>` builds four copies of
`gold.contract_month` (13 932 955 rows, 96 months, 340 365 clients) in the `sandbox` schema and runs
four queries on each copy, three runs per query. The table reports the median.

| Copy | Layout | Files | Size |
|---|---|---:|---:|
| `sandbox.contract_month_unclustered` | none | 16 | 138.3 MB |
| `sandbox.contract_month_liquid` | `CLUSTER BY (sk_id_curr, month)` | 16 | 140.0 MB |
| `sandbox.contract_month_partitioned` | `PARTITIONED BY (month)` | 96 | 149.6 MB |
| `sandbox.contract_month_zorder` | `OPTIMIZE … ZORDER BY (sk_id_curr, month)` | 16 | 139.1 MB |

How the comparison is kept fair:

- Every copy is written from the same rows, scattered by `REPARTITION(16, sk_id_prev)`, so none
  inherits the clustering of the source materialized view, and every copy is compacted with
  `OPTIMIZE`.
- **The file size is reduced on purpose** (`delta.targetFileSize = 8mb`). With the default file
  size the first build produced 4 / 1 / 96 / 4 files: the liquid copy was a single file, so there
  was nothing to skip and nothing to compare. The absolute numbers below therefore describe the
  mechanics, not the production tables.
- Every run returns a unique `run_id` column. A comment or an always-true predicate with a unique
  literal was not enough: 32 of 48 statements were answered from the warehouse result cache. The
  benchmark fails if Query History reports `result_from_cache`.
- Layouts alternate inside a round, so no layout gets all its runs on a colder disk cache.

**Queries.**

| Query | What it does |
|---|---|
| `client_lookup` | all months of one client (`sk_id_curr = <median id>`) |
| `month_range` | count and exposure of one quarter (`month` between 2017-01 and 2017-03) |
| `client_join` | join with `silver.application` on `sk_id_curr`, grouped by contract type |
| `month_aggregate` | full scan, rows and 90+ DPD rows per month |

**Results** (median of 3 runs; duration in ms, sizes in bytes).

| Query | Layout | Duration | Files read | Files pruned | Bytes read | Network sent |
|---|---|---:|---:|---:|---:|---:|
| `client_lookup` | unclustered | 598 | 16 | 0 | 41 523 936 | 0 |
| `client_lookup` | liquid | 539 | 4 | 12 | 36 800 187 | 0 |
| `client_lookup` | partitioned | 818 | 96 | 0 | 78 693 786 | 0 |
| `client_lookup` | zorder | 553 | 4 | 12 | 36 713 618 | 0 |
| `month_range` | unclustered | 595 | 16 | 0 | 76 017 507 | 0 |
| `month_range` | liquid | 624 | 5 | 11 | 22 421 887 | 0 |
| `month_range` | partitioned | 643 | 3 | 93 | 4 618 670 | 0 |
| `month_range` | zorder | 603 | 4 | 12 | 16 821 523 | 0 |
| `client_join` | unclustered | 1 004 | 18 | 0 | 36 973 614 | 0 |
| `client_join` | liquid | 1 254 | 18 | 0 | 36 622 109 | 0 |
| `client_join` | partitioned | 1 089 | 98 | 0 | 60 826 262 | 0 |
| `client_join` | zorder | 1 084 | 18 | 0 | 36 482 070 | 0 |
| `month_aggregate` | unclustered | 599 | 16 | 0 | 17 995 191 | 0 |
| `month_aggregate` | liquid | 559 | 16 | 0 | 14 027 900 | 0 |
| `month_aggregate` | partitioned | 607 | 96 | 0 | 26 758 341 | 0 |
| `month_aggregate` | zorder | 523 | 16 | 0 | 14 030 300 | 0 |

`network_sent_bytes` (the shuffle metric of Query History) is reported as 0 for every statement
on this warehouse, so shuffle volume cannot be compared here.

**Reading the numbers.**

- **Skipping works as designed.** Liquid clustering and Z-ORDER prune 11–12 of 16 files on both
  keys. Partitioning by `month` prunes 93 of 96 files for the month filter and nothing for the
  client filter, where it has to open all 96 files and reads about twice the bytes of the
  unclustered copy.
- **Durations do not separate the layouts.** Everything is between 0.5 and 1.3 s and no layout
  wins consistently (liquid is the fastest lookup and the slowest join): at 140 MB the scan is not
  the bottleneck. No layout can be called faster on this data.
- **The join is not helped by any layout.** `EXPLAIN FORMATTED` shows a `PhotonBroadcastHashJoin`
  with `silver.application` on the build side and a full scan of the copy; no files are pruned.
  The optimizer already broadcasts without a hint.
- **Liquid clustering and Z-ORDER are equivalent here** in files and bytes read.

**Conclusion.** The rule stays: no partitioning. It gives 96 files of about 1.5 MB, helps exactly
one filter and hurts the client lookup. Liquid clustering on (`sk_id_curr`, `month`) serves both
filters, needs no `OPTIMIZE … ZORDER` reruns, and is what the pipeline tables use. The honest
caveat is the scale: with the default file size these tables are 1–4 files, so on this dataset
the layout is a correctness-of-design choice, not a measurable speed-up.

The copies stay in `sandbox` until dropped; rerunning `--step build` replaces them.

## Ingestion: COPY INTO vs Auto Loader

`databricks bundle run copy_into_demo_job -t <target>` loads `bureau.csv` a second time, with
`COPY INTO`, into the job-owned `sandbox.bureau_copy_into`. The project's ingestion path is Auto
Loader (`bronze.bureau`); the demo table is never read by Silver.

| | Auto Loader (`bronze.bureau`) | COPY INTO (`sandbox.bureau_copy_into`) |
|---|---|---|
| Rows loaded | 1 716 428 | 1 716 428 |
| Rerun without new files | no rows (pipeline checkpoint) | `num_inserted_rows = 0` (file tracking in the table) |
| Data files / bytes written | 1 / 24 954 741 | 3 / 24 443 688 |
| Key types (`SK_ID_CURR`, `SK_ID_BUREAU`) | `bigint`, pinned by schema hints | `int`, inferred |
| Other columns | same inferred types | same inferred types |
| Values that do not fit the schema | kept in `_rescued_data` | not captured with the options used |
| Ingestion metadata | `_ingested_at`, `_source_file` | none unless selected explicitly |
| State | checkpoint and schema location managed by the pipeline | none outside the table |
| Runs as | streaming flow of the pipeline | one SQL statement |

Measured durations: the two demo job runs took 44 s and 34 s wall-clock including serverless
start-up; inside the first run the `COPY INTO` commit came 13 s after the `CREATE TABLE`. The Auto
Loader flow runs inside a pipeline update together with all other flows, so there is no
like-for-like duration and none is claimed.

After casting the two keys, the tables hold the same rows (`EXCEPT ALL` in the integration tests).

**Why Auto Loader stays the ingestion path.** Both are idempotent, and for one static file COPY
INTO is simpler. Auto Loader is used because it is a stream: it feeds the pipeline's streaming
tables and the SCD2 change feed, keeps unexpected values in `_rescued_data`, and takes schema
hints, which is what pins the keys to `bigint`. With COPY INTO the same key column would have
been `int` in Bronze and `bigint` elsewhere.
