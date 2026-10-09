# Data model

## Bronze

Source rows exactly as delivered, one table per Kaggle "Home Credit Default Risk" file.
Ingested with Auto Loader from the landing volume
`/Volumes/<catalog>/raw/landing/home_credit/<entity>/`.

| Table | Source file | Grain (not enforced in Bronze) |
|---|---|---|
| `bronze.application_train` | `application_train.csv` | one row per `SK_ID_CURR`, carries `TARGET` |
| `bronze.application_test` | `application_test.csv` | one row per `SK_ID_CURR`, no `TARGET` |
| `bronze.bureau` | `bureau.csv` | one row per `SK_ID_BUREAU` |
| `bronze.bureau_balance` | `bureau_balance.csv` | one row per `SK_ID_BUREAU` × `MONTHS_BALANCE` |
| `bronze.previous_application` | `previous_application.csv` | one row per `SK_ID_PREV` |
| `bronze.pos_cash_balance` | `POS_CASH_balance.csv` | one row per `SK_ID_PREV` × `MONTHS_BALANCE` |
| `bronze.credit_card_balance` | `credit_card_balance.csv` | one row per `SK_ID_PREV` × `MONTHS_BALANCE` |
| `bronze.installments_payments` | `installments_payments.csv` | one row per payment; several rows per installment |

Rules:

- Column names keep the source spelling and case (including the source typo `AMT_RECIVABLE`).
- Types are inferred; keys (`SK_ID_*` → `BIGINT`), key offsets and money amounts (`AMT_*` → `DOUBLE`)
  are pinned with schema hints in `src/credit_risk/common/schemas.py`. Decimal typing, sentinel
  handling and `XNA`/`XAP` cleanup happen in Silver.
- `application_train` and `application_test` are separate tables; they are never unioned in Bronze.
- No expectations, no deduplication, no partitioning or clustering.
- Metadata columns: `_ingested_at` (load timestamp), `_source_file` (`_metadata.file_path`),
  `_rescued_data` (values that did not fit the schema; schema evolution mode is `rescue`).

## Silver

Cleaned tables built as materialized views from Bronze in the same pipeline. Business logic lives
in `src/credit_risk/transformations/silver.py`, the rules in `src/credit_risk/common/expectations.py`.

| Table | Built from | Grain / key | Clustered by |
|---|---|---|---|
| `silver.application` | `bronze.application_train` ∪ `bronze.application_test` | one row per `sk_id_curr` | `sk_id_curr` |
| `silver.bureau` | `bronze.bureau` | one row per `sk_id_bureau` | `sk_id_curr` |
| `silver.bureau_balance` | `bronze.bureau_balance` | one row per `sk_id_bureau` × `months_balance` | `sk_id_bureau`, `month` |
| `silver.previous_application` | `bronze.previous_application` | one row per `sk_id_prev` | `sk_id_curr` |
| `silver.pos_cash_balance` | `bronze.pos_cash_balance` | one row per `sk_id_prev` × `months_balance` | `sk_id_curr`, `month` |
| `silver.credit_card_balance` | `bronze.credit_card_balance` | one row per `sk_id_prev` × `months_balance` | `sk_id_curr`, `month` |
| `silver.installments_payments` | `bronze.installments_payments` | one row per payment (several per installment) | `sk_id_curr`, `sk_id_prev` |

Each table has a companion `silver.<entity>_quarantine` (see Data quality below).

### Conventions

- **Names:** every column is the lowercase source name. One explicit rename fixes the source typo:
  `AMT_RECIVABLE` → `amt_receivable` (`credit_card_balance`).
- **Train / test:** `silver.application` is the only place where train and test are unioned. `is_train`
  tells them apart; `target` exists only for train rows and is null (never imputed) for test rows.
- **Types:** keys `BIGINT`; money columns (`amt_*` pinned in `common/schemas.py`) `DECIMAL(18,3)` —
  the source carries up to 3 decimal places, float noise in `bureau` is rounded; day/month offsets
  and counts `INT`. Columns not listed in the transformation keep their Bronze type.
- **`XNA` / `XAP`** → null in every business string column.
- **Sentinels** → null plus a boolean flag column:

  | Table | Column | Sentinel | Flag | Rows |
  |---|---|---|---|---|
  | `application` | `days_employed` | 365243 | `days_employed_anomaly` | 64 648 |
  | `previous_application` | `days_first_drawing` | 365243 | `days_first_drawing_anomaly` | 934 444 |
  | `previous_application` | `days_first_due` | 365243 | `days_first_due_anomaly` | 40 645 |
  | `previous_application` | `days_last_due_1st_version` | 365243 | `days_last_due_1st_version_anomaly` | 93 864 |
  | `previous_application` | `days_last_due` | 365243 | `days_last_due_anomaly` | 211 221 |
  | `previous_application` | `days_termination` | 365243 | `days_termination_anomaly` | 225 913 |
  | `previous_application` | `sellerplace_area` | -1 | `sellerplace_area_anomaly` | 762 675 |

- **`bureau_balance.status`** is kept and mapped to `dpd_bucket` (`0` = no DPD, `1..5` = DPD buckets,
  null for `C` / `X`), `is_closed` (`C`) and `is_unknown` (`X`). No day counts are derived from it.
- **`sk_dpd` / `sk_dpd_def`** in `pos_cash_balance` and `credit_card_balance` are the source values.
- **Deduplication** is deterministic: one row per key, ordered by `_ingested_at` desc, `_source_file`
  desc, then a hash of the row. `installments_payments` has no key narrower than the whole source row
  (partial payments), so only exact duplicates are removed. The current source files contain no
  duplicates; the step guards against re-delivered files.
- **Metadata:** `_ingested_at`, `_source_file` and `_rescued_data` are carried over from Bronze.
  `_rescued_data` is always null in `silver.<entity>` (rows with rescued data are quarantined).

### Synthetic calendar

The source has **no calendar dates**: `DAYS_*` are day offsets and `MONTHS_BALANCE` month offsets from
the application date. Silver maps them to the calendar through one synthetic `ANCHOR_DATE = 2018-05-01`
(`common/config.py`), i.e. every application is treated as filed on that day. The mapping exists only
in `transformations/calendar.py` (`to_calendar_date`, `to_calendar_month`). The resulting dates are
**not real**; they only make offsets comparable on one time axis and joinable to monthly macro data.

| Table | Derived column | From |
|---|---|---|
| `bureau` | `credit_date`, `credit_end_date`, `credit_end_fact_date`, `credit_update_date` | `days_credit`, `days_credit_enddate`, `days_enddate_fact`, `days_credit_update` |
| `previous_application` | `decision_date` | `days_decision` |
| `bureau_balance`, `pos_cash_balance`, `credit_card_balance` | `month` (first day of month) | `months_balance` |
| `installments_payments` | `instalment_date`, `entry_payment_date` | `days_instalment`, `days_entry_payment` |

### Data quality

| Action | Meaning | Rules |
|---|---|---|
| `expect_or_fail` | update stops | keys not null; `application.target_matches_is_train` |
| `expect_or_drop` | row goes to `silver.<entity>_quarantine` with `_failed_rules` | `no_rescued_data`, `months_balance_not_positive`, `status_in_domain`, `sk_dpd_not_negative`, `instalment_identified` |
| `expect` (warn) | violation is only counted in the event log | plausibility (`amt_credit_positive`, `days_credit_enddate_plausible`, …) and parent presence |

`silver.<entity>` and `silver.<entity>_quarantine` are complementary: together they hold exactly the
deduplicated Bronze rows. With the current source files every quarantine table is empty.

**Orphan contracts.** The Kaggle files are a sample, so child rows exist whose parent contract was not
delivered. They are kept, flagged and reported as warnings (`bureau_record_exists`,
`previous_application_exists`) instead of failing the update:

| Child | Flag | Rows without parent |
|---|---|---|
| `bureau_balance` → `bureau` | `has_bureau_record` | 3 120 184 (43 041 contracts) |
| `pos_cash_balance` → `previous_application` | `has_previous_application` | 340 561 |
| `credit_card_balance` → `previous_application` | `has_previous_application` | 1 082 816 |
| `installments_payments` → `previous_application` | `has_previous_application` | 1 250 826 |

`sk_id_curr` of every child table resolves to `silver.application` (checked in the integration tests).

### Installments

`silver.installments` is derived from `silver.installments_payments` (`transformations/dpd.py`):
one row per `sk_id_prev` × `num_instalment_number` × `num_instalment_version`, clustered by
`sk_id_curr`, `sk_id_prev`. It has no quarantine table, its input is already clean.

| Column | Meaning |
|---|---|
| `instalment_date`, `amt_instalment` | due date (synthetic) and amount, constant within the key |
| `amt_paid`, `n_payments` | sum and number of the payments made |
| `first_payment_date`, `last_payment_date` | first and last payment |
| `paid_in_full_date` | first payment date on which the running sum of payments covers `amt_instalment`; null if never |
| `is_paid_in_full` | `amt_paid >= amt_instalment` |
| `dpd_at_settlement` | days from the due date to `paid_in_full_date`, never negative; null while unsettled |

## Gold

Materialized views in the same pipeline, logic in `src/credit_risk/transformations/gold.py`. The
default definition is described in [default_definition.md](default_definition.md).

| Table | Built from | Grain / key | Clustered by |
|---|---|---|---|
| `gold.contract_month` | `silver.pos_cash_balance` ∪ `silver.credit_card_balance`, `silver.installments`, `silver.previous_application` | one row per `sk_id_prev` × `month` | `sk_id_curr`, `month` |
| `gold.client_month` | `gold.contract_month`, `silver.bureau_balance`, `silver.bureau` | one row per `sk_id_curr` × `month` | `sk_id_curr`, `month` |

`month` is the first day of the synthetic calendar month. Neither table carries `TARGET` or
application attributes. Macro columns are not there yet (ECB phase).

### `gold.contract_month`

A row exists for every month with a balance row or with an installment due or overdue.

| Column | Meaning |
|---|---|
| `contract_type` | `pos_cash`, `credit_card`; null when the month has installments only |
| `name_contract_status` | balance status of the month |
| `source_dpd`, `source_dpd_def` | `sk_dpd`, `sk_dpd_def` as delivered |
| `installment_dpd` | DPD computed from the installments |
| `dpd`, `dpd_source` | `source_dpd` if present (`balance`), otherwise `installment_dpd` (`installments`) |
| `is_active` | status is `Active`, `Signed` or `Demand`; true for installment-only months |
| `exposure_amount` | cards: `amt_balance`; POS / cash: `cnt_instalment_future × amt_annuity` (estimate) |
| `has_previous_application` | false for orphan contracts |

### `gold.client_month`

A row exists for every month in which the client has a contract row or a credit bureau status.

| Column | Meaning |
|---|---|
| `max_dpd`, `max_dpd_def` | maximum `dpd` / `source_dpd_def` over the client's contracts; null without a contract row |
| `dpd_bucket` | `0`, `1-29`, `30-59`, `60-89`, `90+` |
| `default_flag` | `max_dpd >= 90` |
| `default_start_month` | first month of the current uninterrupted default episode; null outside default |
| `exposure_amount` | sum over active contracts |
| `n_active_contracts`, `n_contracts` | active / all contract rows of the month |
| `bureau_max_dpd_bucket`, `bureau_default_flag`, `n_bureau_active_contracts` | credit bureau statuses, kept apart from `max_dpd` |

### Data quality

Rules are in `DERIVED_RULES` (`common/expectations.py`). `expect_or_fail`: keys not null,
`default_flag_matches_max_dpd`, `default_start_month_matches_flag`. Warnings:
`installment_dpd_default_matches_source` (reconciliation of computed and source DPD),
`exposure_amount_not_negative`, `previous_application_exists`, `instalment_paid_in_full`,
`amt_paid_not_above_instalment`. Key uniqueness is an integration test.
