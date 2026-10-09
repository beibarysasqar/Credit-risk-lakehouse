# Default definition

**A client is in default in a month when at least one of their Home Credit contracts reaches
90 or more days past due (DPD) in that month.**

This is the DPD component of CRR Art. 178 only. Unlikeliness-to-pay and the materiality thresholds
are out of scope. The threshold lives in `common/config.py` (`DEFAULT_DPD_THRESHOLD = 90`), the logic
in `transformations/dpd.py` and `transformations/gold.py`. Any change to the definition must update
this document and the tests in the same commit.

`default_flag` describes the client's **previous** Home Credit contracts in the months before the
application. It is not `TARGET` (payment difficulties on the *current* loan), and `TARGET` is never
used to build it.

## Where DPD comes from

| Contract month has | `dpd` used | `dpd_source` |
|---|---|---|
| a row in `pos_cash_balance` or `credit_card_balance` | source `sk_dpd` | `balance` |
| no balance row, but an installment due or overdue | computed `installment_dpd` | `installments` |

- The source value is never overwritten. Where both exist, `gold.contract_month` keeps both and the
  warn expectation `installment_dpd_default_matches_source` reports the months in which they disagree
  on 90+ (see Reconciliation).
- `sk_dpd`, not `sk_dpd_def`, drives the flag. `sk_dpd_def` is the source DPD "with tolerance" (small
  debts ignored), i.e. a materiality filter, which is out of scope. It is published as
  `source_dpd_def` / `max_dpd_def` for comparison; in the source `sk_dpd >= 90` occurs on
  119 274 POS and 48 377 card months, `sk_dpd_def >= 90` on 4 688 and 1 078.

### Computed installment DPD

1. Payments are aggregated to installments (`silver.installments`, key `sk_id_prev`,
   `num_instalment_number`, `num_instalment_version`). An installment is settled on
   `paid_in_full_date`: the first payment date on which the running sum of payments covers
   `amt_instalment`. Partial payments do not settle it.
2. An installment is overdue from its due date until `paid_in_full_date`. If it was never paid in
   full, it stays overdue until the last observed day (offset −1 from the application date,
   `OBSERVATION_END_OFFSET_DAYS`).
3. In a month the installment counts with the DPD reached on the last day of the month, or on the
   settlement day if that comes first. The contract's `installment_dpd` is the maximum over its
   installments, so the oldest unpaid installment sets it.

Every version of an installment number is a separate installment (89 897 number/contract pairs have
more than one version in the source).

## Client level (`gold.client_month`)

- `max_dpd` — maximum `dpd` over the client's contract rows of the month; null when the client has no
  Home Credit contract row in that month (bureau status only).
- `dpd_bucket` — `0`, `1-29`, `30-59`, `60-89`, `90+`.
- `default_flag` — `max_dpd >= 90`; false when `max_dpd` is null.
- `default_start_month` — first month of the current uninterrupted default episode; null outside
  default. A month without default **or a month without any row for the client** ends the episode;
  the next default month starts a new one.

Point-in-time: every column of month M uses only data dated up to the end of M. Unit tests
(`test_no_look_ahead`, `test_client_month_has_no_look_ahead`) fail if later payments or later months
change an earlier month.

## Credit bureau

Bureau statuses are ordinal buckets, not day counts, so they are **not** merged into `max_dpd` or
`default_flag`. They are published separately:

- `bureau_max_dpd_bucket` — maximum bucket over the client's bureau contracts with a known open status;
- `bureau_default_flag` — bucket ≥ 4 (`BUREAU_DEFAULT_BUCKET`). Bucket 3 covers 61–90 days and bucket 4
  91–120, so the bureau proxy starts at 91 days, one day later than the Home Credit definition;
- `n_bureau_active_contracts` — contracts with status `0..5` (closed `C` and unknown `X` are not counted).

## Exposure

`exposure_amount` is the sum over the client's **active** contract rows of the month
(`name_contract_status` in `Active`, `Signed`, `Demand`):

- credit cards: source `amt_balance` (can be negative for an overpaid card; reported by the warn
  expectation `exposure_amount_not_negative`, not corrected);
- POS / cash loans: `cnt_instalment_future × amt_annuity` of the previous application — an
  **estimate**, the source has no outstanding balance for these loans. Null for orphan contracts (no
  previous application) and for months without a balance row.

Bureau debt is not included: the source has it only as a snapshot at the application date.

## Reconciliation of computed and source DPD

Measured on `credit_risk_dev` (full Kaggle files, 2026-10-09). `gold.contract_month` has 13 932 955
rows; 11 536 949 of them carry both a source `sk_dpd` and a computed `installment_dpd`.

| | POS / cash | Credit cards |
|---|---|---|
| Months with both values | 8 744 815 | 2 792 134 |
| Identical day count | 7 677 654 (87.8 %) | 2 557 200 (91.6 %) |
| Within 31 days | 8 678 471 (99.2 %) | 2 787 279 (99.8 %) |
| Both 90+ | 71 007 | 47 002 |
| Source 90+, computed < 90 | 18 | 119 |
| Computed 90+, source < 90 | 52 651 | 2 228 |
| Source 90+, no installment in the month | 48 249 | 1 256 |

The warn expectation `installment_dpd_default_matches_source` fails on 55 016 months (0.48 % of the
months with both values). Almost all of them are "computed 90+, source below 90" (54 879):

- 41 261 months: the installment was eventually paid in full, 90 or more days after
  its due date, but the balance table shows less than 90 DPD (mostly 0) for those months.
- 13 587 months: the installment was never paid in full in the payments table.
- Installments with several versions explain only 31 months, so versioning is not the cause.

The two sources simply disagree there; the rule is not tuned to hide it. Because the source value
wins wherever a balance row exists, these months are **not** in default in Gold.

Effect on `gold.client_month` (15 124 050 rows, 344 074 clients): 169 027 client-months and 7 241
clients are in default. 166 640 of those client-months come from the source `sk_dpd`; 2 387 come from
computed DPD in months without a balance row, and 247 clients are in default only through them.

## Approximations to keep in mind

- The calendar is synthetic: every application is anchored on `2018-05-01` (see `data_model.md`).
  Month boundaries, and therefore month-end DPD values, depend on that anchor.
- The Kaggle files are a sample; contracts and bureau records can be missing.
