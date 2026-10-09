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
