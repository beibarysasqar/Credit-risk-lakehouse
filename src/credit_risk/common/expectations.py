"""Data-quality rules of the Silver and derived tables: expectation name -> SQL condition.

Names are stable snake_case: they are queried from the pipeline event log for DQ reporting.
``fail`` stops the update, ``drop`` moves the row to ``silver.<entity>_quarantine``, ``warn``
only records the violation. Drop rules never evaluate to null, so a row is either kept or
quarantined, never both and never neither.
"""

from dataclasses import dataclass

from credit_risk.common.config import SILVER_ENTITIES


@dataclass(frozen=True)
class Rules:
    """Expectations of one Silver table, grouped by the action taken on violation."""

    fail: dict[str, str]
    drop: dict[str, str]
    warn: dict[str, str]


_NO_RESCUED_DATA = {"no_rescued_data": "_rescued_data IS NULL"}
_MONTHS_BALANCE_NOT_POSITIVE = {"months_balance_not_positive": "coalesce(months_balance, 1) <= 0"}
_SK_DPD_NOT_NEGATIVE = {
    "sk_dpd_not_negative": "coalesce(sk_dpd, 0) >= 0 AND coalesce(sk_dpd_def, 0) >= 0"
}
# Orphans exist in the source (contracts whose parent row was not delivered), hence a warning.
_PREVIOUS_APPLICATION_EXISTS = {"previous_application_exists": "has_previous_application"}

SILVER_RULES: dict[str, Rules] = {
    "application": Rules(
        fail={
            "sk_id_curr_not_null": "sk_id_curr IS NOT NULL",
            "target_matches_is_train": (
                "(is_train AND target IN (0, 1)) OR (NOT is_train AND target IS NULL)"
            ),
        },
        drop=_NO_RESCUED_DATA,
        warn={
            "amt_credit_positive": "amt_credit > 0",
            "amt_income_total_positive": "amt_income_total > 0",
            "days_birth_negative": "days_birth < 0",
        },
    ),
    "bureau": Rules(
        fail={
            "sk_id_bureau_not_null": "sk_id_bureau IS NOT NULL",
            "sk_id_curr_not_null": "sk_id_curr IS NOT NULL",
        },
        drop=_NO_RESCUED_DATA,
        warn={
            "days_credit_not_positive": "days_credit <= 0",
            "days_credit_enddate_plausible": (
                "days_credit_enddate IS NULL OR days_credit_enddate BETWEEN -20000 AND 20000"
            ),
            "amt_credit_sum_not_negative": "amt_credit_sum IS NULL OR amt_credit_sum >= 0",
            "amt_credit_sum_debt_not_negative": (
                "amt_credit_sum_debt IS NULL OR amt_credit_sum_debt >= 0"
            ),
        },
    ),
    "bureau_balance": Rules(
        fail={
            "sk_id_bureau_not_null": "sk_id_bureau IS NOT NULL",
            "months_balance_not_null": "months_balance IS NOT NULL",
        },
        drop={
            **_NO_RESCUED_DATA,
            **_MONTHS_BALANCE_NOT_POSITIVE,
            "status_in_domain": (
                "coalesce(status IN ('0', '1', '2', '3', '4', '5', 'C', 'X'), false)"
            ),
        },
        warn={"bureau_record_exists": "has_bureau_record"},
    ),
    "previous_application": Rules(
        fail={
            "sk_id_prev_not_null": "sk_id_prev IS NOT NULL",
            "sk_id_curr_not_null": "sk_id_curr IS NOT NULL",
        },
        drop=_NO_RESCUED_DATA,
        warn={
            "approved_amt_credit_positive": (
                "name_contract_status IS DISTINCT FROM 'Approved' OR amt_credit > 0"
            ),
            "days_decision_negative": "days_decision < 0",
            "amt_down_payment_not_negative": "amt_down_payment IS NULL OR amt_down_payment >= 0",
        },
    ),
    "pos_cash_balance": Rules(
        fail={
            "sk_id_prev_not_null": "sk_id_prev IS NOT NULL",
            "sk_id_curr_not_null": "sk_id_curr IS NOT NULL",
            "months_balance_not_null": "months_balance IS NOT NULL",
        },
        drop={**_NO_RESCUED_DATA, **_MONTHS_BALANCE_NOT_POSITIVE, **_SK_DPD_NOT_NEGATIVE},
        warn={
            **_PREVIOUS_APPLICATION_EXISTS,
            "sk_dpd_def_not_above_sk_dpd": "sk_dpd_def <= sk_dpd",
        },
    ),
    "credit_card_balance": Rules(
        fail={
            "sk_id_prev_not_null": "sk_id_prev IS NOT NULL",
            "sk_id_curr_not_null": "sk_id_curr IS NOT NULL",
            "months_balance_not_null": "months_balance IS NOT NULL",
        },
        drop={**_NO_RESCUED_DATA, **_MONTHS_BALANCE_NOT_POSITIVE, **_SK_DPD_NOT_NEGATIVE},
        warn={
            **_PREVIOUS_APPLICATION_EXISTS,
            "sk_dpd_def_not_above_sk_dpd": "sk_dpd_def <= sk_dpd",
            "amt_balance_not_negative": "amt_balance IS NULL OR amt_balance >= 0",
        },
    ),
    "installments_payments": Rules(
        fail={
            "sk_id_prev_not_null": "sk_id_prev IS NOT NULL",
            "sk_id_curr_not_null": "sk_id_curr IS NOT NULL",
        },
        drop={
            **_NO_RESCUED_DATA,
            "instalment_identified": (
                "num_instalment_number IS NOT NULL AND num_instalment_version IS NOT NULL "
                "AND days_instalment IS NOT NULL"
            ),
        },
        warn={
            **_PREVIOUS_APPLICATION_EXISTS,
            "amt_instalment_positive": "amt_instalment > 0",
            "payment_recorded": "days_entry_payment IS NOT NULL AND amt_payment IS NOT NULL",
        },
    ),
    "macro_observation": Rules(
        fail={"series_not_null": "series IS NOT NULL"},
        drop={
            **_NO_RESCUED_DATA,
            "period_month_parsed": "period_month IS NOT NULL",
            "obs_value_present": "obs_value IS NOT NULL",
        },
        # All pinned series are rates in percent.
        warn={"obs_value_plausible": "obs_value BETWEEN -10 AND 30"},
    ),
}

assert tuple(SILVER_RULES) == SILVER_ENTITIES

# Tables derived from Silver: the aggregated installments and the Gold contract / client months.
# Nothing is dropped here (the inputs are already clean), so there are no quarantine tables.
_DEFAULT_MATCHES = "(installment_dpd >= 90) = (source_dpd >= 90)"
_EXPOSURE_AMOUNT_NOT_NEGATIVE = {
    "exposure_amount_not_negative": "exposure_amount IS NULL OR exposure_amount >= 0"
}

DERIVED_RULES: dict[str, Rules] = {
    "installments": Rules(
        fail={
            "sk_id_prev_not_null": "sk_id_prev IS NOT NULL",
            "sk_id_curr_not_null": "sk_id_curr IS NOT NULL",
            "instalment_identified": (
                "num_instalment_number IS NOT NULL AND num_instalment_version IS NOT NULL "
                "AND instalment_date IS NOT NULL"
            ),
        },
        drop={},
        warn={
            "instalment_paid_in_full": "is_paid_in_full",
            "amt_paid_not_above_instalment": "amt_paid <= amt_instalment",
        },
    ),
    "contract_month": Rules(
        fail={
            "sk_id_prev_not_null": "sk_id_prev IS NOT NULL",
            "sk_id_curr_not_null": "sk_id_curr IS NOT NULL",
            "month_not_null": "month IS NOT NULL",
            "dpd_not_null": "dpd IS NOT NULL",
        },
        drop={},
        warn={
            **_PREVIOUS_APPLICATION_EXISTS,
            **_EXPOSURE_AMOUNT_NOT_NEGATIVE,
            # Reconciliation of the computed installment DPD with the source SK_DPD.
            "installment_dpd_default_matches_source": (
                f"installment_dpd IS NULL OR source_dpd IS NULL OR {_DEFAULT_MATCHES}"
            ),
        },
    ),
    "client_month": Rules(
        fail={
            "sk_id_curr_not_null": "sk_id_curr IS NOT NULL",
            "month_not_null": "month IS NOT NULL",
            "default_flag_matches_max_dpd": "default_flag = coalesce(max_dpd >= 90, false)",
            "default_start_month_matches_flag": (
                "default_flag = (default_start_month IS NOT NULL) "
                "AND coalesce(default_start_month <= month, true)"
            ),
        },
        drop={},
        warn=_EXPOSURE_AMOUNT_NOT_NEGATIVE,
    ),
}
