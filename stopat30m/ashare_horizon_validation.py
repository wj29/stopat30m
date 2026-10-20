"""Independent H5/H20 execution adapters over the verified stability ledger.

The new prediction package retains its own model/arm identity. Only the old
simulator's in-memory LGB score slot is reused; no old package manifest or
training declaration is relabelled. Source verification and exact original
baseline replay belong to the separately sealed experiment entry point.
"""
from __future__ import annotations

from copy import deepcopy
import re

import numpy as np
import pandas as pd

from stopat30m import ashare_stability_validation as stability
from stopat30m.ashare_frozen_horizon_diagnostics import _native_quality


ARM_HORIZONS = {"H5": 5, "H20": 20}
PERIODS = ("2025", "2026", "continuous")
REBALANCE_SESSIONS = 20


def _horizon(arm):
    if arm not in ARM_HORIZONS:
        raise ValueError("Only the two predeclared H5/H20 arms are permitted")
    return ARM_HORIZONS[arm]


def horizon_execution_config():
    """One execution policy for both arms, with only the declared cadence changed."""
    config = deepcopy(stability.EXECUTION)
    config["rebalance_sessions"] = REBALANCE_SESSIONS
    return config


def _calendar(calendar):
    days = list(calendar)
    if (len(days) < 2 or days != sorted(set(days))
            or any(not isinstance(day, str) or pd.Timestamp(day).date().isoformat() != day for day in days)):
        raise ValueError("A full sorted unique actual exchange calendar is required")
    return days


def _scores(scores, calendar):
    if (not isinstance(scores, pd.Series) or not isinstance(scores.index, pd.MultiIndex)
            or list(scores.index.names) != ["datetime", "instrument"] or scores.empty
            or scores.index.has_duplicates):
        raise ValueError("Unique nonempty datetime/instrument arm predictions are required")
    days = set(_calendar(calendar))
    dates = pd.DatetimeIndex(scores.index.get_level_values("datetime"))
    if (dates.hasnans or dates.tz is not None or not dates.equals(dates.normalize())
            or not set(dates.strftime("%Y-%m-%d")).issubset(days)
            or any(not isinstance(name, str) or not re.fullmatch(r"(?:SH|SZ)\d{6}", name)
                   for name in scores.index.get_level_values("instrument"))):
        raise ValueError("Arm predictions require actual calendar closes and raw security identities")
    values = pd.to_numeric(scores, errors="raise")
    if not np.isfinite(values.to_numpy()).all():
        raise ValueError("Nonfinite arm predictions are not eligible frozen scores")
    return values.astype(float).copy().sort_index()


def horizon_execution_year_scores(predictions, calendar, *, arm):
    """Route each prior close to its next actual execution year's own arm fold.

    December's final close can occur in both annual model files. The model for
    the next session's year supplies that signal, without resetting positions
    or a continuous account's rebalance phase at the year boundary.
    """
    _horizon(arm)
    days = _calendar(calendar)
    required = {"model", "execution_year", "datetime", "instrument", "score"}
    if not isinstance(predictions, pd.DataFrame) or not required.issubset(predictions):
        raise ValueError("A separately verified H5/H20 annual prediction table is required")
    if set(predictions["model"]) != set(ARM_HORIZONS):
        raise ValueError("The package must preserve both actual H5/H20 model identities")
    table = predictions.copy()
    if table.duplicated(["model", "execution_year", "datetime", "instrument"]).any():
        raise ValueError("Duplicate prediction in an arm's annual fold")
    years = pd.to_numeric(table["execution_year"], errors="raise")
    values = pd.to_numeric(table["score"], errors="raise")
    if not np.isfinite(values.to_numpy()).all() or not years.isin([2025, 2026]).all():
        raise ValueError("Only finite declared2025/2026 annual predictions are permitted")
    table["execution_year"], table["score"] = years, values
    if any(set(group["execution_year"]) != {2025, 2026} for _, group in table.groupby("model")):
        raise ValueError("Each arm requires both annual execution folds")
    dates = pd.to_datetime(table["datetime"], errors="raise")
    if dates.isna().any() or getattr(dates.dt, "tz", None) is not None or not dates.equals(dates.dt.normalize()):
        raise ValueError("Predictions must be dated at a timezone-naive actual signal close")
    table["datetime"] = dates
    if not set(dates.dt.strftime("%Y-%m-%d")).issubset(days):
        raise ValueError("Prediction date is absent from the verified actual calendar")
    next_year = {day: int(days[number+1][:4]) for number, day in enumerate(days[:-1])}
    used = table.loc[table.model.eq(arm)
        & table.datetime.dt.strftime("%Y-%m-%d").map(next_year).eq(table.execution_year)]
    return _scores(used.set_index(["datetime", "instrument"])["score"], days)


def prepare_horizon_context(verified_context, scores, *, arm):
    """Isolate one arm without mutating the previously verified source context."""
    horizon = _horizon(arm)
    if verified_context["protocol"].get("execution") != stability.EXECUTION:
        raise ValueError("The input must retain every original verified execution assumption")
    if verified_context.get("horizon_validation") is not None:
        raise ValueError("Do not reinterpret an already prepared arm as the legacy source context")
    if set(verified_context["protocol"].get("periods", {})) != set(PERIODS):
        raise ValueError("All three predeclared account periods are required")
    isolated = {key: value for key, value in verified_context.items()
                if key not in {"protocol", "scores", "score_coverage"}}
    isolated["protocol"] = deepcopy(verified_context["protocol"])
    isolated["protocol"]["execution"] = horizon_execution_config()
    isolated["scores"] = {"LGB": _scores(scores, verified_context["calendar"])}
    isolated["horizon_validation"] = {
        "arm": arm, "label_horizon_actual_sessions": horizon,
        "label": f"Ref($open,-{horizon+1})/Ref($open,-1)-1",
        "label_dependency_actual_sessions": horizon+1,
        "execution_rebalance_actual_sessions": REBALANCE_SESSIONS,
        "old_protocol_is_verified_source_loading_provenance": True,
        "runtime_LGB_score_slot_is_not_an_old_prediction_package": True,
        "a6_minimum_hold_or_exit_caps_added": False,
    }
    return isolated


def horizon_score_quality(context, *, period, arm):
    """Use the arm's own fully mature observed normalized-open label."""
    horizon = _horizon(arm)
    terms = context.get("horizon_validation", {})
    if (terms.get("arm") != arm or terms.get("label_horizon_actual_sessions") != horizon
            or context["protocol"].get("execution") != horizon_execution_config()):
        raise ValueError("Quality must use its own prepared arm and the common execution policy")
    quality, coverage = _native_quality(context, period=period, horizon=horizon)
    result = deepcopy(quality)
    result["summary"].update(prediction_arm=arm, label_horizon_actual_sessions=horizon,
        label_dependency_actual_sessions=horizon+1, execution_rebalance_actual_sessions=REBALANCE_SESSIONS)
    result["coverage"] = coverage
    return result


def simulate_horizon(verified_context, *, period, arm, scores):
    """Run the unchanged ledger with one arm and the shared20-session schedule.

    The orchestrator must have verified the new model package and reproduced
    all three old baseline ledgers before calling this function for real data.
    A7 proxy marks are deliberately absent: original terminal claim validity,
    actual cash, corporate actions, lots, rejected orders and costs are retained.
    """
    if period not in PERIODS:
        raise ValueError("Only the declared2025/2026/continuous account periods are permitted")
    context = prepare_horizon_context(verified_context, scores, arm=arm)
    result = stability.simulate_stability(context, period=period, model="LGB")
    result["score_quality"] = horizon_score_quality(context, period=period, arm=arm)
    result.update(arm=arm, label_horizon_actual_sessions=_horizon(arm),
        execution_rebalance_actual_sessions=REBALANCE_SESSIONS,
        execution_assumptions=horizon_execution_config(),
        horizon_validation=deepcopy(context["horizon_validation"]),
        strategy_promoted=False, active_paper_or_broker_changed=False)
    return result
