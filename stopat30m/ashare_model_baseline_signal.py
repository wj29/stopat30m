"""Read-only diagnostics for frozen A16 scores; never fit, predict or trade.

The caller supplies a verified original-family context and aligned saved score
series.  H5 price labels are overlapping gross research observations, not NAV,
after-fee returns, calibrated forecasts, or independent win probabilities.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
import math

import numpy as np
import pandas as pd

STRATEGIES = ("original", "regularized300", "MOM60", "REV20", "HASH1103", "HASH2207", "HASH3301", "HASH4409", "HASH5519")
GROUPS = ("union", "csi300", "csi1000", "signal_close_affordable_nonST_trading")
HORIZON = 5
TOP_K = 20
BLOCK_SESSIONS = 20
BOOTSTRAP_DRAWS = 2000
BOOTSTRAP_SEED = 20261022
MIN_PAIRED_OBSERVATIONS = 40
MIN_COMPLETE_BLOCKS = 3
PAIRED_METRICS = ("IC", "rank_IC", "top20_gross_label_excess", "top20_minus_bottom20_gross_label_mean")


def _stats(values):
    values = pd.to_numeric(pd.Series(values, dtype=float), errors="raise").dropna()
    if not np.isfinite(values.to_numpy()).all():
        raise ValueError("Only finite or explicitly unknown diagnostic observations are allowed")
    return {"count": len(values), "mean": float(values.mean()) if len(values) else None,
        "std": float(values.std(ddof=0)) if len(values) else None,
        "median": float(values.median()) if len(values) else None,
        "positive_date_fraction_descriptive": float(values.gt(0).mean()) if len(values) else None,
        "independent_observations_claim": False}


def moving_block_mean_ci(values):
    """Fixed, paired-date moving-block percentile CI, retaining internal NaNs.

    Missing observations keep their calendar slots. Blocks never join separate
    periods, months or quarters. The three complete blocks are *overlapping*
    eligible starts, not a claim of three independent observations.
    """
    values = np.asarray(values, dtype=float)
    if values.ndim != 1 or np.isinf(values).any():
        raise ValueError("Bootstrap needs a one-dimensional finite-or-unknown time axis")
    finite = np.isfinite(values)
    complete = int((np.convolve(finite.astype(int), np.ones(BLOCK_SESSIONS, dtype=int), mode="valid") == BLOCK_SESSIONS).sum()) if len(values) >= BLOCK_SESSIONS else 0
    result = {"method": "paired_20_actual_session_moving_block_percentile_nan_preserving",
        "seed": BOOTSTRAP_SEED, "draws": BOOTSTRAP_DRAWS, "block_actual_sessions": BLOCK_SESSIONS,
        "time_axis_sessions": len(values), "paired_observations": int(finite.sum()),
        "missing_calendar_slots": int((~finite).sum()), "complete_overlapping_block_starts": complete,
        "complete_blocks_are_independent": False, "mean": float(values[finite].mean()) if finite.any() else None,
        "confidence_level": .95, "lower": None, "upper": None,
        "status": "insufficient_paired_contiguous_observations", "independent_date_win_probability_claim": False}
    if finite.sum() < MIN_PAIRED_OBSERVATIONS or complete < MIN_COMPLETE_BLOCKS:
        return result
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    starts = rng.integers(0, len(values)-BLOCK_SESSIONS+1,
        size=(BOOTSTRAP_DRAWS, math.ceil(len(values)/BLOCK_SESSIONS)))
    slots = (starts[:, :, None]+np.arange(BLOCK_SESSIONS)).reshape(BOOTSTRAP_DRAWS, -1)[:, :len(values)]
    sampled = values[slots]
    counts = np.isfinite(sampled).sum(axis=1)
    if (counts == 0).any():
        result["status"] = "resamples_with_no_paired_observation_no_ci"
        return result
    means = np.nansum(sampled, axis=1)/counts
    result.update(status="exploratory_block_interval", lower=float(np.quantile(means, .025)),
        upper=float(np.quantile(means, .975)))
    return result


def _validate_inputs(context, score_sets, periods, original_key):
    if original_key != "original" or not isinstance(score_sets, Mapping) or original_key not in score_sets:
        raise ValueError("The frozen original score reference is required")
    if set(score_sets)-set(STRATEGIES):
        raise ValueError("Only the nine predeclared A16 score identities are supported")
    if (isinstance(periods, str) or not isinstance(periods, Sequence) or not periods
            or len(periods) != len(set(periods)) or any(period not in context["protocol"]["periods"] for period in periods)):
        raise ValueError("Explicit unique original-family periods are required")
    calendar = list(context["calendar"])
    if not calendar or calendar != sorted(set(calendar)):
        raise ValueError("The verified original exchange calendar must be sorted and unique")
    opening = context["normalized_open"]
    if (not isinstance(opening, pd.DataFrame) or opening.index.has_duplicates or opening.columns.has_duplicates
            or not set(calendar).issubset(opening.index)):
        raise ValueError("Original normalized opening prices need the complete unique calendar")
    original = context["scores"]["LGB"]
    if (not isinstance(original, pd.Series) or not isinstance(original.index, pd.MultiIndex)
            or original.index.names != ["datetime", "instrument"] or original.index.has_duplicates):
        raise ValueError("Original saved score rows must have the canonical unique dated index")
    validated = {}
    for name, values in score_sets.items():
        if not isinstance(values, pd.Series) or not values.index.equals(original.index) or values.dtype == bool:
            raise ValueError("All strategy score series must preserve the exact original legal index")
        values = pd.to_numeric(values, errors="raise").astype(float)
        if np.isinf(values.to_numpy()).any() or name not in {"MOM60", "REV20"} and values.isna().any():
            raise ValueError("Unknown scores are allowed only for declared missing backward endpoints")
        if name == "original" and not values.equals(pd.to_numeric(original, errors="raise").astype(float)):
            raise ValueError("The original score series cannot be replaced or rescaled")
        validated[name] = values
    raw = context["raw"].copy()
    required = {"date", "instrument", "close", "isST", "tradestatus"}
    if not required.issubset(raw) or raw.duplicated(["date", "instrument"]).any():
        raise ValueError("Unique original raw prices and historical trading/ST status are required")
    raw["close_num"] = pd.to_numeric(raw.close, errors="raise")
    raw["amount_num"] = pd.to_numeric(raw["amount"], errors="raise") if "amount" in raw else np.nan
    return calendar, opening, validated, raw.set_index(["date", "instrument"])


def _group_metrics(ordered, labels, reference_names):
    """Fix head/tail identities on score availability, before future endpoints."""
    score = ordered.dropna()
    selected_sides = (("top20", score.head(TOP_K).index), ("bottom20", score.tail(TOP_K).index))
    observed_labels = labels.reindex(score.index)
    observed = observed_labels.dropna()
    paired_score = score.reindex(observed.index)
    universe = labels.reindex(reference_names).dropna()
    enough = len(observed) >= TOP_K and paired_score.nunique() > 1 and observed.nunique() > 1
    ic = float(paired_score.corr(observed)) if enough else None
    rank_ic = float(paired_score.rank().corr(observed.rank())) if enough else None
    if enough and not np.isfinite([ic, rank_ic]).all():
        raise ValueError("Nonfinite cross-sectional correlations cannot be silently made unknown")
    row = {"declared_score_pairs": len(ordered), "scored_pairs": len(score),
        "missing_backward_score_pairs": int(ordered.isna().sum()), "labelled_pairs": len(observed),
        "missing_label_pairs": int(observed_labels.isna().sum()), "universe_labelled_pairs": len(universe),
        "IC": ic, "rank_IC": rank_ic,
        "universe_gross_label_mean": float(universe.mean()) if len(universe) else None}
    for side, names in selected_sides:
        known = labels.reindex(names)
        complete = len(names) == TOP_K and known.notna().all()
        mean = float(known.mean()) if complete else None
        row.update({side+"_observed_pairs": int(known.notna().sum()), side+"_complete": bool(complete),
            side+"_gross_label_mean": mean,
            side+"_gross_label_excess": mean-float(universe.mean()) if complete and len(universe) else None,
            side+"_members_sha256": hashlib.sha256("\n".join(names).encode()).hexdigest()})
    row["top20_minus_bottom20_gross_label_mean"] = (row["top20_gross_label_mean"]-row["bottom20_gross_label_mean"]
        if row["top20_complete"] and row["bottom20_complete"] else None)
    # These descriptive quantiles condition on known labels; unlike the actual
    # head selection they must never be described as an executable portfolio.
    for number in range(1, 11):
        row[f"labelled_score_decile{number}_mean"] = None
    if len(observed) >= 100:
        for number, values in enumerate(np.array_split(observed.to_numpy(), 10), 1):
            row[f"labelled_score_decile{number}_mean"] = float(values.mean())
    return row, score.head(TOP_K).index


def _summaries(daily, original_key):
    summary, paired, temporal = [], [], []
    if daily.empty:
        return summary, paired, temporal
    metrics = (*PAIRED_METRICS, "top20_gross_label_mean", "bottom20_gross_label_excess",
        *[f"labelled_score_decile{number}_mean" for number in range(1, 11)])
    for (period, group, strategy), frame in daily.groupby(["period", "group", "strategy"], sort=True):
        summary.append({"period": period, "group": group, "strategy": strategy, "signal_days": len(frame),
            "complete_top20_days": int(frame.top20_complete.sum()), "complete_bottom20_days": int(frame.bottom20_complete.sum()),
            "missing_backward_score_pairs": int(frame.missing_backward_score_pairs.sum()),
            "missing_label_pairs": int(frame.missing_label_pairs.sum()),
            "metrics": {metric: _stats(frame[metric]) for metric in metrics}})
        for frequency, column in (("quarter", "execution_quarter"), ("month", "execution_month")):
            for label, part in frame.groupby(column, sort=True):
                temporal.append({"period": period, "group": group, "strategy": strategy,
                    "frequency": frequency, "time_bin": label, "signal_days": len(part),
                    "complete_top20_days": int(part.top20_complete.sum()),
                    "metrics": {metric: _stats(part[metric]) for metric in PAIRED_METRICS}})
    for (period, group), frame in daily.groupby(["period", "group"], sort=True):
        arms = {name: part.set_index("signal_date").sort_index() for name, part in frame.groupby("strategy")}
        comparisons = [(name, original_key) for name in sorted(set(arms)-{original_key})]
        hash_names = [name for name in STRATEGIES if name.startswith("HASH")]
        if set(hash_names).issubset(arms):
            median = arms[original_key].copy()
            for metric in PAIRED_METRICS:
                values = pd.concat([arms[name][metric].rename(name) for name in hash_names], axis=1)
                median[metric] = values.median(axis=1).where(values.notna().all(axis=1))
            arms["HASHmedian_all5_complete"] = median
        for model in ("original", "regularized300"):
            if model in arms:
                comparisons.extend((model, control) for control in ("MOM60", "REV20", "HASHmedian_all5_complete") if control in arms)
        for strategy, reference in comparisons:
            base, other = arms[reference], arms[strategy]
            if not base.index.equals(other.index):
                raise ValueError("Paired strategies must preserve the full common signal-date time axis")
            spans = [("period", period, base.index)]
            for frequency, column in (("quarter", "execution_quarter"), ("month", "execution_month")):
                spans.extend((frequency, str(key), part.index) for key, part in base.groupby(column, sort=True))
            for frequency, label, dates in spans:
                for metric in PAIRED_METRICS:
                    left = pd.to_numeric(base.loc[dates, metric], errors="raise")
                    right = pd.to_numeric(other.loc[dates, metric], errors="raise")
                    known = left.notna() & right.notna()
                    delta = (right-left).where(known)
                    paired.append({"period": period, "group": group, "strategy": strategy,
                        "reference_strategy": reference, "frequency": frequency, "time_bin": label,
                        "metric": metric, "calendar_signal_sessions": len(dates), "common_complete_days": int(known.sum()),
                        "excluded_either_unknown_dates": dates[~known].tolist(),
                        "original_common_date_mean": float(left[known].mean()) if known.any() else None,
                        "strategy_common_date_mean": float(right[known].mean()) if known.any() else None,
                        "paired_delta": moving_block_mean_ci(delta.to_numpy()),
                        "hash_reference_requires_all_five_known_on_same_day": reference == "HASHmedian_all5_complete",
                        "hash_seeds_are_static_identity_controls_not_independent_skill_trials": reference.startswith("HASH"),
                        "units": "gross_adjusted_open_H5_price_label_or_cross_section_correlation_not_afterfee_NAV"})
    return summary, paired, temporal


def diagnose_scores(context, *, score_sets, periods, original_key="original"):
    """Diagnose every supplied family period once using already saved scores."""
    calendar, opening, score_sets, raw = _validate_inputs(context, score_sets, periods, original_key)
    location = {day: number for number, day in enumerate(calendar)}
    score_dates = {name: set(values.index.get_level_values("datetime")) for name, values in score_sets.items()}
    raw_dates = set(raw.index.get_level_values("date"))
    rows, coverage = [], []
    for period in periods:
        start, end = context["protocol"]["periods"][period]
        last = max(number for number, day in enumerate(calendar) if day <= end)
        sessions = [day for day in calendar if start <= day <= end]
        immature = 0
        for execution in sessions:
            position = location[execution]-1
            if position < 0:
                raise ValueError("Every execution session needs its actual previous source close")
            if position+HORIZON+1 > last:
                immature += 1
                continue
            day, label_end = calendar[position], calendar[position+HORIZON+1]
            if any(pd.Timestamp(day) not in dates for dates in score_dates.values()):
                raise ValueError("Saved scores lack a required preceding-close signal date")
            members = list(context["pool"].members_on(day))
            source_scores = score_sets[original_key].xs(pd.Timestamp(day), level="datetime")
            names = pd.Index([inst for inst in members if inst in source_scores], name="instrument")
            if names.has_duplicates:
                raise ValueError("Historical membership cannot contain duplicate instruments")
            left = opening.loc[execution].reindex(names)
            right = opening.loc[label_end].reindex(names)
            labels = (right/left-1).where(np.isfinite(left)&left.gt(0)&np.isfinite(right)&right.gt(0))
            source_raw = raw.loc[day].reindex(names) if day in raw_dates else pd.DataFrame(index=names, columns=raw.columns)
            lots = pd.Series(np.where(names.str.startswith("SH688"), 200, 100), index=names)
            affordable = source_raw.close_num.gt(0)&source_raw.close_num.mul(lots).le(5000)&source_raw.isST.eq("0")&source_raw.tradestatus.eq("1")
            c300 = set(context["pool"].members_on(day, scope="csi300"))
            c1000 = set(context["pool"].members_on(day, scope="csi1000"))
            groups = {"union": names, "csi300": names[names.isin(c300)], "csi1000": names[names.isin(c1000)],
                "signal_close_affordable_nonST_trading": names[affordable]}
            originals = {}
            for strategy in [original_key, *sorted(set(score_sets)-{original_key})]:
                values = score_sets[strategy].xs(pd.Timestamp(day), level="datetime").reindex(names)
                ordered = pd.DataFrame({"score": values}).reset_index().sort_values(["score", "instrument"],
                    ascending=[False, True], na_position="last").set_index("instrument").score
                for group, group_names in groups.items():
                    actual = ordered.loc[ordered.index.isin(group_names)]
                    metrics, head = _group_metrics(actual, labels, group_names)
                    if strategy == original_key:
                        originals[group] = set(head)
                    prices = source_raw.reindex(head)
                    metrics.update(top20_overlap_with_original=len(set(head)&originals[group])/TOP_K if len(head)==TOP_K else None,
                        top20_csi300_fraction=sum(inst in c300 for inst in head)/TOP_K if len(head)==TOP_K else None,
                        top20_median_raw_price=float(prices.close_num.median()) if len(head) and prices.close_num.notna().any() else None,
                        top20_median_raw_amount=float(prices.amount_num.median()) if len(head) and prices.amount_num.notna().any() else None)
                    rows.append({"period": period, "strategy": strategy, "group": group, "signal_date": day,
                        "execution_date": execution, "execution_year": int(execution[:4]), "label_end": label_end,
                        "execution_month": execution[:7], "execution_quarter": f"{execution[:4]}Q{(int(execution[5:7])-1)//3+1}",
                        "previous_year_final_close_anchor": day[:4] != execution[:4], **metrics})
        coverage.append({"period": period, "execution_start": start, "execution_end": end,
            "execution_sessions": len(sessions), "mature_signal_sessions": len(sessions)-immature,
            "immature_signal_sessions": immature, "label_endpoint_must_not_cross_period_end": True,
            "future_membership_filter_used": False, "future_labels_used_for_head_selection": False})
    daily = pd.DataFrame(rows)
    summary, paired, temporal = _summaries(daily, original_key)
    return {"kind": "a16_saved_score_signal_diagnostics", "daily": daily, "coverage": pd.DataFrame(coverage),
        "summary": summary, "paired": paired, "temporal_summary": temporal,
        "horizon_actual_sessions": HORIZON, "top_k": TOP_K,
        "fit_count": 0, "prediction_count": 0, "strategy_simulations": 0, "paper_changed": False,
        "limitations": ["All labels are overlapping gross price observations, not executable afterfee NAV or independent win probabilities.",
            "Head/tail identities precede future label visibility; unknown outcomes remain unknown.",
            "MOM60/REV20 backward-endpoint NaNs are excluded before ranking and separately counted; no score filling occurs.",
            "Group gross universe means use the original legal group, shared across strategies; score availability only affects that strategy's ranking and IC.",
            "Labelled-only deciles are descriptive conditional groups, not prospective portfolio identities.",
            "Monthly/quarterly blocks use execution dates and do not cross their bins; short or fragmented bins have no confidence interval.",
            "Block intervals are exploratory, preserve missing calendar slots, and do not establish independent date wins or account for repeated model searches.",
            "CSI300 membership, raw price and turnover amount are exposure proxies; no historical industry or capitalization neutrality is claimed."]}


def assert_original_reference(daily, reference_daily, *, periods=("2025", "2026"), expected_rows=844):
    """Reproduce the A14 two-group original rows before extending diagnostics."""
    groups = ("union", "signal_close_affordable_nonST_trading")
    old = reference_daily.loc[reference_daily.horizon.eq(HORIZON)].set_index(["signal_date", "group"])
    if old.index.has_duplicates:
        raise ValueError("Original diagnostic reference identities are duplicated")
    actual = daily.loc[daily.strategy.eq("original")&daily.period.isin(periods)&daily.group.isin(groups)]
    checked = 0
    for row in actual.to_dict(orient="records"):
        key = (row["signal_date"], row["group"])
        if key not in old.index:
            raise ValueError("An original A14 diagnostic reference date is missing")
        expected = old.loc[key]
        for field in ("scored_pairs", "labelled_pairs", "top20_observed_pairs", "top20_complete"):
            if row[field] != expected[field]:
                raise ValueError("Original A14 diagnostic coverage differs: "+field)
        for field in ("rank_IC", "universe_gross_label_mean", "top20_gross_label_mean", "top20_gross_label_excess"):
            left, right = row[field], expected[field]
            if pd.isna(left) and pd.isna(right):
                continue
            if pd.isna(left) or pd.isna(right) or abs(float(left)-float(right)) > 2e-15:
                raise ValueError("Original A14 diagnostic metric differs: "+field)
        checked += 1
    if (checked != expected_rows or set(actual.period) != set(periods)
            or actual.duplicated(["period", "signal_date", "group"]).any()
            or actual.groupby(["period", "signal_date"]).size().ne(len(groups)).any()):
        raise ValueError("Nonempty unique original diagnostic rows must be reproduced")
    return {"original_A14_group_days_checked": checked, "metric_absolute_tolerance": 2e-15,
        "reference_files_changed": False, "fits": 0, "predictions": 0}


def assert_quality_reference(daily, expected_quality, *, period):
    """Verify both source families' original saved Pearson/RankIC daily scopes."""
    actual = daily.loc[daily.period.eq(period)&daily.strategy.eq("original")&daily.group.eq("union")].sort_values("signal_date")
    actual = actual.loc[actual.IC.notna()&actual.rank_IC.notna()]
    expected = expected_quality["daily"]
    if len(actual) != len(expected) or not len(actual):
        raise ValueError("Original saved quality signal-date counts differ")
    for row, old in zip(actual.to_dict(orient="records"), expected):
        if (row["signal_date"] != old["signal_date"] or row["label_end"] != old["label_end"]
                or row["labelled_pairs"] != old["pairs"]):
            raise ValueError("Original quality dates, endpoints or labelled coverage differ")
        for field in ("IC", "rank_IC"):
            if abs(row[field]-old[field]) > 2e-15:
                raise ValueError("Original saved quality correlation differs")
    for field in ("IC", "rank_IC"):
        if abs(float(actual[field].mean())-expected_quality["summary"][field+"_mean"]) > 2e-15:
            raise ValueError("Original saved mean quality correlation differs")
    return {"period": period, "original_quality_days_checked": len(actual), "Pearson_and_RankIC_reproduced": True,
        "metric_absolute_tolerance": 2e-15, "fits": 0, "predictions": 0}
