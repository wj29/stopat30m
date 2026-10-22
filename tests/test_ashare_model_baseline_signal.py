"""Offline score-only A16 diagnostics: no model, downloader, or ledger call."""
from contextlib import nullcontext
from copy import deepcopy
import hashlib

import numpy as np
import pandas as pd
import pytest

from stopat30m import ashare_model_baseline_signal as signal
from stopat30m import ashare_union_validation as original


class Pool:
    def __init__(self, names):
        self.names, self.queries = names, []

    def members_on(self, day, scope=None):
        self.queries.append((day, scope))
        return self.names[:60] if scope == "csi300" else self.names[60:] if scope == "csi1000" else self.names

    def verified_batch(self):
        return nullcontext(self)


def make_context(count=16):
    # Explicit synthetic sessions include a prior-year execution anchor.
    days = pd.bdate_range("2022-12-30", periods=count).strftime("%Y-%m-%d").tolist()
    names = [f"SH{600000+n:06d}" for n in range(119)]+["SH688001"]
    opening = pd.DataFrame(10*np.power(1+np.arange(1, 121)[None, :]*.0001, np.arange(count)[:, None]),
        index=days, columns=names)
    index = pd.MultiIndex.from_product([pd.to_datetime(days), names], names=["datetime", "instrument"])
    scores = pd.Series(np.tile(np.arange(120, dtype=float), count), index=index, name="score")
    raw = pd.DataFrame({"date": np.repeat(days, len(names)), "instrument": np.tile(names, count),
        "close": "10", "amount": "1000000", "tradestatus": "1", "isST": "0"})
    return {"calendar": days, "normalized_open": opening, "scores": {"LGB": scores}, "pool": Pool(names),
        "raw": raw, "protocol": {"periods": {"2023": [days[1], days[-1]]}}}


def run(context, **scores):
    return signal.diagnose_scores(context, score_sets={"original": context["scores"]["LGB"].copy(), **scores}, periods=["2023"])


def first_union(result, strategy="original"):
    return result["daily"].loc[result["daily"].strategy.eq(strategy)&result["daily"].group.eq("union")].iloc[0]


def test_original_h5_dates_pearson_rankic_and_labels_reproduce_original_quality_helper():
    context = make_context()
    result = run(context)
    expected = original.score_quality(context, model="LGB", period="2023")
    checked = signal.assert_quality_reference(result["daily"], expected, period="2023")
    assert checked["original_quality_days_checked"] == 10
    assert checked["Pearson_and_RankIC_reproduced"]
    first = first_union(result)
    assert first.signal_date == "2022-12-30"
    assert first.execution_date == "2023-01-02"
    assert first.label_end == context["calendar"][6]
    assert bool(first.previous_year_final_close_anchor)
    assert first.execution_month == "2023-01"
    assert first.execution_quarter == "2023Q1"
    assert result["coverage"].iloc[0].immature_signal_sessions == 5
    assert result["fit_count"] == result["prediction_count"] == result["strategy_simulations"] == 0
    assert result["paper_changed"] is False


def test_top20_is_selected_before_missing_future_label_and_never_replaced():
    context = make_context()
    highest = context["pool"].names[-1]
    context["normalized_open"].loc[context["calendar"][6], highest] = np.nan
    result = run(context)
    first = first_union(result)
    assert first.scored_pairs == 120 and first.labelled_pairs == 119
    assert first.top20_observed_pairs == 19 and not bool(first.top20_complete)
    assert pd.isna(first.top20_gross_label_mean) and pd.isna(first.top20_gross_label_excess)
    selected = list(reversed(context["pool"].names))[:20]
    assert first.top20_members_sha256 == hashlib.sha256("\n".join(selected).encode()).hexdigest()
    assert bool(first.bottom20_complete)
    assert pd.isna(first.top20_minus_bottom20_gross_label_mean)


def test_only_declared_backward_endpoint_nans_leave_rank_and_are_never_filled():
    context = make_context()
    mom = context["scores"]["LGB"].copy()
    mom.loc[pd.IndexSlice[:, context["pool"].names[-1]]] = np.nan
    before = mom.copy()
    result = run(context, MOM60=mom)
    row = first_union(result, "MOM60")
    assert row.declared_score_pairs == 120 and row.scored_pairs == 119
    assert row.missing_backward_score_pairs == 1 and row.labelled_pairs == 119
    assert row.universe_labelled_pairs == 120
    assert bool(row.top20_complete)
    assert row.universe_gross_label_mean == first_union(result).universe_gross_label_mean
    pd.testing.assert_series_equal(mom, before)


@pytest.mark.parametrize("strategy", ["regularized300", "HASH1103", "original"])
def test_model_and_hash_unknown_scores_cannot_be_treated_as_backward_endpoint_exceptions(strategy):
    context = make_context()
    values = context["scores"]["LGB"].copy()
    values.iloc[0] = np.nan
    with pytest.raises(ValueError, match="Unknown scores"):
        run(context, **{strategy: values})


@pytest.mark.parametrize("problem", ["missing_index", "reordered", "duplicate", "infinity", "unrecognized_strategy", "rescaled_original"])
def test_score_identity_and_frozen_index_are_strict(problem):
    context = make_context()
    values = context["scores"]["LGB"].copy()
    key = "regularized300"
    if problem == "missing_index":
        values = values.iloc[1:]
    elif problem == "reordered":
        values = values.iloc[::-1]
    elif problem == "duplicate":
        values.index = pd.MultiIndex.from_tuples([values.index[1], *values.index[1:]], names=values.index.names)
    elif problem == "infinity":
        values.iloc[0] = np.inf
    elif problem == "unrecognized_strategy":
        key = "new_seed_winner"
    else:
        key, values = "original", values*2
    with pytest.raises(ValueError):
        run(context, **{key: values})


def test_affordable_slice_respects_raw_prior_close_star_lot_st_status_and_no_future_membership():
    context = make_context()
    names = context["pool"].names
    raw = context["raw"]
    raw.loc[raw.instrument.eq("SH688001"), "close"] = "26"  # STAR minimum200 exceeds5000.
    raw.loc[raw.instrument.eq(names[-2]), "close"] = "50"  # Ordinary100 exactly5000 is allowed.
    raw.loc[raw.instrument.eq(names[-3]), "isST"] = "1"
    raw.loc[raw.instrument.eq(names[-4]), "tradestatus"] = "0"
    result = run(context)
    group = result["daily"].loc[result["daily"].group.eq("signal_close_affordable_nonST_trading")].iloc[0]
    assert group.scored_pairs == 117 and group.top20_observed_pairs == 20
    excluded = {"SH688001", names[-3], names[-4]}
    expected_head = [name for name in reversed(names) if name not in excluded][:20]
    assert group.top20_members_sha256 == hashlib.sha256("\n".join(expected_head).encode()).hexdigest()
    assert group.top20_overlap_with_original == 1.  # The original reference uses this same feasible group.
    expected_dates = set(context["calendar"][:-6])
    assert {day for day, _ in context["pool"].queries} == expected_dates
    assert context["calendar"][-1] not in {day for day, _ in context["pool"].queries}


def test_missing_raw_status_remains_unknown_and_does_not_remove_union_scores():
    context = make_context()
    day, highest = context["calendar"][0], context["pool"].names[-1]
    context["raw"] = context["raw"].loc[~(context["raw"].date.eq(day)&context["raw"].instrument.eq(highest))]
    result = run(context)
    assert first_union(result).scored_pairs == 120
    affordable = result["daily"].loc[result["daily"].group.eq("signal_close_affordable_nonST_trading")].iloc[0]
    assert affordable.scored_pairs == 119


def test_constant_scores_keep_unknown_ic_but_deterministic_code_tie_head_and_tail():
    context = make_context()
    context["scores"]["LGB"][:] = 1.
    row = first_union(run(context))
    assert pd.isna(row.IC) and pd.isna(row.rank_IC)
    assert bool(row.top20_complete)
    assert row.top20_members_sha256 == hashlib.sha256("\n".join(context["pool"].names[:20]).encode()).hexdigest()
    assert row.bottom20_members_sha256 == hashlib.sha256("\n".join(context["pool"].names[-20:]).encode()).hexdigest()


def test_descending_labelled_deciles_are_descriptive_and_head_minus_bottom_is_gross():
    context = make_context()
    first = first_union(run(context))
    means = [first[f"labelled_score_decile{number}_mean"] for number in range(1, 11)]
    assert all(left > right for left, right in zip(means, means[1:]))
    assert first.top20_minus_bottom20_gross_label_mean == pytest.approx(first.top20_gross_label_mean-first.bottom20_gross_label_mean)
    assert first.top20_gross_label_excess == pytest.approx(first.top20_gross_label_mean-first.universe_gross_label_mean)


def test_moving_block_interval_is_fixed_reproducible_and_constant_pairs_are_not_independent_wins():
    values = np.full(60, .01)
    first = signal.moving_block_mean_ci(values)
    assert first == signal.moving_block_mean_ci(values)
    assert first["lower"] == pytest.approx(.01) and first["upper"] == pytest.approx(.01)
    assert first["seed"] == 20261022 and first["draws"] == 2000 and first["block_actual_sessions"] == 20
    assert first["complete_overlapping_block_starts"] == 41
    assert first["complete_blocks_are_independent"] is False
    assert first["independent_date_win_probability_claim"] is False


def test_bootstrap_preserves_internal_unknown_date_slots_instead_of_compacting_them():
    values = np.linspace(-.02, .04, 60)
    values[30] = np.nan
    before = values.copy()
    result = signal.moving_block_mean_ci(values)
    assert result["status"] == "exploratory_block_interval"
    assert result["time_axis_sessions"] == 60 and result["paired_observations"] == 59
    assert result["missing_calendar_slots"] == 1
    assert result["complete_overlapping_block_starts"] == 21
    np.testing.assert_equal(values, before)


@pytest.mark.parametrize("values", [np.ones(39), np.where(np.arange(60)%10 == 0, np.nan, 1.)])
def test_short_or_fragmented_observations_have_no_confidence_interval(values):
    result = signal.moving_block_mean_ci(values)
    assert result["status"] == "insufficient_paired_contiguous_observations"
    assert result["lower"] is result["upper"] is None


def test_pairs_show_both_models_against_momentum_reversal_and_all_five_hash_median_with_masks():
    context = make_context()
    base = context["scores"]["LGB"]
    scores = {"regularized300": -base, "MOM60": -base, "REV20": base.copy()}
    scores.update({name: (-base if name != "HASH1103" else base.copy())
        for name in signal.STRATEGIES if name.startswith("HASH")})
    highest = context["pool"].names[-1]
    context["normalized_open"].loc[context["calendar"][6], highest] = np.nan
    result = run(context, **scores)
    pairs = [row for row in result["paired"] if row["group"] == "union" and row["frequency"] == "period"
        and row["metric"] == "top20_gross_label_excess"]
    indexed = {(row["strategy"], row["reference_strategy"]): row for row in pairs}
    for model in ("original", "regularized300"):
        for reference in ("MOM60", "REV20", "HASHmedian_all5_complete"):
            assert (model, reference) in indexed
    median_pair = indexed[("regularized300", "HASHmedian_all5_complete")]
    assert context["calendar"][0] in median_pair["excluded_either_unknown_dates"]
    assert median_pair["hash_reference_requires_all_five_known_on_same_day"] is True
    assert median_pair["hash_seeds_are_static_identity_controls_not_independent_skill_trials"] is True
    assert indexed[("original", "MOM60")]["paired_delta"]["mean"] > 0
    assert indexed[("regularized300", "REV20")]["paired_delta"]["mean"] < 0
    monthly = [row for row in result["paired"] if row["frequency"] == "month"]
    assert monthly and all(row["paired_delta"]["lower"] is None for row in monthly)
    assert result["temporal_summary"]


def test_original_reference_reproduction_detects_coverage_metric_and_missing_whole_date():
    context = make_context()
    result = run(context)
    reference = result["daily"].loc[result["daily"].strategy.eq("original")].copy()
    reference["horizon"] = 5
    checked = signal.assert_original_reference(result["daily"], reference, periods=("2023",), expected_rows=20)
    assert checked["original_A14_group_days_checked"] == 20
    for problem in ("metric", "coverage", "whole_date"):
        broken = result["daily"].copy()
        if problem == "metric":
            broken.loc[broken.index[0], "rank_IC"] += .0001
        elif problem == "coverage":
            broken.loc[broken.index[0], "labelled_pairs"] -= 1
        else:
            broken = broken.loc[broken.signal_date.ne(context["calendar"][0])]
        with pytest.raises(ValueError):
            signal.assert_original_reference(broken, reference, periods=("2023",), expected_rows=20)


def test_inputs_and_original_context_are_not_mutated():
    context = make_context()
    raw, opening, scores = context["raw"].copy(deep=True), context["normalized_open"].copy(deep=True), context["scores"]["LGB"].copy(deep=True)
    protocol = deepcopy(context["protocol"])
    run(context, regularized300=-scores)
    pd.testing.assert_frame_equal(context["raw"], raw)
    pd.testing.assert_frame_equal(context["normalized_open"], opening)
    pd.testing.assert_series_equal(context["scores"]["LGB"], scores)
    assert context["protocol"] == protocol


def test_later_available_labels_cannot_extend_an_annual_period_maturity_scope():
    context = make_context(22)
    end = context["calendar"][15]
    context["protocol"]["periods"]["2023"][1] = end
    context["normalized_open"].loc[context["calendar"][16]:] = 999999.
    result = run(context)
    rows = result["daily"].loc[result["daily"].group.eq("union")]
    assert len(rows) == 10
    assert rows.iloc[-1].label_end == end
    assert rows.iloc[-1].signal_date == context["calendar"][9]
    assert rows.label_end.le(end).all()
    checked = signal.assert_quality_reference(result["daily"], original.score_quality(context, model="LGB", period="2023"), period="2023")
    assert checked["original_quality_days_checked"] == 10


@pytest.mark.parametrize("bad", [np.array([[1., 2.]]), np.array([1., np.inf])])
def test_invalid_bootstrap_shapes_and_infinite_observations_are_rejected(bad):
    with pytest.raises(ValueError):
        signal.moving_block_mean_ci(bad)
