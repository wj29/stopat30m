"""Verify normalized model-price time continuity against original references.

Dividing each day's OHLC by that day's stored factor cannot detect an incorrect
factor change: both numerator and factor may contain the same supplier seam.
This audit uses consecutive normalized closes and actual raw close/preclose
observations instead. Its source tick margin and float32 bounds are declared
independently of model or portfolio performance.
"""
from __future__ import annotations

from decimal import Decimal, localcontext, ROUND_CEILING, ROUND_FLOOR
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from .qlib_incremental import read_qlib_security


DEFAULT_SPEC = Path(__file__).resolve().parents[2] / "docs/research/a5_adjusted_price_bridge_spec.json"
RULE_VERSION = "a5-normalized-raw-temporal-bridge-v1"
RULE_VERSION_V2 = "a5-normalized-raw-temporal-bridge-v2-explicit-partial-chain"
MODEL_FIELDS = ("open", "high", "low", "close", "volume", "vwap", "factor")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_adjustment_bridge_spec(path=DEFAULT_SPEC):
    path = Path(path).resolve()
    spec = json.loads(path.read_text())
    if (spec.get("kind") != "a5_adjusted_price_temporal_bridge_spec"
            or spec.get("rule_version") not in {RULE_VERSION, RULE_VERSION_V2}
            or spec.get("status") != "frozen_before_union_model_fit_and_performance"
            or spec.get("source_reference_margin_cny") != "0.011"
            or spec.get("normalized_storage_dtype") != "float32"
            or spec.get("storage_error_bound") != "half_adjacent_float32_ULP_on_each_side_of_each_stored_normalized_close"
            or spec.get("provider_pre_storage_rounding_is_automatically_excused") is not False
            or spec.get("portfolio_returns_consulted_for_this_rule") is not False):
        raise ValueError("Temporal adjustment bridge differs from the declared fixed source rule")
    return spec, {"path": str(path), "sha256": sha(path)}


def load_partial_session_model_policy(path):
    """A single issuer-proven partial halt may lack a model bar, never a quote.

    This cannot approve arbitrary missing bars. The exact original daily row,
    old intraday observation evidence, issuer PDF and new reviewed page are
    bound before fitting. Source quotes and the original trading status remain
    available to the ledger; the exception concerns model scores only.
    """
    if path is None:
        return {}, {}
    path = Path(path).resolve()
    policy = json.loads(path.read_text())
    entries = policy.get("events", [])
    if (policy.get("kind") != "a5_specific_partial_session_model_policy"
            or policy.get("status") != "frozen_before_union_model_fit_and_performance"
            or policy.get("model_bars_or_factors_filled") is not False
            or policy.get("complete_quote_exceptions_allowed") is not False
            or policy.get("portfolio_returns_consulted") is not False
            or len(entries) != 1
            or (entries[0].get("instrument"), entries[0].get("date")) != ("SH688065", "2023-06-15")):
        raise ValueError("Partial model policy is not the specifically evidenced intraday halt")
    proofs = {"partial_model_policy": {"path": str(path), "sha256": sha(path)}}
    for label, source in policy.get("bound_inputs", {}).items():
        original = Path(source["path"])
        if not original.is_absolute() or sha(original) != source["sha256"]:
            raise ValueError("Partial model exception original bytes changed")
        proofs[label] = {"path": str(original), "sha256": sha(original)}
    if not {"intraday_observations", "original_daily", "issuer_pdf", "reviewed_original_page", "rule_spec"}.issubset(proofs):
        raise ValueError("Partial model exception requires its independent original evidence")
    rule = json.loads(Path(proofs["rule_spec"]["path"]).read_text())
    if (rule.get("kind") != "a5_specific_partial_session_model_spec"
            or rule.get("status") != "frozen_before_union_model_fit_and_performance"
            or rule.get("source_reference_margin_cny") != "0.011"
            or rule.get("chain") != "every_original_observed_close_divided_by_its_published_preclose"
            or rule.get("model_bar_imputation") is not False):
        raise ValueError("Partial model exception differs from its frozen chain rule")
    event = entries[0]
    originals = json.loads(Path(proofs["intraday_observations"]["path"]).read_text())["events"]
    matched = [row for row in originals if (row.get("instrument"), row.get("date")) == (event["instrument"], event["date"])]
    if (len(matched) != 1 or matched[0].get("execution_remains_blocked") is not True
            or matched[0].get("source_status_unchanged") != "0"
            or matched[0].get("halt_period") != "from_afternoon_open"
            or event.get("source_response_sha256") != proofs["original_daily"]["sha256"]
            or event.get("source_response_sha256") != matched[0].get("source_response_sha256")
            or event.get("source_observed_open") != matched[0].get("source_observed_open")
            or event.get("source_observed_close") != matched[0].get("source_observed_close")
            or event.get("allowed_missing_model_fields") != list(MODEL_FIELDS)
            or event.get("original_pdf_visual_reviewed") is not True
            or event.get("source_complete_quote") is not False
            or event.get("source_tradestatus") != "0"
            or event.get("observed_partial_quote_preserved") is not True
            or event.get("execution_remains_blocked") is not True):
        raise ValueError("Partial model exception does not match the original intraday observation")
    pdf = policy["bound_inputs"]["issuer_pdf"]
    if (Path(pdf["path"]).suffix.lower() != ".pdf" or not pdf.get("url", "").startswith("https://")
            or pdf.get("pdf_pages_1based") != [1]):
        raise ValueError("Partial model exception requires the reviewed original issuer page")
    raw = json.loads(Path(proofs["original_daily"]["path"]).read_text())
    daily_rows = [dict(zip(raw["fields"], values)) for values in raw["rows"]]
    matching_rows = [row for row in daily_rows if row.get("date") == event["date"]]
    if (len(matching_rows) != 1 or matching_rows[0].get("tradestatus") != "0"
            or matching_rows[0].get("code") != "sh.688065"
            or matching_rows[0].get("open") != event["source_observed_open"]
            or matching_rows[0].get("close") != event["source_observed_close"]):
        raise ValueError("Original partial-session daily row differs from approved source evidence")
    return {(event["instrument"], event["date"]): event}, proofs


def matches_partial_model_exception(instrument, day, row, model_values, exceptions):
    event = exceptions.get((instrument, day))
    return bool(event and not bool(row.get("source_complete_quote", True))
        and bool(row.get("source_observed_market_quote", False)) and str(row.get("tradestatus")) == "0"
        and row.get("source_response_sha256") == event["source_response_sha256"]
        and str(row.get("open")) == event["source_observed_open"]
        and str(row.get("close")) == event["source_observed_close"]
        and set(model_values.keys()) == set(MODEL_FIELDS)
        and all(pd.isna(model_values[field]) for field in MODEL_FIELDS))


def float32_close_interval(value):
    """Real-valued quantization interval of this stored float32, not a fit band."""
    value = float(value)
    if not np.isfinite(value) or value <= 0:
        raise ValueError("A normalized model close must be finite and positive")
    stored = np.float32(value)
    if not np.isfinite(stored) or float(stored) != value:
        raise ValueError("Model close is not an exact stored float32 observation")
    left = float(np.nextafter(stored, np.float32(-np.inf), dtype=np.float32))
    right = float(np.nextafter(stored, np.float32(np.inf), dtype=np.float32))
    low, high = value - (value - left) / 2, value + (right - value) / 2
    if low <= 0 or not np.isfinite(high):
        raise ValueError("Invalid float32 price quantization endpoints")
    return Decimal.from_float(low), Decimal.from_float(high)


def implied_reference_interval(raw_close, previous_normalized_close, current_normalized_close):
    raw = Decimal(str(raw_close))
    if not raw.is_finite() or raw <= 0:
        raise ValueError("Actual original raw close must be positive")
    previous_low, previous_high = float32_close_interval(previous_normalized_close)
    current_low, current_high = float32_close_interval(current_normalized_close)
    with localcontext() as context:
        context.prec = 50
        context.rounding = ROUND_FLOOR
        low = raw * previous_low / current_high
        context.rounding = ROUND_CEILING
        high = raw * previous_high / current_low
    return low, high


def _audit_temporal_v1(dataset, raw, instruments, *, start, end, spec_path=DEFAULT_SPEC):
    """Every unexplained seam blocks; a factor jump never fabricates an event."""
    spec, proof = load_adjustment_bridge_spec(spec_path)
    margin = Decimal(spec["source_reference_margin_cny"])
    grouped = {instrument: frame.sort_values("date").set_index("date")
               for instrument, frame in raw.groupby("instrument")}
    failures, boundaries, records = [], [], []
    checked = 0
    for instrument in instruments:
        checked_before = checked
        source = grouped[instrument]
        observed_column = ("source_observed_market_quote" if "source_observed_market_quote" in source
                           else "source_complete_quote")
        source = source.loc[source[observed_column].astype(bool) & source.index.to_series().le(end)]
        stored = read_qlib_security(dataset, instrument)
        previous = None
        for day, row in source.iterrows():
            current = stored.at[day, "close"] if day in stored.index else np.nan
            if previous is None:
                boundaries.append({"instrument": instrument, "session": day,
                    "reason": "initial_original_observed_quote_has_no_predecessor_in_collected_window"})
            elif start <= day <= end:
                prior_day, prior_close, prior_raw = previous
                base = {"instrument": instrument, "session": day, "previous_observed_session": prior_day,
                    "source_preclose": row.preclose, "source_close": row.close,
                    "source_response_sha256": row.source_response_sha256}
                try:
                    low, high = implied_reference_interval(row.close, prior_close, current)
                except ValueError as exc:
                    failures.append({**base, "reason": "missing_or_non_float32_actual_model_close",
                                     "detail": str(exc)})
                else:
                    reference = Decimal(str(row.preclose))
                    if not reference.is_finite() or reference <= 0:
                        raise ValueError("Actual source exchange reference must be positive")
                    checked += 1
                    if high < reference - margin or low > reference + margin:
                        with localcontext() as context:
                            context.prec = 50
                            source_return = Decimal(str(row.close)) / reference - 1
                            model_return = Decimal.from_float(float(current)) / Decimal.from_float(float(prior_close)) - 1
                        failures.append({**base, "reason": "unexplained_normalized_price_reference_seam",
                            "implied_reference_lower_cny": str(low), "implied_reference_upper_cny": str(high),
                            "source_reference_lower_cny": str(reference - margin),
                            "source_reference_upper_cny": str(reference + margin),
                            "normalized_observed_return": str(model_return), "source_preclose_return": str(source_return),
                            "return_difference": str(model_return - source_return),
                            "raw_previous_observed_close": str(prior_raw),
                            "previous_normalized_close": str(float(prior_close)), "normalized_close": str(float(current)),
                            "classification": "unresolved_provider_precision_or_adjustment_basis_not_confirmed_bad_price_or_action"})
            # Do not hop over a real source quote just because its model bar
            # is absent; the next source observation also requires repair.
            previous = day, current, row.close
        records.append({"instrument": instrument, "actual_source_observed_quotes": len(source)})
        if checked == checked_before:
            failures.append({"instrument": instrument,
                             "reason": "no_verified_adjacent_original_model_quote_pair"})
    failures.sort(key=lambda row: (-abs(Decimal(row.get("return_difference", "0"))), row["instrument"], row.get("session", "")))
    if sha(proof["path"]) != proof["sha256"]:
        raise ValueError("Temporal adjustment source rule changed during audit")
    return {"kind": "a5_normalized_price_temporal_audit", "status": "passed" if not failures else "blocked",
        "start": start, "end": end, "checked_adjacent_observed_quotes": checked,
        "records": records, "failures": failures, "initial_source_boundaries": boundaries,
        "source_rule": proof, "portfolio_returns_computed": False,
        "source_tick_margin_cny": str(margin), "float32_precision_basis": spec["storage_error_bound"],
        "no_automatic_corporate_action_inference": True}


def observed_chain_return_interval(rows, margin):
    """Propagate the unchanged per-leg CNY reference margin, outward rounded."""
    low, high = Decimal(1), Decimal(1)
    with localcontext() as context:
        context.prec = 50
        for row in rows:
            close, reference = Decimal(str(row.close)), Decimal(str(row.preclose))
            if not close.is_finite() or close <= 0 or not reference.is_finite() or reference <= margin:
                raise ValueError("Original partial-chain close/reference must be positive")
            context.rounding = ROUND_FLOOR
            low = low * close / (reference + margin)
            context.rounding = ROUND_CEILING
            high = high * close / (reference - margin)
    return low, high


def audit_temporal_adjustments(dataset, raw, instruments, *, start, end, spec_path=DEFAULT_SPEC,
                               partial_session_model_policy_path=None):
    spec, proof = load_adjustment_bridge_spec(spec_path)
    if spec["rule_version"] == RULE_VERSION:
        if partial_session_model_policy_path is not None:
            raise ValueError("The original temporal rule does not authorize partial model gaps")
        return _audit_temporal_v1(dataset, raw, instruments, start=start, end=end, spec_path=spec_path)
    if (spec.get("partial_missing_model_rule") != "specific_issuer_evidenced_partial_model_gap_with_full_source_return_chain"
            or spec.get("source_chain_margin_policy") != "propagate_unchanged_0.011_CNY_per_original_reference_leg"):
        raise ValueError("Partial temporal chain rule differs from the frozen v2 policy")
    exceptions, proofs = load_partial_session_model_policy(partial_session_model_policy_path)
    if not exceptions:
        raise ValueError("The v2 partial-chain bridge requires its exact original policy receipt")
    if spec.get("partial_rule_spec") != proofs["rule_spec"]:
        raise ValueError("Temporal bridge and partial model policy bind different frozen rules")
    # Start with all strict v1 failures. Only the two explicitly certified gap
    # edges below may be replaced by a successful full original-return chain.
    result = _audit_temporal_v1(dataset, raw, instruments, start=start, end=end, spec_path=spec_path)
    chains = []
    for (instrument, day), exception in exceptions.items():
        if instrument not in instruments or not start <= day <= end:
            continue
        source = raw.loc[raw.instrument.eq(instrument)].sort_values("date").set_index("date")
        source = source.loc[source.source_observed_market_quote.astype(bool) & source.index.to_series().le(end)]
        stored = read_qlib_security(dataset, instrument)
        if day not in source.index or day not in stored.index:
            raise ValueError("Approved partial source/model calendar observation is absent")
        if not matches_partial_model_exception(instrument, day, source.loc[day], stored.loc[day, list(MODEL_FIELDS)], exceptions):
            raise ValueError("The observed partial model gap differs from the specific approved exception")
        location = source.index.get_loc(day)
        if location == 0 or location + 1 == len(source):
            result["failures"].append({"instrument": instrument, "session": day,
                "reason": "partial_model_gap_lacks_both_real_model_anchors"})
            continue
        left, right = source.index[location-1], source.index[location+1]
        if not bool(source.at[left, "source_complete_quote"]) or not bool(source.at[right, "source_complete_quote"]):
            result["failures"].append({"instrument": instrument, "session": day,
                "reason": "partial_model_chain_requires_complete_observed_model_anchors"})
            continue
        chain_rows = [source.loc[day], source.loc[right]]
        try:
            left_low, left_high = float32_close_interval(stored.at[left, "close"])
            right_low, right_high = float32_close_interval(stored.at[right, "close"])
            with localcontext() as context:
                context.prec = 50
                context.rounding = ROUND_FLOOR
                model_low = right_low / left_high
                context.rounding = ROUND_CEILING
                model_high = right_high / left_low
            raw_low, raw_high = observed_chain_return_interval(chain_rows, Decimal(spec["source_reference_margin_cny"]))
        except (KeyError, ValueError) as exc:
            result["failures"].append({"instrument": instrument, "session": day,
                "reason": "partial_model_chain_anchor_or_original_reference_invalid", "detail": str(exc)})
            continue
        chain = {"instrument": instrument, "missing_model_session": day, "left_model_anchor": left,
            "right_model_anchor": right, "original_observed_source_legs": [
                {"session": session, "close": source.at[session, "close"], "preclose": source.at[session, "preclose"],
                 "source_response_sha256": source.at[session, "source_response_sha256"]} for session in (day, right)],
            "normalized_ratio_lower": str(model_low), "normalized_ratio_upper": str(model_high),
            "original_chain_ratio_lower": str(raw_low), "original_chain_ratio_upper": str(raw_high),
            "model_bar_or_factor_imputed": False, "individual_gap_model_return_certified": False}
        if model_high < raw_low or model_low > raw_high:
            result["failures"].append({**chain, "session": right,
                "reason": "unexplained_partial_model_chain_return_seam"})
            continue
        removed = [row for row in result["failures"] if row.get("instrument") == instrument and
            (row.get("reason") == "missing_or_non_float32_actual_model_close" and
             (row.get("session"), row.get("previous_observed_session")) in {(day, left), (right, day)}
             or row.get("reason") == "no_verified_adjacent_original_model_quote_pair")]
        result["failures"] = [row for row in result["failures"] if row not in removed]
        chain.update(status="passed", individually_missing_edges_replaced_by_certified_chain=sum(
            row.get("reason") == "missing_or_non_float32_actual_model_close" for row in removed))
        chains.append(chain)
    result.update(status="passed" if not result["failures"] else "blocked",
        checked_partial_source_chains=len(chains), certified_partial_source_chains=chains,
        partial_model_policy_bound_inputs=proofs, source_rule=proof)
    if any(sha(value["path"]) != value["sha256"] for value in proofs.values()) or sha(proof["path"]) != proof["sha256"]:
        raise ValueError("Partial source bridge rule/originals changed during verification")
    return result
