"""A14 model-only score adapters over the unchanged fixed20 integer ledger.

Original A5 loaders retain their source/package guards. A new prediction
package supplies only the in-memory LGB score slot: no order policy, tax,
corporate action, valuation or current paper account is modified here.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd

from stopat30m import ashare_union_validation as primary
from stopat30m import ashare_stability_validation as stability
from stopat30m import ashare_model_debug_research as training
from stopat30m.ashare_turnover_validation import FIXED_VARIANTS, simulate_turnover


HOLD_RULE = dict(FIXED_VARIANTS[1])
FAMILY_PERIODS = {"primary": ("2023", "2024", "continuous"),
                  "stability": ("2025", "2026", "continuous")}
_REQUIRED_COLUMNS = ["model", "execution_year", "datetime", "instrument", "score"]


def _read(path):
    return json.loads(Path(path).read_bytes())


def _declaration(value):
    if isinstance(value, (str, Path)):
        path = Path(value).resolve()
    elif isinstance(value, Mapping):
        proof = value.get("_self_proof")
        training._assert_proof(proof)
        path = Path(proof["path"])
        if {key: item for key, item in value.items() if key != "_self_proof"} != _read(path):
            raise ValueError("In-memory debug declaration differs from its bound original")
    else:
        raise ValueError("A bound model-debug training declaration path is required")
    declaration, _ = training._load_declaration(path)
    return path, declaration


def load_debug_context(training_declaration, family):
    """Load one original family with all original data/model/protocol guards."""
    if family not in FAMILY_PERIODS:
        raise ValueError("Only the separate primary and stability families are supported")
    path, declaration = _declaration(training_declaration)
    inputs = declaration["source_families"][family]
    manifest = _read(inputs["model_manifest"]["path"])
    protocol = _read(inputs["protocol"]["path"])
    expected_kind = "a5_union_fixed_predictions" if family == "primary" else "a5_union_stability_fixed_predictions"
    if manifest.get("kind") != expected_kind:
        raise ValueError("A model-debug source family cannot be interchanged")
    scope = training._scope_directory(manifest, protocol["historical_scope_manifest_sha256"])
    package = Path(inputs["model_manifest"]["path"]).parent
    merger = declaration.get("merger_actions")
    if merger is None:
        declared_sources = declaration.get("sources", {}).get(family, {})
        merger = declared_sources.get("merger_actions")
        if isinstance(merger, str):
            expected = declaration.get("bound_inputs", {}).get("merger_actions")
            if not expected or expected.get("path") != merger:
                raise ValueError("Merger actions require their own bound source proof")
            merger = expected
    training._assert_proof(merger)
    actual = {"snapshot": manifest["source_snapshot"], "scope_directory": str(scope),
              "protocol": inputs["protocol"]["path"], "market_audit": inputs["market_audit"]["path"],
              "predictions_directory": str(package), "merger_actions": merger["path"]}
    sources = declaration.get("sources", {}).get(family)
    if sources is not None:
        for key, expected in actual.items():
            supplied = sources.get(key)
            if isinstance(supplied, Mapping):
                supplied = supplied.get("path")
            if supplied is None or Path(supplied).resolve() != Path(expected).resolve():
                raise ValueError("Debug context source map differs from original family: " + key)
    loader = primary.load_union_context if family == "primary" else stability.load_stability_context
    context = loader(actual["snapshot"], actual["scope_directory"], actual["protocol"],
                     actual["market_audit"], actual["predictions_directory"], actual["merger_actions"])
    if (context["protocol"]["execution"] != stability.EXECUTION
            or set(context["protocol"]["periods"]) != set(FAMILY_PERIODS[family])):
        raise ValueError("Original source family must retain its fixed execution and account periods")
    if family == "primary":
        if "tax_contracts" in context:
            raise ValueError("Primary source contract unexpectedly contains additional tax contracts")
        # Primary tax evidence already lives in its bound dividend events as
        # effective cash-dividend tax rates. This empty *additional* list only
        # makes the unchanged turnover loop compatible; it waives no known tax.
        context = {**context, "tax_contracts": []}
    elif "tax_contracts" not in context:
        raise ValueError("Stability source requires its actual additional tax contracts")
    context["model_debug"] = {
        "family": family, "training_declaration": training._proof(path),
        "source_snapshot": str(Path(actual["snapshot"]).resolve()),
        "source_manifest_sha256": manifest["source_manifest_sha256"],
        "scope_manifest_sha256": protocol["historical_scope_manifest_sha256"],
        "primary_additional_tax_contracts": ("empty_compatibility_list_original_effective_dividend_tax_rates_retained"
                                              if family == "primary" else "original_bound_contracts"),
        "old_protocol_is_source_loading_provenance": True,
        "new_scores_do_not_relabel_old_package": True,
    }
    return context


def _packages(value):
    if isinstance(value, (str, Path)):
        paths = [Path(value).resolve()]
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        if not value or any(not isinstance(item, (str, Path)) for item in value):
            raise ValueError("Prediction package sequence requires one or two paths")
        paths = [Path(item).resolve() for item in value]
    else:
        raise ValueError("A separately verified model-debug prediction package is required")
    if len(paths) not in (1, 2) or len(set(paths)) != len(paths):
        raise ValueError("Exactly one package or the distinct 2023/2024 pair is permitted")
    return paths


def _prediction_scores(table, *, candidate, years, calendar, expected):
    """Route each close to its next actual execution year, then require coverage."""
    if (not isinstance(table, pd.DataFrame) or list(table.columns) != _REQUIRED_COLUMNS
            or table.empty or table.duplicated(_REQUIRED_COLUMNS[:4]).any()):
        raise ValueError("Unique model/year/signal/security prediction rows are required")
    frame = table.loc[table.model.eq(candidate)].copy()
    if frame.empty:
        raise ValueError("Selected candidate is absent from the model package")
    years_numeric = pd.to_numeric(frame.execution_year, errors="raise")
    values = pd.to_numeric(frame.score, errors="raise")
    if (not np.isfinite(years_numeric).all() or not years_numeric.isin(years).all()
            or set(years_numeric) != set(years) or not np.isfinite(values).all()
            or frame.execution_year.map(lambda value: isinstance(value, (bool, np.bool_))).any()):
        raise ValueError("Candidate scores require exactly the declared finite execution-year folds")
    dates = pd.to_datetime(frame.datetime, errors="raise")
    if (dates.isna().any() or getattr(dates.dt, "tz", None) is not None
            or not dates.equals(dates.dt.normalize())
            or any(not isinstance(name, str) or not re.fullmatch(r"(?:SH|SZ)\d{6}", name)
                   for name in frame.instrument)):
        raise ValueError("Prediction identities and signal dates must retain original source semantics")
    days = list(calendar)
    if len(days) < 2 or days != sorted(set(days)):
        raise ValueError("A unique ordered actual calendar is required")
    day_text = dates.dt.strftime("%Y-%m-%d")
    if not set(day_text).issubset(days):
        raise ValueError("Prediction date is absent from the actual source calendar")
    next_year = {day: int(days[index+1][:4]) for index, day in enumerate(days[:-1])}
    for year in years:
        prior = [day for day in days if day < f"{year}-01-01"]
        current = [day for day in days if f"{year}-01-01" <= day <= f"{year}-12-31"]
        if not prior or not current:
            raise ValueError("Annual predictions need preceding and current actual sessions")
        selected = years_numeric.eq(year)
        if (day_text.loc[selected] < prior[-1]).any() or (day_text.loc[selected] > current[-1]).any():
            raise ValueError("An annual model supplies scores outside its own execution fold")
    routed = frame.loc[day_text.map(next_year).eq(years_numeric)].copy()
    routed["datetime"] = pd.to_datetime(routed.datetime)
    routed["score"] = pd.to_numeric(routed.score)
    if routed.empty or routed.duplicated(["datetime", "instrument"]).any():
        raise ValueError("Annual routing must produce unique preceding-close scores")
    scores = routed.set_index(["datetime", "instrument"])["score"].sort_index()
    expected_days = pd.DatetimeIndex(expected.index.get_level_values("datetime")).strftime("%Y-%m-%d")
    wanted = expected.loc[pd.Series(expected_days.map(next_year), index=expected.index).isin(years)].sort_index()
    if not scores.index.equals(wanted.index):
        raise ValueError("Candidate inference coverage differs from the unchanged original source eligibility")
    return scores


def prepare_debug_context(context, *, candidate, period, prediction_package):
    """Validate stage identity and isolate scores without changing source policy."""
    marker = context.get("model_debug", {})
    family = marker.get("family")
    if (family not in FAMILY_PERIODS or period not in FAMILY_PERIODS[family]
            or context["protocol"].get("execution") != stability.EXECUTION):
        raise ValueError("A verified original debug context and family period are required")
    training._assert_proof(marker.get("training_declaration"))
    if candidate == "baseline":
        if prediction_package is not None:
            raise ValueError("The baseline must use its unchanged original scores directly")
        return {key: ({} if key == "score_coverage" else value) for key, value in context.items()}
    if candidate not in training.CANDIDATES:
        raise ValueError("Only the three predeclared model candidates are supported")
    paths = _packages(prediction_package)
    manifests = [training.verify_debug_model_package(path, marker["training_declaration"]["path"])
                 for path in paths]
    for manifest in manifests:
        source = manifest.get("source_families", {}).get(family, {})
        if (candidate not in manifest.get("models", [])
                or source.get("source_manifest_sha256") != marker["source_manifest_sha256"]
                or source.get("scope_manifest_sha256") != marker["scope_manifest_sha256"]
                or Path(source.get("source_snapshot", "")).resolve() != Path(marker["source_snapshot"]).resolve()):
            raise ValueError("New model package does not belong to this unchanged source family")
    phases = [manifest.get("phase") for manifest in manifests]
    if len(paths) == 2:
        if family != "primary" or period != "continuous" or set(phases) != {"selection", "confirmation"}:
            raise ValueError("Only the predeclared primary continuous account combines two stages")
        selection_index, confirmation_index = phases.index("selection"), phases.index("confirmation")
        decision_proof = manifests[confirmation_index].get("inputs", {}).get("selection_decision")
        training._assert_proof(decision_proof)
        decision = _read(decision_proof["path"])
        if (decision.get("selected_candidate") != candidate
                or decision.get("selection_model_manifest") != training._proof(paths[selection_index]/"manifest.json")):
            raise ValueError("Continuous annual models must share the actual frozen selection decision")
        years = [2023, 2024]
    else:
        phase = phases[0]
        allowed = {("primary", "2023"): "selection", ("primary", "2024"): "confirmation",
                   ("stability", "2025"): "retrospective", ("stability", "2026"): "retrospective",
                   ("stability", "continuous"): "retrospective"}
        if allowed.get((family, period)) != phase:
            raise ValueError("Prediction phase cannot be reinterpreted as another account period")
        years = training.PHASE_YEARS[phase]
    expected_years = [year for manifest in manifests for year in manifest["execution_years"]]
    if sorted(expected_years) != years or len(expected_years) != len(set(expected_years)):
        raise ValueError("Combined model packages require exactly the nonoverlapping actual annual folds")
    table = pd.concat([pd.read_csv(path/"predictions.csv") for path in paths], ignore_index=True)
    scores = _prediction_scores(table, candidate=candidate, years=years,
                                calendar=context["calendar"], expected=context["scores"]["LGB"])
    isolated = {key: value for key, value in context.items() if key not in {"scores", "score_coverage", "protocol"}}
    isolated["protocol"] = deepcopy(context["protocol"])
    isolated["scores"], isolated["score_coverage"] = {"LGB": scores}, {}
    return isolated


def simulate_debug_model(context, *, candidate, period, prediction_package=None):
    """Run one authorized frozen model under the unchanged hold10/cap2 ledger."""
    prepared = prepare_debug_context(context, candidate=candidate, period=period,
                                     prediction_package=prediction_package)
    return simulate_turnover(prepared, period=period, variant=dict(HOLD_RULE))
