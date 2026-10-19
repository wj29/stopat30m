"""Offline evidence audit; never convert inferred fields into execution inputs.

Run with PYTHONPATH=. .venv/bin/python scripts/audit_ashare_data_readiness.py
--help. This tool only reads already captured prices/source code and writes a
separate research report. It performs no network requests and no paper trades.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from stopat30m.ashare_experiment import PackageHistoricalPool
from stopat30m.data.ashare_execution_data import historical_download_targets, load_execution_collection
from stopat30m.data.qlib_incremental import read_qlib_security
from stopat30m.data.qlib_snapshot import guard_qlib_output_path


NORMALIZER_HASHES = {
    "pinned_official_yahoo_collector.py": "6b356884c76e170fde48e3ac58a4952a62d47705bcb2b7a344acd8f3798d7905",
    "pinned_qlib_normalize.py": "07fab1801bafb9a15931c0fc7f0c803b4c5b61d4d90d5d2512ca34206b7d3eac",
}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verified_change_semantics(source_declaration, probes):
    """Bind the interpretation to the exact upstream code already reviewed."""
    declaration = json.loads(Path(source_declaration).read_text())
    sources = declaration.get("source", declaration)
    evidence = []
    for name, expected in NORMALIZER_HASHES.items():
        path = Path(probes) / name
        if sources["evidence_files"][name]["sha256"] != expected or sha(path) != expected:
            raise ValueError("Pinned normalization source differs; change semantics require a fresh code review")
        evidence.append({"path": str(path.resolve()), "sha256": expected})
    return {
        "formula": "raw_close.ffill() / raw_close.ffill().shift(1) - 1",
        "unit": "dimensionless_raw_close_return",
        "calculated_before_adjustment": True,
        "excluded_from_first_close_normalization": True,
        "subtracting_from_raw_close_is_dimensionally_invalid": True,
        "raw_close_divided_by_one_plus_change": "previous observed raw close, not exchange ex-right reference",
        "execution_preclose_reconstruction_allowed": False,
        "historical_ST_reconstruction_allowed": False,
        "source_evidence": evidence,
        "source_declaration": {"path": str(Path(source_declaration).resolve()), "sha256": sha(source_declaration)},
    }


def feature_presence(dataset, *, selection_start, start, end):
    interval = Path(dataset) / "instruments/csi300.txt"
    symbols = historical_download_targets(interval, selection_start, end)
    pool = PackageHistoricalPool(interval, start=selection_start, end=end)
    calendar = (Path(dataset) / "calendars/day.txt").read_text().splitlines()
    days = [day for day in calendar if start <= day <= end]
    if not days:
        raise ValueError("No actual package sessions intersect the requested period")
    memberships = {day: set(pool.members_on(day)) for day in days}
    rows = []
    for instrument in symbols:
        frame = read_qlib_security(dataset, instrument).reindex(days)
        fields = frame[["open", "high", "low", "close", "volume", "vwap", "factor"]]
        complete = np.isfinite(fields).all(axis=1) & fields.gt(0).all(axis=1)
        prices = frame[["open", "high", "low", "close", "vwap"]]
        eligible = [day for day in days if instrument in memberships[day]]
        rows.append({"instrument": instrument, "historical_pool_sessions": len(eligible),
            "complete_7field_sessions": int(complete.sum()),
            "eligible_unavailable_sessions": int((~complete.reindex(eligible)).sum()),
            "partial_price_sessions": int((np.isfinite(prices).any(axis=1) & ~np.isfinite(prices).all(axis=1)).sum())})
    counts = [{"session": day, "historical_members": len(memberships[day])} for day in days]
    return rows, counts, {"selection_start": selection_start, "start": start, "end": end,
        "historical_union_count": len(symbols), "sessions": len(days),
        "daily_members_min": min(row["historical_members"] for row in counts),
        "daily_members_max": max(row["historical_members"] for row in counts),
        "eligible_unavailable_sessions": sum(row["eligible_unavailable_sessions"] for row in rows),
        "partial_price_sessions": sum(row["partial_price_sessions"] for row in rows),
        "membership_source_sha256": sha(interval),
        "missing_quote_status": "unknown unless separately verified; no ST or halt inference",
        "integer_execution_ready": False}


def change_preclose_counterexamples(dataset, raw):
    """Compare an invalid proposed inference with real, separately saved preclose.

    Disagreement demonstrates why even a precisely restored raw price cannot
    recover the exchange reference from this field. Nothing is returned in the
    execution adapter's input schema.
    """
    rows, aggregates = [], []
    for instrument, group in raw.groupby("instrument"):
        observed = group.loc[group.source_complete_quote].set_index("date")
        path = Path(dataset) / "features" / instrument.lower() / "change.day.bin"
        if not path.is_file():
            aggregates.append({"instrument": instrument, "status": "change_unavailable"})
            continue
        frame = read_qlib_security(dataset, instrument, fields=("close", "factor", "change"))
        days = observed.index.intersection(frame.index)
        values = frame.loc[days]
        close = values.close / values.factor
        inferred = close / (1 + values.change)
        actual = pd.to_numeric(observed.loc[days, "preclose"], errors="raise")
        eligible = np.isfinite(inferred) & np.isfinite(actual) & actual.gt(0)
        differences = (inferred - actual).abs()
        counter = eligible & differences.gt(.011)
        aggregates.append({"instrument": instrument, "status": "compared",
            "comparable_sessions": int(eligible.sum()), "counterexample_sessions": int(counter.sum()),
            "max_difference_cny": float(differences.loc[eligible].max()) if eligible.any() else None,
            "change_file_sha256": sha(path)})
        for day in differences.index[counter]:
            source = observed.loc[day]
            rows.append({"instrument": instrument, "session": day, "raw_close": float(close.loc[day]),
                "change": float(values.loc[day, "change"]), "inferred_previous_raw_close": float(inferred.loc[day]),
                "actual_source_preclose": float(actual.loc[day]), "difference_cny": float(differences.loc[day]),
                "original_response_sha256": source.source_response_sha256})
    return rows, aggregates


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--source-declaration", type=Path, required=True)
    parser.add_argument("--source-probes", type=Path, required=True)
    parser.add_argument("--change-dataset", type=Path, required=True,
                        help="Original immutable dataset retaining its own change field")
    parser.add_argument("--execution-collection", type=Path, required=True,
                        help="An existing complete original collection used only for counterexamples")
    parser.add_argument("--selection-start", default="2022-12-30")
    parser.add_argument("--start", default="2023-01-01")
    parser.add_argument("--end", default="2024-12-31")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    output = guard_qlib_output_path(args.output_dir)
    if output.exists() and any(output.iterdir()):
        raise ValueError("Use a separate empty audit output; do not overwrite prior evidence")
    semantics = verified_change_semantics(args.source_declaration, args.source_probes)
    features, counts, presence = feature_presence(args.snapshot / "dataset", selection_start=args.selection_start,
                                                 start=args.start, end=args.end)
    raw, _, receipt = load_execution_collection(args.execution_collection)
    counterexamples, aggregate = change_preclose_counterexamples(args.change_dataset, raw)
    report = {"kind": "offline_ashare_evidence_readiness", "source_semantics": semantics,
        "snapshot_manifest_sha256": sha(args.snapshot / "manifest.json"),
        "feature_presence": presence, "counterexample_execution_manifest_sha256": receipt["manifest_sha256"],
        "counterexample_source_period": {key: receipt["request"][key] for key in ("start_date", "end_date")},
        "counterexample_sessions": len(counterexamples),
        "development_raw_execution_verified": False,
        "conclusion": "Feature availability and later-period counterexamples do not supply missing development execution evidence."}
    output.mkdir(parents=True, exist_ok=True)
    for name, values in [("features", features), ("historical_pool_counts", counts),
                         ("change_counterexamples", counterexamples), ("change_comparisons", aggregate)]:
        pd.DataFrame(values).to_csv(output / (name + ".csv"), index=False)
    (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n")
    print(json.dumps({"report": str((output / "report.json").resolve()),
        "development_execution_ready": False, "counterexample_sessions": len(counterexamples),
        "historical_union_count": presence["historical_union_count"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
