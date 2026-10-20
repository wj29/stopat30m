"""Plot a completed, immutable A8 H5/H20 comparison without fitting or selection.

Example:
    .venv/bin/python scripts/plot_horizon_model_comparison.py \
      --experiment-dir output/research/ashare/experiments/a8_shared_panel_horizon_fee_validation_20261020_v1 \
      --output-dir output/research/ashare/plots/a8_horizon_comparison_20261020_v1

All six paths are mandatory, including invalid or stopped paths. Drawdown uses
the original conservative definition: lower NAV / running upper NAV peak - 1,
with initial cash included. The script neither reruns the ledger nor changes
the source experiment or its accounting-validity flags.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile

import numpy as np
import pandas as pd


KIND = "a8_shared_panel_horizon_costed_comparison"
ARMS = ("H5", "H20")
PERIODS = ("2025", "2026", "continuous")
WINDOWS = {"2025": ("2025-01-01", "2025-12-31"),
    "2026": ("2026-01-01", "2026-10-19"),
    "continuous": ("2025-01-01", "2026-10-19")}
COLORS = {"H5": "#2563eb", "H20": "#c26b16"}
TRADE_FIELDS = ("notional", "commission", "transfer_fee", "stamp_tax", "total_fee", "slippage_cost")
COST_FIELDS = (("total_fee_cny", "Fees", "#4465a3"),
    ("slippage_cost_cny", "Slippage", "#df9d43"),
    ("total_distribution_tax_reserved_cny", "Distribution tax reserve", "#77966c"),
    ("cash_distribution_rounding_reserved_cny", "Rounding reserve", "#b38da6"))


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _read(path):
    return json.loads(Path(path).read_bytes())


def _decimal(value, name, *, nonnegative=False):
    number = Decimal(str(value))
    if not number.is_finite() or (nonnegative and number < 0):
        raise ValueError("Invalid finite accounting value: " + name)
    return number


def _verify_inventory(source, manifest, original_manifest_sha):
    if sha(source / "manifest.json") != original_manifest_sha or manifest.get("kind") != KIND:
        raise ValueError("The completed A8 manifest changed or has the wrong kind")
    files = manifest.get("files")
    if not isinstance(files, dict) or not files or "manifest.json" in files:
        raise ValueError("A complete non-self-referential A8 artifact inventory is required")
    for name, digest in files.items():
        relative = Path(name)
        path = (source / relative).resolve()
        if (relative.is_absolute() or ".." in relative.parts or not path.is_relative_to(source)
                or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
                or not path.is_file() or sha(path) != digest):
            raise ValueError("A8 artifact is unsafe, absent or changed: " + name)
    actual = {str(path.relative_to(source)) for path in source.rglob("*")
        if path.is_file() and path != source / "manifest.json"}
    if actual != set(files):
        raise ValueError("A8 file inventory differs from its completed manifest")


def _bool_column(frame, name):
    if name not in frame or not frame[name].isin(["True", "False"]).all():
        raise ValueError("Explicit original Boolean curve flags are required: " + name)
    return frame[name].eq("True").to_numpy()


def _load_path(source, summary, initial):
    arm, period = summary["arm"], summary["period"]
    directory = source / arm / period
    if _read(directory / "summary.json") != summary:
        raise ValueError("Per-path summary differs from report: " + arm + "/" + period)
    if type(summary.get("performance_valid")) is not bool:
        raise ValueError("Original performance_valid must be an explicit Boolean")
    metrics = summary["metrics"]
    curves = pd.read_csv(directory / "curves.csv", dtype=str, keep_default_na=False)
    required = {"date", "equity_cny", "equity_upper_bound_cny", "nav", "accounting_valid", "valuation_complete"}
    if curves.empty or not required.issubset(curves):
        raise ValueError("Every declared path needs its original nonempty NAV and bounds")
    dates = pd.to_datetime(curves.date, errors="raise")
    first, last = WINDOWS[period]
    if (dates.duplicated().any() or not dates.is_monotonic_increasing
            or dates.dt.tz is not None or not dates.equals(dates.dt.normalize())
            or curves.date.tolist() != dates.dt.strftime("%Y-%m-%d").tolist()
            or curves.date.min() < first or curves.date.max() > last):
        raise ValueError("Curve dates differ from the declared account window")
    lower = [_decimal(value, "equity_cny") for value in curves.equity_cny]
    upper = [_decimal(value, "equity_upper_bound_cny") for value in curves.equity_upper_bound_cny]
    if any(low <= 0 or high < low for low, high in zip(lower, upper)):
        raise ValueError("Original NAV bounds are inconsistent")
    nav = [value / initial for value in lower]
    peak, drawdown = initial, []
    for low, high in zip(lower, upper):
        peak = max(peak, high)
        drawdown.append(low / peak - 1)
    accounting = _bool_column(curves, "accounting_valid")
    valuation_complete = _bool_column(curves, "valuation_complete")
    recorded_nav = pd.to_numeric(curves.nav, errors="raise").to_numpy()
    if (not np.isfinite(recorded_nav).all()
            or not np.allclose(recorded_nav, [float(value) for value in nav], rtol=1e-12, atol=1e-12)
            or int(metrics["session_count"]) != len(curves)
            or _decimal(metrics["final_equity_cny"], "final equity") != lower[-1]
            or _decimal(metrics["total_return_lower_exact"], "net return") != nav[-1] - 1
            or _decimal(metrics["max_drawdown_conservative_exact"], "max drawdown") != min([Decimal(0), *drawdown])
            or int(metrics["accounting_invalid_days"]) != int((~accounting).sum())
            or summary["actual_last_valued_session"] != curves.date.iloc[-1]):
        raise ValueError("Original curve and summary metrics disagree: " + arm + "/" + period)
    valid = summary["performance_valid"]
    if valid and (not accounting.all() or metrics.get("terminal_prices_fresh") is not True
            or summary.get("stopped") is not None or metrics.get("declared_end_reached") is not True):
        raise ValueError("A valid flag contradicts the original accounting/stopped-path evidence")
    if not valid and "invalid" not in summary.get("metrics_basis", "").lower():
        raise ValueError("An invalid path must retain its original invalid metrics basis")
    try:
        trades = pd.read_csv(directory / "trades.csv", dtype=str, keep_default_na=False)
    except pd.errors.EmptyDataError:
        trades = pd.DataFrame()
    if not trades.empty and not set(TRADE_FIELDS).issubset(trades):
        raise ValueError("Original trade cost fields are incomplete")
    if len(trades) != int(metrics["trade_count"]):
        raise ValueError("Original trade count differs from the summary")
    for field in TRADE_FIELDS:
        total = sum((_decimal(value, field, nonnegative=True) for value in trades.get(field, ())), Decimal(0))
        if total != _decimal(metrics[field + "_cny"], field, nonnegative=True):
            raise ValueError("Original filled-trade costs disagree with the summary: " + field)
    costs = {field: _decimal(metrics[field], field, nonnegative=True) for field, _, _ in COST_FIELDS}
    turnover = _decimal(metrics["one_way_turnover_average_nav"], "turnover", nonnegative=True)
    return {"arm": arm, "period": period, "summary": summary, "dates": curves.date.tolist(),
        "nav": np.asarray([float(value) for value in nav]),
        "upper_nav": np.asarray([float(value / initial) for value in upper]),
        "drawdown": np.asarray([float(value) for value in drawdown]),
        "accounting_valid": accounting, "valuation_complete": valuation_complete,
        "valid": valid, "costs": costs, "turnover": float(turnover)}


def _load_experiment(source):
    manifest_sha = sha(source / "manifest.json")
    manifest = _read(source / "manifest.json")
    _verify_inventory(source, manifest, manifest_sha)
    report = _read(source / "report.json")
    status = _read(source / "status.json")
    if (report.get("kind") != KIND or report.get("models") != list(ARMS)
            or report.get("new_ledger_count") != 6 or report.get("new_fit_count") != 4
            or status.get("phase") != "completed"
            or report.get("execution", {}).get("rebalance_sessions") != 20):
        raise ValueError("A completed, separately declared four-fit/six-path A8 report is required")
    results = report.get("results", [])
    keys = [(row.get("arm"), row.get("period")) for row in results]
    expected = {(arm, period) for arm in ARMS for period in PERIODS}
    if len(keys) != 6 or len(set(keys)) != 6 or set(keys) != expected:
        raise ValueError("Exactly H5/H20 x2025/2026/continuous paths are mandatory")
    # Reject unreported summary paths rather than quietly omitting another arm.
    summary_paths = {name for name in manifest["files"] if name.endswith("/summary.json")}
    if summary_paths != {f"{arm}/{period}/summary.json" for arm, period in expected}:
        raise ValueError("The manifest must contain exactly the six reported summary paths")
    initial = _decimal(report["execution"]["initial_cash_cny"], "initial cash")
    if initial <= 0:
        raise ValueError("Positive declared starting cash is required")
    paths = {(row["arm"], row["period"]): _load_path(source, row, initial) for row in results}
    valid = all(path["valid"] for path in paths.values())
    if report.get("all_performance_valid") is not valid or status.get("all_performance_valid") is not valid:
        raise ValueError("Original global validity flags disagree with the six paths")
    return manifest, manifest_sha, report, paths, initial


def _save_figure(figure, output, stem):
    figure.savefig(output / (stem + ".png"), dpi=180, facecolor="white")
    figure.savefig(output / (stem + ".svg"), facecolor="white")


def _plot_paths(paths, output, plt, PercentFormatter):
    fig, axes = plt.subplots(3, 2, figsize=(13, 11), layout="constrained")
    titles = {"2025": "2025", "2026": "2026 through Oct 19 (partial year)",
        "continuous": "Continuous: 2025 to 2026-10-19"}
    for row, period in enumerate(PERIODS):
        left, right = axes[row]
        longest = max((paths[arm, period]["dates"] for arm in ARMS), key=len)
        invalid = []
        for arm in ARMS:
            path = paths[arm, period]
            if path["dates"] != longest[:len(path["dates"])]:
                raise ValueError("Paired arms must share their actual session calendar; no fill is allowed")
            x = np.arange(len(path["dates"]) + 1)
            label = arm + (" | VALID" if path["valid"] else " | indicative INVALID")
            style = "-" if path["valid"] else "--"
            left.plot(x, np.r_[1., path["nav"]], label=label, color=COLORS[arm], linestyle=style, lw=1.7)
            right.plot(x, np.r_[0., path["drawdown"]], label=label, color=COLORS[arm], linestyle=style, lw=1.7)
            if np.any(path["upper_nav"] != path["nav"]):
                left.fill_between(x[1:], path["nav"], path["upper_nav"], color=COLORS[arm], alpha=.10)
            bad = np.flatnonzero(~path["accounting_valid"])
            for axis, values in ((left, path["nav"]), (right, path["drawdown"])):
                if len(bad):
                    axis.scatter(bad + 1, values[bad], color="#b91c1c", marker="x", s=42, zorder=6)
            if not path["valid"]:
                invalid.append(arm + (" (stopped " + path["dates"][-1] + ")"
                    if path["summary"].get("stopped") is not None else ""))
        left.set_title(titles[period] + " | Net accounting NAV")
        right.set_title(titles[period] + " | Conservative drawdown")
        left.set_ylabel("Net NAV (initial cash = 1.00)")
        right.set_ylabel("Lower NAV / prior upper peak - 1")
        right.yaxis.set_major_formatter(PercentFormatter(1))
        left.axhline(1, color="#64748b", lw=.7, alpha=.6)
        right.axhline(0, color="#64748b", lw=.7, alpha=.6)
        # Do not squeeze the cash-start and first-market-session labels into
        # adjacent pixels on a432-session path. Coordinate1 remains plotted.
        ticks = sorted({0, len(longest), *np.linspace(max(1, len(longest)//4), len(longest), 4, dtype=int).tolist()})
        labels = ["Cash\nstart" if value == 0 else longest[value-1] for value in ticks]
        for axis in (left, right):
            axis.set_xticks(ticks, labels, rotation=22, ha="right", fontsize=8)
            axis.set_xlabel("Actual exchange sessions (cash-start anchor is not a market observation)")
            axis.grid(alpha=.16); axis.set_axisbelow(True)
            axis.legend(loc="best", fontsize=8)
            if invalid:
                axis.text(.02, .03, "indicative INVALID: " + ", ".join(invalid), transform=axis.transAxes,
                    color="#b91c1c", fontsize=10, fontweight="bold",
                    bbox={"facecolor": "#fff1f2", "edgecolor": "#ef4444", "alpha": .94, "pad": 4})
    fig.suptitle("A8 matched H5 / H20 models | Same 20-session execution cadence\n"
        "All six original paths retained; INVALID curves are diagnostic carried-ledger values, not qualified returns",
        fontsize=12)
    _save_figure(fig, output, "horizon_nav_and_drawdown")
    plt.close(fig)


def _plot_metrics(paths, initial, output, plt, PercentFormatter):
    fig, axes = plt.subplots(3, 3, figsize=(13, 9.5), layout="constrained")
    for row, period in enumerate(PERIODS):
        for number, arm in enumerate(ARMS):
            path, x = paths[arm, period], number
            metrics = path["summary"]["metrics"]
            net = float(_decimal(metrics["total_return_lower_exact"], "net return"))
            bars = [axes[row, 0].bar(x, net, width=.58, color=COLORS[arm])[0],
                axes[row, 2].bar(x, path["turnover"], width=.58, color=COLORS[arm])[0]]
            bottom = 0.
            for field, label, color in COST_FIELDS:
                value = float(path["costs"][field] / initial)
                bars.append(axes[row, 1].bar(x, value, bottom=bottom, width=.58, color=color,
                    label=label if row == 0 and number == 0 else None)[0])
                bottom += value
            if not path["valid"]:
                for bar in bars:
                    bar.set_hatch("///"); bar.set_edgecolor("#b91c1c"); bar.set_linewidth(1.1)
        for col, title in enumerate(("Net return", "Recorded costs / initial cash", "One-way turnover / mean NAV")):
            axis = axes[row, col]
            axis.set_title(f"{period} | {title}", fontsize=10)
            axis.set_xticks([0, 1], [arm + ("\nVALID" if paths[arm, period]["valid"] else "\nindicative INVALID") for arm in ARMS], fontsize=8)
            for tick, arm in zip(axis.get_xticklabels(), ARMS):
                if not paths[arm, period]["valid"]:
                    tick.set_color("#b91c1c"); tick.set_fontweight("bold")
            axis.axhline(0, color="#64748b", lw=.7)
            axis.grid(axis="y", alpha=.16); axis.set_axisbelow(True)
            if col < 2:
                axis.yaxis.set_major_formatter(PercentFormatter(1))
            else:
                axis.set_ylabel("Times (not annualized)")
    axes[0, 1].legend(fontsize=7, loc="best")
    fig.suptitle("A8 accounting diagnostics | Same initial cash, fees and execution rules\n"
        "Costs are recorded fees, adverse slippage and tax / rounding reserves; not a counterfactual gross-return attribution",
        fontsize=12)
    _save_figure(fig, output, "horizon_return_cost_turnover")
    plt.close(fig)


def plot(source, output):
    source, output = Path(source).resolve(), Path(output).resolve()
    if output.exists() or output.is_relative_to(source) or source.is_relative_to(output):
        raise ValueError("Use a new output directory outside the immutable source experiment")
    implementation_bytes = Path(__file__).read_bytes()
    implementation_sha = hashlib.sha256(implementation_bytes).hexdigest()
    manifest, manifest_sha, report, paths, initial = _load_experiment(source)
    report_sha = sha(source / "report.json")
    os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "stopat30m-a8-matplotlib"))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9,
        "axes.spines.top": False, "axes.spines.right": False, "svg.fonttype": "none"})
    output.mkdir(parents=True)
    try:
        _plot_paths(paths, output, plt, PercentFormatter)
        _plot_metrics(paths, initial, output, plt, PercentFormatter)
        rows, summaries = [], []
        for period in PERIODS:
            for arm in ARMS:
                path = paths[arm, period]
                for index, day in enumerate(path["dates"]):
                    rows.append({"arm": arm, "period": period, "date": day, "net_nav": path["nav"][index],
                        "upper_nav": path["upper_nav"][index], "conservative_drawdown": path["drawdown"][index],
                        "accounting_valid": bool(path["accounting_valid"][index]),
                        "valuation_complete": bool(path["valuation_complete"][index]),
                        "original_performance_valid": path["valid"],
                        "display_status": "VALID" if path["valid"] else "indicative INVALID"})
                summaries.append({"arm": arm, "period": period, "performance_valid": path["valid"],
                    "net_return_exact": path["summary"]["metrics"]["total_return_lower_exact"],
                    "conservative_max_drawdown_exact": path["summary"]["metrics"]["max_drawdown_conservative_exact"],
                    "one_way_turnover_average_nav": path["turnover"],
                    **{field: str(value) for field, value in path["costs"].items()}})
        pd.DataFrame(rows).to_csv(output / "plotted_series.csv", index=False, float_format="%.17g")
        pd.DataFrame(summaries).to_csv(output / "plotted_summaries.csv", index=False)
        (output / "source_plot.py").write_bytes(implementation_bytes)
        shutil.copyfile(source / "manifest.json", output / "input_manifest.json")
        shutil.copyfile(source / "report.json", output / "input_report.json")
        _verify_inventory(source, manifest, manifest_sha)
        if sha(source / "report.json") != report_sha or sha(__file__) != implementation_sha:
            raise ValueError("Plot implementation or original report changed during rendering")
        invalid = [f"{arm}/{period}" for period in PERIODS for arm in ARMS if not paths[arm, period]["valid"]]
        provenance = {"kind": "a8_horizon_model_scientific_plots", "status": "completed",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "source_experiment": str(source), "input_file_inventory_verified": True,
            "source_manifest": {"path": str(source / "manifest.json"), "sha256": manifest_sha},
            "source_report": {"path": str(source / "report.json"), "sha256": report_sha},
            "plot_implementation_sha256": implementation_sha,
            "exact_paths": [f"{arm}/{period}" for period in PERIODS for arm in ARMS],
            "invalid_paths_preserved_and_labelled": invalid,
            "drawdown_definition": "lower accounting NAV / cumulative upper NAV peak - 1; initial cash included",
            "cash_start_anchor": "plot coordinate zero only; not an invented market session or observation",
            "performance_validity_changed": False, "source_experiment_changed": False,
            "training_backtest_or_selection_performed": False,
            "recorded_costs_are_counterfactual_attribution": False,
            "matplotlib_version": matplotlib.__version__,
            "files": {str(path.relative_to(output)): sha(path) for path in sorted(output.rglob("*")) if path.is_file()}}
        (output / "manifest.json").write_text(json.dumps(provenance, indent=2, allow_nan=False) + "\n")
        return provenance
    except Exception as exc:
        (output / "failure.json").write_text(json.dumps({"status": "failed", "exception_type": type(exc).__name__,
            "message": str(exc), "no_completed_plot_claim": True}, indent=2) + "\n")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    result = plot(args.experiment_dir, args.output_dir)
    print(json.dumps({"output_directory": str(args.output_dir.resolve()),
        "invalid_paths_preserved": result["invalid_paths_preserved_and_labelled"]}))
