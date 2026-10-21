"""Plot the completed three-stage A14 comparison without fit, prediction or replay.

All fourteen recorded accounts and all six native fits remain visible. Invalid
or stopped accounts are diagnostic curves, never silently excluded performance.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

PHASES = ("selection", "confirmation", "retrospective")
CANDIDATES = ("capacity300", "regularized300", "recent3_capacity300")
REPORT_KIND = "a14_model_debug_costed_comparison"
MODEL_KIND = "a14_staged_model_debug_predictions"
HOLD_RULE = {"key": "hold10_cap2", "minimum_hold_sessions": 10, "max_replacements": 2}
INITIAL = Decimal("100000.00")
LABELS = {"baseline": "Original 100 trees / 5y", "capacity300": "300 trees / 5y",
    "regularized300": "Regularized 300 / 5y", "recent3_capacity300": "300 trees / 3y"}
COLORS = {"baseline": "#3564a1", "capacity300": "#d78327", "regularized300": "#3d8b60", "recent3_capacity300": "#9659a1"}
TRADE_FIELDS = ("notional", "commission", "transfer_fee", "stamp_tax", "total_fee", "slippage_cost")


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read(path):
    result = json.loads(Path(path).read_bytes())
    if not isinstance(result, dict):
        raise ValueError("An immutable JSON object is required")
    return result


def write(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n")


def number(value):
    if isinstance(value, (bool, np.bool_)):
        raise ValueError("A finite decimal accounting value is required")
    try:
        value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("A finite decimal accounting value is required") from None
    if not value.is_finite():
        raise ValueError("A finite decimal accounting value is required")
    return value


def proof(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": sha(path)}


def assert_proof(value):
    if (not isinstance(value, dict) or not isinstance(value.get("path"), str)
            or not Path(value["path"]).is_absolute() or proof(value["path"]) != value):
        raise ValueError("An exact original absolute file proof is required")


def inventory(directory, *, kind):
    directory = Path(directory).resolve()
    manifest = read(directory / "manifest.json")
    if manifest.get("kind") != kind or not isinstance(manifest.get("files"), dict) or not manifest["files"]:
        raise ValueError("A complete sealed input manifest is required")
    paths = list(directory.rglob("*"))
    if any(path.is_symlink() for path in paths):
        raise ValueError("Immutable input packages cannot contain symbolic links")
    actual = {str(path.relative_to(directory)) for path in paths if path.is_file() and path != directory / "manifest.json"}
    if actual != set(manifest["files"]):
        raise ValueError("Input package inventory omits or invents a file")
    result = {str(directory / "manifest.json"): sha(directory / "manifest.json")}
    for name, digest in manifest["files"].items():
        path = (directory / name).resolve()
        if Path(name).is_absolute() or ".." in Path(name).parts or not path.is_relative_to(directory) or sha(path) != digest:
            raise ValueError("Input artifact changed or escapes its immutable package")
        result[str(path)] = digest
    return manifest, result


def csv(path):
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def selected_from_2023(rows):
    keys = [(r.get("candidate"), r.get("period")) for r in rows]
    if len(keys) != 4 or set(keys) != {(k, "2023") for k in ("baseline", *CANDIDATES)}:
        raise ValueError("Selection must retain exactly four 2023-only accounts")
    paths = {r["candidate"]: r for r in rows}
    if any(type(r.get("performance_valid")) is not bool for r in rows):
        raise ValueError("Every path needs an explicit validity Boolean")
    def beats(a, b):
        return (a["performance_valid"] and b["performance_valid"]
            and number(a["metrics"]["total_return_lower_exact"]) > number(b["metrics"]["total_return_lower_exact"])
            and number(a["metrics"]["max_drawdown_conservative_exact"]) >= number(b["metrics"]["max_drawdown_conservative_exact"]))
    valid = [k for k in CANDIDATES if paths[k]["performance_valid"]]
    eligible = [k for k in valid if beats(paths[k], paths["baseline"])]
    def order(key):
        m = paths[key]["metrics"]
        return (-number(m["total_return_lower_exact"]), -number(m["max_drawdown_conservative_exact"]),
            number(m["total_fee_cny"]) + number(m["slippage_cost_cny"]), key)
    ranked = sorted(eligible or valid, key=order)
    return {"selected_candidate": ranked[0] if ranked else None, "eligible_candidates": sorted(eligible),
        "valid_candidates": sorted(valid), "ranked_candidates_under_frozen_rule": ranked,
        "primary_qualified": bool(eligible), "diagnostic_only": not bool(eligible),
        "selection_year": 2023, "later_years_used_for_selection": False,
        "confirmation_allowed": bool(ranked), "live_model_selected": None,
        "selection_metrics_are_afterfee_ledger_not_rank_IC": True}


def account_window(phase, period):
    if phase == "selection": return "2023-01-01", "2023-12-31"
    if phase == "confirmation": return ("2024-01-01", "2024-12-31") if period == "2024" else ("2023-01-01", "2024-12-31")
    return {"2025": ("2025-01-01", "2025-12-31"), "2026": ("2026-01-01", "2026-10-19"),
        "continuous": ("2025-01-01", "2026-10-19")}[period]


def load_account(directory, row, phase):
    path = Path(directory) / row["candidate"] / row["period"]
    if read(path / "summary.json") != row or type(row.get("performance_valid")) is not bool or row.get("variant") != HOLD_RULE:
        raise ValueError("Original fixed20 account summary, validity or execution terms differ")
    curve = csv(path / "curves.csv")
    required = {"date", "equity_cny", "equity_upper_bound_cny", "nav", "accounting_valid", "valuation_complete"}
    if curve.empty or not required.issubset(curve):
        raise ValueError("Every original account must preserve its recorded curve")
    dates = pd.to_datetime(curve.date, errors="raise")
    start, end = account_window(phase, row["period"])
    if (dates.duplicated().any() or not dates.is_monotonic_increasing or dates.dt.tz is not None
            or curve.date.tolist() != dates.dt.strftime("%Y-%m-%d").tolist()
            or curve.date.min() < start or curve.date.max() > end):
        raise ValueError("Account dates fall outside their original ordered family window")
    if any(not curve[k].isin(["True", "False"]).all() for k in ("accounting_valid", "valuation_complete")):
        raise ValueError("Original curve accounting flags must be explicit Booleans")
    lower = [number(v) for v in curve.equity_cny]
    upper = [number(v) for v in curve.equity_upper_bound_cny]
    if any(lo <= 0 or hi < lo for lo, hi in zip(lower, upper)):
        raise ValueError("Original equity bounds are inconsistent")
    peak, dd = INITIAL, []
    for lo, hi in zip(lower, upper):
        peak = max(peak, hi); dd.append(lo / peak - 1)
    nav = [lo / INITIAL for lo in lower]
    m = row["metrics"]
    invalid_days = sum(v == "False" for v in curve.accounting_valid)
    if (m["session_count"] != len(curve) or number(m["final_equity_cny"]) != lower[-1]
            or number(m["total_return_lower_exact"]) != nav[-1] - 1
            or number(m["max_drawdown_conservative_exact"]) != min([Decimal(0), *dd])
            or m["accounting_invalid_days"] != invalid_days
            or not np.allclose([float(number(v)) for v in curve.nav], [float(v) for v in nav], atol=1e-12, rtol=1e-12)
            or row.get("actual_last_valued_session") != curve.date.iloc[-1]):
        raise ValueError("Original NAV, conservative drawdown or terminal metrics do not reconcile")
    if row["performance_valid"]:
        if (invalid_days or row.get("stopped") is not None or m.get("terminal_prices_fresh") is not True
                or m.get("declared_end_reached") is not True):
            raise ValueError("A financially valid account contradicts its original complete valuation status")
    elif "invalid" not in row.get("metrics_basis", "").lower():
        raise ValueError("Invalid accounts must keep an explicit diagnostic performance basis")
    trades = csv(path / "trades.csv")
    if m["trade_count"] != len(trades):
        raise ValueError("Actual trade count differs from the account metrics")
    for field in TRADE_FIELDS:
        if len(trades) and field not in trades:
            raise ValueError("An original trade cost field is missing")
        total = sum((number(v) for v in trades.get(field, ())), Decimal(0))
        if total < 0 or total != number(m[field + "_cny"]):
            raise ValueError("Original trade cost totals do not reconcile: " + field)
    if number(m["total_fee_cny"]) != sum((number(m[k + "_cny"]) for k in ("commission", "transfer_fee", "stamp_tax")), Decimal(0)):
        raise ValueError("Commission, transfer and stamp fees do not reconcile")
    turnover = float(number(m["notional_cny"]) / Decimal(str(np.mean([float(v) for v in lower]))) / 2)
    if not np.isclose(turnover, float(number(m["one_way_turnover_average_nav"])), atol=1e-12, rtol=1e-12):
        raise ValueError("Original actual-notional turnover does not reconcile")
    return {"phase": phase, "candidate": row["candidate"], "period": row["period"], "valid": row["performance_valid"],
        "dates": dates, "nav": np.asarray([float(v) for v in nav]), "dd": np.asarray([float(v) for v in dd]),
        "invalid_mask": curve.accounting_valid.eq("False").to_numpy(), "summary": row,
        "costs": {"Commission": float(number(m["commission_cny"])), "Transfer + stamp": float(number(m["transfer_fee_cny"]) + number(m["stamp_tax_cny"])),
            "Embedded slippage": float(number(m["slippage_cost_cny"])),
            "Distribution tax reserve": float(number(m.get("total_distribution_tax_reserved_cny", "0"))),
            "Cash rounding": float(number(m.get("cash_distribution_rounding_reserved_cny", "0")))}}


def load_fit(directory, record, phase):
    name = f"{record['candidate']}_{record['execution_year']}"
    folder = Path(directory) / name
    result = {"phase": phase, "candidate": record["candidate"], "execution_year": record["execution_year"],
        "name": name, "lookback_years": record["lookback_calendar_years"]}
    if record.get("actual_tree_count") != 300 or record.get("spec", {}).get("num_boost_round") != 300:
        raise ValueError("Every predeclared candidate fit must preserve its 300-tree budget")
    # Reading saved native text is not model inference.
    if sum(line.startswith("Tree=") for line in (folder / "model/model.txt").read_text().splitlines()) != 300:
        raise ValueError("Saved native tree inventory differs from the recorded fit budget")
    for role in ("training", "validation"):
        stats = read(folder / (role + "_diagnostics.json"))
        if stats != record[role + "_diagnostics"]:
            raise ValueError("Saved fit diagnostics differ from their original model record")
        daily = csv(folder / (role + "_diagnostics_daily.csv"))
        known = pd.to_numeric(daily.rank_ic.replace("", np.nan), errors="raise").dropna()
        expected_mean = float(known.mean()) if len(known) else None
        if (len(daily) != stats["session_count"] or len(known) != stats["known_rank_ic_days"]
                or int(pd.to_numeric(daily.observations).sum()) != stats["row_count"]
                or (stats["mean_daily_rank_ic"] is None) != (expected_mean is None)
                or expected_mean is not None and abs(expected_mean - stats["mean_daily_rank_ic"]) > 1e-14):
            raise ValueError("Fit daily rank IC, coverage or summary does not reconcile")
        if stats.get("independent_observations_claim") is not False or stats.get("in_sample_optimism") is not (role == "training"):
            raise ValueError("Fit diagnostic dependence or in-sample limitation was changed")
        result[role + "_rank_ic"] = expected_mean
        result[role + "_rank_ic_days"] = len(known)
    importance = csv(folder / "feature_importance.csv")
    if (not {"feature", "gain", "split"}.issubset(importance) or importance.feature.duplicated().any() or importance.empty):
        raise ValueError("A unique original native feature importance inventory is required")
    gain = importance.gain.map(number)
    if any(v < 0 for v in gain) or sum(gain) <= 0:
        raise ValueError("Native gains must be nonnegative and informative")
    total = sum(gain)
    result["gain_fraction"] = {name: float(value / total) for name, value in zip(importance.feature, gain)}
    return result


def load_comparison(*, selection_directory, confirmation_directory, retrospective_directory,
                    selection_models, confirmation_models, retrospective_models, selection_decision=None):
    directories = {"selection": Path(selection_directory).resolve(), "confirmation": Path(confirmation_directory).resolve(),
        "retrospective": Path(retrospective_directory).resolve()}
    models = {"selection": Path(selection_models).resolve(), "confirmation": Path(confirmation_models).resolve(),
        "retrospective": Path(retrospective_models).resolve()}
    inputs, reports, manifests, model_manifests = {}, {}, {}, {}
    for phase in PHASES:
        manifests[phase], bound = inventory(directories[phase], kind=REPORT_KIND); inputs.update(bound)
        model_manifests[phase], bound = inventory(models[phase], kind=MODEL_KIND); inputs.update(bound)
        reports[phase] = read(directories[phase] / "report.json")
        report, model = reports[phase], model_manifests[phase]
        if (report.get("kind") != REPORT_KIND or report.get("phase") != phase
                or manifests[phase].get("phase") != phase or model.get("phase") != phase
                or report.get("model_manifest") != proof(models[phase] / "manifest.json")
                or report.get("training_declaration") != model.get("inputs", {}).get("declaration")
                or report.get("active_paper_changed") is not False or report.get("broker_order_sent") is not False
                or report.get("automatic_model_promotion") is not False or report.get("new_native_fits_here") != 0):
            raise ValueError("A stage report, model package or live-account boundary differs")
        assert_proof(report["training_declaration"])
        inputs[report["training_declaration"]["path"]] = report["training_declaration"]["sha256"]
    if len({json.dumps(reports[p]["training_declaration"], sort_keys=True) for p in PHASES}) != 1:
        raise ValueError("All stages must share one frozen training declaration")
    decision_path = Path(selection_decision).resolve() if selection_decision is not None else directories["selection"] / "selection_decision.json"
    decision = read(decision_path); decision_proof = proof(decision_path); inputs[str(decision_path)] = decision_proof["sha256"]
    calculated = selected_from_2023(reports["selection"]["results"])
    if (decision.get("kind") != "a14_model_debug_selection_decision"
            or decision.get("selection_report") != proof(directories["selection"] / "report.json")
            or decision.get("selection_model_manifest") != proof(models["selection"] / "manifest.json")
            or decision.get("training_declaration") != reports["selection"]["training_declaration"]
            or reports["selection"].get("selection") != calculated
            or any(decision.get(k) != value for k, value in calculated.items())):
        raise ValueError("Frozen 2023 decision does not reconcile to its original model and ledger report")
    selected = decision["selected_candidate"]
    if selected not in CANDIDATES:
        raise ValueError("All-stage plots require the fixed selected candidate; no candidate may be invented")
    accounts, fits = [], []
    for phase in PHASES:
        report, model = reports[phase], model_manifests[phase]
        periods = {"selection": ("2023",), "confirmation": ("2024", "continuous"), "retrospective": ("2025", "2026", "continuous")}[phase]
        arms = ("baseline", *CANDIDATES) if phase == "selection" else ("baseline", selected)
        expected = {(arm, period) for arm in arms for period in periods}
        keys = [(row.get("candidate"), row.get("period")) for row in report["results"]]
        if len(keys) != len(expected) or set(keys) != expected or report.get("ledger_count") != len(expected):
            raise ValueError("Every one of the fourteen predeclared accounts must appear exactly once")
        if report.get("all_performance_valid") is not all(row.get("performance_valid") is True for row in report["results"]):
            raise ValueError("Stage validity summary contradicts its original accounts")
        years = {"selection": [2023], "confirmation": [2024], "retrospective": [2025, 2026]}[phase]
        fitted = list(CANDIDATES) if phase == "selection" else [selected]
        if (model.get("models") != fitted or model.get("execution_years") != years
                or model.get("native_fit_count") != len(fitted) * len(years)
                or len(model.get("folds", [])) != len(fitted) * len(years)
                or {(v.get("candidate"), v.get("execution_year")) for v in model["folds"]} != {(k, y) for k in fitted for y in years}):
            raise ValueError("Model package changed the fixed three/one/two fit budget")
        expected_family = "stability" if phase == "retrospective" else "primary"
        if set(model.get("source_families", {})) != {expected_family}:
            raise ValueError("Original primary and stability source families cannot be mixed")
        if phase != "selection" and (model.get("inputs", {}).get("selection_decision") != decision_proof
                or report.get("selected_candidate") != selected):
            raise ValueError("Later model/ledger stages cannot reselect from confirmation or retrospective results")
        if phase != "selection" and pd.Timestamp(model["started_at"]) < pd.Timestamp(decision["declared_at"]):
            raise ValueError("A later candidate fit cannot precede the frozen 2023 decision")
        accounts.extend(load_account(directories[phase], row, phase) for row in report["results"])
        fits.extend(load_fit(models[phase], record, phase) for record in model["folds"])
    return {"accounts": accounts, "fits": fits, "reports": reports, "decision": decision,
        "input_sha256": inputs, "directories": directories, "models": models, "decision_path": decision_path}


def _save_figure(fig, output, name):
    for ext in ("png", "svg"):
        fig.savefig(output / (name + "." + ext), dpi=175)


def _curve_figures(data, output):
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    panels = {"primary": [("selection", "2023"), ("confirmation", "2024"), ("confirmation", "continuous")],
        "retrospective": [("retrospective", "2025"), ("retrospective", "2026"), ("retrospective", "continuous")]}
    titles = {("selection", "2023"): "2023 selection | primary source | 4 predeclared arms",
        ("confirmation", "2024"): "2024 confirmation | primary source | fixed 2023 choice",
        ("confirmation", "continuous"): "2023-2024 continuous | primary source | no account reset",
        ("retrospective", "2025"): "2025 retrospective | stability source | previously viewed",
        ("retrospective", "2026"): "2026 to Oct 19 retrospective | stability source | previously viewed",
        ("retrospective", "continuous"): "2025-2026 continuous | stability source | no account reset"}
    for family, groups in panels.items():
        fig, axes = plt.subplots(3, 2, figsize=(15, 12), layout="constrained")
        for i, (phase, period) in enumerate(groups):
            for account in [a for a in data["accounts"] if a["phase"] == phase and a["period"] == period]:
                key = account["candidate"]; color = COLORS[key]; valid = account["valid"]
                label = LABELS[key] + (" [INVALID diagnostic]" if not valid else "")
                style = "-" if valid else "--"
                for j, values in enumerate((account["nav"], account["dd"] * 100)):
                    axes[i, j].plot(account["dates"], values, color=color, ls=style, lw=1.4, label=label)
                    if not valid:
                        axes[i, j].scatter(account["dates"].iloc[-1], values[-1], color=color, marker="x", s=42, zorder=4)
                        axes[i, j].scatter(account["dates"][account["invalid_mask"]], values[account["invalid_mask"]], color=color, marker="x", s=14, alpha=.5)
                if account["summary"].get("stopped") is not None:
                    axes[i, 0].annotate("STOP", (account["dates"].iloc[-1], account["nav"][-1]), fontsize=7, color=color)
            axes[i, 0].set_title(titles[phase, period], fontsize=10)
            axes[i, 1].set_title("Lower NAV / historical upper peak; cash-start peak included", fontsize=9)
            axes[i, 0].set_ylabel("After-fee lower NAV (start 1.0)")
            axes[i, 1].set_ylabel("Conservative drawdown (%)")
            axes[i, 0].axhline(1, color="black", lw=.6); axes[i, 1].axhline(0, color="black", lw=.6)
            axes[i, 0].legend(fontsize=7, loc="best")
            for ax in axes[i]:
                locator = mdates.AutoDateLocator(minticks=3, maxticks=7)
                ax.xaxis.set_major_locator(locator); ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
                ax.grid(alpha=.2); ax.set_axisbelow(True)
        fig.suptitle("A14 | Fixed 20 names / hold10 / cap2 | original per-order fees and embedded slippage\nAll recorded accounts retained; no cost addback, no live-model switch", fontsize=12)
        _save_figure(fig, output, "model_debug_" + family + "_nav_drawdown"); plt.close(fig)


def _metric_figure(data, output):
    import matplotlib.pyplot as plt
    accounts = data["accounts"]; ys = np.arange(len(accounts)); fig, axes = plt.subplots(1, 4, figsize=(19, 11), sharey=True, layout="constrained")
    labels = []
    for account in accounts:
        phase, period, arm = account["phase"], account["period"], account["candidate"]
        label = f"{phase[:3]} / {period} / {LABELS[arm]}" + (" [INVALID]" if not account["valid"] else "")
        labels.append(label)
    for i, account in enumerate(accounts):
        m = account["summary"]["metrics"]; color = COLORS[account["candidate"]]; hatch = None if account["valid"] else "///"
        for j, key, multiplier in ((0, "total_return_lower_exact", 100), (1, "max_drawdown_conservative_exact", -100), (2, "one_way_turnover_average_nav", 1)):
            axes[j].barh(i, float(number(m[key])) * multiplier, color=color, hatch=hatch, edgecolor="#333333", linewidth=.4)
        left = 0
        cost_colors = ["#4465a3", "#7f9ebc", "#d19a58", "#936f9d", "#b3b3b3"]
        for j, (name, value) in enumerate(account["costs"].items()):
            axes[3].barh(i, value, left=left, color=cost_colors[j], label=name if i == 0 else None, hatch=hatch, edgecolor="#333333", linewidth=.25); left += value
    for j, title in enumerate(("After-fee cumulative return (%)", "Conservative max drawdown magnitude (%)", "Cumulative one-way turnover (times)", "Recorded costs (CNY); already inside NAV")):
        axes[j].set_title(title, fontsize=9); axes[j].grid(axis="x", alpha=.2); axes[j].set_axisbelow(True); axes[j].axvline(0, color="black", lw=.6)
        for boundary in (3.5, 7.5): axes[j].axhline(boundary, color="#555555", lw=.8)
    axes[0].set_yticks(ys, labels, fontsize=8); axes[0].invert_yaxis()
    axes[3].set_xlim(0, max(sum(a["costs"].values()) for a in accounts) * 1.05)
    handles, names = axes[3].get_legend_handles_labels()
    fig.legend(handles, names, fontsize=8, ncol=5, loc="outside lower center")
    fig.suptitle("A14 | All 14 fixed accounts, grouped by selection / confirmation / retrospective\nHatched bars are invalid diagnostic values; independent-year accounts cannot be compounded into continuous NAV", fontsize=12)
    _save_figure(fig, output, "model_debug_return_drawdown_turnover_cost"); plt.close(fig)


def _fit_figure(data, output):
    import matplotlib.pyplot as plt
    fits = data["fits"]; labels = [f"{f['execution_year']} / {LABELS[f['candidate']]}\n{f['phase']}" for f in fits]
    xs = np.arange(len(fits)); fig, axes = plt.subplots(2, 1, figsize=(15, 12), layout="constrained", gridspec_kw={"height_ratios": [1, 1.8]})
    for offset, role, color in ((-.16, "training", "#8ba6c7"), (.16, "validation", "#ba7838")):
        values = [np.nan if f[role + "_rank_ic"] is None else f[role + "_rank_ic"] for f in fits]
        axes[0].bar(xs + offset, values, width=.32, label=("Training (optimistic in-sample)" if role == "training" else "Chronological annual diagnostic (already viewed history)"), color=color)
    axes[0].set(xticks=xs, xticklabels=labels, ylabel="Mean daily Spearman rank IC", title="Six recorded 300-tree fits; overlapping H5 labels, unequal training windows")
    axes[0].axhline(0, color="black", lw=.6); axes[0].legend(fontsize=8); axes[0].grid(axis="y", alpha=.2); axes[0].set_axisbelow(True)
    features = sorted({feature for fit in fits for feature in fit["gain_fraction"]},
        key=lambda feature: (-np.mean([fit["gain_fraction"].get(feature, 0) for fit in fits]), feature))[:15]
    values = np.asarray([[fit["gain_fraction"].get(feature, 0) * 100 for fit in fits] for feature in features])
    heat = axes[1].imshow(values, aspect="auto", cmap="Blues", vmin=0)
    axes[1].set(xticks=xs, xticklabels=labels, yticks=np.arange(len(features)), yticklabels=features,
        title="Native training gain share (%); top15 by mean recorded gain, descriptive only")
    for i in range(len(features)):
        for j in range(len(fits)):
            axes[1].text(j, i, f"{values[i,j]:.1f}", ha="center", va="center", fontsize=7,
                color="white" if values[i,j] > np.max(values) * .6 else "#222222")
    fig.colorbar(heat, ax=axes[1], shrink=.8, label="Recorded native gain share (%)")
    fig.suptitle("A14 | Training-vs-annual validation diagnostics and feature use\nRank IC is not net profit; gain is not causal importance; the 2023 choice stays frozen", fontsize=12)
    _save_figure(fig, output, "model_debug_fit_rankic_and_native_gain"); plt.close(fig)


def render_comparison(*, selection_directory, confirmation_directory, retrospective_directory,
                      selection_models, confirmation_models, retrospective_models, output_directory,
                      selection_decision=None):
    import matplotlib
    matplotlib.use("Agg")
    output = Path(output_directory).resolve()
    if output.exists() or "forward" in output.parts:
        raise ValueError("Preserve old plots and active paper; choose a new offline output directory")
    source_roots = [Path(v).resolve() for v in (selection_directory, confirmation_directory, retrospective_directory,
        selection_models, confirmation_models, retrospective_models)]
    if any(output.is_relative_to(p) or p.is_relative_to(output) for p in source_roots):
        raise ValueError("Plot output must stay separate from every original input package")
    data = load_comparison(selection_directory=selection_directory, confirmation_directory=confirmation_directory,
        retrospective_directory=retrospective_directory, selection_models=selection_models,
        confirmation_models=confirmation_models, retrospective_models=retrospective_models, selection_decision=selection_decision)
    before = dict(data["input_sha256"]); output.mkdir(parents=True, exist_ok=False)
    try:
        _curve_figures(data, output); _metric_figure(data, output); _fit_figure(data, output)
        series = []
        for account in data["accounts"]:
            for date, nav, dd, unknown in zip(account["dates"], account["nav"], account["dd"], account["invalid_mask"]):
                series.append({"phase": account["phase"], "candidate": account["candidate"], "period": account["period"],
                    "date": date.strftime("%Y-%m-%d"), "nav": nav, "drawdown_conservative": dd,
                    "account_valid": account["valid"], "day_accounting_valid": not bool(unknown)})
        pd.DataFrame(series).to_csv(output / "plotted_curves.csv", index=False, float_format="%.17g")
        pd.DataFrame([{**{k: a[k] for k in ("phase", "candidate", "period", "valid")}, **a["summary"]["metrics"],
            **{"plot_cost_" + k: v for k, v in a["costs"].items()}} for a in data["accounts"]]).to_csv(output / "plotted_metrics.csv", index=False)
        write(output / "plotted_fit_diagnostics.json", {"fits": data["fits"]})
        for phase in PHASES:
            archive = output / "inputs" / phase; archive.mkdir(parents=True)
            for root, prefix in ((data["directories"][phase], "cost_"), (data["models"][phase], "model_")):
                for name in ("manifest.json", "report.json"):
                    if (root / name).is_file(): shutil.copyfile(root / name, archive / (prefix + name))
        shutil.copyfile(data["decision_path"], output / "frozen_2023_selection_decision.json")
        shutil.copyfile(__file__, output / Path(__file__).name)
        if any(sha(path) != digest for path, digest in before.items()):
            raise ValueError("Original input bytes changed during rendering")
        write(output / "manifest.json", {"kind": "a14_staged_model_debug_scientific_plots", "completed_at": datetime.now(timezone.utc).isoformat(),
            "input_sha256_before_and_after": before, "source_files_unchanged": True,
            "recorded_accounts": 14, "recorded_fits": 6, "selected_candidate": data["decision"]["selected_candidate"],
            "selection_uses_2023_only": True, "no_fits_predictions_or_strategy_replay": True,
            "active_paper_changed": False, "automatic_promotion": False,
            "original_top20_signal_figures": str(Path("output/research/ashare/plots/a14_original_model_signal_20261021_v1").resolve()),
            "invalid_paths_retained": [f"{a['phase']}/{a['candidate']}/{a['period']}" for a in data["accounts"] if not a["valid"]],
            "files": {str(p.relative_to(output)): sha(p) for p in sorted(output.rglob("*")) if p.is_file()}})
        return output
    except Exception as exc:
        write(output / "failure.json", {"exception_type": type(exc).__name__, "message": str(exc), "source_mutation_performed": False})
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("selection_directory", "confirmation_directory", "retrospective_directory", "selection_models", "confirmation_models", "retrospective_models", "output_directory"):
        parser.add_argument("--" + key.replace("_", "-"), required=True, type=Path)
    parser.add_argument("--selection-decision", type=Path)
    print(render_comparison(**vars(parser.parse_args())))
