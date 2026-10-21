"""Scientific plots of a sealed A10 report; no fit, simulation or selection.

Usage: python scripts/analyze_position_count_comparison.py --report REPORT.json
       --output-dir NEW_DIRECTORY

All twelve recorded paths, including invalid/stopped endpoints, are retained.
Net NAV is never increased by recorded costs. Unknown concentration is a gap,
not zero exposure. The current prospective paper account is never accessed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile

import numpy as np
import pandas as pd

KIND = "a10_fixed_lgb_position_count_results"
COUNTS = (20, 15, 10, 5)
PERIODS = ("2025", "2026", "continuous")
COLORS = {20: "#2563eb", 15: "#d97706", 10: "#15803d", 5: "#9333ea"}
WINDOWS = {"2025": ("2025-01-01", "2025-12-31"), "2026": ("2026-01-01", "2026-10-19"),
           "continuous": ("2025-01-01", "2026-10-19")}
TRADE_FIELDS = ("notional", "commission", "transfer_fee", "stamp_tax", "total_fee", "slippage_cost")
COSTS = (("commission_cny", "Commission", "#4465a3"), ("transfer_fee_cny", "Transfer", "#82a8cb"),
         ("stamp_tax_cny", "Stamp", "#a2bfa0"), ("slippage_cost_cny", "Slippage", "#df9d43"),
         ("total_distribution_tax_reserved_cny", "Distribution tax", "#b38da6"),
         ("cash_distribution_rounding_reserved_cny", "Cash rounding", "#a1a1aa"))
RISK_FIELDS = (("max_single_nav_fraction", "Largest name / lower NAV"),
               ("max_top3_nav_fraction", "Largest 3 names / lower NAV"),
               ("effective_stock_count", "Effective risky-name count (1 / HHI)"),
               ("risky_nav_fraction", "Risky upper exposure / lower NAV"))


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read(path):
    return json.loads(Path(path).read_bytes())


def write(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)+"\n")


def number(value):
    if isinstance(value, bool):
        raise ValueError("A finite decimal accounting value is required")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("A finite decimal accounting value is required") from None
    if not result.is_finite():
        raise ValueError("A finite decimal accounting value is required")
    return result


def verify_inventory(source, manifest, manifest_sha):
    if sha(source/"manifest.json") != manifest_sha or manifest.get("kind") != KIND:
        raise ValueError("Completed A10 manifest changed or has the wrong kind")
    files = manifest.get("files")
    if not isinstance(files, dict) or not files or "manifest.json" in files:
        raise ValueError("A complete non-self-referential artifact inventory is required")
    if any(p.is_symlink() for p in source.rglob("*")):
        raise ValueError("Artifact symlinks are not immutable source files")
    for name, digest in files.items():
        path = Path(name)
        actual = (source/path).resolve()
        if (path.is_absolute() or ".." in path.parts or not actual.is_relative_to(source)
                or not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest)
                or not actual.is_file() or sha(actual) != digest):
            raise ValueError("Changed, missing or unsafe artifact: "+name)
    actual = {str(p.relative_to(source)) for p in source.rglob("*") if p.is_file() and p != source/"manifest.json"}
    if actual != set(files):
        raise ValueError("Artifact inventory differs from the completed manifest")


def csv(path):
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def load_path(source, row, initial):
    n, period = row["target_count"], row["period"]
    directory = source/("top"+str(n))/period
    if read(directory/"summary.json") != row or type(row.get("performance_valid")) is not bool:
        raise ValueError("Original path summary/validity differs from the report")
    diag = read(directory/"diagnostics.json")
    if (diag.get("kind") != "a10_original_ledger_position_count_diagnostics"
            or diag.get("performance_valid") is not row["performance_valid"]
            or {k:v for k,v in diag.items() if k != "daily_exposures"} != row.get("diagnostics")):
        raise ValueError("Original diagnostics differ from the path summary")
    curve, metrics = csv(directory/"curves.csv"), row["metrics"]
    required = {"date", "equity_cny", "equity_upper_bound_cny", "nav", "accounting_valid", "valuation_complete"}
    if curve.empty or not required.issubset(curve):
        raise ValueError("Each declared path needs its complete original curves")
    dates = pd.to_datetime(curve.date, errors="raise")
    start, end = WINDOWS[period]
    if (dates.dt.tz is not None or dates.duplicated().any() or not dates.is_monotonic_increasing
            or curve.date.tolist() != dates.dt.strftime("%Y-%m-%d").tolist()
            or curve.date.min() < start or curve.date.max() > end):
        raise ValueError("Original curve dates are not ordered actual dates within the account window")
    for field in ("accounting_valid", "valuation_complete"):
        if not curve[field].isin(["True", "False"]).all():
            raise ValueError("Original curve flags must be explicit Booleans")
    lower, upper = [[number(value) for value in curve[field]] for field in ("equity_cny", "equity_upper_bound_cny")]
    if any(lo <= 0 or hi < lo for lo, hi in zip(lower, upper)):
        raise ValueError("Original accounting NAV bounds are inconsistent")
    peak, dd = initial, []
    for lo, hi in zip(lower, upper):
        peak = max(peak, hi)
        dd.append(lo/peak-1)
    nav = [lo/initial for lo in lower]
    flags = curve.accounting_valid.eq("True").tolist()
    if (not np.allclose([float(number(v)) for v in curve.nav], [float(v) for v in nav], rtol=1e-12, atol=1e-12)
            or metrics["session_count"] != len(curve) or number(metrics["final_equity_cny"]) != lower[-1]
            or number(metrics["total_return_lower_exact"]) != nav[-1]-1
            or number(metrics["max_drawdown_conservative_exact"]) != min([Decimal(0), *dd])
            or metrics["accounting_invalid_days"] != sum(not v for v in flags)
            or row["actual_last_valued_session"] != curve.date.iloc[-1]):
        raise ValueError("Original curve and recorded path metrics do not reconcile")
    valid = row["performance_valid"]
    if valid and (not all(flags) or metrics.get("terminal_prices_fresh") is not True
                  or metrics.get("declared_end_reached") is not True or row.get("stopped") is not None):
        raise ValueError("A valid path contradicts its original accounting or terminal status")
    if not valid and "invalid" not in row.get("metrics_basis", "").lower():
        raise ValueError("An invalid path must retain its invalid diagnostic basis")
    trades = csv(directory/"trades.csv")
    if len(trades) != metrics["trade_count"]:
        raise ValueError("Original trade count differs from the summary")
    for field in TRADE_FIELDS:
        if not trades.empty and field not in trades:
            raise ValueError("Original filled-trade cost field is absent: "+field)
        total = sum((number(v) for v in trades.get(field, ())), Decimal(0))
        if total < 0 or total != number(metrics[field+"_cny"]) or total != number(diag["costs"][field+"_cny"]):
            raise ValueError("Original actual-fill costs do not reconcile: "+field)
    if (diag["costs"].get("money_reconciliation_passed") is not True
            or diag["costs"].get("cost_addback_performed") is not False
            or diag["costs"]["fill_count"] != len(trades)):
        raise ValueError("Recorded cost diagnostics must preserve actual costs without NAV addback")
    fees = sum((number(metrics[f+"_cny"]) for f in ("commission", "transfer_fee", "stamp_tax")), Decimal(0))
    if fees != number(metrics["total_fee_cny"]):
        raise ValueError("Recorded fee components are inconsistent")
    costs = {field:number(metrics[field]) for field,_,_ in COSTS}
    if any(v < 0 for v in costs.values()):
        raise ValueError("Negative recorded cost")
    daily = diag["daily_exposures"]
    risk = diag["risk"]
    if (not isinstance(daily, list) or [d["date"] for d in daily] != curve.date.tolist()
            or type(risk.get("coverage_complete")) is not bool
            or any(type(d.get("coverage_complete")) is not bool for d in daily)
            or risk["coverage_complete"] is not all(d["coverage_complete"] for d in daily)):
        raise ValueError("Every original curve session needs explicit concentration coverage")
    if not risk["coverage_complete"] and any(risk.get(k) is not None for k in (
            "max_single_nav_fraction", "max_top3_nav_fraction", "min_effective_stock_count", "mean_risky_nav_fraction")):
        raise ValueError("Unknown aggregate concentration must not be represented by zero or a known value")
    risk_series = {}
    for field,_ in RISK_FIELDS:
        values = []
        for observation in daily:
            value = observation.get(field) if observation["coverage_complete"] else None
            if value is not None:
                value = float(number(value))
                if value < 0:
                    raise ValueError("Concentration/exposure must be nonnegative")
            elif observation["coverage_complete"] and field != "effective_stock_count":
                raise ValueError("Complete exposure session lacks its original risk ratio")
            values.append(value)
        risk_series[field] = values
    turnover = number(metrics["one_way_turnover_average_nav"])
    if turnover < 0:
        raise ValueError("Turnover cannot be negative")
    return {"target_count":n,"period":period,"valid":valid,"summary":row,"diagnostics":diag,
        "dates":curve.date.tolist(),"nav":[float(v) for v in nav],"upper_nav":[float(v/initial) for v in upper],
        "drawdown":[float(v) for v in dd],"accounting_valid":flags,
        "valuation_complete":curve.valuation_complete.eq("True").tolist(),
        "costs":{k:str(v) for k,v in costs.items()},"turnover":float(turnover),
        "risk_coverage_complete":[d["coverage_complete"] for d in daily],"risk_series":risk_series}


def load_report(report_path):
    report_path = Path(report_path).resolve()
    if report_path.name != "report.json":
        raise ValueError("Pass the original completed report.json")
    source = report_path.parent
    manifest_sha = sha(source/"manifest.json")
    manifest = read(source/"manifest.json")
    verify_inventory(source, manifest, manifest_sha)
    report, status, declaration = read(report_path), read(source/"status.json"), read(source/"declaration.json")
    if (report.get("kind") != KIND or status.get("phase") != "completed"
            or report.get("native_fits") != 0 or report.get("predictions_regenerated") is not False
            or report.get("active_paper_changed_by_experiment") is not False or report.get("live_broker_orders") != 0):
        raise ValueError("A completed offline zero-fit A10 report with unchanged paper is required")
    initial = number(declaration["design"]["initial_cash_cny"])
    if initial != 100000 or declaration["design"]["target_counts"] != list(COUNTS):
        raise ValueError("The predeclared four-count 100000-CNY comparison is required")
    rows = report.get("results", [])
    keys = [(row.get("target_count"), row.get("period")) for row in rows]
    expected = {(n,p) for n in COUNTS for p in PERIODS}
    if (len(keys) != 12 or set(keys) != expected or any(type(n) is not int for n,_ in keys)
            or {name for name in manifest["files"] if name.endswith("/summary.json")} != {f"top{n}/{p}/summary.json" for n,p in expected}):
        raise ValueError("Exactly all twelve top20/15/10/5 x2025/2026/continuous paths are required")
    if (set(report.get("baseline_full_reproduction", {})) != set(PERIODS)
            or any(report["baseline_full_reproduction"][p].get("passed") is not True for p in PERIODS)):
        raise ValueError("Three exact original top20 reproduction guards are required")
    paths = {(row["target_count"],row["period"]):load_path(source,row,initial) for row in rows}
    valid = all(v["valid"] for v in paths.values())
    if report.get("all_performance_valid") is not valid or status.get("all_performance_valid") is not valid:
        raise ValueError("Original aggregate validity does not match all twelve paths")
    for period in PERIODS:
        longest = max((paths[n,period]["dates"] for n in COUNTS),key=len)
        if any(paths[n,period]["dates"] != longest[:len(paths[n,period]["dates"])] for n in COUNTS):
            raise ValueError("Paired actual calendars must agree; no filling an omitted session")
    return source,manifest,manifest_sha,report,paths,initial


def save_figure(fig, output, name):
    fig.savefig(output/(name+".png"),dpi=175,facecolor="white")
    fig.savefig(output/(name+".svg"),facecolor="white")


def decorate_time(axis, dates, *, cash_anchor=True):
    length=len(dates)
    ticks=sorted(({0} if cash_anchor else {1}) | {length,*np.linspace(max(1,length//4),length,4,dtype=int).tolist()})
    axis.set_xticks(ticks,["Cash\nstart" if x==0 else dates[x-1] for x in ticks],rotation=20,ha="right",fontsize=7)
    axis.grid(alpha=.17);axis.set_axisbelow(True)


def legend(axis, *, location="best"):
    box=axis.legend(fontsize=7,loc=location)
    if box:
        for text in box.get_texts():
            if "INVALID" in text.get_text():text.set_color("#b91c1c")


def plot_curves(paths, output, plt, percent):
    fig,axes=plt.subplots(3,2,figsize=(14,11.5),layout="constrained")
    for r,period in enumerate(PERIODS):
        dates=max((paths[n,period]["dates"] for n in COUNTS),key=len)
        for n in COUNTS:
            p=paths[n,period];x=np.arange(len(p["dates"])+1)
            label=f"N={n} | "+("VALID" if p["valid"] else "indicative INVALID")
            for axis,values in ((axes[r,0],[1.,*p["nav"]]),(axes[r,1],[0.,*p["drawdown"]])):
                axis.plot(x,values,color=COLORS[n],ls="-" if p["valid"] else "--",lw=1.6,label=label)
                axis.scatter(x[-1],values[-1],s=44 if not p["valid"] else 21,marker="X" if not p["valid"] else "o",
                    color="#b91c1c" if not p["valid"] else COLORS[n],zorder=7)
                bad=np.flatnonzero(~np.asarray(p["accounting_valid"]))
                if len(bad):axis.scatter(bad+1,np.asarray(values)[bad+1],marker="x",color="#b91c1c",s=27,zorder=6)
            if p["upper_nav"] != p["nav"]:
                axes[r,0].fill_between(x[1:],p["nav"],p["upper_nav"],color=COLORS[n],alpha=.07)
        for c,title in enumerate(("Net accounting NAV","Conservative drawdown")):
            axis=axes[r,c];axis.set_title(("2026 to Oct 19 (partial year)" if period=="2026" else period)+" | "+title,fontsize=10)
            decorate_time(axis,dates);legend(axis)
        axes[r,0].axhline(1,color="#64748b",lw=.6);axes[r,0].set_ylabel("Cumulative net NAV; cash start = 1")
        axes[r,1].axhline(0,color="#64748b",lw=.6);axes[r,1].set_ylabel("Lower NAV / cumulative upper peak - 1")
        axes[r,1].yaxis.set_major_formatter(percent(1))
    fig.suptitle("A10 | Fixed existing H5 LGB, hold10 / cap2 / keep40 / 5-session cadence\n"
        "All 12 original paths and terminal observations retained; INVALID values are diagnostic, not qualified returns",fontsize=12)
    save_figure(fig,output,"position_count_nav_drawdown");plt.close(fig)


def plot_metrics(paths,initial,output,plt,percent):
    fig,axes=plt.subplots(3,4,figsize=(17,10.5),layout="constrained")
    titles=("Cumulative net return","Conservative max drawdown","One-way turnover / mean NAV","Recorded costs / initial cash")
    for r,period in enumerate(PERIODS):
        for x,n in enumerate(COUNTS):
            p=paths[n,period];m=p["summary"]["metrics"]
            bars=[axes[r,0].bar(x,float(number(m["total_return_lower_exact"])),color=COLORS[n])[0],
                  axes[r,1].bar(x,float(number(m["max_drawdown_conservative_exact"])),color=COLORS[n])[0],
                  axes[r,2].bar(x,p["turnover"],color=COLORS[n])[0]]
            bottom=0.
            for field,label,color in COSTS:
                value=float(number(p["costs"][field])/initial)
                bars.append(axes[r,3].bar(x,value,bottom=bottom,color=color,label=label if r==0 and x==0 else None)[0]);bottom+=value
            if not p["valid"]:
                for bar in bars:bar.set_hatch("///");bar.set_edgecolor("#b91c1c");bar.set_linewidth(1.)
        for c,title in enumerate(titles):
            axis=axes[r,c];axis.set_title(("2026 to Oct 19" if period=="2026" else period)+" | "+title,fontsize=9)
            axis.set_xticks(range(4),[f"N={n}\n"+("VALID" if paths[n,period]["valid"] else "INVALID") for n in COUNTS],fontsize=7)
            for tick,n in zip(axis.get_xticklabels(),COUNTS):
                if not paths[n,period]["valid"]:tick.set_color("#b91c1c");tick.set_fontweight("bold")
            axis.axhline(0,color="#64748b",lw=.6);axis.grid(axis="y",alpha=.17);axis.set_axisbelow(True)
            if c!=2:axis.yaxis.set_major_formatter(percent(1))
            else:axis.set_ylabel("Times; not annualized")
    axes[0,3].legend(fontsize=7,loc="best")
    fig.suptitle("Recorded after-cost diagnostics | Count order is predeclared, not a performance ranking\n"
        "Fees, adverse slippage and tax/rounding are already in NAV; no cost addback or hypothetical gross-return claim",fontsize=12)
    save_figure(fig,output,"position_count_return_dd_turnover_cost");plt.close(fig)


def plot_risk(paths,output,plt,percent):
    fig,axes=plt.subplots(3,4,figsize=(17,10.5),layout="constrained")
    for r,period in enumerate(PERIODS):
        dates=max((paths[n,period]["dates"] for n in COUNTS),key=len)
        for c,(field,title) in enumerate(RISK_FIELDS):
            axis=axes[r,c];unknown=[];undefined=[]
            for n in COUNTS:
                p=paths[n,period];values=[np.nan if v is None else v for v in p["risk_series"][field]]
                missing=sum(not known for known in p["risk_coverage_complete"])
                absent=sum(v is None and known for v,known in zip(p["risk_series"][field],p["risk_coverage_complete"]))
                if missing:unknown.append(f"N{n}:{missing}")
                if absent:undefined.append(f"N{n}:{absent}")
                axis.plot(np.arange(1,len(values)+1),values,color=COLORS[n],ls="-" if p["valid"] else "--",lw=1.25,
                    label=f"N={n}"+(" VALID" if p["valid"] else " indicative INVALID"))
            axis.set_title(("2026 to Oct 19" if period=="2026" else period)+" | "+title,fontsize=9);decorate_time(axis,dates,cash_anchor=False);legend(axis,location="upper right")
            # No artificial zero-risk cash-start observation is added here.
            if unknown:axis.text(.02,.03,"UNKNOWN days "+", ".join(unknown),transform=axis.transAxes,fontsize=7,color="#b91c1c",
                bbox={"facecolor":"#fff1f2","edgecolor":"#fca5a5","alpha":.9,"pad":2})
            if undefined:axis.text(.02,.13,"Undefined on cash-only days "+", ".join(undefined),transform=axis.transAxes,fontsize=6,color="#475569")
            if c!=2:axis.yaxis.set_major_formatter(percent(1))
    fig.suptitle("Actual exposure diagnostics | Physical + noncash claim upper values, divided by lower NAV\n"
        "Unknown coverage is a gap, never zero; cash is not a diversifying stock; original INVALID flags remain unchanged",fontsize=12)
    save_figure(fig,output,"position_count_concentration");plt.close(fig)


def analyze(report_path,output_dir):
    report_path,output=Path(report_path).resolve(),Path(output_dir).resolve()
    source=report_path.parent
    if output.exists() or output.is_relative_to(source) or source.is_relative_to(output):
        raise ValueError("Use a new output directory outside the immutable source experiment")
    implementation=Path(__file__).read_bytes();implementation_sha=hashlib.sha256(implementation).hexdigest()
    source,manifest,manifest_sha,report,paths,initial=load_report(report_path)
    report_sha=sha(report_path)
    os.environ.setdefault("MPLCONFIGDIR",str(Path(tempfile.gettempdir())/"stopat30m-a10-matplotlib"))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter
    plt.rcParams.update({"font.family":"DejaVu Sans","font.size":9,"axes.spines.top":False,"axes.spines.right":False,"svg.fonttype":"none"})
    output.mkdir(parents=True)
    try:
        plot_curves(paths,output,plt,PercentFormatter)
        plot_metrics(paths,initial,output,plt,PercentFormatter)
        plot_risk(paths,output,plt,PercentFormatter)
        plot_rows=[]
        for period in PERIODS:
            for n in COUNTS:
                p=paths[n,period]
                plot_rows.append({**p,"summary":None,"diagnostics":None,
                    "cash_start_anchor":{"date":None,"net_nav":1.,"upper_nav":1.,"drawdown":0.,"is_market_observation":False}})
        write(output/"plot-input.json",{"kind":"a10_frozen_plot_inputs","initial_cash_cny":str(initial),
            "counts":list(COUNTS),"periods":list(PERIODS),"paths":plot_rows,
            "original_decision":report["decision"],"validity_changed":False,"costs_added_back_to_nav":False})
        (output/"plot_source.py").write_bytes(implementation)
        for name in ("report.json","manifest.json","declaration.json","status.json"):
            (output/("input_"+name)).write_bytes((source/name).read_bytes())
        for name in manifest["files"]:
            if name.startswith("source/"):
                target=output/"input_source"/Path(name).relative_to("source")
                target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes((source/name).read_bytes())
        verify_inventory(source,manifest,manifest_sha)
        if sha(report_path)!=report_sha or sha(__file__)!=implementation_sha:
            raise ValueError("Original report or plot implementation changed during rendering")
        result={"kind":"a10_position_count_scientific_plots","status":"completed","created_at":datetime.now(timezone.utc).isoformat(),
            "source_report":{"path":str(report_path),"sha256":report_sha},"source_manifest":{"path":str(source/"manifest.json"),"sha256":manifest_sha},
            "plot_implementation_sha256":implementation_sha,"exact_paths":[f"top{n}/{period}" for period in PERIODS for n in COUNTS],
            "invalid_paths_preserved":[f"top{n}/{period}" for period in PERIODS for n in COUNTS if not paths[n,period]["valid"]],
            "original_curve_rows":sum(len(p["dates"]) for p in paths.values()),
            "unknown_risk_sessions":{f"top{n}/{period}":sum(not v for v in paths[n,period]["risk_coverage_complete"]) for period in PERIODS for n in COUNTS},
            "drawdown":"recorded lower NAV / running upper NAV peak - 1, including initial100000 cash",
            "returns":"cumulative, not annualized; no cost addback","unknown_concentration_imputed_zero":False,
            "source_experiment_changed":False,"new_fit_simulation_or_selection_performed":False,"current_paper_changed":False,
            "matplotlib_version":matplotlib.__version__,
            "files":{str(p.relative_to(output)):sha(p) for p in sorted(output.rglob("*")) if p.is_file()}}
        write(output/"manifest.json",result)
        return result
    except Exception as e:
        write(output/"failure.json",{"status":"failed","error_type":type(e).__name__,"error":str(e),"completed_plot_claim":False})
        raise


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report",type=Path,required=True)
    parser.add_argument("--output-dir",type=Path,required=True)
    args=parser.parse_args()
    result=analyze(args.report,args.output_dir)
    print(json.dumps({"output_dir":str(args.output_dir.resolve()),"invalid_paths_preserved":result["invalid_paths_preserved"]}))
