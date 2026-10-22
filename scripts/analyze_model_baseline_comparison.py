"""Read and reconcile completed A16 ledgers; never train, predict or trade."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import pandas as pd

from scripts import analyze_model_debug_comparison as audit

STRATEGIES = ("original", "regularized300", "MOM60", "REV20", "HASH1103", "HASH2207", "HASH3301", "HASH4409", "HASH5519")
PERIODS = {"primary": ("2023", "2024", "continuous"), "stability": ("2025", "2026", "continuous")}
COLORS = {"original":"#2563eb", "regularized300":"#7c3aed", "MOM60":"#e29018", "REV20":"#079871"}


def summarize_account(account, family):
    row = account["summary"]
    metrics = row["metrics"]
    dates, nav = account["dates"], account["nav"]
    returns = {}
    # Starting wealth for each bin is the preceding *recorded* close, or the
    # single inception cash anchor. Carry accounts never reset at year ends.
    prior = np.r_[1., nav[:-1]]
    for frequency, keys in (("years", dates.dt.strftime("%Y")),
                            ("quarters", dates.dt.to_period("Q").astype(str)),
                            ("months", dates.dt.strftime("%Y-%m"))):
        returns[frequency] = {}
        for key in sorted(set(keys)):
            indices = np.flatnonzero((keys == key).to_numpy())
            returns[frequency][key] = {"net_return":float(nav[indices[-1]]/prior[indices[0]]-1),
                "sessions":len(indices), "start_session":dates.iloc[indices[0]].strftime("%Y-%m-%d"),
                "end_session":dates.iloc[indices[-1]].strftime("%Y-%m-%d"),
                "whole_path_financially_valid":account["valid"],
                "diagnostic_only":not account["valid"]}
    return {"family":family,"period":row["period"],"strategy":row["candidate"],
        "financially_valid":account["valid"],"stopped":row["stopped"],
        "net_return":float(audit.number(metrics["total_return_lower_exact"])),
        "max_drawdown":-float(audit.number(metrics["max_drawdown_conservative_exact"])),
        "one_way_turnover":metrics["one_way_turnover_average_nav"],
        "mean_invested_fraction":metrics["mean_invested_fraction"],
        "fees_plus_embedded_slippage_cny":float(audit.number(metrics["total_fee_cny"])+audit.number(metrics["slippage_cost_cny"])),
        "distribution_tax_reserved_cny":float(audit.number(metrics.get("total_distribution_tax_reserved_cny", "0"))),
        "costs":account["costs"], "incomplete_valuation_days":metrics["incomplete_valuation_days"],
        "accounting_invalid_days":metrics["accounting_invalid_days"],
        "positive_month_fraction":metrics["positive_month_fraction"],
        "rolling_63_sessions":metrics["rolling_63_sessions"],
        "rolling_126_sessions":metrics["rolling_126_sessions"],"calendar_segments":returns,
        "net_already_contains_costs_no_subtraction_or_addback":True}


def analyze(directory, output):
    directory, output = Path(directory).resolve(), Path(output).resolve()
    if output.exists() or output == directory or output.is_relative_to(directory):
        raise ValueError("Preserve the entire sealed experiment and every earlier analysis")
    from stopat30m.ashare_stability_validation import guard_qlib_output_path
    output = guard_qlib_output_path(output)
    if "forward" in output.parts or "models" in output.parts:
        raise ValueError("Analysis cannot write into any paper account or model package")
    manifest, proofs = audit.inventory(directory, kind="a16_fixed_score_baseline_execution")
    report = audit.read(directory/"report.json")
    audit.assert_proof(report["execution_declaration"])
    if report["execution_declaration"] != manifest["execution_declaration"]:
        raise ValueError("The report and sealed inventory must share the same execution declaration")
    proofs[report["execution_declaration"]["path"]]=report["execution_declaration"]["sha256"]
    expected = {(f,p,s) for f,ps in PERIODS.items() for p in ps for s in STRATEGIES}
    rows = report["results"]
    if (len(rows)!=54 or {(r["family"],r["period"],r["strategy"]) for r in rows} != expected
            or report["active_paper_mutated_by_experiment"] is not False
            or report["automatic_model_promotion"] is not False
            or any(report[k]!=0 for k in ("new_native_fits","new_native_predictions","broker_orders"))):
        raise ValueError("All frozen paths and the no-mutation boundary must remain explicit")
    accounts, summaries = {}, []
    for row in rows:
        family,period,strategy = row["family"],row["period"],row["strategy"]
        path=directory/family/strategy/period
        summary=audit.read(path/"summary.json")
        if (Path(row["directory"])!=path or summary!={k:v for k,v in row.items() if k not in {"family","strategy","directory"}}
                or summary["candidate"] != strategy):
            raise ValueError("Saved account identity or summary differs from the sealed report")
        phase = "retrospective" if family=="stability" else "selection" if period=="2023" else "confirmation"
        account=audit.load_account(directory/family, summary, phase)
        accounts[family,period,strategy]=account
        summaries.append(summarize_account(account,family))
    if sum(r["financially_valid"] for r in summaries)!=report["financial_summary"]["financially_valid_paths"]:
        raise ValueError("Every financial invalidity must survive reporting")
    output.mkdir(parents=True)
    summary={"kind":"a16_reconciled_model_baseline_analysis","generated_at":datetime.now(timezone.utc).isoformat(),
        "source_manifest":audit.proof(directory/"manifest.json"),"source_report":audit.proof(directory/"report.json"),
        "paths":summaries,"financial_summary":report["financial_summary"],
        "ledger_reconciliation":"All54 exact decimal NAV, upper-peak DD, fee fields and turnover checked by unchanged A14 reconciler",
        "new_fits":0,"new_predictions":0,"live_paper_changed":False,
        "limitations":["Previously viewed historical out-of-time periods, not a new untouched test.",
            "Two source families have separate inception accounts and cannot be concatenated.",
            "Static HASH controls are persistent identity priorities, not turnover/risk/exposure matched nulls.",
            "Invalid paths are diagnostic only and excluded from win/loss comparisons.",
            "Costs already appear in NAV; listed fees/slippage/reserves are not subtracted or added back.",
            "Overlapping gross H5 labels and their block intervals are not net account returns or independent wins."]}
    audit.write(output/"summary.json",summary)
    pd.DataFrame([{k:v for k,v in row.items() if k not in {"costs","calendar_segments","rolling_63_sessions","rolling_126_sessions"}} for row in summaries]).to_csv(output/"accounts.csv",index=False)
    plot_accounts(accounts,output)
    plot_metrics(summaries,output)
    plot_signals(directory,output)
    plot_paired_signals(directory,output)
    bound={**proofs,str(Path(__file__).resolve()):audit.sha(__file__),str(Path(audit.__file__).resolve()):audit.sha(audit.__file__)}
    for path,digest in bound.items():
        if audit.sha(path)!=digest:raise ValueError("A sealed input changed during read-only analysis")
    audit.write(output/"manifest.json",{"kind":summary["kind"],"bound_inputs":bound,
        "files":{p.name:audit.sha(p) for p in sorted(output.iterdir()) if p.is_file()}})
    return summary


def pyplot():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.size":10,"axes.spines.top":False,"axes.spines.right":False})
    return plt


def save(fig,output,name):
    fig.savefig(output/(name+".png"),dpi=170,bbox_inches="tight")
    fig.savefig(output/(name+".svg"),bbox_inches="tight")


def plot_accounts(accounts,output):
    plt=pyplot()
    fig,axes=plt.subplots(3,2,figsize=(14,12))
    for col,(family,periods) in enumerate(PERIODS.items()):
        for row,period in enumerate(periods):
            ax=axes[row,col]
            for strategy in STRATEGIES:
                a=accounts[family,period,strategy]
                invalid=not a["valid"]
                color=COLORS.get(strategy,"#8a919c")
                label=strategy+(" [INVALID]" if invalid else "")
                ax.plot(a["dates"],a["nav"],color=color,ls="--" if invalid else "-",
                    lw=1.8 if strategy in COLORS else .8,alpha=.9 if strategy in COLORS else .55,label=label)
            title=("2023-2024 carry" if family=="primary" else "2025-2026 carry") if period=="continuous" else period
            ax.set_title(title+(" through Oct 19" if family=="stability" and period in {"2026","continuous"} else ""))
            ax.axhline(1,color="black",lw=.5);ax.set_ylabel("After-fee NAV / inception cash")
            ax.grid(alpha=.15);ax.tick_params(axis="x",labelrotation=25)
    handles,labels=axes[2,1].get_legend_handles_labels()
    fig.legend(handles,labels,loc="lower center",ncol=5,fontsize=8)
    fig.suptitle("A16: fixed20 hold10 / cap2, identical execution; dashed paths are financially INVALID")
    fig.tight_layout(rect=(0,.07,1,.97));save(fig,output,"costed_nav_all54");plt.close(fig)


def plot_metrics(rows,output):
    plt=pyplot()
    fig,axes=plt.subplots(3,2,figsize=(14,12))
    for col,(family,periods) in enumerate(PERIODS.items()):
        for n,period in enumerate(periods):
            ax=axes[n,col];part=[next(r for r in rows if (r["family"],r["period"],r["strategy"])==(family,period,s)) for s in STRATEGIES]
            x=np.arange(9)
            returns=np.asarray([r["net_return"]*100 for r in part]);dd=np.asarray([r["max_drawdown"]*100 for r in part])
            bars=ax.bar(x-.18,returns,width=.36,label="Net return",color="#3269ae")
            dbars=ax.bar(x+.18,-dd,width=.36,label="Conservative DD (negative)",color="#d08a55")
            for index,r in enumerate(part):
                if not r["financially_valid"]:
                    bars[index].set_hatch("///");dbars[index].set_hatch("///")
            ax.axhline(0,color="black",lw=.6);ax.set_xticks(x, [s.replace("regularized300","Reg300") for s in STRATEGIES],rotation=40,ha="right",fontsize=8)
            ax.set_title(f"{family}: {period}");ax.set_ylabel("Percent; hatched = INVALID");ax.grid(axis="y",alpha=.15)
    fig.legend(*axes[0,0].get_legend_handles_labels(),loc="lower center",ncol=2)
    fig.suptitle("Net return and conservative maximum drawdown; invalid paths are diagnostic only")
    fig.tight_layout(rect=(0,.04,1,.97));save(fig,output,"net_return_and_drawdown_all54");plt.close(fig)


def plot_signals(directory,output):
    plt=pyplot();fig,axes=plt.subplots(2,2,figsize=(13,8))
    for col,(family,periods) in enumerate(PERIODS.items()):
        summaries=json.loads((directory/family/"signal_diagnostics/summary.json").read_bytes())
        for row,period in enumerate(periods[:2]):
            ax=axes[row,col];x=np.arange(9)
            values=[];counts=[]
            for strategy in STRATEGIES:
                s=next(s for s in summaries if (s["period"],s["group"],s["strategy"])==(period,"union",strategy))
                value=s["metrics"]["top20_gross_label_excess"]["mean"]
                values.append(value*10000 if value is not None else np.nan)
                counts.append(s["complete_top20_days"])
            ax.bar(x,values,color=[COLORS.get(s,"#9da5b0") for s in STRATEGIES])
            for index,value in enumerate(values):
                if not np.isfinite(value):ax.text(index,0,"unknown",rotation=90,va="bottom",ha="center",fontsize=7)
            ax.axhline(0,color="black",lw=.7);ax.set_xticks(x,[s.replace("regularized300","Reg300")+f"\nn={n}" for s,n in zip(STRATEGIES,counts)],rotation=40,ha="right",fontsize=8)
            ax.set_title(period+(" through Oct 19" if period=="2026" else ""));ax.set_ylabel("Mean top20 minus pool H5 gross label (bp)");ax.grid(axis="y",alpha=.15)
    fig.suptitle("Gross H5 head excess, NOT net returns; own complete-date counts differ across strategies")
    fig.tight_layout(rect=(0,0,1,.96));save(fig,output,"top20_gross_signal_excess");plt.close(fig)


def plot_paired_signals(directory,output):
    plt=pyplot();fig,axes=plt.subplots(2,2,figsize=(13,9))
    controls=("MOM60","REV20","HASHmedian_all5_complete")
    pairs=[(model,control) for model in ("original","regularized300") for control in controls]
    for col,(family,periods) in enumerate(PERIODS.items()):
        rows=json.loads((directory/family/"signal_diagnostics/paired.json").read_bytes())
        for row,period in enumerate(periods[:2]):
            ax=axes[row,col];labels=[]
            for index,(model,control) in enumerate(pairs):
                record=next(r for r in rows if (r["period"],r["group"],r["frequency"],r["metric"],r["strategy"],r["reference_strategy"])==
                    (period,"union","period","top20_gross_label_excess",model,control))
                ci=record["paired_delta"];name=model.replace("regularized300","Reg300")+" - "+control.replace("HASHmedian_all5_complete","HASH median")
                labels.append(name+f" (n={record['common_complete_days']})")
                color=COLORS[model]
                if ci["mean"] is None:ax.text(0,index,"unknown",fontsize=8)
                else:
                    ax.plot(ci["mean"]*10000,index,"o",color=color)
                    if ci["lower"] is not None:
                        ax.hlines(index,ci["lower"]*10000,ci["upper"]*10000,color=color,lw=2)
                    else:ax.annotate("no CI",(ci["mean"]*10000,index),xytext=(5,3),textcoords="offset points",fontsize=7)
            ax.set_yticks(np.arange(len(pairs)),labels,fontsize=8);ax.invert_yaxis()
            ax.axvline(0,color="black",lw=.7);ax.set_title(period);ax.grid(axis="x",alpha=.15)
            ax.set_xlabel("Model minus control on identical complete dates (gross H5 bp)")
    fig.suptitle("Head signal differences: exploratory 95% / 20-session block intervals; not net NAV or independent trials")
    fig.tight_layout(rect=(0,0,1,.96));save(fig,output,"paired_top20_gross_differences");plt.close(fig)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory",type=Path,required=True);parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args();result=analyze(args.directory,args.output)
    print(json.dumps({"status":"completed","paths":len(result["paths"]),"output":str(args.output)}))


if __name__=="__main__":main()
