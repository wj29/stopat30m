"""Bound, exploratory A3 execution reviews; never overwrite the A2 paper model.

The observed 2025--2026 period is a history review, not a fresh model-selection
holdout. Development source/rule readiness is required before any selection.
"""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import hashlib
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

from stopat30m import ashare_execution_experiment as a2
from stopat30m.ashare_experiment import PackageHistoricalPool, VerifiedCorporateActions
from stopat30m.ashare_optimization_execution import (
    RankBufferPolicy, build_rank_buffer_plan, execute_rank_buffer_plan,
)
from stopat30m.ashare_schedule import verify_activation
from stopat30m.backtest import ashare_execution as ex
from stopat30m.data.ashare_execution_data import load_execution_collection
from stopat30m.data.qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot
from stopat30m.data.qlib_incremental import read_qlib_security
from stopat30m.model.evaluator import compute_portfolio_metrics


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)+"\n")


def load_review_context(baseline_dir, spec_path):
    """Reuse verified A2 market evidence without relabelling its provenance."""
    baseline, spec_path = Path(baseline_dir).resolve(), Path(spec_path).resolve()
    verified = verify_activation(baseline)
    spec = json.loads(spec_path.read_text())
    if (verified["manifest_sha256"] != spec["baseline_comparison"]["manifest_sha256"]
            or spec["history_review_is_fresh_oos"] is not False or spec["candidate_count"] != 6):
        raise ValueError("A3 protocol differs from its declared observed-history review")
    manifest = json.loads((baseline/"manifest.json").read_text())
    snapshot = load_qlib_snapshot(spec["source_snapshot"])
    if snapshot.manifest_sha256 != spec["source_manifest_sha256"]:
        raise ValueError("Optimization must retain the repaired evaluation source vintage")
    raw, basic, _ = load_execution_collection(Path(manifest["bound_inputs"]["execution_collection"]["path"]).parent)
    calendar = (snapshot.dataset_dir/"calendars/day.txt").read_text().splitlines()
    old_spec = json.loads((baseline/"report.json").read_text())["spec"]
    pool = PackageHistoricalPool(snapshot.dataset_dir/"instruments/csi300.txt", start=old_spec["train_start"], end=old_spec["oos_end"])
    prices = {inst:read_qlib_security(snapshot.dataset_dir, inst)["close"] for inst in sorted(raw.instrument.unique())}
    normalized = pd.DataFrame(prices).reindex(calendar)
    events = json.loads((baseline/"dividend_events.json").read_text())["events"]
    actions = VerifiedCorporateActions(manifest["bound_inputs"]["merger_actions"]["path"])
    return {"baseline":baseline, "spec":spec, "spec_path":spec_path,
        "raw":raw, "basic":basic, "calendar":calendar, "normalized":normalized,
        "events":events, "actions":actions, "pool":pool, "a2_spec":old_spec,
        "bound":{"spec":{"path":str(spec_path),"sha256":sha(spec_path)},
            "a2_manifest":{"path":str(baseline/"manifest.json"),"sha256":sha(baseline/"manifest.json")},
            "implementation":{"path":str(Path(__file__).resolve()),"sha256":sha(__file__)},
            "execution_adapter":{"path":str(Path(build_rank_buffer_plan.__code__.co_filename).resolve()),
                "sha256":sha(build_rank_buffer_plan.__code__.co_filename)}}}


def relevant_events(state, events, day, first_day):
    """Elide only economically empty rights; preserve all source evidence.

    Empty 0-share actions need no state cloning. This cannot remove an action
    affecting physical record-date shares, an outstanding right, or a residual.
    """
    selected=[]
    for row in events:
        if day not in {row["ex_date"],row["pay_date"],row["listing_date"]}:
            continue
        instrument, record = row["instrument"], row["record_date"]
        if record < first_day:
            continue  # The experiment starts with cash, no prior entitlements.
        if record not in state.holdings_history:
            raise ValueError("Required record-date holdings snapshot is missing; unknown ownership cannot be treated as zero")
        identity=row["event_id"]
        if (state.holdings_history.get(record,{}).get(instrument,0)
                or instrument in state.positions
                or identity+":cash" in state.entitlements or identity+":bonus" in state.entitlements
                or any(claim["instrument"]==instrument and claim["created_date"]<=record for claim in state.claims.values())):
            selected.append(row)
    return selected


def rank_at_close(context, scores, day):
    prices, actions = context["normalized"], context["actions"]
    members=context["pool"].members_on(day)
    eligible=[inst for inst in members if not actions.excluded(inst,day,close=True)
        and inst in prices and np.isfinite(prices.at[day,inst]) and prices.at[day,inst]>0]
    try:
        values=scores.xs(pd.Timestamp(day),level="datetime")
    except KeyError:
        raise ValueError("Required frozen previous-close scores are absent") from None
    ranked=[(inst,float(values.loc[inst])) for inst in eligible if inst in values.index and np.isfinite(values.loc[inst])]
    if not ranked:
        raise ValueError("No finite eligible previous-close model scores")
    return [inst for inst,_ in sorted(ranked,key=lambda row:(-row[1],row[0]))],members


def simulate_review(context, scores, *, case_id, execution_id, policy):
    if execution_id not in {"E0","E1","E2"}:
        raise ValueError("Undeclared execution policy")
    config,calendar=context["a2_spec"],context["calendar"]
    days=[day for day in calendar if config["oos_start"]<=day<=config["oos_end"]]
    # All held marks originate in this experiment; 80 genuine preceding
    # sessions retain every required age/stale/T+1 distinction. Source and
    # momentum windows still use the full unchanged source calendar.
    engine_calendar=calendar[max(0,calendar.index(days[0])-80):calendar.index(days[-1])+1]
    raw,basic,actions=context["raw"],context["basic"],context["actions"]
    symbols=sorted(set(raw.instrument)|{row["to_instrument"] for row in actions.actions})
    signal_raw=raw.loc[raw.source_complete_quote].set_index(["date","instrument"])["close"]
    state,previous=ex.PortfolioState.initial(policy),policy.initial_cash
    rows={name:[] for name in ("curves","trades","orders","positions","claims","signals","corporate_actions","plans")}
    for number,day in enumerate(days):
        location=calendar.index(day);signal_day=calendar[location-1]
        quotes,metadata,closes=a2._daily_inputs(raw,basic,day,symbols)
        suspended=[inst for inst in symbols if actions.suspended(inst,day)]
        events=relevant_events(state,context["events"],day,days[0])
        state,ledger=a2._apply_events(state,day,events,actions,engine_calendar,quotes,policy,days[0])
        rows["corporate_actions"].extend(ledger)
        dealt=[]
        if number % config["rebalance_sessions"]==0:
            if execution_id=="E0":
                weights,members=a2._targets("lightgbm",signal_day,location,context["normalized"],
                    context["pool"],scores,actions,config["top_k"])
                result=ex.execute_target_weights(state,session=day,signal_date=signal_day,targets=weights,
                    quotes=quotes,metadata=metadata,calendar=engine_calendar,policy=policy,verified_suspended=suspended)
                selected=set(weights)
            else:
                ranked,members=rank_at_close(context,scores,signal_day)
                source_prices={inst:signal_raw.loc[(signal_day,inst)] for inst in ranked if (signal_day,inst) in signal_raw.index}
                plan=build_rank_buffer_plan(state,signal_date=signal_day,ranked_instruments=ranked,
                    raw_signal_prices=source_prices,boards={inst:a2.board(inst) for inst in ranked},signal_equity=previous,
                    policy=RankBufferPolicy(top_k=config["top_k"],keep_rank=context["spec"]["rank_exit_threshold"],
                        require_signal_affordability=execution_id=="E2"),execution_policy=policy)
                result=execute_rank_buffer_plan(state,plan=plan,session=day,quotes=quotes,metadata=metadata,
                    calendar=engine_calendar,policy=policy,verified_suspended=suspended)
                rows["plans"].append({"session":day,**result.audit})
                selected=set(plan.selected)
            state,dealt=result.state,result.trades
            rows["trades"].extend(dealt);rows["orders"].extend(result.orders)
            rows["signals"].extend({"session":day,"signal_date":signal_day,"instrument":inst,"selected":inst in selected} for inst in members)
        valued=ex.mark_to_close(state,session=day,prices=closes,calendar=engine_calendar,policy=policy,verified_suspended=suspended)
        state,equity=valued.state,valued.equity_lower_bound
        if state.cash<0 or equity<=0:
            raise ValueError("Invalid optimized cash/share ledger")
        rows["curves"].append({"date":day,"equity_cny":str(equity),"cash_cny":str(state.cash),
            "equity_with_claim_estimate_cny":str(valued.equity_with_claim_estimate),
            "equity_upper_bound_cny":str(valued.equity_upper_bound),"valuation_complete":valued.valuation_complete,
            "nav":float(equity/policy.initial_cash),"net_return":float(equity/previous-1)})
        rows["positions"].extend(valued.positions)
        rows["claims"].extend({"session":day,**row} for row in valued.claims)
        previous=equity
    frame=pd.DataFrame(rows["curves"])
    metrics=compute_portfolio_metrics(frame.net_return)
    metrics.update(final_equity_cny=rows["curves"][-1]["equity_cny"],trade_count=len(rows["trades"]),
        incomplete_valuation_days=int((~frame.valuation_complete).sum()),
        mean_non_cash_equity_fraction=float(((frame.equity_cny.astype(float)-frame.cash_cny.astype(float))/frame.equity_cny.astype(float)).mean()),
        rejected_order_count=sum(row["status"]=="rejected" for row in rows["orders"]))
    for key in ("commission","transfer_fee","stamp_tax","total_fee","slippage_cost","notional"):
        metrics[key+"_cny"]=str(sum((Decimal(row[key]) for row in rows["trades"]),Decimal(0)))
    return {"case_id":case_id,"execution_id":execution_id,"metrics":metrics,"records":rows,
        "final_state":state.to_dict(),"history_review_is_fresh_oos":False}


def assert_baseline_reproduction(context, result):
    old=pd.read_csv(context["baseline"]/"curves.csv",dtype={"equity_cny":str,"cash_cny":str})
    old=old[old.strategy.eq("lightgbm")].reset_index(drop=True)
    actual=result["records"]["curves"]
    if len(old)!=len(actual) or any(row["date"]!=old.iloc[i].date
        or Decimal(row["equity_cny"])!=Decimal(old.iloc[i].equity_cny)
        or Decimal(row["cash_cny"])!=Decimal(old.iloc[i].cash_cny) for i,row in enumerate(actual)):
        raise ValueError("Optimized research harness does not reproduce original baseline daily cash/NAV")
    trades=pd.read_csv(context["baseline"]/"trades.csv",dtype=str)
    trades=trades[trades.strategy.eq("lightgbm")].reset_index(drop=True)
    fields=("quantity","execution_price","commission","transfer_fee","stamp_tax","cash_after","slippage_cost")
    if len(trades)!=len(result["records"]["trades"]):
        raise ValueError("Original baseline trade count changed")
    for i,row in enumerate(result["records"]["trades"]):
        if row["instrument"]!=trades.iloc[i].instrument or row["side"]!=trades.iloc[i].side or any(Decimal(row[key])!=Decimal(trades.iloc[i][key]) for key in fields):
            raise ValueError("Original baseline fill/cost/quantity changed")


def publish_review(context, output_dir, results, *, additional_inputs=None):
    output=guard_qlib_output_path(output_dir)
    if output.exists():
        raise ValueError("Preserve the existing A3 review")
    bound={**context["bound"],**(additional_inputs or {})}
    for proof in bound.values():
        if sha(proof["path"])!=proof["sha256"]:
            raise ValueError("A3 source or implementation changed during review")
    context["actions"].assert_unchanged()
    if "baseline" in context:
        verify_activation(context["baseline"])
    output.mkdir(parents=True)
    # Bind an archived implementation so later source edits do not erase the
    # exact program used for this completed, immutable historical review.
    for key in ("implementation", "execution_adapter"):
        if key in bound:
            original = Path(bound[key]["path"])
            archived = output/"source"/original.name
            archived.parent.mkdir(exist_ok=True)
            shutil.copyfile(original, archived)
            if sha(archived) != bound[key]["sha256"]:
                raise ValueError("Implementation changed while archiving")
            bound[key] = {**bound[key], "path":str(archived), "original_path":str(original)}
    summaries={}
    for result in results:
        case=output/result["case_id"];case.mkdir()
        for name,rows in result["records"].items():
            if name=="plans":save(case/"plans.json",rows)
            else:pd.DataFrame(rows).to_csv(case/(name+".csv"),index=False)
        save(case/"final_state.json",result["final_state"])
        summaries[result["case_id"]]={key:value for key,value in result.items() if key not in {"records","final_state"}}
    report={"schema_version":1,"kind":"a3_observed_history_review","history_review_is_fresh_oos":False,
        "period":context["spec"]["previously_observed_history_review"],"cases":summaries,
        "selection_status":"blocked_required_2023_2024_execution_data_and_historical_rules_not_verified",
        "selected_candidate":None,"active_paper_changed":False,"bound_inputs":bound}
    save(output/"report.json",report)
    save(output/"manifest.json",{"bound_inputs":bound,"files":{str(path.relative_to(output)):sha(path) for path in sorted(output.rglob('*')) if path.is_file()}})
    return report


def run_existing_model_review(baseline_dir, output_dir, *, spec_path, cost_ablation=False, progress=None):
    context=load_review_context(baseline_dir,spec_path)
    origin=Path(context["spec"]["models"][0]["training_origin"])
    origin_manifest = json.loads((origin/"manifest.json").read_text())
    prediction_proof={"path":str((origin/"predictions.csv").resolve()), "sha256":origin_manifest["files"]["predictions.csv"]}
    if sha(prediction_proof["path"]) != prediction_proof["sha256"]:
        raise ValueError("Frozen baseline predictions changed before reading")
    predictions=pd.read_csv(origin/"predictions.csv",parse_dates=["datetime"],float_precision="round_trip")
    scores=predictions.set_index(["datetime","instrument"])["score"]
    policy=a2.execution_policy(context["a2_spec"])
    cases=[("M0_E0","E0",policy)]
    if cost_ablation:
        cases.extend([("M0_E0_no_minimum","E0",replace(policy,minimum_commission=Decimal(0))),
            ("M0_E0_no_slippage","E0",replace(policy,slippage_bps=Decimal(0))),
            ("M0_E0_zero_execution_friction","E0",replace(policy,commission_rate=Decimal(0),minimum_commission=Decimal(0),transfer_rate=Decimal(0),sell_stamp_rate=Decimal(0),slippage_bps=Decimal(0)))])
    else:
        cases.extend((f"M0_{execution}",execution,policy) for execution in ("E1","E2"))
    results=[]
    for case,execution,fees in cases:
        if progress:progress("Starting "+case)
        result=simulate_review(context,scores,case_id=case,execution_id=execution,policy=fees)
        if case=="M0_E0":assert_baseline_reproduction(context,result)
        if progress:progress({"case":case,**result["metrics"]})
        results.append(result)
    return publish_review(context,output_dir,results,additional_inputs={"baseline_predictions":prediction_proof})


def rolling_scores_for_calendar(predictions, calendar, *, model="M1"):
    """Choose each signal's fold by the next real execution session's year."""
    if predictions.duplicated(["model", "execution_year", "datetime", "instrument"]).any():
        raise ValueError("Duplicate frozen rolling prediction keys")
    dates=pd.DatetimeIndex(pd.to_datetime(calendar))
    if dates.has_duplicates or not dates.is_monotonic_increasing:
        raise ValueError("Rolling prediction routing requires a unique actual-session calendar")
    next_year={day:dates[i+1].year for i,day in enumerate(dates[:-1])}
    frame=predictions.loc[predictions.model.eq(model)].copy()
    frame["datetime"]=pd.to_datetime(frame.datetime)
    frame=frame.loc[frame.datetime.map(next_year).eq(frame.execution_year)]
    if frame.empty or frame.duplicated(["datetime", "instrument"]).any():
        raise ValueError("Rolling execution-year routing did not produce unique scores")
    return frame.set_index(["datetime","instrument"])["score"].sort_index()


def run_rolling_model_review(baseline_dir, predictions_dir, output_dir, *, spec_path, progress=None):
    """Review the single declared rolling model with the same three executions."""
    context=load_review_context(baseline_dir,spec_path)
    root=Path(predictions_dir).resolve()
    manifest_path=root/"manifest.json"
    manifest_proof={"path":str(manifest_path),"sha256":sha(manifest_path)}
    manifest=json.loads(manifest_path.read_text())
    if (manifest["declaration_sha256"] != context["bound"]["spec"]["sha256"]
            or manifest["source_manifest_sha256"] != context["spec"]["source_manifest_sha256"]
            or manifest["hyperparameter_search"] is not False or manifest["early_stopping"] is not False
            or manifest["selection_performed"] is not False or manifest["active_paper_changed"] is not False
            or manifest["baseline_reproduction"]["max_absolute_score_error"] != 0
            or manifest["baseline_reproduction"]["overlap_rows"] != manifest["baseline_reproduction"]["original_rows"]):
        raise ValueError("Rolling research source/declaration/baseline reproduction differs")
    for name,digest in manifest["files"].items():
        relative=Path(name)
        if relative.is_absolute() or ".." in relative.parts or sha(root/relative) != digest:
            raise ValueError("Frozen rolling research artifact changed")
    prediction_proof={"path":str(root/"predictions.csv"),"sha256":manifest["files"]["predictions.csv"]}
    frame=pd.read_csv(root/"predictions.csv",float_precision="round_trip")
    scores=rolling_scores_for_calendar(frame,context["calendar"])
    policy=a2.execution_policy(context["a2_spec"])
    results=[]
    for execution in ("E0","E1","E2"):
        case="M1_"+execution
        if progress:progress("Starting "+case)
        result=simulate_review(context,scores,case_id=case,execution_id=execution,policy=policy)
        if progress:progress({"case":case,**result["metrics"]})
        results.append(result)
    return publish_review(context,output_dir,results,additional_inputs={
        "rolling_manifest":manifest_proof,"rolling_predictions":prediction_proof})
