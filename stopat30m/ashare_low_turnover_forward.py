"""Prospective A6 hold10/cap2 paper account; no refit or retrospective fills."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, time
import json
from pathlib import Path
import shutil
import tempfile

import pandas as pd

from stopat30m import ashare_forward as legacy
from stopat30m import ashare_stability_rank_execution as adapter
from stopat30m.ashare_turnover_policy import build_turnover_plan
from stopat30m.ashare_turnover_validation import HoldingAgeTracker, _exit_audit
from stopat30m.backtest import ashare_execution as execution
from stopat30m.data.research_calendar import Calendar

KIND = "isolated_ashare_low_turnover_forward"
POLICY = "a9-prospective-frozen-a5-H5-hold10-cap2-v1"
STRATEGY = {"key": "hold10_cap2", "minimum_hold_sessions": 10,
    "max_replacements": 2, "rebalance_sessions": 5, "top_k": 20,
    "keep_rank": 40, "require_signal_affordability": True}


def _proof(path):
    return {"path": str(Path(path).resolve()), "sha256": legacy._sha(path)}


def implementation_proofs():
    from stopat30m import ashare_low_turnover_candidate, ashare_low_turnover_provider
    from stopat30m import ashare_optimization_execution, ashare_paper_provider
    from stopat30m import ashare_low_turnover_recovery, ashare_paper_report, ashare_low_turnover_recovery_close
    from stopat30m.data import qlib_factor_revision, qlib_continuation
    from stopat30m.data import (ashare_daily_update, ashare_live, ashare_actions,
        qlib_incremental, qlib_snapshot, baostock_market_validated, baostock_validated,
        baostock_paced, baostock_bounded, ashare_daily, ashare_execution_data, research_calendar)
    return {str(Path(path).resolve()): legacy._sha(path) for path in
        (__file__, legacy.__file__, adapter.__file__, execution.__file__,
         build_turnover_plan.__code__.co_filename, HoldingAgeTracker.after_trades.__code__.co_filename,
         ashare_low_turnover_candidate.__file__, ashare_low_turnover_provider.__file__,
         ashare_optimization_execution.__file__, ashare_paper_provider.__file__,
         ashare_daily_update.__file__, ashare_live.__file__, ashare_actions.__file__,
         qlib_incremental.__file__, qlib_snapshot.__file__, baostock_market_validated.__file__,
         baostock_validated.__file__, baostock_paced.__file__, baostock_bounded.__file__,
         ashare_daily.__file__, ashare_execution_data.__file__, research_calendar.__file__,
         ashare_low_turnover_recovery.__file__, ashare_paper_report.__file__,
         ashare_low_turnover_recovery_close.__file__, qlib_factor_revision.__file__, qlib_continuation.__file__)}


def _candidate(manifest):
    from stopat30m.ashare_low_turnover_candidate import load_candidate
    return load_candidate(**manifest["candidate_source"])


def initialize(paper_dir, *, models_directory, protocol_path, market_audit_path,
               calendar_path, universe_path, adapter_config, now=None):
    """New cash-only observation, never a reset or import of an old account."""
    root = Path(paper_dir).resolve()
    if root.exists():
        raise FileExistsError("Preserve all old paper periods")
    clock = legacy._instant(now)
    calendar = Calendar.load(calendar_path)
    first = calendar.next_session(clock.astimezone(legacy.SHANGHAI).date().isoformat())
    source = {"models_directory": str(Path(models_directory).resolve()),
        "protocol_path": str(Path(protocol_path).resolve()),
        "market_audit_path": str(Path(market_audit_path).resolve())}
    candidate = _candidate({"candidate_source": source})
    if legacy._instant(datetime.fromisoformat(candidate.protocol["original_fitted_at"])) > clock:
        raise ValueError("Paper initialization cannot precede actual model fitting")
    universe = json.loads(Path(universe_path).read_bytes())
    if universe.get('frozen_at') and legacy._instant(datetime.fromisoformat(universe['frozen_at'])) > clock:
        raise ValueError("Watchlist was not known at the paper initialization clock")
    pool = universe["instruments"]
    if (len(pool) != 1300 or len(set(pool)) != 1300
            or pool != sorted(pool) or universe["source_date"] > clock.astimezone(legacy.SHANGHAI).date().isoformat()):
        raise ValueError("A separately evidenced fixed prospective 300+1000 universe is required")
    evidence = legacy._evidence(universe["evidence"])
    policy = execution.ExecutionPolicy()
    config = {**adapter_config, "calendar_path": str((root/"calendar.json").resolve()),
        "universe_path": str((root/"universe.json").resolve())}
    if config.get("stock_basic_path"):
        config["stock_basic_sha256"] = legacy._sha(config["stock_basic_path"])
    if config.get("initial_snapshot_path"):
        config["initial_snapshot_manifest_sha256"] = legacy._sha(Path(config["initial_snapshot_path"])/"manifest.json")
    manifest = {"schema_version": 1, "kind": KIND, "policy": POLICY,
        "created_at": clock.isoformat(), "first_possible_execution": first,
        "initial_universe": pool, "universe_evidence": evidence,
        "universe_scope": "fixed_official_1300_anchor_for_prospective_observation_not_daily_current_membership",
        "universe_source_date": universe["source_date"], "min_eligible": 20,
        "initial_capital_per_strategy": str(policy.initial_cash), "strategies": ["model"],
        "strategy": STRATEGY, "execution_policy": policy.to_dict(),
        "execution_source": "tencent.public_level1_observation", "max_quote_age_seconds": 60,
        "open_window": ["09:30", "09:45"], "daily_attempt_limit": 3,
        "candidate_source": source, "candidate_manifest_sha256": candidate.manifest["manifest_sha256"],
        "model_sha256": candidate.model_sha256, "candidate_proofs": candidate.proofs(),
        "implementation_sha256": implementation_proofs(),
        "calendar_sha256": legacy._sha(calendar_path), "universe_sha256": legacy._sha(universe_path),
        "adapter_sha256": legacy._digest(config),
        "rebalance_anchor_policy": "next_actual_session_after_first_complete_postfreeze_signal_never_search_or_reset",
        "historical_activation_claim": False, "new_historical_oos_claim": False,
        "user_authorized_forward_observation_despite_unresolved_historical_claim": True,
        "execution_ready": False, "limitations": [
            "New future account; original A6/A8 invalid paths stay invalid and no old holdings or returns are imported.",
            "The existing 2026 H5 native model is reused, not retrained with latest data.",
            "Frozen official 1300-member anchor is a disclosed pilot pool, not a daily refreshed membership claim.",
            "Current observed execution quotes, integer shares and actual fees are simulated, never broker orders.",
            "Missing held prices/actions or observed-session gaps block orders; no fictitious cash recovery."]}
    manifest["manifest_sha256"] = legacy._digest(manifest)
    state = {"schema_version": 1, "manifest_sha256": manifest["manifest_sha256"],
        "accounts": {"model": execution.PortfolioState.initial(policy).to_dict()},
        "signals": {}, "completed": {}, "attempts": {}, "receipts": [], "valuations": [],
        "current_universe": pool, "corporate_actions": {}, "observation_gaps": {},
        "unresolved_actions": {"model": {}}, "holding_clock": {"holding_started": {}, "record_origins": {}, "audit": []},
        "rebalance_anchor": None, "last_success": None, "last_failure": None}
    root.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".low-turnover-", dir=root.parent) as temporary:
        stage = Path(temporary)/"paper"; stage.mkdir()
        shutil.copyfile(calendar_path, stage/"calendar.json")
        shutil.copyfile(universe_path, stage/"universe.json")
        legacy._write(stage/"manifest.json", manifest)
        legacy._write(stage/"adapter.json", config)
        legacy._save_state(stage, state)
        stage.rename(root)
    return low_turnover_status(root)


def _read_paper(root):
    root = Path(root).resolve()
    manifest = json.loads((root/"manifest.json").read_bytes())
    state = json.loads((root/"state.json").read_bytes())
    from .ashare_low_turnover_recovery import verify_recovery_binding
    verify_recovery_binding(root, manifest, state)
    if (manifest.get("kind") != KIND or manifest.get("policy") != POLICY
            or manifest.get("strategy") != STRATEGY
            or manifest.get("manifest_sha256") != legacy._digest({k:v for k,v in manifest.items() if k != "manifest_sha256"})
            or state.get("manifest_sha256") != manifest["manifest_sha256"]
            or state.get("state_sha256") != legacy._digest({k:v for k,v in state.items() if k != "state_sha256"})):
        raise ValueError("Low-turnover paper declaration/state changed")
    if (legacy._sha(root/"calendar.json") != manifest["calendar_sha256"]
            or legacy._sha(root/"universe.json") != manifest["universe_sha256"]
            or legacy._digest(json.loads((root/"adapter.json").read_bytes())) != manifest["adapter_sha256"]):
        raise ValueError("Frozen calendar, universe or provider configuration changed")
    for path, digest in manifest["implementation_sha256"].items():
        if legacy._sha(path) != digest:
            raise ValueError("Explicit new period required after execution/source code changes: "+path)
    legacy._evidence(manifest["universe_evidence"])
    for proof in manifest["candidate_proofs"].values():
        if legacy._sha(proof["path"]) != proof["sha256"]:
            raise ValueError("Frozen candidate provenance changed")
    for receipt in state["receipts"]:
        if legacy._sha(receipt["path"]) != receipt["sha256"]:
            raise ValueError("Committed paper receipt changed")
    candidate = _candidate(manifest)
    if candidate.model_sha256 != manifest["model_sha256"] or candidate.manifest["manifest_sha256"] != manifest["candidate_manifest_sha256"]:
        raise ValueError("A different model cannot enter a live paper period")
    for account in state["accounts"].values():
        execution.PortfolioState.from_dict(account)
    return root, manifest, state, candidate, Calendar.load(root/"calendar.json")


def low_turnover_status(paper_dir):
    root, manifest, state, candidate, calendar = _read_paper(paper_dir)
    return {"path": str(root), "manifest": manifest, "state": state,
        "model_sha256": candidate.model_sha256, "calendar_source": calendar.source,
        "live_order_count": 0, "real_account_accessed": False}


def _tracker(state, days):
    tracker = HoldingAgeTracker(days)
    for key in ("holding_started", "record_origins", "audit"):
        setattr(tracker, key, deepcopy(state["holding_clock"][key]))
    return tracker


def _save_tracker(state, tracker):
    state["holding_clock"] = {key: deepcopy(getattr(tracker,key))
        for key in ("holding_started", "record_origins", "audit")}


def _guard_actions(state, batch, manifest=None):
    for event in [*state.get("corporate_actions",{}).values(), *batch.get("corporate_actions", [])]:
        inst=event.get("instrument"); record_date=event.get("record_date")
        if execution.decimal(event.get("bonus_ratio",0)) <= 0:
            continue
        if manifest and record_date and record_date < legacy._instant(datetime.fromisoformat(manifest["created_at"])).astimezone(legacy.SHANGHAI).date().isoformat():
            continue
        for saved in state["accounts"].values():
            if inst not in legacy._account_instruments(saved):
                continue
            account=execution.PortfolioState.from_dict(saved)
            recorded=account.holdings_history.get(record_date)
            existing=any(key in account.entitlements for key in (str(event.get("event_id"))+":cash",str(event.get("event_id"))+":bonus"))
            prior_claim=any(row.get("instrument")==inst and row.get("created_date",'9999') <= (record_date or '') for row in account.claims.values())
            if recorded is not None and recorded.get(inst,0)==0 and not existing and not prior_claim:
                continue
            raise ValueError("Held noncash distribution requires a separate verified capital-tax/delivery adapter; orders remain blocked")


def _scope_actions(state,batch,manifest):
    """Do not ask the old action book to price a proven zero entitlement.

    Original provider observations remain archived unmodified. Unknown record
    holdings never count as zero; existing rights and claims always remain.
    """
    scoped=deepcopy(batch); kept=[]; ignored=[]
    start=legacy._instant(datetime.fromisoformat(manifest["created_at"])).astimezone(legacy.SHANGHAI).date().isoformat()
    for event in batch.get("corporate_actions",[]):
        record=event.get("record_date"); inst=event.get("instrument"); identity=event.get("event_id")
        zero=bool(record and record<start)
        if not zero and record and identity:
            zero=True
            for saved in state['accounts'].values():
                account=execution.PortfolioState.from_dict(saved)
                snapshot=account.holdings_history.get(record)
                existing=any(key in account.entitlements for key in (identity+':cash',identity+':bonus'))
                claims=any(row.get('instrument')==inst and row.get('created_date','9999')<=record for row in account.claims.values())
                if snapshot is None or snapshot.get(inst,0) or existing or claims:
                    zero=False; break
        if zero:
            ignored.append({'event_id':identity,'instrument':inst,'record_date':record,
                'reason':'no_preinitialization_or_explicit_zero_record_date_entitlement'})
        else:
            kept.append(event)
    scoped['corporate_actions']=kept
    return scoped,ignored


def _plan(account,signal,days,tracker,policy,session):
    return build_turnover_plan(account,signal_date=signal["signal_date"],session=session,
        ranked_instruments=signal["ranked_instruments"],raw_signal_prices=signal["raw_signal_prices"],
        boards=signal["boards"],signal_equity=signal["signal_equity"],calendar=days,
        holding_started=tracker.holding_started,minimum_hold_sessions=10,max_replacements=2,
        execution_policy=policy,base_policy=adapter.RankBufferPolicy(top_k=20,keep_rank=40,require_signal_affordability=True))


def _close_operation(root, manifest, state, candidate, calendar, batch, clock):
    batch,ignored=_scope_actions(state,batch,manifest)
    _guard_actions(state, batch, manifest)
    days = calendar.sessions(calendar.coverage_start, calendar.coverage_end)
    tracker = _tracker(state, days)
    before = execution.PortfolioState.from_dict(state["accounts"]["model"])
    runtime = deepcopy(manifest)
    runtime["first_possible_execution"] = state["rebalance_anchor"] or calendar.next_session(batch["session"])
    operation = legacy._close_operation
    if "engineering_recovery" in state:
        from .ashare_low_turnover_recovery_close import close_after_explicit_recovery
        operation = close_after_explicit_recovery
    receipt = operation(root, runtime, state, candidate, calendar, batch, clock)
    after = execution.PortfolioState.from_dict(state["accounts"]["model"])
    tracker.after_corporate_actions(before, after, receipt["corporate_actions"]["model"], batch["session"])
    positions = tracker.close(after, batch["session"])
    signal = state["signals"][batch["session"]]
    if receipt["status"] == "completed" and state["rebalance_anchor"] is None:
        state["rebalance_anchor"] = signal["execution_session"]
    scores = signal["diagnostics"].get("model_scores", {})
    signal["ranked_instruments"] = sorted(scores, key=lambda inst:(-scores[inst],inst))
    signal["raw_signal_prices"] = {inst:str(execution.decimal(batch["quotes"][inst]["close"])) for inst in scores}
    signal["boards"] = {inst:batch["metadata"][inst]["board"] for inst in scores}
    signal["signal_equity"] = str(receipt["valuations"]["model"]["equity_lower_bound"])
    signal["targets"] = {"model": signal["targets"].get("model",{})}
    signal["strategy"] = STRATEGY
    signal["signal_sha256"] = legacy._digest({k:v for k,v in signal.items() if k != "signal_sha256"})
    receipt.update(signal_sha256=signal["signal_sha256"], holding_ages=positions,
        rebalance_anchor=state["rebalance_anchor"],ignored_no_entitlement_actions=ignored)
    _save_tracker(state, tracker)
    from .ashare_low_turnover_recovery import activate_after_close
    activate_after_close(state,receipt=receipt,signal=signal,batch=batch,now=clock)
    return receipt


def _live_quotes(manifest, batch, clock):
    quotes, rejected = {}, {}
    for inst,row in batch["quotes"].items():
        try:
            stamp=legacy._instant(datetime.fromisoformat(row.get("as_of",row.get("asof"))))
            fetched=legacy._instant(datetime.fromisoformat(row.get("fetched_at",batch["fetched_at"])))
            if (row.get("source") != manifest["execution_source"] or row.get("session") != batch["session"]
                    or row.get("execution_price_basis") != "live_observed_quote"
                    or stamp > clock or fetched > clock
                    or stamp.astimezone(legacy.SHANGHAI).date().isoformat() != batch["session"]
                    or max((clock-stamp).total_seconds(),(clock-fetched).total_seconds()) > manifest["max_quote_age_seconds"]):
                raise ValueError("Invalid current observation")
            quotes[inst] = execution.OpeningQuote(session=batch["session"],open=row.get("open"),
                preclose=row.get("preclose"),tradestatus=legacy._status(row.get("tradestatus")),
                source=row["source"],execution_price=row.get("execution_price",row.get("latest")),
                execution_price_basis="live_observed_quote",asof=stamp.isoformat())
        except (ValueError,TypeError,KeyError):
            rejected[inst]="missing_stale_or_invalid_live_quote"
    return quotes,rejected


def _open_operation(manifest, state, calendar, batch, clock):
    batch,ignored=_scope_actions(state,batch,manifest)
    session=batch["session"]; prior=calendar.previous_session(session)
    signal=state["signals"].get(prior)
    if (not signal or signal["execution_session"] != session or signal["strategy"] != STRATEGY
            or signal["model_sha256"] != manifest["model_sha256"]
            or signal["model_manifest_sha256"] != manifest["candidate_manifest_sha256"]
            or signal["signal_sha256"] != legacy._digest({k:v for k,v in signal.items() if k != "signal_sha256"})
            or legacy._instant(datetime.fromisoformat(signal["created_at"])) >= datetime.combine(pd.Timestamp(session).date(),time(9,30),legacy.SHANGHAI)):
        raise ValueError("A genuine preceding-close frozen low-turnover signal is required")
    _guard_actions(state,batch,manifest)
    days=calendar.sessions(calendar.coverage_start,calendar.coverage_end)
    tracker=_tracker(state,days)
    before=execution.PortfolioState.from_dict(state["accounts"]["model"])
    events=legacy._action_book(state,batch,clock)
    policy=execution.ExecutionPolicy(**manifest["execution_policy"])
    account,action_records=legacy._apply_actions(before,events,manifest=manifest,session=session,calendar=days,clock=clock)
    tracker.after_corporate_actions(before,account,action_records,session)
    metadata,rejected=legacy._security_metadata(batch["metadata"],session,clock)
    quotes,quote_rejected=_live_quotes(manifest,batch,clock); rejected.update(quote_rejected)
    if not quotes:
        raise ValueError("No current live execution quote; no historical opening may substitute")
    for inst,position in account.positions.items():
        quote=quotes.get(inst)
        if (quote and quote.preclose is not None and position.last_mark is not None
                and position.last_mark_date == prior
                and not legacy._verified_action_reference(account,inst,session,quote.preclose,events)
                and abs(execution.decimal(quote.preclose)-position.last_mark) >= execution.decimal('.01')):
            raise ValueError("Unresolved held exchange reference: "+inst)
    plan,audit=_plan(account,signal,days,tracker,policy,session)
    suspended=[inst for inst,row in batch["quotes"].items() if legacy._status(row.get("tradestatus")) == "suspended"
        and row.get("session") == session and inst in metadata]
    result=adapter.execute_rank_buffer_plan(account,plan=plan,session=session,quotes=quotes,
        metadata=metadata,calendar=days,policy=policy,now=clock,verified_suspended=suspended)
    tracker.after_trades(account,result.state,result.trades,session)
    state["accounts"]["model"]=result.state.to_dict(); _save_tracker(state,tracker)
    return {"status":"completed","phase":"open","session":session,"signal_date":prior,
        "signal_sha256":signal["signal_sha256"],"live_order":False,"source_rejections":rejected,
        "ignored_no_entitlement_actions":ignored,
        "accounts":{"model":{"status":"completed","orders":result.orders,"trades":result.trades,
            "opening_equity":str(result.opening_equity),"corporate_actions":action_records,
            "turnover_audit":_exit_audit(plan,result,audit),"execution_audit":result.audit}}}


def _record_observation_gaps(state, manifest, calendar, session, clock):
    # Before the first actual observation there is no observed return period or
    # position. Failed bootstrap attempts must not invent cash-return days.
    if not state["valuations"]:
        return False
    runtime={**manifest,"created_at":min(row["observed_at"] for row in state["valuations"])}
    return legacy._record_observation_gaps(state,runtime,calendar,session,clock)


def run_low_turnover(paper_dir, *, phase, provider, now=None):
    """One bounded phase. Provider methods are injected; this module has no HTTP.

    ``fetch_close(session,instruments,now)`` returns a JSON batch with current
    source raw close/metadata, immutable Qlib snapshot and actual evidence SHA.
    ``fetch_open`` returns current live observed prices, never a daily-bar replay.
    Closed sessions and non-rebalance opens never invoke either provider.
    """
    if phase not in {"open", "close"}:
        raise ValueError("A-share paper phase must be open or close")
    root = Path(paper_dir).resolve()
    with legacy._lock(root):
        root, manifest, state, candidate, calendar = _read_paper(root)
        clock = legacy._instant(now)
        local = clock.astimezone(legacy.SHANGHAI)
        session = local.date().isoformat()
        if not calendar.is_session(session):
            return {"status": "skipped_non_session", "session": session, "phase": phase, "network_called": False}
        if _record_observation_gaps(state, manifest, calendar, session, clock):
            legacy._save_state(root, state)
        key = session + "_" + phase
        if key in state["completed"]:
            return {**state["completed"][key], "replayed": True, "network_called": False}
        if session < manifest["created_at"][:10] or clock < legacy._instant(datetime.fromisoformat(manifest["created_at"])):
            raise ValueError("Paper cannot backfill dates before its initialization")
        if phase == "open" and not time(9, 30) <= local.time() <= time(9, 45):
            receipt = {"status": "expired_no_fill" if local.time() > time(9, 45) else "not_open_window",
                "session": session, "phase": phase, "recorded_at": clock.isoformat(), "network_called": False,
                "reason": "Only fresh observed prices within 09:30..09:45 authorize execution"}
            if local.time() > time(9, 45):
                state["completed"][key] = receipt
                legacy._save_state(root, state)
            return receipt

        if phase == "close" and local.time() < time(15, 5):
            return {"status": "not_completed_session", "session": session, "phase": phase, "network_called": False}
        from .ashare_low_turnover_recovery import pending_gaps
        if phase == "open" and pending_gaps(state):
            receipt = {"status": "blocked_observation_gap_no_fill", "session": session, "phase": phase,
                "recorded_at": clock.isoformat(), "network_called": False,
                "observation_gaps": state["observation_gaps"], "live_order": False}
            state["completed"][key] = receipt
            state["last_failure"] = receipt
            legacy._save_state(root, state)
            return receipt
        prior = calendar.previous_session(session)
        signal = state["signals"].get(prior) if phase == "open" else None
        if phase == "open" and signal is None:
            return {"status": "no_frozen_previous_signal", "session": session, "phase": phase, "network_called": False}
        if phase == "open" and (not signal["rebalance"] or not any(signal["allowed"].values())):
            receipt = {"status": "skipped_non_rebalance" if not signal["rebalance"] else "blocked_signal_no_fill",
                "session": session, "phase": phase, "recorded_at": clock.isoformat(), "network_called": False}
            state["completed"][key] = receipt
            legacy._save_state(root, state)
            return receipt
        attempted = state["attempts"].get(key, 0)
        if attempted >= manifest["daily_attempt_limit"]:
            return {"status": "attempt_limit", "session": session, "phase": phase, "network_called": False}
        attempt = attempted + 1
        state["attempts"][key] = attempt
        legacy._save_state(root, state)  # survives a killed provider without altering ledgers
        working = json.loads(json.dumps(state))
        rights = set().union(*(legacy._account_instruments(account) for account in state["accounts"].values()))
        needed = sorted(set(state["current_universe"]) | rights)
        if phase == "open":
            days=calendar.sessions(calendar.coverage_start,calendar.coverage_end)
            prior_account=execution.PortfolioState.from_dict(state["accounts"]["model"])
            prior_plan,_=_plan(prior_account,signal,days,_tracker(state,days),
                execution.ExecutionPolicy(**manifest["execution_policy"]),session)
            needed=sorted({inst for inst, *_ in prior_plan.entries}|rights)
        receipt_path = root / "receipts" / f"{key}_{attempt}.json"
        try:
            batch = getattr(provider, "fetch_" + phase)(session, needed, clock)
            clock = legacy._instant(now)
            legacy._batch(batch, phase, session, clock, manifest)
            if phase == "open" and not time(9, 30) <= clock.astimezone(legacy.SHANGHAI).time() <= time(9, 45):
                raise ValueError("Quote collection finished after the allowed live execution window")
            observation = root / "observations" / f"{key}_{attempt}.json"
            legacy._write(observation, batch)
            if phase == "close":
                receipt = _close_operation(root, manifest, working, candidate, calendar, batch, clock)
            else:
                receipt = _open_operation(manifest, working, calendar, batch, clock)
            receipt.update({"attempt": attempt, "recorded_at": clock.isoformat(),
                "observation": str(observation), "observation_sha256": legacy._sha(observation), "network_called": True})
            legacy._write(receipt_path, receipt)
            if receipt["status"] == "completed":
                working["completed"][key] = {**receipt, "receipt_path": str(receipt_path), "receipt_sha256": legacy._sha(receipt_path)}
            working["receipts"].append({"path": str(receipt_path), "sha256": legacy._sha(receipt_path)})
            if receipt["status"] == "completed":
                working["last_success"] = {"session": session, "phase": phase, "recorded_at": clock.isoformat()}
                working["last_failure"] = None
            else:
                working["last_failure"] = {"status": "partial", "session": session, "phase": phase,
                    "warnings": receipt.get("warnings", []), "receipt_path": str(receipt_path),
                    "receipt_sha256": legacy._sha(receipt_path), "recorded_at": clock.isoformat()}
            legacy._save_state(root, working)
            return receipt
        except Exception as exc:
            receipt = {"status": "failed", "session": session, "phase": phase, "attempt": attempt,
                "recorded_at": legacy._instant(now).isoformat(), "error_type": type(exc).__name__,
                "error": str(exc)[:500], "network_called": True, "live_order": False}
            legacy._write(receipt_path, receipt)
            state["last_failure"] = {**receipt, "receipt_path": str(receipt_path), "receipt_sha256": legacy._sha(receipt_path)}
            state["receipts"].append({"path": str(receipt_path), "sha256": legacy._sha(receipt_path)})
            legacy._save_state(root, state)
            return receipt

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest="command",required=True)
    for name in ("run","status"):
        child=commands.add_parser(name)
        child.add_argument("--paper-dir",type=Path,required=True)
        child.add_argument("--report-dir",type=Path)
        if name == "run":
            child.add_argument("--phase",choices=("auto","open","close"),default="auto")
    args=parser.parse_args()
    if args.command == "status":
        from stopat30m.ashare_paper_report import ashare_paper_summary
        result=ashare_paper_summary(args.paper_dir)
    else:
        phase=args.phase
        if phase == "auto":
            local=legacy._instant().astimezone(legacy.SHANGHAI)
            phase="open" if time(9,30)<=local.time()<=time(9,45) else "close" if local.time()>=time(15,5) else None
        if phase is None:
            result={"status":"skipped_outside_window","network_called":False,"live_order":False}
        else:
            class LazyProvider:
                def _fetch(self,name,*values):
                    from stopat30m.ashare_low_turnover_provider import AshareLowTurnoverProvider
                    return getattr(AshareLowTurnoverProvider.from_paper_dir(args.paper_dir),name)(*values)
                def fetch_open(self,*values): return self._fetch("fetch_open",*values)
                def fetch_close(self,*values): return self._fetch("fetch_close",*values)
            result=run_low_turnover(args.paper_dir,phase=phase,provider=LazyProvider())
    if args.report_dir:
        from stopat30m.ashare_paper_report import export_ashare_paper_report
        result["report_export"]=export_ashare_paper_report(args.paper_dir,args.report_dir)
    print(json.dumps(legacy._serial(result),ensure_ascii=False,allow_nan=False))
    if result.get("status") in {"failed","partial","attempt_limit","blocked_observation_gap_no_fill","blocked_signal_no_fill"}:
        parser.exit(1)


if __name__ == "__main__":
    main()
