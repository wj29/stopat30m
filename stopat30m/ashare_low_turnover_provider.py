"""Prospective fixed-union paper data, with immutable, guarded incremental seams.

The 1,300-name anchor is a disclosed watchlist, never a claim of today's index
membership. Daily source failures stop publication; neither stale quotations nor
invented adjustment factors can advance a signal. No brokerage API is used.
"""
from __future__ import annotations

from contextlib import redirect_stdout
from datetime import datetime, time, timezone
import fcntl
import hashlib
import io
import json
from pathlib import Path
import re
from uuid import uuid4
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from .ashare_paper_provider import AsharePaperProvider, _proof, _unique
from .data.ashare_daily import _rows
from .data.ashare_execution_data import EXECUTION_FIELDS
from .data.ashare_daily_update import _board
from .data.qlib_continuation import (BridgePolicy, CanonicalIncrement, NormalizationContinuation,
    audit_bridge, publish_increment, read_qlib_security)
from .data.qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot, prepare_qlib_directory
from .data.research_calendar import Calendar, iso_day, natural_days

SHANGHAI = ZoneInfo("Asia/Shanghai")
KIND = "isolated_ashare_low_turnover_forward"
WATCHLIST_KIND = "frozen_official_union_prospective_watchlist"
OVERLAP_SESSIONS = 40
POLICY = BridgePolicy(base_raw_volume_multiplier=100)
BASIC_FIELDS = ["code", "code_name", "ipoDate", "outDate", "type", "status"]


def _now():
    return datetime.now(timezone.utc)


def _digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)+"\n").encode()
    tmp = path.with_name(path.name+"."+uuid4().hex+".tmp")
    try:
        with tmp.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            import os
            os.fsync(stream.fileno())
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
    return _proof(path)


def _instant(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("A timezone-aware source timestamp is required")
    return value


def _symbols(values):
    values = list(values)
    if any(not isinstance(i, str) or not re.fullmatch(r"(?:SH|SZ)\d{6}", i) for i in values):
        raise ValueError("Canonical Shanghai/Shenzhen stock identifiers are required")
    return sorted(set(values))


def _client(on_guard):
    import baostock as bs
    from .data.baostock_validated import _discard_connection
    from .data.baostock_market_validated import ValidatedBaostockMarketClient
    _discard_connection()
    return ValidatedBaostockMarketClient(bs, timeout=12, min_interval=.6, on_guard=on_guard)


def _validate_guard(proof, payload, *, daily):
    proof = _proof(proof["path"], proof["sha256"])
    guard = json.loads(Path(proof["path"]).read_bytes())
    request, echo = payload["request"], guard.get("server_echo")
    if (guard.get("request") != request or guard.get("status") != "verified"
            or guard.get("server_error_code") != "0"
            or guard.get("complete_row_iteration_observed") is not True
            or type(guard.get("rows_verified")) is not int or guard["rows_verified"] != len(payload["rows"])
            or not isinstance(echo, dict)):
        raise ValueError("Source request/terminal row-iteration guard is incomplete or mismatched")
    if daily:
        if (guard.get("kind") != "baostock_daily_server_echo_guard"
                or {k:v for k,v in echo.items() if k != "adjustflag"} != {k:v for k,v in request.items() if k != "adjustflag"}
                or echo.get("adjustflag") not in ("", request["adjustflag"])
                or guard.get("row_mode_flags_repaired") is not False
                or guard.get("row_mode_anomalies_observed")):
            raise ValueError("Daily source echo or original adjustment mode is invalid")
    elif echo != request:
        raise ValueError("Metadata/dividend server identity echo differs from request")
    start, finish, fetched = map(_instant, (guard["started_at"], guard["finished_at"], payload["fetched_at"]))
    if not start <= finish <= fetched or start.astimezone(SHANGHAI).date() != fetched.astimezone(SHANGHAI).date():
        raise ValueError("Source guard timestamps cannot follow capture or cross a source day")
    return proof


def _read_original(root, receipt, key):
    record = receipt["responses"][key]
    if Path(record["file"]).name != record["file"]:
        raise ValueError("Original response must be local to its capture directory")
    proof = _proof(root/record["file"], record["sha256"])
    payload = json.loads(Path(proof["path"]).read_bytes())
    basic = key == "stock_basic"
    if basic:
        expected = {"code": "", "code_name": ""}
        fields = BASIC_FIELDS
    else:
        inst, mode = key.split("_")
        expected = {"code": inst[:2].lower()+"."+inst[2:], "fields": EXECUTION_FIELDS,
            "start_date": receipt["start_date"], "end_date": receipt["session"],
            "frequency": "d", "adjustflag": {"raw":"3", "hfq":"1"}[mode]}
        fields = EXECUTION_FIELDS.split(",")
    if (payload.get("request") != expected or payload.get("fields") != fields
            or payload.get("generation") != receipt["generation"]
            or record.get("rows") != len(payload["rows"])
            or record.get("fetched_at") != payload.get("fetched_at")
            or _instant(payload["fetched_at"]).astimezone(SHANGHAI).date().isoformat() != receipt["session"]
            or any(len(row) != len(fields) for row in payload["rows"])):
        raise ValueError("Original daily capture request/schema/generation/source day differs")
    guard = _validate_guard(record["guard"], payload, daily=not basic)
    frame = pd.DataFrame(payload["rows"], columns=fields)
    if basic:
        if frame.empty or frame.code.duplicated().any():
            raise ValueError("Current security identity registry is empty or ambiguous")
    elif not frame.empty:
        if (not frame.code.eq(expected["code"]).all() or not frame.adjustflag.eq(expected["adjustflag"]).all()
                or frame.date.tolist() != sorted(set(frame.date))
                or not frame.date.between(receipt["start_date"], receipt["session"]).all()
                or not frame.tradestatus.isin(["0", "1"]).all() or not frame.isST.isin(["0", "1"]).all()):
            raise ValueError("Daily original identity/date/mode/status differs; no source relabelling")
        for day in frame.date:
            iso_day(day)
    return frame, [proof, guard]


def capture_daily_originals(root, start, session, instruments):
    """Serial, fresh-connection capture; first failure stops, successes resume.

    A successful empty response is still guarded. It becomes an explicit absent
    observation later, not a suspension or an invented price. No index endpoint
    is queried: the frozen watchlist is an independently evidenced input.
    """
    root = guard_qlib_output_path(root)
    root.mkdir(parents=True, exist_ok=True)
    symbols = _symbols(instruments)
    if not symbols or iso_day(start) > iso_day(session):
        raise ValueError("Explicit source targets and overlap window are required")
    path = root/"capture.json"
    with (root/".capture.lock").open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        if path.exists():
            receipt = json.loads(path.read_bytes())
            if (receipt.get("kind") != "low_turnover_guarded_daily_originals" or receipt.get("start_date") != start
                    or receipt.get("session") != session or receipt.get("instruments") != symbols):
                raise ValueError("Resume must retain the exact capture window and targets")
        else:
            receipt = {"kind":"low_turnover_guarded_daily_originals", "generation":uuid4().hex,
                "source":"baostock", "start_date":start, "session":session, "instruments":symbols,
                "responses":{}, "status":"collecting", "failures":{}}
            _save(path, receipt)
        keys = ["stock_basic", *[i+"_"+mode for i in symbols for mode in ("raw", "hfq")]]
        if set(receipt["responses"])-set(keys):
            raise ValueError("Unexpected cached original response inventory")
        for key in receipt["responses"]:
            _read_original(root, receipt, key)
        if set(receipt["responses"]) == set(keys):
            if receipt.get("status") != "complete" or receipt.get("failures"):
                raise ValueError("Complete response inventory has an inconsistent capture status")
            return receipt
        if _now().astimezone(SHANGHAI).date().isoformat() != session:
            raise ValueError("An unfinished source generation cannot resume on a later calendar day")
        guards = []
        def on_guard(value):
            guards.append(_save(root/"guards"/(uuid4().hex+".json"), value))
        source = _client(on_guard)
        logged = False
        try:
            with redirect_stdout(io.StringIO()):
                login = source.login()
            if str(login.error_code) != "0":
                raise ValueError("BaoStock guarded daily login failed")
            logged = True
            for key in keys:
                if key in receipt["responses"]:
                    continue
                if key == "stock_basic":
                    request = {"code":"", "code_name":""}
                    query = source.query_stock_basic
                else:
                    inst, mode = key.split("_")
                    request = {"code":inst[:2].lower()+"."+inst[2:], "fields":EXECUTION_FIELDS,
                        "start_date":start, "end_date":session, "frequency":"d",
                        "adjustflag":"3" if mode == "raw" else "1"}
                    query = source.query_history_k_data_plus
                guards.clear()
                response = query(**request)
                rows = _rows(response)
                if len(guards) != 1:
                    raise ValueError("Each original response requires exactly one completed server guard")
                payload = {"request":request, "fields":list(response.fields), "rows":rows,
                    "generation":receipt["generation"], "fetched_at":_now().isoformat()}
                _validate_guard(guards[0], payload, daily=key != "stock_basic")
                file = root/(key+".json")
                if file.exists():
                    raise ValueError("Unreceipted original exists; preserve it and audit before resume")
                proof = _save(file, payload)
                receipt["responses"][key] = {"file":file.name, "sha256":proof["sha256"],
                    "fetched_at":payload["fetched_at"], "rows":len(rows), "guard":guards[0]}
                _read_original(root, receipt, key)
                receipt["failures"] = {}
                _save(path, receipt)
            receipt.update(status="complete", finished_at=_now().isoformat(), failures={})
            _save(path, receipt)
            return receipt
        except Exception as exc:
            receipt.update(status="partial_not_publishable", failures={"capture":{
                "error_type":type(exc).__name__, "error":str(exc)[:300]}}, finished_at=_now().isoformat())
            _save(path, receipt)
            raise
        finally:
            if logged:
                try:
                    with redirect_stdout(io.StringIO()):
                        source.logout()
                except Exception:
                    pass


class AshareLowTurnoverProvider:
    def __init__(self, paper_dir, adapter):
        self.paper_dir = Path(paper_dir).resolve()
        self.adapter = adapter
        def path(key):
            value = adapter.get(key)
            if not isinstance(value, str) or not value:
                raise ValueError("Low-turnover adapter path missing: "+key)
            raw = Path(value)
            return (self.paper_dir/raw).resolve() if not raw.is_absolute() else raw.resolve()
        self.initial_snapshot_path = path("initial_snapshot_path")
        self.calendar_path = path("calendar_path")
        self.universe_path = path("universe_path")
        self.stock_basic_path = path("stock_basic_path")
        self.output_dir = guard_qlib_output_path(path("provider_output_dir"))

    @classmethod
    def from_paper_dir(cls, paper_dir):
        root = Path(paper_dir).resolve()
        return cls(root, json.loads((root/"adapter.json").read_bytes()))

    def _state(self):
        manifest = json.loads((self.paper_dir/"manifest.json").read_bytes())
        state = json.loads((self.paper_dir/"state.json").read_bytes())
        if (manifest.get("kind") != KIND
                or manifest.get("manifest_sha256") != _digest({k:v for k,v in manifest.items() if k != "manifest_sha256"})
                or state.get("manifest_sha256") != manifest["manifest_sha256"]
                or state.get("state_sha256") != _digest({k:v for k,v in state.items() if k != "state_sha256"})
                or manifest.get("adapter_sha256") != _digest(self.adapter)
                or manifest.get("calendar_sha256") != _proof(self.calendar_path)["sha256"]
                or manifest.get("universe_sha256") != _proof(self.universe_path)["sha256"]
                or self.adapter.get("stock_basic_sha256") != _proof(self.stock_basic_path)["sha256"]
                or self.adapter.get("initial_snapshot_manifest_sha256") != _proof(self.initial_snapshot_path/"manifest.json")["sha256"]):
            raise ValueError("New low-turnover paper state/configuration failed its frozen binding")
        return manifest, state

    def _universe(self):
        value = json.loads(self.universe_path.read_bytes())
        symbols = _symbols(value["instruments"])
        if (value.get("kind") != WATCHLIST_KIND or len(symbols) != 1300
                or value["instruments"] != symbols or value.get("as_of") != value.get("source_date")
                or iso_day(value["source_date"]) != "2026-10-19"):
            raise ValueError("The evidenced 2026-10-19 fixed official union watchlist is required")
        proofs = value.get("bound_inputs") or value.get("evidence")
        if not isinstance(proofs, list) or not proofs:
            raise ValueError("Fixed watchlist requires its original membership evidence")
        return symbols, _unique([_proof(self.universe_path), *[_proof(p["path"],p["sha256"],p.get("url")) for p in proofs]])

    def _session(self, session, phase):
        calendar = Calendar.load(self.calendar_path)
        local = _now().astimezone(SHANGHAI)
        if iso_day(session) != local.date().isoformat() or not calendar.is_session(session):
            raise ValueError("Provider requires today's explicitly covered real exchange session")
        if phase == "open" and not time(9,30) <= local.time() <= time(9,45):
            raise ValueError("Provider requires the prospective opening window")
        if phase == "close" and local.time() < time(15,5):
            raise ValueError("Provider requires a complete close after 15:05 Shanghai")
        return calendar

    def _static_basic(self):
        frame = pd.read_csv(self.stock_basic_path, dtype=str, keep_default_na=False)
        if not {"code", "type", "ipoDate", "outDate"}.issubset(frame) or frame.code.duplicated().any():
            raise ValueError("Canonical stock identity CSV is incomplete or ambiguous")
        return frame

    def _load_result(self, path):
        value = json.loads(Path(path).read_bytes())
        if (value.get("kind") != "low_turnover_union_close_increment"
                or value.get("result_sha256") != _digest({k:v for k,v in value.items() if k != "result_sha256"})):
            raise ValueError("Saved union increment result was changed")
        for proof in value["bound_inputs"]:
            _proof(proof["path"],proof["sha256"])
        snapshot = load_qlib_snapshot(value["snapshot_path"])
        if snapshot.manifest_sha256 != value["snapshot_manifest_sha256"]:
            raise ValueError("Saved increment snapshot manifest changed")
        symbols, _ = self._universe()
        if value["current_index_instruments"] != symbols:
            raise ValueError("Saved increment belongs to another fixed universe")
        return value

    def _latest_refresh(self, session, *, before=False):
        choices = []
        for path in sorted((self.output_dir/"daily").glob("*/refresh_result.json")):
            value = self._load_result(path)
            if value["session"] < session or value["session"] == session and not before:
                choices.append((value["session"],0,value,path))
        # An explicit recovery may bind a new, independently verified sibling
        # source version. Original refreshes, signals and observations stay put.
        from .ashare_low_turnover_recovery import source_repair
        _, state = self._state()
        repaired = source_repair(state)
        if repaired is not None:
            path = Path(_proof(repaired["path"], repaired["sha256"])["path"])
            value = self._load_result(path)
            if value["session"] < session or value["session"] == session and not before:
                choices.append((value["session"],1,value,path))
        if choices:
            _, _, value, path = max(choices, key=lambda x:(x[0],x[1]))
            return value, path
        snapshot = load_qlib_snapshot(self.initial_snapshot_path)
        symbols, proofs = self._universe()
        source_day = json.loads(self.universe_path.read_bytes())["source_date"]
        if source_day > session or source_day == session and before:
            raise ValueError("No verified preceding snapshot exists for this session")
        stamp = _now().isoformat()
        basic = self._static_basic().set_index("code")
        metadata = {}
        for inst in symbols:
            code = inst[:2].lower()+"."+inst[2:]
            if code not in basic.index or basic.at[code,"type"] != "1":
                raise ValueError("Fixed watchlist lacks canonical A-share identity: "+inst)
            metadata[inst] = self._identity(inst,basic.loc[code],session,None,stamp)
        proofs += [_proof(self.stock_basic_path),_proof(snapshot.path/"manifest.json"),_proof(self.calendar_path)]
        return {"session":source_day,"snapshot_path":str(snapshot.path),"metadata":metadata,
            "bound_inputs":_unique(proofs),"current_index_instruments":symbols}, None

    def _refresh_proofs(self, result, result_path):
        _, universe = self._universe()
        proofs = [_proof(p["path"],p["sha256"]) for p in result["bound_inputs"]]
        if result_path is not None:
            proofs.append(_proof(result_path))
        return _unique(proofs), universe

    @staticmethod
    def _identity(inst, row, session, is_st, stamp):
        ipo = iso_day(row["ipoDate"])
        out = iso_day(row["outDate"]) if row["outDate"] else None
        return {"instrument":inst,"board":_board(inst),"listing_date":ipo,"ipo_date":ipo,"out_date":out,
            "is_st":is_st,"status_session":session,"source":"baostock.identity+observed_daily_or_live_status",
            "known_at":stamp,"listing_known_at":stamp,"status_known_at":stamp,
            "announcement_pit_verified":False,"delisting_period":bool(out and out <= session)}

    @staticmethod
    def _normalization_continuation(parent):
        """Reconstruct the exact previous source batch from archived originals.

        A supplier unit is inherited only through the increment receipt that
        belongs to this immutable parent. No prior scale is guessed from the
        current failure, and historical supplier revisions are not repaired
        here.
        """
        path = parent.dataset_dir/"incremental_manifest.json"
        if not path.exists():
            return None, []
        increment = json.loads(path.read_bytes())
        evidence = increment.get("source_evidence", {})
        if "verified_factor_revision" in evidence:
            from .data.qlib_factor_revision import load_verified_revision_batch
            contract = evidence["verified_factor_revision"]
            _proof(contract["path"],contract["sha256"])
            previous, proofs = load_verified_revision_batch(contract["path"])
            if (previous.evidence != evidence or previous.generation != increment.get("generation")
                    or previous.source != increment.get("source")):
                raise ValueError("Derived parent source differs from its verified revision contract")
            return NormalizationContinuation(previous,_proof(path)["sha256"]),_unique([_proof(path),*proofs.values()])
        receipts = [item for item in evidence.get("bound_inputs", [])
            if Path(item["path"]).name == "capture.json"]
        if len(receipts) != 1:
            raise ValueError("Parent increment lacks one exact original capture receipt")
        record = receipts[0]
        receipt_path = Path(_proof(record["path"],record["sha256"])["path"])
        receipt = json.loads(receipt_path.read_bytes())
        if (receipt.get("status") != "complete" or receipt.get("failures")
                or receipt.get("generation") != increment.get("generation")):
            raise ValueError("Parent source capture is not the published complete generation")
        raw_parts, hfq_parts, proofs = [], [], [_proof(path),_proof(receipt_path)]
        for inst in sorted(increment.get("securities", {})):
            raw, bound = _read_original(receipt_path.parent,receipt,inst+"_raw");proofs += bound
            hfq, bound = _read_original(receipt_path.parent,receipt,inst+"_hfq");proofs += bound
            raw["instrument"] = inst;hfq["instrument"] = inst
            raw_parts.append(raw);hfq_parts.append(hfq)
        if not raw_parts:
            raise ValueError("Published parent increment has no audited source securities")
        days = set((parent.dataset_dir/"calendars/day.txt").read_text().splitlines())
        rows = pd.DataFrame([{"date":day,"is_open":int(day in days)}
            for day in natural_days(receipt["start_date"],receipt["session"])])
        provenance = increment["calendar_provenance"]
        previous = CanonicalIncrement.from_frames(pd.concat(raw_parts,ignore_index=True),
            pd.concat(hfq_parts,ignore_index=True),source=increment["source"],generation=receipt["generation"],
            calendar_rows=rows,calendar_source=provenance["source"],calendar_source_url=provenance["source_url"],
            requested_start=receipt["start_date"],requested_end=receipt["session"],evidence=evidence)
        return NormalizationContinuation(previous,_proof(path)["sha256"]),_unique(proofs)

    def _refresh_close(self, parent_path, session, instruments, calendar):
        parent = load_qlib_snapshot(parent_path)
        old_days = (parent.dataset_dir/"calendars/day.txt").read_text().splitlines()
        previous = [day for day in old_days if day < session]
        if len(previous) < OVERLAP_SESSIONS or session < old_days[-1]:
            raise ValueError("Increment needs 40 preceding actual sessions and a non-backwards parent")
        start = previous[-OVERLAP_SESSIONS]
        # Parent sessions provide historical flags; independently sourced calendar
        # must agree wherever covered and explicitly supplies every new day.
        old_set, flags = set(old_days), dict(calendar.days)
        for day, flag in calendar.days:
            if old_days[0] <= day <= old_days[-1] and flag != (day in old_set):
                raise ValueError("Source-backed calendar contradicts the immutable parent")
        calendar_rows = []
        for day in natural_days(start,session):
            if day <= old_days[-1]:
                opened = day in old_set
            elif day in flags:
                opened = flags[day]
            else:
                raise ValueError("Unknown exchange calendar tail")
            calendar_rows.append({"date":day,"is_open":int(opened)})
        work = self.output_dir/"daily"/session
        work.mkdir(parents=True,exist_ok=True)
        with (work/".increment.lock").open("a+b") as lock:
            fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
            result_path = work/"refresh_result.json"
            if result_path.exists():
                result = self._load_result(result_path)
                if not set(instruments).issubset(result["daily_quotes"]):
                    raise ValueError("Completed increment omits new held/rights targets; audit a new generation")
                return result,result_path
            root = work/"originals"
            receipt = capture_daily_originals(root,start,session,instruments)
            expected = {"stock_basic",*[i+"_"+m for i in instruments for m in ("raw","hfq")]}
            if receipt["status"] != "complete" or receipt["failures"] or set(receipt["responses"]) != expected:
                raise ValueError("Partial union capture cannot publish a new signal snapshot")
            proofs = [_proof(root/"capture.json"),_proof(self.calendar_path),_proof(self.stock_basic_path),
                _proof(parent.path/"manifest.json"),*self._universe()[1]]
            basic, p = _read_original(root,receipt,"stock_basic"); proofs += p
            raw_parts, hfq_parts, excluded = [], [], {}
            all_raw = []
            for inst in instruments:
                raw,p = _read_original(root,receipt,inst+"_raw"); proofs += p
                hfq,p = _read_original(root,receipt,inst+"_hfq"); proofs += p
                if raw.date.tolist() != hfq.date.tolist():
                    raise ValueError("Raw/HFQ source dates differ")
                for name in ("volume","amount","tradestatus","isST"):
                    if not raw[name].equals(hfq[name]):
                        raise ValueError("Raw/HFQ non-price fields differ")
                raw["instrument"] = inst; hfq["instrument"] = inst
                all_raw.append(raw)
                if session not in raw.date.tolist():
                    excluded[inst] = "source_missing_session_not_inferred_suspension"
                    continue
                raw_parts.append(raw); hfq_parts.append(hfq)
            if not raw_parts:
                raise ValueError("No target has a current source observation; stale bars cannot advance a signal")
            raw = pd.concat(all_raw,ignore_index=True)
            source = {"source":"baostock", "generation":receipt["generation"],
                "parent_manifest_sha256":parent.manifest_sha256,"bound_inputs":_unique(proofs),
                "source_observed_as_of":session,"atomic_supplier_vintage_verified":False,
                "fixed_watchlist_not_daily_index_membership":True}
            batch = CanonicalIncrement.from_frames(pd.concat(raw_parts,ignore_index=True),pd.concat(hfq_parts,ignore_index=True),
                source="baostock", generation=receipt["generation"],calendar_rows=pd.DataFrame(calendar_rows),
                calendar_source="verified_parent_sessions+"+calendar.source,calendar_source_url=calendar.source_url,
                requested_start=start,requested_end=session,evidence=source)
            continuation, continuation_proofs = self._normalization_continuation(parent)
            proofs += continuation_proofs
            bridge = audit_bridge(parent.dataset_dir,batch,POLICY,continuation=continuation)
            dataset = work/"dataset"
            if dataset.exists():
                staged = json.loads((dataset/"incremental_manifest.json").read_bytes())
                if (staged.get("source_bars_sha256") != bridge["source_bars_sha256"]
                        or staged.get("source_evidence") != source or staged.get("policy") != json.loads(json.dumps(bridge["policy"]))
                        or staged.get("normalization_continuation") != bridge.get("normalization_continuation")
                        or any(staged.get("securities",{}).get(inst,{}).get("normalization_scale") != row["normalization_scale"]
                            for inst,row in bridge["securities"].items())):
                    raise ValueError("Existing staged increment binds different source bytes or seam policy")
            else:
                publish_increment(parent.dataset_dir,batch,dataset,POLICY,pilot_only=False,continuation=continuation)
            # Each input is rechecked before the new immutable version is exposed.
            for proof in proofs:
                _proof(proof["path"],proof["sha256"])
            snapshot = prepare_qlib_directory(dataset,work/"versions",source=parent.manifest["source"],
                parent_snapshot=parent,increment_manifest_path=dataset/"incremental_manifest.json")
            quotes,metadata = self._daily_quotes(snapshot,raw,basic,receipt,session,instruments,excluded)
            fixed,_ = self._universe()
            value = {"kind":"low_turnover_union_close_increment","session":session,
                "snapshot_path":str(snapshot.path),"snapshot_manifest_sha256":snapshot.manifest_sha256,
                "daily_quotes":quotes,"metadata":metadata,"current_index_instruments":fixed,
                "universe_scope":"fixed_20261019_official_anchor_not_daily_current_membership",
                "excluded":excluded,"bound_inputs":_unique([*proofs,_proof(snapshot.path/"manifest.json")]),
                "bridge_policy":bridge["policy"],"parent_snapshot_path":str(parent.path),
                "signal_ready":sum(q["bar_complete"] for i,q in quotes.items() if i in fixed) >= 20}
            value["result_sha256"] = _digest(value)
            _save(result_path,value)
            return self._load_result(result_path),result_path

    def _daily_quotes(self,snapshot,raw,basic,receipt,session,instruments,excluded):
        basic = basic.set_index("code")
        quotes, metadata = {},{}
        for inst in instruments:
            code = inst[:2].lower()+"."+inst[2:]
            if code not in basic.index or basic.at[code,"type"] != "1":
                raise ValueError("Current identity query lacks an A-share target: "+inst)
            row = raw.loc[raw.instrument.eq(inst)&raw.date.eq(session)]
            row = row.iloc[0] if len(row)==1 else None
            original = receipt["responses"][inst+"_raw"]
            identity_stamp = receipt["responses"]["stock_basic"]["fetched_at"]
            stamp = max(_instant(identity_stamp),_instant(original["fetched_at"])).isoformat()
            is_st = row.isST == "1" if row is not None else None
            metadata[inst] = self._identity(inst,basic.loc[code],session,is_st,stamp)
            quote = {"instrument":inst,"session":session,"date":session,"source":"baostock.daily_raw",
                "tradestatus":{"1":"trading","0":"suspended"}.get(row.tradestatus,"unknown") if row is not None else "unknown",
                "is_st":is_st,"bar_complete":False,"live_opening_quote":False,
                "fetched_at":original["fetched_at"],"source_response_sha256":original["sha256"],
                "open":None,"close":None,"preclose":None,"factor":None}
            if metadata[inst]["out_date"] and metadata[inst]["out_date"] <= session:
                excluded[inst] = "current_identity_outDate_on_or_before_session"
            if row is not None and inst not in excluded:
                if row.tradestatus == "0":
                    excluded[inst] = "source_tradestatus_0"
                else:
                    numeric = pd.to_numeric(row[["open","high","low","close","preclose","volume","amount"]],errors="raise")
                    if not np.isfinite(numeric.to_numpy(float)).all() or not numeric.gt(0).all():
                        raise ValueError("Current traded observation lacks complete raw prices/volume/amount")
                    frame = read_qlib_security(snapshot.dataset_dir,inst)
                    if session not in frame.index or not np.isfinite(frame.loc[session]).all() or not frame.loc[session].gt(0).all():
                        raise ValueError("Current complete quote lacks seven audited Qlib fields")
                    factor = float(frame.at[session,"factor"])
                    for field in ("open","high","low","close"):
                        if not np.isclose(frame.at[session,field]/factor,float(numeric[field]),rtol=1e-6,atol=1e-7):
                            raise ValueError("Published Qlib prices do not restore the original raw observation")
                    quote.update({field:float(numeric[field]) for field in ("open","high","low","close","preclose")})
                    quote.update(factor=factor,bar_complete=True)
            quotes[inst] = quote
        return quotes,metadata

    def _dividends(self,session,phase,manifest,state,work):
        from .ashare_forward import _account_instruments
        from .data.ashare_actions import collect_dividend_evidence,load_dividend_evidence,dividend_events
        held = _symbols(set().union(*(_account_instruments(a) for a in state["accounts"].values())))
        if not held:
            return [],[]
        # Both phases query their own current originals; no stale cache from a
        # prior day or an unqueried security can imply absence of an action.
        guards=[]
        def on_guard(value):
            guards.append(_save(work/"dividend_guards"/(uuid4().hex+".json"),value))
        root = collect_dividend_evidence(work/"dividends",held,[int(session[:4])],
            client=_client(on_guard),min_interval=.6,timeout=12)
        _,receipt = load_dividend_evidence(root)
        proofs=[_proof(Path(root)/"collection.json")]
        for record in receipt["responses"].values():
            proof=_proof(Path(root)/record["file"],record["sha256"])
            payload=json.loads(Path(proof["path"]).read_bytes())
            candidates=[]
            for p in guards:
                guard=json.loads(Path(p["path"]).read_bytes())
                if guard.get("request")==payload["request"]:
                    candidates.append(p)
            if len(candidates)!=1:
                raise ValueError("Every held dividend response, including empty, needs its exact current server guard")
            proofs.extend([proof,_validate_guard(candidates[0],payload,daily=False)])
            for row in payload["rows"]:
                day=dict(zip(payload["fields"],row)).get("dividOperateDate","")
                if day and iso_day(day)[:4] != payload["request"]["year"]:
                    raise ValueError("Dividend implemented year differs from queried source year")
        events=dividend_events(root,start="2000-01-01",end=session)
        initialization=_instant(manifest["created_at"]).astimezone(SHANGHAI).date().isoformat()
        return [e for e in events if initialization<=e["ex_date"]<=session],_unique(proofs)

    def fetch_close(self,session,instruments,now):
        calendar=self._session(session,"close")
        manifest,state=self._state()
        fixed,_=self._universe()
        from .ashare_forward import _account_instruments
        held=set().union(*(_account_instruments(a) for a in state["accounts"].values()))
        targets=_symbols([*fixed,*instruments,*held])
        result,path=self._latest_refresh(session)
        if result["session"]!=session or not set(targets).issubset(result.get("daily_quotes",{})):
            result,path=self._refresh_close(result["snapshot_path"],session,targets,calendar)
        proofs,universe=self._refresh_proofs(result,path)
        work=self.output_dir/"observations"/(session+"_close_"+uuid4().hex)
        events,action_proofs=self._dividends(session,"close",manifest,state,work)
        return {"phase":"close","session":session,"source":"baostock.daily_raw","fetched_at":_now().isoformat(),
            "quotes":result["daily_quotes"],"metadata":result["metadata"],"instruments":fixed,
            "snapshot_path":result["snapshot_path"],"universe_evidence":universe,
            "evidence":_unique([*proofs,*action_proofs]),"corporate_actions":events,
            "source_exclusions":result["excluded"],"live_opening_quote":False,
            "universe_scope":"fixed_20261019_official_anchor_not_daily_current_membership"}

    def fetch_open(self,session,instruments,now):
        # Reuse only the proven live quote parsing/receive-clock logic. All state,
        # universe, identity, source history and action methods dispatch here.
        batch = AsharePaperProvider.fetch_open(self,session,instruments,now)
        # A retained right can deliver a name outside the fixed signal pool.
        # Current quote status remains live; the fallback registry is explicitly
        # frozen identity evidence, never a new index/IPO source claim.
        missing = set(batch["quotes"]) - set(batch["metadata"])
        if missing:
            basic = self._static_basic().set_index("code")
            for inst in sorted(missing):
                code = inst[:2].lower()+"."+inst[2:]
                if code not in basic.index or basic.at[code,"type"] != "1":
                    raise ValueError("A live held/rights target lacks verified stock identity: "+inst)
                quote = batch["quotes"][inst]
                row = self._identity(inst,basic.loc[code],session,quote["is_st"],quote["as_of"])
                row["source"] = "frozen_registry_identity+tencent.current_quote_status"
                batch["metadata"][inst] = row
            batch["evidence"] = _unique([*batch["evidence"],_proof(self.stock_basic_path)])
        return batch


Provider = AshareLowTurnoverProvider
