"""Offline, explicit effective-date corrections from independently guarded facts.

This deliberately narrow repair reconstructs the original 2026-10-20 batch.
Only five newly appended HFQ rows can change. It never chooses a seam scale,
relaxes a tolerance, downloads data, writes a snapshot, or changes a paper book.
The caller must audit and publish a sibling against the original 10/19 parent.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

import pandas as pd

from .ashare_execution_data import EXECUTION_FIELDS
from .qlib_incremental import BridgePolicy, CanonicalIncrement, _validated_batch
from .qlib_snapshot import load_qlib_snapshot
from .research_calendar import Calendar, iso_day, natural_days

KIND = "a15_verified_effective_factor_revision_contract"
SESSION = "2026-10-20"
PREVIOUS_SESSION = "2026-10-19"
CONFIRMATION_END = "2026-10-21"
REVISED_INSTRUMENTS = ("SH601898", "SH688016", "SZ000563", "SZ300433", "SZ300595")
PRICE_FIELDS = ("open", "high", "low", "close", "preclose")
FACTOR_FIELDS = ("code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor")
SHANGHAI = ZoneInfo("Asia/Shanghai")
POLICY = BridgePolicy(base_raw_volume_multiplier=100)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _proof(value):
    if not isinstance(value, dict) or set(value) != {"path", "sha256"}:
        raise ValueError("An exact path/SHA256 proof is required")
    path = Path(value["path"])
    if (not path.is_absolute() or path.is_symlink() or not path.is_file()
            or not re.fullmatch(r"[0-9a-f]{64}", str(value["sha256"]))
            or _sha(path.read_bytes()) != value["sha256"]):
        raise ValueError("Revision input checksum/path differs")
    return {"path": str(path.resolve()), "sha256": value["sha256"]}


def _file_proof(path):
    path = Path(path)
    return _proof({"path": str(path.absolute()), "sha256": _sha(path.read_bytes())})


def _instant(value):
    try:
        stamp = datetime.fromisoformat(value)
    except (ValueError, TypeError):
        raise ValueError("Revision timestamps must be timezone-aware") from None
    if stamp.tzinfo is None:
        raise ValueError("Revision timestamps must be timezone-aware")
    return stamp


def _positive(value):
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("Factor/price must be a finite positive Decimal") from None
    if not number.is_finite() or number <= 0:
        raise ValueError("Factor/price must be a finite positive Decimal")
    return number


def _validate_contract(contract):
    required = {"kind", "schema_version", "created_at", "session", "revised_instruments",
        "derived_generation", "previous_capture", "confirmation_capture", "factor_verification_report",
        "initial_snapshot_manifest", "previous_snapshot_manifest", "previous_refresh_result", "calendar"}
    if (set(contract) != required or contract.get("kind") != KIND or contract.get("schema_version") != 1
            or contract.get("session") != SESSION or contract.get("revised_instruments") != list(REVISED_INSTRUMENTS)
            or not re.fullmatch(r"derived_effective_factor_revision:[0-9a-f]{32}", str(contract.get("derived_generation", "")))):
        raise ValueError("Only the declared five effective-date corrections are permitted")
    if _instant(contract["created_at"]).astimezone(SHANGHAI).date().isoformat() < "2026-10-22":
        raise ValueError("Derived revision cannot masquerade as an original 10/20 capture")


def _load_snapshots(contract, proofs):
    initial = load_qlib_snapshot(proofs["initial_snapshot_manifest"]["path"])
    previous = load_qlib_snapshot(proofs["previous_snapshot_manifest"]["path"])
    refresh = json.loads(Path(proofs["previous_refresh_result"]["path"]).read_bytes())
    increment_path = previous.dataset_dir / "incremental_manifest.json"
    proofs["previous_increment"] = _file_proof(increment_path)
    increment = json.loads(increment_path.read_bytes())
    if (refresh.get("session") != SESSION or Path(refresh.get("parent_snapshot_path", "")).resolve() != initial.path
            or Path(refresh.get("snapshot_path", "")).resolve() != previous.path
            or refresh.get("snapshot_manifest_sha256") != previous.manifest_sha256
            or previous.manifest["lineage"]["parent_snapshot"]["manifest_sha256"] != initial.manifest_sha256
            or increment.get("base_dir") != str(initial.dataset_dir)
            or increment.get("source") != "baostock" or increment.get("status") != "bridge_verified"
            or increment.get("policy") != json.loads(json.dumps(POLICY.__dict__))
            or increment.get("calendar_tail_not_watermark") != PREVIOUS_SESSION
            or increment.get("audited_common_finite_end") != SESSION):
        raise ValueError("Revision must replace only the original parent's newly appended session")
    if (initial.manifest_sha256 != proofs["initial_snapshot_manifest"]["sha256"]
            or previous.manifest_sha256 != proofs["previous_snapshot_manifest"]["sha256"]):
        raise ValueError("Snapshot manifest proof differs")
    for inst in REVISED_INSTRUMENTS:
        sec = increment.get("securities", {}).get(inst, {})
        if sec.get("before", {}).get("last_complete_finite") != PREVIOUS_SESSION:
            raise ValueError("A revised date was not a new append against the initial parent")
    bound = increment.get("source_evidence", {}).get("bound_inputs", [])
    for key in ("previous_capture", "calendar", "initial_snapshot_manifest"):
        if proofs[key] not in bound:
            raise ValueError("Original published increment does not bind the revision input")
    old_days = (initial.dataset_dir / "calendars/day.txt").read_text().splitlines()
    new_days = (previous.dataset_dir / "calendars/day.txt").read_text().splitlines()
    if (not old_days or old_days[-1] != PREVIOUS_SESSION or new_days != old_days + [SESSION]):
        raise ValueError("The revision must preserve the original calendar prefix")
    return initial, previous, increment


def _read_confirmation(capture, key, expected_request, fields, *, daily):
    # Runtime import avoids the provider's optional import of this offline loader.
    from ..ashare_low_turnover_provider import _validate_guard
    record = capture["responses"][key]
    proof = _proof({"path": record["path"], "sha256": record["sha256"]})
    payload = json.loads(Path(proof["path"]).read_bytes())
    if (payload.get("request") != expected_request or payload.get("fields") != list(fields)
            or payload.get("generation") != capture["generation"]
            or payload.get("query") != ("query_history_k_data_plus" if daily else "query_adjust_factor")
            or record.get("fetched_at") != payload.get("fetched_at")
            or record.get("rows") != len(payload.get("rows", []))
            or any(len(row) != len(fields) for row in payload.get("rows", []))
            or _instant(payload["fetched_at"]).astimezone(SHANGHAI).date().isoformat() != capture["actual_capture_day"]):
        raise ValueError("Independent confirmation request/schema/generation/source day differs")
    guard = _validate_guard(record["guard"], payload, daily=daily)
    if not daily:
        guard_payload = json.loads(Path(guard["path"]).read_bytes())
        if guard_payload.get("kind") != "baostock_metadata_server_echo_guard" or guard_payload.get("query") != "query_adjust_factor":
            raise ValueError("Factor evidence requires the original adjust-factor query guard")
    frame = pd.DataFrame(payload["rows"], columns=fields)
    if frame.empty or not frame.code.eq(expected_request["code"]).all():
        raise ValueError("Independent confirmation has another security or no rows")
    if daily:
        if (frame.date.tolist() != [PREVIOUS_SESSION, SESSION, CONFIRMATION_END]
                or not frame.adjustflag.eq(expected_request["adjustflag"]).all()
                or not frame.tradestatus.eq("1").all() or not frame.isST.isin(["0", "1"]).all()):
            raise ValueError("Confirmation must contain the three exact, original-mode trading rows")
    else:
        days = frame.dividOperateDate.tolist()
        if days != sorted(set(days)) or any(not expected_request["start_date"] <= iso_day(day) <= expected_request["end_date"] for day in days):
            raise ValueError("Factor dates must be unique, ordered and inside the IPO/current window")
        if days[0] != expected_request["start_date"] or _positive(frame.iloc[0].backAdjustFactor) != 1:
            raise ValueError("Complete IPO-anchored factor history is required")
        for name in FACTOR_FIELDS[2:]:
            for value in frame[name]:
                _positive(value)
    return frame, [proof, guard]


def _verified_replacement(inst, old_raw, old_hfq, fresh_raw, fresh_hfq, factors):
    """Prove a dated factor event; no numerical fitting or price tolerance here."""
    raw = old_raw.set_index("date", drop=False)
    hfq = old_hfq.set_index("date", drop=False)
    new_raw = fresh_raw.set_index("date", drop=False)
    new_hfq = fresh_hfq.set_index("date", drop=False)
    if SESSION not in raw.index or SESSION not in hfq.index or PREVIOUS_SESSION not in raw.index:
        raise ValueError("Original two-session correction context is incomplete")
    for day in (PREVIOUS_SESSION, SESSION):
        if raw.loc[day].to_dict() != new_raw.loc[day].to_dict():
            raise ValueError("Raw prices, volume, amount, preclose and statuses must remain exact")
    if hfq.loc[PREVIOUS_SESSION].to_dict() != new_hfq.loc[PREVIOUS_SESSION].to_dict():
        raise ValueError("No prior historical adjusted observation may change")
    old_row, new_row = hfq.loc[SESSION], new_hfq.loc[SESSION]
    if any(old_row[name] != new_row[name] for name in old_row.index if name not in PRICE_FIELDS):
        raise ValueError("Adjusted non-price/status identity must remain exact")
    history = factors.set_index("dividOperateDate", drop=False)
    if SESSION not in history.index or len(history.loc[history.index < SESSION]) == 0:
        raise ValueError("An explicit factor event on the corrected date and its predecessor are required")
    prior = history.loc[history.index < SESSION].iloc[-1]
    current = history.loc[SESSION]
    prior_factor, current_factor = _positive(prior.backAdjustFactor), _positive(current.backAdjustFactor)
    if current_factor == prior_factor:
        raise ValueError("A non-changing factor cannot justify a price revision")
    for day in (PREVIOUS_SESSION, SESSION, CONFIRMATION_END):
        eligible = history.loc[history.index <= day]
        if eligible.empty:
            raise ValueError("Independent factor history does not cover a confirmation day")
        effective = _positive(eligible.iloc[-1].backAdjustFactor)
        for field in PRICE_FIELDS:
            if _positive(new_hfq.loc[day, field]) != _positive(new_raw.loc[day, field]) * effective:
                raise ValueError("Fresh adjusted prices must equal raw times the official effective factor exactly")
    for field in PRICE_FIELDS:
        if _positive(old_row[field]) != _positive(raw.loc[SESSION, field]) * prior_factor:
            raise ValueError("Old price differs from the documented previous-factor correction")
        if old_row[field] == new_row[field]:
            raise ValueError("Each declared corrected price field must change")
    revised = old_hfq.copy(deep=True)
    target = revised.date.eq(SESSION)
    if int(target.sum()) != 1:
        raise ValueError("Exactly one new adjusted row may be replaced")
    for field in PRICE_FIELDS:
        revised.loc[target, field] = new_row[field]
    return revised, {"instrument": inst, "date": SESSION, "prior_factor": str(prior_factor),
        "prior_factor_effective_date": prior.dividOperateDate, "official_factor": str(current_factor),
        "official_factor_effective_date": SESSION,
        "fields": {field: {"previous": old_row[field], "revised": new_row[field]} for field in PRICE_FIELDS},
        "raw_and_non_price_fields_unchanged": True, "all_prior_adjusted_rows_unchanged": True,
        "exact_decimal_reconstruction": True}


def load_verified_revision_batch(revision_contract_path):
    """Return ``(CanonicalIncrement, {name: file_proof})`` without any writes.

    Original network captures retain their own generation, source day and
    guards. The returned batch is an explicitly derived, later-known revision.
    It is not a publication and is never permission to backfill a paper close.
    """
    contract_proof = _file_proof(revision_contract_path)
    contract = json.loads(Path(contract_proof["path"]).read_bytes())
    _validate_contract(contract)
    proofs = {key: _proof(contract[key]) for key in ("previous_capture", "confirmation_capture",
        "factor_verification_report", "initial_snapshot_manifest", "previous_snapshot_manifest",
        "previous_refresh_result", "calendar")}
    proofs["revision_contract"] = contract_proof
    initial, previous, increment = _load_snapshots(contract, proofs)
    old_path = Path(proofs["previous_capture"]["path"])
    old = json.loads(old_path.read_bytes())
    instruments = old.get("instruments", [])
    keys = {"stock_basic", *[inst+"_"+mode for inst in instruments for mode in ("raw", "hfq")]}
    if (old.get("kind") != "low_turnover_guarded_daily_originals" or old.get("source") != "baostock"
            or old.get("status") != "complete" or old.get("failures") or old.get("session") != SESSION
            or old.get("generation") != increment.get("generation") or len(instruments) != 1300
            or instruments != sorted(set(instruments)) or not set(REVISED_INSTRUMENTS).issubset(instruments)
            or set(instruments) != set(increment.get("securities", {})) or set(old.get("responses", {})) != keys):
        raise ValueError("Original full 1,300-security generation is incomplete or differs from its publication")
    confirmation = json.loads(Path(proofs["confirmation_capture"]["path"]).read_bytes())
    if (confirmation.get("kind") != "a15_guarded_factor_revision_evidence" or confirmation.get("source") != "baostock"
            or confirmation.get("status") != "complete" or confirmation.get("failures")
            or confirmation.get("symbols") != list(REVISED_INSTRUMENTS)
            or confirmation.get("queries_attempted") != 15 or confirmation.get("request_limit") != 15
            or confirmation.get("actual_capture_day") != "2026-10-22"
            or confirmation.get("generation") in (old["generation"], contract["derived_generation"])
            or set(confirmation.get("responses", {})) != {i+"_"+m for i in REVISED_INSTRUMENTS for m in ("factors", "raw", "hfq")}):
        raise ValueError("Independent three-query-per-security confirmation is incomplete")
    if _instant(contract["created_at"]) < _instant(confirmation["completed_at"]):
        raise ValueError("Revision declaration cannot predate its supporting facts")
    report = json.loads(Path(proofs["factor_verification_report"]["path"]).read_bytes())
    if (report.get("kind") != "a15_official_factor_revision_verification"
            or report.get("status") != "supports_five_explicit_effective_date_corrections"
            or report.get("fresh_generation") != confirmation["generation"]
            or report.get("all_terminal_guards_verified") is not True
            or report.get("all_60_OHLC_values_match_fresh_HFQ_exactly_in_Decimal") is not True):
        raise ValueError("Independent factor verification report differs")
    for key, record in report.get("bound_inputs", {}).items():
        proofs["factor_report:"+key] = _proof(record)
    if proofs["confirmation_capture"] != proofs.get("factor_report:fresh_capture"):
        raise ValueError("Factor report does not bind the exact independent capture")
    for key in ("collector", "identity_source"):
        proofs["confirmation:"+key] = _proof(confirmation[key])
    if proofs["confirmation:identity_source"] != proofs.get("factor_report:identity_source"):
        raise ValueError("Factor report security identity proof differs")
    identity_payload = json.loads(Path(proofs["confirmation:identity_source"]["path"]).read_bytes())
    if identity_payload.get("fields") != ["code", "code_name", "ipoDate", "outDate", "type", "status"]:
        raise ValueError("Confirmation IPO identity schema differs")
    identity = pd.DataFrame(identity_payload["rows"], columns=identity_payload["fields"])
    if identity.code.duplicated().any():
        raise ValueError("IPO identity registry is ambiguous")
    identity = identity.set_index("code")
    from ..ashare_low_turnover_provider import _read_original
    _, basic_proofs = _read_original(old_path.parent, old, "stock_basic")
    for n, proof in enumerate(basic_proofs):
        proofs[f"original:stock_basic:{n}"] = proof
    raw_parts, old_hfq_parts, revised_parts, corrections = [], [], [], []
    for inst in instruments:
        raw, raw_proofs = _read_original(old_path.parent, old, inst+"_raw")
        hfq, hfq_proofs = _read_original(old_path.parent, old, inst+"_hfq")
        for mode, values in (("raw", raw_proofs), ("hfq", hfq_proofs)):
            for n, proof in enumerate(values):
                proofs[f"original:{inst}:{mode}:{n}"] = proof
        revised = hfq
        if inst in REVISED_INSTRUMENTS:
            code = inst[:2].lower()+"."+inst[2:]
            ipo = iso_day(identity.loc[code, "ipoDate"])
            factors, bound = _read_confirmation(confirmation, inst+"_factors",
                {"code":code,"start_date":ipo,"end_date":confirmation["actual_capture_day"]}, FACTOR_FIELDS, daily=False)
            for n, proof in enumerate(bound):
                proofs[f"confirmation:{inst}:factors:{n}"] = proof
            fresh = {}
            for mode, flag in (("raw", "3"), ("hfq", "1")):
                request = {"code":code,"fields":EXECUTION_FIELDS,"start_date":PREVIOUS_SESSION,
                    "end_date":CONFIRMATION_END,"frequency":"d","adjustflag":flag}
                fresh[mode], bound = _read_confirmation(confirmation,inst+"_"+mode,request,EXECUTION_FIELDS.split(","),daily=True)
                for n, proof in enumerate(bound):
                    proofs[f"confirmation:{inst}:{mode}:{n}"] = proof
                if proofs.get(f"factor_report:{inst}:{mode}") != bound[0] or proofs.get(f"factor_report:{inst}:{mode}:guard") != bound[1]:
                    raise ValueError("Factor report confirmation proof differs")
            if (proofs.get(f"factor_report:{inst}:factors") != proofs[f"confirmation:{inst}:factors:0"]
                    or proofs.get(f"factor_report:{inst}:factors:guard") != proofs[f"confirmation:{inst}:factors:1"]):
                raise ValueError("Factor report factor proof differs")
            revised, correction = _verified_replacement(inst,raw,hfq,fresh["raw"],fresh["hfq"],factors)
            corrections.append(correction)
        raw_parts.append(raw.assign(instrument=inst))
        old_hfq_parts.append(hfq.assign(instrument=inst))
        revised_parts.append(revised.assign(instrument=inst))
    calendar = Calendar.load(proofs["calendar"]["path"])
    old_days = set((initial.dataset_dir/"calendars/day.txt").read_text().splitlines())
    first_day = min(old_days)
    rows = pd.DataFrame([{"date":day,"is_open":int(day in old_days) if first_day <= day <= PREVIOUS_SESSION else int(calendar.is_session(day))}
        for day in natural_days(old["start_date"], SESSION)])
    provenance = increment["calendar_provenance"]
    def build(adjusted, generation, evidence):
        return CanonicalIncrement.from_frames(pd.concat(raw_parts,ignore_index=True),pd.concat(adjusted,ignore_index=True),
            source="baostock",generation=generation,calendar_rows=rows,calendar_source=provenance["source"],
            calendar_source_url=provenance["source_url"],requested_start=old["start_date"],requested_end=SESSION,evidence=evidence)
    original = build(old_hfq_parts,old["generation"],increment["source_evidence"])
    validated, verified_calendar, _ = _validated_batch(original,POLICY)
    if (_sha(validated.to_csv(index=False,float_format="%.17g").encode()) != increment.get("source_bars_sha256")
            or _sha(json.dumps([[day,flag] for day,flag in verified_calendar.days],ensure_ascii=False,sort_keys=True,separators=(",", ":"),allow_nan=False).encode()) != provenance["rows_sha256"]):
        raise ValueError("Original canonical source bars/calendar do not reproduce the published increment")
    evidence = {"source":"baostock","source_url":"https://www.baostock.com/api/",
        "derived_effective_factor_revision":True,"network_capture_generation":False,
        "original_generation":old["generation"],"confirmation_generation":confirmation["generation"],
        "revision_known_at":contract["created_at"],"original_session":SESSION,
        "atomic_supplier_vintage_verified":False,"fixed_watchlist_not_current_index_membership":True,
        "parent_manifest_sha256":initial.manifest_sha256,"verified_factor_revision":contract_proof,
        "corrections":corrections,"bound_inputs":list({(p["path"],p["sha256"]):p for p in proofs.values()}.values()),
        "limitations":["Later-known price correction; no original-day observation or paper close is reconstructed.",
            "Factor arithmetic is not a shareholder cash-dividend ledger or evidence of the supplier's internal cause."]}
    batch = build(revised_parts,contract["derived_generation"],evidence)
    _validated_batch(batch,POLICY)
    return batch, proofs
