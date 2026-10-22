"""Explicit engineering revisions of an existing A9 account, never new capital.

The old gap remains a historical fact. A planned revision permits a current
close audit; only a newly committed, complete close can authorize future opens.
This module contains no downloader, trainer, predictor, or execution entry point.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

from stopat30m import ashare_forward as legacy

DECLARATION_KIND = "a15_low_turnover_engineering_recovery_declaration"
EVENT_KIND = "a15_low_turnover_engineering_recovery_event"
TRANSACTION = ".engineering-recovery-transaction.json"
STATE_KEY = "engineering_recovery"
REVISION_KEY = "engineering_revision"
_ROOT = Path(__file__).resolve().parent
# These are engineering boundaries. Native execution, training, features,
# processors, candidate adapter and historical model evidence are immutable.
_UPDATABLE = {_ROOT / name for name in (
    "ashare_forward.py", "ashare_low_turnover_forward.py",
    "ashare_low_turnover_provider.py", "data/qlib_incremental.py",
    "data/ashare_daily_update.py")}


def _read(path):
    value = json.loads(Path(path).read_bytes())
    if not isinstance(value, dict):
        raise ValueError("A saved JSON object is required")
    return value


def _sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _proof(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": _sha(path)}


def _check_proof(proof, *, within=None):
    if not isinstance(proof, dict) or not re.fullmatch(r"[a-f0-9]{64}", str(proof.get("sha256"))):
        raise ValueError("An exact saved-file proof is required")
    original = Path(proof.get("path", ""))
    path = original.resolve()
    if (not original.is_absolute() or original.is_symlink() or not path.is_file()
            or within is not None and not path.is_relative_to(Path(within).resolve())
            or _sha(path) != proof["sha256"]):
        raise ValueError("Recovery evidence bytes/path differ: " + str(path))
    return path


def _proof_list(items):
    if not isinstance(items, list) or not items:
        raise ValueError("Nonempty original evidence proofs are required")
    for item in items:
        _check_proof(item)


def _clock(now=None):
    value = now if now is not None else datetime.now(timezone.utc)
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Recovery requires an actual timezone-aware instant")
    return value.astimezone(timezone.utc)


def _day(value):
    from datetime import date
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("A canonical actual-session date is required")
    return value


def _valid_digest(value, field):
    return value.get(field) == legacy._digest({k: v for k, v in value.items() if k != field})


def _archived_paper(before):
    before = Path(before).resolve()
    choices = [p for p in (before, before / "paper") if (p / "manifest.json").is_file()]
    if len(choices) != 1:
        raise ValueError("One unambiguous archived paper is required")
    return choices[0]


def _verify_audits(declaration, manifest):
    audit = _read(_check_proof(declaration["offline_audit"]))
    pool = manifest["initial_universe"]
    if (audit.get("kind") != "a15_offline_full_pool_seam_audit" or audit.get("status") != "passed"
            or audit.get("instrument_count") != 1300 or len(pool) != 1300
            or audit.get("instruments_sha256") != legacy._digest(pool)
            or audit.get("session") != declaration["resume_not_before_session"]
            or audit.get("strict_source_validation") is not True
            or audit.get("seam_tolerance_relaxed") is not False):
        raise ValueError("The complete fixed-pool strict offline seam audit is required")
    _proof_list(audit.get("source_proofs"))
    tests = _read(_check_proof(declaration["test_report"]))
    if (tests.get("kind") != "a15_offline_recovery_tests" or tests.get("status") != "passed"
            or type(tests.get("test_count")) is not int or tests["test_count"] < 1
            or tests.get("network_called") is not False):
        raise ValueError("Passing isolated offline recovery tests are required")
    tested = {path: value["after_sha256"] for path, value in declaration.get("authorized_code_updates", {}).items()}
    tested.update(declaration.get("added_implementation_proofs", {}))
    if tests.get("implementation_sha256") != tested:
        raise ValueError("Passing tests must bind the exact revised engineering implementations")
    _proof_list(tests.get("evidence"))
    _verify_source_repair(declaration, audit)


def _verify_source_repair(declaration, audit):
    proof = declaration.get("source_repair")
    if proof is None:
        return None
    value = _read(_check_proof(proof))
    if (value.get("kind") != "low_turnover_union_close_increment"
            or not _valid_digest(value, "result_sha256")
            or _day(value["session"]) >= declaration["resume_not_before_session"]):
        raise ValueError("Only a separately verified earlier source refresh may repair a parent")
    previous = _read(_check_proof(value["supersedes"]))
    _check_proof(value["repair_contract"])
    if previous.get("kind") != value["kind"] or not _valid_digest(previous, "result_sha256"):
        raise ValueError("The unchanged superseded source refresh must remain verifiable")
    allowed = {"snapshot_path", "snapshot_manifest_sha256", "bound_inputs", "result_sha256",
               "supersedes", "repair_contract", "source_repair_schema_version"}
    if {k:v for k,v in value.items() if k not in allowed} != {k:v for k,v in previous.items() if k not in allowed}:
        raise ValueError("Parent source correction cannot alter raw quotes, metadata, pool or strict bridge policy")
    current_inputs = value.get("bound_inputs")
    _proof_list(current_inputs)
    if not {legacy._digest(item) for item in previous.get("bound_inputs", [])}.issubset({legacy._digest(item) for item in current_inputs}):
        raise ValueError("Every superseded source original proof must remain bound")
    snapshot = Path(value["snapshot_path"]).resolve()
    if snapshot == Path(previous["snapshot_path"]).resolve():
        raise ValueError("Publish a corrected sibling; never replace the original source snapshot")
    manifest_proof = _proof(snapshot / "manifest.json")
    if (manifest_proof["sha256"] != value["snapshot_manifest_sha256"]
            or audit.get("corrected_snapshot") != manifest_proof):
        raise ValueError("The complete offline audit must bind the actual corrected snapshot")
    return proof


def _load_event(state):
    recovery = state.get(STATE_KEY)
    if recovery is None:
        return None
    if not isinstance(recovery, dict) or set(recovery) != {"event", "activation"}:
        raise ValueError("Malformed engineering recovery state")
    event = _read(_check_proof(recovery["event"]))
    if event.get("kind") != EVENT_KIND or not _valid_digest(event, "event_sha256"):
        raise ValueError("Engineering event checksum differs")
    declaration = _read(_check_proof(event["declaration"]))
    if (declaration.get("kind") != DECLARATION_KIND or declaration.get("schema_version") != 1
            or event["gap_sessions"] != declaration["gap_sessions"]
            or event["resume_not_before_session"] != declaration["resume_not_before_session"]
            or event.get("source_repair") != declaration.get("source_repair")):
        raise ValueError("Recovery event/declaration identity differs")
    before = _read(_check_proof(event["before_manifest"]))
    _check_proof(event["before_state"])
    _verify_audits(declaration, before)
    for proof in declaration["archived_sources"].values():
        _check_proof(proof, within=event["before_directory"])
    for day in event["gap_sessions"]:
        if legacy._digest(state.get("observation_gaps", {}).get(day)) != event["gap_digests"][day]:
            raise ValueError("Historical observation-gap facts cannot be edited or cleared")
    return event


def source_repair(state):
    """Return the explicitly bound corrected-parent proof for provider loading."""
    event = _load_event(state)
    return deepcopy(event.get("source_repair")) if event else None


def previous_observed_close(state, batch, instrument, previous_session):
    """Read an evidenced prior raw close for the first gap-recovery comparison.

    An execution-time live mark is not a saved daily close. This narrow helper
    returns the actual preceding close from the *current* guarded daily response;
    it never changes a mark, inserts an old valuation, or settles an entitlement.
    The close engine retains its normal preclose/corporate-action comparison.
    """
    from stopat30m.ashare_low_turnover_provider import _read_original
    from stopat30m.data.research_calendar import Calendar

    event = _load_event(state)
    session, previous_session = _day(batch["session"]), _day(previous_session)
    if (event is None or state[STATE_KEY]["activation"] is not None
            or batch.get("phase") != "close" or previous_session not in event["gap_sessions"]
            or session < event["resume_not_before_session"] or pending_gaps(state, close_session=session)):
        raise ValueError("An explicit first recovery close with its unchanged preceding gap is required")
    if any(value.get("phase") == "close" and value.get("status") == "completed"
           and value.get("session", "") >= event["resume_not_before_session"]
           for value in state.get("completed", {}).values()):
        raise ValueError("Prior raw close substitution is limited to the first recovery close")
    if not re.fullmatch(r"(?:SH|SZ)\d{6}", str(instrument)):
        raise ValueError("An explicit canonical held instrument is required")
    position = state.get("accounts", {}).get("model", {}).get("positions", {}).get(instrument)
    if (not position or position.get("last_mark_basis") != "live_observed_quote"
            or position.get("last_mark_date") != previous_session
            or not isinstance(position.get("quantity"), int) or position["quantity"] <= 0):
        raise ValueError("Only an actual held live execution mark can use the audited prior close")
    archived = _archived_paper(event["before_directory"])
    original_manifest = _read(_check_proof(event["before_manifest"]))
    calendar_path = archived / "calendar.json"
    if _sha(calendar_path) != original_manifest["calendar_sha256"]:
        raise ValueError("The original recovery calendar differs")
    if Calendar.load(calendar_path).previous_session(session) != previous_session:
        raise ValueError("The prior close must be the preceding verified exchange session")
    quote = batch.get("quotes", {}).get(instrument, {})
    if (quote.get("source") != "baostock.daily_raw" or quote.get("session") != session
            or quote.get("instrument", instrument) != instrument
            or not re.fullmatch(r"[a-f0-9]{64}", str(quote.get("source_response_sha256")))):
        raise ValueError("The current raw quote must bind its exact source response")
    evidence = batch.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        raise ValueError("Current original daily source evidence is required")
    raw_candidates = [item for item in evidence if item.get("sha256") == quote["source_response_sha256"]]
    if len(raw_candidates) != 1:
        raise ValueError("One exact current raw-response proof is required")
    raw_path = _check_proof(raw_candidates[0])
    key = instrument + "_raw"
    captures = []
    for item in evidence:
        if Path(item.get("path", "")).name != "capture.json":
            continue
        capture_path = _check_proof(item)
        capture = _read(capture_path)
        record = capture.get("responses", {}).get(key, {})
        if (capture.get("kind") == "low_turnover_guarded_daily_originals"
                and capture.get("session") == session
                and record.get("sha256") == raw_candidates[0]["sha256"]
                and (capture_path.parent / record.get("file", "")).resolve() == raw_path):
            captures.append((capture_path, capture))
    if len(captures) != 1:
        raise ValueError("One matching current complete capture must bind the raw response")
    capture_path, capture = captures[0]
    symbols = capture.get("instruments", [])
    if (capture.get("source") != "baostock" or capture.get("status") != "complete"
            or capture.get("failures") != {} or not isinstance(symbols, list)
            or symbols != sorted(set(symbols)) or instrument not in symbols
            or any(not re.fullmatch(r"(?:SH|SZ)\d{6}", str(name)) for name in symbols)
            or set(capture.get("responses", {})) != {"stock_basic", *[name + "_" + mode for name in symbols for mode in ("raw", "hfq")]}):
        raise ValueError("The current daily capture inventory is incomplete or ambiguous")
    frame, consumed = _read_original(capture_path.parent, capture, key)
    bound = {(item.get("path"), item.get("sha256")) for item in evidence}
    if any((item["path"], item["sha256"]) not in bound for item in consumed):
        raise ValueError("The current raw response and terminal guard must both be bound")
    if previous_session not in frame.date.tolist() or session not in frame.date.tolist():
        raise ValueError("Both preceding and current actual source sessions are required")
    current = frame.loc[frame.date == session]
    if len(current) != 1:
        raise ValueError("One current actual source row is required")
    for field in ("close", "preclose"):
        observed = _decimal(current.iloc[0][field])
        if observed <= 0 or _decimal(quote.get(field)) != observed:
            raise ValueError("Current quote close/preclose differs from its bound raw source")
    previous = frame.loc[frame.date == previous_session]
    if len(previous) != 1 or previous.iloc[0]["tradestatus"] != "1":
        raise ValueError("The preceding raw close must be a unique observed trading row")
    value = _decimal(previous.iloc[0]["close"])
    if value <= 0:
        raise ValueError("The preceding observed raw close must be finite and positive")
    return value


def _activation_valid(state, event):
    value = state[STATE_KEY]["activation"]
    if value is None:
        return False
    if not isinstance(value, dict) or not _valid_digest(value, "activation_sha256"):
        raise ValueError("Recovery activation checksum differs")
    session = value["session"]
    if (session < event["resume_not_before_session"] or session <= max(event["gap_sessions"])
            or _clock(value["observed_at"]) < _clock(event["declared_at"])):
        raise ValueError("Recovery cannot backdate an observed close")
    signal = state.get("signals", {}).get(session)
    completed = state.get("completed", {}).get(session + "_close")
    if (not signal or not completed or completed.get("status") != "completed"
            or signal.get("signal_sha256") != value["signal_sha256"]
            or signal.get("signal_sha256") != legacy._digest({k: v for k, v in signal.items() if k != "signal_sha256"})
            or completed.get("signal_sha256") != value["signal_sha256"]
            or signal.get("created_at") != value["observed_at"]
            or signal.get("model_sha256") != event["model_sha256"]
            or completed.get("recorded_at") != value["observed_at"]
            or legacy._digest(completed.get("valuations")) != value["valuations_sha256"]):
        raise ValueError("Only an actual committed new complete close can activate recovery")
    receipt_path = _check_proof({"path": completed["receipt_path"], "sha256": completed["receipt_sha256"]})
    actual = _read(receipt_path)
    trusted = {(item["path"], item["sha256"]) for item in state.get("receipts", [])}
    if ((str(receipt_path), completed["receipt_sha256"]) not in trusted
            or actual != {k: v for k, v in completed.items() if k not in {"receipt_path", "receipt_sha256"}}):
        raise ValueError("Recovery close differs from its committed receipt")
    if not any(row.get("session") == session and row.get("observed_at") == value["observed_at"]
               and legacy._digest(row.get("accounts")) == value["valuations_sha256"]
               for row in state.get("valuations", [])):
        raise ValueError("The activating close valuation is missing")
    if any(not row.get("valuation_complete") for row in completed["valuations"].values()):
        raise ValueError("Partial held valuation cannot activate recovery")
    _proof_list(value["source_evidence"])
    _check_proof(value["rights_collection"])
    return True


def pending_gaps(state, *, close_session=None):
    """Return unresolved gaps; preserve every original entry in state.

    A planned event can authorize only a prospective close audit. Default callers
    (opens/reports) still block until that close is committed and independently
    bound to an activation. A subsequently discovered gap is never covered.
    """
    gaps = deepcopy(state.get("observation_gaps", {}))
    event = _load_event(state)
    if event is None:
        return gaps
    active = _activation_valid(state, event)
    planned_close = (close_session is not None and _day(close_session) >= event["resume_not_before_session"]
                     and close_session > max(event["gap_sessions"]))
    if active or planned_close:
        for day in event["gap_sessions"]:
            gaps.pop(day, None)
    return gaps


def verify_recovery_binding(paper_dir, manifest, state):
    """Fail closed during a two-file migration or on a foreign/rebound event."""
    root = Path(paper_dir).resolve()
    if (root / TRANSACTION).exists():
        raise ValueError("Interrupted engineering transaction requires explicit audited recovery")
    revision = manifest.get(REVISION_KEY)
    if revision is None and STATE_KEY not in state:
        return
    event = _load_event(state)
    if (event is None or not isinstance(revision, dict) or revision.get("event") != state[STATE_KEY]["event"]
            or event["paper_dir"] != str(root) or revision.get("parent_manifest") != event["before_manifest"]
            or state.get("manifest_sha256") != manifest.get("manifest_sha256")):
        raise ValueError("Engineering revision/state lineage differs")
    old = _read(_check_proof(event["before_manifest"]))
    changed = {"implementation_sha256", "candidate_proofs", "manifest_sha256", REVISION_KEY}
    if {k: v for k, v in manifest.items() if k not in changed} != {k: v for k, v in old.items() if k not in changed}:
        raise ValueError("Engineering recovery cannot change model, capital, policy, calendar or universe")
    if manifest["implementation_sha256"] != event["new_implementation_sha256"]:
        raise ValueError("Revision implementation differs from its frozen event")
    if manifest["candidate_proofs"] != event["new_candidate_proofs"]:
        raise ValueError("Revision candidate proof changes were not authorized")
    for path, digest in event["new_implementation_sha256"].items():
        _check_proof({"path": path, "sha256": digest})
    if state[STATE_KEY]["activation"] is not None:
        _activation_valid(state, event)


def _rights_collection(batch, held, session):
    """Verify current guarded dividend coverage, including successful empties."""
    from stopat30m.data.ashare_actions import load_dividend_evidence, dividend_events, SOURCE
    from stopat30m.ashare_low_turnover_provider import _validate_guard
    evidence = batch.get("evidence")
    _proof_list(evidence)
    candidates = []
    for item in evidence:
        path = Path(item["path"])
        if path.name == "collection.json":
            value = _read(path)
            if value.get("source") == SOURCE:
                candidates.append((item, path))
    if len(candidates) != 1:
        raise ValueError("One fresh held-rights collection is required before operational recovery")
    proof, collection = candidates[0]
    _, inventory = load_dividend_evidence(collection.parent)
    if (inventory.get("as_of_date") != session or sorted(inventory["instruments"]) != sorted(held)
            or inventory["years"] != [int(session[:4])]):
        raise ValueError("Current complete held-rights coverage differs")
    all_proofs = {(item["path"], item["sha256"]) for item in evidence}
    guards_by_request = {}
    guard_fields = {"request", "server_echo", "server_error_code", "status", "rows_verified",
                    "started_at", "finished_at", "complete_row_iteration_observed"}
    identity_fields = {"code", "year", "yearType"}
    for item in evidence:
        path = Path(item["path"])
        if path.suffix != ".json":
            continue
        value = _read(path)
        request = value.get("request")
        # The frozen ValidatedBaostockDividendClient emits no kind/query.
        # A raw response also has request, but none of the guard-only fields.
        guard_like = bool((guard_fields - {"request"}).intersection(value) or {"kind", "query"}.intersection(value))
        dividend_like = (isinstance(request, dict) and bool({"year", "yearType"}.intersection(request))
                         or value.get("query") == "query_dividend_data"
                         or not {"kind", "query"}.intersection(value)
                         and bool({"server_echo", "rows_verified"}.intersection(value)))
        if not guard_like or not dividend_like:
            continue
        if (not isinstance(request, dict) or set(request) != identity_fields
                or not re.fullmatch(r"(?:sh|sz)\.\d{6}", str(request.get("code")))
                or not isinstance(request.get("year"), str) or not re.fullmatch(r"\d{4}", request["year"])
                or request.get("yearType") != "operate" or not guard_fields.issubset(value)):
            raise ValueError("Exact original dividend guard schema/request identity is required")
        marked = "kind" in value or "query" in value
        if (marked and (value.get("kind") != "baostock_metadata_server_echo_guard"
                        or value.get("query") != "query_dividend_data")):
            raise ValueError("Dividend guard kind/query markers differ")
        if not marked and set(value) != guard_fields:
            raise ValueError("Unmarked dividend evidence must use the exact frozen SDK guard schema")
        guards_by_request.setdefault(legacy._digest(request), []).append(item)
    for record in inventory["responses"].values():
        source = collection.parent / record["file"]
        if (str(source.resolve()), record["sha256"]) not in all_proofs:
            raise ValueError("Held-rights response is absent from the current close evidence")
        payload = _read(source)
        guards = guards_by_request.get(legacy._digest(payload["request"]), [])
        if len(guards) != 1:
            raise ValueError("Exact terminal held-rights source guard is required")
        _validate_guard(guards[0], payload, daily=False)
    return proof, dividend_events(collection.parent, start="2000-01-01", end=session)


def activate_after_close(state, *, receipt, signal, batch, now=None):
    """Append activation metadata after a genuine current close; no old posting.

    The runner commits this working state and its close receipt atomically under
    its existing phase lock. Until that receipt is committed default gates reject
    the activation. Exceptions discard the working copy, as normal phase errors.
    """
    event = _load_event(state)
    if event is None or state[STATE_KEY]["activation"] is not None:
        return
    clock = _clock(now)
    session = _day(batch["session"])
    if (session < event["resume_not_before_session"] or session <= max(event["gap_sessions"])
            or clock < _clock(event["declared_at"]) or signal.get("created_at") != clock.isoformat()
            or clock.astimezone(legacy.SHANGHAI).date().isoformat() != session
            or signal.get("signal_sha256") != legacy._digest({k: v for k, v in signal.items() if k != "signal_sha256"})
            or batch.get("phase") != "close" or signal.get("signal_date") != session
            or signal.get("model_sha256") != event["model_sha256"]
            or receipt.get("status") != "completed" or pending_gaps(state, close_session=session)):
        raise ValueError("A genuine new complete audited close is required for activation")
    if (not receipt.get("valuations") or any(not row.get("valuation_complete") for row in receipt["valuations"].values())
            or any(state.get("unresolved_actions", {}).values())
            or not all(signal.get("allowed", {}).values())):
        raise ValueError("Unknown held valuation or rights cannot activate future orders")
    held = set().union(*(legacy._account_instruments(a) for a in state["accounts"].values()))
    rights_proof, events = _rights_collection(batch, held, session)
    original = _read(event["before_manifest"]["path"])
    initialized = _clock(original["created_at"]).astimezone(legacy.SHANGHAI).date().isoformat()
    for row in events:
        if row["instrument"] not in held or not min(event["gap_sessions"]) <= row["ex_date"] < session:
            continue
        for account in state["accounts"].values():
            recorded = account.get("holdings_history", {}).get(row.get("record_date"))
            existing = account.get("entitlements", {})
            relevant_claim = any(claim.get("instrument") == row["instrument"]
                and claim.get("created_date", "9999") <= row.get("record_date", "")
                for claim in account.get("claims", {}).values())
            if row.get("record_date", "9999") < initialized:
                continue
            components = [row["event_id"] + ":" + kind for kind, field in
                (("cash", "cash_per_share"), ("bonus", "bonus_ratio")) if _decimal(row.get(field, 0)) > 0]
            if components and all(identity in existing for identity in components):
                continue  # Already evidenced on time; never register or settle again here.
            if (recorded is not None and recorded.get(row["instrument"], 0) == 0 and not relevant_claim
                    and not any(row["event_id"] + ":" + kind in existing for kind in ("cash", "bonus"))):
                continue  # An explicit original zero record-date ownership is evidence.
            raise ValueError("A missed held ex-date requires a separate entitlement audit, never retrospective posting")
    activation = {"session": session, "observed_at": clock.isoformat(),
        "signal_sha256": signal["signal_sha256"], "valuations_sha256": legacy._digest(receipt["valuations"]),
        "source_evidence": deepcopy(batch["evidence"]), "rights_collection": deepcopy(rights_proof),
        "historical_gap_unchanged": True, "historical_fills_or_NAV_added": False}
    activation["activation_sha256"] = legacy._digest(activation)
    state[STATE_KEY]["activation"] = activation


def _decimal(value):
    from stopat30m.backtest.ashare_execution import decimal
    return decimal(value)


def migrate_recovery(paper_dir, *, before_directory, declaration_path, now=None):
    """Same-account explicit revision. Never run against the live paper in tests."""
    root, before = Path(paper_dir).resolve(), Path(before_directory).resolve()
    declaration_path = Path(declaration_path).resolve()
    declaration_proof = _proof(declaration_path)
    declaration = _read(declaration_path)
    clock = _clock(now)
    if (declaration.get("kind") != DECLARATION_KIND or declaration.get("schema_version") != 1
            or declaration.get("paper_dir") != str(root)
            or _clock(declaration["declared_at"]) > clock):
        raise ValueError("An actual predeclared same-paper engineering revision is required")
    with legacy._lock(root):
        if (root / TRANSACTION).exists():
            raise ValueError("Interrupted engineering transaction cannot be silently resumed")
        current_manifest, current_state = _read(root / "manifest.json"), _read(root / "state.json")
        if REVISION_KEY in current_manifest:
            event = _load_event(current_state)
            if event is None or event["declaration"] != declaration_proof:
                raise ValueError("A different engineering revision already exists")
            verify_recovery_binding(root, current_manifest, current_state)
            return {"status": "already_migrated", "event": current_state[STATE_KEY]["event"]}
        archived = _archived_paper(before)
        old, old_state = _read(archived / "manifest.json"), _read(archived / "state.json")
        if (old.get("kind") != "isolated_ashare_low_turnover_forward"
                or old.get("policy") != "a9-prospective-frozen-a5-H5-hold10-cap2-v1"
                or old.get("strategies") != ["model"]
                or any(old.get("strategy", {}).get(key) != value for key, value in
                       {"top_k": 20, "keep_rank": 40, "minimum_hold_sessions": 10,
                        "max_replacements": 2, "rebalance_sessions": 5}.items())
                or not re.fullmatch(r"[a-f0-9]{64}", str(old.get("model_sha256")))
                or not any(proof.get("sha256") == old["model_sha256"] for proof in old.get("candidate_proofs", {}).values())):
            raise ValueError("Only the unchanged original A9 model/holding policy can receive this engineering revision")
        pool = old.get("initial_universe", [])
        if (pool != sorted(set(pool)) or len(pool) != 1300
                or any(not re.fullmatch(r"(?:SH|SZ)\d{6}", name) for name in pool)):
            raise ValueError("The unchanged official fixed 1300-name anchor is required")
        if (not _valid_digest(old, "manifest_sha256") or not _valid_digest(old_state, "state_sha256")
                or old_state["manifest_sha256"] != old["manifest_sha256"]
                or current_manifest != old or current_state != old_state
                or _sha(root / "manifest.json") != _sha(archived / "manifest.json")
                or _sha(root / "state.json") != _sha(archived / "state.json")):
            raise ValueError("The exact frozen pre-migration account and declaration must be archived")
        for name, expected in (("calendar.json", old["calendar_sha256"]), ("universe.json", old["universe_sha256"])):
            if _sha(root / name) != expected or _sha(archived / name) != expected:
                raise ValueError("Original calendar/universe differs")
        from stopat30m.data.research_calendar import Calendar
        Calendar.load(root / "calendar.json")
        if (_sha(root / "adapter.json") != _sha(archived / "adapter.json")
                or legacy._digest(_read(root / "adapter.json")) != old["adapter_sha256"]):
            raise ValueError("The original provider adapter cannot be replaced")
        for receipt in old_state["receipts"]:
            path = _check_proof(receipt)
            if not path.is_relative_to(root) or _sha(archived / path.relative_to(root)) != receipt["sha256"]:
                raise ValueError("Every original receipt must be archived byte-for-byte")
        sources = declaration.get("archived_sources", {})
        if set(sources) != set(old["implementation_sha256"]):
            raise ValueError("Archive every frozen implementation before changing engineering code")
        for path, digest in old["implementation_sha256"].items():
            _check_proof(sources[path], within=before)
            if sources[path]["sha256"] != digest:
                raise ValueError("Archived original implementation differs")
        gaps = declaration["gap_sessions"]
        if (not isinstance(gaps, list) or not gaps or gaps != sorted(set(gaps))
                or any(_day(day) not in old_state.get("observation_gaps", {}) for day in gaps)
                or _day(declaration["resume_not_before_session"]) <= max(gaps)):
            raise ValueError("Explicit existing gap identities and a later prospective close are required")
        _verify_audits(declaration, old)
        updates = declaration.get("authorized_code_updates", {})
        new_implementations = deepcopy(old["implementation_sha256"])
        actual_changed = {path for path, digest in new_implementations.items() if _sha(path) != digest}
        if set(updates) != actual_changed:
            raise ValueError("Every changed engineering source must be explicitly authorized, with no extras")
        for path, value in updates.items():
            if (Path(path) not in _UPDATABLE or value != {"before_sha256": new_implementations[path], "after_sha256": _sha(path)}):
                raise ValueError("Training, model, feature or execution-policy code cannot be rebound")
            new_implementations[path] = value["after_sha256"]
        additions = declaration.get("added_implementation_proofs", {})
        if str(Path(__file__).resolve()) not in additions:
            raise ValueError("The recovery verifier itself must be frozen")
        for path, digest in additions.items():
            if path in new_implementations or not Path(path).resolve().is_relative_to(_ROOT):
                raise ValueError("New engineering dependency must be an explicit repository module")
            _check_proof({"path": path, "sha256": digest})
            new_implementations[path] = digest
        for path, digest in new_implementations.items():
            _check_proof({"path": path, "sha256": digest})
        candidate_proofs = deepcopy(old["candidate_proofs"])
        for proof in candidate_proofs.values():
            if proof["path"] in updates:
                if proof["sha256"] != updates[proof["path"]]["before_sha256"]:
                    raise ValueError("Original inference engineering proof differs")
                proof["sha256"] = updates[proof["path"]]["after_sha256"]
            _check_proof(proof)
        event = {"kind": EVENT_KIND, "schema_version": 1, "declared_at": clock.isoformat(),
            "paper_dir": str(root), "declaration": declaration_proof, "before_directory": str(before),
            "before_manifest": _proof(archived / "manifest.json"), "before_state": _proof(archived / "state.json"),
            "gap_sessions": gaps, "gap_digests": {day: legacy._digest(old_state["observation_gaps"][day]) for day in gaps},
            "resume_not_before_session": declaration["resume_not_before_session"],
            "model_sha256": old["model_sha256"], "new_implementation_sha256": new_implementations,
            "source_repair": deepcopy(declaration.get("source_repair")),
            "new_candidate_proofs": candidate_proofs,
            "capital_positions_cash_clock_phase_and_old_records_changed": False,
            "historical_close_or_fill_backfilled": False}
        event["event_sha256"] = legacy._digest(event)
        event_dir = root / "recoveries" / declaration_proof["sha256"][:24]
        if event_dir.exists():
            raise ValueError("Preserve an incomplete/previous recovery event; do not overwrite")
        event_dir.mkdir(parents=True)
        legacy._write(event_dir / "event.json", event)
        event_proof = _proof(event_dir / "event.json")
        manifest = deepcopy(old)
        manifest["implementation_sha256"] = new_implementations
        manifest["candidate_proofs"] = candidate_proofs
        manifest[REVISION_KEY] = {"kind": EVENT_KIND, "event": event_proof, "parent_manifest": event["before_manifest"]}
        manifest["manifest_sha256"] = legacy._digest({k: v for k, v in manifest.items() if k != "manifest_sha256"})
        state = deepcopy(old_state)
        state["manifest_sha256"] = manifest["manifest_sha256"]
        state[STATE_KEY] = {"event": event_proof, "activation": None}
        state["state_sha256"] = legacy._digest({k: v for k, v in state.items() if k != "state_sha256"})
        legacy._write(event_dir / "manifest.json", manifest)
        legacy._write(event_dir / "initial-state.json", state)
        # Readers explicitly reject this marker even if one replacement happened.
        marker = root / TRANSACTION
        with marker.open("x") as stream:
            json.dump({"event": event_proof, "staged_manifest": _proof(event_dir / "manifest.json"),
                       "staged_state": _proof(event_dir / "initial-state.json")}, stream, sort_keys=True)
            stream.flush()
            import os
            os.fsync(stream.fileno())
        legacy._write(root / "manifest.json", manifest)
        legacy._write(root / "state.json", state)
        marker.unlink()
        verify_recovery_binding(root, manifest, state)
        return {"status": "migrated_planned_awaiting_new_audited_close", "event": event_proof,
                "manifest": _proof(root / "manifest.json"), "state": _proof(root / "state.json")}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paper-dir", required=True, type=Path)
    parser.add_argument("--before-directory", required=True, type=Path)
    parser.add_argument("--declaration", required=True, type=Path)
    args = parser.parse_args(argv)
    result = migrate_recovery(args.paper_dir, before_directory=args.before_directory, declaration_path=args.declaration)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
