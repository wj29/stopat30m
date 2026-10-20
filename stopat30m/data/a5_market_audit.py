"""Source-only CSI300 / CSI1000 audits, separate from frozen A3/A4 experiments.

This module never computes a portfolio return. It joins independently captured
original collections, checks the dated membership view and emits unresolved
source facts before a model or an execution runner may use the union universe.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from contextlib import nullcontext
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from .a5_adjusted_prices import (
    DEFAULT_SPEC, audit_temporal_adjustments, load_partial_session_model_policy,
    matches_partial_model_exception,
)
from .a5_capital_tax import apply_a5_capital_tax
from .a5_source_checks import audit_daily_coverage, audit_qlib_availability, audit_reference_prices
from .ashare_actions import apply_action_overrides, dividend_events, load_dividend_evidence
from .ashare_execution_data import load_execution_collection
from .ashare_history_audit import (
    apply_intraday_observation_evidence, audit_capital_reserve_tax,
    audit_stock_dividend_tax,
)
from .ashare_scope import load_verified_historical_scope
from .qlib_incremental import read_qlib_security
from .qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot


_DAILY_FIELDS = ("code", "open", "high", "low", "close", "preclose", "volume",
                 "amount", "adjustflag", "tradestatus", "isST")
_IDENTITY_FIELDS = ("ipoDate", "outDate", "type")
_ECONOMIC_FIELDS = ("instrument", "record_date", "ex_date", "pay_date", "listing_date",
                    "cash_per_share", "stock_dividend_ratio", "capital_reserve_ratio",
                    "bonus_ratio", "known_at")
OFFICIAL_MEMBERSHIP_CHECKS = (
    "initial_historical_memberships", "all_regular_adjustments", "all_temporary_adjustments",
    "official_effective_dates", "derived_membership_matches_official_events",
    "all_exchange_sessions_covered", "no_current_backfill",
)
# Only this unchanged, previously audited A4 fact document retains its original
# verification contract. New A5 facts may not evade visual/source checks merely
# by omitting the newer fields.
LEGACY_CAPITAL_FACT_DOCUMENT_SHA256 = "3593ba6c90970591eea6069952b0788d3d3cc6cfab2de8363c68bf192f686f16"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _save(path, payload):
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2,
                                   sort_keys=True, allow_nan=False) + "\n")


def _day(value):
    if not isinstance(value, str) or pd.Timestamp(value).date().isoformat() != value:
        raise ValueError("A5 needs explicit ISO dates")
    return value


def _proof(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": sha(path)}


def _capture_runtime_proofs(paths):
    return {name: _proof(path) for name, path in paths.items()}


def _assert_bound_inputs_unchanged(proofs):
    if any(sha(proof["path"]) != proof["sha256"] for proof in proofs.values()):
        raise ValueError("Original A5 source or runtime implementation changed while auditing")


def _scope_batch(scope):
    # Real immutable scopes verify every original on entry and exit. Lightweight
    # fixture/adaptor scopes may retain their original per-call verification.
    return scope.verified_batch() if hasattr(scope, "verified_batch") else nullcontext()


def audit_original_dividend_request(path, key):
    """Reject a stale provider response from another security or operate year.

    A connection can retain an older response after a transport failure. The
    client-side request label alone is therefore not original event identity.
    Empty responses remain subject to the collector's fresh-connection receipt.
    """
    instrument, year = key.split("_")
    payload = json.loads(Path(path).read_text())
    expected = {"code": instrument[:2].lower() + "." + instrument[2:],
                "year": year, "yearType": "operate"}
    if payload.get("request") != expected:
        raise ValueError("Original dividend request identity differs")
    fields = payload.get("fields", [])
    if not {"code", "dividOperateDate"}.issubset(fields):
        raise ValueError("Original dividend response lacks echoed event identity")
    for values in payload.get("rows", []):
        if len(values) != len(fields):
            raise ValueError("Original dividend response field count differs")
        row = dict(zip(fields, values))
        operate_date = _day(row["dividOperateDate"])
        if row["code"] != expected["code"] or operate_date[:4] != year:
            raise ValueError("Original dividend server security/operate year differs from request")
    return {"request_identity_verified": True, "nonempty_operate_year_verified": True,
            "row_count": len(payload.get("rows", [])), "empty_response_is_not_server_echo_proof": not payload.get("rows")}


def audit_dividend_source_correction(directory, receipt_paths=()):
    """Verify the complete recapture transaction, including empty tail responses."""
    directory = Path(directory).resolve()
    manifest_path = directory / "collection.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("source_correction_required") is not True:
        return {}
    matching = []
    for path in receipt_paths:
        value = json.loads(Path(path).read_text())
        if value.get("source_collection_manifest", {}).get("path") == str(manifest_path):
            matching.append((Path(path).resolve(), value))
    if len(matching) != 1:
        raise ValueError("A corrected dividend source requires its unique completed correction receipt")
    path, correction = matching[0]
    proofs = {"correction_receipt": _proof(path)}

    def bind(source, label):
        original = Path(source["path"])
        if not original.is_absolute() or sha(original) != source["sha256"]:
            raise ValueError("Dividend correction original source bytes differ")
        proofs[label] = _proof(original)
        return original

    bound_manifest = bind(correction["source_collection_manifest"], "corrected_manifest")
    quarantine_path = bind(correction["quarantine_receipt"], "quarantine_receipt")
    quarantine = json.loads(quarantine_path.read_text())
    if (bound_manifest != manifest_path or correction.get("kind") != "a5_dividend_source_correction_receipt"
            or correction.get("status") != "passed"
            or correction.get("generation") != manifest.get("generation")
            or correction.get("source_as_of_date") != manifest.get("as_of_date")
            or quarantine.get("kind") != "a5_dividend_source_quarantine_receipt"
            or quarantine.get("status") != "quarantined_pending_strict_recapture"
            or quarantine.get("generation") != manifest.get("generation")
            or quarantine.get("source_as_of_date") != manifest.get("as_of_date")):
        raise ValueError("Dividend correction transaction identity/status differs")
    old_manifest_path = bind(quarantine["original_manifest"], "quarantined_original_manifest")
    bind(quarantine["original_progress"], "quarantined_original_progress")
    old_manifest = json.loads(old_manifest_path.read_text())
    selected = quarantine["selected_tail_keys"]
    all_original_keys = [f"{instrument}_{year}" for instrument in old_manifest["instruments"]
                         for year in old_manifest["years"]]
    first = quarantine["first_untrusted_query"]
    if (not selected or selected != all_original_keys[first["ordinal_1based"] - 1:]
            or selected[0] != first["key"] or len(set(selected)) != len(selected)
            or correction.get("corrected_response_keys") != selected):
        raise ValueError("Dividend correction must recapture the complete untrusted connection tail")
    corrected = correction.get("corrected_responses", [])
    if len(corrected) != len(selected) or {row["key"] for row in corrected} != set(selected):
        raise ValueError("Dividend correction response coverage differs")
    # Trusted bytes preceding the failed connection cannot silently be replaced.
    for key, record in old_manifest["responses"].items():
        if key not in selected and manifest["responses"].get(key) != record:
            raise ValueError("Dividend correction altered an unrelated original response")
    quarantined = {row["key"]: row for row in quarantine["quarantined_responses"]}
    for key, row in quarantined.items():
        bind({"path": row["quarantined_path"], "sha256": row["sha256"]}, "quarantined_response:" + key)
    quarantined_at = datetime.fromisoformat(quarantine["finished_at"])
    if quarantined_at.tzinfo is None:
        raise ValueError("Dividend quarantine needs an explicit timestamp")
    for row in corrected:
        key = row["key"]
        original = bind(row["new_response"], "corrected_response:" + key)
        record = manifest["responses"][key]
        if original != directory / record["file"] or row["new_response"]["sha256"] != record["sha256"]:
            raise ValueError("Dividend correction does not bind its canonical response")
        check = audit_original_dividend_request(original, key)
        payload = json.loads(original.read_text())
        guard_path = bind(row["server_echo_guard"], "server_echo_guard:" + key)
        guard = json.loads(guard_path.read_text())
        if (guard.get("status") != "verified" or guard.get("server_error_code") != "0"
                or guard.get("complete_row_iteration_observed") is not True
                or guard.get("request") != payload["request"] or guard.get("server_echo") != payload["request"]
                or row.get("server_echo") != payload["request"]
                or type(guard.get("rows_verified")) is not int or guard["rows_verified"] != check["row_count"]
                or row.get("rows_verified") != check["row_count"]
                or row.get("new_response_fetched_at") != payload["fetched_at"]):
            raise ValueError("Dividend correction lacks an exact successful server echo and complete row iteration")
        started, finished, fetched = [datetime.fromisoformat(value) for value in
            (guard["started_at"], guard["finished_at"], payload["fetched_at"])]
        if any(value.tzinfo is None for value in (started, finished, fetched)) or not quarantined_at <= started <= finished <= fetched:
            raise ValueError("Dividend strict recapture must occur after quarantine")
        old = row.get("old_quarantined_response")
        if key in quarantined:
            expected = {"path": quarantined[key]["quarantined_path"], "sha256": quarantined[key]["sha256"]}
            if old != expected:
                raise ValueError("Dividend correction lost an old quarantined original")
        elif old is not None:
            raise ValueError("Dividend correction invents a quarantined original")
    return proofs


def _same(values):
    """Compare actual fields, not source-generation IDs or retrieval timestamps."""
    return len({tuple("" if pd.isna(value) else str(value) for value in row)
                for row in values.itertuples(index=False, name=None)}) == 1


def merge_execution_frames(frames, basics):
    """Deduplicate overlapping index members only when all original terms agree."""
    if not frames or len(frames) != len(basics):
        raise ValueError("Both raw market and security identity collections are required")
    merged = pd.concat(frames, ignore_index=True)
    missing = {"instrument", "date", *_DAILY_FIELDS} - set(merged)
    if missing:
        raise ValueError("Original daily fields missing: " + ", ".join(sorted(missing)))
    duplicates = merged.loc[merged.duplicated(["instrument", "date"], keep=False)]
    for key, rows in duplicates.groupby(["instrument", "date"], sort=True):
        if not _same(rows.loc[:, list(_DAILY_FIELDS)]):
            raise ValueError("Conflicting original market evidence for " + "/".join(key))
    merged = merged.drop_duplicates(["instrument", "date"], keep="first").sort_values(
        ["instrument", "date"]).reset_index(drop=True)
    identity = pd.concat(basics, ignore_index=True)
    if not {"code", *_IDENTITY_FIELDS}.issubset(identity):
        raise ValueError("Original IPO / delisting identity fields are required")
    for code, rows in identity.groupby("code", sort=True):
        if not _same(rows.loc[:, list(_IDENTITY_FIELDS)]):
            raise ValueError("Conflicting original IPO/delisting identity for " + code)
    return merged, identity.drop_duplicates("code", keep="first").reset_index(drop=True)


def merge_dividend_events(event_lists):
    """One economic entitlement despite a member's presence in two collections."""
    result = {}
    for events in event_lists:
        for event in events:
            prior = result.get(event["event_id"])
            if prior is not None and any(prior.get(field) != event.get(field)
                                         for field in _ECONOMIC_FIELDS):
                raise ValueError("Conflicting duplicate dividend entitlement")
            result.setdefault(event["event_id"], dict(event))
    return sorted(result.values(), key=lambda row: (row["ex_date"], row["instrument"], row["event_id"]))


def load_event_evidence(paths, *, key_fields):
    """Combine explicit facts and verify every referenced original byte sequence."""
    events, proofs, origins = {}, {}, []
    for number, path in enumerate(paths):
        path = Path(path).resolve()
        proofs[f"document_{number}"] = _proof(path)
        payload = json.loads(path.read_text())
        if not isinstance(payload.get("events"), list):
            raise ValueError("Evidence document needs explicit events")
        for item, event in enumerate(payload["events"]):
            key = tuple(event[field] for field in key_fields)
            if key in events and events[key] != event:
                raise ValueError("Conflicting duplicate independently evidenced fact")
            events[key] = event
            origins.append({"key_fields": list(key_fields), "key_values": list(key),
                            "document": _proof(path)})
            originals = event.get("evidence", event.get("reference_evidence", []))
            originals = originals if isinstance(originals, list) else [originals]
            if not originals:
                raise ValueError("An independently asserted fact needs original source evidence")
            for source_number, original in enumerate(originals):
                source_path = Path(original["path"])
                if not source_path.is_absolute():
                    raise ValueError("A5 source evidence uses absolute original paths")
                proof = _proof(source_path)
                if proof["sha256"] != original["sha256"]:
                    raise ValueError("Original event source bytes changed")
                proofs[f"document_{number}_event_{item}_source_{source_number}"] = proof
            # Rendered page receipts and supplementary originals can sit beside
            # the primary PDF list. Bind those actual bytes as well, rather than
            # treating the enclosing fact JSON hash as a substitute for them.
            already_bound = {(p["path"], p["sha256"]) for p in proofs.values()}

            def bind_supplementary(tree, label):
                if isinstance(tree, dict):
                    if "path" in tree and "sha256" in tree:
                        source_path = Path(tree["path"])
                        if not source_path.is_absolute():
                            raise ValueError("A5 supplementary evidence uses absolute original paths")
                        extra = _proof(source_path)
                        if extra["sha256"] != tree["sha256"]:
                            raise ValueError("Supplementary original event source bytes changed")
                        identity = (extra["path"], extra["sha256"])
                        if identity not in already_bound:
                            proofs[label] = extra
                            already_bound.add(identity)
                    for key, value in tree.items():
                        bind_supplementary(value, label + ":" + str(key))
                elif isinstance(tree, list):
                    for index, value in enumerate(tree):
                        bind_supplementary(value, label + ":" + str(index))

            bind_supplementary(event, f"document_{number}_event_{item}_supplementary")
    return {"events": list(events.values()), "fact_source_documents": origins}, proofs


@dataclass
class A5MarketEvidence:
    raw: pd.DataFrame
    basic: pd.DataFrame
    events: list
    instruments: list
    calendar: list
    source_proofs: dict
    execution_receipts: list
    dividend_receipts: list
    archive_scope_view_receipt: dict = field(default_factory=dict)


def load_a5_market_evidence(execution_collections, dividend_collections, *, scope,
                            calendar, start, end, action_overrides_paths=(), archive_scope_view=False,
                            dividend_source_correction_receipt_paths=()):
    """Load complete actual source collections with exact dated-universe coverage."""
    with _scope_batch(scope):
        return _load_a5_market_evidence(execution_collections, dividend_collections,
            scope=scope, calendar=calendar, start=start, end=end,
            action_overrides_paths=action_overrides_paths, archive_scope_view=archive_scope_view,
            dividend_source_correction_receipt_paths=dividend_source_correction_receipt_paths)


def _load_a5_market_evidence(execution_collections, dividend_collections, *, scope,
                            calendar, start, end, action_overrides_paths=(), archive_scope_view=False,
                            dividend_source_correction_receipt_paths=()):
    start, end = _day(start), _day(end)
    if type(archive_scope_view) is not bool:
        raise ValueError("Archive scope view must be an explicit Boolean choice")
    if start > end or not execution_collections or not dividend_collections:
        raise ValueError("A5 requires complete original execution and dividend collections")
    days = [day for day in calendar if start <= day <= end]
    preceding = [day for day in calendar if day < start]
    if not days or not preceding or calendar != sorted(set(calendar)):
        raise ValueError("Actual ordered calendar with a preceding session is required")
    targets = scope.download_targets(start, end, sessions=calendar, scope="union")
    # Package source histories are retrospective effective membership, not a
    # historical announcement-time database. The scope implementation checks
    # both 300/1000 cardinalities and same-day cross-index disjointness.
    if scope.audit_sessions(days, scope="union")["status"] != "passed":
        raise ValueError("Historical union membership failed its daily audit")
    frames, basics, receipts, dividend_receipts, event_lists = [], [], [], [], []
    proofs, execution_targets, dividend_targets = {}, set(), set()
    view_receipt = {"kind": "a5_existing_immutable_archive_scope_view", "applied": archive_scope_view,
        "source_vintage": "original_archive_generations_preserved_not_new_supplier_collections",
        "permitted_instruments": targets, "execution_inputs": [], "dividend_inputs": [], "action_override_inputs": [],
        "new_download_targets_changed_by_this_loader": False}
    excluded_instruments = set()
    for number, directory in enumerate(execution_collections):
        directory = Path(directory).resolve()
        frame, basic, receipt = load_execution_collection(directory)
        request = receipt["request"]
        if request["start_date"] > preceding[-1] or request["end_date"] < end:
            raise ValueError("Original execution collection lacks the preceding close or audit end")
        outside = sorted(set(request["instruments"]) - set(targets))
        if outside and not archive_scope_view:
            raise ValueError("Execution collection targets exceed the permitted historical union")
        selected = frame.loc[frame.instrument.isin(targets)].copy()
        execution_targets.update(set(request["instruments"]) & set(targets))
        excluded_instruments.update(outside)
        view_receipt["execution_inputs"].append({"archive": str(directory),
            "original_instrument_count": len(request["instruments"]), "excluded_input_instruments": outside,
            "original_record_count": len(frame), "scope_view_record_count": len(selected),
            "excluded_input_record_count": len(frame) - len(selected)})
        frames.append(selected); basics.append(basic); receipts.append(receipt)
        proofs[f"execution_collection_{number}"] = _proof(directory / "execution_collection.json")
        proofs[f"execution_selection_{number}"] = _proof(request["selection_evidence_path"])
        for symbol, response in receipt["responses"].items():
            original = directory / response["file"]
            if sha(original) != response["sha256"]:
                raise ValueError("Original execution response changed")
            proofs[f"execution_{number}_original_{symbol}"] = _proof(original)
    years = list(range(int(start[:4]), int(end[:4]) + 1))
    for number, directory in enumerate(dividend_collections):
        directory = Path(directory).resolve()
        _, receipt = load_dividend_evidence(directory)
        outside = sorted(set(receipt["instruments"]) - set(targets))
        if receipt["years"] != years or outside and not archive_scope_view:
            raise ValueError("Dividend collection years/targets differ from the historical union")
        dividend_targets.update(set(receipt["instruments"]) & set(targets))
        dividend_receipts.append(receipt)
        original_events = dividend_events(directory, start=start, end=end)
        selected_events = [event for event in original_events if event["instrument"] in targets]
        excluded_instruments.update(outside)
        view_receipt["dividend_inputs"].append({"archive": str(directory),
            "original_instrument_count": len(receipt["instruments"]), "excluded_input_instruments": outside,
            "original_window_event_count": len(original_events), "scope_view_window_event_count": len(selected_events),
            "excluded_input_event_count": len(original_events) - len(selected_events)})
        event_lists.append(selected_events)
        proofs[f"dividend_collection_{number}"] = _proof(directory / "collection.json")
        proofs.update({f"dividend_correction_{number}:" + key: value for key, value in
            audit_dividend_source_correction(directory, dividend_source_correction_receipt_paths).items()})
        for key, response in receipt["responses"].items():
            original = directory / response["file"]
            if sha(original) != response["sha256"]:
                raise ValueError("Original dividend response changed")
            audit_original_dividend_request(original, key)
            proofs[f"dividend_{number}_original_{key}"] = _proof(original)
    if execution_targets != set(targets) or dividend_targets != set(targets):
        raise ValueError("Both market and dividend generations must cover every historical union target")
    raw, basic = merge_execution_frames(frames, basics)
    events = merge_dividend_events(event_lists)
    view_receipt["consumed_pre_override_deduplicated_event_count"] = len(events)
    applied_patch_values = {}
    for number, path in enumerate(action_overrides_paths):
        path = Path(path).resolve()
        document = json.loads(path.read_text())
        proofs[f"action_overrides_{number}"] = _proof(path)
        selected_patches, excluded_patches = [], []
        for item, patch in enumerate(document["patches"]):
            original = (path.parent / patch["evidence"]["path"]).resolve()
            if sha(original) != patch["evidence"]["sha256"] or not patch["evidence"].get("url", "").startswith("https://"):
                raise ValueError("Original archived action correction evidence changed")
            proofs[f"action_override_{number}_original_{item}"] = _proof(original)
            for supporting_number, supporting in enumerate(patch.get("supporting_evidence", [])):
                supporting_path = Path(supporting["path"])
                if (not supporting_path.is_absolute() or sha(supporting_path) != supporting["sha256"]
                        or supporting_path.suffix.lower() == ".pdf" and not supporting.get("url", "").startswith("https://")):
                    raise ValueError("Original supporting action evidence changed or lacks source identity")
                proofs[f"action_override_{number}_supporting_{item}_{supporting_number}"] = _proof(supporting_path)
            if patch["instrument"] in targets and start <= patch["ex_date"] <= end:
                for field_name, field_value in patch["values"].items():
                    identity = (patch["instrument"], patch["ex_date"], field_name)
                    if identity in applied_patch_values:
                        prior = applied_patch_values[identity]
                        try:
                            same = Decimal(str(prior)) == Decimal(str(field_value))
                        except ArithmeticError:
                            same = prior == field_value
                        if not same:
                            raise ValueError("Conflicting independently evidenced action correction values")
                    applied_patch_values[identity] = field_value
                selected_patches.append({**patch, "evidence": {**patch["evidence"], "path": str(original)}})
            else:
                excluded_patches.append(patch)
        if excluded_patches and not archive_scope_view:
            raise ValueError("Original action corrections exceed the permitted official scope/window")
        # A local consumption view of an immutable archive is explicit. The
        # original full patch document and all its source PDFs remain bound.
        # Absolute proof paths preserve the frozen correction helper's checks.
        with TemporaryDirectory(prefix="a5_action_scope_view_") as temporary:
            view_path = Path(temporary) / "corrections.json"
            _save(view_path, {**document, "patches": selected_patches})
            events = apply_action_overrides(events, view_path)
        view_receipt["action_override_inputs"].append({"original_document": _proof(path),
            "original_patch_count": len(document["patches"]), "consumed_patch_count": len(selected_patches),
            "excluded_patch_count": len(excluded_patches), "consumed_patches": selected_patches,
            "excluded_patches": excluded_patches,
            "selection_basis": "fixed_official_target_and_audit_date_window_not_performance"})
    if any(event["instrument"] not in execution_targets or not start <= event["ex_date"] <= end
           for event in events):
        raise ValueError("A5 action overrides added an out-of-scope/window entitlement")
    if len({event["event_id"] for event in events}) != len(events):
        raise ValueError("Duplicate post-correction economic event IDs")
    scope.assert_unchanged()
    view_receipt.update(excluded_input_instruments=sorted(excluded_instruments),
        excluded_input_record_count=sum(row["excluded_input_record_count"] for row in view_receipt["execution_inputs"]),
        excluded_input_event_count=sum(row["excluded_input_event_count"] for row in view_receipt["dividend_inputs"]),
        consumed_deduplicated_record_count=len(raw), consumed_post_override_event_count=len(events),
        complete_official_target_coverage_verified=True)
    return A5MarketEvidence(raw, basic, events, targets, list(calendar), proofs, receipts, dividend_receipts, view_receipt)


def audit_member_identity(raw, basic, scope, calendar, *, start, end, identity_events=()):
    """A member must have a listed original identity on each actual session."""
    with _scope_batch(scope):
        return _audit_member_identity(raw, basic, scope, calendar,
            start=start, end=end, identity_events=identity_events)


def _audit_member_identity(raw, basic, scope, calendar, *, start, end, identity_events=()):
    identities = basic.set_index("code")
    by_key = raw.set_index(["instrument", "date"])
    failures, transitions = [], []
    facts = {(event["instrument"], event["effective_date"]): event for event in identity_events}
    if len(facts) != len(identity_events):
        raise ValueError("Duplicate security identity transition facts")
    scope_rows = []
    for day in (day for day in calendar if start <= day <= end):
        view = (scope.view_on(day, scope="union", include_bound_inputs=False)
                if hasattr(scope, "verified_batch") else scope.view_on(day, scope="union"))
        scope_rows.append({"session": day, "member_count": view["member_count"],
                           "index_counts": view["index_counts"]})
        for instrument in view["members"]:
            code = instrument[:2].lower() + "." + instrument[2:]
            if code not in identities.index:
                failures.append({"instrument": instrument, "session": day, "reason": "member_missing_security_identity"})
                continue
            identity = identities.loc[code]
            ipo, out = str(identity.ipoDate).strip(), str(identity.outDate).strip()
            if (not ipo or ipo == "nan" or ipo > day or out not in {"", "nan"} and out <= day
                    or str(identity.type) != "1"):
                failures.append({"instrument": instrument, "session": day, "reason": "member_not_original_listed_stock"})
            elif (instrument, day) not in by_key.index:
                failures.append({"instrument": instrument, "session": day, "reason": "member_has_no_original_daily_status"})
    # A terminal identity change must be accounted explicitly; today's basic
    # row cannot supply the historical delisting-period or conversion terms.
    observed_instruments = set(raw.instrument)
    for identity in basic.to_dict("records"):
        out = str(identity["outDate"]).strip()
        if out in {"", "nan"} or not start <= out <= end:
            continue
        instrument = identity["code"].replace(".", "").upper()
        if instrument not in observed_instruments:
            continue
        fact = facts.get((instrument, out))
        if fact is None or fact.get("kind") not in {"delisting", "share_exchange", "code_change"}:
            failures.append({"instrument": instrument, "effective_date": out,
                             "reason": "terminal_identity_requires_official_accounting_evidence"})
        else:
            transitions.append(fact)
    return {"status": "passed" if not failures else "blocked", "sessions": scope_rows,
            "failures": failures, "terminal_identity_transitions": transitions,
            "announcement_time_PIT": False}


def audit_raw_qlib_bridge(dataset, raw, instruments, *, start, end, partial_session_model_policy_path=None):
    """Report every seam failure under the same fixed float32 tolerance as A2."""
    records, failures, approved_exceptions = [], [], []
    exceptions, exception_proofs = load_partial_session_model_policy(partial_session_model_policy_path)
    grouped = {instrument: frame for instrument, frame in raw.groupby("instrument", sort=False)}
    feature_fields = ["open", "high", "low", "close", "volume", "vwap", "factor"]
    for instrument in instruments:
        local = grouped.get(instrument, raw.iloc[:0])
        source = local.loc[local.date.between(start, end)].set_index("date")
        try:
            stored = read_qlib_security(dataset, instrument).loc[start:end]
        except (FileNotFoundError, ValueError, KeyError) as exc:
            failures.append({"instrument": instrument, "reason": "model_security_unavailable", "error_type": type(exc).__name__})
            continue
        if not set(feature_fields).issubset(stored):
            failures.append({"instrument": instrument, "reason": "model_schema_missing_required_features",
                             "missing_fields": sorted(set(feature_fields) - set(stored))})
            continue
        observed_column = ("source_observed_market_quote" if "source_observed_market_quote" in source
                           else "source_complete_quote")
        missing_sessions = source.index.difference(stored.index)
        for day in missing_sessions:
            if bool(source.loc[day, observed_column]):
                failures.append({"instrument": instrument, "session": day, "reason": "trading_source_outside_model_calendar",
                                 "missing_fields": feature_fields})
        overlap = stored.index.intersection(source.index)
        actual = source.loc[overlap, ["open", "high", "low", "close"]].replace("", np.nan).astype(float)
        factors = pd.to_numeric(stored.loc[overlap, "factor"], errors="coerce")
        restored = stored.loc[overlap, ["open", "high", "low", "close"]].div(factors, axis=0)
        complete = source.loc[overlap, observed_column].astype(bool)
        features = stored.loc[overlap, feature_fields].apply(pd.to_numeric, errors="coerce")
        finite_features = np.isfinite(features) & features.gt(0)
        for day in overlap[complete & ~finite_features.all(axis=1)]:
            if matches_partial_model_exception(instrument, day, source.loc[day], features.loc[day], exceptions):
                approved_exceptions.append({"instrument": instrument, "session": day,
                    "missing_model_fields": feature_fields,
                    "reason": "specifically_evidenced_partial_session_has_no_model_bar",
                    "source_quote_and_status_preserved": True, "model_score_required": False,
                    "execution_remains_blocked": True})
                continue
            failures.append({"instrument": instrument, "session": day,
                "reason": "observed_source_missing_finite_model_feature_fields",
                "missing_fields": [field for field in feature_fields if not finite_features.loc[day, field]]})
        finite_factor = np.isfinite(factors) & factors.gt(0)
        valid = actual.gt(0).all(axis=1) & np.isfinite(actual).all(axis=1) & np.isfinite(restored).all(axis=1) & finite_factor
        if not valid.any():
            failures.append({"instrument": instrument, "reason": "no_observed_raw_model_overlap"})
            continue
        left, right = actual.loc[valid], restored.loc[valid]
        close = np.isclose(left, right, rtol=1e-6, atol=1e-6)
        for position in np.flatnonzero(~close.all(axis=1)):
            day = left.index[position]
            failures.append({"instrument": instrument, "session": day, "reason": "raw_qlib_ohlc_seam_mismatch",
                "raw": {key: float(value) for key, value in left.iloc[position].items()},
                "restored": {key: float(value) for key, value in right.iloc[position].items()}})
        records.append({"instrument": instrument, "overlap_sessions": int(valid.sum()),
            "max_relative_error": float(((left - right).abs() / left).max().max()),
            "max_absolute_error": float((left - right).abs().max().max())})
    if any(sha(proof["path"]) != proof["sha256"] for proof in exception_proofs.values()):
        raise ValueError("Partial model exception changed during price verification")
    return {"status": "passed" if not failures else "blocked", "records": records,
            "failures": failures, "start": start, "end": end,
            "required_model_feature_fields": feature_fields, "rtol": 1e-6, "atol": 1e-6,
            "partial_model_score_exceptions": approved_exceptions,
            "partial_model_policy_bound_inputs": exception_proofs,
            "tolerance_basis": "unchanged_A2_float32_restoration_not_source_seam_adjustment"}


def audit_official_membership_evidence(path, scope_path, *, start, end):
    """Exact daily counts alone cannot certify true historical index members.

    A separate official-source reconstruction must bind its original notices,
    initial dated members and the exact derived intervals actually consumed.
    A missing/incomplete reconstruction remains an explicit blocked audit.
    """
    if path is None:
        return {"status": "blocked", "reason": "official_effective_membership_reconstruction_unavailable",
                "checks": {key: False for key in OFFICIAL_MEMBERSHIP_CHECKS}}, {}
    path = Path(path).resolve()
    value = json.loads(path.read_text())
    if (value.get("kind") != "a5_official_membership_evidence"
            or value.get("historical_scope_manifest_sha256") != sha(Path(scope_path) / "manifest.json")):
        raise ValueError("Official membership reconstruction identity/scope checksum differs")
    if value.get("start", "9999-12-31") > start or value.get("end", "0001-01-01") < end:
        raise ValueError("Official membership evidence does not cover the requested window")
    proofs = {"official_membership_document": _proof(path)}

    def bind(tree, label):
        if isinstance(tree, dict):
            if "path" in tree and "sha256" in tree:
                original = Path(tree["path"])
                if not original.is_absolute():
                    raise ValueError("Official membership originals use absolute source paths")
                proof = _proof(original)
                if proof["sha256"] != tree["sha256"]:
                    raise ValueError("Official membership original bytes changed")
                proofs[label] = proof
            else:
                for key, item in tree.items():
                    bind(item, label + ":" + str(key))
        elif isinstance(tree, list):
            for index, item in enumerate(tree):
                bind(item, label + ":" + str(index))

    bind(value.get("bound_inputs", {}), "official_membership_source")
    checks = {key: value.get("checks", {}).get(key) is True for key in OFFICIAL_MEMBERSHIP_CHECKS}
    passed = value.get("status") == "passed" and all(checks.values()) and len(proofs) > 1
    return {"status": "passed" if passed else "blocked", "checks": checks,
            "start": value.get("start"), "end": value.get("end"),
            "historical_scope_manifest_sha256": value["historical_scope_manifest_sha256"],
            "exact_count_alone_is_sufficient": False,
            "announcement_time_PIT": value.get("announcement_time_PIT", False)}, proofs


def audit_a5_capital_reserve_tax(events, evidence):
    """An issuer fact must match the actual delivered entitlement, not just its date."""
    missing = set(audit_capital_reserve_tax(events, evidence))
    facts = {(row["instrument"], row["ex_date"]): row for row in evidence.get("events", [])}
    for event in events:
        if Decimal(str(event.get("capital_reserve_ratio", "0"))) <= 0:
            continue
        fact = facts.get((event["instrument"], event["ex_date"]), {})
        try:
            same_terms = all(Decimal(str(fact[field])) == Decimal(str(event[field]))
                for field in ("capital_reserve_ratio", "cash_per_share", "stock_dividend_ratio"))
        except (KeyError, ArithmeticError, ValueError):
            same_terms = False
        if fact.get("status") != "verified" or fact.get("review_required") is not False or not same_terms:
            missing.add(event["event_id"])
    return sorted(missing)


def apply_union_capital_tax(events, evidence):
    """Reuse original verified exemptions and separately audit new taxable costs.

    Older A4 facts retain their original evidence/verification statements. They
    are not falsely relabelled as newly visually reviewed PDFs. Nonexempt or
    missing facts must pass the new strict A5 face-value/accounting adapter.
    """
    legacy_keys = set()
    for origin in evidence.get("fact_source_documents", []):
        original = origin.get("document", {})
        if (origin.get("key_fields") == ["instrument", "ex_date"]
                and original.get("sha256") == LEGACY_CAPITAL_FACT_DOCUMENT_SHA256
                and sha(original["path"]) == LEGACY_CAPITAL_FACT_DOCUMENT_SHA256):
            legacy_keys.add(tuple(origin["key_values"]))
    missing = set(audit_a5_capital_reserve_tax(events, evidence))
    missing.update(event["event_id"] for event in events
        if Decimal(str(event.get("capital_reserve_ratio", "0"))) > 0
        and (event["instrument"], event["ex_date"]) not in legacy_keys)
    affected = [event for event in events if event["event_id"] in missing]
    affected_keys = {(event["instrument"], event["ex_date"]) for event in affected}
    selected = {"events": [fact for fact in evidence.get("events", [])
        if (fact["instrument"], fact["ex_date"]) in affected_keys]}
    adjusted, report = apply_a5_capital_tax(affected, selected)
    replacements = {event["event_id"]: event for event in adjusted}
    result = [replacements.get(event["event_id"], event) for event in events]
    legacy_covered = [event["event_id"] for event in events
        if Decimal(str(event.get("capital_reserve_ratio", "0"))) > 0 and event["event_id"] not in missing]
    report["covered_event_ids"] = sorted(set(report["covered_event_ids"]) | set(legacy_covered))
    report["verified_exemption_events_from_original_source_facts"] = legacy_covered
    report["original_exemption_evidence_not_relabelled_as_new_visual_review"] = True
    return result, report


def source_review_queue(raw, events, references, *, capital_evidence, identity_audit,
                        start, end, temporal_audit=None, capital_accounting_report=None):
    """Publish unresolved facts without using returns to decide what to repair."""
    missing_capital = (set(audit_a5_capital_reserve_tax(events, capital_evidence))
        if capital_accounting_report is None else
        {row["event_id"] for row in capital_accounting_report["pending"]})
    missing_stock = set(audit_stock_dividend_tax(events))
    queue = []
    for event in events:
        reasons = []
        if event["event_id"] in missing_stock:
            reasons.append("stock_dividend_face_value_and_effective_tax_reserve")
        if event["event_id"] in missing_capital:
            reasons.append("share_premium_or_explicit_issuer_capital_tax_evidence")
        if reasons:
            queue.append({"instrument": event["instrument"], "ex_date": event["ex_date"],
                          "event_id": event["event_id"], "reasons": reasons, "event": event})
    for detail in references["unexplained"]:
        queue.append({**detail, "reasons": ["official_distribution_or_rights_reference_terms"]})
    selected = raw.loc[raw.date.between(start, end) & raw.tradestatus.eq("0")]
    for row in selected.to_dict("records"):
        numbers = pd.to_numeric(pd.Series({key: row[key] for key in ("open", "high", "low", "close", "volume", "amount")}), errors="coerce")
        if numbers.gt(0).all() and not row.get("source_observed_market_quote", False):
            queue.append({"instrument": row["instrument"], "session": row["date"],
                "source_response_sha256": row["source_response_sha256"],
                "reasons": ["official_partial_session_suspension_and_independent_quote"]})
    queue.extend({**detail, "reasons": [detail["reason"]]} for detail in identity_audit["failures"])
    if temporal_audit is not None:
        queue.extend({**detail, "reasons": [detail["reason"]]} for detail in temporal_audit["failures"])
    return {"kind": "a5_market_source_review_queue", "items": queue,
            "item_count": len(queue), "portfolio_returns_computed": False,
            "tax_classification_unresolved_with_conservative_full_face_tax_covered": (
                [] if capital_accounting_report is None else capital_accounting_report["tax_classification_unresolved"]),
            "no_security_excluded_to_improve_performance": True}


def build_a5_market_audit(snapshot_path, scope_path, execution_collections, dividend_collections,
        output_dir, *, start, end, action_overrides_paths=(), rights_events_paths=(),
        intraday_observations_paths=(), capital_reserve_tax_evidence_paths=(), identity_events_paths=(),
        official_membership_evidence_path=None, historical_scope=None,
        adjustment_bridge_spec_path=DEFAULT_SPEC, archive_scope_view=False,
        dividend_source_correction_receipt_paths=(), partial_session_model_policy_path=None,
        progress=None):
    """Bind a complete or explicitly blocked union market audit, never returns."""
    output = guard_qlib_output_path(output_dir)
    if output.exists():
        raise ValueError("Preserve existing A5 audit generation")
    from .a5_capital_tax import SPEC_PATH as capital_spec_path
    runtime_paths = {"implementation": __file__, "adjustment_bridge_spec": adjustment_bridge_spec_path,
                     "capital_accounting_spec": capital_spec_path}
    for name in ("a5_adjusted_prices.py", "a5_capital_tax.py", "a5_source_checks.py", "ashare_history_audit.py",
                 "ashare_actions.py", "ashare_execution_data.py", "ashare_scope.py",
                 "qlib_incremental.py", "qlib_snapshot.py"):
        runtime_paths["dependency:" + name] = Path(__file__).parent / name
    runtime_proofs = _capture_runtime_proofs(runtime_paths)
    _, partial_proofs = load_partial_session_model_policy(partial_session_model_policy_path)
    runtime_proofs.update({"partial_policy:" + key: value for key, value in partial_proofs.items()})

    def emit(phase):
        if progress:
            progress({"phase": phase})
        _assert_bound_inputs_unchanged(runtime_proofs)

    emit("load_original_sources")
    start, end = _day(start), _day(end)
    snapshot = load_qlib_snapshot(snapshot_path, verify=True)
    calendar_path = snapshot.dataset_dir / "calendars/day.txt"
    calendar = calendar_path.read_text().splitlines()
    # A later official-history reconstruction has its own type/provenance;
    # never mislabel it as the older code-window-only derivation for loading.
    scope = (load_verified_historical_scope(scope_path, start=start, end=end, require_official=True)
             if historical_scope is None else historical_scope)
    scope_manifest = (Path(scope_path) / "manifest.json").resolve()
    if not any(Path(proof["path"]).resolve() == scope_manifest and proof["sha256"] == sha(scope_manifest)
               for proof in scope.source_receipts().values()):
        raise ValueError("Historical scope object is not bound to the declared manifest")
    evidence = load_a5_market_evidence(execution_collections, dividend_collections,
        scope=scope, calendar=calendar, start=start, end=end, action_overrides_paths=action_overrides_paths,
        archive_scope_view=archive_scope_view,
        dividend_source_correction_receipt_paths=dividend_source_correction_receipt_paths)
    proofs = dict(evidence.source_proofs)
    proofs.update(runtime_proofs)
    rights, rights_proofs = load_event_evidence(rights_events_paths, key_fields=("instrument", "ex_date"))
    intraday, intraday_proofs = load_event_evidence(intraday_observations_paths, key_fields=("instrument", "date"))
    capital, capital_proofs = load_event_evidence(capital_reserve_tax_evidence_paths, key_fields=("instrument", "ex_date"))
    identities, identity_proofs = load_event_evidence(identity_events_paths, key_fields=("instrument", "effective_date"))
    for prefix, entries in (("rights", rights_proofs), ("intraday", intraday_proofs),
                            ("capital_tax", capital_proofs), ("identity", identity_proofs)):
        proofs.update({prefix + ":" + key: value for key, value in entries.items()})
    official_membership, official_proofs = audit_official_membership_evidence(
        official_membership_evidence_path, scope_path, start=start, end=end)
    proofs.update(official_proofs)
    evidence.events, capital_accounting = apply_union_capital_tax(evidence.events, capital)
    proofs.update({"capital_accounting:" + key: value for key, value in capital_accounting["bound_inputs"].items()})
    raw = apply_intraday_observation_evidence(evidence.raw, intraday)
    emit("daily_source_coverage")
    coverage = audit_daily_coverage(raw, evidence.basic, calendar, evidence.instruments, start=start, end=end)
    emit("daily_membership_identity")
    membership = audit_member_identity(raw, evidence.basic, scope, calendar, start=start, end=end,
                                      identity_events=identities["events"])
    preceding_signal = max(day for day in calendar if day < start)
    emit("model_availability")
    availability = audit_qlib_availability(snapshot.dataset_dir, raw, evidence.instruments, start=preceding_signal, end=end)
    emit("raw_model_seven_field_bridge")
    bridge = audit_raw_qlib_bridge(snapshot.dataset_dir, raw, evidence.instruments, start=preceding_signal, end=end,
                                 partial_session_model_policy_path=partial_session_model_policy_path)
    emit("normalized_temporal_source_bridge")
    temporal = audit_temporal_adjustments(snapshot.dataset_dir, raw, evidence.instruments,
        start=preceding_signal, end=end, spec_path=adjustment_bridge_spec_path,
        partial_session_model_policy_path=partial_session_model_policy_path)
    emit("issuer_exchange_reference_prices")
    references = audit_reference_prices(raw, evidence.events, calendar, start=start, end=end, rights=rights["events"])
    queue = source_review_queue(raw, evidence.events, references, capital_evidence=capital,
        identity_audit=membership, start=start, end=end, temporal_audit=temporal,
        capital_accounting_report=capital_accounting)
    checks = {"scope_coverage": membership["status"] == "passed" and official_membership["status"] == "passed",
        "scope_listing_validity": membership["status"] == "passed",
        "raw_ohlc": bridge["status"] == "passed" and availability["status"] == "passed" and temporal["status"] == "passed",
        "raw_preclose": references["status"] == "passed", "daily_trading_status": coverage["status"] == "passed",
        "historical_is_st": coverage["status"] == "passed", "ipo_identity": membership["status"] == "passed",
        "calendar_coverage": coverage["status"] == "passed",
        "dividend_events": not audit_stock_dividend_tax(evidence.events) and capital_accounting["status"] == "passed",
        "ex_reference_explained": references["status"] == "passed", "source_vintage": True,
        "identity_events": membership["status"] == "passed"}
    proofs.update({"snapshot_manifest": _proof(snapshot.path / "manifest.json"),
                   "calendar": _proof(calendar_path)})
    if proofs["adjustment_bridge_spec"] != temporal["source_rule"]:
        raise ValueError("Temporal source rule changed after runtime provenance capture")
    proofs.update({"scope:" + key: proof for key, proof in scope.source_receipts().items()})
    # Recheck every original, not just the top-level collector manifests.
    scope.assert_unchanged()
    load_qlib_snapshot(snapshot_path, verify=True)
    _assert_bound_inputs_unchanged(proofs)
    emit("write_canonical_audit_artifacts")
    output.mkdir(parents=True)
    payloads = {"coverage.json": coverage, "membership_audit.json": membership,
        "official_membership_audit.json": official_membership,
        "availability.json": availability, "raw_price_audit.json": bridge,
        "adjusted_price_temporal_audit.json": temporal,
        "ex_reference_audit.json": references, "source_review_queue.json": queue,
        "dividend_events.json": {"events": evidence.events}, "rights_events.json": rights,
        "intraday_observations.json": intraday, "identity_events.json": identities,
        "capital_reserve_tax_evidence.json": capital, "capital_tax_accounting.json": capital_accounting}
    payloads["archive_scope_view.json"] = evidence.archive_scope_view_receipt
    for name, payload in payloads.items():
        _save(output / name, payload)
    # This is an explicitly derived identity table, never a new raw response.
    # Full original registry coverage is preserved for dated training windows;
    # actual buy/download eligibility still comes only from the bound scope.
    evidence.basic.sort_values("code").to_csv(output / "stock_basic.csv", index=False, lineterminator="\n")
    raw.sort_values(["instrument", "date"]).to_csv(output / "daily.csv", index=False, lineterminator="\n")
    proofs["stock_basic"] = _proof(output / "stock_basic.csv")
    proofs["daily_csv"] = _proof(output / "daily.csv")
    for name in ("dividend_events", "rights_events", "intraday_observations", "identity_events", "capital_reserve_tax_evidence", "capital_tax_accounting", "archive_scope_view"):
        proofs[name] = _proof(output / (name + ".json"))
    _assert_bound_inputs_unchanged(proofs)
    audit = {"schema_version": 1, "kind": "a5_union_market_audit",
        "created_at": datetime.now(timezone.utc).isoformat(), "start": start, "end": end,
        "status": "passed" if all(checks.values()) else "blocked", "checks": checks,
        "raw_rows": len(raw), "instrument_count": len(evidence.instruments), "event_count": len(evidence.events),
        "unexplained_references": len(references["unexplained"]), "source_review_items": queue["item_count"],
        "unexplained_normalized_price_temporal_seams": len(temporal["failures"]),
        "missing_stock_dividend_tax_evidence": audit_stock_dividend_tax(evidence.events),
        "partial_model_score_exceptions": bridge["partial_model_score_exceptions"],
        "certified_partial_source_chains": temporal.get("certified_partial_source_chains", []),
        "missing_capital_reserve_tax_evidence": [row["event_id"] for row in capital_accounting["pending"]],
        "tax_classification_unresolved": capital_accounting["tax_classification_unresolved"],
        "all_corporate_tax_source_classifications_verified": capital_accounting["all_source_classifications_verified"],
        "source_snapshot_manifest_sha256": proofs["snapshot_manifest"]["sha256"],
        "historical_scope_manifest_sha256": sha(Path(scope_path) / "manifest.json"),
        "stock_basic_path": proofs["stock_basic"]["path"], "stock_basic_sha256": proofs["stock_basic"]["sha256"],
        "daily_csv_path": proofs["daily_csv"]["path"], "daily_csv_sha256": proofs["daily_csv"]["sha256"],
        "archive_scope_view": evidence.archive_scope_view_receipt,
        "stock_basic_basis": "canonical_CSV_deduplicated_original_source_registry_identity_fields_agree_all_original_responses_bound",
        "bound_inputs": proofs, "audited_input_sha256": {key: proof["sha256"] for key, proof in proofs.items()},
        "historical_membership_full_announcement_time_PIT": False,
        "source_snapshot_atomic_supplier_vintage_proven": False,
        "portfolio_returns_computed": False, "outside_scope_new_download_targets": False,
        "limitations": ["Supplier history and effective memberships reconstructed now, not historical announcement-time PIT.",
                        "Explicit suspended source status is never inferred from absent prices.",
                        "Capital-reserve exemptions require individual issuer evidence; unresolved classifications retain the fixed full-face 20% reserve with no refunds."]}
    _save(output / "market_audit.json", audit)
    _save(output / "manifest.json", {"status": audit["status"], "bound_inputs": proofs,
        "files": {path.name: sha(path) for path in sorted(output.iterdir()) if path.is_file()}})
    return audit
