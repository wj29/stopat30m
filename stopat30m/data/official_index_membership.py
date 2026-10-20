"""Reconstruct dated membership from an official anchor and complete event proof.

An event catalogue review is deliberately independent of member-count checks.
Missing events cannot be repaired by inserting a stock merely to reach 1,000.
No price, issuer identity or original supplier interval is modified here.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil

from stopat30m.data.qlib_snapshot import guard_qlib_output_path

COUNTS = {"csi300": 300, "csi1000": 1000}
COMPLETENESS_CHECKS = ("initial_historical_memberships", "all_regular_adjustments", "all_temporary_adjustments",
    "official_effective_dates", "derived_membership_matches_official_events", "all_exchange_sessions_covered", "no_current_backfill")
SOURCE_CHECKS = COMPLETENESS_CHECKS[:4]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _save(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n")


def _day(value):
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("Official membership dates must be explicit ISO dates")
    return value


def _proof(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": sha(path)}


def verify_proofs(proofs):
    if not isinstance(proofs, dict) or not proofs:
        raise ValueError("Official membership requires original source bindings")
    for proof in proofs.values():
        path = Path(proof.get("path", ""))
        if not path.is_absolute() or not path.is_file() or sha(path) != proof.get("sha256"):
            raise ValueError("Official membership original source checksum differs")


def _members(values, *, expected=None):
    if (not isinstance(values, list) or len(values) != len(set(values))
            or any(not isinstance(value, str) or not re.fullmatch(r"(?:SH6\d{5}|SZ[03]\d{5})", value) for value in values)
            or (expected is not None and len(values) != expected)):
        raise ValueError("Official membership requires unique canonical A-share identities and complete counts")
    return set(values)


def event_first_session(event, calendar):
    """A close-effective event changes the first subsequent actual session."""
    effective = _day(event["effective_on"])
    basis = event.get("effective_basis")
    if basis == "after_close":
        if effective not in calendar:
            raise ValueError("Official close-effective date must be an actual source session")
        later = [day for day in calendar if day > effective]
        if not later:
            raise ValueError("Official event has no subsequent source session")
        result = later[0]
    elif basis == "at_session_open":
        if effective not in calendar:
            raise ValueError("Official session-effective date must be an actual source session")
        result = effective
    else:
        raise ValueError("Official effective-date semantics must be explicit")
    if event.get("first_effective_session") != result:
        raise ValueError("Official event's declared first session differs from its effective-date semantics")
    return result


def reconstruct_membership(calendar, anchor, events, *, start, end):
    """Replay backward; every removal/addition must match the independently known side."""
    start, end = _day(start), _day(end)
    days = list(calendar)
    if not days or days != sorted(set(days)) or any(_day(day) != day for day in days):
        raise ValueError("Official reconstruction requires the original unique actual-session calendar")
    anchor_day = _day(anchor["as_of_date"])
    if not days[0] <= start <= end <= anchor_day <= days[-1] or anchor_day not in days:
        raise ValueError("Official reconstruction dates exceed the actual anchor/calendar coverage")
    current = {scope: _members(anchor["members"][scope], expected=count) for scope, count in COUNTS.items()}
    if current["csi300"] & current["csi1000"]:
        raise ValueError("Official anchor contains cross-index duplicate membership")
    by_session, identities, announcement_timing = defaultdict(list), set(), []
    for event in events:
        identifier = event.get("id")
        code_change = event.get("event_type") == "same_issuer_security_code_change"
        if (not identifier or identifier in identities or (not code_change and event.get("scope") not in COUNTS)
                or (code_change and (event.get("same_legal_issuer") is not True
                    or event.get("share_quantity_changes") is not False or event.get("price_mapping_allowed") is not False))):
            raise ValueError("Official events require unique IDs and an allowed index")
        identities.add(identifier)
        first = event_first_session(event, days)
        published = _day(event["published_on"])
        known = datetime.fromisoformat(event["known_at"])
        if known.tzinfo is None or known.date().isoformat() < published or published > first:
            raise ValueError("Official publication and knowledge timestamps must be explicit and ordered")
        first_open = datetime.fromisoformat(first + "T09:30:00+08:00")
        announcement_timing.append({"event_id": identifier, "published_on": published, "known_at": event["known_at"],
            "first_effective_session": first, "knowledge_after_first_session_open": known > first_open,
            "exact_historical_announcement_time_PIT_claim": False})
        adds = _members([event["new_instrument"]] if code_change else event["adds"])
        removes = _members([event["old_instrument"]] if code_change else event["removes"])
        if not adds or adds & removes or len(adds) != len(removes):
            raise ValueError("Official replacement events require equal nonempty distinct old/new identities")
        originals = event.get("evidence")
        if not isinstance(originals, list) or not originals:
            raise ValueError("Every official event needs its own original announcement/attachment evidence")
        verify_proofs({str(number): proof for number, proof in enumerate(originals)})
        if first <= anchor_day and first >= start:
            by_session[first].append((event, adds, removes))
    observed = {scope: defaultdict(list) for scope in COUNTS}
    audit, reversed_events, code_change_applications = [], [], []
    selected = [day for day in days if start <= day <= anchor_day]
    for day in reversed(selected):
        overlap = current["csi300"] & current["csi1000"]
        if overlap or any(len(current[scope]) != count for scope, count in COUNTS.items()):
            raise ValueError("Official event chain produced incomplete or overlapping dated membership")
        if day <= end:
            for scope, members in current.items():
                for member in members:
                    observed[scope][member].append(day)
            audit.append({"date": day, "csi300": len(current["csi300"]), "csi1000": len(current["csi1000"]), "overlap": 0})
        # Changes effective today are reversed only after recording today's pool.
        # Same-day cross-index migrations are applied as one batch.
        batches = {scope: {"adds": set(), "removes": set()} for scope in COUNTS}
        code_events = []
        for event, adds, removes in by_session.get(day, ()):
            if event.get("event_type") == "same_issuer_security_code_change":
                code_events.append((event, adds, removes))
                continue
            batch = batches[event["scope"]]
            if batch["adds"] & adds or batch["removes"] & removes:
                raise ValueError("Duplicate same-index official adjustments on one effective session")
            batch["adds"].update(adds)
            batch["removes"].update(removes)
            reversed_events.append(event["id"])
        for scope, batch in batches.items():
            if not batch["adds"].issubset(current[scope]) or batch["removes"] & current[scope]:
                raise ValueError("Official inverse event does not match the independently observed later membership: "
                    + json.dumps({"session": day, "scope": scope,
                        "missing_announced_additions": sorted(batch["adds"] - current[scope]),
                        "announced_removals_still_in_later_pool": sorted(batch["removes"] & current[scope])}, ensure_ascii=False))
            current[scope] = (current[scope] - batch["adds"]) | batch["removes"]
        # Undo the day's index entries/exits before translating issuer codes:
        # a newly admitted new code did not necessarily belong to the prior pool.
        for event, adds, removes in code_events:
            new, old = next(iter(adds)), next(iter(removes))
            affected = []
            for scope in COUNTS:
                if new in current[scope]:
                    if old in current[scope]:
                        raise ValueError("Both old and new codes occur in one official membership state")
                    current[scope] = (current[scope] - {new}) | {old}
                    affected.append(scope)
                elif old in current[scope] and old not in batches[scope]["removes"]:
                    raise ValueError("Official later membership still contains a code after its verified validity end")
            reversed_events.append(event["id"])
            code_change_applications.append({"event_id": event["id"], "first_effective_session": day,
                "scopes_restored_from_independently_verified_later_membership": affected,
                "price_data_mapped": False, "share_quantity_changes": False})
    positions = {day: number for number, day in enumerate(days)}
    interval_text = {}
    for scope, histories in observed.items():
        rows = []
        for member, membership_days in sorted(histories.items()):
            membership_days.reverse()
            left = prior = membership_days[0]
            for day in membership_days[1:]:
                if positions[day] != positions[prior] + 1:
                    rows.append(f"{member}\t{left}\t{prior}")
                    left = day
                prior = day
            rows.append(f"{member}\t{left}\t{prior}")
        interval_text[scope] = "\n".join(rows) + "\n"
    return {"intervals": interval_text, "sessions": list(reversed(audit)), "reversed_event_ids": reversed_events,
        "code_change_applications": code_change_applications, "announcement_timing": announcement_timing,
        "anchor_as_of_date": anchor_day, "start": start, "end": end}


def _validate_coverage_review(review, *, start, anchor_day):
    if (review.get("kind") != "a5_official_index_event_coverage_review" or review.get("status") != "passed"
            or review.get("start", "9999") > start or review.get("end", "0000") < anchor_day
            or any(review.get("checks", {}).get(check) is not True for check in SOURCE_CHECKS)):
        raise ValueError("Complete independent official event-catalogue review is required; counts alone do not prove completeness")
    verify_proofs(review.get("bound_inputs"))


def build_official_reconstruction(calendar_path, events_path, review_path, output_dir, *, start, end):
    """Save a new generation after source completeness review and semantic replay."""
    target = guard_qlib_output_path(output_dir)
    if target.exists():
        raise ValueError("Preserve existing official reconstruction generations")
    payload = json.loads(Path(events_path).read_text())
    if payload.get("kind") != "ashare_official_index_adjustment_events" or payload.get("schema_version") != 1:
        raise ValueError("Unsupported canonical official adjustment-event document")
    review = json.loads(Path(review_path).read_text())
    _validate_coverage_review(review, start=start, anchor_day=payload["anchor"]["as_of_date"])
    verify_proofs(payload.get("bound_inputs"))
    calendar = Path(calendar_path).read_text().splitlines()
    replay = reconstruct_membership(calendar, payload["anchor"], payload["events"], start=start, end=end)
    proofs = {"calendar": _proof(calendar_path), "events": _proof(events_path), "coverage_review": _proof(review_path)}
    proofs.update({"event_source:" + key: proof for key, proof in payload["bound_inputs"].items()})
    proofs.update({"review_source:" + key: proof for key, proof in review["bound_inputs"].items()})
    for event in payload["events"]:
        proofs.update({f"event:{event['id']}:{number}": proof for number, proof in enumerate(event["evidence"])})
    target.mkdir(parents=True)
    for scope, text in replay["intervals"].items():
        (target / (scope + ".txt")).write_text(text)
    shutil.copyfile(events_path, target / "events.json")
    shutil.copyfile(review_path, target / "source_coverage_review.json")
    (target / "source").mkdir()
    shutil.copyfile(__file__, target / "source/official_index_membership.py")
    proofs["implementation"] = _proof(target / "source/official_index_membership.py")
    _save(target / "membership_audit.json", {key: value for key, value in replay.items() if key != "intervals"})
    verify_proofs(proofs)
    manifest = {"schema_version": 1, "kind": "ashare_official_event_membership_reconstruction", "status": "passed",
        "operation": "reverse_complete_official_adjustment_events_from_independently_observed_official_anchor",
        "start": start, "end": end, "anchor_as_of_date": replay["anchor_as_of_date"],
        "price_data_modified": False, "execution_data_modified": False, "bound_inputs": proofs,
        "files": {str(file.relative_to(target)): sha(file) for file in sorted(target.rglob("*")) if file.is_file()}}
    _save(target / "manifest.json", manifest)
    # This companion binds the completed manifest; it is excluded from that
    # manifest's files to avoid a circular hash dependency. Downstream audits
    # bind the companion bytes and recursively verify every original source.
    evidence = {"schema_version": 1, "kind": "a5_official_membership_evidence", "status": "passed",
        "start": start, "end": end, "historical_scope_manifest_sha256": sha(target / "manifest.json"),
        "checks": {check: True for check in COMPLETENESS_CHECKS}, "bound_inputs": {
            **proofs, "scope_manifest": _proof(target / "manifest.json"),
            **{"derived:" + name: _proof(target / name) for name in manifest["files"]}},
        "historical_announcement_time_PIT": False,
        "known_at_basis": "Each official announcement has its own declared source publication and conservative observed knowledge cutoff."}
    _save(target / "official_membership_evidence.json", evidence)
    return manifest


def verify_official_reconstruction(directory, *, start, end):
    """Independently replay stored official events and compare derived intervals."""
    root = Path(directory).resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    if (manifest.get("kind") != "ashare_official_event_membership_reconstruction" or manifest.get("status") != "passed"
            or manifest.get("price_data_modified") is not False or manifest.get("execution_data_modified") is not False
            or manifest.get("start", "9999") > start or manifest.get("end", "0000") < end):
        raise ValueError("A complete official event reconstruction covering the requested dates is required")
    for name, digest in manifest["files"].items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts or sha(root / relative) != digest:
            raise ValueError("Official derived membership file checksum differs")
    verify_proofs(manifest["bound_inputs"])
    payload = json.loads((root / "events.json").read_text())
    review = json.loads((root / "source_coverage_review.json").read_text())
    _validate_coverage_review(review, start=manifest["start"], anchor_day=payload["anchor"]["as_of_date"])
    replay = reconstruct_membership(Path(manifest["bound_inputs"]["calendar"]["path"]).read_text().splitlines(),
        payload["anchor"], payload["events"], start=manifest["start"], end=manifest["end"])
    for scope, text in replay["intervals"].items():
        if (root / (scope + ".txt")).read_text() != text:
            raise ValueError("Derived membership differs from semantic replay of official events")
    evidence_path = root / "official_membership_evidence.json"
    evidence = json.loads(evidence_path.read_text())
    if (evidence.get("kind") != "a5_official_membership_evidence" or evidence.get("status") != "passed"
            or evidence.get("historical_scope_manifest_sha256") != sha(root / "manifest.json")
            or any(evidence.get("checks", {}).get(check) is not True for check in COMPLETENESS_CHECKS)):
        raise ValueError("Official membership readiness companion is unavailable or differs")
    verify_proofs(evidence["bound_inputs"])
    return {**manifest["bound_inputs"], "official:manifest": _proof(root / "manifest.json"),
        "official:readiness": _proof(evidence_path), **{"official:derived:" + name: _proof(root / name) for name in manifest["files"]}}
