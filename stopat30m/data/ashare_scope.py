"""Bound CSI300 / CSI1000 views without mutating a broader source dataset.

Historical membership comes exclusively from the effective intervals in both
original source files. A current snapshot has its own observed date and cannot
be used for another date. These are membership views, not an announcement-time
PIT database or an instruction to sell an existing out-of-scope holding.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import shutil
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

from stopat30m.ashare_experiment import PackageHistoricalPool


ALLOWED_SCOPES = ("csi300", "csi1000")
EXPECTED_COUNTS = {"csi300": 300, "csi1000": 1000}
DEFAULT_SCOPE_DECLARATION = Path(__file__).resolve().parents[2] / "docs/research/ashare_scope.json"
SHANGHAI = ZoneInfo("Asia/Shanghai")
_SHA = re.compile(r"[0-9a-f]{64}")
_SECURITY = re.compile(r"(?:SH6\d{5}|SZ[03]\d{5})")


class AshareScopeError(ValueError):
    """The requested membership view lacks complete, unchanged evidence."""


def _day(value):
    if not isinstance(value, str):
        raise AshareScopeError("Scope dates must be explicit ISO calendar dates")
    try:
        result = date.fromisoformat(value)
    except ValueError as exc:
        raise AshareScopeError("Scope dates must be explicit ISO calendar dates") from exc
    if result.isoformat() != value:
        raise AshareScopeError("Scope dates must be explicit ISO calendar dates")
    return value


def _scope(value):
    if value not in {*ALLOWED_SCOPES, "union"}:
        raise AshareScopeError("Only csi300, csi1000 and their union are allowed")
    return value


def _security(value):
    if not isinstance(value, str) or not _SECURITY.fullmatch(value):
        raise AshareScopeError("Membership requires canonical SH/SZ A-share security identities; aliases need explicit source mapping")
    return value


def _sha(path):
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError as exc:
        raise AshareScopeError("Bound membership source is unavailable") from exc


@dataclass(frozen=True)
class SourceProof:
    path: Path
    sha256: str

    @classmethod
    def read(cls, path, expected_sha256=None):
        path = Path(path).expanduser().resolve()
        actual = _sha(path)
        if expected_sha256 is not None and (not _SHA.fullmatch(str(expected_sha256)) or actual != expected_sha256):
            raise AshareScopeError("Original scope source checksum differs")
        return cls(path, actual)

    def assert_unchanged(self):
        if _sha(self.path) != self.sha256:
            raise AshareScopeError("Original scope source changed after binding")

    def to_dict(self):
        return {"path": str(self.path), "sha256": self.sha256}


def _source_paths(paths):
    if not isinstance(paths, Mapping) or set(paths) != set(ALLOWED_SCOPES):
        raise AshareScopeError("Both original csi300 and csi1000 source files are required; other scopes are unsupported")
    result = {scope: Path(paths[scope]).expanduser().resolve() for scope in ALLOWED_SCOPES}
    if len(set(result.values())) != 2:
        raise AshareScopeError("Each index requires its own original membership source file")
    return result


def _membership_view(day, members, *, scope, require_complete=True):
    _scope(scope)
    requested = ALLOWED_SCOPES if scope == "union" else (scope,)
    counts = {name: len(members[name]) for name in ALLOWED_SCOPES}
    overlap = sorted(set(members["csi300"]) & set(members["csi1000"]))
    failures = [name + ":count_" + str(counts[name]) + "_expected_" + str(EXPECTED_COUNTS[name])
        for name in requested if counts[name] != EXPECTED_COUNTS[name]]
    if scope == "union" and overlap:
        failures.append("cross_index_duplicate_membership")
    if require_complete and failures:
        raise AshareScopeError("Incomplete historical/current scope view: " + ", ".join(failures))
    selected = sorted(set().union(*(set(members[name]) for name in requested)))
    return {"as_of_date": day, "scope": scope, "members": selected, "member_count": len(selected),
        "index_counts": counts, "overlap": overlap, "usable": not failures, "failures": failures}


class _ScopeView:
    def assert_unchanged(self):
        for proof in self._proofs.values():
            proof.assert_unchanged()

    def source_receipts(self):
        return {name: proof.to_dict() for name, proof in self._proofs.items()}

    def members_on(self, day, *, scope="union"):
        return self.view_on(day, scope=scope)["members"]

    def filter_cached_instruments(self, instruments: Sequence[str], day, *, scope="union"):
        """Return an eligible view; never delete files or mutate source/cache input."""
        eligible = set(self.members_on(day, scope=scope))
        return sorted(set(instruments) & eligible)

    def access_allowed(self, instrument, day, *, purpose, held_instruments=(), scope="union"):
        return access_allowed(self, instrument=instrument, session=day, purpose=purpose,
            held_instruments=held_instruments, scope=scope)


class HistoricalAshareScope(_ScopeView):
    """Effective historical intervals, bound independently for each index."""

    @classmethod
    def from_files(cls, paths, *, start, end, expected_sha256=None):
        start, end = _day(start), _day(end)
        if start > end:
            raise AshareScopeError("Historical scope coverage is reversed")
        paths = _source_paths(paths)
        if expected_sha256 is not None and set(expected_sha256) != set(ALLOWED_SCOPES):
            raise AshareScopeError("Expected original interval checksums must cover both indices")
        result = cls()
        result.start, result.end = start, end
        result._proofs = {name: SourceProof.read(path, None if expected_sha256 is None else expected_sha256[name]) for name, path in paths.items()}
        result._pools, result._excluded_aliases = {}, {}
        result._basis = "original_historical_effective_intervals_not_today_backfill"
        for name, path in paths.items():
            try:
                pool = PackageHistoricalPool(path, start=start, end=end)
            except (ValueError, OSError) as exc:
                raise AshareScopeError(str(exc)) from exc
            for instrument in pool.intervals:
                _security(instrument)
            result._pools[name] = pool
            result._excluded_aliases[name] = list(pool.clipped_non_numeric_intervals)
        result.assert_unchanged()
        return result

    @classmethod
    def from_derived(cls, directory, *, start, end):
        root = Path(directory).resolve()
        manifest_proof = SourceProof.read(root / "manifest.json")
        manifest = json.loads(manifest_proof.path.read_text())
        if (manifest.get("kind") != "ashare_code_validity_interval_derivation"
                or manifest.get("operation") != "clip_code_validity_windows_only"
                or manifest.get("price_data_modified") is not False or manifest.get("execution_data_modified") is not False):
            raise AshareScopeError("Unsupported historical membership derivation")
        for name, digest in manifest["files"].items():
            relative = Path(name)
            if relative.is_absolute() or ".." in relative.parts:
                raise AshareScopeError("Unsafe derived membership source path")
            SourceProof.read(root / relative, digest)
        proofs = {name: SourceProof.read(proof["path"], proof["sha256"]) for name, proof in manifest["bound_inputs"].items()}
        result = cls.from_files({name: root / (name + ".txt") for name in ALLOWED_SCOPES}, start=start, end=end,
            expected_sha256={name: manifest["files"][name + ".txt"] for name in ALLOWED_SCOPES})
        result._proofs.update({"derivation:" + name: proof for name, proof in proofs.items()})
        result._proofs["derivation:manifest"] = manifest_proof
        result._original_source_sha = {name: proofs["original:" + name].sha256 for name in ALLOWED_SCOPES}
        result._basis = "issuer_evidenced_code_validity_clipped_original_intervals_no_price_or_issuer_mapping"
        result.assert_unchanged()
        return result

    def view_on(self, day, *, scope="union", require_complete=True):
        day = _day(day)
        _scope(scope)
        if not self.start <= day <= self.end:
            raise AshareScopeError("Requested historical date is outside the declared source coverage")
        self.assert_unchanged()
        members = {name: self._pools[name].members_on(day) for name in ALLOWED_SCOPES}
        result = _membership_view(day, members, scope=scope, require_complete=require_complete)
        result.update(membership_basis=self._basis,
            announcement_time_PIT=False, bound_inputs=self.source_receipts())
        return result

    def audit_sessions(self, sessions, *, scope="union"):
        """Audit explicitly supplied actual sessions; never infer weekday sessions."""
        days = list(sessions)
        if not days or days != sorted(set(days)):
            raise AshareScopeError("Scope audit requires unique ordered actual exchange sessions")
        rows = [{key: value for key, value in self.view_on(day, scope=scope, require_complete=False).items()
                 if key not in {"members", "bound_inputs"}} for day in days]
        self.assert_unchanged()
        return {"kind": "ashare_historical_scope_audit", "scope": scope, "status": "passed" if all(row["usable"] for row in rows) else "blocked",
            "sessions": rows, "bound_inputs": self.source_receipts(),
            "outside_window_aliases_preserved_unmapped": {key: list(value) for key, value in self._excluded_aliases.items()},
            "source_datasets_modified": False, "announcement_time_PIT": False}

    def download_targets(self, start, end, *, sessions, scope="union"):
        """Collect actual dated members; do not make every target eligible daily."""
        start, end = _day(start), _day(end)
        if start > end or not self.start <= start <= end <= self.end:
            raise AshareScopeError("Download target dates exceed the declared historical scope coverage")
        days = list(sessions)
        if days != sorted(set(days)):
            raise AshareScopeError("Download targets need unique ordered original exchange sessions")
        selected = [day for day in days if start <= _day(day) <= end]
        if not selected:
            raise AshareScopeError("No actual exchange sessions in the requested download window")
        targets = set()
        for day in selected:
            targets.update(self.members_on(day, scope=scope))
        self.assert_unchanged()
        return sorted(targets)

    def unvalidated_source_candidates(self, start, end, *, sessions, scope="union"):
        """Expose suspect source members for repair only; not collection eligibility."""
        start, end = _day(start), _day(end)
        if start > end or not self.start <= start <= end <= self.end:
            raise AshareScopeError("Source candidate dates exceed the bound historical coverage")
        days = list(sessions)
        if not days or days != sorted(set(days)):
            raise AshareScopeError("Source candidate audit requires unique ordered exchange sessions")
        selected = [day for day in days if start <= _day(day) <= end]
        if not selected:
            raise AshareScopeError("No actual source sessions in the candidate repair window")
        targets = set()
        for day in selected:
            targets.update(self.view_on(day, scope=scope, require_complete=False)["members"])
        self.assert_unchanged()
        return sorted(targets)

    def training_row_mask(self, dates, instruments, *, scope="union"):
        """Keep each row only when its stock belonged to the requested date's pool."""
        dates, instruments = list(dates), list(instruments)
        if len(dates) != len(instruments):
            raise AshareScopeError("Training dates and security identities have different lengths")
        membership, result = {}, []
        for stamp, instrument in zip(dates, instruments):
            day = _day(stamp.date().isoformat() if hasattr(stamp, "date") else str(stamp))
            if day not in membership:
                membership[day] = set(self.members_on(day, scope=scope))
            result.append(instrument in membership[day])
        self.assert_unchanged()
        return result

    def filter_training_frame(self, frame, *, date_column="datetime", instrument_column="instrument", scope="union"):
        """Support daily columns or Qlib's named MultiIndex without source writes."""
        if date_column in frame.columns and instrument_column in frame.columns:
            dates, instruments = frame[date_column], frame[instrument_column]
        elif date_column in frame.index.names and instrument_column in frame.index.names:
            dates = frame.index.get_level_values(date_column)
            instruments = frame.index.get_level_values(instrument_column)
        else:
            raise AshareScopeError("Training view needs explicit datetime/instrument columns or named index levels")
        return frame.loc[self.training_row_mask(dates, instruments, scope=scope)].copy()


def _observed_stamp(value, day):
    try:
        stamp = datetime.fromisoformat(value)
    except (ValueError, TypeError) as exc:
        raise AshareScopeError("Current source observations require timezone-aware fetched_at") from exc
    if stamp.tzinfo is None:
        raise AshareScopeError("Current source observations require timezone-aware fetched_at")
    if stamp.astimezone(SHANGHAI).date().isoformat() < day:
        raise AshareScopeError("Current source fetched_at cannot predate its explicit source as_of_date")
    return stamp


class CurrentAshareScope(_ScopeView):
    """A complete dated two-index observation, never a historical substitute.

    Each input JSON must contain kind, scope, as_of_date, fetched_at, members,
    and source_evidence. Each original source receipt contains path, sha256,
    fetched_at and a nonempty source or public URL. Provider-specific member
    extraction is a separate audited step; this class preserves its receipts.
    """

    @classmethod
    def from_files(cls, paths, *, as_of_date, expected_sha256=None):
        day = _day(as_of_date)
        paths = _source_paths(paths)
        if expected_sha256 is not None and set(expected_sha256) != set(ALLOWED_SCOPES):
            raise AshareScopeError("Expected current membership checksums must cover both indices")
        result = cls()
        result.as_of_date = day
        result._members, result._proofs, result._observed_at = {}, {}, {}
        raw_receipts = {}
        for name, path in paths.items():
            bound = SourceProof.read(path, None if expected_sha256 is None else expected_sha256[name])
            result._proofs[name] = bound
            try:
                value = json.loads(path.read_text())
            except (ValueError, OSError) as exc:
                raise AshareScopeError("Current scope observation is not readable JSON") from exc
            if (value.get("kind") != "ashare_current_scope_observation" or value.get("scope") != name
                    or value.get("as_of_date") != day):
                raise AshareScopeError("Current membership source index/date differs from the requested observation")
            _observed_stamp(value.get("fetched_at"), day)
            result._observed_at[name] = value["fetched_at"]
            members = value.get("members")
            if not isinstance(members, list) or not all(isinstance(inst, str) for inst in members) or len(members) != len(set(members)):
                raise AshareScopeError("Current source membership contains missing or duplicate identities")
            result._members[name] = tuple(sorted(_security(inst) for inst in members))
            proofs = value.get("source_evidence")
            if not isinstance(proofs, list) or not proofs:
                raise AshareScopeError("Each current index requires original dated source evidence")
            raw_receipts[name] = set()
            for index, proof in enumerate(proofs):
                if not isinstance(proof, dict) or not _SHA.fullmatch(str(proof.get("sha256", ""))):
                    raise AshareScopeError("Current membership evidence requires original SHA-256 receipts")
                if not str(proof.get("source") or proof.get("url") or "").strip():
                    raise AshareScopeError("Current membership evidence source is unidentified")
                _observed_stamp(proof.get("fetched_at"), day)
                source_path = Path(proof["path"])
                if not source_path.is_absolute():
                    source_path = path.parent / source_path
                original = SourceProof.read(source_path, proof["sha256"])
                result._proofs[name + ":source:" + str(index)] = original
                raw_receipts[name].add((str(original.path), original.sha256))
        if raw_receipts["csi300"] == raw_receipts["csi1000"]:
            raise AshareScopeError("Two current indices cannot silently reuse one undifferentiated source observation")
        _membership_view(day, result._members, scope="union")
        result.assert_unchanged()
        return result

    def view_on(self, day, *, scope="union", require_complete=True):
        day = _day(day)
        if day != self.as_of_date:
            raise AshareScopeError("Current membership cannot be backdated or carried to another session")
        self.assert_unchanged()
        result = _membership_view(day, self._members, scope=scope, require_complete=require_complete)
        result.update(membership_basis="dated_current_source_observation_for_this_date_only",
            historical_membership=False, historical_signal_availability_proven=False,
            observed_at=dict(self._observed_at), bound_inputs=self.source_receipts())
        return result


def access_allowed(scope_view, *, instrument, session, purpose, held_instruments=(), scope="union"):
    """Keep existing exposure manageable even if fresh membership is unavailable."""
    if purpose not in {"new_buy", "sell", "valuation"}:
        raise AshareScopeError("Scope access purpose must be new_buy, sell or valuation")
    if purpose in {"sell", "valuation"}:
        return instrument in set(held_instruments)
    _day(session)
    _scope(scope)
    _security(instrument)
    if scope_view is None:
        return False
    return instrument in scope_view.members_on(session, scope=scope)


class ProjectAshareScope:
    """Persistent allowlist applied to new collection targets and training views."""

    @classmethod
    def from_file(cls, path=DEFAULT_SCOPE_DECLARATION, *, historical=None, current=None):
        proof = SourceProof.read(path)
        value = json.loads(proof.path.read_text())
        if (value.get("schema_version") != 1 or value.get("kind") != "personal_ashare_research_scope"
                or set(value.get("allowed_indices", {})) != set(ALLOWED_SCOPES)
                or value.get("default_scope") != "union"):
            raise AshareScopeError("Project scope allows only csi300 and csi1000 with an explicit union default")
        for name, count in EXPECTED_COUNTS.items():
            if value["allowed_indices"][name].get("expected_current_count") != count:
                raise AshareScopeError("Project constituent counts differ from CSI300/CSI1000")
        false_policies = ("outside_scope_new_buys", "outside_scope_new_training_samples", "outside_scope_new_download_targets")
        true_policies = ("existing_holdings_sell_and_valuation_always_allowed", "immutable_source_snapshot_preserved",
            "csi1000_requires_separate_data_model_and_fee_validation", "original_a3_and_a4_protocols_are_not_rewritten")
        if any(value.get(key) is not False for key in false_policies) or any(value.get(key) is not True for key in true_policies):
            raise AshareScopeError("Project scope cannot expand new exposure or rewrite frozen subset validation")
        result = cls()
        result._declaration, result._proof = value, proof
        result.historical, result.current = historical, current
        proof.assert_unchanged()
        return result

    def assert_unchanged(self):
        self._proof.assert_unchanged()
        for view in (self.historical, self.current):
            if view is not None:
                view.assert_unchanged()

    def download_targets(self, start, end, *, sessions, scope="union"):
        self._proof.assert_unchanged()
        if self.historical is None:
            raise AshareScopeError("Historical scope evidence is required before selecting new download targets")
        return self.historical.download_targets(start, end, sessions=sessions, scope=scope)

    def filter_training_frame(self, frame, *, scope="union", **columns):
        self._proof.assert_unchanged()
        if self.historical is None:
            raise AshareScopeError("Training requires actual historical membership; current members cannot backfill training")
        return self.historical.filter_training_frame(frame, scope=scope, **columns)

    def access_allowed(self, instrument, day, *, purpose, held_instruments=(), scope="union", current=False):
        if purpose in {"sell", "valuation"}:
            return access_allowed(None, instrument=instrument, session=day, purpose=purpose, held_instruments=held_instruments, scope=scope)
        self._proof.assert_unchanged()
        return access_allowed(self.current if current else self.historical, instrument=instrument,
            session=day, purpose=purpose, held_instruments=held_instruments, scope=scope)


def build_scope_receipt(snapshot_path, start, end, current_dir, output_dir, *, declaration_path=DEFAULT_SCOPE_DECLARATION, historical_dir=None):
    """Write membership-only views; preserve broad snapshots and existing papers."""
    from stopat30m.data.qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot
    output = guard_qlib_output_path(output_dir)
    if output.exists():
        raise AshareScopeError("Preserve the existing bound scope receipt")
    snapshot = load_qlib_snapshot(snapshot_path, verify=True)
    original_intervals = {name: snapshot.dataset_dir / "instruments" / (name + ".txt") for name in ALLOWED_SCOPES}
    if historical_dir is None:
        historical = HistoricalAshareScope.from_files(original_intervals, start=start, end=end)
    else:
        historical = HistoricalAshareScope.from_derived(historical_dir, start=start, end=end)
        if any(_sha(path) != historical._original_source_sha[name] for name, path in original_intervals.items()):
            raise AshareScopeError("Derived membership originated from another immutable source vintage")
    current_paths = {name: Path(current_dir) / (name + ".json") for name in ALLOWED_SCOPES}
    current_dates = {json.loads(path.read_text())["as_of_date"] for path in current_paths.values()}
    if len(current_dates) != 1:
        raise AshareScopeError("The two current source dates differ; union availability is unknown")
    current = CurrentAshareScope.from_files(current_paths, as_of_date=next(iter(current_dates)))
    project = ProjectAshareScope.from_file(declaration_path, historical=historical, current=current)
    calendar_path = snapshot.dataset_dir / "calendars/day.txt"
    calendar = calendar_path.read_text().splitlines()
    sessions = [day for day in calendar if start <= day <= end]
    audit = historical.audit_sessions(sessions)
    candidates = historical.unvalidated_source_candidates(start, end, sessions=calendar)
    targets = project.download_targets(start, end, sessions=calendar) if audit["status"] == "passed" else []
    current_view = current.view_on(current.as_of_date)
    cached = sorted(path.name.upper() for path in (snapshot.dataset_dir / "features").iterdir() if path.is_dir())
    historical_cache = sorted(set(cached) & set(targets))
    current_cache = current.filter_cached_instruments(cached, current.as_of_date)
    bound = {"project_scope": project._proof.to_dict(), "snapshot_manifest": SourceProof.read(snapshot.path / "manifest.json").to_dict(),
        "source_calendar": SourceProof.read(calendar_path).to_dict(),
        **{"historical:" + key: proof for key, proof in historical.source_receipts().items()},
        **{"current:" + key: proof for key, proof in current.source_receipts().items()},
        "implementation": SourceProof.read(__file__).to_dict()}
    project.assert_unchanged()
    load_qlib_snapshot(snapshot_path, verify=True)
    for proof in bound.values():
        SourceProof(Path(proof["path"]), proof["sha256"]).assert_unchanged()
    output.mkdir(parents=True)
    archive = output / "source" / "ashare_scope.py"
    archive.parent.mkdir()
    shutil.copyfile(__file__, archive)
    if _sha(archive) != bound["implementation"]["sha256"]:
        raise AshareScopeError("Scope implementation changed while archiving")
    bound["implementation"] = {**bound["implementation"], "original_path": bound["implementation"]["path"], "path": str(archive)}
    receipt = {"schema_version": 1, "kind": "ashare_project_scope_receipt", "allowed_indices": list(ALLOWED_SCOPES),
        "start_date": start, "end_date": end, "historical_union_security_count": len(candidates), "historical_daily_audit": audit,
        "historical_unvalidated_source_candidates": candidates, "historical_candidates_are_not_training_or_collection_eligibility": audit["status"] != "passed",
        "historical_cached_candidate_symbols": sorted(set(cached) & set(candidates)),
        "historical_download_targets": targets, "historical_cached_eligible_symbols": historical_cache,
        "historical_missing_cached_symbols": sorted(set(targets) - set(cached)), "current": current_view,
        "current_cached_eligible_symbols": current_cache, "current_missing_cached_symbols": sorted(set(current_view["members"]) - set(cached)),
        "bound_inputs": bound, "source_datasets_modified": False, "returns_generated": False,
        "old_csi300_protocol_role": "within_scope_subset_only_no_csi1000_validation_claim",
        "new_union_strategy_activated": False}
    (output / "scope_receipt.json").write_text(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    files = {str(path.relative_to(output)): _sha(path) for path in sorted(output.rglob("*")) if path.is_file()}
    (output / "manifest.json").write_text(json.dumps({"files": files, "bound_inputs": bound}, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    return receipt


def derive_code_validity_intervals(paths, evidence_path, calendar_path, output_dir, *, start, end):
    """Clip only invalid code dates into a separate, evidence-bound generation.

    This removes a future code from an earlier membership interval, rather than
    relabelling prices, merging issuer histories or inventing replacement rows.
    A count audit that remains incomplete stays blocked for new union research.
    """
    from stopat30m.data.qlib_snapshot import guard_qlib_output_path
    output = guard_qlib_output_path(output_dir)
    if output.exists():
        raise AshareScopeError("Preserve the existing code-validity interval generation")
    paths, start, end = _source_paths(paths), _day(start), _day(end)
    if start > end:
        raise AshareScopeError("Code-validity audit coverage is reversed")
    bound = {"original:" + name: SourceProof.read(path) for name, path in paths.items()}
    bound["code_validity_evidence"] = SourceProof.read(evidence_path)
    bound["original_calendar"] = SourceProof.read(calendar_path)
    bound["implementation"] = SourceProof.read(__file__)
    evidence = json.loads(bound["code_validity_evidence"].path.read_text())
    if evidence.get("schema_version") != 1 or evidence.get("kind") != "ashare_reviewed_code_validity_evidence" or not evidence.get("code_changes"):
        raise AshareScopeError("Reviewed issuer code-validity evidence is required")
    changes, codes = [], set()
    for number, event in enumerate(evidence["code_changes"]):
        old, new, effective = _security(event["old_instrument"]), _security(event["new_instrument"]), _day(event["effective_date"])
        proofs = event.get("source_evidence", [])
        if (old == new or old in codes or new in codes or event.get("same_legal_issuer") is not True
                or event.get("share_quantity_changes") is not False or len(proofs) < 2):
            raise AshareScopeError("Only independently evidenced same-issuer code validity windows are supported")
        codes.update((old, new))
        for item, proof in enumerate(proofs):
            if not str(proof.get("url", "")).startswith("https://"):
                raise AshareScopeError("Code validity must cite original public issuer disclosures")
            path = Path(proof["path"])
            if not path.is_absolute():
                path = Path(evidence_path).resolve().parent / path
            bound[f"issuer:{number}:{item}"] = SourceProof.read(path, proof["sha256"])
        changes.append((old, new, effective))
    calendar = Path(calendar_path).read_text().splitlines()
    if calendar != sorted(set(calendar)):
        raise AshareScopeError("Code-validity audit requires the original ordered exchange calendar")
    sessions = [day for day in calendar if start <= _day(day) <= end]
    before = HistoricalAshareScope.from_files(paths, start=start, end=end)
    generated, edits = {}, []
    for name, path in paths.items():
        lines = []
        for line in path.read_text().splitlines(keepends=True):
            if not line.strip():
                lines.append(line)
                continue
            parts = line.split()
            if len(parts) != 3:
                raise AshareScopeError("Original membership interval has an invalid row")
            symbol, begin, finish = parts
            _day(begin); _day(finish)
            revised_begin, revised_finish = begin, finish
            for old, new, effective in changes:
                if symbol == old:
                    revised_finish = min(revised_finish, (date.fromisoformat(effective) - timedelta(days=1)).isoformat())
                elif symbol == new:
                    revised_begin = max(revised_begin, effective)
            if (revised_begin, revised_finish) == (begin, finish):
                lines.append(line)
            else:
                dropped = revised_begin > revised_finish
                edits.append({"scope": name, "original_row": line.rstrip("\r\n"),
                    "derived_row": None if dropped else f"{symbol}\t{revised_begin}\t{revised_finish}",
                    "operation": "drop_code_outside_validity" if dropped else "clip_code_validity_boundary"})
                if not dropped:
                    lines.append(f"{symbol}\t{revised_begin}\t{revised_finish}\n")
        generated[name] = "".join(lines)
    for proof in bound.values():
        proof.assert_unchanged()
    output.mkdir(parents=True)
    for name, text in generated.items():
        (output / (name + ".txt")).write_text(text)
    after = HistoricalAshareScope.from_files({name: output / (name + ".txt") for name in ALLOWED_SCOPES}, start=start, end=end)
    audit_before, audit_after = before.audit_sessions(sessions), after.audit_sessions(sessions)
    differences = []
    for day in sessions:
        for name in ALLOWED_SCOPES:
            old = set(before.view_on(day, scope=name, require_complete=False)["members"])
            new = set(after.view_on(day, scope=name, require_complete=False)["members"])
            if old != new:
                differences.append({"date": day, "scope": name, "removed": sorted(old-new), "added": sorted(new-old),
                    "original_count": len(old), "derived_count": len(new)})
    report = {"kind": "ashare_code_validity_interval_audit", "status": audit_after["status"],
        "audit_start": start, "audit_end": end, "original_audit": audit_before, "derived_audit": audit_after,
        "edits": edits, "session_membership_differences": differences,
        "original_source_candidate_union": before.unvalidated_source_candidates(start, end, sessions=sessions),
        "derived_source_candidate_union": after.unvalidated_source_candidates(start, end, sessions=sessions),
        "price_data_modified": False, "execution_data_modified": False, "full_announcement_time_PIT": False,
        "interpretation": "Issuer identity dates verified; remaining original index effective intervals are reconstructed source membership."}
    for proof in bound.values():
        proof.assert_unchanged()
    archive = output / "source" / "ashare_scope.py"
    archive.parent.mkdir()
    shutil.copyfile(__file__, archive)
    if _sha(archive) != bound["implementation"].sha256:
        raise AshareScopeError("Derivation implementation changed while archiving")
    bound["implementation"] = SourceProof.read(archive)
    (output / "audit.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    manifest = {"schema_version": 1, "kind": "ashare_code_validity_interval_derivation",
        "operation": "clip_code_validity_windows_only", "price_data_modified": False, "execution_data_modified": False,
        "status": audit_after["status"], "bound_inputs": {name: proof.to_dict() for name, proof in bound.items()},
        "files": {str(path.relative_to(output)): _sha(path) for path in sorted(output.rglob("*")) if path.is_file()}}
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    return report
