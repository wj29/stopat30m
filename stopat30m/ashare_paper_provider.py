"""Explicit public-data adapter for the isolated A-share forward runner.

Daily bars and live observations are distinct evidence. No brokerage, real
portfolio, global configuration, or process environment is accessed here.
"""
from __future__ import annotations

from datetime import datetime, time, timezone
import hashlib
import json
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _now():
    return datetime.now(timezone.utc)


def _proof(path, expected=None, url=None):
    original = Path(path)
    path = original.resolve()
    if original.is_symlink() or not path.is_file():
        raise ValueError("An actual immutable source evidence file is required")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if expected is not None and digest != expected:
        raise ValueError("Public source evidence bytes differ from their receipt")
    return {"path": str(path), "sha256": digest, **({"url": url} if url else {})}


def _unique(proofs):
    result = {}
    for record in proofs:
        previous = result.get(record["path"])
        if previous is not None and previous["sha256"] != record["sha256"]:
            raise ValueError("A source evidence path changed within one market batch")
        result[record["path"]] = record
    return list(result.values())


class AsharePaperProvider:
    def __init__(self, paper_dir, adapter):
        from stopat30m.data.qlib_snapshot import guard_qlib_output_path
        self.paper_dir = Path(paper_dir).resolve()
        self.adapter = adapter
        def path(name, *, optional=False):
            value = adapter.get(name)
            if optional and not value:
                return None
            if not isinstance(value, str) or not value:
                raise ValueError("A-share paper adapter path is missing: " + name)
            candidate = Path(value)
            return (self.paper_dir / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
        self.initial_refresh_path = path("initial_refresh_path")
        self.calendar_path = path("calendar_path")
        self.output_dir = guard_qlib_output_path(path("provider_output_dir"))
        self.overrides_path = path("dividend_overrides_path", optional=True)
        self.initial_dividends = path("initial_dividend_collection", optional=True)

    @classmethod
    def from_paper_dir(cls, paper_dir):
        root = Path(paper_dir).resolve()
        adapter = json.loads((root / "adapter.json").read_text())
        if not isinstance(adapter, dict):
            raise ValueError("A-share paper adapter must be a JSON object")
        return cls(root, adapter)

    def _state(self):
        from stopat30m.ashare_forward import _digest
        manifest = json.loads((self.paper_dir / "manifest.json").read_text())
        state = json.loads((self.paper_dir / "state.json").read_text())
        if (manifest.get("manifest_sha256") != _digest({key: value for key, value in manifest.items() if key != "manifest_sha256"})
                or state.get("manifest_sha256") != manifest["manifest_sha256"]
                or state.get("state_sha256") != _digest({key: value for key, value in state.items() if key != "state_sha256"})):
            raise ValueError("Paper adapter cannot read an unverified paper state")
        return manifest, state

    def _session(self, session, phase):
        from stopat30m.data.research_calendar import Calendar
        calendar = Calendar.load(self.calendar_path)
        local = _now().astimezone(SHANGHAI)
        if session != local.date().isoformat() or not calendar.is_session(session):
            raise ValueError("Provider requires today's explicitly covered real exchange session")
        if phase == "open" and not time(9, 30) <= local.time() <= time(9, 45):
            raise ValueError("Provider will not query outside the prospective opening window")
        if phase == "close" and local.time() < time(15, 5):
            raise ValueError("Provider will not query an incomplete close session")
        return calendar

    def _latest_refresh(self, session, *, before=False):
        from stopat30m.data.ashare_daily_update import load_ashare_refresh
        paths = {self.initial_refresh_path, *self.output_dir.joinpath("daily").glob("*/refresh_result.json")}
        choices = []
        for path in sorted(paths):
            result = load_ashare_refresh(path)
            if result["session"] < session or result["session"] == session and not before:
                choices.append((result["session"], str(path), result, path))
        if not choices:
            raise ValueError("No verified preceding close refresh exists for this paper session")
        _, _, result, path = max(choices, key=lambda item: item[:2])
        return result, path

    def _refresh_proofs(self, result, result_path):
        source = result["source_evidence"]
        collection = Path(result["collection_path"])
        proofs = [_proof(result_path), _proof(Path(result["snapshot_path"]) / "manifest.json"),
                  _proof(self.calendar_path)]
        for row in source.get("bootstrap_inputs", []):
            proofs.append(_proof(row["path"], row["sha256"]))
        for row in source["raw_responses"].values():
            proofs.append(_proof(Path(row.get("source_root", collection)) / row["file"], row["sha256"]))
        for row in source.get("responses", {}).values():
            proofs.append(_proof(collection / row["file"], row["sha256"]))
        for filename in ("collection.json", "execution_collection.json"):
            if (collection / filename).exists():
                proofs.append(_proof(collection / filename))
                receipt = json.loads((collection / filename).read_text())
                if "stock_basic" in receipt.get("responses", {}):
                    row = receipt["responses"]["stock_basic"]
                    proofs.append(_proof(collection / row["file"], row["sha256"]))
        universe = result["universe_evidence"]
        file = Path(universe["file"])
        if not file.is_absolute():
            file = collection / file
        universe_proof = _proof(file, universe["sha256"])
        proofs.append(universe_proof)
        return _unique(proofs), [universe_proof]

    def _dividends(self, session, phase, manifest, state, work):
        from stopat30m.ashare_forward import _account_instruments
        from stopat30m.data.ashare_actions import collect_dividend_evidence, load_dividend_evidence, dividend_events, apply_action_overrides
        held = sorted(set().union(*(_account_instruments(account) for account in state["accounts"].values())))
        if held:
            collection = collect_dividend_evidence(work / "dividends", held, [int(session[:4])], min_interval=.6, timeout=8)
        elif self.initial_dividends is not None:
            collection = self.initial_dividends
        else:
            return [], []
        _, receipt = load_dividend_evidence(collection)
        proofs = [_proof(Path(collection) / "collection.json")]
        for record in receipt["responses"].values():
            proofs.append(_proof(Path(collection) / record["file"], record["sha256"]))
        events = dividend_events(collection, start="2000-01-01", end=session)
        initialization = datetime.fromisoformat(manifest["created_at"]).astimezone(SHANGHAI).date().isoformat()
        # A held-only query must not apply an unrelated historical override or
        # pretend the unqueried stock's empty source response was observed.
        if self.overrides_path is not None:
            document = json.loads(self.overrides_path.read_text())
            applicable = []
            keys = {(row["instrument"], row["ex_date"]) for row in events}
            for patch in document["patches"]:
                key = patch["instrument"], patch["ex_date"]
                if key in keys or (patch["operation"] == "add" and patch["instrument"] in receipt["instruments"]
                        and initialization <= patch["ex_date"] <= session):
                    record = dict(patch)
                    proof = patch["evidence"]
                    file = (self.overrides_path.parent / proof["path"]).resolve()
                    proofs.append(_proof(file, proof["sha256"], proof.get("url")))
                    record["evidence"] = {**proof, "path": str(file)}
                    applicable.append(record)
            proofs.append(_proof(self.overrides_path))
            if applicable:
                work.mkdir(parents=True, exist_ok=True)
                scoped = work / "applied_action_overrides.json"
                scoped.write_text(json.dumps({**document, "patches": applicable}, ensure_ascii=False, indent=2) + "\n")
                proofs.append(_proof(scoped))
                events = apply_action_overrides(events, scoped)
        return [row for row in events if initialization <= row["ex_date"] <= session], _unique(proofs)

    def fetch_close(self, session, instruments, now):
        self._session(session, "close")
        manifest, state = self._state()
        from stopat30m.data.ashare_daily_update import refresh_ashare_day, load_ashare_refresh
        result, result_path = self._latest_refresh(session)
        if result["session"] != session or not set(instruments).issubset(result["daily_quotes"]):
            result = refresh_ashare_day(result["snapshot_path"], session, self.output_dir / "daily",
                self.calendar_path, extra_instruments=instruments)
            result_path = self.output_dir / "daily" / session / "refresh_result.json"
            result = load_ashare_refresh(result_path)
        if result["session"] != session:
            raise ValueError("Daily close did not publish the requested actual session")
        proofs, universe = self._refresh_proofs(result, result_path)
        work = self.output_dir / "observations" / f"{session}_close_{uuid4().hex}"
        events, action_proofs = self._dividends(session, "close", manifest, state, work)
        quotes = {key: {**row, "tradestatus": {"1": "trading", "0": "suspended"}.get(row["tradestatus"], row["tradestatus"]),
            "bar_complete": True} for key, row in result["daily_quotes"].items()}
        metadata = {key: {**row, "delisting_period": bool(row.get("out_date") and row["out_date"] <= session)}
                    for key, row in result["metadata"].items()}
        return {"phase": "close", "session": session, "source": "baostock.daily_raw",
            "fetched_at": _now().isoformat(), "quotes": quotes, "metadata": metadata,
            "instruments": result["current_index_instruments"], "snapshot_path": result["snapshot_path"],
            "universe_evidence": universe, "evidence": _unique([*proofs, *action_proofs]),
            "corporate_actions": events, "source_exclusions": result["excluded"], "live_opening_quote": False}

    def fetch_open(self, session, instruments, now):
        self._session(session, "open")
        manifest, state = self._state()
        result, result_path = self._latest_refresh(session, before=True)
        from stopat30m.data.ashare_live import fetch_tencent_execution_quotes
        work = self.output_dir / "observations" / f"{session}_open_{uuid4().hex}"
        events, action_proofs = self._dividends(session, "open", manifest, state, work)
        live_dir = work / "live"
        # Production must sample actual receive time. Passing the runner start
        # clock here would make later source timestamps falsely future/stale.
        live = fetch_tencent_execution_quotes(instruments, live_dir, now=None, require_live=True,
            timeout=8, min_interval=.6)
        proofs, _ = self._refresh_proofs(result, result_path)
        proofs.append(_proof(live_dir / "receipt.json"))
        fetched = {}
        for record in live["responses"]:
            proofs.append(_proof(live_dir / record["file"], record["sha256"], record["url"]))
            fetched.update({key: record["fetched_at"] for key in record["instruments"]})
        quotes, metadata = {}, {}
        for instrument, row in live["quotes"].items():
            if row.get("live_execution_eligible") is not True:
                continue
            quotes[instrument] = {**row, "as_of": row["asof"], "fetched_at": fetched[instrument],
                "tradestatus": {"1": "trading", "0": "suspended"}.get(row["tradestatus"], row["tradestatus"])}
            static = result["metadata"].get(instrument)
            if static is not None:
                metadata[instrument] = {**static, "is_st": row["is_st"], "status_session": session,
                    "known_at": row["asof"], "source": "baostock.static_identity+tencent.current_quote_status",
                    "delisting_period": bool(static.get("out_date") and static["out_date"] <= session)}
        return {"phase": "open", "session": session, "source": live["source"], "fetched_at": _now().isoformat(),
            "quotes": quotes, "metadata": metadata, "evidence": _unique([*proofs, *action_proofs]),
            "corporate_actions": events, "source_rejections": live["rejected"], "guaranteed_fill": False}
