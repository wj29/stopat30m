"""Offline append-only master versions with verified independent collections."""
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from stopat30m.data.research_calendar import Calendar
from stopat30m.data.research_catalog import CatalogError, POOL, build_incremental_dataset, catalog_entries, publish_daily_update
from stopat30m.data.research_http import _hash
from stopat30m.data.research_prices import TencentResearchProvider, download_research_prices

OLD_DAYS = ["2025-12-29", "2025-12-30", "2025-12-31"]
NEW_DAYS = ["2026-01-05", "2026-01-06", "2026-01-07"]


def source_row(day, inst, mode):
    value = 10 + POOL.index(inst) + int(day[-2:]) / 100
    op, close, high, low = value, value + .1, value + .2, value - .1
    if mode:
        op, close, high, low = [2 * x + .5 for x in (op, close, high, low)]
    return [day, *[f"{x:.3f}" for x in (op, close, high, low)], "1000.00", {}, "0", f"{value + .1:.2f}", "", ""]


def payload(inst, mode, days):
    field = mode + "day" if mode else "day"
    # 1000 hands = 100000 units; reported amount is in 10000 CNY.
    rows = [source_row(day, inst, mode) for day in days]
    for row in rows:
        row[8] = f"{(10 + POOL.index(inst) + int(row[0][-2:]) / 100 + .1) * 10:.2f}"
    return {"code": 0, "data": {inst.lower(): {field: rows, "version": "offline-fixture"}}}


class Metadata:
    source, source_url = "baostock", "https://www.baostock.com/"
    calendar_source = "baostock.query_trade_dates"
    def instrument_info(self, inst):
        return {"asset_type": "ETF", "ipo_date": "2013-01-01"}
    def fetch_calendar(self, start, end):
        assert (start, end) == (OLD_DAYS[0], OLD_DAYS[-1])
        return pd.DataFrame({"date": OLD_DAYS, "is_trading_day": [1, 1, 1]})


class HistorySession:
    def get(self, endpoint, **kwargs):
        parts = kwargs["params"]["param"].split(",")
        return SimpleNamespace(status_code=200, headers={}, text=json.dumps(payload(parts[0].upper(), parts[-1], OLD_DAYS)))


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2))


def rehash_wrapper(wrapper):
    wrapper["payload_sha256"] = _hash(wrapper["payload"])
    wrapper["record_sha256"] = _hash({key: wrapper[key] for key in ("request", "payload_sha256", "fetched_at")})


@pytest.fixture
def inputs(tmp_path):
    clock = lambda: 0
    provider = TencentResearchProvider(session=HistorySession(), calendar_provider=Metadata(),
        clock=clock, sleep=lambda seconds: None, random=lambda: 0)
    parent = download_research_prices(list(POOL), OLD_DAYS[0], OLD_DAYS[-1], tmp_path / "history", provider=provider)
    calendar = Calendar.from_sessions(NEW_DAYS, "2026-01-01", NEW_DAYS[-1],
        source="offline-complete-exchange-session-fixture", source_url="https://calendar.invalid/2026")
    calendar_path = calendar.save(tmp_path / "calendar.json")
    cache, generation = tmp_path / "recent", "a" * 32
    scope = {"provider": {"name": "tencent", "version": TencentResearchProvider.version,
        "endpoint": TencentResearchProvider.source_url}, "request": {"instruments": sorted(POOL),
        "start_date": "2025-12-30", "end_date": NEW_DAYS[-1], "adjustment": "hfq", "frequency": "1d"}}
    stamp = datetime.now(timezone.utc).isoformat()
    request = {"request": scope, "request_sha256": _hash(scope), "generation": generation}
    write_json(cache / "generations" / generation / "request.json", request)
    active = {**request, "created_at": stamp, "as_of_date": datetime.fromisoformat(stamp).astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat(), "status": "pending"}
    write_json(cache / "requests" / _hash(scope) / "active.json", active)
    wrappers = {}
    for inst in POOL:
        for mode in ("", "hfq"):
            days = OLD_DAYS + NEW_DAYS
            if inst == "SZ159915" and mode == "hfq":
                days = days[:-1]
            params = {"_var": f"kline_day{mode}2025", "param": f"{inst.lower()},day,2025-01-01,{NEW_DAYS[-1]},640,{mode}", "r": "0.8205512681390605"}
            identity = {"endpoint": TencentResearchProvider.source_url, "generation": generation, "params": params}
            wrapper = {"schema_version": 1, "request": identity, "payload": payload(inst, mode, days), "fetched_at": stamp}
            rehash_wrapper(wrapper)
            path = cache / "generations" / generation / "responses" / (_hash(identity) + ".json")
            write_json(path, wrapper); wrappers[(inst, mode)] = path
    return {"parent": parent, "cache": cache, "calendar": calendar_path, "output": tmp_path / "master", "wrappers": wrappers, "generation": generation}


def build(inputs, **kwargs):
    return build_incremental_dataset(inputs["parent"].path, inputs["cache"], inputs["calendar"], inputs["output"], **kwargs)


def test_append_only_dataset_uses_common_complete_watermark_preserves_parent_and_response_times(inputs):
    before = {str(p): p.read_bytes() for p in [*inputs["parent"].path.iterdir(), *inputs["cache"].rglob("*.json")]}
    artifact = build(inputs)
    assert artifact.manifest["request"]["end_date"] == "2026-01-06"
    assert artifact.prices.date.max() == pd.Timestamp("2026-01-06")
    assert len(artifact.prices) == 15 and len(artifact.calendar) == 5
    lineage = artifact.manifest["dataset_lineage"]
    assert artifact.manifest["dataset_kind"] == "append_only_multi_collection"
    assert artifact.manifest["atomic_supplier_vintage_verified"] is False
    assert lineage["parent"]["artifact_id"] == inputs["parent"].manifest["artifact_id"]
    assert lineage["per_security"]["SZ159915"]["last_observed_date"] == "2026-01-07"
    assert lineage["per_security"]["SZ159915"]["verified_through"] == "2026-01-06"
    assert lineage["recent_collection"]["generation"] == inputs["generation"]
    assert len(lineage["recent_collection"]["responses"]) == 6
    for inst in POOL:
        assert lineage["overlap_audit"][inst]["rows"] == 2
        assert artifact.manifest["coverage"][inst]["appended_rows"] == 2
    assert all(Path(p).read_bytes() == content for p, content in before.items())
    assert artifact.manifest["price_semantics"]["dividend_reinvestment_verified"] is False
    assert catalog_entries(inputs["output"])[0]["id"] == artifact.manifest["artifact_id"]


def test_through_preserves_boundary_and_idempotent_immutable_version(inputs):
    first = build(inputs, through="2026-01-05")
    assert first.prices.date.max() == pd.Timestamp("2026-01-05")
    again = build(inputs, through="2026-01-05")
    assert again.path == first.path and len(catalog_entries(inputs["output"])) == 1
    with pytest.raises(CatalogError, match="watermark"):
        build(inputs, through="2026-01-07")
    assert len(catalog_entries(inputs["output"])) == 1


def test_hole_cannot_be_skipped_even_when_all_sources_have_later_dates(inputs):
    path = inputs["wrappers"][("SH510300", "hfq")]
    wrapper = json.loads(path.read_text()); rows = wrapper["payload"]["data"]["sh510300"]["hfqday"]
    wrapper["payload"]["data"]["sh510300"]["hfqday"] = [row for row in rows if row[0] != "2026-01-06"]
    rehash_wrapper(wrapper); write_json(path, wrapper)
    assert build(inputs).manifest["request"]["end_date"] == "2026-01-05"


@pytest.mark.parametrize("change", ["payload", "metadata", "identity", "source"])
def test_tampered_cache_or_source_rejected_without_publication(inputs, change):
    path = inputs["wrappers"][("SH510300", "")]
    wrapper = json.loads(path.read_text())
    if change == "payload": wrapper["payload"]["code"] = 5
    elif change == "metadata": wrapper["fetched_at"] = "2020-01-01T00:00:00+00:00"
    elif change == "identity": wrapper["request"]["generation"] = "b" * 32
    else:
        request_path = path.parent.parent / "request.json"
        request = json.loads(request_path.read_text()); request["request"]["provider"]["name"] = "eastmoney"
        request["request_sha256"] = _hash(request["request"]);write_json(request_path, request)
    if change != "source": write_json(path, wrapper)
    with pytest.raises(CatalogError): build(inputs)
    assert not inputs["output"].exists()


def test_legitimate_new_collection_revision_requires_explicit_security_rebuild(inputs):
    path = inputs["wrappers"][("SH510500", "")]
    wrapper = json.loads(path.read_text()); row = next(row for row in wrapper["payload"]["data"]["sh510500"]["day"] if row[0] == "2025-12-30")
    row[5] = "1000.01"
    rehash_wrapper(wrapper);write_json(path, wrapper)
    with pytest.raises(CatalogError, match="SH510500.*2025-12-30.*raw_volume.*rebuild"):
        build(inputs)
    assert not inputs["output"].exists()


def test_multiple_generations_require_explicit_choice_and_never_mix(inputs):
    extra = inputs["cache"] / "generations" / ("b" * 32);extra.mkdir()
    with pytest.raises(CatalogError, match="Specify one"):
        build(inputs)
    assert build(inputs, generation=inputs["generation"]).manifest["request"]["end_date"] == "2026-01-06"


def test_catalog_tampering_is_detected(inputs):
    build(inputs)
    path = inputs["output"] / "catalog.json"; index = json.loads(path.read_text());index["versions"][0]["verified_through"] = "2099-01-01";write_json(path,index)
    with pytest.raises(CatalogError, match="catalog checksum"):
        catalog_entries(inputs["output"])


def test_response_symlink_is_rejected(inputs, tmp_path):
    path = inputs["wrappers"][("SH510300", "")]; external=tmp_path / "external.json";external.write_bytes(path.read_bytes());path.unlink();path.symlink_to(external)
    with pytest.raises(CatalogError, match="symlink"):
        build(inputs)


def complete_recent(inputs):
    class RecentMetadata(Metadata):
        def fetch_calendar(self, start, end):
            days = pd.date_range(start, end).strftime("%Y-%m-%d").tolist()
            return pd.DataFrame({"date": days, "is_trading_day": [int(day in OLD_DAYS + NEW_DAYS) for day in days]})
    class RecentSession:
        def get(self, endpoint, **kwargs):
            parts = kwargs["params"]["param"].split(",")
            return SimpleNamespace(status_code=200, headers={}, text=json.dumps(payload(parts[0].upper(), parts[-1], OLD_DAYS + NEW_DAYS)))
    root = inputs["output"].parent / "daily"
    provider = TencentResearchProvider(session=RecentSession(), calendar_provider=RecentMetadata(),
        clock=lambda: datetime.now(timezone.utc).timestamp(), sleep=lambda seconds: None, random=lambda: 0)
    recent = download_research_prices(list(POOL), "2025-12-30", "2026-01-07", root, provider=provider)
    return recent, root


def test_daily_publication_uses_current_master_then_replays_idempotently(inputs):
    first = build(inputs)
    recent, cache = complete_recent(inputs)
    updated = publish_daily_update(inputs["parent"].path, recent, cache, inputs["calendar"], inputs["output"])
    assert updated.manifest["request"]["end_date"] == "2026-01-07"
    assert updated.manifest["dataset_lineage"]["parent"]["artifact_id"] == first.manifest["artifact_id"]
    assert len(catalog_entries(inputs["output"])) == 2
    pointer = (inputs["output"] / "catalog.json").read_bytes()
    repeated = publish_daily_update(inputs["parent"].path, recent.path, cache / "http" / "tencent", inputs["calendar"], inputs["output"])
    assert repeated.path == updated.path
    assert (inputs["output"] / "catalog.json").read_bytes() == pointer


def test_daily_publication_requires_exact_source_generation_not_a_composed_snapshot(inputs):
    master = build(inputs)
    with pytest.raises(CatalogError, match="generation"):
        publish_daily_update(inputs["parent"].path, master, inputs["cache"], inputs["calendar"], inputs["output"])


def test_daily_provenance_cannot_be_silently_rebound_to_changed_cache(inputs):
    recent, cache = complete_recent(inputs)
    path = next((cache / "http" / "tencent" / "generations").glob("*/responses/*.json"))
    wrapper = json.loads(path.read_text())
    wrapper["payload"]["code"] = 5
    rehash_wrapper(wrapper);write_json(path, wrapper)
    with pytest.raises(CatalogError, match="provenance differs"):
        publish_daily_update(inputs["parent"].path, recent, cache, inputs["calendar"], inputs["output"])
    assert not inputs["output"].exists()


def test_current_pointer_is_validated_even_when_catalog_checksum_is_recomputed(inputs):
    build(inputs)
    path = inputs["output"] / "catalog.json"; index = json.loads(path.read_text())
    index["current"] = "f" * 64
    from stopat30m.data.research_prices import _json_bytes, _sha
    index["sha256"] = _sha(_json_bytes({k: v for k, v in index.items() if k != "sha256"}))
    write_json(path,index)
    with pytest.raises(CatalogError, match="current pointer"):
        catalog_entries(inputs["output"])
