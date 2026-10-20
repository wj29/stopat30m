import json
from decimal import Decimal

import pytest

from stopat30m.data import a5_code_clock_adjustment as clock
from stopat30m.data.ashare_daily import FIELDS
from stopat30m.data.ashare_repair import FACTOR_DOCUMENT


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n")


def fixture(tmp_path):
    root = tmp_path / "prices"; root.mkdir()
    factors = tmp_path / "factors"; factors.mkdir()
    guards = tmp_path / "guards"; guards.mkdir()
    days = ["2017-01-03", "2025-02-17", "2025-02-18", "2025-06-17", "2026-10-19"]
    calendar = tmp_path / "calendar.txt"; calendar.write_text("\n".join(days) + "\n")
    event_source = tmp_path / "issuer.pdf"; event_source.write_bytes(b"%PDF-synthetic-code-identity")
    event = {"event_type": "same_issuer_security_code_change", "new_instrument": "SZ302132",
        "old_instrument": "SZ300114", "same_legal_issuer": True, "share_quantity_changes": False,
        "price_mapping_allowed": False, "effective_on": "2025-02-17", "first_effective_session": "2025-02-17",
        "effective_basis": "at_session_open", "known_at": "2025-02-13T23:59:59+08:00",
        "evidence": [{**clock._proof(event_source), "url": "https://fixture.invalid/issuer.pdf", "pdf_pages_1based": [1]}]}
    event_path = tmp_path / "events.json"; save(event_path, {"events": [event]})
    manifest = {"source": "baostock", "instruments": ["SZ302132"], "generation": "price-generation",
        "as_of_date": "2026-10-20", "request_start": "2017-01-01", "request_end": "2026-10-19",
        "responses": {}, "status": "partial_not_publishable"}
    pg = {}; originals = {}
    def guard(path, payload, request, anomalies=(), daily=False):
        save(path, {"status": "verified", "server_error_code": "0", "request": request,
            "server_echo": {**request, **({"adjustflag": ""} if daily else {})},
            "rows_verified": len(payload["rows"]), "complete_row_iteration_observed": True,
            "started_at": "2026-10-20T06:00:00+00:00", "finished_at": "2026-10-20T06:00:01+00:00",
            "row_mode_anomalies_observed": list(anomalies)})
    for mode, flag in [("raw", "3"), ("hfq", "1")]:
        request = {"code": "sz.302132", "fields": FIELDS, "start_date": manifest["request_start"],
                   "end_date": manifest["request_end"], "frequency": "d", "adjustflag": flag}
        rows = []
        for n, day in enumerate(days):
            price = Decimal(100 - n)
            factor = Decimal("1") if n <= 1 else Decimal("6") if n == 2 else Decimal("6.6")
            adjusted = price * factor if mode == "hfq" else price
            rows.append([day, "sz.302132", *[str(adjusted)] * 4, "100", str(price * 100),
                         "3" if mode == "raw" or n == 0 else "1", "1", "0"])
        payload = {"request": request, "fields": FIELDS.split(","), "rows": rows,
                   "generation": manifest["generation"], "fetched_at": "2026-10-20T06:00:02+00:00"}
        path = root / ("SZ302132_" + mode + ".json"); save(path, payload); originals[mode] = clock._proof(path)
        manifest["responses"]["SZ302132_" + mode] = {"file": path.name, "sha256": clock._proof(path)["sha256"],
            "rows": len(rows), "fetched_at": payload["fetched_at"]}
        gp = guards / (mode + ".json")
        anomalies = [{"date": days[0], "requested_mode": "1", "returned_row_mode": "3"}] if mode == "hfq" else []
        guard(gp, payload, request, anomalies, daily=True); pg[mode] = clock._proof(gp)
    save(root / "collection.json", manifest)
    pdf = tmp_path / "method.pdf"; pdf.write_bytes(b"%PDF-synthetic-official-factor-method")
    method = tmp_path / "method.json"; save(method, {"file": pdf.name, "sha256": clock._proof(pdf)["sha256"], "source_url": FACTOR_DOCUMENT})
    fm = {"kind": "baostock_official_factor_identity_evidence", "source": "baostock", "status": "complete",
          "instrument": "SZ302132", "generation": "factor-generation", "request_start": manifest["request_start"],
          "request_end": manifest["request_end"], "responses": {},
          "method_evidence": {"receipt_path": str(method), "receipt_sha256": clock._proof(method)["sha256"]}}
    fg = {}
    for key in ["identity", "factors"]:
        request = {"code": "sz.302132"} if key == "identity" else {
            "code": "sz.302132", "start_date": manifest["request_start"], "end_date": manifest["request_end"]}
        fields = ["code", "code_name", "ipoDate", "outDate", "type", "status"] if key == "identity" else [
            "code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"]
        rows = [["sz.302132", "fixture", "2010-08-27", "", "1", "1"]] if key == "identity" else [
            ["sz.302132", days[1], "0.909091", "1.000000", "1.000000"],
            ["sz.302132", days[2], "0.909091", "6.000000", "6.000000"],
            ["sz.302132", days[3], "1.000000", "6.600000", "1.100000"]]
        payload = {"query": "query_stock_basic" if key == "identity" else "query_adjust_factor", "request": request,
            "fields": fields, "rows": rows, "generation": fm["generation"], "fetched_at": "2026-10-20T06:00:02+00:00"}
        path = factors / (key + ".json"); save(path, payload)
        fm["responses"][key] = {"file": path.name, "sha256": clock._proof(path)["sha256"], "rows": len(rows), "fetched_at": payload["fetched_at"]}
        gp = guards / (key + ".json"); guard(gp, payload, {**request, "code_name": ""} if key == "identity" else request)
        fg[key] = clock._proof(gp)
    save(factors / "factor_evidence.json", fm)
    contract = {"kind": "a5_verified_current_code_forward_factor_derivation_contract",
        "status": "source_only_frozen_before_full_ohlc_derivation", "instrument": "SZ302132",
        "spec": clock._proof(clock.SPEC_PATH), "implementation": clock._proof(clock.__file__),
        "original_collection_manifest": clock._proof(root / "collection.json"),
        "factor_evidence_manifest": clock._proof(factors / "factor_evidence.json"),
        "original_raw_response": originals["raw"], "original_hfq_response": originals["hfq"],
        "price_guard_proofs": pg, "factor_guard_proofs": fg,
        "calendar": clock._proof(calendar), "official_code_events": clock._proof(event_path)}
    path = tmp_path / "contract.json"; save(path, contract)
    return root, factors, path


def test_uniform_legal_code_basis_uses_official_forward_ratios_and_keeps_sources(tmp_path):
    root, factors, contract = fixture(tmp_path)
    before = {p: p.read_bytes() for p in root.iterdir()}
    raw, adjusted, receipt = clock.derive_code_clock_forward_adjusted(root, factors, contract_path=contract)
    assert len(raw) == 4 and raw.date.iloc[0] == "2025-02-17"
    assert receipt["excluded_archived_alias_rows"][0]["date"] == "2017-01-03"
    assert adjusted.close.iloc[1] / adjusted.close.iloc[0] == pytest.approx(98 / 99)
    assert adjusted.close.iloc[-1] == pytest.approx(96 / .909091)
    assert adjusted.adjustflag.tolist() == ["1"] * 4
    assert adjusted.volume.tolist() == raw.volume.tolist() == ["100"] * 4
    assert all(path.read_bytes() == content for path, content in before.items())
    assert receipt["old_code_price_mapping_allowed"] is False
    assert receipt["independent_HFQ_supplier_confirmation_claimed"] is False


@pytest.mark.parametrize("target", ["price", "factor", "guard", "calendar", "event", "method"])
def test_rejects_changed_bound_originals(tmp_path, target):
    root, factors, contract = fixture(tmp_path)
    path = {"price": root / "SZ302132_hfq.json", "factor": factors / "factors.json",
        "guard": tmp_path / "guards/hfq.json", "calendar": tmp_path / "calendar.txt",
        "event": tmp_path / "events.json", "method": tmp_path / "method.pdf"}[target]
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError):
        clock.derive_code_clock_forward_adjusted(root, factors, contract_path=contract)


@pytest.mark.parametrize("mutation", ["discontinuous_fore", "incompatible_scale", "wrong_ipo", "malformed_schema", "missing_session", "bad_hfq", "price_mapping", "late_announcement"])
def test_rehashed_sources_cannot_waive_economic_identity_or_math(tmp_path, mutation):
    root, factors, contract_path = fixture(tmp_path)
    contract = json.loads(contract_path.read_bytes())
    if mutation in {"discontinuous_fore", "incompatible_scale", "wrong_ipo", "malformed_schema"}:
        key = "identity" if mutation == "wrong_ipo" else "factors"
        path = factors / (key + ".json"); payload = json.loads(path.read_bytes())
        if mutation == "wrong_ipo": payload["rows"][0][2] = "2025-02-17"
        if mutation == "discontinuous_fore": payload["rows"][0][2] = "0.900000"
        if mutation == "incompatible_scale": payload["rows"][-1][3] = "6.700000"
        if mutation == "malformed_schema": payload["fields"][-1] = "invalid"
        save(path, payload)
        fm = json.loads((factors / "factor_evidence.json").read_bytes())
        fm["responses"][key]["sha256"] = clock._proof(path)["sha256"]
        save(factors / "factor_evidence.json", fm)
        contract["factor_evidence_manifest"] = clock._proof(factors / "factor_evidence.json")
    elif mutation in {"missing_session", "bad_hfq"}:
        modes = ("raw", "hfq") if mutation == "missing_session" else ("hfq",)
        manifest = json.loads((root / "collection.json").read_bytes())
        for mode in modes:
            path = root / ("SZ302132_" + mode + ".json"); payload = json.loads(path.read_bytes())
            if mutation == "missing_session": payload["rows"].pop(2)
            else: payload["rows"][2][5] = "999"
            save(path, payload)
            record = manifest["responses"]["SZ302132_" + mode]
            record.update(sha256=clock._proof(path)["sha256"], rows=len(payload["rows"]))
            gp = tmp_path / ("guards/" + mode + ".json"); g = json.loads(gp.read_bytes()); g["rows_verified"] = len(payload["rows"]); save(gp, g)
            contract["price_guard_proofs"][mode] = clock._proof(gp)
            contract["original_" + mode + "_response"] = clock._proof(path)
        save(root / "collection.json", manifest)
        contract["original_collection_manifest"] = clock._proof(root / "collection.json")
    else:
        path = tmp_path / "events.json"; events = json.loads(path.read_bytes())
        if mutation == "price_mapping": events["events"][0]["price_mapping_allowed"] = True
        else: events["events"][0]["known_at"] = "2025-02-17T10:00:00+08:00"
        save(path, events); contract["official_code_events"] = clock._proof(path)
    save(contract_path, contract)
    with pytest.raises(ValueError):
        clock.derive_code_clock_forward_adjusted(root, factors, contract_path=contract_path)
