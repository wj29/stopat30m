"""Close update IO is mocked; tiny immutable Qlib fixtures exercise publication."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import zipfile

import numpy as np
import pandas as pd
import pytest

from stopat30m.data import ashare_daily_update as module
from stopat30m.data.ashare_daily import _save
from stopat30m.data.ashare_execution_data import EXECUTION_FIELDS
from stopat30m.data.qlib_snapshot import prepare_qlib_snapshot, load_qlib_snapshot
from stopat30m.data.research_calendar import Calendar

SYMBOLS = [f"SH{600000+i:06d}" for i in range(21)]
EXTRA = "SZ000001"
DAYS = pd.bdate_range("2026-08-03", periods=26).strftime("%Y-%m-%d").tolist()
DAY = DAYS[-1]
INSTANT = datetime.fromisoformat(DAY+"T08:00:00+00:00")


class Response:
    error_code = "0"
    def __init__(self, fields, rows):
        self.fields, self.rows, self.i = fields, rows, -1
    def next(self):
        self.i += 1
        return self.i < len(self.rows)
    def get_row_data(self):
        return self.rows[self.i]


class Source:
    def __init__(self):
        self.calls = []
        self.defect = None
    def login(self):
        return SimpleNamespace(error_code="0")
    def logout(self):
        pass
    def query_hs300_stocks(self, **request):
        self.calls.append(("index", request))
        day = DAYS[-2] if self.defect == "index_date" else request["date"]
        return Response(["date", "code", "code_name"], [[day, s[:2].lower()+"."+s[2:], "fixture"] for s in SYMBOLS])
    def query_stock_basic(self):
        self.calls.append(("basic", {}))
        return Response(["code", "ipoDate", "outDate", "type", "status"],
            [[s[:2].lower()+"."+s[2:], "1999-11-10", "", "1", "1"] for s in [*SYMBOLS, EXTRA]])
    def query_history_k_data_plus(self, **request):
        self.calls.append(("history", request))
        factor = 2 if request["adjustflag"] == "1" else 1
        rows = []
        symbol = request["code"].replace(".", "").upper()
        for day in DAYS:
            if self.defect == "missing" and symbol == SYMBOLS[-1] and day == DAY:
                continue
            if self.defect == "mismatch" and request["adjustflag"] == "1" and symbol == SYMBOLS[-1] and day == DAY:
                continue
            status = "0" if self.defect == "halt" and symbol == SYMBOLS[-1] and day == DAY else "1"
            if self.defect == "insufficient" and symbol in SYMBOLS[-2:] and day == DAY:
                status = "0"
            price = 12 if self.defect == "seam" and symbol == SYMBOLS[-1] and day == DAYS[4] else 10
            preclose = "0" if self.defect == "preclose" and symbol == SYMBOLS[-1] and day == DAY else str(9.9*factor)
            rows.append([day, request["code"], str(price*factor), str((price+1)*factor), str((price-1)*factor),
                str(price*factor), preclose, "1000", str(price*1000), request["adjustflag"], status, "0"])
        return Response(EXECUTION_FIELDS.split(","), rows)


@pytest.fixture
def case(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "EXPECTED_INDEX_SIZE", 21)
    monkeypatch.setattr(module, "_now", lambda: INSTANT)
    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    source = Source()
    monkeypatch.setattr(module, "_client", lambda: source)
    archive = tmp_path / "seed.zip"
    with zipfile.ZipFile(archive, "w") as stream:
        stream.writestr("qlib/calendars/day.txt", "\n".join(DAYS[:-1])+"\n")
        stream.writestr("qlib/calendars/day_future.txt", "\n".join(DAYS)+"\n")
        members = "".join(s+"\t"+DAYS[0]+"\t"+DAYS[-2]+"\n" for s in SYMBOLS)
        stream.writestr("qlib/instruments/all.txt", members+EXTRA+"\t"+DAYS[0]+"\t"+DAYS[-2]+"\n")
        stream.writestr("qlib/instruments/csi300.txt", members)
        for symbol in [*SYMBOLS, EXTRA]:
            for field, value in {"open":1., "high":1.1, "low":.9, "close":1., "factor":.1, "volume":100., "vwap":1.}.items():
                stream.writestr(f"qlib/features/{symbol.lower()}/{field}.day.bin",np.asarray([0, *([value]*25)], dtype="<f4").tobytes())
    parent = prepare_qlib_snapshot(archive,tmp_path/"versions",source={"kind":"community","url":"fixture:seed"},
        expected_sha256=hashlib.sha256(archive.read_bytes()).hexdigest())
    flags = [{"date":str(d.date()),"is_open":int(str(d.date()) in DAYS)} for d in pd.date_range(DAYS[0],DAY)]
    calendar = Calendar.from_rows(flags,DAYS[0],DAY,source="fixture:explicit-sessions",source_url="fixture:calendar")
    path = tmp_path/"calendar.json"
    calendar.save(path)
    return SimpleNamespace(parent=parent, source=source, calendar=path, root=tmp_path/"daily")


def refresh(case, **kwargs):
    return module.refresh_ashare_day(case.parent.path,DAY,case.root,case.calendar,now=INSTANT,**kwargs)


def hashes(root):
    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob("*") if p.is_file()}


def test_complete_refresh_registers_immutable_version_and_cache_restart_does_not_download(case):
    before = hashes(case.parent.dataset_dir)
    result = refresh(case)
    assert result["signal_ready"] and len(result["current_instruments"]) == 21
    quote = result["daily_quotes"][SYMBOLS[0]]
    assert quote["raw_preclose"] == 9.9 and quote["raw_close"] == 10
    assert quote["factor"] == pytest.approx(.1)
    assert result["metadata"][SYMBOLS[0]]["board"] == "sse_main"
    assert result["universe_evidence"]["historical_membership_backfilled"] is False
    child = load_qlib_snapshot(result["snapshot_path"])
    assert child.manifest["audit"]["calendar"]["end"] == DAY
    assert (child.dataset_dir/"instruments/csi300.txt").read_bytes() == (case.parent.dataset_dir/"instruments/csi300.txt").read_bytes()
    assert hashes(case.parent.dataset_dir) == before
    calls = len(case.source.calls)
    assert refresh(case)["result_sha256"] == result["result_sha256"]
    assert len(case.source.calls) == calls


@pytest.mark.parametrize("defect", ["halt", "missing"])
def test_known_halt_or_absent_day_is_explicit_and_does_not_advance_finite_watermark(case, defect):
    case.source.defect = defect
    result = refresh(case)
    assert len(result["current_instruments"]) == 21 and len(result["eligible_current_instruments"]) == 20 and result["signal_ready"]
    assert SYMBOLS[-1] in result["excluded"]
    quote = result["daily_quotes"][SYMBOLS[-1]]
    assert quote["factor"] is None and quote["close"] is None
    child = load_qlib_snapshot(result["snapshot_path"])
    assert child.manifest["audit"]["instruments"][SYMBOLS[-1]]["last_complete_bar_date"] == DAYS[-2]


@pytest.mark.parametrize("defect", ["mismatch", "seam", "preclose", "index_date"])
def test_pair_seam_reference_or_universe_failure_never_publishes_daily_result(case, defect):
    case.source.defect = defect
    before = hashes(case.parent.dataset_dir)
    with pytest.raises(ValueError):
        refresh(case)
    assert not (case.root/DAY/"refresh_result.json").exists()
    assert not (case.root/DAY/"versions").exists()
    assert hashes(case.parent.dataset_dir) == before
    assert list((case.root/DAY).glob("failure_*.json"))


def test_outside_close_phase_or_frozen_output_is_rejected_before_network_or_write(case):
    with pytest.raises(ValueError, match="15:05"):
        module.refresh_ashare_day(case.parent.path,DAY,case.root,case.calendar,
            now=datetime.fromisoformat(DAY+"T06:30:00+00:00"))
    assert not case.root.exists() and not case.source.calls
    with pytest.raises(ValueError, match="immutable"):
        module.refresh_ashare_day(case.parent.path,DAY,case.parent.path/"bad",case.calendar,now=INSTANT)
    assert not (case.parent.path/"bad").exists() and not case.source.calls


def test_committed_original_tamper_is_rejected_on_cache_reuse(case):
    result = refresh(case)
    p = Path(result["collection_path"])/(SYMBOLS[0]+"_raw.json")
    p.write_bytes(p.read_bytes()+b" ")
    with pytest.raises(ValueError, match="checksum"):
        refresh(case)


def test_extra_held_security_is_refreshed_without_joining_current_signal_pool(case):
    result = refresh(case, extra_instruments=[EXTRA])
    assert EXTRA in result["daily_quotes"] and EXTRA in result["metadata"]
    assert result["daily_quotes"][EXTRA]["close"] == 10
    assert result["extra_instruments"] == [EXTRA]
    assert EXTRA not in result["current_instruments"] and EXTRA not in result["current_index_instruments"]
    assert len(result["current_instruments"]) == 21


def test_extra_quotes_cannot_make_up_the_minimum_twenty_current_members(case):
    case.source.defect = "insufficient"
    with pytest.raises(ValueError, match="20"):
        refresh(case, extra_instruments=[EXTRA])
    assert not (case.root/DAY/"versions").exists()


def test_today_board_range_includes_302_but_excludes_depositary_receipts():
    assert module._board("SZ302132") == "chinext"
    with pytest.raises(ValueError, match="board"):
        module._board("SZ309800")


def test_completed_originals_survive_registration_failure_and_resume_without_login(case, monkeypatch):
    original = module.prepare_qlib_directory
    def unavailable(*args, **kwargs):
        raise ValueError("synthetic registration interruption")
    monkeypatch.setattr(module,"prepare_qlib_directory",unavailable)
    with pytest.raises(ValueError,match="interruption"):
        refresh(case)
    raw_path=case.root/DAY/"originals"/(SYMBOLS[0]+"_raw.json")
    before=raw_path.read_bytes()
    monkeypatch.setattr(module,"prepare_qlib_directory",original)
    def no_login():
        raise AssertionError("complete original evidence must resume offline")
    monkeypatch.setattr(module,"_client",no_login)
    result=refresh(case)
    assert result["signal_ready"] and raw_path.read_bytes()==before
    assert result["daily_quotes"][SYMBOLS[0]]["bar_complete"] is True


def test_daily_ipo_derivation_is_computed_and_cannot_waive_a_later_mixed_flag():
    days = ["2022-01-26", "2022-01-27", "2022-01-28"]
    rows = [[d,"sh.688223","10","11","9","10","9.9","1000","10000","3","1","0"] for d in days]
    raw = pd.DataFrame(rows,columns=EXECUTION_FIELDS.split(","))
    hfq = raw.copy()
    hfq.loc[1:,"adjustflag"]="1"
    for field in ["open","high","low","close"]:
        hfq.loc[2,field]=str(float(hfq.loc[2,field])*2)
    basic=pd.DataFrame([["sh.688223",days[0],"1"]],columns=["code","ipoDate","type"])
    request={"start_date":"2000-01-04","end_date":days[-1]}
    factor={"query":"query_adjust_factor","request":{"code":"sh.688223",**request},
        "fields":["code","dividOperateDate","backAdjustFactor"],
        "rows":[["sh.688223",days[1],"1"],["sh.688223",days[2],"2"]]}
    original=hfq.copy(deep=True)
    adjusted,proof=module._derive_ipo_pair(raw,hfq,basic,factor,request,{"sha256":"fixture"})
    assert adjusted.close.tolist()==[10,10,20] and "adjustflag" not in adjusted
    assert proof["original_flags_modified"] is False
    pd.testing.assert_frame_equal(hfq,original)
    hfq.loc[1,"adjustflag"]="3"
    with pytest.raises(ValueError,match="IPO unit-factor"):
        module._derive_ipo_pair(raw,hfq,basic,factor,request,{"sha256":"fixture"})


def test_offline_bootstrap_requires_today_index_and_current_price_factor_agreement(case, tmp_path):
    result = refresh(case)
    original = Path(result["collection_path"])
    capture = json.loads((original/"collection.json").read_bytes())
    execution = tmp_path/"execution"
    execution.mkdir()
    receipt = {"kind":"baostock_daily_execution_evidence","source":"baostock","generation":capture["generation"],
        "as_of_date":DAY,"status":"complete","failures":{},"responses":{},"request":{
            "start_date":DAYS[0],"end_date":DAY,"instruments":SYMBOLS,
            "selection_evidence_path":str(case.parent.dataset_dir/"instruments/csi300.txt"),
            "selection_evidence_sha256":hashlib.sha256((case.parent.dataset_dir/"instruments/csi300.txt").read_bytes()).hexdigest()}}
    for key in ["stock_basic",*SYMBOLS]:
        old_key=key if key=="stock_basic" else key+"_raw"
        record=capture["responses"][old_key]
        raw=(original/record["file"]).read_bytes()
        filename=key+".json"
        (execution/filename).write_bytes(raw)
        receipt["responses"][key]={**record,"file":filename}
    _save(execution/"execution_collection.json",receipt)
    bootstrap = module.bootstrap_ashare_day(result["snapshot_path"],original/"current_index.json",execution,
        case.calendar,session=DAY,output_dir=tmp_path/"bootstrap")
    assert bootstrap["bootstrap"] and len(bootstrap["current_instruments"])==21
    assert module.load_ashare_refresh(tmp_path/"bootstrap/refresh_result.json")["result_sha256"] == bootstrap["result_sha256"]
    assert module.bootstrap_ashare_day(result["snapshot_path"],original/"current_index.json",execution,
        case.calendar,session=DAY,output_dir=tmp_path/"bootstrap")["result_sha256"] == bootstrap["result_sha256"]
    bad=json.loads((original/"current_index.json").read_bytes())
    bad["request"]["date"]=DAYS[-2]
    _save(tmp_path/"old_index.json",bad)
    with pytest.raises(ValueError, match="exact session"):
        module.bootstrap_ashare_day(result["snapshot_path"],tmp_path/"old_index.json",execution,case.calendar,session=DAY)
    with pytest.raises(ValueError, match="factor"):
        module.bootstrap_ashare_day(case.parent.path,original/"current_index.json",execution,case.calendar,session=DAY)
