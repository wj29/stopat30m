"""Prospective source observations cannot turn old opens into new paper fills."""
from datetime import datetime
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from stopat30m.data.ashare_live import fetch_tencent_execution_quotes, load_live_observations, parse_tencent_observations

TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 10, 20, 9, 35, tzinfo=TZ)


def payload(*, code="600000", stamp="20261020093459", price="10.12", opening="10.00"):
    fields = [""] * 31
    for n, value in {0:"1",1:"浦发银行",2:code,3:price,4:"9.99",5:opening,6:"1234",9:"10.11",10:"25",19:"10.12",20:"11",30:stamp}.items():
        fields[n] = value
    return ('v_sh600000="' + "~".join(fields) + '";\n').encode("gb18030")


class Session:
    def __init__(self, raw=None, status=200):
        self.raw, self.status, self.calls = raw or payload(), status, 0
    def get(self, *_args, **kwargs):
        assert kwargs["allow_redirects"] is False
        self.calls += 1
        return SimpleNamespace(status_code=self.status, content=self.raw)


def test_actual_latest_quote_stays_distinct_from_the_open():
    batch = parse_tencent_observations(payload(), ["SH600000"], now=NOW)
    quote = batch["quotes"]["SH600000"]
    assert batch["complete"]
    assert quote["execution_price"] == "10.12" and quote["open"] == "10.00"
    assert quote["execution_price_basis"] == "live_observed_quote"
    assert quote["asof"] == "2026-10-20T09:34:59+08:00"
    assert quote["guaranteed_fill"] is False


@pytest.mark.parametrize("stamp", ["20261019150000", "20261020093000", "20261020093600"])
def test_stale_previous_day_and_future_quotes_cannot_authorize_fills(stamp):
    batch = parse_tencent_observations(payload(stamp=stamp), ["SH600000"], now=NOW)
    assert not batch["complete"] and not batch["quotes"]
    assert "SH600000" in batch["rejected"]


def test_wrong_code_or_zero_open_is_rejected_without_guessing_a_halt():
    for raw in (payload(code="600001"), payload(opening="0")):
        batch = parse_tencent_observations(raw, ["SH600000"], now=NOW)
        assert not batch["quotes"] and not batch["complete"]


def test_no_network_or_output_outside_the_execution_window(tmp_path):
    client = Session()
    with pytest.raises(ValueError, match="outside"):
        fetch_tencent_execution_quotes(["SH600000"], tmp_path / "absent", now=NOW.replace(hour=16), session=client)
    assert client.calls == 0 and not (tmp_path / "absent").exists()


def test_original_bytes_are_saved_and_old_receipts_need_fresh_revalidation(tmp_path):
    batch = fetch_tencent_execution_quotes(["SH600000"], tmp_path / "live", now=NOW, session=Session())
    assert batch["complete"] and (tmp_path / "live/response_000.txt").read_bytes() == payload()
    assert load_live_observations(tmp_path / "live", now=NOW)["complete"]
    later = load_live_observations(tmp_path / "live", now=NOW.replace(minute=40))
    assert not later["complete"] and not later["quotes"]
    audit = load_live_observations(tmp_path / "live", audit_only=True)
    assert audit["complete"] and not audit["quotes"]["SH600000"]["live_execution_eligible"]
    receipt = tmp_path / "live/receipt.json"
    stored = json.loads(receipt.read_text())
    stored["quotes"]["SH600000"]["execution_price"] = "1.00"
    receipt.write_text(json.dumps(stored))
    with pytest.raises(ValueError, match="differ"):
        load_live_observations(tmp_path / "live", audit_only=True)


def test_rate_limit_stops_without_retry_or_source_fallback(tmp_path):
    client = Session(status=429)
    with pytest.raises(ValueError, match="429"):
        fetch_tencent_execution_quotes(["SH600000"], tmp_path / "live", now=NOW, session=client)
    assert client.calls == 1 and (tmp_path / "live/failure.json").is_file()
    assert not (tmp_path / "live/receipt.json").exists()
