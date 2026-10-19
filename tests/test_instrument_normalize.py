"""ETF and index identifiers must not silently choose the wrong exchange."""

from stopat30m.data.normalize import normalize_instrument, to_baostock_code


def test_bare_shanghai_etf_uses_shanghai_market():
    assert normalize_instrument("510300") == "SH510300"
    assert normalize_instrument("511010") == "SH511010"
    assert to_baostock_code("510500") == "sh.510500"


def test_shenzhen_etf_and_explicit_index_keep_exchange():
    assert normalize_instrument("159915") == "SZ159915"
    assert normalize_instrument("sh.000300") == "SH000300"
    assert normalize_instrument("000300.SH") == "SH000300"
