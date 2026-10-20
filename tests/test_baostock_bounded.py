"""The SDK may swallow normal exceptions while spinning on EOF."""
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import signal
import time

import pytest

from stopat30m.data.baostock_bounded import BaostockDeadlineClient


def swallowed_eof_loop():
    try:
        while True:
            time.sleep(0.005)
    except Exception:
        return "SDK swallowed an exception"


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="POSIX collector")
def test_hard_deadline_escapes_sdk_catch_and_restores_alarm():
    previous = signal.getsignal(signal.SIGALRM)
    source = BaostockDeadlineClient(SimpleNamespace(login=swallowed_eof_loop), timeout=0.025)
    with pytest.raises(TimeoutError, match="hard deadline"):
        source.login()
    assert signal.getitimer(signal.ITIMER_REAL) == (0, 0)
    assert signal.getsignal(signal.SIGALRM) == previous


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="POSIX collector")
def test_lazy_result_next_is_also_bounded_without_changing_attributes():
    original = SimpleNamespace(fields=["code"], error_code="0", next=swallowed_eof_loop,
                               get_row_data=lambda: ["sh.600000"])
    result = BaostockDeadlineClient(SimpleNamespace(query=lambda: original), timeout=0.025).query()
    assert result.fields is original.fields
    assert result.error_code == "0"
    assert result.get_row_data() == ["sh.600000"]
    with pytest.raises(TimeoutError):
        result.next()


def test_refuses_worker_thread_before_calling_source():
    invoked = []
    source = BaostockDeadlineClient(SimpleNamespace(login=lambda: invoked.append(True)))
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(source.login)
        with pytest.raises(RuntimeError, match="main thread"):
            future.result()
    assert not invoked


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="POSIX collector")
def test_does_not_override_an_existing_alarm():
    source = BaostockDeadlineClient(SimpleNamespace(login=lambda: True))
    signal.setitimer(signal.ITIMER_REAL, 10)
    try:
        with pytest.raises(RuntimeError, match="active process alarm"):
            source.login()
        assert signal.getitimer(signal.ITIMER_REAL)[0] > 0
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)


@pytest.mark.parametrize("timeout", [0, -1, 61, float("inf"), float("nan"), True, "12"])
def test_rejects_unbounded_or_ambiguous_timeout(timeout):
    with pytest.raises(ValueError):
        BaostockDeadlineClient(SimpleNamespace(), timeout=timeout)
