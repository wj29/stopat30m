import json
import threading
import time

import pytest

from stopat30m.data.maintenance import UpdateBusyError, start_update, update_status


def wait_for_completion(path):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        status = update_status(path)
        if status and status["status"] != "running":
            return status
        time.sleep(0.01)
    raise AssertionError("background update did not terminate")


def test_duplicate_update_rejected_and_failure_recorded(tmp_path):
    entered, release = threading.Event(), threading.Event()

    def fail(**kwargs):
        entered.set()
        assert release.wait(2)
        raise RuntimeError("synthetic upstream failure")

    job = start_update(tmp_path, source="baostock", runner=fail)
    try:
        assert entered.wait(2)
        assert update_status(tmp_path)["id"] == job["id"]
        with pytest.raises(UpdateBusyError):
            start_update(tmp_path, source="baostock", runner=fail)
    finally:
        release.set()
    result = wait_for_completion(tmp_path)
    assert result["status"] == "failed"
    assert result["error"] == "synthetic upstream failure"


def test_returning_without_data_is_not_success(tmp_path):
    start_update(tmp_path, source="baostock", runner=lambda **kwargs: None)
    assert wait_for_completion(tmp_path)["status"] == "failed"


def test_abandoned_job_marked_interrupted(tmp_path):
    (tmp_path / ".update-status.json").write_text(json.dumps({"id": "lost", "status": "running"}))
    result = update_status(tmp_path)
    assert result["status"] == "interrupted"
    assert result["completed_at"]
