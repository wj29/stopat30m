"""Native scheduling stays inside the independent paper research directory."""
from datetime import datetime, timezone, timedelta
import plistlib

import pytest

from stopat30m import research_schedule as schedule


@pytest.fixture
def project(tmp_path, monkeypatch):
    class ShanghaiClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 19, 16, 0, tzinfo=timezone(timedelta(hours=8)))
        def astimezone(self, tz=None):
            return self if tz is None else super().astimezone(tz)
    monkeypatch.setattr(schedule, "datetime", ShanghaiClock)
    root = tmp_path / "project with spaces"
    (root / ".venv/bin").mkdir(parents=True)
    (root / ".venv/bin/python").touch()
    paper = root / "output/research/forward/candidate"
    paper.mkdir(parents=True)
    (paper / "adapter.json").write_text("{}")
    return root, paper


def test_schedule_passes_paths_as_arguments_and_runs_only_paper_research(project, tmp_path):
    root, paper = project
    output = tmp_path / "job.plist"
    job = schedule.write_launch_agent(root, paper, output)
    assert plistlib.loads(output.read_bytes()) == job
    assert job["ProgramArguments"] == [str(root / ".venv/bin/python"), str(root / "main.py"),
                                        "research", "paper-daily", "--paper-dir", str(paper)]
    assert job["RunAtLoad"] is True
    assert all(slot["Weekday"] in range(1, 6) and slot["Hour"] >= 16
               for slot in job["StartCalendarInterval"])
    assert str(paper) in job["StandardErrorPath"]


def test_schedule_cannot_target_account_storage_or_replace_another_job(project, tmp_path):
    root, paper = project
    outside = root / "data/accounts"
    outside.mkdir(parents=True)
    (outside / "adapter.json").touch()
    with pytest.raises(ValueError):
        schedule.launch_agent_definition(root, outside)
    output = tmp_path / "existing.plist"
    output.write_bytes(b"another application job")
    with pytest.raises(ValueError, match="Existing schedule differs"):
        schedule.write_launch_agent(root, paper, output)
    assert output.read_bytes() == b"another application job"


def test_schedule_rejects_wrong_host_timezone(project, monkeypatch):
    class UTCClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 19, 8, 0, tzinfo=timezone.utc)
        def astimezone(self, tz=None):
            return self if tz is None else super().astimezone(tz)
    monkeypatch.setattr(schedule, "datetime", UTCClock)
    with pytest.raises(ValueError, match="host timezone"):
        schedule.launch_agent_definition(*project)
