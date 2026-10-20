"""Offline LaunchAgent definition checks; never install or start a real job."""
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import plistlib

import pytest

from stopat30m import ashare_low_turnover_forward as forward
from stopat30m import ashare_low_turnover_schedule as schedule


def local_clock(offset_hours):
    class FrozenLocal(datetime):
        @classmethod
        def now(cls):
            return cls(2026, 10, 20, 18, 0, tzinfo=timezone(timedelta(hours=offset_hours)))

        def astimezone(self, tz=None):
            # Model the host's own timezone independently of the test machine.
            return self if tz is None else super().astimezone(tz)

    return FrozenLocal


@pytest.fixture
def job(tmp_path, monkeypatch):
    project = tmp_path / "project"
    paper = project / "output/research/ashare/forward/future_low_turnover"
    paper.mkdir(parents=True)
    marker = paper / "state.json"
    marker.write_bytes(b'{"sentinel":"do not rewrite ledger"}\n')
    report = project / "output/research/ashare/paper_reports/future_low_turnover"
    output = tmp_path / "LaunchAgents/com.stopat30m.lowturnover.plist"
    calls = []

    def status(path):
        calls.append(Path(path))
        return {"path": str(path), "manifest": {"kind": "isolated_ashare_low_turnover_forward"},
            "state": {}, "model_sha256": "a" * 64}

    monkeypatch.setattr(forward, "low_turnover_status", status)
    monkeypatch.setattr(schedule, "datetime", local_clock(8))
    return project, paper, report, output, calls, marker


def test_definition_uses_exact_paper_and_reports_and_thirty_weekday_times(job):
    project, paper, report, _, calls, marker = job
    before = marker.read_bytes()
    value = schedule.launch_agent_definition(project, paper, report)
    assert calls == [paper.resolve()]
    assert value["ProgramArguments"] == [str(project / ".venv/bin/python"), "-m",
        "stopat30m.ashare_low_turnover_forward", "run", "--paper-dir", str(paper),
        "--phase", "auto", "--report-dir", str(report)]
    assert value["WorkingDirectory"] == str(project)
    assert value["EnvironmentVariables"] == {"PYTHONUNBUFFERED": "1", "TZ": "Asia/Shanghai"}
    actual = {(row["Weekday"], row["Hour"], row["Minute"]) for row in value["StartCalendarInterval"]}
    expected = {(day, hour, minute) for day in range(1, 6)
        for hour, minute in ((9, 35), (9, 40), (9, 44), (16, 45), (17, 15), (17, 45))}
    assert len(value["StartCalendarInterval"]) == len(actual) == 30
    assert actual == expected  # No weekend or extra execution window.
    assert value["RunAtLoad"] is True
    assert value["ThrottleInterval"] == 60
    assert value["StandardOutPath"] == str(paper / "runner/scheduler.log")
    assert value["StandardErrorPath"] == str(paper / "runner/scheduler-error.log")
    assert marker.read_bytes() == before and not report.exists()


@pytest.mark.parametrize("fault", ["paper_outside_forward", "paper_symlink_escape", "report_is_paper", "report_inside_paper"])
def test_path_guards_run_before_loading_candidate_or_writing_job(job, tmp_path, fault):
    project, paper, report, output, calls, _ = job
    if fault == "paper_outside_forward":
        paper = project / "output/research/ashare/models/unrelated"
    elif fault == "paper_symlink_escape":
        outside = tmp_path / "outside_project"
        outside.mkdir()
        alias = paper.parent / "escaped_paper"
        alias.symlink_to(outside, target_is_directory=True)
        paper = alias
    elif fault == "report_is_paper":
        report = paper
    else:
        report = paper / "reports/daily"
    with pytest.raises(ValueError):
        schedule.write_launch_agent(project, paper, report, output)
    assert not calls and not output.exists()


def test_report_symlink_back_into_ledger_is_refused(job, tmp_path):
    project, paper, _, output, calls, _ = job
    nested = paper / "reports"
    nested.mkdir()
    alias = tmp_path / "report_alias"
    alias.symlink_to(nested, target_is_directory=True)
    with pytest.raises(ValueError, match="outside"):
        schedule.write_launch_agent(project, paper, alias, output)
    assert not calls and not output.exists()


def test_wrong_host_timezone_refuses_job_before_candidate_load(job, monkeypatch):
    project, paper, report, output, calls, _ = job
    monkeypatch.setattr(schedule, "datetime", local_clock(0))
    with pytest.raises(ValueError, match="UTC\\+8"):
        schedule.write_launch_agent(project, paper, report, output)
    assert not calls and not output.exists()


def test_same_paper_has_one_deterministic_label_and_idempotent_plist(job, monkeypatch):
    project, paper, report, output, _, marker = job
    before = marker.read_bytes()
    first = schedule.write_launch_agent(project, paper, report, output)
    original = output.read_bytes()
    expected_label = "com.stopat30m.lowturnover." + hashlib.sha256(str(paper.resolve()).encode()).hexdigest()[:12]
    assert first["Label"] == expected_label
    monkeypatch.chdir(project)
    second = schedule.write_launch_agent(Path("."), paper.relative_to(project), report.relative_to(project), output)
    assert first == second == plistlib.loads(original)
    assert output.read_bytes() == original
    assert list(output.parent.glob("*.plist")) == [output]
    assert marker.read_bytes() == before


def test_different_existing_job_is_not_overwritten(job):
    project, paper, report, output, _, marker = job
    output.parent.mkdir(parents=True)
    original = plistlib.dumps({"Label": "com.another.existing-job", "RunAtLoad": False}, sort_keys=True)
    output.write_bytes(original)
    before = marker.read_bytes()
    with pytest.raises(ValueError, match="different existing schedule"):
        schedule.write_launch_agent(project, paper, report, output)
    assert output.read_bytes() == original and marker.read_bytes() == before


def test_other_paper_cannot_replace_existing_papers_job(job):
    project, paper, report, output, _, _ = job
    first = schedule.write_launch_agent(project, paper, report, output)
    original = output.read_bytes()
    other_paper = paper.parent / "another_new_paper"
    other_paper.mkdir()
    other = schedule.launch_agent_definition(project, other_paper, report.parent / "another_new_paper")
    assert other["Label"] != first["Label"]
    with pytest.raises(ValueError, match="different existing schedule"):
        schedule.write_launch_agent(project, other_paper, report.parent / "another_new_paper", output)
    assert output.read_bytes() == original
