"""Native post-close scheduling for a paper-only research command."""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path
import plistlib


def launch_agent_definition(project_root, paper_dir):
    """Return a reviewable macOS LaunchAgent, with three bounded daily slots.

    The command verifies the real exchange calendar, skips completed days
    before network IO, and never connects to a broker. macOS must be awake and
    this login session available. Linux deployments can schedule the same CLI
    with their native service manager; no dependency on the Codex app.
    """
    root, paper = Path(project_root).resolve(), Path(paper_dir).resolve()
    if datetime.now().astimezone().utcoffset() != timedelta(hours=8):
        raise ValueError("The macOS calendar schedule requires the host timezone to be Asia/Shanghai (UTC+8)")
    paper.relative_to(root / "output/research/forward")
    if not (paper / "adapter.json").is_file() or not (root / ".venv/bin/python").is_file():
        raise ValueError("Initialize and verify the local paper period/runtime before scheduling")
    label = "com.stopat30m.forwardpaper." + hashlib.sha256(str(paper).encode()).hexdigest()[:12]
    logs = paper / "runner"
    logs.mkdir(exist_ok=True)
    return {
        "Label": label,
        "ProgramArguments": [str(root / ".venv/bin/python"), str(root / "main.py"),
                             "research", "paper-daily", "--paper-dir", str(paper)],
        "WorkingDirectory": str(root),
        "EnvironmentVariables": {"PYTHONUNBUFFERED": "1", "TZ": "Asia/Shanghai"},
        "RunAtLoad": True,
        "StartCalendarInterval": [{"Weekday": day, "Hour": hour, "Minute": minute}
                                  for day in range(1, 6) for hour, minute in ((16, 15), (16, 45), (17, 15))],
        "StandardOutPath": str(logs / "scheduler.log"),
        "StandardErrorPath": str(logs / "scheduler-error.log"),
        "ProcessType": "Background", "ThrottleInterval": 60,
    }


def write_launch_agent(project_root, paper_dir, output):
    definition = launch_agent_definition(project_root, paper_dir)
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    data = plistlib.dumps(definition, sort_keys=True)
    if target.exists() and target.read_bytes() != data:
        raise ValueError("Existing schedule differs; do not silently replace another job")
    target.write_bytes(data)
    return definition
