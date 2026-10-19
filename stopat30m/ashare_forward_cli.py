"""Lazy CLI registration for isolated A-share forward research only."""
from __future__ import annotations

from datetime import datetime, time
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import click


def _echo(value):
    click.echo(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False))


def register_commands(research_group):
    @research_group.command("ashare-forward-fit")
    @click.option("--snapshot", type=click.Path(exists=True, path_type=Path), required=True)
    @click.option("--cutoff", required=True, help="Last completed, verified source session (YYYY-MM-DD).")
    @click.option("--output-dir", type=click.Path(path_type=Path), required=True)
    def forward_fit(snapshot, cutoff, output_dir):
        """Freeze a new mature-label Alpha158 model for forward use, without OOS claims."""
        try:
            from stopat30m.data.qlib_snapshot import guard_qlib_output_path
            from stopat30m.ashare_forward import fit_ashare_forward_candidate
            output_dir = guard_qlib_output_path(output_dir)
            click.echo("Fitting an independent forward-only candidate; frozen historical results are not updated.")
            candidate = fit_ashare_forward_candidate(snapshot, output_dir, cutoff=cutoff)
            _echo({"path": str(candidate.path), "manifest": candidate.manifest,
                   "historical_oos_claim": False, "paper_started": False})
        except (ValueError, OSError, RuntimeError, ImportError, KeyError) as exc:
            raise click.ClickException(str(exc)) from exc

    @research_group.command("ashare-forward-status")
    @click.option("--paper-dir", type=click.Path(exists=True, path_type=Path), required=True)
    @click.option("--raw", is_flag=True, help="Show full verified manifest/state instead of the concise after-cost report.")
    def forward_status(paper_dir, raw):
        """Verify the candidate and persisted isolated cash/share paper ledgers, offline."""
        try:
            if raw:
                from stopat30m.ashare_forward import ashare_forward_status
                _echo(ashare_forward_status(paper_dir))
            else:
                from stopat30m.ashare_paper_report import ashare_paper_summary
                _echo(ashare_paper_summary(paper_dir))
        except (ValueError, OSError, RuntimeError, ImportError, KeyError) as exc:
            raise click.ClickException(str(exc)) from exc

    @research_group.command("ashare-paper-run")
    @click.option("--paper-dir", type=click.Path(exists=True, path_type=Path), required=True)
    @click.option("--phase", type=click.Choice(["open", "close", "auto"]), default="auto", show_default=True)
    def paper_run(paper_dir, phase):
        """Run one current-clock paper phase; never fill a past open or place broker orders."""
        try:
            from stopat30m.ashare_forward import run_ashare_forward
            clock = datetime.now(ZoneInfo("Asia/Shanghai"))
            if phase == "auto":
                if time(9, 30) <= clock.time() <= time(9, 45):
                    phase = "open"
                elif clock.time() >= time(15, 5):
                    phase = "close"
                else:
                    _echo({"status": "skipped_outside_window", "observed_at": clock.isoformat(),
                        "network_called": False, "live_order": False})
                    return

            class LazyProvider:
                # The runner first checks the real calendar and phase/replay
                # offline. A holiday or replay must not even construct a source.
                def _fetch(self, method, *args):
                    from stopat30m.ashare_paper_provider import AsharePaperProvider
                    return getattr(AsharePaperProvider.from_paper_dir(paper_dir), method)(*args)

                def fetch_open(self, *args):
                    return self._fetch("fetch_open", *args)

                def fetch_close(self, *args):
                    return self._fetch("fetch_close", *args)

            click.echo(f"A-share isolated paper phase: {phase}; current-clock source observations only.")
            _echo(run_ashare_forward(paper_dir, phase=phase, provider=LazyProvider()))
        except (ValueError, OSError, RuntimeError, ImportError, KeyError) as exc:
            raise click.ClickException(str(exc)) from exc
