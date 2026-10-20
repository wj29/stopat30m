"""Small reproducible research entry points; no live-account configuration."""
from __future__ import annotations

import json
from pathlib import Path

import click


DIRECTORY = click.Path(exists=True, file_okay=False, path_type=Path)
FILE = click.Path(exists=True, dir_okay=False, path_type=Path)


def _source_options(function):
    for name, kind in (("--market-audit", FILE), ("--protocol", FILE),
                       ("--scope-dir", DIRECTORY), ("--snapshot", DIRECTORY)):
        function = click.option(name, type=kind, required=True)(function)
    return click.option("--output-dir", type=click.Path(path_type=Path), required=True)(function)


def register_commands(research_group):
    @research_group.command("ashare-union-audit")
    @click.option("--bundle", type=FILE, required=True, help="JSON request with explicit original source directories and evidence paths.")
    @click.option("--output-dir", type=click.Path(path_type=Path), required=True)
    def audit(bundle, output_dir):
        """Verify dated members, raw quotes, ST/halts and corporate actions first."""
        try:
            from stopat30m.data.a5_market_audit import build_a5_market_audit
            value = json.loads(bundle.read_text())
            if value.get("kind") != "a5_union_market_audit_request":
                raise ValueError("An explicit A5 source-audit request is required")
            def path(raw):
                source = Path(raw)
                return source.resolve() if source.is_absolute() else (bundle.parent / source).resolve()
            def paths(name):
                raw = value.get(name, [])
                if not isinstance(raw, list):
                    raise ValueError("Audit source path lists must be explicit JSON arrays")
                return [path(item) for item in raw]
            options = {name: paths(name) for name in ("action_overrides_paths", "rights_events_paths",
                "intraday_observations_paths", "capital_reserve_tax_evidence_paths", "identity_events_paths",
                "dividend_source_correction_receipt_paths")}
            for name in ("adjustment_bridge_spec_path", "partial_session_model_policy_path"):
                if name in value:
                    if not isinstance(value[name], str) or not value[name].strip():
                        raise ValueError("Optional audit policy paths must be explicit nonempty strings")
                    options[name] = path(value[name])
            archive_view = value.get("archive_scope_view", False)
            if type(archive_view) is not bool:
                raise ValueError("archive_scope_view must be an explicit JSON boolean")
            result = build_a5_market_audit(path(value["snapshot"]), path(value["scope_directory"]),
                paths("execution_collections"), paths("dividend_collections"), output_dir,
                start=value["start"], end=value["end"],
                official_membership_evidence_path=path(value["official_membership_evidence"]),
                archive_scope_view=archive_view,
                progress=lambda row: click.echo(json.dumps(row, ensure_ascii=False)), **options)
            click.echo(f"Union source audit: {result['status']}; {output_dir.resolve()}")
            click.echo(f"Original market rows: {result['raw_rows']}; entitlement events: {result['event_count']}; "
                f"source review items: {result['source_review_items']}")
            failed = sorted(name for name, passed in result["checks"].items() if passed is not True)
            if failed:
                raise click.ClickException("Source checks pending: " + ", ".join(failed))
        except (ValueError, OSError, KeyError) as exc:
            raise click.ClickException(str(exc)) from exc

    @research_group.command("ashare-union-freeze")
    @click.option("--snapshot", type=DIRECTORY, required=True)
    @click.option("--scope-dir", type=DIRECTORY, required=True)
    @click.option("--evaluation-end", required=True, help="Actual last exchange session in the declared period.")
    @click.option("--years", type=click.Choice(["2023-2024", "2023-2026"]), default="2023-2024", show_default=True)
    @click.option("--output", type=click.Path(path_type=Path), required=True)
    def freeze(snapshot, scope_dir, evaluation_end, years, output):
        """Freeze the two strategies, costs, source hashes and annual model protocol."""
        try:
            from stopat30m.ashare_union_validation import freeze_union_protocol
            result = freeze_union_protocol(output, snapshot, scope_dir, evaluation_end=evaluation_end,
                execution_years=[2023, 2024] if years == "2023-2024" else [2023, 2024, 2025, 2026])
            click.echo(f"Union declaration: {output.resolve()}; declared at {result['declared_at']}")
        except (ValueError, OSError, KeyError) as exc:
            raise click.ClickException(str(exc)) from exc

    @research_group.command("ashare-union-train")
    @_source_options
    def train(snapshot, scope_dir, protocol, market_audit, output_dir):
        """Fit fixed annual LightGBM models only after the source audit passes."""
        try:
            from stopat30m.ashare_union_research import build_union_predictions
            audit = json.loads(market_audit.read_text())
            result = build_union_predictions(snapshot, scope_dir, audit["stock_basic_path"],
                protocol, market_audit, output_dir,
                progress=lambda row: click.echo(json.dumps(row, ensure_ascii=False)))
            click.echo(f"Union model package: {result['path']}; prediction rows {result['manifest']['prediction_rows']}")
        except (ValueError, OSError, KeyError) as exc:
            raise click.ClickException(str(exc)) from exc

    @research_group.command("ashare-union-validate")
    @_source_options
    @click.option("--predictions-dir", type=DIRECTORY, required=True)
    @click.option("--merger-actions", type=FILE, required=True)
    def validate(snapshot, scope_dir, protocol, market_audit, output_dir, predictions_dir, merger_actions):
        """Compare fixed MOM60/LGB with fees, drawdowns and rolling stability."""
        try:
            from stopat30m.ashare_union_validation import run_union_validation
            result = run_union_validation(snapshot, scope_dir, protocol, market_audit,
                predictions_dir, merger_actions, output_dir,
                progress=lambda row: click.echo(json.dumps(row, ensure_ascii=False)))
            click.echo(f"Union comparison: {output_dir.resolve()}; all paths valid: {result['all_performance_valid']}")
            for row in result["results"]:
                metrics = row["metrics"]
                click.echo(f"{row['period']}/{row['model']}: return {float(metrics['total_return_lower_exact']):.2%}; "
                    f"drawdown {float(metrics['max_drawdown_conservative_exact']):.2%}; valid {row['performance_valid']}")
        except (ValueError, OSError, KeyError) as exc:
            raise click.ClickException(str(exc)) from exc
