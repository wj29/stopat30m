"""Separate fixed2025/2026 research commands; no live-account activation."""
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


def _progress(row):
    click.echo(json.dumps(row, ensure_ascii=False))


def register_commands(research_group):
    @research_group.command("ashare-stability-freeze")
    @click.option("--snapshot", type=DIRECTORY, required=True)
    @click.option("--scope-dir", type=DIRECTORY, required=True)
    @click.option("--evaluation-end", required=True, help="Actual last session in2026; execution years are fixed2025/2026.")
    @click.option("--historical-rules", type=FILE, default=None)
    @click.option("--modern-rules", type=FILE, default=None)
    @click.option("--rule-original-evidence", type=FILE, default=None,
                  help="Optional reviewed original-rule evidence receipt bound into the declaration.")
    @click.option("--output", type=click.Path(path_type=Path), required=True)
    def freeze(snapshot, scope_dir, evaluation_end, historical_rules, modern_rules,
               rule_original_evidence, output):
        """Freeze a separate2025/2026 declaration before fixed fitting and returns."""
        try:
            from stopat30m.ashare_stability_validation import freeze_stability_protocol
            result = freeze_stability_protocol(output, snapshot, scope_dir,
                evaluation_end=evaluation_end, execution_years=(2025, 2026),
                historical_rules_path=historical_rules, modern_rules_path=modern_rules,
                rule_original_evidence_path=rule_original_evidence)
            click.echo(f"Stability declaration: {output.resolve()}; declared at {result['declared_at']}")
        except (ValueError, OSError, KeyError, RuntimeError) as exc:
            raise click.ClickException(str(exc)) from exc

    @research_group.command("ashare-stability-train")
    @_source_options
    def train(snapshot, scope_dir, protocol, market_audit, output_dir):
        """Fit the fixed annual models after the separate complete13-check audit."""
        try:
            from stopat30m.ashare_stability_research import build_union_predictions
            audit = json.loads(market_audit.read_text())
            result = build_union_predictions(snapshot, scope_dir, audit["stock_basic_path"],
                protocol, market_audit, output_dir, progress=_progress)
            click.echo(f"Stability model package: {result['path']}; prediction rows {result['manifest']['prediction_rows']}")
        except (ValueError, OSError, KeyError, RuntimeError) as exc:
            raise click.ClickException(str(exc)) from exc

    @research_group.command("ashare-stability-validate")
    @_source_options
    @click.option("--predictions-dir", type=DIRECTORY, required=True)
    @click.option("--merger-actions", type=FILE, required=True)
    def validate(snapshot, scope_dir, protocol, market_audit, output_dir, predictions_dir, merger_actions):
        """Run the separate fee comparison; invalid paths exit nonzero with reports kept."""
        try:
            from stopat30m.ashare_stability_validation import run_stability_validation
            result = run_stability_validation(snapshot, scope_dir, protocol, market_audit,
                predictions_dir, merger_actions, output_dir, progress=_progress)
            click.echo(f"Stability comparison: {output_dir.resolve()}; all paths valid: {result['all_performance_valid']}")
            if (result["all_performance_valid"] is not True or not result["results"]
                    or any(row.get("performance_valid") is not True for row in result["results"])):
                raise click.ClickException("Invalid stability paths; reports preserved for diagnosis, returns cannot be used for selection.")
            for row in result["results"]:
                metrics = row["metrics"]
                click.echo(f"{row['period']}/{row['model']}: return {float(metrics['total_return_lower_exact']):.2%}; "
                    f"drawdown {float(metrics['max_drawdown_conservative_exact']):.2%}; valid True")
        except (ValueError, OSError, KeyError, RuntimeError) as exc:
            raise click.ClickException(str(exc)) from exc
