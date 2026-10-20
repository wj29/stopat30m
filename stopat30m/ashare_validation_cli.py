"""Lazy source-audit and immutable historical-validation command registration."""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import click


DEFAULT_SPEC = Path(__file__).resolve().parent.parent / "docs/research/a3_optimization_spec.json"


def register_commands(research_group):
    @research_group.command("ashare-history-audit")
    @click.option("--snapshot", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True)
    @click.option("--execution-collection", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True)
    @click.option("--dividends-collection", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True)
    @click.option("--output-dir", type=click.Path(path_type=Path), required=True)
    @click.option("--action-overrides", type=click.Path(exists=True, dir_okay=False, path_type=Path), default=None)
    @click.option("--rights-events", type=click.Path(exists=True, dir_okay=False, path_type=Path), default=None)
    @click.option("--intraday-observations", type=click.Path(exists=True, dir_okay=False, path_type=Path), default=None)
    @click.option("--capital-reserve-tax-evidence", type=click.Path(exists=True, dir_okay=False, path_type=Path), default=None)
    def history_audit(snapshot, execution_collection, dividends_collection, output_dir, action_overrides, rights_events,
                       intraday_observations, capital_reserve_tax_evidence):
        """Audit original 2023--2024 prices/status/rights, without calculating returns."""
        try:
            from stopat30m.data.ashare_history_audit import build_historical_market_audit
            audit = build_historical_market_audit(snapshot, execution_collection, dividends_collection, output_dir,
                action_overrides_path=action_overrides, rights_events_path=rights_events,
                intraday_observations_path=intraday_observations, capital_reserve_tax_evidence_path=capital_reserve_tax_evidence)
            failed = [name for name, passed in audit["checks"].items() if passed is not True]
            click.echo(f"Historical market audit: {audit['status']}; {output_dir.resolve()}")
            click.echo(f"Raw rows: {audit['raw_rows']}; dividend events: {audit['event_count']}")
            click.echo(f"Blocked checks: {len(failed)}; unexplained references: {audit['unexplained_references']}; "
                f"missing stock-dividend tax evidence: {len(audit['missing_stock_dividend_tax_evidence'])}")
            if failed:
                click.echo("Failed checks: " + ", ".join(sorted(failed)))
            click.echo("Source audit only; no portfolio returns or strategy selection generated.")
        except Exception as exc:
            raise click.ClickException(str(exc)) from exc

    @research_group.command("ashare-validation-declare")
    @click.option("--spec", type=click.Path(exists=True, dir_okay=False, path_type=Path),
        default=DEFAULT_SPEC, show_default=True)
    @click.option("--output", type=click.Path(path_type=Path), required=True)
    def validation_declare(spec, output):
        """Freeze historical validity and selection rules before calculating returns."""
        try:
            from stopat30m.ashare_validation import write_validation_declaration
            result = write_validation_declaration(output, spec_path=spec)
            click.echo(f"Validation declaration: {output.resolve()}")
            click.echo(f"Declared at: {result['declared_at']}; A3 spec SHA-256: {result['a3_spec_sha256']}")
            click.echo("No returns calculated; original A3 specification and active paper period are unchanged.")
        except Exception as exc:
            raise click.ClickException(str(exc)) from exc

    @research_group.command("ashare-validate-history")
    @click.option("--bundle", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
    @click.option("--declaration", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
    @click.option("--output-dir", type=click.Path(path_type=Path), required=True)
    def validate_history(bundle, declaration, output_dir):
        """Run all twelve audited, fee-inclusive 2023/2024 candidate comparisons."""
        try:
            from stopat30m.ashare_validation import run_validation
            report = run_validation(bundle, output_dir, declaration_path=declaration, progress=click.echo)
            click.echo(f"Historical validation: {output_dir.resolve()}")
            for identity, row in sorted(report["cases"].items()):
                metrics = row["metrics"]
                lower = Decimal(metrics["total_return_lower_exact"])
                upper = Decimal(metrics["total_return_upper_exact"])
                dd = Decimal(metrics["max_drawdown_conservative_exact"])
                click.echo(f"{identity}: net return bounds {lower:.2%}..{upper:.2%}; conservative drawdown {dd:.2%}; "
                    f"accounting invalid days {metrics['accounting_invalid_days']}; terminal prices fresh {metrics['terminal_prices_fresh']}")
            click.echo("Selection: " + report["selection_status"])
            click.echo("Recommended candidate: " + (report["selected_candidate"] or "none"))
            click.echo("Retrospective reconstructed history; active paper period is unchanged.")
        except Exception as exc:
            raise click.ClickException(str(exc)) from exc
