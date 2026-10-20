"""Lazy membership scope auditing; no model fitting or portfolio returns."""
from pathlib import Path

import click


DEFAULT_DECLARATION = Path(__file__).resolve().parent.parent / "docs/research/ashare_scope.json"


def register_commands(research_group):
    @research_group.command("ashare-scope-status")
    @click.option("--snapshot", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True)
    @click.option("--start-date", required=True)
    @click.option("--end-date", required=True)
    @click.option("--current-dir", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True)
    @click.option("--output-dir", type=click.Path(path_type=Path), required=True)
    @click.option("--scope-declaration", type=click.Path(exists=True, dir_okay=False, path_type=Path),
        default=DEFAULT_DECLARATION, show_default=True)
    @click.option("--historical-dir", type=click.Path(exists=True, file_okay=False, path_type=Path), default=None,
        help="Independent issuer-evidenced membership derivation; original snapshot is preserved.")
    def scope_status(snapshot, start_date, end_date, current_dir, output_dir, scope_declaration, historical_dir):
        """Bind CSI300/CSI1000 membership and expose filtered cache views."""
        try:
            from stopat30m.data.ashare_scope import build_scope_receipt
            receipt = build_scope_receipt(snapshot, start_date, end_date, current_dir, output_dir,
                declaration_path=scope_declaration, historical_dir=historical_dir)
            click.echo(f"Scope receipt: {output_dir.resolve()}")
            click.echo("Allowed indices: csi300, csi1000")
            click.echo(f"Historical scope audit: {receipt['historical_daily_audit']['status']}; "
                f"source union candidates: {receipt['historical_union_security_count']}; "
                f"validated download targets: {len(receipt['historical_download_targets'])}")
            click.echo(f"Current source as-of: {receipt['current']['as_of_date']}; members: {receipt['current']['member_count']}")
            for name, stamp in sorted(receipt["current"]["observed_at"].items()):
                click.echo(f"{name} actually fetched at: {stamp}")
            click.echo(f"Cached historical eligible symbols: {len(receipt['historical_cached_eligible_symbols'])}; "
                f"cached current eligible symbols: {len(receipt['current_cached_eligible_symbols'])}")
            click.echo("Membership views only; broad source snapshot and active CSI300 paper are preserved. No returns generated.")
            click.echo("Existing CSI300 validation covers its subset; CSI1000 requires separate data/model/fee validation.")
        except Exception as exc:
            raise click.ClickException(str(exc)) from exc
