"""CLI for immutable research data and locked retrospective OOS experiments."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import click

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SPEC = PROJECT_ROOT / "docs/research/m2_etf_eastmoney_spec.json"
DEFAULT_PROTOCOL = PROJECT_ROOT / "docs/research/m2_etf_eastmoney_protocol.json"


def _read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("Experiment specification must be a JSON object.")
    return value


def _universe_path(protocol: Path) -> Path:
    return protocol.with_name(f"{protocol.stem}.universe.json")


def _lock_spec(spec_path: Path, protocol_path: Path):
    from stopat30m.backtest.walk_forward import ResearchConfig, lock_protocol
    from stopat30m.data.historical_universe import HistoricalUniverse

    spec = _read_json(spec_path)
    config = ResearchConfig(**spec["config"])
    pool, request = spec["universe"], spec["data_request"]
    if pool.get("kind") != "fixed":
        raise ValueError("The convenience prepare command requires an explicit fixed pool. Import audited PIT intervals separately.")
    if not {config.benchmark, *pool["instruments"]}.issubset(request["instruments"]):
        raise ValueError("The data request must include the locked pool and benchmark.")
    declared = HistoricalUniverse.fixed_pool(
        spec["name"], pool["instruments"], request["start_date"], request["end_date"],
        selection_date=pool["selection_date"], source="repository_experiment_spec",
        source_url=f"repository:docs/research/{spec_path.name}",
    )
    declaration = declared.as_dict()
    declaration["metadata"]["research_data_request"] = request
    declared = HistoricalUniverse.from_dict(declaration)
    companion = _universe_path(protocol_path)
    if companion.exists():
        universe = HistoricalUniverse.load(companion)
        if universe.content_hash != declared.content_hash:
            raise ValueError("The specification differs from the saved universe/data declaration; use a new protocol path.")
    else:
        if protocol_path.exists():
            raise ValueError("The protocol's bound universe file is missing; restore it rather than recreating its provenance.")
        universe = declared
        companion.parent.mkdir(parents=True, exist_ok=True)
        universe.save(companion)
    protocol = lock_protocol(config, protocol_path, universe=universe)
    return spec, protocol, universe, universe.as_dict()


@click.group("research")
def research_group() -> None:
    """Download audited dual-price inputs and run locked out-of-sample research."""


@research_group.command("lock")
@click.option("--spec", "spec_path", type=click.Path(exists=True, path_type=Path), default=DEFAULT_SPEC, show_default=True)
@click.option("--protocol", "protocol_path", type=click.Path(path_type=Path), default=DEFAULT_PROTOCOL, show_default=True)
def lock_command(spec_path: Path, protocol_path: Path) -> None:
    """Lock rules and a declared pool before downloading/evaluating final OOS."""
    try:
        _lock_spec(spec_path, protocol_path)
    except (ValueError, KeyError, OSError, ImportError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Protocol: {protocol_path.resolve()}")
    click.echo(f"Universe: {_universe_path(protocol_path).resolve()}")


@research_group.command("prepare")
@click.option("--spec", "spec_path", type=click.Path(exists=True, path_type=Path), default=DEFAULT_SPEC, show_default=True)
@click.option("--protocol", "protocol_path", type=click.Path(path_type=Path), default=DEFAULT_PROTOCOL, show_default=True)
@click.option("--cache-dir", type=click.Path(path_type=Path), default=PROJECT_ROOT / "output/research/data", show_default=True)
@click.option("--refresh", is_flag=True, help="Fetch a new immutable snapshot instead of using the validated cache.")
@click.option("--min-interval", type=click.FloatRange(2, 60), default=2.0, show_default=True, help="Minimum seconds between Eastmoney requests.")
@click.option("--max-attempts", type=click.IntRange(1, 6), default=4, show_default=True, help="Bounded attempts for transient Eastmoney failures.")
@click.option("--request-timeout", type=click.FloatRange(1, 60), default=20.0, show_default=True)
def prepare_command(spec_path: Path, protocol_path: Path, cache_dir: Path, refresh: bool,
                    min_interval: float, max_attempts: int, request_timeout: float) -> None:
    """Lock first, then explicitly download actual raw/adjusted daily data."""
    from stopat30m.data.research_prices import EastmoneyResearchProvider, download_research_prices

    download_started = False
    try:
        spec, _, _, _ = _lock_spec(spec_path, protocol_path)
        request = spec["data_request"]
        if request.get("source") not in {"baostock", "eastmoney"}:
            raise ValueError("Explicit source must be baostock or eastmoney; no automatic fallback is used.")
        click.echo(f"Locked protocol: {protocol_path.resolve()}")
        download_started = True
        provider = EastmoneyResearchProvider(
            cache_dir=cache_dir / "download-cache", refresh=refresh, min_interval=min_interval,
            max_attempts=max_attempts, timeout=request_timeout,
        ) if request["source"] == "eastmoney" else None
        try:
            artifact = download_research_prices(
                request["instruments"], request["start_date"], request["end_date"], cache_dir,
                adjustment=request.get("adjustment", "hfq"), refresh=refresh,
                progress=click.echo, provider=provider,
            )
        finally:
            if provider is not None:
                provider.close()
    except (ValueError, KeyError, OSError, ImportError, RuntimeError) as exc:
        if download_started:
            failures = cache_dir / "failures"
            failures.mkdir(parents=True, exist_ok=True)
            failure = {
                "failed_at": datetime.now(timezone.utc).isoformat(), "stage": "research_data_download",
                "protocol": str(protocol_path.resolve()), "request": request,
                "error_type": type(exc).__name__, "error": str(exc)[:2000],
                "usable_snapshot_created": False,
            }
            (failures / f"{uuid4().hex}.json").write_text(json.dumps(failure, ensure_ascii=False, indent=2), encoding="utf-8")
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Snapshot: {artifact.path}")
    click.echo("Adjusted data supports research returns; it does not authorize integer-share cash-account simulation.")


@research_group.command("inspect")
@click.option("--snapshot", type=click.Path(exists=True, path_type=Path), required=True)
def inspect_command(snapshot: Path) -> None:
    """Verify saved checksums and display the research data manifest."""
    from stopat30m.data.research_prices import load_research_prices

    try:
        artifact = load_research_prices(snapshot)
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(artifact.manifest, ensure_ascii=False, indent=2, allow_nan=False))


@research_group.command("run")
@click.option("--snapshot", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--protocol", "protocol_path", type=click.Path(exists=True, path_type=Path), default=DEFAULT_PROTOCOL, show_default=True)
@click.option("--universe", "universe_path", type=click.Path(exists=True, path_type=Path), default=None)
@click.option("--output-dir", type=click.Path(path_type=Path), default=PROJECT_ROOT / "output/research/runs", show_default=True, help="Parent directory for a new uniquely named run.")
def run_command(snapshot: Path, protocol_path: Path, universe_path: Path | None, output_dir: Path) -> None:
    """Run fixed-rule OOS with the parameters and pool bound by the protocol."""
    from stopat30m.backtest.walk_forward import run_walk_forward
    from stopat30m.data.historical_universe import HistoricalUniverse
    from stopat30m.data.research_prices import load_research_prices

    try:
        artifact = load_research_prices(snapshot)
        universe = HistoricalUniverse.load(universe_path or _universe_path(protocol_path))
        tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        run_dir = output_dir / f"oos_{tag}_{uuid4().hex[:8]}"
        result = run_walk_forward(artifact, universe, protocol_path, run_dir)
    except (ValueError, OSError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Run: {result.path}")
    click.echo(json.dumps(result.report, ensure_ascii=False, indent=2, allow_nan=False))


@research_group.command("plot")
@click.option("--run-dir", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--output", type=click.Path(path_type=Path), required=True)
def plot_command(run_dir: Path, output: Path) -> None:
    """Verify a run and export its final OOS NAV/drawdown chart."""
    from stopat30m.backtest.research_report import render_research_curve
    try:
        result = render_research_curve(run_dir, output)
    except (ValueError, OSError, ImportError, KeyError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(str(result.resolve()))


@research_group.command("universe-import")
@click.option("--input", "input_path", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--output", type=click.Path(path_type=Path), required=True)
def universe_import_command(input_path: Path, output: Path) -> None:
    """Validate source-declared membership intervals and save a checksummed file."""
    from stopat30m.data.historical_universe import HistoricalUniverse
    try:
        if output.exists():
            raise ValueError("Universe output already exists; use a new versioned filename.")
        payload = _read_json(input_path)
        universe = HistoricalUniverse.load(input_path) if "sha256" in payload else HistoricalUniverse.from_dict(payload)
        audit = universe.audit(universe.coverage_start, universe.coverage_end, require_pit=universe.kind == "index_pit")
        universe.save(output)
    except (ValueError, KeyError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(audit, ensure_ascii=False, indent=2))
    click.echo(str(output.resolve()))


@research_group.command("universe-audit")
@click.option("--universe", "universe_path", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--start", required=True)
@click.option("--end", required=True)
@click.option("--require-pit", is_flag=True)
def universe_audit_command(universe_path: Path, start: str, end: str, require_pit: bool) -> None:
    """Check coverage, effective intervals and announcement availability."""
    from stopat30m.data.historical_universe import HistoricalUniverse
    try:
        universe = HistoricalUniverse.load(universe_path)
        audit = universe.audit(start, end, require_pit=require_pit)
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(audit, ensure_ascii=False, indent=2))
