"""CLI for immutable research data and locked retrospective OOS experiments."""

from __future__ import annotations

import json
from datetime import date
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


def _artifact_output(path: Path) -> Path:
    from stopat30m.data.qlib_snapshot import guard_qlib_output_path
    return guard_qlib_output_path(path)


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


@research_group.command("ashare-base")
@click.option("--source-spec", type=click.Path(exists=True, path_type=Path),
              default=PROJECT_ROOT / "docs/research/a1_qlib_source.json", show_default=True)
@click.option("--output-dir", type=click.Path(path_type=Path),
              default=PROJECT_ROOT / "output/research/ashare/base", show_default=True)
def ashare_base_command(source_spec: Path, output_dir: Path) -> None:
    """Download one pinned Qlib-format A-share archive, verify and audit it."""
    from stopat30m.data.qlib_snapshot import download_qlib_archive, prepare_qlib_snapshot
    try:
        output_dir = _artifact_output(output_dir)
        source = _read_json(source_spec)
        declared = source["release_manifest"]
        release_tag = source["release_tag"]
        if date.fromisoformat(release_tag).isoformat() != release_tag:
            raise ValueError("The pinned release tag must be an ISO date")
        archive = output_dir / "archives" / f"qlib_bin_{release_tag}.tar.gz"
        download_qlib_archive(source["url"], archive, expected_sha256=declared["archive_sha256"],
                              expected_size_bytes=declared["archive_size_bytes"])
        snapshot = prepare_qlib_snapshot(archive, output_dir, source=source,
            expected_sha256=declared["archive_sha256"], expected_size_bytes=declared["archive_size_bytes"])
    except (ValueError, KeyError, OSError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Qlib dataset: {snapshot.dataset_dir}")
    click.echo(f"Snapshot manifest: {snapshot.path / 'manifest.json'}")
    click.echo(f"Actual security count: {snapshot.manifest['audit']['instrument_count']}")
    click.echo("Coverage is per security; the package calendar tail is not a universal watermark.")


@research_group.command("ashare-collect")
@click.option("--start-date", required=True)
@click.option("--end-date", required=True)
@click.option("--index-date", default=None, help="Current index snapshot used only to choose download targets.")
@click.option("--instruments", default=None, help="Optional comma-separated canonical stock download targets.")
@click.option("--historical-scope-dir", type=click.Path(exists=True, file_okay=False, path_type=Path),
              default=None, help="Verified derived CSI300/CSI1000 scope; required with explicit --instruments.")
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
def ashare_collect_command(start_date: str, end_date: str, index_date: str | None,
                           instruments: str | None, historical_scope_dir: Path | None, output_dir: Path) -> None:
    """Collect original BaoStock stock raw/HFQ responses; no in-place writes."""
    from stopat30m.data.ashare_daily import load_collection_frames
    from stopat30m.data.ashare_collection_scope import collect_scope_checked_increment
    try:
        path = collect_scope_checked_increment(output_dir, start_date, end_date, index_date=index_date,
            instruments=[part.strip().upper() for part in instruments.split(",")] if instruments is not None else None,
            historical_scope_dir=historical_scope_dir, progress=click.echo)
        raw, _, manifest = load_collection_frames(path)
    except (ValueError, KeyError, OSError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Original collection: {path}")
    click.echo(f"Stocks: {len(manifest['instruments'])}; raw rows: {len(raw)}")
    click.echo("This index snapshot does not establish historical membership. Bridge validation is still required.")


@research_group.command("ashare-update")
@click.option("--base-snapshot", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--collection", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--calendar", "calendar_path", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
@click.option("--base-volume-multiplier", type=click.Choice(["1", "100"]), required=True,
              help="Explicit source-verified raw volume basis: shares=1, 100-share lots=100. Never inferred.")
@click.option("--allow-partial", is_flag=True,
              help="Keep failed securities unchanged, publish only strict passes, and save every rejection.")
def ashare_update_command(base_snapshot: Path, collection: Path, calendar_path: Path,
                          output_dir: Path, base_volume_multiplier: str, allow_partial: bool) -> None:
    """Verify all stock seams, clone the base, and publish a new data version."""
    import hashlib
    from dataclasses import replace
    import pandas as pd
    from stopat30m.data.ashare_daily import load_collection_frames
    from stopat30m.data.qlib_incremental import BridgePolicy, CanonicalIncrement, audit_bridge, publish_increment
    from stopat30m.data.qlib_snapshot import load_qlib_snapshot, prepare_qlib_directory
    from stopat30m.data.research_calendar import Calendar
    try:
        base = load_qlib_snapshot(base_snapshot)
        output_dir = _artifact_output(output_dir)
        if output_dir == base.path or base.path in output_dir.parents:
            raise ValueError("Update output must be outside its immutable parent snapshot")
        raw, adjusted, original = load_collection_frames(collection)
        calendar = Calendar.load(calendar_path)
        calendar._require_coverage(original["request_start"], original["request_end"])
        rows = pd.DataFrame([{"date": day, "is_trading_day": int(flag)} for day, flag in calendar.days
                             if original["request_start"] <= day <= original["request_end"]])
        batch = CanonicalIncrement.from_frames(raw, adjusted, source="baostock", generation=original["generation"],
            calendar_rows=rows, calendar_source=calendar.source, calendar_source_url=calendar.source_url,
            requested_start=original["request_start"], requested_end=original["request_end"],
            evidence={"original_collection": str(collection.resolve()),
                      "collection_manifest_sha256": hashlib.sha256((collection / "collection.json").read_bytes()).hexdigest(),
                      "responses": original["responses"], "download_target_selection": original["target_selection"]})
        policy = BridgePolicy(base_raw_volume_multiplier=float(base_volume_multiplier))
        if allow_partial:
            accepted, rejected = [], {}
            for instrument in sorted(batch.bars.instrument.unique()):
                try:
                    audit_bridge(base.dataset_dir, replace(batch, bars=batch.bars.loc[batch.bars.instrument.eq(instrument)].copy()), policy)
                except ValueError as exc:
                    rejected[instrument] = str(exc)
                else:
                    accepted.append(instrument)
            output_dir.mkdir(parents=True, exist_ok=True)
            rejection = {
                "generation": batch.generation, "accepted": accepted, "rejected": rejected,
                "failed_securities_are_unchanged": True,
                "acceptance_does_not_prove_latest_finite_bar": True,
            }
            rejection_path = output_dir / "collection_bridge_rejections.json"
            if rejection_path.exists() and _read_json(rejection_path) != rejection:
                raise ValueError("Existing rejection audit belongs to different inputs; use a new output directory")
            if not rejection_path.exists():
                rejection_path.write_text(json.dumps(rejection, ensure_ascii=False, indent=2) + "\n")
            if not accepted:
                raise ValueError("No stock passed the unchanged strict seam policy")
            batch = replace(batch, bars=batch.bars.loc[batch.bars.instrument.isin(accepted)].copy(),
                            evidence={**batch.evidence, "excluded_securities": rejected,
                                      "source_target_count": len(original["instruments"]),
                                      "published_security_count": len(accepted)})
        staging_version = output_dir / "bridge_dataset"
        if staging_version.exists():
            saved = _read_json(staging_version / "incremental_manifest.json")
            audit = audit_bridge(base.dataset_dir, batch, policy)
            if any(saved.get(key) != value for key, value in audit.items()):
                raise ValueError("Existing bridge belongs to different source inputs/rules; use a new output directory")
        else:
            publish_increment(base.dataset_dir, batch, staging_version, policy, pilot_only=False)
        version = prepare_qlib_directory(staging_version, output_dir / "versions", source=base.manifest["source"],
                                         parent_snapshot=base.path,
                                         increment_manifest_path=staging_version / "incremental_manifest.json")
    except (ValueError, KeyError, OSError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Updated dataset: {version.dataset_dir}")
    click.echo(f"Version manifest: {version.path / 'manifest.json'}")
    click.echo("Only audited stock fields advance. Historical index intervals and other stocks retain their original coverage.")


@research_group.command("ashare-rebuild")
@click.option("--base-snapshot", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--collection", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--calendar", "calendar_path", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--failure-audit", type=click.Path(exists=True, path_type=Path), required=True,
              help="Strict bridge audit selecting exactly the failed securities; no performance-based selection.")
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
def ashare_rebuild_command(base_snapshot: Path, collection: Path, calendar_path: Path,
                           failure_audit: Path, output_dir: Path) -> None:
    """Replace failed seams with whole same-source histories in a new version."""
    from stopat30m.data.qlib_rebuild import rebuild_batch_from_collection, publish_whole_security_rebuild
    from stopat30m.data.qlib_snapshot import load_qlib_snapshot, prepare_qlib_directory
    try:
        output_dir = _artifact_output(output_dir)
        if output_dir.exists():
            raise ValueError("Rebuild output already exists; use a new directory and preserve the original version")
        base = load_qlib_snapshot(base_snapshot)
        batch = rebuild_batch_from_collection(collection, base.dataset_dir, calendar_path)
        dataset = output_dir / "rebuilt_dataset"
        publish_whole_security_rebuild(base.dataset_dir, batch, dataset, failure_audit_path=failure_audit)
        version = prepare_qlib_directory(dataset, output_dir / "versions", source=base.manifest["source"],
            parent_snapshot=base.path, increment_manifest_path=dataset / "incremental_manifest.json")
    except (ValueError, KeyError, OSError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Rebuilt dataset: {version.dataset_dir}")
    click.echo(f"Version manifest: {version.path / 'manifest.json'}")
    click.echo("Whole histories replaced after strict data rejection. This is a new data vintage, not a verified cross-source seam.")


@research_group.command("ashare-run")
@click.option("--snapshot", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--spec", type=click.Path(exists=True, path_type=Path),
              default=PROJECT_ROOT / "docs/research/a1_ashare_experiment.json", show_default=True)
@click.option("--corporate-actions", type=click.Path(exists=True, path_type=Path), default=None,
              help="Evidence-bound share exchanges with announcement knowledge times.")
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
def ashare_run_command(snapshot: Path, spec: Path, corporate_actions: Path | None, output_dir: Path) -> None:
    """Train official Qlib Alpha158/LightGBM and run cost-matched stock OOS."""
    from stopat30m.ashare_experiment import AshareExperimentConfig, run_ashare_experiment
    from stopat30m.data.qlib_snapshot import load_qlib_snapshot
    try:
        data = load_qlib_snapshot(snapshot)
        declaration = _read_json(spec)
        result = run_ashare_experiment(data.dataset_dir, data.path / "manifest.json", output_dir,
            config=AshareExperimentConfig(**declaration["config"]), universe_name=declaration["universe"],
            declaration=declaration, corporate_actions_path=corporate_actions)
    except (ValueError, KeyError, OSError, RuntimeError, ImportError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"A-share experiment: {result['output_dir']}")
    click.echo(f"Historical stock members: {result['report']['historical_member_instruments']}")
    click.echo(f"Backtest: {result['report']['backtest_status']}")
    for strategy, metrics in result["report"]["metrics"].items():
        click.echo(f"{strategy}: net annual return {metrics['annual_return']:.2%}; max drawdown {metrics['max_drawdown']:.2%}")


@research_group.command("ashare-validate")
@click.option("--snapshot", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--execution-collection", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--dividends-collection", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--training-origin", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--spec", type=click.Path(exists=True, path_type=Path), default=PROJECT_ROOT/"docs/research/a2_ashare_execution_spec.json")
@click.option("--corporate-actions", type=click.Path(exists=True, path_type=Path), default=PROJECT_ROOT/"docs/research/a1_ashare_corporate_actions_v2.json")
@click.option("--action-overrides", type=click.Path(exists=True, path_type=Path), default=PROJECT_ROOT/"docs/research/a2_ashare_action_overrides.json")
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
def ashare_validate_command(snapshot, execution_collection, dividends_collection, training_origin, spec,
                            corporate_actions, action_overrides, output_dir):
    """Validate raw cash/integer stock OOS with board rules and actual rights."""
    from stopat30m.ashare_execution_experiment import run_integer_comparison
    try:
        result = run_integer_comparison(snapshot, execution_collection, dividends_collection, output_dir,
            spec_path=spec, corporate_actions_path=corporate_actions, training_origin=training_origin,
            action_overrides_path=action_overrides)
    except (ValueError, KeyError, OSError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    report = result["report"]
    click.echo(f"Integer comparison: {result['output_dir']}; {report['status']}")
    click.echo(f"Forward activation gate: {report['activation_passed']}")
    for strategy, metrics in report["metrics"].items():
        click.echo(f"{strategy}: net annual return {metrics['annual_return']:.2%}; max drawdown {metrics['max_drawdown']:.2%}")
    if report.get("error"):
        click.echo(report["error"])


@research_group.command("ashare-dividends")
@click.option("--execution-collection", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--years", required=True, help="Comma-separated actual ex-date years, never report years.")
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
def ashare_dividends_command(execution_collection: Path, years: str, output_dir: Path) -> None:
    """Collect original cash-dividend/bonus evidence for the historical stock pool."""
    from stopat30m.data.ashare_actions import collect_dividend_evidence, load_dividend_evidence
    from stopat30m.data.ashare_execution_data import load_execution_collection
    try:
        output_dir = _artifact_output(output_dir)
        _, _, receipt = load_execution_collection(execution_collection)
        periods = [int(value.strip()) for value in years.split(",")]
        root = collect_dividend_evidence(output_dir, receipt["request"]["instruments"], periods, progress=click.echo)
        frame, manifest = load_dividend_evidence(root)
        selection = {"execution_collection": str(execution_collection.resolve()),
                     "execution_manifest_sha256": receipt["manifest_sha256"],
                     "instruments": manifest["instruments"], "years": manifest["years"],
                     "selection_basis": "historical_OOS_execution_targets_not_today_backfilled"}
        path = root / "selection_receipt.json"
        if path.exists() and _read_json(path) != selection:
            raise ValueError("Original dividend target receipt changed")
        if not path.exists():
            path.write_text(json.dumps(selection, ensure_ascii=False, indent=2) + "\n")
    except (ValueError, KeyError, OSError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Original dividend collection: {root}")
    click.echo(f"Source rows: {len(frame)}; ex-date years: {manifest['years']}")


@research_group.command("ashare-resume")
@click.option("--experiment-dir", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--corporate-actions", type=click.Path(exists=True, path_type=Path), default=None)
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
def ashare_resume_command(experiment_dir: Path, corporate_actions: Path | None, output_dir: Path) -> None:
    """Reuse frozen model/scores after an evidence-backed accounting correction."""
    from stopat30m.ashare_experiment import resume_ashare_comparison
    try:
        result = resume_ashare_comparison(experiment_dir, output_dir, corporate_actions_path=corporate_actions)
    except (ValueError, KeyError, OSError, RuntimeError, ImportError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Comparison: {result['output_dir']}")
    click.echo(f"Backtest: {result['report']['backtest_status']}")
    for strategy, metrics in result["report"]["metrics"].items():
        click.echo(f"{strategy}: net annual return {metrics['annual_return']:.2%}; max drawdown {metrics['max_drawdown']:.2%}")


def _ashare_research_progress(value) -> None:
    click.echo(json.dumps(value, ensure_ascii=False) if isinstance(value, dict) else str(value))


@research_group.command("ashare-diagnose")
@click.option("--experiment-dir", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True)
@click.option("--training-origin", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True)
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
def ashare_diagnose_command(experiment_dir: Path, training_origin: Path, output_dir: Path) -> None:
    """Attribute existing fees, ticks, turnover and frozen prediction quality."""
    try:
        from stopat30m.ashare_diagnostics import run_diagnostics
        result = run_diagnostics(experiment_dir, training_origin, output_dir)
    except (ValueError, KeyError, OSError, RuntimeError, ImportError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"A-share diagnostics: {output_dir}")
    for strategy, values in result["strategy_cost_attribution"].items():
        click.echo(f"{strategy}: net PnL {values['net_profit_cny']} CNY; "
                   f"recorded execution friction {values['costs']['total_friction_cny']} CNY")
    click.echo("Fixed-ledger cost add-backs are accounting attribution, not zero-cost portfolio reruns.")


@research_group.command("ashare-optimize-review")
@click.option("--baseline-dir", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True)
@click.option("--spec", type=click.Path(exists=True, dir_okay=False, path_type=Path),
              default=PROJECT_ROOT / "docs/research/a3_optimization_spec.json", show_default=True)
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
@click.option("--cost-ablation", is_flag=True, help="Run baseline cost sensitivity; never a deployable optimization.")
@click.option("--rolling-predictions-dir", type=click.Path(exists=True, file_okay=False, path_type=Path), default=None,
              help="Review the frozen rolling M1 predictions with the same execution candidates.")
def ashare_optimize_review_command(baseline_dir: Path, spec: Path, output_dir: Path,
                                  cost_ablation: bool, rolling_predictions_dir: Path | None) -> None:
    """Review declared execution/model candidates on previously observed history."""
    if cost_ablation and rolling_predictions_dir is not None:
        raise click.UsageError("--cost-ablation and --rolling-predictions-dir are mutually exclusive.")
    try:
        from stopat30m.ashare_optimization import run_existing_model_review, run_rolling_model_review
        if rolling_predictions_dir is not None:
            result = run_rolling_model_review(baseline_dir, rolling_predictions_dir, output_dir,
                spec_path=spec, progress=_ashare_research_progress)
        else:
            result = run_existing_model_review(baseline_dir, output_dir, spec_path=spec,
                cost_ablation=cost_ablation, progress=_ashare_research_progress)
    except (ValueError, KeyError, OSError, RuntimeError, ImportError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Observed-history review: {output_dir}")
    for candidate, values in result["cases"].items():
        metrics = values["metrics"]
        incomplete = metrics.get("incomplete_valuation_days")
        click.echo(f"{candidate}: net annual return {metrics['annual_return']:.2%}; "
                   f"max drawdown {metrics['max_drawdown']:.2%}; "
                   f"incomplete valuation days {incomplete if incomplete is not None else 'unknown'}")
        if incomplete is None or incomplete > 0:
            click.echo(f"{candidate}: provisional; incomplete valuation is not a verified performance comparison.")
    click.echo(f"Selection: {result['selection_status']}")
    click.echo("Previously observed history is not fresh OOS; this command does not switch the active paper model.")


@research_group.command("ashare-rolling-fit")
@click.option("--snapshot", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True)
@click.option("--baseline-origin", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True)
@click.option("--spec", type=click.Path(exists=True, dir_okay=False, path_type=Path),
              default=PROJECT_ROOT / "docs/research/a3_optimization_spec.json", show_default=True)
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
def ashare_rolling_fit_command(snapshot: Path, baseline_origin: Path, spec: Path, output_dir: Path) -> None:
    """Fit the four declared annual research folds; preserve baseline and paper."""
    try:
        from stopat30m.ashare_rolling_research import build_research_predictions
        result = build_research_predictions(snapshot, baseline_origin, output_dir, spec,
            progress=_ashare_research_progress)
    except (ValueError, KeyError, OSError, RuntimeError, ImportError) as exc:
        raise click.ClickException(str(exc)) from exc
    manifest = result["manifest"]
    click.echo(f"Rolling research predictions: {output_dir}")
    click.echo(f"Annual folds: {len(manifest['folds'])}; prediction rows: {manifest['prediction_rows']}")
    click.echo("Fits are retrospective research performed now; no historical model-freeze time or paper switch is claimed.")


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
    from stopat30m.data.research_prices import EastmoneyResearchProvider, TencentResearchProvider, download_research_prices

    download_started = False
    try:
        spec, _, _, _ = _lock_spec(spec_path, protocol_path)
        request = spec["data_request"]
        if request.get("source") not in {"baostock", "eastmoney", "tencent"}:
            raise ValueError("Explicit source must be baostock, eastmoney or tencent; no automatic fallback is used.")
        click.echo(f"Locked protocol: {protocol_path.resolve()}")
        download_started = True
        provider_class = {"eastmoney": EastmoneyResearchProvider, "tencent": TencentResearchProvider}.get(request["source"])
        provider = provider_class(
            cache_dir=cache_dir / "http" / request["source"], refresh=refresh, min_interval=min_interval,
            max_attempts=max_attempts, timeout=request_timeout,
        ) if provider_class is not None else None
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


@research_group.command("catalog-update")
@click.option("--parent-snapshot", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--recent-cache-dir", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--calendar", "calendar_path", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
@click.option("--generation", default=None, help="One explicit original HTTP collection; never mix partial collections.")
@click.option("--through", default=None, help="Optional verified common session cutoff, YYYY-MM-DD.")
def catalog_update_command(parent_snapshot: Path, recent_cache_dir: Path, calendar_path: Path,
                           output_dir: Path, generation: str | None, through: str | None) -> None:
    """Offline append of audited complete dates; reject historical revisions."""
    from stopat30m.data.research_catalog import build_incremental_dataset
    try:
        artifact = build_incremental_dataset(parent_snapshot, recent_cache_dir, calendar_path,
                                              output_dir, generation=generation, through=through)
    except (ValueError, OSError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Dataset version: {artifact.path}")
    click.echo(f"Common verified cutoff: {artifact.manifest['request']['end_date']}")
    click.echo("Original collections preserved. Exact overlap checks do not prove point-in-time total returns.")


@research_group.command("forward-train")
@click.option("--snapshot", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--reference-model-protocol", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
def forward_train_command(snapshot: Path, reference_model_protocol: Path, output_dir: Path) -> None:
    """Refit frozen features/parameters on mature labels for a new future period."""
    from stopat30m.data.historical_universe import HistoricalUniverse
    from stopat30m.forward_training import fit_forward_candidate
    try:
        reference = _read_json(reference_model_protocol)
        universe = HistoricalUniverse.from_dict(reference["base_protocol"]["universe"])
        model = fit_forward_candidate(snapshot, universe, reference_model_protocol, output_dir)
    except (ValueError, KeyError, OSError, RuntimeError, ImportError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Forward candidate: {model.path}")
    click.echo(f"Model SHA-256: {model.model_sha256}")
    click.echo(f"Training label cutoff: {model.manifest['training']['declared_cutoff']}")
    click.echo("Historical test dates may now be training input. Validate this version only on future frozen signals.")


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


@research_group.command("model-lock")
@click.option("--protocol", "protocol_path", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--model-protocol", type=click.Path(path_type=Path), required=True)
def model_lock_command(protocol_path: Path, model_protocol: Path) -> None:
    """Freeze one LightGBM feature/label/parameter specification before fitting."""
    from stopat30m.backtest.model_comparison import lock_model_protocol
    try:
        result = lock_model_protocol(protocol_path, model_protocol)
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Model protocol: {model_protocol.resolve()}")
    click.echo(f"SHA-256: {result['protocol_sha256']}")


@research_group.command("compare")
@click.option("--snapshot", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--protocol", "protocol_path", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--model-protocol", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
@click.option("--suspensions", type=click.Path(exists=True, path_type=Path), default=None,
              help="Explicit source-backed suspension declaration bound to this exact snapshot.")
@click.option("--frozen-model", type=click.Path(exists=True, path_type=Path), default=None,
              help="Explicitly reuse a verified model from an interrupted run; never refit or retune it.")
def compare_command(snapshot: Path, protocol_path: Path, model_protocol: Path, output_dir: Path, suspensions: Path | None, frozen_model: Path | None) -> None:
    """Fit once, compare fixed rules/model, and preserve all cost scenarios."""
    from stopat30m.data.historical_universe import HistoricalUniverse
    from stopat30m.data.research_prices import load_research_prices
    from stopat30m.research_experiment import run_comparison
    try:
        result = run_comparison(load_research_prices(snapshot), HistoricalUniverse.load(_universe_path(protocol_path)),
                                protocol_path, model_protocol, output_dir, suspension_evidence=suspensions,
                                frozen_model_path=frozen_model)
    except (ValueError, OSError, RuntimeError, ImportError) as exc:
        if output_dir.is_dir() and not (output_dir / "comparison.json").exists():
            (output_dir / "failed_attempt.json").write_text(json.dumps({
                "failed_at": datetime.now(timezone.utc).isoformat(), "error_type": type(exc).__name__,
                "error": str(exc)[:2000], "complete_comparison_created": False,
                "partial_artifacts_preserved": True,
            }, ensure_ascii=False, indent=2))
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Comparison: {output_dir.resolve()}")
    for row in result["metrics"]:
        if row["split"] == "final_oos" and row["cost_bps_per_side"] == 5:
            click.echo(f"{row['strategy']}: net CAGR {row['annual_return']:.2%}; max drawdown {row['max_drawdown']:.2%}")
    click.echo("Retrospective adjusted-price research; no live performance or broker execution claim.")


@research_group.command("paper-init")
@click.option("--experiment", type=click.Path(exists=True, path_type=Path), default=None)
@click.option("--model-artifact", type=click.Path(exists=True, path_type=Path), default=None)
@click.option("--paper-dir", type=click.Path(path_type=Path), required=True)
@click.option("--calendar", "calendar_path", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--initial-capital", type=click.FloatRange(min=1), default=100000.0, show_default=True)
def paper_init_command(experiment: Path | None, model_artifact: Path | None, paper_dir: Path,
                       calendar_path: Path, initial_capital: float) -> None:
    """Start a distinct future period with one verified, immutable model."""
    from stopat30m.forward_runner import initialize_forward
    try:
        result = initialize_forward(experiment, paper_dir, calendar_path,
                                    initial_capital=initial_capital, model_artifact=model_artifact)
    except (ValueError, OSError, RuntimeError, ImportError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Forward period: {paper_dir.resolve()}")
    click.echo(f"Frozen model: {result['manifest']['model']['model_sha256']}")
    click.echo("Initial cash only. Performance starts after a prior-day signal and the next completed session.")


@research_group.command("paper-daily")
@click.option("--paper-dir", type=click.Path(exists=True, path_type=Path), required=True)
def paper_daily_command(paper_dir: Path) -> None:
    """Post-close bounded download, settle a prior signal, then freeze today's signal."""
    from stopat30m.forward_runner import run_daily
    try:
        result = run_daily(paper_dir)
    except (ValueError, OSError, RuntimeError, ImportError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"{result['session_date']}: {result['status']}")
    if result.get("paper"):
        if result["paper"]["manifest"]["model"].get("return_basis"):
            click.echo("Basis: provider adjusted-price exposure; not investable cash/units total return.")
        click.echo(f"Recorded forward sessions: {result['paper']['session_count']}")
        for strategy, values in result["paper"]["portfolios"].items():
            click.echo(f"{strategy}: net return {values['cumulative_net_return']:.2%}; max drawdown {values['max_drawdown']:.2%}")
        if not result["paper"]["has_performance"]:
            click.echo("No forward performance yet; first signal is frozen for the next verified session.")


@research_group.command("paper-status")
@click.option("--paper-dir", type=click.Path(exists=True, path_type=Path), required=True)
def paper_status_command(paper_dir: Path) -> None:
    """Verify saved signal/NAV chains and show costs and drawdown, without network."""
    from stopat30m.trading.forward_paper import forward_paper_status
    try:
        result = forward_paper_status(paper_dir)
    except (ValueError, OSError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    brief = {key: result[key] for key in ("latest_signal_date", "latest_settlement_date", "session_count", "has_performance", "halted", "gaps", "portfolios")}
    brief["return_basis"] = result["manifest"]["model"].get("return_basis", "adjusted_fractional_exposure_research")
    click.echo(json.dumps(brief, ensure_ascii=False, indent=2, allow_nan=False))


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


from stopat30m.ashare_forward_cli import register_commands as _register_ashare_forward_commands

_register_ashare_forward_commands(research_group)

from stopat30m.ashare_validation_cli import register_commands as _register_ashare_validation_commands

_register_ashare_validation_commands(research_group)

from stopat30m.ashare_scope_cli import register_commands as _register_ashare_scope_commands

_register_ashare_scope_commands(research_group)

from stopat30m.ashare_union_cli import register_commands as _register_ashare_union_commands

_register_ashare_union_commands(research_group)

from stopat30m.ashare_stability_cli import register_commands as _register_ashare_stability_commands

_register_ashare_stability_commands(research_group)
