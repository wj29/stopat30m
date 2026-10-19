"""Run one frozen model against locked rules, with the same portfolio accounting."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import math

import pandas as pd

from stopat30m.backtest.walk_forward import ResearchConfig, load_protocol, lock_protocol, run_walk_forward

COST_SCENARIOS = (5.0, 10.0, 20.0)


def _json(path: Path, value) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n")


def _model_targets(model, predictions, signal_day, execution_day, eligible, artifact, cfg, halts):
    """Exclude only an explicitly evidenced suspension-history feature gap.

    Ordinary missing predictions still stop the run. The baseline uses the
    same conservative 60-session complete-history eligibility requirement.
    No absent OHLC is imputed and every excluded candidate is recorded.
    """
    current = predictions.loc[pd.to_datetime(predictions.signal_date).eq(signal_day)].set_index("instrument")
    location = artifact.calendar.get_loc(signal_day)
    warmup = artifact.calendar[max(0, location - cfg.lookback_sessions):location + 1]
    usable, excluded = [], []
    for instrument in eligible:
        if instrument not in current.index:
            raise ValueError("Missing required model prediction")
        score = float(current.at[instrument, "score"])
        if math.isfinite(score):
            usable.append(instrument)
        elif any((day, instrument) in halts for day in warmup):
            excluded.append({"instrument": instrument, "score": None, "selected": False,
                             "reason": "issuer_verified_suspension_in_required_history",
                             "model_sha256": model.model_sha256})
        else:
            raise ValueError("Missing model features without an evidenced suspension; refusing fallback")
    if not usable:
        raise ValueError("No complete model candidates; refusing to fabricate a cash signal")
    weights, rows = model.target_weights(predictions, signal_day, execution_day, usable,
                                         cash_reserve_pct=cfg.cash_reserve_pct)
    return weights, [*rows, *excluded]


def run_comparison(artifact, universe, protocol_path, model_protocol_path, output_dir, *, suspension_evidence=None,
                   frozen_model_path=None):
    """Fit once on the locked development segment, never select a winner.

    The 5/10/20bp-per-side scenarios were declared in the original research
    specification. They reuse the exact same fitted model and predictions.
    Development metrics are in-sample diagnostics, not profitability evidence.
    """
    from stopat30m.backtest.model_comparison import _load_protocol, fit_frozen_model, load_frozen_model

    base = load_protocol(protocol_path)
    model_protocol, _, _ = _load_protocol(model_protocol_path)
    if (model_protocol["base_protocol_sha256"] != base["protocol_sha256"]
            or model_protocol["base_protocol"] != base):
        raise ValueError("The model protocol is bound to a different base experiment")
    cfg = ResearchConfig(**base["config"])
    halts, halt_declaration = {}, None
    if suspension_evidence is not None:
        declaration_path = Path(suspension_evidence)
        halt_declaration = json.loads(declaration_path.read_text())
        if halt_declaration.get("input_snapshot_id") != artifact.manifest["artifact_id"]:
            raise ValueError("Suspension declaration belongs to a different source snapshot")
        for event in halt_declaration["events"]:
            if event.get("not_tradeable") is not True or event.get("known_at", "9999")[:10] > event["date"]:
                raise ValueError("Invalid or future-known suspension declaration")
            key = (pd.Timestamp(event["date"]), event["instrument"])
            if key in halts:
                raise ValueError("Duplicate suspension declaration")
            halts[key] = event
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=False)
    plan = {
        "schema_version": 1, "base_protocol_sha256": base["protocol_sha256"],
        "model_protocol_sha256": model_protocol["protocol_sha256"],
        "orchestrator_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "cost_bps_per_side": list(COST_SCENARIOS), "selection_performed": False,
        "locked_before_fit_at": datetime.now(timezone.utc).isoformat(),
        "comparison": ["momentum", "lightgbm", "equal_weight", "buy_hold"],
        "development_is_in_sample": True,
        "verified_suspension_evidence": halt_declaration,
        "suspension_valuation_policy": "Explicit issuer-verified halts only: carry last valid close for valuation, no OHLC/volume filling or halted execution; held halts defer rebalancing. Feature gaps remain missing.",
        "return_basis": "provider_adjusted_price_exposure_not_investable_total_return",
        "model_history_gap_policy": "Explicit issuer-verified suspension within the required 60-session history makes that ETF ineligible; no OHLC/feature filling. Ordinary missing scores stop. Same history rule as momentum.",
        "reused_frozen_model": frozen_model_path is not None,
    }
    _json(output / "comparison_plan.json", plan)
    shutil.copyfile(__file__, output / "experiment_implementation.py")
    if frozen_model_path is None:
        model = fit_frozen_model(artifact, universe, model_protocol_path, output / "frozen_model")
    else:
        model = load_frozen_model(frozen_model_path)
        if (model.manifest["protocol_sha256"] != plan["model_protocol_sha256"]
                or model.manifest["base_protocol_sha256"] != base["protocol_sha256"]
                or model.manifest["source_manifest_sha256"] != artifact.manifest_sha256):
            raise ValueError("Existing frozen model does not belong to this model protocol/source snapshot")
        shutil.copytree(model.path, output / "frozen_model")
        model = load_frozen_model(output / "frozen_model")
    predictions = model.predict(artifact)
    predictions.to_csv(output / "predictions.csv", index=False, float_format="%.17g")

    def targets(signal_day, execution_day, eligible):
        return _model_targets(model, predictions, signal_day, execution_day, eligible, artifact, cfg, halts)

    metadata = {
        "model_sha256": model.model_sha256,
        "model_manifest": model.manifest,
        "fitted_once": True, "validation_used_for_model_selection": False,
        "final_oos_used_for_model_selection": False,
        "development_metrics_are_in_sample": True,
        "model_base_protocol_sha256": base["protocol_sha256"],
        "return_basis": plan["return_basis"],
    }
    runs, rows = {}, []
    for side in COST_SCENARIOS:
        scenario = replace(cfg, buy_fee_bps=side - cfg.slippage_bps,
                           sell_fee_bps=side - cfg.slippage_bps)
        if side < cfg.slippage_bps:
            raise ValueError("Cost scenario is smaller than the locked slippage")
        if scenario == cfg:
            locked = Path(protocol_path)
        else:
            locked = output / f"cost_{int(side)}bps_protocol.json"
            lock_protocol(scenario, locked, universe=universe)
        run = run_walk_forward(artifact, universe, locked, output / f"cost_{int(side)}bps",
                               additional_strategies={"lightgbm": targets},
                               comparison_metadata={**metadata, "cost_bps_per_side": side}, known_halts=halts)
        runs[str(int(side))] = str(run.path)
        for split, strategies in run.report["splits"].items():
            for strategy, metrics in strategies.items():
                rows.append({"split": split, "strategy": strategy,
                             "cost_bps_per_side": side, **metrics})
    summary = pd.DataFrame(rows)
    summary.to_csv(output / "comparison.csv", index=False, float_format="%.17g")
    primary = pd.read_csv(output / "cost_5bps/returns.csv", parse_dates=["date"])
    annual = []
    for (split, strategy, year), group in primary.groupby(
            [primary["split"], primary["strategy"], primary["date"].dt.year], sort=True):
        nav = (1 + group.net_return).cumprod()
        peak = nav.cummax().clip(lower=1)
        annual.append({"split": split, "strategy": strategy, "year": int(year),
                       "net_return": float(nav.iloc[-1] - 1),
                       "max_drawdown_within_year": float((nav / peak - 1).min()),
                       "sessions": len(group)})
    pd.DataFrame(annual).to_csv(output / "annual_returns.csv", index=False, float_format="%.17g")
    report = {
        **plan, "completed_at": datetime.now(timezone.utc).isoformat(),
        "source_snapshot": str(artifact.path), "source_provider": artifact.manifest["provider"],
        "source_manifest_sha256": artifact.manifest_sha256,
        "model_sha256": model.model_sha256, "model_path": str(model.path),
        "runs": runs, "metrics": summary.astype(object).where(pd.notna(summary), None).to_dict("records"),
        "limitations": [
            "Three ETFs selected in 2026: retrospective asset-selection bias remains.",
            "Adjusted fractional exposure research; no actual broker execution or integer-share fill claim.",
            "One frozen historical-development model; no validation/OOS tuning or winner promotion.",
            "Daily bars do not establish auction fillability, minimum commission or market impact.",
            "Future paper performance starts only after time-stamped signals have been frozen.",
            "Tencent HFQ is an affine cumulative-cash-adjusted price series, not a dividend-reinvestment total-return index. Returns and ETF rankings may be distorted by pre-entry cumulative cash distributions; this round cannot identify an investable winner.",
        ],
    }
    _json(output / "comparison.json", report)
    lines = ["# 固定策略与 LightGBM 的真实历史对照", "",
             "以下为历史回看研究。开发段为样本内诊断，验证和最终样本外独立列示；没有按结果择优或自动启用实盘。", "",
             "**本轮仅为腾讯供应商复权价格敞口研究，不是ETF可投资总回报。其累计现金分红口径会影响百分比收益和标的排序，需要原价加公司行为现金/份额核验后，才能用于盈利判断。**", "",
             f"数据来源：{artifact.manifest['provider']['name']}；训练区间 {cfg.train_start} 至 {cfg.train_end}。",
             f"最终历史样本外：{cfg.oos_start} 至 {cfg.oos_end}。主结果每边费用加滑点共 5bp；10/20bp 是同模型的成本敏感性。", ""]
    for split, title in (("validation", "验证段（不用于调参）"), ("final_oos", "最终历史样本外")):
        lines += [f"## {title}", ""]
        for row in report["metrics"]:
            if row["split"] == split:
                lines.append(f"- {row['strategy']} / 每边{row['cost_bps_per_side']:g}bp："
                             f"扣费累计收益 {row['total_return']:.2%}，年化 {row['annual_return']:.2%}，"
                             f"最大回撤 {row['max_drawdown']:.2%}，成交 {row['total_trades']} 笔。")
        lines.append("")
    lines += ["## 可复核文件", "", "- comparison.csv：全部分段与成本口径。",
              "- annual_returns.csv：逐年扣费收益和年内回撤。",
              "- frozen_model/：模型、特征、训练标签和锁定声明。",
              "- cost_5bps/：主结果逐日净值、成交、持仓和信号。", "",
              "## 解释范围", "", *[f"- {item}" for item in report["limitations"]], ""]
    (output / "REPORT.md").write_text("\n".join(lines))
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
              for p in sorted(output.iterdir()) if p.is_file()}
    _json(output / "experiment_manifest.json", {"schema_version": 1,
          "output_checksums": hashes, "model_sha256": model.model_sha256,
          "base_protocol_sha256": base["protocol_sha256"]})
    return report
