"""Render verified research results as a standalone NAV/drawdown figure."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


def render_research_curve(run_dir: str | Path, output: str | Path) -> Path:
    root, target = Path(run_dir), Path(output)
    manifest = json.loads((root / "manifest.json").read_text())
    for filename, expected in manifest["output_checksums"].items():
        if Path(filename).name != filename:
            raise ValueError("Invalid run output filename")
        actual = hashlib.sha256((root / filename).read_bytes()).hexdigest()
        if actual != expected:
            raise ValueError(f"Run output checksum mismatch: {filename}")
    frame = pd.read_csv(root / "returns.csv", parse_dates=["date", "signal_date"])
    frame = frame.loc[frame["split"].eq("final_oos")]
    if frame.empty:
        raise ValueError("Run contains no final OOS observations")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    config = manifest["config"]
    names = {
        "momentum": "Momentum, net costs",
        "equal_weight": "Equal weight, net costs",
        "buy_hold": f"{config['buy_hold_instrument']} buy & hold, net costs",
    }
    colors = {"momentum": "#2858a5", "equal_weight": "#9a6730", "buy_hold": "#58806b"}
    fig, axes = plt.subplots(2, 1, figsize=(10.5, 7), sharex=True, gridspec_kw={"height_ratios": [2, 1]})
    for strategy, label in names.items():
        series = frame.loc[frame["strategy"].eq(strategy)].sort_values("date")
        if series.empty:
            continue
        dates = pd.DatetimeIndex([series["signal_date"].iloc[0], *series["date"]])
        nav = pd.Series([1.0, *series["nav"]], index=dates)
        axes[0].plot(nav.index, nav, label=label, color=colors[strategy], linewidth=1.5)
        axes[1].plot(nav.index, nav / nav.cummax() - 1, color=colors[strategy], linewidth=1)
    benchmark = frame.loc[frame["strategy"].eq("momentum")].sort_values("date")
    dates = pd.DatetimeIndex([benchmark["signal_date"].iloc[0], *benchmark["date"]])
    nav = pd.Series([1.0, *benchmark["benchmark_nav"]], index=dates)
    axes[0].plot(nav.index, nav, color="#777777", linestyle="--", label=f"{config['benchmark']} price index, no dividends/costs", linewidth=1)
    axes[1].plot(nav.index, nav / nav.cummax() - 1, color="#777777", linestyle="--", linewidth=1)
    start, end = frame["date"].min().date(), frame["date"].max().date()
    provider = manifest["source_manifest"]["provider"]["name"]
    fig.suptitle(f"Retrospective OOS: {start} to {end}", fontsize=14, x=0.08, ha="left")
    axes[0].set_ylabel("Net asset value (initial = 1)")
    axes[0].legend(loc="upper left", fontsize=8, frameon=False)
    axes[1].set_ylabel("Drawdown (%)")
    axes[1].set_xlabel("Date (CN exchange sessions)")
    axes[1].yaxis.set_major_formatter(PercentFormatter(1))
    for ax in axes:
        ax.grid(axis="y", color="#e7e7e7", linewidth=0.6)
        ax.spines[["right", "top"]].set_visible(False)
    fig.text(0.08, 0.015, f"Source: {provider} | Adjusted fractional exposure research | Rules locked in 2026; no live-performance claim", fontsize=8, color="#555555")
    fig.tight_layout(rect=(0, 0.035, 1, 0.96))
    target.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(target, dpi=160)
    plt.close(fig)
    return target
