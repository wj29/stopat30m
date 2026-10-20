"""Plot completed immutable horizon diagnostics; no training or selection."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def plot(source, output):
    source, output = Path(source).resolve(), Path(output).resolve()
    if output.exists() or output.is_relative_to(source) or source.is_relative_to(output):
        raise ValueError("Use a new directory outside the immutable source experiment")
    manifest = json.loads((source/"manifest.json").read_text())
    if manifest.get("kind") != "a7_frozen_lgb_horizon_diagnostics":
        raise ValueError("A completed, declared fixed-score diagnostic is required")
    for name, digest in manifest["files"].items():
        path = (source/name).resolve()
        if not path.is_relative_to(source) or sha(path) != digest:
            raise ValueError("Diagnostic artifact changed: "+name)
    data = pd.read_csv(source/"comparison.csv")
    if len(data) != 18 or data.duplicated(["period", "mode", "horizon_actual_sessions"]).any():
        raise ValueError("All18 native/matched diagnostic summaries are required")
    os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir())/"stopat30m-a7-matplotlib"))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
        "axes.spines.top": False, "axes.spines.right": False})
    fig, axes = plt.subplots(3, 2, figsize=(11, 10), layout="constrained")
    for row, period in enumerate(("2025", "2026", "continuous")):
        block = data.loc[data.period.eq(period) & data["mode"].eq("matched")].sort_values("horizon_actual_sessions")
        if (block.horizon_actual_sessions.tolist() != [5, 10, 20]
                or block.daily_observations.nunique() != 1
                or not np.isfinite(block[["rank_IC_mean", "rank_IC_positive_fraction"]]).all().all()):
            raise ValueError("Matched horizons require the same valid signal-day population")
        title = {"2025": "2025", "2026": "2026 through Oct 19 (partial)",
                 "continuous": "2025 to 2026-10-19"}[period]
        n = int(block.daily_observations.iloc[0])
        for col, field in enumerate(("rank_IC_mean", "rank_IC_positive_fraction")):
            ax, values = axes[row, col], block[field].to_numpy()
            ax.bar(np.arange(3), values, width=.55, color="#2563eb")
            ax.set_xticks(np.arange(3), ["5", "10", "20"])
            ax.set_title(f"{title} | {n} common signal dates")
            ax.grid(axis="y", alpha=.15); ax.set_axisbelow(True)
            ax.set_xlabel("Forward label horizon (actual sessions)")
            if col == 0:
                ax.set_ylabel("Mean daily cross-sectional Rank IC")
                ax.set_ylim(min(0, values.min()-.02), .16)
            else:
                ax.set_ylabel("Dates with positive Rank IC")
                ax.set_ylim(0, 1); ax.yaxis.set_major_formatter(PercentFormatter(1))
            for index, value in enumerate(values):
                ax.annotate(f"{value:.4f}" if col == 0 else f"{value:.1%}",
                    (index, value), xytext=(0, 4), textcoords="offset points", ha="center")
    fig.suptitle("Frozen LGB predictions: 5 / 10 / 20-session label diagnostics\n"
        "Same dates and stock names within each period; correlations are not portfolio returns", fontsize=13)
    output.mkdir(parents=True)
    fig.savefig(output/"matched_horizon_rank_ic.png", dpi=170)
    plt.close(fig)
    source_bytes = Path(__file__).read_bytes()
    (output/"source_plot.py").write_bytes(source_bytes)
    report = {"kind": "a7_frozen_horizon_scientific_plot", "training_or_strategy_changes": False,
        "source_manifest": {"path": str(source/"manifest.json"), "sha256": sha(source/"manifest.json")},
        "source_comparison": {"path": str(source/"comparison.csv"), "sha256": sha(source/"comparison.csv")},
        "plot_implementation_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "files": {path.name: sha(path) for path in output.iterdir() if path.is_file()}}
    (output/"manifest.json").write_text(json.dumps(report, indent=2)+"\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    plot(args.experiment_dir, args.output_dir)
