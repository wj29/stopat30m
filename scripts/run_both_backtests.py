#!/usr/bin/env python3
from __future__ import annotations

import argparse

from stopat30m.backtest.account_backtest import AccountBacktestEngine
from stopat30m.backtest.common import load_prediction_source
from stopat30m.backtest.signal_backtest import SignalBacktestEngine


def main() -> None:
    parser = argparse.ArgumentParser(description="Run signal and account backtests in one process.")
    parser.add_argument("--model-path", default=None, help="Path to trained model .pkl")
    parser.add_argument("--pred-path", default=None, help="Path to cached predictions .pkl")
    parser.add_argument("--tag", default="", help="Optional output tag")
    parser.add_argument("--universe", default=None, help="Optional universe override")
    parser.add_argument("--factor-groups", default=None, help="Optional comma-separated factor groups")
    args = parser.parse_args()

    pred, _, metadata = load_prediction_source(
        model_path=args.model_path,
        pred_path=args.pred_path,
        universe=args.universe,
        factor_groups=args.factor_groups,
    )
    print(f"predictions_loaded rows={len(pred)} source={metadata.get('source')} universe={metadata.get('universe', metadata.get('source_path'))}")

    signal_engine = SignalBacktestEngine()
    signal_result = signal_engine.run(pred)
    signal_dir = signal_result.save(tag=args.tag)
    print(f"signal_run={signal_dir}")

    account_engine = AccountBacktestEngine()
    account_result = account_engine.run(pred)
    account_dir = account_result.save(tag=args.tag)
    print(f"account_run={account_dir}")


if __name__ == "__main__":
    main()
