"""
vn.py trading engine wrapper.

Manages the lifecycle of vn.py's MainEngine, gateway connections,
and integrates the AlphaPortfolioStrategy with real-time data feeds.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Callable

from loguru import logger

from stopat30m.config import get
from stopat30m.trading.risk import RiskManager
from stopat30m.trading.strategy import AlphaPortfolioStrategy


class TradingEngine:
    """
    Orchestrates vn.py MainEngine with alpha signal consumption.

    Lifecycle:
      1. init() -> connect gateway
      2. load_strategy() -> bind strategy
      3. start() -> event loop (subscribe signals, execute, monitor)
      4. stop() -> graceful shutdown
    """

    def __init__(self, strategy: AlphaPortfolioStrategy | None = None):
        self.strategy = strategy or AlphaPortfolioStrategy()
        self._main_engine: Any = None
        self._event_engine: Any = None
        self._running = False
        self._signal_thread: threading.Thread | None = None

    def init(self) -> None:
        """Initialize vn.py engines and connect gateway."""
        trading_cfg = get("trading") or {}

        if trading_cfg.get("paper_trading", True):
            logger.info("Running in PAPER TRADING mode. No gateway connection.")
            return

        try:
            from vnpy.event import EventEngine
            from vnpy.trader.engine import MainEngine

            self._event_engine = EventEngine()
            self._main_engine = MainEngine(self._event_engine)

            gateway = trading_cfg.get("gateway", "xtp")
            self._load_gateway(gateway)
            self._connect_gateway(trading_cfg)

            self.strategy.set_engine(self._main_engine)
            logger.info("Trading engine initialized.")
        except ImportError:
            logger.warning("vnpy not installed. Falling back to paper trading.")
            self.strategy.paper_trading = True

    def _load_gateway(self, gateway_name: str) -> None:
        """Dynamically load the appropriate vn.py gateway."""
        gateway_map = {
            "xtp": ("vnpy_xtp", "XtpGateway"),
            "ctp": ("vnpy_ctp", "CtpGateway"),
        }

        if gateway_name not in gateway_map:
            raise ValueError(f"Unsupported gateway: {gateway_name}. Supported: {list(gateway_map.keys())}")

        module_name, class_name = gateway_map[gateway_name]
        try:
            import importlib
            mod = importlib.import_module(module_name)
            gateway_cls = getattr(mod, class_name)
            self._main_engine.add_gateway(gateway_cls)
            logger.info(f"Gateway loaded: {gateway_name}")
        except ImportError:
            raise ImportError(f"Gateway package not installed: {module_name}")

    def _connect_gateway(self, cfg: dict) -> None:
        """Connect to broker via gateway."""
        setting = {
            "account": cfg.get("account", ""),
            "password": cfg.get("password", ""),
            "host": cfg.get("broker_host", ""),
            "port": cfg.get("broker_port", 0),
        }
        gateway_name = cfg.get("gateway", "xtp")
        self._main_engine.connect(setting, gateway_name)
        logger.info(f"Connected to gateway: {gateway_name}")

    def start(self, signal_source: str | Path | None = None, poll_interval: int = 60) -> None:
        """
        Start the trading loop.

        Args:
            signal_source: Path to signal file, or None for Redis subscription.
            poll_interval: Seconds between signal polls (file mode).
        """
        self._running = True
        logger.info("Trading engine started.")

        if signal_source:
            self._run_file_mode(Path(signal_source), poll_interval)
        else:
            self._run_redis_mode()

    def _run_file_mode(self, signal_dir: Path, interval: int) -> None:
        """Poll signal directory for new files."""
        seen_files: set[str] = set()

        while self._running:
            try:
                if signal_dir.is_file():
                    signals = self.strategy.load_signals(signal_dir)
                    self.strategy.rebalance(signals)
                    self._running = False
                    break

                csv_files = sorted(signal_dir.glob("signal_*.csv"))
                for f in csv_files:
                    if f.name not in seen_files:
                        seen_files.add(f.name)
                        signals = self.strategy.load_signals(f)
                        self.strategy.rebalance(signals)
                        logger.info(f"Processed signal file: {f.name}")

            except Exception as e:
                logger.error(f"Error in file mode: {e}")

            time.sleep(interval)

    def _run_redis_mode(self) -> None:
        """Subscribe to Redis for real-time signals."""
        try:
            import redis

            cfg = get("redis") or {}
            r = redis.Redis(
                host=cfg.get("host", "localhost"),
                port=cfg.get("port", 6379),
                db=cfg.get("db", 0),
            )
            pubsub = r.pubsub()
            channel = cfg.get("signal_channel", "alpha_signals")
            pubsub.subscribe(channel)
            logger.info(f"Subscribed to Redis channel: {channel}")

            for message in pubsub.listen():
                if not self._running:
                    break
                if message["type"] != "message":
                    continue

                try:
                    import pandas as pd
                    data = json.loads(message["data"])
                    signals = pd.DataFrame(data)
                    self.strategy.rebalance(signals)
                except Exception as e:
                    logger.error(f"Error processing Redis signal: {e}")

        except ImportError:
            logger.error("redis package not installed. Cannot run Redis mode.")
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")

    def stop(self) -> None:
        """Gracefully stop the trading engine."""
        self._running = False

        if self._main_engine is not None:
            try:
                self._main_engine.close()
            except Exception as e:
                logger.error(f"Error closing engine: {e}")

        logger.info("Trading engine stopped.")

    def get_status(self) -> dict:
        """Get comprehensive engine status."""
        return {
            "running": self._running,
            "paper_trading": self.strategy.paper_trading,
            "portfolio": self.strategy.get_portfolio_summary(),
        }
