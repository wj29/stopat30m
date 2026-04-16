# -*- coding: utf-8 -*-
"""
EventMonitor — lightweight event-driven alert system.

Monitors a set of stocks for threshold events and triggers
notifications when conditions are met.  Designed to run as a
background task (e.g. via ``--schedule`` or a dedicated loop).

Currently supported runtime events:
- Price crossing threshold (above / below)
- Volume spike (> N× average)

Other alert types remain defined as enum placeholders for future
extension, but config validation rejects them until the monitor can
actually evaluate them.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from loguru import logger

from stopat30m.config import get


class AlertType(str, Enum):
    PRICE_CROSS = "price_cross"
    VOLUME_SPIKE = "volume_spike"
    SENTIMENT_SHIFT = "sentiment_shift"
    RISK_FLAG = "risk_flag"
    CUSTOM = "custom"


class AlertStatus(str, Enum):
    ACTIVE = "active"
    TRIGGERED = "triggered"
    EXPIRED = "expired"
    DISMISSED = "dismissed"


_RUNTIME_SUPPORTED_ALERT_TYPES = frozenset({
    AlertType.PRICE_CROSS,
    AlertType.VOLUME_SPIKE,
})


def _supported_alert_type_names() -> str:
    return ", ".join(sorted(alert_type.value for alert_type in _RUNTIME_SUPPORTED_ALERT_TYPES))


def _ensure_runtime_supported_alert_type(alert_type: AlertType) -> None:
    if alert_type not in _RUNTIME_SUPPORTED_ALERT_TYPES:
        raise ValueError(
            f"unsupported alert_type for current EventMonitor runtime: {alert_type.value} "
            f"(supported: {_supported_alert_type_names()})"
        )


@dataclass
class AlertRule:
    """Base alert rule definition."""

    stock_code: str
    alert_type: AlertType
    description: str = ""
    status: AlertStatus = AlertStatus.ACTIVE
    created_at: float = field(default_factory=time.time)
    triggered_at: Optional[float] = None
    ttl_hours: float = 24.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PriceAlert(AlertRule):
    """Alert when price crosses a threshold."""

    alert_type: AlertType = AlertType.PRICE_CROSS
    direction: str = "above"
    price: float = 0.0

    def __post_init__(self) -> None:
        if not self.description:
            self.description = f"{self.stock_code} price {self.direction} {self.price}"


@dataclass
class VolumeAlert(AlertRule):
    """Alert when volume exceeds N× average."""

    alert_type: AlertType = AlertType.VOLUME_SPIKE
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        if not self.description:
            self.description = f"{self.stock_code} volume > {self.multiplier}× average"


@dataclass
class SentimentAlert(AlertRule):
    """Alert on sentiment direction change."""

    alert_type: AlertType = AlertType.SENTIMENT_SHIFT
    from_sentiment: str = "positive"
    to_sentiment: str = "negative"

    def __post_init__(self) -> None:
        if not self.description:
            self.description = f"{self.stock_code} sentiment shift: {self.from_sentiment} → {self.to_sentiment}"


@dataclass
class TriggeredAlert:
    """An alert that was triggered, ready for notification."""

    rule: AlertRule
    triggered_at: float = field(default_factory=time.time)
    current_value: Any = None
    message: str = ""


class EventMonitor:
    """Monitor stocks for event-driven alerts."""

    def __init__(self) -> None:
        self.rules: List[AlertRule] = []
        self._callbacks: List[Callable[[TriggeredAlert], None]] = []

    def add_alert(self, rule: AlertRule) -> None:
        """Register a new alert rule."""
        _ensure_runtime_supported_alert_type(rule.alert_type)
        self.rules.append(rule)
        logger.info("[EventMonitor] Added alert: %s", rule.description)

    def remove_expired(self) -> int:
        """Remove alerts that have expired based on TTL."""
        now = time.time()
        before = len(self.rules)
        self.rules = [
            r
            for r in self.rules
            if r.status != AlertStatus.EXPIRED and (now - r.created_at) < r.ttl_hours * 3600
        ]
        removed = before - len(self.rules)
        if removed:
            logger.info("[EventMonitor] Removed %d expired alerts", removed)
        return removed

    def on_trigger(self, callback: Callable[[TriggeredAlert], None]) -> None:
        """Register a callback for when an alert triggers."""
        self._callbacks.append(callback)

    async def check_all(self) -> List[TriggeredAlert]:
        """Check all active rules against current market data."""
        self.remove_expired()
        triggered: List[TriggeredAlert] = []

        for rule in self.rules:
            if rule.status != AlertStatus.ACTIVE:
                continue

            try:
                result = await self._check_rule(rule)
                if result:
                    triggered.append(result)
                    rule.status = AlertStatus.TRIGGERED
                    rule.triggered_at = time.time()
                    for cb in self._callbacks:
                        try:
                            if asyncio.iscoroutinefunction(cb):
                                await cb(result)
                            else:
                                await asyncio.to_thread(cb, result)
                        except Exception as exc:
                            logger.warning("[EventMonitor] Callback error: %s", exc)
            except Exception as exc:
                logger.debug("[EventMonitor] Check failed for %s: %s", rule.description, exc)

        return triggered

    async def _check_rule(self, rule: AlertRule) -> Optional[TriggeredAlert]:
        """Check a single rule.  Returns TriggeredAlert if condition met."""
        if isinstance(rule, PriceAlert):
            return await self._check_price(rule)
        if isinstance(rule, VolumeAlert):
            return await self._check_volume(rule)
        return None

    async def _check_price(self, rule: PriceAlert) -> Optional[TriggeredAlert]:
        """Check price alert against realtime quote."""
        try:

            def _fetch_prices() -> tuple[str, Dict[str, float]]:
                from stopat30m.data.normalize import normalize_instrument
                from stopat30m.data.realtime import fetch_spot_prices

                norm = normalize_instrument(rule.stock_code)
                return norm, fetch_spot_prices([norm])

            norm, prices = await asyncio.to_thread(_fetch_prices)
            current_price = float(prices.get(norm, 0) or 0)
            if current_price <= 0:
                return None

            triggered = False
            if rule.direction == "above" and current_price >= rule.price:
                triggered = True
            elif rule.direction == "below" and current_price <= rule.price:
                triggered = True

            if triggered:
                return TriggeredAlert(
                    rule=rule,
                    current_value=current_price,
                    message=f"🔔 {rule.stock_code} price {rule.direction} {rule.price}: "
                    f"current = {current_price}",
                )
        except Exception as exc:
            logger.debug("[EventMonitor] _check_price error: %s", exc)
        return None

    async def _check_volume(self, rule: VolumeAlert) -> Optional[TriggeredAlert]:
        """Check volume spike against recent average."""
        try:

            def _fetch_daily_data():
                from stopat30m.data.sources import fetch_daily_ohlcv

                return fetch_daily_ohlcv(rule.stock_code, days=20)

            result = await asyncio.to_thread(_fetch_daily_data)
            if result is None:
                return None
            df, _source = result
            if df is None or df.empty:
                return None

            vol_col = "volume" if "volume" in df.columns else None
            if vol_col is None:
                for c in ("vol", "成交量"):
                    if c in df.columns:
                        vol_col = c
                        break
            if vol_col is None:
                return None

            avg_vol = float(df[vol_col].mean())
            latest_vol = float(df[vol_col].iloc[-1])

            if avg_vol > 0 and latest_vol > avg_vol * rule.multiplier:
                return TriggeredAlert(
                    rule=rule,
                    current_value=latest_vol,
                    message=f"📊 {rule.stock_code} volume spike: "
                    f"{latest_vol:,.0f} ({latest_vol / avg_vol:.1f}× avg)",
                )
        except Exception as exc:
            logger.debug("[EventMonitor] _check_volume error: %s", exc)
        return None

    def to_dict_list(self) -> List[Dict[str, Any]]:
        """Serialize all rules for persistence."""
        results = []
        for rule in self.rules:
            entry: Dict[str, Any] = {
                "stock_code": rule.stock_code,
                "alert_type": rule.alert_type.value,
                "description": rule.description,
                "status": rule.status.value,
                "created_at": rule.created_at,
                "ttl_hours": rule.ttl_hours,
            }
            if isinstance(rule, PriceAlert):
                entry["direction"] = rule.direction
                entry["price"] = rule.price
            elif isinstance(rule, VolumeAlert):
                entry["multiplier"] = rule.multiplier
            results.append(entry)
        return results

    @classmethod
    def from_dict_list(cls, data: List[Dict[str, Any]]) -> EventMonitor:
        """Restore an EventMonitor from serialized data."""
        monitor = cls()
        for index, entry in enumerate(data, start=1):
            try:
                validate_event_alert_rule(entry)

                alert_type = entry.get("alert_type", "custom")
                stock_code = entry.get("stock_code", "")
                if alert_type == AlertType.PRICE_CROSS.value:
                    rule = PriceAlert(
                        stock_code=stock_code,
                        direction=entry.get("direction", "above").lower(),
                        price=float(entry.get("price", 0.0)),
                    )
                elif alert_type == AlertType.VOLUME_SPIKE.value:
                    rule = VolumeAlert(
                        stock_code=stock_code,
                        multiplier=float(entry.get("multiplier", 2.0)),
                    )
                else:
                    raise ValueError(f"unsupported alert_type: {alert_type}")
                rule.status = AlertStatus(entry.get("status", "active"))
                raw_created = entry.get("created_at")
                try:
                    rule.created_at = float(raw_created) if raw_created is not None else time.time()
                except (TypeError, ValueError):
                    rule.created_at = time.time()
                rule.ttl_hours = float(entry.get("ttl_hours", 24.0))
                monitor.add_alert(rule)
            except Exception as exc:
                logger.warning("[EventMonitor] Skip invalid rule #%d: %s", index, exc)
        return monitor


def parse_event_alert_rules(raw_rules: Any) -> List[Dict[str, Any]]:
    """Parse event alert rules from config JSON or already-loaded objects."""
    if raw_rules is None:
        return []

    parsed = raw_rules
    if isinstance(raw_rules, str):
        cleaned = raw_rules.strip()
        if not cleaned:
            return []
        parsed = json.loads(cleaned)

    if isinstance(parsed, dict):
        parsed = parsed.get("rules", [])

    if not isinstance(parsed, list):
        raise ValueError("Event alert rules must be a JSON array")

    invalid_indices = [idx for idx, entry in enumerate(parsed) if not isinstance(entry, dict)]
    if invalid_indices:
        raise ValueError(
            "Event alert rules list must contain only objects; "
            f"invalid entries at positions: {invalid_indices}"
        )

    return parsed


def validate_event_alert_rule(rule: Dict[str, Any]) -> None:
    """Validate one serialized EventMonitor rule."""
    if not isinstance(rule, dict):
        raise ValueError("Event alert rule must be an object")

    stock_code = str(rule.get("stock_code") or "").strip()
    if not stock_code:
        raise ValueError("stock_code is required")

    try:
        alert_type = AlertType(rule.get("alert_type", ""))
    except ValueError as exc:
        raise ValueError(f"invalid alert_type: {rule.get('alert_type')}") from exc
    _ensure_runtime_supported_alert_type(alert_type)

    status = rule.get("status")
    if status is not None:
        try:
            AlertStatus(status)
        except ValueError as exc:
            raise ValueError(f"invalid status: {status}") from exc

    ttl_hours = rule.get("ttl_hours")
    if ttl_hours is not None:
        try:
            ttl_value = float(ttl_hours)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid ttl_hours: {ttl_hours}") from exc
        if ttl_value <= 0:
            raise ValueError("ttl_hours must be > 0")

    if alert_type == AlertType.PRICE_CROSS:
        direction = str(rule.get("direction", "above")).lower()
        if direction not in {"above", "below"}:
            raise ValueError(f"invalid direction: {direction}")
        try:
            price = float(rule.get("price"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid price: {rule.get('price')}") from exc
        if price <= 0:
            raise ValueError("price must be > 0")
    elif alert_type == AlertType.VOLUME_SPIKE:
        try:
            multiplier = float(rule.get("multiplier", 2.0))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid multiplier: {rule.get('multiplier')}") from exc
        if multiplier <= 0:
            raise ValueError("multiplier must be > 0")


def build_event_monitor_from_config(config: Any = None, notifier: Any = None) -> Optional[EventMonitor]:
    """Build an EventMonitor from runtime config and attach notification callbacks."""
    enabled = bool(
        get("analysis", "event_monitor_enabled", False) or get("agent", "event_monitor_enabled", False)
    )
    if not enabled:
        return None

    raw_rules = (
        get("analysis", "event_alert_rules_json")
        or get("agent", "event_alert_rules_json")
        or ""
    ) or ""
    try:
        rules = parse_event_alert_rules(raw_rules)
    except Exception as exc:
        logger.warning("[EventMonitor] Failed to parse configured alert rules: %s", exc)
        return None

    if not rules:
        logger.info("[EventMonitor] Enabled but no alert rules configured")
        return None

    monitor = EventMonitor.from_dict_list(rules)
    if not monitor.rules:
        return None

    from stopat30m.notification.service import NotificationBuilder, NotificationService

    notification_service = notifier or NotificationService()

    def _notify(triggered: TriggeredAlert) -> None:
        title = f"Event Alert | {triggered.rule.stock_code}"
        content = triggered.message or triggered.rule.description or "Alert triggered"
        alert_text = NotificationBuilder.build_simple_alert(title=title, content=content, alert_type="warning")
        sent = notification_service.send(alert_text)
        if not sent:
            logger.info("[EventMonitor] No notification channel available for alert: %s", title)

    monitor.on_trigger(_notify)
    logger.info("[EventMonitor] Loaded %d configured alert rule(s)", len(monitor.rules))
    return monitor


def run_event_monitor_once(monitor: EventMonitor) -> List[TriggeredAlert]:
    """Run one synchronous monitor cycle."""
    return asyncio.run(monitor.check_all())
