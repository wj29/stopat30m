"""Clean-connection capture of original daily raw/HFQ responses."""
from __future__ import annotations

from datetime import date, datetime, timezone
import re

from .baostock_paced import PacedBaostockDeadlineClient
from .baostock_validated import ValidatedBaostockDividendClient


class ValidatedBaostockMarketClient(ValidatedBaostockDividendClient):
    """Verify daily-bar server echoes without editing original row mode flags."""

    def _metadata_query(self, name, request, *, fields, echo_keys):
        guard = {"kind": "baostock_metadata_server_echo_guard", "query": name,
                 "request": request, "status": "requesting", "rows_verified": 0,
                 "server_echo": None, "started_at": datetime.now(timezone.utc).isoformat()}
        self._guard = guard
        if self._transport_state["dirty"]:
            self._invalid("Metadata connection is dirty; require an explicit fresh connection")
        try:
            result = super().__getattr__(name)(**request)
        except Exception as exc:
            self._invalid("Metadata SDK request failed: " + type(exc).__name__)
        result._on_guard, result._guard = self._on_guard, guard
        result._transport_state = self._transport_state
        echo = {key: str(getattr(result, key, "")) for key in echo_keys}
        guard["server_echo"], guard["server_error_code"] = echo, str(result.error_code)
        if str(result.error_code) != "0":
            result._invalid("Metadata SDK server returned an unsuccessful response")
        if any(echo[key] != request[key] for key in echo_keys) or list(result.fields) != fields:
            result._invalid("Metadata server request echo/schema differs from request")
        guard["status"] = "server_echo_verified_pending_row_iteration"
        return result

    def query_stock_basic(self, code="", code_name=""):
        return self._metadata_query("query_stock_basic", {"code": code, "code_name": code_name},
            fields=["code", "code_name", "ipoDate", "outDate", "type", "status"],
            echo_keys=("code", "code_name"))

    def query_adjust_factor(self, *, code, start_date, end_date):
        return self._metadata_query("query_adjust_factor", {"code": code,
            "start_date": start_date, "end_date": end_date},
            fields=["code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"],
            echo_keys=("code", "start_date", "end_date"))

    def query_history_k_data_plus(self, *, code, fields, start_date, end_date,
                                  frequency="d", adjustflag="3"):
        request = {"code": code, "fields": fields, "start_date": start_date,
                   "end_date": end_date, "frequency": frequency, "adjustflag": adjustflag}
        guard = {"kind": "baostock_daily_server_echo_guard", "request": request,
                 "status": "requesting", "rows_verified": 0, "row_mode_anomalies_observed": [],
                 "server_echo": None, "started_at": datetime.now(timezone.utc).isoformat(),
                 "row_mode_flags_repaired": False}
        self._guard = guard
        if self._transport_state["dirty"]:
            self._invalid("Daily-bar connection is dirty; require an explicit fresh connection")
        try:
            result = super().__getattr__("query_history_k_data_plus")(**request)
        except Exception as exc:
            self._invalid("Daily-bar SDK request failed: " + type(exc).__name__)
        result._on_guard, result._guard = self._on_guard, guard
        result._transport_state = self._transport_state
        echo = {key: str(getattr(result, key, "")) for key in
                ("code", "start_date", "end_date", "frequency", "adjustflag")}
        echo["fields"] = ",".join(result.fields)
        guard["server_echo"], guard["server_error_code"] = echo, str(result.error_code)
        if str(result.error_code) != "0":
            result._invalid("Daily-bar SDK server returned an unsuccessful response")
        # The live daily-bar protocol returns an empty mode header for both
        # raw and HFQ requests. Preserve that absence; original row flags are
        # checked later by the strict source loader, never fabricated here.
        identity = {key: value for key, value in echo.items() if key != "adjustflag"}
        expected_identity = {key: value for key, value in request.items() if key != "adjustflag"}
        if identity != expected_identity or echo["adjustflag"] not in ("", adjustflag):
            result._invalid("Daily-bar server request/mode echo differs from request")
        guard["mode_server_echo_present"] = bool(echo["adjustflag"])
        guard["mode_validation_basis"] = ("server_echo_and_original_row_flags" if echo["adjustflag"]
            else "original_row_flags_retained_for_strict_source_loader;server_mode_echo_absent")
        guard["status"] = "server_echo_verified_pending_row_iteration"
        return result

    def get_row_data(self):
        if self._guard is not None and self._guard.get("kind") == "baostock_metadata_server_echo_guard":
            row = PacedBaostockDeadlineClient.__getattr__(self, "get_row_data")()
            if len(row) != len(self.fields):
                self._invalid("Metadata row fields differ from server schema")
            value = dict(zip(self.fields, row))
            request = self._guard["request"]
            if (not re.fullmatch(r"(?:sh|sz|bj)\.\d{6}", value.get("code", ""))
                    or request["code"] and value["code"] != request["code"]):
                self._invalid("Metadata row security differs from request")
            if self._guard["query"] == "query_adjust_factor":
                try:
                    day = date.fromisoformat(value["dividOperateDate"])
                except (ValueError, KeyError):
                    self._invalid("Factor row has an invalid operation date")
                if not request["start_date"] <= day.isoformat() <= request["end_date"]:
                    self._invalid("Factor row date lies outside its requested window")
            self._guard["rows_verified"] += 1
            return row
        if self._guard is None or self._guard.get("kind") != "baostock_daily_server_echo_guard":
            return super().get_row_data()
        row = PacedBaostockDeadlineClient.__getattr__(self, "get_row_data")()
        if len(row) != len(self.fields):
            self._invalid("Daily-bar row fields differ from server schema")
        values = dict(zip(self.fields, row))
        expected = self._guard["request"]
        if values.get("code") != expected["code"]:
            self._invalid("Daily-bar row security differs from request")
        try:
            day = date.fromisoformat(values["date"])
        except (ValueError, KeyError):
            self._invalid("Daily-bar row has an invalid date")
        if not expected["start_date"] <= day.isoformat() <= expected["end_date"]:
            self._invalid("Daily-bar row date lies outside its requested window")
        if values.get("adjustflag") != expected["adjustflag"]:
            self._guard["row_mode_anomalies_observed"].append({"date": day.isoformat(),
                "requested_mode": expected["adjustflag"], "returned_row_mode": values.get("adjustflag")})
        self._guard["rows_verified"] += 1
        return row
