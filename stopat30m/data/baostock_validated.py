"""Validate server-side dividend identity before a response can be captured."""
from __future__ import annotations

from datetime import date, datetime, timezone
import re

from .baostock_paced import PacedBaostockDeadlineClient


def _discard_connection():
    """A failed request can leave a late response on the SDK's shared socket."""
    import baostock.common.context as context

    connection = getattr(context, "default_socket", None)
    if connection is not None:
        try:
            connection.close()
        finally:
            context.default_socket = None


class ValidatedBaostockDividendClient(PacedBaostockDeadlineClient):
    """Check response code/year/yearType echoes and every implemented row year.

    A timeout, failed server response, identity mismatch, or malformed row
    discards the connection. This adapter never reconnects or retries. The
    surrounding collector keeps its bounded consecutive-failure policy.
    ``on_guard`` receives evidence metadata for each completed or failed query.
    """

    def __init__(self, source, *, timeout=12, min_interval=0.6, on_guard=None):
        super().__init__(source, timeout=timeout, min_interval=min_interval)
        self._on_guard = on_guard
        self._guard = None
        self._transport_state = {"dirty": False}

    def _invalid(self, message):
        self._transport_state["dirty"] = True
        if self._guard is not None:
            self._guard.update(status="invalid", error=message,
                               finished_at=datetime.now(timezone.utc).isoformat())
            if self._on_guard is not None:
                self._on_guard(self._guard)
        _discard_connection()
        raise ValueError(message)

    def query_dividend_data(self, code, year=None, yearType="operate"):
        if (not isinstance(code, str) or not re.fullmatch(r"(?:sh|sz)\.\d{6}", code)
                or not re.fullmatch(r"\d{4}", str(year)) or yearType != "operate"):
            raise ValueError("Require explicit dividend code/year/operate request")
        request = {"code": code, "year": str(year), "yearType": yearType}
        guard = {"request": request, "status": "requesting", "rows_verified": 0,
                 "server_echo": None, "started_at": datetime.now(timezone.utc).isoformat()}
        self._guard = guard
        if self._transport_state["dirty"]:
            self._invalid("Dividend connection is dirty; require an explicit fresh connection")
        try:
            result = super().__getattr__("query_dividend_data")(**request)
        except Exception as exc:
            self._invalid("Dividend SDK request failed: " + type(exc).__name__)
        result._on_guard, result._guard = self._on_guard, guard
        result._transport_state = self._transport_state
        echo = {key: str(getattr(result, key, "")) for key in request}
        guard["server_echo"] = echo
        guard["server_error_code"] = str(result.error_code)
        if str(result.error_code) != "0":
            result._invalid("Dividend SDK server returned an unsuccessful response")
        if echo != request:
            result._invalid("Dividend server code/year/yearType echo differs from request")
        guard["status"] = "server_echo_verified_pending_row_iteration"
        return result

    def next(self):
        try:
            available = super().__getattr__("next")()
        except Exception as exc:
            self._invalid("Dividend result iteration failed: " + type(exc).__name__)
        if not available and self._guard is not None:
            from baostock.common.contants import BAOSTOCK_PER_PAGE_COUNT

            page = getattr(self._source, "data", None)
            if (page is not None and len(page) == BAOSTOCK_PER_PAGE_COUNT
                    and getattr(self._source, "cur_row_num", 0) >= len(page)):
                self._invalid("Dividend pagination has no confirmed terminal page")
            if str(self.error_code) != "0":
                self._invalid("Dividend result iteration returned a source error")
            self._guard.update(status="verified", complete_row_iteration_observed=True,
                               finished_at=datetime.now(timezone.utc).isoformat())
            if self._on_guard is not None:
                self._on_guard(self._guard)
        return available

    def get_row_data(self):
        row = super().__getattr__("get_row_data")()
        if self._guard is None:
            return row
        if len(row) != len(self.fields):
            self._invalid("Dividend row fields differ from server schema")
        observed = dict(zip(self.fields, row))
        expected = self._guard["request"]
        if observed.get("code") != expected["code"]:
            self._invalid("Dividend row security differs from request")
        day = str(observed.get("dividOperateDate", "")).strip()
        if day:
            try:
                actual = date.fromisoformat(day)
            except ValueError:
                self._invalid("Dividend implemented row has an invalid ex-date")
            if str(actual.year) != expected["year"]:
                self._invalid("Dividend implemented row ex-date year differs from request")
        self._guard["rows_verified"] += 1
        return row
