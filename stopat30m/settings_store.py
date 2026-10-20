"""Validated local settings with atomic audit, optimistic revisions and no secret echoes."""
from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import math
import os
import secrets
import tempfile
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import yaml


class SettingsError(ValueError):
    def __init__(self, message, status_code=400):
        super().__init__(message)
        self.status_code = status_code


LEGACY_ALLOWED_UNIVERSES = ("csi300", "csi1000")


def validate_legacy_universe(value, *, require_audited_samples=False):
    """Restrict legacy entry points without relabelling a CSI300 validation."""
    if not isinstance(value, str) or value not in LEGACY_ALLOWED_UNIVERSES:
        raise SettingsError("个人系统股票池仅支持 csi300（沪深300）和 csi1000（中证1000）。")
    if require_audited_samples and value == "csi1000":
        raise SettingsError("旧训练、预测和回测入口暂不支持 csi1000：请先完成已审计历史样本、独立模型和含费验证；不会自动改用 csi300。", 409)
    return value


def validate_legacy_prediction_scope(metadata, universe):
    """Require a declared matching pool before starting a new legacy backtest."""
    if not isinstance(metadata, dict) or not metadata.get("universe"):
        raise SettingsError("缓存预测缺少明确的股票池来源，不能用于新的限池回测；已有缓存和历史结果保持不变。")
    actual = validate_legacy_universe(metadata["universe"], require_audited_samples=True)
    if actual != universe:
        raise SettingsError("缓存预测股票池与本次请求不一致；不会静默更换股票池。")


_GROUPS = {
    "research": ("研究下载偏好", "仅影响后续下载任务；不改动已锁协议、策略和现有产物。"),
    "llm": ("AI 分析", "已有请求保持原参数，新实例使用保存值；凭据和服务地址需重启进程。"),
    "search": ("新闻搜索", "后续搜索实例使用保存值；无可用新闻时明确披露缺失，不自动轮换公共实例。"),
    "backtest": ("传统回测默认值", "后续传统回测默认值，不影响已锁研究协议。"),
    "signal": ("信号与诊断默认值", "只影响后续生成的信号和诊断任务。"),
    "account": ("账户回测默认值", "只影响新建账户回测，不修改手工持仓和成交记录。"),
    "risk": ("模拟引擎风险参数", "仅新模拟引擎/风险管理器实例生效；不是手工成交账本的拦截规则。"),
}
_FIELDS = {}


def _field(key, group, label, kind, default, low=None, high=None, options=None, description="", unit=None):
    entry = {"key": key, "group": group, "label": label, "type": kind, "default": default,
        "description": description, "apply_scope": "新实例/后续任务生效，已有请求保持原参数"}
    if low is not None:
        entry["min"] = low
    if high is not None:
        entry["max"] = high
    if options is not None:
        entry["options"] = [{"value": value, "label": value} for value in options]
    if unit:
        entry["unit"] = unit
    _FIELDS[key] = entry


_field("research.min_interval", "research", "最小请求间隔", "number", 3.0, 2, 60, unit="秒")
_field("research.max_attempts", "research", "最多尝试次数", "integer", 4, 1, 6)
_field("research.request_timeout", "research", "单次请求超时", "number", 15.0, 1, 60, unit="秒")
_field("llm.enabled", "llm", "启用 AI 分析", "boolean", True)
_field("llm.provider", "llm", "服务商", "enum", "auto", options=["auto", "openai", "anthropic", "deepseek", "gemini", "ollama", "aihubmix"],
       description="保存后使用所选服务商的独立配置；auto 兼容按模型前缀识别的已有设置。")
_field("llm.model", "llm", "模型标识", "string", "deepseek/deepseek-chat", description="使用现有 LiteLLM 模型标识，不会自动调用或测试模型。")
_field("llm.api_type", "llm", "接口协议", "enum", "chat_completions", options=["chat_completions", "responses"],
       description="Chat Completions 使用 /chat/completions，Responses 使用 /responses。按服务商实际支持选择；基础地址只填接口前缀，不要带这两个端点。")
_FIELDS["llm.api_type"]["options"] = [
    {"value": "chat_completions", "label": "Chat Completions（/chat/completions）"},
    {"value": "responses", "label": "Responses（/responses）"},
]
_field("llm.temperature", "llm", "温度", "number", .3, 0, 2)
_field("llm.max_tokens", "llm", "最大输出 token", "integer", 4000, 1, 200000)
_field("llm.timeout", "llm", "请求超时", "number", 90, 1, 600, unit="秒")
_field("llm.num_retries", "llm", "网络重试次数", "integer", 2, 0, 6)
_field("llm.integrity_retries", "llm", "完整性重试次数", "integer", 1, 0, 3)
for provider in ("aihubmix", "deepseek", "openai", "gemini", "anthropic"):
    _field(f"llm.{provider}_api_key", "llm", f"{provider} API key", "secret", "",
           description="空输入保留；勾选清除才删除。密钥不返回浏览器，保存后重启服务。")
for key, label in (("deepseek_base_url", "DeepSeek 服务地址"), ("openai_base_url", "OpenAI 兼容服务地址"), ("anthropic_base_url", "Claude 服务地址"), ("ollama_api_base", "Ollama 服务地址")):
    _field(f"llm.{key}", "llm", label, "string", "", description="仅 http/https 地址，不支持在地址中携带用户名、密码或查询凭据；保存后重启服务。")
_FIELDS["llm.anthropic_base_url"]["description"] = "官方服务留空；兼容服务填写根地址或 /v1 前缀，不要填写 /v1/messages。"
for provider, label in (("bocha", "博查"), ("tavily", "Tavily"), ("serpapi", "SerpAPI"),
                        ("anspire", "Anspire"), ("brave", "Brave"), ("minimax", "MiniMax")):
    key = "serpapi_keys" if provider == "serpapi" else f"{provider}_api_keys"
    _field(f"search.{key}", "search", f"{label} 搜索密钥", "secret", "",
           description="多个密钥用逗号分隔。留空保留，显式勾选清除；请在服务商确认可用额度。")
_field("search.searxng_base_urls", "search", "自建 SearXNG 地址", "string", "",
       description="一个或逗号分隔的自建地址，需启用 JSON 输出；不支持 URL 凭据、查询参数或片段。")
_field("search.searxng_public_instances_enabled", "search", "允许公共 SearXNG 发现", "boolean", False,
       description="默认关闭；公共实例可能拒绝访问或要求浏览器验证，不保证可用性。")
_field("search.news_strategy_profile", "search", "新闻时间窗口", "enum", "short", options=["short", "medium", "long"],
       description="short / medium / long 分别查询近 3 / 7 / 14 天。")
for section, group in (("backtest", "backtest"), ("signal", "signal"), ("signal_backtest", "signal"), ("account_backtest", "account")):
    _field(f"{section}.top_k", group, f"{section} 选股数量", "integer", 10, 1, 1000)
    _field(f"{section}.rebalance_freq", group, f"{section} 调仓间隔", "integer", 5, 1, 252, unit="交易日")
for key, default in (("buy_cost", .0003), ("sell_cost", .0013)):
    _field(f"backtest.{key}", "backtest", "买入费率" if key == "buy_cost" else "卖出费率", "number", default, 0, .1, unit="比例")
_field("backtest.deal_price", "backtest", "执行价格", "enum", "open", options=["open", "close"])
for section, group in (("backtest", "backtest"), ("signal_backtest", "signal"), ("account_backtest", "account")):
    _field(f"{section}.benchmark", group, f"{section} 基准", "string", "SH000300")
_field("signal_backtest.group_count", "signal", "分组诊断数量", "integer", 10, 2, 100)
_field("account_backtest.initial_capital", "account", "初始资金", "number", 1000000, 1, 1e12, unit="元")
_field("account_backtest.execution_price", "account", "执行价格", "enum", "open", options=["open", "close"])
_field("account_backtest.order_type", "account", "订单类型", "enum", "market", options=["market", "limit"])
_field("account_backtest.slippage_bps", "account", "滑点", "number", 5, 0, 1000, unit="bp")
_field("account_backtest.allow_partial_fill", "account", "允许部分成交", "boolean", True)
_field("account_backtest.participation_rate", "account", "成交量参与率", "number", .1, .000001, 1, unit="比例")
_field("account_backtest.cash_reserve_pct", "account", "现金保留比例", "number", .02, 0, .99, unit="比例")
_field("account_backtest.enable_risk_manager", "account", "启用回测风险管理器", "boolean", True)
for key, label, default in (("max_drawdown", "最大回撤", .15), ("max_daily_loss", "单日最大亏损", .03),
        ("max_single_loss", "单笔最大亏损", .05), ("max_concentration", "单标的集中度", .1), ("circuit_breaker_loss", "熔断亏损阈值", .08)):
    _field(f"risk.{key}", "risk", label, "number", default, .000001, 1, unit="比例")
for key, label, default in (("max_position_pct", "单笔仓位上限", .05), ("max_total_position_pct", "总仓位上限", .95),
        ("stop_loss_pct", "止损比例", .08), ("take_profit_pct", "止盈比例", .15)):
    _field(f"trading.{key}", "risk", label, "number", default, .000001, 1, unit="比例")

_SECRET_KEYS = {key for key, field in _FIELDS.items() if field["type"] == "secret"}
_URL_KEYS = {"llm.deepseek_base_url", "llm.openai_base_url", "llm.anthropic_base_url", "llm.ollama_api_base"}
_RESTART_KEYS = {key for key in _SECRET_KEYS | _URL_KEYS if key.startswith("llm.")}
_ENV_ALIASES = {"llm.deepseek_api_key": "DEEPSEEK_API_KEY", "llm.openai_api_key": "OPENAI_API_KEY",
    "llm.gemini_api_key": "GEMINI_API_KEY", "llm.anthropic_api_key": "ANTHROPIC_API_KEY",
    "llm.aihubmix_api_key": "AIHUBMIX_KEY", "llm.deepseek_base_url": "DEEPSEEK_BASE_URL",
    "llm.openai_base_url": "OPENAI_BASE_URL", "llm.anthropic_base_url": "ANTHROPIC_BASE_URL", "llm.ollama_api_base": "OLLAMA_API_BASE"}
_LEGACY_SECTIONS = {"model", "factors", "data", "backtest", "signal", "signal_backtest", "account_backtest"}
_PROCESS_ID = uuid4().hex


def _get(mapping, dotted, default=None):
    current = mapping
    for key in dotted.split("."):
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _set(mapping, dotted, value):
    keys = dotted.split(".")
    current = mapping
    for key in keys[:-1]:
        if key in current and not isinstance(current[key], dict):
            raise SettingsError("Existing local configuration has an incompatible section shape.", 409)
        current = current.setdefault(key, {})
    current[keys[-1]] = value


def _merge(base, override):
    result = copy.deepcopy(base)
    for key, value in override.items():
        result[key] = _merge(result[key], value) if isinstance(result.get(key), dict) and isinstance(value, dict) else copy.deepcopy(value)
    return result


def _yaml(path):
    if not path.exists():
        return {}
    if path.is_symlink():
        raise SettingsError("Configuration symlinks are not accepted.", 409)
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (yaml.YAMLError, UnicodeError, OSError):
        raise SettingsError("Configuration could not be read safely; no changes were saved.", 409) from None
    if not isinstance(value, dict):
        raise SettingsError("Configuration must be a mapping.", 409)
    return value


def _validate(key, value):
    field = _FIELDS[key]
    kind = field["type"]
    if kind == "boolean":
        valid = isinstance(value, bool)
    elif kind == "integer":
        valid = isinstance(value, int) and not isinstance(value, bool)
    elif kind == "number":
        valid = isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    else:
        valid = isinstance(value, str)
    if not valid:
        raise SettingsError(f"Invalid value type for {key}.")
    if kind in {"integer", "number"} and not field.get("min", -math.inf) <= value <= field.get("max", math.inf):
        raise SettingsError(f"Value outside the permitted range for {key}.")
    if kind == "enum" and value not in {option["value"] for option in field["options"]}:
        raise SettingsError(f"Unsupported option for {key}.")
    if isinstance(value, str):
        if len(value) > (8192 if kind == "secret" else 500) or any(ord(char) < 32 for char in value):
            raise SettingsError(f"Invalid string format for {key}.")
        if kind == "secret" and value and not value.strip():
            raise SettingsError(f"A secret cannot contain only whitespace: {key}.")
        if kind != "secret":
            value = value.strip()
        if (key.endswith(".benchmark") or key == "llm.model") and not value:
            raise SettingsError(f"A nonempty value is required for {key}.")
    if key in _URL_KEYS and value:
        try:
            parts = urlsplit(value)
            if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password or parts.query or parts.fragment:
                raise ValueError
            _ = parts.port
        except ValueError:
            raise SettingsError(f"Invalid service URL for {key}; credentials in URLs are not accepted.") from None
        if key == "llm.openai_base_url" and parts.path.rstrip("/").endswith(("/chat/completions", "/responses")):
            raise SettingsError("OpenAI base URL must omit /chat/completions and /responses endpoints.")
        if key == "llm.anthropic_base_url" and parts.path.rstrip("/").endswith("/messages"):
            raise SettingsError("Claude base URL must omit /v1/messages endpoint.")
    if key == "search.searxng_base_urls" and value:
        try:
            for base in value.split(","):
                parts = urlsplit(base.strip())
                if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password or parts.query or parts.fragment:
                    raise ValueError
                _ = parts.port
        except ValueError:
            raise SettingsError("Use http/https SearXNG base URLs without credentials or query parameters.") from None
    if key.endswith(".benchmark"):
        import re
        if not re.fullmatch(r"(?:SH|SZ)[0-9]{6}", value):
            raise SettingsError(f"Use a canonical SH/SZ benchmark code for {key}.")
    return value


class SettingsStore:
    def __init__(self, config_path=None, *, environ=None):
        from stopat30m.config import _DEFAULT_CONFIG_PATH
        self.base_path = Path(config_path or _DEFAULT_CONFIG_PATH).expanduser().absolute()
        self.local_path = self.base_path.with_name(f"{self.base_path.stem}.local{self.base_path.suffix}")
        self.lock_path = self.base_path.with_name(f".{self.base_path.stem}.settings.lock")
        self.environ = os.environ if environ is None else environ

    @contextmanager
    def _lock(self):
        if self.lock_path.is_symlink():
            raise SettingsError("Configuration lock symlinks are not accepted.", 409)
        descriptor = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        with os.fdopen(descriptor, "a+b") as handle:
            os.fchmod(handle.fileno(), 0o600)
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _env_name(self, key):
        name = "STOPAT30M_" + key.replace(".", "__").upper()
        if name in self.environ:
            return name
        alias = _ENV_ALIASES.get(key)
        return alias if alias and self.environ.get(alias) else None

    def _effective(self, base, local, key):
        field = _FIELDS[key]
        merged = _merge(base, local)
        value = _get(merged, key, field["default"])
        source = "local" if _get(local, key, _MISSING) is not _MISSING else "default"
        env = self._env_name(key)
        if env:
            source = "environment"
            raw = self.environ[env]
            try:
                if field["type"] == "boolean":
                    if str(raw).lower() not in {"true", "false", "1", "0", "yes", "no"}:
                        raise ValueError
                    value = str(raw).lower() in {"true", "1", "yes"}
                elif field["type"] == "integer":
                    value = int(raw)
                elif field["type"] == "number":
                    value = float(raw)
                else:
                    value = str(raw)
            except (ValueError, TypeError):
                raise SettingsError(f"Environment override is invalid for {key}.", 409) from None
        try:
            return _validate(key, value), source
        except SettingsError:
            raise SettingsError(f"Effective configuration is invalid for {key}; no values are included in this error.", 409) from None

    def _revision(self, base, local):
        values = {"base": base, "local": {key: value for key, value in local.items() if key != "_settings_store"},
            "environment": {key: value for key, value in self.environ.items()
                if key.startswith("STOPAT30M_") or key in _ENV_ALIASES.values()}}
        try:
            encoded = json.dumps(values, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False).encode()
        except (TypeError, ValueError):
            raise SettingsError("Configuration contains unsupported nonfinite values.", 409) from None
        return hashlib.sha256(encoded).hexdigest()

    def _view(self, base, local):
        groups = {key: {"id": key, "title": title, "description": description, "fields": []}
                  for key, (title, description) in _GROUPS.items()}
        for key, field in _FIELDS.items():
            value, source = self._effective(base, local, key)
            item = {name: copy.deepcopy(val) for name, val in field.items() if name not in {"default", "group"}}
            item.update(value=None if key in _SECRET_KEYS else value, source=source, read_only=source == "environment")
            if key in _SECRET_KEYS:
                item["configured"] = bool(value.strip())
            groups[field["group"]]["fields"].append(item)
        metadata = local.get("_settings_store", {})
        restart = metadata.get("restart_required", []) if isinstance(metadata, dict) and metadata.get("restart_process_id") == _PROCESS_ID else []
        return {"revision": self._revision(base, local), "groups": list(groups.values()),
            "restart_required": [key for key in restart if key in _RESTART_KEYS]}

    def get(self):
        with self._lock():
            return self._view(_yaml(self.base_path), _yaml(self.local_path))

    def _cross_validate(self, base, local):
        values = {key: self._effective(base, local, key)[0] for key in _FIELDS}
        model, provider = values["llm.model"], values["llm.provider"]
        if model.endswith("/"):
            raise SettingsError("A model name is required after the provider prefix.")
        prefix = model.split("/", 1)[0] if "/" in model else None
        known = {"openai", "anthropic", "deepseek", "gemini", "ollama", "ollama_chat", "aihubmix"}
        allowed_prefix = {"openai": {"openai"}, "aihubmix": {"openai", "aihubmix"}, "anthropic": {"anthropic"},
                          "deepseek": {"deepseek"}, "gemini": {"gemini"}, "ollama": {"ollama", "ollama_chat"}}
        if provider != "auto" and prefix in known and prefix not in allowed_prefix[provider]:
            raise SettingsError("Selected provider and model prefix do not match.")
        compatible = (provider in {"openai", "aihubmix"} if provider != "auto" else prefix in {None, "openai", "aihubmix"})
        if values["llm.api_type"] == "responses" and not compatible:
            raise SettingsError("Responses protocol requires an OpenAI-compatible model identifier.")
        if not values["trading.max_position_pct"] <= values["risk.max_concentration"] <= values["trading.max_total_position_pct"]:
            raise SettingsError("Require per-order position <= concentration <= total position limit.")
        if not values["risk.max_daily_loss"] <= values["risk.circuit_breaker_loss"] <= values["risk.max_drawdown"]:
            raise SettingsError("Require daily loss <= circuit-breaker loss <= maximum drawdown.")
        merged = _merge(base, local)
        dates = [self.environ.get(f"STOPAT30M_DATA__{key.upper()}", _get(merged, f"data.{key}"))
                 for key in ("train_start", "train_end", "valid_start", "valid_end", "test_start")]
        if all(isinstance(value, str) and value for value in dates):
            try:
                parsed = [date.fromisoformat(value) for value in dates]
            except ValueError:
                raise SettingsError("Legacy development split dates must use YYYY-MM-DD.") from None
            if not parsed[0] <= parsed[1] < parsed[2] <= parsed[3] < parsed[4]:
                raise SettingsError("Legacy training, validation and test periods must be ordered and non-overlapping.")

    def _commit(self, base, local, changed, *, user_id, username, secret_keys=()):
        revision = self._revision(base, local)
        metadata = local.setdefault("_settings_store", {})
        if not isinstance(metadata, dict):
            raise SettingsError("Local settings metadata is invalid.", 409)
        audit = metadata.get("audit", [])
        if not isinstance(audit, list):
            raise SettingsError("Local settings audit metadata is invalid.", 409)
        entry = {"id": uuid4().hex, "at": datetime.now(timezone.utc).isoformat(), "user_id": user_id,
            "username": str(username), "changed_keys": sorted(changed), "secret_keys": sorted(secret_keys), "revision": revision}
        metadata["audit"] = [*audit[-49:], entry]
        pending = set(metadata.get("restart_required", [])) if metadata.get("restart_process_id") == _PROCESS_ID else set()
        metadata["restart_required"] = sorted(pending | (set(changed) & _RESTART_KEYS))
        metadata["restart_process_id"] = _PROCESS_ID
        if self.local_path.is_symlink():
            raise SettingsError("Local configuration symlinks are not accepted.", 409)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(prefix=".settings-", dir=self.local_path.parent, delete=False) as handle:
                temporary = Path(handle.name)
                os.fchmod(handle.fileno(), 0o600)
                handle.write(yaml.safe_dump(local, allow_unicode=True, sort_keys=False).encode())
                handle.flush()
                os.fsync(handle.fileno())
            temporary.replace(self.local_path)
        except (OSError, yaml.YAMLError):
            raise SettingsError("Local configuration could not be saved; no secret values are included in this error.", 409) from None
        finally:
            if temporary:
                temporary.unlink(missing_ok=True)
        import stopat30m.config as config_mod
        config_mod._config_cache = None
        if any(key.startswith("search.") for key in changed):
            from stopat30m.analysis.search_service import reset_search_service
            reset_search_service()
        return revision

    def update(self, revision, values, clear_secrets=None, *, user_id=None, username=""):
        clear_secrets = [] if clear_secrets is None else clear_secrets
        if not isinstance(revision, str) or not isinstance(values, dict) or not isinstance(clear_secrets, list):
            raise SettingsError("Expected revision, values object and clear_secrets list.")
        if any(not isinstance(key, str) or key not in _FIELDS for key in values):
            raise SettingsError("Unknown settings key; no submitted values are included in this error.")
        if any(not isinstance(key, str) or key not in _SECRET_KEYS for key in clear_secrets):
            raise SettingsError("Only registered secret fields may be explicitly cleared.")
        with self._lock():
            base, original = _yaml(self.base_path), _yaml(self.local_path)
            if revision != self._revision(base, original):
                raise SettingsError("Settings changed since this page loaded; reload before saving.", 409)
            local, changed = copy.deepcopy(original), []
            for key in set(values) | set(clear_secrets):
                if self._env_name(key):
                    raise SettingsError(f"Environment-controlled field is read-only: {key}.", 409)
                value = "" if key in clear_secrets else _validate(key, values[key])
                if key in _SECRET_KEYS and key not in clear_secrets and value == "":
                    continue
                if _get(local, key, _MISSING) != value:
                    _set(local, key, value)
                    changed.append(key)
            self._cross_validate(base, local)
            if changed:
                self._commit(base, local, changed, user_id=user_id, username=username, secret_keys=set(changed) & _SECRET_KEYS)
            result = self._view(base, local)
            return {**result, "changed_keys": sorted(changed)}

    def update_legacy(self, payload, *, user_id=None, username=""):
        """Preserve the old model-lab section API without a second public-file writer."""
        if not isinstance(payload, dict) or not set(payload).issubset(_LEGACY_SECTIONS):
            raise SettingsError("Unsupported legacy model configuration section.")
        with self._lock():
            base, original = _yaml(self.base_path), _yaml(self.local_path)
            merged = _merge(base, original)
            local, changed = copy.deepcopy(original), []
            def merge_fields(overrides, reference, prefix):
                if not isinstance(overrides, dict) or not isinstance(reference, dict):
                    raise SettingsError("Legacy configuration sections must be objects.")
                for name, value in overrides.items():
                    if not isinstance(name, str):
                        raise SettingsError("Unknown legacy configuration key.")
                    key = f"{prefix}.{name}"
                    if name not in reference and key != "data.universe":
                        raise SettingsError("Unknown legacy configuration key.")
                    if any(word in name.lower() for word in ("api_key", "password", "secret", "token")):
                        raise SettingsError("Credentials cannot be edited through the legacy model interface.")
                    reference_value = reference.get(name, "csi300")
                    if isinstance(reference_value, dict):
                        merge_fields(value, reference_value, key)
                        continue
                    if self._env_name(key):
                        raise SettingsError(f"Environment-controlled field is read-only: {key}.", 409)
                    if key == "data.universe":
                        value = validate_legacy_universe(value)
                    elif key in _FIELDS:
                        value = _validate(key, value)
                    else:
                        old = reference_value
                        if isinstance(old, bool):
                            valid = isinstance(value, bool)
                        elif isinstance(old, int):
                            valid = isinstance(value, int) and not isinstance(value, bool)
                        elif isinstance(old, float):
                            valid = isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
                        elif isinstance(old, str):
                            valid = isinstance(value, str) and len(value) <= 2048 and "\x00" not in value
                        elif isinstance(old, list):
                            valid = isinstance(value, list) and len(value) <= 100 and all(isinstance(item, int) and not isinstance(item, bool) and 1 <= item <= 252 for item in value)
                        else:
                            valid = False
                        if not valid:
                            raise SettingsError(f"Invalid legacy value type or range for {key}.")
                        if key.endswith(".method") and value != "top_k":
                            raise SettingsError("Only top_k is supported by the current legacy backtest interface.")
                    if _get(local, key, _MISSING) != value:
                        _set(local, key, value)
                        changed.append(key)
            for section, values in payload.items():
                merge_fields(values, merged.get(section, {}), section)
            self._cross_validate(base, local)
            if changed:
                self._commit(base, local, changed, user_id=user_id, username=username)
            return {"status": "ok", "updated_sections": ", ".join(sorted(payload))}

    def audit(self):
        with self._lock():
            metadata = _yaml(self.local_path).get("_settings_store", {})
            audit = metadata.get("audit", []) if isinstance(metadata, dict) else []
            if not isinstance(audit, list):
                raise SettingsError("Local audit metadata is invalid.", 409)
            import re
            allowed = {"id", "at", "user_id", "username", "changed_keys", "secret_keys", "revision"}
            result = []
            for entry in reversed(audit[-50:]):
                if (not isinstance(entry, dict) or set(entry) != allowed
                        or not re.fullmatch(r"[0-9a-f]{32}", str(entry.get("id", "")))
                        or not re.fullmatch(r"[0-9a-f]{64}", str(entry.get("revision", "")))
                        or not isinstance(entry.get("username"), str)
                        or not isinstance(entry.get("changed_keys"), list) or not isinstance(entry.get("secret_keys"), list)
                        or any(not isinstance(key, str) or not re.fullmatch(r"[a-zA-Z0-9_.]+", key) for key in entry["changed_keys"])
                        or any(key not in _SECRET_KEYS | {"auth.secret_key"} for key in entry["secret_keys"])):
                    raise SettingsError("Local settings audit record is invalid; no record contents were disclosed.", 409)
                result.append({key: copy.deepcopy(entry[key]) for key in allowed})
            return result

    def ensure_auth_secret(self):
        with self._lock():
            base, local = _yaml(self.base_path), _yaml(self.local_path)
            env = self.environ.get("STOPAT30M_AUTH__SECRET_KEY")
            actual = env if env is not None else _get(_merge(base, local), "auth.secret_key", "")
            if actual and str(actual).strip():
                return False
            if env is not None:
                raise SettingsError("The auth secret environment override is empty; remove or configure it before initialization.", 409)
            _set(local, "auth.secret_key", secrets.token_urlsafe(48))
            self._commit(base, local, ["auth.secret_key"], user_id=None, username="bootstrap", secret_keys=["auth.secret_key"])
            return True


_MISSING = object()


def ensure_auth_secret():
    """Generate an absent auth secret under the same local settings lock; never return it."""
    return SettingsStore().ensure_auth_secret()


def research_preferences():
    store = SettingsStore()
    with store._lock():
        base, local = _yaml(store.base_path), _yaml(store.local_path)
        return {key: store._effective(base, local, f"research.{key}")[0]
                for key in ("min_interval", "max_attempts", "request_timeout")}
