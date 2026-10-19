"""Explicit, read-only model catalog checks; never generate text or echo credentials."""
from datetime import datetime, timezone
import re
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends
import requests

from stopat30m.auth.deps import require_role
from stopat30m.config import get

router = APIRouter(prefix="/system", dependencies=[Depends(require_role("admin"))])
_MAX_BYTES = 1024 * 1024


def _model_catalog(base, key):
    headers = {"Accept": "application/json"}
    if key:
        headers["Authorization"] = f"Bearer {key}"
    with requests.get(base.rstrip("/") + "/models", headers=headers, timeout=(5, 10),
                      allow_redirects=False, stream=True) as response:
        if response.status_code != 200:
            return response.status_code, None
        body = bytearray()
        for chunk in response.iter_content(16384):
            body.extend(chunk)
            if len(body) > _MAX_BYTES:
                return 200, None
        import json
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeError):
            return 200, None
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        return 200, None
    models = []
    for item in payload["data"]:
        model = item.get("id") if isinstance(item, dict) else None
        if (isinstance(model, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}", model)
                and not model.startswith("sk-") and (not key or key not in model)):
            models.append(model)
    return 200, sorted(set(models))


@router.get("/llm/diagnostics")
def model_diagnostics():
    cfg = get("llm") or {}
    result = {"checked_at": datetime.now(timezone.utc).isoformat(), "model": str(cfg.get("model") or ""),
              "api_type": cfg.get("api_type", "chat_completions"), "models": [],
              "model_listed": None, "generation_tested": False}
    def respond(status, message):
        return {**result, "status": status, "message": message}
    from stopat30m.analysis.llm_transport import llm_effective_provider, llm_request_options
    try:
        provider = llm_effective_provider(cfg)
        result["provider"] = provider
        if provider not in {"openai", "aihubmix"}:
            return respond("unsupported", "当前只检测 OpenAI 兼容服务的模型列表，不代表其他服务配置错误。")
        options = llm_request_options(cfg)
        base, key = options.get("api_base") or "https://api.openai.com/v1", options.get("api_key") or ""
        parts = urlsplit(base)
        if (parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password
                or parts.query or parts.fragment):
            return respond("unavailable", "服务基础地址无效，请在系统设置核对。")
        _ = parts.port
        status, models = _model_catalog(base, key)
    except requests.RequestException:
        return respond("unavailable", "模型列表连接失败或超时，请核对服务地址和网络。未发送生成请求。")
    except (TypeError, ValueError):
        return respond("unavailable", "模型服务配置无效，请核对模型、接口协议和基础地址。")
    if status in {401, 403}:
        return respond("unavailable", f"模型服务拒绝模型列表访问（HTTP {status}），请核对凭据及权限。这不是网站登录失效。")
    if status in {404, 405}:
        return respond("unsupported", "服务未提供模型列表接口；无法据此判断生成接口是否可用。")
    if status != 200 or models is None:
        return respond("unavailable", f"未取得有效模型列表（HTTP {status}）；未执行生成测试。")
    bare = options["model"].removeprefix("openai/").removeprefix("responses/")
    result["model_listed"] = bare in models
    result["models"] = models[:256]
    if result["model_listed"] and bare not in result["models"]:
        result["models"][-1] = bare
    result["models_truncated"] = len(models) > len(result["models"])
    if result["model_listed"]:
        return respond("ok", "当前模型在服务商列表中；这不代表生成接口、额度或参数已经验证。")
    return respond("warning", "当前模型不在服务商列表中，请选择服务商支持的模型；未自动修改配置或请求生成。")
