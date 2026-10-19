"""Admin-only local readiness and consistent account database backups."""

from datetime import datetime, timezone
from functools import lru_cache
from importlib.util import find_spec
import time

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from stopat30m.api.deps import get_db_session
from stopat30m.auth.deps import get_current_user, require_role
from stopat30m.config import get
from stopat30m.operations import BackupError, BackupStore
from stopat30m.research_workspace import ResearchWorkspace
from stopat30m.storage.models import PortfolioAccount, User
from .research import get_workspace

router = APIRouter(prefix="/system", dependencies=[Depends(require_role("admin"))])
_STARTED = time.monotonic()


@lru_cache(maxsize=1)
def get_backup_store():
    return BackupStore()


def _backup_call(fn, *args):
    try:
        return fn(*args)
    except (BackupError, OSError, ValueError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None


@router.get("/operations")
def operations(db: Session = Depends(get_db_session), user: User = Depends(get_current_user),
               workspace: ResearchWorkspace = Depends(get_workspace), store: BackupStore = Depends(get_backup_store)):
    checks = []
    def add(key, label, ready, message, link):
        checks.append({"id": key, "label": label, "status": "ok" if ready else "pending", "message": message, "to": link})
    healthy = db.execute(text("PRAGMA quick_check")).fetchall() == [("ok",)]
    add("database", "账户数据库", healthy, "一致性检查通过。" if healthy else "一致性检查未通过，请保留原文件并核查。", "/operations")
    account = db.query(PortfolioAccount.id).filter_by(user_id=user.id).first() is not None
    add("account", "个人资金账户", account, "已建立个人账本。" if account else "先录入开户本金，再按时间顺序记账。", "/trading")
    stable_key = bool(str(get("auth", "secret_key", "") or "").strip())
    add("session", "持久登录签名", stable_key, "已配置持久签名。" if stable_key else "当前使用临时签名，重启后需要重新登录。", "/settings")
    try:
        ready = any(item.get("validation_status") == "valid" and "eastmoney_etf" in item["compatible_protocols"] for item in workspace.snapshots())
        add("research", "完整研究数据", ready, "有通过校验且匹配主协议的数据快照。" if ready else "尚无匹配主协议的完整数据；先在系统设置下载并核对。", "/settings")
    except (ValueError, OSError) as exc:
        add("research", "完整研究数据", False, f"无法确认研究目录：{exc}", "/settings")
    try:
        models = find_spec("qlib") is not None
    except (ValueError, ImportError):
        models = False
    add("models", "模型训练组件", models, "Qlib 已安装，训练仍需核对模型数据。" if models else "Qlib 未安装；当前固定规则研究与账户功能可以独立使用。", "/model-lab")
    missing = []
    for module in ("markdown2", "litellm", "json_repair", "akshare", "efinance"):
        try:
            installed = find_spec(module) is not None
        except (ValueError, ImportError):
            installed = False
        if not installed:
            missing.append(module)
    add("reports", "大盘复盘与报告组件", not missing,
        "组件已安装，行情连通性与 AI 服务凭据仍需实际核对。" if not missing else
        f"缺少 {', '.join(missing)}。请在项目环境安装 requirements-web.txt 后重启服务。", "/market-review")
    backups = _backup_call(store.list)
    valid = [row for row in backups if row["validation_status"] == "valid"]
    add("backup", "账户备份", bool(valid), f"最近备份：{valid[0]['created_at']}" if valid else "尚无通过校验的账户数据库备份。", "/operations")
    return {"checked_at": datetime.now(timezone.utc).isoformat(), "uptime_seconds": int(time.monotonic() - _STARTED),
            "checks": checks, "external_services_checked": False}


@router.get("/backups")
def backups(store: BackupStore = Depends(get_backup_store)):
    return _backup_call(store.list)


@router.post("/backups", status_code=201)
def create_backup(store: BackupStore = Depends(get_backup_store), db: Session = Depends(get_db_session)):
    path = db.get_bind().url.database
    if not path or path == ":memory:":
        raise HTTPException(status_code=409, detail="当前数据库不支持文件备份。")
    return _backup_call(store.create, path)


@router.get("/backups/{backup_id}/download")
def download_backup(backup_id: str, store: BackupStore = Depends(get_backup_store)):
    path = _backup_call(store.archive, backup_id)
    return FileResponse(path, filename=f"{backup_id}.zip", media_type="application/zip",
                        headers={"Cache-Control": "no-store"})
