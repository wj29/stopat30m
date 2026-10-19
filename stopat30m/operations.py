"""Consistent local SQLite exports; configuration credentials are never copied."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import tempfile
from urllib.parse import quote
from uuid import uuid4
import zipfile

from stopat30m.config import PROJECT_ROOT

_ID = re.compile(r"backup_\d{8}_\d{6}_[0-9a-f]{8}\Z")


class BackupError(ValueError):
    pass


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class BackupStore:
    def __init__(self, root=None):
        self.root = Path(root or PROJECT_ROOT / "output/backups").expanduser().absolute()

    def _guard(self, path):
        path = Path(path).absolute()
        if not path.is_relative_to(self.root):
            raise BackupError("Invalid backup path.")
        for current in (path, *path.parents):
            if current.is_symlink():
                raise BackupError("Backup paths cannot contain symlinks.")
        return path

    @contextmanager
    def _lock(self):
        self._guard(self.root).mkdir(parents=True, exist_ok=True)
        with self._guard(self.root / ".lock").open("a+b") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise BackupError("正在创建另一份备份，请稍后重试。") from None
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def create(self, database_path: str | Path) -> dict:
        source = Path(database_path).expanduser().resolve()
        if not source.is_file():
            raise BackupError("账户数据库尚未创建。")
        with self._lock():
            now = datetime.now(timezone.utc)
            identifier = f"backup_{now.strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
            with tempfile.TemporaryDirectory(prefix=".building-", dir=self.root) as temporary:
                stage = Path(temporary) / "bundle"
                stage.mkdir(mode=0o700)
                snapshot = stage / "account.sqlite3"
                # SQLite's backup API includes committed WAL contents and never
                # copies an inconsistent database/WAL pair as ordinary files.
                with sqlite3.connect(f"file:{quote(str(source), safe='/')}?mode=ro", uri=True, timeout=5) as src:
                    with sqlite3.connect(snapshot) as destination:
                        src.backup(destination)
                        if destination.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                            raise BackupError("数据库一致性检查未通过，未发布备份。")
                snapshot.chmod(0o600)
                manifest = {"schema_version": 1, "id": identifier, "created_at": now.isoformat(),
                    "database_sha256": _sha(snapshot), "database_bytes": snapshot.stat().st_size,
                    "scope": "Full SQLite application database, including ledgers, authentication hashes, analysis and chat history",
                    "excluded": ["configuration files", "config.local.yaml", "market data", "models", "research artifacts"],
                    "restore_mode": "offline_review_required"}
                content = json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
                archive = stage / "archive.zip"
                with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
                    bundle.write(snapshot, "account.sqlite3")
                    bundle.writestr("manifest.json", content)
                archive.chmod(0o600)
                snapshot.unlink()
                metadata = {**manifest, "archive_sha256": _sha(archive), "archive_bytes": archive.stat().st_size}
                (stage / "manifest.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
                (stage / "manifest.json").chmod(0o600)
                stage.rename(self.root / identifier)
            return self.verify(identifier)

    def verify(self, identifier: str) -> dict:
        if not _ID.fullmatch(identifier):
            raise BackupError("Invalid backup identifier.")
        directory = self._guard(self.root / identifier)
        try:
            manifest = json.loads(self._guard(directory / "manifest.json").read_text())
            archive = self._guard(directory / "archive.zip")
            if (manifest["id"] != identifier or manifest["schema_version"] != 1
                    or archive.stat().st_size != manifest["archive_bytes"] or _sha(archive) != manifest["archive_sha256"]):
                raise BackupError("备份文件校验和不匹配。")
            with zipfile.ZipFile(archive) as bundle:
                if sorted(bundle.namelist()) != ["account.sqlite3", "manifest.json"]:
                    raise BackupError("备份内容不符合预期。")
                inner = json.loads(bundle.read("manifest.json"))
                if inner != {key: value for key, value in manifest.items() if key not in {"archive_sha256", "archive_bytes"}}:
                    raise BackupError("备份清单不一致。")
                digest, size = hashlib.sha256(), 0
                with bundle.open("account.sqlite3") as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        size += len(block)
                        digest.update(block)
                if digest.hexdigest() != manifest["database_sha256"] or size != manifest["database_bytes"]:
                    raise BackupError("备份数据库校验失败。")
            return {**manifest, "validation_status": "valid", "validation_error": None}
        except (OSError, KeyError, TypeError, ValueError, RuntimeError, zipfile.BadZipFile) as exc:
            if isinstance(exc, BackupError):
                raise
            raise BackupError("备份文件无法读取或已损坏。") from None

    def list(self) -> list[dict]:
        if not self.root.exists():
            return []
        self._guard(self.root)
        rows = []
        for path in self.root.iterdir():
            if path.name.startswith("."):
                continue
            try:
                rows.append(self.verify(path.name))
            except BackupError as exc:
                rows.append({"id": path.name, "created_at": "", "archive_bytes": None,
                    "validation_status": "invalid", "validation_error": str(exc)})
        return sorted(rows, key=lambda row: (row["created_at"], row["id"]), reverse=True)

    def archive(self, identifier: str) -> Path:
        self.verify(identifier)
        return self._guard(self.root / identifier / "archive.zip")
