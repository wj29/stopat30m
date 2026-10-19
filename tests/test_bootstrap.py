"""Local first-use setup must be one-time, atomic and independent of a CLI."""
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stopat30m import config, settings_store
from stopat30m.api.deps import get_db_session
from stopat30m.api.v1.endpoints import auth, bootstrap
from stopat30m.auth import service
from stopat30m.storage.models import Base, User


@pytest.fixture
def installation(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text('auth:\n  single_user: true\n  secret_key: ""\n')
    monkeypatch.setattr(config, "_DEFAULT_CONFIG_PATH", path)
    monkeypatch.setattr(config, "_config_cache", None)
    monkeypatch.setattr(service, "_secret_key", None)
    monkeypatch.delenv("STOPAT30M_AUTH__SECRET_KEY", raising=False)
    engine = create_engine(f"sqlite:///{tmp_path / 'bootstrap.db'}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    app = FastAPI()
    app.include_router(auth.router, prefix="/api/v1")
    app.include_router(bootstrap.router, prefix="/api/v1")
    def sessions():
        with factory.begin() as db:
            yield db
    app.dependency_overrides[get_db_session] = sessions
    yield app, factory, path
    engine.dispose()


def local_client(app, **kwargs):
    return TestClient(app, base_url="http://127.0.0.1:8000", client=("127.0.0.1", 45000), **kwargs)


def test_browser_bootstrap_persists_signing_secret_and_closes_after_first_user(installation):
    app, factory, path = installation
    with local_client(app) as client:
        assert client.get("/api/v1/auth/bootstrap").json() == {"required": True, "allowed": True, "reason": None}
        response = client.post("/api/v1/auth/bootstrap", headers={"Origin": "http://127.0.0.1:8000"}, json={
            "username": "owner", "password": "offline-password",
        })
        assert response.status_code == 201, response.text
        with factory() as db:
            user = db.query(User).one()
            assert user.role == "admin" and user.username == "owner"
            assert service.verify_password("offline-password", user.password_hash)
        assert path.with_name("config.local.yaml").is_file()
        token = response.json()["access_token"]
        service._secret_key = None  # Recreated process reads the persisted key.
        config._config_cache = None
        assert service.decode_access_token(token)["username"] == "owner"
        assert client.post("/api/v1/auth/login", json={"username": "owner", "password": "offline-password"}).status_code == 200
        assert client.get("/api/v1/auth/bootstrap").json()["required"] is False
        assert client.post("/api/v1/auth/bootstrap", json={"username": "other", "password": "offline-password"}).status_code == 409


@pytest.mark.parametrize("peer,headers", [
    ("198.51.100.9", {}),
    ("198.51.100.9", {"X-Forwarded-For": "127.0.0.1"}),
    ("127.0.0.1", {"Host": "external.example"}),
    ("127.0.0.1", {"Origin": "https://other.example"}),
    ("127.0.0.1", {"Origin": "http://127.0.0.1:9000"}),
    ("127.0.0.1", {"Origin": "null"}),
    ("127.0.0.1", {"Sec-Fetch-Site": "cross-site"}),
    ("127.0.0.1", {"Forwarded": "for=127.0.0.1"}),
    ("127.0.0.1", {"X-Real-IP": "198.51.100.9"}),
    ("127.0.0.1", {"X-Forwarded-Port": "8000"}),
])
def test_bootstrap_rejects_remote_host_and_cross_origin_requests(installation, peer, headers):
    app, factory, path = installation
    with TestClient(app, base_url="http://127.0.0.1:8000", client=(peer, 45000)) as client:
        assert client.get("/api/v1/auth/bootstrap", headers=headers).json()["allowed"] is False
        result = client.post("/api/v1/auth/bootstrap", headers=headers, json={"username": "owner", "password": "offline-password"})
        assert result.status_code == 403
    with factory() as db:
        assert db.query(User).count() == 0
    assert not path.with_name("config.local.yaml").exists()


def test_bootstrap_rejects_invalid_password_without_echo_and_rolls_back_on_storage_failure(installation, monkeypatch):
    app, factory, _ = installation
    with local_client(app) as client:
        response = client.post("/api/v1/auth/bootstrap", json={"username": "owner", "password": "tiny-pass"})
        assert response.status_code == 400 and "tiny-pass" not in response.text
        def fail():
            raise OSError("private-config-detail")
        monkeypatch.setattr(settings_store, "ensure_auth_secret", fail)
        result = client.post("/api/v1/auth/bootstrap", json={"username": "owner", "password": "offline-password"})
        assert result.status_code == 503 and "private-config-detail" not in result.text
    with factory() as db:
        assert db.query(User).count() == 0


def test_two_local_bootstrap_requests_cannot_create_two_admins(installation):
    app, factory, _ = installation
    def create(name):
        with local_client(app) as client:
            return client.post("/api/v1/auth/bootstrap", json={"username": name, "password": "offline-password"}).status_code
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, ["owner-one", "owner-two"]))
    assert sorted(results) == [201, 409]
    with factory() as db:
        assert db.query(User).count() == 1
