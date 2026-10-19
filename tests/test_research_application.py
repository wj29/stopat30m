"""Exercise actual app routing, JWT roles and an isolated research workspace."""

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stopat30m.api.app import create_app
from stopat30m.api.v1.endpoints import research
from stopat30m.auth import service
from stopat30m.research_workspace import ResearchWorkspace
from stopat30m.storage import database
from stopat30m.storage.models import User


def test_research_pages_and_api_use_actual_login_and_admin_boundary(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'web.db'}", connect_args={"check_same_thread": False})
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr(database, "_engine", engine)
    monkeypatch.setattr(database, "_SessionFactory", factory)
    monkeypatch.setattr(service, "_secret_key", "offline-research-web-integration")
    workspace = ResearchWorkspace(root=tmp_path / "research")
    static = tmp_path / "static"
    static.mkdir()
    (static / "index.html").write_text("<main>Offline web integration</main>")
    app = create_app(static_dir=str(static))
    app.dependency_overrides[research.get_workspace] = lambda: workspace

    try:
        with TestClient(app) as client:
            for page in ("/settings", "/backtest"):
                assert client.get(page).status_code == 200
            assert client.get("/api/v1/research/missing").status_code == 404
            with factory.begin() as db:
                db.add_all([
                    User(username="research-owner", password_hash=service.hash_password("offline-owner"), role="admin"),
                    User(username="research-viewer", password_hash=service.hash_password("offline-viewer"), role="user"),
                ])

            def login(username, password):
                result = client.post("/api/v1/auth/login", json={"username": username, "password": password})
                assert result.status_code == 200
                return {"Authorization": f"Bearer {result.json()['access_token']}"}

            owner = login("research-owner", "offline-owner")
            viewer = login("research-viewer", "offline-viewer")
            for path in ("protocols", "snapshots", "jobs", "runs"):
                url = f"/api/v1/research/{path}"
                assert client.get(url).status_code == 401
                assert client.get(url, headers=viewer).status_code == 403
                response = client.get(url, headers=owner)
                assert response.status_code == 200, response.text
                assert isinstance(response.json(), list)
                if path != "protocols":
                    assert response.json() == []
                else:
                    main = next(item for item in response.json() if item["id"] == "eastmoney_etf")
                    assert main["source"] == "eastmoney"
                    assert main["request"]["adjustment"] == "hfq"

            for path, body in (
                ("download", {"protocol_id": "eastmoney_etf"}),
                ("run", {"protocol_id": "eastmoney_etf", "snapshot_id": "0" * 64}),
            ):
                assert client.post(f"/api/v1/research/{path}", json=body).status_code == 401
                assert client.post(f"/api/v1/research/{path}", json=body, headers=viewer).status_code == 403

            # Invalid parameters cannot start a background network request.
            invalid = client.post("/api/v1/research/download", headers=owner, json={
                "protocol_id": "eastmoney_etf", "min_interval": 0,
            })
            assert invalid.status_code == 422
            assert workspace.jobs() == []
    finally:
        engine.dispose()
