import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool

from greengraph.database import get_session
from greengraph.main import app


@pytest.fixture(name="client")
def client_fixture():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)

    def get_session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = get_session_override
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_list_graphs_empty(client: TestClient):
    res = client.get("/api/graphs/")
    assert res.status_code == 200
    assert res.json() == []


def test_create_and_get_graph(client: TestClient):
    payload = {"name": "Test Graph", "description": "A test", "data": {"nodes": []}}
    res = client.post("/api/graphs/", json=payload)
    assert res.status_code == 201
    graph = res.json()
    assert graph["name"] == "Test Graph"
    assert graph["id"] is not None

    res2 = client.get(f"/api/graphs/{graph['id']}")
    assert res2.status_code == 200
    assert res2.json()["name"] == "Test Graph"


def test_update_graph(client: TestClient):
    res = client.post("/api/graphs/", json={"name": "Original"})
    gid = res.json()["id"]

    res2 = client.put(f"/api/graphs/{gid}", json={"name": "Updated"})
    assert res2.status_code == 200
    assert res2.json()["name"] == "Updated"


def test_delete_graph(client: TestClient):
    res = client.post("/api/graphs/", json={"name": "ToDelete"})
    gid = res.json()["id"]

    res2 = client.delete(f"/api/graphs/{gid}")
    assert res2.status_code == 204

    res3 = client.get(f"/api/graphs/{gid}")
    assert res3.status_code == 404


def test_graph_not_found(client: TestClient):
    res = client.get("/api/graphs/999")
    assert res.status_code == 404
