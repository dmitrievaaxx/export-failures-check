"""Пример теста. Нужна запущенная база: DATABASE_URL, как в README."""

from fastapi.testclient import TestClient

from app.main import app


def test_health_answers_and_sees_the_database():
    with TestClient(app) as client:
        response = client.get("/health")
    assert response.status_code == 200
    # only "status": the contract adds "records" here, and the test must not break on it
    assert response.json()["status"] == "ok"
