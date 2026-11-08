"""HTTP component tests with isolated SQLite DB and mocked remote dependencies.
TestClient calls the public HTTP contract; no production infrastructure needed.
"""
import httpx
from fastapi.testclient import TestClient
from services.users.main import create_app as users_app
from services.trainings.main import create_app as trainings_app
from services.reviews.main import create_app as reviews_app
from services.subscriptions.main import create_app as subscriptions_app
from services.payments.main import create_app as payments_app
from services.tours.main import create_app as tours_app


def dburl(tmp_path, service):
    return f"sqlite:///{tmp_path / (service + '.db')}"


def success(data):
    return httpx.Response(200, json=data, request=httpx.Request("GET", "https://test.local"))


def test_users_registration_login_and_duplicate(tmp_path):
    with TestClient(users_app(dburl(tmp_path, "users"))) as client:
        payload = {"email": "client@example.org", "name": "Клиент", "password": "secretpass123"}
        response = client.post("/users", json=payload)
        assert response.status_code == 201
        identifier = response.json()["id"]
        assert "password" not in response.json()
        assert client.post("/users", json=payload).status_code == 409
        assert client.post("/users:login", json={"email": payload["email"], "password": "wrong"}).status_code == 401
        assert client.post("/users:login", json={"email": payload["email"], "password": payload["password"]}).status_code == 200
        assert client.patch(f"/users/{identifier}", json={"name": "Обновлено"}).json()["name"] == "Обновлено"
        assert client.get("/users/unknown").status_code == 404


def test_training_booking_checks_remote_user_and_capacity(tmp_path, monkeypatch):
    monkeypatch.setattr("services.trainings.main.remote_get", lambda url: success({"id": "user-1"}))
    with TestClient(trainings_app(dburl(tmp_path, "trainings"))) as client:
        created = client.post("/trainings", json={"title": "Йога", "starts_at": "2026-12-01T10:00:00", "capacity": 1})
        assert created.status_code == 201
        identifier = created.json()["id"]
        assert len(client.get("/trainings?q=Йога").json()) == 1
        assert client.post(f"/trainings/{identifier}/bookings", json={"user_id": "user-1"}).status_code == 201
        assert client.post(f"/trainings/{identifier}/bookings", json={"user_id": "user-1"}).status_code == 409
        assert client.post(f"/trainings/{identifier}/bookings", json={"user_id": "user-2"}).status_code == 409
        assert client.patch(f"/trainings/{identifier}", json={"status": "started"}).json()["status"] == "started"
        assert client.patch(f"/trainings/{identifier}", json={"status": "finished"}).status_code == 200
        assert client.patch(f"/trainings/{identifier}", json={"status": "started"}).status_code == 409


def test_review_only_after_finished(tmp_path, monkeypatch):
    state = {"status": "planned"}
    monkeypatch.setattr("services.reviews.main.remote_get", lambda url: success(state))
    with TestClient(reviews_app(dburl(tmp_path, "reviews"))) as client:
        payload = {"training_id": "t1", "user_id": "u1", "text": "Хорошо"}
        assert client.post("/reviews", json=payload).status_code == 409
        state["status"] = "finished"
        response = client.post("/reviews", json=payload)
        assert response.status_code == 201
        review_id = response.json()["id"]
        assert len(client.get("/reviews?training_id=t1").json()) == 1
        assert client.patch(f"/reviews/{review_id}", json={"text": "Отлично"}).json()["text"] == "Отлично"
        assert client.delete(f"/reviews/{review_id}").status_code == 204
        assert client.get("/reviews").json() == []


def test_payment_lifecycle(tmp_path):
    with TestClient(payments_app(dburl(tmp_path, "payments"))) as client:
        response = client.post("/payments", json={"user_id": "u1", "amount": "100.00"})
        assert response.status_code == 201
        payment_id = response.json()["id"]
        assert client.post(f"/payments/{payment_id}:refund").status_code == 409
        assert client.post(f"/payments/{payment_id}:confirm").json()["status"] == "confirmed"
        assert client.get(f"/payments/{payment_id}").json()["amount"] == "100.00"
        assert client.post(f"/payments/{payment_id}:refund").json()["status"] == "refunded"
        assert client.post(f"/payments/{payment_id}:confirm").status_code == 409


def test_subscription_payment_validation_and_freeze(tmp_path, monkeypatch):
    monkeypatch.setattr("services.subscriptions.main.remote_get", lambda url: success({"status": "confirmed", "user_id": "u1"}))
    with TestClient(subscriptions_app(dburl(tmp_path, "subscriptions"))) as client:
        response = client.post("/subscriptions", json={"user_id": "u1"})
        sid = response.json()["id"]
        assert client.patch(f"/subscriptions/{sid}", json={"action": "activate"}).status_code == 422
        active = client.patch(f"/subscriptions/{sid}", json={"action": "activate", "payment_id": "p1", "expires_at": "2027-01-01T00:00:00"})
        assert active.status_code == 200
        assert active.json()["status"] == "active"
        assert client.patch(f"/subscriptions/{sid}", json={"action": "freeze"}).json()["status"] == "frozen"
        assert client.patch(f"/subscriptions/{sid}", json={"action": "unfreeze"}).json()["status"] == "active"
        assert client.patch(f"/subscriptions/{sid}", json={"action": "extend", "expires_at": "2027-02-01T00:00:00"}).status_code == 200
        assert client.patch(f"/subscriptions/{sid}", json={"action": "terminate"}).json()["status"] == "terminated"
        assert client.patch(f"/subscriptions/{sid}", json={"action": "freeze"}).status_code == 409
        assert len(client.get("/subscriptions?user_id=u1").json()) == 1


def test_tours_request_confirmation_planning(tmp_path):
    with TestClient(tours_app(dburl(tmp_path, "tours"))) as client:
        response = client.post("/tours", json={"applicant_name": "Посетитель", "preferred_at": "2026-12-03T14:00:00"})
        assert response.status_code == 201
        tour_id = response.json()["id"]
        assert client.patch(f"/tours/{tour_id}", json={"action": "plan"}).status_code == 409
        assert client.patch(f"/tours/{tour_id}", json={"action": "confirm"}).json()["status"] == "confirmed"
        assert client.patch(f"/tours/{tour_id}", json={"action": "plan"}).status_code == 422
        assert client.patch(f"/tours/{tour_id}", json={"action": "plan", "planned_at": "2026-12-04T14:00:00"}).json()["status"] == "planned"
        assert client.patch(f"/tours/{tour_id}", json={"action": "conduct"}).json()["status"] == "conducted"
        assert len(client.get("/tours").json()) == 1
        assert client.get("/tours/missing").status_code == 404
