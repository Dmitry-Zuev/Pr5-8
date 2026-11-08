"""Unit/component tests for the RabbitMQ-domain integration, no real broker."""
import logging

import pytest
from fastapi.testclient import TestClient
from common.events import EVENT_ROUTES, DOMAIN_EXCHANGE, declare_topology, make_envelope
from common.runtime import Base, make_sessions
from rabbitmq.workers import apply_event, WORKERS
from services.subscriptions.main import Subscription, create_app as subscription_app
from services.payments.main import Payment, create_app as payment_app
from services.reviews.main import CompletedTraining, create_app as reviews_app
from services.trainings.main import create_app as trainings_app
from services.tours.main import create_app as tours_app


def sessions(tmp_path, tag, tables):
    engine, factory = make_sessions(f"sqlite:///{tmp_path / (tag + '.db')}")
    Base.metadata.create_all(engine, tables=tables)
    return engine, factory


def event(name, data):
    return {"type": name, "event_id": "demo-event", "data": data}


def test_variant3_contract():
    from rabbitmq.exercise import VARIANTS, get_delay
    assert VARIANTS[3] == {"queue": "auto_delete", "exchange": "fanout", "delay": "random"}
    assert all(1 <= get_delay(3, "hello") <= 5 for _ in range(15))


def test_envelope_and_routing_table():
    e = make_envelope("training.finished", {"training_id": "t1"})
    assert e["type"] == "training.finished" and e["data"]["training_id"] == "t1"
    assert e["event_id"] and e["occurred_at"]
    assert EVENT_ROUTES["payment.confirmed"] == WORKERS["subscriptions"][1]
    assert len(EVENT_ROUTES) == 4


def test_durable_rabbit_topology_is_declared():
    class FakeChannel:
        def __init__(self): self.calls = []
        def exchange_declare(self, **kw): self.calls.append(("exchange", kw))
        def queue_declare(self, **kw): self.calls.append(("queue", kw))
        def queue_bind(self, **kw): self.calls.append(("bind", kw))
    fake = FakeChannel()
    declare_topology(fake)
    assert ("exchange", {"exchange": DOMAIN_EXCHANGE, "exchange_type": "topic", "durable": True}) in fake.calls
    assert any(t == "queue" and k.get("queue") == "gym.failed.events" for t,k in fake.calls)
    assert sum(1 for t,k in fake.calls if t == "bind" and k.get("exchange") == DOMAIN_EXCHANGE) == 4


def test_payment_confirmed_activates_only_linked_subscription(tmp_path):
    engine, db_factory = sessions(tmp_path, "subs", [Subscription.__table__])
    with db_factory() as db:
        db.add(Subscription(id="s1", user_id="u1", payment_id="p1",
                            expires_at=__import__("datetime").datetime(2027, 1, 1), status="selected"))
        db.add(Subscription(id="s2", user_id="u2", payment_id="p2", status="selected"))
        db.commit()
    assert apply_event("subscriptions", event("payment.confirmed", {"payment_id": "p1", "user_id": "u1"}), db_factory) == 1
    assert apply_event("subscriptions", event("payment.confirmed", {"payment_id": "p1", "user_id": "u1"}), db_factory) == 0
    assert apply_event("subscriptions", event("payment.confirmed", {"payment_id": "p2", "user_id": "u2"}), db_factory) == 0
    with db_factory() as db:
        assert db.get(Subscription, "s1").status == "active"
        assert db.get(Subscription, "s2").status == "selected"
    engine.dispose()


def test_termination_refunds_payment_once(tmp_path):
    from decimal import Decimal
    engine, db_factory = sessions(tmp_path, "payments", [Payment.__table__])
    with db_factory() as db:
        db.add(Payment(id="p1", user_id="u1", status="confirmed", amount=Decimal("200.00")))
        db.commit()
    evt = event("subscription.terminated", {"payment_id": "p1", "user_id": "u1", "subscription_id": "s1"})
    assert apply_event("payments", evt, db_factory) == 1
    assert apply_event("payments", evt, db_factory) == 0
    with db_factory() as db:
        assert db.get(Payment, "p1").status == "refunded"
    with pytest.raises(ValueError, match="mismatch"):
        apply_event("payments", event("subscription.terminated", {"payment_id": "p1", "user_id": "u2"}), db_factory)
    engine.dispose()


def test_completed_training_projection_deduplicates(tmp_path):
    engine, db_factory = sessions(tmp_path, "reviews", [CompletedTraining.__table__])
    evt = event("training.finished", {"training_id": "t1"})
    assert apply_event("reviews", evt, db_factory) == 1
    assert apply_event("reviews", evt, db_factory) == 0
    with db_factory() as db:
        assert db.get(CompletedTraining, "t1") is not None
    engine.dispose()


def test_tour_confirmation_only_logs_pending_manager_action(tmp_path, caplog):
    engine, db_factory = sessions(tmp_path, "tours", [])
    with caplog.at_level(logging.INFO):
        assert apply_event("tours", event("tour.confirmed", {"tour_id": "tour-1"}), db_factory) == 1
    assert "ожидает планирования" in caplog.text
    engine.dispose()


def test_http_payment_publishes_confirmation(tmp_path, monkeypatch):
    events = []
    monkeypatch.setattr("services.payments.main.publish_event", lambda t, d: events.append((t, d)))
    with TestClient(payment_app(f"sqlite:///{tmp_path / 'p.db'}")) as client:
        payment = client.post("/payments", json={"user_id": "u1", "amount": "25.00"}).json()
        assert client.post(f"/payments/{payment['id']}:confirm").status_code == 200
    assert events == [("payment.confirmed", {"payment_id": payment["id"], "user_id": "u1"})]


def test_http_subscription_publishes_termination(tmp_path, monkeypatch):
    import httpx
    monkeypatch.setattr("services.subscriptions.main.remote_get", lambda u: httpx.Response(200, json={"status":"confirmed", "user_id":"u1"}, request=httpx.Request("GET", u)))
    events=[]
    monkeypatch.setattr("services.subscriptions.main.publish_event", lambda t,d: events.append((t,d)))
    with TestClient(subscription_app(f"sqlite:///{tmp_path / 's.db'}")) as client:
        sub=client.post("/subscriptions", json={"user_id":"u1"}).json()
        assert client.patch(f"/subscriptions/{sub['id']}", json={"action":"activate","payment_id":"p1", "expires_at":"2027-12-31T00:00:00"}).status_code == 200
        assert client.patch(f"/subscriptions/{sub['id']}", json={"action":"terminate"}).status_code == 200
    assert len(events) == 1 and events[0][0] == "subscription.terminated"
    assert events[0][1]["payment_id"] == "p1"


def test_http_training_and_tour_publish_events(tmp_path, monkeypatch):
    emitted=[]
    monkeypatch.setattr("services.trainings.main.publish_event", lambda t,d: emitted.append((t,d)))
    monkeypatch.setattr("services.tours.main.publish_event", lambda t,d: emitted.append((t,d)))
    with TestClient(trainings_app(f"sqlite:///{tmp_path / 'tr.db'}")) as client:
        tr=client.post("/trainings", json={"title":"Зал", "starts_at":"2027-12-01T10:00:00", "capacity":1}).json()
        assert client.patch(f"/trainings/{tr['id']}", json={"status":"started"}).status_code==200
        assert client.patch(f"/trainings/{tr['id']}", json={"status":"finished"}).status_code==200
    with TestClient(tours_app(f"sqlite:///{tmp_path / 'to.db'}")) as client:
        tour=client.post("/tours",json={"applicant_name":"Гость","preferred_at":"2027-12-01T10:00:00"}).json()
        assert client.patch(f"/tours/{tour['id']}", json={"action":"confirm"}).status_code==200
    assert [e[0] for e in emitted] == ["training.finished", "tour.confirmed"]


def test_review_can_use_rabbit_projection_without_remote_call(tmp_path, monkeypatch):
    url=f"sqlite:///{tmp_path / 'rev.db'}"
    with TestClient(reviews_app(url)) as client:
        # Simulate Rabbit consumer inserting completed-training record.
        engine, factory=make_sessions(url)
        with factory() as db:
            db.add(CompletedTraining(training_id="t1"));db.commit()
        engine.dispose()
        monkeypatch.setattr("services.reviews.main.remote_get", lambda u: (_ for _ in ()).throw(AssertionError("No REST needed")))
        assert client.post("/reviews", json={"training_id":"t1","user_id":"u1","text":"Хорошо"}).status_code == 201
