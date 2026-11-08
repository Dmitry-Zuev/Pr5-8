"""Subscription service: key state transitions and checks with PaymentService."""
from contextlib import asynccontextmanager
import os
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import String, DateTime, select
from sqlalchemy.orm import Mapped, mapped_column
from common.runtime import Base, make_sessions, uid, remote_get, ensure_remote_ok
from common.events import publish_event

class Subscription(Base):
    __tablename__ = "subscriptions"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(36), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    payment_id: Mapped[str | None] = mapped_column(String(36), nullable=True, unique=True)

class Create(BaseModel):
    user_id: str
    # Optional link for RabbitMQ automatic activation when payment confirms;
    # expiry is supplied by client because prior practicals do not specify tariffs.
    payment_id: str | None = None
    expires_at: datetime | None = None

class Change(BaseModel):
    action: str
    payment_id: str | None = None
    expires_at: datetime | None = None

def view(s):
    return {"id": s.id, "user_id": s.user_id, "status": s.status, "payment_id": s.payment_id, "expires_at": s.expires_at.isoformat() if s.expires_at else None}

def create_app(database_url=None, payment_service_url=None):
    engine, Sessions = make_sessions(database_url)
    payment_url = payment_service_url or os.getenv("PAYMENT_SERVICE_URL", "http://localhost:8005")
    @asynccontextmanager
    async def lifespan(app):
        Base.metadata.create_all(engine, tables=[Subscription.__table__]); yield; engine.dispose()
    app = FastAPI(title="Gym — SubscriptionService", lifespan=lifespan)

    @app.post("/subscriptions", status_code=201)
    def create(data: Create):
        if (data.payment_id is None) != (data.expires_at is None):
            raise HTTPException(422, "payment_id и expires_at указываются вместе")
        with Sessions() as db:
            s = Subscription(id=uid(), user_id=data.user_id, status="selected",
                             payment_id=data.payment_id, expires_at=data.expires_at)
            db.add(s); db.commit(); return view(s)

    @app.get("/subscriptions")
    def listing(user_id: str | None = None):
        with Sessions() as db:
            stmt = select(Subscription)
            if user_id: stmt = stmt.where(Subscription.user_id == user_id)
            return [view(s) for s in db.scalars(stmt).all()]

    @app.get("/subscriptions/{subscription_id}")
    def get(subscription_id: str):
        with Sessions() as db:
            s = db.get(Subscription, subscription_id)
            if not s: raise HTTPException(404, "Абонемент не найден")
            return view(s)

    @app.patch("/subscriptions/{subscription_id}")
    def change(subscription_id: str, data: Change):
        states = {"selected": {"activate", "terminate"}, "active": {"freeze", "extend", "terminate", "expire"},
                  "frozen": {"unfreeze", "terminate"}}
        targets = {"activate": "active", "freeze": "frozen", "unfreeze": "active", "extend": "active", "terminate": "terminated", "expire": "expired"}
        with Sessions() as db:
            s = db.get(Subscription, subscription_id)
            if not s: raise HTTPException(404, "Абонемент не найден")
            if data.action not in states.get(s.status, set()): raise HTTPException(409, "Недопустимый переход абонемента")
            if data.action == "activate":
                if not data.payment_id or data.expires_at is None: raise HTTPException(422, "Требуются payment_id и expires_at")
                p = ensure_remote_ok(remote_get(f"{payment_url}/payments/{data.payment_id}"), "платёж")
                if p["status"] != "confirmed" or p["user_id"] != s.user_id:
                    raise HTTPException(409, "Платёж не подтвержден или принадлежит другому пользователю")
                s.payment_id, s.expires_at = data.payment_id, data.expires_at
            if data.action == "extend":
                if data.expires_at is None or (s.expires_at and data.expires_at <= s.expires_at):
                    raise HTTPException(422, "Новая дата должна быть позже текущей")
                s.expires_at = data.expires_at
            was_paid = s.status in {"active", "frozen"} and bool(s.payment_id)
            s.status = targets[data.action]
            db.commit()
            result = view(s)
            if data.action == "terminate" and was_paid:
                # PaymentService processes this asynchronously via RabbitMQ.
                publish_event("subscription.terminated", {
                    "subscription_id": s.id, "payment_id": s.payment_id, "user_id": s.user_id})
            return result
    return app

app = create_app()
