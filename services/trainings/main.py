"""Контекст тренировок; REST validation with UserService and Redis cache (optional for local tests)."""
from contextlib import asynccontextmanager
import json
from datetime import datetime
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import String, Integer, DateTime, ForeignKey, select
from sqlalchemy.orm import Mapped, mapped_column
from common.runtime import Base, make_sessions, uid, remote_get, ensure_remote_ok
from common.events import publish_event

class Training(Base):
    __tablename__ = "trainings"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    starts_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="planned")

class Booking(Base):
    __tablename__ = "bookings"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    training_id: Mapped[str] = mapped_column(String(36), ForeignKey("trainings.id"), nullable=False)
    user_id: Mapped[str] = mapped_column(String(36), nullable=False)

class TrainingCreate(BaseModel):
    title: str = Field(min_length=1)
    starts_at: datetime
    capacity: int = Field(gt=0)

class BookingCreate(BaseModel):
    user_id: str = Field(min_length=1)

class StateChange(BaseModel):
    status: str

def view(t):
    return {"id": t.id, "title": t.title, "starts_at": t.starts_at.isoformat(), "capacity": t.capacity, "status": t.status}

def create_app(database_url=None, user_service_url=None):
    engine, Sessions = make_sessions(database_url)
    user_url = user_service_url or os.getenv("USER_SERVICE_URL", "http://localhost:8001")
    @asynccontextmanager
    async def lifespan(app):
        Base.metadata.create_all(engine, tables=[Training.__table__, Booking.__table__]); yield; engine.dispose()
    app = FastAPI(title="Gym — TrainingService", lifespan=lifespan)
    def clear_cache():
        if os.getenv("REDIS_URL"):
            try:
                import redis
                redis.from_url(os.environ["REDIS_URL"], socket_connect_timeout=1).delete("gym:training:list")
            except Exception:
                pass  # Cache must never break primary datastore

    @app.post("/trainings", status_code=201)
    def create(data: TrainingCreate):
        with Sessions() as db:
            t = Training(id=uid(), title=data.title, starts_at=data.starts_at, capacity=data.capacity, status="planned")
            db.add(t); db.commit(); clear_cache(); return view(t)

    @app.get("/trainings")
    def list_trainings(q: str | None = None):
        cache = None
        if not q and os.getenv("REDIS_URL"):
            try:
                import redis
                cache = redis.from_url(os.environ["REDIS_URL"], socket_connect_timeout=1)
                found = cache.get("gym:training:list")
                if found: return json.loads(found)
            except Exception:
                cache = None
        with Sessions() as db:
            stmt = select(Training)
            if q: stmt = stmt.where(Training.title.ilike(f"%{q}%"))
            result = [view(t) for t in db.scalars(stmt).all()]
        if cache:
            try: cache.setex("gym:training:list", 60, json.dumps(result, ensure_ascii=False))
            except Exception: pass
        return result

    @app.get("/trainings/{training_id}")
    def get(training_id: str):
        with Sessions() as db:
            t = db.get(Training, training_id)
            if not t: raise HTTPException(404, "Тренировка не найдена")
            return view(t)

    @app.post("/trainings/{training_id}/bookings", status_code=201)
    def book(training_id: str, data: BookingCreate):
        ensure_remote_ok(remote_get(f"{user_url}/users/{data.user_id}"), "пользователь")
        with Sessions() as db:
            t = db.get(Training, training_id)
            if t is None: raise HTTPException(404, "Тренировка не найдена")
            if t.status != "planned": raise HTTPException(409, "Запись закрыта")
            if db.scalar(select(Booking).where(Booking.training_id == training_id, Booking.user_id == data.user_id)):
                raise HTTPException(409, "Повторная запись")
            if len(db.scalars(select(Booking).where(Booking.training_id == training_id)).all()) >= t.capacity:
                raise HTTPException(409, "Свободных мест нет")
            b = Booking(id=uid(), training_id=training_id, user_id=data.user_id)
            db.add(b); db.commit(); clear_cache()
            return {"id": b.id, "training_id": b.training_id, "user_id": b.user_id}

    @app.patch("/trainings/{training_id}")
    def change_status(training_id: str, data: StateChange):
        valid = {"planned": {"started", "cancelled"}, "started": {"finished", "cancelled"}}
        with Sessions() as db:
            t = db.get(Training, training_id)
            if t is None: raise HTTPException(404, "Тренировка не найдена")
            if data.status not in valid.get(t.status, set()): raise HTTPException(409, "Недопустимый переход")
            t.status = data.status
            db.commit(); clear_cache()
            result = view(t)
            if data.status == "finished":
                publish_event("training.finished", {"training_id": t.id})
            return result
    return app

app = create_app()
