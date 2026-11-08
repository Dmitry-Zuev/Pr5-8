"""Excursion requests and manager-driven state transitions."""
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import String, DateTime, select
from sqlalchemy.orm import Mapped, mapped_column
from common.runtime import Base, make_sessions, uid
from common.events import publish_event

class Tour(Base):
    __tablename__ = "tours"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    applicant_name: Mapped[str] = mapped_column(String(150), nullable=False)
    preferred_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    planned_at: Mapped[datetime | None] = mapped_column(DateTime)

class Create(BaseModel):
    applicant_name: str = Field(min_length=1)
    preferred_at: datetime

class Change(BaseModel):
    action: str
    planned_at: datetime | None = None

def view(t):
    return {"id": t.id, "applicant_name": t.applicant_name, "preferred_at": t.preferred_at.isoformat(),
            "status": t.status, "planned_at": t.planned_at.isoformat() if t.planned_at else None}

def create_app(database_url=None):
    engine, Sessions = make_sessions(database_url)
    @asynccontextmanager
    async def lifespan(app):
        Base.metadata.create_all(engine, tables=[Tour.__table__]); yield; engine.dispose()
    app = FastAPI(title="Gym — TourService", lifespan=lifespan)

    @app.post("/tours", status_code=201)
    def create(data: Create):
        with Sessions() as db:
            t = Tour(id=uid(), applicant_name=data.applicant_name, preferred_at=data.preferred_at, status="requested")
            db.add(t); db.commit(); return view(t)

    @app.get("/tours")
    def listing():
        with Sessions() as db:
            return [view(t) for t in db.scalars(select(Tour)).all()]

    @app.get("/tours/{tour_id}")
    def get(tour_id: str):
        with Sessions() as db:
            t = db.get(Tour, tour_id)
            if not t: raise HTTPException(404, "Заявка на экскурсию не найдена")
            return view(t)

    @app.patch("/tours/{tour_id}")
    def change(tour_id: str, data: Change):
        actions = {"requested": {"confirm", "cancel"}, "confirmed": {"plan", "cancel"}, "planned": {"conduct", "cancel"}}
        target = {"confirm": "confirmed", "plan": "planned", "conduct": "conducted", "cancel": "cancelled"}
        with Sessions() as db:
            t = db.get(Tour, tour_id)
            if not t: raise HTTPException(404, "Заявка на экскурсию не найдена")
            if data.action not in actions.get(t.status, set()): raise HTTPException(409, "Недопустимый переход экскурсии")
            if data.action == "plan":
                if not data.planned_at: raise HTTPException(422, "Необходимо время экскурсии")
                t.planned_at = data.planned_at
            t.status = target[data.action]
            db.commit()
            result = view(t)
            if data.action == "confirm":
                publish_event("tour.confirmed", {"tour_id": t.id})
            return result
    return app

app = create_app()
