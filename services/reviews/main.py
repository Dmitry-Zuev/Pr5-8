"""Review service — a review is allowed after the training has finished."""
from contextlib import asynccontextmanager
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import String, select
from sqlalchemy.orm import Mapped, mapped_column
from common.runtime import Base, make_sessions, uid, remote_get, ensure_remote_ok

class Review(Base):
    __tablename__ = "reviews"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    training_id: Mapped[str] = mapped_column(String(36), nullable=False)
    user_id: Mapped[str] = mapped_column(String(36), nullable=False)
    text: Mapped[str] = mapped_column(String(2000), nullable=False)

class CompletedTraining(Base):
    __tablename__ = "completed_trainings"
    training_id: Mapped[str] = mapped_column(String(36), primary_key=True)

class Create(BaseModel):
    training_id: str
    user_id: str
    text: str = Field(min_length=1, max_length=2000)

class Edit(BaseModel):
    text: str = Field(min_length=1, max_length=2000)

def view(r):
    return {"id": r.id, "training_id": r.training_id, "user_id": r.user_id, "text": r.text}

def create_app(database_url=None, training_service_url=None):
    engine, Sessions = make_sessions(database_url)
    training_url = training_service_url or os.getenv("TRAINING_SERVICE_URL", "http://localhost:8002")
    @asynccontextmanager
    async def lifespan(app):
        Base.metadata.create_all(engine, tables=[Review.__table__, CompletedTraining.__table__]); yield; engine.dispose()
    app = FastAPI(title="Gym — ReviewService", lifespan=lifespan)

    @app.post("/reviews", status_code=201)
    def create(data: Create):
        # When training.finished arrived via RabbitMQ, the local projection
        # lets this endpoint work without a synchronous lookup.
        with Sessions() as db:
            completed = db.get(CompletedTraining, data.training_id) is not None
        if not completed:
            training = ensure_remote_ok(remote_get(f"{training_url}/trainings/{data.training_id}"), "тренировка")
            if training["status"] != "finished":
                raise HTTPException(409, "Отзывы доступны после завершения тренировки")
        with Sessions() as db:
            r = Review(id=uid(), **data.model_dump()); db.add(r); db.commit(); return view(r)

    @app.get("/reviews")
    def listing(training_id: str | None = None):
        with Sessions() as db:
            stmt = select(Review)
            if training_id: stmt = stmt.where(Review.training_id == training_id)
            return [view(r) for r in db.scalars(stmt).all()]

    @app.patch("/reviews/{review_id}")
    def edit(review_id: str, data: Edit):
        with Sessions() as db:
            r = db.get(Review, review_id)
            if not r: raise HTTPException(404, "Отзыв не найден")
            r.text = data.text; db.commit(); return view(r)

    @app.delete("/reviews/{review_id}", status_code=204)
    def delete(review_id: str):
        with Sessions() as db:
            r = db.get(Review, review_id)
            if not r: raise HTTPException(404, "Отзыв не найден")
            db.delete(r); db.commit()
    return app

app = create_app()
