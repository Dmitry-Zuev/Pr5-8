"""Payment status model for study: no actual charge is made."""
from contextlib import asynccontextmanager
from decimal import Decimal
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import String, Numeric
from sqlalchemy.orm import Mapped, mapped_column
from common.runtime import Base, make_sessions, uid
from common.events import publish_event

class Payment(Base):
    __tablename__ = "payments"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(36), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)

class Create(BaseModel):
    user_id: str
    amount: Decimal = Field(gt=0, decimal_places=2)

def view(p):
    return {"id": p.id, "user_id": p.user_id, "amount": str(p.amount), "status": p.status}

def create_app(database_url=None):
    engine, Sessions = make_sessions(database_url)
    @asynccontextmanager
    async def lifespan(app):
        Base.metadata.create_all(engine, tables=[Payment.__table__]); yield; engine.dispose()
    app = FastAPI(title="Gym — PaymentService (simulation)", lifespan=lifespan)

    @app.post("/payments", status_code=201)
    def create(data: Create):
        with Sessions() as db:
            p = Payment(id=uid(), user_id=data.user_id, amount=data.amount, status="initiated")
            db.add(p); db.commit(); return view(p)

    @app.get("/payments/{payment_id}")
    def get(payment_id: str):
        with Sessions() as db:
            p = db.get(Payment, payment_id)
            if not p: raise HTTPException(404, "Платёж не найден")
            return view(p)

    def transition(payment_id, expected, target):
        with Sessions() as db:
            p = db.get(Payment, payment_id)
            if not p: raise HTTPException(404, "Платёж не найден")
            if p.status != expected: raise HTTPException(409, "Некорректный переход платежа")
            p.status = target
            db.commit()
            result = view(p)
            if target == "confirmed":
                publish_event("payment.confirmed", {"payment_id": p.id, "user_id": p.user_id})
            return result

    @app.post("/payments/{payment_id}:confirm")
    def confirm(payment_id: str):
        return transition(payment_id, "initiated", "confirmed")

    @app.post("/payments/{payment_id}:decline")
    def decline(payment_id: str):
        return transition(payment_id, "initiated", "declined")

    @app.post("/payments/{payment_id}:refund")
    def refund(payment_id: str):
        return transition(payment_id, "confirmed", "refunded")
    return app

app = create_app()
