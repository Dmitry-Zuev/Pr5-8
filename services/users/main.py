"""Контекст управления пользователем. Four public endpoints."""
from contextlib import asynccontextmanager
from hashlib import pbkdf2_hmac
from os import urandom
import hmac
from typing import Annotated
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import String, select
from sqlalchemy.orm import Mapped, mapped_column
from common.runtime import Base, make_sessions, uid

class User(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)

class Register(BaseModel):
    email: EmailStr
    name: str = Field(min_length=1)
    password: str = Field(min_length=8)

class Login(BaseModel):
    email: EmailStr
    password: str

class Update(BaseModel):
    name: str = Field(min_length=1)

def hash_password(password: str) -> str:
    salt = urandom(16)
    digest = pbkdf2_hmac("sha256", password.encode(), salt, 200000)
    return f"{salt.hex()}:{digest.hex()}"

def verify_password(password: str, encoded: str) -> bool:
    salt, digest = encoded.split(":", 1)
    actual = pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 200000)
    return hmac.compare_digest(actual, bytes.fromhex(digest))

def view(u):
    return {"id": u.id, "email": u.email, "name": u.name}

def create_app(database_url=None):
    engine, Sessions = make_sessions(database_url)
    @asynccontextmanager
    async def lifespan(app):
        Base.metadata.create_all(engine, tables=[User.__table__])
        yield
        engine.dispose()
    app = FastAPI(title="Gym — UserService", lifespan=lifespan)

    @app.post("/users", status_code=201)
    def register(data: Register):
        with Sessions() as db:
            if db.scalar(select(User).where(User.email == str(data.email))):
                raise HTTPException(409, "Email уже зарегистрирован")
            user = User(id=uid(), email=str(data.email), name=data.name, password_hash=hash_password(data.password))
            db.add(user); db.commit()
            return view(user)

    @app.post("/users:login")
    def login(data: Login):
        with Sessions() as db:
            user = db.scalar(select(User).where(User.email == str(data.email)))
            if user is None or not verify_password(data.password, user.password_hash):
                raise HTTPException(401, "Неверные учётные данные")
            return {"id": user.id, "authenticated": True}  # Не JWT и не сессия

    @app.get("/users/{user_id}")
    def get_user(user_id: str):
        with Sessions() as db:
            user = db.get(User, user_id)
            if user is None: raise HTTPException(404, "Пользователь не найден")
            return view(user)

    @app.patch("/users/{user_id}")
    def update_user(user_id: str, data: Update):
        with Sessions() as db:
            user = db.get(User, user_id)
            if user is None: raise HTTPException(404, "Пользователь не найден")
            user.name = data.name
            db.commit()
            return view(user)
    return app

app = create_app()
