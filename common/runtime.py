"""Shared infrastructure; each service still owns its own engine/database."""
import os
from uuid import uuid4
import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from fastapi import HTTPException

class Base(DeclarativeBase):
    pass

def make_sessions(url=None):
    url = url or os.getenv("DATABASE_URL", "sqlite:///./local-dev.db")
    engine = create_engine(url, connect_args={"check_same_thread": False} if url.startswith("sqlite:") else {}, pool_pre_ping=True)
    return engine, sessionmaker(bind=engine, expire_on_commit=False)

def uid():
    return str(uuid4())

def remote_get(url):
    try:
        with httpx.Client(timeout=3.0) as client:
            return client.get(url)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail="Зависимый сервис недоступен") from exc

def remote_post(url):
    try:
        with httpx.Client(timeout=3.0) as client:
            return client.post(url)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail="Зависимый сервис недоступен") from exc

def ensure_remote_ok(response, label):
    if response.status_code == 404:
        raise HTTPException(404, f"Не найдено: {label}")
    if response.status_code >= 400:
        raise HTTPException(502, f"Ошибка зависимого сервиса: {label}")
    return response.json()
