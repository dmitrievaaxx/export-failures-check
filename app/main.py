"""Стартовый сервис: FastAPI и PostgreSQL.

Платформа запускает его так же, как вы локально: `uvicorn app.main:app` на порту 8000, адрес
базы — в переменной DATABASE_URL. Отсюда начинается решение кейса: добавляйте свои модули в
папку app/ и эндпоинты по контракту API из задания.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import db


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.migrate()  # схема создаётся при старте и переживает перезапуск сервиса
    yield


app = FastAPI(title="Сервис кейса", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    """Платформа ждёт, пока сервис ответит здесь, и только потом начинает проверку."""
    return {"status": "ok", "db": db.ping()}
