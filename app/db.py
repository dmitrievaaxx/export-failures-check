"""Подключение к PostgreSQL по DATABASE_URL.

На проверке платформа сама поднимает базу и передаёт её адрес; локально его задаёт compose.yaml.
"""

import os

import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://app:app@localhost:5432/app")

SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    dataset TEXT NOT NULL,
    key     TEXT NOT NULL,
    data    JSONB NOT NULL,
    PRIMARY KEY (dataset, key)
);
"""


def connect() -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL, connect_timeout=5)


def migrate() -> None:
    with connect() as conn:
        conn.execute(SCHEMA)


def ping() -> bool:
    try:
        with connect() as conn:
            conn.execute("SELECT 1")
        return True
    except psycopg.Error:
        return False
