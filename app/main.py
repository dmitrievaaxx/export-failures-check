"""Стартовый сервис: FastAPI и PostgreSQL.

Платформа запускает его так же, как вы локально: `uvicorn app.main:app` на порту 8000, адрес
базы — в переменной DATABASE_URL. Ниже — заготовки запросов этапа «Загрузка данных»: допишите
их; свои модули кладите в папку app/.
"""

from contextlib import asynccontextmanager

import json

from fastapi import Body, FastAPI, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from app import db
from app import history
from app.importer import KEY, FileError, broken, rows_of

KEYS: dict[str, set] = {"runs": set(), "events": set(), "outcomes": set()}


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.migrate()  # схема создаётся при старте и переживает перезапуск сервиса
    with db.connect() as conn:
        for dataset, key, data in conn.execute("SELECT dataset, key, data FROM records ORDER BY dataset"):
            KEYS[dataset].add(key)
            history.add(dataset, data)
    yield


app = FastAPI(title="Сервис кейса", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    """Платформа ждёт ответа здесь и только потом начинает проверку.

    records — сколько записей каждого вида сейчас в хранилище.
    """
    return {"status": "ok", "records": {d: len(KEYS[d]) for d in ("runs", "events", "outcomes")}}


@app.post("/import/{dataset}")
async def import_file(dataset: str, file: UploadFile | None = None) -> dict:
    """Загрузка файла: dataset — runs, events или outcomes; файл — в поле file формы.

    Ответ: {"dataset": dataset, "total": строк данных в файле, "accepted": …, "rejected": …,
            "skipped": …, "errors": [{"row": номер строки, "code": код}, …]}
    В errors — каждая отклонённая строка. Повторная загрузка того же файла ничего не добавляет.

    Негодный файл отклоняется целиком: ответ 400 и {"error": {"code": …, "message": …}}.
    Коды: UNKNOWN_DATASET — набор не runs, events или outcomes; FILE_REQUIRED — нет файла;
    MISSING_COLUMNS — в заголовке нет нужной колонки; EMPTY_FILE — нет даже заголовка.
    Файл только с заголовком — не ошибка: ответ 200 и нули.
    """
    try:
        rows = rows_of(dataset, await file.read() if file is not None else None)
    except FileError as e:
        return JSONResponse({"error": {"code": e.code, "message": e.code}}, status_code=400)
    stored, runs = KEYS[dataset], KEYS["runs"]
    with db.connect() as conn:
        errors, accepted, skipped = [], [], 0
        for n, row in enumerate(rows, start=1):
            code = broken(dataset, row, runs)
            if code:
                errors.append({"row": n, "code": code})
            elif row[KEY[dataset]] in stored:
                skipped += 1
            else:
                stored.add(row[KEY[dataset]])
                accepted.append(row)
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO records (dataset, key, data) VALUES (%s, %s, %s)",
                            [(dataset, r[KEY[dataset]], json.dumps(r)) for r in accepted])
    for r in accepted:
        history.add(dataset, r)
    return {"dataset": dataset, "total": len(rows), "accepted": len(accepted), "rejected": len(errors),
            "skipped": skipped, "errors": errors}


# Запросы о запусках (этап 5 и дальше). Тело — {"run_ids": [...]}, до 500 запусков; в ответе — элемент
# на каждый известный запуск, неизвестные пропускаются. Ответ о запуске зависит только от того, что было
# в хранилище к его prediction_at: план и события с observed_at не позже prediction_at. Платформа
# спрашивает чуть позже prediction_at, когда более поздние события уже могут быть загружены, а в конце
# спрашивает ещё раз — ответ не должен измениться.


@app.post("/features")
def features(body: dict = Body(...)) -> dict:
    """Признаки запусков: {"features": [{"run_id": …, "values": {"имя": число или null, …}}, …]}.

    Набор имён одинаковый у всех запусков; какие признаки считать и как их назвать — решаете вы.
    """
    known = [r for r in body.get("run_ids", [])
             if r in history.runs and history.events[history.runs[r]["flow_id"]]]  # без истории — пропускаем
    return {"features": [{"run_id": r, "values": history.features_of(r)} for r in known]}


@app.post("/baseline")
def baseline(body: dict = Body(...)) -> dict:
    """Простой прогноз на правилах, без обучения:
    {"predictions": [{"run_id": …, "error": 0 или 1, "sla": 0 или 1}, …]}.
    """
    out = []
    for r in body.get("run_ids", []):
        if r in history.runs:
            f = history.features_of(r)
            out.append({"run_id": r, "error": int(f["errors_24h"] >= 2 or f["unstable_1h"] >= 1 or f["last_failed"] == 1),
                        "sla": int(f["lag_1h"] >= 1 or f["last_failed"] == 1)})
    return {"predictions": out}
