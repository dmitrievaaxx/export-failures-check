"""The data platform's helper: import, history, features, forecast and alerts (case/api-contract.md).

PostgreSQL holds what was imported (app/store.py); the feature engine (app/engine.py) is rebuilt
from it at start. A run's answers are computed once, at the first question about it — from what
was in the store at its moment — and kept: the forecast is made before the run, not rewritten
after it.
"""

import os
import threading
from pathlib import Path

import joblib
import numpy as np
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app import rules
from app.engine import NAMES, Engine
from app.store import FIELDS, FileError, Store, parse, validate

# our own model file, trained by train.py and committed with the code (joblib unpickles: never load
# a file from anywhere else)
MODEL = joblib.load(Path(__file__).resolve().parents[1] / "model" / "model.joblib")
MODEL["thresholds"] = {"error": 0.5, "sla": 0.5}
app = FastAPI()
lock = threading.Lock()
store = Store(os.environ.get("DATABASE_URL", "postgresql://app@127.0.0.1:5432/app"))
engine = Engine(MODEL["sources"])
for row in store.rows("runs"):
    engine.add_run(row)
for row in store.rows("events", order="observed_at, event_id"):
    engine.add_event(row)
cache: dict[str, dict] = {}


def error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse({"error": {"code": code, "message": message}}, status_code=status)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "records": {name: len(store.keys[name]) for name in FIELDS}}


@app.post("/import/{dataset}")
async def import_file(dataset: str, request: Request):
    if dataset not in FIELDS:
        return error(400, "UNKNOWN_DATASET", f"no dataset {dataset!r}")
    form = await request.form()
    upload = form.get("file")
    if upload is None or isinstance(upload, str):
        return error(400, "FILE_REQUIRED", "send the CSV in the form field 'file'")
    try:
        rows = parse(dataset, await upload.read())
    except FileError as exc:
        return error(400, exc.code, str(exc))
    with lock:
        accepted, errors, skipped = validate(dataset, rows, store.keys[dataset], store.keys["runs"])
        store.save(dataset, accepted)
        for row in accepted:
            if dataset == "runs":
                engine.add_run(row)
            elif dataset == "events":
                engine.add_event(row)
    return {"dataset": dataset, "total": len(rows), "accepted": len(accepted),
            "rejected": len(errors), "skipped": skipped, "errors": errors}  # fmt: skip


def answers(run_ids: list[str]) -> dict[str, dict]:
    """Each known run's answers, computed at the first question, in the order of their moments."""
    known = [engine.runs[r] for r in dict.fromkeys(run_ids) if r in engine.runs]
    fresh = sorted((r for r in known if r.run_id not in cache), key=lambda r: (r.pred, r.run_id))
    if fresh:
        rows = [engine.features(r) for r in fresh]
        x = np.array([[np.nan if f[n] is None else f[n] for n in NAMES] for f in rows], dtype=float)
        p_error = MODEL["error"].predict_proba(x)[:, 1]
        p_sla = MODEL["sla"].predict_proba(x)[:, 1]
        for run, f, pe, ps in zip(fresh, rows, p_error, p_sla):
            cache[run.run_id] = {"features": f, "p_error": round(float(pe), 6),
                                 "p_sla": round(float(ps), 6)}  # fmt: skip
    return {r.run_id: cache[r.run_id] for r in known}


def run_ids(body: dict) -> list[str]:
    ids = body.get("run_ids") if isinstance(body, dict) else None
    return [r for r in ids if isinstance(r, str)] if isinstance(ids, list) else []


@app.post("/features")
async def features(request: Request):
    with lock:
        found = answers(run_ids(await request.json()))
    return {"features": [{"run_id": r, "values": a["features"]} for r, a in found.items()]}


@app.post("/baseline")
async def baseline(request: Request):
    with lock:
        found = answers(run_ids(await request.json()))
    out = []
    for r, a in found.items():
        e, s = rules.baseline(a["features"])
        out.append({"run_id": r, "error": e, "sla": s})
    return {"predictions": out}


@app.post("/predict")
async def predict(request: Request):
    with lock:
        found = answers(run_ids(await request.json()))
    return {"predictions": [{"run_id": r, "p_error": a["p_error"], "p_sla": a["p_sla"]}
                            for r, a in found.items()]}  # fmt: skip


@app.get("/model")
def model() -> dict:
    return {"thresholds": MODEL["thresholds"], "version": MODEL["version"]}


@app.post("/alerts")
async def alerts(request: Request):
    with lock:
        found = answers(run_ids(await request.json()))
    out = []
    for r, a in found.items():
        e = a["p_error"] >= MODEL["thresholds"]["error"]
        s = a["p_sla"] >= MODEL["thresholds"]["sla"]
        reasons, action = rules.words(a["features"]) if e or s else ([], "")
        out.append({"run_id": r, "p_error": a["p_error"], "p_sla": a["p_sla"], "error": e,
                    "sla": s, "alert": e or s, "reasons": reasons, "recommendation": action})  # fmt: skip
    return {"alerts": out}
