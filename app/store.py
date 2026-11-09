"""Import by the data contract (case/data-contract.md) and the store in PostgreSQL.

Rows are checked in the contract's order, the first broken rule names the code; a duplicate key
among correct rows is skipped. Accepted rows go to PostgreSQL as they came (text), so the history
survives a restart; the keys are kept in memory to tell duplicates without a query per row.
"""

import csv
import io
import json
import re
from datetime import datetime

import psycopg

FIELDS = {
    "runs": ["run_id", "flow_id", "scheduled_start_at", "prediction_at", "sla_deadline_at"],
    "events": ["event_id", "run_id", "flow_id", "event_ts", "observed_at", "event_type", "payload"],
    "outcomes": ["run_id", "final_status", "finished_at", "y_error", "y_sla_breach"],
}
KEY = {"runs": "run_id", "events": "event_id", "outcomes": "run_id"}
TIMES = {
    "runs": ("scheduled_start_at", "prediction_at", "sla_deadline_at"),
    "events": ("event_ts", "observed_at"),
    "outcomes": ("finished_at",),
}
EVENT_TYPES = {"run_started", "extract_finished", "load_finished", "warning", "error",
               "retry_started", "run_finished"}  # fmt: skip
STAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")


class FileError(Exception):
    """The whole file is refused: 400 with the code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _time(value: str) -> datetime | None:
    if not STAMP.fullmatch(value):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def check(dataset: str, row: dict, runs: set[str]) -> str | None:
    """The code of the first broken rule, or None for a correct row."""
    if any(not (row.get(f) or "").strip() for f in FIELDS[dataset]):
        return "MISSING_FIELD"
    times = {f: _time(row[f]) for f in TIMES[dataset]}
    if any(v is None for v in times.values()):
        return "INVALID_TIMESTAMP"
    if dataset == "runs":
        if times["sla_deadline_at"] <= times["scheduled_start_at"]:
            return "INVALID_DEADLINE"
        if times["prediction_at"] > times["scheduled_start_at"]:
            return "PREDICTION_AFTER_START"
    elif dataset == "events":
        if row["event_type"] not in EVENT_TYPES:
            return "UNKNOWN_EVENT_TYPE"
        try:
            payload = json.loads(row["payload"])
        except ValueError:
            return "INVALID_PAYLOAD"
        if not isinstance(payload, dict):
            return "INVALID_PAYLOAD"
        if times["observed_at"] < times["event_ts"]:
            return "OBSERVED_BEFORE_EVENT"
        if row["run_id"] not in runs:
            return "UNKNOWN_RUN"
    else:
        if row["final_status"] not in ("success", "failed") or any(
            row[f] not in ("0", "1") for f in ("y_error", "y_sla_breach")
        ):
            return "INVALID_LABEL"
        if row["run_id"] not in runs:
            return "UNKNOWN_RUN"
    return None


def parse(dataset: str, content: bytes) -> list[dict]:
    if dataset not in FIELDS:
        raise FileError("UNKNOWN_DATASET", f"no dataset {dataset!r}: runs, events or outcomes")
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise FileError("INVALID_ENCODING", "the file is not UTF-8") from exc
    if not text.strip():
        raise FileError("EMPTY_FILE", "the file has no header")
    reader = csv.DictReader(io.StringIO(text))
    missing = [f for f in FIELDS[dataset] if f not in (reader.fieldnames or [])]
    if missing:
        raise FileError("MISSING_COLUMNS", "missing columns: " + ", ".join(missing))
    return [{f: (row.get(f) or "") for f in FIELDS[dataset]} for row in reader]


def validate(dataset: str, rows: list[dict], keys: set[str], runs: set[str]) -> tuple[list[dict], list[dict], int]:
    """(accepted rows, errors, skipped). ``keys`` — the keys already stored for the dataset."""
    accepted: list[dict] = []
    errors: list[dict] = []
    seen = set(keys)
    skipped = 0
    for number, row in enumerate(rows, start=1):
        code = check(dataset, row, runs)
        if code is not None:
            errors.append({"row": number, "code": code})
            continue
        key = row[KEY[dataset]]
        if key in seen:
            skipped += 1
            continue
        seen.add(key)
        accepted.append(row)
    return accepted, errors, skipped


class Store:
    def __init__(self, url: str) -> None:
        self.conn = psycopg.connect(url, autocommit=True)
        with self.conn.cursor() as cur:
            for name, fields in FIELDS.items():
                columns = ", ".join(f"{f} text{' primary key' if f == KEY[name] else ''}" for f in fields)
                cur.execute(f"CREATE TABLE IF NOT EXISTS {name} ({columns})")  # noqa: S608 — our names
        self.keys = {name: set() for name in FIELDS}
        for name in FIELDS:
            self.keys[name] = {r[0] for r in self.conn.execute(f"SELECT {KEY[name]} FROM {name}")}  # noqa: S608

    def rows(self, dataset: str, order: str | None = None) -> list[dict]:
        fields = FIELDS[dataset]
        sql = f"SELECT {', '.join(fields)} FROM {dataset}" + (f" ORDER BY {order}" if order else "")  # noqa: S608
        return [dict(zip(fields, r, strict=True)) for r in self.conn.execute(sql)]

    def save(self, dataset: str, rows: list[dict]) -> None:
        if not rows:
            return
        fields = FIELDS[dataset]
        with self.conn.cursor() as cur, cur.copy(f"COPY {dataset} ({', '.join(fields)}) FROM STDIN") as copy:  # noqa: S608
            for row in rows:
                copy.write_row([row[f] for f in fields])
        self.keys[dataset].update(row[KEY[dataset]] for row in rows)
