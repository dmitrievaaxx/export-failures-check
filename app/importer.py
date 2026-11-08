"""Загрузка трёх файлов — правила выведены по truth.json и строкам открытых данных."""

import csv
import io
import json
import re

FIELDS = {
    "runs": ["run_id", "flow_id", "scheduled_start_at", "prediction_at", "sla_deadline_at"],
    "events": ["event_id", "run_id", "flow_id", "event_ts", "observed_at", "event_type", "payload"],
    "outcomes": ["run_id", "final_status", "finished_at", "y_error", "y_sla_breach"],
}
KEY = {"runs": "run_id", "events": "event_id", "outcomes": "run_id"}
TIMES = {"runs": ["scheduled_start_at", "prediction_at", "sla_deadline_at"],
         "events": ["event_ts", "observed_at"], "outcomes": ["finished_at"]}
TYPES = {"run_started", "extract_finished", "load_finished", "warning", "error", "retry_started", "run_finished"}
STAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")


class FileError(Exception):
    def __init__(self, code: str):
        self.code = code


def rows_of(dataset: str, content: bytes | None) -> list[dict]:
    if dataset not in FIELDS:
        raise FileError("UNKNOWN_DATASET")
    if content is None:
        raise FileError("FILE_REQUIRED")
    text = content.decode("utf-8")
    if not text.strip():
        raise FileError("EMPTY_FILE")
    reader = csv.DictReader(io.StringIO(text))
    if any(f not in (reader.fieldnames or []) for f in FIELDS[dataset]):
        raise FileError("MISSING_COLUMNS")
    return list(reader)


def broken(dataset: str, row: dict, runs: set[str]) -> str | None:
    if any(not row.get(f) for f in FIELDS[dataset]):
        return "MISSING_FIELD"
    if any(not STAMP.fullmatch(row[f]) for f in TIMES[dataset]):
        return "INVALID_TIMESTAMP"
    if dataset == "runs":
        if row["sla_deadline_at"] <= row["scheduled_start_at"]:
            return "INVALID_DEADLINE"
        if row["prediction_at"] > row["scheduled_start_at"]:
            return "PREDICTION_AFTER_START"
    if dataset == "events":
        if row["event_type"] not in TYPES:
            return "UNKNOWN_EVENT_TYPE"
        try:
            ok = isinstance(json.loads(row["payload"]), dict)
        except ValueError:
            ok = False
        if not ok:
            return "INVALID_PAYLOAD"
        if row["observed_at"] < row["event_ts"]:
            return "OBSERVED_BEFORE_EVENT"
    if dataset == "outcomes" and (row["y_error"] not in ("0", "1") or row["y_sla_breach"] not in ("0", "1")):
        return "INVALID_LABEL"
    if dataset != "runs" and row["run_id"] not in runs:
        return "UNKNOWN_RUN"
    return None
