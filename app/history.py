"""История в памяти для запросов о запусках: что было в хранилище к моменту прогноза."""

import bisect
import json
from collections import defaultdict

runs: dict[str, dict] = {}                       # run_id → строка плана
events: dict[str, list] = defaultdict(list)      # flow_id → [(observed_at, event_ts, type, payload)]
answers: dict[str, dict] = {}                    # ответ о запуске считается один раз


def add(dataset: str, row: dict) -> None:
    if dataset == "runs":
        runs[row["run_id"]] = row
    elif dataset == "events":
        bisect.insort(events[row["flow_id"]], (row["observed_at"], row["event_ts"], row["event_type"], row["payload"]))


def minus_hours(stamp: str, hours: int) -> str:
    from datetime import datetime, timedelta
    t = datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ") - timedelta(hours=hours)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def features_of(run_id: str) -> dict:
    if run_id in answers:
        return answers[run_id]
    run = runs[run_id]
    p = run["prediction_at"]
    seen = events[run["flow_id"]]
    upto = bisect.bisect_right(seen, (p, "￿"))  # только события с observed_at ≤ prediction_at
    day, hour = minus_hours(p, 24), minus_hours(p, 1)
    errors_24h = warnings_1h = lags_1h = 0
    last_status = None
    for observed, _, kind, payload in seen[:upto]:
        if kind == "error" and observed > day:
            errors_24h += 1
        if kind == "warning" and observed > hour:
            code = json.loads(payload).get("code")
            warnings_1h += code == "source_unstable"
            lags_1h += code == "source_lag"
        if kind == "run_finished":
            last_status = json.loads(payload).get("status")  # статус прошлого запуска как есть
    answers[run_id] = {"errors_24h": errors_24h, "unstable_1h": warnings_1h, "lag_1h": lags_1h,
                       "last_failed": last_status}
    return answers[run_id]
