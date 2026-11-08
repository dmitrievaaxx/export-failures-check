"""Features of a run at its moment of forecast, from what the store held then.

Events are applied in the order they reached the store (`observed_at`, then `event_id`) and only
up to the moment asked about: `features(run)` first applies every pending event observed at or
before the run's `prediction_at` and nothing later. A finished run enters its flow's history when
its `run_finished` is observed. The same engine builds the training set (train.py replays the open
70 days) and answers the service, so the model sees at training exactly what it sees in service.
"""

import bisect
import json
import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta

NEVER = 1e5  # minutes since a signal the source never showed
CODES = {"SOURCE_TIMEOUT": 1, "OUT_OF_MEMORY": 2, "TARGET_UNAVAILABLE": 3, "SCHEMA_MISMATCH": 4,
         "TRANSIENT_ERROR": 5}  # fmt: skip
SIGNALS = ("unst", "lag", "warn", "srcerr", "srclate", "retry")
WINDOWS = (1, 3, 6, 24)
HOLIDAYS = {"2025-11-03", "2025-11-04"}  # days off by the production calendar, like a Sunday


def ts(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Run:
    run_id: str
    flow_id: str
    sched: datetime
    pred: datetime
    deadline: datetime


@dataclass
class Live:
    """A run whose events are coming in."""

    started: datetime | None = None
    rows: float | None = None
    bpr: float | None = None
    codes: list[str] = field(default_factory=list)
    warned: bool = False


@dataclass
class Done:
    """A finished run in its flow's history."""

    sched: datetime
    err: int
    late: int
    ratio: float
    dur: float | None
    rows: float | None
    bpr: float | None
    code: int
    oom: int
    warned: int
    slack: float


class Engine:
    def __init__(self, sources: list[str] | None = None) -> None:
        # the model's own source codes (train.py saves them): a code never changes meaning
        self.codes = {s: i for i, s in enumerate(sources or [])}
        self.runs: dict[str, Run] = {}
        self.pending: list[tuple[datetime, str, dict]] = []
        self.sorted_pending = True
        self.now: datetime | None = None
        self.live: dict[str, Live] = {}
        self.history: dict[str, list[Done]] = {}
        self.started: dict[str, int] = {}
        self.source: dict[str, str] = {}
        self.signals: dict[str, dict[str, list[datetime]]] = {s: {} for s in SIGNALS}
        self.all_errors: list[datetime] = []
        self.src_last_bad: dict[str, int] = {}
        self.plan_by_flow: dict[str, list[datetime]] = {}
        self.plan_times: list[datetime] = []

    # ── input ──
    def add_run(self, row: dict) -> None:
        run = Run(row["run_id"], row["flow_id"], ts(row["scheduled_start_at"]),
                  ts(row["prediction_at"]), ts(row["sla_deadline_at"]))  # fmt: skip
        self.runs[run.run_id] = run
        bisect.insort(self.plan_by_flow.setdefault(run.flow_id, []), run.sched)
        bisect.insort(self.plan_times, run.sched)

    def add_event(self, row: dict) -> None:
        item = (ts(row["observed_at"]), row["event_id"], row)
        if self.pending and item[:2] < self.pending[-1][:2]:
            self.sorted_pending = False
        self.pending.append(item)

    def advance(self, until: datetime) -> None:
        """Applies the pending events observed at or before ``until``, in store order."""
        if not self.sorted_pending:
            self.pending.sort(key=lambda item: item[:2])
            self.sorted_pending = True
        k = 0
        while k < len(self.pending) and self.pending[k][0] <= until:
            self._apply(self.pending[k][0], self.pending[k][2])
            k += 1
        if k:
            del self.pending[:k]
        self.now = until if self.now is None else max(self.now, until)

    def _signal(self, name: str, src: str | None, at: datetime) -> None:
        if src is not None:
            self.signals[name].setdefault(src, []).append(at)

    def _apply(self, obs: datetime, row: dict) -> None:
        kind, run_id, flow = row["event_type"], row["run_id"], row["flow_id"]
        payload = json.loads(row["payload"])
        live = self.live.setdefault(run_id, Live())
        src = self.source.get(flow)
        if kind == "run_started":
            live.started = ts(row["event_ts"])
            self.started[flow] = self.started.get(flow, 0) + 1
            if flow not in self.source and "source" in payload:
                self.source[flow] = src = str(payload["source"])
        elif kind == "extract_finished":
            rows = float(payload.get("rows") or 0)
            live.rows = rows
            live.bpr = float(payload.get("bytes") or 0) / max(1.0, rows)
        elif kind == "warning":
            live.warned = True
            self._signal("warn", src, obs)
            code = payload.get("code")
            self._signal("unst" if code == "source_unstable" else "lag", src, obs)
        elif kind == "error":
            live.codes.append(str(payload.get("code")))
            self._signal("srcerr", src, obs)
            self.all_errors.append(obs)
        elif kind == "retry_started":
            self._signal("retry", src, obs)
        elif kind == "run_finished":
            run = self.runs.get(run_id)
            if run is None:
                return
            end = ts(row["event_ts"])
            window = (run.deadline - run.sched).total_seconds()
            late = int(payload.get("status") == "failed" or end > run.deadline)
            done = Done(
                sched=run.sched,
                err=int(bool(live.codes)),
                late=late,
                ratio=(end - run.sched).total_seconds() / window,
                dur=(end - live.started).total_seconds() / 60 if live.started else None,
                rows=live.rows,
                bpr=live.bpr,
                code=CODES.get(live.codes[0], 0) if live.codes else 0,
                oom=int("OUT_OF_MEMORY" in live.codes),
                warned=int(live.warned),
                slack=window / 60,
            )
            self.history.setdefault(flow, []).append(done)
            if late:
                self._signal("srclate", src, obs)
            if src is not None:
                self.src_last_bad[src] = int(done.err or done.late)
            self.live.pop(run_id, None)

    # ── output ──
    def features(self, run: Run) -> dict[str, float | None]:
        self.advance(run.pred)
        t = run.pred
        hist = self.history.get(run.flow_id, [])
        n = len(hist)
        last5, last10 = hist[-5:], hist[-10:]
        rows = [h.rows for h in hist if h.rows is not None]
        bprs = [h.bpr for h in hist if h.bpr is not None]
        warned = [h for h in hist if h.warned]
        src = self.source.get(run.flow_id)
        plan = self.plan_by_flow.get(run.flow_id, [])
        gaps = [(b - a).total_seconds() / 3600 for a, b in zip(plan, plan[1:])]
        back = [h for h in hist if h.sched <= run.sched - timedelta(days=6.5)]
        slack = (run.deadline - run.sched).total_seconds() / 60
        day = run.sched.strftime("%Y-%m-%d")
        dim = (run.sched.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        out: dict[str, float | None] = {
            "n_hist": n,
            "err_rate": _mean([h.err for h in hist]),
            "late_rate": _mean([h.late for h in hist]),
            "err_rate10": _mean([h.err for h in last10]),
            "late_rate10": _mean([h.late for h in last10]),
            "last_err": hist[-1].err if n else None,
            "last_late": hist[-1].late if n else None,
            "ratio_last": hist[-1].ratio if n else None,
            "ratio5": _mean([h.ratio for h in last5]),
            "ratio5_max": max((h.ratio for h in last5), default=None),
            "dur_last": hist[-1].dur if n else None,
            "rows_trend": rows[-1] / statistics.median(rows[-10:]) if len(rows) >= 3 and statistics.median(rows[-10:]) else 1.0,
            "bpr_jump": bprs[-1] / statistics.median(bprs[-10:]) if len(bprs) >= 3 and statistics.median(bprs[-10:]) else 1.0,
            "running": int(self.started.get(run.flow_id, 0) > n),
            "late_streak": _streak([h.late for h in hist]),
            "err_streak": _streak([h.err for h in hist]),
            "err_when_warned": _mean([h.err for h in warned]),
            "late_when_warned": _mean([h.late for h in warned]),
            "n_warned": len(warned),
            "last_code": hist[-1].code if n else -1,
            "oom3": sum(h.oom for h in hist[-3:]),
            "ratio_week": back[-1].ratio if back else None,
            "rows_week": (back[-1].rows / statistics.median(rows)) if back and back[-1].rows is not None and rows and statistics.median(rows) else None,
            "slack_change": slack / hist[0].slack if n and hist[0].slack else 1.0,
            "src": self.codes.get(src, -1) if src else -1,
            "period_h": statistics.median(gaps) if gaps else None,
            "allerr1": _count(self.all_errors, t, 1),
            "allerr3": _count(self.all_errors, t, 3),
            "load": bisect.bisect_right(self.plan_times, run.sched + timedelta(minutes=60))
            - bisect.bisect_left(self.plan_times, run.sched - timedelta(minutes=60)) - 1,
            "hour": run.sched.hour,
            "dow": 6 if day in HOLIDAYS else run.sched.weekday(),
            "month_end": int(run.sched.day >= dim.day - 1 or run.sched.day == 1),
            "slack": slack,
            "src_last_bad": self.src_last_bad.get(src, 0) if src else 0,
        }  # fmt: skip
        for name in ("unst", "lag", "srcerr"):
            times = self.signals[name].get(src, []) if src else []
            k = bisect.bisect_right(times, t)
            out[f"since_{name}"] = (t - times[k - 1]).total_seconds() / 60 if k else NEVER
        for name in SIGNALS:
            times = self.signals[name].get(src, []) if src else []
            for w in WINDOWS:
                out[f"{name}{w}"] = _count(times, t, w)
        return {k: (None if v is None or (isinstance(v, float) and not math.isfinite(v)) else float(v))
                for k, v in out.items()}  # fmt: skip


def _mean(values: list) -> float | None:
    return sum(values) / len(values) if values else None


def _streak(values: list[int]) -> int:
    k = 0
    for v in reversed(values):
        if not v:
            break
        k += 1
    return k


def _count(times: list[datetime], t: datetime, hours: int) -> int:
    return bisect.bisect_right(times, t) - bisect.bisect_right(times, t - timedelta(hours=hours))


NAMES = [
    "n_hist", "err_rate", "late_rate", "err_rate10", "late_rate10", "last_err", "last_late",
    "ratio_last", "ratio5", "ratio5_max", "dur_last", "rows_trend", "bpr_jump", "running",
    "late_streak", "err_streak", "err_when_warned", "late_when_warned", "n_warned", "last_code",
    "oom3", "ratio_week", "rows_week", "slack_change", "src", "period_h", "allerr1", "allerr3",
    "load", "hour", "dow", "month_end", "slack", "src_last_bad", "since_unst", "since_lag",
    "since_srcerr", *(f"{s}{w}" for s in SIGNALS for w in WINDOWS),
]
