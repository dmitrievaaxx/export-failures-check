"""Trains the risk model on the open 70 days and writes model/model.joblib.

    python train.py ../datasets/open

The files are checked as the service checks them (app/store.py) and replayed through the same
feature engine (app/engine.py): each run's features are taken at its prediction_at, from the
events observed by then. Training — the first 50 days, validation — the last 20. On validation
the thresholds are chosen: the pair with the fewest alarms that still catches 80 % of the errors,
of the missed deadlines and of the critical runs (a margin over the 75 % bar: the hidden period is
another 40 days).
"""

import csv
import sys
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier

from app.engine import NAMES, Engine
from app.store import parse, validate

SPLIT = datetime(2025, 10, 23)
TARGET = 0.80
CATEGORICAL = [NAMES.index("last_code"), NAMES.index("src")]


def load(folder: Path) -> dict[str, list[dict]]:
    keys: dict[str, set] = {"runs": set(), "events": set(), "outcomes": set()}
    out = {}
    for name in ("runs", "events", "outcomes"):
        rows = parse(name, (folder / f"{name}.csv").read_bytes())
        accepted, _, _ = validate(name, rows, keys[name], keys["runs"])
        keys[name] |= {r["event_id" if name == "events" else "run_id"] for r in accepted}
        out[name] = accepted
    return out


def recall(flag: np.ndarray, y: np.ndarray) -> float:
    return float((flag & (y == 1)).sum() / max(1, (y == 1).sum()))


def thresholds(pe, ps, ye, ys) -> tuple[float, float]:
    yc = ye | ys
    best = None
    for a in np.unique(np.quantile(pe, np.linspace(0.3, 0.998, 90))):
        fe = pe >= a
        if recall(fe, ye) < TARGET:
            continue
        for b in np.unique(np.quantile(ps, np.linspace(0.3, 0.998, 90))):
            fs = ps >= b
            if recall(fs, ys) < TARGET or recall(fe | fs, yc) < TARGET:
                continue
            if best is None or (fe | fs).sum() < best[0]:
                best = ((fe | fs).sum(), float(a), float(b))
    if best is None:
        raise SystemExit("no thresholds reach the target on validation")
    return best[1], best[2]


def main() -> None:
    data = load(Path(sys.argv[1]))
    sources = sorted({__import__("json").loads(e["payload"]).get("source")
                      for e in data["events"] if e["event_type"] == "run_started"} - {None})  # fmt: skip
    engine = Engine(sources)
    for row in data["runs"]:
        engine.add_run(row)
    for row in data["events"]:
        engine.add_event(row)
    labels = {o["run_id"]: (int(o["y_error"]), int(o["y_sla_breach"])) for o in data["outcomes"]}
    runs = sorted((r for r in engine.runs.values() if r.run_id in labels), key=lambda r: (r.pred, r.run_id))
    rows = [engine.features(r) for r in runs]
    x = np.array([[np.nan if f[n] is None else f[n] for n in NAMES] for f in rows], dtype=float)
    ye = np.array([labels[r.run_id][0] for r in runs])
    ys = np.array([labels[r.run_id][1] for r in runs])
    tr = np.array([r.sched < SPLIT for r in runs])
    va = ~tr
    models = {}
    for name, y in (("error", ye), ("sla", ys)):
        m = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, max_depth=4,
                                           min_samples_leaf=40, l2_regularization=1.0,
                                           random_state=0, categorical_features=CATEGORICAL)  # fmt: skip
        models[name] = m.fit(x[tr], y[tr])
    pe = models["error"].predict_proba(x[va])[:, 1]
    ps = models["sla"].predict_proba(x[va])[:, 1]
    a, b = thresholds(pe, ps, ye[va], ys[va])
    fe, fs = pe >= a, ps >= b
    alarm, yc = fe | fs, ye[va] | ys[va]
    print(f"train {tr.sum()} runs, validation {va.sum()} runs")
    print(f"validation: recall alarm {recall(alarm, yc):.3f}, error {recall(fe, ye[va]):.3f}, "
          f"sla {recall(fs, ys[va]):.3f}, precision {float((alarm & (yc == 1)).sum() / max(1, alarm.sum())):.3f}, "
          f"thresholds error {a:.4f} sla {b:.4f}")  # fmt: skip
    target = Path(__file__).resolve().parent / "model" / "model.joblib"
    target.parent.mkdir(exist_ok=True)
    joblib.dump({"error": models["error"], "sla": models["sla"], "names": NAMES, "sources": sources,
                 "thresholds": {"error": round(a, 6), "sla": round(b, 6)}, "version": "2025-11-11"},
                target, compress=3)  # fmt: skip
    print(target, target.stat().st_size, "bytes")


if __name__ == "__main__":
    main()
