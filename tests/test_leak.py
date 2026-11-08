"""Features see nothing observed after the run's moment, whatever was loaded later."""

from app.engine import Engine

PLAN = [
    {"run_id": "r_1", "flow_id": "f_1", "scheduled_start_at": "2025-11-12T01:00:00Z",
     "prediction_at": "2025-11-12T00:30:00Z", "sla_deadline_at": "2025-11-12T02:00:00Z"},
    {"run_id": "r_2", "flow_id": "f_2", "scheduled_start_at": "2025-11-12T00:00:00Z",
     "prediction_at": "2025-11-11T23:30:00Z", "sla_deadline_at": "2025-11-12T01:00:00Z"},
]


def _warning(eid: str, observed: str) -> dict:
    return {"event_id": eid, "run_id": "r_2", "flow_id": "f_2", "event_ts": observed,
            "observed_at": observed, "event_type": "warning", "payload": '{"code": "source_unstable"}'}


def _engine(events: list[dict]) -> Engine:
    engine = Engine(["s"])
    for row in PLAN:
        engine.add_run(row)
    start = {"event_id": "e_0", "run_id": "r_2", "flow_id": "f_2", "event_ts": "2025-11-12T00:00:01Z",
             "observed_at": "2025-11-12T00:00:02Z", "event_type": "run_started", "payload": '{"source": "s"}'}
    engine.add_event({**start, "flow_id": "f_1", "run_id": "r_2", "event_id": "e_00"})
    for row in [start, *events]:
        engine.add_event(row)
    return engine


def test_a_warning_after_the_moment_does_not_change_the_features():
    before = _engine([_warning("e_1", "2025-11-12T00:20:00Z")])
    after = _engine([_warning("e_1", "2025-11-12T00:20:00Z"), _warning("e_2", "2025-11-12T00:40:00Z")])
    run = before.runs["r_1"]
    assert before.features(run) == after.features(after.runs["r_1"])
    assert before.features(run)["unst1"] == 1
