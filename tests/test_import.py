"""The import's rules on rows made here: the contract's order of checks, duplicates, keys."""

from app.store import check, validate

RUN = {"run_id": "r_1", "flow_id": "f_1", "scheduled_start_at": "2025-11-12T01:00:00Z",
       "prediction_at": "2025-11-12T00:30:00Z", "sla_deadline_at": "2025-11-12T02:00:00Z"}
EVENT = {"event_id": "e_1", "run_id": "r_1", "flow_id": "f_1", "event_ts": "2025-11-12T01:00:05Z",
         "observed_at": "2025-11-12T01:00:09Z", "event_type": "run_started", "payload": '{"source": "s"}'}


def test_the_first_broken_rule_names_the_code():
    bad = {**EVENT, "event_ts": "2025-11-12 01:00 MSK", "run_id": "r_404"}
    assert check("events", bad, {"r_1"}) == "INVALID_TIMESTAMP"
    assert check("events", {**EVENT, "run_id": "r_404"}, {"r_1"}) == "UNKNOWN_RUN"
    assert check("events", {**EVENT, "payload": "[1]"}, {"r_1"}) == "INVALID_PAYLOAD"
    assert check("runs", {**RUN, "sla_deadline_at": RUN["scheduled_start_at"]}, set()) == "INVALID_DEADLINE"


def test_a_duplicate_is_skipped_and_a_rejected_row_takes_no_key():
    rows = [{**EVENT, "flow_id": ""}, EVENT, dict(EVENT)]
    accepted, errors, skipped = validate("events", rows, set(), {"r_1"})
    assert [r["event_id"] for r in accepted] == ["e_1"]
    assert errors == [{"row": 1, "code": "MISSING_FIELD"}] and skipped == 1


def test_a_repeated_file_adds_nothing():
    accepted, _, _ = validate("runs", [RUN], set(), set())
    again, errors, skipped = validate("runs", [RUN], {r["run_id"] for r in accepted}, set())
    assert again == [] and errors == [] and skipped == 1
