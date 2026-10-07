"""Acceptance tests for the task run trajectory, controlled retry and lease fencing.

Covers the three acceptance points:
- an expired lease can be taken over and every late write from the old worker
  (stage results, heartbeats, failure/terminal transitions, report insert) is
  rejected;
- double-clicking retry is idempotent and still produces at most one report;
- a spectrum-stage failure leaves diagnostics only, never a pseudo-normal report.
"""

from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import update

from app import pipeline
from app.database import SessionLocal
from app.models import AnalysisTask, Report, utcnow
from app.pipeline import LeaseLost, acquire_task, execute_task, heartbeat, save_stage, _fail
from tests.synthetic import (
    CHANNELS,
    completed_recording,
    create_manifest,
    make_calibration,
    make_chunks,
    upload_chunks,
)


def create_task(client: TestClient, manifest_id: str, calibration_id: str) -> str:
    response = client.post(
        "/analysis-tasks",
        json={"manifest_id": manifest_id, "calibration_version_id": calibration_id},
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


def expire_lease(task_id: str) -> None:
    with SessionLocal() as db:
        db.execute(
            update(AnalysisTask)
            .where(AnalysisTask.id == task_id)
            .values(lease_until=utcnow() - timedelta(seconds=1))
        )
        db.commit()


def test_expired_lease_takeover_and_stale_worker_write_rejected(client: TestClient):
    manifest, calibration = completed_recording(client)
    task_id = create_task(client, manifest["id"], calibration["id"])

    # Worker A picks up the task and stalls (no heartbeat, lease runs out).
    with SessionLocal() as db:
        task = acquire_task(db, task_id, lease_seconds=30, worker_id="worker-a")
        db.commit()
        assert task is not None and task.status == "running"

    expire_lease(task_id)

    # Worker B takes over the expired lease and completes the run.
    outcome = execute_task(task_id, worker_id="worker-b")
    assert outcome["status"] == "succeeded"
    assert outcome["published"] is True

    # The stale worker A wakes up: every write-back must be rejected.
    with SessionLocal() as db:
        task = db.get(AnalysisTask, task_id)
        with pytest.raises(LeaseLost):
            save_stage(db, task, "spectrum", {"quality_status": "ok"}, worker_id="worker-a")
        with pytest.raises(LeaseLost):
            heartbeat(db, task_id, "worker-a")
        outcome = _fail(db, task, "spectrum_crash", "late failure from stale worker", fatal=False, worker_id="worker-a")
        assert outcome["status"] == "lease_lost"
        assert outcome["published"] is False

    with SessionLocal() as db:
        task = db.get(AnalysisTask, task_id)
        assert task.status == "succeeded"
        assert task.error_code is None
        assert db.query(Report).filter(Report.task_id == task_id).count() == 1

    timeline = client.get(f"/analysis-tasks/{task_id}/timeline").json()
    takeovers = [event for event in timeline["events"] if event["event"] == "lease_recovered"]
    assert len(takeovers) == 1
    assert takeovers[0]["detail"]["worker_id"] == "worker-b"
    assert takeovers[0]["detail"]["attempt"] == 2


def test_stale_worker_report_rolled_back_when_lease_lost_at_publish(client: TestClient, monkeypatch):
    manifest, calibration = completed_recording(client)
    task_id = create_task(client, manifest["id"], calibration["id"])

    original_publish = pipeline.publish_report

    def hostile_publish(db, task, manifest_obj, calibration_obj, result):
        # A rival worker takes the lease after the last stage but before the
        # terminal commit of the stale worker.
        with SessionLocal() as other:
            other.execute(
                update(AnalysisTask)
                .where(AnalysisTask.id == task.id)
                .values(lease_owner="worker-b", lease_until=utcnow() + timedelta(seconds=900))
            )
            other.commit()
        return original_publish(db, task, manifest_obj, calibration_obj, result)

    monkeypatch.setattr(pipeline, "publish_report", hostile_publish)
    outcome = execute_task(task_id, worker_id="worker-a")
    assert outcome["status"] == "lease_lost"
    assert outcome["published"] is False

    with SessionLocal() as db:
        task = db.get(AnalysisTask, task_id)
        # The fenced terminal transition matched nothing, the pending report
        # insert was rolled back, and the rival's state is untouched.
        assert db.query(Report).filter(Report.task_id == task_id).count() == 0
        assert task.lease_owner == "worker-b"
        assert task.status == "running"


def test_double_retry_click_still_publishes_exactly_one_report(client: TestClient):
    manifest, calibration = completed_recording(client)
    task_id = create_task(client, manifest["id"], calibration["id"])
    outcome = execute_task(task_id, crash_after_stage="spectrum")
    assert outcome["status"] == "retry_wait"

    # Impatient user double-clicks retry: both calls succeed and are idempotent.
    for _ in range(2):
        response = client.post(f"/analysis-tasks/{task_id}/retry")
        assert response.status_code == 200
        assert response.json()["status"] == "retry_wait"

    # Two workers race for the requeued task; only one can publish.
    first = execute_task(task_id, worker_id="worker-1")
    second = execute_task(task_id, worker_id="worker-2")
    assert first["published"] is True
    assert second["published"] is False

    with SessionLocal() as db:
        task = db.get(AnalysisTask, task_id)
        assert task.status == "succeeded"
        assert db.query(Report).filter(Report.task_id == task_id).count() == 1

    # Retrying a succeeded task with a published report is rejected.
    response = client.post(f"/analysis-tasks/{task_id}/retry")
    assert response.status_code == 409


def test_retry_rejected_while_lease_active_and_allowed_after_expiry(client: TestClient):
    manifest, calibration = completed_recording(client)
    task_id = create_task(client, manifest["id"], calibration["id"])
    with SessionLocal() as db:
        task = acquire_task(db, task_id, lease_seconds=900, worker_id="worker-a")
        db.commit()
        assert task is not None

    # Lease still held: a retry would race the live worker, so it is refused.
    response = client.post(f"/analysis-tasks/{task_id}/retry")
    assert response.status_code == 409
    with SessionLocal() as db:
        assert db.get(AnalysisTask, task_id).status == "running"

    expire_lease(task_id)
    response = client.post(f"/analysis-tasks/{task_id}/retry")
    assert response.status_code == 200
    assert response.json()["status"] == "retry_wait"

    outcome = execute_task(task_id, worker_id="worker-b")
    assert outcome["published"] is True
    with SessionLocal() as db:
        assert db.query(Report).filter(Report.task_id == task_id).count() == 1


def test_timeline_records_full_successful_run(client: TestClient):
    manifest, calibration = completed_recording(client)
    task_id = create_task(client, manifest["id"], calibration["id"])
    outcome = client.post(f"/analysis-tasks/{task_id}/run").json()
    assert outcome["published"] is True

    timeline = client.get(f"/analysis-tasks/{task_id}/timeline").json()
    task = timeline["task"]
    assert task["status"] == "succeeded"
    assert task["attempts"] == 1
    assert task["heartbeat_at"] is not None
    assert task["lease_owner"] is None  # released on completion

    events = [(event["event"], event["status"]) for event in timeline["events"]]
    assert events[0] == ("queued", "queued")
    assert ("running", "running") in events
    stages = [event["detail"]["stage"] for event in timeline["events"] if event["event"] == "stage"]
    assert stages == ["fixed_snapshot", "raw_inventory", "raw_bytes", "segmentation", "spectrum"]
    assert events[-1] == ("succeeded", "succeeded")
    assert timeline["report"]["status"] == "published"


def test_spectrum_crash_timeline_shows_diagnostics_and_no_report(client: TestClient):
    manifest, calibration = completed_recording(client)
    task_id = create_task(client, manifest["id"], calibration["id"])
    outcome = execute_task(task_id, crash_after_stage="spectrum")
    assert outcome["status"] == "retry_wait"

    timeline = client.get(f"/analysis-tasks/{task_id}/timeline").json()
    task = timeline["task"]
    assert task["status"] == "retry_wait"
    assert task["error_code"] == "spectrum_crash"
    assert task["attempts"] == 1
    # Diagnostics only: stage results exist, but no report row at all.
    assert timeline["report"] is None
    assert "spectrum" in task["stage_results"]

    events = timeline["events"]
    assert events[-1]["event"] == "retry_wait"
    assert events[-1]["detail"]["error_code"] == "spectrum_crash"
    assert not any(event["event"] == "succeeded" for event in events)
    stages = [event["detail"]["stage"] for event in events if event["event"] == "stage"]
    assert stages == ["fixed_snapshot", "raw_inventory", "raw_bytes", "segmentation", "spectrum"]


def test_quality_error_timeline_marks_failed_with_diagnostic_report(client: TestClient):
    channels = ["Va", "Vb"]
    chunks = make_chunks(sample_chunks=(720,), channels=channels)
    manifest = create_manifest(client, chunks)
    upload_chunks(client, manifest["id"], chunks)
    client.post(f"/manifests/{manifest['id']}/finalize")
    calibration = client.post(
        "/calibrations",
        json={
            "channel_set_hash": manifest["channel_set_hash"],
            "coefficients": {channel: {"gain": 1, "offset": 0, "phase_shift_rad": 0} for channel in channels},
        },
    ).json()
    task_id = create_task(client, manifest["id"], calibration["id"])
    outcome = client.post(f"/analysis-tasks/{task_id}/run").json()
    assert outcome["published"] is False

    timeline = client.get(f"/analysis-tasks/{task_id}/timeline").json()
    assert timeline["task"]["status"] == "failed"
    assert timeline["task"]["error_code"] == "quality_error"
    report = timeline["report"]
    assert report["status"] == "diagnostic_failed"
    assert report["published_at"] is None
    terminal = timeline["events"][-1]
    assert terminal["event"] == "failed"
    assert terminal["detail"]["published"] is False
    assert terminal["detail"]["report_status"] == "diagnostic_failed"
