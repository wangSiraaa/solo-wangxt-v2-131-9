from datetime import timedelta

from fastapi.testclient import TestClient

from app.database import SessionLocal
from app.models import AnalysisTask, Report, utcnow
from app.pipeline import execute_task
from tests.synthetic import CHANNELS, create_and_run_task, create_manifest, make_calibration, make_chunks, upload_chunks

def completed_recording(client):
    chunks = make_chunks(sample_chunks=(300, 420))
    manifest = create_manifest(client, chunks)
    upload_chunks(client, manifest["id"], chunks, order=[1, 0])
    client.post(f"/manifests/{manifest['id']}/finalize")
    calibration = make_calibration(client, manifest["channel_set_hash"])
    return manifest, calibration


def test_task_fixes_manifest_calibration_and_params_and_publishes_once(client: TestClient):
    manifest, calibration = completed_recording(client)
    task_id = create_and_run_task(client, manifest["id"], calibration["id"])
    second_run = execute_task(task_id)
    assert second_run["published"] is False
    with SessionLocal() as db:
        assert db.query(Report).filter(Report.task_id == task_id).count() == 1


def test_spectrum_crash_preserves_stage_results_but_publishes_no_report(client: TestClient):
    manifest, calibration = completed_recording(client)
    response = client.post(
        "/analysis-tasks",
        json={"manifest_id": manifest["id"], "calibration_version_id": calibration["id"]},
    )
    task_id = response.json()["id"]
    outcome = execute_task(task_id, crash_after_stage="spectrum")
    assert outcome["status"] == "retry_wait"
    assert outcome["published"] is False
    with SessionLocal() as db:
        task = db.get(AnalysisTask, task_id)
        assert "spectrum" in task.stage_results
        assert task.error_code == "spectrum_crash"
        assert db.query(Report).filter(Report.task_id == task_id).count() == 0


def test_worker_restart_lease_then_retry_does_not_duplicate_report(client: TestClient):
    manifest, calibration = completed_recording(client)
    task_id = create_and_run_task(client, manifest["id"], calibration["id"])
    with SessionLocal() as db:
        task = db.get(AnalysisTask, task_id)
        task.status = "running"
        task.lease_owner = "dead-worker"
        task.lease_until = utcnow() - timedelta(seconds=1)
        task.attempts = 1
        db.commit()
    recovered = client.post("/maintenance/recover-stale-tasks").json()
    assert recovered["recovered"] == 1
    execute_task(task_id)
    with SessionLocal() as db:
        task = db.get(AnalysisTask, task_id)
        assert task.attempts == 2
        assert db.query(Report).filter(Report.task_id == task_id).count() == 1


def test_cancel_before_run_marks_task_cancelled(client: TestClient):
    manifest, calibration = completed_recording(client)
    response = client.post(
        "/analysis-tasks",
        json={"manifest_id": manifest["id"], "calibration_version_id": calibration["id"]},
    )
    task_id = response.json()["id"]
    cancelled = client.post(f"/analysis-tasks/{task_id}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"
    outcome = execute_task(task_id)
    assert outcome["published"] is False


def test_calibration_update_marks_old_report_needs_review_without_mixing_coefficients(client: TestClient):
    manifest, calibration = completed_recording(client)
    task_id = create_and_run_task(client, manifest["id"], calibration["id"])
    old_report = client.get(f"/reports?manifest_id={manifest['id']}").json()[0]
    assert old_report["calibration_version_id"] == calibration["id"]

    updated = client.post(
        "/calibrations",
        json={
            "channel_set_hash": manifest["channel_set_hash"],
            "coefficients": {
                channel: {"gain": 1.05 if channel == "Va" else 1.0, "offset": 0, "phase_shift_rad": 0}
                for channel in CHANNELS
            },
            "change_note": "revised gain",
        },
    ).json()
    old_report = client.get(f"/reports/{old_report['id']}").json()
    assert old_report["status"] == "needs_review"
    assert old_report["superseded_by_calibration_id"] == updated["id"]
    # Snapshot inside old report remains immutable and still names old version.
    assert old_report["result"]["fixed_snapshot"]["calibration_version_id"] == calibration["id"]


def test_non_integer_cycle_is_warning_but_report_completes(client: TestClient):
    chunks = make_chunks(sample_chunks=(600,))
    manifest = create_manifest(client, chunks)
    upload_chunks(client, manifest["id"], chunks)
    client.post(f"/manifests/{manifest['id']}/finalize")
    calibration = make_calibration(client, manifest["channel_set_hash"])
    task_id = create_and_run_task(client, manifest["id"], calibration["id"])
    task = client.get(f"/analysis-tasks/{task_id}").json()
    report = client.get(f"/reports?manifest_id={manifest['id']}").json()[0]
    assert task["status"] == "succeeded"
    assert report["status"] == "published"
    assert any(item["code"] == "non_integer_cycle" for item in report["result"]["quality"])
    assert report["result"]["quality_status"] == "warning"


def test_saturation_is_quality_state(client: TestClient):
    chunks = make_chunks(sample_chunks=(720,), saturation=True)
    manifest = create_manifest(client, chunks)
    upload_chunks(client, manifest["id"], chunks)
    client.post(f"/manifests/{manifest['id']}/finalize")
    coefficients = {
        channel: {
            "gain": 1,
            "offset": 0,
            "phase_shift_rad": 0,
            "saturation_low": -400 if channel == "Va" else None,
            "saturation_high": 400 if channel == "Va" else None,
        }
        for channel in CHANNELS
    }
    calibration = client.post(
        "/calibrations",
        json={"channel_set_hash": manifest["channel_set_hash"], "coefficients": coefficients},
    ).json()
    response = client.post(
        "/analysis-tasks",
        json={
            "manifest_id": manifest["id"],
            "calibration_version_id": calibration["id"],
            "params": {"saturation_warning_fraction": 1e-9},
        },
    )
    task_id = response.json()["id"]
    outcome = client.post(f"/analysis-tasks/{task_id}/run").json()
    assert outcome["published"] is True
    task = client.get(f"/analysis-tasks/{task_id}").json()
    assert task["status"] == "succeeded"
    report = client.get(f"/reports?manifest_id={manifest['id']}").json()[0]
    assert any(item["code"] == "saturation" for item in report["result"]["quality"])
    assert report["result"]["quality_status"] == "warning"


def test_missing_phase_creates_diagnostic_failed_report_not_published(client: TestClient):
    channels = ["Va", "Vb"]
    chunks = make_chunks(sample_chunks=(720,), channels=channels)
    manifest = create_manifest(client, chunks)
    upload_chunks(client, manifest["id"], chunks)
    client.post(f"/manifests/{manifest['id']}/finalize")
    incomplete_coefficients = {
        "Va": {"gain": 1, "offset": 0, "phase_shift_rad": 0},
        "Vb": {"gain": 1, "offset": 0, "phase_shift_rad": 0},
    }
    calibration = client.post(
        "/calibrations",
        json={"channel_set_hash": manifest["channel_set_hash"], "coefficients": incomplete_coefficients},
    ).json()
    response = client.post(
        "/analysis-tasks",
        json={"manifest_id": manifest["id"], "calibration_version_id": calibration["id"]},
    )
    task_id = response.json()["id"]
    outcome = client.post(f"/analysis-tasks/{task_id}/run").json()
    assert outcome["published"] is False
    task = client.get(f"/analysis-tasks/{task_id}").json()
    assert task["status"] == "failed"
    report = client.get(f"/reports?manifest_id={manifest['id']}").json()[0]
    assert report["status"] == "diagnostic_failed"


# ---------------------------------------------------------------------------
# Acceptance: task trajectory, lease takeover fencing and controlled retry
# ---------------------------------------------------------------------------


def test_task_trace_exposes_stages_lease_heartbeat_attempts_and_failure_reason(client: TestClient):
    manifest, calibration = completed_recording(client)
    response = client.post(
        "/analysis-tasks",
        json={"manifest_id": manifest["id"], "calibration_version_id": calibration["id"]},
    )
    task_id = response.json()["id"]
    execute_task(task_id, crash_after_stage="spectrum")

    task = client.get(f"/analysis-tasks/{task_id}").json()
    assert task["status"] == "retry_wait"
    assert task["attempts"] == 1
    assert task["error_code"] == "spectrum_crash"
    assert task["error_message"]
    assert task["heartbeat_at"] is not None
    # Timeline carries queued -> running lease acquisition -> each stage -> failure.
    kinds = [event["kind"] for event in task["events"]]
    statuses = [event.get("status") for event in task["events"]]
    assert kinds[0] == "status" and statuses[0] == "queued"
    assert "lease_acquired" in kinds
    assert kinds.count("stage") == 5
    assert statuses[-1] == "retry_wait"
    # Each persisted stage carries its own timestamp.
    for stage in ("fixed_snapshot", "raw_inventory", "raw_bytes", "segmentation", "spectrum"):
        assert task["stage_results"][stage]["at"]
    advice = (task["retry_allowed"], task["retry_reason"])
    assert advice[0] is True


def test_expired_lease_is_taken_over_and_old_worker_writeback_is_rejected(client: TestClient):
    manifest, calibration = completed_recording(client)
    response = client.post(
        "/analysis-tasks",
        json={"manifest_id": manifest["id"], "calibration_version_id": calibration["id"]},
    )
    task_id = response.json()["id"]
    # Simulate a worker that started and died while holding its lease.
    outcome = execute_task(task_id, lease_seconds=1, crash_after_stage="raw_inventory")
    assert outcome["status"] == "retry_wait"
    with SessionLocal() as db:
        task = db.get(AnalysisTask, task_id)
        dead_generation = task.lease_generation
        task.status = "running"
        task.lease_owner = "dead-worker"
        task.lease_until = utcnow() - timedelta(seconds=1)
        db.commit()

    # The old worker later "wakes up" after its lease expired but before any
    # takeover: publication is refused and the task is parked in retry_wait.
    from app.pipeline import complete_task

    with SessionLocal() as db:
        rejected = complete_task(
            db,
            task_id,
            dead_generation,
            manifest_id=manifest["id"],
            calibration_id=calibration["id"],
            result={"quality_status": "ok", "snapshot_digest": "stale"},
        )
    assert rejected["status"] == "retry_wait"
    assert rejected["published"] is False
    with SessionLocal() as db:
        assert db.query(Report).filter(Report.task_id == task_id).count() == 0

    # Re-arm the dead worker state, then let a new owner take the lease over.
    with SessionLocal() as db:
        task = db.get(AnalysisTask, task_id)
        task.status = "running"
        task.lease_owner = "dead-worker"
        task.lease_until = utcnow() - timedelta(seconds=1)
        db.commit()

    # A new worker can take the expired lease over and finish successfully.
    execute_task(task_id, lease_seconds=900)
    with SessionLocal() as db:
        task = db.get(AnalysisTask, task_id)
        assert task.status == "succeeded"
        assert task.attempts == 2
        assert task.lease_generation == dead_generation + 1
        kinds = [event["kind"] for event in task.events]
        assert "lease_taken" in kinds
        taken = next(event for event in task.events if event["kind"] == "lease_taken")
        assert taken["previous_owner"] == "dead-worker"
        assert db.query(Report).filter(Report.task_id == task_id).count() == 1

    # The dead worker's late write is fenced off after the new owner won.
    with SessionLocal() as db:
        rejected_again = complete_task(
            db,
            task_id,
            dead_generation,
            manifest_id=manifest["id"],
            calibration_id=calibration["id"],
            result={"quality_status": "ok", "snapshot_digest": "stale"},
        )
    assert rejected_again["status"] == "lease_lost"
    assert rejected_again["published"] is False
    with SessionLocal() as db:
        assert db.query(Report).filter(Report.task_id == task_id).count() == 1


def test_repeated_retry_clicks_never_produce_more_than_one_report(client: TestClient):
    import concurrent.futures

    manifest, calibration = completed_recording(client)
    response = client.post(
        "/analysis-tasks",
        json={"manifest_id": manifest["id"], "calibration_version_id": calibration["id"]},
    )
    task_id = response.json()["id"]
    execute_task(task_id, crash_after_stage="spectrum")
    assert client.get(f"/analysis-tasks/{task_id}").json()["status"] == "retry_wait"

    # Hammer the controlled retry endpoint; every call is a guarded no-op after
    # the first transition and attempts are only incremented at acquisition.
    def click(_):
        return client.post(f"/analysis-tasks/{task_id}/retry").status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        codes = list(executor.map(click, range(16)))
    assert codes.count(200) == 1
    assert all(code in (200, 409) for code in codes)

    # Two workers race to execute after takeover: lease fencing + the unique
    # reports.task_id constraint together allow exactly one report.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _: execute_task(task_id), range(2)))
    assert sum(1 for item in outcomes if item.get("published")) == 1
    with SessionLocal() as db:
        assert db.query(Report).filter(Report.task_id == task_id).count() == 1


def test_retry_reuses_frozen_snapshot_without_creating_a_new_task(client: TestClient):
    manifest, calibration = completed_recording(client)
    task_id = create_and_run_task(client, manifest["id"], calibration["id"])
    with SessionLocal() as db:
        original = db.get(AnalysisTask, task_id)
        snapshot_digest_before = original.manifest_snapshot
        assert original.report.status == "published"

    # A published report blocks retry entirely: the controlled entry is a 409.
    blocked = client.post(f"/analysis-tasks/{task_id}/retry")
    assert blocked.status_code == 409

    # A retry_wait task keeps the exact same frozen snapshot across attempts.
    response = client.post(
        "/analysis-tasks",
        json={"manifest_id": manifest["id"], "calibration_version_id": calibration["id"]},
    )
    second = response.json()["id"]
    execute_task(second, crash_after_stage="spectrum")
    with SessionLocal() as db:
        before = db.get(AnalysisTask, second)
        frozen_before = dict(before.manifest_snapshot)
    client.post(f"/analysis-tasks/{second}/retry")
    execute_task(second)
    with SessionLocal() as db:
        after = db.get(AnalysisTask, second)
        assert after.manifest_snapshot == frozen_before
        assert after.calibration_version_id == calibration["id"]
        assert after.status == "succeeded"
        assert db.query(Report).filter(Report.task_id == second).count() == 1
    assert snapshot_digest_before  # original untouched


def test_spectrum_failure_shows_only_diagnostics_never_a_fake_normal_report(client: TestClient):
    manifest, calibration = completed_recording(client)
    response = client.post(
        "/analysis-tasks",
        json={"manifest_id": manifest["id"], "calibration_version_id": calibration["id"]},
    )
    task_id = response.json()["id"]
    outcome = execute_task(task_id, crash_after_stage="spectrum")
    assert outcome["published"] is False

    reports = client.get(f"/reports?manifest_id={manifest['id']}").json()
    assert reports == []  # a crashed spectrum stage publishes nothing, not even a diagnostic row
    task = client.get(f"/analysis-tasks/{task_id}").json()
    # The diagnostics live on the task trajectory, not on a fake normal report.
    assert task["status"] == "retry_wait"
    assert task["stage_results"]["spectrum"]["quality_status"] in {"ok", "warning"}
    assert task["error_code"] == "spectrum_crash"
    assert task["retry_allowed"] is True


def test_quality_error_retry_is_allowed_and_turns_diagnostic_row_into_one_report(client: TestClient):
    channels = ["Va", "Vb"]
    chunks = make_chunks(sample_chunks=(720,), channels=channels)
    manifest = create_manifest(client, chunks)
    upload_chunks(client, manifest["id"], chunks)
    client.post(f"/manifests/{manifest['id']}/finalize")
    calibration = client.post(
        "/calibrations",
        json={
            "channel_set_hash": manifest["channel_set_hash"],
            "coefficients": {
                "Va": {"gain": 1, "offset": 0, "phase_shift_rad": 0},
                "Vb": {"gain": 1, "offset": 0, "phase_shift_rad": 0},
            },
        },
    ).json()
    task_id = client.post(
        "/analysis-tasks",
        json={"manifest_id": manifest["id"], "calibration_version_id": calibration["id"]},
    ).json()["id"]
    execute_task(task_id)
    report = client.get(f"/reports?manifest_id={manifest['id']}").json()[0]
    assert report["status"] == "diagnostic_failed"
    task = client.get(f"/analysis-tasks/{task_id}").json()
    assert task["status"] == "failed"
    # The diagnostic row is not a published report, so the controlled retry is open.
    assert task["retry_allowed"] is True
