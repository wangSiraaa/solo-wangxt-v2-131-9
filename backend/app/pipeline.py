from __future__ import annotations

import hashlib
import os
import tempfile
import uuid
from datetime import timedelta
from pathlib import Path

from sqlalchemy import and_, case, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .config import get_settings
from .database import SessionLocal
from .dsp import analyze_all_segments, group_chunks_by_rate
from .models import AnalysisTask, CalibrationVersion, Chunk, Manifest, Report, utcnow
from .services import snapshot_digest
from .storage import get_object_store

WORKER_ID = os.environ.get("PQ_WORKER_ID", f"worker-{uuid.uuid4().hex[:8]}")


class TaskCancelled(RuntimeError):
    pass


class SpectrumCrash(RuntimeError):
    """Synthetic crash used by the acceptance suite to prove no partial report is published."""


class LeaseLost(RuntimeError):
    """Raised when a worker writes after its lease generation has been superseded.

    The write boundary is the conditional UPDATE keyed on lease_generation;
    this exception only carries the decision so the stale worker stops cleanly
    and never overwrites the new owner's state.
    """


def acquire_task(db: Session, task_id: str, lease_seconds: int, worker_id: str = WORKER_ID) -> AnalysisTask | None:
    now = utcnow()
    lease_until = now + timedelta(seconds=lease_seconds)
    result = db.execute(
        update(AnalysisTask)
        .where(
            AnalysisTask.id == task_id,
            AnalysisTask.status.in_(["queued", "retry_wait"]),
            AnalysisTask.cancellation_requested.is_(False),
        )
        .values(
            status="running",
            lease_owner=worker_id,
            lease_until=lease_until,
            heartbeat_at=now,
            started_at=now,
            attempts=AnalysisTask.attempts + 1,
            lease_generation=AnalysisTask.lease_generation + 1,
            retry_request_count=0,
        )
        .execution_options(synchronize_session=False)
    )
    if result.rowcount:
        db.flush()
        task = db.get(AnalysisTask, task_id)
        task.append_event(
            "lease_acquired",
            status="running",
            owner=worker_id,
            lease_until=lease_until.isoformat(),
            attempt=task.attempts,
            generation=task.lease_generation,
        )
        db.commit()
        task = db.get(AnalysisTask, task_id)
        db.refresh(task)
        return task
    # Reclaim a crashed worker's stale lease. The conditional update is the
    # fencing boundary: exactly one new owner can take over, and the bumped
    # generation makes every later write from the old owner a no-op.
    previous = db.scalar(
        select(AnalysisTask).where(AnalysisTask.id == task_id).limit(1)
    )
    result = db.execute(
        update(AnalysisTask)
        .where(
            AnalysisTask.id == task_id,
            AnalysisTask.status == "running",
            AnalysisTask.lease_until < now,
        )
        .values(
            status="running",
            lease_owner=worker_id,
            lease_until=lease_until,
            heartbeat_at=now,
            started_at=now,
            attempts=AnalysisTask.attempts + 1,
            lease_generation=AnalysisTask.lease_generation + 1,
            retry_request_count=0,
        )
        .execution_options(synchronize_session=False)
    )
    if result.rowcount:
        db.flush()
        task = db.get(AnalysisTask, task_id)
        task.append_event(
            "lease_taken",
            status="running",
            previous_owner=previous.lease_owner if previous else None,
            owner=worker_id,
            lease_until=lease_until.isoformat(),
            attempt=task.attempts,
            generation=task.lease_generation,
        )
        db.commit()
        task = db.get(AnalysisTask, task_id)
        db.refresh(task)
        return task
    return None


def request_cancel(db: Session, task: AnalysisTask) -> AnalysisTask:
    task.cancellation_requested = True
    if task.status in {"queued", "retry_wait"}:
        task.status = "cancelled"
        task.ended_at = utcnow()
        task.lease_owner = None
        task.lease_until = None
        task.append_event("status", status="cancelled", message="cancellation requested before execution")
    else:
        task.append_event("cancel_requested", status=task.status)
    db.flush()
    return task


def request_retry(db: Session, task: AnalysisTask) -> AnalysisTask:
    """Atomically move a task back to retry_wait, reusing its frozen snapshot.

    The transition is a single conditional UPDATE: it succeeds at most once per
    worker attempt because it also increments retry_request_count and is gated
    on retry_request_count < attempts. Concurrent UI clicks therefore serialize
    at the database — only one returns 200, the rest affect zero rows. Bumping
    the lease generation fences off any worker still holding a stale lease. The
    same frozen snapshot, calibration and params are reused; no new task or
    manifest version is created.
    """
    if task.report is not None and task.report.status in {"published", "needs_review"}:
        raise ValueError("a published report cannot be retried; create a new fixed task")
    now = utcnow()
    previous_status = task.status
    previous_code = task.error_code
    previous_message = task.error_message
    previous_attempts = task.attempts
    rowcount = db.execute(
        update(AnalysisTask)
        .where(
            AnalysisTask.id == task.id,
            AnalysisTask.status.in_(["retry_wait", "failed", "running"]),
            or_(
                and_(
                    AnalysisTask.status.in_(["retry_wait", "failed"]),
                    AnalysisTask.retry_request_count < AnalysisTask.attempts,
                ),
                and_(
                    AnalysisTask.status == "running",
                    AnalysisTask.lease_until < now,
                ),
            ),
        )
        .values(
            status="retry_wait",
            cancellation_requested=False,
            lease_owner=None,
            lease_until=None,
            lease_generation=AnalysisTask.lease_generation + 1,
            # Only a failed-attempt retry consumes a manual retry slot; taking
            # over an expired running lease does not.
            retry_request_count=case(
                (AnalysisTask.status == "running", AnalysisTask.retry_request_count),
                else_=AnalysisTask.retry_request_count + 1,
            ),
            error_code=None,
            error_message=None,
            ended_at=None,
        )
        .execution_options(synchronize_session=False)
    ).rowcount
    if not rowcount:
        db.rollback()
        raise ValueError(
            "retry is not available (already requested, lease still live, or task in a terminal state)"
        )
    db.flush()
    task = db.get(AnalysisTask, task.id)
    task.append_event(
        "retry_requested",
        status="retry_wait",
        message="manual retry requested; frozen snapshot is reused",
        previous_status=previous_status,
        previous_error_code=previous_code,
        previous_error_message=previous_message,
        attempts=previous_attempts,
    )
    db.flush()
    return task


def recover_stale_tasks(db: Session) -> int:
    now = utcnow()
    stale = db.scalars(
        select(AnalysisTask)
        .where(AnalysisTask.status == "running", AnalysisTask.lease_until < now)
        .order_by(AnalysisTask.requested_at)
    ).all()
    for task in stale:
        previous_owner = task.lease_owner
        task.status = "retry_wait"
        task.lease_owner = None
        task.lease_until = None
        task.lease_generation = task.lease_generation + 1
        # The manual retry slot belongs to the (failed) attempt that lost the
        # lease; reopening it lets the lab trigger the takeover from the UI.
        task.retry_request_count = 0
        task.append_event(
            "lease_recovered",
            status="retry_wait",
            previous_owner=previous_owner,
            message="stale lease reclaimed by maintenance recovery",
        )
    db.commit()
    return len(stale)


def save_stage(
    db: Session,
    task: AnalysisTask,
    stage: str,
    value: dict,
    *,
    generation: int,
    lease_seconds: int,
    status: str = "running",
) -> None:
    """Persist a stage result with a fenced heartbeat/lease renewal.

    The heartbeat is a conditional UPDATE keyed on lease_generation and a
    non-expired lease. After a takeover the old worker's renewal affects zero
    rows and raises LeaseLost, so the stale pipeline stops before it can write
    stage results or publish anything.
    """
    now = utcnow()
    renewed_until = now + timedelta(seconds=lease_seconds)
    rowcount = db.execute(
        update(AnalysisTask)
        .where(
            AnalysisTask.id == task.id,
            AnalysisTask.lease_generation == generation,
            (AnalysisTask.lease_until.is_(None)) | (AnalysisTask.lease_until > now),
        )
        .values(status=status, heartbeat_at=now, lease_until=renewed_until)
        .execution_options(synchronize_session=False)
    ).rowcount
    if not rowcount:
        db.rollback()
        raise LeaseLost("lease was taken over or expired before stage write; write rejected")
    task = db.get(AnalysisTask, task.id)
    task.stage_results = {
        **(task.stage_results or {}),
        stage: {"at": now.isoformat(), **value},
    }
    task.append_event("stage", status=status, stage=stage)
    db.commit()


def _raise_if_cancelled(db: Session, task: AnalysisTask) -> None:
    db.refresh(task)
    if task.cancellation_requested:
        raise TaskCancelled("task cancellation requested")


def _load_calibration(db: Session, task: AnalysisTask) -> CalibrationVersion:
    calibration = db.get(CalibrationVersion, task.calibration_version_id)
    if calibration is None:
        raise LookupError(f"fixed calibration version is missing: {task.calibration_version_id}")
    return calibration


def execute_task(task_id: str, *, lease_seconds: int = 900, crash_after_stage: str | None = None) -> dict:
    """Idempotent execution boundary.

    A report row is inserted only in the terminal `complete_task` transaction,
    guarded by the worker's lease generation and the reports.task_id unique
    constraint. If the spectrum stage crashes, prior diagnostics remain on the
    task but the task is not `succeeded` and no published report exists.
    """

    db = SessionLocal()
    generation: int | None = None
    try:
        acquired = acquire_task(db, task_id, lease_seconds)
        if acquired is None:
            task = db.get(AnalysisTask, task_id)
            return {"status": task.status if task else "missing", "published": False}
        generation = acquired.lease_generation

        task = db.get(AnalysisTask, task_id)
        manifest = db.get(Manifest, task.manifest_snapshot["manifest_id"])
        if manifest is None:
            return _fail(db, task_id, generation, "manifest_missing", "manifest no longer exists", fatal=True)
        calibration = _load_calibration(db, task)
        snapshot = dict(task.manifest_snapshot)
        params = dict(snapshot["params"])
        # Defensive invariant: task creation fixes parameters and coefficients.
        # The mutable calibration row is used only to prove the frozen version
        # still exists; its current coefficients are never copied in.
        if calibration.channel_set_hash != snapshot["channel_set_hash"]:
            return _fail(
                db,
                task_id,
                generation,
                "calibration_channel_mismatch",
                "calibration does not belong to frozen channel set",
                fatal=True,
            )

        save_stage(
            db,
            task,
            "fixed_snapshot",
            {"snapshot_digest": snapshot_digest(snapshot)},
            generation=generation,
            lease_seconds=lease_seconds,
        )
        _raise_if_cancelled(db, task)

        expected_by_seq = {int(item["sequence"]): item for item in snapshot["expected_chunks"]}
        chunks = db.scalars(select(Chunk).where(Chunk.manifest_id == manifest.id)).all()
        loaded_meta = sorted(
            (
                {
                    "sequence": chunk.sequence,
                    "sha256": chunk.sha256,
                    "byte_offset": chunk.byte_offset,
                    "byte_length": chunk.byte_length,
                    "sample_count": chunk.sample_count,
                    "sample_rate": chunk.sample_rate,
                    "channels": list(chunk.channels),
                    "start_time": chunk.start_time,
                    "end_time": chunk.end_time,
                    "encoding": chunk.encoding,
                    "object_key": chunk.object_key,
                }
                for chunk in chunks
                if chunk.sequence in expected_by_seq
            ),
            key=lambda item: item["sequence"],
        )
        if len(loaded_meta) != len(expected_by_seq):
            missing = sorted(set(expected_by_seq) - {item["sequence"] for item in loaded_meta})
            return _fail(
                db,
                task_id,
                generation,
                "frozen_manifest_incomplete",
                f"frozen manifest cannot be assembled; missing sequences: {missing}",
                fatal=True,
            )
        save_stage(
            db,
            task,
            "raw_inventory",
            {"sequences": [item["sequence"] for item in loaded_meta], "count": len(loaded_meta)},
            generation=generation,
            lease_seconds=lease_seconds,
        )
        _raise_if_cancelled(db, task)
        if crash_after_stage == "raw_inventory":
            raise SpectrumCrash("simulated crash after raw inventory")

        store = get_object_store()
        for item in loaded_meta:
            expected = expected_by_seq[item["sequence"]]
            if item["sha256"] != expected["sha256"]:
                return _fail(
                    db,
                    task_id,
                    generation,
                    "frozen_metadata_mismatch",
                    f"chunk {item['sequence']} metadata differs",
                    fatal=True,
                )
        settings = get_settings()
        os.makedirs(settings.spool_dir, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=settings.spool_dir, prefix="analysis-") as scratch_name:
            raw_dir = os.path.join(scratch_name, "raw")
            segment_dir = os.path.join(scratch_name, "segments")
            os.makedirs(raw_dir, exist_ok=True)

            def fetch_raw(item: dict):
                expected = expected_by_seq[int(item["sequence"])]
                destination = Path(raw_dir) / f"{int(item['sequence']):09d}.bin"
                store.get_to_path(item["object_key"], destination)
                digest = hashlib.sha256()
                with destination.open("rb") as source:
                    for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
                        digest.update(block)
                if digest.hexdigest() != expected["sha256"]:
                    raise ValueError(f"object digest mismatch at sequence {item['sequence']}")
                return str(destination)

            segments = group_chunks_by_rate(loaded_meta, work_dir=segment_dir, fetch_raw=fetch_raw)
            bytes_loaded = sum(int(item["byte_length"]) for item in loaded_meta)
            save_stage(
                db,
                task,
                "raw_bytes",
                {"bytes_loaded": bytes_loaded},
                generation=generation,
                lease_seconds=lease_seconds,
            )
            if crash_after_stage == "raw_bytes":
                raise SpectrumCrash("simulated crash after raw bytes")

            save_stage(
                db,
                task,
                "segmentation",
                {
                    "segments": [
                        {
                            "sample_rate": segment["sample_rate"],
                            "samples": segment["data"].shape[0],
                            "sequences": [segment["start_sequence"], segment["end_sequence"]],
                        }
                        for segment in segments
                    ]
                },
                generation=generation,
                lease_seconds=lease_seconds,
            )
            if crash_after_stage == "segmentation":
                raise SpectrumCrash("simulated crash after segmentation")

            result = analyze_all_segments(
                segments,
                snapshot["calibration_coefficients"],
                params,
                sample_rate_changed=len({segment["sample_rate"] for segment in segments}) > 1,
            )
            save_stage(
                db,
                task,
                "spectrum",
                {
                    "quality_status": result["quality_status"],
                    "segments": len(result["segments"]),
                    "quality_codes": sorted({item["code"] for item in result["quality"]}),
                },
                generation=generation,
                lease_seconds=lease_seconds,
            )
            if crash_after_stage == "spectrum":
                raise SpectrumCrash("simulated crash before publication")

            result["fixed_snapshot"] = snapshot
            result["snapshot_digest"] = snapshot_digest(snapshot)
            outcome = complete_task(
                db,
                task_id,
                generation,
                manifest_id=manifest.id,
                calibration_id=calibration.id,
                result=result,
            )

            for segment in segments:
                segment["data"]._mmap.close()

        return outcome
    except TaskCancelled as exc:
        return _cancel(db, task_id, generation, str(exc))
    except LeaseLost as exc:
        db.rollback()
        return {"status": "lease_lost", "published": False, "error_code": "lease_lost", "detail": str(exc)}
    except SpectrumCrash as exc:
        return _fail(db, task_id, generation, "spectrum_crash", str(exc), fatal=False)
    except Exception as exc:  # diagnostics are mandatory for unexpected stage failures
        return _fail(db, task_id, generation, type(exc).__name__, str(exc), fatal=False)
    finally:
        db.close()


def complete_task(
    db: Session,
    task_id: str,
    generation: int,
    *,
    manifest_id: str,
    calibration_id: str,
    result: dict,
) -> dict:
    """Terminal publication boundary.

    Two independent barriers prevent a duplicate report:
    1. the task row is moved to its terminal status by a conditional UPDATE
       keyed on lease_generation (a superseded worker affects zero rows);
    2. the unique constraint on reports.task_id plus an existence check make
       publication idempotent even across commit-time retries.

    Quality error results land as `diagnostic_failed` rows that are never
    surfaced as normal completed reports.
    """
    quality_error = result.get("quality_status") == "error"
    # Fence first: a worker whose lease generation is no longer current must be
    # rejected even if a report already exists for the task.
    owner_task = db.get(AnalysisTask, task_id)
    if owner_task is None:
        return {"status": "missing", "published": False}
    if owner_task.lease_generation != generation:
        return {
            "status": "lease_lost",
            "published": False,
            "error_code": "lease_lost",
            "duplicate": True,
        }
    existing = db.scalar(select(Report).where(Report.task_id == task_id).limit(1))
    if existing is not None and existing.status in {"published", "needs_review"}:
        return {
            "status": "succeeded" if existing.status == "published" else "failed",
            "published": False,
            "duplicate": True,
        }

    now = utcnow()
    terminal_status = "failed" if quality_error else "succeeded"
    rowcount = db.execute(
        update(AnalysisTask)
        .where(
            AnalysisTask.id == task_id,
            AnalysisTask.lease_generation == generation,
            (AnalysisTask.lease_until.is_(None)) | (AnalysisTask.lease_until > now),
        )
        .values(
            status=terminal_status,
            lease_owner=None,
            lease_until=None,
            heartbeat_at=now,
            ended_at=now,
            error_code="quality_error" if quality_error else None,
            error_message=(
                "one or more analysis quality checks failed; diagnostic report is not published as complete"
                if quality_error
                else None
            ),
        )
        .execution_options(synchronize_session=False)
    ).rowcount
    if not rowcount:
        # Taken over by another generation: never publish. Or the lease simply
        # expired with no new owner: park the task in retry_wait instead of
        # publishing after expiry.
        db.rollback()
        fallback = db.execute(
            update(AnalysisTask)
            .where(
                AnalysisTask.id == task_id,
                AnalysisTask.lease_generation == generation,
                AnalysisTask.status == "running",
            )
            .values(
                status="retry_wait",
                error_code="lease_expired",
                error_message="lease expired before publication; task waits for takeover",
                lease_owner=None,
                lease_until=None,
                heartbeat_at=now,
            )
            .execution_options(synchronize_session=False)
        ).rowcount
        if fallback:
            task = db.get(AnalysisTask, task_id)
            task.append_event("status", status="retry_wait", error_code="lease_expired")
            db.commit()
            return {"status": "retry_wait", "published": False, "error_code": "lease_expired"}
        raise LeaseLost("task lease was taken over; refusing to publish a report")

    if existing is not None:
        # The unique reports.task_id constraint allows a single row per task.
        # A prior diagnostic_failed row is not a published report, so a later
        # successful attempt on the same fixed task turns that row in place.
        existing.status = "published" if not quality_error else "diagnostic_failed"
        existing.result = result
        existing.snapshot_digest = result["snapshot_digest"]
        existing.published_at = now if not quality_error else None
        existing.review_reason = (
            "analysis quality status is error; diagnostic only, not a normal completed report"
            if quality_error
            else None
        )
        report = existing
    else:
        report = Report(
            task_id=task_id,
            manifest_id=manifest_id,
            calibration_version_id=calibration_id,
            status="published" if not quality_error else "diagnostic_failed",
            result=result,
            snapshot_digest=result["snapshot_digest"],
            published_at=now if not quality_error else None,
            review_reason="analysis quality status is error; diagnostic only, not a normal completed report"
            if quality_error
            else None,
        )
        db.add(report)
        try:
            db.flush()
        except IntegrityError:
            # Final DB-level duplicate-execution boundary.
            db.rollback()
            return {"status": terminal_status, "published": False, "duplicate": True}

    task = db.get(AnalysisTask, task_id)
    task.append_event(
        "status",
        status=terminal_status,
        report_status=report.status,
        message="diagnostic only, not a published report" if quality_error else "report published",
    )
    db.commit()
    return {"status": terminal_status, "published": not quality_error}


def _terminalize(
    db: Session,
    task_id: str | None,
    generation: int | None,
    status: str,
    code: str | None,
    message: str | None,
) -> dict:
    if task_id is None or generation is None:
        return {"status": "missing", "published": False}
    now = utcnow()
    rowcount = db.execute(
        update(AnalysisTask)
        .where(
            AnalysisTask.id == task_id,
            AnalysisTask.lease_generation == generation,
            (AnalysisTask.lease_until.is_(None)) | (AnalysisTask.lease_until > now),
        )
        .values(
            status=status,
            error_code=code,
            error_message=message,
            ended_at=now,
            lease_owner=None,
            lease_until=None,
            heartbeat_at=now,
        )
        .execution_options(synchronize_session=False)
    ).rowcount
    if not rowcount:
        # Either the lease was taken over (new owner owns the row) or it simply
        # expired before this failure landed. If nobody took over yet, mark the
        # task retry_wait; otherwise leave the new owner's state untouched.
        db.rollback()
        fallback = db.execute(
            update(AnalysisTask)
            .where(
                AnalysisTask.id == task_id,
                AnalysisTask.lease_generation == generation,
                AnalysisTask.status == "running",
            )
            .values(
                status="retry_wait",
                error_code=code,
                error_message=message,
                lease_owner=None,
                lease_until=None,
                heartbeat_at=now,
            )
            .execution_options(synchronize_session=False)
        ).rowcount
        if fallback:
            task = db.get(AnalysisTask, task_id)
            task.append_event(
                "status",
                status="retry_wait",
                error_code=code,
                message=(f"{message} (written after lease expiry, before takeover)" if message else None),
            )
            db.commit()
            return {"status": "retry_wait", "published": False, "error_code": code}
        return {"status": "lease_lost", "published": False, "error_code": "lease_lost"}
    task = db.get(AnalysisTask, task_id)
    task.append_event("status", status=status, error_code=code, message=message)
    db.commit()
    return {"status": status, "published": False, "error_code": code}


def _fail(
    db: Session,
    task_id: str | None,
    generation: int | None,
    code: str,
    message: str,
    *,
    fatal: bool,
) -> dict:
    return _terminalize(
        db,
        task_id,
        generation,
        "failed" if fatal else "retry_wait",
        code,
        message,
    )


def _cancel(db: Session, task_id: str | None, generation: int | None, message: str) -> dict:
    return _terminalize(db, task_id, generation, "cancelled", "cancelled", message)
