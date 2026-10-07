from __future__ import annotations

import hashlib
import os
import tempfile
import uuid
from datetime import timedelta
from pathlib import Path

from sqlalchemy import and_, or_, select, update
from sqlalchemy.orm import Session

from .config import get_settings
from .database import SessionLocal
from .dsp import analyze_all_segments, group_chunks_by_rate
from .models import AnalysisTask, CalibrationVersion, Chunk, Manifest, Report, TaskEvent, utcnow
from .services import snapshot_digest
from .storage import get_object_store

WORKER_ID = os.environ.get("PQ_WORKER_ID", f"worker-{uuid.uuid4().hex[:8]}")


class TaskCancelled(RuntimeError):
    pass


class SpectrumCrash(RuntimeError):
    """Synthetic crash used by the acceptance suite to prove no partial report is published."""


class LeaseLost(RuntimeError):
    """Raised when a fenced write finds the lease owned by another worker.

    The stale worker must not touch task state anymore: the task has been
    taken over and any write-back would corrupt the new owner's trajectory.
    """


def record_event(db: Session, task_id: str, event: str, status: str, detail: dict | None = None) -> TaskEvent:
    """Append one trajectory entry. Committed together with the state change it describes."""

    entry = TaskEvent(task_id=task_id, event=event, status=status, detail=detail or {})
    db.add(entry)
    db.flush()
    return entry


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
        )
        .execution_options(synchronize_session=False)
    )
    if result.rowcount:
        db.flush()
        task = db.get(AnalysisTask, task_id)
        record_event(
            db,
            task_id,
            "running",
            "running",
            {"worker_id": worker_id, "attempt": task.attempts, "lease_until": lease_until.isoformat()},
        )
        return task
    # Reclaim a crashed worker's stale lease. This still only permits one owner.
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
            attempts=AnalysisTask.attempts + 1,
        )
        .execution_options(synchronize_session=False)
    )
    if result.rowcount:
        db.flush()
        task = db.get(AnalysisTask, task_id)
        record_event(
            db,
            task_id,
            "lease_recovered",
            "running",
            {
                "worker_id": worker_id,
                "attempt": task.attempts,
                "lease_until": lease_until.isoformat(),
                "note": "previous lease expired; taken over by conditional update",
            },
        )
        return task
    return None


def request_cancel(db: Session, task: AnalysisTask) -> AnalysisTask:
    task.cancellation_requested = True
    if task.status in {"queued", "retry_wait"}:
        task.status = "cancelled"
        task.ended_at = utcnow()
        task.lease_owner = None
        task.lease_until = None
        record_event(db, task.id, "cancelled", "cancelled", {"trigger": "manual"})
    else:
        record_event(db, task.id, "cancel_requested", task.status, {"trigger": "manual"})
    db.flush()
    return task


def request_retry(db: Session, task: AnalysisTask) -> AnalysisTask:
    """Controlled retry: a single conditional update decides who may requeue.

    Allowed from failed/retry_wait/cancelled, and from running only when the
    lease has already expired (safe takeover). Concurrent double-clicks are
    idempotent: both updates set the same values, and the lease fencing plus
    the unique report constraint keep publication single.
    """

    if task.report is not None and task.report.status == "published":
        raise ValueError("a published report cannot be retried; create a new fixed task")
    now = utcnow()
    previous_status = task.status
    result = db.execute(
        update(AnalysisTask)
        .where(
            AnalysisTask.id == task.id,
            or_(
                AnalysisTask.status.in_(["failed", "retry_wait", "cancelled"]),
                and_(AnalysisTask.status == "running", AnalysisTask.lease_until < now),
            ),
        )
        .values(
            status="retry_wait",
            cancellation_requested=False,
            lease_owner=None,
            lease_until=None,
            error_code=None,
            error_message=None,
        )
        .execution_options(synchronize_session=False)
    )
    if not result.rowcount:
        if task.status == "running":
            raise ValueError(
                f"task is running and the lease is still held by {task.lease_owner} "
                f"until {task.lease_until}; retry becomes safe once the lease expires"
            )
        raise ValueError(f"cannot retry task in status {task.status}")
    record_event(
        db,
        task.id,
        "retry_wait",
        "retry_wait",
        {"trigger": "manual", "previous_status": previous_status, "attempts": task.attempts},
    )
    db.flush()
    task.status = "retry_wait"
    return task


def recover_stale_tasks(db: Session) -> int:
    now = utcnow()
    stale = db.scalars(
        select(AnalysisTask).where(AnalysisTask.status == "running", AnalysisTask.lease_until < now)
    ).all()
    recovered = 0
    for task in stale:
        # Per-task conditional update: a worker that heartbeats between the
        # select and the update keeps its task, and only one recoverer wins.
        result = db.execute(
            update(AnalysisTask)
            .where(
                AnalysisTask.id == task.id,
                AnalysisTask.status == "running",
                AnalysisTask.lease_until < now,
            )
            .values(status="retry_wait", lease_owner=None, lease_until=None)
            .execution_options(synchronize_session=False)
        )
        if result.rowcount:
            recovered += 1
            record_event(
                db,
                task.id,
                "lease_recovered",
                "retry_wait",
                {
                    "previous_owner": task.lease_owner,
                    "lease_expired_at": task.lease_until.isoformat() if task.lease_until else None,
                    "recovered_by": "maintenance",
                },
            )
    db.commit()
    return recovered


def save_stage(db: Session, task: AnalysisTask, stage: str, value: dict, status: str = "running", *, worker_id: str) -> None:
    """Persist a stage result with lease fencing.

    The conditional update only succeeds while this worker still owns the
    lease of a running task; a stale worker's write-back is rejected.
    """

    now = utcnow()
    stage_results = {**(task.stage_results or {}), stage: {**value, "at": now.isoformat()}}
    result = db.execute(
        update(AnalysisTask)
        .where(
            AnalysisTask.id == task.id,
            AnalysisTask.status == "running",
            AnalysisTask.lease_owner == worker_id,
        )
        .values(stage_results=stage_results, status=status, heartbeat_at=now)
        .execution_options(synchronize_session=False)
    )
    if not result.rowcount:
        db.rollback()
        raise LeaseLost(f"lease lost before writing stage '{stage}'; stale worker write rejected")
    record_event(db, task.id, "stage", status, {"stage": stage, **value})
    db.commit()


def heartbeat(db: Session, task_id: str, worker_id: str) -> None:
    """Keepalive for long stages; also a fencing probe for stale workers."""

    result = db.execute(
        update(AnalysisTask)
        .where(
            AnalysisTask.id == task_id,
            AnalysisTask.status == "running",
            AnalysisTask.lease_owner == worker_id,
        )
        .values(heartbeat_at=utcnow())
        .execution_options(synchronize_session=False)
    )
    if not result.rowcount:
        db.rollback()
        raise LeaseLost("lease lost during heartbeat; stale worker must stop")
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


def execute_task(task_id: str, *, lease_seconds: int = 900, crash_after_stage: str | None = None, worker_id: str = WORKER_ID) -> dict:
    """Idempotent execution boundary.

    A report row is inserted only in the terminal `publish_report` transaction,
    and the terminal status transition is fenced by the lease: if the lease was
    taken over mid-run, the stale worker's report insert is rolled back and no
    state is overwritten. If the spectrum stage crashes, prior diagnostics
    remain on the task but the task is not `succeeded` and no report exists.
    """

    db = SessionLocal()
    try:
        task = acquire_task(db, task_id, lease_seconds, worker_id=worker_id)
        db.commit()
        if task is None:
            task = db.get(AnalysisTask, task_id)
            return {"status": task.status if task else "missing", "published": False}

        task = db.get(AnalysisTask, task_id)
        manifest = db.get(Manifest, task.manifest_snapshot["manifest_id"])
        if manifest is None:
            return _fail(db, task, "manifest_missing", "manifest no longer exists", fatal=True, worker_id=worker_id)
        calibration = _load_calibration(db, task)
        snapshot = dict(task.manifest_snapshot)
        params = dict(snapshot["params"])
        # Defensive invariant: task creation fixes parameters and coefficients.
        # The mutable calibration row is used only to prove the frozen version
        # still exists; its current coefficients are never copied in.
        if calibration.channel_set_hash != snapshot["channel_set_hash"]:
            return _fail(db, task, "calibration_channel_mismatch", "calibration does not belong to frozen channel set", fatal=True, worker_id=worker_id)

        save_stage(db, task, "fixed_snapshot", {"snapshot_digest": snapshot_digest(snapshot)}, worker_id=worker_id)
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
                task,
                "frozen_manifest_incomplete",
                f"frozen manifest cannot be assembled; missing sequences: {missing}",
                fatal=True,
                worker_id=worker_id,
            )
        save_stage(
            db,
            task,
            "raw_inventory",
            {"sequences": [item["sequence"] for item in loaded_meta], "count": len(loaded_meta)},
            worker_id=worker_id,
        )
        _raise_if_cancelled(db, task)
        if crash_after_stage == "raw_inventory":
            raise SpectrumCrash("simulated crash after raw inventory")

        store = get_object_store()
        for item in loaded_meta:
            expected = expected_by_seq[item["sequence"]]
            if item["sha256"] != expected["sha256"]:
                return _fail(db, task, "frozen_metadata_mismatch", f"chunk {item['sequence']} metadata differs", fatal=True, worker_id=worker_id)
        settings = get_settings()
        os.makedirs(settings.spool_dir, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=settings.spool_dir, prefix="analysis-") as scratch_name:
            raw_dir = os.path.join(scratch_name, "raw")
            segment_dir = os.path.join(scratch_name, "segments")
            os.makedirs(raw_dir, exist_ok=True)
            bytes_loaded = 0

            def fetch_raw(item: dict):
                heartbeat(db, task_id, worker_id)
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
            save_stage(db, task, "raw_bytes", {"bytes_loaded": bytes_loaded}, worker_id=worker_id)
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
                worker_id=worker_id,
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
                worker_id=worker_id,
            )
            if crash_after_stage == "spectrum":
                raise SpectrumCrash("simulated crash before publication")

            result["fixed_snapshot"] = snapshot
            result["snapshot_digest"] = snapshot_digest(snapshot)
            published = publish_report(db, task, manifest, calibration, result)

            for segment in segments:
                segment["data"]._mmap.close()

        if not published:
            # Defensive: a report row already exists for this task. Release the
            # lease and leave the existing report untouched.
            existing = db.scalar(select(Report).where(Report.task_id == task.id).limit(1))
            terminal = "succeeded" if existing is not None and existing.status == "published" else "failed"
            now = utcnow()
            transition = db.execute(
                update(AnalysisTask)
                .where(
                    AnalysisTask.id == task.id,
                    AnalysisTask.status == "running",
                    AnalysisTask.lease_owner == worker_id,
                )
                .values(
                    status=terminal,
                    ended_at=now,
                    heartbeat_at=now,
                    lease_owner=None,
                    lease_until=None,
                )
                .execution_options(synchronize_session=False)
            )
            if transition.rowcount:
                record_event(
                    db,
                    task.id,
                    terminal,
                    terminal,
                    {
                        "published": False,
                        "report_status": existing.status if existing else None,
                        "note": "a report already exists for this task; the original is kept",
                    },
                )
            db.commit()
            return {"status": "already_reported", "published": False}

        quality_error = result.get("quality_status") == "error"
        terminal = "failed" if quality_error else "succeeded"
        now = utcnow()
        # Fenced terminal transition: if the lease was taken over mid-run, this
        # update matches nothing and the pending report insert is rolled back.
        transition = db.execute(
            update(AnalysisTask)
            .where(
                AnalysisTask.id == task.id,
                AnalysisTask.status == "running",
                AnalysisTask.lease_owner == worker_id,
            )
            .values(
                status=terminal,
                error_code="quality_error" if quality_error else None,
                error_message="one or more analysis quality checks failed; diagnostic report is not published as complete"
                if quality_error
                else None,
                ended_at=now,
                heartbeat_at=now,
                lease_owner=None,
                lease_until=None,
            )
            .execution_options(synchronize_session=False)
        )
        if not transition.rowcount:
            db.rollback()
            return {"status": "lease_lost", "published": False, "error_code": "lease_lost"}
        record_event(
            db,
            task.id,
            terminal,
            terminal,
            {
                "published": not quality_error,
                "report_status": "diagnostic_failed" if quality_error else "published",
                "snapshot_digest": result["snapshot_digest"],
                "quality_status": result.get("quality_status"),
            },
        )
        db.commit()
        return {"status": terminal, "published": not quality_error}
    except TaskCancelled as exc:
        return _cancel(db, db.get(AnalysisTask, task_id), str(exc), worker_id=worker_id)
    except LeaseLost:
        db.rollback()
        return {"status": "lease_lost", "published": False, "error_code": "lease_lost"}
    except SpectrumCrash as exc:
        return _fail(db, db.get(AnalysisTask, task_id), "spectrum_crash", str(exc), fatal=False, worker_id=worker_id)
    except Exception as exc:  # diagnostics are mandatory for unexpected stage failures
        return _fail(db, db.get(AnalysisTask, task_id), type(exc).__name__, str(exc), fatal=False, worker_id=worker_id)
    finally:
        db.close()


def publish_report(db: Session, task: AnalysisTask, manifest: Manifest, calibration, result: dict) -> bool:
    # The unique constraint on task_id and this existence check make duplicate
    # publication impossible even if a worker retries a commit timeout.
    existing = db.scalar(select(Report).where(Report.task_id == task.id).limit(1))
    if existing:
        return False
    report = Report(
        task_id=task.id,
        manifest_id=manifest.id,
        calibration_version_id=calibration.id,
        status="published" if result.get("quality_status") != "error" else "diagnostic_failed",
        result=result,
        snapshot_digest=result["snapshot_digest"],
        published_at=utcnow() if result.get("quality_status") != "error" else None,
        review_reason="analysis quality status is error; diagnostic only, not a normal completed report"
        if result.get("quality_status") == "error"
        else None,
    )
    db.add(report)
    db.flush()
    return True


def _fail(db: Session, task: AnalysisTask | None, code: str, message: str, *, fatal: bool, worker_id: str) -> dict:
    if task is None:
        return {"status": "missing", "published": False}
    now = utcnow()
    final = "failed" if fatal else "retry_wait"
    values = {
        "status": final,
        "error_code": code,
        "error_message": message,
        "heartbeat_at": now,
        "lease_owner": None,
        "lease_until": None,
    }
    if fatal:
        values["ended_at"] = now
    # Fenced: a stale worker may not fail a task that someone else now owns.
    result = db.execute(
        update(AnalysisTask)
        .where(
            AnalysisTask.id == task.id,
            AnalysisTask.status == "running",
            AnalysisTask.lease_owner == worker_id,
        )
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    if not result.rowcount:
        db.rollback()
        return {"status": "lease_lost", "published": False, "error_code": "lease_lost"}
    record_event(db, task.id, final, final, {"error_code": code, "error_message": message, "fatal": fatal})
    db.commit()
    return {"status": final, "published": False, "error_code": code}


def _cancel(db: Session, task: AnalysisTask | None, message: str, *, worker_id: str) -> dict:
    if task is None:
        return {"status": "missing", "published": False}
    now = utcnow()
    result = db.execute(
        update(AnalysisTask)
        .where(
            AnalysisTask.id == task.id,
            AnalysisTask.status == "running",
            AnalysisTask.lease_owner == worker_id,
        )
        .values(
            status="cancelled",
            error_code="cancelled",
            error_message=message,
            ended_at=now,
            heartbeat_at=now,
            lease_owner=None,
            lease_until=None,
        )
        .execution_options(synchronize_session=False)
    )
    if not result.rowcount:
        db.rollback()
        return {"status": "lease_lost", "published": False, "error_code": "lease_lost"}
    record_event(db, task.id, "cancelled", "cancelled", {"reason": message})
    db.commit()
    return {"status": "cancelled", "published": False}
