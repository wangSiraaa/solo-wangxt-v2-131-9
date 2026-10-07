from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..database import get_db
from ..dsp import default_params
from ..models import AnalysisTask, CalibrationVersion, Manifest, Report, TaskEvent
from ..pipeline import execute_task, recover_stale_tasks, request_cancel, request_retry
from ..schemas import (
    AnalysisCreate,
    CalibrationCreate,
    CalibrationOut,
    ReportOut,
    RetryOut,
    TaskOut,
    TaskTimelineOut,
)
from ..services import create_analysis_task, create_calibration, get_active_calibration

router = APIRouter(tags=["analysis"])


@router.post("/calibrations", response_model=CalibrationOut, status_code=201)
def post_calibration(payload: CalibrationCreate, db: Session = Depends(get_db)):
    try:
        version = create_calibration(
            db,
            channel_set_hash=payload.channel_set_hash,
            coefficients=payload.coefficients,
            change_note=payload.change_note,
            created_by=payload.created_by,
            mark_previous_for_review=payload.mark_previous_for_review,
        )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    db.commit()
    db.refresh(version)
    return version


@router.get("/calibrations", response_model=list[CalibrationOut])
def list_calibrations(channel_set_hash: str | None = None, db: Session = Depends(get_db)):
    stmt = select(CalibrationVersion).order_by(CalibrationVersion.created_at.desc())
    if channel_set_hash:
        stmt = stmt.where(CalibrationVersion.channel_set_hash == channel_set_hash)
    return db.scalars(stmt).all()


@router.get("/calibrations/{version_id}", response_model=CalibrationOut)
def get_calibration(version_id: str, db: Session = Depends(get_db)):
    version = db.get(CalibrationVersion, version_id)
    if version is None:
        raise HTTPException(404, "calibration version not found")
    return version


@router.post("/analysis-tasks", response_model=TaskOut, status_code=201)
def post_analysis(payload: AnalysisCreate, db: Session = Depends(get_db)):
    manifest = db.get(Manifest, payload.manifest_id)
    if manifest is None:
        raise HTTPException(404, "manifest not found")
    if manifest.status != "completed":
        raise HTTPException(422, f"manifest is {manifest.status}; complete immutable upload validation first")

    if payload.calibration_version_id:
        calibration = db.get(CalibrationVersion, payload.calibration_version_id)
    else:
        calibration = get_active_calibration(db, manifest.channel_set_hash)
    if calibration is None:
        raise HTTPException(422, "no calibration version specified or active for channel set")

    params = {**default_params(), **payload.params}
    task = create_analysis_task(
        db,
        manifest=manifest,
        calibration=calibration,
        params=params,
        idempotency_key=payload.idempotency_key,
    )
    db.commit()
    db.refresh(task)
    # The Celery send is deliberately after commit. In local/test mode a missing
    # broker leaves the task queued; /run or a worker can execute it later. The
    # DB lease remains the final duplicate-execution boundary.
    try:
        from ..celery_app import run_analysis

        run_analysis.delay(task.id)
    except Exception:
        pass
    return task


@router.get("/analysis-tasks", response_model=list[TaskOut])
def list_tasks(manifest_id: str | None = None, db: Session = Depends(get_db)):
    stmt = select(AnalysisTask).order_by(AnalysisTask.requested_at.desc())
    if manifest_id:
        stmt = stmt.where(AnalysisTask.manifest_id == manifest_id)
    return db.scalars(stmt).all()


@router.get("/analysis-tasks/{task_id}", response_model=TaskOut)
def get_task(task_id: str, db: Session = Depends(get_db)):
    task = db.get(AnalysisTask, task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    return task


@router.get("/analysis-tasks/{task_id}/timeline", response_model=TaskTimelineOut)
def get_task_timeline(task_id: str, db: Session = Depends(get_db)):
    """Run trajectory for one task: lifecycle events plus the report, if any.

    The task payload carries the operational fields the lab needs to judge
    retry safety: stage_results, lease_owner/lease_until, heartbeat_at,
    attempts and error_code/error_message.
    """

    task = db.get(AnalysisTask, task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    events = db.scalars(select(TaskEvent).where(TaskEvent.task_id == task_id).order_by(TaskEvent.id)).all()
    report = db.scalar(select(Report).where(Report.task_id == task_id).limit(1))
    return {"task": task, "events": events, "report": report}


@router.post("/analysis-tasks/{task_id}/run", response_model=dict)
def run_task(task_id: str, db: Session = Depends(get_db)):
    task = db.get(AnalysisTask, task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    db.commit()
    return execute_task(task_id)


@router.post("/analysis-tasks/{task_id}/retry", response_model=RetryOut)
def retry_task(task_id: str, db: Session = Depends(get_db)):
    task = db.get(AnalysisTask, task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    try:
        request_retry(db, task)
    except ValueError as exc:
        # 409: published report exists, or the lease is still actively held.
        raise HTTPException(409, str(exc)) from exc
    db.commit()
    db.refresh(task)
    # Best-effort dispatch, same contract as task creation: in local/test mode
    # without a broker the task stays retry_wait until /run or a worker takes
    # it. The DB lease remains the duplicate-execution boundary.
    try:
        from ..celery_app import run_analysis

        run_analysis.delay(task.id)
    except Exception:
        pass
    return {"task_id": task.id, "status": task.status, "attempts": task.attempts}


@router.post("/analysis-tasks/{task_id}/cancel", response_model=TaskOut)
def cancel_task(task_id: str, db: Session = Depends(get_db)):
    task = db.get(AnalysisTask, task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    request_cancel(db, task)
    db.commit()
    db.refresh(task)
    return task


@router.post("/maintenance/recover-stale-tasks")
def recover_tasks(db: Session = Depends(get_db)):
    return {"recovered": recover_stale_tasks(db)}


@router.get("/reports", response_model=list[ReportOut])
def list_reports(manifest_id: str | None = None, db: Session = Depends(get_db)):
    stmt = select(Report).order_by(Report.published_at.desc().nullslast())
    if manifest_id:
        stmt = stmt.where(Report.manifest_id == manifest_id)
    return db.scalars(stmt).all()


@router.get("/reports/{report_id}", response_model=ReportOut)
def get_report(report_id: str, db: Session = Depends(get_db)):
    report = db.get(Report, report_id)
    if report is None:
        raise HTTPException(404, "report not found")
    return report
