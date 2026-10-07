from __future__ import annotations

from celery import Celery

from .config import get_settings

settings = get_settings()
celery_app = Celery("power_quality", broker=settings.redis_url)
celery_app.conf.task_default_queue = "pq-analysis"
celery_app.conf.task_acks_late = True
celery_app.conf.worker_prefetch_multiplier = 1
celery_app.conf.task_track_started = True
# Task results live in the database (status, stage_results, events, reports);
# no Celery result backend is needed, and publishing must fail fast when the
# broker is down: the queued DB row plus the lease protocol is the real
# execution boundary.
celery_app.conf.task_ignore_result = True
celery_app.conf.broker_connection_timeout = 1
celery_app.conf.task_publish_retry_policy = {
    "max_retries": 2,
    "interval_start": 0.1,
    "interval_step": 0.1,
    "interval_max": 0.2,
}
celery_app.conf.broker_transport_options = {"visibility_timeout": settings.task_lock_seconds * 2}


@celery_app.task(name="run_analysis", bind=True, max_retries=None)
def run_analysis(self, task_id: str) -> dict:
    from .pipeline import execute_task

    return execute_task(task_id, lease_seconds=get_settings().task_lock_seconds)


@celery_app.task(name="run_analysis_crash_at")
def run_analysis_crash_at(task_id: str, stage: str) -> dict:
    from .pipeline import execute_task

    return execute_task(task_id, crash_after_stage=stage)
