from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def uuid_str() -> str:
    return str(uuid.uuid4())


class Manifest(Base):
    __tablename__ = "manifests"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="open", index=True)
    expected_chunks: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    channel_set: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    channel_set_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    nominal_sample_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    start_time: Mapped[str | None] = mapped_column(String(40), nullable=True)
    end_time: Mapped[str | None] = mapped_column(String(40), nullable=True)
    manifest_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    chunks: Mapped[list["Chunk"]] = relationship(back_populates="manifest", cascade="all, delete-orphan")
    issues: Mapped[list["ValidationIssue"]] = relationship(back_populates="manifest", cascade="all, delete-orphan")
    tasks: Mapped[list["AnalysisTask"]] = relationship(back_populates="manifest")


class Chunk(Base):
    __tablename__ = "chunks"
    __table_args__ = (
        UniqueConstraint("manifest_id", "sequence", name="uq_chunk_manifest_sequence"),
        UniqueConstraint("object_key", name="uq_chunk_object_key"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    manifest_id: Mapped[str] = mapped_column(ForeignKey("manifests.id", ondelete="CASCADE"), index=True, nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    object_key: Mapped[str] = mapped_column(String(512), nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    byte_offset: Mapped[int] = mapped_column(Integer, nullable=False)
    byte_length: Mapped[int] = mapped_column(Integer, nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    sample_rate: Mapped[float] = mapped_column(Float, nullable=False)
    channels: Mapped[list] = mapped_column(JSON, nullable=False)
    start_time: Mapped[str] = mapped_column(String(40), nullable=False)
    end_time: Mapped[str] = mapped_column(String(40), nullable=False)
    encoding: Mapped[str] = mapped_column(String(64), nullable=False, default="float32le-interleaved")
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    manifest: Mapped[Manifest] = relationship(back_populates="chunks")


class ValidationIssue(Base):
    __tablename__ = "validation_issues"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    manifest_id: Mapped[str] = mapped_column(ForeignKey("manifests.id", ondelete="CASCADE"), index=True, nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)  # error|warning
    code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    details: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    manifest: Mapped[Manifest] = relationship(back_populates="issues")


class CalibrationVersion(Base):
    __tablename__ = "calibration_versions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    channel_set_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", index=True)
    coefficients: Mapped[dict] = mapped_column(JSON, nullable=False)
    change_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    supersedes_id: Mapped[str | None] = mapped_column(ForeignKey("calibration_versions.id"), nullable=True)
    created_by: Mapped[str] = mapped_column(String(128), nullable=False, default="lab")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    activated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class AnalysisTask(Base):
    __tablename__ = "analysis_tasks"
    __table_args__ = (UniqueConstraint("idempotency_key", name="uq_analysis_idempotency_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    manifest_id: Mapped[str] = mapped_column(ForeignKey("manifests.id"), index=True, nullable=False)
    calibration_version_id: Mapped[str] = mapped_column(ForeignKey("calibration_versions.id"), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="queued", index=True)
    params: Mapped[dict] = mapped_column(JSON, nullable=False)
    manifest_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False)
    stage_results: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    # Append-only task trajectory: status transitions, lease takeovers and
    # stage completions, each with an ISO timestamp.
    events: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Fencing token: bumped on every acquisition/takeover/retry. A worker's
    # writes are conditional on the generation it acquired, so an old worker
    # whose lease expired can never write back after another owner takes over.
    lease_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Manual retries consumed for the current attempt. Reset to zero whenever a
    # new worker attempt acquires the lease. The controlled retry transition is
    # a conditional UPDATE against attempts, so concurrent UI clicks serialize
    # at the database and only one can succeed per failed attempt.
    retry_request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    cancellation_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    manifest: Mapped[Manifest] = relationship(back_populates="tasks")
    report: Mapped["Report | None"] = relationship(back_populates="task", uselist=False)

    @staticmethod
    def aware(value: datetime | None) -> datetime | None:
        """DateTime columns come back naive on SQLite; normalize to UTC for comparisons."""
        if value is None:
            return None
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)

    def append_event(self, kind: str, status: str | None = None, **details) -> None:
        event = {"at": utcnow().isoformat(), "kind": kind}
        if status is not None:
            event["status"] = status
        event.update({key: value for key, value in details.items() if value is not None})
        self.events = [*(self.events or []), event]

    @property
    def lease_expired(self) -> bool:
        if self.lease_until is None:
            return False
        return self.aware(self.lease_until) <= utcnow()

    def retry_advice(self) -> dict:
        """Server-side decision the UI uses to gate the controlled retry entry."""
        if self.report is not None and self.report.status in {"published", "needs_review"}:
            return {
                "allowed": False,
                "reason": "a published report already exists; retries never replace it — create a new fixed task",
            }
        if self.status == "succeeded":
            return {"allowed": False, "reason": "task already succeeded"}
        if self.status == "cancelled":
            return {"allowed": False, "reason": "cancelled tasks are not retried; create a new task"}
        if self.status == "queued":
            return {"allowed": False, "reason": "task is queued; no retry needed"}
        # Exactly mirrors the atomic WHERE in pipeline.request_retry:
        # one open retry slot per failed attempt, or an expired running lease.
        if self.status == "running":
            if self.lease_expired:
                return {"allowed": True, "reason": "the current lease has expired and can be taken over"}
            return {"allowed": False, "reason": "a live worker still holds the lease"}
        if self.retry_request_count >= self.attempts:
            return {"allowed": False, "reason": "retry already requested; waiting for the next worker attempt"}
        if self.status in {"retry_wait", "failed"}:
            return {"allowed": True, "reason": "the fixed snapshot will be reused on the next attempt"}
        return {"allowed": False, "reason": f"retry is not defined for status {self.status}"}


class Report(Base):
    __tablename__ = "reports"
    __table_args__ = (UniqueConstraint("task_id", name="uq_report_task"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    task_id: Mapped[str] = mapped_column(ForeignKey("analysis_tasks.id"), nullable=False)
    manifest_id: Mapped[str] = mapped_column(ForeignKey("manifests.id"), index=True, nullable=False)
    calibration_version_id: Mapped[str] = mapped_column(ForeignKey("calibration_versions.id"), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="published", index=True)
    result: Mapped[dict] = mapped_column(JSON, nullable=False)
    snapshot_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    review_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    superseded_by_calibration_id: Mapped[str | None] = mapped_column(
        ForeignKey("calibration_versions.id"), nullable=True
    )

    task: Mapped[AnalysisTask] = relationship(back_populates="report")
