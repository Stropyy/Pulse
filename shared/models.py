import enum
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import (
    DateTime,
    Enum,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

    @classmethod
    def terminal_statuses(cls) -> set["JobStatus"]:
        return {cls.SUCCEEDED, cls.FAILED}

    def is_terminal(self) -> bool:
        return self in (JobStatus.SUCCEEDED, JobStatus.FAILED)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    task_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=JobStatus.PENDING.value,
        index=True,
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    result: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    worker_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    locked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now,
    )

    __table_args__ = (
        Index("ix_jobs_status_locked_at", "status", "locked_at"),
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "task_name": self.task_name,
            "payload": self.payload,
            "status": self.status,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "result": self.result,
            "error": self.error,
            "worker_id": self.worker_id,
            "locked_at": self.locked_at.isoformat() if self.locked_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
