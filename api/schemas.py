from typing import Any, Optional
from pydantic import BaseModel, Field


class JobCreateRequest(BaseModel):
    task_name: str = Field(..., description="Name of the registered task to execute")
    payload: dict[str, Any] = Field(default_factory=dict, description="Task arguments / input payload")
    max_attempts: int = Field(default=3, ge=1, le=10, description="Maximum execution attempts before failure")


class JobCreateResponse(BaseModel):
    job_id: str
    status: str = "PENDING"


class JobDetailResponse(BaseModel):
    id: str
    task_name: str
    payload: dict[str, Any]
    status: str
    attempts: int
    max_attempts: int
    result: Optional[Any] = None
    error: Optional[str] = None
    worker_id: Optional[str] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    updated_at: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    postgres: str
    redis: str
