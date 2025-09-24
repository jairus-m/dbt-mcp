from typing import Any, Optional
from pydantic import BaseModel


class RunStepSchema(BaseModel):
    """Schema for individual run step from get_job_run_details()."""

    name: str
    status: int  # 20 = error
    index: int
    finished_at: Optional[str] = None

    class Config:
        extra = "allow"


class RunDetailsSchema(BaseModel):
    """Schema for get_job_run_details() response."""

    is_cancelled: bool
    run_steps: list[RunStepSchema]
    finished_at: Optional[str] = None

    class Config:
        extra = "allow"


class RunResultSchema(BaseModel):
    """Schema for individual result in run_results.json."""

    unique_id: str
    status: str  # "success", "error", "fail", "skip"
    message: Optional[str] = None
    relation_name: Optional[str] = None

    class Config:
        extra = "allow"


class RunResultsArtifactSchema(BaseModel):
    """Schema for get_job_run_artifact() response (run_results.json)."""

    results: list[RunResultSchema]
    args: Optional[dict[str, Any]] = None
    metadata: Optional[dict[str, Any]] = None

    class Config:
        extra = "allow"


class ErrorResultSchema(BaseModel):
    """Schema for individual error result."""

    unique_id: Optional[str] = None
    relation_name: Optional[str] = None
    message: str


class MultiErrorResultSchema(BaseModel):
    """Schema for multiple errors results."""

    target: Optional[str] = None
    step_name: Optional[str] = None
    finished_at: Optional[str] = None
    errors: list[ErrorResultSchema]
