from pydantic import BaseModel, Field, HttpUrl
from typing import Dict, Any, Optional
from enum import Enum
from datetime import datetime
from uuid import uuid4


class SourceType(str, Enum):
    corp_site = "corp_site"
    press_release = "press_release"
    conference = "conference"
    job_posting = "job_posting"
    other = "other"


class Evidence(BaseModel):
    """
    Atomic, immutable record of publicly available evidence.

    IMPORTANT:
    - Evidence represents a single factual observation.
    - It does NOT assert employment role, fiduciary responsibility,
      or formal association with any specific plan.
    - Evidence should never be modified once created.
    """

    evidence_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for this evidence record"
    )

    sponsor_name: str = Field(
        ...,
        description="Plan sponsor associated with this evidence (e.g., The Boeing Company)"
    )

    source_type: SourceType = Field(
        ...,
        description="Type of public source where the evidence was found"
    )

    url: HttpUrl = Field(
        ...,
        description="Public URL where the evidence was collected"
    )

    snippet: str = Field(
        ...,
        description="Extracted text supporting the evidence (human-readable)"
    )

    date_found: datetime = Field(
        default_factory=datetime.utcnow,
        description="Timestamp when this evidence was collected"
    )

    parsed_fields: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Structured fields parsed from the snippet "
            "(e.g., name, title, employer). "
            "Used for downstream synthesis, not as authoritative data."
        )
    )

    person_name: Optional[str] = Field(
        None,
        description="Person name mentioned in the evidence, if parsed"
    )

    employer: Optional[str] = Field(
        None,
        description="Employer mentioned in the evidence, if parsed"
    )

    confidence: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Confidence score for this evidence item alone"
    )

    notes: Optional[str] = Field(
        None,
        description="Additional context, uncertainty, or caveats"
    )
