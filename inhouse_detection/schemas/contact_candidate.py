from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum
from datetime import datetime
from uuid import uuid4


class SourceType(str, Enum):
    corp_site = "corp_site"
    press_release = "press_release"
    conference = "conference"
    job_posting = "job_posting"
    other = "other"


class ContactCandidate(BaseModel):
    """
    Synthesized contact candidate derived from one or more Evidence records.

    IMPORTANT:
    - This model represents a hypothesis supported by public evidence.
    - It does NOT assert employment role, fiduciary responsibility,
      or formal association with any specific plan.
    """

    candidate_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for this synthesized contact candidate"
    )

    sponsor_name: str = Field(
        ...,
        description="Plan sponsor this candidate is associated with (e.g., The Boeing Company)"
    )

    name: str = Field(
        ...,
        description="Candidate's full name as found in public sources"
    )

    title: Optional[str] = Field(
        None,
        description="Candidate's job title as listed in public sources"
    )

    employer: Optional[str] = Field(
        None,
        description="Employer listed in public sources (may or may not equal sponsor)"
    )

    source_types: List[SourceType] = Field(
        ...,
        description="Distinct source types contributing evidence for this candidate"
    )

    evidence_ids: List[str] = Field(
        ...,
        description="List of Evidence IDs supporting this candidate"
    )

    confidence_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Overall confidence score for this candidate based on supporting evidence"
    )

    first_seen: Optional[datetime] = Field(
        None,
        description="Earliest date this candidate appeared in collected evidence"
    )

    last_seen: Optional[datetime] = Field(
        None,
        description="Most recent date this candidate appeared in collected evidence"
    )

    public_contact_path: Optional[str] = Field(
        None,
        description="Publicly visible contact path (e.g., conference bio, corporate page)"
    )

    notes: Optional[str] = Field(
        None,
        description="Additional synthesis notes, uncertainty, or caveats"
    )
