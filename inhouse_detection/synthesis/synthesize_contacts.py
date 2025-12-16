def synthesize_candidates(evidence_list: List[Evidence]) -> List[ContactCandidate]:
"""
Synthesis layer: Converts normalized Evidence objects into ContactCandidate objects.
Pure, deterministic, and auditable. No scraping, UI, or orchestration.
"""
from typing import List, Dict, Tuple, Optional
from collections import defaultdict, Counter
from schemas.contact_candidate import ContactCandidate, SourceType
from schemas.evidence import Evidence
from datetime import datetime

def synthesize_candidates(evidence_list: List[Evidence]) -> Tuple[List[ContactCandidate], List[Dict]]:
    """
    Groups normalized Evidence into ContactCandidate objects.
    Skips evidence with no person_name, tracking skipped items with reasons.
    Returns (candidates, skipped_evidence)
    """
    # Group evidence by (sponsor_name, person_name)
    groups = defaultdict(list)
    skipped = []
    for ev in evidence_list:
        if not ev.person_name or not ev.sponsor_name:
            skipped.append({"evidence_id": ev.evidence_id, "reason": "Missing person_name or sponsor_name"})
            continue
        key = (ev.sponsor_name.strip(), ev.person_name.strip())
        groups[key].append(ev)

    candidates = []
    for (sponsor_name, person_name), group in groups.items():
        # Aggregate fields
        titles = [ev.parsed_fields.get("title") for ev in group if ev.parsed_fields and ev.parsed_fields.get("title")]
        employers = [ev.employer for ev in group if ev.employer]
        source_types = list({ev.source_type for ev in group})
        evidence_ids = [ev.evidence_id for ev in group]
        confidences = [ev.confidence if ev.confidence is not None else 0.5 for ev in group]
        first_seen = min((ev.date_found for ev in group if ev.date_found), default=None)
        last_seen = max((ev.date_found for ev in group if ev.date_found), default=None)
        public_contact_paths = [ev.url for ev in group if ev.url]
        notes = "; ".join(filter(None, [ev.notes for ev in group])) or None

        # Most common title and employer (if tie, set to None)
        title = Counter(titles).most_common(1)[0][0] if titles and Counter(titles).most_common(1)[0][1] == 1 else None
        if titles:
            c = Counter(titles)
            most_common = c.most_common()
            if len(most_common) == 1 or (len(most_common) > 1 and most_common[0][1] > most_common[1][1]):
                title = most_common[0][0]
            else:
                title = None
        else:
            title = None
        employer = Counter(employers).most_common(1)[0][0] if employers and Counter(employers).most_common(1)[0][1] == 1 else None
        if employers:
            c = Counter(employers)
            most_common = c.most_common()
            if len(most_common) == 1 or (len(most_common) > 1 and most_common[0][1] > most_common[1][1]):
                employer = most_common[0][0]
            else:
                employer = None
        else:
            employer = None

        # Confidence: mean of all supporting evidence
        confidence_score = min(max(sum(confidences) / len(confidences), 0.0), 1.0) if confidences else 0.5

        # Public contact path: first available URL
        public_contact_path = public_contact_paths[0] if public_contact_paths else None

        candidate = ContactCandidate(
            sponsor_name=sponsor_name,
            name=person_name,
            title=title,
            employer=employer,
            source_types=source_types,
            evidence_ids=evidence_ids,
            confidence_score=confidence_score,
            first_seen=first_seen,
            last_seen=last_seen,
            public_contact_path=public_contact_path,
            notes=notes
        )
        candidates.append(candidate)
    return candidates, skipped
