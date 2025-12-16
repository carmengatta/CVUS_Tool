
"""
Data normalization and deduplication logic for Evidence objects.
No inference, no synthesis, no UI.
"""
from typing import List
from schemas.evidence import Evidence
import re

def normalize_evidence(evidence_list: List[Evidence]) -> List[Evidence]:
    """
    Deduplicate and normalize a list of Evidence objects.
    Deduplication is conservative: only identical sponsor_name, source_type, url, and normalized snippet are considered duplicates.
    Normalization collapses whitespace in snippet for comparison, but preserves original in output.
    """
    seen = set()
    result = []
    for ev in evidence_list:
        # Normalize snippet for deduplication (collapse whitespace, lowercase)
        norm_snippet = re.sub(r"\s+", " ", ev.snippet).strip().lower()
        key = (ev.sponsor_name, ev.source_type, str(ev.url), norm_snippet)
        if key not in seen:
            seen.add(key)
            # Optionally, could also normalize snippet in output, but here we preserve original
            result.append(ev)
    return result
