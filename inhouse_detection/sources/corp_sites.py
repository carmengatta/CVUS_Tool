
"""
Corporate site ingestion module for collecting publicly visible evidence of in-house actuarial roles.
- Single-source: Only processes one sponsor at a time.
- Conservative: No inference, only direct extraction from public web pages.
- Returns: List of Evidence objects (schemas.evidence.Evidence)
- No orchestration, synthesis, or UI logic included.
"""
import requests
from bs4 import BeautifulSoup
from typing import List
from schemas.evidence import Evidence, SourceType
from datetime import datetime

USER_AGENT = "Mozilla/5.0 (compatible; ContactIntelBot/1.0; +https://yourdomain.example/contact-intel)"
HEADERS = {"User-Agent": USER_AGENT}

def fetch_corp_site_evidence(sponsor_name: str, site_url: str) -> List[Evidence]:
    """
    Fetches and parses the given corporate site URL for publicly visible actuarial staff evidence.
    Args:
        sponsor_name: Name of the plan sponsor (e.g., 'The Boeing Company')
        site_url: Public URL of the sponsor's staff or careers page
    Returns:
        List of Evidence objects (may be empty if no evidence found)
    """
    evidence_list = []
    try:
        resp = requests.get(site_url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        # Log or handle error as needed
        return evidence_list

    soup = BeautifulSoup(resp.text, "html.parser")
    # Conservative: Only extract visible text from the page
    # Example: Look for table rows or divs with 'actuar' in text (case-insensitive)
    for tag in soup.find_all(["div", "li", "tr", "p"]):
        text = tag.get_text(separator=" ", strip=True)
        if not text:
            continue
        if "actuar" in text.lower():
            snippet = text
            evidence = Evidence(
                sponsor_name=sponsor_name,
                source_type=SourceType.corp_site,
                url=site_url,
                snippet=snippet,
                date_found=datetime.utcnow(),
                parsed_fields=None,
                person_name=None,
                employer=None,
                confidence=0.5,  # Conservative default; adjust if more context is available
                notes="Extracted from corporate site; no inference applied."
            )
            evidence_list.append(evidence)
    return evidence_list
