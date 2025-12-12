"""
serp_helpers.py
----------------
Utility functions for performing SERP API lookups, building queries,
extracting relevant actuarial signals, and managing rate limits.

This module centralizes all web search logic so the batch lookup script
and Streamlit UI stay clean and simple.
"""

import os
import time
from serpapi import GoogleSearch
from dotenv import load_dotenv

load_dotenv()

SERP_API_KEY = os.getenv("SERP_API_KEY")


# ------------------------------------------------------------
# Validation
# ------------------------------------------------------------
if not SERP_API_KEY:
    raise ValueError("Missing SERP_API_KEY in .env — visit serpapi.com to obtain it")


# ------------------------------------------------------------
# Query Builder
# ------------------------------------------------------------
def build_actuary_query(sponsor_name: str):
    """
    Builds a structured search query targeted at finding actuarial staff.
    """
    return (
        f'"{sponsor_name}" actuary OR "actuarial team" OR '
        f'"pension actuary" OR "enrolled actuary" OR '
        f'"defined benefit" actuarial'
    )


# ------------------------------------------------------------
# SERP API Runner
# ------------------------------------------------------------
def run_serp_search(query: str, delay: float = 1.5):
    """
    Executes a SERP API search using the Google engine.
    Returns JSON dictionary or None.
    """

    params = {
        "engine": "google",
        "q": query,
        "api_key": SERP_API_KEY
    }

    try:
        search = GoogleSearch(params)
        result = search.get_dict()
    except Exception as e:
        p
