"""
Batch In-House Actuary Detection (SERP API)
------------------------------------------
TEST VERSION — Runs SERP lookups only for the TOP 5 sponsors
with the largest annuitant populations.

Uses:
    inhouse_detection/serp_helpers.py

Outputs cached results to:
    data_output/inhouse_lookup_results.csv

Run manually:
    python inhouse_detection/run_inhouse_lookup.py
"""

import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

from inhouse_detection.serp_helpers import (
    build_actuary_query,
    run_serp_search,
    extract_top_results,
    detect_actuary_keywords,
)

load_dotenv()

SPONSOR_PATH = "data_output/sponsor_rollup_latest.parquet"
OUTPUT_PATH = "data_output/inhouse_lookup_results.csv"

# ------------------------------------------------------------
# Load sponsor-level data
# ------------------------------------------------------------
if not os.path.exists(SPONSOR_PATH):
    raise FileNotFoundError(f"Missing sponsor rollup dataset: {SPONSOR_PATH}")

sponsor_df = pd.read_parquet(SPONSOR_PATH)

# Filter and sort sponsors with >10,000 retirees
large_sponsors = (
    sponsor_df[sponsor_df["retired"] > 10000]
    .sort_values("retired", ascending=False)
    .reset_index(drop=True)
)

# LIMIT SEARCH TO TOP 5 FOR TESTING
large_sponsors = large_sponsors.head(5)

print("==========================================================")
print(" IN-HOUSE ACTUARY DETECTION — TEST RUN (Top 5 Sponsors)  ")
print("==========================================================")
print(large_sponsors[["ein", "sponsor_name", "retired"]])
print("\n")


# ------------------------------------------------------------
# Load existing cache
# ------------------------------------------------------------
if os.path.exists(OUTPUT_PATH):
    cache = pd.read_csv(OUTPUT_PATH)
    print(f"Loaded lookup cache with {len(cache)} rows.\n")
else:
    cache = pd.DataFrame(columns=[
        "ein", "sponsor_name", "retired",
        "query", "result_title", "result_link", "result_snippet",
        "possible_inhouse", "timestamp"
    ])

already_processed = set(cache["ein"].astype(str))


# ------------------------------------------------------------
# Run SERP lookups
# ------------------------------------------------------------
new_rows = []

for _, row in large_sponsors.iterrows():
    ein = str(row["ein"])
    sponsor = row["sponsor_name"]
    retired = int(row["retired"])

    if ein in already_processed:
        print(f"Skipping {sponsor} (EIN {ein}) — already processed.\n")
        continue

    print(f"🔎 Running lookup for: {sponsor} (EIN {ein})...")

    # Build query
    query = build_actuary_query(sponsor)

    # Run SERP request
    results = run_serp_search(query)

    if not results:
        print("⚠ No results returned.\n")
        continue

    # Extract top SERP hits
    hits = extract_top_results(results, max_results=3)

    if not hits:
        print("⚠ SERP returned no organic results.\n")
        continue

    # Process results
    for h in hits:
        snippet = h.get("snippet", "")
        flag = detect_actuary_keywords(snippet)

        new_rows.append({
            "ein": ein,
            "sponsor_name": sponsor,
            "retired": retired,
            "query": query,
            "result_title": h.get("title"),
            "result_link": h.get("link"),
            "result_snippet": snippet,
            "possible_inhouse": flag,
            "timestamp": datetime.now(),
        })

    print(f"✔ Saved {len(hits)} results for {sponsor}\n")


# ------------------------------------------------------------
# Save updated results
# ------------------------------------------------------------
if new_rows:
    new_df = pd.DataFrame(new_rows)
    final = pd.concat([cache, new_df], ignore_index=True)
    final.to_csv(OUTPUT_PATH, index=False)
    print(f"🎉 Saved updated lookup results → {OUTPUT_PATH}")
else:
    print("No new results to save.")
