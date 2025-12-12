import pandas as pd
import os
from serpapi import GoogleSearch
from dotenv import load_dotenv

# ----------------------------------------------------------
# Load environment variables (SERP_API_KEY)
# ----------------------------------------------------------
load_dotenv()

SERP_API_KEY = os.getenv("SERP_API_KEY")
if not SERP_API_KEY:
    raise ValueError("Missing SERP_API_KEY in your .env file!")

ROLLUP_PATH = "data_output/sponsor_rollup_latest.parquet"


# ----------------------------------------------------------
# SERP LOOKUP
# ----------------------------------------------------------
def serp_lookup(query):
    """Perform SERP API lookup and return the top 5 organic results."""
    params = {
        "engine": "google",
        "q": query,
        "api_key": SERP_API_KEY,
        "num": 10
    }

    try:
        search = GoogleSearch(params)
        results = search.get_dict()
        return results.get("organic_results", [])[:5]
    except Exception as e:
        print(f"SERP Lookup Error: {e}")
        return []


# ----------------------------------------------------------
# IMPROVED PENSION-ACTUARY QUERY GENERATOR
# ----------------------------------------------------------
def build_query(company_name):
    """
    Very targeted query designed to only detect DB/pension actuarial teams.
    Excludes health/P&C/pricing actuarial noise.
    """
    query = (
        f'"{company_name}" '
        f'("pension actuary" OR "retirement actuary" OR '
        f'"defined benefit actuary" OR "pension valuation" OR '
        f'"EA" OR "Enrolled Actuary" OR "pension administration") '
        f'-"health actuarial" -"health actuary" '
        f'-"property actuarial" -"casualty actuarial" '
        f'-"pricing actuary" -"insurance actuarial" '
    )
    return query


# ----------------------------------------------------------
# FILTER RELEVANT ACTUARIAL HITS
# ----------------------------------------------------------
def analyze_search_results(results):
    """
    Identify only pension-oriented actuarial staff or references.
    """
    # DB/Pension-focused keywords
    key_terms = [
        "pension", "defined benefit", "retirement actuary",
        "pension actuary", "ea", "enrolled actuary",
        "pension valuation", "pension administration"
    ]

    found = []
    for r in results:
        title = (r.get("title") or "").lower()
        snippet = (r.get("snippet") or "").lower()

        if any(term in title for term in key_terms) or any(term in snippet for term in key_terms):
            found.append({
                "title": r.get("title"),
                "url": r.get("link"),
                "snippet": r.get("snippet")
            })

    return found


# ----------------------------------------------------------
# MAIN BATCH PROCESS
# ----------------------------------------------------------
def run_batch_search():
    df = pd.read_parquet(ROLLUP_PATH)

    # Column validation
    if "sponsor_dfe_name" not in df.columns:
        raise ValueError(f"sponsor_dfe_name missing. Actual columns: {df.columns}")

    if "retired" not in df.columns:
        raise ValueError("Column 'retired' missing from dataset.")

    # Filter >10,000 annuitants
    df_large = df[df["retired"] > 10000].copy()

    # Sort descending (biggest plans at top)
    df_sorted = df_large.sort_values("retired", ascending=False)

    # TOP 5 + BOTTOM 5
    top5 = df_sorted.head(5)
    bottom5 = df_sorted.tail(5)

    batch = pd.concat([top5, bottom5])

    print("\n=== Running SERP Lookup for These 10 Sponsors ===")
    print(batch[["sponsor_dfe_name", "ein", "retired"]])

    results_output = []

    for _, row in batch.iterrows():

        sponsor = row["sponsor_dfe_name"]
        ein = row["ein"]
        retired = row["retired"]

        print(f"\n🔍 Searching for: {sponsor} (EIN {ein}, Retired {retired:,})")

        query = build_query(sponsor)
        print(f"Query used: {query}\n")

        search_results = serp_lookup(query)
        hits = analyze_search_results(search_results)

        results_output.append({
            "sponsor": sponsor,
            "ein": ein,
            "retired": retired,
            "query_used": query,
            "raw_results": search_results,
            "actuarial_hits": hits
        })

        print(f"Found {len(hits)} actuarial-related pension hits.")

    return results_output


# ----------------------------------------------------------
# OUTPUT DISPLAY
# ----------------------------------------------------------
if __name__ == "__main__":
    output = run_batch_search()

    print("\n\n=== FINAL STRUCTURED OUTPUT ===")

    for res in output:
        print("\n----")
        print(f"{res['sponsor']} (Retired {res['retired']:,})")
        if not res["actuarial_hits"]:
            print(" • No pension-actuary indicators found.")
        for h in res["actuarial_hits"]:
            print(f" • {h['title']} → {h['url']}")
