"""
Build Sponsor-Level Consolidated DB Dataset

Aggregates all DB plans by EIN:
- Sums participants and liabilities
- Creates plan count
- Combines plan numbers into one field
- Keeps first non-null sponsor name
"""

import os
import pandas as pd

def build_sponsor_rollup(master_df: pd.DataFrame) -> pd.DataFrame:

    # Helper function: first non-null value
    def first_non_null(series):
        return next((x for x in series if pd.notna(x) and x != ""), pd.NA)

    # Determine worst merge_status per EIN
    def worst_merge_status(group):
        if "NO_5500" in group.values:
            return "NO_5500"
        if "PARTIAL_MATCH" in group.values:
            return "PARTIAL_MATCH"
        return "FULL_MATCH"

    # Group and aggregate
    grouped = master_df.groupby("ein").agg({
        "plan_number": lambda s: ",".join(sorted(set(s.dropna().astype(str)))),
        "sponsor_dfe_name": first_non_null,
        "plan_name": "first",
        "active": "sum",
        "retired": "sum",
        "terminated": "sum",
        "total": "sum",
        "liability_total": "sum",
        "liability_active": "sum",
        "liability_retired": "sum",
        "merge_status": worst_merge_status
    }).reset_index()

    # Add plan count
    grouped["plan_count"] = grouped["plan_number"].apply(
        lambda x: len(x.split(",")) if pd.notna(x) else 0
    )

    # Sort by retired descending (same logic as master)
    grouped = grouped.sort_values(
        by="retired",
        ascending=False,
        na_position="last"
    ).reset_index(drop=True)

    return grouped


def save_sponsor_rollup_parquet(df, filename="sponsor_rollup_latest.parquet"):
    output_path = os.path.join("data_output", filename)
    df.to_parquet(output_path, index=False)
    print(f"[✔] Saved Sponsor Parquet → {output_path}")


def save_sponsor_rollup_csv(df, filename="sponsor_rollup_latest.csv"):
    output_path = os.path.join("data_output", filename)
    df.to_csv(output_path, index=False)
    print(f"[✔] Saved Sponsor CSV → {output_path}")
