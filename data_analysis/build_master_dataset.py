"""
Build Master Enriched DB Dataset

Combines:
- SB actuarial fields (full coverage)
- Form 5500 metadata (partial coverage)
- Derived fields for analytics

This is the central table used for ranking, lead scoring,
actuary analysis, and UI dashboards.
"""

import os
import pandas as pd
from data_ingestion.merge_sb_5500 import merge_sb_5500


def build_master_dataset(sb_df: pd.DataFrame, f5500_df: pd.DataFrame) -> pd.DataFrame:
    
    # Merge SB with Form 5500 headers
    merged = merge_sb_5500(sb_df, f5500_df)

    # -------------------------------------------------------------
    # FORCE NUMERIC TYPES (prevents string arithmetic errors)
    # -------------------------------------------------------------
    numeric_cols = [
        "active", "retired", "terminated", "total",
        "liability_total", "liability_active", "liability_retired",
        "segment_rate_1", "segment_rate_2", "segment_rate_3",
        "effective_interest_rate", "mortality_code"
    ]

    for col in numeric_cols:
        if col in merged.columns:
            merged[col] = (
                pd.to_numeric(merged[col], errors="coerce")
                .astype("Float64")
            )

    # --- DERIVED FIELDS --------------------------------------------------

    # Filing date extracted from ACK_ID (YYYYMMDD)
    merged["filing_date"] = merged["ack_id"].astype(str).str[:8]

    # Liability metrics
    merged["liability_per_retiree"] = (
        merged["liability_retired"] / merged["retired"].replace({0: pd.NA})
    )

    merged["liability_per_active"] = (
        merged["liability_active"] / merged["active"].replace({0: pd.NA})
    )

    # Annuitant ratio (longevity risk indicator)
    merged["annuitant_ratio"] = (
        merged["retired"] / merged["active"].replace({0: pd.NA})
    )

    # DB size categories based on total liability
    merged["db_size_category"] = pd.cut(
        merged["liability_total"],
        bins=[0, 50_000_000, 500_000_000, 5_000_000_000, float("inf")],
        labels=["Small DB", "Mid DB", "Large DB", "Mega DB"]
    )

    # Merge status indicator
    def merge_status(row):
        if pd.isna(row.get("sponsor_dfe_name", None)):
            return "NO_5500"
        if not row.get("ein_match", False) or not row.get("pn_match", False):
            return "PARTIAL_MATCH"
        return "FULL_MATCH"

    merged["merge_status"] = merged.apply(merge_status, axis=1)

    # -------------------------------------------------------------
    # SELECT & REORDER FINAL COLUMNS
    # -------------------------------------------------------------
    important_cols = [
        "ack_id", "filing_date",
        "ein", "plan_number",
        "sponsor_dfe_name", "plan_name", "business_code",
        "active", "retired", "terminated", "total",
        "liability_total", "liability_active", "liability_retired",
        "liability_per_active", "liability_per_retiree",
        "effective_interest_rate", "segment_rate_1", "segment_rate_2", "segment_rate_3",
        "mortality_code",
        "actuary_name", "actuary_firm",
        "annuitant_ratio", "db_size_category",
        "merge_status", "ein_match", "pn_match"
    ]

    # Only keep columns that exist
    final_cols = [c for c in important_cols if c in merged.columns]

    master = merged[final_cols].copy()

    # -------------------------------------------------------------
    # SORT: Put plans with the most retirees ON TOP
    # -------------------------------------------------------------
    master = master.sort_values(
        by="retired",
        ascending=False,
        na_position="last"
    ).reset_index(drop=True)

    return master


# -------------------------------------------------------------
# SAVE FUNCTIONS — at module level for easy import
# -------------------------------------------------------------

def save_master_as_parquet(df, filename="master_db_latest.parquet"):
    """Save master dataset in Parquet format for fast analytics."""
    output_path = os.path.join("data_output", filename)
    df.to_parquet(output_path, index=False)
    print(f"\n[✔] Saved Parquet file → {output_path}")


def save_master_as_csv(df, filename="master_db_latest.csv"):
    """Save master dataset in CSV format for Excel/human inspection."""
    output_path = os.path.join("data_output", filename)
    df.to_csv(output_path, index=False)
    print(f"[✔] Saved CSV file → {output_path}")
