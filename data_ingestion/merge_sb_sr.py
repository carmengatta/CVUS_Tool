"""
Form 5500 Data Ingestion: Merge SB and Schedule R

Responsibilities:
- Merge SB ↔ Schedule R
- Inner join only
- Match priority: EIN + PLAN_NUMBER + YEAR, fallback to ACK_ID + YEAR
- Only include Schedule R rows that match an SB-backed plan
- No cross-year matching
- No DC plan contamination
"""

import pandas as pd

def merge_sb_sr(sb_df: pd.DataFrame, sr_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge SB and Schedule R dataframes using inner join logic.
    Match priority: EIN + PLAN_NUMBER + YEAR, fallback to ACK_ID + YEAR.
    Only plans present in SB are retained (DB-only).

    Args:
        sb_df (pd.DataFrame): Normalized SB DataFrame (DB plans only)
        sr_df (pd.DataFrame): Normalized Schedule R DataFrame

    Returns:
        pd.DataFrame: Merged DataFrame (DB plans only)
    """
    # Primary: EIN + PLAN_NUMBER + YEAR
    merged = pd.merge(
        sb_df,
        sr_df,
        how="inner",
        on=["EIN", "PLAN_NUMBER", "YEAR"],
        suffixes=("_SB", "_SR")
    )
    # Find SB plans not matched by EIN+PLAN_NUMBER+YEAR
    sb_unmatched = sb_df.loc[~sb_df.set_index(["EIN", "PLAN_NUMBER", "YEAR"]).index.isin(merged.set_index(["EIN", "PLAN_NUMBER", "YEAR"]).index)]
    sr_unmatched = sr_df.loc[~sr_df.set_index(["EIN", "PLAN_NUMBER", "YEAR"]).index.isin(merged.set_index(["EIN", "PLAN_NUMBER", "YEAR"]).index)]
    # Fallback: match by ACK_ID + YEAR
    fallback = pd.merge(
        sb_unmatched,
        sr_unmatched,
        how="inner",
        left_on=["ACK_ID", "YEAR"],
        right_on=["ACK_ID", "YEAR"],
        suffixes=("_SB", "_SR")
    )
    # Concatenate both matches
    merged_final = pd.concat([merged, fallback], ignore_index=True)
    # Drop any duplicate SB plans (should not happen, but enforce)
    merged_final = merged_final.drop_duplicates(subset=["EIN", "PLAN_NUMBER", "YEAR"])
    return merged_final.reset_index(drop=True)
