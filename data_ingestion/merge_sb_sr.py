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
    # Primary: Left join SB ← SR on EIN + PLAN_NUMBER + YEAR
    # SB defines the plan universe; all SB plans are preserved even without SR match
    merged = pd.merge(
        sb_df,
        sr_df,
        how="left",
        on=["EIN", "PLAN_NUMBER", "YEAR"],
        suffixes=("_SB", "_SR")
    )
    # Find SB plans not matched by EIN+PLAN_NUMBER+YEAR (have no SR data)
    sb_matched_keys = set(zip(merged.loc[merged.filter(regex='_SR$').notna().any(axis=1), 'EIN'],
                              merged.loc[merged.filter(regex='_SR$').notna().any(axis=1), 'PLAN_NUMBER'],
                              merged.loc[merged.filter(regex='_SR$').notna().any(axis=1), 'YEAR']))
    sb_unmatched = sb_df.loc[~sb_df.set_index(["EIN", "PLAN_NUMBER", "YEAR"]).index.isin(
        pd.MultiIndex.from_tuples(sb_matched_keys, names=["EIN", "PLAN_NUMBER", "YEAR"]) if sb_matched_keys else pd.MultiIndex.from_tuples([], names=["EIN", "PLAN_NUMBER", "YEAR"])
    )]
    sr_matched_keys = set(zip(sr_df['EIN'], sr_df['PLAN_NUMBER'], sr_df['YEAR'])) & set(zip(sb_df['EIN'], sb_df['PLAN_NUMBER'], sb_df['YEAR']))
    sr_unmatched = sr_df.loc[~sr_df.set_index(["EIN", "PLAN_NUMBER", "YEAR"]).index.isin(
        pd.MultiIndex.from_tuples(sr_matched_keys, names=["EIN", "PLAN_NUMBER", "YEAR"]) if sr_matched_keys else pd.MultiIndex.from_tuples([], names=["EIN", "PLAN_NUMBER", "YEAR"])
    )]
    # Fallback: left join unmatched SB plans with remaining SR by ACK_ID + YEAR
    if not sb_unmatched.empty and not sr_unmatched.empty:
        fallback = pd.merge(
            sb_unmatched,
            sr_unmatched,
            how="left",
            left_on=["ACK_ID", "YEAR"],
            right_on=["ACK_ID", "YEAR"],
            suffixes=("_SB", "_SR")
        )
        # Update only the unmatched rows in merged
        merged = pd.concat([
            merged[merged.set_index(["EIN", "PLAN_NUMBER", "YEAR"]).index.isin(
                pd.MultiIndex.from_tuples(sb_matched_keys, names=["EIN", "PLAN_NUMBER", "YEAR"]) if sb_matched_keys else pd.MultiIndex.from_tuples([], names=["EIN", "PLAN_NUMBER", "YEAR"])
            )],
            fallback
        ], ignore_index=True)
    # Drop any duplicate SB plans (should not happen, but enforce)
    merged_final = merged.drop_duplicates(subset=["EIN", "PLAN_NUMBER", "YEAR"])
    
    # Consolidate ACK_ID: prefer ACK_ID_SB (from Schedule SB), fall back to ACK_ID if exists
    if 'ACK_ID_SB' in merged_final.columns:
        merged_final['ACK_ID'] = merged_final['ACK_ID_SB']
    elif 'ACK_ID' not in merged_final.columns:
        merged_final['ACK_ID'] = pd.NA
    
    return merged_final.reset_index(drop=True)
