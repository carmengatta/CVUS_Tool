"""
merge_sb_5500.py

Merge Schedule SB actuarial data with Form 5500 sponsor metadata.

Rules:
- Schedule SB defines the plan universe
- Form 5500 is supplemental only
- Only Form 5500 rows matching SB plans are retained
- Enforce exactly one row per SB plan per year
"""

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def merge_sb_5500(df_sb: pd.DataFrame, df_5500: pd.DataFrame) -> pd.DataFrame:
    """
    Merge SB (base) with Form 5500 (supplemental).

    Parameters
    ----------
    df_sb : pd.DataFrame
        Normalized Schedule SB dataframe (one row per plan-year)
    df_5500 : pd.DataFrame
        Raw / lightly normalized Form 5500 dataframe (many rows per plan-year)

    Returns
    -------
    pd.DataFrame
        SB-driven, DB-only dataframe with at most one row per plan-year
    """

    # Defensive copies
    df_sb = df_sb.copy() if df_sb is not None else pd.DataFrame()
    df_5500 = df_5500.copy() if df_5500 is not None else pd.DataFrame()

    if df_sb.empty:
        logging.warning("No SB data provided; returning empty DataFrame.")
        return pd.DataFrame()

    # ----------------------------
    # Normalize SB keys
    # ----------------------------
    df_sb["EIN"] = df_sb["EIN"].astype(str).str.strip()
    df_sb["PLAN_NUMBER"] = df_sb["PLAN_NUMBER"].astype(str).str.strip().str.zfill(3)
    df_sb["PLAN_YEAR"] = pd.to_numeric(df_sb["PLAN_YEAR"], errors="coerce")

    df_sb["SB_KEY"] = (
        df_sb["EIN"]
        + "-"
        + df_sb["PLAN_NUMBER"]
        + "-"
        + df_sb["PLAN_YEAR"].astype(str)
    )

    # ----------------------------
    # Normalize & filter Form 5500
    # ----------------------------
    if not df_5500.empty:
        df_5500["EIN"] = df_5500["EIN"].astype(str).str.strip()
        df_5500["PLAN_NUMBER"] = (
            df_5500["PLAN_NUMBER"].astype(str).str.strip().str.zfill(3)
        )
        if "PLAN_YEAR" in df_5500.columns:
            df_5500["PLAN_YEAR"] = pd.to_numeric(
                df_5500["PLAN_YEAR"], errors="coerce"
            )

        df_5500["SB_KEY"] = (
            df_5500["EIN"]
            + "-"
            + df_5500["PLAN_NUMBER"]
            + "-"
            + df_5500["PLAN_YEAR"].astype(str)
        )

        # Keep only Form 5500 rows that match SB plans
        before_ct = len(df_5500)
        df_5500 = df_5500[df_5500["SB_KEY"].isin(df_sb["SB_KEY"])]
        after_ct = len(df_5500)

        logging.info(
            f"{before_ct - after_ct} Form 5500 records dropped as non-SB plans."
        )
    else:
        df_5500 = pd.DataFrame(columns=df_sb.columns)

    # ----------------------------
    # Merge: SB as base
    # ----------------------------
    merged = pd.merge(
        df_sb,
        df_5500,
        on=["EIN", "PLAN_NUMBER", "PLAN_YEAR"],
        how="left",
        suffixes=("", "_5500"),
    )

    # ----------------------------
    # CRITICAL: Enforce 1 row per SB plan
    # ----------------------------
    # Form 5500 can have multiple rows per plan-year;
    # collapse deterministically to one row per SB_KEY
    merged = (
        merged.sort_values(by=["SB_KEY"])
        .groupby("SB_KEY", as_index=False)
        .first()
    )

    # ----------------------------
    # Cleanup: remove interest / rate columns
    # ----------------------------
    interest_cols = [
        c for c in merged.columns
        if "INTEREST" in c or "RATE" in c or "DISCOUNT" in c
    ]
    merged = merged.drop(columns=interest_cols, errors="ignore")

    return merged.reset_index(drop=True)
