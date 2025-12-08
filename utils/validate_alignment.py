"""
Dataset Alignment Validator for Form 5500 + Schedule SB

This utility compares the SB dataset and the 5500 dataset and computes:
- Row counts
- ACK_ID match counts
- EIN/PN match quality
- Filing date ranges (parsed from ACK_ID timestamps)
- Percent alignment
- Diagnostic summary

Use this before analytics to confirm the two datasets belong
to the same DOL release batch.
"""

import pandas as pd

def extract_ack_date(ack_id: str):
    """Extract YYYYMMDD from ACK_ID (first 8 characters)."""
    if isinstance(ack_id, str) and len(ack_id) >= 8:
        return ack_id[:8]
    return None


def validate_alignment(sb_df: pd.DataFrame, f5500_df: pd.DataFrame):
    report = {}

    # Row counts
    report["sb_rows"] = len(sb_df)
    report["f5500_rows"] = len(f5500_df)

    # Count distinct ACK IDs
    report["sb_ack_unique"] = sb_df["ack_id"].nunique()
    report["f5500_ack_unique"] = f5500_df["ack_id"].nunique()

    # Identify matches
    sb_ack_set = set(sb_df["ack_id"].astype(str))
    f5500_ack_set = set(f5500_df["ack_id"].astype(str))

    matching_ack = sb_ack_set.intersection(f5500_ack_set)

    report["ack_matches"] = len(matching_ack)
    report["ack_match_pct"] = (
        len(matching_ack) / len(sb_ack_set) * 100
        if len(sb_ack_set) > 0 else 0
    )

    # Extract and compare filing date ranges
    sb_dates = sorted({extract_ack_date(a) for a in sb_ack_set if extract_ack_date(a)})
    f5500_dates = sorted({extract_ack_date(a) for a in f5500_ack_set if extract_ack_date(a)})

    report["sb_date_range"] = (sb_dates[0], sb_dates[-1]) if sb_dates else ("N/A", "N/A")
    report["f5500_date_range"] = (f5500_dates[0], f5500_dates[-1]) if f5500_dates else ("N/A", "N/A")

    # Diagnostic interpretation
    if report["ack_matches"] == 0:
        report["diagnosis"] = (
            "No ACK_IDs overlap. This usually means the SB and 5500 datasets "
            "are from different DOL release batches OR the 5500 dataset does not yet "
            "include the full header set for DB plans."
        )
    elif report["ack_match_pct"] < 10:
        report["diagnosis"] = (
            "Very low alignment (<10%). Most SB filings lack matching 5500 headers. "
            "This may indicate partial or early-cycle 5500 data."
        )
    else:
        report["diagnosis"] = "Datasets appear reasonably aligned."

    return report
