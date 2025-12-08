"""
Merge Schedule SB actuarial data with Form 5500 sponsor metadata.
Primary merge key: ACK_ID
Some SB ACK_IDs will not appear in the Form 5500 dataset.
"""

import pandas as pd

def merge_sb_5500(sb_df: pd.DataFrame, f5500_df: pd.DataFrame) -> pd.DataFrame:

    sb_df["ack_id"] = sb_df["ack_id"].astype(str)
    f5500_df["ack_id"] = f5500_df["ack_id"].astype(str)

    merged = pd.merge(
        sb_df,
        f5500_df,
        on="ack_id",
        how="left",            # keep ALL SB rows
        validate="many_to_one" # many SB → one 5500 (or none)
    )

    # EIN match flag only if 5500 EIN exists
    if "spons_dfe_ein" in merged.columns:
        merged["ein_match"] = (
            merged["ein"].astype(str) == merged["spons_dfe_ein"].astype(str)
        )
    else:
        merged["ein_match"] = False

    # PN match flag only if 5500 PN exists
    if "spons_dfe_pn" in merged.columns:
        merged["pn_match"] = (
            merged["plan_number"].astype(str) == merged["spons_dfe_pn"].astype(str)
        )
    else:
        merged["pn_match"] = False

    # Warnings when mismatch occurs
    merged["merge_warning"] = merged.apply(
        lambda row: (
            "NO_5500_MATCH" if pd.isna(row["sponsor_dfe_name"]) else
            ("EIN_MISMATCH" if not row["ein_match"] else
             ("PN_MISMATCH" if not row["pn_match"] else ""))
        ),
        axis=1
    )

    return merged
