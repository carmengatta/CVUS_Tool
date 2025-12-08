"""
Normalize Schedule SB actuarial fields:
- Preserve ACK_ID
- Extract participant counts
- Extract liabilities
- Extract actuary info
"""

import pandas as pd

def normalize_sb_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    # KEEP ACK_ID
    if "ack_id" in df.columns:
        out["ack_id"] = df["ack_id"]
    else:
        raise KeyError("ACK_ID not found in SB dataset")

    # EIN and Plan Number
    out["ein"] = df.get("sb_ein", pd.NA)
    out["plan_number"] = df.get("sb_pn", pd.NA)

    # Participant counts
    out["active"] = df.get("sb_act_partcp_cnt", pd.NA)
    out["retired"] = df.get("sb_rtd_partcp_cnt", pd.NA)
    out["terminated"] = df.get("sb_term_partcp_cnt", pd.NA)
    out["total"] = df.get("sb_tot_partcp_cnt", pd.NA)

    # Liabilities
    out["liability_total"] = df.get("sb_tot_fndng_tgt_amt", pd.NA)
    out["liability_retired"] = df.get("sb_rtd_fndng_tgt_amt", pd.NA)
    out["liability_terminated"] = df.get("sb_term_fndng_tgt_amt", pd.NA)
    out["liability_active"] = df.get("sb_act_vstd_fndng_tgt_amt", pd.NA)

    # Actuary information
    out["actuary_name"] = df.get("sb_actuary_name_line", pd.NA)
    out["actuary_firm"] = df.get("sb_actuary_firm_name", pd.NA)
    out["actuary_enrollment"] = df.get("sb_actry_enrlmt_num", pd.NA)

    # Assumptions
    out["effective_interest_rate"] = df.get("sb_eff_int_rate_prcnt", pd.NA)
    out["segment_rate_1"] = df.get("sb_1st_seg_rate_prcnt", pd.NA)
    out["segment_rate_2"] = df.get("sb_2nd_seg_rate_prcnt", pd.NA)
    out["segment_rate_3"] = df.get("sb_3rd_seg_rate_prcnt", pd.NA)
    out["mortality_code"] = df.get("sb_mortality_tbl_cd", pd.NA)

    return out
