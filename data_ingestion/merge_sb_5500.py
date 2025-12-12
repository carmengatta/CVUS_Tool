"""
Merge Schedule SB actuarial data with Form 5500 sponsor metadata.
Primary merge key: ACK_ID
Some SB ACK_IDs will not appear in the Form 5500 dataset.
"""


import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')



def merge_sb_5500(df_5500: pd.DataFrame, df_sb: pd.DataFrame, df_sr: pd.DataFrame = None):
    # Defensive copies
    df_5500 = df_5500.copy() if df_5500 is not None else pd.DataFrame()
    df_sb = df_sb.copy() if df_sb is not None else pd.DataFrame()
    df_sr = df_sr.copy() if df_sr is not None else pd.DataFrame()

    if df_sb.empty:
        logging.warning("No SB data provided; returning empty DataFrame.")
        return pd.DataFrame()

    # Normalize keys for SB
    df_sb['EIN'] = df_sb['EIN'].astype(str).str.strip()
    df_sb['PLAN_NUMBER'] = df_sb['PLAN_NUMBER'].astype(str).str.strip().str.zfill(3)
    if 'PLAN_YEAR' in df_sb.columns:
        df_sb['PLAN_YEAR'] = pd.to_numeric(df_sb['PLAN_YEAR'], errors='coerce')

    # Create unique key for SB
    df_sb['SB_KEY'] = df_sb['EIN'] + '-' + df_sb['PLAN_NUMBER'] + '-' + df_sb['PLAN_YEAR'].astype(str)

    # Filter 5500 to SB universe
    if not df_5500.empty:
        df_5500['EIN'] = df_5500['EIN'].astype(str).str.strip()
        df_5500['PLAN_NUMBER'] = df_5500['PLAN_NUMBER'].astype(str).str.strip().str.zfill(3)
        if 'PLAN_YEAR' in df_5500.columns:
            df_5500['PLAN_YEAR'] = pd.to_numeric(df_5500['PLAN_YEAR'], errors='coerce')
        df_5500['SB_KEY'] = df_5500['EIN'] + '-' + df_5500['PLAN_NUMBER'] + '-' + df_5500['PLAN_YEAR'].astype(str)
        df_5500 = df_5500[df_5500['SB_KEY'].isin(df_sb['SB_KEY'])]
    else:
        df_5500 = pd.DataFrame(columns=df_sb.columns)

    # Filter SR to SB universe
    if not df_sr.empty:
        df_sr['EIN'] = df_sr['EIN'].astype(str).str.strip()
        df_sr['PLAN_NUMBER'] = df_sr['PLAN_NUMBER'].astype(str).str.strip().str.zfill(3)
        if 'PLAN_YEAR' in df_sr.columns:
            df_sr['PLAN_YEAR'] = pd.to_numeric(df_sr['PLAN_YEAR'], errors='coerce')
        df_sr['SB_KEY'] = df_sr['EIN'] + '-' + df_sr['PLAN_NUMBER'] + '-' + df_sr['PLAN_YEAR'].astype(str)
        df_sr = df_sr[df_sr['SB_KEY'].isin(df_sb['SB_KEY'])]
    else:
        df_sr = pd.DataFrame(columns=df_sb.columns)

    # Use SB as base, left-merge 5500 and SR onto SB
    merged = df_sb.copy()
    if not df_5500.empty:
        merged = pd.merge(merged, df_5500, on=['EIN', 'PLAN_NUMBER', 'PLAN_YEAR'], how='left', suffixes=('', '_5500'))
    if not df_sr.empty:
        merged = pd.merge(merged, df_sr, on=['EIN', 'PLAN_NUMBER', 'PLAN_YEAR'], how='left', suffixes=('', '_SR'))

    merged['TRACKING_ID'] = merged['EIN'] + '-' + merged['PLAN_NUMBER']

    # Log any non-SB plans (should be none, but for audit)
    if not df_5500.empty:
        non_sb_5500 = df_5500[~df_5500['SB_KEY'].isin(df_sb['SB_KEY'])]
        logging.info(f"{len(non_sb_5500)} Form 5500 records dropped as non-SB plans.")
    
    if not df_sr.empty:
        non_sb_sr = df_sr[~df_sr['SB_KEY'].isin(df_sb['SB_KEY'])]
        logging.info(f"{len(non_sb_sr)} Schedule R records dropped as non-SB plans.")

    # Remove interest / discount rate columns
    interest_cols = [
        c for c in merged.columns
        if "INTEREST" in c or "RATE" in c or "DISCOUNT" in c
    ]


    merged = merged.drop(columns=interest_cols, errors='ignore')

    return merged.reset_index(drop=True)
