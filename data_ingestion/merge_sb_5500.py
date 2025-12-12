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
    df_5500 = df_5500.copy()
    df_sb = df_sb.copy() if df_sb is not None else pd.DataFrame()
    df_sr = df_sr.copy() if df_sr is not None else pd.DataFrame()

    # Standardize merge keys and robust PLAN_NUMBER normalization
    PLAN_NUMBER_MAP = [
        # Form 5500
        'SPONS_DFE_PN', 'PLAN_NUM', 'PLAN_NUMBER', 'PNUM', 'PN',
        # Schedule SB
        'SB_PLAN_NUM', 'SB_PN', 'PLAN_NO',
        # Schedule R
        'SCH_R_PN',
    ]
    for df, ein_col, pn_col, year_col in [
        (df_5500, 'EIN', 'PLAN_NUMBER', 'PLAN_YEAR'),
        (df_sb, 'EIN', 'PLAN_NUMBER', 'PLAN_YEAR'),
        (df_sr, 'EIN', 'PLAN_NUMBER', 'PLAN_YEAR')
    ]:
        if not df.empty:
            # PLAN_NUMBER normalization (first match, zero-pad, string, drop if missing)
            plan_number_col = None
            for col in PLAN_NUMBER_MAP:
                if col in df.columns:
                    plan_number_col = col
                    break
            if plan_number_col:
                df['PLAN_NUMBER'] = df[plan_number_col].astype(str).str.strip().str.zfill(3)
            else:
                logging.warning("No recognizable plan number field found in merge; dropping rows.")
                df.drop(df.index, inplace=True)
            if ein_col in df.columns:
                df[ein_col] = df[ein_col].astype(str).str.strip()
            if year_col in df.columns:
                df[year_col] = pd.to_numeric(df[year_col], errors='coerce')


    # Within-year merge: use ACK_ID if present in both, else fall back to EIN+PLAN_NUMBER+PLAN_YEAR
    if 'ACK_ID' in df_5500.columns and 'ACK_ID' in df_sb.columns:
        merged = pd.merge(
            df_5500,
            df_sb,
            on="ACK_ID",
            how="outer",
            suffixes=("_5500", "_SB"),
            indicator=False
        )
        merged['sb_merge_flag'] = ~merged['EIN_SB'].isna() if 'EIN_SB' in merged.columns else False
        # Secondary merge for unmatched (EIN+PLAN_NUMBER+PLAN_YEAR)
        unmatched = merged[~merged['sb_merge_flag']].copy() if 'sb_merge_flag' in merged.columns else pd.DataFrame()
        if not unmatched.empty and not df_sb.empty:
            sec_merge = pd.merge(
                unmatched.drop([c for c in unmatched.columns if c.endswith('_SB')], axis=1, errors='ignore'),
                df_sb,
                left_on=['EIN_5500', 'PLAN_NUMBER_5500', 'PLAN_YEAR_5500'],
                right_on=['EIN', 'PLAN_NUMBER', 'PLAN_YEAR'],
                how='left',
                suffixes=('', '_SB2'),
                indicator=False
            )
            for col in df_sb.columns:
                if col in sec_merge.columns and col+'_SB' in merged.columns:
                    merged.loc[unmatched.index, col+'_SB'] = sec_merge[col]
            merged.loc[unmatched.index, 'sb_merge_flag'] = ~sec_merge['EIN'].isna()
    else:
        # No ACK_ID: merge on EIN+PLAN_NUMBER+PLAN_YEAR only
        merged = pd.merge(
            df_5500,
            df_sb,
            on=['EIN', 'PLAN_NUMBER', 'PLAN_YEAR'],
            how='outer',
            suffixes=("_5500", "_SB"),
            indicator=False
        )
        merged['sb_merge_flag'] = ~merged['EIN_SB'].isna() if 'EIN_SB' in merged.columns else False
    # Add TRACKING_ID for multi-year tracking (cleaned EIN and PLAN_NUMBER)
    for col in ['EIN', 'PLAN_NUMBER']:
        if col in merged.columns:
            merged[col] = merged[col].astype(str).str.strip()
    merged['TRACKING_ID'] = merged['EIN'] + '-' + merged['PLAN_NUMBER']

    # Merge Schedule R (outer, EIN+PLAN_NUMBER+PLAN_YEAR)
    if not df_sr.empty:
        merged = pd.merge(
            merged,
            df_sr,
            left_on=['EIN_5500', 'PLAN_NUMBER_5500', 'PLAN_YEAR_5500'],
            right_on=['EIN', 'PLAN_NUMBER', 'PLAN_YEAR'],
            how='left',
            suffixes=('', '_SR'),
            indicator=False
        )
        merged['sr_merge_flag'] = ~merged['EIN'].isna()
    else:
        merged['sr_merge_flag'] = False

    # Merge quality flag
    merged['merge_quality'] = merged['sb_merge_flag'].astype(int) * 2  # 2 if ACK/secondary match, 0 if not
    # If secondary merge only, set to 1
    if 'sb_merge_flag' in merged.columns and 'EIN_SB' in merged.columns:
        merged.loc[(merged['sb_merge_flag']) & merged['ACK_ID'].isna(), 'merge_quality'] = 1

    # Uniform Schedule R columns
    sr_cols = [
        'ASSET_EQUITY_PCT', 'ASSET_FIXED_INCOME_PCT', 'ASSET_REAL_ESTATE_PCT',
        'ASSET_ALTERNATIVES_PCT', 'ASSET_CASH_PCT', 'ANNUITY_PURCHASES',
        'TRANSFERRED_TO_INSURERS', 'BENEFITS_PAID', 'CONTRIBUTIONS'
    ]
    for col in sr_cols:
        if col not in merged.columns:
            merged[col] = None

    # Remove all interest rate/segment rate fields if present
    interest_cols = [c for c in merged.columns if 'INTEREST' in c.upper() or 'SEGMENT' in c.upper() or 'RATE' in c.upper()]
    merged = merged.drop(columns=interest_cols, errors='ignore')

    # Return clean, merge-ready DataFrame
    return merged.reset_index(drop=True)
