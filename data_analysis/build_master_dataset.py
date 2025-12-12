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



def build_master_dataset(yearly_merged_list) -> pd.DataFrame:
    import numpy as np
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    # Define the full schema for all years
    schema = [
        'EIN', 'PLAN_NUMBER', 'PLAN_YEAR', 'ACK_ID',
        'SPONSOR_DFE_NAME', 'PLAN_NAME', 'BUSINESS_CODE',
        'ACTIVE_COUNT', 'RETIREE_COUNT', 'SEPARATED_COUNT', 'TOTAL_PARTICIPANTS',
        'ACT_LIABILITY', 'RET_LIABILITY', 'TERM_LIABILITY', 'TOTAL_LIABILITY',
        'MORTALITY_CODE',
        # Schedule R
        'ASSET_EQUITY_PCT', 'ASSET_FIXED_INCOME_PCT', 'ASSET_REAL_ESTATE_PCT',
        'ASSET_ALTERNATIVES_PCT', 'ASSET_CASH_PCT', 'ANNUITY_PURCHASES',
        'TRANSFERRED_TO_INSURERS', 'BENEFITS_PAID', 'CONTRIBUTIONS'
    ]

    # Standardize and collect all years
    all_years = []
    for plan_year, merged_df in yearly_merged_list:
        df = merged_df.copy()
        df['PLAN_YEAR'] = plan_year
        for col in schema:
            if col not in df.columns:
                df[col] = None
        df = df[schema]
        # Dtype enforcement
        df['EIN'] = df['EIN'].astype(str).str.strip()
        df['PLAN_NUMBER'] = df['PLAN_NUMBER'].astype(str).str.strip()
        df['PLAN_YEAR'] = pd.to_numeric(df['PLAN_YEAR'], errors='coerce').astype('Int64')
        for c in ['ACTIVE_COUNT', 'RETIREE_COUNT', 'SEPARATED_COUNT', 'TOTAL_PARTICIPANTS']:
            df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')
        for c in ['ACT_LIABILITY', 'RET_LIABILITY', 'TERM_LIABILITY', 'TOTAL_LIABILITY',
                  'ASSET_EQUITY_PCT', 'ASSET_FIXED_INCOME_PCT', 'ASSET_REAL_ESTATE_PCT',
                  'ASSET_ALTERNATIVES_PCT', 'ASSET_CASH_PCT', 'ANNUITY_PURCHASES',
                  'TRANSFERRED_TO_INSURERS', 'BENEFITS_PAID', 'CONTRIBUTIONS']:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        all_years.append(df)

    master = pd.concat(all_years, ignore_index=True, sort=False)
    # Add TRACKING_ID for multi-year tracking (cleaned EIN and PLAN_NUMBER)
    master['EIN'] = master['EIN'].astype(str).str.strip()
    master['PLAN_NUMBER'] = master['PLAN_NUMBER'].astype(str).str.strip()
    master['TRACKING_ID'] = master['EIN'] + '-' + master['PLAN_NUMBER']

    # Compute YoY metrics for each TRACKING_ID
    master = master.sort_values(['TRACKING_ID', 'PLAN_YEAR']).reset_index(drop=True)
    group = master.groupby('TRACKING_ID')
    master['ACTIVE_YOY_CHANGE'] = group['ACTIVE_COUNT'].diff()
    master['RETIREE_YOY_CHANGE'] = group['RETIREE_COUNT'].diff()
    master['SEPARATED_YOY_CHANGE'] = group['SEPARATED_COUNT'].diff()
    master['ANNUTIANT_RATIO'] = (master['RETIREE_COUNT'] + master['SEPARATED_COUNT']) / master['TOTAL_PARTICIPANTS'].replace({0: np.nan})
    master['ANNUTIANT_RATIO_YOY_CHANGE'] = group['ANNUTIANT_RATIO'].diff()
    master['TOTAL_LIABILITY_YOY_CHANGE'] = group['TOTAL_LIABILITY'].diff()
    master['RETIREE_LIABILITY_YOY_CHANGE'] = group['RET_LIABILITY'].diff()
    master['BENEFITS_PAID_YOY_CHANGE'] = group['BENEFITS_PAID'].diff()
    master['CONTRIBUTIONS_YOY_CHANGE'] = group['CONTRIBUTIONS'].diff()
    master['EQUITY_PCT_YOY_CHANGE'] = group['ASSET_EQUITY_PCT'].diff()
    master['FIXED_INCOME_PCT_YOY_CHANGE'] = group['ASSET_FIXED_INCOME_PCT'].diff()
    master['REAL_ESTATE_PCT_YOY_CHANGE'] = group['ASSET_REAL_ESTATE_PCT'].diff()
    master['ALTERNATIVES_PCT_YOY_CHANGE'] = group['ASSET_ALTERNATIVES_PCT'].diff()

    # Multi-year behavioral flags
    # is_freezing_pattern: multi-year decline in actives
    def freezing_flag(subdf):
        return (subdf['ACTIVE_YOY_CHANGE'] < 0).sum() >= 2
    # is_annuity_purchase_pattern: decline in retirees + liability + annuity purchase > 0
    def annuity_flag(subdf):
        return ((subdf['RETIREE_YOY_CHANGE'] < 0) & (subdf['RETIREE_LIABILITY_YOY_CHANGE'] < 0) & (subdf['ANNUITY_PURCHASES'] > 0)).any()
    # asset_shift_toward_fi: increase in fixed income pct over time
    def asset_shift_flag(subdf):
        return (subdf['FIXED_INCOME_PCT_YOY_CHANGE'] > 0).sum() >= 2

    flags = master.groupby(['EIN', 'PLAN_NUMBER']).apply(lambda subdf: pd.Series({
        'is_freezing_pattern': freezing_flag(subdf),
        'is_annuity_purchase_pattern': annuity_flag(subdf),
        'asset_shift_toward_fi': asset_shift_flag(subdf)
    }))
    flags = flags.reset_index()
    master = master.merge(flags, on=['EIN', 'PLAN_NUMBER'], how='left')

    # Write output
    output_path = os.path.join('data_output', 'master_db_all_years.parquet')
    master.to_parquet(output_path, index=False)
    print(f"[✔] Saved all-years master DB → {output_path}")
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
