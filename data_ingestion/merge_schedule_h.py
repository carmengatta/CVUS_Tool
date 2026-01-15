"""
Merge Schedule H Financial Data with DB Plans Data

Joins Schedule H (financial/PRT data) with the main DB plans dataset.
"""

import pandas as pd
import logging
import sys
import os

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.naics_codes import get_naics_sector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def merge_schedule_h(db_plans: pd.DataFrame, sch_h: pd.DataFrame) -> pd.DataFrame:
    """
    Merge Schedule H financial data with DB plans data.
    
    Uses ACK_ID as the primary join key (same ACK_ID across all schedules in a filing).
    
    Args:
        db_plans: Main DB plans DataFrame (from SB + 5500)
        sch_h: Schedule H normalized DataFrame
        
    Returns:
        Merged DataFrame with Schedule H fields appended
    """
    if sch_h.empty:
        logging.warning("Schedule H data is empty, returning db_plans unchanged")
        return db_plans
    
    # Ensure consistent types for ACK_ID join key
    db_plans = db_plans.copy()
    db_plans['_ACK_ID'] = db_plans['ACK_ID'].astype(str).str.strip()
    
    sch_h = sch_h.copy()
    sch_h['_ACK_ID'] = sch_h['ACK_ID'].astype(str).str.strip()
    
    # Schedule H fields to merge (exclude join keys and temp keys)
    sch_h_fields = [col for col in sch_h.columns if col not in ['ACK_ID', 'EIN', 'PLAN_NUMBER', 'PLAN_YEAR', 'YEAR', '_ACK_ID']]
    
    # Prepare merge dataframe
    merge_cols = ['_ACK_ID'] + sch_h_fields
    sch_h_subset = sch_h[merge_cols].copy()
    
    # Prefix Schedule H columns to avoid conflicts
    rename_map = {col: f'SCH_H_{col}' for col in sch_h_fields}
    sch_h_subset = sch_h_subset.rename(columns=rename_map)
    
    # Drop duplicates on join key (keep first)
    sch_h_subset = sch_h_subset.drop_duplicates(subset=['_ACK_ID'], keep='first')
    
    merged = db_plans.merge(
        sch_h_subset,
        on=['_ACK_ID'],
        how='left'
    )
    
    # Drop temp columns
    merged = merged.drop(columns=['_ACK_ID'], errors='ignore')
    
    # Log merge statistics
    matched = merged['SCH_H_TOTAL_ASSETS_EOY'].notna().sum()
    total = len(merged)
    prt_count = (merged['SCH_H_PRT_AMOUNT'].fillna(0) > 0).sum()
    
    logging.info(f"Merged Schedule H on ACK_ID: {matched}/{total} plans matched ({matched/total*100:.1f}%)")
    logging.info(f"Plans with PRT data: {prt_count}")
    
    return merged


def add_prt_analysis_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived fields for PRT analysis.
    
    Args:
        df: Merged DataFrame with Schedule H data
        
    Returns:
        DataFrame with additional PRT analysis fields
    """
    # PRT category based on amount
    def categorize_prt(amount):
        if pd.isna(amount) or amount <= 0:
            return 'No PRT'
        elif amount < 10_000_000:
            return 'Small (<$10M)'
        elif amount < 100_000_000:
            return 'Medium ($10M-$100M)'
        elif amount < 500_000_000:
            return 'Large ($100M-$500M)'
        else:
            return 'Mega (>$500M)'
    
    df['PRT_CATEGORY'] = df['SCH_H_PRT_AMOUNT'].apply(categorize_prt)
    
    # Asset size category
    def categorize_assets(amount):
        if pd.isna(amount) or amount <= 0:
            return 'Unknown'
        elif amount < 10_000_000:
            return 'Small (<$10M)'
        elif amount < 100_000_000:
            return 'Medium ($10M-$100M)'
        elif amount < 500_000_000:
            return 'Large ($100M-$500M)'
        elif amount < 1_000_000_000:
            return 'Very Large ($500M-$1B)'
        else:
            return 'Mega (>$1B)'
    
    df['ASSET_SIZE_CATEGORY'] = df['SCH_H_TOTAL_ASSETS_EOY'].apply(categorize_assets)
    
    # PRT readiness score (higher = more likely candidate)
    # Factors: high retiree %, funded status > 80%, large assets, not already using substitute mortality
    df['PRT_READINESS_SCORE'] = 0.0
    
    # Retiree percentage factor (higher is better for PRT)
    if 'RETIREE_PCT' in df.columns:
        df['PRT_READINESS_SCORE'] += (df['RETIREE_PCT'].fillna(0) / 100) * 30
    
    # Funding status factor
    if 'FUNDING_TARGET_PCT' in df.columns:
        # Bonus for >80% funded, penalty for <60%
        df.loc[df['FUNDING_TARGET_PCT'] >= 80, 'PRT_READINESS_SCORE'] += 20
        df.loc[df['FUNDING_TARGET_PCT'] >= 95, 'PRT_READINESS_SCORE'] += 10
    
    # Asset size factor (larger plans = more PRT activity)
    df.loc[df['SCH_H_TOTAL_ASSETS_EOY'] >= 100_000_000, 'PRT_READINESS_SCORE'] += 10
    df.loc[df['SCH_H_TOTAL_ASSETS_EOY'] >= 500_000_000, 'PRT_READINESS_SCORE'] += 10
    df.loc[df['SCH_H_TOTAL_ASSETS_EOY'] >= 1_000_000_000, 'PRT_READINESS_SCORE'] += 10
    
    # Not using substitute mortality = potential opportunity
    if 'MORTALITY_CODE' in df.columns:
        df.loc[df['MORTALITY_CODE'] != 3, 'PRT_READINESS_SCORE'] += 20
    
    # Add INDUSTRY_SECTOR if BUSINESS_CODE exists
    if 'BUSINESS_CODE' in df.columns:
        df['INDUSTRY_SECTOR'] = df['BUSINESS_CODE'].apply(get_naics_sector)
    
    # Round score
    df['PRT_READINESS_SCORE'] = df['PRT_READINESS_SCORE'].round(1)
    
    return df


if __name__ == "__main__":
    import os
    
    # Test the merge
    year = 2024
    db_path = f"../data_output/yearly/db_plans_{year}.parquet"
    
    if os.path.exists(db_path):
        db_plans = pd.read_parquet(db_path)
        print(f"Loaded {len(db_plans)} DB plans from {year}")
        
        # Load Schedule H
        from normalize_sch_h_fields import load_and_normalize_sch_h
        sch_h_path = f"../data_raw/F_SCH_H_{year}_latest.csv"
        sch_h = load_and_normalize_sch_h(sch_h_path, year=year)
        
        # Merge
        merged = merge_schedule_h(db_plans, sch_h)
        merged = add_prt_analysis_fields(merged)
        
        print(f"\nPRT Categories:")
        print(merged['PRT_CATEGORY'].value_counts())
        
        print(f"\nAsset Size Categories:")
        print(merged['ASSET_SIZE_CATEGORY'].value_counts())
        
        print(f"\nTop 10 PRT Readiness Scores (no PRT yet):")
        candidates = merged[merged['PRT_CATEGORY'] == 'No PRT'].nlargest(10, 'PRT_READINESS_SCORE')
        print(candidates[['SPONSOR_DFE_NAME', 'SCH_H_TOTAL_ASSETS_EOY', 'PRT_READINESS_SCORE']].to_string())
