
from data_ingestion.load_csv import load_5500_csv, load_sb_csv, load_sr_csv
import logging
try:
    from data_ingestion.normalize_sr_fields import normalize_sr_fields
    HAS_SR = True
except ImportError:
    HAS_SR = False
    def normalize_sr_fields(df):
        logging.warning("normalize_sr_fields not available; skipping normalization for Schedule R.")
        return df


def load_multi_year_data():
    """
    Loads, normalizes, and merges Form 5500, Schedule SB, and Schedule R data for years 2019–2024.
    Returns a single concatenated DataFrame for all years.
    """
    all_years = []
    for year in range(2019, 2025):
        try:
            f5500_path = os.path.join(DATA_RAW, f"f_5500_{year}_latest.csv")
            sb_path = os.path.join(DATA_RAW, f"F_SCH_SB_{year}_latest.csv")
            sr_path = os.path.join(DATA_RAW, f"F_SCH_R_{year}_latest.csv")

            sb_df = load_sb_csv(sb_path) if os.path.exists(sb_path) else pd.DataFrame()
            if sb_df.empty:
                logging.warning(f"No Schedule SB data for {year}; skipping year.")
                continue
            sb_df = normalize_sb_fields(sb_df, plan_year=year)
            # Create unique key for SB
            sb_df['EIN'] = sb_df['EIN'].astype(str).str.strip()
            sb_df['PLAN_NUMBER'] = sb_df['PLAN_NUMBER'].astype(str).str.strip().str.zfill(3)
            sb_df['PLAN_YEAR'] = year
            sb_df['SB_KEY'] = sb_df['EIN'] + '-' + sb_df['PLAN_NUMBER'] + '-' + sb_df['PLAN_YEAR'].astype(str)

            # Load and filter 5500
            f5500_df = load_5500_csv(f5500_path) if os.path.exists(f5500_path) else pd.DataFrame()
            if not f5500_df.empty:
                f5500_df['EIN'] = f5500_df['EIN'].astype(str).str.strip()
                f5500_df['PLAN_NUMBER'] = f5500_df['PLAN_NUMBER'].astype(str).str.strip().str.zfill(3)
                f5500_df['PLAN_YEAR'] = year
                f5500_df['SB_KEY'] = f5500_df['EIN'] + '-' + f5500_df['PLAN_NUMBER'] + '-' + f5500_df['PLAN_YEAR'].astype(str)
                f5500_df = f5500_df[f5500_df['SB_KEY'].isin(sb_df['SB_KEY'])]
            else:
                f5500_df = pd.DataFrame(columns=sb_df.columns)

            # Load and filter SR
            sr_df = load_sr_csv(sr_path) if os.path.exists(sr_path) else pd.DataFrame()
            if not sr_df.empty:
                if HAS_SR:
                    sr_df = normalize_sr_fields(sr_df)
                else:
                    logging.warning(f"normalize_sr_fields not available for {year}; skipping normalization for Schedule R.")
                sr_df['EIN'] = sr_df['EIN'].astype(str).str.strip()
                sr_df['PLAN_NUMBER'] = sr_df['PLAN_NUMBER'].astype(str).str.strip().str.zfill(3)
                sr_df['PLAN_YEAR'] = year
                sr_df['SB_KEY'] = sr_df['EIN'] + '-' + sr_df['PLAN_NUMBER'] + '-' + sr_df['PLAN_YEAR'].astype(str)
                sr_df = sr_df[sr_df['SB_KEY'].isin(sb_df['SB_KEY'])]
            else:
                sr_df = pd.DataFrame(columns=sb_df.columns)

            # Use SB as base, left-merge 5500 and SR onto SB
            merged = sb_df.copy()
            if not f5500_df.empty:
                merged = pd.merge(merged, f5500_df, on=['EIN', 'PLAN_NUMBER', 'PLAN_YEAR'], how='left', suffixes=('', '_5500'))
            if not sr_df.empty:
                merged = pd.merge(merged, sr_df, on=['EIN', 'PLAN_NUMBER', 'PLAN_YEAR'], how='left', suffixes=('', '_SR'))

            merged['TRACKING_ID'] = merged['EIN'] + '-' + merged['PLAN_NUMBER']
            merged['PLAN_YEAR'] = year

            # Output Parquet for this year
            yearly_dir = os.path.join(DATA_OUTPUT, 'yearly')
            os.makedirs(yearly_dir, exist_ok=True)
            out_path = os.path.join(yearly_dir, f"merged_{year}.parquet")
            merged.to_parquet(out_path, index=False)
            logging.info(f"Wrote {out_path}")
            all_years.append(merged)
        except Exception as e:
            logging.warning(f"Error processing year {year}: {e}")
            continue
    if all_years:
        return pd.concat(all_years, ignore_index=True, sort=False)
    else:
        return pd.DataFrame()
"""
Multi-Year Ingestion Engine

This script loads, normalizes, and merges Form 5500, Schedule SB, and Schedule R data for all available years (2019–2024), producing unified master and sponsor rollup datasets.
"""

import os
import pandas as pd
from glob import glob
from data_ingestion.load_csv import load_5500_csv
from data_ingestion.load_csv import load_5500_csv as load_sb_csv  # If SB uses same loader
from data_ingestion.load_csv import load_5500_csv as load_r_csv   # If R uses same loader
from data_ingestion.normalize_sb_fields import normalize_sb_fields
from data_ingestion.merge_sb_5500 import merge_sb_5500
from data_analysis.build_sponsor_rollup import build_sponsor_rollup, save_sponsor_rollup_parquet

DATA_RAW = "data_raw"
DATA_OUTPUT = "data_output"
YEARS = list(range(2019, 2025))

master_dfs = []
sponsor_rollups = []

for year in YEARS:
    # Detect files
    f5500_files = glob(os.path.join(DATA_RAW, f"f_5500_{year}_latest.csv"))
    sb_files = glob(os.path.join(DATA_RAW, f"F_SCH_SB_{year}_latest.csv"))
    r_files = glob(os.path.join(DATA_RAW, f"F_SCH_R_{year}_latest.csv"))
    if not f5500_files:
        print(f"[!] Missing Form 5500 for {year}, skipping year.")
        continue
    print(f"[+] Loading {year}...")
    # Load and normalize
    f5500_df = load_5500_csv(f5500_files[0])
    sb_df = load_sb_csv(sb_files[0]) if sb_files else pd.DataFrame()
    r_df = load_r_csv(r_files[0]) if r_files else pd.DataFrame()
    # Normalize SB
    if not sb_df.empty:
        sb_df = normalize_sb_fields(sb_df)
    # Merge 5500 + SB
    if not sb_df.empty:
        merged, _ = merge_sb_5500(sb_df, f5500_df) if isinstance(merge_sb_5500(sb_df, f5500_df), tuple) else (merge_sb_5500(sb_df, f5500_df), None)
    else:
        merged = f5500_df.copy()
    # Merge R if available (left join on EIN, PLAN_NUMBER)
    if not r_df.empty and {'EIN', 'PLAN_NUMBER'}.issubset(merged.columns) and {'EIN', 'PLAN_NUMBER'}.issubset(r_df.columns):
        merged = pd.merge(merged, r_df, on=['EIN', 'PLAN_NUMBER'], how='left', suffixes=('', '_R'))
    # Add PLAN_YEAR
    merged['PLAN_YEAR'] = year
    master_dfs.append(merged)
    # Sponsor rollup
    try:
        sponsor_rollup = build_sponsor_rollup(merged)
        sponsor_rollup['PLAN_YEAR'] = year
        sponsor_rollups.append(sponsor_rollup)
        # Save yearly rollup
        save_sponsor_rollup_parquet(sponsor_rollup, filename=f"sponsor_rollup_{year}.parquet")
    except Exception as e:
        print(f"[!] Sponsor rollup failed for {year}: {e}")

# Combine all years
if master_dfs:
    master_all = pd.concat(master_dfs, ignore_index=True, sort=False)
    master_all.to_parquet(os.path.join(DATA_OUTPUT, "master_db_all_years.parquet"), index=False)
    print(f"[✔] Saved all-years master DB → {os.path.join(DATA_OUTPUT, 'master_db_all_years.parquet')}")
if sponsor_rollups:
    sponsor_all = pd.concat(sponsor_rollups, ignore_index=True, sort=False)
    sponsor_all.to_parquet(os.path.join(DATA_OUTPUT, "sponsor_rollup_all_years.parquet"), index=False)
    print(f"[✔] Saved all-years sponsor rollup → {os.path.join(DATA_OUTPUT, 'sponsor_rollup_all_years.parquet')}")
