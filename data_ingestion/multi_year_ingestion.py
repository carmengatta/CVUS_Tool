
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

            f5500_df = load_5500_csv(f5500_path) if os.path.exists(f5500_path) else pd.DataFrame()
            sb_df = load_sb_csv(sb_path) if os.path.exists(sb_path) else pd.DataFrame()
            sr_df = load_sr_csv(sr_path) if os.path.exists(sr_path) else pd.DataFrame()

            if not f5500_df.empty:
                if not sb_df.empty:
                    sb_df = normalize_sb_fields(sb_df)
                if not sr_df.empty:
                    if HAS_SR:
                        sr_df = normalize_sr_fields(sr_df)
                    else:
                        logging.warning(f"normalize_sr_fields not available for {year}; skipping normalization for Schedule R.")
                # Merge 5500 + SB
                merged = merge_sb_5500(f5500_df, sb_df) if not sb_df.empty else f5500_df.copy()
                # Merge R (left join on ACK_ID if present, else EIN+PLAN_NUMBER+PLAN_YEAR)
                if not sr_df.empty:
                    if 'ACK_ID' in merged.columns and 'ACK_ID' in sr_df.columns:
                        merged = pd.merge(merged, sr_df, on='ACK_ID', how='left', suffixes=('', '_SR'))
                    elif {'EIN', 'PLAN_NUMBER', 'PLAN_YEAR'}.issubset(merged.columns) and {'EIN', 'PLAN_NUMBER', 'PLAN_YEAR'}.issubset(sr_df.columns):
                        merged = pd.merge(merged, sr_df, on=['EIN', 'PLAN_NUMBER', 'PLAN_YEAR'], how='left', suffixes=('', '_SR'))
                    else:
                        logging.warning(f"Could not merge Schedule R for {year}: missing merge keys.")
                merged['PLAN_YEAR'] = year
                if 'EIN' in merged.columns and 'PLAN_NUMBER' in merged.columns:
                    merged['TRACKING_ID'] = merged['EIN'].astype(str).str.strip() + '-' + merged['PLAN_NUMBER'].astype(str).str.strip()
                else:
                    merged['TRACKING_ID'] = None
                all_years.append(merged)
            else:
                logging.warning(f"No Form 5500 data for {year}; skipping year.")
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
