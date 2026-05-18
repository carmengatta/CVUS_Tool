"""
End-to-end test for Form 5500 + Schedule SB pipeline.

This script:
1. Loads and normalizes SB actuarial data (via multi-year pipeline)
2. Loads Form 5500 sponsor metadata
3. Merges SB + 5500
4. Validates dataset alignment
5. Builds the Master Enriched DB Dataset
6. Builds a Sponsor-Level Consolidated Dataset (sum by EIN)
7. Saves outputs (CSV + Parquet)
"""

import os
import logging
import pandas as pd

from data_ingestion.load_csv import load_csv, load_5500_csv
from data_ingestion.normalize_sb_fields import normalize_sb_fields
from data_ingestion.normalize_sr_fields import normalize_sr_fields
from data_ingestion.merge_sb_5500 import merge_sb_5500
from data_ingestion.merge_sb_sr import merge_sb_sr
from utils.validate_alignment import validate_alignment

from data_analysis.build_master_dataset import (
    build_master_dataset,
    save_master_as_parquet,
    save_master_as_csv
)

from data_analysis.build_sponsor_rollup import (
    build_sponsor_rollup,
    save_sponsor_rollup_parquet,
    save_sponsor_rollup_csv
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

DATA_RAW = "data_raw"
YEARS = list(range(2019, 2025))


def run_test():

    yearly_merged = []

    for year in YEARS:
        sb_path = os.path.join(DATA_RAW, f"F_SCH_SB_{year}_latest.csv")
        f5500_path = os.path.join(DATA_RAW, f"f_5500_{year}_latest.csv")
        sr_path = os.path.join(DATA_RAW, f"F_SCH_R_{year}_latest.csv")

        if not os.path.exists(sb_path):
            logging.warning(f"No SB file for {year}, skipping.")
            continue

        # ---------------------------------------------------------
        # LOAD & NORMALIZE SB DATA
        # ---------------------------------------------------------
        print(f"\n[Year {year}] Loading SB dataset...")
        sb_df = load_csv(sb_path, year)
        sb_df = normalize_sb_fields(sb_df, plan_year=year)
        print(f"[Year {year}] SB rows: {len(sb_df)}")

        # ---------------------------------------------------------
        # LOAD FORM 5500 DATA
        # ---------------------------------------------------------
        print(f"[Year {year}] Loading Form 5500 dataset...")
        f5500_df = load_csv(f5500_path, year) if os.path.exists(f5500_path) else pd.DataFrame()

        # ---------------------------------------------------------
        # VALIDATE ALIGNMENT
        # ---------------------------------------------------------
        if not f5500_df.empty:
            print(f"[Year {year}] Running dataset alignment validation...")
            results = validate_alignment(sb_df, f5500_df)
            print(f"[Year {year}] ACK_ID match: {results['ack_match_pct']:.1f}% — {results['diagnosis']}")

        # ---------------------------------------------------------
        # MERGE SB + 5500
        # ---------------------------------------------------------
        print(f"[Year {year}] Merging SB + 5500...")
        merged = merge_sb_5500(sb_df, f5500_df)
        print(f"[Year {year}] Merged rows: {len(merged)}")

        # ---------------------------------------------------------
        # MERGE SR (optional)
        # ---------------------------------------------------------
        if os.path.exists(sr_path):
            print(f"[Year {year}] Loading and merging Schedule R...")
            sr_df = load_csv(sr_path, year)
            sr_df = normalize_sr_fields(sr_df)
            merged = merge_sb_sr(merged, sr_df)
            print(f"[Year {year}] After SR merge: {len(merged)} rows")

        # ---------------------------------------------------------
        # VALIDATE MERGED DATA
        # ---------------------------------------------------------
        print(f"[Year {year}] Sample columns: {list(merged.columns[:10])}")
        preview_cols = [c for c in [
            "ACK_ID", "EIN", "PLAN_NUMBER",
            "ACTIVE_COUNT", "RETIREE_COUNT", "SEPARATED_COUNT", "TOTAL_LIABILITY",
            "SPONSOR_DFE_NAME", "BUSINESS_CODE",
        ] if c in merged.columns]
        print(merged[preview_cols].head())

        yearly_merged.append((year, merged))

    if not yearly_merged:
        print("\nERROR: No years could be processed. Check data_raw/ for CSV files.")
        return

    print(f"\nSUCCESS: Processed {len(yearly_merged)} years.")

    # ---------------------------------------------------------
    # BUILD MASTER ENRICHED DATASET
    # ---------------------------------------------------------
    print("\nBuilding Master Enriched DB Dataset...")
    master_df = build_master_dataset(yearly_merged)

    print("\n=== MASTER DATASET PREVIEW (Top 10 by Retiree Count) ===")
    if "RETIREE_COUNT" in master_df.columns:
        print(master_df.nlargest(10, "RETIREE_COUNT")[
            [c for c in ["EIN", "PLAN_NUMBER", "PLAN_YEAR", "RETIREE_COUNT", "TOTAL_LIABILITY"] if c in master_df.columns]
        ])

    print("\nMaster dataset shape:", master_df.shape)

    # ---------------------------------------------------------
    # SAVE MASTER OUTPUTS
    # ---------------------------------------------------------
    print("\nSaving master dataset outputs...")
    save_master_as_parquet(master_df)
    save_master_as_csv(master_df)

    # ---------------------------------------------------------
    # BUILD SPONSOR ROLLUP (BY EIN)
    # ---------------------------------------------------------
    print("\nBuilding Sponsor-Level Rollup Dataset...")
    sponsor_df = build_sponsor_rollup(master_df)

    print("\n=== SPONSOR ROLLUP PREVIEW (Top 10) ===")
    print(sponsor_df.head(10))

    print("\nSponsor dataset shape:", sponsor_df.shape)

    # ---------------------------------------------------------
    # SAVE SPONSOR ROLLUP OUTPUTS
    # ---------------------------------------------------------
    print("\nSaving sponsor rollup outputs...")
    save_sponsor_rollup_parquet(sponsor_df)
    save_sponsor_rollup_csv(sponsor_df)

    print("\n=== FULL PIPELINE TEST COMPLETE ===\n")


if __name__ == "__main__":
    run_test()
