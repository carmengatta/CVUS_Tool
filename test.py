"""
End-to-end test for Form 5500 + Schedule SB pipeline.

This script:
1. Loads and normalizes SB actuarial data
2. Loads Form 5500 sponsor metadata
3. Merges the two using ACK_ID
4. Validates dataset alignment
5. Builds the Master Enriched DB Dataset
6. Builds a Sponsor-Level Consolidated Dataset (sum by EIN)
7. Saves outputs (CSV + Parquet)
"""

from data_ingestion.combine_years import combine_years
from data_ingestion.normalize_sb_fields import normalize_sb_fields
from data_ingestion.merge_sb_5500 import merge_sb_5500
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


def run_test():

    # ---------------------------------------------------------
    # LOAD & NORMALIZE SB DATA
    # ---------------------------------------------------------
    print("\nLoading SB dataset...")
    sb_df = combine_years("data_raw/F_SCH_SB_*.csv")
    sb_df = normalize_sb_fields(sb_df)

    print("\nSB sample:")
    print(sb_df.head())

    # ---------------------------------------------------------
    # LOAD FORM 5500 DATA
    # ---------------------------------------------------------
    print("\nLoading Form 5500 dataset...")
    f5500_df = combine_years("data_raw/F_5500_*.csv")

    print("\n5500 sample:")
    print(f5500_df.head())

    # ---------------------------------------------------------
    # MERGE SB + 5500
    # ---------------------------------------------------------
    print("\nMerging using ACK_ID...")
    merged = merge_sb_5500(sb_df, f5500_df)

    print("\nMerged sample (first 5 rows):")
    print(merged[[
        "ack_id", "ein", "plan_number",
        "active", "retired", "terminated", "liability_total",
        "sponsor_dfe_name", "business_code",
        "ein_match", "pn_match", "merge_warning"
    ]].head())

    print("\nSUCCESS: ACK_ID merge test completed.")

    # ---------------------------------------------------------
    # VALIDATE ALIGNMENT
    # ---------------------------------------------------------
    print("\nRunning dataset alignment validation...")
    results = validate_alignment(sb_df, f5500_df)

    print("\nAlignment Report:")
    for key, value in results.items():
        print(f"{key}: {value}")

    # ---------------------------------------------------------
    # BUILD MASTER ENRICHED DATASET
    # ---------------------------------------------------------
    print("\nBuilding Master Enriched DB Dataset...")
    master_df = build_master_dataset(sb_df, f5500_df)

    print("\n=== MASTER DATASET PREVIEW (Top 20 by Retiree Count) ===")
    print(master_df.head(20))

    print("\nMaster dataset shape:", master_df.shape)

    # ---------------------------------------------------------
    # SAVE MASTER OUTPUTS
    # ---------------------------------------------------------
    print("\nSaving master dataset outputs...")
    save_master_as_parquet(master_df)
    save_master_as_csv(master_df)

    print("\nMaster dataset saved successfully.")

    # ---------------------------------------------------------
    # BUILD SPONSOR ROLLUP (BY EIN)
    # ---------------------------------------------------------
    print("\nBuilding Sponsor-Level Rollup Dataset...")
    sponsor_df = build_sponsor_rollup(master_df)

    print("\n=== SPONSOR ROLLUP PREVIEW (Top 20) ===")
    print(sponsor_df.head(20))

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
