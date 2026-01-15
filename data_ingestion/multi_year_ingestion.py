"""
Form 5500 Data Ingestion: Multi-Year Pipeline

Responsibilities:
- For each year (2019–2024):
    - Load SB, 5500, SR
    - Normalize each dataset
    - Merge SB → 5500 → SR
    - Add YEAR
    - Add TRACKING_ID
    - Validate row counts
- Concatenate all years
- Enforce no duplicate (TRACKING_ID, YEAR)
- Write final parquet outputs
- Add explicit validation logging
"""

import os
import pandas as pd
from .load_csv import load_csv
from .normalize_sb_fields import normalize_sb_fields
from .normalize_sr_fields import normalize_sr_fields
from .normalize_sch_h_fields import load_and_normalize_sch_h
from .merge_sb_5500 import merge_sb_5500
from .merge_sb_sr import merge_sb_sr
from .merge_schedule_h import merge_schedule_h, add_prt_analysis_fields

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data_raw")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data_output", "yearly")

YEARS = list(range(2019, 2025))

SB_PREFIX = "F_SCH_SB_"
F5500_PREFIX = "F_5500_"
SR_PREFIX = "F_SCH_R_"
SCH_H_PREFIX = "F_SCH_H_"

SB_TERM_PARTCP_CNT = "SB_TERM_PARTCP_CNT"

def process_year(year: int, include_schedule_h: bool = True) -> pd.DataFrame:
    """
    Process a single year: load, normalize, merge, validate, and return merged DataFrame.
    
    Args:
        year: Year to process
        include_schedule_h: Whether to include Schedule H data (PRT, assets)
    """
    # Filepaths
    sb_path = os.path.join(RAW_DIR, f"{SB_PREFIX}{year}_latest.csv")
    f5500_path = os.path.join(RAW_DIR, f"{F5500_PREFIX}{year}_latest.csv")
    sr_path = os.path.join(RAW_DIR, f"{SR_PREFIX}{year}_latest.csv")
    sch_h_path = os.path.join(RAW_DIR, f"{SCH_H_PREFIX}{year}_latest.csv")

    # Load
    sb = load_csv(sb_path, year)
    f5500 = load_csv(f5500_path, year)
    sr = load_csv(sr_path, year)

    # Normalize
    sb = normalize_sb_fields(sb)
    # Form 5500 keys are normalized by load_csv():
    # - Uppercase column names
    # - String dtype for identifiers
    # - Trimmed values
    sr = normalize_sr_fields(sr)

    # Merge SB ↔ 5500
    merged = merge_sb_5500(sb, f5500)

    # Merge SB ↔ SR with ACK_ID fallback logic
    merged_sr = merge_sb_sr(merged, sr)

    # Merge Schedule H (PRT and asset data) if available
    if include_schedule_h and os.path.exists(sch_h_path):
        sch_h = load_and_normalize_sch_h(sch_h_path, year=year)
        if not sch_h.empty:
            merged_sr = merge_schedule_h(merged_sr, sch_h)
            merged_sr = add_prt_analysis_fields(merged_sr)
            print(f"[Year {year}] Schedule H merged: {(merged_sr['SCH_H_TOTAL_ASSETS_EOY'].notna()).sum()} plans with asset data")

    # Add TRACKING_ID
    merged_sr["TRACKING_ID"] = merged_sr["EIN"].astype(str) + "-" + merged_sr["PLAN_NUMBER"].astype(str)

    # Validation: No null EIN/PLAN_NUMBER, no duplicate keys
    assert merged_sr["EIN"].notnull().all(), f"Null EINs in year {year} after merge."
    assert merged_sr["PLAN_NUMBER"].notnull().all(), f"Null PLAN_NUMBERs in year {year} after merge."
    assert merged_sr[["TRACKING_ID", "YEAR"]].duplicated().sum() == 0, f"Duplicate (TRACKING_ID, YEAR) in year {year}."

    # Validation: Approximate % of Form 5500 rows dropped
    pct_dropped = 1 - (len(merged) / len(f5500)) if len(f5500) > 0 else 0
    print(f"[Year {year}] Approx. Form 5500 rows dropped: {pct_dropped:.2%} ({len(f5500) - len(merged)}/{len(f5500)})")
    print(f"[Year {year}] SB row count: {len(sb)}, merged row count: {len(merged)}")

    # Validation: SEPARATED_COUNT must come only from SB
    if SB_TERM_PARTCP_CNT in merged_sr.columns:
        assert merged_sr[SB_TERM_PARTCP_CNT].notnull().all(), f"Null SB_TERM_PARTCP_CNT in year {year}."

    # Write annual output
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"db_plans_{year}.parquet")
    merged_sr.to_parquet(out_path, index=False)
    print(f"[Year {year}] Wrote {out_path} ({len(merged_sr)} rows)")
    return merged_sr

def run_multi_year_pipeline():
    """
    Run the full multi-year pipeline and write master output.
    """
    all_years = []
    for year in YEARS:
        df = process_year(year)
        all_years.append(df)
    master = pd.concat(all_years, ignore_index=True)
    # Final validation: no duplicate (TRACKING_ID, YEAR)
    assert master[["TRACKING_ID", "YEAR"]].duplicated().sum() == 0, "Duplicate (TRACKING_ID, YEAR) in master dataset."
    master_out = os.path.join(OUTPUT_DIR, "db_plans_master.parquet")
    master.to_parquet(master_out, index=False)
    print(f"[ALL YEARS] Wrote {master_out} ({len(master)} rows)")

# Example usage (for testing only, remove in production):
# run_multi_year_pipeline()