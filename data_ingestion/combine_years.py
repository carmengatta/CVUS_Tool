import os
import glob
import logging
import re
import pandas as pd
from data_ingestion.load_csv import load_5500_csv

# -------------------------------------------
# Utility: extract year from filename
# -------------------------------------------
def extract_year_from_filename(filename: str) -> int:
    """
    Extract a 4-digit year anywhere in the filename.
    Works for files like:
        F_5500_2024_latest.csv
        F_SCH_SB_2023_v2.csv
    """
    match = re.search(r"(19|20)\d{2}", filename)
    if match:
        return int(match.group(0))
    raise ValueError(f"No year found in filename: {filename}")


# -------------------------------------------
# Required fields for validation
# -------------------------------------------
REQUIRED_SB_FIELDS = {'EIN', 'PLAN_NUMBER'}
REQUIRED_5500_FIELDS = {'EIN', 'PLAN_NUMBER'}


# -------------------------------------------
# Validation for Form 5500 chunks
# -------------------------------------------
def validate_5500_chunk(chunk, year):
    # Check for required fields
    missing_fields = REQUIRED_5500_FIELDS - set(chunk.columns)
    if missing_fields:
        logging.error(f"Year {year}: Missing required 5500 fields: {missing_fields}")
        raise ValueError(f"Missing required 5500 fields: {missing_fields}")

    # Drop rows with missing EIN / PLAN_NUMBER
    before = len(chunk)
    chunk = chunk.dropna(subset=['EIN', 'PLAN_NUMBER'])
    after = len(chunk)

    if after < before:
        logging.warning(f"Year {year}: Dropped {before - after} rows with missing EIN or PLAN_NUMBER.")

    # EIN and PLAN_NUMBER must be digits
    mask = chunk['EIN'].str.isdigit() & chunk['PLAN_NUMBER'].str.isdigit()
    malformed = (~mask).sum()

    if malformed > 0:
        logging.warning(f"Year {year}: Dropped {malformed} rows with malformed EIN or PLAN_NUMBER.")

    chunk = chunk[mask]
    return chunk


# -------------------------------------------
# Validation for SB chunks
# -------------------------------------------
def validate_sb_chunk(chunk, year):
    missing_fields = REQUIRED_SB_FIELDS - set(chunk.columns)
    if missing_fields:
        logging.error(f"Year {year}: Missing required SB fields: {missing_fields}")
        raise ValueError(f"Missing required SB fields: {missing_fields}")

    before = len(chunk)
    chunk = chunk.dropna(subset=['EIN', 'PLAN_NUMBER'])
    after = len(chunk)

    if after < before:
        logging.warning(f"Year {year}: Dropped {before - after} SB rows with missing EIN or PLAN_NUMBER.")

    mask = chunk['EIN'].str.isdigit() & chunk['PLAN_NUMBER'].str.isdigit()
    malformed = (~mask).sum()

    if malformed > 0:
        logging.warning(f"Year {year}: Dropped {malformed} SB rows with malformed EIN or PLAN_NUMBER.")

    chunk = chunk[mask]
    return chunk


# -------------------------------------------
# NEW: Multi-year ingestion engine for 2019–2024
# -------------------------------------------
def combine_years(
    years=range(2019, 2025),
    data_dir="data_raw",
    output_dir="data_output/yearly",
    load_5500=None,
    load_sb=None,
    load_sr=None,
    normalize_sb=None,
    merge_func=None
):
    """
    Multi-year ingestion engine used by main_multi_year.py

    Loads 5500, SB, and SR for each year.
    Applies SB normalization.
    Merges using merge_func (ACK_ID within-year; TRACKING_ID added later).
    Writes yearly parquet files.
    Returns list of (year, merged_df).
    """

    os.makedirs(output_dir, exist_ok=True)
    results = []

    for year in years:
        logging.info(f"Processing year {year}")

        f5500_path = os.path.join(data_dir, f"f_5500_{year}_latest.csv")
        sb_path    = os.path.join(data_dir, f"F_SCH_SB_{year}_latest.csv")
        sr_path    = os.path.join(data_dir, f"F_SCH_R_{year}_latest.csv")

        # Load SB and normalize
        df_sb = load_sb(sb_path) if load_sb and os.path.exists(sb_path) else None
        if df_sb is None or df_sb.empty:
            logging.warning(f"No Schedule SB data for {year}; skipping year.")
            continue
        if normalize_sb:
            df_sb = normalize_sb(df_sb, plan_year=year)
        df_sb['EIN'] = df_sb['EIN'].astype(str).str.strip()
        df_sb['PLAN_NUMBER'] = df_sb['PLAN_NUMBER'].astype(str).str.strip().str.zfill(3)
        df_sb['PLAN_YEAR'] = year
        df_sb['SB_KEY'] = df_sb['EIN'] + '-' + df_sb['PLAN_NUMBER'] + '-' + df_sb['PLAN_YEAR'].astype(str)

        # Load and filter 5500
        df_5500 = load_5500(f5500_path) if load_5500 and os.path.exists(f5500_path) else None
        if df_5500 is not None and not df_5500.empty:
            df_5500['EIN'] = df_5500['EIN'].astype(str).str.strip()
            df_5500['PLAN_NUMBER'] = df_5500['PLAN_NUMBER'].astype(str).str.strip().str.zfill(3)
            df_5500['PLAN_YEAR'] = year
            df_5500['SB_KEY'] = df_5500['EIN'] + '-' + df_5500['PLAN_NUMBER'] + '-' + df_5500['PLAN_YEAR'].astype(str)
            df_5500 = df_5500[df_5500['SB_KEY'].isin(df_sb['SB_KEY'])]
        else:
            df_5500 = pd.DataFrame(columns=df_sb.columns)

        # Load and filter SR
        df_sr = load_sr(sr_path) if load_sr and os.path.exists(sr_path) else None
        if df_sr is not None and not df_sr.empty:
            df_sr['EIN'] = df_sr['EIN'].astype(str).str.strip()
            df_sr['PLAN_NUMBER'] = df_sr['PLAN_NUMBER'].astype(str).str.strip().str.zfill(3)
            df_sr['PLAN_YEAR'] = year
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
        merged['PLAN_YEAR'] = year

        out_path = os.path.join(output_dir, f"merged_{year}.parquet")
        merged.to_parquet(out_path, index=False)
        logging.info(f"Wrote {out_path}")
        results.append((year, merged))

    return results


# -------------------------------------------
# OPTIONAL: SB combiner for future use
# -------------------------------------------
def combine_sb_years(years, data_dir, file_prefix, file_suffix, output_file, chunk_size=500_000):
    """
    Optional helper: chunked reader for SB files, with validation.
    Not used in the current multi-year engine, but preserved for future utilities.
    """

    def year_chunks():
        for year in years:
            file_path = os.path.join(data_dir, f"{file_prefix}{year}{file_suffix}")
            logging.info(f"Loading {file_path}")

            try:
                for chunk in pd.read_csv(file_path, low_memory=False, chunksize=chunk_size, dtype=str):
                    chunk['YEAR'] = year
                    try:
                        chunk = validate_sb_chunk(chunk, year)
                        yield chunk
                    except Exception as e:
                        logging.error(f"Error validating SB chunk from {file_path}: {e}")

            except FileNotFoundError:
                logging.error(f"File not found: {file_path}")
            except Exception as e:
                logging.error(f"Error reading {file_path}: {e}")

    first = True
    for chunk in year_chunks():
        chunk.to_csv(output_file, mode='w' if first else 'a', index=False, header=first)
        first = False

    logging.info(f"Combined SB written to {output_file}")
