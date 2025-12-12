"""
File: data_ingestion/combine_years.py


import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

REQUIRED_SB_FIELDS = {'EIN', 'PLAN_NUMBER'}
REQUIRED_5500_FIELDS = {'EIN', 'PLAN_NUMBER'}


def validate_5500_chunk(chunk, year):
    # Check for required fields
    missing_fields = REQUIRED_5500_FIELDS - set(chunk.columns)
    if missing_fields:
        logging.error(f"Year {year}: Missing required 5500 fields: {missing_fields}")
        raise ValueError(f"Missing required 5500 fields: {missing_fields}")
    # Drop rows with missing EIN or PLAN_NUMBER
    before = len(chunk)
    chunk = chunk.dropna(subset=['EIN', 'PLAN_NUMBER'])
    after = len(chunk)
    if after < before:
        logging.warning(f"Year {year}: Dropped {before - after} rows with missing EIN or PLAN_NUMBER.")
    # Check for malformed EIN/PLAN_NUMBER (should be digits)
    mask = chunk['EIN'].str.isdigit() & chunk['PLAN_NUMBER'].str.isdigit()
    malformed = (~mask).sum()
    if malformed > 0:
        logging.warning(f"Year {year}: Dropped {malformed} rows with malformed EIN or PLAN_NUMBER.")
    chunk = chunk[mask]
    return chunk

<<<<<<< HEAD
def combine_5500_years(years, data_dir, file_prefix, file_suffix, output_file, chunk_size=500_000):
    def year_chunks():
        for year in years:
            file_path = os.path.join(data_dir, f"{file_prefix}{year}{file_suffix}")
            logging.info(f"Loading {file_path}")
            try:
                for chunk in pd.read_csv(file_path, low_memory=False, chunksize=chunk_size, dtype=str):
                    chunk['YEAR'] = year
                    try:
                        chunk = validate_5500_chunk(chunk, year)
                        yield chunk
                    except Exception as e:
                        logging.error(f"Error validating chunk from {file_path}: {e}")
            except FileNotFoundError:
                logging.error(f"File not found: {file_path}")
            except Exception as e:
                logging.error(f"Error reading {file_path}: {e}")

    first_chunk = True
    for chunk in year_chunks():
        chunk.to_csv(output_file, mode='w' if first_chunk else 'a', index=False, header=first_chunk)
        first_chunk = False
    logging.info(f"Combined file written to {output_file}")
=======

# Multi-year combine function for 2019–2024, no interest fields, interim outputs, returns list of (year, merged_df)
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
    os.makedirs(output_dir, exist_ok=True)
    results = []
    for year in years:
        logging.info(f"Processing year {year}")
        f5500_path = os.path.join(data_dir, f"f_5500_{year}_latest.csv")
        sb_path = os.path.join(data_dir, f"F_SCH_SB_{year}_latest.csv")
        sr_path = os.path.join(data_dir, f"F_SCH_R_{year}_latest.csv")
        # Load
        df_5500 = load_5500(f5500_path) if load_5500 and os.path.exists(f5500_path) else None
        df_sb = load_sb(sb_path) if load_sb and os.path.exists(sb_path) else None
        df_sr = load_sr(sr_path) if load_sr and os.path.exists(sr_path) else None
        # Normalize SB
        if df_sb is not None and normalize_sb:
            df_sb = normalize_sb(df_sb, plan_year=year)
        # Merge
        if merge_func:
            merged = merge_func(df_5500, df_sb, df_sr)
        else:
            merged = df_5500  # fallback
        # Write interim output
        out_path = os.path.join(output_dir, f"merged_{year}.parquet")
        if merged is not None:
            merged.to_parquet(out_path, index=False)
            logging.info(f"Wrote {out_path}")
            results.append((year, merged))
        else:
            logging.warning(f"No merged data for year {year}")
    return results
>>>>>>> 97a434b (upd. coding after agent updates)


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

def combine_sb_years(years, data_dir, file_prefix, file_suffix, output_file, chunk_size=500_000):
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

    first_chunk = True
    for chunk in year_chunks():
        chunk.to_csv(output_file, mode='w' if first_chunk else 'a', index=False, header=first_chunk)
        first_chunk = False
    logging.info(f"Combined file written to {output_file}")
    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(f"No CSV files found matching pattern: {pattern}")

    dfs = []

    for f in files:
        if f.lower().endswith(".txt"):
            continue  # ignore layout files

        year = extract_year_from_filename(f)
        df = load_5500_csv(f)
        df["year"] = year
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)
