"""
File: data_ingestion/combine_years.py
Purpose:
    Load and combine multiple Form 5500 CSV datasets across different years.
"""

import pandas as pd
import glob
import re
from data_ingestion.load_csv import load_5500_csv

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

def combine_years(pattern="data_raw/*.csv"):
    """
    Load all CSV files matching pattern, ignore layout TXT files,
    extract year, and return a combined DataFrame.
    """
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
