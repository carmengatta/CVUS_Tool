"""
File: data_ingestion/load_csv.py
Purpose:
    Load and preprocess raw Form 5500 CSV files obtained from DOL/EFAST2.
    - Normalizes column names
    - Converts common numeric fields
    - Returns a clean pandas DataFrame for downstream processing
"""

import pandas as pd

def load_5500_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, dtype=str)

    # Normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # Convert numerical fields where possible
    num_cols = [
        "participants_total",
        "participants_active",
        "participants_retired",
        "participants_terminated",
        "benefit_liability",
        "assets_boy",
        "assets_eoy"
    ]

    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df
