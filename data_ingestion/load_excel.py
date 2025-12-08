"""
File: data_ingestion/load_excel.py
Purpose:
    Load and preprocess Excel files (XLS or XLSX) containing Form 5500 or
    Schedule SB data. This is useful when the data is exported manually,
    or when only Excel versions of filings or summaries are provided.
    
    Functions include:
        - Reading Excel into pandas
        - Normalizing column names
        - Converting numeric fields when possible
        - Returning a clean DataFrame consistent with CSV ingestion format
"""

import pandas as pd

def load_5500_excel(filepath: str) -> pd.DataFrame:
    """
    Load an Excel file and preprocess it into a standardized DataFrame.

    Parameters
    ----------
    filepath : str
        Full path to the Excel file (*.xls or *.xlsx).

    Returns
    -------
    pd.DataFrame
        Cleaned and normalized DataFrame ready for analysis.
    """
    # Read Excel file into DataFrame
    df = pd.read_excel(filepath, dtype=str)

    # Normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # Convert known numeric fields (same list as CSV ingestion)
    numeric_cols = [
        "participants_total",
        "participants_active",
        "participants_retired",
        "participants_terminated",
        "benefit_liability",
        "assets_boy",
        "assets_eoy"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df
