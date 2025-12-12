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
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


def load_5500_excel(filepath: str, required_columns=None, dtype_overrides=None, encodings=("utf-8", "latin1", "cp1252")) -> pd.DataFrame:
        # PLAN_NUMBER normalization mapping
        PLAN_NUMBER_MAP = [
            # Form 5500
            'SPONS_DFE_PN', 'PLAN_NUM', 'PLAN_NUMBER', 'PNUM', 'PN',
            # Schedule SB
            'SB_PLAN_NUM', 'SB_PN', 'PLAN_NO',
            # Schedule R
            'SCH_R_PN',
        ]
    """
    Load an Excel file and preprocess it into a standardized DataFrame with robust error handling.
    """
    if required_columns is None:
        required_columns = ["EIN", "PLAN_NUMBER", "ACK_ID"]
    if dtype_overrides is None:
        dtype_overrides = {"EIN": str, "PLAN_NUMBER": str, "ACK_ID": str}

    # Try different encodings (for CSV fallback), but Excel usually handles encoding internally
    try:
        df = pd.read_excel(filepath, dtype=str)
        logging.info(f"Loaded {filepath} as Excel")
    except Exception as e:
        logging.error(f"Failed to load Excel file {filepath}: {e}")
        return pd.DataFrame()  # Fail gracefully

    # Normalize column names to uppercase, strip whitespace
    df.columns = df.columns.str.strip().str.upper().str.replace(" ", "_").str.replace("-", "_")

    # PLAN_NUMBER normalization (first match, zero-pad, string, drop if missing)
    plan_number_col = None
    for col in PLAN_NUMBER_MAP:
        if col in df.columns:
            plan_number_col = col
            break
    if plan_number_col:
        df['PLAN_NUMBER'] = df[plan_number_col].astype(str).str.strip().str.zfill(3)
    else:
        logging.warning(f"No recognizable plan number field found in {filepath}; dropping rows.")
        return pd.DataFrame()  # Drop all rows if PLAN_NUMBER missing

    # Required column validation
    missing = [col for col in required_columns if col.upper() not in df.columns]
    if missing:
        logging.error(f"Missing required columns in {filepath}: {missing}")

    # Dtype inference with overrides
    for col, typ in dtype_overrides.items():
        if col.upper() in df.columns:
            try:
                df[col.upper()] = df[col.upper()].astype(typ)
            except Exception as e:
                logging.warning(f"Failed dtype conversion for {col} in {filepath}: {e}")

    # Whitespace trim for all string fields
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()

    # Numeric fields (SB/5500 known fields)
    num_cols = [
        "PARTICIPANTS_TOTAL",
        "PARTICIPANTS_ACTIVE",
        "PARTICIPANTS_RETIRED",
        "PARTICIPANTS_TERMINATED",
        "BENEFIT_LIABILITY",
        "ASSETS_BOY",
        "ASSETS_EOY"
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def load_5500_excel(filepath: str, required_columns=None, dtype_overrides=None, schedule_r=False) -> pd.DataFrame:
    """
    Load an Excel file and preprocess it into a standardized DataFrame with robust error handling.
    """
    import re
    if required_columns is None:
        required_columns = ["EIN", "PLAN_NUMBER", "ACK_ID"]
    if dtype_overrides is None:
        dtype_overrides = {"EIN": str, "PLAN_NUMBER": str, "ACK_ID": str}

    try:
        df = pd.read_excel(filepath, dtype=str)
        logging.info(f"Loaded {filepath} as Excel")
    except Exception as e:
        logging.error(f"Failed to load Excel file {filepath}: {e}")
        return pd.DataFrame()  # Fail gracefully

    # Normalize column names: uppercase, strip whitespace, remove BOM/control chars
    df.columns = df.columns.str.encode('utf-8').str.decode('utf-8-sig').str.replace(r'[\x00-\x1F\x7F]', '', regex=True)
    df.columns = df.columns.str.strip().str.upper().str.replace(" ", "_").str.replace("-", "_")

    # Year inference from filename
    plan_year = None
    m = re.search(r'(20\d{2})', filepath)
    if m:
        plan_year = int(m.group(1))
        df['PLAN_YEAR'] = plan_year

    # Required column validation
    missing = [col for col in required_columns if col.upper() not in df.columns]
    if missing:
        logging.warning(f"Missing required columns in {filepath}: {missing}")

    # Dtype enforcement
    for col, typ in dtype_overrides.items():
        if col.upper() in df.columns:
            try:
                df[col.upper()] = df[col.upper()].astype(typ)
            except Exception as e:
                logging.warning(f"Failed dtype conversion for {col} in {filepath}: {e}")

    # Whitespace trim for all string fields
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()

    # Multi-year robust dtype enforcement
    int_fields = [c for c in df.columns if 'PARTICIPANT' in c or 'COUNT' in c]
    float_fields = [c for c in df.columns if 'LIABILITY' in c or (schedule_r and 'ASSET_' in c) or c in [
        'ANNUITY_PURCHASES', 'TRANSFERRED_TO_INSURERS', 'BENEFITS_PAID', 'CONTRIBUTIONS']]
    for col in int_fields:
        try:
            df[col] = df[col].str.replace(',', '').astype('Int64')
        except Exception:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    for col in float_fields:
        try:
            df[col] = df[col].str.replace(',', '').astype(float)
        except Exception:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Schedule R: ensure asset composition columns exist and are float
    if schedule_r:
        r_cols = [
            'ASSET_EQUITY_PCT', 'ASSET_FIXED_INCOME_PCT', 'ASSET_REAL_ESTATE_PCT',
            'ASSET_ALTERNATIVES_PCT', 'ASSET_CASH_PCT', 'ANNUITY_PURCHASES',
            'TRANSFERRED_TO_INSURERS', 'BENEFITS_PAID', 'CONTRIBUTIONS'
        ]
        for col in r_cols:
            if col not in df.columns:
                df[col] = None
            if col in df.columns:
                try:
                    df[col] = df[col].astype(float)
                except Exception:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

    # Remove any interest-rate or discount-rate columns
    interest_cols = [c for c in df.columns if 'INTEREST' in c or 'RATE' in c or 'DISCOUNT' in c]
    df = df.drop(columns=interest_cols, errors='ignore')

    return df
