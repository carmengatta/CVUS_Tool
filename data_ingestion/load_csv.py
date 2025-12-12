def load_sr_csv(filepath: str, encodings=("utf-8", "latin1", "cp1252")) -> pd.DataFrame:
    """
    Load a Schedule R CSV file and return a DataFrame with all columns preserved. No normalization is performed.
    """
    import pandas as pd
    """
    last_err = None
    for enc in encodings:
        try:
            last_err = e
    logging.error(f"All encoding attempts failed for {filepath}")
    return pd.DataFrame()  # Fail gracefully

"""
File: data_ingestion/load_csv.py
Purpose:
    Load and preprocess raw Form 5500 CSV files obtained from DOL/EFAST2.
    - Normalizes column names
    - Converts common numeric fields
    - Returns a clean pandas DataFrame for downstream processing
"""


import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def load_5500_csv(filepath: str, required_columns=None, dtype_overrides=None, encodings=("utf-8", "latin1", "cp1252"), schedule_r=False) -> pd.DataFrame:
    """
    Load a Form 5500 or Schedule R CSV file with robust error handling and normalization.
    """
    # PLAN_NUMBER normalization mapping
    PLAN_NUMBER_MAP = [
        # Form 5500
        'SPONS_DFE_PN', 'PLAN_NUM', 'PLAN_NUMBER', 'PNUM', 'PN',
        # Schedule SB
        'SB_PLAN_NUM', 'SB_PN', 'PLAN_NO',
        # Schedule R
        'SCH_R_PN',
    ]
    # Canonical header normalization for Form 5500
    HEADER_MAP_5500 = {
        'SPONS_DFE_EIN': 'EIN', 'SB_EIN': 'EIN', 'SCH_R_EIN': 'EIN', 'EIN': 'EIN',
        'PLAN_NUM': 'PLAN_NUMBER', 'PLAN_NUMBER': 'PLAN_NUMBER', 'SB_PN': 'PLAN_NUMBER', 'SCH_R_PLAN_NUM': 'PLAN_NUMBER',
        'PLAN_YR': 'PLAN_YEAR', 'PLAN_YEAR': 'PLAN_YEAR', 'SB_PLAN_YR': 'PLAN_YEAR', 'SCH_R_PLAN_YR': 'PLAN_YEAR',
        'ACK_ID': 'ACK_ID', 'SCH_R_ACK_ID': 'ACK_ID',
        'ACT_PARTCP_CNT': 'ACTIVE_COUNT', 'ACTIVE_COUNT': 'ACTIVE_COUNT',
        'RTD_PARTCP_CNT': 'RETIREE_COUNT', 'RETIREE_COUNT': 'RETIREE_COUNT',
        'TERM_PARTCP_CNT': 'TERMINATED_COUNT', 'TERMINATED_COUNT': 'TERMINATED_COUNT',
        'TOT_PARTCP_CNT': 'TOTAL_PARTICIPANTS', 'TOTAL_PARTICIPANTS': 'TOTAL_PARTICIPANTS',
        'ACT_LIABILITY': 'ACT_LIABILITY', 'RET_LIABILITY': 'RET_LIABILITY', 'TERM_LIABILITY': 'TERM_LIABILITY', 'TOTAL_LIABILITY': 'TOTAL_LIABILITY',
        'MORTALITY_CODE': 'MORTALITY_CODE',
        'ASSET_EQUITY_PCT': 'ASSET_EQUITY_PCT', 'ASSET_FIXED_INCOME_PCT': 'ASSET_FIXED_INCOME_PCT',
        'ASSET_REAL_ESTATE_PCT': 'ASSET_REAL_ESTATE_PCT', 'ASSET_ALTERNATIVES_PCT': 'ASSET_ALTERNATIVES_PCT',
        'ASSET_CASH_EQUIVALENT_PCT': 'ASSET_CASH_EQUIVALENT_PCT',
        'ANNUITY_PURCHASES': 'ANNUITY_PURCHASES', 'TRANSFERRED_TO_INSURERS': 'TRANSFERRED_TO_INSURERS',
        'CONTRIBUTIONS': 'CONTRIBUTIONS', 'BENEFITS_PAID': 'BENEFITS_PAID',
    }

    import re
    if required_columns is None:
        required_columns = ["EIN", "PLAN_NUMBER", "ACK_ID"]
    if dtype_overrides is None:
        dtype_overrides = {"EIN": str, "PLAN_NUMBER": str, "ACK_ID": str}

    last_err = None
    for enc in encodings:
        try:
            df = pd.read_csv(filepath, dtype=str, encoding=enc)
            logging.info(f"Loaded {filepath} with encoding {enc}")
            break
        except Exception as e:
            logging.warning(f"Failed to load {filepath} with encoding {enc}: {e}")
            last_err = e
    else:
        logging.error(f"All encoding attempts failed for {filepath}")
        return pd.DataFrame()  # Fail gracefully

    # Normalize column names: uppercase, strip whitespace, remove BOM/control chars
    df.columns = df.columns.str.encode('utf-8').str.decode('utf-8-sig').str.replace(r'[\x00-\x1F\x7F]', '', regex=True)
    df.columns = df.columns.str.strip().str.upper().str.replace(" ", "_").str.replace("-", "_")
    # Apply canonical header mapping
    df = df.rename(columns={k: v for k, v in HEADER_MAP_5500.items() if k in df.columns})
    # Fallbacks for canonical columns
    FALLBACK_5500 = {
        'EIN': ['SPONS_DFE_EIN', 'SB_EIN', 'SCH_R_EIN', 'EIN'],
        'PLAN_YEAR': ['PLAN_YR', 'PLAN_YEAR', 'SB_PLAN_YR', 'SCH_R_PLAN_YR'],
        'ACK_ID': ['ACK_ID', 'SCH_R_ACK_ID'],
    }
    for canon, fallbacks in FALLBACK_5500.items():
        for alt in fallbacks:
            if canon not in df.columns and alt in df.columns:
                df[canon] = df[alt]

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

    # Synthesize ACK_ID if missing
    if 'ACK_ID' not in df.columns and all(x in df.columns for x in ['EIN', 'PLAN_NUMBER', 'PLAN_YEAR']):
        df['ACK_ID'] = df['EIN'].astype(str) + '-' + df['PLAN_NUMBER'].astype(str) + '-' + df['PLAN_YEAR'].astype(str)

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
