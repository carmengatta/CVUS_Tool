"""
Normalize Schedule R fields to canonical names for multi-year ingestion.
"""

import pandas as pd
import logging

# Canonical header mapping for Schedule R (add more as needed)
SR_HEADER_MAP = {
    'SCH_R_EIN': 'EIN',
# Documentation for the new normalization process
"""
Form 5500 Data Ingestion: Normalize Schedule R Fields

Responsibilities:
- Normalize Schedule R fields only
- Standardize keys (EIN, PLAN_NUMBER, ACK_ID)
- No assumptions about plan existence
- No filtering based on plan type
"""
    'SCH_R_PLAN_NUM': 'PLAN_NUMBER',
    'SCH_R_PLAN_YR': 'PLAN_YEAR',
    'SCH_R_ACK_ID': 'ACK_ID',
    'ASSET_EQUITY': 'ASSET_EQUITY_PCT',
    'ASSET_FIXED_INCOME': 'ASSET_FIXED_INCOME_PCT',
    'ASSET_REAL_ESTATE': 'ASSET_REAL_ESTATE_PCT',
    'ASSET_ALTERNATIVES': 'ASSET_ALTERNATIVES_PCT',
    'ASSET_CASH_EQUIVALENT': 'ASSET_CASH_EQUIVALENT_PCT',
    'ANNUITY_PURCHASES': 'ANNUITY_PURCHASES',
    'TRANSFERRED_TO_INSURERS': 'TRANSFERRED_TO_INSURERS',
    'CONTRIBUTIONS': 'CONTRIBUTIONS',
    'BENEFITS_PAID': 'BENEFITS_PAID',
    # Add more mappings and fallbacks as needed
}

FALLBACK_SR_NAMES = {
    'EIN': ['SCH_R_EIN', 'SPONS_DFE_EIN', 'SB_EIN', 'EIN'],
    'PLAN_NUMBER': ['SCH_R_PLAN_NUM', 'PLAN_NUMBER', 'SB_PLAN_NUM'],
    'PLAN_YEAR': ['SCH_R_PLAN_YR', 'PLAN_YEAR', 'SB_PLAN_YR'],
    'ACK_ID': ['SCH_R_ACK_ID', 'ACK_ID'],
}

def normalize_sr_fields(df: pd.DataFrame) -> pd.DataFrame:
    # PLAN_NUMBER normalization mapping for SR
    PLAN_NUMBER_MAP = ['SCH_R_PN', 'PLAN_NUMBER', 'PN']

    # Rename columns using header map
    df = df.rename(columns={k: v for k, v in SR_HEADER_MAP.items() if k in df.columns})
    # Fallbacks for canonical columns
    for canon, fallbacks in FALLBACK_SR_NAMES.items():
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
        logging.warning("No recognizable plan number field found in SR; dropping rows.")
        return pd.DataFrame(index=df.index)  # Drop all rows if PLAN_NUMBER missing

    # EIN strict normalization
    if 'EIN' in df.columns:
        df['EIN'] = df['EIN'].astype(str).str.strip()

    # PLAN_YEAR strict normalization
    if 'PLAN_YEAR' in df.columns:
        df['PLAN_YEAR'] = pd.to_numeric(df['PLAN_YEAR'], errors='coerce')

    # No filtering or plan existence logic
    # Synthesize ACK_ID if missing
    if 'ACK_ID' not in df.columns and all(x in df.columns for x in ['EIN', 'PLAN_NUMBER', 'PLAN_YEAR']):
        df['ACK_ID'] = df['EIN'].astype(str).str.strip() + '-' + df['PLAN_NUMBER'].astype(str).str.strip().str.zfill(3) + '-' + df['PLAN_YEAR'].astype(str)
    return df
