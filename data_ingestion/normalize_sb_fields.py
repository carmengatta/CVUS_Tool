"""
Normalize Schedule SB actuarial fields.

Responsibilities:
- Extract participant counts
- Extract liabilities
- Extract actuary info
"""

import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
# Helper functions

def parse_int(val):
    try:
        if pd.isna(val) or str(val).strip().upper() in {'', 'NA', 'N/A', 'NONE'}:
            return pd.NA
        return int(str(val).replace(',', '').strip())
    except Exception:
        return pd.NA


def parse_float(val):
    try:
        if pd.isna(val) or str(val).strip().upper() in {'', 'NA', 'N/A', 'NONE'}:
            return pd.NA
        return float(str(val).replace(',', '').strip())
    except Exception:
        return pd.NA

def parse_segment_rate(val, row_idx=None):
    f = parse_float(val)
    if f is pd.NA and row_idx is not None:
        logging.warning(f"Malformed SB segment rate at row {row_idx}: {val}")
    return f

def parse_liability(val, row_idx=None):
    f = parse_float(val)
    if f is pd.NA and row_idx is not None:
        logging.warning(f"Malformed SB liability at row {row_idx}: {val}")
    return f

def parse_participant_count(val, row_idx=None):
    i = parse_int(val)
    if i is pd.NA and row_idx is not None:
        logging.warning(f"Malformed SB participant count at row {row_idx}: {val}")
    return i



def normalize_sb_fields(df: pd.DataFrame, plan_year: int = None) -> pd.DataFrame:
    # Canonical header normalization for Schedule SB (no PLAN_NUMBER)
    FIELD_MAP = {
        'EIN': ['SB_EIN', 'SPONS_DFE_EIN', 'SCH_R_EIN', 'EIN'],
        'PLAN_YEAR': ['SB_PLAN_YR', 'PLAN_YEAR', 'PLAN_YR', 'SCH_R_PLAN_YR'],
        'ACK_ID': ['ACK_ID', 'SCH_R_ACK_ID'],
        'ACTIVE_COUNT': ['SB_ACT_PARTCP_CNT', 'ACTIVE_COUNT', 'ACT_PARTCP_CNT', 'ACTIVES'],
        'RETIREE_COUNT': ['SB_RTD_PARTCP_CNT', 'RETIREE_COUNT', 'RTD_PARTCP_CNT', 'RETIREE'],
        'SEPARATED_COUNT': ['SB_TERM_PARTCP_CNT'],
        'TOTAL_PARTICIPANTS': ['SB_TOT_PARTCP_CNT', 'TOTAL_PARTICIPANTS', 'TOT_PARTCP_CNT', 'TOTAL'],
        'ACT_LIABILITY': ['SB_ACT_VSTD_FNDNG_TGT_AMT', 'ACT_LIABILITY'],
        'RET_LIABILITY': ['SB_RTD_FNDNG_TGT_AMT', 'RET_LIABILITY'],
        'TERM_LIABILITY': ['SB_TERM_FNDNG_TGT_AMT', 'TERM_LIABILITY'],
        'TOTAL_LIABILITY': ['SB_TOT_FNDNG_TGT_AMT', 'TOTAL_LIABILITY'],
        'MORTALITY_CODE': ['SB_MORTALITY_TBL_CD', 'MORTALITY_CODE'],
        # Actuary info fields
        'ACTUARY_FIRM_NAME': ['SB_ACTUARY_FIRM_NAME', 'ACTUARY_FIRM_NAME'],
        'ACTUARY_NAME': ['SB_ACTUARY_NAME_LINE', 'ACTUARY_NAME'],
        'ACTUARY_CITY': ['SB_ACTUARY_US_CITY', 'ACTUARY_CITY'],
        'ACTUARY_STATE': ['SB_ACTUARY_US_STATE', 'ACTUARY_STATE'],
    }

    # Rename columns using canonical mapping (first match)
    for canon, alts in FIELD_MAP.items():
        for alt in alts:
            if canon not in df.columns and alt in df.columns:
                df[canon] = df[alt]

    # Create output DataFrame
    out = pd.DataFrame(index=df.index)

    # Helper to get and clean a column by possible names (PLAN_NUMBER is not handled here)
    def get_col(possible_names, dtype='str'):
        for name in possible_names:
            if name in df.columns:
                col = df[name]
                if dtype == 'str':
                    return col.astype(str).str.strip().replace({'': None, 'NA': None, 'N/A': None})
                elif dtype == 'int':
                    return col.apply(parse_int)
                elif dtype == 'float':
                    return col.apply(parse_float)
        return pd.Series([None]*len(df), index=df.index)

    # EIN: always string, strip whitespace
    out['EIN'] = get_col(FIELD_MAP['EIN'], 'str').str.replace(r'\s+', '', regex=True)
    # PLAN_NUMBER: always string, zero-padded to 3, strict
    if 'PLAN_NUMBER' in df.columns:
        out['PLAN_NUMBER'] = df['PLAN_NUMBER'].astype(str).str.strip().str.zfill(3)
    else:
        out['PLAN_NUMBER'] = pd.NA

    # PLAN_YEAR: use argument if provided, else try to extract
    if plan_year is not None:
        out['PLAN_YEAR'] = plan_year
    else:
        out['PLAN_YEAR'] = get_col(FIELD_MAP['PLAN_YEAR'], 'int')

    # ACK_ID: synthesize if missing
    if 'ACK_ID' not in df.columns and all(x in df.columns for x in ['EIN', 'PLAN_NUMBER', 'PLAN_YEAR']):
        df['ACK_ID'] = df['EIN'].astype(str).str.strip() + '-' + df['PLAN_NUMBER'].astype(str).str.strip().str.zfill(3) + '-' + str(plan_year if plan_year is not None else '')

    # PARTICIPANT COUNTS
    out['ACTIVE_COUNT'] = get_col(FIELD_MAP['ACTIVE_COUNT'], 'int')
    out['RETIREE_COUNT'] = get_col(FIELD_MAP['RETIREE_COUNT'], 'int')
    # SEPARATED_COUNT — terminated / vested separated participants from SB only
    sep_col = get_col(FIELD_MAP['SEPARATED_COUNT'], 'int')
    out['SEPARATED_COUNT'] = sep_col.fillna(0)

    # Guarantee column exists
    if 'SEPARATED_COUNT' not in out.columns:
        out['SEPARATED_COUNT'] = 0
    # TOTAL_PARTICIPANTS: fallback logic
    total_part = get_col(FIELD_MAP['TOTAL_PARTICIPANTS'], 'int')
    if total_part.isnull().all():
        # Try to sum if possible
        total_part = out[['ACTIVE_COUNT', 'RETIREE_COUNT', 'SEPARATED_COUNT']].sum(axis=1, min_count=1)
    out['TOTAL_PARTICIPANTS'] = total_part

    # LIABILITIES
    out['ACT_LIABILITY'] = get_col(FIELD_MAP['ACT_LIABILITY'], 'float')
    out['RET_LIABILITY'] = get_col(FIELD_MAP['RET_LIABILITY'], 'float')
    out['TERM_LIABILITY'] = get_col(FIELD_MAP['TERM_LIABILITY'], 'float')
    out['TOTAL_LIABILITY'] = get_col(FIELD_MAP['TOTAL_LIABILITY'], 'float')

    # MORTALITY_CODE
    out['MORTALITY_CODE'] = get_col(FIELD_MAP['MORTALITY_CODE'], 'str')

    # ACTUARY INFO
    out['ACTUARY_FIRM_NAME'] = get_col(FIELD_MAP['ACTUARY_FIRM_NAME'], 'str')
    out['ACTUARY_NAME'] = get_col(FIELD_MAP['ACTUARY_NAME'], 'str')
    out['ACTUARY_CITY'] = get_col(FIELD_MAP['ACTUARY_CITY'], 'str')
    out['ACTUARY_STATE'] = get_col(FIELD_MAP['ACTUARY_STATE'], 'str')

    # Ensure all required columns exist (fallback to None)
    for col in ['EIN', 'PLAN_NUMBER', 'PLAN_YEAR', 'ACTIVE_COUNT', 'RETIREE_COUNT', 'SEPARATED_COUNT', 'TOTAL_PARTICIPANTS',
                'ACT_LIABILITY', 'RET_LIABILITY', 'TERM_LIABILITY', 'TOTAL_LIABILITY', 'MORTALITY_CODE',
                'ACTUARY_FIRM_NAME', 'ACTUARY_NAME', 'ACTUARY_CITY', 'ACTUARY_STATE']:
        if col not in out.columns:
            out[col] = None

    # Output only the normalized schema columns, in order
    schema = ['EIN', 'PLAN_NUMBER', 'PLAN_YEAR', 'ACTIVE_COUNT', 'RETIREE_COUNT', 'SEPARATED_COUNT',
              'TOTAL_PARTICIPANTS', 'ACT_LIABILITY', 'RET_LIABILITY', 'TERM_LIABILITY', 'TOTAL_LIABILITY', 'MORTALITY_CODE',
              'ACTUARY_FIRM_NAME', 'ACTUARY_NAME', 'ACTUARY_CITY', 'ACTUARY_STATE']
    return out[schema]
