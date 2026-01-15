"""
Schedule H Data Normalization

Extracts key financial fields from Schedule H for:
- Pension Risk Transfer (PRT) analysis
- Asset allocation and size analysis
- Plan financial health indicators
"""

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def parse_float(val):
    """Parse a value to float, handling common edge cases."""
    try:
        if pd.isna(val) or str(val).strip().upper() in {'', 'NA', 'N/A', 'NONE'}:
            return pd.NA
        return float(str(val).replace(',', '').strip())
    except Exception:
        return pd.NA


def normalize_sch_h_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Schedule H fields to a canonical schema.
    
    Key fields extracted:
    - PRT/Insurance carrier benefits
    - Total assets (BOY and EOY)
    - Net assets
    - Contributions and distributions
    - Investment allocations
    """
    
    out = pd.DataFrame(index=df.index)
    
    # Identifiers
    out['ACK_ID'] = df['ACK_ID'].astype(str).str.strip() if 'ACK_ID' in df.columns else pd.NA
    out['EIN'] = df['SCH_H_EIN'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True) if 'SCH_H_EIN' in df.columns else pd.NA
    out['PLAN_NUMBER'] = df['SCH_H_PN'].astype(str).str.strip().str.zfill(3) if 'SCH_H_PN' in df.columns else pd.NA
    
    # Plan Year
    if 'SCH_H_TAX_PRD' in df.columns:
        out['PLAN_YEAR'] = pd.to_datetime(df['SCH_H_TAX_PRD'], errors='coerce').dt.year
    else:
        out['PLAN_YEAR'] = pd.NA
    
    # === PRT / Insurance Carrier Benefits ===
    out['PRT_AMOUNT'] = df['INS_CARRIER_BNFTS_AMT'].apply(parse_float) if 'INS_CARRIER_BNFTS_AMT' in df.columns else 0
    
    # === Asset Values ===
    out['TOTAL_ASSETS_BOY'] = df['TOT_ASSETS_BOY_AMT'].apply(parse_float) if 'TOT_ASSETS_BOY_AMT' in df.columns else pd.NA
    out['TOTAL_ASSETS_EOY'] = df['TOT_ASSETS_EOY_AMT'].apply(parse_float) if 'TOT_ASSETS_EOY_AMT' in df.columns else pd.NA
    out['NET_ASSETS_BOY'] = df['NET_ASSETS_BOY_AMT'].apply(parse_float) if 'NET_ASSETS_BOY_AMT' in df.columns else pd.NA
    out['NET_ASSETS_EOY'] = df['NET_ASSETS_EOY_AMT'].apply(parse_float) if 'NET_ASSETS_EOY_AMT' in df.columns else pd.NA
    
    # === Liabilities ===
    out['TOTAL_LIABILITIES_BOY'] = df['TOT_LIABILITIES_BOY_AMT'].apply(parse_float) if 'TOT_LIABILITIES_BOY_AMT' in df.columns else pd.NA
    out['TOTAL_LIABILITIES_EOY'] = df['TOT_LIABILITIES_EOY_AMT'].apply(parse_float) if 'TOT_LIABILITIES_EOY_AMT' in df.columns else pd.NA
    
    # === Contributions ===
    out['EMPLOYER_CONTRIB'] = df['EMPLR_CONTRIB_INCOME_AMT'].apply(parse_float) if 'EMPLR_CONTRIB_INCOME_AMT' in df.columns else pd.NA
    out['PARTICIPANT_CONTRIB'] = df['PARTICIPANT_CONTRIB_AMT'].apply(parse_float) if 'PARTICIPANT_CONTRIB_AMT' in df.columns else pd.NA
    out['TOTAL_CONTRIBUTIONS'] = df['TOT_CONTRIB_AMT'].apply(parse_float) if 'TOT_CONTRIB_AMT' in df.columns else pd.NA
    
    # === Distributions ===
    out['DIRECT_PARTICIPANT_DISTRIB'] = df['DISTRIB_DRT_PARTCP_AMT'].apply(parse_float) if 'DISTRIB_DRT_PARTCP_AMT' in df.columns else pd.NA
    out['OTHER_BENEFIT_PAYMENTS'] = df['OTH_BNFT_PAYMENT_AMT'].apply(parse_float) if 'OTH_BNFT_PAYMENT_AMT' in df.columns else pd.NA
    out['TOTAL_DISTRIBUTIONS'] = df['TOT_DISTRIB_BNFT_AMT'].apply(parse_float) if 'TOT_DISTRIB_BNFT_AMT' in df.columns else pd.NA
    
    # === Investment Income ===
    out['TOTAL_INTEREST'] = df['TOTAL_INTEREST_AMT'].apply(parse_float) if 'TOTAL_INTEREST_AMT' in df.columns else pd.NA
    out['TOTAL_DIVIDENDS'] = df['TOTAL_DIVIDENDS_AMT'].apply(parse_float) if 'TOTAL_DIVIDENDS_AMT' in df.columns else pd.NA
    out['GAIN_LOSS_SALE'] = df['TOT_GAIN_LOSS_SALE_AST_AMT'].apply(parse_float) if 'TOT_GAIN_LOSS_SALE_AST_AMT' in df.columns else pd.NA
    out['UNREALIZED_APPRECIATION'] = df['TOT_UNREALZD_APPRCTN_AMT'].apply(parse_float) if 'TOT_UNREALZD_APPRCTN_AMT' in df.columns else pd.NA
    out['TOTAL_INCOME'] = df['TOT_INCOME_AMT'].apply(parse_float) if 'TOT_INCOME_AMT' in df.columns else pd.NA
    out['NET_INCOME'] = df['NET_INCOME_AMT'].apply(parse_float) if 'NET_INCOME_AMT' in df.columns else pd.NA
    
    # === Expenses ===
    out['TOTAL_EXPENSES'] = df['TOT_EXPENSES_AMT'].apply(parse_float) if 'TOT_EXPENSES_AMT' in df.columns else pd.NA
    out['ADMIN_EXPENSES'] = df['TOT_ADMIN_EXPENSES_AMT'].apply(parse_float) if 'TOT_ADMIN_EXPENSES_AMT' in df.columns else pd.NA
    out['INVESTMENT_MGMT_FEES'] = df['INVST_MGMT_FEES_AMT'].apply(parse_float) if 'INVST_MGMT_FEES_AMT' in df.columns else pd.NA
    out['ACTUARIAL_FEES'] = df['ACTUARIAL_FEES_AMT'].apply(parse_float) if 'ACTUARIAL_FEES_AMT' in df.columns else pd.NA
    
    # === Investment Allocations (EOY) ===
    out['CASH_EOY'] = df['INT_BEAR_CASH_EOY_AMT'].apply(parse_float) if 'INT_BEAR_CASH_EOY_AMT' in df.columns else pd.NA
    out['GOVT_SECURITIES_EOY'] = df['GOVT_SEC_EOY_AMT'].apply(parse_float) if 'GOVT_SEC_EOY_AMT' in df.columns else pd.NA
    out['CORP_DEBT_EOY'] = (
        df['CORP_DEBT_PREFERRED_EOY_AMT'].apply(parse_float).fillna(0) + 
        df['CORP_DEBT_OTHER_EOY_AMT'].apply(parse_float).fillna(0)
    ) if 'CORP_DEBT_PREFERRED_EOY_AMT' in df.columns else pd.NA
    out['COMMON_STOCK_EOY'] = df['COMMON_STOCK_EOY_AMT'].apply(parse_float) if 'COMMON_STOCK_EOY_AMT' in df.columns else pd.NA
    out['PREF_STOCK_EOY'] = df['PREF_STOCK_EOY_AMT'].apply(parse_float) if 'PREF_STOCK_EOY_AMT' in df.columns else pd.NA
    out['REAL_ESTATE_EOY'] = df['REAL_ESTATE_EOY_AMT'].apply(parse_float) if 'REAL_ESTATE_EOY_AMT' in df.columns else pd.NA
    out['INS_CO_GEN_ACCT_EOY'] = df['INS_CO_GEN_ACCT_EOY_AMT'].apply(parse_float) if 'INS_CO_GEN_ACCT_EOY_AMT' in df.columns else pd.NA
    
    # === Transfers ===
    out['TRANSFERS_TO'] = df['TOT_TRANSFERS_TO_AMT'].apply(parse_float) if 'TOT_TRANSFERS_TO_AMT' in df.columns else pd.NA
    out['TRANSFERS_FROM'] = df['TOT_TRANSFERS_FROM_AMT'].apply(parse_float) if 'TOT_TRANSFERS_FROM_AMT' in df.columns else pd.NA
    
    # === PBGC Coverage ===
    out['PBGC_COVERED'] = df['COVERED_PBGC_INSURANCE_IND'].astype(str).str.upper().isin(['Y', 'YES', '1', 'TRUE']) if 'COVERED_PBGC_INSURANCE_IND' in df.columns else pd.NA
    
    # === Derived Fields ===
    # Asset change YoY
    out['ASSET_CHANGE'] = pd.to_numeric(out['TOTAL_ASSETS_EOY'], errors='coerce') - pd.to_numeric(out['TOTAL_ASSETS_BOY'], errors='coerce')
    
    # Safe division for percentage calculations (avoid division by zero)
    boy_assets = pd.to_numeric(out['TOTAL_ASSETS_BOY'], errors='coerce')
    boy_assets_safe = boy_assets.replace(0, float('nan'))  # Replace 0 with NaN for division
    out['ASSET_CHANGE_PCT'] = (out['ASSET_CHANGE'] / boy_assets_safe * 100).round(2)
    
    # PRT as percentage of BOY assets
    prt_amount = pd.to_numeric(out['PRT_AMOUNT'], errors='coerce').fillna(0)
    out['PRT_PCT_OF_ASSETS'] = (prt_amount / boy_assets_safe * 100).round(2)
    
    # Has done PRT flag
    out['HAS_PRT'] = prt_amount > 0
    
    logging.info(f"Normalized {len(out)} Schedule H records")
    logging.info(f"Plans with PRT activity: {out['HAS_PRT'].sum()}")
    
    return out


def load_and_normalize_sch_h(filepath: str, year: int = None) -> pd.DataFrame:
    """
    Load a Schedule H CSV file and normalize it.
    
    Args:
        filepath: Path to the CSV file
        year: Optional year to add to the dataframe
        
    Returns:
        Normalized DataFrame
    """
    import os
    
    if not os.path.exists(filepath):
        logging.warning(f"Schedule H file not found: {filepath}")
        return pd.DataFrame()
    
    df = pd.read_csv(filepath, low_memory=False)
    normalized = normalize_sch_h_fields(df)
    
    if year:
        normalized['YEAR'] = year
    
    return normalized


if __name__ == "__main__":
    # Test with 2024 data
    import os
    
    filepath = os.path.join(os.path.dirname(__file__), "..", "data_raw", "F_SCH_H_2024_latest.csv")
    if os.path.exists(filepath):
        df = load_and_normalize_sch_h(filepath, year=2024)
        print(f"\nLoaded {len(df)} records")
        print(f"PRT transactions: {df['HAS_PRT'].sum()}")
        print(f"Total PRT amount: ${df['PRT_AMOUNT'].sum():,.0f}")
        print(f"\nTop 10 PRT transactions:")
        print(df[df['HAS_PRT']].nlargest(10, 'PRT_AMOUNT')[['EIN', 'PRT_AMOUNT', 'TOTAL_ASSETS_EOY', 'PRT_PCT_OF_ASSETS']].to_string())
    else:
        print(f"File not found: {filepath}")
