"""
Build Sponsor-Level Consolidated DB Dataset

Aggregates all DB plans by EIN:
- Sums participants and liabilities
- Creates plan count
- Combines plan numbers into one field
- Keeps first non-null sponsor name
"""

import os
import pandas as pd



def build_sponsor_rollup(master_df: pd.DataFrame) -> pd.DataFrame:
    import numpy as np
    import pandas as pd
    from collections import Counter

    # Helper functions
    def first_non_null(series):
        return next((x for x in series if pd.notna(x) and x != ""), pd.NA)

    def most_frequent(series):
        vals = [x for x in series if pd.notna(x) and x != ""]
        if not vals:
            return pd.NA
        return Counter(vals).most_common(1)[0][0]

    def unique_join(series):
        return ",".join(sorted(set(str(x) for x in series if pd.notna(x) and x != "")))

    df = master_df.copy()
    # Add TRACKING_ID for multi-year tracking (cleaned EIN and PLAN_NUMBER)
    df['EIN'] = df['EIN'].astype(str).str.strip()
    df['PLAN_NUMBER'] = df['PLAN_NUMBER'].astype(str).str.strip()
    df['TRACKING_ID'] = df['EIN'] + '-' + df['PLAN_NUMBER']

    # Group by TRACKING_ID, PLAN_YEAR for year-by-year sponsor rollup
    agg_dict = {
        'PLAN_NUMBER': unique_join,
        'ACTIVE_COUNT': 'sum',
        'RETIREE_COUNT': 'sum',
        'SEPARATED_COUNT': 'sum',
        'TOTAL_PARTICIPANTS': 'sum',
        'ACT_LIABILITY': 'sum',
        'RET_LIABILITY': 'sum',
        'TERM_LIABILITY': 'sum',
        'TOTAL_LIABILITY': 'sum',
        'ANNUITY_PURCHASES': 'sum',
        'TRANSFERRED_TO_INSURERS': 'sum',
        'BENEFITS_PAID': 'sum',
        'CONTRIBUTIONS': 'sum',
    }
    # Only include optional columns if they exist
    for col in ['SPONSOR_DFE_NAME', 'PLAN_NAME']:
        if col in df.columns:
            agg_dict[col] = first_non_null if col == 'SPONSOR_DFE_NAME' else unique_join
    for col in ['ASSET_EQUITY_PCT', 'ASSET_FIXED_INCOME_PCT', 'ASSET_REAL_ESTATE_PCT',
                'ASSET_ALTERNATIVES_PCT', 'ASSET_CASH_PCT']:
        if col in df.columns:
            agg_dict[col] = 'mean'

    sponsor_year = df.groupby(['TRACKING_ID', 'PLAN_YEAR']).agg(agg_dict).reset_index()

    # Derive EIN from TRACKING_ID for sponsor-level operations
    sponsor_year['EIN'] = sponsor_year['TRACKING_ID'].str.split('-').str[0]

    # Derived fields
    sponsor_year['TOTAL_ANNUITANTS'] = sponsor_year['RETIREE_COUNT'] + sponsor_year['SEPARATED_COUNT']
    sponsor_year['ANNUITANT_RATIO'] = sponsor_year['TOTAL_ANNUITANTS'] / sponsor_year['TOTAL_PARTICIPANTS'].replace({0: np.nan})

    # Compute YOY and multi-year trends for each TRACKING_ID
    sponsor_year = sponsor_year.sort_values(['TRACKING_ID', 'PLAN_YEAR']).reset_index(drop=True)
    group = sponsor_year.groupby('TRACKING_ID')
    sponsor_year['ACTIVE_YOY_CHANGE'] = group['ACTIVE_COUNT'].diff()
    sponsor_year['RETIREE_YOY_CHANGE'] = group['RETIREE_COUNT'].diff()
    sponsor_year['ANNUITANT_RATIO_YOY_CHANGE'] = group['ANNUITANT_RATIO'].diff()
    sponsor_year['LIABILITY_YOY_CHANGE'] = group['TOTAL_LIABILITY'].diff()
    sponsor_year['BENEFITS_VS_CONTRIB_YOY'] = group['BENEFITS_PAID'].diff() - group['CONTRIBUTIONS'].diff()

    # Asset allocation YOY changes (only if columns exist)
    if 'ASSET_EQUITY_PCT' in sponsor_year.columns:
        sponsor_year['EQUITY_PCT_YOY_CHANGE'] = group['ASSET_EQUITY_PCT'].diff()
    if 'ASSET_FIXED_INCOME_PCT' in sponsor_year.columns:
        sponsor_year['FIXED_INCOME_PCT_YOY_CHANGE'] = group['ASSET_FIXED_INCOME_PCT'].diff()

    # Multi-year trend metrics (5-year, or as many as available)
    def five_year_rate(subdf, col):
        if col not in subdf.columns or len(subdf) < 2:
            return np.nan
        first = subdf.iloc[0][col]
        last = subdf.iloc[-1][col]
        if pd.isna(first) or pd.isna(last) or first == 0:
            return np.nan
        return (last - first) / abs(first)

    def safe_benefits_vs_contrib_5yr(subdf):
        benefits_rate = five_year_rate(subdf, 'BENEFITS_PAID')
        contrib_rate = five_year_rate(subdf, 'CONTRIBUTIONS')
        if pd.isna(benefits_rate) or pd.isna(contrib_rate):
            return np.nan
        return benefits_rate - contrib_rate

    # Recompute group after sorting
    group = sponsor_year.groupby('TRACKING_ID')

    trend_dict = {
        'active_decline_rate_5yr': lambda subdf: five_year_rate(subdf, 'ACTIVE_COUNT'),
        'retiree_growth_rate_5yr': lambda subdf: five_year_rate(subdf, 'RETIREE_COUNT'),
        'liability_drift_5yr': lambda subdf: five_year_rate(subdf, 'TOTAL_LIABILITY'),
        'annuitant_ratio_traj': lambda subdf: five_year_rate(subdf, 'ANNUITANT_RATIO'),
        'benefits_vs_contrib_5yr': safe_benefits_vs_contrib_5yr,
    }
    if 'ASSET_FIXED_INCOME_PCT' in sponsor_year.columns:
        trend_dict['asset_shift_fi_5yr'] = lambda subdf: five_year_rate(subdf, 'ASSET_FIXED_INCOME_PCT')

    trend_df = group.apply(lambda subdf: pd.Series({k: v(subdf) for k, v in trend_dict.items()})).reset_index()
    sponsor_year = sponsor_year.merge(trend_df, on='TRACKING_ID', how='left')

    # Sponsor-level behavioral flags (computed AFTER trend data is merged)
    def sponsor_is_freezing(subdf):
        if 'ACTIVE_YOY_CHANGE' not in subdf.columns:
            return False
        return (subdf['ACTIVE_YOY_CHANGE'] < 0).sum() >= 2

    def sponsor_is_derisking(subdf):
        if 'FIXED_INCOME_PCT_YOY_CHANGE' not in subdf.columns:
            return False
        return (subdf['FIXED_INCOME_PCT_YOY_CHANGE'] > 0).sum() >= 2

    def sponsor_asset_shift_fi(subdf):
        if 'asset_shift_fi_5yr' not in subdf.columns:
            return False
        val = subdf['asset_shift_fi_5yr'].iloc[0] if len(subdf) > 0 else np.nan
        return val > 0.05 if pd.notna(val) else False

    def sponsor_is_annuity_purchasing(subdf):
        if 'ANNUITY_PURCHASES' not in subdf.columns:
            return False
        return (subdf['ANNUITY_PURCHASES'] > 0).any()

    # Compute peer industry median annuitant ratio for longevity risk
    peer_median_ann_ratio = sponsor_year.groupby('PLAN_YEAR')['ANNUITANT_RATIO'].median()
    sponsor_year = sponsor_year.merge(peer_median_ann_ratio.rename('PEER_MEDIAN_ANN_RATIO'), on='PLAN_YEAR', how='left')

    def sponsor_high_longevity_risk(row):
        if pd.isna(row['ANNUITANT_RATIO']) or pd.isna(row['PEER_MEDIAN_ANN_RATIO']):
            return False
        return row['ANNUITANT_RATIO'] > row['PEER_MEDIAN_ANN_RATIO']

    # Recompute group for flag computation
    group = sponsor_year.groupby('TRACKING_ID')

    # Apply flags per TRACKING_ID (not EIN — merge on TRACKING_ID to match groupby)
    flag_df = group.apply(lambda subdf: pd.Series({
        'sponsor_is_freezing': sponsor_is_freezing(subdf),
        'sponsor_is_derisking': sponsor_is_derisking(subdf),
        'sponsor_asset_shift_fi': sponsor_asset_shift_fi(subdf),
        'sponsor_is_annuity_purchasing': sponsor_is_annuity_purchasing(subdf),
    })).reset_index()
    sponsor_year = sponsor_year.merge(flag_df, on='TRACKING_ID', how='left')
    sponsor_year['sponsor_high_longevity_risk'] = sponsor_year.apply(sponsor_high_longevity_risk, axis=1)

    # Write outputs
    output_dir = 'data_output'
    os.makedirs(output_dir, exist_ok=True)
    all_years_path = os.path.join(output_dir, 'sponsor_rollup_all_years.parquet')
    sponsor_year.to_parquet(all_years_path, index=False)
    print(f"[+] Saved multi-year sponsor rollup -> {all_years_path}")

    # Latest year per sponsor
    idx = sponsor_year.groupby('EIN')['PLAN_YEAR'].idxmax()
    sponsor_latest = sponsor_year.loc[idx].reset_index(drop=True)
    latest_path = os.path.join(output_dir, 'sponsor_rollup_latest.parquet')
    sponsor_latest.to_parquet(latest_path, index=False)
    print(f"[+] Saved sponsor latest snapshot -> {latest_path}")

    return sponsor_year



def save_sponsor_rollup_parquet(df, filename="sponsor_rollup_latest.parquet"):
    output_path = os.path.join("data_output", filename)
    df.to_parquet(output_path, index=False)
    print(f"[+] Saved Sponsor Parquet -> {output_path}")


def save_sponsor_rollup_csv(df, filename="sponsor_rollup_latest.csv"):
    output_path = os.path.join("data_output", filename)
    df.to_csv(output_path, index=False)
    print(f"[+] Saved Sponsor CSV -> {output_path}")
