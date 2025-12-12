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

    # Standardize column names for rollup
    col_map = {
        'EIN': 'EIN',
        'PLAN_NUMBER': 'PLAN_NUMBER',
        'PLAN_YEAR': 'PLAN_YEAR',
        'SPONSOR_DFE_NAME': 'SPONSOR_DFE_NAME',
        'PLAN_NAME': 'PLAN_NAME',
        'ACTIVE_COUNT': 'ACTIVE_COUNT',
        'RETIREE_COUNT': 'RETIREE_COUNT',
        'SEPARATED_COUNT': 'SEPARATED_COUNT',
        'TOTAL_PARTICIPANTS': 'TOTAL_PARTICIPANTS',
        'ACT_LIABILITY': 'ACT_LIABILITY',
        'RET_LIABILITY': 'RET_LIABILITY',
        'TERM_LIABILITY': 'TERM_LIABILITY',
        'TOTAL_LIABILITY': 'TOTAL_LIABILITY',
        'ASSET_EQUITY_PCT': 'ASSET_EQUITY_PCT',
        'ASSET_FIXED_INCOME_PCT': 'ASSET_FIXED_INCOME_PCT',
        'ASSET_REAL_ESTATE_PCT': 'ASSET_REAL_ESTATE_PCT',
        'ASSET_ALTERNATIVES_PCT': 'ASSET_ALTERNATIVES_PCT',
        'ASSET_CASH_PCT': 'ASSET_CASH_PCT',
        'ANNUITY_PURCHASES': 'ANNUITY_PURCHASES',
        'TRANSFERRED_TO_INSURERS': 'TRANSFERRED_TO_INSURERS',
        'BENEFITS_PAID': 'BENEFITS_PAID',
        'CONTRIBUTIONS': 'CONTRIBUTIONS',
    }
    df = master_df.rename(columns=col_map).copy()
    # Add TRACKING_ID for multi-year tracking (cleaned EIN and PLAN_NUMBER)
    df['EIN'] = df['EIN'].astype(str).str.strip()
    df['PLAN_NUMBER'] = df['PLAN_NUMBER'].astype(str).str.strip()
    df['TRACKING_ID'] = df['EIN'] + '-' + df['PLAN_NUMBER']

    # Group by TRACKING_ID, PLAN_YEAR for year-by-year sponsor rollup
    sponsor_year = df.groupby(['TRACKING_ID', 'PLAN_YEAR']).agg({
        'PLAN_NUMBER': unique_join,
        'SPONSOR_DFE_NAME': first_non_null,
        'PLAN_NAME': unique_join,
        'ACTIVE_COUNT': 'sum',
        'RETIREE_COUNT': 'sum',
        'SEPARATED_COUNT': 'sum',
        'TOTAL_PARTICIPANTS': 'sum',
        'ACT_LIABILITY': 'sum',
        'RET_LIABILITY': 'sum',
        'TERM_LIABILITY': 'sum',
        'TOTAL_LIABILITY': 'sum',
        'ASSET_EQUITY_PCT': 'mean',
        'ASSET_FIXED_INCOME_PCT': 'mean',
        'ASSET_REAL_ESTATE_PCT': 'mean',
        'ASSET_ALTERNATIVES_PCT': 'mean',
        'ASSET_CASH_PCT': 'mean',
        'ANNUITY_PURCHASES': 'sum',
        'TRANSFERRED_TO_INSURERS': 'sum',
        'BENEFITS_PAID': 'sum',
        'CONTRIBUTIONS': 'sum',
    }).reset_index()

    # Derived fields
    sponsor_year['TOTAL_ANNUITANTS'] = sponsor_year['RETIREE_COUNT'] + sponsor_year['SEPARATED_COUNT']
    sponsor_year['ANNUITANT_RATIO'] = sponsor_year['TOTAL_ANNUITANTS'] / sponsor_year['TOTAL_PARTICIPANTS'].replace({0: np.nan})

    # Compute YOY and multi-year trends for each TRACKING_ID
    sponsor_year = sponsor_year.sort_values(['TRACKING_ID', 'PLAN_YEAR']).reset_index(drop=True)
    group = sponsor_year.groupby('TRACKING_ID')
    sponsor_year['ACTIVE_YOY_CHANGE'] = group['ACTIVE_COUNT'].diff()
    sponsor_year['RETIREE_YOY_CHANGE'] = group['RETIREE_COUNT'].diff()
    sponsor_year['ANNUTIANT_RATIO_YOY_CHANGE'] = group['ANNUITANT_RATIO'].diff()
    sponsor_year['LIABILITY_YOY_CHANGE'] = group['TOTAL_LIABILITY'].diff()
    sponsor_year['BENEFITS_VS_CONTRIB_YOY'] = group['BENEFITS_PAID'].diff() - group['CONTRIBUTIONS'].diff()
    sponsor_year['EQUITY_PCT_YOY_CHANGE'] = group['ASSET_EQUITY_PCT'].diff()
    sponsor_year['FIXED_INCOME_PCT_YOY_CHANGE'] = group['ASSET_FIXED_INCOME_PCT'].diff()

    # Multi-year trend metrics (5-year, or as many as available)
    def five_year_rate(subdf, col):
        if len(subdf) < 2:
            return np.nan
        first = subdf.iloc[0][col]
        last = subdf.iloc[-1][col]
        if pd.isna(first) or pd.isna(last) or first == 0:
            return np.nan
        return (last - first) / abs(first)

    trend_df = group.apply(lambda subdf: pd.Series({
        'active_decline_rate_5yr': five_year_rate(subdf, 'ACTIVE_COUNT'),
        'retiree_growth_rate_5yr': five_year_rate(subdf, 'RETIREE_COUNT'),
        'liability_drift_5yr': five_year_rate(subdf, 'TOTAL_LIABILITY'),
        'asset_shift_fi_5yr': five_year_rate(subdf, 'ASSET_FIXED_INCOME_PCT'),
        'benefits_vs_contrib_5yr': five_year_rate(subdf, 'BENEFITS_PAID') - five_year_rate(subdf, 'CONTRIBUTIONS'),
        'annuitant_ratio_traj': five_year_rate(subdf, 'ANNUITANT_RATIO'),
    })).reset_index()

    sponsor_year = sponsor_year.merge(trend_df, on='TRACKING_ID', how='left')

    # Sponsor-level behavioral flags
    def sponsor_is_freezing(subdf):
        return (subdf['ACTIVE_YOY_CHANGE'] < 0).sum() >= 2
    def sponsor_is_derisking(subdf):
        return (subdf['FIXED_INCOME_PCT_YOY_CHANGE'] > 0).sum() >= 2
    def sponsor_asset_shift_fi(subdf):
        return subdf['asset_shift_fi_5yr'] > 0.05
    def sponsor_is_annuity_purchasing(subdf):
        return (subdf['ANNUITY_PURCHASES'] > 0).any()

    # Compute peer industry median annuitant ratio for longevity risk
    peer_median_ann_ratio = sponsor_year.groupby('PLAN_YEAR')['ANNUITANT_RATIO'].median()
    sponsor_year = sponsor_year.merge(peer_median_ann_ratio.rename('PEER_MEDIAN_ANN_RATIO'), on='PLAN_YEAR', how='left')
    def sponsor_high_longevity_risk(row):
        return row['ANNUITANT_RATIO'] > row['PEER_MEDIAN_ANN_RATIO']

    # Apply flags per EIN
    flag_df = group.apply(lambda subdf: pd.Series({
        'sponsor_is_freezing': sponsor_is_freezing(subdf),
        'sponsor_is_derisking': sponsor_is_derisking(subdf),
        'sponsor_asset_shift_fi': sponsor_asset_shift_fi(subdf),
        'sponsor_is_annuity_purchasing': sponsor_is_annuity_purchasing(subdf),
    })).reset_index()
    sponsor_year = sponsor_year.merge(flag_df, on='EIN', how='left')
    sponsor_year['sponsor_high_longevity_risk'] = sponsor_year.apply(sponsor_high_longevity_risk, axis=1)

    # Write outputs
    output_dir = 'data_output'
    os.makedirs(output_dir, exist_ok=True)
    all_years_path = os.path.join(output_dir, 'sponsor_rollup_all_years.parquet')
    sponsor_year.to_parquet(all_years_path, index=False)
    print(f"[✔] Saved multi-year sponsor rollup → {all_years_path}")

    # Latest year per sponsor
    idx = sponsor_year.groupby('EIN')['PLAN_YEAR'].idxmax()
    sponsor_latest = sponsor_year.loc[idx].reset_index(drop=True)
    latest_path = os.path.join(output_dir, 'sponsor_rollup_latest.parquet')
    sponsor_latest.to_parquet(latest_path, index=False)
    print(f"[✔] Saved sponsor latest snapshot → {latest_path}")

    return sponsor_year



def save_sponsor_rollup_parquet(df, filename="sponsor_rollup_latest.parquet"):
    output_path = os.path.join("data_output", filename)
    df.to_parquet(output_path, index=False)
    print(f"[✔] Saved Sponsor Parquet → {output_path}")


def save_sponsor_rollup_csv(df, filename="sponsor_rollup_latest.csv"):
    output_path = os.path.join("data_output", filename)
    df.to_csv(output_path, index=False)
    print(f"[✔] Saved Sponsor CSV → {output_path}")
