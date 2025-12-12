"""
Longevity Insights Agent

This module provides a LongevityInsightsAgent class for analyzing longevity risk and mortality assumption usage for DB sponsors.
"""

import pandas as pd
import numpy as np

# --- Mortality Assumption Detection ---
def detect_mortality_pattern(row, sb_mortality_col="mortality_code", acct_mortality_col="acct_mortality_code"):
    pattern = {}
    sb_code = row.get(sb_mortality_col, None)
    acct_code = row.get(acct_mortality_col, None)
    pattern['sb_substitute'] = sb_code not in (None, '', 'STD', 'STANDARD')
    pattern['sb_code'] = sb_code
    pattern['acct_code'] = acct_code
    return pattern

# --- Peer Comparison ---
def compare_to_peers(sponsor_row, peer_df):
    ann_ratio = sponsor_row.get('annuitant_ratio', np.nan)
    peer_ann = peer_df['annuitant_ratio'].mean() if 'annuitant_ratio' in peer_df else np.nan
    high_retiree = ann_ratio > (peer_ann + 0.15) if not np.isnan(peer_ann) else False
    exposure = {
        'annuitant_ratio': ann_ratio,
        'peer_annuitant_ratio': peer_ann,
        'high_retiree_share': high_retiree
    }
    # Liability age distribution
    active = sponsor_row.get('active', np.nan)
    retired = sponsor_row.get('retired', np.nan)
    total = sponsor_row.get('total', np.nan)
    if not np.isnan(active) and not np.isnan(retired) and not np.isnan(total) and total > 0:
        exposure['active_pct'] = active / total
        exposure['retired_pct'] = retired / total
    else:
        exposure['active_pct'] = np.nan
        exposure['retired_pct'] = np.nan
    return exposure

# --- Longevity Risk Flags ---
def longevity_risk_flags(sponsor_row, peer_df):
    flags = {}
    ann_ratio = sponsor_row.get('annuitant_ratio', np.nan)
    peer_ann = peer_df['annuitant_ratio'].mean() if 'annuitant_ratio' in peer_df else np.nan
    flags['high_longevity_risk'] = ann_ratio > (peer_ann + 0.15) if not np.isnan(peer_ann) else False
    flags['substitute_mortality'] = detect_mortality_pattern(sponsor_row)['sb_substitute']
    return flags

# --- Main Agent ---
class LongevityInsightsAgent:
    """
    Longevity insights agent supporting multi-year analytics from master_db_all_years.parquet and sponsor_rollup_all_years.parquet.
    - Tracks mortality assumption changes across years.
    - Detects persistent use of substitute mortality.
    - Identifies rising longevity exposure (multi-year annuitant ratio trends).
    - Compares sponsor 5-year longevity metrics to peer industry norms.
    - Adds new output fields: mortality_trend, longevity_risk_trend, multi_year_annuitant_ratio_path.
    - Robustly handles new long-format data and missing data.
    """
    def __init__(self, master_df):
        self.master_df = master_df.copy()
        if 'industry' not in self.master_df.columns:
            self.master_df['industry'] = self.master_df['business_code'].astype(str)
        self.is_multi_year = 'plan_year' in self.master_df.columns or 'PLAN_YEAR' in self.master_df.columns

    def _get_year_col(self, df):
        return 'plan_year' if 'plan_year' in df.columns else 'PLAN_YEAR' if 'PLAN_YEAR' in df.columns else 'year'

    def analyze_sponsor(self, sponsor_ein):
        df = self.master_df.copy()
        ein_col = 'ein' if 'ein' in df.columns else 'EIN'
        year_col = self._get_year_col(df)
        sdf = df[df[ein_col] == str(sponsor_ein)].copy()
        if sdf.empty:
            return {'error': 'Sponsor not found'}
        sdf = sdf.sort_values(year_col)
        # Multi-year annuitant ratio path
        ann_path = list(sdf['annuitant_ratio'].dropna()) if 'annuitant_ratio' in sdf.columns else []
        # Mortality trend: track changes in mortality_code
        mort_trend = list(sdf['mortality_code'].dropna().unique()) if 'mortality_code' in sdf.columns else []
        # Persistent substitute mortality
        persistent_substitute = False
        if 'mortality_code' in sdf.columns:
            persistent_substitute = all(x not in ('STD', 'STANDARD', '', None) for x in sdf['mortality_code'].dropna())
        # Longevity risk trend: 5-year slope of annuitant ratio
        def _slope(series, years=5):
            s = pd.Series(series).dropna()
            if len(s) < 2:
                return np.nan
            s = s.tail(years)
            x = np.arange(len(s))
            y = s.values
            if len(x) < 2:
                return np.nan
            return np.polyfit(x, y, 1)[0]
        longevity_risk_trend = _slope(ann_path, 5)
        # Peer comparison (latest year)
        latest_year = sdf[year_col].max() if not sdf.empty else None
        industry = sdf['industry'].iloc[0] if not sdf.empty else None
        peer_df = df[(df['industry'] == industry) & (df[year_col] == latest_year)] if latest_year is not None else pd.DataFrame()
        # Output
        return {
            'mortality_trend': mort_trend,
            'persistent_substitute_mortality': persistent_substitute,
            'longevity_risk_trend': longevity_risk_trend,
            'multi_year_annuitant_ratio_path': ann_path,
            'peer_annuitant_ratio_latest': peer_df['annuitant_ratio'].mean() if not peer_df.empty else np.nan,
            'high_longevity_risk': (ann_path[-1] > peer_df['annuitant_ratio'].mean()) if ann_path and not peer_df.empty else False
        }
