"""
De-risking Pattern Detection Agent

This module provides a DeRiskingAgent class for detecting de-risking and plan freezing patterns across multiple years for each sponsor.
"""

import pandas as pd
import numpy as np

# --- Helper Functions ---
def year_over_year_change(series):
    """Returns year-over-year percent change, sorted by year."""
    s = series.dropna().sort_index()
    return s.pct_change().fillna(0)

def detect_freezing(sponsor_df):
    # Active count decline
    active = sponsor_df.set_index('year')['active'].sort_index()
    active_chg = year_over_year_change(active)
    sharp_decline = (active_chg < -0.15).any()  # >15% drop any year
    # Retiree ratio
    total = sponsor_df.set_index('year')['total'].sort_index()
    retired = sponsor_df.set_index('year')['retired'].sort_index()
    retiree_ratio = (retired / total).fillna(0)
    retiree_ratio_increasing = retiree_ratio.diff().fillna(0).gt(0.05).any()
    # New entrants (if available)
    if 'new_entrants' in sponsor_df.columns:
        new_entrants = sponsor_df.set_index('year')['new_entrants'].sort_index()
        entrants_low = (new_entrants < 5).all()
    else:
        entrants_low = False
    return sharp_decline, retiree_ratio_increasing, entrants_low

def detect_asset_shift(sponsor_df):
    # Look for increase in fixed income/LDI allocation
    if 'fixed_income_pct' in sponsor_df.columns:
        fi = sponsor_df.set_index('year')['fixed_income_pct'].sort_index()
        fi_chg = year_over_year_change(fi)
        return (fi_chg > 0.10).any()  # >10% increase any year
    return False

def detect_liability_transfer(sponsor_df):
    # Retiree count and liability both drop
    retired = sponsor_df.set_index('year')['retired'].sort_index()
    liability_retired = sponsor_df.set_index('year')['liability_retired'].sort_index() if 'liability_retired' in sponsor_df.columns else pd.Series(dtype=float)
    retired_chg = year_over_year_change(retired)
    liability_chg = year_over_year_change(liability_retired) if not liability_retired.empty else pd.Series(dtype=float)
    if not retired_chg.empty and not liability_chg.empty:
        for y in retired_chg.index:
            if retired_chg[y] < -0.10 and liability_chg.get(y, 0) < -0.10:
                return True
    return False

def compute_prt_readiness(sponsor_df):
    # High annuitant share, stable assets, liability decline, high funding
    total = sponsor_df.set_index('year')['total'].sort_index()
    retired = sponsor_df.set_index('year')['retired'].sort_index()
    ann_share = (retired / total).mean() if not total.empty else 0
    assets = sponsor_df.set_index('year')['mv_assets'].sort_index() if 'mv_assets' in sponsor_df.columns else pd.Series(dtype=float)
    assets_stable = assets.pct_change().abs().mean() < 0.10 if not assets.empty else False
    liability_retired = sponsor_df.set_index('year')['liability_retired'].sort_index() if 'liability_retired' in sponsor_df.columns else pd.Series(dtype=float)
    liability_decline = (liability_retired.pct_change() < -0.05).any() if not liability_retired.empty else False
    funded = sponsor_df.set_index('year')['funded_status'].sort_index() if 'funded_status' in sponsor_df.columns else pd.Series(dtype=float)
    high_funding = funded.mean() > 0.95 if not funded.empty else False
    score = sum([ann_share > 0.5, assets_stable, liability_decline, high_funding])
    return score

# --- Main Agent ---
class DeRiskingAgent:
    """
    De-risking agent supporting multi-year trend analytics from master_db_all_years.parquet and sponsor_rollup_all_years.parquet.
    - Computes 3-year & 5-year slopes for actives, retirees, liabilities.
    - Detects freeze patterns, asset shifts, annuity purchase signals.
    - Adds composite multi-year de-risking score and new output fields.
    - Robustly handles new long-format data and missing data.
    """
    def __init__(self, master_df):
        self.master_df = master_df.copy()
        # Detect if multi-year (long format)
        self.is_multi_year = 'plan_year' in self.master_df.columns or 'PLAN_YEAR' in self.master_df.columns

    def _get_year_col(self, df):
        return 'plan_year' if 'plan_year' in df.columns else 'PLAN_YEAR' if 'PLAN_YEAR' in df.columns else 'year'

    def _slope(self, series, years=5):
        # Compute slope (trend) over last N years
        s = series.dropna().sort_index()
        if len(s) < 2:
            return np.nan
        s = s.tail(years)
        x = np.arange(len(s))
        y = s.values
        if len(x) < 2:
            return np.nan
        slope = np.polyfit(x, y, 1)[0]
        return slope

    def analyze_sponsor(self, sponsor_ein):
        df = self.master_df.copy()
        ein_col = 'ein' if 'ein' in df.columns else 'EIN'
        year_col = self._get_year_col(df)
        sdf = df[df[ein_col] == str(sponsor_ein)].copy()
        if sdf.empty:
            return {'error': 'Sponsor not found'}
        if sdf[year_col].nunique() < 2:
            return {'error': 'Insufficient years for trend analysis'}
        sdf = sdf.sort_values(year_col)
        sdf = sdf.set_index(year_col)
        # Compute 3-year and 5-year slopes for actives, retirees, liabilities
        slopes = {}
        for col in ['active', 'retired', 'liability_total']:
            if col in sdf.columns:
                slopes[f'{col}_3yr_slope'] = self._slope(sdf[col], 3)
                slopes[f'{col}_5yr_slope'] = self._slope(sdf[col], 5)
        # Annuitant ratio trend
        if 'annuitant_ratio' in sdf.columns:
            slopes['annuitant_ratio_5yr_trend'] = self._slope(sdf['annuitant_ratio'], 5)
        # Asset shift toward fixed income
        if 'asset_fixed_income_pct' in sdf.columns:
            slopes['fixed_income_shift_5yr'] = self._slope(sdf['asset_fixed_income_pct'], 5)
        # PRT likelihood: decline in retirees + retiree liability + annuity purchases
        prt_likelihood = False
        if all(col in sdf.columns for col in ['retired', 'liability_retired', 'annuity_purchases']):
            prt_likelihood = (
                (sdf['retired'].diff().min() < 0) and
                (sdf['liability_retired'].diff().min() < 0) and
                (sdf['annuity_purchases'].max() > 0)
            )
        # Freeze pattern: steep declines vs flat zeros
        freeze_pattern = (slopes.get('active_5yr_slope', 0) < -10) or (sdf['active'].min() == 0)
        # Asset shift timeline
        asset_shift_timeline = list(sdf['asset_fixed_income_pct'].diff().dropna()) if 'asset_fixed_income_pct' in sdf.columns else []
        # Composite de-risking score
        score = sum([
            slopes.get('active_5yr_slope', 0) < 0,
            slopes.get('annuitant_ratio_5yr_trend', 0) > 0,
            slopes.get('fixed_income_shift_5yr', 0) > 0,
            prt_likelihood
        ])
        # Output structure
        return {
            'five_year_slopes': slopes,
            'derisking_components': {
                'active_decline_rate': slopes.get('active_5yr_slope', np.nan),
                'annuitant_ratio_trend': slopes.get('annuitant_ratio_5yr_trend', np.nan),
                'fixed_income_shift': slopes.get('fixed_income_shift_5yr', np.nan),
                'annuity_purchase_signal': prt_likelihood
            },
            'prt_likelihood': prt_likelihood,
            'asset_shift_timeline': asset_shift_timeline,
            'sponsor_is_freezing': freeze_pattern,
            'sponsor_is_derisking': score >= 2,
            'composite_derisking_score': score
        }
