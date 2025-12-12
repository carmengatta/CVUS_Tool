"""
Peer Benchmarking Agent

This module provides a PeerBenchmarkAgent class for benchmarking a sponsor's DB plan metrics against industry peers using the master dataset.
"""

import pandas as pd
import numpy as np
import re
from collections import defaultdict

# --- Industry Classification ---
def classify_industry(row, business_code_col="business_code", sponsor_col="sponsor_dfe_name"):
    # Map business codes to industry (example mapping, expand as needed)
    business_code_map = {
        '5411': 'Professional Services',
        '3361': 'Manufacturing',
        '5241': 'Insurance',
        '6111': 'Education',
        '6221': 'Healthcare',
        # ... add more mappings ...
    }
    code = str(row.get(business_code_col, '')).strip()
    if code in business_code_map:
        return business_code_map[code]
    # Infer by sponsor name if code missing
    name = str(row.get(sponsor_col, '')).lower()
    if re.search(r'university|college|school', name):
        return 'Education'
    if re.search(r'hospital|clinic|health', name):
        return 'Healthcare'
    if re.search(r'insurance|mutual', name):
        return 'Insurance'
    if re.search(r'manufactur|auto|motor', name):
        return 'Manufacturing'
    if re.search(r'consult|law|account', name):
        return 'Professional Services'
    return 'Other'

# --- Peer Group Construction ---
def get_peer_group(master_df, sponsor_row, industry_col="industry"):
    industry = sponsor_row[industry_col]
    return master_df[master_df[industry_col] == industry]

# --- Peer Metrics Computation ---
def compute_peer_metrics(peer_df):
    metrics = {}
    metrics['annuitant_ratio'] = peer_df['annuitant_ratio'].mean()
    metrics['liability_per_active'] = peer_df['liability_per_active'].mean()
    metrics['liability_per_annuitant'] = peer_df['liability_per_retiree'].mean() if 'liability_per_retiree' in peer_df else np.nan
    metrics['funded_status'] = peer_df['funded_status'].mean()
    metrics['active_pct'] = (peer_df['active'].sum() / peer_df['total'].sum()) if peer_df['total'].sum() else np.nan
    metrics['retired_pct'] = (peer_df['retired'].sum() / peer_df['total'].sum()) if peer_df['total'].sum() else np.nan
    return metrics

# --- Z-score and Percentile ---
def compute_z_score(val, peer_series):
    mu = peer_series.mean()
    sigma = peer_series.std()
    if sigma == 0 or np.isnan(sigma):
        return 0
    return (val - mu) / sigma

def compute_percentile(val, peer_series):
    return (peer_series < val).mean()

# --- Mortality Comparison ---
def compare_mortality(sponsor_row, peer_df, mortality_col="mortality_code"):
    sponsor_mort = sponsor_row.get(mortality_col, None)
    peer_morts = peer_df[mortality_col].dropna().unique()
    if sponsor_mort is None or sponsor_mort == '':
        return False, "No mortality code reported."
    if sponsor_mort not in peer_morts:
        return True, "You differ from peers on mortality usage."
    return False, "Mortality usage is consistent with peers."

# --- Main Agent ---
class PeerBenchmarkAgent:
    """
    Peer benchmarking agent supporting multi-year analytics from master_db_all_years.parquet and sponsor_rollup_all_years.parquet.
    - Defaults to latest year for year-specific comparisons.
    - Adds multi-year peer comparison functions using 5-year averages.
    - Adds peer-based z-scores/percentiles using multi-year means.
    - Adds new output fields: multi_year_peer_summary, five_year_metrics, trend_vs_peers.
    - Preserves backward compatibility if only a single year is provided.
    """
    def __init__(self, master_df):
        self.master_df = master_df.copy()
        # Add industry classification if not present
        if 'industry' not in self.master_df.columns:
            self.master_df['industry'] = self.master_df.apply(classify_industry, axis=1)

        # Detect if multi-year (long format) or single-year
        self.is_multi_year = 'plan_year' in self.master_df.columns or 'PLAN_YEAR' in self.master_df.columns

    def _get_latest_year(self, df, ein_col='ein', year_col='plan_year'):
        # Robustly get the latest year for each EIN
        if year_col not in df.columns:
            year_col = year_col.upper()
        if ein_col not in df.columns:
            ein_col = ein_col.upper()
        idx = df.groupby(ein_col)[year_col].idxmax()
        return df.loc[idx].reset_index(drop=True)

    def _five_year_metrics(self, peer_df, year_col='plan_year'):
        # Compute 5-year averages for key metrics
        if year_col not in peer_df.columns:
            year_col = year_col.upper()
        last5 = peer_df.sort_values(year_col).groupby('ein').tail(5)
        metrics = {}
        metrics['annuitant_ratio_5yr'] = last5['annuitant_ratio'].mean()
        metrics['liability_per_active_5yr'] = last5['liability_per_active'].mean()
        metrics['liability_per_annuitant_5yr'] = last5['liability_per_retiree'].mean() if 'liability_per_retiree' in last5 else np.nan
        metrics['retiree_share_5yr'] = (last5['retired'].sum() / last5['total'].sum()) if last5['total'].sum() else np.nan
        metrics['liability_growth_5yr'] = last5['liability_total'].pct_change().mean() if 'liability_total' in last5 else np.nan
        return metrics

    def _trend_vs_peers(self, sponsor_metrics, peer_metrics):
        # Compare sponsor's 5-year metrics to peer 5-year metrics
        trend = {}
        for k in sponsor_metrics:
            if k in peer_metrics and peer_metrics[k] not in [None, np.nan]:
                diff = sponsor_metrics[k] - peer_metrics[k]
                trend[k + '_vs_peer'] = diff
        return trend

    def benchmark_sponsor(self, sponsor_ein):
        # Multi-year: use latest year for year-specific, 5-year for trends
        df = self.master_df.copy()
        ein_col = 'ein' if 'ein' in df.columns else 'EIN'
        year_col = 'plan_year' if 'plan_year' in df.columns else 'PLAN_YEAR'
        # Defensive: fallback to single-year if only one year exists
        if self.is_multi_year and df[ein_col].value_counts().max() > 1:
            latest = self._get_latest_year(df, ein_col, year_col)
            sponsor_row = latest[latest[ein_col] == str(sponsor_ein)].iloc[0]
            industry = sponsor_row['industry']
            peer_group = latest[latest['industry'] == industry]
            # 5-year peer group for multi-year metrics
            peer_multi = df[df['industry'] == industry]
            peer_5yr = self._five_year_metrics(peer_multi, year_col)
            sponsor_5yr = self._five_year_metrics(df[df[ein_col] == str(sponsor_ein)], year_col)
            trend_vs_peers = self._trend_vs_peers(sponsor_5yr, peer_5yr)
            # Z-scores and percentiles for 5-year means
            comparison_flags = {}
            for metric in ['annuitant_ratio', 'liability_per_active', 'liability_per_annuitant']:
                sponsor_val = sponsor_5yr.get(f'{metric}_5yr', np.nan)
                peer_vals = peer_multi.groupby(ein_col)[metric].mean()
                z = compute_z_score(sponsor_val, peer_vals)
                pct = compute_percentile(sponsor_val, peer_vals)
                comparison_flags[f'{metric}_5yr_zscore'] = z
                comparison_flags[f'{metric}_5yr_percentile'] = pct
            # Mortality comparison (latest year)
            diff_mort, mort_msg = compare_mortality(sponsor_row, peer_group)
            comparison_flags['mortality_differs'] = diff_mort
            comparison_flags['mortality_message'] = mort_msg
            # Output
            return {
                'industry': industry,
                'peer_group_summary': {
                    'count': len(peer_group),
                    'peer_metrics': compute_peer_metrics(peer_group)
                },
                'sponsor_metrics': {
                    'annuitant_ratio_5yr': sponsor_5yr['annuitant_ratio_5yr'],
                    'liability_per_active_5yr': sponsor_5yr['liability_per_active_5yr'],
                    'liability_per_annuitant_5yr': sponsor_5yr['liability_per_annuitant_5yr'],
                    'retiree_share_5yr': sponsor_5yr['retiree_share_5yr'],
                    'liability_growth_5yr': sponsor_5yr['liability_growth_5yr']
                },
                'multi_year_peer_summary': peer_5yr,
                'five_year_metrics': sponsor_5yr,
                'trend_vs_peers': trend_vs_peers,
                'comparison_flags': comparison_flags,
                'interpretation': f"Sponsor {sponsor_ein} 5-year trend vs peers computed."
            }
        else:
            # Single-year fallback (backward compatible)
            sponsor_row = df[df[ein_col] == str(sponsor_ein)].iloc[0]
            industry = sponsor_row['industry']
            peer_group = get_peer_group(df, sponsor_row)
            peer_metrics = compute_peer_metrics(peer_group)
            sponsor_metrics = {
                'annuitant_ratio': sponsor_row.get('annuitant_ratio', np.nan),
                'liability_per_active': sponsor_row.get('liability_per_active', np.nan),
                'liability_per_annuitant': sponsor_row.get('liability_per_retiree', np.nan),
                'funded_status': sponsor_row.get('funded_status', np.nan),
                'active_pct': sponsor_row.get('active', np.nan) / sponsor_row.get('total', np.nan) if sponsor_row.get('total', np.nan) else np.nan,
                'retired_pct': sponsor_row.get('retired', np.nan) / sponsor_row.get('total', np.nan) if sponsor_row.get('total', np.nan) else np.nan,
            }
            # Z-scores and percentiles
            comparison_flags = {}
            for metric in ['annuitant_ratio', 'liability_per_active', 'liability_per_annuitant', 'funded_status']:
                if metric in sponsor_metrics and metric in peer_metrics:
                    z = compute_z_score(sponsor_metrics[metric], peer_group[metric])
                    pct = compute_percentile(sponsor_metrics[metric], peer_group[metric])
                    comparison_flags[f'{metric}_zscore'] = z
                    comparison_flags[f'{metric}_percentile'] = pct
            # Mortality comparison
            diff_mort, mort_msg = compare_mortality(sponsor_row, peer_group)
            comparison_flags['mortality_differs'] = diff_mort
            comparison_flags['mortality_message'] = mort_msg
            interpretation = f"Sponsor {sponsor_ein} in industry '{industry}' is {'above' if comparison_flags.get('annuitant_ratio_zscore',0) > 0 else 'below'} peer average annuitant ratio. "
            if diff_mort:
                interpretation += mort_msg
            return {
                'industry': industry,
                'peer_group_summary': {
                    'count': len(peer_group),
                    'peer_metrics': peer_metrics
                },
                'sponsor_metrics': sponsor_metrics,
                'comparison_flags': comparison_flags,
                'interpretation': interpretation
            }
