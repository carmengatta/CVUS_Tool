"""
Multi-Year PRT Transaction Analysis

Analyzes PRT history across years 2019-2024 to identify:
- Plans with repeat PRT transactions
- Total PRT volume by sponsor
- Patterns in transaction timing
"""

import pandas as pd
import os
from pathlib import Path

DATA_OUTPUT_DIR = Path(__file__).parent.parent / "data_output" / "yearly"


def load_all_years(years: range = range(2019, 2025)) -> pd.DataFrame:
    """Load and combine all years of DB plan data."""
    all_dfs = []
    for year in years:
        path = DATA_OUTPUT_DIR / f"db_plans_{year}.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            df['YEAR'] = year
            all_dfs.append(df)
            prt_count = (df['SCH_H_PRT_AMOUNT'].fillna(0) > 0).sum()
            print(f"{year}: {len(df):,} plans, PRT > 0: {prt_count}")
    
    if not all_dfs:
        raise ValueError("No yearly data files found")
    
    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal records: {len(combined):,}")
    return combined


def analyze_prt_history(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze PRT transaction history across years.
    
    Returns DataFrame with one row per plan, showing all years with PRT activity.
    """
    # Create unique tracking ID per plan
    df['TRACKING_ID'] = df['EIN'].astype(str) + '_' + df['PLAN_NUMBER'].astype(str)
    
    # Filter to plans with PRT > 0
    prt_plans = df[df['SCH_H_PRT_AMOUNT'].fillna(0) > 0].copy()
    print(f"Total PRT transactions: {len(prt_plans):,}")
    
    # Determine sponsor name column (varies by year)
    sponsor_col = 'SPONSOR_DFE_NAME' if 'SPONSOR_DFE_NAME' in prt_plans.columns else 'SPONSOR_NAME'
    
    # Build aggregation dict with available columns
    agg_dict = {
        sponsor_col: 'first',
        'PLAN_NAME': 'first',
        'YEAR': list,
        'SCH_H_PRT_AMOUNT': ['sum', 'count', list],
        'EIN': 'first',
        'PLAN_NUMBER': 'first',
    }
    
    # Add optional columns if available
    if 'INDUSTRY_SECTOR' in prt_plans.columns:
        agg_dict['INDUSTRY_SECTOR'] = 'first'
    if 'SCH_H_TOTAL_ASSETS_EOY' in prt_plans.columns:
        agg_dict['SCH_H_TOTAL_ASSETS_EOY'] = 'last'
    
    # Group by tracking ID to aggregate across years
    prt_history = prt_plans.groupby('TRACKING_ID').agg(agg_dict).reset_index()
    
    # Flatten column names
    flat_cols = ['TRACKING_ID', 'SPONSOR_NAME', 'PLAN_NAME', 'YEARS', 
                 'TOTAL_PRT', 'NUM_TRANSACTIONS', 'PRT_BY_YEAR',
                 'EIN', 'PLAN_NUMBER']
    if 'INDUSTRY_SECTOR' in prt_plans.columns:
        flat_cols.append('INDUSTRY_SECTOR')
    if 'SCH_H_TOTAL_ASSETS_EOY' in prt_plans.columns:
        flat_cols.append('LATEST_ASSETS')
    
    prt_history.columns = flat_cols
    
    # Sort years and amounts together
    for idx, row in prt_history.iterrows():
        years_amounts = sorted(zip(row['YEARS'], row['PRT_BY_YEAR']))
        prt_history.at[idx, 'YEARS'] = [y for y, a in years_amounts]
        prt_history.at[idx, 'PRT_BY_YEAR'] = [a for y, a in years_amounts]
    
    return prt_history.sort_values('TOTAL_PRT', ascending=False)


def get_repeat_transactors(prt_history: pd.DataFrame, min_transactions: int = 2) -> pd.DataFrame:
    """Filter to plans with multiple PRT transactions across years."""
    return prt_history[prt_history['NUM_TRANSACTIONS'] >= min_transactions].copy()


def print_summary(prt_history: pd.DataFrame):
    """Print summary statistics."""
    total_prt = prt_history['TOTAL_PRT'].sum()
    unique_plans = len(prt_history)
    
    repeat = get_repeat_transactors(prt_history)
    repeat_prt = repeat['TOTAL_PRT'].sum()
    
    print("\n" + "="*60)
    print("PRT TRANSACTION HISTORY SUMMARY (2019-2024)")
    print("="*60)
    print(f"Unique plans with PRT: {unique_plans:,}")
    print(f"Total PRT volume: ${total_prt/1e9:.2f}B")
    print(f"\nPlans with repeat transactions: {len(repeat):,}")
    print(f"PRT from repeat transactors: ${repeat_prt/1e9:.2f}B ({repeat_prt/total_prt*100:.1f}%)")
    
    # Transaction frequency distribution
    print("\n--- Transaction Frequency ---")
    freq = prt_history['NUM_TRANSACTIONS'].value_counts().sort_index()
    for n_trans, count in freq.items():
        print(f"  {n_trans} transaction(s): {count} plans")
    
    print("\n--- Top 20 by Total PRT Volume ---")
    for i, (_, row) in enumerate(prt_history.head(20).iterrows(), 1):
        years_str = ', '.join([str(y) for y in row['YEARS']])
        print(f"{i:2}. {row['SPONSOR_NAME'][:45]:45} | Years: {years_str:20} | Total: ${row['TOTAL_PRT']/1e6:,.1f}M")
    
    print("\n--- Top 15 Repeat Transactors ---")
    for i, (_, row) in enumerate(repeat.head(15).iterrows(), 1):
        years_str = ', '.join([str(y) for y in row['YEARS']])
        amounts_str = ', '.join([f"${x/1e6:.1f}M" for x in row['PRT_BY_YEAR']])
        print(f"{i:2}. {row['SPONSOR_NAME'][:45]}")
        print(f"    Years: {years_str}")
        print(f"    Amounts: {amounts_str}")
        print(f"    Total: ${row['TOTAL_PRT']/1e6:,.1f}M")


def save_prt_history(prt_history: pd.DataFrame, output_path: Path = None):
    """Save PRT history to parquet and CSV."""
    if output_path is None:
        output_path = DATA_OUTPUT_DIR.parent
    
    # Save full history
    parquet_path = output_path / "prt_multi_year_history.parquet"
    prt_history.to_parquet(parquet_path, index=False)
    print(f"\nSaved: {parquet_path}")
    
    # Save repeat transactors to CSV for easy viewing
    repeat = get_repeat_transactors(prt_history)
    csv_path = output_path / "prt_repeat_transactors.csv"
    
    # Convert lists to strings for CSV
    repeat_csv = repeat.copy()
    repeat_csv['YEARS'] = repeat_csv['YEARS'].apply(lambda x: ', '.join(map(str, x)))
    repeat_csv['PRT_BY_YEAR'] = repeat_csv['PRT_BY_YEAR'].apply(lambda x: ', '.join([f"${v/1e6:.2f}M" for v in x]))
    repeat_csv.to_csv(csv_path, index=False)
    print(f"Saved: {csv_path}")
    
    return parquet_path, csv_path


if __name__ == "__main__":
    print("Loading multi-year DB plan data...")
    combined = load_all_years()
    
    print("\nAnalyzing PRT history...")
    prt_history = analyze_prt_history(combined)
    
    print_summary(prt_history)
    
    save_prt_history(prt_history)
