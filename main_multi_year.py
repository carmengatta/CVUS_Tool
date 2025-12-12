import os
import logging
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# --- Step 1: Multi-year ingestion ---
logging.info('Step 1: Loading all raw Form 5500 / SB / Schedule R files (2019–2024)')
from data_ingestion.multi_year_ingestion import load_multi_year_data  # Assumes this exists per pipeline design

yearly_merged_list = load_multi_year_data()
logging.info(f'Loaded {len(yearly_merged_list)} year(s) of merged data.')

# --- Step 2: Build master dataset ---
logging.info('Step 2: Building master dataset (all years)')
from data_analysis.build_master_dataset import build_master_dataset
master_df = build_master_dataset(yearly_merged_list)
logging.info(f'Master dataset shape: {master_df.shape}')

# --- Step 3: Build sponsor multi-year rollup ---
logging.info('Step 3: Building sponsor multi-year rollup')
from data_analysis.build_sponsor_rollup import build_sponsor_rollup
sponsor_rollup_df = build_sponsor_rollup(master_df)
logging.info(f'Sponsor rollup shape: {sponsor_rollup_df.shape}')

# --- Step 4: Select test sponsor (largest liability) ---
logging.info('Step 4: Selecting test sponsor (largest liability)')
liab_col = 'TOTAL_LIABILITY' if 'TOTAL_LIABILITY' in sponsor_rollup_df.columns else 'liability_total'
test_row = sponsor_rollup_df.sort_values(liab_col, ascending=False).iloc[0]
test_ein = test_row['EIN'] if 'EIN' in test_row else test_row['ein']
logging.info(f'Test sponsor EIN: {test_ein}')

# --- Step 5: Run all agents ---
logging.info('Step 5: Running all agents on test sponsor')
from agents.peer_benchmark_agent import PeerBenchmarkAgent
from agents.derisking_agent import DeRiskingAgent
from agents.longevity_insights_agent import LongevityInsightsAgent
from agents.report_generation_agent import ReportGenerationAgent

peer_agent = PeerBenchmarkAgent(sponsor_rollup_df)
derisk_agent = DeRiskingAgent(sponsor_rollup_df)
longevity_agent = LongevityInsightsAgent(sponsor_rollup_df)
report_agent = ReportGenerationAgent()

peer_output = peer_agent.benchmark_sponsor(test_ein)
derisk_output = derisk_agent.analyze_sponsor(test_ein)
longevity_output = longevity_agent.analyze_sponsor(test_ein)
report_output = report_agent.generate_report(peer_output, derisk_output, longevity_output, test_row)

# --- Step 6: Print structured outputs ---
logging.info('Step 6: Printing structured outputs for validation')
import pprint
pp = pprint.PrettyPrinter(indent=2)
print('\n--- Peer Benchmark Output ---')
pp.pprint(peer_output)
print('\n--- De-risking Output ---')
pp.pprint(derisk_output)
print('\n--- Longevity Insights Output ---')
pp.pprint(longevity_output)
print('\n--- Report Output ---')
pp.pprint(report_output)

logging.info('Pipeline completed successfully.')
