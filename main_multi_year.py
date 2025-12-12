import os
import logging
import pandas as pd

"""
Form 5500 Multi-Year Pipeline Orchestration

This script orchestrates the multi-year DB plan pipeline.
No data logic is present here.
"""

import logging
import sys
from data_ingestion.multi_year_ingestion import run_multi_year_pipeline

def main():
	logging.basicConfig(
		level=logging.INFO,
		format="[%(asctime)s] %(levelname)s: %(message)s",
		datefmt="%Y-%m-%d %H:%M:%S"
	)
	logging.info("Starting Form 5500 multi-year DB plan pipeline...")
	try:
		run_multi_year_pipeline()
		logging.info("Pipeline completed successfully.")
	except Exception as e:
		logging.error(f"Pipeline failed: {e}")
		sys.exit(1)

if __name__ == "__main__":
	main()
