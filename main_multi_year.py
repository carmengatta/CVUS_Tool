import logging
import sys

"""
Form 5500 Multi-Year Pipeline Orchestration

Responsibilities:
- Orchestrate per-year DB plan ingestion (Schedule SB–driven)
- Produce canonical yearly parquet files
- Optionally produce derived multi-year datasets

No data logic should live here.
"""

from data_ingestion.multi_year_ingestion import run_multi_year_pipeline


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info("Starting Form 5500 multi-year DB plan pipeline...")
    logging.info("Canonical outputs: per-year DB-only parquet files")

    try:
        run_multi_year_pipeline()
        logging.info("Pipeline completed successfully.")
    except Exception as e:
        logging.exception("Pipeline failed with an unexpected error.")
        sys.exit(1)


if __name__ == "__main__":
    main()
