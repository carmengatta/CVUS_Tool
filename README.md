Form5500 Tool -- Analytics Platform for U.S. Defined Benefit Plans

The Form5500 Tool is a data ingestion, cleaning, and analytics platform designed to extract actionable insights from Form 5500 and Schedule SB filings for U.S. private-sector defined benefit (DB) pension plans.

It produces enriched plan-level and sponsor-level datasets, rankings, and a Streamlit dashboard for interactive exploration -- enabling actuarial, consulting, and longevity insights at scale.

Key Features

1. Automated Data Ingestion

- Reads raw Form 5500, Schedule SB, Schedule R, and Schedule H datasets (CSV)
- Normalizes actuarial fields (participant counts, liabilities, segment rates, etc.)
- Standardizes EIN, plan number, sponsor information, and actuarial vendor data
- Handles messy or missing fields using robust preprocessing logic
- Supports multi-year ingestion (2019-2024)

2. Intelligent Dataset Merging

- Merges Schedule SB and Form 5500 using EIN + PLAN_NUMBER + PLAN_YEAR (compound key)
- Left-joins Schedule R asset allocation data (preserving all SB plans)
- Left-joins Schedule H financial data (PRT transactions, assets, expenses)
- Flags mismatches, partial matches, and missing metadata

3. Master Enriched DB Dataset

Produces a unified plan-level dataset containing:

- Participants: Active, Retired, Terminated, Total
- Actuarial liabilities (active, retired, total)
- Segment rates (1st, 2nd, 3rd) and effective interest rate
- Mortality code
- Sponsor information (industry, business code)
- Actuary name & firm
- Schedule H: PRT transactions, total assets, investment allocations, PBGC coverage
- Derived metrics: Annuitant Ratio, YoY changes, freeze detection, de-risking flags

4. Sponsor-Level Rollups

Aggregates all plans sharing a TRACKING_ID (EIN + PLAN_NUMBER):

- Total liabilities and participant counts
- Multi-year trend metrics (5-year rates of change)
- Behavioral flags: freezing, de-risking, annuity purchasing, longevity risk
- Peer median comparisons

5. PRT Multi-Year Analysis

- Identifies plans with Pension Risk Transfer transactions across 2019-2024
- Tracks repeat transactors and transaction volumes
- Sponsor-level PRT history

6. Streamlit Interactive Dashboard

Includes:

- Dashboard with KPIs (plans, retirees, liability, participants)
- Substitute Mortality analysis by industry and actuarial firm
- Industry Explorer (NAICS-based filtering)
- PRT Analysis (transactions, opportunities, asset analysis)
- PRT History (multi-year, repeat transactors, trends)
- Actuarial Firms browser with name normalization
- Data Explorer
- Password-protected access (via Streamlit secrets or environment variable)

Repository Structure

    Form5500_Tool/
    |
    |-- data_ingestion/           # Load & normalize raw SB + 5500 + SR + H data
    |   |-- load_csv.py           # Robust CSV loader with encoding fallback
    |   |-- load_excel.py         # Excel file loader
    |   |-- normalize_sb_fields.py    # Schedule SB actuarial field normalization
    |   |-- normalize_sr_fields.py    # Schedule R field normalization
    |   |-- normalize_sch_h_fields.py # Schedule H financial field normalization
    |   |-- merge_sb_5500.py      # Merge SB + Form 5500
    |   |-- merge_sb_sr.py        # Merge SB + Schedule R (left join)
    |   |-- merge_schedule_h.py   # Merge Schedule H + PRT analysis fields
    |   |-- multi_year_ingestion.py   # Primary multi-year pipeline orchestrator
    |   |-- combine_years.py      # Alternative multi-year engine (legacy)
    |
    |-- data_analysis/            # Build master datasets & rollups
    |   |-- build_master_dataset.py   # Multi-year master with YoY metrics
    |   |-- build_sponsor_rollup.py   # Sponsor-level aggregation & flags
    |   |-- prt_multi_year_analysis.py # PRT transaction history analysis
    |
    |-- utils/                    # Shared helpers & validation
    |   |-- validate_alignment.py # SB/5500 dataset alignment checker
    |   |-- constants.py          # Shared column name constants
    |   |-- naics_codes.py        # NAICS industry code mapping
    |   |-- normalize_firm_names.py # Actuarial firm name canonicalization
    |
    |-- data_output/              # Final parquet datasets used by Streamlit
    |   |-- yearly/               # Per-year DB plan parquets
    |   |-- prt_multi_year_history.parquet
    |   |-- master_db_all_years.parquet
    |   |-- sponsor_rollup_all_years.parquet
    |
    |-- streamlit_app/            # Cloud-deployable UI
    |   |-- app.py
    |   |-- requirements.txt
    |
    |-- pdf_extraction/           # PDF Form 5500 text extraction & chunking
    |-- inhouse_detection/        # In-house actuary detection via SERP/scraping
    |
    |-- test.py                   # End-to-end pipeline test runner
    |-- main_multi_year.py        # Pipeline orchestrator
    |-- .gitignore
    |-- README.md

Local Setup

1. Clone the repo:
   git clone https://github.com/carmengatta/Form5500_Tool.git
   cd Form5500_Tool

2. Install dependencies:
   pip install -r streamlit_app/requirements.txt

3. Run the pipeline:
   python main_multi_year.py

4. Run the Streamlit dashboard locally:
   streamlit run streamlit_app/app.py

Deployment (Streamlit Cloud)

This project is deployable with one click via Streamlit Cloud.

Deployment Steps:

1. Go to: https://share.streamlit.io
2. Select the repository: carmengatta/Form5500_Tool
3. App entry point: streamlit_app/app.py
4. Add Streamlit secrets (.streamlit/secrets.toml equivalent):
   [auth]
   password = "yourpasswordhere"

The app will automatically rebuild whenever new dataset outputs are committed.

Security & Data Handling

- Raw federal datasets (Form 5500 & SB input files) are not stored in the repo.
- Only compressed & preprocessed parquet outputs are versioned.
- Streamlit app password is read from secrets/environment (not hardcoded).
- Safe for internal analysis, demos, and client engagements.

Future Enhancements (Roadmap)

- Automatic data ingestion from DOL EFAST2 API
- Funded status calculation (Assets / Funding Target)
- Actuarial firm switching detection
- Lead scoring engine for business development
- Interactive liability/participant trend charts
- Upload-your-own-SB-file app mode

Author

Carmen Gatta FSA, EA, MAAA
Club Vita, US
https://github.com/carmengatta
