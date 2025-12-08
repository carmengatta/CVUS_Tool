📘 Form5500 Tool — Analytics Platform for U.S. Defined Benefit Plans

The Form5500 Tool is a data ingestion, cleaning, and analytics platform designed to extract actionable insights from Form 5500 and Schedule SB filings for U.S. private-sector defined benefit (DB) pension plans.

It produces enriched plan-level and sponsor-level datasets, rankings, and a Streamlit dashboard for interactive exploration — enabling actuarial, consulting, and longevity insights at scale.

🚀 Key Features
🔍 1. Automated Data Ingestion

Reads raw Form 5500 and Schedule SB datasets (CSV, TXT, Excel)

Normalizes actuarial fields (participant counts, liabilities, segment rates, etc.)

Standardizes EIN, plan number, sponsor information, and actuarial vendor data

Handles messy or missing fields using robust preprocessing logic

🔗 2. Intelligent Dataset Merging

Merges Schedule SB and Form 5500 using:

Primary key: ACK_ID

Cross-verification: EIN + Plan Number

Flags mismatches, partial matches, and missing metadata

📊 3. Master Enriched DB Dataset

Produces a unified plan-level dataset containing:

Participants: Active, Retired, Terminated, Total

Actuarial liabilities (active, retired, total)

Effective interest rates and segment rates

Mortality code

Sponsor information (industry, business code)

Actuary name & firm

Derived metrics:

Annuitant Ratio

Liability per Active / Retiree

DB Size Category

Merge Quality Indicators

🏢 4. Sponsor-Level Rollups

Aggregates all plans sharing an EIN:

Total liabilities

Total participant counts

Count of plans under the sponsor

Combined annuitant exposure

Sorted profiles for outreach/business development

📈 5. Streamlit Interactive Dashboard

Includes:

Plan Explorer (filter by EIN, sponsor, plan name)

Sponsor Profiles

Actuary firm identification

Largest annuitant populations

Lead scoring and prioritization

Searchable and sortable tables

Password-protected access (via Streamlit secrets)

📁 Repository Structure
Form5500_Tool/
│
├── data_ingestion/        # Load & normalize raw SB + 5500 data
│   ├── combine_years.py
│   ├── merge_sb_5500.py
│   ├── normalize_sb_fields.py
│
├── data_analysis/         # Build master datasets & rollups
│   ├── build_master_dataset.py
│   ├── build_sponsor_rollup.py
│
├── utils/                 # Shared helpers & validation
│   ├── validate_alignment.py
│   ├── constants.py
│
├── data_output/           # Final parquet datasets used by Streamlit
│   ├── master_db_latest.parquet
│   ├── sponsor_rollup_latest.parquet
│
├── streamlit_app/         # Cloud-deployable UI
│   ├── app.py
│   ├── requirements.txt
│
├── test.py                # End-to-end pipeline test runner
├── main.py                # Optional pipeline orchestrator
├── .gitignore
└── README.md

🛠 Local Setup
1️⃣ Clone the repo:
git clone https://github.com/carmengatta/Form5500_Tool.git
cd Form5500_Tool

2️⃣ Install dependencies:
pip install -r streamlit_app/requirements.txt

3️⃣ Run the Streamlit dashboard locally:
streamlit run streamlit_app/app.py

🧱 Deployment (Streamlit Cloud)

This project is deployable with one click via Streamlit Cloud.

Deployment Steps

Go to: https://share.streamlit.io

Select the repository:

carmengatta/Form5500_Tool


App entry point:

streamlit_app/app.py


Add Streamlit secrets (.streamlit/secrets.toml equivalent):

[auth]
password = "yourpasswordhere"


The app will automatically rebuild whenever new dataset outputs are committed.

🔐 Security & Data Handling

Raw federal datasets (Form 5500 & SB input files) are not stored in the repo.

Only compressed & preprocessed parquet outputs are versioned.

Streamlit app includes optional password protection.

Safe for internal analysis, demos, and client engagements.

📬 Future Enhancements (Roadmap)

Automatic data ingestion from DOL EFAST2 API

Multi-year trend analysis

Freeze detection & de-risking classification

Actuarial firm switching detection

Lead scoring engine for business development

Interactive liability/participant charts

Upload-your-own-SB-file app mode

✨ Author

Carmen Gatta FSA, EA, MAAA
Club Vita, US
https://github.com/carmengatta