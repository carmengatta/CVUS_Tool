# Form5500 Tool

Analytics platform for U.S. private-sector defined benefit plans using Form 5500 and Schedule SB data.

### Features
- Ingests raw Form 5500 + Schedule SB filings
- Normalizes and merges plan-level actuarial data
- Generates master datasets and sponsor rollups
- Identifies plan risks, frozen plans, annuitant-heavy profiles
- Streamlit dashboard for interactive exploration

### Repository Structure
Form5500_Tool/
├── data_ingestion/
├── data_analysis/
├── utils/
├── streamlit_app/
├── data_output/ (only parquet files tracked)
└── test.py


### Deployment
This app deploys to Streamlit Cloud using `streamlit_app/app.py`.

---
