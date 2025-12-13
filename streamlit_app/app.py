
import streamlit as st
import pandas as pd
import os
import re

# =============================
# SIMPLE PASSWORD PROTECTION
# =============================
PASSWORD = "CVUSTool"  # CHANGE FOR DEPLOYMENT
def password_gate():
    pw = st.text_input("Enter password:", type="password")
    if pw == PASSWORD:
        st.session_state["authenticated"] = True
        st.success("Access granted!")
        st.rerun()
    elif pw != "":
        st.error("Incorrect password")
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False
if not st.session_state["authenticated"]:
    password_gate()
    st.stop()

# =============================
# SIDEBAR NAVIGATION
# =============================
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/6/6b/Logo_example.png", width=120)
st.sidebar.title("Form 5500 DB Tool")
st.sidebar.markdown("---")

menu = st.sidebar.radio(
    "Navigation",
    ["Dashboard", "Data Explorer", "About", "Logout"],
    index=0,
    key="nav_radio"
)

if menu == "Logout":
    st.session_state["authenticated"] = False
    st.rerun()

# =============================
# YEAR SELECTION & DATA LOADING
# =============================
YEARLY_DIR = "data_output/yearly"
year_files = [f for f in os.listdir(YEARLY_DIR) if f.startswith("db_plans_") and f.endswith(".parquet")]
years_available = sorted([
    int(m.group(1))
    for f in year_files
    if (m := re.match(r"db_plans_(\d{4})\.parquet$", f))
])
if not years_available:
    st.error("No yearly DB plan files found in data_output/yearly.")
    st.stop()
def_year = max(years_available)
st.sidebar.markdown("## Data Controls")
selected_year = st.sidebar.selectbox("Filing Year", years_available, index=years_available.index(def_year))
st.sidebar.markdown("---")

@st.cache_data
def load_db_parquet(year):
    path = os.path.join(YEARLY_DIR, f"db_plans_{year}.parquet")
    if not os.path.exists(path):
        st.error(f"Missing required dataset: `{path}`")
        st.stop()
    return pd.read_parquet(path)

db = load_db_parquet(selected_year)

# =============================
# DASHBOARD PAGE
# =============================
if menu == "Dashboard":
    st.title(f"Defined Benefit Plan Dashboard — {selected_year}")
    st.caption("All data is DB-only, SB-driven, and year-specific.")

    # --- KPIs ---
    kpi_cols = st.columns(4)
    total_plans = len(db)
    retiree_col = next((c for c in ["RETIREE_COUNT", "RETIRED"] if c in db.columns), None)
    total_retirees = int(db[retiree_col].sum()) if retiree_col else "N/A"
    liability_col = "LIABILITY_TOTAL" if "LIABILITY_TOTAL" in db.columns else None
    total_liability = float(db[liability_col].sum()) if liability_col else "N/A"
    participant_col = "SB_TERM_PARTCP_CNT" if "SB_TERM_PARTCP_CNT" in db.columns else None
    total_participants = int(db[participant_col].sum()) if participant_col else "N/A"
    kpi_cols[0].metric("Total Plans", total_plans)
    kpi_cols[1].metric("Total Retirees", total_retirees)
    kpi_cols[2].metric("Total Liability", f"${total_liability:,.0f}" if total_liability != "N/A" else "N/A")
    kpi_cols[3].metric("Total Participants", total_participants)

    st.markdown("---")
    tab1, tab2, tab3, tab4 = st.tabs([
        "Top Plans", "Top Companies", "Plan Size Distribution", "Participant Mix"
    ])

    with tab1:
        st.subheader("Top Plans by Retiree Count")
        top_n = st.slider("Show top N plans", 5, 50, 10, key="top_n_slider")
        retiree_col = next((c for c in ["RETIREE_COUNT", "RETIRED"] if c in db.columns), None)
        if retiree_col:
            cols = [c for c in ["EIN", "PLAN_NAME", retiree_col, "SB_TERM_PARTCP_CNT", "LIABILITY_TOTAL"] if c in db.columns]
            top_plans = db.sort_values(retiree_col, ascending=False).head(top_n)
            st.dataframe(top_plans[cols], use_container_width=True)
            st.download_button("Download Table", top_plans[cols].to_csv(index=False), file_name="top_plans.csv")
        else:
            st.warning("Retiree count column not found in this file.")

    with tab2:
        st.subheader("Top Companies by Total Retirees (EIN Rollup)")
        retiree_col = next((c for c in ["RETIREE_COUNT", "RETIRED"] if c in db.columns), None)
        if retiree_col and "EIN" in db.columns:
            agg_dict = {retiree_col: "sum"}
            if "PLAN_NAME" in db.columns:
                agg_dict["PLAN_NAME"] = "count"
            if "LIABILITY_TOTAL" in db.columns:
                agg_dict["LIABILITY_TOTAL"] = "sum"
            ein_rollup = db.groupby(["EIN"]).agg(agg_dict).rename(columns={"PLAN_NAME": "NUM_PLANS"}).reset_index()
            ein_rollup = ein_rollup.sort_values(retiree_col, ascending=False)
            st.dataframe(ein_rollup.head(top_n), use_container_width=True)
            st.download_button("Download Table", ein_rollup.head(top_n).to_csv(index=False), file_name="top_companies.csv")
        else:
            st.warning("Required columns not found for company rollup.")

    with tab3:
        st.subheader("Plan Size Distribution")
        st.write("Distribution of plans by participant count (SB_TERM_PARTCP_CNT)")
        if "SB_TERM_PARTCP_CNT" in db.columns:
            st.bar_chart(db["SB_TERM_PARTCP_CNT"].value_counts().sort_index())
        else:
            st.warning("Participant count column not found in this file.")

    with tab4:
        st.subheader("Participant Mix (Active / Retired / Terminated)")
        active_col = next((c for c in ["ACTIVE_COUNT", "ACTIVE"] if c in db.columns), None)
        retiree_col = next((c for c in ["RETIREE_COUNT", "RETIRED"] if c in db.columns), None)
        term_col = next((c for c in ["SEPARATED_COUNT", "TERMINATED"] if c in db.columns), None)
        if all([active_col, retiree_col, term_col]):
            mix = db[[active_col, retiree_col, term_col]].sum()
            mix_pct = mix / mix.sum()
            st.write("Percent composition of all loaded plans:")
            st.bar_chart(mix_pct)
        else:
            st.info("Participant mix columns not available in this dataset.")

# =============================
# DATA EXPLORER PAGE
# =============================
elif menu == "Data Explorer":
    st.title("Data Explorer")
    st.caption(f"Explore and filter DB plan data for {selected_year}.")
    st.markdown("---")
    # Filter widgets
    col1, col2 = st.columns(2)
    with col1:
        ein_filter = st.text_input("Filter by EIN (partial or full)")
    with col2:
        plan_filter = st.text_input("Filter by Plan Name (partial)")
    filtered = db.copy()
    if ein_filter:
        filtered = filtered[filtered["EIN"].astype(str).str.contains(ein_filter, case=False, na=False)]
    if plan_filter:
        filtered = filtered[filtered["PLAN_NAME"].astype(str).str.contains(plan_filter, case=False, na=False)]
    st.write(f"Showing {len(filtered)} plans.")
    st.dataframe(filtered, use_container_width=True)
    st.download_button("Download Filtered Data", filtered.to_csv(index=False), file_name="filtered_plans.csv")

# =============================
# ABOUT PAGE
# =============================
elif menu == "About":
    st.title("About This Tool")
    st.markdown("""
    **Form 5500 Defined Benefit Plan Dashboard**  
    Version: 2.0 (Redesigned Dec 2025)
    
    This tool provides interactive analytics and data exploration for Defined Benefit (DB) plans using Form 5500 data.  
    - **Dashboard:** Key metrics, top plans, and participant mix
    - **Data Explorer:** Search and filter all plans
    - **Password protected** for data privacy
    
    **Data Source:** Processed from annual Form 5500 filings (SB-driven, DB-only)
    
    **Contact:** [Your Name/Org] — [your@email.com]
    """)
else:
    st.info("Participant mix columns not available in this dataset.")
