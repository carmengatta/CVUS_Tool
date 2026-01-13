
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
    ["Dashboard", "Actuarial Firms", "Data Explorer", "About", "Logout"],
    index=0,
    key="nav_radio"
)

if menu == "Logout":
    st.session_state["authenticated"] = False
    st.rerun()

# =============================
# YEAR SELECTION & DATA LOADING
# =============================
# Get the directory where this script lives, then go up one level to the project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
YEARLY_DIR = os.path.join(PROJECT_ROOT, "data_output", "yearly")
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
    retiree_col = "RETIREE_COUNT" if "RETIREE_COUNT" in db.columns else None
    total_retirees = int(db[retiree_col].sum()) if retiree_col else "N/A"
    liability_col = "LIABILITY_TOTAL" if "LIABILITY_TOTAL" in db.columns else None
    total_liability = float(db[liability_col].sum()) if liability_col else "N/A"
    participant_col = "TOTAL_PARTICIPANTS" if "TOTAL_PARTICIPANTS" in db.columns else None
    total_participants = int(db[participant_col].sum()) if participant_col else "N/A"
    kpi_cols[0].metric("Total Plans", total_plans)
    kpi_cols[1].metric("Total Retirees", total_retirees)
    kpi_cols[2].metric("Total Liability", f"{total_liability:,.0f}" if total_liability != "N/A" else "N/A")
    kpi_cols[3].metric("Total Participants", total_participants)

    st.markdown("---")
    # Modified: Removed 'Plan Size Distribution' and 'Participant Mix' tabs, added 'Location' tab
    tab1, tab2, tab3 = st.tabs([
        "Top Plans", "Top Companies", "Location"
    ])

    with tab1:
        st.subheader("Top Plans by Retiree Count")
        top_n = st.slider("Show top N plans", 5, 50, 10, key="top_n_slider")
        retiree_col = "RETIREE_COUNT" if "RETIREE_COUNT" in db.columns else None
        separated_col = "SEPARATED_COUNT" if "SEPARATED_COUNT" in db.columns else None
        active_col = "ACTIVE_COUNT" if "ACTIVE_COUNT" in db.columns else None
        total_col = "TOTAL_PARTICIPANTS" if "TOTAL_PARTICIPANTS" in db.columns else None
        if retiree_col:
            cols = [c for c in ["EIN", "PLAN_NAME", active_col, retiree_col, separated_col, total_col, "LIABILITY_TOTAL"] if c and c in db.columns]
            top_plans = db.sort_values(retiree_col, ascending=False).head(top_n)
            st.dataframe(top_plans[cols], use_container_width=True)
            st.download_button("Download Table", top_plans[cols].to_csv(index=False), file_name="top_plans.csv")
        else:
            st.warning("Retiree count column not found in this file.")

    with tab2:
        st.subheader("Top Companies by Total Retirees (EIN Rollup)")
        retiree_col = "RETIREE_COUNT" if "RETIREE_COUNT" in db.columns else None
        sponsor_col = next((c for c in ["SPONSOR_DFE_NAME", "SPONSOR_NAME"] if c in db.columns), None)
        if retiree_col and "EIN" in db.columns:
            agg_dict = {retiree_col: "sum"}
            if "PLAN_NAME" in db.columns:
                agg_dict["PLAN_NAME"] = "count"
            if "LIABILITY_TOTAL" in db.columns:
                agg_dict["LIABILITY_TOTAL"] = "sum"
            # For sponsor name, take the first non-null value per EIN
            if sponsor_col:
                sponsor_names = db.groupby("EIN")[sponsor_col].first().reset_index()
            else:
                sponsor_names = None
            ein_rollup = db.groupby(["EIN"]).agg(agg_dict).rename(columns={"PLAN_NAME": "NUM_PLANS"}).reset_index()
            if sponsor_names is not None:
                ein_rollup = ein_rollup.merge(sponsor_names, on="EIN", how="left")
            # Reorder columns for clarity
            display_cols = ["EIN"]
            if sponsor_col:
                display_cols.append(sponsor_col)
            display_cols += [col for col in [retiree_col, "NUM_PLANS", "LIABILITY_TOTAL"] if col in ein_rollup.columns]
            ein_rollup = ein_rollup.sort_values(retiree_col, ascending=False)
            st.dataframe(ein_rollup[display_cols].head(top_n), use_container_width=True)
            st.download_button("Download Table", ein_rollup[display_cols].head(top_n).to_csv(index=False), file_name="top_companies.csv")
        else:
            st.warning("Required columns not found for company rollup.")
        st.write(f"{total_plans:,}")
        st.write(f"{total_retirees:,}")

    # New Location tab
    with tab3:
        st.subheader("Plan Location (City & State)")
        # Identify columns
        city_col = next((c for c in ["SPONS_DFE_MAIL_US_CITY", "SPONSOR_CITY", "CITY"] if c in db.columns), None)
        state_col = next((c for c in ["SPONS_DFE_MAIL_US_STATE", "SPONSOR_STATE", "STATE"] if c in db.columns), None)
        sponsor_col = next((c for c in ["SPONSOR_DFE_NAME", "SPONSOR_NAME"] if c in db.columns), None)
        active_col = "ACTIVE_COUNT" if "ACTIVE_COUNT" in db.columns else None
        retiree_col = "RETIREE_COUNT" if "RETIREE_COUNT" in db.columns else None
        separated_col = "SEPARATED_COUNT" if "SEPARATED_COUNT" in db.columns else None
        total_col = "TOTAL_PARTICIPANTS" if "TOTAL_PARTICIPANTS" in db.columns else None

        if city_col and state_col:
            # Build DataFrame with all required columns
            columns = ["EIN", "PLAN_NAME"]
            if sponsor_col:
                columns.append(sponsor_col)
            columns += [city_col, state_col]
            if active_col:
                columns.append(active_col)
            if retiree_col:
                columns.append(retiree_col)
            if separated_col:
                columns.append(separated_col)
            if total_col:
                columns.append(total_col)
            loc_df = db[columns].drop_duplicates()
            rename_dict = {city_col: "City", state_col: "State"}
            if sponsor_col:
                rename_dict[sponsor_col] = "Plan Sponsor"
            if active_col:
                rename_dict[active_col] = "Active Count"
            if retiree_col:
                rename_dict[retiree_col] = "Retiree Count"
            if separated_col:
                rename_dict[separated_col] = "Terminated Vested Count"
            if total_col:
                rename_dict[total_col] = "Total Count"
            loc_df = loc_df.rename(columns=rename_dict)

            # Multi-select filter for State
            all_states = sorted(loc_df["State"].dropna().unique())
            selected_states = st.multiselect("Filter by State(s)", all_states, default=all_states)
            filtered_df = loc_df[loc_df["State"].isin(selected_states)]

            st.dataframe(filtered_df, use_container_width=True)
            st.download_button("Download Locations", filtered_df.to_csv(index=False), file_name="plan_locations.csv")
        else:
            st.warning("City and/or State columns not found in this file.")

# =============================
# ACTUARIAL FIRMS PAGE
# =============================
elif menu == "Actuarial Firms":
    st.title(f"Actuarial Firms — {selected_year}")
    st.caption("View and filter plans by their actuarial firm.")
    st.markdown("---")
    
    # Identify actuary firm column
    actuary_firm_col = next((c for c in ["ACTUARY_FIRM_NAME", "SB_ACTUARY_FIRM_NAME"] if c in db.columns), None)
    actuary_name_col = next((c for c in ["ACTUARY_NAME", "SB_ACTUARY_NAME_LINE"] if c in db.columns), None)
    actuary_city_col = next((c for c in ["ACTUARY_CITY", "SB_ACTUARY_US_CITY"] if c in db.columns), None)
    actuary_state_col = next((c for c in ["ACTUARY_STATE", "SB_ACTUARY_US_STATE"] if c in db.columns), None)
    
    if actuary_firm_col and actuary_firm_col in db.columns:
        # Clean up firm names for filtering
        db_firms = db.copy()
        db_firms[actuary_firm_col] = db_firms[actuary_firm_col].fillna("").astype(str).str.strip()
        db_firms = db_firms[db_firms[actuary_firm_col] != ""]
        
        # Get list of unique firms sorted by plan count
        firm_counts = db_firms.groupby(actuary_firm_col).size().reset_index(name='Plan Count')
        firm_counts = firm_counts.sort_values('Plan Count', ascending=False)
        all_firms = firm_counts[actuary_firm_col].tolist()
        
        # KPIs for actuarial firms
        kpi_cols = st.columns(3)
        kpi_cols[0].metric("Total Firms", len(all_firms))
        kpi_cols[1].metric("Plans with Actuary Data", len(db_firms))
        top_firm = all_firms[0] if all_firms else "N/A"
        top_firm_count = firm_counts['Plan Count'].iloc[0] if len(firm_counts) > 0 else 0
        kpi_cols[2].metric("Largest Firm (by # Plans)", f"{top_firm_count:,} plans")
        
        st.markdown("---")
        
        # Tabs for different views
        tab1, tab2 = st.tabs(["Browse by Firm", "Firm Rankings"])
        
        with tab1:
            st.subheader("Filter Plans by Actuarial Firm")
            
            # Search box for firm name
            firm_search = st.text_input("Search for actuarial firm (partial name)", key="firm_search")
            
            # Filter firms based on search
            if firm_search:
                filtered_firms = [f for f in all_firms if firm_search.lower() in f.lower()]
            else:
                filtered_firms = all_firms
            
            if filtered_firms:
                # Selectbox to pick a firm
                selected_firm = st.selectbox(
                    "Select Actuarial Firm",
                    options=filtered_firms,
                    index=0,
                    key="firm_select"
                )
                
                # Filter data for selected firm
                firm_data = db_firms[db_firms[actuary_firm_col] == selected_firm]
                
                st.markdown(f"### Plans managed by: **{selected_firm}**")
                st.write(f"Total plans: **{len(firm_data):,}**")
                
                # Build display columns
                display_cols = ["EIN", "PLAN_NAME"]
                sponsor_col = next((c for c in ["SPONSOR_DFE_NAME", "SPONSOR_NAME"] if c in firm_data.columns), None)
                if sponsor_col:
                    display_cols.append(sponsor_col)
                if actuary_name_col and actuary_name_col in firm_data.columns:
                    display_cols.append(actuary_name_col)
                if actuary_city_col and actuary_city_col in firm_data.columns:
                    display_cols.append(actuary_city_col)
                if actuary_state_col and actuary_state_col in firm_data.columns:
                    display_cols.append(actuary_state_col)
                
                # Add participant/liability columns
                for col in ["ACTIVE_COUNT", "RETIREE_COUNT", "SEPARATED_COUNT", "TOTAL_PARTICIPANTS", "TOTAL_LIABILITY"]:
                    if col in firm_data.columns:
                        display_cols.append(col)
                
                # Filter out columns that don't exist
                display_cols = [c for c in display_cols if c in firm_data.columns]
                
                # Rename columns for display
                rename_map = {
                    "SPONSOR_DFE_NAME": "Plan Sponsor",
                    actuary_name_col: "Actuary Name" if actuary_name_col else None,
                    actuary_city_col: "Actuary City" if actuary_city_col else None,
                    actuary_state_col: "Actuary State" if actuary_state_col else None,
                    "ACTIVE_COUNT": "Active",
                    "RETIREE_COUNT": "Retirees",
                    "SEPARATED_COUNT": "Terminated",
                    "TOTAL_PARTICIPANTS": "Total Participants",
                    "TOTAL_LIABILITY": "Total Liability"
                }
                rename_map = {k: v for k, v in rename_map.items() if k and v}
                
                display_df = firm_data[display_cols].copy()
                display_df = display_df.rename(columns=rename_map)
                
                st.dataframe(display_df, use_container_width=True)
                st.download_button(
                    "Download Plans for This Firm",
                    display_df.to_csv(index=False),
                    file_name=f"plans_{selected_firm.replace(' ', '_')[:30]}.csv"
                )
                
                # Summary stats for this firm
                st.markdown("#### Firm Summary Statistics")
                sum_cols = st.columns(4)
                if "TOTAL_PARTICIPANTS" in firm_data.columns:
                    sum_cols[0].metric("Total Participants", f"{firm_data['TOTAL_PARTICIPANTS'].sum():,.0f}")
                if "RETIREE_COUNT" in firm_data.columns:
                    sum_cols[1].metric("Total Retirees", f"{firm_data['RETIREE_COUNT'].sum():,.0f}")
                if "TOTAL_LIABILITY" in firm_data.columns:
                    sum_cols[2].metric("Total Liability", f"${firm_data['TOTAL_LIABILITY'].sum():,.0f}")
                sum_cols[3].metric("Number of Plans", len(firm_data))
            else:
                st.warning("No firms found matching your search.")
        
        with tab2:
            st.subheader("Actuarial Firm Rankings")
            
            # Build aggregation dictionary - use EIN to count plans, not the groupby column
            agg_dict = {'EIN': 'count'}  # Count plans by counting EINs
            if "TOTAL_PARTICIPANTS" in db_firms.columns:
                agg_dict["TOTAL_PARTICIPANTS"] = "sum"
            if "RETIREE_COUNT" in db_firms.columns:
                agg_dict["RETIREE_COUNT"] = "sum"
            if "TOTAL_LIABILITY" in db_firms.columns:
                agg_dict["TOTAL_LIABILITY"] = "sum"
            
            # Aggregate by firm
            firm_stats = db_firms.groupby(actuary_firm_col).agg(agg_dict).reset_index()
            firm_stats = firm_stats.rename(columns={
                actuary_firm_col: "Actuarial Firm",
                'EIN': "Plan Count",
                'TOTAL_PARTICIPANTS': "Total Participants",
                'RETIREE_COUNT': "Total Retirees",
                'TOTAL_LIABILITY': "Total Liability ($)"
            })
            
            # Sort options
            sort_by = st.selectbox(
                "Sort firms by:",
                ["Plan Count", "Total Participants", "Total Retirees", "Total Liability ($)"],
                index=0,
                key="firm_sort"
            )
            
            if sort_by in firm_stats.columns:
                firm_stats = firm_stats.sort_values(sort_by, ascending=False)
            
            top_n = st.slider("Show top N firms", 10, 100, 25, key="firm_top_n")
            
            st.dataframe(firm_stats.head(top_n), use_container_width=True)
            st.download_button(
                "Download Firm Rankings",
                firm_stats.to_csv(index=False),
                file_name="actuarial_firm_rankings.csv"
            )
    else:
        st.warning("Actuarial firm data (ACTUARY_FIRM_NAME) not found in this dataset. Please re-run the data pipeline to include this field.")
        st.info("The ACTUARY_FIRM_NAME field comes from Schedule SB and needs to be included in the data normalization process.")

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
        sponsor_col = next((c for c in ["SPONSOR_DFE_NAME", "SPONSOR_NAME"] if c in db.columns), None)
        sponsor_filter = st.text_input("Filter by Plan Sponsor Name (partial)")
    filtered = db.copy()
    if ein_filter:
        filtered = filtered[filtered["EIN"].astype(str).str.contains(ein_filter, case=False, na=False)]
    if sponsor_filter and sponsor_col:
        filtered = filtered[filtered[sponsor_col].astype(str).str.contains(sponsor_filter, case=False, na=False)]
    st.write(f"Showing {len(filtered)} plans.")
    # Determine sponsor and plan name columns
    sponsor_col = next((c for c in ["SPONSOR_DFE_NAME", "SPONSOR_NAME"] if c in filtered.columns), None)
    plan_name_col = next((c for c in ["PLAN_NAME"] if c in filtered.columns), None)
    # Build display columns: always show sponsor and plan name if available
    display_cols = []
    if sponsor_col:
        display_cols.append(sponsor_col)
    if plan_name_col:
        display_cols.append(plan_name_col)
    # Add the rest of the columns (avoid duplicates)
    display_cols += [col for col in filtered.columns if col not in display_cols]
    st.dataframe(filtered[display_cols], use_container_width=True)
    st.download_button("Download Filtered Data", filtered[display_cols].to_csv(index=False), file_name="filtered_plans.csv")

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
