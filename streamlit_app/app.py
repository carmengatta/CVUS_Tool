
import streamlit as st
import pandas as pd
import os
import re
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.normalize_firm_names import normalize_firm_name
from utils.naics_codes import get_naics_sector, get_naics_description

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
    ["Dashboard", "Substitute Mortality", "Industry Explorer", "PRT Analysis", "PRT History", "Actuarial Firms", "Data Explorer", "About", "Logout"],
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
# SUBSTITUTE MORTALITY PAGE
# =============================
elif menu == "Substitute Mortality":
    st.title(f"Substitute Mortality Analysis — {selected_year}")
    st.caption("Analysis of mortality table codes from Schedule SB filings.")
    st.markdown("""
    **Mortality Table Codes (SB_MORTALITY_TBL_CD):**
    - **Code 1 — Prescribed Combined:** Uses the IRS-prescribed unisex mortality table (male and female blended) with the applicable improvement scale.
    - **Code 2 — Prescribed Separate:** Uses the IRS-prescribed mortality tables with separate male and female rates, projected with the applicable improvement scale.
    - **Code 3 — Substitute:** Uses a plan-specific, credibility-based mortality table approved by the IRS in lieu of the standard prescribed tables.
    """)
    st.markdown("---")
    
    # Identify the mortality code column
    mortality_col = next((c for c in ["MORTALITY_CODE", "SB_MORTALITY_TBL_CD"] if c in db.columns), None)
    
    if mortality_col and mortality_col in db.columns:
        # Clean up mortality codes
        db_mort = db.copy()
        db_mort[mortality_col] = pd.to_numeric(db_mort[mortality_col], errors='coerce')
        
        # Filter to valid codes (1, 2, 3)
        db_mort_valid = db_mort[db_mort[mortality_col].isin([1, 2, 3])].copy()
        
        # Define labels for the codes
        code_labels = {
            1: "Prescribed Combined",
            2: "Prescribed Separate",
            3: "Substitute"
        }
        db_mort_valid["Mortality Type"] = db_mort_valid[mortality_col].map(code_labels)
        
        # --- KPIs ---
        total_with_code = len(db_mort_valid)
        code_1_count = len(db_mort_valid[db_mort_valid[mortality_col] == 1])
        code_2_count = len(db_mort_valid[db_mort_valid[mortality_col] == 2])
        code_3_count = len(db_mort_valid[db_mort_valid[mortality_col] == 3])
        
        kpi_cols = st.columns(4)
        kpi_cols[0].metric("Plans with Mortality Data", f"{total_with_code:,}")
        kpi_cols[1].metric("Prescribed Combined (1)", f"{code_1_count:,}")
        kpi_cols[2].metric("Prescribed Separate (2)", f"{code_2_count:,}")
        kpi_cols[3].metric("Substitute (3)", f"{code_3_count:,}")
        
        st.markdown("---")
        
        # Percentage breakdown
        if total_with_code > 0:
            pct_1 = (code_1_count / total_with_code) * 100
            pct_2 = (code_2_count / total_with_code) * 100
            pct_3 = (code_3_count / total_with_code) * 100
            
            st.subheader("Distribution by Plan Count")
            col1, col2, col3 = st.columns(3)
            col1.metric("Prescribed Combined %", f"{pct_1:.1f}%")
            col2.metric("Prescribed Separate %", f"{pct_2:.1f}%")
            col3.metric("Substitute %", f"{pct_3:.1f}%")
            
            # Aggregate by retirees and liability for substitute mortality
            retiree_col = "RETIREE_COUNT" if "RETIREE_COUNT" in db_mort_valid.columns else None
            liability_col = next((c for c in ["TOTAL_LIABILITY", "LIABILITY_TOTAL"] if c in db_mort_valid.columns), None)
            
            if retiree_col or liability_col:
                st.markdown("---")
                st.subheader("Substitute Mortality Impact (Code 3)")
                
                substitute_plans = db_mort_valid[db_mort_valid[mortality_col] == 3]
                prescribed_plans = db_mort_valid[db_mort_valid[mortality_col] == 2]
                
                impact_cols = st.columns(4)
                
                if retiree_col:
                    sub_retirees = substitute_plans[retiree_col].sum()
                    presc_retirees = prescribed_plans[retiree_col].sum()
                    total_retirees = db_mort_valid[retiree_col].sum()
                    pct_retirees_sub = (sub_retirees / total_retirees * 100) if total_retirees > 0 else 0
                    impact_cols[0].metric("Retirees (Substitute)", f"{sub_retirees:,.0f}")
                    impact_cols[1].metric("% of All Retirees", f"{pct_retirees_sub:.1f}%")
                
                if liability_col:
                    sub_liability = substitute_plans[liability_col].sum()
                    presc_liability = prescribed_plans[liability_col].sum()
                    total_liability = db_mort_valid[liability_col].sum()
                    pct_liability_sub = (sub_liability / total_liability * 100) if total_liability > 0 else 0
                    impact_cols[2].metric("Liability (Substitute)", f"${sub_liability:,.0f}")
                    impact_cols[3].metric("% of Total Liability", f"{pct_liability_sub:.1f}%")
        
        # Add industry classification to the data
        if "BUSINESS_CODE" in db_mort_valid.columns:
            db_mort_valid["INDUSTRY_SECTOR"] = db_mort_valid["BUSINESS_CODE"].astype(str).apply(get_naics_sector)
            db_mort_valid["INDUSTRY_NAME"] = db_mort_valid["BUSINESS_CODE"].astype(str).apply(get_naics_description)
        
        st.markdown("---")
        
        # Tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs(["Substitute Mortality Plans", "By Industry", "By Actuarial Firm", "Summary Table"])
        
        with tab1:
            st.subheader("Plans Using Substitute Mortality (Code 3)")
            
            substitute_plans = db_mort_valid[db_mort_valid[mortality_col] == 3].copy()
            
            if len(substitute_plans) > 0:
                # Build display columns
                display_cols = ["EIN", "PLAN_NAME"]
                sponsor_col = next((c for c in ["SPONSOR_DFE_NAME", "SPONSOR_NAME"] if c in substitute_plans.columns), None)
                if sponsor_col:
                    display_cols.insert(1, sponsor_col)
                
                actuary_firm_col = next((c for c in ["ACTUARY_FIRM_NAME", "SB_ACTUARY_FIRM_NAME"] if c in substitute_plans.columns), None)
                if actuary_firm_col:
                    display_cols.append(actuary_firm_col)
                
                for col in ["ACTIVE_COUNT", "RETIREE_COUNT", "SEPARATED_COUNT", "TOTAL_PARTICIPANTS"]:
                    if col in substitute_plans.columns:
                        display_cols.append(col)
                
                if liability_col:
                    display_cols.append(liability_col)
                
                # Filter to existing columns
                display_cols = [c for c in display_cols if c in substitute_plans.columns]
                
                # Sort options
                sort_by = st.selectbox(
                    "Sort by:",
                    ["RETIREE_COUNT", "TOTAL_PARTICIPANTS", liability_col, "PLAN_NAME"] if liability_col else ["RETIREE_COUNT", "TOTAL_PARTICIPANTS", "PLAN_NAME"],
                    index=0,
                    key="sub_mort_sort"
                )
                
                if sort_by and sort_by in substitute_plans.columns:
                    substitute_plans = substitute_plans.sort_values(sort_by, ascending=False)
                
                top_n = st.slider("Show top N plans", 10, 200, 50, key="sub_mort_top_n")
                
                # Rename for display
                rename_map = {
                    "SPONSOR_DFE_NAME": "Plan Sponsor",
                    "ACTUARY_FIRM_NAME": "Actuarial Firm",
                    "SB_ACTUARY_FIRM_NAME": "Actuarial Firm",
                    "ACTIVE_COUNT": "Active",
                    "RETIREE_COUNT": "Retirees",
                    "SEPARATED_COUNT": "Terminated",
                    "TOTAL_PARTICIPANTS": "Total Participants",
                    "TOTAL_LIABILITY": "Total Liability",
                    "LIABILITY_TOTAL": "Total Liability"
                }
                
                display_df = substitute_plans[display_cols].head(top_n).copy()
                display_df = display_df.rename(columns={k: v for k, v in rename_map.items() if k in display_df.columns})
                
                st.write(f"**{len(substitute_plans):,} plans** use substitute mortality tables.")
                st.dataframe(display_df, use_container_width=True)
                st.download_button(
                    "Download Substitute Mortality Plans",
                    substitute_plans[display_cols].to_csv(index=False),
                    file_name=f"substitute_mortality_plans_{selected_year}.csv"
                )
            else:
                st.info("No plans with substitute mortality (Code 3) found for this year.")
        
        with tab2:
            st.subheader("Substitute Mortality by Industry")
            
            if "INDUSTRY_SECTOR" in db_mort_valid.columns:
                substitute_plans = db_mort_valid[db_mort_valid[mortality_col] == 3].copy()
                
                if len(substitute_plans) > 0:
                    # --- By Sector ---
                    st.markdown("#### By Industry Sector")
                    agg_dict_sector = {"EIN": "count"}
                    if retiree_col:
                        agg_dict_sector[retiree_col] = "sum"
                    if liability_col:
                        agg_dict_sector[liability_col] = "sum"
                    
                    sector_summary = substitute_plans.groupby("INDUSTRY_SECTOR").agg(agg_dict_sector).reset_index()
                    sector_summary = sector_summary.rename(columns={
                        "INDUSTRY_SECTOR": "Industry Sector",
                        "EIN": "# Plans",
                        retiree_col: "Total Retirees" if retiree_col else None,
                        liability_col: "Total Liability ($)" if liability_col else None
                    })
                    sector_summary = sector_summary.sort_values("# Plans", ascending=False)
                    
                    st.dataframe(sector_summary, use_container_width=True)
                    
                    # --- By Detailed Industry ---
                    st.markdown("---")
                    st.markdown("#### By Detailed Industry (NAICS)")
                    
                    agg_dict_ind = {"EIN": "count"}
                    if retiree_col:
                        agg_dict_ind[retiree_col] = "sum"
                    if liability_col:
                        agg_dict_ind[liability_col] = "sum"
                    
                    industry_summary = substitute_plans.groupby(["BUSINESS_CODE", "INDUSTRY_NAME"]).agg(agg_dict_ind).reset_index()
                    industry_summary = industry_summary.rename(columns={
                        "BUSINESS_CODE": "NAICS Code",
                        "INDUSTRY_NAME": "Industry",
                        "EIN": "# Plans",
                        retiree_col: "Total Retirees" if retiree_col else None,
                        liability_col: "Total Liability ($)" if liability_col else None
                    })
                    industry_summary = industry_summary.sort_values("# Plans", ascending=False)
                    
                    st.dataframe(industry_summary.head(25), use_container_width=True)
                    st.download_button(
                        "Download Industry Analysis",
                        industry_summary.to_csv(index=False),
                        file_name=f"substitute_mortality_by_industry_{selected_year}.csv"
                    )
                    
                    # --- Compare to overall population ---
                    st.markdown("---")
                    st.markdown("#### Substitute Mortality Rate by Sector")
                    st.caption("Percentage of plans in each sector using substitute mortality vs prescribed tables")
                    
                    # Get all plans with valid mortality code by sector
                    all_by_sector = db_mort_valid.groupby("INDUSTRY_SECTOR").agg({"EIN": "count"}).reset_index()
                    all_by_sector = all_by_sector.rename(columns={"EIN": "Total Plans"})
                    
                    # Get substitute plans by sector
                    sub_by_sector = substitute_plans.groupby("INDUSTRY_SECTOR").agg({"EIN": "count"}).reset_index()
                    sub_by_sector = sub_by_sector.rename(columns={"EIN": "Substitute Plans"})
                    
                    # Merge
                    comparison = all_by_sector.merge(sub_by_sector, on="INDUSTRY_SECTOR", how="left")
                    comparison["Substitute Plans"] = comparison["Substitute Plans"].fillna(0).astype(int)
                    comparison["% Using Substitute"] = (comparison["Substitute Plans"] / comparison["Total Plans"] * 100).round(1)
                    comparison = comparison.rename(columns={"INDUSTRY_SECTOR": "Industry Sector"})
                    comparison = comparison.sort_values("% Using Substitute", ascending=False)
                    
                    st.dataframe(comparison, use_container_width=True)
                else:
                    st.info("No plans with substitute mortality (Code 3) found for this year.")
            else:
                st.warning("BUSINESS_CODE column not found in the dataset.")
        
        with tab3:
            st.subheader("Substitute Mortality by Actuarial Firm")
            
            actuary_firm_col = next((c for c in ["ACTUARY_FIRM_NAME", "SB_ACTUARY_FIRM_NAME"] if c in db_mort_valid.columns), None)
            
            if actuary_firm_col:
                substitute_plans = db_mort_valid[db_mort_valid[mortality_col] == 3].copy()
                substitute_plans[actuary_firm_col] = substitute_plans[actuary_firm_col].fillna("Unknown").astype(str).str.strip()
                substitute_plans = substitute_plans[substitute_plans[actuary_firm_col] != ""]
                
                if len(substitute_plans) > 0:
                    # Aggregate by firm
                    agg_dict = {"EIN": "count"}
                    if retiree_col:
                        agg_dict[retiree_col] = "sum"
                    if liability_col:
                        agg_dict[liability_col] = "sum"
                    
                    firm_summary = substitute_plans.groupby(actuary_firm_col).agg(agg_dict).reset_index()
                    firm_summary = firm_summary.rename(columns={
                        actuary_firm_col: "Actuarial Firm",
                        "EIN": "# Plans (Substitute)",
                        retiree_col: "Total Retirees" if retiree_col else None,
                        liability_col: "Total Liability ($)" if liability_col else None
                    })
                    firm_summary = firm_summary.sort_values("# Plans (Substitute)", ascending=False)
                    
                    st.write(f"**{len(firm_summary):,} actuarial firms** have clients using substitute mortality.")
                    st.dataframe(firm_summary, use_container_width=True)
                    st.download_button(
                        "Download Firm Summary",
                        firm_summary.to_csv(index=False),
                        file_name=f"substitute_mortality_by_firm_{selected_year}.csv"
                    )
                else:
                    st.info("No substitute mortality plans with actuarial firm data.")
            else:
                st.warning("Actuarial firm column not found in the dataset.")
        
        with tab4:
            st.subheader("Summary by Mortality Code")
            
            # Build summary table
            summary_data = []
            for code, label in code_labels.items():
                code_df = db_mort_valid[db_mort_valid[mortality_col] == code]
                row = {
                    "Code": code,
                    "Description": label,
                    "# Plans": len(code_df),
                    "% of Plans": f"{len(code_df) / total_with_code * 100:.1f}%" if total_with_code > 0 else "N/A"
                }
                if retiree_col:
                    row["Total Retirees"] = code_df[retiree_col].sum()
                if liability_col:
                    row["Total Liability"] = code_df[liability_col].sum()
                summary_data.append(row)
            
            summary_df = pd.DataFrame(summary_data)
            st.dataframe(summary_df, use_container_width=True)
            st.download_button(
                "Download Summary",
                summary_df.to_csv(index=False),
                file_name=f"mortality_code_summary_{selected_year}.csv"
            )
            
            # Show plans without mortality code
            missing_code = db[~db.index.isin(db_mort_valid.index)]
            st.markdown("---")
            st.write(f"**{len(missing_code):,} plans** have missing or invalid mortality code data.")
    else:
        st.warning("Mortality code column (MORTALITY_CODE or SB_MORTALITY_TBL_CD) not found in this dataset.")
        st.info("Ensure the data pipeline includes the SB_MORTALITY_TBL_CD field from Schedule SB filings.")

# =============================
# INDUSTRY EXPLORER PAGE
# =============================
elif menu == "Industry Explorer":
    st.title(f"Industry Explorer — {selected_year}")
    st.caption("Explore DB plans by industry sector, NAICS code, and mortality table usage.")
    st.markdown("---")
    
    # Add industry classification to data
    if "BUSINESS_CODE" in db.columns:
        db_ind = db.copy()
        db_ind["INDUSTRY_SECTOR"] = db_ind["BUSINESS_CODE"].astype(str).apply(get_naics_sector)
        db_ind["INDUSTRY_NAME"] = db_ind["BUSINESS_CODE"].astype(str).apply(get_naics_description)
        db_ind["MORTALITY_CODE"] = pd.to_numeric(db_ind.get("MORTALITY_CODE", pd.Series()), errors='coerce')
        
        # Mortality code labels
        mortality_labels = {1: "Prescribed Combined", 2: "Prescribed Separate", 3: "Substitute"}
        db_ind["MORTALITY_TYPE"] = db_ind["MORTALITY_CODE"].map(mortality_labels).fillna("Unknown")
        
        # --- Filters in sidebar ---
        st.sidebar.markdown("## Industry Filters")
        
        # Sector filter
        all_sectors = sorted(db_ind["INDUSTRY_SECTOR"].dropna().unique())
        selected_sectors = st.sidebar.multiselect(
            "Industry Sector(s)",
            options=all_sectors,
            default=[],
            help="Filter by broad industry sector"
        )
        
        # Mortality code filter
        mortality_options = ["All", "Prescribed Combined (1)", "Prescribed Separate (2)", "Substitute (3)", "Not Substitute (1 or 2)"]
        selected_mortality = st.sidebar.selectbox(
            "Mortality Table Type",
            options=mortality_options,
            index=0,
            help="Filter by mortality table usage"
        )
        
        # Apply filters
        filtered = db_ind.copy()
        
        if selected_sectors:
            filtered = filtered[filtered["INDUSTRY_SECTOR"].isin(selected_sectors)]
        
        if selected_mortality == "Prescribed Combined (1)":
            filtered = filtered[filtered["MORTALITY_CODE"] == 1]
        elif selected_mortality == "Prescribed Separate (2)":
            filtered = filtered[filtered["MORTALITY_CODE"] == 2]
        elif selected_mortality == "Substitute (3)":
            filtered = filtered[filtered["MORTALITY_CODE"] == 3]
        elif selected_mortality == "Not Substitute (1 or 2)":
            filtered = filtered[filtered["MORTALITY_CODE"].isin([1, 2])]
        
        # --- KPIs ---
        kpi_cols = st.columns(4)
        kpi_cols[0].metric("Plans", f"{len(filtered):,}")
        
        retiree_col = "RETIREE_COUNT" if "RETIREE_COUNT" in filtered.columns else None
        if retiree_col:
            kpi_cols[1].metric("Total Retirees", f"{filtered[retiree_col].sum():,.0f}")
        
        liability_col = next((c for c in ["TOTAL_LIABILITY", "LIABILITY_TOTAL"] if c in filtered.columns), None)
        if liability_col:
            kpi_cols[2].metric("Total Liability", f"${filtered[liability_col].sum():,.0f}")
        
        participant_col = "TOTAL_PARTICIPANTS" if "TOTAL_PARTICIPANTS" in filtered.columns else None
        if participant_col:
            kpi_cols[3].metric("Total Participants", f"{filtered[participant_col].sum():,.0f}")
        
        st.markdown("---")
        
        # Tabs for different views
        tab1, tab2, tab3 = st.tabs(["Plan List", "By Industry Sector", "By Actuarial Firm"])
        
        with tab1:
            st.subheader("Filtered Plan List")
            
            # Build display columns
            display_cols = ["EIN", "SPONSOR_DFE_NAME", "PLAN_NAME", "INDUSTRY_SECTOR", "INDUSTRY_NAME", "MORTALITY_TYPE"]
            
            actuary_col = next((c for c in ["ACTUARY_FIRM_NAME", "SB_ACTUARY_FIRM_NAME"] if c in filtered.columns), None)
            if actuary_col:
                display_cols.append(actuary_col)
            
            for col in ["ACTIVE_COUNT", "RETIREE_COUNT", "TOTAL_PARTICIPANTS"]:
                if col in filtered.columns:
                    display_cols.append(col)
            
            if liability_col:
                display_cols.append(liability_col)
            
            display_cols = [c for c in display_cols if c in filtered.columns]
            
            # Sort options
            sort_options = ["RETIREE_COUNT", "TOTAL_PARTICIPANTS", "TOTAL_LIABILITY", "SPONSOR_DFE_NAME"]
            sort_options = [s for s in sort_options if s in filtered.columns]
            sort_by = st.selectbox("Sort by:", sort_options, index=0, key="ind_exp_sort")
            
            if sort_by in filtered.columns:
                filtered_sorted = filtered.sort_values(sort_by, ascending=False)
            else:
                filtered_sorted = filtered
            
            top_n = st.slider("Show top N plans", 25, 500, 100, key="ind_exp_top_n")
            
            # Rename for display
            rename_map = {
                "SPONSOR_DFE_NAME": "Plan Sponsor",
                "INDUSTRY_SECTOR": "Sector",
                "INDUSTRY_NAME": "Industry",
                "MORTALITY_TYPE": "Mortality",
                "ACTUARY_FIRM_NAME": "Actuarial Firm",
                "SB_ACTUARY_FIRM_NAME": "Actuarial Firm",
                "ACTIVE_COUNT": "Active",
                "RETIREE_COUNT": "Retirees",
                "TOTAL_PARTICIPANTS": "Total",
                "TOTAL_LIABILITY": "Liability",
                "LIABILITY_TOTAL": "Liability"
            }
            
            display_df = filtered_sorted[display_cols].head(top_n).copy()
            display_df = display_df.rename(columns={k: v for k, v in rename_map.items() if k in display_df.columns})
            
            st.write(f"Showing **{min(top_n, len(filtered)):,}** of **{len(filtered):,}** plans")
            st.dataframe(display_df, use_container_width=True)
            
            # Download button
            st.download_button(
                "Download Filtered Plans (CSV)",
                filtered_sorted[display_cols].to_csv(index=False),
                file_name=f"industry_filtered_plans_{selected_year}.csv"
            )
        
        with tab2:
            st.subheader("Summary by Industry Sector")
            
            agg_dict = {"EIN": "count"}
            if retiree_col:
                agg_dict[retiree_col] = "sum"
            if liability_col:
                agg_dict[liability_col] = "sum"
            
            sector_summary = filtered.groupby("INDUSTRY_SECTOR").agg(agg_dict).reset_index()
            sector_summary = sector_summary.rename(columns={
                "INDUSTRY_SECTOR": "Industry Sector",
                "EIN": "# Plans",
                retiree_col: "Total Retirees" if retiree_col else None,
                liability_col: "Total Liability" if liability_col else None
            })
            sector_summary = sector_summary.sort_values("# Plans", ascending=False)
            
            st.dataframe(sector_summary, use_container_width=True)
            
            # Show mortality breakdown by sector if filtering by sector
            if selected_sectors:
                st.markdown("---")
                st.subheader("Mortality Code Breakdown")
                
                mort_by_sector = filtered.groupby(["INDUSTRY_SECTOR", "MORTALITY_TYPE"]).agg({"EIN": "count"}).reset_index()
                mort_by_sector = mort_by_sector.rename(columns={"EIN": "# Plans"})
                mort_pivot = mort_by_sector.pivot(index="INDUSTRY_SECTOR", columns="MORTALITY_TYPE", values="# Plans").fillna(0)
                st.dataframe(mort_pivot, use_container_width=True)
        
        with tab3:
            st.subheader("By Actuarial Firm")
            
            if actuary_col:
                # Normalize firm names
                filtered["NORMALIZED_FIRM"] = filtered[actuary_col].apply(normalize_firm_name)
                
                agg_dict = {"EIN": "count"}
                if retiree_col:
                    agg_dict[retiree_col] = "sum"
                if liability_col:
                    agg_dict[liability_col] = "sum"
                
                firm_summary = filtered.groupby("NORMALIZED_FIRM").agg(agg_dict).reset_index()
                firm_summary = firm_summary.rename(columns={
                    "NORMALIZED_FIRM": "Actuarial Firm",
                    "EIN": "# Plans",
                    retiree_col: "Total Retirees" if retiree_col else None,
                    liability_col: "Total Liability" if liability_col else None
                })
                firm_summary = firm_summary.sort_values("Total Retirees" if retiree_col else "# Plans", ascending=False)
                
                st.dataframe(firm_summary.head(30), use_container_width=True)
                st.download_button(
                    "Download Firm Summary",
                    firm_summary.to_csv(index=False),
                    file_name=f"industry_by_firm_{selected_year}.csv"
                )
            else:
                st.warning("Actuarial firm data not available.")
    else:
        st.warning("BUSINESS_CODE column not found in the dataset.")

# =============================
# PRT ANALYSIS PAGE
# =============================
elif menu == "PRT Analysis":
    st.title(f"Pension Risk Transfer Analysis — {selected_year}")
    st.caption("Analyze PRT transactions and identify plans ripe for pension risk transfer.")
    st.markdown("---")
    
    # Check if Schedule H data is available
    has_prt_data = 'SCH_H_PRT_AMOUNT' in db.columns
    
    if not has_prt_data:
        st.warning("""
        ⚠️ **Schedule H data not yet loaded for this year.**
        
        To enable PRT analysis, re-run the data pipeline with Schedule H integration:
        
        ```python
        from data_ingestion.multi_year_ingestion import run_multi_year_pipeline
        run_multi_year_pipeline()
        ```
        
        This will merge Schedule H financial data (including PRT transactions and asset sizes) with the existing plan data.
        """)
        st.stop()
    
    # PRT Tabs
    prt_tab1, prt_tab2, prt_tab3, prt_tab4, prt_tab5 = st.tabs([
        "📊 Overview", "💰 PRT Transactions", "🏭 By Industry", "🎯 PRT Opportunities", "📈 Asset Analysis"
    ])
    
    # === TAB 1: OVERVIEW ===
    with prt_tab1:
        st.subheader("PRT Market Overview")
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        prt_col = 'SCH_H_PRT_AMOUNT'
        assets_col = 'SCH_H_TOTAL_ASSETS_EOY'
        
        # PRT transactions
        prt_mask = db[prt_col].fillna(0) > 0
        prt_plans = prt_mask.sum()
        total_prt = db[prt_col].fillna(0).sum()
        
        # Assets
        total_assets = db[assets_col].fillna(0).sum()
        avg_assets = db[assets_col].fillna(0).mean()
        
        col1.metric("Plans with PRT Activity", f"{prt_plans:,}")
        col2.metric("Total PRT Volume", f"${total_prt/1e9:,.2f}B")
        col3.metric("Total Plan Assets", f"${total_assets/1e12:,.2f}T")
        col4.metric("Avg Plan Assets", f"${avg_assets/1e6:,.1f}M")
        
        st.markdown("---")
        
        # PRT Category distribution
        st.subheader("PRT Transaction Size Distribution")
        if 'PRT_CATEGORY' in db.columns:
            prt_cat = db[db[prt_col].fillna(0) > 0]['PRT_CATEGORY'].value_counts()
            
            # Order categories
            cat_order = ['Small (<$10M)', 'Medium ($10M-$100M)', 'Large ($100M-$500M)', 'Mega (>$500M)']
            prt_cat = prt_cat.reindex([c for c in cat_order if c in prt_cat.index])
            
            col1, col2 = st.columns(2)
            with col1:
                st.bar_chart(prt_cat)
            with col2:
                st.dataframe(prt_cat.reset_index().rename(columns={'index': 'Category', 'PRT_CATEGORY': 'Plan Count'}))
        
        # Asset size distribution
        st.subheader("Asset Size Distribution")
        if 'ASSET_SIZE_CATEGORY' in db.columns:
            asset_dist = db['ASSET_SIZE_CATEGORY'].value_counts()
            
            # Order categories
            asset_order = ['Small (<$10M)', 'Medium ($10M-$100M)', 'Large ($100M-$500M)', 
                          'Very Large ($500M-$1B)', 'Mega (>$1B)', 'Unknown']
            asset_dist = asset_dist.reindex([c for c in asset_order if c in asset_dist.index])
            
            col1, col2 = st.columns(2)
            with col1:
                st.bar_chart(asset_dist)
            with col2:
                # Add total assets by category
                asset_by_cat = db.groupby('ASSET_SIZE_CATEGORY')[assets_col].sum().reindex(
                    [c for c in asset_order if c in asset_dist.index]
                )
                summary_df = pd.DataFrame({
                    'Plan Count': asset_dist,
                    'Total Assets': asset_by_cat.apply(lambda x: f"${x/1e9:,.2f}B")
                })
                st.dataframe(summary_df)
    
    # === TAB 2: PRT TRANSACTIONS ===
    with prt_tab2:
        st.subheader("PRT Transactions")
        
        # Filter to plans with PRT
        prt_df = db[db[prt_col].fillna(0) > 0].copy()
        
        if len(prt_df) == 0:
            st.info("No PRT transactions found in this year's data.")
        else:
            # Size filter
            min_prt = st.slider(
                "Minimum PRT Amount ($ millions)", 
                min_value=0, 
                max_value=1000, 
                value=0,
                step=10
            )
            prt_df = prt_df[prt_df[prt_col] >= min_prt * 1e6]
            
            st.write(f"**{len(prt_df):,} plans** with PRT transactions ≥ ${min_prt}M")
            
            # Display columns
            display_cols = ['SPONSOR_DFE_NAME', 'EIN', prt_col, assets_col]
            if 'INDUSTRY_SECTOR' in db.columns:
                display_cols.insert(1, 'INDUSTRY_SECTOR')
            if 'ACTUARY_FIRM_NAME' in db.columns:
                display_cols.append('ACTUARY_FIRM_NAME')
            if 'PRT_PCT_OF_ASSETS' in db.columns:
                display_cols.append('SCH_H_PRT_PCT_OF_ASSETS')
            
            available_cols = [c for c in display_cols if c in prt_df.columns]
            
            # Sort by PRT amount
            prt_display = prt_df[available_cols].sort_values(prt_col, ascending=False)
            
            # Format currency columns
            format_df = prt_display.copy()
            format_df[prt_col] = format_df[prt_col].apply(lambda x: f"${x/1e6:,.1f}M" if pd.notna(x) else "N/A")
            format_df[assets_col] = format_df[assets_col].apply(lambda x: f"${x/1e6:,.1f}M" if pd.notna(x) else "N/A")
            
            st.dataframe(format_df.head(100), use_container_width=True)
            
            # Download
            st.download_button(
                "📥 Download PRT Transactions CSV",
                prt_display.to_csv(index=False),
                file_name=f"prt_transactions_{selected_year}.csv"
            )
    
    # === TAB 3: BY INDUSTRY ===
    with prt_tab3:
        st.subheader("PRT Activity by Industry")
        
        if 'BUSINESS_CODE' in db.columns:
            # Add industry sector
            db_industry = db.copy()
            db_industry['INDUSTRY_SECTOR'] = db_industry['BUSINESS_CODE'].apply(get_naics_sector)
            
            # Aggregate by industry
            industry_prt = db_industry.groupby('INDUSTRY_SECTOR').agg({
                prt_col: ['sum', lambda x: (x.fillna(0) > 0).sum()],
                assets_col: 'sum',
                'EIN': 'count'
            }).round(0)
            
            industry_prt.columns = ['Total PRT ($)', 'PRT Plans', 'Total Assets ($)', 'Total Plans']
            industry_prt['PRT Rate (%)'] = (industry_prt['PRT Plans'] / industry_prt['Total Plans'] * 100).round(1)
            industry_prt = industry_prt.sort_values('Total PRT ($)', ascending=False)
            
            # Display with formatting
            display_ind = industry_prt.copy()
            display_ind['Total PRT ($)'] = display_ind['Total PRT ($)'].apply(lambda x: f"${x/1e9:,.2f}B")
            display_ind['Total Assets ($)'] = display_ind['Total Assets ($)'].apply(lambda x: f"${x/1e9:,.2f}B")
            
            st.dataframe(display_ind, use_container_width=True)
            
            # Chart
            st.subheader("PRT Volume by Industry")
            chart_data = industry_prt['Total PRT ($)'].head(10) / 1e9
            st.bar_chart(chart_data)
        else:
            st.warning("Industry data not available.")
    
    # === TAB 4: PRT OPPORTUNITIES ===
    with prt_tab4:
        st.subheader("PRT Opportunity Identification")
        st.caption("Plans with high PRT readiness scores that haven't done a transfer yet.")
        
        if 'PRT_READINESS_SCORE' in db.columns:
            # Plans without PRT but high readiness
            no_prt = db[db[prt_col].fillna(0) == 0].copy()
            
            # Filters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                min_score = st.slider("Minimum PRT Readiness Score", 0, 100, 30, step=5)
            
            with col2:
                min_assets = st.selectbox(
                    "Minimum Asset Size",
                    ["No minimum", "$10M+", "$50M+", "$100M+", "$500M+", "$1B+"]
                )
                asset_threshold = {
                    "No minimum": 0,
                    "$10M+": 10_000_000,
                    "$50M+": 50_000_000,
                    "$100M+": 100_000_000,
                    "$500M+": 500_000_000,
                    "$1B+": 1_000_000_000
                }[min_assets]
            
            with col3:
                industry_filter = st.selectbox(
                    "Industry Sector",
                    ["All Industries"] + sorted(db['INDUSTRY_SECTOR'].dropna().unique().tolist()) if 'INDUSTRY_SECTOR' in db.columns else ["All Industries"]
                )
            
            # Apply filters
            candidates = no_prt[no_prt['PRT_READINESS_SCORE'] >= min_score]
            candidates = candidates[candidates[assets_col].fillna(0) >= asset_threshold]
            
            if industry_filter != "All Industries" and 'INDUSTRY_SECTOR' in candidates.columns:
                candidates = candidates[candidates['INDUSTRY_SECTOR'] == industry_filter]
            
            # Sort by readiness score
            candidates = candidates.sort_values('PRT_READINESS_SCORE', ascending=False)
            
            st.write(f"**{len(candidates):,} potential PRT opportunities** matching your criteria")
            
            # Display columns
            display_cols = ['SPONSOR_DFE_NAME', 'PRT_READINESS_SCORE', assets_col]
            if 'INDUSTRY_SECTOR' in candidates.columns:
                display_cols.insert(1, 'INDUSTRY_SECTOR')
            if 'RETIREE_PCT' in candidates.columns:
                display_cols.append('RETIREE_PCT')
            if 'FUNDING_TARGET_PCT' in candidates.columns:
                display_cols.append('FUNDING_TARGET_PCT')
            if 'MORTALITY_CODE' in candidates.columns:
                display_cols.append('MORTALITY_CODE')
            if 'ACTUARY_FIRM_NAME' in candidates.columns:
                display_cols.append('ACTUARY_FIRM_NAME')
            
            available_cols = [c for c in display_cols if c in candidates.columns]
            
            cand_display = candidates[available_cols].copy()
            cand_display[assets_col] = cand_display[assets_col].apply(lambda x: f"${x/1e6:,.1f}M" if pd.notna(x) else "N/A")
            
            st.dataframe(cand_display.head(100), use_container_width=True)
            
            # Download
            st.download_button(
                "📥 Download PRT Opportunities CSV",
                candidates[available_cols].to_csv(index=False),
                file_name=f"prt_opportunities_{selected_year}.csv"
            )
            
            # Score explanation
            with st.expander("ℹ️ How is PRT Readiness Score calculated?"):
                st.markdown("""
                The PRT Readiness Score (0-100) is calculated based on factors that typically indicate 
                a plan is a good candidate for pension risk transfer:
                
                | Factor | Points |
                |--------|--------|
                | High retiree percentage | Up to 30 pts |
                | Funding status ≥80% | 20 pts |
                | Funding status ≥95% | +10 pts |
                | Assets ≥$100M | 10 pts |
                | Assets ≥$500M | +10 pts |
                | Assets ≥$1B | +10 pts |
                | Not using substitute mortality | 20 pts |
                
                **Higher scores indicate plans more likely to be PRT candidates.**
                """)
        else:
            st.warning("PRT Readiness Score not calculated. Re-run the data pipeline to generate this metric.")
    
    # === TAB 5: ASSET ANALYSIS ===
    with prt_tab5:
        st.subheader("Asset Analysis")
        
        # Asset growth metrics
        if 'SCH_H_TOTAL_ASSETS_BOY' in db.columns and assets_col in db.columns:
            st.write("### Asset Changes Year-over-Year")
            
            # Plans with asset data
            asset_df = db[[assets_col, 'SCH_H_TOTAL_ASSETS_BOY', 'SCH_H_ASSET_CHANGE', 'SCH_H_ASSET_CHANGE_PCT']].dropna()
            
            if len(asset_df) > 0:
                col1, col2, col3, col4 = st.columns(4)
                
                total_boy = asset_df['SCH_H_TOTAL_ASSETS_BOY'].sum()
                total_eoy = asset_df[assets_col].sum()
                net_change = total_eoy - total_boy
                avg_change_pct = asset_df['SCH_H_ASSET_CHANGE_PCT'].mean()
                
                col1.metric("Total Assets BOY", f"${total_boy/1e12:,.2f}T")
                col2.metric("Total Assets EOY", f"${total_eoy/1e12:,.2f}T")
                col3.metric("Net Change", f"${net_change/1e9:,.1f}B", delta=f"{net_change/total_boy*100:.1f}%")
                col4.metric("Avg Plan Change", f"{avg_change_pct:.1f}%")
        
        # Investment allocation
        st.write("### Investment Allocation Summary")
        
        allocation_cols = {
            'SCH_H_CASH_EOY': 'Cash',
            'SCH_H_GOVT_SECURITIES_EOY': 'Government Securities',
            'SCH_H_CORP_DEBT_EOY': 'Corporate Debt',
            'SCH_H_COMMON_STOCK_EOY': 'Common Stock',
            'SCH_H_PREF_STOCK_EOY': 'Preferred Stock',
            'SCH_H_REAL_ESTATE_EOY': 'Real Estate',
            'SCH_H_INS_CO_GEN_ACCT_EOY': 'Insurance Co. General Account'
        }
        
        alloc_data = {}
        for col, name in allocation_cols.items():
            if col in db.columns:
                alloc_data[name] = db[col].fillna(0).sum()
        
        if alloc_data:
            alloc_df = pd.DataFrame.from_dict(alloc_data, orient='index', columns=['Total ($)'])
            alloc_df['% of Total'] = (alloc_df['Total ($)'] / alloc_df['Total ($)'].sum() * 100).round(1)
            alloc_df = alloc_df.sort_values('Total ($)', ascending=False)
            
            col1, col2 = st.columns(2)
            with col1:
                # Format for display
                alloc_display = alloc_df.copy()
                alloc_display['Total ($)'] = alloc_display['Total ($)'].apply(lambda x: f"${x/1e9:,.1f}B")
                alloc_display['% of Total'] = alloc_display['% of Total'].apply(lambda x: f"{x:.1f}%")
                st.dataframe(alloc_display)
            
            with col2:
                st.bar_chart(alloc_df['% of Total'])
        
        # Income/Expense analysis
        st.write("### Income & Expense Summary")
        
        income_cols = {
            'SCH_H_TOTAL_INCOME': 'Total Income',
            'SCH_H_TOTAL_CONTRIBUTIONS': 'Total Contributions',
            'SCH_H_TOTAL_DISTRIBUTIONS': 'Total Distributions',
            'SCH_H_TOTAL_EXPENSES': 'Total Expenses'
        }
        
        income_data = {}
        for col, name in income_cols.items():
            if col in db.columns:
                income_data[name] = db[col].fillna(0).sum()
        
        if income_data:
            income_df = pd.DataFrame.from_dict(income_data, orient='index', columns=['Total ($)'])
            income_df['Total ($)'] = income_df['Total ($)'].apply(lambda x: f"${x/1e9:,.1f}B")
            st.dataframe(income_df)

# =============================
# PRT HISTORY PAGE (MULTI-YEAR)
# =============================
elif menu == "PRT History":
    st.title("PRT Transaction History (2019-2024)")
    st.caption("Analyze pension risk transfer patterns across multiple years.")
    st.markdown("---")
    
    # Helper function to format PRT amounts (handles lists, numpy arrays, etc.)
    def format_prt_amounts(amounts):
        """Convert list/array of PRT amounts to formatted string."""
        try:
            # Handle various input types
            if amounts is None:
                return "N/A"
            # Convert numpy array or other iterables to list
            if hasattr(amounts, 'tolist'):
                amounts = amounts.tolist()
            if not isinstance(amounts, (list, tuple)):
                amounts = [amounts]
            # Format each amount
            formatted = []
            for v in amounts:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    formatted.append("N/A")
                elif abs(v) >= 1e9:
                    formatted.append(f"${v/1e9:.1f}B")
                elif abs(v) >= 1e6:
                    formatted.append(f"${v/1e6:.0f}M")
                else:
                    formatted.append(f"${v/1e3:.0f}K")
            return ', '.join(formatted)
        except Exception:
            return str(amounts)
    
    def format_years(years):
        """Convert list/array of years to formatted string."""
        try:
            if years is None:
                return "N/A"
            if hasattr(years, 'tolist'):
                years = years.tolist()
            if not isinstance(years, (list, tuple)):
                years = [years]
            return ', '.join(str(int(y)) for y in years)
        except Exception:
            return str(years)
    
    # Load multi-year history if available
    prt_history_path = os.path.join(PROJECT_ROOT, "data_output", "prt_multi_year_history.parquet")
    
    if not os.path.exists(prt_history_path):
        st.warning("""
        ⚠️ **Multi-year PRT history not yet generated.**
        
        Run the PRT analysis script to generate the history:
        
        ```python
        python data_analysis/prt_multi_year_analysis.py
        ```
        """)
        st.stop()
    
    @st.cache_data
    def load_prt_history():
        return pd.read_parquet(prt_history_path)
    
    prt_hist = load_prt_history()
    
    # Tabs for different views
    hist_tab1, hist_tab2, hist_tab3, hist_tab4, hist_tab5 = st.tabs([
        "📊 Summary", "🏢 By Sponsor", "🔄 Repeat Transactors", "📈 Trends", "🔍 Search"
    ])
    
    # === TAB 1: SUMMARY ===
    with hist_tab1:
        st.subheader("Multi-Year PRT Summary")
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        total_prt = prt_hist['TOTAL_PRT'].sum()
        unique_plans = len(prt_hist)
        repeat_count = (prt_hist['NUM_TRANSACTIONS'] >= 2).sum()
        repeat_prt = prt_hist[prt_hist['NUM_TRANSACTIONS'] >= 2]['TOTAL_PRT'].sum()
        
        col1.metric("Total PRT Volume", f"${total_prt/1e9:,.1f}B")
        col2.metric("Unique Plans with PRT", f"{unique_plans:,}")
        col3.metric("Repeat Transactors", f"{repeat_count:,}")
        col4.metric("% from Repeat", f"{repeat_prt/total_prt*100:.1f}%")
        
        st.markdown("---")
        
        # Transaction frequency distribution
        st.subheader("Transaction Frequency Distribution")
        freq = prt_hist['NUM_TRANSACTIONS'].value_counts().sort_index()
        freq_df = pd.DataFrame({
            'Transactions': freq.index,
            'Plan Count': freq.values
        })
        
        col1, col2 = st.columns(2)
        with col1:
            st.bar_chart(freq_df.set_index('Transactions'))
        with col2:
            freq_df['% of Total'] = (freq_df['Plan Count'] / freq_df['Plan Count'].sum() * 100).round(1)
            st.dataframe(freq_df, use_container_width=True)
        
        # Top 20 by total PRT
        st.subheader("Top 20 Plans by Total PRT Volume")
        top20 = prt_hist.head(20).copy()
        top20['YEARS_STR'] = top20['YEARS'].apply(format_years)
        top20['PRT_BY_YEAR_FMT'] = top20['PRT_BY_YEAR'].apply(format_prt_amounts)
        top20['TOTAL_PRT_FMT'] = top20['TOTAL_PRT'].apply(lambda x: f"${x/1e6:,.0f}M" if x >= 1e6 else f"${x/1e3:,.0f}K")
        
        display_cols = ['SPONSOR_NAME', 'YEARS_STR', 'PRT_BY_YEAR_FMT', 'TOTAL_PRT_FMT']
        if 'INDUSTRY_SECTOR' in top20.columns:
            display_cols.insert(1, 'INDUSTRY_SECTOR')
        
        top20_display = top20[display_cols].rename(columns={
            'SPONSOR_NAME': 'Sponsor',
            'YEARS_STR': 'Years',
            'PRT_BY_YEAR_FMT': 'PRT by Year',
            'TOTAL_PRT_FMT': 'Total PRT'
        })
        if 'INDUSTRY_SECTOR' in top20_display.columns:
            top20_display = top20_display.rename(columns={'INDUSTRY_SECTOR': 'Industry'})
        
        st.dataframe(top20_display, use_container_width=True)
    
    # === TAB 2: BY SPONSOR ===
    with hist_tab2:
        st.subheader("PRT Summary by Plan Sponsor")
        st.caption("Aggregated view of PRT activity across all plans for each sponsor")
        
        # Aggregate by sponsor name (normalize to handle slight variations)
        sponsor_agg = prt_hist.groupby('SPONSOR_NAME').agg({
            'TOTAL_PRT': 'sum',
            'TRACKING_ID': 'count',  # Number of plans
            'NUM_TRANSACTIONS': 'sum',  # Total transactions across all plans
            'YEARS': lambda x: sorted(set(y for years in x for y in (years.tolist() if hasattr(years, 'tolist') else years))),
            'EIN': 'first',
        }).reset_index()
        
        sponsor_agg.columns = ['SPONSOR_NAME', 'TOTAL_PRT', 'NUM_PLANS', 'TOTAL_TRANSACTIONS', 'YEARS_ACTIVE', 'EIN']
        sponsor_agg = sponsor_agg.sort_values('TOTAL_PRT', ascending=False)
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        unique_sponsors = len(sponsor_agg)
        multi_plan_sponsors = (sponsor_agg['NUM_PLANS'] > 1).sum()
        top_sponsor_prt = sponsor_agg['TOTAL_PRT'].iloc[0] if len(sponsor_agg) > 0 else 0
        top_sponsor_name = sponsor_agg['SPONSOR_NAME'].iloc[0] if len(sponsor_agg) > 0 else "N/A"
        
        col1.metric("Unique Sponsors", f"{unique_sponsors:,}")
        col2.metric("Multi-Plan Sponsors", f"{multi_plan_sponsors:,}")
        col3.metric("Top Sponsor PRT", f"${top_sponsor_prt/1e9:.1f}B")
        col4.metric("Top Sponsor", top_sponsor_name[:20])
        
        st.markdown("---")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            min_plans_filter = st.selectbox("Minimum Plans", [1, 2, 3, 5], index=0, key="sponsor_min_plans")
        with col2:
            min_prt_sponsor = st.number_input("Minimum Total PRT ($M)", min_value=0, value=0, step=50, key="sponsor_min_prt")
        with col3:
            sort_by = st.selectbox("Sort By", ["Total PRT", "Number of Plans", "Number of Transactions"], index=0)
        
        # Apply filters
        filtered_sponsors = sponsor_agg[sponsor_agg['NUM_PLANS'] >= min_plans_filter].copy()
        filtered_sponsors = filtered_sponsors[filtered_sponsors['TOTAL_PRT'] >= min_prt_sponsor * 1e6]
        
        # Apply sorting
        sort_col = {'Total PRT': 'TOTAL_PRT', 'Number of Plans': 'NUM_PLANS', 'Number of Transactions': 'TOTAL_TRANSACTIONS'}[sort_by]
        filtered_sponsors = filtered_sponsors.sort_values(sort_col, ascending=False)
        
        st.write(f"**{len(filtered_sponsors):,} sponsors** matching criteria")
        
        # Prepare display
        display_sponsors = filtered_sponsors.copy()
        display_sponsors['YEARS_STR'] = display_sponsors['YEARS_ACTIVE'].apply(format_years)
        display_sponsors['TOTAL_PRT_FMT'] = display_sponsors['TOTAL_PRT'].apply(
            lambda x: f"${x/1e9:.2f}B" if x >= 1e9 else f"${x/1e6:,.0f}M"
        )
        display_sponsors['AVG_PRT_PER_PLAN'] = (display_sponsors['TOTAL_PRT'] / display_sponsors['NUM_PLANS']).apply(
            lambda x: f"${x/1e6:,.0f}M"
        )
        
        display_cols = ['SPONSOR_NAME', 'NUM_PLANS', 'TOTAL_TRANSACTIONS', 'YEARS_STR', 'TOTAL_PRT_FMT', 'AVG_PRT_PER_PLAN']
        
        st.dataframe(
            display_sponsors[display_cols].rename(columns={
                'SPONSOR_NAME': 'Sponsor',
                'NUM_PLANS': '# Plans',
                'TOTAL_TRANSACTIONS': '# Transactions',
                'YEARS_STR': 'Years Active',
                'TOTAL_PRT_FMT': 'Total PRT',
                'AVG_PRT_PER_PLAN': 'Avg per Plan'
            }),
            use_container_width=True
        )
        
        # Download
        st.download_button(
            "📥 Download Sponsor Summary CSV",
            display_sponsors[['SPONSOR_NAME', 'EIN', 'NUM_PLANS', 'TOTAL_TRANSACTIONS', 'YEARS_STR', 'TOTAL_PRT']].to_csv(index=False),
            file_name="prt_by_sponsor.csv"
        )
        
        st.markdown("---")
        
        # Drill-down: Select a sponsor to see their plans
        st.subheader("Sponsor Drill-Down")
        sponsor_list = filtered_sponsors['SPONSOR_NAME'].head(100).tolist()
        if sponsor_list:
            selected_sponsor = st.selectbox("Select a sponsor to view their plans", sponsor_list, key="sponsor_drilldown")
            
            if selected_sponsor:
                sponsor_plans = prt_hist[prt_hist['SPONSOR_NAME'] == selected_sponsor].copy()
                
                st.write(f"**{len(sponsor_plans)} plan(s)** for {selected_sponsor}")
                
                # Show each plan
                for _, plan in sponsor_plans.iterrows():
                    years_str = format_years(plan['YEARS'])
                    amounts_str = format_prt_amounts(plan['PRT_BY_YEAR'])
                    total_fmt = f"${plan['TOTAL_PRT']/1e6:,.0f}M" if plan['TOTAL_PRT'] >= 1e6 else f"${plan['TOTAL_PRT']/1e3:,.0f}K"
                    
                    with st.expander(f"{plan['PLAN_NAME']} (EIN: {plan['EIN']}, Plan #{plan['PLAN_NUMBER']})"):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Years with PRT:** {years_str}")
                            st.write(f"**Number of Transactions:** {plan['NUM_TRANSACTIONS']}")
                        with col2:
                            st.write(f"**PRT by Year:** {amounts_str}")
                            st.write(f"**Total PRT:** {total_fmt}")
    
    # === TAB 3: REPEAT TRANSACTORS ===
    with hist_tab3:
        st.subheader("Repeat PRT Transactors")
        st.caption("Plans with PRT transactions in multiple years (2+ years)")
        
        # Filters
        col1, col2 = st.columns(2)
        with col1:
            min_trans = st.selectbox("Minimum Transactions", [2, 3, 4, 5, 6], index=0)
        with col2:
            min_prt = st.number_input("Minimum Total PRT ($M)", min_value=0, value=0, step=10)
        
        # Filter data
        repeat_df = prt_hist[prt_hist['NUM_TRANSACTIONS'] >= min_trans].copy()
        repeat_df = repeat_df[repeat_df['TOTAL_PRT'] >= min_prt * 1e6]
        
        st.write(f"**{len(repeat_df):,} plans** with {min_trans}+ transactions")
        
        # Prepare display with proper formatting
        repeat_df['YEARS_STR'] = repeat_df['YEARS'].apply(format_years)
        repeat_df['AMOUNTS_STR'] = repeat_df['PRT_BY_YEAR'].apply(format_prt_amounts)
        repeat_df['TOTAL_PRT_FMT'] = repeat_df['TOTAL_PRT'].apply(lambda x: f"${x/1e6:,.0f}M" if x >= 1e6 else f"${x/1e3:,.0f}K")
        
        display_cols = ['SPONSOR_NAME', 'YEARS_STR', 'AMOUNTS_STR', 'TOTAL_PRT_FMT', 'EIN']
        repeat_display = repeat_df[display_cols].rename(columns={
            'SPONSOR_NAME': 'Sponsor',
            'YEARS_STR': 'Years',
            'AMOUNTS_STR': 'PRT by Year',
            'TOTAL_PRT_FMT': 'Total PRT',
            'EIN': 'EIN'
        })
        
        st.dataframe(repeat_display, use_container_width=True)
        
        # Download
        st.download_button(
            "📥 Download Repeat Transactors CSV",
            repeat_df[['SPONSOR_NAME', 'EIN', 'PLAN_NUMBER', 'YEARS_STR', 'AMOUNTS_STR', 'TOTAL_PRT']].to_csv(index=False),
            file_name="prt_repeat_transactors.csv"
        )
    
    # === TAB 4: TRENDS ===
    with hist_tab4:
        st.subheader("PRT Trends Over Time")
        
        # Calculate yearly totals from the transaction lists
        yearly_data = []
        for _, row in prt_hist.iterrows():
            if isinstance(row['YEARS'], list) and isinstance(row['PRT_BY_YEAR'], list):
                for year, amount in zip(row['YEARS'], row['PRT_BY_YEAR']):
                    yearly_data.append({'Year': year, 'PRT_Amount': amount})
        
        if yearly_data:
            yearly_df = pd.DataFrame(yearly_data)
            yearly_agg = yearly_df.groupby('Year').agg({
                'PRT_Amount': ['sum', 'count']
            }).reset_index()
            yearly_agg.columns = ['Year', 'Total PRT', 'Transaction Count']
            yearly_agg = yearly_agg.sort_values('Year')
            
            # Display metrics
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("### Total PRT Volume by Year")
                chart_data = yearly_agg.set_index('Year')['Total PRT'] / 1e9
                st.bar_chart(chart_data)
            
            with col2:
                st.write("### Transaction Count by Year")
                st.bar_chart(yearly_agg.set_index('Year')['Transaction Count'])
            
            # Summary table
            st.write("### Yearly Summary")
            yearly_display = yearly_agg.copy()
            yearly_display['Total PRT'] = yearly_display['Total PRT'].apply(lambda x: f"${x/1e9:,.2f}B")
            yearly_display['Avg Transaction'] = (yearly_agg['Total PRT'] / yearly_agg['Transaction Count']).apply(lambda x: f"${x/1e6:,.1f}M")
            st.dataframe(yearly_display, use_container_width=True)
    
    # === TAB 5: SEARCH ===
    with hist_tab5:
        st.subheader("Search PRT History")
        
        # Search by sponsor name
        search_term = st.text_input("Search by sponsor name", placeholder="e.g., IBM, AT&T, Lockheed")
        
        if search_term:
            search_results = prt_hist[prt_hist['SPONSOR_NAME'].str.contains(search_term, case=False, na=False)].copy()
            
            if len(search_results) == 0:
                st.info(f"No plans found matching '{search_term}'")
            else:
                st.write(f"**{len(search_results):,} plans** matching '{search_term}'")
                
                # Prepare display with proper formatting
                search_results['YEARS_STR'] = search_results['YEARS'].apply(format_years)
                search_results['AMOUNTS_STR'] = search_results['PRT_BY_YEAR'].apply(format_prt_amounts)
                search_results['TOTAL_PRT_FMT'] = search_results['TOTAL_PRT'].apply(lambda x: f"${x/1e6:,.0f}M" if x >= 1e6 else f"${x/1e3:,.0f}K")
                
                display_cols = ['SPONSOR_NAME', 'PLAN_NAME', 'EIN', 'YEARS_STR', 'AMOUNTS_STR', 'TOTAL_PRT_FMT']
                available_cols = [c for c in display_cols if c in search_results.columns]
                
                st.dataframe(search_results[available_cols].rename(columns={
                    'SPONSOR_NAME': 'Sponsor',
                    'PLAN_NAME': 'Plan Name',
                    'YEARS_STR': 'Years',
                    'AMOUNTS_STR': 'PRT by Year',
                    'TOTAL_PRT_FMT': 'Total PRT'
                }), use_container_width=True)
        
        # EIN lookup
        st.markdown("---")
        ein_search = st.text_input("Search by EIN", placeholder="e.g., 133937090")
        
        if ein_search:
            ein_clean = ein_search.replace('-', '').strip()
            ein_results = prt_hist[prt_hist['EIN'].astype(str).str.contains(ein_clean, na=False)].copy()
            
            if len(ein_results) == 0:
                st.info(f"No plans found for EIN '{ein_search}'")
            else:
                st.write(f"**{len(ein_results):,} plans** for EIN '{ein_search}'")
                
                for _, row in ein_results.iterrows():
                    years_str = format_years(row['YEARS'])
                    amounts_str = format_prt_amounts(row['PRT_BY_YEAR'])
                    total_prt_fmt = f"${row['TOTAL_PRT']/1e6:,.0f}M" if row['TOTAL_PRT'] >= 1e6 else f"${row['TOTAL_PRT']/1e3:,.0f}K"
                    
                    with st.expander(f"{row['SPONSOR_NAME']} - EIN: {row['EIN']}"):
                        st.write(f"**Plan Name:** {row['PLAN_NAME']}")
                        st.write(f"**Years with PRT:** {years_str}")
                        st.write(f"**PRT Amounts:** {amounts_str}")
                        st.write(f"**Total PRT:** {total_prt_fmt}")
                        st.write(f"**Number of Transactions:** {row['NUM_TRANSACTIONS']}")

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
        
        # Store original firm names for reference
        db_firms["ORIGINAL_FIRM_NAME"] = db_firms[actuary_firm_col]
        
        # Apply firm name normalization to consolidate variations
        db_firms["NORMALIZED_FIRM"] = db_firms[actuary_firm_col].apply(normalize_firm_name)
        
        # Toggle for normalized vs raw view
        use_normalized = st.sidebar.checkbox("Consolidate firm name variations", value=True, 
                                              help="Combines variations like 'AON CONSULTING INC' and 'AON CONSULTING, INC.' into a single entry")
        
        firm_col_to_use = "NORMALIZED_FIRM" if use_normalized else actuary_firm_col
        
        # Get list of unique firms sorted by plan count
        firm_counts = db_firms.groupby(firm_col_to_use).size().reset_index(name='Plan Count')
        firm_counts = firm_counts.sort_values('Plan Count', ascending=False)
        all_firms = firm_counts[firm_col_to_use].tolist()
        
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
                
                # Filter data for selected firm (use the appropriate column based on toggle)
                firm_data = db_firms[db_firms[firm_col_to_use] == selected_firm]
                
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
            
            # Aggregate by firm (use normalized or raw based on toggle)
            firm_stats = db_firms.groupby(firm_col_to_use).agg(agg_dict).reset_index()
            firm_stats = firm_stats.rename(columns={
                firm_col_to_use: "Actuarial Firm",
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
