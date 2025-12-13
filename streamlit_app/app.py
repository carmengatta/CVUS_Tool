
import streamlit as st
import pandas as pd
import os

# ==========================================================
# SIMPLE PASSWORD PROTECTION (Streamlit v1.25+ compatible)
# ==========================================================
PASSWORD = "CVUSTool"  # CHANGE FOR DEPLOYMENT

def password_gate():
    pw = st.text_input("Enter password:", type="password")
    if pw == PASSWORD:
        st.session_state["authenticated"] = True
        st.success("Access granted!")
        st.rerun()
    elif pw != "":
        st.error("Incorrect password")

# Initialize session state
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

# Block access until logged in
if not st.session_state["authenticated"]:
    password_gate()
    st.stop()


# ==========================================================
# SIDEBAR NAVIGATION
# ==========================================================
st.sidebar.title("📘 Navigation")

page = st.sidebar.radio(
    "Go to:",
    [
        "📊 Dashboard",
        "🔍 Plan Explorer",
        "🏢 Sponsor Rollups",
        "⭐ Lead Scoring (Coming Soon)"
    ]
)

# Logout Button
if st.sidebar.button("Logout"):
    st.session_state["authenticated"] = False
    st.rerun()



# ==========================================================
# YEAR SELECTION & DATA LOADING (with caching)
# ==========================================================
st.sidebar.markdown("---")

# --- Improved year extraction: only use files with numeric year suffix ---
import re
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



# ==========================================================
# DYNAMIC DASHBOARD: TOP PLANS & COMPANY ROLLUP (selected year)
# ==========================================================
if page == "📊 Dashboard":
    st.title(f"Defined Benefit Plan Dashboard — {selected_year}")
    st.caption("All data is DB-only, SB-driven, and year-specific.")

    # --- Top Plans by Retiree Count ---

    st.subheader("Top Plans by Retiree Count")
    top_n = st.slider("Show top N plans", 5, 50, 10)
    if "RETIRED" in db.columns:
        cols = [c for c in ["EIN", "PLAN_NAME", "RETIRED", "SB_TERM_PARTCP_CNT", "LIABILITY_TOTAL"] if c in db.columns]
        top_plans = db.sort_values("RETIRED", ascending=False).head(top_n)
        st.dataframe(top_plans[cols], use_container_width=True)
    else:
        st.warning("Retiree count column (RETIRED) not found in this file. Please check your data export.")

    st.divider()

    # --- Top Companies by Total Retirees (EIN rollup) ---

    st.subheader("Top Companies by Total Retirees (EIN Rollup)")
    if "RETIRED" in db.columns and "EIN" in db.columns:
        ein_rollup = db.groupby(["EIN"]).agg({
            k: v for k, v in {"RETIRED": "sum", "PLAN_NAME": "count", "LIABILITY_TOTAL": "sum"}.items() if k in db.columns
        }).rename(columns={"PLAN_NAME": "NUM_PLANS"}).reset_index()
        ein_rollup = ein_rollup.sort_values("RETIRED", ascending=False)
        st.dataframe(ein_rollup.head(top_n), use_container_width=True)
    else:
        st.warning("Required columns (RETIRED, EIN) not found for company rollup.")

    st.divider()

    # --- Plan Size Distribution ---

    st.subheader("Plan Size Distribution")
    st.write("Distribution of plans by participant count (SB_TERM_PARTCP_CNT)")
    if "SB_TERM_PARTCP_CNT" in db.columns:
        st.bar_chart(db["SB_TERM_PARTCP_CNT"].value_counts().sort_index())
    else:
        st.warning("Participant count column (SB_TERM_PARTCP_CNT) not found in this file.")

    # --- Participant Mix ---
    st.subheader("Participant Mix (Active / Retired / Terminated)")
    if all(col in db.columns for col in ["ACTIVE", "RETIRED", "TERMINATED"]):
        mix = db[["ACTIVE", "RETIRED", "TERMINATED"]].sum()
        mix_pct = mix / mix.sum()
        st.write("Percent composition of all loaded plans:")
        st.bar_chart(mix_pct)
    else:
        st.info("Participant mix columns not available in this dataset.")


# ==========================================================
# PAGE 2 — PLAN EXPLORER
# ==========================================================
elif page == "🔍 Plan Explorer":
    st.title("🔍 Explore Individual Plans")

    search_name = st.text_input("Search by sponsor or plan name:")

    filtered = master.copy()
    if search_name:
        filtered = filtered[
            filtered["sponsor_dfe_name"].str.contains(search_name, case=False, na=False)
            | filtered["plan_name"].str.contains(search_name, case=False, na=False)
        ]

    st.dataframe(
        filtered.sort_values("retired", ascending=False),
        use_container_width=True,
        height=600
    )


# ==========================================================
# PAGE 3 — SPONSOR ROLLOUPS
# ==========================================================
elif page == "🏢 Sponsor Rollups":
    st.title("🏢 Sponsor-Level Profiles")

    search_ein = st.text_input("Search EIN:")

    if search_ein:
        df_show = sponsor[sponsor["ein"].astype(str).str.contains(search_ein)]
    else:
        df_show = sponsor.head(50)

    st.dataframe(df_show, use_container_width=True, height=600)


# ==========================================================
# SECTION 4 — IN-HOUSE ACTUARY DETECTION
# ==========================================================
st.header("🧠 In-House Actuary Detection")

st.write("""
This tool helps identify whether a sponsor employs **internal actuaries** by performing 
a structured web lookup (company site, SOA directory, LinkedIn, news articles, etc.).
Only sponsors with **10,000+ annuitants** are shown.
""")

# Step 1 — Filter sponsors with large populations
LARGE_SPONSORS = sponsor[sponsor["retired"] > 10000].copy()

if LARGE_SPONSORS.empty:
    st.info("No sponsors with more than 10,000 annuitants found.")
else:
    sponsor_select = st.selectbox(
        "Select a sponsor to investigate:",
        LARGE_SPONSORS["sponsor_name"].unique(),
        index=0
    )

    selected_row = LARGE_SPONSORS[
        LARGE_SPONSORS["sponsor_name"] == sponsor_select
    ].iloc[0]

    ein = str(selected_row["ein"])
    retired_count = int(selected_row["retired"])
    st.subheader(f"🔎 Searching for actuaries at: **{sponsor_select}**")
    st.caption(f"EIN: {ein} — Retirees: {retired_count:,}")

    # ------------------------------------------------------
    # Button: Run SERP API Web Search
    # ------------------------------------------------------
    def serp_lookup(query):
        """Perform SERP API request."""
        from serpapi import GoogleSearch
        import os

        api_key = os.getenv("SERP_API_KEY")
        if not api_key:
            st.error("Missing SERP_API_KEY in .env file.")
            return None

        params = {
            "engine": "google",
            "q": query,
            "api_key": api_key
        }

        try:
            search = GoogleSearch(params)
            results = search.get_dict()
            return results
        except Exception as e:
            st.error(f"Search failed: {e}")
            return None

    # ------------------------------------------------------
    # Query builder
    # ------------------------------------------------------
    query = f'"{sponsor_select}" actuary OR actuarial team OR pension actuarial staff'

    if st.button("Run Actuary Search"):
        with st.spinner("Searching the web for actuarial staff…"):
            result = serp_lookup(query)

        if result is None:
            st.stop()

        # Extract best results
        organic = result.get("organic_results", [])
        st.subheader("📄 Search Results")

        if not organic:
            st.warning("No relevant results found.")
        else:
            for entry in organic[:5]:  # show top 5
                st.write(f"### [{entry.get('title')}]({entry.get('link')})")
                st.write(entry.get("snippet", ""))
                st.write("---")

    # ------------------------------------------------------
    # (Optional) Save results for future reuse
    # ------------------------------------------------------
    # TODO: You can later add persistent caching using a small SQLite db
