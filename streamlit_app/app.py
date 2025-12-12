import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables like SERP_API_KEY
load_dotenv()


# ==========================================================
# SIMPLE PASSWORD PROTECTION (Streamlit v1.25+ compatible)
# ==========================================================
PASSWORD = "CVUSTool"  # CHANGE FOR DEPLOYMENT

def password_gate():
    st.title("🔐 Secure Access")
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
# LOAD DATA HELPERS
# ==========================================================
MASTER_PATH = "data_output/master_db_latest.parquet"
SPONSOR_PATH = "data_output/sponsor_rollup_latest.parquet"

@st.cache_data
def load_master():
    if not os.path.exists(MASTER_PATH):
        st.error(f"Missing required dataset: `{MASTER_PATH}`")
        st.stop()
    return pd.read_parquet(MASTER_PATH)

@st.cache_data
def load_sponsor_rollup():
    if not os.path.exists(SPONSOR_PATH):
        st.error(f"Missing required dataset: `{SPONSOR_PATH}`")
        st.stop()
    return pd.read_parquet(SPONSOR_PATH)

master = load_master()
sponsor = load_sponsor_rollup()


# ==========================================================
# PAGE 1 — DASHBOARD
# ==========================================================
if page == "📊 Dashboard":
    st.title("📊 US Private-Sector DB Plan Analytics Dashboard")
    st.write("Built from Form 5500 + Schedule SB filings")
    st.success("Datasets loaded successfully!")

    # ----- Summary Metrics -----
    st.header("📌 Summary Metrics")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total DB Plans", f"{len(master):,}")
    col2.metric("Unique Sponsors (EIN)", f"{master['ein'].nunique():,}")
    col3.metric("Total Retirees", f"{master['retired'].sum():,}")
    col4.metric("Total Liability ($)", f"${master['liability_total'].sum():,}")

    st.divider()

    # ----- Top Plans -----
    st.header("🏆 Top 25 Plans by Retiree Count")

    st.dataframe(
        master[
            ["sponsor_dfe_name", "plan_name", "retired", "liability_total"]
        ].sort_values("retired", ascending=False).head(25),
        use_container_width=True,
        height=600
    )


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
