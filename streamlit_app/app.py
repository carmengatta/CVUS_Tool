import streamlit as st
import pandas as pd
import os

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
# PAGE 4 — LEAD SCORING (COMING SOON)
# ==========================================================
elif page == "⭐ Lead Scoring (Coming Soon)":
    st.title("⭐ Lead Scoring Engine")

    st.write("""
    This module will identify DB plans most likely to need:

    - actuarial consulting  
    - risk transfer services  
    - valuation model upgrades  
    - in-house actuary replacement  
    - longevity analytics  
    - contribution / funding optimization  

    **Planned additions include:**
    - Detection of *in-house actuaries*  
    - Firm-size classification (WTW/Mercer/Aon vs boutique firms)  
    - Retiree-heavy plan scoring  
    - Funding stress indicators  
    - Volatility risk scoring  

    This page will become interactive as soon as we build the scoring engine.
    """)

