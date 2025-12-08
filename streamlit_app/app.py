import streamlit as st
import pandas as pd
import os

# ==========================================================
# SIMPLE PASSWORD PROTECTION (FIXED)
# ==========================================================
PASSWORD = "CVUSTool"  # <<< CHANGE FOR DEPLOYMENT

def password_gate():
    st.title("🔐 Secure Access")
    pw = st.text_input("Enter password:", type="password")

    if pw == PASSWORD:
        st.session_state["authenticated"] = True
        st.success("Access granted!")
        st.experimental_rerun()   # <<< THIS LINE FIXES YOUR ISSUE
    elif pw != "":
        st.error("Incorrect password")

# Initialize state if missing
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

# Stop app unless authenticated
if not st.session_state["authenticated"]:
    password_gate()
    st.stop()

# ==========================================================
# MAIN APP STARTS HERE
# ==========================================================

st.title("📊 US Private-Sector DB Plan Analytics Dashboard")
st.write("Built from Form 5500 + Schedule SB filings")

# ----------------------------------------------------------
# LOAD DATA HELPERS
# ----------------------------------------------------------
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

# Load data
master = load_master()
sponsor = load_sponsor_rollup()

st.success("Datasets loaded successfully!")

# ==========================================================
# SECTION 1 — HIGH-LEVEL SUMMARY METRICS
# ==========================================================
st.header("📌 Summary Metrics")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total DB Plans", f"{len(master):,}")
col2.metric("Unique Sponsors (EIN)", f"{master['ein'].nunique():,}")
col3.metric("Total Retirees", f"{master['retired'].sum():,}")
col4.metric("Total Liability ($)", f"${master['liability_total'].sum():,}")

# ==========================================================
# SECTION 2 — PLAN TABLE EXPLORER
# ==========================================================
st.header("🔍 Explore Individual Plans")

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
    height=500
)

# ==========================================================
# SECTION 3 — SPONSOR-LEVEL ROLLOUPS
# ==========================================================
st.header("🏢 Sponsor-Level Profiles")

search_ein = st.text_input("Search EIN:")
if search_ein:
    sponsor_filtered = sponsor[
        sponsor["ein"].astype(str).str.contains(search_ein)
    ]
    st.dataframe(sponsor_filtered, use_container_width=True)
else:
    st.dataframe(
        sponsor.head(50),
        use_container_width=True
    )
