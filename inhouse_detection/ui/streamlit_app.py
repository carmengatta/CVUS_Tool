def main():

"""
Read-only Streamlit UI for manual review of Evidence and ContactCandidate data.
No scraping triggers, no editing, no synthesis or orchestration.
Focus: clarity, auditability, confidence visualization, drill-down.
"""
import streamlit as st
import pandas as pd
import json
from pathlib import Path
from schemas.evidence import Evidence
from schemas.contact_candidate import ContactCandidate

DATA_DIR = Path("inhouse_detection/ui/data")
EVIDENCE_FILE = DATA_DIR / "evidence.json"
CANDIDATE_FILE = DATA_DIR / "candidates.json"

def load_evidence():
    if not EVIDENCE_FILE.exists():
        return []
    with open(EVIDENCE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [Evidence(**item) for item in data]

def load_candidates():
    if not CANDIDATE_FILE.exists():
        return []
    with open(CANDIDATE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [ContactCandidate(**item) for item in data]

def evidence_table(evidence_list):
    df = pd.DataFrame([e.dict() for e in evidence_list])
    st.subheader("Evidence Records")
    st.dataframe(df, use_container_width=True)
    st.caption("Showing all loaded evidence. Use search/filter in your browser as needed.")

def candidate_table(candidate_list, evidence_list):
    df = pd.DataFrame([c.dict() for c in candidate_list])
    st.subheader("Contact Candidates")
    # Add confidence bar
    if "confidence_score" in df.columns:
        st.dataframe(
            df.style.bar(subset=["confidence_score"], color="#6fa8dc", vmin=0, vmax=1),
            use_container_width=True
        )
    else:
        st.dataframe(df, use_container_width=True)
    st.caption("Showing all loaded candidates. Click a row to drill down.")

    # Drill-down: select candidate
    selected = st.selectbox(
        "Select a candidate to view supporting evidence:",
        options=[(c.candidate_id, c.name) for c in candidate_list],
        format_func=lambda x: f"{x[1]} ({x[0][:8]})" if x else ""
    )
    if selected:
        candidate = next((c for c in candidate_list if c.candidate_id == selected[0]), None)
        if candidate:
            st.markdown(f"### Candidate: {candidate.name}")
            st.write(candidate)
            # Show supporting evidence
            supporting = [e for e in evidence_list if e.evidence_id in candidate.evidence_ids]
            st.markdown("#### Supporting Evidence")
            if supporting:
                st.dataframe(pd.DataFrame([e.dict() for e in supporting]), use_container_width=True)
            else:
                st.info("No supporting evidence found for this candidate.")

def main():
    st.title("In-House Actuarial Contact Discovery (Read-Only Review)")
    st.write("This interface allows manual review of pre-saved evidence and candidate data. No editing or scraping is possible.")

    evidence_list = load_evidence()
    candidate_list = load_candidates()

    st.sidebar.header("Navigation")
    page = st.sidebar.radio("Go to:", ["Candidates", "Evidence"])

    if page == "Candidates":
        candidate_table(candidate_list, evidence_list)
    else:
        evidence_table(evidence_list)

    st.sidebar.markdown("---")
    st.sidebar.caption("Data loaded from 'ui/data/evidence.json' and 'ui/data/candidates.json'.")

if __name__ == "__main__":
    main()
