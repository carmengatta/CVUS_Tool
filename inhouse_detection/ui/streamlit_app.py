"""
Streamlit UI for manual review of evidence and candidate profiles.
Should provide:
- Manual trigger for scraping/refresh
- Evidence review table
- Candidate profile view with confidence visualization
- Audit log viewer
"""
import streamlit as st

def main():
    st.title("In-House Actuarial Contact Discovery")
    st.write("This UI allows manual review of evidence and candidate profiles.")
    # TODO: Add controls for scraping, evidence table, candidate view, and logs

if __name__ == "__main__":
    main()
