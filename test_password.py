import streamlit as st

PASSWORD = "CVUSTool"

if "auth" not in st.session_state:
    st.session_state["auth"] = False

if not st.session_state["auth"]:
    pw = st.text_input("Password:", type="password")
    if pw == PASSWORD:
        st.session_state["auth"] = True
        st.rerun()
    st.stop()

st.write("Access granted!")
