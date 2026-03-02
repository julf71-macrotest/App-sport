import streamlit as st

st.set_page_config(page_title="Sport App", layout="wide")

st.title("Sport App perso")
st.write("Pages: Exercices, Programmes, Séance, Historique.")

st.info(
    "Conseil: commence par créer des exercices, puis des programmes, puis lance une séance. "
    "Tout est stocké dans Google Sheets."
)
