import pandas as pd
import streamlit as st

from lib.sheets import SheetClient, normalize_df
from lib.utils import safe_int

st.set_page_config(page_title="Historique", layout="wide")
st.title("Historique")

@st.cache_resource
def get_client():
    return SheetClient.from_service_account_info(
        dict(st.secrets["google_service_account"]),
        st.secrets["sheet_id"],
    )

client = get_client()

df_sessions = normalize_df(client.read_df("sessions"))
df_tasks = normalize_df(client.read_df("session_tasks"))

if df_sessions.empty:
    st.info("Pas encore de séances.")
    st.stop()

done = df_sessions[df_sessions["status"] == "done"].copy()
done = done.sort_values("ended_at", ascending=False)

st.subheader("3 dernières séances")
last3 = done.head(3).copy()
if last3.empty:
    st.info("Aucune séance terminée.")
    st.stop()

st.dataframe(last3[["session_id","program_name_snapshot","started_at","ended_at"]], use_container_width=True, height=160)

session_ids = [""] + last3["session_id"].tolist() + done["session_id"].tolist()
sel = st.selectbox("Voir détail d'une séance", session_ids)

if not sel:
    st.stop()

st.divider()
st.subheader("Détail")

st.write(f"Session: {sel}")

tasks = df_tasks[df_tasks["session_id"].astype(str) == str(sel)].copy()
if tasks.empty:
    st.warning("Aucune tâche.")
    st.stop()

tasks["order_int"] = tasks["order_index"].map(lambda x: safe_int(x, 999999))
tasks = tasks.sort_values("order_int")

tasks_done = tasks[tasks["completed_at"].astype(str).str.strip() != ""].copy()
st.write(f"Tâches terminées: {len(tasks_done)} / {len(tasks)}")

show = tasks_done[[
    "order_index","block_name","round_index","exercise_name_snapshot","set_index",
    "actual_reps","actual_time_sec","actual_weight","completed_at"
]].copy()
show.columns = ["ordre","bloc","round","exercice","set","reps","time_sec","poids","fait_le"]
st.dataframe(show, use_container_width=True, height=420)
