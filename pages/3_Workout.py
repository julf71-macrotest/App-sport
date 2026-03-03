import time
from streamlit.runtime.scriptrunner import add_script_run_ctx  # optionnel
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import streamlit as st

from lib.sheets import SheetClient, normalize_df
from lib.utils import now_iso, safe_int, safe_float
from lib.workout_engine import build_session_tasks

st.set_page_config(page_title="Séance", layout="wide")
st.title("Séance")

@st.cache_resource
def get_client():
    return SheetClient.from_service_account_info(
        dict(st.secrets["google_service_account"]),
        st.secrets["sheet_id"],
    )

client = get_client()

df_prog = normalize_df(client.read_df("programs"))
df_struct = normalize_df(client.read_df("program_structure"))
df_ex = normalize_df(client.read_df("exercises"))
df_sessions = normalize_df(client.read_df("sessions"))
df_tasks = normalize_df(client.read_df("session_tasks"))

if df_sessions.empty:
    df_sessions = pd.DataFrame(columns=["session_id","program_id","program_name_snapshot","started_at","ended_at","duration_sec","status","notes"])
if df_tasks.empty:
    df_tasks = pd.DataFrame(columns=[
        "task_id","session_id","order_index","block_name","round_index","exercise_id","exercise_name_snapshot","set_index",
        "target_reps","target_time_sec","target_weight","target_rest_sec",
        "actual_reps","actual_time_sec","actual_weight","completed_at"
    ])

# detect in progress sessions
in_prog = df_sessions[df_sessions["status"] == "in_progress"].copy()
in_prog = in_prog.sort_values("started_at", ascending=False)

st.markdown("### Démarrer ou reprendre")
c1, c2 = st.columns([2, 2], gap="large")

with c1:
    st.subheader("Démarrer depuis un programme")
    prog_map = {r["name"]: r["program_id"] for _, r in df_prog.iterrows() if str(r.get("name","")).strip()}
    prog_names = [""] + sorted(prog_map.keys())
    sel_prog = st.selectbox("Programme", prog_names)

    if st.button("Démarrer", type="primary", disabled=not bool(sel_prog)):
        program_id = prog_map[sel_prog]
        session_id, tasks = build_session_tasks(
            program_id=program_id,
            program_name=sel_prog,
            df_structure=df_struct,
            df_exercises=df_ex,
        )
        if not tasks:
            st.error("Programme vide. Ajoute au moins un bloc et un item.")
        else:
            # create session
            client.append_row_dict("sessions", {
                "session_id": session_id,
                "program_id": program_id,
                "program_name_snapshot": sel_prog,
                "started_at": now_iso(),
                "ended_at": "",
                "duration_sec": "",
                "status": "in_progress",
                "notes": "",
            })
            # append tasks
            for t in tasks:
                client.append_row_dict("session_tasks", t)

            st.session_state["active_session_id"] = session_id
            st.session_state["rest_start"] = None
            st.session_state["rest_dur"] = 0
            st.success("Séance démarrée.")
            st.rerun()

with c2:
    st.subheader("Reprendre")
    if in_prog.empty:
        st.info("Aucune séance en cours.")
    else:
        last = in_prog.iloc[0].to_dict()
        label = f"{last.get('program_name_snapshot','')} | {last.get('started_at','')}"
        st.write(f"Séance en cours: {label}")
        if st.button("Reprendre la séance"):
            st.session_state["active_session_id"] = last["session_id"]
            st.rerun()

st.divider()

active_session_id = st.session_state.get("active_session_id", "")
if not active_session_id:
    st.info("Démarre ou reprends une séance.")
    st.stop()

# Reload tasks for this session
df_tasks = normalize_df(client.read_df("session_tasks"))
session_tasks = df_tasks[df_tasks["session_id"].astype(str) == str(active_session_id)].copy()
if session_tasks.empty:
    st.error("Aucune tâche trouvée pour cette séance.")
    st.stop()

session_tasks["order_int"] = session_tasks["order_index"].map(lambda x: safe_int(x, 999999))
session_tasks = session_tasks.sort_values("order_int")

# Find next task not completed
pending = session_tasks[session_tasks["completed_at"].astype(str).str.strip() == ""]
if pending.empty:
    st.success("Toutes les tâches sont terminées.")
    if st.button("Terminer la séance"):
        # close session
        df_sessions = normalize_df(client.read_df("sessions"))
        srow = df_sessions[df_sessions["session_id"].astype(str) == str(active_session_id)].iloc[0].to_dict()
        started = srow.get("started_at","")
        # duration is best effort
        dur = ""
        try:
            # not parsing fully to keep dependencies low
            dur = ""
        except Exception:
            dur = ""
        client.update_row_by_id("sessions", "session_id", active_session_id, {
            "ended_at": now_iso(),
            "duration_sec": dur,
            "status": "done",
        })
        # 1) On marque la tâche comme terminée en base (déjà fait via update_row_by_id)

        # 2) On démarre le repos si besoin
        rest_sec = safe_int(current_task.get("target_rest_sec"), 0)
        if rest_sec > 0:
            st.session_state["rest_end_ts"] = time.time() + rest_sec
            st.session_state["in_rest"] = True
        else:
            st.session_state["in_rest"] = False
            st.session_state["rest_end_ts"] = None

        # 3) On force un refresh des caches de data pour que la prochaine tâche apparaisse
        try:
            st.cache_data.clear()
        except Exception:
            pass

        client.invalidate_cache()
        st.rerun()
        st.session_state["active_session_id"] = ""
        st.session_state["rest_start"] = None
        st.session_state["rest_dur"] = 0
        st.success("Séance terminée.")
        st.rerun()
    st.stop()

current_task = pending.iloc[0].to_dict()

st.markdown("### Exécution")
top1, top2 = st.columns([3, 2], gap="large")

with top1:
    st.subheader("Tâche actuelle")
    ex_name = current_task.get("exercise_name_snapshot","") or current_task.get("exercise_id","")
    block_name = current_task.get("block_name","")
    round_idx = current_task.get("round_index","")
    set_idx = current_task.get("set_index","")

    st.write(f"Bloc: {block_name} | Round: {round_idx} | Set: {set_idx}")
    st.write(f"Exercice: {ex_name}")

    target_reps = current_task.get("target_reps","")
    target_time = current_task.get("target_time_sec","")
    target_weight = current_task.get("target_weight","")
    target_rest = safe_int(current_task.get("target_rest_sec"), 0) or 0

    st.write("Objectif")
    st.write(f"Reps: {target_reps if str(target_reps).strip() else '-'}")
    st.write(f"Temps (sec): {target_time if str(target_time).strip() else '-'}")
    st.write(f"Poids (kg): {target_weight if str(target_weight).strip() else '-'}")
    st.write(f"Repos (sec): {target_rest}")

    st.divider()
    st.subheader("Réalisé (modifiable)")
    a1, a2, a3 = st.columns(3)
    with a1:
        actual_reps = st.text_input("Reps réalisées", value=str(current_task.get("actual_reps","")).strip())
    with a2:
        actual_time = st.text_input("Temps réalisé (sec)", value=str(current_task.get("actual_time_sec","")).strip())
    with a3:
        actual_weight = st.text_input("Poids réel (kg)", value=str(current_task.get("actual_weight","")).strip())

    if st.button("Valider la tâche", type="primary"):
        # If empty actual values, fallback to target when relevant
        if not str(actual_reps).strip() and str(target_reps).strip():
            actual_reps = str(target_reps).strip()
        if not str(actual_time).strip() and str(target_time).strip():
            actual_time = str(target_time).strip()
        if not str(actual_weight).strip() and str(target_weight).strip():
            actual_weight = str(target_weight).strip()

        client.update_row_by_id("session_tasks", "task_id", current_task["task_id"], {
            "actual_reps": str(actual_reps).strip(),
            "actual_time_sec": str(actual_time).strip(),
            "actual_weight": str(actual_weight).strip(),
            "completed_at": now_iso(),
        })

        # start rest timer in session_state
        st.session_state["rest_start"] = time.time()
        st.session_state["rest_dur"] = target_rest

        st.success("Validé. Repos démarré.")
        st.rerun()

with top2:
    st.subheader("Repos")
    rest_start = st.session_state.get("rest_start", None)
    rest_dur = safe_int(st.session_state.get("rest_dur", 0), 0) or 0

    if rest_start is None or rest_dur <= 0:
        st.info("Pas de repos en cours.")
    else:
        elapsed = int(time.time() - float(rest_start))
        remaining = max(0, rest_dur - elapsed)
        st.metric("Temps restant (sec)", remaining)

        if remaining == 0:
            st.success("Repos terminé.")
        r1, r2 = st.columns([1,1])
        with r1:
            if st.button("Stop repos"):
                st.session_state["rest_start"] = None
                st.session_state["rest_dur"] = 0
                st.rerun()
        with r2:
            if st.button("Ajouter +30s"):
                st.session_state["rest_dur"] = rest_dur + 30
                st.rerun()

st.divider()
st.markdown("### À venir")

next_tasks = pending.head(10).copy()
show = next_tasks[["order_index","block_name","round_index","exercise_name_snapshot","set_index","target_reps","target_time_sec","target_weight","target_rest_sec"]].copy()
show.columns = ["ordre","bloc","round","exercice","set","reps","time_sec","poids","repos_sec"]
st.dataframe(show, use_container_width=True, height=300)
