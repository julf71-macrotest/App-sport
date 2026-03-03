import time
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from lib.sheets import SheetClient, normalize_df
from lib.utils import now_iso, safe_int
from lib.workout_engine import build_session_tasks

st.set_page_config(page_title="Séance", layout="wide")
st.title("Séance")

# -------------------------
# Client / data access
# -------------------------
@st.cache_resource
def get_client():
    return SheetClient.from_service_account_info(
        dict(st.secrets["google_service_account"]),
        st.secrets["sheet_id"],
    )

client = get_client()

def load_df(name: str) -> pd.DataFrame:
    # NOTE: on garde la lecture simple, le cache anti-quota est dans SheetClient
    return normalize_df(client.read_df(name))

# -------------------------
# Session state init
# -------------------------
if "active_session_id" not in st.session_state:
    st.session_state["active_session_id"] = ""
if "in_rest" not in st.session_state:
    st.session_state["in_rest"] = False
if "rest_end_ts" not in st.session_state:
    st.session_state["rest_end_ts"] = None

# -------------------------
# Load base data
# -------------------------
df_prog = load_df("programs")
df_struct = load_df("program_structure")
df_ex = load_df("exercises")
df_sessions = load_df("sessions")

if df_sessions.empty:
    df_sessions = pd.DataFrame(
        columns=["session_id","program_id","program_name_snapshot","started_at","ended_at","duration_sec","status","notes"]
    )

# detect in progress sessions
in_prog = df_sessions[df_sessions["status"] == "in_progress"].copy()
if not in_prog.empty:
    in_prog = in_prog.sort_values("started_at", ascending=False)

st.markdown("### Démarrer ou reprendre")
c1, c2 = st.columns([2, 2], gap="large")

with c1:
    st.subheader("Démarrer depuis un programme")
    prog_map = {r["name"]: r["program_id"] for _, r in df_prog.iterrows() if str(r.get("name","")).strip()}
    prog_names = [""] + sorted(prog_map.keys())
    sel_prog = st.selectbox("Programme", prog_names, key="workout_prog_select")

    if st.button("Démarrer", type="primary", disabled=not bool(sel_prog), key="workout_start_btn"):
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

            # IMPORTANT: invalider les caches après écritures
            if hasattr(client, "invalidate_cache"):
                client.invalidate_cache()

            st.session_state["active_session_id"] = session_id
            st.session_state["in_rest"] = False
            st.session_state["rest_end_ts"] = None
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
        if st.button("Reprendre la séance", key="workout_resume_btn"):
            st.session_state["active_session_id"] = last["session_id"]
            st.rerun()

st.divider()

active_session_id = st.session_state.get("active_session_id", "")
if not active_session_id:
    st.info("Démarre ou reprends une séance.")
    st.stop()

# -------------------------
# Load tasks for the active session
# -------------------------
df_tasks = load_df("session_tasks")
if df_tasks.empty:
    st.error("Aucune tâche trouvée (sheet session_tasks vide).")
    st.stop()

session_tasks = df_tasks[df_tasks["session_id"].astype(str) == str(active_session_id)].copy()
if session_tasks.empty:
    st.error("Aucune tâche trouvée pour cette séance.")
    st.stop()

session_tasks["order_int"] = session_tasks["order_index"].map(lambda x: safe_int(x, 999999))
session_tasks = session_tasks.sort_values("order_int")

pending = session_tasks[session_tasks["completed_at"].astype(str).str.strip() == ""].copy()

# -------------------------
# If rest is running, show timer and auto-advance at end
# -------------------------
in_rest = bool(st.session_state.get("in_rest", False))
rest_end_ts = st.session_state.get("rest_end_ts", None)

# UI sections
st.markdown("### Exécution")
top1, top2 = st.columns([3, 2], gap="large")

with top2:
    st.subheader("Repos")

    if in_rest and rest_end_ts:
        remaining = int(max(0, float(rest_end_ts) - time.time()))
        st.metric("Temps restant (sec)", remaining)

        # Auto refresh every second only during rest
        st_autorefresh(interval=1000, key="rest_tick")

        r1, r2 = st.columns([1, 1])
        with r1:
            if st.button("Stop repos", key="rest_stop_btn"):
                st.session_state["in_rest"] = False
                st.session_state["rest_end_ts"] = None
                st.rerun()
        with r2:
            if st.button("Ajouter +30s", key="rest_add30_btn"):
                st.session_state["rest_end_ts"] = float(rest_end_ts) + 30
                st.rerun()

        if remaining <= 0:
            st.success("Repos terminé.")
            # End rest, then rerun to show next task
            st.session_state["in_rest"] = False
            st.session_state["rest_end_ts"] = None
            st.rerun()
    else:
        st.info("Pas de repos en cours.")

# -------------------------
# If no pending tasks: finish session
# -------------------------
if pending.empty:
    st.success("Toutes les tâches sont terminées.")
    if st.button("Terminer la séance", type="primary", key="finish_session_btn"):
        client.update_row_by_id("sessions", "session_id", active_session_id, {
            "ended_at": now_iso(),
            "duration_sec": "",
            "status": "done",
        })
        if hasattr(client, "invalidate_cache"):
            client.invalidate_cache()

        st.session_state["active_session_id"] = ""
        st.session_state["in_rest"] = False
        st.session_state["rest_end_ts"] = None
        st.success("Séance terminée.")
        st.rerun()
    st.stop()

# If rest is running, we do not allow validating tasks (prevents double clicks / odd states)
current_task = pending.iloc[0].to_dict()

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
        actual_reps = st.text_input(
            "Reps réalisées",
            value=str(current_task.get("actual_reps","")).strip(),
            key=f"actual_reps_{current_task['task_id']}",
            disabled=in_rest,
        )
    with a2:
        actual_time = st.text_input(
            "Temps réalisé (sec)",
            value=str(current_task.get("actual_time_sec","")).strip(),
            key=f"actual_time_{current_task['task_id']}",
            disabled=in_rest,
        )
    with a3:
        actual_weight = st.text_input(
            "Poids réel (kg)",
            value=str(current_task.get("actual_weight","")).strip(),
            key=f"actual_weight_{current_task['task_id']}",
            disabled=in_rest,
        )

    if st.button("Valider la tâche", type="primary", key=f"validate_{current_task['task_id']}", disabled=in_rest):
        # fallback to targets
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

        # IMPORTANT: invalidate caches so next task appears immediately
        if hasattr(client, "invalidate_cache"):
            client.invalidate_cache()

        # Start rest or advance immediately
        if target_rest > 0:
            st.session_state["in_rest"] = True
            st.session_state["rest_end_ts"] = time.time() + target_rest
            st.success("Validé. Repos démarré.")
        else:
            st.session_state["in_rest"] = False
            st.session_state["rest_end_ts"] = None
            st.success("Validé.")

        st.rerun()

st.divider()
st.markdown("### À venir")

next_tasks = pending.head(10).copy()
show = next_tasks[[
    "order_index","block_name","round_index","exercise_name_snapshot","set_index",
    "target_reps","target_time_sec","target_weight","target_rest_sec"
]].copy()
show.columns = ["ordre","bloc","round","exercice","set","reps","time_sec","poids","repos_sec"]

# warning streamlit: use_container_width deprecated -> width="stretch"
st.dataframe(show, width="stretch", height=300)
