import pandas as pd
import streamlit as st

from lib.sheets import SheetClient, normalize_df
from lib.utils import uid, now_iso, safe_int, safe_float

st.set_page_config(page_title="Programmes", layout="wide")
st.title("Programmes")

@st.cache_resource
def get_client():
    return SheetClient.from_service_account_info(
        dict(st.secrets["google_service_account"]),
        st.secrets["sheet_id"],
    )

client = get_client()

df_prog = normalize_df(client.read_df("programs"))
if df_prog.empty:
    df_prog = pd.DataFrame(columns=["program_id","name","description","updated_at"])

df_struct = normalize_df(client.read_df("program_structure"))
if df_struct.empty:
    df_struct = pd.DataFrame(columns=[
        "row_id","program_id","type","parent_block_id","order_index","block_name","rounds",
        "exercise_id","sets","reps_target","time_target_sec","weight_target","rest_sec","notes"
    ])

df_ex = normalize_df(client.read_df("exercises"))
if df_ex.empty:
    df_ex = pd.DataFrame(columns=["exercise_id","name","media_url","media_type","muscles","notes","updated_at"])

ex_options = {r["name"]: r["exercise_id"] for _, r in df_ex.iterrows() if str(r.get("name","")).strip()}
ex_names_sorted = [""] + sorted(ex_options.keys())

left, right = st.columns([2, 5], gap="large")

with left:
    st.subheader("Liste des programmes")
    st.dataframe(df_prog[["program_id","name"]], use_container_width=True, height=280)

    st.divider()
    st.subheader("Créer")
    new_name = st.text_input("Nom du nouveau programme", "")
    new_desc = st.text_area("Description", "", height=80)
    if st.button("Créer le programme"):
        if not new_name.strip():
            st.error("Nom obligatoire.")
        else:
            pid = uid("prog")
            client.append_row_dict("programs", {
                "program_id": pid,
                "name": new_name.strip(),
                "description": new_desc.strip(),
                "updated_at": now_iso(),
            })
            st.success("Programme créé.")
            st.session_state["selected_program_id"] = pid
            st.rerun()

with right:
    st.subheader("Édition programme")

    prog_ids = [""] + df_prog["program_id"].tolist()
    current = st.session_state.get("selected_program_id", "")
    selected_pid = st.selectbox("Choisir", prog_ids, index=prog_ids.index(current) if current in prog_ids else 0)
    st.session_state["selected_program_id"] = selected_pid

    if not selected_pid:
        st.info("Choisis ou crée un programme.")
        st.stop()

    prog_row = df_prog[df_prog["program_id"] == selected_pid].iloc[0].to_dict()
    pname = st.text_input("Nom", prog_row["name"])
    pdesc = st.text_area("Description", prog_row["description"], height=80)

    if st.button("Sauvegarder infos programme", type="primary"):
        client.update_row_by_id("programs", "program_id", selected_pid, {
            "name": pname.strip(),
            "description": pdesc.strip(),
            "updated_at": now_iso(),
        })
        st.success("Sauvegardé.")
        st.rerun()

    st.divider()

    dfp = df_struct[df_struct["program_id"].astype(str) == str(selected_pid)].copy()

    blocks = dfp[dfp["type"] == "block"].copy()
    blocks["order_index_int"] = blocks["order_index"].map(lambda x: safe_int(x, 9999))
    blocks = blocks.sort_values("order_index_int")

    st.markdown("### Blocs")
    with st.expander("Ajouter un bloc", expanded=False):
        bname = st.text_input("Nom du bloc", "")
        border = st.number_input("Ordre", min_value=1, max_value=999, value=1)
        brounds = st.number_input("Rounds (tours)", min_value=1, max_value=100, value=1)
        if st.button("Ajouter bloc"):
            if not bname.strip():
                st.error("Nom obligatoire.")
            else:
                bid = uid("blk")
                client.append_row_dict("program_structure", {
                    "row_id": bid,
                    "program_id": selected_pid,
                    "type": "block",
                    "parent_block_id": "",
                    "order_index": int(border),
                    "block_name": bname.strip(),
                    "rounds": int(brounds),
                    "exercise_id": "",
                    "sets": "",
                    "reps_target": "",
                    "time_target_sec": "",
                    "weight_target": "",
                    "rest_sec": "",
                    "notes": "",
                })
                st.success("Bloc ajouté.")
                st.rerun()

    if blocks.empty:
        st.warning("Aucun bloc. Ajoute un bloc.")
        st.stop()

    for _, b in blocks.iterrows():
        block_id = str(b["row_id"])
        block_label = f"{safe_int(b.get('order_index'), 0)}. {b.get('block_name','Block')} (rounds: {safe_int(b.get('rounds'),1)})"
        with st.expander(block_label, expanded=True):
            c1, c2, c3 = st.columns([3, 1, 1])
            with c1:
                new_block_name = st.text_input("Nom", str(b.get("block_name","")), key=f"bn_{block_id}")
            with c2:
                new_order = st.number_input("Ordre", min_value=1, max_value=999, value=safe_int(b.get("order_index"),1), key=f"bo_{block_id}")
            with c3:
                new_rounds = st.number_input("Rounds", min_value=1, max_value=100, value=safe_int(b.get("rounds"),1), key=f"br_{block_id}")

            cc1, cc2 = st.columns([1,1])
            with cc1:
                if st.button("Sauver bloc", key=f"saveb_{block_id}"):
                    client.update_row_by_id("program_structure", "row_id", block_id, {
                        "block_name": new_block_name.strip(),
                        "order_index": int(new_order),
                        "rounds": int(new_rounds),
                    })
                    st.success("Bloc mis à jour.")
                    st.rerun()
            with cc2:
                if st.button("Supprimer bloc + items", key=f"delb_{block_id}"):
                    # delete items in df then overwrite for simplicity
                    df_all = df_struct.copy()
                    mask_prog = df_all["program_id"].astype(str) == str(selected_pid)
                    mask_block = df_all["row_id"].astype(str) == block_id
                    mask_items = df_all["parent_block_id"].astype(str) == block_id
                    df_all = df_all[~(mask_prog & (mask_block | mask_items))]
                    client.write_df_overwrite("program_structure", df_all)
                    st.success("Bloc supprimé.")
                    st.rerun()

            st.markdown("#### Items")
            items = dfp[(dfp["type"] == "item") & (dfp["parent_block_id"].astype(str) == block_id)].copy()
            items["order_index_int"] = items["order_index"].map(lambda x: safe_int(x, 9999))
            items = items.sort_values("order_index_int")

            if not items.empty:
                show_cols = ["row_id","order_index","exercise_id","sets","reps_target","time_target_sec","weight_target","rest_sec","notes"]
                st.dataframe(items[show_cols], use_container_width=True, height=180)

            with st.expander("Ajouter un item", expanded=False):
                iorder = st.number_input("Ordre item", min_value=1, max_value=999, value=1, key=f"io_{block_id}")
                ex_name = st.selectbox("Exercice", ex_names_sorted, key=f"iex_{block_id}")
                sets = st.number_input("Sets", min_value=1, max_value=50, value=1, key=f"is_{block_id}")
                reps = st.text_input("Reps cible (vide si temps)", "", key=f"ir_{block_id}")
                time_sec = st.text_input("Temps cible (sec) (vide si reps)", "", key=f"it_{block_id}")
                weight = st.text_input("Poids cible (kg) optionnel", "", key=f"iw_{block_id}")
                rest = st.number_input("Repos (sec)", min_value=0, max_value=900, value=60, key=f"ires_{block_id}")
                notes = st.text_input("Notes", "", key=f"in_{block_id}")

                if st.button("Ajouter item", key=f"additem_{block_id}"):
                    if not ex_name:
                        st.error("Choisis un exercice.")
                    else:
                        iid = uid("it")
                        client.append_row_dict("program_structure", {
                            "row_id": iid,
                            "program_id": selected_pid,
                            "type": "item",
                            "parent_block_id": block_id,
                            "order_index": int(iorder),
                            "block_name": "",
                            "rounds": "",
                            "exercise_id": ex_options[ex_name],
                            "sets": int(sets),
                            "reps_target": reps.strip(),
                            "time_target_sec": time_sec.strip(),
                            "weight_target": weight.strip(),
                            "rest_sec": int(rest),
                            "notes": notes.strip(),
                        })
                        st.success("Item ajouté.")
                        st.rerun()

            # Quick edit items
            st.markdown("#### Modifier un item")
            item_ids = [""] + items["row_id"].tolist()
            sel_item = st.selectbox("Item", item_ids, key=f"selitem_{block_id}")
            if sel_item:
                r = items[items["row_id"] == sel_item].iloc[0].to_dict()
                ex_id = str(r.get("exercise_id",""))
                ex_name_current = ""
                for n, eid in ex_options.items():
                    if eid == ex_id:
                        ex_name_current = n
                        break

                e1, e2, e3 = st.columns([2,1,1])
                with e1:
                    ex_name_new = st.selectbox("Exercice", ex_names_sorted, index=ex_names_sorted.index(ex_name_current) if ex_name_current in ex_names_sorted else 0, key=f"edit_ex_{sel_item}")
                with e2:
                    order_new = st.number_input("Ordre", min_value=1, max_value=999, value=safe_int(r.get("order_index"),1), key=f"edit_ord_{sel_item}")
                with e3:
                    sets_new = st.number_input("Sets", min_value=1, max_value=50, value=safe_int(r.get("sets"),1), key=f"edit_sets_{sel_item}")

                f1, f2, f3, f4 = st.columns([1,1,1,1])
                with f1:
                    reps_new = st.text_input("Reps", str(r.get("reps_target","")), key=f"edit_reps_{sel_item}")
                with f2:
                    time_new = st.text_input("Temps sec", str(r.get("time_target_sec","")), key=f"edit_time_{sel_item}")
                with f3:
                    weight_new = st.text_input("Poids", str(r.get("weight_target","")), key=f"edit_w_{sel_item}")
                with f4:
                    rest_new = st.number_input("Repos sec", min_value=0, max_value=900, value=safe_int(r.get("rest_sec"),60) or 60, key=f"edit_rest_{sel_item}")

                notes_new = st.text_input("Notes", str(r.get("notes","")), key=f"edit_notes_{sel_item}")

                g1, g2 = st.columns([1,1])
                with g1:
                    if st.button("Sauver item", key=f"save_item_{sel_item}"):
                        client.update_row_by_id("program_structure", "row_id", sel_item, {
                            "exercise_id": ex_options.get(ex_name_new, ""),
                            "order_index": int(order_new),
                            "sets": int(sets_new),
                            "reps_target": reps_new.strip(),
                            "time_target_sec": time_new.strip(),
                            "weight_target": weight_new.strip(),
                            "rest_sec": int(rest_new),
                            "notes": notes_new.strip(),
                        })
                        st.success("Item mis à jour.")
                        st.rerun()
                with g2:
                    if st.button("Supprimer item", key=f"del_item_{sel_item}"):
                        df_all = df_struct.copy()
                        df_all = df_all[~(df_all["row_id"].astype(str) == str(sel_item))]
                        client.write_df_overwrite("program_structure", df_all)
                        st.success("Item supprimé.")
                        st.rerun()
