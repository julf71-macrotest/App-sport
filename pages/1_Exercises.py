import pandas as pd
import streamlit as st

from lib.sheets import SheetClient, normalize_df
from lib.utils import uid, now_iso

st.set_page_config(page_title="Exercices", layout="wide")
st.title("Exercices")

@st.cache_resource
def get_client():
    return SheetClient.from_service_account_info(
        dict(st.secrets["google_service_account"]),
        st.secrets["sheet_id"],
    )

client = get_client()

df = normalize_df(client.read_df("exercises"))
if df.empty:
    df = pd.DataFrame(columns=[
        "exercise_id","name","media_url","media_type","muscles","notes","updated_at"
    ])

colA, colB = st.columns([2, 3], gap="large")

with colA:
    st.subheader("Liste")
    q = st.text_input("Recherche (nom ou muscle)", "")
    dff = df.copy()
    if q.strip():
        mask = dff["name"].str.contains(q, case=False, na=False) | dff["muscles"].str.contains(q, case=False, na=False)
        dff = dff[mask]
    st.dataframe(dff[["exercise_id","name","muscles","media_type"]], use_container_width=True, height=520)

    st.divider()
    st.subheader("Créer un exercice")
    if st.button("Nouveau"):
        st.session_state["selected_exercise_id"] = ""

with colB:
    st.subheader("Édition")

    options = [""] + df["exercise_id"].tolist()
    current = st.session_state.get("selected_exercise_id", "")
    selected = st.selectbox("Choisir un exercice", options, index=options.index(current) if current in options else 0)
    st.session_state["selected_exercise_id"] = selected

    if selected:
        row = df[df["exercise_id"] == selected].iloc[0].to_dict()
    else:
        row = {
            "exercise_id": "",
            "name": "",
            "media_url": "",
            "media_type": "",
            "muscles": "",
            "notes": "",
            "updated_at": "",
        }

    name = st.text_input("Nom", row["name"])
    media_url = st.text_input("Media URL (optionnel)", row["media_url"])
    media_type = st.selectbox("Type media", ["", "image", "video"], index=["", "image", "video"].index(row["media_type"] if row["media_type"] in ["", "image", "video"] else ""))
    muscles = st.text_input("Muscles (séparés par virgules)", row["muscles"])
    notes = st.text_area("Notes", row["notes"], height=120)

    if media_url and media_type == "image":
        st.image(media_url, caption="Aperçu image", use_column_width=True)
    if media_url and media_type == "video":
        st.video(media_url)

    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        if st.button("Enregistrer", type="primary"):
            if not name.strip():
                st.error("Nom obligatoire.")
            else:
                if selected:
                    ok = client.update_row_by_id(
                        "exercises", "exercise_id", selected,
                        {
                            "name": name.strip(),
                            "media_url": media_url.strip(),
                            "media_type": media_type,
                            "muscles": muscles.strip(),
                            "notes": notes.strip(),
                            "updated_at": now_iso(),
                        }
                    )
                    if ok:
                        st.success("Exercice mis à jour.")
                        st.rerun()
                    st.error("Exercice introuvable.")
                else:
                    new_id = uid("ex")
                    client.append_row_dict(
                        "exercises",
                        {
                            "exercise_id": new_id,
                            "name": name.strip(),
                            "media_url": media_url.strip(),
                            "media_type": media_type,
                            "muscles": muscles.strip(),
                            "notes": notes.strip(),
                            "updated_at": now_iso(),
                        }
                    )
                    st.success("Exercice créé.")
                    st.session_state["selected_exercise_id"] = new_id
                    st.rerun()
    with c2:
        if st.button("Supprimer", disabled=not bool(selected)):
            ok = client.delete_row_by_id("exercises", "exercise_id", selected)
            if ok:
                st.success("Supprimé.")
                st.session_state["selected_exercise_id"] = ""
                st.rerun()
            else:
                st.error("Impossible de supprimer.")
    with c3:
        if st.button("Rafraîchir"):
            st.rerun()
