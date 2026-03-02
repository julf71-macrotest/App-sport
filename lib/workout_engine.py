from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd

from lib.utils import uid, now_iso, safe_int, safe_float


def build_session_tasks(
    program_id: str,
    program_name: str,
    df_structure: pd.DataFrame,
    df_exercises: pd.DataFrame,
) -> Tuple[str, List[Dict]]:
    """
    Generate session_tasks rows from program_structure.
    Supports:
    - blocks with rounds
    - items with sets and reps or time
    """
    session_id = uid("sess")

    df_structure = df_structure.copy()
    df_exercises = df_exercises.copy()

    # Build lookup for exercise name
    ex_name = {}
    if not df_exercises.empty:
        for _, r in df_exercises.iterrows():
            ex_name[str(r.get("exercise_id", ""))] = str(r.get("name", "")).strip()

    # Filter by program
    dfp = df_structure[df_structure["program_id"].astype(str) == str(program_id)].copy()
    if dfp.empty:
        return session_id, []

    # Identify blocks
    blocks = dfp[dfp["type"].astype(str) == "block"].copy()
    blocks["order_index_int"] = blocks["order_index"].map(lambda x: safe_int(x, 9999))
    blocks = blocks.sort_values("order_index_int")

    tasks: List[Dict] = []
    order_index = 0

    for _, b in blocks.iterrows():
        block_id = str(b.get("row_id", "")).strip()
        block_name = str(b.get("block_name", "")).strip() or "Block"
        rounds = safe_int(b.get("rounds", 1), 1) or 1

        items = dfp[(dfp["type"].astype(str) == "item") & (dfp["parent_block_id"].astype(str) == block_id)].copy()
        items["order_index_int"] = items["order_index"].map(lambda x: safe_int(x, 9999))
        items = items.sort_values("order_index_int")

        for round_idx in range(1, rounds + 1):
            for _, it in items.iterrows():
                exercise_id = str(it.get("exercise_id", "")).strip()
                sets = safe_int(it.get("sets", 1), 1) or 1

                reps_target = safe_int(it.get("reps_target"), None)
                time_target_sec = safe_int(it.get("time_target_sec"), None)
                weight_target = safe_float(it.get("weight_target"), None)
                rest_sec = safe_int(it.get("rest_sec"), 0) or 0

                for set_idx in range(1, sets + 1):
                    order_index += 1
                    tasks.append(
                        {
                            "task_id": uid("task"),
                            "session_id": session_id,
                            "order_index": order_index,
                            "block_name": block_name,
                            "round_index": round_idx,
                            "exercise_id": exercise_id,
                            "exercise_name_snapshot": ex_name.get(exercise_id, ""),
                            "set_index": set_idx,
                            "target_reps": reps_target if reps_target is not None else "",
                            "target_time_sec": time_target_sec if time_target_sec is not None else "",
                            "target_weight": weight_target if weight_target is not None else "",
                            "target_rest_sec": rest_sec,
                            "actual_reps": "",
                            "actual_time_sec": "",
                            "actual_weight": "",
                            "completed_at": "",
                        }
                    )

    return session_id, tasks
