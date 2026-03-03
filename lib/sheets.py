from __future__ import annotations
import time

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


@dataclass
class SheetClient:
    gc: gspread.Client
    sh: gspread.Spreadsheet

    @staticmethod
    def from_service_account_info(service_account_info: Dict[str, Any], sheet_id: str) -> "SheetClient":
        creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        return SheetClient(gc=gc, sh=sh)

    def worksheet(self, name: str) -> gspread.Worksheet:
            # Cache worksheet objects to avoid repeated metadata fetches
        if not hasattr(self, "_ws_cache"):
            self._ws_cache = {}

        key = name.strip()
        if key in self._ws_cache:
        return self._ws_cache[key]

        # 1) Try gspread native exact lookup first (most reliable)
        try:
            ws = self.sh.worksheet(key)
            self._ws_cache[key] = ws
            return ws
        except gspread.WorksheetNotFound:
            pass  # we'll do a tolerant lookup below

        # 2) Build / rebuild title map (tolerant lookup)
        def build_title_map():
            return {ws.title.strip().lower(): ws for ws in self.sh.worksheets()}

        if not hasattr(self, "_ws_title_map") or not isinstance(self._ws_title_map, dict) or len(self._ws_title_map) == 0:
            self._ws_title_map = build_title_map()

        # If still empty, force rebuild once (handles a previous bad cache build)
        if len(self._ws_title_map) == 0:
            self._ws_title_map = build_title_map()

        ws = self._ws_title_map.get(key.lower())
        if ws is None:
            titles = [w.title for w in self.sh.worksheets()]
            raise gspread.WorksheetNotFound(f"{name} (available: {titles})")

        self._ws_cache[key] = ws
        return ws
        def read_df(self, ws_name: str, ttl_sec: int = 120) -> pd.DataFrame:
            # simple in-memory cache to avoid Sheets quota bursts
            if not hasattr(self, "_df_cache"):
                self._df_cache = {}
            if not hasattr(self, "_df_cache_ts"):
            self._df_cache_ts = {}

            now = time.time()
            if ws_name in self._df_cache and (now - self._df_cache_ts.get(ws_name, 0)) < ttl_sec:
                return self._df_cache[ws_name].copy()

            ws = self.worksheet(ws_name)
            values = ws.get_all_values()
            if not values:
                df = pd.DataFrame()
            else:
                header = values[0]
                rows = values[1:]
                df = pd.DataFrame(rows, columns=header)

            self._df_cache[ws_name] = df
            self._df_cache_ts[ws_name] = now
            return df.copy()

    def invalidate_cache(self):
        if hasattr(self, "_df_cache"):
            self._df_cache = {}
        if hasattr(self, "_df_cache_ts"):
            self._df_cache_ts = {}
        if hasattr(self, "_ws_cache"):
            self._ws_cache = {}
        if hasattr(self, "_ws_title_map"):
            self._ws_title_map = {}
    
    def write_df_overwrite(self, ws_name: str, df: pd.DataFrame) -> None:
        ws = self.worksheet(ws_name)
        ws.clear()
        if df.empty:
            ws.append_row(list(df.columns))
            return
        ws.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist())

    def append_row_dict(self, ws_name: str, row: Dict[str, Any]) -> None:
        ws = self.worksheet(ws_name)
        header = ws.row_values(1)
        ordered = [str(row.get(col, "")) for col in header]
        ws.append_row(ordered)

    def update_row_by_id(
        self,
        ws_name: str,
        id_col: str,
        id_value: str,
        updates: Dict[str, Any],
    ) -> bool:
        ws = self.worksheet(ws_name)
        header = ws.row_values(1)
        try:
            id_idx = header.index(id_col) + 1
        except ValueError:
            raise ValueError(f"Column {id_col} not found in worksheet {ws_name}")

        col_values = ws.col_values(id_idx)
        # col_values includes header at index 0
        row_num = None
        for i, v in enumerate(col_values[1:], start=2):
            if v == str(id_value):
                row_num = i
                break
        if row_num is None:
            return False

        for col, val in updates.items():
            if col not in header:
                continue
            col_idx = header.index(col) + 1
            ws.update_cell(row_num, col_idx, str(val if val is not None else ""))
        return True

    def delete_row_by_id(self, ws_name: str, id_col: str, id_value: str) -> bool:
        ws = self.worksheet(ws_name)
        header = ws.row_values(1)
        if id_col not in header:
            raise ValueError(f"Column {id_col} not found in worksheet {ws_name}")
        id_idx = header.index(id_col) + 1
        col_values = ws.col_values(id_idx)

        row_num = None
        for i, v in enumerate(col_values[1:], start=2):
            if v == str(id_value):
                row_num = i
                break
        if row_num is None:
            return False

        ws.delete_rows(row_num)
        return True


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Trim spaces in headers and values
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).map(lambda x: x.strip() if isinstance(x, str) else x)
    return df
