from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize dataframe: ensure columns are strings, strip column names, keep empty df safe."""
    if df is None or not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


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

    # -------------------------
    # Cache helpers
    # -------------------------
    def invalidate_cache(self) -> None:
        if hasattr(self, "_df_cache"):
            self._df_cache = {}
        if hasattr(self, "_df_cache_ts"):
            self._df_cache_ts = {}
        if hasattr(self, "_ws_cache"):
            self._ws_cache = {}
        if hasattr(self, "_ws_title_map"):
            self._ws_title_map = {}

    # -------------------------
    # Worksheet access (robust)
    # -------------------------
    def worksheet(self, name: str) -> gspread.Worksheet:
        if not hasattr(self, "_ws_cache"):
            self._ws_cache = {}

        key = str(name).strip()
        if key in self._ws_cache:
            return self._ws_cache[key]

        # Try exact match first (fast + reliable)
        try:
            ws = self.sh.worksheet(key)
            self._ws_cache[key] = ws
            return ws
        except gspread.WorksheetNotFound:
            pass

        # Tolerant lookup by listing worksheets (can handle case/space issues)
        def build_title_map():
            return {ws.title.strip().lower(): ws for ws in self.sh.worksheets()}

        if not hasattr(self, "_ws_title_map") or not isinstance(self._ws_title_map, dict) or len(self._ws_title_map) == 0:
            self._ws_title_map = build_title_map()

        if len(self._ws_title_map) == 0:
            self._ws_title_map = build_title_map()

        ws = self._ws_title_map.get(key.lower())
        if ws is None:
            titles = [w.title for w in self.sh.worksheets()]
            raise gspread.WorksheetNotFound(f"{name} (available: {titles})")

        self._ws_cache[key] = ws
        return ws

    # -------------------------
    # Read/write helpers
    # -------------------------
    def read_df(self, ws_name: str, ttl_sec: int = 120) -> pd.DataFrame:
        # In-memory df cache to reduce Sheets quota usage
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

    def append_row_dict(self, ws_name: str, row: Dict[str, Any]) -> None:
        ws = self.worksheet(ws_name)
        headers = ws.row_values(1)

        if not headers:
            # If sheet is empty, create header row from dict keys
            headers = list(row.keys())
            ws.append_row(headers, value_input_option="RAW")

        # Ensure all keys exist in headers
        missing = [k for k in row.keys() if k not in headers]
        if missing:
            headers = headers + missing
            ws.update("1:1", [headers])

        row_values = [row.get(h, "") for h in headers]
        ws.append_row(row_values, value_input_option="RAW")

        self.invalidate_cache()

    def update_row_by_id(self, ws_name: str, id_col: str, id_value: str, updates: Dict[str, Any]) -> None:
        ws = self.worksheet(ws_name)
        values = ws.get_all_values()
        if not values:
            raise ValueError(f"Worksheet '{ws_name}' is empty")

        headers = values[0]
        try:
            id_idx = headers.index(id_col)
        except ValueError as e:
            raise ValueError(f"Column '{id_col}' not found in worksheet '{ws_name}'") from e

        target_row = None
        for i, r in enumerate(values[1:], start=2):  # 1-based sheet rows, header is row 1
            if len(r) > id_idx and str(r[id_idx]).strip() == str(id_value).strip():
                target_row = i
                break

        if target_row is None:
            raise ValueError(f"Row with {id_col}={id_value} not found in worksheet '{ws_name}'")

        # Expand headers if needed
        missing = [k for k in updates.keys() if k not in headers]
        if missing:
            headers = headers + missing
            ws.update("1:1", [headers])

        # Map header to column index (1-based)
        col_map = {h: (idx + 1) for idx, h in enumerate(headers)}

        # Update each cell
        for k, v in updates.items():
            ws.update_cell(target_row, col_map[k], v)

        self.invalidate_cache()
