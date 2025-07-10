"""
excel_cleaner.py
Clean finance exports for *any* LOB / region in one go.
----------------------------------------------------------------
Usage (inside a notebook / script):

    from excel_cleaner import clean_workbook, save_outputs

    out = clean_workbook(
        workbook="Raw international financial data_FY22-25.xlsx",
        sheet    ="FY25 Mar Est - Europe (2)",
        lob_map  ={
            "Products"  : r"Product",           # regex(es) that identify rows
            "TV Sales"  : r"TV Sales",
            "Production": r"Production",
            "HV"        : r"\bHV\b",            # Home-Video block
            "Streaming" : r"Streaming",
        },
        one_row_per_territory=False            # set True for the “split” mode
    )

    save_outputs(out, "outputs/clean_FY25_allLOB_EU")      # csv + xlsx
"""

from __future__ import annotations

import itertools
import math
import re
import string
from collections import defaultdict
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.api.types import is_object_dtype, is_string_dtype

# ─────────────────────────────── CONSTANTS ────────────────────────────────
BRACKETS = [
    (25, "0-25K"),
    (49, "26-49K"),
    (100, "50-100K"),
    (249, "101-249K"),
    (499, "250-499K"),
    (math.inf, "500+"),
]
BINS, LABELS = zip(*BRACKETS, strict=False)

# final column order identical for *all* LOBs
FINAL_COLS = [
    "Region",
    "LOB",
    "Combined SW #",
    "Partner",
    "Category",
    "Primary territory",
    "Start date",
    "End date",
    "Bracket",
    "Lifecycle",
    "High / Med/Low touch",
    "Details",
    "Net revenue",
    *LABELS,
]

_RENAME = {  # raw → clean column names
    "Licensee": "Partner",
    "Product Category": "Category",
    # "Product Category/Show / Right": "Category",
    "Primary Territory": "Primary territory",
    "SW #": "Combined SW #",
    "FY25 Mar Est Total Net (US$)": "Net revenue",
    # "Net Revenue": "Net revenue",
    "Start Date": "Start date",
    "End Date": "End date",
}


# ──────────────────────────── LOW-LEVEL HELPERS ───────────────────────────
def _detect_header(bytes_or_path, sheet, marker="Sub Dept", nrows=60) -> int:
    tmp = pd.read_excel(bytes_or_path, sheet, header=None, nrows=nrows)
    row = tmp.index[tmp.iloc[:, 0].astype(str).str.contains(marker, na=False)]
    if row.empty:
        raise ValueError("Header with column 'Sub Dept' not found.")
    return int(row[0])


def _tidy_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading / trailing blanks **only** for true string scalars."""
    obj_cols = [c for c in df.columns if is_object_dtype(df[c]) or is_string_dtype(df[c])]
    for c in obj_cols:
        df[c] = df[c].apply(lambda v: v.strip() if isinstance(v, str) else v)
    return df


def _aggregate_exact(df: pd.DataFrame) -> pd.DataFrame:
    """Roll-up truly duplicate rows but:
    *Keep one row per territory for ‘New Business’ placeholder deals*."""
    base_key = ["Partner", "Combined SW #", "Category", "Start date", "End date"]
    nb_mask = df["Partner"].str.lower() == "new business"

    # 1) normal rows  – concat distinct territories, sum revenue
    regular = (
        df.loc[~nb_mask]
        .groupby(base_key, dropna=False, as_index=False)
        .agg({"Net revenue": "sum", "Primary territory": lambda s: ", ".join(sorted(set(s.dropna())))})
    )
    # 2) NB rows      – *keep* territory in the key
    nb_key = base_key + ["Primary territory"]
    newbiz = df.loc[nb_mask].groupby(nb_key, dropna=False, as_index=False).agg({"Net revenue": "sum"})
    return pd.concat([regular, newbiz], ignore_index=True)


def _split_territories(df: pd.DataFrame) -> pd.DataFrame:
    """Duplicate a row per comma-separated territory, divide revenue equally."""
    out = []
    for _, r in df.iterrows():
        terrs = [t.strip() for t in str(r["Primary territory"]).split(",") if t.strip()]
        if len(terrs) <= 1:  # nothing to split
            out.append(r)
            continue
        share = r["Net revenue"] / len(terrs) if pd.notnull(r["Net revenue"]) else np.nan
        for terr in terrs:
            nr = r.copy()
            nr["Primary territory"] = terr
            nr["Net revenue"] = share
            out.append(nr)
    return pd.DataFrame(out)


def _rollup_fx(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse all FX Gain/Loss lines into one."""
    m = df["Partner"].str.contains(r"^FX Gain", case=False, na=False)
    if m.sum() <= 1:
        return df
    total = df.loc[m, "Net revenue"].sum()
    row = df.loc[m].iloc[0].copy()
    row["Net revenue"] = total
    row["Primary territory"] = "Benelux"
    return pd.concat([df.loc[~m], row.to_frame().T], ignore_index=True)


def _assign_ids(df: pd.DataFrame) -> pd.DataFrame:
    id_counter, blank_it, tbd_it, out_ids = defaultdict(int), itertools.count(1), itertools.count(1), []

    for base_raw, partner in zip(df["_base"], df["Partner"], strict=False):
        base = str(base_raw).strip().upper()
        # -------- placeholders ----------
        if base in {"", "NAN", "NONE", "TB", "TBD"}:
            if partner.strip().lower() == "new business":
                out_ids.append(f"Blank {next(blank_it)}")
            elif partner.lower().startswith("fx gain"):
                out_ids.append("FX Gain(Loss)")
            else:
                out_ids.append(f"TBD{next(tbd_it)}")
            continue
        # -------- real SW number --------
        seq = id_counter[base]
        suffix = "" if seq == 0 else string.ascii_uppercase[seq - 1]
        out_ids.append(f"{base}{suffix}")
        id_counter[base] += 1

    df["Combined SW #"] = out_ids
    return df


def _add_brackets(df: pd.DataFrame) -> pd.DataFrame:
    df["Bracket"] = pd.cut(df["Net revenue"], bins=[-np.inf, *BINS], labels=LABELS)
    for lab in LABELS:
        df[lab] = (df["Bracket"] == lab).astype(int)
    return df


# ───────────────────────────── CORE TRANSFORM ─────────────────────────────
def _transform_subset(df: pd.DataFrame, lob_name: str, one_row_per_territory: bool) -> pd.DataFrame:

    # ------------- aggregate / split -----------------
    df = _aggregate_exact(df)
    if one_row_per_territory:
        df = _split_territories(df)

    # ------------- static cols -----------------------
    df["Region"] = "EUROPE"  # <- quick default; override later if needed
    df["LOB"] = lob_name

    # ------------- Combined-SW assignment ------------
    df["_base"] = df["Combined SW #"].fillna("").astype(str).str.replace(r"[A-Z]$", "", regex=True)
    df = df.sort_values(["_base", "Partner", "Primary territory"])
    df = _assign_ids(df)

    # ------------- add blanks for user columns -------
    for col in ("Lifecycle", "High / Med/Low touch", "Details"):
        df[col] = ""

    # ------------- final polish ----------------------
    df = _rollup_fx(df)
    df = _add_brackets(df)

    return df[FINAL_COLS]


# ───────────────────────── PUBLIC ENTRY POINT ─────────────────────────────
def clean_workbook(
    workbook: str | Path | bytes | BytesIO,
    sheet: str,
    lob_map: dict[str, str] | None = None,
    one_row_per_territory: bool = False,
) -> pd.DataFrame:
    """
    Parameters
    ----------
    workbook
        Path, bytes, or BytesIO of the raw Excel file.
    sheet
        Worksheet name (exact, case-sensitive like in Excel tab).
    lob_map
        Dict → { "LOB label": r"regex pattern to match Sub Dept" }.
        If *None*, will auto-discover each unique `Sub Dept` root word.
    one_row_per_territory
        *False*  ⇒ keep comma-separated territories in one row (original “Products” mode);
        *True*   ⇒ explode each comma into its own row (original “per-territory” mode).

    Returns
    -------
    pandas.DataFrame - all requested LOBs stacked together & cleaned.
    """
    # ---- load sheet (works for paths *and* bytes, no FutureWarning) ----
    xlsx = BytesIO(workbook) if isinstance(workbook, (bytes, bytearray)) else workbook
    hdr = _detect_header(xlsx, sheet)
    print(hdr, "header row detected")
    raw = pd.read_excel(xlsx, sheet, header=hdr)
    standardized_col_map = {
        col: col.replace('\n', ' ').strip() for col in raw.columns
    }
    raw = raw.rename(columns=standardized_col_map)
    raw = raw.rename(columns=_RENAME)
    raw = _tidy_strings(raw)

    # drop rows w/o category (= obvious placeholders)
    raw = raw[raw["Category"].notna() & (raw["Category"].str.len() > 0)]

    # exclude subtotal / grand-total lines
    total_mask = raw["Sub Dept"].str.contains("Total", case=False, na=False)
    raw = raw[~total_mask].copy()

    # ensure dates
    for col in ("Start date", "End date"):
        raw[col] = pd.to_datetime(raw[col], errors="coerce")

    # if lob_map is None:  # auto-discover ← more tolerant regex
    #     lob_series = raw["Sub Dept"].apply(lambda x: x.split("-", 1)[1].strip())
    #     lob_map = {lob: rf"{re.escape(lob)}" for lob in lob_series.dropna().unique()}
    if lob_map is None:  # auto-discover ← more tolerant regex
        def get_lob_name(sub_dept_string):
            sub_dept_string = str(sub_dept_string).strip()
            if not sub_dept_string:
                return None # Handle empty strings or NaNs

            # Try to split by hyphen first (e.g., "Dept - TV Sales" -> "TV Sales")
            if '-' in sub_dept_string:
                return sub_dept_string.split("-", 1)[1].strip()
            
            # If no hyphen, for cases like "Dept. 143 Interactive",
            # we assume the LOB is the last word/part after any leading identifiers.
            # This handles "Dept. 143 Interactive" -> "Interactive"
            words = sub_dept_string.split(' ')
            if len(words) > 1:
                return words[-1].strip()
                
            return sub_dept_string.strip() # Fallback for single-word entries or unexpected formats

        lob_series = raw["Sub Dept"].apply(get_lob_name)
        lob_map = {lob: rf"{re.escape(lob)}" for lob in lob_series.dropna().unique()}

    frames = []
    for lob, pattern in lob_map.items():
        subset = raw[raw["Sub Dept"].str.contains(pattern, case=False, na=False)].copy()
        if subset.empty:
            continue  # nothing matched – skip
        frames.append(_transform_subset(subset, lob, one_row_per_territory))

    if not frames:
        raise ValueError("No rows matched any of the supplied LOB patterns.")

    combined = pd.concat(frames, ignore_index=True)
    return combined[FINAL_COLS]


# ─────────────────────────── SAVE CONVENIENCE ─────────────────────────────
def save_outputs(df: pd.DataFrame, stem: str, output_dir: str | Path = "outputs") -> None:
    """
    Write *stem*.csv and *stem*.xlsx (folder auto-created).
    """
    outdir = Path(output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    csv_p = outdir / f"{Path(stem).stem}.csv"
    xlsx_p = outdir / f"{Path(stem).stem}.xlsx"

    df.to_csv(csv_p, index=False)
    with pd.ExcelWriter(xlsx_p, engine="openpyxl") as xls:
        df.to_excel(xls, sheet_name="AUTO_CLEAN", index=False)

    print("✔ Saved:", csv_p, "and", xlsx_p)
