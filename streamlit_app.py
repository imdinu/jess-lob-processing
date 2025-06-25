# app.py ---------------------------------------------------------------
"""
Interactive cleaner for Sesame finance exports
----------------------------------------------

* Upload the raw Excel workbook.
* Pick the worksheet you want to process.
* Decide whether you want **one row per territory** or the original
  “comma separated” behaviour.
* Inspect the cleaned frame directly in the browser.
* One-click download as **CSV** or **XLSX** (no temp files – we stream
  everything from memory).

Requires:  streamlit, pandas, openpyxl  (plus the standard deps
           already used inside sw_cleaner.py)
"""
from __future__ import annotations

from io import BytesIO

import pandas as pd
import streamlit as st

from excel_cleaner import clean_workbook  # <- the module we crafted earlier

# ------------- streamlit layout ------------------------------------------
st.set_page_config("Finance Cleaner", layout="wide")
st.title("SW Finance Cleaner")

col1, col2 = st.columns([0.5, 0.5])
col1.subheader("Coonfiguration")
uploaded = col1.file_uploader("Upload raw Excel workbook", type=["xlsx"])
if not uploaded:
    col1.info("… waiting for file ☝️")
    col1.stop()

# ---------- discover sheet names -----------------------------------------
try:
    xls = pd.ExcelFile(uploaded)  # Reads from UploadedFile stream
except Exception as exc:
    col1.error(f"❌ Could not read workbook: {exc}")
    col1.stop()

sheet_name = col1.selectbox("Choose worksheet", xls.sheet_names)

split_flag = col1.checkbox("One row **per territory** (split comma-lists)", value=False)

# -------------------------------------------------------------------------
if st.button("🚀 Transform"):
    with st.spinner("Crunching numbers…"):
        try:
            df = clean_workbook(
                workbook=uploaded.getvalue(),  # bytes (no warning)
                sheet=sheet_name,
                lob_map=None,  # auto-detect *all* LOBs
                one_row_per_territory=split_flag,
            )
        except Exception as exc:  # any parsing / cleaning issue
            st.exception(exc)
            st.stop()

    df_summary = df.groupby("LOB").agg(
        {"Net revenue": "sum", "Primary territory": "nunique", "Combined SW #": "count"}
    ).rename(
        columns={
            "Net revenue": "Total net revenue",
            "Primary territory": "Territories",
            "Combined SW #": "Combined SW # count",
        }
    ).reset_index().sort_values("Total net revenue", ascending=False).reset_index(drop=True)
    col2.subheader("Summary of LOBs")
    col2.dataframe(df_summary, use_container_width=True, height=300)

    st.success(f"Done! {len(df):,} rows · " f"{df['Net revenue'].sum():,.0f} net revenue total")
    st.dataframe(df, use_container_width=True, height=500)

    # ---------- download helpers -----------------------------------------
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    excel_bytes = BytesIO()
    with pd.ExcelWriter(excel_bytes, engine="openpyxl") as xlw:
        df.to_excel(xlw, index=False, sheet_name="AUTO_CLEAN")
    excel_bytes.seek(0)

    st.info("Download cleaned data:")
    col1, col2 = st.columns([0.5, 0.5])
    col21, col22 = col1.columns([0.5, 0.5])
    col21.download_button("⬇️  CSV", data=csv_bytes, file_name="cleaned_data.csv", mime="text/csv", use_container_width=True)
    col22.download_button(
        "⬇️  XLSX",
        data=excel_bytes,
        file_name="cleaned_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
