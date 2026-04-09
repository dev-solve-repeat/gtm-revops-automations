"""
CRM Data Cleaner — Streamlit UI
================================
Drag-and-drop a dirty CRM export (CSV, Excel, JSON).
Optionally merge a second CRM export (e.g. HubSpot + Salesforce).
Choose your output format: CSV, Excel, or a single Excel workbook.

Run with:
  streamlit run app.py
"""

import io
import os
import tempfile

import pandas as pd
import streamlit as st

from crm_cleaner import clean, HAS_RAPIDFUZZ, HAS_PHONENUMBERS

# ─── PAGE CONFIG ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="CRM Data Cleaner",
    page_icon="🧹",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  [data-testid="stMetricValue"] { font-size: 2rem !important; }
  .stDownloadButton button { width: 100%; }
</style>
""", unsafe_allow_html=True)

# ─── SIDEBAR ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("CRM Data Cleaner")
    st.caption("Upload a dirty CRM export and get back clean, deduplicated, standardised contacts.")

    st.divider()
    st.subheader("Settings")

    crm_choice = st.selectbox(
        "CRM type",
        ["Auto-detect from headers", "HubSpot", "Salesforce", "Generic"],
        help="Leave on Auto-detect unless the script picks the wrong CRM type.",
    )
    crm_override = None if crm_choice == "Auto-detect from headers" else crm_choice.lower()

    out_format_choice = st.selectbox(
        "Output format",
        ["Excel Workbook (one file, 3 sheets)", "Separate Excel files (.xlsx)", "CSV files"],
        help=(
            "Workbook = one .xlsx with Clean / Flagged / Duplicates sheets — easiest to share.\n"
            "Separate Excel = three individual .xlsx files.\n"
            "CSV = three plain text files."
        ),
    )
    out_format_map = {
        "Excel Workbook (one file, 3 sheets)": "workbook",
        "Separate Excel files (.xlsx)":        "xlsx",
        "CSV files":                           "csv",
    }
    out_format = out_format_map[out_format_choice]

    st.divider()
    st.subheader("Merge a second CRM export (optional)")
    st.caption("Combine e.g. a HubSpot export + a Salesforce export before cleaning.")
    uploaded_merge = st.file_uploader(
        "Second CRM file (optional)",
        type=["csv", "xlsx", "xls", "json"],
        help="Leave blank to clean just the primary file.",
        label_visibility="visible",
    )

    st.divider()
    st.subheader("Library status")
    st.markdown(f"{'✅' if HAS_RAPIDFUZZ else '❌'} **rapidfuzz** — fuzzy near-duplicate detection")
    st.markdown(f"{'✅' if HAS_PHONENUMBERS else '❌'} **phonenumbers** — E.164 phone normalisation")
    if not HAS_RAPIDFUZZ or not HAS_PHONENUMBERS:
        st.info("Run `pip install rapidfuzz phonenumbers` for full features.")

    st.divider()
    st.caption("Accepted: `.csv` `.xlsx` `.xls` `.json`")

# ─── MAIN AREA ────────────────────────────────────────────────────────────────

st.header("Upload your CRM export")

uploaded = st.file_uploader(
    "Primary CRM export — drag and drop or browse",
    type=["csv", "xlsx", "xls", "json"],
    help="HubSpot export, Salesforce export, or any CRM CSV/Excel/JSON.",
    label_visibility="collapsed",
)

if uploaded is None:
    st.info(
        "Upload a CRM export above to get started. "
        "A sample file `crm_raw.csv` is in the project folder if you want to test."
    )
    st.stop()


# ── Save uploaded files to temp paths ─────────────────────────────────────────

def save_upload(file) -> str:
    suffix = os.path.splitext(file.name)[-1]
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(file.read())
    tmp.flush()
    return tmp.name


primary_tmp = save_upload(uploaded)
merge_tmp   = save_upload(uploaded_merge) if uploaded_merge else None

# ── Run the cleaner ────────────────────────────────────────────────────────────

with tempfile.TemporaryDirectory() as tmpdir:
    ext = ".xlsx" if out_format in ("xlsx", "workbook") else ".csv"
    out_clean   = os.path.join(tmpdir, f"crm_cleaned{ext}")
    out_flagged = os.path.join(tmpdir, f"crm_flagged{ext}")
    out_dupl    = os.path.join(tmpdir, f"crm_duplicates{ext}")
    out_report  = os.path.join(tmpdir, "crm_report.html")

    label = uploaded.name
    if uploaded_merge:
        label += f" + {uploaded_merge.name}"

    with st.spinner(f"Cleaning **{label}** …"):
        try:
            result = clean(
                input_path=primary_tmp,
                out_clean=out_clean,
                out_flagged=out_flagged,
                out_dupl=out_dupl,
                out_report=out_report,
                crm_override=crm_override,
                generate_report=True,
                verbose=False,
                out_format=out_format,
                merge_path=merge_tmp,
            )
        except Exception as exc:
            st.error(f"Error during cleaning: {exc}")
            os.unlink(primary_tmp)
            if merge_tmp:
                os.unlink(merge_tmp)
            st.stop()

    # ── Summary metrics ───────────────────────────────────────────────────────
    merged_note = f" (merged with {uploaded_merge.name})" if uploaded_merge else ""
    st.success(
        f"Done! Processed **{uploaded.name}**{merged_note} "
        f"as **{result['crm_type'].title()}** export."
    )
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Raw records",         result["raw_count"])
    c2.metric("Clean",               len(result["clean"]))
    c3.metric("Flagged for review",  len(result["flagged"]))
    c4.metric("Duplicates removed",  len(result["duplicates"]))
    c5.metric("Titles standardised", result["titles_changed"])
    st.divider()

    # ── Preview tabs ──────────────────────────────────────────────────────────
    tab_clean, tab_flagged, tab_dupl = st.tabs([
        f"✅  Clean  ({len(result['clean'])})",
        f"⚠️  Flagged  ({len(result['flagged'])})",
        f"🗑️  Duplicates  ({len(result['duplicates'])})",
    ])

    with tab_clean:
        if result["clean"].empty:
            st.warning("No clean records — all records were flagged or removed as duplicates.")
        else:
            st.dataframe(result["clean"], use_container_width=True, height=400)

    with tab_flagged:
        if result["flagged"].empty:
            st.success("No records flagged — everything looks good!")
        else:
            st.caption("Records with missing required fields, invalid/generic emails, or low quality scores.")
            flag_col = "_flag_reasons"
            cols_order = (
                [flag_col, "_quality_score"]
                + [c for c in result["flagged"].columns if c not in (flag_col, "_quality_score")]
            )
            cols_order = [c for c in cols_order if c in result["flagged"].columns]
            st.dataframe(result["flagged"][cols_order], use_container_width=True, height=400)

    with tab_dupl:
        if result["duplicates"].empty:
            st.success("No duplicates found.")
        else:
            st.caption("Records removed as duplicates. `_duplicate_of` and `_duplicate_reason` explain each case.")
            st.dataframe(result["duplicates"], use_container_width=True, height=400)

    st.divider()

    # ── Download buttons ──────────────────────────────────────────────────────
    st.subheader("Download outputs")

    def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
        return df.to_csv(index=False).encode("utf-8")

    def df_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        return buf.getvalue()

    def workbook_bytes(clean_df, flagged_df, dupl_df) -> bytes:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            clean_df.to_excel(writer,   sheet_name="Clean",      index=False)
            flagged_df.to_excel(writer, sheet_name="Flagged",    index=False)
            dupl_df.to_excel(writer,    sheet_name="Duplicates", index=False)
        return buf.getvalue()

    if out_format == "workbook":
        # Single workbook download
        dl1, dl2 = st.columns(2)
        dl1.download_button(
            label="⬇ Download Excel Workbook (all 3 sheets)",
            data=workbook_bytes(result["clean"], result["flagged"], result["duplicates"]),
            file_name="crm_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        with open(out_report, "r", encoding="utf-8") as fh:
            html_content = fh.read()
        dl2.download_button(
            label="⬇ HTML Report",
            data=html_content,
            file_name="crm_report.html",
            mime="text/html",
        )

    elif out_format == "xlsx":
        dl1, dl2, dl3, dl4 = st.columns(4)
        dl1.download_button(
            "⬇ Clean (.xlsx)",
            df_to_xlsx_bytes(result["clean"]),
            file_name="crm_cleaned.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        dl2.download_button(
            "⬇ Flagged (.xlsx)",
            df_to_xlsx_bytes(result["flagged"]),
            file_name="crm_flagged.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        dl3.download_button(
            "⬇ Duplicates (.xlsx)",
            df_to_xlsx_bytes(result["duplicates"]),
            file_name="crm_duplicates.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        with open(out_report, "r", encoding="utf-8") as fh:
            html_content = fh.read()
        dl4.download_button(
            "⬇ HTML Report",
            html_content,
            file_name="crm_report.html",
            mime="text/html",
        )

    else:  # csv
        dl1, dl2, dl3, dl4 = st.columns(4)
        dl1.download_button(
            "⬇ Clean CSV",
            df_to_csv_bytes(result["clean"]),
            file_name="crm_cleaned.csv",
            mime="text/csv",
        )
        dl2.download_button(
            "⬇ Flagged CSV",
            df_to_csv_bytes(result["flagged"]),
            file_name="crm_flagged.csv",
            mime="text/csv",
        )
        dl3.download_button(
            "⬇ Duplicates CSV",
            df_to_csv_bytes(result["duplicates"]),
            file_name="crm_duplicates.csv",
            mime="text/csv",
        )
        with open(out_report, "r", encoding="utf-8") as fh:
            html_content = fh.read()
        dl4.download_button(
            "⬇ HTML Report",
            html_content,
            file_name="crm_report.html",
            mime="text/html",
        )

# Cleanup temp files
os.unlink(primary_tmp)
if merge_tmp:
    os.unlink(merge_tmp)
