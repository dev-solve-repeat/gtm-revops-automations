# CRM Data Cleanup Bot

A Python automation that cleans messy CRM exports — deduplicates contacts, standardises job titles, flags incomplete records, and generates a summary report. Supports HubSpot, Salesforce, and generic CRM formats.

## Features

- **Deduplication** — removes duplicates by email (exact) and by name + company (fuzzy, if `rapidfuzz` is installed); keeps the record with the most fields filled
- **Job title standardisation** — maps 30+ messy variants to canonical titles (e.g. "VP - Mktg" → "VP of Marketing")
- **Incomplete record flagging** — flags records missing required fields, invalid/generic emails, or low quality scores
- **Phone normalisation** — converts phone numbers to E.164 format (requires `phonenumbers`)
- **Multi-CRM support** — auto-detects HubSpot, Salesforce, or generic CSV/Excel/JSON exports
- **CRM merging** — optionally combine two exports (e.g. HubSpot + Salesforce) before cleaning
- **HTML report** — generates a visual summary with stats and data previews
- **Three output modes** — CSV files, separate Excel files, or a single Excel workbook with 3 sheets

## Project Structure

```
CRM_Data_Cleanup_Bot/
├── crm_cleaner.py        ← core cleaning logic (importable library + CLI)
├── app.py                ← Streamlit browser UI
├── folder_watcher.py     ← auto-clean files dropped into a folder
├── requirements.txt      ← Python dependencies
├── watch_inbox/          ← drop CRM exports here (used by folder watcher)
└── watch_output/         ← timestamped output folders (used by folder watcher)
```

## Installation

```bash
pip install -r requirements.txt
```

**Core dependencies:** `pandas`, `openpyxl`
**Optional (recommended):** `rapidfuzz` (fuzzy deduplication), `phonenumbers` (phone normalisation)
**UI only:** `streamlit`
**Folder watcher only:** `watchdog`

## Usage

### Option 1 — CLI

```bash
# Clean a file with default settings (outputs Excel workbook)
python crm_cleaner.py --input crm_raw.csv

# Choose output format
python crm_cleaner.py --input crm_raw.csv --format csv
python crm_cleaner.py --input crm_raw.csv --format xlsx
python crm_cleaner.py --input crm_raw.csv --format workbook   # default

# Merge two CRM exports before cleaning
python crm_cleaner.py --input hubspot.csv --merge salesforce.csv

# Force a specific CRM type (skips auto-detection)
python crm_cleaner.py --input export.csv --crm hubspot
```

### Option 2 — Streamlit UI

```bash
streamlit run app.py
```

Open the browser, upload a CRM export, configure settings in the sidebar, and download the results. Supports CSV, Excel, and JSON uploads. Includes live previews of clean, flagged, and duplicate records.

### Option 3 — Folder Watcher

```bash
# Watch the default watch_inbox/ folder
python folder_watcher.py

# Custom inbox folder and output format
python folder_watcher.py --inbox my_inbox --format workbook
```

Drop any CRM export into the inbox folder. The watcher detects new files automatically, runs the cleaner, and saves timestamped results to `watch_output/<timestamp>_<filename>/`. Press `Ctrl-C` to stop.

## Output Files

Each run produces:

| File | Contents |
|---|---|
| `crm_cleaned` | Deduplicated, standardised records ready for re-import |
| `crm_flagged` | Records with missing fields, invalid emails, or quality issues |
| `crm_duplicates` | Removed duplicates with `_duplicate_of` and `_duplicate_reason` columns |
| `crm_report.html` | Visual summary report with stats and data previews |

## Input Format

The script auto-detects the CRM type from column headers. It handles standard exports from:

- **HubSpot** — `First Name`, `Last Name`, `Email`, `Phone`, `Company`, `Job Title`, `City`, `Country`, `Lead Status`
- **Salesforce** — `FirstName`, `LastName`, `Email`, `Phone`, `AccountName`, `Title`, etc.
- **Generic** — any CSV/Excel/JSON with recognisable contact columns

## Customisation

Key variables in `crm_cleaner.py`:

| Variable | What it controls |
|---|---|
| `REQUIRED_FIELDS` | Fields that must be present for a record to pass |
| `TITLE_MAP` | Regex patterns mapping messy titles to canonical forms |
| `GENERIC_EMAIL_DOMAINS` | Email domains treated as invalid (e.g. `example.com`) |

## Resume Line

> Built a Python automation that cleans HubSpot/Salesforce CRM exports — deduplicates contacts (exact + fuzzy), standardises 30+ job title variants to canonical formats, and flags incomplete records. Includes a Streamlit UI and a folder watcher for fully automated processing. Reduced manual CRM cleanup from hours to seconds.
