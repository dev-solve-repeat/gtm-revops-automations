# HubSpot CRM Data Cleaner — Claude Code Project

## What this project does

This is a Python automation that cleans a raw HubSpot CRM export CSV file.
It deduplicates contacts, standardises job titles to canonical formats, and
flags incomplete records. Output is three clean CSV files ready for review
or re-import into HubSpot.

---

## Project structure

```
crm-cleaner/
├── CLAUDE.md              ← you are here
├── crm_cleaner.py         ← main script
├── crm_raw.csv            ← input: raw HubSpot export (messy data)
├── crm_cleaned.csv        ← output: clean, deduplicated records
├── crm_flagged.csv        ← output: records with missing required fields
└── crm_duplicates.csv     ← output: removed duplicates (audit trail)
```

---

## How to run

```bash
# Install dependency
pip install pandas

# Run with default input file (crm_raw.csv)
python crm_cleaner.py

# Run with a custom HubSpot export file
python crm_cleaner.py --input your_hubspot_export.csv
```

---

## What the script does — step by step

1. Loads the CSV and normalises name casing (title case) and email casing (lowercase)
2. Standardises job titles using a regex map (e.g. "VP - Mktg" → "VP of Marketing")
3. Deduplicates by email first, then by First Name + Last Name + Company for records with no email. When duplicates exist, keeps the record with the most fields filled in.
4. Flags any record missing one or more required fields: First Name, Last Name, Email, Company
5. Saves three output CSVs and prints a summary report

---

## Input file format

The script expects a CSV with these exact column headers (standard HubSpot export format):

```
First Name, Last Name, Email, Phone, Company, Job Title, City, Country, Lead Status
```

If your HubSpot export has different column names, update the `REQUIRED_FIELDS`
list and the column references in `crm_cleaner.py` accordingly.

---

## Key variables to customise

In `crm_cleaner.py`, these are the main things you may want to change:

| Variable | What it controls |
|---|---|
| `REQUIRED_FIELDS` | Which fields must be present for a record to pass (not be flagged) |
| `TITLE_MAP` | The regex patterns that map messy titles to canonical ones |
| `INPUT_FILE` | Default input filename if no --input argument is passed |
| `OUTPUT_CLEAN` | Filename for the cleaned output |
| `OUTPUT_FLAGGED` | Filename for the flagged incomplete records |
| `OUTPUT_DUPLICATES` | Filename for the removed duplicates audit file |

---

## How to extend this project

These are good next steps to make this project more impressive:

### Add more job title mappings
Open `CLAUDE.md` and ask Claude Code:
> "Add 10 more job title variants to the TITLE_MAP in crm_cleaner.py — include common abbreviations for Director, Manager, and Head of roles"

### Add a summary HTML report
> "Add a function that generates a simple HTML report showing: total records processed, duplicates removed, titles standardised, and flagged records — save it as crm_report.html"

### Connect to HubSpot API
> "Add an optional --upload flag that takes the cleaned CSV and pushes the records to HubSpot using the HubSpot Contacts API"

### Add email validation
> "Add a step that validates email format using regex and flags records where the email does not match a valid email pattern"

### Add a Streamlit UI
> "Wrap the script in a Streamlit app so I can upload a CSV through a browser, see the results in a table, and download the outputs"

---

## Skills this project demonstrates (for your resume/portfolio)

- CRM data operations and hygiene (core RevOps skill)
- Python scripting with pandas
- Regex-based data standardisation
- Deduplication logic with priority rules
- Structured output and audit trails
- CLI tool design with argparse

---

## Resume line for this project

> Built a Python automation that cleans HubSpot CRM exports — deduplicates
> contacts, standardises 30+ job title variants to canonical formats, and flags
> incomplete records. Reduced manual CRM cleanup from hours to seconds.

---

## Suggested Claude Code prompts to use while building

Start here if you are building this from scratch in VS Code:

**To generate the sample messy data:**
> "Create a crm_raw.csv with 20 messy HubSpot-style contact records — include duplicates with slightly different job titles, some records with missing first name, last name, or email, and inconsistent job title formats like VP, V.P., Vice President"

**To build the cleaner script:**
> "Write a Python script called crm_cleaner.py that reads crm_raw.csv, deduplicates contacts by email (keeping the record with the most fields filled), standardises job titles using a regex map, flags records missing First Name / Last Name / Email / Company, and saves three output CSVs: crm_cleaned.csv, crm_flagged.csv, and crm_duplicates.csv. Print a summary at the end."

**To test it:**
> "Run crm_cleaner.py and show me the output. Then check if Arjun Mehta appears only once in crm_cleaned.csv and confirm his job title was standardised correctly."

**To add the HTML report:**
> "Add a generate_report() function to crm_cleaner.py that creates an HTML file showing the cleanup summary stats and a preview of the first 5 rows of each output file."
