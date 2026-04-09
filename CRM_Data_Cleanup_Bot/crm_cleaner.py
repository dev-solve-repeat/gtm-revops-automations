#!/usr/bin/env python3
"""
CRM Data Cleaner
================
Cleans dirty CRM exports from HubSpot, Salesforce, or any generic CSV / Excel / JSON.

Features
--------
- Auto-detect CRM type from column headers (HubSpot, Salesforce, Generic)
- Support CSV (.csv), Excel (.xlsx/.xls), and JSON input
- Normalise name, email, company casing
- Standardise 50+ job title variants to canonical forms (VP → Vice President, etc.)
- Normalise phone numbers to E.164 format   [requires: pip install phonenumbers]
- Normalise country name variants (USA → United States, UK → United Kingdom, etc.)
- Deduplicate by exact email, then by fuzzy name+company  [requires: pip install rapidfuzz]
- Enrich keeper records: pull missing fields from duplicate donors before discarding
- Infer Company from email domain when Company field is blank
- Flag records missing required fields, with invalid/generic emails, or low quality score
- Data quality score (0–100) per record based on 10 key field completeness
- Three output CSVs: cleaned, flagged, duplicates (with audit columns)
- Optional HTML summary report

Usage
-----
  python crm_cleaner.py                          # default input: crm_raw.csv
  python crm_cleaner.py --input export.csv
  python crm_cleaner.py --input export.xlsx
  python crm_cleaner.py --input export.json
  python crm_cleaner.py --crm salesforce         # override CRM detection
  python crm_cleaner.py --no-report              # skip HTML report
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("Error: pandas is required. Run: pip install pandas")
    sys.exit(1)

# Optional libraries — script works without them, with reduced functionality
try:
    from rapidfuzz import fuzz
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False

try:
    import phonenumbers
    HAS_PHONENUMBERS = True
except ImportError:
    HAS_PHONENUMBERS = False


# ─── CONFIG ───────────────────────────────────────────────────────────────────

INPUT_FILE        = "crm_raw.csv"
WATCH_OUTPUT_DIR  = "watch_output"
OUTPUT_CLEAN      = "crm_cleaned.csv"
OUTPUT_FLAGGED    = "crm_flagged.csv"
OUTPUT_DUPLICATES = "crm_duplicates.csv"
OUTPUT_REPORT     = "crm_report.html"

# Fields that must be non-empty for a record to be considered "clean"
REQUIRED_FIELDS = ["first_name", "last_name", "email", "company"]

# Fields used to compute the 0–100 quality score
QUALITY_FIELDS = [
    "first_name", "last_name", "email", "company", "job_title",
    "phone", "city", "country", "industry", "linkedin_url",
]

# rapidfuzz similarity threshold (0–100): above this score = near-duplicate
FUZZY_THRESHOLD = 88

# Email address prefixes that indicate role/generic addresses
GENERIC_EMAIL_PREFIXES = {
    "info", "admin", "hello", "contact", "sales", "support",
    "noreply", "no-reply", "team", "office", "hr", "help",
    "enquiries", "enquiry", "billing", "accounts", "marketing",
    "careers", "jobs", "press", "media", "legal", "privacy",
}


# ─── CRM COLUMN MAPS ──────────────────────────────────────────────────────────

CRM_COLUMN_MAPS = {
    "hubspot": {
        "First Name":          "first_name",
        "Last Name":           "last_name",
        "Email":               "email",
        "Phone":               "phone",
        "Company":             "company",
        "Job Title":           "job_title",
        "City":                "city",
        "State/Region":        "state",
        "Country":             "country",
        "Lead Status":         "lead_status",
        "HubSpot Owner":       "owner",
        "Lifecycle Stage":     "lifecycle_stage",
        "Create Date":         "created_date",
        "LinkedIn URL":        "linkedin_url",
        "Website URL":         "website",
        "Number of Employees": "num_employees",
        "Industry":            "industry",
        "Annual Revenue":      "annual_revenue",
    },
    "salesforce": {
        "FirstName":           "first_name",
        "LastName":            "last_name",
        "Email":               "email",
        "Phone":               "phone",
        "MobilePhone":         "mobile",
        "Account Name":        "company",
        "AccountName":         "company",
        "Title":               "job_title",
        "MailingCity":         "city",
        "MailingState":        "state",
        "MailingCountry":      "country",
        "Status":              "lead_status",
        "OwnerId":             "owner",
        "LeadSource":          "lead_source",
        "CreatedDate":         "created_date",
        "LinkedIn__c":         "linkedin_url",
        "Website":             "website",
        "Industry":            "industry",
        "NumberOfEmployees":   "num_employees",
        "AnnualRevenue":       "annual_revenue",
    },
}

# Fuzzy column aliases for generic/unknown CRM exports
GENERIC_COLUMN_ALIASES = {
    "firstname": "first_name",   "first": "first_name",
    "lastname": "last_name",     "last": "last_name",     "surname": "last_name",
    "email": "email",            "emailaddress": "email",  "mail": "email",
    "phone": "phone",            "phonenumber": "phone",   "telephone": "phone",
    "mobile": "phone",
    "company": "company",        "organization": "company", "organisation": "company",
    "account": "company",        "accountname": "company",
    "jobtitle": "job_title",     "title": "job_title",    "position": "job_title",
    "role": "job_title",
    "city": "city",              "town": "city",
    "state": "state",            "region": "state",        "province": "state",
    "country": "country",        "nation": "country",
    "leadstatus": "lead_status", "status": "lead_status",
    "industry": "industry",      "sector": "industry",
    "website": "website",        "url": "website",
    "linkedin": "linkedin_url",  "linkedinurl": "linkedin_url",
    "revenue": "annual_revenue", "annualrevenue": "annual_revenue",
    "employees": "num_employees","numberofemployees": "num_employees",
}


# ─── COUNTRY NORMALISATION ────────────────────────────────────────────────────

COUNTRY_MAP = {
    # United States
    "us": "United States",  "usa": "United States",  "u.s.a.": "United States",
    "u.s.": "United States", "america": "United States",
    "united states of america": "United States",
    # United Kingdom
    "uk": "United Kingdom",  "u.k.": "United Kingdom",  "gb": "United Kingdom",
    "great britain": "United Kingdom", "england": "United Kingdom",
    "britain": "United Kingdom",
    # Other common variants
    "ca": "Canada",          "can": "Canada",
    "au": "Australia",       "aus": "Australia",
    "de": "Germany",         "ger": "Germany",         "deutschland": "Germany",
    "fr": "France",
    "es": "Spain",
    "it": "Italy",
    "nl": "Netherlands",     "the netherlands": "Netherlands",
    "in": "India",           "ind": "India",
    "cn": "China",           "prc": "China",
    "jp": "Japan",           "jpn": "Japan",
    "br": "Brazil",          "bra": "Brazil",
    "mx": "Mexico",          "mex": "Mexico",
    "sg": "Singapore",       "sgp": "Singapore",
    "ae": "United Arab Emirates", "uae": "United Arab Emirates",
    "za": "South Africa",    "rsa": "South Africa",
    "nz": "New Zealand",
    "se": "Sweden",          "swe": "Sweden",
    "no": "Norway",          "nor": "Norway",
    "dk": "Denmark",         "dnk": "Denmark",
    "fi": "Finland",         "fin": "Finland",
    "ch": "Switzerland",     "che": "Switzerland",
    "be": "Belgium",         "bel": "Belgium",
    "pl": "Poland",          "pol": "Poland",
    "kr": "South Korea",     "kor": "South Korea",
    "hk": "Hong Kong",
    "my": "Malaysia",        "mys": "Malaysia",
    "id": "Indonesia",       "idn": "Indonesia",
    "ph": "Philippines",     "phl": "Philippines",
    "th": "Thailand",        "tha": "Thailand",
    "vn": "Vietnam",         "vnm": "Vietnam",
}


def normalise_country(value) -> str:
    if not isinstance(value, str) or not value.strip():
        return value
    key = value.strip().lower().rstrip(".")
    return COUNTRY_MAP.get(key, value.strip().title())


# ─── JOB TITLE STANDARDISATION ───────────────────────────────────────────────

# Pass 1 — expand common abbreviations before rank normalisation
ABBREV_EXPANSIONS = [
    (r'\bMktg\b',           "Marketing"),
    (r'\bMgmt\b',           "Management"),
    (r'\bOps\b',            "Operations"),
    (r'\bEng(?:g)?\b',     "Engineering"),
    (r'\bBiz\s*Dev\b',     "Business Development"),
    (r'\bProd\b',           "Product"),
    (r'\bAcct\b',           "Account"),
    (r'\bSls\b',            "Sales"),
    (r'\bComms\b',          "Communications"),
    (r'\bIntl\b',           "International"),
    (r'\bNatl\b',           "National"),
    (r'\bGlbl\b',           "Global"),
    (r'\bHR\b',             "Human Resources"),
    (r'\bIT\b',             "Information Technology"),
    (r'\bPR\b',             "Public Relations"),
    (r'\bCS\b',             "Customer Success"),
    (r'\bMgr\.?\b',        "Manager"),
    (r'\bDir\.?\b',         "Director"),
]

# Pass 2 — normalise rank/level prefixes and IC role codes (most specific first)
TITLE_RANK_MAP = [
    # Named IC roles (must come before generic "AE", "SE" etc. get reused)
    (r'\bAE\b',     "Account Executive"),
    (r'\bSDR\b',    "Sales Development Representative"),
    (r'\bBDR\b',    "Business Development Representative"),
    (r'\bCSM\b',    "Customer Success Manager"),
    (r'\bSE\b',     "Solutions Engineer"),
    (r'\bSA\b',     "Solutions Architect"),
    (r'\bTA\b',     "Technical Architect"),

    # C-Suite — full phrase first, then acronym catch
    (r'\bChief\s+Exec(?:utive)?\s+Off(?:icer)?\b|\bCEO\b',   "Chief Executive Officer"),
    (r'\bChief\s+Fin(?:ancial)?\s+Off(?:icer)?\b|\bCFO\b',   "Chief Financial Officer"),
    (r'\bChief\s+Tech(?:nology)?\s+Off(?:icer)?\b|\bCTO\b',  "Chief Technology Officer"),
    (r'\bChief\s+Mark(?:eting)?\s+Off(?:icer)?\b|\bCMO\b',   "Chief Marketing Officer"),
    (r'\bChief\s+Op(?:erating|s)?\s+Off(?:icer)?\b|\bCOO\b', "Chief Operating Officer"),
    (r'\bChief\s+Rev(?:enue)?\s+Off(?:icer)?\b|\bCRO\b',     "Chief Revenue Officer"),
    (r'\bChief\s+Prod(?:uct)?\s+Off(?:icer)?\b',             "Chief Product Officer"),
    (r'\bChief\s+Info(?:rmation)?\s+Off(?:icer)?\b|\bCIO\b', "Chief Information Officer"),
    (r'\bChief\s+Data\s+Off(?:icer)?\b|\bCDO\b',             "Chief Data Officer"),
    (r'\bChief\s+Human\s+Res(?:ources)?\s+Off(?:icer)?\b|\bCHRO\b', "Chief Human Resources Officer"),
    (r'\bChief\s+Security\s+Off(?:icer)?\b',                 "Chief Security Officer"),
    (r'\bChief\s+People\s+Off(?:icer)?\b',                   "Chief People Officer"),

    # VP-level (most specific first: EVP > SVP > VP)
    (r'\bEVP\b|\bExec(?:utive)?\s+V\.?P\.?\b|\bExec(?:utive)?\s+Vice\s+Pres(?:ident)?\b',
                                                              "Executive Vice President"),
    (r'\bSVP\b|\bSr\.?\s*V\.?P\.?\b|\bSenior\s+Vice\s+Pres(?:ident)?\b',
                                                              "Senior Vice President"),
    (r'\bV\.?P\.?\b|\bVice\s+Pres(?:ident)?\b',             "Vice President"),

    # Director-level
    (r'\bSr\.?\s*Director\b|\bSenior\s+Director\b',          "Senior Director"),
    (r'\bAssoc(?:iate)?\s*Director\b',                        "Associate Director"),
    (r'\bGroup\s+Director\b',                                 "Group Director"),
    (r'\bDirector\b',                                         "Director"),

    # Manager-level
    (r'\bSr\.?\s*Manager\b|\bSenior\s+Manager\b',            "Senior Manager"),
    (r'\bAssoc(?:iate)?\s*Manager\b',                         "Associate Manager"),

    # Stray seniority prefixes
    (r'\bSr\.?\s+',                                           "Senior "),
    (r'\bJr\.?\s+',                                           "Junior "),
]


def standardise_title(title) -> str:
    """Normalise a raw job title string to its canonical form."""
    if not isinstance(title, str) or not title.strip():
        return title
    t = title.strip()
    # Strip stray leading/trailing quotes or dash characters
    t = re.sub(r'^[\"\'\-–—]+|[\"\'\-–—]+$', '', t).strip()
    # Replace " - " separators with a space (e.g. "VP - Mktg" → "VP Mktg")
    t = re.sub(r'\s*[-–—]\s*', ' ', t)
    # Pass 1: expand abbreviations
    for pattern, replacement in ABBREV_EXPANSIONS:
        t = re.sub(pattern, replacement, t, flags=re.IGNORECASE)
    # Pass 2: normalise rank/level
    for pattern, replacement in TITLE_RANK_MAP:
        t = re.sub(pattern, replacement, t, flags=re.IGNORECASE)
    # Collapse multiple spaces
    return re.sub(r'\s{2,}', ' ', t).strip()


# ─── PHONE NORMALISATION ──────────────────────────────────────────────────────

def normalise_phone(value, default_region: str = "US") -> str:
    """Attempt to parse and reformat a phone number to E.164. Falls back to stripped input."""
    if not isinstance(value, str) or not value.strip():
        return value
    if not HAS_PHONENUMBERS:
        return value.strip()
    try:
        parsed = phonenumbers.parse(value, default_region)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        pass
    return value.strip()


# ─── CRM DETECTION + COLUMN NORMALISATION ────────────────────────────────────

def detect_crm(columns: list) -> str:
    col_set = set(columns)
    hubspot_signals    = {"First Name", "Last Name", "Lead Status", "Lifecycle Stage", "HubSpot Owner"}
    salesforce_signals = {"FirstName", "LastName", "MailingCity", "OwnerId", "LeadSource"}
    if len(hubspot_signals & col_set) >= 2:
        return "hubspot"
    if len(salesforce_signals & col_set) >= 2:
        return "salesforce"
    return "generic"


def normalise_columns(df: pd.DataFrame, crm_type: str) -> pd.DataFrame:
    if crm_type in CRM_COLUMN_MAPS:
        col_map = CRM_COLUMN_MAPS[crm_type]
        return df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    # Generic: strip and lowercase column names, then look up alias table
    rename_map = {}
    for col in df.columns:
        key = re.sub(r'[\s_\-]+', '', col.lower())
        if key in GENERIC_COLUMN_ALIASES and col not in rename_map.values():
            rename_map[col] = GENERIC_COLUMN_ALIASES[key]
    return df.rename(columns=rename_map)


# ─── FILE LOADING ─────────────────────────────────────────────────────────────

def load_file(path: str) -> pd.DataFrame:
    """Load CSV, Excel, or JSON into a string-typed DataFrame."""
    ext = Path(path).suffix.lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path, dtype=str)
    if ext == ".json":
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            return pd.DataFrame(data).astype(str)
        if isinstance(data, dict):
            for key in ("records", "contacts", "data", "items", "results"):
                if key in data and isinstance(data[key], list):
                    return pd.DataFrame(data[key]).astype(str)
        return pd.DataFrame([data]).astype(str)
    return pd.read_csv(path, dtype=str)


# ─── VALIDATION ───────────────────────────────────────────────────────────────

EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')


def validate_email(email) -> tuple:
    """Returns (is_valid: bool, reason: str)."""
    if not isinstance(email, str) or not email.strip():
        return False, "Email is empty"
    email = email.strip().lower()
    if not EMAIL_RE.match(email):
        return False, f"Invalid email format: {email}"
    prefix = email.split("@")[0].lower()
    if prefix in GENERIC_EMAIL_PREFIXES:
        return False, f"Generic/role-based email: {email}"
    return True, ""


def compute_quality_score(row: pd.Series, all_cols: list) -> int:
    """Return a 0–100 completeness score based on QUALITY_FIELDS."""
    present = sum(
        1 for f in QUALITY_FIELDS
        if f in all_cols
        and pd.notna(row.get(f))
        and str(row.get(f)).strip() not in ("", "nan")
    )
    return round((present / len(QUALITY_FIELDS)) * 100)


# ─── ENRICHMENT ───────────────────────────────────────────────────────────────

def enrich_from_donor(keeper: pd.Series, donor: pd.Series) -> pd.Series:
    """Fill empty fields in `keeper` with non-empty values from `donor`."""
    for col in keeper.index:
        val = keeper[col]
        if pd.isna(val) or str(val).strip() in ("", "nan"):
            donor_val = donor.get(col)
            if pd.notna(donor_val) and str(donor_val).strip() not in ("", "nan"):
                keeper[col] = donor_val
    return keeper


def infer_company_from_email(email) -> str:
    """
    Best-guess company name from email domain.
    john@acme.com → "Acme"   (skips generic providers like gmail, yahoo).
    """
    if not isinstance(email, str) or "@" not in email:
        return ""
    domain = email.split("@")[-1].lower()
    parts = domain.split(".")
    if len(parts) < 2:
        return ""
    name = parts[-2]
    generic_providers = {
        "gmail", "yahoo", "hotmail", "outlook", "icloud", "aol",
        "protonmail", "mail", "live", "msn", "me", "googlemail",
        "ymail", "zoho",
    }
    return "" if name in generic_providers else name.title()


# ─── DEDUPLICATION ────────────────────────────────────────────────────────────

def _count_filled(row: pd.Series) -> int:
    return sum(1 for v in row if pd.notna(v) and str(v).strip() not in ("", "nan"))


def _resolve_group(group: pd.DataFrame, df: pd.DataFrame, duplicates: list,
                   reason: str, label_col: str) -> int:
    """
    From a group of duplicate rows, keep the most-complete record, enrich it
    from the others, record discarded rows in `duplicates`, and return the
    keeper's index.
    """
    ranked = group.copy()
    ranked["__score"] = ranked.apply(_count_filled, axis=1)
    ranked = ranked.sort_values("__score", ascending=False)
    keeper_idx = ranked.index[0]
    keeper = df.loc[keeper_idx].copy()
    keeper_label = str(df.loc[keeper_idx, label_col]) if label_col in df.columns else str(keeper_idx)

    for idx in ranked.index[1:]:
        keeper = enrich_from_donor(keeper, df.loc[idx])
        donor = df.loc[idx].copy()
        donor["_duplicate_of"]     = keeper_label
        donor["_duplicate_reason"] = reason
        duplicates.append(donor)

    df.loc[keeper_idx] = keeper
    return keeper_idx


def deduplicate(df: pd.DataFrame) -> tuple:
    """
    Two-pass deduplication:
      Pass 1 — exact email (case-insensitive)
      Pass 2 — fuzzy name+company (rapidfuzz) or exact name+company fallback

    Returns (clean_df, duplicates_df).
    """
    duplicates = []

    # ── Pass 1: exact email dedup ──────────────────────────────────────────
    if "email" in df.columns:
        df["__email_key"] = df["email"].str.strip().str.lower().fillna("")
        keep_indices = []
        groups = df[df["__email_key"] != ""].groupby("__email_key", sort=False)
        for _, group in groups:
            if len(group) == 1:
                keep_indices.append(group.index[0])
            else:
                ki = _resolve_group(group, df, duplicates, "Duplicate email", "email")
                keep_indices.append(ki)
        no_email_indices = df[df["__email_key"] == ""].index.tolist()
        df = df.loc[keep_indices + no_email_indices].copy()

    # ── Pass 2: name+company dedup ─────────────────────────────────────────
    name_fields = [f for f in ["first_name", "last_name", "company"] if f in df.columns]
    if name_fields:
        df["__name_key"] = df[name_fields].fillna("").apply(
            lambda r: " ".join(str(v).strip().lower() for v in r), axis=1
        )
        drop_indices = set()

        if HAS_RAPIDFUZZ:
            # Fuzzy clustering: O(n²) — acceptable for typical CRM export sizes
            processed = set()
            indices = df.index.tolist()
            for i, idx_a in enumerate(indices):
                if idx_a in processed:
                    continue
                key_a = df.loc[idx_a, "__name_key"].strip()
                if not key_a.replace(" ", ""):
                    processed.add(idx_a)
                    continue
                cluster = [idx_a]
                for idx_b in indices[i + 1:]:
                    if idx_b in processed:
                        continue
                    key_b = df.loc[idx_b, "__name_key"].strip()
                    if fuzz.token_sort_ratio(key_a, key_b) >= FUZZY_THRESHOLD:
                        cluster.append(idx_b)
                if len(cluster) > 1:
                    group = df.loc[cluster]
                    ki = _resolve_group(group, df, duplicates,
                                        "Fuzzy duplicate (name+company)", "__name_key")
                    for idx in cluster:
                        processed.add(idx)
                        if idx != ki:
                            drop_indices.add(idx)
                else:
                    processed.add(idx_a)
        else:
            # Exact name+company fallback (no rapidfuzz)
            for key, group in df.groupby("__name_key", sort=False):
                if not key.replace(" ", "") or len(group) == 1:
                    continue
                ki = _resolve_group(group, df, duplicates,
                                    "Duplicate name+company", "__name_key")
                for idx in group.index:
                    if idx != ki:
                        drop_indices.add(idx)

        df = df[~df.index.isin(drop_indices)].copy()

    # Drop internal helper columns
    for col in ["__email_key", "__name_key", "__score"]:
        df.drop(columns=[col], errors="ignore", inplace=True)

    dupl_df = pd.DataFrame(duplicates) if duplicates else pd.DataFrame(columns=list(df.columns))
    for col in ["__email_key", "__name_key", "__score"]:
        dupl_df.drop(columns=[col], errors="ignore", inplace=True)

    return df.reset_index(drop=True), dupl_df.reset_index(drop=True)


# ─── FLAGGING ─────────────────────────────────────────────────────────────────

def flag_records(df: pd.DataFrame) -> tuple:
    """
    Splits df into (clean_df, flagged_df).
    Adds _flag_reasons and _quality_score columns to flagged records.
    """
    all_cols = list(df.columns)
    flag_reasons_list = []
    flagged_mask = []

    for _, row in df.iterrows():
        reasons = []

        # Missing required fields
        for field in REQUIRED_FIELDS:
            if field in all_cols:
                val = row.get(field)
                if pd.isna(val) or str(val).strip() in ("", "nan"):
                    reasons.append(f"Missing {field.replace('_', ' ').title()}")

        # Email validation
        if "email" in all_cols:
            email_val = row.get("email")
            if pd.notna(email_val) and str(email_val).strip() not in ("", "nan"):
                valid, reason = validate_email(str(email_val))
                if not valid:
                    reasons.append(reason)

        # Low quality score
        score = compute_quality_score(row, all_cols)
        if score < 40:
            reasons.append(f"Low quality score ({score}/100)")

        flag_reasons_list.append("; ".join(reasons))
        flagged_mask.append(bool(reasons))

    df = df.copy()
    df["_flag_reasons"]  = flag_reasons_list
    df["_quality_score"] = [compute_quality_score(row, all_cols) for _, row in df.iterrows()]

    mask    = pd.Series(flagged_mask, index=df.index)
    flagged = df[mask].copy()
    clean   = df[~mask].copy()
    return clean, flagged


# ─── OUTPUT WRITER ────────────────────────────────────────────────────────────

def save_outputs(
    clean_df: pd.DataFrame,
    flagged_df: pd.DataFrame,
    dupl_df: pd.DataFrame,
    out_clean: str,
    out_flagged: str,
    out_dupl: str,
    out_format: str = "csv",
) -> list:
    """
    Write the three output DataFrames in the chosen format.
    out_format options:
      "csv"      — three separate CSV files (default)
      "xlsx"     — three separate Excel files
      "workbook" — one Excel workbook with three sheets (Clean / Flagged / Duplicates)

    Returns a list of file paths that were written.
    """
    written = []

    if out_format == "workbook":
        # Derive workbook path from the clean output path
        stem = os.path.splitext(out_clean)[0].replace("_cleaned", "")
        wb_path = stem + "_results.xlsx"
        with pd.ExcelWriter(wb_path, engine="openpyxl") as writer:
            clean_df.to_excel(writer,   sheet_name="Clean",      index=False)
            flagged_df.to_excel(writer, sheet_name="Flagged",    index=False)
            dupl_df.to_excel(writer,    sheet_name="Duplicates", index=False)
        written.append(wb_path)

    elif out_format == "xlsx":
        p_clean   = os.path.splitext(out_clean)[0]   + ".xlsx"
        p_flagged = os.path.splitext(out_flagged)[0] + ".xlsx"
        p_dupl    = os.path.splitext(out_dupl)[0]    + ".xlsx"
        clean_df.to_excel(p_clean,    index=False, engine="openpyxl")
        flagged_df.to_excel(p_flagged, index=False, engine="openpyxl")
        dupl_df.to_excel(p_dupl,      index=False, engine="openpyxl")
        written.extend([p_clean, p_flagged, p_dupl])

    else:  # csv (default)
        clean_df.to_csv(out_clean,    index=False)
        flagged_df.to_csv(out_flagged, index=False)
        dupl_df.to_csv(out_dupl,      index=False)
        written.extend([out_clean, out_flagged, out_dupl])

    return written


# ─── HTML REPORT ──────────────────────────────────────────────────────────────

def generate_html_report(
    raw_count: int,
    clean_df: pd.DataFrame,
    flagged_df: pd.DataFrame,
    dupl_df: pd.DataFrame,
    titles_changed: int,
    crm_type: str,
    input_file: str,
) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def df_preview(df: pd.DataFrame, max_rows: int = 5) -> str:
        if df.empty:
            return "<p><em>No records.</em></p>"
        cols = [c for c in df.columns if not c.startswith("__")]
        return df[cols].head(max_rows).to_html(index=False, border=0, classes="table", na_rep="—")

    libs = []
    if HAS_RAPIDFUZZ:    libs.append("rapidfuzz (fuzzy dedup)")
    if HAS_PHONENUMBERS: libs.append("phonenumbers (E.164 normalisation)")
    libs_str = ", ".join(libs) if libs else \
        "none active — install rapidfuzz &amp; phonenumbers for full features"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>CRM Cleaner Report — {ts}</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; }}
  body {{
    font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
    max-width: 1140px; margin: 40px auto; padding: 0 20px;
    color: #111827; background: #f8fafc;
  }}
  h1 {{ color: #1d4ed8; margin-bottom: 4px; font-size: 1.8rem; }}
  .meta {{ color: #6b7280; font-size: .88rem; margin-bottom: 28px; }}
  h2 {{
    color: #1f2937; font-size: 1.1rem; font-weight: 600;
    border-bottom: 2px solid #e5e7eb; padding-bottom: 8px; margin-top: 40px;
  }}
  .stats {{ display: flex; gap: 14px; flex-wrap: wrap; margin: 20px 0 30px; }}
  .card {{
    background: #fff; border: 1px solid #e5e7eb; border-radius: 10px;
    padding: 16px 22px; min-width: 130px;
    box-shadow: 0 1px 3px rgba(0,0,0,.06);
  }}
  .card .num {{ font-size: 2rem; font-weight: 700; color: #1d4ed8; line-height: 1; }}
  .card .lbl {{ font-size: .75rem; color: #6b7280; margin-top: 5px; }}
  .table-wrap {{
    overflow-x: auto; background: #fff; border-radius: 8px;
    border: 1px solid #e5e7eb; box-shadow: 0 1px 2px rgba(0,0,0,.04);
  }}
  table.table {{ width: 100%; border-collapse: collapse; font-size: .81rem; }}
  table.table th {{
    background: #f3f4f6; padding: 9px 12px; text-align: left;
    border-bottom: 1px solid #e5e7eb; white-space: nowrap; font-weight: 600;
  }}
  table.table td {{ padding: 8px 12px; border-bottom: 1px solid #f3f4f6; }}
  table.table tr:last-child td {{ border-bottom: none; }}
  table.table tr:hover td {{ background: #fafafa; }}
  .libs {{ font-size: .78rem; color: #6b7280; margin-top: -18px; margin-bottom: 28px; }}
  footer {{ margin-top: 52px; font-size: .76rem; color: #9ca3af; text-align: center; }}
</style>
</head>
<body>
<h1>CRM Data Cleaner — Report</h1>
<div class="meta">
  Generated: <strong>{ts}</strong> &nbsp;|&nbsp;
  Source file: <strong>{os.path.basename(input_file)}</strong> &nbsp;|&nbsp;
  CRM type detected: <strong>{crm_type.title()}</strong>
</div>

<h2>Summary</h2>
<div class="stats">
  <div class="card"><div class="num">{raw_count}</div><div class="lbl">Raw records loaded</div></div>
  <div class="card"><div class="num">{len(clean_df)}</div><div class="lbl">Clean records</div></div>
  <div class="card"><div class="num">{len(flagged_df)}</div><div class="lbl">Flagged for review</div></div>
  <div class="card"><div class="num">{len(dupl_df)}</div><div class="lbl">Duplicates removed</div></div>
  <div class="card"><div class="num">{titles_changed}</div><div class="lbl">Titles standardised</div></div>
</div>
<p class="libs">Optional libraries active: {libs_str}</p>

<h2>Clean Records
  <small style="font-weight:400;color:#6b7280;font-size:.82rem">
    — first 5 of {len(clean_df)}
  </small>
</h2>
<div class="table-wrap">{df_preview(clean_df)}</div>

<h2>Flagged for Review
  <small style="font-weight:400;color:#6b7280;font-size:.82rem">
    — first 5 of {len(flagged_df)}
  </small>
</h2>
<div class="table-wrap">{df_preview(flagged_df)}</div>

<h2>Duplicates Removed
  <small style="font-weight:400;color:#6b7280;font-size:.82rem">
    — first 5 of {len(dupl_df)}
  </small>
</h2>
<div class="table-wrap">{df_preview(dupl_df)}</div>

<footer>Generated by CRM Data Cleaner &nbsp;·&nbsp; {ts}</footer>
</body>
</html>"""


# ─── PUBLIC API (importable by Streamlit / other scripts) ─────────────────────

def clean(
    input_path: str,
    out_clean: str = OUTPUT_CLEAN,
    out_flagged: str = OUTPUT_FLAGGED,
    out_dupl: str = OUTPUT_DUPLICATES,
    out_report: str = OUTPUT_REPORT,
    crm_override: str = None,
    generate_report: bool = True,
    verbose: bool = True,
    out_format: str = "csv",
    merge_path: str = None,
) -> dict:
    """
    Full cleaning pipeline. Returns a dict with keys:
      raw_count, clean (DataFrame), flagged (DataFrame),
      duplicates (DataFrame), titles_changed, crm_type, written_files (list).

    Parameters
    ----------
    input_path   : Path to the primary CRM export (CSV / Excel / JSON).
    merge_path   : Optional path to a second CRM export to merge before cleaning.
                   Both files are column-normalised for their respective CRM types,
                   then concatenated with a _source column added.
    out_format   : "csv" | "xlsx" | "workbook"
                   "workbook" writes one Excel file with three sheets.
    """

    def log(msg: str):
        if verbose:
            print(msg)

    log(f"\n{'=' * 60}")
    log("  CRM Data Cleaner")
    log(f"{'=' * 60}")
    if not HAS_RAPIDFUZZ:
        log("  [!] rapidfuzz not installed — fuzzy dedup disabled. Run: pip install rapidfuzz")
    if not HAS_PHONENUMBERS:
        log("  [!] phonenumbers not installed — phone normalisation disabled. Run: pip install phonenumbers")
    log(f"  Loading: {input_path}")

    # Load
    df = load_file(input_path)
    # Sanitise literal "nan" / "None" strings produced by string-cast reads
    df = df.replace({"nan": pd.NA, "None": pd.NA, "NaN": pd.NA})
    raw_count = len(df)
    log(f"  Records loaded: {raw_count}")

    # Detect & normalise columns
    crm_type = crm_override or detect_crm(list(df.columns))
    log(f"  CRM type: {crm_type}")
    df = normalise_columns(df, crm_type)

    # ── Merge a second CRM file (option 4) ────────────────────────────────
    if merge_path:
        if not os.path.exists(merge_path):
            print(f"Error: merge file not found: {merge_path}")
            sys.exit(1)
        log(f"  Merging: {merge_path}")
        df2 = load_file(merge_path)
        df2 = df2.replace({"nan": pd.NA, "None": pd.NA, "NaN": pd.NA})
        crm_type2 = detect_crm(list(df2.columns))
        df2 = normalise_columns(df2, crm_type2)
        log(f"  Second file CRM type: {crm_type2} — {len(df2)} records")
        # Tag each row with its source filename for traceability
        df["_source"]  = os.path.basename(input_path)
        df2["_source"] = os.path.basename(merge_path)
        df = pd.concat([df, df2], ignore_index=True)
        raw_count = len(df)
        log(f"  Total after merge: {raw_count}")

    # Casing normalisation
    for field in ("first_name", "last_name"):
        if field in df.columns:
            df[field] = df[field].str.strip().str.title()
    if "email" in df.columns:
        df["email"] = df["email"].str.strip().str.lower()
    if "company" in df.columns:
        df["company"] = df["company"].str.strip().str.title()

    # Country normalisation
    if "country" in df.columns:
        df["country"] = df["country"].apply(normalise_country)

    # Phone normalisation
    if "phone" in df.columns:
        df["phone"] = df["phone"].apply(normalise_phone)

    # Job title standardisation
    titles_changed = 0
    if "job_title" in df.columns:
        original = df["job_title"].copy()
        df["job_title"] = df["job_title"].apply(standardise_title)
        titles_changed = int((original.fillna("") != df["job_title"].fillna("")).sum())
    log(f"  Job titles standardised: {titles_changed}")

    # Enrichment — infer Company from email domain when blank
    if "company" in df.columns and "email" in df.columns:
        missing_company = df["company"].isna() | (df["company"].astype(str).str.strip().isin(["", "nan"]))
        df.loc[missing_company, "company"] = df.loc[missing_company, "email"].apply(
            infer_company_from_email
        )

    # Deduplication (also enriches keeper from donors)
    df, dupl_df = deduplicate(df)
    log(f"  Duplicates removed: {len(dupl_df)}")

    # Flagging
    clean_df, flagged_df = flag_records(df)
    log(f"  Clean records:   {len(clean_df)}")
    log(f"  Flagged records: {len(flagged_df)}")

    # Prepare output DataFrames
    def strip_internal(d: pd.DataFrame) -> pd.DataFrame:
        return d.drop(columns=[c for c in d.columns if c.startswith("__")], errors="ignore")

    clean_out   = strip_internal(clean_df).drop(
        columns=["_flag_reasons", "_quality_score"], errors="ignore"
    )
    flagged_out = strip_internal(flagged_df)   # keep _flag_reasons + _quality_score
    dupl_out    = strip_internal(dupl_df)

    # Write outputs (CSV / xlsx / workbook)
    written_files = save_outputs(
        clean_out, flagged_out, dupl_out,
        out_clean, out_flagged, out_dupl,
        out_format=out_format,
    )
    log(f"\n  Output files written:")
    for p in written_files:
        log(f"    {p}")

    # HTML report
    if generate_report:
        html = generate_html_report(
            raw_count=raw_count,
            clean_df=clean_out,
            flagged_df=flagged_out,
            dupl_df=dupl_out,
            titles_changed=titles_changed,
            crm_type=crm_type,
            input_file=input_path,
        )
        with open(out_report, "w", encoding="utf-8") as fh:
            fh.write(html)
        log(f"    {out_report}")

    log(f"\n{'=' * 60}\n")

    return {
        "raw_count":      raw_count,
        "clean":          clean_out,
        "flagged":        flagged_out,
        "duplicates":     dupl_out,
        "titles_changed": titles_changed,
        "crm_type":       crm_type,
        "written_files":  written_files,
    }


# ─── CLI ENTRY POINT ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="CRM Data Cleaner — HubSpot / Salesforce / Generic CSV, Excel, JSON",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--input",        default=INPUT_FILE,        help="Path to raw CRM export")
    parser.add_argument("--out-clean",    default=OUTPUT_CLEAN,      help="Output: clean records CSV")
    parser.add_argument("--out-flagged",  default=OUTPUT_FLAGGED,    help="Output: flagged records CSV")
    parser.add_argument("--out-dupl",     default=OUTPUT_DUPLICATES, help="Output: duplicates CSV")
    parser.add_argument("--report",       default=OUTPUT_REPORT,     help="Output: HTML report")
    parser.add_argument("--no-report",    action="store_true",       help="Skip HTML report generation")
    parser.add_argument("--crm",          choices=["hubspot", "salesforce", "generic"],
                        default=None, help="Override CRM type auto-detection")
    parser.add_argument(
        "--out-format", choices=["csv", "xlsx", "workbook"], default="csv",
        help=(
            "Output format. csv = three CSV files (default); "
            "xlsx = three Excel files; "
            "workbook = one Excel file with three sheets (Clean / Flagged / Duplicates)"
        ),
    )
    parser.add_argument(
        "--merge", default=None, metavar="FILE",
        help="Path to a second CRM export to merge with --input before cleaning. "
             "Useful for combining a HubSpot and a Salesforce export in one pass.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"Error: input file not found: {args.input}")
        sys.exit(1)

    # If the user didn't override the output paths, auto-route into watch_output/<ts>_<stem>/
    defaults = {args.out_clean, args.out_flagged, args.out_dupl, args.report}
    using_defaults = defaults == {OUTPUT_CLEAN, OUTPUT_FLAGGED, OUTPUT_DUPLICATES, OUTPUT_REPORT}
    if using_defaults:
        from datetime import datetime
        ts   = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        stem = os.path.splitext(os.path.basename(args.input))[0]
        run_dir = os.path.join(WATCH_OUTPUT_DIR, f"{ts}_{stem}")
        os.makedirs(run_dir, exist_ok=True)
        ext = ".xlsx" if args.out_format in ("xlsx", "workbook") else ".csv"
        args.out_clean   = os.path.join(run_dir, f"crm_cleaned{ext}")
        args.out_flagged = os.path.join(run_dir, f"crm_flagged{ext}")
        args.out_dupl    = os.path.join(run_dir, f"crm_duplicates{ext}")
        args.report      = os.path.join(run_dir, "crm_report.html")

    clean(
        input_path=args.input,
        out_clean=args.out_clean,
        out_flagged=args.out_flagged,
        out_dupl=args.out_dupl,
        out_report=args.report,
        crm_override=args.crm,
        generate_report=not args.no_report,
        verbose=True,
        out_format=args.out_format,
        merge_path=args.merge,
    )


if __name__ == "__main__":
    main()
