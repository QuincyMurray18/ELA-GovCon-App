
import os
import sqlite3
from contextlib import closing
from typing import Optional, Any, Dict, List, Tuple
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

# External
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

APP_TITLE = "ELA GovCon Suite"
BUILD_LABEL = "Master A–F — SAM • RFP Analyzer • L&M • Proposal • Subs+Outreach • Quotes • Pricing • Win Prob • Chat • Capability"

st.set_page_config(page_title=APP_TITLE, layout="wide")

DATA_DIR = "data"
DB_PATH = os.path.join(DATA_DIR, "govcon.db")
UPLOADS_DIR = os.path.join(DATA_DIR, "uploads")
SAM_ENDPOINT = "https://api.sam.gov/opportunities/v2/search"


# -------------------- setup --------------------
def ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(UPLOADS_DIR, exist_ok=True)


@st.cache_resource(show_spinner=False)
def get_db() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    with closing(conn.cursor()) as cur:
        cur.execute("PRAGMA foreign_keys = ON;")

        # Core
        cur.execute("""
            CREATE TABLE IF NOT EXISTS contacts(
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                org TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS deals(
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                agency TEXT,
                status TEXT,
                value NUMERIC,
                notice_id TEXT,
                solnum TEXT,
                posted_date TEXT,
                rfp_deadline TEXT,
                naics TEXT,
                psc TEXT,
                sam_url TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS app_settings(
                key TEXT PRIMARY KEY,
                val TEXT
            );
        """)

        # Org profile (Phase F - Capability Statement)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS org_profile(
                id INTEGER PRIMARY KEY CHECK (id=1),
                company_name TEXT,
                tagline TEXT,
                address TEXT,
                phone TEXT,
                email TEXT,
                website TEXT,
                uei TEXT,
                cage TEXT,
                naics TEXT,
                core_competencies TEXT,
                differentiators TEXT,
                certifications TEXT,
                past_performance TEXT,
                primary_poc TEXT
            );
        """)

        # Phase B (RFP analyzer artifacts)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rfps(
                id INTEGER PRIMARY KEY,
                title TEXT,
                solnum TEXT,
                notice_id TEXT,
                sam_url TEXT,
                file_path TEXT,
                created_at TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rfp_sections(
                id INTEGER PRIMARY KEY,
                rfp_id INTEGER NOT NULL REFERENCES rfps(id) ON DELETE CASCADE,
                section TEXT,
                content TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lm_items(
                id INTEGER PRIMARY KEY,
                rfp_id INTEGER NOT NULL REFERENCES rfps(id) ON DELETE CASCADE,
                item_text TEXT,
                is_must INTEGER DEFAULT 1,
                status TEXT DEFAULT 'Open'
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clin_lines(
                id INTEGER PRIMARY KEY,
                rfp_id INTEGER NOT NULL REFERENCES rfps(id) ON DELETE CASCADE,
                clin TEXT,
                description TEXT,
                qty TEXT,
                unit TEXT,
                unit_price TEXT,
                extended_price TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS key_dates(
                id INTEGER PRIMARY KEY,
                rfp_id INTEGER NOT NULL REFERENCES rfps(id) ON DELETE CASCADE,
                label TEXT,
                date_text TEXT,
                date_iso TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pocs(
                id INTEGER PRIMARY KEY,
                rfp_id INTEGER NOT NULL REFERENCES rfps(id) ON DELETE CASCADE,
                name TEXT,
                role TEXT,
                email TEXT,
                phone TEXT
            );
        """)

        # Phase D (vendors + outreach)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vendors(
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                cage TEXT,
                uei TEXT,
                naics TEXT,
                city TEXT,
                state TEXT,
                phone TEXT,
                email TEXT,
                website TEXT,
                notes TEXT,
                last_seen_award TEXT
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendors_naics_state ON vendors(naics, state);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vendor_contacts(
                id INTEGER PRIMARY KEY,
                vendor_id INTEGER NOT NULL REFERENCES vendors(id) ON DELETE CASCADE,
                name TEXT,
                email TEXT,
                phone TEXT,
                role TEXT
            );
        """)

        # Phase E (quotes + pricing)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS quotes(
                id INTEGER PRIMARY KEY,
                rfp_id INTEGER NOT NULL REFERENCES rfps(id) ON DELETE CASCADE,
                vendor TEXT NOT NULL,
                received_date TEXT,
                notes TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS quote_lines(
                id INTEGER PRIMARY KEY,
                quote_id INTEGER NOT NULL REFERENCES quotes(id) ON DELETE CASCADE,
                clin TEXT,
                description TEXT,
                qty REAL,
                unit_price REAL,
                extended_price REAL
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_quote_lines ON quote_lines(quote_id, clin);")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS pricing_scenarios(
                id INTEGER PRIMARY KEY,
                rfp_id INTEGER NOT NULL REFERENCES rfps(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                overhead_pct REAL DEFAULT 0.0,
                gna_pct REAL DEFAULT 0.0,
                fee_pct REAL DEFAULT 0.0,
                contingency_pct REAL DEFAULT 0.0,
                created_at TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pricing_labor(
                id INTEGER PRIMARY KEY,
                scenario_id INTEGER NOT NULL REFERENCES pricing_scenarios(id) ON DELETE CASCADE,
                labor_cat TEXT,
                hours REAL,
                rate REAL,
                fringe_pct REAL DEFAULT 0.0
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pricing_other(
                id INTEGER PRIMARY KEY,
                scenario_id INTEGER NOT NULL REFERENCES pricing_scenarios(id) ON DELETE CASCADE,
                label TEXT,
                cost REAL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS past_perf(
                id INTEGER PRIMARY KEY,
                project_title TEXT NOT NULL,
                customer TEXT,
                contract_no TEXT,
                naics TEXT,
                role TEXT,
                pop_start TEXT,
                pop_end TEXT,
                value NUMERIC,
                scope TEXT,
                results TEXT,
                cpars_rating TEXT,
                contact_name TEXT,
                contact_email TEXT,
                contact_phone TEXT,
                keywords TEXT,
                notes TEXT
            );
        """)

        # Phase H (White Paper Builder)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS white_templates(
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                created_at TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS white_template_sections(
                id INTEGER PRIMARY KEY,
                template_id INTEGER NOT NULL REFERENCES white_templates(id) ON DELETE CASCADE,
                position INTEGER NOT NULL,
                title TEXT,
                body TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS white_papers(
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                subtitle TEXT,
                rfp_id INTEGER REFERENCES rfps(id) ON DELETE SET NULL,
                created_at TEXT,
                updated_at TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS white_paper_sections(
                id INTEGER PRIMARY KEY,
                paper_id INTEGER NOT NULL REFERENCES white_papers(id) ON DELETE CASCADE,
                position INTEGER NOT NULL,
                title TEXT,
                body TEXT,
                image_path TEXT
            );
        """)
        conn.commit()
    return conn


def _file_hash() -> str:
    try:
        import hashlib
        with open(__file__, 'rb') as f:
            return hashlib.sha256(f.read()).hexdigest()[:12]
    except Exception:
        return "unknown"


def save_uploaded_file(uploaded_file, subdir: str = "") -> Optional[str]:
    if not uploaded_file:
        return None
    base_dir = UPLOADS_DIR if not subdir else os.path.join(UPLOADS_DIR, subdir)
    os.makedirs(base_dir, exist_ok=True)
    path = os.path.join(base_dir, uploaded_file.name)
    with open(path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return path


# -------------------- SAM Watch helpers (Phase A) --------------------
def get_sam_api_key() -> Optional[str]:
    key = st.session_state.get("temp_sam_key")
    if key:
        return key
    try:
        key = st.secrets.get("sam", {}).get("api_key")
        if key:
            return key
    except Exception:
        pass
    try:
        key = st.secrets.get("SAM_API_KEY")
        if key:
            return key
    except Exception:
        pass
    return os.getenv("SAM_API_KEY")


@st.cache_data(show_spinner=False, ttl=300)
def sam_search_cached(params: Dict[str, Any]) -> Dict[str, Any]:
    api_key = params.get("api_key")
    if not api_key:
        return {"totalRecords": 0, "records": [], "error": "Missing API key"}

    limit = int(params.get("limit", 100))
    max_pages = int(params.pop("_max_pages", 3))
    params["limit"] = min(max(1, limit), 1000)

    all_records: List[Dict[str, Any]] = []
    offset = int(params.get("offset", 0))

    for _ in range(max_pages):
        q = {**params, "offset": offset}
        try:
            resp = requests.get(SAM_ENDPOINT, params=q, headers={"X-Api-Key": api_key}, timeout=30)
        except Exception as ex:
            return {"totalRecords": 0, "records": [], "error": f"Request error: {ex}"}

        if resp.status_code != 200:
            try:
                j = resp.json()
                msg = j.get("message") or j.get("error") or str(j)
            except Exception:
                msg = resp.text
            return {"totalRecords": 0, "records": [], "error": f"HTTP {resp.status_code}: {msg}", "status": resp.status_code, "body": msg}

        data = resp.json() or {}
        records = data.get("opportunitiesData", data.get("data", []))
        if not isinstance(records, list):
            records = []
        all_records.extend(records)

        total = data.get("totalRecords", len(all_records))
        if len(all_records) >= total:
            break
        offset += params["limit"]

    return {"totalRecords": len(all_records), "records": all_records, "error": None}


def flatten_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for r in records:
        title = r.get("title") or ""
        solnum = r.get("solicitationNumber") or r.get("solnum") or ""
        posted = r.get("postedDate") or ""
        ptype = r.get("type") or r.get("baseType") or ""
        set_aside = r.get("setAside") or ""
        set_aside_code = r.get("setAsideCode") or ""
        naics = r.get("naicsCode") or r.get("ncode") or ""
        psc = r.get("classificationCode") or r.get("ccode") or ""
        deadline = r.get("reponseDeadLine") or r.get("responseDeadline") or ""
        org_path = r.get("fullParentPathName") or r.get("organizationName") or ""
        notice_id = r.get("noticeId") or r.get("noticeid") or r.get("id") or ""
        sam_url = f"https://sam.gov/opp/{notice_id}/view" if notice_id else ""
        rows.append(
            {
                "Title": title,
                "Solicitation": solnum,
                "Type": ptype,
                "Posted": posted,
                "Response Due": deadline,
                "Set-Aside": set_aside,
                "Set-Aside Code": set_aside_code,
                "NAICS": naics,
                "PSC": psc,
                "Agency Path": org_path,
                "Notice ID": notice_id,
                "SAM Link": sam_url,
            }
        )
    df = pd.DataFrame(rows)
    wanted = [
        "Title", "Solicitation", "Type", "Posted", "Response Due",
        "Set-Aside", "Set-Aside Code", "NAICS", "PSC",
        "Agency Path", "Notice ID", "SAM Link",
    ]
    return df[wanted] if not df.empty else df


# ---------------------- Phase B: RFP parsing helpers ----------------------
def _safe_import_pdf_extractors():
    pdf_lib = None
    try:
        import PyPDF2  # type: ignore
        pdf_lib = ('pypdf2', PyPDF2)
    except Exception:
        try:
            import pdfplumber  # type: ignore
            pdf_lib = ('pdfplumber', pdfplumber)
        except Exception:
            pdf_lib = None
    return pdf_lib


def extract_text_from_file(path: str) -> str:
    path_lower = path.lower()
    if path_lower.endswith('.pdf'):
        lib = _safe_import_pdf_extractors()
        if lib and lib[0] == 'pypdf2':
            PyPDF2 = lib[1]
            try:
                with open(path, 'rb') as f:
                    reader = PyPDF2.PdfReader(f)
                    pages = []
                    for page in reader.pages[:50]:
                        try:
                            pages.append(page.extract_text() or '')
                        except Exception:
                            pages.append('')
                    return '\n'.join(pages)
            except Exception:
                return ''
        elif lib and lib[0] == 'pdfplumber':
            pdfplumber = lib[1]
            try:
                with pdfplumber.open(path) as pdf:
                    texts = []
                    for pg in pdf.pages[:50]:
                        try:
                            texts.append(pg.extract_text() or '')
                        except Exception:
                            texts.append('')
                    return '\n'.join(texts)
            except Exception:
                return ''
        else:
            return ''
    elif path_lower.endswith('.docx'):
        try:
            import docx  # python-docx
            doc = docx.Document(path)
            return '\n'.join(p.text for p in doc.paragraphs)
        except Exception:
            return ''
    elif path_lower.endswith('.txt'):
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        except Exception:
            return ''
    else:
        return ''


import re
def extract_sections_L_M(text: str) -> dict:
    out = {}
    if not text:
        return out
    mL = re.search(r'(SECTION\s+L[\s\S]*?)(?=SECTION\s+[A-Z]|\Z)', text, re.IGNORECASE)
    if mL:
        out['L'] = mL.group(1)
    mM = re.search(r'(SECTION\s+M[\s\S]*?)(?=SECTION\s+[A-Z]|\Z)', text, re.IGNORECASE)
    if mM:
        out['M'] = mM.group(1)
    return out


def derive_lm_items(section_text: str) -> list:
    if not section_text:
        return []
    items = []
    for line in section_text.splitlines():
        s = line.strip()
        if len(s) < 4:
            continue
        if re.match(r'^([\-\u2022\*]|\(?[a-zA-Z0-9]\)|[0-9]+\.)\s+', s):
            items.append(s)
    seen = set()
    uniq = []
    for it in items:
        if it not in seen:
            uniq.append(it)
            seen.add(it)
    return uniq[:500]


def extract_clins(text: str) -> list:
    if not text:
        return []
    lines = text.splitlines()
    rows = []
    for i, ln in enumerate(lines):
        m = re.search(r'\bCLIN\s*([A-Z0-9\-]+)', ln, re.IGNORECASE)
        if m:
            clin = m.group(1)
            desc = lines[i+1].strip() if i+1 < len(lines) else ''
            mqty = re.search(r'\b(QTY|Quantity)[:\s]*([0-9,.]+)', ln + ' ' + desc, re.IGNORECASE)
            qty = mqty.group(2) if mqty else ''
            munit = re.search(r'\b(UNIT|Units?)[:\s]*([A-Za-z/]+)', ln + ' ' + desc, re.IGNORECASE)
            unit = munit.group(2) if munit else ''
            rows.append({
                'clin': clin,
                'description': desc[:300],
                'qty': qty,
                'unit': unit,
                'unit_price': '',
                'extended_price': ''
            })
    seen = set()
    uniq = []
    for r in rows:
        if r['clin'] not in seen:
            uniq.append(r)
            seen.add(r['clin'])
    return uniq[:500]


def extract_dates(text: str) -> list:
    if not text:
        return []
    patterns = [
        r'(Questions(?:\s+Due)?|Q&A(?:\s+Due)?|Inquiry Deadline)[:\s]*([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}|\d{1,2}/\d{1,2}/\d{2,4})',
        r'(Proposals?\s+Due|Offers?\s+Due|Closing Date)[:\s]*([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}|\d{1,2}/\d{1,2}/\d{2,4})',
        r'(Site\s+Visit)[:\s]*([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}|\d{1,2}/\d{1,2}/\d{2,4})',
        r'(Period\s+of\s+Performance|POP)[:\s]*([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}|\d{1,2}/\d{1,2}/\d{2,4})',
    ]
    out = []
    for pat in patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            out.append({'label': m.group(1).strip(), 'date_text': m.group(2).strip(), 'date_iso': ''})
    return out[:200]


def extract_pocs(text: str) -> list:
    if not text:
        return []
    emails = list(set(re.findall(r'[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}', text)))
    phones = list(set(re.findall(r'(?:\+?1\s*)?(?:\(\d{3}\)|\d{3})[\s\-]?\d{3}[\s\-]?\d{4}', text)))
    poc_blocks = re.findall(r'(Contracting Officer|Contract Specialist|Point of Contact|POC).*?(?:\n\n|$)', text, re.IGNORECASE|re.DOTALL)
    names = []
    for blk in poc_blocks:
        for nm in re.findall(r'([A-Z][a-zA-Z\-]+\s+[A-Z][a-zA-Z\-]+)', blk):
            names.append(nm)
    out = []
    for i in range(max(len(names), len(emails), len(phones))):
        out.append({
            'name': names[i] if i < len(names) else '',
            'role': 'POC',
            'email': emails[i] if i < len(emails) else '',
            'phone': phones[i] if i < len(phones) else '',
        })
    return out[:100]


# -------------------- Modules --------------------
def run_contacts(conn: sqlite3.Connection) -> None:
    st.header("Contacts")
    with st.form("add_contact", clear_on_submit=True):
        c1, c2, c3 = st.columns([2, 2, 2])
        with c1:
            name = st.text_input("Name")
        with c2:
            email = st.text_input("Email")
        with c3:
            org = st.text_input("Organization")
        submitted = st.form_submit_button("Add Contact")
    if submitted:
        try:
            with closing(conn.cursor()) as cur:
                cur.execute(
                    "INSERT INTO contacts(name, email, org) VALUES (?, ?, ?);",
                    (name.strip(), email.strip(), org.strip()),
                )
                conn.commit()
            st.success(f"Added contact {name}")
        except Exception as e:
            st.error(f"Error saving contact {e}")

    try:
        df = pd.read_sql_query(
            "SELECT name, email, org FROM contacts ORDER BY name;", conn
        )
        st.subheader("Contact List")
        if df.empty:
            st.write("No contacts yet")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Failed to load contacts {e}")


def run_deals(conn: sqlite3.Connection) -> None:
    st.header("Deals")
    with st.form("add_deal", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
        with c1:
            title = st.text_input("Title")
        with c2:
            agency = st.text_input("Agency")
        with c3:
            status = st.selectbox(
                "Status",
                ["New", "Qualifying", "Bidding", "Submitted", "Awarded", "Lost"],
            )
        with c4:
            value = st.number_input("Est Value", min_value=0.0, step=1000.0, format="%.2f")
        submitted = st.form_submit_button("Add Deal")
    if submitted:
        try:
            with closing(conn.cursor()) as cur:
                cur.execute(
                    "INSERT INTO deals(title, agency, status, value) VALUES (?, ?, ?, ?);",
                    (title.strip(), agency.strip(), status, float(value)),
                )
                conn.commit()
            st.success(f"Added deal {title}")
        except Exception as e:
            st.error(f"Error saving deal {e}")

    try:
        df = pd.read_sql_query(
            "SELECT title, agency, status, value, sam_url FROM deals ORDER BY id DESC;",
            conn,
        )
        st.subheader("Pipeline")
        if df.empty:
            st.write("No deals yet")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Failed to load deals {e}")


# ---------- SAM Watch (Phase A) ----------

def run_sam_watch(conn: sqlite3.Connection) -> None:
    st.header("SAM Watch")
    st.caption("Live search from SAM.gov v2 API. Push selected notices to Deals or RFP Analyzer.")

    api_key = get_sam_api_key()

    # Search filters (dates optional)
    with st.expander("Search Filters", expanded=True):
        today = datetime.now().date()
        default_from = today - timedelta(days=30)

        c1, c2, c3 = st.columns([2, 2, 2])
        with c1:
            use_dates = st.checkbox("Filter by posted date", value=False)
        with c2:
            active_only = st.checkbox("Active only", value=True)
        with c3:
            org_name = st.text_input("Organization/Agency contains")

        if use_dates:
            d1, d2 = st.columns([2, 2])
            with d1:
                posted_from = st.date_input("Posted From", value=default_from, key="sam_posted_from")
            with d2:
                posted_to = st.date_input("Posted To", value=today, key="sam_posted_to")

        e1, e2, e3 = st.columns([2, 2, 2])
        with e1:
            keywords = st.text_input("Keywords (Title contains)")
        with e2:
            naics = st.text_input("NAICS (6-digit)")
        with e3:
            psc = st.text_input("PSC")

        e4, e5, e6 = st.columns([2, 2, 2])
        with e4:
            state = st.text_input("Place of Performance State (e.g., TX)")
        with e5:
            set_aside = st.text_input("Set-Aside Code (SB, 8A, SDVOSB)")
        with e6:
            pass

        ptype_map = {
            "Pre-solicitation": "p",
            "Sources Sought": "r",
            "Special Notice": "s",
            "Solicitation": "o",
            "Combined Synopsis/Solicitation": "k",
            "Justification (J&A)": "u",
            "Sale of Surplus Property": "g",
            "Intent to Bundle (DoD)": "i",
            "Award Notice": "a",
        }
        types = st.multiselect(
            "Notice Types",
            list(ptype_map.keys()),
            default=["Solicitation", "Combined Synopsis/Solicitation", "Sources Sought"],
        )

        g1, g2 = st.columns([2, 2])
        with g1:
            limit = st.number_input("Results per page", min_value=1, max_value=1000, value=100, step=50)
        with g2:
            max_pages = st.slider("Pages to fetch", min_value=1, max_value=10, value=3)

        run = st.button("Run Search", type="primary")

    results_df = st.session_state.get("sam_results_df", pd.DataFrame())

    if run:
        if not api_key:
            st.error("Missing SAM API key. Add SAM_API_KEY to your Streamlit secrets.")
            return

        params: Dict[str, Any] = {
            "api_key": api_key,
            "limit": int(limit),
            "offset": 0,
            "_max_pages": int(max_pages),
        }
        if active_only:
            params["status"] = "active"
        if "use_dates" in locals() and use_dates:
            params["postedFrom"] = posted_from.strftime("%m/%d/%Y")
            params["postedTo"] = posted_to.strftime("%m/%d/%Y")
        else:
            # SAM.gov API requires postedFrom/postedTo; use implicit last 30 days when filter is off
            _today = datetime.now().date()
            _from = _today - timedelta(days=30)
            params["postedFrom"] = _from.strftime("%m/%d/%Y")
            params["postedTo"] = _today.strftime("%m/%d/%Y")
        if keywords:
            params["title"] = keywords
        if naics:
            params["ncode"] = naics
        if psc:
            params["ccode"] = psc
        if state:
            params["state"] = state
        if set_aside:
            params["typeOfSetAside"] = set_aside
        if org_name:
            params["organizationName"] = org_name
        if types:
            params["ptype"] = ",".join(ptype_map[t] for t in types if t in ptype_map)

        with st.spinner("Searching SAM.gov..."):
            out = sam_search_cached(params)

        if out.get("error"):
            st.error(out["error"])
            return

        recs = out.get("records", [])
        results_df = flatten_records(recs)
        st.session_state["sam_results_df"] = results_df
        st.success(f"Fetched {len(results_df)} notices")

    if (results_df is None or results_df.empty) and not run:
        st.info("Set filters and click Run Search")

    if results_df is not None and not results_df.empty:
        st.dataframe(results_df, use_container_width=True, hide_index=True)
        titles = [f"{row['Title']} [{row.get('Solicitation') or '—'}]" for _, row in results_df.iterrows()]
        idx = st.selectbox("Select a notice", options=list(range(len(titles))), format_func=lambda i: titles[i])
        row = results_df.iloc[idx]

        with st.expander("Opportunity Details", expanded=True):
            c1, c2 = st.columns([3, 2])
            with c1:
                st.write(f"**Title:** {row['Title']}")
                st.write(f"**Solicitation:** {row['Solicitation']}")
                st.write(f"**Type:** {row['Type']}")
                st.write(f"**Set-Aside:** {row['Set-Aside']} ({row['Set-Aside Code']})")
                st.write(f"**NAICS:** {row['NAICS']}  **PSC:** {row['PSC']}")
                st.write(f"**Agency Path:** {row['Agency Path']}")
            with c2:
                st.write(f"**Posted:** {row['Posted']}")
                st.write(f"**Response Due:** {row['Response Due']}")
                st.write(f"**Notice ID:** {row['Notice ID']}")
                if row['SAM Link']:
                    st.markdown(f"[Open in SAM]({row['SAM Link']})")

        c3, c4, c5 = st.columns([2, 2, 2])
        with c3:
            if st.button("Add to Deals", key="add_to_deals"):
                try:
                    with closing(conn.cursor()) as cur:
                        cur.execute(
                            """
                            INSERT INTO deals(title, agency, status, value, notice_id, solnum, posted_date, rfp_deadline, naics, psc, sam_url)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                            """,
                            (
                                row["Title"],
                                row["Agency Path"],
                                "Bidding",
                                None,
                                row["Notice ID"],
                                row["Solicitation"],
                                row["Posted"],
                                row["Response Due"],
                                row["NAICS"],
                                row["PSC"],
                                row["SAM Link"],
                            ),
                        )
                        conn.commit()
                    st.success("Saved to Deals")
                except Exception as e:
                    st.error(f"Failed to save deal: {e}")
        with c4:
            if st.button("Push to RFP Analyzer", key="push_to_rfp"):
                st.session_state["rfp_selected_notice"] = row.to_dict()
                st.success("Sent to RFP Analyzer. Switch to that tab to continue.")
        with c5:
            st.caption("Use Open in SAM for attachments and full details")

def run_rfp_analyzer(conn: sqlite3.Connection) -> None:
    st.header("RFP Analyzer")
    ctx = st.session_state.get("rfp_selected_notice")
    if ctx:
        st.info(
            f"Loaded from SAM Watch: **{ctx.get('Title','')}** | Solicitation: {ctx.get('Solicitation','')} | Notice ID: {ctx.get('Notice ID','')}"
        )
        if ctx.get("SAM Link"):
            st.markdown(f"[Open in SAM]({ctx['SAM Link']})")

    colA, colB = st.columns([3,2])
    with colA:
        up = st.file_uploader("Upload RFP (PDF/DOCX/TXT)", type=["pdf", "docx", "txt"])
        with st.expander("Manual Text Paste (optional)", expanded=False):
            pasted = st.text_area("Paste any text to include in parsing", height=150)
        do_parse = st.button("Parse RFP", type="primary")
    with colB:
        st.caption("This will attempt to extract:")
        st.markdown("- Section L and M blocks\n- L/M checklist bullets\n- CLIN lines (heuristic)\n- Key dates (due dates, Q&A, POP)\n- POCs (emails/phones)")

    if do_parse:
        file_path = None
        text = ''
        if up is not None:
            file_path = save_uploaded_file(up, subdir="rfp")
            text = extract_text_from_file(file_path)
        if pasted:
            text = (text + '\n' + pasted).strip() if text else pasted

        if not text:
            st.error("Could not read any text. Try .txt or paste content.")
            return

        secs = extract_sections_L_M(text)
        l_items = derive_lm_items(secs.get('L','')) + derive_lm_items(secs.get('M',''))
        clins = extract_clins(text)
        dates = extract_dates(text)
        pocs = extract_pocs(text)

        with st.expander("Section L (preview)", expanded=bool(secs.get('L'))):
            st.write(secs.get('L','')[:4000] or "Not found")
        with st.expander("Section M (preview)", expanded=bool(secs.get('M'))):
            st.write(secs.get('M','')[:4000] or "Not found")

        st.subheader("Checklist Items (L & M)")
        df_lm = pd.DataFrame({"item_text": l_items}) if l_items else pd.DataFrame(columns=["item_text"])
        st.dataframe(df_lm.rename(columns={"item_text": "Item"}), use_container_width=True, hide_index=True)

        st.subheader("CLIN Lines (heuristic)")
        df_clin = pd.DataFrame(clins)
        st.dataframe(df_clin if not df_clin.empty else pd.DataFrame(columns=['clin','description','qty','unit','unit_price','extended_price']), use_container_width=True, hide_index=True)

        st.subheader("Key Dates (heuristic)")
        df_dates = pd.DataFrame(dates)
        st.dataframe(df_dates if not df_dates.empty else pd.DataFrame(columns=['label','date_text','date_iso']), use_container_width=True, hide_index=True)

        st.subheader("POCs (heuristic)")
        df_pocs = pd.DataFrame(pocs)
        st.dataframe(df_pocs if not df_pocs.empty else pd.DataFrame(columns=['name','role','email','phone']), use_container_width=True, hide_index=True)

        if st.button("Save Extraction to DB", type="primary"):
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute(
                        "INSERT INTO rfps(title, solnum, notice_id, sam_url, file_path, created_at) VALUES (?, ?, ?, ?, ?, datetime('now'));",
                        (
                            ctx.get('Title') if ctx else None,
                            ctx.get('Solicitation') if ctx else None,
                            ctx.get('Notice ID') if ctx else None,
                            ctx.get('SAM Link') if ctx else None,
                            file_path,
                        ),
                    )
                    rfp_id = cur.lastrowid
                    for sec_name in ['L','M']:
                        content = secs.get(sec_name)
                        if content:
                            cur.execute(
                                "INSERT INTO rfp_sections(rfp_id, section, content) VALUES (?, ?, ?);",
                                (rfp_id, sec_name, content),
                            )
                    for it in l_items:
                        cur.execute(
                            "INSERT INTO lm_items(rfp_id, item_text, is_must, status) VALUES (?, ?, ?, ?);",
                            (rfp_id, it, 1, 'Open'),
                        )
                    for r in clins:
                        cur.execute(
                            "INSERT INTO clin_lines(rfp_id, clin, description, qty, unit, unit_price, extended_price) VALUES (?, ?, ?, ?, ?, ?, ?);",
                            (rfp_id, r.get('clin'), r.get('description'), r.get('qty'), r.get('unit'), r.get('unit_price'), r.get('extended_price')),
                        )
                    for d in dates:
                        cur.execute(
                            "INSERT INTO key_dates(rfp_id, label, date_text, date_iso) VALUES (?, ?, ?, ?);",
                            (rfp_id, d.get('label'), d.get('date_text'), d.get('date_iso')),
                        )
                    for p in pocs:
                        cur.execute(
                            "INSERT INTO pocs(rfp_id, name, role, email, phone) VALUES (?, ?, ?, ?, ?);",
                            (rfp_id, p.get('name'), p.get('role'), p.get('email'), p.get('phone')),
                        )
                    conn.commit()
                st.success("Extraction saved.")
                st.session_state['current_rfp_id'] = rfp_id
            except Exception as e:
                st.error(f"Failed to save extraction: {e}")


# ---------- L & M Checklist ----------
def run_lm_checklist(conn: sqlite3.Connection) -> None:
    st.header("L and M Checklist")
    rfp_id = st.session_state.get('current_rfp_id')
    if not rfp_id:
        try:
            df_rf = pd.read_sql_query("SELECT id, title, solnum, created_at FROM rfps ORDER BY id DESC;", conn)
        except Exception as e:
            st.error(f"Failed to load RFPs: {e}")
            return
        if df_rf.empty:
            st.info("No saved RFP extractions yet. Use RFP Analyzer to parse and save.")
            return
        opt = st.selectbox("Select an RFP context", options=df_rf['id'].tolist(), format_func=lambda rid: f"#{rid} — {df_rf.loc[df_rf['id']==rid,'title'].values[0] or 'Untitled'}")
        rfp_id = opt
        st.session_state['current_rfp_id'] = rfp_id

    st.caption(f"Working RFP ID: {rfp_id}")
    try:
        df_items = pd.read_sql_query("SELECT id, item_text, is_must, status FROM lm_items WHERE rfp_id=?;", conn, params=(rfp_id,))
    except Exception as e:
        st.error(f"Failed to load items: {e}")
        return

    if df_items.empty:
        st.info("No L/M items found for this RFP.")
        return

    c1, c2 = st.columns([2,2])
    with c1:
        if st.button("Mark all Complete"):
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute("UPDATE lm_items SET status='Complete' WHERE rfp_id=?;", (rfp_id,))
                    conn.commit()
                st.success("All items marked Complete")
            except Exception as e:
                st.error(f"Update failed: {e}")
    with c2:
        if st.button("Reset all to Open"):
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute("UPDATE lm_items SET status='Open' WHERE rfp_id=?;", (rfp_id,))
                    conn.commit()
                st.success("All items reset")
            except Exception as e:
                st.error(f"Update failed: {e}")

    for _, row in df_items.iterrows():
        key = f"lm_{row['id']}"
        checked = st.checkbox(row['item_text'], value=(row['status']=='Complete'), key=key)
        new_status = 'Complete' if checked else 'Open'
        if new_status != row['status']:
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute("UPDATE lm_items SET status=? WHERE id=?;", (new_status, int(row['id'])))
                    conn.commit()
            except Exception as e:
                st.error(f"Failed to update item {row['id']}: {e}")


# ---------- Proposal Builder (Phase C) ----------
def _load_rfp_context(conn: sqlite3.Connection, rfp_id: Optional[int]):
    ctx = {"rfp": None, "sections": pd.DataFrame(), "items": pd.DataFrame(), "clins": pd.DataFrame(), "dates": pd.DataFrame(), "pocs": pd.DataFrame()}
    try:
        if rfp_id:
            ctx["rfp"] = pd.read_sql_query("SELECT * FROM rfps WHERE id=?;", conn, params=(rfp_id,))
            ctx["sections"] = pd.read_sql_query("SELECT section, content FROM rfp_sections WHERE rfp_id=?;", conn, params=(rfp_id,))
            ctx["items"] = pd.read_sql_query("SELECT item_text, status FROM lm_items WHERE rfp_id=?;", conn, params=(rfp_id,))
            ctx["clins"] = pd.read_sql_query("SELECT clin, description, qty, unit, unit_price, extended_price FROM clin_lines WHERE rfp_id=?;", conn, params=(rfp_id,))
            ctx["dates"] = pd.read_sql_query("SELECT label, date_text FROM key_dates WHERE rfp_id=?;", conn, params=(rfp_id,))
            ctx["pocs"] = pd.read_sql_query("SELECT name, role, email, phone FROM pocs WHERE rfp_id=?;", conn, params=(rfp_id,))
    except Exception as e:
        st.error(f"Failed to load RFP context {e}")
    return ctx


def _estimate_pages(word_count: int, spacing: str) -> float:
    if spacing == "Single":
        wpp = 500
    elif spacing == "1.15":
        wpp = 430
    else:
        wpp = 300
    return round(max(1, word_count) / wpp, 2)


def _export_docx(output_path: str, doc_title: str, sections: List[Dict[str, str]],
                 clins: pd.DataFrame, checklist: pd.DataFrame, metadata: Dict[str, str],
                 font_name: str = "Times New Roman", font_size_pt: int = 11, spacing: str = "1.15") -> Optional[str]:
    try:
        from docx import Document  # type: ignore
        from docx.shared import Pt, Inches  # type: ignore
        from docx.enum.text import WD_LINE_SPACING  # type: ignore
    except Exception:
        st.error("python-docx is required. pip install python-docx")
        return None

    doc = Document()
    secs = doc.sections
    for s in secs:
        s.top_margin = Inches(1)
        s.bottom_margin = Inches(1)
        s.left_margin = Inches(1)
        s.right_margin = Inches(1)

    style = doc.styles["Normal"]
    style.font.name = font_name
    style.font.size = Pt(font_size_pt)

    doc.add_heading(doc_title or "Proposal", level=1)
    if metadata:
        p = doc.add_paragraph()
        for k, v in metadata.items():
            if v:
                p.add_run(f"{k}: {v}  ")

    for sec in sections:
        doc.add_heading(sec.get("title") or "Section", level=2)
        body = sec.get("body") or ""
        for para in body.split("\n\n"):
            if not para.strip():
                continue
            p = doc.add_paragraph(para.strip())
            if spacing == "Single":
                p.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE
            elif spacing == "1.15":
                p.paragraph_format.line_spacing = 1.15
            else:
                p.paragraph_format.line_spacing_rule = WD_LINE_SPACING.DOUBLE

    if clins is not None and not clins.empty:
        doc.add_heading("CLIN Table", level=2)
        cols = ["clin", "description", "qty", "unit", "unit_price", "extended_price"]
        t = doc.add_table(rows=1, cols=len(cols))
        hdr = t.rows[0].cells
        for i, c in enumerate(cols):
            hdr[i].text = c.upper()
        for _, r in clins.iterrows():
            row = t.add_row().cells
            for i, c in enumerate(cols):
                row[i].text = str(r.get(c, "") or "")

    if checklist is not None and not checklist.empty:
        doc.add_heading("Compliance Checklist", level=2)
        cols = ["item_text", "status"]
        t = doc.add_table(rows=1, cols=len(cols))
        for i, c in enumerate(["Requirement", "Status"]):
            t.rows[0].cells[i].text = c
        for _, r in checklist.iterrows():
            row = t.add_row().cells
            row[0].text = str(r.get("item_text", ""))
            row[1].text = str(r.get("status", ""))

    doc.save(output_path)
    return output_path


def run_proposal_builder(conn: sqlite3.Connection) -> None:
    st.header("Proposal Builder")
    df_rf = pd.read_sql_query("SELECT id, title, solnum, notice_id FROM rfps ORDER BY id DESC;", conn)
    if df_rf.empty:
        st.info("No RFP context found. Use RFP Analyzer first to parse and save.")
        return
    rfp_id = st.selectbox(
        "RFP context",
        options=df_rf["id"].tolist(),
        format_func=lambda rid: f"#{rid} — {df_rf.loc[df_rf['id']==rid,'title'].values[0] or 'Untitled'}",
        index=0,
    )
    st.session_state["current_rfp_id"] = rfp_id
    ctx = _load_rfp_context(conn, rfp_id)

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Sections")
        default_sections = [
            "Cover Letter","Executive Summary","Understanding of Requirements","Technical Approach","Management Plan",
            "Staffing and Key Personnel","Quality Assurance","Past Performance Summary","Pricing and CLINs","Certifications and Reps","Appendices",
        ]
        selected = st.multiselect("Include sections", default_sections, default=default_sections)
        content_map: Dict[str, str] = {}
        for sec in selected:
            default_val = st.session_state.get(f"pb_section_{sec}", "")
            content_map[sec] = st.text_area(sec, value=default_val, height=140)

    with right:
        st.subheader("Guidance and limits")
        spacing = st.selectbox("Line spacing", ["Single", "1.15", "Double"], index=1)
        font_name = st.selectbox("Font", ["Times New Roman", "Calibri"], index=0)
        font_size = st.number_input("Font size", min_value=10, max_value=12, value=11)
        page_limit = st.number_input("Page limit for narrative", min_value=1, max_value=200, value=10)

        st.markdown("**Must address items from L and M**")
        items = ctx["items"] if isinstance(ctx.get("items"), pd.DataFrame) else pd.DataFrame()
        if not items.empty:
            st.dataframe(items.rename(columns={"item_text": "Item", "status": "Status"}), use_container_width=True, hide_index=True, height=240)
        else:
            st.caption("No checklist items found for this RFP")

        total_words = sum(len((content_map.get(k) or "").split()) for k in selected)
        est_pages = _estimate_pages(total_words, spacing)
        st.info(f"Current word count {total_words}  Estimated pages {est_pages}")
        if est_pages > page_limit:
            st.error("Content likely exceeds page limit. Consider trimming or tighter formatting")

        out_name = f"Proposal_RFP_{int(rfp_id)}.docx"
        out_path = os.path.join(DATA_DIR, out_name)
        if st.button("Export DOCX", type="primary"):
            sections = [{"title": k, "body": content_map.get(k, "")} for k in selected]
            exported = _export_docx(
                out_path,
                doc_title=ctx["rfp"].iloc[0]["title"] if ctx["rfp"] is not None and not ctx["rfp"].empty else "Proposal",
                sections=sections,
                clins=ctx["clins"],
                checklist=ctx["items"],
                metadata={
                    "Solicitation": (ctx["rfp"].iloc[0]["solnum"] if ctx["rfp"] is not None and not ctx["rfp"].empty else ""),
                    "Notice ID": (ctx["rfp"].iloc[0]["notice_id"] if ctx["rfp"] is not None and not ctx["rfp"].empty else ""),
                },
                font_name=font_name,
                font_size_pt=int(font_size),
                spacing=spacing,
            )
            if exported:
                st.success(f"Exported to {exported}")
                st.markdown(f"[Download DOCX]({exported})")


# ---------- Subcontractor Finder (Phase D) ----------
def run_subcontractor_finder(conn: sqlite3.Connection) -> None:
    st.header("Subcontractor Finder")
    st.caption("Seed and manage vendors by NAICS/PSC/state; handoff selected vendors to Outreach.")

    ctx = st.session_state.get("rfp_selected_notice", {})
    default_naics = ctx.get("NAICS") or ""
    default_state = ""

    with st.expander("Filters", expanded=True):
        c1, c2, c3, c4 = st.columns([2,2,2,2])
        with c1:
            f_naics = st.text_input("NAICS", value=default_naics, key="filter_naics")
        with c2:
            f_state = st.text_input("State (e.g., TX)", value=default_state, key="filter_state")
        with c3:
            f_city = st.text_input("City contains", key="filter_city")
        with c4:
            f_kw = st.text_input("Keyword in name/notes", key="filter_kw")
        st.caption("Use CSV import or add vendors manually. Internet seeding can be added later.")

    with st.expander("Import Vendors (CSV)", expanded=False):
        st.caption("Headers: name, email, phone, city, state, naics, cage, uei, website, notes")
        up = st.file_uploader("Upload vendor CSV", type=["csv"], key="vendor_csv")
        if up and st.button("Import CSV"):
            try:
                df = pd.read_csv(up)
                if "name" not in {c.lower() for c in df.columns}:
                    st.error("CSV must include a 'name' column")
                else:
                    df.columns = [c.lower() for c in df.columns]
                    n=0
                    with closing(conn.cursor()) as cur:
                        for _, r in df.iterrows():
                            cur.execute(
                                """
                                INSERT INTO vendors(name, cage, uei, naics, city, state, phone, email, website, notes)
                                VALUES(?,?,?,?,?,?,?,?,?,?)
                                ;
                                """,
                                (
                                    str(r.get("name",""))[:200],
                                    str(r.get("cage",""))[:20],
                                    str(r.get("uei",""))[:40],
                                    str(r.get("naics",""))[:20],
                                    str(r.get("city",""))[:100],
                                    str(r.get("state",""))[:10],
                                    str(r.get("phone",""))[:40],
                                    str(r.get("email",""))[:120],
                                    str(r.get("website",""))[:200],
                                    str(r.get("notes",""))[:500],
                                ),
                            )
                            n+=1
                    conn.commit()
                    st.success(f"Imported {n} vendors")
            except Exception as e:
                st.error(f"Import failed: {e}")

    with st.expander("Add Vendor", expanded=False):
        c1, c2, c3 = st.columns([2,2,2])
        with c1:
            v_name = st.text_input("Company name", key="add_name")
            v_email = st.text_input("Email", key="add_email")
            v_phone = st.text_input("Phone", key="add_phone")
        with c2:
            v_city = st.text_input("City", key="add_city")
            v_state = st.text_input("State", key="add_state")
            v_naics = st.text_input("NAICS", key="add_naics")
        with c3:
            v_cage = st.text_input("CAGE", key="add_cage")
            v_uei = st.text_input("UEI", key="add_uei")
            v_site = st.text_input("Website", key="add_site")
        v_notes = st.text_area("Notes", height=80, key="add_notes")
        if st.button("Save Vendor"):
            if not v_name.strip():
                st.error("Name is required")
            else:
                try:
                    with closing(conn.cursor()) as cur:
                        cur.execute(
                            """
                            INSERT INTO vendors(name, cage, uei, naics, city, state, phone, email, website, notes)
                            VALUES(?,?,?,?,?,?,?,?,?,?)
                            ;
                            """,
                            (v_name.strip(), v_cage.strip(), v_uei.strip(), v_naics.strip(), v_city.strip(), v_state.strip(), v_phone.strip(), v_email.strip(), v_site.strip(), v_notes.strip()),
                        )
                        conn.commit()
                    st.success("Vendor saved")
                except Exception as e:
                    st.error(f"Save failed: {e}")

    q = "SELECT id, name, email, phone, city, state, naics, cage, uei, website, notes FROM vendors WHERE 1=1"
    params: List[Any] = []
    if f_naics:
        q += " AND (naics LIKE ? )"
        params.append(f"%{f_naics}%")
    if f_state:
        q += " AND (state LIKE ?)"
        params.append(f"%{f_state}%")
    if f_city:
        q += " AND (city LIKE ?)"
        params.append(f"%{f_city}%")
    if f_kw:
        q += " AND (name LIKE ? OR notes LIKE ?)"
        params.extend([f"%{f_kw}%", f"%{f_kw}%"])

    try:
        df_v = pd.read_sql_query(q + " ORDER BY name ASC;", conn, params=params)
    except Exception as e:
        st.error(f"Query failed: {e}")
        df_v = pd.DataFrame()

    st.subheader("Vendors")
    if df_v.empty:
        st.write("No vendors match filters")
    else:
        selected_ids = []
        for _, row in df_v.iterrows():
            chk = st.checkbox(f"Select — {row['name']}  ({row['email'] or 'no email'})", key=f"vend_{int(row['id'])}")
            if chk:
                selected_ids.append(int(row['id']))
        c1, c2 = st.columns([2,2])
        with c1:
            if st.button("Send to Outreach ▶") and selected_ids:
                st.session_state['rfq_vendor_ids'] = selected_ids
                st.success(f"Queued {len(selected_ids)} vendors for Outreach")
        with c2:
            st.caption("Selections are stored in session and available in Outreach tab")


# ---------- Outreach (Phase D) ----------
def _smtp_settings() -> Dict[str, Any]:
    out = {"host": None, "port": 587, "username": None, "password": None, "from_email": None, "from_name": "ELA Management", "use_tls": True}
    try:
        cfg = st.secrets.get("smtp", {})
        out.update({k: cfg.get(k, out[k]) for k in out})
    except Exception:
        pass
    for k in list(out.keys()):
        if not out[k]:
            try:
                v = st.secrets.get(k)
                if v:
                    out[k] = v
            except Exception:
                pass
    return out


def send_email_smtp(to_email: str, subject: str, html_body: str, attachments: List[str]) -> Tuple[bool, str]:
    cfg = _smtp_settings()
    if not all([cfg.get("host"), cfg.get("port"), cfg.get("username"), cfg.get("password"), cfg.get("from_email")]):
        return False, "Missing SMTP settings in secrets"

    msg = MIMEMultipart()
    msg["From"] = f"{cfg.get('from_name') or ''} <{cfg['from_email']}>"
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    for path in attachments or []:
        try:
            with open(path, "rb") as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(path)}"')
                msg.attach(part)
        except Exception:
            pass

    try:
        server = smtplib.SMTP(cfg['host'], int(cfg['port']))
        if cfg.get('use_tls', True):
            server.starttls()
        server.login(cfg['username'], cfg['password'])
        server.sendmail(cfg['from_email'], [to_email], msg.as_string())
        server.quit()
        return True, "sent"
    except Exception as e:
        return False, str(e)


def _merge_text(t: str, vendor: Dict[str, Any], notice: Dict[str, Any]) -> str:
    repl = {
        "company": vendor.get("name", ""),
        "email": vendor.get("email", ""),
        "phone": vendor.get("phone", ""),
        "city": vendor.get("city", ""),
        "state": vendor.get("state", ""),
        "naics": vendor.get("naics", ""),
        "title": notice.get("Title", ""),
        "solicitation": notice.get("Solicitation", ""),
        "due": notice.get("Response Due", ""),
        "notice_id": notice.get("Notice ID", ""),
    }
    out = t
    for k, v in repl.items():
        out = out.replace(f"{{{{{k}}}}}", str(v))
    return out


def run_outreach(conn: sqlite3.Connection) -> None:
    st.header("Outreach")
    st.caption("Mail-merge RFQs to selected vendors. Uses SMTP settings from secrets.")

    notice = st.session_state.get("rfp_selected_notice", {})
    vendor_ids: List[int] = st.session_state.get("rfq_vendor_ids", [])

    if vendor_ids:
        ph = ",".join(["?"] * len(vendor_ids))
        df_sel = pd.read_sql_query(
            f"SELECT id, name, email, phone, city, state, naics FROM vendors WHERE id IN ({ph});",
            conn,
            params=vendor_ids,
        )
    else:
        st.info("No vendors queued. Use Subcontractor Finder to select vendors, or pick by filter below.")
        f_naics = st.text_input("NAICS filter")
        f_state = st.text_input("State filter")
        q = "SELECT id, name, email, phone, city, state, naics FROM vendors WHERE 1=1"
        params: List[Any] = []
        if f_naics:
            q += " AND naics LIKE ?"
            params.append(f"%{f_naics}%")
        if f_state:
            q += " AND state LIKE ?"
            params.append(f"%{f_state}%")
        df_sel = pd.read_sql_query(q + " ORDER BY name;", conn, params=params)

    st.subheader("Recipients")
    if df_sel.empty:
        st.write("No recipients")
        return
    st.dataframe(df_sel, use_container_width=True, hide_index=True)

    st.subheader("Template")
    st.markdown("Use tags: {{company}}, {{email}}, {{phone}}, {{city}}, {{state}}, {{naics}}, {{title}}, {{solicitation}}, {{due}}, {{notice_id}}")
    subj = st.text_input("Subject", value="RFQ: {{title}} (Solicitation {{solicitation}})")
    body = st.text_area(
        "Email Body (HTML supported)",
        value=(
            "Hello {{company}},<br><br>"
            "We are preparing a competitive quote for {{title}} (Solicitation {{solicitation}})."
            " Responses are due {{due}}. We’d like your quote and capability confirmation."
            "<br><br>Could you reply with pricing and any questions?"
            "<br><br>Thank you,<br>ELA Management"
        ),
        height=200,
    )

    with st.expander("Attachments", expanded=False):
        files = st.file_uploader("Attach files (optional)", type=["pdf", "docx", "xlsx", "zip"], accept_multiple_files=True)
        attach_paths: List[str] = []
        if files:
            for f in files:
                pth = save_uploaded_file(f, subdir="outreach")
                if pth:
                    attach_paths.append(pth)
            if attach_paths:
                st.success(f"Saved {len(attach_paths)} attachment(s)")

    c1, c2, c3 = st.columns([2,2,2])
    with c1:
        if st.button("Preview first merge"):
            v0 = df_sel.iloc[0].to_dict()
            st.info(f"Subject → {_merge_text(subj, v0, notice)}")
            st.write(_merge_text(body, v0, notice), unsafe_allow_html=True)
    with c2:
        if st.button("Export recipients CSV"):
            csv = df_sel.to_csv(index=False)
            path = os.path.join(DATA_DIR, "outreach_recipients.csv")
            with open(path, "w", encoding="utf-8") as f:
                f.write(csv)
            st.success("Exported recipients CSV")
            st.markdown(f"[Download recipients CSV]({path})")
    with c3:
        sent = st.button("Send emails (SMTP)", type="primary")

    if sent:
        ok = 0
        fail = 0
        log_rows = []
        for _, row in df_sel.iterrows():
            vendor = row.to_dict()
            to_email = vendor.get("email")
            if not to_email:
                log_rows.append({"vendor": vendor.get("name"), "email": "", "status": "skipped: no email"})
                continue
            s = _merge_text(subj, vendor, notice)
            b = _merge_text(body, vendor, notice)
            success, msg = send_email_smtp(to_email, s, b, attach_paths)
            ok += 1 if success else 0
            fail += 0 if success else 1
            log_rows.append({"vendor": vendor.get("name"), "email": to_email, "status": ("sent" if success else msg)})
        st.success(f"Done. Sent: {ok}  Failed: {fail}")
        df_log = pd.DataFrame(log_rows)
        st.dataframe(df_log, use_container_width=True, hide_index=True)
        path = os.path.join(DATA_DIR, "outreach_send_log.csv")
        df_log.to_csv(path, index=False)
        st.markdown(f"[Download send log]({path})")


# ---------- Quotes (Phase E) ----------
def _calc_extended(qty: Optional[float], unit_price: Optional[float]) -> Optional[float]:
    try:
        if qty is None or unit_price is None:
            return None
        return float(qty) * float(unit_price)
    except Exception:
        return None


def run_quote_comparison(conn: sqlite3.Connection) -> None:
    st.header("Quote Comparison")
    df = pd.read_sql_query("SELECT id, title, solnum FROM rfps ORDER BY id DESC;", conn)
    if df.empty:
        st.info("No RFPs in DB. Use RFP Analyzer to create one (Parse → Save).")
        return
    rfp_id = st.selectbox("RFP context", options=df["id"].tolist(), format_func=lambda rid: f"#{rid} — {df.loc[df['id']==rid, 'title'].values[0] or 'Untitled'}")

    st.subheader("Upload / Add Quotes")
    with st.expander("CSV Import", expanded=False):
        st.caption("Columns: vendor, clin, qty, unit_price, description (optional). One row = one CLIN line.")
        up = st.file_uploader("Quotes CSV", type=["csv"], key="quotes_csv")
        if up and st.button("Import Quotes CSV"):
            try:
                df_csv = pd.read_csv(up)
                required = {"vendor", "clin", "qty", "unit_price"}
                if not required.issubset({c.lower() for c in df_csv.columns}):
                    st.error("CSV missing required columns: vendor, clin, qty, unit_price")
                else:
                    df_csv.rename(columns={c: c.lower() for c in df_csv.columns}, inplace=True)
                    with closing(conn.cursor()) as cur:
                        by_vendor = df_csv.groupby("vendor", dropna=False)
                        total_rows = 0
                        for vendor, block in by_vendor:
                            cur.execute(
                                "INSERT INTO quotes(rfp_id, vendor, received_date, notes) VALUES(?,?,?,?);",
                                (int(rfp_id), str(vendor)[:200], datetime.utcnow().isoformat(), "imported")
                            )
                            qid = cur.lastrowid
                            for _, r in block.iterrows():
                                qty = float(r.get("qty", 0) or 0)
                                upx = float(r.get("unit_price", 0) or 0)
                                ext = _calc_extended(qty, upx) or 0.0
                                cur.execute(
                                    "INSERT INTO quote_lines(quote_id, clin, description, qty, unit_price, extended_price) VALUES(?,?,?,?,?,?);",
                                    (qid, str(r.get("clin",""))[:50], str(r.get("description",""))[:300], qty, upx, ext)
                                )
                                total_rows += 1
                        conn.commit()
                    st.success(f"Imported {len(by_vendor)} quotes / {total_rows} lines.")
            except Exception as e:
                st.error(f"Import failed: {e}")

    with st.expander("Add Quote (manual)", expanded=False):
        vendor = st.text_input("Vendor name")
        date = st.date_input("Received date", value=datetime.utcnow().date())
        notes = st.text_input("Notes", value="")
        add_quote = st.button("Create Quote")
        if add_quote and vendor.strip():
            with closing(conn.cursor()) as cur:
                cur.execute("INSERT INTO quotes(rfp_id, vendor, received_date, notes) VALUES(?,?,?,?);",
                            (int(rfp_id), vendor.strip(), date.isoformat(), notes.strip()))
                qid = cur.lastrowid
                conn.commit()
                st.success(f"Created quote for {vendor}. Now add lines below (Quote ID {qid}).")
                st.session_state["current_quote_id"] = qid

    df_q = pd.read_sql_query("SELECT id, vendor, received_date, notes FROM quotes WHERE rfp_id=? ORDER BY vendor;", conn, params=(rfp_id,))
    if not df_q.empty:
        st.subheader("Quotes")
        st.dataframe(df_q, use_container_width=True, hide_index=True)
        qid = st.selectbox("Edit lines for quote", options=df_q["id"].tolist(), format_func=lambda qid: f"#{qid} — {df_q.loc[df_q['id']==qid,'vendor'].values[0]}")
        with st.form("add_quote_line", clear_on_submit=True):
            c1, c2, c3 = st.columns([2, 1, 1])
            with c1:
                clin = st.text_input("CLIN")
                desc = st.text_input("Description")
            with c2:
                qty = st.number_input("Qty", min_value=0.0, step=1.0)
            with c3:
                price = st.number_input("Unit Price", min_value=0.0, step=1.0)
            submitted = st.form_submit_button("Add Line")
        if submitted:
            ext = _calc_extended(qty, price) or 0.0
            with closing(conn.cursor()) as cur:
                cur.execute(
                    "INSERT INTO quote_lines(quote_id, clin, description, qty, unit_price, extended_price) VALUES(?,?,?,?,?,?);",
                    (qid, clin.strip(), desc.strip(), float(qty), float(price), float(ext))
                )
                conn.commit()
            st.success("Line added.")

    st.subheader("Comparison")
    df_target = pd.read_sql_query("SELECT clin, description FROM clin_lines WHERE rfp_id=? GROUP BY clin, description ORDER BY clin;", conn, params=(rfp_id,))
    df_lines = pd.read_sql_query("""
        SELECT q.vendor, l.clin, l.qty, l.unit_price, l.extended_price
        FROM quote_lines l
        JOIN quotes q ON q.id = l.quote_id
        WHERE q.rfp_id=?
    """, conn, params=(rfp_id,))
    if df_lines.empty:
        st.info("No quote lines yet.")
        return

    mat = df_lines.pivot_table(index="clin", columns="vendor", values="extended_price", aggfunc="sum").fillna(0.0)
    mat = mat.sort_index()
    st.dataframe(mat.style.format("{:,.2f}"), use_container_width=True)

    best_vendor_by_clin = mat.replace(0, float("inf")).idxmin(axis=1).to_frame("Best Vendor")
    st.caption("Best vendor per CLIN")
    st.dataframe(best_vendor_by_clin, use_container_width=True, hide_index=False)

    totals = df_lines.groupby("vendor")["extended_price"].sum().to_frame("Total").sort_values("Total")
    if not df_target.empty:
        coverage = df_lines.groupby("vendor")["clin"].nunique().to_frame("CLINs Quoted")
        coverage["Required CLINs"] = df_target["clin"].nunique()
        coverage["Coverage %"] = (coverage["CLINs Quoted"] / coverage["Required CLINs"] * 100).round(1)
        totals = totals.join(coverage, how="left")
    st.subheader("Totals & Coverage")
    st.dataframe(totals.style.format({"Total": "{:,.2f}", "Coverage %": "{:.1f}"}), use_container_width=True)

    if st.button("Export comparison CSV"):
        path = os.path.join(DATA_DIR, "quote_comparison.csv")
        out = mat.copy()
        out["Best Vendor"] = best_vendor_by_clin["Best Vendor"]
        out.to_csv(path)
        st.success("Exported.")
        st.markdown(f"[Download comparison CSV]({path})")


# ---------- Pricing Calculator (Phase E) ----------
def _scenario_summary(conn: sqlite3.Connection, scenario_id: int) -> Dict[str, float]:
    dl = pd.read_sql_query("SELECT hours, rate, fringe_pct FROM pricing_labor WHERE scenario_id=?;", conn, params=(scenario_id,))
    other = pd.read_sql_query("SELECT cost FROM pricing_other WHERE scenario_id=?;", conn, params=(scenario_id,))
    base = pd.read_sql_query("SELECT overhead_pct, gna_pct, fee_pct, contingency_pct FROM pricing_scenarios WHERE id=?;", conn, params=(scenario_id,))
    if base.empty:
        return {}
    overhead_pct, gna_pct, fee_pct, contingency_pct = base.iloc[0]
    direct_labor = float((dl["hours"] * dl["rate"]).sum()) if not dl.empty else 0.0
    fringe = float((dl["hours"] * dl["rate"] * (dl["fringe_pct"].fillna(0.0) / 100)).sum()) if not dl.empty else 0.0
    other_dir = float(other["cost"].sum()) if not other.empty else 0.0
    overhead = (direct_labor + fringe) * (float(overhead_pct) / 100.0)
    gna = (direct_labor + fringe + overhead + other_dir) * (float(gna_pct) / 100.0)
    subtotal = direct_labor + fringe + overhead + gna + other_dir
    contingency = subtotal * (float(contingency_pct) / 100.0)
    fee = (subtotal + contingency) * (float(fee_pct) / 100.0)
    total = subtotal + contingency + fee
    return {
        "Direct Labor": round(direct_labor, 2),
        "Fringe": round(fringe, 2),
        "Overhead": round(overhead, 2),
        "G&A": round(gna, 2),
        "Other Direct": round(other_dir, 2),
        "Contingency": round(contingency, 2),
        "Fee/Profit": round(fee, 2),
        "Total": round(total, 2),
    }


def run_pricing_calculator(conn: sqlite3.Connection) -> None:
    st.header("Pricing Calculator")
    df = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn)
    if df.empty:
        st.info("No RFP context. Use RFP Analyzer (parse & save) first.")
        return
    rfp_id = st.selectbox("RFP context", options=df["id"].tolist(), format_func=lambda rid: f"#{rid} — {df.loc[df['id']==rid, 'title'].values[0]}")

    st.subheader("Scenario")
    df_sc = pd.read_sql_query("SELECT id, name FROM pricing_scenarios WHERE rfp_id=? ORDER BY id DESC;", conn, params=(rfp_id,))
    mode = st.radio("Mode", ["Create new", "Edit existing"], horizontal=True)
    if mode == "Create new":
        name = st.text_input("Scenario name", value="Base")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            overhead = st.number_input("Overhead %", min_value=0.0, value=20.0, step=1.0)
        with c2:
            gna = st.number_input("G&A %", min_value=0.0, value=10.0, step=1.0)
        with c3:
            fee = st.number_input("Fee/Profit %", min_value=0.0, value=7.0, step=0.5)
        with c4:
            contingency = st.number_input("Contingency %", min_value=0.0, value=0.0, step=0.5)
        if st.button("Create scenario", type="primary"):
            with closing(conn.cursor()) as cur:
                cur.execute("""
                    INSERT INTO pricing_scenarios(rfp_id, name, overhead_pct, gna_pct, fee_pct, contingency_pct, created_at)
                    VALUES(?,?,?,?,?,?,?);
                """, (int(rfp_id), name.strip(), float(overhead), float(gna), float(fee), float(contingency), datetime.utcnow().isoformat()))
                conn.commit()
            st.success("Scenario created.")
            st.rerun()
        return
    else:
        if df_sc.empty:
            st.info("No scenarios yet. Switch to 'Create new'.")
            return
        scenario_id = st.selectbox("Pick a scenario", options=df_sc["id"].tolist(), format_func=lambda sid: df_sc.loc[df_sc["id"]==sid, "name"].values[0])

    st.subheader("Labor")
    with st.form("add_labor", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
        with c1:
            cat = st.text_input("Labor Category")
        with c2:
            hrs = st.number_input("Hours", min_value=0.0, step=1.0)
        with c3:
            rate = st.number_input("Rate", min_value=0.0, step=1.0)
        with c4:
            fringe = st.number_input("Fringe %", min_value=0.0, value=0.0, step=0.5)
        add_lab = st.form_submit_button("Add labor row")
    if add_lab:
        with closing(conn.cursor()) as cur:
            cur.execute("""
                INSERT INTO pricing_labor(scenario_id, labor_cat, hours, rate, fringe_pct) VALUES(?,?,?,?,?);
            """, (int(scenario_id), cat.strip(), float(hrs), float(rate), float(fringe)))
            conn.commit()
        st.success("Added.")

    df_lab = pd.read_sql_query("""
        SELECT id, labor_cat, hours, rate, fringe_pct, (hours*rate) AS direct, (hours*rate*fringe_pct/100.0) AS fringe
        FROM pricing_labor WHERE scenario_id=?;
    """, conn, params=(scenario_id,))
    st.dataframe(df_lab, use_container_width=True, hide_index=True)

    st.subheader("Other Direct Costs")
    with st.form("add_odc", clear_on_submit=True):
        c1, c2 = st.columns([3, 1])
        with c1:
            label = st.text_input("Label")
        with c2:
            cost = st.number_input("Cost", min_value=0.0, step=100.0)
        add_odc = st.form_submit_button("Add ODC")
    if add_odc:
        with closing(conn.cursor()) as cur:
            cur.execute("INSERT INTO pricing_other(scenario_id, label, cost) VALUES(?, ?, ?);", (int(scenario_id), label.strip(), float(cost)))
            conn.commit()
        st.success("Added ODC.")

    df_odc = pd.read_sql_query("SELECT id, label, cost FROM pricing_other WHERE scenario_id=?;", conn, params=(scenario_id,))
    st.dataframe(df_odc, use_container_width=True, hide_index=True)

    st.subheader("Summary")
    s = _scenario_summary(conn, int(scenario_id))
    if not s:
        st.info("Add labor/ODCs to see a summary.")
        return
    df_sum = pd.DataFrame(list(s.items()), columns=["Component", "Amount"])
    st.dataframe(df_sum.style.format({"Amount": "{:,.2f}"}), use_container_width=True, hide_index=True)

    if st.button("Export pricing CSV"):
        path = os.path.join(DATA_DIR, f"pricing_scenario_{int(scenario_id)}.csv")
        df_sum.to_csv(path, index=False)
        st.success("Exported.")
        st.markdown(f"[Download pricing CSV]({path})")


# ---------- Win Probability (Phase E) ----------
def _price_competitiveness(conn: sqlite3.Connection, rfp_id: int, our_total: Optional[float]) -> Optional[float]:
    df = pd.read_sql_query("""
        SELECT q.vendor, SUM(l.extended_price) AS total
        FROM quotes q JOIN quote_lines l ON q.id = l.quote_id
        WHERE q.rfp_id=?
        GROUP BY q.vendor
        ORDER BY total ASC;
    """, conn, params=(rfp_id,))
    if df.empty or our_total is None:
        return None
    comp_min = float(df["total"].min())
    if our_total <= comp_min:
        return 100.0
    ratio = (our_total - comp_min) / comp_min
    if ratio <= 0.05:
        return 85 + (0.05 - ratio) * (15/0.05)
    if ratio <= 0.10:
        return 70 + (0.10 - ratio) * (15/0.05)
    if ratio <= 0.25:
        return 70 * (0.25 - ratio) / 0.15
    return 0.0


def run_win_probability(conn: sqlite3.Connection) -> None:
    st.header("Win Probability")
    df = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn)
    if df.empty:
        st.info("No RFP context. Use RFP Analyzer first.")
        return
    rfp_id = st.selectbox("RFP context", options=df["id"].tolist(), format_func=lambda rid: f"#{rid} — {df.loc[df['id']==rid, 'title'].values[0]}")

    df_items = pd.read_sql_query("SELECT status FROM lm_items WHERE rfp_id=?;", conn, params=(rfp_id,))
    if df_items.empty:
        compliance = st.slider("Compliance (est.)", 0, 100, 70)
    else:
        done = (df_items["status"] == "Complete").sum()
        total = len(df_items)
        compliance = int(round(done / max(1, total) * 100))

    tech = st.slider("Technical fit", 0, 100, 75)
    past_perf = st.slider("Past performance relevance", 0, 100, 70)
    team = st.slider("Team strength / subs readiness", 0, 100, 70)
    smallbiz = st.slider("Set-aside / socio-economic alignment", 0, 100, 80)

    df_sc = pd.read_sql_query("SELECT id, name FROM pricing_scenarios WHERE rfp_id=? ORDER BY id DESC;", conn, params=(rfp_id,))
    price_score = None
    our_total = None
    if not df_sc.empty:
        sid = st.selectbox("Use pricing scenario (optional)", options=[None] + df_sc["id"].tolist(),
                           format_func=lambda x: "None" if x is None else df_sc.loc[df_sc["id"]==x, "name"].values[0])
        if sid:
            our_total = _scenario_summary(conn, int(sid)).get("Total")
    if our_total is None:
        our_total = st.number_input("Our total price (if no scenario)", min_value=0.0, value=0.0, step=1000.0)
    price_score = _price_competitiveness(conn, int(rfp_id), our_total)
    if price_score is None:
        price_score = st.slider("Price competitiveness (est.)", 0, 100, 70)

    st.subheader("Weights")
    c1, c2, c3 = st.columns(3)
    with c1:
        w_comp = st.number_input("Weight: Compliance", 0, 100, 20)
        w_tech = st.number_input("Weight: Technical", 0, 100, 25)
    with c2:
        w_past = st.number_input("Weight: Past Perf", 0, 100, 15)
        w_team = st.number_input("Weight: Team", 0, 100, 15)
    with c3:
        w_price = st.number_input("Weight: Price", 0, 100, 25)
        w_small = st.number_input("Weight: Small Biz", 0, 100, 0)
    total_w = w_comp + w_tech + w_past + w_team + w_price + w_small
    if total_w == 0:
        st.error("Weights must sum to > 0")
        return

    comp = {
        "Compliance": compliance,
        "Technical": tech,
        "Past Performance": past_perf,
        "Team": team,
        "Price": int(round(price_score)),
        "Small Business": smallbiz,
    }
    df_scores = pd.DataFrame(list(comp.items()), columns=["Factor", "Score (0-100)"])
    st.dataframe(df_scores, use_container_width=True, hide_index=True)

    weighted = (
        compliance * w_comp + tech * w_tech + past_perf * w_past + team * w_team + int(round(price_score)) * w_price + smallbiz * w_small
    ) / total_w
    win_prob = round(float(weighted), 1)
    st.subheader(f"Estimated Win Probability: **{win_prob}%**")

    if st.button("Export assessment CSV"):
        path = os.path.join(DATA_DIR, "win_probability_assessment.csv")
        out = df_scores.copy()
        out.loc[len(out)] = ["Weighted Result", win_prob]
        out.to_csv(path, index=False)
        st.success("Exported.")
        st.markdown(f"[Download assessment CSV]({path})")


# ---------- Phase F: Chat Assistant (rules-based over DB) ----------
def _kb_search(conn: sqlite3.Connection, rfp_id: Optional[int], query: str) -> Dict[str, Any]:
    q = query.lower()
    res: Dict[str, Any] = {}
    # RFP sections
    if rfp_id:
        dfL = pd.read_sql_query("SELECT section, content FROM rfp_sections WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfL = pd.read_sql_query("SELECT section, content FROM rfp_sections;", conn)
    if not dfL.empty:
        dfL["score"] = dfL["content"].str.lower().apply(lambda t: sum(1 for w in q.split() if w in (t or "")))
        res["sections"] = dfL.sort_values("score", ascending=False).head(5)

    # Checklist
    if rfp_id:
        dfCk = pd.read_sql_query("SELECT item_text, status FROM lm_items WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfCk = pd.read_sql_query("SELECT item_text, status FROM lm_items;", conn)
    if not dfCk.empty:
        dfCk["score"] = dfCk["item_text"].str.lower().apply(lambda t: sum(1 for w in q.split() if w in (t or "")))
        res["checklist"] = dfCk.sort_values("score", ascending=False).head(10)

    # CLINs
    if rfp_id:
        dfCL = pd.read_sql_query("SELECT clin, description, qty, unit FROM clin_lines WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfCL = pd.read_sql_query("SELECT clin, description, qty, unit FROM clin_lines;", conn)
    if not dfCL.empty:
        dfCL["score"] = (dfCL["clin"].astype(str) + " " + dfCL["description"].astype(str)).str.lower().apply(lambda t: sum(1 for w in q.split() if w in (t or "")))
        res["clins"] = dfCL.sort_values("score", ascending=False).head(10)

    # Dates
    if rfp_id:
        dfDt = pd.read_sql_query("SELECT label, date_text FROM key_dates WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfDt = pd.read_sql_query("SELECT label, date_text FROM key_dates;", conn)
    if not dfDt.empty:
        dfDt["score"] = (dfDt["label"].astype(str) + " " + dfDt["date_text"].astype(str)).str.lower().apply(lambda t: sum(1 for w in q.split() if w in (t or "")))
        res["dates"] = dfDt.sort_values("score", ascending=False).head(10)

    # POCs
    if rfp_id:
        dfP = pd.read_sql_query("SELECT name, role, email, phone FROM pocs WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfP = pd.read_sql_query("SELECT name, role, email, phone FROM pocs;", conn)
    if not dfP.empty:
        dfP["score"] = (dfP["name"].astype(str) + " " + dfP["role"].astype(str) + " " + dfP["email"].astype(str)).str.lower().apply(lambda t: sum(1 for w in q.split() if w in (t or "")))
        res["pocs"] = dfP.sort_values("score", ascending=False).head(10)

    # Quotes summary by vendor
    if rfp_id:
        dfQ = pd.read_sql_query("""
            SELECT q.vendor, SUM(l.extended_price) AS total, COUNT(DISTINCT l.clin) AS clins_quoted
            FROM quotes q JOIN quote_lines l ON q.id=l.quote_id
            WHERE q.rfp_id=?
            GROUP BY q.vendor
            ORDER BY total ASC;
        """, conn, params=(rfp_id,))
        res["quotes"] = dfQ

    # Coverage & compliance
    if rfp_id:
        df_target = pd.read_sql_query("SELECT DISTINCT clin FROM clin_lines WHERE rfp_id=?;", conn, params=(rfp_id,))
        total_clins = int(df_target["clin"].nunique()) if not df_target.empty else 0
        df_items = pd.read_sql_query("SELECT status FROM lm_items WHERE rfp_id=?;", conn, params=(rfp_id,))
        compl = 0
        if not df_items.empty:
            compl = int(round(((df_items["status"]=="Complete").sum() / max(1, len(df_items))) * 100))
        res["meta"] = {"total_clins": total_clins, "compliance_pct": compl}

    return res


def run_chat_assistant(conn: sqlite3.Connection) -> None:
    st.header("Chat Assistant (DB-aware)")
    st.caption("Answers from your saved RFPs, checklist, CLINs, dates, POCs, quotes, and pricing — no external API.")

    df_rf = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn)
    rfp_opt = None
    if not df_rf.empty:
        rfp_opt = st.selectbox("Context (optional)", options=[None] + df_rf["id"].tolist(),
                               format_func=lambda rid: "All RFPs" if rid is None else f"#{rid} — {df_rf.loc[df_rf['id']==rid, 'title'].values[0]}")

    q = st.text_input("Ask a question (e.g., 'When are proposals due?', 'Show POCs', 'Which vendor is lowest?')")
    ask = st.button("Ask", type="primary")
    if not ask:
        st.caption("Quick picks: due date • POCs • open checklist • CLINs • quotes total • compliance")
        return

    res = _kb_search(conn, rfp_opt, q or "")
    # Heuristic intents
    ql = (q or "").lower()
    if any(w in ql for w in ["due", "deadline", "close"]):
        st.subheader("Key Dates")
        df = res.get("dates", pd.DataFrame())
        if df is not None and not df.empty:
            st.dataframe(df[["label","date_text"]], use_container_width=True, hide_index=True)
    if any(w in ql for w in ["poc", "contact", "officer", "specialist"]):
        st.subheader("Points of Contact")
        df = res.get("pocs", pd.DataFrame())
        if df is not None and not df.empty:
            st.dataframe(df[["name","role","email","phone"]], use_container_width=True, hide_index=True)
    if "clin" in ql:
        st.subheader("CLINs")
        df = res.get("clins", pd.DataFrame())
        if df is not None and not df.empty:
            st.dataframe(df[["clin","description","qty","unit"]], use_container_width=True, hide_index=True)
    if any(w in ql for w in ["checklist", "compliance"]):
        st.subheader("Checklist (top hits)")
        df = res.get("checklist", pd.DataFrame())
        if df is not None and not df.empty:
            st.dataframe(df[["item_text","status"]], use_container_width=True, hide_index=True)
        meta = res.get("meta", {})
        if meta:
            st.info(f"Compliance completion: {meta.get('compliance_pct',0)}%")
    if any(w in ql for w in ["quote", "price", "vendor", "lowest"]):
        st.subheader("Quote Totals by Vendor")
        df = res.get("quotes", pd.DataFrame())
        if df is not None and not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption("Lowest total appears at the top.")

    # Generic best-matches
    sec = res.get("sections", pd.DataFrame())
    if sec is not None and not sec.empty:
        st.subheader("Relevant RFP Sections (snippets)")
        sh = sec.copy()
        sh["snippet"] = sh["content"].str.slice(0, 400)
        st.dataframe(sh[["section","snippet","score"]], use_container_width=True, hide_index=True)


# ---------- Phase F: Capability Statement ----------
def _export_capability_docx(path: str, profile: Dict[str, str]) -> Optional[str]:
    try:
        from docx import Document  # type: ignore
        from docx.shared import Pt, Inches  # type: ignore
    except Exception:
        st.error("python-docx is required. pip install python-docx")
        return None

    doc = Document()
    for s in doc.sections:
        s.top_margin = Inches(0.7); s.bottom_margin = Inches(0.7); s.left_margin = Inches(0.7); s.right_margin = Inches(0.7)

    title = profile.get("company_name") or "Capability Statement"
    doc.add_heading(title, level=1)
    if profile.get("tagline"):
        p = doc.add_paragraph(profile["tagline"]); p.runs[0].italic = True

    meta = [
        ("Address", "address"), ("Phone", "phone"), ("Email", "email"), ("Website", "website"),
        ("UEI", "uei"), ("CAGE", "cage")
    ]
    p = doc.add_paragraph()
    for label, key in meta:
        val = profile.get(key, "")
        if val:
            p.add_run(f"{label}: {val}  ")

    def add_bullets(title, key):
        txt = (profile.get(key) or "").strip()
        if not txt:
            return
        doc.add_heading(title, level=2)
        for line in [x.strip() for x in txt.splitlines() if x.strip()]:
            doc.add_paragraph(line, style="List Bullet")

    # Content blocks
    add_bullets("Core Competencies", "core_competencies")
    add_bullets("Differentiators", "differentiators")
    add_bullets("Certifications", "certifications")
    add_bullets("Past Performance Highlights", "past_performance")

    naics = (profile.get("naics") or "").replace(",", ", ")
    if naics.strip():
        doc.add_heading("NAICS Codes", level=2)
        doc.add_paragraph(naics)

    contact = profile.get("primary_poc", "")
    if contact.strip():
        doc.add_heading("Primary POC", level=2)
        doc.add_paragraph(contact)

    doc.save(path)
    return path


def run_capability_statement(conn: sqlite3.Connection) -> None:
    st.header("Capability Statement")
    st.caption("Store your company profile and export a polished 1-page DOCX capability statement.")

    # Load existing (id=1)
    df = pd.read_sql_query("SELECT * FROM org_profile WHERE id=1;", conn)
    vals = df.iloc[0].to_dict() if not df.empty else {}

    with st.form("org_profile_form"):
        c1, c2 = st.columns([2,2])
        with c1:
            company_name = st.text_input("Company Name", value=vals.get("company_name",""))
            tagline = st.text_input("Tagline (optional)", value=vals.get("tagline",""))
            address = st.text_area("Address", value=vals.get("address",""), height=70)
            phone = st.text_input("Phone", value=vals.get("phone",""))
            email = st.text_input("Email", value=vals.get("email",""))
            website = st.text_input("Website", value=vals.get("website",""))
        with c2:
            uei = st.text_input("UEI", value=vals.get("uei",""))
            cage = st.text_input("CAGE", value=vals.get("cage",""))
            naics = st.text_input("NAICS (comma separated)", value=vals.get("naics",""))
            core_competencies = st.text_area("Core Competencies (one per line)", value=vals.get("core_competencies",""), height=110)
            differentiators = st.text_area("Differentiators (one per line)", value=vals.get("differentiators",""), height=110)
        c3, c4 = st.columns([2,2])
        with c3:
            certifications = st.text_area("Certifications (one per line)", value=vals.get("certifications",""), height=110)
        with c4:
            past_performance = st.text_area("Past Performance Highlights (one per line)", value=vals.get("past_performance",""), height=110)
            primary_poc = st.text_area("Primary POC (name, title, email, phone)", value=vals.get("primary_poc",""), height=70)
        saved = st.form_submit_button("Save Profile", type="primary")

    if saved:
        try:
            with closing(conn.cursor()) as cur:
                cur.execute("DELETE FROM org_profile WHERE id=1;")
                cur.execute("""
                    INSERT INTO org_profile(id, company_name, tagline, address, phone, email, website, uei, cage, naics, core_competencies, differentiators, certifications, past_performance, primary_poc)
                    VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                """, (company_name, tagline, address, phone, email, website, uei, cage, naics, core_competencies, differentiators, certifications, past_performance, primary_poc))
                conn.commit()
            st.success("Profile saved.")
        except Exception as e:
            st.error(f"Save failed: {e}")

    # Export
    if st.button("Export Capability Statement DOCX"):
        prof = pd.read_sql_query("SELECT * FROM org_profile WHERE id=1;", conn)
        if prof.empty:
            st.error("Save your profile first.")
        else:
            p = prof.iloc[0].to_dict()
            path = os.path.join(DATA_DIR, "Capability_Statement.docx")
            out = _export_capability_docx(path, p)
            if out:
                st.success("Exported.")
                st.markdown(f"[Download DOCX]({out})")




# ---------- Phase G: Past Performance Library + Generator ----------
def _pp_score_one(rec: dict, rfp_title: str, rfp_sections: pd.DataFrame) -> int:
    title = (rfp_title or "").lower()
    hay = (title + " " + " ".join((rfp_sections["content"].tolist() if isinstance(rfp_sections, pd.DataFrame) and not rfp_sections.empty else []))).lower()
    score = 0
    # NAICS bonus
    if rec.get("naics") and rec["naics"] in hay:
        score += 40
    # Keywords
    kws = (rec.get("keywords") or "").lower().replace(";", ",").split(",")
    kws = [k.strip() for k in kws if k.strip()]
    for k in kws[:10]:
        if k in hay:
            score += 6
    # Recency via POP end
    try:
        from datetime import datetime
        if rec.get("pop_end"):
            y = int(str(rec["pop_end"]).split("-")[0])
            age = max(0, datetime.now().year - y)
            score += max(0, 20 - (age * 4))  # up to +20, decays 4/yr
    except Exception:
        pass
    # CPARS bonus
    if (rec.get("cpars_rating") or "").strip():
        score += 8
    # Value signal
    try:
        val = float(rec.get("value") or 0)
        if val >= 1000000: score += 6
        elif val >= 250000: score += 3
    except Exception:
        pass
    return min(score, 100)


def _pp_writeup_block(rec: dict) -> str:
    parts = []
    title = rec.get("project_title") or "Project"
    cust = rec.get("customer") or ""
    cn = rec.get("contract_no") or ""
    role = rec.get("role") or ""
    pop = " – ".join([x for x in [rec.get("pop_start") or "", rec.get("pop_end") or ""] if x])
    val = rec.get("value") or ""
    parts.append(f"**{title}** — {cust} {('(' + cn + ')') if cn else ''}")
    meta_bits = [b for b in [f"Role: {role}" if role else "", f"POP: {pop}" if pop else "", f"Value: ${val:,.0f}" if isinstance(val,(int,float)) else (f"Value: {val}" if val else ""), f"NAICS: {rec.get('naics','')}"] if b]
    if meta_bits:
        parts.append("  \n" + " | ".join(meta_bits))
    if rec.get("scope"):
        parts.append(f"**Scope/Work:** {rec['scope']}")
    if rec.get("results"):
        parts.append(f"**Results/Outcome:** {rec['results']}")
    if rec.get("cpars_rating"):
        parts.append(f"**CPARS:** {rec['cpars_rating']}")
    if any([rec.get("contact_name"), rec.get("contact_email"), rec.get("contact_phone")]):
        parts.append("**POC:** " + ", ".join([x for x in [rec.get("contact_name"), rec.get("contact_email"), rec.get("contact_phone")] if x]))
    return "\n\n".join(parts)


def _export_past_perf_docx(path: str, records: list) -> str | None:
    try:
        from docx import Document  # type: ignore
        from docx.shared import Inches  # type: ignore
    except Exception:
        st.error("python-docx is required. pip install python-docx")
        return None
    doc = Document()
    for s in doc.sections:
        s.top_margin = Inches(1); s.bottom_margin = Inches(1); s.left_margin = Inches(1); s.right_margin = Inches(1)
    doc.add_heading("Past Performance", level=1)
    for i, rec in enumerate(records, start=1):
        doc.add_heading(f"{i}. {rec.get('project_title')}", level=2)
        blk = _pp_writeup_block(rec).replace("**", "")  # simple conversion
        for para in blk.split("\n\n"):
            doc.add_paragraph(para)
    doc.save(path)
    return path


def run_past_performance(conn: sqlite3.Connection) -> None:
    st.header("Past Performance Library")
    st.caption("Store/import projects, score relevance vs an RFP, generate writeups, and push to Proposal Builder.")

    # CSV Import
    with st.expander("Import CSV", expanded=False):
        st.caption("Columns: project_title, customer, contract_no, naics, role, pop_start, pop_end, value, scope, results, cpars_rating, contact_name, contact_email, contact_phone, keywords, notes")
        up = st.file_uploader("Upload CSV", type=["csv"], key="pp_csv")
        if up and st.button("Import", key="pp_do_import"):
            try:
                df = pd.read_csv(up)
                # Normalize headers
                df.columns = [c.strip().lower() for c in df.columns]
                required = {"project_title"}
                if not required.issubset(set(df.columns)):
                    st.error("CSV must include at least 'project_title'")
                else:
                    n=0
                    with closing(conn.cursor()) as cur:
                        for _, r in df.iterrows():
                            cur.execute("""
                                INSERT INTO past_perf(project_title, customer, contract_no, naics, role, pop_start, pop_end, value, scope, results, cpars_rating, contact_name, contact_email, contact_phone, keywords, notes)
                                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                            """, (
                                str(r.get("project_title",""))[:200],
                                str(r.get("customer",""))[:200],
                                str(r.get("contract_no",""))[:100],
                                str(r.get("naics",""))[:20],
                                str(r.get("role",""))[:100],
                                str(r.get("pop_start",""))[:20],
                                str(r.get("pop_end",""))[:20],
                                float(r.get("value")) if str(r.get("value","")).strip() not in ("","nan") else None,
                                str(r.get("scope",""))[:2000],
                                str(r.get("results",""))[:2000],
                                str(r.get("cpars_rating",""))[:100],
                                str(r.get("contact_name",""))[:200],
                                str(r.get("contact_email",""))[:200],
                                str(r.get("contact_phone",""))[:100],
                                str(r.get("keywords",""))[:500],
                                str(r.get("notes",""))[:500],
                            ))
                            n+=1
                    conn.commit()
                    st.success(f"Imported {n} projects.")
            except Exception as e:
                st.error(f"Import failed: {e}")

    # Add Project
    with st.expander("Add Project", expanded=False):
        c1, c2, c3 = st.columns([2,2,2])
        with c1:
            project_title = st.text_input("Project Title")
            customer = st.text_input("Customer (Agency/Prime)")
            contract_no = st.text_input("Contract #")
            naics = st.text_input("NAICS")
            role = st.text_input("Role (Prime/Sub)")
        with c2:
            pop_start = st.text_input("POP Start (YYYY-MM)")
            pop_end = st.text_input("POP End (YYYY-MM)")
            value = st.text_input("Value (number)")
            cpars_rating = st.text_input("CPARS Rating (optional)")
            keywords = st.text_input("Keywords (comma-separated)")
        with c3:
            contact_name = st.text_input("POC Name")
            contact_email = st.text_input("POC Email")
            contact_phone = st.text_input("POC Phone")
            scope = st.text_area("Scope/Work", height=100)
            results = st.text_area("Results/Outcome", height=100)
        notes = st.text_area("Notes", height=70)
        if st.button("Save Project", key="pp_save_project"):
            try:
                v = float(value) if value.strip() else None
            except Exception:
                v = None
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute("""
                        INSERT INTO past_perf(project_title, customer, contract_no, naics, role, pop_start, pop_end, value, scope, results, cpars_rating, contact_name, contact_email, contact_phone, keywords, notes)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    """, (project_title.strip(), customer.strip(), contract_no.strip(), naics.strip(), role.strip(), pop_start.strip(), pop_end.strip(), v, scope.strip(), results.strip(), cpars_rating.strip(), contact_name.strip(), contact_email.strip(), contact_phone.strip(), keywords.strip(), notes.strip()))
                    conn.commit()
                st.success("Saved project.")
            except Exception as e:
                st.error(f"Save failed: {e}")

    # Filters
    with st.expander("Filter", expanded=True):
        f1, f2, f3 = st.columns([2,2,2])
        with f1:
            f_kw = st.text_input("Keyword in title/scope/results")
        with f2:
            f_naics = st.text_input("NAICS filter")
        with f3:
            f_role = st.text_input("Role filter")
    q = "SELECT * FROM past_perf WHERE 1=1"
    params = []
    if f_kw:
        q += " AND (project_title LIKE ? OR scope LIKE ? OR results LIKE ?)"
        params.extend([f"%{f_kw}%", f"%{f_kw}%", f"%{f_kw}%"])
    if f_naics:
        q += " AND naics LIKE ?"
        params.append(f"%{f_naics}%")
    if f_role:
        q += " AND role LIKE ?"
        params.append(f"%{f_role}%")
    df = pd.read_sql_query(q + " ORDER BY id DESC;", conn, params=params)
    if df.empty:
        st.info("No projects found.")
        return

    st.subheader("Projects")
    st.dataframe(df[["id","project_title","customer","contract_no","naics","role","pop_start","pop_end","value","cpars_rating"]], use_container_width=True, hide_index=True)
    selected_ids = st.multiselect("Select projects for writeup", options=df["id"].tolist(), format_func=lambda i: f"#{i} — {df.loc[df['id']==i, 'project_title'].values[0]}")

    # Relevance scoring vs RFP
    df_rf = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn)
    rfp_id = None
    if not df_rf.empty:
        rfp_id = st.selectbox("RFP context for relevance scoring (optional)", options=[None] + df_rf["id"].tolist(),
                              format_func=lambda rid: "None" if rid is None else f"#{rid} — {df_rf.loc[df_rf['id']==rid,'title'].values[0]}")
    if rfp_id:
        ctx = _load_rfp_context(conn, int(rfp_id))
        title = (ctx["rfp"].iloc[0]["title"] if ctx["rfp"] is not None and not ctx["rfp"].empty else "")
        secs = ctx.get("sections", pd.DataFrame())
        # Compute scores
        scores = []
        for _, r in df.iterrows():
            scores.append(_pp_score_one(r.to_dict(), title, secs))
        df_sc = df.copy()
        df_sc["Relevance"] = scores
        st.subheader("Relevance vs selected RFP")
        st.dataframe(df_sc[["project_title","naics","role","pop_end","value","Relevance"]].sort_values("Relevance", ascending=False),
                     use_container_width=True, hide_index=True)

    # Generate writeups
    st.subheader("Generate Writeups")
    tone = st.selectbox("Template", ["Concise bullets", "Narrative paragraph"])
    max_n = st.slider("How many projects", 1, 7, min(3, len(selected_ids)) if selected_ids else 3)
    do_gen = st.button("Generate", type="primary")
    if do_gen:
        picked = df[df["id"].isin(selected_ids)].head(max_n).to_dict(orient="records")
        if not picked:
            st.error("Select at least one project.")
            return
        # Build markdown text
        blocks = []
        for r in picked:
            blk = _pp_writeup_block(r)
            if tone == "Concise bullets":
                # convert sentences to bullets
                bullets = []
                for line in blk.split("\n"):
                    line = line.strip()
                    if not line: 
                        continue
                    if not line.startswith("**"):
                        bullets.append(f"- {line}")
                    else:
                        bullets.append(line)
                blocks.append("\n".join(bullets))
            else:
                blocks.append(blk)
        final_md = "\n\n".join(blocks)
        st.markdown("**Preview**")
        st.write(final_md)

        # Push to Proposal Builder section
        st.session_state["pb_section_Past Performance Summary"] = final_md
        st.success("Pushed to Proposal Builder → Past Performance Summary")

        # Export DOCX
        out_path = str(Path(DATA_DIR) / "Past_Performance_Writeups.docx")
        exp = _export_past_perf_docx(out_path, picked)
        if exp:
            st.markdown(f"[Download DOCX]({exp})")




# ---------- Phase H: White Paper Builder ----------
def _wp_load_template(conn: sqlite3.Connection, template_id: int) -> pd.DataFrame:
    return pd.read_sql_query(
        "SELECT id, position, title, body FROM white_template_sections WHERE template_id=? ORDER BY position ASC;",
        conn, params=(template_id,)
    )

def _wp_load_paper(conn: sqlite3.Connection, paper_id: int) -> pd.DataFrame:
    return pd.read_sql_query(
        "SELECT id, position, title, body, image_path FROM white_paper_sections WHERE paper_id=? ORDER BY position ASC;",
        conn, params=(paper_id,)
    )

def _wp_export_docx(path: str, title: str, subtitle: str, sections: pd.DataFrame) -> str | None:
    try:
        from docx import Document  # type: ignore
        from docx.shared import Inches  # type: ignore
    except Exception:
        st.error("python-docx is required. pip install python-docx")
        return None
    doc = Document()
    doc.add_heading(title or "White Paper", level=1)
    if subtitle:
        p = doc.add_paragraph(subtitle); p.runs[0].italic = True
    for _, r in sections.sort_values("position").iterrows():
        doc.add_heading(r.get("title") or "Section", level=2)
        body = r.get("body") or ""
        for para in str(body).split("\n\n"):
            if para.strip():
                doc.add_paragraph(para.strip())
        img = r.get("image_path")
        if img and Path(img).exists():
            try:
                doc.add_picture(img, width=Inches(5.5))
            except Exception:
                pass
    doc.save(path)
    return path

def run_white_paper_builder(conn: sqlite3.Connection) -> None:
    st.header("White Paper Builder")
    st.caption("Templates → Drafts → DOCX export. Can include images per section.")

    # --- Templates ---
    with st.expander("Templates", expanded=False):
        df_t = pd.read_sql_query("SELECT id, name, description, created_at FROM white_templates ORDER BY id DESC;", conn)
        t_col1, t_col2 = st.columns([2,2])
        with t_col1:
            st.subheader("Create Template")
            t_name = st.text_input("Template name", key="wp_t_name")
            t_desc = st.text_area("Description", key="wp_t_desc", height=70)
            if st.button("Save Template", key="wp_t_save"):
                if not t_name.strip():
                    st.error("Name required")
                else:
                    with closing(conn.cursor()) as cur:
                        cur.execute("INSERT INTO white_templates(name, description, created_at) VALUES(?,?,datetime('now'));", (t_name.strip(), t_desc.strip()))
                        conn.commit()
                    st.success("Template saved"); st.rerun()
        with t_col2:
            if df_t.empty:
                st.info("No templates yet.")
            else:
                st.subheader("Edit Template Sections")
                t_sel = st.selectbox("Choose template", options=df_t["id"].tolist(), format_func=lambda tid: df_t.loc[df_t["id"]==tid, "name"].values[0], key="wp_t_sel")
                df_ts = _wp_load_template(conn, int(t_sel))
                st.dataframe(df_ts, use_container_width=True, hide_index=True)
                st.markdown("**Add section**")
                ts_title = st.text_input("Section title", key="wp_ts_title")
                ts_body = st.text_area("Default body", key="wp_ts_body", height=120)
                if st.button("Add section to template", key="wp_ts_add"):
                    pos = int((df_ts["position"].max() if not df_ts.empty else 0) + 1)
                    with closing(conn.cursor()) as cur:
                        cur.execute("INSERT INTO white_template_sections(template_id, position, title, body) VALUES(?,?,?,?);",
                                    (int(t_sel), pos, ts_title.strip(), ts_body.strip()))
                        conn.commit()
                    st.success("Section added"); st.rerun()
                # Reorder / delete (simple)
                if not df_ts.empty:
                    st.markdown("**Reorder / Delete**")
                    for _, r in df_ts.iterrows():
                        c1, c2, c3 = st.columns([2,1,1])
                        with c1:
                            new_pos = st.number_input(f"#{int(r['id'])} pos", min_value=1, value=int(r['position']), step=1, key=f"wp_ts_pos_{int(r['id'])}")
                        with c2:
                            if st.button("Apply", key=f"wp_ts_pos_apply_{int(r['id'])}"):
                                with closing(conn.cursor()) as cur:
                                    cur.execute("UPDATE white_template_sections SET position=? WHERE id=?;", (int(new_pos), int(r["id"])))
                                    conn.commit()
                                st.success("Updated position"); st.rerun()
                        with c3:
                            if st.button("Delete", key=f"wp_ts_del_{int(r['id'])}"):
                                with closing(conn.cursor()) as cur:
                                    cur.execute("DELETE FROM white_template_sections WHERE id=?;", (int(r["id"]),))
                                    conn.commit()
                                st.success("Deleted"); st.rerun()

    st.divider()

    # --- Drafts ---
    st.subheader("Drafts")
    df_p = pd.read_sql_query("SELECT id, title, subtitle, created_at, updated_at FROM white_papers ORDER BY id DESC;", conn)
    c1, c2 = st.columns([2,2])
    with c1:
        st.markdown("**Create draft from template**")
        df_t = pd.read_sql_query("SELECT id, name FROM white_templates ORDER BY id DESC;", conn)
        d_title = st.text_input("Draft title", key="wp_d_title")
        d_sub = st.text_input("Subtitle (optional)", key="wp_d_sub")
        if df_t.empty:
            st.caption("No templates available")
            t_sel2 = None
        else:
            t_sel2 = st.selectbox("Template", options=[None] + df_t["id"].tolist(),
                                  format_func=lambda x: "Blank" if x is None else df_t.loc[df_t["id"]==x, "name"].values[0],
                                  key="wp_d_template")
        if st.button("Create draft", key="wp_d_create"):
            if not d_title.strip():
                st.error("Title required")
            else:
                with closing(conn.cursor()) as cur:
                    cur.execute("INSERT INTO white_papers(title, subtitle, rfp_id, created_at, updated_at) VALUES(?,?,?,?,datetime('now'));",
                                (d_title.strip(), d_sub.strip(), None, datetime.utcnow().isoformat()))
                    pid = cur.lastrowid
                    if t_sel2:
                        df_ts2 = _wp_load_template(conn, int(t_sel2))
                        for _, r in df_ts2.sort_values("position").iterrows():
                            cur.execute("INSERT INTO white_paper_sections(paper_id, position, title, body) VALUES(?,?,?,?);",
                                        (int(pid), int(r["position"]), r.get("title"), r.get("body")))
                    conn.commit()
                st.success("Draft created"); st.rerun()
    with c2:
        if df_p.empty:
            st.info("No drafts yet.")
        else:
            st.markdown("**Open a draft**")
            p_sel = st.selectbox("Draft", options=df_p["id"].tolist(), format_func=lambda pid: df_p.loc[df_p["id"]==pid, "title"].values[0], key="wp_d_sel")

    # Editing panel
    if 'p_sel' in locals() and p_sel:
        st.subheader(f"Editing draft #{int(p_sel)}")
        df_sec = _wp_load_paper(conn, int(p_sel))
        # Add section
        st.markdown("**Add section**")
        ns_title = st.text_input("Section title", key="wp_ns_title")
        ns_body = st.text_area("Body", key="wp_ns_body", height=140)
        ns_img = st.file_uploader("Optional image", type=["png","jpg","jpeg"], key="wp_ns_img")
        if st.button("Add section", key="wp_ns_add"):
            img_path = None
            if ns_img is not None:
                img_path = save_uploaded_file(ns_img, subdir="whitepapers")
            pos = int((df_sec["position"].max() if not df_sec.empty else 0) + 1)
            with closing(conn.cursor()) as cur:
                cur.execute("INSERT INTO white_paper_sections(paper_id, position, title, body, image_path) VALUES(?,?,?,?,?);",
                            (int(p_sel), pos, ns_title.strip(), ns_body.strip(), img_path))
                cur.execute("UPDATE white_papers SET updated_at=datetime('now') WHERE id=?;", (int(p_sel),))
                conn.commit()
            st.success("Section added"); st.rerun()

        # Section list
        if df_sec.empty:
            st.info("No sections yet.")
        else:
            for _, r in df_sec.iterrows():
                st.markdown(f"**Section #{int(r['position'])}: {r.get('title') or 'Untitled'}**")
                e1, e2, e3, e4 = st.columns([2,1,1,1])
                with e1:
                    new_title = st.text_input("Title", value=r.get("title") or "", key=f"wp_sec_title_{int(r['id'])}")
                    new_body = st.text_area("Body", value=r.get("body") or "", key=f"wp_sec_body_{int(r['id'])}", height=140)
                with e2:
                    new_pos = st.number_input("Pos", value=int(r["position"]), min_value=1, step=1, key=f"wp_sec_pos_{int(r['id'])}")
                    if st.button("Apply", key=f"wp_sec_apply_{int(r['id'])}"):
                        with closing(conn.cursor()) as cur:
                            cur.execute("UPDATE white_paper_sections SET title=?, body=?, position=? WHERE id=?;",
                                        (new_title.strip(), new_body.strip(), int(new_pos), int(r["id"])))
                            cur.execute("UPDATE white_papers SET updated_at=datetime('now') WHERE id=?;", (int(p_sel),))
                            conn.commit()
                        st.success("Updated"); st.rerun()
                with e3:
                    up_img = st.file_uploader("Replace image", type=["png","jpg","jpeg"], key=f"wp_sec_img_{int(r['id'])}")
                    if st.button("Save image", key=f"wp_sec_img_save_{int(r['id'])}"):
                        if up_img is None:
                            st.warning("Choose an image first")
                        else:
                            img_path = save_uploaded_file(up_img, subdir="whitepapers")
                            with closing(conn.cursor()) as cur:
                                cur.execute("UPDATE white_paper_sections SET image_path=? WHERE id=?;", (img_path, int(r["id"])))
                                cur.execute("UPDATE white_papers SET updated_at=datetime('now') WHERE id=?;", (int(p_sel),))
                                conn.commit()
                            st.success("Image saved"); st.rerun()
                with e4:
                    if st.button("Delete", key=f"wp_sec_del_{int(r['id'])}"):
                        with closing(conn.cursor()) as cur:
                            cur.execute("DELETE FROM white_paper_sections WHERE id=?;", (int(r["id"]),))
                            cur.execute("UPDATE white_papers SET updated_at=datetime('now') WHERE id=?;", (int(p_sel),))
                            conn.commit()
                        st.success("Deleted"); st.rerun()
                st.divider()

            # Export & Push
            x1, x2 = st.columns([2,2])
            with x1:
                if st.button("Export DOCX", key="wp_export"):
                    out_path = str(Path(DATA_DIR) / f"White_Paper_{int(p_sel)}.docx")
                    exp = _wp_export_docx(out_path,
                                          df_p.loc[df_p["id"]==p_sel, "title"].values[0],
                                          df_p.loc[df_p["id"]==p_sel, "subtitle"].values[0] if "subtitle" in df_p.columns else "",
                                          _wp_load_paper(conn, int(p_sel)))
                    if exp:
                        st.success("Exported"); st.markdown(f"[Download DOCX]({exp})")
            with x2:
                if st.button("Push narrative to Proposal Builder", key="wp_push"):
                    # Concatenate sections to markdown
                    secs = _wp_load_paper(conn, int(p_sel))
                    lines = []
                    for _, rr in secs.sort_values("position").iterrows():
                        lines.append(f"## {rr.get('title') or 'Section'}\n\n{rr.get('body') or ''}")
                    md = "\n\n".join(lines)
                    st.session_state["pb_section_White Paper"] = md
                    st.success("Pushed to Proposal Builder → 'White Paper' section")

# ---------- nav + main ----------
def init_session() -> None:
    if "initialized" not in st.session_state:
        st.session_state.initialized = True


def nav() -> str:
    st.sidebar.title("Workspace")
    st.sidebar.caption(BUILD_LABEL)
    st.sidebar.caption(f"SHA {_file_hash()}")
    return st.sidebar.selectbox(
        "Go to",
        [
            "SAM Watch",
            "RFP Analyzer",
            "L and M Checklist",
            "Proposal Builder",
            "Past Performance",
            "White Paper Builder",
            "Subcontractor Finder",
            "Outreach",
            "Quote Comparison",
            "Pricing Calculator",
            "Win Probability",
            "Chat Assistant",
            "Capability Statement",
            "Contacts",
            "Deals",
        ],
    )


def router(page: str, conn: sqlite3.Connection) -> None:
    if page == "SAM Watch":
        run_sam_watch(conn)
    elif page == "RFP Analyzer":
        run_rfp_analyzer(conn)
    elif page == "L and M Checklist":
        run_lm_checklist(conn)
    elif page == "Proposal Builder":
        run_proposal_builder(conn)
    elif page == "Past Performance":
        run_past_performance(conn)
    elif page == "White Paper Builder":
        run_white_paper_builder(conn)
    elif page == "Subcontractor Finder":
        run_subcontractor_finder(conn)
    elif page == "Outreach":
        run_outreach(conn)
    elif page == "Quote Comparison":
        run_quote_comparison(conn)
    elif page == "Pricing Calculator":
        run_pricing_calculator(conn)
    elif page == "Win Probability":
        run_win_probability(conn)
    elif page == "Chat Assistant":
        run_chat_assistant(conn)
    elif page == "Capability Statement":
        run_capability_statement(conn)
    elif page == "Contacts":
        run_contacts(conn)
    elif page == "Deals":
        run_deals(conn)
    else:
        st.error("Unknown page")


def main() -> None:
    conn = get_db()
    st.title(APP_TITLE)
    st.caption(BUILD_LABEL)
    router(nav(), conn)


if __name__ == "__main__":
    main()
