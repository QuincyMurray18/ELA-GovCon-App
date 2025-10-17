
import re
import os
import sqlite3
from contextlib import closing
from typing import Optional, Any, Dict, List, Tuple
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import json
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
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_vendors_naics_state ON vendors(naics, state);")
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
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_quote_lines ON quote_lines(quote_id, clin);")

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

        cur.execute("""
            CREATE TABLE IF NOT EXISTS activities(
                id INTEGER PRIMARY KEY,
                ts TEXT,
                type TEXT,
                subject TEXT,
                notes TEXT,
                deal_id INTEGER REFERENCES deals(id) ON DELETE SET NULL,
                contact_id INTEGER REFERENCES contacts(id) ON DELETE SET NULL
            );
        """)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_activities_ts ON activities(ts);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_activities_rel ON activities(deal_id, contact_id);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tasks(
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                due_date TEXT,
                status TEXT DEFAULT 'Open',
                priority TEXT DEFAULT 'Normal',
                deal_id INTEGER REFERENCES deals(id) ON DELETE SET NULL,
                contact_id INTEGER REFERENCES contacts(id) ON DELETE SET NULL,
                created_at TEXT,
                completed_at TEXT
            );
        """)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(due_date, status);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS deal_stage_log(
                id INTEGER PRIMARY KEY,
                deal_id INTEGER NOT NULL REFERENCES deals(id) ON DELETE CASCADE,
                stage TEXT NOT NULL,
                changed_at TEXT
            );
        """)

        # Phase J (File Manager)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS files(
                id INTEGER PRIMARY KEY,
                owner_type TEXT,            -- 'RFP' | 'Deal' | 'Vendor' | 'Other'
                owner_id INTEGER,           -- nullable when owner_type='Other'
                filename TEXT,
                path TEXT,
                size INTEGER,
                mime TEXT,
                tags TEXT,
                notes TEXT,
                uploaded_at TEXT
            );
        """)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_owner ON files(owner_type, owner_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_tags ON files(tags);")

        # Phase L (RFQ Pack)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rfq_packs(
                id INTEGER PRIMARY KEY,
                rfp_id INTEGER REFERENCES rfps(id) ON DELETE SET NULL,
                deal_id INTEGER REFERENCES deals(id) ON DELETE SET NULL,
                title TEXT NOT NULL,
                instructions TEXT,
                due_date TEXT,
                created_at TEXT,
                updated_at TEXT
            );
        """)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_rfq_packs_ctx ON rfq_packs(rfp_id, deal_id);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rfq_lines(
                id INTEGER PRIMARY KEY,
                pack_id INTEGER NOT NULL REFERENCES rfq_packs(id) ON DELETE CASCADE,
                clin_code TEXT,
                description TEXT,
                qty REAL,
                unit TEXT,
                naics TEXT,
                psc TEXT
            );
        """)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_rfq_lines_pack ON rfq_lines(pack_id);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rfq_vendors(
                id INTEGER PRIMARY KEY,
                pack_id INTEGER NOT NULL REFERENCES rfq_packs(id) ON DELETE CASCADE,
                vendor_id INTEGER NOT NULL REFERENCES vendors(id) ON DELETE CASCADE
            );
        """)
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_rfq_vendors_unique ON rfq_vendors(pack_id, vendor_id);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rfq_attach(
                id INTEGER PRIMARY KEY,
                pack_id INTEGER NOT NULL REFERENCES rfq_packs(id) ON DELETE CASCADE,
                file_id INTEGER REFERENCES files(id) ON DELETE SET NULL,
                name TEXT,
                path TEXT
            );
        """)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_rfq_attach_pack ON rfq_attach(pack_id);")

        # Phase M (Tenancy 1)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tenants(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                created_at TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS current_tenant(
                id INTEGER PRIMARY KEY CHECK(id=1),
                ctid INTEGER
            );
        """)
        cur.execute(
            "INSERT OR IGNORE INTO tenants(id, name, created_at) VALUES(1, 'Default', datetime('now'));")
        cur.execute(
            "INSERT OR IGNORE INTO current_tenant(id, ctid) VALUES(1, 1);")

        def _add_tenant_id(table: str):
            try:
                cols = pd.read_sql_query(f"PRAGMA table_info({table});", conn)
                if "tenant_id" not in cols["name"].tolist():
                    cur.execute(
                        f"ALTER TABLE {table} ADD COLUMN tenant_id INTEGER;")
                    conn.commit()
            except Exception:
                pass
            try:
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{table}_tenant ON {table}(tenant_id);")
            except Exception:
                pass

        core_tables = ["rfps", "lm_items", "lm_meta", "deals", "activities", "tasks", "deal_stage_log",
                       "vendors", "files", "rfq_packs", "rfq_lines", "rfq_vendors", "rfq_attach", "contacts"]
        for t in core_tables:
            _add_tenant_id(t)

        # AFTER INSERT triggers: always stamp tenant_id to current_tenant
        def _ensure_trigger(table: str):
            trg = f"{table}_ai_tenant"
            try:
                cur.execute(f"""
                    CREATE TRIGGER IF NOT EXISTS {trg}
                    AFTER INSERT ON {table}
                    BEGIN
                        UPDATE {table}
                        SET tenant_id=(SELECT ctid FROM current_tenant WHERE id=1)
                        WHERE rowid=NEW.rowid;
                    END;
                """)
            except Exception:
                pass
        for t in core_tables:
            _ensure_trigger(t)

        # Scoped views
        def _create_view(table: str):
            v = f"{table}_t"
            try:
                cur.execute(
                    f"CREATE VIEW IF NOT EXISTS {v} AS SELECT * FROM {table} WHERE tenant_id=(SELECT ctid FROM current_tenant WHERE id=1);")
            except Exception:
                pass
        for t in core_tables:
            _create_view(t)

        # Phase N (Persist): Pragmas
        try:
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.execute("PRAGMA busy_timeout=5000;")
        except Exception:
            pass

        # Schema version for migrations
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_version(
                id INTEGER PRIMARY KEY CHECK(id=1),
                ver INTEGER
            );
        """)
        cur.execute(
            "INSERT OR IGNORE INTO schema_version(id, ver) VALUES(1, 0);")
        conn.commit()
    try:
        migrate(conn)
    except Exception:
        pass
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
            resp = requests.get(SAM_ENDPOINT, params=q, headers={
                                "X-Api-Key": api_key}, timeout=30)
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
        org_path = r.get("fullParentPathName") or r.get(
            "organizationName") or ""
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


def extract_sections_L_M(text: str) -> dict:
    out = {}
    if not text:
        return out
    mL = re.search(
        r'(SECTION\s+L[\s\S]*?)(?=SECTION\s+[A-Z]|\Z)', text, re.IGNORECASE)
    if mL:
        out['L'] = mL.group(1)
    mM = re.search(
        r'(SECTION\s+M[\s\S]*?)(?=SECTION\s+[A-Z]|\Z)', text, re.IGNORECASE)
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
            mqty = re.search(
                r'\b(QTY|Quantity)[:\s]*([0-9,.]+)', ln + ' ' + desc, re.IGNORECASE)
            qty = mqty.group(2) if mqty else ''
            munit = re.search(
                r'\b(UNIT|Units?)[:\s]*([A-Za-z/]+)', ln + ' ' + desc, re.IGNORECASE)
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
            out.append({'label': m.group(1).strip(),
                       'date_text': m.group(2).strip(), 'date_iso': ''})
    return out[:200]


def extract_pocs(text: str) -> list:
    if not text:
        return []
    emails = list(set(re.findall(r'[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}', text)))
    phones = list(set(re.findall(
        r'(?:\+?1\s*)?(?:\(\d{3}\)|\d{3})[\s\-]?\d{3}[\s\-]?\d{4}', text)))
    poc_blocks = re.findall(
        r'(Contracting Officer|Contract Specialist|Point of Contact|POC).*?(?:\n\n|$)', text, re.IGNORECASE | re.DOTALL)
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
            "SELECT name, email, org FROM contacts_t ORDER BY name;", conn
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
            value = st.number_input(
                "Est Value", min_value=0.0, step=1000.0, format="%.2f")
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
            "SELECT title, agency, status, value, sam_url FROM deals_t ORDER BY id DESC;",
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
    st.caption("Save searches, monitor a watchlist, and push winners to Deals.")

    # --- helpers (scoped here to avoid collisions) ---
    def _ensure_sam_p_schema(_conn: sqlite3.Connection) -> None:
        with closing(_conn.cursor()) as _c:
            _c.execute("""
                CREATE TABLE IF NOT EXISTS sam_searches(
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    params_json TEXT NOT NULL,
                    auto_push INTEGER DEFAULT 0,
                    created_at TEXT,
                    updated_at TEXT
                );
            """)
            _c.execute("""
                CREATE TABLE IF NOT EXISTS sam_watch(
                    id INTEGER PRIMARY KEY,
                    notice_id TEXT UNIQUE,
                    title TEXT,
                    solnum TEXT,
                    agency TEXT,
                    posted TEXT,
                    link TEXT,
                    added_at TEXT
                );
            """)
            _c.execute("""
                CREATE TABLE IF NOT EXISTS sam_runs(
                    id INTEGER PRIMARY KEY,
                    search_id INTEGER REFERENCES sam_searches(id) ON DELETE CASCADE,
                    run_at TEXT,
                    new_count INTEGER,
                    seen_ids TEXT
                );
            """)
            # tenancy-aware fallback views (okay if they already exist)
            try:
                _c.execute("""CREATE VIEW IF NOT EXISTS sam_searches_t AS
                              SELECT * FROM sam_searches
                              WHERE COALESCE(tenant_id,(SELECT ctid FROM current_tenant WHERE id=1)) =
                                    (SELECT ctid FROM current_tenant WHERE id=1);""")
            except Exception:
                _c.execute(
                    "CREATE VIEW IF NOT EXISTS sam_searches_t AS SELECT * FROM sam_searches;")
            try:
                _c.execute("""CREATE VIEW IF NOT EXISTS sam_watch_t AS
                              SELECT * FROM sam_watch
                              WHERE COALESCE(tenant_id,(SELECT ctid FROM current_tenant WHERE id=1)) =
                                    (SELECT ctid FROM current_tenant WHERE id=1);""")
            except Exception:
                _c.execute(
                    "CREATE VIEW IF NOT EXISTS sam_watch_t AS SELECT * FROM sam_watch;")
            try:
                _c.execute("""CREATE VIEW IF NOT EXISTS sam_runs_t AS
                              SELECT * FROM sam_runs
                              WHERE COALESCE(tenant_id,(SELECT ctid FROM current_tenant WHERE id=1)) =
                                    (SELECT ctid FROM current_tenant WHERE id=1);""")
            except Exception:
                _c.execute(
                    "CREATE VIEW IF NOT EXISTS sam_runs_t AS SELECT * FROM sam_runs;")
            _conn.commit()

    def _safe_read(sql1: str, sql_fallback: str | None = None, params: tuple = ()):
        try:
            return pd.read_sql_query(sql1, conn, params=params)
        except Exception:
            if sql_fallback:
                return pd.read_sql_query(sql_fallback, conn, params=params)
            raise

    def _mmddyyyy(d) -> str:
        try:
            return pd.to_datetime(d).strftime("%m/%d/%Y")
        except Exception:
            return ""

    def _sam_build_params(ui: dict) -> dict:
        # valid ptype codes: p,k,r,g,s,f,i,u,a
        ptype_map = {
            "Presolicitation": "p",
            "Combined Synopsis/Solicitation": "k",
            "Sources Sought": "r",
            "Sale of Surplus Property": "g",
            "Special Notice": "s",
            "Award Notice": "a",
            "Intent to Bundle Requirements (DoD-Funded)": "i",
            "Justification (J&A)": "u",
            # "Solicitation": not in allowed list above; map to 'k' to avoid 400s
            "Solicitation": "k",
        }
        today = datetime.now().date()
        default_from = today - timedelta(days=30)

        p = {"limit": int(ui.get("limit", 200)),
             "api_key": get_sam_api_key(), "_max_pages": 3}
        pf = ui.get("posted_from") or default_from
        pt = ui.get("posted_to") or today
        p["postedFrom"] = _mmddyyyy(pf)
        p["postedTo"] = _mmddyyyy(pt)

        if ui.get("active_only"):
            p["status"] = "active"
        if ui.get("keywords"):
            p["title"] = ui["keywords"]
        if ui.get("naics"):
            p["ncode"] = str(ui["naics"]).strip()
        if ui.get("set_aside"):
            sa_map = {"SB": "SBA", "WOSB": "WOSB", "SDVOSB": "SDVOSBC",
                      "8A": "8A", "HUBZone": "HZC", "VOSB": "VSA", "SDB": "SBA"}
            p["typeOfSetAside"] = sa_map.get(ui["set_aside"], ui["set_aside"])
        if ui.get("state"):
            p["state"] = ui["state"]
        if ui.get("org_name"):
            p["organizationName"] = ui["org_name"]

        sel = ui.get("types") or []
        if isinstance(sel, str):
            sel = [s.strip() for s in sel.split(",") if s.strip()]
        codes = [ptype_map[t] for t in sel if t in ptype_map]
        if codes:
            p["ptype"] = ",".join(codes)
        return p

    _ensure_sam_p_schema(conn)

    tab_search, tab_saved, tab_watch = st.tabs(
        ["Search", "Saved", "Watchlist"])

    # ---------------- SEARCH ----------------
    with tab_search:
        today = datetime.now().date()
        default_from = today - timedelta(days=30)

        with st.expander("Filters", expanded=True):
            c1, c2, c3 = st.columns([2, 2, 2])
            with c1:
                use_dates = st.checkbox(
                    "Filter by posted date", value=False, key="sam_use_dates")
            with c2:
                active_only = st.checkbox(
                    "Active only", value=True, key="sam_active_only")
            with c3:
                limit = st.number_input(
                    "Max records", min_value=10, max_value=1000, step=10, value=200, key="sam_limit")

            posted_from = default_from
            posted_to = today
            if use_dates:
                d1, d2 = st.columns([2, 2])
                with d1:
                    posted_from = st.date_input(
                        "Posted From", value=default_from, key="sam_posted_from")
                with d2:
                    posted_to = st.date_input(
                        "Posted To", value=today, key="sam_posted_to")

            e1, e2, e3 = st.columns([2, 2, 2])
            with e1:
                keywords = st.text_input(
                    "Keywords (title contains)", key="sam_kw")
            with e2:
                naics = st.text_input("NAICS (6-digit)", key="sam_naics")
            with e3:
                set_aside = st.selectbox(
                    "Set-Aside", options=["", "SB", "WOSB", "SDVOSB", "8A", "HUBZone", "VOSB", "SDB"], key="sam_sa")

            f1, f2, f3 = st.columns([2, 2, 2])
            with f1:
                state = st.text_input("State (e.g., VA)", key="sam_state")
            with f2:
                org_name = st.text_input("Agency/Org name", key="sam_org")
            with f3:
                types = st.multiselect(
                    "Notice types",
                    options=[
                        "Presolicitation",
                        "Solicitation",
                        "Combined Synopsis/Solicitation",
                        "Sources Sought",
                        "Special Notice",
                        "Award Notice",
                        "Justification (J&A)",
                        "Sale of Surplus Property",
                        "Intent to Bundle Requirements (DoD-Funded)",
                    ],
                    key="sam_types",
                )

            # Save search
            st.markdown("**Save this search**")
            s1, s2 = st.columns([3, 1])
            with s1:
                sname = st.text_input("Search name", key="sam_save_name",
                                      placeholder="e.g., VA Hospitals – 541519 – SDVOSB")
            with s2:
                auto_push = st.checkbox(
                    "Auto-push to Deals", value=False, key="sam_auto_push")

            c_run, c_save = st.columns([2, 2])
            with c_run:
                run_search = st.button(
                    "Run Search", type="primary", key="sam_run")
            with c_save:
                if st.button("Save Search", key="sam_save_btn"):
                    params = _sam_build_params(
                        {
                            "limit": limit,
                            "posted_from": (posted_from if use_dates else default_from),
                            "posted_to": (posted_to if use_dates else today),
                            "active_only": active_only,
                            "keywords": keywords,
                            "naics": naics,
                            "set_aside": set_aside,
                            "state": state,
                            "org_name": org_name,
                            "types": types,
                        }
                    )
                    with closing(conn.cursor()) as cur:
                        cur.execute(
                            """
                            INSERT INTO sam_searches(name, params_json, auto_push, created_at, updated_at)
                            VALUES(?,?,?,?,datetime('now'));
                            """,
                            (sname.strip() or "Saved Search", json.dumps(params),
                             1 if auto_push else 0, datetime.utcnow().isoformat()),
                        )
                        conn.commit()
                    st.success("Saved search")

        # Execute ad-hoc search
        if run_search:
            params = _sam_build_params(
                {
                    "limit": limit,
                    "posted_from": (posted_from if use_dates else default_from),
                    "posted_to": (posted_to if use_dates else today),
                    "active_only": active_only,
                    "keywords": keywords,
                    "naics": naics,
                    "set_aside": set_aside,
                    "state": state,
                    "org_name": org_name,
                    "types": types,
                }
            )
            with st.spinner("Searching SAM.gov..."):
                out = sam_search_cached(params)

            if out.get("error"):
                st.error(out["error"])
            else:
                records = out.get("records", [])
                st.success(
                    f"Found {out.get('totalRecords', 0)} (showing {len(records)})")
                df = pd.DataFrame(records)
                if not df.empty:
                    cols = [c for c in ["noticeId", "title", "solicitationNumber", "postedDate",
                                        "department", "office", "type", "naics", "baseType", "link"] if c in df.columns]
                    st.dataframe(
                        df[cols], use_container_width=True, hide_index=True)

                    selected = st.multiselect("Select notices", options=df.get(
                        "noticeId", pd.Series(dtype=str)).tolist(), key="sam_sel")
                    g1, g2, g3 = st.columns([2, 2, 2])

                    with g1:
                        if st.button("⭐ Add to Watchlist", key="sam_add_watch"):
                            with closing(conn.cursor()) as cur:
                                for nid in selected:
                                    try:
                                        row = df[df["noticeId"] ==
                                                 nid].iloc[0].to_dict()
                                        cur.execute(
                                            """
                                            INSERT OR IGNORE INTO sam_watch(notice_id, title, solnum, agency, posted, link, added_at)
                                            VALUES(?,?,?,?,?,?,datetime('now'));
                                            """,
                                            (
                                                row.get("noticeId"),
                                                row.get("title"),
                                                row.get("solicitationNumber"),
                                                row.get("department") or row.get(
                                                    "office"),
                                                row.get("postedDate"),
                                                row.get("link"),
                                            ),
                                        )
                                    except Exception:
                                        pass
                                conn.commit()
                            st.success("Added to watchlist")

                    with g2:
                        if st.button("Push to Deals", key="sam_push_deals"):
                            pushed = 0
                            with closing(conn.cursor()) as cur:
                                for nid in selected:
                                    try:
                                        r = df[df["noticeId"] ==
                                               nid].iloc[0].to_dict()
                                        cur.execute(
                                            """
                                            INSERT INTO deals(title, solnum, agency, status, stage, created_at, source_url)
                                            VALUES(?, ?, ?, 'Open', 'New', datetime('now'), ?);
                                            """,
                                            (
                                                r.get("title") or "Untitled",
                                                r.get(
                                                    "solicitationNumber") or "",
                                                r.get("department") or r.get(
                                                    "office") or "",
                                                r.get("link") or "",
                                            ),
                                        )
                                        pushed += 1
                                    except Exception:
                                        pass
                                conn.commit()
                            st.success(f"Pushed {pushed} deal(s)")

                    with g3:
                        if st.button("Export CSV", key="sam_export_csv"):
                            csv_bytes = df.to_csv(index=False).encode("utf-8")
                            st.download_button(
                                "Download CSV", data=csv_bytes, file_name=f"sam_results_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv")

    # ---------------- SAVED ----------------
    with tab_saved:
        df_s = _safe_read(
            "SELECT id, name, auto_push, created_at, updated_at FROM sam_searches_t ORDER BY id DESC;",
            "SELECT id, name, auto_push, created_at, updated_at FROM sam_searches ORDER BY id DESC;",
        )
        if df_s.empty:
            st.info("No saved searches yet.")
        else:
            st.dataframe(df_s, use_container_width=True, hide_index=True)
            s_sel = st.selectbox(
                "Pick a saved search",
                options=df_s["id"].tolist(),
                format_func=lambda i: f"#{i} — {df_s.loc[df_s['id'] == i, 'name'].values[0]}",
                key="sam_saved_sel",
            )

            c1, c2, c3, c4 = st.columns([2, 2, 2, 2])

            with c1:
                if st.button("Run Saved Search", type="primary", key="sam_run_saved"):
                    row = _safe_read("SELECT params_json, auto_push FROM sam_searches WHERE id=?;", None, params=(
                        int(s_sel),)).iloc[0]
                    ui = json.loads(row["params_json"])
                    params = _sam_build_params(
                        {
                            "limit": ui.get("limit", 200),
                            "posted_from": ui.get("postedFrom") or ui.get("posted_from"),
                            "posted_to": ui.get("postedTo") or ui.get("posted_to"),
                            "active_only": (ui.get("status") == "active") or (ui.get("active") in ("true", True)),
                            "keywords": ui.get("title"),
                            "naics": ui.get("ncode") or ui.get("naics"),
                            "set_aside": ui.get("typeOfSetAside"),
                            "state": ui.get("state"),
                            "org_name": ui.get("organizationName"),
                            "types": ui.get("types") or ui.get("ptype"),
                        }
                    )
                    with st.spinner("Searching SAM.gov..."):
                        out = sam_search_cached(params)
                    if out.get("error"):
                        st.error(out["error"])
                    else:
                        recs = out.get("records", [])
                        st.success(
                            f"Found {out.get('totalRecords', 0)} (showing {len(recs)})")
                        df = pd.DataFrame(recs)
                        if not df.empty:
                            show_cols = [c for c in ["noticeId", "title", "solicitationNumber",
                                                     "postedDate", "department", "type", "naics", "link"] if c in df.columns]
                            st.dataframe(
                                df[show_cols], use_container_width=True, hide_index=True)
                            if int(row["auto_push"]) == 1:
                                with closing(conn.cursor()) as cur:
                                    for _, r in df.iterrows():
                                        try:
                                            cur.execute(
                                                """
                                                INSERT INTO deals(title, solnum, agency, status, stage, created_at, source_url)
                                                VALUES(?, ?, ?, 'Open', 'New', datetime('now'), ?);
                                                """,
                                                (r.get("title") or "Untitled", r.get("solicitationNumber") or "", r.get(
                                                    "department") or "", r.get("link") or ""),
                                            )
                                        except Exception:
                                            pass
                                    conn.commit()
                                st.info("Auto-pushed to Deals.")
                            try:
                                seen = ",".join(df["noticeId"].astype(
                                    str).tolist()) if "noticeId" in df.columns else ""
                                with closing(conn.cursor()) as cur:
                                    cur.execute("INSERT INTO sam_runs(search_id, run_at, new_count, seen_ids) VALUES(?, datetime('now'), ?, ?);", (int(
                                        s_sel), int(len(df)), seen))
                                    conn.commit()
                            except Exception:
                                pass

            with c2:
                if st.button("Export Alerts CSV", key="sam_saved_alert_csv"):
                    df_r = _safe_read(
                        "SELECT run_at, new_count FROM sam_runs_t WHERE search_id=? ORDER BY id DESC LIMIT 1;",
                        "SELECT run_at, new_count FROM sam_runs WHERE search_id=? ORDER BY id DESC LIMIT 1;",
                        params=(int(s_sel),),
                    )
                    last = df_r.iloc[0].to_dict() if not df_r.empty else {
                        "run_at": "—", "new_count": 0}
                    csv_bytes = pd.DataFrame(
                        [{"Search": df_s[df_s["id"] == s_sel]["name"].values[0],
                            "Last Run (UTC)": last["run_at"], "Last Count": last["new_count"]}]
                    ).to_csv(index=False).encode("utf-8")
                    st.download_button("Download Alerts CSV", data=csv_bytes,
                                       file_name=f"sam_alert_{int(s_sel)}_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv")

            with c3:
                new_name = st.text_input("Rename to", key="sam_saved_rename")
                if st.button("Rename", key="sam_saved_rename_btn"):
                    with closing(conn.cursor()) as cur:
                        cur.execute("UPDATE sam_searches SET name=?, updated_at=datetime('now') WHERE id=?;", (
                            new_name.strip() or "Saved Search", int(s_sel)))
                        conn.commit()
                    st.success("Renamed")
                    st.rerun()

            with c4:
                if st.button("Delete Saved", key="sam_saved_delete"):
                    with closing(conn.cursor()) as cur:
                        cur.execute(
                            "DELETE FROM sam_searches WHERE id=?;", (int(s_sel),))
                        conn.commit()
                    st.success("Deleted")
                    st.rerun()

    # ---------------- WATCHLIST ----------------
    with tab_watch:
        df_w = _safe_read(
            "SELECT id, notice_id, title, solnum, agency, posted, link, added_at FROM sam_watch_t ORDER BY added_at DESC;",
            "SELECT id, notice_id, title, solnum, agency, posted, link, added_at FROM sam_watch ORDER BY added_at DESC;",
        )
        st.dataframe(df_w, use_container_width=True, hide_index=True)
        w_sel = st.multiselect(
            "Select watchlist items",
            options=(df_w["id"].astype(int).tolist()
                     if not df_w.empty else []),
            key="sam_watch_sel",
            format_func=lambda i: f"#{i} — {df_w.loc[df_w['id'] == i, 'title'].values[0][:60]}",
        )

        h1, h2, h3 = st.columns([2, 2, 2])
        with h1:
            if st.button("Push selected to Deals", key="sam_watch_push"):
                pushed = 0
                with closing(conn.cursor()) as cur:
                    for wid in w_sel:
                        try:
                            row = df_w[df_w["id"] == wid].iloc[0].to_dict()
                            cur.execute(
                                """
                                INSERT INTO deals(title, solnum, agency, status, stage, created_at, source_url)
                                VALUES(?, ?, ?, 'Open', 'New', datetime('now'), ?);
                                """,
                                (row.get("title") or "Untitled", row.get("solnum") or "", row.get(
                                    "agency") or "", row.get("link") or ""),
                            )
                            pushed += 1
                        except Exception:
                            pass
                    conn.commit()
                st.success(f"Pushed {pushed} deal(s)")
        with h2:
            if st.button("Remove from Watchlist", key="sam_watch_remove"):
                with closing(conn.cursor()) as cur:
                    for wid in w_sel:
                        try:
                            cur.execute(
                                "DELETE FROM sam_watch WHERE id=?;", (int(wid),))
                        except Exception:
                            pass
                    conn.commit()
                st.success("Removed")
                st.rerun()
        with h3:
            if st.button("Export Watchlist CSV", key="sam_watch_csv"):
                csv_bytes = (df_w if not df_w.empty else pd.DataFrame()).to_csv(
                    index=False).encode("utf-8")
                st.download_button("Download Watchlist CSV", data=csv_bytes,
                                   file_name=f"sam_watch_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv")

    def _sam_build_params(ui: dict) -> dict:
        # Date format MM/dd/yyyy
        def fmt(d):
            try:
                return pd.to_datetime(d).strftime("%m/%d/%Y")
            except Exception:
                return None
        today = datetime.now().date()
        default_from = today - timedelta(days=30)

        p = {"limit": int(ui.get("limit", 200)),
             "api_key": get_sam_api_key(), "_max_pages": 3}

        pf = ui.get("posted_from") or default_from
        pt = ui.get("posted_to") or today
        p["postedFrom"] = fmt(pf)
        p["postedTo"] = fmt(pt)

        if ui.get("active_only"):
            p["status"] = "active"

        if ui.get("keywords"):
            p["title"] = ui["keywords"]
        # NAICS uses ncode per spec
        if ui.get("naics"):
            p["ncode"] = str(ui["naics"]).strip()

        if ui.get("set_aside"):
            # pass through; user may give official codes (e.g., SDVOSBC); otherwise try light map
            sa_map = {"SB": "SBA", "WOSB": "WOSB", "SDVOSB": "SDVOSBC",
                      "8A": "8A", "HUBZone": "HZC", "VOSB": "VSA", "SDB": "SBA"}
            p["typeOfSetAside"] = sa_map.get(ui["set_aside"], ui["set_aside"])

        if ui.get("state"):
            p["state"] = ui["state"]
        if ui.get("org_name"):
            p["organizationName"] = ui["org_name"]

        # ptype letters
        type_map = {
            "Presolicitation": "p",
            "Solicitation": "o",
            "Combined Synopsis/Solicitation": "k",
            "Sources Sought": "r",
            "Special Notice": "s",
            "Award Notice": "a",
            "Justification (J&A)": "u",
            "Sale of Surplus Property": "g",
            "Intent to Bundle Requirements (DoD-Funded)": "i",
        }
        sel = ui.get("types") or []
        if sel:
            codes = [type_map[t] for t in sel if t in type_map]
            if codes:
                p["ptype"] = ",".join(codes)

        return p
    # --- ensure Phase P tables/views exist + safe read helper ---

    def _ensure_sam_p_schema(_conn: sqlite3.Connection):
        with closing(_conn.cursor()) as _c:
            _c.execute("""
                CREATE TABLE IF NOT EXISTS sam_searches(
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    params_json TEXT NOT NULL,
                    auto_push INTEGER DEFAULT 0,
                    created_at TEXT,
                    updated_at TEXT
                );
            """)
            _c.execute("""
                CREATE TABLE IF NOT EXISTS sam_watch(
                    id INTEGER PRIMARY KEY,
                    notice_id TEXT UNIQUE,
                    title TEXT,
                    solnum TEXT,
                    agency TEXT,
                    posted TEXT,
                    link TEXT,
                    added_at TEXT
                );
            """)
            _c.execute("""
                CREATE TABLE IF NOT EXISTS sam_runs(
                    id INTEGER PRIMARY KEY,
                    search_id INTEGER REFERENCES sam_searches(id) ON DELETE CASCADE,
                    run_at TEXT,
                    new_count INTEGER,
                    seen_ids TEXT
                );
            """)
            # Tenancy-aware views if current_tenant exists; otherwise simple passthrough views
            try:
                _c.execute("""CREATE VIEW IF NOT EXISTS sam_searches_t AS
                              SELECT * FROM sam_searches
                              WHERE COALESCE(tenant_id,(SELECT ctid FROM current_tenant WHERE id=1)) =
                                    (SELECT ctid FROM current_tenant WHERE id=1);""")
            except Exception:
                _c.execute(
                    "CREATE VIEW IF NOT EXISTS sam_searches_t AS SELECT * FROM sam_searches;")
            try:
                _c.execute("""CREATE VIEW IF NOT EXISTS sam_watch_t AS
                              SELECT * FROM sam_watch
                              WHERE COALESCE(tenant_id,(SELECT ctid FROM current_tenant WHERE id=1)) =
                                    (SELECT ctid FROM current_tenant WHERE id=1);""")
            except Exception:
                _c.execute(
                    "CREATE VIEW IF NOT EXISTS sam_watch_t AS SELECT * FROM sam_watch;")
            try:
                _c.execute("""CREATE VIEW IF NOT EXISTS sam_runs_t AS
                              SELECT * FROM sam_runs
                              WHERE COALESCE(tenant_id,(SELECT ctid FROM current_tenant WHERE id=1)) =
                                    (SELECT ctid FROM current_tenant WHERE id=1);""")
            except Exception:
                _c.execute(
                    "CREATE VIEW IF NOT EXISTS sam_runs_t AS SELECT * FROM sam_runs;")
            _conn.commit()

    def _safe_read(sql1: str, sql_fallback: str | None = None, params: tuple = ()):
        try:
            return pd.read_sql_query(sql1, conn, params=params)
        except Exception:
            if sql_fallback:
                return pd.read_sql_query(sql_fallback, conn, params=params)
            raise

    _ensure_sam_p_schema(conn)
    st.header("SAM Watch")
    st.caption("Save searches, monitor watchlist, and push winners to Deals.")

    # Tabs
    tab_search, tab_saved, tab_watch = st.tabs(
        ["Search", "Saved", "Watchlist"])

    # ---------- SEARCH TAB ----------
    with tab_search:
        with st.expander("Filters", expanded=True):
            today = datetime.now().date()
            default_from = today - timedelta(days=30)

            c1, c2, c3 = st.columns([2, 2, 2])
            with c1:
                use_dates = st.checkbox(
                    "Filter by posted date", value=False, key="sam_use_dates")
            with c2:
                active_only = st.checkbox(
                    "Active only", value=True, key="sam_active_only")
            with c3:
                limit = st.number_input(
                    "Max records", min_value=10, max_value=1000, step=10, value=200, key="sam_limit")

            if use_dates:
                d1, d2 = st.columns([2, 2])
                with d1:
                    posted_from = st.date_input(
                        "Posted From", value=default_from, key="sam_posted_from")
                with d2:
                    posted_to = st.date_input(
                        "Posted To", value=today, key="sam_posted_to")

            e1, e2, e3 = st.columns([2, 2, 2])
            with e1:
                keywords = st.text_input(
                    "Keywords (title contains)", key="sam_kw")
            with e2:
                naics = st.text_input("NAICS (6-digit)", key="sam_naics")
            with e3:
                set_aside = st.selectbox(
                    "Set-Aside", options=["", "SB", "WOSB", "SDVOSB", "8A", "HUBZone", "VOSB", "SDB"], key="sam_sa")

            f1, f2, f3 = st.columns([2, 2, 2])
            with f1:
                state = st.text_input("State (e.g., VA)", key="sam_state")
            with f2:
                org_name = st.text_input("Agency/Org name", key="sam_org")
            with f3:
                types = st.multiselect("Notice types", options=[
                                       "Presolicitation", "Solicitation", "Combined Synopsis/Solicitation", "Sources Sought", "Special Notice", "Award Notice"], key="sam_types")

            # Save search controls
            st.markdown("**Save this search**")
            s1, s2 = st.columns([3, 1])
            with s1:
                sname = st.text_input("Search name", key="sam_save_name",
                                      placeholder="e.g., VA Hospitals – 541519 – SDVOSB")
            with s2:
                auto_push = st.checkbox(
                    "Auto-push to Deals", value=False, key="sam_auto_push")

            c_run, c_save = st.columns([2, 2])
            with c_run:
                run_search = st.button(
                    "Run Search", type="primary", key="sam_run")
            with c_save:
                if st.button("Save Search", key="sam_save_btn"):
            params = _sam_build_params({
                "limit": limit,
                "posted_from": (posted_from if use_dates else default_from),
                "posted_to": (posted_to if use_dates else today),
                "active_only": active_only,
                "keywords": keywords,
                "naics": naics,
                "set_aside": set_aside,
                "state": state,
                "org_name": org_name,
                "types": types,
            })
            with closing(conn.cursor()) as cur:
                cur.execute(
                    """
                    INSERT INTO sam_searches(name, params_json, auto_push, created_at, updated_at)
                    VALUES(?,?,?,?,datetime('now'));
                    """,
                    (sname.strip() or "Saved Search", json.dumps(params),
                     1 if auto_push else 0, datetime.utcnow().isoformat()),
                )
                conn.commit()
            st.success("Saved search")

        # Execute ad-hoc search

        if run_search:
            params = _sam_build_params({

                "limit": limit,
                "posted_from": (posted_from if use_dates else default_from),
                "posted_to": (posted_to if use_dates else today),
                "active_only": active_only,
                "keywords": keywords,
                "naics": naics,
                "set_aside": set_aside,
                "state": state,
                "org_name": org_name,
                "types": types,
            })
            with st.spinner("Searching SAM.gov..."):
                out = sam_search_cached(params)

            if out.get("error"):
                st.error(out["error"])
            else:
                records = out.get("records", [])
                st.success(
                    f"Found {out.get('totalRecords', 0)} (showing {len(records)})")
                df = pd.DataFrame(records)
                if not df.empty:
                    cols = [c for c in ["noticeId", "title", "solicitationNumber", "postedDate",
                                        "department", "office", "type", "naics", "baseType", "link"] if c in df.columns]
                    st.dataframe(
                        df[cols], use_container_width=True, hide_index=True)

                    selected = st.multiselect("Select notices", options=df.get(
                        "noticeId", pd.Series(dtype=str)).tolist(), key="sam_sel")
                    g1, g2, g3 = st.columns([2, 2, 2])
                    with g1:
                        if st.button("⭐ Add to Watchlist", key="sam_add_watch"):
                            with closing(conn.cursor()) as cur:
                                for nid in selected:
                                    try:
                                        row = df[df["noticeId"] ==
                                                 nid].iloc[0].to_dict()
                                        cur.execute(
                                            """
                                            INSERT OR IGNORE INTO sam_watch(notice_id, title, solnum, agency, posted, link, added_at)
                                            VALUES(?,?,?,?,?,?,datetime('now'));
                                            """,
                                            (row.get("noticeId"), row.get("title"), row.get("solicitationNumber"),
                                             row.get("department") or row.get(
                                                 "office"),
                                             row.get("postedDate"), row.get("link"))
                                        )
                                    except Exception:
                                        pass
                                conn.commit()
                            st.success("Added to watchlist")
                    with g2:
                        if st.button("Push to Deals", key="sam_push_deals"):
                            pushed = 0
                            with closing(conn.cursor()) as cur:
                                for nid in selected:
                                    try:
                                        r = df[df["noticeId"] ==
                                               nid].iloc[0].to_dict()
                                        cur.execute(
                                            """
                                            INSERT INTO deals(title, solnum, agency, status, stage, created_at, source_url)
                                            VALUES(?, ?, ?, 'Open', 'New', datetime('now'), ?);
                                            """,
                                            (r.get("title") or "Untitled",
                                             r.get("solicitationNumber") or "",
                                             r.get("department") or r.get(
                                                 "office") or "",
                                             r.get("link") or "")
                                        )
                                        pushed += 1
                                    except Exception:
                                        pass
                                conn.commit()
                            st.success(f"Pushed {pushed} deal(s)")
                    with g3:
                        if st.button("Export CSV", key="sam_export_csv"):
                            path = str(Path(
                                DATA_DIR) / f"sam_results_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}.csv")
                            df.to_csv(path, index=False)
                            st.markdown(f"[Download CSV]({path})")

    # ---------- SAVED TAB ----------
    with tab_saved:
        df_s = _safe_read("SELECT id, name, auto_push, created_at, updated_at FROM sam_searches_t ORDER BY id DESC;",
                          "SELECT id, name, auto_push, created_at, updated_at FROM sam_searches ORDER BY id DESC;")
        if df_s.empty:
            st.info("No saved searches yet.")
        else:
            st.dataframe(df_s, use_container_width=True, hide_index=True)
            s_sel = st.selectbox("Pick a saved search", options=df_s["id"].tolist(
            ), format_func=lambda i: f"#{i} — {df_s.loc[df_s['id'] == i, 'name'].values[0]}", key="sam_saved_sel")

            c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
            with c1:
                if st.button("Run Saved Search", type="primary", key="sam_run_saved"):
                    row = pd.read_sql_query(
                        "SELECT params_json, auto_push FROM sam_searches WHERE id=?;", conn, params=(int(s_sel),)).iloc[0]
                    ui = json.loads(row["params_json"])
                    # Backward compat: allow older saved shape; map to our UI keys if needed
                    params = _sam_build_params({
                        "limit": ui.get("limit", 200),
                        "posted_from": ui.get("postedFrom") or ui.get("posted_from"),
                        "posted_to": ui.get("postedTo") or ui.get("posted_to"),
                        "active_only": (ui.get("status") == "active") or ui.get("active") in ("true", True),
                        "keywords": ui.get("title"),
                        "naics": ui.get("ncode") or ui.get("naics"),
                        "set_aside": ui.get("typeOfSetAside"),
                        "state": ui.get("state"),
                        "org_name": ui.get("organizationName"),
                        "types": ui.get("types") or ui.get("ptype"),
                    })
                    with st.spinner("Searching SAM.gov..."):
                        out = sam_search_cached(params)
                    if out.get("error"):
                        st.error(out["error"])
                    else:
                        recs = out.get("records", [])
                        st.success(
                            f"Found {out.get('totalRecords', 0)} (showing {len(recs)})")
                        df = pd.DataFrame(recs)
                        if not df.empty:
                            st.dataframe(df[[c for c in ["noticeId", "title", "solicitationNumber", "postedDate", "department",
                                         "type", "naics", "link"] if c in df.columns]], use_container_width=True, hide_index=True)
                            # Auto-push if enabled
                            if int(row["auto_push"]) == 1:
                                with closing(conn.cursor()) as cur:
                                    for _, r in df.iterrows():
                                        try:
                                            cur.execute("""
                                                INSERT INTO deals(title, solnum, agency, status, stage, created_at, source_url)
                                                VALUES(?, ?, ?, 'Open', 'New', datetime('now'), ?);
                                            """, (r.get("title") or "Untitled", r.get("solicitationNumber") or "", r.get("department") or "", r.get("link") or ""))
                                        except Exception:
                                            pass
                                    conn.commit()
                                st.info(
                                    "Auto-pushed to Deals (new rows may be de-duplicated by your workflow).")
                            # Log run
                            try:
                                seen = ",".join(df["noticeId"].astype(
                                    str).tolist()) if "noticeId" in df.columns else ""
                                with closing(conn.cursor()) as cur:
                                    cur.execute("INSERT INTO sam_runs(search_id, run_at, new_count, seen_ids) VALUES(?, datetime('now'), ?, ?);", (int(
                                        s_sel), int(len(df)), seen))
                                    conn.commit()
                            except Exception:
                                pass
            with c2:
                if st.button("Export Alerts CSV", key="sam_saved_alert_csv"):
                    df_r = _safe_read("SELECT run_at, new_count FROM sam_runs_t WHERE search_id=? ORDER BY id DESC LIMIT 1;",
                                      "SELECT run_at, new_count FROM sam_runs WHERE search_id=? ORDER BY id DESC LIMIT 1;",
                                      params=(int(s_sel),))
                    last = df_r.iloc[0].to_dict() if not df_r.empty else {
                        "run_at": "—", "new_count": 0}
                    df_srow = df_s[df_s["id"] == s_sel].iloc[0]
                    csvp = Path(
                        DATA_DIR) / f"sam_alert_{int(s_sel)}_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
                    pd.DataFrame([{
                        "Search": df_srow["name"],
                        "Last Run (UTC)": last["run_at"],
                        "Last Count": last["new_count"]
                    }]).to_csv(csvp, index=False)
                    st.markdown(f"[Download CSV]({csvp})")
            with c3:
                new_name = st.text_input("Rename to", key="sam_saved_rename")
                if st.button("Rename", key="sam_saved_rename_btn"):
                    with closing(conn.cursor()) as cur:
                        cur.execute("UPDATE sam_searches SET name=?, updated_at=datetime('now') WHERE id=?;", (
                            new_name.strip() or "Saved Search", int(s_sel)))
                        conn.commit()
                    st.success("Renamed")
                    st.rerun()
            with c4:
                if st.button("Delete Saved", key="sam_saved_delete"):
                    with closing(conn.cursor()) as cur:
                        cur.execute(
                            "DELETE FROM sam_searches WHERE id=?;", (int(s_sel),))
                        conn.commit()
                    st.success("Deleted")
                    st.rerun()

    # ---------- WATCHLIST TAB ----------
    with tab_watch:
        df_w = _safe_read("SELECT id, notice_id, title, solnum, agency, posted, link, added_at FROM sam_watch_t ORDER BY added_at DESC;",
                          "SELECT id, notice_id, title, solnum, agency, posted, link, added_at FROM sam_watch ORDER BY added_at DESC;")
        st.dataframe(df_w, use_container_width=True, hide_index=True)

        w_sel = st.multiselect("Select watchlist items", options=df_w["id"].astype(int).tolist() if not df_w.empty else [], key="sam_watch_sel",
                               format_func=lambda i: f"#{i} — {df_w.loc[df_w['id'] == i, 'title'].values[0][:60]}")
        h1, h2, h3 = st.columns([2, 2, 2])
        with h1:
            if st.button("Push selected to Deals", key="sam_watch_push"):
                pushed = 0
                with closing(conn.cursor()) as cur:
                    for wid in w_sel:
                        try:
                            row = df_w[df_w["id"] == wid].iloc[0].to_dict()
                            cur.execute("""
                                INSERT INTO deals(title, solnum, agency, status, stage, created_at, source_url)
                                VALUES(?, ?, ?, 'Open', 'New', datetime('now'), ?);
                            """, (row.get("title") or "Untitled", row.get("solnum") or "", row.get("agency") or "", row.get("link") or ""))
                            pushed += 1
                        except Exception:
                            pass
                    conn.commit()
                st.success(f"Pushed {pushed} deal(s)")
        with h2:
            if st.button("Remove from Watchlist", key="sam_watch_remove"):
                with closing(conn.cursor()) as cur:
                    for wid in w_sel:
                        try:
                            cur.execute(
                                "DELETE FROM sam_watch WHERE id=?;", (int(wid),))
                        except Exception:
                            pass
                    conn.commit()
                st.success("Removed")
                st.rerun()
        with h3:
            if st.button("Export Watchlist CSV", key="sam_watch_csv"):
                path = str(Path(
                    DATA_DIR) / f"sam_watch_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}.csv")
                (df_w if not df_w.empty else pd.DataFrame()).to_csv(
                    path, index=False)
                st.markdown(f"[Download CSV]({path})")


def run_rfp_analyzer(conn: sqlite3.Connection) -> None:
    st.header("RFP Analyzer")
    ctx = st.session_state.get("rfp_selected_notice")
    if ctx:
        st.info(
            f"Loaded from SAM Watch: **{ctx.get('Title', '')}** | Solicitation: {ctx.get('Solicitation', '')} | Notice ID: {ctx.get('Notice ID', '')}"
        )
        if ctx.get("SAM Link"):
            st.markdown(f"[Open in SAM]({ctx['SAM Link']})")

    colA, colB = st.columns([3, 2])
    with colA:
        up = st.file_uploader("Upload RFP (PDF/DOCX/TXT)",
                              type=["pdf", "docx", "txt"])
        with st.expander("Manual Text Paste (optional)", expanded=False):
            pasted = st.text_area(
                "Paste any text to include in parsing", height=150)
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
        l_items = derive_lm_items(secs.get('L', '')) + \
            derive_lm_items(secs.get('M', ''))
        clins = extract_clins(text)
        dates = extract_dates(text)
        pocs = extract_pocs(text)

        with st.expander("Section L (preview)", expanded=bool(secs.get('L'))):
            st.write(secs.get('L', '')[:4000] or "Not found")
        with st.expander("Section M (preview)", expanded=bool(secs.get('M'))):
            st.write(secs.get('M', '')[:4000] or "Not found")

        st.subheader("Checklist Items (L & M)")
        df_lm = pd.DataFrame({"item_text": l_items}) if l_items else pd.DataFrame(
            columns=["item_text"])
        st.dataframe(df_lm.rename(
            columns={"item_text": "Item"}), use_container_width=True, hide_index=True)

        st.subheader("CLIN Lines (heuristic)")
        df_clin = pd.DataFrame(clins)
        st.dataframe(df_clin if not df_clin.empty else pd.DataFrame(columns=[
                     'clin', 'description', 'qty', 'unit', 'unit_price', 'extended_price']), use_container_width=True, hide_index=True)

        st.subheader("Key Dates (heuristic)")
        df_dates = pd.DataFrame(dates)
        st.dataframe(df_dates if not df_dates.empty else pd.DataFrame(columns=[
                     'label', 'date_text', 'date_iso']), use_container_width=True, hide_index=True)

        st.subheader("POCs (heuristic)")
        df_pocs = pd.DataFrame(pocs)
        st.dataframe(df_pocs if not df_pocs.empty else pd.DataFrame(columns=[
                     'name', 'role', 'email', 'phone']), use_container_width=True, hide_index=True)

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
                    for sec_name in ['L', 'M']:
                        content = secs.get(sec_name)
                        if content:
                            cur.execute(
                                "INSERT INTO rfp_sections(rfp_id, section, content) VALUES (?, ?, ?);",
                                (rfp_id, sec_name, content),
                            )
                    for it in l_items:
                        cur.execute(
                            "INSERT INTO lm_items(rfp_id, item_text, is_must, status) VALUES (?, ?, ?, ?);",
                            (rfp_id, it, (1 if re.search(
                                r"\b(shall|must|required|mandatory|no later than|shall not|will)\b", it, re.IGNORECASE) else 0), 'Open'),
                        )
                    for r in clins:
                        cur.execute(
                            "INSERT INTO clin_lines(rfp_id, clin, description, qty, unit, unit_price, extended_price) VALUES (?, ?, ?, ?, ?, ?, ?);",
                            (rfp_id, r.get('clin'), r.get('description'), r.get('qty'), r.get(
                                'unit'), r.get('unit_price'), r.get('extended_price')),
                        )
                    for d in dates:
                        cur.execute(
                            "INSERT INTO key_dates(rfp_id, label, date_text, date_iso) VALUES (?, ?, ?, ?);",
                            (rfp_id, d.get('label'), d.get(
                                'date_text'), d.get('date_iso')),
                        )
                    for p in pocs:
                        cur.execute(
                            "INSERT INTO pocs(rfp_id, name, role, email, phone) VALUES (?, ?, ?, ?, ?);",
                            (rfp_id, p.get('name'), p.get('role'),
                             p.get('email'), p.get('phone')),
                        )
                    conn.commit()
                st.success("Extraction saved.")
                st.session_state['current_rfp_id'] = rfp_id
            except Exception as e:
                st.error(f"Failed to save extraction: {e}")


# ---------- L & M Checklist ----------

# ---- Compliance (Phase K) helpers ----
def _compliance_progress(df_items: pd.DataFrame) -> int:
    if df_items is None or df_items.empty:
        return 0
    done = int((df_items["status"] == "Complete").sum())
    total = int(len(df_items))
    return int(round(done / max(1, total) * 100))


def _load_compliance_matrix(conn: sqlite3.Connection, rfp_id: int) -> pd.DataFrame:
    q = """
        SELECT i.id AS lm_id, i.item_text, i.is_must, i.status,
               COALESCE(m.owner,'') AS owner,
               COALESCE(m.ref_page,'') AS ref_page,
               COALESCE(m.ref_para,'') AS ref_para,
               COALESCE(m.evidence,'') AS evidence,
               COALESCE(m.risk,'Green') AS risk,
               COALESCE(m.notes,'') AS notes
        FROM lm_items_t i
        LEFT JOIN lm_meta_t m ON m.lm_id = i.id
        WHERE i.rfp_id = ?
        ORDER BY i.id ASC;
    """
    return pd.read_sql_query(q, conn, params=(rfp_id,))


def _compliance_flags(ctx: dict, df_items: pd.DataFrame) -> pd.DataFrame:
    rows = []
    sections = ctx.get("sections", pd.DataFrame())
    text_all = " ".join((sections["content"].tolist() if isinstance(
        sections, pd.DataFrame) and not sections.empty else []))
    tl = text_all.lower()

    m = re.search(
        r'(?:page\s+limit|not\s+exceed)\s+(?:of\s+)?(\d{1,3})\s+pages?', tl)
    if m:
        rows.append({"Rule": "Page Limit",
                    "Detail": f"Limit {m.group(1)} pages detected", "Severity": "Amber"})
    if re.search(r'(font|typeface).{0,20}(size|pt).{0,5}(10|11)', tl):
        rows.append(
            {"Rule": "Font size", "Detail": "Minimum font size 10/11pt likely required", "Severity": "Amber"})
    if re.search(r'margin[s]?\s+(?:of|at\s+least)\s+\d', tl):
        rows.append(
            {"Rule": "Margins", "Detail": "Specific margin requirements detected", "Severity": "Amber"})
    if re.search(r'volume[s]?\s+(i{1,3}|iv|v|technical|price)', tl):
        rows.append(
            {"Rule": "Volumes", "Detail": "Multiple volumes required", "Severity": "Amber"})
    if re.search(r'(sam\.gov|piee|wawf|email submission|portal)', tl):
        rows.append({"Rule": "Submission portal",
                    "Detail": "Specific portal/email submission detected", "Severity": "Amber"})

    dates = ctx.get("dates", pd.DataFrame())
    if isinstance(dates, pd.DataFrame) and not dates.empty:
        due = dates[dates["label"].str.contains("due", case=False, na=False)]
        if not due.empty:
            dt = pd.to_datetime(due.iloc[0]["date_text"], errors="coerce")
            if pd.notnull(dt):
                days = (pd.Timestamp(dt) - pd.Timestamp.utcnow()).days
                if days <= 3:
                    rows.append(
                        {"Rule": "Timeline", "Detail": f"Proposals due in {days} day(s)", "Severity": "Red"})
                elif days <= 7:
                    rows.append(
                        {"Rule": "Timeline", "Detail": f"Proposals due in {days} days", "Severity": "Amber"})

    if isinstance(df_items, pd.DataFrame) and not df_items.empty:
        open_musts = df_items[(df_items["is_must"] == 1)
                              & (df_items["status"] != "Complete")]
        if not open_musts.empty:
            rows.append({"Rule": "Open MUST items",
                        "Detail": f"{len(open_musts)} mandatory items still open", "Severity": "Red"})

    return pd.DataFrame(rows)


def run_lm_checklist(conn: sqlite3.Connection) -> None:

    st.header("L and M Checklist")
    rfp_id = st.session_state.get('current_rfp_id')
    if not rfp_id:
        try:
            df_rf = pd.read_sql_query(
                "SELECT id, title, solnum, created_at FROM rfps_t ORDER BY id DESC;", conn)
        except Exception as e:
            st.error(f"Failed to load RFPs: {e}")
            return
        if df_rf.empty:
            st.info(
                "No saved RFP extractions yet. Use RFP Analyzer to parse and save.")
            return
        opt = st.selectbox("Select an RFP context", options=df_rf['id'].tolist(),
                           format_func=lambda rid: f"#{rid} — {df_rf.loc[df_rf['id'] == rid, 'title'].values[0] or 'Untitled'}")
        rfp_id = opt
        st.session_state['current_rfp_id'] = rfp_id

    st.caption(f"Working RFP ID: {rfp_id}")
    try:
        df_items = pd.read_sql_query(
            "SELECT id, item_text, is_must, status FROM lm_items WHERE rfp_id=?;", conn, params=(rfp_id,))
    except Exception as e:
        st.error(f"Failed to load items: {e}")
        return
    if df_items.empty:
        st.info("No L/M items found for this RFP.")
        return

    pct = _compliance_progress(df_items)
    st.progress(pct/100.0, text=f"{pct}% complete")

    c1, c2, c3 = st.columns([2, 2, 2])
    with c1:
        if st.button("Mark all Complete"):
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute(
                        "UPDATE lm_items SET status='Complete' WHERE rfp_id=?;", (rfp_id,))
                    conn.commit()
                st.success("All items marked Complete")
            except Exception as e:
                st.error(f"Update failed: {e}")
    with c2:
        if st.button("Reset all to Open"):
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute(
                        "UPDATE lm_items SET status='Open' WHERE rfp_id=?;", (rfp_id,))
                    conn.commit()
                st.success("All items reset")
            except Exception as e:
                st.error(f"Update failed: {e}")
    with c3:
        if st.button("Mark all MUST to Open"):
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute(
                        "UPDATE lm_items SET status='Open' WHERE rfp_id=? AND is_must=1;", (rfp_id,))
                    conn.commit()
                st.success("All MUST items set to Open")
            except Exception as e:
                st.error(f"Update failed: {e}")

    st.subheader("Checklist")
    for _, row in df_items.iterrows():
        key = f"lm_{row['id']}"
        label = ("[MUST] " if row['is_must'] == 1 else "") + row['item_text']
        checked = st.checkbox(label, value=(
            row['status'] == 'Complete'), key=key)
        new_status = 'Complete' if checked else 'Open'
        if new_status != row['status']:
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute(
                        "UPDATE lm_items SET status=? WHERE id=?;", (new_status, int(row['id'])))
                    conn.commit()
            except Exception as e:
                st.error(f"Failed to update item {row['id']}: {e}")

    st.divider()

    st.subheader("Compliance Matrix")
    df_mx = _load_compliance_matrix(conn, int(rfp_id))
    if df_mx.empty:
        st.info("No items to show.")
        return

    view = df_mx.rename(columns={
        "item_text": "Requirement", "is_must": "Must?", "status": "Status",
        "owner": "Owner", "ref_page": "Page", "ref_para": "Para",
        "evidence": "Evidence/Link", "risk": "Risk", "notes": "Notes"
    })
    st.dataframe(view[["Requirement", "Must?", "Status", "Owner", "Page", "Para", "Evidence/Link", "Risk", "Notes"]],
                 use_container_width=True, hide_index=True)

    st.markdown("**Edit selected requirement**")
    pick = st.selectbox("Requirement", options=df_mx["lm_id"].tolist(),
                        format_func=lambda lid: f"#{lid} — {df_mx.loc[df_mx['lm_id'] == lid, 'item_text'].values[0][:80]}")

    rec = df_mx[df_mx["lm_id"] == pick].iloc[0].to_dict()
    e1, e2, e3, e4 = st.columns([2, 1, 1, 1])
    with e1:
        owner = st.text_input("Owner", value=rec.get(
            "owner", ""), key=f"mx_owner_{pick}")
        notes = st.text_area("Notes", value=rec.get(
            "notes", ""), key=f"mx_notes_{pick}", height=90)
    with e2:
        page = st.text_input("Page", value=rec.get(
            "ref_page", ""), key=f"mx_page_{pick}")
        para = st.text_input("Paragraph", value=rec.get(
            "ref_para", ""), key=f"mx_para_{pick}")
    with e3:
        risk = st.selectbox("Risk", ["Green", "Yellow", "Red"],
                            index=["Green", "Yellow", "Red"].index(rec.get("risk", "Green")), key=f"mx_risk_{pick}")
    with e4:
        evidence = st.text_input(
            "Evidence/Link", value=rec.get("evidence", ""), key=f"mx_evid_{pick}")

    csave, cexp = st.columns([2, 2])
    with csave:
        if st.button("Save Matrix Row", key=f"mx_save_{pick}"):
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute("""
                        INSERT INTO lm_meta(lm_id, owner, ref_page, ref_para, evidence, risk, notes)
                        VALUES(?,?,?,?,?,?,?)
                        ON CONFLICT(lm_id) DO UPDATE SET
                            owner=excluded.owner, ref_page=excluded.ref_page, ref_para=excluded.ref_para,
                            evidence=excluded.evidence, risk=excluded.risk, notes=excluded.notes;
                    """, (int(pick), owner.strip(), page.strip(), para.strip(), evidence.strip(), risk, notes.strip()))
                    conn.commit()
                st.success("Saved")
                st.rerun()
            except Exception as e2:
                st.error(f"Save failed: {e2}")
    with cexp:
        if st.button("Export Matrix CSV", key="mx_export"):
            out = view.copy()
            path = str(Path(DATA_DIR) /
                       f"compliance_matrix_rfp_{int(rfp_id)}.csv")
            out.to_csv(path, index=False)
            st.success("Exported")
            st.markdown(f"[Download CSV]({path})")

    st.subheader("Red-Flag Finder")
    ctx = _load_rfp_context(conn, int(rfp_id))
    flags = _compliance_flags(ctx, df_items)
    if flags is None or flags.empty:
        st.write("No obvious flags detected.")
    else:
        st.dataframe(flags, use_container_width=True, hide_index=True)


def run_proposal_builder(conn: sqlite3.Connection) -> None:
    st.header("Proposal Builder")
    df_rf = pd.read_sql_query(
        "SELECT id, title, solnum, notice_id FROM rfps_t ORDER BY id DESC;", conn)
    if df_rf.empty:
        st.info("No RFP context found. Use RFP Analyzer first to parse and save.")
        return
    rfp_id = st.selectbox(
        "RFP context",
        options=df_rf["id"].tolist(),
        format_func=lambda rid: f"#{rid} — {df_rf.loc[df_rf['id'] == rid, 'title'].values[0] or 'Untitled'}",
        index=0,
    )
    st.session_state["current_rfp_id"] = rfp_id
    ctx = _load_rfp_context(conn, rfp_id)

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Sections")
        default_sections = [
            "Cover Letter", "Executive Summary", "Understanding of Requirements", "Technical Approach", "Management Plan",
            "Staffing and Key Personnel", "Quality Assurance", "Past Performance Summary", "Pricing and CLINs", "Certifications and Reps", "Appendices",
        ]
        selected = st.multiselect(
            "Include sections", default_sections, default=default_sections)
        content_map: Dict[str, str] = {}
        for sec in selected:
            default_val = st.session_state.get(f"pb_section_{sec}", "")
            content_map[sec] = st.text_area(sec, value=default_val, height=140)

    with right:
        st.subheader("Guidance and limits")
        spacing = st.selectbox(
            "Line spacing", ["Single", "1.15", "Double"], index=1)
        font_name = st.selectbox(
            "Font", ["Times New Roman", "Calibri"], index=0)
        font_size = st.number_input(
            "Font size", min_value=10, max_value=12, value=11)
        page_limit = st.number_input(
            "Page limit for narrative", min_value=1, max_value=200, value=10)

        st.markdown("**Must address items from L and M**")
        items = ctx["items"] if isinstance(
            ctx.get("items"), pd.DataFrame) else pd.DataFrame()
        if not items.empty:
            st.dataframe(items.rename(columns={
                         "item_text": "Item", "status": "Status"}), use_container_width=True, hide_index=True, height=240)
        else:
            st.caption("No checklist items found for this RFP")

        total_words = sum(len((content_map.get(k) or "").split())
                          for k in selected)
        est_pages = _estimate_pages(total_words, spacing)
        st.info(
            f"Current word count {total_words}  Estimated pages {est_pages}")
        if est_pages > page_limit:
            st.error(
                "Content likely exceeds page limit. Consider trimming or tighter formatting")

        out_name = f"Proposal_RFP_{int(rfp_id)}.docx"
        out_path = os.path.join(DATA_DIR, out_name)
        if st.button("Export DOCX", type="primary"):
            sections = [
                {"title": k, "body": content_map.get(k, "")} for k in selected]
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
    st.caption(
        "Seed and manage vendors by NAICS/PSC/state; handoff selected vendors to Outreach.")

    ctx = st.session_state.get("rfp_selected_notice", {})
    default_naics = ctx.get("NAICS") or ""
    default_state = ""

    with st.expander("Filters", expanded=True):
        c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
        with c1:
            f_naics = st.text_input(
                "NAICS", value=default_naics, key="filter_naics")
        with c2:
            f_state = st.text_input(
                "State (e.g., TX)", value=default_state, key="filter_state")
        with c3:
            f_city = st.text_input("City contains", key="filter_city")
        with c4:
            f_kw = st.text_input("Keyword in name/notes", key="filter_kw")
        st.caption(
            "Use CSV import or add vendors manually. Internet seeding can be added later.")

    with st.expander("Import Vendors (CSV)", expanded=False):
        st.caption(
            "Headers: name, email, phone, city, state, naics, cage, uei, website, notes")
        up = st.file_uploader("Upload vendor CSV", type=[
                              "csv"], key="vendor_csv")
        if up and st.button("Import CSV"):
            try:
                df = pd.read_csv(up)
                if "name" not in {c.lower() for c in df.columns}:
                    st.error("CSV must include a 'name' column")
                else:
                    df.columns = [c.lower() for c in df.columns]
                    n = 0
                    with closing(conn.cursor()) as cur:
                        for _, r in df.iterrows():
                            cur.execute(
                                """
                                INSERT INTO vendors(name, cage, uei, naics, city, state, phone, email, website, notes)
                                VALUES(?,?,?,?,?,?,?,?,?,?)
                                ;
                                """,
                                (
                                    str(r.get("name", ""))[:200],
                                    str(r.get("cage", ""))[:20],
                                    str(r.get("uei", ""))[:40],
                                    str(r.get("naics", ""))[:20],
                                    str(r.get("city", ""))[:100],
                                    str(r.get("state", ""))[:10],
                                    str(r.get("phone", ""))[:40],
                                    str(r.get("email", ""))[:120],
                                    str(r.get("website", ""))[:200],
                                    str(r.get("notes", ""))[:500],
                                ),
                            )
                            n += 1
                    conn.commit()
                    st.success(f"Imported {n} vendors")
            except Exception as e:
                st.error(f"Import failed: {e}")

    with st.expander("Add Vendor", expanded=False):
        c1, c2, c3 = st.columns([2, 2, 2])
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
                            (v_name.strip(), v_cage.strip(), v_uei.strip(), v_naics.strip(), v_city.strip(
                            ), v_state.strip(), v_phone.strip(), v_email.strip(), v_site.strip(), v_notes.strip()),
                        )
                        conn.commit()
                    st.success("Vendor saved")
                except Exception as e:
                    st.error(f"Save failed: {e}")

    q = "SELECT id, name, email, phone, city, state, naics, cage, uei, website, notes FROM vendors_t WHERE 1=1"
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
        df_v = pd.read_sql_query(
            q + " ORDER BY name ASC;", conn, params=params)
    except Exception as e:
        st.error(f"Query failed: {e}")
        df_v = pd.DataFrame()

    st.subheader("Vendors")
    if df_v.empty:
        st.write("No vendors match filters")
    else:
        selected_ids = []
        for _, row in df_v.iterrows():
            chk = st.checkbox(
                f"Select — {row['name']}  ({row['email'] or 'no email'})", key=f"vend_{int(row['id'])}")
            if chk:
                selected_ids.append(int(row['id']))
        c1, c2 = st.columns([2, 2])
        with c1:
            if st.button("Send to Outreach ▶") and selected_ids:
                st.session_state['rfq_vendor_ids'] = selected_ids
                st.success(f"Queued {len(selected_ids)} vendors for Outreach")
        with c2:
            st.caption(
                "Selections are stored in session and available in Outreach tab")


# ---------- Outreach (Phase D) ----------
def _smtp_settings() -> Dict[str, Any]:
    out = {"host": None, "port": 587, "username": None, "password": None,
           "from_email": None, "from_name": "ELA Management", "use_tls": True}
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
                part.add_header(
                    'Content-Disposition', f'attachment; filename="{os.path.basename(path)}"')
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
    st.caption(
        "Mail-merge RFQs to selected vendors. Uses SMTP settings from secrets.")

    notice = st.session_state.get("rfp_selected_notice", {})
    vendor_ids: List[int] = st.session_state.get("rfq_vendor_ids", [])

    if vendor_ids:
        ph = ",".join(["?"] * len(vendor_ids))
        df_sel = pd.read_sql_query(
            f"SELECT id, name, email, phone, city, state, naics FROM vendors_t WHERE id IN ({ph});",
            conn,
            params=vendor_ids,
        )
    else:
        st.info(
            "No vendors queued. Use Subcontractor Finder to select vendors, or pick by filter below.")
        f_naics = st.text_input("NAICS filter")
        f_state = st.text_input("State filter")
        q = "SELECT id, name, email, phone, city, state, naics FROM vendors_t WHERE 1=1"
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
    st.markdown(
        "Use tags: {{company}}, {{email}}, {{phone}}, {{city}}, {{state}}, {{naics}}, {{title}}, {{solicitation}}, {{due}}, {{notice_id}}")
    subj = st.text_input(
        "Subject", value="RFQ: {{title}} (Solicitation {{solicitation}})")
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
        files = st.file_uploader("Attach files (optional)", type=[
                                 "pdf", "docx", "xlsx", "zip"], accept_multiple_files=True)
        attach_paths: List[str] = []
        if files:
            for f in files:
                pth = save_uploaded_file(f, subdir="outreach")
                if pth:
                    attach_paths.append(pth)
            if attach_paths:
                st.success(f"Saved {len(attach_paths)} attachment(s)")

    c1, c2, c3 = st.columns([2, 2, 2])
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
                log_rows.append({"vendor": vendor.get("name"),
                                "email": "", "status": "skipped: no email"})
                continue
            s = _merge_text(subj, vendor, notice)
            b = _merge_text(body, vendor, notice)
            success, msg = send_email_smtp(to_email, s, b, attach_paths)
            ok += 1 if success else 0
            fail += 0 if success else 1
            log_rows.append({"vendor": vendor.get(
                "name"), "email": to_email, "status": ("sent" if success else msg)})
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
    df = pd.read_sql_query(
        "SELECT id, title, solnum FROM rfps_t ORDER BY id DESC;", conn)
    if df.empty:
        st.info("No RFPs in DB. Use RFP Analyzer to create one (Parse → Save).")
        return
    rfp_id = st.selectbox("RFP context", options=df["id"].tolist(
    ), format_func=lambda rid: f"#{rid} — {df.loc[df['id'] == rid, 'title'].values[0] or 'Untitled'}")

    st.subheader("Upload / Add Quotes")
    with st.expander("CSV Import", expanded=False):
        st.caption(
            "Columns: vendor, clin, qty, unit_price, description (optional). One row = one CLIN line.")
        up = st.file_uploader("Quotes CSV", type=["csv"], key="quotes_csv")
        if up and st.button("Import Quotes CSV"):
            try:
                df_csv = pd.read_csv(up)
                required = {"vendor", "clin", "qty", "unit_price"}
                if not required.issubset({c.lower() for c in df_csv.columns}):
                    st.error(
                        "CSV missing required columns: vendor, clin, qty, unit_price")
                else:
                    df_csv.rename(columns={c: c.lower()
                                  for c in df_csv.columns}, inplace=True)
                    with closing(conn.cursor()) as cur:
                        by_vendor = df_csv.groupby("vendor", dropna=False)
                        total_rows = 0
                        for vendor, block in by_vendor:
                            cur.execute(
                                "INSERT INTO quotes(rfp_id, vendor, received_date, notes) VALUES(?,?,?,?);",
                                (int(rfp_id), str(vendor)[
                                 :200], datetime.utcnow().isoformat(), "imported")
                            )
                            qid = cur.lastrowid
                            for _, r in block.iterrows():
                                qty = float(r.get("qty", 0) or 0)
                                upx = float(r.get("unit_price", 0) or 0)
                                ext = _calc_extended(qty, upx) or 0.0
                                cur.execute(
                                    "INSERT INTO quote_lines(quote_id, clin, description, qty, unit_price, extended_price) VALUES(?,?,?,?,?,?);",
                                    (qid, str(r.get("clin", ""))[:50], str(
                                        r.get("description", ""))[:300], qty, upx, ext)
                                )
                                total_rows += 1
                        conn.commit()
                    st.success(
                        f"Imported {len(by_vendor)} quotes / {total_rows} lines.")
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
                st.success(
                    f"Created quote for {vendor}. Now add lines below (Quote ID {qid}).")
                st.session_state["current_quote_id"] = qid

    df_q = pd.read_sql_query(
        "SELECT id, vendor, received_date, notes FROM quotes WHERE rfp_id=? ORDER BY vendor;", conn, params=(rfp_id,))
    if not df_q.empty:
        st.subheader("Quotes")
        st.dataframe(df_q, use_container_width=True, hide_index=True)
        qid = st.selectbox("Edit lines for quote", options=df_q["id"].tolist(
        ), format_func=lambda qid: f"#{qid} — {df_q.loc[df_q['id'] == qid, 'vendor'].values[0]}")
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
                    (qid, clin.strip(), desc.strip(),
                     float(qty), float(price), float(ext))
                )
                conn.commit()
            st.success("Line added.")

    st.subheader("Comparison")
    df_target = pd.read_sql_query(
        "SELECT clin, description FROM clin_lines WHERE rfp_id=? GROUP BY clin, description ORDER BY clin;", conn, params=(rfp_id,))
    df_lines = pd.read_sql_query("""
        SELECT q.vendor, l.clin, l.qty, l.unit_price, l.extended_price
        FROM quote_lines l
        JOIN quotes q ON q.id = l.quote_id
        WHERE q.rfp_id=?
    """, conn, params=(rfp_id,))
    if df_lines.empty:
        st.info("No quote lines yet.")
        return

    mat = df_lines.pivot_table(
        index="clin", columns="vendor", values="extended_price", aggfunc="sum").fillna(0.0)
    mat = mat.sort_index()
    st.dataframe(mat.style.format("{:,.2f}"), use_container_width=True)

    best_vendor_by_clin = mat.replace(0, float("inf")).idxmin(
        axis=1).to_frame("Best Vendor")
    st.caption("Best vendor per CLIN")
    st.dataframe(best_vendor_by_clin,
                 use_container_width=True, hide_index=False)

    totals = df_lines.groupby("vendor")["extended_price"].sum().to_frame(
        "Total").sort_values("Total")
    if not df_target.empty:
        coverage = df_lines.groupby(
            "vendor")["clin"].nunique().to_frame("CLINs Quoted")
        coverage["Required CLINs"] = df_target["clin"].nunique()
        coverage["Coverage %"] = (
            coverage["CLINs Quoted"] / coverage["Required CLINs"] * 100).round(1)
        totals = totals.join(coverage, how="left")
    st.subheader("Totals & Coverage")
    st.dataframe(totals.style.format(
        {"Total": "{:,.2f}", "Coverage %": "{:.1f}"}), use_container_width=True)

    if st.button("Export comparison CSV"):
        path = os.path.join(DATA_DIR, "quote_comparison.csv")
        out = mat.copy()
        out["Best Vendor"] = best_vendor_by_clin["Best Vendor"]
        out.to_csv(path)
        st.success("Exported.")
        st.markdown(f"[Download comparison CSV]({path})")


# ---------- Pricing Calculator (Phase E) ----------
def _scenario_summary(conn: sqlite3.Connection, scenario_id: int) -> Dict[str, float]:
    dl = pd.read_sql_query(
        "SELECT hours, rate, fringe_pct FROM pricing_labor WHERE scenario_id=?;", conn, params=(scenario_id,))
    other = pd.read_sql_query(
        "SELECT cost FROM pricing_other WHERE scenario_id=?;", conn, params=(scenario_id,))
    base = pd.read_sql_query(
        "SELECT overhead_pct, gna_pct, fee_pct, contingency_pct FROM pricing_scenarios WHERE id=?;", conn, params=(scenario_id,))
    if base.empty:
        return {}
    overhead_pct, gna_pct, fee_pct, contingency_pct = base.iloc[0]
    direct_labor = float((dl["hours"] * dl["rate"]).sum()
                         ) if not dl.empty else 0.0
    fringe = float((dl["hours"] * dl["rate"] *
                   (dl["fringe_pct"].fillna(0.0) / 100)).sum()) if not dl.empty else 0.0
    other_dir = float(other["cost"].sum()) if not other.empty else 0.0
    overhead = (direct_labor + fringe) * (float(overhead_pct) / 100.0)
    gna = (direct_labor + fringe + overhead +
           other_dir) * (float(gna_pct) / 100.0)
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
    df = pd.read_sql_query(
        "SELECT id, title FROM rfps_t ORDER BY id DESC;", conn)
    if df.empty:
        st.info("No RFP context. Use RFP Analyzer (parse & save) first.")
        return
    rfp_id = st.selectbox("RFP context", options=df["id"].tolist(
    ), format_func=lambda rid: f"#{rid} — {df.loc[df['id'] == rid, 'title'].values[0]}")

    st.subheader("Scenario")
    df_sc = pd.read_sql_query(
        "SELECT id, name FROM pricing_scenarios WHERE rfp_id=? ORDER BY id DESC;", conn, params=(rfp_id,))
    mode = st.radio("Mode", ["Create new", "Edit existing"], horizontal=True)
    if mode == "Create new":
        name = st.text_input("Scenario name", value="Base")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            overhead = st.number_input(
                "Overhead %", min_value=0.0, value=20.0, step=1.0)
        with c2:
            gna = st.number_input("G&A %", min_value=0.0, value=10.0, step=1.0)
        with c3:
            fee = st.number_input(
                "Fee/Profit %", min_value=0.0, value=7.0, step=0.5)
        with c4:
            contingency = st.number_input(
                "Contingency %", min_value=0.0, value=0.0, step=0.5)
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
        scenario_id = st.selectbox("Pick a scenario", options=df_sc["id"].tolist(
        ), format_func=lambda sid: df_sc.loc[df_sc["id"] == sid, "name"].values[0])

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
            fringe = st.number_input(
                "Fringe %", min_value=0.0, value=0.0, step=0.5)
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
            cur.execute("INSERT INTO pricing_other(scenario_id, label, cost) VALUES(?, ?, ?);", (int(
                scenario_id), label.strip(), float(cost)))
            conn.commit()
        st.success("Added ODC.")

    df_odc = pd.read_sql_query(
        "SELECT id, label, cost FROM pricing_other WHERE scenario_id=?;", conn, params=(scenario_id,))
    st.dataframe(df_odc, use_container_width=True, hide_index=True)

    st.subheader("Summary")
    s = _scenario_summary(conn, int(scenario_id))
    if not s:
        st.info("Add labor/ODCs to see a summary.")
        return
    df_sum = pd.DataFrame(list(s.items()), columns=["Component", "Amount"])
    st.dataframe(df_sum.style.format(
        {"Amount": "{:,.2f}"}), use_container_width=True, hide_index=True)

    if st.button("Export pricing CSV"):
        path = os.path.join(
            DATA_DIR, f"pricing_scenario_{int(scenario_id)}.csv")
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
    df = pd.read_sql_query(
        "SELECT id, title FROM rfps_t ORDER BY id DESC;", conn)
    if df.empty:
        st.info("No RFP context. Use RFP Analyzer first.")
        return
    rfp_id = st.selectbox("RFP context", options=df["id"].tolist(
    ), format_func=lambda rid: f"#{rid} — {df.loc[df['id'] == rid, 'title'].values[0]}")

    df_items = pd.read_sql_query(
        "SELECT status FROM lm_items WHERE rfp_id=?;", conn, params=(rfp_id,))
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

    df_sc = pd.read_sql_query(
        "SELECT id, name FROM pricing_scenarios WHERE rfp_id=? ORDER BY id DESC;", conn, params=(rfp_id,))
    price_score = None
    our_total = None
    if not df_sc.empty:
        sid = st.selectbox("Use pricing scenario (optional)", options=[None] + df_sc["id"].tolist(),
                           format_func=lambda x: "None" if x is None else df_sc.loc[df_sc["id"] == x, "name"].values[0])
        if sid:
            our_total = _scenario_summary(conn, int(sid)).get("Total")
    if our_total is None:
        our_total = st.number_input(
            "Our total price (if no scenario)", min_value=0.0, value=0.0, step=1000.0)
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
    df_scores = pd.DataFrame(list(comp.items()), columns=[
                             "Factor", "Score (0-100)"])
    st.dataframe(df_scores, use_container_width=True, hide_index=True)

    weighted = (
        compliance * w_comp + tech * w_tech + past_perf * w_past + team *
        w_team + int(round(price_score)) * w_price + smallbiz * w_small
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
        dfL = pd.read_sql_query(
            "SELECT section, content FROM rfp_sections WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfL = pd.read_sql_query(
            "SELECT section, content FROM rfp_sections;", conn)
    if not dfL.empty:
        dfL["score"] = dfL["content"].str.lower().apply(
            lambda t: sum(1 for w in q.split() if w in (t or "")))
        res["sections"] = dfL.sort_values("score", ascending=False).head(5)

    # Checklist
    if rfp_id:
        dfCk = pd.read_sql_query(
            "SELECT item_text, status FROM lm_items WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfCk = pd.read_sql_query(
            "SELECT item_text, status FROM lm_items;", conn)
    if not dfCk.empty:
        dfCk["score"] = dfCk["item_text"].str.lower().apply(
            lambda t: sum(1 for w in q.split() if w in (t or "")))
        res["checklist"] = dfCk.sort_values("score", ascending=False).head(10)

    # CLINs
    if rfp_id:
        dfCL = pd.read_sql_query(
            "SELECT clin, description, qty, unit FROM clin_lines WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfCL = pd.read_sql_query(
            "SELECT clin, description, qty, unit FROM clin_lines;", conn)
    if not dfCL.empty:
        dfCL["score"] = (dfCL["clin"].astype(str) + " " + dfCL["description"].astype(
            str)).str.lower().apply(lambda t: sum(1 for w in q.split() if w in (t or "")))
        res["clins"] = dfCL.sort_values("score", ascending=False).head(10)

    # Dates
    if rfp_id:
        dfDt = pd.read_sql_query(
            "SELECT label, date_text FROM key_dates WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfDt = pd.read_sql_query(
            "SELECT label, date_text FROM key_dates;", conn)
    if not dfDt.empty:
        dfDt["score"] = (dfDt["label"].astype(str) + " " + dfDt["date_text"].astype(
            str)).str.lower().apply(lambda t: sum(1 for w in q.split() if w in (t or "")))
        res["dates"] = dfDt.sort_values("score", ascending=False).head(10)

    # POCs
    if rfp_id:
        dfP = pd.read_sql_query(
            "SELECT name, role, email, phone FROM pocs WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfP = pd.read_sql_query(
            "SELECT name, role, email, phone FROM pocs;", conn)
    if not dfP.empty:
        dfP["score"] = (dfP["name"].astype(str) + " " + dfP["role"].astype(str) + " " + dfP["email"].astype(
            str)).str.lower().apply(lambda t: sum(1 for w in q.split() if w in (t or "")))
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
        df_target = pd.read_sql_query(
            "SELECT DISTINCT clin FROM clin_lines WHERE rfp_id=?;", conn, params=(rfp_id,))
        total_clins = int(df_target["clin"].nunique()
                          ) if not df_target.empty else 0
        df_items = pd.read_sql_query(
            "SELECT status FROM lm_items WHERE rfp_id=?;", conn, params=(rfp_id,))
        compl = 0
        if not df_items.empty:
            compl = int(
                round(((df_items["status"] == "Complete").sum() / max(1, len(df_items))) * 100))
        res["meta"] = {"total_clins": total_clins, "compliance_pct": compl}

    return res


def run_chat_assistant(conn: sqlite3.Connection) -> None:
    st.header("Chat Assistant (DB-aware)")
    st.caption(
        "Answers from your saved RFPs, checklist, CLINs, dates, POCs, quotes, and pricing — no external API.")

    df_rf = pd.read_sql_query(
        "SELECT id, title FROM rfps_t ORDER BY id DESC;", conn)
    rfp_opt = None
    if not df_rf.empty:
        rfp_opt = st.selectbox("Context (optional)", options=[None] + df_rf["id"].tolist(),
                               format_func=lambda rid: "All RFPs" if rid is None else f"#{rid} — {df_rf.loc[df_rf['id'] == rid, 'title'].values[0]}")

    q = st.text_input(
        "Ask a question (e.g., 'When are proposals due?', 'Show POCs', 'Which vendor is lowest?')")
    ask = st.button("Ask", type="primary")
    if not ask:
        st.caption(
            "Quick picks: due date • POCs • open checklist • CLINs • quotes total • compliance")
        return

    res = _kb_search(conn, rfp_opt, q or "")
    # Heuristic intents
    ql = (q or "").lower()
    if any(w in ql for w in ["due", "deadline", "close"]):
        st.subheader("Key Dates")
        df = res.get("dates", pd.DataFrame())
        if df is not None and not df.empty:
            st.dataframe(df[["label", "date_text"]],
                         use_container_width=True, hide_index=True)
    if any(w in ql for w in ["poc", "contact", "officer", "specialist"]):
        st.subheader("Points of Contact")
        df = res.get("pocs", pd.DataFrame())
        if df is not None and not df.empty:
            st.dataframe(df[["name", "role", "email", "phone"]],
                         use_container_width=True, hide_index=True)
    if "clin" in ql:
        st.subheader("CLINs")
        df = res.get("clins", pd.DataFrame())
        if df is not None and not df.empty:
            st.dataframe(df[["clin", "description", "qty", "unit"]],
                         use_container_width=True, hide_index=True)
    if any(w in ql for w in ["checklist", "compliance"]):
        st.subheader("Checklist (top hits)")
        df = res.get("checklist", pd.DataFrame())
        if df is not None and not df.empty:
            st.dataframe(df[["item_text", "status"]],
                         use_container_width=True, hide_index=True)
        meta = res.get("meta", {})
        if meta:
            st.info(f"Compliance completion: {meta.get('compliance_pct', 0)}%")
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
        st.dataframe(sh[["section", "snippet", "score"]],
                     use_container_width=True, hide_index=True)


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
        s.top_margin = Inches(0.7)
        s.bottom_margin = Inches(0.7)
        s.left_margin = Inches(0.7)
        s.right_margin = Inches(0.7)

    title = profile.get("company_name") or "Capability Statement"
    doc.add_heading(title, level=1)
    if profile.get("tagline"):
        p = doc.add_paragraph(profile["tagline"])
        p.runs[0].italic = True

    meta = [
        ("Address", "address"), ("Phone", "phone"), ("Email",
                                                     "email"), ("Website", "website"),
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
    st.caption(
        "Store your company profile and export a polished 1-page DOCX capability statement.")

    # Load existing (id=1)
    df = pd.read_sql_query("SELECT * FROM org_profile WHERE id=1;", conn)
    vals = df.iloc[0].to_dict() if not df.empty else {}

    with st.form("org_profile_form"):
        c1, c2 = st.columns([2, 2])
        with c1:
            company_name = st.text_input(
                "Company Name", value=vals.get("company_name", ""))
            tagline = st.text_input(
                "Tagline (optional)", value=vals.get("tagline", ""))
            address = st.text_area(
                "Address", value=vals.get("address", ""), height=70)
            phone = st.text_input("Phone", value=vals.get("phone", ""))
            email = st.text_input("Email", value=vals.get("email", ""))
            website = st.text_input("Website", value=vals.get("website", ""))
        with c2:
            uei = st.text_input("UEI", value=vals.get("uei", ""))
            cage = st.text_input("CAGE", value=vals.get("cage", ""))
            naics = st.text_input("NAICS (comma separated)",
                                  value=vals.get("naics", ""))
            core_competencies = st.text_area("Core Competencies (one per line)", value=vals.get(
                "core_competencies", ""), height=110)
            differentiators = st.text_area("Differentiators (one per line)", value=vals.get(
                "differentiators", ""), height=110)
        c3, c4 = st.columns([2, 2])
        with c3:
            certifications = st.text_area("Certifications (one per line)", value=vals.get(
                "certifications", ""), height=110)
        with c4:
            past_performance = st.text_area("Past Performance Highlights (one per line)", value=vals.get(
                "past_performance", ""), height=110)
            primary_poc = st.text_area("Primary POC (name, title, email, phone)", value=vals.get(
                "primary_poc", ""), height=70)
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
    hay = (title + " " + " ".join((rfp_sections["content"].tolist() if isinstance(
        rfp_sections, pd.DataFrame) and not rfp_sections.empty else []))).lower()
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
        if val >= 1000000:
            score += 6
        elif val >= 250000:
            score += 3
    except Exception:
        pass
    return min(score, 100)


def _pp_writeup_block(rec: dict) -> str:
    parts = []
    title = rec.get("project_title") or "Project"
    cust = rec.get("customer") or ""
    cn = rec.get("contract_no") or ""
    role = rec.get("role") or ""
    pop = " – ".join(
        [x for x in [rec.get("pop_start") or "", rec.get("pop_end") or ""] if x])
    val = rec.get("value") or ""
    parts.append(f"**{title}** — {cust} {('(' + cn + ')') if cn else ''}")
    meta_bits = [b for b in [f"Role: {role}" if role else "", f"POP: {pop}" if pop else "", f"Value: ${val:,.0f}" if isinstance(
        val, (int, float)) else (f"Value: {val}" if val else ""), f"NAICS: {rec.get('naics', '')}"] if b]
    if meta_bits:
        parts.append("  \n" + " | ".join(meta_bits))
    if rec.get("scope"):
        parts.append(f"**Scope/Work:** {rec['scope']}")
    if rec.get("results"):
        parts.append(f"**Results/Outcome:** {rec['results']}")
    if rec.get("cpars_rating"):
        parts.append(f"**CPARS:** {rec['cpars_rating']}")
    if any([rec.get("contact_name"), rec.get("contact_email"), rec.get("contact_phone")]):
        parts.append("**POC:** " + ", ".join([x for x in [rec.get(
            "contact_name"), rec.get("contact_email"), rec.get("contact_phone")] if x]))
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
        s.top_margin = Inches(1)
        s.bottom_margin = Inches(1)
        s.left_margin = Inches(1)
        s.right_margin = Inches(1)
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
    st.caption(
        "Store/import projects, score relevance vs an RFP, generate writeups, and push to Proposal Builder.")

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
                    n = 0
                    with closing(conn.cursor()) as cur:
                        for _, r in df.iterrows():
                            cur.execute("""
                                INSERT INTO past_perf(project_title, customer, contract_no, naics, role, pop_start, pop_end, value, scope, results, cpars_rating, contact_name, contact_email, contact_phone, keywords, notes)
                                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                            """, (
                                str(r.get("project_title", ""))[:200],
                                str(r.get("customer", ""))[:200],
                                str(r.get("contract_no", ""))[:100],
                                str(r.get("naics", ""))[:20],
                                str(r.get("role", ""))[:100],
                                str(r.get("pop_start", ""))[:20],
                                str(r.get("pop_end", ""))[:20],
                                float(r.get("value")) if str(
                                    r.get("value", "")).strip() not in ("", "nan") else None,
                                str(r.get("scope", ""))[:2000],
                                str(r.get("results", ""))[:2000],
                                str(r.get("cpars_rating", ""))[:100],
                                str(r.get("contact_name", ""))[:200],
                                str(r.get("contact_email", ""))[:200],
                                str(r.get("contact_phone", ""))[:100],
                                str(r.get("keywords", ""))[:500],
                                str(r.get("notes", ""))[:500],
                            ))
                            n += 1
                    conn.commit()
                    st.success(f"Imported {n} projects.")
            except Exception as e:
                st.error(f"Import failed: {e}")

    # Add Project
    with st.expander("Add Project", expanded=False):
        c1, c2, c3 = st.columns([2, 2, 2])
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
        f1, f2, f3 = st.columns([2, 2, 2])
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
    st.dataframe(df[["id", "project_title", "customer", "contract_no", "naics", "role", "pop_start",
                 "pop_end", "value", "cpars_rating"]], use_container_width=True, hide_index=True)
    selected_ids = st.multiselect("Select projects for writeup", options=df["id"].tolist(
    ), format_func=lambda i: f"#{i} — {df.loc[df['id'] == i, 'project_title'].values[0]}")

    # Relevance scoring vs RFP
    df_rf = pd.read_sql_query(
        "SELECT id, title FROM rfps_t ORDER BY id DESC;", conn)
    rfp_id = None
    if not df_rf.empty:
        rfp_id = st.selectbox("RFP context for relevance scoring (optional)", options=[None] + df_rf["id"].tolist(),
                              format_func=lambda rid: "None" if rid is None else f"#{rid} — {df_rf.loc[df_rf['id'] == rid, 'title'].values[0]}")
    if rfp_id:
        ctx = _load_rfp_context(conn, int(rfp_id))
        title = (ctx["rfp"].iloc[0]["title"] if ctx["rfp"]
                 is not None and not ctx["rfp"].empty else "")
        secs = ctx.get("sections", pd.DataFrame())
        # Compute scores
        scores = []
        for _, r in df.iterrows():
            scores.append(_pp_score_one(r.to_dict(), title, secs))
        df_sc = df.copy()
        df_sc["Relevance"] = scores
        st.subheader("Relevance vs selected RFP")
        st.dataframe(df_sc[["project_title", "naics", "role", "pop_end", "value", "Relevance"]].sort_values("Relevance", ascending=False),
                     use_container_width=True, hide_index=True)

    # Generate writeups
    st.subheader("Generate Writeups")
    tone = st.selectbox("Template", ["Concise bullets", "Narrative paragraph"])
    max_n = st.slider("How many projects", 1, 7, min(
        3, len(selected_ids)) if selected_ids else 3)
    do_gen = st.button("Generate", type="primary")
    if do_gen:
        picked = df[df["id"].isin(selected_ids)].head(
            max_n).to_dict(orient="records")
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
        p = doc.add_paragraph(subtitle)
        p.runs[0].italic = True
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
        df_t = pd.read_sql_query(
            "SELECT id, name, description, created_at FROM white_templates ORDER BY id DESC;", conn)
        t_col1, t_col2 = st.columns([2, 2])
        with t_col1:
            st.subheader("Create Template")
            t_name = st.text_input("Template name", key="wp_t_name")
            t_desc = st.text_area("Description", key="wp_t_desc", height=70)
            if st.button("Save Template", key="wp_t_save"):
                if not t_name.strip():
                    st.error("Name required")
                else:
                    with closing(conn.cursor()) as cur:
                        cur.execute("INSERT INTO white_templates(name, description, created_at) VALUES(?,?,datetime('now'));", (
                            t_name.strip(), t_desc.strip()))
                        conn.commit()
                    st.success("Template saved")
                    st.rerun()
        with t_col2:
            if df_t.empty:
                st.info("No templates yet.")
            else:
                st.subheader("Edit Template Sections")
                t_sel = st.selectbox("Choose template", options=df_t["id"].tolist(
                ), format_func=lambda tid: df_t.loc[df_t["id"] == tid, "name"].values[0], key="wp_t_sel")
                df_ts = _wp_load_template(conn, int(t_sel))
                st.dataframe(df_ts, use_container_width=True, hide_index=True)
                st.markdown("**Add section**")
                ts_title = st.text_input("Section title", key="wp_ts_title")
                ts_body = st.text_area(
                    "Default body", key="wp_ts_body", height=120)
                if st.button("Add section to template", key="wp_ts_add"):
                    pos = int((df_ts["position"].max()
                              if not df_ts.empty else 0) + 1)
                    with closing(conn.cursor()) as cur:
                        cur.execute("INSERT INTO white_template_sections(template_id, position, title, body) VALUES(?,?,?,?);",
                                    (int(t_sel), pos, ts_title.strip(), ts_body.strip()))
                        conn.commit()
                    st.success("Section added")
                    st.rerun()
                # Reorder / delete (simple)
                if not df_ts.empty:
                    st.markdown("**Reorder / Delete**")
                    for _, r in df_ts.iterrows():
                        c1, c2, c3 = st.columns([2, 1, 1])
                        with c1:
                            new_pos = st.number_input(f"#{int(r['id'])} pos", min_value=1, value=int(
                                r['position']), step=1, key=f"wp_ts_pos_{int(r['id'])}")
                        with c2:
                            if st.button("Apply", key=f"wp_ts_pos_apply_{int(r['id'])}"):
                                with closing(conn.cursor()) as cur:
                                    cur.execute("UPDATE white_template_sections SET position=? WHERE id=?;", (int(
                                        new_pos), int(r["id"])))
                                    conn.commit()
                                st.success("Updated position")
                                st.rerun()
                        with c3:
                            if st.button("Delete", key=f"wp_ts_del_{int(r['id'])}"):
                                with closing(conn.cursor()) as cur:
                                    cur.execute(
                                        "DELETE FROM white_template_sections WHERE id=?;", (int(r["id"]),))
                                    conn.commit()
                                st.success("Deleted")
                                st.rerun()

    st.divider()

    # --- Drafts ---
    st.subheader("Drafts")
    df_p = pd.read_sql_query(
        "SELECT id, title, subtitle, created_at, updated_at FROM white_papers ORDER BY id DESC;", conn)
    c1, c2 = st.columns([2, 2])
    with c1:
        st.markdown("**Create draft from template**")
        df_t = pd.read_sql_query(
            "SELECT id, name FROM white_templates ORDER BY id DESC;", conn)
        d_title = st.text_input("Draft title", key="wp_d_title")
        d_sub = st.text_input("Subtitle (optional)", key="wp_d_sub")
        if df_t.empty:
            st.caption("No templates available")
            t_sel2 = None
        else:
            t_sel2 = st.selectbox("Template", options=[None] + df_t["id"].tolist(),
                                  format_func=lambda x: "Blank" if x is None else df_t.loc[
                                      df_t["id"] == x, "name"].values[0],
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
                st.success("Draft created")
                st.rerun()
    with c2:
        if df_p.empty:
            st.info("No drafts yet.")
        else:
            st.markdown("**Open a draft**")
            p_sel = st.selectbox("Draft", options=df_p["id"].tolist(
            ), format_func=lambda pid: df_p.loc[df_p["id"] == pid, "title"].values[0], key="wp_d_sel")

    # Editing panel
    if 'p_sel' in locals() and p_sel:
        st.subheader(f"Editing draft #{int(p_sel)}")
        df_sec = _wp_load_paper(conn, int(p_sel))
        # Add section
        st.markdown("**Add section**")
        ns_title = st.text_input("Section title", key="wp_ns_title")
        ns_body = st.text_area("Body", key="wp_ns_body", height=140)
        ns_img = st.file_uploader("Optional image", type=[
                                  "png", "jpg", "jpeg"], key="wp_ns_img")
        if st.button("Add section", key="wp_ns_add"):
            img_path = None
            if ns_img is not None:
                img_path = save_uploaded_file(ns_img, subdir="whitepapers")
            pos = int((df_sec["position"].max()
                      if not df_sec.empty else 0) + 1)
            with closing(conn.cursor()) as cur:
                cur.execute("INSERT INTO white_paper_sections(paper_id, position, title, body, image_path) VALUES(?,?,?,?,?);",
                            (int(p_sel), pos, ns_title.strip(), ns_body.strip(), img_path))
                cur.execute(
                    "UPDATE white_papers SET updated_at=datetime('now') WHERE id=?;", (int(p_sel),))
                conn.commit()
            st.success("Section added")
            st.rerun()

        # Section list
        if df_sec.empty:
            st.info("No sections yet.")
        else:
            for _, r in df_sec.iterrows():
                st.markdown(
                    f"**Section #{int(r['position'])}: {r.get('title') or 'Untitled'}**")
                e1, e2, e3, e4 = st.columns([2, 1, 1, 1])
                with e1:
                    new_title = st.text_input("Title", value=r.get(
                        "title") or "", key=f"wp_sec_title_{int(r['id'])}")
                    new_body = st.text_area("Body", value=r.get(
                        "body") or "", key=f"wp_sec_body_{int(r['id'])}", height=140)
                with e2:
                    new_pos = st.number_input("Pos", value=int(
                        r["position"]), min_value=1, step=1, key=f"wp_sec_pos_{int(r['id'])}")
                    if st.button("Apply", key=f"wp_sec_apply_{int(r['id'])}"):
                        with closing(conn.cursor()) as cur:
                            cur.execute("UPDATE white_paper_sections SET title=?, body=?, position=? WHERE id=?;",
                                        (new_title.strip(), new_body.strip(), int(new_pos), int(r["id"])))
                            cur.execute(
                                "UPDATE white_papers SET updated_at=datetime('now') WHERE id=?;", (int(p_sel),))
                            conn.commit()
                        st.success("Updated")
                        st.rerun()
                with e3:
                    up_img = st.file_uploader("Replace image", type=[
                                              "png", "jpg", "jpeg"], key=f"wp_sec_img_{int(r['id'])}")
                    if st.button("Save image", key=f"wp_sec_img_save_{int(r['id'])}"):
                        if up_img is None:
                            st.warning("Choose an image first")
                        else:
                            img_path = save_uploaded_file(
                                up_img, subdir="whitepapers")
                            with closing(conn.cursor()) as cur:
                                cur.execute(
                                    "UPDATE white_paper_sections SET image_path=? WHERE id=?;", (img_path, int(r["id"])))
                                cur.execute(
                                    "UPDATE white_papers SET updated_at=datetime('now') WHERE id=?;", (int(p_sel),))
                                conn.commit()
                            st.success("Image saved")
                            st.rerun()
                with e4:
                    if st.button("Delete", key=f"wp_sec_del_{int(r['id'])}"):
                        with closing(conn.cursor()) as cur:
                            cur.execute(
                                "DELETE FROM white_paper_sections WHERE id=?;", (int(r["id"]),))
                            cur.execute(
                                "UPDATE white_papers SET updated_at=datetime('now') WHERE id=?;", (int(p_sel),))
                            conn.commit()
                        st.success("Deleted")
                        st.rerun()
                st.divider()

            # Export & Push
            x1, x2 = st.columns([2, 2])
            with x1:
                if st.button("Export DOCX", key="wp_export"):
                    out_path = str(Path(DATA_DIR) /
                                   f"White_Paper_{int(p_sel)}.docx")
                    exp = _wp_export_docx(out_path,
                                          df_p.loc[df_p["id"] ==
                                                   p_sel, "title"].values[0],
                                          df_p.loc[df_p["id"] == p_sel,
                                                   "subtitle"].values[0] if "subtitle" in df_p.columns else "",
                                          _wp_load_paper(conn, int(p_sel)))
                    if exp:
                        st.success("Exported")
                        st.markdown(f"[Download DOCX]({exp})")
            with x2:
                if st.button("Push narrative to Proposal Builder", key="wp_push"):
                    # Concatenate sections to markdown
                    secs = _wp_load_paper(conn, int(p_sel))
                    lines = []
                    for _, rr in secs.sort_values("position").iterrows():
                        lines.append(
                            f"## {rr.get('title') or 'Section'}\n\n{rr.get('body') or ''}")
                    md = "\n\n".join(lines)
                    st.session_state["pb_section_White Paper"] = md
                    st.success(
                        "Pushed to Proposal Builder → 'White Paper' section")


# ---------- Phase I: CRM (Activities • Tasks • Pipeline) ----------
def _stage_probability(stage: str) -> int:
    mapping = {
        "New": 10, "Qualifying": 30, "Bidding": 50, "Submitted": 60, "Awarded": 100, "Lost": 0
    }
    return mapping.get(stage or "", 10)


def run_crm(conn: sqlite3.Connection) -> None:
    st.header("CRM")
    tabs = st.tabs(["Activities", "Tasks", "Pipeline"])

    # --- Activities
    with tabs[0]:
        st.subheader("Log Activity")
        df_deals = pd.read_sql_query(
            "SELECT id, title FROM deals_t ORDER BY id DESC;", conn)
        df_contacts = pd.read_sql_query(
            "SELECT id, name FROM contacts_t ORDER BY name;", conn)
        a_col1, a_col2, a_col3 = st.columns([2, 2, 2])
        with a_col1:
            a_type = st.selectbox(
                "Type", ["Call", "Email", "Meeting", "Note"], key="act_type")
            a_subject = st.text_input("Subject", key="act_subject")
        with a_col2:
            a_deal = st.selectbox("Related Deal (optional)", options=[None] + df_deals["id"].tolist(),
                                  format_func=lambda x: "None" if x is None else f"#{x} — {df_deals.loc[df_deals['id'] == x, 'title'].values[0]}",
                                  key="act_deal")
            a_contact = st.selectbox("Related Contact (optional)", options=[None] + df_contacts["id"].tolist(),
                                     format_func=lambda x: "None" if x is None else df_contacts.loc[
                                         df_contacts["id"] == x, "name"].values[0],
                                     key="act_contact")
        with a_col3:
            a_notes = st.text_area("Notes", height=100, key="act_notes")
            if st.button("Save Activity", key="act_save"):
                with closing(conn.cursor()) as cur:
                    cur.execute("""
                        INSERT INTO activities(ts, type, subject, notes, deal_id, contact_id) VALUES(datetime('now'),?,?,?,?,?);
                    """, (a_type, a_subject.strip(), a_notes.strip(), a_deal if a_deal else None, a_contact if a_contact else None))
                    conn.commit()
                st.success("Saved")

        st.subheader("Activity Log")
        f1, f2, f3 = st.columns([2, 2, 2])
        with f1:
            f_type = st.multiselect(
                "Type filter", ["Call", "Email", "Meeting", "Note"])
        with f2:
            f_deal = st.selectbox("Deal filter", options=[None] + df_deals["id"].tolist(),
                                  format_func=lambda x: "All" if x is None else f"#{x} — {df_deals.loc[df_deals['id'] == x, 'title'].values[0]}",
                                  key="act_f_deal")
        with f3:
            f_contact = st.selectbox("Contact filter", options=[None] + df_contacts["id"].tolist(),
                                     format_func=lambda x: "All" if x is None else df_contacts.loc[
                                         df_contacts["id"] == x, "name"].values[0],
                                     key="act_f_contact")
        q = "SELECT ts, type, subject, notes, deal_id, contact_id FROM activities_t WHERE 1=1"
        params = []
        if f_type:
            q += " AND type IN (%s)" % ",".join(["?"]*len(f_type))
            params.extend(f_type)
        if f_deal:
            q += " AND deal_id=?"
            params.append(f_deal)
        if f_contact:
            q += " AND contact_id=?"
            params.append(f_contact)
        q += " ORDER BY ts DESC"
        df_a = pd.read_sql_query(q, conn, params=params)
        if df_a.empty:
            st.write("No activities")
        else:
            st.dataframe(df_a, use_container_width=True, hide_index=True)
            if st.button("Export CSV", key="act_export"):
                path = str(Path(DATA_DIR) / "activities.csv")
                df_a.to_csv(path, index=False)
                st.markdown(f"[Download CSV]({path})")

    # --- Tasks
    with tabs[1]:
        st.subheader("New Task")
        df_deals = pd.read_sql_query(
            "SELECT id, title FROM deals_t ORDER BY id DESC;", conn)
        df_contacts = pd.read_sql_query(
            "SELECT id, name FROM contacts_t ORDER BY name;", conn)
        t1, t2, t3 = st.columns([2, 2, 2])
        with t1:
            t_title = st.text_input("Task title", key="task_title")
            t_due = st.date_input("Due date", key="task_due")
        with t2:
            t_priority = st.selectbox(
                "Priority", ["Low", "Normal", "High"], index=1, key="task_priority")
            t_status = st.selectbox(
                "Status", ["Open", "In Progress", "Done"], index=0, key="task_status")
        with t3:
            t_deal = st.selectbox("Related Deal (optional)", options=[None] + df_deals["id"].tolist(),
                                  format_func=lambda x: "None" if x is None else f"#{x} — {df_deals.loc[df_deals['id'] == x, 'title'].values[0]}",
                                  key="task_deal")
            t_contact = st.selectbox("Related Contact (optional)", options=[None] + df_contacts["id"].tolist(),
                                     format_func=lambda x: "None" if x is None else df_contacts.loc[
                                         df_contacts["id"] == x, "name"].values[0],
                                     key="task_contact")
        if st.button("Add Task", key="task_add"):
            with closing(conn.cursor()) as cur:
                cur.execute("""
                    INSERT INTO tasks(title, due_date, status, priority, deal_id, contact_id, created_at)
                    VALUES(?,?,?,?,?,?,datetime('now'));
                """, (t_title.strip(), t_due.isoformat() if t_due else None, t_status, t_priority, t_deal if t_deal else None, t_contact if t_contact else None))
                conn.commit()
            st.success("Task added")

        st.subheader("My Tasks")
        f1, f2 = st.columns([2, 2])
        with f1:
            tf_status = st.multiselect("Status", ["Open", "In Progress", "Done"], default=[
                                       "Open", "In Progress"])
        with f2:
            tf_priority = st.multiselect(
                "Priority", ["Low", "Normal", "High"], default=[])
        q = "SELECT id, title, due_date, status, priority, deal_id, contact_id FROM tasks_t WHERE 1=1"
        params = []
        if tf_status:
            q += " AND status IN (%s)" % ",".join(["?"]*len(tf_status))
            params.extend(tf_status)
        if tf_priority:
            q += " AND priority IN (%s)" % ",".join(["?"]*len(tf_priority))
            params.extend(tf_priority)
        q += " ORDER BY COALESCE(due_date,'9999-12-31') ASC"
        df_t = pd.read_sql_query(q, conn, params=params)
        if df_t.empty:
            st.write("No tasks")
        else:
            for _, r in df_t.iterrows():
                c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
                with c1:
                    st.write(f"**{r['title']}**  — due {r['due_date'] or '—'}")
                with c2:
                    new_status = st.selectbox("Status", ["Open", "In Progress", "Done"],
                                              index=["Open", "In Progress", "Done"].index(r["status"] if r["status"] in [
                                                                                          "Open", "In Progress", "Done"] else "Open"),
                                              key=f"task_status_{int(r['id'])}")
                with c3:
                    new_pri = st.selectbox("Priority", ["Low", "Normal", "High"],
                                           index=["Low", "Normal", "High"].index(r["priority"] if r["priority"] in [
                                                                                 "Low", "Normal", "High"] else "Normal"),
                                           key=f"task_pri_{int(r['id'])}")
                with c4:
                    if st.button("Apply", key=f"task_apply_{int(r['id'])}"):
                        with closing(conn.cursor()) as cur:
                            cur.execute("UPDATE tasks SET status=?, priority=?, completed_at=CASE WHEN ?='Done' THEN datetime('now') ELSE completed_at END WHERE id=?;",
                                        (new_status, new_pri, new_status, int(r["id"])))
                            conn.commit()
                        st.success("Updated")

            if st.button("Export CSV", key="task_export"):
                path = str(Path(DATA_DIR) / "tasks.csv")
                df_t.to_csv(path, index=False)
                st.markdown(f"[Download CSV]({path})")

    # --- Pipeline
    with tabs[2]:
        st.subheader("Weighted Pipeline")
        df = pd.read_sql_query(
            "SELECT id, title, agency, status, value FROM deals_t ORDER BY id DESC;", conn)
        if df.empty:
            st.info("No deals")
        else:
            df["prob_%"] = df["status"].apply(_stage_probability)
            df["expected_value"] = (df["value"].fillna(
                0).astype(float) * df["prob_%"] / 100.0).round(2)
            # Stage age: days since last stage change
            df_log = pd.read_sql_query(
                "SELECT deal_id, stage, MAX(changed_at) AS last_change FROM deal_stage_log_t GROUP BY deal_id, stage;", conn)

            def stage_age(row):
                try:
                    last = df_log[(df_log["deal_id"] == row["id"]) & (
                        df_log["stage"] == row["status"])]["last_change"]
                    if last.empty:
                        return None
                    dt = pd.to_datetime(last.values[0])
                    return (pd.Timestamp.utcnow() - dt).days
                except Exception:
                    return None
            df["stage_age_days"] = df.apply(stage_age, axis=1)
            st.dataframe(df[["title", "agency", "status", "value", "prob_%", "expected_value",
                         "stage_age_days"]], use_container_width=True, hide_index=True)

            st.subheader("Summary by Stage")
            summary = df.groupby("status").agg(
                deals=("id", "count"),
                value=("value", "sum"),
                expected=("expected_value", "sum")
            ).reset_index().sort_values("expected", ascending=False)
            st.dataframe(summary, use_container_width=True, hide_index=True)
            if st.button("Export Pipeline CSV", key="pipe_export"):
                path = str(Path(DATA_DIR) / "pipeline.csv")
                df.to_csv(path, index=False)
                st.markdown(f"[Download CSV]({path})")


def _ensure_files_table(conn: sqlite3.Connection) -> None:
    try:
        with closing(conn.cursor()) as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files(
                    id INTEGER PRIMARY KEY,
                    owner_type TEXT,
                    owner_id INTEGER,
                    filename TEXT,
                    path TEXT,
                    size INTEGER,
                    mime TEXT,
                    tags TEXT,
                    notes TEXT,
                    uploaded_at TEXT
                );
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_owner ON files(owner_type, owner_id);")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_tags ON files(tags);")
            conn.commit()
    except Exception:
        pass

# ---------- Phase J: File Manager & Submission Kit ----------


def _detect_mime(name: str) -> str:
    name = (name or "").lower()
    if name.endswith(".pdf"):
        return "application/pdf"
    if name.endswith(".docx"):
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if name.endswith(".xlsx"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if name.endswith(".zip"):
        return "application/zip"
    if name.endswith(".png"):
        return "image/png"
    if name.endswith(".jpg") or name.endswith(".jpeg"):
        return "image/jpeg"
    if name.endswith(".txt"):
        return "text/plain"
    return "application/octet-stream"


def run_file_manager(conn: sqlite3.Connection) -> None:
    _ensure_files_table(conn)
    st.header("File Manager")
    st.caption(
        "Attach files to RFPs / Deals / Vendors, tag them, and build a zipped submission kit.")

    # --- Attach uploader ---
    with st.expander("Upload & Attach", expanded=True):
        c1, c2 = st.columns([2, 2])
        with c1:
            owner_type = st.selectbox(
                "Attach to", ["RFP", "Deal", "Vendor", "Other"], key="fm_owner_type")
            owner_id = None
            if owner_type == "RFP":
                df_rf = pd.read_sql_query(
                    "SELECT id, title FROM rfps_t ORDER BY id DESC;", conn)
                if not df_rf.empty:
                    owner_id = st.selectbox("RFP", options=df_rf["id"].tolist(),
                                            format_func=lambda i: f"#{i} — {df_rf.loc[df_rf['id'] == i, 'title'].values[0]}",
                                            key="fm_owner_rfp")
            elif owner_type == "Deal":
                df_deal = pd.read_sql_query(
                    "SELECT id, title FROM deals_t ORDER BY id DESC;", conn)
                if not df_deal.empty:
                    owner_id = st.selectbox("Deal", options=df_deal["id"].tolist(),
                                            format_func=lambda i: f"#{i} — {df_deal.loc[df_deal['id'] == i, 'title'].values[0]}",
                                            key="fm_owner_deal")
            elif owner_type == "Vendor":
                df_v = pd.read_sql_query(
                    "SELECT id, name FROM vendors_t ORDER BY name;", conn)
                if not df_v.empty:
                    owner_id = st.selectbox("Vendor", options=df_v["id"].tolist(),
                                            format_func=lambda i: f"#{i} — {df_v.loc[df_v['id'] == i, 'name'].values[0]}",
                                            key="fm_owner_vendor")
            # Owner_id can be None for "Other"
        with c2:
            tags = st.text_input("Tags (comma-separated)", key="fm_tags")
            notes = st.text_area("Notes (optional)", height=70, key="fm_notes")

        ups = st.file_uploader("Select files", type=None,
                               accept_multiple_files=True, key="fm_files")
        if st.button("Upload", key="fm_upload"):
            if not ups:
                st.warning("Pick at least one file")
            else:
                saved = 0
                for f in ups:
                    pth = save_uploaded_file(f, subdir="attachments")
                    if not pth:
                        continue
                    try:
                        with closing(conn.cursor()) as cur:
                            cur.execute("""
                                INSERT INTO files(owner_type, owner_id, filename, path, size, mime, tags, notes, uploaded_at)
                                VALUES(?,?,?,?,?,?,?,?,datetime('now'));
                            """, (
                                owner_type, int(
                                    owner_id) if owner_id else None, f.name, pth, f.size, _detect_mime(f.name),
                                tags.strip(), notes.strip()
                            ))
                            conn.commit()
                            saved += 1
                    except Exception as e:
                        st.error(f"DB save failed: {e}")
                st.success(f"Uploaded {saved} file(s).")

    # --- Library & filters ---
    with st.expander("Library", expanded=True):
        l1, l2, l3 = st.columns([2, 2, 2])
        with l1:
            f_owner = st.selectbox(
                "Filter by type", ["All", "RFP", "Deal", "Vendor", "Other"], key="fm_f_owner")
        with l2:
            f_tag = st.text_input("Tag contains", key="fm_f_tag")
        with l3:
            f_kw = st.text_input("Filename contains", key="fm_f_kw")

        q = "SELECT id, owner_type, owner_id, filename, path, size, mime, tags, notes, uploaded_at FROM files_t WHERE 1=1"
        params = []
        if f_owner and f_owner != "All":
            q += " AND owner_type=?"
            params.append(f_owner)
        if f_tag:
            q += " AND tags LIKE ?"
            params.append(f"%{f_tag}%")
        if f_kw:
            q += " AND filename LIKE ?"
            params.append(f"%{f_kw}%")
        q += " ORDER BY uploaded_at DESC"
        try:
            df_files = pd.read_sql_query(q, conn, params=params)
        except Exception as e:
            _ensure_files_table(conn)
            try:
                df_files = pd.read_sql_query(q, conn, params=params)
            except Exception as e2:
                st.error(f"Failed to load files: {e2}")
                df_files = pd.DataFrame()
        if df_files.empty:
            st.write("No files yet.")
        else:
            st.dataframe(df_files.drop(
                columns=["path"]), use_container_width=True, hide_index=True)
            # Per-row controls
            for _, r in df_files.iterrows():
                c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
                with c1:
                    st.caption(
                        f"#{int(r['id'])} — {r['filename']} ({r['owner_type']} {int(r['owner_id']) if r['owner_id'] else ''})")
                with c2:
                    new_tags = st.text_input("Tags", value=r.get(
                        "tags") or "", key=f"fm_row_tags_{int(r['id'])}")
                with c3:
                    new_notes = st.text_input("Notes", value=r.get(
                        "notes") or "", key=f"fm_row_notes_{int(r['id'])}")
                with c4:
                    b1, b2 = st.columns(2)
                    with b1:
                        if st.button("Save", key=f"fm_row_save_{int(r['id'])}"):
                            with closing(conn.cursor()) as cur:
                                cur.execute("UPDATE files SET tags=?, notes=? WHERE id=?;", (new_tags.strip(
                                ), new_notes.strip(), int(r["id"])))
                                conn.commit()
                            st.success("Updated")
                    with b2:
                        if st.button("Delete", key=f"fm_row_del_{int(r['id'])}"):
                            with closing(conn.cursor()) as cur:
                                cur.execute(
                                    "DELETE FROM files_t WHERE id=?;", (int(r["id"]),))
                                conn.commit()
                            try:
                                import os
                                if r.get("path") and os.path.exists(r["path"]):
                                    os.remove(r["path"])
                            except Exception:
                                pass
                            st.success("Deleted")
                            st.rerun()

    # --- Submission Kit (ZIP) ---
    st.subheader("Submission Kit (ZIP)")
    df_rf_all = pd.read_sql_query(
        "SELECT id, title FROM rfps_t ORDER BY id DESC;", conn)
    if df_rf_all.empty:
        st.info("Create an RFP in RFP Analyzer first (Parse → Save).")
        return

    kit_rfp = st.selectbox("RFP", options=df_rf_all["id"].tolist(),
                           format_func=lambda rid: f"#{rid} — {df_rf_all.loc[df_rf_all['id'] == rid, 'title'].values[0]}",
                           key="fm_kit_rfp")

    # Load files for this RFP
    try:
        df_kit = pd.read_sql_query(
            "SELECT id, filename, path, tags FROM files_t WHERE owner_type='RFP' AND owner_id=? ORDER BY uploaded_at DESC;", conn, params=(int(kit_rfp),))
    except Exception:
        _ensure_files_table(conn)
        df_kit = pd.DataFrame(columns=["id", "filename", "path", "tags"])
    st.caption("Select attachments to include")
    selected = []
    if df_kit.empty:
        st.write("No attachments linked to this RFP yet.")
    else:
        for _, r in df_kit.iterrows():
            if st.checkbox(f"{r['filename']}  {('['+r['tags']+']') if r.get('tags') else ''}", key=f"fm_ck_{int(r['id'])}"):
                selected.append(int(r["id"]))

    # Optional: include generated docs if they exist
    st.markdown("**Optional generated docs to include (if found):**")
    gen_paths = []
    # Proposal doc
    prop_path = str(Path(DATA_DIR) / f"Proposal_RFP_{int(kit_rfp)}.docx")
    if Path(prop_path).exists():
        if st.checkbox("Include Proposal DOCX", key="fm_inc_prop"):
            gen_paths.append(prop_path)
    # Past Performance writeups
    pp_path = str(Path(DATA_DIR) / "Past_Performance_Writeups.docx")
    if Path(pp_path).exists():
        if st.checkbox("Include Past Performance DOCX", key="fm_inc_pp"):
            gen_paths.append(pp_path)
    # White papers (include any)
    white_candidates = sorted(Path(DATA_DIR).glob("White_Paper_*.docx"))
    if white_candidates:
        inc_wp = st.multiselect("Include White Papers", options=[str(p) for p in white_candidates],
                                format_func=lambda p: Path(p).name, key="fm_inc_wp")
        gen_paths.extend(inc_wp)

    if st.button("Build ZIP", type="primary", key="fm_build_zip"):
        if not selected and not gen_paths:
            st.warning("Select at least one attachment or generated document.")
        else:
            # Collect paths
            rows = []
            if selected:
                ph = ",".join(["?"]*len(selected))
                df_sel = pd.read_sql_query(
                    f"SELECT filename, path FROM files_t WHERE id IN ({ph});", conn, params=selected)
                for _, r in df_sel.iterrows():
                    rows.append((r["filename"], r["path"]))
            for p in gen_paths:
                rows.append((Path(p).name, p))

            # Create ZIP
            from zipfile import ZipFile, ZIP_DEFLATED
            ts = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
            zip_path = str(Path(DATA_DIR) /
                           f"submission_kit_RFP_{int(kit_rfp)}_{ts}.zip")
            try:
                with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as z:
                    for fname, p in rows:
                        try:
                            z.write(p, arcname=fname)
                        except Exception:
                            pass
                    # Add a manifest
                    manifest = "Submission Kit Manifest\n"
                    manifest += f"RFP ID: {int(kit_rfp)}\n"
                    manifest += "\nIncluded files:\n" + \
                        "\n".join(f"- {fname}" for fname, _ in rows)
                    z.writestr("MANIFEST.txt", manifest)
                st.success("Submission kit created")
                st.markdown(f"[Download ZIP]({zip_path})")
            except Exception as e:
                st.error(f"ZIP failed: {e}")


# ---------- Phase L: RFQ Pack ----------
def _rfq_pack_by_id(conn: sqlite3.Connection, pid: int) -> dict | None:
    df = pd.read_sql_query(
        "SELECT * FROM rfq_packs_t WHERE id=?;", conn, params=(pid,))
    return None if df.empty else df.iloc[0].to_dict()


def _rfq_lines(conn: sqlite3.Connection, pid: int) -> pd.DataFrame:
    return pd.read_sql_query("SELECT id, clin_code, description, qty, unit, naics, psc FROM rfq_lines_t WHERE pack_id=? ORDER BY id ASC;", conn, params=(pid,))


def _rfq_vendors(conn: sqlite3.Connection, pid: int) -> pd.DataFrame:
    q = """
        SELECT rv.id, rv.vendor_id, v.name, v.email, v.phone
        FROM rfq_vendors_t rv
        JOIN vendors v ON v.id = rv.vendor_id
        WHERE rv.pack_id=?
        ORDER BY v.name;
    """
    try:
        return pd.read_sql_query(q, conn, params=(pid,))
    except Exception:
        return pd.DataFrame(columns=["id", "vendor_id", "name", "email", "phone"])


def _rfq_attachments(conn: sqlite3.Connection, pid: int) -> pd.DataFrame:
    return pd.read_sql_query("SELECT id, file_id, name, path FROM rfq_attach_t WHERE pack_id=? ORDER BY id ASC;", conn, params=(pid,))


def _rfq_build_zip(conn: sqlite3.Connection, pack_id: int) -> str | None:
    from zipfile import ZipFile, ZIP_DEFLATED
    pack = _rfq_pack_by_id(conn, pack_id)
    if not pack:
        st.error("Pack not found")
        return None
    title = pack.get("title") or f"RFQ_{pack_id}"
    # Files to include
    df_att = _rfq_attachments(conn, pack_id)
    files = []
    for _, r in df_att.iterrows():
        if r.get("path"):
            files.append((r["name"] or Path(r["path"]).name, r["path"]))
        elif r.get("file_id"):
            # fallback to files table
            try:
                df = pd.read_sql_query(
                    "SELECT filename, path FROM files_t WHERE id=?;", conn, params=(int(r["file_id"]),))
                if not df.empty:
                    files.append((df.iloc[0]["filename"], df.iloc[0]["path"]))
            except Exception:
                pass
    # CLINs CSV
    df_lines = _rfq_lines(conn, pack_id)
    clin_csv_path = str(Path(DATA_DIR) / f"rfq_{pack_id}_CLINs.csv")
    df_lines.to_csv(clin_csv_path, index=False)
    files.append((Path(clin_csv_path).name, clin_csv_path))

    # Mail-merge CSV for vendors
    df_v = _rfq_vendors(conn, pack_id)
    mail_csv_path = str(
        Path(DATA_DIR) / f"rfq_{pack_id}_vendors_mailmerge.csv")
    mm = df_v.rename(columns={"name": "VendorName", "email": "VendorEmail", "phone": "VendorPhone"})[
        ["VendorName", "VendorEmail", "VendorPhone"]]
    mm["Subject"] = f"Request for Quote – {title}"
    due = pack.get("due_date") or ""
    mm["Body"] = (
        f"Hello {{VendorName}},\n\n"
        f"Please review the attached RFQ package for '{title}'. "
        f"Reply with pricing and availability no later than {due}.\n\n"
        f"Thank you,"
    )
    mm.to_csv(mail_csv_path, index=False)
    files.append((Path(mail_csv_path).name, mail_csv_path))

    # Build zip
    ts = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    zip_path = str(Path(DATA_DIR) / f"RFQ_Pack_{pack_id}_{ts}.zip")
    try:
        with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as z:
            for fname, pth in files:
                try:
                    z.write(pth, arcname=fname)
                except Exception:
                    pass
            # manifest
            manifest = "RFQ Pack Manifest\n"
            manifest += f"Title: {title}\n"
            manifest += f"Due: {pack.get('due_date') or ''}\n"
            manifest += f"Lines: {len(df_lines)}\n"
            manifest += f"Vendors: {len(df_v)}\n"
            z.writestr("MANIFEST.txt", manifest)
        return zip_path
    except Exception as e:
        st.error(f"ZIP failed: {e}")
        return None


def run_rfq_pack(conn: sqlite3.Connection) -> None:
    st.header("RFQ Pack")
    st.caption(
        "Build vendor-ready RFQ packages from your CLINs, attachments, and vendor list.")

    # -- Create / open
    left, right = st.columns([2, 2])
    with left:
        st.subheader("Create")
        df_rf = pd.read_sql_query(
            "SELECT id, title FROM rfps_t ORDER BY id DESC;", conn)
        rf_opt = st.selectbox("RFP (optional)", options=[None] + df_rf["id"].tolist(),
                              format_func=lambda x: "None" if x is None else f"#{x} — {df_rf.loc[df_rf['id'] == x, 'title'].values[0]}",
                              key="rfq_rfp_sel")
        title = st.text_input("Pack title", key="rfq_title")
        due = st.date_input("Quote due date", key="rfq_due")
        instr = st.text_area(
            "Instructions to vendors (email body)", height=100, key="rfq_instr")
        if st.button("Create RFQ Pack", key="rfq_create"):
            if not title.strip():
                st.error("Title required")
            else:
                with closing(conn.cursor()) as cur:
                    cur.execute("""
                        INSERT INTO rfq_packs(rfp_id, deal_id, title, instructions, due_date, created_at, updated_at)
                        VALUES(?,?,?,?,?,datetime('now'),datetime('now'));
                    """, (rf_opt if rf_opt else None, None, title.strip(), instr.strip(), str(due)))
                    conn.commit()
                st.success("Created")
                st.rerun()
    with right:
        st.subheader("Open")
        df_pk = pd.read_sql_query(
            "SELECT id, title, due_date, created_at FROM rfq_packs_t ORDER BY id DESC;", conn)
        if df_pk.empty:
            st.info("No RFQ packs yet")
            return
        pk_sel = st.selectbox("RFQ Pack", options=df_pk["id"].tolist(),
                              format_func=lambda pid: f"#{pid} — {df_pk.loc[df_pk['id'] == pid, 'title'].values[0]} (due {df_pk.loc[df_pk['id'] == pid, 'due_date'].values[0] or '—'})",
                              key="rfq_open_sel")

    st.divider()
    st.subheader(f"Editing pack #{int(pk_sel)}")

    # ---- CLINs / Lines ----
    st.markdown("### CLINs / Lines")
    df_lines = _rfq_lines(conn, int(pk_sel))
    st.dataframe(df_lines, use_container_width=True, hide_index=True)
    c1, c2, c3, c4, c5, c6 = st.columns([1.2, 3, 1, 1, 1, 1])
    with c1:
        l_code = st.text_input("CLIN", key="rfq_line_code")
    with c2:
        l_desc = st.text_input("Description", key="rfq_line_desc")
    with c3:
        l_qty = st.number_input("Qty", min_value=0.0,
                                value=1.0, step=1.0, key="rfq_line_qty")
    with c4:
        l_unit = st.text_input("Unit", value="EA", key="rfq_line_unit")
    with c5:
        l_naics = st.text_input("NAICS", key="rfq_line_naics")
    with c6:
        l_psc = st.text_input("PSC", key="rfq_line_psc")
    if st.button("Add Line", key="rfq_line_add"):
        with closing(conn.cursor()) as cur:
            cur.execute("""
                INSERT INTO rfq_lines(pack_id, clin_code, description, qty, unit, naics, psc)
                VALUES(?,?,?,?,?,?,?);
            """, (int(pk_sel), l_code.strip(), l_desc.strip(), float(l_qty or 0), l_unit.strip(), l_naics.strip(), l_psc.strip()))
            conn.commit()
        st.success("Line added")
        st.rerun()

    if not df_lines.empty:
        st.markdown("**Edit existing lines**")
        for _, r in df_lines.iterrows():
            ec1, ec2, ec3, ec4 = st.columns([3, 1, 1, 1])
            with ec1:
                nd = st.text_input(
                    "Desc", value=r["description"] or "", key=f"rfq_line_e_desc_{int(r['id'])}")
            with ec2:
                nq = st.number_input("Qty", value=float(
                    r["qty"] or 0), step=1.0, key=f"rfq_line_e_qty_{int(r['id'])}")
            with ec3:
                nu = st.text_input(
                    "Unit", value=r["unit"] or "EA", key=f"rfq_line_e_unit_{int(r['id'])}")
            with ec4:
                if st.button("Save", key=f"rfq_line_e_save_{int(r['id'])}"):
                    with closing(conn.cursor()) as cur:
                        cur.execute("UPDATE rfq_lines SET description=?, qty=?, unit=? WHERE id=?;",
                                    (nd.strip(), float(nq or 0), nu.strip(), int(r["id"])))
                        conn.commit()
                    st.success("Updated")
                    st.rerun()

    st.divider()

    # ---- Attachments ----
    st.markdown("### Attachments")
    pack = _rfq_pack_by_id(conn, int(pk_sel))
    rfp_id = pack.get("rfp_id")
    if rfp_id:
        df_rfp_files = pd.read_sql_query(
            "SELECT id, filename, path, tags FROM files_t WHERE owner_type='RFP' AND owner_id=? ORDER BY uploaded_at DESC;", conn, params=(int(rfp_id),))
    else:
        df_rfp_files = pd.DataFrame(columns=["id", "filename", "path", "tags"])
    df_att = _rfq_attachments(conn, int(pk_sel))
    st.dataframe(df_att.drop(columns=[]),
                 use_container_width=True, hide_index=True)

    st.markdown("**Add from File Manager**")
    # allow selecting from all files
    df_all_files = pd.read_sql_query(
        "SELECT id, filename FROM files_t ORDER BY uploaded_at DESC;", conn)
    add_file = st.selectbox("File", options=[None] + df_all_files["id"].astype(int).tolist(),
                            format_func=lambda i: "Choose…" if i is None else f"#{i} — {df_all_files.loc[df_all_files['id'] == i, 'filename'].values[0]}",
                            key="rfq_att_file")
    if st.button("Add Attachment", key="rfq_att_add"):
        if add_file is None:
            st.warning("Pick a file")
        else:
            df_one = pd.read_sql_query(
                "SELECT filename, path FROM files_t WHERE id=?;", conn, params=(int(add_file),))
            if df_one.empty:
                st.error("File not found")
            else:
                with closing(conn.cursor()) as cur:
                    cur.execute("INSERT INTO rfq_attach(pack_id, file_id, name, path) VALUES(?,?,?,?);",
                                (int(pk_sel), int(add_file), df_one.iloc[0]["filename"], df_one.iloc[0]["path"]))
                    conn.commit()
                st.success("Added")
                st.rerun()

    if not df_att.empty:
        for _, r in df_att.iterrows():
            dc1, dc2 = st.columns([3, 1])
            with dc1:
                st.caption(
                    f"#{int(r['id'])} — {r['name'] or Path(r['path']).name}")
            with dc2:
                if st.button("Remove", key=f"rfq_att_del_{int(r['id'])}"):
                    with closing(conn.cursor()) as cur:
                        cur.execute(
                            "DELETE FROM rfq_attach_t WHERE id=?;", (int(r["id"]),))
                        conn.commit()
                    st.success("Removed")
                    st.rerun()

    st.divider()

    # ---- Vendors ----
    st.markdown("### Vendors")
    try:
        df_vendors = pd.read_sql_query(
            "SELECT id, name, email FROM vendors_t ORDER BY name;", conn)
    except Exception as e:
        st.info("No vendors table yet. Use Subcontractor Finder to add vendors.")
        df_vendors = pd.DataFrame(columns=["id", "name", "email"])
    df_rv = _rfq_vendors(conn, int(pk_sel))
    st.dataframe(df_rv[["name", "email", "phone"]] if not df_rv.empty else pd.DataFrame(
    ), use_container_width=True, hide_index=True)

    add_vs = st.multiselect("Add vendors", options=df_vendors["id"].astype(int).tolist(),
                            format_func=lambda vid: df_vendors.loc[df_vendors["id"]
                                                                   == vid, "name"].values[0],
                            key="rfq_vendor_add")
    if st.button("Add Selected Vendors", key="rfq_vendor_add_btn"):
        with closing(conn.cursor()) as cur:
            for vid in add_vs:
                try:
                    cur.execute("INSERT OR IGNORE INTO rfq_vendors(pack_id, vendor_id) VALUES(?,?);", (int(
                        pk_sel), int(vid)))
                except Exception:
                    pass
            conn.commit()
        st.success("Vendors added")
        st.rerun()

    if not df_rv.empty:
        for _, r in df_rv.iterrows():
            vc1, vc2 = st.columns([3, 1])
            with vc1:
                st.caption(f"{r['name']} — {r.get('email') or ''}")
            with vc2:
                if st.button("Remove", key=f"rfq_vendor_del_{int(r['id'])}"):
                    with closing(conn.cursor()) as cur:
                        cur.execute(
                            "DELETE FROM rfq_vendors_t WHERE id=?;", (int(r["id"]),))
                        conn.commit()
                    st.success("Removed")
                    st.rerun()

    st.divider()

    # ---- Build + Exports ----
    st.markdown("### Build & Export")
    czip, cmcsv, cclin = st.columns([2, 2, 2])
    with czip:
        if st.button("Build RFQ ZIP", type="primary", key="rfq_build_zip"):
            z = _rfq_build_zip(conn, int(pk_sel))
            if z:
                st.success("ZIP ready")
                st.markdown(f"[Download ZIP]({z})")

    with cmcsv:
        if st.button("Export Vendors Mail-Merge CSV", key="rfq_mail_csv"):
            df_v = _rfq_vendors(conn, int(pk_sel))
            if df_v.empty:
                st.warning("No vendors selected")
            else:
                out = df_v.rename(columns={"name": "VendorName", "email": "VendorEmail", "phone": "VendorPhone"})[
                    ["VendorName", "VendorEmail", "VendorPhone"]]
                out["Subject"] = f"Request for Quote – {_rfq_pack_by_id(conn, int(pk_sel)).get('title')}"
                out["Body"] = _rfq_pack_by_id(
                    conn, int(pk_sel)).get("instructions") or ""
                path = str(Path(DATA_DIR) / f"rfq_{int(pk_sel)}_mailmerge.csv")
                out.to_csv(path, index=False)
                st.success("Exported")
                st.markdown(f"[Download CSV]({path})")

    with cclin:
        if st.button("Export CLINs CSV", key="rfq_clins_csv"):
            df = _rfq_lines(conn, int(pk_sel))
            if df.empty:
                st.warning("No CLINs yet")
            else:
                path = str(Path(DATA_DIR) / f"rfq_{int(pk_sel)}_CLINs.csv")
                df.to_csv(path, index=False)
                st.success("Exported")
                st.markdown(f"[Download CSV]({path})")


def _db_path_from_conn(conn: sqlite3.Connection) -> str:
    try:
        df = pd.read_sql_query("PRAGMA database_list;", conn)
        p = df[df["name"] == "main"]["file"].values[0]
        return p or str(Path(DATA_DIR) / "app.db")
    except Exception:
        return str(Path(DATA_DIR) / "app.db")


def migrate(conn: sqlite3.Connection) -> None:
    """Lightweight idempotent migrations and indices."""
    with closing(conn.cursor()) as cur:
        # read current version
        try:
            ver = int(pd.read_sql_query(
                "SELECT ver FROM schema_version WHERE id=1;", conn).iloc[0]["ver"])
        except Exception:
            ver = 0

        # v1: add common indexes
        if ver < 1:
            try:
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_deals_stage ON deals(stage);")
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_deals_status ON deals(status);")
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_lm_items_rfp ON lm_items(rfp_id);")
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_files_owner2 ON files(owner_type, owner_id);")
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(due_date);")
            except Exception:
                pass
            cur.execute("UPDATE schema_version SET ver=1 WHERE id=1;")
            conn.commit()

        # v2: ensure NOT NULL defaults where safe (no schema changes if exists)
        if ver < 2:
            try:
                cur.execute("PRAGMA foreign_keys=ON;")
            except Exception:
                pass
            cur.execute("UPDATE schema_version SET ver=2 WHERE id=1;")
            conn.commit()

        # v3: WAL checkpoint to ensure clean state
        if ver < 3:
            try:
                cur.execute("PRAGMA wal_checkpoint(FULL);")
            except Exception:
                pass
            cur.execute("UPDATE schema_version SET ver=3 WHERE id=1;")
            conn.commit()


# ---------- Phase N: Backup & Data ----------
def _current_tenant(conn: sqlite3.Connection) -> int:
    try:
        return int(pd.read_sql_query("SELECT ctid FROM current_tenant WHERE id=1;", conn).iloc[0]["ctid"])
    except Exception:
        return 1


def _safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s or "")


def _backup_db(conn: sqlite3.Connection) -> str | None:
    # Prefer VACUUM INTO; fallback to file copy using sqlite3 backup API
    db_path = _db_path_from_conn(conn)
    ts = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    out = Path(DATA_DIR) / f"backup_{ts}.db"
    try:
        with closing(conn.cursor()) as cur:
            cur.execute(f"VACUUM INTO '{str(out)}';")
        return str(out)
    except Exception:
        # fallback: use backup API
        try:
            import sqlite3 as _sq
            src = _sq.connect(db_path)
            dst = _sq.connect(str(out))
            with dst:
                src.backup(dst)
            src.close()
            dst.close()
            return str(out)
        except Exception as e:
            st.error(f"Backup failed: {e}")
            return None


def _restore_db_from_upload(conn: sqlite3.Connection, upload) -> bool:
    # Use backup API to copy uploaded DB into main DB file
    db_path = _db_path_from_conn(conn)
    tmp = Path(DATA_DIR) / ("restore_" + _safe_name(upload.name))
    try:
        tmp.write_bytes(upload.getbuffer())
    except Exception as e:
        st.error(f"Could not write uploaded file: {e}")
        return False
    try:
        import sqlite3 as _sq
        src = _sq.connect(str(tmp))
        dst = _sq.connect(db_path)
        with dst:
            src.backup(dst)  # replaces content
        src.close()
        dst.close()
        return True
    except Exception as e:
        st.error(f"Restore failed: {e}")
        return False


def _export_table_csv(conn: sqlite3.Connection, table_or_view: str, scoped: bool = True) -> str | None:
    name = table_or_view
    if scoped and not name.endswith("_t"):
        # if a view exists, prefer it
        name_t = name + "_t"
        try:
            pd.read_sql_query(f"SELECT 1 FROM {name_t} LIMIT 1;", conn)
            name = name_t
        except Exception:
            pass
    try:
        df = pd.read_sql_query(f"SELECT * FROM {name};", conn)
        if df.empty:
            st.info("No rows to export.")
        path = Path(
            DATA_DIR) / f"export_{name}_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(path, index=False)
        return str(path)
    except Exception as e:
        st.error(f"Export failed: {e}")
        return None


def _import_csv_into_table(conn: sqlite3.Connection, csv_file, table: str, scoped_to_current: bool = True) -> int:
    # Read CSV and insert rows. If tenant_id missing and scoped, stamp with current tenant.
    import io
    try:
        df = pd.read_csv(io.BytesIO(csv_file.getbuffer()))
    except Exception as e:
        st.error(f"CSV read failed: {e}")
        return 0
    if scoped_to_current and "tenant_id" not in df.columns:
        df["tenant_id"] = _current_tenant(conn)
    # Align columns with destination
    try:
        cols = pd.read_sql_query(f"PRAGMA table_info({table});", conn)[
            "name"].tolist()
    except Exception as e:
        st.error(f"Table info failed: {e}")
        return 0
    present = [c for c in df.columns if c in cols]
    if not present:
        st.error("No matching columns in CSV.")
        return 0
    df2 = df[present].copy()
    # Drop ID if autoincrement
    if "id" in df2.columns:
        try:
            df2 = df2.drop(columns=["id"])
        except Exception:
            pass
    # Insert
    try:
        placeholders = ",".join(["?"]*len(df2.columns))
        sql = f"INSERT INTO {table}({','.join(df2.columns)}) VALUES({placeholders});"
        with closing(conn.cursor()) as cur:
            cur.executemany(sql, df2.itertuples(index=False, name=None))
            conn.commit()
        return len(df2)
    except Exception as e:
        st.error(f"Import failed: {e}")
        return 0


def run_backup_and_data(conn: sqlite3.Connection) -> None:
    st.header("Backup & Data")
    st.caption(
        "WAL on; lightweight migrations; export/import CSV; backup/restore the SQLite DB.")

    st.subheader("Database Info")
    dbp = _db_path_from_conn(conn)
    st.write(f"Path: `{dbp}`")
    try:
        ver = pd.read_sql_query(
            "SELECT ver FROM schema_version WHERE id=1;", conn).iloc[0]["ver"]
    except Exception:
        ver = "n/a"
    st.write(f"Schema version: **{ver}**")

    c1, c2, c3 = st.columns([2, 2, 2])
    with c1:
        if st.button("Run Migrations"):
            try:
                migrate(conn)
                st.success("Migrations done")
            except Exception as e:
                st.error(f"Migrations failed: {e}")
    with c2:
        if st.button("WAL Checkpoint (FULL)"):
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute("PRAGMA wal_checkpoint(FULL);")
                st.success("Checkpoint complete")
            except Exception as e:
                st.error(f"Checkpoint failed: {e}")
    with c3:
        if st.button("Analyze DB"):
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute("ANALYZE;")
                st.success("ANALYZE done")
            except Exception as e:
                st.error(f"ANALYZE failed: {e}")

    st.divider()
    st.subheader("Backup & Restore")
    b1, b2 = st.columns([2, 2])
    with b1:
        if st.button("Create Backup (.db)"):
            p = _backup_db(conn)
            if p:
                st.success("Backup created")
                st.markdown(f"[Download backup]({p})")
    with b2:
        up = st.file_uploader("Restore from .db file", type=[
                              "db", "sqlite", "sqlite3"])
        if up and st.button("Restore Now"):
            ok = _restore_db_from_upload(conn, up)
            if ok:
                st.success("Restore completed. Please rerun the app.")
                st.experimental_rerun()

    st.divider()
    st.subheader("Export / Import CSV")
    tables = ["rfps", "lm_items", "lm_meta", "deals", "activities", "tasks", "deal_stage_log",
              "vendors", "files", "rfq_packs", "rfq_lines", "rfq_vendors", "rfq_attach", "contacts"]
    tsel = st.selectbox("Table", options=tables, key="persist_tbl")
    e1, e2 = st.columns([2, 2])
    with e1:
        if st.button("Export CSV (current workspace)"):
            p = _export_table_csv(conn, tsel, scoped=True)
            if p:
                st.success("Exported")
                st.markdown(f"[Download CSV]({p})")
    with e2:
        if st.button("Export CSV (all rows)"):
            p = _export_table_csv(conn, tsel, scoped=False)
            if p:
                st.success("Exported")
                st.markdown(f"[Download CSV]({p})")

    upcsv = st.file_uploader("Import into selected table from CSV", type=[
                             "csv"], key="persist_upcsv")
    if upcsv and st.button("Import CSV"):
        n = _import_csv_into_table(conn, upcsv, tsel, scoped_to_current=True)
        if n:
            st.success(f"Imported {n} row(s) into {tsel}")
            st.experimental_rerun()


# ---------- Phase O: Global Theme & Layout ----------
def apply_theme() -> None:
    css = """
    <style>
    /* Base font and spacing */
    html, body, [class*="css"]  { font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji"; }
    .main .block-container { padding-top: 1rem; padding-bottom: 4rem; max-width: 1400px; }
    /* Headings */
    h1, h2, h3 { letter-spacing: 0.2px; }
    h1 { font-size: 1.8rem; margin-bottom: .25rem; }
    h2 { font-size: 1.2rem; margin-top: 1rem; }
    /* Sidebar */
    section[data-testid="stSidebar"] { width: 320px !important; }
    .sidebar-brand { font-weight: 700; font-size: 1.1rem; margin: .25rem 0 .5rem 0; }
    .sidebar-subtle { color: rgba(0,0,0,.55); font-size: .85rem; margin-bottom: .5rem; }
    /* Cards */
    .card { border: 1px solid rgba(0,0,0,.08); border-radius: 14px; padding: 14px 16px; margin: 8px 0 14px 0; box-shadow: 0 1px 2px rgba(0,0,0,.04); background: #fff; }
    .card h3 { margin: 0 0 6px 0; font-size: 1.05rem; }
    /* Dataframes */
    div[data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; border: 1px solid rgba(0,0,0,.08); }
    /* Tabs */
    button[data-baseweb="tab"] { padding-top: 6px !important; padding-bottom: 6px !important; font-weight: 600; }
    /* Buttons */
    .stButton>button { border-radius: 10px; padding: 0.4rem 0.8rem; }
    /* Hide Streamlit default footer/menu */
    #MainMenu {visibility: hidden;} footer {visibility: hidden;}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def page_header(title: str, subtitle: str | None = None) -> None:
    st.markdown(f"<div class='card'><h3>{title}</h3>" + (
        f"<div class='sidebar-subtle'>{subtitle}</div>" if subtitle else "") + "</div>", unsafe_allow_html=True)

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
            "File Manager",
            "Past Performance",
            "White Paper Builder",
            "Subcontractor Finder",
            "Outreach",
            "RFQ Pack",
            "Backup & Data",
            "Quote Comparison",
            "Pricing Calculator",
            "Win Probability",
            "Chat Assistant",
            "Capability Statement",
            "CRM",
            "Contacts",
            "Deals",
        ],
    )


def render_workspace_switcher(conn: sqlite3.Connection) -> None:
    with st.sidebar.expander("Workspace", expanded=True):
        try:
            df_tenants = pd.read_sql_query(
                "SELECT id, name FROM tenants ORDER BY id;", conn)
        except Exception:
            df_tenants = pd.DataFrame(columns=["id", "name"])
        try:
            cur_tid = int(pd.read_sql_query(
                "SELECT ctid FROM current_tenant WHERE id=1;", conn).iloc[0]["ctid"])
        except Exception:
            cur_tid = 1
        opt = st.selectbox("Organization", options=(df_tenants["id"].astype(int).tolist() if not df_tenants.empty else [1]),
                           format_func=lambda i: (
                               df_tenants.loc[df_tenants["id"] == i, "name"].values[0] if not df_tenants.empty else "Default"),
                           key="tenant_sel")
        if st.button("Switch", key="tenant_switch"):
            with closing(conn.cursor()) as cur:
                cur.execute(
                    "UPDATE current_tenant SET ctid=? WHERE id=1;", (int(opt),))
                conn.commit()
            st.session_state['tenant_id'] = int(opt)
            st.success("Workspace switched")
            st.rerun()

        st.divider()
        new_name = st.text_input("New workspace name", key="tenant_new_name")
        if st.button("Create workspace", key="tenant_create"):
            if new_name.strip():
                with closing(conn.cursor()) as cur:
                    cur.execute(
                        "INSERT OR IGNORE INTO tenants(name, created_at) VALUES(?, datetime('now'));", (new_name.strip(),))
                    conn.commit()
                st.success("Workspace created")
                st.rerun()
            else:
                st.warning("Enter a name")


def router(page: str, conn: sqlite3.Connection) -> None:
    if page == "SAM Watch":
        run_sam_watch(conn)
    elif page == "RFP Analyzer":
        run_rfp_analyzer(conn)
    elif page == "L and M Checklist":
        run_lm_checklist(conn)
    elif page == "Proposal Builder":
        run_proposal_builder(conn)
    elif page == "File Manager":
        run_file_manager(conn)
    elif page == "Past Performance":
        run_past_performance(conn)
    elif page == "White Paper Builder":
        run_white_paper_builder(conn)
    elif page == "Subcontractor Finder":
        run_subcontractor_finder(conn)
    elif page == "Outreach":
        run_outreach(conn)
    elif page == "RFQ Pack":
        run_rfq_pack(conn)
    elif page == "Backup & Data":
        run_backup_and_data(conn)
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
    elif page == "CRM":
        run_crm(conn)
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
