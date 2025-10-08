# ELA Management GovCon App
# Refactor scaffold into 7 phases for a 10 out of 10 all in one system
# Streamlit single file architecture with modular sections

import os
import re
import io
import json
import sqlite3
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple

import pandas as pd
import streamlit as st

# Optional docx export
try:
    from docx import Document
    from docx.shared import Pt, Inches
    DOCX_AVAILABLE = True
except Exception:
    DOCX_AVAILABLE = False

APP_TITLE = "ELA Management GovCon App"
DB_PATH = os.environ.get("ELA_DB_PATH", os.path.join(os.getcwd(), "data", "ela_govcon.db"))

################################################################################
# Utilities
################################################################################

def get_db():
        # Ensure directory exists
        db_dir = os.path.dirname(DB_PATH)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        try:
            conn = sqlite3.connect(DB_PATH, check_same_thread=False)
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA busy_timeout = 3000")
            return conn
        except sqlite3.OperationalError:
            # Fallback to local file in CWD
            fallback = os.path.join(os.getcwd(), "ela_govcon_fallback.db")
            conn = sqlite3.connect(fallback, check_same_thread=False)
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA busy_timeout = 3000")
            return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    # Users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        full_name TEXT,
        role TEXT NOT NULL,
        email TEXT,
        active INTEGER DEFAULT 1
    )
    """)
    # Deals pipeline
    cur.execute("""
    CREATE TABLE IF NOT EXISTS deals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        agency TEXT,
        naics TEXT,
        value_estimate REAL,
        stage TEXT NOT NULL,
        due_date TEXT,
        assigned_to TEXT,
        created_by TEXT,
        created_at TEXT,
        updated_at TEXT
    )
    """)
    # Contacts and subcontractors
    cur.execute("""
    CREATE TABLE IF NOT EXISTS contacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        org TEXT,
        role TEXT,
        email TEXT,
        phone TEXT,
        type TEXT,
        rating INTEGER,
        notes TEXT,
        created_at TEXT
    )
    """)
    # Activities for audit logging
    cur.execute("""
    CREATE TABLE IF NOT EXISTS activities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user TEXT,
        action TEXT,
        entity TEXT,
        entity_id INTEGER,
        ts TEXT
    )
    """)
    # Proposal artifacts
    cur.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_id INTEGER,
        doc_type TEXT,
        title TEXT,
        path TEXT,
        created_at TEXT,
        FOREIGN KEY(deal_id) REFERENCES deals(id) ON DELETE CASCADE
    )
    """)
    conn.commit()
    # Seed users if empty
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        seed_users = [
            ("", "", "admin", "@example.com"),
            ("quincy", "Quincy", "analyst", "quincy@example.com"),
            ("collin", "Collin", "analyst", "collin@example.com"),
            ("charles", "Charles", "sdr", "charles@example.com"),
        ]
        cur.executemany(
            "INSERT INTO users(username, full_name, role, email) VALUES(?,?,?,?)",
            seed_users
        )
        conn.commit()
    conn.close()


def ensure_crm_schema():
    """Ensure all CRM-related tables exist (deals, contacts, deal_contacts, deal_notes, deal_tasks)."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS deal_contacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_id INTEGER NOT NULL,
        contact_id INTEGER NOT NULL,
        UNIQUE(deal_id, contact_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS deal_notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_id INTEGER NOT NULL,
        user TEXT,
        note TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS deal_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_id INTEGER NOT NULL,
        task TEXT NOT NULL,
        due_date TEXT,
        status TEXT DEFAULT 'Open',
        created_at TEXT
    )
    """)

    conn.commit()
    conn.close()

def get_deal_contacts(deal_id:int):
    q = """
        SELECT c.* FROM contacts c
        JOIN deal_contacts dc ON dc.contact_id = c.id
        WHERE dc.deal_id=?
        ORDER BY c.name
    """
    return fetch_table(q, (deal_id,))

def set_deal_contacts(deal_id:int, contact_ids):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM deal_contacts WHERE deal_id=?", (deal_id,))
    for cid in contact_ids:
        cur.execute("INSERT OR IGNORE INTO deal_contacts(deal_id, contact_id) VALUES(?,?)", (deal_id, int(cid)))
    conn.commit()
    conn.close()

def get_deal_notes(deal_id:int):
    return fetch_table("SELECT * FROM deal_notes WHERE deal_id=? ORDER BY datetime(created_at) DESC", (deal_id,))

def add_deal_note(deal_id:int, user:str, note:str):
    execute("INSERT INTO deal_notes(deal_id, user, note, created_at) VALUES(?,?,?,?)",
            (deal_id, user, note, datetime.utcnow().isoformat()))

def get_deal_tasks(deal_id:int):
    return fetch_table("SELECT * FROM deal_tasks WHERE deal_id=? ORDER BY COALESCE(due_date, '9999-12-31')", (deal_id,))

def add_deal_task(deal_id:int, task:str, due_date:str):
    execute("INSERT INTO deal_tasks(deal_id, task, due_date, status, created_at) VALUES(?,?,?,?,?)",
            (deal_id, task, due_date, 'Open', datetime.utcnow().isoformat()))

def update_task_status(task_id:int, status:str):
    update("UPDATE deal_tasks SET status=? WHERE id=?", (status, task_id))
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS deal_contacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_id INTEGER NOT NULL,
        contact_id INTEGER NOT NULL,
        UNIQUE(deal_id, contact_id)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS deal_notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_id INTEGER NOT NULL,
        user TEXT,
        note TEXT,
        created_at TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS deal_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_id INTEGER NOT NULL,
        task TEXT NOT NULL,
        due_date TEXT,
        status TEXT DEFAULT 'Open',
        created_at TEXT
    )
    """)
    conn.commit()
    conn.close()
def log_activity(user: str, action: str, entity: str, entity_id: Optional[int]):
    conn = get_db()
    conn.execute(
        "INSERT INTO activities(user, action, entity, entity_id, ts) VALUES(?,?,?,?,?)",
        (user, action, entity, entity_id, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()

def fetch_table(query: str, params: Tuple = ()):
    conn = get_db()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def execute(query: str, params: Tuple = ()):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    rowid = cur.lastrowid
    conn.close()
    return rowid

def update(query: str, params: Tuple = ()): 
        import sqlite3 as _sqlite3
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
            conn.close()
        except _sqlite3.OperationalError:
            # Re-init schema and retry once
            init_db()
            conn = get_db()
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
            conn.close()


################################################################################
# Authentication and Roles  Phase 1
################################################################################

ROLES = {
    "admin": ["view", "edit", "submit", "manage_users", "export", "integrations"],
    "analyst": ["view", "edit", "export"],
    "sdr": ["view", "edit"],
    "viewer": ["view"]
}

def login_ui():
    st.sidebar.subheader("Login")
    usernames = fetch_table("SELECT username FROM users WHERE active=1")["username"].tolist()
    username = st.sidebar.selectbox("Select user", usernames, index=0)
    # Use a very simple sign in flow for now
    if st.sidebar.button("Sign in"):
        st.session_state["user"] = username
        role = fetch_table("SELECT role FROM users WHERE username=?", (username,)).iloc[0]["role"]
        st.session_state["role"] = role
        st.success(f"Signed in as {username} with role {role}")

def require_capability(cap: str) -> bool:
    role = st.session_state.get("role", "viewer")
    caps = ROLES.get(role, [])
    if cap in caps:
        return True
    st.info("You do not have permission for this action")
    return False

################################################################################
# CRM and Pipeline  Phase 2

def current_user() -> str:
        return st.session_state.get("user", "")

def visibility_scope() -> str:
        return st.session_state.get("scope", "Mine")


################################################################################

PIPELINE_STAGES = [
    "Lead",
    "Contacted",
    "Quote Sent",
    "Proposal",
    "Awarded",
    "Closed Lost"
]


def _deal_card(df_row):
    st.markdown(f"**{df_row['title']}**  — ${df_row['value_estimate'] or 0:,.0f}")
    st.caption(f"Agency: {df_row.get('agency','') or 'N/A'} | NAICS: {df_row.get('naics','') or 'N/A'} | Due: {df_row.get('due_date','')}")
    with st.expander("Open deal"):
        deal_id = int(df_row['id'])
        with st.form(f"edit_deal_{deal_id}"):
            colA, colB = st.columns(2)
            with colA:
                new_title = st.text_input("Title", df_row['title'])
                new_agency = st.text_input("Agency", df_row.get('agency','') or '')
                new_naics = st.text_input("NAICS", df_row.get('naics','') or '')
                new_value = st.number_input("Value estimate", value=float(df_row.get('value_estimate') or 0.0), step=1000.0)
            with colB:
                new_stage = st.selectbox("Stage", PIPELINE_STAGES, index=PIPELINE_STAGES.index(df_row['stage']) if df_row['stage'] in PIPELINE_STAGES else 0)
                try:
                    dd_default = datetime.strptime(df_row.get('due_date') or '', "%Y-%m-%d").date()
                except Exception:
                    dd_default = datetime.now().date()
                new_due = st.date_input("Due date", value=dd_default)
                users = fetch_table("SELECT username FROM users")["username"].tolist()
                new_assigned = st.selectbox("Assigned to", users, index=users.index(df_row.get('assigned_to','')) if df_row.get('assigned_to','') in users else 0)
            if st.form_submit_button("Save changes"):
                update("""UPDATE deals 
                          SET title=?, agency=?, naics=?, value_estimate=?, stage=?, due_date=?, assigned_to=?, updated_at=?
                          WHERE id=?""",
                       (new_title, new_agency, new_naics, float(new_value), new_stage, str(new_due), new_assigned, datetime.utcnow().isoformat(), deal_id))
                log_activity(current_user() or "system", "update", "deal", deal_id)
                st.success("Deal updated")
                st.experimental_rerun()

        st.markdown("**Contacts on this deal**")
        contacts_df = fetch_table("SELECT id, name FROM contacts ORDER BY name ASC")
        current_links = get_deal_contacts(deal_id)["id"].tolist() if not get_deal_contacts(deal_id).empty else []
        selected = st.multiselect("Link contacts", options=contacts_df["id"].tolist(), format_func=lambda i: contacts_df.set_index("id").loc[i,"name"], default=current_links, key=f"contacts_{deal_id}")
        if st.button("Save contacts", key=f"save_contacts_{deal_id}"):
            set_deal_contacts(deal_id, selected)
            log_activity(current_user() or "system", "link_contacts", "deal", deal_id)
            st.success("Contacts updated")
            st.experimental_rerun()

        st.markdown("**Notes**")
        with st.form(f"add_note_{deal_id}", clear_on_submit=True):
            note = st.text_area("Add a note")
            if st.form_submit_button("Add note") and note.strip():
                add_deal_note(deal_id, current_user() or "system", note.strip())
                log_activity(current_user() or "system", "note", "deal", deal_id)
                st.success("Note added")
        notes_df = get_deal_notes(deal_id)
        if not notes_df.empty:
            for _, r in notes_df.iterrows():
                st.write(f"- {r['created_at'][:19]} by {r.get('user','')}  — {r.get('note','')}")

        st.markdown("**Tasks**")
        with st.form(f"add_task_{deal_id}", clear_on_submit=True):
            tcol1, tcol2 = st.columns(2)
            with tcol1:
                task = st.text_input("Task")
            with tcol2:
                due = st.date_input("Due", value=datetime.now().date())
            if st.form_submit_button("Add task") and task.strip():
                add_deal_task(deal_id, task.strip(), str(due))
                log_activity(current_user() or "system", "add_task", "deal", deal_id)
                st.success("Task added")
        tdf = get_deal_tasks(deal_id)
        if not tdf.empty:
            for _, tr in tdf.iterrows():
                left, right = st.columns([3,1])
                with left:
                    st.write(f"- [{tr['status']}] {tr['task']} (due {tr.get('due_date','')})")
                with right:
                    new_status = st.selectbox("Status", ["Open","In Progress","Done"], index=["Open","In Progress","Done"].index(tr["status"] if tr["status"] in ["Open","In Progress","Done"] else "Open"), key=f"tskstat_{tr['id']}")
                    if st.button("Update", key=f"updtsk_{tr['id']}"):
                        update_task_status(int(tr["id"]), new_status)
                        st.success("Task updated")
                        st.experimental_rerun()

def crm_pipeline_ui():
    st.header("CRM and Pipeline")
    ensure_crm_schema()

    with st.expander("New deal"):
        with st.form("deal_form_v2", clear_on_submit=True):
            col1, col2, col3 = st.columns(3)
            with col1:
                title = st.text_input("Deal title")
                agency = st.text_input("Agency")
                naics = st.text_input("NAICS")
            with col2:
                value_estimate = st.number_input("Value estimate", min_value=0.0, step=1000.0)
                stage = st.selectbox("Stage", PIPELINE_STAGES, index=0)
            with col3:
                due_date = st.date_input("Due date")
                assigned_to = st.selectbox("Assigned to", fetch_table("SELECT username FROM users")["username"].tolist())
            submitted = st.form_submit_button("Save deal")
            if submitted and title:
                now = datetime.utcnow().isoformat()
                deal_id = execute("""
                    INSERT INTO deals(title, agency, naics, value_estimate, stage, due_date, assigned_to, created_by, created_at, updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                """, (title, agency, naics, value_estimate, stage, str(due_date), assigned_to, current_user() or "system", now, now))
                log_activity(current_user() or "system", "create", "deal", deal_id)
                st.success(f"Saved deal {title}")

    with st.sidebar:
        st.subheader("Pipeline filters")
        stage_filter = st.selectbox("Stage", ["All"] + PIPELINE_STAGES, key="stage_filter")
        user_filter = st.selectbox("Owner", ["All"] + fetch_table("SELECT username FROM users")["username"].tolist(), key="owner_filter")
        q = st.text_input("Keyword search", key="kw")

    df = fetch_table("SELECT * FROM deals ORDER BY updated_at DESC")
    if stage_filter != "All":
        df = df[df["stage"] == stage_filter]
    if user_filter != "All":
        df = df[df["assigned_to"] == user_filter]
    if q:
        mask = df["title"].str.contains(q, case=False, na=False) | df["agency"].str.contains(q, case=False, na=False) | df["naics"].str.contains(q, case=False, na=False)
        df = df[mask]

    st.subheader("Pipeline board")
    cols = st.columns(len(PIPELINE_STAGES))
    for i, stage in enumerate(PIPELINE_STAGES):
        with cols[i]:
            st.markdown(f"### {stage}")
            col_df = df[df["stage"] == stage]
            if col_df.empty:
                st.caption("No deals")
            else:
                for _, row in col_df.iterrows():
                    with st.container(border=True):
                        _deal_card(row)
                        new_stage = st.selectbox("Move to", PIPELINE_STAGES, index=PIPELINE_STAGES.index(stage), key=f"move_{row['id']}")
                        if new_stage != stage and st.button("Apply", key=f"apply_{row['id']}"):
                            update("UPDATE deals SET stage=?, updated_at=? WHERE id=?", (new_stage, datetime.utcnow().isoformat(), int(row["id"])))
                            log_activity(current_user() or "system", "move_stage", "deal", int(row["id"]))
                            st.experimental_rerun()

################################################################################
# Compliance and Proposal Accuracy  Phase 3
################################################################################

PLACEHOLDER_PATTERN = re.compile(r"INSERT|TBD|LOREM|YOUR COMPANY|YOUR NAME", re.IGNORECASE)

def clean_placeholders(text: str) -> str:
    # Replace common placeholders with empty string safely
    return PLACEHOLDER_PATTERN.sub("", text)

def find_placeholders(text: str) -> List[str]:
    return list(set(m.group(0) for m in PLACEHOLDER_PATTERN.finditer(text)))

def export_proposal_docx_or_txt(filename_base: str, sections: List[Tuple[str,str]]) -> str:
    # First clean and validate
    raw = "\n\n".join([f"{sec}\n{txt}" for sec, txt in sections])
    flagged = find_placeholders(raw)
    if flagged:
        raise ValueError(f"Export blocked. Placeholder text detected: {', '.join(flagged)}")
    # Try DOCX then fallback to TXT
    if DOCX_AVAILABLE:
        doc = Document()
        doc.add_heading("ELA Management Proposal", level=1)
        for sec, txt in sections:
            doc.add_heading(sec, level=2)
            p = doc.add_paragraph(txt)
            p.style.font.size = Pt(11)
        out_path = f"/mnt/data/{filename_base}.docx"
        doc.save(out_path)
        return out_path
    else:
        out_path = f"/mnt/data/{filename_base}.txt"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("ELA Management Proposal\n\n")
            for sec, txt in sections:
                f.write(f"{sec}\n{txt}\n\n")
        return out_path

def compliance_ui():
    st.header("Compliance and Proposal Builder")
    st.caption("Runs SAM status, NAICS match, FAR checklist and blocks export if placeholders remain")

    colA, colB = st.columns(2)
    with colA:
        deal_df = fetch_table("SELECT id, title, agency, naics, stage, due_date, created_by, assigned_to FROM deals ORDER BY updated_at DESC")
        if visibility_scope() == "Mine" and not deal_df.empty:
            u = current_user()
            deal_df = deal_df[(deal_df["created_by"] == u) | (deal_df["assigned_to"] == u)]
        deal_id = st.selectbox("Select deal", deal_df["id"].tolist() if not deal_df.empty else [None])
        if deal_id:
            deal_row = deal_df[deal_df["id"] == deal_id].iloc[0]
            st.write(f"Deal: {deal_row['title']}  Agency: {deal_row['agency']}  NAICS: {deal_row['naics']}")
        st.subheader("Checklist")
        sam_active = st.checkbox("SAM active and current", value=True)
        naics_ok = st.checkbox("NAICS aligned with scope", value=True)
        far_ok = st.checkbox("FAR clauses reviewed", value=True)
        pricing_ok = st.checkbox("Pricing model verified", value=True)

    st.subheader("Draft sections")
    default_sections = {
        "Cover Letter": "ELA Management LLC cover letter to the Government.",
        "Technical Approach": "Describe method, staffing, key personnel, and quality assurance.",
        "Management Plan": "Org chart, roles, and responsibilities.",
        "Past Performance": "Relevant projects and CPARS summaries.",
        "Pricing Narrative": "Basis of estimate and assumptions."
    }
    tabs = st.tabs(list(default_sections.keys()))
    edited_sections = []
    for t, (sec, default) in zip(tabs, default_sections.items()):
        with t:
            text = st.text_area(f"{sec}", default, height=180, key=f"sec_{sec}")
            # live warnings
            flagged = find_placeholders(text)
            if flagged:
                st.warning(f"Remove placeholders found: {', '.join(flagged)}")
            edited_sections.append((sec, text))

    if st.button("Export Proposal"):
        if not all([sam_active, naics_ok, far_ok, pricing_ok]):
            st.error("All compliance checks must be confirmed before export")
        else:
            try:
                out_path = export_proposal_docx_or_txt(
                    f"ELA_Proposal_{deal_id or 'draft'}_{datetime.now().strftime('%Y%m%d_%H%M')}",
                    edited_sections
                )
                st.success(f"Exported proposal to {out_path}")
                execute("INSERT INTO documents(deal_id, doc_type, title, path, created_at) VALUES(?,?,?,?,?)",
                        (deal_id, "proposal", f"Proposal for deal {deal_id}", out_path, datetime.utcnow().isoformat()))
                log_activity(st.session_state.get("user","system"), "export", "document", deal_id or -1)
                st.download_button("Download file", data=open(out_path,"rb").read(),
                                   file_name=os.path.basename(out_path))
            except Exception as e:
                st.error(str(e))

################################################################################
# Networking and Subcontractor Outreach  Phase 4
################################################################################

def partners_ui():
    st.header("Subcontractor and Partner Network")
    st.caption("Store partners, rate performance, send inquiry emails")
    with st.form("new_partner", clear_on_submit=True):
        name = st.text_input("Contact or Company")
        org = st.text_input("Organization")
        role = st.text_input("Role or Capability")
        email = st.text_input("Email")
        phone = st.text_input("Phone")
        ptype = st.selectbox("Type", ["Subcontractor", "Teaming Partner", "Agency POC"])
        rating = st.slider("Rating", 1, 5, 4)
        notes = st.text_area("Notes")
        submitted = st.form_submit_button("Add partner")
        if submitted and name:
            execute("""
                INSERT INTO contacts(name, org, role, email, phone, type, rating, notes, created_at)
                VALUES(?,?,?,?,?,?,?,?,?)
            """, (name, org, role, email, phone, ptype, rating, notes, st.session_state.get("user",""), datetime.utcnow().isoformat()))
            log_activity(st.session_state.get("user","system"), "create", "contact", None)
            st.success(f"Added {name}")
    st.subheader("Directory")
    df = fetch_table("SELECT * FROM contacts ORDER BY created_at DESC")
    st.dataframe(df, use_container_width=True)

    st.subheader("Generate outreach email")
    col1, col2 = st.columns(2)
    with col1:
        proj = st.text_input("Project name")
        scope = st.text_area("Short scope summary")
    with col2:
        naics = st.text_input("Target NAICS")
        due = st.date_input("Response deadline", datetime.now().date() + timedelta(days=7))
    if st.button("Create email draft"):
        body = f"""Subject: Teaming inquiry for {proj}

        Hello,

        We are ELA Management LLC and are seeking a capable partner for {proj}.
        Scope: {scope}
        NAICS: {naics}
        Please reply with capability statement, past performance, and pricing by {due}.

        Thank you,
        ELA Management LLC
        """
        st.code(body, language="text")

################################################################################
# Smart Automation and AI Features  Phase 5
################################################################################

def ai_tools_ui():
    st.header("Smart Automation and AI Tools")
    st.caption("RFP extractor and win probability are placeholders for now")
    uploaded = st.file_uploader("Upload RFP or SOW PDF or DOCX", type=["pdf","docx","txt"])
    if uploaded:
        st.info("In a future update this will auto extract dates, requirements, and evaluation criteria")
        st.write(f"Received file {uploaded.name} with size {uploaded.size} bytes")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Quick Q and A")
        q = st.text_area("Ask about FAR compliance or proposal structure")
        if st.button("Get answer"):
            st.write("This is a placeholder response. In production this would use a secure hosted model.")
    with col2:
        st.subheader("Win probability estimate")
        val = st.number_input("Estimated contract value", 0.0, step=1000.0)
        agency = st.text_input("Agency name")
        past_win = st.slider("Past win rate percent", 0, 100, 20)
        if st.button("Estimate probability"):
            # Very simple heuristic for now
            base = past_win / 100.0
            agency_adj = 0.05 if agency.strip() else 0.0
            size_adj = 0.05 if val <= 250000 else -0.05
            prob = max(0.02, min(0.85, base + agency_adj + size_adj))
            st.metric("Estimated win probability", f"{prob*100:.1f}%")

################################################################################
# Visual Dashboard  Phase 6
################################################################################

def dashboard_ui():
    st.header("Executive Dashboard")
    deals = fetch_table("SELECT * FROM deals")
    total_bids = len(deals)
    wins = len(deals[deals["stage"]=="Awarded"])
    win_rate = (wins / total_bids * 100.0) if total_bids else 0.0
    monthly = deals.copy()
    monthly["month"] = pd.to_datetime(monthly["created_at"]).dt.to_period("M").astype(str) if not monthly.empty else []
    if not monthly.empty:
        m = monthly.groupby("month").size().reset_index(name="bids")
    else:
        m = pd.DataFrame({"month":[], "bids":[]})
    col1, col2, col3 = st.columns(3)
    col1.metric("Total bids", total_bids)
    col2.metric("Awards", wins)
    col3.metric("Win rate", f"{win_rate:.1f}%")
    st.subheader("Bids per month")
    st.bar_chart(m.set_index("month") if not m.empty else pd.DataFrame({"bids":[]}) )

################################################################################
# System Integrations  Phase 7
################################################################################

def integrations_ui():
    st.header("Integrations")
    st.caption("Stubs for SAM, FPDS, USAspending, and GSA eLibrary")
    st.write("These functions are placeholders. They should call official APIs with keys stored in secrets.")
    with st.expander("SAM entity check"):
        uei = st.text_input("Enter UEI")
        if st.button("Verify SAM"):
            st.info("This would query the SAM.gov API to confirm active registration")
    with st.expander("FPDS awards search"):
        vendor = st.text_input("Vendor name for FPDS search")
        if st.button("Search FPDS"):
            st.info("This would pull award history for the vendor")

################################################################################
# Main
################################################################################



def ensure_users_schema():
        conn = get_db()
        cur = conn.cursor()
        # Ensure users table exists
        cur.execute("""CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            full_name TEXT,
            role TEXT NOT NULL,
            email TEXT,
            password TEXT,
            active INTEGER DEFAULT 1
        )""")
        conn.commit()
        # Ensure password column exists
        cur.execute("PRAGMA table_info(users)")
        cols = [r[1] for r in cur.fetchall()]
        if "password" not in cols:
            try:
                cur.execute("ALTER TABLE users ADD COLUMN password TEXT")
                conn.commit()
            except Exception:
                pass
        conn.close()



def seed_admin_users():
        conn = get_db()
        cur = conn.cursor()
        admins = [
            ("quincy", "Quincy", "admin", "quincy@example.com", "change_me", 1),
            ("collin", "Collin", "admin", "collin@example.com", "change_me", 1),
            ("charles", "Charles", "admin", "charles@example.com", "change_me", 1),
        ]
        for u in admins:
            # upsert-like logic
            cur.execute("SELECT id FROM users WHERE username=?", (u[0],))
            row = cur.fetchone()
            if row is None:
                cur.execute("INSERT INTO users(username, full_name, role, email, password, active) VALUES(?,?,?,?,?,?)", u)
            else:
                cur.execute("UPDATE users SET role=?, email=?, active=? WHERE username=?", (u[2], u[3], u[5], u[0]))
        # Remove any legacy 'latrice' if present
        try:
            cur.execute("DELETE FROM users WHERE username=?", ("latrice",))
        except Exception:
            pass
        conn.commit()
        conn.close()



def set_password(username: str, new_pwd: str):
        if not username or not new_pwd:
            return
        ensure_users_schema()
        # Upsert user row then update password
        conn = get_db()
        cur = conn.cursor()
        # Ensure row exists
        cur.execute("INSERT OR IGNORE INTO users(username, role, active) VALUES(?, ?, 1)", (username, "admin"))
        # Update password
        cur.execute("UPDATE users SET password=? WHERE username=?", (new_pwd, username))
        conn.commit()
        conn.close()

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    init_db()
    ensure_crm_schema()
    seed_admin_users()

    if "user" not in st.session_state:
        login_ui()
        st.stop()

    # Main navigation for the 7 phases
    pages = {
        "Dashboard": dashboard_ui,
        "CRM and Pipeline": crm_pipeline_ui,
        "Compliance and Proposal": compliance_ui,
        "Partners": partners_ui,
        "AI Tools": ai_tools_ui,
        "Integrations": integrations_ui
    }
    st.sidebar.selectbox("Data visibility", ["Mine", "All"], index=0, key="scope")
    choice = st.sidebar.radio("Go to", list(pages.keys()))
    pages[choice]()

    # Session controls
    if st.sidebar.button("Sign out"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()

    # Password change
    st.sidebar.markdown("**Account**")
    with st.sidebar.expander("Change password"):
        new_pwd = st.text_input("New password", type="password", key="pwd1")
        confirm_pwd = st.text_input("Confirm password", type="password", key="pwd2")
        if st.button("Update password") and current_user() and new_pwd and confirm_pwd:
            if not new_pwd:
                st.error("Password cannot be empty")
            elif new_pwd != confirm_pwd:
                st.error("Passwords do not match")
        else:
            set_password(current_user(), new_pwd)
            st.success("Password updated")

    # Activity log in sidebar
    st.sidebar.subheader("Activity Log")
    act = fetch_table("SELECT * FROM activities ORDER BY ts DESC LIMIT 50")
    if visibility_scope() == "Mine" and not act.empty:
        act = act[act["user"] == current_user()]
    st.sidebar.dataframe(act, use_container_width=True, height=250)

if __name__ == "__main__":
    main()



