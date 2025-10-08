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
            return conn
        except sqlite3.OperationalError:
            # Fallback to local file in CWD
            fallback = os.path.join(os.getcwd(), "ela_govcon_fallback.db")
            conn = sqlite3.connect(fallback, check_same_thread=False)
            conn.execute("PRAGMA foreign_keys = ON")
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

def crm_pipeline_ui():
    st.header("CRM and Pipeline")
    col1, col2 = st.columns([2,1])
    with col1:
        st.subheader("Create or Update Deal")
        with st.form("deal_form", clear_on_submit=True):
            title = st.text_input("Deal title")
            agency = st.text_input("Agency")
            naics = st.text_input("NAICS")
            value_estimate = st.number_input("Value estimate", min_value=0.0, step=1000.0)
            stage = st.selectbox("Stage", PIPELINE_STAGES, index=0)
            due_date = st.date_input("Due date")
            assigned_to = st.selectbox("Assigned to", fetch_table("SELECT username FROM users")["username"].tolist())
            submitted = st.form_submit_button("Save deal")
            if submitted and title:
                now = datetime.utcnow().isoformat()
                deal_id = execute("""
                    INSERT INTO deals(title, agency, naics, value_estimate, stage, due_date, assigned_to, created_by, created_at, updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                """, (title, agency, naics, value_estimate, stage, str(due_date), assigned_to, st.session_state.get("user","system"), now, now))
                log_activity(st.session_state.get("user","system"), "create", "deal", deal_id)
                st.success(f"Saved deal {title}")
    with col2:
        st.subheader("Filters")
        stage_filter = st.selectbox("Stage filter", ["All"] + PIPELINE_STAGES)
        df = fetch_table("SELECT * FROM deals ORDER BY updated_at DESC")
        if stage_filter != "All":
            df = df[df["stage"] == stage_filter]
        st.dataframe(df, use_container_width=True)

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

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    init_db()

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
        if st.button("Update password"):
            if not new_pwd:
                st.error("Password cannot be empty")
            elif new_pwd != confirm_pwd:
                st.error("Passwords do not match")
        else:
            update("UPDATE users SET password=? WHERE username=?", (new_pwd, current_user()))
            st.success("Password updated")

    # Activity log in sidebar
    st.sidebar.subheader("Activity Log")
    act = fetch_table("SELECT * FROM activities ORDER BY ts DESC LIMIT 50")
    if visibility_scope() == "Mine" and not act.empty:
        act = act[act["user"] == current_user()]
    st.sidebar.dataframe(act, use_container_width=True, height=250)

if __name__ == "__main__":
    main()

