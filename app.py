
import os
import sqlite3
from contextlib import closing
from typing import Optional, Tuple

import pandas as pd
import streamlit as st


# 1 Config
st.set_page_config(page_title="ELA GovCon Suite", layout="wide")
APP_TITLE = "ELA GovCon Suite"
DB_PATH = os.path.join("data", "govcon.db")
DATA_DIR = "data"
UPLOADS_DIR = os.path.join(DATA_DIR, "uploads")


# 2 Utilities
def ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(UPLOADS_DIR, exist_ok=True)


@st.cache_resource(show_spinner=False)
def get_db() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    with closing(conn.cursor()) as cur:
        cur.execute("PRAGMA foreign_keys = ON;")
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
            value NUMERIC
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS app_settings(
            key TEXT PRIMARY KEY,
            val TEXT
        );
        """)
        conn.commit()
    return conn


def save_uploaded_file(uploaded_file, subdir: str = "") -> Optional[str]:
    """Save Streamlit UploadedFile into data directory and return file path"""
    if not uploaded_file:
        return None
    base_dir = UPLOADS_DIR if not subdir else os.path.join(UPLOADS_DIR, subdir)
    os.makedirs(base_dir, exist_ok=True)
    file_path = os.path.join(base_dir, uploaded_file.name)
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return file_path


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
                    (name.strip(), email.strip(), org.strip())
                )
                conn.commit()
            st.success(f"Added contact {name}")
        except Exception as e:
            st.error(f"Error saving contact {e}")

    try:
        df = pd.read_sql_query("SELECT name, email, org FROM contacts ORDER BY name;", conn)
        st.subheader("Contact List")
        if df.empty:
            st.write("No contacts yet")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Failed to load contacts {e}")


def run_deals(conn: sqlite3.Connection) -> None:
    st.header("Deals")
    st.caption("Simple pipeline placeholder. Add real fields later")
    with st.form("add_deal", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
        with c1:
            title = st.text_input("Title")
        with c2:
            agency = st.text_input("Agency")
        with c3:
            status = st.selectbox("Status", ["New", "Qualifying", "Bidding", "Submitted", "Awarded", "Lost"])
        with c4:
            value = st.number_input("Est Value", min_value=0.0, step=1000.0, format="%.2f")
        submitted = st.form_submit_button("Add Deal")
    if submitted:
        try:
            with closing(conn.cursor()) as cur:
                cur.execute(
                    "INSERT INTO deals(title, agency, status, value) VALUES (?, ?, ?, ?);",
                    (title.strip(), agency.strip(), status, float(value))
                )
                conn.commit()
            st.success(f"Added deal {title}")
        except Exception as e:
            st.error(f"Error saving deal {e}")

    try:
        df = pd.read_sql_query("SELECT title, agency, status, value FROM deals ORDER BY id DESC;", conn)
        st.subheader("Pipeline")
        if df.empty:
            st.write("No deals yet")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Failed to load deals {e}")


def run_sam_watch(conn: sqlite3.Connection) -> None:
    st.header("SAM Watch")
    st.caption("Foundation only. Search and results placeholders. Wire your API logic later")
    with st.expander("Search Filters", expanded=True):
        c1, c2, c3 = st.columns([2, 2, 2])
        with c1:
            keywords = st.text_input("Keywords")
        with c2:
            naics = st.text_input("NAICS")
        with c3:
            psc = st.text_input("PSC")
        c4, c5 = st.columns([2, 2])
        with c4:
            state = st.text_input("State")
        with c5:
            set_aside = st.text_input("Set Aside")
        st.button("Run Search", key="sam_search_btn")
    st.info("Results will render here when wired to SAM API")

    st.subheader("Selected Notice")
    st.write("Choose a notice to push into RFP Analyzer and Deals when you add real results")


def run_rfp_analyzer(conn: sqlite3.Connection) -> None:
    st.header("RFP Analyzer")
    st.caption("Upload RFP docs then parse and extract requirements. Placeholder only")
    up = st.file_uploader("Upload RFP PDF or ZIP", type=["pdf", "zip", "docx"])
    if up:
        path = save_uploaded_file(up, subdir="rfp")
        st.success(f"Saved file to {path}")
        st.info("Implement analyzer logic to extract Sections L and M, SOW, CLINs, dates, and POC fields")


def run_lm_checklist(conn: sqlite3.Connection) -> None:
    st.header("L and M Checklist")
    st.write("Build a dynamic checklist from analyzer output. Placeholder")


def run_past_performance(conn: sqlite3.Connection) -> None:
    st.header("Past Performance and RFQ Generator")
    st.write("Store past performance and generate RFQs. Placeholder")


def run_subcontractor_finder(conn: sqlite3.Connection) -> None:
    st.header("Subcontractor Finder")
    st.write("Seed vendors by NAICS and state. Score responsiveness. Placeholder")


def run_outreach(conn: sqlite3.Connection) -> None:
    st.header("Outreach")
    st.write("Email and message templates with mail merge. Placeholder")


def run_quote_comparison(conn: sqlite3.Connection) -> None:
    st.header("Quote Comparison")
    st.write("Compare vendor quotes and coverage. Placeholder")


def run_pricing_calculator(conn: sqlite3.Connection) -> None:
    st.header("Pricing Calculator")
    st.write("Cost build up, burden, and margin tools. Placeholder")


def run_win_probability(conn: sqlite3.Connection) -> None:
    st.header("Win Probability")
    st.write("Simple scoring model from compliance and fit. Placeholder")


def run_proposal_builder(conn: sqlite3.Connection) -> None:
    st.header("Proposal Builder")
    st.write("Compose sections, insert CLIN tables, enforce page limits. Placeholder")


def run_chat_assistant(conn: sqlite3.Connection) -> None:
    st.header("Chat Assistant")
    st.write("Internal helper for prompts and Q and A. Placeholder")


def run_capability_statement(conn: sqlite3.Connection) -> None:
    st.header("Capability Statement")
    st.write("Generate and export capability statements. Placeholder")


def run_white_paper_builder(conn: sqlite3.Connection) -> None:
    st.header("White Paper Builder")
    st.write("Draft white papers with sources. Placeholder")


# 3 Session and Nav
def init_session() -> None:
    if "initialized" not in st.session_state:
        st.session_state.initialized = True
        st.session_state.active_user = None


def nav() -> str:
    st.sidebar.title("Workspace")
    page = st.sidebar.selectbox(
        "Go to",
        [
            "SAM Watch",
            "RFP Analyzer",
            "L and M Checklist",
            "Past Performance and RFQ Generator",
            "Subcontractor Finder",
            "Outreach",
            "Quote Comparison",
            "Pricing Calculator",
            "Win Probability",
            "Proposal Builder",
            "Chat Assistant",
            "Capability Statement",
            "White Paper Builder",
            "Contacts",
            "Deals",
        ],
    )
    return page


# 4 Router
def router(page: str, conn: sqlite3.Connection) -> None:
    if page == "SAM Watch":
        run_sam_watch(conn)
    elif page == "RFP Analyzer":
        run_rfp_analyzer(conn)
    elif page == "L and M Checklist":
        run_lm_checklist(conn)
    elif page == "Past Performance and RFQ Generator":
        run_past_performance(conn)
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
    elif page == "Proposal Builder":
        run_proposal_builder(conn)
    elif page == "Chat Assistant":
        run_chat_assistant(conn)
    elif page == "Capability Statement":
        run_capability_statement(conn)
    elif page == "White Paper Builder":
        run_white_paper_builder(conn)
    elif page == "Contacts":
        run_contacts(conn)
    elif page == "Deals":
        run_deals(conn)
    else:
        st.error("Unknown page")


# 5 Main
def main() -> None:
    init_session()
    conn = get_db()
    st.title(APP_TITLE)
    st.caption("Clean base. Modular. Reliable. Ready for feature merges")
    page = nav()
    router(page, conn)


if __name__ == "__main__":
    main()
