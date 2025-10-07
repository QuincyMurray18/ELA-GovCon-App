def ensure_core_tables():
    try:
        conn = get_db()
    except Exception:
        return
    with conn:
        conn.execute("""create table if not exists attachments(
            id integer primary key,
            session_id text,
            filename text,
            content_text text,
            uploaded_at text default current_timestamp)""")
        conn.execute("""create table if not exists proposal_drafts(
            id integer primary key,
            session_id text,
            section text,
            content text,
            updated_at text default current_timestamp)""")
        conn.execute("""create table if not exists rfp_messages(
            id integer primary key,
            session_id text,
            role text,
            content text,
            created_at text default current_timestamp)""")
try:
    ensure_core_tables()
except Exception:
    pass

# ===== app.py =====
import os, re, io, json, sqlite3, time
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urljoin, urlparse


# ===== Proposal drafts utilities =====
from datetime import datetime
import os, io

def _ensure_drafts_dir():
    base = os.path.join(os.getcwd(), "drafts", "proposals")
    os.makedirs(base, exist_ok=True)
    return base

def save_proposal_draft(title: str, content_md: str) -> str:
    base = _ensure_drafts_dir()
    safe = re.sub(r'[^A-Za-z0-9_.-]+', '_', title.strip() or "untitled")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"{ts}__{safe}.md"
    path = os.path.join(base, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content_md or "")
    return path

def list_proposal_drafts():
    base = _ensure_drafts_dir()
    items = []
    for f in sorted(os.listdir(base)):
        if f.lower().endswith(".md"):
            full = os.path.join(base, f)
            import contextlib
    try:
        size = os.path.getsize(full)
    except Exception:
        size = 0
    items.append({'name': f, 'path': full, 'size': size})
    return list(reversed(items))  # newest first

def load_proposal_draft(path: str) -> str:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception:
        return ''
    st.caption(f"[Control Center diversity note: {_e_div}]")