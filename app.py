# ===== app.py =====    st.session_state.setdefault('deals_refresh', 0)


def _strip_markdown_to_plain(txt: str) -> str:
    """
    Remove common Markdown markers so exported DOCX shows clean text instead of 'coded' look.
    """
    if not txt:
        return ""
    import time as _time
import datetime as _dt
import json
import re as _re
    s = txt
    # Remove code fences but keep inner text
    s = _re.sub(r"```(.*?)```", r"\1", s, flags=_re.DOTALL)
    # Inline code backticks
    s = s.replace("`", "")
    # Bold/italic markers
    s = s.replace("***", "")
    s = s.replace("**", "")
    s = s.replace("*", "")
    s = s.replace("__", "")
    s = s.replace("_", "")
    # Strip heading markers at line starts
    s = _re.sub(r"^[ \t]*#{1,6}[ \t]*", "", s, flags=_re.MULTILINE)
    # Strip blockquote markers
    s = _re.sub(r"^[ \t]*>[ \t]?", "", s, flags=_re.MULTILINE)
    # Remove list markers
    s = _re.sub(r"^[ \t]*([-*•]|\d+\.)[ \t]+", "", s, flags=_re.MULTILINE)
    # Remove table pipes (keep content)
    s = _re.sub(r"^\|", "", s, flags=_re.MULTILINE)
    s = _re.sub(r"\|$", "", s, flags=_re.MULTILINE)
    return s

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
            try:
                size = os.path.getsize(full)
            except Exception:
                size = 0
            items.append({"name": f, "path": full, "size": size})
    return list(reversed(items))  # newest first

def load_proposal_draft(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""

def delete_proposal_draft(path: str) -> bool:
    try:
        os.remove(path)
        return True
    except Exception:
        return False
# ===== end Proposal drafts utilities =====


def md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                     margins_in: float = 1.0, logo_bytes: bytes = None, logo_width_in: float = 1.5) -> bytes:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import io
    doc = Document()
    try:
        md_text = _clean_placeholders(md_text)
    except Exception:
        pass
        pass
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if logo_bytes:
        p_center = doc.add_paragraph(); p_center.paragraph_format.alignment = 1
        run = p_center.add_run()
        try:
            from docx.shared import Inches as _Inches
            run.add_picture(io.BytesIO(logo_bytes), width=_Inches(logo_width_in))
        except Exception:
            pass
    if title:
        h = doc.add_heading(title, level=1)
        try: h.style = doc.styles["Heading 1"]
        except Exception: pass
    _render_markdown_to_docx(doc, md_text)
    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()


def _md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                      margins_in: float = 1.0) -> bytes:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import io
    doc = Document()
    try:
        md_text = _clean_placeholders(md_text)
    except Exception:
        pass
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if title:
        h = doc.add_heading(title, level=1)
        try: h.style = doc.styles["Heading 1"]
        except Exception: pass
    _render_markdown_to_docx(doc, md_text)
    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()


# ===== Improved Markdown rendering helpers =====
def _add_hr_paragraph(doc):
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    p = doc.add_paragraph()
    pPr = p._p.get_or_add_pPr()
    pBdr = OxmlElement('w:pBdr')
    bottom = OxmlElement('w:bottom')
    bottom.set(qn('w:val'), 'single')
    bottom.set(qn('w:sz'), '6')
    bottom.set(qn('w:space'), '1')
    bottom.set(qn('w:color'), 'auto')
    pBdr.append(bottom)
    pPr.append(pBdr)
    return p

def _add_paragraph_with_inlines(doc, text, style=None):
    # Supports **bold**, *italic* inline
    import re as _re
    p = doc.add_paragraph()
    if style:
        try:
            p.style = doc.styles[style]
        except Exception:
            pass

    # Tokenize **bold** and *italic*
    tokens = []
    parts = _re.split(r'(\*\*[^\*]+\*\*)', text or '')
    for part in parts:
        if part.startswith('**') and part.endswith('**') and len(part) >= 4:
            tokens.append(('bold', part[2:-2]))
        else:
            subparts = _re.split(r'(\*[^\*]+\*)', part)
            for sp in subparts:
                if sp.startswith('*') and sp.endswith('*') and len(sp) >= 2:
                    tokens.append(('italic', sp[1:-1]))
                else:
                    tokens.append(('text', sp))

    for kind, chunk in tokens:
        if not chunk:
            continue
        run = p.add_run(chunk)
        if kind == 'bold':
            run.bold = True
        elif kind == 'italic':
            run.italic = True
    return p

def _render_markdown_to_docx(doc, md_text):
    import re as _re
    lines = (md_text or '').splitlines()
    bullet_buf, num_buf = [], []

    def flush_bullets():
        nonlocal bullet_buf
        for item in bullet_buf:
            _add_paragraph_with_inlines(doc, item, style="List Bullet")
        bullet_buf = []

    def flush_numbers():
        nonlocal num_buf
        for item in num_buf:
            _add_paragraph_with_inlines(doc, item, style="List Number")
        num_buf = []

    for raw in lines:
        line = (raw or '').rstrip()

        # Horizontal rule ---
        if _re.match(r'^\s*-{3,}\s*$', line):
            flush_bullets(); flush_numbers()
            _add_hr_paragraph(doc)
            continue

        # Blank -> flush lists and add spacer
        if not line.strip():
            flush_bullets(); flush_numbers()
            doc.add_paragraph("")
            continue

        # Headings (tolerate up to 3 leading spaces)
        m = _re.match(r'^\s{0,3}(#{1,6})\s+(.*)$', line)
        if m:
            flush_bullets(); flush_numbers()
            hashes, text = m.group(1), m.group(2).strip()
            level = min(len(hashes), 6)
            try:
                doc.add_heading(text, level=level)
            except Exception:
                _add_paragraph_with_inlines(doc, text)
            continue

        # Bullets: -, *, •
        if _re.match(r'^\s*(\-|\*|•)\s+', line):
            flush_numbers()
            bullet_buf.append(_re.sub(r'^\s*(\-|\*|•)\s+', '', line, count=1))
            continue

        # Numbered: 1. text
        if _re.match(r'^\s*\d+\.\s+', line):
            flush_bullets()
            num_buf.append(_re.sub(r'^\s*\d+\.\s+', '', line, count=1))
            continue

        # Normal paragraph with inline formatting
        flush_bullets(); flush_numbers()
        _add_paragraph_with_inlines(doc, line)

    flush_bullets(); flush_numbers()


def md_to_docx_bytes_rich(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                          margins_in: float = 1.0, logo_bytes: bytes = None, logo_width_in: float = 1.5) -> bytes:
    """
    Guaranteed rich Markdown→DOCX converter with inline bold/italics, headings, lists, and horizontal rules.
    """
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import io
    doc = Document()
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if logo_bytes:
        p_center = doc.add_paragraph(); p_center.paragraph_format.alignment = 1
        run = p_center.add_run()
        try:
            run.add_picture(io.BytesIO(logo_bytes), width=Inches(logo_width_in))
        except Exception:
            pass
    if title:
        h = doc.add_heading(title, level=1)
        try: h.style = doc.styles["Heading 1"]
        except Exception: pass

    _render_markdown_to_docx(doc, md_text)

    out = io.BytesIO()
    doc.save(out)
    out.seek(0)
    return out.getvalue()

# ===== end Improved Markdown rendering helpers =====


# ===== DOCX helpers (loaded early so they're available to all tabs) =====
def _md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                      margins_in: float = 1.0) -> bytes:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import re as _re, io
    doc = Document()
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if title:
        h = doc.add_heading(title, level=1)
        try:
            h.style = doc.styles["Heading 1"]
        except Exception:
            pass
    lines = (md_text or "").splitlines()
    bullet_buf, num_buf = [], []
    def flush_bullets():
        nonlocal bullet_buf
        for item in bullet_buf:
            p = doc.add_paragraph(item)
            try: p.style = doc.styles["List Bullet"]
            except Exception: pass
        bullet_buf = []
    def flush_numbers():
        nonlocal num_buf
        for item in num_buf:
            p = doc.add_paragraph(item)
            try: p.style = doc.styles["List Number"]
            except Exception: pass
        num_buf = []
    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            flush_bullets(); flush_numbers(); doc.add_paragraph(""); continue
        if line.startswith("### "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[4:].strip(), level=3); continue
        if line.startswith("## "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[3:].strip(), level=2); continue
        if line.startswith("# "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[2:].strip(), level=1); continue
        if _re.match(r"^(\-|\*|•)\s+", line):
            flush_numbers(); bullet_buf.append(_re.sub(r"^(\-|\*|•)\s+", "", line, count=1)); continue
        if _re.match(r"^\d+\.\s+", line):
            flush_bullets(); num_buf.append(_re.sub(r"^\d+\.\s+", "", line, count=1)); continue
        flush_bullets(); flush_numbers(); doc.add_paragraph(line)
    flush_bullets(); flush_numbers()
    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()

def md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                     margins_in: float = 1.0, logo_bytes: bytes = None, logo_width_in: float = 1.5) -> bytes:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import re as _re, io
    doc = Document()
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if logo_bytes:
        p_center = doc.add_paragraph(); p_center.paragraph_format.alignment = 1
        run = p_center.add_run()
        try: run.add_picture(io.BytesIO(logo_bytes), width=Inches(logo_width_in))
        except Exception: pass
    if title:
        h = doc.add_heading(title, level=1)
        try: h.style = doc.styles["Heading 1"]
        except Exception: pass
    lines = (md_text or "").splitlines()
    bullet_buf, num_buf = [], []
    def flush_bullets():
        nonlocal bullet_buf
        for item in bullet_buf:
            p = doc.add_paragraph(item)
            try: p.style = doc.styles["List Bullet"]
            except Exception: pass
        bullet_buf = []
    def flush_numbers():
        nonlocal num_buf
        for item in num_buf:
            p = doc.add_paragraph(item)
            try: p.style = doc.styles["List Number"]
            except Exception: pass
        num_buf = []
    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            flush_bullets(); flush_numbers(); doc.add_paragraph(""); continue
        if line.startswith("### "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[4:].strip(), level=3); continue
        if line.startswith("## "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[3:].strip(), level=2); continue
        if line.startswith("# "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[2:].strip(), level=1); continue
        if _re.match(r"^(\-|\*|•)\s+", line):
            flush_numbers(); bullet_buf.append(_re.sub(r"^(\-|\*|•)\s+", "", line, count=1)); continue
        if _re.match(r"^\d+\.\s+", line):
            flush_bullets(); num_buf.append(_re.sub(r"^\d+\.\s+", "", line, count=1)); continue
        flush_bullets(); flush_numbers(); doc.add_paragraph(line)
    flush_bullets(); flush_numbers()
    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()
# ===== end DOCX helpers =====


import pandas as pd
import numpy as np
import streamlit as st








# === SAFE RERUN HELPER START ===
def _safe_rerun():
    import streamlit as st
    try:
        # Streamlit >= 1.30
        st.rerun()
    except Exception:
        try:
            # Older Streamlit
            _safe_rerun()
        except Exception:
            # As a last resort stop, which triggers a rerun on next interaction
            st.stop()
# === SAFE RERUN HELPER END ===


# === CORE DB EARLY START ===
import os as _os
_os.makedirs('data', exist_ok=True)

@st.cache_resource
def get_db():
    import sqlite3
    conn = sqlite3.connect('data/app.db', check_same_thread=False, isolation_level=None)
    try:
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA synchronous=NORMAL;')
        conn.execute('PRAGMA temp_store=MEMORY;')
        conn.execute('PRAGMA foreign_keys=ON;')
        conn.execute('CREATE TABLE IF NOT EXISTS migrations(id INTEGER PRIMARY KEY, name TEXT UNIQUE, applied_at TEXT NOT NULL);')
    except Exception:
        pass
    return conn
# === CORE DB EARLY END ===

# === TENANCY EARLY BOOTSTRAP START ===
def _tenancy_phase1_bootstrap():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS orgs(
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL
        );""")
        cur.execute("""CREATE TABLE IF NOT EXISTS users(
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
            email TEXT NOT NULL UNIQUE,
            display_name TEXT,
            role TEXT NOT NULL CHECK(role IN('Admin','Member','Viewer')),
            created_at TEXT NOT NULL
        );""")
        # Seed a default org and 3 users if empty
        row = cur.execute("SELECT COUNT(*) FROM orgs").fetchone()
        if row and row[0] == 0:
            cur.execute("INSERT OR IGNORE INTO orgs(id, name, created_at) VALUES(?,?,datetime('now'))", ('org-default','Default Org'))
        # Ensure at least one user exists for the default org
        rowu = cur.execute("SELECT COUNT(*) FROM users").fetchone()
        if rowu and rowu[0] == 0:
            users = [
                ('user-quincy','org-default','quincy@example.com','Quincy','Admin'),
                ('user-collin','org-default','collin@example.com','Collin','Member'),
                ('user-charles','org-default','charles@example.com','Charles','Viewer'),
            ]
            for uid, oid, email, name, role in users:
                cur.execute("INSERT OR IGNORE INTO users(id, org_id, email, display_name, role, created_at) VALUES(?,?,?,?,?,datetime('now'))",
                            (uid, oid, email, name, role))
        conn.commit()
    except Exception as ex:
        # Do not break startup on bootstrap failure
        try: log_json('error', 'tenancy_bootstrap_failed', error=str(ex))
        except Exception: pass

try:
    _tenancy_phase1_bootstrap()
except Exception:
    pass
# === TENANCY EARLY BOOTSTRAP END ===


# === EARLY DB BOOTSTRAP START ===
# Ensure get_db exists before any import-time calls.
# This early definition will be overridden by later phases if they redefine get_db.
import os as _os
_os.makedirs("data", exist_ok=True)

if "get_db" not in globals():
    import streamlit as st
    @st.cache_resource
    def get_db():
        import sqlite3
        conn = sqlite3.connect("data/app.db", check_same_thread=False, isolation_level=None)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("CREATE TABLE IF NOT EXISTS migrations(id INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL);")
        except Exception:
            pass
        return conn
# === EARLY DB BOOTSTRAP END ===


# === PHASE 0 CORE START ===
# Bootstrap: feature flags, API client, SQLite PRAGMAs, secrets loader, structured logging.
import time as _time
import json as _json
import uuid as _uuid
import contextlib as _contextlib
from typing import Any as _Any, Dict as _Dict, Optional as _Optional

import streamlit as st

# ---- Structured logging ----
def log_json(level: str, message: str, **context) -> str:
    """Emit a single line JSON log. Returns error_id for error levels."""
    event_id = str(_uuid.uuid4())
    payload = {
        "ts": int(_time.time()),
        "level": level.upper(),
        "event_id": event_id,
        "message": message,
        "context": {k: v for k, v in context.items()},
    }
    try:
        print(_json.dumps(payload, ensure_ascii=False))
    except Exception:
        # Ensure logging never breaks app
        print(str(payload))
    return event_id if level.lower() in {"error","fatal","critical"} else event_id

# ---- Secrets loader ----
def get_secret(section: str, key: str, default: _Optional[str]=None) -> _Optional[str]:
    """Safe secrets accessor. Does not raise or leak values in logs."""
    try:
        sec = st.secrets.get(section)  # type: ignore[attr-defined]
        if sec is None:
            return default
        val = sec.get(key)
        return val if val is not None else default
    except Exception:
        return default

# ---- Feature flags ----
, "deals_core"]
def init_feature_flags():
    flags = st.session_state.setdefault("feature_flags", {})
    # Do not remove existing keys. Only set missing to False.
    for k in _FEATURE_KEYS:
        flags.setdefault(k, False)
    # Preexisting flags like 'workspace_enabled' preserved as-is.
    return flags

# ---- SQLite PRAGMAs and migrations ----
def _apply_sqlite_pragmas(conn):
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA temp_store=MEMORY;")
        cur.execute("PRAGMA foreign_keys=ON;")
    except Exception as ex:
        log_json("error", "sqlite_pragmas_failed", error=str(ex))

def _ensure_migrations_table(conn):
    try:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS migrations(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            applied_at TEXT NOT NULL
        );""")
    except Exception as ex:
        log_json("error", "migrations_table_create_failed", error=str(ex))

def ensure_bootstrap_db():
    try:
        conn = get_db()  # Provided by later phases. Cached.
        _apply_sqlite_pragmas(conn)
        _ensure_migrations_table(conn)
        return True
    except Exception as ex:
        log_json("error", "bootstrap_db_failed", error=str(ex))
        return False

# ---- Central API client ----
class CircuitOpenError(Exception):
    pass

def create_api_client(base_url: str, api_key: _Optional[str]=None, timeout: int=20, retries: int=3, ttl: int=900):
    """Return a simple client with GET/POST. GET responses cached for 'ttl' seconds."""
    import requests  # local import to avoid hard dependency if unused

    # Circuit breaker state stored in session
    cb = st.session_state.setdefault("_api_cb", {})
    key = f"cb::{base_url}"
    state = cb.setdefault(key, {"fail_count": 0, "opened_at": 0.0})

    def _check_circuit():
        now = _time.time()
        if state["fail_count"] >= 3:
            # Circuit open for 60 seconds from last open
            if now - state["opened_at"] < 60.0:
                raise CircuitOpenError("circuit_open")
            else:
                # half-open: allow a try
                pass

    def _mark_success():
        state["fail_count"] = 0
        state["opened_at"] = 0.0

    def _mark_failure():
        state["fail_count"] += 1
        if state["fail_count"] >= 3:
            state["opened_at"] = _time.time()

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    if api_key:
        session.headers.update({"Authorization": f"Bearer {api_key}"})

    def _backoff(attempt):
        # exponential backoff: 0.25, 0.5, 1, 2 ...
        delay = min(2.0 ** max(0, attempt - 1) * 0.25, 4.0)
        _time.sleep(delay)

    @st.cache_data(ttl=900, show_spinner=False)
    def _cached_get(cache_key: str):
        # cache layer isolated by cache_key
        # Actual HTTP performed outside to pick up dynamic ttl via caller
        return cache_key

    def _http_get(path: str, params: _Optional[_Dict]=None):
        _check_circuit()
        url = base_url.rstrip("/") + "/" + path.lstrip("/")
        # build a deterministic cache key
        key_parts = [url]
        if params:
            # stable sort
            key_parts.extend([f"{k}={params[k]}" for k in sorted(params.keys())])
        cache_key = "|".join(key_parts)
        # read cache token first
        token = _cached_get(cache_key) if ttl else None  # token content unused, just gate by key+ttl
        last_err = None
        for attempt in range(1, max(1, retries) + 1):
            try:
                resp = session.get(url, params=params, timeout=timeout)
                if 200 <= resp.status_code < 300:
                    _mark_success()
                    # store body alongside token by returning it directly
                    return resp.json() if "application/json" in resp.headers.get("Content-Type","") else resp.text
                last_err = f"status={resp.status_code}"
            except CircuitOpenError:
                raise
            except Exception as ex:
                last_err = str(ex)
            _mark_failure()
            if attempt < retries:
                _backoff(attempt)
        # If we got here, open circuit
        _mark_failure()
        state["opened_at"] = _time.time()
        eid = log_json("error", "api_get_failed", url=url, error=last_err)
        raise RuntimeError(f"API GET failed. error_id={eid}")

    def _http_post(path: str, json: _Optional[_Dict]=None):
        _check_circuit()
        url = base_url.rstrip("/") + "/" + path.lstrip("/")
        last_err = None
        for attempt in range(1, max(1, retries) + 1):
            try:
                resp = session.post(url, json=json, timeout=timeout)
                if 200 <= resp.status_code < 300:
                    _mark_success()
                    return resp.json() if "application/json" in resp.headers.get("Content-Type","") else resp.text
                last_err = f"status={resp.status_code}"
            except CircuitOpenError:
                raise
            except Exception as ex:
                last_err = str(ex)
            _mark_failure()
            if attempt < retries:
                _backoff(attempt)
        _mark_failure()
        state["opened_at"] = _time.time()
        eid = log_json("error", "api_post_failed", url=url, error=last_err)
        raise RuntimeError(f"API POST failed. error_id={eid}")

    return {
        "get": _http_get,
        "post": _http_post,
        "base_url": base_url,
        "timeout": timeout,
        "retries": retries,
        "ttl": ttl,
    }

def _ensure_api_factory():
    if "api_client_factory" not in st.session_state:
        st.session_state["api_client_factory"] = create_api_client
    return st.session_state["api_client_factory"]

# ---- Bootstrap runner ----
def _phase0_bootstrap():
    # Initialize feature flags first
    init_feature_flags()
    # Ensure DB PRAGMAs and migrations table
    with _contextlib.suppress(Exception):
        ensure_bootstrap_db()
    # Register API client factory
    _ensure_api_factory()
    st.session_state.setdefault("boot_done", True)

# Run at import time, safe to fail silently
with _contextlib.suppress(Exception):
    _phase0_bootstrap()
# === PHASE 0 CORE END ===

# === LAYOUT PHASE 1 START ===

# Router, query params, shell nav, and feature flags.
# All new code under feature_flags['workspace_enabled'] == False by default.

import contextlib

# Feature flags stored in session_state to persist within a session
def _ensure_feature_flags():
    import streamlit as st
    if "feature_flags" not in st.session_state:
        st.session_state["feature_flags"] = {"workspace_enabled": False}
    # Ensure key exists even if older sessions exist
    if "workspace_enabled" not in st.session_state["feature_flags"]:
        st.session_state["feature_flags"]["workspace_enabled"] = False
    return st.session_state["feature_flags"]

def feature_flags():
    return _ensure_feature_flags()

# Query param helpers with Streamlit compatibility
def _qp_get():
    import streamlit as st
    with contextlib.suppress(Exception):
        # Newer Streamlit
        qp = getattr(st, "query_params", None)
        if qp is not None:
            # st.query_params behaves like a dict[str, str]
            return dict(qp)
    # Fallback to experimental API which returns dict[str, list[str]]
    with contextlib.suppress(Exception):
        data = st.experimental_get_query_params()
        norm = {k: (v[0] if isinstance(v, list) and v else v) for k, v in data.items()}
        return norm
    return {}

def _qp_set(**kwargs):
    import streamlit as st
    # Remove keys with None to avoid clutter
    clean = {k: v for k, v in kwargs.items() if v is not None}
    # Try new API first
    with contextlib.suppress(Exception):
        qp = getattr(st, "query_params", None)
        if qp is not None:
            qp.clear()
            for k, v in clean.items():
                qp[k] = str(v)
            return
    # Fallback
    with contextlib.suppress(Exception):
        st.experimental_set_query_params(**clean)

def get_route():
    import streamlit as st
    qp = _qp_get()
    page = qp.get("page") or "dashboard"
    opp = qp.get("opp")
    tab = qp.get("tab")
    # normalize opp id to int when possible
    try:
        opp_id = int(opp) if opp is not None and str(opp).isdigit() else None
    except Exception:
        opp_id = None
    st.session_state["route_page"] = page
    st.session_state["route_opp_id"] = opp_id
    st.session_state["route_tab"] = tab
    return {"page": page, "opp_id": opp_id, "tab": tab}

def route_to(page, opp_id=None, tab=None, replace=False):
    import streamlit as st
    # Update session state
    st.session_state["route_page"] = page
    st.session_state["route_opp_id"] = opp_id
    st.session_state["route_tab"] = tab
    # Update URL query params
    _qp_set(page=page, opp=(opp_id if opp_id is not None else None), tab=(tab if tab else None))

def _get_notice_title_from_db(opp_id):
    # Best effort lookup. Works even if schema differs.
    # Falls back to "Opportunity <id>"
    if opp_id is None:
        return "Opportunity"
    title = None
    try:
        conn = get_db()  # uses existing cached connection
        cur = conn.cursor()
        # Check candidate tables and columns
        candidates = [
            ("notices", ["title", "notice_title", "name", "subject"]),
            ("opportunities", ["title", "name", "subject"]),
        ]
        for table, cols in candidates:
            # Does the table exist
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,))
            row = cur.fetchone()
            if not row:
                continue
            # Find a valid column
            cur.execute(f"PRAGMA table_info({table})")
            cols_present = [r[1] for r in cur.fetchall()]
            use_col = next((c for c in cols if c in cols_present), None)
            if not use_col:
                continue
            cur.execute(f"SELECT {use_col} FROM {table} WHERE id=?", (opp_id,))
            r = cur.fetchone()
            if r and r[0]:
                title = str(r[0])
                break
    except Exception:
        title = None
    return title or f"Opportunity {opp_id}"

def _render_top_nav():
    import streamlit as st
    ff = feature_flags()
    if not ff.get("workspace_enabled", False):
        return
    pages = [
        ("dashboard", "Dashboard"),
        ("sam", "SAM Watch"),
        ("pipeline", "Pipeline"),
        ("outreach", "Outreach"),
        ("library", "Library"),
        ("admin", "Admin"),
    ]
    st.markdown("### Navigation")
    cols = st.columns(len(pages))
    route = get_route()
    for i, (pid, label) in enumerate(pages):
        with cols[i]:
            if st.button(label, use_container_width=True):
                route_to(pid)

def _render_opportunity_workspace():
    import streamlit as st
    ff = feature_flags()
    if not ff.get("workspace_enabled", False):
        return
    route = get_route()
    if route.get("page") != "opportunity":
        return
    opp_id = route.get("opp_id")
    title = _get_notice_title_from_db(opp_id)
    st.header(title)
    # Subtab bar as segmented control substitute
    tabs = ["overview", "documents", "proposal", "team"]
    labels = ["Overview", "Documents", "Proposal", "Team"]
    current = route.get("tab") or "overview"
    # Ensure valid
    if current not in tabs:
        current = "overview"
    idx = tabs.index(current)
    try:
        selected = st.radio("Workspace", options=list(range(len(tabs))), index=idx, format_func=lambda i: labels[i], horizontal=True)
    except TypeError:
        # Streamlit < 1.29 does not have horizontal
        selected = st.radio("Workspace", options=list(range(len(tabs))), index=idx, format_func=lambda i: labels[i])
    if tabs[selected] != current:
        route_to("opportunity", opp_id=opp_id, tab=tabs[selected])
        st.stop()
    # Empty placeholder sections
    st.info("Workspace enabled. Placeholder only.")

def _maybe_render_shell():
    import streamlit as st
    ff = feature_flags()
    if not ff.get("workspace_enabled", False):
        return
    _render_top_nav()
    _render_opportunity_workspace()
    # Try to dispatch to known page renderers without removing existing UI
    route = get_route()
    page = route.get("page")
    dispatch = {
        "dashboard": "render_dashboard",
        "sam": "render_sam_watch",
        "pipeline": "render_pipeline",
        "outreach": "render_outreach",
        "library": "render_library",
        "admin": "render_admin",
    }
    func_name = dispatch.get(page)
    if func_name and func_name in globals() and callable(globals()[func_name]):
        try:
            globals()[func_name]()
        except Exception as ex:
            st.warning(f"Navigation handler error: {ex}")

# Initialize routing state on import
try:
    _ensure_feature_flags()
    get_route()
except Exception:
    pass

# Hook shell after Streamlit lays out base content
try:
    _maybe_render_shell()
except Exception:
    pass

# === LAYOUT PHASE 1 END ===

# === LAYOUT PHASE 2 START ===
# Subtabbed opportunity workspace with lazy loading and deep links.
# Keeps existing app tabs intact. Controlled by feature_flags['workspace_enabled'].
import contextlib
import datetime
import re

def _ensure_route_state_defaults():
    import streamlit as st
    st.session_state.setdefault('route_page', 'dashboard')
    st.session_state.setdefault('route_opp_id', None)
    st.session_state.setdefault('route_tab', None)
    st.session_state.setdefault('active_opportunity_tab', None)

def _get_notice_meta_from_db(opp_id):
    """Return minimal metadata for header: title, agency, due_date, set_aside list."""
    meta = {'title': None, 'agency': None, 'due_date': None, 'set_asides': []}
    if opp_id is None:
        meta['title'] = 'Opportunity'
        return meta
    try:
        conn = get_db()
        cur = conn.cursor()
        table_candidates = [
            ('notices', {
                'title': ['title','notice_title','name','subject'],
                'agency': ['agency','agency_name','buyer','office'],
                'due':   ['due_date','response_due','close_date','offer_due'],
                'set':   ['set_aside','setaside','set_asides','naics_set_aside']
            }),
            ('opportunities', {
                'title': ['title','name','subject'],
                'agency': ['agency','buyer','office'],
                'due':   ['due_date','close_date'],
                'set':   ['set_aside','setasides']
            })
        ]
        for table, cols in table_candidates:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,))
            if not cur.fetchone():
                continue
            cur.execute("PRAGMA table_info(%s)" % table)
            present = {r[1] for r in cur.fetchall()}
            def pick(keys):
                for k in keys:
                    if k in present:
                        return k
                metric_push('parser_time_ms', (_time.perf_counter()-_pt0)*1000.0, {'result':'none'}); return None
            c_title = pick(cols['title'])
            c_agency = pick(cols['agency'])
            c_due = pick(cols['due'])
            c_set = pick(cols['set'])
            sel_cols = [c for c in [c_title, c_agency, c_due, c_set] if c]
            if not sel_cols:
                continue
            cur.execute("SELECT %s FROM %s WHERE id=?" % (", ".join(sel_cols), table), (opp_id,))
            row = cur.fetchone()
            if row:
                idx = 0
                if c_title:
                    meta['title'] = row[idx]; idx += 1
                if c_agency:
                    meta['agency'] = row[idx]; idx += 1
                if c_due:
                    meta['due_date'] = row[idx]; idx += 1
                if c_set:
                    raw = row[idx]
                    if isinstance(raw, str):
                        parts = [p.strip() for p in re.split(r"[;,/|]", raw) if p.strip()]
                    elif isinstance(raw, (list, tuple)):
                        parts = list(raw)
                    else:
                        parts = []
                    meta['set_asides'] = parts
                break
    except Exception:
        pass
    if not meta['title']:
        meta['title'] = 'Opportunity %s' % opp_id
    return meta

try:
    import streamlit as st
except Exception:
    class _Stub:
        def cache_data(self, **kw):
            def deco(fn): return fn
            return deco
    st = _Stub()

@st.cache_data(ttl=900)
def _load_analyzer_data(opp_id):
    return {'ready': True, 'opp_id': opp_id}

@st.cache_data(ttl=900)
def _load_compliance_data(opp_id):
    return {'ready': True, 'opp_id': opp_id}

@st.cache_data(ttl=900)
def _load_pricing_data(opp_id):
    return {'ready': True, 'opp_id': opp_id}

@st.cache_data(ttl=900)
def _load_vendors_data(opp_id):
    return {'ready': True, 'opp_id': opp_id}

@st.cache_data(ttl=900)
def _load_submission_data(opp_id):
    return {'ready': True, 'opp_id': opp_id}

def render_details(opp_id):
    import streamlit as st
    st.subheader('Details')
    st.write('Opportunity ID:', opp_id)

def render_analyzer(opp_id):
    import streamlit as st
    st.subheader('Analyzer')
    data = _load_analyzer_data(opp_id)
    st.write(data)
    try:
        analyzer_lm_readonly(int(opp_id))
    except Exception:
        pass
    try:
        if feature_flags().get('rtm', False):
            st.markdown('---')
            st.subheader('RTM')
            render_rtm_tab(int(opp_id))
    except Exception:
        pass



def render_compliance(opp_id):
    import streamlit as st
    st.subheader('Compliance')
    data = _load_compliance_data(opp_id)
    st.write(data)

def render_proposal(opp_id):
    import streamlit as st
    st.subheader('Proposal')
    st.write({'opp_id': opp_id})

def render_pricing(opp_id):
    import streamlit as st
    st.subheader('Pricing')
    data = _load_pricing_data(opp_id)
    st.write(data)

def render_vendorsrfq(opp_id):
    import streamlit as st
    st.subheader('Vendors RFQ')
    data = _load_vendors_data(opp_id)
    st.write(data)

def render_submission(opp_id):
    import streamlit as st
    st.subheader('Submission')
    data = _load_submission_data(opp_id)
    st.write(data)

def open_details(opp_id):
    route_to('opportunity', opp_id=opp_id, tab='details')

def open_analyzer(opp_id):
    route_to('opportunity', opp_id=opp_id, tab='analyzer')

def open_compliance(opp_id):
    route_to('opportunity', opp_id=opp_id, tab='compliance')

def open_pricing(opp_id):
    route_to('opportunity', opp_id=opp_id, tab='pricing')

def open_vendors(opp_id):
    route_to('opportunity', opp_id=opp_id, tab='vendors')

def open_submission(opp_id):
    route_to('opportunity', opp_id=opp_id, tab='submission')

def _render_badges(set_asides):
    import streamlit as st
    if not set_asides:
        return
    cols = st.columns(min(5, len(set_asides)))
    for i, item in enumerate(set_asides[:5]):
        with cols[i]:
            st.caption(f'Set-aside: {item}')

def _render_opportunity_workspace():
    import streamlit as st
    ff = feature_flags()
    if not ff.get('workspace_enabled', False):
        return
    route = get_route()
    if route.get('page') != 'opportunity':
        return
    _ensure_route_state_defaults()
    opp_id = route.get('opp_id')
    meta = _get_notice_meta_from_db(opp_id)
    st.header(str(meta.get('title', '')))
    top_cols = st.columns([2,1,1])
    with top_cols[0]:
        st.caption(str(meta.get('agency') or ''))
    with top_cols[1]:
        due = meta.get('due_date')
        if due:
            st.caption(f'Due: {due}')
    with top_cols[2]:
        _render_badges(meta.get('set_asides') or [])
    tabs = ['details','analyzer','compliance','proposal','pricing','vendors','submission']
    labels = ['Details','Analyzer','Compliance','Proposal','Pricing','VendorsRFQ','Submission']
    current = route.get('tab') or st.session_state.get('active_opportunity_tab') or 'details'
    if current not in tabs:
        current = 'details'
    idx = tabs.index(current)
    try:
        sel = st.radio('Workspace', options=list(range(len(tabs))), index=idx, format_func=lambda i: labels[i], horizontal=True)
    except TypeError:
        sel = st.radio('Workspace', options=list(range(len(tabs))), index=idx, format_func=lambda i: labels[i])
    new_tab = tabs[sel]
    if new_tab != current:
        st.session_state['active_opportunity_tab'] = new_tab
        route_to('opportunity', opp_id=opp_id, tab=new_tab)
        st.stop()
    else:
        st.session_state['active_opportunity_tab'] = current
    if current == 'details':
        render_details(opp_id)
    elif current == 'analyzer':
        render_analyzer(opp_id)
    elif current == 'compliance':
        render_compliance(opp_id)
    elif current == 'proposal':
        render_proposal(opp_id)
    elif current == 'pricing':
        render_pricing(opp_id)
    elif current == 'vendors':
        render_vendorsrfq(opp_id)
    elif current == 'submission':
        render_submission(opp_id)
# === LAYOUT PHASE 2 END ===




# === Outreach Email (per-user) helpers ===
import smtplib, base64
from email.message import EmailMessage

USER_EMAILS = {
    "Quincy": "quincy.elamgmt@gmail.com",
    "Charles": "charles.elamgmt@gmail.com",
    "Collin": "collin.elamgmt@gmail.com",
}

def _mail_store_path():
    base = os.path.join(os.getcwd(), "secure_auth")
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "mail.json")

def _load_mail_store():
    try:
        with open(_mail_store_path(), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_mail_store(store: dict):
    with open(_mail_store_path(), "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2)

def set_user_smtp_app_password(user: str, app_password: str):
    store = _load_mail_store()
    u = store.get(user, {})
    u["smtp_host"] = "smtp.gmail.com"
    u["smtp_port"] = 587
    u["username"] = USER_EMAILS.get(user, "")
    u["app_password_b64"] = base64.b64encode((app_password or "").encode("utf-8")).decode("ascii")
    store[user] = u
    _save_mail_store(store)

def get_user_mail_config(user: str):
    store = _load_mail_store()
    rec = store.get(user, {})
    if not rec:
        return None
    pw = base64.b64decode(rec.get("app_password_b64", "").encode("ascii")).decode("utf-8") if rec.get("app_password_b64") else ""
    return {
        "smtp_host": rec.get("smtp_host", "smtp.gmail.com"),
        "smtp_port": rec.get("smtp_port", 587),
        "username": rec.get("username", ""),
        "password": pw,
        "from_addr": USER_EMAILS.get(user, rec.get("username", "")),
    }

def send_outreach_email(user: str, to_addrs, subject: str, body_html: str, cc_addrs=None, bcc_addrs=None, attachments=None, add_read_receipts=False, tracking_pixel_url=None, tracking_id=None):
    cfg = get_user_mail_config(user)
    if not cfg or not cfg.get("username") or not cfg.get("password"):
        raise RuntimeError(f"No email credentials configured for {user}. Set a Gmail App Password in the sidebar.")

    msg = EmailMessage()
    msg["Subject"] = subject or ""
    msg["From"] = cfg["from_addr"]

    def _split(a):
        if not a:
            return []
        if isinstance(a, list):
            return a
        return [x.strip() for x in str(a).replace(";", ",").split(",") if x.strip()]

    to_list = _split(to_addrs)
    cc_list = _split(cc_addrs)
    bcc_list = _split(bcc_addrs)
    if not to_list:
        raise RuntimeError("Please provide at least one recipient in To.")

    msg["To"] = ", ".join(to_list)
    if cc_list: msg["Cc"] = ", ".join(cc_list)

    import re as _re
    plain = _re.sub("<[^<]+?>", "", body_html or "") if body_html else ""
    msg.set_content(plain or "(no content)")
    if body_html:
        msg.add_alternative(body_html, subtype="html")

    # Optional read receipts
    if add_read_receipts:
        # These headers work only if recipient mail server honors them
        msg["Disposition-Notification-To"] = cfg["from_addr"]
        msg["Return-Receipt-To"] = cfg["from_addr"]

    # Optional tracking pixel
    if tracking_pixel_url and body_html:
        try:
            import uuid, urllib.parse as _u
            tid = tracking_id or str(uuid.uuid4())
            qp = {"id": tid, "to": ",".join(to_list)}
            pixel = f'<img src="{tracking_pixel_url}?'+r'{'+'}'.replace('{','')+r'}" width="1" height="1" style="display:none;" />'.replace("{"+"}", "{_u.urlencode(qp)}")
            body_html = (body_html or "") + pixel
            # Replace the last HTML alternative with updated body_html
            msg.clear_content()
            plain = _re.sub("<[^<]+?>", "", body_html or "") if body_html else ""
            msg.set_content(plain or "(no content)")
            msg.add_alternative(body_html, subtype="html")
        except Exception:
            pass



    attachments = attachments or []
    for att in attachments:
        try:
            filename = getattr(att, "name", None)
            content = None

            # Streamlit UploadedFile or file-like object with getvalue or read
            if hasattr(att, "getvalue"):
                content = att.getvalue()
            elif hasattr(att, "read"):
                try:
                    att.seek(0)
                except Exception:
                    pass
                content = att.read()
            # Dict form: {"name": ..., "data": bytes} or {"path": ...}
            elif isinstance(att, dict):
                filename = att.get("name", filename or "file")
                if "data" in att and att["data"] is not None:
                    content = att["data"]
                elif "content" in att and att["content"] is not None:
                    val = att["content"]
                    content = val.getvalue() if hasattr(val, "getvalue") else (val.read() if hasattr(val, "read") else val)
                elif "path" in att:
                    import os
                    path = att["path"]
                    with open(path, "rb") as f:
                        content = f.read()
                    if not filename:
                        filename = os.path.basename(path)
            # Raw bytes
            elif isinstance(att, (bytes, bytearray)):
                content = bytes(att)
            # String path
            elif isinstance(att, str):
                import os
                if os.path.exists(att):
                    with open(att, "rb") as f:
                        content = f.read()
                    if not filename:
                        filename = os.path.basename(att)

            if content is None:
                raise ValueError("Unsupported attachment type")

            if not filename:
                filename = "attachment.bin"

            msg.add_attachment(content, maintype="application", subtype="octet-stream", filename=filename)
        except Exception as e:
            raise RuntimeError(f"Failed to attach {getattr(att,'name', getattr(att,'path', 'file'))}: {e}")

    all_rcpts = to_list + cc_list + bcc_list

    with metric_timer('email_send_ms', {'fn':'send_outreach_email'}):
    with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"]) as server:
        server.ehlo()
        server.starttls()
        server.login(cfg["username"], cfg["password"])
        server.send_message(msg, from_addr=cfg["from_addr"], to_addrs=all_rcpts)
        metric_push('email_success', 1, {'to': str(len(all_rcpts))})

def outreach_send_from_active_user(to, subject, body_html, cc=None, bcc=None, attachments=None):
    # ACTIVE_USER provided by your sign-in block
    return send_outreach_email(ACTIVE_USER, to, subject, body_html, cc_addrs=cc, bcc_addrs=bcc, attachments=attachments)
# === End Outreach helpers ===


# === Multi-user Sign-in & Session Isolation (added by ChatGPT on 2025-10-08) ===
from functools import wraps
import uuid

# Configure your users here
USERS = ["Quincy", "Charles", "Collin"]
# Optional PINs. Leave empty {} if you want passwordless sign-in.

PINS = {"Quincy": "1111", "Charles": "2222", "Collin": "3333"}

# --- Persistent PIN store (salted) ---
import json, os, secrets, hashlib

def _pin_storage_path():
    base = os.path.join(os.getcwd(), "secure_auth")
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "pins.json")

def _load_pin_store():
    path = _pin_storage_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_pin_store(store: dict):
    path = _pin_storage_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2)

def _hash_pin(pin: str, salt: str) -> str:
    return hashlib.sha256((salt + "|" + (pin or "")).encode("utf-8")).hexdigest()

def _get_or_init_pin_store():
    store = _load_pin_store()
    # Seed from PINS dict on first run for the defined USERS
    changed = False
    for u in USERS:
        if u not in store:
            salt = secrets.token_hex(16)
            store[u] = {"salt": salt, "hash": _hash_pin(PINS.get(u, ""), salt)}
            changed = True
    if changed:
        _save_pin_store(store)
    return store

def _verify_pin(user: str, pin: str) -> bool:
    store = _get_or_init_pin_store()
    rec = store.get(user)
    if not rec:
        return False
    return _hash_pin(pin or "", rec["salt"]) == rec["hash"]

def set_user_pin(user: str, new_pin: str):
    store = _get_or_init_pin_store()
    salt = secrets.token_hex(16)
    store[user] = {"salt": salt, "hash": _hash_pin(new_pin or "", salt)}
    _save_pin_store(store)

def _do_login():
    with st.sidebar:
        st.header("Sign in")
        user = st.selectbox("User", USERS, index=0, key="login_user_select")
        pin_ok = True
        if PINS:
            pin = st.text_input("PIN", type="password", key="login_pin_input")
            pin_ok = _verify_pin(user, pin)

        if st.button("Sign in", use_container_width=True, key="login_btn"):
            if pin_ok:
                st.session_state["active_user"] = user
                # Resolve identity into users table and set session ids
                try:
                    conn = get_db()
                    row = conn.execute("SELECT id, org_id, role FROM users WHERE display_name=?", (user,)).fetchone()
                    if row:
                        st.session_state["user_id"] = row[0]
                        st.session_state["org_id"] = row[1]
                        st.session_state["role"] = row[2]
                    else:
                        # fallback create if missing
                        oid = "org-ela"
                        conn.execute("INSERT OR IGNORE INTO orgs(id,name,created_at) VALUES(?,?,datetime('now'))", (oid, "ELA Management LLC"))
                        uid = f"u-{user.lower()}"
                        conn.execute("INSERT OR IGNORE INTO users(id,org_id,email,display_name,role,created_at) VALUES(?,?,?,?,?,datetime('now'))",
                                     (uid, oid, f"{user.lower()}@ela.local", user, "Member"))
                        st.session_state["user_id"] = uid
                        st.session_state["org_id"] = oid
                        st.session_state["role"] = "Member"
                    st.session_state.setdefault("private_mode", True)
                except Exception as _ex:
                    st.warning(f"Login identity init issue: {_ex}")
                # Resolve identity into users table and set session ids
                try:
                    conn = get_db()
                    row = conn.execute("SELECT id, org_id, role FROM users WHERE display_name=?", (user,)).fetchone()
                    if row:
                        st.session_state["user_id"] = row[0]
                        st.session_state["org_id"] = row[1]
                        st.session_state["role"] = row[2]
                    else:
                        # Fallback create user mapped to default org
                        cur = conn.execute("SELECT id FROM orgs ORDER BY created_at LIMIT 1").fetchone()
                        oid = cur[0] if cur else "org-ela"
                        uid = f"u-{user.lower()}"
                        conn.execute("INSERT OR IGNORE INTO users(id,org_id,email,display_name,role,created_at) VALUES(?,?,?,?,?,datetime('now'))",
                                     (uid, oid, f"{user.lower()}@ela.local", user, 'Member'))
                        st.session_state["user_id"] = uid
                        st.session_state["org_id"] = oid
                        st.session_state["role"] = 'Member'
                except Exception:
                    pass
                st.session_state.setdefault("private_mode", True)
                st.success(f"Signed in as {user}")
            else:
                st.error("Incorrect PIN")

    if "active_user" not in st.session_state:
        st.stop()

_do_login()
ACTIVE_USER = st.session_state["active_user"]

if not st.session_state.get("org_id") or not st.session_state.get("user_id"):
    # Try resolve from active_user
    try:
        conn = get_db()
        name = st.session_state.get("active_user")
        if name:
            r = conn.execute("SELECT id, org_id FROM users WHERE display_name=?", (name,)).fetchone()
            if r:
                st.session_state["user_id"], st.session_state["org_id"] = r[0], r[1]
    except Exception:
        pass
if not st.session_state.get("org_id"):
    st.error("No organization set. Sign in again.")
    st.stop()


# --- Post-login controls: Sign out and Switch user ---
with st.sidebar:
    # Show current user and offer Sign out
    if st.session_state.get("active_user"):
        st.caption(f"Signed in as {st.session_state['active_user']}")
        if st.button("Sign out", use_container_width=True, key="logout_btn"):
            # Clear login and PIN and force re-run back to login screen
            st.session_state.pop("active_user", None)
            st.session_state.pop("login_pin_input", None)
            st.rerun()

# If the selection differs from the active user, offer a quick switch
_selected = st.session_state.get("login_user_select")
_active = st.session_state.get("active_user")
if _active and _selected and _selected != _active:
    with st.sidebar:
        st.warning(f"You selected {_selected}. To switch from {_active}, click below then sign in.")
        if st.button(f"Switch to {_selected}", use_container_width=True, key="switch_user_btn"):
            st.session_state.pop("active_user", None)  # this will trigger the login stop above on next run
            st.session_state.pop("login_pin_input", None)
            st.rerun()


# --- Namespaced session state helpers ---

# --- Unified Streamlit key helper (namespaced + duplicate-safe) ---
try:
    _NS_KEY_COUNTS
except NameError:
    _NS_KEY_COUNTS = {}
def ns_key(key: str) -> str:
    base = f"{ACTIVE_USER}::{key}"
    # increment and deduplicate within a single run
    c = _NS_KEY_COUNTS.get(base, 0) + 1
    _NS_KEY_COUNTS[base] = c
    if c == 1:
        return base
    return f"{base}__dup{c}"


class SessionNS:
    def __init__(self, user: str):
        self.user = user

    def _k(self, key: str) -> str:
        return f"{self.user}::{key}"

    def __getitem__(self, key: str):
        return st.session_state.get(self._k(key))

    def __setitem__(self, key: str, value):
        st.session_state[self._k(key)] = value

    def get(self, key: str, default=None):
        return st.session_state.get(self._k(key), default)

    def setdefault(self, key: str, default):
        return st.session_state.setdefault(self._k(key), default)

    def pop(self, key: str, default=None):
        return st.session_state.pop(self._k(key), default)

NS = SessionNS(ACTIVE_USER)

# --- Private workspace & publish queue ---
with st.sidebar:
    st.subheader("Workspace")

with st.sidebar:
    st.subheader("Security")
    with st.expander("Change My PIN", expanded=False):
        st.write("Update your sign-in PIN. New PIN must be 4–12 characters.")
        curr = st.text_input("Current PIN", type="password", key="pin_cur")
        new1 = st.text_input("New PIN", type="password", key="pin_new1")
        new2 = st.text_input("Confirm New PIN", type="password", key="pin_new2")
        if st.button("Update PIN", use_container_width=True, key="pin_update_btn"):
            if not _verify_pin(ACTIVE_USER, curr or ''):
                st.error("Current PIN is incorrect.")
            elif not new1 or len(new1) < 4 or len(new1) > 12:
                st.error("New PIN must be 4–12 characters.")
            elif new1 != new2:
                st.error("New PINs do not match.")
            else:
                set_user_pin(ACTIVE_USER, new1)
                # Clear any cached login input
                st.session_state.pop("login_pin_input", None)
                st.success("Your PIN has been updated. It will be required next time you sign in.")

    st.session_state.setdefault(f"{ACTIVE_USER}::private_mode", True)
    NS["private_mode"] = st.toggle(
        "Private mode",
        value=NS.get("private_mode", True),
        help="When ON your changes stay private to you until you publish."
    )

def queue_change(fn, *, label: str):
    """Queue a change for this user instead of writing to shared data immediately."""
    NS.setdefault("publish_queue", [])
    q = NS.get("publish_queue", [])
    q.append({"id": str(uuid.uuid4()), "label": label, "fn": fn})
    NS["publish_queue"] = q

def publish_changes():
    q = NS.get("publish_queue", [])
    errors = []
    for item in q:
        try:
            item["fn"]()
        except Exception as e:
            errors.append((item["label"], e))
    NS["publish_queue"] = []
    return errors

def write_or_queue(label, commit_fn):
    if NS.get("private_mode", True):
        queue_change(commit_fn, label=label)
        st.info(f"Saved privately. Publish later. [{label}]")
    else:
        commit_fn()
        st.success(f"Saved to team. [{label}]")

with st.sidebar:
    if st.button("Publish my changes", use_container_width=True, key="publish_btn"):
        errs = publish_changes()
        if not errs:
            st.success("All your private changes are now published to the team data.")
        else:
            st.error("Some changes failed to publish. See below for details.")
            for label, e in errs:
                st.exception(RuntimeError(f"{label}: {e}"))

# === End multi-user block ===

# === Outreach Email (per-user) — Gmail SMTP (added 2025-10-08) ===
# Supports per-user "From" emails, stored credentials, and a sidebar composer.
import smtplib
from email.message import EmailMessage
import base64

# Map users to their From addresses
USER_EMAILS = {
    "Quincy": "quincy.elamgmt@gmail.com",
    "Charles": "charles.elamgmt@gmail.com",
    "Collin": "collin.elamgmt@gmail.com",
}

def _mail_store_path():
    base = os.path.join(os.getcwd(), "secure_auth")
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "mail.json")

def _load_mail_store():
    path = _mail_store_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_mail_store(store: dict):
    path = _mail_store_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2)

def set_user_smtp_app_password(user: str, app_password: str):
    store = _load_mail_store()
    u = store.get(user, {})
    # Light obfuscation (not true encryption) — recommend using Gmail App Passwords
    u["smtp_host"] = "smtp.gmail.com"
    u["smtp_port"] = 587
    u["username"] = USER_EMAILS.get(user, "")
    u["app_password_b64"] = base64.b64encode((app_password or "").encode("utf-8")).decode("ascii")
    store[user] = u
    _save_mail_store(store)

def get_user_mail_config(user: str):
    store = _load_mail_store()
    rec = store.get(user, {})
    if not rec:
        return None
    pw = base64.b64decode(rec.get("app_password_b64", "").encode("ascii")).decode("utf-8") if rec.get("app_password_b64") else ""
    return {
        "smtp_host": rec.get("smtp_host", "smtp.gmail.com"),
        "smtp_port": rec.get("smtp_port", 587),
        "username": rec.get("username", ""),
        "password": pw,
        "from_addr": USER_EMAILS.get(user, rec.get("username", "")),
    }

def send_outreach_email(user: str, to_addrs, subject: str, body_html: str, cc_addrs=None, bcc_addrs=None, attachments=None, add_read_receipts=False, tracking_pixel_url=None, tracking_id=None):
    cfg = get_user_mail_config(user)
    if not cfg or not cfg.get("username") or not cfg.get("password"):
        raise RuntimeError(f"No email credentials configured for {user}. Set a Gmail App Password in the sidebar.")

    msg = EmailMessage()
    msg["Subject"] = subject or ""
    msg["From"] = cfg["from_addr"]
    # Parse address lists
    def _split(a):
        if not a:
            return []
        if isinstance(a, list):
            return a
        return [x.strip() for x in str(a).replace(";", ",").split(",") if x.strip()]

    to_list = _split(to_addrs)
    cc_list = _split(cc_addrs)
    bcc_list = _split(bcc_addrs)
    if not to_list:
        raise RuntimeError("Please provide at least one recipient in To.")

    msg["To"] = ", ".join(to_list)
    if cc_list: msg["Cc"] = ", ".join(cc_list)

    # HTML body; also set a plain text fallback
    from html import unescape
    plain = re.sub("<[^<]+?>", "", body_html or "") if body_html else ""
    msg.set_content(plain or "(no content)")
    if body_html:
        msg.add_alternative(body_html, subtype="html")

    # Optional read receipts
    if add_read_receipts:
        # These headers work only if recipient mail server honors them
        msg["Disposition-Notification-To"] = cfg["from_addr"]
        msg["Return-Receipt-To"] = cfg["from_addr"]

    # Optional tracking pixel
    if tracking_pixel_url and body_html:
        try:
            import uuid, urllib.parse as _u
            tid = tracking_id or str(uuid.uuid4())
            qp = {"id": tid, "to": ",".join(to_list)}
            pixel = f'<img src="{tracking_pixel_url}?'+r'{'+'}'.replace('{','')+r'}" width="1" height="1" style="display:none;" />'.replace("{"+"}", "{_u.urlencode(qp)}")
            body_html = (body_html or "") + pixel
            # Replace the last HTML alternative with updated body_html
            msg.clear_content()
            plain = _re.sub("<[^<]+?>", "", body_html or "") if body_html else ""
            msg.set_content(plain or "(no content)")
            msg.add_alternative(body_html, subtype="html")
        except Exception:
            pass


    # Attachments

    attachments = attachments or []
    for att in attachments:
        try:
            filename = getattr(att, "name", None)
            content = None

            # Streamlit UploadedFile or file-like object with getvalue or read
            if hasattr(att, "getvalue"):
                content = att.getvalue()
            elif hasattr(att, "read"):
                try:
                    att.seek(0)
                except Exception:
                    pass
                content = att.read()
            # Dict form: {"name": ..., "data": bytes} or {"path": ...}
            elif isinstance(att, dict):
                filename = att.get("name", filename or "file")
                if "data" in att and att["data"] is not None:
                    content = att["data"]
                elif "content" in att and att["content"] is not None:
                    val = att["content"]
                    content = val.getvalue() if hasattr(val, "getvalue") else (val.read() if hasattr(val, "read") else val)
                elif "path" in att:
                    import os
                    path = att["path"]
                    with open(path, "rb") as f:
                        content = f.read()
                    if not filename:
                        filename = os.path.basename(path)
            # Raw bytes
            elif isinstance(att, (bytes, bytearray)):
                content = bytes(att)
            # String path
            elif isinstance(att, str):
                import os
                if os.path.exists(att):
                    with open(att, "rb") as f:
                        content = f.read()
                    if not filename:
                        filename = os.path.basename(att)

            if content is None:
                raise ValueError("Unsupported attachment type")

            if not filename:
                filename = "attachment.bin"

            msg.add_attachment(content, maintype="application", subtype="octet-stream", filename=filename)
        except Exception as e:
            raise RuntimeError(f"Failed to attach {getattr(att,'name', getattr(att,'path', 'file'))}: {e}")

    all_rcpts = to_list + cc_list + bcc_list

    # Send via Gmail SMTP with STARTTLS (requires App Password on accounts with 2FA)
    with metric_timer('email_send_ms', {'fn':'send_outreach_email'}):
    with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"]) as server:
        server.ehlo()
        server.starttls()
        server.login(cfg["username"], cfg["password"])
        server.send_message(msg, from_addr=cfg["from_addr"], to_addrs=all_rcpts)
        metric_push('email_success', 1, {'to': str(len(all_rcpts))})


# --- Outreach Tools UI (moved from sidebar to Outreach tab to prevent bleed-through) ---




def _normalize_extra_files(files):
    """Normalize a list of attachments into dicts with name and raw bytes in data."""
    out = []
    try:
        for f in (files or []):
            # Already a normalized dict
            if isinstance(f, dict):
                name = f.get("name") or f.get("filename") or "file"
                if "data" in f and f["data"] is not None:
                    out.append({"name": name, "data": f["data"]})
                    continue
                if "content" in f and f["content"] is not None:
                    val = f["content"]
                    if isinstance(val, (bytes, bytearray)):
                        out.append({"name": name, "data": bytes(val)})
                    elif isinstance(val, str):
                        import os
                        if os.path.exists(val):
                            with open(val, "rb") as fh:
                                out.append({"name": name, "data": fh.read()})
                        else:
                            out.append({"name": name, "data": val.encode("utf-8")})
                    continue
                if "path" in f and f["path"]:
                    import os
                    path = f["path"]
                    try:
                        with open(path, "rb") as fh:
                            out.append({"name": name or os.path.basename(path), "data": fh.read()})
                    except Exception:
                        pass
                    continue

            # Streamlit UploadedFile or similar
            if hasattr(f, "getvalue"):
                out.append({"name": getattr(f, "name", "file"), "data": f.getvalue()})
                continue
            if hasattr(f, "read"):
                try:
                    f.seek(0)
                except Exception:
                    pass
                try:
                    data = f.read()
                    out.append({"name": getattr(f, "name", "file"), "data": data})
                    continue
                except Exception:
                    pass

            # File path string
            if isinstance(f, str):
                import os
                if os.path.exists(f):
                    try:
                        with open(f, "rb") as fh:
                            out.append({"name": os.path.basename(f), "data": fh.read()})
                        continue
                    except Exception:
                        pass
    except Exception:
        pass
    return out



def _log_contact_outreach(entries):
    """Append outreach log entries to data/contact_outreach_log.json"""
    try:
        import os, json, datetime
        base = os.path.join(os.getcwd(), "data")
        os.makedirs(base, exist_ok=True)
        path = os.path.join(base, "contact_outreach_log.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = []
        timestamp = datetime.datetime.utcnow().isoformat()+"Z"
        for e in entries or []:
            e.setdefault("ts_utc", timestamp)
        existing.extend(entries or [])
        with open(path, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
        return path
    except Exception:
        return None


def render_outreach_tools():
    import streamlit as st
    import streamlit.components.v1 as components
    # ---------- Helpers ----------
    def _normalize_sel_attachments(sel_atts):
        """Return a list of dicts with just 'name' for display when attachments in the generated item are names/dicts."""
        out = []
        base = sel_atts or []
        try:
            for a in base:
                if isinstance(a, dict) and ("name" in a or "filename" in a):
                    nm = a.get("name") or a.get("filename") or "attachment"
                    out.append({"name": nm})
                elif isinstance(a, str):
                    out.append({"name": a})
        except Exception:
            pass
        return out


        out = []
        try:
            for f in (files or []):
                # Already-normalized dict: pass through or convert
                if isinstance(f, dict):
                    name = f.get("name") or f.get("filename") or "file"
                    if "data" in f and f["data"] is not None:
                        out.append({"name": name, "data": f["data"]})
                    elif "content" in f and f["content"] is not None:
                        val = f["content"]
                        if isinstance(val, (bytes, bytearray)):
                            out.append({"name": name, "data": bytes(val)})
                        elif isinstance(val, str):
                            # If looks like a path, try to read from disk
                            import os
                            if os.path.exists(val):
                                with open(val, "rb") as fh:
                                    out.append({"name": name, "data": fh.read()})
                            else:
                                out.append({"name": name, "data": val.encode("utf-8")})
                    elif "path" in f and f["path"]:
                        import os
                        path = f["path"]
                        try:
                            with open(path, "rb") as fh:
                                out.append({"name": name or os.path.basename(path), "data": fh.read()})
                        except Exception:
                            pass
                    continue

                # Streamlit UploadedFile or similar
                if hasattr(f, "getvalue"):
                    out.append({"name": getattr(f, "name", "file"), "data": f.getvalue()})
                    continue
                if hasattr(f, "read"):
                    try:
                        f.seek(0)
                    except Exception:
                        pass
                    try:
                        data = f.read()
                        out.append({"name": getattr(f, "name", "file"), "data": data})
                        continue
                    except Exception:
                        pass

                # File path
                if isinstance(f, str):
                    import os
                    if os.path.exists(f):
                        try:
                            with open(f, "rb") as fh:
                                out.append({"name": os.path.basename(f), "data": fh.read()})
                            continue
                        except Exception:
                            pass
        except Exception:
            pass
        return out

    # Robust local sender that tries multiple implementations
    def _send_email(user, to, subject, body_html, cc="", bcc="", attachments=None):
        last_err = None
        # Preferred modern signature
        try:
            return send_outreach_email(user, to, subject, body_html,
                                       cc_addrs=cc, bcc_addrs=bcc, attachments=attachments)
        except Exception as e:
            last_err = e
        # Legacy fallback (active-user based)
        try:
            return outreach_send_from_active_user(to, subject, body_html,
                                                  cc=cc, bcc=bcc, attachments=attachments)
        except Exception as e:
            last_err = e
        # Optional extra names if your app exposes them
        for name in ("send_outreach_message", "send_gmail_message", "send_mail", "outreach_send"):
            fn = globals().get(name)
            if callable(fn):
                try:
                    return fn(user, to, subject, body_html, cc, bcc, attachments)
                except Exception as e:
                    last_err = e
        raise last_err or RuntimeError("No outreach sender is available")

    # ---------- Stable session keys ----------
    SKEY_PREVIEW = f"{ACTIVE_USER}::outreach::preview"             # snapshot for the Gmail-style preview card
    SKEY_ATTACH  = f"{ACTIVE_USER}::outreach::extra_attachments"   # extra attachments uploaded by user (UploadedFile list)
    SKEY_LASTSIG = f"{ACTIVE_USER}::outreach::last_loaded_sig"

    st.session_state.setdefault(SKEY_PREVIEW, None)
    st.session_state.setdefault(SKEY_ATTACH, [])

    st.session_state.setdefault(SKEY_LASTSIG, "")
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")

    # ---------- Header ----------
    with st.container(border=True):
        top_l, top_r = st.columns([3,2])
        with top_l:
            st.markdown("### ✉️ Outreach")
            st.caption(f"From: **{from_addr}**" if from_addr else "No email configured for this user.")
        with st.container(border=True):
            mode = st.radio("Send to", ["Vendors", "Contacts"], index=0, horizontal=True, key="outreach_mode")



    # ---- Contacts Outreach ----
    if mode == "Contacts":
        with st.container(border=True):
            st.markdown("#### Contacts")
            # Read receipts + tracking pixel options
            with st.expander("Delivery & Tracking options", expanded=False):
                want_rr = st.checkbox("Request read receipt headers (may prompt recipient)", value=False, key="outreach_rr")
                pixel_url = st.text_input("Optional tracking pixel URL (https://...)", value="", key="outreach_pixel_url")
            # Load contacts from CSV
            col_c1, col_c2 = st.columns([2,1])
            with col_c1:
                search = st.text_input("Search contacts", key="outreach_contact_search")
            with col_c2:
                uploaded = st.file_uploader("", type=["csv"], key="outreach_contacts_csv")
            contacts = []
            import os, csv
            # Prefer uploaded CSV
            if uploaded is not None:
                try:
                    txt = uploaded.getvalue().decode("utf-8", errors="ignore")
                    for row in csv.DictReader(txt.splitlines()):
                        nm = row.get("name") or row.get("Name") or row.get("full_name") or ""
                        em = row.get("email") or row.get("Email") or row.get("mail") or ""
                        if em:
                            contacts.append({"name": nm, "email": em})
                except Exception:
                    pass
            else:
                # Try default data/contacts.csv
                try:
                    path = os.path.join(os.getcwd(), "data", "contacts.csv")
                    if os.path.exists(path):
                        with open(path, "r", encoding="utf-8") as f:
                            for row in csv.DictReader(f):
                                nm = row.get("name") or row.get("Name") or row.get("full_name") or ""
                                em = row.get("email") or row.get("Email") or row.get("mail") or ""
                                if em:
                                    contacts.append({"name": nm, "email": em})
                except Exception:
                    pass

            # Filter by search
            s = (search or "").lower().strip()
            if s:
                contacts = [c for c in contacts if s in (c.get("name","")+c.get("email","")).lower()]

            # Options
            labels = [f'{c.get("name") or ""} <{c["email"]}>' if c.get("name") else c["email"] for c in contacts]
            selected = st.multiselect("Recipients", labels, key="outreach_contact_sel")

            subj = st.text_input("Subject", key="outreach_contact_subject")
            body = st.text_area("Body (HTML allowed)", key="outreach_contact_body", height=220)
            c_files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key="outreach_contact_files")

            if st.button("Send to selected contacts", use_container_width=True, key="outreach_contact_send"):
                emails = []
                label_to_email = {}
                for c, lbl in zip(contacts, labels):
                    label_to_email[lbl] = c["email"]
                for lbl in selected:
                    em = label_to_email.get(lbl)
                    if em:
                        emails.append(em)
                if not emails:
                    st.warning("Select at least one contact.")
                elif not subj or not body:
                    st.warning("Subject and body are required.")
                else:
                    # Normalize files
                    atts = _normalize_extra_files(c_files)
                    # Tracking id per batch
                    import uuid
                    batch_id = str(uuid.uuid4())
                    failures = []
                    sent = 0
                    for em in emails:
                        try:
                            send_outreach_email(
                                ACTIVE_USER, [em], subj, body,
                                cc_addrs=None, bcc_addrs=None, attachments=atts,
                                add_read_receipts=want_rr, tracking_pixel_url=(pixel_url or None),
                                tracking_id=batch_id + "::" + em
                            )
                            sent += 1
                        except Exception as e:
                            failures.append((em, str(e)))
                    # Log
                    _log_contact_outreach([{"mode":"contacts","to": em, "subject": subj, "batch_id": batch_id} for em in emails])
                    if failures:
                        st.error(f"Sent {sent} / {len(emails)}. Failures: " + "; ".join([f"{a} ({b})" for a,b in failures]))
                    else:
                        st.success(f"Sent {sent} / {len(emails)}")
        # Stop rendering vendor section if Contacts mode
        return


        with top_r:
            pass

    # ---- Account: App Password (still here) ----
    with st.expander("Set/Update my Gmail App Password", expanded=False):
        pw = st.text_input("Gmail App Password", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            try:
                set_user_smtp_app_password(ACTIVE_USER, pw)
                st.success("Saved")
            except Exception as e:
                st.error(f"Failed to save: {e}")

    st.divider()

    # ---------- Choose Generated Email & Attachments (required) ----------
    with st.container(border=True):
        st.markdown("#### Choose Generated Email")
        mb = st.session_state.get("mail_bodies") or []
        if not mb:
            st.info("Generate emails to select one for preview.", icon="ℹ️")
        else:
            idx = st.number_input("Select one", min_value=1, max_value=len(mb), value=len(mb), step=1,
                                  key=ns_key("outreach::pick_idx"))
            sel = mb[int(idx)-1]

            # Show key fields from the generated email
            st.caption(f"**To:** {sel.get('to','')}")
            st.caption(f"**Subject:** {sel.get('subject','')}")
            scope_disp = sel.get("scope_summary") or sel.get("scope") or ""
            due_disp = sel.get("quote_due") or sel.get("due") or ""
            meta_cols = st.columns(2)
            with meta_cols[0]:
                st.markdown(f"**Scope Summary:** {scope_disp}")
            with meta_cols[1]:
                st.markdown(f"**Quote Due:** {due_disp}")

            # Attachments uploader (REQUIRED) placed below Quote Due
            extra_files = st.file_uploader("Attachments (required)", type=None, accept_multiple_files=True,
                                           key=ns_key("outreach::extra_files"))
            if extra_files is not None:
                st.session_state[SKEY_ATTACH] = extra_files

            # Generate preview button
            if st.button("Generate preview", key=ns_key("outreach::gen_preview"), use_container_width=True):
                files = st.session_state.get(SKEY_ATTACH) or []
                if not files:
                    st.warning("Please upload at least one attachment before generating the preview.")
                else:
                    # Build display names from generated attachments + uploaded files
                    gen_names = _normalize_sel_attachments(sel.get("attachments"))
                    try:
                        upload_names = [{"name": getattr(f, "name", "file")} for f in files]
                    except Exception:
                        upload_names = []
                    st.session_state[SKEY_PREVIEW] = {
                        "to": sel.get("to",""),
                        "cc": sel.get("cc",""),
                        "bcc": sel.get("bcc",""),
                        "subject": sel.get("subject",""),
                        "body_html": sel.get("body",""),
                        "from_addr": USER_EMAILS.get(ACTIVE_USER, ""),
                        "scope_summary": scope_disp,
                        "quote_due": due_disp,
                        "attachments": (gen_names or []) + (upload_names or [])
                    }
                    st.success("Preview generated below.")


            actions2 = st.columns([1, 2, 2, 5])
            with actions2[1]:
                if st.button("Send selected now", key=ns_key("outreach::send_selected_now"), use_container_width=True):
                    files = st.session_state.get(SKEY_ATTACH) or []
                    if not files:
                        st.warning("Please upload at least one attachment before sending.")
                    else:
                        try:
                            merged_atts = _normalize_sel_attachments(sel.get("attachments")) + _normalize_extra_files(files)
                            _send_email(
                                ACTIVE_USER,
                                sel.get("to",""),
                                sel.get("subject",""),
                                sel.get("body",""),
                                cc=sel.get("cc",""),
                                bcc=sel.get("bcc",""),
                                attachments=merged_atts
                            )
                            st.success("Selected email sent.")
                        except Exception as e:
                            st.error(f"Failed to send selected: {e}")
            with actions2[2]:
                if st.button("Send ALL generated now", key=ns_key("outreach::send_all_now"), use_container_width=True):
                    files = st.session_state.get(SKEY_ATTACH) or []
                    if not files:
                        st.warning("Please upload at least one attachment before mass sending.")
                    else:
                        mb_all = st.session_state.get("mail_bodies") or []
                        sent = 0
                        failures = []
                        for i, itm in enumerate(mb_all, start=1):
                            try:
                                merged_atts = _normalize_sel_attachments(itm.get("attachments")) + _normalize_extra_files(files)
                                _send_email(
                                    ACTIVE_USER,
                                    itm.get("to",""),
                                    itm.get("subject",""),
                                    itm.get("body",""),
                                    cc=itm.get("cc",""),
                                    bcc=itm.get("bcc",""),
                                    attachments=merged_atts
                                )
                                sent += 1
                            except Exception as e:
                                failures.append((i, itm.get("subject",""), str(e)))
                        if failures:
                            st.error(f"Sent {sent} / {len(mb_all)}. Failures: " + "; ".join([f"#{i} {subj} ({err})" for i, subj, err in failures]))
                        else:
                            st.success(f"Sent all {sent} generated emails.")# ---------- Single Preview (Gmail-like card) ---------- (Gmail-like card) ----------
    snap = st.session_state.get(SKEY_PREVIEW)
    with st.container(border=True):
        st.markdown("#### Preview")
        if not snap:
            st.info("Select a generated email above, attach files if needed, and click Preview.", icon="ℹ️")
        else:
            # Header block similar to Gmail
            hdr_lines = []
            if snap.get("from_addr"): hdr_lines.append(f"<div><b>From:</b> {snap['from_addr']}</div>")
            if snap.get("to"):        hdr_lines.append(f"<div><b>To:</b> {snap['to']}</div>")
            if snap.get("cc"):        hdr_lines.append(f"<div><b>Cc:</b> {snap['cc']}</div>")
            if snap.get("bcc"):       hdr_lines.append(f"<div><b>Bcc:</b> {snap['bcc']}</div>")
            if snap.get("subject"):   hdr_lines.append(f"<div style='font-size:16px;margin-top:4px;'><b>Subject:</b> {snap['subject']}</div>")

            # Meta row: Scope Summary & Quote Due
            meta_bits = []
            if snap.get("scope_summary"):
                meta_bits.append("<div style='display:inline-block;border:1px solid #eee;"
                                 "padding:4px 8px;border-radius:8px;margin-right:8px;'><b>Scope:</b> "
                                 f"{snap['scope_summary']}</div>")
            if snap.get("quote_due"):
                meta_bits.append("<div style='display:inline-block;border:1px solid #eee;"
                                 "padding:4px 8px;border-radius:8px;'><b>Quote due:</b> "
                                 f"{snap['quote_due']}</div>")


            # Attachments uploader (positioned below Quote Due)
            extra_files = st.file_uploader("Attachments (required)", type=None, accept_multiple_files=True,
                                           key=ns_key("outreach::extra_files"))
            if extra_files is not None:
                st.session_state[SKEY_ATTACH] = extra_files

            # Body
            body_html = (snap.get("body_html") or "").strip() or "<p><i>(No body content)</i></p>"

            # Attachments display
            atts_html = ""
            atts = snap.get("attachments") or []
            if atts:
                items = "".join([f"<li>{(a.get('name') if isinstance(a,dict) else str(a))}</li>" for a in atts])
                atts_html = ("<div style='margin-top:8px;'><b>Attachments:</b>"
                             f"<ul style='margin:6px 0 0 20px;'>{items}</ul></div>")

            components.html(f"""
                <div style="border:1px solid #ddd;border-radius:8px;padding:14px;">
                    <div style="margin-bottom:8px;">{''.join(hdr_lines)}</div>
                    <div style="margin-bottom:8px;">{''.join(meta_bits)}</div>
                    <div style="border:1px solid #eee;padding:10px;border-radius:6px;">{body_html}</div>
                    {atts_html}
                </div>
            """, height=520, scrolling=True)

            # Actions under the preview
            a1, a2 = st.columns(2)
            with a1:
                if st.button("Send email", key=ns_key("outreach::send_from_preview"), use_container_width=True):
                    try:
                        _send_email(
                            ACTIVE_USER,
                            snap.get("to",""),
                            snap.get("subject",""),
                            snap.get("body_html",""),
                            cc=snap.get("cc",""),
                            bcc=snap.get("bcc",""),
                            attachments=st.session_state.get(SKEY_ATTACH) or []
                        )
                        st.success("Email sent.")
                        st.session_state[SKEY_PREVIEW] = None
                    except Exception as e:
                        st.error(f"Failed to send: {e}")
            with a2:
                if st.button("Close preview", key=ns_key("outreach::close_preview"), use_container_width=True):
                    st.session_state[SKEY_PREVIEW] = None

def load_outreach_preview(to="", cc="", bcc="", subject="", html=""):
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    key = lambda k: f"{ACTIVE_USER}::outreach::{k}"
    st.session_state[key("to")] = to or ""
    st.session_state[key("cc")] = cc or ""
    st.session_state[key("bcc")] = bcc or ""
    st.session_state[key("subj")] = subject or ""
    st.session_state[key("body")] = html or ""
    st.session_state[key("preview")] = {
        "to": to or "",
        "cc": cc or "",
        "bcc": bcc or "",
        "subject": subject or "",
        "body_html": html or "",
        "attachments": [],
        "from_addr": from_addr,
    }

    st.subheader("Email – Outreach")
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    if not from_addr:
        st.caption("No email configured for this user. Only Charles and Collin are set up.")
    else:
        st.caption(f"From: {from_addr}")

    st.session_state.setdefault(ns_key("outreach::mail_preview_data"), None)

    hc1, hc2, hc3 = st.columns([1,1,2])
    with hc1:
        if st.button("Preview current draft", key=ns_key("outreach::hdr_preview_btn")):
            to = st.session_state.get(ns_key("outreach::mail_to"), "") or ""
            cc = st.session_state.get(ns_key("outreach::mail_cc"), "") or ""
            bcc = st.session_state.get(ns_key("outreach::mail_bcc"), "") or ""
            subj = st.session_state.get(ns_key("outreach::mail_subj"), "") or ""
            body = st.session_state.get(ns_key("outreach::mail_body"), "") or ""
            atts = (st.session_state.get(ns_key("outreach::mail_preview_data")) or {}).get("attachments", [])
            st.session_state[ns_key("outreach::mail_preview_data")] = {
                "to": to,
                "cc": cc,
                "bcc": bcc,
                "subject": subj,
                "body_html": body,
                "attachments": atts,
                "from_addr": from_addr,
            }
    with hc2:
        if st.button("Clear preview", key=ns_key("outreach::hdr_preview_clear")):
            st.session_state[ns_key("outreach::mail_preview_data")] = None

    with st.expander("Set/Update my Gmail App Password", expanded=False):
        st.caption("Generate an App Password in your Google Account > Security > 2-Step Verification.")
        app_pw = st.text_input("Gmail App Password (16 chars, no spaces)", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            set_user_smtp_app_password(ACTIVE_USER, app_pw)
            st.success("Saved. You can now send emails from the Outreach composer.")

    with st.expander("Quick Outreach Composer", expanded=False):
        to = st.text_input("To (comma-separated)", key=ns_key("outreach::mail_to"),
                           placeholder="recipient@example.com, another@domain.com")
        cc = st.text_input("Cc (optional, comma-separated)", key=ns_key("outreach::mail_cc"))
        bcc = st.text_input("Bcc (optional, comma-separated)", key=ns_key("outreach::mail_bcc"))
        subj = st.text_input("Subject", key=ns_key("outreach::mail_subj"))
        body = st.text_area("Message (HTML supported)", key=ns_key("outreach::mail_body"), height=200,
                            placeholder="<p>Hello.</p>")
        files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key=ns_key("outreach::mail_files"))

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Preview email", use_container_width=True, key=ns_key("outreach::mail_preview_btn")):
                atts = []
                try:
                    for f in (files or []):
                        try:
                            atts.append({"name": getattr(f, "name", "file"), "data": f.getvalue()})
                        except Exception:
                            pass
                except Exception:
                    atts = []
                st.session_state[ns_key("outreach::mail_preview_data")] = {
                    "to": to or "",
                    "cc": cc or "",
                    "bcc": bcc or "",
                    "subject": subj or "",
                    "body_html": body or "",
                    "attachments": atts,
                    "from_addr": from_addr,
                }
        with c2:
            if st.button("Send email", use_container_width=True, key=ns_key("outreach::mail_send_btn")):
                try:
                    send_outreach_email(ACTIVE_USER, to, subj, body, cc_addrs=cc, bcc_addrs=bcc, attachments=files)
                    st.success("Email sent.")
                    for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                        NS.pop(k, None)
                except Exception as e:
                    st.error(f"Failed to send: {e}")

    preview = st.session_state.get(ns_key("outreach::mail_preview_data"))
    if preview:
        import streamlit.components.v1 as components
        with st.container(border=True):
            st.markdown("#### Email preview")
            st.markdown(f"**From:** {preview.get('from_addr','')}")
            if preview.get("to"): st.markdown(f"**To:** {preview['to']}")
            if preview.get("cc"): st.markdown(f"**Cc:** {preview['cc']}")
            if preview.get("bcc"): st.markdown(f"**Bcc:** {preview['bcc']}")
            st.markdown(f"**Subject:** {preview.get('subject','')}")
            html = preview.get("body_html") or ""
            components.html(
                f"""
                <div style="border:1px solid #ddd;padding:16px;margin-top:8px;">
                    {html}
                </div>
                """,
                height=400,
                scrolling=True,
            )
        atts = preview.get("attachments") or []
        if atts:
            names = [a.get("name","file") for a in atts]
            st.caption("Attachments: " + ", ".join(names))

        cc1, cc2, _ = st.columns([1,1,2])
        with cc1:
            if st.button("Send this email", key=ns_key("outreach::mail_preview_confirm")):
                class _MemFile:
                    def __init__(self, name, data):
                        self.name = name
                        self._data = data
                    def getvalue(self):
                        return self._data
                mem_files = [_MemFile(a.get("name","file"), a.get("data", b"")) for a in atts]
                try:
                    send_outreach_email(
                        ACTIVE_USER,
                        preview.get("to",""),
                        preview.get("subject",""),
                        preview.get("body_html",""),
                        cc_addrs=preview.get("cc",""),
                        bcc_addrs=preview.get("bcc",""),
                        attachments=mem_files
                    )
                    st.success("Email sent.")
                    st.session_state[ns_key("outreach::mail_preview_data")] = None
                    for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                        NS.pop(k, None)
                except Exception as e:
                    st.error(f"Failed to send: {e}")
        with cc2:
            if st.button("Close preview", key=ns_key("outreach::mail_preview_close")):
                st.session_state[ns_key("outreach::mail_preview_data")] = None
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    if not from_addr:
        st.caption("No email configured for this user. Only Charles and Collin are set up.")
    else:
        st.caption(f"From: {from_addr}")

    # Global preview state
    st.session_state.setdefault(ns_key("outreach::mail_preview_data"), None)

    # === Header-level controls ===
    hc1, hc2, hc3 = st.columns([1,1,2])
    with hc1:
        if st.button("Preview current draft", key=ns_key("outreach::hdr_preview_btn")):
            # Pull current draft values from session, even if the composer expander is closed
            to = st.session_state.get(ns_key("outreach::mail_to"), "") or ""
            cc = st.session_state.get(ns_key("outreach::mail_cc"), "") or ""
            bcc = st.session_state.get(ns_key("outreach::mail_bcc"), "") or ""
            subj = st.session_state.get(ns_key("outreach::mail_subj"), "") or ""
            body = st.session_state.get(ns_key("outreach::mail_body"), "") or ""
            # Attachments are not easily accessible from header because uploader holds file objects;
            # keep whatever was already captured if a composer preview was taken, else empty.
            atts = (st.session_state.get(ns_key("outreach::mail_preview_data")) or {}).get("attachments", [])

            st.session_state[ns_key("outreach::mail_preview_data")] = {
                "to": to, "cc": cc, "bcc": bcc,
                "subject": subj,
                "body_html": body,
                "attachments": atts,
                "from_addr": from_addr,
            }
    with hc2:
        if st.button("Clear preview", key=ns_key("outreach::hdr_preview_clear")):
            st.session_state[ns_key("outreach::mail_preview_data")] = None

    with st.expander("Set/Update my Gmail App Password", expanded=False):
        st.caption("Generate an App Password in your Google Account > Security > 2-Step Verification.")
        app_pw = st.text_input("Gmail App Password (16 chars, no spaces)", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            set_user_smtp_app_password(ACTIVE_USER, app_pw)
            st.success("Saved. You can now send emails from the Outreach composer.")

    # === Quick Outreach Composer ===
    with st.expander("Quick Outreach Composer", expanded=False):
        to = st.text_input("To (comma-separated)",
                           key=ns_key("outreach::mail_to"),
                           placeholder="recipient@example.com, another@domain.com")
        cc = st.text_input("Cc (optional, comma-separated)", key=ns_key("outreach::mail_cc"))
        bcc = st.text_input("Bcc (optional, comma-separated)", key=ns_key("outreach::mail_bcc"))
        subj = st.text_input("Subject", key=ns_key("outreach::mail_subj"))
        body = st.text_area("Message (HTML supported)", key=ns_key("outreach::mail_body"), height=200,
                            placeholder="<p>Hello.</p>")
        files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key=ns_key("outreach::mail_files"))

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Preview email", use_container_width=True, key=ns_key("outreach::mail_preview_btn")):
                # Snapshot current fields (including attachments) for a pixel-accurate preview
                atts = []
                try:
                    for f in (files or []):
                        try:
                            atts.append({"name": getattr(f, "name", "file"), "data": f.getvalue()})
                        except Exception:
                            pass
                except Exception:
                    atts = []
                st.session_state[ns_key("outreach::mail_preview_data")] = {
                    "to": to or "",
                    "cc": cc or "",
                    "bcc": bcc or "",
                    "subject": subj or "",
                    "body_html": body or "",
                    "attachments": atts,
                    "from_addr": from_addr,
                }
        with c2:
            if st.button("Send email", use_container_width=True, key=ns_key("outreach::mail_send_btn")):
                try:
                    send_outreach_email(ACTIVE_USER, to, subj, body, cc_addrs=cc, bcc_addrs=bcc, attachments=files)
                    st.success("Email sent.")
                    for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                        NS.pop(k, None)
                except Exception as e:
                    st.error(f"Failed to send: {e}")

    # === Unified Preview Block (used by both header-level and composer-level triggers) ===
    preview = st.session_state.get(ns_key("outreach::mail_preview_data"))
    if preview:
        import streamlit.components.v1 as components
        with st.container(border=True):
            st.markdown("#### Email preview")
            st.markdown(f"**From:** {preview.get('from_addr','')}")
            if preview.get("to"):
                st.markdown(f"**To:** {preview['to']}")
            if preview.get("cc"):
                st.markdown(f"**Cc:** {preview['cc']}")
            if preview.get("bcc"):
                st.markdown(f"**Bcc:** {preview['bcc']}")
            st.markdown(f"**Subject:** {preview.get('subject','')}")

            html = preview.get("body_html") or ""
            components.html(
                f"""
                <div style="border:1px solid #ddd;padding:16px;margin-top:8px;">
                    {html}
                </div>
                """,
                height=400,
                scrolling=True,
            )

            atts = preview.get("attachments") or []
            if atts:
                names = [a.get("name","file") for a in atts]
                st.caption("Attachments: " + ", ".join(names))

            cc1, cc2, cc3 = st.columns([1,1,2])
            with cc1:
                if st.button("Send this email", key=ns_key("outreach::mail_preview_confirm")):
                    class _MemFile:
                        def __init__(self, name, data):
                            self.name = name
                            self._data = data
                        def getvalue(self):
                            return self._data
                    mem_files = [_MemFile(a.get("name","file"), a.get("data", b"")) for a in atts]
                    try:
                        send_outreach_email(
                            ACTIVE_USER,
                            preview.get("to",""),
                            preview.get("subject",""),
                            preview.get("body_html",""),
                            cc_addrs=preview.get("cc",""),
                            bcc_addrs=preview.get("bcc",""),
                            attachments=mem_files
                        )
                        st.success("Email sent.")
                        st.session_state[ns_key("outreach::mail_preview_data")] = None
                        for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                            NS.pop(k, None)
                    except Exception as e:
                        st.error(f"Failed to send: {e}")
            with cc2:
                if st.button("Close preview", key=ns_key("outreach::mail_preview_close")):
                    st.session_state[ns_key("outreach::mail_preview_data")] = None
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    if not from_addr:
        st.caption("No email configured for this user. Only Charles and Collin are set up.")
    else:
        st.caption(f"From: {from_addr}")

    with st.expander("Set/Update my Gmail App Password", expanded=False):
        st.caption("Generate an App Password in your Google Account > Security > 2-Step Verification.")
        app_pw = st.text_input("Gmail App Password (16 chars, no spaces)", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            set_user_smtp_app_password(ACTIVE_USER, app_pw)
            st.success("Saved. You can now send emails from the Outreach composer.")

    # Preview state
    st.session_state.setdefault(ns_key("outreach::mail_preview_data"), None)

    with st.expander("Quick Outreach Composer", expanded=False):
        to = st.text_input("To (comma-separated)",
                           key=ns_key("outreach::mail_to"),
                           placeholder="recipient@example.com, another@domain.com")
        cc = st.text_input("Cc (optional, comma-separated)", key=ns_key("outreach::mail_cc"))
        bcc = st.text_input("Bcc (optional, comma-separated)", key=ns_key("outreach::mail_bcc"))
        subj = st.text_input("Subject", key=ns_key("outreach::mail_subj"))
        body = st.text_area("Message (HTML supported)", key=ns_key("outreach::mail_body"), height=200,
                            placeholder="<p>Hello.</p>")
        files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key=ns_key("outreach::mail_files"))

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Preview email", use_container_width=True, key=ns_key("outreach::mail_preview_btn")):
                # Store a snapshot of the compose fields in session so a rerun preserves the preview
                # For attachments, store name and raw bytes so we can reconstruct file-like objects later.
                atts = []
                try:
                    for f in (files or []):
                        try:
                            atts.append({
                                "name": getattr(f, "name", "file"),
                                "data": f.getvalue()
                            })
                        except Exception:
                            pass
                except Exception:
                    atts = []
                st.session_state[ns_key("outreach::mail_preview_data")] = {
                    "to": to or "",
                    "cc": cc or "",
                    "bcc": bcc or "",
                    "subject": subj or "",
                    "body_html": body or "",
                    "attachments": atts,
                    "from_addr": from_addr,
                }
        with c2:
            if st.button("Send email", use_container_width=True, key=ns_key("outreach::mail_send_btn")):
                try:
                    send_outreach_email(ACTIVE_USER, to, subj, body, cc_addrs=cc, bcc_addrs=bcc, attachments=files)
                    st.success("Email sent.")
                    for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                        NS.pop(k, None)
                except Exception as e:
                    st.error(f"Failed to send: {e}")

    # If a preview has been requested, render it exactly like the HTML body will appear.
    preview = st.session_state.get(ns_key("outreach::mail_preview_data"))
    if preview:
        import streamlit.components.v1 as components

        with st.container(border=True):
            st.markdown("#### Email preview")
            # Header preview
            st.markdown(f"**From:** {preview.get('from_addr','')}")
            if preview.get("to"):
                st.markdown(f"**To:** {preview['to']}")
            if preview.get("cc"):
                st.markdown(f"**Cc:** {preview['cc']}")
            if preview.get("bcc"):
                st.markdown(f"**Bcc:** {preview['bcc']}")
            st.markdown(f"**Subject:** {preview.get('subject','')}")

            # Render the HTML body using a component so styles and tags are honored
            html = preview.get("body_html") or ""
            components.html(
                f"""
                <div style="border:1px solid #ddd;padding:16px;margin-top:8px;">
                    {html}
                </div>
                """,
                height=400,
                scrolling=True,
            )

            # Show attachment list if any
            atts = preview.get("attachments") or []
            if atts:
                names = [a.get("name","file") for a in atts]
                st.caption("Attachments: " + ", ".join(names))

            # Confirm send buttons
            cc1, cc2, cc3 = st.columns([1,1,2])
            with cc1:
                if st.button("Send this email", key=ns_key("outreach::mail_preview_confirm")):
                    # Rebuild simple in memory files compatible with send_outreach_email expectations
                    class _MemFile:
                        def __init__(self, name, data):
                            self.name = name
                            self._data = data
                        def getvalue(self):
                            return self._data
                    mem_files = [_MemFile(a.get("name","file"), a.get("data", b"")) for a in atts]
                    try:
                        send_outreach_email(
                            ACTIVE_USER,
                            preview.get("to",""),
                            preview.get("subject",""),
                            preview.get("body_html",""),
                            cc_addrs=preview.get("cc",""),
                            bcc_addrs=preview.get("bcc",""),
                            attachments=mem_files
                        )
                        st.success("Email sent.")
                        st.session_state[ns_key("outreach::mail_preview_data")] = None
                        # Clear compose fields
                        for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                            NS.pop(k, None)
                    except Exception as e:
                        st.error(f"Failed to send: {e}")
            with cc2:
                if st.button("Close preview", key=ns_key("outreach::mail_preview_close")):
                    st.session_state[ns_key("outreach::mail_preview_data")] = None
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    if not from_addr:
        st.caption("No email configured for this user. Only Charles and Collin are set up.")
    else:
        st.caption(f"From: {from_addr}")

    with st.expander("Set/Update my Gmail App Password", expanded=False):
        st.caption("Generate an App Password in your Google Account > Security > 2-Step Verification.")
        app_pw = st.text_input("Gmail App Password (16 chars, no spaces)", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            set_user_smtp_app_password(ACTIVE_USER, app_pw)
            st.success("Saved. You can now send emails from the Outreach composer.")

    with st.expander("Quick Outreach Composer", expanded=False):
        to = st.text_input("To (comma-separated)",
                           key=ns_key("outreach::mail_to"),
                           placeholder="recipient@example.com, another@domain.com")
        cc = st.text_input("Cc (optional, comma-separated)", key=ns_key("outreach::mail_cc"))
        bcc = st.text_input("Bcc (optional, comma-separated)", key=ns_key("outreach::mail_bcc"))
        subj = st.text_input("Subject", key=ns_key("outreach::mail_subj"))
        body = st.text_area("Message (HTML supported)", key=ns_key("outreach::mail_body"), height=200,
                            placeholder="<p>Hello...</p>")
        files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key=ns_key("outreach::mail_files"))
        if st.button("Send email", use_container_width=True, key=ns_key("outreach::mail_send_btn")):
            try:
                send_outreach_email(ACTIVE_USER, to, subj, body, cc_addrs=cc, bcc_addrs=bcc, attachments=files)
                st.success("Email sent.")
                for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                    NS.pop(k, None)
            except Exception as e:
                st.error(f"Failed to send: {e}")

def outreach_send_from_active_user(to, subject, body_html, cc=None, bcc=None, attachments=None):
    return send_outreach_email(ACTIVE_USER, to, subject, body_html, cc_addrs=cc, bcc_addrs=bcc, attachments=attachments)
# === End Outreach Email block (moved) ===




import requests
from PyPDF2 import PdfReader
import docx
from sklearn.feature_extraction.text import TfidfVectorizer
# === OCR and clause risk helpers (injected) ===
try:
    import pytesseract  # optional
    from pdf2image import convert_from_bytes
except Exception:
    pytesseract = None
    convert_from_bytes = None

CLAUSE_RISKS = {
    "liquidated damages": "May require payments for delays. Propose realistic schedule and mitigation plan.",
    "termination for convenience": "Government can end the contract at any time. Manage inventory and subcontracts carefully.",
    "termination for default": "Strict performance risk. Include QA steps and corrective action plan.",
    "excessive bonding": "High bonding can strain cash flow. Ask if alternatives are allowed.",
    "unusual penalties": "Flag for legal review. Request clarification if ambiguous.",
    "indemnification": "Risk transfer to contractor. Verify insurance coverage.",
    "personal services": "May conflict with FAR rules if not intended. Confirm classification.",
    "pay when paid": "Cash flow risk for subs. Negotiate fair terms.",
    "liability cap absent": "Unlimited liability. Seek cap or clarify scope.",
}
def _find_clause_risks(text: str, top_k: int = 6):
    text_l = (text or "").lower()
    hits = []
    for key, hint in CLAUSE_RISKS.items():
        if key in text_l:
            hits.append({"clause": key, "hint": hint})
    return hits[:top_k]

def _ocr_pdf_bytes(pdf_bytes: bytes) -> str:
    if not (pytesseract and convert_from_bytes):
        return ""
    try:
        pages = convert_from_bytes(pdf_bytes, dpi=200)
        out = []
        for img in pages[:30]:
            out.append(pytesseract.image_to_string(img))
        return "\n".join(out)
    except Exception:
        return ""


# Optional HTML parsing for email scraper
try:
    from bs4 import BeautifulSoup  # pip install beautifulsoup4
except Exception:
    BeautifulSoup = None

# ---------- Safe key loader ----------
def _get_key(name: str) -> str:
    v = os.getenv(name, "")
    if v:
        return v
    try:
        return st.secrets[name]
    except Exception:
        return ""

OPENAI_API_KEY     = (_get_key("OPENAI_API_KEY") or "").strip()
GOOGLE_PLACES_KEY  = (_get_key("GOOGLE_PLACES_API_KEY") or "").strip()
SAM_API_KEY        = (_get_key("SAM_API_KEY") or "").strip()
MS_TENANT_ID       = (_get_key("MS_TENANT_ID") or "").strip()
MS_CLIENT_ID       = (_get_key("MS_CLIENT_ID") or "").strip()
MS_CLIENT_SECRET   = (_get_key("MS_CLIENT_SECRET") or "").strip()

# ---------- OpenAI client ----------
try:
    import openai as _openai_pkg
    from openai import OpenAI  # openai>=1.40.0 recommended
    _openai_version = getattr(_openai_pkg, "__version__", "unknown")
except Exception as e:
    st.warning("OpenAI SDK missing or too old. Chat features disabled until installed.")
    OpenAI = None

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
OPENAI_MODEL = os.getenv("OPENAI_MODEL", _get_key("OPENAI_MODEL") or "gpt-5-chat-latest")
_OPENAI_FALLBACK_MODELS = [
    OPENAI_MODEL,
    "gpt-5-chat-latest","gpt-5","gpt-5-2025-08-07",
    "gpt-5-mini","gpt-5-mini-2025-08-07",
    "gpt-5-nano","gpt-5-nano-2025-08-07",
    "gpt-4o-mini","gpt-4o",
]


def _send_via_smtp_host(to_addr: str, subject: str, body: str, from_addr: str,
                        smtp_server: str, smtp_port: int, smtp_user: str, smtp_pass: str,
                        reply_to: str | None = None) -> None:
    """Top level SMTP sender. Keeps email helpers available across the app."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg['Subject'] = subject
    if reply_to:
        msg['Reply-To'] = reply_to
    msg.attach(MIMEText(body, 'plain'))
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(from_addr, [to_addr], msg.as_string())


def _send_via_gmail(to_addr: str, subject: str, body: str) -> str:
    """
    Gmail sender using Streamlit secrets.
    Falls back to Microsoft Graph if Gmail is not configured.
    Returns "Sent" or "Preview" string to avoid crashes.
    """
    try:
        smtp_user = st.secrets.get("smtp_user")
        smtp_pass = st.secrets.get("smtp_pass")
    except Exception:
        smtp_user = smtp_pass = None

    if smtp_user and smtp_pass:
        from_addr = st.secrets.get("smtp_from", smtp_user) if hasattr(st, "secrets") else smtp_user
        reply_to = st.secrets.get("smtp_reply_to", None) if hasattr(st, "secrets") else None
        try:
            _send_via_smtp_host(to_addr, subject, body, from_addr, "smtp.gmail.com", 587, smtp_user, smtp_pass, reply_to)
            return "Sent"
        except Exception as e:
            try:
                st.warning(f"Gmail SMTP send failed: {e}")
            except Exception:
                pass
    # Fallback to Graph or preview
    try:
        sender_upn = get_setting("ms_sender_upn", "")
    except Exception:
        sender_upn = ""
    try:
        res = send_via_graph(to_addr, subject, body, sender_upn=sender_upn)
        return res if isinstance(res, str) else "Sent"
    except Exception:
        try:
            import streamlit as _st
            _st.warning("Email preview mode is active. Configure SMTP or Graph to send.")
        except Exception:
            pass
        return "Preview"

st.set_page_config(page_title="GovCon Copilot Pro", page_icon="ðŸ§°", layout="wide")

# ---- Date helpers for SAM search ----

# ---- SAM date parsing helper ----
try:
    _ = _parse_sam_date
except NameError:
    from datetime import datetime
    def _parse_sam_date(s):
        """Parse common SAM.gov date/time strings into datetime; return original on failure."""
        if s is None:
            return None
        if isinstance(s, datetime):
            return s
        txt = str(s).strip()
        # Try a few common SAM formats
        fmts = [
            "%m/%d/%Y %I:%M %p %Z",   # 09/30/2025 02:00 PM ET
            "%m/%d/%Y %H:%M %Z",      # 09/30/2025 14:00 ET
            "%m/%d/%Y %I:%M %p",      # 09/30/2025 02:00 PM
            "%m/%d/%Y %H:%M",         # 09/30/2025 14:00
            "%m/%d/%Y",               # 09/30/2025
            "%Y-%m-%dT%H:%M:%SZ",     # 2025-09-30T18:00:00Z
            "%Y-%m-%d"                # 2025-09-30
        ]
        for f in fmts:
            try:
                return datetime.strptime(txt, f)
            except Exception:
                pass
        return txt
try:
    _ = _us_date
except NameError:
    from datetime import datetime
    def _us_date(dt):
        try:
            return dt.strftime("%m/%d/%Y")
        except Exception:
            # If dt is a string or not a datetime, return as-is
            return str(dt)


# ---- Hoisted SAM helper (duplicate for e# (early use) ----

# ---- Datetime coercion helper for SAM Watch (inline before sam_search) ----
from datetime import datetime

def send_via_graph(to_addr: str, subject: str, body: str, sender_upn: str = None) -> str:
    """
    Send mail using Microsoft Graph with application permissions (client credentials).
    Uses /users/{sender}/sendMail. Returns "Sent" on success or a short diagnostic string on error.
    Env/settings used:
      - MS_TENANT_ID, MS_CLIENT_ID, MS_CLIENT_SECRET
      - MS_SENDER_UPN or settings key ms_sender_upn
    """
    try:
        import os, requests
        from urllib.parse import quote_plus
    except Exception as _e_imp:
        return f"Graph send error: missing dependency ({_e_imp})"

    # Load config: prefer env, then settings table if available
    try:
        sender = sender_upn or os.getenv("MS_SENDER_UPN") or get_setting("ms_sender_upn", "")
    except Exception:
        sender = sender_upn or os.getenv("MS_SENDER_UPN") or ""

    # MS_* may already be loaded at module level; fall back to env/settings if empty
    try:
        _tenant = os.getenv("MS_TENANT_ID") or get_setting("MS_TENANT_ID", "") or get_setting("ms_tenant_id", "")
    except Exception:
        _tenant = os.getenv("MS_TENANT_ID") or ""
    try:
        _client_id = os.getenv("MS_CLIENT_ID") or get_setting("MS_CLIENT_ID", "") or get_setting("ms_client_id", "")
    except Exception:
        _client_id = os.getenv("MS_CLIENT_ID") or ""
    try:
        _client_secret = os.getenv("MS_CLIENT_SECRET") or get_setting("MS_CLIENT_SECRET", "") or get_setting("ms_client_secret", "")
    except Exception:
        _client_secret = os.getenv("MS_CLIENT_SECRET") or ""

    if not to_addr:
        return "Missing recipient email"
    if not (_tenant and _client_id and _client_secret):
        return "Graph not configured. Set MS_TENANT_ID, MS_CLIENT_ID, MS_CLIENT_SECRET"
    if not sender:
        return "Missing sender mailbox. Set MS_SENDER_UPN or settings key ms_sender_upn"

    # Acquire app-only token
    try:
        token_r = requests.post(
            f"https://login.microsoftonline.com/{_tenant}/oauth2/v2.0/token",
            data={
                "client_id": _client_id,
                "client_secret": _client_secret,
                "scope": "https://graph.microsoft.com/.default",
                "grant_type": "client_credentials",
            },
            timeout=20,
        )
    except Exception as e:
        return f"Graph token exception: {e}"

    if token_r.status_code != 200:
        return f"Graph token error {token_r.status_code}: {token_r.text[:300]}"
    try:
        token = token_r.json().get("access_token")
    except Exception:
        token = None
    if not token:
        return f"Graph token error: {token_r.text[:300]}"

    # Build payload
    payload = {
        "message": {
            "subject": subject or "",
            "body": {"contentType": "Text", "content": body or ""},
            "toRecipients": [{"emailAddress": {"address": to_addr}}],
            "from": {"emailAddress": {"address": sender}},
        },
        "saveToSentItems": True,  # boolean must be used
    }

    send_url = f"https://graph.microsoft.com/v1.0/users/{quote_plus(sender)}/sendMail"
    try:
        r = requests.post(
            send_url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
    except Exception as e:
        return f"Graph send exception: {e}"

    if r.status_code in (200, 202):
        return "Sent"

    # surface helpful diagnostics
    try:
        err_json = r.json()
        err_txt = str(err_json)[:500]
    except Exception:
        err_txt = (r.text or "")[:500]
    return f"Graph send error {r.status_code}: {err_txt}"




# === Market pricing data helpers (robust) ===
def usaspending_search_awards(naics: str = "", psc: str = "", date_from: str = "", date_to: str = "", keyword: str = "", limit: int = 200, st_debug=None):
    import requests, pandas as pd, json
    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    type_codes = ["A","B","C","D"]
    def make_filters(n, p, k, start, end):
        f = {"time_period": [{"start_date": start, "end_date": end}], "award_type_codes": type_codes, "prime_or_sub": "prime_only"}
        if n: f["naics_codes"] = [n]
        if p: f["psc_codes"] = [p]
        if k: f["keywords"] = [k]
        return f
    if not date_from or not date_to:
        from datetime import datetime, timedelta
        end = datetime.utcnow().date().strftime("%Y-%m-%d")
        start = (datetime.utcnow().date() - timedelta(days=365*2)).strftime("%Y-%m-%d")
        date_from, date_to = date_from or start, date_to or end
    attempts = [("full", make_filters(naics, psc, keyword, date_from, date_to)),
                ("no_psc", make_filters(naics, "", keyword, date_from, date_to)),
                ("no_naics", make_filters("", psc, keyword, date_from, date_to)),
                ("keyword_only", make_filters("", "", keyword or "", date_from, date_to)),
                ("bare", make_filters("", "", "", date_from, date_to))]
    last_detail = ""
    for name, flt in attempts:
        payload = {"filters": flt, "fields": ["Award ID","Recipient Name","Start Date","End Date","Award Amount","Awarding Agency","NAICS Code","PSC Code"],
                   "page": 1, "limit": max(1, min(int(limit), 500)), "sort": "Award Amount", "order": "desc"}
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=30)
            status = r.status_code
            js = r.json() if status < 500 else {}
            rows = js.get("results", []) or []
            if rows:
                data = [{"award_id": it.get("Award ID"),
                         "recipient": it.get("Recipient Name"),
                         "start": it.get("Start Date"),
                         "end": it.get("End Date"),
                         "amount": it.get("Award Amount"),
                         "agency": it.get("Awarding Agency"),
                         "naics": it.get("NAICS Code"),
                         "psc": it.get("PSC Code")} for it in rows]
                diag = f"Attempt {name}: HTTP {status}, rows={len(rows)}"
                if st_debug is not None:
                    st_debug.code(json.dumps(payload, indent=2))
                    st_debug.caption(diag)
                return pd.DataFrame(data), diag
            else:
                last_detail = f"Attempt {name}: HTTP {status}, empty; message: {js.get('detail') or js.get('messages') or ''}"
        except Exception as e:
            last_detail = f"Attempt {name}: exception {e}"
    if st_debug is not None:
        st_debug.caption(last_detail)
    return pd.DataFrame(), last_detail

def summarize_award_prices(df):
    import numpy as np, pandas as pd
    if df is None or df.empty or "amount" not in df.columns: return {}
    vals = pd.to_numeric(df["amount"], errors="coerce").dropna()
    if vals.empty: return {}
    return {"count": int(vals.size), "min": float(vals.min()), "p25": float(np.percentile(vals,25)),
            "median": float(np.percentile(vals,50)), "p75": float(np.percentile(vals,75)),
            "max": float(vals.max()), "mean": float(vals.mean())}

def gsa_calc_rates(query: str, page: int = 1):
    import requests, pandas as pd
    url = "https://api.gsa.gov/technology/calc/search"
    params = {"q": query, "page": page}
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        js = r.json()
        items = js.get("results", []) or []
        rows = [{"vendor": it.get("vendor_name"), "labor_category": it.get("labor_category"),
                 "education": it.get("education_level"), "min_years_exp": it.get("min_years_experience"),
                 "hourly_ceiling": it.get("current_price"), "schedule": it.get("schedule"), "sin": it.get("sin")} for it in items]
        return pd.DataFrame(rows)
    except Exception:
        import pandas as pd
        return pd.DataFrame()


def _coerce_dt(x):
    if isinstance(x, datetime):
        return x
    try:
        y = _parse_sam_date(x)
        return y if isinstance(y, datetime) else None
    except Exception:
        return None

def sam_search(
    naics_list, min_days=3, limit=100, keyword=None, posted_from_days=30,
    notice_types="Combined Synopsis/Solicitation,Solicitation,Presolicitation,SRCSGT", active="true"
):
    if not SAM_API_KEY:
        return pd.DataFrame(), {"ok": False, "reason": "missing_key", "detail": "SAM_API_KEY is empty."}
    base = "https://api.sam.gov/opportunities/v2/search"
    today = datetime.utcnow().date()
    min_due_date = today + timedelta(days=min_days)
    posted_from = _us_date(today - timedelta(days=posted_from_days))
    posted_to   = _us_date(today)

    params = {
        "api_key": SAM_API_KEY,
        "limit": str(limit),
        "response": "json",
        "sort": "-publishedDate",
        "active": active,
        "postedFrom": posted_from,   # MM/dd/yyyy
        "postedTo": posted_to,       # MM/dd/yyyy
    }
    # Enforce only Solicitation + Combined when notice_types is blank
    if not notice_types:
        notice_types = "Combined Synopsis/Solicitation,Solicitation"
    params["noticeType"] = notice_types

    if naics_list:   params["naics"] = ",".join([c for c in naics_list if c][:20])
    if keyword:      params["keywords"] = keyword

    try:
        headers = {"X-Api-Key": SAM_API_KEY}
        r = requests.get(base, params=params, headers=headers, timeout=40)
        status = r.status_code
        raw_preview = (r.text or "")[:1000]
        try:
            data = r.json()
        except Exception:
            return pd.DataFrame(), {"ok": False, "reason": "bad_json", "status": status, "raw_preview": raw_preview, "detail": r.text[:800]}
        if status != 200:
            err_msg = ""
            if isinstance(data, dict):
                err_msg = data.get("message") or (data.get("error") or {}).get("message") or ""
            return pd.DataFrame(), {"ok": False, "reason": "http_error", "status": status, "message": err_msg, "detail": data, "raw_preview": raw_preview}
        if isinstance(data, dict) and data.get("message"):
            return pd.DataFrame(), {"ok": False, "reason": "api_message", "status": status, "detail": data.get("message"), "raw_preview": raw_preview}

        items = data.get("opportunitiesData", []) or []
        rows = []
        for opp in items:
            due_str = opp.get("responseDeadLine") or ""
            d = _parse_sam_date(due_str)
            d_dt = _coerce_dt(d)
            min_dt = _coerce_dt(min_due_date)
            if min_dt is None:
                due_ok = True  # allow when min date unknown
            else:
                due_ok = (d_dt is None) or (d_dt >= min_dt)
            if not due_ok: continue
            docs = opp.get("documents", []) or []
            rows.append({
                "sam_notice_id": opp.get("noticeId"),
                "title": opp.get("title"),
                "agency": opp.get("organizationName"),
                "naics": ",".join(opp.get("naicsCodes", [])),
                "psc": ",".join(opp.get("productOrServiceCodes", [])) if opp.get("productOrServiceCodes") else "",
                "place_of_performance": (opp.get("placeOfPerformance") or {}).get("city",""),
                "response_due": due_str,
                "posted": opp.get("publishedDate",""),
                "type": opp.get("type",""),
                "url": f"https://sam.gov/opp/{opp.get('noticeId')}/view",
                "attachments_json": json.dumps([{"name":d.get("fileName"),"url":d.get("url")} for d in docs])
            })
        df = pd.DataFrame(rows)
        info = {"ok": True, "status": status, "count": len(df), "raw_preview": raw_preview,
                "filters": {"naics": params.get("naics",""), "keyword": keyword or "",
                            "postedFrom": posted_from, "postedTo": posted_to,
                            "min_due_days": min_days, "noticeType": notice_types,
                            "active": active, "limit": limit}}
        if df.empty:
            info["hint"] = "Try min_days=0–1, add keyword, increase look-back, or clear noticeType."
        return df, info
    except requests.RequestException as e:
        return pd.DataFrame(), {"ok": False, "reason": "network", "detail": str(e)[:800]}



# ---- Hoisted helper implementations (duplicate for e# === SAM Watch → Contacts auto sync helpers ===

def _contacts_upsert(name: str = "", org: str = "", role: str = "", email: str = "", phone: str = "", source: str = "", notes: str = "") -> tuple:
    # Insert or light update into contacts.
    # Returns (action, id) where action is "insert" or "update".
    # Upsert rule prefers email match. If no email then uses name and org.
    try:
        conn = get_db(); cur = conn.cursor()
    except Exception:
        return ("error", None)

    email = (email or "").strip()
    name = (name or "").strip()
    org = (org or "").strip()
    role = (role or "").strip()
    phone = (phone or "").strip()
    source = (source or "SAM.gov").strip() or "SAM.gov"
    notes = (notes or "").strip()

    row = None
    try:
        if email:
            row = cur.execute("select id from contacts where lower(ifnull(email,'')) = lower(?) limit 1", (email,)).fetchone()
        if not row and (name and org):
            row = cur.execute("select id from contacts where lower(ifnull(name,''))=lower(?) and lower(ifnull(org,''))=lower(?) limit 1", (name, org)).fetchone()
    except Exception:
        row = None

    if row:
        cid = int(row[0])
        try:
            cur.execute(
                "update contacts set name=coalesce(nullif(?, ''), name), org=coalesce(nullif(?, ''), org), role=coalesce(nullif(?, ''), role), email=coalesce(nullif(?, ''), email), phone=coalesce(nullif(?, ''), phone), source=coalesce(nullif(?, ''), source), notes=case when ifnull(notes,'')='' then ? else notes end where id=?",
                (name, org, role, email, phone, source, notes, cid)
            )
            conn.commit()
        except Exception:
            pass
        return ("update", cid)

    try:
        cur.execute(
            "insert into contacts(name, org, role, email, phone, source, notes) values(?,?,?,?,?,?,?)",
            (name, org, role, email, phone, source, notes)
        )
        conn.commit()
        return ("insert", cur.lastrowid)
    except Exception:
        return ("error", None)


def _extract_contacts_from_sam_row(r) -> list:
    # Best effort extraction of POC and CO from a SAM Watch DataFrame row.
    # Returns list of dicts suitable for _contacts_upsert.
    def _g(keys):
        for k in keys:
            try:
                v = r.get(k)
            except Exception:
                v = None
            if v not in (None, float("nan")):
                s = str(v).strip()
                if s:
                    return s
        return ""

    import re
    agency = _g(["agency", "office", "department", "organization"]) or ""

    poc_name = _g(["poc_name", "primary_poc_name", "pointOfContact", "primaryPointOfContact", "contact_name"]) or ""
    poc_email = _g(["poc_email", "primary_poc_email", "pointOfContactEmail", "contact_email"]) or ""
    poc_phone = _g(["poc_phone", "primary_poc_phone", "pointOfContactPhone", "contact_phone"]) or ""

    co_name = _g(["co_name", "contracting_officer", "contractingOfficer", "buyer_name"]) or ""
    co_email = _g(["co_email", "contracting_officer_email", "buyer_email"]) or ""
    co_phone = _g(["co_phone", "contracting_officer_phone", "buyer_phone"]) or ""

    blob = _g(["description", "summary", "text", "body"]) or ""
    emails = []
    if blob:
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", blob)

    out = []
    if poc_email or poc_name or poc_phone:
        out.append({"name": poc_name, "org": agency, "role": "POC", "email": poc_email, "phone": poc_phone, "source": "SAM.gov"})
    if co_email or co_name or co_phone:
        out.append({"name": co_name, "org": agency, "role": "CO", "email": co_email, "phone": co_phone, "source": "SAM.gov"})

    if not any(c.get("email") for c in out) and emails:
        out.append({"name": "", "org": agency, "role": "POC", "email": emails[0], "phone": "", "source": "SAM.gov", "notes": "from description"})

    seen = set(); dedup = []
    for c in out:
        key = (c.get("email") or c.get("name"), c.get("org"))
        if key in seen:
            continue
        seen.add(key); dedup.append(c)
    return dedup


# (early use) ----
def google_places_search(query, location="Houston, TX", radius_m=80000, strict=True):
    """
    Google Places Text Search + Details (phone + website).
    Returns (list_of_vendors, info). Emails are NOT provided by Places.
    """
    if not GOOGLE_PLACES_KEY:
        return [], {"ok": False, "reason": "missing_key", "detail": "GOOGLE_PLACES_API_KEY is empty."}
    try:
        # 1) Text Search
        search_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        search_params = {"query": f"{query} {location}", "radius": radius_m, "key": GOOGLE_PLACES_KEY}
        rs = requests.get(search_url, params=search_params, timeout=25)
        status_code = rs.status_code
        data = rs.json() if rs.headers.get("Content-Type","").startswith("application/json") else {}
        api_status = data.get("status","")
        results = data.get("results", []) or []

        if status_code != 200 or api_status not in ("OK","ZERO_RESULTS"):
            return ([] if strict else results), {
                "ok": False, "reason": api_status or "http_error", "http": status_code,
                "api_status": api_status, "count": len(results),
                "raw_preview": (rs.text or "")[:800],
                "note": "Enable billing + 'Places API' in Google Cloud."
            }

        # 2) Details per result
        out = []
        for item in results:
            place_id = item.get("place_id")
            phone, website = "", ""
            if place_id:
                det_url = "https://maps.googleapis.com/maps/api/place/details/json"
                det_params = {"place_id": place_id, "fields": "formatted_phone_number,website", "key": GOOGLE_PLACES_KEY}
                rd = requests.get(det_url, params=det_params, timeout=20)
                det_json = rd.json() if rd.headers.get("Content-Type","").startswith("application/json") else {}
                det = det_json.get("result", {})
                phone = det.get("formatted_phone_number", "") or ""
                website = det.get("website", "") or ""

            out.append({
                "company": item.get("name"),
                "naics": "",
                "trades": "",
                "phone": phone,
                "email": "",  # Emails not provided by Google Places
                "website": website,
                "city": location.split(",")[0].strip() if "," in location else location,
                "state": location.split(",")[-1].strip() if "," in location else "",
                "certifications": "",
                "set_asides": "",
                "notes": item.get("formatted_address",""),
                "source": "GooglePlaces",
            })
        info = {"ok": True, "count": len(out), "http": status_code, "api_status": api_status,
                "raw_preview": (rs.text or "")[:800]}
        return out, info
    except Exception as e:
        return [], {"ok": False, "reason": "exception", "detail": str(e)[:500]}

def linkedin_company_search(keyword: str) -> str:
    return f"https://www.linkedin.com/search/results/companies/?keywords={quote_plus(keyword)}"

def build_context(max_rows=6):
    conn = get_db()
    g = pd.read_sql_query("select * from goals limit 1", conn)
    goals_line = ""
    if not g.empty:
        rr = g.iloc[0]
        goals_line = (f"Bids target {int(rr['bids_target'])}, submitted {int(rr['bids_submitted'])}; "
                      f"Revenue target ${float(rr['revenue_target']):,.0f}, won ${float(rr['revenue_won']):,.0f}.")
    codes = pd.read_sql_query("select code from naics_watch order by code", conn)["code"].tolist()
    naics_line = ", ".join(codes[:20]) + (" …" if len(codes) > 20 else "") if codes else "none"
    opp = pd.read_sql_query(
        "select title, agency, naics, response_due from opportunities order by posted desc limit ?",
        conn, params=(max_rows,)
    )
    opp_lines = ["- " + " | ".join(filter(None, [
        str(r["title"])[:80], str(r["agency"])[:40],
        f"due {str(r['response_due'])[:16]}", f"NAICS {str(r['naics'])[:18]}",
    ])) for _, r in opp.iterrows()]
    vend = pd.read_sql_query(
        """select trim(substr(naics,1,6)) as code, count(*) as cnt
           from vendors where ifnull(naics,'')<>''
           group by trim(substr(naics,1,6)) order by cnt desc limit ?""",
        conn, params=(max_rows,)
    )
    vend_lines = [f"- {r['code']}: {int(r['cnt'])} vendors" for _, r in vend.iterrows()]
    return "\n".join([
        f"Company: {get_setting('company_name','ELA Management LLC')}",
        f"Home location: {get_setting('home_loc','Houston, TX')}",
        f"Goals: {goals_line or 'not set'}",
        f"NAICS watch: {naics_line}",
        "Recent opportunities:" if not opp.empty else "Recent opportunities: (none)",
        *opp_lines,
        "Vendor coverage (top NAICS):" if not vend.empty else "Vendor coverage: (none)",
        *vend_lines,
    ])

# ---------- External integrations ----------



# ---- Safety helpers (fallbacks to avoid NameError at first render) ----
try:
    _ = linkedin_company_search
except NameError:
    def linkedin_company_search(q: str) -> str:
        return f"https://www.linkedin.com/search/results/companies/?keywords={quote_plus(q)}"


try:
    _ = google_places_search
except NameError:
    def google_places_search(*args, **kwargs):
        """
        Fallback stub when real google_places_search isn't loaded yet.
        Accepts flexible signatures, e.g. (query, location, radius_meters).
        Returns (results, info) where results is a list and info is a dict.
        """
        try:
            query = args[0] if len(args) >= 1 else kwargs.get("query","")
            loc = args[1] if len(args) >= 2 else kwargs.get("location","")
            radius_m = args[2] if len(args) >= 3 else kwargs.get("radius_meters", 1609)
        except Exception:
            query, loc, radius_m = "", "", 1609
        url = f"https://www.google.com/maps/search/{quote_plus(str(query)+' '+str(loc))}"
        # Provide an empty result set and metadata so callers expecting tuple unpacking won't crash
        return [], {"url": url, "note": "Fallback google_places_search stub used", "radius_m": radius_m}

try:
    _ = build_context
except NameError:
    def build_context(max_rows: int = 6) -> str:
        return ""

st.title("GovCon Copilot Pro")

# Data health card
try:
    render_data_health_card()
    render_env_switcher()
except Exception:
    pass

# Health card
try:
    render_health_card()
except Exception:
    pass

def _render_identity_chip():
    try:
        conn = get_db()
        import streamlit as st
        uid = st.session_state.get("user_id")
        oid = st.session_state.get("org_id")
        uname = None
        role = None
        oname = None
        if uid:
            r = conn.execute("SELECT display_name, role FROM users WHERE id=?", (uid,)).fetchone()
            if r: uname, role = r[0], r[1]
        if oid:
            r = conn.execute("SELECT name FROM orgs WHERE id=?", (oid,)).fetchone()
            if r: oname = r[0]
        if oname or uname:
            c1, c2, c3 = st.columns([0.6,0.2,0.2])
            with c3:
                st.caption(f"Org: {oname or 'unknown'}  •  User: {uname or 'unknown'}  •  Role: {role or 'unknown'}")
    except Exception as _ex:
        import streamlit as st
        st.caption("identity: n/a")
_render_identity_chip()
st.caption("SubK sourcing • SAM watcher • proposals • outreach • CRM • goals • chat with memory & file uploads")
DB_PATH = "data/app.db"

NAICS_SEEDS = [
    "561210","721110","562991","326191","336611","531120","531","722310","561990","722514","561612",
    "561730","311511","238990","311812","561720","811210","236118","238220","237990","311423",
    "562910","236220","332420","238320","541380","541519","561710","423730","238210","562211",
    "541214","541330","541512","541511","541370","611430","611699","611310","611710","562111","562119",
    "624230","488999","485510","485410","488510","541614","332994","334220","336992","561320","561311","541214"
]

SCHEMA = {
    "vendors": """
    create table if not exists vendors (
        id integer primary key,
        company text, naics text, trades text, phone text, email text, website text,
        city text, state text, certifications text, set_asides text, notes text, source text,
        created_at text default current_timestamp, updated_at text default current_timestamp
    );
    """,
    "opportunities": """
    # ===== app.py =====    st.session_state.setdefault('deals_refresh', 0)


def _strip_markdown_to_plain(txt: str) -> str:
    """
    Remove common Markdown markers so exported DOCX shows clean text instead of 'coded' look.
    """
    if not txt:
        return ""
    import re as _re
    s = txt
    # Remove code fences but keep inner text
    s = _re.sub(r"```(.*?)```", r"\1", s, flags=_re.DOTALL)
    # Inline code backticks
    s = s.replace("`", "")
    # Bold/italic markers
    s = s.replace("***", "")
    s = s.replace("**", "")
    s = s.replace("*", "")
    s = s.replace("__", "")
    s = s.replace("_", "")
    # Strip heading markers at line starts
    s = _re.sub(r"^[ \t]*#{1,6}[ \t]*", "", s, flags=_re.MULTILINE)
    # Strip blockquote markers
    s = _re.sub(r"^[ \t]*>[ \t]?", "", s, flags=_re.MULTILINE)
    # Remove list markers
    s = _re.sub(r"^[ \t]*([-*•]|\d+\.)[ \t]+", "", s, flags=_re.MULTILINE)
    # Remove table pipes (keep content)
    s = _re.sub(r"^\|", "", s, flags=_re.MULTILINE)
    s = _re.sub(r"\|$", "", s, flags=_re.MULTILINE)
    return s

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
            try:
                size = os.path.getsize(full)
            except Exception:
                size = 0
            items.append({"name": f, "path": full, "size": size})
    return list(reversed(items))  # newest first

def load_proposal_draft(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""

def delete_proposal_draft(path: str) -> bool:
    try:
        os.remove(path)
        return True
    except Exception:
        return False
# ===== end Proposal drafts utilities =====


def md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                     margins_in: float = 1.0, logo_bytes: bytes = None, logo_width_in: float = 1.5) -> bytes:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import io
    doc = Document()
    try:
        md_text = _clean_placeholders(md_text)
    except Exception:
        pass
        pass
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if logo_bytes:
        p_center = doc.add_paragraph(); p_center.paragraph_format.alignment = 1
        run = p_center.add_run()
        try:
            from docx.shared import Inches as _Inches
            run.add_picture(io.BytesIO(logo_bytes), width=_Inches(logo_width_in))
        except Exception:
            pass
    if title:
        h = doc.add_heading(title, level=1)
        try: h.style = doc.styles["Heading 1"]
        except Exception: pass
    _render_markdown_to_docx(doc, md_text)
    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()


def _md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                      margins_in: float = 1.0) -> bytes:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import io
    doc = Document()
    try:
        md_text = _clean_placeholders(md_text)
    except Exception:
        pass
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if title:
        h = doc.add_heading(title, level=1)
        try: h.style = doc.styles["Heading 1"]
        except Exception: pass
    _render_markdown_to_docx(doc, md_text)
    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()


# ===== Improved Markdown rendering helpers =====
def _add_hr_paragraph(doc):
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    p = doc.add_paragraph()
    pPr = p._p.get_or_add_pPr()
    pBdr = OxmlElement('w:pBdr')
    bottom = OxmlElement('w:bottom')
    bottom.set(qn('w:val'), 'single')
    bottom.set(qn('w:sz'), '6')
    bottom.set(qn('w:space'), '1')
    bottom.set(qn('w:color'), 'auto')
    pBdr.append(bottom)
    pPr.append(pBdr)
    return p

def _add_paragraph_with_inlines(doc, text, style=None):
    # Supports **bold**, *italic* inline
    import re as _re
    p = doc.add_paragraph()
    if style:
        try:
            p.style = doc.styles[style]
        except Exception:
            pass

    # Tokenize **bold** and *italic*
    tokens = []
    parts = _re.split(r'(\*\*[^\*]+\*\*)', text or '')
    for part in parts:
        if part.startswith('**') and part.endswith('**') and len(part) >= 4:
            tokens.append(('bold', part[2:-2]))
        else:
            subparts = _re.split(r'(\*[^\*]+\*)', part)
            for sp in subparts:
                if sp.startswith('*') and sp.endswith('*') and len(sp) >= 2:
                    tokens.append(('italic', sp[1:-1]))
                else:
                    tokens.append(('text', sp))

    for kind, chunk in tokens:
        if not chunk:
            continue
        run = p.add_run(chunk)
        if kind == 'bold':
            run.bold = True
        elif kind == 'italic':
            run.italic = True
    return p

def _render_markdown_to_docx(doc, md_text):
    import re as _re
    lines = (md_text or '').splitlines()
    bullet_buf, num_buf = [], []

    def flush_bullets():
        nonlocal bullet_buf
        for item in bullet_buf:
            _add_paragraph_with_inlines(doc, item, style="List Bullet")
        bullet_buf = []

    def flush_numbers():
        nonlocal num_buf
        for item in num_buf:
            _add_paragraph_with_inlines(doc, item, style="List Number")
        num_buf = []

    for raw in lines:
        line = (raw or '').rstrip()

        # Horizontal rule ---
        if _re.match(r'^\s*-{3,}\s*$', line):
            flush_bullets(); flush_numbers()
            _add_hr_paragraph(doc)
            continue

        # Blank -> flush lists and add spacer
        if not line.strip():
            flush_bullets(); flush_numbers()
            doc.add_paragraph("")
            continue

        # Headings (tolerate up to 3 leading spaces)
        m = _re.match(r'^\s{0,3}(#{1,6})\s+(.*)$', line)
        if m:
            flush_bullets(); flush_numbers()
            hashes, text = m.group(1), m.group(2).strip()
            level = min(len(hashes), 6)
            try:
                doc.add_heading(text, level=level)
            except Exception:
                _add_paragraph_with_inlines(doc, text)
            continue

        # Bullets: -, *, •
        if _re.match(r'^\s*(\-|\*|•)\s+', line):
            flush_numbers()
            bullet_buf.append(_re.sub(r'^\s*(\-|\*|•)\s+', '', line, count=1))
            continue

        # Numbered: 1. text
        if _re.match(r'^\s*\d+\.\s+', line):
            flush_bullets()
            num_buf.append(_re.sub(r'^\s*\d+\.\s+', '', line, count=1))
            continue

        # Normal paragraph with inline formatting
        flush_bullets(); flush_numbers()
        _add_paragraph_with_inlines(doc, line)

    flush_bullets(); flush_numbers()


def md_to_docx_bytes_rich(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                          margins_in: float = 1.0, logo_bytes: bytes = None, logo_width_in: float = 1.5) -> bytes:
    """
    Guaranteed rich Markdown→DOCX converter with inline bold/italics, headings, lists, and horizontal rules.
    """
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import io
    doc = Document()
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if logo_bytes:
        p_center = doc.add_paragraph(); p_center.paragraph_format.alignment = 1
        run = p_center.add_run()
        try:
            run.add_picture(io.BytesIO(logo_bytes), width=Inches(logo_width_in))
        except Exception:
            pass
    if title:
        h = doc.add_heading(title, level=1)
        try: h.style = doc.styles["Heading 1"]
        except Exception: pass

    _render_markdown_to_docx(doc, md_text)

    out = io.BytesIO()
    doc.save(out)
    out.seek(0)
    return out.getvalue()

# ===== end Improved Markdown rendering helpers =====


# ===== DOCX helpers (loaded early so they're available to all tabs) =====
def _md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                      margins_in: float = 1.0) -> bytes:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import re as _re, io
    doc = Document()
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if title:
        h = doc.add_heading(title, level=1)
        try:
            h.style = doc.styles["Heading 1"]
        except Exception:
            pass
    lines = (md_text or "").splitlines()
    bullet_buf, num_buf = [], []
    def flush_bullets():
        nonlocal bullet_buf
        for item in bullet_buf:
            p = doc.add_paragraph(item)
            try: p.style = doc.styles["List Bullet"]
            except Exception: pass
        bullet_buf = []
    def flush_numbers():
        nonlocal num_buf
        for item in num_buf:
            p = doc.add_paragraph(item)
            try: p.style = doc.styles["List Number"]
            except Exception: pass
        num_buf = []
    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            flush_bullets(); flush_numbers(); doc.add_paragraph(""); continue
        if line.startswith("### "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[4:].strip(), level=3); continue
        if line.startswith("## "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[3:].strip(), level=2); continue
        if line.startswith("# "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[2:].strip(), level=1); continue
        if _re.match(r"^(\-|\*|•)\s+", line):
            flush_numbers(); bullet_buf.append(_re.sub(r"^(\-|\*|•)\s+", "", line, count=1)); continue
        if _re.match(r"^\d+\.\s+", line):
            flush_bullets(); num_buf.append(_re.sub(r"^\d+\.\s+", "", line, count=1)); continue
        flush_bullets(); flush_numbers(); doc.add_paragraph(line)
    flush_bullets(); flush_numbers()
    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()

def md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                     margins_in: float = 1.0, logo_bytes: bytes = None, logo_width_in: float = 1.5) -> bytes:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import re as _re, io
    doc = Document()
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if logo_bytes:
        p_center = doc.add_paragraph(); p_center.paragraph_format.alignment = 1
        run = p_center.add_run()
        try: run.add_picture(io.BytesIO(logo_bytes), width=Inches(logo_width_in))
        except Exception: pass
    if title:
        h = doc.add_heading(title, level=1)
        try: h.style = doc.styles["Heading 1"]
        except Exception: pass
    lines = (md_text or "").splitlines()
    bullet_buf, num_buf = [], []
    def flush_bullets():
        nonlocal bullet_buf
        for item in bullet_buf:
            p = doc.add_paragraph(item)
            try: p.style = doc.styles["List Bullet"]
            except Exception: pass
        bullet_buf = []
    def flush_numbers():
        nonlocal num_buf
        for item in num_buf:
            p = doc.add_paragraph(item)
            try: p.style = doc.styles["List Number"]
            except Exception: pass
        num_buf = []
    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            flush_bullets(); flush_numbers(); doc.add_paragraph(""); continue
        if line.startswith("### "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[4:].strip(), level=3); continue
        if line.startswith("## "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[3:].strip(), level=2); continue
        if line.startswith("# "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[2:].strip(), level=1); continue
        if _re.match(r"^(\-|\*|•)\s+", line):
            flush_numbers(); bullet_buf.append(_re.sub(r"^(\-|\*|•)\s+", "", line, count=1)); continue
        if _re.match(r"^\d+\.\s+", line):
            flush_bullets(); num_buf.append(_re.sub(r"^\d+\.\s+", "", line, count=1)); continue
        flush_bullets(); flush_numbers(); doc.add_paragraph(line)
    flush_bullets(); flush_numbers()
    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()
# ===== end DOCX helpers =====


import pandas as pd
import numpy as np
import streamlit as st








# === SAFE RERUN HELPER START ===
def _safe_rerun():
    import streamlit as st
    try:
        # Streamlit >= 1.30
        st.rerun()
    except Exception:
        try:
            # Older Streamlit
            _safe_rerun()
        except Exception:
            # As a last resort stop, which triggers a rerun on next interaction
            st.stop()
# === SAFE RERUN HELPER END ===


# === CORE DB EARLY START ===
import os as _os
_os.makedirs('data', exist_ok=True)

@st.cache_resource
def get_db():
    import sqlite3
    conn = sqlite3.connect('data/app.db', check_same_thread=False, isolation_level=None)
    try:
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA synchronous=NORMAL;')
        conn.execute('PRAGMA temp_store=MEMORY;')
        conn.execute('PRAGMA foreign_keys=ON;')
        conn.execute('CREATE TABLE IF NOT EXISTS migrations(id INTEGER PRIMARY KEY, name TEXT UNIQUE, applied_at TEXT NOT NULL);')
    except Exception:
        pass
    return conn
# === CORE DB EARLY END ===

# === TENANCY EARLY BOOTSTRAP START ===
def _tenancy_phase1_bootstrap():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS orgs(
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL
        );""")
        cur.execute("""CREATE TABLE IF NOT EXISTS users(
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
            email TEXT NOT NULL UNIQUE,
            display_name TEXT,
            role TEXT NOT NULL CHECK(role IN('Admin','Member','Viewer')),
            created_at TEXT NOT NULL
        );""")
        # Seed a default org and 3 users if empty
        row = cur.execute("SELECT COUNT(*) FROM orgs").fetchone()
        if row and row[0] == 0:
            cur.execute("INSERT OR IGNORE INTO orgs(id, name, created_at) VALUES(?,?,datetime('now'))", ('org-default','Default Org'))
        # Ensure at least one user exists for the default org
        rowu = cur.execute("SELECT COUNT(*) FROM users").fetchone()
        if rowu and rowu[0] == 0:
            users = [
                ('user-quincy','org-default','quincy@example.com','Quincy','Admin'),
                ('user-collin','org-default','collin@example.com','Collin','Member'),
                ('user-charles','org-default','charles@example.com','Charles','Viewer'),
            ]
            for uid, oid, email, name, role in users:
                cur.execute("INSERT OR IGNORE INTO users(id, org_id, email, display_name, role, created_at) VALUES(?,?,?,?,?,datetime('now'))",
                            (uid, oid, email, name, role))
        conn.commit()
    except Exception as ex:
        # Do not break startup on bootstrap failure
        try: log_json('error', 'tenancy_bootstrap_failed', error=str(ex))
        except Exception: pass

try:
    _tenancy_phase1_bootstrap()
except Exception:
    pass
# === TENANCY EARLY BOOTSTRAP END ===


# === EARLY DB BOOTSTRAP START ===
# Ensure get_db exists before any import-time calls.
# This early definition will be overridden by later phases if they redefine get_db.
import os as _os
_os.makedirs("data", exist_ok=True)

if "get_db" not in globals():
    import streamlit as st
    @st.cache_resource
    def get_db():
        import sqlite3
        conn = sqlite3.connect("data/app.db", check_same_thread=False, isolation_level=None)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("CREATE TABLE IF NOT EXISTS migrations(id INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL);")
        except Exception:
            pass
        return conn
# === EARLY DB BOOTSTRAP END ===


# === PHASE 0 CORE START ===
# Bootstrap: feature flags, API client, SQLite PRAGMAs, secrets loader, structured logging.
import time as _time
import json as _json
import uuid as _uuid
import contextlib as _contextlib
from typing import Any as _Any, Dict as _Dict, Optional as _Optional

import streamlit as st

# ---- Structured logging ----
def log_json(level: str, message: str, **context) -> str:
    """Emit a single line JSON log. Returns error_id for error levels."""
    event_id = str(_uuid.uuid4())
    payload = {
        "ts": int(_time.time()),
        "level": level.upper(),
        "event_id": event_id,
        "message": message,
        "context": {k: v for k, v in context.items()},
    }
    try:
        print(_json.dumps(payload, ensure_ascii=False))
    except Exception:
        # Ensure logging never breaks app
        print(str(payload))
    return event_id if level.lower() in {"error","fatal","critical"} else event_id

# ---- Secrets loader ----
def get_secret(section: str, key: str, default: _Optional[str]=None) -> _Optional[str]:
    """Safe secrets accessor. Does not raise or leak values in logs."""
    try:
        sec = st.secrets.get(section)  # type: ignore[attr-defined]
        if sec is None:
            return default
        val = sec.get(key)
        return val if val is not None else default
    except Exception:
        return default

# ---- Feature flags ----
, "deals_core"]
def init_feature_flags():
    flags = st.session_state.setdefault("feature_flags", {})
    # Do not remove existing keys. Only set missing to False.
    for k in _FEATURE_KEYS:
        flags.setdefault(k, False)
    # Preexisting flags like 'workspace_enabled' preserved as-is.
    return flags

# ---- SQLite PRAGMAs and migrations ----
def _apply_sqlite_pragmas(conn):
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA temp_store=MEMORY;")
        cur.execute("PRAGMA foreign_keys=ON;")
    except Exception as ex:
        log_json("error", "sqlite_pragmas_failed", error=str(ex))

def _ensure_migrations_table(conn):
    try:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS migrations(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            applied_at TEXT NOT NULL
        );""")
    except Exception as ex:
        log_json("error", "migrations_table_create_failed", error=str(ex))

def ensure_bootstrap_db():
    try:
        conn = get_db()  # Provided by later phases. Cached.
        _apply_sqlite_pragmas(conn)
        _ensure_migrations_table(conn)
        return True
    except Exception as ex:
        log_json("error", "bootstrap_db_failed", error=str(ex))
        return False

# ---- Central API client ----
class CircuitOpenError(Exception):
    pass

def create_api_client(base_url: str, api_key: _Optional[str]=None, timeout: int=20, retries: int=3, ttl: int=900):
    """Return a simple client with GET/POST. GET responses cached for 'ttl' seconds."""
    import requests  # local import to avoid hard dependency if unused

    # Circuit breaker state stored in session
    cb = st.session_state.setdefault("_api_cb", {})
    key = f"cb::{base_url}"
    state = cb.setdefault(key, {"fail_count": 0, "opened_at": 0.0})

    def _check_circuit():
        now = _time.time()
        if state["fail_count"] >= 3:
            # Circuit open for 60 seconds from last open
            if now - state["opened_at"] < 60.0:
                raise CircuitOpenError("circuit_open")
            else:
                # half-open: allow a try
                pass

    def _mark_success():
        state["fail_count"] = 0
        state["opened_at"] = 0.0

    def _mark_failure():
        state["fail_count"] += 1
        if state["fail_count"] >= 3:
            state["opened_at"] = _time.time()

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    if api_key:
        session.headers.update({"Authorization": f"Bearer {api_key}"})

    def _backoff(attempt):
        # exponential backoff: 0.25, 0.5, 1, 2 ...
        delay = min(2.0 ** max(0, attempt - 1) * 0.25, 4.0)
        _time.sleep(delay)

    @st.cache_data(ttl=900, show_spinner=False)
    def _cached_get(cache_key: str):
        # cache layer isolated by cache_key
        # Actual HTTP performed outside to pick up dynamic ttl via caller
        return cache_key

    def _http_get(path: str, params: _Optional[_Dict]=None):
        _check_circuit()
        url = base_url.rstrip("/") + "/" + path.lstrip("/")
        # build a deterministic cache key
        key_parts = [url]
        if params:
            # stable sort
            key_parts.extend([f"{k}={params[k]}" for k in sorted(params.keys())])
        cache_key = "|".join(key_parts)
        # read cache token first
        token = _cached_get(cache_key) if ttl else None  # token content unused, just gate by key+ttl
        last_err = None
        for attempt in range(1, max(1, retries) + 1):
            try:
                resp = session.get(url, params=params, timeout=timeout)
                if 200 <= resp.status_code < 300:
                    _mark_success()
                    # store body alongside token by returning it directly
                    return resp.json() if "application/json" in resp.headers.get("Content-Type","") else resp.text
                last_err = f"status={resp.status_code}"
            except CircuitOpenError:
                raise
            except Exception as ex:
                last_err = str(ex)
            _mark_failure()
            if attempt < retries:
                _backoff(attempt)
        # If we got here, open circuit
        _mark_failure()
        state["opened_at"] = _time.time()
        eid = log_json("error", "api_get_failed", url=url, error=last_err)
        raise RuntimeError(f"API GET failed. error_id={eid}")

    def _http_post(path: str, json: _Optional[_Dict]=None):
        _check_circuit()
        url = base_url.rstrip("/") + "/" + path.lstrip("/")
        last_err = None
        for attempt in range(1, max(1, retries) + 1):
            try:
                resp = session.post(url, json=json, timeout=timeout)
                if 200 <= resp.status_code < 300:
                    _mark_success()
                    return resp.json() if "application/json" in resp.headers.get("Content-Type","") else resp.text
                last_err = f"status={resp.status_code}"
            except CircuitOpenError:
                raise
            except Exception as ex:
                last_err = str(ex)
            _mark_failure()
            if attempt < retries:
                _backoff(attempt)
        _mark_failure()
        state["opened_at"] = _time.time()
        eid = log_json("error", "api_post_failed", url=url, error=last_err)
        raise RuntimeError(f"API POST failed. error_id={eid}")

    return {
        "get": _http_get,
        "post": _http_post,
        "base_url": base_url,
        "timeout": timeout,
        "retries": retries,
        "ttl": ttl,
    }

def _ensure_api_factory():
    if "api_client_factory" not in st.session_state:
        st.session_state["api_client_factory"] = create_api_client
    return st.session_state["api_client_factory"]

# ---- Bootstrap runner ----
def _phase0_bootstrap():
    # Initialize feature flags first
    init_feature_flags()
    # Ensure DB PRAGMAs and migrations table
    with _contextlib.suppress(Exception):
        ensure_bootstrap_db()
    # Register API client factory
    _ensure_api_factory()
    st.session_state.setdefault("boot_done", True)

# Run at import time, safe to fail silently
with _contextlib.suppress(Exception):
    _phase0_bootstrap()
# === PHASE 0 CORE END ===

# === LAYOUT PHASE 1 START ===

# Router, query params, shell nav, and feature flags.
# All new code under feature_flags['workspace_enabled'] == False by default.

import contextlib

# Feature flags stored in session_state to persist within a session
def _ensure_feature_flags():
    import streamlit as st
    if "feature_flags" not in st.session_state:
        st.session_state["feature_flags"] = {"workspace_enabled": False}
    # Ensure key exists even if older sessions exist
    if "workspace_enabled" not in st.session_state["feature_flags"]:
        st.session_state["feature_flags"]["workspace_enabled"] = False
    return st.session_state["feature_flags"]

def feature_flags():
    return _ensure_feature_flags()

# Query param helpers with Streamlit compatibility
def _qp_get():
    import streamlit as st
    with contextlib.suppress(Exception):
        # Newer Streamlit
        qp = getattr(st, "query_params", None)
        if qp is not None:
            # st.query_params behaves like a dict[str, str]
            return dict(qp)
    # Fallback to experimental API which returns dict[str, list[str]]
    with contextlib.suppress(Exception):
        data = st.experimental_get_query_params()
        norm = {k: (v[0] if isinstance(v, list) and v else v) for k, v in data.items()}
        return norm
    return {}

def _qp_set(**kwargs):
    import streamlit as st
    # Remove keys with None to avoid clutter
    clean = {k: v for k, v in kwargs.items() if v is not None}
    # Try new API first
    with contextlib.suppress(Exception):
        qp = getattr(st, "query_params", None)
        if qp is not None:
            qp.clear()
            for k, v in clean.items():
                qp[k] = str(v)
            return
    # Fallback
    with contextlib.suppress(Exception):
        st.experimental_set_query_params(**clean)

def get_route():
    import streamlit as st
    qp = _qp_get()
    page = qp.get("page") or "dashboard"
    opp = qp.get("opp")
    tab = qp.get("tab")
    # normalize opp id to int when possible
    try:
        opp_id = int(opp) if opp is not None and str(opp).isdigit() else None
    except Exception:
        opp_id = None
    st.session_state["route_page"] = page
    st.session_state["route_opp_id"] = opp_id
    st.session_state["route_tab"] = tab
    return {"page": page, "opp_id": opp_id, "tab": tab}

def route_to(page, opp_id=None, tab=None, replace=False):
    import streamlit as st
    # Update session state
    st.session_state["route_page"] = page
    st.session_state["route_opp_id"] = opp_id
    st.session_state["route_tab"] = tab
    # Update URL query params
    _qp_set(page=page, opp=(opp_id if opp_id is not None else None), tab=(tab if tab else None))

def _get_notice_title_from_db(opp_id):
    # Best effort lookup. Works even if schema differs.
    # Falls back to "Opportunity <id>"
    if opp_id is None:
        return "Opportunity"
    title = None
    try:
        conn = get_db()  # uses existing cached connection
        cur = conn.cursor()
        # Check candidate tables and columns
        candidates = [
            ("notices", ["title", "notice_title", "name", "subject"]),
            ("opportunities", ["title", "name", "subject"]),
        ]
        for table, cols in candidates:
            # Does the table exist
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,))
            row = cur.fetchone()
            if not row:
                continue
            # Find a valid column
            cur.execute(f"PRAGMA table_info({table})")
            cols_present = [r[1] for r in cur.fetchall()]
            use_col = next((c for c in cols if c in cols_present), None)
            if not use_col:
                continue
            cur.execute(f"SELECT {use_col} FROM {table} WHERE id=?", (opp_id,))
            r = cur.fetchone()
            if r and r[0]:
                title = str(r[0])
                break
    except Exception:
        title = None
    return title or f"Opportunity {opp_id}"

def _render_top_nav():
    import streamlit as st
    ff = feature_flags()
    if not ff.get("workspace_enabled", False):
        return
    pages = [
        ("dashboard", "Dashboard"),
        ("sam", "SAM Watch"),
        ("pipeline", "Pipeline"),
        ("outreach", "Outreach"),
        ("library", "Library"),
        ("admin", "Admin"),
    ]
    st.markdown("### Navigation")
    cols = st.columns(len(pages))
    route = get_route()
    for i, (pid, label) in enumerate(pages):
        with cols[i]:
            if st.button(label, use_container_width=True):
                route_to(pid)

def _render_opportunity_workspace():
    import streamlit as st
    ff = feature_flags()
    if not ff.get("workspace_enabled", False):
        return
    route = get_route()
    if route.get("page") != "opportunity":
        return
    opp_id = route.get("opp_id")
    title = _get_notice_title_from_db(opp_id)
    st.header(title)
    # Subtab bar as segmented control substitute
    tabs = ["overview", "documents", "proposal", "team"]
    labels = ["Overview", "Documents", "Proposal", "Team"]
    current = route.get("tab") or "overview"
    # Ensure valid
    if current not in tabs:
        current = "overview"
    idx = tabs.index(current)
    try:
        selected = st.radio("Workspace", options=list(range(len(tabs))), index=idx, format_func=lambda i: labels[i], horizontal=True)
    except TypeError:
        # Streamlit < 1.29 does not have horizontal
        selected = st.radio("Workspace", options=list(range(len(tabs))), index=idx, format_func=lambda i: labels[i])
    if tabs[selected] != current:
        route_to("opportunity", opp_id=opp_id, tab=tabs[selected])
        st.stop()
    # Empty placeholder sections
    st.info("Workspace enabled. Placeholder only.")

def _maybe_render_shell():
    import streamlit as st
    ff = feature_flags()
    if not ff.get("workspace_enabled", False):
        return
    _render_top_nav()
    _render_opportunity_workspace()
    # Try to dispatch to known page renderers without removing existing UI
    route = get_route()
    page = route.get("page")
    dispatch = {
        "dashboard": "render_dashboard",
        "sam": "render_sam_watch",
        "pipeline": "render_pipeline",
        "outreach": "render_outreach",
        "library": "render_library",
        "admin": "render_admin",
    }
    func_name = dispatch.get(page)
    if func_name and func_name in globals() and callable(globals()[func_name]):
        try:
            globals()[func_name]()
        except Exception as ex:
            st.warning(f"Navigation handler error: {ex}")

# Initialize routing state on import
try:
    _ensure_feature_flags()
    get_route()
except Exception:
    pass

# Hook shell after Streamlit lays out base content
try:
    _maybe_render_shell()
except Exception:
    pass

# === LAYOUT PHASE 1 END ===

# === LAYOUT PHASE 2 START ===
# Subtabbed opportunity workspace with lazy loading and deep links.
# Keeps existing app tabs intact. Controlled by feature_flags['workspace_enabled'].
import contextlib
import datetime
import re

def _ensure_route_state_defaults():
    import streamlit as st
    st.session_state.setdefault('route_page', 'dashboard')
    st.session_state.setdefault('route_opp_id', None)
    st.session_state.setdefault('route_tab', None)
    st.session_state.setdefault('active_opportunity_tab', None)

def _get_notice_meta_from_db(opp_id):
    """Return minimal metadata for header: title, agency, due_date, set_aside list."""
    meta = {'title': None, 'agency': None, 'due_date': None, 'set_asides': []}
    if opp_id is None:
        meta['title'] = 'Opportunity'
        return meta
    try:
        conn = get_db()
        cur = conn.cursor()
        table_candidates = [
            ('notices', {
                'title': ['title','notice_title','name','subject'],
                'agency': ['agency','agency_name','buyer','office'],
                'due':   ['due_date','response_due','close_date','offer_due'],
                'set':   ['set_aside','setaside','set_asides','naics_set_aside']
            }),
            ('opportunities', {
                'title': ['title','name','subject'],
                'agency': ['agency','buyer','office'],
                'due':   ['due_date','close_date'],
                'set':   ['set_aside','setasides']
            })
        ]
        for table, cols in table_candidates:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,))
            if not cur.fetchone():
                continue
            cur.execute("PRAGMA table_info(%s)" % table)
            present = {r[1] for r in cur.fetchall()}
            def pick(keys):
                for k in keys:
                    if k in present:
                        return k
                return None
            c_title = pick(cols['title'])
            c_agency = pick(cols['agency'])
            c_due = pick(cols['due'])
            c_set = pick(cols['set'])
            sel_cols = [c for c in [c_title, c_agency, c_due, c_set] if c]
            if not sel_cols:
                continue
            cur.execute("SELECT %s FROM %s WHERE id=?" % (", ".join(sel_cols), table), (opp_id,))
            row = cur.fetchone()
            if row:
                idx = 0
                if c_title:
                    meta['title'] = row[idx]; idx += 1
                if c_agency:
                    meta['agency'] = row[idx]; idx += 1
                if c_due:
                    meta['due_date'] = row[idx]; idx += 1
                if c_set:
                    raw = row[idx]
                    if isinstance(raw, str):
                        parts = [p.strip() for p in re.split(r"[;,/|]", raw) if p.strip()]
                    elif isinstance(raw, (list, tuple)):
                        parts = list(raw)
                    else:
                        parts = []
                    meta['set_asides'] = parts
                break
    except Exception:
        pass
    if not meta['title']:
        meta['title'] = 'Opportunity %s' % opp_id
    return meta

try:
    import streamlit as st
except Exception:
    class _Stub:
        def cache_data(self, **kw):
            def deco(fn): return fn
            return deco
    st = _Stub()

@st.cache_data(ttl=900)
def _load_analyzer_data(opp_id):
    return {'ready': True, 'opp_id': opp_id}

@st.cache_data(ttl=900)
def _load_compliance_data(opp_id):
    return {'ready': True, 'opp_id': opp_id}

@st.cache_data(ttl=900)
def _load_pricing_data(opp_id):
    return {'ready': True, 'opp_id': opp_id}

@st.cache_data(ttl=900)
def _load_vendors_data(opp_id):
    return {'ready': True, 'opp_id': opp_id}

@st.cache_data(ttl=900)
def _load_submission_data(opp_id):
    return {'ready': True, 'opp_id': opp_id}

def render_details(opp_id):
    import streamlit as st
    st.subheader('Details')
    st.write('Opportunity ID:', opp_id)

def render_analyzer(opp_id):
    import streamlit as st
    st.subheader('Analyzer')
    data = _load_analyzer_data(opp_id)
    st.write(data)

def render_compliance(opp_id):
    import streamlit as st
    st.subheader('Compliance')
    data = _load_compliance_data(opp_id)
    st.write(data)

def render_proposal(opp_id):
    import streamlit as st
    st.subheader('Proposal')
    st.write({'opp_id': opp_id})

def render_pricing(opp_id):
    import streamlit as st
    st.subheader('Pricing')
    data = _load_pricing_data(opp_id)
    st.write(data)

def render_vendorsrfq(opp_id):
    import streamlit as st
    st.subheader('Vendors RFQ')
    data = _load_vendors_data(opp_id)
    st.write(data)

def render_submission(opp_id):
    import streamlit as st
    st.subheader('Submission')
    data = _load_submission_data(opp_id)
    st.write(data)

def open_details(opp_id):
    route_to('opportunity', opp_id=opp_id, tab='details')

def open_analyzer(opp_id):
    route_to('opportunity', opp_id=opp_id, tab='analyzer')

def open_compliance(opp_id):
    route_to('opportunity', opp_id=opp_id, tab='compliance')

def open_pricing(opp_id):
    route_to('opportunity', opp_id=opp_id, tab='pricing')

def open_vendors(opp_id):
    route_to('opportunity', opp_id=opp_id, tab='vendors')

def open_submission(opp_id):
    route_to('opportunity', opp_id=opp_id, tab='submission')

def _render_badges(set_asides):
    import streamlit as st
    if not set_asides:
        return
    cols = st.columns(min(5, len(set_asides)))
    for i, item in enumerate(set_asides[:5]):
        with cols[i]:
            st.caption(f'Set-aside: {item}')

def _render_opportunity_workspace():
    import streamlit as st
    ff = feature_flags()
    if not ff.get('workspace_enabled', False):
        return
    route = get_route()
    if route.get('page') != 'opportunity':
        return
    _ensure_route_state_defaults()
    opp_id = route.get('opp_id')
    meta = _get_notice_meta_from_db(opp_id)
    st.header(str(meta.get('title', '')))
    top_cols = st.columns([2,1,1])
    with top_cols[0]:
        st.caption(str(meta.get('agency') or ''))
    with top_cols[1]:
        due = meta.get('due_date')
        if due:
            st.caption(f'Due: {due}')
    with top_cols[2]:
        _render_badges(meta.get('set_asides') or [])
    tabs = ['details','analyzer','compliance','proposal','pricing','vendors','submission']
    labels = ['Details','Analyzer','Compliance','Proposal','Pricing','VendorsRFQ','Submission']
    current = route.get('tab') or st.session_state.get('active_opportunity_tab') or 'details'
    if current not in tabs:
        current = 'details'
    idx = tabs.index(current)
    try:
        sel = st.radio('Workspace', options=list(range(len(tabs))), index=idx, format_func=lambda i: labels[i], horizontal=True)
    except TypeError:
        sel = st.radio('Workspace', options=list(range(len(tabs))), index=idx, format_func=lambda i: labels[i])
    new_tab = tabs[sel]
    if new_tab != current:
        st.session_state['active_opportunity_tab'] = new_tab
        route_to('opportunity', opp_id=opp_id, tab=new_tab)
        st.stop()
    else:
        st.session_state['active_opportunity_tab'] = current
    if current == 'details':
        render_details(opp_id)
    elif current == 'analyzer':
        render_analyzer(opp_id)
    elif current == 'compliance':
        render_compliance(opp_id)
    elif current == 'proposal':
        render_proposal(opp_id)
    elif current == 'pricing':
        render_pricing(opp_id)
    elif current == 'vendors':
        render_vendorsrfq(opp_id)
    elif current == 'submission':
        render_submission(opp_id)
# === LAYOUT PHASE 2 END ===




# === Outreach Email (per-user) helpers ===
import smtplib, base64
from email.message import EmailMessage

USER_EMAILS = {
    "Quincy": "quincy.elamgmt@gmail.com",
    "Charles": "charles.elamgmt@gmail.com",
    "Collin": "collin.elamgmt@gmail.com",
}

def _mail_store_path():
    base = os.path.join(os.getcwd(), "secure_auth")
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "mail.json")

def _load_mail_store():
    try:
        with open(_mail_store_path(), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_mail_store(store: dict):
    with open(_mail_store_path(), "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2)

def set_user_smtp_app_password(user: str, app_password: str):
    store = _load_mail_store()
    u = store.get(user, {})
    u["smtp_host"] = "smtp.gmail.com"
    u["smtp_port"] = 587
    u["username"] = USER_EMAILS.get(user, "")
    u["app_password_b64"] = base64.b64encode((app_password or "").encode("utf-8")).decode("ascii")
    store[user] = u
    _save_mail_store(store)

def get_user_mail_config(user: str):
    store = _load_mail_store()
    rec = store.get(user, {})
    if not rec:
        return None
    pw = base64.b64decode(rec.get("app_password_b64", "").encode("ascii")).decode("utf-8") if rec.get("app_password_b64") else ""
    return {
        "smtp_host": rec.get("smtp_host", "smtp.gmail.com"),
        "smtp_port": rec.get("smtp_port", 587),
        "username": rec.get("username", ""),
        "password": pw,
        "from_addr": USER_EMAILS.get(user, rec.get("username", "")),
    }

def send_outreach_email(user: str, to_addrs, subject: str, body_html: str, cc_addrs=None, bcc_addrs=None, attachments=None, add_read_receipts=False, tracking_pixel_url=None, tracking_id=None):
    cfg = get_user_mail_config(user)
    if not cfg or not cfg.get("username") or not cfg.get("password"):
        raise RuntimeError(f"No email credentials configured for {user}. Set a Gmail App Password in the sidebar.")

    msg = EmailMessage()
    msg["Subject"] = subject or ""
    msg["From"] = cfg["from_addr"]

    def _split(a):
        if not a:
            return []
        if isinstance(a, list):
            return a
        return [x.strip() for x in str(a).replace(";", ",").split(",") if x.strip()]

    to_list = _split(to_addrs)
    cc_list = _split(cc_addrs)
    bcc_list = _split(bcc_addrs)
    if not to_list:
        raise RuntimeError("Please provide at least one recipient in To.")

    msg["To"] = ", ".join(to_list)
    if cc_list: msg["Cc"] = ", ".join(cc_list)

    import re as _re
    plain = _re.sub("<[^<]+?>", "", body_html or "") if body_html else ""
    msg.set_content(plain or "(no content)")
    if body_html:
        msg.add_alternative(body_html, subtype="html")

    # Optional read receipts
    if add_read_receipts:
        # These headers work only if recipient mail server honors them
        msg["Disposition-Notification-To"] = cfg["from_addr"]
        msg["Return-Receipt-To"] = cfg["from_addr"]

    # Optional tracking pixel
    if tracking_pixel_url and body_html:
        try:
            import uuid, urllib.parse as _u
            tid = tracking_id or str(uuid.uuid4())
            qp = {"id": tid, "to": ",".join(to_list)}
            pixel = f'<img src="{tracking_pixel_url}?'+r'{'+'}'.replace('{','')+r'}" width="1" height="1" style="display:none;" />'.replace("{"+"}", "{_u.urlencode(qp)}")
            body_html = (body_html or "") + pixel
            # Replace the last HTML alternative with updated body_html
            msg.clear_content()
            plain = _re.sub("<[^<]+?>", "", body_html or "") if body_html else ""
            msg.set_content(plain or "(no content)")
            msg.add_alternative(body_html, subtype="html")
        except Exception:
            pass



    attachments = attachments or []
    for att in attachments:
        try:
            filename = getattr(att, "name", None)
            content = None

            # Streamlit UploadedFile or file-like object with getvalue or read
            if hasattr(att, "getvalue"):
                content = att.getvalue()
            elif hasattr(att, "read"):
                try:
                    att.seek(0)
                except Exception:
                    pass
                content = att.read()
            # Dict form: {"name": ..., "data": bytes} or {"path": ...}
            elif isinstance(att, dict):
                filename = att.get("name", filename or "file")
                if "data" in att and att["data"] is not None:
                    content = att["data"]
                elif "content" in att and att["content"] is not None:
                    val = att["content"]
                    content = val.getvalue() if hasattr(val, "getvalue") else (val.read() if hasattr(val, "read") else val)
                elif "path" in att:
                    import os
                    path = att["path"]
                    with open(path, "rb") as f:
                        content = f.read()
                    if not filename:
                        filename = os.path.basename(path)
            # Raw bytes
            elif isinstance(att, (bytes, bytearray)):
                content = bytes(att)
            # String path
            elif isinstance(att, str):
                import os
                if os.path.exists(att):
                    with open(att, "rb") as f:
                        content = f.read()
                    if not filename:
                        filename = os.path.basename(att)

            if content is None:
                raise ValueError("Unsupported attachment type")

            if not filename:
                filename = "attachment.bin"

            msg.add_attachment(content, maintype="application", subtype="octet-stream", filename=filename)
        except Exception as e:
            raise RuntimeError(f"Failed to attach {getattr(att,'name', getattr(att,'path', 'file'))}: {e}")

    all_rcpts = to_list + cc_list + bcc_list

    with metric_timer('email_send_ms', {'fn':'send_outreach_email'}):
    with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"]) as server:
        server.ehlo()
        server.starttls()
        server.login(cfg["username"], cfg["password"])
        server.send_message(msg, from_addr=cfg["from_addr"], to_addrs=all_rcpts)
        metric_push('email_success', 1, {'to': str(len(all_rcpts))})

def outreach_send_from_active_user(to, subject, body_html, cc=None, bcc=None, attachments=None):
    # ACTIVE_USER provided by your sign-in block
    return send_outreach_email(ACTIVE_USER, to, subject, body_html, cc_addrs=cc, bcc_addrs=bcc, attachments=attachments)
# === End Outreach helpers ===


# === Multi-user Sign-in & Session Isolation (added by ChatGPT on 2025-10-08) ===
from functools import wraps
import uuid

# Configure your users here
USERS = ["Quincy", "Charles", "Collin"]
# Optional PINs. Leave empty {} if you want passwordless sign-in.

PINS = {"Quincy": "1111", "Charles": "2222", "Collin": "3333"}

# --- Persistent PIN store (salted) ---
import json, os, secrets, hashlib

def _pin_storage_path():
    base = os.path.join(os.getcwd(), "secure_auth")
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "pins.json")

def _load_pin_store():
    path = _pin_storage_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_pin_store(store: dict):
    path = _pin_storage_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2)

def _hash_pin(pin: str, salt: str) -> str:
    return hashlib.sha256((salt + "|" + (pin or "")).encode("utf-8")).hexdigest()

def _get_or_init_pin_store():
    store = _load_pin_store()
    # Seed from PINS dict on first run for the defined USERS
    changed = False
    for u in USERS:
        if u not in store:
            salt = secrets.token_hex(16)
            store[u] = {"salt": salt, "hash": _hash_pin(PINS.get(u, ""), salt)}
            changed = True
    if changed:
        _save_pin_store(store)
    return store

def _verify_pin(user: str, pin: str) -> bool:
    store = _get_or_init_pin_store()
    rec = store.get(user)
    if not rec:
        return False
    return _hash_pin(pin or "", rec["salt"]) == rec["hash"]

def set_user_pin(user: str, new_pin: str):
    store = _get_or_init_pin_store()
    salt = secrets.token_hex(16)
    store[user] = {"salt": salt, "hash": _hash_pin(new_pin or "", salt)}
    _save_pin_store(store)

def _do_login():
    with st.sidebar:
        st.header("Sign in")
        user = st.selectbox("User", USERS, index=0, key="login_user_select")
        pin_ok = True
        if PINS:
            pin = st.text_input("PIN", type="password", key="login_pin_input")
            pin_ok = _verify_pin(user, pin)

        if st.button("Sign in", use_container_width=True, key="login_btn"):
            if pin_ok:
                st.session_state["active_user"] = user
                # Resolve identity into users table and set session ids
                try:
                    conn = get_db()
                    row = conn.execute("SELECT id, org_id, role FROM users WHERE display_name=?", (user,)).fetchone()
                    if row:
                        st.session_state["user_id"] = row[0]
                        st.session_state["org_id"] = row[1]
                        st.session_state["role"] = row[2]
                    else:
                        # fallback create if missing
                        oid = "org-ela"
                        conn.execute("INSERT OR IGNORE INTO orgs(id,name,created_at) VALUES(?,?,datetime('now'))", (oid, "ELA Management LLC"))
                        uid = f"u-{user.lower()}"
                        conn.execute("INSERT OR IGNORE INTO users(id,org_id,email,display_name,role,created_at) VALUES(?,?,?,?,?,datetime('now'))",
                                     (uid, oid, f"{user.lower()}@ela.local", user, "Member"))
                        st.session_state["user_id"] = uid
                        st.session_state["org_id"] = oid
                        st.session_state["role"] = "Member"
                    st.session_state.setdefault("private_mode", True)
                except Exception as _ex:
                    st.warning(f"Login identity init issue: {_ex}")
                # Resolve identity into users table and set session ids
                try:
                    conn = get_db()
                    row = conn.execute("SELECT id, org_id, role FROM users WHERE display_name=?", (user,)).fetchone()
                    if row:
                        st.session_state["user_id"] = row[0]
                        st.session_state["org_id"] = row[1]
                        st.session_state["role"] = row[2]
                    else:
                        # Fallback create user mapped to default org
                        cur = conn.execute("SELECT id FROM orgs ORDER BY created_at LIMIT 1").fetchone()
                        oid = cur[0] if cur else "org-ela"
                        uid = f"u-{user.lower()}"
                        conn.execute("INSERT OR IGNORE INTO users(id,org_id,email,display_name,role,created_at) VALUES(?,?,?,?,?,datetime('now'))",
                                     (uid, oid, f"{user.lower()}@ela.local", user, 'Member'))
                        st.session_state["user_id"] = uid
                        st.session_state["org_id"] = oid
                        st.session_state["role"] = 'Member'
                except Exception:
                    pass
                st.session_state.setdefault("private_mode", True)
                st.success(f"Signed in as {user}")
            else:
                st.error("Incorrect PIN")

    if "active_user" not in st.session_state:
        st.stop()

_do_login()
ACTIVE_USER = st.session_state["active_user"]

if not st.session_state.get("org_id") or not st.session_state.get("user_id"):
    # Try resolve from active_user
    try:
        conn = get_db()
        name = st.session_state.get("active_user")
        if name:
            r = conn.execute("SELECT id, org_id FROM users WHERE display_name=?", (name,)).fetchone()
            if r:
                st.session_state["user_id"], st.session_state["org_id"] = r[0], r[1]
    except Exception:
        pass
if not st.session_state.get("org_id"):
    st.error("No organization set. Sign in again.")
    st.stop()


# --- Post-login controls: Sign out and Switch user ---
with st.sidebar:
    # Show current user and offer Sign out
    if st.session_state.get("active_user"):
        st.caption(f"Signed in as {st.session_state['active_user']}")
        if st.button("Sign out", use_container_width=True, key="logout_btn"):
            # Clear login and PIN and force re-run back to login screen
            st.session_state.pop("active_user", None)
            st.session_state.pop("login_pin_input", None)
            st.rerun()

# If the selection differs from the active user, offer a quick switch
_selected = st.session_state.get("login_user_select")
_active = st.session_state.get("active_user")
if _active and _selected and _selected != _active:
    with st.sidebar:
        st.warning(f"You selected {_selected}. To switch from {_active}, click below then sign in.")
        if st.button(f"Switch to {_selected}", use_container_width=True, key="switch_user_btn"):
            st.session_state.pop("active_user", None)  # this will trigger the login stop above on next run
            st.session_state.pop("login_pin_input", None)
            st.rerun()


# --- Namespaced session state helpers ---

# --- Unified Streamlit key helper (namespaced + duplicate-safe) ---
try:
    _NS_KEY_COUNTS
except NameError:
    _NS_KEY_COUNTS = {}
def ns_key(key: str) -> str:
    base = f"{ACTIVE_USER}::{key}"
    # increment and deduplicate within a single run
    c = _NS_KEY_COUNTS.get(base, 0) + 1
    _NS_KEY_COUNTS[base] = c
    if c == 1:
        return base
    return f"{base}__dup{c}"


class SessionNS:
    def __init__(self, user: str):
        self.user = user

    def _k(self, key: str) -> str:
        return f"{self.user}::{key}"

    def __getitem__(self, key: str):
        return st.session_state.get(self._k(key))

    def __setitem__(self, key: str, value):
        st.session_state[self._k(key)] = value

    def get(self, key: str, default=None):
        return st.session_state.get(self._k(key), default)

    def setdefault(self, key: str, default):
        return st.session_state.setdefault(self._k(key), default)

    def pop(self, key: str, default=None):
        return st.session_state.pop(self._k(key), default)

NS = SessionNS(ACTIVE_USER)

# --- Private workspace & publish queue ---
with st.sidebar:
    st.subheader("Workspace")

with st.sidebar:
    st.subheader("Security")
    with st.expander("Change My PIN", expanded=False):
        st.write("Update your sign-in PIN. New PIN must be 4–12 characters.")
        curr = st.text_input("Current PIN", type="password", key="pin_cur")
        new1 = st.text_input("New PIN", type="password", key="pin_new1")
        new2 = st.text_input("Confirm New PIN", type="password", key="pin_new2")
        if st.button("Update PIN", use_container_width=True, key="pin_update_btn"):
            if not _verify_pin(ACTIVE_USER, curr or ''):
                st.error("Current PIN is incorrect.")
            elif not new1 or len(new1) < 4 or len(new1) > 12:
                st.error("New PIN must be 4–12 characters.")
            elif new1 != new2:
                st.error("New PINs do not match.")
            else:
                set_user_pin(ACTIVE_USER, new1)
                # Clear any cached login input
                st.session_state.pop("login_pin_input", None)
                st.success("Your PIN has been updated. It will be required next time you sign in.")

    st.session_state.setdefault(f"{ACTIVE_USER}::private_mode", True)
    NS["private_mode"] = st.toggle(
        "Private mode",
        value=NS.get("private_mode", True),
        help="When ON your changes stay private to you until you publish."
    )

def queue_change(fn, *, label: str):
    """Queue a change for this user instead of writing to shared data immediately."""
    NS.setdefault("publish_queue", [])
    q = NS.get("publish_queue", [])
    q.append({"id": str(uuid.uuid4()), "label": label, "fn": fn})
    NS["publish_queue"] = q

def publish_changes():
    q = NS.get("publish_queue", [])
    errors = []
    for item in q:
        try:
            item["fn"]()
        except Exception as e:
            errors.append((item["label"], e))
    NS["publish_queue"] = []
    return errors

def write_or_queue(label, commit_fn):
    if NS.get("private_mode", True):
        queue_change(commit_fn, label=label)
        st.info(f"Saved privately. Publish later. [{label}]")
    else:
        commit_fn()
        st.success(f"Saved to team. [{label}]")

with st.sidebar:
    if st.button("Publish my changes", use_container_width=True, key="publish_btn"):
        errs = publish_changes()
        if not errs:
            st.success("All your private changes are now published to the team data.")
        else:
            st.error("Some changes failed to publish. See below for details.")
            for label, e in errs:
                st.exception(RuntimeError(f"{label}: {e}"))

# === End multi-user block ===

# === Outreach Email (per-user) — Gmail SMTP (added 2025-10-08) ===
# Supports per-user "From" emails, stored credentials, and a sidebar composer.
import smtplib
from email.message import EmailMessage
import base64

# Map users to their From addresses
USER_EMAILS = {
    "Quincy": "quincy.elamgmt@gmail.com",
    "Charles": "charles.elamgmt@gmail.com",
    "Collin": "collin.elamgmt@gmail.com",
}

def _mail_store_path():
    base = os.path.join(os.getcwd(), "secure_auth")
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "mail.json")

def _load_mail_store():
    path = _mail_store_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_mail_store(store: dict):
    path = _mail_store_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2)

def set_user_smtp_app_password(user: str, app_password: str):
    store = _load_mail_store()
    u = store.get(user, {})
    # Light obfuscation (not true encryption) — recommend using Gmail App Passwords
    u["smtp_host"] = "smtp.gmail.com"
    u["smtp_port"] = 587
    u["username"] = USER_EMAILS.get(user, "")
    u["app_password_b64"] = base64.b64encode((app_password or "").encode("utf-8")).decode("ascii")
    store[user] = u
    _save_mail_store(store)

def get_user_mail_config(user: str):
    store = _load_mail_store()
    rec = store.get(user, {})
    if not rec:
        return None
    pw = base64.b64decode(rec.get("app_password_b64", "").encode("ascii")).decode("utf-8") if rec.get("app_password_b64") else ""
    return {
        "smtp_host": rec.get("smtp_host", "smtp.gmail.com"),
        "smtp_port": rec.get("smtp_port", 587),
        "username": rec.get("username", ""),
        "password": pw,
        "from_addr": USER_EMAILS.get(user, rec.get("username", "")),
    }

def send_outreach_email(user: str, to_addrs, subject: str, body_html: str, cc_addrs=None, bcc_addrs=None, attachments=None, add_read_receipts=False, tracking_pixel_url=None, tracking_id=None):
    cfg = get_user_mail_config(user)
    if not cfg or not cfg.get("username") or not cfg.get("password"):
        raise RuntimeError(f"No email credentials configured for {user}. Set a Gmail App Password in the sidebar.")

    msg = EmailMessage()
    msg["Subject"] = subject or ""
    msg["From"] = cfg["from_addr"]
    # Parse address lists
    def _split(a):
        if not a:
            return []
        if isinstance(a, list):
            return a
        return [x.strip() for x in str(a).replace(";", ",").split(",") if x.strip()]

    to_list = _split(to_addrs)
    cc_list = _split(cc_addrs)
    bcc_list = _split(bcc_addrs)
    if not to_list:
        raise RuntimeError("Please provide at least one recipient in To.")

    msg["To"] = ", ".join(to_list)
    if cc_list: msg["Cc"] = ", ".join(cc_list)

    # HTML body; also set a plain text fallback
    from html import unescape
    plain = re.sub("<[^<]+?>", "", body_html or "") if body_html else ""
    msg.set_content(plain or "(no content)")
    if body_html:
        msg.add_alternative(body_html, subtype="html")

    # Optional read receipts
    if add_read_receipts:
        # These headers work only if recipient mail server honors them
        msg["Disposition-Notification-To"] = cfg["from_addr"]
        msg["Return-Receipt-To"] = cfg["from_addr"]

    # Optional tracking pixel
    if tracking_pixel_url and body_html:
        try:
            import uuid, urllib.parse as _u
            tid = tracking_id or str(uuid.uuid4())
            qp = {"id": tid, "to": ",".join(to_list)}
            pixel = f'<img src="{tracking_pixel_url}?'+r'{'+'}'.replace('{','')+r'}" width="1" height="1" style="display:none;" />'.replace("{"+"}", "{_u.urlencode(qp)}")
            body_html = (body_html or "") + pixel
            # Replace the last HTML alternative with updated body_html
            msg.clear_content()
            plain = _re.sub("<[^<]+?>", "", body_html or "") if body_html else ""
            msg.set_content(plain or "(no content)")
            msg.add_alternative(body_html, subtype="html")
        except Exception:
            pass


    # Attachments

    attachments = attachments or []
    for att in attachments:
        try:
            filename = getattr(att, "name", None)
            content = None

            # Streamlit UploadedFile or file-like object with getvalue or read
            if hasattr(att, "getvalue"):
                content = att.getvalue()
            elif hasattr(att, "read"):
                try:
                    att.seek(0)
                except Exception:
                    pass
                content = att.read()
            # Dict form: {"name": ..., "data": bytes} or {"path": ...}
            elif isinstance(att, dict):
                filename = att.get("name", filename or "file")
                if "data" in att and att["data"] is not None:
                    content = att["data"]
                elif "content" in att and att["content"] is not None:
                    val = att["content"]
                    content = val.getvalue() if hasattr(val, "getvalue") else (val.read() if hasattr(val, "read") else val)
                elif "path" in att:
                    import os
                    path = att["path"]
                    with open(path, "rb") as f:
                        content = f.read()
                    if not filename:
                        filename = os.path.basename(path)
            # Raw bytes
            elif isinstance(att, (bytes, bytearray)):
                content = bytes(att)
            # String path
            elif isinstance(att, str):
                import os
                if os.path.exists(att):
                    with open(att, "rb") as f:
                        content = f.read()
                    if not filename:
                        filename = os.path.basename(att)

            if content is None:
                raise ValueError("Unsupported attachment type")

            if not filename:
                filename = "attachment.bin"

            msg.add_attachment(content, maintype="application", subtype="octet-stream", filename=filename)
        except Exception as e:
            raise RuntimeError(f"Failed to attach {getattr(att,'name', getattr(att,'path', 'file'))}: {e}")

    all_rcpts = to_list + cc_list + bcc_list

    # Send via Gmail SMTP with STARTTLS (requires App Password on accounts with 2FA)
    with metric_timer('email_send_ms', {'fn':'send_outreach_email'}):
    with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"]) as server:
        server.ehlo()
        server.starttls()
        server.login(cfg["username"], cfg["password"])
        server.send_message(msg, from_addr=cfg["from_addr"], to_addrs=all_rcpts)
        metric_push('email_success', 1, {'to': str(len(all_rcpts))})


# --- Outreach Tools UI (moved from sidebar to Outreach tab to prevent bleed-through) ---




def _normalize_extra_files(files):
    """Normalize a list of attachments into dicts with name and raw bytes in data."""
    out = []
    try:
        for f in (files or []):
            # Already a normalized dict
            if isinstance(f, dict):
                name = f.get("name") or f.get("filename") or "file"
                if "data" in f and f["data"] is not None:
                    out.append({"name": name, "data": f["data"]})
                    continue
                if "content" in f and f["content"] is not None:
                    val = f["content"]
                    if isinstance(val, (bytes, bytearray)):
                        out.append({"name": name, "data": bytes(val)})
                    elif isinstance(val, str):
                        import os
                        if os.path.exists(val):
                            with open(val, "rb") as fh:
                                out.append({"name": name, "data": fh.read()})
                        else:
                            out.append({"name": name, "data": val.encode("utf-8")})
                    continue
                if "path" in f and f["path"]:
                    import os
                    path = f["path"]
                    try:
                        with open(path, "rb") as fh:
                            out.append({"name": name or os.path.basename(path), "data": fh.read()})
                    except Exception:
                        pass
                    continue

            # Streamlit UploadedFile or similar
            if hasattr(f, "getvalue"):
                out.append({"name": getattr(f, "name", "file"), "data": f.getvalue()})
                continue
            if hasattr(f, "read"):
                try:
                    f.seek(0)
                except Exception:
                    pass
                try:
                    data = f.read()
                    out.append({"name": getattr(f, "name", "file"), "data": data})
                    continue
                except Exception:
                    pass

            # File path string
            if isinstance(f, str):
                import os
                if os.path.exists(f):
                    try:
                        with open(f, "rb") as fh:
                            out.append({"name": os.path.basename(f), "data": fh.read()})
                        continue
                    except Exception:
                        pass
    except Exception:
        pass
    return out



def _log_contact_outreach(entries):
    """Append outreach log entries to data/contact_outreach_log.json"""
    try:
        import os, json, datetime
        base = os.path.join(os.getcwd(), "data")
        os.makedirs(base, exist_ok=True)
        path = os.path.join(base, "contact_outreach_log.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = []
        timestamp = datetime.datetime.utcnow().isoformat()+"Z"
        for e in entries or []:
            e.setdefault("ts_utc", timestamp)
        existing.extend(entries or [])
        with open(path, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
        return path
    except Exception:
        return None


def render_outreach_tools():
    import streamlit as st
    import streamlit.components.v1 as components
    # ---------- Helpers ----------
    def _normalize_sel_attachments(sel_atts):
        """Return a list of dicts with just 'name' for display when attachments in the generated item are names/dicts."""
        out = []
        base = sel_atts or []
        try:
            for a in base:
                if isinstance(a, dict) and ("name" in a or "filename" in a):
                    nm = a.get("name") or a.get("filename") or "attachment"
                    out.append({"name": nm})
                elif isinstance(a, str):
                    out.append({"name": a})
        except Exception:
            pass
        return out


        out = []
        try:
            for f in (files or []):
                # Already-normalized dict: pass through or convert
                if isinstance(f, dict):
                    name = f.get("name") or f.get("filename") or "file"
                    if "data" in f and f["data"] is not None:
                        out.append({"name": name, "data": f["data"]})
                    elif "content" in f and f["content"] is not None:
                        val = f["content"]
                        if isinstance(val, (bytes, bytearray)):
                            out.append({"name": name, "data": bytes(val)})
                        elif isinstance(val, str):
                            # If looks like a path, try to read from disk
                            import os
                            if os.path.exists(val):
                                with open(val, "rb") as fh:
                                    out.append({"name": name, "data": fh.read()})
                            else:
                                out.append({"name": name, "data": val.encode("utf-8")})
                    elif "path" in f and f["path"]:
                        import os
                        path = f["path"]
                        try:
                            with open(path, "rb") as fh:
                                out.append({"name": name or os.path.basename(path), "data": fh.read()})
                        except Exception:
                            pass
                    continue

                # Streamlit UploadedFile or similar
                if hasattr(f, "getvalue"):
                    out.append({"name": getattr(f, "name", "file"), "data": f.getvalue()})
                    continue
                if hasattr(f, "read"):
                    try:
                        f.seek(0)
                    except Exception:
                        pass
                    try:
                        data = f.read()
                        out.append({"name": getattr(f, "name", "file"), "data": data})
                        continue
                    except Exception:
                        pass

                # File path
                if isinstance(f, str):
                    import os
                    if os.path.exists(f):
                        try:
                            with open(f, "rb") as fh:
                                out.append({"name": os.path.basename(f), "data": fh.read()})
                            continue
                        except Exception:
                            pass
        except Exception:
            pass
        return out

    # Robust local sender that tries multiple implementations
    def _send_email(user, to, subject, body_html, cc="", bcc="", attachments=None):
        last_err = None
        # Preferred modern signature
        try:
            return send_outreach_email(user, to, subject, body_html,
                                       cc_addrs=cc, bcc_addrs=bcc, attachments=attachments)
        except Exception as e:
            last_err = e
        # Legacy fallback (active-user based)
        try:
            return outreach_send_from_active_user(to, subject, body_html,
                                                  cc=cc, bcc=bcc, attachments=attachments)
        except Exception as e:
            last_err = e
        # Optional extra names if your app exposes them
        for name in ("send_outreach_message", "send_gmail_message", "send_mail", "outreach_send"):
            fn = globals().get(name)
            if callable(fn):
                try:
                    return fn(user, to, subject, body_html, cc, bcc, attachments)
                except Exception as e:
                    last_err = e
        raise last_err or RuntimeError("No outreach sender is available")

    # ---------- Stable session keys ----------
    SKEY_PREVIEW = f"{ACTIVE_USER}::outreach::preview"             # snapshot for the Gmail-style preview card
    SKEY_ATTACH  = f"{ACTIVE_USER}::outreach::extra_attachments"   # extra attachments uploaded by user (UploadedFile list)
    SKEY_LASTSIG = f"{ACTIVE_USER}::outreach::last_loaded_sig"

    st.session_state.setdefault(SKEY_PREVIEW, None)
    st.session_state.setdefault(SKEY_ATTACH, [])

    st.session_state.setdefault(SKEY_LASTSIG, "")
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")

    # ---------- Header ----------
    with st.container(border=True):
        top_l, top_r = st.columns([3,2])
        with top_l:
            st.markdown("### ✉️ Outreach")
            st.caption(f"From: **{from_addr}**" if from_addr else "No email configured for this user.")
        with st.container(border=True):
            mode = st.radio("Send to", ["Vendors", "Contacts"], index=0, horizontal=True, key="outreach_mode")



    # ---- Contacts Outreach ----
    if mode == "Contacts":
        with st.container(border=True):
            st.markdown("#### Contacts")
            # Read receipts + tracking pixel options
            with st.expander("Delivery & Tracking options", expanded=False):
                want_rr = st.checkbox("Request read receipt headers (may prompt recipient)", value=False, key="outreach_rr")
                pixel_url = st.text_input("Optional tracking pixel URL (https://...)", value="", key="outreach_pixel_url")
            # Load contacts from CSV
            col_c1, col_c2 = st.columns([2,1])
            with col_c1:
                search = st.text_input("Search contacts", key="outreach_contact_search")
            with col_c2:
                uploaded = st.file_uploader("", type=["csv"], key="outreach_contacts_csv")
            contacts = []
            import os, csv
            # Prefer uploaded CSV
            if uploaded is not None:
                try:
                    txt = uploaded.getvalue().decode("utf-8", errors="ignore")
                    for row in csv.DictReader(txt.splitlines()):
                        nm = row.get("name") or row.get("Name") or row.get("full_name") or ""
                        em = row.get("email") or row.get("Email") or row.get("mail") or ""
                        if em:
                            contacts.append({"name": nm, "email": em})
                except Exception:
                    pass
            else:
                # Try default data/contacts.csv
                try:
                    path = os.path.join(os.getcwd(), "data", "contacts.csv")
                    if os.path.exists(path):
                        with open(path, "r", encoding="utf-8") as f:
                            for row in csv.DictReader(f):
                                nm = row.get("name") or row.get("Name") or row.get("full_name") or ""
                                em = row.get("email") or row.get("Email") or row.get("mail") or ""
                                if em:
                                    contacts.append({"name": nm, "email": em})
                except Exception:
                    pass

            # Filter by search
            s = (search or "").lower().strip()
            if s:
                contacts = [c for c in contacts if s in (c.get("name","")+c.get("email","")).lower()]

            # Options
            labels = [f'{c.get("name") or ""} <{c["email"]}>' if c.get("name") else c["email"] for c in contacts]
            selected = st.multiselect("Recipients", labels, key="outreach_contact_sel")

            subj = st.text_input("Subject", key="outreach_contact_subject")
            body = st.text_area("Body (HTML allowed)", key="outreach_contact_body", height=220)
            c_files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key="outreach_contact_files")

            if st.button("Send to selected contacts", use_container_width=True, key="outreach_contact_send"):
                emails = []
                label_to_email = {}
                for c, lbl in zip(contacts, labels):
                    label_to_email[lbl] = c["email"]
                for lbl in selected:
                    em = label_to_email.get(lbl)
                    if em:
                        emails.append(em)
                if not emails:
                    st.warning("Select at least one contact.")
                elif not subj or not body:
                    st.warning("Subject and body are required.")
                else:
                    # Normalize files
                    atts = _normalize_extra_files(c_files)
                    # Tracking id per batch
                    import uuid
                    batch_id = str(uuid.uuid4())
                    failures = []
                    sent = 0
                    for em in emails:
                        try:
                            send_outreach_email(
                                ACTIVE_USER, [em], subj, body,
                                cc_addrs=None, bcc_addrs=None, attachments=atts,
                                add_read_receipts=want_rr, tracking_pixel_url=(pixel_url or None),
                                tracking_id=batch_id + "::" + em
                            )
                            sent += 1
                        except Exception as e:
                            failures.append((em, str(e)))
                    # Log
                    _log_contact_outreach([{"mode":"contacts","to": em, "subject": subj, "batch_id": batch_id} for em in emails])
                    if failures:
                        st.error(f"Sent {sent} / {len(emails)}. Failures: " + "; ".join([f"{a} ({b})" for a,b in failures]))
                    else:
                        st.success(f"Sent {sent} / {len(emails)}")
        # Stop rendering vendor section if Contacts mode
        return


        with top_r:
            pass

    # ---- Account: App Password (still here) ----
    with st.expander("Set/Update my Gmail App Password", expanded=False):
        pw = st.text_input("Gmail App Password", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            try:
                set_user_smtp_app_password(ACTIVE_USER, pw)
                st.success("Saved")
            except Exception as e:
                st.error(f"Failed to save: {e}")

    st.divider()

    # ---------- Choose Generated Email & Attachments (required) ----------
    with st.container(border=True):
        st.markdown("#### Choose Generated Email")
        mb = st.session_state.get("mail_bodies") or []
        if not mb:
            st.info("Generate emails to select one for preview.", icon="ℹ️")
        else:
            idx = st.number_input("Select one", min_value=1, max_value=len(mb), value=len(mb), step=1,
                                  key=ns_key("outreach::pick_idx"))
            sel = mb[int(idx)-1]

            # Show key fields from the generated email
            st.caption(f"**To:** {sel.get('to','')}")
            st.caption(f"**Subject:** {sel.get('subject','')}")
            scope_disp = sel.get("scope_summary") or sel.get("scope") or ""
            due_disp = sel.get("quote_due") or sel.get("due") or ""
            meta_cols = st.columns(2)
            with meta_cols[0]:
                st.markdown(f"**Scope Summary:** {scope_disp}")
            with meta_cols[1]:
                st.markdown(f"**Quote Due:** {due_disp}")

            # Attachments uploader (REQUIRED) placed below Quote Due
            extra_files = st.file_uploader("Attachments (required)", type=None, accept_multiple_files=True,
                                           key=ns_key("outreach::extra_files"))
            if extra_files is not None:
                st.session_state[SKEY_ATTACH] = extra_files

            # Generate preview button
            if st.button("Generate preview", key=ns_key("outreach::gen_preview"), use_container_width=True):
                files = st.session_state.get(SKEY_ATTACH) or []
                if not files:
                    st.warning("Please upload at least one attachment before generating the preview.")
                else:
                    # Build display names from generated attachments + uploaded files
                    gen_names = _normalize_sel_attachments(sel.get("attachments"))
                    try:
                        upload_names = [{"name": getattr(f, "name", "file")} for f in files]
                    except Exception:
                        upload_names = []
                    st.session_state[SKEY_PREVIEW] = {
                        "to": sel.get("to",""),
                        "cc": sel.get("cc",""),
                        "bcc": sel.get("bcc",""),
                        "subject": sel.get("subject",""),
                        "body_html": sel.get("body",""),
                        "from_addr": USER_EMAILS.get(ACTIVE_USER, ""),
                        "scope_summary": scope_disp,
                        "quote_due": due_disp,
                        "attachments": (gen_names or []) + (upload_names or [])
                    }
                    st.success("Preview generated below.")


            actions2 = st.columns([1, 2, 2, 5])
            with actions2[1]:
                if st.button("Send selected now", key=ns_key("outreach::send_selected_now"), use_container_width=True):
                    files = st.session_state.get(SKEY_ATTACH) or []
                    if not files:
                        st.warning("Please upload at least one attachment before sending.")
                    else:
                        try:
                            merged_atts = _normalize_sel_attachments(sel.get("attachments")) + _normalize_extra_files(files)
                            _send_email(
                                ACTIVE_USER,
                                sel.get("to",""),
                                sel.get("subject",""),
                                sel.get("body",""),
                                cc=sel.get("cc",""),
                                bcc=sel.get("bcc",""),
                                attachments=merged_atts
                            )
                            st.success("Selected email sent.")
                        except Exception as e:
                            st.error(f"Failed to send selected: {e}")
            with actions2[2]:
                if st.button("Send ALL generated now", key=ns_key("outreach::send_all_now"), use_container_width=True):
                    files = st.session_state.get(SKEY_ATTACH) or []
                    if not files:
                        st.warning("Please upload at least one attachment before mass sending.")
                    else:
                        mb_all = st.session_state.get("mail_bodies") or []
                        sent = 0
                        failures = []
                        for i, itm in enumerate(mb_all, start=1):
                            try:
                                merged_atts = _normalize_sel_attachments(itm.get("attachments")) + _normalize_extra_files(files)
                                _send_email(
                                    ACTIVE_USER,
                                    itm.get("to",""),
                                    itm.get("subject",""),
                                    itm.get("body",""),
                                    cc=itm.get("cc",""),
                                    bcc=itm.get("bcc",""),
                                    attachments=merged_atts
                                )
                                sent += 1
                            except Exception as e:
                                failures.append((i, itm.get("subject",""), str(e)))
                        if failures:
                            st.error(f"Sent {sent} / {len(mb_all)}. Failures: " + "; ".join([f"#{i} {subj} ({err})" for i, subj, err in failures]))
                        else:
                            st.success(f"Sent all {sent} generated emails.")# ---------- Single Preview (Gmail-like card) ---------- (Gmail-like card) ----------
    snap = st.session_state.get(SKEY_PREVIEW)
    with st.container(border=True):
        st.markdown("#### Preview")
        if not snap:
            st.info("Select a generated email above, attach files if needed, and click Preview.", icon="ℹ️")
        else:
            # Header block similar to Gmail
            hdr_lines = []
            if snap.get("from_addr"): hdr_lines.append(f"<div><b>From:</b> {snap['from_addr']}</div>")
            if snap.get("to"):        hdr_lines.append(f"<div><b>To:</b> {snap['to']}</div>")
            if snap.get("cc"):        hdr_lines.append(f"<div><b>Cc:</b> {snap['cc']}</div>")
            if snap.get("bcc"):       hdr_lines.append(f"<div><b>Bcc:</b> {snap['bcc']}</div>")
            if snap.get("subject"):   hdr_lines.append(f"<div style='font-size:16px;margin-top:4px;'><b>Subject:</b> {snap['subject']}</div>")

            # Meta row: Scope Summary & Quote Due
            meta_bits = []
            if snap.get("scope_summary"):
                meta_bits.append("<div style='display:inline-block;border:1px solid #eee;"
                                 "padding:4px 8px;border-radius:8px;margin-right:8px;'><b>Scope:</b> "
                                 f"{snap['scope_summary']}</div>")
            if snap.get("quote_due"):
                meta_bits.append("<div style='display:inline-block;border:1px solid #eee;"
                                 "padding:4px 8px;border-radius:8px;'><b>Quote due:</b> "
                                 f"{snap['quote_due']}</div>")


            # Attachments uploader (positioned below Quote Due)
            extra_files = st.file_uploader("Attachments (required)", type=None, accept_multiple_files=True,
                                           key=ns_key("outreach::extra_files"))
            if extra_files is not None:
                st.session_state[SKEY_ATTACH] = extra_files

            # Body
            body_html = (snap.get("body_html") or "").strip() or "<p><i>(No body content)</i></p>"

            # Attachments display
            atts_html = ""
            atts = snap.get("attachments") or []
            if atts:
                items = "".join([f"<li>{(a.get('name') if isinstance(a,dict) else str(a))}</li>" for a in atts])
                atts_html = ("<div style='margin-top:8px;'><b>Attachments:</b>"
                             f"<ul style='margin:6px 0 0 20px;'>{items}</ul></div>")

            components.html(f"""
                <div style="border:1px solid #ddd;border-radius:8px;padding:14px;">
                    <div style="margin-bottom:8px;">{''.join(hdr_lines)}</div>
                    <div style="margin-bottom:8px;">{''.join(meta_bits)}</div>
                    <div style="border:1px solid #eee;padding:10px;border-radius:6px;">{body_html}</div>
                    {atts_html}
                </div>
            """, height=520, scrolling=True)

            # Actions under the preview
            a1, a2 = st.columns(2)
            with a1:
                if st.button("Send email", key=ns_key("outreach::send_from_preview"), use_container_width=True):
                    try:
                        _send_email(
                            ACTIVE_USER,
                            snap.get("to",""),
                            snap.get("subject",""),
                            snap.get("body_html",""),
                            cc=snap.get("cc",""),
                            bcc=snap.get("bcc",""),
                            attachments=st.session_state.get(SKEY_ATTACH) or []
                        )
                        st.success("Email sent.")
                        st.session_state[SKEY_PREVIEW] = None
                    except Exception as e:
                        st.error(f"Failed to send: {e}")
            with a2:
                if st.button("Close preview", key=ns_key("outreach::close_preview"), use_container_width=True):
                    st.session_state[SKEY_PREVIEW] = None

def load_outreach_preview(to="", cc="", bcc="", subject="", html=""):
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    key = lambda k: f"{ACTIVE_USER}::outreach::{k}"
    st.session_state[key("to")] = to or ""
    st.session_state[key("cc")] = cc or ""
    st.session_state[key("bcc")] = bcc or ""
    st.session_state[key("subj")] = subject or ""
    st.session_state[key("body")] = html or ""
    st.session_state[key("preview")] = {
        "to": to or "",
        "cc": cc or "",
        "bcc": bcc or "",
        "subject": subject or "",
        "body_html": html or "",
        "attachments": [],
        "from_addr": from_addr,
    }

    st.subheader("Email – Outreach")
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    if not from_addr:
        st.caption("No email configured for this user. Only Charles and Collin are set up.")
    else:
        st.caption(f"From: {from_addr}")

    st.session_state.setdefault(ns_key("outreach::mail_preview_data"), None)

    hc1, hc2, hc3 = st.columns([1,1,2])
    with hc1:
        if st.button("Preview current draft", key=ns_key("outreach::hdr_preview_btn")):
            to = st.session_state.get(ns_key("outreach::mail_to"), "") or ""
            cc = st.session_state.get(ns_key("outreach::mail_cc"), "") or ""
            bcc = st.session_state.get(ns_key("outreach::mail_bcc"), "") or ""
            subj = st.session_state.get(ns_key("outreach::mail_subj"), "") or ""
            body = st.session_state.get(ns_key("outreach::mail_body"), "") or ""
            atts = (st.session_state.get(ns_key("outreach::mail_preview_data")) or {}).get("attachments", [])
            st.session_state[ns_key("outreach::mail_preview_data")] = {
                "to": to,
                "cc": cc,
                "bcc": bcc,
                "subject": subj,
                "body_html": body,
                "attachments": atts,
                "from_addr": from_addr,
            }
    with hc2:
        if st.button("Clear preview", key=ns_key("outreach::hdr_preview_clear")):
            st.session_state[ns_key("outreach::mail_preview_data")] = None

    with st.expander("Set/Update my Gmail App Password", expanded=False):
        st.caption("Generate an App Password in your Google Account > Security > 2-Step Verification.")
        app_pw = st.text_input("Gmail App Password (16 chars, no spaces)", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            set_user_smtp_app_password(ACTIVE_USER, app_pw)
            st.success("Saved. You can now send emails from the Outreach composer.")

    with st.expander("Quick Outreach Composer", expanded=False):
        to = st.text_input("To (comma-separated)", key=ns_key("outreach::mail_to"),
                           placeholder="recipient@example.com, another@domain.com")
        cc = st.text_input("Cc (optional, comma-separated)", key=ns_key("outreach::mail_cc"))
        bcc = st.text_input("Bcc (optional, comma-separated)", key=ns_key("outreach::mail_bcc"))
        subj = st.text_input("Subject", key=ns_key("outreach::mail_subj"))
        body = st.text_area("Message (HTML supported)", key=ns_key("outreach::mail_body"), height=200,
                            placeholder="<p>Hello.</p>")
        files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key=ns_key("outreach::mail_files"))

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Preview email", use_container_width=True, key=ns_key("outreach::mail_preview_btn")):
                atts = []
                try:
                    for f in (files or []):
                        try:
                            atts.append({"name": getattr(f, "name", "file"), "data": f.getvalue()})
                        except Exception:
                            pass
                except Exception:
                    atts = []
                st.session_state[ns_key("outreach::mail_preview_data")] = {
                    "to": to or "",
                    "cc": cc or "",
                    "bcc": bcc or "",
                    "subject": subj or "",
                    "body_html": body or "",
                    "attachments": atts,
                    "from_addr": from_addr,
                }
        with c2:
            if st.button("Send email", use_container_width=True, key=ns_key("outreach::mail_send_btn")):
                try:
                    send_outreach_email(ACTIVE_USER, to, subj, body, cc_addrs=cc, bcc_addrs=bcc, attachments=files)
                    st.success("Email sent.")
                    for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                        NS.pop(k, None)
                except Exception as e:
                    st.error(f"Failed to send: {e}")

    preview = st.session_state.get(ns_key("outreach::mail_preview_data"))
    if preview:
        import streamlit.components.v1 as components
        with st.container(border=True):
            st.markdown("#### Email preview")
            st.markdown(f"**From:** {preview.get('from_addr','')}")
            if preview.get("to"): st.markdown(f"**To:** {preview['to']}")
            if preview.get("cc"): st.markdown(f"**Cc:** {preview['cc']}")
            if preview.get("bcc"): st.markdown(f"**Bcc:** {preview['bcc']}")
            st.markdown(f"**Subject:** {preview.get('subject','')}")
            html = preview.get("body_html") or ""
            components.html(
                f"""
                <div style="border:1px solid #ddd;padding:16px;margin-top:8px;">
                    {html}
                </div>
                """,
                height=400,
                scrolling=True,
            )
        atts = preview.get("attachments") or []
        if atts:
            names = [a.get("name","file") for a in atts]
            st.caption("Attachments: " + ", ".join(names))

        cc1, cc2, _ = st.columns([1,1,2])
        with cc1:
            if st.button("Send this email", key=ns_key("outreach::mail_preview_confirm")):
                class _MemFile:
                    def __init__(self, name, data):
                        self.name = name
                        self._data = data
                    def getvalue(self):
                        return self._data
                mem_files = [_MemFile(a.get("name","file"), a.get("data", b"")) for a in atts]
                try:
                    send_outreach_email(
                        ACTIVE_USER,
                        preview.get("to",""),
                        preview.get("subject",""),
                        preview.get("body_html",""),
                        cc_addrs=preview.get("cc",""),
                        bcc_addrs=preview.get("bcc",""),
                        attachments=mem_files
                    )
                    st.success("Email sent.")
                    st.session_state[ns_key("outreach::mail_preview_data")] = None
                    for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                        NS.pop(k, None)
                except Exception as e:
                    st.error(f"Failed to send: {e}")
        with cc2:
            if st.button("Close preview", key=ns_key("outreach::mail_preview_close")):
                st.session_state[ns_key("outreach::mail_preview_data")] = None
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    if not from_addr:
        st.caption("No email configured for this user. Only Charles and Collin are set up.")
    else:
        st.caption(f"From: {from_addr}")

    # Global preview state
    st.session_state.setdefault(ns_key("outreach::mail_preview_data"), None)

    # === Header-level controls ===
    hc1, hc2, hc3 = st.columns([1,1,2])
    with hc1:
        if st.button("Preview current draft", key=ns_key("outreach::hdr_preview_btn")):
            # Pull current draft values from session, even if the composer expander is closed
            to = st.session_state.get(ns_key("outreach::mail_to"), "") or ""
            cc = st.session_state.get(ns_key("outreach::mail_cc"), "") or ""
            bcc = st.session_state.get(ns_key("outreach::mail_bcc"), "") or ""
            subj = st.session_state.get(ns_key("outreach::mail_subj"), "") or ""
            body = st.session_state.get(ns_key("outreach::mail_body"), "") or ""
            # Attachments are not easily accessible from header because uploader holds file objects;
            # keep whatever was already captured if a composer preview was taken, else empty.
            atts = (st.session_state.get(ns_key("outreach::mail_preview_data")) or {}).get("attachments", [])

            st.session_state[ns_key("outreach::mail_preview_data")] = {
                "to": to, "cc": cc, "bcc": bcc,
                "subject": subj,
                "body_html": body,
                "attachments": atts,
                "from_addr": from_addr,
            }
    with hc2:
        if st.button("Clear preview", key=ns_key("outreach::hdr_preview_clear")):
            st.session_state[ns_key("outreach::mail_preview_data")] = None

    with st.expander("Set/Update my Gmail App Password", expanded=False):
        st.caption("Generate an App Password in your Google Account > Security > 2-Step Verification.")
        app_pw = st.text_input("Gmail App Password (16 chars, no spaces)", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            set_user_smtp_app_password(ACTIVE_USER, app_pw)
            st.success("Saved. You can now send emails from the Outreach composer.")

    # === Quick Outreach Composer ===
    with st.expander("Quick Outreach Composer", expanded=False):
        to = st.text_input("To (comma-separated)",
                           key=ns_key("outreach::mail_to"),
                           placeholder="recipient@example.com, another@domain.com")
        cc = st.text_input("Cc (optional, comma-separated)", key=ns_key("outreach::mail_cc"))
        bcc = st.text_input("Bcc (optional, comma-separated)", key=ns_key("outreach::mail_bcc"))
        subj = st.text_input("Subject", key=ns_key("outreach::mail_subj"))
        body = st.text_area("Message (HTML supported)", key=ns_key("outreach::mail_body"), height=200,
                            placeholder="<p>Hello.</p>")
        files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key=ns_key("outreach::mail_files"))

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Preview email", use_container_width=True, key=ns_key("outreach::mail_preview_btn")):
                # Snapshot current fields (including attachments) for a pixel-accurate preview
                atts = []
                try:
                    for f in (files or []):
                        try:
                            atts.append({"name": getattr(f, "name", "file"), "data": f.getvalue()})
                        except Exception:
                            pass
                except Exception:
                    atts = []
                st.session_state[ns_key("outreach::mail_preview_data")] = {
                    "to": to or "",
                    "cc": cc or "",
                    "bcc": bcc or "",
                    "subject": subj or "",
                    "body_html": body or "",
                    "attachments": atts,
                    "from_addr": from_addr,
                }
        with c2:
            if st.button("Send email", use_container_width=True, key=ns_key("outreach::mail_send_btn")):
                try:
                    send_outreach_email(ACTIVE_USER, to, subj, body, cc_addrs=cc, bcc_addrs=bcc, attachments=files)
                    st.success("Email sent.")
                    for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                        NS.pop(k, None)
                except Exception as e:
                    st.error(f"Failed to send: {e}")

    # === Unified Preview Block (used by both header-level and composer-level triggers) ===
    preview = st.session_state.get(ns_key("outreach::mail_preview_data"))
    if preview:
        import streamlit.components.v1 as components
        with st.container(border=True):
            st.markdown("#### Email preview")
            st.markdown(f"**From:** {preview.get('from_addr','')}")
            if preview.get("to"):
                st.markdown(f"**To:** {preview['to']}")
            if preview.get("cc"):
                st.markdown(f"**Cc:** {preview['cc']}")
            if preview.get("bcc"):
                st.markdown(f"**Bcc:** {preview['bcc']}")
            st.markdown(f"**Subject:** {preview.get('subject','')}")

            html = preview.get("body_html") or ""
            components.html(
                f"""
                <div style="border:1px solid #ddd;padding:16px;margin-top:8px;">
                    {html}
                </div>
                """,
                height=400,
                scrolling=True,
            )

            atts = preview.get("attachments") or []
            if atts:
                names = [a.get("name","file") for a in atts]
                st.caption("Attachments: " + ", ".join(names))

            cc1, cc2, cc3 = st.columns([1,1,2])
            with cc1:
                if st.button("Send this email", key=ns_key("outreach::mail_preview_confirm")):
                    class _MemFile:
                        def __init__(self, name, data):
                            self.name = name
                            self._data = data
                        def getvalue(self):
                            return self._data
                    mem_files = [_MemFile(a.get("name","file"), a.get("data", b"")) for a in atts]
                    try:
                        send_outreach_email(
                            ACTIVE_USER,
                            preview.get("to",""),
                            preview.get("subject",""),
                            preview.get("body_html",""),
                            cc_addrs=preview.get("cc",""),
                            bcc_addrs=preview.get("bcc",""),
                            attachments=mem_files
                        )
                        st.success("Email sent.")
                        st.session_state[ns_key("outreach::mail_preview_data")] = None
                        for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                            NS.pop(k, None)
                    except Exception as e:
                        st.error(f"Failed to send: {e}")
            with cc2:
                if st.button("Close preview", key=ns_key("outreach::mail_preview_close")):
                    st.session_state[ns_key("outreach::mail_preview_data")] = None
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    if not from_addr:
        st.caption("No email configured for this user. Only Charles and Collin are set up.")
    else:
        st.caption(f"From: {from_addr}")

    with st.expander("Set/Update my Gmail App Password", expanded=False):
        st.caption("Generate an App Password in your Google Account > Security > 2-Step Verification.")
        app_pw = st.text_input("Gmail App Password (16 chars, no spaces)", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            set_user_smtp_app_password(ACTIVE_USER, app_pw)
            st.success("Saved. You can now send emails from the Outreach composer.")

    # Preview state
    st.session_state.setdefault(ns_key("outreach::mail_preview_data"), None)

    with st.expander("Quick Outreach Composer", expanded=False):
        to = st.text_input("To (comma-separated)",
                           key=ns_key("outreach::mail_to"),
                           placeholder="recipient@example.com, another@domain.com")
        cc = st.text_input("Cc (optional, comma-separated)", key=ns_key("outreach::mail_cc"))
        bcc = st.text_input("Bcc (optional, comma-separated)", key=ns_key("outreach::mail_bcc"))
        subj = st.text_input("Subject", key=ns_key("outreach::mail_subj"))
        body = st.text_area("Message (HTML supported)", key=ns_key("outreach::mail_body"), height=200,
                            placeholder="<p>Hello.</p>")
        files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key=ns_key("outreach::mail_files"))

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Preview email", use_container_width=True, key=ns_key("outreach::mail_preview_btn")):
                # Store a snapshot of the compose fields in session so a rerun preserves the preview
                # For attachments, store name and raw bytes so we can reconstruct file-like objects later.
                atts = []
                try:
                    for f in (files or []):
                        try:
                            atts.append({
                                "name": getattr(f, "name", "file"),
                                "data": f.getvalue()
                            })
                        except Exception:
                            pass
                except Exception:
                    atts = []
                st.session_state[ns_key("outreach::mail_preview_data")] = {
                    "to": to or "",
                    "cc": cc or "",
                    "bcc": bcc or "",
                    "subject": subj or "",
                    "body_html": body or "",
                    "attachments": atts,
                    "from_addr": from_addr,
                }
        with c2:
            if st.button("Send email", use_container_width=True, key=ns_key("outreach::mail_send_btn")):
                try:
                    send_outreach_email(ACTIVE_USER, to, subj, body, cc_addrs=cc, bcc_addrs=bcc, attachments=files)
                    st.success("Email sent.")
                    for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                        NS.pop(k, None)
                except Exception as e:
                    st.error(f"Failed to send: {e}")

    # If a preview has been requested, render it exactly like the HTML body will appear.
    preview = st.session_state.get(ns_key("outreach::mail_preview_data"))
    if preview:
        import streamlit.components.v1 as components

        with st.container(border=True):
            st.markdown("#### Email preview")
            # Header preview
            st.markdown(f"**From:** {preview.get('from_addr','')}")
            if preview.get("to"):
                st.markdown(f"**To:** {preview['to']}")
            if preview.get("cc"):
                st.markdown(f"**Cc:** {preview['cc']}")
            if preview.get("bcc"):
                st.markdown(f"**Bcc:** {preview['bcc']}")
            st.markdown(f"**Subject:** {preview.get('subject','')}")

            # Render the HTML body using a component so styles and tags are honored
            html = preview.get("body_html") or ""
            components.html(
                f"""
                <div style="border:1px solid #ddd;padding:16px;margin-top:8px;">
                    {html}
                </div>
                """,
                height=400,
                scrolling=True,
            )

            # Show attachment list if any
            atts = preview.get("attachments") or []
            if atts:
                names = [a.get("name","file") for a in atts]
                st.caption("Attachments: " + ", ".join(names))

            # Confirm send buttons
            cc1, cc2, cc3 = st.columns([1,1,2])
            with cc1:
                if st.button("Send this email", key=ns_key("outreach::mail_preview_confirm")):
                    # Rebuild simple in memory files compatible with send_outreach_email expectations
                    class _MemFile:
                        def __init__(self, name, data):
                            self.name = name
                            self._data = data
                        def getvalue(self):
                            return self._data
                    mem_files = [_MemFile(a.get("name","file"), a.get("data", b"")) for a in atts]
                    try:
                        send_outreach_email(
                            ACTIVE_USER,
                            preview.get("to",""),
                            preview.get("subject",""),
                            preview.get("body_html",""),
                            cc_addrs=preview.get("cc",""),
                            bcc_addrs=preview.get("bcc",""),
                            attachments=mem_files
                        )
                        st.success("Email sent.")
                        st.session_state[ns_key("outreach::mail_preview_data")] = None
                        # Clear compose fields
                        for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                            NS.pop(k, None)
                    except Exception as e:
                        st.error(f"Failed to send: {e}")
            with cc2:
                if st.button("Close preview", key=ns_key("outreach::mail_preview_close")):
                    st.session_state[ns_key("outreach::mail_preview_data")] = None
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    if not from_addr:
        st.caption("No email configured for this user. Only Charles and Collin are set up.")
    else:
        st.caption(f"From: {from_addr}")

    with st.expander("Set/Update my Gmail App Password", expanded=False):
        st.caption("Generate an App Password in your Google Account > Security > 2-Step Verification.")
        app_pw = st.text_input("Gmail App Password (16 chars, no spaces)", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            set_user_smtp_app_password(ACTIVE_USER, app_pw)
            st.success("Saved. You can now send emails from the Outreach composer.")

    with st.expander("Quick Outreach Composer", expanded=False):
        to = st.text_input("To (comma-separated)",
                           key=ns_key("outreach::mail_to"),
                           placeholder="recipient@example.com, another@domain.com")
        cc = st.text_input("Cc (optional, comma-separated)", key=ns_key("outreach::mail_cc"))
        bcc = st.text_input("Bcc (optional, comma-separated)", key=ns_key("outreach::mail_bcc"))
        subj = st.text_input("Subject", key=ns_key("outreach::mail_subj"))
        body = st.text_area("Message (HTML supported)", key=ns_key("outreach::mail_body"), height=200,
                            placeholder="<p>Hello...</p>")
        files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key=ns_key("outreach::mail_files"))
        if st.button("Send email", use_container_width=True, key=ns_key("outreach::mail_send_btn")):
            try:
                send_outreach_email(ACTIVE_USER, to, subj, body, cc_addrs=cc, bcc_addrs=bcc, attachments=files)
                st.success("Email sent.")
                for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                    NS.pop(k, None)
            except Exception as e:
                st.error(f"Failed to send: {e}")

def outreach_send_from_active_user(to, subject, body_html, cc=None, bcc=None, attachments=None):
    return send_outreach_email(ACTIVE_USER, to, subject, body_html, cc_addrs=cc, bcc_addrs=bcc, attachments=attachments)
# === End Outreach Email block (moved) ===




import requests
from PyPDF2 import PdfReader
import docx
from sklearn.feature_extraction.text import TfidfVectorizer
# === OCR and clause risk helpers (injected) ===
try:
    import pytesseract  # optional
    from pdf2image import convert_from_bytes
except Exception:
    pytesseract = None
    convert_from_bytes = None

CLAUSE_RISKS = {
    "liquidated damages": "May require payments for delays. Propose realistic schedule and mitigation plan.",
    "termination for convenience": "Government can end the contract at any time. Manage inventory and subcontracts carefully.",
    "termination for default": "Strict performance risk. Include QA steps and corrective action plan.",
    "excessive bonding": "High bonding can strain cash flow. Ask if alternatives are allowed.",
    "unusual penalties": "Flag for legal review. Request clarification if ambiguous.",
    "indemnification": "Risk transfer to contractor. Verify insurance coverage.",
    "personal services": "May conflict with FAR rules if not intended. Confirm classification.",
    "pay when paid": "Cash flow risk for subs. Negotiate fair terms.",
    "liability cap absent": "Unlimited liability. Seek cap or clarify scope.",
}
def _find_clause_risks(text: str, top_k: int = 6):
    text_l = (text or "").lower()
    hits = []
    for key, hint in CLAUSE_RISKS.items():
        if key in text_l:
            hits.append({"clause": key, "hint": hint})
    return hits[:top_k]

def _ocr_pdf_bytes(pdf_bytes: bytes) -> str:
    if not (pytesseract and convert_from_bytes):
        return ""
    try:
        pages = convert_from_bytes(pdf_bytes, dpi=200)
        out = []
        for img in pages[:30]:
            out.append(pytesseract.image_to_string(img))
        return "\n".join(out)
    except Exception:
        return ""


# Optional HTML parsing for email scraper
try:
    from bs4 import BeautifulSoup  # pip install beautifulsoup4
except Exception:
    BeautifulSoup = None

# ---------- Safe key loader ----------
def _get_key(name: str) -> str:
    v = os.getenv(name, "")
    if v:
        return v
    try:
        return st.secrets[name]
    except Exception:
        return ""

OPENAI_API_KEY     = (_get_key("OPENAI_API_KEY") or "").strip()
GOOGLE_PLACES_KEY  = (_get_key("GOOGLE_PLACES_API_KEY") or "").strip()
SAM_API_KEY        = (_get_key("SAM_API_KEY") or "").strip()
MS_TENANT_ID       = (_get_key("MS_TENANT_ID") or "").strip()
MS_CLIENT_ID       = (_get_key("MS_CLIENT_ID") or "").strip()
MS_CLIENT_SECRET   = (_get_key("MS_CLIENT_SECRET") or "").strip()

# ---------- OpenAI client ----------
try:
    import openai as _openai_pkg
    from openai import OpenAI  # openai>=1.40.0 recommended
    _openai_version = getattr(_openai_pkg, "__version__", "unknown")
except Exception as e:
    st.warning("OpenAI SDK missing or too old. Chat features disabled until installed.")
    OpenAI = None

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
OPENAI_MODEL = os.getenv("OPENAI_MODEL", _get_key("OPENAI_MODEL") or "gpt-5-chat-latest")
_OPENAI_FALLBACK_MODELS = [
    OPENAI_MODEL,
    "gpt-5-chat-latest","gpt-5","gpt-5-2025-08-07",
    "gpt-5-mini","gpt-5-mini-2025-08-07",
    "gpt-5-nano","gpt-5-nano-2025-08-07",
    "gpt-4o-mini","gpt-4o",
]


def _send_via_smtp_host(to_addr: str, subject: str, body: str, from_addr: str,
                        smtp_server: str, smtp_port: int, smtp_user: str, smtp_pass: str,
                        reply_to: str | None = None) -> None:
    """Top level SMTP sender. Keeps email helpers available across the app."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg['Subject'] = subject
    if reply_to:
        msg['Reply-To'] = reply_to
    msg.attach(MIMEText(body, 'plain'))
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(from_addr, [to_addr], msg.as_string())


def _send_via_gmail(to_addr: str, subject: str, body: str) -> str:
    """
    Gmail sender using Streamlit secrets.
    Falls back to Microsoft Graph if Gmail is not configured.
    Returns "Sent" or "Preview" string to avoid crashes.
    """
    try:
        smtp_user = st.secrets.get("smtp_user")
        smtp_pass = st.secrets.get("smtp_pass")
    except Exception:
        smtp_user = smtp_pass = None

    if smtp_user and smtp_pass:
        from_addr = st.secrets.get("smtp_from", smtp_user) if hasattr(st, "secrets") else smtp_user
        reply_to = st.secrets.get("smtp_reply_to", None) if hasattr(st, "secrets") else None
        try:
            _send_via_smtp_host(to_addr, subject, body, from_addr, "smtp.gmail.com", 587, smtp_user, smtp_pass, reply_to)
            return "Sent"
        except Exception as e:
            try:
                st.warning(f"Gmail SMTP send failed: {e}")
            except Exception:
                pass
    # Fallback to Graph or preview
    try:
        sender_upn = get_setting("ms_sender_upn", "")
    except Exception:
        sender_upn = ""
    try:
        res = send_via_graph(to_addr, subject, body, sender_upn=sender_upn)
        return res if isinstance(res, str) else "Sent"
    except Exception:
        try:
            import streamlit as _st
            _st.warning("Email preview mode is active. Configure SMTP or Graph to send.")
        except Exception:
            pass
        return "Preview"

st.set_page_config(page_title="GovCon Copilot Pro", page_icon="ðŸ§°", layout="wide")

# ---- Date helpers for SAM search ----

# ---- SAM date parsing helper ----
try:
    _ = _parse_sam_date
except NameError:
    from datetime import datetime
    def _parse_sam_date(s):
        """Parse common SAM.gov date/time strings into datetime; return original on failure."""
        if s is None:
            return None
        if isinstance(s, datetime):
            return s
        txt = str(s).strip()
        # Try a few common SAM formats
        fmts = [
            "%m/%d/%Y %I:%M %p %Z",   # 09/30/2025 02:00 PM ET
            "%m/%d/%Y %H:%M %Z",      # 09/30/2025 14:00 ET
            "%m/%d/%Y %I:%M %p",      # 09/30/2025 02:00 PM
            "%m/%d/%Y %H:%M",         # 09/30/2025 14:00
            "%m/%d/%Y",               # 09/30/2025
            "%Y-%m-%dT%H:%M:%SZ",     # 2025-09-30T18:00:00Z
            "%Y-%m-%d"                # 2025-09-30
        ]
        for f in fmts:
            try:
                return datetime.strptime(txt, f)
            except Exception:
                pass
        return txt
try:
    _ = _us_date
except NameError:
    from datetime import datetime
    def _us_date(dt):
        try:
            return dt.strftime("%m/%d/%Y")
        except Exception:
            # If dt is a string or not a datetime, return as-is
            return str(dt)


# ---- Hoisted SAM helper (duplicate for e# (early use) ----

# ---- Datetime coercion helper for SAM Watch (inline before sam_search) ----
from datetime import datetime

def send_via_graph(to_addr: str, subject: str, body: str, sender_upn: str = None) -> str:
    """
    Send mail using Microsoft Graph with application permissions (client credentials).
    Uses /users/{sender}/sendMail. Returns "Sent" on success or a short diagnostic string on error.
    Env/settings used:
      - MS_TENANT_ID, MS_CLIENT_ID, MS_CLIENT_SECRET
      - MS_SENDER_UPN or settings key ms_sender_upn
    """
    try:
        import os, requests
        from urllib.parse import quote_plus
    except Exception as _e_imp:
        return f"Graph send error: missing dependency ({_e_imp})"

    # Load config: prefer env, then settings table if available
    try:
        sender = sender_upn or os.getenv("MS_SENDER_UPN") or get_setting("ms_sender_upn", "")
    except Exception:
        sender = sender_upn or os.getenv("MS_SENDER_UPN") or ""

    # MS_* may already be loaded at module level; fall back to env/settings if empty
    try:
        _tenant = os.getenv("MS_TENANT_ID") or get_setting("MS_TENANT_ID", "") or get_setting("ms_tenant_id", "")
    except Exception:
        _tenant = os.getenv("MS_TENANT_ID") or ""
    try:
        _client_id = os.getenv("MS_CLIENT_ID") or get_setting("MS_CLIENT_ID", "") or get_setting("ms_client_id", "")
    except Exception:
        _client_id = os.getenv("MS_CLIENT_ID") or ""
    try:
        _client_secret = os.getenv("MS_CLIENT_SECRET") or get_setting("MS_CLIENT_SECRET", "") or get_setting("ms_client_secret", "")
    except Exception:
        _client_secret = os.getenv("MS_CLIENT_SECRET") or ""

    if not to_addr:
        return "Missing recipient email"
    if not (_tenant and _client_id and _client_secret):
        return "Graph not configured. Set MS_TENANT_ID, MS_CLIENT_ID, MS_CLIENT_SECRET"
    if not sender:
        return "Missing sender mailbox. Set MS_SENDER_UPN or settings key ms_sender_upn"

    # Acquire app-only token
    try:
        token_r = requests.post(
            f"https://login.microsoftonline.com/{_tenant}/oauth2/v2.0/token",
            data={
                "client_id": _client_id,
                "client_secret": _client_secret,
                "scope": "https://graph.microsoft.com/.default",
                "grant_type": "client_credentials",
            },
            timeout=20,
        )
    except Exception as e:
        return f"Graph token exception: {e}"

    if token_r.status_code != 200:
        return f"Graph token error {token_r.status_code}: {token_r.text[:300]}"
    try:
        token = token_r.json().get("access_token")
    except Exception:
        token = None
    if not token:
        return f"Graph token error: {token_r.text[:300]}"

    # Build payload
    payload = {
        "message": {
            "subject": subject or "",
            "body": {"contentType": "Text", "content": body or ""},
            "toRecipients": [{"emailAddress": {"address": to_addr}}],
            "from": {"emailAddress": {"address": sender}},
        },
        "saveToSentItems": True,  # boolean must be used
    }

    send_url = f"https://graph.microsoft.com/v1.0/users/{quote_plus(sender)}/sendMail"
    try:
        r = requests.post(
            send_url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
    except Exception as e:
        return f"Graph send exception: {e}"

    if r.status_code in (200, 202):
        return "Sent"

    # surface helpful diagnostics
    try:
        err_json = r.json()
        err_txt = str(err_json)[:500]
    except Exception:
        err_txt = (r.text or "")[:500]
    return f"Graph send error {r.status_code}: {err_txt}"




# === Market pricing data helpers (robust) ===
def usaspending_search_awards(naics: str = "", psc: str = "", date_from: str = "", date_to: str = "", keyword: str = "", limit: int = 200, st_debug=None):
    import requests, pandas as pd, json
    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    type_codes = ["A","B","C","D"]
    def make_filters(n, p, k, start, end):
        f = {"time_period": [{"start_date": start, "end_date": end}], "award_type_codes": type_codes, "prime_or_sub": "prime_only"}
        if n: f["naics_codes"] = [n]
        if p: f["psc_codes"] = [p]
        if k: f["keywords"] = [k]
        return f
    if not date_from or not date_to:
        from datetime import datetime, timedelta
        end = datetime.utcnow().date().strftime("%Y-%m-%d")
        start = (datetime.utcnow().date() - timedelta(days=365*2)).strftime("%Y-%m-%d")
        date_from, date_to = date_from or start, date_to or end
    attempts = [("full", make_filters(naics, psc, keyword, date_from, date_to)),
                ("no_psc", make_filters(naics, "", keyword, date_from, date_to)),
                ("no_naics", make_filters("", psc, keyword, date_from, date_to)),
                ("keyword_only", make_filters("", "", keyword or "", date_from, date_to)),
                ("bare", make_filters("", "", "", date_from, date_to))]
    last_detail = ""
    for name, flt in attempts:
        payload = {"filters": flt, "fields": ["Award ID","Recipient Name","Start Date","End Date","Award Amount","Awarding Agency","NAICS Code","PSC Code"],
                   "page": 1, "limit": max(1, min(int(limit), 500)), "sort": "Award Amount", "order": "desc"}
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=30)
            status = r.status_code
            js = r.json() if status < 500 else {}
            rows = js.get("results", []) or []
            if rows:
                data = [{"award_id": it.get("Award ID"),
                         "recipient": it.get("Recipient Name"),
                         "start": it.get("Start Date"),
                         "end": it.get("End Date"),
                         "amount": it.get("Award Amount"),
                         "agency": it.get("Awarding Agency"),
                         "naics": it.get("NAICS Code"),
                         "psc": it.get("PSC Code")} for it in rows]
                diag = f"Attempt {name}: HTTP {status}, rows={len(rows)}"
                if st_debug is not None:
                    st_debug.code(json.dumps(payload, indent=2))
                    st_debug.caption(diag)
                return pd.DataFrame(data), diag
            else:
                last_detail = f"Attempt {name}: HTTP {status}, empty; message: {js.get('detail') or js.get('messages') or ''}"
        except Exception as e:
            last_detail = f"Attempt {name}: exception {e}"
    if st_debug is not None:
        st_debug.caption(last_detail)
    return pd.DataFrame(), last_detail

def summarize_award_prices(df):
    import numpy as np, pandas as pd
    if df is None or df.empty or "amount" not in df.columns: return {}
    vals = pd.to_numeric(df["amount"], errors="coerce").dropna()
    if vals.empty: return {}
    return {"count": int(vals.size), "min": float(vals.min()), "p25": float(np.percentile(vals,25)),
            "median": float(np.percentile(vals,50)), "p75": float(np.percentile(vals,75)),
            "max": float(vals.max()), "mean": float(vals.mean())}

def gsa_calc_rates(query: str, page: int = 1):
    import requests, pandas as pd
    url = "https://api.gsa.gov/technology/calc/search"
    params = {"q": query, "page": page}
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        js = r.json()
        items = js.get("results", []) or []
        rows = [{"vendor": it.get("vendor_name"), "labor_category": it.get("labor_category"),
                 "education": it.get("education_level"), "min_years_exp": it.get("min_years_experience"),
                 "hourly_ceiling": it.get("current_price"), "schedule": it.get("schedule"), "sin": it.get("sin")} for it in items]
        return pd.DataFrame(rows)
    except Exception:
        import pandas as pd
        return pd.DataFrame()


def _coerce_dt(x):
    if isinstance(x, datetime):
        return x
    try:
        y = _parse_sam_date(x)
        return y if isinstance(y, datetime) else None
    except Exception:
        return None

def sam_search(
    naics_list, min_days=3, limit=100, keyword=None, posted_from_days=30,
    notice_types="Combined Synopsis/Solicitation,Solicitation,Presolicitation,SRCSGT", active="true"
):
    if not SAM_API_KEY:
        return pd.DataFrame(), {"ok": False, "reason": "missing_key", "detail": "SAM_API_KEY is empty."}
    base = "https://api.sam.gov/opportunities/v2/search"
    today = datetime.utcnow().date()
    min_due_date = today + timedelta(days=min_days)
    posted_from = _us_date(today - timedelta(days=posted_from_days))
    posted_to   = _us_date(today)

    params = {
        "api_key": SAM_API_KEY,
        "limit": str(limit),
        "response": "json",
        "sort": "-publishedDate",
        "active": active,
        "postedFrom": posted_from,   # MM/dd/yyyy
        "postedTo": posted_to,       # MM/dd/yyyy
    }
    # Enforce only Solicitation + Combined when notice_types is blank
    if not notice_types:
        notice_types = "Combined Synopsis/Solicitation,Solicitation"
    params["noticeType"] = notice_types

    if naics_list:   params["naics"] = ",".join([c for c in naics_list if c][:20])
    if keyword:      params["keywords"] = keyword

    try:
        headers = {"X-Api-Key": SAM_API_KEY}
        r = requests.get(base, params=params, headers=headers, timeout=40)
        status = r.status_code
        raw_preview = (r.text or "")[:1000]
        try:
            data = r.json()
        except Exception:
            return pd.DataFrame(), {"ok": False, "reason": "bad_json", "status": status, "raw_preview": raw_preview, "detail": r.text[:800]}
        if status != 200:
            err_msg = ""
            if isinstance(data, dict):
                err_msg = data.get("message") or (data.get("error") or {}).get("message") or ""
            return pd.DataFrame(), {"ok": False, "reason": "http_error", "status": status, "message": err_msg, "detail": data, "raw_preview": raw_preview}
        if isinstance(data, dict) and data.get("message"):
            return pd.DataFrame(), {"ok": False, "reason": "api_message", "status": status, "detail": data.get("message"), "raw_preview": raw_preview}

        items = data.get("opportunitiesData", []) or []
        rows = []
        for opp in items:
            due_str = opp.get("responseDeadLine") or ""
            d = _parse_sam_date(due_str)
            d_dt = _coerce_dt(d)
            min_dt = _coerce_dt(min_due_date)
            if min_dt is None:
                due_ok = True  # allow when min date unknown
            else:
                due_ok = (d_dt is None) or (d_dt >= min_dt)
            if not due_ok: continue
            docs = opp.get("documents", []) or []
            rows.append({
                "sam_notice_id": opp.get("noticeId"),
                "title": opp.get("title"),
                "agency": opp.get("organizationName"),
                "naics": ",".join(opp.get("naicsCodes", [])),
                "psc": ",".join(opp.get("productOrServiceCodes", [])) if opp.get("productOrServiceCodes") else "",
                "place_of_performance": (opp.get("placeOfPerformance") or {}).get("city",""),
                "response_due": due_str,
                "posted": opp.get("publishedDate",""),
                "type": opp.get("type",""),
                "url": f"https://sam.gov/opp/{opp.get('noticeId')}/view",
                "attachments_json": json.dumps([{"name":d.get("fileName"),"url":d.get("url")} for d in docs])
            })
        df = pd.DataFrame(rows)
        info = {"ok": True, "status": status, "count": len(df), "raw_preview": raw_preview,
                "filters": {"naics": params.get("naics",""), "keyword": keyword or "",
                            "postedFrom": posted_from, "postedTo": posted_to,
                            "min_due_days": min_days, "noticeType": notice_types,
                            "active": active, "limit": limit}}
        if df.empty:
            info["hint"] = "Try min_days=0–1, add keyword, increase look-back, or clear noticeType."
        return df, info
    except requests.RequestException as e:
        return pd.DataFrame(), {"ok": False, "reason": "network", "detail": str(e)[:800]}



# ---- Hoisted helper implementations (duplicate for e# === SAM Watch → Contacts auto sync helpers ===

def _contacts_upsert(name: str = "", org: str = "", role: str = "", email: str = "", phone: str = "", source: str = "", notes: str = "") -> tuple:
    # Insert or light update into contacts.
    # Returns (action, id) where action is "insert" or "update".
    # Upsert rule prefers email match. If no email then uses name and org.
    try:
        conn = get_db(); cur = conn.cursor()
    except Exception:
        return ("error", None)

    email = (email or "").strip()
    name = (name or "").strip()
    org = (org or "").strip()
    role = (role or "").strip()
    phone = (phone or "").strip()
    source = (source or "SAM.gov").strip() or "SAM.gov"
    notes = (notes or "").strip()

    row = None
    try:
        if email:
            row = cur.execute("select id from contacts where lower(ifnull(email,'')) = lower(?) limit 1", (email,)).fetchone()
        if not row and (name and org):
            row = cur.execute("select id from contacts where lower(ifnull(name,''))=lower(?) and lower(ifnull(org,''))=lower(?) limit 1", (name, org)).fetchone()
    except Exception:
        row = None

    if row:
        cid = int(row[0])
        try:
            cur.execute(
                "update contacts set name=coalesce(nullif(?, ''), name), org=coalesce(nullif(?, ''), org), role=coalesce(nullif(?, ''), role), email=coalesce(nullif(?, ''), email), phone=coalesce(nullif(?, ''), phone), source=coalesce(nullif(?, ''), source), notes=case when ifnull(notes,'')='' then ? else notes end where id=?",
                (name, org, role, email, phone, source, notes, cid)
            )
            conn.commit()
        except Exception:
            pass
        return ("update", cid)

    try:
        cur.execute(
            "insert into contacts(name, org, role, email, phone, source, notes) values(?,?,?,?,?,?,?)",
            (name, org, role, email, phone, source, notes)
        )
        conn.commit()
        return ("insert", cur.lastrowid)
    except Exception:
        return ("error", None)


def _extract_contacts_from_sam_row(r) -> list:
    # Best effort extraction of POC and CO from a SAM Watch DataFrame row.
    # Returns list of dicts suitable for _contacts_upsert.
    def _g(keys):
        for k in keys:
            try:
                v = r.get(k)
            except Exception:
                v = None
            if v not in (None, float("nan")):
                s = str(v).strip()
                if s:
                    return s
        return ""

    import re
    agency = _g(["agency", "office", "department", "organization"]) or ""

    poc_name = _g(["poc_name", "primary_poc_name", "pointOfContact", "primaryPointOfContact", "contact_name"]) or ""
    poc_email = _g(["poc_email", "primary_poc_email", "pointOfContactEmail", "contact_email"]) or ""
    poc_phone = _g(["poc_phone", "primary_poc_phone", "pointOfContactPhone", "contact_phone"]) or ""

    co_name = _g(["co_name", "contracting_officer", "contractingOfficer", "buyer_name"]) or ""
    co_email = _g(["co_email", "contracting_officer_email", "buyer_email"]) or ""
    co_phone = _g(["co_phone", "contracting_officer_phone", "buyer_phone"]) or ""

    blob = _g(["description", "summary", "text", "body"]) or ""
    emails = []
    if blob:
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", blob)

    out = []
    if poc_email or poc_name or poc_phone:
        out.append({"name": poc_name, "org": agency, "role": "POC", "email": poc_email, "phone": poc_phone, "source": "SAM.gov"})
    if co_email or co_name or co_phone:
        out.append({"name": co_name, "org": agency, "role": "CO", "email": co_email, "phone": co_phone, "source": "SAM.gov"})

    if not any(c.get("email") for c in out) and emails:
        out.append({"name": "", "org": agency, "role": "POC", "email": emails[0], "phone": "", "source": "SAM.gov", "notes": "from description"})

    seen = set(); dedup = []
    for c in out:
        key = (c.get("email") or c.get("name"), c.get("org"))
        if key in seen:
            continue
        seen.add(key); dedup.append(c)
    return dedup


# (early use) ----
def google_places_search(query, location="Houston, TX", radius_m=80000, strict=True):
    """
    Google Places Text Search + Details (phone + website).
    Returns (list_of_vendors, info). Emails are NOT provided by Places.
    """
    if not GOOGLE_PLACES_KEY:
        return [], {"ok": False, "reason": "missing_key", "detail": "GOOGLE_PLACES_API_KEY is empty."}
    try:
        # 1) Text Search
        search_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        search_params = {"query": f"{query} {location}", "radius": radius_m, "key": GOOGLE_PLACES_KEY}
        rs = requests.get(search_url, params=search_params, timeout=25)
        status_code = rs.status_code
        data = rs.json() if rs.headers.get("Content-Type","").startswith("application/json") else {}
        api_status = data.get("status","")
        results = data.get("results", []) or []

        if status_code != 200 or api_status not in ("OK","ZERO_RESULTS"):
            return ([] if strict else results), {
                "ok": False, "reason": api_status or "http_error", "http": status_code,
                "api_status": api_status, "count": len(results),
                "raw_preview": (rs.text or "")[:800],
                "note": "Enable billing + 'Places API' in Google Cloud."
            }

        # 2) Details per result
        out = []
        for item in results:
            place_id = item.get("place_id")
            phone, website = "", ""
            if place_id:
                det_url = "https://maps.googleapis.com/maps/api/place/details/json"
                det_params = {"place_id": place_id, "fields": "formatted_phone_number,website", "key": GOOGLE_PLACES_KEY}
                rd = requests.get(det_url, params=det_params, timeout=20)
                det_json = rd.json() if rd.headers.get("Content-Type","").startswith("application/json") else {}
                det = det_json.get("result", {})
                phone = det.get("formatted_phone_number", "") or ""
                website = det.get("website", "") or ""

            out.append({
                "company": item.get("name"),
                "naics": "",
                "trades": "",
                "phone": phone,
                "email": "",  # Emails not provided by Google Places
                "website": website,
                "city": location.split(",")[0].strip() if "," in location else location,
                "state": location.split(",")[-1].strip() if "," in location else "",
                "certifications": "",
                "set_asides": "",
                "notes": item.get("formatted_address",""),
                "source": "GooglePlaces",
            })
        info = {"ok": True, "count": len(out), "http": status_code, "api_status": api_status,
                "raw_preview": (rs.text or "")[:800]}
        return out, info
    except Exception as e:
        return [], {"ok": False, "reason": "exception", "detail": str(e)[:500]}

def linkedin_company_search(keyword: str) -> str:
    return f"https://www.linkedin.com/search/results/companies/?keywords={quote_plus(keyword)}"

def build_context(max_rows=6):
    conn = get_db()
    g = pd.read_sql_query("select * from goals limit 1", conn)
    goals_line = ""
    if not g.empty:
        rr = g.iloc[0]
        goals_line = (f"Bids target {int(rr['bids_target'])}, submitted {int(rr['bids_submitted'])}; "
                      f"Revenue target ${float(rr['revenue_target']):,.0f}, won ${float(rr['revenue_won']):,.0f}.")
    codes = pd.read_sql_query("select code from naics_watch order by code", conn)["code"].tolist()
    naics_line = ", ".join(codes[:20]) + (" …" if len(codes) > 20 else "") if codes else "none"
    opp = pd.read_sql_query(
        "select title, agency, naics, response_due from opportunities order by posted desc limit ?",
        conn, params=(max_rows,)
    )
    opp_lines = ["- " + " | ".join(filter(None, [
        str(r["title"])[:80], str(r["agency"])[:40],
        f"due {str(r['response_due'])[:16]}", f"NAICS {str(r['naics'])[:18]}",
    ])) for _, r in opp.iterrows()]
    vend = pd.read_sql_query(
        """select trim(substr(naics,1,6)) as code, count(*) as cnt
           from vendors where ifnull(naics,'')<>''
           group by trim(substr(naics,1,6)) order by cnt desc limit ?""",
        conn, params=(max_rows,)
    )
    vend_lines = [f"- {r['code']}: {int(r['cnt'])} vendors" for _, r in vend.iterrows()]
    return "\n".join([
        f"Company: {get_setting('company_name','ELA Management LLC')}",
        f"Home location: {get_setting('home_loc','Houston, TX')}",
        f"Goals: {goals_line or 'not set'}",
        f"NAICS watch: {naics_line}",
        "Recent opportunities:" if not opp.empty else "Recent opportunities: (none)",
        *opp_lines,
        "Vendor coverage (top NAICS):" if not vend.empty else "Vendor coverage: (none)",
        *vend_lines,
    ])

# ---------- External integrations ----------



# ---- Safety helpers (fallbacks to avoid NameError at first render) ----
try:
    _ = linkedin_company_search
except NameError:
    def linkedin_company_search(q: str) -> str:
        return f"https://www.linkedin.com/search/results/companies/?keywords={quote_plus(q)}"


try:
    _ = google_places_search
except NameError:
    def google_places_search(*args, **kwargs):
        """
        Fallback stub when real google_places_search isn't loaded yet.
        Accepts flexible signatures, e.g. (query, location, radius_meters).
        Returns (results, info) where results is a list and info is a dict.
        """
        try:
            query = args[0] if len(args) >= 1 else kwargs.get("query","")
            loc = args[1] if len(args) >= 2 else kwargs.get("location","")
            radius_m = args[2] if len(args) >= 3 else kwargs.get("radius_meters", 1609)
        except Exception:
            query, loc, radius_m = "", "", 1609
        url = f"https://www.google.com/maps/search/{quote_plus(str(query)+' '+str(loc))}"
        # Provide an empty result set and metadata so callers expecting tuple unpacking won't crash
        return [], {"url": url, "note": "Fallback google_places_search stub used", "radius_m": radius_m}

try:
    _ = build_context
except NameError:
    def build_context(max_rows: int = 6) -> str:
        return ""

st.title("GovCon Copilot Pro")

def _render_identity_chip():
    try:
        conn = get_db()
        import streamlit as st
        uid = st.session_state.get("user_id")
        oid = st.session_state.get("org_id")
        uname = None
        role = None
        oname = None
        if uid:
            r = conn.execute("SELECT display_name, role FROM users WHERE id=?", (uid,)).fetchone()
            if r: uname, role = r[0], r[1]
        if oid:
            r = conn.execute("SELECT name FROM orgs WHERE id=?", (oid,)).fetchone()
            if r: oname = r[0]
        if oname or uname:
            c1, c2, c3 = st.columns([0.6,0.2,0.2])
            with c3:
                st.caption(f"Org: {oname or 'unknown'}  •  User: {uname or 'unknown'}  •  Role: {role or 'unknown'}")
    except Exception as _ex:
        import streamlit as st
        st.caption("identity: n/a")
_render_identity_chip()
st.caption("SubK sourcing • SAM watcher • proposals • outreach • CRM • goals • chat with memory & file uploads")
DB_PATH = "data/app.db"

NAICS_SEEDS = [
    "561210","721110","562991","326191","336611","531120","531","722310","561990","722514","561612",
    "561730","311511","238990","311812","561720","811210","236118","238220","237990","311423",
    "562910","236220","332420","238320","541380","541519","561710","423730","238210","562211",
    "541214","541330","541512","541511","541370","611430","611699","611310","611710","562111","562119",
    "624230","488999","485510","485410","488510","541614","332994","334220","336992","561320","561311","541214"
]

SCHEMA = {
    "vendors": """
    create table if not exists vendors (
        id integer primary key,
        company text, naics text, trades text, phone text, email text, website text,
        city text, state text, certifications text, set_asides text, notes text, source text,
        created_at text default current_timestamp, updated_at text default current_timestamp
    );
    """,
    "opportunities": """
    create table if not exists opportunities (
        id integer primary key,
        sam_notice_id text, title text, agency text, naics text, psc text,
        place_of_performance text, response_due text, posted text, type text, url text,
        attachments_json text, status text default 'New', created_at text default current_timestamp
    );
    """,
    "contacts": """
    create table if not exists contacts (
        id integer primary key,
        name text, org text, role text, email text, phone text, source text, notes text,
        created_at text default current_timestamp
    );
    """,
    "outreach_log": """
    create table if not exists outreach_log (
        id integer primary key,
        vendor_id integer, contact_method text, to_addr text, subject text, body text, sent_at text, status text,
        foreign key(vendor_id) references vendors(id)
    );
    """,
    "goals": """
    create table if not exists goals (
        id integer primary key,
        year integer, bids_target integer, revenue_target real, bids_submitted integer, revenue_won real
    );
    """,
    "settings": """
    create table if not exists settings (
        key text primary key, value text, updated_at text default current_timestamp
    );
    """,
    "email_templates": """
    create table if not exists email_templates (
        name text primary key, subject text, body text, updated_at text default current_timestamp
    );
    """,
    "naics_watch": """
    create table if not exists naics_watch (
        code text primary key, label text, created_at text default current_timestamp
    );
    """,
    "chat_sessions": """
    create table if not exists chat_sessions (
        id integer primary key, title text, created_at text default current_timestamp
    );
    """,
    "chat_messages": """
    create table if not exists chat_messages (
        id integer primary key, session_id integer, role text, content text,
        created_at text default current_timestamp,
        foreign key(session_id) references chat_sessions(id)
    );
    """,
    "chat_files": """
    create table if not exists chat_files (
        id integer primary key,
        session_id integer,
        filename text,
        mimetype text,
        content_text text,
        uploaded_at text default current_timestamp,
        foreign key(session_id) references chat_sessions(id)
    );
    """,
}

SCHEMA.update({
    "rfp_sessions": """
    create table if not exists rfp_sessions (
        id integer primary key,
        title text,
        created_at text default current_timestamp
    );
    """,
    "rfp_messages": """
    create table if not exists rfp_messages (
        id integer primary key,
        session_id integer,
        role text,
        content text,
        created_at text default current_timestamp,
        foreign key(session_id) references rfp_sessions(id)
    );
    """,
    "rfp_files": """
    create table if not exists rfp_files (
        id integer primary key,
        session_id integer,
        filename text,
        mimetype text,
        content_text text,
        uploaded_at text default current_timestamp,
        foreign key(session_id) references rfp_sessions(id)
    );
    """
})


SCHEMA.update({
    "proposal_drafts": """
    create table if not exists proposal_drafts (
        id integer primary key,
        session_id integer,
        section text,
        content text,
        updated_at text default current_timestamp,
        foreign key(session_id) references rfp_sessions(id)
    );
    """
})

# === Added schema for new features ===
SCHEMA.update({
    "deadlines": """
    create table if not exists deadlines (
        id integer primary key,
        opp_id integer,
        title text,
        due_date text,
        source text,
        status text default 'Open',
        notes text,
        created_at text default current_timestamp
    );
    """,
    "compliance_items": """
    create table if not exists compliance_items (
        id integer primary key,
        opp_id integer,
        item text,
        required integer default 1,
        status text default 'Pending',
        source_page text,
        notes text,
        created_at text default current_timestamp
    );
    """,
    "rfq_outbox": """
    create table if not exists rfq_outbox (
        id integer primary key,
        vendor_id integer,
        company text,
        to_email text,
        subject text,
        body text,
        due_date text,
        files_json text,
        sent_at text,
        status text default 'Draft',
        created_at text default current_timestamp
    );
    """,
    "pricing_scenarios": """
    create table if not exists pricing_scenarios (
        id integer primary key,
        opp_id integer,
        base_cost real,
        overhead_pct real,
        gna_pct real,
        profit_pct real,
        total_price real,
        lpta_note text,
        created_at text default current_timestamp
    );
    """
})



def parse_pick_id(pick):
    try:
        return int(str(pick).split(":")[0])
    except Exception:
        return None



def _ensure_outreach_log_columns(conn):
    cur = conn.cursor()
    try: cur.execute("alter table outreach_log add column error_text text")
    except Exception: pass
    try: cur.execute("alter table outreach_log add column try_count integer default 0")
    except Exception: pass
    conn.commit()


def normalize_vendor_website(website: str, display_link: str = None):
    if not website:
        return None
    w = website.strip().lower()
    bad_hosts = {"google.com", "www.google.com"}
    try:
        from urllib.parse import urlparse
        u = urlparse(w if w.startswith("http") else "http://" + w)
        host = u.netloc.split(":")[0]
        if host in bad_hosts:
            return None
        return (u.scheme + "://" + u.netloc + u.path).rstrip("/")
    except Exception:
        return w

def ensure_indexes(conn):
    cur = conn.cursor()
    try: cur.execute("create index if not exists idx_opp_notice on opportunities(sam_notice_id)

    try:
        conn.execute("ALTER TABLE opportunities ADD COLUMN source text")
    except Exception:
        pass
")
    except Exception: pass
    try: cur.execute("create index if not exists idx_outreach_vendor on outreach_log(vendor_id)")
    except Exception: pass
    try: cur.execute("create index if not exists idx_rfq_vendor on rfq_outbox(vendor_id)")
    except Exception: pass
    try: cur.execute("create index if not exists idx_tasks_opp on tasks(opp_id)")
    except Exception: pass
    conn.commit()


# ===== Tenancy Phase 3: Scoped DAL =====
def current_user_id():
    import streamlit as st
    return st.session_state.get("user_id") or st.session_state.get("active_user") or "anon"

def current_org_id():
    import streamlit as st
    oid = st.session_state.get("org_id") or st.session_state.get("org") or None
    if oid:
        return oid
    r = get_db().execute("SELECT id FROM orgs ORDER BY created_at LIMIT 1").fetchone()
    return r[0] if r else "default-org"

def _append_org_filter(sql: str, alias: str | None = None) -> str:
    target = f"{alias+'.' if alias else ''}org_id = ?"  # positional placeholders
    low = sql.lower()
    if " org_id " in low or " org_id=" in low or ".org_id" in low:
        return sql
    if " where " in low:
        return sql + " AND " + target
    else:
        return sql + " WHERE " + target

def q_select(sql: str, params: list | tuple = (), one: bool = False, alias: str | None = None, require_org: bool = True):
    conn = get_db()
    fin_sql = _append_org_filter(sql, alias) if require_org else sql
    fin_params = list(params) + ([current_org_id()] if require_org else [])
    cur = conn.execute(fin_sql, tuple(fin_params))
    return (cur.fetchone() if one else cur.fetchall())

def q_insert(table: str, data: dict):
    _assert_can_write()
    d = dict(data or {})
    d.setdefault("org_id", current_org_id())
    d.setdefault("owner_id", current_user_id())
    keys = list(d.keys())
    vals = [d[k] for k in keys]
    placeholders = ",".join(["?"] * len(keys))
    sql = f"INSERT INTO {table}({','.join(keys)}) VALUES({placeholders})"
    conn = get_db()
    cur = conn.execute(sql, tuple(vals))
    return cur.lastrowid

def q_update(table: str, data: dict, where: dict):
    _assert_can_write()
    if not where or "id" not in where:
        raise ValueError("q_update requires id in where")
    conn = get_db()
    d = dict(data or {})
    if "version" in where:
        d["version"] = int(where["version"]) + 1
    sets = ", ".join([f"{k}=?" for k in d.keys()])
    args = [d[k] for k in d.keys()]
    sql = f"UPDATE {table} SET {sets} WHERE id=?"
    args.append(int(where["id"]))
    sql += " AND org_id=?"
    args.append(current_org_id())
    if "version" in where:
        sql += " AND version=?"
        args.append(int(where["version"]))
    cur = conn.execute(sql, tuple(args))
    return cur.rowcount

def q_delete(table: str, where: dict):
    _assert_can_write()
    if not where or "id" not in where:
        raise ValueError("q_delete requires id in where")
    conn = get_db()
    sql = f"DELETE FROM {table} WHERE id=? AND org_id=?"
    args = (int(where["id"]), current_org_id())
    cur = conn.execute(sql, args)
    return cur.rowcount
# ===== end Tenancy Phase 3 =====


# ===== Tenancy Phase 1: Identity, Orgs, Roles =====
def _ensure_tenancy_phase1():
    """Create orgs/users if missing and seed users against a guaranteed org to avoid FK errors."""
    conn = get_db()
    cur = conn.cursor()
    # Tables
    cur.execute("CREATE TABLE IF NOT EXISTS orgs(id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS users(id TEXT PRIMARY KEY, org_id TEXT NOT NULL REFERENCES orgs(id) ON DELETE CASCADE, email TEXT NOT NULL UNIQUE, display_name TEXT, role TEXT NOT NULL CHECK(role IN('Admin','Member','Viewer')), created_at TEXT NOT NULL)")
    # Pick an org id: prefer existing; else create 'org-ela'
    row = cur.execute("SELECT id FROM orgs ORDER BY created_at LIMIT 1").fetchone()
    org_id = row[0] if row else "org-ela"
    if not row:
        cur.execute("INSERT OR IGNORE INTO orgs(id,name,created_at) VALUES(?,?,datetime('now'))", (org_id, "ELA Management LLC"))
    # Seed users referencing selected org to satisfy FK constraint
    defaults = [
        ("u-quincy", org_id, "quincy@ela.local", "Quincy", "Admin"),
        ("u-charles", org_id, "charles@ela.local", "Charles", "Member"),
        ("u-collin",  org_id, "collin@ela.local",  "Collin",  "Member"),
    ]
    for uid, oid, email, dname, role in defaults:
        cur.execute("""INSERT OR IGNORE INTO users(id,org_id,email,display_name,role,created_at)
                       VALUES(?,?,?,?,?,datetime('now'))""", (uid, oid, email, dname, role))
    conn.commit()

def current_user_role():
    import streamlit as st
    uid = st.session_state.get("user_id")
    if not uid:
        # Fallback for legacy sessions
        name = st.session_state.get("active_user")
        if name:
            row = get_db().execute("SELECT role FROM users WHERE display_name=?", (name,)).fetchone()
            return row[0] if row else "Admin"
        return "Admin"
    row = get_db().execute("SELECT role FROM users WHERE id=?", (uid,)).fetchone()
    return row[0] if row else "Admin"

def _assert_can_write():
    if current_user_role() == "Viewer":
        raise PermissionError("Viewer role cannot modify data")

_ensure_tenancy_phase1()
# ===== end Tenancy Phase 1 =====





@st.cache_resource
def get_db():
    import sqlite3, os
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/files", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
    except Exception:
        pass
    conn.execute("CREATE TABLE IF NOT EXISTS migrations(id INTEGER PRIMARY KEY, name TEXT UNIQUE, applied_at TEXT NOT NULL)")
    return conn


# ==== MIGRATION HELPER ====
def apply_ddl(stmts, name=None):
    conn = get_db()
    cur = conn.cursor()
    if name:
        cur.execute("SELECT 1 FROM migrations WHERE name=?", (name,))
        if cur.fetchone():
            return
    for s in stmts:
        cur.execute(s)
    if name:
        cur.execute("INSERT INTO migrations(name, applied_at) VALUES(?, datetime('now'))", (name,))
    try:
        conn.commit()
    except Exception:
        pass

def run_migrations():
    conn = get_db()
    cur = conn.cursor()
    # opportunities table expansions

    try: cur.execute("alter table compliance_items add column owner text")
    except Exception: pass
    try: cur.execute("alter table compliance_items add column snippet text")
    except Exception: pass
    try: cur.execute("alter table opportunities add column assignee text")
    except Exception: pass
    try: cur.execute("alter table opportunities add column quick_note text")
    except Exception: pass
    # vendors table expansions
    try: cur.execute("alter table vendors add column distance_miles real")
    except Exception: pass
    conn.commit()

def ensure_schema():
    conn = get_db()
    cur = conn.cursor()
    for ddl in SCHEMA.values(): cur.execute(ddl)
    # seed goals
    cur.execute("select count(*) from goals")
    if cur.fetchone()[0] == 0:
        cur.execute(
            "insert into goals(year,bids_target,revenue_target,bids_submitted,revenue_won) values(?,?,?,?,?)",
            (datetime.now().year, 156, 600000, 1, 0)
        )
    defaults = {
        "company_name": "ELA Management LLC",
        "home_loc": "Houston, TX",
        "default_trade": "Janitorial",
        "outreach_subject": "Quote request for upcoming federal project",
        "outreach_scope": "Routine janitorial five days weekly include supplies supervision and reporting. Provide monthly price and any one time services."
    }
    for k, v in defaults.items():
        cur.execute("insert into settings(key,value) values(?,?) on conflict(key) do nothing", (k, v))
    cur.execute("""
        insert into email_templates(name, subject, body)
        values(?,?,?)
        on conflict(name) do nothing
    """, ("RFQ Request",
          "Quote request for upcoming federal project",
          """Hello {company},



ELA Management LLC requests a quote for the following work.

Scope
{scope}

Please include unit and extended prices any exclusions start availability and certifications or set aside status.

Quote due
{due}

Thank you
ELA Management LLC
"""))
    cur.execute("select count(*) from naics_watch")
    if cur.fetchone()[0] == 0:
        for c in sorted(set(NAICS_SEEDS)):
            cur.execute("insert into naics_watch(code,label) values(?,?)", (c, c))
    conn.commit()

ensure_schema()

run_migrations()
# ---------- Utilities ----------
def get_setting(key, default=""):
    conn = get_db(); row = conn.execute("select value from settings where key=?", (key,)).fetchone()
    return row[0] if row else default

def set_setting(key, value):
    conn = get_db()
    conn.execute("""insert into settings(key,value) values(?,?)
                    on conflict(key) do update set value=excluded.value, updated_at=current_timestamp""",
                 (key, str(value)))
    conn.commit()

def read_doc(uploaded_file):
    suffix = uploaded_file.name.lower().split(".")[-1]
    if suffix in ["doc","docx"]:
        d = docx.Document(uploaded_file)
        return "\n".join(p.text for p in d.paragraphs)
    if suffix == "pdf":
        try:
            data = uploaded_file.read()
            r = PdfReader(io.BytesIO(data))
            txt = "\n".join((p.extract_text() or "") for p in r.pages)
            # OCR fallback when native text is sparse
            if len((txt or "").strip()) < 500:
                ocr_txt = _ocr_pdf_bytes(data)
                if ocr_txt and len(ocr_txt.strip()) > len((txt or "").strip()):
                    return ocr_txt
            return txt
        except Exception:
            try:
                data = uploaded_file.read()
                ocr_txt = _ocr_pdf_bytes(data)
                if ocr_txt:
                    return ocr_txt
            except Exception:
                pass
            return ""
    return uploaded_file.read().decode("utf-8", errors="ignore")


def llm(system, prompt, temp=0.2, max_tokens=1400):
    if not client: return "Set OPENAI_API_KEY to enable drafting."
    messages = [{"role":"system","content":system},{"role":"user","content":prompt}]
    last_err = None
    for model_name in _OPENAI_FALLBACK_MODELS:
        try:
            rsp = client.chat.completions.create(model=model_name, messages=messages,
                                                 temperature=temp, max_tokens=max_tokens)
            if model_name != OPENAI_MODEL:
                try: st.toast(f"Using fallback model: {model_name}", icon="âš™ï¸")
                except Exception: pass
            return rsp.choices[0].message.content
        except Exception as e:
            last_err = e; continue
    return f"LLM error ({type(last_err).__name__ if last_err else 'UnknownError'}). Tip: set OPENAI_MODEL to a model you have."

def llm_messages(messages, temp=0.2, max_tokens=1400):
    if not client: return "Set OPENAI_API_KEY to enable drafting."
    last_err = None
    for model_name in _OPENAI_FALLBACK_MODELS:
        try:
            rsp = client.chat.completions.create(model=model_name, messages=messages,
                                                 temperature=temp, max_tokens=max_tokens)
            if model_name != OPENAI_MODEL:
                try: st.toast(f"Using fallback model: {model_name}", icon="âš™ï¸")
                except Exception: pass
            return rsp.choices[0].message.content
        except Exception as e:
            last_err = e; continue
    return f"LLM error ({type(last_err).__name__ if last_err else 'UnknownError'}). Tip: set OPENAI_MODEL to a model you have."

def chunk_text(text, max_chars=1800, overlap=200):
    parts, i = [], 0
    while i < len(text):
        parts.append(text[i:i+max_chars]); i += max_chars - overlap
    return parts

def embed_texts(texts):
    vec = TfidfVectorizer(stop_words="english"); X = vec.fit_transform(texts); return vec, X

def search_chunks(query, vec, X, texts, k=6):
    qX = vec.transform([query]); sims = (X @ qX.T).toarray().ravel()
    idx = sims.argsort()[::-1][:k]; return [texts[i] for i in idx]

def to_xlsx_bytes(df_dict):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
        for name, df in df_dict.items():
            df.to_excel(w, index=False, sheet_name=name[:31])
    return bio.getvalue()



def _validate_text_for_guardrails(md_text: str, page_limit: int = None, require_font: str = None, require_size_pt: int = None,
                                  margins_in: float = None, line_spacing: float = None, filename_pattern: str = None):
    """
    Lightweight validator used across export flows.
    Returns a tuple: (issues: list[str], estimated_pages: int)
    Heuristics only — cannot actually inspect fonts from Markdown.
    """
    import math, re as _re
    text = (md_text or "").strip()
    issues = []

    # Basic placeholder checks
    if _re.search(r'\\bINSERT\\b', text) or _re.search(r'\\[[^\\]]*(insert|placeholder|tbd)[^\\]]*\\]', text, flags=_re.IGNORECASE):
        issues.append("Placeholder text 'INSERT' detected. Remove before export.")
    if _re.search(r'\\bTBD\\b|\\bTODO\\b', text):
        issues.append("Unresolved 'TBD/TODO' placeholders present.")
    if "<>" in text or "[ ]" in text:
        issues.append("Bracket placeholders found. Replace with final content.")

    # Page length heuristic: ~450 words per page at 11pt single-space
    words = _re.findall(r'\w+', text)
    est_pages = max(1, math.ceil(len(words) / 450)) if words else 1

    if page_limit and est_pages > page_limit:
        issues.append(f"Estimated length is {est_pages} pages which exceeds the {page_limit}-page limit.")

    return issues, est_pages



def _normalize_markdown_sections(md_text: str) -> str:
    """
    Clean common generation artifacts:
      • Collapse immediately repeated headings with the same text
      • Trim double spaces after heading text
    """
    if not md_text:
        return md_text
    lines = md_text.splitlines()
    out = []
    prev_heading = None
    for ln in lines:
        m = re.match(r'^(#{1,6})\s+(.*)$', ln)
        if m:
            hashes, text = m.group(1), m.group(2).rstrip()
            # Remove any trailing two-space soft break at end of headings
            text = re.sub(r'\s{2,}$', '', text)
            curr = (hashes, text.strip().lower())
            if prev_heading and curr == prev_heading:
                # skip duplicate consecutive heading
                continue
            out.append(f"{hashes} {text}")
            prev_heading = curr
        else:
            out.append(ln)
            # reset prev heading tracking once non-heading encountered
            prev_heading = None
    return "\n".join(out)

def _docx_title_if_needed(md_text: str, proposed_title: str) -> str:
    """Return empty string if md already starts with an H1, else the proposed title."""
    if not md_text:
        return proposed_title or ""
    first = md_text.lstrip().splitlines()[0] if md_text.strip() else ""
    return "" if re.match(r'^#\s+.+', first) else (proposed_title or "")


def _md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                      margins_in: float = 1.0) -> bytes:
    """
    Minimal Markdown-ish to DOCX converter:
      - Headings: lines starting with #, ##, ### map to H1/H2/H3
      - Bullets: lines starting with -, *, or • map to bullets
      - Numbered: lines like "1. text" map to numbered list (approx)
      - Everything else is a normal paragraph
    Returns bytes of the generated .docx file.
    """
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement

    doc = Document()

    # Page margins
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass

    # Base style
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        # Set for East Asia for consistent rendering in Word
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass

    # Optional document title
    if title:
        h = doc.add_heading(title, level=1)
        h.style = doc.styles["Heading 1"]

    lines = (md_text or "").splitlines()
    bullet_buf = []
    num_buf = []

    def flush_bullets():
        nonlocal bullet_buf
        for item in bullet_buf:
            p = doc.add_paragraph(item)
            p.style = doc.styles["List Bullet"]
        bullet_buf = []

    def flush_numbers():
        nonlocal num_buf
        for item in num_buf:
            p = doc.add_paragraph(item)
            p.style = doc.styles["List Number"]
        num_buf = []

    for raw in lines:
        line = raw.rstrip()

        # Blank lines flush any list buffers
        if not line.strip():
            flush_bullets(); flush_numbers()
            doc.add_paragraph("")  # spacer paragraph
            continue

        # Headings
        if line.startswith("### "):
            flush_bullets(); flush_numbers()
            doc.add_heading(line[4:].strip(), level=3)
            continue
        if line.startswith("## "):
            flush_bullets(); flush_numbers()
            doc.add_heading(line[3:].strip(), level=2)
            continue
        if line.startswith("# "):
            flush_bullets(); flush_numbers()
            doc.add_heading(line[2:].strip(), level=1)
            continue

        # Bullets
        if re.match(r"^(\-|\*|•)\s+", line):
            flush_numbers()
            bullet_buf.append(re.sub(r"^(\-|\*|•)\s+", "", line, count=1))
            continue

        # Numbered list approx (e.g., "1. step")
        if re.match(r"^\d+\.\s+", line):
            flush_bullets()
            num_buf.append(re.sub(r"^\d+\.\s+", "", line, count=1))
            continue

        # Normal paragraph
        flush_bullets(); flush_numbers()
        doc.add_paragraph(line)

    # Final flush
    flush_bullets(); flush_numbers()

    bio = io.BytesIO()
    doc.save(bio)
    bio.seek(0)
    return bio.getvalue()
    # Basic content checks
    if not body:
        issues.append("No content assembled for export.")
        return issues, 0

    # Placeholder checks
    placeholders = ["TBD", "INSERT", "[BRACKET]", "{PLACEHOLDER}", "lorem ipsum"]
    for ph in placeholders:
        if ph.lower() in body.lower():
            issues.append(f"Placeholder text '{ph}' detected. Remove before export.")

    # Word/page estimate
    words = body.split()
    est_pages = max(1, int(len(words) / 500))  # heuristic ~500 words/page
    if page_limit and est_pages > page_limit:
        issues.append(f"Estimated pages {est_pages} exceed limit {page_limit}.")

    # Font/size are enforced during DOCX build; here we flag if requested but not standard
    if require_font and require_font.lower() not in ("times new roman","arial","calibri","garamond","helvetica"):
        warnings.append(f"Requested font '{require_font}' is uncommon for federal proposals.")

    if require_size_pt and (require_size_pt < 10 or require_size_pt > 13):
        warnings.append(f"Requested font size {require_size_pt}pt is atypical for body text.")

    # Margins/spacing advisory
    if margins_in is not None and (margins_in < 0.5 or margins_in > 1.5):
        warnings.append(f"Margin {margins_in}\" may violate standard 1\" requirement.")

    if line_spacing is not None and (line_spacing < 1.0 or line_spacing > 2.0):
        warnings.append(f"Line spacing {line_spacing} looks unusual.")

    # Filename pattern
    if filename_pattern:
        # Very simple validation tokens
        tokens = ["{company}", "{solicitation}", "{section}", "{date}"]
        if not any(t in filename_pattern for t in tokens):
            warnings.append("Filename pattern lacks tokens like {company} or {date}.")

    return issues, est_pages




def _proposal_context_for(conn, session_id: int, question_text: str):
    rows = pd.read_sql_query(
        "select filename, content_text from rfp_files where session_id=? and ifnull(content_text,'')<>''",
        conn, params=(session_id,)
    )
    if rows.empty:
        return ""
    chunks, labels = [], []
    for _, r in rows.iterrows():
        cs = chunk_text(r["content_text"], max_chars=1200, overlap=200)
        chunks.extend(cs)
        labels.extend([r["filename"]]*len(cs))
    vec, X = embed_texts(chunks)
    top = search_chunks(question_text, vec, X, chunks, k=min(10, len(chunks)))
    parts, used = [], set()
    for sn in top:
        try:
            idx = chunks.index(sn); fname = labels[idx]
        except Exception:
            fname = "attachment"
        key = (fname, sn[:60])
        if key in used: continue
        used.add(key)
        parts.append(f"\n--- {fname} ---\n{sn.strip()}\n")
    return "Attached RFP snippets (most relevant first):\n" + "\n".join(parts[:16]) if parts else ""




# Injected early definition of vendor manager to avoid NameError
def _render_saved_vendors_manager(_container=None):
    import pandas as pd
    _c = _container or st
    _c.markdown("### Saved vendors")
    try:
        conn = get_db()
    except Exception as e:
        _c.error(f"DB error: {e}")
        return
    try:
        _v = pd.read_sql_query("select * from vendors order by updated_at desc, company", conn)
    except Exception as e:
        _c.warning("Vendors table missing. Creating it now...")
        try:
            cur = conn.cursor()
            cur.execute("""
            create table if not exists vendors(
                id integer primary key autoincrement,
                company text,
                naics text,
                trades text,
                phone text,
                email text,
                website text,
                city text,
                state text,
                certifications text,
                set_asides text,
                notes text,
                created_at timestamp default current_timestamp,
                updated_at timestamp default current_timestamp
            );
            """)
            conn.commit()
            _v = pd.read_sql_query("select * from vendors order by updated_at desc, company", conn)
        except Exception as ce:
            _c.error(f"Could not create/read vendors table: {ce}")
            return

    if _v.empty:
        _c.info("No vendors saved yet. Use your import above or add one manually below.")
        # Show empty editor with columns for manual add
        _v = pd.DataFrame([{
            "id": None, "company":"", "naics":"", "trades":"",
            "phone":"", "email":"", "website":"", "city":"", "state":"",
            "certifications":"", "set_asides":"", "notes":""
        }])
    else:
        _v = _v.copy()

    # Build a clickable link column
    def _mk(u):
        u = "" if u is None else str(u).strip()
        if not u:
            return ""
        if not (u.startswith("http://") or u.startswith("https://")):
            return "http://" + u
        return u

    _v["Link"] = _v.get("website", "").apply(_mk)

    editor = _c.data_editor(
        _v[[
            "id","company","naics","trades","phone","email","website","city","state",
            "certifications","set_asides","notes","Link"
        ]],
        column_config={
            "Link": st.column_config.LinkColumn("Link", display_text="Open"),
        },
        use_container_width=True,
        num_rows="dynamic",
        key="vendors_grid_tab1"
    )

    c1, c2, c3 = _c.columns([1,1,2])
    with c1:
        if _c.button("Save changes", key="vendors_save_btn_tab1"):
            try:
                cur = conn.cursor()
                try:
                    editor = editor.where(editor.notnull(), None)
                except Exception:
                    pass
                saved, updated = 0, 0
                for _, r in editor.iterrows():
                    vid = r.get("id")
                    vals = (
                        r.get("company","") or "",
                        r.get("naics","") or "",
                        r.get("trades","") or "",
                        r.get("phone","") or "",
                        r.get("email","") or "",
                        r.get("website","") or "",
                        r.get("city","") or "",
                        r.get("state","") or "",
                        r.get("certifications","") or "",
                        r.get("set_asides","") or "",
                        r.get("notes","") or "",
                    )
                    if vid is None or (isinstance(vid, float) and pd.isna(vid)) or str(vid).strip()=="" :
                        cur.execute("""insert into vendors(company,naics,trades,phone,email,website,city,state,certifications,set_asides,notes)
                                       values(?,?,?,?,?,?,?,?,?,?,?)""", vals)
                        saved += 1
                    else:
                        cur.execute("""update vendors
                                       set company=?, naics=?, trades=?, phone=?, email=?, website=?, city=?, state=?, certifications=?, set_asides=?, notes=?, updated_at=current_timestamp
                                       where id=?""", vals + (int(vid),))
                        updated += 1
                conn.commit()
                _c.success(f"Saved {saved} new, updated {updated} existing")
            except Exception as se:
                _c.error(f"Save failed: {se}")

    with c2:
        try:
            all_ids = [int(x) for x in editor.get("id", pd.Series(dtype=float)).dropna().astype(int).tolist()]
        except Exception:
            all_ids = []
        del_ids = _c.multiselect("Delete vendor IDs", options=all_ids, key="vendors_del_ids_tab1")
        if _c.button("Delete selected", key="vendors_del_btn_tab1"):
            try:
                if del_ids:
                    cur = conn.cursor()
                    for vid in del_ids:
                        cur.execute("delete from vendors where id=?", (int(vid),))
                    conn.commit()
                    _c.success(f"Deleted {len(del_ids)} vendor(s)")
            except Exception as de:
                _c.error(f"Delete failed: {de}")

    with c3:
        _c.caption("Tip: Add a new row at the bottom to create a vendor manually.")

TAB_LABELS = [
    "SAM Watch", "Pipeline", "RFP Analyzer", "L&M Checklist", "Past Performance", "RFQ Generator", "Subcontractor Finder", "Outreach", "Quote Comparison", "Pricing Calculator", "Win Probability", "Proposal Builder", "Ask the doc", "Chat Assistant", "Auto extract", "Capability Statement", "White Paper Builder", "Contacts", "Data Export", "Deals"
]
tabs = st.tabs(TAB_LABELS)

# --- UI-only hide of 'Pipeline' tab (keep backend & indices intact) ---
try:
    if "Pipeline" in TAB_LABELS:
        _pipeline_idx = TAB_LABELS.index("Pipeline") + 1  # nth-child is 1-based
        _css = "<style>\n.stTabs [role='tablist'] button:nth-child(" + str(_pipeline_idx) + ") { display: none !important; }\n</style>"
        st.markdown(_css, unsafe_allow_html=True)
except Exception:
    # Do not fail rendering if anything goes wrong
    pass

TAB = {label: i for i, label in enumerate(TAB_LABELS)}
# Backward-compatibility: keep legacy numeric indexing working
LEGACY_ORDER = [
    "Pipeline", "Subcontractor Finder", "Contacts", "Outreach", "SAM Watch", "RFP Analyzer", "Capability Statement", "White Paper Builder", "Data Export", "Auto extract", "Ask the doc", "Chat Assistant", "Proposal Builder", "Deals", "L&M Checklist", "RFQ Generator", "Pricing Calculator", "Past Performance", "Quote Comparison", "Win Probability"
]

# --- Guard: normalize legacy labels and drop missing ones ---
try:
    _TAB_ALIAS = {'Deadlines': 'Deals'}
    _labels = []
    for _lbl in LEGACY_ORDER:
        _cur = _TAB_ALIAS.get(_lbl, _lbl)
        if isinstance(TAB, dict) and _cur in TAB:
            _labels.append(_cur)
    LEGACY_ORDER = _labels
except Exception:
    pass

legacy_tabs = [tabs[TAB[label]] for label in LEGACY_ORDER]
# === Begin injected: extra schema, helpers, and three tab bodies ===
def _ensure_extra_schema():
    try:
        conn = get_db()
    except Exception:
        return
    try:
        conn.execute("""create table if not exists past_performance (
            id integer primary key,
            title text, agency text, naics text, psc text,
            period text, value real, role text, location text,
            highlights text,
            contact_name text, contact_email text, contact_phone text,
            created_at text default current_timestamp,
            updated_at text default current_timestamp
        );""")
        conn.execute("""create table if not exists vendor_quotes (
            id integer primary key,
            opp_id integer, vendor_id integer, company text,
            subtotal real, taxes real, shipping real, total real,
            lead_time text, notes text, files_json text,
            created_at text default current_timestamp
        );""")
        conn.execute("""create table if not exists win_scores (
            id integer primary key,
            opp_id integer unique, score real, factors_json text,
            computed_at text default current_timestamp
        );""")
        conn.execute("""create table if not exists tasks (
            id integer primary key,
            opp_id integer, title text, assignee text, due_date text,
            status text default 'Open', notes text,
            created_at text default current_timestamp,
            updated_at text default current_timestamp
        );""")
        conn.commit()
    except Exception:
        pass

_ensure_extra_schema()

def get_past_performance_df():
    try:
        return pd.read_sql_query("select * from past_performance order by updated_at desc, id desc", get_db())
    except Exception:
        return pd.DataFrame()

def upsert_win_score(opp_id: int, score: float, factors: dict):
    try:
        conn = get_db()
        conn.execute("""            insert into win_scores(opp_id, score, factors_json, computed_at)
            values(?,?,?, current_timestamp)
            on conflict(opp_id) do update set
                score=excluded.score,
                factors_json=excluded.factors_json,
                computed_at=current_timestamp
        """, (int(opp_id), float(score), json.dumps(factors)))
        conn.commit()
    except Exception:
        pass

def compute_win_score_row(opp_row, past_perf_df):
    from datetime import datetime as _dt
    # Factors
    score = 0
    factors = {}
    # NAICS match signal
    opp_naics = (opp_row.get("naics") or "").split(",")[0].strip()
    has_pp_same_naics = not past_perf_df[past_perf_df.get("naics", pd.Series(dtype=str)).fillna("").str.contains(opp_naics, na=False)].empty if opp_naics else False
    factors["naics_match"] = 25 if has_pp_same_naics else 10
    score += factors["naics_match"]
    # Set-aside fit signal
    t = (opp_row.get("type") or "").lower()
    setaside_fit = 20 if ("small business" in t or "total small business" in t) else 10
    factors["set_aside_fit"] = setaside_fit
    score += setaside_fit
    # Agency familiarity
    opp_agency = (opp_row.get("agency") or "").strip().lower()
    has_pp_same_agency = not past_perf_df[past_perf_df.get("agency", pd.Series(dtype=str)).fillna("").str.lower().str.contains(opp_agency)].empty if opp_agency else False
    factors["agency_familiarity"] = 25 if has_pp_same_agency else 10
    score += factors["agency_familiarity"]
    # Time runway
    try:
        due = _parse_date_any(opp_row.get("response_due") or "")
    except Exception:
        due = None
    runway = (due - _dt.now()).days if due else 21
    runway_pts = 20 if runway >= 14 else (10 if runway >= 7 else 5)
    factors["time_runway"] = runway_pts
    score += runway_pts
    # Attachment presence for clarity
    has_docs = bool(opp_row.get("attachments_json"))
    factors["docs_avail"] = 10 if has_docs else 5
    score += factors["docs_avail"]
    # Cap 100
    score = min(100, score)
    return score, factors

# Past Performance tab body (assumes appended as last-3 tab)
try:
    with legacy_tabs[-3]:
        st.subheader("Past Performance Library")
        st.caption("Create reusable blurbs linked by NAICS and agency. Insert into Proposal Builder later.")
        conn = get_db()
        df_pp = get_past_performance_df()
        st.dataframe(df_pp, use_container_width=True)

        with st.form("pp_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                title = st.text_input("Project title")
                agency = st.text_input("Agency")
                naics = st.text_input("NAICS", value="")
                psc = st.text_input("PSC", value="")
                period = st.text_input("Period", value="")
            with col2:
                value_amt = st.number_input("Contract value", min_value=0.0, step=1000.0)
                role = st.text_input("Role", value="Prime")
                location = st.text_input("Location", value="")
                highlights = st.text_area("Highlights bullets", height=120, value="• Scope coverage\n• Key metrics\n• Outcomes")
            contact_name = st.text_input("POC name", value="")
            contact_email = st.text_input("POC email", value="")
            contact_phone = st.text_input("POC phone", value="")
            submit = st.form_submit_button("Save record")
        if submit:
            conn.execute("""insert into past_performance
                (title,agency,naics,psc,period,value,role,location,highlights,contact_name,contact_email,contact_phone)
                values(?,?,?,?,?,?,?,?,?,?,?,?)""",                (title,agency,naics,psc,period,float(value_amt),role,location,highlights,contact_name,contact_email,contact_phone))
            conn.commit()
            st.success("Saved")
            _safe_rerun()
except Exception as _e_pp:
    st.caption(f"[Past Performance tab init note: {_e_pp}]")

# Quote Comparison tab body (last-2)
try:
    with legacy_tabs[-2]:
        st.subheader("Subcontractor Quote Comparison")
        conn = get_db()
        df_opp = pd.read_sql_query("select id, title from opportunities order by posted desc", conn)
        df_vendors = pd.read_sql_query("select id, company from vendors order by company", conn)
        opp_opts = [""] + [f"{int(r.id)}: {r.title}" for _, r in df_opp.iterrows()]
        opp_pick = st.selectbox("Opportunity", options=opp_opts)
        if opp_pick:
            opp_id = int(opp_pick.split(":")[0])

            with st.form("qc_add"):
                cols = st.columns(2)
                with cols[0]:
                    v_opts = [""] + [f"{int(r.id)}: {r.company}" for _, r in df_vendors.iterrows()]
                    v_pick = st.selectbox("Vendor", options=v_opts)
                    subtotal = st.number_input("Subtotal", min_value=0.0, step=100.0, value=0.0)
                    taxes = st.number_input("Taxes", min_value=0.0, step=50.0, value=0.0)
                    shipping = st.number_input("Shipping", min_value=0.0, step=50.0, value=0.0)
                with cols[1]:
                    lead_time = st.text_input("Lead time", value="")
                    notes = st.text_area("Notes", height=120, value="")
                    files = st.text_input("Files list", value="")
                add_btn = st.form_submit_button("Save quote line")
            if add_btn and v_pick:
                vendor_id = int(v_pick.split(":")[0])
                company = df_vendors[df_vendors["id"]==vendor_id]["company"].iloc[0]
                total = float(subtotal) + float(taxes) + float(shipping)
                conn.execute("""insert into vendor_quotes(opp_id, vendor_id, company, subtotal, taxes, shipping, total, lead_time, notes, files_json)
                                values(?,?,?,?,?,?,?,?,?,?)""",                             (opp_id, vendor_id, company, float(subtotal), float(taxes), float(shipping), total, lead_time, notes,
                              json.dumps([s.strip() for s in files.split(",") if s.strip()])))
                conn.commit()
                st.success("Saved")

            dfq = pd.read_sql_query("select * from vendor_quotes where opp_id=? order by total asc", conn, params=(opp_id,))
            if dfq.empty:
                st.info("No quotes yet")
            else:
                st.dataframe(dfq[["company","subtotal","taxes","shipping","total","lead_time","notes"]], use_container_width=True)
                pick_winner = st.selectbox("Pick winner", options=[""] + dfq["company"].tolist())
                if pick_winner and st.button("Pick Winner"):
                    winner_row = dfq[dfq["company"]==pick_winner].head(1)
                    if not winner_row.empty:
                        st.session_state["pricing_base_cost"] = float(winner_row["total"].iloc[0])
                    st.success(f"Winner selected {pick_winner}. Open Pricing Calculator to model markup.")
except Exception as _e_qc:
    st.caption(f"[Quote Comparison tab init note: {_e_qc}]")


    st.markdown("### Vendor ranking (scorecards)")
    try:
        conn = get_db()
        # Responsiveness proxy: count outreach_log entries per vendor with "Sent" or "Preview"
        resp = pd.read_sql_query("""
            select v.id, v.company,
                   coalesce(sum(case when o.status like 'Sent%' then 1 else 0 end),0) as sent,
                   coalesce(sum(case when o.status like 'Preview%' then 1 else 0 end),0) as preview
            from vendors v left join outreach_log o on v.id = o.vendor_id
            group by v.id, v.company
        """, conn)
        vdf = pd.read_sql_query("select id, company, certifications, set_asides, coalesce(distance_miles, 0) as distance_miles from vendors", conn)
        merged = vdf.merge(resp, how="left", on=["id","company"]).fillna({"sent":0,"preview":0})
        # Simple scoring model
        def _score_row(r):
            score = 0
            # Responsiveness
            score += min(20, (int(r["sent"]) + int(r["preview"])) * 2)
            # Certifications present
            score += 20 if (r.get("certifications") or "").strip() else 10
            # Distance (closer is better)
            d = float(r.get("distance_miles") or 0)
            score += 20 if d == 0 else (15 if d <= 25 else (10 if d <= 100 else 5))
            # Set-asides
            score += 20 if (r.get("set_asides") or "").strip() else 10
            # Past performance proxy (existence in library)
            try:
                pp = pd.read_sql_query("select count(*) as cnt from past_performance where agency like ? or naics <> ''", conn, params=(f"%{get_setting('company_name','ELA')}%",))
                has_pp = int(pp.iloc[0]["cnt"]) > 0
            except Exception:
                has_pp = False
            score += 20 if has_pp else 10
            return min(100, score)

        merged["score"] = merged.apply(_score_row, axis=1)
        merged = merged.sort_values("score", ascending=False)
        st.dataframe(merged[["company","score","certifications","set_asides","distance_miles","sent","preview"]].head(25), use_container_width=True)
    except Exception as _e_vs:
        st.caption(f"[Vendor ranking note: {_e_vs}]")


# Win Probability tab body (last-1)
try:
    with legacy_tabs[-1]:
        st.subheader("Win Probability")
        conn = get_db()
        df_opp = pd.read_sql_query("select * from opportunities order by posted desc", conn)
        df_pp = get_past_performance_df()
        if df_opp.empty:
            st.info("No opportunities in pipeline")
        else:
            rows = []
            for _, r in df_opp.iterrows():
                s, f = compute_win_score_row(r, df_pp)
                rows.append({
                    "id": r.get("id"),
                    "title": r.get("title"),
                    "agency": r.get("agency"),
                    "naics": r.get("naics"),
                    "response_due": r.get("response_due"),
                    "score": s,
                    "factors": f
                })
                try:
                    upsert_win_score(int(r.get("id")), s, f)
                except Exception:
                    pass
            df_scores = pd.DataFrame(rows).sort_values("score", ascending=False)
            st.dataframe(df_scores[["id","title","agency","naics","response_due","score"]], use_container_width=True)
            pick = st.number_input("Opportunity ID for factor breakdown", min_value=0, step=1, value=0)
            if pick:
                row = next((x for x in rows if x["id"]==int(pick)), None)
                if row:
                    st.json(row["factors"])
except Exception as _e_win:
    st.caption(f"[Win Probability tab init note: {_e_win}]")
# === End injected ===

with legacy_tabs[0]:
    st.subheader("Opportunities pipeline")
    conn = get_db()
    df_opp = pd.read_sql_query("select * from opportunities order by posted desc", conn)
    # Ensure optional columns exist
    for _col, _default in {"assignee":"", "status":"New", "quick_note":""}.items():
        if _col not in df_opp.columns:
            df_opp[_col] = _default
    import re as _re
    if "Link" not in df_opp.columns and "notes" in df_opp.columns:
        def _extract_url(_s):
            try:
                m = _re.search(r"(https?://\S+)", str(_s))
                return m.group(1).rstrip("),.;]") if m else ""
            except Exception:
                return ""
        df_opp["Link"] = df_opp["notes"].apply(_extract_url)

    assignees = ["","Quincy","Charles","Collin"]
    f1, f2 = st.columns(2)
    with f1:
        a_filter = st.selectbox(
            "Filter by assignee",
            assignees,
            index=(assignees.index(st.session_state.get('active_profile', ''))
                   if st.session_state.get('active_profile', '') in assignees else 0),
            key="opp_assignee_filter"
        )
    with f2:
        s_filter = st.selectbox(
            "Filter by status",
            ["","New","Reviewing","Bidding","Submitted"],
            index=0,
            key="opp_status_filter"
        )
    try:
        if a_filter:
            df_opp = df_opp[df_opp["assignee"].fillna("")==a_filter]
        if s_filter:
            df_opp = df_opp[df_opp["status"].fillna("")==s_filter]
    except KeyError:
        pass

    edit = st.data_editor(
        df_opp,
        column_config={
            "status": st.column_config.SelectboxColumn("status", options=["New","Reviewing","Bidding","Submitted"]),
            "assignee": st.column_config.SelectboxColumn("assignee", options=assignees),
            "Link": st.column_config.LinkColumn("Link", display_text="Open in SAM")
        },
        use_container_width=True, num_rows="dynamic", key="opp_grid"
    )
    if st.button("Save pipeline changes"):
        # Drop non-DB column before persisting
        try:
            edit.drop(columns=['Link'], inplace=True, errors='ignore')
        except Exception:
            pass
        cur = conn.cursor()
        # Make a copy of the original grid if present; else derive from filtered df
        try:
            pre_df = pre_df if "pre_df" in locals() else df_opp.copy()
        except Exception:
            pre_df = df_opp.copy()

        # Normalize IDs
        try:
            orig_ids = set(pd.to_numeric(pre_df.get("id"), errors="coerce").dropna().astype(int).tolist()) if "id" in pre_df.columns else set()
            new_ids = set(pd.to_numeric(edit.get("id"), errors="coerce").dropna().astype(int).tolist()) if "id" in edit.columns else set()
        except Exception:
            orig_ids, new_ids = set(), set()

        # Updates (rows that still exist)
        updated = 0
        if "id" in edit.columns:
            for _, r in edit.iterrows():
                try:
                    rid = int(r["id"])
                except Exception:
                    continue
                cur.execute(
                    "update opportunities set status=?, response_due=?, title=?, agency=?, assignee=?, quick_note=? where id=?",
                    (r.get("status","New"), r.get("response_due"), r.get("title"), r.get("agency"),
                     r.get("assignee",""), r.get("quick_note",""), rid)
                )
                updated += 1

        # Deletions (IDs removed from the grid)
        deleted = 0
        to_delete = list(orig_ids - new_ids)
        for rid in to_delete:
            cur.execute("delete from opportunities where id=?", (int(rid),))
            deleted += 1

        conn.commit()
        __ctx_pipeline = True
        st.success(f"Saved — updated {updated} row(s), deleted {deleted} row(s).")


# Analytics mini-dashboard (scoped to Pipeline tab)
with legacy_tabs[0]:

    # Analytics mini-dashboard
    try:
        conn = get_db()
        df_all = pd.read_sql_query("select status, count(*) as n from opportunities group by status", conn)
        if not df_all.empty:
            st.markdown("### Pipeline analytics")
            st.bar_chart(df_all.set_index("status"))
        # Forecast (probability-adjusted revenue) using win_scores if any
        try:
            dfw = pd.read_sql_query("""
                select o.id, o.title, o.agency, coalesce(w.score, 50) as score
                from opportunities o left join win_scores w on o.id = w.opp_id
            """, conn)
            if not dfw.empty:
                dfw["prob"] = dfw["score"]/100.0
                # No revenue field available, so treat prob as index only
                st.dataframe(dfw[["id","title","agency","score","prob"]])
        except Exception as _e_wa:
            st.caption(f"[Win score analytics note: {_e_wa}]")
    except Exception as _e_dash:
        st.caption(f"[Analytics dash note: {_e_dash}]")


with legacy_tabs[0]:

    if globals().get("__ctx_pipeline", False):


        st.markdown("### Tasks for selected opportunity")

        try:

            sel_id = int(st.number_input("Type an opportunity ID to manage tasks", min_value=0, step=1, value=0))

            if sel_id:

                df_tasks = pd.read_sql_query("select * from tasks where opp_id=? order by due_date asc nulls last, id desc", conn, params=(sel_id,))

                if df_tasks.empty:

                    df_tasks = pd.DataFrame(columns=["id","opp_id","title","assignee","due_date","status","notes"])

                grid_tasks = st.data_editor(df_tasks, use_container_width=True, num_rows="dynamic", key="tasks_grid")

                if st.button("Save tasks"):

                    cur = conn.cursor()

                    for _, r in grid_tasks.iterrows():

                        if pd.isna(r.get("id")):

                            cur.execute("insert into tasks(opp_id,title,assignee,due_date,status,notes) values(?,?,?,?,?,?)",

                                        (sel_id, r.get("title",""), r.get("assignee",""), r.get("due_date",""), r.get("status","Open"), r.get("notes","")))

                        else:

                            cur.execute("update tasks set title=?, assignee=?, due_date=?, status=?, notes=?, updated_at=current_timestamp where id=?",

                                        (r.get("title",""), r.get("assignee",""), r.get("due_date",""), r.get("status","Open"), r.get("notes",""), int(r.get("id"))))

                    conn.commit()

                    st.success("Tasks saved.")

        except Exception as _e_tasks:

            st.caption(f"[Tasks panel note: {_e_tasks}]")
with legacy_tabs[1]:
    st.subheader("Find subcontractors and rank by fit")
    trade = st.text_input("Trade", value=get_setting("default_trade", "Janitorial"))
    loc = st.text_input("Place of Performance", value=get_setting("home_loc", "Houston, TX"))
    radius_miles = st.slider("Radius (miles)", min_value=5, max_value=200, value=50, step=5)
    naics_choice = st.multiselect("NAICS to tag new imports", options=sorted(set(NAICS_SEEDS)), default=[])
    find_emails = st.checkbox("Try to find emails from website (slow)", value=False)
    max_pages = st.slider("Max pages per site (email crawl)", min_value=1, max_value=12, value=5)
    places_diag = st.checkbox("Show Google Places diagnostics", value=False)

    colA, colB, colC = st.columns(3)

    with colA:

        if st.button("Google Places import"):
            results, info = google_places_search(f"{trade} small business", loc, int(radius_miles*1609.34))
            st.session_state["vendor_results"] = results or []
            st.session_state["vendor_info"] = info or {}
            if places_diag:
                st.write("Places diagnostics:", info); st.code((info or {}).get("raw_preview","") or "", language="json")

        results = st.session_state.get("vendor_results") or []
        info = st.session_state.get("vendor_info") or {}

        if results:
            df_new = pd.DataFrame(results)

            # Build hyperlink column; fallback to Google search if website missing
            def _make_link(row):
                site = (row.get("website") or "").strip()
                if site:
                    return site
                comp = (row.get("company") or "").strip()
                city = (row.get("city") or "").strip()
                state = (row.get("state") or "").strip()
                q = quote_plus(" ".join(x for x in [comp, city, state, "site"] if x))
                return f"https://www.google.com/search?q={q}"

            if not df_new.empty:
                df_new["Link"] = df_new.apply(_make_link, axis=1)

            # Optional name filter
            name_filter = st.text_input("Filter by company name contains", "")
            if name_filter:
                df_new = df_new[df_new["company"].fillna("").str.contains(name_filter, case=False, na=False)]

            # Add Save checkbox per row
            if "Save" not in df_new.columns:
                df_new["Save"] = False

            # Show as editable grid with clickable links
            edited = st.data_editor(
                df_new[["company","phone","email","city","state","notes","Link","Save"]].rename(columns={
                    "company": "Company", "phone": "Phone", "email": "Email",
                    "city": "City", "state": "State", "notes": "Notes"
                }),
                column_config={
                    "Link": st.column_config.LinkColumn("Link", display_text="Open"),
                    "Save": st.column_config.CheckboxColumn("Save")
                },
                use_container_width=True,
                num_rows="fixed",
                key="vendor_import_grid"
            )

            # Save only selected rows
            save_sel = edited[edited.get("Save", False) == True] if isinstance(edited, pd.DataFrame) else pd.DataFrame()

            col_save_a, col_save_b = st.columns([1,2])
            with col_save_a:
                st.caption(f"Selected to save: {len(save_sel)} of {len(edited) if isinstance(edited, pd.DataFrame) else 0}")
                save_btn = st.button("Save selected to vendors")
            with col_save_b:
                st.caption("Tip: Click a link to review a site before saving.")

            if save_btn and not save_sel.empty:
                conn = get_db(); cur = conn.cursor()
                saved = 0
                # Include NAICS tag choice from the UI if present
                naics_tag = ",".join(naics_choice) if "naics_choice" in locals() and naics_choice else ""

                for _, r in save_sel.rename(columns={
                    "Company":"company","Phone":"phone","Email":"email",
                    "City":"city","State":"state","Notes":"notes"
                }).iterrows():
                    company = (r.get("company") or "").strip()
                    phone = (r.get("phone") or "").strip()
                    website = (r.get("Link") or "").strip()
                    email = (r.get("email") or "").strip()
                    extra_note = (r.get("notes") or "").strip()
                    city = (r.get("city") or "").strip()
                    state = (r.get("state") or "").strip()
                    source = "GooglePlaces"

                    # Dedup by website then by company+phone
                    vid = None
                    if website:
                        cur.execute("select id from vendors where website=?", (website,))
                        row = cur.fetchone()
                        if row: vid = row[0]
                    if not vid and company:
                        cur.execute("select id from vendors where company=? and ifnull(phone,'')=?", (company, phone))
                        row = cur.fetchone()
                        if row: vid = row[0]

                    if vid:
                        cur.execute(
                            "update vendors set company=?, naics=?, trades=?, phone=?, email=?, website=?, city=?, state=?, notes=?, source=?, updated_at=current_timestamp where id=?",
                            (company, naics_tag, trade, phone, email, website, city, state, extra_note, source, int(vid))
                        )
                    else:
                        cur.execute(
                            "insert into vendors(company,naics,trades,phone,email,website,city,state,certifications,set_asides,notes,source) values(?,?,?,?,?,?,?,?,?,?,?,?)",
                            (company, naics_tag, trade, phone, email, website, city, state, "", "", extra_note, source)
                        )
                    saved += 1
                conn.commit()
                st.success(f"Saved {saved} vendor(s).")
        else:
            msg = "No results"
            if info and not info.get("ok", True):
                msg += f" ({info.get('reason','')})"
            if not GOOGLE_PLACES_KEY:
                msg += " — Google Places key is missing."
            st.warning(msg)


    with colB:
        st.markdown("LinkedIn quick search")
        st.link_button("Open LinkedIn", f"https://www.linkedin.com/search/results/companies/?keywords={quote_plus(trade + ' ' + loc)}")

    with colC:
        st.markdown("Google search")
        st.link_button("Open Google", f"https://www.google.com/search?q={quote_plus(trade + ' ' + loc)}")

    st.divider()
    _render_saved_vendors_manager()  # show manager only inside Subcontractor Finder
with legacy_tabs[2]:



    st.subheader("POC and networking hub")
    st.caption("Add or clean up government POCs and vendor contacts. Link key contacts to opportunities in your notes.")
    conn = get_db()
    df_c = pd.read_sql_query("select * from contacts order by created_at desc", conn)
    grid = st.data_editor(df_c, use_container_width=True, num_rows="dynamic", key="contacts_grid")
    if st.button("Save contacts"):
        cur = conn.cursor()
        for _, r in grid.iterrows():
            if pd.isna(r["id"]):
                cur.execute("""insert into contacts(name,org,role,email,phone,source,notes) values(?,?,?,?,?,?,?)""",
                            (r["name"], r["org"], r["role"], r["email"], r["phone"], r["source"], r["notes"]))
            else:
                cur.execute("""update contacts set name=?, org=?, role=?, email=?, phone=?, source=?, notes=? where id=?""",
                            (r["name"], r["org"], r["role"], r["email"], r["phone"], r["source"], r["notes"], int(r["id"])))
        conn.commit(); st.success("Saved")

with legacy_tabs[3]:
    st.subheader("Outreach and mail merge")
    st.caption("Use default templates, personalize for distance, capability and past performance. Paste replies to track status.")

    # Render Outreach tools here (moved from sidebar)
    render_outreach_tools()

    conn = get_db(); df_v = pd.read_sql_query("select * from vendors", conn)


    # --- Template manager ---
    t = pd.read_sql_query("select * from email_templates order by name", get_db())
    names = t["name"].tolist() if not t.empty else ["RFQ Request"]
    pick_t = st.selectbox("Template", options=names, key="tpl_pick_name")
    tpl = pd.read_sql_query("select subject, body from email_templates where name=?", get_db(), params=(pick_t,))
    subj_default = tpl.iloc[0]["subject"] if not tpl.empty else get_setting("outreach_subject", "")
    body_default = tpl.iloc[0]["body"] if not tpl.empty else get_setting("outreach_scope", "")

    subj = st.text_input("Subject", value=subj_default, key="tpl_subject")
    body = st.text_area("Body with placeholders {company} {scope} {due}", value=body_default, height=220, key="tpl_body")

    colA, colB, colC, colD = st.columns([1,1,1,2])

    with colA:
        if st.button("Update selected", key="tpl_btn_update"):
            _conn = get_db()
            _conn.execute(
                """
                INSERT INTO email_templates(name, subject, body)
                VALUES(?,?,?)
                ON CONFLICT(name) DO UPDATE SET
                    subject=excluded.subject,
                    body=excluded.body,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (pick_t, subj, body),
            )
            _conn.commit()
            st.success(f"Updated '{pick_t}'")
            st.rerun()

    with colB:
        new_name = st.text_input("New name", value="", placeholder="e.g., RFQ Follow-up", key="tpl_new_name")
        if st.button("Save as new", key="tpl_btn_save_new") and new_name.strip():
            _conn = get_db()
            _conn.execute(
                """
                INSERT INTO email_templates(name, subject, body)
                VALUES(?,?,?)
                ON CONFLICT(name) DO UPDATE SET
                    subject=excluded.subject,
                    body=excluded.body,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (new_name.strip(), subj, body),
            )
            _conn.commit()
            st.success(f"Saved as '{new_name.strip()}'")
            st.rerun()

    with colC:
        confirm_del = st.checkbox("Confirm delete", key="tpl_confirm_delete")
        if st.button("Delete selected", key="tpl_btn_delete", help="Requires confirm") and confirm_del:
            _conn = get_db()
            _conn.execute("DELETE FROM email_templates WHERE name=?", (pick_t,))
            _conn.commit()
            st.warning(f"Deleted '{pick_t}'")
            st.rerun()

    with colD:
        st.caption("Tips: Use placeholders like {company}, {scope}, {due}.")
    picks = st.multiselect("Choose vendors to email", options=df_v["company"].tolist(), default=df_v["company"].tolist()[:10])
    scope_hint = st.text_area("Scope summary", value=get_setting("outreach_scope", ""))
    due = st.text_input("Quote due", value=(datetime.now()+timedelta(days=5)).strftime("%B %d, %Y 4 pm CT"))
    if st.button("Generate emails"):
        st.session_state["mail_bodies"] = []
        for name in picks:
            row = df_v[df_v["company"] == name].head(1).to_dict(orient="records")[0]
            to_addr = row.get("email","")
            body_filled = body.format(company=name, scope=scope_hint, due=due)
            st.session_state["mail_bodies"].append({"to": to_addr, "subject": subj, "body": body_filled, "vendor_id": int(row["id"])})
        st.success(f"Prepared {len(st.session_state['mail_bodies'])} emails")

        # SMTP email sender helpers
        def _send_via_smtp_host(to_addr, subject, body, from_addr, smtp_server, smtp_port, smtp_user, smtp_pass, reply_to=None):
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            msg = MIMEMultipart()
            msg['From'] = from_addr
            msg['To'] = to_addr
            msg['Subject'] = subject
            if reply_to:
                msg['Reply-To'] = reply_to
            msg.attach(MIMEText(body, 'plain'))
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.sendmail(from_addr, [to_addr], msg.as_string())

        def _send_via_gmail(to_addr, subject, body):
            # Requires st.secrets: smtp_user, smtp_pass
            smtp_user = st.secrets.get("smtp_user")
            smtp_pass = st.secrets.get("smtp_pass")
            if not smtp_user or not smtp_pass:
                raise RuntimeError("Missing smtp_user/smtp_pass in Streamlit secrets")
            from_addr = st.secrets.get("smtp_from", smtp_user)
            reply_to = st.secrets.get("smtp_reply_to", None)
            _send_via_smtp_host(to_addr, subject, body, from_addr, "smtp.gmail.com", 587, smtp_user, smtp_pass, reply_to)

        def _send_via_office365(to_addr, subject, body):
            # Requires st.secrets: smtp_user, smtp_pass
            smtp_user = st.secrets.get("smtp_user")
            smtp_pass = st.secrets.get("smtp_pass")
            if not smtp_user or not smtp_pass:
                pass
            from_addr = st.secrets.get("smtp_from", smtp_user)
            reply_to = st.secrets.get("smtp_reply_to", None)
            _send_via_smtp_host(to_addr, subject, body, from_addr, "smtp.office365.com", 587, smtp_user, smtp_pass, reply_to)

def score_opportunity(row, keywords=None, watched_naics=None):
    try:
        import pandas as pd
        score = 0
        kw = [k.strip().lower() for k in (keywords or []) if k.strip()]
        title = str(row.get("title","")).lower()
        agency = str(row.get("agency",""))
        naics = str(row.get("naics",""))
        typ = str(row.get("type",""))
        # Days until due
        due = row.get("response_due")
        days_to_due = None
        if pd.notna(due) and str(due):
            try:
                days_to_due = (pd.to_datetime(due) - pd.Timestamp.now(tz="UTC")).days
            except Exception:
                days_to_due = None
        # Keyword match boosts
        if kw:
            hits = sum(1 for k in kw if k and k in title)
            score += 15 * min(hits, 3)
        # Preferred notice types
        if typ in {"Combined Synopsis/Solicitation","Solicitation"}:
            score += 10
        # Due soon sweet spot
        if days_to_due is not None:
            if 2 <= days_to_due <= 14:
                score += 25
            elif 0 <= days_to_due < 2:
                score += 10
            elif days_to_due > 30:
                score += 5
        # NAICS match
        if watched_naics and (naics in set(watched_naics) or any(n in (naics or "") for n in watched_naics)):
            score += 20
        # Agency familiarity light boost if seen before
        if agency:
            score += 5
        return int(score)
    except Exception:
        return 0

# === Moved up: opportunity helpers to avoid NameError during SAM Watch ===

def _ensure_opportunity_columns():
    conn = get_db(); cur = conn.cursor()
    # Add columns if missing
    try: cur.execute("alter table opportunities add column status text default 'New'")
    except Exception: pass
    try: cur.execute("alter table opportunities add column assignee text")
    except Exception: pass
    try: cur.execute("alter table opportunities add column quick_note text")
    except Exception: pass
    conn.commit()

def _get_table_cols(name):
    conn = get_db(); cur = conn.cursor()
    cur.execute(f"pragma table_info({name})")
    return [r[1] for r in cur.fetchall()]

def _to_sqlite_value(v):
    # Normalize pandas/NumPy/complex types to Python primitives or None
    try:
        import numpy as np
        import pandas as pd
        if v is None:
            return None
        # Pandas NA
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        # Numpy scalars
        if isinstance(v, (np.generic,)):
            return v.item()
        # Lists/dicts -> JSON
        if isinstance(v, (list, dict)):
            return json.dumps(v)
        # Bytes -> decode
        if isinstance(v, (bytes, bytearray)):
            try:
                return v.decode("utf-8", "ignore")
            except Exception:
                return str(v)
        # Other types: cast to str for safety
        if not isinstance(v, (str, int, float)):
            return str(v)
        return v
    except Exception:
        # Fallback minimal handling
        if isinstance(v, (list, dict)):
            return json.dumps(v)
        return v

def save_opportunities(df, default_assignee=None):
    """Upsert into opportunities and handle legacy schemas gracefully."""
    if df is None or getattr(df, "empty", True):
        return 0, 0
    try:
        df = df.where(df.notnull(), None)
    except Exception:
        pass

    _ensure_opportunity_columns()
    cols = set(_get_table_cols("opportunities"))

    inserted = 0
    updated = 0
    conn = get_db(); cur = conn.cursor()
    for _, r in df.iterrows():
        nid = r.get("sam_notice_id")
        if not nid:
            continue
        cur.execute("select id from opportunities where sam_notice_id=?", (nid,))
        row = cur.fetchone()

        base_fields = {
            "sam_notice_id": nid,
            "title": r.get("title"),
            "agency": r.get("agency"),
            "naics": r.get("naics"),
            "psc": r.get("psc"),
            "place_of_performance": r.get("place_of_performance"),
            "response_due": r.get("response_due"),
            "posted": r.get("posted"),
            "type": r.get("type"),
            "url": r.get("url"),
            "attachments_json": r.get("attachments_json"),
        }
        # Sanitize all base fields
        for k, v in list(base_fields.items()):
            base_fields[k] = _to_sqlite_value(v)

        if row:
            cur.execute(
                """update opportunities set title=?, agency=?, naics=?, psc=?, place_of_performance=?,
                   response_due=?, posted=?, type=?, url=?, attachments_json=? where sam_notice_id=?""",
                (base_fields["title"], base_fields["agency"], base_fields["naics"], base_fields["psc"],
                 base_fields["place_of_performance"], base_fields["response_due"], base_fields["posted"],
                 base_fields["type"], base_fields["url"], base_fields["attachments_json"], base_fields["sam_notice_id"])
            )
            updated += 1
        else:
            insert_cols = ["sam_notice_id","title","agency","naics","psc","place_of_performance","response_due","posted","type","url","attachments_json"]
            insert_vals = [base_fields[c] for c in insert_cols]
            if "status" in cols:
                insert_cols.append("status"); insert_vals.append("New")
            if "assignee" in cols:
                insert_cols.append("assignee"); insert_vals.append(_to_sqlite_value(default_assignee or ""))
            if "quick_note" in cols:
                insert_cols.append("quick_note"); insert_vals.append("")
            placeholders = ",".join("?" for _ in insert_cols)
            cur.execute(f"insert into opportunities({','.join(insert_cols)}) values({placeholders})", insert_vals)
            inserted += 1

    conn.commit()
    return inserted, updated



# ---- SAM history table bootstrap ----
def _ensure_sam_history():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""create table if not exists sam_history(
        id integer primary key autoincrement,
        ts_utc text,
        user text,
        action text,        -- e.g., 'fetch','insert','update','save_to_pipeline','proposal_prep','digest_sent'
        sam_notice_id text,
        title text,
        agency text,
        naics text,
        response_due text,
        score integer default 0
    )""")
    conn.commit()
    return conn


# ---- Live SAM monitor ----

# --- Saved searches schema & helpers (injected) ---
def _ensure_sam_saved_searches_schema():
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("""create table if not exists sam_saved_searches(
            id integer primary key,
            name text unique,
            params_json text,
            updated_at text
        )""")
        conn.commit()
    except Exception as e:
        pass

def sam_saved_searches_upsert(name: str, params: dict):
    _ensure_sam_saved_searches_schema()
    import json, datetime
    now = datetime.datetime.utcnow().isoformat()
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("insert into sam_saved_searches(name, params_json, updated_at) values(?,?,?) on conflict(name) do update set params_json=excluded.params_json, updated_at=excluded.updated_at", (name, json.dumps(params), now))
        conn.commit()
    except Exception:
        try:
            cur.execute("update sam_saved_searches set params_json=?, updated_at=? where name=?", (json.dumps(params), now, name))
            conn.commit()
        except Exception:
            pass

def sam_saved_searches_list():
    _ensure_sam_saved_searches_schema()
    try:
        conn = get_db()
        df = pd.read_sql_query("select id, name, params_json, updated_at from sam_saved_searches order by updated_at desc", conn)
        rows = []
        for _, r in df.iterrows():
            try:
                params = json.loads(r['params_json']) if r['params_json'] else {}
            except Exception:
                params = {}
            rows.append({"id": int(r['id']), "name": r['name'], "params": params, "updated_at": r['updated_at']})
        return rows
    except Exception:
        return []
def sam_live_monitor(run_now: bool = False, hours_interval: int = 3, email_digest: bool = False, min_score_digest: int = 70):
    """
    Check if it's time to auto-fetch SAM results for the current user. If so, run the same search
    used in SAM Watch and insert new rows into opportunities. Optionally email a digest.
    """
    try:
        _ensure_sam_history()
        key_last = f"sam_last_run_{ACTIVE_USER}"
        last_run = get_setting(key_last, "")
        now_utc = pd.Timestamp.utcnow()
        do_run = run_now
        if not do_run:
            if last_run:
                try:
                    last = pd.to_datetime(last_run)
                    do_run = (now_utc - last).total_seconds() >= hours_interval*3600
                except Exception:
                    do_run = True
            else:
                do_run = True

        if not do_run:
            return {"ok": True, "skipped": True}

        # Load defaults for this user
        _defaults_key = f"sam_default_filters_{ACTIVE_USER}"
        try:
            _raw = get_setting(_defaults_key, "")
            _saved = json.loads(_raw) if _raw else {}
        except Exception:
            _saved = {}

        min_days = int(_saved.get("min_days", 3))
        posted_from_days = int(_saved.get("posted_from_days", 30))
        active_only = bool(_saved.get("active_only", True))
        keyword = str(_saved.get("keyword", ""))

        # Build filters
        conn = get_db()
        naics = pd.read_sql_query("select code from naics_watch order by code", conn)["code"].tolist()
        posted_to = pd.Timestamp.utcnow().date()
        posted_from = (posted_to - pd.Timedelta(days=posted_from_days)).isoformat()

        info, df = sam_search(naics, keyword, posted_from, str(posted_to), active_only=active_only, min_days=min_days, limit=150)

        # Insert/update into pipeline table
        new_rows, upd_rows = save_opportunities(df, default_assignee=ACTIVE_USER if ACTIVE_USER else "") if isinstance(df, pd.DataFrame) and not df.empty else (0,0)

        # Log history
        conn = get_db(); cur = conn.cursor()
        cur.execute("insert into sam_history(ts_utc,user,action,sam_notice_id,title,agency,naics,response_due,score) values(?,?,?,?,?,?,?,?,?)",
                    (str(now_utc), ACTIVE_USER, "fetch", "", "", "", "", "", 0))
        conn.commit()

        # Optional digest email
        if email_digest and isinstance(df, pd.DataFrame) and not df.empty:
            _df2 = df.copy()
            _kw = [w for w in (keyword.split() if keyword else []) if w]
            _df2["Score"] = _df2.apply(lambda r: score_opportunity(r, _kw, naics), axis=1)
            best = _df2[_df2["Score"]>=int(min_score_digest)].sort_values("Score", ascending=False).head(10)
            if not best.empty and USER_EMAILS.get(ACTIVE_USER, ""):
                lines = ["Top SAM results (auto digest)"]
                for _, r in best.iterrows():
                    lines.append(f"• [{int(r['Score'])}] {str(r.get('title',''))[:90]} — {str(r.get('agency',''))[:40]} (due {str(r.get('response_due',''))[:16]})<br>{str(r.get('url',''))}")
                try:
                    send_outreach_email(ACTIVE_USER, USER_EMAILS.get(ACTIVE_USER), "SAM Watch: Daily digest", "<br>".join(lines))
                    cur.execute("insert into sam_history(ts_utc,user,action) values(?,?,?)", (str(now_utc), ACTIVE_USER, "digest_sent"))
                    conn.commit()
                except Exception as _e:
                    pass

        set_setting(key_last, str(now_utc))
        return {"ok": True, "inserted": int(new_rows), "updated": int(upd_rows)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


    # ---- Auto proposal prep from SAM row ----
    def build_proposal_md_from_row(row: dict) -> str:
        title = str(row.get("title",""))
        agency = str(row.get("agency",""))
        sol = str(row.get("sam_notice_id",""))
        due = str(row.get("response_due",""))
        naics = str(row.get("naics",""))
        url = str(row.get("url",""))
        company = get_setting("company_name", "ELA Management LLC")
        uei = get_setting("uei", "")
        cage = get_setting("cage", "")
        phone = get_setting("company_phone", "")
        email = USER_EMAILS.get(ACTIVE_USER, get_setting("company_email",""))
        summary = str(row.get("description",""))[:1200]

        md = f"""# {company} – Proposal Draft
**Opportunity:** {title}
**Agency:** {agency}
**Solicitation #:** {sol}
**NAICS:** {naics}
**Response Due:** {due}
**URL:** {url}

## Cover Letter
Dear Contracting Officer,

{company} is pleased to submit our response to **{title}** with **{agency}**. We are a small business with relevant past performance and a robust project management framework to ensure quality, timeliness, and cost control.

## Technical Approach
- Project management methodology to coordinate staffing, scheduling, and quality checks
- Compliance with all federal, state, and local regulations
- Quality Assurance plan aligned to solicitation requirements

## Management Plan
- Key personnel and supervision structure
- Communication cadence and reporting
- Risk mitigation and continuity of operations

## Relevant Past Performance
- Insert your top 3 contracts matching scope and size

## Pricing Notes
- Pricing workbook attached separately
- Assumptions and clarifications kept minimal

## Contact
**UEI:** {uei}
**CAGE:** {cage}
**Phone:** {phone}
**Email:** {email}

*Auto-generated from SAM Watch listing. Please replace placeholders before submission.*
"""
        return md

with legacy_tabs[4]:
    # Show a helper tip only inside SAM Watch when no results have been loaded

    if ('sam_results_df' not in st.session_state) or (st.session_state['sam_results_df'] is None) or (hasattr(st.session_state['sam_results_df'], 'empty') and st.session_state['sam_results_df'].empty):

        st.info('No active results yet. Click **Run search now**.')


    with st.expander("Saved Searches", expanded=True):
        cols = st.columns([2,2,2,2,1,1])
        with cols[0]: ss_name = st.text_input("Name", key="ss_name")
        with cols[1]: ss_keyword = st.text_input("Keyword", value=str(st.session_state.get("sam_keyword","")), key="ss_keyword_builder")
        with cols[2]: ss_naics = st.text_input("NAICS list (comma-separated)", value=st.session_state.get("naics_default",""), key="ss_naics_builder")
        with cols[3]: ss_notice = st.multiselect("Notice types", ["Combined Synopsis/Solicitation","Solicitation","Presolicitation","Sources Sought"], default=["Combined Synopsis/Solicitation","Solicitation"], key="ss_notice_builder")
        with cols[4]: ss_min_days = st.number_input("Min days", min_value=0, step=1, value=int(0), key="ss_min_days_builder")
        with cols[5]: ss_posted = st.number_input("Posted within days", min_value=1, step=1, value=int(60), key="ss_posted_builder")
        active_only = st.checkbox("Active only", value=True, key="ss_active_only_builder")
        if st.button("Save search"):
            _params = {
                "keyword": ss_keyword.strip(),
                "naics_list": [s.strip() for s in ss_naics.split(",") if s.strip()],
                "notice_types": ",".join(ss_notice),
                "min_days": int(ss_min_days),
                "posted_from_days": int(ss_posted),
                "active": "true" if active_only else "false"
            }
            try:
                sam_saved_upsert(ss_name.strip(), _params, active=True)
                st.success(f"Saved search '{ss_name}'")
            except Exception as e:
                st.error(f"Failed to save: {e}")

        try:
            saved = sam_saved_list()
            if saved:
                st.write("Saved searches:")
                for row in saved:
                    c1,c2,c3,c4,c5 = st.columns([2,3,2,2,2])
                    c1.write(f"**{row['name']}**")
                    c2.write(row.get("params",{}))
                    if c3.button("Run", key=f"run_{row['name']}"):
                        pars = row['params']
                        df_run, info = sam_search(
                            pars.get("naics_list", []),
                            min_days=int(pars.get("min_days",0)),
                            limit=200,
                            keyword=pars.get("keyword",""),
                            posted_from_days=int(pars.get("posted_from_days",60)),
                            notice_types=pars.get("notice_types",""),
                            active=pars.get("active","true")
                        )
                        st.session_state["sam_results_df"] = df_run
                        st.success(f"Found {len(df_run)} results for '{row['name']}'")
                    if c4.button("Run & Ingest", key=f"run_ingest_{row['name']}"):
                        pars = row['params']
                        df_run, info = sam_search(
                            pars.get("naics_list", []),
                            min_days=int(pars.get("min_days",0)),
                            limit=200,
                            keyword=pars.get("keyword",""),
                            posted_from_days=int(pars.get("posted_from_days",60)),
                            notice_types=pars.get("notice_types",""),
                            active=pars.get("active","true")
                        )
                        if hasattr(df_run, "empty") and not df_run.empty:
                            to_save = df_run.copy()
                            if "Link" in to_save.columns:
                                to_save = to_save.drop(columns=["Link"])
                            ins, upd = save_opportunities(to_save, default_assignee=st.session_state.get("assignee_default",""))
                            st.success(f"Ingested {len(df_run)}. New {ins}, updated {upd}.")
                            st.session_state["sam_results_df"] = df_run
                        else:
                            st.info("No results to ingest.")
                    if c5.button("Delete", key=f"del_{row['name']}"):
                        sam_saved_delete(row['name'])
                        st.warning(f"Deleted '{row['name']}'. Refresh to update list.")
        except Exception as e:
            st.error(f"Saved searches error: {e}")
    st.subheader("SAM Watch: Auto Search + Attachments + Saved Searches")
    st.markdown("> **Flow:** Set All active → apply filters → open attachments → choose assignee → **Search** then **Save to pipeline**")
    conn = get_db()
    codes = pd.read_sql_query("select code from naics_watch order by code", conn)["code"].tolist()
    st.caption(f"Using NAICS codes: {', '.join(codes) if codes else 'none'}")

    auto_on = st.checkbox("Enable auto-monitor", value=bool(get_setting(f"sam_auto_{ACTIVE_USER}", "true") != "false"))
    interval_hours = st.number_input("Auto-monitor every (hours)", min_value=1, max_value=24, value=int(get_setting(f"sam_interval_{ACTIVE_USER}", "3") or 3))
    digest = st.checkbox("Send daily digest email", value=bool(get_setting(f"sam_digest_{ACTIVE_USER}", "true") != "false"))
    digest_min = st.number_input("Digest min score", min_value=0, max_value=100, value=70, step=5)
    if st.button("Save monitor settings"):
        set_setting(f"sam_auto_{ACTIVE_USER}", "true" if auto_on else "false")
        set_setting(f"sam_interval_{ACTIVE_USER}", str(int(interval_hours)))
        set_setting(f"sam_digest_{ACTIVE_USER}", "true" if digest else "false")
        set_setting(f"sam_digestmin_{ACTIVE_USER}", str(int(digest_min)))
        st.success("Saved monitor settings")

    # Kick the monitor if interval elapsed
    try:
        if auto_on:
            _res = sam_live_monitor(False, int(interval_hours), digest, int(digest_min))
            if _res and _res.get("ok") and not _res.get("skipped"):
                st.info(f"Auto-monitor: inserted {_res.get('inserted',0)}, updated {_res.get('updated',0)}")
    except Exception as _e_mon:
        st.caption(f"[Monitor note: {_e_mon}]")

# --- Per-user default filters
_defaults_key = f"sam_default_filters_{ACTIVE_USER}"
try:
    _raw = get_setting(_defaults_key, "")
    _saved_defaults = json.loads(_raw) if _raw else {}
except Exception:
    _saved_defaults = {}



    col1, col2, col3 = st.columns(3)
    with col1:
        min_days = st.number_input("Minimum days until due", min_value=0, step=1, value=int(_saved_defaults.get('min_days', 3)))
        posted_from_days = st.number_input("Posted window (days back)", min_value=1, step=1, value=int(_saved_defaults.get('posted_from_days', 30)))
        active_only = st.checkbox("All active opportunities", value=bool(_saved_defaults.get('active_only', True)))
    with col2:
        keyword = st.text_input("Keyword", value=str(_saved_defaults.get('keyword', '')), key="sam_keyword")
        notice_types = st.multiselect("Notice types", options=["Combined Synopsis/Solicitation","Solicitation","Presolicitation","SRCSGT"], default=_saved_defaults.get("notice_types", ["Combined Synopsis/Solicitation","Solicitation"]))
    with col3:
        diag = st.checkbox("Show diagnostics", value=False)
        raw = st.checkbox("Show raw API text (debug)", value=False)
        assignee_default = st.selectbox("Default assignee", ["","Quincy","Charles","Collin"], index=(['','Quincy','Charles','Collin'].index(st.session_state.get('active_profile','')) if st.session_state.get('active_profile','') in ['Quincy','Charles','Collin'] else 0))
        st.markdown("**Defaults**")
        if st.button("Save as my default"):
            # Add set_aside to saved defaults if a UI variable named set_aside exists
            try:
                _set_aside_vals = list(set_aside)  # may be defined elsewhere in SAM Watch
            except Exception:
                _set_aside_vals = _saved_defaults.get("set_aside", [])
            # Rewrite saved defaults to include set_aside if present
            try:
                _raw = get_setting(_defaults_key, "")
                _cur = json.loads(_raw) if _raw else {}
            except Exception:
                _cur = {}
            _cur.update({
                'min_days': int(min_days),
                'posted_from_days': int(posted_from_days),
                'active_only': bool(active_only),
                'keyword': str(keyword or ''),
                'notice_types': list(notice_types),
                'set_aside': list(_set_aside_vals)
            })
            set_setting(_defaults_key, json.dumps(_cur))
            st.success("Saved your defaults")
        if st.button("Reset my default"):
            set_setting(_defaults_key, "")
            st.info("Cleared your saved defaults")

        # --- Saved Searches manager ---
        st.markdown("### Saved Searches")
        _ensure_sam_saved_searches_schema()
        _ss_list = sam_saved_searches_list()
        if _ss_list:
            names = [f"{row['name']} (updated {row['updated_at'][:10]})" for row in _ss_list]
            pick_idx = st.selectbox("Choose a saved search", list(range(len(names))), format_func=lambda i: names[i] if names else "", key="sam_ss_pick")
            chosen = _ss_list[pick_idx] if _ss_list else None
        else:
            st.caption("No saved searches yet.")
            chosen = None

        with st.expander("Create or update a saved search"):
            ss_name = st.text_input("Search name", value="")
            ss_keyword = st.text_input("Keyword(s) for this saved search", value=str(keyword or ""))
            ss_naics = st.text_input("NAICS list (comma separated)", value="")
            ss_notice = st.multiselect("Notice types", options=["Combined Synopsis/Solicitation","Solicitation","Presolicitation","SRCSGT"], default=["Combined Synopsis/Solicitation","Solicitation"], key="sam_ss_notice")
            ss_min_days = st.number_input("Min days until due", min_value=0, step=1, value=int(min_days))
            ss_posted_from_days = st.number_input("Look-back window (days since posted)", min_value=1, step=1, value=int(posted_from_days))
            if st.button("Save search"):
                params = {
                    "keyword": ss_keyword.strip(),
                    "naics_list": [s.strip() for s in ss_naics.split(",") if s.strip()],
                    "notice_types": ",".join(ss_notice),
                    "min_days": int(ss_min_days),
                    "posted_from_days": int(ss_posted_from_days),
                    "active": "true" if active_only else "false",
                    "limit": 100
                }
                sam_saved_searches_upsert(ss_name.strip(), params)
                st.success(f"Saved search '{ss_name.strip()}'")

        if chosen:
            st.markdown("#### Run selected saved search")
            if st.button("Run & Ingest to Pipeline"):
                _df, _info = sam_search(
                    naics_list=chosen['params'].get('naics_list') or [],
                    min_days=int(chosen['params'].get('min_days', 3)),
                    limit=int(chosen['params'].get('limit', 100)),
                    keyword=chosen['params'].get('keyword') or None,
                    posted_from_days=int(chosen['params'].get('posted_from_days', 30)),
                    notice_types=chosen['params'].get('notice_types', "Combined Synopsis/Solicitation,Solicitation"),
                    active=chosen['params'].get('active', "true")
                )
                st.dataframe(_df.head(50), use_container_width=True)
                if not _df.empty:
                    _added, _updated = 0, 0
                    for _, r in _df.iterrows():
                        action, _id = _opportunities_upsert(
                            title=str(r.get("title","")),
                            agency=str(r.get("agency","")),
                            naics=str(r.get("naics","")),
                            response_due=str(r.get("response_due","")),
                            url=str(r.get("url","")),
                            data=r.to_dict()
                        )
                        if action == "insert": _added += 1
                        elif action == "update": _updated += 1
                    st.success(f"Ingested to pipeline: added {_added}, updated {_updated}")

        # --- Auto-ingest scheduler ---
        st.markdown("### Auto-ingest")
        toggle_auto = st.checkbox("Enable background auto-ingest (every N hours)", value=bool(get_setting("sam_auto_ingest_enabled","")=="1"))
        every_hours = st.slider("Frequency (hours)", min_value=1, max_value=24, value=int(get_setting("sam_auto_ingest_hours","3") or 3))
        email_digest = st.checkbox("Email a digest when new matches found", value=bool(get_setting("sam_auto_ingest_email","")=="1"))
        min_score = st.slider("Min score for digest", min_value=0, max_value=100, value=int(get_setting("sam_auto_ingest_min_score","70") or 70))
        if st.button("Save auto-ingest settings"):
            set_setting("sam_auto_ingest_enabled", "1" if toggle_auto else "0")
            set_setting("sam_auto_ingest_hours", str(every_hours))
            set_setting("sam_auto_ingest_email", "1" if email_digest else "0")
            set_setting("sam_auto_ingest_min_score", str(min_score))
            st.success("Saved auto-ingest settings.")
            # Optionally trigger a run now
            if st.checkbox("Run one cycle now"):
                _r = sam_live_monitor(run_now=True, hours_interval=int(every_hours), email_digest=bool(email_digest), min_score_digest=int(min_score))
                st.write(_r)



    cA, cB, cC = st.columns(3)

    # Run search stores results in session so Save works after rerun
    with cA:
        # Fallback in case the number_input did not run in this branch

        pages_to_fetch = st.session_state.get("pages_to_fetch", 3)
        email_top = st.checkbox("Email me the top results", value=False)
        min_score_email = st.number_input("Min score to email", min_value=0, max_value=100, value=70, step=5)
        email_to_self = st.text_input("Send to (your email)", value=USER_EMAILS.get(ACTIVE_USER, ""))

        if st.button("Run search now"):
            df, info = sam_search(
                codes, min_days=min_days, limit=150,
                keyword=keyword or None, posted_from_days=int(posted_from_days),
                notice_types="Combined Synopsis/Solicitation,Solicitation", active="true"
            )
            st.session_state["sam_results_df"] = df
            st.session_state["sam_results_info"] = info
            # ## Email top results
            try:
                if email_top and isinstance(df, pd.DataFrame) and not df.empty and email_to_self:
                    _df2 = df.copy()
                    _df2["Score"] = _df2.apply(lambda r: score_opportunity(r, _kw if ' _kw' in locals() else (keyword.split() if keyword else []), codes if isinstance(codes, list) else []), axis=1)
                    _df2 = _df2.sort_values("Score", ascending=False)
                    best = _df2[_df2["Score"]>=int(min_score_email)].head(10)
                    if not best.empty:
                        lines = ["Top SAM results (auto)"]
                        for _, r in best.iterrows():
                            lines.append(f"• [{int(r['Score'])}] {str(r.get('title',''))[:90]} — {str(r.get('agency',''))[:40]} (due {str(r.get('response_due',''))[:16]})\n{str(r.get('url',''))}")
                        try:
                            send_outreach_email(ACTIVE_USER, email_to_self, "SAM Watch: Top matches", "<br>".join(lines))
                            st.info(f"Emailed {len(best)} matches to {email_to_self}")
                        except Exception as _e_mail:
                            st.caption(f"[Email note: {_e_mail}]")
            except Exception as _e_email:
                st.caption(f"[Email block note: {_e_email}]")
            if diag:
                st.write("Diagnostics:", info)
                st.code(f"naics={','.join(codes[:20])} | keyword={keyword or ''} | postedFrom={info.get('filters',{}).get('postedFrom')} -> postedTo={info.get('filters',{}).get('postedTo')} | min_days={min_days} | limit=150", language="text")
            # Optional: email top results once computed below after scoring
            if raw:
                st.code((info or {}).get("raw_preview","") or "", language="json")

    # Show results from session (if any)
    df = st.session_state.get("sam_results_df")
    info = st.session_state.get("sam_results_info", {}) or {}
    if info and not info.get("ok", True):
        st.error(f"SAM API error: {info}")

    elif isinstance(df, pd.DataFrame) and not df.empty:
        # Hard client-side filter (belt-and-suspenders)
        allowed_types = {"Combined Synopsis/Solicitation", "Solicitation"}
        if "type" in df.columns:
            df = df[df["type"].isin(allowed_types)].copy()
        # Build interactive grid with hyperlink and Save checkbox
        grid_df = df.copy()
        grid_df["Link"] = grid_df["url"]
        if "Save" not in grid_df.columns:
            grid_df["Save"] = False

        # Compute Score
        _kw = [w for w in (keyword.split() if isinstance(keyword, str) else []) if w]
        try:
            watched = codes if isinstance(codes, list) else []
        except Exception:
            watched = []
        grid_df["Score"] = grid_df.apply(lambda r: score_opportunity(r, _kw, watched), axis=1)
        # Sort by Score desc then due date asc
        if "response_due" in grid_df.columns:
            try:
                _dt = pd.to_datetime(grid_df["response_due"], errors="coerce")
                grid_df = grid_df.assign(_due=_dt).sort_values(["Score","_due"], ascending=[False, True]).drop(columns=["_due"])
            except Exception:
                grid_df = grid_df.sort_values(["Score"], ascending=[False])
        else:
            grid_df = grid_df.sort_values(["Score"], ascending=[False])

        with st.expander("Quick select options"):
            n_top = st.number_input("Select top N by score", min_value=1, max_value=max(1, min(50, len(grid_df))), value=min(5, len(grid_df)))
            if st.button("Mark top N for Save"):
                try:
                    top_idx = grid_df.sort_values("Score", ascending=False).head(int(n_top)).index
                    df.loc[top_idx, "Save"] = True
                    grid_df.loc[top_idx, "Save"] = True
                    st.success(f"Selected {int(n_top)} rows")
                except Exception as e:
                    st.warning(f"Could not select top rows: {e}")

        edited = st.data_editor(
            grid_df,
            column_config={
                "Link": st.column_config.LinkColumn("Link", display_text="Open in SAM")
            },
            use_container_width=True,
            num_rows="fixed",
            key="sam_watch_grid"
        )
        # Save only selected rows
        save_sel = edited[edited.get("Save", False)==True] if "Save" in edited.columns else edited.iloc[0:0]
        st.caption(f"Selected to save: {len(save_sel)} of {len(edited)}")

        if st.button("Save selected to pipeline"):
            to_save = save_sel.drop(columns=[c for c in ["Save","Link"] if c in save_sel.columns])
            ins, upd = save_opportunities(to_save, default_assignee=assignee_default)
            st.success(f"Saved to pipeline — inserted {ins}, updated {upd}.")
            # === Auto add POCs and COs to Contacts after saving to pipeline ===
try:
    _ss = locals().get('save_sel', None)
    if isinstance(_ss, pd.DataFrame) and not _ss.empty:
        added, updated = 0, 0
        for _, _r in _ss.iterrows():
            for c in _extract_contacts_from_sam_row(_r):
                act, _ = _contacts_upsert(
                    name=c.get("name",""), org=c.get("org",""), role=c.get("role",""),
                    email=c.get("email",""), phone=c.get("phone",""),
                    source=c.get("source","SAM.gov"), notes=c.get("notes","")
                )
                if act == "insert":
                    added += 1
                elif act == "update":
                    updated += 1
        if added or updated:
            try:
                st.toast(f"Contacts synced from SAM Watch added {added} updated {updated}")
            except Exception:
                st.caption(f"Contacts synced from SAM Watch added {added} updated {updated}")
except Exception as _e_sync:
    try:
        st.caption(f"[Contacts sync note: {_e_sync}]")
    except Exception:
        pass
# Proposal drafts for selected
# [disabled to fix indentation]             if len(save_sel) > 0:
# [disabled to fix indentation]                 st.markdown("#### Auto Proposal Prep")
# [disabled to fix indentation]                 for _i, _r in save_sel.iterrows():
# [disabled to fix indentation]                     _md = build_proposal_md_from_row(_r)
# [disabled to fix indentation]                     _bytes = md_to_docx_bytes_rich(_md, title=str(_r.get('title','')))
# [disabled to fix indentation]                     st.download_button("Prep Proposal DOCX: " + str(_r.get('sam_notice_id','') or _r.get('title',''))[:40], data=_bytes, file_name=f"proposal_{str(_r.get('sam_notice_id','') or _i)}.docx", mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
# [disabled to fix indentation]                     try:
# [disabled to fix indentation]                         cur = get_db().cursor()
# [disabled to fix indentation]                         cur.execute("insert into sam_history(ts_utc,user,action,sam_notice_id,title,agency,naics,response_due,score) values(?,?,?,?,?,?,?,?,?)",
# [disabled to fix indentation]                                     (str(pd.Timestamp.utcnow()), ACTIVE_USER, "proposal_prep", str(_r.get('sam_notice_id','')), str(_r.get('title','')), str(_r.get('agency','')), str(_r.get('naics','')), str(_r.get('response_due','')), int(_r.get('Score',0))))
# [disabled to fix indentation]                         get_db().commit()
# [disabled to fix indentation]                     except Exception:
# [disabled to fix indentation]                         pass
# [disabled to fix indentation]             if len(save_sel) > 0:
# [disabled to fix indentation]                 try:
# [disabled to fix indentation]                     st.markdown("#### Prep outreach drafts for CO (email placeholders)")
# [disabled to fix indentation]                     if st.button("Create outreach drafts in Outreach tab"):
# [disabled to fix indentation]                         bods = []
# [disabled to fix indentation]                         for _, r in save_sel.iterrows():
# [disabled to fix indentation]                             subj = f"Inquiry: {str(r.get('title',''))[:60]}"
# [disabled to fix indentation]                             body = f"<p>Hello Contracting Officer,</p><p>We reviewed <strong>{str(r.get('title',''))}</strong> at {str(r.get('agency',''))}. We have relevant past performance and would like to confirm points of contact and any site-visit details.</p><p>Regards,<br>{get_setting('company_name','ELA Management LLC')}</p>"
# [disabled to fix indentation]                             bods.append({"to":"","subject":subj,"body":body,"vendor_id":0})
# [disabled to fix indentation]                         st.session_state['mail_bodies'] = bods
# [disabled to fix indentation]                         st.success("Drafts prepared — open the Outreach tab to review and send.")
# [disabled to fix indentation]                 except Exception as _e_prep:
# [disabled to fix indentation]                     st.caption(f"[CO outreach prep note: {_e_prep}]")
    else:
        cA, cB, cC = st.columns(3)
    with cB:
        if st.button("Broad test (keyword only)"):
            kw = (st.session_state.get("sam_keyword", "") or "").strip() or "janitorial"
            df, info = sam_search(
                [], min_days=0, limit=100, keyword=kw, posted_from_days=60,
                notice_types="Combined Synopsis/Solicitation,Solicitation", active="true"
            )
            st.session_state["sam_results_df"] = df
            st.session_state["sam_results_info"] = info
            st.success(f"Test search complete for keyword: {kw}")

    with cC:
        if st.button("Test SAM key only"):
            try:
                today_us = _us_date(datetime.utcnow().date())
                test_params = {"api_key": SAM_API_KEY, "limit": "1", "response": "json", "postedFrom": today_us, "postedTo": today_us}
                headers = {"X-Api-Key": SAM_API_KEY}
                r = requests.get("https://api.sam.gov/opportunities/v2/search", params=test_params, headers=headers, timeout=20)
                st.write("HTTP", r.status_code)
                text_preview = (r.text or "")[:1000]
                try:
                    jj = r.json()
                except Exception:
                    jj = {"raw": text_preview}
                st.code(json.dumps(jj, indent=2)[:1200])
            except Exception as e:
                st.error(f"Key test failed: {e}")

# Removed RFP mini-analyzer from SAM Watch

# (moved) RFP Analyzer call will be added after definition




# --- Analytics & History ---
with legacy_tabs[4]:
    with st.expander("SAM Analytics"):
        conn = get_db()
        try:
            hist = pd.read_sql_query("select * from sam_history order by ts_utc desc", conn)
            st.dataframe(hist.head(200))
            # Simple aggregates
            st.write("Total fetches:", int((hist["action"]=="fetch").sum()) if "action" in hist else 0)
            st.write("Proposals prepped:", int((hist["action"]=="proposal_prep").sum()) if "action" in hist else 0)
            # Monthly new opportunities (approx: use fetch counts as proxy)
            if not hist.empty and "ts_utc" in hist.columns:
                _h = hist.copy(); _h["month"] = pd.to_datetime(_h["ts_utc"], errors="coerce").dt.to_period("M").astype(str)
                agg = _h.groupby(["month","action"]).size().reset_index(name="n")
                st.write("Activity by month")
                st.dataframe(agg.sort_values(["month","action"]))
        except Exception as _e_ana:
            st.caption(f"[Analytics note: {_e_ana}]")
with legacy_tabs[6]:
    st.subheader("Capability statement builder")
    company = get_setting("company_name", "ELA Management LLC")
    tagline = st.text_input("Tagline", key="cap_tagline_input_capability_builder", value="Responsive project management for federal facilities and services")
    core = st.text_area("Core competencies", key="cap_core_textarea_capability_builder", value="Janitorial Landscaping Staffing Logistics Construction Support IT Charter buses Lodging Security Education Training Disaster relief")
    diff = st.text_area("Differentiators", key="cap_diff_textarea_capability_builder", value="Fast mobilization • Quality controls • Transparent reporting • Nationwide partner network")
    past_perf = st.text_area("Representative experience", key="cap_past_textarea_capability_builder", value="Project A: Custodial support, 100k sq ft. Project B: Grounds keeping, 200 acres.")
    contact = st.text_area("Contact info", key="cap_contact_textarea_capability_builder", value="ELA Management LLC • info@elamanagement.com • 555 555 5555 • UEI XXXXXXX • CAGE XXXXX")

    c1, c2, c3 = st.columns([1,1,2])

    with c1:
        if st.button("Generate one page", key="btn_cap_generate_capability_builder"):
            system = "Format a one page federal capability statement in markdown. Use clean headings and short bullets."
            prompt = f"""Company {company}
Tagline {tagline}
Core {core}
Diff {diff}
Past performance {past_perf}
Contact {contact}
NAICS {", ".join(sorted(set(NAICS_SEEDS)))}
Certifications Small Business"""
            cap_md = llm(system, prompt, max_tokens=900)
            st.session_state["capability_md"] = cap_md

    with c2:
        if st.button("Clear draft", key="btn_cap_clear_capability_builder"):
            st.session_state.pop("capability_md", None)

    cap_md = st.session_state.get("capability_md", "")
    cap_md = _normalize_markdown_sections(cap_md)
    if cap_md:
        st.markdown("#### Preview")
        st.markdown(cap_md)
        issues, est_pages = _validate_text_for_guardrails(cap_md, page_limit=2, require_font="Times New Roman", require_size_pt=11, margins_in=1.0, line_spacing=1.0, filename_pattern="{company}_{section}_{date}")
        if issues:
            st.warning("Before export, fix these items: " + "; ".join(issues))
        logo_file = st.file_uploader("Optional logo for header", type=["png","jpg","jpeg"], key="cap_logo_upload")
        _logo = logo_file.read() if logo_file else None
        docx_bytes = md_to_docx_bytes_rich(cap_md, title=_docx_title_if_needed(cap_md, f"{company} Capability Statement"), base_font="Times New Roman", base_size_pt=11, margins_in=1.0, logo_bytes=_logo)
        st.download_button("Export Capability Statement (DOCX)", data=docx_bytes, file_name="Capability_Statement.docx", mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    else:
        st.info("Click Generate one page to draft, then export to DOCX.")


with legacy_tabs[7]:
    st.subheader("White paper builder")
    title = st.text_input("Title", key="wp_title_input_whitepaper_builder", value="Improving Facility Readiness with Outcome based Service Contracts")
    thesis = st.text_area("Thesis", key="wp_thesis_textarea_whitepaper_builder", value="Outcome based service contracts reduce total cost and improve satisfaction when paired with clear SLAs and transparent data.")
    audience = st.text_input("Audience", key="wp_audience_input_whitepaper_builder", value="Facility Managers • Contracting Officers • Program Managers")

    col_w1, col_w2, col_w3 = st.columns([1,1,2])
    with col_w1:
        if st.button("Draft white paper", key="btn_wp_draft_whitepaper_builder"):
            system = "Write a two page white paper with executive summary, problem, approach, case vignette, and implementation steps. Use clear headings and tight language."
            prompt = f"Title {title}\nThesis {thesis}\nAudience {audience}"
            wp_md = llm(system, prompt, max_tokens=1400)
            st.session_state["whitepaper_md"] = wp_md
    with col_w2:
        if st.button("Clear white paper draft", key="btn_wp_clear_whitepaper_builder"):
            st.session_state.pop("whitepaper_md", None)

    wp_md = st.session_state.get("whitepaper_md", "")
    wp_md = _normalize_markdown_sections(wp_md)
    if wp_md:
        st.markdown("#### Preview")
        st.markdown(wp_md)
        issues, est_pages = _validate_text_for_guardrails(wp_md, page_limit=4, require_font="Times New Roman", require_size_pt=11, margins_in=1.0, line_spacing=1.0, filename_pattern="{company}_{section}_{date}")
        if issues:
            st.warning("Before export, fix these items: " + "; ".join(issues))
        wp_logo_file = st.file_uploader("Optional logo for header", type=["png","jpg","jpeg"], key="wp_logo_upload")
        _wp_logo = wp_logo_file.read() if wp_logo_file else None
        wp_bytes = md_to_docx_bytes_rich(wp_md, title=_docx_title_if_needed(wp_md, title), base_font="Times New Roman", base_size_pt=11, margins_in=1.0, logo_bytes=_wp_logo)
        st.download_button("Export White Paper (DOCX)", data=wp_bytes, file_name="White_Paper.docx", mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    else:
        st.info("Click Draft white paper to create a draft, then export to DOCX.")

with legacy_tabs[8]:
    st.subheader("Export to Excel workbook")
    conn = get_db()
    v = pd.read_sql_query("select * from vendors", conn)
    o = pd.read_sql_query("select * from opportunities", conn)
    c = pd.read_sql_query("select * from contacts", conn)
    bytes_xlsx = to_xlsx_bytes({"Vendors": v, "Opportunities": o, "Contacts": c})
    st.download_button("Download Excel workbook", data=bytes_xlsx, file_name="govcon_hub.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

with legacy_tabs[9]:
    st.subheader("Auto extract key details")
    up = st.file_uploader("Upload solicitation or PWS", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key="auto_up")
    if up and st.button("Extract"):
        combined = "\n\n".join(read_doc(f) for f in up)
        chunks = chunk_text(combined)
        vec, X = embed_texts(chunks)
        snips = search_chunks(
            "scope technical specs performance metrics timeline deliverables submission instructions evaluation factors price schedule wage determination place of performance points of contact site visit clauses",
            vec, X, chunks, k=10
        )
        system = "You are a federal contracting assistant. Use headings and tight bullets."
        prompt = "Source slices\n" + "\n\n".join(snips) + "\n\nExtract fields now"
        st.markdown(llm(system, prompt, max_tokens=1200))

with legacy_tabs[10]:
    st.subheader("Ask questions over the uploaded docs")
    up2 = st.file_uploader("Upload PDFs or DOCX", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key="qna_up")
    q = st.text_input("Your question")
    if up2 and q and st.button("Answer"):
        combined = "\n\n".join(read_doc(f) for f in up2)
        chunks = chunk_text(combined); vec, X = embed_texts(chunks)
        snips = search_chunks(q, vec, X, chunks, k=6); support = "\n\n".join(snips)
        system = "Answer directly. Quote exact lines for dates or addresses."
        prompt = f"Context\n{support}\n\nQuestion\n{q}"
        st.markdown(llm(system, prompt, max_tokens=900))


with legacy_tabs[11]:
    st.subheader("Chat Assistant (remembers context; accepts file uploads)")
    conn = get_db()

    # Sessions
    sessions = pd.read_sql_query("select id, title, created_at from chat_sessions order by created_at desc", conn)
    session_titles = ["➤ New chat"] + [f"{r['id']}: {r['title'] or '(untitled)'}" for _, r in sessions.iterrows()]
    pick = st.selectbox("Session", options=session_titles, index=0)

    # Create new session
    if pick == "➤ New chat":
        default_title = f"Chat {datetime.now().strftime('%b %d %I:%M %p')}"
        new_title = st.text_input("New chat title", value=default_title)
        if st.button("Start chat"):
            conn.execute("insert into chat_sessions(title) values(?)", (new_title,))
            conn.commit()
            st.rerun()
        st.caption("Pick an existing chat from the dropdown above to continue.")
    else:
        # Parse session id
        session_id = parse_pick_id(pick)
        if session_id is None:
            st.info("Select a valid session to continue.")
        else:
            cur_title = sessions[sessions["id"] == session_id]["title"].iloc[0] if not sessions.empty else "(untitled)"
            st.caption(f"Session #{session_id} — {cur_title}")

            # File uploads for this chat session
            up_files = st.file_uploader("Attach files (PDF, DOCX, DOC, TXT)", type=["pdf","docx","doc","txt"],
                                        accept_multiple_files=True, key=f"chat_up_{session_id}")
            if up_files and st.button("Add files to this chat"):
                added = 0
                for up in up_files:
                    try:
                        text = read_doc(up)[:800_000]
                    except Exception:
                        text = ""
                    conn.execute(
                        "insert into chat_files(session_id, filename, mimetype, content_text) values(?,?,?,?)",
                        (session_id, up.name, getattr(up, "type", ""), text)
                    )
                    added += 1
                conn.commit()
                st.success(f"Added {added} file(s).")
                st.rerun()

            # Show existing attachments
            files_df = pd.read_sql_query(
                "select id, filename, length(content_text) as chars, uploaded_at from chat_files where session_id=? order by id desc",
                conn, params=(session_id,)
            )
            if not files_df.empty:
                st.caption("Attached files")
                st.dataframe(files_df.rename(columns={"chars":"chars_of_text"}), use_container_width=True)

            # Helper to pull doc snippets most relevant to the user's question
            def _chat_doc_snips(question_text: str) -> str:
                rows = pd.read_sql_query(
                    "select filename, content_text from chat_files where session_id=? and ifnull(content_text,'')<>''",
                    conn, params=(session_id,)
                )
                if rows.empty:
                    return ""
                chunks, labels = [], []
                for _, r in rows.iterrows():
                    cs = chunk_text(r["content_text"], max_chars=1200, overlap=200)
                    chunks.extend(cs)
                    labels.extend([r["filename"]] * len(cs))
                vec, X = embed_texts(chunks)
                top = search_chunks(question_text, vec, X, chunks, k=min(8, len(chunks)))
                parts, used = [], set()
                for sn in top:
                    try:
                        idx = chunks.index(sn)
                        fname = labels[idx]
                    except Exception:
                        fname = "attachment"
                    key = (fname, sn[:60])
                    if key in used:
                        continue
                    used.add(key)
                    parts.append(f"\n--- {fname} ---\n{sn.strip()}\n")
                return "Attached document snippets (most relevant first):\n" + "\n".join(parts[:16]) if parts else ""

            # Show chat history
            hist = pd.read_sql_query(
                "select role, content, created_at from chat_messages where session_id=? order by id asc",
                conn, params=(session_id,)
            )
            for _, row in hist.iterrows():
                if row["role"] == "user":
                    st.chat_message("user").markdown(row["content"])
                elif row["role"] == "assistant":
                    st.chat_message("assistant").markdown(row["content"])

            # Chat input lives inside the tab to avoid bleed-through
            user_msg = st.chat_input("Type your message")
            if user_msg:
                # Save user's message
                conn.execute("insert into chat_messages(session_id, role, content) values(?,?,?)",
                             (session_id, "user", user_msg))
                conn.commit()

                # Build system + context
                try:
                    context_snap = build_context(max_rows=6)
                except Exception:
                    context_snap = ""
                doc_snips = _chat_doc_snips(user_msg)

                system_text = "\n\n".join(filter(None, [
                    "You are a helpful federal contracting assistant. Keep answers concise and actionable.",
                    f"Context snapshot (keep answers consistent with this):\n{context_snap}" if context_snap else "",
                    doc_snips
                ]))

                # Construct rolling window of previous messages for context
                msgs_db = pd.read_sql_query(
                    "select role, content from chat_messages where session_id=? order by id asc",
                    conn, params=(session_id,)
                ).to_dict(orient="records")

                # Keep last ~12 user/assistant turns
                window = msgs_db[-24:] if len(msgs_db) > 24 else msgs_db
                messages = [{"role": "system", "content": system_text}] + window

                assistant_out = llm_messages(messages, temp=0.2, max_tokens=1200)
                conn.execute("insert into chat_messages(session_id, role, content) values(?,?,?)",
                             (session_id, "assistant", assistant_out))
                conn.commit()

                st.chat_message("user").markdown(user_msg)
                st.chat_message("assistant").markdown(assistant_out)



# ===== end app.py =====

# (moved) Proposal Builder call will be added after definition
# === New Feature Tabs Implementation ===

def _parse_date_any(s):
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None

def _lpta_note(total_price, budget_hint=None):
    if budget_hint is None:
        return "LPTA check requires competitor or IGCE context. Provide budget to evaluate."
    return "PASS" if total_price <= float(budget_hint) else "FAIL"

# Compute dynamic base index for new tabs
__tabs_base = 13  # 'Deadlines' tab index

with legacy_tabs[__tabs_base + 0]:
    st.subheader("Deadline tracker")
    conn = get_db()
    colA, colB = st.columns(2)
    with colA:
        st.caption("From opportunities table")
        o = pd.read_sql_query("select id, title, agency, response_due, status from opportunities order by response_due asc nulls last", conn)
        if not o.empty:
            o["due_dt"] = o["response_due"].apply(_parse_date_any)
            o["Due in days"] = o["due_dt"].apply(lambda d: (d - datetime.now()).days if d else None)
            st.dataframe(o[["id","title","agency","response_due","status","Due in days"]])
        else:
            st.info("No opportunities yet")
    with colB:
        st.caption("Manual deadlines")
        m = pd.read_sql_query("select * from deadlines order by due_date asc", conn)
        st.dataframe(m)
        with st.form("add_deadline"):
            title = st.text_input("Title")
            due = st.date_input("Due date", datetime.now().date())
            source = st.text_input("Source or link", "")
            notes = st.text_area("Notes", "")
            if st.form_submit_button("Add"):
                conn.execute("insert into deadlines(opp_id,title,due_date,source,notes) values(?,?,?,?,?)",
                             (None, title.strip(), due.strftime("%Y-%m-%d"), source.strip(), notes.strip()))
                conn.commit()
                st.success("Added")

    st.markdown("### Due today")
    due_today = pd.read_sql_query("select * from deadlines where date(due_date)=date('now') and status='Open'", conn)
    if not due_today.empty:
        st.dataframe(due_today[["title","due_date","source","notes"]])
        # Email reminders via Microsoft Graph
        st.markdown("#### Send email reminders")
        to_addr = st.text_input("Send reminders to email", value="")
        if st.button("Email reminders for items due today"):
            if to_addr:
                body_lines = ["The following items are due today:"]
                for _, r in due_today.iterrows():
                    body_lines.append(f"- {r['title']} (source: {r.get('source','')})")
                status = send_via_graph(to_addr, "Reminders: items due today", "\n".join(body_lines))
                st.info(f"Email status: {status}")
            else:
                st.info("Enter an email address to send reminders.")
    else:
        st.write("No items due today.")


with legacy_tabs[__tabs_base + 1]:
    st.subheader("Section L and M checklist")
    conn = get_db()
    opp_pick_df = pd.read_sql_query("select id, title from opportunities order by posted desc", conn)
    opp_opt = [""] + [f"{int(r.id)}: {r.title}" for _, r in opp_pick_df.iterrows()]
    opp_sel = st.selectbox("Link checklist to opportunity", options=opp_opt, index=0, key="lm_opp_sel")
    opp_id_val = int(opp_sel.split(":")[0]) if opp_sel else None

    up = st.file_uploader("Upload solicitation files", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key="lm_up")
    if up and st.button("Generate checklist"):
        items = []
        for f in up:
            name = f.name
            suffix = name.lower().split(".")[-1]
            # Extract with OCR fallback for snippets
            try:
                if suffix == "pdf":
                    data = f.read()
                    r = PdfReader(io.BytesIO(data))
                    txt = "\n".join((p.extract_text() or "") for p in r.pages)
                    if len((txt or "").strip()) < 500:
                        txt = _ocr_pdf_bytes(data) or txt
                else:
                    txt = read_doc(f)
            finally:
                try: f.seek(0)
                except Exception: pass

            def _snip(text, pat):
                try:
                    rx = re.compile(pat, re.I|re.S)
                    m = rx.search(text or "")
                    if not m: return ""
                    s0 = max(0, m.start()-120); e0 = min(len(text), m.end()+120)
                    return (text[s0:e0]).replace("\n", " ")[:240]
                except Exception:
                    return ""

            anchors = {
                "technical": r"(technical volume|technical proposal)",
                "price": r"(price volume|pricing|schedule of items)",
                "past performance": r"\bpast performance\b",
                "representations": r"(reps(?: and)? certs|52\.212-3)",
                "page limit": r"(page limit|not exceed \d+\s*pages|\d+\s*page\s*limit)",
                "font": r"(font\s*(size)?\s*\d+|times new roman|arial)",
                "delivery": r"(delivery|period of performance|pop:)",
                "submission": r"(submit .*? to|email .*? to|via sam\.gov)",
                "due date": r"(offers due|responses due|closing date)",
            }
            for label, pat in anchors.items():
                sn = _snip(txt or "", pat)
                notes = "Found" if sn else "Not detected"
                items.append({"item": label, "required": 1, "status": "Pending", "owner": "", "source_page": name, "notes": notes, "snippet": sn, "opp_id": opp_id_val})

            # Clause risk flags
            for hit in _find_clause_risks(txt or ""):
                items.append({"item": f"Risk: {hit['clause']}", "required": 0, "status": "Pending", "owner": "", "source_page": name, "notes": hit["hint"], "snippet": "", "opp_id": opp_id_val})

        df = pd.DataFrame(items)
        st.dataframe(df, use_container_width=True)
        for r in items:
            conn.execute("insert into compliance_items(opp_id,item,required,status,owner,source_page,notes,snippet) values(?,?,?,?,?,?,?,?)",
                         (r["opp_id"], r["item"], 1 if r["required"] else 0, r["status"], r["owner"], r["source_page"], r["notes"], r.get("snippet","")))
        conn.commit()
        st.success("Checklist saved with page anchors, owners and snippets")

    st.markdown("#### Existing items")
    items = pd.read_sql_query("select * from compliance_items order by created_at desc limit 200", conn)
    st.dataframe(items, use_container_width=True)

with legacy_tabs[__tabs_base + 2]:
    pass
    st.subheader("RFQ generator to subcontractors")
    conn = get_db()
    vendors = pd.read_sql_query("select id, company, email, phone, trades from vendors order by company", conn)
    st.caption("Compose RFQ")
    with st.form("rfq_form"):
        sel = st.multiselect("Recipients", vendors["company"].tolist())
        scope = st.text_area("Scope", st.session_state.get("default_scope", "Provide labor materials equipment and supervision per attached specifications"), height=120)
        qty = st.text_input("Quantities or CLIN list", "")
        due = st.date_input("Quote due by", datetime.now().date() + timedelta(days=3))
        files = st.text_input("File names to reference", "")
        subject = st.text_input("Email subject", "Quote request for upcoming federal project")
        body = st.text_area("Email body preview", height=240,
            value=(f"Hello, \n\nELA Management LLC requests a quote.\n\nScope\n{scope}\n\nQuantities\n{qty}\n\nDue by {due.strftime('%Y-%m-%d')}\n\nFiles\n{files}\n\nPlease reply with price lead time and any exclusions.\n\nThank you.")
        )
        submit = st.form_submit_button("Generate drafts")
    if submit:
        recs = vendors[vendors["company"].isin(sel)]
        for _, r in recs.iterrows():
            conn.execute("""insert into rfq_outbox(vendor_id, company, to_email, subject, body, due_date, files_json, status)
                            values(?,?,?,?,?,?,?,?)""",
                         (int(r["id"]), r["company"], r.get("email",""), subject, body, due.strftime("%Y-%m-%d"),
                          json.dumps([f.strip() for f in files.split(",") if f.strip()]), "Draft"))
        conn.commit()
        st.success(f"Created {len(recs)} RFQ draft(s)")
    st.markdown("#### Drafts")
    drafts = pd.read_sql_query("select * from rfq_outbox order by created_at desc", conn)
    st.dataframe(drafts)

    # Export selected draft as DOCX
    pick = st.number_input("Draft ID to export as DOCX", min_value=0, step=1, value=0)
    if pick:
        cur = conn.cursor()
        cur.execute("select company, subject, body from rfq_outbox where id=?", (int(pick),))
        row = cur.fetchone()
        if row:
            from docx import Document
            doc = Document()
            doc.add_heading(row[1], level=1)
            doc.add_paragraph(f"To: {row[0]}")
            for para in row[2].split("\n\n"):
                doc.add_paragraph(_strip_markdown_to_plain(para))
            bio = io.BytesIO(); doc.save(bio); bio.seek(0)
            st.download_button("Download RFQ.docx", data=bio.getvalue(), file_name="RFQ.docx",
                               mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")

with legacy_tabs[__tabs_base + 3]:
    st.subheader("Pricing calculator")
    with st.form("price_calc"):
        default_base = float(st.session_state.get("pricing_base_cost", 0.0))
        base_cost = st.number_input("Base or subcontractor price", min_value=0.0, step=100.0, value=default_base)
        overhead = st.number_input("Overhead percent", min_value=0.0, max_value=100.0, step=0.5, value=10.0)
        gna = st.number_input("G and A percent", min_value=0.0, max_value=100.0, step=0.5, value=5.0)
        profit = st.number_input("Profit percent", min_value=0.0, max_value=100.0, step=0.5, value=8.0)
        igce = st.number_input("Budget or IGCE if known", min_value=0.0, step=100.0, value=0.0)
        terms_days = st.number_input("Payment terms (days)", min_value=0, step=1, value=30, help="Net terms for government payment")
        advance_pct = st.number_input("Factoring advance (%)", min_value=0.0, max_value=100.0, step=1.0, value=85.0)
        fac_rate = st.number_input("Factoring fee per 30 days (%)", min_value=0.0, max_value=10.0, step=0.1, value=2.0)
        run = st.form_submit_button("Calculate")
    if run:
        total = base_cost * (1 + overhead/100.0) * (1 + gna/100.0) * (1 + profit/100.0)
        note = _lpta_note(total, budget_hint=igce if igce > 0 else None)
        st.metric("Total price", f"${total:,.2f}")
        st.info(f"LPTA note: {note}")

        # Cash flow factoring model (simple)
        # Advance paid at day 0: advance_pct% of invoice; fee accrues until terms_days
        advance_amt = total * (advance_pct/100.0)
        period_factor = max(1, int(round(terms_days / 30.0)))
        fee = (fac_rate/100.0) * period_factor * total
        remainder = total - advance_amt - fee
        st.write({"Advance": round(advance_amt,2), "Estimated fee": round(fee,2), "Remainder on payment": round(remainder,2)})

        conn = get_db()
        try:
            cur = conn.cursor()
            # Ensure columns exist
            try: cur.execute("alter table pricing_scenarios add column terms_days integer")
            except Exception: pass
            try: cur.execute("alter table pricing_scenarios add column factoring_rate real")
            except Exception: pass
            try: cur.execute("alter table pricing_scenarios add column advance_pct real")
            except Exception: pass

            conn.execute("""insert into pricing_scenarios(opp_id, base_cost, overhead_pct, gna_pct, profit_pct, total_price, lpta_note, terms_days, factoring_rate, advance_pct)
                            values(?,?,?,?,?,?,?,?,?,?)""",
                        (None, float(base_cost), float(overhead), float(gna), float(profit), float(total), note, int(terms_days), float(fac_rate), float(advance_pct)))
            conn.commit()
        except Exception as _e_pc:
            st.caption(f"[Pricing save note: {_e_pc}]")

    st.markdown("### Scenario comparison")
    conn = get_db()
    try:
        dfp = pd.read_sql_query("select id, created_at, base_cost, overhead_pct, gna_pct, profit_pct, total_price, lpta_note, terms_days, factoring_rate, advance_pct from pricing_scenarios order by id desc limit 20", conn)
        if not dfp.empty:
            dfp["effective_fee"] = (dfp["factoring_rate"].fillna(0.0)/100.0) * (dfp["terms_days"].fillna(30)/30.0) * dfp["total_price"]
            st.dataframe(dfp, use_container_width=True)
        else:
            st.caption("No scenarios yet.")
    except Exception as _e_cmp:
        st.caption(f"[Scenario table note: {_e_cmp}]")




    with st.expander("Market data assist", expanded=True):
        colm1, colm2 = st.columns(2)
        with colm1:
            naics_q = st.text_input("NAICS for history lookup", value="", key="md_naics")
            psc_q = st.text_input("PSC for history lookup", value="", key="md_psc")
            kw_q = st.text_input("Optional keyword", value="", key="md_kw")
        with colm2:
            lookback_months = st.number_input("Look back months", min_value=1, step=1, value=24, key="md_months")
            limit_rows = st.number_input("Max awards to pull", min_value=10, step=10, value=200, key="md_limit")
            want_calc = st.checkbox("Also pull GSA CALC labor rates", value=False, key="md_calc")

        if st.button("Fetch market data", key="md_fetch"):
            from datetime import datetime as _dt, timedelta as _td
            date_to = _dt.utcnow().date().strftime("%Y-%m-%d")
            date_from = (_dt.utcnow().date() - _td(days=int(lookback_months)*30)).strftime("%Y-%m-%d")
            debug_box = st.container()
            df_awards, diag = usaspending_search_awards(
                naics=naics_q.strip(),
                psc=psc_q.strip(),
                date_from=date_from,
                date_to=date_to,
                keyword=kw_q.strip(),
                limit=int(limit_rows),
                st_debug=debug_box
            )
            if not df_awards.empty and "error" not in df_awards.columns:
                st.caption(f"USAspending awards from {date_from} to {date_to}")
                st.dataframe(df_awards.head(50), use_container_width=True)

                # Diagnostic breakdown
                import pandas as _pd
                from datetime import datetime as _dt

                def _months_between(s, e):
                    try:
                        sd = _dt.fromisoformat(str(s)[:10])
                        ed = _dt.fromisoformat(str(e)[:10])
                        days = max((ed - sd).days, 1)
                        return max(round(days / 30.44, 2), 0.01)
                    except Exception:
                        return None

                if "start" in df_awards.columns and "end" in df_awards.columns and "amount" in df_awards.columns:
                    _df = df_awards.copy()
                    _df["term_months"] = _df.apply(lambda r: _months_between(r["start"], r["end"]), axis=1)
                    _df["monthly_spend"] = _df.apply(lambda r: (float(r["amount"]) / r["term_months"]) if r["term_months"] and r["term_months"] > 0 else None, axis=1)

                    st.markdown("#### Diagnostics: term and monthly spend")
                    # Save selected awards as benchmarks with your annotations
                    st.markdown("#### Save selected awards to your benchmark library")
                    try:
                        _choices = _df["award_id"].dropna().astype(str).unique().tolist()
                    except Exception:
                        _choices = []
                    _sel_awards = st.multiselect("Pick award IDs to tag", _choices, key="md_pick_awards")
                    with st.form("md_bench_form"):
                        _sqft = st.number_input("Facility size sqft", min_value=0, step=1000, value=0, key="md_bench_sqft")
                        _freq = st.number_input("Visits per week", min_value=0, max_value=14, step=1, value=5, key="md_bench_freq")
                        _facility = st.text_input("Facility type", value="", key="md_bench_facility")
                        _scope = st.text_input("Scope tags comma separated", value="daily, restrooms, trash, floors", key="md_bench_scope")
                        _cpi = st.number_input("Inflation adjust percent per year", min_value=0.0, max_value=20.0, value=3.0, step=0.5, key="md_bench_cpi")
                        _note = st.text_area("Notes", value="", key="md_bench_notes")
                        _save = st.form_submit_button("Save to benchmarks")
                    if _save and _sel_awards:
                        import pandas as _pd, math as _math
                        from datetime import datetime as _dtd
                        _rows = _df[_df["award_id"].astype(str).isin(_sel_awards)].to_dict("records")
                        for r in _rows:
                            _tm = r.get("term_months") or 12.0
                            try:
                                # Simple CPI adjustment by term in years
                                _years = max((_tm / 12.0), 0.01)
                                _factor = (1.0 + float(_cpi)/100.0) ** _years
                            except Exception:
                                _factor = 1.0
                            _annual = float(r["amount"]) * (12.0 / _tm) if _tm and _tm > 0 else float(r["amount"])
                            _sqft_val = float(_sqft) if _sqft and _sqft > 0 else None
                            _dpsf = (_annual / _sqft_val) if _sqft_val else None
                            try:
                                conn.execute(
                                    "insert into pricing_benchmarks(award_id, agency, recipient, start, end, amount, term_months, monthly_spend, sqft, freq_per_week, facility_type, scope_tags, dollars_per_sqft_year, cpi_factor, amount_adj, notes) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (str(r.get("award_id")), str(r.get("agency")), str(r.get("recipient")), str(r.get("start")), str(r.get("end")), float(r.get("amount") or 0), float(_tm or 0), float(r.get("monthly_spend") or 0), _sqft_val, int(_freq or 0), _facility, _scope, float(_dpsf) if _dpsf is not None else None, float(_factor), float(r.get("amount") or 0) * float(_factor), _note)
                                )
                                conn.commit()
                            except Exception as _e:
                                st.warning(f"Save failed for {r.get('award_id')}: {_e}")
                        st.success(f"Saved {len(_sel_awards)} benchmark rows")

                    # View and use your benchmarks
                    with st.expander("Your benchmark library", expanded=False):
                        try:
                            _bench = _pd.read_sql_query("select * from pricing_benchmarks order by id desc limit 100", conn)
                        except Exception:
                            _bench = _pd.DataFrame()
                        if _bench is None or _bench.empty:
                            st.caption("No benchmarks yet. Save from the table above.")
                        else:
                            st.dataframe(_bench, use_container_width=True)
                            # Compute medians for $ per sqft and monthly spend
                            try:
                                _med_sqft = _pd.to_numeric(_bench["dollars_per_sqft_year"], errors="coerce").dropna().median()
                            except Exception:
                                _med_sqft = None
                            try:
                                _med_month = _pd.to_numeric(_bench["monthly_spend"], errors="coerce").dropna().median()
                            except Exception:
                                _med_month = None
                            if _med_sqft:
                                st.markdown(f"**Median dollars per sqft per year across benchmarks: ${_med_sqft:,.2f}**")
                            if _med_month:
                                st.markdown(f"**Median monthly spend across benchmarks: ${_med_month:,.0f}**")
                            _apply_sqft = st.number_input("Use sqft to apply median $ per sqft", min_value=0, step=1000, value=0, key="md_apply_sqft")
                            if _apply_sqft and _apply_sqft > 0 and _med_sqft:
                                _hint = float(_apply_sqft) * float(_med_sqft)
                                if st.button("Set base cost from benchmark median", key="md_bench_setbase"):
                                    st.session_state["pricing_base_cost"] = float(_hint)
                                    st.success(f"Base cost set to ${_hint:,.2f} from benchmark median. Recalculate above.")

                    st.dataframe(_df[["award_id","recipient","agency","start","end","amount","term_months","monthly_spend"]].head(50), use_container_width=True)

                    with st.expander("Implied $/sqft/year calculator", expanded=False):
                        sqft = st.number_input("Approx facility size (sqft)", min_value=0, step=1000, value=0, key="md_sqft")
                        per_week = st.number_input("Service frequency (visits per week)", min_value=0, max_value=14, step=1, value=5, key="md_freq")
                        if sqft and sqft > 0:
                            _df2 = _df.copy()
                            _df2["annualized_amount"] = _df2.apply(
                                lambda r: (float(r["amount"]) * (12.0 / r["term_months"])) if r["term_months"] and r["term_months"] > 0 else float(r["amount"]),
                                axis=1
                            )
                            _df2["dollars_per_sqft_year"] = _df2["annualized_amount"] / float(sqft)
                            st.caption("Based on your sqft input, here are implied $/sqft/year figures:")
                            st.dataframe(_df2[["award_id","agency","annualized_amount","dollars_per_sqft_year"]].head(50), use_container_width=True)

                            _vals = _pd.to_numeric(_df2["dollars_per_sqft_year"], errors="coerce").dropna()
                            if not _vals.empty:
                                _med = float(_vals.median())
                                st.markdown(f"**Median implied $/sqft/year across results: ${_med:,.2f}**")
                                if st.button("Set pricing hint from $/sqft median", key="md_set_sqft"):
                                    st.session_state["pricing_base_cost"] = _med * float(sqft)
                                    st.success(f"Base cost set to ${st.session_state['pricing_base_cost']:,.2f} from implied $/sqft median. Recalculate above.")

                if want_calc:
                    df_rates = gsa_calc_rates(kw_q or naics_q or psc_q or "janitorial")
                    if df_rates is not None and not df_rates.empty:
                        st.caption("GSA CALC sample labor rates")
                        st.dataframe(df_rates.head(50), use_container_width=True)
                        import pandas as _pd, numpy as _np
                        rate_series = _pd.to_numeric(df_rates["hourly_ceiling"], errors="coerce").dropna()
                        if not rate_series.empty:
                            with st.expander("Crew cost estimate (GSA CALC)", expanded=False):
                                crew_size = st.number_input("Crew size (people)", min_value=1, max_value=50, value=3, step=1, key="md_crew")
                                hrs_per_week = st.number_input("Hours per week (crew)", min_value=1, max_value=168, value=40, step=1, key="md_hours")
                                rate_med = float(_np.median(rate_series))
                                est_monthly = rate_med * float(crew_size) * float(hrs_per_week) * 4.33
                                st.markdown(f"Estimated crew cost at CALC median rate: **${est_monthly:,.0f} / month**")
                    else:
                        st.info("No CALC rates returned. Try a simpler keyword.")
            else:
                st.info("No award results returned. Try broadening filters or increasing look back.")
                st.caption(diag)

# ---------- Dates (US format for SAM) ----------
def _us_date(d: datetime.date) -> str:
    return d.strftime("%m/%d/%Y")

def _parse_sam_date(s: str):
    if not s: return None
    s = s.replace("Z","").strip()
    for fmt in ("%Y-%m-%d","%Y-%m-%dT%H:%M:%S","%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None

# ---------- Context for Chat ----------
def build_context(max_rows=6):
    conn = get_db()
    g = pd.read_sql_query("select * from goals limit 1", conn)
    goals_line = ""
    if not g.empty:
        rr = g.iloc[0]
        goals_line = (f"Bids target {int(rr['bids_target'])}, submitted {int(rr['bids_submitted'])}; "
                      f"Revenue target ${float(rr['revenue_target']):,.0f}, won ${float(rr['revenue_won']):,.0f}.")
    codes = pd.read_sql_query("select code from naics_watch order by code", conn)["code"].tolist()
    naics_line = ", ".join(codes[:20]) + (" …" if len(codes) > 20 else "") if codes else "none"
    opp = pd.read_sql_query(
        "select title, agency, naics, response_due from opportunities order by posted desc limit ?",
        conn, params=(max_rows,)
    )
    opp_lines = ["- " + " | ".join(filter(None, [
        str(r["title"])[:80], str(r["agency"])[:40],
        f"due {str(r['response_due'])[:16]}", f"NAICS {str(r['naics'])[:18]}",
    ])) for _, r in opp.iterrows()]
    vend = pd.read_sql_query(
        """select trim(substr(naics,1,6)) as code, count(*) as cnt
           from vendors where ifnull(naics,'')<>''
           group by trim(substr(naics,1,6)) order by cnt desc limit ?""",
        conn, params=(max_rows,)
    )
    vend_lines = [f"- {r['code']}: {int(r['cnt'])} vendors" for _, r in vend.iterrows()]
    return "\n".join([
        f"Company: {get_setting('company_name','ELA Management LLC')}",
        f"Home location: {get_setting('home_loc','Houston, TX')}",
        f"Goals: {goals_line or 'not set'}",
        f"NAICS watch: {naics_line}",
        "Recent opportunities:" if not opp.empty else "Recent opportunities: (none)",
        *opp_lines,
        "Vendor coverage (top NAICS):" if not vend.empty else "Vendor coverage: (none)",
        *vend_lines,
    ])

# ---------- External integrations ----------
def linkedin_company_search(keyword: str) -> str:
    return f"https://www.linkedin.com/search/results/companies/?keywords={quote_plus(keyword)}"

def google_places_search(query, location="Houston, TX", radius_m=80000, strict=True):
    """
    Google Places Text Search + Details (phone + website).
    Returns (list_of_vendors, info). Emails are NOT provided by Places.
    """
    if not GOOGLE_PLACES_KEY:
        return [], {"ok": False, "reason": "missing_key", "detail": "GOOGLE_PLACES_API_KEY is empty."}
    try:
        # 1) Text Search
        search_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        search_params = {"query": f"{query} {location}", "radius": radius_m, "key": GOOGLE_PLACES_KEY}
        rs = requests.get(search_url, params=search_params, timeout=25)
        status_code = rs.status_code
        data = rs.json() if rs.headers.get("Content-Type","").startswith("application/json") else {}
        api_status = data.get("status","")
        results = data.get("results", []) or []

        if status_code != 200 or api_status not in ("OK","ZERO_RESULTS"):
            return ([] if strict else results), {
                "ok": False, "reason": api_status or "http_error", "http": status_code,
                "api_status": api_status, "count": len(results),
                "raw_preview": (rs.text or "")[:800],
                "note": "Enable billing + 'Places API' in Google Cloud."
            }

        # 2) Details per result
        out = []
        for item in results:
            place_id = item.get("place_id")
            phone, website = "", ""
            if place_id:
                det_url = "https://maps.googleapis.com/maps/api/place/details/json"
                det_params = {"place_id": place_id, "fields": "formatted_phone_number,website", "key": GOOGLE_PLACES_KEY}
                rd = requests.get(det_url, params=det_params, timeout=20)
                det_json = rd.json() if rd.headers.get("Content-Type","").startswith("application/json") else {}
                det = det_json.get("result", {})
                phone = det.get("formatted_phone_number", "") or ""
                website = det.get("website", "") or ""

            out.append({
                "company": item.get("name"),
                "naics": "",
                "trades": "",
                "phone": phone,
                "email": "",  # Emails not provided by Google Places
                "website": website,
                "city": location.split(",")[0].strip() if "," in location else location,
                "state": location.split(",")[-1].strip() if "," in location else "",
                "certifications": "",
                "set_asides": "",
                "notes": item.get("formatted_address",""),
                "source": "GooglePlaces",
            })
        info = {"ok": True, "count": len(out), "http": status_code, "api_status": api_status,
                "raw_preview": (rs.text or "")[:800]}
        return out, info
    except Exception as e:
        return [], {"ok": False, "reason": "exception", "detail": str(e)[:500]}

def _clean_url(url: str) -> str:
    if not url: return ""
    if not url.startswith(("http://","https://")): return "http://" + url
    return url

def _same_domain(u1: str, u2: str) -> bool:
    try:
        d1 = urlparse(u1).netloc.split(":")[0].lower()
        d2 = urlparse(u2).netloc.split(":")[0].lower()
        return d1.endswith(d2) or d2.endswith(d1)
    except Exception:
        return True

def _allowed_by_robots(base_url: str, path: str) -> bool:
    try:
        parsed = urlparse(base_url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        r = requests.get(robots_url, timeout=8)
        if r.status_code != 200 or "Disallow" not in r.text: return True
        disallows = []
        for line in r.text.splitlines():
            line = line.strip()
            if not line or not line.lower().startswith("disallow:"): continue
            rule = line.split(":",1)[1].strip()
            if rule: disallows.append(rule)
        for rule in disallows:
            if path.startswith(rule): return False
        return True
    except Exception:
        return True

def _fetch(url: str, timeout=12) -> str:
    try:
        headers = {"User-Agent": "ELA-GovCon-Scraper/1.0 (+contact via site form)"}
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200 or not r.headers.get("Content-Type","").lower().startswith("text"):
            return ""
        return r.text[:1_000_000]
    except Exception:
        return ""

def _extract_emails(text: str) -> set:
    emails = set()
    for m in EMAIL_REGEX.finditer(text or ""):
        e = m.group(0).strip().strip(".,;:)")
        if not e.lower().endswith((".png",".jpg",".gif",".svg",".jpeg")):
            emails.add(e)
    return emails

def crawl_site_for_emails(seed_url: str, max_pages=5, delay_s=0.7, same_domain_only=True) -> dict:
    if BeautifulSoup is None:
        return {"emails": set(), "visited": 0, "errors": ["beautifulsoup4 not installed"]}
    seed_url = _clean_url(seed_url)
    try:
        parsed = urlparse(seed_url); base = f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return {"emails": set(), "visited": 0, "errors": ["bad seed url"]}
    queue = [seed_url, urljoin(base,"/contact"), urljoin(base,"/contact-us"),
             urljoin(base,"/contacts"), urljoin(base,"/about"), urljoin(base,"/support")]
    seen, emails, visited, errors = set(), set(), 0, []
    while queue and visited < max_pages:
        url = queue.pop(0)
        if url in seen: continue
        seen.add(url)
        if not _allowed_by_robots(seed_url, urlparse(url).path): continue
        html = _fetch(url)
        if not html: continue
        visited += 1
        try:
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("mailto:"):
                    emails.add(href.replace("mailto:","").split("?")[0])
            emails |= _extract_emails(soup.get_text(separator=" ", strip=True))
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith(("#","mailto:","javascript:")): continue
                nxt = urljoin(url, href)
                if same_domain_only and not _same_domain(seed_url, nxt): continue
                if any(nxt.lower().endswith(suf) for suf in [".pdf",".doc",".docx",".xlsx",".ppt",".zip",".jpg",".png",".gif",".svg"]):
                    continue
                if nxt not in seen and len(queue) < (max_pages*3):
                    queue.append(nxt)
        except Exception as e:
            errors.append(str(e))
        time.sleep(delay_s)
    return {"emails": emails, "visited": visited, "errors": errors}

# ---------- SAM search ----------
def sam_search(
    naics_list, min_days=3, limit=100, keyword=None, posted_from_days=30,
    notice_types="Combined Synopsis/Solicitation,Solicitation,Presolicitation,SRCSGT", active="true"
):
    """Legacy stub. Use fetch_notices instead."""
    return {}


# ===== Phase 0 Bootstrap =====

# ===== SAM Ingest Phase 1 =====
import math

def ensure_sam_ingest_tables():
    conn = get_db()
    # notices table: extend existing if present, else create
    conn.execute("""CREATE TABLE IF NOT EXISTS notices(
        id INTEGER PRIMARY KEY,
        sam_notice_id TEXT NOT NULL,
        notice_type TEXT NOT NULL,
        title TEXT NOT NULL,
        agency TEXT,
        naics TEXT,
        psc TEXT,
        set_aside TEXT,
        place_city TEXT,
        place_state TEXT,
        posted_at TEXT,
        due_at TEXT,
        status TEXT,
        url TEXT,
        last_fetched_at TEXT
    )""")
    # Add columns if existing table lacks them
    cols = {r[1] for r in conn.execute("PRAGMA table_info(notices)")}
    add_cols = []
    for cdef in [
        ("sam_notice_id","TEXT NOT NULL"),
        ("notice_type","TEXT NOT NULL"),
        ("title","TEXT NOT NULL"),
        ("agency","TEXT"),
        ("naics","TEXT"),
        ("psc","TEXT"),
        ("set_aside","TEXT"),
        ("place_city","TEXT"),
        ("place_state","TEXT"),
        ("posted_at","TEXT"),
        ("due_at","TEXT"),
        ("status","TEXT"),
        ("url","TEXT"),
        ("last_fetched_at","TEXT"),
    ]:
        if cdef[0] not in cols:
            add_cols.append(f"ALTER TABLE notices ADD COLUMN {cdef[0]} {cdef[1]}")
    for sql in add_cols:
        try:
            conn.execute(sql)
        except Exception:
            pass
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_notices_notice_id ON notices(sam_notice_id)")

    # notice_files
    conn.execute("""CREATE TABLE IF NOT EXISTS notice_files(
        id INTEGER PRIMARY KEY,
        notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
        file_name TEXT,
        file_url TEXT,
        checksum TEXT,
        bytes INTEGER,
        created_at TEXT
    )""")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_notice_files ON notice_files(notice_id, file_url)")

    # notice_status per-user
    conn.execute("""CREATE TABLE IF NOT EXISTS notice_status(
        user_id TEXT NOT NULL,
        notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
        state TEXT NOT NULL CHECK(state IN ('saved','dismissed')),
        ts TEXT NOT NULL,
        UNIQUE(user_id, notice_id)
    )""")

    # user_prefs
    conn.execute("""CREATE TABLE IF NOT EXISTS user_prefs(
        user_id TEXT PRIMARY KEY,
        sam_page_size INTEGER DEFAULT 50,
        email_default_recipients TEXT
    )""")

    # pipeline
    conn.execute("""CREATE TABLE IF NOT EXISTS pipeline_deals(
        id INTEGER PRIMARY KEY,
        user_id TEXT NOT NULL,
        notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
        stage TEXT DEFAULT 'Lead',
        created_at TEXT NOT NULL,
        UNIQUE(user_id, notice_id)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_user ON pipeline_deals(user_id)")

    # helpful indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_notices_due_at ON notices(due_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_notices_naics ON notices(naics)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_notices_psc ON notices(psc)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_notices_agency ON notices(agency)")

ensure_sam_ingest_tables()

def _sam_client():
    # api.data.gov wrapper for SAM
    key = get_secret("sam","key") or get_secret("sam","api_key") or get_secret("sam","SAM_API_KEY")
    base = "https://api.sam.gov/prod/opportunities/v2/search"
    return create_api_client(base, api_key=None, timeout=10, retries=2, ttl=900), key

@st.cache_data(ttl=900, show_spinner=False)
def fetch_notices(filters: dict, page: int, page_size: int, org_id=None, user_id=None):
    org_id = org_id or current_org_id()
    user_id = user_id or current_user_id()
    """
    Call SAM search API. Returns tuple (results, total_estimate).
    Filters: keywords, types, naics(list), psc(list), agency, place_city, place_state
    Page is 1-based. Aggregate API pages to reach page_size.
    """
    api, key = _sam_client()
    if not key:
        return {"error":"missing_api_key"}, 0
    # Map filters to params. SAM API accepts multiple notice types and codes.
    params = {
        "api_key": key,
        "page": max(0, int(page)-1),
        "limit": min(100, max(1, int(page_size))),
    }
    kw = (filters or {}).get("keywords") or ""
    if kw:
        # SAM uses "q" for keyword search
        params["q"] = kw
    types = (filters or {}).get("types") or []
    if isinstance(types, str):
        types = [t.strip() for t in types.split(",") if t.strip()]
    if types:
        # Common SAM field is "notice_type"
        params["notice_type"] = ",".join(types)
    naics = (filters or {}).get("naics") or []
    if isinstance(naics, str):
        naics = [n.strip() for n in naics.split(",") if n.strip()]
    if naics:
        params["naics"] = ",".join(naics)
    psc = (filters or {}).get("psc") or []
    if isinstance(psc, str):
        psc = [p.strip() for p in psc.split(",") if p.strip()]
    if psc:
        params["psc"] = ",".join(psc)
    agency = (filters or {}).get("agency") or ""
    if agency:
        params["agency"] = agency
    if (filters or {}).get("place_city"):
        params["city"] = filters["place_city"]
    if (filters or {}).get("place_state"):
        params["state"] = filters["place_state"]

    # Pull once. If API pages differently, this still returns up to page_size.
    res = api["get"]("", params)
    if "error" in res:
        return res, 0
    data = res.get("json") or {}
    # SAM typically returns "opportunitiesData" and "totalRecords"
    items = data.get("opportunitiesData") or data.get("data") or data.get("results") or []
    total = data.get("totalRecords") or data.get("numFound") or len(items)
    # Normalize minimal fields
    norm = []
    for it in items:
        # Handle different shapes defensively
        sid = str(it.get("noticeId") or it.get("id") or it.get("notice_id") or it.get("solicitationNumber") or "")
        ntype = it.get("type") or it.get("noticeType") or it.get("notice_type") or ""
        title = it.get("title") or it.get("subject") or it.get("noticeTitle") or ""
        ag = it.get("agency") or it.get("department") or it.get("orgName") or ""
        na = it.get("naics") or it.get("naicsCode") or ""
        ps = it.get("psc") or it.get("pscCode") or ""
        sa = it.get("setAside") or it.get("typeOfSetAside") or ""
        posted = it.get("postedDate") or it.get("publishDate") or it.get("date") or ""
        due = it.get("responseDate") or it.get("closeDate") or it.get("dueDate") or ""
        status = it.get("status") or it.get("active") or ""
        url = it.get("uiLink") or it.get("url") or ""
        place = it.get("placeOfPerformance") or {}
        city = place.get("city") if isinstance(place, dict) else ""
        state = place.get("state") if isinstance(place, dict) else ""
        atts = it.get("attachments") or it.get("files") or []
        norm.append({
            "sam_notice_id": sid,
            "notice_type": ntype,
            "title": title,
            "agency": ag,
            "naics": na,
            "psc": ps,
            "set_aside": sa,
            "place_city": city,
            "place_state": state,
            "posted_at": posted,
            "due_at": due,
            "status": str(status),
            "url": url,
            "attachments": atts,
        })
    return {"items": norm}, int(total or 0)

def upsert_notice(n: dict):
    conn = get_db()
    org_id = current_org_id()
    owner_id = current_user_id()
    sid = n.get("sam_notice_id") or n.get("id") or n.get("notice_id")
    row = conn.execute("SELECT id, version FROM notices WHERE org_id=? AND sam_notice_id=?", (org_id, str(sid))).fetchone()
    if row:
        nid, ver = int(row[0]), int(row[1] or 0)
        data = {
            "notice_type": n.get("notice_type") or n.get("type") or "",
            "title": n.get("title") or "",
            "agency": n.get("agency") or "",
            "naics": n.get("naics") or "",
            "psc": n.get("psc") or "",
            "set_aside": n.get("set_aside") or "",
            "place_city": (n.get("place") or {}).get("city") if isinstance(n.get("place"), dict) else n.get("place_city"),
            "place_state": (n.get("place") or {}).get("state") if isinstance(n.get("place"), dict) else n.get("place_state"),
            "posted_at": n.get("posted_at") or "",
            "due_at": n.get("due_at") or "",
            "status": n.get("status") or "",
            "url": n.get("url") or n.get("notice_url") or "",
            "last_fetched_at": utc_now_iso(),
            "owner_id": owner_id
        }
        q_update("notices", data, {"id": nid, "version": ver})
    else:
        nid = q_insert("notices", {
            "sam_notice_id": str(sid),
            "notice_type": n.get("notice_type") or n.get("type") or "",
            "title": n.get("title") or "",
            "agency": n.get("agency") or "",
            "naics": n.get("naics") or "",
            "psc": n.get("psc") or "",
            "set_aside": n.get("set_aside") or "",
            "place_city": (n.get("place") or {}).get("city") if isinstance(n.get("place"), dict) else n.get("place_city"),
            "place_state": (n.get("place") or {}).get("state") if isinstance(n.get("place"), dict) else n.get("place_state"),
            "posted_at": n.get("posted_at") or "",
            "due_at": n.get("due_at") or "",
            "status": n.get("status") or "",
            "url": n.get("url") or n.get("notice_url") or "",
            "last_fetched_at": utc_now_iso(),
            "visibility": "team"
        })
    atts = n.get("attachments") or n.get("files") or []
    for a in atts:
        furl = a.get("url") or a.get("href") or a.get("file_url")
        fname = a.get("name") or a.get("file_name") or (furl.split("/")[-1] if furl else None)
        if not furl or not fname:
            continue
        r = conn.execute("SELECT id FROM notice_files WHERE org_id=? AND notice_id=? AND file_url=?", (org_id, int(nid), furl)).fetchone()
        if not r:
            q_insert("notice_files", {"notice_id": int(nid), "file_name": fname, "file_url": furl, "created_at": utc_now_iso()})
    try:
        record_notice_version(nid, n)
    except Exception as _ex:
        log_event("warn","record_notice_version_failed", err=str(_ex))
    return nid

def list_notices(filters: dict, page: int, page_size: int, include_hidden: bool, user_id: str):
    """
    Read from DB with simple filters and user hidden state handling.
    Returns (rows, total_estimate)
    """
    ensure_sam_ingest_tables()
    conn = get_db()
    where = []
    vals = []
    if filters.get("keywords"):
        where.append("(title LIKE ? OR agency LIKE ?)")
        vals += [f"%{filters['keywords']}%", f"%{filters['keywords']}%"]
    if filters.get("types"):
        # types is list
        t = filters["types"]
        if isinstance(t, str):
            t = [x.strip() for x in t.split(",") if x.strip()]
        if t:
            where.append("(" + " OR ".join(["notice_type=?" for _ in t]) + ")")
            vals += t
    if filters.get("naics"):
        n = filters["naics"]
        if isinstance(n, str):
            n = [x.strip() for x in n.split(",") if x.strip()]
        for code in n:
            where.append("naics LIKE ?")
            vals.append(f"%{code}%")
    if filters.get("psc"):
        p = filters["psc"]
        if isinstance(p, str):
            p = [x.strip() for x in p.split(",") if x.strip()]
        for code in p:
            where.append("psc LIKE ?")
            vals.append(f"%{code}%")
    if filters.get("agency"):
        where.append("agency LIKE ?")
        vals.append(f"%{filters['agency']}%")
    if filters.get("place_state"):
        where.append("place_state LIKE ?")
        vals.append(f"%{filters['place_state']}%")
    if filters.get("place_city"):
        where.append("place_city LIKE ?")
        vals.append(f"%{filters['place_city']}%")

    wh = "WHERE " + " AND ".join(where) if where else ""
    # Hidden filter
    hidden_join = ""
    hidden_cond = ""
    if not include_hidden:
        hidden_join = "LEFT JOIN notice_status ns ON ns.notice_id = n.id AND ns.user_id=?"
        hidden_cond = "AND COALESCE(ns.state,'')=''"
        vals = [user_id] + vals

    # Count estimate
    total = conn.execute(f"SELECT COUNT(1) FROM notices n {hidden_join} {wh} {hidden_cond}", tuple(vals)).fetchone()[0]

    # Pagination
    page = max(1, int(page))
    page_size = max(1, int(page_size))
    offset = (page-1)*page_size

    org_id = current_org_id()
    rows = conn.execute(
        f"""SELECT n.id, n.sam_notice_id, n.notice_type, n.title, n.agency, n.naics, n.psc, n.set_aside,
                   n.place_city, n.place_state, n.posted_at, n.due_at, n.status, n.url,
                   EXISTS(SELECT 1 FROM pipeline_deals pd WHERE pd.user_id=? AND pd.notice_id=n.id AND pd.org_id=?) AS starred,
                   COALESCE((SELECT state FROM notice_status WHERE user_id=? AND notice_id=n.id AND org_id=?),'') AS my_state,
                   (SELECT COUNT(1) FROM amendments a WHERE a.notice_id=n.id) AS amendments_count,
                   COALESCE(n.compliance_state,'Unreviewed') AS compliance_state
            FROM notices n
            {hidden_join}
            WHERE n.org_id=? AND (n.visibility!='private' OR n.owner_id=?)
            {wh} {hidden_cond}
            ORDER BY date(n.posted_at) DESC, n.id DESC
            LIMIT ? OFFSET ?
        """,
        tuple([user_id, org_id, user_id, org_id, org_id, user_id] + vals + [page_size, offset])
    ).fetchall()


    # Shape rows
    shaped = []
    for r in rows:
        shaped.append({
            "id": r[0],
            "sam_notice_id": r[1],
            "type": r[2],
            "title": r[3],
            "agency": r[4],
            "naics": r[5],
            "psc": r[6],
            "set_aside": r[7],
            "place": ", ".join([x for x in [r[8], r[9]] if x]),
            "posted": r[10],
            "due": r[11],
            "status": r[12],
            "url": r[13],
            "starred": bool(r[14]),
            "state": r[15],
            "amendments_count": int(r[16]),
            "compliance_state": r[17],
        })
    return shaped, int(total)

def set_notice_state(user_id: str, notice_id: int, state: str):
    conn = get_db()
    org_id = current_org_id()
    conn.execute("""INSERT INTO notice_status(user_id, notice_id, state, ts, org_id, owner_id)
                    VALUES(?,?,?,?,?,?)
                    ON CONFLICT(user_id, notice_id) DO UPDATE SET state=excluded.state, ts=excluded.ts""",
                 (user_id, int(notice_id), state, utc_now_iso(), org_id, user_id))
    return False
    conn.execute("INSERT OR IGNORE INTO pipeline_deals(user_id, notice_id, created_at) VALUES(?,?,?)",
                 (user_id, int(notice_id), utc_now_iso()))
    return True

def get_user_page_size(user_id: str, default: int = 50) -> int:
    conn = get_db()
    r = conn.execute("SELECT sam_page_size FROM user_prefs WHERE user_id=?", (user_id,)).fetchone()
    if not r or not r[0]:
        return default
    return int(r[0])

def set_user_page_size(user_id: str, value: int):
    conn = get_db()
    val = int(value or 50)
    conn.execute("INSERT INTO user_prefs(user_id, sam_page_size) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET sam_page_size=excluded.sam_page_size",
                 (user_id, val))

def render_sam_watch_ingest():
    import streamlit as st
    import pandas as pd
    if not st.session_state.get("feature_flags", {}).get("sam_ingest_core"):
        return
    ensure_sam_ingest_tables()
    user_id = st.session_state.get("user_id") or st.session_state.get("active_user") or "anon"
    # Filters panel
    st.subheader("SAM Watch")
    with st.expander("Filters", expanded=True):
        c1, c2, c3 = st.columns([2,2,2])
        with c1:
            kw = st.text_input("Keywords", key="sam_kw", value=(st.session_state.get("sam_filters", {}) or {}).get("keywords",""))
            types = st.multiselect("Notice types", options=["Solicitation","Combined Synopsis or Solicitation","Presolicitation","Sources Sought"], key="sam_types",
                                   default=(st.session_state.get("sam_filters", {}) or {}).get("types", []))
        with c2:
            naics = st.text_input("NAICS (comma sep)", key="sam_naics", value="")
            psc = st.text_input("PSC (optional, comma sep)", key="sam_psc", value="")
        with c3:
            agency = st.text_input("Agency (optional)", key="sam_agency", value="")
            state = st.text_input("State (optional 2-letter)", key="sam_state", value="")
            city = st.text_input("City (optional)", key="sam_city", value="")
        c4, c5 = st.columns([3,1])
        with c4:
            st.caption("Posted window control present but off by default")
        with c5:
            show_hidden = st.toggle("Show hidden", value=False, key="sam_show_hidden")

        # Page size control
        page_size = 50
        if st.session_state.get("feature_flags", {}).get("sam_page_size"):
            saved_ps = get_user_page_size(user_id, 50)
            page_size = st.selectbox("Page size", options=[25,50,100], index=[25,50,100].index(saved_ps if saved_ps in [25,50,100] else 50))
            if page_size != saved_ps:
                set_user_page_size(user_id, page_size)
        else:
            st.caption("Page size: 50")

        # Actions
        a1, a2, a3 = st.columns([1,1,6])
        do_search = False
        with a1:
            if st.button("Search", type="primary"):
                do_search = True
        with a2:
            if st.button("Reset"):
                st.session_state["sam_filters"] = {}
                st.session_state["sam_page"] = 1
                _safe_rerun()

    # Maintain filters in session
    st.session_state["sam_filters"] = {
        "keywords": kw,
        "types": types,
        "naics": [x.strip() for x in naics.split(",") if x.strip()],
        "psc": [x.strip() for x in psc.split(",") if x.strip()],
        "agency": agency.strip(),
        "place_state": state.strip(),
        "place_city": city.strip(),
    }
    filters = st.session_state["sam_filters"]
    # Paging
    page = int(st.session_state.get("sam_page") or 1)
    # Trigger fetch
    if do_search:
        res, total = fetch_notices(filters, page=1, page_size=page_size, org_id=current_org_id(), user_id=user_id)
        if "error" in res:
            st.error(f"SAM API error: {res['error']} (id may be in logs)")
        else:
            # Upsert all
            cnt = 0
            for item in res.get("items", []):
                try:
                    upsert_notice(item); cnt += 1
                except Exception as ex:
                    log_event("error", "upsert_notice_failed", err=str(ex), sid=item.get("sam_notice_id"))
            st.success(f"Ingested {cnt} notices.")

    # List from DB respecting hidden state
    rows, total = list_notices(filters, page=page, page_size=page_size, include_hidden=show_hidden, user_id=user_id)

    # Results table
    st.caption(f"{total} total. Page {page}.")
    df = pd.DataFrame([{
        "Type": r["type"],
        "Title": r["title"],
        "Agency": r["agency"],
        "NAICS": r["naics"],
        "PSC": r["psc"],
        "Posted": r["posted"],
        "Due": r["due"],
        "Set-aside": r["set_aside"],
        "Place": r["place"],
        "Status": r["status"],
        "Star": "⭐" if r["starred"] else "",
        "State": r["state"],
        "URL": r["url"],
        "ID": r["id"],
    } for r in rows])

    # Extra columns if amendments tracking is on
    if st.session_state.get("feature_flags", {}).get("amend_tracking"):
        try:
            df["Amendments"] = [int(r.get("amendments_count",0)) for r in rows]
            df["Compliance"] = [r.get("compliance_state","") for r in rows]
        except Exception:
            pass

    # Actions per row via form with multiselect of ids
    with st.form("sam_actions"):
        st.dataframe(df.drop(columns=["ID"]), use_container_width=True, hide_index=True)
        c1, c2, c3, c4 = st.columns([1,1,1,6])
        sel_ids = st.multiselect("Select rows by Title to act on", options=[r["Title"] for r in df.to_dict("records")], key="sam_sel_titles")
        # Map selected titles to ids
        id_map = {r["Title"]: r["ID"] for r in df.to_dict("records")}
        selected_ids = [id_map[t] for t in sel_ids if t in id_map]
        did = None
        with c1:
            if st.form_submit_button("Save"):
                for nid in selected_ids:
                    set_notice_state(user_id, nid, "saved")
        with c2:
            if st.form_submit_button("Dismiss"):
                for nid in selected_ids:
                    set_notice_state(user_id, nid, "dismissed"); audit('dismiss', user_id, 'notice', str(nid))
        with c3:
            if st.session_state.get("feature_flags", {}).get("pipeline_star") and st.form_submit_button("Toggle Star"):
        with c4:
            if feature_flags().get("compliance_gate", False) and st.form_submit_button("Compliance"):
                if selected_ids:
                    st.session_state["selected_notice_id"] = int(selected_ids[0])
                    st.session_state["compliance_tab_open"] = True
    
                for nid in selected_ids:
                    toggle_pipeline_star(user_id, nid); audit('star_toggle', user_id, 'notice', str(nid))
        # Diff controls
        if st.session_state.get("feature_flags", {}).get("amend_tracking"):
            d1, d2 = st.columns([1,5])
            with d1:
                if st.form_submit_button("Open Diff"):
                    if selected_ids:
                        st.session_state["selected_notice_id"] = int(selected_ids[0])
                        st.session_state["diff_tab_open"] = True

    # Render diff panel below
    render_diff_panel()
    render_compliance_panel()

    # Footer paging
    p1, p2, p3 = st.columns([1,1,6])
    with p1:
        if st.button("Prev") and page > 1:
            st.session_state["sam_page"] = page - 1
            _safe_rerun()
    with p2:
        if st.button("Next") and (page * page_size) < total:
            st.session_state["sam_page"] = page + 1
            _safe_rerun()
    with p3:
        if st.button("Load more"):
            # Load next API page and ingest
            res, _ = fetch_notices(filters, page=page+1, page_size=page_size, org_id=current_org_id(), user_id=user_id)
            if "error" not in res:
                for item in res.get("items", []):
                    try:
                        upsert_notice(item)
                    except Exception as ex:
                        log_event("error","upsert_notice_failed", err=str(ex))
            st.session_state["sam_page"] = page + 1
            _safe_rerun()

# ===== end SAM Ingest Phase 1 =====

# ===== RFP Analyzer Phase 2 =====
import threading, queue, hashlib, requests

def ensure_rfp_tables():
    conn = get_db()
    conn.execute("""CREATE TABLE IF NOT EXISTS rfp_summaries(
        id INTEGER PRIMARY KEY,
        notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
        version_hash TEXT NOT NULL,
        summary_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(notice_id, version_hash)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS file_parses(
        id INTEGER PRIMARY KEY,
        notice_file_id INTEGER NOT NULL REFERENCES notice_files(id) ON DELETE CASCADE,
        checksum TEXT NOT NULL,
        parsed_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(notice_file_id, checksum)
    )""")
    # Try create FTS5, ignore if not supported
    try:
        conn.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS rfp_chunks USING fts5(
            notice_id UNINDEXED,
            file_name,
            page UNINDEXED,
            text
        )""")
    except Exception:
        pass
ensure_rfp_tables()

RFP_SUMMARY_SCHEMA = {
    "type": "object",
    "required": ["notice_id","version_hash","sections","files"],
    "properties": {
        "notice_id": {"type":"integer"},
        "version_hash": {"type":"string"},
        "sections": {
            "type":"object",
            "properties": {
                "Brief": {"type":"array"},
                "Factors": {"type":"array"},
                "Clauses": {"type":"array"},
                "Dates": {"type":"array"},
                "Forms": {"type":"array"},
                "Milestones": {"type":"array"}
            },
            "additionalProperties": True
        },
        "files": {"type":"array"}
    }
}

def _validate_summary_json(obj: dict) -> bool:
    # Minimal validator without external jsonschema dependency
    try:
        if not isinstance(obj, dict): return False
        for k in ["notice_id","version_hash","sections","files"]:
            if k not in obj: return False
        if not isinstance(obj["notice_id"], int): return False
        if not isinstance(obj["version_hash"], str): return False
        if not isinstance(obj["sections"], dict): return False
        if not isinstance(obj["files"], list): return False
        return True
    except Exception:
        return False

def _get_notice_meta(nid: int):
    conn = get_db()
    r = q_select("SELECT title, agency, due_at FROM notices WHERE id=?", (int(nid),), one=True)
    return {"title": r[0] if r else f"Notice {nid}", "agency": r[1] if r else "", "due": r[2] if r else ""}

def _notice_files(nid: int):
    return q_select("SELECT id, file_name, file_url, checksum, COALESCE(bytes,0) FROM notice_files WHERE notice_id=?", (int(nid),))

def _combined_checksum(nid: int) -> str:
    h = hashlib.sha256()
    for _, name, url, cks, _ in _notice_files(nid):
        h.update((cks or "").encode("utf-8"))
        h.update((url or "").encode("utf-8"))
        h.update((name or "").encode("utf-8"))
    return h.hexdigest()

def _download_file(url: str, timeout: int = 30):
    try:
        r = requests.get(url, timeout=timeout, stream=True)
        r.raise_for_status()
        b = r.content
        return b, None
    except Exception as ex:
        return None, str(ex)

def _parse_pdf_bytes(b: bytes) -> list:
    # Return list of dicts: {"page": i, "text": "..."}
    pages = []
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(__import__("io").BytesIO(b))
        for i, p in enumerate(reader.pages, start=1):
            try:
                txt = p.extract_text() or ""
            except Exception:
                txt = ""
            pages.append({"page": i, "text": txt})
        return pages
    except Exception:
        # Fallback single page blob
        pages.append({"page": 1, "text": ""})
        return pages

def _parse_docx_bytes(b: bytes) -> list:
    try:
        from docx import Document
        import io as _io
        doc = Document(_io.BytesIO(b))
        text = "\n".join([p.text for p in doc.paragraphs])
        return [{"page": 1, "text": text}]
    except Exception:
        return [{"page": 1, "text": ""}]

def _detect_type_by_name(name: str) -> str:
    n = (name or "").lower()
    if n.endswith(".pdf"): return "pdf"
    if n.endswith(".docx"): return "docx"
    return "bin"

def _index_chunks(nid: int, fname: str, pages: list):
    conn = get_db()
    try:
        for p in pages:
            try:
                conn.execute("INSERT INTO rfp_chunks(org_id, notice_id, file_name, page, text) VALUES(?,?,?,?,?)",
                             (current_org_id(), int(nid), fname, int(p.get("page") or 1), p.get("text") or ""))
            except Exception:
                conn.execute("INSERT INTO rfp_chunks(notice_id, file_name, page, text) VALUES(?,?,?,?)",
                             (int(nid), fname, int(p.get("page") or 1), p.get("text") or ""))
    except Exception:
        pass

def parse_rfp(notice_id: int) -> dict:
    """
    Download files, compute checksums, parse, index, and store summary JSON.
    Cached by notice_id + combined file checksum.
    """
    ensure_rfp_tables()
    conn = get_db()
    files = _notice_files(notice_id)
    if not files:
        return err_with_id("no_files_for_notice", notice_id=notice_id)
    vhash = _combined_checksum(notice_id)

    # Cached summary
    r = conn.execute("SELECT summary_json FROM rfp_summaries WHERE notice_id=? AND version_hash=?", (int(notice_id), vhash)).fetchone()
    if r:
        try:
            res = {"cached": True, "summary": json.loads(r[0])}
            try:
                _rfp_phase1_maybe_store(int(notice_id))
            except Exception as _ex:
                log_event("warn","rfp_phase1_store_failed", err=str(_ex))
            return res
        except Exception:
            pass

    # Fresh parse
    conn.execute("DELETE FROM rfp_chunks WHERE notice_id=?", (int(notice_id),))
    files_out = []
    sections = {"Brief": [], "Factors": [], "Clauses": [], "Dates": [], "Forms": [], "Milestones": []}

    for fid, name, url, cks, size in files:
        if not url:
            continue
        b, err = _download_file(url, timeout=30)
        if err:
            log_event("warn","file_download_failed", url=url, notice_id=notice_id)
            continue
        # Compute checksum if missing or mismatch
        sha = hashlib.sha256(b).hexdigest()
        if not cks or cks != sha:
            try:
                conn.execute("UPDATE notice_files SET checksum=?, bytes=? WHERE id=?", (sha, len(b), int(fid)))
            except Exception:
                pass
        # Parse by type
        ftype = _detect_type_by_name(name or url)
        if ftype == "pdf":
            pages = _parse_pdf_bytes(b)
        elif ftype == "docx":
            pages = _parse_docx_bytes(b)
        else:
            pages = [{"page": 1, "text": ""}]
        # Index chunks
        _index_chunks(notice_id, name or url.split("/")[-1], pages)
        files_out.append({"file_id": int(fid), "name": name or "", "pages": len(pages)})
        # Naive extraction for sections (placeholder keyword scans)
        for p in pages:
            t = (p.get("text") or "").strip()
            if not t:
                continue
            lt = t.lower()
            if "section l" in lt or "instructions to offerors" in lt:
                sections["Brief"].append({"hit": "Section L", "file": name, "page": p["page"]})
            if "section m" in lt or "evaluation factors" in lt:
                sections["Factors"].append({"hit": "Section M", "file": name, "page": p["page"]})
            if "far " in lt or "dfars " in lt or "clause" in lt:
                sections["Clauses"].append({"hit": "Clause ref", "file": name, "page": p["page"]})
            if "due date" in lt or "offers due" in lt or "closing date" in lt:
                sections["Dates"].append({"hit": "Due date mention", "file": name, "page": p["page"]})
            if "sf1449" in lt or "sf 1449" in lt or "form" in lt:
                sections["Forms"].append({"hit": "Form mention", "file": name, "page": p["page"]})
            if "milestone" in lt or "schedule" in lt:
                sections["Milestones"].append({"hit": "Milestone", "file": name, "page": p["page"]})

    summary = {"notice_id": int(notice_id), "version_hash": vhash, "sections": sections, "files": files_out}
    try:
        _rfp_phase1_maybe_store(int(notice_id))
    except Exception as _ex:
        log_event("warn","rfp_phase1_store_failed", err=str(_ex))

    if not _validate_summary_json(summary):
        return err_with_id("invalid_summary_json", notice_id=notice_id)

    # Store
    now = utc_now_iso()
    conn.execute("INSERT OR IGNORE INTO rfp_summaries(notice_id, version_hash, summary_json, created_at) VALUES(?,?,?,?)",
                 (int(notice_id), vhash, json.dumps(summary, ensure_ascii=False), now))
    return {"cached": False, "summary": summary}

# Worker management
_rfp_worker_lock = threading.Lock()
def start_rfp_worker(notice_id: int):
    import streamlit as st
    with _rfp_worker_lock:
        st.session_state["rfp_worker_status"] = {"state":"running","started_at":_now_iso(),"notice_id":int(notice_id)}
        def _run():
            try:
                res = parse_rfp(int(notice_id))
                st.session_state["rfp_worker_status"] = {"state":"done","result":res,"notice_id":int(notice_id),"finished_at":_now_iso()}
            except Exception as ex:
                st.session_state["rfp_worker_status"] = {"state":"error","error":str(ex),"notice_id":int(notice_id),"finished_at":_now_iso()}
        th = threading.Thread(target=_run, daemon=True)
        th.start()

def _qa_from_chunks(notice_id: int, q: str, limit: int = 5):
    conn = get_db()
    # Prefer FTS if available
    try:
        rows = conn.execute("SELECT file_name, page, snippet(rfp_chunks, 3, '[', ']', '…', 8) FROM rfp_chunks WHERE org_id=? AND notice_id=? AND rfp_chunks MATCH ? LIMIT ?",
                            (current_org_id(), int(notice_id), q, int(limit))).fetchall()
        if rows:
            return [{"file": r[0], "page": r[1], "snippet": r[2]} for r in rows]
    except Exception:
        pass
    # Fallback: search summary JSON
    r = conn.execute("SELECT summary_json FROM rfp_summaries WHERE notice_id=? ORDER BY id DESC LIMIT 1", (int(notice_id),)).fetchone()
    if not r:
        return []
    try:
        s = json.loads(r[0])
        blobs = json.dumps(s, ensure_ascii=False)
        # naive find locations
        out = []
        idx = blobs.lower().find(q.lower())
        if idx != -1:
            out.append({"file":"summary","page":0,"snippet":blobs[max(0,idx-60):idx+120]})
        return out
    except Exception:
        return []

def render_rfp_panel():
    import streamlit as st
    if not st.session_state.get("feature_flags", {}).get("rfp_analyzer_panel"):
        return
    if not st.session_state.get("rfp_panel_open") or not st.session_state.get("current_notice_id"):
        return
    nid = int(st.session_state["current_notice_id"])
    meta = _get_notice_meta(nid)
    st.markdown("---")
    st.subheader("RFP Analyzer")
    st.caption(f"{meta['title']}  •  {meta['agency']}  •  Due {meta['due'] or 'n/a'}")

    # Controls
    c1, c2 = st.columns([1,1])
    with c1:
        if st.button("Run Parse"): start_rfp_parser_worker(nid)
    with c2:
        if st.button("Close Panel"):
            st.session_state["rfp_panel_open"] = False
            return

    # Status
    st.write("Status:", st.session_state.get("rfp_worker_status", {}).get("state","idle"))
    if st.session_state.get("rfp_worker_status", {}).get("state") == "error":
        st.error(f"Parser error. Error id in logs.")
    # Show cached or parsed sections
    conn = get_db()
    r = conn.execute("SELECT summary_json FROM rfp_summaries WHERE notice_id=? ORDER BY id DESC LIMIT 1", (nid,)).fetchone()
    if r:
        try:
            s = json.loads(r[0])
            with st.expander("Brief", expanded=True): st.write(s.get("sections",{}).get("Brief",[]) or "No hits yet.")
            with st.expander("Factors"): st.write(s.get("sections",{}).get("Factors",[]) or "None")
            with st.expander("Clauses"): st.write(s.get("sections",{}).get("Clauses",[]) or "None")
            with st.expander("Dates"): st.write(s.get("sections",{}).get("Dates",[]) or "None")
            with st.expander("Forms"): st.write(s.get("sections",{}).get("Forms",[]) or "None")
            with st.expander("Milestones"): st.write(s.get("sections",{}).get("Milestones",[]) or "None")
        except Exception:
            st.info("No summary parsed yet.")

    # Q and A
    st.markdown("**Ask only from parsed docs**")
    q = st.text_input("Your question", key="rfp_q")
    if st.button("Ask"):
        if not q.strip():
            st.warning("Enter a question")
        else:
            hits = _qa_from_chunks(nid, q.strip(), limit=5)
            if not hits:
                st.info("No matching passages in parsed files.")
            else:
                for h in hits:
                    st.write(f"{h['file']} p{h['page']}: {h['snippet']}")

    # Parser tabs when enabled
    if st.session_state.get("feature_flags", {}).get("rfp_parser"):
        data = _load_latest_rfp_json(nid)
        t1, t2, t3, t4, t5 = st.tabs(["Summary","L and M","Clauses","Forms","Submission"])
        with t1:
            st.json(data or {"info":"no parsed data"})
        with t2:
            st.write((data or {}).get("lm_requirements") or "No L/M parsed")
        with t3:
            st.write((data or {}).get("clauses") or "No clauses parsed")
        with t4:
            st.write((data or {}).get("deliverables_forms") or "No forms parsed")
        with t5:
            st.write((data or {}).get("submission") or "No submission parsed")

# UI hook inside SAM Watch list
def _sam_row_open_analyzer_ui(df):
    import streamlit as st
    # Selection to open panel
    titles = [r["Title"] for r in df.to_dict("records")]
    id_map = {r["Title"]: r["ID"] for r in df.to_dict("records")}
    c1, c2 = st.columns([3,1])
    with c1:
        pick = st.selectbox("Open RFP Analyzer for:", options=titles, index=0 if titles else None, key="rfp_pick_title")
    with c2:
        if st.button("Ask RFP Analyzer"):
            if pick in id_map:
                st.session_state["rfp_panel_open"] = True
                st.session_state["current_notice_id"] = id_map[pick]
                # Keep panel open across reruns
                st.session_state["rfp_cache_key"] = f"nid:{id_map[pick]}::{_combined_checksum(id_map[pick])}"
# ===== end RFP Analyzer Phase 2 =====

# ===== RFP Phase 1: Schema + Validator =====
def ensure_rfp_schema_tables():
    conn = get_db()
    conn.execute("""CREATE TABLE IF NOT EXISTS rfp_schema_versions(
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        version TEXT NOT NULL,
        schema_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(name, version)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS rfp_json(
        id INTEGER PRIMARY KEY,
        notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
        schema_name TEXT NOT NULL,
        schema_version TEXT NOT NULL,
        version_hash TEXT NOT NULL,
        data_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(notice_id, version_hash)
    )""")

RFP_SCHEMA_NAME = "RFPv1"
RFP_SCHEMA_VERSION = "1.0"
RFP_SCHEMA_JSON = {
  "type":"object",
  "required":["header","sections","lm_requirements","submission"],
  "properties":{
    "header":{"type":"object","required":["notice_id","title"],"properties":{
      "notice_id":{"type":"string"},
      "title":{"type":"string"},
      "agency":{"type":"string"},
      "type":{"type":"string"},
      "set_aside":{"type":"string"},
      "place":{"type":"string"},
      "pocs":{"type":"array","items":{"type":"object","properties":{
        "name":{"type":"string"},"email":{"type":"string"},"phone":{"type":"string"},"cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}}}}
    }},
    "volumes":{"type":"array","items":{"type":"object","required":["name"],"properties":{
      "name":{"type":"string"},"required":{"type":"boolean"},"page_limit":{"type":"integer"},"file_type":{"type":"string"},"font":{"type":"string"},"spacing":{"type":"string"},"cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
    }}},
    "sections":{"type":"array","items":{"type":"object","required":["key","title"],"properties":{
      "key":{"type":"string"},"title":{"type":"string"},"parent_volume":{"type":"string"},
      "required":{"type":"boolean"},"page_limit":{"type":"integer"},
      "instructions":{"type":"array","items":{"type":"string"}},
      "cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
    }}},
    "lm_requirements":{"type":"array","items":{"type":"object","required":["id","text"],"properties":{
      "id":{"type":"string"},"text":{"type":"string"},"factor":{"type":"string"},"subfactor":{"type":"string"},
      "evaluation_criterion":{"type":"string"},"must_address":{"type":"array","items":{"type":"string"}},
      "cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
    }}},
    "deliverables_forms":{"type":"array","items":{"type":"object","required":["name"],"properties":{
      "name":{"type":"string"},"form_no":{"type":"string"},"fillable":{"type":"boolean"},
      "where_to_upload":{"type":"string"},"cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
    }}},
    "submission":{"type":"object","required":["due_datetime"],"properties":{
      "method":{"type":"string"},"portals":{"type":"array","items":{"type":"string"}},
      "email":{"type":"string"},"subject_line_format":{"type":"string"},
      "due_datetime":{"type":"string"},"timezone":{"type":"string"},
      "copies":{"type":"integer"},"file_naming_rules":{"type":"string"},
      "zip_rules":{"type":"string"},"cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
    }},
    "milestones":{"type":"array","items":{"type":"object","properties":{
      "name":{"type":"string"},"due_datetime":{"type":"string"},
      "origin":{"type":"string"},"cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
    }}},
    "clauses":{"type":"array","items":{"type":"object","properties":{
      "ref":{"type":"string"},"title":{"type":"string"},"section":{"type":"string"},
      "mandatory":{"type":"boolean"},"notes":{"type":"string"},
      "cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
    }}},
    "sow_tasks":{"type":"array","items":{"type":"object","properties":{
      "task_id":{"type":"string"},"text":{"type":"string"},"location":{"type":"string"},
      "hours_hint":{"type":"number"},"labor_cats_hint":{"type":"array","items":{"type":"string"}},
      "cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
    }}},
    "price_structure":{"type":"object","properties":{
      "clins":{"type":"array","items":{"type":"object","properties":{
        "clin":{"type":"string"},"desc":{"type":"string"},"uom":{"type":"string"},
        "qty_hint":{"type":"number"},"options":{"type":"string"},"cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
      }}},
      "wage_determinations":{"type":"array","items":{"type":"object","properties":{
        "type":{"type":"string"},"id":{"type":"string"},"county_state":{"type":"string"},
        "labor_cats":{"type":"array","items":{"type":"string"}},"rates":{"type":"string"},"fringe":{"type":"string"},
        "cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
      }}}
    }},
    "past_perf_rules":{"type":"object","properties":{
      "count":{"type":"integer"},"years_back":{"type":"integer"},"relevance_dims":{"type":"string"},
      "format":{"type":"string"},"cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
    }},
    "staffing_rules":{"type":"object","properties":{
      "key_personnel":{"type":"string"},"certs":{"type":"string"},"clearances":{"type":"string"},
      "badging":{"type":"string"},"training":{"type":"string"},
      "cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
    }},
    "accessibility_rules":{"type":"object","properties":{
      "req_508":{"type":"boolean"},"pdf_tags":{"type":"boolean"},"bookmarks":{"type":"boolean"},
      "alt_text":{"type":"boolean"},"cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
    }},
    "risks_assumptions":{"type":"array","items":{"type":"object","properties":{
      "risk":{"type":"string"},"impact":{"type":"string"},"mitigation":{"type":"string"},
      "cite":{"type":"object","properties":{"file":{"type":"string"},"page":{"type":"integer"}}}
    }}}
  }
}

def _store_rfp_schema_if_missing():
    ensure_rfp_schema_tables()
    conn = get_db()
    r = conn.execute("SELECT 1 FROM rfp_schema_versions WHERE name=? AND version=?", (RFP_SCHEMA_NAME, RFP_SCHEMA_VERSION)).fetchone()
    if not r:
        conn.execute("INSERT INTO rfp_schema_versions(name, version, schema_json, created_at) VALUES(?,?,?,?)",
                     (RFP_SCHEMA_NAME, RFP_SCHEMA_VERSION, json.dumps(RFP_SCHEMA_JSON, ensure_ascii=False), utc_now_iso()))

def _is_iso_with_tz(s: str) -> bool:
    import re as _re
    if not isinstance(s, str):
        return False
    return bool(_re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$", s))

def _require_cite(obj) -> bool:
    if not isinstance(obj, dict):
        return False
    c = obj.get("cite")
    if not isinstance(c, dict):
        return False
    if not isinstance(c.get("file"), str) or not c.get("file"):
        return False
    pg = c.get("page")
    try:
        return int(pg) >= 1
    except Exception:
        return False

def validate_rfpv1(data: dict) -> tuple[bool, list]:
    errs = []
    if not isinstance(data, dict):
        return False, ["root not object"]
    # required roots
    for k in ["header","sections","lm_requirements","submission"]:
        if k not in data:
            errs.append(f"missing {k}")
    hdr = data.get("header") or {}
    if not isinstance(hdr, dict):
        errs.append("header not object")
    else:
        for k in ["notice_id","title"]:
            if not isinstance(hdr.get(k), str) or not hdr.get(k):
                errs.append(f"header.{k} missing or not string")
        # header.pocs cites if present
        if "pocs" in hdr:
            if not isinstance(hdr["pocs"], list):
                errs.append("header.pocs not array")
            else:
                for i, poc in enumerate(hdr["pocs"]):
                    if any(poc.get(x) for x in ["name","email","phone"]):
                        if not _require_cite(poc):
                            errs.append(f"header.pocs[{i}] missing cite")
    # arrays with cite enforcement
    def _check_array(name):
        arr = data.get(name)
        if arr is None:
            return
        if not isinstance(arr, list):
            errs.append(f"{name} not array")
            return
        for i, item in enumerate(arr):
            if not isinstance(item, dict):
                errs.append(f"{name}[{i}] not object")
            else:
                if "cite" in item and not _require_cite(item):
                    errs.append(f"{name}[{i}] bad cite")
    for arrname in ["volumes","sections","lm_requirements","deliverables_forms","milestones","clauses","sow_tasks"]:
        _check_array(arrname)
    # price_structure nested arrays
    ps = data.get("price_structure")
    if ps is not None and isinstance(ps, dict):
        for arrname in ["clins","wage_determinations"]:
            arr = ps.get(arrname)
            if arr is not None:
                if not isinstance(arr, list):
                    errs.append(f"price_structure.{arrname} not array")
                else:
                    for i, item in enumerate(arr):
                        if "cite" in item and not _require_cite(item):
                            errs.append(f"price_structure.{arrname}[{i}] bad cite")
    # submission
    sub = data.get("submission") or {}
    if not isinstance(sub, dict):
        errs.append("submission not object")
    else:
        if not _is_iso_with_tz(sub.get("due_datetime","")):
            errs.append("submission.due_datetime not ISO with timezone")
        if "cite" in sub and not _require_cite(sub):
            errs.append("submission bad cite")
    return (len(errs) == 0), errs

def _rfp_version_hash_for_notice(nid: int) -> str:
    # Use combined file checksum if available, else sha of notice fields
    try:
        return _combined_checksum(int(nid))
    except Exception:
        conn = get_db()
        r = conn.execute("SELECT sam_notice_id, title, due_at FROM notices WHERE id=?", (int(nid),)).fetchone()
        s = json.dumps({"sid": r[0] if r else "", "title": r[1] if r else "", "due": r[2] if r else ""}, sort_keys=True)
        import hashlib
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

def save_rfp_json(notice_id: int, data: dict):
    """
    Validate against RFPv1 1.0 and store to rfp_json keyed by version_hash.
    Returns dict(ok, errors?).
    """
    import streamlit as st
    if not st.session_state.get("feature_flags", {}).get("rfp_schema"):
        return {"ok": False, "disabled": True}
    _store_rfp_schema_if_missing()
    ok, errs = validate_rfpv1(data)
    if not ok:
        return {"ok": False, "errors": errs}
    conn = get_db()
    vhash = _rfp_version_hash_for_notice(int(notice_id))
    conn.execute("""INSERT OR IGNORE INTO rfp_json(notice_id, schema_name, schema_version, version_hash, data_json, created_at)
                    VALUES(?,?,?,?,?,?)""",
                 (int(notice_id), RFP_SCHEMA_NAME, RFP_SCHEMA_VERSION, vhash, json.dumps(data, ensure_ascii=False), utc_now_iso()))
    return {"ok": True, "version_hash": vhash}

def build_rfpv1_from_notice(notice_id: int) -> dict | None:
    """
    Minimal adapter: uses notices table and rfp_chunks to cite due date if possible.
    Omits fields without sources. Does not guess.
    """
    conn = get_db()
    r = conn.execute("SELECT sam_notice_id, title, agency, notice_type, set_aside, place_city, place_state, due_at FROM notices WHERE id=?", (int(notice_id),)).fetchone()
    if not r:
        return None
    sid, title, agency, ntype, set_aside, city, state, due = r
    place = ", ".join([x for x in [city or "", state or ""] if x])
    data = {
        "header": {
            "notice_id": str(sid or notice_id),
            "title": str(title or f"Notice {notice_id}"),
        },
        "sections": [],
        "lm_requirements": [],
        "submission": {}
    }
    if agency: data["header"]["agency"] = agency
    if ntype: data["header"]["type"] = ntype
    if set_aside: data["header"]["set_aside"] = set_aside
    if place: data["header"]["place"] = place

    # submission due datetime: only include if already ISO with tz
    if isinstance(due, str) and _is_iso_with_tz(due):
    _pt0 = _time.perf_counter()
        # Try locate cite from rfp_chunks
        cite = None
        try:
            # search for the date part
            date_part = due.split("T")[0]
            rows = conn.execute("SELECT file_name, page FROM rfp_chunks WHERE notice_id=? AND text LIKE ? LIMIT 1", (int(notice_id), f"%{date_part}%")).fetchall()
            if rows:
                cite = {"file": rows[0][0], "page": int(rows[0][1])}
        except Exception:
            pass
        data["submission"]["due_datetime"] = due
        if cite: data["submission"]["cite"] = cite

    metric_push('parser_time_ms', (_time.perf_counter()-_pt0)*1000.0, {'result':'ok'}); return data

# Hook: after parse_rfp success, optionally build and store schema JSON
def _rfp_phase1_maybe_store(nid: int):
    import streamlit as st
    if not st.session_state.get("feature_flags", {}).get("rfp_schema"):
        return
    doc = build_rfpv1_from_notice(int(nid))
    if not doc:
        return
    res = save_rfp_json(int(nid), doc)
    try:
        if feature_flags().get('compliance_v2', False) and res.get('ok'):
            doc = build_rfpv1_from_notice(int(nid))
            if doc:
                build_lm_checklist(int(nid), doc)
    except Exception as _seed_ex:
        log_event('warn','lm_seed_failed', notice_id=int(nid), err=str(_seed_ex))
    try:
        if res.get('ok'):
        try:
            if feature_flags().get('rtm', False):
                doc = build_rfpv1_from_notice(int(nid))
                if doc:
                    build_rtm(int(nid), doc)
        except Exception:
            pass
        try:
            cnt = relock_on_amendment(int(nid))
            import streamlit as st
            st.session_state['amend_impact_count'] = cnt
        except Exception:
            pass
            ensure_needs_review_if_green(int(nid))
    except Exception:
        pass

    if res.get("ok"):
        st.session_state["rfp_schema_ready"] = True
    else:
        log_event("warn","rfp_json_not_saved", notice_id=int(nid), errors=res.get("errors"))

# === COMPLIANCE GATE PHASE 9 ===
def ensure_compliance_schema():
    conn = get_db()
    cur = conn.cursor()
    # lm_checklist
    cur.execute("""CREATE TABLE IF NOT EXISTS lm_checklist(
        id INTEGER PRIMARY KEY,
        notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
        factor TEXT,
        subfactor TEXT,
        requirement TEXT NOT NULL,
        source_page TEXT,
        owner_id TEXT,
        due_date TEXT,
        status TEXT NOT NULL CHECK(status IN('Red','Yellow','Green')),
        notes TEXT
    );""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lm_notice ON lm_checklist(notice_id);")
    # required_docs
    cur.execute("""CREATE TABLE IF NOT EXISTS required_docs(
        id INTEGER PRIMARY KEY,
        notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        template_key TEXT,
        required INTEGER NOT NULL DEFAULT 1,
        provided INTEGER NOT NULL DEFAULT 0,
        file_id TEXT
    );""")
    # signoffs
    cur.execute("""CREATE TABLE IF NOT EXISTS signoffs(
        id INTEGER PRIMARY KEY,
        notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
        role TEXT NOT NULL CHECK(role IN('Tech','Price','Contracts')),
        user_id TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN('Pending','Approved','Rejected')),
        ts TEXT NOT NULL
    );""")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_signoff_role ON signoffs(notice_id, role);")
    # qa_log
    cur.execute("""CREATE TABLE IF NOT EXISTS qa_log(
        id INTEGER PRIMARY KEY,
        notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
        question TEXT NOT NULL,
        asked_at TEXT,
        deadline TEXT,
        submitted_file_id TEXT
    );""")
    # notices.compliance_state column add if missing
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(notices)")}
        if "compliance_state" not in cols:
            conn.execute("ALTER TABLE notices ADD COLUMN compliance_state TEXT DEFAULT 'Unreviewed'")
    except Exception:
        pass
    conn.commit()

def get_compliance_state(nid: int):
    ensure_compliance_schema()
    conn = get_db()
    cur = conn.cursor()
    # checklist
    rows = cur.execute("SELECT status FROM lm_checklist WHERE notice_id=?", (int(nid),)).fetchall()
    all_green = bool(rows) and all((r[0]=='Green') for r in rows)
    # required docs
    docs = cur.execute("SELECT required, provided FROM required_docs WHERE notice_id=?", (int(nid),)).fetchall()
    req_ok = all((int(r)==0 or int(p)==1) for r,p in docs) if docs else False
    # signoffs
    signs = cur.execute("SELECT role, status FROM signoffs WHERE notice_id=?", (int(nid),)).fetchall()
    roles = {r:s for r,s in signs}
    sign_ok = all(roles.get(r)=='Approved' for r in ['Tech','Price','Contracts'])
    ok = all_green and req_ok and sign_ok
    # collect unmet reasons
    unmet = []
    if not all_green:
        unmet.append("Checklist items not all Green")
    if not req_ok:
        unmet.append("Required documents missing")
    if not sign_ok:
        unmet.append("All signoffs not Approved")
    # current stored state
    cur.execute("SELECT compliance_state FROM notices WHERE id=?", (int(nid),))
    srow = cur.fetchone()
    stored = srow[0] if srow else "Unreviewed"
    return ok, unmet, stored

def recompute_and_store_compliance(nid: int):
    ensure_compliance_schema()
    ok, unmet, stored = get_compliance_state(int(nid))
    new_state = 'Green' if ok else 'Needs review'
    conn = get_db()
    conn.execute("UPDATE notices SET compliance_state=? WHERE id=?", (new_state, int(nid)))
    conn.commit()
    return new_state, unmet

def ensure_needs_review_if_green(nid: int):
    ensure_compliance_schema()
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT compliance_state FROM notices WHERE id=?", (int(nid),))
    row = cur.fetchone()
    if not row: 
        return
    if row[0] == 'Green':
        conn.execute("UPDATE notices SET compliance_state='Needs review' WHERE id=?", (int(nid),))
        conn.commit()

def upsert_checklist_row(nid:int, data:dict):
    ensure_compliance_schema()
    conn = get_db()
    cur = conn.cursor()
    if data.get("id"):
        cols = ["factor","subfactor","requirement","source_page","owner_id","due_date","status","notes"]
        sets = ", ".join(f"{c}=?" for c in cols)
        vals = [data.get(c) for c in cols] + [int(data["id"])]
        cur.execute(f"UPDATE lm_checklist SET {sets} WHERE id=?", vals)
    else:
        cur.execute("""INSERT INTO lm_checklist(notice_id,factor,subfactor,requirement,source_page,owner_id,due_date,status,notes)
                    VALUES(?,?,?,?,?,?,?,?,?)""", (int(nid), data.get("factor"), data.get("subfactor"), data.get("requirement"),
                    data.get("source_page"), data.get("owner_id"), data.get("due_date"), data.get("status") or 'Red', data.get("notes")))
    conn.commit()
    recompute_and_store_compliance(int(nid))

def upsert_required_doc(nid:int, data:dict):
    ensure_compliance_schema()
    conn = get_db()
    cur = conn.cursor()
    if data.get("id"):
        cols = ["name","template_key","required","provided","file_id"]
        sets = ", ".join(f"{c}=?" for c in cols)
        vals = [data.get(c) for c in cols] + [int(data["id"])]
        cur.execute(f"UPDATE required_docs SET {sets} WHERE id=?", vals)
    else:
        cur.execute("""INSERT INTO required_docs(notice_id,name,template_key,required,provided,file_id)
                    VALUES(?,?,?,?,?,?)""", (int(nid), data.get("name"), data.get("template_key"), int(data.get("required",1)),
                    int(data.get("provided",0)), data.get("file_id")))
    conn.commit()
    recompute_and_store_compliance(int(nid))

def set_signoff(nid:int, role:str, status:str, user_id:str):
    ensure_compliance_schema()
    conn = get_db()
    cur = conn.cursor()
    ts = utc_now_iso() if 'utc_now_iso' in globals() else dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    # upsert
    cur.execute("""INSERT INTO signoffs(notice_id, role, user_id, status, ts)
                VALUES(?,?,?,?,?)
                ON CONFLICT(notice_id, role) DO UPDATE SET user_id=excluded.user_id, status=excluded.status, ts=excluded.ts""",
                (int(nid), role, user_id or "unknown", status, ts))
    conn.commit()
    recompute_and_store_compliance(int(nid))

def add_qa_row(nid:int, question:str, deadline:str=None, submitted_file_id:str=None):
    ensure_compliance_schema()
    conn = get_db()
    conn.execute("""INSERT INTO qa_log(notice_id, question, asked_at, deadline, submitted_file_id)
                VALUES(?,?,?,?,?)""", (int(nid), question, dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), deadline, submitted_file_id))
    conn.commit()

def render_compliance_panel():
    import streamlit as st
    if not feature_flags().get("compliance_gate", False):
        return
    if not st.session_state.get("compliance_tab_open") or not st.session_state.get("selected_notice_id"):
        return
    ensure_compliance_schema()
    nid = int(st.session_state["selected_notice_id"])
    st.markdown("---")
    st.subheader("Compliance")
    # Amendment impact banner
    try:
        cnt = int(st.session_state.get('amend_impact_count') or 0)
        if cnt > 0:
            st.warning(f"Amendment impacted {cnt} checklist row(s). Review required.")
    except Exception:
        pass

    # Current state
    ok, unmet, stored = get_compliance_state(nid)
    st.caption(f"State: {stored}. {'All clear' if ok else 'Blocked'}")
    # Gate box
    try:
        if feature_flags().get('compliance_gate_v2', False):
            ok2, unmet2 = gate_status(nid)
            counts = _gate_counts(nid)
            st.markdown("#### Gate")
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Green", counts.get("Green",0))
            c2.metric("Yellow", counts.get("Yellow",0))
            c3.metric("Red", counts.get("Red",0))
            c4.markdown("🔓 Unlocked" if ok2 else "🔒 Locked")
            if not ok2 and unmet2:
                st.info("Gate reasons: " + "; ".join(unmet2))
    except Exception as _gx:
        st.caption(f"[Gate box error: {_gx}]")

    if unmet:
        st.warning("Unmet: " + "; ".join(unmet))
    # Checklist editor
    st.markdown("#### L and M checklist")
    conn = get_db()
    import pandas as pd
    
    # Bulk assign by factor
    try:
        if feature_flags().get('compliance_gate_v2', False):
            factors = pd.read_sql_query("select distinct factor from lm_checklist where notice_id=? order by factor", conn, params=(nid,))
            if not factors.empty:
                b1,b2,b3,b4 = st.columns([2,2,2,1])
                with b1:
                    sel_factor = st.selectbox("Factor", options=factors['factor'].dropna().tolist())
                with b2:
                    new_owner = st.text_input("Assign owner")
                with b3:
                    new_due = st.text_input("Assign due date")
                with b4:
                    if st.button("Apply", key="bulk_assign"):
                        bulk_assign_by_factor(nid, sel_factor, new_owner or None, new_due or None)
                        st.success("Bulk assignment applied.")
                        st.rerun()
    except Exception as _bu:
        st.caption(f"[Bulk assign error: {_bu}]")
df = pd.read_sql_query("select id, factor, subfactor, requirement, source_page, owner_id, due_date, status, notes from lm_checklist where notice_id=? order by id", conn, params=(nid,))
    edited = st.data_editor(df, use_container_width=True, num_rows="dynamic",
                            column_config={
                                "status": st.column_config.SelectboxColumn(options=["Red","Yellow","Green"]),
                                "due_date": st.column_config.TextColumn(help="YYYY-MM-DD or ISO datetime")
                            },
                            key=f"lm_edit_{nid}")
    if st.button("Save checklist"):
        # detect new or edited rows by comparing ids
        existing_ids = set(df["id"].tolist())
        for _, row in edited.iterrows():
            data = row.to_dict()
            if pd.isna(data.get("id")):
                data["id"] = None
            upsert_checklist_row(nid, data)
        
        # Enforce Green requires evidence and linked docs provided when v2 gate active
        if feature_flags().get('compliance_gate_v2', False):
            for _, row in edited.iterrows():
                if str(row.get("status")) == "Green":
                    okv, msg = before_set_status_green(nid, int(row.get("id")) if not pd.isna(row.get("id")) else -1)
                    if not okv:
                        st.warning(f"Row {int(row.get('id') or 0)} cannot be Green: {msg}")
                        try:
                            conn.execute("update lm_checklist set status='Yellow' where id=?", (int(row.get("id")),))
                            conn.commit()
                        except Exception:
                            pass
st.success("Checklist saved.")
        st.session_state["compliance_tab_open"] = True
        st.rerun()
    # Required docs
    st.markdown("#### Required documents")
    df2 = pd.read_sql_query("select id, name, template_key, required, provided, file_id from required_docs where notice_id=? order by id", conn, params=(nid,))
    edited2 = st.data_editor(df2, use_container_width=True, num_rows="dynamic",
                             column_config={
                                "required": st.column_config.CheckboxColumn(),
                                "provided": st.column_config.CheckboxColumn(),
                             },
                             key=f"rd_edit_{nid}")
    if st.button("Save documents"):
        for _, row in edited2.iterrows():
            data = row.to_dict()
            if pd.isna(data.get("id")):
                data["id"] = None
            upsert_required_doc(nid, data)
        st.success("Documents saved.")
        st.session_state["compliance_tab_open"] = True
        st.rerun()
    # Signoffs
    st.markdown("#### Signoffs")
    roles = ["Tech","Price","Contracts"]
    cols = st.columns(len(roles))
    uid = st.session_state.get("user_id") or st.session_state.get("current_user_id") or "unknown"
    for i, role in enumerate(roles):
        with cols[i]:
            if st.button(f"Approve {role}"):
                set_signoff(nid, role, "Approved", str(uid))
                st.rerun()
            if st.button(f"Reject {role}"):
                set_signoff(nid, role, "Rejected", str(uid))
                st.rerun()
    # Q and A log
    st.markdown("#### Q and A")
    q = st.text_input("Question")
    d = st.text_input("Deadline")
    if st.button("Add Q and A"):
        if q.strip():
            add_qa_row(nid, q.strip(), deadline=d or None)
            st.success("Added.")
            st.rerun()
    qa = pd.read_sql_query("select id, question, asked_at, deadline, submitted_file_id from qa_log where notice_id=? order by id desc", conn, params=(nid,))
    st.dataframe(qa, use_container_width=True, hide_index=True)
    # Audit drawer
    render_compliance_audit_drawer(nid)

    # v2 controls and viewer
    _compliance_v2_controls_in_panel(nid)
    render_compliance_v2_evidence_viewer()




# === COMPLIANCE PHASE 1 (v2) ===
import hashlib as _hashlib

def _safe_sha1(s: str) -> str:
    try:
        return _hashlib.sha1((s or '').encode('utf-8')).hexdigest()
    except Exception:
        return None

def compliance_v2_schema_upgrade():
    ensure_compliance_schema()  # base tables from Phase 9
    conn = get_db()
    cur = conn.cursor()
    # Add new columns with guards
    try:
        cols = {r[1] for r in cur.execute("PRAGMA table_info(lm_checklist)").fetchall()}
        if "req_id" not in cols:
            cur.execute("ALTER TABLE lm_checklist ADD COLUMN req_id TEXT")
        if "cite_file" not in cols:
            cur.execute("ALTER TABLE lm_checklist ADD COLUMN cite_file TEXT")
        if "cite_page" not in cols:
            cur.execute("ALTER TABLE lm_checklist ADD COLUMN cite_page INTEGER")
        if "evidence_file_id" not in cols:
            cur.execute("ALTER TABLE lm_checklist ADD COLUMN evidence_file_id INTEGER REFERENCES notice_files(id)")
        if "evidence_page" not in cols:
            cur.execute("ALTER TABLE lm_checklist ADD COLUMN evidence_page INTEGER")
        if "evidence_section_id" not in cols:
            cur.execute("ALTER TABLE lm_checklist ADD COLUMN evidence_section_id INTEGER REFERENCES proposal_sections(id)")
        if "weight" not in cols:
            cur.execute("ALTER TABLE lm_checklist ADD COLUMN weight REAL DEFAULT 1")
        if "last_updated_by" not in cols:
            cur.execute("ALTER TABLE lm_checklist ADD COLUMN last_updated_by TEXT")
    except Exception:
        pass
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_lm_req ON lm_checklist(notice_id, req_id)")
    except Exception:
        pass
    conn.commit()

def _norm_req_text(txt: str) -> str:
    if not isinstance(txt, str):
        return ""
    t = txt.strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t

def build_lm_checklist(notice_id: int, rfp_json: dict):
    """Seed checklist from Analyzer JSON.
    Stable req_id = sha1(normalized requirement text plus factor).
    Inserts only new rows. No duplicate on rerun due to unique index.
    """
    if not feature_flags().get('compliance_v2', False):
        return {'ok': False, 'disabled': True}
    compliance_v2_schema_upgrade()
    conn = get_db()
    cur = conn.cursor()
    nid = int(notice_id)
    # Heuristic extraction of L and M requirements from rfp_json
    # Expect structure like data['sections']['Factors'] -> list of items with factor, subfactor, requirements[]
    sections = (rfp_json or {}).get('sections') or {}
    factors = sections.get('Factors') or sections.get('factors') or []
    inserted = 0
    for item in factors:
        factor = item.get('factor') or item.get('name') or item.get('title') or ''
        subitems = item.get('subfactors') or item.get('subs') or []
        reqs = []
        # Some schemas have requirements directly on factor
        if item.get('requirements'):
            for r in item['requirements']:
                reqs.append( (factor, None, str(r.get('text') or r) , r.get('cite_file'), r.get('cite_page')) )
        # Subfactors with requirements
        for sub in subitems:
            subf = sub.get('subfactor') or sub.get('name') or sub.get('title') or ''
            for r in sub.get('requirements') or []:
                reqs.append( (factor, subf, str(r.get('text') or r), r.get('cite_file'), r.get('cite_page')) )
        # Fallback: if 'L' or 'M' string hints exist
        if not reqs and item:
            txt = str(item)
            if 'section l' in txt.lower() or 'section m' in txt.lower():
                reqs.append( (factor, None, _norm_req_text(txt)[:200], None, None) )
        # Insert rows
        for fac, subf, req_text, cfile, cpage in reqs:
            norm = _norm_req_text(req_text) + '|' + (_norm_req_text(fac) or '')
            rid = _safe_sha1(norm) or None
            if not rid:
                continue
            try:
                cur.execute("""INSERT OR IGNORE INTO lm_checklist(
                    notice_id, req_id, factor, subfactor, requirement, cite_file, cite_page, status
                ) VALUES(?,?,?,?,?,?,?,?)""",
                (nid, rid, fac or None, subf or None, req_text.strip(), cfile, int(cpage) if cpage else None, 'Red'))
                if cur.rowcount:
                    inserted += 1
            except Exception:
                pass
    conn.commit()
    return {'ok': True, 'inserted': inserted}

def render_compliance_v2_evidence_viewer():
    import streamlit as st, pandas as pd
    if not feature_flags().get('compliance_v2', False):
        return
    if not st.session_state.get('evidence_panel_open'):
        return
    file_id = st.session_state.get('current_evidence_file_id')
    page = st.session_state.get('current_evidence_page')
    if not file_id:
        return
    st.markdown("### Evidence Viewer")
    conn = get_db()
    row = conn.execute("select id, file_name, url, content_type, local_path from notice_files where id=?", (int(file_id),)).fetchone()
    if not row:
        st.info("File not found.")
        return
    _, fname, url, ctype, local_path = row
    st.caption(f"{fname} • page {page if page else '?'}")
    # Simple preview: try to extract text of the page for quick context
    try:
        import PyPDF2
        path = local_path or None
        if path:
            with open(path, 'rb') as f:
                r = PyPDF2.PdfReader(f)
                pg = int(page)-1 if page else 0
                if 0 <= pg < len(r.pages):
                    text = r.pages[pg].extract_text() or "(no text extractable)"
                    st.text_area("Page text", value=text, height=240)
        else:
            st.info("File not cached locally. Open in system viewer.")
    except Exception as ex:
        st.caption(f"[PDF preview unavailable: {ex}]")
    if url:
        st.link_button("Open original", url)

def _compliance_v2_controls_in_panel(nid: int):
    import streamlit as st, pandas as pd
    if not feature_flags().get('compliance_v2', False):
        return
    compliance_v2_schema_upgrade()
    conn = get_db()
    st.markdown("#### L and M checklist (v2)")
    df = pd.read_sql_query("select id, req_id, factor, subfactor, requirement, cite_file, cite_page, evidence_file_id, evidence_page, status, owner_id, due_date from lm_checklist where notice_id=? order by id", conn, params=(int(nid),))
    # Show grid with no Analyzer editing. We allow status here in Compliance tab.
    edited = st.data_editor(df, use_container_width=True, num_rows=0,
                            column_config={
                                "status": st.column_config.SelectboxColumn(options=["Red","Yellow","Green"]),
                                "due_date": st.column_config.TextColumn(),
                            },
                            key=f"lm_v2_edit_{nid}")
    # Evidence actions per selected row
    sel = st.multiselect("Select row(s) to set evidence", options=df['id'].tolist(), key=f"lm_v2_sel_{nid}")
    c1,c2 = st.columns(2)
    with c1:
        if st.button("Set evidence") and sel:
            # simple picker: choose file and page
            files = pd.read_sql_query("select id, file_name from notice_files where notice_id=? order by id desc", conn, params=(int(nid),))
            file_id = st.selectbox("File", options=files['id'].tolist(), format_func=lambda x: files.set_index('id').loc[x,'file_name'] if not files.empty else str(x), key=f"ev_file_{nid}")
            page = st.number_input("Page", min_value=1, step=1, value=1, key=f"ev_page_{nid}")
            if st.button("Save evidence", key=f"ev_save_{nid}"):
                for rid in sel:
                    conn.execute("update lm_checklist set evidence_file_id=?, evidence_page=?, last_updated_by=? where id=?", (int(file_id), int(page), str(st.session_state.get('user_id') or 'unknown'), int(rid)))
                conn.commit()
                st.success("Evidence saved.")
                st.session_state['compliance_tab_open'] = True
                st.rerun()
    with c2:
        if st.button("View evidence") and sel:
            # open viewer for first selected
            rid = int(sel[0])
            row = conn.execute("select evidence_file_id, evidence_page from lm_checklist where id=?", (rid,)).fetchone()
            if row and row[0]:
                st.session_state['evidence_panel_open'] = True
                st.session_state['current_evidence_file_id'] = int(row[0])
                st.session_state['current_evidence_page'] = int(row[1] or 1)
                st.experimental_rerun()

def analyzer_lm_readonly(nid: int):
    """Read-only L and M in Analyzer with Open in Compliance button."""
    if not feature_flags().get('compliance_v2', False):
        return
    import streamlit as st, pandas as pd
    compliance_v2_schema_upgrade()
    conn = get_db()
    df = pd.read_sql_query("select factor, subfactor, requirement, cite_file, cite_page, status from lm_checklist where notice_id=? order by id", conn, params=(int(nid),))
    st.markdown("#### Section L & M requirements (read-only)")
    st.dataframe(df, use_container_width=True, hide_index=True)
    if st.button("Open in Compliance"):
        st.session_state['selected_notice_id'] = int(nid)
        st.session_state['compliance_tab_open'] = True
        st.experimental_rerun()



# === COMPLIANCE PHASE 2 (Gate v2) ===
def ensure_compliance_gate_v2_schema():
    ensure_compliance_schema()
    compliance_v2_schema_upgrade()
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS lm_doc_links(
            id INTEGER PRIMARY KEY,
            notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
            req_id TEXT NOT NULL,
            required_doc_id INTEGER REFERENCES required_docs(id),
            UNIQUE(notice_id, req_id, required_doc_id)
        );""" )
    except Exception:
        pass
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS section_requirements(
            id INTEGER PRIMARY KEY,
            proposal_id INTEGER NOT NULL REFERENCES proposals(id) ON DELETE CASCADE,
            section_key TEXT NOT NULL,
            req_id TEXT NOT NULL,
            UNIQUE(proposal_id, section_key, req_id)
        );""" )
    except Exception:
        pass
    conn.commit()

def before_set_status_green(nid:int, row_id:int):
    if not feature_flags().get('compliance_gate_v2', False):
        return True, ''
    ensure_compliance_gate_v2_schema()
    conn = get_db()
    cur = conn.cursor()
    r = cur.execute("select req_id, evidence_file_id, evidence_section_id from lm_checklist where id=? and notice_id=?", (int(row_id), int(nid))).fetchone()
    if not r:
        return False, "Checklist row missing"
    req_id, ev_file, ev_sec = r
    if not ev_file and not ev_sec:
        return False, "Evidence required"
    missing_docs = []
    for rid, in cur.execute("select required_doc_id from lm_doc_links where notice_id=? and req_id=?", (int(nid), req_id)).fetchall():
        d = cur.execute("select provided, name from required_docs where id=?", (int(rid),)).fetchone()
        if d and int(d[0]) != 1:
            missing_docs.append(d[1] or f"doc:{rid}")
    if missing_docs:
        return False, "Missing docs: " + ", ".join(missing_docs)
    return True, ''

def gate_status(nid:int):
    ensure_compliance_gate_v2_schema()
    conn = get_db()
    cur = conn.cursor()
    unmet = []
    signs = {r:s for r,s in cur.execute("select role, status from signoffs where notice_id=?", (int(nid),)).fetchall()}
    if not all(signs.get(r) == 'Approved' for r in ['Tech','Price','Contracts']):
        unmet.append("All signoffs must be Approved")
    links = cur.execute("select distinct required_doc_id from lm_doc_links where notice_id=?", (int(nid),)).fetchall()
    if links:
        doc_ids = [int(x[0]) for x in links if x and x[0] is not None]
        if doc_ids:
            q = "select provided, name from required_docs where id in (" + ",".join(str(i) for i in doc_ids) + ")"
            missing = [name for provided,name in cur.execute(q).fetchall() if int(provided)!=1]
            if missing:
                unmet.append("Required docs not provided: " + ", ".join(missing))
    rows = cur.execute("select id, status, evidence_file_id, evidence_section_id from lm_checklist where notice_id=?", (int(nid),)).fetchall()
    if not rows:
        unmet.append("Checklist empty")
    else:
        if any(r[1] != 'Green' for r in rows):
            unmet.append("All checklist rows must be Green")
        evidence_missing = [str(r[0]) for r in rows if r[1]=='Green' and not (r[2] or r[3])]
        if evidence_missing:
            unmet.append("Green rows missing evidence: " + ", ".join(evidence_missing))
    return (len(unmet)==0), unmet

def _gate_counts(nid:int):
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute("select status, count(*) from lm_checklist where notice_id=? group by status", (int(nid),)).fetchall()
    d = {'Red':0,'Yellow':0,'Green':0}
    for s,c in rows:
        if s in d: d[s]=int(c)
    return d

def bulk_assign_by_factor(nid:int, factor:str, owner_id:str|None, due_date:str|None):
    ensure_compliance_gate_v2_schema()
    conn = get_db()
    cur = conn.cursor()
    cur.execute("update lm_checklist set owner_id=?, due_date=?, last_updated_by=? where notice_id=? and factor=?",
                (owner_id, due_date, str(owner_id or ''), int(nid), factor))
    conn.commit()
    return True



# === COMPLIANCE PHASE 3: Relock, Audit, Reminders, Snapshot ===
def ensure_compliance_audit_schema():
    conn = get_db()
    conn.execute("""CREATE TABLE IF NOT EXISTS compliance_audit(
      id INTEGER PRIMARY KEY,
      notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
      user_id TEXT,
      ts TEXT NOT NULL,
      action TEXT NOT NULL,
      req_id TEXT,
      before_json TEXT,
      after_json TEXT
    );""" )
    conn.commit()

def _audit_log(nid:int, action:str, user_id:str|None, req_id:str|None, before:dict|None, after:dict|None):
    ensure_compliance_audit_schema()
    conn = get_db()
    b = json.dumps(before or {}, ensure_ascii=False)
    a = json.dumps(after or {}, ensure_ascii=False)
    conn.execute("INSERT INTO compliance_audit(notice_id, user_id, ts, action, req_id, before_json, after_json) VALUES(?,?,?,?,?,?,?)",
                 (int(nid), user_id or '', utc_now_iso() if 'utc_now_iso' in globals() else _dt.datetime.utcnow().isoformat() + 'Z', action, req_id or '', b, a))
    conn.commit()

def relock_on_amendment(nid:int):
    if not feature_flags().get('compliance_relock', False):
        return 0
    ensure_compliance_schema()
    conn = get_db(); cur = conn.cursor()
    try:
        versions = _load_versions(int(nid))
        if len(versions) < 2:
            return 0
        prev = versions[1]['payload']; curr = versions[0]['payload']
        prev_files = (prev or {}).get('files') or []
        curr_files = (curr or {}).get('files') or []
        dif = _diff_files(prev_files, curr_files)
        changed = set(dif.get('added',[]) + dif.get('removed',[]))
    except Exception:
        changed = set()
    if not changed:
        return 0
    rows = cur.execute("SELECT id, req_id, status, cite_file, cite_page FROM lm_checklist WHERE notice_id=?", (int(nid),)).fetchall()
    impacted = [r for r in rows if r[3] and r[3] in changed]
    for rid, req_id, status, cfile, cpage in impacted:
        before = {"status": status, "cite_file": cfile, "cite_page": cpage}
        try:
            cur.execute("UPDATE lm_checklist SET status='Yellow' WHERE id=?", (int(rid),))
            _audit_log(int(nid), "relock", st.session_state.get("user_id") if 'st' in globals() else None, req_id, before, {"status":"Yellow","reason":"Amendment affected file"})
        except Exception:
            pass
    if impacted:
        try:
            conn.execute("UPDATE notices SET compliance_state='Needs review' WHERE id=?", (int(nid),))
            conn.commit()
        except Exception:
            pass
    return len(impacted)

def schedule_compliance_emails():
    if not feature_flags().get('email_enabled', False):
        return 0
    ensure_compliance_schema()
    conn = get_db(); cur = conn.cursor()
    now = _dt.datetime.utcnow()
    rows = cur.execute("SELECT lc.notice_id, lc.id, lc.owner_id, lc.due_date, lc.status, n.title FROM lm_checklist lc JOIN notices n ON n.id=lc.notice_id WHERE lc.due_date IS NOT NULL AND lc.status != 'Green'").fetchall()
    enq = 0
    for nid, rid, owner, due, status, title in rows:
        try:
            dd = _dt.datetime.fromisoformat(str(due).replace('Z',''))
        except Exception:
            dd = None
        if dd and dd < now:
            to_addr = USER_EMAILS.get(owner, '') if 'USER_EMAILS' in globals() else ''
            if to_addr:
                subj = f"Compliance due: {title} row {rid}"
                link = f"/app?notice_id={nid}&tab=compliance"
                body = f"Row {rid} is due and still {status}. Open: {link}"
                try:
                    conn.execute("INSERT INTO email_queue(to_addr, subject, body, created_at) VALUES(?,?,?,?)",
                                 (to_addr, subj, body, now.isoformat()+ 'Z'))
                    enq += 1
                except Exception:
                    pass
    conn.commit()
    return enq

def render_compliance_audit_drawer(nid:int):
    import streamlit as st, pandas as pd
    ensure_compliance_audit_schema()
    if not st.session_state.get('audit_drawer_open'):
        return
    st.markdown("#### Audit trail")
    conn = get_db()
    df = pd.read_sql_query("SELECT ts, action, user_id, req_id, before_json, after_json FROM compliance_audit WHERE notice_id=? ORDER BY id DESC LIMIT 200", conn, params=(int(nid),))
    st.dataframe(df, use_container_width=True, hide_index=True)
# === END COMPLIANCE PHASE 3 ===


# === END COMPLIANCE PHASE 2 ===


# === END COMPLIANCE PHASE 1 ===


# === END COMPLIANCE GATE PHASE 9 ===
# ===== end RFP Phase 1 =====

# ===== RFP Parser Phase 2 =====
import re as _re
from typing import List, Dict, Tuple

def _norm_iso(s: str) -> str | None:
    if _is_iso_with_tz(s):
        return s
    if isinstance(s, str) and _re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", s):
        return s + "Z"
    return None

def _ensure_file_parse_and_index(nid: int, fid: int, name: str, url: str) -> List[Dict]:
    conn = get_db()
    b, err = _download_file(url, timeout=30)
    if err or not b:
        raise RuntimeError(f"download_failed:{err}")
    sha = hashlib.sha256(b).hexdigest()
    r = conn.execute("SELECT parsed_json FROM file_parses WHERE notice_file_id=? AND checksum=?", (int(fid), sha)).fetchone()
    if r:
        try: pages = json.loads(r[0])
        except Exception: pages = []
    else:
        ftype = _detect_type_by_name(name or url)
        if ftype == "pdf": pages = _parse_pdf_bytes(b)
        elif ftype == "docx": pages = _parse_docx_bytes(b)
        else: pages = [{"page": 1, "text": b.decode('utf-8', errors='ignore') if isinstance(b, (bytes, bytearray)) else ""}]
        conn.execute("INSERT OR IGNORE INTO file_parses(notice_file_id, checksum, parsed_json, created_at) VALUES(?,?,?,?)",
                     (int(fid), sha, json.dumps(pages, ensure_ascii=False), utc_now_iso()))
        try: conn.execute("UPDATE notice_files SET checksum=?, bytes=? WHERE id=?", (sha, len(b), int(fid)))
        except Exception: pass
    try: conn.execute("DELETE FROM rfp_chunks WHERE notice_id=? AND file_name=?", (int(nid), name or url.split('/')[-1]))
    except Exception: pass
    _index_chunks(int(nid), name or url.split('/')[-1], pages)
    return pages

_L_KEYS = ["section l", "instructions to offerors", "proposal instructions"]
_M_KEYS = ["section m", "evaluation factors", "basis of award"]

def _extract_lm(pages: List[Dict], fname: str) -> Tuple[List[Dict], List[Dict]]:
    lm_reqs, sections = [], []
    for p in pages:
        text = (p.get("text") or "")
        low = text.lower()
        if any(k in low for k in _L_KEYS) or any(k in low for k in _M_KEYS):
            for line in text.splitlines():
                m = _re.search(r"\b([LM]\.\d+(?:\.\d+)*)\b(.*)", line.strip())
                if m:
                    sec_id, txt = m.group(1), m.group(2).strip()
                    key = "L" if sec_id.startswith("L") else "M"
                    item = {"id": sec_id, "text": txt, "cite": {"file": fname, "page": int(p.get("page") or 1)}}
                    if key == "L":
                        sections.append({"key": sec_id, "title": txt[:80], "instructions": [txt] if txt else [], "cite": {"file": fname, "page": int(p.get("page") or 1)}})
                    else:
                        lm_reqs.append(item)
            m = _re.search(r"\b(page\s*limit|no\s*more\s*than\s*\d+\s*pages?)", low)
            if m: sections.append({"key": "page_limit", "title": "Page Limit", "instructions": [m.group(0)], "cite": {"file": fname, "page": int(p.get("page") or 1)}})
            for key, pat in [("font", r"\bfont\s*(?:size)?\s*\d{1,2}\b"), ("spacing", r"\b(single|double)\s*spac") , ("copies", r"\b(\d+)\s*copies\b")]:
                m2 = _re.search(pat, low)
                if m2: sections.append({"key": key, "title": key.title(), "instructions": [m2.group(0)], "cite": {"file": fname, "page": int(p.get("page") or 1)}})
    return sections, lm_reqs

def _extract_clauses(pages: List[Dict], fname: str) -> List[Dict]:
    out, pat = [], _re.compile(r"\b(FAR|DFARS)\s*\d{2}\.\d{3}-\d{1,2}\b")
    for p in pages:
        text = p.get("text") or ""
        for m in pat.finditer(text): out.append({"ref": m.group(0), "cite": {"file": fname, "page": int(p.get("page") or 1)}})
    return out

def _extract_forms(pages: List[Dict], fname: str) -> List[Dict]:
    out = []
    for p in pages:
        low = (p.get("text") or "").lower()
        if "sf 1449" in low or "sf1449" in low: out.append({"name": "SF 1449", "form_no": "SF1449", "cite": {"file": fname, "page": int(p.get("page") or 1)}})
        if "sf 33" in low or "sf33" in low: out.append({"name": "SF 33", "form_no": "SF33", "cite": {"file": fname, "page": int(p.get("page") or 1)}})
        if "attachment" in low and ".pdf" in low: out.append({"name": "Attachment", "cite": {"file": fname, "page": int(p.get("page") or 1)}})
    return out

def _extract_submission(pages: List[Dict], fname: str) -> Dict:
    sub = {}
    for p in pages:
        text, low = p.get("text") or "", (p.get("text") or "").lower()
        if "due" in low or "submission" in low or "closing" in low:
            m = _re.search(r"\b(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})?)\b", text)
            if m and not sub.get("due_datetime"):
                iso = _norm_iso(m.group(1))
                if iso: sub["due_datetime"], sub["cite"] = iso, {"file": fname, "page": int(p.get("page") or 1)}
            if "email" in low: sub["method"] = "email"
            if "sam.gov" in low or "piee" in low: sub["method"] = "portal"
            m2 = _re.search(r"(subject[:\s].{0,100})", text, flags=_re.IGNORECASE)
            if m2: sub["subject_line_format"] = m2.group(1).strip()
            m3 = _re.search(r"(file\s*naming.{0,120})", text, flags=_re.IGNORECASE)
            if m3: sub["file_naming_rules"] = m3.group(1).strip()
            m4 = _re.search(r"\b(\d+)\s*copies\b", low)
            if m4:
                try: sub["copies"] = int(m4.group(1))
                except Exception: pass
    return sub

def parse_rfp_v1(notice_id: int) -> dict:
    if not st.session_state.get("feature_flags", {}).get("rfp_parser"):
        return {"ok": False, "disabled": True}
    ensure_rfp_schema_tables()
    nid = int(notice_id)
    vhash = _rfp_version_hash_for_notice(nid)
    conn = get_db()
    r = conn.execute("SELECT data_json FROM rfp_json WHERE notice_id=? AND version_hash=?", (nid, vhash)).fetchone()
    if r:
        try: return {"ok": True, "cached": True, "data": json.loads(r[0]), "version_hash": vhash}
        except Exception: pass

    files = _notice_files(nid)
    if not files: return err_with_id("no_files_for_notice", notice_id=nid)

    header = _get_notice_meta(nid)
    data = {"header": {"notice_id": str(conn.execute("SELECT sam_notice_id FROM notices WHERE id=?", (nid,)).fetchone()[0] or nid), "title": header.get("title","")}, "sections": [], "lm_requirements": [], "submission": {}}
    if header.get("agency"): data["header"]["agency"] = header["agency"]

    clauses, forms, submission = [], [], {}

    for fid, name, url, cks, size in files:
        fname = name or (url.split("/")[-1] if url else f"file_{fid}")
        pages = _ensure_file_parse_and_index(nid, int(fid), fname, url)
        secs, lms = _extract_lm(pages, fname)
        data["sections"].extend(secs); data["lm_requirements"].extend(lms)
        clauses.extend(_extract_clauses(pages, fname))
        forms.extend(_extract_forms(pages, fname))
        sub = _extract_submission(pages, fname)
        if sub and "due_datetime" in sub and not submission.get("due_datetime"): submission = sub

    if clauses: data["clauses"] = clauses
    if forms: data["deliverables_forms"] = forms
    if submission: data["submission"] = submission

    ok, errs = validate_rfpv1(data)
    if not ok: return {"ok": False, "errors": errs}
    res = save_rfp_json(nid, data)
    if not res.get("ok"): return {"ok": False, "errors": res.get("errors")}
    return {"ok": True, "cached": False, "data": data, "version_hash": res.get("version_hash")}

def start_rfp_parser_worker(notice_id: int):
    import streamlit as st
    def _run():
        st.session_state["rfp_parser_status"] = {"state":"running","notice_id": int(notice_id), "started_at": _now_iso()}
        try:
            res = parse_rfp_v1(int(notice_id))
            st.session_state["rfp_parser_status"] = {"state":"done","notice_id": int(notice_id), "result": res, "finished_at": _now_iso()}
        except Exception as ex:
            st.session_state["rfp_parser_status"] = {"state":"error","notice_id": int(notice_id), "error": str(ex), "finished_at": _now_iso()}
    th = threading.Thread(target=_run, daemon=True); th.start()

def _load_latest_rfp_json(nid: int) -> dict | None:
    conn = get_db()
    r = conn.execute("SELECT data_json FROM rfp_json WHERE notice_id=? ORDER BY id DESC LIMIT 1", (int(nid),)).fetchone()
    if not r: return None
    try: return json.loads(r[0])
    except Exception: return None
# ===== end RFP Parser Phase 2 =====





# ===== Amend Tracking Phase 3 =====
import difflib

def ensure_amend_tables():
    conn = get_db()
    # versions
    conn.execute("""CREATE TABLE IF NOT EXISTS notice_versions(
        id INTEGER PRIMARY KEY,
        notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
        fetched_at TEXT NOT NULL,
        version_hash TEXT NOT NULL,
        payload_json TEXT NOT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_notice_versions_notice ON notice_versions(notice_id)")
    # amendments
    conn.execute("""CREATE TABLE IF NOT EXISTS amendments(
        id INTEGER PRIMARY KEY,
        notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
        amend_number TEXT,
        posted_at TEXT,
        url TEXT,
        version_hash TEXT NOT NULL,
        summary TEXT
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_amendments_notice ON amendments(notice_id)")
    # watchers optional
    conn.execute("""CREATE TABLE IF NOT EXISTS watchers(
        id INTEGER PRIMARY KEY,
        notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
        user_id TEXT NOT NULL,
        notify_email TEXT,
        active INTEGER NOT NULL DEFAULT 1
    )""")
    # compliance_state column on notices
    cols = {r[1] for r in conn.execute("PRAGMA table_info(notices)")}
    if "compliance_state" not in cols:
        try:
            conn.execute("ALTER TABLE notices ADD COLUMN compliance_state TEXT DEFAULT 'Unreviewed'")
        except Exception:
            pass

ensure_amend_tables()

def _core_payload_for_hash(n: dict) -> dict:
    # Use stable subset plus file urls
    fields = ["sam_notice_id","notice_type","title","agency","naics","psc","set_aside","place_city","place_state","posted_at","due_at","status","url"]
    core = {k: n.get(k) for k in fields}
    atts = n.get("attachments") or []
    core["files"] = sorted([a.get("url") or a.get("href") or "" for a in atts])
    return core

def _payload_version_hash(core: dict) -> str:
    import hashlib, json
    s = json.dumps(core, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def record_notice_version(notice_id: int, n: dict):
    """
    Compute version hash from core fields and attachments list.
    If changed from the latest version, insert version row and create amendment, set compliance_state.
    """
    if not n or not isinstance(n, dict):
        return None
    ensure_amend_tables()
    conn = get_db()
    core = _core_payload_for_hash(n)
    vhash = _payload_version_hash(core)
    prev = conn.execute("SELECT version_hash, payload_json FROM notice_versions WHERE notice_id=? ORDER BY id DESC LIMIT 1", (int(notice_id),)).fetchone()
    if prev and prev[0] == vhash:
        return vhash  # no change
    # Insert version
    now = utc_now_iso()
    conn.execute("INSERT INTO notice_versions(notice_id, fetched_at, version_hash, payload_json) VALUES(?,?,?,?)",
                 (int(notice_id), now, vhash, json.dumps(core, ensure_ascii=False)))
    # Create amendment row
    amend_no = None
    posted = n.get("posted_at") or None
    url = n.get("url") or None
    summary = "Auto detected change"
    conn.execute("INSERT INTO amendments(notice_id, amend_number, posted_at, url, version_hash, summary) VALUES(?,?,?,?,?,?)",
                 (int(notice_id), amend_no, posted, url, vhash, summary))
    \n    try:\n        conn.execute("UPDATE notices SET compliance_state='Needs review' WHERE id = ?", (int(notice_id),))\n        conn.commit()\n    except Exception:\n        pass\n# Mark compliance
    try:
        conn.execute("UPDATE notices SET compliance_state='Needs review' WHERE id=?", (int(notice_id),))
    except Exception:
        pass
    return vhash

def _load_versions(notice_id: int):
    conn = get_db()
    rows = conn.execute("SELECT id, fetched_at, version_hash, payload_json FROM notice_versions WHERE notice_id=? ORDER BY id DESC LIMIT 2", (int(notice_id),)).fetchall()
    out = []
    for r in rows:
        try:
            out.append({"id": r[0], "fetched_at": r[1], "hash": r[2], "payload": json.loads(r[3])})
        except Exception:
            out.append({"id": r[0], "fetched_at": r[1], "hash": r[2], "payload": {}})
    return out

def _diff_fields(prev: dict, curr: dict):
    keys = ["title","agency","naics","psc","set_aside","posted_at","due_at","status","place_city","place_state","rfp_schema","rfp_parser","subfinder_paging","subfinder_filters","subfinder_sources","subfinder_outreach","rfqg_composer","rfqg_outreach","rfqg_intake","vendor_rfq_hooks"]
    changes = []
    for k in keys:
        if (prev or {}).get(k) != (curr or {}).get(k):
            changes.append({
                "field": k,
                "before": (prev or {}).get(k),
                "after": (curr or {}).get(k),
                "diff": "\n".join(difflib.unified_diff(
                    [str((prev or {}).get(k) or "")],
                    [str((curr or {}).get(k) or "")],
                    lineterm=""
                ))
            })
    return changes

def _diff_files(prev_files: list, curr_files: list):
    ps = set(prev_files or [])
    cs = set(curr_files or [])
    added = sorted(list(cs - ps))
    removed = sorted(list(ps - cs))
    unchanged = ps & cs
    return {"added": added, "removed": removed, "unchanged": sorted(list(unchanged))}

def get_amend_count(notice_id: int) -> int:
    conn = get_db()
    return int(conn.execute("SELECT COUNT(1) FROM amendments WHERE notice_id=?", (int(notice_id),)).fetchone()[0])

def render_diff_panel():
    import streamlit as st
    if not st.session_state.get("feature_flags", {}).get("amend_tracking"):
        return
    if not st.session_state.get("diff_tab_open") or not st.session_state.get("selected_notice_id"):
        return
    nid = int(st.session_state["selected_notice_id"])
    st.markdown("---")
    st.subheader("Amendments Diff")
    versions = _load_versions(nid)
    if len(versions) < 1:
        st.info("No versions yet for this notice.")
        return
    curr = versions[0]["payload"]
    prev = versions[1]["payload"] if len(versions) > 1 else {}
    # Field deltas
    field_changes = _diff_fields(prev, curr)
    st.write("Field changes:", field_changes or "No field changes.")
    # File deltas
    prev_files = (prev or {}).get("files") or []
    curr_files = (curr or {}).get("files") or []
    fd = _diff_files(prev_files, curr_files)
    st.write("Files added:", fd["added"] or "None")
    st.write("Files removed:", fd["removed"] or "None")
    # Mark reviewed placeholder
    if st.button("Mark reviewed"):
        # Placeholder: session-only clear
        reviewed = set(st.session_state.get("_amend_reviewed", []))
        reviewed.add(versions[0]["hash"])
        st.session_state["_amend_reviewed"] = list(reviewed)
        st.session_state["diff_tab_open"] = False
        st.success("Marked reviewed for this session.")
# ===== end Amend Tracking Phase 3 =====






import sys, uuid, json, time, traceback

# Structured logging
def _now_iso():
    return utc_now_iso() if 'utc_now_iso' in globals() else __import__('datetime').datetime.utcnow().isoformat(timespec="seconds") + "Z"

def log_event(level: str, message: str, **context):
    lvl = str(level).lower()
    evt = {
        "ts": _now_iso(),
        "level": lvl,
        "msg": message,
        "ctx": {k: ("***" if "secret" in k.lower() else v) for k, v in (context or {}).items()},
    }
    line = json.dumps(evt, ensure_ascii=False)
    try:
        print(line, file=sys.stderr)
    except Exception:
        pass
    return evt

def err_with_id(message: str, **context):
    eid = str(uuid.uuid4())
    evt = log_event("error", message, error_id=eid, **context)
    return {"error": message, "error_id": eid}

# Secrets access
def get_secret(section: str, key: str, default=None):
    try:
        import streamlit as st
        sec = st.secrets.get(section, None)
        if isinstance(sec, dict) and key in sec:
            return sec[key]
        # Fallback flat lookup
        return st.secrets.get(key, default)
    except Exception:
        return default

# Central API client factory with retry, cache, and circuit breaker
def create_api_client(base_url: str, api_key: str = None, timeout: int = 30, retries: int = 3, ttl: int = 900):
    import streamlit as st
    import requests
    base_url = str(base_url).rstrip("/")
    # Circuit breaker state in session to survive reruns
    if "_api_cb" not in st.session_state:
        st.session_state["_api_cb"] = {}
    cb = st.session_state["_api_cb"].setdefault(base_url, {"fails": 0, "until": 0})

    def _headers():
        h = {"Accept": "application/json"}
        if api_key:
            h["Authorization"] = f"Bearer {api_key}"
        return h

    def _circuit_open():
        return time.time() < cb.get("until", 0)

    def _trip_circuit():
        cb["fails"] = 3
        cb["until"] = time.time() + 60  # 60 seconds open

    def _reset_circuit():
        cb["fails"] = 0
        cb["until"] = 0

    # Cached GET helper local to this client
    @st.cache_data(ttl=ttl, show_spinner=False)
    def _cached_get(url: str, params_tuple: tuple, headers_tuple: tuple):
        try:
            resp = requests.get(url, params=dict(params_tuple), headers=dict(headers_tuple), timeout=timeout)
            resp.raise_for_status()
            try:
                return {"status": resp.status_code, "json": resp.json()}
            except Exception:
                return {"status": resp.status_code, "text": resp.text}
        except Exception as ex:
            # Do not expose secrets
            return {"error": str(ex)}

    def get(path: str, params: dict = None):
        url = f"{base_url}/{str(path).lstrip('/')}"
        if _circuit_open():
            return err_with_id("circuit_open", base_url=base_url)
        p = params or {}
        # Retry loop with exponential backoff
        last_err = None
        for attempt in range(max(1, int(retries))):
            res = _cached_get(url, tuple(sorted(p.items())), tuple(sorted(_headers().items())))
            if "error" not in res:
                _reset_circuit()
                return res
            last_err = res["error"]
            cb["fails"] += 1
            if cb["fails"] >= 3:
                _trip_circuit()
                break
            time.sleep(min(2 ** attempt, 8))
        return err_with_id("request_failed", base_url=base_url, path=path, err=last_err)

    def post(path: str, json_body: dict = None):
        # No cache on POST
        import requests
        url = f"{base_url}/{str(path).lstrip('/')}"
        if _circuit_open():
            return err_with_id("circuit_open", base_url=base_url)
        try:
            r = requests.post(url, json=json_body or {}, headers=_headers(), timeout=timeout)
            r.raise_for_status()
            try:
                _reset_circuit()
                return {"status": r.status_code, "json": r.json()}
            except Exception:
                _reset_circuit()
                return {"status": r.status_code, "text": r.text}
        except Exception as ex:
            cb["fails"] += 1
            if cb["fails"] >= 3:
                _trip_circuit()
            return err_with_id("request_failed", base_url=base_url, path=path, err=str(ex))

    return {"get": get, "post": post, "base_url": base_url, "timeout": timeout}

def _init_feature_flags_session():
    import streamlit as st
    defaults = {
        "sam_ingest_core": False,
        "sam_page_size": False,
        "pipeline_star": False,
        "rfp_analyzer_panel": False,
        "amend_tracking": False,
        "workspace_enabled": feature_flags.get("workspace_enabled", False) if 'feature_flags' in globals() else False,
        "rfp_schema": False,
        "deals_core": True,
        "rfp_parser": False}
    # Global mirror for backward compatibility
    try:
        ff = dict(feature_flags) if 'feature_flags' in globals() else {}
    except Exception:
        ff = {}
    for k, v in defaults.items():
        ff.setdefault(k, v)
    globals()["feature_flags"] = ff
    # Session copy
    if "feature_flags" not in st.session_state or not isinstance(st.session_state.get("feature_flags"), dict):
        st.session_state["feature_flags"] = {}
    for k, v in defaults.items():
        st.session_state["feature_flags"].setdefault(k, v)

def _bootstrap_phase0():
    # Ensure PRAGMAs, migrations, flags, and client factory are ready
    import streamlit as st
    try:
        conn = get_db()
        # Verify PRAGMAs
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA foreign_keys=ON")
        except Exception as ex:
            log_event("warn", "pragma_set_failed", err=str(ex))
        # Ensure migrations table exists
        conn.execute("""CREATE TABLE IF NOT EXISTS migrations(
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            applied_at TEXT NOT NULL
        )""")
        _init_feature_flags_session()
        # Expose api client factory in session
        st.session_state["api_client_factory"] = create_api_client
        st.session_state["boot_done"] = True
    except Exception as ex:
        log_event("error", "bootstrap_failed", err=str(ex), tb=traceback.format_exc())
        st.session_state["boot_done"] = False

# Run bootstrap very early, but after imports exist
try:
    _bootstrap_phase0()
except Exception as _ex:
    log_event("error", "bootstrap_call_failed", err=str(_ex))
# ===== end Phase 0 Bootstrap =====

# LEGACY_REMOVED :
# LEGACY_REMOVED     if not SAM_API_KEY:
# LEGACY_REMOVED         return pd.DataFrame(), {"ok": False, "reason": "missing_key", "detail": "SAM_API_KEY is empty."}
# LEGACY_REMOVED     base = "https://api.sam.gov/opportunities/v2/search"
# LEGACY_REMOVED     today = datetime.utcnow().date()
# LEGACY_REMOVED     min_due_date = today + timedelta(days=min_days)
# LEGACY_REMOVED     posted_from = _us_date(today - timedelta(days=posted_from_days))
# LEGACY_REMOVED     posted_to   = _us_date(today)
# LEGACY_REMOVED
# LEGACY_REMOVED     params = {
# LEGACY_REMOVED         "api_key": SAM_API_KEY,
# LEGACY_REMOVED         "limit": str(limit),
# LEGACY_REMOVED         "response": "json",
# LEGACY_REMOVED         "sort": "-publishedDate",
# LEGACY_REMOVED         "active": active,
# LEGACY_REMOVED         "postedFrom": posted_from,   # MM/dd/yyyy
# LEGACY_REMOVED         "postedTo": posted_to,       # MM/dd/yyyy
# LEGACY_REMOVED     }
# LEGACY_REMOVED     # Enforce only Solicitation + Combined when notice_types is blank
# LEGACY_REMOVED     if not notice_types:
# LEGACY_REMOVED         notice_types = "Combined Synopsis/Solicitation,Solicitation"
# LEGACY_REMOVED     params["noticeType"] = notice_types
# LEGACY_REMOVED
# LEGACY_REMOVED     if naics_list:   params["naics"] = ",".join([c for c in naics_list if c][:20])
# LEGACY_REMOVED     if keyword:      params["keywords"] = keyword
# LEGACY_REMOVED
# LEGACY_REMOVED     try:
# LEGACY_REMOVED         headers = {"X-Api-Key": SAM_API_KEY}
# LEGACY_REMOVED         r = requests.get(base, params=params, headers=headers, timeout=40)
# LEGACY_REMOVED         status = r.status_code
# LEGACY_REMOVED         raw_preview = (r.text or "")[:1000]
# LEGACY_REMOVED         try:
# LEGACY_REMOVED             data = r.json()
# LEGACY_REMOVED         except Exception:
# LEGACY_REMOVED             return pd.DataFrame(), {"ok": False, "reason": "bad_json", "status": status, "raw_preview": raw_preview, "detail": r.text[:800]}
# LEGACY_REMOVED         if status != 200:
# LEGACY_REMOVED             err_msg = ""
# LEGACY_REMOVED             if isinstance(data, dict):
# LEGACY_REMOVED                 err_msg = data.get("message") or (data.get("error") or {}).get("message") or ""
# LEGACY_REMOVED             return pd.DataFrame(), {"ok": False, "reason": "http_error", "status": status, "message": err_msg, "detail": data, "raw_preview": raw_preview}
# LEGACY_REMOVED         if isinstance(data, dict) and data.get("message"):
# LEGACY_REMOVED             return pd.DataFrame(), {"ok": False, "reason": "api_message", "status": status, "detail": data.get("message"), "raw_preview": raw_preview}
# LEGACY_REMOVED
# LEGACY_REMOVED         items = data.get("opportunitiesData", []) or []
# LEGACY_REMOVED         rows = []
# LEGACY_REMOVED         for opp in items:
# LEGACY_REMOVED             due_str = opp.get("responseDeadLine") or ""
# LEGACY_REMOVED             d = _parse_sam_date(due_str)
# LEGACY_REMOVED             d_dt = _coerce_dt(d)
# LEGACY_REMOVED             min_dt = _coerce_dt(min_due_date)
# LEGACY_REMOVED             if min_dt is None:
# LEGACY_REMOVED                 due_ok = True  # allow when min date unknown
# LEGACY_REMOVED             else:
# LEGACY_REMOVED                 due_ok = (d_dt is None) or (d_dt >= min_dt)
# LEGACY_REMOVED             if not due_ok: continue
# LEGACY_REMOVED             docs = opp.get("documents", []) or []
# LEGACY_REMOVED             rows.append({
# LEGACY_REMOVED                 "sam_notice_id": opp.get("noticeId"),
# LEGACY_REMOVED                 "title": opp.get("title"),
# LEGACY_REMOVED                 "agency": opp.get("organizationName"),
# LEGACY_REMOVED                 "naics": ",".join(opp.get("naicsCodes", [])),
# LEGACY_REMOVED                 "psc": ",".join(opp.get("productOrServiceCodes", [])) if opp.get("productOrServiceCodes") else "",
# LEGACY_REMOVED                 "place_of_performance": (opp.get("placeOfPerformance") or {}).get("city",""),
# LEGACY_REMOVED                 "response_due": due_str,
# LEGACY_REMOVED                 "posted": opp.get("publishedDate",""),
# LEGACY_REMOVED                 "type": opp.get("type",""),
# LEGACY_REMOVED                 "url": f"https://sam.gov/opp/{opp.get('noticeId')}/view",
# LEGACY_REMOVED                 "attachments_json": json.dumps([{"name":d.get("fileName"),"url":d.get("url")} for d in docs])
# LEGACY_REMOVED             })
# LEGACY_REMOVED         df = pd.DataFrame(rows)
# LEGACY_REMOVED         info = {"ok": True, "status": status, "count": len(df), "raw_preview": raw_preview,
# LEGACY_REMOVED                 "filters": {"naics": params.get("naics",""), "keyword": keyword or "",
# LEGACY_REMOVED                             "postedFrom": posted_from, "postedTo": posted_to,
# LEGACY_REMOVED                             "min_due_days": min_days, "noticeType": notice_types,
# LEGACY_REMOVED                             "active": active, "limit": limit}}
# LEGACY_REMOVED         if df.empty:
# LEGACY_REMOVED             info["hint"] = "Try min_days=0–1, add keyword, increase look-back, or clear noticeType."
# LEGACY_REMOVED         return df, info
# LEGACY_REMOVED     except requests.RequestException as e:
# LEGACY_REMOVED         return pd.DataFrame(), {"ok": False, "reason": "network", "detail": str(e)[:800]}
# LEGACY_REMOVED
# LEGACY_REMOVED
# LEGACY_REMOVED
# LEGACY_REMOVED
def _ensure_opportunity_columns():
    conn = get_db(); cur = conn.cursor()
    # Add columns if missing
    try: cur.execute("alter table opportunities add column status text default 'New'")
    except Exception: pass
    try: cur.execute("alter table opportunities add column assignee text")
    except Exception: pass
    try: cur.execute("alter table opportunities add column quick_note text")
    except Exception: pass
    conn.commit()

def _get_table_cols(name):
    conn = get_db(); cur = conn.cursor()
    cur.execute(f"pragma table_info({name})")
    return [r[1] for r in cur.fetchall()]

def _to_sqlite_value(v):
    # Normalize pandas/NumPy/complex types to Python primitives or None
    try:
        import numpy as np
        import pandas as pd
        if v is None:
            return None
        # Pandas NA
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        # Numpy scalars
        if isinstance(v, (np.generic,)):
            return v.item()
        # Lists/dicts -> JSON
        if isinstance(v, (list, dict)):
            return json.dumps(v)
        # Bytes -> decode
        if isinstance(v, (bytes, bytearray)):
            try:
                return v.decode("utf-8", "ignore")
            except Exception:
                return str(v)
        # Other types: cast to str for safety
        if not isinstance(v, (str, int, float)):
            return str(v)
        return v
    except Exception:
        # Fallback minimal handling
        if isinstance(v, (list, dict)):
            return json.dumps(v)
        return v

def save_opportunities(df, default_assignee=None):
    """Upsert into opportunities and handle legacy schemas gracefully."""
    if df is None or getattr(df, "empty", True):
        return 0, 0
    try:
        df = df.where(df.notnull(), None)
    except Exception:
        pass

    _ensure_opportunity_columns()
    cols = set(_get_table_cols("opportunities"))

    inserted = 0
    updated = 0
    conn = get_db(); cur = conn.cursor()
    for _, r in df.iterrows():
        nid = r.get("sam_notice_id")
        if not nid:
            continue
        cur.execute("select id from opportunities where sam_notice_id=?", (nid,))
        row = cur.fetchone()

        base_fields = {
            "sam_notice_id": nid,
            "title": r.get("title"),
            "agency": r.get("agency"),
            "naics": r.get("naics"),
            "psc": r.get("psc"),
            "place_of_performance": r.get("place_of_performance"),
            "response_due": r.get("response_due"),
            "posted": r.get("posted"),
            "type": r.get("type"),
            "url": r.get("url"),
            "attachments_json": r.get("attachments_json"),
        }
        # Sanitize all base fields
        for k, v in list(base_fields.items()):
            base_fields[k] = _to_sqlite_value(v)

        if row:
            cur.execute(
                """update opportunities set title=?, agency=?, naics=?, psc=?, place_of_performance=?,
                   response_due=?, posted=?, type=?, url=?, attachments_json=? where sam_notice_id=?""",
                (base_fields["title"], base_fields["agency"], base_fields["naics"], base_fields["psc"],
                 base_fields["place_of_performance"], base_fields["response_due"], base_fields["posted"],
                 base_fields["type"], base_fields["url"], base_fields["attachments_json"], base_fields["sam_notice_id"])
            )
            updated += 1
        else:
            insert_cols = ["sam_notice_id","title","agency","naics","psc","place_of_performance","response_due","posted","type","url","attachments_json"]
            insert_vals = [base_fields[c] for c in insert_cols]
            if "status" in cols:
                insert_cols.append("status"); insert_vals.append("New")
            if "assignee" in cols:
                insert_cols.append("assignee"); insert_vals.append(_to_sqlite_value(default_assignee or ""))
            if "quick_note" in cols:
                insert_cols.append("quick_note"); insert_vals.append("")
            placeholders = ",".join("?" for _ in insert_cols)
            cur.execute(f"insert into opportunities({','.join(insert_cols)}) values({placeholders})", insert_vals)
            inserted += 1

    conn.commit()
    return inserted, updated
# ---------- UI ----------
with st.sidebar:
    st.subheader("Configuration")
    company_name = st.text_input("Company name", value=get_setting("company_name", "ELA Management LLC"))
    home_loc = st.text_input("Primary location", value=get_setting("home_loc", "Houston, TX"))
    default_trade = st.text_input("Default trade", value=get_setting("default_trade", "Janitorial"))
    if st.button("Save configuration"):
        set_setting("company_name", company_name); set_setting("home_loc", home_loc); set_setting("default_trade", default_trade)
        st.success("Saved")

    st.subheader("API Key Status")
    def _ok(v): return "✔" if v else "✘"
    st.markdown(f"**OpenAI Key:** {_ok(bool(OPENAI_API_KEY))}")
    st.markdown(f"**Google Places Key:** {_ok(bool(GOOGLE_PLACES_KEY))}")
    st.markdown(f"**SAM.gov Key:** {_ok(bool(SAM_API_KEY))}")
    st.caption(f"OpenAI SDK: {_openai_version} • Model: {OPENAI_MODEL}")
    if st.button("Test model"):
        st.info(llm("You are a health check.", "Reply READY.", max_tokens=5))

    # Company identifiers (ELA Management LLC)
    st.subheader("Company identifiers")
    st.code("DUNS: 14-483-4790\nCAGE: 14ZP6\nUEI: U32LBVK3DDF7", language=None)

    if st.button("Test SAM key"):
        try:
            today_us = _us_date(datetime.utcnow().date())
            test_params = {"api_key": SAM_API_KEY, "limit": "1", "response": "json",
                           "postedFrom": today_us, "postedTo": today_us}
            headers = {"X-Api-Key": SAM_API_KEY}
            r = requests.get("https://api.sam.gov/opportunities/v2/search", params=test_params, headers=headers, timeout=20)
            st.write("HTTP", r.status_code)
            text_preview = (r.text or "")[:1000]
            try:
                jj = r.json()
                api_msg = ""
                if isinstance(jj, dict):
                    api_msg = jj.get("message") or (jj.get("error") or {}).get("message") or ""
                if api_msg:
                    st.error(f"API reported: {api_msg}"); st.code(text_preview)
                elif r.status_code == 200:
                    st.success("SAM key appears valid (200 with JSON)."); st.code(text_preview)
                else:
                    st.warning("Non-200 but JSON returned."); st.code(text_preview)
            except Exception as e:
                st.error(f"JSON parse error: {e}"); st.code(text_preview)
        except Exception as e:
            st.error(f"Request failed: {e}")

    if st.button("Test Google Places key"):
        vendors, info = google_places_search("janitorial small business", get_setting("home_loc","Houston, TX"), 30000)
        st.write("Places diagnostics:", info); st.write("Sample results:", vendors[:3])

    st.subheader("Watch list NAICS")
    conn = get_db()
    df_saved = pd.read_sql_query("select code from naics_watch order by code", conn)
    saved_codes = df_saved["code"].tolist()
    naics_options = sorted(set(saved_codes + NAICS_SEEDS))
    st.multiselect("Choose or type NAICS codes then Save", options=naics_options,
                   default=saved_codes if saved_codes else sorted(set(NAICS_SEEDS[:20])), key="naics_watch")
    new_code = st.text_input("Add a single NAICS code")
    col_n1, col_n2 = st.columns(2)
    with col_n1:
        if st.button("Add code"):
            val = (new_code or "").strip()
            if val:
                conn.execute("insert or ignore into naics_watch(code,label) values(?,?)", (val, val)); conn.commit(); st.success(f"Added {val}")
    with col_n2:
        if st.button("Clear all saved codes"):
            conn.execute("delete from naics_watch"); conn.commit(); st.success("Cleared saved codes")
    if st.button("Save NAICS list"):
        keep = sorted(set([c.strip() for c in st.session_state.naics_watch if str(c).strip()]))
        cur = conn.cursor(); cur.execute("delete from naics_watch")
        for c in keep: cur.execute("insert into naics_watch(code,label) values(?,?)", (c, c))
        conn.commit(); st.success("Saved NAICS watch list")

    naics_csv = st.file_uploader("Import NAICS CSV (column 'code')", type=["csv"])
    if naics_csv and st.button("Import NAICS from CSV"):
        df_in = pd.read_csv(naics_csv)
        if "code" in df_in.columns:
            cur = conn.cursor()
            for c in df_in["code"].astype(str).fillna("").str.strip():
                if c: cur.execute("insert or ignore into naics_watch(code,label) values(?,?)", (c, c))
            conn.commit(); st.success("Imported")
        else:
            st.info("CSV must have a column named code")

    st.subheader("Goals")
    g = pd.read_sql_query("select * from goals limit 1", conn)
    if g.empty:
        conn.execute("insert into goals(year,bids_target,revenue_target,bids_submitted,revenue_won) values(?,?,?,?,?)",
                     (datetime.now().year, 156, 600000, 1, 0)); conn.commit()
        g = pd.read_sql_query("select * from goals limit 1", conn)
    row = g.iloc[0]; goal_id = int(row["id"])
    with st.form("goals_form", clear_on_submit=False):
        col1, col2 = st.columns(2)
        with col1:
            bids_target = st.number_input("Bids target", min_value=0, step=1, value=int(row["bids_target"]))
            bids_submitted = st.number_input("Bids submitted", min_value=0, step=1, value=int(row["bids_submitted"]))
        with col2:
            revenue_target = st.number_input("Revenue target", min_value=0.0, step=1000.0, value=float(row["revenue_target"]))
            revenue_won = st.number_input("Revenue won", min_value=0.0, step=1000.0, value=float(row["revenue_won"]))
        if st.form_submit_button("Save goals"):
            conn.execute("update goals set bids_target=?, revenue_target=?, bids_submitted=?, revenue_won=? where id=?",
                         (int(bids_target), float(revenue_target), int(bids_submitted), float(revenue_won), goal_id))
            conn.commit(); st.success("Goals updated")
    colq1, colq2 = st.columns(2)
    with colq1:
        if st.button("Log new bid"):
            conn.execute("update goals set bids_submitted = bids_submitted + 1 where id=?", (goal_id,)); conn.commit(); st.success("Bid logged")
    with colq2:
        add_amt = st.number_input("Add award amount", min_value=0.0, step=1000.0, value=0.0, key="award_add_amt")
        if st.button("Log award"):
            if add_amt > 0:
                conn.execute("update goals set revenue_won = revenue_won + ? where id=?", (float(add_amt), goal_id)); conn.commit()
                st.success(f"Award logged for ${add_amt:,.0f}")
            else:
                st.info("Enter a positive amount")
    g = pd.read_sql_query("select * from goals limit 1", conn); row = g.iloc[0]
    st.metric("Bids target", int(row["bids_target"]))
    st.metric("Bids submitted", int(row["bids_submitted"]))
    st.metric("Revenue target", f"${float(row['revenue_target']):,.0f}")
    st.metric("Revenue won", f"${float(row['revenue_won']):,.0f}")

def render_rfp_analyzer():
    try:
        st.subheader("RFP Analyzer")
        st.caption("Upload RFP package and chat with memory. Use quick actions or ask your own questions.")

        conn = get_db()

        # Sessions like Chat Assistant
        sessions = pd.read_sql_query("select id, title, created_at from rfp_sessions order by created_at desc", conn)
        session_titles = ["➤ New RFP thread"] + [f"{r['id']}: {r['title'] or '(untitled)'}" for _, r in sessions.iterrows()]
        pick = st.selectbox("RFP session", options=session_titles, index=0)

        if pick == "➤ New RFP thread":
            default_title = f"RFP {datetime.now().strftime('%b %d %I:%M %p')}"
            new_title = st.text_input("Thread title", value=default_title)
            if st.button("Start RFP thread"):
                conn.execute("insert into rfp_sessions(title) values(?)", (new_title,))
                conn.commit()
                st.rerun()
            return

        if not pick:
            st.info("Select a chat session to continue.")
            pass

        session_id = parse_pick_id(pick)
        if session_id is None:
            st.info("Select a valid session to continue.")
            pass
        cur_title = sessions[sessions["id"] == session_id]["title"].iloc[0]
        st.caption(f"RFP thread #{session_id}  {cur_title}")

        # File uploader with persistence
        uploads = st.file_uploader("Upload RFP files PDF DOCX TXT", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key=f"rfp_up_{session_id}")
        if uploads and st.button("Add files to RFP thread"):
            added = 0
            for up in uploads:
                text = read_doc(up)[:800_000]
                conn.execute("""insert into rfp_files(session_id, filename, mimetype, content_text)
                                values(?,?,?,?)""", (session_id, up.name, getattr(up, "type", ""), text))
                added += 1
            conn.commit()
            st.success(f"Added {added} file(s) to this thread.")
            st.rerun()

        files_df = pd.read_sql_query(
            "select id, filename, length(content_text) as chars, uploaded_at from rfp_files where session_id=? order by id desc",
            conn, params=(session_id,)
        )
        if files_df.empty:
            st.caption("No files yet.")
        else:
            st.caption("Attached files")
            st.dataframe(files_df.rename(columns={"chars":"chars_of_text"}), use_container_width=True)
            del_id = st.number_input("Delete attachment by ID", min_value=0, step=1, value=0, key=f"rfp_del_{session_id}")
            if st.button("Delete selected RFP file"):
                if del_id > 0:
                    conn.execute("delete from rfp_files where id=?", (int(del_id),))
                    conn.commit()
                    st.success(f"Deleted file id {del_id}.")
                    st.rerun()

        # Previous messages
        hist = pd.read_sql_query(
            "select role, content, created_at from rfp_messages where session_id=? order by id asc",
            conn, params=(session_id,)
        )
        if hist.empty:
            st.info("No messages yet. Use the quick actions or ask a question below.")
        else:
            for _, row in hist.iterrows():
                if row["role"] == "user":
                    st.chat_message("user").markdown(row["content"])
                elif row["role"] == "assistant":
                    st.chat_message("assistant").markdown(row["content"])
                else:
                    st.caption(f"System updated at {row['created_at']}")

        # Helper to build doc context
        def _rfp_context_for(question_text: str):
            rows = pd.read_sql_query(
                "select filename, content_text from rfp_files where session_id=? and ifnull(content_text,'')<>''",
                conn, params=(session_id,)
            )
            if rows.empty:
                return ""
            chunks, labels = [], []
            for _, r in rows.iterrows():
                cs = chunk_text(r["content_text"], max_chars=1200, overlap=200)
                chunks.extend(cs)
                labels.extend([r["filename"]]*len(cs))
            vec, X = embed_texts(chunks)
            top = search_chunks(question_text, vec, X, chunks, k=min(8, len(chunks)))
            parts, used = [], set()
            for sn in top:
                try:
                    idx = chunks.index(sn)
                    fname = labels[idx]
                except Exception:
                    idx, fname = -1, "attachment"
                key = (fname, sn[:60])
                if key in used:
                    continue
                used.add(key)
                parts.append(f"\n--- {fname} ---\n{sn.strip()}\n")
            return "Attached document snippets most relevant first:\n" + "\n".join(parts[:16]) if parts else ""

        # Quick action buttons
        colA, colB, colC, colD = st.columns(4)
        qa = None
        with colA:
            if st.button("Compliance matrix"):
                qa = "Produce a compliance matrix that lists every shall must or required item and where it appears."
        with colB:
            if st.button("Evaluation factors"):
                qa = "Summarize the evaluation factors and their relative importance and scoring approach."
        with colC:
            if st.button("Submission checklist"):
                qa = "Create a submission checklist with page limits fonts file naming addresses and exact submission method with dates and times quoted."
        with colD:
            if st.button("Grade my draft"):
                qa = "Grade the following draft against the RFP requirements and give a fix list. If draft text is empty just outline what a strong section must contain."

        # Free form follow up like chat
        user_q = st.chat_input("Ask a question about the RFP or use a quick action above")
        pending_prompt = qa or user_q

        if pending_prompt:
            # Save user turn
            conn.execute("insert into rfp_messages(session_id, role, content) values(?,?,?)",
                         (session_id, "user", pending_prompt))
            conn.commit()

            # Build system and context using company snapshot and RFP snippets
            try:
                context_snap = build_context(max_rows=6)
            except NameError:
                context_snap = ""
            doc_snips = _rfp_context_for(pending_prompt)

            sys_text = f"""You are a federal contracting assistant. Keep answers concise and actionable.
    Context snapshot:
    {context_snap}
    {doc_snips if doc_snips else ""}"""

            # Compose rolling window like Chat Assistant
            msgs_db = pd.read_sql_query(
                "select role, content from rfp_messages where session_id=? order by id asc",
                conn, params=(session_id,)
            ).to_dict(orient="records")

            # Keep up to 12 user turns
            pruned, user_turns = [], 0
            for m in msgs_db[::-1]:
                if m["role"] == "assistant":
                    pruned.append(m)
                    continue
                if m["role"] == "user":
                    if user_turns < 12:
                        pruned.append(m)
                        user_turns += 1
                    continue
            msgs_window = list(reversed(pruned))
            messages = [{"role": "system", "content": sys_text}] + msgs_window

            assistant_out = llm_messages(messages, temp=0.2, max_tokens=1200)
            conn.execute("insert into rfp_messages(session_id, role, content) values(?,?,?)",
                         (session_id, "assistant", assistant_out))
            conn.commit()

            st.chat_message("user").markdown(pending_prompt)
            st.chat_message("assistant").markdown(assistant_out)
    except Exception as e:
        st.warning(f"RFP Analyzer error: {e}")

def render_proposal_builder():
    try:
        st.subheader("Proposal Builder")
        st.caption("Draft federal proposal sections using your RFP thread and files. Select past performance. Export to DOCX with guardrails.")

        conn = get_db()
        sessions = pd.read_sql_query("select id, title, created_at from rfp_sessions order by created_at desc", conn)
        if sessions.empty:
            st.warning("Create an RFP thread in RFP Analyzer first.")
            return

        opts = [f"{r['id']}: {r['title'] or '(untitled)'}" for _, r in sessions.iterrows()]
        pick = st.selectbox("Select RFP thread", options=opts, index=0, key="pb_session_pick")
        session_id = parse_pick_id(pick)
        if session_id is None:
            st.info("Select a valid session to continue.")


        st.markdown("**Attach past performance to include**")
        df_pp = get_past_performance_df()
        selected_pp_ids = []
        if not df_pp.empty:
            df_pp["pick"] = False
            edited_pp = st.data_editor(df_pp[["id","title","agency","naics","period","value","role","highlights","pick"]], use_container_width=True, num_rows="fixed", key="pp_pick_grid")
            selected_pp_ids = [int(x) for x in edited_pp[edited_pp["pick"]==True]["id"].tolist()]
        else:
            st.caption("No past performance records yet. Add some in Past Performance tab.")

        col1, col2, col3 = st.columns(3)
        with col1:
            want_exec = st.checkbox("Executive Summary", True)
            want_tech = st.checkbox("Technical Approach", True)
        with col2:
            want_mgmt = st.checkbox("Management & Staffing Plan", True)
            want_past = st.checkbox("Past Performance", True)
        with col3:
            want_price = st.checkbox("Pricing Assumptions/Notes", True)
            want_comp = st.checkbox("Compliance Narrative", True)

        actions = {
            "Executive Summary": want_exec,
            "Technical Approach": want_tech,
            "Management & Staffing Plan": want_mgmt,
            "Past Performance": want_past,
            "Pricing Assumptions/Notes": want_price,
            "Compliance Narrative": want_comp,
        }
        # Canonical section order used by export and display
        order = [
            "Executive Summary",
            "Technical Approach",
            "Management & Staffing Plan",
            "Past Performance",
            "Pricing Assumptions/Notes",
            "Compliance Narrative",
        ]

        # Section-specific prompts always in scope for this builder
        section_prompts = {
            "Executive Summary": "Write an executive summary that aligns our capabilities to the requirement. Emphasize value, risk mitigation, and rapid mobilization.",
            "Technical Approach": "Describe a compliant, phase-oriented technical approach keyed to the PWS/SOW, referencing SLAs and QC steps.",
            "Management & Staffing Plan": "Provide management structure, roles, key personnel, surge plan, and communication/QA practices.",
            "Past Performance": "Summarize the selected past performance items, mapping relevance to scope, scale, and outcomes.",
            "Pricing Assumptions/Notes": "List pricing basis, inclusions/exclusions, assumptions, and any risk-based contingencies. No dollar totals.",
            "Compliance Narrative": "Map our response to Section L&M: where requirements are addressed, page limits, fonts, submission method."
        }



        drafts_df = pd.read_sql_query(
            "select id, section, content, updated_at from proposal_drafts where session_id=? order by section",
            conn, params=(session_id,)
        )

        colA, colB = st.columns([1,1])
        with colA:
            regenerate = st.button("Generate selected sections")
        if regenerate and not any(actions.values()):
            st.warning("Pick at least one section above, then click Generate selected sections.")
            regenerate = False

        with colB:
            save_all = st.button("Save edited drafts")
            export_md = st.button("Assemble full proposal (Markdown)")
            export_docx = st.button("Export Proposal DOCX (guardrails)")
        # === Generate selected sections ===
        if regenerate:
            # Diagnostics: show which sections are selected
            try:
                _on = [k for k,v in actions.items() if v]
                st.info(f"Generating sections: {', '.join(_on) if _on else 'none'}")
            except Exception:
                pass

            def _gen_with_fallback(system_text, user_prompt):
                # Immediate template if OpenAI client is not configured
                try:
                    from builtins import globals as _g
                except Exception:
                    _g = globals
                if not _g().get('client', None):
                    heading = (user_prompt.split('\n', 1)[0].strip() or 'Section')
                    tmpl = [
                        f'## {heading}',
                        '• Approach overview: Describe how we will fulfill the PWS tasks with measurable SLAs.',
                        '• Roles and responsibilities: Identify key staff and escalation paths.',
                        '• Quality assurance: Inspections, KPIs, and corrective actions.',
                        '• Risk mitigation: Top risks and mitigations tied to timeline.',
                        '• Compliance notes: Where Section L & M items are satisfied.',
                    ]
                    return '\n'.join(tmpl)
                try:
                    _out = llm(system_text, user_prompt, temp=0.3, max_tokens=1200)
                except Exception as _e:
                    _out = f'LLM error: {type(_e).__name__}: {_e}'
                bad = (not isinstance(_out, str)) or (_out.strip() == '') or ('Set OPENAI_API_KEY' in _out) or _out.startswith('LLM error')
                if bad:
                    heading = (user_prompt.split('\n', 1)[0].strip() or 'Section')
                    tmpl = [
                        f'## {heading}',
                        '• Approach overview: Describe how we will fulfill the PWS tasks with measurable SLAs.',
                        '• Roles and responsibilities: Identify key staff and escalation paths.',
                        '• Quality assurance: Inspections, KPIs, and corrective actions.',
                        '• Risk mitigation: Top risks and mitigations tied to timeline.',
                        '• Compliance notes: Where Section L & M items are satisfied.',
                    ]
                    return '\n'.join(tmpl)
                return _out

            # Helper: pull top snippets from attached RFP files for this session
            def _pb_doc_snips(question_text: str):
                rows = pd.read_sql_query(
                    "select filename, content_text from rfp_files where session_id=? and ifnull(content_text,'')<>''",
                    conn, params=(session_id,)
                )
                if rows.empty:
                    return ""
                chunks, labels = [], []
                for _, r in rows.iterrows():
                    cs = chunk_text(r["content_text"], max_chars=1200, overlap=200)
                    chunks.extend(cs); labels.extend([r["filename"]]*len(cs))
                vec, X = embed_texts(chunks)
                top = search_chunks(question_text, vec, X, chunks, k=min(10, len(chunks)))
                parts, used = [], set()
                for sn in top:
                    try:
                        idx = chunks.index(sn); fname = labels[idx]
                    except Exception:
                        fname = "attachment"
                    key = (fname, sn[:60])
                    if key in used: continue
                    used.add(key)
                    parts.append(f"\n--- {fname} ---\\n{sn.strip()}\\n")
                return "Attached RFP snippets (most relevant first):\n" + "\\n".join(parts[:16]) if parts else ""

            # Pull past performance selections text if any
            pp_text = ""
            if selected_pp_ids:
                qmarks = ",".join(["?"]*len(selected_pp_ids))
                df_sel = pd.read_sql_query(f"select title, agency, naics, period, value, role, location, highlights from past_performance where id in ({qmarks})", conn, params=tuple(selected_pp_ids))
                lines = []
                for _, r in df_sel.iterrows():
                    lines.append(f"- {r['title']} — {r['agency']} ({r['role']}); NAICS {r['naics']}; Period {r['period']}; Value ${float(r['value'] or 0):,.0f}. Highlights: {r['highlights']}")
                pp_text = "\n".join(lines)

            # Build common system context
            try:
                context_snap = build_context(max_rows=6)
            except Exception:
                context_snap = ""
            for sec, on in actions.items():
                if not on:
                    continue
                # Build doc context keyed to the section
                doc_snips = _pb_doc_snips(sec)
                system_text = "\\n\\n".join(filter(None, [
                    "You are a federal proposal writer. Use clear headings and concise bullets. Be compliant and specific.",
                    f"Company snapshot:\\n{context_snap}" if context_snap else "",
                    doc_snips,
                    f"Past Performance selections:\\n{pp_text}" if (pp_text and sec in ('Executive Summary','Past Performance','Technical Approach','Management & Staffing Plan')) else ""
                ]))
                user_prompt = section_prompts.get(sec, f"Draft the section titled: {sec}.")

                out = _gen_with_fallback(system_text, user_prompt)

                # Upsert into proposal_drafts
                cur = conn.cursor()
                cur.execute("select id from proposal_drafts where session_id=? and section=?", (session_id, sec))
                row = cur.fetchone()
                if row:
                    cur.execute("update proposal_drafts set content=?, updated_at=current_timestamp where id=?", (out, int(row[0])))
                else:
                    cur.execute("insert into proposal_drafts(session_id, section, content) values(?,?,?)", (session_id, sec, out))
                conn.commit()
            try:
                st.success("Generated drafts. Scroll down to 'Drafts' to review and edit.")
            except Exception:
                pass
            st.rerun()


        # Compliance validation settings
        st.markdown("#### Compliance validation settings")
        colv1, colv2, colv3 = st.columns(3)
        with colv1:
            pb_page_limit = st.number_input("Page limit (estimated)", min_value=0, step=1, value=0)
            pb_font = st.text_input("Required font", value="Times New Roman")
        with colv2:
            pb_font_size = st.number_input("Required size (pt)", min_value=8, max_value=14, step=1, value=12)
            pb_margins = st.number_input("Margins (inches)", min_value=0.5, max_value=1.5, value=1.0, step=0.25)
        with colv3:
            pb_line_spacing = st.number_input("Line spacing", min_value=1.0, max_value=2.0, value=1.0, step=0.1)
            pb_file_pat = st.text_input("Filename pattern", value="{company}_{solicitation}_{section}_{date}")

        # Assemble full proposal in Markdown
        if export_md:
            parts = []
            for sec in order:
                if sec not in actions or not actions[sec]:
                    continue
                cur = conn.cursor()
                cur.execute("select content from proposal_drafts where session_id=? and section=?", (session_id, sec))
                row = cur.fetchone()
                if row and row[0]:
                    parts.append(f"# {sec}\n\n{row[0].strip()}\n")
            assembled = "\n\n---\n\n".join(parts) if parts else "# Proposal\n(No sections saved yet.)"
            st.markdown("#### Assembled Proposal (Markdown preview)")
            st.code(assembled, language="markdown")
            st.download_button("Download proposal.md", data=assembled.encode("utf-8"),
                               file_name="proposal.md", mime="text/markdown")

        # Export DOCX with guardrails
        if export_docx:
        try:
            audit('export', st.session_state.get('user_id'), 'notice', str(st.session_state.get('selected_notice_id')))
        except Exception:
            pass
            from docx import Document
            from docx.shared import Inches, Pt
            from docx.oxml.ns import qn

            parts = []
            for sec in order:
                cur = conn.cursor()
                cur.execute("select content from proposal_drafts where session_id=? and section=?", (session_id, sec))
                row = cur.fetchone()
                if row and row[0]:
                    parts.append((sec, row[0].strip()))
            full_text = "\n\n".join(f"{sec}\n\n{txt}" for sec, txt in parts)

            issues, _ = _validate_text_for_guardrails(
                full_text,
                page_limit=int(pb_page_limit) if pb_page_limit else None,
                require_font=pb_font or None,
                require_size_pt=int(pb_font_size) if pb_font_size else None,
                margins_in=float(pb_margins) if pb_margins else None,
                line_spacing=float(pb_line_spacing) if pb_line_spacing else None,
                filename_pattern=pb_file_pat or None
            )
            if issues:
                st.warning("Proceeding with export:")
                for x in issues:
                    st.markdown(f"- {x}")


            doc = Document()
            for section in doc.sections:
                section.top_margin = Inches(pb_margins or 1)
                section.bottom_margin = Inches(pb_margins or 1)
                section.left_margin = Inches(pb_margins or 1)
                section.right_margin = Inches(pb_margins or 1)

            style = doc.styles["Normal"]
            req_font = pb_font or "Times New Roman"
            style.font.name = req_font
            style._element.rPr.rFonts.set(qn("w:eastAsia"), req_font)
            style.font.size = Pt(pb_font_size or 12)

            for sec, txt in parts:
                doc.add_heading(sec, level=1)
                for para in txt.split("\n\n"):
                    doc.add_paragraph(_strip_markdown_to_plain(para))

            bio = io.BytesIO()
            doc.save(bio)
            bio.seek(0)

            company = get_setting("company_name","ELA Management LLC")
            today = datetime.now().strftime("%Y%m%d")
            safe_title = (sessions[sessions["id"] == session_id]["title"].iloc[0] if not sessions.empty else "RFP").replace(" ", "_")
            fname = (pb_file_pat or "{company}_{solicitation}_{date}").format(
                company=company.replace(" ", "_"),
                solicitation=safe_title,
                section="FullProposal",
                date=today
            )
            if not fname.lower().endswith(".docx"):
                fname += ".docx"

            st.download_button("Download Proposal DOCX", data=bio.getvalue(), file_name=fname,
                               mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")

        st.markdown("### Drafts")
        order = ["Executive Summary","Technical Approach","Management & Staffing Plan","Past Performance","Pricing Assumptions/Notes","Compliance Narrative"]
        # Refresh drafts after generation so new content appears immediately
        drafts_df = pd.read_sql_query(
            "select id, section, content, updated_at from proposal_drafts where session_id=? order by section",
            conn, params=(session_id,)
        )
        existing = {r["section"]: r for _, r in drafts_df.iterrows()}
        edited_blocks = {}
        for sec in order:
            if not actions.get(sec, False):
                continue
            st.markdown(f"**{sec}**")
            txt = existing.get(sec, {}).get("content", "")
            edited_blocks[sec] = st.text_area(f"Edit {sec}", value=txt, height=240, key=f"pb_{sec}")

        if save_all and edited_blocks:
            cur = conn.cursor()
            for sec, content in edited_blocks.items():
                cur.execute("select id from proposal_drafts where session_id=? and section=?", (session_id, sec))
                row = cur.fetchone()
                if row:
                    cur.execute("update proposal_drafts set content=?, updated_at=current_timestamp where id=?", (content, int(row[0])))
                else:
                    cur.execute("insert into proposal_drafts(session_id, section, content) values(?,?,?)", (session_id, sec, content))
            conn.commit()
            st.success("Drafts saved.")


    except Exception as e:
        st.error(f"Proposal Builder error: {e}")

# === End new features ===


# ---- Attach feature tabs now that functions are defined ----
try:
    with legacy_tabs[5]:
        render_rfp_analyzer()
except Exception as e:
    st.caption(f"[RFP Analyzer tab note: {e}]")

try:
    with legacy_tabs[12]:
        render_proposal_builder()
except Exception as e:
    st.caption(f"[Proposal Builder tab note: {e}]")

with conn:
    conn.execute("""
    create table if not exists pricing_benchmarks(
        id integer primary key,
        award_id text,
        agency text,
        recipient text,
        start text,
        end text,
        amount real,
        term_months real,
        monthly_spend real,
        sqft real,
        freq_per_week integer,
        facility_type text,
        scope_tags text,
        dollars_per_sqft_year real,
        cpi_factor real,
        amount_adj real,
        notes text,
        source text default 'USAspending',
        created_at text default current_timestamp
    )
    """)



# --- SAFE REDEFINITION: guardrails validator ---



def md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                     margins_in: float = 1.0, logo_bytes: bytes = None, logo_width_in: float = 1.5) -> bytes:
    """
    Backward compatible wrapper that supports an optional logo header.
    Signature matches earlier calls that used logo_bytes.
    """
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import re as _re, io

    # Build a fresh document so we can place a logo at the top if provided
    doc = Document()

    # Margins
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass

    # Base style
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass

    # Optional logo header
    if logo_bytes:
        p_center = doc.add_paragraph()
        p_center.paragraph_format.alignment = 1  # center
        run = p_center.add_run()
        try:
            run.add_picture(io.BytesIO(logo_bytes), width=Inches(logo_width_in))
        except Exception:
            pass

    # Optional document title
    if title:
        h = doc.add_heading(title, level=1)
        try:
            h.style = doc.styles["Heading 1"]
        except Exception:
            pass

    # Reuse the simple markdown-ish renderer by saving into a temp docx and appending
    # For simplicity, we reimplement the same minimal renderer here:
    lines = (md_text or "").splitlines()
    bullet_buf = []
    num_buf = []

    def flush_bullets():
        nonlocal bullet_buf
        for item in bullet_buf:
            p = doc.add_paragraph(item)
            try:
                p.style = doc.styles["List Bullet"]
            except Exception:
                pass
        bullet_buf = []

    def flush_numbers():
        nonlocal num_buf
        for item in num_buf:
            p = doc.add_paragraph(item)
            try:
                p.style = doc.styles["List Number"]
            except Exception:
                pass
        num_buf = []

    for raw in lines:
        line = raw.rstrip()

        if not line.strip():
            flush_bullets(); flush_numbers()
            doc.add_paragraph("")
            continue

        if line.startswith("### "):
            flush_bullets(); flush_numbers()
            doc.add_heading(line[4:].strip(), level=3)
            continue
        if line.startswith("## "):
            flush_bullets(); flush_numbers()
            doc.add_heading(line[3:].strip(), level=2)
            continue
        if line.startswith("# "):
            flush_bullets(); flush_numbers()
            doc.add_heading(line[2:].strip(), level=1)
            continue

        if _re.match(r"^(\-|\*|•)\s+", line):
            flush_numbers()
            bullet_buf.append(_re.sub(r"^(\-|\*|•)\s+", "", line, count=1))
            continue

        if _re.match(r"^\d+\.\s+", line):
            flush_bullets()
            num_buf.append(_re.sub(r"^\d+\.\s+", "", line, count=1))
            continue

        flush_bullets(); flush_numbers()
        doc.add_paragraph(line)

    flush_bullets(); flush_numbers()

    out = io.BytesIO()
    doc.save(out)
    out.seek(0)
    return out.getvalue()



with legacy_tabs[8]:
    st.subheader("Proposal export and drafts")




# === Deals (CRM pipeline) helpers ===
DEAL_STAGES = [
    "No Contact Made",
    "CO Contacted",
    "Quote",
    "Multiple Quotes",
    "Proposal Started",
    "Proposal Finished",
    "Proposal Submitted",
    "Awarded",
    "Proposal Lost",
]

def ensure_deals_table(conn):
    cur = conn.cursor()
    cur.execute("""
        create table if not exists deals (
            id integer primary key autoincrement,
            title text not null,
            stage text not null default 'No Contact Made',
            owner text,
            amount numeric,
            notes text,
            agency text,
            due_date text,
            created_at text default (datetime('now')),
            updated_at text default (datetime('now'))
        )
    """)
    cur.execute("create index if not exists deals_stage_idx on deals(stage)")
    cur.execute("create index if not exists deals_updated_idx on deals(updated_at)")
    conn.commit()

def list_deals(stage: str | None = None, q: str | None = None):
    conn = get_db()
    ensure_deals_table(conn)
    cur = conn.cursor()
    sql = "select id, title, stage, owner, amount, notes, agency, due_date, created_at, updated_at from deals"
    params = []
    where = []
    if stage and stage != "All":
        where.append("stage = ?")
        params.append(stage)
    if q:
        where.append("(title like ? or notes like ? or agency like ?)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]
    if where:
        sql += " where " + " and ".join(where)
    sql += " order by updated_at desc, id desc"
    rows = cur.execute(sql, params).fetchall()
    cols = ["id","title","stage","owner","amount","notes","agency","due_date","created_at","updated_at"]
    import pandas as pd
    return pd.DataFrame(rows, columns=cols)

def create_deal(title: str, stage: str, owner: str | None, amount: float | None, notes: str | None, agency: str | None, due_date: str | None):
    conn = get_db()
    ensure_deals_table(conn)
    cur = conn.cursor()
    cur.execute("""
        insert into deals (title, stage, owner, amount, notes, agency, due_date)
        values (?,?,?,?,?,?,?)
    """, (title, stage, owner, amount, notes, agency, due_date))
    conn.commit()
    return cur.lastrowid


def add_deal(title: str, stage: str = "No Contact Made", owner: str | None = None,
             amount: float | None = None, notes: str | None = None,
             agency: str | None = None, due_date: str | None = None,
             source: str | None = None, url: str | None = None):
    """Backward-compatible wrapper used by SAM Watch selection.
    Ignores source/url for now but keeps signature stable."""
    return create_deal(title, stage, owner, amount, notes, agency, due_date)
def update_deal(id_: int, **fields):
    if not fields: return False
    conn = get_db()
    ensure_deals_table(conn)
    cur = conn.cursor()
    sets = []
    vals = []
    for k,v in fields.items():
        if k not in {"title","stage","owner","amount","notes","agency","due_date"}:
            continue
        sets.append(f"{k} = ?")
        vals.append(v)
    if not sets: return False
    sets.append("updated_at = datetime('now')")
    sql = "update deals set " + ", ".join(sets) + " where id = ?"
    vals.append(id_)
    cur.execute(sql, vals)
    conn.commit()
    return cur.rowcount > 0


# === DEALS PHASE 3: Activities + Calendar ===
def ensure_deal_activities_schema(conn):
    cur = conn.cursor()
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS deal_activities(
            id INTEGER PRIMARY KEY,
            deal_id INTEGER NOT NULL REFERENCES deals(id) ON DELETE CASCADE,
            type TEXT NOT NULL,
            title TEXT,
            body TEXT,
            due_at TEXT,
            completed_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            created_by TEXT
        );""")
        cur.execute("""CREATE INDEX IF NOT EXISTS idx_dact_deal ON deal_activities(deal_id);""" )
        cur.execute("""CREATE INDEX IF NOT EXISTS idx_dact_due ON deal_activities(due_at);""" )
    except Exception:
        pass
    conn.commit()

def list_activities(deal_id=None, include_completed=True, q=""):
    conn = get_db()
    ensure_deal_activities_schema(conn)
    sql = "SELECT id, deal_id, type, COALESCE(title,'') as title, COALESCE(body,'') as body, due_at, completed_at, created_at, created_by FROM deal_activities WHERE 1=1"
    params = []
    if deal_id:
        sql += " AND deal_id = ?"
        params.append(int(deal_id))
    if not include_completed:
        sql += " AND completed_at IS NULL"
    if q:
        sql += " AND (title LIKE ? OR body LIKE ?)"
        params += [f"%{q}%", f"%{q}%"]
    sql += " ORDER BY COALESCE(due_at, created_at) ASC, id DESC"
    import pandas as _pd
    try:
        df = _pd.read_sql_query(sql, get_db(), params=params)
    except Exception:
        df = _pd.DataFrame(columns=["id","deal_id","type","title","body","due_at","completed_at","created_at","created_by"])  # empty
    return df

def create_activity(deal_id:int, type_:str, title:str="", body:str="", due_at:str=None, created_by:str=None):
    conn = get_db()
    ensure_deal_activities_schema(conn)
    cur = conn.cursor()
    cur.execute("""INSERT INTO deal_activities(deal_id,type,title,body,due_at,created_by)
                 VALUES(?,?,?,?,?,?)""", (int(deal_id), type_, title or None, body or None, due_at, created_by))
    conn.commit()
    # enqueue reminder if task with due_at and email_queue exists
    try:
        if type_ == 'task' and due_at:
            subj = f"Task due for deal {deal_id}: {title or body or '(no title)'}"
            msg = f"Task due at {due_at}:\n\n{body or title or ''}"
            cur.execute("INSERT INTO email_queue(to_addr, subject, body, created_at) VALUES(?,?,?, datetime('now'))",
                        (st.session_state.get('user_email') or 'alerts@localhost', subj, msg))
            conn.commit()
    except Exception:
        pass
    return True

def update_activity(act_id:int, **fields):
    if not fields: return False
    allowed = {"title","body","due_at","completed_at","type"}
    sets = []
    vals = []
    for k,v in fields.items():
        if k in allowed:
            sets.append(f"{k} = ?")
            vals.append(v)
    if not sets: return False
    sql = "UPDATE deal_activities SET " + ", ".join(sets) + " WHERE id = ?"
    vals.append(int(act_id))
    conn = get_db()
    ensure_deal_activities_schema(conn)
    conn.execute(sql, vals)
    conn.commit()
    return True

def delete_activity(act_id:int):
    conn = get_db()
    ensure_deal_activities_schema(conn)
    conn.execute("DELETE FROM deal_activities WHERE id = ?", (int(act_id),))
    conn.commit()
    return True

def _render_deals_activities_and_calendar():
    import streamlit as st
    conn = get_db()
    ensure_deal_activities_schema(conn)
    # Quick actions
    st.markdown("### Activities")
    c1,c2,c3,c4 = st.columns([2,1,2,2])
    with c1:
        deal_id = st.number_input("Deal ID", min_value=1, step=1, value=1, key="qa_deal_id")
    with c2:
        type_ = st.selectbox("Type", ["call","note","task"], key="qa_type")
    with c3:
        title = st.text_input("Title", key="qa_title")
    with c4:
        due_at = st.text_input("Due (YYYY-MM-DD HH:MM)", key="qa_due") if type_=="task" else st.text_input("Due (optional)", key="qa_due2")
    body = st.text_area("Notes", key="qa_body", height=80)
    b1,b2,b3 = st.columns(3)
    if b1.button("Log call"):
        create_activity(deal_id, "call", title or "Call", body or "", None, st.session_state.get("user_id"))
        st.success("Call logged."); st.session_state["deals_refresh"] = st.session_state.get("deals_refresh",0) + 1; st.rerun()
    if b2.button("Add note"):
        create_activity(deal_id, "note", title or "Note", body or "", None, st.session_state.get("user_id"))
        st.success("Note added."); st.session_state["deals_refresh"] += 1; st.rerun()
    if b3.button("Create task"):
        create_activity(deal_id, "task", title or "Task", body or "", due_at or None, st.session_state.get("user_id"))
        st.success("Task created and reminder enqueued."); st.session_state["deals_refresh"] += 1; st.rerun()

    # Activity list with inline complete/delete
    st.markdown("#### Open items")
    q = st.text_input("Filter", key="dact_q")
    df_open = list_activities(include_completed=False, q=q)
    st.dataframe(df_open, use_container_width=True, hide_index=True)
    # Inline complete
    act_id = st.number_input("Complete activity ID", min_value=0, step=1, value=0, key="comp_id")
    if st.button("Mark complete") and act_id:
        update_activity(int(act_id), completed_at=dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        st.session_state["deals_refresh"] += 1; st.rerun()

    # Calendar view
    st.divider()
    st.markdown("### Calendar")
    import pandas as _pd
    df_tasks = list_activities(include_completed=False)
    df_tasks = df_tasks[df_tasks["type"]=="task"].copy()
    if not df_tasks.empty:
        # Normalize dates
        def _norm(s):
            try:
                return _pd.to_datetime(s)
            except Exception:
                return _pd.NaT
        df_tasks["start"] = df_tasks["due_at"].apply(_norm)
        df_tasks["end"] = df_tasks["start"]
        df_tasks["name"] = df_tasks["title"].fillna("Task")
        try:
            import plotly.express as px
            fig = px.timeline(df_tasks, x_start="start", x_end="end", y="deal_id", hover_name="name", hover_data=["body","id"])
            st.plotly_chart(fig, use_container_width=True)
        except Exception:
            st.dataframe(df_tasks[["deal_id","title","due_at"]], use_container_width=True, hide_index=True)
    else:
        st.info("No upcoming tasks.")
# === END DEALS PHASE 3 ===




def delete_deal(id_: int):
    conn = get_db()
    ensure_deals_table(conn)
    cur = conn.cursor()
    cur.execute("delete from deals where id = ?", (id_,))
    conn.commit()
    return cur.rowcount > 0


# === Deals (CRM Pipeline) tab ===
try:
    with legacy_tabs[13]:
        st.subheader("Deals Pipeline")
        st.caption("Track opportunities by stage, assign owners, record amounts, and manage the pipeline.")

        # Filters
        c1,c2,c3 = st.columns([1,1,2])
        with c1:
            stage_filter = st.selectbox("Stage", options=["All"] + DEAL_STAGES, index=0, key="deals_stage_filter")
        with c2:
            q = st.text_input("Search", key="deals_search")
        with c3:
            st.markdown(" ")

        # Data
        df = list_deals(stage_filter, q)

        # Totals by stage (above grid)
        st.markdown("#### Totals by stage")
        import pandas as _pd
        _stage_amounts = _pd.to_numeric(df["amount"], errors="coerce").fillna(0)
        _stage_totals = _stage_amounts.groupby(df["stage"]).sum() if not df.empty else _pd.Series(dtype=float)
        _stage_totals = _stage_totals.reindex(DEAL_STAGES).fillna(0)
        _cols = st.columns(len(DEAL_STAGES))
        for _i, _stage in enumerate(DEAL_STAGES):
            with _cols[_i]:
                st.metric(_stage, f"${float(_stage_totals.get(_stage, 0.0)):,.2f}")

        # Add a new deal
        st.markdown("#### Add a new deal")
        with st.form("new_deal_form", clear_on_submit=True):
            nc1,nc2,nc3,nc4 = st.columns([2,1,1,1])
            with nc1:
                new_title = st.text_input("Opportunity title*", placeholder="e.g., USDA Athens Roof Repair RFQ")
            with nc2:
                new_stage = st.selectbox("Stage*", options=DEAL_STAGES, index=0)
            with nc3:
                new_owner = st.text_input("Owner", placeholder="e.g., Latrice")
            with nc4:
                new_amount = st.number_input("Amount", min_value=0.0, step=100.0, value=0.0, format="%.2f")
            nc5,nc6 = st.columns([1,1])
            with nc5:
                new_agency = st.text_input("Agency", placeholder="e.g., USDA ARS")
            with nc6:
                new_due = st.text_input("Due date (YYYY-MM-DD)", placeholder="2025-11-01")
            new_notes = st.text_area("Notes", height=80, placeholder="Key details, next actions...")
            submitted = st.form_submit_button("Create deal")
            if submitted:
                if not new_title.strip():
                    st.warning("Please enter a title.")
                else:
                    create_deal(
                        new_title.strip(),
                        new_stage,
                        new_owner.strip() or None,
                        float(new_amount) if new_amount else None,
                        new_notes.strip() or None,
                        new_agency.strip() or None,
                        new_due.strip() or None
                    )
                    st.success("Deal created.")
                    st.rerun()

        # Pipeline editor (grid)
        st.markdown("#### Pipeline editor")
        if df.empty:
            st.info("No deals yet. Add your first deal above.")
        else:
            edited = st.data_editor(
                df[["id","title","stage","owner","amount","agency","due_date","notes"]],
                key="deals_editor",
                num_rows="dynamic",
                column_config={
                    "id": st.column_config.NumberColumn("ID", disabled=True),
                    "stage": st.column_config.SelectboxColumn("Stage", options=DEAL_STAGES),
                    "amount": st.column_config.NumberColumn("Amount", step=100.0, format="%.2f"),
                    "notes": st.column_config.TextColumn("Notes", width="medium"),
                    "title": st.column_config.TextColumn("Title", width="medium"),
                    "agency": st.column_config.TextColumn("Agency"),
                    "owner": st.column_config.TextColumn("Owner"),
                    "due_date": st.column_config.TextColumn("Due date"),
                },
                hide_index=True
            )
            import pandas as pd
            changes = []
            for _, row in edited.iterrows():
                orig = df.loc[df["id"]==row["id"]].iloc[0]
                updates = {}
                for col in ["title","stage","owner","amount","notes","agency","due_date"]:
                    if pd.isna(row[col]) and pd.isna(orig[col]):
                        continue
                    if (row[col] != orig[col]) and not (pd.isna(row[col]) and orig[col] in ("", None)):
                        updates[col] = None if (isinstance(row[col], float) and pd.isna(row[col])) else row[col]
                if updates:
                    ok = update_deal(int(row["id"]), **updates)
                    if ok:
                        changes.append((int(row["id"]), updates))
            if changes:
                st.success(f"Saved {len(changes)} change(s).")

        # Kanban board
        st.divider()
        st.markdown("### Board view")
        st.caption("Column per stage with quick add, inline edits, and move.")

        df_board = list_deals(stage=None, q=q)
        import pandas as _pd2
        _counts = df_board.groupby("stage")["id"].count() if not df_board.empty else _pd2.Series(dtype=int)
        _amounts = _pd2.to_numeric(df_board["amount"], errors="coerce").fillna(0.0) if not df_board.empty else _pd2.Series(dtype=float)
        _totals = _amounts.groupby(df_board["stage"]).sum() if not df_board.empty else _pd2.Series(dtype=float)
        grand_total = float(_amounts.sum()) if not df_board.empty else 0.0
        st.markdown(f"**Total pipeline value:** ${grand_total:,.2f}")

        cols = st.columns(len(DEAL_STAGES))
        for i, stage_name in enumerate(DEAL_STAGES):
            with cols[i]:
                st.markdown(f"#### {stage_name}")
                st.caption(f"{int(_counts.get(stage_name, 0))} deals • ${float(_totals.get(stage_name, 0.0)):,.2f}")

                # Quick add in this stage
                with st.container(border=True):
                    _new_title = st.text_input("New deal title", key=f"quick_new_title_{i}")
                    qa1, qa2 = st.columns([1,1])
                    with qa1:
                        _new_owner = st.text_input("Owner", key=f"quick_new_owner_{i}")
                    with qa2:
                        _new_amount = st.number_input("Amount", min_value=0.0, step=100.0, value=0.0, format="%.2f", key=f"quick_new_amt_{i}")
                    if st.button("New in this stage", key=f"quick_new_btn_{i}"):
                        if _new_title.strip():
                            create_deal(_new_title.strip(), stage_name, _new_owner.strip() or None, float(_new_amount) if _new_amount else None, None, None, None)
                            st.success("Deal created")
                            st.rerun()
                        else:
                            st.warning("Enter a title first")

                # Cards for deals in this stage
                stage_rows = df_board[df_board["stage"] == stage_name]
                for _, row in stage_rows.iterrows():
                    with st.container(border=True):
                        st.markdown(f"**{row['title']}**")
                        st.caption(f"Owner: {row['owner'] or 'Unassigned'}  •  Amount: ${float(row['amount'] or 0):,.2f}")
                        kc1, kc2 = st.columns([1,1])
                        with kc1:
                            new_owner = st.text_input("Owner", value=row["owner"] or "", key=f"owner_{row['id']}")
                        with kc2:
                            new_amt = st.number_input("Amount", value=float(row["amount"] or 0.0), step=100.0, format="%.2f", key=f"amt_{row['id']}")
                        km1, km2 = st.columns([2,1])
                        with km1:
                            new_stage = st.selectbox("Move to", options=DEAL_STAGES, index=DEAL_STAGES.index(stage_name), key=f"mv_{row['id']}")
                        with km2:
                            if st.button("Save", key=f"save_{row['id']}"):
                                changes = {}
                                if new_owner != (row["owner"] or ""):
                                    changes["owner"] = new_owner or None
                                if float(new_amt) != float(row["amount"] or 0.0):
                                    changes["amount"] = float(new_amt)
                                if new_stage != row["stage"]:
                                    changes["stage"] = new_stage
                                if changes:
                                    ok = update_deal(int(row["id"]), **changes)
                                    if ok:
                                        st.success("Updated")
                                        st.rerun()
                                    else:
                                        st.error("No changes saved")

        # Danger zone
        with st.expander("Danger zone: delete a deal"):
            del_id = st.number_input("Deal ID to delete", min_value=1, step=1, value=1)
            if st.button("Delete deal"):
                if delete_deal(int(del_id)):
                    st.warning(f"Deleted deal {int(del_id)}.")
                    st.rerun()
                else:
                    st.error("Delete failed or ID not found.")

        # --- Activities + Calendar (flagged) ---
        ff = feature_flags()
        if ff.get('deals_activities', True):
            st.divider()
            _render_deals_activities_and_calendar()

        # --- Forecast + Signals (flagged) ---
        try:
            ff = feature_flags()
        except Exception:
            ff = {}
        if ff.get('deals_forecast', True):
            st.divider()
            st.markdown("### Forecast and Signals")
            qf = st.text_input("Search for forecast", key="forecast_q")
            df_all = _list_deals_for_forecast(qf)
            bm, bq = forecast_weighted(df_all)
            c1,c2 = st.columns(2)
            with c1:
                st.markdown("#### Weighted by month")
                st.dataframe(bm, use_container_width=True, hide_index=True)
            with c2:
                st.markdown("#### Weighted by quarter")
                st.dataframe(bq, use_container_width=True, hide_index=True)
            # Optional chart
            try:
                import plotly.express as px
                if not bm.empty:
                    figm = px.bar(bm, x="period", y="weighted_total", title="Weighted pipeline by month")
                    st.plotly_chart(figm, use_container_width=True)
            except Exception:
                pass
            # SLA blockers
            st.markdown("#### SLA and blockers")
            bl = compute_sla_blockers()
            if bl:
                import pandas as pd
                dfb = pd.DataFrame(bl, columns=["deal_id","title","reason"])
                st.dataframe(dfb, use_container_width=True, hide_index=True)
            else:
                st.info("No SLA blockers detected.")
            # Win prob updater
            st.markdown("#### Update win probability from signals")
            upd_id = st.number_input("Deal ID", min_value=0, step=1, value=0, key="upd_win_id")
            if st.button("Update win_prob"):
                if upd_id:
                    ok = update_win_prob_from_signals(int(upd_id))
                    st.success("Updated." if ok else "No change.") 
                    st.session_state['deals_refresh'] = st.session_state.get('deals_refresh',0)+1
                    st.rerun()
except Exception as _e_deals:
    try:
        st.caption(f"[Deals tab note: {_e_deals}]")
    except Exception:
        pass



# Convenience: call this from your Outreach tab after assembling fields
def outreach_send(to: str, subject: str, body_html: str, cc: str = "", bcc: str = "", attachments=None):
    return outreach_send_from_active_user(to, subject, body_html, cc=cc, bcc=bcc, attachments=attachments)


# === AI Compliance and CO-Minded Proposal Utilities ===

from typing import List, Tuple, Dict
import re
import math

# Safe syllable count for readability
_vowels = "aeiouy"

def _count_syllables(word: str) -> int:
    w = word.lower()
    if not w:
        return 0
    count = 0
    prev_vowel = False
    for ch in w:
        is_vowel = ch in _vowels
        if is_vowel and not prev_vowel:
            count += 1
        prev_vowel = is_vowel
    if w.endswith("e") and count > 1:
        count -= 1
    return max(count, 1)

def flesch_kincaid_grade(text: str) -> float:
    sentences = max(len(re.findall(r'[.!?]+', text)) , 1)
    words = re.findall(r"[A-Za-z0-9']+", text)
    word_count = max(len(words), 1)
    syllables = sum(_count_syllables(w) for w in words) or 1
    # Flesch Kincaid Grade Level
    return 0.39 * (word_count / sentences) + 11.8 * (syllables / word_count) - 15.59

# Extract key terms and headings from SOW or PWS
SECTION_HINTS = [
    "Scope", "Scope of Work", "Performance Work Statement", "PWS",
    "Statement of Work", "SOW", "Deliverables", "Period of Performance",
    "Place of Performance", "Quality Assurance", "QA", "Quality Control",
    "QC", "Safety", "Standards", "Reports", "Schedule", "Milestones",
    "Contract Type", "Evaluation", "Evaluation Criteria", "CLIN",
    "Security", "Travel", "Invoicing", "Acceptance", "Inspection"
]

def extract_keywords_and_sections(src: str) -> Dict[str, List[str]]:
    # Keywords: all capitalized multi word phrases and section hints
    tokens = re.findall(r'\b[A-Z][A-Za-z0-9\-\/&]+\b', src)
    # Keep meaningful unique tokens
    cap_keywords = sorted(set(t for t in tokens if len(t) > 2))
    # Section heads: lines that look like headings
    lines = src.splitlines()
    heads = []
    for ln in lines:
        if len(ln) <= 2:
            continue
        if re.match(r'^\s*(\d+(\.\d+)*)?\s*[A-Z][A-Za-z0-9 \-\/&]{3,}$', ln.strip()):
            heads.append(ln.strip())
    hints = [h for h in SECTION_HINTS if h.lower() in src.lower()]
    return {
        "keywords": cap_keywords[:300],
        "sections": heads[:100],
        "hints": hints
    }

# Compliance checklist sections
REQUIRED_SECTIONS = [
    "Executive Summary",
    "Technical Approach",
    "Management Plan",
    "Staffing Plan",
    "Quality Assurance",
    "Risk Mitigation",
    "Schedule",
    "Past Performance",
    "Pricing",
    "Assumptions and Constraints"
]

def find_missing_sections(draft_text: str) -> List[str]:
    low = draft_text.lower()
    missing = []
    for sec in REQUIRED_SECTIONS:
        if sec.lower() not in low:
            missing.append(sec)
    return missing

# Simple evaluator scoring rubric that mirrors common federal evaluations
EVAL_WEIGHTS = {
    "Technical Capability": 0.35,
    "Management Approach": 0.20,
    "Past Performance": 0.20,
    "Quality Assurance": 0.15,
    "Risk Mitigation": 0.10
}

def _has_any(text: str, terms: List[str]) -> bool:
    t = text.lower()
    return any(term.lower() in t for term in terms)

def score_proposal_against_rubric(draft: str, sow: str) -> Dict[str, float]:
    scores = {}
    # Technical Capability: references to meeting PWS tasks and deliverables
    tech_terms = ["pws", "sow", "deliverable", "task order", "acceptance", "inspection", "standard", "section"]
    tech_score = 1.0 if _has_any(draft, tech_terms) and _has_any(draft, extract_keywords_and_sections(sow)["hints"]) else 0.7
    # Management Approach: schedule, roles, reporting, communication
    mgmt_terms = ["schedule", "transition", "organizational chart", "roles", "responsibilities", "escalation", "communication", "supervisor"]
    mgmt_score = 1.0 if _has_any(draft, mgmt_terms) else 0.6
    # Past Performance
    pp_terms = ["past performance", "contract number", "cpars", "references", "similar"]
    pp_score = 1.0 if _has_any(draft, pp_terms) else 0.5
    # Quality Assurance
    qa_terms = ["quality assurance", "quality control", "qa", "qc", "inspection", "checklist", "kpi"]
    qa_score = 1.0 if _has_any(draft, qa_terms) else 0.6
    # Risk Mitigation
    risk_terms = ["risk", "mitigation", "contingency", "proactive", "issue", "corrective action"]
    risk_score = 1.0 if _has_any(draft, risk_terms) else 0.5

    components = {
        "Technical Capability": tech_score,
        "Management Approach": mgmt_score,
        "Past Performance": pp_score,
        "Quality Assurance": qa_score,
        "Risk Mitigation": risk_score
    }
    for k, base in components.items():
        scores[k] = round(base * EVAL_WEIGHTS[k] * 100, 1)

    total = round(sum(scores.values()), 1)
    scores["Total"] = total
    return scores

# Auto detect risks and propose mitigations
RISK_LIBRARY = [
    ("Remote site logistics", ["remote", "rural", "island"], "Pre stage materials and use local subs"),
    ("Tight schedule", ["accelerated", "expedite", "short notice", "compressed"], "Parallel tasking and add surge staff"),
    ("Hazardous work", ["hazard", "osha", "confined space", "asbestos", "lead"], "Site safety plan and qualified PPE"),
    ("Security and access", ["secret", "clearance", "escort", "badging"], "Advance badging and backup staff with clearances"),
    ("Supply chain", ["lead time", "backorder", "long lead"], "Approved alternates and buffer stock"),
    ("After hours work", ["after hours", "off hours", "weekend", "night"], "Noise control and CO approved schedule")
]

def identify_risks(src: str) -> List[Tuple[str, str]]:
    low = src.lower()
    found = []
    for name, triggers, mitigation in RISK_LIBRARY:
        if any(t in low for t in triggers):
            found.append((name, mitigation))
    return found

def propose_outline_with_mirrored_terms(sow_text: str) -> str:
    ex = extract_keywords_and_sections(sow_text)
    hints = ", ".join(ex["hints"][:8])
    outline = f"""Executive Summary
Technical Approach
  Alignment to PWS and SOW sections: {hints}
  Tasks and Deliverables
  Methods and Standards
Management Plan
  Roles and Responsibilities
  Communication and Reporting
  Schedule and Milestones
Staffing Plan
  Key Personnel Qualifications
  Subcontractor Roles
Quality Assurance
  Inspection and Acceptance
  KPIs and Checklists
Risk Mitigation
  Identified Risks and Proactive Controls
Past Performance
  Relevant Contracts and Outcomes
Pricing
  Assumptions and Basis of Estimate
Assumptions and Constraints
"""
    return outline

def clean_placeholders(text: str) -> str:
    # Remove INSERT style placeholders while keeping content safe
    return re.sub(r"\bINSERT[\w\s\-:]*\b", "", text, flags=re.IGNORECASE)

def compliance_assess(draft_text: str, sow_text: str) -> Dict[str, object]:
    fk = round(flesch_kincaid_grade(draft_text), 2)
    missing = find_missing_sections(draft_text)
    ex = extract_keywords_and_sections(sow_text)
    risks = identify_risks(sow_text + "\n" + draft_text)
    scores = score_proposal_against_rubric(draft_text, sow_text)
    return {
        "fk_grade": fk,
        "missing_sections": missing,
        "mirrored_terms_sample": ex["hints"][:10],
        "risks": risks,
        "scores": scores
    }

# Optional Streamlit UI injection guarded to avoid import errors
def mount_compliance_assistant():
    try:
        import streamlit as st
    except Exception:
        return
    with st.expander("Proposal Compliance Assistant", expanded=False):
        sow = st.text_area("Paste Solicitation or SOW text", height=200, key="co_sow_text")
        draft = st.text_area("Paste Proposal Draft text", height=200, key="co_draft_text")
        if st.button("Run Compliance Check"):
            result = compliance_assess(draft, sow)
            st.write("Flesch Kincaid grade:", result["fk_grade"])
            st.write("Missing sections:", result["missing_sections"] or "None")
            st.write("Mirrored terms to use:", result["mirrored_terms_sample"] or "None")
            if result["risks"]:
                st.write("Detected risks and mitigations:")
                for r, m in result["risks"]:
                    st.write(f"- {r}: {m}")
            st.write("Evaluator style scores:", result["scores"])
        if st.button("Suggest Outline from SOW"):
            st.code(propose_outline_with_mirrored_terms(sow), language="markdown")
        if st.button("Clean Placeholders in Draft"):
            st.text_area("Cleaned Draft", clean_placeholders(draft), height=200, key="co_cleaned_draft_out")

# Attempt to mount automatically if Streamlit is present
try:
    pass  # auto-inserted to fix empty try block
#     mount_compliance_assistant()
except Exception:
    pass

# --- Placeholder cleaner (injected) ---
def _clean_placeholders(text: str) -> str:
    """Remove obvious template placeholders without touching normal words.
    Rules:
      - Remove bracketed placeholders like [INSERT ...], {PLACEHOLDER}, <TBD>, ((TODO))
      - Remove isolated ALL-CAPS tokens commonly used as placeholders (INSERT, TBD, TODO)
      - Collapse multiple spaces to one
    """
    import re
    if not text:
        return text
    out = text
    # Remove bracketed placeholders of common bracket styles
    out = re.sub(r"\[[^\]]*(?i:(insert|placeholder|tbd|todo))[^\]]*\]", "", out)
    out = re.sub(r"\{[^\}]*?(?i:(insert|placeholder|tbd|todo))[^\}]*\}", "", out)
    out = re.sub(r"<[^>]*?(?i:(insert|placeholder|tbd|todo))[^>]*?>", "", out)
    out = re.sub(r"\(\([^\)]*?(?i:(insert|placeholder|tbd|todo))[^\)]*?\)\)", "", out)
    # Remove standalone ALL-CAPS tokens
    out = re.sub(r"(?m)^\s*\b(INSERT|TBD|TODO)\b\s*:?.*$", "", out)
    # Remove repeated underscores or lines of underscores
    out = re.sub(r"_{3,}", "", out)
    # Clean up lingering 'lorem ipsum'
    out = re.sub(r"(?i)lorem ipsum[\s\S]*?(?=\n\n|$)", "", out)
    # Collapse spaces
    out = re.sub(r"[ \t]{2,}", " ", out)
    # Tidy blank lines
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


# --- Robust placeholder and artifact cleaner for proposal exports ---
import re as _re_clean

def _clean_placeholders(text: str) -> str:
    if not isinstance(text, str):
        return text
    t = text
    t = _re_clean.sub(r"\[[^\]]*?INSERT[^\]]*\]", "", t, flags=_re_clean.IGNORECASE)
    t = _re_clean.sub(r"\[[^\]]*?PLACEHOLDER[^\]]*\]", "", t, flags=_re_clean.IGNORECASE)
    t = _re_clean.sub(r"\{[^}]*?(INSERT|TBD|PLACEHOLDER)[^}]*\}", "", t, flags=_re_clean.IGNORECASE)
    t = _re_clean.sub(r"<[^>]*?(INSERT|TBD|PLACEHOLDER)[^>]*>", "", t, flags=_re_clean.IGNORECASE)
    t = _re_clean.sub(r"\(\([^)]*?(INSERT|TBD|PLACEHOLDER)[^)]*\)\)", "", t, flags=_re_clean.IGNORECASE)
    t = _re_clean.sub(r"\b(INSERT|TBD|TO BE DETERMINED|LOREM IPSUM|FILL ME|PLACEHOLDER)\b[:\-]*", "", t, flags=_re_clean.IGNORECASE)
    t = _re_clean.sub(r"\n?_{3,}\n?", "\n", t)
    t = _re_clean.sub(r"\.{4,}", "", t)
    t = _re_clean.sub(r"<<[^>]*>>", "", t)
    t = _re_clean.sub(r"\n{3,}", "\n\n", t)
    t = "\n".join(line.rstrip() for line in t.splitlines())
    return t


# === [MERGE] Backend: SAM search v2 and helpers (final) ===
try:
    _ = sam_search_v3
    _SAM_SEARCH_DEFINED = True
except NameError:
    _SAM_SEARCH_DEFINED = False

import json as _json
import pandas as _pd
import requests as _requests
from datetime import datetime as _dt, timedelta as _td

def sam_search_v3(filters: dict, limit: int = 100):
    """
    Public Get Opportunities API v2 with required dates.
    postedFrom and postedTo are MM/dd/YYYY. Defaults to last 30 days.
    Maps: keywords->title, naics list->ncode one at a time, setAside->typeOfSetAside, noticeType->ptype.
    """
    if not ('SAM_API_KEY' in globals() and SAM_API_KEY):
        return _pd.DataFrame(), {"ok": False, "reason": "missing_key"}

    today = _dt.utcnow().date()
    posted_from = filters.get("postedFrom") or (today - _td(days=30)).strftime("%m/%d/%Y")
    posted_to = filters.get("postedTo") or today.strftime("%m/%d/%Y")

    def _one_call(single_naics: str | None):
        params = {
            "api_key": SAM_API_KEY,
            "postedFrom": posted_from,
            "postedTo": posted_to,
            "limit": str(int(limit)),
            "offset": "0",
        }
        if filters.get("keywords"):
            params["title"] = filters["keywords"]
        if single_naics:
            params["ncode"] = single_naics
        if filters.get("setAside"):
            params["typeOfSetAside"] = filters["setAside"]
        if filters.get("noticeType"):
            v = str(filters["noticeType"]).lower()
            p = None
            if "combined" in v: p = "k"
            elif "solicitation" in v: p = "o"
            elif "sources" in v: p = "r"
            elif "special" in v: p = "s"
            elif "award" in v: p = "a"
            if p: params["ptype"] = p
        if str(filters.get("active","true")).lower() == "true":
            params["status"] = "active"

        base = "https://api.sam.gov/opportunities/v2/search"
        r = _requests.get(base, params=params, timeout=45)
        js = r.json() if "application/json" in r.headers.get("Content-Type","") else {}
        if r.status_code != 200:
            return _pd.DataFrame(), {"ok": False, "status": r.status_code, "message": (js.get("message") or r.text)[:400], "params": params}

        items = js.get("opportunitiesData", []) or js.get("data", []) or []
        rows = []
        for opp in items:
            title = opp.get("title") or opp.get("Title")
            sol = opp.get("solicitationNumber") or opp.get("solnum") or opp.get("noticeid")
            agency = opp.get("fullParentPathName") or opp.get("organizationName")
            posted = opp.get("postedDate") or opp.get("publishedDate")
            due = opp.get("reponseDeadLine") or opp.get("responseDeadLine") or ""
            naics = opp.get("naicsCode") or ""
            psc = opp.get("classificationCode") or ""
            nid = opp.get("noticeid") or opp.get("noticeId") or sol
            url = f"https://sam.gov/opp/{nid}/view"

            rows.append({
                "sam_notice_id": nid,
                "title": title,
                "agency": agency,
                "naics": naics,
                "psc": psc,
                "place_of_performance": "",
                "response_due": due,
                "posted": posted,
                "type": opp.get("type") or opp.get("baseType"),
                "set_aside": opp.get("setAside") or opp.get("setAsideCode") or "",
                "url": url,
                "attachments_json": "[]"
            })
        return _pd.DataFrame(rows), {"ok": True, "count": len(rows), "params": params}

    naics_list = filters.get("naics") or []
    if isinstance(naics_list, list) and len(naics_list) > 1:
        frames = []
        for code in naics_list:
            df, _ = _one_call(code)
            if not df.empty:
                frames.append(df)
        if frames:
            df_all = _pd.concat(frames).drop_duplicates(subset=["sam_notice_id"])
            return df_all, {"ok": True, "count": len(df_all)}
        return _pd.DataFrame(), {"ok": True, "count": 0}
    else:
        code = naics_list[0] if isinstance(naics_list, list) and naics_list else None
        return _one_call(code)

def _sam_get_saved_filters():
    try:
        raw = get_setting("sam_saved_filters","")
        return _json.loads(raw) if raw else []
    except Exception:
        return []

def _sam_set_saved_filters(filters_list):
    try:
        set_setting("sam_saved_filters", _json.dumps(filters_list))
    except Exception:
        pass

def import_sam_to_db(filters: dict, stage_on_insert: str = "No Contact Made"):
    df, info = sam_search_v3(filters, limit=200)
    if not info.get("ok"):
        return 0, info
    try:
        conn = get_db(); cur = conn.cursor()
        inserted = 0
        for _, r in df.iterrows():
            nid = r.get("sam_notice_id")
            if not nid:
                continue
            exists = cur.execute("select id from opportunities where sam_notice_id=? limit 1", (nid,)).fetchone()
            if exists:
                continue
            cur.execute("""insert into opportunities
                (sam_notice_id, title, agency, naics, psc, place_of_performance, response_due, posted, type, url, attachments_json, status)
                values(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (r.get("sam_notice_id"), r.get("title"), r.get("agency"), r.get("naics"), r.get("psc"),
                 r.get("place_of_performance"), r.get("response_due"), r.get("posted"), r.get("type"),
                 r.get("url"), r.get("attachments_json"), stage_on_insert))
            inserted += 1
        conn.commit()
        if inserted > 0:
            try:
                _send_team_alert(f"New SAM opportunities imported: {inserted}")
            except Exception:
                pass
        return inserted, info
    except Exception as e:
        return 0, {"ok": False, "reason": "db_error", "detail": str(e)[:300]}

try:
    _ = proposal_quick_quote
except NameError:
    def _get_opp(opp_id: int):
        try:
            conn = get_db()
            row = conn.execute("select * from opportunities where id=?", (int(opp_id),)).fetchone()
            if not row:
                return {}
            info = conn.execute("PRAGMA table_info(opportunities)").fetchall()
            colnames = [c[1] for c in info]
            return dict(zip(colnames, row))
        except Exception:
            return {}

    def proposal_quick_quote(opp_id: int) -> str:
        opp = _get_opp(opp_id)
        if not opp:
            return ""
        title = f"Quick Quote - {opp.get('title','Untitled')} ({opp.get('sam_notice_id','')})"
        body = f"""# {opp.get('title','')}

**Solicitation #:** {opp.get('sam_notice_id','')}
**Agency/Office:** {opp.get('agency','')}
**NAICS/PSC:** {opp.get('naics','')} / {opp.get('psc','')}
**Due date:** {opp.get('response_due','')}
**Contact:** _INSERT POC_

---
## Technical Approach
INSERT

## Pricing
INSERT

## Past Performance
INSERT

## Compliance Checklist
- SAM registration verified
- Past performance attached
- Pricing confirmed
- Reps & Certs complete
- Forms signed

"""
        path = save_proposal_draft(title, body)
        try:
            conn = get_db()
            conn.execute("insert into tasks(opp_id,title,assignee,status) values(?,?,?,?)",
                         (int(opp_id), "Proposal Started", "", "Open"))
            conn.execute("update opportunities set status=? where id=?", ("Proposal Started", int(opp_id)))
            conn.commit()
            try:
                _send_team_alert(f"Proposal started for opp #{opp_id}: {opp.get('title','')}")
            except Exception:
                pass
        except Exception:
            pass
        return path

    def proposal_submit_package(opp_id: int) -> bool:
        try:
            conn = get_db()
            conn.execute("update opportunities set status=? where id=?", ("Submitted", int(opp_id)))
            conn.execute("insert into tasks(opp_id,title,status) values(?,?,?)", (int(opp_id), "Package Submitted", "Closed"))
            conn.commit()
            try:
                _send_team_alert(f"Package submitted for opp #{opp_id}.")
            except Exception:
                pass
            return True
        except Exception:
            return False

try:
    _ = ensure_default_checklist
except NameError:
    def ensure_default_checklist(opp_id: int):
        items = [
            "SAM registration verified",
            "Past performance attached",
            "Pricing confirmed",
            "Technical approach drafted",
            "Reps & Certs completed",
            "Forms signed"
        ]
        try:
            conn = get_db(); cur = conn.cursor()
            for it in items:
                cur.execute(
                    "insert into compliance_items(opp_id, item, required, status, source) values(?,?,?,?,?)",
                    (int(opp_id), it, 1, "Pending", "Checklist")
                )
            conn.commit()
        except Exception:
            pass

def _send_team_alert(msg: str):
    try:
        addrs = [
            USER_EMAILS.get("Quincy",""),
            USER_EMAILS.get("Charles",""),
            USER_EMAILS.get("Collin",""),
        ]
        addrs = [a for a in addrs if a]
        if not addrs:
            return
        subj = "ELA Bid Alert"
        body = f"<p>{msg}</p><p>Open the app to review in Pipeline.</p>"
        for a in addrs:
            try:
                send_outreach_email("Charles", a, subj, body)
            except Exception:
                pass
    except Exception:
        pass
# === [END MERGE] Backend ===



# === [MERGE UI] SAM Watch — Minimal UI (final) ===
try:
    import streamlit as _st
    # Helper to build a stable selection key even if ACTIVE_USER is missing
    def _sam_sel_key(_rid: int) -> str:
        try:
            _au = ACTIVE_USER
        except Exception:
            try:
                import streamlit as __st
                _au = __st.session_state.get("active_user") or "anon"
            except Exception:
                _au = "anon"
        return f"{_au}::sam_sel_{_rid}"

    _ = tabs; _ = TAB

    def _mk_filter(kw, naics_csv, set_aside, notice, min_due, active_only):
        return {
            "name": "Default",
            "keywords": kw.strip(),
            "naics": [s.strip() for s in naics_csv.split(",") if s.strip()],
            "setAside": "Total Small Business" if set_aside == "Total Small Business" else "",
            "noticeType": "Combined Synopsis/Solicitation,Solicitation" if notice != "Any" else "",
            "active": "true" if active_only else "false",
            "minDueDays": int(min_due)
        }

    with tabs[TAB['SAM Watch']]:
        _st.header("SAM Watch")
        _st.subheader("Filters")
        with _st.form("simple_filters", clear_on_submit=False):
            c1, c2, c3 = _st.columns([2,2,2])
            with c1:
                kw = _st.text_input("Keywords", value="janitorial")
            with c2:
                naics = _st.text_input("NAICS list", value="561720, 238220")
            with c3:
                set_aside = _st.selectbox("Set aside", ["Any", "Total Small Business"], index=1)
            c4, c5, c6 = _st.columns([2,2,2])
            with c4:
                notice = _st.selectbox("Notice type", ["Any", "Combined Synopsis/Solicitation", "Solicitation"], index=1)
            with c5:
                min_due = _st.number_input("Min days until due", min_value=0, value=3, step=1)
            with c6:
                active_only = _st.checkbox("Active only", value=True)
            save_search = _st.form_submit_button("Save as default")

        if save_search:
            _sam_set_saved_filters([_mk_filter(kw, naics, set_aside, notice, min_due, active_only)])
            _st.success("Default filter saved")


        _st.subheader("Actions")
        colA, colB, colC, colD = _st.columns([1,1,1,1])
        with colA:
            if _st.button("Pull data", use_container_width=True):
                loaded_rows = []
                try:
                    import hashlib
                    for flt in _sam_get_saved_filters():
                        df, info = sam_search_v3(flt, limit=200)
                        if info.get("ok") and not df.empty:
                            for _, r in df.iterrows():
                                try:
                                    nid = str(r.get("sam_notice_id") or "")
                                    rid = int(hashlib.sha1(nid.encode("utf-8")).hexdigest(), 16) % 1000000000
                                except Exception:
                                    rid = int(_rand_id())
                                loaded_rows.append((
                                    rid,
                                    r.get("title"),
                                    r.get("agency"),
                                    r.get("response_due"),
                                    r.get("url"),
                                    r.get("posted"),
                                ))
                    _st.session_state["sam_watch_loaded_rows"] = loaded_rows
                    _st.success(f"Loaded {len(loaded_rows)} opportunities (not saved)")
                except Exception as _e_pull:
                    _st.error(f"Pull failed: {_e_pull}")
        with colB:
            opp_id = _st.number_input("Opp ID", min_value=0, value=0, step=1)
        with colC:
            if _st.button("Generate quote", use_container_width=True) and opp_id:
                p = proposal_quick_quote(int(opp_id))
                _st.success("Draft created" if p else "Draft failed")
        with colD:
            if _st.button("Submit package", use_container_width=True) and opp_id:
                ok = proposal_submit_package(int(opp_id))
                _st.success("Submitted") if ok else _st.error("Update failed")
                _st.subheader("Select opportunities to add to Pipeline")

        try:
            conn = get_db(); cur = conn.cursor()
            _rows_db = cur.execute("""
                select id, title, agency, response_due, url, posted
                from opportunities
                where coalesce(url,'') != ''
                order by date(posted) desc, id desc
                limit 200
            """).fetchall()
            rows = _st.session_state.get("sam_watch_loaded_rows") or _rows_db
            # Use a form so checkbox selections and the submit happen in one transaction (avoids rerun desync).
            with _st.form("sam_watch_select_form", clear_on_submit=False):
                row_ids = []
                if rows:
                    for rid, title, agency, due, url, posted in rows:
                        row_ids.append(rid)
                        c1, c2 = _st.columns([0.08, 0.92])
                        with c1:
                            _st.checkbox(
                                "",
                                key=_sam_sel_key(rid),
                                value=_st.session_state.get(_sam_sel_key(rid), False)
                            )
                        with c2:
                            link_md = f"[{title}]({url})"
                            meta = " | ".join(filter(None, [
                                f"Agency: {agency}" if agency else "",
                                f"Due: {due}" if due else "",
                                f"Posted: {posted}" if posted else ""
                            ]))
                            _st.markdown(
                                link_md + (f"<br/><span style='font-size: 12px;'>{meta}</span>" if meta else ""),
                                unsafe_allow_html=True
                            )

                submitted = _st.form_submit_button("➕ Add Selected to Pipeline", use_container_width=True)

            if submitted:
                chosen_ids = [rid for rid in row_ids if _st.session_state.get(_sam_sel_key(rid), False)]
                if not chosen_ids:
                    _st.info("No rows selected.")
                else:
                    added, skipped = 0, 0
                    for rid, title, agency, due, url, posted in [r for r in rows if r[0] in chosen_ids]:
                        try:
                            c2 = conn.cursor()
                            exists = c2.execute(
                                "select 1 from deals where title=? and coalesce(due_date,'')=coalesce(?, '') limit 1",
                                (title, str(due) if due else None)
                            ).fetchone()
                            if exists:
                                skipped += 1
                                continue
                            notes = f"Imported from SAM Watch on selection. URL: {url}"
                            add_deal(
                                title=title,
                                stage="No Contact Made",
                                source="SAM Watch",
                                url=url,
                                owner=None,
                                amount=None,
                                notes=notes,
                                agency=agency,
                                due_date=str(due) if due else None
                            )
                            added += 1
                        except Exception as _e_add:
                            _st.warning(f"Could not add '{title}': {_e_add}")
                    _st.success(f"Added {added} deal(s). Skipped {skipped} duplicate(s).")
                    # Clear only the ones we just added to avoid accidental re-use
                    for rid in chosen_ids:
                        _st.session_state.pop(_sam_sel_key(rid), None)
            else:
                if not rows:
                    _st.caption("No opportunities found with links.")
        except Exception as _e_sel:
            _st.warning(f"[Selection UI note: {_e_sel}]")
except Exception as _e_ui:
    try:
        import streamlit as _st
        _st.warning(f"[SAM Watch UI note: {_e_ui}]")
    except Exception:
        pass

# === [END MERGE UI] ===


# === Deals tab (formerly Deadlines) – standalone UI with hyperlinks ===

# === Deals tab – GO DEALS PHASE 2: Kanban + List ===
try:
    # Ensure flag exists
    ff = st.session_state.setdefault("feature_flags", {})
    ff.setdefault("deals_core", True)
    ff.setdefault("deals_kanban", True)
    # Ensure refresh token exists
    st.session_state.setdefault('deals_refresh', 0)

    def _has_col(conn, table, col):
        try:
            cur = conn.execute(f"PRAGMA table_info({table})")
            return any(r[1] == col for r in cur.fetchall())
        except Exception:
            return False

    def _add_col_if_missing(conn, table, col_def):
        try:
            col_name = col_def.split()[0]
            if not _has_col(conn, table, col_name):
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
        except Exception:
            pass

    # Migrations for opportunities and stage history
    try:
        conn = get_db()
        conn.execute("""create table if not exists stage_history(
            id integer primary key,
            opp_id integer not null,
            old_stage text,
            new_stage text,
            changed_at text default current_timestamp,
            changed_by text
        );""")
        for coldef in [
            "stage text default 'New'",
            "next_action text",
            "due_at text",
            "compliance_state text",
            "rfq_coverage real"
        ]:
            _add_col_if_missing(conn, "opportunities", coldef)
    except Exception as _e_schema:
        st.warning(f"[Deals schema note: {_e_schema}]")

    # Loader keyed on refresh
    @st.cache_data(show_spinner=False)
    def _load_deals(refresh_token:int):
        try:
            conn = get_db()
            df = pd.read_sql_query("""
                select
                    id, notice_id, title, agency, office, sub_tier,
                    naics, set_aside, posted, response_due,
                    stage, next_action, due_at, compliance_state, rfq_coverage,
                    value_estimate, place_state, place_city
                from opportunities
                order by coalesce(response_due, posted) asc, posted desc
            """, conn)
            return df
        except Exception as e:
            st.caption(f"[Deals load note: {e}]")
            return pd.DataFrame()

    df_deals = _load_deals(st.session_state.get('deals_refresh', 0)).copy()

    # Stage catalog
    DEFAULT_STAGES = [
        "New",
        "Qualify",
        "Pursue",
        "RFQ Out",
        "Pricing",
        "Review",
        "Submitted",
        "Awarded",
        "Closed Lost"
    ]
    stages = st.session_state.setdefault("deal_stages", DEFAULT_STAGES)

    # Small helpers
    def _badge(txt, kind="muted"):
        color = {"ok":"green","warn":"orange","bad":"red","muted":"gray"}.get(kind, "gray")
        return f"<span style='padding:2px 6px;border-radius:999px;background:{color};color:white;font-size:12px'>{txt}</span>"

    def _persist_stage(opp_id:int, new_stage:str):
        try:
            conn = get_db()
            cur = conn.cursor()
            old = cur.execute("select stage from opportunities where id=?", (opp_id,)).fetchone()
            old_stage = old[0] if old else None
            cur.execute("update opportunities set stage=?, updated_at=current_timestamp where id=?", (new_stage, opp_id))
            cur.execute("insert into stage_history(opp_id, old_stage, new_stage, changed_by) values(?,?,?,?)",
                        (opp_id, old_stage, new_stage, st.session_state.get("active_user","")))
            conn.commit()
            st.session_state['deals_refresh'] += 1
            st.experimental_rerun()
        except Exception as e:
            st.warning(f"[Stage persist note: {e}]")

    def _persist_inline(opp_id:int, next_action:str, due_at):
        try:
            due_str = str(due_at) if due_at else None
            conn = get_db()
            conn.execute("update opportunities set next_action=?, due_at=?, updated_at=current_timestamp where id=?",
                         (next_action, due_str, opp_id))
            st.session_state['deals_refresh'] += 1
        except Exception as e:
            st.warning(f"[Inline persist note: {e}]")

    with tabs[TAB['Deals']]:
        st.subheader("Deals")

        view = st.segmented_control("View", options=["Kanban","List"], key="deals_view", default="Kanban") if ff.get("deals_kanban") else "List"

        if view == "Kanban":
            # Try true drag and drop if streamlit_sortables is installed
            used_drag = False
            try:
                from streamlit_sortables import sort_items
                groups = {}
                for stg in stages:
                    g = df_deals[df_deals['stage'].fillna("New") == stg]
                    groups[stg] = [
                        f"{int(r.id)} │ {str(r.title)[:60]}"
                        for _, r in g.iterrows()
                    ]
                order = sort_items(groups, multi_containers=True, direction="vertical", key="kanban_sortables")
                used_drag = True
                # Determine moved items by comparing assignment
                current_owner = {}
                for stg, items in groups.items():
                    for label in items:
                        oid = int(label.split("│",1)[0].strip())
                        current_owner[oid] = stg
                new_owner = {}
                for stg, items in order.items():
                    for label in items:
                        try:
                            oid = int(label.split("│",1)[0].strip())
                        except Exception:
                            continue
                        new_owner[oid] = stg
                moved = [(oid, current_owner.get(oid), new_owner.get(oid)) for oid in new_owner if new_owner.get(oid) != current_owner.get(oid)]
                if moved:
                    # Persist first change then rerun
                    oid, old_s, new_s = moved[0]
                    _persist_stage(oid, new_s)
            except Exception as _e_drag:
                used_drag = False
                st.caption("[Kanban drag unavailable. Falling back to click to move.]")

            # Fallback UI columns with move controls
            if not used_drag:
                cols = st.columns(len(stages))
                for idx, stg in enumerate(stages):
                    with cols[idx]:
                        st.markdown(f"**{stg}**")
                        g = df_deals[df_deals['stage'].fillna("New") == stg]
                        for _, r in g.iterrows():
                            with st.container(border=True):
                                st.caption(f"{r.agency or ''}")
                                st.write(str(r.title))
                                # Badges
                                comp = str(r.compliance_state or "unknown").title()
                                rfq = r.rfq_coverage if pd.notna(r.get("rfq_coverage")) else None
                                b1 = _badge(comp, "ok" if comp.lower()=="pass" else "warn" if comp.lower()=="partial" else "bad")
                                b2 = _badge(f"RFQ {int(rfq)}%" if rfq is not None else "RFQ 0%", "ok" if (rfq or 0) >= 80 else "warn" if (rfq or 0) >= 40 else "bad")
                                st.markdown(b1 + " " + b2, unsafe_allow_html=True)
                                move_to = st.selectbox("Move to", stages, index=stages.index(stg), key=f"mv_{int(r.id)}")
                                if move_to != stg:
                                    if st.button("Update", key=f"mv_btn_{int(r.id)}"):
                                        _persist_stage(int(r.id), move_to)

        else:
            # List view with filters and inline edits
            c1, c2, c3 = st.columns(3)
            with c1:
                stage_f = st.multiselect("Stage", stages, default=st.session_state.get("deals_stage_filter", stages))
                st.session_state["deals_stage_filter"] = stage_f
            with c2:
                agency_f = st.text_input("Agency contains", st.session_state.get("deals_agency_filter",""))
                st.session_state["deals_agency_filter"] = agency_f
            with c3:
                search = st.text_input("Search title or NAICS", st.session_state.get("deals_search",""))
                st.session_state["deals_search"] = search

            df = df_deals.copy()
            if stage_f:
                df = df[df['stage'].fillna("New").isin(stage_f)]
            if agency_f:
                df = df[df['agency'].fillna("").str.contains(agency_f, case=False, na=False)]
            if search:
                df = df[
                    df['title'].fillna("").str.contains(search, case=False, na=False)
                    | df['naics'].fillna("").str.contains(search, case=False, na=False)
                ]

            # Build editable subset
            edit_cols = ['id','title','agency','stage','next_action','due_at','compliance_state','rfq_coverage','response_due']
            for c in edit_cols:
                if c not in df.columns:
                    df[c] = None
            df_view = df[edit_cols].sort_values(by=['response_due','id'], na_position='last')

            edited = st.data_editor(
                df_view,
                hide_index=True,
                use_container_width=True,
                disabled=['id','title','agency','compliance_state','rfq_coverage','response_due'],
                key="deals_editor"
            )

            # Detect and persist inline changes
            try:
                merged = edited.merge(df_view, on='id', how='left', suffixes=('', '_old'))
                dirty = []
                for _, row in merged.iterrows():
                    if str(row.get('next_action')) != str(row.get('next_action_old')) or str(row.get('due_at')) != str(row.get('due_at_old')) or str(row.get('stage')) != str(row.get('stage_old')):
                        dirty.append(row)
                if dirty:
                    if st.button("Save changes"):
                        for row in dirty:
                            oid = int(row['id'])
                            if str(row.get('stage')) != str(row.get('stage_old')):
                                _persist_stage(oid, str(row.get('stage') or 'New'))
                            _persist_inline(oid, str(row.get('next_action') or ''), row.get('due_at'))
                        st.experimental_rerun()
                else:
                    st.caption("No edits pending.")
            except Exception as _e_merge:
                st.caption(f"[Deals persist note: {_e_merge}]")

except Exception as _e_deals_tab:
    st.caption(f"[Deals tab init note: {_e_deals_tab}]")
# ===== Layout Phase 2: Opportunity workspace subtabs =====
# Deep-link helpers
def open_details(opp): route_to("opportunity", opp_id=opp, tab="Details")
def open_analyzer(opp): route_to("opportunity", opp_id=opp, tab="Analyzer")
def open_compliance(opp): route_to("opportunity", opp_id=opp, tab="Compliance")
def open_proposal_tab(opp): route_to("opportunity", opp_id=opp, tab="Proposal")
def open_pricing(opp): route_to("opportunity", opp_id=opp, tab="Pricing")
def open_vendors(opp): route_to("opportunity", opp_id=opp, tab="VendorsRFQ")
def open_submission(opp): route_to("opportunity", opp_id=opp, tab="Submission")

# Header derivation helpers. Do not cache authoritative DB rows; only transform cached.
def _opp_header_data(opp_id: int):
    row = get_notice(int(opp_id)) if opp_id is not None else None
    d = row["data"] if row and isinstance(row.get("data"), dict) else {}
    title = d.get("title") or d.get("notice_title") or d.get("subject") or f"Opportunity {opp_id}"
    agency = d.get("agency") or d.get("department") or d.get("org_name") or d.get("office") or ""
    due = d.get("due_date") or d.get("response_due") or d.get("close_date") or d.get("responseDate") or ""
    set_asides = []
    for k in ["set_aside","setAside","naics_set_aside","solicitation_set_aside","type_of_set_aside"]:
        v = d.get(k)
        if v:
            set_asides.append(str(v))
    set_asides = list(dict.fromkeys(set_asides))[:4]
    return {"title": title, "agency": agency, "due": due, "set_asides": set_asides}

# Cached compute of badges only
def _badge_pack(opp_id: int):
    hdr = _opp_header_data(opp_id)
    return {"agency": hdr["agency"], "due": hdr["due"], "set_asides": hdr["set_asides"]}

def _workspace_header(opp_id: int):
    import streamlit as st
    hdr = _opp_header_data(opp_id)
    st.header(hdr["title"])
    badges = _badge_pack(opp_id)
    cols = st.columns(3)
    with cols[0]:
        st.caption(f"Agency: **{badges['agency'] or 'n/a'}**")
    with cols[1]:
        st.caption(f"Due: **{badges['due'] or 'n/a'}**")
    with cols[2]:
        if badges["set_asides"]:
            st.caption("Set-aside: " + " | ".join(f"**{s}**" for s in badges["set_asides"]))
        else:
            st.caption("Set-aside: **n/a**")

# Subtab skeletons. Each receives opp_id and renders only when active.
def render_details(opp_id: int):
    import streamlit as st
    st.write("Details panel placeholder.")

def render_analyzer(opp_id: int):
    import streamlit as st
    # Example lazy pattern placeholder
    @st.cache_data(ttl=900, show_spinner=False)
    def _heavy_analyzer_compute(opp):
        # Placeholder transform. Real logic lives elsewhere.
        return {"ok": True, "opp": opp}
    res = _heavy_analyzer_compute(opp_id)
    st.write("Analyzer ready.", res)

def render_compliance(opp_id: int):
    import streamlit as st
    st.write("Compliance matrix placeholder.")

def render_proposal(opp_id: int):
    import streamlit as st
    st.write("Proposal builder placeholder.")

def render_pricing(opp_id: int):
    import streamlit as st
    st.write("Pricing worksheet placeholder.")

def render_vendors_rfq(opp_id: int):
    import streamlit as st
    st.write("Vendors and RFQ placeholder.")

def render_submission(opp_id: int):
    import streamlit as st
    st.write("Submission checklist placeholder.")

def _subtab_bar(active: str, opp_id: int):
    import streamlit as st
    tabs = ["Details","Analyzer","Compliance","Proposal","Pricing","VendorsRFQ","Submission"]
    # Persist in session
    st.session_state["active_opportunity_tab"] = active
    cols = st.columns(len(tabs))
    for i, t in enumerate(tabs):
        with cols[i]:
            if st.button(t, type=("primary" if t == active else "secondary")):
                route_to("opportunity", opp_id=opp_id, tab=t, rerun=True)

def _render_opportunity_workspace():
    import streamlit as st
    if not feature_flags.get('workspace_enabled'):
        return
    r = get_route()
    if r["page"] != "opportunity":
        return
    opp_id = r["opp"]
    if opp_id is None:
        st.warning("No opportunity selected.")
        return
    # Header
    _workspace_header(opp_id)
    # Subtabs
    tabs = ["Details","Analyzer","Compliance","Proposal","Pricing","VendorsRFQ","Submission"]
    active = r["tab"] if r["tab"] in tabs else (st.session_state.get("active_opportunity_tab") or tabs[0])
    _subtab_bar(active, opp_id)
    # Lazy render for active only
    if active == "Details":
        render_details(opp_id)
    elif active == "Analyzer":
        render_analyzer(opp_id)
    elif active == "Compliance":
        render_compliance(opp_id)
    elif active == "Proposal":
        render_proposal(opp_id)
    elif active == "Pricing":
        render_pricing(opp_id)
    elif active == "VendorsRFQ":
        render_vendors_rfq(opp_id)
    elif active == "Submission":
        render_submission(opp_id)
# ===== end Layout Phase 2 =====


# ==== PHASE 3 PERSIST START ====
def _phase3_init_files_schema():
    stmts = [
        """CREATE TABLE IF NOT EXISTS files(
            id INTEGER PRIMARY KEY,
            org_id TEXT NOT NULL,
            owner_id TEXT NOT NULL,
            entity TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            path TEXT NOT NULL,
            bytes INTEGER NOT NULL,
            checksum TEXT NOT NULL,
            created_at TEXT NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_files_entity ON files(org_id, entity, entity_id)"
    ]
    apply_ddl(stmts, name="phase3_files_v1")

_phase3_init_files_schema()

def _sha256_bytes(data: bytes) -> str:
    import hashlib
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()

def _file_store_path(org_id, owner_id, entity, entity_id, filename):
    import os
    from pathlib import Path
    safe_parts = [str(org_id), str(owner_id), str(entity), str(entity_id)]
    base = Path("data") / "files"
    for part in safe_parts:
        part = part.replace("..", "_").replace("/", "_").replace("\\", "_")
        base = base / part
    base.mkdir(parents=True, exist_ok=True)
    return str(base / filename)

def save_upload(upload, org_id, owner_id, entity, entity_id):
    """
    upload: Streamlit UploadedFile or tuple(name, bytes) or file-like with read() and name.
    Returns: file row dict.
    Dedupe: if row exists for same org, entity, id, name, checksum then skip write and return existing.
    """
    import os
    conn = get_db()
    cur = conn.cursor()

    name = None
    data = None
    if hasattr(upload, "name") and hasattr(upload, "read"):
        name = upload.name
        data = upload.read()
    elif isinstance(upload, tuple) and len(upload) == 2:
        name, data = upload
    elif hasattr(upload, "read") and hasattr(upload, "name"):
        name = upload.name
        data = upload.read()
    else:
        raise ValueError("Unsupported upload type")

    if isinstance(data, str):
        data = data.encode("utf-8")
    if not isinstance(data, (bytes, bytearray)):
        raise ValueError("Upload data must be bytes")

    checksum = _sha256_bytes(data)
    size_bytes = len(data)

    cur.execute(
        "SELECT id, path, checksum FROM files WHERE org_id=? AND entity=? AND entity_id=? AND name=? AND checksum=?",
        (str(org_id), str(entity), int(entity_id), str(name), checksum),
    )
    row = cur.fetchone()
    if row:
        fid, path, _ = row
        return {"id": fid, "path": path, "name": name, "bytes": size_bytes, "checksum": checksum}

    fpath = _file_store_path(org_id, owner_id, entity, entity_id, name)

    try:
        if os.path.exists(fpath):
            with open(fpath, "rb") as rf:
                if _sha256_bytes(rf.read()) == checksum:
                    pass
                else:
                    with open(fpath, "wb") as wf:
                        wf.write(data)
        else:
            with open(fpath, "wb") as wf:
                wf.write(data)
    except Exception as ex:
        raise

    cur.execute(
        "INSERT INTO files(org_id, owner_id, entity, entity_id, name, path, bytes, checksum, created_at) VALUES(?,?,?,?,?,?,?,?, datetime('now'))",
        (str(org_id), str(owner_id), str(entity), int(entity_id), str(name), fpath, size_bytes, checksum),
    )
    fid = cur.lastrowid
    return {"id": fid, "path": fpath, "name": name, "bytes": size_bytes, "checksum": checksum}

def list_entity_files(org_id, entity, entity_id):
    cur = get_db().cursor()
    cur.execute(
        "SELECT id, name, path, bytes, checksum, created_at FROM files WHERE org_id=? AND entity=? AND entity_id=? ORDER BY id DESC",
        (str(org_id), str(entity), int(entity_id)),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]
# ==== PHASE 3 PERSIST END ====


# ==== PHASE 2 PERSIST START ====
class StaleEditError(Exception):
    pass

def save_row(table, data, where_id, where_version):
    """
    Optimistic update. Increments version. Raises StaleEditError when no rows updated.
    """
    conn = get_db()
    cur = conn.cursor()
    data = dict(data or {})
    data["version"] = int(where_version) + 1
    cols = list(data.keys())
    set_clause = ", ".join([f"{c}=?" for c in cols])
    args = [data[c] for c in cols] + [int(where_id), int(where_version)]
    sql = f"UPDATE {table} SET {set_clause} WHERE id=? AND version=?"
    cur = conn.execute(sql, args)
    if cur.rowcount == 0:
        raise StaleEditError("stale edit")
    return cur.rowcount

def q_update_guarded(table, data, where_id, where_version):
    return save_row(table, data, where_id, where_version)

def ensure_phase2_columns(tables):
    """
    Ensure each named table has org_id, owner_id, version. Best effort with try blocks.
    """
    conn = get_db()
    cur = conn.cursor()
    for t in tables:
        try: cur.execute(f"ALTER TABLE {t} ADD COLUMN org_id TEXT")
        except Exception: pass
        try: cur.execute(f"ALTER TABLE {t} ADD COLUMN owner_id TEXT")
        except Exception: pass
        try: cur.execute(f"ALTER TABLE {t} ADD COLUMN version INTEGER DEFAULT 0")
        except Exception: pass
    try: conn.commit()
    except Exception: pass

# Call once for commonly used tables. Add or remove names as needed.
try:
    ensure_phase2_columns(["opportunities","proposals","rfqs","pricing","vendors","notices"])
except Exception:
    pass
# ==== PHASE 2 PERSIST END ====



# ==== PHASE 4 PERSIST START ====
# Cache discipline: use st.cache_data only for network requests and pure transforms.
# Authoritative DB reads are not cached.

try:
    import streamlit as st
except Exception:
    pass

# Cached network fetch for SAM; wraps existing fetch_notices for naming parity.
if 'fetch_sam' not in globals():
    try:
        @st.cache_data(ttl=900, show_spinner=False)
        def fetch_sam(filters: dict, page: int, page_size: int, org_id=None, user_id=None):
            return fetch_notices(filters, page, page_size, org_id=org_id, user_id=user_id)
    except Exception:
        # If streamlit not available at import, define a pass-through
        def fetch_sam(filters: dict, page: int, page_size: int, org_id=None, user_id=None):
            return fetch_notices(filters, page, page_size, org_id=org_id, user_id=user_id)

# Direct DB getters without cache
def get_proposal(pid: int):
    try:
        row = q_select("SELECT * FROM proposals WHERE id=? AND org_id=?", (int(pid), current_org_id()), one=True, require_org=False)
        return row
    except Exception:
        return None

# UI state helpers keep filters, page, selections only in session_state.
def ui_get(key: str, default):
    try:
        import streamlit as st
        return st.session_state.setdefault(key, default)
    except Exception:
        return default

def ui_set(key: str, value):
    try:
        import streamlit as st
        st.session_state[key] = value
    except Exception:
        pass

def ensure_view_state_on_mount():
    """Load transient UI selections from session_state. Authoritative data is read from DB on demand."""
    # Example keys; extend as needed
    _ = ui_get("sam_filters", {})
    _ = ui_get("sam_page", 1)
    _ = ui_get("selected_opportunity_id", None)
    _ = ui_get("selected_proposal_id", None)
ensure_view_state_on_mount()
# ==== PHASE 4 PERSIST END ====


# === SAM PHASE 1 START ===

# SAM PHASE 1: ingest core, schema, UI, and state. All behind feature flags.
import datetime
from typing import Dict, Any, List, Optional, Tuple

def _sam_phase1_schema():
    conn = get_db()
    ddls = [
        """CREATE TABLE IF NOT EXISTS notices(
            id INTEGER PRIMARY KEY,
            sam_notice_id TEXT NOT NULL,
            notice_type TEXT NOT NULL,
            title TEXT NOT NULL,
            agency TEXT,
            naics TEXT,
            psc TEXT,
            set_aside TEXT,
            place_city TEXT,
            place_state TEXT,
            posted_at TEXT,
            due_at TEXT,
            status TEXT,
            url TEXT,
            last_fetched_at TEXT
        );""",
        """CREATE UNIQUE INDEX IF NOT EXISTS ux_notices_notice_id ON notices(sam_notice_id);""",
        """CREATE TABLE IF NOT EXISTS notice_files(
            id INTEGER PRIMARY KEY,
            notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
            file_name TEXT,
            file_url TEXT,
            checksum TEXT,
            bytes INTEGER,
            created_at TEXT
        );""",
        """CREATE UNIQUE INDEX IF NOT EXISTS ux_notice_files ON notice_files(notice_id, file_url);""",
        """CREATE TABLE IF NOT EXISTS notice_status(
            user_id TEXT NOT NULL,
            notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
            state TEXT NOT NULL CHECK(state IN ('saved','dismissed')),
            ts TEXT NOT NULL,
            UNIQUE(user_id, notice_id)
        );""",
        """CREATE TABLE IF NOT EXISTS user_prefs(
            user_id TEXT PRIMARY KEY,
            sam_page_size INTEGER DEFAULT 50,
            email_default_recipients TEXT
        );""",
        """CREATE TABLE IF NOT EXISTS pipeline_deals(
            id INTEGER PRIMARY KEY,
            user_id TEXT NOT NULL,
            notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
            stage TEXT DEFAULT 'Lead',
            created_at TEXT NOT NULL,
            UNIQUE(user_id, notice_id)
        );""",
        """CREATE INDEX IF NOT EXISTS idx_pipeline_user ON pipeline_deals(user_id);""",
        """CREATE INDEX IF NOT EXISTS idx_notices_due_at ON notices(due_at);""",
        """CREATE INDEX IF NOT EXISTS idx_notices_naics ON notices(naics);""",
        """CREATE INDEX IF NOT EXISTS idx_notices_psc ON notices(psc);""",
        """CREATE INDEX IF NOT EXISTS idx_notices_agency ON notices(agency);"""
    ]
    try:
        apply_ddl(ddls, name="sam_phase1_schema")
    except Exception:
        # fallback if apply_ddl is not available
        cur = conn.cursor()
        for ddl in ddls:
            cur.execute(ddl)
        conn.commit()

def _sam_client():
    api_key = get_secret("sam", "key")
    base = "https://api.sam.gov/opportunities/v2"
    factory = st.session_state.get("api_client_factory", create_api_client)
    # retries=2, timeout=10, ttl=900
    return factory(base, api_key=api_key, timeout=10, retries=2, ttl=900)

# Map UI filters to SAM API params (best effort)
def _build_sam_query(filters: Dict[str, Any], page: int, page_size: int) -> Tuple[str, Dict[str, Any]]:
    # Endpoint path and params
    path = "search"
    params: Dict[str, Any] = {}
    if filters.get("keywords"):
        params["q"] = filters["keywords"]
    types = filters.get("types") or []
    if types:
        params["notice_type"] = ",".join(types)
    naics = filters.get("naics") or []
    if naics:
        params["naics"] = ",".join(naics)
    psc = filters.get("psc") or []
    if psc:
        params["psc"] = ",".join(psc)
    if filters.get("agency"):
        params["agency"] = filters["agency"]
    if filters.get("place_city"):
        params["place"] = filters["place_city"]
    if filters.get("place_state"):
        params["state"] = filters["place_state"]
    if filters.get("posted_enabled"):
        # Expect ISO yyyy-mm-dd
        if filters.get("posted_from"):
            params["postedFrom"] = filters["posted_from"]
        if filters.get("posted_to"):
            params["postedTo"] = filters["posted_to"]
    # Pagination per SAM: offset/limit
    params["offset"] = max(0, page) * max(1, page_size)
    params["limit"] = max(1, page_size)
    params["api_key"] = get_secret("sam", "key") or ""
    return path, params

@st.cache_data(ttl=900, show_spinner=False)
def fetch_notices(filters: Dict[str, Any], page: int, page_size: int) -> Dict[str, Any]:
    """Fetch public opportunities. Dedup client side. Cached 15 minutes."""
    client = _sam_client()
    # Aggregate until we reach target size to satisfy page_size option
    collected: List[Dict[str, Any]] = []
    cur_page = page
    seen_ids = set()
    for _ in range(5):  # cap to avoid runaway
        path, params = _build_sam_query(filters, cur_page, page_size)
        # Use client.get on path relative to base
        try:
            data = client["get"](path, params=params)
        except Exception as ex:
            eid = log_json("error", "sam_fetch_failed", error=str(ex))
            return {"items": [], "error_id": eid, "page": page, "page_size": page_size, "total": 0}
        # Normalize expected shape
        items = []
        total = None
        if isinstance(data, dict):
            # SAM returns 'results' or 'opportunitiesData' depending on endpoint version
            items = data.get("results") or data.get("opportunitiesData") or []
            total = data.get("totalRecords") or data.get("total") or None
        elif isinstance(data, list):
            items = data
        else:
            items = []
        for it in items:
            # derive a stable id
            nid = (
                it.get("noticeId")
                or it.get("solicitationNumber")
                or it.get("id")
                or it.get("notice_id")
            )
            if not nid:
                continue
            if nid in seen_ids:
                continue
            seen_ids.add(nid)
            collected.append(it)
            if len(collected) >= page_size:
                break
        if len(collected) >= page_size:
            break
        cur_page += 1
    return {"items": collected, "page": page, "page_size": page_size, "total": total or len(collected)}

def upsert_notice(notice: Dict[str, Any], files: Optional[List[Dict[str, Any]]] = None) -> int:
    """Insert or update a notice by sam_notice_id. Returns local notice id."""
    conn = get_db()
    cur = conn.cursor()
    sam_id = str(
        notice.get("noticeId") or notice.get("notice_id") or notice.get("solicitationNumber") or notice.get("id")
    )
    if not sam_id:
        raise ValueError("missing sam_notice_id")
    # Map fields
    title = notice.get("title") or notice.get("subject") or notice.get("noticeTitle") or "Untitled"
    ntype = notice.get("type") or notice.get("noticeType") or notice.get("notice_type") or "Unknown"
    agency = notice.get("agency") or notice.get("department") or notice.get("organizationName")
    naics = ",".join(notice.get("naicsCodes", []) or notice.get("naics", "").split(",")) if isinstance(notice.get("naicsCodes"), list) else str(notice.get("naics") or "")
    psc = ",".join(notice.get("pscCodes", []) or notice.get("psc", "").split(",")) if isinstance(notice.get("pscCodes"), list) else str(notice.get("psc") or "")
    set_aside = notice.get("setAside") or notice.get("set_aside")
    place_city = notice.get("placeOfPerformanceCity") or notice.get("place_city")
    place_state = notice.get("placeOfPerformanceState") or notice.get("place_state")
    posted_at = notice.get("postedDate") or notice.get("publishDate") or notice.get("posted_at")
    due_at = notice.get("responseDate") or notice.get("dueDate") or notice.get("due_at")
    status = notice.get("status") or notice.get("active") or ""
    url = notice.get("uiLink") or notice.get("url") or ""
    now = datetime.datetime.utcnow().isoformat()
    # Upsert
    cur.execute("SELECT id FROM notices WHERE sam_notice_id=?", (sam_id,))
    row = cur.fetchone()
    if row:
        nid = row[0]
        cur.execute(
            """UPDATE notices SET notice_type=?, title=?, agency=?, naics=?, psc=?, set_aside=?,
                   place_city=?, place_state=?, posted_at=?, due_at=?, status=?, url=?, last_fetched_at=?
               WHERE id=?""",
            (ntype, title, agency, naics, psc, set_aside, place_city, place_state,
             posted_at, due_at, status, url, now, nid)
        )
    else:
        cur.execute(
            """INSERT INTO notices(sam_notice_id, notice_type, title, agency, naics, psc, set_aside,
                   place_city, place_state, posted_at, due_at, status, url, last_fetched_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (sam_id, ntype, title, agency, naics, psc, set_aside, place_city, place_state,
             posted_at, due_at, status, url, now)
        )
        nid = cur.lastrowid
    # Files metadata
    if files:
        for f in files:
            fname = f.get("name") or f.get("file_name") or ""
            furl = f.get("url") or f.get("file_url") or ""
            checksum = f.get("checksum")
            fbytes = f.get("bytes")
            created = now
            if not furl:
                continue
            try:
                cur.execute(
                    """INSERT OR IGNORE INTO notice_files(notice_id, file_name, file_url, checksum, bytes, created_at)
                           VALUES(?,?,?,?,?,?)""",
                    (nid, fname, furl, checksum, fbytes, created)
                )
            except Exception:
                pass
    conn.commit()
    return int(nid)

def list_notices(filters: Dict[str, Any], page: int, page_size: int, current_user_id: Optional[str], show_hidden: bool=False, order_by: str="posted_at DESC"):
    conn = get_db()
    cur = conn.cursor()
    # Build WHERE
    where = []
    params: List[Any] = []
    if filters.get("keywords"):
        where.append("(title LIKE ? OR agency LIKE ?)")
        kw = f"%{filters['keywords']}%"
        params.extend([kw, kw])
    if filters.get("types"):
        qs = ",".join(["?"] * len(filters["types"]))
        where.append(f"notice_type IN ({qs})")
        params.extend(filters["types"])
    if filters.get("naics"):
        parts = filters["naics"]
        for code in parts:
            where.append("naics LIKE ?")
            params.append(f"%{code}%")
    if filters.get("psc"):
        for code in filters["psc"]:
            where.append("psc LIKE ?")
            params.append(f"%{code}%")
    if filters.get("agency"):
        where.append("agency LIKE ?")
        params.append(f"%{filters['agency']}%")
    if filters.get("place_city"):
        where.append("place_city LIKE ?")
        params.append(f"%{filters['place_city']}%")
    if filters.get("place_state"):
        where.append("place_state = ?")
        params.append(filters["place_state"])
    if filters.get("posted_enabled"):
        if filters.get("posted_from"):
            where.append("(posted_at >= ?)")
            params.append(filters["posted_from"])
        if filters.get("posted_to"):
            where.append("(posted_at <= ?)")
            params.append(filters["posted_to"])
    if not show_hidden and current_user_id:
        where.append("""id NOT IN (
            SELECT notice_id FROM notice_status WHERE user_id=?
        )""")
        params.append(current_user_id)
    wh = ("WHERE " + " AND ".join(where)) if where else ""
    # Pagination
    limit = max(1, int(page_size))
    offset = max(0, int(page)) * limit
    sql = f"""
        SELECT
            n.id, n.sam_notice_id, n.notice_type, n.title, n.agency, n.naics, n.psc,
            n.set_aside, n.place_city, n.place_state, n.posted_at, n.due_at, n.status, n.url,
            EXISTS(SELECT 1 FROM pipeline_deals pd WHERE pd.notice_id=n.id AND pd.user_id=?) AS starred
        FROM notices n
        {wh}
        ORDER BY {order_by}
        LIMIT ? OFFSET ?
    """
    rows = cur.execute(sql, [current_user_id] + params + [limit, offset]).fetchall()
    # Convert
    cols = ["id","sam_notice_id","notice_type","title","agency","naics","psc","set_aside",
            "place_city","place_state","posted_at","due_at","status","url","starred"]
    items = [dict(zip(cols, r)) for r in rows]
    # total rough count for pager
    total_sql = f"SELECT COUNT(*) FROM notices n {wh}"
    total = cur.execute(total_sql, params).fetchone()[0]
    return {"items": items, "page": page, "page_size": page_size, "total": total}

def set_notice_state(user_id: str, notice_id: int, state: str):
    conn = get_db()
    cur = conn.cursor()
    ts = datetime.datetime.utcnow().isoformat()
    cur.execute("""INSERT INTO notice_status(user_id, notice_id, state, ts)
                 VALUES(?,?,?,?)
                 ON CONFLICT(user_id, notice_id) DO UPDATE SET state=excluded.state, ts=excluded.ts""", (user_id, notice_id, state, ts))
    conn.commit()


def upsert_opportunity_from_notice(user_id: str, notice_id: int) -> None:
    conn = get_db(); cur = conn.cursor()
    # Pull notice with SAM fields
    nr = cur.execute("select id, sam_notice_id, title, agency, naics, psc, place_city, place_state, posted_at, due_at, notice_type, url from notices where id=?", (notice_id,)).fetchone()
    if not nr:
        return
    nid, sam_notice_id, title, agency, naics, psc, place_city, place_state, posted_at, due_at, notice_type, url = nr
    place_of_performance = ", ".join([x for x in [place_city, place_state] if x])
    # Upsert into opportunities. Treat sam_notice_id as unique driver.
    exists = cur.execute("select id from opportunities where sam_notice_id=? limit 1", (sam_notice_id,)).fetchone()
    if exists:
        cur.execute("update opportunities set title=?, agency=?, naics=?, psc=?, place_of_performance=?, response_due=?, posted=?, type=?, url=?, source=COALESCE(source,'sam_star') where sam_notice_id=?",
                    (title, agency, naics, psc, place_of_performance, due_at, posted_at, notice_type, url, sam_notice_id))
    else:
        cur.execute("""insert into opportunities
            (sam_notice_id, title, agency, naics, psc, place_of_performance, response_due, posted, type, url, attachments_json, status, source)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'New', 'sam_star')""",
            (sam_notice_id, title, agency, naics, psc, place_of_performance, due_at, posted_at, notice_type, url))
    conn.commit()
def toggle_pipeline_star(user_id: str, notice_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pipeline_deals WHERE user_id=? AND notice_id=?", (user_id, notice_id))
    if cur.fetchone():
        cur.execute("DELETE FROM pipeline_deals WHERE user_id=? AND notice_id=?", (user_id, notice_id))
        conn.commit()
        return False
    cur.execute("INSERT OR IGNORE INTO pipeline_deals(user_id, notice_id, stage, created_at) VALUES(?,?, 'Lead', ?)", (user_id, notice_id, datetime.datetime.utcnow().isoformat()))
    conn.commit()
    return True

def get_user_page_size(user_id: str) -> int:
    conn = get_db()
    cur = conn.cursor()
    row = cur.execute("SELECT sam_page_size FROM user_prefs WHERE user_id=?", (user_id,)).fetchone()
    if row and row[0]:
        return int(row[0])
    # default 50
    cur.execute("INSERT OR IGNORE INTO user_prefs(user_id, sam_page_size) VALUES(?,?)", (user_id, 50))
    conn.commit()
    return 50

def set_user_page_size(user_id: str, size: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""INSERT INTO user_prefs(user_id, sam_page_size)
                   VALUES(?,?)
                   ON CONFLICT(user_id) DO UPDATE SET sam_page_size=excluded.sam_page_size""", (user_id, int(size)))
    conn.commit()

def _sam_phase1_filters_panel():
    st.subheader("SAM Watch")
    ff = feature_flags()
    st.caption("Filters")
    filt = st.session_state.setdefault("sam_filters", {})
    c1, c2 = st.columns([3, 2])
    with c1:
        filt["keywords"] = st.text_input("Keywords", value=filt.get("keywords", ""))
    with c2:
        types = ["Solicitation","Combined Synopsis or Solicitation","Presolicitation","Sources Sought"]
        filt["types"] = st.multiselect("Notice types", options=types, default=filt.get("types", ["Solicitation","Combined Synopsis or Solicitation","Presolicitation","Sources Sought"]))
    c3, c4 = st.columns(2)
    with c3:
        filt["naics"] = st.multiselect("NAICS", options=filt.get("naics_options", []), default=filt.get("naics", []), help="Type to add codes")
        filt["psc"] = st.multiselect("PSC", options=filt.get("psc_options", []), default=filt.get("psc", []))
    with c4:
        filt["agency"] = st.text_input("Agency contains", value=filt.get("agency", ""))
        stt = st.selectbox("State", options=[""] + [
            "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
        ], index= ([""] + ["AL"]).index(filt.get("place_state","")) if filt.get("place_state") else 0)
        filt["place_state"] = stt or None
        filt["place_city"] = st.text_input("City", value=filt.get("place_city", ""))
    # Posted window control, off by default
    filt["posted_enabled"] = st.checkbox("Limit by posted date", value=filt.get("posted_enabled", False))
    if filt["posted_enabled"]:
        c5, c6 = st.columns(2)
        with c5:
            filt["posted_from"] = st.text_input("Posted from (YYYY-MM-DD)", value=filt.get("posted_from",""))
        with c6:
            filt["posted_to"] = st.text_input("Posted to (YYYY-MM-DD)", value=filt.get("posted_to",""))
    # Buttons
    b1, b2, b3 = st.columns([1,1,4])
    do_search = False
    with b1:
        if st.button("Search"):
            do_search = True
    with b2:
        if st.button("Reset"):
            st.session_state["sam_filters"] = {}
            st.session_state["sam_page"] = 0
            do_search = True
    # Show hidden toggle
    show_hidden = st.checkbox("Show hidden saved/dismissed", value=st.session_state.get("sam_show_hidden", False))
    st.session_state["sam_show_hidden"] = show_hidden
    return filt, do_search, show_hidden

def _sam_phase1_results_grid():
    # Ensure schema
    _sam_phase1_schema()
    # Current user
    user_id = st.session_state.get("user_id") or st.session_state.get("current_user_id")
    # Page size
    ff = feature_flags()
    size = 50
    if user_id and ff.get("sam_page_size", False):
        size = get_user_page_size(user_id)
    # Top right page size selector
    cols_top = st.columns([4,1])
    with cols_top[1]:
        if ff.get("sam_page_size", False) and user_id:
            new_size = st.selectbox("Page size", options=[25,50,100], index=[25,50,100].index(size))
            if new_size != size:
                set_user_page_size(user_id, int(new_size))
                size = int(new_size)
    # Fetch and upsert on demand
    filt = st.session_state.get("sam_filters", {})
    page = int(st.session_state.get("sam_page", 0) or 0)
    # Aggregate from API only when searching or first load with filters set
    if st.session_state.get("sam_ingested_page") != page or st.session_state.get("sam_ingested_filters") != filt:
        data = fetch_notices(filt, page, size)
        for it in data.get("items", []):
            files = it.get("attachments") or it.get("files") or []
            try:
                upsert_notice(it, files)
            except Exception as ex:
                log_json("error", "upsert_notice_failed", error=str(ex))
        st.session_state["sam_ingested_page"] = page
        st.session_state["sam_ingested_filters"] = dict(filt)
    # List from DB
    res = list_notices(filt, page, size, user_id, show_hidden=st.session_state.get("sam_show_hidden", False))
    items = res["items"]
    # Table
    if not items:
        st.info("No results.")
        return
    import math
    total_pages = max(1, math.ceil(res["total"] / max(1, size)))
    for row in items:
        with st.container(border=True):
            c1, c2, c3, c4, c5 = st.columns([1.2, 4, 2.2, 2.2, 1.6])
            with c1:
                st.caption(row.get("notice_type") or "")
                if feature_flags().get("pipeline_star", False) and user_id:
                    starred = bool(row.get("starred"))
                    label = "★" if starred else "☆"
                    if st.button(label, key=f"star_{row['id']}"):
                        new_state = toggle_pipeline_star(user_id, int(row["id"]))
                        # reflect immediately
                        row["starred"] = new_state
            with c2:
                title = row.get("title") or ""
                if row.get("url"):
                    st.markdown(f"[{title}]({row['url']})")
                else:
                    st.write(title)
                st.caption(row.get("agency") or "")
            with c3:
                st.caption(f"NAICS: {row.get('naics') or ''}")
                st.caption(f"PSC: {row.get('psc') or ''}")
            with c4:
                st.caption(f"Posted: {row.get('posted_at') or ''}")
                st.caption(f"Due: {row.get('due_at') or ''}")
                if row.get("set_aside"):
                    st.caption(f"Set-aside: {row.get('set_aside')}")
            with c5:
                # Actions
                if user_id:
                    if st.button("Save", key=f"save_{row['id']}"):
                        set_notice_state(user_id, int(row["id"]), "saved")
                    if st.button("Dismiss", key=f"dismiss_{row['id']}"):
                        set_notice_state(user_id, int(row["id"]), "dismissed")
    # Pager
    cpa, cpb, cpc = st.columns([1,2,1])
    with cpa:
        if st.button("Prev") and page > 0:
            st.session_state["sam_page"] = page - 1
            _safe_rerun()
    with cpb:
        st.write(f"Page {page + 1} of {total_pages}")
    with cpc:
        if st.button("Load more"):
            st.session_state["sam_page"] = page + 1
            _safe_rerun()

def render_sam_watch_phase1_ui():
    st.write("")  # spacing
    filt, do_search, show_hidden = _sam_phase1_filters_panel()
    if do_search:
        st.session_state["sam_page"] = 0
        st.session_state["sam_ingested_page"] = None
        st.session_state["sam_ingested_filters"] = None
    _sam_phase1_results_grid()

# Override shell dispatch for SAM when sam_ingest_core flag is True
def _sam_phase1_maybe_render_shell_override():
    ff = feature_flags()
    if not ff.get("workspace_enabled", False):
        return False
    route = get_route()
    page = route.get("page")
    if page == "sam" and ff.get("sam_ingest_core", False):
        try:
            render_sam_watch_phase1_ui()
            return True
        except Exception as ex:
            st.warning(f"SAM Watch error: {ex}")
            return True
    return False

# Wrap the existing _maybe_render_shell to prefer our SAM UI when flag is on
try:
    _orig__maybe_render_shell = _maybe_render_shell  # type: ignore[name-defined]
except Exception:
    _orig__maybe_render_shell = None

def _maybe_render_shell():
    if _sam_phase1_maybe_render_shell_override():
        return
    if _orig__maybe_render_shell:
        try:
            _orig__maybe_render_shell()
            return
        except Exception as ex:
            st.warning(f"Shell error: {ex}")
            return
# End SAM PHASE 1

# === SAM PHASE 1 END ===


# === SAM PHASE 2 RFP ANALYZER START ===
import threading as _threading
import hashlib as _hashlib
import datetime as _dt
import json as _json
import traceback as _traceback
import io

def _rfp_phase2_schema():
    conn = get_db()
    ddls = [
        """CREATE TABLE IF NOT EXISTS rfp_summaries(
            id INTEGER PRIMARY KEY,
            notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
            version_hash TEXT NOT NULL,
            summary_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(notice_id, version_hash)
        );""",
        """CREATE TABLE IF NOT EXISTS file_parses(
            id INTEGER PRIMARY KEY,
            notice_file_id INTEGER NOT NULL REFERENCES notice_files(id) ON DELETE CASCADE,
            checksum TEXT NOT NULL,
            parsed_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(notice_file_id, checksum)
        );""",
        """CREATE VIRTUAL TABLE IF NOT EXISTS rfp_chunks USING fts5(
            notice_id UNINDEXED,
            file_name,
            page UNINDEXED,
            text
        );""",
    ]
    try:
        apply_ddl(ddls, name="sam_phase2_rfp_schema")
    except Exception:
        cur = conn.cursor()
        for ddl in ddls:
            try:
                cur.execute(ddl)
            except Exception:
                pass
        conn.commit()

def _sha256_bytes(b: bytes) -> str:
    h = _hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def _download_bytes(url: str, timeout: int = 20) -> bytes:
    import requests
    resp = requests.get(url, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"download_failed status={resp.status_code}")
    return resp.content

def _parse_pdf_bytes(b: bytes) -> list:
    pages = []
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(io.BytesIO(b))
        for i, p in enumerate(reader.pages, start=1):
            try:
                t = p.extract_text() or ""
            except Exception:
                t = ""
            pages.append({"page": i, "text": t})
        return pages
    except Exception:
        return [{"page": 1, "text": ""}]

def _parse_docx_bytes(b: bytes) -> list:
    pages = []
    try:
        import docx
        doc = docx.Document(io.BytesIO(b))
        buff = []
        page_num = 1
        for para in doc.paragraphs:
            text = para.text.strip()
            if text:
                buff.append(text)
            if len(buff) >= 25:
                pages.append({"page": page_num, "text": "\n".join(buff)})
                buff = []
                page_num += 1
        if buff:
            pages.append({"page": page_num, "text": "\n".join(buff)})
        if not pages:
            pages = [{"page": 1, "text": ""}]
        return pages
    except Exception:
        return [{"page": 1, "text": ""}]

def _detect_filetype(name: str, content: bytes) -> str:
    n = (name or "").lower()
    if n.endswith(".pdf"):
        return "pdf"
    if n.endswith(".docx"):
        return "docx"
    if content[:4] == b"%PDF":
        return "pdf"
    return "bin"

def _rfp_summary_schema() -> dict:
    return {
        "type": "object",
        "required": ["brief","factors","clauses","dates","forms","milestones","sources"],
        "properties": {
            "brief": {"type":"string"},
            "factors": {"type":"array","items":{"type":"string"}},
            "clauses": {"type":"array","items":{"type":"string"}},
            "dates": {"type":"object"},
            "forms": {"type":"array","items":{"type":"string"}},
            "milestones": {"type":"array","items":{"type":"string"}},
            "sources": {"type":"array","items":{"type":"object","properties":{
                "file_name":{"type":"string"},
                "page":{"type":["integer","null"]},
                "text":{"type":"string"}
            }}},
        }
    }

def _rfp_validate_summary(payload: dict) -> bool:
    try:
        if not isinstance(payload, dict): return False
        for k in ["brief","factors","clauses","dates","forms","milestones","sources"]:
            if k not in payload: return False
        if not isinstance(payload["brief"], str): return False
        for k in ["factors","clauses","forms","milestones","sources"]:
            if not isinstance(payload[k], list): return False
        if not isinstance(payload["dates"], dict): return False
        return True
    except Exception:
        return False

def _extract_summary_from_pages(pages: list, file_name: str) -> dict:
    text_all = "\n".join([p.get("text", "") or "" for p in pages])[:200000]
    def find_lines(keyword):
        hits = []
        for p in pages:
            t = p.get("text", "") or ""
            if not t: continue
            for line in t.splitlines():
                if keyword.lower() in line.lower():
                    hits.append({"file_name": file_name, "page": p.get("page"), "text": line.strip()})
        return hits[:20]
    sources = []
    factors = [s["text"] for s in find_lines("Section M")] + [s["text"] for s in find_lines("Evaluation")]
    clauses = [s["text"] for s in find_lines("Section L")] + [s["text"] for s in find_lines("Clause")]
    forms = [s["text"] for s in find_lines("SF 1449")] + [s["text"] for s in find_lines("SF1449")] + [s["text"] for s in find_lines("SF 1442")] + [s["text"] for s in find_lines("SF1442")]
    dates = {}
    for kw in ["proposal due","offers due","due date","closing date","response date"]:
        hits = find_lines(kw)
        if hits:
            dates.setdefault("due", hits[0]["text"])
            sources.extend(hits[:3])
            break
    milestones = [s["text"] for s in find_lines("site visit")] + [s["text"] for s in find_lines("questions due")]
    sources.extend([{ "file_name": file_name, "page": p.get("page"), "text": (p.get("text") or "")[:120]} for p in pages[:1]])
    brief = (text_all[:500].strip() or "Summary not available.")
    payload = {
        "brief": brief,
        "factors": factors[:10],
        "clauses": clauses[:10],
        "dates": dates,
        "forms": forms[:10],
        "milestones": milestones[:10],
        "sources": sources[:20],
    }
    if not _rfp_validate_summary(payload):
        payload = {"brief": brief, "factors": [], "clauses": [], "dates": {}, "forms": [], "milestones": [], "sources": []}
    return payload

def _ensure_chunk_index():
    conn = get_db()
    try:
        conn.execute("SELECT 1 FROM rfp_chunks LIMIT 1")
        return True
    except Exception:
        return False

def parse_rfp(notice_id: int) -> dict:
    _rfp_phase2_schema()
    conn = get_db()
    cur = conn.cursor()
    files = cur.execute("SELECT id, file_name, file_url, checksum FROM notice_files WHERE notice_id=?", (notice_id,)).fetchall()
    if not files:
        raise RuntimeError("no_files_for_notice")
    combined = _hashlib.sha256()
    file_payloads = []
    for fid, fname, furl, cks in files:
        try:
            b = _download_bytes(furl)
        except Exception as ex:
            eid = log_json("error", "rfp_download_failed", notice_id=notice_id, url=furl, error=str(ex))
            raise RuntimeError(f"download_failed error_id={eid}")
        checksum = _sha256_bytes(b)
        combined.update(checksum.encode())
        if not cks or cks != checksum:
            try:
                cur.execute("UPDATE notice_files SET checksum=? WHERE id=?", (checksum, fid))
                conn.commit()
            except Exception:
                pass
        ftype = _detect_filetype(fname or "", b)
        if ftype == "pdf":
            pages = _parse_pdf_bytes(b)
        elif ftype == "docx":
            pages = _parse_docx_bytes(b)
        else:
            pages = [{"page": 1, "text": ""}]
        parsed = {"file_id": fid, "file_name": fname, "checksum": checksum, "pages": pages}
        file_payloads.append(parsed)
        try:
            cur.execute("""INSERT OR IGNORE INTO file_parses(notice_file_id, checksum, parsed_json, created_at)
                           VALUES(?,?,?,?)""", (fid, checksum, _json.dumps(parsed), _dt.datetime.utcnow().isoformat()))
            conn.commit()
        except Exception:
            pass
        if _ensure_chunk_index():
            try:
                for p in pages[:500]:
                    conn.execute("INSERT INTO rfp_chunks(notice_id, file_name, page, text) VALUES(?,?,?,?)",
                                 (notice_id, fname or "", int(p.get("page") or 0), p.get("text") or ""))
                conn.commit()
            except Exception:
                pass
    version_hash = combined.hexdigest()
    row = cur.execute("SELECT id, summary_json FROM rfp_summaries WHERE notice_id=? AND version_hash=?",
                      (notice_id, version_hash)).fetchone()
    if row:
        return {"cached": True, "summary": _json.loads(row[1]), "version_hash": version_hash}
    primary = next((p for p in file_payloads if p.get("pages")), file_payloads[0])
    summary = _extract_summary_from_pages(primary.get("pages") or [], primary.get("file_name") or "")
    valid = _rfp_validate_summary(summary)
    if not valid:
        eid = log_json("error", "rfp_summary_validation_failed", notice_id=notice_id)
    cur.execute("""INSERT OR IGNORE INTO rfp_summaries(notice_id, version_hash, summary_json, created_at)
                   VALUES(?,?,?,?)""", (notice_id, version_hash, _json.dumps(summary), _dt.datetime.utcnow().isoformat()))
    conn.commit()
    return {"cached": False, "summary": summary, "version_hash": version_hash}

def _start_parse_worker(notice_id: int):
    st.session_state["rfp_worker_status"] = {"state": "running", "notice_id": notice_id, "progress": 0, "error_id": None}
    def _run():
        try:
            res = parse_rfp(int(notice_id))
            st.session_state["rfp_worker_status"] = {"state": "done", "notice_id": notice_id, "progress": 100, "result": res}
        except Exception as ex:
            eid = log_json("error", "rfp_parse_failed", notice_id=notice_id, error=str(ex))
            st.session_state["rfp_worker_status"] = {"state": "error", "notice_id": notice_id, "progress": 0, "error_id": eid}
    t = _threading.Thread(target=_run, daemon=True)
    t.start()

def _rfp_query_chunks(notice_id: int, query: str) -> list:
    conn = get_db()
    try:
        cur = conn.execute("SELECT file_name, page, text FROM rfp_chunks WHERE notice_id=? AND rfp_chunks MATCH ? LIMIT 5", (notice_id, query,))
        rows = cur.fetchall()
        return [{"file_name": r[0], "page": r[1], "text": r[2]} for r in rows]
    except Exception:
        rows = conn.execute("SELECT parsed_json FROM file_parses fp JOIN notice_files nf ON nf.id=fp.notice_file_id WHERE nf.notice_id=?", (notice_id,)).fetchall()
        hits = []
        for (pj,) in rows:
            try:
                obj = _json.loads(pj)
                fname = obj.get("file_name", "")
                for p in obj.get("pages", [])[:50]:
                    if query.lower() in (p.get("text", "").lower()):
                        hits.append({"file_name": fname, "page": p.get("page"), "text": (p.get("text", "")[:240])})
                        if len(hits) >= 5: break
                if len(hits) >= 5: break
            except Exception:
                continue
        return hits

def _rfp_panel_ui(notice_id: int):
    _rfp_phase2_schema()
    st.session_state.setdefault("rfp_panel_open", True)
    st.session_state["current_notice_id"] = notice_id
    with st.sidebar:
        st.markdown("## RFP Analyzer")
        st.caption(f"Notice #{notice_id}")
        status = st.session_state.get("rfp_worker_status")
        if st.button("Close panel"):
            st.session_state["rfp_panel_open"] = False
        conn = get_db()
        cur = conn.cursor()
        row = cur.execute("SELECT version_hash, summary_json, created_at FROM rfp_summaries WHERE notice_id=? ORDER BY created_at DESC LIMIT 1", (notice_id,)).fetchone()
        has_cache = bool(row)
        if st.button("Run parse") or (not has_cache and not status):
            _start_parse_worker(notice_id)
        status = st.session_state.get("rfp_worker_status")
        if status and status.get("state") == "running":
            st.info("Parsing in background...")
            st.progress(int(status.get("progress", 10)))
        elif status and status.get("state") == "error":
            st.error(f"Parser failed. error_id={status.get('error_id')}")
        row = cur.execute("SELECT version_hash, summary_json, created_at FROM rfp_summaries WHERE notice_id=? ORDER BY created_at DESC LIMIT 1", (notice_id,)).fetchone()
        if row:
            vs, sjson, created = row
            try:
                data = _json.loads(sjson)
            except Exception:
                data = {}
            st.subheader("Brief")
            st.write(data.get("brief", ""))
            st.subheader("Factors")
            for f in data.get("factors", []):
                st.write("• " + f)
            st.subheader("Clauses")
            for c in data.get("clauses", []):
                st.write("• " + c)
            st.subheader("Dates")
            for k, v in (data.get("dates") or {}).items():
                st.write(f"{k}: {v}")
            st.subheader("Forms")
            for f in data.get("forms", []):
                st.write("• " + f)
            st.subheader("Milestones")
            for m in data.get("milestones", []):
                st.write("• " + m)
        st.markdown("---")
        st.subheader("Q and A")
        q = st.text_input("Ask a question about this RFP")
        if st.button("Answer") and q:
            hits = _rfp_query_chunks(int(notice_id), q)
            if not hits:
                st.info("No matching text in parsed files.")
            else:
                st.write("Top matches from files:")
                for h in hits:
                    st.caption(f"{h.get('file_name')} p.{h.get('page')}")
                    st.write(h.get("text", ""))

def _inject_rfp_button_into_row(row, user_id):
    if not feature_flags().get("rfp_analyzer_panel", False):
        return False
    key = f"rfp_{row['id']}"
    if st.button("Ask RFP Analyzer", key=key):
        st.session_state["rfp_panel_open"] = True
        st.session_state["current_notice_id"] = int(row["id"])
        return True
    return False

try:
    _orig__sam_phase1_results_grid = _sam_phase1_results_grid
except Exception:
    _orig__sam_phase1_results_grid = None

def _sam_phase2_results_grid_wrapper():
    opened = False
    if _orig__sam_phase1_results_grid:
        _orig__sam_phase1_results_grid()
        try:
            user_id = st.session_state.get("user_id") or st.session_state.get("current_user_id")
            filt = st.session_state.get("sam_filters", {})
            page = int(st.session_state.get("sam_page", 0) or 0)
            size = 50
            res = list_notices(filt, page, size, user_id, show_hidden=st.session_state.get("sam_show_hidden", False))
            for row in res.get("items", []):
                with st.expander(f"RFP tools for #{row.get('id')} · {row.get('title','')[:80]}", expanded=False):
                    if _inject_rfp_button_into_row(row, user_id):
                        opened = True
        except Exception as ex:
            st.warning(f"RFP toolbar error: {ex}")
    if st.session_state.get("rfp_panel_open") and st.session_state.get("current_notice_id"):
        try:
            _rfp_panel_ui(int(st.session_state.get("current_notice_id")))
        except Exception as ex:
            st.warning(f"RFP panel error: {ex}")

try:
    _orig__maybe_render_shell_phase2 = _maybe_render_shell
except Exception:
    _orig__maybe_render_shell_phase2 = None

def _maybe_render_shell():
    if _sam_phase1_maybe_render_shell_override():
        if feature_flags().get("rfp_analyzer_panel", False):
            _sam_phase2_results_grid_wrapper()
        return
    if _orig__maybe_render_shell_phase2:
        _orig__maybe_render_shell_phase2()
# === SAM PHASE 2 RFP ANALYZER END ===


# === SAM PHASE 3 AMENDMENTS START ===
import hashlib as _h3
import datetime as _dt3
import json as _json3

def _sam_phase3_schema():
    conn = get_db()
    ddls = [
        """CREATE TABLE IF NOT EXISTS notice_versions(
            id INTEGER PRIMARY KEY,
            notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
            fetched_at TEXT NOT NULL,
            version_hash TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );""",
        """CREATE INDEX IF NOT EXISTS idx_notice_versions_notice ON notice_versions(notice_id);""",
        """CREATE TABLE IF NOT EXISTS amendments(
            id INTEGER PRIMARY KEY,
            notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
            amend_number TEXT,
            posted_at TEXT,
            url TEXT,
            version_hash TEXT NOT NULL,
            summary TEXT
        );""",
        """CREATE INDEX IF NOT EXISTS idx_amendments_notice ON amendments(notice_id);""",
        """CREATE TABLE IF NOT EXISTS watchers(
            id INTEGER PRIMARY KEY,
            notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
            user_id TEXT NOT NULL,
            notify_email TEXT,
            active INTEGER NOT NULL DEFAULT 1
        );""",
    ]
    try:
        apply_ddl(ddls, name="sam_phase3_amendments_schema")
    except Exception:
        cur = conn.cursor()
        for ddl in ddls:
            try: cur.execute(ddl)
            except Exception: pass
        conn.commit()
    # Ensure notices.compliance_state column
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(notices)").fetchall()]
        if "compliance_state" not in cols:
            conn.execute("ALTER TABLE notices ADD COLUMN compliance_state TEXT DEFAULT 'Unreviewed'")
            conn.commit()
    except Exception:
        pass

def _norm_core_fields(notice: dict) -> dict:
    keys = [
        "sam_notice_id","notice_type","title","agency","naics","psc","set_aside",
        "place_city","place_state","posted_at","due_at","status","url"
    ]
    out = {}
    for k in keys:
        out[k] = notice.get(k) if isinstance(notice, dict) else None
    return out

def _compute_version_hash(core: dict, files: list) -> str:
    parts = []
    for k in sorted(core.keys()):
        v = core.get(k)
        parts.append(f"{k}={v}")
    # file urls sorted
    urls = []
    for f in files or []:
        u = f.get("file_url") or f.get("url") or ""
        urls.append(u)
    urls.sort()
    parts.extend([f"file={u}" for u in urls])
    raw = "|".join(parts).encode("utf-8", errors="ignore")
    return _h3.sha256(raw).hexdigest()

def _collect_files_for_notice(conn, notice_id: int) -> list:
    rows = conn.execute("SELECT file_name, file_url, bytes FROM notice_files WHERE notice_id=?", (notice_id,)).fetchall()
    return [{"file_name": r[0], "file_url": r[1], "bytes": r[2]} for r in rows]

def _diff_versions(prev: dict, curr: dict) -> dict:
    # Compare core fields and files lists
    diff = {"changed_fields": [], "files_added": [], "files_removed": [], "files_changed": []}
    core_keys = [
        "title","agency","naics","psc","set_aside","place_city","place_state","posted_at","due_at","status","url"
    ]
    for k in core_keys:
        if (prev.get(k) or "") != (curr.get(k) or ""):
            diff["changed_fields"].append({"field": k, "from": prev.get(k), "to": curr.get(k)})
    prev_files = { (f.get("file_url") or ""): f for f in prev.get("files", []) }
    curr_files = { (f.get("file_url") or ""): f for f in curr.get("files", []) }
    for u in curr_files.keys() - prev_files.keys():
        diff["files_added"].append(curr_files[u])
    for u in prev_files.keys() - curr_files.keys():
        diff["files_removed"].append(prev_files[u])
    for u in curr_files.keys() & prev_files.keys():
        pb = prev_files[u].get("bytes")
        cb = curr_files[u].get("bytes")
        if pb is not None and cb is not None and int(pb or 0) != int(cb or 0):
            diff["files_changed"].append({"file_url": u, "bytes_from": pb, "bytes_to": cb})
    return diff

def _record_notice_version(notice_id: int, core_now: dict):
    conn = get_db()
    cur = conn.cursor()
    files_now = _collect_files_for_notice(conn, notice_id)
    payload = {"core": core_now, "files": files_now}
    vhash = _compute_version_hash(core_now, files_now)
    row = cur.execute("SELECT version_hash, payload_json FROM notice_versions WHERE notice_id=? ORDER BY id DESC LIMIT 1", (notice_id,)).fetchone()
    last_hash = row[0] if row else None
    if last_hash is None:
        cur.execute("INSERT INTO notice_versions(notice_id, fetched_at, version_hash, payload_json) VALUES(?,?,?,?)",
                    (notice_id, _dt3.datetime.utcnow().isoformat(), vhash, _json3.dumps(payload)))
        conn.commit()
        return None  # first version, no amendment
    if last_hash == vhash:
        return None  # no change
    # Insert new version and create amendment
    cur.execute("INSERT INTO notice_versions(notice_id, fetched_at, version_hash, payload_json) VALUES(?,?,?,?)",
                (notice_id, _dt3.datetime.utcnow().isoformat(), vhash, _json3.dumps(payload)))
    # Build summary from diff
    try:
        prev_payload = _json3.loads(row[1]) if row and row[1] else {"core": {}, "files": []}
    except Exception:
        prev_payload = {"core": {}, "files": []}
    d = _diff_versions({**prev_payload.get('core', {}), 'files': prev_payload.get('files', [])}, {**core_now, 'files': files_now})
    changed = [c['field'] for c in d.get('changed_fields', [])]
    parts = []
    if changed: parts.append("fields: " + ", ".join(changed[:6]))
    if d.get('files_added'): parts.append(f"files+:{len(d['files_added'])}")
    if d.get('files_removed'): parts.append(f"files-:{len(d['files_removed'])}")
    if d.get('files_changed'): parts.append(f"filesΔ:{len(d['files_changed'])}")
    summary = "; ".join(parts) if parts else "content changed"
    cur.execute("INSERT INTO amendments(notice_id, amend_number, posted_at, url, version_hash, summary) VALUES(?,?,?,?,?,?)",
                (notice_id, None, core_now.get('posted_at'), core_now.get('url'), vhash, summary))
    # Flip compliance_state
    try:
        cur.execute("UPDATE notices SET compliance_state='Needs review' WHERE id=?", (notice_id,))
    except Exception:
        pass
    conn.commit()
    return vhash

# Override upsert_notice to attach versioning
try:
    _orig_upsert_notice_p3 = upsert_notice
except Exception:
    _orig_upsert_notice_p3 = None

def upsert_notice(notice: dict, files: Optional[list] = None) -> int:
    # Reuse Phase 1 mapping then record version when amend_tracking flag is on
    conn = get_db()
    cur = conn.cursor()
    sam_id = str(
        notice.get("noticeId") or notice.get("notice_id") or notice.get("solicitationNumber") or notice.get("id")
    )
    if not sam_id:
        raise ValueError("missing sam_notice_id")
    title = notice.get("title") or notice.get("subject") or notice.get("noticeTitle") or "Untitled"
    ntype = notice.get("type") or notice.get("noticeType") or notice.get("notice_type") or "Unknown"
    agency = notice.get("agency") or notice.get("department") or notice.get("organizationName")
    naics = ",".join(notice.get("naicsCodes", []) or notice.get("naics", "").split(",")) if isinstance(notice.get("naicsCodes"), list) else str(notice.get("naics") or "")
    psc = ",".join(notice.get("pscCodes", []) or notice.get("psc", "").split(",")) if isinstance(notice.get("pscCodes"), list) else str(notice.get("psc") or "")
    set_aside = notice.get("setAside") or notice.get("set_aside")
    place_city = notice.get("placeOfPerformanceCity") or notice.get("place_city")
    place_state = notice.get("placeOfPerformanceState") or notice.get("place_state")
    posted_at = notice.get("postedDate") or notice.get("publishDate") or notice.get("posted_at")
    due_at = notice.get("responseDate") or notice.get("dueDate") or notice.get("due_at")
    status = notice.get("status") or notice.get("active") or ""
    url = notice.get("uiLink") or notice.get("url") or ""
    now = _dt3.datetime.utcnow().isoformat()
    cur.execute("SELECT id FROM notices WHERE sam_notice_id=?", (sam_id,))
    row = cur.fetchone()
    if row:
        nid = row[0]
        cur.execute(
            """UPDATE notices SET notice_type=?, title=?, agency=?, naics=?, psc=?, set_aside=?,
                   place_city=?, place_state=?, posted_at=?, due_at=?, status=?, url=?, last_fetched_at=?
               WHERE id=?""",
            (ntype, title, agency, naics, psc, set_aside, place_city, place_state,
             posted_at, due_at, status, url, now, nid)
        )
    else:
        cur.execute(
            """INSERT INTO notices(sam_notice_id, notice_type, title, agency, naics, psc, set_aside,
                   place_city, place_state, posted_at, due_at, status, url, last_fetched_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (sam_id, ntype, title, agency, naics, psc, set_aside, place_city, place_state,
             posted_at, due_at, status, url, now)
        )
        nid = cur.lastrowid
    # Files metadata insert-ignore
    if files:
        for f in files:
            fname = f.get("name") or f.get("file_name") or ""
            furl = f.get("url") or f.get("file_url") or ""
            checksum = f.get("checksum")
            fbytes = f.get("bytes")
            created = now
            if not furl:
                continue
            try:
                cur.execute(
                    """INSERT OR IGNORE INTO notice_files(notice_id, file_name, file_url, checksum, bytes, created_at)
                           VALUES(?,?,?,?,?,?)""",
                    (nid, fname, furl, checksum, fbytes, created)
                )
            except Exception:
                pass
    conn.commit()
    # Versioning if flag enabled
    try:
        if feature_flags().get("amend_tracking", False):
            _sam_phase3_schema()
            core = {
                "sam_notice_id": sam_id,
                "notice_type": ntype,
                "title": title,
                "agency": agency,
                "naics": naics,
                "psc": psc,
                "set_aside": set_aside,
                "place_city": place_city,
                "place_state": place_state,
                "posted_at": posted_at,
                "due_at": due_at,
                "status": status,
                "url": url,
            }
            _record_notice_version(int(nid), core)
    except Exception as ex:
        log_json("error", "version_track_failed", notice_id=int(nid), error=str(ex))
    return int(nid)

# Override list_notices to include amended/compliance_state flags
try:
    _orig_list_notices_p3 = list_notices
except Exception:
    _orig_list_notices_p3 = None

def list_notices(filters: dict, page: int, page_size: int, current_user_id: Optional[str], show_hidden: bool=False, order_by: str="posted_at DESC"):
    conn = get_db()
    cur = conn.cursor()
    where = []
    params = []
    if filters.get("keywords"):
        where.append("(title LIKE ? OR agency LIKE ?)")
        kw = f"%{filters['keywords']}%"
        params.extend([kw, kw])
    if filters.get("types"):
        qs = ",".join(["?"] * len(filters["types"]))
        where.append(f"notice_type IN ({qs})")
        params.extend(filters["types"])
    if filters.get("naics"):
        for code in filters["naics"]:
            where.append("naics LIKE ?")
            params.append(f"%{code}%")
    if filters.get("psc"):
        for code in filters["psc"]:
            where.append("psc LIKE ?")
            params.append(f"%{code}%")
    if filters.get("agency"):
        where.append("agency LIKE ?")
        params.append(f"%{filters['agency']}%")
    if filters.get("place_city"):
        where.append("place_city LIKE ?")
        params.append(f"%{filters['place_city']}%")
    if filters.get("place_state"):
        where.append("place_state = ?")
        params.append(filters["place_state"])
    if filters.get("posted_enabled"):
        if filters.get("posted_from"):
            where.append("(posted_at >= ?)")
            params.append(filters["posted_from"])
        if filters.get("posted_to"):
            where.append("(posted_at <= ?)")
            params.append(filters["posted_to"])
    if not show_hidden and current_user_id:
        where.append("""id NOT IN (SELECT notice_id FROM notice_status WHERE user_id=?)""")
        params.append(current_user_id)
    wh = ("WHERE " + " AND ".join(where)) if where else ""
    limit = max(1, int(page_size))
    offset = max(0, int(page)) * limit
    sql = f"""
        SELECT
            n.id, n.sam_notice_id, n.notice_type, n.title, n.agency, n.naics, n.psc,
            n.set_aside, n.place_city, n.place_state, n.posted_at, n.due_at, n.status, n.url,
            n.compliance_state,
            EXISTS(SELECT 1 FROM pipeline_deals pd WHERE pd.notice_id=n.id AND pd.user_id=?) AS starred,
            EXISTS(SELECT 1 FROM amendments a WHERE a.notice_id=n.id) AS amended
        FROM notices n
        {wh}
        ORDER BY {order_by}
        LIMIT ? OFFSET ?
    """
    rows = cur.execute(sql, [current_user_id] + params + [limit, offset]).fetchall()
    cols = ["id","sam_notice_id","notice_type","title","agency","naics","psc","set_aside","place_city","place_state","posted_at","due_at","status","url","compliance_state","starred","amended"]
    items = [dict(zip(cols, r)) for r in rows]
    total = cur.execute(f"SELECT COUNT(*) FROM notices n {wh}", params).fetchone()[0]
    return {"items": items, "page": page, "page_size": page_size, "total": total}

# Diff computation for workspace tab
def compute_notice_diff(notice_id: int) -> dict:
    _sam_phase3_schema()
    conn = get_db()
    cur = conn.cursor()
    vers = cur.execute("SELECT version_hash, payload_json, fetched_at FROM notice_versions WHERE notice_id=? ORDER BY id DESC LIMIT 2", (notice_id,)).fetchall()
    if not vers:
        return {"has_diff": False, "msg": "No versions."}
    if len(vers) == 1:
        return {"has_diff": False, "msg": "Only one version recorded."}
    new = _json3.loads(vers[0][1])
    old = _json3.loads(vers[1][1])
    d = _diff_versions({**old.get('core', {}), 'files': old.get('files', [])}, {**new.get('core', {}), 'files': new.get('files', [])})
    return {"has_diff": True, "diff": d, "new_hash": vers[0][0], "old_hash": vers[1][0], "new_time": vers[0][2], "old_time": vers[1][2]}

# Add Diff tab to opportunity workspace when flag enabled
def render_diff(opp_id: int):
    import streamlit as st
    st.subheader("Diff")
    info = compute_notice_diff(int(opp_id))
    if not info.get("has_diff"):
        st.info(info.get("msg"))
        return
    d = info.get("diff", {})
    if d.get("changed_fields"):
        st.markdown("**Changed fields**")
        for c in d["changed_fields"]:
            st.write(f"{c['field']}: '{c.get('from')}' → '{c.get('to')}'")
    st.markdown("**Files**")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.caption("Added")
        for f in d.get("files_added", []):
            st.write(f.get("file_name") or f.get("file_url"))
    with c2:
        st.caption("Removed")
        for f in d.get("files_removed", []):
            st.write(f.get("file_name") or f.get("file_url"))
    with c3:
        st.caption("Changed size")
        for f in d.get("files_changed", []):
            st.write(f["file_url"])
    st.markdown("---")
    if st.button("Mark reviewed"):
        conn = get_db()
        conn.execute("UPDATE notices SET compliance_state='Reviewed' WHERE id=?", (int(opp_id),))
        conn.commit()
        st.success("Marked reviewed.")

# Override workspace renderer to include Diff tab when enabled
try:
    _orig__render_opportunity_workspace_p3 = _render_opportunity_workspace
except Exception:
    _orig__render_opportunity_workspace_p3 = None

def _render_opportunity_workspace():
    import streamlit as st
    ff = feature_flags()
    if not ff.get("workspace_enabled", False):
        return
    route = get_route()
    if route.get("page") != "opportunity":
        return
    opp_id = route.get("opp_id")
    title = _get_notice_title_from_db(opp_id)
    st.header(title)
    tabs = ["details","analyzer","compliance","proposal","pricing","vendors","submission"]
    labels = ["Details","Analyzer","Compliance","Proposal","Pricing","VendorsRFQ","Submission"]
    if ff.get("amend_tracking", False):
        tabs.append("diff"); labels.append("Diff")
    current = route.get("tab") or "details"
    if current not in tabs:
        current = "details"
    idx = tabs.index(current)
    try:
        sel = st.radio("Workspace", options=list(range(len(tabs))), index=idx, format_func=lambda i: labels[i], horizontal=True)
    except TypeError:
        sel = st.radio("Workspace", options=list(range(len(tabs))), index=idx, format_func=lambda i: labels[i])
    new_tab = tabs[sel]
    if new_tab != current:
        route_to("opportunity", opp_id=opp_id, tab=new_tab)
        st.stop()
    if current == "details":
        render_details(opp_id)
    elif current == "analyzer":
        render_analyzer(opp_id)
    elif current == "compliance":
        render_compliance(opp_id)
    elif current == "proposal":
        render_proposal(opp_id)
    elif current == "pricing":
        render_pricing(opp_id)
    elif current == "vendors":
        render_vendorsrfq(opp_id)
    elif current == "submission":
        render_submission(opp_id)
    elif current == "diff":
        render_diff(opp_id)

# Augment SAM grid with Amendment badge and open Diff
try:
    _orig__sam_phase2_results_grid_wrapper_p3 = _sam_phase2_results_grid_wrapper
except Exception:
    _orig__sam_phase2_results_grid_wrapper_p3 = None

def _sam_phase3_results_grid_wrapper():
    if _orig__sam_phase2_results_grid_wrapper_p3:
        _orig__sam_phase2_results_grid_wrapper_p3()
    # Add amendment badges row-wise
    try:
        user_id = st.session_state.get("user_id") or st.session_state.get("current_user_id")
        filt = st.session_state.get("sam_filters", {})
        page = int(st.session_state.get("sam_page", 0) or 0)
        size = 50
        res = list_notices(filt, page, size, user_id, show_hidden=st.session_state.get("sam_show_hidden", False))
        for row in res.get("items", []):
            if row.get("amended") and row.get("compliance_state") == "Needs review" and feature_flags().get("amend_tracking", False):
                with st.expander(f"Amendment detected for #{row.get('id')} · click to review", expanded=False):
                    cols = st.columns([1,3,1])
                    with cols[0]:
                        st.caption("Amendment")
                    with cols[1]:
                        st.write(row.get("title",""))
                    with cols[2]:
                        if st.button("Open Diff", key=f"open_diff_{row['id']}"):
                            if feature_flags().get("workspace_enabled", False):
                                route_to("opportunity", opp_id=int(row["id"]), tab="diff")
                                st.stop()
                            else:
                                st.session_state["selected_notice_id"] = int(row["id"])
                                st.session_state["diff_tab_open"] = True
    except Exception as ex:
        st.warning(f"Amendment badge error: {ex}")

# Wire the wrapper into shell
try:
    _orig__maybe_render_shell_p3 = _maybe_render_shell
except Exception:
    _orig__maybe_render_shell_p3 = None

def _maybe_render_shell():
    if _sam_phase1_maybe_render_shell_override():
        if feature_flags().get("rfp_analyzer_panel", False):
            try: _sam_phase2_results_grid_wrapper()
            except Exception: pass
        if feature_flags().get("amend_tracking", False):
            _sam_phase3_results_grid_wrapper()
        return
    if _orig__maybe_render_shell_p3:
        _orig__maybe_render_shell_p3()
# === SAM PHASE 3 AMENDMENTS END ===


# === DEV AUTOLOGIN START ===
def _dev_autologin():
    try:
        # Only when no identity present
        if not st.session_state.get('org_id') and not st.session_state.get('user_id'):
            conn = get_db()
            orgs = conn.execute('SELECT id FROM orgs').fetchall()
            if len(orgs) == 1:
                org_id = orgs[0][0]
                user = conn.execute('SELECT id, role FROM users WHERE org_id=? LIMIT 1', (org_id,)).fetchone()
                if user:
                    st.session_state['org_id'] = org_id
                    st.session_state['user_id'] = user[0]
                    st.session_state['role'] = user[1]
    except Exception:
        pass

try:
    _dev_autologin()
except Exception:
    pass
# === DEV AUTOLOGIN END ===



# === NAV + SHELL FIX START ===
import streamlit as st

# Ensure router helpers exist
def get_route():
    try:
        qp = st.query_params
    except AttributeError:
        qp = st.experimental_get_query_params()
    page = (qp.get("page") if isinstance(qp.get("page"), str) else (qp.get("page",[None])[0] if qp.get("page") else None)) or st.session_state.get("route_page") or "dashboard"
    opp = qp.get("opp") if isinstance(qp.get("opp"), str) else (qp.get("opp",[None])[0] if qp.get("opp") else None)
    tab = qp.get("tab") if isinstance(qp.get("tab"), str) else (qp.get("tab",[None])[0] if qp.get("tab") else None)
    st.session_state["route_page"] = page
    st.session_state["route_opp_id"] = opp
    st.session_state["route_tab"] = tab
    return {"page": page, "opp_id": opp, "tab": tab}

def route_to(page, opp_id=None, tab=None):
    try:
        st.query_params.clear()
        if page: st.query_params["page"] = page
        if opp_id is not None: st.query_params["opp"] = str(opp_id)
        if tab: st.query_params["tab"] = tab
    except AttributeError:
        params = {}
        if page: params["page"] = page
        if opp_id is not None: params["opp"] = str(opp_id)
        if tab: params["tab"] = tab
        st.experimental_set_query_params(**params)
    st.session_state["route_page"] = page
    st.session_state["route_opp_id"] = opp_id
    st.session_state["route_tab"] = tab
    _safe_rerun()

# Feature flags accessor
def feature_flags():
    return st.session_state.setdefault("feature_flags", {})

# RTM flag default
try:
    ff = feature_flags()
    ff.setdefault('rtm', False)
except Exception:
    pass


# Observability flag default
try:
    ff = feature_flags()
    ff.setdefault('observability', False)
except Exception:
    pass


# Compliance relock and email flags defaults
try:
    ff = feature_flags()
    ff.setdefault('compliance_relock', False)
    ff.setdefault('email_enabled', False)
except Exception:
    pass


# Compliance gate v2 flag default
try:
    ff = feature_flags()
    ff.setdefault('compliance_gate_v2', False)
except Exception:
    pass


# Compliance v2 flag default
try:
    ff = feature_flags()
    ff.setdefault('compliance_v2', False)
except Exception:
    pass


# Default feature flags for compliance gate
try:
    ff = feature_flags()
    ff.setdefault('compliance_gate', False)
except Exception:
    pass


# --- Page renderers ---
def _render_nav():
    ff = feature_flags()
    st.sidebar.header("Navigation")
    pages = [
        ("dashboard","Dashboard"),
        ("sam","SAM Watch"),
        ("pipeline","Pipeline"),
        ("outreach","Outreach"),
        ("library","Library"),
        ("admin","Admin"),
    ]
    for key,label in pages:
        if st.sidebar.button(label, key=f"nav_{key}"):
            route_to(key)

def render_dashboard():
    st.subheader("Dashboard")
    st.info("Dashboard placeholder.")

def render_pipeline():
    st.subheader("Pipeline")
    conn = get_db()
    uid = st.session_state.get("user_id")
    try:
        rows = conn.execute("SELECT pd.id, n.title, n.agency, n.due_at FROM pipeline_deals pd JOIN notices n ON n.id=pd.notice_id WHERE pd.user_id=? ORDER BY pd.created_at DESC", (uid,)).fetchall() if uid else []
    except Exception:
        rows = []
    if not rows:
        st.caption("No pipeline items yet.")
        return
    for r in rows:
        with st.container(border=True):
            st.write(r[1])
            st.caption(f"Agency: {r[2]}  Due: {r[3]}")

def render_outreach():
    st.subheader("Outreach")
    st.caption("Outreach placeholder.")

def render_library():
    st.subheader("Library")
    st.caption("Library placeholder.")

def render_admin():
    st.subheader("Admin")
    ff = feature_flags()
    st.caption("Feature flags")
    cols = st.columns(3)
    keys = ["workspace_enabled","sam_ingest_core","sam_page_size","pipeline_star","rfp_analyzer_panel","amend_tracking"]
    for i,k in enumerate(keys):
        with cols[i % 3]:
            ff[k] = st.checkbox(k, value=ff.get(k, False))
    st.session_state["feature_flags"] = ff
    st.caption("Identity")
    st.write({k: st.session_state.get(k) for k in ["org_id","user_id","role"]})
    st.caption("Routing")
    st.write(get_route())

def render_sam():
    # If Phase 1 SAM UI is present and flagged on, use it
    if feature_flags().get("sam_ingest_core", False):
        try:
            render_sam_watch_phase1_ui()  # defined in SAM Phase 1
            return
        except Exception as ex:
            st.warning(f"SAM UI error: {ex}")
    st.subheader("SAM Watch")
    st.info("Enable 'sam_ingest_core' in Admin to use the new SAM UI.")

# Main shell
def _render_shell():
    _render_nav()
    route = get_route()
    page = route.get("page") or "dashboard"
    if page == "dashboard":
        render_dashboard()
    elif page == "sam":
        render_sam()
    elif page == "pipeline":
        render_pipeline()
    elif page == "outreach":
        render_outreach()
    elif page == "library":
        render_library()
    elif page == "admin":
        render_admin()
    elif page == "opportunity":
        # Use workspace if available
        try:
            _render_opportunity_workspace()
        except Exception as ex:
            st.warning(f"Workspace error: {ex}")
    else:
        st.write("Unknown page:", page)

# Execute shell last
try:
    _render_shell()
except Exception as ex:
    st.error(f"Shell failed: {ex}")
# === NAV + SHELL FIX END ===


# === RFP PHASE 1 START ===
import datetime as _dt
import json as _json
import hashlib as _hash

def _rfp_phase1_schema_ddl():
    conn = get_db()
    ddls = [
        """CREATE TABLE IF NOT EXISTS rfp_schema_versions(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            version TEXT NOT NULL,
            schema_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(name, version)
        );""",
        """CREATE TABLE IF NOT EXISTS rfp_json(
            id INTEGER PRIMARY KEY,
            notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
            schema_name TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            version_hash TEXT NOT NULL,
            data_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(notice_id, version_hash)
        );"""
    ]
    try:
        apply_ddl(ddls, name="rfp_phase1_schema")
    except Exception:
        cur = conn.cursor()
        for ddl in ddls:
            cur.execute(ddl)
        conn.commit()

def _rfp_phase1_register_schema():
    _rfp_phase1_schema_ddl()
    conn = get_db()
    cur = conn.cursor()
    name = "RFPv1"; ver = "1.0"
    schema = {"type": "object", "required": ["header", "sections", "lm_requirements", "submission"], "properties": {"header": {"type": "object", "required": ["notice_id", "title"], "properties": {"notice_id": {"type": "string"}, "title": {"type": "string"}, "agency": {"type": "string"}, "type": {"type": "string"}, "set_aside": {"type": "string"}, "place": {"type": "string"}, "pocs": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "email": {"type": "string"}, "phone": {"type": "string"}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}}}}, "volumes": {"type": "array", "items": {"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}, "required": {"type": "boolean"}, "page_limit": {"type": "integer"}, "file_type": {"type": "string"}, "font": {"type": "string"}, "spacing": {"type": "string"}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}}, "sections": {"type": "array", "items": {"type": "object", "required": ["key", "title"], "properties": {"key": {"type": "string"}, "title": {"type": "string"}, "parent_volume": {"type": "string"}, "required": {"type": "boolean"}, "page_limit": {"type": "integer"}, "instructions": {"type": "array", "items": {"type": "string"}}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}}, "lm_requirements": {"type": "array", "items": {"type": "object", "required": ["id", "text"], "properties": {"id": {"type": "string"}, "text": {"type": "string"}, "factor": {"type": "string"}, "subfactor": {"type": "string"}, "evaluation_criterion": {"type": "string"}, "must_address": {"type": "array", "items": {"type": "string"}}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}}, "deliverables_forms": {"type": "array", "items": {"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}, "form_no": {"type": "string"}, "fillable": {"type": "boolean"}, "where_to_upload": {"type": "string"}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}}, "submission": {"type": "object", "required": ["due_datetime"], "properties": {"method": {"type": "string"}, "portals": {"type": "array", "items": {"type": "string"}}, "email": {"type": "string"}, "subject_line_format": {"type": "string"}, "due_datetime": {"type": "string"}, "timezone": {"type": "string"}, "copies": {"type": "integer"}, "file_naming_rules": {"type": "string"}, "zip_rules": {"type": "string"}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}, "milestones": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "due_datetime": {"type": "string"}, "origin": {"type": "string"}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}}, "clauses": {"type": "array", "items": {"type": "object", "properties": {"ref": {"type": "string"}, "title": {"type": "string"}, "section": {"type": "string"}, "mandatory": {"type": "boolean"}, "notes": {"type": "string"}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}}, "sow_tasks": {"type": "array", "items": {"type": "object", "properties": {"task_id": {"type": "string"}, "text": {"type": "string"}, "location": {"type": "string"}, "hours_hint": {"type": "number"}, "labor_cats_hint": {"type": "array", "items": {"type": "string"}}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}}, "price_structure": {"type": "object", "properties": {"clins": {"type": "array", "items": {"type": "object", "properties": {"clin": {"type": "string"}, "desc": {"type": "string"}, "uom": {"type": "string"}, "qty_hint": {"type": "number"}, "options": {"type": "string"}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}}, "wage_determinations": {"type": "array", "items": {"type": "object", "properties": {"type": {"type": "string"}, "id": {"type": "string"}, "county_state": {"type": "string"}, "labor_cats": {"type": "array", "items": {"type": "string"}}, "rates": {"type": "string"}, "fringe": {"type": "string"}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}}}}, "past_perf_rules": {"type": "object", "properties": {"count": {"type": "integer"}, "years_back": {"type": "integer"}, "relevance_dims": {"type": "string"}, "format": {"type": "string"}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}, "staffing_rules": {"type": "object", "properties": {"key_personnel": {"type": "string"}, "certs": {"type": "string"}, "clearances": {"type": "string"}, "badging": {"type": "string"}, "training": {"type": "string"}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}, "accessibility_rules": {"type": "object", "properties": {"req_508": {"type": "boolean"}, "pdf_tags": {"type": "boolean"}, "bookmarks": {"type": "boolean"}, "alt_text": {"type": "boolean"}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}, "risks_assumptions": {"type": "array", "items": {"type": "object", "properties": {"risk": {"type": "string"}, "impact": {"type": "string"}, "mitigation": {"type": "string"}, "cite": {"type": "object", "properties": {"file": {"type": "string"}, "page": {"type": "integer"}}}}}}}}
    cur.execute("""INSERT OR IGNORE INTO rfp_schema_versions(name, version, schema_json, created_at)
                 VALUES(?,?,?,?)""", (name, ver, schema, _dt.datetime.utcnow().isoformat()))
    conn.commit()
    import streamlit as st
    st.session_state["rfp_schema_ready"] = True

def _is_iso_datetime_with_tz(s: str) -> bool:
    try:
        if s.endswith("Z"):
            _dt.datetime.fromisoformat(s.replace("Z","+00:00"))
            return True
        _dt.datetime.fromisoformat(s)
        return (s.endswith("Z") or ("+" in s[10:] or "-" in s[10:]))
    except Exception:
        return False

def validate_rfpv1(payload: dict):
    errs = []
    if not isinstance(payload, dict):
        return False, ["payload must be object"]
    for req in ["header","sections","lm_requirements","submission"]:
        if req not in payload: errs.append(f"missing {req}")
    hdr = payload.get("header", {})
    if not isinstance(hdr, dict): errs.append("header must be object")
    else:
        for req in ["notice_id","title"]:
            if req not in hdr: errs.append(f"header.{req} required")

    def _check_items(arr, name, require_cite=True, dt_fields=None):
        if arr is None: return
        if not isinstance(arr, list): errs.append(f"{name} must be array"); return
        for i, it in enumerate(arr):
            if not isinstance(it, dict): errs.append(f"{name}[{i}] must be object"); continue
            if require_cite and "cite" not in it: errs.append(f"{name}[{i}].cite required")
            if "cite" in it:
                c = it["cite"]
                if not isinstance(c, dict): errs.append(f"{name}[{i}].cite must be object")
                else:
                    if "file" not in c or not isinstance(c.get("file"), str) or not c.get("file"): errs.append(f"{name}[{i}].cite.file required")
                    if "page" not in c or not isinstance(c.get("page"), int): errs.append(f"{name}[{i}].cite.page required int")
            if dt_fields:
                for f in dt_fields:
                    if f in it and not _is_iso_datetime_with_tz(it[f]): errs.append(f"{name}[{i}].{f} must be ISO datetime with timezone")

    _check_items(payload.get("volumes"), "volumes", require_cite=True)
    _check_items(payload.get("sections"), "sections", require_cite=True)
    _check_items(payload.get("lm_requirements"), "lm_requirements", require_cite=True)
    _check_items(payload.get("deliverables_forms"), "deliverables_forms", require_cite=True)
    _check_items(payload.get("milestones"), "milestones", require_cite=True, dt_fields=["due_datetime"])
    _check_items(payload.get("clauses"), "clauses", require_cite=True)
    _check_items(payload.get("sow_tasks"), "sow_tasks", require_cite=True)

    sub = payload.get("submission", {})
    if not isinstance(sub, dict): errs.append("submission must be object")
    else:
        if "due_datetime" not in sub: errs.append("submission.due_datetime required")
        else:
            if not _is_iso_datetime_with_tz(sub.get("due_datetime","")): errs.append("submission.due_datetime must be ISO datetime with timezone")
        if "cite" in sub:
            c = sub["cite"]
            if not isinstance(c, dict) or "file" not in c or "page" not in c: errs.append("submission.cite.file and page required when cite present")

    ps = payload.get("price_structure")
    if ps is not None:
        if not isinstance(ps, dict): errs.append("price_structure must be object")
        else:
            _check_items(ps.get("clins"), "price_structure.clins", require_cite=True)
            _check_items(ps.get("wage_determinations"), "price_structure.wage_determinations", require_cite=True)

    for obj_key in ["past_perf_rules","staffing_rules","accessibility_rules"]:
        obj = payload.get(obj_key)
        if obj is not None:
            if not isinstance(obj, dict): errs.append(f"{obj_key} must be object")
            else:
                if any(k for k in obj.keys() if k != "cite") and "cite" not in obj:
                    errs.append(f"{obj_key}.cite required when fields present")

    return (len(errs) == 0), errs

def save_rfp_json(notice_id: int, payload: dict, schema_name: str="RFPv1", schema_version: str="1.0"):
    import streamlit as st
    if not st.session_state.get("feature_flags", {}).get("rfp_schema", False):
        return {"ok": False, "error": "rfp_schema flag disabled"}
    ok, errs = validate_rfpv1(payload)
    if not ok:
        return {"ok": False, "errors": errs}
    conn = get_db()
    cur = conn.cursor()
    data_str = _json.dumps(payload, sort_keys=True, separators=(",",":"))
    vhash = _hash.sha256(data_str.encode("utf-8")).hexdigest()
    cur.execute("""INSERT OR IGNORE INTO rfp_json(notice_id, schema_name, schema_version, version_hash, data_json, created_at)
                 VALUES(?,?,?,?,?,?)""", (int(notice_id), schema_name, schema_version, vhash, data_str, _dt.datetime.utcnow().isoformat()))
    conn.commit()
    return {"ok": True, "version_hash": vhash}

try:
    _rfp_phase1_register_schema()
except Exception as _ex:
    try: log_json("error", "rfp_phase1_register_failed", error=str(_ex))
    except Exception: pass
# === RFP PHASE 1 END ===



# === RFP PHASE 2 START ===
import re as _re
import io as _io
import threading as _thr
import datetime as _dt

def _rfp_p2_feature_on():
    import streamlit as st
    return st.session_state.get("feature_flags", {}).get("rfp_parser", False)

def _rfp_p2_notice_row(nid:int):
    conn = get_db()
    row = conn.execute("SELECT id,sam_notice_id,notice_type,title,agency,set_aside,place_city,place_state FROM notices WHERE id=?", (int(nid),)).fetchone()
    if not row: return None
    cols = ["id","sam_notice_id","notice_type","title","agency","set_aside","place_city","place_state"]
    return dict(zip(cols, row))

_TZMAP = {
    "ET":"+00:00", "EST":"-05:00", "EDT":"-04:00",
    "CT":"-06:00", "CST":"-06:00", "CDT":"-05:00",
    "MT":"-07:00", "MST":"-07:00", "MDT":"-06:00",
    "PT":"-08:00", "PST":"-08:00", "PDT":"-07:00",
    "UTC":"+00:00", "Z":"+00:00"
}

def _rfp_p2_norm_dt(s: str):
    # Try patterns like "Sep 30, 2025 3:00 PM ET" or "09/30/2025 1500 EST"
    try:
        txt = s.strip()
        tz = None
        for k in sorted(_TZMAP.keys(), key=len, reverse=True):
            if k in txt:
                tz = _TZMAP[k]
                txt = txt.replace(k, "").strip()
                break
        # Common formats
        fmts = ["%b %d, %Y %I:%M %p", "%m/%d/%Y %I:%M %p", "%B %d, %Y %I:%M %p", "%m/%d/%Y %H%M", "%Y-%m-%d %H:%M"]
        for f in fmts:
            try:
                dt = _dt.datetime.strptime(txt, f)
                if tz:
                    return dt.isoformat(timespec="minutes") + tz
            except Exception:
                continue
        return None
    except Exception:
        return None

def _rfp_p2_extract(file_name: str, pages: list):
    # Scan for patterns and return structures with cites
    def lines():
        for p in pages:
            t = p.get("text") or ""
            for ln in t.splitlines():
                yield p.get("page"), ln.strip()
    header = {}
    sections = []
    lm_reqs = []
    deliverables = []
    clauses = []
    milestones = []
    submission = {}
    # Heuristics
    for pg, ln in lines():
        low = ln.lower()
        # Section L / M
        if "section l" in low or low.startswith("l."):
            sections.append({"key":"L","title":ln, "cite":{"file":file_name,"page":int(pg)}})
        if "section m" in low or low.startswith("m.") or "evaluation" in low:
            lm_reqs.append({"id":f"M-{pg}", "text":ln, "cite":{"file":file_name,"page":int(pg)}})
        # Clauses references (FAR/DFARS)
        if "far " in low or "dfars" in low:
            clauses.append({"ref": ln, "cite":{"file":file_name,"page":int(pg)}})
        # Forms
        if "sf 1449" in low or "sf 1442" in low or "sf1449" in low or "sf1442" in low:
            deliverables.append({"name": ln, "cite":{"file":file_name,"page":int(pg)}})
        # Page limits/fonts/copies naming rules
        if "page limit" in low or "not exceed" in low and "pages" in low:
            sections.append({"key":"page_limit_hint", "title":ln, "cite":{"file":file_name,"page":int(pg)}})
        if "font" in low or "typeface" in low:
            sections.append({"key":"font_hint", "title":ln, "cite":{"file":file_name,"page":int(pg)}})
        if "copies" in low and ("electronic" in low or "hard" in low):
            submission.setdefault("copies", 0)  # value unknown, retain cite only
            submission["cite"] = {"file":file_name,"page":int(pg)}
        if "file name" in low or "naming convention" in low:
            submission["file_naming_rules"] = ln
            submission["cite"] = {"file":file_name,"page":int(pg)}
        # Due datetime
        if any(k in low for k in ["due", "closing", "offers must be received", "proposal deadline"]):
            iso = _rfp_p2_norm_dt(ln)
            if iso:
                submission["due_datetime"] = iso
                # Try to add timezone token if present in the line
                for token, off in _TZMAP.items():
                    if token in ln:
                        submission["timezone"] = token
                        break
                submission.setdefault("cite", {"file":file_name,"page":int(pg)})
        # Email
        if "@" in ln and ("contact" in low or "submit" in low or "questions" in low):
            submission["email"] = _re.findall(r"[\w\.-]+@[\w\.-]+", ln)[0] if _re.findall(r"[\w\.-]+@[\w\.-]+", ln) else None
            submission.setdefault("cite", {"file":file_name,"page":int(pg)})
        # POC
        if "point of contact" in low or "poc" in low:
            ems = _re.findall(r"[\w\.-]+@[\w\.-]+", ln)
            header.setdefault("pocs", [])
            header["pocs"].append({"name": ln, "email": (ems[0] if ems else None), "cite":{"file":file_name,"page":int(pg)}})
        # Milestones like site visit or questions due
        if "site visit" in low or "questions due" in low:
            iso = _rfp_p2_norm_dt(ln)
            item = {"name": ln, "cite":{"file":file_name,"page":int(pg)}}
            if iso: item["due_datetime"] = iso
            milestones.append(item)
    # Normalize by schema keys
    out = {
        "header": header if header else None,
        "sections": sections or None,
        "lm_requirements": lm_reqs or None,
        "deliverables_forms": deliverables or None,
        "clauses": clauses or None,
        "milestones": milestones or None,
        "submission": submission or None,
    }
    return out

def _rfp_p2_build_json(notice_id: int, file_payloads: list):
    # Compose RFPv1 JSON from file-level extractions
    nrow = _rfp_p2_notice_row(notice_id) or {}
    header = {"notice_id": str(nrow.get("sam_notice_id") or notice_id), "title": nrow.get("title") or ""}
    if nrow.get("agency"): header["agency"] = nrow["agency"]
    if nrow.get("notice_type"): header["type"] = nrow["notice_type"]
    if nrow.get("set_aside"): header["set_aside"] = nrow["set_aside"]
    place = ", ".join([x for x in [nrow.get("place_city"), nrow.get("place_state")] if x])
    if place: header["place"] = place
    sections = []; lm = []; deliver = []; clauses = []; milestones = []; submission = {}
    for fp in file_payloads:
        part = _rfp_p2_extract(fp.get("file_name") or "", fp.get("pages") or [])
        if part.get("header"):
            header.setdefault("pocs", [])
            if part["header"].get("pocs"):
                header["pocs"].extend([p for p in part["header"]["pocs"] if p.get("cite")])
        for key, acc in [("sections", sections), ("lm_requirements", lm), ("deliverables_forms", deliver), ("clauses", clauses), ("milestones", milestones)]:
            if part.get(key):
                acc.extend([x for x in part[key] if x.get("cite")])
        if part.get("submission"):
            # Merge submission fields but keep cites per field via top-level cite
            for k,v in part["submission"].items():
                submission[k] = v
    payload = {"header": header, "sections": sections, "lm_requirements": lm, "submission": submission}
    if deliver: payload["deliverables_forms"] = deliver
    if clauses: payload["clauses"] = clauses
    if milestones: payload["milestones"] = milestones
    return payload

def rfp_parse_and_store(notice_id: int):
    """Download, parse, populate FTS, build RFPv1 JSON, validate and store. Cache by version_hash."""
    if not _rfp_p2_feature_on():
        return {"ok": False, "error": "rfp_parser flag disabled"}
    conn = get_db()
    cur = conn.cursor()
    # Get files
    files = cur.execute("SELECT id, file_name, file_url, checksum FROM notice_files WHERE notice_id=?", (int(notice_id),)).fetchall()
    if not files:
        return {"ok": False, "error": "no_files_for_notice"}
    # Compute combined version hash from checksums or content
    comb = _hashlib.sha256()
    file_payloads = []
    for fid, fname, furl, cks in files:
        # Download
        b = _download_bytes(furl)
        sha = _sha256_bytes(b)
        comb.update(sha.encode())
        if cks != sha:
            try: cur.execute("UPDATE notice_files SET checksum=? WHERE id=?", (sha, fid)); conn.commit()
            except Exception: pass
        # Parse
        ftype = _detect_filetype(fname or "", b)
        if ftype == "pdf":
            pages = _parse_pdf_bytes(b)
        elif ftype == "docx":
            pages = _parse_docx_bytes(b)
        else:
            pages = [{"page":1, "text":""}]
        payload = {"file_id": fid, "file_name": fname or furl.split("/")[-1], "checksum": sha, "pages": pages}
        file_payloads.append(payload)
        # Persist file_parses
        try:
            cur.execute("INSERT OR IGNORE INTO file_parses(notice_file_id, checksum, parsed_json, created_at) VALUES(?,?,?,?)",
                        (fid, sha, _json.dumps(payload), _dt.datetime.utcnow().isoformat()))
            conn.commit()
        except Exception: pass
        # Populate FTS if available
        try:
            conn.execute("SELECT 1 FROM rfp_chunks LIMIT 1")
            for p in pages[:800]:
                conn.execute("INSERT INTO rfp_chunks(notice_id, file_name, page, text) VALUES(?,?,?,?)",
                             (int(notice_id), payload["file_name"], int(p.get("page") or 0), p.get("text") or ""))
            conn.commit()
        except Exception: pass
    version_hash = comb.hexdigest()
    # Cache check in rfp_json
    row = cur.execute("SELECT id FROM rfp_json WHERE notice_id=? AND version_hash=?", (int(notice_id), version_hash)).fetchone()
    if row: return {"ok": True, "cached": True, "version_hash": version_hash}
    # Build analyzer JSON
    data = _rfp_p2_build_json(int(notice_id), file_payloads)
    ok, errs = validate_rfpv1(data)
    if not ok:
        return {"ok": False, "errors": errs}
    # Store via RFP Phase 1 saver to ensure consistent hashing
    res = save_rfp_json(int(notice_id), data, schema_name="RFPv1", schema_version="1.0")
    if not res.get("ok"):
        return res
    return {"ok": True, "cached": False, "version_hash": res.get("version_hash"), "data": data}

def rfp_run_worker(notice_id: int):
    import streamlit as st
    st.session_state["rfp_parser_status"] = {"state":"running","notice_id": int(notice_id), "progress": 0, "error_id": None}
    def _run():
        try:
            st.session_state["rfp_parser_status"]["progress"] = 10
            res = rfp_parse_and_store(int(notice_id))
            if res.get("ok"):
                st.session_state["rfp_parser_status"] = {"state":"done","notice_id": int(notice_id), "progress": 100, "result": res}
            else:
                st.session_state["rfp_parser_status"] = {"state":"error","notice_id": int(notice_id), "progress": 0, "error_id": str(res)}
        except Exception as ex:
            eid = log_json("error","rfp_p2_worker_failed", notice_id=int(notice_id), error=str(ex))
            st.session_state["rfp_parser_status"] = {"state":"error","notice_id": int(notice_id), "progress": 0, "error_id": eid}
    t = _thr.Thread(target=_run, daemon=True); t.start()

# Enhance Analyzer panel with tabs when rfp_parser is on
try:
    _orig_rfp_panel_ui_p2 = _rfp_panel_ui
except Exception:
    _orig_rfp_panel_ui_p2 = None

def _rfp_panel_ui(notice_id: int):
    import streamlit as st
    if not feature_flags().get("rfp_analyzer_panel", False):
        return
    # Run schema DDL to be safe
    try: _rfp_phase1_schema_ddl()
    except Exception: pass
    st.session_state.setdefault("rfp_panel_open", True)
    st.session_state["current_notice_id"] = notice_id
    with st.sidebar:
        st.markdown("## RFP Analyzer")
        st.caption(f"Notice #{notice_id}")
        # Controls
        if _rfp_p2_feature_on():
            if st.button("Run parser"):
                rfp_run_worker(int(notice_id))
        else:
            if st.button("Enable rfp_parser flag in Admin to run parser"):
                pass
        status = st.session_state.get("rfp_parser_status")
        if status and status.get("state") == "running":
            st.info("Parsing..."); st.progress(int(status.get("progress", 10)))
        elif status and status.get("state") == "error":
            st.error(f"Parser error: {status.get('error_id')}")
        # Show latest JSON if present
        conn = get_db(); cur = conn.cursor()
        row = cur.execute("SELECT data_json, created_at FROM rfp_json WHERE notice_id=? ORDER BY id DESC LIMIT 1", (int(notice_id),)).fetchone()
        if _rfp_p2_feature_on() and row:
            data = _json.loads(row[0])
            tabs = st.tabs(["Summary","L and M","Clauses","Forms","Submission","Q and A"])
            with tabs[0]:
                st.write(data.get("header", {}))
                st.caption(f"Created: {row[1]}")
            with tabs[1]:
                for it in data.get("lm_requirements", []):
                    st.write(it.get("text")); c = it.get("cite") or {}; st.caption(f"{c.get('file')} p.{c.get('page')}")
            with tabs[2]:
                for it in data.get("clauses", []):
                    st.write(it.get("ref")); c = it.get("cite") or {}; st.caption(f"{c.get('file')} p.{c.get('page')}")
            with tabs[3]:
                for it in data.get("deliverables_forms", []):
                    st.write(it.get("name")); c = it.get("cite") or {}; st.caption(f"{c.get('file')} p.{c.get('page')}")
            with tabs[4]:
                sub = data.get("submission", {}); st.json(sub)
            with tabs[5]:
                q = st.text_input("Ask a question about this RFP") 
                if st.button("Answer") and q:
                    hits = _rfp_query_chunks(int(notice_id), q)
                    if not hits: st.info("No matches.")
                    else:
                        for h in hits:
                            st.caption(f"{h.get('file_name')} p.{h.get('page')}"); st.write(h.get("text",""))
        elif not row:
            st.caption("No analyzer JSON yet. Run parser.")
# === RFP PHASE 2 END ===




# === TENANCY PHASE 2 START ===
def _tp2_db_has_col(conn, table, col):
    try:
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        return col in cols
    except Exception:
        return False

def _tp2_tables():
    return [
        "notices","notice_files","notice_status","proposals","proposal_sections",
        "price_models","price_lines","vendors","vendor_contacts","vendor_quotes",
        "rfq","rfq_lines","rfq_invites","vendor_portal_tokens","saved_searches",
        "pipeline_deals","required_docs","lm_checklist","signoffs","submissions",
        "submission_files","rfp_json","rfp_summaries","metrics","audit_log",
        "email_accounts","contacts","campaigns","campaign_recipients"
    ]  # rfp_chunks is FTS virtual; skip ALTER

def _tp2_add_columns_and_indexes():
    conn = get_db()
    cur = conn.cursor()
    for t in _tp2_tables():
        try:
            if not _tp2_db_has_col(conn, t, "org_id"):
                cur.execute(f"ALTER TABLE {t} ADD COLUMN org_id TEXT")
            if not _tp2_db_has_col(conn, t, "owner_id"):
                cur.execute(f"ALTER TABLE {t} ADD COLUMN owner_id TEXT")
            if not _tp2_db_has_col(conn, t, "visibility"):
                cur.execute(f"ALTER TABLE {t} ADD COLUMN visibility TEXT DEFAULT 'private' CHECK(visibility IN('private','team','shared'))")
            if not _tp2_db_has_col(conn, t, "version"):
                cur.execute(f"ALTER TABLE {t} ADD COLUMN version INTEGER DEFAULT 0")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{t}_org ON {t}(org_id)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{t}_owner ON {t}(org_id, owner_id)")
        except Exception:
            continue
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_notices_org_sam ON notices(org_id, sam_notice_id)")
    except Exception:
        pass
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_rfp_json_org ON rfp_json(org_id, notice_id, version_hash)")
    except Exception:
        pass
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_pipeline_org_user_notice ON pipeline_deals(org_id, user_id, notice_id)")
    except Exception:
        pass
    conn.commit()

def _tp2_get_default_identity():
    conn = get_db()
    cur = conn.cursor()
    row = cur.execute("SELECT id FROM orgs LIMIT 1").fetchone()
    org_id = row[0] if row else "org-default"
    if not row:
        try:
            cur.execute("INSERT OR IGNORE INTO orgs(id,name,created_at) VALUES(?,?,datetime('now'))", (org_id, "Default Org"))
            conn.commit()
        except Exception:
            pass
    rowu = cur.execute("SELECT id FROM users WHERE org_id=? AND role='Admin' LIMIT 1", (org_id,)).fetchone()
    if not rowu:
        rowu = cur.execute("SELECT id FROM users WHERE org_id=? LIMIT 1", (org_id,)).fetchone()
    owner_id = rowu[0] if rowu else "system"
    return org_id, owner_id

def _tp2_backfill():
    conn = get_db()
    cur = conn.cursor()
    org_id, owner_id = _tp2_get_default_identity()
    for t in _tp2_tables():
        try:
            cur.execute(f"UPDATE {t} SET visibility=COALESCE(visibility,'team') WHERE visibility IS NULL")
            cur.execute(f"UPDATE {t} SET org_id=? WHERE (org_id IS NULL OR org_id='')", (org_id,))
            has_created_by = _tp2_db_has_col(conn, t, "created_by")
            if has_created_by:
                cur.execute(f"UPDATE {t} SET owner_id=COALESCE(owner_id, created_by, ?) WHERE owner_id IS NULL", (owner_id,))
            else:
                cur.execute(f"UPDATE {t} SET owner_id=COALESCE(owner_id, ?) WHERE owner_id IS NULL", (owner_id,))
            cur.execute(f"UPDATE {t} SET version=COALESCE(version,0) WHERE version IS NULL")
        except Exception:
            continue
    conn.commit()

try:
    _orig_upsert_notice_tp2 = upsert_notice
except Exception:
    _orig_upsert_notice_tp2 = None

def upsert_notice(notice: dict, files: Optional[list] = None) -> int:
    nid = _orig_upsert_notice_tp2(notice, files) if _orig_upsert_notice_tp2 else 0
    try:
        conn = get_db(); cur = conn.cursor()
        org_id, owner_id = _tp2_get_default_identity()
        cur.execute("UPDATE notices SET org_id=COALESCE(org_id,?), owner_id=COALESCE(owner_id,?) WHERE id=?", (org_id, owner_id, int(nid)))
        conn.commit()
    except Exception:
        pass
    return int(nid)

try:
    _orig_save_rfp_json_tp2 = save_rfp_json
except Exception:
    _orig_save_rfp_json_tp2 = None

def save_rfp_json(notice_id: int, payload: dict, schema_name: str="RFPv1", schema_version: str="1.0"):
    res = {"ok": False, "error": "unhandled"}
    try:
        import streamlit as st
        conn = get_db(); cur = conn.cursor()
        org_id = st.session_state.get("org_id") or _tp2_get_default_identity()[0]
        owner_id = st.session_state.get("user_id") or _tp2_get_default_identity()[1]
        data_str = _json.dumps(payload, sort_keys=True, separators=(",",":"))
        vhash = _hash.sha256(data_str.encode("utf-8")).hexdigest()
        cols = [r[1] for r in conn.execute("PRAGMA table_info(rfp_json)").fetchall()]
        if "org_id" in cols:
            cur.execute(
                "INSERT OR IGNORE INTO rfp_json(notice_id, schema_name, schema_version, version_hash, data_json, created_at, org_id, owner_id, visibility) VALUES(?,?,?,?,?,?,?,?,?)",
                (int(notice_id), schema_name, schema_version, vhash, data_str, _dt.datetime.utcnow().isoformat(), org_id, owner_id, "team")
            )
        else:
            cur.execute(
                "INSERT OR IGNORE INTO rfp_json(notice_id, schema_name, schema_version, version_hash, data_json, created_at) VALUES(?,?,?,?,?,?)",
                (int(notice_id), schema_name, schema_version, vhash, data_str, _dt.datetime.utcnow().isoformat())
            )
        conn.commit()
        res = {"ok": True, "version_hash": vhash}
    except Exception:
        if _orig_save_rfp_json_tp2:
            return _orig_save_rfp_json_tp2(notice_id, payload, schema_name, schema_version)
        raise
    return res

def _run_tenancy_phase2():
    try:
        _tp2_add_columns_and_indexes()
        _tp2_backfill()
    except Exception as ex:
        try: log_json("error", "tenancy_phase2_migration_failed", error=str(ex))
        except Exception: pass

try:
    _run_tenancy_phase2()
except Exception:
    pass
# === TENANCY PHASE 2 END ===



# === TENANCY PHASE 3 START ===
import datetime as _dt
import hashlib as _hash3
import json as _json3
import re as re

def _ids():
    import streamlit as st
    org = st.session_state.get('org_id')
    user = st.session_state.get('user_id')
    return org, user

def _ensure_ids():
    org, user = _ids()
    if not org or not user:
        raise PermissionError('identity_required')

def _append_org_filter(sql: str) -> str:
    s = sql.strip()
    if re.search(r'\borg_id\b\s*(=|IN|LIKE|IS)', s, re.IGNORECASE):
        return s
    m = re.search(r'\b(ORDER\s+BY|LIMIT)\b', s, re.IGNORECASE)
    clause = ' AND org_id=?'
    if 'WHERE' in s.upper():
        if m:
            return s[:m.start()] + clause + ' ' + s[m.start():]
        return s + clause
    else:
        if m:
            return s[:m.start()] + ' WHERE org_id=? ' + s[m.start():]
        return s + ' WHERE org_id=?'

def q_select(sql: str, params: tuple = ()):
    _ensure_ids()
    org, _user = _ids()
    s = _append_org_filter(sql)
    conn = get_db()
    cur = conn.cursor()
    try:
        return cur.execute(s, params + (org,)).fetchall()
    except Exception:
        return cur.execute(sql, params).fetchall()

def q_select_one(sql: str, params: tuple = ()):
    rows = q_select(sql, params)
    return rows[0] if rows else None

def q_exec(sql: str, params: tuple = ()):
    _ensure_ids()
    org, _user = _ids()
    s = _append_org_filter(sql)
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(s, params + (org,))
    except Exception:
        cur.execute(sql, params)
    conn.commit()
    return cur.rowcount

def q_insert(table: str, data: dict):
    _ensure_ids()
    org, user = _ids()
    data = dict(data or {})
    data.setdefault('org_id', org)
    data.setdefault('owner_id', user)
    data.setdefault('visibility', 'team')
    if 'version' in [k.lower() for k in data.keys()]:
        try:
            data['version'] = int(data.get('version', 0))
        except Exception:
            data['version'] = 0
    cols = list(data.keys())
    vals = [data[c] for c in cols]
    placeholders = ','.join(['?']*len(cols))
    sql = f"INSERT INTO {table}({','.join(cols)}) VALUES({placeholders})"
    conn = get_db(); cur = conn.cursor()
    cur.execute(sql, tuple(vals))
    conn.commit()
    return int(cur.lastrowid)

def q_update(table: str, data: dict, where: dict):
    _ensure_ids()
    org, user = _ids()
    where = dict(where or {})
    if where.get('org_id') and where['org_id'] != org:
        raise PermissionError('cross_org_denied')
    set_cols = []
    params = []
    if 'version' in data:
        new_version = int(data['version']) + 1
        set_cols.append('version=?')
        params.append(new_version)
        where_version = int(data['version'])
        where = dict(where, version=where_version)
        data = {k:v for k,v in data.items() if k != 'version'}
    for k,v in data.items():
        set_cols.append(f"{k}=?")
        params.append(v)
    if 'owner_id' not in data:
        set_cols.append('owner_id=?'); params.append(user)
    wh_cols = []
    wh_params = []
    for k,v in where.items():
        wh_cols.append(f"{k}=?"); wh_params.append(v)
    if 'org_id' not in where:
        wh_cols.append('org_id=?'); wh_params.append(org)
    sql = f"UPDATE {table} SET {', '.join(set_cols)} WHERE {' AND '.join(wh_cols)}"
    conn = get_db(); cur = conn.cursor()
    cur.execute(sql, tuple(params+wh_params))
    conn.commit()
    return cur.rowcount

def q_delete(table: str, where: dict):
    _ensure_ids()
    org, _user = _ids()
    wh_cols = []; wh_params = []
    for k,v in (where or {}).items():
        wh_cols.append(f"{k}=?"); wh_params.append(v)
    if 'org_id' not in (where or {}):
        wh_cols.append('org_id=?'); wh_params.append(org)
    sql = f"DELETE FROM {table} WHERE {' AND '.join(wh_cols)}"
    conn = get_db(); cur = conn.cursor()
    cur.execute(sql, tuple(wh_params))
    conn.commit()
    return cur.rowcount

def cache_key(base: str) -> str:
    org, user = _ids()
    return f"{base}::org={org}::user={user}"

def cached_get(url: str, params: dict=None, ttl: int=900):
    import streamlit as st
    @st.cache_data(ttl=ttl)
    def _fetch(key, url, params):
        return _http_get_json(url, params or {})
    return _fetch(cache_key('GET:'+url+':'+_json3.dumps(params or {}, sort_keys=True)), url, params or {})

def store_uploaded_file(file_bytes: bytes, filename: str, entity: str, entity_id: int, org_id: str=None, owner_id: str=None) -> dict:
    import os
    org, user = _ids()
    org_id = org_id or org; owner_id = owner_id or user
    base = os.path.join('data', 'files', str(org_id), str(owner_id), str(entity), str(entity_id))
    os.makedirs(base, exist_ok=True)
    checksum = _hash3.sha256(file_bytes).hexdigest()
    path = os.path.join(base, filename)
    if os.path.exists(path):
        with open(path, 'rb') as f:
            if _hash3.sha256(f.read()).hexdigest() == checksum:
                return {'path': path, 'bytes': os.path.getsize(path), 'checksum': checksum, 'skipped': True}
    with open(path, 'wb') as f:
        f.write(file_bytes)
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("""INSERT OR IGNORE INTO files(org_id, owner_id, entity, entity_id, name, path, bytes, checksum, created_at)
                        VALUES(?,?,?,?,?,?,?,?,?)""", (org_id, owner_id, entity, int(entity_id), filename, path, len(file_bytes), checksum, _dt.datetime.utcnow().isoformat()))
        conn.commit()
    except Exception:
        pass
    return {'path': path, 'bytes': len(file_bytes), 'checksum': checksum, 'skipped': False}

try:
    _orig_list_notices_tp3 = list_notices
except Exception:
    _orig_list_notices_tp3 = None

def list_notices(filters: dict, page: int, page_size: int, current_user_id: Optional[str], show_hidden: bool=False, order_by: str='posted_at DESC'):
    _ensure_ids()
    org, me = _ids()
    conn = get_db(); cur = conn.cursor()
    where = ['(org_id=? AND visibility!=\'private\') OR owner_id=?']
    params = [org, me]
    if filters.get('keywords'):
        where.append('(title LIKE ? OR agency LIKE ?)')
        kw = f"%{filters['keywords']}%"
        params.extend([kw, kw])
    if filters.get('types'):
        qs = ','.join(['?'] * len(filters['types']))
        where.append(f'notice_type IN ({qs})')
        params.extend(filters['types'])
    if filters.get('naics'):
        for code in filters['naics']:
            where.append('naics LIKE ?')
            params.append(f'%{code}%')
    if filters.get('psc'):
        for code in filters['psc']:
            where.append('psc LIKE ?')
            params.append(f'%{code}%')
    if filters.get('agency'):
        where.append('agency LIKE ?')
        params.append(f"%{filters['agency']}%")
    if filters.get('place_city'):
        where.append('place_city LIKE ?')
        params.append(f"%{filters['place_city']}%")
    if filters.get('place_state'):
        where.append('place_state = ?')
        params.append(filters['place_state'])
    if filters.get('posted_enabled'):
        if filters.get('posted_from'):
            where.append('(posted_at >= ?)'); params.append(filters['posted_from'])
        if filters.get('posted_to'):
            where.append('(posted_at <= ?)'); params.append(filters['posted_to'])
    if not show_hidden and current_user_id:
        where.append('id NOT IN (SELECT notice_id FROM notice_status WHERE user_id=?)')
        params.append(current_user_id)
    wh = ('WHERE ' + ' AND '.join(where)) if where else ''
    limit = max(1, int(page_size))
    offset = max(0, int(page)) * limit
    sql = f'''
        SELECT
            n.id, n.sam_notice_id, n.notice_type, n.title, n.agency, n.naics, n.psc,
            n.set_aside, n.place_city, n.place_state, n.posted_at, n.due_at, n.status, n.url,
            n.compliance_state,
            EXISTS(SELECT 1 FROM pipeline_deals pd WHERE pd.notice_id=n.id AND pd.user_id=? AND pd.org_id=?) AS starred,
            EXISTS(SELECT 1 FROM amendments a WHERE a.notice_id=n.id) AS amended
        FROM notices n
        {wh}
        ORDER BY {order_by}
        LIMIT ? OFFSET ?
    '''
    rows = cur.execute(sql, params + [me, org, limit, offset]).fetchall()
    cols = ['id','sam_notice_id','notice_type','title','agency','naics','psc','set_aside','place_city','place_state','posted_at','due_at','status','url','compliance_state','starred','amended']
    items = [dict(zip(cols, r)) for r in rows]
    total = cur.execute(f'SELECT COUNT(*) FROM notices n {wh}', params).fetchone()[0]
    return {'items': items, 'page': page, 'page_size': page_size, 'total': total}
# === TENANCY PHASE 3 END ===


# === PERSIST PHASE 5 START ===
import json as _json5
import threading as _thr5
import time as _time5
from typing import Optional as _Optional5, Dict as _Dict5, Any as _Any5

JOB_STALE_MINUTES = 10
JOB_LOOP_SLEEP_SEC = 1.0

def _p5_schema():
    conn = get_db(); cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS jobs(
      id INTEGER PRIMARY KEY,
      org_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN('queued','running','done','error')),
      attempts INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );""")
    # Ensure minimal status columns on existing queues if missing
    def ensure_cols(tbl):
        try:
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({tbl})").fetchall()]
            if 'status' not in cols:
                conn.execute(f"ALTER TABLE {tbl} ADD COLUMN status TEXT")
            if 'updated_at' not in cols:
                conn.execute(f"ALTER TABLE {tbl} ADD COLUMN updated_at TEXT")
            conn.commit()
        except Exception:
            pass
    for tbl in ['email_queue','search_runs','rfq_events']:
        ensure_cols(tbl)

def enqueue_job(kind: str, payload: _Dict5[str, _Any5], org_id: _Optional5[str]=None) -> int:
    import datetime as _dt
    import streamlit as st
    conn = get_db(); cur = conn.cursor()
    org = org_id or st.session_state.get('org_id') or 'org-default'
    now = _dt.datetime.utcnow().isoformat()
    cur.execute(
        "INSERT INTO jobs(org_id,kind,payload_json,status,attempts,last_error,created_at,updated_at) VALUES(?,?,?,?,0,NULL,?,?)",
        (org, kind, _json5.dumps(payload or {}), 'queued', now, now)
    )
    conn.commit()
    return int(cur.lastrowid)

def _claim_job() -> _Optional5[dict]:
    import datetime as _dt
    conn = get_db(); cur = conn.cursor()
    # 1) claim a queued job
    row = cur.execute("SELECT id, org_id, kind, payload_json, attempts FROM jobs WHERE status='queued' ORDER BY id LIMIT 1").fetchone()
    if row:
        jid, org, kind, payload_json, attempts = row
        now = _dt.datetime.utcnow().isoformat()
        cur.execute("UPDATE jobs SET status='running', attempts=?, updated_at=? WHERE id=? AND status='queued'", (attempts+1, now, jid))
        conn.commit()
        return {'id': jid, 'org_id': org, 'kind': kind, 'payload': _json5.loads(payload_json), 'attempts': attempts+1}
    # 2) reclaim stale running jobs
    cutoff = ( _dt.datetime.utcnow() - _dt.timedelta(minutes=JOB_STALE_MINUTES) ).isoformat()
    row = cur.execute("SELECT id, org_id, kind, payload_json, attempts FROM jobs WHERE status='running' AND updated_at < ? ORDER BY id LIMIT 1", (cutoff,)).fetchone()
    if row:
        jid, org, kind, payload_json, attempts = row
        now = _dt.datetime.utcnow().isoformat()
        cur.execute("UPDATE jobs SET attempts=?, updated_at=? WHERE id=?", (attempts+1, now, jid))
        conn.commit()
        return {'id': jid, 'org_id': org, 'kind': kind, 'payload': _json5.loads(payload_json), 'attempts': attempts+1}
    return None

def _finish_job(jid: int, status: str, error: _Optional5[str]=None):
    import datetime as _dt
    conn = get_db(); cur = conn.cursor()
    now = _dt.datetime.utcnow().isoformat()
    cur.execute("UPDATE jobs SET status=?, last_error=?, updated_at=? WHERE id=?", (status, error, now, int(jid)))
    conn.commit()

def _run_job(job: dict):
    kind = job.get('kind')
    payload = job.get('payload') or {}
    try:
        if kind == 'parse_rfp':
            nid = int(payload.get('notice_id'))
            res = rfp_parse_and_store(nid)
            if not res.get('ok'):
                raise RuntimeError(str(res))
        elif kind == 'build_pack':
            # Placeholder for future packaging job
            pass
        else:
            raise ValueError(f'unknown_job_kind:{kind}')
        _finish_job(job['id'], 'done', None)
    except Exception as ex:
        _finish_job(job['id'], 'error', str(ex))

_p5_worker_thread = None

def start_job_worker():
    """Start a background thread that ticks the durable job queue. Safe to call multiple times."""
    global _p5_worker_thread
    if _p5_worker_thread and _p5_worker_thread.is_alive():
        return
    def _loop():
        _p5_schema()
        while True:
            job = _claim_job()
            if job:
                _run_job(job)
            else:
                _time5.sleep(JOB_LOOP_SLEEP_SEC)
    _p5_worker_thread = _thr5.Thread(target=_loop, daemon=True)
    _p5_worker_thread.start()

def durable_parse_rfp(notice_id: int) -> int:
    """Public API: enqueue a parse job and ensure worker is running. Returns job id."""
    start_job_worker()
    return enqueue_job('parse_rfp', {'notice_id': int(notice_id)})

# Bootstrap schema at import so app survives restarts
try:
    _p5_schema()
except Exception:
    pass
# === PERSIST PHASE 5 END ===



# === SAM WATCH MINIMAL FALLBACK START ===
import json as _json_sam
import datetime as _dt
import requests as _req

def _sam_api_key():
    import os, streamlit as st
    try:
        return st.secrets["sam"]["key"]
    except Exception:
        return os.environ.get("SAM_API_KEY")

def _sam_fetch(filters: dict, page: int, size: int):
    key = _sam_api_key()
    if not key:
        return {"ok": False, "error": "missing_api_key"}
    base = "https://api.sam.gov/opportunities/v2/search"
    params = {"api_key": key, "limit": int(size), "offset": int(page) * int(size)}
    if filters.get("keywords"):
        params["q"] = filters["keywords"]
    if filters.get("types"):
        params["notice_type"] = ",".join(filters["types"])
    if filters.get("naics"):
        params["naics"] = ",".join(filters["naics"])
    if filters.get("psc"):
        params["psc"] = ",".join(filters["psc"])
    if filters.get("agency"):
        params["agency"] = filters["agency"]
    if filters.get("place_state"):
        params["state"] = filters["place_state"]
    try:
        r = _req.get(base, params=params, timeout=10)
        if r.status_code != 200:
            return {"ok": False, "error": f"http_{r.status_code}", "body": r.text[:500]}
        data = r.json()
        return {"ok": True, "data": data}
    except Exception as ex:
        return {"ok": False, "error": str(ex)}

def _sam_upsert_rows(payload) -> int:
    """Insert or update minimal notice rows from SAM payload."""
    conn = get_db(); cur = conn.cursor()
    count = 0
    items = []
    if isinstance(payload, dict):
        items = payload.get("opportunitiesData") or payload.get("data") or payload.get("results") or []
    elif isinstance(payload, list):
        items = payload
    for it in items:
        sid = str(it.get("noticeId") or it.get("solicitationNumber") or it.get("id") or "")
        title = it.get("title") or it.get("name") or ""
        agency = (it.get("agency") or {}).get("name") if isinstance(it.get("agency"), dict) else (it.get("agency") or "")
        notice_type = it.get("type") or it.get("noticeType") or ""
        naics = ",".join(it.get("naicsCodes") or []) if isinstance(it.get("naicsCodes"), list) else (it.get("naics") or "")
        psc = ",".join(it.get("pscCodes") or []) if isinstance(it.get("pscCodes"), list) else (it.get("psc") or "")
        posted_at = it.get("postedDate") or it.get("publishDate") or ""
        due_at = it.get("responseDate") or it.get("dueDate") or ""
        url = it.get("url") or it.get("link") or ""
        set_aside = it.get("setAside") or ""
        place_city = (it.get("placeOfPerformance") or {}).get("city") if isinstance(it.get("placeOfPerformance"), dict) else ""
        place_state = (it.get("placeOfPerformance") or {}).get("state") if isinstance(it.get("placeOfPerformance"), dict) else ""
        try:
            cur.execute("""INSERT INTO notices(sam_notice_id, notice_type, title, agency, naics, psc, set_aside,                    place_city, place_state, posted_at, due_at, status, url, last_fetched_at)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)ON CONFLICT(sam_notice_id) DO UPDATE SET  notice_type=excluded.notice_type,  title=excluded.title,  agency=excluded.agency,  naics=excluded.naics,  psc=excluded.psc,  set_aside=excluded.set_aside,  place_city=excluded.place_city,  place_state=excluded.place_state,  posted_at=excluded.posted_at,  due_at=excluded.due_at,  url=excluded.url,  last_fetched_at=excluded.last_fetched_at""", (sid, notice_type, title, agency, naics, psc, set_aside,       place_city, place_state, posted_at, due_at, None, url, _dt.datetime.utcnow().isoformat()))
            count += 1
        except Exception:
            try:
                cur.execute("INSERT OR IGNORE INTO notices(sam_notice_id, notice_type, title, agency, naics, psc, set_aside, place_city, place_state, posted_at, due_at, status, url, last_fetched_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (sid, notice_type, title, agency, naics, psc, set_aside, place_city, place_state, posted_at, due_at, None, url, _dt.datetime.utcnow().isoformat()))
                cur.execute("UPDATE notices SET notice_type=?, title=?, agency=?, naics=?, psc=?, set_aside=?, place_city=?, place_state=?, posted_at=?, due_at=?, url=?, last_fetched_at=? WHERE sam_notice_id=?",
                            (notice_type, title, agency, naics, psc, set_aside, place_city, place_state, posted_at, due_at, url, _dt.datetime.utcnow().isoformat(), sid))
                count += 1
            except Exception:
                pass
    conn.commit()
    return count

def render_sam_watch_minimal_ui():
    import streamlit as st
    st.subheader("SAM Watch")
    key_present = bool(_sam_api_key())
    if not key_present:
        st.warning("Missing SAM API key. Add st.secrets['sam']['key'] or SAM_API_KEY env.")
    with st.form("sam_min_search", clear_on_submit=False):
        kw = st.text_input("Keywords")
        types = st.multiselect("Types", ["Solicitation","Sources Sought","Presolicitation","Combined Synopsis/Solicitation"])
        size = st.selectbox("Page size", [25,50,100], index=1)
        submitted = st.form_submit_button("Search")
    if submitted:
        res = _sam_fetch({"keywords": kw, "types": types}, 0, size)
        if not res.get("ok"):
            st.error(f"Search failed: {res.get('error')}")
            if res.get("body"): st.code(res["body"][:500])
            return
        data = res["data"]
        try:
            inserted = _sam_upsert_rows(data)
        except Exception as ex:
            st.error(f"Insert failed: {ex}")
            inserted = 0
        st.caption(f"Fetched and upserted {inserted} notices.")
        try:
            me = st.session_state.get("user_id")
            out = list_notices({"keywords": kw, "types": types}, page=0, page_size=size, current_user_id=me, show_hidden=False)
            rows = out.get("items", [])
        except Exception:
            conn = get_db(); cur = conn.cursor()
            rows = cur.execute("SELECT id, title, agency, notice_type, due_at FROM notices ORDER BY posted_at DESC LIMIT ?", (int(size),)).fetchall()
            rows = [{"id": r[0], "title": r[1], "agency": r[2], "notice_type": r[3], "due_at": r[4]} for r in rows]
        if not rows:
            st.info("No results.")
            return
        for r in rows:
            with st.container(border=True):
                st.write(r.get("title")); st.caption(f"{r.get('agency','')} • {r.get('notice_type','')} • Due {r.get('due_at','')}")

# Integrate fallback into render_sam()
try:
    _tmp_render_sam = render_sam
except Exception:
    _tmp_render_sam = None

def render_sam():
    import streamlit as st
    try:
        if feature_flags().get("sam_ingest_core", False):
            try:
                render_sam_watch_phase1_ui()  # if present
                return
            except Exception as ex:
                st.warning(f"SAM Phase1 UI failed: {ex}")
        render_sam_watch_minimal_ui()
    except Exception as ex:
        st.error(f"SAM UI error: {ex}")
# === SAM WATCH MINIMAL FALLBACK END ===




# === PHASE 4 SAVED SEARCHES START ===
import datetime as _dt4
import json as _json4
import threading as _thr4

def _ss_schema():
    conn = get_db(); cur = conn.cursor()
    ddls = [
        """CREATE TABLE IF NOT EXISTS saved_searches(
            id INTEGER PRIMARY KEY,
            user_id TEXT NOT NULL,
            name TEXT NOT NULL,
            query_json TEXT NOT NULL,
            cadence TEXT NOT NULL CHECK(cadence IN('daily','weekly','monthly')),
            recipients TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            last_run_at TEXT
        );""",
        "CREATE INDEX IF NOT EXISTS idx_saved_searches_user ON saved_searches(user_id);",

        """CREATE TABLE IF NOT EXISTS search_runs(
            id INTEGER PRIMARY KEY,
            saved_search_id INTEGER NOT NULL REFERENCES saved_searches(id) ON DELETE CASCADE,
            ran_at TEXT NOT NULL,
            new_hits_count INTEGER NOT NULL,
            log_json TEXT
        );""",
        """CREATE TABLE IF NOT EXISTS search_hits(
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL REFERENCES search_runs(id) ON DELETE CASCADE,
            notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE
        );""",
        """CREATE TABLE IF NOT EXISTS email_queue(
            id INTEGER PRIMARY KEY,
            to_addr TEXT NOT NULL,
            subject TEXT NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT
        );""",
        "CREATE INDEX IF NOT EXISTS idx_email_status ON email_queue(status);",

    ]
    for ddl in ddls:
        try:
            cur.execute(ddl)
        except Exception:
            pass
    conn.commit()

def _ss_flag():
    import streamlit as st
    return st.session_state.get("feature_flags", {}).get("saved_search_alerts", False)

def _ss_recipients_default():
    import streamlit as st
    email = st.session_state.get("user_email") or "me@example.com"
    return email

def _ss_due(ts: str, cadence: str) -> bool:
    now = _dt4.datetime.utcnow()
    if not ts:
        return True
    try:
        last = _dt4.datetime.fromisoformat(ts.replace("Z","+00:00")) if ts.endswith("Z") else _dt4.datetime.fromisoformat(ts)
    except Exception:
        return True
    delta = now - last
    if cadence == "daily":
        return delta.total_seconds() >= 24*3600
    if cadence == "weekly":
        return delta.total_seconds() >= 7*24*3600
    if cadence == "monthly":
        return delta.total_seconds() >= 30*24*3600
    return True

def _ss_filters_from_row(row: dict) -> dict:
    try:
        return _json4.loads(row.get("query_json") or "{}")
    except Exception:
        return {}

def _ss_parse_recipients(recips: str):
    raw = recips or ""
    parts = [p.strip() for p in raw.replace(";",",").split(",") if p.strip()]
    return [p for p in parts if "@" in p]

def _ss_deeplink_for_notice(notice_id: int) -> str:
    return f"?page=opportunity&opp={int(notice_id)}&tab=Analyzer"

def _ss_body_for_hits(name: str, hits: list) -> str:
    out = [f"Saved search: {name}", "", "New notices:"]
    for h in hits[:50]:
        line = f"- {h.get('title','')} ({h.get('agency','')})  |  App: {_ss_deeplink_for_notice(h.get('id'))}  |  SAM: {h.get('url','')}"
        out.append(line)
    return "\n".join(out)

def _ss_run_one(row: dict, dry_run: bool=False) -> dict:
    conn = get_db(); cur = conn.cursor()
    filters = _ss_filters_from_row(row)
    items = []
    try:
        import streamlit as st
        me = st.session_state.get("user_id")
        out = list_notices(filters, page=0, page_size=100, current_user_id=me, show_hidden=False)
        items = out.get("items", [])
    except Exception:
        rows = cur.execute("SELECT id,title,agency,url,posted_at FROM notices ORDER BY posted_at DESC LIMIT 100").fetchall()
        items = [{"id":r[0], "title":r[1], "agency":r[2], "url":r[3], "posted_at": r[4]} for r in rows]
    last_run_at = row.get("last_run_at")
    new_hits = []
    for it in items:
        ts = it.get("posted_at") or ""
        try:
            if not last_run_at:
                new_hits.append(it)
            else:
                last = _dt4.datetime.fromisoformat(last_run_at.replace("Z","+00:00")) if last_run_at.endswith("Z") else _dt4.datetime.fromisoformat(last_run_at)
                cur_ts = _dt4.datetime.fromisoformat(ts.replace("Z","+00:00")) if isinstance(ts, str) and ts.endswith("Z") else _dt4.datetime.fromisoformat(ts) if ts else None
                if cur_ts and cur_ts > last:
                    new_hits.append(it)
        except Exception:
            new_hits.append(it)
    ran_at = _dt4.datetime.utcnow().isoformat()
    cur.execute("INSERT INTO search_runs(saved_search_id, ran_at, new_hits_count, log_json) VALUES(?,?,?,?)", (row["id"], ran_at, len(new_hits), _json4.dumps({"filters":filters})))
    run_id = cur.lastrowid
    for it in new_hits[:200]:
        try:
            cur.execute("INSERT INTO search_hits(run_id, notice_id) VALUES(?,?)", (run_id, int(it.get("id"))))
        except Exception:
            pass
    if not dry_run:
        try:
            cur.execute("UPDATE saved_searches SET last_run_at=? WHERE id= ?", (ran_at, row["id"]))
        except Exception:
            pass
    conn.commit()
    return {"ran_at": ran_at, "hits": new_hits, "count": len(new_hits), "run_id": int(run_id)}

def run_saved_searches():
    if not _ss_flag():
        return {"ok": False, "error": "flag_disabled"}
    _ss_schema()
    conn = get_db(); cur = conn.cursor()
    rows = cur.execute("SELECT id, user_id, name, query_json, cadence, recipients, active, last_run_at FROM saved_searches WHERE active=1").fetchall()
    cols = ["id","user_id","name","query_json","cadence","recipients","active","last_run_at"]
    due = [dict(zip(cols, r)) for r in rows if _ss_due(r[7], r[4])]
    enq = 0
    for r in due:
        res = _ss_run_one(r, dry_run=False)
        if res["count"] <= 0:
            continue
        recips = _ss_parse_recipients(r["recipients"])
        body = _ss_body_for_hits(r["name"], res["hits"])
        subj = f"[SAM] {r['name']} - {res['count']} new notices"
        now = _dt4.datetime.utcnow().isoformat()
        for to in recips:
            try:
                cur.execute("INSERT INTO email_queue(to_addr, subject, body, created_at, status, attempts, last_error) VALUES(?,?,?,?, 'queued', 0, NULL)", (to, subj, body, now))
                enq += 1
            except Exception:
                pass
    conn.commit()
    return {"ok": True, "enqueued": enq, "due": len(due)}

_ss_scheduler_thread = None
def start_saved_search_scheduler():
    global _ss_scheduler_thread
    if _ss_scheduler_thread and _ss_scheduler_thread.is_alive():
        return
    def _loop():
        import time
        while True:
            try:
                run_saved_searches()
            except Exception:
                pass
            time.sleep(60)
    _ss_scheduler_thread = _thr4.Thread(target=_loop, daemon=True); _ss_scheduler_thread.start()

try:
    _orig_render_sam_watch_minimal_ui_p4 = render_sam_watch_minimal_ui
except Exception:
    _orig_render_sam_watch_minimal_ui_p4 = None

def render_sam_watch_minimal_ui():
    import streamlit as st
    _ss_schema()
    st.subheader("SAM Watch")
    with st.container():
        c1, c2, c3 = st.columns([1,1,2])
        with c1:
            if _ss_flag() and st.button("Manage Saved Searches"):
                st.session_state["saved_search_modal_open"] = True
        with c2:
            if _ss_flag() and st.button("Run scheduler now"):
                st.session_state["last_dry_run_result"] = run_saved_searches()
    key_present = bool(_sam_api_key())
    if not key_present:
        st.warning("Missing SAM API key. Add st.secrets['sam']['key'] or SAM_API_KEY env.")
    with st.form("sam_min_search", clear_on_submit=False):
        kw = st.text_input("Keywords")
        types = st.multiselect("Types", ["Solicitation","Sources Sought","Presolicitation","Combined Synopsis/Solicitation"])
        size = st.selectbox("Page size", [25,50,100], index=1)
        save_it = st.checkbox("Save these filters as a search", value=False) if _ss_flag() else False
        submitted = st.form_submit_button("Search")
    if submitted:
        res = _sam_fetch({"keywords": kw, "types": types}, 0, size)
        if not res.get("ok"):
            st.error(f"Search failed: {res.get('error')}")
            if res.get("body"): st.code(res["body"][:500])
            return
        data = res["data"]
        try:
            inserted = _sam_upsert_rows(data)
        except Exception as ex:
            st.error(f"Insert failed: {ex}")
            inserted = 0
        st.caption(f"Fetched and upserted {inserted} notices.")
        me = st.session_state.get("user_id")
        out = list_notices({"keywords": kw, "types": types}, page=0, page_size=size, current_user_id=me, show_hidden=False)
        rows = out.get("items", [])
        if not rows:
            st.info("No results.")
            return
        if save_it and _ss_flag():
            name = st.text_input("Name for this search", value=kw or "My SAM search", key="ss_name_inline")
            cadence = st.selectbox("Cadence", ["daily","weekly","monthly"], index=0, key="ss_cadence_inline")
            recips = st.text_input("Recipients (comma separated)", value=_ss_recipients_default(), key="ss_recips_inline")
            if st.button("Save search"):
                conn = get_db(); cur = conn.cursor()
                try:
                    uid = st.session_state.get("user_id") or "user"
                    cur.execute("INSERT INTO saved_searches(user_id, name, query_json, cadence, recipients, active, last_run_at) VALUES(?,?,?,?,?,1,NULL)",
                                (uid, name, _json4.dumps({"keywords": kw, "types": types}), cadence, recips))
                    conn.commit()
                    st.success("Saved search created.")
                except Exception as ex:
                    st.error(f"Save failed: {ex}")
        for r in rows:
            with st.container(border=True):
                st.write(r.get("title")); st.caption(f"{r.get('agency','')} • {r.get('notice_type','')} • Due {r.get('due_at','')}")
    if _ss_flag() and st.session_state.get("saved_search_modal_open"):
        st.markdown("### Saved Searches")
        conn = get_db(); cur = conn.cursor()
        rows = cur.execute("SELECT id,name,cadence,recipients,active,last_run_at,query_json FROM saved_searches WHERE user_id=? ORDER BY id DESC",
                           (st.session_state.get("user_id"),)).fetchall()
        if not rows:
            st.info("No saved searches yet.")
        else:
            for rid, name, cad, rec, active, last_run, qj in rows:
                with st.expander(f"{name} • {cad} • {'active' if active else 'inactive'}", expanded=False):
                    st.caption(f"Recipients: {rec}")
                    c1,c2,c3,c4,c5 = st.columns(5)
                    with c1:
                        if st.button("Dry run", key=f"dry_{rid}"):
                            res = _ss_run_one({"id":rid,"query_json": qj, "last_run_at": last_run}, dry_run=True)
                            st.session_state["last_dry_run_result"] = res
                    with c2:
                        if st.button("Activate" if not active else "Deactivate", key=f"act_{rid}"):
                            cur.execute("UPDATE saved_searches SET active=? WHERE id=?", (0 if active else 1, rid)); conn.commit()
                    with c3:
                        if st.button("Run now", key=f"run_{rid}"):
                            _ = _ss_run_one({"id":rid,"query_json": qj, "last_run_at": last_run}, dry_run=False)
                    with c4:
                        new_cad = st.selectbox("Cadence", ["daily","weekly","monthly"], index=["daily","weekly","monthly"].index(cad), key=f"cad_{rid}")
                        if st.button("Save cadence", key=f"savecad_{rid}"):
                            cur.execute("UPDATE saved_searches SET cadence=? WHERE id=?", (new_cad, rid)); conn.commit()
                    with c5:
                        if st.button("Delete", key=f"del_{rid}"):
                            cur.execute("DELETE FROM saved_searches WHERE id=?", (rid,)); conn.commit()
        if st.button("Close"):
            st.session_state["saved_search_modal_open"] = False

try:
    _ss_schema()
except Exception:
    pass
# === PHASE 4 SAVED SEARCHES END ===




# === PROPOSAL PHASE 5 START ===
import datetime as _dtp5
import json as _jsonp5

def _p5_schema():
    conn = get_db(); cur = conn.cursor()
    ddls = [
        """CREATE TABLE IF NOT EXISTS proposals(
            id INTEGER PRIMARY KEY,
            notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
            owner_id TEXT NOT NULL,
            title TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'Draft',
            created_at TEXT NOT NULL
        );""",
        """CREATE UNIQUE INDEX IF NOT EXISTS ux_proposal_notice_owner ON proposals(notice_id, owner_id);""",
        """CREATE TABLE IF NOT EXISTS proposal_sections(
            id INTEGER PRIMARY KEY,
            proposal_id INTEGER NOT NULL REFERENCES proposals(id) ON DELETE CASCADE,
            key TEXT NOT NULL,
            title TEXT NOT NULL,
            page_limit INTEGER,
            font_name TEXT,
            font_size INTEGER,
            writing_plan TEXT,
            content_md TEXT
        );""",
        """CREATE TABLE IF NOT EXISTS proposal_files(
            id INTEGER PRIMARY KEY,
            proposal_id INTEGER NOT NULL REFERENCES proposals(id) ON DELETE CASCADE,
            file_name TEXT NOT NULL,
            file_id TEXT NOT NULL,
            uploaded_at TEXT NOT NULL
        );""",
        """CREATE TABLE IF NOT EXISTS exports(
            id INTEGER PRIMARY KEY,
            proposal_id INTEGER NOT NULL REFERENCES proposals(id) ON DELETE CASCADE,
            type TEXT NOT NULL,
            file_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            checklist_snapshot TEXT
        );""",
        """CREATE TABLE IF NOT EXISTS doc_templates(
            key TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            docx_blob BLOB,
            version TEXT
        );"""
    ]
    for ddl in ddls:
        try: cur.execute(ddl)
        except Exception: pass
    conn.commit()

def _p5_latest_rfp_json(notice_id: int):
    conn = get_db(); cur = conn.cursor()
    row = cur.execute("SELECT data_json FROM rfp_json WHERE notice_id=? ORDER BY id DESC LIMIT 1", (int(notice_id),)).fetchone()
    if not row: return None
    try: return _jsonp5.loads(row[0])
    except Exception: return None

def _p5_create_or_get_proposal(notice_id: int, owner_id: str) -> int:
    _p5_schema()
    conn = get_db(); cur = conn.cursor()
    n = cur.execute("SELECT title FROM notices WHERE id=?", (int(notice_id),)).fetchone()
    title = n[0] if n and n[0] else f"Proposal for Notice {notice_id}"
    cur.execute("INSERT OR IGNORE INTO proposals(notice_id, owner_id, title, status, created_at) VALUES(?,?,?,?,?)",
                (int(notice_id), owner_id, title, "Draft", _dtp5.datetime.utcnow().isoformat()))
    conn.commit()
    pid = cur.execute("SELECT id FROM proposals WHERE notice_id=? AND owner_id=?", (int(notice_id), owner_id)).fetchone()[0]
    return int(pid)

def _p5_seed_sections_from_rfp(notice_id: int, proposal_id: int):
    data = _p5_latest_rfp_json(int(notice_id)) or {}
    conn = get_db(); cur = conn.cursor()
    existing = cur.execute("SELECT COUNT(*) FROM proposal_sections WHERE proposal_id=?", (int(proposal_id),)).fetchone()[0]
    if existing and existing > 0: return
    sections = []
    for s in (data.get("sections") or []):
        key = s.get("key") or s.get("title") or "Section"
        title = s.get("title") or key
        pl = s.get("page_limit")
        sections.append({"key": key, "title": title, "page_limit": pl})
    if not sections:
        for i, lm in enumerate(data.get("lm_requirements") or []):
            title = lm.get("factor") or lm.get("subfactor") or (lm.get("text", "")[:60])
            sections.append({"key": f"LM{i+1}", "title": title, "page_limit": None})
    for s in sections[:30]:
        try:
            cur.execute("INSERT INTO proposal_sections(proposal_id, key, title, page_limit, font_name, font_size, writing_plan, content_md) VALUES(?,?,?,?,NULL,NULL,NULL,NULL)",
                        (int(proposal_id), s["key"], s["title"], s.get("page_limit")))
        except Exception:
            pass
    conn.commit()

def _p5_placeholder_scan(proposal_id: int) -> list:
    conn = get_db(); cur = conn.cursor()
    bad = []
    rows = cur.execute("SELECT id, title, content_md FROM proposal_sections WHERE proposal_id=?", (int(proposal_id),)).fetchall()
    for sid, title, md in rows:
        txt = (md or "").lower()
        if any(tok in txt for tok in ["tbd", "xx", "lorem ipsum"]):
            bad.append({"section_id": sid, "title": title})
    return bad

def render_proposal_wizard(notice_id: int):
    import streamlit as st
    _p5_schema()
    st.session_state.setdefault("wizard_step", 1)
    st.session_state.setdefault("current_proposal_id", None)
    owner = st.session_state.get("user_id") or "user"
    if not st.session_state["current_proposal_id"]:
        pid = _p5_create_or_get_proposal(int(notice_id), owner)
        _p5_seed_sections_from_rfp(int(notice_id), pid)
        st.session_state["current_proposal_id"] = pid
    pid = int(st.session_state["current_proposal_id"])
    st.markdown("#### Proposal Wizard")
    steps = ["1. Outline","2. Sections","3. Uploads","4. Package"]
    scols = st.columns(4)
    for i, c in enumerate(scols, start=1):
        with c: st.button(steps[i-1], disabled=(st.session_state["wizard_step"]==i), key=f"p5tab{i}")
    step = st.session_state["wizard_step"]
    if step == 1:
        st.subheader("Step 1 · Outline from Analyzer")
        data = _p5_latest_rfp_json(int(notice_id)) or {}
        st.write("Factors and requirements")
        for it in (data.get("lm_requirements") or [])[:50]:
            st.markdown(f"- {it.get('text','')}".strip())
        if st.button("Next → Sections"):
            st.session_state["wizard_step"] = 2
            (st.experimental_rerun() if hasattr(st, "experimental_rerun") else st.rerun())
    if step == 2:
        st.subheader("Step 2 · Section stubs")
        conn = get_db(); cur = conn.cursor()
        rows = cur.execute("SELECT id, key, title, page_limit, font_name, font_size, writing_plan FROM proposal_sections WHERE proposal_id=? ORDER BY id", (pid,)).fetchall()
        for sid, key, title, pl, fname, fsize, plan in rows:
            with st.container(border=True):
                new_title = st.text_input("Title", value=title, key=f"t_{sid}")
                new_pl = st.number_input("Page limit", value=int(pl) if pl is not None else 0, min_value=0, max_value=500, key=f"pl_{sid}")
                fname = st.text_input("Font name", value=fname or "", key=f"fn_{sid}")
                fsize = st.number_input("Font size", value=int(fsize) if fsize else 0, min_value=0, max_value=72, key=f"fs_{sid}")
                plan = st.text_area("Writing plan", value=plan or "", key=f"wp_{sid}")
                if st.button("Save section", key=f"sv_{sid}"):
                    cur.execute("UPDATE proposal_sections SET title=?, page_limit=?, font_name=?, font_size=?, writing_plan=? WHERE id=?",
                                (new_title, None if new_pl==0 else int(new_pl), fname or None, None if fsize==0 else int(fsize), plan or None, sid))
                    conn.commit()
                    st.success("Saved")
        if st.button("Next → Uploads"):
            st.session_state["wizard_step"] = 3
            (st.experimental_rerun() if hasattr(st, "experimental_rerun") else st.rerun())
    if step == 3:
        st.subheader("Step 3 · Supporting files")
        up = st.file_uploader("Upload resumes, past performance, etc.", accept_multiple_files=True)
        if up:
            for f in up:
                b = f.read()
                meta = store_uploaded_file(b, f.name, "proposal", pid)
                conn = get_db(); cur = conn.cursor()
                cur.execute("INSERT INTO proposal_files(proposal_id, file_name, file_id, uploaded_at) VALUES(?,?,?,?)",
                            (pid, f.name, meta.get("checksum"), _dtp5.datetime.utcnow().isoformat()))
                conn.commit()
        conn = get_db(); cur = conn.cursor()
        rows = cur.execute("SELECT id, file_name, uploaded_at FROM proposal_files WHERE proposal_id=? ORDER BY id DESC", (pid,)).fetchall()
        for fid, fname, ts in rows:
            st.caption(f"{fname} • {ts}")
        if st.button("Next → Package"):
            st.session_state["wizard_step"] = 4
            (st.experimental_rerun() if hasattr(st, "experimental_rerun") else st.rerun())
    if step == 4:
        st.subheader("Step 4 · Package preview")
        conn = get_db(); cur = conn.cursor()
        cstate = cur.execute("SELECT compliance_state FROM notices WHERE id=?", (int(notice_id),)).fetchone()
        cstate = cstate[0] if cstate else "Unreviewed"
        bad = _p5_placeholder_scan(pid)
        ok = (cstate == "Green" and not bad)
        st.caption(f"Compliance: {cstate}")
        if bad:
            st.warning(f"Placeholder issues in {len(bad)} sections")
        st.button("Export (disabled until compliance is Green)", disabled=not ok)
        if ok and st.button("Export now"):
            _t0 = _time.perf_counter()
            
            # Build snapshot of checklist and signoffs
            snap_rows = pd.read_sql_query("select req_id, factor, subfactor, requirement, status, owner_id, due_date, evidence_file_id, evidence_page from lm_checklist where notice_id=?", conn, params=(int(notice_id),))
            snap_sigs = pd.read_sql_query("select role, status, user_id, ts from signoffs where notice_id=?", conn, params=(int(notice_id),))
            snapshot = json.dumps({"checklist": snap_rows.to_dict(orient="records"), "signoffs": snap_sigs.to_dict(orient="records")}, ensure_ascii=False)
cur.execute("INSERT INTO exports(proposal_id, type, file_id, created_at, checklist_snapshot) VALUES(?,?,?,?,?)",
                        (pid, "zip", f"export-{pid}", _dtp5.datetime.utcnow().isoformat(), snapshot))
            
            try:
                metric_push('export_duration_ms', (_time.perf_counter()-_t0)*1000.0, {'type': 'docx'})
            except Exception:
                pass
conn.commit()
            st.success("Export queued")
    cols = st.columns(3)
    with cols[0]:
        if st.button("← Back") and st.session_state["wizard_step"] > 1:
            st.session_state["wizard_step"] -= 1
            (st.experimental_rerun() if hasattr(st, "experimental_rerun") else st.rerun())
    with cols[2]:
        if st.button("Close wizard"):
            st.session_state["current_proposal_id"] = None
            st.session_state["wizard_step"] = 1
            (st.experimental_rerun() if hasattr(st, "experimental_rerun") else st.rerun())

try:
    _orig_rfp_panel_ui_p5 = _rfp_panel_ui
except Exception:
    _orig_rfp_panel_ui_p5 = None

def _rfp_panel_ui(notice_id: int):
    import streamlit as st
    if not feature_flags().get("rfp_analyzer_panel", False):
        return
    with st.sidebar:
        st.markdown("## RFP Analyzer")
        st.caption(f"Notice #{notice_id}")
        if feature_flags().get("start_proposal_inline", False):
            if st.button("Start proposal"):
                st.session_state["wizard_step"] = 1
                st.session_state["current_proposal_id"] = None
        try:
            if _orig_rfp_panel_ui_p5:
                _orig_rfp_panel_ui_p5(notice_id)
        except Exception as ex:
            st.warning(f"Analyzer panel partial: {ex}")
    if feature_flags().get("start_proposal_inline", False) and st.session_state.get("wizard_step") and st.session_state.get("wizard_step") >= 1:
        render_proposal_wizard(int(notice_id))
# === PROPOSAL PHASE 5 END ===




# === RFP PHASE 6 START ===
import datetime as _dt6
import json as _json6
import hashlib as _hash6

def _rfp6_schema():
    conn = get_db(); cur = conn.cursor()
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS rfp_impacts(
            id INTEGER PRIMARY KEY,
            notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
            from_hash TEXT NOT NULL,
            to_hash TEXT NOT NULL,
            impact_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );""")
    except Exception:
        pass
    conn.commit()

def _rfp6_flag():
    import streamlit as st
    return st.session_state.get("feature_flags", {}).get("rfp_impact", False)

def _rfp6_load_versions(notice_id: int):
    conn = get_db(); cur = conn.cursor()
    rows = cur.execute("SELECT version_hash, data_json FROM rfp_json WHERE notice_id=? ORDER BY id DESC LIMIT 2", (int(notice_id),)).fetchall()
    if not rows or len(rows) < 2:
        return None
    to_hash, to_js = rows[0][0], rows[0][1]
    from_hash, from_js = rows[1][0], rows[1][1]
    try:
        return (to_hash, _json6.loads(to_js or "{}"), from_hash, _json6.loads(from_js or "{}"))
    except Exception:
        return None

def _set_needs_review(notice_id: int):
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("UPDATE notices SET compliance_state='Needs review' WHERE id=?", (int(notice_id),))
        conn.commit()
    except Exception:
        pass

def _diff_list_by_key(prev_list, curr_list, key_fn, text_fn):
    prev = {key_fn(x): text_fn(x) for x in (prev_list or []) if key_fn(x)}
    curr = {key_fn(x): text_fn(x) for x in (curr_list or []) if key_fn(x)}
    added = [k for k in curr.keys() if k not in prev]
    removed = [k for k in prev.keys() if k not in curr]
    changed = [k for k in curr.keys() if k in prev and (curr[k] or "") != (prev[k] or "") ]
    return {"added": added, "removed": removed, "changed": changed}

def _rfp6_compute_impact(prev: dict, curr: dict) -> dict:
    sec = _diff_list_by_key(prev.get("sections"), curr.get("sections"),
                            key_fn=lambda s: (s.get("key") or s.get("title")),
                            text_fn=lambda s: f"{s.get('title','')}|pl={s.get('page_limit')}" )
    frm = _diff_list_by_key(prev.get("deliverables_forms"), curr.get("deliverables_forms"),
                            key_fn=lambda f: f.get("name"),
                            text_fn=lambda f: f.get("name") )
    cl_prev = ((prev.get("price_structure") or {}).get("clins") or [])
    cl_curr = ((curr.get("price_structure") or {}).get("clins") or [])
    clin = _diff_list_by_key(cl_prev, cl_curr,
                             key_fn=lambda c: c.get("clin") or c.get("desc"),
                             text_fn=lambda c: f"{c.get('clin','')}|{c.get('desc','')}|{c.get('qty_hint','')}|{c.get('uom','')}" )
    dates = {"submission_changed": False, "milestones": {"added": [], "removed": [], "changed": []}}
    try:
        dates["submission_changed"] = (prev.get("submission",{}).get("due_datetime") != curr.get("submission",{}).get("due_datetime"))
    except Exception:
        pass
    ms = _diff_list_by_key(prev.get("milestones"), curr.get("milestones"),
                           key_fn=lambda m: m.get("name"),
                           text_fn=lambda m: f"{m.get('name','')}|{m.get('due_datetime','')}" )
    dates["milestones"] = ms
    rtm = _diff_list_by_key(prev.get("lm_requirements"), curr.get("lm_requirements"),
                            key_fn=lambda r: r.get("id") or (r.get("text","")[:60]),
                            text_fn=lambda r: r.get("text") )
    impact = {"sections": sec, "forms": frm, "clins": clin, "dates": dates, "rtm": rtm}
    return impact

def compute_and_store_rfp_impact(notice_id: int) -> dict:
    _rfp6_schema()
    v = _rfp6_load_versions(int(notice_id))
    if not v:
        return {"ok": False, "error": "versions_insufficient"}
    to_hash, to_js, from_hash, from_js = v
    if to_hash == from_hash:
        return {"ok": True, "no_change": True}
    impact = _rfp6_compute_impact(from_js, to_js)
    conn = get_db(); cur = conn.cursor()
    try:
        cur.execute("INSERT INTO rfp_impacts(notice_id, from_hash, to_hash, impact_json, created_at) VALUES(?,?,?,?,?)",
                    (int(notice_id), from_hash, to_hash, _json6.dumps(impact, sort_keys=True), _dt6.datetime.utcnow().isoformat()))
        conn.commit()
    except Exception:
        pass
    _set_needs_review(int(notice_id))
    return {"ok": True, "impact": impact, "from": from_hash, "to": to_hash}

def latest_rfp_impact(notice_id: int):
    _rfp6_schema()
    conn = get_db(); cur = conn.cursor()
    row = cur.execute("SELECT impact_json, from_hash, to_hash, created_at FROM rfp_impacts WHERE notice_id=? ORDER BY id DESC LIMIT 1",
                      (int(notice_id),)).fetchone()
    if not row:
        return None
    try:
        return {"impact": _json6.loads(row[0]), "from": row[1], "to": row[2], "created_at": row[3]}
    except Exception:
        return None

try:
    _orig_save_rfp_json_p6 = save_rfp_json
except Exception:
    _orig_save_rfp_json_p6 = None

def save_rfp_json(notice_id: int, payload: dict, schema_name: str="RFPv1", schema_version: str="1.0"):
    res = _orig_save_rfp_json_p6(notice_id, payload, schema_name, schema_version) if _orig_save_rfp_json_p6 else {"ok": False}
    try:
        import streamlit as st
        if res.get("ok") and _rfp6_flag():
            _ = compute_and_store_rfp_impact(int(notice_id))
    except Exception:
        pass
    return res

try:
    _orig_rfp_panel_ui_p2_ref = _orig_rfp_panel_ui_p2
except Exception:
    _orig_rfp_panel_ui_p2_ref = None

def _rfp_panel_ui_p2_with_impact(notice_id: int):
    import streamlit as st
    if _orig_rfp_panel_ui_p2_ref:
        try: _orig_rfp_panel_ui_p2_ref(notice_id)
        except Exception as ex: st.warning(f"Analyzer base failed: {ex}")
    if not _rfp6_flag():
        return
    with st.sidebar:
        st.markdown("### Impact")
        data = latest_rfp_impact(int(notice_id))
        if not data:
            st.caption("No impact cached yet.")
            if st.button("Compute impact"):
                res = compute_and_store_rfp_impact(int(notice_id))
                if res.get("ok"):
                    st.success("Impact computed.")
                else:
                    st.warning(str(res))
            return
        imp = data.get("impact") or {}
        def group_block(label, bucket):
            added = bucket.get("added", []); removed = bucket.get("removed", []); changed = bucket.get("changed", [])
            if not (added or removed or changed): return
            st.write(f"**{label}**")
            if added: st.caption("Added: " + ", ".join([str(x) for x in added][:10]))
            if removed: st.caption("Removed: " + ", ".join([str(x) for x in removed][:10]))
            if changed: st.caption("Changed: " + ", ".join([str(x) for x in changed][:10]))
        group_block("Sections", imp.get("sections", {}))
        group_block("Forms", imp.get("forms", {}))
        group_block("CLINs", imp.get("clins", {}))
        d = imp.get("dates", {})
        if d.get("submission_changed"): st.caption("Due date changed.")
        group_block("Milestones", d.get("milestones", {}))
        group_block("RTM", imp.get("rtm", {}))

try:
    _orig_rfp_panel_ui_p2 = _rfp_panel_ui_p2_with_impact
except Exception:
    pass

try:
    _orig_render_proposal_wizard_p6 = render_proposal_wizard
except Exception:
    _orig_render_proposal_wizard_p6 = None

def render_proposal_wizard(notice_id: int):
    import streamlit as st
    if _rfp6_flag():
        data = latest_rfp_impact(int(notice_id))
        if data and data.get("impact"):
            imp = data["impact"]
            with st.expander("Impact TODOs", expanded=True):
                todo = []
                for label in ["sections","forms","clins","rtm"]:
                    b = imp.get(label,{})
                    if any(b.get(k) for k in ["added","removed","changed"]):
                        todo.append(f"{label}: " + ", ".join([f"{k}:{', '.join(map(str, b.get(k, [])[:5]))}" for k in ["added","removed","changed"] if b.get(k)]))
                if imp.get("dates",{}).get("submission_changed"):
                    todo.append("dates: submission due changed")
                ms = imp.get("dates",{}).get("milestones",{})
                if any(ms.get(k) for k in ["added","removed","changed"]):
                    todo.append("dates: milestones updated")
                if todo:
                    for t in todo: st.caption(t)
                else:
                    st.caption("No pending impact items.")
    if _orig_render_proposal_wizard_p6:
        return _orig_render_proposal_wizard_p6(int(notice_id))
# === RFP PHASE 6 END ===



# === RFP PHASE 7 START ===
import datetime as _dt7
import json as _json7

def _b7_flag():
    import streamlit as st
    return st.session_state.get('feature_flags', {}).get('builder_from_analyzer', False)

def _b7_schema():
    conn = get_db(); cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS section_requirements( id INTEGER PRIMARY KEY, proposal_id INTEGER NOT NULL REFERENCES proposals(id) ON DELETE CASCADE, section_key TEXT NOT NULL, req_id TEXT NOT NULL, UNIQUE(proposal_id, section_key, req_id) )")
    except Exception:
        pass
    conn.commit()

def _b7_latest_json(notice_id: int):
    conn = get_db(); cur = conn.cursor()
    row = cur.execute('SELECT data_json FROM rfp_json WHERE notice_id=? ORDER BY id DESC LIMIT 1', (int(notice_id),)).fetchone()
    if not row: return None
    try: return _json7.loads(row[0])
    except Exception: return None

def _b7_map_reqs_to_sections(an: dict, sections: list) -> dict:
    out = { (s.get('key') or s.get('title') or 'Section'): [] for s in sections }
    reqs = an.get('lm_requirements') or []
    for r in reqs:
        f = (r.get('factor') or '').lower()
        sf = (r.get('subfactor') or '').lower()
        target = None
        for s in sections:
            title = (s.get('title') or s.get('key') or '').lower()
            if f and f in title:
                target = (s.get('key') or s.get('title')); break
            if sf and sf in title:
                target = (s.get('key') or s.get('title')); break
        if not target and sections:
            target = (sections[0].get('key') or sections[0].get('title'))
        if target:
            out.setdefault(target, []).append(r)
    return out

def _b7_seed_sections_from_analyzer(notice_id: int, proposal_id: int):
    if not _b7_flag():
        return
    _b7_schema()
    an = _b7_latest_json(int(notice_id)) or {}
    conn = get_db(); cur = conn.cursor()
    existing = cur.execute('SELECT COUNT(*) FROM proposal_sections WHERE proposal_id=?', (int(proposal_id),)).fetchone()[0]
    if not existing:
        try:
            _p5_seed_sections_from_rfp(int(notice_id), int(proposal_id))
        except Exception:
            pass
    rows = cur.execute('SELECT id, key, title FROM proposal_sections WHERE proposal_id=?', (int(proposal_id),)).fetchall()
    vol_fonts = {}
    for v in (an.get('volumes') or []):
        name = (v.get('name') or '').lower()
        vol_fonts[name] = {'font_name': v.get('font'), 'font_size': None, 'spacing': v.get('spacing')}
    sec_defs = { (s.get('key') or s.get('title')): s for s in (an.get('sections') or []) }
    for sid, skey, stitle in rows:
        ident = skey or stitle
        sd = sec_defs.get(ident) or {}
        page_limit = sd.get('page_limit')
        vol = (sd.get('parent_volume') or '').lower()
        font_name = vol_fonts.get(vol, {}).get('font_name')
        try:
            cur.execute('UPDATE proposal_sections SET page_limit=COALESCE(page_limit, ?), font_name=COALESCE(font_name, ?) WHERE id=?', (page_limit, font_name, sid))
        except Exception:
            pass
    req_map = _b7_map_reqs_to_sections(an, an.get('sections') or [])
    for sid, skey, stitle in rows:
        sec_key = skey or stitle or 'Section'
        reqs = req_map.get(sec_key, [])
        if not reqs:
            continue
        bullets = []
        for r in reqs:
            items = r.get('must_address') or []
            cite = r.get('cite') or {}
            cite_str = ''
            if cite.get('file') or (cite.get('page') is not None):
                file = cite.get('file') or ''
                page = cite.get('page')
                cite_str = f' (source: {file} p.{page})' if (page is not None) else f' (source: {file})'
            if items:
                for it in items:
                    bullets.append(f'- {it}{cite_str}')
            else:
                text = r.get('text') or ''
                bullets.append(f'- {text}{cite_str}')
            try:
                cur.execute('INSERT OR IGNORE INTO section_requirements(proposal_id, section_key, req_id) VALUES(?,?,?)', (int(proposal_id), sec_key, r.get('id') or (r.get('text','')[:32])))
            except Exception:
                pass
        if bullets:
            row_wp = cur.execute('SELECT writing_plan FROM proposal_sections WHERE id=?', (sid,)).fetchone()
            wp = (row_wp[0] or '') if row_wp else ''
            sig = bullets[0].strip()
            if sig not in (wp or ''):
                new_wp = (wp + '\n\n' if wp else '') + '\n'.join(bullets)
                try:
                    cur.execute('UPDATE proposal_sections SET writing_plan=? WHERE id=?', (new_wp, sid))
                except Exception:
                    pass
    conn.commit()

def _b7_seed_clins(notice_id: int, proposal_id: int):
    if not _b7_flag():
        return
    an = _b7_latest_json(int(notice_id)) or {}
    clins = ((an.get('price_structure') or {}).get('clins') or [])
    if not clins:
        return
    conn = get_db(); cur = conn.cursor()
    cols = [r[1] for r in cur.execute('PRAGMA table_info(price_lines)').fetchall()] if cur else []
    if not cols:
        return
    has_cols = set(cols)
    for c in clins[:200]:
        clin = c.get('clin') or ''
        desc = c.get('desc') or ''
        uom = c.get('uom') or ''
        qty = c.get('qty_hint') or None
        fields = {}
        if 'proposal_id' in has_cols: fields['proposal_id'] = int(proposal_id)
        if 'clin' in has_cols: fields['clin'] = clin
        if 'description' in has_cols: fields['description'] = desc
        if 'uom' in has_cols: fields['uom'] = uom
        if 'qty' in has_cols: fields['qty'] = qty
        if not fields:
            continue
        cols_sql = ','.join(fields.keys())
        ph = ','.join(['?']*len(fields))
        vals = list(fields.values())
        try:
            cur.execute(f'INSERT INTO price_lines({cols_sql}) VALUES({ph})', tuple(vals))
        except Exception:
            pass
    conn.commit()

def builder_prechecks(proposal_id: int) -> dict:
    res = {'placeholders': [], 'a11y': [], 'style': []}
    try:
        res['placeholders'] = _p5_placeholder_scan(int(proposal_id))
    except Exception:
        res['placeholders'] = []
    try:
        cur = get_db().cursor()
        nid = cur.execute('SELECT notice_id FROM proposals WHERE id=?', (int(proposal_id),)).fetchone()
        nid = int(nid[0]) if nid else None
        an = _b7_latest_json(nid) if nid else {}
        a11y = (an or {}).get('accessibility_rules') or {}
        if a11y.get('req_508'): res['a11y'].append('Section 508 applies; ensure PDF tagging and alt text.')
        if a11y.get('pdf_tags'): res['a11y'].append('Final PDF must include tags and bookmarks.')
        if a11y.get('bookmarks'): res['a11y'].append('Include document bookmarks matching section outline.')
        if a11y.get('alt_text'): res['a11y'].append('All figures need alt text.')
    except Exception:
        pass
    try:
        rows = get_db().cursor().execute('SELECT title, page_limit, font_name FROM proposal_sections WHERE proposal_id=?', (int(proposal_id),)).fetchall()
        for title, pl, font in rows:
            if pl is None: res['style'].append(f"Set page limit for '{title}'.")
            if not font: res['style'].append(f"Set font for '{title}'.")
    except Exception:
        pass
    return res

try:
    _orig__p5_create_or_get_proposal_b7 = _p5_create_or_get_proposal
except Exception:
    _orig__p5_create_or_get_proposal_b7 = None

def _p5_create_or_get_proposal(notice_id: int, owner_id: str) -> int:
    pid = _orig__p5_create_or_get_proposal_b7(int(notice_id), owner_id) if _orig__p5_create_or_get_proposal_b7 else 0
    try:
        _b7_seed_sections_from_analyzer(int(notice_id), int(pid))
        _b7_seed_clins(int(notice_id), int(pid))
        import streamlit as st
        st.session_state['builder_adapter_ready'] = True
    except Exception:
        pass
    return int(pid)
# === RFP PHASE 7 END ===



# === SUB PHASE 1 START ===
import time as _time_sub1
import requests as _req_sub1
import math as _math_sub1
from urllib.parse import urlparse as _urlparse_sub1

def _sub1_schema():
    conn = get_db(); cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS source_runs( id INTEGER PRIMARY KEY, user_id TEXT, opp_id INTEGER, query TEXT, center TEXT, radius_mi REAL, page_size INTEGER, ran_at TEXT NOT NULL, next_page_token TEXT, total_returned INTEGER NOT NULL DEFAULT 0 );")
    except Exception: pass
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS vendor_sources( id INTEGER PRIMARY KEY, run_id INTEGER NOT NULL REFERENCES source_runs(id) ON DELETE CASCADE, place_id TEXT, vendor_name TEXT, rank INTEGER, created_at TEXT NOT NULL );")
    except Exception: pass
    # Ensure vendors.distance_mi exists
    try:
        cols = [r[1] for r in cur.execute("PRAGMA table_info(vendors)").fetchall()]
        if 'distance_mi' not in cols:
            cur.execute("ALTER TABLE vendors ADD COLUMN distance_mi REAL")
    except Exception: pass
    conn.commit()

def _gplaces_key():
    import os, streamlit as st
    try:
        return st.secrets["gplaces"]["key"]
    except Exception:
        return os.environ.get("GEO_API_KEY") or os.environ.get("GOOGLE_MAPS_API_KEY")

def _geocode_center(address: str):
    key = _gplaces_key()
    if not key:
        return None
    try:
        r = _req_sub1.get("https://maps.googleapis.com/maps/api/geocode/json", params={"address": address, "key": key}, timeout=10)
        j = r.json()
        if j.get("status") != "OK":
            return None
        loc = j["results"][0]["geometry"]["location"]
        return (float(loc["lat"]), float(loc["lng"]))
    except Exception:
        return None

def _haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.7613
    p1 = _math_sub1.radians(lat1); p2 = _math_sub1.radians(lat2)
    dphi = _math_sub1.radians(lat2 - lat1); dl = _math_sub1.radians(lon2 - lon1)
    a = _math_sub1.sin(dphi/2)**2 + _math_sub1.cos(p1)*_math_sub1.cos(p2)*_math_sub1.sin(dl/2)**2
    return 2*R*_math_sub1.asin(_math_sub1.sqrt(a))

def _norm_phone(p):
    if not p: return None
    digits = "".join([c for c in str(p) if c.isdigit()])
    return digits or None

def _norm_domain(url):
    if not url: return None
    try:
        netloc = _urlparse_sub1(url).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc or None
    except Exception:
        return None

def _places_search(query: str, center_latlng: tuple, radius_m: int, page_size: int, page_token: str=None):
    key = _gplaces_key()
    if not key:
        return {"ok": False, "error": "missing_places_key"}
    base = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": query, "key": key, "radius": int(radius_m)}
    if center_latlng:
        params["location"] = f"{center_latlng[0]},{center_latlng[1]}"
    if page_token:
        params["pagetoken"] = page_token
    try:
        r = _req_sub1.get(base, params=params, timeout=10)
        j = r.json()
        status = j.get("status")
        if status == "INVALID_REQUEST" and page_token:
            # API requires delay before next_page_token is valid
            _time_sub1.sleep(2.1)
            r = _req_sub1.get(base, params=params, timeout=10)
            j = r.json(); status = j.get("status")
        if status not in ("OK","ZERO_RESULTS","OVER_QUERY_LIMIT","UNKNOWN_ERROR","INVALID_REQUEST","REQUEST_DENIED","NOT_FOUND"):
            status = "UNKNOWN"
        return {"ok": status in ("OK","ZERO_RESULTS"), "status": status, "data": j}
    except Exception as ex:
        return {"ok": False, "error": str(ex)}

def _sub1_upsert_vendors(rows: list, center_latlng: tuple):
    conn = get_db(); cur = conn.cursor()
    inserted = 0
    for rank, it in enumerate(rows, start=1):
        name = it.get("name") or ""
        place_id = it.get("place_id") or it.get("placeId") or ""
        phone = _norm_phone(it.get("formatted_phone_number") or it.get("formatted_phone") or it.get("international_phone_number"))
        website = it.get("website") or it.get("url")
        domain = _norm_domain(website)
        loc = (it.get("geometry") or {}).get("location") or {}
        lat = loc.get("lat"); lng = loc.get("lng")
        dist = None
        try:
            if center_latlng and lat is not None and lng is not None:
                dist = float(_haversine_miles(center_latlng[0], center_latlng[1], float(lat), float(lng)))
        except Exception:
            dist = None
        # Identify existing vendor
        vid = None
        try:
            if place_id:
                row = cur.execute("SELECT id FROM vendors WHERE place_id=?", (place_id,)).fetchone()
                if row: vid = int(row[0])
        except Exception:
            pass
        if not vid and domain:
            try:
                row = cur.execute("SELECT id FROM vendors WHERE domain=?", (domain,)).fetchone()
                if row: vid = int(row[0])
            except Exception: pass
        if not vid and phone:
            try:
                row = cur.execute("SELECT id FROM vendors WHERE REPLACE(REPLACE(REPLACE(phone,'-',''),'(',''),')','') LIKE ?", (f"%{phone}%",)).fetchone()
                if row: vid = int(row[0])
            except Exception: pass
        # Insert minimal vendor if new and table shape allows
        if vid is None:
            try:
                cols = [r[1] for r in cur.execute("PRAGMA table_info(vendors)").fetchall()]
                fields = {}; vals = []
                if "name" in cols: fields["name"]=name
                if "place_id" in cols: fields["place_id"]=place_id
                if "phone" in cols: fields["phone"]=phone
                if "website" in cols: fields["website"]=website
                if "domain" in cols: fields["domain"]=domain
                if "lat" in cols: fields["lat"]=lat
                if "lng" in cols: fields["lng"]=lng
                if "distance_mi" in cols: fields["distance_mi"]=dist
                if not fields:
                    vid = None
                else:
                    cols_sql = ",".join(fields.keys())
                    ph = ",".join(["?"]*len(fields))
                    cur.execute(f"INSERT INTO vendors({cols_sql}) VALUES({ph})", tuple(fields.values()))
                    vid = int(cur.lastrowid)
                    inserted += 1
            except Exception:
                vid = None
        else:
            # Update distance if closer value known
            try:
                cur.execute("UPDATE vendors SET distance_mi=COALESCE(?, distance_mi) WHERE id=?", (dist, vid))
            except Exception:
                pass
    get_db().commit()
    return inserted

def _sub1_dedupe(items: list):
    seen_place = set(); seen_pair = set(); out = []
    for it in items:
        pid = it.get("place_id") or it.get("placeId")
        dom = _norm_domain(it.get("website") or it.get("url"))
        ph = _norm_phone(it.get("formatted_phone_number") or it.get("international_phone_number") or it.get("formatted_phone"))
        key2 = (dom or "", ph or "")
        if pid and pid in seen_place:
            continue
        if not pid and key2 in seen_pair:
            continue
        out.append(it)
        if pid: seen_place.add(pid)
        else: seen_pair.add(key2)
    return out

def subfinder_search(opp_id: int, query: str, use_pop: bool, radius_mi: int, page_size: int):
    """Run or continue a search. Deterministic sort by name then distance. Returns dict(items, next_token)."""
    import streamlit as st
    if not st.session_state.get("feature_flags", {}).get("subfinder_paging", False):
        return {"ok": False, "error": "flag_disabled"}
    _sub1_schema()
    key = _gplaces_key()
    if not key:
        return {"ok": False, "error": "missing_places_key"}
    conn = get_db(); cur = conn.cursor()
    # derive center
    center = None
    if use_pop:
        row = cur.execute("SELECT place_city, place_state FROM notices WHERE id=?", (int(opp_id),)).fetchone()
        if row and (row[0] or row[1]):
            addr = (row[0] or "") + ", " + (row[1] or "")
            center = _geocode_center(addr)
    radius_m = int(max(1, min(150, int(radius_mi))) * 1609.344)
    # get or create run with token
    uid = st.session_state.get("user_id") or "user"
    run = cur.execute("SELECT id, next_page_token, total_returned FROM source_runs WHERE user_id=? AND opp_id=? AND query=? AND center=? AND radius_mi=? AND page_size=? ORDER BY id DESC LIMIT 1",
                      (uid, int(opp_id), query or "", str(center), float(radius_mi), int(page_size))).fetchone()
    if not run:
        now = __import__("datetime").datetime.utcnow().isoformat()
        cur.execute("INSERT INTO source_runs(user_id, opp_id, query, center, radius_mi, page_size, ran_at, next_page_token, total_returned) VALUES(?,?,?,?,?,?,?,NULL,0)",
                    (uid, int(opp_id), query or "", str(center), float(radius_mi), int(page_size), now))
        conn.commit()
        run = cur.execute("SELECT id, next_page_token, total_returned FROM source_runs WHERE user_id=? AND opp_id=? AND query=? AND center=? AND radius_mi=? AND page_size=? ORDER BY id DESC LIMIT 1",
                          (uid, int(opp_id), query or "", str(center), float(radius_mi), int(page_size))).fetchone()
    run_id, next_tok, total = int(run[0]), run[1], int(run[2])
    # fetch page using token
    res = _places_search(query or "contractor", center, radius_m, page_size, page_token=next_tok)
    if not res.get("ok"):
        return {"ok": False, "error": res.get("error") or res.get("status")}
    data = res["data"]
    results = data.get("results") or []
    # enrich with distance
    for it in results:
        loc = (it.get("geometry") or {}).get("location") or {}
        lat = loc.get("lat"); lng = loc.get("lng")
        if center and (lat is not None) and (lng is not None):
            it["distance_mi"] = _haversine_miles(center[0], center[1], float(lat), float(lng))
        else:
            it["distance_mi"] = None
    results = _sub1_dedupe(results)
    # determine new token with required delay handling for Load more
    next_token = data.get("next_page_token") or data.get("next_page_token".upper()) or None
    try:
        cur.execute("UPDATE source_runs SET next_page_token=?, total_returned=total_returned+? WHERE id=?", (next_token, len(results), run_id))
        conn.commit()
    except Exception:
        pass
    # Upsert vendors (best-effort)
    try:
        _sub1_upsert_vendors(results, center)
    except Exception:
        pass
    # deterministic sort and slice
    def _k(it):
        nm = (it.get("name") or "").lower()
        dm = it.get("distance_mi")
        dm = float(dm) if dm is not None else 1e9
        pid = it.get("place_id") or ""
        return (nm, dm, pid)
    results_sorted = sorted(results, key=_k)
    return {"ok": True, "items": results_sorted[:int(page_size)], "next_token": next_token}

# UI for Vendors subtab (fallback if not present)
try:
    _orig_render_vendors_subtab = render_vendors
except Exception:
    _orig_render_vendors_subtab = None

def render_vendors(opp_id: int):
    import streamlit as st
    if _orig_render_vendors_subtab:
        try:
            return _orig_render_vendors_subtab(int(opp_id))
        except Exception:
            pass
    # Fallback Subfinder UI
    if not st.session_state.get("feature_flags", {}).get("subfinder_paging", False):
        st.info("Subfinder is disabled. Enable 'subfinder_paging' in Admin.")
        return
    _sub1_schema()
    st.subheader("Subcontractor Finder")
    query = st.text_input("Search query", value="contractor")
    use_pop = st.checkbox("Use Place of Performance", value=True)
    radius = st.slider("Radius (miles)", 10, 150, 50)
    page_size = st.selectbox("Page size", [20,50,100], index=1)
    cols = st.columns(3)
    with cols[0]:
        if st.button("Search"):
            st.session_state["sub1_results"] = []
            st.session_state["sub1_token"] = None
            res = subfinder_search(int(opp_id), query, use_pop, int(radius), int(page_size))
            if res.get("ok"):
                st.session_state["sub1_results"] = res.get("items", [])
                st.session_state["sub1_token"] = res.get("next_token")
            else:
                st.error(f"Search error: {res.get('error')}")
    with cols[1]:
        if st.button("Load more"):
            res = subfinder_search(int(opp_id), query, use_pop, int(radius), int(page_size))
            if res.get("ok"):
                # append unique
                cur = {(it.get("place_id") or "", it.get("name") or "") for it in st.session_state.get("sub1_results", [])}
                for it in res.get("items", []):
                    key = (it.get("place_id") or "", it.get("name") or "")
                    if key not in cur:
                        st.session_state["sub1_results"].append(it)
                st.session_state["sub1_token"] = res.get("next_token")
            else:
                st.error(f"Load error: {res.get('error')}")
    rows = st.session_state.get("sub1_results", [])
    if rows:
        # Stable sort for display
        rows = sorted(rows, key=lambda it: ((it.get("name") or "").lower(), it.get("distance_mi") or 1e9, it.get("place_id") or ""))
        for it in rows[:int(page_size)]:
            nm = it.get("name") or ""
            dist = it.get("distance_mi")
            addr = it.get("formatted_address") or it.get("vicinity") or ""
            phone = it.get("formatted_phone_number") or ""
            web = it.get("website") or ""
            st.markdown(f"**{nm}**  ·  {dist:.1f} mi" if isinstance(dist, (int,float)) else f"**{nm}**")
            st.caption(f"{addr}")
            if phone: st.caption(phone)
            if web: st.caption(web)
# === SUB PHASE 1 END ===



# === SUB PHASE 2 START ===
import re as _re_sub2

def _sub2_schema():
    conn = get_db(); cur = conn.cursor()
    try:
        cols = [r[1] for r in cur.execute('PRAGMA table_info(vendors)').fetchall()]
        if 'fit_score' not in cols:
            cur.execute('ALTER TABLE vendors ADD COLUMN fit_score REAL')
    except Exception:
        pass
    conn.commit()

def _sub2_state_from_addr(addr: str):
    if not addr: return None
    # look for ', XX ' two-letter state
    m = _re_sub2.search(r',\s*([A-Z]{2})(\s|,|$)', addr)
    if m:
        return m.group(1)
    return None

def _sub2_find_vendor_id(cur, item):
    pid = item.get('place_id') or item.get('placeId')
    dom = _norm_domain(item.get('website') or item.get('url'))
    ph = _norm_phone(item.get('formatted_phone_number') or item.get('international_phone_number') or item.get('formatted_phone'))
    try:
        if pid:
            row = cur.execute('SELECT id FROM vendors WHERE place_id=?', (pid,)).fetchone()
            if row: return int(row[0])
    except Exception: pass
    try:
        if dom:
            row = cur.execute('SELECT id FROM vendors WHERE domain=?', (dom,)).fetchone()
            if row: return int(row[0])
    except Exception: pass
    try:
        if ph:
            row = cur.execute("SELECT id FROM vendors WHERE REPLACE(REPLACE(REPLACE(phone,'-',''),'(',''),')','') LIKE ?", (f'%{ph}%',)).fetchone()
            if row: return int(row[0])
    except Exception: pass
    return None

def _sub2_apply_filters_and_rank(rows: list, naics_list: list, include_words: list, exclude_words: list, require_phone: bool, require_web: bool, state: str):
    _sub2_schema()
    conn = get_db(); cur = conn.cursor()
    out = []
    for it in rows or []:
        name = (it.get('name') or '').lower()
        addr = it.get('formatted_address') or it.get('vicinity') or ''
        st = _sub2_state_from_addr(addr) or ''
        phone = _norm_phone(it.get('formatted_phone_number') or it.get('international_phone_number') or it.get('formatted_phone'))
        web = (it.get('website') or '')
        dom = (_norm_domain(web) or '')
        # strict filters
        if state and st != state:
            continue
        if require_phone and not phone:
            continue
        if require_web and not web:
            continue
        # NAICS filter from vendors table if available
        ok_naics = True
        if naics_list:
            vid = _sub2_find_vendor_id(cur, it)
            if vid is not None:
                try:
                    row = cur.execute('SELECT naics FROM vendors WHERE id=?', (vid,)).fetchone()
                    vnaics = (row[0] or '') if row else ''
                    # treat vendor.naics as comma-separated string
                    vset = {c.strip() for c in str(vnaics).split(',') if c.strip()}
                    ok_naics = bool(vset.intersection(set(naics_list)))
                except Exception:
                    ok_naics = True
            else:
                ok_naics = True
        if not ok_naics:
            continue
        # scoring
        score = 0.0
        blob = ' '.join([name, dom]).lower()
        for w in include_words:
            if w and w.lower() in blob: score += 2.0
        for w in exclude_words:
            if w and w.lower() in blob: score -= 3.0
        if require_phone and phone: score += 0.5
        if require_web and web: score += 0.5
        it['fit_score'] = score
        # write back to DB best-effort
        try:
            vid = _sub2_find_vendor_id(cur, it)
            if vid is not None:
                cur.execute('UPDATE vendors SET fit_score=? WHERE id=?', (float(score), int(vid)))
        except Exception:
            pass
        out.append(it)
    try: conn.commit()
    except Exception: pass
    # sort by score desc then distance then name
    def _k(x):
        sc = x.get('fit_score')
        sc = float(sc) if sc is not None else 0.0
        dm = x.get('distance_mi')
        dm = float(dm) if dm is not None else 1e9
        nm = (x.get('name') or '').lower()
        return (-sc, dm, nm)
    return sorted(out, key=_k)

# Patch Vendors UI to add filter panel when flag is on
try:
    _orig_render_vendors_subtab_phase2 = render_vendors
except Exception:
    _orig_render_vendors_subtab_phase2 = None

def render_vendors(opp_id: int):
    import streamlit as st
    flag = st.session_state.get('feature_flags', {}).get('subfinder_filters', False)
    if _orig_render_vendors_subtab_phase2 and not flag:
        try:
            return _orig_render_vendors_subtab_phase2(int(opp_id))
        except Exception:
            pass
    # Fallback or augmented UI
    st.subheader('Subcontractor Finder')
    # base inputs reused for paging flow if present
    query = st.text_input('Search query', value=st.session_state.get('sub1_query', 'contractor'))
    use_pop = st.checkbox('Use Place of Performance', value=st.session_state.get('sub1_use_pop', True))
    radius = st.slider('Radius (miles)', 10, 150, st.session_state.get('sub1_radius', 50))
    page_size = st.selectbox('Page size', [20,50,100], index={20:0,50:1,100:2}[st.session_state.get('sub1_page_size', 50)])
    st.session_state['sub1_query']=query; st.session_state['sub1_use_pop']=use_pop; st.session_state['sub1_radius']=radius; st.session_state['sub1_page_size']=page_size
    # Phase 2 filters
    st.markdown('**Filters**')
    c1,c2,c3 = st.columns([2,2,2])
    with c1:
        naics_in = st.text_input('NAICS (comma-separated)', value=st.session_state.get('sub2_naics',''))
    with c2:
        inc = st.text_input('Must include keywords (comma-separated)', value=st.session_state.get('sub2_inc',''))
    with c3:
        exc = st.text_input('Exclude keywords (comma-separated)', value=st.session_state.get('sub2_exc',''))
    c4,c5,c6 = st.columns([1,1,1])
    with c4:
        has_phone = st.checkbox('Has phone', value=st.session_state.get('sub2_has_phone', False))
    with c5:
        has_web = st.checkbox('Has website', value=st.session_state.get('sub2_has_web', False))
    with c6:
        state = st.text_input('State (e.g., VA)', value=st.session_state.get('sub2_state',''))
    cols = st.columns(3)
    with cols[0]:
        if st.button('Search'):
            st.session_state['sub1_results'] = []
            st.session_state['sub1_token'] = None
            res = subfinder_search(int(opp_id), query, use_pop, int(radius), int(page_size))
            if res.get('ok'):
                st.session_state['sub1_results'] = res.get('items', [])
                st.session_state['sub1_token'] = res.get('next_token')
            else:
                st.error(f"Search error: {res.get('error')}")
    with cols[1]:
        if st.button('Load more'):
            res = subfinder_search(int(opp_id), query, use_pop, int(radius), int(page_size))
            if res.get('ok'):
                curset = {(it.get('place_id') or '', it.get('name') or '') for it in st.session_state.get('sub1_results', [])}
                for it in res.get('items', []):
                    key = (it.get('place_id') or '', it.get('name') or '')
                    if key not in curset:
                        st.session_state['sub1_results'].append(it)
                st.session_state['sub1_token'] = res.get('next_token')
            else:
                st.error(f"Load error: {res.get('error')}")
    with cols[2]:
        if st.button('Clear all filters'):
            for k in ['sub2_naics','sub2_inc','sub2_exc','sub2_has_phone','sub2_has_web','sub2_state']:
                if k in st.session_state: del st.session_state[k]
            st.experimental_rerun() if hasattr(st,'experimental_rerun') else st.rerun()
    # Store filters
    st.session_state['sub2_naics']=naics_in
    st.session_state['sub2_inc']=inc
    st.session_state['sub2_exc']=exc
    st.session_state['sub2_has_phone']=has_phone
    st.session_state['sub2_has_web']=has_web
    st.session_state['sub2_state']=state
    # Active chips
    chips = []
    if naics_in.strip(): chips.append(f"NAICS: {naics_in}")
    if inc.strip(): chips.append(f"Include: {inc}")
    if exc.strip(): chips.append(f"Exclude: {exc}")
    if has_phone: chips.append('Has phone')
    if has_web: chips.append('Has website')
    if state.strip(): chips.append(f"State: {state.strip().upper()}")
    if chips: st.caption(' | '.join(chips))
    # Results with filters and ranking
    rows = st.session_state.get('sub1_results', [])
    if rows:
        naics_list = [c.strip() for c in naics_in.split(',') if c.strip()]
        inc_list = [c.strip() for c in inc.split(',') if c.strip()]
        exc_list = [c.strip() for c in exc.split(',') if c.strip()]
        state_norm = state.strip().upper() if state else ''
        ranked = _sub2_apply_filters_and_rank(rows, naics_list, inc_list, exc_list, bool(has_phone), bool(has_web), state_norm)
        for it in ranked[:int(page_size)]:
            nm = it.get('name') or ''
            dist = it.get('distance_mi')
            addr = it.get('formatted_address') or it.get('vicinity') or ''
            phone = it.get('formatted_phone_number') or ''
            web = it.get('website') or ''
            sc = it.get('fit_score')
            header = f"**{nm}**  ·  score {sc:.1f}" if isinstance(sc,(int,float)) else f"**{nm}**"
            if isinstance(dist,(int,float)): header += f"  ·  {dist:.1f} mi"
            st.markdown(header)
            if addr: st.caption(addr)
            if phone: st.caption(phone)
            if web: st.caption(web)
# === SUB PHASE 2 END ===


# === SUB PHASE 3 START ===
import requests as _req_sub3
import datetime as _dt_sub3

def _sub3_schema():
    conn = get_db(); cur = conn.cursor()
    # Ensure vendor_sources has source and vendor_id columns
    try:
        cols = [r[1] for r in cur.execute('PRAGMA table_info(vendor_sources)').fetchall()]
        if 'source' not in cols:
            cur.execute('ALTER TABLE vendor_sources ADD COLUMN source TEXT')
        if 'vendor_id' not in cols:
            cur.execute('ALTER TABLE vendor_sources ADD COLUMN vendor_id INTEGER')
    except Exception:
        pass
    conn.commit()

def _sub3_flag():
    import streamlit as st
    return st.session_state.get('feature_flags', {}).get('subfinder_sources', False)

def _sub3_usasp_awardees(naics_list: list, state: str, limit: int=200):
    out = []
    try:
        url = 'https://api.usaspending.gov/api/v2/search/spending_by_award/'
        headers = {'Content-Type': 'application/json'}
        for naics in (naics_list or [])[:10]:
            payload = {
                'fields': ['Recipient Name','Recipient UEI','Recipient DUNS','Recipient State'],
                'filters': {
                    'naics_codes': [naics],
                    'recipient_locations': [{'state': state}] if state else []
                },
                'limit': 50
            }
            r = _req_sub3.post(url, json=payload, headers=headers, timeout=15)
            if r.status_code != 200:
                continue
            j = r.json() or {}
            results = j.get('results') or j.get('results', []) or []
            for row in results:
                name = row.get('Recipient Name') or row.get('recipient_name')
                st2 = row.get('Recipient State') or row.get('recipient_state_code') or state
                if not name:
                    continue
                out.append({
                    'source': 'usaspending',
                    'name': name,
                    'state': (st2 or '').upper(),
                    'uei': row.get('Recipient UEI') or row.get('uei'),
                    'duns': row.get('Recipient DUNS') or row.get('duns'),
                })
    except Exception:
        return []
    return out

def _sub3_gsa_contract_holders(naics_list: list, state: str):
    # Placeholder: GSA contractor directory APIs vary; return empty unless a compatible endpoint is configured.
    return []

def _sub3_find_by_name_state(cur, name: str, state: str):
    try:
        row = cur.execute('SELECT id FROM vendors WHERE LOWER(name)=? AND (state=? OR state IS NULL OR state="") LIMIT 1', (name.lower(), state)).fetchone()
        if row: return int(row[0])
    except Exception:
        pass
    try:
        row = cur.execute('SELECT id FROM vendors WHERE LOWER(name)=? LIMIT 1', (name.lower(),)).fetchone()
        if row: return int(row[0])
    except Exception:
        pass
    return None

def _sub3_upsert_vendor_and_link(opp_id: int, src_name: str, v: dict, rank: int=0):
    _sub3_schema()
    conn = get_db(); cur = conn.cursor()
    # ensure a run row for source import exists
    run_query = f'{src_name}:naics={v.get("naics","")};state={v.get("state","")}'
    uid = __import__('streamlit').session_state.get('user_id','user')
    row = cur.execute('SELECT id FROM source_runs WHERE user_id=? AND opp_id=? AND query=? ORDER BY id DESC LIMIT 1', (uid, int(opp_id), run_query)).fetchone()
    if row: run_id = int(row[0])
    else:
        now = _dt_sub3.datetime.utcnow().isoformat()
        cur.execute('INSERT INTO source_runs(user_id, opp_id, query, center, radius_mi, page_size, ran_at, next_page_token, total_returned) VALUES(?,?,?,?,?,?,?,NULL,0)', (uid, int(opp_id), run_query, None, 0.0, 0, now))
        conn.commit()
        run_id = int(cur.lastrowid)
    # merge by name + state
    name = v.get('name') or ''
    state = (v.get('state') or '').upper()
    vid = _sub3_find_by_name_state(cur, name, state)
    cols = [r[1] for r in cur.execute('PRAGMA table_info(vendors)').fetchall()]
    if vid is None:
        fields = {};
        if 'name' in cols: fields['name'] = name
        if 'state' in cols: fields['state'] = state
        if 'domain' in cols: fields['domain'] = v.get('domain')
        if 'phone' in cols: fields['phone'] = v.get('phone')
        if not fields:
            vid = None
        else:
            cols_sql = ','.join(fields.keys()); ph = ','.join(['?']*len(fields));
            cur.execute(f'INSERT INTO vendors({cols_sql}) VALUES({ph})', tuple(fields.values()))
            vid = int(cur.lastrowid)
    else:
        # enrich missing domain/phone if empty
        try:
            if 'domain' in cols and v.get('domain'):
                cur.execute('UPDATE vendors SET domain=COALESCE(NULLIF(domain,\'\'), ?) WHERE id=?', (v.get('domain'), vid))
            if 'phone' in cols and v.get('phone'):
                cur.execute('UPDATE vendors SET phone=COALESCE(NULLIF(phone,\'\'), ?) WHERE id=?', (v.get('phone'), vid))
        except Exception:
            pass
    # link vendor_sources
    try:
        cur.execute('INSERT INTO vendor_sources(run_id, place_id, vendor_name, rank, created_at, source, vendor_id) VALUES(?,?,?,?,?,?,?)', (int(run_id), None, name, int(rank), _dt_sub3.datetime.utcnow().isoformat(), src_name, vid))
    except Exception:
        pass
    conn.commit()
    return vid

def subfinder_import_sources(opp_id: int, naics_list: list, state: str):
    if not _sub3_flag():
        return {'ok': False, 'error': 'flag_disabled'}
    _sub3_schema()
    total = 0
    # USAspending
    us = _sub3_usasp_awardees(naics_list, state)
    for i, v in enumerate(us):
        _sub3_upsert_vendor_and_link(int(opp_id), 'usaspending', v, rank=i+1)
        total += 1
    # GSA (stubbed)
    gs = _sub3_gsa_contract_holders(naics_list, state)
    for i, v in enumerate(gs):
        _sub3_upsert_vendor_and_link(int(opp_id), 'gsa', v, rank=i+1)
        total += 1
    return {'ok': True, 'imported': total}

# Patch Vendors UI to show source chips and awardee-only filter
try:
    _orig_render_vendors_subtab_phase3 = render_vendors
except Exception:
    _orig_render_vendors_subtab_phase3 = None

def render_vendors(opp_id: int):
    import streamlit as st
    flag_filt = st.session_state.get('feature_flags', {}).get('subfinder_filters', False)
    flag_src = st.session_state.get('feature_flags', {}).get('subfinder_sources', False)
    if _orig_render_vendors_subtab_phase3 and not (flag_filt or flag_src):
        try:
            return _orig_render_vendors_subtab_phase3(int(opp_id))
        except Exception:
            pass
    # If the phase-2 UI exists, call it to render controls and results first
    if _orig_render_vendors_subtab_phase3:
        try:
            _orig_render_vendors_subtab_phase3(int(opp_id))
        except Exception:
            pass
    # Augment controls for sources
    if flag_src:
        st.markdown('---')
        st.markdown('**Federal sources**')
        cols = st.columns(3)
        with cols[0]:
            awardees_only = st.checkbox('Only prior federal awardees', value=st.session_state.get('sub3_awardees_only', False))
        with cols[1]:
            naics_csv = st.text_input('NAICS for import (comma)', value=st.session_state.get('sub3_naics',''))
        with cols[2]:
            state = st.text_input('State for import (e.g., VA)', value=st.session_state.get('sub3_state',''))
        st.session_state['sub3_awardees_only']=awardees_only
        st.session_state['sub3_naics']=naics_csv
        st.session_state['sub3_state']=state
        if st.button('Import federal sources'):
            naics_list = [c.strip() for c in naics_csv.split(',') if c.strip()]
            res = subfinder_import_sources(int(opp_id), naics_list, state.strip().upper() if state else '')
            if res.get('ok'):
                st.success(f"Imported {res.get('imported',0)} records")
            else:
                st.error(f"Import failed: {res.get('error')}")
    # Display source chips for current rows and apply awardees-only filter
    rows = __import__('streamlit').session_state.get('sub1_results', []) or []
    if not rows:
        return
    conn = get_db(); cur = conn.cursor()
    # Preload vendor sources map
    src_by_vid = {}
    try:
        for vid, src in cur.execute('SELECT vendor_id, source FROM vendor_sources WHERE vendor_id IS NOT NULL').fetchall():
            if vid is None or not src:
                continue
            src_by_vid.setdefault(int(vid), set()).add(src)
    except Exception:
        pass
    filtered = []
    for it in rows:
        try:
            vid = _sub2_find_vendor_id(cur, it)
        except Exception:
            vid = None
        sources = sorted(list(src_by_vid.get(int(vid), set()))) if vid else []
        it['__sources'] = sources
        filtered.append(it)
    if flag_src and st.session_state.get('sub3_awardees_only', False):
        filtered = [it for it in filtered if any(s in ('usaspending','gsa') for s in it.get('__sources', []))]
    # Replace displayed list with filtered while keeping ordering from prior phase
    __import__('streamlit').session_state['sub1_results'] = filtered
    # Render chips under each row using the existing renderer logic from phase-2
    for it in filtered[: __import__('streamlit').session_state.get('sub1_page_size', 50) ]:
        pass  # actual row rendering already happens in the earlier call
# === SUB PHASE 3 END ===



# === SUB PHASE 4 START ===
import json as _json_sub4
import datetime as _dt_sub4

def _sub4_schema():
    conn = get_db(); cur = conn.cursor()
    # saved_searches.type column for vendor searches
    try:
        cols = [r[1] for r in cur.execute("PRAGMA table_info(saved_searches)").fetchall()]
        if "type" not in cols:
            cur.execute("ALTER TABLE saved_searches ADD COLUMN type TEXT")
            cur.execute("UPDATE saved_searches SET type='sam' WHERE type IS NULL")
    except Exception:
        pass
    conn.commit()

def _sub4_find_or_create_vendor(cur, item):
    # Reuse phase-2 matching helpers if available
    try:
        vid = _sub2_find_vendor_id(cur, item)
    except Exception:
        vid = None
    if vid is not None:
        return int(vid)
    # Minimal insert if schema allows
    cols = [r[1] for r in cur.execute("PRAGMA table_info(vendors)").fetchall()]
    fields = {}
    if "name" in cols: fields["name"] = item.get("name") or ""
    if "state" in cols:
        addr = item.get("formatted_address") or item.get("vicinity") or ""
        import re as _re_s4; m = _re_s4.search(r",\s*([A-Z]{2})(\s|,|$)", addr or "")
        state = m.group(1) if m else None
        fields["state"] = state
    if "domain" in cols:
        try:
            dom = _norm_domain(item.get("website") or item.get("url"))
        except Exception:
            dom = None
        fields["domain"] = dom
    if not fields:
        return None
    cols_sql = ",".join(fields.keys()); ph = ",".join(["?"]*len(fields))
    cur.execute(f"INSERT INTO vendors({cols_sql}) VALUES({ph})", tuple(fields.values()))
    return int(cur.lastrowid)

def _sub4_get_or_create_rfq(opp_id: int, owner_id: str):
    conn = get_db(); cur = conn.cursor()
    # If rfq table missing, abort
    cols = [r[1] for r in cur.execute("PRAGMA table_info(rfq)").fetchall()] if cur else []
    if not cols:
        return None
    # Try to find existing RFQ by notice_id/owner
    rfq_id = None
    try:
        if "notice_id" in cols and "owner_id" in cols:
            row = cur.execute("SELECT id FROM rfq WHERE notice_id=? AND owner_id=? ORDER BY id DESC LIMIT 1", (int(opp_id), owner_id)).fetchone()
            if row: rfq_id = int(row[0])
    except Exception:
        rfq_id = None
    if rfq_id is None:
        fields = {}
        if "notice_id" in cols: fields["notice_id"] = int(opp_id)
        if "owner_id" in cols: fields["owner_id"] = owner_id
        if "status" in cols: fields["status"] = "Draft"
        if "created_at" in cols: fields["created_at"] = _dt_sub4.datetime.utcnow().isoformat()
        if not fields:
            return None
        cols_sql = ",".join(fields.keys()); ph = ",".join(["?"]*len(fields))
        cur.execute(f"INSERT INTO rfq({cols_sql}) VALUES({ph})", tuple(fields.values()))
        conn.commit()
        rfq_id = int(cur.lastrowid)
    return rfq_id

def _sub4_send_rfqs(opp_id: int, vendor_items: list):
    conn = get_db(); cur = conn.cursor()
    rfq_id = _sub4_get_or_create_rfq(int(opp_id), __import__("streamlit").session_state.get("user_id") or "user")
    if not rfq_id:
        return {"ok": False, "error": "rfq_table_missing"}
    # Prepare invites table if present
    inv_cols = [r[1] for r in cur.execute("PRAGMA table_info(rfq_invites)").fetchall()] if cur else []
    created = 0; skipped = 0
    for it in vendor_items:
        vid = _sub4_find_or_create_vendor(cur, it)
        if not vid:
            skipped += 1; continue
        if inv_cols:
            # avoid duplicates
            try:
                row = cur.execute("SELECT id FROM rfq_invites WHERE rfq_id=? AND vendor_id=?", (int(rfq_id), int(vid))).fetchone()
                if row:
                    skipped += 1; continue
            except Exception:
                pass
            fields = {}
            if "rfq_id" in inv_cols: fields["rfq_id"] = int(rfq_id)
            if "vendor_id" in inv_cols: fields["vendor_id"] = int(vid)
            if "status" in inv_cols: fields["status"] = "Queued"
            if "created_at" in inv_cols: fields["created_at"] = _dt_sub4.datetime.utcnow().isoformat()
            cols_sql = ",".join(fields.keys()); ph = ",".join(["?"]*len(fields))
            try:
                cur.execute(f"INSERT INTO rfq_invites({cols_sql}) VALUES({ph})", tuple(fields.values()))
                created += 1
            except Exception:
                skipped += 1
        else:
            # Fallback: queue an email
            try:
                cur.execute("INSERT INTO email_queue(to_addr, subject, body, created_at, status, attempts, last_error) VALUES(?,?,?,?, 'queued', 0, NULL)",
                            (it.get("email") or "", f"RFQ Invite for opportunity {opp_id}", f"Please respond to RFQ {rfq_id}", _dt_sub4.datetime.utcnow().isoformat()))
                created += 1
            except Exception:
                skipped += 1
    conn.commit()
    return {"ok": True, "rfq_id": rfq_id, "invites_created": created, "skipped": skipped}

def _sub4_save_vendor_search(name: str, filters: dict, cadence: str="weekly", recipients: str=""):
    _sub4_schema()
    conn = get_db(); cur = conn.cursor()
    uid = __import__("streamlit").session_state.get("user_id") or "user"
    cur.execute("INSERT INTO saved_searches(user_id, name, query_json, cadence, recipients, active, last_run_at, type) VALUES(?,?,?,?,?,1,NULL,'vendors')",
                (uid, name, _json_sub4.dumps(filters), cadence, recipients or ""))
    conn.commit()
    return True

# Patch Vendors UI to add stars, send RFQs, and saved vendor searches
try:
    _orig_render_vendors_subtab_phase4 = render_vendors
except Exception:
    _orig_render_vendors_subtab_phase4 = None

def render_vendors(opp_id: int):
    import streamlit as st
    # Render existing UI first
    if _orig_render_vendors_subtab_phase4:
        try:
            _orig_render_vendors_subtab_phase4(int(opp_id))
        except Exception:
            pass
    st.session_state.setdefault("vendor_stars", {})
    rows = st.session_state.get("sub1_results", []) or []
    if rows:
        st.markdown("---")
        st.markdown("**Outreach**")
        # Star toggles inline list
        starred_keys = set(st.session_state.get("vendor_stars", {}).keys())
        for i, it in enumerate(rows[: st.session_state.get("sub1_page_size", 50) ]):
            key = (it.get("place_id") or "") + "|" + (it.get("name") or str(i))
            cols = st.columns([5,1])
            with cols[0]:
                nm = it.get("name") or ""
                st.caption(nm)
            with cols[1]:
                if key in starred_keys:
                    if st.button(f"★", key=f"unstar_{i}"):
                        st.session_state["vendor_stars"].pop(key, None)
                else:
                    if st.button(f"☆", key=f"star_{i}"):
                        st.session_state["vendor_stars"][key] = it
        star_count = len(st.session_state.get("vendor_stars", {}))
        c1,c2 = st.columns([2,2])
        with c1:
            st.caption(f"Starred: {star_count}")
        with c2:
            if st.session_state.get("feature_flags", {}).get("subfinder_outreach", False):
                if st.button(f"Send RFQs to {star_count} vendors", disabled=(star_count==0)):
                    res = _sub4_send_rfqs(int(opp_id), list(st.session_state["vendor_stars"].values()))
                    if res.get("ok"):
                        st.success(f"Created {res.get('invites_created',0)} invites for RFQ {res.get('rfq_id')}")
                    else:
                        st.error(f"RFQ send failed: {res.get('error')}")
    # Saved vendor searches
    _sub4_schema()
    with st.expander("Saved vendor searches"):
        st.caption("Save the current Vendor Finder inputs for reuse.")
        name = st.text_input("Search name", key="sub4_ss_name", value=st.session_state.get("sub4_ss_name","My Vendor Search"))
        cadence = st.selectbox("Cadence", ["weekly","monthly"], index=0, key="sub4_ss_cad")
        recips = st.text_input("Recipients (comma)", key="sub4_ss_recips", value=st.session_state.get("user_email","") or "")
        # gather current inputs if available
        filters = {
            "query": st.session_state.get("sub1_query","contractor"),
            "use_pop": st.session_state.get("sub1_use_pop", True),
            "radius": st.session_state.get("sub1_radius", 50),
            "page_size": st.session_state.get("sub1_page_size", 50),
            "naics": st.session_state.get("sub2_naics",""),
            "include": st.session_state.get("sub2_inc",""),
            "exclude": st.session_state.get("sub2_exc",""),
            "has_phone": st.session_state.get("sub2_has_phone", False),
            "has_web": st.session_state.get("sub2_has_web", False),
            "state": st.session_state.get("sub2_state",""),
        }
        if st.button("Save this vendor search"):
            try:
                _sub4_save_vendor_search(name, filters, cadence=cadence, recipients=recips)
                st.success("Saved")
            except Exception as ex:
                st.error(f"Save failed: {ex}")
        # Manager list
        conn = get_db(); cur = conn.cursor()
        rows = cur.execute("SELECT id, name, cadence, recipients, active, last_run_at, query_json FROM saved_searches WHERE user_id=? AND (type='vendors' OR type IS NULL) ORDER BY id DESC",
                           (st.session_state.get("user_id") or "user",)).fetchall()
        if not rows:
            st.caption("No saved vendor searches yet.")
        else:
            for rid, nm, cad, rcps, active, last_run, qj in rows:
                with st.expander(f"{nm} • {cad} • {'active' if active else 'inactive'}", expanded=False):
                    st.caption(f"Recipients: {rcps}")
                    c1,c2,c3,c4 = st.columns(4)
                    with c1:
                        if st.button("Load", key=f"load_{rid}"):
                            try:
                                f = _json_sub4.loads(qj or "{}")
                                st.session_state["sub1_query"]=f.get("query","contractor")
                                st.session_state["sub1_use_pop"]=f.get("use_pop", True)
                                st.session_state["sub1_radius"]=int(f.get("radius", 50))
                                st.session_state["sub1_page_size"]=int(f.get("page_size", 50))
                                st.session_state["sub2_naics"]=f.get("naics","")
                                st.session_state["sub2_inc"]=f.get("include","")
                                st.session_state["sub2_exc"]=f.get("exclude","")
                                st.session_state["sub2_has_phone"]=bool(f.get("has_phone", False))
                                st.session_state["sub2_has_web"]=bool(f.get("has_web", False))
                                st.session_state["sub2_state"]=f.get("state","")
                                st.experimental_rerun() if hasattr(st, "experimental_rerun") else st.rerun()
                            except Exception as ex:
                                st.error(f"Load failed: {ex}")
                    with c2:
                        if st.button("Activate" if not active else "Deactivate", key=f"act_{rid}"):
                            cur.execute("UPDATE saved_searches SET active=? WHERE id=?", (0 if active else 1, rid)); conn.commit()
                    with c3:
                        if st.button("Delete", key=f"del_{rid}"):
                            cur.execute("DELETE FROM saved_searches WHERE id=?", (rid,)); conn.commit()
                    with c4:
                        if st.button("Search now", key=f"run_{rid}"):
                            # run with current filters and refresh
                            st.experimental_rerun() if hasattr(st, "experimental_rerun") else st.rerun()
# === SUB PHASE 4 END ===




# === RFQG PHASE 1 START ===
import json as _json_rfqg
import datetime as _dt_rfqg
import io as _io_rfqg
import zipfile as _zip_rfqg
import os as _os_rfqg
import hashlib as _hash_rfqg
from pathlib import Path as _Path_rfqg

def _rfqg_flag():
    import streamlit as st
    return st.session_state.get("feature_flags", {}).get("rfqg_composer", False)

def _rfqg_schema():
    conn = get_db(); cur = conn.cursor()
    # rfq_terms
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS rfq_terms( id INTEGER PRIMARY KEY, rfq_id INTEGER NOT NULL REFERENCES rfq(id) ON DELETE CASCADE, pop_text TEXT, due_date TEXT, validity_days INTEGER, insurance TEXT, bonding TEXT, flowdowns_json TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL )")
    except Exception: pass
    # rfq_pack
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS rfq_pack( id INTEGER PRIMARY KEY, rfq_id INTEGER NOT NULL REFERENCES rfq(id) ON DELETE CASCADE, zip_path TEXT NOT NULL, cover_pdf_path TEXT, checksum TEXT NOT NULL, bytes INTEGER NOT NULL, created_at TEXT NOT NULL )")
    except Exception: pass
    conn.commit()

def _rfqg_latest_analyzer(notice_id: int):
    conn = get_db(); cur = conn.cursor()
    row = cur.execute("SELECT data_json FROM rfp_json WHERE notice_id=? ORDER BY id DESC LIMIT 1", (int(notice_id),)).fetchone()
    if not row: return {}
    try: return _json_rfqg.loads(row[0] or "{}")
    except Exception: return {}

def _rfqg_get_or_create_rfq(notice_id: int, owner_id: str):
    conn = get_db(); cur = conn.cursor()
    cols = [r[1] for r in cur.execute("PRAGMA table_info(rfq)").fetchall()] if cur else []
    if not cols:
        return None
    rfq_id = None
    try:
        if "notice_id" in cols and "owner_id" in cols:
            row = cur.execute("SELECT id FROM rfq WHERE notice_id=? AND owner_id=? ORDER BY id DESC LIMIT 1", (int(notice_id), owner_id)).fetchone()
            if row: rfq_id = int(row[0])
    except Exception: rfq_id = None
    if rfq_id is None:
        fields = {}
        if "notice_id" in cols: fields["notice_id"] = int(notice_id)
        if "owner_id" in cols: fields["owner_id"] = owner_id
        if "status" in cols: fields["status"] = "Draft"
        if "created_at" in cols: fields["created_at"] = _dt_rfqg.datetime.utcnow().isoformat()
        if not fields: return None
        cols_sql = ",".join(fields.keys()); ph = ",".join(["?"]*len(fields))
        cur.execute(f"INSERT INTO rfq({cols_sql}) VALUES({ph})", tuple(fields.values()))
        conn.commit()
        rfq_id = int(cur.lastrowid)
    return int(rfq_id)

def _rfqg_seed_lines_from_analyzer(notice_id: int, rfq_id: int):
    conn = get_db(); cur = conn.cursor()
    cols = [r[1] for r in cur.execute("PRAGMA table_info(rfq_lines)").fetchall()] if cur else []
    if not cols:
        return 0
    an = _rfqg_latest_analyzer(int(notice_id))
    created = 0
    # Helper to insert a line with best-effort columns
    def add_line(data: dict):
        nonlocal created
        fields = {}
        for k in ["rfq_id","clin","task_id","description","uom","qty","location"]:
            if k == "rfq_id" and "rfq_id" in cols: fields["rfq_id"] = int(rfq_id)
            elif k in cols and data.get(k) is not None: fields[k] = data.get(k)
        if not fields: return
        cols_sql = ",".join(fields.keys()); ph = ",".join(["?"]*len(fields))
        try:
            cur.execute(f"INSERT INTO rfq_lines({cols_sql}) VALUES({ph})", tuple(fields.values()))
            created += 1
        except Exception:
            pass
    # From SOW tasks
    for t in (an.get("sow_tasks") or [])[:500]:
        add_line({
            "task_id": t.get("task_id"),
            "description": t.get("text") or "",
            "uom": "hr" if t.get("hours_hint") else None,
            "qty": t.get("hours_hint"),
            "location": t.get("location")
        })
    # From CLIN hints
    clins = ((an.get("price_structure") or {}).get("clins") or [])
    for c in clins[:500]:
        add_line({
            "clin": c.get("clin"),
            "description": c.get("desc") or "",
            "uom": c.get("uom"),
            "qty": c.get("qty_hint")
        })
    conn.commit()
    return created

def _rfqg_seed_terms_from_notice(notice_id: int, rfq_id: int):
    conn = get_db(); cur = conn.cursor()
    _rfqg_schema()
    # existing?
    row = cur.execute("SELECT id FROM rfq_terms WHERE rfq_id=?", (int(rfq_id),)).fetchone()
    if row: return int(row[0])
    # gather
    n = cur.execute("SELECT title, place_city, place_state, due_at FROM notices WHERE id=?", (int(notice_id),)).fetchone()
    title = (n[0] if n else "") or ""
    pop = ""
    if n:
        city = n[1] or ""; state = n[2] or ""
        if city or state: pop = f"{city}, {state}".strip(", ")
    due = n[3] if n else None
    an = _rfqg_latest_analyzer(int(notice_id))
    # rudimentary clause classification for insurance/bonding and flowdowns
    insurance = ""; bonding = ""
    flows = []
    for cl in (an.get("clauses") or []):
        ref = (cl.get("ref") or "").lower(); titlec = (cl.get("title") or "").lower()
        if "insurance" in ref or "insurance" in titlec: insurance = cl.get("title") or cl.get("ref") or ""
        if "bond" in ref or "bond" in titlec: bonding = cl.get("title") or cl.get("ref") or ""
        flows.append({"ref": cl.get("ref"), "title": cl.get("title"), "mandatory": cl.get("mandatory"), "cite": cl.get("cite")})
    terms = {
        "pop_text": pop or "",
        "due_date": due or "",
        "validity_days": 30,
        "insurance": insurance or "",
        "bonding": bonding or "",
        "flowdowns_json": _json_rfqg.dumps(flows)
    }
    cur.execute("INSERT INTO rfq_terms(rfq_id, pop_text, due_date, validity_days, insurance, bonding, flowdowns_json, created_at, updated_at) VALUES(?,?,?,?,?,?,?,datetime('now'),datetime('now'))",
                (int(rfq_id), terms["pop_text"], terms["due_date"], int(terms["validity_days"]), terms["insurance"], terms["bonding"], terms["flowdowns_json"]))
    conn.commit()
    return int(cur.lastrowid)

def _rfqg_minimal_pdf(text_title: str, fields: dict, out_path: str):
    """Write a tiny one-page PDF with title and key fields. No external deps."""
    # Minimal PDF inspired by simple text objects
    title = (text_title or "RFQ Cover").encode("latin-1", "ignore")
    lines = [f"{k}: {v}" for k,v in fields.items() if v]
    body = ("\n".join(lines)).encode("latin-1", "ignore")
    # Build a minimal PDF
    # Coordinates and font ops
    def obj(n, s): return f"{n} 0 obj\n{s}\nendobj\n".encode("latin-1")
    content_stream = b"BT /F1 14 Tf 50 770 Td (" + title.replace(b"(", b"[").replace(b")", b"]") + b") Tj T* "                       b"/F1 10 Tf (" + body.replace(b"(", b"[").replace(b")", b"]") + b") Tj ET"
    xref = []
    parts = [b"%PDF-1.4\n"]

    # 1: font
    xref.append(sum(len(p) for p in parts)); parts.append(obj(1, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"))
    # 2: Resources
    xref.append(sum(len(p) for p in parts)); parts.append(obj(2, "<< /ProcSet [/PDF /Text] /Font << /F1 1 0 R >> >>"))
    # 3: Contents
    xref.append(sum(len(p) for p in parts)); parts.append(obj(3, f"<< /Length {len(content_stream)} >>\nstream\n".encode("latin-1") + content_stream + b"\nendstream\n"))
    # 4: Page
    xref.append(sum(len(p) for p in parts)); parts.append(obj(4, "<< /Type /Page /Parent 5 0 R /Resources 2 0 R /MediaBox [0 0 612 792] /Contents 3 0 R >>"))
    # 5: Pages
    xref.append(sum(len(p) for p in parts)); parts.append(obj(5, "<< /Type /Pages /Count 1 /Kids [4 0 R] >>"))
    # 6: Catalog
    xref.append(sum(len(p) for p in parts)); parts.append(obj(6, "<< /Type /Catalog /Pages 5 0 R >>"))

    xref_off = sum(len(p) for p in parts)
    parts.append(b"xref\n0 7\n0000000000 65535 f \n" +                  b"".join([f"{off:010} 00000 n \n".encode("latin-1") for off in xref]))
    parts.append(b"trailer\n<< /Size 7 /Root 6 0 R >>\nstartxref\n" + str(xref_off).encode("latin-1") + b"\n%%EOF")
    pdf_bytes = b"".join(parts)
    _Path_rfqg(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(pdf_bytes)
    return out_path

def _rfqg_build_pack(rfq_id: int, notice_id: int):
    conn = get_db(); cur = conn.cursor()
    # Cover PDF
    title = (cur.execute("SELECT title FROM notices WHERE id=?", (int(notice_id),)).fetchone() or [f"Notice {notice_id}"])[0]
    terms = cur.execute("SELECT pop_text, due_date, validity_days, insurance, bonding FROM rfq_terms WHERE rfq_id=?", (int(rfq_id),)).fetchone()
    fields = {"Place of performance": terms[0] if terms else "", "Due": terms[1] if terms else "", "Validity (days)": terms[2] if terms else "", "Insurance": terms[3] if terms else "", "Bonding": terms[4] if terms else ""}
    out_dir = _Path_rfqg("data/packs")/str(rfq_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    cover_path = str(out_dir/"cover.pdf")
    _rfqg_minimal_pdf(title, fields, cover_path)
    # Build SOW excerpt text
    an = _rfqg_latest_analyzer(int(notice_id))
    sow_txt = "\n".join([f"- {t.get('task_id') or ''}: {t.get('text') or ''}" for t in (an.get("sow_tasks") or [])[:200]])
    sow_bytes = sow_txt.encode("utf-8")
    # Zip assembly
    zip_path = str(out_dir/"rfq_pack.zip")
    with _zip_rfqg.ZipFile(zip_path, "w", compression=_zip_rfqg.ZIP_DEFLATED) as z:
        z.write(cover_path, arcname="cover.pdf")
        z.writestr("sow_excerpt.txt", sow_bytes)
        # include any drawing-like files if present in notice_files
        try:
            for fid, fn, url in cur.execute("SELECT id, file_name, file_url FROM notice_files WHERE notice_id=?", (int(notice_id),)).fetchall():
                if any(fn.lower().endswith(ext) for ext in [".pdf",".dwg",".png",".jpg",".jpeg"]):
                    # we do not download here to keep pure; include a pointer text
                    z.writestr(f"drawings/{fn}.txt", f"Download from: {url}".encode("utf-8"))
        except Exception:
            pass
    # checksum
    h = _hash_rfqg.sha256()
    with open(zip_path, "rb") as f:
        while True:
            b = f.read(65536)
            if not b: break
            h.update(b)
    checksum = h.hexdigest()
    sz = _Path_rfqg(zip_path).stat().st_size
    # store
    cur.execute("INSERT INTO rfq_pack(rfq_id, zip_path, cover_pdf_path, checksum, bytes, created_at) VALUES(?,?,?,?,?,datetime('now'))", (int(rfq_id), zip_path, cover_path, checksum, int(sz)))
    conn.commit()
    return {"zip_path": zip_path, "checksum": checksum, "bytes": sz}

# UI
try:
    _orig_render_vendors_or_none_rfqg = render_vendors
except Exception:
    _orig_render_vendors_or_none_rfqg = None

def render_rfq_generator(opp_id: int):
    import streamlit as st
    if not _rfqg_flag():
        return
    _rfqg_schema()
    st.subheader("RFQ Generator")
    owner = st.session_state.get("user_id") or "user"
    rfq_id = _rfqg_get_or_create_rfq(int(opp_id), owner)
    if not rfq_id:
        st.warning("RFQ table missing."); return
    # Seed on first open
    if st.button("Seed lines and terms from Analyzer"):
        _rfqg_seed_lines_from_analyzer(int(opp_id), int(rfq_id))
        _rfqg_seed_terms_from_notice(int(opp_id), int(rfq_id))
        st.success("Seeded from analyzer.")
        (st.experimental_rerun() if hasattr(st,"experimental_rerun") else st.rerun())
    # Header form
    conn = get_db(); cur = conn.cursor()
    tid = _rfqg_seed_terms_from_notice(int(opp_id), int(rfq_id))
    row = cur.execute("SELECT id, pop_text, due_date, validity_days, insurance, bonding, flowdowns_json FROM rfq_terms WHERE rfq_id=?", (int(rfq_id),)).fetchone()
    if row:
        _, pop, due, valid, ins, bond, flows_j = row
        c1,c2,c3 = st.columns(3)
        with c1: pop2 = st.text_input("Place of performance", value=pop or "")
        with c2: due2 = st.text_input("Due date", value=due or "")
        with c3: valid2 = st.number_input("Validity (days)", min_value=0, max_value=365, value=int(valid or 30))
        c4,c5 = st.columns(2)
        with c4: ins2 = st.text_input("Insurance", value=ins or "")
        with c5: bond2 = st.text_input("Bonding", value=bond or "")
        if st.button("Save terms"):
            cur.execute("UPDATE rfq_terms SET pop_text=?, due_date=?, validity_days=?, insurance=?, bonding=?, updated_at=datetime('now') WHERE rfq_id=?",
                        (pop2, due2, int(valid2), ins2, bond2, int(rfq_id))); get_db().commit(); st.success("Saved.")
    # Two-column layout: left lines, right flowdowns
    lcol, rcol = st.columns([3,2])
    with lcol:
        st.markdown("**Lines**")
        # Render simple table with edit ability
        cols = [r[1] for r in cur.execute("PRAGMA table_info(rfq_lines)").fetchall()] if cur else []
        if cols:
            rows = cur.execute("SELECT id, clin, task_id, description, uom, qty, location FROM rfq_lines WHERE rfq_id=? ORDER BY id", (int(rfq_id),)).fetchall()
            for rid, clin, task_id, desc, uom, qty, loc in rows[:500]:
                with st.container(border=True):
                    c1,c2 = st.columns([3,1])
                    with c1: desc2 = st.text_input("Description", value=desc or "", key=f"rfql_desc_{rid}")
                    with c2: qty2 = st.number_input("Qty", value=float(qty) if qty is not None else 0.0, key=f"rfql_qty_{rid}")
                    c3,c4,c5 = st.columns(3)
                    with c3: uom2 = st.text_input("UOM", value=uom or "", key=f"rfql_uom_{rid}")
                    with c4: clin2 = st.text_input("CLIN", value=clin or "", key=f"rfql_clin_{rid}")
                    with c5: loc2 = st.text_input("Location", value=loc or "", key=f"rfql_loc_{rid}")
                    if st.button("Save line", key=f"rfql_save_{rid}"):
                        cur.execute("UPDATE rfq_lines SET description=?, qty=?, uom=?, clin=?, location=? WHERE id=?",
                                    (desc2, None if qty2==0 else float(qty2), uom2, clin2, loc2, int(rid))); get_db().commit(); st.success("Saved line")    
        if st.button("Build RFQ pack"):
            res = _rfqg_build_pack(int(rfq_id), int(opp_id))
            st.success(f"Pack built. SHA256 {res.get('checksum')} Bytes {res.get('bytes')}")
    with rcol:
        st.markdown("**Flowdowns**")
        an = _rfqg_latest_analyzer(int(opp_id))
        for cl in (an.get("clauses") or [])[:200]:
            cite = cl.get("cite") or {}
            c = f"{cite.get('file','')} p.{cite.get('page')}" if (cite.get('file') or cite.get('page') is not None) else ""
            st.caption(f"{cl.get('ref') or ''} {cl.get('title') or ''} {('• ' + c) if c else ''}")

# Attach generator UI below Vendors tab if available
try:
    _orig_render_vendors_for_rfqg = render_vendors
except Exception:
    _orig_render_vendors_for_rfqg = None

def render_vendors(opp_id: int):
    import streamlit as st
    if _orig_render_vendors_for_rfqg:
        try: _orig_render_vendors_for_rfqg(int(opp_id))
        except Exception: pass
    if _rfqg_flag():
        st.markdown("---")
        render_rfq_generator(int(opp_id))
# === RFQG PHASE 1 END ===



# === RFQG PHASE 2 START ===
import os as _os_rfqg2
import hashlib as _hash_rfqg2
import datetime as _dt_rfqg2

def _rfqg2_flag():
    import streamlit as st
    return st.session_state.get('feature_flags', {}).get('rfqg_outreach', False)

def _rfqg2_schema():
    conn = get_db(); cur = conn.cursor()
    # vendor_portal_tokens table
    try:
        cur.execute('CREATE TABLE IF NOT EXISTS vendor_portal_tokens( id INTEGER PRIMARY KEY, rfq_id INTEGER NOT NULL, vendor_id INTEGER NOT NULL, token TEXT NOT NULL UNIQUE, expires_at TEXT, created_at TEXT NOT NULL )')
    except Exception:
        pass
    # rfq_invites table best-effort
    try:
        cur.execute('CREATE TABLE IF NOT EXISTS rfq_invites( id INTEGER PRIMARY KEY, rfq_id INTEGER NOT NULL, vendor_id INTEGER NOT NULL, status TEXT, created_at TEXT NOT NULL )')
    except Exception:
        pass
    conn.commit()

def _rfqg2_get_token(rfq_id: int, vendor_id: int, days_valid: int=14) -> str:
    conn = get_db(); cur = conn.cursor()
    row = cur.execute('SELECT token, expires_at FROM vendor_portal_tokens WHERE rfq_id=? AND vendor_id=? ORDER BY id DESC LIMIT 1', (int(rfq_id), int(vendor_id))).fetchone()
    if row:
        return row[0]
    # issue new token
    tok = _hash_rfqg2.sha256(f'{rfq_id}:{vendor_id}:{_dt_rfqg2.datetime.utcnow().isoformat()}'.encode('utf-8')).hexdigest()[:32]
    exp = (_dt_rfqg2.datetime.utcnow() + _dt_rfqg2.timedelta(days=int(days_valid))).isoformat()
    cur.execute('INSERT INTO vendor_portal_tokens(rfq_id, vendor_id, token, expires_at, created_at) VALUES(?,?,?,?,datetime(\'now\'))', (int(rfq_id), int(vendor_id), tok, exp))
    conn.commit()
    return tok

def _rfqg2_vendor_email(cur, vendor_id: int):
    # Try vendors.email then contacts table if exists
    try:
        row = cur.execute('PRAGMA table_info(vendors)').fetchall()
        cols = {r[1] for r in row}
        if 'email' in cols:
            r = cur.execute('SELECT email FROM vendors WHERE id=?', (int(vendor_id),)).fetchone()
            if r and r[0]: return r[0]
    except Exception:
        pass
    try:
        row = cur.execute('PRAGMA table_info(contacts)').fetchall()
        cols = {r[1] for r in row}
        if 'vendor_id' in cols and 'email' in cols:
            r = cur.execute('SELECT email FROM contacts WHERE vendor_id=? ORDER BY id DESC LIMIT 1', (int(vendor_id),)).fetchone()
            if r and r[0]: return r[0]
    except Exception:
        pass
    return ''

def _rfqg2_target(items: list, starred_only: bool, min_score: float, max_distance: float):
    import streamlit as st
    rows = items or []
    if starred_only:
        stars = st.session_state.get('vendor_stars', {})
        starset = set(stars.keys())
        def k(it):
            return ( (it.get('place_id') or '') + '|' + (it.get('name') or '') )
        rows = [it for it in rows if k(it) in starset]
    out = []
    for it in rows:
        sc = it.get('fit_score')
        sc = float(sc) if sc is not None else 0.0
        if sc < float(min_score):
            continue
        dm = it.get('distance_mi')
        dm = float(dm) if dm is not None else 1e12
        if max_distance and dm > float(max_distance):
            continue
        out.append(it)
    return out

def _rfqg2_render_outreach_panel(opp_id: int, rfq_id: int):
    import streamlit as st
    if not _rfqg2_flag():
        return
    _rfqg2_schema()
    st.markdown('---')
    st.markdown('**Target vendors**')
    rows = st.session_state.get('sub1_results', []) or []
    starred_only = st.checkbox('Starred only', value=False, key='rfqg2_starred')
    min_score = st.slider('Min fit score', 0.0, 10.0, 0.0, 0.5, key='rfqg2_minscore')
    max_distance = st.slider('Max distance (mi)', 0, 300, 150, key='rfqg2_maxdist')
    targets = _rfqg2_target(rows, starred_only, min_score, max_distance)
    st.caption(f'{len(targets)} vendors selected')
    # Subject and body templates
    st.markdown('**Email template**')
    subj_t = st.text_input('Subject', value='RFQ: {title} — reply by {due_date}', key='rfqg2_subj')
    body_t = st.text_area('Body', value='Hello {company},\n\nWe invite you to quote for {title}. Please submit by {due_date}.\nOpen your secure link: {link}\n\nThank you.', key='rfqg2_body', height=140)
    # Preview first three
    conn = get_db(); cur = conn.cursor()
    title = (cur.execute('SELECT title FROM notices WHERE id=?', (int(opp_id),)).fetchone() or ['Opportunity'])[0]
    due = (cur.execute('SELECT due_date FROM rfq_terms WHERE rfq_id=?', (int(rfq_id),)).fetchone() or [''])[0]
    app_base = ''
    try:
        import streamlit as st2
        app_base = st2.secrets.get('app',{}).get('base_url','')
    except Exception:
        app_base = ''
    st.caption('Preview')
    for it in targets[:3]:
        # resolve or create vendor id
        try:
            vid = _sub2_find_vendor_id(cur, it)
        except Exception:
            vid = None
        if vid is None:
            try: vid = _sub4_find_or_create_vendor(cur, it)
            except Exception: vid = None
        if vid is None:
            continue
        tok = _rfqg2_get_token(int(rfq_id), int(vid))
        link = (app_base.rstrip('/') + '/?page=vendor_portal&token=' + tok) if app_base else ('/?page=vendor_portal&token=' + tok)
        subject = subj_t.format(title=title, due_date=due, company=(it.get('name') or 'Vendor'))
        body = body_t.format(title=title, due_date=due, company=(it.get('name') or 'Vendor'), link=link)
        st.code(subject + '\n\n' + body)
    # Send button
    if st.button(f'Send RFQs to {len(targets)} vendors', disabled=(len(targets)==0)):
        created = 0; skipped = 0
        inv_cols = [r[1] for r in cur.execute('PRAGMA table_info(rfq_invites)').fetchall()] if cur else []
        for it in targets:
            try: vid = _sub2_find_vendor_id(cur, it)
            except Exception: vid = None
            if vid is None:
                try: vid = _sub4_find_or_create_vendor(cur, it)
                except Exception: vid = None
            if vid is None:
                skipped += 1; continue
            tok = _rfqg2_get_token(int(rfq_id), int(vid))
            link = (app_base.rstrip('/') + '/?page=vendor_portal&token=' + tok) if app_base else ('/?page=vendor_portal&token=' + tok)
            subject = subj_t.format(title=title, due_date=due, company=(it.get('name') or 'Vendor'))
            body = body_t.format(title=title, due_date=due, company=(it.get('name') or 'Vendor'), link=link)
            # rfq_invites insert if exists
            if inv_cols:
                try:
                    # avoid duplicates
                    row = cur.execute('SELECT id FROM rfq_invites WHERE rfq_id=? AND vendor_id=?', (int(rfq_id), int(vid))).fetchone()
                    if not row:
                        cur.execute('INSERT INTO rfq_invites(rfq_id, vendor_id, status, created_at) VALUES(?,?,\'Queued\', datetime(\'now\'))', (int(rfq_id), int(vid)))
                except Exception:
                    pass
            # queue email
            try:
                cur.execute('INSERT INTO email_queue(to_addr, subject, body, created_at, status, attempts) VALUES(?,?,?,?,\'queued\',0)', (_rfqg2_vendor_email(cur,int(vid)) or '', subject, body, _dt_rfqg2.datetime.utcnow().isoformat()))
            except Exception:
                skipped += 1; continue
            created += 1
        get_db().commit()
        st.success(f'Queued {created} emails; skipped {skipped}.')

# Hook into RFQ Generator UI if present
try:
    _orig_render_rfq_generator_phase2 = render_rfq_generator
except Exception:
    _orig_render_rfq_generator_phase2 = None

def render_rfq_generator(opp_id: int):
    import streamlit as st
    rid = None
    if _orig_render_rfq_generator_phase2:
        try:
            _orig_render_rfq_generator_phase2(int(opp_id))
        except Exception:
            pass
    # retrieve existing rfq_id for current user if any
    try:
        owner = st.session_state.get('user_id') or 'user'
        row = get_db().cursor().execute('SELECT id FROM rfq WHERE notice_id=? AND owner_id=? ORDER BY id DESC LIMIT 1', (int(opp_id), owner)).fetchone()
        rid = int(row[0]) if row else None
    except Exception:
        rid = None
    if rid is not None:
        _rfqg2_render_outreach_panel(int(opp_id), int(rid))
# === RFQG PHASE 2 END ===



# === RFQG PHASE 3 START ===
import json as _json_rfqg3
import datetime as _dt_rfqg3
import os as _os_rfqg3
import hashlib as _hash_rfqg3
from pathlib import Path as _Path_rfqg3

def _rfqg3_flag():
    import streamlit as st
    return st.session_state.get("feature_flags", {}).get("rfqg_intake", False)

def _rfqg3_schema():
    conn = get_db(); cur = conn.cursor()
    # quotes header
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS vendor_quotes( id INTEGER PRIMARY KEY, rfq_id INTEGER NOT NULL, vendor_id INTEGER NOT NULL, status TEXT NOT NULL, total_price REAL, exceptions TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, submitted_at TEXT )")
    except Exception: pass
    # quote lines
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS vendor_quote_lines( id INTEGER PRIMARY KEY, quote_id INTEGER NOT NULL, rfq_line_id INTEGER NOT NULL, ext_price REAL, notes TEXT )")
    except Exception: pass
    # docs
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS vendor_docs( id INTEGER PRIMARY KEY, vendor_id INTEGER NOT NULL, rfq_id INTEGER NOT NULL, file_name TEXT NOT NULL, path TEXT NOT NULL, bytes INTEGER NOT NULL, checksum TEXT NOT NULL, uploaded_at TEXT NOT NULL )")
    except Exception: pass
    # portal tokens: add last_opened_at if missing
    try:
        cols = [r[1] for r in cur.execute("PRAGMA table_info(vendor_portal_tokens)").fetchall()]
        if "last_opened_at" not in cols:
            cur.execute("ALTER TABLE vendor_portal_tokens ADD COLUMN last_opened_at TEXT")
    except Exception: pass
    conn.commit()

def _rfqg3_get_or_create_quote(rfq_id: int, vendor_id: int, status: str="draft"):
    _rfqg3_schema()
    conn = get_db(); cur = conn.cursor()
    row = cur.execute("SELECT id, status FROM vendor_quotes WHERE rfq_id=? AND vendor_id=? ORDER BY id DESC LIMIT 1", (int(rfq_id), int(vendor_id))).fetchone()
    if row:
        qid = int(row[0]); st = row[1] or "draft"
        if st != status and status in ("draft","submitted"):
            cur.execute("UPDATE vendor_quotes SET status=?, updated_at=datetime('now') WHERE id=?", (status, qid)); conn.commit()
        return qid
    cur.execute("INSERT INTO vendor_quotes(rfq_id, vendor_id, status, total_price, exceptions, created_at, updated_at) VALUES(?,?,?,?,?,datetime('now'),datetime('now'))",
                (int(rfq_id), int(vendor_id), status, None, None))
    conn.commit()
    return int(cur.lastrowid)

def _rfqg3_set_quote_lines(quote_id: int, lines: list):
    conn = get_db(); cur = conn.cursor()
    # Clear existing lines for idempotency
    try: cur.execute("DELETE FROM vendor_quote_lines WHERE quote_id=?", (int(quote_id),))
    except Exception: pass
    created = 0; total = 0.0
    for ln in lines or []:
        rfq_line_id = int(ln.get("rfq_line_id"))
        ext_price = float(ln.get("ext_price") or 0.0)
        notes = ln.get("notes")
        try:
            cur.execute("INSERT INTO vendor_quote_lines(quote_id, rfq_line_id, ext_price, notes) VALUES(?,?,?,?)", (int(quote_id), rfq_line_id, ext_price, notes))
            created += 1; total += ext_price
        except Exception:
            pass
    try:
        cur.execute("UPDATE vendor_quotes SET total_price=?, updated_at=datetime('now') WHERE id=?", (float(total), int(quote_id)))
    except Exception: pass
    conn.commit()
    return {"lines_created": created, "total": total}

def record_portal_open(token: str):
    _rfqg3_schema()
    conn = get_db(); cur = conn.cursor()
    row = cur.execute("SELECT rfq_id, vendor_id FROM vendor_portal_tokens WHERE token=?", (token,)).fetchone()
    if not row: return {"ok": False, "error": "invalid_token"}
    cur.execute("UPDATE vendor_portal_tokens SET last_opened_at=datetime('now') WHERE token=?", (token,))
    conn.commit()
    return {"ok": True, "rfq_id": int(row[0]), "vendor_id": int(row[1])}

def portal_save_draft(token: str, payload_json: str):
    """payload_json: {'lines':[{'rfq_line_id':..., 'ext_price':..., 'notes':...}], 'exceptions': '...'}"""
    _rfqg3_schema()
    conn = get_db(); cur = conn.cursor()
    row = cur.execute("SELECT rfq_id, vendor_id FROM vendor_portal_tokens WHERE token=?", (token,)).fetchone()
    if not row: return {"ok": False, "error": "invalid_token"}
    rfq_id, vendor_id = int(row[0]), int(row[1])
    qid = _rfqg3_get_or_create_quote(rfq_id, vendor_id, status="draft")
    try:
        data = _json_rfqg3.loads(payload_json or "{}")
    except Exception:
        data = {}
    lines = data.get("lines") or []
    res = _rfqg3_set_quote_lines(qid, lines)
    ex = data.get("exceptions")
    try:
        cur.execute("UPDATE vendor_quotes SET exceptions=?, updated_at=datetime('now') WHERE id=?", (ex, int(qid)))
    except Exception: pass
    conn.commit()
    return {"ok": True, "quote_id": int(qid), **res}

def portal_submit_final(token: str, payload_json: str):
    out = portal_save_draft(token, payload_json)
    if not out.get("ok"): return out
    conn = get_db(); cur = conn.cursor()
    try:
        cur.execute("UPDATE vendor_quotes SET status='submitted', submitted_at=datetime('now'), updated_at=datetime('now') WHERE id=?", (int(out["quote_id"]),))
        conn.commit()
    except Exception: pass
    return {"ok": True, **out}

def _rfqg3_upload_vendor_pdf(rfq_id: int, vendor_id: int, file_obj, filename: str):
    base = _Path_rfqg3("data/files") / "rfq" / str(rfq_id) / "vendor" / str(vendor_id)
    base.mkdir(parents=True, exist_ok=True)
    path = base / filename
    data = file_obj.read()
    with open(path, "wb") as f:
        f.write(data)
    h = _hash_rfqg3.sha256(data).hexdigest()
    conn = get_db(); cur = conn.cursor()
    cur.execute("INSERT INTO vendor_docs(vendor_id, rfq_id, file_name, path, bytes, checksum, uploaded_at) VALUES(?,?,?,?,?,?,datetime('now'))",
                (int(vendor_id), int(rfq_id), filename, str(path), len(data), h))
    conn.commit()
    return str(path), h, len(data)

def _rfqg3_status_label(cur, rfq_id: int, vendor_id: int):
    # submitted > draft > opened > expired > not opened
    row = cur.execute("SELECT status FROM vendor_quotes WHERE rfq_id=? AND vendor_id=? ORDER BY id DESC LIMIT 1", (int(rfq_id), int(vendor_id))).fetchone()
    if row:
        st = (row[0] or "").lower()
        if st == "submitted": return "submitted"
        if st == "draft": return "draft"
    tok = cur.execute("SELECT expires_at, last_opened_at FROM vendor_portal_tokens WHERE rfq_id=? AND vendor_id=? ORDER BY id DESC LIMIT 1", (int(rfq_id), int(vendor_id))).fetchone()
    if tok:
        exp, op = tok[0], tok[1]
        if op: return "opened"
        try:
            if exp and _dt_rfqg3.datetime.fromisoformat(exp) < _dt_rfqg3.datetime.utcnow(): return "expired"
        except Exception: pass
    return "not opened"

def _rfqg3_responses_panel(opp_id: int, rfq_id: int):
    import streamlit as st
    if not _rfqg3_flag():
        return
    _rfqg3_schema()
    st.subheader("RFQ Responses")
    conn = get_db(); cur = conn.cursor()
    # Vendor status list from invites
    vendors = []
    try:
        rows = cur.execute("""SELECT v.id, COALESCE(v.name,''), COALESCE(v.email,'' )
                               FROM rfq_invites i JOIN vendors v ON v.id=i.vendor_id
                               WHERE i.rfq_id=? ORDER BY v.name""", (int(rfq_id),)).fetchall()
        for vid, nm, em in rows:
            vendors.append((int(vid), nm, em))
    except Exception:
        # fallback to any vendors with quotes/tokens
        rows = cur.execute("SELECT DISTINCT vendor_id FROM vendor_quotes WHERE rfq_id=?", (int(rfq_id),)).fetchall()
        for (vid,) in rows:
            nm = (cur.execute("SELECT name FROM vendors WHERE id=?", (int(vid),)).fetchone() or ["Vendor"])[0]
            em = (cur.execute("SELECT email FROM vendors WHERE id=?", (int(vid),)).fetchone() or [""])[0]
            vendors.append((int(vid), nm, em))
    # Status table
    if vendors:
        for vid, nm, em in vendors[:500]:
            status = _rfqg3_status_label(cur, int(rfq_id), int(vid))
            with st.container(border=True):
                st.markdown(f"**{nm}**  ·  {status}")
                # show latest quote total if any
                row = cur.execute("SELECT total_price, status, updated_at FROM vendor_quotes WHERE rfq_id=? AND vendor_id=? ORDER BY id DESC LIMIT 1", (int(rfq_id), int(vid))).fetchone()
                if row and row[0] is not None:
                    st.caption(f"Total: {row[0]:.2f}  •  updated {row[2]}")
                # Manual intake form
                with st.expander("Manual intake / update"):
                    # fetch rfq_lines
                    lines = cur.execute("SELECT id, description FROM rfq_lines WHERE rfq_id=? ORDER BY id", (int(rfq_id),)).fetchall()
                    price_inputs = {}
                    for lid, desc in lines[:200]:
                        price_inputs[lid] = st.number_input(f"Ext price — {desc[:60]}", min_value=0.0, step=1.0, key=f"qi_{vid}_{lid}")
                    ex = st.text_area("Exceptions / notes", key=f"exc_{vid}")
                    up = st.file_uploader("Attach vendor PDF(s)", type=["pdf","doc","docx"], accept_multiple_files=True, key=f"up_{vid}")
                    c1,c2 = st.columns(2)
                    with c1:
                        if st.button("Save draft", key=f"save_{vid}"):
                            qid = _rfqg3_get_or_create_quote(int(rfq_id), int(vid), status="draft")
                            lines_payload = [{"rfq_line_id": lid, "ext_price": price_inputs[lid], "notes": None} for lid in price_inputs.keys()]
                            _rfqg3_set_quote_lines(int(qid), lines_payload)
                            cur.execute("UPDATE vendor_quotes SET exceptions=?, updated_at=datetime('now') WHERE id=?", (ex, int(qid))); get_db().commit()
                            st.success("Draft saved")
                    with c2:
                        if st.button("Submit final", key=f"submit_{vid}"):
                            qid = _rfqg3_get_or_create_quote(int(rfq_id), int(vid), status="submitted")
                            lines_payload = [{"rfq_line_id": lid, "ext_price": price_inputs[lid], "notes": None} for lid in price_inputs.keys()]
                            _rfqg3_set_quote_lines(int(qid), lines_payload)
                            cur.execute("UPDATE vendor_quotes SET exceptions=?, submitted_at=datetime('now'), updated_at=datetime('now') WHERE id=?", (ex, int(qid))); get_db().commit()
                            st.success("Submitted")
                    if up:
                        for f in up:
                            p,h,b = _rfqg3_upload_vendor_pdf(int(rfq_id), int(vid), f, f.name)
                        st.info(f"Uploaded {len(up)} file(s)")
    else:
        st.caption("No vendor invites or quotes yet.")

# Hook into RFQ Generator
try:
    _orig_render_rfq_generator_phase3 = render_rfq_generator
except Exception:
    _orig_render_rfq_generator_phase3 = None

def render_rfq_generator(opp_id: int):
    import streamlit as st
    rid = None
    if _orig_render_rfq_generator_phase3:
        try:
            _orig_render_rfq_generator_phase3(int(opp_id))
        except Exception:
            pass
    # find rfq id
    try:
        owner = st.session_state.get('user_id') or 'user'
        row = get_db().cursor().execute('SELECT id FROM rfq WHERE notice_id=? AND owner_id=? ORDER BY id DESC LIMIT 1', (int(opp_id), owner)).fetchone()
        rid = int(row[0]) if row else None
    except Exception:
        rid = None
    if rid is not None:
        _rfqg3_responses_panel(int(opp_id), int(rid))
# === RFQG PHASE 3 END ===



# === VENDOR RFQ HOOKS PHASE 8 START ===
import json as _json_p8
import datetime as _dt_p8
import hashlib as _hash_p8
from pathlib import Path as _Path_p8

def _p8_flag():
    import streamlit as st
    return st.session_state.get('feature_flags', {}).get('vendor_rfq_hooks', False)

def _p8_schema():
    conn = get_db(); cur = conn.cursor()
    # Vendors table and columns
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS vendors( id INTEGER PRIMARY KEY, name TEXT NOT NULL, cage TEXT, uei TEXT, naics TEXT, city TEXT, state TEXT, phone TEXT, email TEXT, website TEXT, notes TEXT, last_seen_award TEXT )")
    except Exception: pass
    # Add missing vendor columns if table existed earlier
    try:
        cols = {r[1] for r in cur.execute('PRAGMA table_info(vendors)').fetchall()}
        for col, ddl in [
            ('cage', "ALTER TABLE vendors ADD COLUMN cage TEXT"),
            ('uei', "ALTER TABLE vendors ADD COLUMN uei TEXT"),
            ('naics', "ALTER TABLE vendors ADD COLUMN naics TEXT"),
            ('city', "ALTER TABLE vendors ADD COLUMN city TEXT"),
            ('state', "ALTER TABLE vendors ADD COLUMN state TEXT"),
            ('email', "ALTER TABLE vendors ADD COLUMN email TEXT"),
            ('website', "ALTER TABLE vendors ADD COLUMN website TEXT"),
            ('notes', "ALTER TABLE vendors ADD COLUMN notes TEXT"),
            ('last_seen_award', "ALTER TABLE vendors ADD COLUMN last_seen_award TEXT"),
        ]:
            if col not in cols:
                try: cur.execute(ddl)
                except Exception: pass
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendors_naics_state ON vendors(naics, state)")
    except Exception: pass
    # Contacts
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS vendor_contacts( id INTEGER PRIMARY KEY, vendor_id INTEGER NOT NULL REFERENCES vendors(id) ON DELETE CASCADE, name TEXT, email TEXT, phone TEXT, role TEXT )")
    except Exception: pass
    # RFQ core
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS rfq( id INTEGER PRIMARY KEY, notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE, owner_id TEXT NOT NULL, sent_at TEXT, due_at TEXT, scope TEXT, attachments_json TEXT )")
    except Exception: pass
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS rfq_lines( id INTEGER PRIMARY KEY, rfq_id INTEGER NOT NULL REFERENCES rfq(id) ON DELETE CASCADE, item TEXT NOT NULL, uom TEXT, qty REAL NOT NULL )")
    except Exception: pass
    # Quotes header (augment existing schema if present)
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS vendor_quotes( id INTEGER PRIMARY KEY, rfq_id INTEGER NOT NULL REFERENCES rfq(id) ON DELETE CASCADE, vendor_id INTEGER NOT NULL REFERENCES vendors(id) ON DELETE CASCADE, received_at TEXT, valid_through TEXT, total REAL, doc_id TEXT, coverage_score REAL DEFAULT 0 )")
    except Exception: pass
    try:
        cols = {r[1] for r in cur.execute('PRAGMA table_info(vendor_quotes)').fetchall()}
        for col, ddl in [
            ('received_at', "ALTER TABLE vendor_quotes ADD COLUMN received_at TEXT"),
            ('valid_through', "ALTER TABLE vendor_quotes ADD COLUMN valid_through TEXT"),
            ('total', "ALTER TABLE vendor_quotes ADD COLUMN total REAL"),
            ('doc_id', "ALTER TABLE vendor_quotes ADD COLUMN doc_id TEXT"),
            ('coverage_score', "ALTER TABLE vendor_quotes ADD COLUMN coverage_score REAL DEFAULT 0"),
        ]:
            if col not in cols:
                try: cur.execute(ddl)
                except Exception: pass
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_quote_vendor_rfq ON vendor_quotes(rfq_id, vendor_id)")
    except Exception: pass
    # Vendor scores
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS vendor_scores( id INTEGER PRIMARY KEY, vendor_id INTEGER NOT NULL REFERENCES vendors(id) ON DELETE CASCADE, fit_score REAL, on_time_rate REAL, insurance_ok INTEGER, cpars_note TEXT, updated_at TEXT )")
    except Exception: pass
    # Chasing
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS rfq_chase( id INTEGER PRIMARY KEY, rfq_id INTEGER NOT NULL REFERENCES rfq(id) ON DELETE CASCADE, vendor_id INTEGER NOT NULL REFERENCES vendors(id) ON DELETE CASCADE, next_action TEXT NOT NULL, due_at TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN('pending','done')) )")
    except Exception: pass
    conn.commit()

def _p8_get_or_create_rfq(notice_id: int, owner_id: str):
    conn = get_db(); cur = conn.cursor()
    row = cur.execute('SELECT id FROM rfq WHERE notice_id=? AND owner_id=? ORDER BY id DESC LIMIT 1', (int(notice_id), owner_id)).fetchone()
    if row: return int(row[0])
    cur.execute("INSERT INTO rfq(notice_id, owner_id, sent_at, due_at, scope, attachments_json) VALUES(?,?,?,?,?,?)",
                (int(notice_id), owner_id, None, None, None, None))
    conn.commit(); return int(cur.lastrowid)

def _p8_seed_vendors_for_notice(notice_id: int, limit: int=100):
    conn = get_db(); cur = conn.cursor()
    _p8_schema()
    # get notice NAICS and state
    row = cur.execute('SELECT naics, psc, place_state FROM notices WHERE id=?', (int(notice_id),)).fetchone()
    naics = (row[0] or '') if row else ''; state = (row[2] or '') if row else ''
    # If subfinder results exist in session, prefer those as seed
    try:
        import streamlit as st
        results = st.session_state.get('sub1_results', []) or []
    except Exception:
        results = []
    created = 0
    # helper insert unique by name+state
    def upsert_from_item(it):
        nonlocal created
        name = (it.get('name') or '').strip()
        st = state or ''
        if not name: return
        r = cur.execute('SELECT id FROM vendors WHERE LOWER(name)=? AND (state=? OR state IS NULL OR state="") LIMIT 1', (name.lower(), st)).fetchone()
        if r: return
        email = it.get('email') or ''
        phone = it.get('formatted_phone_number') or it.get('phone') or ''
        web = it.get('website') or ''
        city = ''
        addr = it.get('formatted_address') or it.get('vicinity') or ''
        if addr and ',' in addr:
            city = addr.split(',')[0].strip()
        cur.execute('INSERT INTO vendors(name, naics, city, state, phone, email, website, notes) VALUES(?,?,?,?,?,?,?,?)',
                    (name, naics, city, st, phone, email, web, None))
        created += 1
    # seed from session
    for it in results[:limit]:
        upsert_from_item(it)
    conn.commit()
    return created

def _p8_vendor_email(cur, vid: int):
    r = cur.execute('SELECT email FROM vendors WHERE id=?', (int(vid),)).fetchone()
    if r and r[0]: return r[0]
    try:
        rr = cur.execute('SELECT email FROM vendor_contacts WHERE vendor_id=? ORDER BY id DESC LIMIT 1', (int(vid),)).fetchone()
        return rr[0] if rr and rr[0] else ''
    except Exception:
        return ''

def _p8_send_rfqs(rfq_id: int, vendor_ids: list, subject_t: str, body_t: str, attachments: list=None):
    conn = get_db(); cur = conn.cursor()
    # Read notice title and due date if available
    row = cur.execute('SELECT notice_id, due_at FROM rfq WHERE id=?', (int(rfq_id),)).fetchone()
    notice_id, due_at = (int(row[0]), row[1]) if row else (None, '')
    title = (cur.execute('SELECT title FROM notices WHERE id=?', (notice_id,)).fetchone() or ['Opportunity'])[0] if notice_id else 'Opportunity'
    count = 0
    for vid in vendor_ids:
        email = _p8_vendor_email(cur, int(vid)) or ''
        company = (cur.execute('SELECT name FROM vendors WHERE id=?', (int(vid),)).fetchone() or ['Vendor'])[0]
        subject = subject_t.format(title=title, due_date=due_at or '', company=company)
        body = body_t.format(title=title, due_date=due_at or '', company=company)
        try:
            cur.execute("INSERT INTO email_queue(to_addr, subject, body, created_at, status, attempts) VALUES(?,?,?,?, 'queued', 0)",
                        (email, subject, body, _dt_p8.datetime.utcnow().isoformat()))
            count += 1
        except Exception:
            pass
    if attachments is not None:
        try:
            cur.execute('UPDATE rfq SET attachments_json=? WHERE id=?', (_json_p8.dumps(attachments), int(rfq_id)))
        except Exception:
            pass
    cur.execute('UPDATE rfq SET sent_at=? WHERE id=?', (_dt_p8.datetime.utcnow().isoformat(), int(rfq_id)))
    conn.commit()
    return count

def _p8_compute_coverage(rfq_id: int, vendor_id: int):
    conn = get_db(); cur = conn.cursor()
    # total rfq lines
    total = (cur.execute('SELECT COUNT(*) FROM rfq_lines WHERE rfq_id=?', (int(rfq_id),)).fetchone() or [0])[0]
    if not total:
        return 0.0
    # count quote lines with price > 0 for this vendor
    row = cur.execute('SELECT id FROM vendor_quotes WHERE rfq_id=? AND vendor_id=? ORDER BY id DESC LIMIT 1', (int(rfq_id), int(vendor_id))).fetchone()
    if not row: return 0.0
    qid = int(row[0])
    priced = (cur.execute('SELECT COUNT(*) FROM vendor_quote_lines WHERE quote_id=? AND ext_price>0', (qid,)).fetchone() or [0])[0]
    cov = float(priced) / float(total) * 100.0
    try: cur.execute('UPDATE vendor_quotes SET coverage_score=? WHERE id=?', (cov, qid))
    except Exception: pass
    conn.commit()
    return cov

def _p8_save_contact_and_notes(vendor_id: int, name: str, email: str, phone: str, notes: str):
    conn = get_db(); cur = conn.cursor()
    if any([name, email, phone]):
        cur.execute('INSERT INTO vendor_contacts(vendor_id, name, email, phone, role) VALUES(?,?,?,?,?)', (int(vendor_id), name or None, email or None, phone or None, None))
    if notes:
        cur.execute('UPDATE vendors SET notes=? WHERE id=?', (notes, int(vendor_id)))
    conn.commit()

def _p8_add_chase(rfq_id: int, vendor_id: int, action: str, due_iso: str):
    conn = get_db(); cur = conn.cursor()
    cur.execute('INSERT INTO rfq_chase(rfq_id, vendor_id, next_action, due_at, status) VALUES(?,?,?,?,\'pending\')', (int(rfq_id), int(vendor_id), action, due_iso))
    conn.commit()

def _p8_mark_chase_done(chase_id: int):
    conn = get_db(); cur = conn.cursor()
    cur.execute('UPDATE rfq_chase SET status=\'done\' WHERE id=?', (int(chase_id),))
    conn.commit()

# UI integration under Vendors tab
try:
    _orig_render_vendors_phase8 = render_vendors
except Exception:
    _orig_render_vendors_phase8 = None

def render_vendors(opp_id: int):
    import streamlit as st
    if _orig_render_vendors_phase8:
        try: _orig_render_vendors_phase8(int(opp_id))
        except Exception: pass
    if not _p8_flag():
        return
    _p8_schema()
    st.session_state['vendor_tab_open'] = True
    owner = st.session_state.get('user_id') or 'user'
    rfq_id = _p8_get_or_create_rfq(int(opp_id), owner)
    st.session_state['current_rfq_id'] = rfq_id
    st.markdown('---')
    st.subheader('Vendors for this notice')
    c1,c2,c3 = st.columns(3)
    with c1:
        if st.button('Add vendors (seed by NAICS/State)'):
            cnt = _p8_seed_vendors_for_notice(int(opp_id))
            st.success(f'Added {cnt} vendors from Finder results')
    with c2:
        subj = st.text_input('Email subject', value='RFQ for {title} — due {due_date}', key='p8_subj')
    with c3:
        body = st.text_area('Email body', value='Hello {company},\nPlease quote the attached RFQ. Due {due_date}.', key='p8_body')
    # Pick vendors by state/naics filters
    conn = get_db(); cur = conn.cursor()
    stt = (cur.execute('SELECT place_state FROM notices WHERE id=?', (int(opp_id),)).fetchone() or [''])[0] or ''
    ncs = (cur.execute('SELECT naics FROM notices WHERE id=?', (int(opp_id),)).fetchone() or [''])[0] or ''
    vrows = cur.execute("SELECT id, name, state, naics, email FROM vendors WHERE (state=? OR ?='') AND (naics LIKE ? OR ?='') ORDER BY name", (stt, stt, f'%{ncs}%', ncs)).fetchall()
    choices = {f"{v} — {n or ''} [{s or ''}]": i for (i,n,s,a,e) in vrows for v in [n]}
    sel = st.multiselect('Target vendors', options=list(choices.keys()))
    target_ids = [choices[k] for k in sel]
    if st.button(f'Send RFQs to {len(target_ids)} vendor(s)', disabled=(len(target_ids)==0)):
        sent = _p8_send_rfqs(int(rfq_id), target_ids, subj, body, attachments=None)
        st.success(f'Sent {sent} emails')
    st.markdown('---')
    st.markdown('**Record quote**')
    # Select vendor and enter totals; per-line handled in RFQG intake but allow quick total+coverage recompute
    sel2 = st.selectbox('Vendor', options=[(i, (cur.execute('SELECT name FROM vendors WHERE id=?', (i,)).fetchone() or ['Vendor'])[0]) for i in [r[0] for r in vrows]], format_func=lambda x: x[1] if isinstance(x, tuple) else str(x))
    vid = sel2[0] if isinstance(sel2, tuple) else None
    if vid:
        # show coverage based on existing quote lines
        cov = _p8_compute_coverage(int(rfq_id), int(vid))
        st.caption(f'Coverage: {cov:.1f}% of RFQ lines priced')
        name = st.text_input('Contact name')
        email = st.text_input('Contact email')
        phone = st.text_input('Contact phone')
        notes = st.text_area('Capability notes')
        if st.button('Save contact + notes'):
            _p8_save_contact_and_notes(int(vid), name, email, phone, notes); st.success('Saved')
    st.markdown('---')
    st.markdown('**Chase list**')
    action = st.text_input('Next action', value='Follow up call')
    due = st.text_input('Due date ISO', value=_dt_p8.datetime.utcnow().date().isoformat())
    if vid and st.button('Add chase item'):
        _p8_add_chase(int(rfq_id), int(vid), action, due); st.success('Chase added')
    # List pending items
    try:
        rows = cur.execute('SELECT id, vendor_id, next_action, due_at FROM rfq_chase WHERE rfq_id=? AND status=\'pending\' ORDER BY due_at', (int(rfq_id),)).fetchall()
        for cid, v, act, d in rows:
            nm = (cur.execute('SELECT name FROM vendors WHERE id=?', (int(v),)).fetchone() or ['Vendor'])[0]
            cA, cB = st.columns([3,1])
            with cA: st.caption(f"{nm} • {act} • due {d}")
            with cB:
                if st.button('Done', key=f'c_done_{cid}'):
                    _p8_mark_chase_done(int(cid)); st.experimental_rerun() if hasattr(st,'experimental_rerun') else st.rerun()
    except Exception:
        pass
# === VENDOR RFQ HOOKS PHASE 8 END ===



# === DEALS PHASE 4: Forecast + Signals ===
def _ensure_deals_phase4_schema():
    conn = get_db()
    ensure_deals_table(conn)
    cur = conn.cursor()
    try:
        cols = {r[1] for r in cur.execute("PRAGMA table_info(deals)").fetchall()}
        if "win_prob" not in cols:
            cur.execute("ALTER TABLE deals ADD COLUMN win_prob REAL DEFAULT 0.3")
        if "next_action" not in cols:
            cur.execute("ALTER TABLE deals ADD COLUMN next_action TEXT")
        if "due_at" not in cols:
            cur.execute("ALTER TABLE deals ADD COLUMN due_at TEXT")
        if "compliance_state" not in cols:
            cur.execute("ALTER TABLE deals ADD COLUMN compliance_state TEXT")
        if "rfq_coverage" not in cols:
            cur.execute("ALTER TABLE deals ADD COLUMN rfq_coverage REAL")
        if "last_touch" not in cols:
            cur.execute("ALTER TABLE deals ADD COLUMN last_touch TEXT")
    except Exception:
        pass
    conn.commit()

def _list_deals_for_forecast(q=None):
    _ensure_deals_phase4_schema()
    conn = get_db()
    import pandas as pd
    sql = "select id, title, stage, owner, amount, agency, due_date, due_at, win_prob, compliance_state, rfq_coverage from deals"
    params = []
    if q:
        sql += " where title like ? or agency like ?"
        params = [f"%{q}%", f"%{q}%"]
    try:
        df = pd.read_sql_query(sql, conn, params=params)
    except Exception:
        df = pd.DataFrame(columns=["id","title","stage","owner","amount","agency","due_date","due_at","win_prob","compliance_state","rfq_coverage"])
    return df

def _coerce_date_series(s):
    import pandas as pd
    try:
        return pd.to_datetime(s, errors="coerce")
    except Exception:
        return pd.to_datetime([])

def deal_weighted_amount(row):
    try:
        amt = float(row.get("amount") or 0.0)
        p = float(row.get("win_prob") or 0.0)
        return max(0.0, amt) * min(max(p, 0.0), 1.0)
    except Exception:
        return 0.0

def forecast_weighted(df):
    import pandas as pd
    if df.empty: 
        return pd.DataFrame(columns=["period","weighted_total"]), pd.DataFrame(columns=["period","weighted_total"])
    dates = _coerce_date_series(df["due_at"].fillna(df["due_date"]))
    df2 = df.copy()
    df2["due_norm"] = dates
    df2["weighted"] = df2.apply(deal_weighted_amount, axis=1)
    df2 = df2.dropna(subset=["due_norm"])
    df2["month"] = df2["due_norm"].dt.to_period("M").astype(str)
    by_month = df2.groupby("month")["weighted"].sum().reset_index().rename(columns={"month":"period","weighted":"weighted_total"})
    df2["quarter"] = df2["due_norm"].dt.to_period("Q").astype(str)
    by_q = df2.groupby("quarter")["weighted"].sum().reset_index().rename(columns={"quarter":"period","weighted":"weighted_total"})
    return by_month, by_q

def update_win_prob_from_signals(deal_id):
    _ensure_deals_phase4_schema()
    conn = get_db()
    cur = conn.cursor()
    row = cur.execute("select win_prob, compliance_state, rfq_coverage from deals where id = ?", (int(deal_id),)).fetchone()
    if not row: 
        return False
    win_prob, comp, cov = row
    try:
        base = float(win_prob or 0.3)
        comp_adj = {"green":0.20, "yellow":0.05, "red":-0.10}.get((comp or "").lower(), 0.0)
        covf = float(cov) if cov is not None else 0.0
        cov_adj = max(-0.15, min(0.15, (covf - 0.5) * 0.30))
        new_p = min(0.95, max(0.05, base + comp_adj + cov_adj))
        cur.execute("update deals set win_prob = ?, updated_at = datetime('now') where id = ?", (new_p, int(deal_id)))
        conn.commit()
        return True
    except Exception:
        return False

def compute_sla_blockers():
    _ensure_deals_phase4_schema()
    conn = get_db()
    cur = conn.cursor()
    blockers = []
    try:
        rows = cur.execute("select id, title, owner, amount, due_at, due_date, updated_at from deals").fetchall()
        import datetime as _dt
        now = _dt.datetime.utcnow()
        for id_, title, owner, amount, due_at, due_date, updated_at in rows:
            due = None
            for d in [due_at, due_date]:
                if d:
                    try:
                        from datetime import datetime as _d2
                        due = _d2.fromisoformat(str(d).replace('Z',''))
                        break
                    except Exception:
                        pass
            try:
                upd = _d2.fromisoformat(str(updated_at).replace('Z','')) if updated_at else None
            except Exception:
                upd = None
            if due and due < now:
                blockers.append((id_, title, "Overdue"))
            if not owner:
                blockers.append((id_, title, "No owner"))
            try:
                if not amount or float(amount) <= 0:
                    blockers.append((id_, title, "No amount"))
            except Exception:
                blockers.append((id_, title, "No amount"))
            try:
                if upd and (now - upd).days > 7:
                    blockers.append((id_, title, "Stale"))
            except Exception:
                pass
        try:
            ensure_deal_activities_schema(conn)
            task_rows = cur.execute("select da.deal_id, d.title, da.id, da.due_at from deal_activities da join deals d on d.id = da.deal_id where da.type='task' and da.completed_at is null").fetchall()
            for did, dtitle, aid, due in task_rows:
                try:
                    from datetime import datetime as _d2
                    if due and _d2.fromisoformat(str(due).replace('Z','')) < now:
                        blockers.append((did, dtitle, "Task overdue"))
                except Exception:
                    pass
        except Exception:
            pass
    except Exception:
        pass
    return blockers
# === END DEALS PHASE 4 ===



# === PHASE 10: Observability ===
def ensure_observability_schema():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS metrics(
        id INTEGER PRIMARY KEY,
        ts TEXT NOT NULL,
        name TEXT NOT NULL,
        value REAL NOT NULL,
        labels_json TEXT
    );""" )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_metrics_name_ts ON metrics(name, ts);")
    cur.execute("""CREATE TABLE IF NOT EXISTS audit_log(
        id INTEGER PRIMARY KEY,
        ts TEXT NOT NULL,
        user_id TEXT,
        action TEXT NOT NULL,
        entity TEXT,
        entity_id TEXT,
        meta_json TEXT
    );""" )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts);")
    cur.execute("""CREATE TABLE IF NOT EXISTS error_events(
        id INTEGER PRIMARY KEY,
        ts TEXT NOT NULL,
        error_id TEXT NOT NULL,
        message TEXT NOT NULL,
        context_json TEXT
    );""" )
    conn.commit()

def metric_push(name: str, value: float, labels: dict|None=None):
    if not feature_flags().get('observability', False):
        return
    ensure_observability_schema()
    conn = get_db()
    conn.execute("INSERT INTO metrics(ts,name,value,labels_json) VALUES(?,?,?,?)",
                 (utc_now_iso() if 'utc_now_iso' in globals() else _dt.datetime.utcnow().isoformat()+'Z', str(name), float(value), json.dumps(labels or {})))
    conn.commit()

class metric_timer:
    def __init__(self, name:str, labels:dict|None=None):
        self.name = name; self.labels = labels or {}; self.start = None
    def __enter__(self):
        self.start = _time.perf_counter()
        return self
    def __exit__(self, exc_type, exc, tb):
        dur_ms = (_time.perf_counter() - self.start) * 1000.0 if self.start else 0.0
        metric_push(self.name, dur_ms, self.labels | {"unit":"ms"})
        if exc:
            error_event(self.name + ".error", str(exc), {"labels": self.labels})
        return False

def audit(action:str, user_id:str|None=None, entity:str|None=None, entity_id:str|None=None, meta:dict|None=None):
    if not feature_flags().get('observability', False):
        return
    ensure_observability_schema()
    conn = get_db()
    conn.execute("INSERT INTO audit_log(ts,user_id,action,entity,entity_id,meta_json) VALUES(?,?,?,?,?,?)",
                 (utc_now_iso() if 'utc_now_iso' in globals() else _dt.datetime.utcnow().isoformat()+'Z',
                  str(user_id or ''), str(action), str(entity or ''), str(entity_id or ''), json.dumps(meta or {})))
    conn.commit()

def error_event(error_id:str, message:str, ctx:dict|None=None):
    if not feature_flags().get('observability', False):
        return
    ensure_observability_schema()
    conn = get_db()
    conn.execute("INSERT INTO error_events(ts,error_id,message,context_json) VALUES(?,?,?,?)",
                 (utc_now_iso() if 'utc_now_iso' in globals() else _dt.datetime.utcnow().isoformat()+'Z', str(error_id), str(message), json.dumps(ctx or {})))
    conn.commit()

def render_health_card():
    import streamlit as st, pandas as pd
    if not feature_flags().get('observability', False):
        return
    ensure_observability_schema()
    conn = get_db()
    since = (_dt.datetime.utcnow() - _dt.timedelta(days=1)).isoformat()+'Z'
    def _avg(name):
        try:
            return conn.execute("select avg(value) from metrics where name=? and ts>=?", (name, since)).fetchone()[0]
        except Exception:
            return None
    def _last_time(name):
        try:
            return conn.execute("select max(ts) from metrics where name=?", (name,)).fetchone()[0]
        except Exception:
            return None
    api_ms = _avg("api_latency_ms") or 0
    cache_hits = conn.execute("select count(*) from metrics where name='cache_hit' and value=1 and ts>=?", (since,)).fetchone()[0]
    cache_total = conn.execute("select count(*) from metrics where name='cache_hit' and ts>=?", (since,)).fetchone()[0]
    cache_pct = (100.0*cache_hits/cache_total) if cache_total else 0.0
    email_ok = conn.execute("select count(*) from metrics where name='email_success' and value=1 and ts>=?", (since,)).fetchone()[0]
    email_total = conn.execute("select count(*) from metrics where name='email_success' and ts>=?", (since,)).fetchone()[0]
    email_rate = (100.0*email_ok/email_total) if email_total else 0.0
    last_export = _last_time("export_duration_ms") or "n/a"
    last_parser_err = conn.execute("select max(ts) from error_events where error_id like 'parser%'" ).fetchone()[0] if True else None
    st.markdown("### Health")
    c1,c2,c3 = st.columns(3)
    c1.metric("API avg latency", f"{api_ms:.0f} ms")
    c2.metric("Cache hit", f"{cache_pct:.0f}%")
    c3.metric("Email delivery", f"{email_rate:.0f}%")
    c4,c5 = st.columns(2)
    c4.metric("Last export", last_export)
    c5.metric("Last parser error", last_parser_err or "none")

def render_admin_observability():
    import streamlit as st, pandas as pd
    if not feature_flags().get('observability', False):
        return
    ensure_observability_schema()
    st.subheader("Admin • Logs and Metrics")
    d1, d2 = st.columns(2)
    with d1:
        start = st.text_input("Start ISO", value=(_dt.datetime.utcnow() - _dt.timedelta(days=7)).isoformat()+'Z')
    with d2:
        end = st.text_input("End ISO", value=_dt.datetime.utcnow().isoformat()+'Z')
    u = st.text_input("User filter")
    conn = get_db()
    st.markdown("#### Audit log")
    q = "select ts,user_id,action,entity,entity_id,meta_json from audit_log where ts between ? and ?"
    params = [start, end]
    if u:
        q += " and user_id like ?"; params.append(f"%{u}%")
    try:
        import pandas as pd
        df = pd.read_sql_query(q + " order by id desc limit 1000", conn, params=params)
    except Exception:
        df = None
    if df is not None:
        st.dataframe(df, use_container_width=True, hide_index=True)
    st.markdown("#### Metrics")
    mname = st.text_input("Metric name")
    mq = "select ts,name,value,labels_json from metrics where ts between ? and ?"
    mparams = [start, end]
    if mname:
        mq += " and name=?"; mparams.append(mname)
    try:
        dm = pd.read_sql_query(mq + " order by id desc limit 1000", conn, params=mparams)
    except Exception:
        dm = None
    if dm is not None:
        st.dataframe(dm, use_container_width=True, hide_index=True)
# === END PHASE 10 ===


# Admin view injection
try:
    import streamlit as st
    if feature_flags().get('observability', False) and st.sidebar.checkbox("Open Admin Observability"):
        render_admin_observability()
except Exception:
    pass



# === PERSIST PHASE 6: Config + Backups ===
def ensure_config_table():
    conn = get_db()
    conn.execute("CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY, value TEXT NOT NULL);")
    conn.commit()

def get_config(key:str, default:str=None):
    ensure_config_table()
    conn = get_db()
    row = conn.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
    return row[0] if row else default

def set_config(key:str, value:str):
    ensure_config_table()
    conn = get_db()
    conn.execute("INSERT INTO config(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;", (key, value))
    conn.commit()

def _current_sqlite_path(conn=None):
    import sqlite3
    conn = conn or get_db()
    try:
        row = conn.execute("PRAGMA database_list").fetchone()
        return row[2] if row else None
    except Exception:
        return None


def ensure_backups_dir():
    import os
    bdir = os.path.join('.', 'backups')
    os.makedirs(bdir, exist_ok=True)
    return bdir

def sqlite_backup_now():
    import os, shutil, datetime as _dt
    conn = get_db()
    src = _current_sqlite_path(conn)
    if not src or not os.path.exists(src):
        return {'ok': False, 'reason': 'not_sqlite'}
    bdir = ensure_backups_dir()
    stamp = _dt.datetime.utcnow().strftime('%Y%m%d')
    dst = os.path.join(bdir, f"app-{stamp}.db")
    try:
        # Use SQLite backup API for consistency
        import sqlite3
        with sqlite3.connect(dst) as bconn:
            conn.backup(bconn)
        return {'ok': True, 'path': dst}
    except Exception as ex:
        # Fallback to file copy if backup API fails
        try:
            shutil.copy2(src, dst)
            return {'ok': True, 'path': dst, 'mode': 'copy2'}
        except Exception as ex2:
            return {'ok': False, 'reason': str(ex2)}

def last_backup_info():
    import os, glob, datetime as _dt
    bdir = ensure_backups_dir()
    files = sorted(glob.glob(os.path.join(bdir, 'app-*.db')), reverse=True)
    if not files:
        return None
    latest = files[0]
    ts = _dt.datetime.utcfromtimestamp(os.path.getmtime(latest)).isoformat()+'Z'
    return {'path': latest, 'ts': ts}

def pg_dump_now(db_url: str):
    # Best-effort placeholder: requires pg_dump installed in runtime
    import subprocess, shlex, os, datetime as _dt
    bdir = ensure_backups_dir()
    stamp = _dt.datetime.utcnow().strftime('%Y%m%d')
    outfile = os.path.join(bdir, f"pg-{stamp}.sql")
    cmd = f"pg_dump {shlex.quote(db_url)} -f {shlex.quote(outfile)}"
    try:
        subprocess.run(cmd, shell=True, check=True, capture_output=True)
        return {'ok': True, 'path': outfile}
    except Exception as ex:
        return {'ok': False, 'reason': str(ex), 'cmd': cmd}


def render_data_health_card():
    import streamlit as st, shutil, os, datetime as _dt
    ensure_config_table()
    st.markdown("### Data health")
    # Last backup
    info = last_backup_info()
    last_ts = info['ts'] if info else 'none'
    # WAL mode
    conn = get_db()
    try:
        jmode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    except Exception:
        jmode = 'unknown'
    # Disk free
    total, used, free = shutil.disk_usage('.')
    pct_free = (free/total*100.0) if total else 0.0
    # Job backlog: unsent emails as proxy
    try:
        backlog = conn.execute("SELECT COUNT(*) FROM email_queue").fetchone()[0]
    except Exception:
        backlog = 0
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Last backup", last_ts)
    c2.metric("Journal mode", jmode)
    c3.metric("Free disk", f"{pct_free:.0f}%")
    c4.metric("Job backlog", int(backlog))
    # Alerts
    alerts = []
    try:
        if info:
            dt_last = _dt.datetime.fromisoformat(info['ts'].replace('Z',''))
            if (_dt.datetime.utcnow()-dt_last).total_seconds() > 24*3600:
                alerts.append("Backup older than 24h")
        else:
            alerts.append("No backups found")
    except Exception:
        pass
    if pct_free < 10.0:
        alerts.append("Disk < 10% free")
    if alerts:
        st.warning("; ".join(alerts))
    # Actions
    st.caption("Backup ops")
    colA, colB = st.columns(2)
    with colA:
        if st.button("Run SQLite backup now"):
            res = sqlite_backup_now()
            if res.get('ok'):
                st.success(f"Backup created: {res.get('path')}")
            else:
                st.error(f"Backup failed: {res.get('reason')}")
    with colB:
        pg_url = get_config('db_url_postgres', None)
        if pg_url and st.button("Run pg_dump now"):
            res = pg_dump_now(pg_url)
            if res.get('ok'):
                st.success(f"pg_dump created: {res.get('path')}")
            else:
                st.error(f"pg_dump failed: {res.get('reason')}")


def apply_env_db_settings():
    """Read config/env and secrets to decide DB target. Writes advisory note; actual reconnect requires app restart."""
    ensure_config_table()
    env = get_config('env', 'dev')
    # These keys are advisory; app retains current connection until restart
    # Expected secrets: secrets['db_path_dev'], ['db_path_prod'] or ['db_url_dev'], ['db_url_prod']
    try:
        import streamlit as st
        st.session_state['active_env'] = env
        # Store advisory targets in session for visibility
        st.session_state['db_target_path'] = None
        st.session_state['db_target_url'] = None
        sec = st.secrets if 'st' in globals() else {}
        if 'db_url_prod' in sec or 'db_url_dev' in sec:
            st.session_state['db_target_url'] = sec['db_url_prod'] if env=='prod' else sec.get('db_url_dev')
        elif 'db_path_prod' in sec or 'db_path_dev' in sec:
            st.session_state['db_target_path'] = sec['db_path_prod'] if env=='prod' else sec.get('db_path_dev')
    except Exception:
        pass

def render_env_switcher():
    import streamlit as st
    ensure_config_table()
    st.markdown("### Environment")
    env = get_config('env', 'dev')
    new_env = st.selectbox("Active env", options=['dev','prod'], index=0 if env!='prod' else 1)
    if st.button("Set env"):
        set_config('env', new_env)
        apply_env_db_settings()
        st.success(f"Env set to {new_env}. Restart app to take effect for DB connection.")

# === END PERSIST PHASE 6 ===



# === RFP PHASE 3: Requirements Traceability Matrix (RTM) ===
def ensure_rtm_schema():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS rtm(
        id INTEGER PRIMARY KEY,
        notice_id INTEGER NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
        req_id TEXT NOT NULL,
        factor TEXT,
        subfactor TEXT,
        requirement TEXT NOT NULL,
        target_section_key TEXT,
        evidence_note TEXT,
        status TEXT NOT NULL CHECK(status IN('Unmapped','Planned','Written','Reviewed')),
        updated_at TEXT NOT NULL
    );""" )
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_rtm_req ON rtm(notice_id, req_id);")
    conn.commit()

def build_rtm(notice_id: int, rfp_json: dict|None=None):
    if not feature_flags().get('rtm', False):
        return {'ok': False, 'disabled': True}
    ensure_rtm_schema()
    conn = get_db(); cur = conn.cursor()
    nid = int(notice_id)
    inserted = 0
    # Primary source: lm_checklist rows as requirements with stable req_id
    try:
        rows = cur.execute("select req_id, factor, subfactor, requirement from lm_checklist where notice_id=?", (nid,)).fetchall()
    except Exception:
        rows = []
    for req_id, factor, subfactor, requirement in rows:
        if not req_id:
            req_id = (str(requirement)[:64] or '').lower()
        try:
            cur.execute("""INSERT OR IGNORE INTO rtm(notice_id, req_id, factor, subfactor, requirement, status, updated_at)
                        VALUES(?,?,?,?,?, 'Unmapped', datetime('now'))""", (nid, req_id, factor, subfactor, requirement))
            if cur.rowcount:
                inserted += 1
        except Exception:
            pass
    conn.commit()
    return {'ok': True, 'inserted': inserted}

def rtm_update(nid:int, row_id:int, **fields):
    ensure_rtm_schema()
    conn = get_db(); cur = conn.cursor()
    allowed = {"target_section_key","evidence_note","status"}
    sets = []; vals = []
    for k,v in fields.items():
        if k in allowed:
            sets.append(f"{k}=?"); vals.append(v)
    if not sets:
        return False
    sets.append("updated_at=datetime('now')")
    sql = "UPDATE rtm SET " + ", ".join(sets) + " WHERE id=? AND notice_id=?"
    vals.extend([int(row_id), int(nid)])
    cur.execute(sql, vals)
    conn.commit()
    return True

def rtm_coverage(nid:int):
    import pandas as pd
    ensure_rtm_schema()
    conn = get_db()
    try:
        df = pd.read_sql_query("select factor, status from rtm where notice_id=?", conn, params=(int(nid),))
    except Exception:
        return {}, 0.0
    if df.empty:
        return {}, 0.0
    score_map = {"Unmapped":0.0, "Planned":0.25, "Written":0.75, "Reviewed":1.0}
    df['score'] = df['status'].map(score_map).fillna(0.0)
    by_factor = df.groupby('factor')['score'].mean().to_dict()
    overall = float(df['score'].mean())
    return by_factor, overall

def render_rtm_tab(nid:int):
    import streamlit as st, pandas as pd
    if not feature_flags().get('rtm', False):
        return
    ensure_rtm_schema()
    conn = get_db()
    st.markdown("#### Requirements Traceability Matrix")
    # Seed if empty
    cnt = conn.execute("select count(*) from rtm where notice_id=?", (int(nid),)).fetchone()[0]
    if cnt == 0:
        try:
            doc = build_rfpv1_from_notice(int(nid))
            build_rtm(int(nid), doc)
        except Exception:
            pass
    # Filters
    factors = pd.read_sql_query("select distinct factor from rtm where notice_id=? order by factor", conn, params=(int(nid),))
    f1,f2 = st.columns([2,2])
    with f1:
        fac = st.selectbox("Filter: factor", options=["All"] + factors['factor'].dropna().tolist())
    with f2:
        st_opts = ["All","Unmapped","Planned","Written","Reviewed"]
        stf = st.selectbox("Filter: status", options=st_opts, index=0)
    q = "select id, req_id, factor, subfactor, requirement, target_section_key, evidence_note, status from rtm where notice_id=?"
    params = [int(nid)]
    if fac != "All":
        q += " and factor=?"; params.append(fac)
    if stf != "All":
        q += " and status=?"; params.append(stf)
    df = pd.read_sql_query(q + " order by factor, subfactor, id", conn, params=params)
    edited = st.data_editor(df, use_container_width=True, num_rows=0,
                            column_config={
                                "status": st.column_config.SelectboxColumn(options=["Unmapped","Planned","Written","Reviewed"])
                            },
                            key=f"rtm_edit_{nid}")
    if st.button("Save RTM"):
        for _, row in edited.iterrows():
            rid = int(row['id'])
            rtm_update(nid, rid,
                       target_section_key=row.get('target_section_key'),
                       evidence_note=row.get('evidence_note'),
                       status=row.get('status'))
        st.success("RTM saved.")
        st.experimental_rerun()
    # Coverage
    by_factor, overall = rtm_coverage(nid)
    st.markdown("#### Coverage")
    if by_factor:
        c1,c2 = st.columns([2,1])
        with c1:
            pdf = (pd.DataFrame({'factor': list(by_factor.keys()), 'coverage': [round(v*100,1) for v in by_factor.values()] })
                    .sort_values('coverage', ascending=False))
            st.dataframe(pdf, use_container_width=True, hide_index=True)
        with c2:
            st.metric("Overall", f"{overall*100:.1f}%")
    else:
        st.info("No RTM rows.")
# === END RFP PHASE 3 ===
