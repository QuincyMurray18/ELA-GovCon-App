from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Any, Dict, List, Tuple
import io
import json
import os
import re
import math
import sqlite3

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import docx  # for DOCX reading in _read_file
import re
try:
    import mathquests  # optional plugin
except Exception:
    mathquests = None
import smtplib
import streamlit as st

# --- Optional PDF backends for Phase X1 ---
try:
    import pdfplumber as _pdfplumber  # type: ignore
except Exception:
    _pdfplumber = None
try:
    import PyPDF2 as _pypdf  # type: ignore
except Exception:
    try:
        import pypdf as _pypdf  # type: ignore
    except Exception:
        _pypdf = None




# --- hashing helper ---
def compute_sha256(b: bytes) -> str:
    try:
        return hashlib.sha256(b or b"").hexdigest()
    except Exception:
        import hashlib as _h
        return _h.sha256(b or b"").hexdigest()

# Phase X unified settings
from types import SimpleNamespace as _NS
from openai import OpenAI

def getenv_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except Exception:
        return default

SETTINGS = _NS(
    APP_NAME=os.environ.get("ELA_APP_NAME", "ELA GovCon Suite"),
    APP_VERSION=os.environ.get("ELA_APP_VERSION", "X-Base"),
    DATA_DIR=os.environ.get("ELA_DATA_DIR", "data"),
    UPLOADS_SUBDIR=os.environ.get("ELA_UPLOADS_SUBDIR", "uploads"),
    DEFAULT_PAGE_SIZE=getenv_int("ELA_PAGE_SIZE", 50),
)

SETTINGS.UPLOADS_DIR = os.path.join(SETTINGS.DATA_DIR, SETTINGS.UPLOADS_SUBDIR)

# Ensure directories exist
try:
    os.makedirs(SETTINGS.DATA_DIR, exist_ok=True)
    os.makedirs(SETTINGS.UPLOADS_DIR, exist_ok=True)
except Exception:
    pass

# Back-compat constants
DATA_DIR = SETTINGS.DATA_DIR
UPLOADS_DIR = SETTINGS.UPLOADS_DIR

# Feature flag alias
def flag(name: str, default: bool=False) -> bool:
    return feature_flag(name, default)

def feature_flag(name: str, default: bool=False) -> bool:
    """
    Read a feature flag from environment or Streamlit secrets.
    Precedence: os.environ["FEATURE_<NAME>"] then st.secrets["features"][name] then default.
    Does not raise if Streamlit is absent.
    """
    val = None
    try:
        import os as _os
        env_key = f"FEATURE_{name.upper()}"
        if env_key in _os.environ:
            val = _os.environ[env_key]
    except Exception:
        pass


def _guess_solnum(text: str) -> str:
    if not text:
        return ""
    t = text
    m = re.search(r'(?i)Solicitation\s*(?:Number|No\.?|#)\s*[:#]?\s*([A-Z0-9][A-Z0-9\-\._/]{4,})', t)
    if m:
        return m.group(1)[:60]
    m = re.search(r'\b([A-Z0-9]{2,6}[A-Z0-9\-]{0,4}\d{2}[A-Z]?-?[A-Z]?-?\d{3,6})\b', t)
    if m:
        return m.group(1)[:60]
    m = re.search(r'\b(RFQ|RFP|IFB|RFI)[\s#:]*([A-Z0-9][A-Z0-9\-\._/]{3,})\b', t, re.I)
    if m:
        return (m.group(1).upper() + "-" + m.group(2))[:60]
    return ""
    if val is None:
        try:
            import streamlit as _st  # type: ignore
            sec = _st.secrets.get("features", {})
            if isinstance(sec, dict) and name in sec:
                val = sec.get(name)
        except Exception:
            pass
    if isinstance(val, str):
        return val.lower() in {"1","true","yes","on"}
    if isinstance(val, bool):
        return val
    return bool(val) if val is not None else bool(default)


# External


APP_TITLE = "ELA GovCon Suite"
BUILD_LABEL = "Master A–F — SAM • RFP Analyzer • L&M • Proposal • Subs+Outreach • Quotes • Pricing • Win Prob • Chat • Capability"

st.set_page_config(page_title=APP_TITLE, layout="wide")


# === Y0: GPT-5 Thinking CO assistant (streaming) ===
try:
    from openai import OpenAI as _Y0OpenAI
except Exception:
    _Y0OpenAI = None

SYSTEM_CO = ("Act as a GS-1102 Contracting Officer. Cite exact pages. "
             "Flag non-compliance. Be concise. If evidence is missing, say so.")

# === helper: auto-select number of sources to cite (Y1–Y3) ===
def y_auto_k(text: str) -> int:
    t = (text or '').lower()
    n = len(t)
    broad = any(k in t for k in [
        'overview','summary','summarize','list all','requirements','compliance',
        'section l','section m','evaluation factors','factors','checklist',
        'compare','differences','conflict','conflicts','crosswalk','matrix'
    ])
    with Research:
        run_research_tab(conn)
    if not t.strip():
        return 4
    base = 7 if broad else 4
    if n > 500:
        base += 1
    if n > 1200:
        base += 1
    return max(3, min(8, base))

import os

def _resolve_model():
    # Priority: Streamlit secrets -> env var -> safe default
    try:
        import streamlit as st  # noqa: F401
        for key in ("OPENAI_MODEL", "openai_model", "model"):
            try:
                val = st.secrets.get(key)  # type: ignore[attr-defined]
                if isinstance(val, str) and val.strip():
                    return val.strip()
            except Exception:
                pass
    except Exception:
        pass
    return os.getenv("OPENAI_MODEL", "gpt-4o-mini")

_ai_client = None
def get_ai():
    import streamlit as st  # ensure st exists
    global _ai_client
    if _ai_client is None:
        if _Y0OpenAI is None:
            raise RuntimeError("openai library missing")
        _ai_client = _Y0OpenAI()  # uses OPENAI_API_KEY from Streamlit secrets
    return _ai_client

def ask_ai(messages, tools=None, temperature=0.2):
    client = get_ai()
    model_name = _resolve_model()
    try:
        resp = client.chat.completions.create(
            model=model_name,
            messages=[{"role":"system","content": SYSTEM_CO}, *messages],
            tools=tools or [],
            temperature=float(temperature),
            stream=True
        )
    except Exception as _e:
        if "model_not_found" in str(_e) or "does not exist" in str(_e):
            model_name = "gpt-4o-mini"
            resp = client.chat.completions.create(
                model=model_name,
                messages=[{"role":"system","content": SYSTEM_CO}, *messages],
                tools=tools or [],
                temperature=float(temperature),
                stream=True
            )
        else:
            yield f"AI unavailable: {type(_e).__name__}: {_e}"
            return
    for ch in resp:
        try:
            delta = ch.choices[0].delta
            if hasattr(delta, "content") and delta.content:
                yield delta.content
        except Exception:
            pass

def y0_ai_panel():
    import streamlit as st
    st.header(f"Ask the CO (AI) · {_resolve_model()}")
    q = st.text_area("Your question", key="y0_q", height=120)
    if st.button("Ask", key="y0_go"):
        if not (q or "").strip():
            st.warning("Enter a question")
        else:
            ph = st.empty()
            acc = []
            for tok in ask_ai([{"role":"user","content": q.strip()}]):
                acc.append(tok)
                ph.markdown("".join(acc))
# === end Y0 ===



# --- key namespacing helper (Phase U) ---
def ns(*parts) -> str:
    """Generate stable, unique Streamlit widget keys."""
    return "k_" + "_".join(str(p) for p in parts if p is not None)
DATA_DIR = "data"
DB_PATH = os.path.join(DATA_DIR, "govcon.db")
UPLOADS_DIR = os.path.join(DATA_DIR, "uploads")
SAM_ENDPOINT = "https://api.sam.gov/opportunities/v2/search"


# -------------------- setup --------------------
def ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(UPLOADS_DIR, exist_ok=True)
# === Y1: Retrieval (chunks • embeddings • citations) ===
def _ensure_y1_schema(conn: sqlite3.Connection) -> None:
    try:
        with closing(conn.cursor()) as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS rfp_chunks(
                    id INTEGER PRIMARY KEY,
                    rfp_id INTEGER,
                    rfp_file_id INTEGER,
                    file_name TEXT,
                    page INTEGER,
                    chunk_idx INTEGER,
                    text TEXT,
                    emb TEXT
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_rfp ON rfp_chunks(rfp_id);")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_chunk_key ON rfp_chunks(rfp_file_id, page, chunk_idx);")
            conn.commit()
    except Exception:
        pass

def _resolve_embed_model() -> str:
    # Streamlit secrets or env, else default
    try:
        import streamlit as _st
        for k in ("OPENAI_EMBED_MODEL","openai_embed_model","EMBED_MODEL"):
            v = _st.secrets.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    except Exception:
        pass
    import os as _os
    return _os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small")

def _embed_texts(texts: list[str]) -> list[list[float]]:
    client = get_ai()
    model = _resolve_embed_model()
    clean = [t if (t or "").strip() else " " for t in texts]
    resp = client.embeddings.create(model=model, input=clean)
    out = []
    for d in resp.data:
        try:
            out.append(list(d.embedding))
        except Exception:
            out.append([])
    return out

def _cos_sim(u: list[float], v: list[float]) -> float:
    if not u or not v:
        return 0.0
    try:
        import numpy as _np
        a = _np.array(u, dtype=float); b = _np.array(v, dtype=float)
        num = float((_np.dot(a, b)))
        den = float(_np.linalg.norm(a) * _np.linalg.norm(b))
        return (num / den) if den else 0.0
    except Exception:
        num = 0.0; su = 0.0; sv = 0.0
        for i in range(min(len(u), len(v))):
            x = float(u[i]); y = float(v[i])
            num += x*y; su += x*x; sv += y*y
        den = (su**0.5) * (sv**0.5)
        return (num / den) if den else 0.0

def _split_chunks(text: str, max_chars: int = 1600, overlap: int = 200) -> list[str]:
    t = (text or "").strip()
    if not t:
        return []
    chunks = []
    i = 0
    n = len(t)
    while i < n:
        j = min(n, i + max(200, max_chars))
        chunk = t[i:j]
        k = chunk.rfind(". ")
        if k > 900:
            chunk = chunk[:k+1]
            j = i + k + 1
        chunks.append(chunk)
        i = max(j - overlap, j)
    return chunks

def y1_index_rfp(conn: sqlite3.Connection, rfp_id: int, max_pages: int = 100, rebuild: bool = False) -> dict:
    _ensure_y1_schema(conn)
    try:
        df = pd.read_sql_query("SELECT id, filename, mime, pages FROM rfp_files WHERE rfp_id=? ORDER BY id;", conn, params=(int(rfp_id),))
    except Exception as e:
        return {"ok": False, "error": str(e)}
    if df is None or df.empty:
        return {"ok": False, "error": "No linked files"}
    added = 0
    skipped = 0
    for _, row in df.iterrows():
        fid = int(row["id"]); name = row.get("filename") or f"file_{fid}"
        try:
            blob = pd.read_sql_query("SELECT bytes, mime FROM rfp_files WHERE id=?;", conn, params=(fid,)).iloc[0]
            b = blob["bytes"]; mime = blob.get("mime") or (row.get("mime") or "application/octet-stream")
        except Exception:
            continue
        pages = extract_text_pages(b, mime) or []
        if not pages:
            continue
        pages = pages[:max_pages]
        for pi, txt in enumerate(pages, start=1):
            parts = _split_chunks(txt or "", 1600, 200)
            for ci, ch in enumerate(parts):
                try:
                    if not rebuild:
                        q = pd.read_sql_query("SELECT id FROM rfp_chunks WHERE rfp_file_id=? AND page=? AND chunk_idx=?;", conn, params=(fid, pi, ci))
                        if q is not None and not q.empty:
                            skipped += 1
                            continue
                except Exception:
                    pass
                emb = _embed_texts([ch])[0]
                with closing(conn.cursor()) as cur:
                    cur.execute("""
                        INSERT OR REPLACE INTO rfp_chunks(rfp_id, rfp_file_id, file_name, page, chunk_idx, text, emb)
                        VALUES(?,?,?,?,?,?,?);
                    """, (int(rfp_id), fid, name, int(pi), int(ci), ch, json.dumps(emb)))
                    conn.commit()
                added += 1
    return {"ok": True, "added": added, "skipped": skipped}

def y1_search(conn: sqlite3.Connection, rfp_id: int, query: str, k: int = 6) -> list[dict]:
    _ensure_y1_schema(conn)
    if not (query or "").strip():
        return []
    try:
        df = pd.read_sql_query("SELECT id, rfp_file_id, file_name, page, chunk_idx, text, emb FROM rfp_chunks WHERE rfp_id=?;", conn, params=(int(rfp_id),))
    except Exception:
        return []
    if df is None or df.empty:
        return []
    q_emb = _embed_texts([query])[0]
    rows = []
    for _, r in df.iterrows():
        try:
            emb = json.loads(r.get("emb") or "[]")
        except Exception:
            emb = []
        sim = _cos_sim(q_emb, emb)
        rows.append({"id": int(r["id"]), "fid": int(r["rfp_file_id"]), "file": r.get("file_name"), "page": int(r.get("page") or 0), "chunk": int(r.get("chunk_idx") or 0), "text": r.get("text") or "", "score": round(float(sim), 6)})
    rows.sort(key=lambda x: x["score"], reverse=True)
    return rows[:max(1, int(k))]

def ask_ai_with_citations(conn: sqlite3.Connection, rfp_id: int, question: str, k: int = 6, temperature: float = 0.2):
    hits = y1_search(conn, int(rfp_id), question or "", k=k)
    if not hits:
        for t in ask_ai([{"role":"user", "content": question or ""}], temperature=temperature):
            yield t
        return
    ev_lines = []
    for i, h in enumerate(hits, start=1):
        tag = f"[C{i}]"
        src = f"{h['file']} p.{h['page']}"
        snip = h["text"].strip().replace("\n", " ")
        ev_lines.append(f"{tag} {src} — {snip}")
    evidence = "\n".join(ev_lines[:k])
    system = SYSTEM_CO + " Use only the EVIDENCE provided. Add bracketed citations like [C1] next to claims tied to sources."
    user = f"QUESTION: {question}\n\nEVIDENCE:\n{evidence}\n\nWrite a concise answer. If evidence is insufficient, say what is missing."
    client = get_ai()
    model_name = _resolve_model()
    try:
        resp = client.chat.completions.create(
            model=model_name,
            messages=[{"role":"system","content": system}, {"role":"user","content": user}],
            temperature=float(temperature),
            stream=True
        )
    except Exception as _e:
        if "model_not_found" in str(_e) or "does not exist" in str(_e):
            try:
                resp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role":"system","content": system}, {"role":"user","content": user}],
                    temperature=float(temperature),
                    stream=True
                )
            except Exception as _e2:
                yield f"AI unavailable: {type(_e2).__name__}: {_e2}"
                return
        else:
            yield f"AI unavailable: {type(_e).__name__}: {_e}"
            return
    for ch in resp:
        try:
            delta = ch.choices[0].delta
            if hasattr(delta, "content") and delta.content:
                yield delta.content
        except Exception:
            pass
def y1_ensure_schema_no_cache(conn: sqlite3.Connection) -> None:
    try:
        with closing(conn.cursor()) as cur:
            cur.execute('CREATE TABLE IF NOT EXISTS rfp_chunks(id INTEGER PRIMARY KEY, rfp_id INTEGER, rfp_file_id INTEGER, file_name TEXT, page INTEGER, chunk_idx INTEGER, text TEXT, emb TEXT);')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_chunks_rfp ON rfp_chunks(rfp_id);')
            cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS uq_chunk_key ON rfp_chunks(rfp_file_id, page, chunk_idx);')
            conn.commit()
    except Exception:
        pass

_ensure_y1_schema = y1_ensure_schema_no_cache
# === end Y1 ===

# === Y2: CO Chat with per-RFP memory (threads) ===

def co_mode_for_thread(rfp_id: int | None, user_flag: str | None) -> str:
    """
    Decide mode for CO Chat.
    - "CO_strict": default when an RFP id is present.
    - "Research_flex": allows broader context and web research excerpts if enabled.
    """
    if isinstance(user_flag, str) and user_flag.strip().lower() in {"flex","research","on"}:
        return "Research_flex"
    return "CO_strict" if (rfp_id is not None and int(rfp_id) > 0) else "Research_flex"

def _ensure_y2_schema(conn: sqlite3.Connection) -> None:
    try:
        with closing(conn.cursor()) as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS rfp_threads(
                    id INTEGER PRIMARY KEY,
                    rfp_id INTEGER REFERENCES rfps(id) ON DELETE CASCADE,
                    title TEXT,
                    created_at TEXT,
                    last_activity TEXT
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS rfp_msgs(
                    id INTEGER PRIMARY KEY,
                    thread_id INTEGER NOT NULL REFERENCES rfp_threads(id) ON DELETE CASCADE,
                    role TEXT,
                    content TEXT,
                    ts TEXT
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_msgs_thread ON rfp_msgs(thread_id);")
            conn.commit()
    except Exception:
        pass

def y2_list_threads(conn: sqlite3.Connection, rfp_id: int) -> list[dict]:
    _ensure_y2_schema(conn)
    try:
        df = pd.read_sql_query("SELECT id, title, created_at, last_activity FROM rfp_threads WHERE rfp_id=? ORDER BY COALESCE(last_activity, created_at) DESC, id DESC;", conn, params=(int(rfp_id),))
        return ([] if df is None or df.empty else [{k:(row[k] if k in row else None) for k in df.columns} for _,row in df.iterrows()])
    except Exception:
        return []

def y2_create_thread(conn: sqlite3.Connection, rfp_id: int, title: str = "New CO Chat") -> int:
    _ensure_y2_schema(conn)
    with closing(conn.cursor()) as cur:
        cur.execute("INSERT INTO rfp_threads(rfp_id, title, created_at, last_activity) VALUES(?,?, datetime('now'), datetime('now'));", (int(rfp_id), title.strip() or "New CO Chat"))
        conn.commit()
        return int(cur.lastrowid)

def y2_rename_thread(conn: sqlite3.Connection, thread_id: int, title: str) -> None:
    _ensure_y2_schema(conn)
    try:
        with closing(conn.cursor()) as cur:
            cur.execute("UPDATE rfp_threads SET title=?, last_activity=datetime('now') WHERE id=?;", (title.strip(), int(thread_id)))
            conn.commit()
    except Exception:
        pass

def y2_delete_thread(conn: sqlite3.Connection, thread_id: int) -> None:
    _ensure_y2_schema(conn)
    try:
        with closing(conn.cursor()) as cur:
            cur.execute("DELETE FROM rfp_threads WHERE id=?;", (int(thread_id),))
            conn.commit()
    except Exception:
        pass

def y2_get_messages(conn: sqlite3.Connection, thread_id: int) -> list[dict]:
    _ensure_y2_schema(conn)
    try:
        df = pd.read_sql_query("SELECT role, content, ts FROM rfp_msgs WHERE thread_id=? ORDER BY id ASC;", conn, params=(int(thread_id),))
        if df is None or df.empty:
            return []
        return [{"role": row["role"], "content": row["content"]} for _, row in df.iterrows()]
    except Exception:
        return []

def y2_append_message(conn: sqlite3.Connection, thread_id: int, role: str, content: str) -> None:
    try:
        with closing(conn.cursor()) as cur:
            cur.execute("INSERT INTO rfp_msgs(thread_id, role, content, ts) VALUES(?,?,?, datetime('now'));", (int(thread_id), str(role), str(content)))
            cur.execute("UPDATE rfp_threads SET last_activity=datetime('now') WHERE id=?;", (int(thread_id),))
            conn.commit()
    except Exception:
        pass

def _y2_build_messages(conn: sqlite3.Connection, rfp_id: int, thread_id: int, user_q: str, k: int = 6) -> list[dict]:
    """
    Build messages for CO Chat with strict format.
    If the user question is generic or short, produce a full CO Readout inside "Answer".
    Output shape is fixed:
    Answer
    Citations: [C#,...]
    Missing: [list or []]
    """
    def _is_generic(q: str) -> bool:
        s = (q or "").strip().lower()
        if len(s) < 8:
            return True
        generic_terms = [
            "what is this", "overview", "break down", "breakdown", "help me understand",
            "explain", "summarize", "summary", "walk me through", "details", "key points"
        ]
        return any(g in s for g in generic_terms)

    system = (
        "You are Contracting Officer Chat for ELA. Purpose answer questions strictly from EVIDENCE. "
        "Never use outside knowledge. If evidence is insufficient say what is missing and stop. "
        "Cite every factual claim with [C#] that maps to the cited chunk id. Style concise. "
        "No speculation. No advice that changes scope. "
        "If the question is generic or short, write a CO Readout in the Answer with these labeled parts: "
        "Overview; Key dates; POCs; NAICS and set-aside; Place of performance; Scope summary; "
        "Submission instructions; Evaluation criteria; CLINs summary; Risks; Missing items. Each labeled part must end with "Sources: [C#,…]"."
        "\\nFormat:\\nAnswer\\nCitations: [C#,...]\\nMissing: [list or []]"
    )

    msgs: list[dict] = [{"role": "system", "content": system}]
    # Research mode advisory
    try:
        import streamlit as _st_local  # ensure st
        if bool(_st_local.session_state.get("y2_flex", False)):
            msgs.append({"role":"system","content":"Research mode ON. Include policy context from research excerpts in EVIDENCE when helpful. Do not contradict the RFP text."})
    except Exception:
        pass

    # Thread history
    hist = y2_get_messages(conn, int(thread_id))
    if len(hist) > 40:
        try:
            mid = hist[:-16]
            joined = "\\n".join([f"{m['role'][:1]}: {m['content']}" for m in mid][-2000:])
            client = get_ai()
            model_name = _resolve_model()
            try:
                sr = client.chat.completions.create(
                    model=model_name,
                    messages=[
                        {"role":"system","content":"Summarize the prior CO chat into 5 bullets with only facts and decisions."},
                        {"role":"user","content":joined or "No content"}
                    ],
                    temperature=0.1
                )
                summ = sr.choices[0].message.content.strip() if sr and sr.choices else ""
                if summ:
                    msgs.append({"role":"system","content":"THREAD SUMMARY\\n" + summ})
            except Exception:
                pass
        except Exception:
            pass
        msgs.extend(hist[-16:])
    else:
        msgs.extend(hist)

    q_in = (user_q or "").strip()
    long_mode = _is_generic(q_in)

    # Evidence build with query expansion
    queries = []
    if q_in:
        queries.append(q_in)
    if long_mode:
        queries.extend([
            "offers due OR quotes due OR proposals due OR closing date OR submission deadline",
            "questions due OR Q&A due OR RFI due",
            "NAICS OR North American Industry Classification",
            "set-aside OR small business set-aside OR 8(a) OR SDVOSB OR HUBZone OR WOSB",
            "place of performance OR POP OR performance location OR site",
            "Section L OR Instructions to Offerors OR submission instructions",
            "Section M OR Evaluation OR basis of award OR best value OR LPTA",
            "CLIN OR Schedule of Items OR Bid Schedule OR price sheet",
            "period of performance OR POP OR base period OR option",
            "Points of Contact OR POC OR contracting officer OR specialist OR email OR phone",
            "Statement of Work OR SOW OR Performance Work Statement OR PWS OR scope",
            "attachments OR amendment OR addendum OR incorporated by reference"
        ])
    queries.append("due date OR proposal due OR quotes due OR closing time")

    seen = set()
    ev = []
    for qi in queries:
        try:
            hits = y1_search(conn, int(rfp_id), qi, k=int(max(k, 6 if long_mode else k)))
        except Exception:
            hits = []
        for h in hits or []:
            key = (h.get("file"), h.get("page"), (h.get("text") or "")[:80])
            if key in seen:
                continue
            seen.add(key)
            ev.append(h)

    max_ev = 30 if long_mode else 14
    ev = ev[:max_ev]

    ev_lines = []
    for i, h in enumerate(ev, start=1):
        tag = f"[C{i}]"
        src_line = f"{h.get('file','')} p.{h.get('page','')}"
        snip = (h.get("text") or "").strip().replace("\\n"," ")
        ev_lines.append(f"{tag} {src_line} — {snip}")
    evidence = "\\n".join(ev_lines)

    if long_mode:
        user_hdr = "QUESTION\\nProvide a CO Readout with the labeled parts requested in the system message."
    else:
        user_hdr = "QUESTION\\n" + q_in
    user = user_hdr + "\\n\\nEVIDENCE\\n" + (evidence if evidence else "(none)")
    msgs.append({"role":"user","content": user})
    return msgs

def y2_stream_answer(conn: sqlite3.Connection, rfp_id: int, thread_id: int, user_q: str, k: int = 6, temperature: float = 0.2):
    msgs = _y2_build_messages(conn, int(rfp_id), int(thread_id), user_q or "", k=int(k))
    client = get_ai()
    model_name = _resolve_model()
    # Planning step (R3)
    try:
        ev_text = ""
        for m in msgs:
            if m.get("role")=="user" and "EVIDENCE" in (m.get("content") or ""):
                ev_text = m["content"].split("EVIDENCE",1)[-1][:18000]
                break
        plan_sys = "Plan first. List the labeled parts you will produce and map each to [C#] you intend to use. No prose."
        plan_user = "EVIDENCE" + (ev_text or "")
        try:
            pr = client.chat.completions.create(
                model=model_name,
                messages=[{"role":"system","content": plan_sys},{"role":"user","content": plan_user}],
                temperature=0.1
            )
        except Exception:
            pr = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role":"system","content": plan_sys},{"role":"user","content": plan_user}],
                temperature=0.1
            )
        plan = (pr.choices[0].message.content or "").strip()
        if plan:
            msgs.insert(1, {"role":"system","content": "Use this plan:\n" + plan[:1200]})
    except Exception:
        pass
    try:
        resp = client.chat.completions.create(model=model_name, messages=msgs, temperature=float(temperature), stream=True)
    except Exception as _e:
        if "model_not_found" in str(_e) or "does not exist" in str(_e):
            resp = client.chat.completions.create(model="gpt-4o-mini", messages=msgs, temperature=float(temperature), stream=True)
        else:
            yield f"AI unavailable: {type(_e).__name__}: {_e}"
            return
    for ch in resp:
        try:
            delta = ch.choices[0].delta
            if hasattr(delta, "content") and delta.content:
                yield delta.content
        except Exception:
            pass

def y2_ui_threaded_chat(conn: sqlite3.Connection) -> None:
    st.caption("CO Chat with memory. Threads are stored per RFP.")
    df_rf = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
    if df_rf is None or df_rf.empty:
        st.info("No RFPs yet. Parse & save first.")
        return
    rfp_id = st.selectbox("RFP context", options=df_rf["id"].tolist(), format_func=lambda i: f"#{i} — {df_rf.loc[df_rf['id']==i,'title'].values[0]}", key="y2_rfp_sel")
    threads = y2_list_threads(conn, int(rfp_id))
    create = st.button("New thread", key="y2_new")
    if create:
        tid = y2_create_thread(conn, int(rfp_id), title="CO guidance")
        st.session_state["y2_thread_id"] = tid
        st.rerun()
    if threads:
        pick = st.selectbox("Thread", options=[t["id"] for t in threads], format_func=lambda i: next((f"#{t['id']} — {t.get('title') or 'Untitled'}" for t in threads if t['id']==i), f"#{i}"), key="y2_pick")
        thread_id = int(pick)
    else:
        st.info("No threads yet. Click New thread.")
        return
    with st.expander("Thread settings", expanded=False):
        cur_title = next((t.get("title") or "Untitled" for t in threads if t["id"]==thread_id), "Untitled")
        new_title = st.text_input("Rename", value=cur_title, key="y2_rename")
        colA, colB = st.columns([2,1])
        with colA:
            if st.button("Save name", key="y2_save_name"):
                y2_rename_thread(conn, int(thread_id), new_title or "Untitled")
                st.success("Renamed")
                st.rerun()
        with colB:
            if st.button("Delete thread", key="y2_del"):
                y2_delete_thread(conn, int(thread_id))
                st.success("Deleted")
                st.rerun()
    st.checkbox("Research mode (flex)", value=bool(st.session_state.get("y2_flex", False)), key="y2_flex")
st.subheader("History")
    msgs = y2_get_messages(conn, int(thread_id))
    if msgs:
        chat_md = []
        for m in msgs:
            if m["role"] == "user":
                chat_md.append(f"**You:** {m['content']}")
            elif m["role"] == "assistant":
                chat_md.append(m["content"])
        st.markdown("\n\n".join(chat_md))
    else:
        st.caption("No messages yet.")
    q = st.text_area("Your question", height=120, key="y2_q")
    k = y_auto_k(q)
    if st.button("Ask (store to thread)", type="primary", key="y2_go"):
        if not (q or "").strip():
            st.warning("Enter a question")
        else:
            y2_append_message(conn, int(thread_id), "user", q.strip())
            ph = st.empty(); acc = []
            for tok in y2_stream_answer(conn, int(rfp_id), int(thread_id), q.strip(), k=int(k)):
                acc.append(tok); ph.markdown("".join(acc))
            ans = "".join(acc).strip()
            if ans:
                y2_append_message(conn, int(thread_id), "assistant", ans)
                st.success("Saved to thread")
                with st.expander("Add to Proposal Drafts", expanded=False):
                    sec = st.text_input("Section label", value="CO Chat Notes", key="y5_sec_y2")
                    if st.button("Add to drafts", key="y5_add_y2"):
                        y5_save_snippet(conn, int(rfp_id), sec, ans, source="Y2 Chat")
                        st.success("Saved to drafts")
# === end Y2 ===

# === Research cache (R2) ===
def _research_cache_dir() -> str:
    d = os.path.join(DATA_DIR, "research_cache")
    try:
        os.makedirs(d, exist_ok=True)
    except Exception:
        pass
    return d

def _sha16(s: str) -> str:
    try:
        import hashlib
        return hashlib.sha256((s or "").encode("utf-8")).hexdigest()[:16]
    except Exception:
        return str(abs(hash((s or ""))))[:16]

def research_fetch(url: str, ttl_hours: int = 24) -> dict:
    """
    Fetch a URL with simple on-disk cache.
    Returns dict {url, path, cached, status, text}.
    """
    out = {"url": url, "cached": False, "status": 0, "path": "", "text": ""}
    if not (url or "").strip():
        return out
    key = _sha16(url)
    dirp = _research_cache_dir()
    meta_path = os.path.join(dirp, f"{key}.json")
    txt_path  = os.path.join(dirp, f"{key}.txt")

    # serve cache if fresh
    try:
        if os.path.exists(meta_path) and os.path.exists(txt_path):
            with open(meta_path, "r", encoding="utf-8") as fh:
                meta = json.load(fh)
            age = time.time() - float(meta.get("ts", 0))
            if age < ttl_hours * 3600:
                out.update(meta)
                with open(txt_path, "r", encoding="utf-8", errors="ignore") as fh:
                    out["text"] = fh.read()
                out["cached"] = True
                out["status"] = int(out.get("status", 200) or 200)
                out["path"] = txt_path
                return out
    except Exception:
        pass

    # fetch fresh
    try:
        import requests  # lazy import
        r = requests.get(url, timeout=20, headers={"User-Agent":"ELA-GovCon/1.0"})
        status = int(getattr(r, "status_code", 0) or 0)
        text = r.text if hasattr(r, "text") else ""
        # persist
        try:
            with open(txt_path, "w", encoding="utf-8") as fh:
                fh.write(text or "")
            with open(meta_path, "w", encoding="utf-8") as fh:
                json.dump({"url": url, "status": status, "ts": time.time(), "path": txt_path}, fh)
        except Exception:
            pass
        out.update({"status": status, "text": text, "path": txt_path})
        return out
    except Exception as e:
        out.update({"status": 0, "error": str(e)})
        return out

def research_extract_excerpt(text: str, query: str, window: int = 380) -> str:
    t = (text or "")
    if not t:
        return ""
    q = (query or "").strip()
    idx = -1
    if q:
        try:
            idx = t.lower().find(q.lower())
        except Exception:
            idx = -1
    if idx < 0:
        return t[:window]
    start = max(0, idx - window//2)
    end = min(len(t), idx + window//2)
    return t[start:end]



# === Y3: Proposal drafting from evidence (per-RFP) ===
def _y3_collect_ctx(conn: sqlite3.Connection, rfp_id: int, max_items: int = 20) -> dict:
    ctx: dict = {}
    try:
        df_items = pd.read_sql_query("SELECT item_text FROM lm_items WHERE rfp_id=? ORDER BY id;", conn, params=(int(rfp_id),))
    except Exception:
        df_items = pd.DataFrame(columns=["item_text"])
    ctx["lm"] = df_items["item_text"].tolist()[:max_items] if isinstance(df_items, pd.DataFrame) and not df_items.empty else []
    try:
        df_clins = pd.read_sql_query("SELECT clin, description FROM clin_lines WHERE rfp_id=? ORDER BY id;", conn, params=(int(rfp_id),))
    except Exception:
        df_clins = pd.DataFrame(columns=["clin","description"])
    ctx["clins"] = [{"clin": str(r.get("clin","")), "desc": str(r.get("description",""))[:160]} for _, r in df_clins.head(max_items).iterrows()] if isinstance(df_clins, pd.DataFrame) and not df_clins.empty else []
    try:
        df_meta = pd.read_sql_query("SELECT key, value FROM rfp_meta WHERE rfp_id=?;", conn, params=(int(rfp_id),))
    except Exception:
        df_meta = pd.DataFrame(columns=["key","value"])
    if isinstance(df_meta, pd.DataFrame) and not df_meta.empty:
        ctx["meta"] = {str(k): str(v) for k,v in zip(df_meta["key"], df_meta["value"])}
    else:
        ctx["meta"] = {}
    try:
        df_rfp = pd.read_sql_query("SELECT title, solnum FROM rfps WHERE id=?;", conn, params=(int(rfp_id),))
        ctx["title"] = df_rfp.iloc[0]["title"] if not df_rfp.empty else ""
        ctx["solnum"] = df_rfp.iloc[0]["solnum"] if not df_rfp.empty else ""
    except Exception:
        ctx["title"] = ""; ctx["solnum"] = ""
    return ctx

def _y3_build_messages(conn: sqlite3.Connection, rfp_id: int, section_title: str, notes: str, k: int = 6, max_words: int | None = None) -> list[dict]:
    ctx = _y3_collect_ctx(conn, int(rfp_id))
    q = f"{section_title} Section L Section M instructions {ctx.get('title','')} {ctx.get('solnum','')}"
    hits = y1_search(conn, int(rfp_id), q, k=int(k)) or []
    ev_lines = []
    for i, h in enumerate(hits, start=1):
        tag = f"[C{i}]"
        src = f"{h['file']} p.{h['page']}"
        snip = (h.get("text") or "").strip().replace("\n"," ")
        ev_lines.append(f"{tag} {src} — {snip}")
    evidence = "\n".join(ev_lines)
    style = (SYSTEM_CO + " Write a proposal section for a federal RFP. Use short, compliant sentences."
             + " Map claims to evidence with bracketed citations like [C1]."
             + " Do not invent requirements. If evidence is missing, state the gap plainly.")
    bullets = "\n".join([f"- {it}" for it in (ctx.get("lm") or [])])
    clins = "\n".join([f"- {c['clin']}: {c['desc']}" for c in (ctx.get("clins") or [])])
    constraints = []
    if ctx["meta"].get("naics"): constraints.append(f"NAICS: {ctx['meta']['naics']}")
    if ctx["meta"].get("set_aside"): constraints.append(f"Set-Aside: {ctx['meta']['set_aside']}")
    if ctx["meta"].get("place_of_performance"): constraints.append(f"Place of Performance: {ctx['meta']['place_of_performance']}")
    limit_line = f"Target length: <= {int(max_words)} words." if isinstance(max_words, int) and max_words>0 else "Target brevity."
    user = f"""Draft the section: {section_title}
{limit_line}

RFP title: {ctx.get('title','')}  Solicitation: {ctx.get('solnum','')}
Constraints: {' | '.join(constraints) if constraints else 'n/a'}

Key L/M items to cover:
{bullets or '- n/a'}

Relevant CLINs:
{clins or '- n/a'}

Notes from author:
{notes or 'n/a'}

EVIDENCE:
{evidence}
Write a structured section with a short lead paragraph, 3–6 bullets, and an optional close. Use [C#] next to any factual or requirement-based claim that is tied to EVIDENCE.
"""
    return [{"role":"system","content": style}, {"role":"user","content": user}]

def y3_stream_draft(conn: sqlite3.Connection, rfp_id: int, section_title: str, notes: str, k: int = 6, max_words: int | None = None, temperature: float = 0.2):
    msgs = _y3_build_messages(conn, int(rfp_id), section_title, notes, k=int(k), max_words=max_words)
    client = get_ai()
    model_name = _resolve_model()
    try:
        resp = client.chat.completions.create(model=model_name, messages=msgs, temperature=float(temperature), stream=True)
    except Exception as _e:
        if "model_not_found" in str(_e) or "does not exist" in str(_e):
            resp = client.chat.completions.create(model="gpt-4o-mini", messages=msgs, temperature=float(temperature), stream=True)
        else:
            yield f"AI unavailable: {type(_e).__name__}: {_e}"
            return
    for ch in resp:
        try:
            delta = ch.choices[0].delta
            if hasattr(delta, "content") and delta.content:
                yield delta.content
        except Exception:
            pass
# === end Y3 ===

# === Y4: CO Review (scored compliance with citations) ===
def _y4_build_messages(conn: sqlite3.Connection, rfp_id: int, draft_text: str, k: int = 6) -> list[dict]:
    """
    Build messages for a Contracting Officer style review.
    Output must include: Score 0–100, Strengths, Gaps, Risks, Required fixes, and short Conclusion.
    Use [C#] next to claims grounded in EVIDENCE.
    """
    # Reuse Section L/M + compliance as retrieval query
    q = f"Section L Section M compliance checklist evaluation {draft_text[:400]}"
    hits = y1_search(conn, int(rfp_id), q, k=int(k)) or []
    ev_lines = []
    for i, h in enumerate(hits, start=1):
        tag = f"[C{i}]"
        src = f"{h['file']} p.{h['page']}"
        snip = (h.get("text") or "").strip().replace("\\n"," ")
        ev_lines.append(f"{tag} {src} — {snip}")
    evidence = "\\n".join(ev_lines)

    system = (SYSTEM_CO
              + " Score the draft 0-100 for compliance and clarity."
              + " Use short, direct sentences. No fluff."
              + " Structure exactly as:\\n"
              + "Score: <0-100>\\n"
              + "Strengths: <bullets>\\n"
              + "Gaps: <bullets>\\n"
              + "Risks: <bullets>\\n"
              + "Required fixes: <bullets>\\n"
              + "Conclusion: <2-3 lines>\\n"
              + "Map factual or requirement statements to EVIDENCE using [C#]."
             )

    user = f"DRAFT TO REVIEW:\\n{draft_text or '(empty)'}\\n\\nEVIDENCE:\\n{evidence or '(no evidence found)'}"
    return [{"role":"system","content": system}, {"role":"user","content": user}]

def y4_stream_review(conn: sqlite3.Connection, rfp_id: int, draft_text: str, k: int = 6, temperature: float = 0.1):
    msgs = _y4_build_messages(conn, int(rfp_id), draft_text or "", k=int(k))
    client = get_ai()
    model_name = _resolve_model()
    try:
        resp = client.chat.completions.create(model=model_name, messages=msgs, temperature=float(temperature), stream=True)
    except Exception as _e:
        if "model_not_found" in str(_e) or "does not exist" in str(_e):
            resp = client.chat.completions.create(model="gpt-4o-mini", messages=msgs, temperature=float(temperature), stream=True)
        else:
            yield f"AI unavailable: {type(_e).__name__}: {_e}"; return
    for ch in resp:
        try:
            delta = ch.choices[0].delta
            if hasattr(delta, "content") and delta.content:
                yield delta.content
        except Exception:
            pass

def y4_ui_review(conn: sqlite3.Connection) -> None:
    st.caption("CO Review with score, strengths, gaps, risks, and required fixes. Citations auto-selected.")
    df_rf = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
    if df_rf is None or df_rf.empty:
        st.info("No RFPs yet. Parse & save first."); return
    rfp_id = st.selectbox("RFP context", options=df_rf["id"].tolist(),
                          format_func=lambda i: f"#{i} — {df_rf.loc[df_rf['id']==i,'title'].values[0]}",
                          key="y4_rfp_sel")
    mode = st.radio("Mode", ["Paste", "Upload", "Use RFP files"], horizontal=True, key="y5_mode")
    st.subheader("Draft to review")

    draft_text = ""
    uploaded = None
    if mode == "Paste":
        draft_text = st.text_area("Paste your draft here", value=st.session_state.get("y4_draft",""), height=260, key="y4_draft_ta")
    elif mode == "Upload":
        uploaded = st.file_uploader("Upload DOCX/PDF/TXT", type=["docx","pdf","txt"], accept_multiple_files=True, key="y5_up")
        if uploaded:
            with st.spinner("Extracting..."):
                draft_text = y5_extract_from_uploads(uploaded)[:400000]
            st.text_area("Preview", value=draft_text[:20000], height=240)
    else:
        if st.button("Assemble from linked RFP files", key="y5_from_rfp"):
            with st.spinner("Collecting text from linked files..."):
                draft_text = y5_extract_from_rfp(conn, int(rfp_id))[:400000]
            st.session_state["y5_rfp_text"] = draft_text
        draft_text = st.session_state.get("y5_rfp_text","")
        if draft_text:
            st.text_area("Preview", value=draft_text[:20000], height=240)

    k = y_auto_k(draft_text or "review")
    chunking = st.checkbox("Auto-chunk long text", value=True, key="y5_chunk_on")
    run = st.button("Run CO Review", type="primary", key="y4_go")
    if run:
        if not (draft_text or "").strip():
            st.warning("Provide input text."); return
        texts = y5_chunk_text(draft_text) if chunking else [draft_text]
        ph = st.empty()
        all_out = []
        for idx, t in enumerate(texts, start=1):
            acc = []
            ph.caption(f"Reviewing chunk {idx}/{len(texts)}")
            for tok in y4_stream_review(conn, int(rfp_id), t.strip(), k=int(k)):
                acc.append(tok)
                ph.markdown("".join(acc))
            all_out.append("".join(acc))
        final = "\n\n".join(all_out).strip()
        st.session_state["y4_last_review"] = final
        st.subheader("Combined result")
        st.markdown(final or "_no output_")

        # Sources table
        hits = y1_search(conn, int(rfp_id), f"Section L Section M compliance {draft_text[:200]}", k=int(k))
        if hits:
            import pandas as _pd
            dfh = _pd.DataFrame([{"Tag": f"[C{i+1}]", "File": h["file"], "Page": h["page"], "Score": h["score"]} for i,h in enumerate(hits)])
            st.subheader("Sources used")
            st.dataframe(dfh, use_container_width=True, hide_index=True)

        with st.expander("Add to Proposal Drafts", expanded=True):
            section = st.text_input("Section label", value="CO Review Notes", key="y5_sec_rev")
            if st.button("Add review to drafts", key="y5_add_rev"):
                y5_save_snippet(conn, int(rfp_id), section, final or draft_text, source="Y4 Review")
                st.success("Saved to drafts")


# === Y5: Upload Review + Drafts plumbing ===
from contextlib import closing
import io

def ensure_y5_tables(conn: sqlite3.Connection) -> None:
    try:
        with closing(conn.cursor()) as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS draft_snippets("
                "id INTEGER PRIMARY KEY,"
                "rfp_id INTEGER,"
                "section TEXT,"
                "source TEXT,"
                "text TEXT,"
                "created_at TEXT DEFAULT (datetime('now'))"
                ");"
            )
            conn.commit()
    except Exception:
        pass

def y5_chunk_text(text: str, target_chars: int = 9000, overlap: int = 500) -> list[str]:
    t = (text or "").strip()
    if not t:
        return []
    if len(t) <= target_chars:
        return [t]
    out: list[str] = []
    i = 0
    n = len(t)
    while i < n:
        j = min(n, i + max(2000, target_chars))
        chunk = t[i:j]
        k = chunk.rfind(". ")
        if k > len(chunk) * 0.6:
            chunk = chunk[:k+1]
            j = i + k + 1
        out.append(chunk)
        i = max(j - overlap, j)
    return out

def _extract_docx_bytes(data: bytes) -> str:
    try:
        import docx  # python-docx
        doc = docx.Document(io.BytesIO(data))
        return "\n".join(p.text for p in doc.paragraphs)
    except Exception:
        return ""

def _extract_pdf_bytes(data: bytes) -> str:
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(data))
        return "\n\n".join((page.extract_text() or "") for page in reader.pages)
    except Exception:
        return ""

def y5_extract_from_uploads(files) -> str:
    if not files:
        return ""
    parts: list[str] = []
    for f in files:
        try:
            try:
                data = f.getbuffer().tobytes()
            except Exception:
                data = f.read()
            name = (getattr(f, "name", "") or "").lower()
            if name.endswith(".txt"):
                try:
                    parts.append(data.decode("utf-8", "ignore"))
                except Exception:
                    parts.append(str(data))
            elif name.endswith(".docx"):
                parts.append(_extract_docx_bytes(data))
            elif name.endswith(".pdf"):
                parts.append(_extract_pdf_bytes(data))
        except Exception:
            continue
    return "\n\n".join([p for p in parts if p]).strip()

def y5_extract_from_rfp(conn: sqlite3.Connection, rfp_id: int) -> str:
    # Expect rfp_files(filename TEXT, mime TEXT, bytes BLOB, rfp_id INT)
    try:
        df = pd.read_sql_query(
            "SELECT filename, mime, bytes FROM rfp_files WHERE rfp_id=? ORDER BY id;",
            conn, params=(int(rfp_id),)
        )
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty:
        return ""
    parts: list[str] = []
    for _, r in df.iterrows():
        data = r.get("bytes")
        if data is None:
            continue
        name = (r.get("filename") or "").lower()
        if name.endswith(".txt"):
            if isinstance(data, (bytes, bytearray)):
                try:
                    parts.append(bytes(data).decode("utf-8", "ignore"))
                except Exception:
                    continue
        elif name.endswith(".docx"):
            parts.append(_extract_docx_bytes(bytes(data)))
        elif name.endswith(".pdf"):
            parts.append(_extract_pdf_bytes(bytes(data)))
    return "\n\n".join([p for p in parts if p]).strip()

def y5_save_snippet(conn: sqlite3.Connection, rfp_id: int, section: str, text: str, source: str = "Y5") -> None:
    if not (text or "").strip():
        return
    try:
        with closing(conn.cursor()) as cur:
            cur.execute(
                "INSERT INTO draft_snippets(rfp_id, section, source, text) VALUES(?,?,?,?);",
                (int(rfp_id), (section or "General").strip(), (source or "Y5").strip(), text.strip())
            )
            conn.commit()
    except Exception:
        pass
# === end Y5 ===



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
                rfp_id INTEGER REFERENCES rfps(id) ON DELETE SET NULL,
                amount REAL,
                stage TEXT,
                status TEXT DEFAULT 'Open',
                close_date TEXT,
                owner TEXT,
                created_at TEXT,
                updated_at TEXT
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
            CREATE TABLE IF NOT EXISTS rfp_files(
    id INTEGER PRIMARY KEY,
    rfp_id INTEGER REFERENCES rfps(id) ON DELETE SET NULL,
    filename TEXT,
    mime TEXT,
    sha256 TEXT UNIQUE,
    pages INTEGER,
    bytes BLOB,
    created_at TEXT
);
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rfp_files_rfp ON rfp_files(rfp_id);")
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_activities_ts ON activities(ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_activities_rel ON activities(deal_id, contact_id);")
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(due_date, status);")
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_owner ON files(owner_type, owner_id);")
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rfq_packs_ctx ON rfq_packs(rfp_id, deal_id);");
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rfq_lines_pack ON rfq_lines(pack_id);");
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rfq_vendors(
                id INTEGER PRIMARY KEY,
                pack_id INTEGER NOT NULL REFERENCES rfq_packs(id) ON DELETE CASCADE,
                vendor_id INTEGER NOT NULL REFERENCES vendors(id) ON DELETE CASCADE
            );
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_rfq_vendors_unique ON rfq_vendors(pack_id, vendor_id);");
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rfq_attach(
                id INTEGER PRIMARY KEY,
                pack_id INTEGER NOT NULL REFERENCES rfq_packs(id) ON DELETE CASCADE,
                file_id INTEGER REFERENCES files(id) ON DELETE SET NULL,
                name TEXT,
                path TEXT
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rfq_attach_pack ON rfq_attach(pack_id);");

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
        cur.execute("INSERT OR IGNORE INTO tenants(id, name, created_at) VALUES(1, 'Default', datetime('now'));")
        cur.execute("INSERT OR IGNORE INTO current_tenant(id, ctid) VALUES(1, 1);")

        def _add_tenant_id(table: str):
            try:
                cols = pd.read_sql_query(f"PRAGMA table_info({table});", conn)
                if "tenant_id" not in cols["name"].tolist():
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN tenant_id INTEGER;")
                    conn.commit()
            except Exception:
                pass
            try:
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_tenant ON {table}(tenant_id);")
            except Exception:
                pass

        core_tables = ["rfps","lm_items","lm_meta","deals","activities","tasks","deal_stage_log",
                       "vendors","files","rfq_packs","rfq_lines","rfq_vendors","rfq_attach","contacts"]
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
                cur.execute(f"CREATE VIEW IF NOT EXISTS {v} AS SELECT * FROM {table} WHERE tenant_id=(SELECT ctid FROM current_tenant WHERE id=1);")
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
        cur.execute("INSERT OR IGNORE INTO schema_version(id, ver) VALUES(1, 0);")
        conn.commit()
    try:
        ensure_y5_tables(conn)
    except Exception:
        pass
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
    if _pypdf is not None:
        return ('pypdf', _pypdf)
    if _pdfplumber is not None:
        return ('pdfplumber', _pdfplumber)
    return None



def extract_text_from_file(path: str) -> str:
    """Unified extractor. Uses Phase X1 backends. Safe on environments missing PDF libs."""
    try:
        with open(path, 'rb') as fh:
            b = fh.read()
    except Exception:
        return ''
    name = os.path.basename(path)
    mime = _detect_mime_light(name)
    pages = extract_text_pages(b, mime)
    if not pages:
        try:
            return b.decode('utf-8', errors='ignore')
        except Exception:
            try:
                return b.decode('latin-1', errors='ignore')
            except Exception:
                return ''
    return "\n\n".join(pages)

        

def _detect_mime_light(name: str) -> str:
    n = (name or "").lower()
    if n.endswith(".pdf"): return "application/pdf"
    if n.endswith(".docx"): return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if n.endswith(".xlsx"): return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if n.endswith(".txt"): return "text/plain"
    return "application/octet-stream"

def _tesseract_ok() -> bool:
    try:
        import pytesseract  # type: ignore
        return True
    except Exception:
        return False


def extract_text_pages(file_bytes: bytes, mime: str) -> list:
    """Return a list of page texts. Best-effort. Up to 100 pages to keep fast."""
    out = []
    m = (mime or "").lower()
    if "pdf" in m:
        # Try vector: pdfplumber then pypdf. Never reference missing names.
        if _pdfplumber is not None:
            try:
                import io as _io
                with _pdfplumber.open(_io.BytesIO(file_bytes)) as pdf:
                    for i, pg in enumerate(pdf.pages[:100]):
                        try:
                            txt = pg.extract_text() or ""
                        except Exception:
                            txt = ""
                        out.append(txt)
            except Exception:
                pass
        if not out and _pypdf is not None:
            try:
                import io as _io
                reader = _pypdf.PdfReader(_io.BytesIO(file_bytes))
                for i, p in enumerate(reader.pages[:100]):
                    try:
                        out.append(p.extract_text() or "")
                    except Exception:
                        out.append("")
            except Exception:
                pass
        # As a very last resort, try a naive decode of bytes to avoid empty output.
        if not out:
            try:
                out = [file_bytes.decode("utf-8", errors="ignore")]
            except Exception:
                out = []
    elif "wordprocessingml" in m or (mime == "" and file_bytes[:4] == b"PK\x03\x04"):
        try:
            import io as _io, docx  # type: ignore
            doc = docx.Document(_io.BytesIO(file_bytes))
            txt = "\n".join(p.text or "" for p in doc.paragraphs)
            out = [txt] if txt else []
        except Exception:
            out = []
    elif "spreadsheetml" in m:
        try:
            import io as _io, pandas as _pd  # type: ignore
            x = _pd.read_excel(_io.BytesIO(file_bytes), sheet_name=None, dtype=str)
            for sname, df in list(x.items())[:10]:
                txt = sname + "\n" + df.fillna("").astype(str).to_csv(sep="\t", index=False)
                out.append(txt)
        except Exception:
            out = []
    elif "text/plain" in m:
        try:
            out = [file_bytes.decode("utf-8", errors="ignore")]
        except Exception:
            out = [file_bytes.decode("latin-1", errors="ignore")]
    return out

def ocr_pages_if_empty(file_bytes: bytes, mime: str, pages_text: list) -> tuple:
    """Run OCR on empty PDF pages if pytesseract available. Returns (new_pages, ocr_count)."""
    if "pdf" not in (mime or "").lower():
        return pages_text, 0
    if not _tesseract_ok():
        return pages_text, 0
    try:
        import io as _io
        pdfplumber = _pdfplumber  # type: ignore
        import pytesseract  # type: ignore
        from PIL import Image  # type: ignore
        new_pages = list(pages_text)
        ocr_count = 0
        if pdfplumber is None:
            return pages_text, 0
        with pdfplumber.open(_io.BytesIO(file_bytes)) as pdf:
            for i, pg in enumerate(pdf.pages[:min(len(new_pages) or 100, 100)]):
                if i >= len(new_pages): new_pages.append("")
                if (new_pages[i] or "").strip():
                    continue
                try:
                    img = pg.to_image(resolution=200).original
                    if img is None:
                        continue
                    if not isinstance(img, Image.Image):
                        img = Image.fromarray(img)
                    txt = pytesseract.image_to_string(img) or ""
                    if txt.strip():
                        new_pages[i] = txt
                        ocr_count += 1
                except Exception:
                    pass
        return new_pages, ocr_count
    except Exception:
        return pages_text, 0

def save_rfp_file_db(conn: sqlite3.Connection, rfp_id: int | None, name: str, file_bytes: bytes) -> dict:
    """Dedup by sha256. Store bytes and basic stats. Return dict with id and stats."""
    mime = _detect_mime_light(name)
    sha = compute_sha256(file_bytes)
    with closing(conn.cursor()) as cur:
        # Dedup
        cur.execute("SELECT id, pages FROM rfp_files WHERE sha256=?;", (sha,))
        row = cur.fetchone()
        if row:
            rid = int(row[0]); pages = int(row[1]) if row[1] is not None else None
            # if not linked to RFP yet, link now
            if rfp_id is not None:
                try:
                    cur.execute("UPDATE rfp_files SET rfp_id=COALESCE(rfp_id, ?) WHERE id=?;", (int(rfp_id), rid))
                    conn.commit()
                except Exception:
                    pass
            return {"id": rid, "sha256": sha, "filename": name, "mime": mime, "pages": pages, "dedup": True, "ocr_pages": 0}
        # New insert
        pages_text = extract_text_pages(file_bytes, mime)
        pages_before = len(pages_text)
        pages_text, ocr_count = ocr_pages_if_empty(file_bytes, mime, pages_text)
        pages = len(pages_text) if pages_text else None
        cur.execute(
            "INSERT INTO rfp_files(rfp_id, filename, mime, sha256, pages, bytes, created_at) VALUES(?,?,?,?,?,?, datetime('now'));",
            (int(rfp_id) if rfp_id is not None else None, name, mime, sha, pages or 0, sqlite3.Binary(file_bytes))
        )
        rid = cur.lastrowid
        conn.commit()
        return {"id": rid, "sha256": sha, "filename": name, "mime": mime, "pages": pages or 0, "dedup": False, "ocr_pages": ocr_count}

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
def _num_from_words(s: str) -> int | None:
    m = re.search(r'\d+', s or '')
    if m:
        return int(m.group(0))
    words = {'one':1,'two':2,'three':3,'four':4,'five':5,'six':6,'seven':7,'eight':8,'nine':9,'ten':10,'eleven':11,'twelve':12}
    t = (s or '').strip().lower()
    return words.get(t)

def extract_pop_structure(text: str) -> dict:
    if not text:
        return {}
    tl = text.lower()
    base = 1 if re.search(r'\bbase\s+(year|period)\b', tl) else 0
    oy = 0
    for pat in [r'(\d+)\s+option\s+(?:years?|periods?)',
                r'(one|two|three|four|five|six|seven|eight|nine|ten)\s+option\s+(?:years?|periods?)',
                r'option\s+years?\s+(one|two|three|four|five|six|seven|eight|nine|ten|\d+)\s*(?:through|-|to)\s*(one|two|three|four|five|six|seven|eight|nine|ten|\d+)\b']:
        m = re.search(pat, tl)
        if m:
            if len(m.groups())==2 and m.group(2):
                a = _num_from_words(m.group(1)) or 0
                b = _num_from_words(m.group(2)) or 0
                if b>=a and a>0:
                    oy = max(oy, b - a + 1)
            else:
                oy = max(oy, _num_from_words(m.group(1)) or 0)
    total_years = None
    m = re.search(r'(ordering|contract)\s+period[^\d]{0,20}(\d+)\s+year', tl)
    if m:
        try:
            total_years = int(m.group(2))
        except Exception:
            total_years = None
    base_months = None
    m = re.search(r'base\s+(?:year|period)[^\d]{0,12}(\d{1,2})\s+month', tl)
    if m:
        try:
            base_months = int(m.group(1))
        except Exception:
            base_months = None
    out = {}
    if base or oy:
        label = "Base" if base else ""
        if oy:
            if label:
                label += " + "
            label += f"{oy} Option Year{'s' if oy!=1 else ''}"
        out['pop_structure'] = label.strip()
    if total_years:
        out['ordering_period_years'] = total_years
    if base_months:
        out['base_months'] = base_months
    return out


# === Y5.5: AI-assisted Parse & Save (safe indent) ===
def y55_ai_parse(text: str) -> dict:
    out = {"title":"", "solnum":"", "meta":{}, "l_items":[], "clins":[], "dates":[], "pocs":[]}
    t = (text or "").strip()
    if not t:
        return out
    try:
        client = get_ai()
        model_name = _resolve_model()
        sys_msg = SYSTEM_CO + " You extract procurement data exactly and return strict JSON."
        user_msg = (
            "Extract these from the RFP text. Return JSON with keys: "
            "title, solnum, meta:{naics,set_aside,place_of_performance}, "
            "l_items: [bulleted requirement lines], "
            "clins: [{clin, description, qty, unit, unit_price, extended_price}], "
            "dates: [{label, date_text}], "
            "pocs: [{name, role, email, phone}]. "
            "Be terse. Do not hallucinate unknown numbers; leave fields empty if not present.\n\n"
            "RFP TEXT START\n" + t[:180000] + "\nRFP TEXT END"
        )
        try:
            resp = client.chat.completions.create(
                model=model_name,
                messages=[{"role":"system","content":sys_msg},{"role":"user","content":user_msg}],
                temperature=0.0
            )
        except Exception as _e:
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role":"system","content":sys_msg},{"role":"user","content":user_msg}],
                temperature=0.0
            )
        raw = ""
        try:
            raw = resp.choices[0].message.content or ""
        except Exception:
            raw = ""
        import json, re
        m = re.search(r'\{.*\}', raw, re.S)
        data = {}
        if m:
            try:
                data = json.loads(m.group(0))
            except Exception:
                data = {}
        if not data and raw.strip():
            raw2 = raw.strip().strip("`").strip()
            try:
                data = json.loads(raw2)
            except Exception:
                data = {}
        if isinstance(data, dict):
            for k in out.keys():
                if k in data and data[k]:
                    out[k] = data[k]
        if not isinstance(out.get("l_items"), list): out["l_items"] = []
        if not isinstance(out.get("clins"), list): out["clins"] = []
        if not isinstance(out.get("dates"), list): out["dates"] = []
        if not isinstance(out.get("pocs"), list): out["pocs"] = []
        if not isinstance(out.get("meta"), dict): out["meta"] = {}
        out["title"] = (out.get("title") or "").strip()[:200]
        out["solnum"] = (out.get("solnum") or "").strip()[:80]
        return out
    except Exception:
        return out

def _y55_norm_str(x):
    import re
    try:
        return re.sub(r'\s+',' ', str(x or '')).strip()
    except Exception:
        return str(x)

def y55_merge_lm(base_list, ai_list):
    base = [_y55_norm_str(x) for x in (base_list or []) if _y55_norm_str(x)]
    ai = [_y55_norm_str(x) for x in (ai_list or []) if _y55_norm_str(x)]
    seen = set(); out = []
    for it in base + ai:
        if it not in seen:
            seen.add(it); out.append(it)
    return out[:1000]

def y55_merge_clins(base_rows, ai_rows):
    def norm_row(r):
        return {
            "clin": _y55_norm_str((r or {}).get("clin")),
            "description": _y55_norm_str((r or {}).get("description"))[:300],
            "qty": _y55_norm_str((r or {}).get("qty")),
            "unit": _y55_norm_str((r or {}).get("unit")),
            "unit_price": _y55_norm_str((r or {}).get("unit_price")),
            "extended_price": _y55_norm_str((r or {}).get("extended_price")),
        }
    base = [norm_row(r) for r in (base_rows or [])]
    ai = [norm_row(r) for r in (ai_rows or [])]
    out = []; seen = set()
    for r in base + ai:
        key = (r["clin"], r["description"], r["qty"], r["unit_price"], r["extended_price"])
        if key in seen: 
            continue
        seen.add(key); out.append(r)
    return out[:2000]

def y55_merge_dates(base_rows, ai_rows):
    def norm(r):
        return {"label": _y55_norm_str((r or {}).get("label")), "date_text": _y55_norm_str((r or {}).get("date_text")), "date_iso": _y55_norm_str((r or {}).get("date_iso"))}
    base = [norm(r) for r in (base_rows or [])]
    ai = [norm(r) for r in (ai_rows or [])]
    out = []; seen = set()
    for r in base + ai:
        key = (r["label"], r["date_text"])
        if key in seen: 
            continue
        seen.add(key); out.append(r)
    return out[:300]

def y55_merge_pocs(base_rows, ai_rows):
    def norm(r):
        return {"name": _y55_norm_str((r or {}).get("name")), "role": _y55_norm_str((r or {}).get("role") or "POC"),
                "email": _y55_norm_str((r or {}).get("email")), "phone": _y55_norm_str((r or {}).get("phone"))}
    base = [norm(r) for r in (base_rows or [])]
    ai = [norm(r) for r in (ai_rows or [])]
    out = []; seen = set()
    for r in base + ai:
        key = (r["name"], r["email"], r["phone"])
        if key in seen: 
            continue
        seen.add(key); out.append(r)
    return out[:300]

def y55_apply_enhancement(text, l_items, clins, dates, pocs, meta, title, solnum):
    ai = y55_ai_parse(text or "")
    l_items2 = y55_merge_lm(l_items, ai.get("l_items", []))
    clins2 = y55_merge_clins(clins, ai.get("clins", []))
    dates2 = y55_merge_dates(dates, ai.get("dates", []))
    pocs2 = y55_merge_pocs(pocs, ai.get("pocs", []))
    meta2 = dict(meta or {})
    for k in ("naics","set_aside","place_of_performance"):
        v = (ai.get("meta", {}) or {}).get(k)
        if v:
            meta2[k] = _y55_norm_str(v)
    title2 = (ai.get("title") or "").strip() or (title or "")
    solnum2 = (ai.get("solnum") or "").strip() or (solnum or "")
    return (l_items2, clins2, dates2, pocs2, meta2, title2, solnum2)
# === end Y5.5 ===

def extract_clins_xlsx(file_bytes: bytes) -> list:
    try:
        import io, pandas as _pd
        wb = _pd.read_excel(io.BytesIO(file_bytes), sheet_name=None, dtype=str)
    except Exception:
        return []
    rows = []
    for sname, df in wb.items():
        if df is None or df.empty:
            continue
        df2 = df.copy()
        df2.columns = [str(c).strip() for c in df2.columns]
        low = [c.lower() for c in df2.columns]
        def _pick(fn):
            for i,c in enumerate(low):
                try:
                    if fn(c):
                        return df2.columns[i]
                except Exception:
                    continue
            return None
        col_clin = _pick(lambda c: 'clin' in c or 'line item' in c or c.startswith('clin') or c.startswith('line'))
        col_desc = _pick(lambda c: 'desc' in c or c=='item' or 'description' in c)
        col_qty  = _pick(lambda c: 'qty' in c or 'quantity' in c)
        col_unit = _pick(lambda c: c in ('unit','u/i','uom') or 'unit ' in c or 'uom' in c)
        col_upr  = _pick(lambda c: 'unit price' in c or (('unit' in c or 'price' in c) and 'total' not in c and 'ext' not in c and 'extended' not in c))
        col_ext  = _pick(lambda c: 'extended' in c or 'amount' in c or c=='total' or 'total price' in c or 'ext price' in c)
        if (col_clin or col_desc) and (col_qty or col_upr or col_ext):
            for _, r in df2.iterrows():
                def gv(col):
                    return "" if col is None else str(r.get(col, "")).strip()
                clin = gv(col_clin); desc = gv(col_desc); qty  = gv(col_qty)
                unit = gv(col_unit); upr  = gv(col_upr);  ext  = gv(col_ext)
                if any([clin, desc, qty, upr, ext]):
                    rows.append({'clin': clin, 'description': desc[:300] if desc else "", 'qty': qty, 'unit': unit, 'unit_price': upr, 'extended_price': ext})
    seen = set(); uniq = []
    for r in rows:
        key = (r['clin'], r['description'], r['qty'], r['unit_price'], r['extended_price'])
        if key in seen: continue
        seen.add(key); uniq.append(r)
    return uniq[:2000]



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



def run_research_tab(conn: sqlite3.Connection) -> None:
    st.header("Research (FAR/DFARS/Wage/NAICS)")
    url = st.text_input("URL", placeholder="https://www.acquisition.gov/...")
    ttl = st.number_input("Cache TTL (hours)", min_value=1, max_value=168, value=24, step=1)
    q = st.text_input("Highlight phrase (optional)")
    if st.button("Fetch", type="primary", key="research_fetch_btn"):
        with st.spinner("Fetching..."):
            rec = research_fetch(url.strip(), ttl_hours=int(ttl))
        if rec.get("status", 0) != 200 and not rec.get("cached"):
            st.error(f"Fetch failed or not cached. Status {rec.get('status')} — {rec.get('error','')}")
        else:
            st.success(("Loaded from cache" if rec.get("cached") else "Fetched") + f" — status {rec.get('status')}")
            txt = rec.get("text","")
            ex = research_extract_excerpt(txt, q or "")
            st.text_area("Excerpt", value=ex, height=240)
            if rec.get("path"):
                st.markdown(f"[Open cached text]({rec['path']})")
    st.caption("Shortcuts: FAR | DFARS | Wage Determinations | NAICS | SBA Size Standards")

def run_rfp_analyzer(conn: sqlite3.Connection) -> None:
    st.header("RFP Analyzer")
    tab_parse, tab_checklist, tab_data, tab_y1, tab_y2, tab_y4 = st.tabs(["Research", "Parse & Save", "Checklist", "CLINs/Dates/POCs", "Ask with citations (Y1)", "CO Chat (Y2)", "CO Review (Y4)"])
    

    # --- heuristics to auto-fill Title and Solicitation # ---
    def _guess_title(text: str, fallback: str) -> str:
        for line in (text or "").splitlines():
            s = line.strip()
            if len(s) >= 8 and not s.lower().startswith(("department of", "u.s.", "united states", "naics", "set-aside", "solicitation", "request for", "rfp", "rfq", "sources sought")):
                return s[:200]
        return fallback

    def _guess_solnum(text: str) -> str:
        if not text:
            return ""
    # --- meta extractors (NAICS, Set-Aside, Place of Performance) ---
    def _extract_naics(text: str) -> str:
        if not text: return ""
        m = re.search(r'(?i)NAICS(?:\s*Code)?\s*[:#]?\s*([0-9]{5,6})', text)
        if m: return m.group(1)[:6]
        m = re.search(r'(?i)NAICS[^\n]{0,50}?([0-9]{6})', text)
        if m: return m.group(1)
        m = re.search(r'(?i)(?:industry|classification)[^\n]{0,50}?([0-9]{6})', text)
        return m.group(1) if m else ""

    def _extract_set_aside(text: str) -> str:
        if not text: return ""
        tags = ["SDVOSB","SDVOSBC","WOSB","EDWOSB","8(a)","8A","HUBZone","SBA","SDB","VOSB","Small Business","Total Small Business"]
        for t in tags:
            if re.search(rf'(?i)\b{re.escape(t)}\b', text):
                norm = t.upper().replace("(A)","8A").replace("TOTAL SMALL BUSINESS","SMALL BUSINESS")
                if norm == "8(A)": norm = "8A"
                return norm
        m = re.search(r'(?i)Set[- ]Aside\s*[:#]?\s*([A-Za-z0-9 \-/\(\)]+)', text)
        if m:
            v = m.group(1).strip()
            v = re.sub(r'\s+', ' ', v)
            return v[:80]
        return ""

    def _extract_place(text: str) -> str:
        if not text: return ""
        m = re.search(r'(?i)Place\s+of\s+Performance\s*[:\-]?\s*([^\n]{3,80})', text)
        if m: return m.group(1).strip()
        m = re.search(r'\b([A-Z][a-zA-Z]+,\s*(?:[A-Z]{2}|[A-Za-z\. ]{3,}))\b', text)
        return m.group(1).strip() if m else ""

    # ensure rfp_meta exists
    try:
        with closing(conn.cursor()) as _c:

            _c.execute("""
                CREATE TABLE IF NOT EXISTS rfp_chat(
                    id INTEGER PRIMARY KEY,
                    rfp_id INTEGER NOT NULL REFERENCES rfps(id) ON DELETE CASCADE,
                    ts TEXT,
                    q TEXT,
                    a TEXT
                );
            """)
            _c.execute("""
                CREATE TABLE IF NOT EXISTS rfp_meta(
                    id INTEGER PRIMARY KEY,
                    rfp_id INTEGER REFERENCES rfps(id) ON DELETE CASCADE,
                    key TEXT,
                    value TEXT
                );
            """)
            conn.commit()
    except Exception:
        pass

        m = re.search(r'(?i)Solicitation\s*(Number|No\.?)\s*[:#]?\s*([A-Z0-9][A-Z0-9\-\._/]{4,})', text)
        if m:
            return m.group(2)[:60]
        m = re.search(r'\b([A-Z0-9]{2,6}[A-Z0-9\-]{0,4}\d{2}[A-Z]?-?[A-Z]?-?\d{3,6})\b', text)
        if m:
            return m.group(1)[:60]
        m = re.search(r'\b(RFQ|RFP|IFB|RFI)[\s#:]*([A-Z0-9][A-Z0-9\-\._/]{3,})\b', text, re.I)
        if m:
            return (m.group(1).upper() + "-" + m.group(2))[:60]
        return ""
# ---------------- PARSE & SAVE ----------------
    with tab_parse:

        # --- X1 Ingest: File Library + Health ---
        if True:
            with st.expander("X1 Ingest: File Library + Health", expanded=False):
                st.caption("Accepts PDF, DOCX, XLSX, TXT. Deduplicates by SHA-256. Attempts OCR on image-only PDFs if pytesseract is available. — X7 applied")
                try:
                    df_rf_list = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
                    opt_rf = [None] + df_rf_list["id"].tolist() if df_rf_list is not None else [None]
                except Exception:
                    df_rf_list = pd.DataFrame()
                    opt_rf = [None]
                def _fmt_rfp(x):
                    try:
                        if x is None:
                            return "None"
                        ttl = df_rf_list.loc[df_rf_list["id"]==x,"title"]
                        return f"#{x} — {ttl.values[0] if len(ttl) else ''}"
                    except Exception:
                        return str(x) if x is not None else "None"
                link_now_rfp = st.selectbox("Link to existing RFP (optional)", options=opt_rf, format_func=_fmt_rfp, key="x1_link_now_rfp")
                ing_files = st.file_uploader("Files to ingest", type=["pdf","docx","xlsx","txt"], accept_multiple_files=True, key="x1_ing")
                link_to_rfp = st.checkbox("Also remember these to auto-link to the next new RFP created below", value=False)
                if st.button("Ingest Files", key="x1_ingest_btn"):
                    if not ing_files:
                        st.warning("No files selected")
                    else:
                        rows = []
                        ids = []
                        for f in ing_files:
                            try:
                                b = f.getbuffer().tobytes()
                            except Exception:
                                b = f.read()
                            rec = save_rfp_file_db(conn, int(link_now_rfp) if link_now_rfp is not None else None, f.name, b)
                            rows.append({
                                "Filename": rec["filename"],
                                "SHA256": rec["sha256"][:12],
                                "MIME": rec["mime"],
                                "Pages": rec["pages"],
                                "OCR pages": rec.get("ocr_pages", 0),
                                "Dedup?": "Yes" if rec.get("dedup") else "No",
                                "rfp_file_id": rec["id"],
                                "Linked RFP": (int(link_now_rfp) if link_now_rfp is not None else None),
                            })
                            ids.append(int(rec["id"]))
                        import pandas as _pd
                        df_ing = _pd.DataFrame(rows)
                        st.dataframe(df_ing, use_container_width=True, hide_index=True)
                        st.session_state["x1_last_ingested_ids"] = ids
                        st.session_state["x1_pending_link_after_create"] = bool(link_to_rfp)
                        if link_now_rfp is not None:
                            st.success(f"Ingested {len(rows)} file(s). Linked to RFP #{int(link_now_rfp)}.")
                        else:
                            st.success(f"Ingested {len(rows)} file(s).")
        colA, colB = st.columns([3,2])
        with colA:
            ups = st.file_uploader(
                "Upload RFP(s) (PDF/DOCX/TXT)",
                type=["pdf","docx","xlsx","txt"],
                accept_multiple_files=True,
                key="rfp_ups"
            )
            with st.expander("Manual Text Paste (optional)", expanded=False):
                pasted = st.text_area("Paste any text to include in parsing", height=150, key="rfp_paste")
            title = st.text_input("RFP Title (used if combining)", key="rfp_title")
            solnum = st.text_input("Solicitation # (used if combining)", key="rfp_solnum")
            sam_url = st.text_input("SAM URL (used if combining)", key="rfp_sam_url", placeholder="https://sam.gov/...")
            mode = st.radio("Save mode", ["One record per file", "Combine all into one RFP"], index=0, horizontal=True)
        with colB:
            st.markdown("**Parse Controls**")
            run = st.button("Parse & Save", type="primary", key="rfp_parse_btn")
            st.caption("We’ll auto-extract L/M checklist items, CLINs, key dates, and POCs.")

        def _read_file(file):
            name = file.name.lower()
            data = file.read()
            if name.endswith(".txt"):
                try:
                    return data.decode("utf-8")
                except Exception:
                    return data.decode("latin-1", errors="ignore")
            if name.endswith(".pdf"):
                try:
                    reader = _pypdf.PdfReader(io.BytesIO(data))
                    pages = [(p.extract_text() or "") for p in reader.pages]
                    return "\\n".join(pages)
                except Exception as e:
                    st.warning(f"PDF text extraction failed for {file.name}: {e}. Falling back to binary decode.")
                    return data.decode("latin-1", errors="ignore")
            if name.endswith(".docx"):
                try:
                    f = io.BytesIO(data)
                    doc = docx.Document(f)
                    return "\\n".join([p.text for p in doc.paragraphs])
                except Exception as e:
                    st.warning(f"DOCX parse failed for {file.name}: {e}.")
                    return ""
            if name.endswith(".xlsx"):
                try:
                    pages = extract_text_pages(data, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    if pages:
                        return "\n\n".join(pages)
                    return ""
                except Exception as e:
                    st.warning(f"XLSX parse failed for {file.name}: {e}.")
                    return ""
            st.error(f"Unsupported file type: {file.name}")
            return ""

        if run:
            if (not ups) and (not pasted):
                st.error("No input. Upload at least one file or paste text.")
            else:
                if mode == "Combine all into one RFP":
                    text_parts = []
                    for f in ups or []:
                        text_parts.append(_read_file(f))
                    if pasted:
                        text_parts.append(pasted)
                    full_text = "\\n\\n".join([t for t in text_parts if t]).strip()
                    # X3: parse pricing matrices from any uploaded XLSX files
                    x3_clins = []
                    for _f in (ups or []):
                        try:
                            _name = (_f.name or "").lower()
                            _b = _f.getbuffer().tobytes() if hasattr(_f, "getbuffer") else _f.read()
                        except Exception:
                            _name = (_f.name or "").lower()
                            try:
                                _b = _f.read()
                            except Exception:
                                _b = b""
                        if _name.endswith(".xlsx") and _b:
                            x3_clins.extend(extract_clins_xlsx(_b))
                    if not full_text:
                        st.error("Nothing readable found.")
                    else:
                        secs = extract_sections_L_M(full_text)
                        l_items = derive_lm_items(secs.get('L','')) + derive_lm_items(secs.get('M',''))
                        clins = extract_clins(full_text) + (x3_clins or []); dates = extract_dates(full_text); pocs = extract_pocs(full_text)
                        meta = {
                            'naics': _extract_naics(full_text),
                            'set_aside': _extract_set_aside(full_text),
                            'place_of_performance': _extract_place(full_text),
                        }
                        
                        
                        l_items, clins, dates, pocs, meta, title, solnum = y55_apply_enhancement(full_text if 'full_text' in locals() else (text if 'text' in locals() else ''), l_items, clins, dates, pocs, meta, title if 'title' in locals() else '', solnum if 'solnum' in locals() else '')
# X4: persist meta to rfp_meta for combined RFP
                        try:
                            for _k, _v in (meta or {}).items():
                                if _v:
                                    cur.execute("INSERT INTO rfp_meta(rfp_id, key, value) VALUES(?,?,?);", (int(rfp_id), str(_k), str(_v)))
                        except Exception:
                            pass
                        with closing(conn.cursor()) as cur:
                            cur.execute(
                                "INSERT INTO rfps(title, solnum, notice_id, sam_url, file_path, created_at) VALUES (?,?,?,?,?, datetime('now'));",
                                (_guess_title(full_text, title.strip() or "Untitled"), (solnum.strip() or _guess_solnum(full_text)), "", sam_url.strip() or "", "",)
                            )
                            rfp_id = cur.lastrowid
                            for it in l_items:
                                cur.execute("INSERT INTO lm_items(rfp_id, item_text, is_must, status) VALUES (?,?,?,?);",
                                            (rfp_id, it, 1 if re.search(r'\\b(shall|must|required|mandatory|no later than|shall not|will)\\b', it, re.IGNORECASE) else 0, "Open"))
                            for r in clins:
                                cur.execute("INSERT INTO clin_lines(rfp_id, clin, description, qty, unit, unit_price, extended_price) VALUES (?,?,?,?,?,?,?);",
                                            (rfp_id, r.get('clin'), r.get('description'), r.get('qty'), r.get('unit'), r.get('unit_price'), r.get('extended_price')))
                            for d in dates:
                                cur.execute("INSERT INTO key_dates(rfp_id, label, date_text, date_iso) VALUES (?,?,?,?);",
                                            (rfp_id, d.get('label'), d.get('date_text'), d.get('date_iso')))
                            for pc in pocs:
                                cur.execute("INSERT INTO pocs(rfp_id, name, role, email, phone) VALUES (?,?,?,?,?);",
                                            (rfp_id, pc.get('name'), pc.get('role'), pc.get('email'), pc.get('phone')))
                            
                            # X3: store POP / ordering period in meta and key_dates
                            try:
                                _pop = extract_pop_structure(full_text)
                                if _pop:
                                    for k,v in _pop.items():
                                        cur.execute("INSERT INTO rfp_meta(rfp_id, key, value) VALUES(?,?,?);", (int(rfp_id), str(k), str(v)))
                                    if _pop.get('pop_structure'):
                                        cur.execute("INSERT INTO key_dates(rfp_id, label, date_text, date_iso) VALUES (?,?,?,?);",
                                                    (int(rfp_id), 'Ordering/POP', _pop['pop_structure'], ''))
                            except Exception:
                                pass

                            # X3: POP / ordering period from this file's text
                            try:
                                _pop = extract_pop_structure(text)
                                if _pop:
                                    for k,v in _pop.items():
                                        cur.execute("INSERT INTO rfp_meta(rfp_id, key, value) VALUES(?,?,?);", (int(rfp_id), str(k), str(v)))
                                    if _pop.get('pop_structure'):
                                        cur.execute("INSERT INTO key_dates(rfp_id, label, date_text, date_iso) VALUES (?,?,?,?);",
                                                    (int(rfp_id), 'Ordering/POP', _pop['pop_structure'], ''))
                            except Exception:
                                pass
                            conn.commit()
                            # X2: auto-link any pending ingested files to this new RFP
                            try:
                                if st.session_state.get("x1_pending_link_after_create") and st.session_state.get("x1_last_ingested_ids"):
                                    ids = tuple(int(i) for i in st.session_state.get("x1_last_ingested_ids") or [])
                                    if ids:
                                        ph = ",".join(["?"]*len(ids))
                                        with closing(conn.cursor()) as _cur2:
                                            _cur2.execute(f"UPDATE rfp_files SET rfp_id=? WHERE id IN ({ph});", (int(rfp_id), *ids))
                                            conn.commit()
                                    st.session_state["x1_pending_link_after_create"] = False
                            except Exception:
                                pass
                            st.success(f"Combined and saved RFP #{rfp_id} (items: {len(l_items)}, CLINs: {len(clins)}, dates: {len(dates)}, POCs: {len(pocs)}).")
                else:
                    saved = 0
                    for f in ups or []:
                        try:
                            _bytes = f.getbuffer().tobytes()
                        except Exception:
                            try:
                                _bytes = f.read()
                            except Exception:
                                _bytes = b""
                        text = _read_file(type('F', (), {'name': f.name, 'read': lambda self=None: _bytes})())
                        if not text.strip():
                            continue
                        secs = extract_sections_L_M(text)
                        l_items = derive_lm_items(secs.get('L','')) + derive_lm_items(secs.get('M',''))
                        clins = extract_clins(text) + (extract_clins_xlsx(_bytes) if (f.name or '').lower().endswith('.xlsx') else []); dates = extract_dates(text); pocs = extract_pocs(text)
                        meta = {
                            'naics': _extract_naics(text),
                            'set_aside': _extract_set_aside(text),
                            'place_of_performance': _extract_place(text),
                        }
                        
                        
                        l_items, clins, dates, pocs, meta, title, solnum = y55_apply_enhancement(full_text if 'full_text' in locals() else (text if 'text' in locals() else ''), l_items, clins, dates, pocs, meta, title if 'title' in locals() else '', solnum if 'solnum' in locals() else '')
# X4: persist meta to rfp_meta per-file
                        try:
                            for _k, _v in (meta or {}).items():
                                if _v:
                                    cur.execute("INSERT INTO rfp_meta(rfp_id, key, value) VALUES(?,?,?);", (int(rfp_id), str(_k), str(_v)))
                        except Exception:
                            pass
                        with closing(conn.cursor()) as cur:
                            cur.execute(
                                "INSERT INTO rfps(title, solnum, notice_id, sam_url, file_path, created_at) VALUES (?,?,?,?,?, datetime('now'));",
                                (_guess_title(text, f.name), _guess_solnum(text), "", "", "",)
                            )
                            rfp_id = cur.lastrowid
                            for it in l_items:
                                cur.execute("INSERT INTO lm_items(rfp_id, item_text, is_must, status) VALUES (?,?,?,?);",
                                            (rfp_id, it, 1 if re.search(r'\\b(shall|must|required|mandatory|no later than|shall not|will)\\b', it, re.IGNORECASE) else 0, "Open"))
                            for r in clins:
                                cur.execute("INSERT INTO clin_lines(rfp_id, clin, description, qty, unit, unit_price, extended_price) VALUES (?,?,?,?,?,?,?);",
                                            (rfp_id, r.get('clin'), r.get('description'), r.get('qty'), r.get('unit'), r.get('unit_price'), r.get('extended_price')))
                            for d in dates:
                                cur.execute("INSERT INTO key_dates(rfp_id, label, date_text, date_iso) VALUES (?,?,?,?);",
                                            (rfp_id, d.get('label'), d.get('date_text'), d.get('date_iso')))
                            for pc in pocs:
                                cur.execute("INSERT INTO pocs(rfp_id, name, role, email, phone) VALUES (?,?,?,?,?);",
                                            (rfp_id, pc.get('name'), pc.get('role'), pc.get('email'), pc.get('phone')))
                            conn.commit()
                        last_rfp_id = rfp_id
                        saved += 1
                    # X2: if pending, link last created RFP to recently ingested files
                    try:
                        if st.session_state.get("x1_pending_link_after_create") and st.session_state.get("x1_last_ingested_ids") and "last_rfp_id" in locals():
                            ids = tuple(int(i) for i in st.session_state.get("x1_last_ingested_ids") or [])
                            if ids:
                                ph = ",".join(["?"]*len(ids))
                                with closing(conn.cursor()) as _cur3:
                                    _cur3.execute(f"UPDATE rfp_files SET rfp_id=? WHERE id IN ({ph});", (int(last_rfp_id), *ids))
                                    conn.commit()
                            st.session_state["x1_pending_link_after_create"] = False
                    except Exception:
                        pass
                    st.success(f"Saved {saved} RFP record(s).")
    # ---------------- Y1: Ask with citations ----------------
    with tab_y1:
        st.caption("Build a local search index once, then ask CO-style questions with bracketed citations.")
        df_rf_y1 = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
        if df_rf_y1.empty:
            st.info("No RFPs yet. Parse & save first.")
        else:
            rid_y1 = st.selectbox("RFP context", options=df_rf_y1["id"].tolist(),
                                  format_func=lambda i: f"#{i} — {df_rf_y1.loc[df_rf_y1['id']==i,'title'].values[0]}",
                                  key="y1_rfp_sel")
            c1, c2 = st.columns([2,2])
            with c1:
                if st.button("Build/Update search index for this RFP"):
                    with st.spinner("Indexing linked files..."):
                        out = y1_index_rfp(conn, int(rid_y1), rebuild=False)
                    if out.get("ok"):
                        st.success(f"Indexed. Added {out.get('added',0)} chunk(s). Skipped {out.get('skipped',0)} existing.")
                    else:
                        st.error(out.get("error","Index error"))
            with c2:
                if st.button("Rebuild index (overwrite)"):
                    with st.spinner("Rebuilding..."):
                        out = y1_index_rfp(conn, int(rid_y1), rebuild=True)
                    if out.get("ok"):
                        st.success(f"Rebuilt. Added {out.get('added',0)} chunk(s).")
                    else:
                        st.error(out.get("error","Index error"))
            q_y1 = st.text_area("Your question", height=120, key="y1_q")
            k = y_auto_k(q_y1)
            if st.button("Ask with citations", type="primary"):
                if not (q_y1 or "").strip():
                    st.warning("Enter a question")
                else:
                    ph = st.empty(); acc = []
                    for tok in ask_ai_with_citations(conn, int(rid_y1), q_y1.strip(), k=int(k)):
                        acc.append(tok)
                        ph.markdown("".join(acc))
                    hits = y1_search(conn, int(rid_y1), q_y1.strip(), k=int(k))
                    if hits:
                        import pandas as _pd
                        dfh = _pd.DataFrame([{"Tag": f"[C{i+1}]", "File": h["file"], "Page": h["page"], "Score": h["score"]} for i,h in enumerate(hits)])
                        st.subheader("Sources used")
                        st.dataframe(dfh, use_container_width=True, hide_index=True)



    
    # ---------------- Y2: CO Chat with memory ----------------
    with tab_y2:
        y2_ui_threaded_chat(conn)

    # ---------------- Y4: CO Review UI ----------------
    with tab_y4:
        y4_ui_review(conn)
# ---------------- CHECKLIST ----------------
    with tab_checklist:
        df_rf = pd.read_sql_query("SELECT id, title, solnum FROM rfps ORDER BY id DESC;", conn, params=())
        if df_rf.empty:
            st.info("No RFPs yet. Parse one on the first tab.")
        else:
            rid = st.selectbox("Select an RFP", options=df_rf['id'].tolist(), format_func=lambda i: f"#{i} — {df_rf.loc[df_rf['id']==i,'title'].values[0]}", key="rfp_sel")
            df_lm = pd.read_sql_query("SELECT id, item_text, is_must, status FROM lm_items WHERE rfp_id=? ORDER BY id;", conn, params=(int(rid),))

        with st.expander("Q&A Memory (X7)", expanded=False):
            st.caption("Ask questions about this RFP. Answers are pulled from saved checklists, CLINs, dates, POCs, and linked file text. History is saved.")
            q = st.text_input("Your question", key="x7_q")
            ask = st.button("Ask (store)", key="x7_ask")
            if ask and (q or "").strip():
                try:
                    ql = (q or "").lower()
                    rid_safe = locals().get("rid", None)
                    if rid_safe is None:
                        st.warning("Select an RFP in the Data tab above first."); raise Exception("No rid in scope")
                    res = _kb_search(conn, int(rid_safe), q or "")
                    ans_parts = []
                    if any(w in ql for w in ["due","deadline","close"]):
                        df = res.get("dates")
                        if df is not None and not df.empty:
                            top = df.iloc[0]
                            ans_parts.append(f"Due: {top.get('date_text','')} ({top.get('label','')})")
                    if any(w in ql for w in ["poc","contact","officer","specialist"]):
                        df = res.get("pocs")
                        if df is not None and not df.empty:
                            s = "; ".join([f"{r.get('name','')} ({r.get('email','')})" for _, r in df.head(3).iterrows()])
                            if s: ans_parts.append("POCs: " + s)
                    if "clin" in ql:
                        df = res.get("clins")
                        if df is not None and not df.empty:
                            ans_parts.append(f"CLINs detected: {len(df)}; first: {df.iloc[0].get('clin','')}")
                    if any(w in ql for w in ["checklist","compliance","shall","must"]):
                        df = res.get("checklist")
                        if df is not None and not df.empty:
                            open_cnt = int((df['status']!='Complete').sum())
                            ans_parts.append(f"Checklist items: {len(df)}. Open: {open_cnt}.")
                    if not ans_parts:
                        sec = res.get("sections")
                        if sec is not None and not sec.empty:
                            snip = (sec.iloc[0].get("content","") or "").strip().replace("\n"," ")
                            ans_parts.append("Top section snippet: " + snip[:300])
                    a = " | ".join(ans_parts) if ans_parts else "No direct answer found in saved data."
                    from contextlib import closing as _closing_x7
                    with _closing_x7(conn.cursor()) as cur:
                        cur.execute("INSERT INTO rfp_chat(rfp_id, ts, q, a) VALUES(?,?,?,?);",
                                    (int(rid), datetime.utcnow().isoformat(), q.strip(), a))
                        conn.commit()
                    st.success("Stored Q&A.")
                except Exception as e:
                    st.error(f"Q&A failed: {e}")
            try:
                df_hist = pd.read_sql_query("SELECT ts, q, a FROM rfp_chat WHERE rfp_id=? ORDER BY id DESC LIMIT 50;", conn, params=(int(locals().get("rid")),))
                if df_hist is not None and not df_hist.empty:
                    st.dataframe(df_hist, use_container_width=True, hide_index=True)
                    c1, c2 = st.columns([1,1])
                    with c1:
                        if st.button("Export Q&A CSV", key="x7_export"):
                            csvb = df_hist.to_csv(index=False).encode("utf-8")
                            st.download_button("Download Q&A CSV", data=csvb, file_name=f"rfp_{int(rid)}_qa.csv", mime="text/csv", key="x7_dl")
                    with c2:
                        if st.button("Clear history", key="x7_clear"):
                            try:
                                with closing(conn.cursor()) as cur:
                                    cur.execute("DELETE FROM rfp_chat WHERE rfp_id=?;", (int(rid),))
                                    conn.commit()
                                st.success("Cleared.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Clear failed: {e}")
                else:
                    st.caption("No Q&A yet for this RFP.")
            except Exception as e:
                st.info(f"No history available: {e}")
            # X7 guard: ensure checklist dataframe is defined and rid is available
            _rid = locals().get("rid", None)
            if _rid is None:
                st.caption("Checklist viewer: select an RFP above to load items.")
            else:
                try:
                    df_lm = pd.read_sql_query(
                        "SELECT id, item_text, is_must, status FROM lm_items WHERE rfp_id=? ORDER BY id;",
                        conn, params=(int(_rid),)
                    )

                    with st.expander("Add to Proposal Drafts", expanded=False):
                        sec = st.text_input("Section label", value="Research Notes", key="y5_sec_y1")
                        ans_txt = "".join(acc).strip()
                        if st.button("Add to drafts", key="y5_add_y1"):
                            y5_save_snippet(conn, int(rid_y1), sec, ans_txt or q_y1.strip(), source="Y1 Q&A")
                            st.success("Saved to drafts")
                except Exception:
                    df_lm = pd.DataFrame(columns=['id','item_text','is_must','status'])
                df_lm = df_lm.fillna("")
                st.caption(f"{len(df_lm)} checklist items")
                st.dataframe(df_lm, use_container_width=True, hide_index=True)
                new_status = st.selectbox("Set status for selected IDs", ["Open","In Progress","Complete","N/A"], index=0, key="lm_set_status")
                sel_ids = st.text_input("IDs to update (comma-separated)", key="lm_ids")
                if st.button("Update Status", key="lm_status_btn"):
                    ids = [int(x) for x in sel_ids.split(",") if x.strip().isdigit()]
                    if ids:
                        with closing(conn.cursor()) as cur:
                            cur.executemany("UPDATE lm_items SET status=? WHERE id=? AND rfp_id=?;", [(new_status, iid, int(_rid)) for iid in ids])
                            conn.commit()
                    st.success(f"Updated {len(ids)} item(s).")
                    st.rerun()
            # Export
            if st.button("Export Compliance Matrix (CSV)", key="lm_export_csv"):
                out = df_lm.copy()
                out.insert(0, "rfp_id", int(rid))
                csv_bytes = out.to_csv(index=False).encode("utf-8")
                st.download_button("Download CSV", data=csv_bytes, file_name=f"rfp_{rid}_compliance.csv", mime="text/csv", key="lm_dl")

    # ---------------- CLINs / Dates / POCs ----------------
    with tab_data:
        # X2: Files for this RFP
        with st.expander("Files for this RFP (X2)", expanded=False):
            try:
                df_files = pd.read_sql_query("SELECT id, filename, mime, pages, sha256 FROM rfp_files WHERE rfp_id=? ORDER BY id DESC;", conn, params=(int(rid),))
            except Exception as e:
                df_files = pd.DataFrame()
            st.caption("Linked files")
            if df_files is None or df_files.empty:
                st.write("No files linked.")
            else:
                st.dataframe(df_files.assign(sha=df_files["sha256"].str.slice(0,12)).drop(columns=["sha256"]), use_container_width=True, hide_index=True)
                # X5: preview and download selected linked file
                try:
                    pick = st.selectbox(
                        "Open file",
                        options=df_files["id"].tolist(),
                        format_func=lambda i: f"#{i} — {df_files.loc[df_files['id']==i,'filename'].values[0]}",
                        key=f"file_pick_{rid}"
                    )
                    if pick:
                        row = pd.read_sql_query(
                            "SELECT filename, mime, bytes FROM rfp_files WHERE id=?;",
                            conn, params=(int(pick),)
                        ).iloc[0]
                        fname = row.get("filename") or f"rfp_file_{int(pick)}"
                        mime = row.get("mime") or "application/octet-stream"
                        b = row.get("bytes")
                        st.download_button("Download original", data=b, file_name=fname, mime=mime, key=f"dl_{pick}")
                        try:
                            pages = extract_text_pages(b, mime)
                            preview = ("\n\n".join(pages) if pages else "").strip()[:20000]
                            if preview:
                                st.text_area("Preview (first 20k chars)", value=preview, height=300)
                        except Exception as e:
                            st.info(f"Preview unavailable: {e}")
                except Exception as e:
                    st.info(f"No preview available: {e}")

                # X6: bulk ZIP download, inventory CSV, and simple search across linked files
                import io as _io, zipfile as _zipfile
                c_zip, c_inv = st.columns([1,1])
                with c_zip:
                    if st.button("Download all linked files as ZIP", key=f"zip_all_{rid}"):
                        try:
                            # Fetch bytes for all linked files
                            df_bytes = pd.read_sql_query("SELECT filename, bytes, mime FROM rfp_files WHERE rfp_id=?;", conn, params=(int(rid),))
                            if df_bytes is None or df_bytes.empty:
                                st.warning("No files to package.")
                            else:
                                buf = _io.BytesIO()
                                with _zipfile.ZipFile(buf, mode="w", compression=_zipfile.ZIP_DEFLATED) as zf:
                                    for _, r in df_bytes.iterrows():
                                        fname = (r.get("filename") or f"file_{_}.bin")
                                        # Avoid directory traversal
                                        fname = fname.replace("..","").replace("\\","/").split("/")[-1]
                                        b = r.get("bytes")
                                        if isinstance(b, (bytes, bytearray)):
                                            zf.writestr(fname, b)
                                st.download_button(
                                    "Download ZIP",
                                    data=buf.getvalue(),
                                    file_name=f"rfp_{int(rid)}_linked_files.zip",
                                    mime="application/zip",
                                    key=f"zip_dl_{rid}"
                                )
                        except Exception as e:
                            st.error(f"ZIP build failed: {e}")
                with c_inv:
                    try:
                        inv = df_files.copy()
                        inv["sha_short"] = inv["sha256"].str.slice(0,12)
                        csvb = inv[["id","filename","mime","pages","sha_short"]].to_csv(index=False).encode("utf-8")
                        st.download_button("Export file inventory CSV", data=csvb, file_name=f"rfp_{int(rid)}_file_inventory.csv", mime="text/csv", key=f"inv_{rid}")
                    except Exception as e:
                        st.info(f"Inventory export unavailable: {e}")

                with st.expander("Find in linked files (simple)", expanded=False):
                    q = st.text_input("Search phrase", key=f"find_files_{rid}")
                    if q:
                        try:
                            hits = []
                            pool = pd.read_sql_query("SELECT id, filename, mime, bytes FROM rfp_files WHERE rfp_id=? ORDER BY id DESC;", conn, params=(int(rid),))
                            for _, r in pool.iterrows():
                                b = r.get("bytes"); mime = r.get("mime") or ""
                                pages = extract_text_pages(b, mime)
                                text = ("\n\n".join(pages) if pages else "")
                                pos = text.lower().find(q.lower())
                                if pos >= 0:
                                    start = max(0, pos-120); end = min(len(text), pos+120)
                                    ctx = text[start:end].replace("\n"," ")
                                    hits.append({"file_id": int(r["id"]), "filename": r.get("filename"), "snippet": ctx})
                            if hits:
                                dfh = pd.DataFrame(hits)
                                st.dataframe(dfh, use_container_width=True, hide_index=True)
                            else:
                                st.write("No hits.")
                        except Exception as e:
                            st.info(f"No search results: {e}")

                to_unlink = st.multiselect("Unlink file IDs", options=df_files["id"].tolist(), key=f"unlink_{rid}")
                if st.button("Unlink selected", key=f"unlink_btn_{rid}") and to_unlink:
                    try:
                        ph = ",".join(["?"]*len(to_unlink))
                        with closing(conn.cursor()) as _cur:
                            _cur.execute(f"UPDATE rfp_files SET rfp_id=NULL WHERE id IN ({ph});", tuple(int(i) for i in to_unlink))
                            conn.commit()
                        st.success(f"Unlinked {len(to_unlink)} file(s)."); st.rerun()
                    except Exception as e:
                        st.error(f"Unlink failed: {e}")
            st.caption("Attach from library")
            try:
                df_pool = pd.read_sql_query("SELECT id, filename, mime, pages FROM rfp_files WHERE rfp_id IS NULL ORDER BY id DESC LIMIT 500;", conn, params=())
            except Exception:
                df_pool = pd.DataFrame()
            if df_pool is None or df_pool.empty:
                st.write("No unlinked files in library.")
            else:
                st.dataframe(df_pool, use_container_width=True, hide_index=True)
                to_link = st.multiselect("Attach file IDs", options=df_pool["id"].tolist(), key=f"link_{rid}")
                if st.button("Attach selected to this RFP", key=f"link_btn_{rid}") and to_link:
                    try:
                        ph = ",".join(["?"]*len(to_link))
                        with closing(conn.cursor()) as _cur2:
                            _cur2.execute(f"UPDATE rfp_files SET rfp_id=? WHERE id IN ({ph});", (int(rid), *[int(i) for i in to_link]))
                            conn.commit()
                        st.success(f"Linked {len(to_link)} file(s)."); st.rerun()
                    except Exception as e:
                        st.error(f"Link failed: {e}")
        df_rf = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
        if df_rf.empty:
            st.info("No RFPs yet.")
            return
        rid = st.selectbox(
            "RFP for data views",
            options=df_rf["id"].tolist(),
            format_func=lambda i: f"#{i} — {df_rf.loc[df_rf['id']==i, 'title'].values[0]}",
            key="rfp_data_sel"
        )
        
        
        with st.expander("Acquisition Meta (X4)", expanded=False):
            try:
                df_meta_all = pd.read_sql_query("SELECT key, value FROM rfp_meta WHERE rfp_id=?;", conn, params=(int(rid),))
            except Exception:
                df_meta_all = pd.DataFrame(columns=['key','value'])
            if df_meta_all is None or df_meta_all.empty:
                st.write("No meta extracted yet.")
            else:
                # Highlight common fields
                want = ['naics','set_aside','place_of_performance','pop_structure','ordering_period_years','base_months']
                show = df_meta_all[df_meta_all["key"].isin(want)]
                if show.empty:
                    st.dataframe(df_meta_all, use_container_width=True, hide_index=True)
                else:
                    st.dataframe(show, use_container_width=True, hide_index=True)

        with st.expander("Ordering / POP (X3)", expanded=False):
            try:
                df_meta = pd.read_sql_query("SELECT key, value FROM rfp_meta WHERE rfp_id=?;", conn, params=(int(rid),))
            except Exception:
                df_meta = pd.DataFrame(columns=['key','value'])
            if df_meta is None or df_meta.empty:
                st.write("No POP metadata yet.")
            else:
                pop = df_meta[df_meta["key"].isin(["pop_structure","ordering_period_years","base_months"])]
                if pop.empty:
                    st.dataframe(df_meta, use_container_width=True, hide_index=True)
                else:
                    st.dataframe(pop, use_container_width=True, hide_index=True)

        # === Phase S: Manual Editors (LM / CLINs / Dates / POCs / Meta) ===

        import pandas as _pd
        from contextlib import closing as _closing_ed
        with st.expander('Manual Editors', expanded=False):
            tab_lm, tab_clin, tab_dates, tab_pocs, tab_meta = st.tabs(['L/M Items','CLINs','Key Dates','POCs','Meta'])
            with tab_lm:
                try:
                    df_lm_e = _pd.read_sql_query('SELECT item_text, is_must, status FROM lm_items WHERE rfp_id=? ORDER BY id;', conn, params=(int(rid),))
                except Exception:
                    df_lm_e = _pd.DataFrame(columns=['item_text','is_must','status'])
                df_lm_e = df_lm_e.fillna('')
                ed_lm = st.data_editor(df_lm_e, num_rows='dynamic', use_container_width=True, key=f'ed_lm_{rid}')
                if st.button('Save L/M', key=f'save_lm_{rid}'):
                    with _closing_ed(conn.cursor()) as cur:
                        cur.execute('DELETE FROM lm_items WHERE rfp_id=?;', (int(rid),))
                        for _, r in ed_lm.fillna('').iterrows():
                            txt = str(r.get('item_text','')).strip()
                            if not txt: continue
                            cur.execute('INSERT INTO lm_items(rfp_id, item_text, is_must, status) VALUES (?,?,?,?);', (int(rid), txt, int(r.get('is_must') or 0), str(r.get('status') or 'Open')))
                        conn.commit()
                    st.success('L/M saved.')
            with tab_clin:
                try:
                    df_c_e = _pd.read_sql_query('SELECT clin, description, qty, unit, unit_price, extended_price FROM clin_lines WHERE rfp_id=?;', conn, params=(int(rid),))
                except Exception:
                    df_c_e = _pd.DataFrame(columns=['clin','description','qty','unit','unit_price','extended_price'])
                df_c_e = df_c_e.fillna('')
                ed_c = st.data_editor(df_c_e, num_rows='dynamic', use_container_width=True, key=f'ed_clin_{rid}')
                if st.button('Save CLINs', key=f'save_clin_{rid}'):
                    with _closing_ed(conn.cursor()) as cur:
                        cur.execute('DELETE FROM clin_lines WHERE rfp_id=?;', (int(rid),))
                        for _, r in ed_c.fillna('').iterrows():
                            if not any(str(r.get(col,'')).strip() for col in ['clin','description','qty','unit','unit_price','extended_price']):
                                continue
                            cur.execute('INSERT INTO clin_lines(rfp_id, clin, description, qty, unit, unit_price, extended_price) VALUES (?,?,?,?,?,?,?);', (int(rid), str(r.get('clin','')), str(r.get('description','')), str(r.get('qty','')), str(r.get('unit','')), str(r.get('unit_price','')), str(r.get('extended_price',''))))
                        conn.commit()
                    st.success('CLINs saved.')
            with tab_dates:
                try:
                    df_d_e = _pd.read_sql_query('SELECT label, date_text, date_iso FROM key_dates WHERE rfp_id=?;', conn, params=(int(rid),))
                except Exception:
                    df_d_e = _pd.DataFrame(columns=['label','date_text','date_iso'])
                df_d_e = df_d_e.fillna('')
                ed_d = st.data_editor(df_d_e, num_rows='dynamic', use_container_width=True, key=f'ed_dates_{rid}')
                if st.button('Save Dates', key=f'save_dates_{rid}'):
                    with _closing_ed(conn.cursor()) as cur:
                        cur.execute('DELETE FROM key_dates WHERE rfp_id=?;', (int(rid),))
                        for _, r in ed_d.fillna('').iterrows():
                            if not any(str(r.get(col,'')).strip() for col in ['label','date_text','date_iso']):
                                continue
                            cur.execute('INSERT INTO key_dates(rfp_id, label, date_text, date_iso) VALUES (?,?,?,?);', (int(rid), str(r.get('label','')), str(r.get('date_text','')), str(r.get('date_iso',''))))
                        conn.commit()
                    st.success('Dates saved.')
            with tab_pocs:
                try:
                    df_p_e = _pd.read_sql_query('SELECT name, role, email, phone FROM pocs WHERE rfp_id=?;', conn, params=(int(rid),))
                except Exception:
                    df_p_e = _pd.DataFrame(columns=['name','role','email','phone'])
                df_p_e = df_p_e.fillna('')
                ed_p = st.data_editor(df_p_e, num_rows='dynamic', use_container_width=True, key=f'ed_pocs_{rid}')
                if st.button('Save POCs', key=f'save_pocs_{rid}'):
                    with _closing_ed(conn.cursor()) as cur:
                        cur.execute('DELETE FROM pocs WHERE rfp_id=?;', (int(rid),))
                        for _, r in ed_p.fillna('').iterrows():
                            if not any(str(r.get(col,'')).strip() for col in ['name','role','email','phone']):
                                continue
                            cur.execute('INSERT INTO pocs(rfp_id, name, role, email, phone) VALUES (?,?,?,?,?);', (int(rid), str(r.get('name','')), str(r.get('role','')), str(r.get('email','')), str(r.get('phone',''))))
                        conn.commit()
                    st.success('POCs saved.')
            with tab_meta:
                try:
                    df_m_e = _pd.read_sql_query('SELECT key, value FROM rfp_meta WHERE rfp_id=?;', conn, params=(int(rid),))
                except Exception:
                    df_m_e = _pd.DataFrame(columns=['key','value'])
                df_m_e = df_m_e.fillna('')
                ed_m = st.data_editor(df_m_e, num_rows='dynamic', use_container_width=True, key=f'ed_meta_{rid}')
                if st.button('Save Meta', key=f'save_meta_{rid}'):
                    with _closing_ed(conn.cursor()) as cur:
                        cur.execute('DELETE FROM rfp_meta WHERE rfp_id=?;', (int(rid),))
                        for _, r in ed_m.fillna('').iterrows():
                            k = str(r.get('key','')).strip(); v = str(r.get('value','')).strip()
                            if not k and not v: continue
                            cur.execute('INSERT INTO rfp_meta(rfp_id, key, value) VALUES (?,?,?);', (int(rid), k, v))
                        conn.commit()
                    st.success('Meta saved.')
        # === End Phase S ===
        col1, col2, col3 = st.columns(3)
        with col1:
            df_c = pd.read_sql_query("SELECT clin, description, qty, unit, unit_price, extended_price FROM clin_lines WHERE rfp_id=?;", conn, params=(int(rid),))
            st.subheader("CLINs"); st.dataframe(df_c, use_container_width=True, hide_index=True)
        with col2:
            df_d = pd.read_sql_query("SELECT label, date_text, date_iso FROM key_dates WHERE rfp_id=?;", conn, params=(int(rid),))
            st.subheader("Key Dates"); st.dataframe(df_d, use_container_width=True, hide_index=True)
        with col3:
            df_p = pd.read_sql_query("SELECT name, role, email, phone FROM pocs WHERE rfp_id=?;", conn, params=(int(rid),))
            st.subheader("POCs"); st.dataframe(df_p, use_container_width=True, hide_index=True)
        st.subheader("Attributes")
        df_meta = pd.read_sql_query("SELECT key, value FROM rfp_meta WHERE rfp_id=?;", conn, params=(int(rid),))
        st.dataframe(df_meta, use_container_width=True, hide_index=True)

def _compliance_progress(df_items: pd.DataFrame) -> int:
    if df_items is None or df_items.empty:
        return 0
    done = int((df_items["status"]=="Complete").sum())
    total = int(len(df_items))
    return int(round(done / max(1, total) * 100))



def _load_compliance_matrix(conn: sqlite3.Connection, rfp_id: int) -> pd.DataFrame:
    """
    Robust loader:
      1) If tenancy views exist (lm_items_t/lm_meta_t), use them.
      2) Else if base tables exist (lm_items/lm_meta), use them.
      3) Else return lm_items-only with blank meta columns.
    """
    # Ensure lm_meta exists (no-op if already there)
    try:
        with closing(conn.cursor()) as c:
            c.execute("CREATE TABLE IF NOT EXISTS lm_meta(\n"
                      " id INTEGER PRIMARY KEY,\n"
                      " lm_id INTEGER REFERENCES lm_items(id) ON DELETE CASCADE,\n"
                      " owner TEXT, ref_page TEXT, ref_para TEXT, evidence TEXT, risk TEXT, notes TEXT\n"
                      ");")
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_lm_meta_lm ON lm_meta(lm_id);")
            conn.commit()
    except Exception:
        pass

    def _has(name: str) -> bool:
        try:
            q = "SELECT name FROM sqlite_master WHERE name=?;"
            return pd.read_sql_query(q, conn, params=(name,)).shape[0] > 0
        except Exception:
            return False

    use_views = _has("lm_items_t")
    use_meta_view = _has("lm_meta_t")

    if use_views and use_meta_view:
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
            ORDER BY i.id;
        """
        try:
            return pd.read_sql_query(q, conn, params=(rfp_id,))
        except Exception:
            pass  # fall through

    # Base tables path (works even if lm_meta is empty)
    if _has("lm_items"):
        # Only join lm_meta if it truly exists (older DBs may lack it)
        if _has("lm_meta"):
            q = """
                SELECT i.id AS lm_id, i.item_text, i.is_must, i.status,
                       COALESCE(m.owner,'') AS owner,
                       COALESCE(m.ref_page,'') AS ref_page,
                       COALESCE(m.ref_para,'') AS ref_para,
                       COALESCE(m.evidence,'') AS evidence,
                       COALESCE(m.risk,'Green') AS risk,
                       COALESCE(m.notes,'') AS notes
                FROM lm_items i
                LEFT JOIN lm_meta m ON m.lm_id = i.id
                WHERE i.rfp_id = ?
                ORDER BY i.id;
            """
        else:
            q = """
                SELECT i.id AS lm_id, i.item_text, i.is_must, i.status,
                       '' AS owner, '' AS ref_page, '' AS ref_para, '' AS evidence, 'Green' AS risk, '' AS notes
                FROM lm_items i
                WHERE i.rfp_id = ?
                ORDER BY i.id;
            """
        try:
            return pd.read_sql_query(q, conn, params=(rfp_id,))
        except Exception:
            pass

    # Final fallback: empty frame with expected columns
    cols = ["lm_id","item_text","is_must","status","owner","ref_page","ref_para","evidence","risk","notes"]
    return pd.DataFrame(columns=cols)

def _compliance_flags(ctx: dict, df_items: pd.DataFrame) -> pd.DataFrame:
    rows = []
    sections = ctx.get("sections", pd.DataFrame())
    text_all = " ".join((sections["content"].tolist() if isinstance(sections, pd.DataFrame) and not sections.empty else []))
    tl = text_all.lower()

    m = re.search(r'(?:page\s+limit|not\s+exceed)\s+(?:of\s+)?(\d{1,3})\s+pages?', tl)
    if m: rows.append({"Rule":"Page Limit","Detail":f"Limit {m.group(1)} pages detected","Severity":"Amber"})
    if re.search(r'(font|typeface).{0,20}(size|pt).{0,5}(10|11)', tl):
        rows.append({"Rule":"Font size","Detail":"Minimum font size 10/11pt likely required","Severity":"Amber"})
    if re.search(r'margin[s]?\s+(?:of|at\s+least)\s+\d', tl):
        rows.append({"Rule":"Margins","Detail":"Specific margin requirements detected","Severity":"Amber"})
    if re.search(r'volume[s]?\s+(i{1,3}|iv|v|technical|price)', tl):
        rows.append({"Rule":"Volumes","Detail":"Multiple volumes required","Severity":"Amber"})
    if re.search(r'(sam\.gov|piee|wawf|email submission|portal)', tl):
        rows.append({"Rule":"Submission portal","Detail":"Specific portal/email submission detected","Severity":"Amber"})

    dates = ctx.get("dates", pd.DataFrame())
    if isinstance(dates, pd.DataFrame) and not dates.empty:
        due = dates[dates["label"].str.contains("due", case=False, na=False)]
        if not due.empty:
            dt = pd.to_datetime(due.iloc[0]["date_text"], errors="coerce")
            if pd.notnull(dt):
                days = (pd.Timestamp(dt) - pd.Timestamp.utcnow()).days
                if days <= 3: rows.append({"Rule":"Timeline","Detail":f"Proposals due in {days} day(s)","Severity":"Red"})
                elif days <= 7: rows.append({"Rule":"Timeline","Detail":f"Proposals due in {days} days","Severity":"Amber"})

    if isinstance(df_items, pd.DataFrame) and not df_items.empty:
        open_musts = df_items[(df_items["is_must"]==1) & (df_items["status"]!="Complete")]
        if not open_musts.empty:
            rows.append({"Rule":"Open MUST items","Detail":f"{len(open_musts)} mandatory items still open","Severity":"Red"})

    return pd.DataFrame(rows)


def _load_rfp_context(conn: sqlite3.Connection, rfp_id: int) -> dict:
    try:
        rf = pd.read_sql_query("SELECT id, title, solnum, sam_url, created_at FROM rfps WHERE id=?;", conn, params=(int(rfp_id),))
    except Exception:
        rf = pd.DataFrame()
    try:
        df_items = pd.read_sql_query("SELECT id, item_text, is_must, status FROM lm_items WHERE rfp_id=? ORDER BY id;", conn, params=(int(rfp_id),))
    except Exception:
        df_items = pd.DataFrame(columns=["id","item_text","is_must","status"])
    joined = "\n".join(df_items["item_text"].astype(str).tolist()) if not df_items.empty else ""
    sections = pd.DataFrame([{"name":"Checklist Items","content": joined}])
    meta = rf.iloc[0].to_dict() if not rf.empty else {}
    return {"rfp": meta, "sections": sections, "items": df_items}


def run_lm_checklist(conn: sqlite3.Connection) -> None:

    st.header("L and M Checklist")
    rfp_id = st.session_state.get('current_rfp_id')
    if not rfp_id:
        try:
            df_rf = pd.read_sql_query("SELECT id, title, solnum, created_at FROM rfps_t ORDER BY id DESC;", conn, params=())
        except Exception as e:
            st.error(f"Failed to load RFPs: {e}")
            return
        if df_rf.empty:
            st.info("No saved RFP extractions yet. Use RFP Analyzer to parse and save.")
            return
        opt = st.selectbox("Select an RFP context", options=df_rf['id'].tolist(),
                           format_func=lambda rid: f"#{rid} — {df_rf.loc[df_rf['id']==rid,'title'].values[0] or 'Untitled'}")
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

    pct = _compliance_progress(df_items)
    st.progress(pct/100.0, text=f"{pct}% complete")

    c1, c2, c3 = st.columns([2,2,2])
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
    with c3:
        if st.button("Mark all MUST to Open"):
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute("UPDATE lm_items SET status='Open' WHERE rfp_id=? AND is_must=1;", (rfp_id,))
                    conn.commit()
                st.success("All MUST items set to Open")
            except Exception as e:
                st.error(f"Update failed: {e}")

    st.subheader("Checklist")
    for _, row in df_items.iterrows():
        key = f"lm_{row['id']}"
        label = ("[MUST] " if row['is_must']==1 else "") + row['item_text']
        checked = st.checkbox(label, value=(row['status']=='Complete'), key=key)
        new_status = 'Complete' if checked else 'Open'
        if new_status != row['status']:
            try:
                with closing(conn.cursor()) as cur:
                    cur.execute("UPDATE lm_items SET status=? WHERE id=?;", (new_status, int(row['id'])))
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
        "item_text":"Requirement","is_must":"Must?","status":"Status",
        "owner":"Owner","ref_page":"Page","ref_para":"Para",
        "evidence":"Evidence/Link","risk":"Risk","notes":"Notes"
    })
    st.dataframe(view[["Requirement","Must?","Status","Owner","Page","Para","Evidence/Link","Risk","Notes"]],
                 use_container_width=True, hide_index=True)

    st.markdown("**Edit selected requirement**")
    pick = st.selectbox("Requirement", options=df_mx["lm_id"].tolist(),
                        format_func=lambda lid: f"#{lid} — {df_mx.loc[df_mx['lm_id']==lid,'item_text'].values[0][:80]}")

    rec = df_mx[df_mx["lm_id"]==pick].iloc[0].to_dict()
    e1, e2, e3, e4 = st.columns([2,1,1,1])
    with e1:
        owner = st.text_input("Owner", value=rec.get("owner",""), key=f"mx_owner_{pick}")
        notes = st.text_area("Notes", value=rec.get("notes",""), key=f"mx_notes_{pick}", height=90)
    with e2:
        page = st.text_input("Page", value=rec.get("ref_page",""), key=f"mx_page_{pick}")
        para = st.text_input("Paragraph", value=rec.get("ref_para",""), key=f"mx_para_{pick}")
    with e3:
        risk = st.selectbox("Risk", ["Green","Yellow","Red"],
                            index=["Green","Yellow","Red"].index(rec.get("risk","Green")), key=f"mx_risk_{pick}")
    with e4:
        evidence = st.text_input("Evidence/Link", value=rec.get("evidence",""), key=f"mx_evid_{pick}")

    csave, cexp = st.columns([2,2])
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
                st.success("Saved"); st.rerun()
            except Exception as e2:
                st.error(f"Save failed: {e2}")
    with cexp:
        if st.button("Export Matrix CSV", key="mx_export"):
            out = view.copy()
            path = str(Path(DATA_DIR) / f"compliance_matrix_rfp_{int(rfp_id)}.csv")
            out.to_csv(path, index=False)
            st.success("Exported"); st.markdown(f"[Download CSV]({path})")

    st.subheader("Red-Flag Finder")
    ctx = _load_rfp_context(conn, int(rfp_id))
    flags = _compliance_flags(ctx, df_items)
    if flags is None or flags.empty:
        st.write("No obvious flags detected.")
    else:
        st.dataframe(flags, use_container_width=True, hide_index=True)
    



def _estimate_pages(total_words: int, spacing: str = "1.15", words_per_page: Optional[int] = None) -> float:
    """Rough page estimate at common spacings for 11pt fonts."""
    if words_per_page is None:
        s = (spacing or "1.15").strip().lower()
        if s in {"1", "1.0", "single"}:
            wpp = 500
        elif s in {"1.15", "1,15"}:
            wpp = 400
        elif s in {"1.5", "1,5"}:
            wpp = 300
        elif s in {"double", "2", "2.0"}:
            wpp = 250
        else:
            wpp = 400
    else:
        wpp = max(50, int(words_per_page))
    return round((total_words or 0) / float(wpp), 2)


def _export_docx(path: str, doc_title: str, sections: List[dict], clins: Optional[pd.DataFrame] = None, checklist: Optional[pd.DataFrame] = None, metadata: Optional[dict] = None,
                 font_name: str = "Times New Roman",
                 font_size_pt: int = 11,
                 spacing: str = "1.15") -> Optional[str]:
    try:
        from docx import Document  # type: ignore
        from docx.shared import Pt  # type: ignore
        from docx.enum.text import WD_LINE_SPACING  # type: ignore
    except Exception:
        st.error("python-docx is required. pip install python-docx")
        return None
    spacing_map = {
        "single": WD_LINE_SPACING.SINGLE, "1": WD_LINE_SPACING.SINGLE, "1.0": WD_LINE_SPACING.SINGLE,
        "1.15": WD_LINE_SPACING.ONE_POINT_FIVE, "1,15": WD_LINE_SPACING.ONE_POINT_FIVE,
        "1.5": WD_LINE_SPACING.ONE_POINT_FIVE, "double": WD_LINE_SPACING.DOUBLE,
        "2": WD_LINE_SPACING.DOUBLE, "2.0": WD_LINE_SPACING.DOUBLE,
    }
    line_spacing = spacing_map.get((spacing or "1.15").lower(), WD_LINE_SPACING.ONE_POINT_FIVE)
    doc = Document()
    h = doc.add_heading(doc_title or "Proposal", level=1)
    if metadata:
        p = doc.add_paragraph(" | ".join(f"{k}: {v}" for k,v in metadata.items()))
    for s in (sections or []):
        title = str(s.get("title","")).strip()
        body = str(s.get("body","")).strip()
        if title: doc.add_heading(title, level=2)
        for para in body.split("\n\n"):
            if para.strip():
                p = doc.add_paragraph(para.strip())
                try: p.paragraph_format.line_spacing_rule = line_spacing
                except Exception: pass
                for run in p.runs:
                    try: run.font.name = font_name; run.font.size = Pt(font_size_pt)
                    except Exception: pass
    try:
        if isinstance(clins, pd.DataFrame) and not clins.empty:
            tbl = doc.add_table(rows=1, cols=len(clins.columns))
            for j, col in enumerate(clins.columns): tbl.rows[0].cells[j].text = str(col)
            for _, row in clins.iterrows():
                cells = tbl.add_row().cells
                for j, col in enumerate(clins.columns):
                    val = row.get(col); cells[j].text = "" if pd.isna(val) else str(val)
    except Exception: pass
    try:
        if isinstance(checklist, pd.DataFrame) and not checklist.empty:
            doc.add_heading("Compliance Checklist", level=2)
            for _, r in checklist.iterrows():
                txt = str(r.get("item_text","")).strip()
                if txt: doc.add_paragraph(txt, style="List Bullet")
    except Exception: pass
    doc.save(path)
    return path

def run_proposal_builder(conn: sqlite3.Connection) -> None:
    st.header("Proposal Builder")
    df_rf = pd.read_sql_query("SELECT id, title, solnum, notice_id FROM rfps_t ORDER BY id DESC;", conn, params=())
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
            st.markdown(f"**{sec}**")
            notes = st.text_input(f"Notes for {sec}", key=f"y3_notes_{sec}")
            cA, cB, cC = st.columns([1,1,1])
            with cA:
                k = y_auto_k(f"{sec} {notes}")
            with cB:
                maxw = st.number_input(f"Max words — {sec}", min_value=0, value=220, step=10, key=f"y3_maxw_{sec}")
            with cC:
                if st.button(f"Draft {sec}", key=f"y3_draft_{sec}"):
                    ph = st.empty(); acc=[]
                    for tok in y3_stream_draft(conn, int(rfp_id), sec, notes or "", k=int(k), max_words=int(maxw) if maxw>0 else None):
                        acc.append(tok); ph.markdown("".join(acc))
                    drafted = "".join(acc).strip()
                    if drafted:
                        st.session_state[f"pb_section_{sec}"] = drafted
                        default_val = drafted
            content_map[sec] = st.text_area(sec, value=default_val, height=200, key=f"pb_ta_{sec}")
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

        with st.expander("Snippets Inbox (from Y1/Y2/Y4/Y5)", expanded=True):
            try:
                df_snip = pd.read_sql_query("SELECT id, section, source, text, created_at FROM draft_snippets WHERE rfp_id=? ORDER BY id DESC;", conn, params=(int(rfp_id),))
            except Exception:
                df_snip = pd.DataFrame()
            if df_snip is None or df_snip.empty:
                st.caption("No snippets saved yet")
            else:
                st.dataframe(df_snip[["id","section","source","created_at"]], use_container_width=True, hide_index=True, height=200)
                sid = st.selectbox("Pick snippet ID", options=df_snip["id"].tolist())
                sec_choice = st.selectbox("Insert into section", options=selected, index=min(len(selected)-1, 0))
                if st.button("Insert snippet"):
                    txt = df_snip[df_snip["id"]==sid].iloc[0]["text"]
                    key = f"pb_ta_{sec_choice}"
                    cur = st.session_state.get(key, "")
                    st.session_state[key] = (cur + ("\n\n" if cur else "") + str(txt)).strip()
                    st.session_state[f"pb_section_{sec_choice}"] = st.session_state[key]
                    st.success("Inserted into section")
    

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
            f"SELECT id, name, email, phone, city, state, naics FROM vendors_t WHERE id IN ({ph});",
            conn,
            params=vendor_ids,
        )
    else:
        st.info("No vendors queued. Use Subcontractor Finder to select vendors, or pick by filter below.")
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
    df = pd.read_sql_query("SELECT id, title, solnum FROM rfps_t ORDER BY id DESC;", conn, params=())
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
    df = pd.read_sql_query("SELECT id, title FROM rfps_t ORDER BY id DESC;", conn, params=())
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
    df = pd.read_sql_query("SELECT id, title FROM rfps_t ORDER BY id DESC;", conn, params=())
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
        dfL = pd.read_sql_query("SELECT section, content FROM rfp_sections;", conn, params=())
    if not dfL.empty:
        dfL["score"] = dfL["content"].str.lower().apply(lambda t: sum(1 for w in q.split() if w in (t or "")))
        res["sections"] = dfL.sort_values("score", ascending=False).head(5)

    # Checklist
    if rfp_id:
        dfCk = pd.read_sql_query("SELECT item_text, status FROM lm_items WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfCk = pd.read_sql_query("SELECT item_text, status FROM lm_items;", conn, params=())
    if not dfCk.empty:
        dfCk["score"] = dfCk["item_text"].str.lower().apply(lambda t: sum(1 for w in q.split() if w in (t or "")))
        res["checklist"] = dfCk.sort_values("score", ascending=False).head(10)

    # CLINs
    if rfp_id:
        dfCL = pd.read_sql_query("SELECT clin, description, qty, unit FROM clin_lines WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfCL = pd.read_sql_query("SELECT clin, description, qty, unit FROM clin_lines;", conn, params=())
    if not dfCL.empty:
        dfCL["score"] = (dfCL["clin"].astype(str) + " " + dfCL["description"].astype(str)).str.lower().apply(lambda t: sum(1 for w in q.split() if w in (t or "")))
        res["clins"] = dfCL.sort_values("score", ascending=False).head(10)

    # Dates
    if rfp_id:
        dfDt = pd.read_sql_query("SELECT label, date_text FROM key_dates WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfDt = pd.read_sql_query("SELECT label, date_text FROM key_dates;", conn, params=())
    if not dfDt.empty:
        dfDt["score"] = (dfDt["label"].astype(str) + " " + dfDt["date_text"].astype(str)).str.lower().apply(lambda t: sum(1 for w in q.split() if w in (t or "")))
        res["dates"] = dfDt.sort_values("score", ascending=False).head(10)

    # POCs
    if rfp_id:
        dfP = pd.read_sql_query("SELECT name, role, email, phone FROM pocs WHERE rfp_id=?;", conn, params=(rfp_id,))
    else:
        dfP = pd.read_sql_query("SELECT name, role, email, phone FROM pocs;", conn, params=())
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

    df_rf = pd.read_sql_query("SELECT id, title FROM rfps_t ORDER BY id DESC;", conn, params=())
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
    df = pd.read_sql_query("SELECT * FROM org_profile WHERE id=1;", conn, params=())
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
        prof = pd.read_sql_query("SELECT * FROM org_profile WHERE id=1;", conn, params=())
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


def _export_past_perf_docx(path: str, records: list) -> Optional[str]:
    try:
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
    df_rf = pd.read_sql_query("SELECT id, title FROM rfps_t ORDER BY id DESC;", conn, params=())
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

def _wp_export_docx(path: str, title: str, subtitle: str, sections: pd.DataFrame) -> Optional[str]:
    try:
        from docx import Document
        from docx.shared import Inches
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
        df_t = pd.read_sql_query("SELECT id, name, description, created_at FROM white_templates ORDER BY id DESC;", conn, params=())
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
    df_p = pd.read_sql_query("SELECT id, title, subtitle, created_at, updated_at FROM white_papers ORDER BY id DESC;", conn, params=())
    c1, c2 = st.columns([2,2])
    with c1:
        st.markdown("**Create draft from template**")
        df_t = pd.read_sql_query("SELECT id, name FROM white_templates ORDER BY id DESC;", conn, params=())
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
        df_deals = pd.read_sql_query("SELECT id, title FROM deals_t ORDER BY id DESC;", conn, params=())
        df_contacts = pd.read_sql_query("SELECT id, name FROM contacts_t ORDER BY name;", conn, params=())
        a_col1, a_col2, a_col3 = st.columns([2,2,2])
        with a_col1:
            a_type = st.selectbox("Type", ["Call","Email","Meeting","Note"], key="act_type")
            a_subject = st.text_input("Subject", key="act_subject")
        with a_col2:
            a_deal = st.selectbox("Related Deal (optional)", options=[None] + df_deals["id"].tolist(),
                                  format_func=lambda x: "None" if x is None else f"#{x} — {df_deals.loc[df_deals['id']==x,'title'].values[0]}",
                                  key="act_deal")
            a_contact = st.selectbox("Related Contact (optional)", options=[None] + df_contacts["id"].tolist(),
                                     format_func=lambda x: "None" if x is None else df_contacts.loc[df_contacts["id"]==x, "name"].values[0],
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
        f1, f2, f3 = st.columns([2,2,2])
        with f1:
            f_type = st.multiselect("Type filter", ["Call","Email","Meeting","Note"])
        with f2:
            f_deal = st.selectbox("Deal filter", options=[None] + df_deals["id"].tolist(),
                                  format_func=lambda x: "All" if x is None else f"#{x} — {df_deals.loc[df_deals['id']==x,'title'].values[0]}",
                                  key="act_f_deal")
        with f3:
            f_contact = st.selectbox("Contact filter", options=[None] + df_contacts["id"].tolist(),
                                     format_func=lambda x: "All" if x is None else df_contacts.loc[df_contacts["id"]==x, "name"].values[0],
                                     key="act_f_contact")
        q = "SELECT ts, type, subject, notes, deal_id, contact_id FROM activities_t WHERE 1=1"
        params = []
        if f_type:
            q += " AND type IN (%s)" % ",".join(["?"]*len(f_type))
            params.extend(f_type)
        if f_deal:
            q += " AND deal_id=?"; params.append(f_deal)
        if f_contact:
            q += " AND contact_id=?"; params.append(f_contact)
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
        df_deals = pd.read_sql_query("SELECT id, title FROM deals_t ORDER BY id DESC;", conn, params=())
        df_contacts = pd.read_sql_query("SELECT id, name FROM contacts_t ORDER BY name;", conn, params=())
        t1, t2, t3 = st.columns([2,2,2])
        with t1:
            t_title = st.text_input("Task title", key="task_title")
            t_due = st.date_input("Due date", key="task_due")
        with t2:
            t_priority = st.selectbox("Priority", ["Low","Normal","High"], index=1, key="task_priority")
            t_status = st.selectbox("Status", ["Open","In Progress","Done"], index=0, key="task_status")
        with t3:
            t_deal = st.selectbox("Related Deal (optional)", options=[None] + df_deals["id"].tolist(),
                                  format_func=lambda x: "None" if x is None else f"#{x} — {df_deals.loc[df_deals['id']==x,'title'].values[0]}",
                                  key="task_deal")
            t_contact = st.selectbox("Related Contact (optional)", options=[None] + df_contacts["id"].tolist(),
                                     format_func=lambda x: "None" if x is None else df_contacts.loc[df_contacts["id"]==x, "name"].values[0],
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
        f1, f2 = st.columns([2,2])
        with f1:
            tf_status = st.multiselect("Status", ["Open","In Progress","Done"], default=["Open","In Progress"])
        with f2:
            tf_priority = st.multiselect("Priority", ["Low","Normal","High"], default=[])
        q = "SELECT id, title, due_date, status, priority, deal_id, contact_id FROM tasks_t WHERE 1=1"
        params = []
        if tf_status:
            q += " AND status IN (%s)" % ",".join(["?"]*len(tf_status)); params.extend(tf_status)
        if tf_priority:
            q += " AND priority IN (%s)" % ",".join(["?"]*len(tf_priority)); params.extend(tf_priority)
        q += " ORDER BY COALESCE(due_date,'9999-12-31') ASC"
        df_t = pd.read_sql_query(q, conn, params=params)
        if df_t.empty:
            st.write("No tasks")
        else:
            for _, r in df_t.iterrows():
                c1, c2, c3, c4 = st.columns([3,2,2,2])
                with c1:
                    st.write(f"**{r['title']}**  — due {r['due_date'] or '—'}")
                with c2:
                    new_status = st.selectbox("Status", ["Open","In Progress","Done"],
                                              index=["Open","In Progress","Done"].index(r["status"] if r["status"] in ["Open","In Progress","Done"] else "Open"),
                                              key=f"task_status_{int(r['id'])}")
                with c3:
                    new_pri = st.selectbox("Priority", ["Low","Normal","High"],
                                            index=["Low","Normal","High"].index(r["priority"] if r["priority"] in ["Low","Normal","High"] else "Normal"),
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
        df = pd.read_sql_query("SELECT id, title, agency, status, value FROM deals_t ORDER BY id DESC;", conn, params=())
        if df.empty:
            st.info("No deals")
        else:
            df["prob_%"] = df["status"].apply(_stage_probability)
            df["expected_value"] = (df["value"].fillna(0).astype(float) * df["prob_%"] / 100.0).round(2)
            # Stage age: days since last stage change
            df_log = pd.read_sql_query("SELECT deal_id, stage, MAX(changed_at) AS last_change FROM deal_stage_log_t GROUP BY deal_id, stage;", conn, params=())
            def stage_age(row):
                try:
                    last = df_log[(df_log["deal_id"]==row["id"]) & (df_log["stage"]==row["status"])]["last_change"]
                    if last.empty: return None
                    dt = pd.to_datetime(last.values[0])
                    return (pd.Timestamp.utcnow() - dt).days
                except Exception:
                    return None
            df["stage_age_days"] = df.apply(stage_age, axis=1)
            st.dataframe(df[["title","agency","status","value","prob_%","expected_value","stage_age_days"]], use_container_width=True, hide_index=True)

            st.subheader("Summary by Stage")
            summary = df.groupby("status").agg(
                deals=("id","count"),
                value=("value","sum"),
                expected=("expected_value","sum")
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
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_owner ON files(owner_type, owner_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_tags ON files(tags);")
            conn.commit()
    except Exception:
        pass

# ---------- Phase J: File Manager & Submission Kit ----------
def _detect_mime(name: str) -> str:
    name = (name or "").lower()
    if name.endswith(".pdf"): return "application/pdf"
    if name.endswith(".docx"): return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if name.endswith(".xlsx"): return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if name.endswith(".zip"): return "application/zip"
    if name.endswith(".png"): return "image/png"
    if name.endswith(".jpg") or name.endswith(".jpeg"): return "image/jpeg"
    if name.endswith(".txt"): return "text/plain"
    return "application/octet-stream"


def run_file_manager(conn: sqlite3.Connection) -> None:
    _ensure_files_table(conn)
    st.header("File Manager")
    st.caption("Attach files to RFPs / Deals / Vendors, tag them, and build a zipped submission kit.")

    # --- Attach uploader ---
    with st.expander("Upload & Attach", expanded=True):
        c1, c2 = st.columns([2,2])
        with c1:
            owner_type = st.selectbox("Attach to", ["RFP", "Deal", "Vendor", "Other"], key="fm_owner_type")
            owner_id = None
            if owner_type == "RFP":
                df_rf = pd.read_sql_query("SELECT id, title FROM rfps_t ORDER BY id DESC;", conn, params=())
                if not df_rf.empty:
                    owner_id = st.selectbox("RFP", options=df_rf["id"].tolist(),
                                            format_func=lambda i: f"#{i} — {df_rf.loc[df_rf['id']==i, 'title'].values[0]}",
                                            key="fm_owner_rfp")
            elif owner_type == "Deal":
                df_deal = pd.read_sql_query("SELECT id, title FROM deals_t ORDER BY id DESC;", conn, params=())
                if not df_deal.empty:
                    owner_id = st.selectbox("Deal", options=df_deal["id"].tolist(),
                                            format_func=lambda i: f"#{i} — {df_deal.loc[df_deal['id']==i, 'title'].values[0]}",
                                            key="fm_owner_deal")
            elif owner_type == "Vendor":
                df_v = pd.read_sql_query("SELECT id, name FROM vendors_t ORDER BY name;", conn, params=())
                if not df_v.empty:
                    owner_id = st.selectbox("Vendor", options=df_v["id"].tolist(),
                                            format_func=lambda i: f"#{i} — {df_v.loc[df_v['id']==i, 'name'].values[0]}",
                                            key="fm_owner_vendor")
            # Owner_id can be None for "Other"
        with c2:
            tags = st.text_input("Tags (comma-separated)", key="fm_tags")
            notes = st.text_area("Notes (optional)", height=70, key="fm_notes")

        ups = st.file_uploader("Select files", type=None, accept_multiple_files=True, key="fm_files")
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
                                owner_type, int(owner_id) if owner_id else None, f.name, pth, f.size, _detect_mime(f.name),
                                tags.strip(), notes.strip()
                            ))
                            conn.commit()
                            saved += 1
                    except Exception as e:
                        st.error(f"DB save failed: {e}")
                st.success(f"Uploaded {saved} file(s).")

    # --- Library & filters ---
    with st.expander("Library", expanded=True):
        l1, l2, l3 = st.columns([2,2,2])
        with l1:
            f_owner = st.selectbox("Filter by type", ["All", "RFP", "Deal", "Vendor", "Other"], key="fm_f_owner")
        with l2:
            f_tag = st.text_input("Tag contains", key="fm_f_tag")
        with l3:
            f_kw = st.text_input("Filename contains", key="fm_f_kw")

        q = "SELECT id, owner_type, owner_id, filename, path, size, mime, tags, notes, uploaded_at FROM files_t WHERE 1=1"
        params = []
        if f_owner and f_owner != "All":
            q += " AND owner_type=?"; params.append(f_owner)
        if f_tag:
            q += " AND tags LIKE ?"; params.append(f"%{f_tag}%")
        if f_kw:
            q += " AND filename LIKE ?"; params.append(f"%{f_kw}%")
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
            st.dataframe(df_files.drop(columns=["path"]), use_container_width=True, hide_index=True)
            # Per-row controls
            for _, r in df_files.iterrows():
                c1, c2, c3, c4 = st.columns([3,2,2,2])
                with c1:
                    st.caption(f"#{int(r['id'])} — {r['filename']} ({r['owner_type']} {int(r['owner_id']) if r['owner_id'] else ''})")
                with c2:
                    new_tags = st.text_input("Tags", value=r.get("tags") or "", key=f"fm_row_tags_{int(r['id'])}")
                with c3:
                    new_notes = st.text_input("Notes", value=r.get("notes") or "", key=f"fm_row_notes_{int(r['id'])}")
                with c4:
                    b1, b2 = st.columns(2)
                    with b1:
                        if st.button("Save", key=f"fm_row_save_{int(r['id'])}"):
                            with closing(conn.cursor()) as cur:
                                cur.execute("UPDATE files SET tags=?, notes=? WHERE id=?;", (new_tags.strip(), new_notes.strip(), int(r["id"])))
                                conn.commit()
                            st.success("Updated")
                    with b2:
                        if st.button("Delete", key=f"fm_row_del_{int(r['id'])}"):
                            with closing(conn.cursor()) as cur:
                                cur.execute("DELETE FROM files_t WHERE id=?;", (int(r["id"]),))
                                conn.commit()
                            try:
                                if r.get("path") and os.path.exists(r["path"]):
                                    os.remove(r["path"])
                            except Exception:
                                pass
                            st.success("Deleted"); st.rerun()

    # --- Submission Kit (ZIP) ---
    st.subheader("Submission Kit (ZIP)")
    df_rf_all = pd.read_sql_query("SELECT id, title FROM rfps_t ORDER BY id DESC;", conn, params=())
    if df_rf_all.empty:
        st.info("Create an RFP in RFP Analyzer first (Parse → Save).")
        return

    kit_rfp = st.selectbox("RFP", options=df_rf_all["id"].tolist(),
                           format_func=lambda rid: f"#{rid} — {df_rf_all.loc[df_rf_all['id']==rid,'title'].values[0]}",
                           key="fm_kit_rfp")

    # Load files for this RFP
    try:
        df_kit = pd.read_sql_query("SELECT id, filename, path, tags FROM files_t WHERE owner_type='RFP' AND owner_id=? ORDER BY uploaded_at DESC;", conn, params=(int(kit_rfp),))
    except Exception:
        _ensure_files_table(conn)
        df_kit = pd.DataFrame(columns=["id","filename","path","tags"])
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
                df_sel = pd.read_sql_query(f"SELECT filename, path FROM files_t WHERE id IN ({ph});", conn, params=selected)
                for _, r in df_sel.iterrows():
                    rows.append((r["filename"], r["path"]))
            for p in gen_paths:
                rows.append((Path(p).name, p))

            # Create ZIP
            from zipfile import ZipFile, ZIP_DEFLATED
            ts = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
            zip_path = str(Path(DATA_DIR) / f"submission_kit_RFP_{int(kit_rfp)}_{ts}.zip")
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
                    manifest += "\nIncluded files:\n" + "\n".join(f"- {fname}" for fname, _ in rows)
                    z.writestr("MANIFEST.txt", manifest)
                st.success("Submission kit created")
                st.markdown(f"[Download ZIP]({zip_path})")
            except Exception as e:
                st.error(f"ZIP failed: {e}")



# ---------- Phase L: RFQ Pack ----------
def _rfq_pack_by_id(conn: sqlite3.Connection, pid: int) -> dict | None:
    df = pd.read_sql_query("SELECT * FROM rfq_packs_t WHERE id=?;", conn, params=(pid,))
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
        return pd.DataFrame(columns=["id","vendor_id","name","email","phone"])

def _rfq_attachments(conn: sqlite3.Connection, pid: int) -> pd.DataFrame:
    return pd.read_sql_query("SELECT id, file_id, name, path FROM rfq_attach_t WHERE pack_id=? ORDER BY id ASC;", conn, params=(pid,))

def _rfq_build_zip(conn: sqlite3.Connection, pack_id: int) -> Optional[str]:
    pack = _rfq_pack_by_id(conn, pack_id)
    if not pack: 
        st.error("Pack not found"); return None
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
                df = pd.read_sql_query("SELECT filename, path FROM files_t WHERE id=?;", conn, params=(int(r["file_id"]),))
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
    mail_csv_path = str(Path(DATA_DIR) / f"rfq_{pack_id}_vendors_mailmerge.csv")
    mm = df_v.rename(columns={"name":"VendorName", "email":"VendorEmail", "phone":"VendorPhone"})[["VendorName","VendorEmail","VendorPhone"]]
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
                try: z.write(pth, arcname=fname)
                except Exception: pass
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
    st.caption("Build vendor-ready RFQ packages from your CLINs, attachments, and vendor list.")

    # -- Create / open
    left, right = st.columns([2,2])
    with left:
        st.subheader("Create")
        df_rf = pd.read_sql_query("SELECT id, title FROM rfps_t ORDER BY id DESC;", conn, params=())
        rf_opt = st.selectbox("RFP (optional)", options=[None] + df_rf["id"].tolist(),
                              format_func=lambda x: "None" if x is None else f"#{x} — {df_rf.loc[df_rf['id']==x,'title'].values[0]}",
                              key="rfq_rfp_sel")
        title = st.text_input("Pack title", key="rfq_title")
        due = st.date_input("Quote due date", key="rfq_due")
        instr = st.text_area("Instructions to vendors (email body)", height=100, key="rfq_instr")
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
                st.success("Created"); st.rerun()
    with right:
        st.subheader("Open")
        df_pk = pd.read_sql_query("SELECT id, title, due_date, created_at FROM rfq_packs_t ORDER BY id DESC;", conn, params=())
        if df_pk.empty:
            st.info("No RFQ packs yet")
            return
        pk_sel = st.selectbox("RFQ Pack", options=df_pk["id"].tolist(),
                              format_func=lambda pid: f"#{pid} — {df_pk.loc[df_pk['id']==pid,'title'].values[0]} (due {df_pk.loc[df_pk['id']==pid,'due_date'].values[0] or '—'})",
                              key="rfq_open_sel")

    st.divider()
    st.subheader(f"Editing pack #{int(pk_sel)}")

    # ---- CLINs / Lines ----
    st.markdown("### CLINs / Lines")
    df_lines = _rfq_lines(conn, int(pk_sel))
    st.dataframe(df_lines, use_container_width=True, hide_index=True)
    c1, c2, c3, c4, c5, c6 = st.columns([1.2,3,1,1,1,1])
    with c1:
        l_code = st.text_input("CLIN", key="rfq_line_code")
    with c2:
        l_desc = st.text_input("Description", key="rfq_line_desc")
    with c3:
        l_qty = st.number_input("Qty", min_value=0.0, value=1.0, step=1.0, key="rfq_line_qty")
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
        st.success("Line added"); st.rerun()

    if not df_lines.empty:
        st.markdown("**Edit existing lines**")
        for _, r in df_lines.iterrows():
            ec1, ec2, ec3, ec4 = st.columns([3,1,1,1])
            with ec1:
                nd = st.text_input("Desc", value=r["description"] or "", key=f"rfq_line_e_desc_{int(r['id'])}")
            with ec2:
                nq = st.number_input("Qty", value=float(r["qty"] or 0), step=1.0, key=f"rfq_line_e_qty_{int(r['id'])}")
            with ec3:
                nu = st.text_input("Unit", value=r["unit"] or "EA", key=f"rfq_line_e_unit_{int(r['id'])}")
            with ec4:
                if st.button("Save", key=f"rfq_line_e_save_{int(r['id'])}"):
                    with closing(conn.cursor()) as cur:
                        cur.execute("UPDATE rfq_lines SET description=?, qty=?, unit=? WHERE id=?;",
                                    (nd.strip(), float(nq or 0), nu.strip(), int(r["id"])))
                        conn.commit()
                    st.success("Updated"); st.rerun()

    st.divider()

    # ---- Attachments ----
    st.markdown("### Attachments")
    pack = _rfq_pack_by_id(conn, int(pk_sel))
    rfp_id = pack.get("rfp_id")
    if rfp_id:
        df_rfp_files = pd.read_sql_query("SELECT id, filename, path, tags FROM files_t WHERE owner_type='RFP' AND owner_id=? ORDER BY uploaded_at DESC;", conn, params=(int(rfp_id),))
    else:
        df_rfp_files = pd.DataFrame(columns=["id","filename","path","tags"])
    df_att = _rfq_attachments(conn, int(pk_sel))
    st.dataframe(df_att.drop(columns=[]), use_container_width=True, hide_index=True)

    st.markdown("**Add from File Manager**")
    # allow selecting from all files
    df_all_files = pd.read_sql_query("SELECT id, filename FROM files_t ORDER BY uploaded_at DESC;", conn, params=())
    add_file = st.selectbox("File", options=[None] + df_all_files["id"].astype(int).tolist(),
                            format_func=lambda i: "Choose…" if i is None else f"#{i} — {df_all_files.loc[df_all_files['id']==i,'filename'].values[0]}",
                            key="rfq_att_file")
    if st.button("Add Attachment", key="rfq_att_add"):
        if add_file is None:
            st.warning("Pick a file")
        else:
            df_one = pd.read_sql_query("SELECT filename, path FROM files_t WHERE id=?;", conn, params=(int(add_file),))
            if df_one.empty:
                st.error("File not found")
            else:
                with closing(conn.cursor()) as cur:
                    cur.execute("INSERT INTO rfq_attach(pack_id, file_id, name, path) VALUES(?,?,?,?);",
                                (int(pk_sel), int(add_file), df_one.iloc[0]["filename"], df_one.iloc[0]["path"]))
                    conn.commit()
                st.success("Added"); st.rerun()

    if not df_att.empty:
        for _, r in df_att.iterrows():
            dc1, dc2 = st.columns([3,1])
            with dc1:
                st.caption(f"#{int(r['id'])} — {r['name'] or Path(r['path']).name}")
            with dc2:
                if st.button("Remove", key=f"rfq_att_del_{int(r['id'])}"):
                    with closing(conn.cursor()) as cur:
                        cur.execute("DELETE FROM rfq_attach_t WHERE id=?;", (int(r["id"]),))
                        conn.commit()
                    st.success("Removed"); st.rerun()

    st.divider()

    # ---- Vendors ----
    st.markdown("### Vendors")
    try:
        df_vendors = pd.read_sql_query("SELECT id, name, email FROM vendors_t ORDER BY name;", conn, params=())
    except Exception as e:
        st.info("No vendors table yet. Use Subcontractor Finder to add vendors.")
        df_vendors = pd.DataFrame(columns=["id","name","email"])
    df_rv = _rfq_vendors(conn, int(pk_sel))
    st.dataframe(df_rv[["name","email","phone"]] if not df_rv.empty else pd.DataFrame(), use_container_width=True, hide_index=True)

    add_vs = st.multiselect("Add vendors", options=df_vendors["id"].astype(int).tolist(),
                            format_func=lambda vid: df_vendors.loc[df_vendors["id"]==vid, "name"].values[0],
                            key="rfq_vendor_add")
    if st.button("Add Selected Vendors", key="rfq_vendor_add_btn"):
        with closing(conn.cursor()) as cur:
            for vid in add_vs:
                try:
                    cur.execute("INSERT OR IGNORE INTO rfq_vendors(pack_id, vendor_id) VALUES(?,?);", (int(pk_sel), int(vid)))
                except Exception:
                    pass
            conn.commit()
        st.success("Vendors added"); st.rerun()

    if not df_rv.empty:
        for _, r in df_rv.iterrows():
            vc1, vc2 = st.columns([3,1])
            with vc1:
                st.caption(f"{r['name']} — {r.get('email') or ''}")
            with vc2:
                if st.button("Remove", key=f"rfq_vendor_del_{int(r['id'])}"):
                    with closing(conn.cursor()) as cur:
                        cur.execute("DELETE FROM rfq_vendors_t WHERE id=?;", (int(r["id"]),))
                        conn.commit()
                    st.success("Removed"); st.rerun()

    st.divider()

    # ---- Build + Exports ----
    st.markdown("### Build & Export")
    czip, cmcsv, cclin = st.columns([2,2,2])
    with czip:
        if st.button("Build RFQ ZIP", type="primary", key="rfq_build_zip"):
            z = _rfq_build_zip(conn, int(pk_sel))
            if z:
                st.success("ZIP ready"); st.markdown(f"[Download ZIP]({z})")

    with cmcsv:
        if st.button("Export Vendors Mail-Merge CSV", key="rfq_mail_csv"):
            df_v = _rfq_vendors(conn, int(pk_sel))
            if df_v.empty:
                st.warning("No vendors selected")
            else:
                out = df_v.rename(columns={"name":"VendorName","email":"VendorEmail","phone":"VendorPhone"})[["VendorName","VendorEmail","VendorPhone"]]
                out["Subject"] = f"Request for Quote – {_rfq_pack_by_id(conn, int(pk_sel)).get('title')}"
                out["Body"] = _rfq_pack_by_id(conn, int(pk_sel)).get("instructions") or ""
                path = str(Path(DATA_DIR) / f"rfq_{int(pk_sel)}_mailmerge.csv")
                out.to_csv(path, index=False)
                st.success("Exported"); st.markdown(f"[Download CSV]({path})")

    with cclin:
        if st.button("Export CLINs CSV", key="rfq_clins_csv"):
            df = _rfq_lines(conn, int(pk_sel))
            if df.empty:
                st.warning("No CLINs yet")
            else:
                path = str(Path(DATA_DIR) / f"rfq_{int(pk_sel)}_CLINs.csv")
                df.to_csv(path, index=False)
                st.success("Exported"); st.markdown(f"[Download CSV]({path})")


def _db_path_from_conn(conn: sqlite3.Connection) -> str:
    try:
        df = pd.read_sql_query("PRAGMA database_list;", conn, params=())
        p = df[df["name"]=="main"]["file"].values[0]
        return p or str(Path(DATA_DIR) / "app.db")
    except Exception:
        return str(Path(DATA_DIR) / "app.db")

def migrate(conn: sqlite3.Connection) -> None:
    """Lightweight idempotent migrations and indices."""
    with closing(conn.cursor()) as cur:
        # read current version
        try:
            ver = int(pd.read_sql_query("SELECT ver FROM schema_version WHERE id=1;", conn, params=()).iloc[0]["ver"])
        except Exception:
            ver = 0

        # v1: add common indexes
        if ver < 1:
            try:
                cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_stage ON deals(stage);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_status ON deals(status);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_lm_items_rfp ON lm_items(rfp_id);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_files_owner2 ON files(owner_type, owner_id);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(due_date);")
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
        return int(pd.read_sql_query("SELECT ctid FROM current_tenant WHERE id=1;", conn, params=()).iloc[0]["ctid"])
    except Exception:
        return 1

def _safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s or "")

def _backup_db(conn: sqlite3.Connection) -> Optional[str]:
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
            src.close(); dst.close()
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
        src = _sq.connect(str(tmp))
        dst = _sq.connect(db_path)
        with dst:
            src.backup(dst)  # replaces content
        src.close(); dst.close()
        return True
    except Exception as e:
        st.error(f"Restore failed: {e}")
        return False

def _export_table_csv(conn: sqlite3.Connection, table_or_view: str, scoped: bool = True) -> Optional[str]:
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
        path = Path(DATA_DIR) / f"export_{name}_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(path, index=False)
        return str(path)
    except Exception as e:
        st.error(f"Export failed: {e}")
        return None

def _import_csv_into_table(conn: sqlite3.Connection, csv_file, table: str, scoped_to_current: bool=True) -> int:
    # Read CSV and insert rows. If tenant_id missing and scoped, stamp with current tenant.
    try:
        df = pd.read_csv(io.BytesIO(csv_file.getbuffer()))
    except Exception as e:
        st.error(f"CSV read failed: {e}")
        return 0
    if scoped_to_current and "tenant_id" not in df.columns:
        df["tenant_id"] = _current_tenant(conn)
    # Align columns with destination
    try:
        cols = pd.read_sql_query(f"PRAGMA table_info({table});", conn)["name"].tolist()
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
        try: df2 = df2.drop(columns=["id"])
        except Exception: pass
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
    st.caption("WAL on; lightweight migrations; export/import CSV; backup/restore the SQLite DB.")

    st.subheader("Database Info")
    dbp = _db_path_from_conn(conn)
    st.write(f"Path: `{dbp}`")
    try:
        ver = pd.read_sql_query("SELECT ver FROM schema_version WHERE id=1;", conn, params=()).iloc[0]["ver"]
    except Exception:
        ver = "n/a"
    st.write(f"Schema version: **{ver}**")

    c1, c2, c3 = st.columns([2,2,2])
    with c1:
        if st.button("Run Migrations"):
            try:
                migrate(conn); st.success("Migrations done")
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
    b1, b2 = st.columns([2,2])
    with b1:
        if st.button("Create Backup (.db)"):
            p = _backup_db(conn)
            if p: st.success("Backup created"); st.markdown(f"[Download backup]({p})")
    with b2:
        up = st.file_uploader("Restore from .db file", type=["db","sqlite","sqlite3"])
        if up and st.button("Restore Now"):
            ok = _restore_db_from_upload(conn, up)
            if ok:
                st.success("Restore completed. Please rerun the app.")
                st.rerun()

    st.divider()
    st.subheader("Export / Import CSV")
    tables = ["rfps","lm_items","lm_meta","deals","activities","tasks","deal_stage_log",
              "vendors","files","rfq_packs","rfq_lines","rfq_vendors","rfq_attach","contacts"]
    tsel = st.selectbox("Table", options=tables, key="persist_tbl")
    e1, e2 = st.columns([2,2])
    with e1:
        if st.button("Export CSV (current workspace)"):
            p = _export_table_csv(conn, tsel, scoped=True)
            if p: st.success("Exported"); st.markdown(f"[Download CSV]({p})")
    with e2:
        if st.button("Export CSV (all rows)"):
            p = _export_table_csv(conn, tsel, scoped=False)
            if p: st.success("Exported"); st.markdown(f"[Download CSV]({p})")

    upcsv = st.file_uploader("Import into selected table from CSV", type=["csv"], key="persist_upcsv")
    if upcsv and st.button("Import CSV"):
        n = _import_csv_into_table(conn, upcsv, tsel, scoped_to_current=True)
        if n:
            st.success(f"Imported {n} row(s) into {tsel}")
            st.rerun()



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
    st.markdown(f"<div class='card'><h3>{title}</h3>" + (f"<div class='sidebar-subtle'>{subtitle}</div>" if subtitle else "") + "</div>", unsafe_allow_html=True)

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
            df_tenants = pd.read_sql_query("SELECT id, name FROM tenants ORDER BY id;", conn, params=())
        except Exception:
            df_tenants = pd.DataFrame(columns=["id","name"])
        try:
            cur_tid = int(pd.read_sql_query("SELECT ctid FROM current_tenant WHERE id=1;", conn, params=()).iloc[0]["ctid"])
        except Exception:
            cur_tid = 1
        opt = st.selectbox("Organization", options=(df_tenants["id"].astype(int).tolist() if not df_tenants.empty else [1]),
                           format_func=lambda i: (df_tenants.loc[df_tenants["id"]==i,"name"].values[0] if not df_tenants.empty else "Default"),
                           key="tenant_sel")
        if st.button("Switch", key="tenant_switch"):
            with closing(conn.cursor()) as cur:
                cur.execute("UPDATE current_tenant SET ctid=? WHERE id=1;", (int(opt),))
                conn.commit()
            st.session_state['tenant_id'] = int(opt)
            st.success("Workspace switched"); st.rerun()

        st.divider()
        new_name = st.text_input("New workspace name", key="tenant_new_name")
        if st.button("Create workspace", key="tenant_create"):
            if new_name.strip():
                with closing(conn.cursor()) as cur:
                    cur.execute("INSERT OR IGNORE INTO tenants(name, created_at) VALUES(?, datetime('now'));", (new_name.strip(),))
                    conn.commit()
                st.success("Workspace created"); st.rerun()
            else:
                st.warning("Enter a name")



# --- Phase U helper: namespaced keys for Streamlit ---
def ns(scope: str, key: str) -> str:
    """Generate stable, unique Streamlit widget keys."""
    return f"{scope}::{key}"

def router(page: str, conn: sqlite3.Connection) -> None:
    if page == "SAM Watch":
        run_sam_watch(conn)
    elif page == "RFP Analyzer":
        run_rfp_analyzer(conn)
    elif page == "L and M Checklist":
        run_lm_checklist(conn)
    elif page == "Proposal Builder":
        run_proposal_builder(conn)
        # Phase V panel
        globals().get('pb_phase_v_section_library', lambda _c: None)(conn)
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



# === Y2 LOCK PATCH (evidence-only CO Chat) ===
# This override ensures CO Chat will not answer generically when no RFP evidence is available.
# Behavior:
# - If y1_search returns no hits for the question, the assistant refuses and instructs the user to build/update the index.
# - When evidence exists, normal streaming continues with bracketed citations as before.
def y2_stream_answer(conn, rfp_id: int, thread_id: int, user_q: str, k: int = 6, temperature: float = 0.2):
    try:
        _k = max(3, int(k))
    except Exception:
        _k = 6
    # Require evidence from the indexed RFP chunks
    _hits = y1_search(conn, int(rfp_id), user_q or "", k=int(_k))
    if not _hits:
        msg = ("Insufficient evidence in linked RFP files. "
               "Build or Update the search index for this RFP on 'Ask with citations (Y1)', then ask again. "
               "General answers are disabled in CO Chat.")
        # Persist refusal to the thread for auditability
        try:
            y2_append_message(conn, int(thread_id), "assistant", msg)
        except Exception:
            pass
        # Stream the refusal once
        yield msg
        return

    # Otherwise continue with the standard message construction which embeds EVIDENCE
    msgs = _y2_build_messages(conn, int(rfp_id), int(thread_id), user_q or "", k=int(_k))
    client = get_ai()
    model_name = _resolve_model()
    try:
        resp = client.chat.completions.create(model=model_name, messages=msgs, temperature=float(temperature), stream=True)
    except Exception as _e:
        if "model_not_found" in str(_e) or "does not exist" in str(_e):
            resp = client.chat.completions.create(model="gpt-4o-mini", messages=msgs, temperature=float(temperature), stream=True)
        else:
            yield f"AI unavailable: {type(_e).__name__}: {_e}"
            return
    for ch in resp:
        try:
            delta = ch.choices[0].delta
            if hasattr(delta, "content") and delta.content:
                yield delta.content
        except Exception:
            pass
# === End Y2 LOCK PATCH ===

def main() -> None:
    conn = get_db()
    st.title(APP_TITLE)
    st.caption(BUILD_LABEL)
    # Y0 main panel (always on)
    try:
        y0_ai_panel()
    except Exception:
        pass
    router(nav(), conn)


if __name__ == "__main__":
    main()


# -------------------- Phase V: Proposal Builder — Section Library / Templates --------------------
def pb_phase_v_section_library(conn: sqlite3.Connection) -> None:
    st.markdown("### Section Library (Phase V)")
    cols = st.columns([3,2,2])
    with cols[0]:
        title = st.text_input("Title", key=ns("pbv","title"))
    with cols[1]:
        tags = st.text_input("Tags (comma-separated)", key=ns("pbv","tags"))
    with cols[2]:
        add_btn = st.button("Add / Update", key=ns("pbv","add"))
    body = st.text_area("Body (Markdown supported)", height=180, key=ns("pbv","body"))

    sel_id = st.session_state.get(ns("pbv","sel_id"))
    if add_btn:
        with closing(conn.cursor()) as cur:
            if sel_id:
                cur.execute("UPDATE pb_sections_t SET title=?, body=?, tags=?, updated_at=datetime('now') WHERE id=?;",
                            (title.strip(), body, tags.strip(), sel_id))
            else:
                cur.execute("INSERT INTO pb_sections_t(title, body, tags) VALUES (?,?,?);",
                            (title.strip(), body, tags.strip()))
            conn.commit()
            st.success("Saved")
            st.session_state.pop(ns("pbv","sel_id"), None)

    # Table of existing sections
    df = pd.read_sql_query("SELECT id, title, tags, created_at, updated_at FROM pb_sections_t ORDER BY updated_at DESC;", conn, params=())
    st.dataframe(df, use_container_width=True, hide_index=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        sel = st.number_input("Select ID to edit", min_value=0, step=1, key=ns("pbv","pick_id"))
        if st.button("Load", key=ns("pbv","load")) and sel:
            row = pd.read_sql_query("SELECT id, title, body, tags FROM pb_sections_t WHERE id=?;", conn, params=(int(sel),))
            if not row.empty:
                st.session_state[ns("pbv","sel_id")] = int(row.at[0,"id"])
                st.session_state[ns("pbv","title")] = row.at[0,"title"] or ""
                st.session_state[ns("pbv","body")] = row.at[0,"body"] or ""
                st.session_state[ns("pbv","tags")] = row.at[0,"tags"] or ""
                st.rerun()

    with c2:
        if st.button("Delete Selected", key=ns("pbv","del")) and sel:
            with closing(conn.cursor()) as cur:
                cur.execute("DELETE FROM pb_sections_t WHERE id=?;", (int(sel),))
                conn.commit()
            st.warning("Deleted")
            st.rerun()

    with c3:
        if st.button("Insert into Proposal (Compose)", key=ns("pbv","insert")) and sel:
            row = pd.read_sql_query("SELECT title, body FROM pb_sections_t WHERE id=?;", conn, params=(int(sel),))
            if not row.empty:
                # Use session 'pb_prefill' to hand off to Proposal Builder compose
                pre = st.session_state.get('pb_prefill') or {}
                pre = dict(pre)
                t = (row.at[0,'title'] or 'Untitled').strip()
                b = (row.at[0,'body'] or '').strip()
                # put under a unique key
                key_name = f"Section: {t}"
                pre[key_name] = b
                st.session_state['pb_prefill'] = pre
                st.success("Added to compose. Open 'Proposal Builder' -> Import.")

# === helper: auto-select number of sources to cite ===
def y_auto_k(text: str) -> int:
    t = (text or "").lower()
    n = len(t)
    broad = any(k in t for k in [
        "overview", "summary", "summarize", "list all", "requirements", "compliance",
        "section l", "section m", "evaluation factors", "factors", "checklist",
        "compare", "differences", "conflict", "conflicts", "crosswalk", "matrix"
    ])
    if not t.strip():
        return 4
    base = 7 if broad else 4
    if n > 500:
        base += 1
    if n > 1200:
        base += 1
    return max(3, min(8, base))