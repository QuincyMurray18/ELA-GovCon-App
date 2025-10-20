
# x8_ai.py — Phase X8: GPT‑5 RAG + CO Brief for ELA GovCon Suite
from __future__ import annotations
import os, io, re, sqlite3, json, math, struct
from contextlib import closing
from typing import List, Dict, Any, Tuple, Optional
import numpy as np
import pandas as pd

try:
    import streamlit as st
except Exception:
    st = None  # allow headless imports

# --- OpenAI client helpers ---
def _client():
    try:
        from openai import OpenAI
    except Exception as e:
        raise RuntimeError("openai package not installed: pip install openai>=1.40.0") from e
    # key precedence: env -> secrets.openai.api_key -> secrets.OPENAI_API_KEY
    key = os.getenv("OPENAI_API_KEY")
    if not key and st is not None:
        try:
            key = st.secrets.get("openai", {}).get("api_key") or st.secrets.get("OPENAI_API_KEY")
        except Exception:
            key = None
    if not key:
        raise RuntimeError("Missing OPENAI_API_KEY. Add to environment or Streamlit secrets.")
    return OpenAI(api_key=key)

def _models():
    fast = "gpt-5-mini"
    heavy = "gpt-5"
    embed = "text-embedding-3-small"
    if st is not None:
        try:
            sect = st.secrets.get("models", {})
            fast = sect.get("fast", fast)
            heavy = sect.get("heavy", heavy)
            embed = sect.get("embed", embed)
        except Exception:
            pass
    return fast, heavy, embed

# --- Tiny vector store in SQLite ---
def ensure_ai_schema(conn: sqlite3.Connection) -> None:
    with closing(conn.cursor()) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ai_embeds(
                id INTEGER PRIMARY KEY,
                rfp_id INTEGER,
                file_id INTEGER,
                chunk_id INTEGER,
                filename TEXT,
                text TEXT,
                vec BLOB
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_embeds_rfp ON ai_embeds(rfp_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_embeds_file ON ai_embeds(file_id);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ai_index_log(
                id INTEGER PRIMARY KEY,
                rfp_id INTEGER,
                chunk_count INTEGER,
                created_at TEXT
            );
        """)
        conn.commit()

def _to_blob(vec: np.ndarray) -> bytes:
    # float32 little-endian
    return struct.pack("<%sf" % len(vec), *vec.astype(np.float32).tolist())

def _from_blob(b: bytes) -> np.ndarray:
    n = len(b) // 4
    return np.array(struct.unpack("<%sf" % n, b), dtype=np.float32)

def _chunk_text(t: str, size: int = 900, overlap: int = 150) -> List[str]:
    t = re.sub(r"\s+", " ", t or "").strip()
    if not t:
        return []
    chunks = []
    i = 0
    while i < len(t):
        end = min(len(t), i + size)
        chunks.append(t[i:end])
        i = end - overlap
        if i < 0: i = 0
        if end == len(t): break
    return chunks[:1000]

# minimal extractors to avoid importing the host app
def _detect_mime_light(name: str) -> str:
    n = (name or "").lower()
    if n.endswith(".pdf"): return "application/pdf"
    if n.endswith(".docx"): return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if n.endswith(".xlsx"): return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if n.endswith(".txt"): return "text/plain"
    return "application/octet-stream"

def _read_bytes_to_text(name: str, b: bytes) -> str:
    mime = _detect_mime_light(name)
    if "text/plain" in mime:
        try: return b.decode("utf-8")
        except Exception: return b.decode("latin-1", errors="ignore")
    if "spreadsheetml" in mime:
        try:
            import pandas as _pd, io as _io
            x = _pd.read_excel(_io.BytesIO(b), sheet_name=None, dtype=str)
            pages = []
            for s, df in list(x.items())[:8]:
                pages.append(s + "\n" + df.fillna("").astype(str).to_csv(sep="\t", index=False))
            return "\n".join(pages)
        except Exception:
            return ""
    if "wordprocessingml" in mime or (mime=="" and b[:4]==b"PK\x03\x04"):
        try:
            import docx, io as _io
            d = docx.Document(_io.BytesIO(b))
            return "\n".join(p.text or "" for p in d.paragraphs)
        except Exception:
            return ""
    if "pdf" in mime:
        # try pypdf first
        try:
            import pypdf, io as _io
            reader = pypdf.PdfReader(_io.BytesIO(b))
            txt = "\n".join([(p.extract_text() or "") for p in reader.pages[:100]])
            if txt.strip(): return txt
        except Exception:
            pass
        # fallback to pdfplumber
        try:
            import pdfplumber, io as _io
            with pdfplumber.open(_io.BytesIO(b)) as pdf:
                return "\n".join([(pg.extract_text() or "") for pg in pdf.pages[:100]])
        except Exception:
            return ""
    # generic fallback
    try: return b.decode("utf-8", errors="ignore")
    except Exception: return ""

def _embed_texts(client, model: str, texts: List[str]) -> List[np.ndarray]:
    if not texts:
        return []
    resp = client.embeddings.create(model=model, input=texts)
    vecs = [np.array(d.embedding, dtype=np.float32) for d in resp.data]
    return vecs

def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    da = float(np.linalg.norm(a)); db = float(np.linalg.norm(b))
    if da == 0.0 or db == 0.0: return 0.0
    return float(np.dot(a, b) / (da * db))

def index_rfp(conn: sqlite3.Connection, rfp_id: int) -> Tuple[int, int]:
    """
    Build embeddings for all linked files of an RFP into ai_embeds.
    Returns (files_indexed, chunks_indexed).
    """
    ensure_ai_schema(conn)
    with closing(conn.cursor()) as cur:
        cur.execute("SELECT id, filename, bytes FROM rfp_files WHERE rfp_id=? ORDER BY id;", (int(rfp_id),))
        rows = cur.fetchall()
    if not rows:
        return (0, 0)

    client = _client()
    _, _, EMBED = _models()

    total_chunks = 0
    files_done = 0
    with closing(conn.cursor()) as cur:
        for fid, fname, b in rows:
            if not isinstance(b, (bytes, bytearray)): 
                continue
            text = _read_bytes_to_text(fname or f"file_{fid}", b)
            chunks = _chunk_text(text, size=900, overlap=150)
            if not chunks:
                continue
            vecs = _embed_texts(client, EMBED, chunks)
            for i, (t, v) in enumerate(zip(chunks, vecs)):
                cur.execute(
                    "INSERT INTO ai_embeds(rfp_id, file_id, chunk_id, filename, text, vec) VALUES(?,?,?,?,?,?);",
                    (int(rfp_id), int(fid), int(i), str(fname or f\"file_{fid}\"), t, _to_blob(v))
                )
            total_chunks += len(chunks)
            files_done += 1
        cur.execute("INSERT INTO ai_index_log(rfp_id, chunk_count, created_at) VALUES(?,?, datetime('now'));",
                    (int(rfp_id), int(total_chunks)))
        conn.commit()
    return (files_done, total_chunks)

def _top_k(conn: sqlite3.Connection, rfp_id: int, query: str, k: int = 8) -> List[Dict[str, Any]]:
    ensure_ai_schema(conn)
    client = _client()
    _, _, EMBED = _models()
    qv = _embed_texts(client, EMBED, [query])[0]

    # pull candidate rows; limit for speed
    df = pd.read_sql_query(
        "SELECT id, file_id, filename, chunk_id, text, vec FROM ai_embeds WHERE rfp_id=? ORDER BY id DESC LIMIT 5000;",
        conn, params=(int(rfp_id),)
    )
    if df.empty: 
        return []

    sims = []
    for _, r in df.iterrows():
        v = _from_blob(r["vec"])
        sims.append(_cosine(qv, v))
    df["score"] = sims
    top = df.sort_values("score", ascending=False).head(k)
    out = []
    for _, r in top.iterrows():
        out.append({
            "file_id": int(r["file_id"]),
            "filename": r["filename"],
            "chunk_id": int(r["chunk_id"]),
            "text": r["text"],
            "score": float(r["score"]),
        })
    return out

def _build_messages(rfp_meta: Dict[str, Any], query: str, passages: List[Dict[str, Any]], brief: bool = False) -> List[Dict[str, str]]:
    sys = (
        "You are a U.S. Government Contracting Officer reviewing an RFP. "
        "Answer only from the provided excerpts. "
        "Be precise, concise, and compliance-focused. "
        "When you state a fact, add bracketed citations like [filename:chunk]. "
        "If unknown from the excerpts, say you do not know."
    )
    ctx_blocks = []
    for p in passages:
        label = f"{p['filename']}:{p['chunk_id']}"
        txt = p["text"]
        ctx_blocks.append(f"[{label}] {txt}")
    ctx = "\n\n".join(ctx_blocks[:10])
    if brief:
        user = (
            f"Create an executive CO brief for this RFP based on the context.\n"
            f"Include: what it is, vehicle/type, set-aside, NAICS/PSC if present, key dates, POP/ordering period, CLIN/price structure, submission method, risks, and action items.\n"
            f"Question focus: {query}\n\n"
            f"Context:\n{ctx}"
        )
    else:
        user = f"Question: {query}\n\nContext:\n{ctx}\n\nAnswer with citations."
    return [{"role":"system","content":sys},{"role":"user","content":user}]

def ai_answer(conn: sqlite3.Connection, rfp_id: int, query: str, brief: bool = False) -> Dict[str, Any]:
    top = _top_k(conn, int(rfp_id), query, k=12)
    if not top:
        return {"answer":"No AI index found for this RFP. Click 'Build AI Index' first.","sources":[]}
    client = _client()
    FAST, HEAVY, _ = _models()
    messages = _build_messages({}, query, top, brief=brief)
    model = HEAVY if brief else FAST
    try:
        # Chat Completions API for broad compatibility
        resp = client.chat.completions.create(model=model, messages=messages, temperature=0.2)
        text = resp.choices[0].message.content.strip()
    except Exception as e:
        text = f"AI error: {e}"
    sources = [{"file":p["filename"], "chunk":int(p["chunk_id"]), "score": round(p["score"],3)} for p in top[:8]]
    return {"answer": text, "sources": sources}

# --- Streamlit panel ---
def st_x8_panel(conn: sqlite3.Connection, rfp_id: int):
    ensure_ai_schema(conn)
    st.subheader("X8 AI: CO Brief + Q&A")
    c1, c2 = st.columns([1,1])
    with c1:
        if st.button("Build AI Index", key=f"x8_idx_{rfp_id}"):
            files, chunks = index_rfp(conn, int(rfp_id))
            st.success(f"Indexed {files} file(s), {chunks} chunks. Models: fast={_models()[0]}, heavy={_models()[1]}")
    with c2:
        st.caption("Uses GPT‑5 with citations. Requires OPENAI_API_KEY in secrets/env.")
    q = st.text_input("Ask a question", key=f"x8_q_{rfp_id}")
    ask = st.button("Ask with citations", key=f"x8_ask_{rfp_id}")
    if ask and (q or "").strip():
        out = ai_answer(conn, int(rfp_id), q.strip(), brief=False)
        st.markdown(out["answer"] or "")
        if out.get("sources"):
            st.caption("Sources")
            df = pd.DataFrame(out["sources"])
            st.dataframe(df, use_container_width=True, hide_index=True)
    with st.expander("Generate CO Executive Brief", expanded=False):
        prompt = st.text_input("Optional focus (e.g., 'submission rules and page limits')", key=f"x8_brief_{rfp_id}")
        if st.button("Create Brief", key=f"x8_brief_btn_{rfp_id}"):
            out = ai_answer(conn, int(rfp_id), prompt.strip() if prompt else "Full brief", brief=True)
            st.markdown(out["answer"] or "")
            if out.get("sources"):
                st.caption("Sources")
                df = pd.DataFrame(out["sources"])
                st.dataframe(df, use_container_width=True, hide_index=True)
