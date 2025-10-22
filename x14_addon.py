
# x14_addon.py — X14 SAM Watch v1
# Import in app.py after other addons:
#   import x14_addon
#
# Features:
# - Import SAM.gov opportunity CSV/XLSX exports
# - Normalize columns and store in sqlite (sam_opps)
# - Simple relevance scoring by NAICS + keyword embeddings
# - Create RFP shell record from a row

import re, io
from contextlib import closing

try:
    import streamlit as st
    import pandas as pd
    import numpy as np
except Exception as _e:
    raise

def _x14_models():
    try:
        m = st.secrets.get("models", {})
        embed = m.get("embed") or st.secrets.get("embed_model") or "text-embedding-3-small"
        return embed
    except Exception:
        return "text-embedding-3-small"

def _x14_client():
    try:
        return client, None  # reuse
    except NameError:
        pass
    try:
        from openai import OpenAI  # type: ignore
        import os as _os
        _key = st.secrets.get("openai",{}).get("api_key") or st.secrets.get("OPENAI_API_KEY") or _os.getenv("OPENAI_API_KEY")
        if not _key:
            return None, "OpenAI API key missing"
        c = OpenAI(api_key=_key)
        globals()["client"] = c
        return c, None
    except Exception as e:
        return None, f"OpenAI init failed: {e}"

_EXPECT = {
    "notice id":"source_id",
    "noticeid":"source_id",
    "notice title":"title",
    "title":"title",
    "solicitation number":"sol_number",
    "sol number":"sol_number",
    "solicitation":"sol_number",
    "notice type":"type",
    "response deadline":"due",
    "response date":"due",
    "publish date":"posted",
    "posted date":"posted",
    "naics code":"naics",
    "naics":"naics",
    "set aside":"set_aside",
    "department/ind agency":"agency",
    "department/ind. agency":"agency",
    "department":"agency",
    "office":"office",
    "url":"url",
    "link":"url",
    "keywords":"keywords",
}

def _x14_norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    low = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame()
    for k,v in _EXPECT.items():
        if k in low:
            out[v] = df[low[k]]
    # ensure columns
    for v in set(_EXPECT.values()):
        if v not in out.columns:
            out[v] = ""
    return out

def _x14_tables(conn):
    with closing(conn.cursor()) as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS sam_opps(
            id INTEGER PRIMARY KEY,
            source_id TEXT,
            posted TEXT,
            due TEXT,
            agency TEXT,
            office TEXT,
            title TEXT,
            sol_number TEXT,
            naics TEXT,
            set_aside TEXT,
            type TEXT,
            url TEXT,
            keywords TEXT,
            score REAL,
            created_at TEXT DEFAULT (datetime('now'))
        );""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sam_source ON sam_opps(source_id);")
        conn.commit()

def _x14_import_csv(conn, file):
    try:
        df = pd.read_csv(file)
    except Exception as e:
        st.error(f"CSV read failed: {e}")
        return 0
    df = _x14_norm_cols(df).fillna("")
    rows = df.to_dict(orient="records")
    with closing(conn.cursor()) as cur:
        for r in rows:
            cur.execute("""INSERT INTO sam_opps(source_id, posted, due, agency, office, title, sol_number, naics, set_aside, type, url, keywords)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?);""",
                        (r.get("source_id"), r.get("posted"), r.get("due"), r.get("agency"), r.get("office"),
                         r.get("title"), r.get("sol_number"), r.get("naics"), r.get("set_aside"),
                         r.get("type"), r.get("url"), r.get("keywords")))
        conn.commit()
    return len(rows)

def _x14_import_xlsx(conn, file):
    try:
        df = pd.read_excel(file)
    except Exception as e:
        st.error(f"XLSX read failed: {e}")
        return 0
    df = _x14_norm_cols(df).fillna("")
    rows = df.to_dict(orient="records")
    with closing(conn.cursor()) as cur:
        for r in rows:
            cur.execute("""INSERT INTO sam_opps(source_id, posted, due, agency, office, title, sol_number, naics, set_aside, type, url, keywords)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?);""",
                        (r.get("source_id"), r.get("posted"), r.get("due"), r.get("agency"), r.get("office"),
                         r.get("title"), r.get("sol_number"), r.get("naics"), r.get("set_aside"),
                         r.get("type"), r.get("url"), r.get("keywords")))
        conn.commit()
    return len(rows)

def _x14_score(conn, query: str):
    embed_model = _x14_models()
    cli, err = _x14_client()
    if err or cli is None:
        st.error(err or "OpenAI unavailable")
        return
    try:
        df = pd.read_sql_query("SELECT id, title, naics, set_aside, keywords FROM sam_opps ORDER BY id DESC LIMIT 1000;", conn, params=())
    except Exception as e:
        st.error(f"Load failed: {e}")
        return
    if df is None or df.empty:
        st.info("No opportunities to score.")
        return
    base = query or ""
    try:
        emq = cli.embeddings.create(model=embed_model, input=[base])
        import numpy as _np
        qv = _np.array(emq.data[0].embedding, dtype=_np.float32)
    except Exception:
        emq = cli.Embedding.create(model=embed_model, input=[base])
        import numpy as _np
        qv = _np.array(emq["data"][0]["embedding"], dtype=_np.float32)

    # batch embed texts
    texts = (df["title"].fillna("") + " " + df["naics"].fillna("") + " " + df["set_aside"].fillna("") + " " + df["keywords"].fillna("")).tolist()
    vecs = []
    step = 64
    for i in range(0, len(texts), step):
        batch = texts[i:i+step]
        try:
            em = cli.embeddings.create(model=embed_model, input=batch)
            vecs += [ _np.array(e.embedding, dtype=_np.float32) for e in em.data ]
        except Exception:
            em = cli.Embedding.create(model=embed_model, input=batch)
            vecs += [ _np.array(e["embedding"], dtype=_np.float32) for e in em["data"] ]
    M = _np.vstack(vecs)
    sims = (M @ qv) / (_np.linalg.norm(M, axis=1) * (float(_np.linalg.norm(qv))+1e-9))
    # persist
    df2 = df.copy()
    df2["score"] = sims
    with closing(conn.cursor()) as cur:
        for _, r in df2.iterrows():
            cur.execute("UPDATE sam_opps SET score=? WHERE id=?;", (float(r["score"]), int(r["id"])))
        conn.commit()
    st.success(f"Scored {len(df2)} opportunities.")
    return df2.sort_values(["score"], ascending=False).reset_index(drop=True)

def _x14_make_rfp(conn, row: dict) -> int:
    # Create a shell RFP record
    try:
        with closing(conn.cursor()) as cur:
            cur.execute("""INSERT INTO rfps(title, agency, sol_number, created_at) VALUES(?,?,?, datetime('now'));""",
                        (row.get("title") or "", row.get("agency") or "", row.get("sol_number") or ""))
            rid = cur.lastrowid
            # store meta
            cur.execute("CREATE TABLE IF NOT EXISTS rfp_meta(id INTEGER PRIMARY KEY, rfp_id INTEGER, key TEXT, value TEXT);")
            for k, v in [("naics", row.get("naics","")), ("set_aside", row.get("set_aside","")), ("source_url", row.get("url",""))]:
                cur.execute("INSERT INTO rfp_meta(rfp_id, key, value) VALUES(?,?,?);", (int(rid), k, v))
            conn.commit()
            return int(rid)
    except Exception:
        return 0

def _x14_ui(conn):
    _x14_tables(conn)
    st.markdown("### X14 — SAM Watch v1")
    st.caption("X14 active — import SAM CSV/XLSX, score relevance, create RFP shell")
    tabs = st.tabs(["Import", "List & Score", "Create RFP"])

    with tabs[0]:
        up_csv = st.file_uploader("Upload SAM CSV", type=["csv"], key="x14_csv")
        if up_csv is not None and st.button("Import CSV", key="x14_ic"):
            n = _x14_import_csv(conn, up_csv)
            st.success(f"Imported {n} rows.")
        up_xlsx = st.file_uploader("Upload SAM XLSX", type=["xlsx","xls"], key="x14_x")
        if up_xlsx is not None and st.button("Import XLSX", key="x14_ix"):
            n = _x14_import_xlsx(conn, up_xlsx)
            st.success(f"Imported {n} rows.")

    with tabs[1]:
        try:
            df = pd.read_sql_query("SELECT id, posted, due, agency, title, sol_number, naics, set_aside, type, url, score FROM sam_opps ORDER BY COALESCE(score,0) DESC, id DESC LIMIT 500;", conn, params=())
        except Exception as e:
            df = None
            st.error(f"Load failed: {e}")
        if df is None or df.empty:
            st.info("No opportunities yet.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
            q = st.text_input("Scoring query (e.g., NAICS 561720; janitorial; IDIQ)")
            if st.button("Score relevance", key="x14_sc"):
                out = _x14_score(conn, q or "")
                if out is not None:
                    st.dataframe(out[["id","score","title","agency","naics","set_aside","due"]], use_container_width=True, hide_index=True)

    with tabs[2]:
        try:
            df = pd.read_sql_query("SELECT id, agency, title, sol_number, naics, set_aside, url FROM sam_opps ORDER BY id DESC LIMIT 300;", conn, params=())
        except Exception as e:
            df = None
            st.error(f"Load failed: {e}")
        if df is None or df.empty:
            st.info("No rows.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
            sel = st.number_input("sam_opps.id to create RFP shell", min_value=0, step=1, value=0)
            if st.button("Create RFP", key="x14_make"):
                if sel>0:
                    row = df[df["id"]==sel].iloc[0].to_dict()
                    rid = _x14_make_rfp(conn, row)
                    if rid:
                        st.success(f"RFP shell created: #{rid}")
                    else:
                        st.error("Create failed.")

# Hook into existing analyzer
try:
    _orig_run_rfp_analyzer = run_rfp_analyzer
except NameError:
    _orig_run_rfp_analyzer = None

if _orig_run_rfp_analyzer:
    def run_rfp_analyzer(conn):
        _orig_run_rfp_analyzer(conn)
        try:
            _x14_ui(conn)
        except Exception as e:
            st.info(f"X14 panel unavailable: {e}")
