
# x12_addon.py — X12 Subcontractor Finder v1
# Import in app.py after other addons:
#   import x12_addon
#
# Adds "X12 — Subcontractor Finder v1" to RFP Analyzer:
# - Seed search tokens from RFP (NAICS, keywords)
# - Import leads from CSV/XLSX
# - Score leads vs RFP using embeddings + rules
# - Edit, filter, de-duplicate, and export

import re
from contextlib import closing
import io

try:
    import streamlit as st
    import pandas as pd
    import numpy as np
except Exception as _e:
    raise

# ---------------- models / client ----------------

def _x12_models():
    try:
        m = st.secrets.get("models", {})
        chat = m.get("heavy") or st.secrets.get("x8_model") or "gpt-5"
        embed = m.get("embed") or st.secrets.get("embed_model") or "text-embedding-3-small"
        return chat, embed
    except Exception:
        return "gpt-5", "text-embedding-3-small"

def _x12_client():
    # Reuse global client if present
    try:
        return client, None  # noqa
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

# ---------------- storage ----------------

def _ensure_tables(conn):
    with closing(conn.cursor()) as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS subs_leads(
            id INTEGER PRIMARY KEY,
            rfp_id INTEGER NOT NULL,
            company TEXT,
            cage TEXT,
            duns TEXT,
            uei TEXT,
            naics TEXT,
            socio TEXT,
            capabilities TEXT,
            contact TEXT,
            email TEXT,
            phone TEXT,
            location TEXT,
            notes TEXT,
            score REAL,
            source TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_subs_rfp ON subs_leads(rfp_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_subs_email ON subs_leads(email);")
        conn.commit()

def _load_leads(conn, rid: int) -> pd.DataFrame:
    try:
        df = pd.read_sql_query(
            "SELECT id, company, cage, duns, uei, naics, socio, capabilities, contact, email, phone, location, notes, score, source "
            "FROM subs_leads WHERE rfp_id=? ORDER BY score DESC NULLS LAST, company;",
            conn, params=(int(rid),)
        )
        return df
    except Exception:
        return pd.DataFrame(columns=["id","company","cage","duns","uei","naics","socio","capabilities","contact","email","phone","location","notes","score","source"])

def _insert_rows(conn, rid: int, rows: list):
    if not rows: 
        return 0
    with closing(conn.cursor()) as cur:
        for r in rows:
            cur.execute("""INSERT INTO subs_leads(rfp_id, company, cage, duns, uei, naics, socio, capabilities, contact, email, phone, location, notes, source)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?);""",
                        (int(rid), r.get("company"), r.get("cage"), r.get("duns"), r.get("uei"),
                         r.get("naics"), r.get("socio"), r.get("capabilities"), r.get("contact"),
                         r.get("email"), r.get("phone"), r.get("location"), r.get("notes"), r.get("source")))
        conn.commit()
    return len(rows)

# ---------------- seeding ----------------

def _seed_from_rfp(conn, rid: int):
    """Return default tokens dict: naics, keywords string."""
    toks = {"naics":"", "keywords":""}
    try:
        dfm = pd.read_sql_query("SELECT key, value FROM rfp_meta WHERE rfp_id=?;", conn, params=(int(rid),))
        if dfm is not None and not dfm.empty:
            naics = dfm.loc[dfm["key"]=="naics","value"]
            if len(naics): toks["naics"] = str(naics.values[0])
    except Exception:
        pass
    # Pull top chunks from ai_index to extract keywords
    try:
        df = pd.read_sql_query("SELECT source, chunk_no, text FROM ai_index WHERE rfp_id=? LIMIT 200;", conn, params=(int(rid),))
    except Exception:
        df = None
    if df is not None and not df.empty:
        text = " ".join([str(t) for t in df["text"].tolist()])[:20000]
        # lightweight keyword extraction
        words = re.findall(r"[A-Za-z][A-Za-z\-]{3,}", text)
        stop = set(["shall","must","will","the","and","with","from","into","under","this","proposal","contract","offeror","include","including","provide","for","all","each","any","not","that","are","is","to"])
        freq = {}
        for w in words:
            wl = w.lower()
            if wl in stop: 
                continue
            freq[wl] = freq.get(wl,0)+1
        top = sorted(freq.items(), key=lambda x: -x[1])[:30]
        toks["keywords"] = ", ".join([k for k,_ in top])
    return toks

# ---------------- importers ----------------

_EXPECTED = ["company","cage","duns","uei","naics","socio","capabilities","contact","email","phone","location","notes"]

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame()
    for want in _EXPECTED:
        # find best match
        match = None
        for c0,c in cols.items():
            if c0==want or c0.replace(" ", "")==want or want in c0:
                match = c; break
        if match is not None:
            out[want] = df[match]
        else:
            out[want] = ""
    return out

def _import_csv(conn, rid: int, file):
    try:
        df = pd.read_csv(file)
    except Exception as e:
        st.error(f"CSV read failed: {e}")
        return 0
    df = _normalize_columns(df)
    rows = df.fillna("").to_dict(orient="records")
    for r in rows:
        r["source"] = "upload_csv"
    return _insert_rows(conn, rid, rows)

def _import_xlsx(conn, rid: int, file):
    try:
        df = pd.read_excel(file)
    except Exception as e:
        st.error(f"XLSX read failed: {e}")
        return 0
    df = _normalize_columns(df)
    rows = df.fillna("").to_dict(orient="records")
    for r in rows:
        r["source"] = "upload_xlsx"
    return _insert_rows(conn, rid, rows)

# ---------------- scoring ----------------

def _valid_email(e: str) -> bool:
    try:
        e = (e or "").strip()
        return bool(re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", e))
    except Exception:
        return False

def _score_leads(conn, rid: int, query_text: str, embed_model: str, client):
    df = _load_leads(conn, rid)
    if df is None or df.empty:
        return None
    base = (query_text or "").strip()
    # Build default query from seed if empty
    if not base:
        seed = _seed_from_rfp(conn, rid)
        base = f"NAICS {seed.get('naics','')}; keywords: {seed.get('keywords','')}"
    # Embed query
    try:
        emq = client.embeddings.create(model=embed_model, input=[base])
        import numpy as _np
        qv = _np.array(emq.data[0].embedding, dtype=_np.float32)
    except Exception:
        emq = client.Embedding.create(model=embed_model, input=[base])
        import numpy as _np
        qv = _np.array(emq["data"][0]["embedding"], dtype=_np.float32)

    # Build candidate vectors from capabilities+notes
    import numpy as _np
    texts = (df["capabilities"].fillna("") + " " + df["notes"].fillna("")).tolist()
    # chunk to avoid token limits
    vecs = []
    step = 64
    for i in range(0, len(texts), step):
        batch = texts[i:i+step]
        try:
            em = client.embeddings.create(model=embed_model, input=batch)
            vecs += [ _np.array(e.embedding, dtype=_np.float32) for e in em.data ]
        except Exception:
            em = client.Embedding.create(model=embed_model, input=batch)
            vecs += [ _np.array(e["embedding"], dtype=_np.float32) for e in em["data"] ]

    M = _np.vstack(vecs) if vecs else _np.zeros((len(texts), len(qv)), dtype=_np.float32)
    sims = (M @ qv) / (_np.linalg.norm(M, axis=1) * (float(_np.linalg.norm(qv))+1e-9) + 1e-9)
    sims = sims.tolist()
    # rule bonuses
    naics_hint = re.findall(r"\b(\d{6})\b", base)
    naics_hint = set(naics_hint)
    bonuses = []
    for i, row in df.iterrows():
        b = 0.0
        # NAICS exact match bonus
        row_naics = re.findall(r"\b(\d{6})\b", str(row.get("naics") or ""))
        if naics_hint and set(row_naics) & naics_hint:
            b += 0.2
        # socio preference if any in query_text
        if re.search(r"\b8\(a\)\b|\bsdvosb\b|\bwosb\b|\bhubzone\b|\bveteran", base, re.I):
            if re.search(r"\b8\(a\)|sdvosb|wosb|hubzone|veteran", str(row.get("socio") or ""), re.I):
                b += 0.15
        # email present bonus
        if _valid_email(str(row.get("email") or "")):
            b += 0.05
        bonuses.append(b)
    scores = [float(s) + float(b) for s,b in zip(sims, bonuses)]
    df2 = df.copy()
    df2["score"] = scores
    # persist scores
    with closing(conn.cursor()) as cur:
        for i, r in df2.iterrows():
            cur.execute("UPDATE subs_leads SET score=? WHERE id=?;", (float(r["score"]), int(r["id"])))
        conn.commit()
    return df2.sort_values(["score"], ascending=False).reset_index(drop=True)

# ---------------- UI ----------------

def _x12_ui(conn):
    _ensure_tables(conn)
    st.markdown("### X12 — Subcontractor Finder v1")
    st.caption("X12 active — seed from RFP, import CSV/XLSX, score vs scope, export")
    try:
        _rfps = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
    except Exception:
        _rfps = None
    if _rfps is None or _rfps.empty:
        st.info("No RFP found. Parse & save first.")
        return
    rid = st.selectbox("RFP context (for X12)", options=_rfps["id"].tolist(),
                       format_func=lambda i: f"#{i} — {_rfps.loc[_rfps['id']==i,'title'].values[0]}",
                       key="x12_rid")

    tabs = st.tabs(["Seed", "Import", "Leads", "Score & Filter", "Export"])

    with tabs[0]:
        seed = _seed_from_rfp(conn, int(rid))
        st.text_input("NAICS", value=seed.get("naics",""), key=f"x12_naics_{rid}")
        st.text_area("Keywords", value=seed.get("keywords",""), height=120, key=f"x12_kw_{rid}")
        st.caption("Edit the tokens if needed. These feed the scoring function.")

    with tabs[1]:
        st.write("Upload a CSV or XLSX with columns like: company, cage, duns, uei, naics, socio, capabilities, contact, email, phone, location, notes.")
        up_csv = st.file_uploader("Upload CSV", type=["csv"], key=f"x12_up_csv_{rid}")
        if up_csv is not None and st.button("Import CSV", key=f"x12_imp_csv_{rid}"):
            n = _import_csv(conn, int(rid), up_csv)
            st.success(f"Imported {n} rows from CSV.")
        up_xlsx = st.file_uploader("Upload XLSX", type=["xlsx","xls"], key=f"x12_up_x_{rid}")
        if up_xlsx is not None and st.button("Import XLSX", key=f"x12_imp_x_{rid}"):
            n = _import_xlsx(conn, int(rid), up_xlsx)
            st.success(f"Imported {n} rows from XLSX.")

    with tabs[2]:
        df = _load_leads(conn, int(rid))
        if df is None or df.empty:
            st.info("No leads yet. Import first.")
        else:
            st.caption("Edit leads inline. De-duplicate by company or email before scoring.")
            df_edit = st.data_editor(df, use_container_width=True, hide_index=True, num_rows="dynamic")
            # persist simple edits back
            if st.button("Save edits", key=f"x12_save_{rid}"):
                with closing(conn.cursor()) as cur:
                    for _, r in df_edit.iterrows():
                        cur.execute("""UPDATE subs_leads 
                                       SET company=?, cage=?, duns=?, uei=?, naics=?, socio=?, capabilities=?, contact=?, email=?, phone=?, location=?, notes=? 
                                       WHERE id=?;""",
                                    (r.get("company"), r.get("cage"), r.get("duns"), r.get("uei"), r.get("naics"),
                                     r.get("socio"), r.get("capabilities"), r.get("contact"), r.get("email"),
                                     r.get("phone"), r.get("location"), r.get("notes"), int(r.get("id"))))
                    conn.commit()
                st.success("Saved.")

    with tabs[3]:
        client, err = _x12_client()
        if err or client is None:
            st.error(err or "OpenAI client error")
        else:
            df = _load_leads(conn, int(rid))
            if df is None or df.empty:
                st.info("No leads to score.")
            else:
                kw = st.text_area("Scoring query (defaults to NAICS + keywords from Seed)", value="", key=f"x12_q_{rid}")
                if st.button("Score leads", key=f"x12_score_{rid}"):
                    chat_model, embed_model = _x12_models()
                    scored = _score_leads(conn, int(rid), kw, embed_model, client)
                    if scored is None or scored.empty:
                        st.warning("Nothing to score.")
                    else:
                        # quick filters
                        min_score = st.slider("Min score", 0.0, 1.5, 0.3, 0.05, key=f"x12_min_{rid}")
                        show = scored[scored["score"]>=min_score].reset_index(drop=True)
                        st.dataframe(show, use_container_width=True, hide_index=True)
                        st.caption("Higher score = better match to RFP scope and NAICS.")

    with tabs[4]:
        df = _load_leads(conn, int(rid))
        if df is None or df.empty:
            st.info("No leads to export.")
        else:
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button("Download leads CSV", data=csv, file_name=f"rfp_{rid}_sub_leads.csv", mime="text/csv", key=f"x12_csv_{rid}")
            try:
                import pandas as _pd
                import io
                buf = io.BytesIO()
                with _pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                    df.to_excel(writer, sheet_name="Leads", index=False)
                    ws = writer.sheets["Leads"]
                    for i, col in enumerate(df.columns):
                        width = max(12, min(60, int(df[col].astype(str).str.len().quantile(0.9)) + 4))
                        ws.set_column(i, i, width)
                buf.seek(0)
                st.download_button("Download leads XLSX", data=buf.read(), file_name=f"rfp_{rid}_sub_leads.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"x12_xlsx_{rid}")
            except Exception as e:
                st.info(f"XLSX export unavailable: {e}")

# Hook into existing analyzer
try:
    _orig_run_rfp_analyzer = run_rfp_analyzer
except NameError:
    _orig_run_rfp_analyzer = None

if _orig_run_rfp_analyzer:
    def run_rfp_analyzer(conn):
        _orig_run_rfp_analyzer(conn)
        try:
            _x12_ui(conn)
        except Exception as e:
            st.info(f"X12 panel unavailable: {e}")
