
# x84_addon.py — X8.4 CO Brief + Q&A (strict) addon
# Place this file next to app.py and import it once (e.g., `import x84_addon`).
# It monkey-patches run_rfp_analyzer to append the X8.4 panel without editing your app.

try:
    import streamlit as st
    import pandas as pd
    import numpy as np
    from contextlib import closing
except Exception as _e:
    raise

def _x84_models():
    try:
        m = st.secrets.get("models", {})
        chat = m.get("heavy") or st.secrets.get("x8_model") or "gpt-5"
        embed = m.get("embed") or st.secrets.get("embed_model") or "text-embedding-3-small"
        return chat, embed
    except Exception:
        return "gpt-5", "text-embedding-3-small"

def _x84_build_index(conn, rid, df_files, embed_model, client):
    rows = []
    for _, r in df_files.iterrows():
        name = r.get("filename") or "RFP"
        text = r.get("text") or ""
        buf, cur_len, idx = [], 0, 0
        for line in str(text).splitlines():
            if cur_len + len(line) + 1 > 1600 and buf:
                rows.append({"source": name, "chunk_no": idx+1, "text": "\n".join(buf)})
                buf, cur_len, idx = [], 0, idx+1
            buf.append(line)
            cur_len += len(line) + 1
        if buf:
            rows.append({"source": name, "chunk_no": idx+1, "text": "\n".join(buf)})
    if not rows:
        return 0, "no rows"

    try:
        em = client.embeddings.create(model=embed_model, input=[r["text"] for r in rows])
        vecs = [ np.array(e.embedding, dtype=np.float32) for e in em.data ]
    except Exception:
        em = client.Embedding.create(model=embed_model, input=[r["text"] for r in rows])
        vecs = [ np.array(e["embedding"], dtype=np.float32) for e in em["data"] ]

    with closing(conn.cursor()) as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS ai_index(
            id INTEGER PRIMARY KEY,
            rfp_id INTEGER NOT NULL,
            source TEXT,
            chunk_no INTEGER,
            text TEXT,
            embedding BLOB,
            dim INTEGER,
            created_at TEXT
        );""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_index_rfp ON ai_index(rfp_id);")
        cur.execute("DELETE FROM ai_index WHERE rfp_id=?;", (int(rid),))
        dim = int(vecs[0].shape[0])
        for r, v in zip(rows, vecs):
            cur.execute("INSERT INTO ai_index(rfp_id, source, chunk_no, text, embedding, dim, created_at) VALUES(?,?,?,?,?,?, datetime('now'));",
                        (int(rid), r["source"], int(r["chunk_no"]), r["text"], v.tobytes(), dim))
        conn.commit()
    return len(rows), None

def _x84_facts(conn, rid):
    facts = []
    try:
        ddf = pd.read_sql_query("SELECT label, date_text FROM key_dates WHERE rfp_id=?;", conn, params=(int(rid),))
        due = ddf[ddf["label"].str.contains("due", case=False, na=False)]
        if due is not None and not due.empty:
            facts.append(f"Due date: {due.iloc[0]['date_text']}")
    except Exception:
        pass
    try:
        meta = pd.read_sql_query("SELECT key, value FROM rfp_meta WHERE rfp_id=?;", conn, params=(int(rid),))
        def gv(k):
            try:
                v = meta.loc[meta["key"]==k,"value"]
                return v.values[0] if len(v) else ""
            except Exception:
                return ""
        naics = gv("naics"); setaside = gv("set_aside"); pop = gv("pop_summary") or gv("pop_structure")
        if naics: facts.append(f"NAICS: {naics}")
        if setaside: facts.append(f"Set-Aside: {setaside}")
        if pop: facts.append(f"POP: {pop}")
    except Exception:
        pass
    try:
        p = pd.read_sql_query("SELECT name, role, email, phone FROM pocs WHERE rfp_id=?;", conn, params=(int(rid),))
        if p is not None and not p.empty:
            p = p.fillna("")
            def _dom(e):
                try: return (e or "").split("@",1)[1].lower()
                except Exception: return ""
            p["domain"] = p["email"].apply(_dom)
            gov = p["domain"].str.contains(r"\.(gov|mil)$", case=False, regex=True).astype(int)
            rolew = p["role"].str.contains(r"contract(ing)? officer|\bko\b|contract specialist|\bcor\b", case=False, regex=True).astype(int) * 2
            freq = p["domain"].map(p["domain"].value_counts().to_dict())
            bop = p["domain"].str.contains(r"\bbop\.gov\b", case=False, regex=True).astype(int) * 3
            doj = p["domain"].str.contains(r"(justice|usdoj)\.gov", case=False, regex=True).astype(int) * 2
            p["score"] = gov + rolew + freq + bop + doj
            r = p.sort_values(["score"], ascending=False).iloc[0]
            facts.append(f"Primary POC: {(r.get('name') or '').strip()} — {(r.get('role') or 'POC').strip()} — {(r.get('email') or '').strip()} — {(r.get('phone') or '').strip()}")
    except Exception:
        pass
    return facts

def _x84_qna(conn, rid, q, client, chat_model, embed_model):
    import pandas as _pd
    df = _pd.read_sql_query("SELECT source, chunk_no, text, embedding, dim FROM ai_index WHERE rfp_id=?;", conn, params=(int(rid),))
    if df is None or df.empty:
        st.warning("No index. Click Build AI Index first.")
        return
    # embed query
    try:
        emq = client.embeddings.create(model=embed_model, input=[q])
        import numpy as _np
        qv = _np.array(emq.data[0].embedding, dtype=_np.float32)
    except Exception:
        emq = client.Embedding.create(model=embed_model, input=[q])
        import numpy as _np
        qv = _np.array(emq["data"][0]["embedding"], dtype=_np.float32)
    import numpy as _np
    M = _np.vstack([_np.frombuffer(b, dtype=_np.float32, count=int(d)) for b,d in zip(df["embedding"], df["dim"])])
    sims = (M @ qv) / (_np.linalg.norm(M, axis=1) * (float(_np.linalg.norm(qv))+1e-9))
    order = sims.argsort()[::-1][:8]
    hits = []
    for i in order:
        row = df.iloc[int(i)]
        hits.append({"text": str(row["text"])[:4000], "meta": {"source": str(row["source"]), "ref": f"p.{int(row['chunk_no'])}"}, "score": float(sims[int(i)])})
    ctx = "\\n\\n".join([f"[{h['meta']['source']} {h['meta']['ref']}]\\n{h['text']}" for h in hits])

    facts_text = "\\n".join(f"- {x}" for x in _x84_facts(conn, rid)) or "- Not found."
    system_prompt = ("You are a senior U.S. Government Contracting Officer. Use only the FACTS and CONTEXT. "
                     "Cite each factual sentence with [filename p.X]. If a field is not in FACTS/CONTEXT, say 'not found'. "
                     "Show exactly one primary POC in Answer; put others in Sources.")
    user_prompt = (
        "FACTS:\\n" + facts_text + "\\n\\n"
        "CONTEXT:\\n" + ctx + "\\n\\n"
        "Write the response in these sections, exactly in order:\\n"
        "- Answer (<=220 words)\\n"
        "- Key requirements (must/shall)\\n"
        "- Risks / Red flags\\n"
        "- Unknowns / Questions for KO\\n"
        "- Next actions\\n"
        "- Sources\\n\\n"
        "Rules:\\n"
        "- Cite with [filename p.X] that matches the labels in CONTEXT.\\n"
        "- Use ISO dates (YYYY-MM-DD) when possible.\\n"
        "- Summarize POP as Base + Options when present."
    )
    try:
        resp = client.chat.completions.create(
            model=chat_model,
            messages=[
                {"role":"system","content": system_prompt},
                {"role":"user","content": user_prompt},
            ],
        )
        ans = resp.choices[0].message.content
        st.success("Answer")
        st.write(ans)
        rows = [{"#": i+1, "Source": f"{h['meta']['source']} {h['meta']['ref']}", "Score": round(h["score"],4)} for i,h in enumerate(hits)]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    except Exception as ex:
        st.error(f"Chat failed: {ex}")

def _x84_brief(conn, rid, client, chat_model):
    import pandas as _pd
    df = _pd.read_sql_query("SELECT source, chunk_no, text FROM ai_index WHERE rfp_id=?;", conn, params=(int(rid),))
    if df is None or df.empty:
        st.warning("No index. Click Build AI Index first.")
        return
    hits = [{"text": str(df.iloc[i]["text"]), "meta": {"source": str(df.iloc[i]["source"]), "ref": f"p.{int(df.iloc[i]['chunk_no'])}"}} for i in range(min(12, len(df)))]
    ctx_b = "\\n\\n".join([f"[{h['meta']['source']} {h['meta']['ref']}]\\n{h['text']}" for h in hits])
    brief_system = "You prepare precise, compliance-aware CO briefs. Use only the provided context. Cite [filename p.X]."
    brief_user = (
        "Create a one-page CO executive brief. Include sections: "
        "Solicitation Overview; Set-Aside & NAICS; Key Dates; POP/Ordering Period; CLIN/Price Structure; "
        "Submission Instructions; Primary POC; Compliance Must-Dos; Risks. "
        "Use bullets and cite each factual sentence with [filename p.X].\\n\\n"
        "CONTEXT:\\n" + ctx_b
    )
    try:
        resp = client.chat.completions.create(
            model=chat_model,
            messages=[
                {"role":"system","content": brief_system},
                {"role":"user","content": brief_user},
            ],
        )
        doc = resp.choices[0].message.content
        st.success("CO Brief")
        st.write(doc)
    except Exception as ex:
        st.error(f"Brief failed: {ex}")

# Monkey-patch run_rfp_analyzer
try:
    _orig_run_rfp_analyzer = run_rfp_analyzer
except NameError:
    _orig_run_rfp_analyzer = None

if _orig_run_rfp_analyzer:
    def run_rfp_analyzer(conn):
        _orig_run_rfp_analyzer(conn)
        # Render X8.4
        st.markdown("### X8.4 — CO Brief + Q&A (strict)")
        st.caption("X8.4 active — page-anchored citations + strict answer template")
        try:
            _rfps = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
        except Exception:
            _rfps = None
        rid = None
        if _rfps is None or _rfps.empty:
            st.info("No RFP found. Parse & save first.")
            return
        rid = st.selectbox("RFP context (for X8.4)", options=_rfps["id"].tolist(),
                           format_func=lambda i: f"#{i} — {_rfps.loc[_rfps['id']==i,'title'].values[0]}",
                           key="x84_rid_main")
        chat_model, embed_model = _x84_models()
        # Access global OpenAI client from app
        try:
            client  # noqa
        except NameError:
            from openai import OpenAI  # type: ignore
            import os as _os
            _key = st.secrets.get("openai",{}).get("api_key") or st.secrets.get("OPENAI_API_KEY") or _os.getenv("OPENAI_API_KEY")
            globals()["client"] = OpenAI(api_key=_key)

        c1, c2, c3 = st.columns([2,2,2])
        with c1:
            x84_build = st.button("Build AI Index", key=f"x84_build_{rid}")
        with c2:
            x84_ask = st.button("Answer with citations", key=f"x84_ask_{rid}")
        with c3:
            x84_brief = st.button("Generate CO Brief", key=f"x84_brief_{rid}")
        x84_q = st.text_input("Your question", key=f"x84_q_{rid}", placeholder="e.g., What are the due date, submission method, and POP?")

        if x84_build:
            try:
                df_files = pd.read_sql_query("SELECT filename, text FROM files WHERE rfp_id=? ORDER BY id;", conn, params=(int(rid),))
            except Exception:
                df_files = None
            if df_files is None or df_files.empty:
                st.warning("No file text available for this RFP.")
            else:
                n, err = _x84_build_index(conn, rid, df_files, embed_model, client)
                if err:
                    st.error(f"Index error: {err}")
                else:
                    st.success(f\"Indexed {n} chunks for RFP #{rid}.\")

        if x84_ask and (x84_q or '').strip():
            _x84_qna(conn, rid, x84_q, client, chat_model, embed_model)

        if x84_brief:
            _x84_brief(conn, rid, client, chat_model)
