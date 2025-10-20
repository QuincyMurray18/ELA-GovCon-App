# ==== X8 AI: CO Brief + Q&A — HOTFIX (drop-in) ====
# Paste this whole block into app.py replacing the existing X8 panel section.
# It fixes NameError by keeping all UI logic inside st_x8_panel and avoids top-level references.

from contextlib import closing

def _x8_get_openai_client():
    try:
        # New SDK
        from openai import OpenAI  # type: ignore
        key = None
        try:
            key = st.secrets.get("openai", {}).get("api_key") or st.secrets.get("OPENAI_API_KEY")
        except Exception:
            pass
        import os as _os
        key = key or _os.getenv("OPENAI_API_KEY")
        if not key:
            return None, "Missing OPENAI_API_KEY in secrets/env"
        client = OpenAI(api_key=key)
        return client, None
    except Exception as e:
        # Legacy fallback
        try:
            import openai  # type: ignore
            key = None
            try:
                key = st.secrets.get("openai", {}).get("api_key") or st.secrets.get("OPENAI_API_KEY")
            except Exception:
                pass
            import os as _os
            key = key or _os.getenv("OPENAI_API_KEY")
            if not key:
                return None, "Missing OPENAI_API_KEY in secrets/env"
            openai.api_key = key
            return openai, None
        except Exception as e2:
            return None, f"OpenAI SDK not available: {e2}"

def _x8_models():
    try:
        m = st.secrets.get("models", {})
        heavy = m.get("heavy") or "gpt-5"
        fast = m.get("fast") or "gpt-5-mini"
        embed = m.get("embed") or "text-embedding-3-small"
        return {"chat": heavy, "embed": embed}
    except Exception:
        return {"chat": "gpt-5", "embed": "text-embedding-3-small"}

def _x8_np():
    try:
        import numpy as np  # type: ignore
        return np, None
    except Exception as e:
        return None, f"numpy not available: {e}"

def st_x8_panel(conn, rfp_id: int):
    import streamlit as _st, pandas as _pd, re
    _st.subheader("X8 AI: CO Brief + Q&A")
    _st.caption("X8.2 active — deterministic POC chooser + stricter citations")
    _st.caption("Indexes linked files and saved artifacts. Answers with citations.")

    # Ensure index table
    try:
        with closing(conn.cursor()) as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ai_index(
                    id INTEGER PRIMARY KEY,
                    rfp_id INTEGER NOT NULL,
                    source TEXT,
                    chunk_no INTEGER,
                    text TEXT,
                    embedding BLOB,
                    dim INTEGER,
                    created_at TEXT
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_index_rfp ON ai_index(rfp_id);")
            conn.commit()
    except Exception as e:
        _st.info(f"AI index unavailable: {e}")
        return

    c1, c2, c3 = _st.columns([2,2,2])
    with c1:
        rebuild = _st.button("Build AI Index", key=f"x8_build_{rfp_id}")
    with c2:
        ask = _st.button("Answer with citations", key=f"x8_ask_{rfp_id}")
    with c3:
        brief = _st.button("Generate CO Executive Brief", key=f"x8_brief_{rfp_id}")
    q = _st.text_input("Your question", key=f"x8_q_{rfp_id}", placeholder="e.g., What are the submission requirements and due dates?")

    models = _x8_models()

    # Index rebuild
    if rebuild:
        client, err = _x8_get_openai_client()
        if err or client is None:
            _st.error(err or "OpenAI client error"); return
        rows = []
        try:
            # gather from db
            df_files = _pd.read_sql_query("SELECT id, filename, text FROM files WHERE rfp_id=? ORDER BY id;", conn, params=(int(rfp_id),))
            for _, r in df_files.iterrows():
                name = r.get("filename") or "RFP"
                text = r.get("text") or ""
                # chunk per ~1600 chars by lines
                buf, cur_len, idx = [], 0, 0
                for line in str(text).splitlines():
                    if cur_len + len(line) + 1 > 1600 and buf:
                        rows.append({"source": f"{name}:{idx+1}", "text": "\n".join(buf)})
                        buf, cur_len, idx = [], 0, idx+1
                    buf.append(line)
                    cur_len += len(line) + 1
                if buf:
                    rows.append({"source": f"{name}:{idx+1}", "text": "\n".join(buf)})
        except Exception as e:
            _st.error(f"Gather failed: {e}"); return

        if not rows:
            _st.warning("No text to index for this RFP."); return

        # Embeddings
        np, np_err = _x8_np()
        if np is None:
            _st.error(np_err); return
        try:
            try:
                from openai import OpenAI  # type: ignore
                em = client.embeddings.create(model=models["embed"], input=[r["text"] for r in rows])
                vecs = [np.array(e.embedding, dtype=np.float32) for e in em.data]
            except Exception:
                # legacy
                em = client.Embedding.create(model=models["embed"], input=[r["text"] for r in rows])
                vecs = [np.array(e["embedding"], dtype=np.float32) for e in em["data"]]
        except Exception as e:
            _st.error(f"Embedding failed: {e}"); return

        dim = int(vecs[0].shape[0]) if vecs else 0
        try:
            with closing(conn.cursor()) as cur:
                cur.execute("DELETE FROM ai_index WHERE rfp_id=?;", (int(rfp_id),))
                for r, v in zip(rows, vecs):
                    src = r["source"]
                    base, chunk_no = src.split(":")[0], int(src.split(":")[-1])
                    cur.execute(
                        "INSERT INTO ai_index(rfp_id, source, chunk_no, text, embedding, dim, created_at) VALUES(?,?,?,?,?,?, datetime('now'));",
                        (int(rfp_id), base, chunk_no, r["text"], v.tobytes(), dim)
                    )
                conn.commit()
            _st.success(f"Indexed {len(vecs)} chunks using {models['embed']}.")
        except Exception as e:
            _st.error(f"Index write failed: {e}"); return

    # Q&A
    if ask and (q or "").strip():
        client, err = _x8_get_openai_client()
        if err or client is None:
            _st.error(err or "OpenAI client error"); return
        np, np_err = _x8_np()
        if np is None:
            _st.error(np_err); return
        df = _pd.read_sql_query("SELECT source, chunk_no, text, embedding, dim FROM ai_index WHERE rfp_id=?;", conn, params=(int(rfp_id),))
        if df is None or df.empty:
            _st.warning("No AI index for this RFP. Click Build AI Index first."); return
        # embed query
        try:
            try:
                from openai import OpenAI  # type: ignore
                emq = client.embeddings.create(model=models["embed"], input=[q])
                qvec = np.array(emq.data[0].embedding, dtype=np.float32)
            except Exception:
                emq = client.Embedding.create(model=models["embed"], input=[q])
                qvec = np.array(emq["data"][0]["embedding"], dtype=np.float32)
        except Exception as e:
            _st.error(f"Query embedding failed: {e}"); return
        # cosine sims
        def _frombuf(b, dim):
            try:
                return np.frombuffer(b, dtype=np.float32, count=int(dim))
            except Exception:
                return None
        emb = [_frombuf(b, d) for b, d in zip(df["embedding"], df["dim"])]
        mask = [v is not None for v in emb]
        if not any(mask):
            _st.error("Embeddings unavailable. Rebuild the index."); return
        M = np.vstack([v for v, m in zip(emb, mask) if m])
        sims = (M @ qvec) / (np.linalg.norm(M, axis=1) * (np.linalg.norm(qvec) + 1e-9))
        top_idx = np.argsort(-sims)[:8].tolist()
        hits = []
        for i in top_idx:
            row = df.iloc[i]
            hits.append({
                "text": str(row["text"])[:4000],
                "meta": {"source": str(row["source"]), "ref": f"p{int(row['chunk_no'])}"},
                "score": float(sims[i])
            })
        ctx = "\n\n".join([f"[{i+1}] {h['meta']['source']} {h['meta']['ref']}\n{h['text']}" for i,h in enumerate(hits)])
        prompt = (
            "You are a US government Contracting Officer assistant. "
            "Answer the user's question only from the provided context. "
            "Cite sources like [1], [2] matching the chunk numbers. "
            "Be concise and specific.\n\nContext:\n" + ctx + "\n\nQuestion: " + q
        )
        try:
            # do not set temperature for gpt-5 family
            resp = client.chat.completions.create(
                model=models["chat"],
                messages=[
                    {"role":"system","content":"You write precise, compliance-aware answers with citations."},
                    {"role":"user","content":prompt}
                ],
            )
            ans = resp.choices[0].message.content
            _st.success("Answer")
            _st.write(ans)
            # Sources table
            rows = [{"#": i+1, "Source": h["meta"]["source"], "Ref": h["meta"]["ref"], "Score": round(h["score"],4)} for i,h in enumerate(hits)]
            _st.dataframe(_pd.DataFrame(rows), use_container_width=True, hide_index=True)
        except Exception as ex:
            _st.error(f"Chat failed: {ex}")

    # CO brief
    if brief:
        client, err = _x8_get_openai_client()
        if err or client is None:
            _st.error(err or "OpenAI client error"); return
        np, np_err = _x8_np()
        if np is None:
            _st.error(np_err); return
        df = _pd.read_sql_query("SELECT source, chunk_no, text, embedding, dim FROM ai_index WHERE rfp_id=?;", conn, params=(int(rfp_id),))
        if df is None or df.empty:
            _st.warning("No AI index for this RFP. Click Build AI Index first."); return
        # top 12 chunks for brief: use simple TF/IDF-ish scoring by length
        texts = df["text"].astype(str).tolist()
        # Select the first 12 non-empty chunks
        sel_idx = [i for i,t in enumerate(texts) if len(t.strip())>0][:12]
        hits = [{"text": texts[i], "meta":{"source": str(df.iloc[i]["source"]), "ref": f"p{int(df.iloc[i]['chunk_no'])}"}} for i in sel_idx]
        ctx = "\n\n".join([f"[{i+1}] {h['meta']['source']} {h['meta']['ref']}\n{h['text']}" for i,h in enumerate(hits)])
        prompt = (
            "Create a 1-page executive CO brief from the context. "
            "Include: Solicitation ID, NAICS, Set-Aside, Response Due, POP/Ordering Period, "
            "CLIN overview, Key POCs, Submission method, and 3-5 compliance must-dos. "
            "Use short sections and bullet points. Cite with [n] markers.\n\nContext:\n" + ctx
        )
        try:
            resp = client.chat.completions.create(
                model=models["chat"],
                messages=[
                    {"role":"system","content":"You prepare concise CO-style briefs with citations."},
                    {"role":"user","content":prompt}
                ],
            )
            doc = resp.choices[0].message.content
            _st.success("CO Brief")
            _st.write(doc)
        except Exception as ex:
            _st.error(f"Brief failed: {ex}")

# ---- Render inside RFP Analyzer without manual edits ----
try:
    _orig_run_rfp_analyzer = run_rfp_analyzer
    def run_rfp_analyzer(conn):
        _orig_run_rfp_analyzer(conn)
        try:
            import pandas as _pd, streamlit as _st
            rid = None
            # reuse a selected rid if present
            for k in ("rfp_data_sel","rfp_sel","x8_pick","x8_pick_inline"):
                if k in _st.session_state and _st.session_state.get(k):
                    rid = int(_st.session_state.get(k)); break
            if rid is None:
                df = _pd.read_sql_query("SELECT id FROM rfps ORDER BY id DESC LIMIT 1;", conn, params=())
                if df is not None and not df.empty:
                    rid = int(df.iloc[0]["id"])
            if rid:
                st_x8_panel(conn, int(rid))
        except Exception as _e:
            try:
                import streamlit as _st
                _st.info(f"X8 panel unavailable: {_e}")
            except Exception:
                pass
except Exception:
    pass
# ================== END X8 PANEL HOTFIX ==================
