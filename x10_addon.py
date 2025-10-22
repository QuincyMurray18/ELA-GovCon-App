
# x10_addon.py — X10 Proposal Builder v1
# Import in app.py after other addons:
#   import x10_addon
#
# Adds "X10 — Proposal Builder v1" to RFP Analyzer:
# - Outline from L&M (factors/subfactors), word budgets from page limits
# - Draft per section using OpenAI with compliant template
# - Page-limit guard with live word counts
# - Export Markdown

from contextlib import closing
import math
import re

try:
    import streamlit as st
    import pandas as pd
    import numpy as np
except Exception as _e:
    raise

# ---------------- models / client ----------------

def _x10_models():
    try:
        m = st.secrets.get("models", {})
        chat = m.get("writer") or m.get("heavy") or st.secrets.get("x8_model") or "gpt-5"
        embed = m.get("embed") or st.secrets.get("embed_model") or "text-embedding-3-small"
        return chat, embed
    except Exception:
        return "gpt-5", "text-embedding-3-small"

def _x10_client():
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

# ---------------- data helpers ----------------

def _x10_fetch_lm(conn, rid: int) -> pd.DataFrame:
    try:
        df = pd.read_sql_query(
            "SELECT req_id, section, factor, subfactor, text, source, page_limit, weight "
            "FROM lm_reqs WHERE rfp_id=? ORDER BY section, factor, subfactor, req_id;",
            conn, params=(int(rid),)
        )
        return df
    except Exception:
        return pd.DataFrame(columns=["req_id","section","factor","subfactor","text","source","page_limit","weight"])

def _x10_outline_from_lm(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["section_key","title","factor","subfactor","req_count","page_limit","word_budget"])
    # Group by section->factor->subfactor
    def title_row(sec, fac, sub):
        parts = []
        if str(fac or "").strip():
            parts.append(f"Factor {fac}")
        if str(sub or "").strip():
            parts.append(f"Subfactor {sub}")
        if not parts:
            parts = [ "General Requirements" if str(sec).upper()=="L" else "Evaluation Alignment" if str(sec).upper()=="M" else "Requirements" ]
        return " — ".join(parts)
    rows = []
    for (sec, fac, sub), g in df.groupby(["section","factor","subfactor"], dropna=False):
        secu = str(sec or "").upper()
        key = f"{secu}:{fac or ''}:{sub or ''}"
        # Page limit heuristic: first non-empty page_limit text seen in group
        pl_text = ""
        for t in g.get("page_limit", []):
            if t and isinstance(t, str) and len(t) >= 4:
                pl_text = t
                break
        # Parse a number of pages if present
        pages = None
        m = re.search(r"(\d+)\s*page", pl_text, re.I) if pl_text else None
        if m:
            try:
                pages = int(m.group(1))
            except Exception:
                pages = None
        # Derive word budget: default 500 words/page
        budget = pages * 500 if pages else None
        rows.append({
            "section_key": key,
            "title": title_row(secu, fac, sub),
            "factor": fac or "",
            "subfactor": sub or "",
            "req_count": int(len(g)),
            "page_limit": pages if pages else "",
            "word_budget": budget if budget else ""
        })
    out = pd.DataFrame(rows).sort_values(["section_key"]).reset_index(drop=True)
    return out

def _x10_get_ai_index(conn, rid: int) -> pd.DataFrame:
    try:
        df = pd.read_sql_query("SELECT source, chunk_no, text, embedding, dim FROM ai_index WHERE rfp_id=?;", conn, params=(int(rid),))
        return df
    except Exception:
        return pd.DataFrame(columns=["source","chunk_no","text","embedding","dim"])

def _x10_hits_for_section(df_index: pd.DataFrame, df_lm: pd.DataFrame, sec_key: str, embed_model: str, client):
    # Build a pseudo-query from requirement texts of that section
    try:
        sec, fac, sub = sec_key.split(":", 2)
    except Exception:
        sec, fac, sub = "", "", ""
    subset = df_lm[(df_lm["section"].str.upper()==sec) & (df_lm["factor"].astype(str)==(fac or "")) & (df_lm["subfactor"].astype(str)==(sub or ""))]
    seed = " ".join(list(subset["text"])[:40])[:6000] if not subset.empty else ""
    if not isinstance(df_index, pd.DataFrame) or df_index.empty or not seed:
        return [], ""
    # Embed seed
    try:
        emq = client.embeddings.create(model=embed_model, input=[seed])
        import numpy as _np
        qv = _np.array(emq.data[0].embedding, dtype=_np.float32)
    except Exception:
        emq = client.Embedding.create(model=embed_model, input=[seed])
        import numpy as _np
        qv = _np.array(emq["data"][0]["embedding"], dtype=_np.float32)
    import numpy as _np
    M = _np.vstack([_np.frombuffer(b, dtype=_np.float32, count=int(d)) for b,d in zip(df_index["embedding"], df_index["dim"])])
    sims = (M @ qv) / (_np.linalg.norm(M, axis=1) * (float(_np.linalg.norm(qv))+1e-9))
    order = sims.argsort()[::-1][:10]
    hits = []
    for i in order:
        row = df_index.iloc[int(i)]
        hits.append({"text": str(row["text"])[:4000], "meta": {"source": str(row["source"]), "ref": f"p.{int(row['chunk_no'])}"}, "score": float(sims[int(i)])})
    ctx = "\n\n".join([f"[{h['meta']['source']} {h['meta']['ref']}]\n{h['text']}" for h in hits])
    return hits, ctx

# ---------------- drafting ----------------

def _x10_draft(conn, rid: int, sec_key: str, addl: str):
    chat_model, embed_model = _x10_models()
    client, err = _x10_client()
    if err or client is None:
        return None, f"OpenAI error: {err}"
    df_lm = _x10_fetch_lm(conn, rid)
    if df_lm is None or df_lm.empty:
        return None, "No L&M requirements. Run X9 first."
    df_index = _x10_get_ai_index(conn, rid)
    hits, ctx = _x10_hits_for_section(df_index, df_lm, sec_key, embed_model, client)
    # Build section facts and requirements
    sec, fac, sub = (sec_key.split(":",2)+["","",""])[:3]
    subset = df_lm[(df_lm["section"].str.upper()==sec) & (df_lm["factor"].astype(str)==(fac or "")) & (df_lm["subfactor"].astype(str)==(sub or ""))]
    req_lines = "\n".join(f"- {t}" for t in subset["text"].tolist()[:80])
    # Word budget inference
    pages = None
    for p in subset.get("page_limit", []):
        m = re.search(r"(\d+)\s*page", str(p), re.I)
        if m:
            try:
                pages = int(m.group(1)); break
            except Exception: pass
    budget = pages * 500 if pages else 0

    sys = ("You are a senior proposal writer for U.S. Federal bids. Draft precise, compliant text. "
           "Mirror the solicitation language where appropriate. Do not fabricate facts. "
           "Cite in-line as [filename p.X] only when quoting or stating a requirement. "
           "Avoid marketing fluff. Active voice. Short sentences.")
    usr = (
        f"RFP ID: {rid}\n"
        f"SECTION KEY: {sec_key}\n"
        "REQUIREMENTS (verbatim snippets):\n"
        f"{req_lines}\n\n"
        "CONTEXT EXCERPTS (for support, not all must be used):\n"
        f"{ctx}\n\n"
        "TASK:\n"
        "Write a proposal section that fully answers the REQUIREMENTS for this section key. "
        "Organize with an opening promise, numbered subsections that map to each 'must/shall', and a short close. "
        "If page limits imply a tight budget, keep to the point.\n"
    )
    if addl:
        usr += f"\nADDITIONAL DIRECTION FROM SME:\n{addl}\n"

    try:
        resp = client.chat.completions.create(
            model=chat_model,
            messages=[
                {"role":"system","content": sys},
                {"role":"user","content": usr},
            ],
        )
        text = resp.choices[0].message.content.strip()
        return {"text": text, "budget": budget}, None
    except Exception as e:
        return None, f"Draft failed: {e}"

# ---------------- UI ----------------

def _x10_ui(conn):
    st.markdown("### X10 — Proposal Builder v1")
    st.caption("X10 active — outline from L&M, section drafts, page-limit guard, export")
    try:
        _rfps = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
    except Exception:
        _rfps = None
    if _rfps is None or _rfps.empty:
        st.info("No RFP found. Parse & save first.")
        return
    rid = st.selectbox("RFP context (for X10)", options=_rfps["id"].tolist(),
                       format_func=lambda i: f\"#{i} — {_rfps.loc[_rfps['id']==i,'title'].values[0]}\",
                       key="x10_rid")

    tabs = st.tabs(["Outline", "Draft", "Limits / Export"])

    with tabs[0]:
        df_lm = _x10_fetch_lm(conn, int(rid))
        if df_lm is None or df_lm.empty:
            st.info("Run X9 to build L&M first.")
        else:
            outline = _x10_outline_from_lm(df_lm)
            st.dataframe(outline, use_container_width=True, hide_index=True)
            if not outline.empty:
                csv = outline.to_csv(index=False).encode("utf-8")
                st.download_button("Download Outline CSV", data=csv, file_name=f"rfp_{rid}_outline.csv", mime="text/csv", key=f"x10_csv_{rid}")

    with tabs[1]:
        df_lm = _x10_fetch_lm(conn, int(rid))
        outline = _x10_outline_from_lm(df_lm) if df_lm is not None else pd.DataFrame()
        if outline is None or outline.empty:
            st.info("No outline. Build L&M first.")
        else:
            sec_key = st.selectbox("Select section key", options=outline["section_key"].tolist(), key=f"x10_key_{rid}")
            addl = st.text_area("Optional SME notes for this section", key=f"x10_notes_{rid}")
            if st.button("Draft section", key=f"x10_draft_{rid}"):
                res, err = _x10_draft(conn, int(rid), sec_key, addl)
                if err:
                    st.error(err)
                else:
                    text = res["text"]
                    budget = res.get("budget") or 0
                    # keep in session
                    key = f"x10_drafts_{rid}"
                    if key not in st.session_state:
                        st.session_state[key] = {}
                    st.session_state[key][sec_key] = {"text": text, "budget": budget}
                    st.success(f"Draft ready. Budget ~{budget} words" if budget else "Draft ready.")
                    st.text_area("Draft", value=text, height=400, key=f"x10_text_{rid}_{sec_key}")
                    st.download_button("Download this section (.md)", data=text.encode("utf-8"), file_name=f"rfp_{rid}_{sec_key.replace(':','_')}.md", mime="text/markdown")

    with tabs[2]:
        wpp = st.number_input("Words per page (guard)", min_value=300, max_value=700, value=500, step=25, key=f"x10_wpp_{rid}")
        key = f"x10_drafts_{rid}"
        data = st.session_state.get(key, {})
        rows = []
        total_words = 0
        total_budget = 0
        for sk, v in data.items():
            txt = v.get("text") or ""
            words = len(txt.split())
            budget = v.get("budget") or ""
            if isinstance(budget, int) and budget == 0:
                budget = ""
            rows.append({"section_key": sk, "words": words, "budget": budget})
            total_words += words
            total_budget += (budget or 0)
        if rows:
            dfw = pd.DataFrame(rows)
            st.dataframe(dfw, use_container_width=True, hide_index=True)
            st.caption(f"Total words: {total_words}" + (f" | Total budget: {total_budget}" if total_budget else ""))
            if st.button("Export all drafts (.md combined)", key=f"x10_exp_{rid}"):
                parts = []
                for sk, v in data.items():
                    parts.append(f"## {sk}\n\n{v.get('text') or ''}\n")
                bundle = "\n\n".join(parts)
                st.download_button("Download combined (.md)", data=bundle.encode("utf-8"), file_name=f"rfp_{rid}_drafts.md", mime="text/markdown", key=f"x10_dl_all_{rid}")
        else:
            st.info("No drafts generated yet.")

# Hook into existing analyzer
try:
    _orig_run_rfp_analyzer = run_rfp_analyzer
except NameError:
    _orig_run_rfp_analyzer = None

if _orig_run_rfp_analyzer:
    def run_rfp_analyzer(conn):
        _orig_run_rfp_analyzer(conn)
        try:
            _x10_ui(conn)
        except Exception as e:
            st.info(f"X10 panel unavailable: {e}")
