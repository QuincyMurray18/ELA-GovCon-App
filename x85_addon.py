
# x85_addon.py — X8.5 Accuracy Hardening addon
# Import this AFTER x84_addon in app.py:
#   import x84_addon
#   import x85_addon
#
# It appends "X8.5 — Accuracy hardening" tools to RFP Analyzer:
# - Solicitation-first precedence
# - L/M override when applicable
# - Single-POC enforcement
# - Email denylist and gov/mil-only rule
# - Stricter prompts

from contextlib import closing
import re

try:
    import streamlit as st
    import pandas as pd
    import numpy as np
except Exception as _e:
    raise

_DENY_EMAIL_RE = re.compile(
    r"@(gmail|yahoo|outlook|hotmail|aol|icloud|protonmail|live|msn)\\.com$", re.I
)
_BAD_LOCALPART_RE = re.compile(r"(no-?reply|abuse|security|cert|helpdesk|support)", re.I)

_SOLICITATION_HINT_RE = re.compile(
    r"(solicitation|sf1449|section\\s*[lm]\\b|instruc|evaluation|rfp\\b|pws\\b|sow\\b|schedule|clins?)",
    re.I
)
_L_HINT_RE = re.compile(r"(section\\s*l\\b|instructions|proposal\\s+submission|page\\s+limit)", re.I)
_M_HINT_RE = re.compile(r"(section\\s*m\\b|evaluation|basis\\s+of\\s+award|factors?)", re.I)

def _x85_valid_gov(email: str) -> bool:
    try:
        email = (email or "").strip().lower()
        if not email or "@" not in email: 
            return False
        local, dom = email.split("@", 1)
        if _DENY_EMAIL_RE.search("@" + dom): 
            return False
        if not (dom.endswith(".gov") or dom.endswith(".mil")):
            return False
        if _BAD_LOCALPART_RE.search(local):
            return False
        # domain sanity: must have at least one dot
        if "." not in dom:
            return False
        return True
    except Exception:
        return False

def _x85_pick_primary_poc(df_p: pd.DataFrame, agency_hint: str = "") -> pd.Series:
    if df_p is None or df_p.empty:
        return pd.Series({"name":"", "role":"POC", "email":"", "phone":""})
    df = df_p.fillna("").copy()
    df["email_ok"] = df["email"].apply(_x85_valid_gov).astype(int)
    def _dom(e):
        try: return (e or "").split("@",1)[1].lower()
        except Exception: return ""
    df["domain"] = df["email"].apply(_dom)
    gov = df["domain"].str.contains(r"\\.(gov|mil)$", case=False, regex=True).astype(int)
    rolew = df["role"].str.contains(r"contract(ing)? officer|\\bko\\b|contract specialist|\\bcor\\b", case=False, regex=True).astype(int) * 2
    freq = df["domain"].map(df["domain"].value_counts().to_dict())
    # Agency hint bonus, e.g., bop.gov for BOP
    ah = (agency_hint or "").lower()
    if ah:
        hint_bonus = df["domain"].str.contains(re.escape(ah), case=False).astype(int) * 3
    else:
        hint_bonus = 0
    df["score"] = df["email_ok"] * 3 + gov + rolew + freq + (hint_bonus if isinstance(hint_bonus, pd.Series) else 0)
    return df.sort_values(["score"], ascending=False).iloc[0]

def _x85_weight_row(source: str, chunk_no: int, text: str, question: str) -> float:
    w = 0.0
    s = f"{source} p.{chunk_no} :: {text[:400]}".lower()
    if _SOLICITATION_HINT_RE.search(s): w += 3.0
    if _L_HINT_RE.search(s) and ("instruction" in question.lower() or "submit" in question.lower() or "page" in question.lower()): 
        w += 2.5
    if _M_HINT_RE.search(s) and ("evaluate" in question.lower() or "factor" in question.lower() or "award" in question.lower()):
        w += 2.5
    # Prefer longer, denser chunks slightly
    w += min(len(text) / 1200.0, 1.0)
    return w

def _x85_models():
    try:
        m = st.secrets.get("models", {})
        chat = m.get("heavy") or st.secrets.get("x8_model") or "gpt-5"
        embed = m.get("embed") or st.secrets.get("embed_model") or "text-embedding-3-small"
        return chat, embed
    except Exception:
        return "gpt-5", "text-embedding-3-small"

def _x85_facts(conn, rid):
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
            # infer agency hint from majority gov domain
            domains = p["email"].fillna("").apply(lambda e: (e.split("@",1)[1].lower() if "@" in e else ""))
            candidates = [d for d in domains if d.endswith(".gov") or d.endswith(".mil")]
            hint = ""
            if candidates:
                # pick the most common agency base like "bop.gov" or "usdoj.gov"
                base = pd.Series(candidates).value_counts().index[0]
                # take last two labels: e.g., bop.gov
                parts = base.split(".")
                if len(parts) >= 2:
                    hint = parts[-2] + "." + parts[-1]
            row = _x85_pick_primary_poc(p, hint)
            facts.append(f"Primary POC: {(row.get('name') or '').strip()} — {(row.get('role') or 'POC').strip()} — {(row.get('email') or '').strip()} — {(row.get('phone') or '').strip()}")
    except Exception:
        pass
    return facts

def _x85_qna_hardened(conn, rid, q, client, chat_model, embed_model):
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
    sims = (M @ qv) / (_np.linalg.norm(M, axis1:=1) * (float(_np.linalg.norm(qv))+1e-9))

    # Hard re-ranking: solicitation-first and L/M overrides
    weights = []
    q_lower = q.lower()
    for i in range(len(df)):
        w = float(sims[i]) + _x85_weight_row(str(df.iloc[i]["source"]), int(df.iloc[i]["chunk_no"]), str(df.iloc[i]["text"]), q_lower)
        weights.append(w)
    order = np.argsort(-np.array(weights))[:8]
    hits = []
    for i in order:
        row = df.iloc[int(i)]
        hits.append({"text": str(row["text"])[:4000], "meta": {"source": str(row["source"]), "ref": f"p.{int(row['chunk_no'])}"}, "score": float(weights[int(i)])})
    ctx = "\\n\\n".join([f"[{h['meta']['source']} {h['meta']['ref']}]\\n{h['text']}" for h in hits])

    facts_text = "\\n".join(f"- {x}" for x in _x85_facts(conn, rid)) or "- Not found."
    system_prompt = (
        "You are a senior U.S. Government Contracting Officer. Use only the FACTS and CONTEXT. "
        "If sources conflict, prefer the solicitation and Sections L/M over other attachments. "
        "Cite each factual sentence with [filename p.X]. "
        "Show exactly one primary POC in Answer; put others in Sources. Do not invent data."
    )
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
        "- Summarize POP as Base + Options when present.\\n"
        "- If you cannot find a field, write 'not found' instead of guessing."
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
        st.success("Answer (hardened)")
        st.write(ans)
        rows = [{"#": i+1, "Source": f"{h['meta']['source']} {h['meta']['ref']}", "Score": round(h["score"],4)} for i,h in enumerate(hits)]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    except Exception as ex:
        st.error(f"Chat failed: {ex}")

def _x85_add_panel(conn):
    st.markdown("### X8.5 — Accuracy hardening")
    st.caption("X8.5 active — solicitation-first, L/M override, single-POC, denylist")
    try:
        _rfps = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
    except Exception:
        _rfps = None
    if _rfps is None or _rfps.empty:
        st.info("No RFP found. Parse & save first.")
        return
    rid = st.selectbox("RFP context (for X8.5)", options=_rfps["id"].tolist(),
                       format_func=lambda i: f"#{i} — {_rfps.loc[_rfps['id']==i,'title'].values[0]}",
                       key="x85_rid")
    chat_model, embed_model = _x85_models()
    # Ensure OpenAI client
    try:
        client  # noqa
    except NameError:
        try:
            from openai import OpenAI  # type: ignore
            import os as _os
            _key = st.secrets.get("openai",{}).get("api_key") or st.secrets.get("OPENAI_API_KEY") or _os.getenv("OPENAI_API_KEY")
            globals()["client"] = OpenAI(api_key=_key)
        except Exception as e:
            st.error(f"OpenAI client init failed: {e}")
            return

    c1, c2 = st.columns([2,2])
    with c1:
        btn = st.button("Answer (hardened)", key=f"x85_ask_{rid}")
    with c2:
        btnb = st.button("Show resolved FACTS", key=f"x85_facts_{rid}")

    if btn:
        q = st.text_input("Your question", key=f"x85_q_{rid}", placeholder="e.g., What are the due date, submission method, and POP?")
        if (q or "").strip():
            _x85_qna_hardened(conn, rid, q, client, chat_model, embed_model)
        else:
            st.warning("Enter a question first.")

    if btnb:
        facts = _x85_facts(conn, rid)
        if facts:
            st.success("Resolved FACTS")
            st.write("\\n".join(f"- {x}" for x in facts))
        else:
            st.warning("No resolvable facts found.")

# Hook into existing analyzer
try:
    _orig_run_rfp_analyzer = run_rfp_analyzer
except NameError:
    _orig_run_rfp_analyzer = None

if _orig_run_rfp_analyzer:
    def run_rfp_analyzer(conn):
        _orig_run_rfp_analyzer(conn)
        try:
            _x85_add_panel(conn)
        except Exception as e:
            st.info(f"X8.5 panel unavailable: {e}")
