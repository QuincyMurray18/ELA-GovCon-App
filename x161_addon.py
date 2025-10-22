
# x161_addon.py — Capability Statement fallback + X16.1 AI helper
# Usage: add `import x161_addon` near the top of app.py (after imports).
# If your app lacks run_capability_statement, this defines one.
# If present, this renders a minimal Capability page plus an AI helper.

from contextlib import closing

try:
    import streamlit as st
    import pandas as pd
except Exception as _e:
    raise

def _get_client():
    # Prefer app's client if provided via get_ai()
    try:
        c = get_ai()  # provided by host app
        return c, None
    except Exception:
        pass
    # Fallback to OpenAI client via secrets/env
    try:
        from openai import OpenAI  # type: ignore
        import os as _os
        key = st.secrets.get("openai", {}).get("api_key") or st.secrets.get("OPENAI_API_KEY") or _os.getenv("OPENAI_API_KEY")
        if not key:
            return None, "OpenAI API key missing"
        return OpenAI(api_key=key), None
    except Exception as e:
        return None, f"OpenAI init failed: {e}"

def _resolve_model_default():
    try:
        return (globals().get("_resolve_model") or (lambda: "gpt-5"))()
    except Exception:
        return "gpt-5"

def _ai(kind: str, company: str, audience: str, tone: str, tagline0: str, core0: str, diff0: str, ctx: str, include_pp: bool):
    sys = "You are a senior federal capture writer. Use short, precise bullets. Avoid marketing fluff. No emojis."
    req = f"""Company: {company}
Audience: {audience or '(general federal)'}
Tone: {tone}
Existing tagline: {tagline0[:200]}
Existing core competencies: {core0[:800]}
Existing differentiators: {diff0[:800]}
Include past performance: {bool(include_pp)}
Task: Draft {kind} for a one-page capability statement.
Constraints:
- 4–7 bullets for lists. 12–18 words each.
- Use federal terms. No hyperbole. No first person.
- If RFP context provided, align content.
RFP Context (optional, may be empty):
{ctx if ctx else '(none)'}"""
    client, err = _get_client()
    if err or client is None:
        return f"AI unavailable: {err or 'unknown error'}"
    try:
        model = _resolve_model_default()
        r = client.chat.completions.create(
            model=model,
            messages=[{"role":"system","content":sys},{"role":"user","content":req}],
            temperature=0.2
        )
        return (r.choices[0].message.content or "").strip()
    except Exception as e:
        return f"AI error: {e}"

def _x161_panel(conn):
    st.markdown("### X16.1 — AI drafting helper")
    st.caption("Active — tagline, Core Competencies, Differentiators")
    # Load org profile if exists
    try:
        dfp = pd.read_sql_query("SELECT * FROM org_profile WHERE id=1;", conn)
    except Exception:
        dfp = None
    profile = (dfp.iloc[0].to_dict() if isinstance(dfp, pd.DataFrame) and not dfp.empty else {})
    company = (profile.get("company_name") or "").strip() or "Your Company"
    tagline0 = profile.get("tagline","") or ""
    core0 = profile.get("core_competencies","") or ""
    diff0 = profile.get("differentiators","") or ""

    audience = st.text_input("Audience focus", key="x161_aud")
    tone = st.selectbox("Tone", ["Crisp federal", "Technical", "Plain language"], index=0, key="x161_tone")
    include_pp = st.checkbox("Include past performance hints", value=True, key="x161_pp")

    # Optional RFP context
    ctx = ""
    try:
        dfr = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn)
    except Exception:
        dfr = None
    sel = st.selectbox("Optional RFP context", options=[None] + (dfr["id"].tolist() if dfr is not None and not dfr.empty else []),
                       format_func=lambda i: "None" if i is None else f"#{i} — {dfr.loc[dfr['id']==i,'title'].values[0]}",
                       key="x161_rfp")
    if sel:
        try:
            hits = pd.read_sql_query("SELECT text FROM ai_index WHERE rfp_id=? LIMIT 40;", conn, params=(int(sel),))
            ctx = "\n".join((hits["text"].fillna("").tolist() if hits is not None else []))[:12000]
        except Exception:
            ctx = ""

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Draft Tagline", key="x161_btn_tl"):
            st.session_state["x161_tagline"] = _ai("a concise 8–14 word tagline", company, audience, tone, tagline0, core0, diff0, ctx, include_pp)
    with c2:
        if st.button("Draft Core Competencies", key="x161_btn_cc"):
            st.session_state["x161_core"] = _ai("Core Competencies bullets", company, audience, tone, tagline0, core0, diff0, ctx, include_pp)
    with c3:
        if st.button("Draft Differentiators", key="x161_btn_df"):
            st.session_state["x161_diff"] = _ai("Differentiators bullets", company, audience, tone, tagline0, core0, diff0, ctx, include_pp)

    st.text_input("Tagline (AI)", value=st.session_state.get("x161_tagline",""), key="x161_tagline_box")
    st.text_area("Core Competencies (AI)", value=st.session_state.get("x161_core",""), height=160, key="x161_core_box")
    st.text_area("Differentiators (AI)", value=st.session_state.get("x161_diff",""), height=160, key="x161_diff_box")

    if st.button("Save to org_profile", key="x161_save"):
        try:
            with closing(conn.cursor()) as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS org_profile(id INTEGER PRIMARY KEY, company_name TEXT, tagline TEXT, core_competencies TEXT, differentiators TEXT);")
                cur.execute("INSERT OR IGNORE INTO org_profile(id, company_name) VALUES(1, ?);", (company,))
                cur.execute("UPDATE org_profile SET tagline=?, core_competencies=?, differentiators=? WHERE id=1;",
                            (st.session_state.get('x161_tagline',''), st.session_state.get('x161_core',''), st.session_state.get('x161_diff','')))
                conn.commit()
            st.success("Saved.")
        except Exception as e:
            st.error(f"Save failed: {e}")

# If the host app doesn't define run_capability_statement, define it.
if "run_capability_statement" not in globals():
    def run_capability_statement(conn):
        st.header("Capability Statement")
        st.caption("Fallback view provided by x161_addon")
        _x161_panel(conn)
else:
    # Wrap by appending our panel after the host view.
    _orig_run_capability_statement = run_capability_statement
    def run_capability_statement(conn):
        _orig_run_capability_statement(conn)
        _x161_panel(conn)
