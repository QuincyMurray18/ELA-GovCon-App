
# x17_addon.py — X17 White Paper Assistant
# Import in app.py:
#   import x17_addon
#
# Drafts a short white paper aligned to an opportunity.

try:
    import streamlit as st
    import pandas as pd
except Exception as _e:
    raise

def _x17_client():
    try:
        return client, None
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

def _x17_ui(conn):
    st.markdown("### X17 — White Paper Assistant")
    st.caption("X17 active — 3–5 page concept paper draft and export")
    try:
        _rfps = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
    except Exception:
        _rfps = None
    rid = st.selectbox("RFP context (for X17)", options=_rfps["id"].tolist() if _rfps is not None and not _rfps.empty else [],
                       format_func=lambda i: f"#{i} — {_rfps.loc[_rfps['id']==i,'title'].values[0]}", key="x17_rid")
    prob = st.text_area("Problem statement", height=120, key=f"x17_prob_{rid}")
    sol = st.text_area("Solution approach", height=120, key=f"x17_sol_{rid}")
    value = st.text_area("Value proposition / benefits", height=120, key=f"x17_val_{rid}")
    if st.button("Draft white paper", key=f"x17_go_{rid}"):
        c, err = _x17_client()
        if err or c is None:
            st.error(err or "OpenAI unavailable")
            return
        sys = "You write concise federal concept papers. Structure with headings, bullets, and short paragraphs."
        usr = f"RFP: {rid}\nProblem:\n{prob}\n\nSolution:\n{sol}\n\nValue:\n{value}\n\nDraft 3–5 pages of content. Avoid marketing fluff."
        try:
            r = c.chat.completions.create(model=st.secrets.get("models",{}).get("writer") or "gpt-5",
                                          messages=[{"role":"system","content":sys},{"role":"user","content":usr}])
            text = r.choices[0].message.content
            st.text_area("Draft", value=text, height=420, key=f"x17_text_{rid}")
            st.download_button("Download .md", data=text.encode("utf-8"), file_name=f"white_paper_rfp_{rid}.md", mime="text/markdown", key=f"x17_dl_{rid}")
        except Exception as e:
            st.error(f"Draft failed: {e}")

# Hook
try:
    _orig_run_rfp_analyzer = run_rfp_analyzer
except NameError:
    _orig_run_rfp_analyzer = None

if _orig_run_rfp_analyzer:
    def run_rfp_analyzer(conn):
        _orig_run_rfp_analyzer(conn)
        try:
            _x17_ui(conn)
        except Exception as e:
            st.info(f"X17 panel unavailable: {e}")
