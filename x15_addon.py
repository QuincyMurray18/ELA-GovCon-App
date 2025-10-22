
# x15_addon.py — X15 Outreach Kit v1
# Import in app.py:
#   import x15_addon
#
# Generates outreach emails for subs/teaming/agency Q&A from DB context.

from contextlib import closing

try:
    import streamlit as st
    import pandas as pd
except Exception as _e:
    raise

def _x15_client():
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

def _x15_meta(conn, rid: int) -> dict:
    out = {"title":"","agency":"","sol":""}
    try:
        df = pd.read_sql_query("SELECT id, title, agency, sol_number FROM rfps WHERE id=?;", conn, params=(int(rid),))
        if df is not None and not df.empty:
            r = df.iloc[0]
            out.update({"title":r.get("title") or "", "agency": r.get("agency") or "", "sol": r.get("sol_number") or ""})
    except Exception:
        pass
    return out

def _x15_template(kind: str, meta: dict, notes: str):
    c, err = _x15_client()
    if err or c is None:
        return f"Subject: {kind} re {meta.get('title','')}\n\n<Write your {kind} message here>\n"
    sys = "You draft concise, professional federal contracting emails. 150-220 words."
    usr = f"KIND: {kind}\nAGENCY: {meta.get('agency','')}\nSOLICITATION: {meta.get('sol','')}\nTITLE: {meta.get('title','')}\nNOTES: {notes or ''}\nDraft a complete email with subject and body."
    try:
        r = c.chat.completions.create(model=st.secrets.get("models",{}).get("writer") or "gpt-5",
                                      messages=[{"role":"system","content":sys},{"role":"user","content":usr}])
        return r.choices[0].message.content.strip()
    except Exception:
        return f"Subject: {kind} re {meta.get('title','')}\n\n<Email body>\n"

def _x15_ui(conn):
    st.markdown("### X15 — Outreach Kit v1")
    st.caption("X15 active — generate emails for subs, teaming, and KO Q&A")
    try:
        _rfps = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
    except Exception:
        _rfps = None
    if _rfps is None or _rfps.empty:
        st.info("No RFP found.")
        return
    rid = st.selectbox("RFP context (for X15)", options=_rfps["id"].tolist(),
                       format_func=lambda i: f"#{i} — {_rfps.loc[_rfps['id']==i,'title'].values[0]}",
                       key="x15_rid")
    kind = st.selectbox("Email type", ["Subcontractor outreach","Teaming invitation","Questions to KO"], key=f"x15_kind_{rid}")
    notes = st.text_area("Notes to include", key=f"x15_notes_{rid}")
    if st.button("Generate email", key=f"x15_go_{rid}"):
        meta = _x15_meta(conn, int(rid))
        txt = _x15_template(kind, meta, notes)
        st.text_area("Draft", value=txt, height=260, key=f"x15_txt_{rid}")
        st.download_button("Download .txt", data=txt.encode("utf-8"), file_name=f"rfp_{rid}_outreach.txt", mime="text/plain", key=f"x15_dl_{rid}")

# Hook into existing analyzer
try:
    _orig_run_rfp_analyzer = run_rfp_analyzer
except NameError:
    _orig_run_rfp_analyzer = None

if _orig_run_rfp_analyzer:
    def run_rfp_analyzer(conn):
        _orig_run_rfp_analyzer(conn)
        try:
            _x15_ui(conn)
        except Exception as e:
            st.info(f"X15 panel unavailable: {e}")
