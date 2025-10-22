
# x18_addon.py — X18 Memory + Audit v1
# Import in app.py:
#   import x18_addon
#
# Adds lightweight chat memory and an audit trail of AI calls.
# Offers a wrapper to log OpenAI chat requests/responses into sqlite.

import json
from contextlib import closing

try:
    import streamlit as st
    import pandas as pd
except Exception as _e:
    raise

def _x18_tables(conn):
    with closing(conn.cursor()) as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS ai_audit(
            id INTEGER PRIMARY KEY,
            ts TEXT DEFAULT (datetime('now')),
            model TEXT,
            role_system TEXT,
            prompt TEXT,
            response TEXT,
            rfp_id INTEGER,
            panel TEXT
        );""")
        cur.execute("""CREATE TABLE IF NOT EXISTS chat_mem(
            id INTEGER PRIMARY KEY,
            ts TEXT DEFAULT (datetime('now')),
            rfp_id INTEGER,
            key TEXT,
            value TEXT
        );""")
        conn.commit()

def _x18_wrap_client():
    # Wrap client.chat.completions.create to log calls if not already wrapped
    try:
        c = client  # noqa
    except NameError:
        return False, "OpenAI client not found"
    try:
        orig = c.chat.completions.create
    except Exception:
        return False, "Client does not support chat.completions.create"

    if getattr(c.chat.completions, "_x18_wrapped", False):
        return True, "already wrapped"

    def wrapper(*args, **kwargs):
        model = kwargs.get("model", "")
        msgs = kwargs.get("messages", [])
        sys = ""
        for m in msgs:
            if m.get("role") == "system":
                sys = m.get("content",""); break
        user = ""
        for m in msgs[::-1]:
            if m.get("role") == "user":
                user = m.get("content",""); break
        res = orig(*args, **kwargs)
        try:
            text = res.choices[0].message.content
        except Exception:
            text = ""
        # store in a transient buffer attached to streamlit session state
        st.session_state.setdefault("_x18_last_call", {"model":model,"system":sys,"prompt":user,"response":text})
        return res

    c.chat.completions.create = wrapper
    c.chat.completions._x18_wrapped = True
    return True, "wrapped"

def _x18_ui(conn):
    _x18_tables(conn)
    st.markdown("### X18 — Memory + Audit v1")
    st.caption("X18 active — wrap OpenAI calls for logging, save notes, export")

    c1, c2, c3 = st.columns([2,2,2])
    with c1:
        if st.button("Enable audit wrapper"):
            ok, msg = _x18_wrap_client()
            st.success(f"Audit: {msg}" if ok else f"Audit not enabled: {msg}")
    with c2:
        rid = st.number_input("RFP id (optional context)", min_value=0, step=1, value=0, key="x18_rid")
    with c3:
        panel = st.text_input("Panel tag", value="", key="x18_tag")

    if st.button("Save last AI call"):
        data = st.session_state.get("_x18_last_call")
        if not data:
            st.info("No AI call captured yet.")
        else:
            with closing(conn.cursor()) as cur:
                cur.execute("""INSERT INTO ai_audit(model, role_system, prompt, response, rfp_id, panel)
                               VALUES(?,?,?,?,?,?);""",
                            (data.get("model",""), data.get("system",""), data.get("prompt",""), data.get("response",""),
                             int(rid) if rid else None, panel or ""))
                conn.commit()
            st.success("Saved.")

    st.subheader("Memory")
    key = st.text_input("Key", value="", key="x18_mem_k")
    val = st.text_area("Value", height=100, key="x18_mem_v")
    if st.button("Save memory"):
        with closing(conn.cursor()) as cur:
            cur.execute("INSERT INTO chat_mem(rfp_id, key, value) VALUES(?,?,?);", (int(rid) if rid else None, key, val))
            conn.commit()
        st.success("Memory saved.")

    st.subheader("Browse Audit")
    try:
        df = pd.read_sql_query("SELECT id, ts, model, substr(prompt,1,80) AS prompt, substr(response,1,80) AS response, rfp_id, panel FROM ai_audit ORDER BY id DESC LIMIT 500;", conn, params=())
        st.dataframe(df, use_container_width=True, hide_index=True)
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Download audit CSV", data=csv, file_name="ai_audit.csv", mime="text/csv", key="x18_dl")
    except Exception as e:
        st.info(f"Audit read unavailable: {e}")

# Hook
try:
    _orig_run_rfp_analyzer = run_rfp_analyzer
except NameError:
    _orig_run_rfp_analyzer = None

if _orig_run_rfp_analyzer:
    def run_rfp_analyzer(conn):
        _orig_run_rfp_analyzer(conn)
        try:
            _x18_ui(conn)
        except Exception as e:
            st.info(f"X18 panel unavailable: {e}")
