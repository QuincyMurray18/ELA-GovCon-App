
# x9_addon.py — X9 L&M Decoder + Compliance Matrix
# Import in app.py (after other addons if any):
#   import x9_addon
#
# Provides an "X9 — L&M Decoder" section in RFP Analyzer with:
# - Build L&M: parse Section L (Instructions) and Section M (Evaluation)
# - Compliance Matrix: tabular requirements with IDs, factors, page limits, etc.
# - Gaps → Actions: auto actions for unresolved items
# - CSV export
#
# Tables:
#   lm_reqs(id, rfp_id, req_id, section, factor, subfactor, text, source, page_limit, weight, applies_to, status, owner, due, notes, created_at)

import re
from contextlib import closing
import datetime as _dt

try:
    import streamlit as st
    import pandas as pd
    import numpy as np
except Exception as _e:
    raise

# ---------- helpers ----------

_REQ_PAT = re.compile(r'\\b(shall|must|will|required to|is required to)\\b', re.I)
_FACT_PAT = re.compile(r'\\bfactor\\s*([A-Z]|\\d+)\\b', re.I)
_SUBF_PAT = re.compile(r'\\bsub[- ]?factor\\s*([A-Z]|\\d+)\\b', re.I)
_PAGELIM_PAT = re.compile(r'(\\bpage(?:s)?\\s*limit\\b|\\bnot exceed\\s*\\d+\\s*page)', re.I)
_WEIGHT_PAT = re.compile(r'\\b(weight|importance|significant(ly)? more important|equal to|less important)\\b', re.I)
_L_HDR = re.compile(r'(^|\\n)\\s*section\\s*l\\b[\\s\\-:]*', re.I)
_M_HDR = re.compile(r'(^|\\n)\\s*section\\s*m\\b[\\s\\-:]*', re.I)
_L_ALT = re.compile(r'(instructions to offerors|proposal instructions)', re.I)
_M_ALT = re.compile(r'(evaluation factors|basis of award)', re.I)

def _now():
    return _dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

def _ensure_tables(conn):
    with closing(conn.cursor()) as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS lm_reqs(
            id INTEGER PRIMARY KEY,
            rfp_id INTEGER NOT NULL,
            req_id TEXT,
            section TEXT,
            factor TEXT,
            subfactor TEXT,
            text TEXT,
            source TEXT,
            page_limit TEXT,
            weight TEXT,
            applies_to TEXT,
            status TEXT DEFAULT 'Unresolved',
            owner TEXT,
            due TEXT,
            notes TEXT,
            created_at TEXT
        );""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_lm_reqs_rfp ON lm_reqs(rfp_id);")
        conn.commit()

def _section_splits(full_text: str):
    t = full_text or ""
    # try headers
    l_idx = _L_HDR.search(t)
    m_idx = _M_HDR.search(t)
    if not l_idx and _L_ALT.search(t):
        l_idx = _L_ALT.search(t)
    if not m_idx and _M_ALT.search(t):
        m_idx = _M_ALT.search(t)
    L, M = "", ""
    if l_idx and m_idx:
        if l_idx.start() < m_idx.start():
            L = t[l_idx.start():m_idx.start()]
            M = t[m_idx.start():]
        else:
            M = t[m_idx.start():l_idx.start()]
            L = t[l_idx.start():]
    elif l_idx:
        L = t[l_idx.start():]
    elif m_idx:
        M = t[m_idx.start():]
    return L, M

def _extract_requirements(text: str, prefix: str, source_label: str):
    out = []
    if not text:
        return out
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    factor, subfactor = "", ""
    ctr = 0
    for ln in lines:
        lf = _FACT_PAT.search(ln)
        if lf:
            factor = lf.group(1)
        ls = _SUBF_PAT.search(ln)
        if ls:
            subfactor = ls.group(1)
        if _REQ_PAT.search(ln):
            ctr += 1
            req_id = f"{prefix}-{ctr:03d}"
            page_lim = ln if _PAGELIM_PAT.search(ln) else ""
            weight = ln if _WEIGHT_PAT.search(ln) else ""
            out.append({
                "req_id": req_id,
                "factor": factor,
                "subfactor": subfactor,
                "text": ln,
                "source": source_label,
                "page_limit": page_lim,
                "weight": weight,
                "applies_to": ""  # user can fill later
            })
    return out

def _approx_chunk(source_name: str, idx: int):
    # page anchor like "[file p.X]" with sequence number when true page is unknown
    return f"{source_name} p.{idx}"

def _gaps_to_actions(df: pd.DataFrame):
    actions = []
    for _, r in df.iterrows():
        if str(r.get("status") or "").lower() in ("", "unresolved", "no", "gap"):
            txt = str(r.get("text") or "")
            req_id = r.get("req_id")
            sec = (r.get("section") or "").upper()
            factor = r.get("factor") or ""
            action = None
            if "past performance" in txt.lower():
                action = f"Collect 3-5 relevant past performance write-ups mapped to {req_id} ({sec} {factor})."
            elif "key personnel" in txt.lower() or "resume" in txt.lower():
                action = f"Assemble resumes and quals matrix for key personnel per {req_id}."
            elif "quality" in txt.lower() or "qa" in txt.lower():
                action = f"Draft Quality Control/Assurance Plan aligned to {req_id}."
            elif "management" in txt.lower() and "plan" in txt.lower():
                action = f"Draft Management Plan answering every must/shall in {req_id}."
            elif "technical" in txt.lower() and "approach" in txt.lower():
                action = f"Draft Technical Approach section covering {req_id} elements."
            elif "subcontract" in txt.lower():
                action = f"Identify subcontractor coverage and letters of commitment for {req_id}."
            elif "price" in txt.lower() or "pricing" in txt.lower() or "clins" in txt.lower():
                action = f"Build pricing workbook and narrative tied to CLINs for {req_id}."
            elif "security" in txt.lower() or "clearance" in txt.lower():
                action = f"Confirm security/clearance requirements and staffing implications for {req_id}."
            else:
                action = f"Draft compliant content to satisfy {req_id}; mirror language and provide evidence."
            actions.append({"req_id": req_id, "action": action})
    return pd.DataFrame(actions)

# ---------- core ----------

def _x9_build_lm(conn, rid: int):
    _ensure_tables(conn)
    with closing(conn.cursor()) as cur:
        cur.execute("DELETE FROM lm_reqs WHERE rfp_id=?;", (int(rid),))
        conn.commit()

    try:
        df_files = pd.read_sql_query("SELECT filename, text FROM files WHERE rfp_id=? ORDER BY id;", conn, params=(int(rid),))
    except Exception as e:
        st.error(f"Files load failed: {e}")
        return 0

    if df_files is None or df_files.empty:
        st.warning("No files to parse.")
        return 0

    total = 0
    rows_to_insert = []
    for idx, r in df_files.iterrows():
        name = r.get("filename") or "RFP"
        text = r.get("text") or ""
        L, M = _section_splits(text)
        srcL = _approx_chunk(name, 1)
        srcM = _approx_chunk(name, 1)
        L_reqs = _extract_requirements(L, "L", srcL)
        M_reqs = _extract_requirements(M, "M", srcM)
        # If no explicit L/M, fall back to global must/shall
        if not L_reqs and not M_reqs:
            fallback = _extract_requirements(text, "G", _approx_chunk(name, 1))
            for rr in fallback:
                rr["section"] = "G"
                rows_to_insert.append(rr)
        else:
            for rr in L_reqs:
                rr["section"] = "L"
                rows_to_insert.append(rr)
            for rr in M_reqs:
                rr["section"] = "M"
                rows_to_insert.append(rr)

    # persist
    with closing(conn.cursor()) as cur:
        for rr in rows_to_insert:
            cur.execute("""INSERT INTO lm_reqs(rfp_id, req_id, section, factor, subfactor, text, source, page_limit, weight, applies_to, status, created_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,'Unresolved',?);""",
                        (int(rid), rr["req_id"], rr["section"], rr["factor"], rr["subfactor"],
                         rr["text"], rr["source"], rr["page_limit"], rr["weight"], rr["applies_to"], _now()))
        conn.commit()
    total = len(rows_to_insert)
    return total

def _x9_ui(conn):
    st.markdown("### X9 — L&M Decoder + Compliance Matrix")
    st.caption("X9 active — parses L/M, builds compliance matrix, flags gaps, exports CSV")
    try:
        _rfps = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
    except Exception:
        _rfps = None
    if _rfps is None or _rfps.empty:
        st.info("No RFP found. Parse & save first.")
        return
    rid = st.selectbox("RFP context (for X9)", options=_rfps["id"].tolist(),
                       format_func=lambda i: f"#{i} — {_rfps.loc[_rfps['id']==i,'title'].values[0]}",
                       key="x9_rid")
    tabs = st.tabs(["L&M Decoder", "Compliance Matrix", "Gaps → Actions"])

    with tabs[0]:
        if st.button("Build L&M", key=f"x9_build_{rid}"):
            n = _x9_build_lm(conn, int(rid))
            if n > 0:
                st.success(f"Extracted {n} requirement lines.")
            else:
                st.warning("No L/M requirements found.")

    with tabs[1]:
        try:
            df = pd.read_sql_query("SELECT req_id, section, factor, subfactor, text, source, page_limit, weight, applies_to, status FROM lm_reqs WHERE rfp_id=? ORDER BY section, req_id;", conn, params=(int(rid),))
        except Exception as e:
            df = None
            st.error(f"Load matrix failed: {e}")
        if df is None or df.empty:
            st.info("Matrix empty. Run Build L&M.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
            # CSV export
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button("Download CSV", data=csv, file_name=f"rfp_{rid}_lm_matrix.csv", mime="text/csv", key=f"x9_csv_{rid}")

    with tabs[2]:
        try:
            df = pd.read_sql_query("SELECT req_id, section, factor, subfactor, text, status FROM lm_reqs WHERE rfp_id=? ORDER BY section, req_id;", conn, params=(int(rid),))
        except Exception as e:
            df = None
            st.error(f"Load for gaps failed: {e}")
        if df is None or df.empty:
            st.info("No data. Run Build L&M.")
        else:
            acts = _gaps_to_actions(df)
            if acts is not None and not acts.empty:
                st.dataframe(acts, use_container_width=True, hide_index=True)
                csv = acts.to_csv(index=False).encode("utf-8")
                st.download_button("Download Actions CSV", data=csv, file_name=f"rfp_{rid}_lm_actions.csv", mime="text/csv", key=f"x9_act_csv_{rid}")
            else:
                st.success("No gaps detected by heuristic.")

# Hook into existing analyzer
try:
    _orig_run_rfp_analyzer = run_rfp_analyzer
except NameError:
    _orig_run_rfp_analyzer = None

if _orig_run_rfp_analyzer:
    def run_rfp_analyzer(conn):
        _orig_run_rfp_analyzer(conn)
        try:
            _x9_ui(conn)
        except Exception as e:
            st.info(f"X9 panel unavailable: {e}")
