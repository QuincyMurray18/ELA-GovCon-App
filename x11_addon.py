
# x11_addon.py — X11 Pricing Pack v1
# Import in app.py after other addons:
#   import x11_addon
#
# Adds "X11 — Pricing Pack v1" to RFP Analyzer:
# - Parse pricing tables from solicitation text (PDF/DOCX/TXT) already in DB
# - Heuristic CLIN extractor, QTY/UOM detection, Base/Option mapping
# - Editable matrix with auto extensions (qty * unit)
# - CSV/XLSX export

import re
from contextlib import closing
import io

try:
    import streamlit as st
    import pandas as pd
    import numpy as np
except Exception as _e:
    raise

# ------------ helpers ------------

_NUM = re.compile(r"[-+]?\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d+)?")
_CLIN = re.compile(r"\b(?:CLIN|Item(?:\s*No\.?)?|Line\s*Item)\s*[:#]?\s*([A-Z]?\d{3,6}[A-Z]?)\b", re.I)
_SIMPLE_CLIN = re.compile(r"^\s*([A-Z]?\d{3,6}[A-Z]?)\s{2,}", re.I)
_QTY = re.compile(r"\b(qty|quantity)\b[:#]?\s*(\d{1,9}(?:,\d{3})*)?", re.I)
_UOM = re.compile(r"\b(ea|each|month|mo|year|yr|day|hour|hr|lot|job|lb|lbs|ft|sf|lf|gallon|gal|ton|unit|set)\b", re.I)
_PERIOD = re.compile(r"(base(?:\s*year)?|option\s*year\s*\d+|option\s*\d+|oy\s*\d+|oy\d+)", re.I)

def _clean_num(s):
    if s is None: return None
    s = str(s)
    s = s.replace("$","").replace(",","").strip()
    if not s: return None
    try:
        return float(s)
    except Exception:
        m = _NUM.search(s)
        if m:
            try:
                return float(m.group(0).replace("$","").replace(",",""))
            except Exception:
                return None
        return None

def _find_period(text_line):
    m = _PERIOD.search(text_line or "")
    if not m: 
        return ""
    t = m.group(0).lower()
    t = t.replace("  "," ").strip()
    t = t.replace("option year","OY")
    t = t.replace("option","OY")
    t = t.replace("year","")
    t = t.replace("oy ", "OY")
    t = t.replace("oy", "OY")
    t = t.replace("base ", "Base ")
    if t.startswith("base"):
        return "Base"
    # normalize OY number
    n = re.findall(r"\d+", t)
    if n:
        return f"OY{n[0]}"
    return "Option"

def _row_from_line(line, cur_period):
    # Try to split a likely table line
    raw = line.strip()
    if not raw or len(raw) < 3:
        return None
    clin = None
    m = _CLIN.search(raw)
    if m:
        clin = m.group(1).upper()
    else:
        m2 = _SIMPLE_CLIN.match(line)
        if m2:
            clin = m2.group(1).upper()

    qty = None
    m = _QTY.search(raw)
    if m:
        qty = _clean_num(m.group(2))

    # pick first unit-like token
    uom = ""
    mu = _UOM.search(raw)
    if mu:
        uom = mu.group(0)

    # guess description by removing obvious tokens
    desc = raw
    desc = _CLIN.sub("", desc)
    desc = _QTY.sub("", desc)
    desc = _UOM.sub("", desc)
    desc = re.sub(r"\s{2,}", " ", desc).strip(" -:")

    return {
        "clin": clin or "",
        "desc": desc[:400],
        "uom": uom,
        "qty": qty if qty is not None else "",
        "unit_price": "",
        "ext_price": "",
        "period": cur_period or ""
    }

def _extract_pricing_rows(text):
    rows = []
    if not text:
        return rows
    period = ""
    for ln in str(text).splitlines():
        p = _find_period(ln)
        if p:
            period = p
        r = _row_from_line(ln, period)
        if r and (r["clin"] or (r["qty"] or r["uom"])):
            rows.append(r)
    return rows

def _ensure_tables(conn):
    with closing(conn.cursor()) as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS pricing_items(
            id INTEGER PRIMARY KEY,
            rfp_id INTEGER NOT NULL,
            source TEXT,
            line_no INTEGER,
            clin TEXT,
            desc TEXT,
            uom TEXT,
            qty REAL,
            unit_price REAL,
            ext_price REAL,
            period TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pricing_rfp ON pricing_items(rfp_id);")
        conn.commit()

# ------------ core ------------

def _x11_build_pricing(conn, rid: int):
    _ensure_tables(conn)
    with closing(conn.cursor()) as cur:
        cur.execute("DELETE FROM pricing_items WHERE rfp_id=?;", (int(rid),))
        conn.commit()

    try:
        df_files = pd.read_sql_query("SELECT filename, text FROM files WHERE rfp_id=? ORDER BY id;", conn, params=(int(rid),))
    except Exception as e:
        st.error(f"Files load failed: {e}")
        return 0

    if df_files is None or df_files.empty:
        st.warning("No files to parse.")
        return 0

    rows_to_insert = []
    for _, r in df_files.iterrows():
        name = r.get("filename") or "RFP"
        text = r.get("text") or ""
        rows = _extract_pricing_rows(text)
        # attach source and line numbers
        for i, rr in enumerate(rows, 1):
            rr["source"] = name
            rr["line_no"] = i
            rows_to_insert.append(rr)

    if not rows_to_insert:
        return 0

    with closing(conn.cursor()) as cur:
        for rr in rows_to_insert:
            cur.execute("""INSERT INTO pricing_items(rfp_id, source, line_no, clin, desc, uom, qty, unit_price, ext_price, period)
                           VALUES(?,?,?,?,?,?,?,?,?,?);""",
                        (int(rid), rr.get("source"), int(rr.get("line_no") or 0), rr.get("clin"),
                         rr.get("desc"), rr.get("uom"), rr.get("qty") if rr.get("qty")!="" else None,
                         rr.get("unit_price") if rr.get("unit_price")!="" else None,
                         rr.get("ext_price") if rr.get("ext_price")!="" else None,
                         rr.get("period")))
        conn.commit()
    return len(rows_to_insert)

def _x11_load_matrix(conn, rid: int) -> pd.DataFrame:
    try:
        df = pd.read_sql_query(
            "SELECT id, source, line_no, clin, desc, uom, qty, unit_price, ext_price, period "
            "FROM pricing_items WHERE rfp_id=? ORDER BY source, period, line_no;",
            conn, params=(int(rid),)
        )
        return df
    except Exception:
        return pd.DataFrame(columns=["id","source","line_no","clin","desc","uom","qty","unit_price","ext_price","period"])

def _x11_compute(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: 
        return df
    out = df.copy()
    # Normalize numbers
    for col in ["qty","unit_price","ext_price"]:
        out[col] = out[col].apply(_clean_num)
    # Auto ext_price if missing and qty/unit present
    def _ext(r):
        if pd.notna(r.get("ext_price")) and float(r["ext_price"])>0:
            return r["ext_price"]
        q = r.get("qty"); u = r.get("unit_price")
        if pd.notna(q) and pd.notna(u):
            try: return float(q) * float(u)
            except Exception: return None
        return None
    out["ext_price"] = out.apply(_ext, axis=1)
    return out

def _x11_export_xlsx(df: pd.DataFrame, rid: int) -> bytes:
    import pandas as _pd
    import io
    buf = io.BytesIO()
    with _pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Pricing", index=False)
        ws = writer.sheets["Pricing"]
        # basic formatting
        for i, col in enumerate(df.columns):
            width = max(12, min(60, int(df[col].astype(str).str.len().quantile(0.9)) + 4))
            ws.set_column(i, i, width)
    buf.seek(0)
    return buf.read()

# ------------ UI ------------

def _x11_ui(conn):
    st.markdown("### X11 — Pricing Pack v1")
    st.caption("X11 active — CLIN/QTY/UOM extraction, Base/Option mapping, editable matrix, CSV/XLSX export")
    try:
        _rfps = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
    except Exception:
        _rfps = None
    if _rfps is None or _rfps.empty:
        st.info("No RFP found. Parse & save first.")
        return
    rid = st.selectbox("RFP context (for X11)", options=_rfps["id"].tolist(),
                       format_func=lambda i: f"#{i} — {_rfps.loc[_rfps['id']==i,'title'].values[0]}",
                       key="x11_rid")

    c1, c2, c3 = st.columns([2,2,2])
    with c1:
        if st.button("Parse pricing", key=f"x11_build_{rid}"):
            n = _x11_build_pricing(conn, int(rid))
            if n>0: st.success(f"Extracted {n} pricing rows")
            else: st.warning("No pricing rows detected")
    with c2:
        refresh = st.button("Refresh", key=f"x11_refresh_{rid}")
    with c3:
        show_all = st.checkbox("Show blank rows", value=False, key=f"x11_blank_{rid}")

    if refresh or True:
        df = _x11_load_matrix(conn, int(rid))
        df = _x11_compute(df)
        if not show_all:
            # hide rows without any CLIN/QTY/UOM
            mask = (df["clin"].astype(str).str.len()>0) | df["uom"].astype(str).str.len()>0 | df["qty"].notna()
            df = df[mask].reset_index(drop=True)
        if df is None or df.empty:
            st.info("No pricing matrix. Click Parse pricing.")
        else:
            st.caption("Edit cells if needed. Extensions auto-calc when qty and unit_price set.")
            edit_cols = ["clin","desc","uom","qty","unit_price","ext_price","period"]
            df_edit = st.data_editor(df[["id","source","line_no"]+edit_cols], use_container_width=True, hide_index=True, num_rows="dynamic")
            # compute and show totals
            df_show = df_edit.copy()
            df_show = _x11_compute(df_show)
            total = df_show["ext_price"].fillna(0).sum()
            st.markdown(f"**Total (calculated): ${total:,.2f}**")
            # Export
            csv = df_show.to_csv(index=False).encode("utf-8")
            st.download_button("Download CSV", data=csv, file_name=f"rfp_{rid}_pricing.csv", mime="text/csv", key=f"x11_csv_{rid}")
            try:
                xlsb = _x11_export_xlsx(df_show, int(rid))
                st.download_button("Download XLSX", data=xlsb, file_name=f"rfp_{rid}_pricing.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"x11_xlsx_{rid}")
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
            _x11_ui(conn)
        except Exception as e:
            st.info(f"X11 panel unavailable: {e}")
