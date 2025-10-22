
# x13_addon.py — X13 RFQ Pack Builder v1
# Import in app.py after other addons:
#   import x13_addon
#
# Adds "X13 — RFQ Pack Builder v1" to RFP Analyzer:
# - Assemble a submission-ready ZIP: cover letter, technical draft, pricing, compliance, checklist, stubs
# - Pulls from X9 (lm_reqs), X10 (session drafts), X11 (pricing_items), and metadata tables
# - Filename conventions and ordering
# - No feature flags required

import io, zipfile, re
from contextlib import closing
import datetime as _dt

try:
    import streamlit as st
    import pandas as pd
    import numpy as np
except Exception as _e:
    raise

# ---------------- models / client ----------------

def _x13_models():
    try:
        m = st.secrets.get("models", {})
        chat = m.get("writer") or m.get("heavy") or st.secrets.get("x8_model") or "gpt-5"
        return chat
    except Exception:
        return "gpt-5"

def _x13_client():
    try:
        return client, None  # reuse
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

def _x13_fetch_meta(conn, rid: int) -> dict:
    meta = {"title":"", "agency":"", "sol_number":"", "naics":"", "set_aside":"", "due":"","pop":"","poc_name":"","poc_email":"","poc_phone":""}
    try:
        rfps = pd.read_sql_query("SELECT id, title, agency, sol_number FROM rfps WHERE id=?;", conn, params=(int(rid),))
        if rfps is not None and not rfps.empty:
            r = rfps.iloc[0]
            meta["title"] = str(r.get("title") or "")
            meta["agency"] = str(r.get("agency") or "")
            meta["sol_number"] = str(r.get("sol_number") or "")
    except Exception:
        pass
    try:
        ddf = pd.read_sql_query("SELECT label, date_text FROM key_dates WHERE rfp_id=?;", conn, params=(int(rid),))
        if ddf is not None and not ddf.empty:
            due = ddf[ddf["label"].str.contains("due|closing|close", case=False, na=False)]
            if due is not None and not due.empty:
                meta["due"] = str(due.iloc[0].get("date_text") or "")
    except Exception:
        pass
    try:
        m = pd.read_sql_query("SELECT key, value FROM rfp_meta WHERE rfp_id=?;", conn, params=(int(rid),))
        def gv(k):
            try:
                v = m.loc[m["key"]==k,"value"]
                return v.values[0] if len(v) else ""
            except Exception:
                return ""
        meta["naics"] = gv("naics")
        meta["set_aside"] = gv("set_aside")
        meta["pop"] = gv("pop_summary") or gv("pop_structure")
    except Exception:
        pass
    try:
        p = pd.read_sql_query("SELECT name, role, email, phone FROM pocs WHERE rfp_id=?;", conn, params=(int(rid),))
        if p is not None and not p.empty:
            def _dom(e):
                try: return (e or "").split("@",1)[1].lower()
                except Exception: return ""
            df = p.fillna("")
            df["domain"] = df["email"].apply(_dom)
            gov = df["domain"].str.contains(r"\.(gov|mil)$", case=False, regex=True).astype(int)
            rolew = df["role"].str.contains(r"contract(ing)? officer|\bko\b|contract specialist|\bcor\b", case=False, regex=True).astype(int) * 2
            freq = df["domain"].map(df["domain"].value_counts().to_dict())
            score = gov + rolew + freq
            best = df.sort_values(["score"], ascending=False).iloc[0]
            meta["poc_name"] = (best.get("name") or "").strip()
            meta["poc_email"] = (best.get("email") or "").strip()
            meta["poc_phone"] = (best.get("phone") or "").strip()
    except Exception:
        pass
    return meta

def _x13_load_compliance(conn, rid: int) -> pd.DataFrame:
    try:
        df = pd.read_sql_query("SELECT req_id, section, factor, subfactor, text, source, page_limit, weight, applies_to, status FROM lm_reqs WHERE rfp_id=? ORDER BY section, req_id;", conn, params=(int(rid),))
        return df
    except Exception:
        return pd.DataFrame(columns=["req_id","section","factor","subfactor","text","source","page_limit","weight","applies_to","status"])

def _x13_load_pricing(conn, rid: int) -> pd.DataFrame:
    try:
        df = pd.read_sql_query("SELECT source, line_no, clin, desc, uom, qty, unit_price, ext_price, period FROM pricing_items WHERE rfp_id=? ORDER BY source, period, line_no;", conn, params=(int(rid),))
        return df
    except Exception:
        return pd.DataFrame(columns=["source","line_no","clin","desc","uom","qty","unit_price","ext_price","period"])

def _clean_num(s):
    if s is None: return None
    s = str(s).replace("$","").replace(",","").strip()
    if not s: return None
    try: return float(s)
    except Exception: return None

def _x13_compute_pricing(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    out = df.copy()
    out["qty"] = out["qty"].apply(_clean_num)
    out["unit_price"] = out["unit_price"].apply(_clean_num)
    def _ext(r):
        if pd.notna(r.get("ext_price")) and str(r.get("ext_price")).strip():
            try: return float(str(r.get("ext_price")).replace(",","").replace("$",""))
            except Exception: return None
        q = r.get("qty"); u = r.get("unit_price")
        if pd.notna(q) and pd.notna(u):
            try: return float(q) * float(u)
            except Exception: return None
        return None
    out["ext_price"] = out.apply(_ext, axis=1)
    return out

def _x13_collect_drafts(rid: int) -> str:
    key = f"x10_drafts_{rid}"
    data = st.session_state.get(key, {})
    if not data:
        return "# Technical Volume\n\n_Draft not found in session. Use X10 to generate sections._\n"
    parts = []
    for sk, v in data.items():
        parts.append(f"## {sk}\n\n{v.get('text') or ''}\n")
    return "\n\n".join(parts)

def _x13_checklist(df_lm: pd.DataFrame) -> str:
    if df_lm is None or df_lm.empty:
        return "# Submission Checklist\n\n- Build technical volume per Section L.\n- Include pricing and signed reps/certs.\n"
    items = []
    for _, r in df_lm.iterrows():
        txt = str(r.get("text") or "")
        snippet = (txt[:180] + "…") if len(txt) > 180 else txt
        items.append(f"- [{r.get('req_id')}] {snippet}")
    return "# Submission Checklist (from Section L/M)\n\n" + "\n".join(items) + "\n"

def _x13_cover_letter(meta: dict, extra: str) -> str:
    chat_model = _x13_models()
    c, err = _x13_client()
    # Fallback cover if client missing
    if err or c is None:
        return (f"# Cover Letter\n\n"
                f"{meta.get('agency','')} — {meta.get('title','')} (Solicitation {meta.get('sol_number','')})\n\n"
                f"Dear Contracting Officer,\n\n"
                f"Please find enclosed our compliant proposal in response to the above solicitation. "
                f"We confirm submission by {meta.get('due','TBD')} and our understanding of the POP {meta.get('pop','TBD')}.\n\n"
                f"Sincerely,\nYour Company\n")
    sys = ("You write concise, compliant cover letters for federal proposal submissions. "
           "Use formal tone. 220 words max.")
    usr = (
        f"AGENCY: {meta.get('agency','')}\n"
        f"SOLICITATION: {meta.get('sol_number','')}\n"
        f"TITLE: {meta.get('title','')}\n"
        f"DUE: {meta.get('due','')}\n"
        f"POP: {meta.get('pop','')}\n"
        f"NAICS: {meta.get('naics','')}\n"
        f"SET-ASIDE: {meta.get('set_aside','')}\n"
        f"KO/POC: {meta.get('poc_name','')} <{meta.get('poc_email','')}> {meta.get('poc_phone','')}\n"
        f"EXTRA: {extra or ''}\n\n"
        "Draft a brief cover letter acknowledging receipt of the solicitation, our intent to comply with Section L instructions, "
        "and to meet Section M evaluation factors. Note our set-aside eligibility if relevant. "
        "Close with contact info and signature block placeholder."
    )
    try:
        resp = c.chat.completions.create(
            model=chat_model,
            messages=[{"role":"system","content":sys},{"role":"user","content":usr}],
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        return (f"# Cover Letter\n\n"
                f"{meta.get('agency','')} — {meta.get('title','')} (Solicitation {meta.get('sol_number','')})\n\n"
                f"Dear Contracting Officer,\n\n"
                f"Please find enclosed our compliant proposal.\n\nSincerely,\nYour Company\n")

def _x13_zip(conn, rid: int, opts: dict, extra_cover: str) -> bytes:
    meta = _x13_fetch_meta(conn, rid)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        stamp = _dt.datetime.utcnow().strftime("%Y%m%d")
        base = f"RFP{rid}_{meta.get('sol_number','SOL')}_{stamp}"
        # 00 Cover letter
        if opts.get("cover", True):
            cov = _x13_cover_letter(meta, extra_cover)
            z.writestr(f"{base}/00_Cover_Letter.md", cov)
        # 01 Technical
        if opts.get("technical", True):
            tech = _x13_collect_drafts(rid)
            z.writestr(f"{base}/01_Technical.md", tech)
        # 02 Pricing
        if opts.get("pricing_csv", True) or opts.get("pricing_xlsx", True):
            dfp = _x13_compute_pricing(_x13_load_pricing(conn, rid))
            if dfp is not None and not dfp.empty:
                if opts.get("pricing_csv", True):
                    z.writestr(f"{base}/02_Pricing.csv", dfp.to_csv(index=False))
                if opts.get("pricing_xlsx", True):
                    import io as _io
                    from pandas import ExcelWriter
                    b = _io.BytesIO()
                    with ExcelWriter(b, engine="xlsxwriter") as w:
                        dfp.to_excel(w, sheet_name="Pricing", index=False)
                    b.seek(0)
                    z.writestr(f"{base}/02_Pricing.xlsx", b.read())
        # 03 Compliance
        if opts.get("compliance", True):
            dfl = _x13_load_compliance(conn, rid)
            if dfl is not None and not dfl.empty:
                z.writestr(f"{base}/03_Compliance_Matrix.csv", dfl.to_csv(index=False))
                z.writestr(f"{base}/98_Submission_Checklist.md", _x13_checklist(dfl))
            else:
                z.writestr(f"{base}/98_Submission_Checklist.md", _x13_checklist(None))
        # 04 Stubs
        if opts.get("stubs", True):
            z.writestr(f"{base}/04_Past_Performance.md", "# Past Performance\n\n_Add 3–5 relevant references._\n")
            z.writestr(f"{base}/05_Reps_and_Certs_PLACEHOLDER.txt", "Upload signed SF forms and reps/certs here.\n")
        # 99 Readme
        z.writestr(f"{base}/README.txt", "RFQ pack generated by X13 — RFQ Pack Builder v1.\n")
    buf.seek(0)
    return buf.read()

# ---------------- UI ----------------

def _x13_ui(conn):
    st.markdown("### X13 — RFQ Pack Builder v1")
    st.caption("X13 active — assemble cover letter, technical, pricing, compliance, checklist into a ZIP")
    try:
        _rfps = pd.read_sql_query("SELECT id, title FROM rfps ORDER BY id DESC;", conn, params=())
    except Exception:
        _rfps = None
    if _rfps is None or _rfps.empty:
        st.info("No RFP found. Parse & save first.")
        return
    rid = st.selectbox("RFP context (for X13)", options=_rfps["id"].tolist(),
                       format_func=lambda i: f"#{i} — {_rfps.loc[_rfps['id']==i,'title'].values[0]}",
                       key="x13_rid")

    st.subheader("Components")
    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        cover = st.checkbox("Cover letter", True, key=f"x13_cov_{rid}")
        technical = st.checkbox("Technical draft", True, key=f"x13_tech_{rid}")
    with c2:
        pricing_csv = st.checkbox("Pricing CSV", True, key=f"x13_pcsv_{rid}")
        pricing_xlsx = st.checkbox("Pricing XLSX", True, key=f"x13_px_{rid}")
    with c3:
        compliance = st.checkbox("Compliance matrix + checklist", True, key=f"x13_comp_{rid}")
        stubs = st.checkbox("Stubs (PP + Reps/Certs)", True, key=f"x13_stub_{rid}")

    extra = st.text_area("Optional note to include in cover letter", key=f"x13_extra_{rid}", height=80)

    if st.button("Build RFQ Pack (.zip)", key=f"x13_go_{rid}"):
        opts = {"cover":cover, "technical":technical, "pricing_csv":pricing_csv, "pricing_xlsx":pricing_xlsx, "compliance":compliance, "stubs":stubs}
        try:
            z = _x13_zip(conn, int(rid), opts, extra)
            st.success("RFQ Pack ready")
            st.download_button("Download RFQ Pack (.zip)", data=z, file_name=f"rfp_{rid}_rfq_pack.zip", mime="application/zip", key=f"x13_dl_{rid}")
        except Exception as e:
            st.error(f"Pack build failed: {e}")

# Hook into existing analyzer
try:
    _orig_run_rfp_analyzer = run_rfp_analyzer
except NameError:
    _orig_run_rfp_analyzer = None

if _orig_run_rfp_analyzer:
    def run_rfp_analyzer(conn):
        _orig_run_rfp_analyzer(conn)
        try:
            _x13_ui(conn)
        except Exception as e:
            st.info(f"X13 panel unavailable: {e}")
