# ===== app.py =====
# Full GovCon Copilot Pro with safe key loading, saved NAICS, editable goals, settings, templates

import os, re, io, json, sqlite3
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import pandas as pd
import numpy as np
import streamlit as st
import requests
from PyPDF2 import PdfReader
import docx
from sklearn.feature_extraction.text import TfidfVectorizer

# --- Safe key loader: env first, then st.secrets (if available), else "" ---
def _get_key(name: str) -> str:
    v = os.getenv(name, "")
    if v:
        return v
    try:
        return st.secrets[name]  # works on Streamlit Cloud or local secrets.toml
    except Exception:
        return ""

OPENAI_API_KEY     = _get_key("OPENAI_API_KEY")
GOOGLE_PLACES_KEY  = _get_key("GOOGLE_PLACES_API_KEY")
SAM_API_KEY        = _get_key("SAM_API_KEY")
MS_TENANT_ID       = _get_key("MS_TENANT_ID")
MS_CLIENT_ID       = _get_key("MS_CLIENT_ID")
MS_CLIENT_SECRET   = _get_key("MS_CLIENT_SECRET")

# --- OpenAI SDK import guard & client ---
try:
    import openai as _openai_pkg
    from openai import OpenAI  # requires openai>=1.0.0 (recommend >=1.40.0)
    _openai_version = getattr(_openai_pkg, "__version__", "unknown")
except Exception as e:
    raise RuntimeError(
        "OpenAI SDK is missing or too old. "
        "Fix: in requirements.txt set 'openai>=1.40.0' and redeploy. "
        f"Original import error: {e}"
    )
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

st.set_page_config(page_title="GovCon Copilot Pro", page_icon="🧰", layout="wide")
DB_PATH = "govcon.db"

# ---- Seed NAICS list (yours) ----
NAICS_SEEDS = [
    "561210","721110","562991","326191","336611","531120","531","722310","561990","722514","561612",
    "561730","311511","238990","311812","561720","811210","236118","238220","237990","311423",
    "562910","236220","332420","238320","541380","541519","561710","423730","238210","562211",
    "541214","541330","541512","541511","541370","611430","611699","611310","611710","562111","562119",
    "624230","488999","485510","485410","488510","541614","332994","334220","336992","561320","561311","541214"
]

# ---- Database schema ----
SCHEMA = {
    "vendors": """
    create table if not exists vendors (
        id integer primary key,
        company text,
        naics text,
        trades text,
        phone text,
        email text,
        website text,
        city text,
        state text,
        certifications text,
        set_asides text,
        notes text,
        source text,
        created_at text default current_timestamp,
        updated_at text default current_timestamp
    );
    """,
    "opportunities": """
    create table if not exists opportunities (
        id integer primary key,
        sam_notice_id text,
        title text,
        agency text,
        naics text,
        psc text,
        place_of_performance text,
        response_due text,
        posted text,
        type text,
        url text,
        attachments_json text,
        status text default 'New',
        created_at text default current_timestamp
    );
    """,
    "contacts": """
    create table if not exists contacts (
        id integer primary key,
        name text,
        org text,
        role text,
        email text,
        phone text,
        source text,
        notes text,
        created_at text default current_timestamp
    );
    """,
    "outreach_log": """
    create table if not exists outreach_log (
        id integer primary key,
        vendor_id integer,
        contact_method text,
        to_addr text,
        subject text,
        body text,
        sent_at text,
        status text,
        foreign key(vendor_id) references vendors(id)
    );
    """,
    "goals": """
    create table if not exists goals (
        id integer primary key,
        year integer,
        bids_target integer,
        revenue_target real,
        bids_submitted integer,
        revenue_won real
    );
    """,
    "settings": """
    create table if not exists settings (
        key text primary key,
        value text,
        updated_at text default current_timestamp
    );
    """,
    "email_templates": """
    create table if not exists email_templates (
        name text primary key,
        subject text,
        body text,
        updated_at text default current_timestamp
    );
    """,
    "naics_watch": """
    create table if not exists naics_watch (
        code text primary key,
        label text,
        created_at text default current_timestamp
    );
    """
}

def get_db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def ensure_schema():
    conn = get_db()
    cur = conn.cursor()
    for ddl in SCHEMA.values():
        cur.execute(ddl)

    # seed goals
    cur.execute("select count(*) from goals")
    if cur.fetchone()[0] == 0:
        cur.execute(
            "insert into goals(year,bids_target,revenue_target,bids_submitted,revenue_won) values(?,?,?,?,?)",
            (datetime.now().year, 156, 600000, 1, 0)
        )

    # seed settings
    defaults = {
        "company_name": "ELA Management LLC",
        "home_loc": "Houston, TX",
        "default_trade": "Janitorial",
        "outreach_subject": "Quote request for upcoming federal project",
        "outreach_scope": "Routine janitorial five days weekly include supplies supervision and reporting. Provide monthly price and any one time services."
    }
    for k, v in defaults.items():
        cur.execute("insert into settings(key,value) values(?,?) on conflict(key) do nothing", (k, v))

    # seed email template
    cur.execute("""
        insert into email_templates(name, subject, body)
        values(?,?,?)
        on conflict(name) do nothing
    """, ("RFQ Request",
          "Quote request for upcoming federal project",
          """Hello {company},

ELA Management LLC requests a quote for the following work.

Scope
{scope}

Please include unit and extended prices any exclusions start availability and certifications or set aside status.

Quote due
{due}

Thank you
ELA Management LLC
"""))

    # seed NAICS watch with initial list only if empty
    cur.execute("select count(*) from naics_watch")
    if cur.fetchone()[0] == 0:
        for c in sorted(set(NAICS_SEEDS)):
            cur.execute("insert into naics_watch(code,label) values(?,?)", (c, c))

    conn.commit()

ensure_schema()

# ---- Helpers ----
def get_setting(key, default=""):
    conn = get_db()
    row = conn.execute("select value from settings where key=?", (key,)).fetchone()
    return row[0] if row else default

def set_setting(key, value):
    conn = get_db()
    conn.execute("""
        insert into settings(key,value) values(?,?)
        on conflict(key) do update set value=excluded.value, updated_at=current_timestamp
    """, (key, str(value)))
    conn.commit()

def read_doc(uploaded_file):
    suffix = uploaded_file.name.lower().split(".")[-1]
    if suffix in ["doc","docx"]:
        d = docx.Document(uploaded_file)
        return "\n".join(p.text for p in d.paragraphs)
    if suffix == "pdf":
        r = PdfReader(uploaded_file)
        return "\n".join((page.extract_text() or "") for page in r.pages)
    return uploaded_file.read().decode("utf-8", errors="ignore")

def llm(system, prompt, temp=0.2, max_tokens=1400):
    if not client:
        return "Set OPENAI_API_KEY to enable drafting."
    rsp = client.chat.completions.create(
        model="gpt-5-thinking",
        messages=[{"role":"system","content":system},{"role":"user","content":prompt}],
        temperature=temp, max_tokens=max_tokens,
    )
    return rsp.choices[0].message.content

def chunk_text(text, max_chars=1800, overlap=200):
    parts, i = [], 0
    while i < len(text):
        parts.append(text[i:i+max_chars])
        i += max_chars - overlap
    return parts

def embed_texts(texts):
    vec = TfidfVectorizer(stop_words="english")
    X = vec.fit_transform(texts)
    return vec, X

def search_chunks(query, vec, X, texts, k=6):
    qX = vec.transform([query])
    sims = (X @ qX.T).toarray().ravel()
    idx = sims.argsort()[::-1][:k]
    return [texts[i] for i in idx]

def to_xlsx_bytes(df_dict):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        for name, df in df_dict.items():
            df.to_excel(writer, index=False, sheet_name=name[:31])
    return bio.getvalue()

# ---- External integrations ----
def google_places_search(query, location="Houston, TX", radius_m=80000):
    if not GOOGLE_PLACES_KEY:
        return []
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": f"{query} {location}", "radius": radius_m, "key": GOOGLE_PLACES_KEY}
    r = requests.get(url, params=params, timeout=20)
    data = r.json()
    out = []
    for item in data.get("results", []):
        out.append({
            "company": item.get("name"),
            "website": "",
            "phone": "",
            "city": location.split(",")[0].strip() if "," in location else location,
            "state": location.split(",")[-1].strip() if "," in location else "",
            "source": "GooglePlaces",
            "notes": item.get("formatted_address","")
        })
    return out

def linkedin_company_search(keyword):
    return f"https://www.linkedin.com/search/results/companies/?keywords={quote_plus(keyword)}"

def send_via_graph(to_addr, subject, body):
    if not (MS_TENANT_ID and MS_CLIENT_ID and MS_CLIENT_SECRET):
        return "Graph not configured"
    token_r = requests.post(
        f"https://login.microsoftonline.com/{MS_TENANT_ID}/oauth2/v2.0/token",
        data={
            "client_id": MS_CLIENT_ID,
            "client_secret": MS_CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }, timeout=20
    )
    token = token_r.json().get("access_token")
    if not token:
        return "Graph token error"
    user_id = "me"  # or a specific mailbox address
    r = requests.post(
        f"https://graph.microsoft.com/v1.0/users/{user_id}/sendMail",
        headers={"Authorization": f"Bearer {token}", "Content-Type":"application/json"},
        json={
            "message": {
                "subject": subject,
                "body": {"contentType": "Text", "content": body},
                "toRecipients": [{"emailAddress": {"address": to_addr}}],
            },
            "saveToSentItems": "true",
        }, timeout=20
    )
    return "Sent" if r.status_code in (202,200) else f"Error {r.status_code}"

def sam_search(naics_list, min_days=3, limit=100):
    if not SAM_API_KEY:
        return pd.DataFrame()
    base = "https://api.sam.gov/opportunities/v2/search"
    min_due = datetime.utcnow().date() + timedelta(days=min_days)
    params = {
        "api_key": SAM_API_KEY,
        "limit": str(limit),
        "response": "json",
        "naics": ",".join([c for c in naics_list if c][:20]),
        "sort": "-publishedDate",
        "noticeType": "Combined Synopsis/Solicitation,Solicitation,Presolicitation,SRCSGT",
        "active": "true",
    }
    r = requests.get(base, params=params, timeout=30)
    data = r.json()
    rows = []
    for opp in data.get("opportunitiesData", []):
        due = opp.get("responseDeadLine") or ""
        try:
            due_ok = (not due) or (datetime.fromisoformat(due.replace("Z","")).date() >= min_due)
        except Exception:
            due_ok = True
        if not due_ok:
            continue
        docs = opp.get("documents", []) or []
        rows.append({
            "sam_notice_id": opp.get("noticeId"),
            "title": opp.get("title"),
            "agency": opp.get("organizationName"),
            "naics": ",".join(opp.get("naicsCodes", [])),
            "psc": ",".join(opp.get("productOrServiceCodes", [])) if opp.get("productOrServiceCodes") else "",
            "place_of_performance": opp.get("placeOfPerformance", {}).get("city",""),
            "response_due": due,
            "posted": opp.get("publishedDate",""),
            "type": opp.get("type",""),
            "url": f"https://sam.gov/opp/{opp.get('noticeId')}/view",
            "attachments_json": json.dumps([{"name":d.get("fileName"),"url":d.get("url")} for d in docs])
        })
    return pd.DataFrame(rows)

def save_opportunities(df):
    if df.empty:
        return
    conn = get_db()
    cur = conn.cursor()
    for _, r in df.iterrows():
        cur.execute("""insert into opportunities
            (sam_notice_id,title,agency,naics,psc,place_of_performance,response_due,posted,type,url,attachments_json,status)
            values(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (r.sam_notice_id, r.title, r.agency, r.naics, r.psc, r.place_of_performance, r.response_due, r.posted, r.type, r.url, r.attachments_json, "New"))
    conn.commit()

# ---- UI ----
st.title("GovCon Copilot Pro")
st.caption("Subcontractor sourcing, SAM watcher, proposal drafting, outreach, CRM, saved settings & goals")

with st.sidebar:
    st.subheader("Configuration")
    company_name = st.text_input("Company name", value=get_setting("company_name", "ELA Management LLC"))
    home_loc = st.text_input("Primary location", value=get_setting("home_loc", "Houston, TX"))
    default_trade = st.text_input("Default trade", value=get_setting("default_trade", "Janitorial"))
    if st.button("Save configuration"):
        set_setting("company_name", company_name)
        set_setting("home_loc", home_loc)
        set_setting("default_trade", default_trade)
        st.success("Saved")

    # API Key Status
    st.subheader("API Key Status")
    def _ok(v): return "✅" if v else "❌"
    st.markdown(f"**OpenAI Key:** {_ok(bool(OPENAI_API_KEY))}")
    st.markdown(f"**Google Places Key:** {_ok(bool(GOOGLE_PLACES_KEY))}")
    st.markdown(f"**SAM.gov Key:** {_ok(bool(SAM_API_KEY))}")
    st.caption(f"OpenAI SDK: {_openai_version}")

    # Saved NAICS Watch List
    st.subheader("Watch list NAICS")
    conn = get_db()
    df_saved = pd.read_sql_query("select code from naics_watch order by code", conn)
    saved_codes = df_saved["code"].tolist()
    naics_options = sorted(set(saved_codes + NAICS_SEEDS))
    st.multiselect(
        "Choose or type NAICS codes then Save",
        options=naics_options,
        default=saved_codes if saved_codes else sorted(set(NAICS_SEEDS[:20])),
        key="naics_watch"
    )
    new_code = st.text_input("Add a single NAICS code")
    col_n1, col_n2 = st.columns(2)
    with col_n1:
        if st.button("Add code"):
            val = (new_code or "").strip()
            if val:
                conn.execute("insert or ignore into naics_watch(code,label) values(?,?)", (val, val))
                conn.commit()
                st.success(f"Added {val}")
    with col_n2:
        if st.button("Clear all saved codes"):
            conn.execute("delete from naics_watch")
            conn.commit()
            st.success("Cleared saved codes")

    if st.button("Save NAICS list"):
        keep = sorted(set([c.strip() for c in st.session_state.naics_watch if str(c).strip()]))
        cur = conn.cursor()
        cur.execute("delete from naics_watch")
        for c in keep:
            cur.execute("insert into naics_watch(code,label) values(?,?)", (c, c))
        conn.commit()
        st.success("Saved NAICS watch list")

    naics_csv = st.file_uploader("Import NAICS CSV (column named 'code')", type=["csv"])
    if naics_csv and st.button("Import NAICS from CSV"):
        df_in = pd.read_csv(naics_csv)
        if "code" in df_in.columns:
            cur = conn.cursor()
            for c in df_in["code"].astype(str).fillna("").str.strip():
                if c:
                    cur.execute("insert or ignore into naics_watch(code,label) values(?,?)", (c, c))
            conn.commit()
            st.success("Imported NAICS codes")
        else:
            st.info("CSV must have a column named code")

    # Goals (editable)
    st.subheader("Goals")
    g = pd.read_sql_query("select * from goals limit 1", conn)
    if g.empty:
        conn.execute(
            "insert into goals(year,bids_target,revenue_target,bids_submitted,revenue_won) values(?,?,?,?,?)",
            (datetime.now().year, 156, 600000, 1, 0)
        )
        conn.commit()
        g = pd.read_sql_query("select * from goals limit 1", conn)
    row = g.iloc[0]
    goal_id = int(row["id"])

    with st.form("goals_form", clear_on_submit=False):
        col1, col2 = st.columns(2)
        with col1:
            bids_target = st.number_input("Bids target", min_value=0, step=1, value=int(row["bids_target"]))
            bids_submitted = st.number_input("Bids submitted", min_value=0, step=1, value=int(row["bids_submitted"]))
        with col2:
            revenue_target = st.number_input("Revenue target", min_value=0.0, step=1000.0, value=float(row["revenue_target"]))
            revenue_won = st.number_input("Revenue won", min_value=0.0, step=1000.0, value=float(row["revenue_won"]))
        saved = st.form_submit_button("Save goals")
        if saved:
            conn.execute(
                "update goals set bids_target=?, revenue_target=?, bids_submitted=?, revenue_won=? where id=?",
                (int(bids_target), float(revenue_target), int(bids_submitted), float(revenue_won), goal_id)
            )
            conn.commit()
            st.success("Goals updated")

    st.caption("Quick update")
    colq1, colq2 = st.columns(2)
    with colq1:
        if st.button("Log new bid"):
            conn.execute("update goals set bids_submitted = bids_submitted + 1 where id=?", (goal_id,))
            conn.commit()
            st.success("Bid logged")
    with colq2:
        add_amt = st.number_input("Add award amount", min_value=0.0, step=1000.0, value=0.0, key="award_add_amt")
        if st.button("Log award"):
            if add_amt > 0:
                conn.execute("update goals set revenue_won = revenue_won + ? where id=?", (float(add_amt), goal_id))
                conn.commit()
                st.success(f"Award logged for ${add_amt:,.0f}")
            else:
                st.info("Enter a positive amount")

    g = pd.read_sql_query("select * from goals limit 1", conn)
    row = g.iloc[0]
    st.metric("Bids target", int(row["bids_target"]))
    st.metric("Bids submitted", int(row["bids_submitted"]))
    st.metric("Revenue target", f"${float(row['revenue_target']):,.0f}")
    st.metric("Revenue won", f"${float(row['revenue_won']):,.0f}")

tabs = st.tabs([
    "Pipeline",
    "Subcontractor Finder",
    "Contacts",
    "Outreach",
    "SAM Watch",
    "RFP Analyzer",
    "Capability Statement",
    "White Paper Builder",
    "Data Export",
    "Auto extract",
    "Ask the doc"
])

# ---- Pipeline ----
with tabs[0]:
    st.subheader("Opportunities pipeline")
    conn = get_db()
    df_opp = pd.read_sql_query("select * from opportunities order by posted desc", conn)
    if df_opp.empty:
        st.info("No opportunities yet. Use SAM Watch to pull new items.")
    else:
        edit = st.data_editor(df_opp, use_container_width=True, num_rows="dynamic", key="opp_grid")
        if st.button("Save pipeline changes"):
            cur = conn.cursor()
            for _, r in edit.iterrows():
                cur.execute("update opportunities set status=?, response_due=?, title=?, agency=? where id=?",
                            (r["status"], r["response_due"], r["title"], r["agency"], int(r["id"])))
            conn.commit()
            st.success("Saved")

# ---- Subcontractor Finder ----
with tabs[1]:
    st.subheader("Find subcontractors and rank by fit")
    trade = st.text_input("Trade", value=get_setting("default_trade", "Janitorial"))
    loc = st.text_input("Search location", value=get_setting("home_loc", "Houston, TX"))
    naics_choice = st.multiselect("NAICS to tag new imports", options=sorted(set(NAICS_SEEDS)), default=[])

    colA, colB, colC = st.columns(3)
    with colA:
        if st.button("Google Places import"):
            results = google_places_search(f"{trade} small business", loc)
            if results:
                df_new = pd.DataFrame(results)
                st.dataframe(df_new, use_container_width=True)
                if st.button("Save to vendors"):
                    conn = get_db()
                    df_new["naics"] = ",".join(naics_choice) if naics_choice else ""
                    for _, r in df_new.iterrows():
                        conn.execute("""insert into vendors(company,naics,trades,phone,email,website,city,state,certifications,set_asides,notes,source)
                                     values(?,?,?,?,?,?,?,?,?,?,?,?)""",
                                     (r.get("company"), r.get("naics",""), trade, r.get("phone",""), r.get("email",""),
                                      r.get("website",""), r.get("city",""), r.get("state",""), "", "", r.get("notes",""), r.get("source","")))
                    conn.commit()
                    st.success("Saved")
            else:
                st.warning("No results or Places key missing")
    with colB:
        st.markdown("LinkedIn quick search")
        st.link_button("Open LinkedIn", linkedin_company_search(f"{trade} {loc}"))
    with colC:
        vendor_csv = st.file_uploader("Upload vendor CSV to merge", type=["csv"])
        if vendor_csv:
            df_u = pd.read_csv(vendor_csv)
            st.dataframe(df_u.head(50), use_container_width=True)
            if st.button("Append uploaded vendors"):
                conn = get_db()
                for _, r in df_u.fillna("").iterrows():
                    conn.execute("""insert into vendors(company,naics,trades,phone,email,website,city,state,certifications,set_asides,notes,source)
                                 values(?,?,?,?,?,?,?,?,?,?,?,?)""",
                                 (r.get("Company") or r.get("company"),
                                  r.get("NAICS") or r.get("naics",""),
                                  r.get("Trades") or r.get("trades", trade),
                                  r.get("Phone") or r.get("phone",""),
                                  r.get("Email") or r.get("email",""),
                                  r.get("Website") or r.get("website",""),
                                  r.get("City") or r.get("city",""),
                                  r.get("State") or r.get("state",""),
                                  r.get("Certifications") or r.get("certifications",""),
                                  r.get("SetAsides") or r.get("set_asides",""),
                                  r.get("Notes") or r.get("notes",""),
                                  "CSV"))
                conn.commit()
                st.success("Merged")

    st.markdown("Vendor table")
    conn = get_db()
    df_v = pd.read_sql_query("select * from vendors order by updated_at desc, created_at desc", conn)
    grid = st.data_editor(df_v, use_container_width=True, num_rows="dynamic", key="vendor_grid")
    if st.button("Save vendor edits"):
        cur = conn.cursor()
        for _, r in grid.iterrows():
            cur.execute("""update vendors set company=?, naics=?, trades=?, phone=?, email=?, website=?, city=?, state=?, certifications=?, set_asides=?, notes=?, updated_at=current_timestamp where id=?""",
                        (r["company"], r["naics"], r["trades"], r["phone"], r["email"], r["website"], r["city"], r["state"], r["certifications"], r["set_asides"], r["notes"], int(r["id"])))
        conn.commit()
        st.success("Saved")

# ---- Contacts ----
with tabs[2]:
    st.subheader("POC and networking hub")
    conn = get_db()
    df_c = pd.read_sql_query("select * from contacts order by created_at desc", conn)
    grid = st.data_editor(df_c, use_container_width=True, num_rows="dynamic", key="contacts_grid")
    if st.button("Save contacts"):
        cur = conn.cursor()
        for _, r in grid.iterrows():
            if pd.isna(r["id"]):
                cur.execute("""insert into contacts(name,org,role,email,phone,source,notes) values(?,?,?,?,?,?,?)""",
                            (r["name"], r["org"], r["role"], r["email"], r["phone"], r["source"], r["notes"]))
            else:
                cur.execute("""update contacts set name=?, org=?, role=?, email=?, phone=?, source=?, notes=? where id=?""",
                            (r["name"], r["org"], r["role"], r["email"], r["phone"], r["source"], r["notes"], int(r["id"])))
        conn.commit()
        st.success("Saved")

# ---- Outreach (templates + mail merge) ----
with tabs[3]:
    st.subheader("Outreach and mail merge")
    conn = get_db()
    df_v = pd.read_sql_query("select * from vendors", conn)

    st.markdown("Email template")
    t = pd.read_sql_query("select * from email_templates order by name", conn)
    names = t["name"].tolist() if not t.empty else ["RFQ Request"]
    pick_t = st.selectbox("Template", options=names)
    tpl = pd.read_sql_query("select subject, body from email_templates where name=?", conn, params=(pick_t,))
    subj_default = tpl.iloc[0]["subject"] if not tpl.empty else get_setting("outreach_subject", "")
    body_default = tpl.iloc[0]["body"] if not tpl.empty else get_setting("outreach_scope", "")

    subj = st.text_input("Subject", value=subj_default)
    body = st.text_area("Body with placeholders {company} {scope} {due}", value=body_default, height=220)
    if st.button("Save template"):
        conn.execute("""insert into email_templates(name, subject, body)
                        values(?,?,?)
                        on conflict(name) do update set subject=excluded.subject, body=excluded.body, updated_at=current_timestamp
                     """, (pick_t, subj, body))
        conn.commit()
        st.success("Template saved")

    picks = st.multiselect("Choose vendors to email", options=df_v["company"].tolist(), default=df_v["company"].tolist()[:10])
    scope_hint = st.text_area("Scope summary", value=get_setting("outreach_scope", ""))
    due = st.text_input("Quote due", value=(datetime.now()+timedelta(days=5)).strftime("%B %d, %Y 4 pm CT"))

    if st.button("Generate emails"):
        st.session_state["mail_bodies"] = []
        for name in picks:
            row = df_v[df_v["company"] == name].head(1).to_dict(orient="records")[0]
            to_addr = row.get("email","")
            body_filled = body.format(company=name, scope=scope_hint, due=due)
            st.session_state["mail_bodies"].append({"to": to_addr, "subject": subj, "body": body_filled, "vendor_id": int(row["id"])})
        st.success(f"Prepared {len(st.session_state['mail_bodies'])} emails")

    if st.session_state.get("mail_bodies"):
        send_method = st.selectbox("Send with", ["Preview only","Microsoft Graph"])
        if st.button("Send now"):
            sent = 0
            for m in st.session_state["mail_bodies"]:
                if send_method == "Microsoft Graph":
                    status = send_via_graph(m["to"], m["subject"], m["body"])
                else:
                    status = "Preview"
                get_db().execute("""insert into outreach_log(vendor_id,contact_method,to_addr,subject,body,sent_at,status)
                                 values(?,?,?,?,?,?,?)""",
                                 (m["vendor_id"], send_method, m["to"], m["subject"], m["body"], datetime.now().isoformat(), status))
                get_db().commit()
                sent += 1
            st.success(f"Processed {sent} messages")

# ---- SAM Watch ----
with tabs[4]:
    st.subheader("SAM.gov auto search with attachments")
    codes = pd.read_sql_query("select code from naics_watch order by code", get_db())["code"].tolist()
    st.caption(f"Using NAICS codes: {', '.join(codes) if codes else 'none'}")
    st.caption("Finds items due in three or more days across your NAICS")
    if st.button("Run search now"):
        df = sam_search(codes, min_days=3, limit=100)
        if df.empty:
            st.warning("No results or SAM key missing")
        else:
            st.dataframe(df, use_container_width=True)
            save_opportunities(df)
            st.success("Saved to pipeline")
    st.markdown("Latest saved opportunities")
    conn = get_db()
    st.dataframe(pd.read_sql_query("select title, agency, naics, response_due, url from opportunities order by posted desc limit 50", conn), use_container_width=True)

# ---- RFP Analyzer ----
with tabs[5]:
    st.subheader("Analyze RFP or RFQ and build compliance")
    up = st.file_uploader("Upload RFP RFQ PWS or SOW PDF or DOCX", type=["pdf","docx","doc","txt"], accept_multiple_files=True)
    if up and st.button("Analyze"):
        full = "\n\n".join([read_doc(f) for f in up])[:400000]
        system = "You are a federal proposal analyst who extracts compliance matrices and outlines."
        prompt = f"""
From the following RFP content produce
1 a compliance matrix table with columns Requirement Section ID Text Deliverable Due Date Owner
2 an evaluation factors summary with weightings if stated
3 a proposal outline mapped to requirements including technical management staffing past performance price volumes
4 a QnA list of clarifications to ask the government
5 a risk register with mitigations

RFP content
{full}
"""
        out = llm(system, prompt, max_tokens=2000)
        st.markdown(out)

# ---- Capability Statement ----
with tabs[6]:
    st.subheader("Capability statement builder")
    company = get_setting("company_name", "ELA Management LLC")
    tagline = st.text_input("Tagline", value="Responsive project management for federal facilities and services")
    core = st.text_area("Core competencies", value="Janitorial Landscaping Staffing Logistics Construction Support IT Charter buses Lodging Security Education Training Disaster relief")
    diff = st.text_area("Differentiators", value="Fast mobilization • Quality controls • Transparent reporting • Nationwide partner network")
    past_perf = st.text_area("Representative experience", value="Project A: Custodial support, 100k sq ft. Project B: Grounds keeping, 200 acres.")
    contact = st.text_area("Contact info", value="ELA Management LLC • info@elamanagement.com • 555 555 5555 • UEI XXXXXXX • CAGE XXXXX")
    if st.button("Generate one page"):
        system = "Format a one page federal capability statement in markdown. Use clean headings and short bullets."
        prompt = f"""
Company {company}
Tagline {tagline}
Core {core}
Diff {diff}
Past performance {past_perf}
Contact {contact}
NAICS {", ".join(sorted(set(NAICS_SEEDS)))}
Certifications Small Business
Goals 156 bids and 600000 revenue this year. Submitted 1 to date.
"""
        out = llm(system, prompt, max_tokens=900)
        st.markdown(out)

# ---- White Paper ----
with tabs[7]:
    st.subheader("White paper builder")
    title = st.text_input("Title", value="Improving Facility Readiness with Outcome based Service Contracts")
    thesis = st.text_area("Thesis", value="Outcome based service contracts reduce total cost and improve satisfaction when paired with clear SLAs and transparent data.")
    audience = st.text_input("Audience", value="Facility Managers • Contracting Officers • Program Managers")
    if st.button("Draft white paper"):
        system = "Write a two page white paper with executive summary, problem, approach, case vignette, and implementation steps. Use clear headings and tight language."
        prompt = f"Title {title}\nThesis {thesis}\nAudience {audience}"
        out = llm(system, prompt, max_tokens=1400)
        st.markdown(out)

# ---- Data Export ----
with tabs[8]:
    st.subheader("Export to Excel workbook")
    conn = get_db()
    v = pd.read_sql_query("select * from vendors", conn)
    o = pd.read_sql_query("select * from opportunities", conn)
    c = pd.read_sql_query("select * from contacts", conn)
    bytes_xlsx = to_xlsx_bytes({"Vendors": v, "Opportunities": o, "Contacts": c})
    st.download_button("Download Excel workbook", data=bytes_xlsx, file_name="govcon_hub.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ---- Auto Extract ----
with tabs[9]:
    st.subheader("Auto extract key details")
    up = st.file_uploader("Upload solicitation or PWS", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key="auto_up")
    if up and st.button("Extract"):
        combined = "\n\n".join(read_doc(f) for f in up)
        chunks = chunk_text(combined)
        vec, X = embed_texts(chunks)
        snips = search_chunks(
            "scope technical specs performance metrics timeline deliverables submission instructions evaluation factors price schedule wage determination place of performance points of contact site visit clauses",
            vec, X, chunks, k=10
        )
        system = "You are a federal contracting assistant. Use headings and tight bullets."
        prompt = "Source slices\n" + "\n\n".join(snips) + "\n\nExtract fields now"
        st.markdown(llm(system, prompt, max_tokens=1200))

# ---- Ask the Doc ----
with tabs[10]:
    st.subheader("Ask questions over the uploaded docs")
    up2 = st.file_uploader("Upload PDFs or DOCX", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key="qna_up")
    q = st.text_input("Your question")
    if up2 and q and st.button("Answer"):
        combined = "\n\n".join(read_doc(f) for f in up2)
        chunks = chunk_text(combined)
        vec, X = embed_texts(chunks)
        snips = search_chunks(q, vec, X, chunks, k=6)
        support = "\n\n".join(snips)
        system = "Answer directly. Quote exact lines for dates or addresses."
        prompt = f"Context\n{support}\n\nQuestion\n{q}"
        st.markdown(llm(system, prompt, max_tokens=900))
# ===== end app.py =====
