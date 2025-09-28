# ===== app.py =====
import os, re, io, json, sqlite3, time
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urljoin, urlparse

import pandas as pd
import numpy as np
import streamlit as st
import requests
from PyPDF2 import PdfReader
import docx
from sklearn.feature_extraction.text import TfidfVectorizer

# Optional HTML parsing for email scraper
try:
    from bs4 import BeautifulSoup  # pip install beautifulsoup4
except Exception:
    BeautifulSoup = None

# ---------- Safe key loader ----------
def _get_key(name: str) -> str:
    v = os.getenv(name, "")
    if v:
        return v
    try:
        return st.secrets[name]
    except Exception:
        return ""

OPENAI_API_KEY     = (_get_key("OPENAI_API_KEY") or "").strip()
GOOGLE_PLACES_KEY  = (_get_key("GOOGLE_PLACES_API_KEY") or "").strip()
SAM_API_KEY        = (_get_key("SAM_API_KEY") or "").strip()
MS_TENANT_ID       = (_get_key("MS_TENANT_ID") or "").strip()
MS_CLIENT_ID       = (_get_key("MS_CLIENT_ID") or "").strip()
MS_CLIENT_SECRET   = (_get_key("MS_CLIENT_SECRET") or "").strip()

# ---------- OpenAI client ----------
try:
    import openai as _openai_pkg
    from openai import OpenAI  # openai>=1.40.0 recommended
    _openai_version = getattr(_openai_pkg, "__version__", "unknown")
except Exception as e:
    raise RuntimeError("OpenAI SDK missing or too old. Install: openai>=1.40.0") from e

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
OPENAI_MODEL = os.getenv("OPENAI_MODEL", _get_key("OPENAI_MODEL") or "gpt-5-chat-latest")
_OPENAI_FALLBACK_MODELS = [
    OPENAI_MODEL,
    "gpt-5-chat-latest","gpt-5","gpt-5-2025-08-07",
    "gpt-5-mini","gpt-5-mini-2025-08-07",
    "gpt-5-nano","gpt-5-nano-2025-08-07",
    "gpt-4o-mini","gpt-4o",
]

st.set_page_config(page_title="GovCon Copilot Pro", page_icon="ðŸ§°", layout="wide")
DB_PATH = "govcon.db"

NAICS_SEEDS = [
    "561210","721110","562991","326191","336611","531120","531","722310","561990","722514","561612",
    "561730","311511","238990","311812","561720","811210","236118","238220","237990","311423",
    "562910","236220","332420","238320","541380","541519","561710","423730","238210","562211",
    "541214","541330","541512","541511","541370","611430","611699","611310","611710","562111","562119",
    "624230","488999","485510","485410","488510","541614","332994","334220","336992","561320","561311","541214"
]

SCHEMA = {
    "vendors": """
    create table if not exists vendors (
        id integer primary key,
        company text, naics text, trades text, phone text, email text, website text,
        city text, state text, certifications text, set_asides text, notes text, source text,
        created_at text default current_timestamp, updated_at text default current_timestamp
    );
    """,
    "opportunities": """
    create table if not exists opportunities (
        id integer primary key,
        sam_notice_id text, title text, agency text, naics text, psc text,
        place_of_performance text, response_due text, posted text, type text, url text,
        attachments_json text, status text default 'New', created_at text default current_timestamp
    );
    """,
    "contacts": """
    create table if not exists contacts (
        id integer primary key,
        name text, org text, role text, email text, phone text, source text, notes text,
        created_at text default current_timestamp
    );
    """,
    "outreach_log": """
    create table if not exists outreach_log (
        id integer primary key,
        vendor_id integer, contact_method text, to_addr text, subject text, body text, sent_at text, status text,
        foreign key(vendor_id) references vendors(id)
    );
    """,
    "goals": """
    create table if not exists goals (
        id integer primary key,
        year integer, bids_target integer, revenue_target real, bids_submitted integer, revenue_won real
    );
    """,
    "settings": """
    create table if not exists settings (
        key text primary key, value text, updated_at text default current_timestamp
    );
    """,
    "email_templates": """
    create table if not exists email_templates (
        name text primary key, subject text, body text, updated_at text default current_timestamp
    );
    """,
    "naics_watch": """
    create table if not exists naics_watch (
        code text primary key, label text, created_at text default current_timestamp
    );
    """,
    "chat_sessions": """
    create table if not exists chat_sessions (
        id integer primary key, title text, created_at text default current_timestamp
    );
    """,
    "chat_messages": """
    create table if not exists chat_messages (
        id integer primary key, session_id integer, role text, content text,
        created_at text default current_timestamp,
        foreign key(session_id) references chat_sessions(id)
    );
    """,
    "chat_files": """
    create table if not exists chat_files (
        id integer primary key,
        session_id integer,
        filename text,
        mimetype text,
        content_text text,
        uploaded_at text default current_timestamp,
        foreign key(session_id) references chat_sessions(id)
    );
    """,
}

SCHEMA.update({
    "rfp_sessions": """
    create table if not exists rfp_sessions (
        id integer primary key,
        title text,
        created_at text default current_timestamp
    );
    """,
    "rfp_messages": """
    create table if not exists rfp_messages (
        id integer primary key,
        session_id integer,
        role text,
        content text,
        created_at text default current_timestamp,
        foreign key(session_id) references rfp_sessions(id)
    );
    """,
    "rfp_files": """
    create table if not exists rfp_files (
        id integer primary key,
        session_id integer,
        filename text,
        mimetype text,
        content_text text,
        uploaded_at text default current_timestamp,
        foreign key(session_id) references rfp_sessions(id)
    );
    """
})
def get_db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def run_migrations():
    conn = get_db()
    cur = conn.cursor()
    # opportunities table expansions
    try: cur.execute("alter table opportunities add column assignee text")
    except Exception: pass
    try: cur.execute("alter table opportunities add column quick_note text")
    except Exception: pass
    # vendors table expansions
    try: cur.execute("alter table vendors add column distance_miles real")
    except Exception: pass
        conn.commit()


def ensure_schema():
    conn = get_db()
    cur = conn.cursor()
    for ddl in SCHEMA.values(): cur.execute(ddl)
    # seed goals
    cur.execute("select count(*) from goals")
    if cur.fetchone()[0] == 0:
        cur.execute(
            "insert into goals(year,bids_target,revenue_target,bids_submitted,revenue_won) values(?,?,?,?,?)",
            (datetime.now().year, 156, 600000, 1, 0)
        )
    defaults = {
        "company_name": "ELA Management LLC",
        "home_loc": "Houston, TX",
        "default_trade": "Janitorial",
        "outreach_subject": "Quote request for upcoming federal project",
        "outreach_scope": "Routine janitorial five days weekly include supplies supervision and reporting. Provide monthly price and any one time services."
    }
    for k, v in defaults.items():
        cur.execute("insert into settings(key,value) values(?,?) on conflict(key) do nothing", (k, v))
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
    cur.execute("select count(*) from naics_watch")
    if cur.fetchone()[0] == 0:
        for c in sorted(set(NAICS_SEEDS)):
            cur.execute("insert into naics_watch(code,label) values(?,?)", (c, c))
        conn.commit()

ensure_schema()

# ---------- Utilities ----------
def get_setting(key, default=""):
    conn = get_db(); row = conn.execute("select value from settings where key=?", (key,)).fetchone()
    return row[0] if row else default

def set_setting(key, value):
    conn = get_db()
    conn.execute("""insert into settings(key,value) values(?,?)
                    on conflict(key) do update set value=excluded.value, updated_at=current_timestamp""",
                 (key, str(value)))
        conn.commit()

def read_doc(uploaded_file):
    suffix = uploaded_file.name.lower().split(".")[-1]
    if suffix in ["doc","docx"]:
        d = docx.Document(uploaded_file); return "\n".join(p.text for p in d.paragraphs)
    if suffix == "pdf":
        r = PdfReader(uploaded_file); return "\n".join((p.extract_text() or "") for p in r.pages)
    return uploaded_file.read().decode("utf-8", errors="ignore")

def llm(system, prompt, temp=0.2, max_tokens=1400):
    if not client: return "Set OPENAI_API_KEY to enable drafting."
    messages = [{"role":"system","content":system},{"role":"user","content":prompt}]
    last_err = None
    for model_name in _OPENAI_FALLBACK_MODELS:
        try:
            rsp = client.chat.completions.create(model=model_name, messages=messages,
                                                 temperature=temp, max_tokens=max_tokens)
            if model_name != OPENAI_MODEL:
                try: st.toast(f"Using fallback model: {model_name}", icon="âš™ï¸")
                except Exception: pass
            return rsp.choices[0].message.content
        except Exception as e:
            last_err = e; continue
    return f"LLM error ({type(last_err).__name__ if last_err else 'UnknownError'}). Tip: set OPENAI_MODEL to a model you have."

def llm_messages(messages, temp=0.2, max_tokens=1400):
    if not client: return "Set OPENAI_API_KEY to enable drafting."
    last_err = None
    for model_name in _OPENAI_FALLBACK_MODELS:
        try:
            rsp = client.chat.completions.create(model=model_name, messages=messages,
                                                 temperature=temp, max_tokens=max_tokens)
            if model_name != OPENAI_MODEL:
                try: st.toast(f"Using fallback model: {model_name}", icon="âš™ï¸")
                except Exception: pass
            return rsp.choices[0].message.content
        except Exception as e:
            last_err = e; continue
    return f"LLM error ({type(last_err).__name__ if last_err else 'UnknownError'}). Tip: set OPENAI_MODEL to a model you have."

def chunk_text(text, max_chars=1800, overlap=200):
    parts, i = [], 0
    while i < len(text):
        parts.append(text[i:i+max_chars]); i += max_chars - overlap
    return parts

def embed_texts(texts):
    vec = TfidfVectorizer(stop_words="english"); X = vec.fit_transform(texts); return vec, X

def search_chunks(query, vec, X, texts, k=6):
    qX = vec.transform([query]); sims = (X @ qX.T).toarray().ravel()
    idx = sims.argsort()[::-1][:k]; return [texts[i] for i in idx]

def to_xlsx_bytes(df_dict):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
        for name, df in df_dict.items():
            df.to_excel(w, index=False, sheet_name=name[:31])
    return bio.getvalue()

# ---------- Dates (US format for SAM) ----------
def _us_date(d: datetime.date) -> str:
    return d.strftime("%m/%d/%Y")

def _parse_sam_date(s: str):
    if not s: return None
    s = s.replace("Z","").strip()
    for fmt in ("%Y-%m-%d","%Y-%m-%dT%H:%M:%S","%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None

# ---------- Context for Chat ----------
def build_context(max_rows=6):
    conn = get_db()
    g = pd.read_sql_query("select * from goals limit 1", conn)
    goals_line = ""
    if not g.empty:
        rr = g.iloc[0]
        goals_line = (f"Bids target {int(rr['bids_target'])}, submitted {int(rr['bids_submitted'])}; "
                      f"Revenue target ${float(rr['revenue_target']):,.0f}, won ${float(rr['revenue_won']):,.0f}.")
    codes = pd.read_sql_query("select code from naics_watch order by code", conn)["code"].tolist()
    naics_line = ", ".join(codes[:20]) + (" …" if len(codes) > 20 else "") if codes else "none"
    opp = pd.read_sql_query(
        "select title, agency, naics, response_due from opportunities order by posted desc limit ?",
        conn, params=(max_rows,)
    )
    opp_lines = ["- " + " | ".join(filter(None, [
        str(r["title"])[:80], str(r["agency"])[:40],
        f"due {str(r['response_due'])[:16]}", f"NAICS {str(r['naics'])[:18]}",
    ])) for _, r in opp.iterrows()]
    vend = pd.read_sql_query(
        """select trim(substr(naics,1,6)) as code, count(*) as cnt
           from vendors where ifnull(naics,'')<>''
           group by trim(substr(naics,1,6)) order by cnt desc limit ?""",
        conn, params=(max_rows,)
    )
    vend_lines = [f"- {r['code']}: {int(r['cnt'])} vendors" for _, r in vend.iterrows()]
    return "\n".join([
        f"Company: {get_setting('company_name','ELA Management LLC')}",
        f"Home location: {get_setting('home_loc','Houston, TX')}",
        f"Goals: {goals_line or 'not set'}",
        f"NAICS watch: {naics_line}",
        "Recent opportunities:" if not opp.empty else "Recent opportunities: (none)",
        *opp_lines,
        "Vendor coverage (top NAICS):" if not vend.empty else "Vendor coverage: (none)",
        *vend_lines,
    ])

# ---------- External integrations ----------
def linkedin_company_search(keyword: str) -> str:
    return f"https://www.linkedin.com/search/results/companies/?keywords={quote_plus(keyword)}"

def google_places_search(query, location="Houston, TX", radius_m=80000, strict=True):
    """
    Google Places Text Search + Details (phone + website).
    Returns (list_of_vendors, info). Emails are NOT provided by Places.
    """
    if not GOOGLE_PLACES_KEY:
        return [], {"ok": False, "reason": "missing_key", "detail": "GOOGLE_PLACES_API_KEY is empty."}
    try:
        # 1) Text Search
        search_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        search_params = {"query": f"{query} {location}", "radius": radius_m, "key": GOOGLE_PLACES_KEY}
        rs = requests.get(search_url, params=search_params, timeout=25)
        status_code = rs.status_code
        data = rs.json() if rs.headers.get("Content-Type","").startswith("application/json") else {}
        api_status = data.get("status","")
        results = data.get("results", []) or []

        if status_code != 200 or api_status not in ("OK","ZERO_RESULTS"):
            return ([] if strict else results), {
                "ok": False, "reason": api_status or "http_error", "http": status_code,
                "api_status": api_status, "count": len(results),
                "raw_preview": (rs.text or "")[:800],
                "note": "Enable billing + 'Places API' in Google Cloud."
            }

        # 2) Details per result
        out = []
        for item in results:
            place_id = item.get("place_id")
            phone, website = "", ""
            if place_id:
                det_url = "https://maps.googleapis.com/maps/api/place/details/json"
                det_params = {"place_id": place_id, "fields": "formatted_phone_number,website", "key": GOOGLE_PLACES_KEY}
                rd = requests.get(det_url, params=det_params, timeout=20)
                det_json = rd.json() if rd.headers.get("Content-Type","").startswith("application/json") else {}
                det = det_json.get("result", {})
                phone = det.get("formatted_phone_number", "") or ""
                website = det.get("website", "") or ""

            out.append({
                "company": item.get("name"),
                "naics": "",
                "trades": "",
                "phone": phone,
                "email": "",  # Emails not provided by Google Places
                "website": website,
                "city": location.split(",")[0].strip() if "," in location else location,
                "state": location.split(",")[-1].strip() if "," in location else "",
                "certifications": "",
                "set_asides": "",
                "notes": item.get("formatted_address",""),
                "source": "GooglePlaces",
            })
        info = {"ok": True, "count": len(out), "http": status_code, "api_status": api_status,
                "raw_preview": (rs.text or "")[:800]}
        return out, info
    except Exception as e:
        return [], {"ok": False, "reason": "exception", "detail": str(e)[:500]}

def send_via_graph(to_addr, subject, body):
    if not (MS_TENANT_ID and MS_CLIENT_ID and MS_CLIENT_SECRET):
        return "Graph not configured"
    try:
        token_r = requests.post(
            f"https://login.microsoftonline.com/{MS_TENANT_ID}/oauth2/v2.0/token",
            data={"client_id": MS_CLIENT_ID, "client_secret": MS_CLIENT_SECRET,
                  "scope": "https://graph.microsoft.com/.default", "grant_type": "client_credentials"},
            timeout=20,
        )
        token = token_r.json().get("access_token")
        if not token: return f"Graph token error: {token_r.text[:200]}"
        r = requests.post(
            f"https://graph.microsoft.com/v1.0/users/me/sendMail",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"message": {"subject": subject, "body": {"contentType": "Text", "content": body},
                              "toRecipients": [{"emailAddress": {"address": to_addr}}]}, "saveToSentItems": "true"},
            timeout=20,
        )
        return "Sent" if r.status_code in (200, 202) else f"Graph send error {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return f"Graph send exception: {e}"

# ---------- Email Scraper (polite, small crawl) ----------
EMAIL_REGEX = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

def _clean_url(url: str) -> str:
    if not url: return ""
    if not url.startswith(("http://","https://")): return "http://" + url
    return url

def _same_domain(u1: str, u2: str) -> bool:
    try:
        d1 = urlparse(u1).netloc.split(":")[0].lower()
        d2 = urlparse(u2).netloc.split(":")[0].lower()
        return d1.endswith(d2) or d2.endswith(d1)
    except Exception:
        return True

def _allowed_by_robots(base_url: str, path: str) -> bool:
    try:
        parsed = urlparse(base_url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        r = requests.get(robots_url, timeout=8)
        if r.status_code != 200 or "Disallow" not in r.text: return True
        disallows = []
        for line in r.text.splitlines():
            line = line.strip()
            if not line or not line.lower().startswith("disallow:"): continue
            rule = line.split(":",1)[1].strip()
            if rule: disallows.append(rule)
        for rule in disallows:
            if path.startswith(rule): return False
        return True
    except Exception:
        return True

def _fetch(url: str, timeout=12) -> str:
    try:
        headers = {"User-Agent": "ELA-GovCon-Scraper/1.0 (+contact via site form)"}
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200 or not r.headers.get("Content-Type","").lower().startswith("text"):
            return ""
        return r.text[:1_000_000]
    except Exception:
        return ""

def _extract_emails(text: str) -> set:
    emails = set()
    for m in EMAIL_REGEX.finditer(text or ""):
        e = m.group(0).strip().strip(".,;:)")
        if not e.lower().endswith((".png",".jpg",".gif",".svg",".jpeg")):
            emails.add(e)
    return emails

def crawl_site_for_emails(seed_url: str, max_pages=5, delay_s=0.7, same_domain_only=True) -> dict:
    if BeautifulSoup is None:
        return {"emails": set(), "visited": 0, "errors": ["beautifulsoup4 not installed"]}
    seed_url = _clean_url(seed_url)
    try:
        parsed = urlparse(seed_url); base = f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return {"emails": set(), "visited": 0, "errors": ["bad seed url"]}
    queue = [seed_url, urljoin(base,"/contact"), urljoin(base,"/contact-us"),
             urljoin(base,"/contacts"), urljoin(base,"/about"), urljoin(base,"/support")]
    seen, emails, visited, errors = set(), set(), 0, []
    while queue and visited < max_pages:
        url = queue.pop(0)
        if url in seen: continue
        seen.add(url)
        if not _allowed_by_robots(seed_url, urlparse(url).path): continue
        html = _fetch(url)
        if not html: continue
        visited += 1
        try:
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("mailto:"):
                    emails.add(href.replace("mailto:","").split("?")[0])
            emails |= _extract_emails(soup.get_text(separator=" ", strip=True))
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith(("#","mailto:","javascript:")): continue
                nxt = urljoin(url, href)
                if same_domain_only and not _same_domain(seed_url, nxt): continue
                if any(nxt.lower().endswith(suf) for suf in [".pdf",".doc",".docx",".xlsx",".ppt",".zip",".jpg",".png",".gif",".svg"]):
                    continue
                if nxt not in seen and len(queue) < (max_pages*3):
                    queue.append(nxt)
        except Exception as e:
            errors.append(str(e))
        time.sleep(delay_s)
    return {"emails": emails, "visited": visited, "errors": errors}

# ---------- SAM search ----------
def sam_search(
    naics_list, min_days=3, limit=100, keyword=None, posted_from_days=30,
    notice_types="Combined Synopsis/Solicitation,Solicitation,Presolicitation,SRCSGT", active="true"
):
    if not SAM_API_KEY:
        return pd.DataFrame(), {"ok": False, "reason": "missing_key", "detail": "SAM_API_KEY is empty."}
    base = "https://api.sam.gov/opportunities/v2/search"
    today = datetime.utcnow().date()
    min_due_date = today + timedelta(days=min_days)
    posted_from = _us_date(today - timedelta(days=posted_from_days))
    posted_to   = _us_date(today)

    params = {
        "api_key": SAM_API_KEY,
        "limit": str(limit),
        "response": "json",
        "sort": "-publishedDate",
        "active": active,
        "postedFrom": posted_from,   # MM/dd/yyyy
        "postedTo": posted_to,       # MM/dd/yyyy
    }
    # Enforce only Solicitation + Combined when notice_types is blank
    if not notice_types:
        notice_types = "Combined Synopsis/Solicitation,Solicitation"
    params["noticeType"] = notice_types

    if naics_list:   params["naics"] = ",".join([c for c in naics_list if c][:20])
    if keyword:      params["keywords"] = keyword

    try:
        headers = {"X-Api-Key": SAM_API_KEY}
        r = requests.get(base, params=params, headers=headers, timeout=40)
        status = r.status_code
        raw_preview = (r.text or "")[:1000]
        try:
            data = r.json()
        except Exception:
            return pd.DataFrame(), {"ok": False, "reason": "bad_json", "status": status, "raw_preview": raw_preview, "detail": r.text[:800]}
        if status != 200:
            err_msg = ""
            if isinstance(data, dict):
                err_msg = data.get("message") or (data.get("error") or {}).get("message") or ""
            return pd.DataFrame(), {"ok": False, "reason": "http_error", "status": status, "message": err_msg, "detail": data, "raw_preview": raw_preview}
        if isinstance(data, dict) and data.get("message"):
            return pd.DataFrame(), {"ok": False, "reason": "api_message", "status": status, "detail": data.get("message"), "raw_preview": raw_preview}

        items = data.get("opportunitiesData", []) or []
        rows = []
        for opp in items:
            due_str = opp.get("responseDeadLine") or ""
            d = _parse_sam_date(due_str)
            due_ok = (d is None) or (d >= min_due_date)
            if not due_ok: continue
            docs = opp.get("documents", []) or []
            rows.append({
                "sam_notice_id": opp.get("noticeId"),
                "title": opp.get("title"),
                "agency": opp.get("organizationName"),
                "naics": ",".join(opp.get("naicsCodes", [])),
                "psc": ",".join(opp.get("productOrServiceCodes", [])) if opp.get("productOrServiceCodes") else "",
                "place_of_performance": (opp.get("placeOfPerformance") or {}).get("city",""),
                "response_due": due_str,
                "posted": opp.get("publishedDate",""),
                "type": opp.get("type",""),
                "url": f"https://sam.gov/opp/{opp.get('noticeId')}/view",
                "attachments_json": json.dumps([{"name":d.get("fileName"),"url":d.get("url")} for d in docs])
            })
        df = pd.DataFrame(rows)
        info = {"ok": True, "status": status, "count": len(df), "raw_preview": raw_preview,
                "filters": {"naics": params.get("naics",""), "keyword": keyword or "",
                            "postedFrom": posted_from, "postedTo": posted_to,
                            "min_due_days": min_days, "noticeType": notice_types,
                            "active": active, "limit": limit}}
        if df.empty:
            info["hint"] = "Try min_days=0–1, add keyword, increase look-back, or clear noticeType."
        return df, info
    except requests.RequestException as e:
        return pd.DataFrame(), {"ok": False, "reason": "network", "detail": str(e)[:800]}




def _ensure_opportunity_columns():
    conn = get_db(); cur = conn.cursor()
    # Add columns if missing
    try: cur.execute("alter table opportunities add column status text default 'New'")
    except Exception: pass
    try: cur.execute("alter table opportunities add column assignee text")
    except Exception: pass
    try: cur.execute("alter table opportunities add column quick_note text")
    except Exception: pass
        conn.commit()

def _get_table_cols(name):
    conn = get_db(); cur = conn.cursor()
    cur.execute(f"pragma table_info({name})")
    return [r[1] for r in cur.fetchall()]

def _to_sqlite_value(v):
    # Normalize pandas/NumPy/complex types to Python primitives or None
    try:
        import numpy as np
        import pandas as pd
        if v is None:
            return None
        # Pandas NA
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        # Numpy scalars
        if isinstance(v, (np.generic,)):
            return v.item()
        # Lists/dicts -> JSON
        if isinstance(v, (list, dict)):
            return json.dumps(v)
        # Bytes -> decode
        if isinstance(v, (bytes, bytearray)):
            try:
                return v.decode("utf-8", "ignore")
            except Exception:
                return str(v)
        # Other types: cast to str for safety
        if not isinstance(v, (str, int, float)):
            return str(v)
        return v
    except Exception:
        # Fallback minimal handling
        if isinstance(v, (list, dict)):
            return json.dumps(v)
        return v

def save_opportunities(df, default_assignee=None):
    """Upsert into opportunities and handle legacy schemas gracefully."""
    if df is None or getattr(df, "empty", True):
        return 0, 0
    try:
        df = df.where(df.notnull(), None)
    except Exception:
        pass

    _ensure_opportunity_columns()
    cols = set(_get_table_cols("opportunities"))

    inserted = 0
    updated = 0
    conn = get_db(); cur = conn.cursor()
    for _, r in df.iterrows():
        nid = r.get("sam_notice_id")
        if not nid:
            continue
        cur.execute("select id from opportunities where sam_notice_id=?", (nid,))
        row = cur.fetchone()

        base_fields = {
            "sam_notice_id": nid,
            "title": r.get("title"),
            "agency": r.get("agency"),
            "naics": r.get("naics"),
            "psc": r.get("psc"),
            "place_of_performance": r.get("place_of_performance"),
            "response_due": r.get("response_due"),
            "posted": r.get("posted"),
            "type": r.get("type"),
            "url": r.get("url"),
            "attachments_json": r.get("attachments_json"),
        }
        # Sanitize all base fields
        for k, v in list(base_fields.items()):
            base_fields[k] = _to_sqlite_value(v)

        if row:
            cur.execute(
                """update opportunities set title=?, agency=?, naics=?, psc=?, place_of_performance=?,
                   response_due=?, posted=?, type=?, url=?, attachments_json=? where sam_notice_id=?""",
                (base_fields["title"], base_fields["agency"], base_fields["naics"], base_fields["psc"],
                 base_fields["place_of_performance"], base_fields["response_due"], base_fields["posted"],
                 base_fields["type"], base_fields["url"], base_fields["attachments_json"], base_fields["sam_notice_id"])
            )
            updated += 1
        else:
                    insert_cols = ["sam_notice_id","title","agency","naics","psc","place_of_performance","response_due","posted","type","url","attachments_json"]
                    insert_vals = [base_fields[c] for c in insert_cols]
                    if "status" in cols:
                        insert_cols.append("status"); insert_vals.append("New")
                    if "assignee" in cols:
                        insert_cols.append("assignee"); insert_vals.append(_to_sqlite_value(default_assignee or ""))
                    if "quick_note" in cols:
                        insert_cols.append("quick_note"); insert_vals.append("")
                    placeholders = ",".join("?" for _ in insert_cols)
                    cur.execute(f"insert into opportunities({','.join(insert_cols)}) values({placeholders})", insert_vals)
                    inserted += 1

        conn.commit()
            return inserted, updated
        # ---------- UI ----------
        st.title("GovCon Copilot Pro")
        st.caption("SubK sourcing • SAM watcher • proposals • outreach • CRM • goals • chat with memory & file uploads")




        with st.sidebar:
            st.subheader("Configuration")
            company_name = st.text_input("Company name", value=get_setting("company_name", "ELA Management LLC"))
            home_loc = st.text_input("Primary location", value=get_setting("home_loc", "Houston, TX"))
            default_trade = st.text_input("Default trade", value=get_setting("default_trade", "Janitorial"))
            if st.button("Save configuration"):
                set_setting("company_name", company_name); set_setting("home_loc", home_loc); set_setting("default_trade", default_trade)
        st.success("Saved")

            st.subheader("API Key Status")
            def _ok(v): return "✔" if v else "✘"
            st.markdown(f"**OpenAI Key:** {_ok(bool(OPENAI_API_KEY))}")
            st.markdown(f"**Google Places Key:** {_ok(bool(GOOGLE_PLACES_KEY))}")
            st.markdown(f"**SAM.gov Key:** {_ok(bool(SAM_API_KEY))}")
            st.caption(f"OpenAI SDK: {_openai_version} • Model: {OPENAI_MODEL}")
            if st.button("Test model"):
                st.info(llm("You are a health check.", "Reply READY.", max_tokens=5))

            if st.button("Test SAM key"):
                try:
                    today_us = _us_date(datetime.utcnow().date())
                    test_params = {"api_key": SAM_API_KEY, "limit": "1", "response": "json",
                                   "postedFrom": today_us, "postedTo": today_us}
                    headers = {"X-Api-Key": SAM_API_KEY}
                    r = requests.get("https://api.sam.gov/opportunities/v2/search", params=test_params, headers=headers, timeout=20)
                    st.write("HTTP", r.status_code)
                    text_preview = (r.text or "")[:1000]
                    try:
                        jj = r.json()
                        api_msg = ""
                        if isinstance(jj, dict):
                            api_msg = jj.get("message") or (jj.get("error") or {}).get("message") or ""
                        if api_msg:
                            st.error(f"API reported: {api_msg}"); st.code(text_preview)
                        elif r.status_code == 200:
        st.success("SAM key appears valid (200 with JSON)."); st.code(text_preview)
                        else:
                            st.warning("Non-200 but JSON returned."); st.code(text_preview)
                    except Exception as e:
                        st.error(f"JSON parse error: {e}"); st.code(text_preview)
                except Exception as e:
                    st.error(f"Request failed: {e}")

            if st.button("Test Google Places key"):
                vendors, info = google_places_search("janitorial small business", get_setting("home_loc","Houston, TX"), 30000)
                st.write("Places diagnostics:", info); st.write("Sample results:", vendors[:3])

            st.subheader("Watch list NAICS")
            conn = get_db()
            df_saved = pd.read_sql_query("select code from naics_watch order by code", conn)
            saved_codes = df_saved["code"].tolist()
            naics_options = sorted(set(saved_codes + NAICS_SEEDS))
            st.multiselect("Choose or type NAICS codes then Save", options=naics_options,
                           default=saved_codes if saved_codes else sorted(set(NAICS_SEEDS[:20])), key="naics_watch")
            new_code = st.text_input("Add a single NAICS code")
            col_n1, col_n2 = st.columns(2)
            with col_n1:
                if st.button("Add code"):
                    val = (new_code or "").strip()
                    if val:
                        conn.execute("insert or ignore into naics_watch(code,label) values(?,?)", (val, val)); conn.commit(); st.success(f"Added {val}")
            with col_n2:
                if st.button("Clear all saved codes"):
                    conn.execute("delete from naics_watch"); conn.commit(); st.success("Cleared saved codes")
            if st.button("Save NAICS list"):
                keep = sorted(set([c.strip() for c in st.session_state.naics_watch if str(c).strip()]))
                cur = conn.cursor(); cur.execute("delete from naics_watch")
                for c in keep: cur.execute("insert into naics_watch(code,label) values(?,?)", (c, c))
        conn.commit(); st.success("Saved NAICS watch list")

            naics_csv = st.file_uploader("Import NAICS CSV (column 'code')", type=["csv"])
            if naics_csv and st.button("Import NAICS from CSV"):
                df_in = pd.read_csv(naics_csv)
                if "code" in df_in.columns:
                    cur = conn.cursor()
                    for c in df_in["code"].astype(str).fillna("").str.strip():
                        if c: cur.execute("insert or ignore into naics_watch(code,label) values(?,?)", (c, c))
        conn.commit(); st.success("Imported")
                else:
                    st.info("CSV must have a column named code")

            st.subheader("Goals")
            g = pd.read_sql_query("select * from goals limit 1", conn)
            if g.empty:
                conn.execute("insert into goals(year,bids_target,revenue_target,bids_submitted,revenue_won) values(?,?,?,?,?)",
                             (datetime.now().year, 156, 600000, 1, 0)); conn.commit()
                g = pd.read_sql_query("select * from goals limit 1", conn)
            row = g.iloc[0]; goal_id = int(row["id"])
            with st.form("goals_form", clear_on_submit=False):
                col1, col2 = st.columns(2)
                with col1:
                    bids_target = st.number_input("Bids target", min_value=0, step=1, value=int(row["bids_target"]))
                    bids_submitted = st.number_input("Bids submitted", min_value=0, step=1, value=int(row["bids_submitted"]))
                with col2:
                    revenue_target = st.number_input("Revenue target", min_value=0.0, step=1000.0, value=float(row["revenue_target"]))
                    revenue_won = st.number_input("Revenue won", min_value=0.0, step=1000.0, value=float(row["revenue_won"]))
                if st.form_submit_button("Save goals"):
                    conn.execute("update goals set bids_target=?, revenue_target=?, bids_submitted=?, revenue_won=? where id=?",
                                 (int(bids_target), float(revenue_target), int(bids_submitted), float(revenue_won), goal_id))
        conn.commit(); st.success("Goals updated")
            colq1, colq2 = st.columns(2)
            with colq1:
                if st.button("Log new bid"):
                    conn.execute("update goals set bids_submitted = bids_submitted + 1 where id=?", (goal_id,)); conn.commit(); st.success("Bid logged")
            with colq2:
                add_amt = st.number_input("Add award amount", min_value=0.0, step=1000.0, value=0.0, key="award_add_amt")
                if st.button("Log award"):
                    if add_amt > 0:
                        conn.execute("update goals set revenue_won = revenue_won + ? where id=?", (float(add_amt), goal_id)); conn.commit()
        st.success(f"Award logged for ${add_amt:,.0f}")
                    else:
                        st.info("Enter a positive amount")
            g = pd.read_sql_query("select * from goals limit 1", conn); row = g.iloc[0]
            st.metric("Bids target", int(row["bids_target"]))
            st.metric("Bids submitted", int(row["bids_submitted"]))
            st.metric("Revenue target", f"${float(row['revenue_target']):,.0f}")
            st.metric("Revenue won", f"${float(row['revenue_won']):,.0f}")

        tabs = st.tabs([
            "Pipeline","Subcontractor Finder","Contacts","Outreach","SAM Watch",
            "RFP Analyzer","Capability Statement","White Paper Builder",
            "Data Export","Auto extract","Ask the doc","Chat Assistant",
        ])


        with tabs[0]:
            st.subheader("Opportunities pipeline")
            conn = get_db()
            df_opp = pd.read_sql_query("select * from opportunities order by posted desc", conn)
            # Ensure optional columns exist
            for _col, _default in {"assignee":"", "status":"New", "quick_note":""}.items():
                if _col not in df_opp.columns:
                    df_opp[_col] = _default
            if "url" in df_opp.columns and "Link" not in df_opp.columns:
                df_opp["Link"] = df_opp["url"]

            assignees = ["","Quincy","Charles","Collin"]
            f1, f2 = st.columns(2)
            with f1:
                a_filter = st.selectbox("Filter by assignee", assignees, index=(assignees.index(st.session_state.get('active_profile','')) if st.session_state.get('active_profile','') in assignees else 0))
            with f2:
                s_filter = st.selectbox("Filter by status", ["","New","Reviewing","Bidding","Submitted"], index=0)
            try:
                if a_filter:
                    df_opp = df_opp[df_opp["assignee"].fillna("")==a_filter]
                if s_filter:
                    df_opp = df_opp[df_opp["status"].fillna("")==s_filter]
            except KeyError:
                pass

            edit = st.data_editor(
                df_opp,
                column_config={
                    "status": st.column_config.SelectboxColumn("status", options=["New","Reviewing","Bidding","Submitted"]),
                    "assignee": st.column_config.SelectboxColumn("assignee", options=assignees),
                    "Link": st.column_config.LinkColumn("Link", display_text="Open in SAM")
                },
                use_container_width=True, num_rows="dynamic", key="opp_grid"
            )
            if st.button("Save pipeline changes"):
                cur = conn.cursor()
                # Make a copy of the original grid if present; else derive from filtered df
                try:
                    pre_df = pre_df if "pre_df" in locals() else df_opp.copy()
                except Exception:
                    pre_df = df_opp.copy()

                # Normalize IDs
                try:
                    orig_ids = set(pd.to_numeric(pre_df.get("id"), errors="coerce").dropna().astype(int).tolist()) if "id" in pre_df.columns else set()
                    new_ids = set(pd.to_numeric(edit.get("id"), errors="coerce").dropna().astype(int).tolist()) if "id" in edit.columns else set()
                except Exception:
                    orig_ids, new_ids = set(), set()

                # Updates (rows that still exist)
                updated = 0
                if "id" in edit.columns:
                    for _, r in edit.iterrows():
                        try:
                            rid = int(r["id"])
                        except Exception:
                            continue
                        cur.execute(
                            "update opportunities set status=?, response_due=?, title=?, agency=?, assignee=?, quick_note=? where id=?",
                            (r.get("status","New"), r.get("response_due"), r.get("title"), r.get("agency"),
                             r.get("assignee",""), r.get("quick_note",""), rid)
                        )
                        updated += 1

                # Deletions (IDs removed from the grid)
                deleted = 0
                to_delete = list(orig_ids - new_ids)
                for rid in to_delete:
                    cur.execute("delete from opportunities where id=?", (int(rid),))
                    deleted += 1

        conn.commit()
        st.success(f"Saved — updated {updated} row(s), deleted {deleted} row(s).")

        with tabs[1]:
            st.subheader("Find subcontractors and rank by fit")
            trade = st.text_input("Trade", value=get_setting("default_trade", "Janitorial"))
            loc = st.text_input("Place of Performance", value=get_setting("home_loc", "Houston, TX"))
            radius_miles = st.slider("Radius (miles)", min_value=5, max_value=200, value=50, step=5)
            naics_choice = st.multiselect("NAICS to tag new imports", options=sorted(set(NAICS_SEEDS)), default=[])
            find_emails = st.checkbox("Try to find emails from website (slow)", value=False)
            max_pages = st.slider("Max pages per site (email crawl)", min_value=1, max_value=12, value=5)
            places_diag = st.checkbox("Show Google Places diagnostics", value=False)

            colA, colB, colC = st.columns(3)

            with colA:

                if st.button("Google Places import"):
                    results, info = google_places_search(f"{trade} small business", loc, int(radius_miles*1609.34))
                    st.session_state["vendor_results"] = results or []
                    st.session_state["vendor_info"] = info or {}
                    if places_diag:
                        st.write("Places diagnostics:", info); st.code((info or {}).get("raw_preview","") or "", language="json")

                results = st.session_state.get("vendor_results") or []
                info = st.session_state.get("vendor_info") or {}

                if results:
                    df_new = pd.DataFrame(results)

                    # Build hyperlink column; fallback to Google search if website missing
                    def _make_link(row):
                        site = (row.get("website") or "").strip()
                        if site:
                            return site
                        comp = (row.get("company") or "").strip()
                        city = (row.get("city") or "").strip()
                        state = (row.get("state") or "").strip()
                        q = quote_plus(" ".join(x for x in [comp, city, state, "site"] if x))
                        return f"https://www.google.com/search?q={q}"

                    if not df_new.empty:
                        df_new["Link"] = df_new.apply(_make_link, axis=1)

                    # Optional name filter
                    name_filter = st.text_input("Filter by company name contains", "")
                    if name_filter:
                        df_new = df_new[df_new["company"].fillna("").str.contains(name_filter, case=False, na=False)]

                    # Add Save checkbox per row
                    if "Save" not in df_new.columns:
                        df_new["Save"] = False

                    # Show as editable grid with clickable links
                    edited = st.data_editor(
                        df_new[["company","phone","email","city","state","notes","Link","Save"]].rename(columns={
                            "company": "Company", "phone": "Phone", "email": "Email",
                            "city": "City", "state": "State", "notes": "Notes"
                        }),
                        column_config={
                            "Link": st.column_config.LinkColumn("Link", display_text="Open"),
                            "Save": st.column_config.CheckboxColumn("Save")
                        },
                        use_container_width=True,
                        num_rows="fixed",
                        key="vendor_import_grid"
                    )

                    # Save only selected rows
                    save_sel = edited[edited.get("Save", False) == True] if isinstance(edited, pd.DataFrame) else pd.DataFrame()

                    col_save_a, col_save_b = st.columns([1,2])
                    with col_save_a:
                        st.caption(f"Selected to save: {len(save_sel)} of {len(edited) if isinstance(edited, pd.DataFrame) else 0}")
                        save_btn = st.button("Save selected to vendors")
                    with col_save_b:
                        st.caption("Tip: Click a link to review a site before saving.")

                    if save_btn and not save_sel.empty:
                        conn = get_db(); cur = conn.cursor()
                        saved = 0
                        naics_tag = ",".join(naics_choice) if "naics_choice" in locals() and naics_choice else ""
                    
                        for _, r in save_sel.rename(columns={
                            "Company":"company","Phone":"phone","Email":"email",
                            "City":"city","State":"state","Notes":"notes"
                        }).iterrows():
                            company = (r.get("company") or "").strip()
                            phone = (r.get("phone") or "").strip()
                            website = (r.get("Link") or "").strip()
                            email = (r.get("email") or "").strip()
                            extra_note = (r.get("notes") or "").strip()
                            city = (r.get("city") or "").strip()
                            state = (r.get("state") or "").strip()
                            source = "GooglePlaces"
                    
                            vid = None
                            if website:
                                cur.execute("select id from vendors where website=?", (website,))
                                row = cur.fetchone()
                                if row:
                                    vid = row[0]
                            if not vid and company:
                                cur.execute("select id from vendors where company=? and ifnull(phone,'')=?", (company, phone))
                                row = cur.fetchone()
                                if row:
                                    vid = row[0]
                    
                            if vid:
                                cur.execute(
                                    "update vendors set company=?, naics=?, trades=?, phone=?, email=?, website=?, city=?, state=?, notes=?, source=?, updated_at=current_timestamp where id=?",
                                    (company, naics_tag, trade, phone, email, website, city, state, extra_note, source, int(vid))
                                )
                            else:
                                cur.execute(
                                    "insert into vendors(company,naics,trades,phone,email,website,city,state,certifications,set_asides,notes,source) values(?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (company, naics_tag, trade, phone, email, website, city, state, "", "", extra_note, source)
                                )
                    
                            # Also upsert contact
                            try:
                                notes_contact = f"Vendor website: {website}" if website else "Vendor imported from Subcontractor Finder"
                                cur.execute("select id from contacts where (email=? and ifnull(email,'')<>'') or (name=? and ifnull(phone,'')=?)",
                                            (email, company, phone))
                                _rowc = cur.fetchone()
                                if _rowc:
                                    cur.execute(
                                        "update contacts set name=?, org=?, role=?, email=?, phone=?, source=?, notes=? where id=?",
                                        (company, company, "Vendor", email, phone, "VendorImport", notes_contact, int(_rowc[0]))
                                    )
                                else:
                                    cur.execute(
                                        "insert into contacts(name, org, role, email, phone, source, notes) values(?,?,?,?,?,?,?)",
                                        (company, company, "Vendor", email, phone, "VendorImport", notes_contact)
                                    )
                            except Exception:
                                pass
                    
                            saved += 1
                    
        conn.commit()
        st.success(f"Saved {saved} vendor(s).")

                else:
                    msg = "No results"
                    if info and not info.get("ok", True):
                        msg += f" ({info.get('reason','')})"
                    if not GOOGLE_PLACES_KEY:
                        msg += " — Google Places key is missing."
                    st.warning(msg)


            with colB:
                st.markdown("LinkedIn quick search")
                st.link_button("Open LinkedIn", linkedin_company_search(f"{trade} {loc}"))

            with colC:
                st.markdown("Google search")
                st.link_button("Open Google", f"https://www.google.com/search?q={quote_plus(trade + ' ' + loc)}")

        with tabs[2]:



            st.subheader("POC and networking hub")
            st.caption("Add or clean up government POCs and vendor contacts. Link key contacts to opportunities in your notes.")
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
        conn.commit(); st.success("Saved")

        with tabs[3]:
            st.subheader("Outreach and mail merge")
            st.caption("Use default templates, personalize for distance, capability and past performance. Paste replies to track status.")
            conn = get_db(); df_v = pd.read_sql_query("select * from vendors", conn)
            t = pd.read_sql_query("select * from email_templates order by name", conn)
            names = t["name"].tolist() if not t.empty else ["RFQ Request"]
            pick_t = st.selectbox("Template", options=names)
            tpl = pd.read_sql_query("select subject, body from email_templates where name=?", conn, params=(pick_t,))
            subj_default = tpl.iloc[0]["subject"] if not tpl.empty else get_setting("outreach_subject", "")
            body_default = tpl.iloc[0]["body"] if not tpl.empty else get_setting("outreach_scope", "")
            subj = st.text_input("Subject", value=subj_default)
            body = st.text_area("Body with placeholders {company} {scope} {due}", value=body_default, height=220)
            if st.button("Save template"):
                conn.execute("""insert into email_templates(name, subject, body) values(?,?,?)
                                on conflict(name) do update set subject=excluded.subject, body=excluded.body, updated_at=current_timestamp""",
                             (pick_t, subj, body)); conn.commit(); st.success("Template saved")
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
                        status = send_via_graph(m["to"], m["subject"], m["body"]) if send_method=="Microsoft Graph" else "Preview"
                        get_db().execute("""insert into outreach_log(vendor_id,contact_method,to_addr,subject,body,sent_at,status)
                                         values(?,?,?,?,?,?,?)""",
                                         (m["vendor_id"], send_method, m["to"], m["subject"], m["body"], datetime.now().isoformat(), status))
                        get_db().commit(); sent += 1
        st.success(f"Processed {sent} messages")


        with tabs[4]:
            st.subheader("SAM.gov auto search with attachments")
            st.markdown("> **Flow:** Set All active → apply filters → open attachments → choose assignee → **Search** then **Save to pipeline**")
            conn = get_db()
            codes = pd.read_sql_query("select code from naics_watch order by code", conn)["code"].tolist()
            st.caption(f"Using NAICS codes: {', '.join(codes) if codes else 'none'}")

            col1, col2, col3 = st.columns(3)
            with col1:
                min_days = st.number_input("Minimum days until due", min_value=0, step=1, value=3)
                posted_from_days = st.number_input("Posted window (days back)", min_value=1, step=1, value=30)
                active_only = st.checkbox("All active opportunities", value=True)
            with col2:
                keyword = st.text_input("Keyword", value="")
                notice_types = st.multiselect("Notice types", options=["Combined Synopsis/Solicitation","Solicitation","Presolicitation","SRCSGT"], default=["Combined Synopsis/Solicitation","Solicitation"])
            with col3:
                diag = st.checkbox("Show diagnostics", value=False)
                raw = st.checkbox("Show raw API text (debug)", value=False)
                assignee_default = st.selectbox("Default assignee", ["","Quincy","Charles","Collin"], index=(['','Quincy','Charles','Collin'].index(st.session_state.get('active_profile','')) if st.session_state.get('active_profile','') in ['Quincy','Charles','Collin'] else 0))

            cA, cB, cC = st.columns(3)

            # Run search stores results in session so Save works after rerun
            with cA:
                # Fallback in case the number_input did not run in this branch

                pages_to_fetch = st.session_state.get("pages_to_fetch", 3)

                if st.button("Run search now"):
                    df, info = sam_search(
                        codes, min_days=min_days, limit=150,
                        keyword=keyword or None, posted_from_days=int(posted_from_days),
                        notice_types="Combined Synopsis/Solicitation,Solicitation", active="true"
                    )
                    st.session_state["sam_results_df"] = df
                    st.session_state["sam_results_info"] = info
                    if diag:
                        st.write("Diagnostics:", info)
                        st.code(f"naics={','.join(codes[:20])} | keyword={keyword or ''} | postedFrom={info.get('filters',{}).get('postedFrom')} -> postedTo={info.get('filters',{}).get('postedTo')} | min_days={min_days} | limit=150", language="text")
                    if raw:
                        st.code((info or {}).get("raw_preview","") or "", language="json")

            # Show results from session (if any)
            df = st.session_state.get("sam_results_df")
            info = st.session_state.get("sam_results_info", {}) or {}
            if info and not info.get("ok", True):
                st.error(f"SAM API error: {info}")

            elif isinstance(df, pd.DataFrame) and not df.empty:
                # Hard client-side filter (belt-and-suspenders)
                allowed_types = {"Combined Synopsis/Solicitation", "Solicitation"}
                if "type" in df.columns:
                    df = df[df["type"].isin(allowed_types)].copy()
                # Build interactive grid with hyperlink and Save checkbox
                grid_df = df.copy()
                grid_df["Link"] = grid_df["url"]
                if "Save" not in grid_df.columns:
                    grid_df["Save"] = False

                edited = st.data_editor(
                    grid_df,
                    column_config={
                        "Link": st.column_config.LinkColumn("Link", display_text="Open in SAM")
                    },
                    use_container_width=True,
                    num_rows="fixed",
                    key="sam_watch_grid"
                )
                # Save only selected rows
                save_sel = edited[edited.get("Save", False)==True] if "Save" in edited.columns else edited.iloc[0:0]
                st.caption(f"Selected to save: {len(save_sel)} of {len(edited)}")

                if st.button("Save selected to pipeline"):
                    to_save = save_sel.drop(columns=[c for c in ["Save","Link"] if c in save_sel.columns])
                    ins, upd = save_opportunities(to_save, default_assignee=assignee_default)
        st.success(f"Saved to pipeline — inserted {ins}, updated {upd}.")
            else:
                st.info("No active results yet. Click **Run search now**.")

            with cB:
                if st.button("Broad test (keyword only)"):
                    kw = keyword.strip() or "janitorial"
                    df, info = sam_search(
                        [], min_days=0, limit=100, keyword=kw, posted_from_days=60,
                        notice_types="Combined Synopsis/Solicitation,Solicitation", active="true"
                    )
                    st.session_state["sam_results_df"] = df
                    st.session_state["sam_results_info"] = info
        st.success(f"Test search complete for keyword: {kw}")

            with cC:
                if st.button("Test SAM key only"):
                    try:
                        today_us = _us_date(datetime.utcnow().date())
                        test_params = {"api_key": SAM_API_KEY, "limit": "1", "response": "json", "postedFrom": today_us, "postedTo": today_us}
                        headers = {"X-Api-Key": SAM_API_KEY}
                        r = requests.get("https://api.sam.gov/opportunities/v2/search", params=test_params, headers=headers, timeout=20)
                        st.write("HTTP", r.status_code)
                        text_preview = (r.text or "")[:1000]
                        try:
                            jj = r.json()
                        except Exception:
                            jj = {"raw": text_preview}
                        st.code(json.dumps(jj, indent=2)[:1200])
                    except Exception as e:
                        st.error(f"Key test failed: {e}")

        # Removed RFP mini-analyzer from SAM Watch

        with tabs[5]:
            st.subheader("RFP Analyzer")
            st.caption("Upload RFP package and chat with memory. Use quick actions or ask your own questions.")

            conn = get_db()

            # Sessions like Chat Assistant
            sessions = pd.read_sql_query("select id, title, created_at from rfp_sessions order by created_at desc", conn)
            session_titles = ["➤ New RFP thread"] + [f"{r['id']}: {r['title'] or '(untitled)'}" for _, r in sessions.iterrows()]
            pick = st.selectbox("RFP session", options=session_titles, index=0)
            in_new_rfp = (pick == "➤ New RFP thread")
            if in_new_rfp:
                default_title = f"RFP {datetime.now().strftime('%b %d %I:%M %p')}"
                new_title = st.text_input("Thread title", value=default_title)
                if st.button("Start RFP thread"):
                    conn.execute("insert into rfp_sessions(title) values(?)", (new_title,))
        conn.commit()
                    st.rerun()
                st.info("Create a thread to begin. Other tabs remain available.")
            else:


            session_id = int(pick.split(":")[0])
            cur_title = sessions[sessions["id"] == session_id]["title"].iloc[0]
            st.caption(f"RFP thread #{session_id}  {cur_title}")

            # File uploader with persistence
            uploads = st.file_uploader("Upload RFP files PDF DOCX TXT", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key=f"rfp_up_{session_id}")
            if uploads and st.button("Add files to RFP thread"):
                added = 0
                for up in uploads:
                    text = read_doc(up)[:800_000]
                    conn.execute("""insert into rfp_files(session_id, filename, mimetype, content_text)
                                    values(?,?,?,?)""", (session_id, up.name, getattr(up, "type", ""), text))
                    added += 1
        conn.commit()
        st.success(f"Added {added} file(s) to this thread.")
                st.rerun()

            files_df = pd.read_sql_query(
                "select id, filename, length(content_text) as chars, uploaded_at from rfp_files where session_id=? order by id desc",
                conn, params=(session_id,)
            )
            if files_df.empty:
                st.caption("No files yet.")
            else:
                st.caption("Attached files")
                st.dataframe(files_df.rename(columns={"chars":"chars_of_text"}), use_container_width=True)
                del_id = st.number_input("Delete attachment by ID", min_value=0, step=1, value=0, key=f"rfp_del_{session_id}")
                if st.button("Delete selected RFP file"):
                    if del_id > 0:
                        conn.execute("delete from rfp_files where id=?", (int(del_id),))
        conn.commit()
        st.success(f"Deleted file id {del_id}.")
                        st.rerun()

            # Previous messages
            hist = pd.read_sql_query(
                "select role, content, created_at from rfp_messages where session_id=? order by id asc",
                conn, params=(session_id,)
            )
            if hist.empty:
                st.info("No messages yet. Use the quick actions or ask a question below.")
            else:
                for _, row in hist.iterrows():
                    if row["role"] == "user":
                        st.chat_message("user").markdown(row["content"])
                    elif row["role"] == "assistant":
                        st.chat_message("assistant").markdown(row["content"])
                    else:
                        st.caption(f"System updated at {row['created_at']}")

            # Helper to build doc context
            def _rfp_context_for(question_text: str):
                rows = pd.read_sql_query(
                    "select filename, content_text from rfp_files where session_id=? and ifnull(content_text,'')<>''",
                    conn, params=(session_id,)
                )
                if rows.empty:
                    return ""
                chunks, labels = [], []
                for _, r in rows.iterrows():
                    cs = chunk_text(r["content_text"], max_chars=1200, overlap=200)
                    chunks.extend(cs)
                    labels.extend([r["filename"]]*len(cs))
                vec, X = embed_texts(chunks)
                top = search_chunks(question_text, vec, X, chunks, k=min(8, len(chunks)))
                parts, used = [], set()
                for sn in top:
                    try:
                        idx = chunks.index(sn)
                        fname = labels[idx]
                    except Exception:
                        idx, fname = -1, "attachment"
                    key = (fname, sn[:60])
                    if key in used:
                        continue
                    used.add(key)
                    parts.append(f"\n--- {fname} ---\n{sn.strip()}\n")
                return "Attached document snippets most relevant first:\n" + "\n".join(parts[:16]) if parts else ""

            # Quick action buttons
            colA, colB, colC, colD = st.columns(4)
            qa = None
            with colA:
                if st.button("Compliance matrix"):
                    qa = "Produce a compliance matrix that lists every shall must or required item and where it appears."
            with colB:
                if st.button("Evaluation factors"):
                    qa = "Summarize the evaluation factors and their relative importance and scoring approach."
            with colC:
                if st.button("Submission checklist"):
                    qa = "Create a submission checklist with page limits fonts file naming addresses and exact submission method with dates and times quoted."
            with colD:
                if st.button("Grade my draft"):
                    qa = "Grade the following draft against the RFP requirements and give a fix list. If draft text is empty just outline what a strong section must contain."

            # Free form follow up like chat
            user_q = st.chat_input("Ask a question about the RFP or use a quick action above")
            pending_prompt = qa or user_q

            if pending_prompt:
                # Save user turn
                conn.execute("insert into rfp_messages(session_id, role, content) values(?,?,?)",
                             (session_id, "user", pending_prompt))
        conn.commit()

                # Build system and context using company snapshot and RFP snippets
                context_snap = build_context(max_rows=6)
                doc_snips = _rfp_context_for(pending_prompt)

                sys_text = f"""You are a federal contracting assistant. Keep answers concise and actionable.
        Context snapshot:
        {context_snap}
        {doc_snips if doc_snips else ""}"""

                # Compose rolling window like Chat Assistant
                msgs_db = pd.read_sql_query(
                    "select role, content from rfp_messages where session_id=? order by id asc",
                    conn, params=(session_id,)
                ).to_dict(orient="records")

                # Keep up to 12 user turns
                pruned, user_turns = [], 0
                for m in msgs_db[::-1]:
                    if m["role"] == "assistant":
                        pruned.append(m)
                        continue
                    if m["role"] == "user":
                        if user_turns < 12:
                            pruned.append(m)
                            user_turns += 1
                        continue
                msgs_window = list(reversed(pruned))
                messages = [{"role": "system", "content": sys_text}] + msgs_window

                assistant_out = llm_messages(messages, temp=0.2, max_tokens=1200)
                conn.execute("insert into rfp_messages(session_id, role, content) values(?,?,?)",
                             (session_id, "assistant", assistant_out))
        conn.commit()

                st.chat_message("user").markdown(pending_prompt)
                st.chat_message("assistant").markdown(assistant_out)

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
        prompt = f"""Company {company}
Tagline {tagline}
Core {core}
Diff {diff}
Past performance {past_perf}
Contact {contact}
NAICS {", ".join(sorted(set(NAICS_SEEDS)))}
Certifications Small Business
Goals 156 bids and 600000 revenue this year. Submitted 1 to date."""
        st.markdown(llm(system, prompt, max_tokens=900))

with tabs[7]:
    st.subheader("White paper builder")
    title = st.text_input("Title", value="Improving Facility Readiness with Outcome based Service Contracts")
    thesis = st.text_area("Thesis", value="Outcome based service contracts reduce total cost and improve satisfaction when paired with clear SLAs and transparent data.")
    audience = st.text_input("Audience", value="Facility Managers • Contracting Officers • Program Managers")
    if st.button("Draft white paper"):
        system = "Write a two page white paper with executive summary, problem, approach, case vignette, and implementation steps. Use clear headings and tight language."
        prompt = f"Title {title}\nThesis {thesis}\nAudience {audience}"
        st.markdown(llm(system, prompt, max_tokens=1400))

with tabs[8]:
    st.subheader("Export to Excel workbook")
    conn = get_db()
    v = pd.read_sql_query("select * from vendors", conn)
    o = pd.read_sql_query("select * from opportunities", conn)
    c = pd.read_sql_query("select * from contacts", conn)
    bytes_xlsx = to_xlsx_bytes({"Vendors": v, "Opportunities": o, "Contacts": c})
    st.download_button("Download Excel workbook", data=bytes_xlsx, file_name="govcon_hub.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

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

with tabs[10]:
    st.subheader("Ask questions over the uploaded docs")
    up2 = st.file_uploader("Upload PDFs or DOCX", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key="qna_up")
    q = st.text_input("Your question")
    if up2 and q and st.button("Answer"):
        combined = "\n\n".join(read_doc(f) for f in up2)
        chunks = chunk_text(combined); vec, X = embed_texts(chunks)
        snips = search_chunks(q, vec, X, chunks, k=6); support = "\n\n".join(snips)
        system = "Answer directly. Quote exact lines for dates or addresses."
        prompt = f"Context\n{support}\n\nQuestion\n{q}"
        st.markdown(llm(system, prompt, max_tokens=900))

with tabs[11]:
    st.subheader("Chat Assistant (remembers context; accepts file uploads)")
    conn = get_db()
    sessions = pd.read_sql_query("select id, title, created_at from chat_sessions order by created_at desc", conn)
    session_titles = ["➤ New chat"] + [f"{r['id']}: {r['title'] or '(untitled)'}" for _, r in sessions.iterrows()]
    pick = st.selectbox("Session", options=session_titles, index=0)
    if pick == "➤ New chat":
        default_title = f"Chat {datetime.now().strftime('%b %d %I:%M %p')}"
        new_title = st.text_input("New chat title", value=default_title)
        if st.button("Start chat"):
            conn.execute("insert into chat_sessions(title) values(?)", (new_title,))
        conn.commit(); st.rerun()
        st.stop()

    session_id = int(pick.split(":")[0])
    cur_title = sessions[sessions["id"] == session_id]["title"].iloc[0]
    st.caption(f"Session #{session_id} — {cur_title}")

    g = pd.read_sql_query("select * from goals limit 1", conn)
    goals_line = ""
    if not g.empty:
        rr = g.iloc[0]
        goals_line = f"Bids target {int(rr['bids_target'])}, submitted {int(rr['bids_submitted'])}; Revenue target ${float(rr['revenue_target']):,.0f}, won ${float(rr['revenue_won']):,.0f}."
    default_system = f"""You are a senior federal contracting copilot.
Company: {get_setting('company_name','ELA Management LLC')}.
Home location: {get_setting('home_loc','Houston, TX')}.
Default trade: {get_setting('default_trade','Janitorial')}.
Goals: {goals_line}
Keep responses concise and actionable. Use bullet points when helpful. Ask clarifying questions only when necessary."""
    sys_prompt = st.text_area("System instructions (optional)", value=default_system, height=120)

    st.markdown("**Attach files for this chat** (PDF, DOCX, TXT)")
    uploads = st.file_uploader("Drop files here", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key="chat_uploads")
    if uploads and st.button("Add to chat"):
        added = 0
        for up in uploads:
            text = read_doc(up)[:800_000]
            conn.execute("""insert into chat_files(session_id, filename, mimetype, content_text)
                            values(?,?,?,?)""", (session_id, up.name, getattr(up, "type", ""), text))
            added += 1
        conn.commit(); st.success(f"Added {added} file(s) to this chat.")

    files_df = pd.read_sql_query(
        "select id, filename, length(content_text) as chars, uploaded_at from chat_files where session_id=? order by id desc",
        conn, params=(session_id,)
    )
    if files_df.empty:
        st.caption("No files attached yet.")
    else:
        st.caption("Attached files:")
        st.dataframe(files_df.rename(columns={"chars":"chars_of_text"}), use_container_width=True)
        del_id = st.number_input("Delete attachment by ID", min_value=0, step=1, value=0)
        if st.button("Delete selected file") and del_id > 0:
            conn.execute("delete from chat_files where id=?", (int(del_id),)); conn.commit(); st.success(f"Deleted file id {del_id}."); st.rerun()

    hist = pd.read_sql_query("select role, content, created_at from chat_messages where session_id=? order by id asc",
                             conn, params=(session_id,))
    if hist.empty:
        st.info("No messages yet. Ask your first question below.")
    else:
        for _, row in hist.iterrows():
            if row["role"] == "user": st.chat_message("user").markdown(row["content"])
            elif row["role"] == "assistant": st.chat_message("assistant").markdown(row["content"])
            else: st.caption(f"ðŸ§  System updated at {row['created_at']}")

    user_msg = st.chat_input("Ask a question… e.g., 'Draft staffing plan for janitorial at VA clinic'")
    if user_msg:
        last_sys = pd.read_sql_query(
            "select id, content from chat_messages where session_id=? and role='system' order by id desc limit 1",
            conn, params=(session_id,))
        if last_sys.empty or last_sys.iloc[0]["content"] != sys_prompt:
            conn.execute("insert into chat_messages(session_id, role, content) values(?,?,?)", (session_id, "system", sys_prompt)); conn.commit()
        conn.execute("insert into chat_messages(session_id, role, content) values(?,?,?)", (session_id, "user", user_msg)); conn.commit()

        msgs = pd.read_sql_query("select role, content from chat_messages where session_id=? order by id asc",
                                 conn, params=(session_id,)).to_dict(orient="records")
        pruned, sys_seen, user_turns = [], False, 0
        for m in msgs[::-1]:
            if m["role"] == "assistant": pruned.append(m); continue
            if m["role"] == "user":
                if user_turns < 12: pruned.append(m); user_turns += 1
                continue
            if m["role"] == "system" and not sys_seen: pruned.append(m); sys_seen = True
        msgs_window = list(reversed(pruned))

        # Build doc context
        rows = pd.read_sql_query(
            "select filename, content_text from chat_files where session_id=? and ifnull(content_text,'')<>''",
            conn, params=(session_id,)
        )
        doc_snips = ""
        if not rows.empty:
            chunks, labels = [], []
            for _, r in rows.iterrows():
                cs = chunk_text(r["content_text"], max_chars=1200, overlap=200)
                chunks.extend(cs); labels.extend([r["filename"]]*len(cs))
            vec, X = embed_texts(chunks)
            top = search_chunks(user_msg, vec, X, chunks, k=min(8, len(chunks)))
            parts, used = [], set()
            for sn in top:
                idx = chunks.index(sn) if sn in chunks else -1
                fname = labels[idx] if 0 <= idx < len(labels) else "attachment"
                key = (fname, sn[:60])
                if key in used: continue
                used.add(key)
                parts.append(f"\n--- {fname} ---\n{sn.strip()}\n")
            if parts: doc_snips = "Attached document snippets (most relevant first):\n" + "\n".join(parts[:16])

        context_snap = build_context(max_rows=6)
        sys_blocks = [f"Context snapshot (keep answers consistent with this):\n{context_snap}"]
        if doc_snips: sys_blocks.append(doc_snips)
        msgs_with_ctx = [{"role":"system","content":"\n\n".join(sys_blocks)}] + msgs_window

        assistant_out = llm_messages(msgs_with_ctx, temp=0.2, max_tokens=1200)
        conn.execute("insert into chat_messages(session_id, role, content) values(?,?,?)", (session_id, "assistant", assistant_out)); conn.commit()
        st.chat_message("user").markdown(user_msg)
        st.chat_message("assistant").markdown(assistant_out)
# ===== end app.py =====