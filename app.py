# ===== app.py =====
import os, re, io, json, sqlite3, time
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urljoin, urlparse

import pandas as pd
import streamlit as st
import numpy as np
import requests
from PyPDF2 import PdfReader
import docx
from sklearn.feature_extraction.text import TfidfVectorizer


# ---- User/session helpers ----
def get_current_user():
    try:
        return st.session_state.get("current_user") or st.query_params.get("user", [None])[0]
    except Exception:
        return st.session_state.get("current_user")

def set_current_user(name: str):
    st.session_state["current_user"] = name
    try:
        st.query_params["user"] = name
    except Exception:
        try:
            st.experimental_set_query_params(user=name)
        except Exception:
            pass

def audit(action: str, entity: str, entity_id: str = "", payload: str = ""):
    try:
        conn = get_db()
        conn.execute("insert into audit_logs(user_name, action, entity, entity_id, payload) values(?,?,?,?,?)",
                     (get_current_user() or "", action, entity, entity_id, payload[:2000]))
        conn.commit()
    except Exception:
        pass


# Company identifiers
COMPANY_CAGE = "14ZP6"
COMPANY_UEI = "U32LBVK3DDF7"
COMPANY_DUNS = "14-483-4790"
COMPANY_EMAIL = "elamgmtllc@gmail.com"






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

st.set_page_config(page_title="GovCon Copilot Pro", page_icon="🧰", layout="wide")
DB_PATH = "govcon.db"

NAICS_SEEDS = [
    "561210","721110","562991","326191","336611","531120","531","722310","561990","722514","561612",
    "561730","311511","238990","311812","561720","811210","236118","238220","237990","311423",
    "562910","236220","332420","238320","541380","541519","561710","423730","238210","562211",
    "541214","541330","541512","541511","541370","611430","611699","611310","611710","562111","562119",
    "624230","488999","485510","485410","488510","541614","332994","334220","336992","561320","561311","541214"
]

SCHEMA = {
    "rfp_sessions": """
    create table if not exists rfp_sessions (
        id integer primary key,
        title text,
        draft_text text,
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
    """,
    "users": """
    create table if not exists users (
        id integer primary key,
        name text unique,
        email text,
        created_at text default current_timestamp
    );
    """,
    "audit_logs": """
    create table if not exists audit_logs (
        id integer primary key,
        user_name text,
        action text,
        entity text,
        entity_id text,
        payload text,
        created_at text default current_timestamp
    );
    """,
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


# ===== Email discovery and verification helpers =====
import re as _re
from urllib.parse import urljoin, urlparse
from html import unescape as _html_unescape

def _extract_emails_from_text(txt: str):
    if not isinstance(txt, str) or not txt:
        return set()
    # decode common obfuscations
    t = txt
    t = _html_unescape(t)
    t = t.replace(" [at] ", "@").replace(" (at) ", "@").replace("[at]", "@").replace("(at)", "@")
    t = t.replace(" [dot] ", ".").replace(" (dot) ", ".").replace("[dot]", ".").replace("(dot)", ".")
    t = t.replace(" at ", "@") if "@" not in t else t
    # generic email regex
    rx = _re.compile(r'[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}', _re.I)
    return set(m.group(0).lower() for m in rx.finditer(t))


def crawl_site_for_emails(base_url: str, max_pages: int = 5):
    """
    Crawl a company's site to find emails.
    - No dependency on BeautifulSoup (uses regex fallback).
    - Prioritizes common contact pages.
    - Follows internal links up to max_pages.
    Returns {'emails': [...], 'pages_crawled': n, 'ok': True/False, 'error': '...'}
    """
    out = {'emails': [], 'pages_crawled': 0, 'ok': True}
    try:
        import requests
        from urllib.parse import urljoin, urlparse
        import re as _re
    except Exception as e:
        out['ok'] = False; out['error'] = f"Missing libs: {e}"
        return out

    try:
        if not base_url:
            out['ok'] = False; out['error'] = "empty url"; return out
        if not base_url.startswith(("http://", "https://")):
            base_url = "http://" + base_url

        parsed = urlparse(base_url)
        root = f"{parsed.scheme}://{parsed.netloc}"
        session = requests.Session(); session.headers.update({"User-Agent": "ELA-Email-Crawler/1.0"})
        seen = set()
        queue = []

        # Prioritize common paths
        for path in ["/contact", "/contact-us", "/about", "/team", "/leadership", "/staff", "/people", "/privacy", "/terms", "/sitemap", "/press"]:
            queue.append(urljoin(root, path))
        queue.append(base_url)

        email_rx = _re.compile(r'[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}', _re.I)
        href_rx = _re.compile(r"href=[\"\']([^\"\']+)[\"\']", _re.I)
        mailto_rx = _re.compile(r"href=[\"\']mailto:([^\"\']+)[\"\']", _re.I)

        def _decode(text: str) -> str:
            t = text or ""
            t = t.replace(" [at] ", "@").replace(" (at) ", "@").replace("[at]", "@").replace("(at)", "@")
            t = t.replace(" [dot] ", ".").replace(" (dot) ", ".").replace("[dot]", ".").replace("(dot)", ".")
            return t

        emails = set()

        while queue and out['pages_crawled'] < max_pages:
            url = queue.pop(0)
            if url in seen:
                continue
            seen.add(url)
            try:
                r = session.get(url, timeout=8)
                out['pages_crawled'] += 1
                if r.status_code >= 400 or not r.content:
                    continue
                html = r.text or ""

                # Email extraction
                for m in email_rx.finditer(_decode(html)):
                    emails.add(m.group(0).lower())

                # mailto links
                for m in mailto_rx.finditer(html):
                    addr = m.group(1).split("?")[0]
                    for e in email_rx.findall(_decode(addr)):
                        emails.add(e.lower())

                # Internal links
                for m in href_rx.finditer(html):
                    href = m.group(1)
                    if href.startswith("#") or href.startswith("javascript:"):
                        continue
                    nxt = urljoin(url, href)
                    # Only crawl within the same host
                    if urlparse(nxt).netloc == parsed.netloc and nxt not in seen and len(seen) + len(queue) < 100:
                        low = nxt.lower()
                        if any(seg in low for seg in ("/contact","/about","/team","/lead","/staff","/people","/privacy","/press","/sitemap")):
                            queue.insert(0, nxt)
                        else:
                            queue.append(nxt)
            except Exception:
                continue

        out['emails'] = sorted(emails)
        return out
    except Exception as e:
        out['ok'] = False; out['error'] = str(e); return out


def verify_email_smtp(address: str, timeout: int = 5):
    """
    Lightweight verifier: DNS MX lookup + optional SMTP helo+rcpt check.
    Returns tuple(status, detail) where status in {'valid','invalid','unknown'}.
    Robust to environments where outbound network is blocked.
    """
    try:
        if not isinstance(address, str) or "@" not in address:
            return ("invalid", "not an email")
        local, domain = address.rsplit("@",1)
        if not local or not domain:
            return ("invalid", "bad parts")
        status = "unknown"; detail = "not verified"
        try:
            import dns.resolver  # type: ignore
            answers = dns.resolver.resolve(domain, 'MX', lifetime=timeout)
            if not answers:
                return ("unknown", "no MX")
            mx_host = sorted([(r.preference, str(r.exchange).rstrip('.')) for r in answers])[0][1]
        except Exception as e:
            return ("unknown", f"dns fail: {e}")
        # Try SMTP dialogue (best effort)
        try:
            import smtplib
            server = smtplib.SMTP(timeout=timeout)
            server.connect(mx_host)
            server.helo("example.com")
            server.mail("check@example.com")
            code, resp = server.rcpt(address)
            server.quit()
            if 200 <= code < 300:
                status = "valid"; detail = f"smtp {code}"
            elif 500 <= code < 600:
                status = "invalid"; detail = f"smtp {code}"
            else:
                status = "unknown"; detail = f"smtp {code}"
        except Exception as e:
            status = "unknown"; detail = f"smtp fail: {e}"
        return (status, detail)
    except Exception as e:
        return ("unknown", f"err: {e}")

def ensure_vendor_email_status_column(conn):
    try:
        conn.execute("alter table vendors add column email_status text")
        conn.commit()
    except Exception:
        pass  # already exists

def get_db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def ensure_schema():
    conn = get_db()
    cur = conn.cursor()
    for ddl in SCHEMA.values(): cur.execute(ddl)
    try:
        cur.execute("create unique index if not exists idx_opportunities_notice on opportunities(sam_notice_id)")
    except Exception:
        pass
    # seed default users
    try:
        cur.execute("insert or ignore into users(name,email) values('Quincy',''),('Charles',''),('Collin','')")
    except Exception:
        pass
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
                try: st.toast(f"Using fallback model: {model_name}", icon="⚙️")
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
                try: st.toast(f"Using fallback model: {model_name}", icon="⚙️")
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

# ---------- SAM search ----------
def sam_search(
    naics_list, min_days=3, limit=100, keyword=None, posted_from_days=30,
    notice_types="Combined Synopsis/Solicitation,Solicitation,Presolicitation,SRCSGT", active="true"
):
    if not SAM_API_KEY:
        return pd.DataFrame(), {"ok": False, "reason": "missing_key", "detail": "SAM_API_KEY is empty."}
    if not naics_list and not keyword:
        return pd.DataFrame(), {"ok": False, "reason": "empty_filters", "detail": "Provide NAICS or a keyword."}

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
    if notice_types: params["noticeType"] = notice_types
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

def save_opportunities(df):
    if df is None or df.empty:
        return
    
    conn = get_db()
    cur = conn.cursor()
    cols = ["sam_notice_id","title","agency","naics","psc","place_of_performance",
            "response_due","posted","type","url","attachments_json"]
    df2 = df.copy()
    for c in cols:
        if c not in df2.columns:
            df2[c] = None
    df2 = df2.where(pd.notnull(df2), None)
    def norm(x):
        if x is None: return None
        try:
            import numpy as _np
            if isinstance(x, float) and _np.isnan(x): return None
        except Exception: pass
        s = str(x); return None if s.lower() == "nan" else s
    for _, r in df2.iterrows():
        cur.execute(
            """insert or ignore into opportunities
               (sam_notice_id,title,agency,naics,psc,place_of_performance,
                response_due,posted,type,url,attachments_json,status)
               values(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (norm(r["sam_notice_id"]), norm(r["title"]), norm(r["agency"]), norm(r["naics"]),
             norm(r["psc"]), norm(r["place_of_performance"]), norm(r["response_due"]),
             norm(r["posted"]), norm(r["type"]), norm(r["url"]), norm(r["attachments_json"]), "New"))
    conn.commit()

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
    def _ok(v): return "✅" if v else "❌"
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

tabs = st.tabs(["SAM Watch","Pipeline","Subcontractor Finder","Contacts","RFP Analyzer","Pricing & Quote Builder","Outreach","Capability Statement","White Paper Builder","Auto extract","Data Export","Chat Assistant"])
with tabs[7]:

st.subheader("Capability Statement Builder")

col1, col2 = st.columns(2)
with col1:
    company_name = st.text_input("Company Name", value=get_setting("cap_company","ELA Management LLC"))
    duns_cage = st.text_input("UEI / CAGE (optional)", value=get_setting("cap_uei",""))
    address = st.text_area("Address", height=70, value=get_setting("cap_addr","Houston, TX"))
    contacts = st.text_area("Contacts", height=70, value=get_setting("cap_contacts","Email: info@elamanagement.com\nPhone: (###) ###-####"))
with col2:
    core_comp = st.text_area("Core Competencies (one per line)", height=140, value=get_setting("cap_core","• Facility maintenance\n• Janitorial services\n• Grounds maintenance"))
    diffs     = st.text_area("Differentiators (one per line)", height=140, value=get_setting("cap_diff","• Rapid mobilization\n• Cleared staff\n• Quality control"))
    past_perf = st.text_area("Past Performance (bullets)", height=140, value=get_setting("cap_past","• Agency X — $1.2M, CPARS Very Good"))
    naics_list = st.text_input("NAICS (comma-separated)", value=get_setting("cap_naics","561720, 561730, 561210"))
    psc_list   = st.text_input("PSC (comma-separated)", value=get_setting("cap_psc","S201, S208"))

st.markdown("#### Preview")
st.write(f"**{company_name}**")
st.write(address)
st.write(contacts)
st.write("**Core Competencies**")
st.write(core_comp)
st.write("**Differentiators**")
st.write(diffs)
st.write("**Past Performance**")
st.write(past_perf)
st.write("**Codes**")
st.write(f"NAICS: {naics_list} | PSC: {psc_list}")

colA, colB = st.columns(2)
with colA:
    if st.button("Export DOCX", key="cap_export_docx"):
        from docx import Document
        doc = Document()
        doc.add_heading(company_name, 0)
        doc.add_paragraph(address)
        doc.add_paragraph(contacts)
        doc.add_heading("Core Competencies", level=1)
        for line in core_comp.splitlines():
            if line.strip():
                doc.add_paragraph(line.strip("• ").strip(), style='List Bullet')
        doc.add_heading("Differentiators", level=1)
        for line in diffs.splitlines():
            if line.strip():
                doc.add_paragraph(line.strip("• ").strip(), style='List Bullet')
        doc.add_heading("Past Performance", level=1)
        for line in past_perf.splitlines():
            if line.strip():
                doc.add_paragraph(line.strip("• ").strip(), style='List Bullet')
        doc.add_heading("Codes", level=1)
        doc.add_paragraph(f"NAICS: {naics_list}")
        doc.add_paragraph(f"PSC: {psc_list}")
        outp = "/mnt/data/capability_statement.docx"
        doc.save(outp)
        st.success("DOCX exported.")
        st.markdown(f"[Download Capability Statement](sandbox:{outp})")
with colB:
    try:
        from reportlab.lib.pagesizes import LETTER
        from reportlab.pdfgen import canvas
        if st.button("Export PDF", key="cap_export_pdf"):
            pdf_path = "/mnt/data/capability_statement.pdf"
            c = canvas.Canvas(pdf_path, pagesize=LETTER)
            width, height = LETTER
            y = height - 36
            def write_line(txt, size=11, bold=False):
                nonlocal y
                if y < 54:
                    c.showPage()
                    y = height - 36
                if bold:
                    c.setFont("Helvetica-Bold", size)
                else:
                    c.setFont("Helvetica", size)
                c.drawString(36, y, txt[:110])
                y -= 14
            write_line(company_name, 16, True)
            for line in (address + "\n" + contacts).splitlines():
                write_line(line)
            write_line("Core Competencies", 13, True)
            for line in core_comp.splitlines():
                write_line("• " + line.strip("• ").strip())
            write_line("Differentiators", 13, True)
            for line in diffs.splitlines():
                write_line("• " + line.strip("• ").strip())
            write_line("Past Performance", 13, True)
            for line in past_perf.splitlines():
                write_line("• " + line.strip("• ").strip())
            write_line("Codes", 13, True)
            write_line(f"NAICS: {naics_list}")
            write_line(f"PSC: {psc_list}")
            c.save()
            st.success("PDF exported.")
            st.markdown(f"[Download PDF](sandbox:{pdf_path})")
    except Exception as e:
        st.caption("PDF export unavailable here; DOCX export works.")


\n\n
# ================== Business Docs Tools ==================
tool_tabs = st.tabs(["Capability Statement","White Paper Builder","Auto extract","Data Export","Chat Assistant"])

# Capability Statement
with tool_tabs[0]:

st.subheader("Capability Statement Builder")

col1, col2 = st.columns(2)
with col1:
    company_name = st.text_input("Company Name", value=get_setting("cap_company","ELA Management LLC"))
    duns_cage = st.text_input("UEI / CAGE (optional)", value=get_setting("cap_uei",""))
    address = st.text_area("Address", height=70, value=get_setting("cap_addr","Houston, TX"))
    contacts = st.text_area("Contacts", height=70, value=get_setting("cap_contacts","Email: info@elamanagement.com\nPhone: (###) ###-####"))
with col2:
    core_comp = st.text_area("Core Competencies (one per line)", height=140, value=get_setting("cap_core","• Facility maintenance\n• Janitorial services\n• Grounds maintenance"))
    diffs     = st.text_area("Differentiators (one per line)", height=140, value=get_setting("cap_diff","• Rapid mobilization\n• Cleared staff\n• Quality control"))
    past_perf = st.text_area("Past Performance (bullets)", height=140, value=get_setting("cap_past","• Agency X — $1.2M, CPARS Very Good"))
    naics_list = st.text_input("NAICS (comma-separated)", value=get_setting("cap_naics","561720, 561730, 561210"))
    psc_list   = st.text_input("PSC (comma-separated)", value=get_setting("cap_psc","S201, S208"))

st.markdown("#### Preview")
st.write(f"**{company_name}**")
st.write(address)
st.write(contacts)
st.write("**Core Competencies**")
st.write(core_comp)
st.write("**Differentiators**")
st.write(diffs)
st.write("**Past Performance**")
st.write(past_perf)
st.write("**Codes**")
st.write(f"NAICS: {naics_list} | PSC: {psc_list}")

colA, colB = st.columns(2)
with colA:
    if st.button("Export DOCX", key="cap_export_docx"):
        from docx import Document
        doc = Document()
        doc.add_heading(company_name, 0)
        doc.add_paragraph(address)
        doc.add_paragraph(contacts)
        doc.add_heading("Core Competencies", level=1)
        for line in core_comp.splitlines():
            if line.strip():
                doc.add_paragraph(line.strip("• ").strip(), style='List Bullet')
        doc.add_heading("Differentiators", level=1)
        for line in diffs.splitlines():
            if line.strip():
                doc.add_paragraph(line.strip("• ").strip(), style='List Bullet')
        doc.add_heading("Past Performance", level=1)
        for line in past_perf.splitlines():
            if line.strip():
                doc.add_paragraph(line.strip("• ").strip(), style='List Bullet')
        doc.add_heading("Codes", level=1)
        doc.add_paragraph(f"NAICS: {naics_list}")
        doc.add_paragraph(f"PSC: {psc_list}")
        outp = "/mnt/data/capability_statement.docx"
        doc.save(outp)
        st.success("DOCX exported.")
        st.markdown(f"[Download Capability Statement](sandbox:{outp})")
with colB:
    try:
        from reportlab.lib.pagesizes import LETTER
        from reportlab.pdfgen import canvas
        if st.button("Export PDF", key="cap_export_pdf"):
            pdf_path = "/mnt/data/capability_statement.pdf"
            c = canvas.Canvas(pdf_path, pagesize=LETTER)
            width, height = LETTER
            y = height - 36
            def write_line(txt, size=11, bold=False):
                nonlocal y
                if y < 54:
                    c.showPage()
                    y = height - 36
                if bold:
                    c.setFont("Helvetica-Bold", size)
                else:
                    c.setFont("Helvetica", size)
                c.drawString(36, y, txt[:110])
                y -= 14
            write_line(company_name, 16, True)
            for line in (address + "\n" + contacts).splitlines():
                write_line(line)
            write_line("Core Competencies", 13, True)
            for line in core_comp.splitlines():
                write_line("• " + line.strip("• ").strip())
            write_line("Differentiators", 13, True)
            for line in diffs.splitlines():
                write_line("• " + line.strip("• ").strip())
            write_line("Past Performance", 13, True)
            for line in past_perf.splitlines():
                write_line("• " + line.strip("• ").strip())
            write_line("Codes", 13, True)
            write_line(f"NAICS: {naics_list}")
            write_line(f"PSC: {psc_list}")
            c.save()
            st.success("PDF exported.")
            st.markdown(f"[Download PDF](sandbox:{pdf_path})")
    except Exception as e:
        st.caption("PDF export unavailable here; DOCX export works.")


# White Paper Builder
with tool_tabs[1]:

st.subheader("White Paper Builder")

topic = st.text_input("Topic", value=get_setting("wp_topic","Improving grounds maintenance outcomes with data-driven scheduling"))
problem = st.text_area("Problem Statement", height=120, value=get_setting("wp_problem","Agencies struggle to balance cost and quality in grounds maintenance."))
audience = st.text_input("Audience", value=get_setting("wp_audience","Contracting Officers and Program Managers"))
sections = st.text_area("Custom Sections (one per line)", height=120, value=get_setting("wp_sections","Executive Summary\nBackground\nCurrent Challenges\nProposed Approach\nImplementation Plan\nBenefits\nRisks & Mitigations\nConclusion"))

if st.button("Generate Draft"):
    st.session_state["wp_draft"] = []
    for s in sections.splitlines():
        s = s.strip()
        if not s:
            continue
        para = f"{s}: For {audience}, this paper addresses {problem} by proposing practical steps around {topic}. Provide evidence, cite sources, and include metrics."
        st.session_state["wp_draft"].append({"section": s, "content": para})
draft = st.session_state.get("wp_draft", [])

if draft:
    for blk in draft:
        st.markdown(f"### {blk['section']}")
        st.write(blk["content"])

    if st.button("Export DOCX", key="wp_export"):
        from docx import Document
        doc = Document()
        doc.add_heading(topic, 0)
        doc.add_paragraph(f"Audience: {audience}")
        doc.add_paragraph(f"Problem: {problem}")
        for blk in draft:
            doc.add_heading(blk["section"], level=1)
            doc.add_paragraph(blk["content"])
        outp = "/mnt/data/white_paper_draft.docx"
        doc.save(outp)
        st.success("DOCX exported.")
        st.markdown(f"[Download White Paper](sandbox:{outp})")

# Auto Extract
with tool_tabs[2]:

st.subheader("Auto Extract Key Details from SOW / Solicitation")

uploads = st.file_uploader("Upload txt/pdf/docx", type=["txt","pdf","docx"], accept_multiple_files=True, key="autoex_files")
pasted = st.text_area("Or paste text", height=220, key="autoex_paste")

def _read_txt(file):
    try:
        return file.read().decode("utf-8", errors="ignore")
    except Exception:
        try:
            return file.read().decode("latin-1", errors="ignore")
        except Exception:
            return ""

def _read_pdf(file):
    try:
        reader = PdfReader(file)
        return "\n".join((page.extract_text() or "") for page in reader.pages)
    except Exception:
        return ""

def _read_docx(file):
    try:
        d = docx.Document(file)
        return "\n".join(p.text for p in d.paragraphs)
    except Exception:
        return ""

def collect_text(files, pasted):
    parts = []
    for f in (files or []):
        name = (f.name or "").lower()
        if name.endswith(".pdf"):
            parts.append(_read_pdf(f))
        elif name.endswith(".docx"):
            parts.append(_read_docx(f))
        else:
            parts.append(_read_txt(f))
    if pasted and pasted.strip():
        parts.append(pasted)
    return "\n\n".join([p for p in parts if p])

def extract_sections(text):
    import re as _re
    tx = text
    def find_block(*labels):
        for lab in labels:
            m = _re.search(lab, tx, _re.I)
            if m:
                start = m.start()
                # take 1200 chars from label start as heuristic block
                return tx[start:start+1200]
        return ""
    data = {
        "Scope of Work": find_block(r"scope of work", r"statement of work", r"^1\.\s*scope", r"SOW"),
        "Technical Specifications": find_block(r"technical requirements", r"specifications", r"performance work statement", r"PWS"),
        "Performance Metrics": find_block(r"performance standards", r"quality", r"acceptance criteria", r"service levels"),
        "Timeline and Milestones": find_block(r"period of performance", r"schedule", r"timeline", r"milestone"),
        "Evaluation Criteria": find_block(r"evaluation criteria", r"factors", r"basis of award", r"best value", r"lowest price"),
        "Submission Requirements": find_block(r"proposal submission", r"offers due", r"instructions to offerors", r"addendum to 52\.212-1", r"email.*submit", r"hand deliver|mailed"),
    }
    return data

if st.button("Extract Details", key="autoex_go"):
    text = collect_text(uploads, pasted)
    if not text.strip():
        st.warning("No text found.")
    else:
        data = extract_sections(text)
        df = pd.DataFrame(list(data.items()), columns=["Section","Excerpt"])
        st.dataframe(df, use_container_width=True)
        st.session_state["autoex_json"] = data

if "autoex_json" in st.session_state:
    if st.button("Download JSON", key="autoex_dl"):
        import json as _json
        outp = "/mnt/data/auto_extract.json"
        with open(outp, "w", encoding="utf-8") as f:
            _json.dump(st.session_state["autoex_json"], f, indent=2)
        st.markdown(f"[Download Extracted JSON](sandbox:{outp})")

# Data Export
with tool_tabs[3]:

st.subheader("Data Export")

df = st.session_state.get("sam_results_df")
if isinstance(df, pd.DataFrame) and not df.empty:
    st.dataframe(df, use_container_width=True)
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Export CSV", key="export_csv"):
            outp = "/mnt/data/sam_results.csv"
            df.to_csv(outp, index=False)
            st.success("CSV exported.")
            st.markdown(f"[Download CSV](sandbox:{outp})")
    with c2:
        if st.button("Export XLSX", key="export_xlsx"):
            outp = "/mnt/data/sam_results.xlsx"
            with pd.ExcelWriter(outp) as xw:
                df.to_excel(xw, index=False, sheet_name="SAM Results")
            st.success("XLSX exported.")
            st.markdown(f"[Download XLSX](sandbox:{outp})")
else:
    st.info("No SAM results available yet.")

# Chat Assistant
with tool_tabs[4]:

st.subheader("Chat Assistant")

if "chat_history" not in st.session_state:
    st.session_state["chat_history"] = []

uploads = st.file_uploader("Optional: attach files (txt/pdf/docx)", type=["txt","pdf","docx"], accept_multiple_files=True, key="chat_files")
if uploads:
    st.caption(f"Attached: {', '.join([u.name for u in uploads])}")

user_msg = st.chat_input("Type your message")
if user_msg:
    st.session_state["chat_history"].append({"role":"user","content":user_msg})
    # Echo-style assistant with simple acknowledgement
    reply = "I read your message. If you want me to analyze attachments, say 'analyze files'."
    st.session_state["chat_history"].append({"role":"assistant","content":reply})
    st.rerun()

for m in st.session_state["chat_history"]:
    with st.chat_message(m["role"]):
        st.write(m["content"])

