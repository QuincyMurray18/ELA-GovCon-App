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


# Brand sidebar
try:
    st.sidebar.image("/mnt/data/ELA Logo.png", caption="ELA Management LLC", use_container_width=True)
    st.sidebar.markdown(f"**Contact:** {COMPANY_EMAIL}")
    st.sidebar.markdown(f"**UEI:** {COMPANY_UEI}  **CAGE:** {COMPANY_CAGE}")
    # user picker
    try:
        conn = get_db()
        names = [r[0] for r in conn.execute("select name from users order by name").fetchall()]
    except Exception:
        names = ["Quincy","Charles","Collin"]
    default_idx = 0
    cur_user = get_current_user() or names[0]
    if cur_user in names:
        default_idx = names.index(cur_user)
    pick_user = st.sidebar.selectbox("I am", options=names, index=default_idx, key="user_pick")
    set_current_user(pick_user)

except Exception as _e:
    pass

with tabs[1]:
    st.subheader("Opportunities pipeline")
    try:
        st.caption(f"ELA Management LLC — UEI {COMPANY_UEI} • CAGE {COMPANY_CAGE} • DUNS {COMPANY_DUNS} • {COMPANY_EMAIL}")
    except Exception:
        pass

    conn = get_db()

    # Load pipeline table (saved opportunities only)
    df_opp = pd.read_sql_query("select * from opportunities order by created_at desc", conn)

    if df_opp.empty:
        st.info("No opportunities saved yet. Use **SAM Watch** to select and save notices into the pipeline.")
    else:
        # Filters
        colf1, colf2, colf3 = st.columns(3)
        with colf1:
            assignees = sorted([x for x in df_opp.get("assignee", pd.Series([], dtype=str)).fillna("").unique().tolist() if x])
            default_team = ["Quincy", "Charles", "Collin"]
            for t in default_team:
                if t not in assignees:
                    assignees.append(t)
            assignee_pick = st.multiselect("Assignee", options=assignees or default_team, default=assignees or default_team)
        with colf2:
            statuses = sorted([x for x in df_opp.get("status", pd.Series([], dtype=str)).fillna("").unique().tolist() if x]) or ["New","Qualify","Bid","No Bid","Submitted","Won","Lost"]
            status_pick = st.multiselect("Status", options=statuses, default=statuses)
        with colf3:
            qtext = st.text_input("Search title/agency/NAICS", value="")

        # Apply filters
        dfv = df_opp.copy()
        if assignee_pick and "assignee" in dfv.columns:
            dfv = dfv[dfv["assignee"].fillna("").isin(assignee_pick)]
        if status_pick and "status" in dfv.columns:
            dfv = dfv[dfv["status"].fillna("").isin(status_pick)]
        if qtext:
            m = None
            for col in ["title","agency","naics","psc","place_of_performance"]:
                if col in dfv.columns:
                    s = dfv[col].fillna("").astype(str).str.contains(qtext, case=False, na=False)
                    m = s if m is None else (m | s)
            if m is not None:
                dfv = dfv[m]

        # Display editor with hyperlink column for 'url'
        st.data_editor(
            dfv.reset_index(drop=True),
            use_container_width=True,
            num_rows="dynamic",
            key="pipeline_grid",
            column_config={"url": st.column_config.LinkColumn("Link", display_text="open")}
        )

        # Optional: bulk assign by Notice IDs
        with st.expander("Bulk assign by SAM Notice IDs (optional)"):
            ids_str = st.text_input("Enter SAM Notice IDs (comma-separated)", value="")
            new_asg = st.selectbox("Assign to", options=["Quincy","Charles","Collin",""], index=0)
            if st.button("Apply bulk assignment"):
                if ids_str.strip() and new_asg:
                    ids = [s.strip() for s in ids_str.split(",") if s.strip()]
                    cur = conn.cursor()
                    for sid in ids:
                        try:
                            cur.execute("update opportunities set assignee=? where sam_notice_id=?", (new_asg, sid))
                        except Exception:
                            pass
                    conn.commit()
                    st.success(f"Assigned {len(ids)} notice(s) to {new_asg}.")
                else:
                    st.info("Provide IDs and an assignee.")

        # Save inline edits (status/assignee/notes)
        if st.button("Save pipeline edits"):
            try:
                grid_df = st.session_state.get("pipeline_grid")
                if isinstance(grid_df, pd.DataFrame) and not grid_df.empty:
                    cur = conn.cursor()
                    editable_cols = {"assignee","status","notes"}
                    for _, r in grid_df.fillna("").iterrows():
                        sets = []
                        vals = []
                        for c in ["assignee","status","notes"]:
                            if c in grid_df.columns and c in editable_cols:
                                sets.append(f"{c}=?")
                                vals.append(str(r.get(c,"")))
                        if sets and "sam_notice_id" in grid_df.columns:
                            vals.append(str(r.get("sam_notice_id","")))
                            cur.execute(f"update opportunities set {', '.join(sets)}, updated_at=current_timestamp where sam_notice_id=?", tuple(vals))
                    conn.commit()
                    st.success("Pipeline updated.")
                else:
                    st.info("No edits to save.")
            except Exception as e:
                st.warning(f"Could not save edits: {e}")

with tabs[2]:
    st.subheader("Find subcontractors and rank by fit")

    # Controls
    trade = st.text_input("Trade", value=get_setting("default_trade", "Janitorial"))
    loc = st.text_input("Search location or Place of Performance", value=get_setting("home_loc", "Houston, TX"))
    radius_miles = st.slider("Search radius (miles)", min_value=5, max_value=200, value=50)
    name_filter = st.text_input("Filter by name contains (optional)", value="")
    sort_by = st.selectbox("Sort by", options=["distance_miles", "company", "website", "phone"], index=0)
    find_emails = st.checkbox("Try to find emails from website (slow)", value=False)
    max_pages = st.slider("Max pages per site (email crawl)", min_value=1, max_value=12, value=5)

    colA, colB, colC = st.columns(3)

    with colA:
        if st.button("Google Places import"):
            radius_m = int(radius_miles * 1609.34)
            results, info = google_places_search(f"{trade} small business", loc, radius_m)

            df_new = pd.DataFrame(results) if results else pd.DataFrame(
                columns=["company", "website", "phone", "distance_km", "city", "state", "notes"]
            )

            if not df_new.empty:
                # Convert km → miles; apply filters/sort; de-dupe
                if "distance_km" in df_new.columns:
                    df_new["distance_miles"] = (pd.to_numeric(df_new["distance_km"], errors="coerce").fillna(0) * 0.621371).round(2)
                else:
                    df_new["distance_miles"] = None

                if name_filter:
                    df_new = df_new[df_new["company"].astype(str).str.contains(name_filter, case=False, na=False)]

                if sort_by in df_new.columns:
                    df_new = df_new.sort_values(by=sort_by, na_position="last")

                df_new = df_new.drop_duplicates(subset=["company", "website"], keep="first")

                st.session_state["places_df"] = df_new

                st.dataframe(
                    df_new,
                    use_container_width=True,
                    column_config={
                        "website": st.column_config.LinkColumn("Website", display_text="open"),
                        "distance_miles": st.column_config.NumberColumn("Distance (miles)")
                    },
                )
                st.success(f"Loaded {len(df_new)} vendors from Places")
            else:
                st.warning("No results (check API key, location, or filters).")

        if st.button("Save to vendors"):
            df_new = st.session_state.get("places_df")
            if df_new is None or df_new.empty:
                st.info("Run Google Places import first.")
            else:
                conn = get_db()
                saved, skipped = 0, 0

                for _, r in df_new.fillna("").iterrows():
                    try:
                        email = r.get("email", "")
                        notes = r.get("notes", "")

                        if find_emails and r.get("website"):
                            crawled = crawl_site_for_emails(r.get("website"), max_pages=max_pages)
                            emails = sorted(crawled.get("emails", []))
                            email = email or (emails[0] if emails else "")
                            others = [e for e in emails if e and e != email]
                            if others:
                                notes = (notes + (" | other emails: " + ", ".join(others[:5]))).strip()

                        conn.execute(
                            """insert or ignore into vendors
                               (company, naics, trades, phone, email, website, city, state, certifications, set_asides, notes, source)
                               values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                r.get("company"), "", trade,
                                r.get("phone", ""), email, r.get("website", ""),
                                r.get("city", ""), r.get("state", ""),
                                "", "", notes, "GooglePlaces",
                            ),
                        )
                        saved += 1
                    except Exception:
                        skipped += 1

                conn.commit()
                st.success(f"Saved {saved} vendors (skipped {skipped})")

    with colB:
        st.markdown("LinkedIn quick search")
        st.link_button("Open LinkedIn", linkedin_company_search(f"{trade} {loc}"))

    with colC:
        vendor_csv = st.file_uploader("Upload vendor CSV to merge", type=["csv"])
        if vendor_csv is not None:
            df_u = pd.read_csv(vendor_csv)
            st.dataframe(df_u.head(50), use_container_width=True)

            if st.button("Append uploaded vendors"):
                conn = get_db()
                for _, r in df_u.fillna("").iterrows():
                    conn.execute(
                        """insert or ignore into vendors
                           (company, naics, trades, phone, email, website, city, state, certifications, set_asides, notes, source)
                           values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            r.get("Company") or r.get("company"),
                            r.get("NAICS") or r.get("naics", ""),
                            r.get("Trades") or r.get("trades", trade),
                            r.get("Phone") or r.get("phone", ""),
                            r.get("Email") or r.get("email", ""),
                            r.get("Website") or r.get("website", ""),
                            r.get("City") or r.get("city", ""),
                            r.get("State") or r.get("state", ""),
                            r.get("Certifications") or r.get("certifications", ""),
                            r.get("SetAsides") or r.get("set_asides", ""),
                            r.get("Notes") or r.get("notes", ""),
                            "CSV",
                        ),
                    )
                conn.commit()
                st.success("Merged")

    # Existing vendor table (websites as hyperlinks)
    st.markdown("Vendor table")
    conn = get_db()
    df_v = pd.read_sql_query("select * from vendors order by updated_at desc, created_at desc", conn)
    ensure_vendor_email_status_column(conn)
    st.caption("Tip: click 'Verify emails' to mark addresses as valid/invalid/unknown.")
    grid = st.data_editor(
        df_v,
        use_container_width=True,
        num_rows="dynamic",
        key="vendor_grid",
        column_config={"website": st.column_config.LinkColumn("Website", display_text="open")},
    )
    if st.button("Save vendor edits"):
        cur = conn.cursor()
        for _, r in grid.iterrows():
            cur.execute(
                """update vendors set company=?, naics=?, trades=?, phone=?, email=?, website=?,
                   city=?, state=?, certifications=?, set_asides=?, notes=?, updated_at=current_timestamp
                   where id=?""",
                (
                    r["company"], r["naics"], r["trades"], r["phone"], r["email"], r["website"],
                    r["city"], r["state"], r["certifications"], r["set_asides"], r["notes"], int(r["id"])
                ),
            )
        conn.commit()
        st.success("Saved")

with tabs[3]:
    
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


with tabs[5]:
    st.subheader("Pricing & Quote Builder")
    st.markdown("### A) Pricing Intel — find comparable awards")
    st.caption("Tip: If API lookups are limited, upload a CSV of comparable awards (columns like 'Award Amount', 'Years', 'NAICS', 'Place', 'Keywords').")
    col_pi1, col_pi2, col_pi3 = st.columns(3)
    with col_pi1:
        pi_naics = st.text_input("NAICS (optional)", value="")
        pi_keywords = st.text_input("Keywords (optional, e.g., janitorial, mowing)", value="")
    with col_pi2:
        pi_place = st.text_input("Place of performance (optional city/state)", value="")
        pi_years_back = st.number_input("Lookback (years)", min_value=1, max_value=10, value=5, step=1)
    with col_pi3:
        pi_min_award = st.number_input("Min award $ (optional)", min_value=0.0, value=0.0, step=1000.0)
        pi_max_award = st.number_input("Max award $ (optional)", min_value=0.0, value=0.0, step=1000.0)
    comp_csv = st.file_uploader("Upload comparable awards CSV (optional)", type=["csv"], key="pricing_comp_csv")
    def _try_fetch_awards(naics, keywords, place, years_back, low, high):
        import requests, json, datetime
        info = {"ok": False, "reason": "", "hint": ""}
        rows = []
        try:
            url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
            since_year = datetime.date.today().year - int(years_back)
            filters = {"time_period":[{"fy":str(y)} for y in range(since_year, datetime.date.today().year+1)]}
            filters["award_type_codes"] = ["A","B","C","D"]
            if naics:
                filters.setdefault("naics_codes", [naics])
            if keywords:
                filters.setdefault("keywords", [keywords])
            body = {"filters": filters, "fields": ["Award ID","Recipient Name","Award Amount","Period of Performance Start Date","Period of Performance Current End Date","Awarding Agency Name","NAICS"], "page": 1, "limit": 25, "sort": "Award Amount", "order": "desc"}
            r = requests.post(url, json=body, timeout=25)
            if r.status_code != 200:
                try:
                    errj = r.json(); hint = errj.get("detail") or str(errj)[:300]
                except Exception:
                    hint = r.text[:300]
                info.update(ok=False, reason=f"http_{r.status_code}", hint=hint); return rows, info
            data = r.json(); results = data.get("results", []) or []
            for it in results:
                amt = it.get("Award Amount") or 0
                if low and amt < low: continue
                if high and high > 0 and amt > high: continue
                start = it.get("Period of Performance Start Date") or ""
                end = it.get("Period of Performance Current End Date") or ""
                rows.append({"award_id": it.get("Award ID",""),"recipient": it.get("Recipient Name",""),"amount": amt,"start": start,"end": end,"agency": it.get("Awarding Agency Name",""),"naics": it.get("NAICS","")})
            info.update(ok=True, count=len(rows)); return rows, info
        except Exception as e:
            info.update(ok=False, reason="exception", hint=str(e)[:200]); return rows, info
    comp_rows, comp_info = [], {}
    if st.button("Fetch comparable awards"):
        comp_rows, comp_info = _try_fetch_awards(pi_naics.strip(), pi_keywords.strip(), pi_place.strip(), pi_years_back, pi_min_award, pi_max_award)
        if not comp_info.get("ok"):
            st.warning(f"Award lookup limited: {comp_info}")
    if comp_rows:
        df_comp = pd.DataFrame(comp_rows)
        def _years(start, end):
            try:
                s = pd.to_datetime(start); e = pd.to_datetime(end)
                if pd.isna(s) or pd.isna(e): return 0.0
                return max(0.0, (e - s).days / 365.25)
            except Exception: return 0.0
        if not df_comp.empty:
            df_comp["years"] = df_comp.apply(lambda r: _years(r["start"], r["end"]), axis=1)
            df_comp["avg_per_year"] = df_comp.apply(lambda r: (r["amount"] / r["years"]) if r["years"] and r["years"] > 0 else r["amount"], axis=1)
            def _ctype(y):
                if y < 1.25: return "One year or one-time"
                if y < 2.75: return "2 to 3 years"
                if y < 3.75: return "3 to 4 years"
                if y < 5.5: return "5 years typical base + 4 options"
                return "Multi-year"
            df_comp["likely_type"] = df_comp["years"].apply(_ctype)
            st.dataframe(df_comp, use_container_width=True)
            st.caption("Heuristic: If years is around 1 it is likely one year or one-time. Around 5 suggests base plus four option years.")
            med_award = df_comp['amount'].median()
            med_per_year = df_comp['avg_per_year'].median()
            st.metric("Comparable awards", len(df_comp)); st.metric("Median award $", f"${med_award:,.0f}"); st.metric("Median avg per year $", f"${med_per_year:,.0f}")
            st.markdown("#### Compare your target price")
            your_price = st.number_input("Your target total price $", min_value=0.0, value=0.0, step=1000.0, key="your_target_price")
            your_years_hint = st.number_input("Expected contract years for your quote", min_value=1.0, value=5.0, step=0.5, key="your_target_years")
            if your_price > 0:
                bench_total = med_per_year * your_years_hint if med_per_year and your_years_hint else med_award
                if bench_total and bench_total > 0:
                    pct = (your_price - bench_total) / bench_total * 100.0
                    verdict = "near market"; 
                    if pct > 15: verdict = "above market"
                    if pct < -10: verdict = "below market"
                    st.metric("Bench total for duration", f"${bench_total:,.0f}"); st.metric("Your price vs bench", f"{pct:+.1f}% {verdict}")
    if comp_csv is not None:
        import pandas as _pd
        df_up = _pd.read_csv(comp_csv)
        st.dataframe(df_up.head(50), use_container_width=True)
        if "Award Amount" in df_up.columns:
            st.caption("Stats from uploaded awards"); st.metric("Rows", len(df_up)); st.metric("Median award $", f"${_pd.to_numeric(df_up['Award Amount'], errors='coerce').median():,.0f}")
    st.markdown("### B) BOE (Basis of Estimate) Checklist")
    boe_scope = st.text_area("Describe the scope (size, frequency, constraints, SLAs, shifts)", height=140, value="")
    boe_naics = st.text_input("Service/NAICS context (e.g., 561720 Janitorial)", value="")
    if st.button("Generate BOE checklist"):
        system = "You are a federal pricing analyst. Create a BOE checklist for completeness, accuracy, and auditability."
        prompt = f"Scope: {boe_scope}\\nContext: {boe_naics}\\nProvide a structured checklist: labor categories/rates, burden/fringe, OH/G&A, supplies/equipment, travel, subs, escalation, fee/profit, assumptions, exclusions, risks, compliance with SCA/WD if applicable."
        st.markdown(llm(system, prompt, max_tokens=900))
    st.markdown("### C) Quote Builder")
    col_q1, col_q2, col_q3 = st.columns(3)
    with col_q1:
        size_unit = st.selectbox("Sizing basis", ["Square Feet", "Acres", "FTEs"], index=0)
        size_value = st.number_input(f"{size_unit}", min_value=0.0, value=100000.0, step=1000.0)
        shifts = st.number_input("Shifts per day", min_value=1, value=1, step=1)
        days_per_week = st.number_input("Days per week", min_value=1, max_value=7, value=5, step=1)
    with col_q2:
        base_years = st.number_input("Base years", min_value=0, max_value=5, value=1, step=1)
        option_years = st.number_input("Option years", min_value=0, max_value=5, value=4, step=1)
        wage_rate = st.number_input("Average wage $/hr", min_value=0.0, value=16.0, step=0.5)
        burden_pct = st.number_input("Burden/Fringe %", min_value=0.0, value=30.0, step=1.0)
    with col_q3:
        overhead_pct = st.number_input("Overhead %", min_value=0.0, value=10.0, step=0.5)
        ga_pct = st.number_input("G&A %", min_value=0.0, value=8.0, step=0.5)
        materials_pct = st.number_input("Supplies/Materials %", min_value=0.0, value=6.0, step=0.5)
        travel_pct = st.number_input("Travel %", min_value=0.0, value=0.0, step=0.5)
        escalation_pct = st.number_input("Annual escalation %", min_value=0.0, value=3.0, step=0.5)
        profit_pct = st.number_input("Profit %", min_value=0.0, value=10.0, step=0.5)
    def estimate_ftes(unit, size, shifts):
        if unit == "Square Feet": return max(0.5, (size / 22000.0) * shifts)
        if unit == "Acres": return max(0.5, (size / 13.5) * shifts)
        return max(0.1, size * shifts)
    ftes = estimate_ftes(size_unit, size_value, shifts); st.caption(f"Estimated FTEs: {ftes:.2f} (heuristic)")
    hours_per_year = days_per_week * 52 * 8; years_total = (base_years + option_years) or 1
    rows = []; labor_rate_loaded = wage_rate * (1 + burden_pct/100.0)
    for y in range(1, years_total + 1):
        esc_multiplier = (1 + escalation_pct/100.0) ** (y - 1)
        labor_cost = ftes * hours_per_year * labor_rate_loaded * esc_multiplier
        overhead = labor_cost * (overhead_pct/100.0)
        ga = (labor_cost + overhead) * (ga_pct/100.0)
        direct = labor_cost
        other_directs = direct * (materials_pct/100.0) + direct * (travel_pct/100.0)
        subtotal = direct + overhead + ga + other_directs
        profit = subtotal * (profit_pct/100.0)
        total = subtotal + profit
        rows.append({"year": y, "labor_cost": labor_cost, "overhead": overhead, "g&a": ga, "other_directs": other_directs, "profit": profit, "total": total})
    df_quote = pd.DataFrame(rows)
    if not df_quote.empty:
        st.dataframe(df_quote.style.format({"labor_cost":"${:,.0f}","overhead":"${:,.0f}","g&a":"${:,.0f}","other_directs":"${:,.0f}","profit":"${:,.0f}","total":"${:,.0f}"}), use_container_width=True)
        st.metric("Total price (all years)", f"${df_quote['total'].sum():,.0f}")
        if size_value > 0 and size_unit in ("Square Feet","Acres"):
            per_unit = df_quote["total"].sum() / size_value / years_total
            unit_label = "sq ft-year" if size_unit=="Square Feet" else "acre-year"
            st.metric(f"Price per {unit_label}", f"${per_unit:,.2f}")
        xbytes = to_xlsx_bytes({"Quote": df_quote})
        st.download_button("Download quote to Excel", data=xbytes, file_name="quote_builder.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    st.markdown("### D) Margin guidance")
    st.caption("These are general ranges — tune to your risk, competition, and set-aside status.")
    st.write("- Routine services (janitorial/grounds): often 7–15% profit")
    st.write("- Specialized trades or remote work: often 12–20% profit")
    st.write("- High-risk or surge: potentially higher, justified in BOE")

with tabs[6]:
    
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


with tabs[0]:
    if st.button("Search SAM now"):

        open_browse = (not manual_naics.strip()) and (not keyword.strip()) and search_all
        kword = (keyword or "").strip()
        if open_browse:
            kword = "*"
        df, info = sam_search(naics_for_query, min_days=int(min_days), limit=400,
                              keyword=(keyword or None), posted_from_days=int(posted_from_days),
                              notice_types=",".join(picked_notice), active="true")
        if not info.get("ok") and (info.get("reason") == "empty_filters" or "Provide NAICS" in str(info)):
            df, info = sam_search(naics_for_query, min_days=int(min_days), limit=400,
                                  keyword=kword or "*", posted_from_days=int(posted_from_days),
                                  notice_types=",".join(picked_notice), active="true")
        if not info.get("ok"):
            st.error(f"SAM API error: {info}")
        else:
            if manual_naics.strip() and "naics" in df.columns:
                keep = set(a.strip() for a in manual_naics.split(",") if a.strip())
                df = df[df["naics"].astype(str).isin(keep)]
            if picked_notice and "type" in df.columns:
                df = df[df["type"].astype(str).isin(set(picked_notice))]
            if "response_due" in df.columns:
                try:
                    due = pd.to_datetime(df["response_due"], errors="coerce")
                    now = pd.Timestamp.utcnow().normalize()
                    df = df[(due.isna()) | (due >= now + pd.Timedelta(days=int(min_days)))]
                except Exception:
                    pass
            if "posted" in df.columns and int(posted_from_days) > 0:
                try:
                    posted = pd.to_datetime(df["posted"], errors="coerce")
                    cutoff = pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=int(posted_from_days))
                    df = df[(posted.isna()) | (posted >= cutoff)]
                except Exception:
                    pass
            if "attachments_json" in df.columns:
                import json as _json
                try:
                    df["attachments_count"] = df["attachments_json"].apply(lambda s: len(_json.loads(s)) if isinstance(s, str) and s.strip() else 0)
                except Exception:
                    df["attachments_count"] = 0
            st.session_state["sam_results_df"] = df.reset_index(drop=True)

    
    conn = get_db()

    col1, col2, col3 = st.columns(3)
    with col1:
        search_all = st.checkbox("All active (ignore watchlist)", value=True)
        posted_from_days = st.number_input("Look back (days)", min_value=1, step=1, value=30)
        min_days = st.number_input("Min days until due", min_value=0, step=1, value=3)
    with col2:
        manual_naics = st.text_input("Filter NAICS (comma-separated)", value="")
        keyword = st.text_input("Keyword (optional)", value="")
    with col3:
        notice_choices = ["Combined Synopsis/Solicitation","Solicitation","Presolicitation","SRCSGT"]
        picked_notice = st.multiselect("Notice types", options=notice_choices, default=notice_choices)
        default_assignee = st.selectbox("Default assignee on save", options=["","Quincy","Charles","Collin"],
                                        index=(["","Quincy","Charles","Collin"].index(get_current_user()) if get_current_user() in ["Quincy","Charles","Collin"] else 0))

    watch_codes = pd.read_sql_query("select code from naics_watch order by code", conn)["code"].tolist()
    naics_for_query = []
    if manual_naics.strip():
        naics_for_query = [c.strip() for c in manual_naics.split(",") if c.strip()]
    elif not search_all:
        naics_for_query = watch_codes

    

# --- Render SAM results (persisted) ---
df = st.session_state.get("sam_results_df")
if isinstance(df, pd.DataFrame) and not df.empty:
    show_cols = ["sam_notice_id","title","agency","naics","psc","place_of_performance","posted","response_due","type","attachments_count","url"]
    show_cols = [c for c in show_cols if c in df.columns]
    if "select" not in df.columns:
        df.insert(0, "select", False)
    grid = st.data_editor(df[["select"] + show_cols], use_container_width=True,
                          key="sam_results_grid",
                          column_config={"url": st.column_config.LinkColumn("Link", display_text="open")})
    sel_df = grid[grid["select"] == True] if "select" in grid.columns else df.iloc[0:0]
    st.caption(f"Selected: {len(sel_df)}")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Save selected to pipeline"):
            if sel_df is None or sel_df.empty:
                st.info("No rows selected.")
            else:
                save_opportunities(sel_df)
                st.success(f"Saved {len(sel_df)} selected opportunities to pipeline")
    with c2:
        if st.button("Clear results"):
            st.session_state["sam_results_df"] = None
    st.markdown("**View attachments for a notice**")
    pick_id = st.text_input("Enter SAM Notice ID", value="")
    if pick_id and "sam_notice_id" in df.columns:
        row = df[df["sam_notice_id"].astype(str) == str(pick_id)].head(1)
        if row.empty:
            st.info("Notice ID not found in this result set.")
        else:
            import json as _json
            atts = []
            try:
                atts = _json.loads(row.iloc[0].get("attachments_json") or "[]")
            except Exception:
                atts = []
            if atts:
                df_atts = pd.DataFrame(atts)
                st.dataframe(df_atts, use_container_width=True,
                             column_config={"url": st.column_config.LinkColumn("Attachment", display_text="download")})
            else:
                st.caption("No attachments listed for this notice.")
else:
    st.caption("Run a search to see results here.")

with tabs[4]:
    st.subheader("RFP Analyzer — persistent sessions with grading")
    conn = get_db()
    try:
        conn.execute("create table if not exists rfp_sessions (id integer primary key, title text, draft_text text, created_at text default current_timestamp)")
        conn.execute("create table if not exists rfp_messages (id integer primary key, session_id integer, role text, content text, created_at text default current_timestamp)")
        conn.execute("create table if not exists rfp_files (id integer primary key, session_id integer, filename text, mimetype text, content_text text, uploaded_at text default current_timestamp)")
        conn.commit()
    except Exception:
        pass
    sessions = pd.read_sql_query("select id, title, created_at from rfp_sessions order by created_at desc", conn)
    session_titles = ["➕ New RFP session"] + [f"{r['id']}: {r['title'] or '(untitled)'}" for _, r in sessions.iterrows()]
    pick = st.selectbox("Session", options=session_titles, index=0, key="rfp_pick")
    if pick == "➕ New RFP session":
        default_title = f"RFP {datetime.now().strftime('%b %d %I:%M %p')}"
        new_title = st.text_input("New RFP session title", value=default_title, key="rfp_new_title")
        if st.button("Start RFP session"):
            conn.execute("insert into rfp_sessions(title) values(?)", (new_title,))
            conn.commit(); st.rerun()
        st.stop()
    rfp_session_id = int(pick.split(":")[0])
    cur_title_row = pd.read_sql_query("select title from rfp_sessions where id=?", conn, params=(rfp_session_id,))
    cur_title = cur_title_row.iloc[0]["title"] if not cur_title_row.empty else "(untitled)"
    st.caption(f"RFP Session #{rfp_session_id} — {cur_title}")
    row = pd.read_sql_query("select draft_text from rfp_sessions where id=?", conn, params=(rfp_session_id,))
    draft_text = (row.iloc[0]["draft_text"] if not row.empty else "") or ""
    draft = st.text_area("Optional: paste your proposal draft to grade and correct", height=220, value=draft_text, key=f"rfp_draft_{rfp_session_id}")
    col_d1, col_d2 = st.columns(2)
    with col_d1:
        if st.button("Save draft to session"):
            conn.execute("update rfp_sessions set draft_text=? where id=?", (draft, rfp_session_id)); conn.commit(); st.success("Draft saved.")
    with col_d2:
        if st.button("Clear draft"):
            conn.execute("update rfp_sessions set draft_text='' where id=?", (rfp_session_id,)); conn.commit(); st.success("Draft cleared."); st.rerun()
    st.markdown("**Attach RFP documents (PDF/DOCX/TXT)**")
    uploads = st.file_uploader("Drop files", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key=f"rfp_uploads_{rfp_session_id}")
    if uploads and st.button("Add files to this RFP"):
        added = 0
        for up in uploads:
            text_content = read_doc(up)[:800_000]
            conn.execute("""insert into rfp_files(session_id, filename, mimetype, content_text)
                            values(?,?,?,?)""", (rfp_session_id, up.name, getattr(up, "type", ""), text_content))
            added += 1
        conn.commit(); st.success(f"Added {added} file(s) to this RFP session."); st.rerun()
    files_df = pd.read_sql_query(
        "select id, filename, length(content_text) as chars, uploaded_at from rfp_files where session_id=? order by id desc",
        conn, params=(rfp_session_id,)
    )
    if files_df.empty:
        st.caption("No RFP files attached yet.")
    else:
        st.caption("Attached RFP files:")
        st.dataframe(files_df.rename(columns={"chars":"chars_of_text"}), use_container_width=True)
        del_id = st.number_input("Delete attachment by ID", min_value=0, step=1, value=0, key=f"rfp_del_id_{rfp_session_id}")
        if st.button("Delete selected RFP file") and del_id > 0:
            conn.execute("delete from rfp_files where id=?", (int(del_id),)); conn.commit(); st.success(f"Deleted file id {del_id}."); st.rerun()
    hist = pd.read_sql_query("select role, content, created_at from rfp_messages where session_id=? order by id asc", conn, params=(rfp_session_id,))
    if hist.empty:
        st.info("No messages yet. Ask your first question below.")
    else:
        for _, rowm in hist.iterrows():
            if rowm["role"] == "user": st.chat_message("user").markdown(rowm["content"])
            elif rowm["role"] == "assistant": st.chat_message("assistant").markdown(rowm["content"])
            else: st.caption(f"🧠 System updated at {rowm['created_at']}")
    prompt = st.chat_input("Ask about compliance, evaluation, outline, risks, or request a grade of your draft")
    if prompt:
        conn.execute("insert into rfp_messages(session_id, role, content) values(?,?,?)", (rfp_session_id, "user", prompt)); conn.commit(); audit("rfp_message", "rfp_sessions", str(rfp_session_id), "user")
        rows = pd.read_sql_query("select filename, content_text from rfp_files where session_id=? and ifnull(content_text,'')<>''", conn, params=(rfp_session_id,))
        doc_snips = ""
        if not rows.empty:
            chunks, labels = [], []
            for _, r in rows.iterrows():
                cs = chunk_text(r["content_text"], max_chars=1200, overlap=200)
                chunks.extend(cs); labels.extend([r["filename"]]*len(cs))
            vec, X = embed_texts(chunks)
            top = search_chunks(prompt + " compliance evaluation factors submission instructions deliverables schedule scoring", vec, X, chunks, k=min(10, len(chunks)))
            parts, used = [], set()
            for sn in top:
                idx = chunks.index(sn) if sn in chunks else -1
                fname = labels[idx] if 0 <= idx < len(labels) else "attachment"
                key = (fname, sn[:60])
                if key in used: continue
                used.add(key)
                parts.append(f"\\n--- {fname} ---\\n{sn.strip()}\\n")
            if parts: doc_snips = "Attached RFP document snippets (most relevant first):\\n" + "\\n".join(parts[:16])
        grade_block = ""
        draft_row = pd.read_sql_query("select draft_text from rfp_sessions where id=?", conn, params=(rfp_session_id,))
        draft_text2 = (draft_row.iloc[0]["draft_text"] if not draft_row.empty else "") or ""
        if draft_text2:
            grade_block = f"""
You also have the vendor draft below. Act like a contracting officer. Derive a scoring rubric from evaluation factors and Section M if present. Score on a 0 to 5 scale per factor and provide strengths, weaknesses, risks, and specific corrections. Then produce 'Targeted Improvements by Section' with concrete rewrites mapped to section IDs. Quote section IDs and page numbers when visible.
Vendor draft
{draft_text2[:120000]}
"""
        system = "You are a federal proposal analyst and government evaluator. Be direct. Use bullets and cite section IDs and page numbers when visible."
        sys_blocks = [doc_snips] if doc_snips else []
        final_prompt = "\\n\\n".join([b for b in sys_blocks if b]) + f"\\n\\nUser request\\n{prompt}\\n\\n{grade_block}"
        assistant_out = llm(system, final_prompt, max_tokens=1800)
        conn.execute("insert into rfp_messages(session_id, role, content) values(?,?,?)", (rfp_session_id, "assistant", assistant_out)); conn.commit(); audit("rfp_message", "rfp_sessions", str(rfp_session_id), "assistant")
        st.chat_message("assistant").markdown(assistant_out)

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
        st.markdown(llm(system, prompt, max_tokens=2000))

with tabs[7]:
    st.subheader('Capability statement builder')
    c1, c2 = st.columns([2,1])
    with c1:
        company_name = st.text_input("Company Name", value=get_setting("company_name","ELA Management LLC"))
        cage = st.text_input("CAGE Code", value=get_setting("company_cage", COMPANY_CAGE))
        uei = st.text_input("UEI", value=get_setting("company_uei", COMPANY_UEI))
        duns = st.text_input("DUNS", value=get_setting("company_duns", COMPANY_DUNS))
        email = st.text_input("Business Email", value=get_setting("company_email", COMPANY_EMAIL))
        website = st.text_input("Website", value=get_setting("company_website", "https://"))
    with c2:
        logo_file = st.file_uploader("Logo (optional)", type=["png","jpg","jpeg"])

    core_comp = st.text_area("Core Competencies (one per line)", height=140,
                             value=get_setting("cap_core","• Facility maintenance\n• Janitorial services\n• Grounds & snow"))
    diffs     = st.text_area("Differentiators (one per line)", height=140,
                             value=get_setting("cap_diff","• Rapid mobilization\n• Cleared staff\n• Strong past performance"))
    past_perf = st.text_area("Past Performance (short bullets)", height=140,
                             value=get_setting("cap_past","• Agency X — $1.2M, CPARS Very Good"))
    naics_list = st.text_input("NAICS (comma-separated)", value=get_setting("cap_naics","561720, 561730, 561210"))
    psc_list   = st.text_input("PSC (comma-separated)", value=get_setting("cap_psc","S201, S208"))

    colA, colB = st.columns(2)
    with colA:
        if st.button("Export PDF"):
            try:
                from reportlab.lib.pagesizes import LETTER
                from reportlab.pdfgen import canvas
                from reportlab.lib.units import inch
                out = "/mnt/data/capability_statement.pdf"
                c = canvas.Canvas(out, pagesize=LETTER)
                w, h = LETTER; y = h - 1*inch
                c.setFont("Helvetica-Bold", 16); c.drawString(1*inch, y, company_name); y -= 18
                c.setFont("Helvetica", 10); c.drawString(1*inch, y, f"CAGE {cage} • UEI {uei} • DUNS {duns} • {email} • {website}"); y -= 24
                def block(title, txt):
                    nonlocal y
                    c.setFont("Helvetica-Bold", 12); c.drawString(1*inch, y, title); y -= 14
                    c.setFont("Helvetica", 10)
                    for line in txt.splitlines():
                        c.drawString(1.1*inch, y, line); y -= 12
                        if y < 0.9*inch: c.showPage(); y = h - 1*inch
                    y -= 6
                block("Core Competencies", core_comp); block("Differentiators", diffs)
                block("Past Performance", past_perf); block("Codes", f"NAICS: {naics_list} | PSC: {psc_list}")
                c.save()
                st.download_button("Download Capability Statement (PDF)", data=open(out,"rb").read(),
                                   file_name="Capability_Statement.pdf")
            except Exception as e:
                st.warning(f"PDF export failed ({e}); try DOCX.")
    with colB:
        if st.button("Export DOCX"):
            import docx as docxlib
            out = "/mnt/data/capability_statement.docx"
            d = docxlib.Document()
            d.add_heading(company_name, 0)
            d.add_paragraph(f"CAGE {cage} • UEI {uei} • DUNS {duns} • {email} • {website}")
            for title, txt in [("Core Competencies", core_comp), ("Differentiators", diffs),
                               ("Past Performance", past_perf), ("Codes", f"NAICS: {naics_list} | PSC: {psc_list}")]:
                d.add_heading(title, level=2)
                for line in txt.splitlines():
                    d.add_paragraph(line, style="List Bullet")
            d.save(out)
            st.download_button("Download Capability Statement (DOCX)", data=open(out,"rb").read(),
                               file_name="Capability_Statement.docx")

with tabs[8]:
    st.subheader('White paper builder')
    title = st.text_input("Paper Title", value=get_setting("wp_title","Optimizing Facilities O&M with Predictive Maintenance"))
    audience = st.text_input("Audience", value=get_setting("wp_aud","Federal facility managers"))
    abstract = st.text_area("Abstract", height=140, value=get_setting("wp_abs","This paper outlines..."))
    sections = st.text_area("Sections (Markdown-ish; use # and ## for headings)", height=280,
                            value=get_setting("wp_sections","# Introduction\n...\n## Approach\n...\n## Results\n...\n# Conclusion\n..."))

    if st.button("Export DOCX"):
        import docx as docxlib
        out = "/mnt/data/white_paper.docx"
        d = docxlib.Document()
        d.add_heading(title, 0)
        d.add_paragraph(f"Audience: {audience}")
        d.add_heading("Abstract", level=1)
        d.add_paragraph(abstract)
        for line in sections.splitlines():
            if line.startswith("## "):
                d.add_heading(line[3:], level=2)
            elif line.startswith("# "):
                d.add_heading(line[2:], level=1)
            else:
                d.add_paragraph(line)
        d.save(out)
        st.download_button("Download White Paper (DOCX)", data=open(out,"rb").read(), file_name="White_Paper.docx")

with tabs[10]:
    st.subheader('Data Export')
    st.caption("Export tables to Excel.")
    conn = get_db()
    tables = {
        "opportunities": "select * from opportunities",
        "vendors": "select * from vendors",
        "contacts": "select * from contacts"
    }
    pick = st.multiselect("Pick tables to export", options=list(tables.keys()),
                          default=["opportunities","vendors"])

    def to_xlsx_bytes(dfs: dict) -> bytes:
        import io
        from pandas import ExcelWriter
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as xw:
            for name, df in dfs.items():
                df.to_excel(xw, index=False, sheet_name=(name[:31] or "sheet"))
        return buf.getvalue()

    if st.button("Export selected to Excel"):
        data = {name: pd.read_sql_query(sql, conn) for name, sql in tables.items() if name in pick}
        if not data:
            st.info("Select at least one table.")
        else:
            x = to_xlsx_bytes(data)
            st.download_button("Download Excel", data=x, file_name="ela_export.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

with tabs[9]:
    st.subheader('Auto extract key details')
    st.caption("Upload RFP files; we’ll text-extract and give you a quick preview + CSV.")
    files = st.file_uploader("Upload files", type=["pdf","docx","txt"], accept_multiple_files=True)
    if files:
        rows = []
        for f in files:
            name = f.name.lower()
            try:
                if name.endswith(".pdf"):
                    from PyPDF2 import PdfReader
                    r = PdfReader(f); text = "\n".join(p.extract_text() or "" for p in r.pages)
                elif name.endswith(".docx"):
                    import docx as docxlib
                    d = docxlib.Document(f); text = "\n".join(p.text for p in d.paragraphs)
                else:
                    text = f.read().decode("utf-8", errors="ignore")
            except Exception:
                text = f"[[Could not parse {f.name}]]"
            rows.append({"file": f.name, "chars": len(text or ""), "preview": (text or "")[:1500]})
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True)
        st.download_button("Download CSV", data=df.to_csv(index=False).encode("utf-8"),
                           file_name="auto_extract.csv")
    else:
        st.info("Upload at least one file.")

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
    st.subheader('Chat Assistant (remembers context; accepts file uploads)')
    st.caption("This is a simple local chat stub. We can later connect it to your preferred LLM.")
    if "chat_history" not in st.session_state:
        st.session_state["chat_history"] = []
    uploads = st.file_uploader("Optional: attach files (txt/pdf/docx)", type=["txt","pdf","docx"], accept_multiple_files=True)
    if uploads:
        st.caption(f"Attached: {', '.join([u.name for u in uploads])}")
    prompt = st.chat_input("Type your message")
    if prompt:
        st.session_state["chat_history"].append({"role":"user","content":prompt})
        st.session_state["chat_history"].append({"role":"assistant","content":"(Assistant) " + prompt})
    for m in st.session_state["chat_history"]:
        with st.chat_message(m["role"]):
            st.write(m["content"])
