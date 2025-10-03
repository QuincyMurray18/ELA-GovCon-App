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
# === OCR and clause risk helpers (injected) ===
try:
    import pytesseract  # optional
    from pdf2image import convert_from_bytes
except Exception:
    pytesseract = None
    convert_from_bytes = None

CLAUSE_RISKS = {
    "liquidated damages": "May require payments for delays. Propose realistic schedule and mitigation plan.",
    "termination for convenience": "Government can end the contract at any time. Manage inventory and subcontracts carefully.",
    "termination for default": "Strict performance risk. Include QA steps and corrective action plan.",
    "excessive bonding": "High bonding can strain cash flow. Ask if alternatives are allowed.",
    "unusual penalties": "Flag for legal review. Request clarification if ambiguous.",
    "indemnification": "Risk transfer to contractor. Verify insurance coverage.",
    "personal services": "May conflict with FAR rules if not intended. Confirm classification.",
    "pay when paid": "Cash flow risk for subs. Negotiate fair terms.",
    "liability cap absent": "Unlimited liability. Seek cap or clarify scope.",
}
def _find_clause_risks(text: str, top_k: int = 6):
    text_l = (text or "").lower()
    hits = []
    for key, hint in CLAUSE_RISKS.items():
        if key in text_l:
            hits.append({"clause": key, "hint": hint})
    return hits[:top_k]

def _ocr_pdf_bytes(pdf_bytes: bytes) -> str:
    if not (pytesseract and convert_from_bytes):
        return ""
    try:
        pages = convert_from_bytes(pdf_bytes, dpi=200)
        out = []
        for img in pages[:30]:
            out.append(pytesseract.image_to_string(img))
        return "\n".join(out)
    except Exception:
        return ""


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
    st.warning("OpenAI SDK missing or too old. Chat features disabled until installed.")
    OpenAI = None

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
OPENAI_MODEL = os.getenv("OPENAI_MODEL", _get_key("OPENAI_MODEL") or "gpt-5-chat-latest")
_OPENAI_FALLBACK_MODELS = [
    OPENAI_MODEL,
    "gpt-5-chat-latest","gpt-5","gpt-5-2025-08-07",
    "gpt-5-mini","gpt-5-mini-2025-08-07",
    "gpt-5-nano","gpt-5-nano-2025-08-07",
    "gpt-4o-mini","gpt-4o",
]


def _send_via_smtp_host(to_addr: str, subject: str, body: str, from_addr: str,
                        smtp_server: str, smtp_port: int, smtp_user: str, smtp_pass: str,
                        reply_to: str | None = None) -> None:
    """Top level SMTP sender. Keeps email helpers available across the app."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg['Subject'] = subject
    if reply_to:
        msg['Reply-To'] = reply_to
    msg.attach(MIMEText(body, 'plain'))
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(from_addr, [to_addr], msg.as_string())


def _send_via_gmail(to_addr: str, subject: str, body: str) -> str:
    """
    Gmail sender using Streamlit secrets.
    Falls back to Microsoft Graph if Gmail is not configured.
    Returns "Sent" or "Preview" string to avoid crashes.
    """
    try:
        smtp_user = st.secrets.get("smtp_user")
        smtp_pass = st.secrets.get("smtp_pass")
    except Exception:
        smtp_user = smtp_pass = None

    if smtp_user and smtp_pass:
        from_addr = st.secrets.get("smtp_from", smtp_user) if hasattr(st, "secrets") else smtp_user
        reply_to = st.secrets.get("smtp_reply_to", None) if hasattr(st, "secrets") else None
        try:
            _send_via_smtp_host(to_addr, subject, body, from_addr, "smtp.gmail.com", 587, smtp_user, smtp_pass, reply_to)
            return "Sent"
        except Exception as e:
            try:
                st.warning(f"Gmail SMTP send failed: {e}")
            except Exception:
                pass
    # Fallback to Graph or preview
    try:
        sender_upn = get_setting("ms_sender_upn", "")
    except Exception:
        sender_upn = ""
    try:
        res = send_via_graph(to_addr, subject, body, sender_upn=sender_upn)
        return res if isinstance(res, str) else "Sent"
    except Exception:
        try:
            import streamlit as _st
            _st.warning("Email preview mode is active. Configure SMTP or Graph to send.")
        except Exception:
            pass
        return "Preview"

st.set_page_config(page_title="GovCon Copilot Pro", page_icon="ðŸ§°", layout="wide")

# ---- Date helpers for SAM search ----

# ---- SAM date parsing helper ----
try:
    _ = _parse_sam_date
except NameError:
    from datetime import datetime
    def _parse_sam_date(s):
        """Parse common SAM.gov date/time strings into datetime; return original on failure."""
        if s is None:
            return None
        if isinstance(s, datetime):
            return s
        txt = str(s).strip()
        # Try a few common SAM formats
        fmts = [
            "%m/%d/%Y %I:%M %p %Z",   # 09/30/2025 02:00 PM ET
            "%m/%d/%Y %H:%M %Z",      # 09/30/2025 14:00 ET
            "%m/%d/%Y %I:%M %p",      # 09/30/2025 02:00 PM
            "%m/%d/%Y %H:%M",         # 09/30/2025 14:00
            "%m/%d/%Y",               # 09/30/2025
            "%Y-%m-%dT%H:%M:%SZ",     # 2025-09-30T18:00:00Z
            "%Y-%m-%d"                # 2025-09-30
        ]
        for f in fmts:
            try:
                return datetime.strptime(txt, f)
            except Exception:
                pass
        return txt
try:
    _ = _us_date
except NameError:
    from datetime import datetime
    def _us_date(dt):
        try:
            return dt.strftime("%m/%d/%Y")
        except Exception:
            # If dt is a string or not a datetime, return as-is
            return str(dt)


# ---- Hoisted SAM helper (duplicate for early use) ----

# ---- Datetime coercion helper for SAM Watch (inline before sam_search) ----
from datetime import datetime

def send_via_graph(to_addr: str, subject: str, body: str, sender_upn: str = None) -> str:
    """
    Send mail using Microsoft Graph with application permissions (client credentials).
    Uses /users/{sender}/sendMail. Returns "Sent" on success or a short diagnostic string on error.
    Env/settings used:
      - MS_TENANT_ID, MS_CLIENT_ID, MS_CLIENT_SECRET
      - MS_SENDER_UPN or settings key ms_sender_upn
    """
    try:
        import os, requests
        from urllib.parse import quote_plus
    except Exception as _e_imp:
        return f"Graph send error: missing dependency ({_e_imp})"

    # Load config: prefer env, then settings table if available
    try:
        sender = sender_upn or os.getenv("MS_SENDER_UPN") or get_setting("ms_sender_upn", "")
    except Exception:
        sender = sender_upn or os.getenv("MS_SENDER_UPN") or ""

    # MS_* may already be loaded at module level; fall back to env/settings if empty
    try:
        _tenant = os.getenv("MS_TENANT_ID") or get_setting("MS_TENANT_ID", "") or get_setting("ms_tenant_id", "")
    except Exception:
        _tenant = os.getenv("MS_TENANT_ID") or ""
    try:
        _client_id = os.getenv("MS_CLIENT_ID") or get_setting("MS_CLIENT_ID", "") or get_setting("ms_client_id", "")
    except Exception:
        _client_id = os.getenv("MS_CLIENT_ID") or ""
    try:
        _client_secret = os.getenv("MS_CLIENT_SECRET") or get_setting("MS_CLIENT_SECRET", "") or get_setting("ms_client_secret", "")
    except Exception:
        _client_secret = os.getenv("MS_CLIENT_SECRET") or ""

    if not to_addr:
        return "Missing recipient email"
    if not (_tenant and _client_id and _client_secret):
        return "Graph not configured. Set MS_TENANT_ID, MS_CLIENT_ID, MS_CLIENT_SECRET"
    if not sender:
        return "Missing sender mailbox. Set MS_SENDER_UPN or settings key ms_sender_upn"

    # Acquire app-only token
    try:
        token_r = requests.post(
            f"https://login.microsoftonline.com/{_tenant}/oauth2/v2.0/token",
            data={
                "client_id": _client_id,
                "client_secret": _client_secret,
                "scope": "https://graph.microsoft.com/.default",
                "grant_type": "client_credentials",
            },
            timeout=20,
        )
    except Exception as e:
        return f"Graph token exception: {e}"

    if token_r.status_code != 200:
        return f"Graph token error {token_r.status_code}: {token_r.text[:300]}"
    try:
        token = token_r.json().get("access_token")
    except Exception:
        token = None
    if not token:
        return f"Graph token error: {token_r.text[:300]}"

    # Build payload
    payload = {
        "message": {
            "subject": subject or "",
            "body": {"contentType": "Text", "content": body or ""},
            "toRecipients": [{"emailAddress": {"address": to_addr}}],
            "from": {"emailAddress": {"address": sender}},
        },
        "saveToSentItems": True,  # boolean must be used
    }

    send_url = f"https://graph.microsoft.com/v1.0/users/{quote_plus(sender)}/sendMail"
    try:
        r = requests.post(
            send_url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
    except Exception as e:
        return f"Graph send exception: {e}"

    if r.status_code in (200, 202):
        return "Sent"

    # surface helpful diagnostics
    try:
        err_json = r.json()
        err_txt = str(err_json)[:500]
    except Exception:
        err_txt = (r.text or "")[:500]
    return f"Graph send error {r.status_code}: {err_txt}"




# === Market pricing data helpers (robust) ===
def usaspending_search_awards(naics: str = "", psc: str = "", date_from: str = "", date_to: str = "", keyword: str = "", limit: int = 200, st_debug=None):
    import requests, pandas as pd, json
    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    type_codes = ["A","B","C","D"]
    def make_filters(n, p, k, start, end):
        f = {"time_period": [{"start_date": start, "end_date": end}], "award_type_codes": type_codes, "prime_or_sub": "prime_only"}
        if n: f["naics_codes"] = [n]
        if p: f["psc_codes"] = [p]
        if k: f["keywords"] = [k]
        return f
    if not date_from or not date_to:
        from datetime import datetime, timedelta
        end = datetime.utcnow().date().strftime("%Y-%m-%d")
        start = (datetime.utcnow().date() - timedelta(days=365*2)).strftime("%Y-%m-%d")
        date_from, date_to = date_from or start, date_to or end
    attempts = [("full", make_filters(naics, psc, keyword, date_from, date_to)),
                ("no_psc", make_filters(naics, "", keyword, date_from, date_to)),
                ("no_naics", make_filters("", psc, keyword, date_from, date_to)),
                ("keyword_only", make_filters("", "", keyword or "", date_from, date_to)),
                ("bare", make_filters("", "", "", date_from, date_to))]
    last_detail = ""
    for name, flt in attempts:
        payload = {"filters": flt, "fields": ["Award ID","Recipient Name","Start Date","End Date","Award Amount","Awarding Agency","NAICS Code","PSC Code"],
                   "page": 1, "limit": max(1, min(int(limit), 500)), "sort": "Award Amount", "order": "desc"}
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=30)
            status = r.status_code
            js = r.json() if status < 500 else {}
            rows = js.get("results", []) or []
            if rows:
                data = [{"award_id": it.get("Award ID"),
                         "recipient": it.get("Recipient Name"),
                         "start": it.get("Start Date"),
                         "end": it.get("End Date"),
                         "amount": it.get("Award Amount"),
                         "agency": it.get("Awarding Agency"),
                         "naics": it.get("NAICS Code"),
                         "psc": it.get("PSC Code")} for it in rows]
                diag = f"Attempt {name}: HTTP {status}, rows={len(rows)}"
                if st_debug is not None:
                    st_debug.code(json.dumps(payload, indent=2))
                    st_debug.caption(diag)
                return pd.DataFrame(data), diag
            else:
                last_detail = f"Attempt {name}: HTTP {status}, empty; message: {js.get('detail') or js.get('messages') or ''}"
        except Exception as e:
            last_detail = f"Attempt {name}: exception {e}"
    if st_debug is not None:
        st_debug.caption(last_detail)
    return pd.DataFrame(), last_detail

def summarize_award_prices(df):
    import numpy as np, pandas as pd
    if df is None or df.empty or "amount" not in df.columns: return {}
    vals = pd.to_numeric(df["amount"], errors="coerce").dropna()
    if vals.empty: return {}
    return {"count": int(vals.size), "min": float(vals.min()), "p25": float(np.percentile(vals,25)),
            "median": float(np.percentile(vals,50)), "p75": float(np.percentile(vals,75)),
            "max": float(vals.max()), "mean": float(vals.mean())}

def gsa_calc_rates(query: str, page: int = 1):
    import requests, pandas as pd
    url = "https://api.gsa.gov/technology/calc/search"
    params = {"q": query, "page": page}
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        js = r.json()
        items = js.get("results", []) or []
        rows = [{"vendor": it.get("vendor_name"), "labor_category": it.get("labor_category"),
                 "education": it.get("education_level"), "min_years_exp": it.get("min_years_experience"),
                 "hourly_ceiling": it.get("current_price"), "schedule": it.get("schedule"), "sin": it.get("sin")} for it in items]
        return pd.DataFrame(rows)
    except Exception:
        import pandas as pd
        return pd.DataFrame()


def _coerce_dt(x):
    if isinstance(x, datetime):
        return x
    try:
        y = _parse_sam_date(x)
        return y if isinstance(y, datetime) else None
    except Exception:
        return None

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
            d_dt = _coerce_dt(d)
            min_dt = _coerce_dt(min_due_date)
            if min_dt is None:
                due_ok = True  # allow when min date unknown
            else:
                due_ok = (d_dt is None) or (d_dt >= min_dt)
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



# ---- Hoisted helper implementations (duplicate for early use) ----
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

def linkedin_company_search(keyword: str) -> str:
    return f"https://www.linkedin.com/search/results/companies/?keywords={quote_plus(keyword)}"

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



# ---- Safety helpers (fallbacks to avoid NameError at first render) ----
try:
    _ = linkedin_company_search
except NameError:
    def linkedin_company_search(q: str) -> str:
        return f"https://www.linkedin.com/search/results/companies/?keywords={quote_plus(q)}"


try:
    _ = google_places_search
except NameError:
    def google_places_search(*args, **kwargs):
        """
        Fallback stub when real google_places_search isn't loaded yet.
        Accepts flexible signatures, e.g. (query, location, radius_meters).
        Returns (results, info) where results is a list and info is a dict.
        """
        try:
            query = args[0] if len(args) >= 1 else kwargs.get("query","")
            loc = args[1] if len(args) >= 2 else kwargs.get("location","")
            radius_m = args[2] if len(args) >= 3 else kwargs.get("radius_meters", 1609)
        except Exception:
            query, loc, radius_m = "", "", 1609
        url = f"https://www.google.com/maps/search/{quote_plus(str(query)+' '+str(loc))}"
        # Provide an empty result set and metadata so callers expecting tuple unpacking won't crash
        return [], {"url": url, "note": "Fallback google_places_search stub used", "radius_m": radius_m}

try:
    _ = build_context
except NameError:
    def build_context(max_rows: int = 6) -> str:
        return ""

st.title("GovCon Copilot Pro")
st.caption("SubK sourcing • SAM watcher • proposals • outreach • CRM • goals • chat with memory & file uploads")

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


SCHEMA.update({
    "proposal_drafts": """
    create table if not exists proposal_drafts (
        id integer primary key,
        session_id integer,
        section text,
        content text,
        updated_at text default current_timestamp,
        foreign key(session_id) references rfp_sessions(id)
    );
    """
})

# === Added schema for new features ===
SCHEMA.update({
    "deadlines": """
    create table if not exists deadlines (
        id integer primary key,
        opp_id integer,
        title text,
        due_date text,
        source text,
        status text default 'Open',
        notes text,
        created_at text default current_timestamp
    );
    """,
    "compliance_items": """
    create table if not exists compliance_items (
        id integer primary key,
        opp_id integer,
        item text,
        required integer default 1,
        status text default 'Pending',
        source_page text,
        notes text,
        created_at text default current_timestamp
    );
    """,
    "rfq_outbox": """
    create table if not exists rfq_outbox (
        id integer primary key,
        vendor_id integer,
        company text,
        to_email text,
        subject text,
        body text,
        due_date text,
        files_json text,
        sent_at text,
        status text default 'Draft',
        created_at text default current_timestamp
    );
    """,
    "pricing_scenarios": """
    create table if not exists pricing_scenarios (
        id integer primary key,
        opp_id integer,
        base_cost real,
        overhead_pct real,
        gna_pct real,
        profit_pct real,
        total_price real,
        lpta_note text,
        created_at text default current_timestamp
    );
    """
})



def parse_pick_id(pick):
    try:
        return int(str(pick).split(":")[0])
    except Exception:
        return None

def get_db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def run_migrations():
    conn = get_db()
    cur = conn.cursor()
    # opportunities table expansions

    try: cur.execute("alter table compliance_items add column owner text")
    except Exception: pass
    try: cur.execute("alter table compliance_items add column snippet text")
    except Exception: pass
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

run_migrations()
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
        d = docx.Document(uploaded_file)
        return "\n".join(p.text for p in d.paragraphs)
    if suffix == "pdf":
        try:
            data = uploaded_file.read()
            r = PdfReader(io.BytesIO(data))
            txt = "\n".join((p.extract_text() or "") for p in r.pages)
            # OCR fallback when native text is sparse
            if len((txt or "").strip()) < 500:
                ocr_txt = _ocr_pdf_bytes(data)
                if ocr_txt and len(ocr_txt.strip()) > len((txt or "").strip()):
                    return ocr_txt
            return txt
        except Exception:
            try:
                data = uploaded_file.read()
                ocr_txt = _ocr_pdf_bytes(data)
                if ocr_txt:
                    return ocr_txt
            except Exception:
                pass
            return ""
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


def _validate_text_for_guardrails(md_text: str, page_limit: int = None, require_font: str = None, require_size_pt: int = None,
                                  margins_in: float = None, line_spacing: float = None, filename_pattern: str = None):
    issues = []
    warnings = []
    body = (md_text or "").strip()

    # Basic content checks
    if not body:
        issues.append("No content assembled for export.")
        return issues, 0

    # Placeholder checks
    placeholders = ["TBD", "INSERT", "[BRACKET]", "{PLACEHOLDER}", "lorem ipsum"]
    for ph in placeholders:
        if ph.lower() in body.lower():
            issues.append(f"Placeholder text '{ph}' detected. Remove before export.")

    # Word/page estimate
    words = body.split()
    est_pages = max(1, int(len(words) / 500))  # heuristic ~500 words/page
    if page_limit and est_pages > page_limit:
        issues.append(f"Estimated pages {est_pages} exceed limit {page_limit}.")

    # Font/size are enforced during DOCX build; here we flag if requested but not standard
    if require_font and require_font.lower() not in ("times new roman","arial","calibri","garamond","helvetica"):
        warnings.append(f"Requested font '{require_font}' is uncommon for federal proposals.")

    if require_size_pt and (require_size_pt < 10 or require_size_pt > 13):
        warnings.append(f"Requested font size {require_size_pt}pt is atypical for body text.")

    # Margins/spacing advisory
    if margins_in is not None and (margins_in < 0.5 or margins_in > 1.5):
        warnings.append(f"Margin {margins_in}\" may violate standard 1\" requirement.")

    if line_spacing is not None and (line_spacing < 1.0 or line_spacing > 2.0):
        warnings.append(f"Line spacing {line_spacing} looks unusual.")

    # Filename pattern
    if filename_pattern:
        # Very simple validation tokens
        tokens = ["{company}", "{solicitation}", "{section}", "{date}"]
        if not any(t in filename_pattern for t in tokens):
            warnings.append("Filename pattern lacks tokens like {company} or {date}.")

    return issues, est_pages




def _proposal_context_for(conn, session_id: int, question_text: str):
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
    top = search_chunks(question_text, vec, X, chunks, k=min(10, len(chunks)))
    parts, used = [], set()
    for sn in top:
        try:
            idx = chunks.index(sn); fname = labels[idx]
        except Exception:
            fname = "attachment"
        key = (fname, sn[:60])
        if key in used: continue
        used.add(key)
        parts.append(f"\n--- {fname} ---\n{sn.strip()}\n")
    return "Attached RFP snippets (most relevant first):\n" + "\n".join(parts[:16]) if parts else ""




# Injected early definition of vendor manager to avoid NameError
def _render_saved_vendors_manager(_container=None):
    import pandas as pd
    _c = _container or st
    _c.markdown("### Saved vendors")
    try:
        conn = get_db()
    except Exception as e:
        _c.error(f"DB error: {e}")
        return
    try:
        _v = pd.read_sql_query("select * from vendors order by updated_at desc, company", conn)
    except Exception as e:
        _c.warning("Vendors table missing. Creating it now...")
        try:
            cur = conn.cursor()
            cur.execute("""
            create table if not exists vendors(
                id integer primary key autoincrement,
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
                created_at timestamp default current_timestamp,
                updated_at timestamp default current_timestamp
            );
            """)
            conn.commit()
            _v = pd.read_sql_query("select * from vendors order by updated_at desc, company", conn)
        except Exception as ce:
            _c.error(f"Could not create/read vendors table: {ce}")
            return

    if _v.empty:
        _c.info("No vendors saved yet. Use your import above or add one manually below.")
        # Show empty editor with columns for manual add
        _v = pd.DataFrame([{
            "id": None, "company":"", "naics":"", "trades":"",
            "phone":"", "email":"", "website":"", "city":"", "state":"",
            "certifications":"", "set_asides":"", "notes":""
        }])
    else:
        _v = _v.copy()

    # Build a clickable link column
    def _mk(u):
        u = "" if u is None else str(u).strip()
        if not u:
            return ""
        if not (u.startswith("http://") or u.startswith("https://")):
            return "http://" + u
        return u

    _v["Link"] = _v.get("website", "").apply(_mk)

    editor = _c.data_editor(
        _v[[
            "id","company","naics","trades","phone","email","website","city","state",
            "certifications","set_asides","notes","Link"
        ]],
        column_config={
            "Link": st.column_config.LinkColumn("Link", display_text="Open"),
        },
        use_container_width=True,
        num_rows="dynamic",
        key="vendors_grid_tab1"
    )

    c1, c2, c3 = _c.columns([1,1,2])
    with c1:
        if _c.button("Save changes", key="vendors_save_btn_tab1"):
            try:
                cur = conn.cursor()
                try:
                    editor = editor.where(editor.notnull(), None)
                except Exception:
                    pass
                saved, updated = 0, 0
                for _, r in editor.iterrows():
                    vid = r.get("id")
                    vals = (
                        r.get("company","") or "",
                        r.get("naics","") or "",
                        r.get("trades","") or "",
                        r.get("phone","") or "",
                        r.get("email","") or "",
                        r.get("website","") or "",
                        r.get("city","") or "",
                        r.get("state","") or "",
                        r.get("certifications","") or "",
                        r.get("set_asides","") or "",
                        r.get("notes","") or "",
                    )
                    if vid is None or (isinstance(vid, float) and pd.isna(vid)) or str(vid).strip()=="" :
                        cur.execute("""insert into vendors(company,naics,trades,phone,email,website,city,state,certifications,set_asides,notes)
                                       values(?,?,?,?,?,?,?,?,?,?,?)""", vals)
                        saved += 1
                    else:
                        cur.execute("""update vendors
                                       set company=?, naics=?, trades=?, phone=?, email=?, website=?, city=?, state=?, certifications=?, set_asides=?, notes=?, updated_at=current_timestamp
                                       where id=?""", vals + (int(vid),))
                        updated += 1
                conn.commit()
                _c.success(f"Saved {saved} new, updated {updated} existing")
            except Exception as se:
                _c.error(f"Save failed: {se}")

    with c2:
        try:
            all_ids = [int(x) for x in editor.get("id", pd.Series(dtype=float)).dropna().astype(int).tolist()]
        except Exception:
            all_ids = []
        del_ids = _c.multiselect("Delete vendor IDs", options=all_ids, key="vendors_del_ids_tab1")
        if _c.button("Delete selected", key="vendors_del_btn_tab1"):
            try:
                if del_ids:
                    cur = conn.cursor()
                    for vid in del_ids:
                        cur.execute("delete from vendors where id=?", (int(vid),))
                    conn.commit()
                    _c.success(f"Deleted {len(del_ids)} vendor(s)")
            except Exception as de:
                _c.error(f"Delete failed: {de}")

    with c3:
        _c.caption("Tip: Add a new row at the bottom to create a vendor manually.")

TAB_LABELS = [
    "SAM Watch", "Pipeline", "RFP Analyzer", "L&M Checklist", "Past Performance", "RFQ Generator", "Subcontractor Finder", "Outreach", "Quote Comparison", "Pricing Calculator", "Win Probability", "Proposal Builder", "Ask the doc", "Chat Assistant", "Auto extract", "Capability Statement", "White Paper Builder", "Contacts", "Data Export", "Deadlines"
]
tabs = st.tabs(TAB_LABELS)
TAB = {label: i for i, label in enumerate(TAB_LABELS)}
# Backward-compatibility: keep legacy numeric indexing working
LEGACY_ORDER = [
    "Pipeline", "Subcontractor Finder", "Contacts", "Outreach", "SAM Watch", "RFP Analyzer", "Capability Statement", "White Paper Builder", "Data Export", "Auto extract", "Ask the doc", "Chat Assistant", "Proposal Builder", "Deadlines", "L&M Checklist", "RFQ Generator", "Pricing Calculator", "Past Performance", "Quote Comparison", "Win Probability"
]
legacy_tabs = [tabs[TAB[label]] for label in LEGACY_ORDER]
# === Begin injected: extra schema, helpers, and three tab bodies ===
def _ensure_extra_schema():
    try:
        conn = get_db()
    except Exception:
        return
    try:
        conn.execute("""create table if not exists past_performance (
            id integer primary key,
            title text, agency text, naics text, psc text,
            period text, value real, role text, location text,
            highlights text,
            contact_name text, contact_email text, contact_phone text,
            created_at text default current_timestamp,
            updated_at text default current_timestamp
        );""")
        conn.execute("""create table if not exists vendor_quotes (
            id integer primary key,
            opp_id integer, vendor_id integer, company text,
            subtotal real, taxes real, shipping real, total real,
            lead_time text, notes text, files_json text,
            created_at text default current_timestamp
        );""")
        conn.execute("""create table if not exists win_scores (
            id integer primary key,
            opp_id integer unique, score real, factors_json text,
            computed_at text default current_timestamp
        );""")
        conn.execute("""create table if not exists tasks (
            id integer primary key,
            opp_id integer, title text, assignee text, due_date text,
            status text default 'Open', notes text,
            created_at text default current_timestamp,
            updated_at text default current_timestamp
        );""")
        conn.commit()
    except Exception:
        pass

_ensure_extra_schema()

def get_past_performance_df():
    try:
        return pd.read_sql_query("select * from past_performance order by updated_at desc, id desc", get_db())
    except Exception:
        return pd.DataFrame()

def upsert_win_score(opp_id: int, score: float, factors: dict):
    try:
        conn = get_db()
        conn.execute("""            insert into win_scores(opp_id, score, factors_json, computed_at)
            values(?,?,?, current_timestamp)
            on conflict(opp_id) do update set
                score=excluded.score,
                factors_json=excluded.factors_json,
                computed_at=current_timestamp
        """, (int(opp_id), float(score), json.dumps(factors)))
        conn.commit()
    except Exception:
        pass

def compute_win_score_row(opp_row, past_perf_df):
    from datetime import datetime as _dt
    # Factors
    score = 0
    factors = {}
    # NAICS match signal
    opp_naics = (opp_row.get("naics") or "").split(",")[0].strip()
    has_pp_same_naics = not past_perf_df[past_perf_df.get("naics", pd.Series(dtype=str)).fillna("").str.contains(opp_naics, na=False)].empty if opp_naics else False
    factors["naics_match"] = 25 if has_pp_same_naics else 10
    score += factors["naics_match"]
    # Set-aside fit signal
    t = (opp_row.get("type") or "").lower()
    setaside_fit = 20 if ("small business" in t or "total small business" in t) else 10
    factors["set_aside_fit"] = setaside_fit
    score += setaside_fit
    # Agency familiarity
    opp_agency = (opp_row.get("agency") or "").strip().lower()
    has_pp_same_agency = not past_perf_df[past_perf_df.get("agency", pd.Series(dtype=str)).fillna("").str.lower().str.contains(opp_agency)].empty if opp_agency else False
    factors["agency_familiarity"] = 25 if has_pp_same_agency else 10
    score += factors["agency_familiarity"]
    # Time runway
    try:
        due = _parse_date_any(opp_row.get("response_due") or "")
    except Exception:
        due = None
    runway = (due - _dt.now()).days if due else 21
    runway_pts = 20 if runway >= 14 else (10 if runway >= 7 else 5)
    factors["time_runway"] = runway_pts
    score += runway_pts
    # Attachment presence for clarity
    has_docs = bool(opp_row.get("attachments_json"))
    factors["docs_avail"] = 10 if has_docs else 5
    score += factors["docs_avail"]
    # Cap 100
    score = min(100, score)
    return score, factors

# Past Performance tab body (assumes appended as last-3 tab)
try:
    with legacy_tabs[-3]:
        st.subheader("Past Performance Library")
        st.caption("Create reusable blurbs linked by NAICS and agency. Insert into Proposal Builder later.")
        conn = get_db()
        df_pp = get_past_performance_df()
        st.dataframe(df_pp, use_container_width=True)

        with st.form("pp_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                title = st.text_input("Project title")
                agency = st.text_input("Agency")
                naics = st.text_input("NAICS", value="")
                psc = st.text_input("PSC", value="")
                period = st.text_input("Period", value="")
            with col2:
                value_amt = st.number_input("Contract value", min_value=0.0, step=1000.0)
                role = st.text_input("Role", value="Prime")
                location = st.text_input("Location", value="")
                highlights = st.text_area("Highlights bullets", height=120, value="• Scope coverage\n• Key metrics\n• Outcomes")
            contact_name = st.text_input("POC name", value="")
            contact_email = st.text_input("POC email", value="")
            contact_phone = st.text_input("POC phone", value="")
            submit = st.form_submit_button("Save record")
        if submit:
            conn.execute("""insert into past_performance
                (title,agency,naics,psc,period,value,role,location,highlights,contact_name,contact_email,contact_phone)
                values(?,?,?,?,?,?,?,?,?,?,?,?)""",                (title,agency,naics,psc,period,float(value_amt),role,location,highlights,contact_name,contact_email,contact_phone))
            conn.commit()
            st.success("Saved")
            st.experimental_rerun()
except Exception as _e_pp:
    st.caption(f"[Past Performance tab init note: {_e_pp}]")

# Quote Comparison tab body (last-2)
try:
    with legacy_tabs[-2]:
        st.subheader("Subcontractor Quote Comparison")
        conn = get_db()
        df_opp = pd.read_sql_query("select id, title from opportunities order by posted desc", conn)
        df_vendors = pd.read_sql_query("select id, company from vendors order by company", conn)
        opp_opts = [""] + [f"{int(r.id)}: {r.title}" for _, r in df_opp.iterrows()]
        opp_pick = st.selectbox("Opportunity", options=opp_opts)
        if opp_pick:
            opp_id = int(opp_pick.split(":")[0])

            with st.form("qc_add"):
                cols = st.columns(2)
                with cols[0]:
                    v_opts = [""] + [f"{int(r.id)}: {r.company}" for _, r in df_vendors.iterrows()]
                    v_pick = st.selectbox("Vendor", options=v_opts)
                    subtotal = st.number_input("Subtotal", min_value=0.0, step=100.0, value=0.0)
                    taxes = st.number_input("Taxes", min_value=0.0, step=50.0, value=0.0)
                    shipping = st.number_input("Shipping", min_value=0.0, step=50.0, value=0.0)
                with cols[1]:
                    lead_time = st.text_input("Lead time", value="")
                    notes = st.text_area("Notes", height=120, value="")
                    files = st.text_input("Files list", value="")
                add_btn = st.form_submit_button("Save quote line")
            if add_btn and v_pick:
                vendor_id = int(v_pick.split(":")[0])
                company = df_vendors[df_vendors["id"]==vendor_id]["company"].iloc[0]
                total = float(subtotal) + float(taxes) + float(shipping)
                conn.execute("""insert into vendor_quotes(opp_id, vendor_id, company, subtotal, taxes, shipping, total, lead_time, notes, files_json)
                                values(?,?,?,?,?,?,?,?,?,?)""",                             (opp_id, vendor_id, company, float(subtotal), float(taxes), float(shipping), total, lead_time, notes,
                              json.dumps([s.strip() for s in files.split(",") if s.strip()])))
                conn.commit()
                st.success("Saved")

            dfq = pd.read_sql_query("select * from vendor_quotes where opp_id=? order by total asc", conn, params=(opp_id,))
            if dfq.empty:
                st.info("No quotes yet")
            else:
                st.dataframe(dfq[["company","subtotal","taxes","shipping","total","lead_time","notes"]], use_container_width=True)
                pick_winner = st.selectbox("Pick winner", options=[""] + dfq["company"].tolist())
                if pick_winner and st.button("Pick Winner"):
                    winner_row = dfq[dfq["company"]==pick_winner].head(1)
                    if not winner_row.empty:
                        st.session_state["pricing_base_cost"] = float(winner_row["total"].iloc[0])
                    st.success(f"Winner selected {pick_winner}. Open Pricing Calculator to model markup.")
except Exception as _e_qc:
    st.caption(f"[Quote Comparison tab init note: {_e_qc}]")


    st.markdown("### Vendor ranking (scorecards)")
    try:
        conn = get_db()
        # Responsiveness proxy: count outreach_log entries per vendor with "Sent" or "Preview"
        resp = pd.read_sql_query("""
            select v.id, v.company,
                   coalesce(sum(case when o.status like 'Sent%' then 1 else 0 end),0) as sent,
                   coalesce(sum(case when o.status like 'Preview%' then 1 else 0 end),0) as preview
            from vendors v left join outreach_log o on v.id = o.vendor_id
            group by v.id, v.company
        """, conn)
        vdf = pd.read_sql_query("select id, company, certifications, set_asides, coalesce(distance_miles, 0) as distance_miles from vendors", conn)
        merged = vdf.merge(resp, how="left", on=["id","company"]).fillna({"sent":0,"preview":0})
        # Simple scoring model
        def _score_row(r):
            score = 0
            # Responsiveness
            score += min(20, (int(r["sent"]) + int(r["preview"])) * 2)
            # Certifications present
            score += 20 if (r.get("certifications") or "").strip() else 10
            # Distance (closer is better)
            d = float(r.get("distance_miles") or 0)
            score += 20 if d == 0 else (15 if d <= 25 else (10 if d <= 100 else 5))
            # Set-asides
            score += 20 if (r.get("set_asides") or "").strip() else 10
            # Past performance proxy (existence in library)
            try:
                pp = pd.read_sql_query("select count(*) as cnt from past_performance where agency like ? or naics <> ''", conn, params=(f"%{get_setting('company_name','ELA')}%",))
                has_pp = int(pp.iloc[0]["cnt"]) > 0
            except Exception:
                has_pp = False
            score += 20 if has_pp else 10
            return min(100, score)

        merged["score"] = merged.apply(_score_row, axis=1)
        merged = merged.sort_values("score", ascending=False)
        st.dataframe(merged[["company","score","certifications","set_asides","distance_miles","sent","preview"]].head(25), use_container_width=True)
    except Exception as _e_vs:
        st.caption(f"[Vendor ranking note: {_e_vs}]")


# Win Probability tab body (last-1)
try:
    with legacy_tabs[-1]:
        st.subheader("Win Probability")
        conn = get_db()
        df_opp = pd.read_sql_query("select * from opportunities order by posted desc", conn)
        df_pp = get_past_performance_df()
        if df_opp.empty:
            st.info("No opportunities in pipeline")
        else:
            rows = []
            for _, r in df_opp.iterrows():
                s, f = compute_win_score_row(r, df_pp)
                rows.append({
                    "id": r.get("id"),
                    "title": r.get("title"),
                    "agency": r.get("agency"),
                    "naics": r.get("naics"),
                    "response_due": r.get("response_due"),
                    "score": s,
                    "factors": f
                })
                try:
                    upsert_win_score(int(r.get("id")), s, f)
                except Exception:
                    pass
            df_scores = pd.DataFrame(rows).sort_values("score", ascending=False)
            st.dataframe(df_scores[["id","title","agency","naics","response_due","score"]], use_container_width=True)
            pick = st.number_input("Opportunity ID for factor breakdown", min_value=0, step=1, value=0)
            if pick:
                row = next((x for x in rows if x["id"]==int(pick)), None)
                if row:
                    st.json(row["factors"])
except Exception as _e_win:
    st.caption(f"[Win Probability tab init note: {_e_win}]")
# === End injected ===

with legacy_tabs[0]:
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
        __ctx_pipeline = True
        st.success(f"Saved — updated {updated} row(s), deleted {deleted} row(s).")


# Analytics mini-dashboard (scoped to Pipeline tab)
with legacy_tabs[0]:

    # Analytics mini-dashboard
    try:
        conn = get_db()
        df_all = pd.read_sql_query("select status, count(*) as n from opportunities group by status", conn)
        if not df_all.empty:
            st.markdown("### Pipeline analytics")
            st.bar_chart(df_all.set_index("status"))
        # Forecast (probability-adjusted revenue) using win_scores if any
        try:
            dfw = pd.read_sql_query("""
                select o.id, o.title, o.agency, coalesce(w.score, 50) as score
                from opportunities o left join win_scores w on o.id = w.opp_id
            """, conn)
            if not dfw.empty:
                dfw["prob"] = dfw["score"]/100.0
                # No revenue field available, so treat prob as index only
                st.dataframe(dfw[["id","title","agency","score","prob"]])
        except Exception as _e_wa:
            st.caption(f"[Win score analytics note: {_e_wa}]")
    except Exception as _e_dash:
        st.caption(f"[Analytics dash note: {_e_dash}]")


with legacy_tabs[0]:

    if globals().get("__ctx_pipeline", False):


        st.markdown("### Tasks for selected opportunity")

        try:

            sel_id = int(st.number_input("Type an opportunity ID to manage tasks", min_value=0, step=1, value=0))

            if sel_id:

                df_tasks = pd.read_sql_query("select * from tasks where opp_id=? order by due_date asc nulls last, id desc", conn, params=(sel_id,))

                if df_tasks.empty:

                    df_tasks = pd.DataFrame(columns=["id","opp_id","title","assignee","due_date","status","notes"])

                grid_tasks = st.data_editor(df_tasks, use_container_width=True, num_rows="dynamic", key="tasks_grid")

                if st.button("Save tasks"):

                    cur = conn.cursor()

                    for _, r in grid_tasks.iterrows():

                        if pd.isna(r.get("id")):

                            cur.execute("insert into tasks(opp_id,title,assignee,due_date,status,notes) values(?,?,?,?,?,?)",

                                        (sel_id, r.get("title",""), r.get("assignee",""), r.get("due_date",""), r.get("status","Open"), r.get("notes","")))

                        else:

                            cur.execute("update tasks set title=?, assignee=?, due_date=?, status=?, notes=?, updated_at=current_timestamp where id=?",

                                        (r.get("title",""), r.get("assignee",""), r.get("due_date",""), r.get("status","Open"), r.get("notes",""), int(r.get("id"))))

                    conn.commit()

                    st.success("Tasks saved.")

        except Exception as _e_tasks:

            st.caption(f"[Tasks panel note: {_e_tasks}]")
with legacy_tabs[1]:
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
                # Include NAICS tag choice from the UI if present
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

                    # Dedup by website then by company+phone
                    vid = None
                    if website:
                        cur.execute("select id from vendors where website=?", (website,))
                        row = cur.fetchone()
                        if row: vid = row[0]
                    if not vid and company:
                        cur.execute("select id from vendors where company=? and ifnull(phone,'')=?", (company, phone))
                        row = cur.fetchone()
                        if row: vid = row[0]

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
        st.link_button("Open LinkedIn", f"https://www.linkedin.com/search/results/companies/?keywords={quote_plus(trade + ' ' + loc)}")

    with colC:
        st.markdown("Google search")
        st.link_button("Open Google", f"https://www.google.com/search?q={quote_plus(trade + ' ' + loc)}")

    st.divider()
    _render_saved_vendors_manager()  # show manager only inside Subcontractor Finder
with legacy_tabs[2]:



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

with legacy_tabs[3]:
    st.subheader("Outreach and mail merge")
    st.caption("Use default templates, personalize for distance, capability and past performance. Paste replies to track status.")

    conn = get_db()
    df_v = pd.read_sql_query("select * from vendors", conn)

    # Template manager
    t = pd.read_sql_query("select * from email_templates order by name", conn)
    names = t["name"].tolist()

    pick_t = st.selectbox("Choose template", options=(["➤ New template"] + names) if names else ["➤ New template"])

    if pick_t == "➤ New template":
        new_name = st.text_input("New template name")
        subj_default, body_default = "", ""
    else:
        new_name = pick_t
        tpl = pd.read_sql_query("select subject, body from email_templates where name=?", conn, params=(pick_t,))
        subj_default = tpl.iloc[0]["subject"] if not tpl.empty else ""
        body_default = tpl.iloc[0]["body"] if not tpl.empty else ""

    subj = st.text_input("Subject", value=subj_default)
    body = st.text_area("Body with placeholders {company} {scope} {due}", value=body_default, height=220)

    col_save, col_del = st.columns([1,1])
    with col_save:
        if st.button("Save template"):
            nm = (new_name or "").strip()
            if nm:
                conn.execute(
                    """insert into email_templates(name, subject, body) values(?,?,?)
                       on conflict(name) do update set subject=excluded.subject, body=excluded.body, updated_at=current_timestamp""",
                    (nm, subj, body)
                )
                conn.commit()
                st.success(f"Template '{nm}' saved")
                st.experimental_rerun()
            else:
                st.error("Please enter a template name")
    with col_del:
        if pick_t != "➤ New template" and st.button("Delete template"):
            conn.execute("delete from email_templates where name=?", (pick_t,))
            conn.commit()
            st.success(f"Template '{pick_t}' deleted")
            st.experimental_rerun()

    st.divider()

    # Mail merge controls
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
        st.subheader("Email drafts")
        for m in st.session_state["mail_bodies"]:
            st.text_area(f"To: {m['to']} | Subject: {m['subject']}", value=m["body"], height=180)

    # SMTP email sender helpers
    def _send_via_smtp_host(to_addr, subject, body, from_addr, smtp_server, smtp_port, smtp_user, smtp_pass, reply_to=None):
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        msg = MIMEMultipart()
        msg['From'] = from_addr
        msg['To'] = to_addr
        msg['Subject'] = subject
        if reply_to:
            msg['Reply-To'] = reply_to
        msg.attach(MIMEText(body, 'plain'))
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_addr, [to_addr], msg.as_string())

    def _send_via_gmail(to_addr, subject, body):
        # Requires st.secrets: smtp_user, smtp_pass, optional smtp_from, smtp_reply_to
        smtp_user = st.secrets.get("smtp_user")
        smtp_pass = st.secrets.get("smtp_pass")
        if not smtp_user or not smtp_pass:
            raise RuntimeError("Missing smtp_user/smtp_pass in Streamlit secrets")
        from_addr = st.secrets.get("smtp_from", smtp_user)
        _send_via_smtp_host(
            to_addr, subject, body, from_addr,
            "smtp.office365.com", 587, smtp_user, smtp_pass,
            st.secrets.get("smtp_reply_to")
        )

with legacy_tabs[4]:
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

# (moved) RFP Analyzer call will be added after definition
with legacy_tabs[6]:
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

with legacy_tabs[7]:
    st.subheader("White paper builder")
    title = st.text_input("Title", value="Improving Facility Readiness with Outcome based Service Contracts")
    thesis = st.text_area("Thesis", value="Outcome based service contracts reduce total cost and improve satisfaction when paired with clear SLAs and transparent data.")
    audience = st.text_input("Audience", value="Facility Managers • Contracting Officers • Program Managers")
    if st.button("Draft white paper"):
        system = "Write a two page white paper with executive summary, problem, approach, case vignette, and implementation steps. Use clear headings and tight language."
        prompt = f"Title {title}\nThesis {thesis}\nAudience {audience}"
        st.markdown(llm(system, prompt, max_tokens=1400))

with legacy_tabs[8]:
    st.subheader("Export to Excel workbook")
    conn = get_db()
    v = pd.read_sql_query("select * from vendors", conn)
    o = pd.read_sql_query("select * from opportunities", conn)
    c = pd.read_sql_query("select * from contacts", conn)
    bytes_xlsx = to_xlsx_bytes({"Vendors": v, "Opportunities": o, "Contacts": c})
    st.download_button("Download Excel workbook", data=bytes_xlsx, file_name="govcon_hub.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

with legacy_tabs[9]:
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

with legacy_tabs[10]:
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


with legacy_tabs[11]:
    st.subheader("Chat Assistant (remembers context; accepts file uploads)")
    conn = get_db()

    # Sessions
    sessions = pd.read_sql_query("select id, title, created_at from chat_sessions order by created_at desc", conn)
    session_titles = ["➤ New chat"] + [f"{r['id']}: {r['title'] or '(untitled)'}" for _, r in sessions.iterrows()]
    pick = st.selectbox("Session", options=session_titles, index=0)

    # Create new session
    if pick == "➤ New chat":
        default_title = f"Chat {datetime.now().strftime('%b %d %I:%M %p')}"
        new_title = st.text_input("New chat title", value=default_title)
        if st.button("Start chat"):
            conn.execute("insert into chat_sessions(title) values(?)", (new_title,))
            conn.commit()
            st.rerun()
        st.caption("Pick an existing chat from the dropdown above to continue.")
    else:
        # Parse session id
        session_id = parse_pick_id(pick)
        if session_id is None:
            st.info("Select a valid session to continue.")
        else:
            cur_title = sessions[sessions["id"] == session_id]["title"].iloc[0] if not sessions.empty else "(untitled)"
            st.caption(f"Session #{session_id} — {cur_title}")

            # File uploads for this chat session
            up_files = st.file_uploader("Attach files (PDF, DOCX, DOC, TXT)", type=["pdf","docx","doc","txt"],
                                        accept_multiple_files=True, key=f"chat_up_{session_id}")
            if up_files and st.button("Add files to this chat"):
                added = 0
                for up in up_files:
                    try:
                        text = read_doc(up)[:800_000]
                    except Exception:
                        text = ""
                    conn.execute(
                        "insert into chat_files(session_id, filename, mimetype, content_text) values(?,?,?,?)",
                        (session_id, up.name, getattr(up, "type", ""), text)
                    )
                    added += 1
                conn.commit()
                st.success(f"Added {added} file(s).")
                st.rerun()

            # Show existing attachments
            files_df = pd.read_sql_query(
                "select id, filename, length(content_text) as chars, uploaded_at from chat_files where session_id=? order by id desc",
                conn, params=(session_id,)
            )
            if not files_df.empty:
                st.caption("Attached files")
                st.dataframe(files_df.rename(columns={"chars":"chars_of_text"}), use_container_width=True)

            # Helper to pull doc snippets most relevant to the user's question
            def _chat_doc_snips(question_text: str) -> str:
                rows = pd.read_sql_query(
                    "select filename, content_text from chat_files where session_id=? and ifnull(content_text,'')<>''",
                    conn, params=(session_id,)
                )
                if rows.empty:
                    return ""
                chunks, labels = [], []
                for _, r in rows.iterrows():
                    cs = chunk_text(r["content_text"], max_chars=1200, overlap=200)
                    chunks.extend(cs)
                    labels.extend([r["filename"]] * len(cs))
                vec, X = embed_texts(chunks)
                top = search_chunks(question_text, vec, X, chunks, k=min(8, len(chunks)))
                parts, used = [], set()
                for sn in top:
                    try:
                        idx = chunks.index(sn)
                        fname = labels[idx]
                    except Exception:
                        fname = "attachment"
                    key = (fname, sn[:60])
                    if key in used:
                        continue
                    used.add(key)
                    parts.append(f"\n--- {fname} ---\n{sn.strip()}\n")
                return "Attached document snippets (most relevant first):\n" + "\n".join(parts[:16]) if parts else ""

            # Show chat history
            hist = pd.read_sql_query(
                "select role, content, created_at from chat_messages where session_id=? order by id asc",
                conn, params=(session_id,)
            )
            for _, row in hist.iterrows():
                if row["role"] == "user":
                    st.chat_message("user").markdown(row["content"])
                elif row["role"] == "assistant":
                    st.chat_message("assistant").markdown(row["content"])

            # Chat input lives inside the tab to avoid bleed-through
            user_msg = st.chat_input("Type your message")
            if user_msg:
                # Save user's message
                conn.execute("insert into chat_messages(session_id, role, content) values(?,?,?)",
                             (session_id, "user", user_msg))
                conn.commit()

                # Build system + context
                try:
                    context_snap = build_context(max_rows=6)
                except Exception:
                    context_snap = ""
                doc_snips = _chat_doc_snips(user_msg)

                system_text = "\n\n".join(filter(None, [
                    "You are a helpful federal contracting assistant. Keep answers concise and actionable.",
                    f"Context snapshot (keep answers consistent with this):\n{context_snap}" if context_snap else "",
                    doc_snips
                ]))

                # Construct rolling window of previous messages for context
                msgs_db = pd.read_sql_query(
                    "select role, content from chat_messages where session_id=? order by id asc",
                    conn, params=(session_id,)
                ).to_dict(orient="records")

                # Keep last ~12 user/assistant turns
                window = msgs_db[-24:] if len(msgs_db) > 24 else msgs_db
                messages = [{"role": "system", "content": system_text}] + window

                assistant_out = llm_messages(messages, temp=0.2, max_tokens=1200)
                conn.execute("insert into chat_messages(session_id, role, content) values(?,?,?)",
                             (session_id, "assistant", assistant_out))
                conn.commit()

                st.chat_message("user").markdown(user_msg)
                st.chat_message("assistant").markdown(assistant_out)



# ===== end app.py =====

# (moved) Proposal Builder call will be added after definition
# === New Feature Tabs Implementation ===

def _parse_date_any(s):
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None

def _lpta_note(total_price, budget_hint=None):
    if budget_hint is None:
        return "LPTA check requires competitor or IGCE context. Provide budget to evaluate."
    return "PASS" if total_price <= float(budget_hint) else "FAIL"

# Compute dynamic base index for new tabs
__tabs_base = 13  # 'Deadlines' tab index

with legacy_tabs[__tabs_base + 0]:
    st.subheader("Deadline tracker")
    conn = get_db()
    colA, colB = st.columns(2)
    with colA:
        st.caption("From opportunities table")
        o = pd.read_sql_query("select id, title, agency, response_due, status from opportunities order by response_due asc nulls last", conn)
        if not o.empty:
            o["due_dt"] = o["response_due"].apply(_parse_date_any)
            o["Due in days"] = o["due_dt"].apply(lambda d: (d - datetime.now()).days if d else None)
            st.dataframe(o[["id","title","agency","response_due","status","Due in days"]])
        else:
            st.info("No opportunities yet")
    with colB:
        st.caption("Manual deadlines")
        m = pd.read_sql_query("select * from deadlines order by due_date asc", conn)
        st.dataframe(m)
        with st.form("add_deadline"):
            title = st.text_input("Title")
            due = st.date_input("Due date", datetime.now().date())
            source = st.text_input("Source or link", "")
            notes = st.text_area("Notes", "")
            if st.form_submit_button("Add"):
                conn.execute("insert into deadlines(opp_id,title,due_date,source,notes) values(?,?,?,?,?)",
                             (None, title.strip(), due.strftime("%Y-%m-%d"), source.strip(), notes.strip()))
                conn.commit()
                st.success("Added")

    st.markdown("### Due today")
    due_today = pd.read_sql_query("select * from deadlines where date(due_date)=date('now') and status='Open'", conn)
    if not due_today.empty:
        st.dataframe(due_today[["title","due_date","source","notes"]])
        # Email reminders via Microsoft Graph
        st.markdown("#### Send email reminders")
        to_addr = st.text_input("Send reminders to email", value="")
        if st.button("Email reminders for items due today"):
            if to_addr:
                body_lines = ["The following items are due today:"]
                for _, r in due_today.iterrows():
                    body_lines.append(f"- {r['title']} (source: {r.get('source','')})")
                status = send_via_graph(to_addr, "Reminders: items due today", "\n".join(body_lines))
                st.info(f"Email status: {status}")
            else:
                st.info("Enter an email address to send reminders.")
    else:
        st.write("No items due today.")


with legacy_tabs[__tabs_base + 1]:
    st.subheader("Section L and M checklist")
    conn = get_db()
    opp_pick_df = pd.read_sql_query("select id, title from opportunities order by posted desc", conn)
    opp_opt = [""] + [f"{int(r.id)}: {r.title}" for _, r in opp_pick_df.iterrows()]
    opp_sel = st.selectbox("Link checklist to opportunity", options=opp_opt, index=0, key="lm_opp_sel")
    opp_id_val = int(opp_sel.split(":")[0]) if opp_sel else None

    up = st.file_uploader("Upload solicitation files", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key="lm_up")
    if up and st.button("Generate checklist"):
        items = []
        for f in up:
            name = f.name
            suffix = name.lower().split(".")[-1]
            # Extract with OCR fallback for snippets
            try:
                if suffix == "pdf":
                    data = f.read()
                    r = PdfReader(io.BytesIO(data))
                    txt = "\n".join((p.extract_text() or "") for p in r.pages)
                    if len((txt or "").strip()) < 500:
                        txt = _ocr_pdf_bytes(data) or txt
                else:
                    txt = read_doc(f)
            finally:
                try: f.seek(0)
                except Exception: pass

            def _snip(text, pat):
                try:
                    rx = re.compile(pat, re.I|re.S)
                    m = rx.search(text or "")
                    if not m: return ""
                    s0 = max(0, m.start()-120); e0 = min(len(text), m.end()+120)
                    return (text[s0:e0]).replace("\n", " ")[:240]
                except Exception:
                    return ""

            anchors = {
                "technical": r"(technical volume|technical proposal)",
                "price": r"(price volume|pricing|schedule of items)",
                "past performance": r"\bpast performance\b",
                "representations": r"(reps(?: and)? certs|52\.212-3)",
                "page limit": r"(page limit|not exceed \d+\s*pages|\d+\s*page\s*limit)",
                "font": r"(font\s*(size)?\s*\d+|times new roman|arial)",
                "delivery": r"(delivery|period of performance|pop:)",
                "submission": r"(submit .*? to|email .*? to|via sam\.gov)",
                "due date": r"(offers due|responses due|closing date)",
            }
            for label, pat in anchors.items():
                sn = _snip(txt or "", pat)
                notes = "Found" if sn else "Not detected"
                items.append({"item": label, "required": 1, "status": "Pending", "owner": "", "source_page": name, "notes": notes, "snippet": sn, "opp_id": opp_id_val})

            # Clause risk flags
            for hit in _find_clause_risks(txt or ""):
                items.append({"item": f"Risk: {hit['clause']}", "required": 0, "status": "Pending", "owner": "", "source_page": name, "notes": hit["hint"], "snippet": "", "opp_id": opp_id_val})

        df = pd.DataFrame(items)
        st.dataframe(df, use_container_width=True)
        for r in items:
            conn.execute("insert into compliance_items(opp_id,item,required,status,owner,source_page,notes,snippet) values(?,?,?,?,?,?,?,?)",
                         (r["opp_id"], r["item"], 1 if r["required"] else 0, r["status"], r["owner"], r["source_page"], r["notes"], r.get("snippet","")))
        conn.commit()
        st.success("Checklist saved with page anchors, owners and snippets")

    st.markdown("#### Existing items")
    items = pd.read_sql_query("select * from compliance_items order by created_at desc limit 200", conn)
    st.dataframe(items, use_container_width=True)

with legacy_tabs[__tabs_base + 2]:
    pass
    st.subheader("RFQ generator to subcontractors")
    conn = get_db()
    vendors = pd.read_sql_query("select id, company, email, phone, trades from vendors order by company", conn)
    st.caption("Compose RFQ")
    with st.form("rfq_form"):
        sel = st.multiselect("Recipients", vendors["company"].tolist())
        scope = st.text_area("Scope", st.session_state.get("default_scope", "Provide labor materials equipment and supervision per attached specifications"), height=120)
        qty = st.text_input("Quantities or CLIN list", "")
        due = st.date_input("Quote due by", datetime.now().date() + timedelta(days=3))
        files = st.text_input("File names to reference", "")
        subject = st.text_input("Email subject", "Quote request for upcoming federal project")
        body = st.text_area("Email body preview", height=240,
            value=(f"Hello, \n\nELA Management LLC requests a quote.\n\nScope\n{scope}\n\nQuantities\n{qty}\n\nDue by {due.strftime('%Y-%m-%d')}\n\nFiles\n{files}\n\nPlease reply with price lead time and any exclusions.\n\nThank you.")
        )
        submit = st.form_submit_button("Generate drafts")
    if submit:
        recs = vendors[vendors["company"].isin(sel)]
        for _, r in recs.iterrows():
            conn.execute("""insert into rfq_outbox(vendor_id, company, to_email, subject, body, due_date, files_json, status)
                            values(?,?,?,?,?,?,?,?)""",
                         (int(r["id"]), r["company"], r.get("email",""), subject, body, due.strftime("%Y-%m-%d"),
                          json.dumps([f.strip() for f in files.split(",") if f.strip()]), "Draft"))
        conn.commit()
        st.success(f"Created {len(recs)} RFQ draft(s)")
    st.markdown("#### Drafts")
    drafts = pd.read_sql_query("select * from rfq_outbox order by created_at desc", conn)
    st.dataframe(drafts)

    # Export selected draft as DOCX
    pick = st.number_input("Draft ID to export as DOCX", min_value=0, step=1, value=0)
    if pick:
        cur = conn.cursor()
        cur.execute("select company, subject, body from rfq_outbox where id=?", (int(pick),))
        row = cur.fetchone()
        if row:
            from docx import Document
            doc = Document()
            doc.add_heading(row[1], level=1)
            doc.add_paragraph(f"To: {row[0]}")
            for para in row[2].split("\\n\\n"):
                doc.add_paragraph(para)
            bio = io.BytesIO(); doc.save(bio); bio.seek(0)
            st.download_button("Download RFQ.docx", data=bio.getvalue(), file_name="RFQ.docx",
                               mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")

with legacy_tabs[__tabs_base + 3]:
    st.subheader("Pricing calculator")
    with st.form("price_calc"):
        default_base = float(st.session_state.get("pricing_base_cost", 0.0))
        base_cost = st.number_input("Base or subcontractor price", min_value=0.0, step=100.0, value=default_base)
        overhead = st.number_input("Overhead percent", min_value=0.0, max_value=100.0, step=0.5, value=10.0)
        gna = st.number_input("G and A percent", min_value=0.0, max_value=100.0, step=0.5, value=5.0)
        profit = st.number_input("Profit percent", min_value=0.0, max_value=100.0, step=0.5, value=8.0)
        igce = st.number_input("Budget or IGCE if known", min_value=0.0, step=100.0, value=0.0)
        terms_days = st.number_input("Payment terms (days)", min_value=0, step=1, value=30, help="Net terms for government payment")
        advance_pct = st.number_input("Factoring advance (%)", min_value=0.0, max_value=100.0, step=1.0, value=85.0)
        fac_rate = st.number_input("Factoring fee per 30 days (%)", min_value=0.0, max_value=10.0, step=0.1, value=2.0)
        run = st.form_submit_button("Calculate")
    if run:
        total = base_cost * (1 + overhead/100.0) * (1 + gna/100.0) * (1 + profit/100.0)
        note = _lpta_note(total, budget_hint=igce if igce > 0 else None)
        st.metric("Total price", f"${total:,.2f}")
        st.info(f"LPTA note: {note}")

        # Cash flow factoring model (simple)
        # Advance paid at day 0: advance_pct% of invoice; fee accrues until terms_days
        advance_amt = total * (advance_pct/100.0)
        period_factor = max(1, int(round(terms_days / 30.0)))
        fee = (fac_rate/100.0) * period_factor * total
        remainder = total - advance_amt - fee
        st.write({"Advance": round(advance_amt,2), "Estimated fee": round(fee,2), "Remainder on payment": round(remainder,2)})

        conn = get_db()
        try:
            cur = conn.cursor()
            # Ensure columns exist
            try: cur.execute("alter table pricing_scenarios add column terms_days integer")
            except Exception: pass
            try: cur.execute("alter table pricing_scenarios add column factoring_rate real")
            except Exception: pass
            try: cur.execute("alter table pricing_scenarios add column advance_pct real")
            except Exception: pass

            conn.execute("""insert into pricing_scenarios(opp_id, base_cost, overhead_pct, gna_pct, profit_pct, total_price, lpta_note, terms_days, factoring_rate, advance_pct)
                            values(?,?,?,?,?,?,?,?,?,?)""",
                        (None, float(base_cost), float(overhead), float(gna), float(profit), float(total), note, int(terms_days), float(fac_rate), float(advance_pct)))
            conn.commit()
        except Exception as _e_pc:
            st.caption(f"[Pricing save note: {_e_pc}]")

    st.markdown("### Scenario comparison")
    conn = get_db()
    try:
        dfp = pd.read_sql_query("select id, created_at, base_cost, overhead_pct, gna_pct, profit_pct, total_price, lpta_note, terms_days, factoring_rate, advance_pct from pricing_scenarios order by id desc limit 20", conn)
        if not dfp.empty:
            dfp["effective_fee"] = (dfp["factoring_rate"].fillna(0.0)/100.0) * (dfp["terms_days"].fillna(30)/30.0) * dfp["total_price"]
            st.dataframe(dfp, use_container_width=True)
        else:
            st.caption("No scenarios yet.")
    except Exception as _e_cmp:
        st.caption(f"[Scenario table note: {_e_cmp}]")




    with st.expander("Market data assist", expanded=True):
        colm1, colm2 = st.columns(2)
        with colm1:
            naics_q = st.text_input("NAICS for history lookup", value="", key="md_naics")
            psc_q = st.text_input("PSC for history lookup", value="", key="md_psc")
            kw_q = st.text_input("Optional keyword", value="", key="md_kw")
        with colm2:
            lookback_months = st.number_input("Look back months", min_value=1, step=1, value=24, key="md_months")
            limit_rows = st.number_input("Max awards to pull", min_value=10, step=10, value=200, key="md_limit")
            want_calc = st.checkbox("Also pull GSA CALC labor rates", value=False, key="md_calc")

        if st.button("Fetch market data", key="md_fetch"):
            from datetime import datetime as _dt, timedelta as _td
            date_to = _dt.utcnow().date().strftime("%Y-%m-%d")
            date_from = (_dt.utcnow().date() - _td(days=int(lookback_months)*30)).strftime("%Y-%m-%d")
            debug_box = st.container()
            df_awards, diag = usaspending_search_awards(
                naics=naics_q.strip(),
                psc=psc_q.strip(),
                date_from=date_from,
                date_to=date_to,
                keyword=kw_q.strip(),
                limit=int(limit_rows),
                st_debug=debug_box
            )
            if not df_awards.empty and "error" not in df_awards.columns:
                st.caption(f"USAspending awards from {date_from} to {date_to}")
                st.dataframe(df_awards.head(50), use_container_width=True)

                # Diagnostic breakdown
                import pandas as _pd
                from datetime import datetime as _dt

                def _months_between(s, e):
                    try:
                        sd = _dt.fromisoformat(str(s)[:10])
                        ed = _dt.fromisoformat(str(e)[:10])
                        days = max((ed - sd).days, 1)
                        return max(round(days / 30.44, 2), 0.01)
                    except Exception:
                        return None

                if "start" in df_awards.columns and "end" in df_awards.columns and "amount" in df_awards.columns:
                    _df = df_awards.copy()
                    _df["term_months"] = _df.apply(lambda r: _months_between(r["start"], r["end"]), axis=1)
                    _df["monthly_spend"] = _df.apply(lambda r: (float(r["amount"]) / r["term_months"]) if r["term_months"] and r["term_months"] > 0 else None, axis=1)

                    st.markdown("#### Diagnostics: term and monthly spend")
                    # Save selected awards as benchmarks with your annotations
                    st.markdown("#### Save selected awards to your benchmark library")
                    try:
                        _choices = _df["award_id"].dropna().astype(str).unique().tolist()
                    except Exception:
                        _choices = []
                    _sel_awards = st.multiselect("Pick award IDs to tag", _choices, key="md_pick_awards")
                    with st.form("md_bench_form"):
                        _sqft = st.number_input("Facility size sqft", min_value=0, step=1000, value=0, key="md_bench_sqft")
                        _freq = st.number_input("Visits per week", min_value=0, max_value=14, step=1, value=5, key="md_bench_freq")
                        _facility = st.text_input("Facility type", value="", key="md_bench_facility")
                        _scope = st.text_input("Scope tags comma separated", value="daily, restrooms, trash, floors", key="md_bench_scope")
                        _cpi = st.number_input("Inflation adjust percent per year", min_value=0.0, max_value=20.0, value=3.0, step=0.5, key="md_bench_cpi")
                        _note = st.text_area("Notes", value="", key="md_bench_notes")
                        _save = st.form_submit_button("Save to benchmarks")
                    if _save and _sel_awards:
                        import pandas as _pd, math as _math
                        from datetime import datetime as _dtd
                        _rows = _df[_df["award_id"].astype(str).isin(_sel_awards)].to_dict("records")
                        for r in _rows:
                            _tm = r.get("term_months") or 12.0
                            try:
                                # Simple CPI adjustment by term in years
                                _years = max((_tm / 12.0), 0.01)
                                _factor = (1.0 + float(_cpi)/100.0) ** _years
                            except Exception:
                                _factor = 1.0
                            _annual = float(r["amount"]) * (12.0 / _tm) if _tm and _tm > 0 else float(r["amount"])
                            _sqft_val = float(_sqft) if _sqft and _sqft > 0 else None
                            _dpsf = (_annual / _sqft_val) if _sqft_val else None
                            try:
                                conn.execute(
                                    "insert into pricing_benchmarks(award_id, agency, recipient, start, end, amount, term_months, monthly_spend, sqft, freq_per_week, facility_type, scope_tags, dollars_per_sqft_year, cpi_factor, amount_adj, notes) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (str(r.get("award_id")), str(r.get("agency")), str(r.get("recipient")), str(r.get("start")), str(r.get("end")), float(r.get("amount") or 0), float(_tm or 0), float(r.get("monthly_spend") or 0), _sqft_val, int(_freq or 0), _facility, _scope, float(_dpsf) if _dpsf is not None else None, float(_factor), float(r.get("amount") or 0) * float(_factor), _note)
                                )
                                conn.commit()
                            except Exception as _e:
                                st.warning(f"Save failed for {r.get('award_id')}: {_e}")
                        st.success(f"Saved {len(_sel_awards)} benchmark rows")

                    # View and use your benchmarks
                    with st.expander("Your benchmark library", expanded=False):
                        try:
                            _bench = _pd.read_sql_query("select * from pricing_benchmarks order by id desc limit 100", conn)
                        except Exception:
                            _bench = _pd.DataFrame()
                        if _bench is None or _bench.empty:
                            st.caption("No benchmarks yet. Save from the table above.")
                        else:
                            st.dataframe(_bench, use_container_width=True)
                            # Compute medians for $ per sqft and monthly spend
                            try:
                                _med_sqft = _pd.to_numeric(_bench["dollars_per_sqft_year"], errors="coerce").dropna().median()
                            except Exception:
                                _med_sqft = None
                            try:
                                _med_month = _pd.to_numeric(_bench["monthly_spend"], errors="coerce").dropna().median()
                            except Exception:
                                _med_month = None
                            if _med_sqft:
                                st.markdown(f"**Median dollars per sqft per year across benchmarks: ${_med_sqft:,.2f}**")
                            if _med_month:
                                st.markdown(f"**Median monthly spend across benchmarks: ${_med_month:,.0f}**")
                            _apply_sqft = st.number_input("Use sqft to apply median $ per sqft", min_value=0, step=1000, value=0, key="md_apply_sqft")
                            if _apply_sqft and _apply_sqft > 0 and _med_sqft:
                                _hint = float(_apply_sqft) * float(_med_sqft)
                                if st.button("Set base cost from benchmark median", key="md_bench_setbase"):
                                    st.session_state["pricing_base_cost"] = float(_hint)
                                    st.success(f"Base cost set to ${_hint:,.2f} from benchmark median. Recalculate above.")
    
                    st.dataframe(_df[["award_id","recipient","agency","start","end","amount","term_months","monthly_spend"]].head(50), use_container_width=True)

                    with st.expander("Implied $/sqft/year calculator", expanded=False):
                        sqft = st.number_input("Approx facility size (sqft)", min_value=0, step=1000, value=0, key="md_sqft")
                        per_week = st.number_input("Service frequency (visits per week)", min_value=0, max_value=14, step=1, value=5, key="md_freq")
                        if sqft and sqft > 0:
                            _df2 = _df.copy()
                            _df2["annualized_amount"] = _df2.apply(
                                lambda r: (float(r["amount"]) * (12.0 / r["term_months"])) if r["term_months"] and r["term_months"] > 0 else float(r["amount"]),
                                axis=1
                            )
                            _df2["dollars_per_sqft_year"] = _df2["annualized_amount"] / float(sqft)
                            st.caption("Based on your sqft input, here are implied $/sqft/year figures:")
                            st.dataframe(_df2[["award_id","agency","annualized_amount","dollars_per_sqft_year"]].head(50), use_container_width=True)

                            _vals = _pd.to_numeric(_df2["dollars_per_sqft_year"], errors="coerce").dropna()
                            if not _vals.empty:
                                _med = float(_vals.median())
                                st.markdown(f"**Median implied $/sqft/year across results: ${_med:,.2f}**")
                                if st.button("Set pricing hint from $/sqft median", key="md_set_sqft"):
                                    st.session_state["pricing_base_cost"] = _med * float(sqft)
                                    st.success(f"Base cost set to ${st.session_state['pricing_base_cost']:,.2f} from implied $/sqft median. Recalculate above.")

                if want_calc:
                    df_rates = gsa_calc_rates(kw_q or naics_q or psc_q or "janitorial")
                    if df_rates is not None and not df_rates.empty:
                        st.caption("GSA CALC sample labor rates")
                        st.dataframe(df_rates.head(50), use_container_width=True)
                        import pandas as _pd, numpy as _np
                        rate_series = _pd.to_numeric(df_rates["hourly_ceiling"], errors="coerce").dropna()
                        if not rate_series.empty:
                            with st.expander("Crew cost estimate (GSA CALC)", expanded=False):
                                crew_size = st.number_input("Crew size (people)", min_value=1, max_value=50, value=3, step=1, key="md_crew")
                                hrs_per_week = st.number_input("Hours per week (crew)", min_value=1, max_value=168, value=40, step=1, key="md_hours")
                                rate_med = float(_np.median(rate_series))
                                est_monthly = rate_med * float(crew_size) * float(hrs_per_week) * 4.33
                                st.markdown(f"Estimated crew cost at CALC median rate: **${est_monthly:,.0f} / month**")
                    else:
                        st.info("No CALC rates returned. Try a simpler keyword.")
            else:
                st.info("No award results returned. Try broadening filters or increasing look back.")
                st.caption(diag)

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
            d_dt = _coerce_dt(d)
            min_dt = _coerce_dt(min_due_date)
            if min_dt is None:
                due_ok = True  # allow when min date unknown
            else:
                due_ok = (d_dt is None) or (d_dt >= min_dt)
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

def render_rfp_analyzer():
    try:
        st.subheader("RFP Analyzer")
        st.caption("Upload RFP package and chat with memory. Use quick actions or ask your own questions.")

        conn = get_db()

        # Sessions like Chat Assistant
        sessions = pd.read_sql_query("select id, title, created_at from rfp_sessions order by created_at desc", conn)
        session_titles = ["➤ New RFP thread"] + [f"{r['id']}: {r['title'] or '(untitled)'}" for _, r in sessions.iterrows()]
        pick = st.selectbox("RFP session", options=session_titles, index=0)

        if pick == "➤ New RFP thread":
            default_title = f"RFP {datetime.now().strftime('%b %d %I:%M %p')}"
            new_title = st.text_input("Thread title", value=default_title)
            if st.button("Start RFP thread"):
                conn.execute("insert into rfp_sessions(title) values(?)", (new_title,))
                conn.commit()
                st.rerun()
            return

        if not pick:
            st.info("Select a chat session to continue.")
            st.stop()

        session_id = parse_pick_id(pick)
        if session_id is None:
            st.info("Select a valid session to continue.")
            st.stop()
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
            try:
                context_snap = build_context(max_rows=6)
            except NameError:
                context_snap = ""
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
    except Exception as e:
        st.error(f"RFP Analyzer error: {e}")

def render_proposal_builder():
    try:
        st.subheader("Proposal Builder")
        st.caption("Draft federal proposal sections using your RFP thread and files. Select past performance. Export to DOCX with guardrails.")

        conn = get_db()
        sessions = pd.read_sql_query("select id, title, created_at from rfp_sessions order by created_at desc", conn)
        if sessions.empty:
            st.warning("Create an RFP thread in RFP Analyzer first.")
            return

        opts = [f"{r['id']}: {r['title'] or '(untitled)'}" for _, r in sessions.iterrows()]
        pick = st.selectbox("Select RFP thread", options=opts, index=0, key="pb_session_pick")
        session_id = parse_pick_id(pick)
        if session_id is None:
            st.info("Select a valid session to continue.")
            st.stop()

        st.markdown("**Attach past performance to include**")
        df_pp = get_past_performance_df()
        selected_pp_ids = []
        if not df_pp.empty:
            df_pp["pick"] = False
            edited_pp = st.data_editor(df_pp[["id","title","agency","naics","period","value","role","highlights","pick"]], use_container_width=True, num_rows="fixed", key="pp_pick_grid")
            selected_pp_ids = [int(x) for x in edited_pp[edited_pp["pick"]==True]["id"].tolist()]
        else:
            st.caption("No past performance records yet. Add some in Past Performance tab.")

        col1, col2, col3 = st.columns(3)
        with col1:
            want_exec = st.checkbox("Executive Summary", True)
            want_tech = st.checkbox("Technical Approach", True)
        with col2:
            want_mgmt = st.checkbox("Management & Staffing Plan", True)
            want_past = st.checkbox("Past Performance", True)
        with col3:
            want_price = st.checkbox("Pricing Assumptions/Notes", True)
            want_comp = st.checkbox("Compliance Narrative", True)

        actions = {
            "Executive Summary": want_exec,
            "Technical Approach": want_tech,
            "Management & Staffing Plan": want_mgmt,
            "Past Performance": want_past,
            "Pricing Assumptions/Notes": want_price,
            "Compliance Narrative": want_comp,
        }
        # Canonical section order used by export and display
        order = [
            "Executive Summary",
            "Technical Approach",
            "Management & Staffing Plan",
            "Past Performance",
            "Pricing Assumptions/Notes",
            "Compliance Narrative",
        ]

        # Section-specific prompts always in scope for this builder
        section_prompts = {
            "Executive Summary": "Write an executive summary that aligns our capabilities to the requirement. Emphasize value, risk mitigation, and rapid mobilization.",
            "Technical Approach": "Describe a compliant, phase-oriented technical approach keyed to the PWS/SOW, referencing SLAs and QC steps.",
            "Management & Staffing Plan": "Provide management structure, roles, key personnel, surge plan, and communication/QA practices.",
            "Past Performance": "Summarize the selected past performance items, mapping relevance to scope, scale, and outcomes.",
            "Pricing Assumptions/Notes": "List pricing basis, inclusions/exclusions, assumptions, and any risk-based contingencies. No dollar totals.",
            "Compliance Narrative": "Map our response to Section L&M: where requirements are addressed, page limits, fonts, submission method."
        }



        drafts_df = pd.read_sql_query(
            "select id, section, content, updated_at from proposal_drafts where session_id=? order by section",
            conn, params=(session_id,)
        )

        colA, colB = st.columns([1,1])
        with colA:
            regenerate = st.button("Generate selected sections")
        if regenerate and not any(actions.values()):
            st.warning("Pick at least one section above, then click Generate selected sections.")
            regenerate = False

        with colB:
            save_all = st.button("Save edited drafts")
            export_md = st.button("Assemble full proposal (Markdown)")
            export_docx = st.button("Export Proposal DOCX (guardrails)")
        # === Generate selected sections ===
        if regenerate:
            # Diagnostics: show which sections are selected
            try:
                _on = [k for k,v in actions.items() if v]
                st.info(f"Generating sections: {', '.join(_on) if _on else 'none'}")
            except Exception:
                pass

            def _gen_with_fallback(system_text, user_prompt):
                # Immediate template if OpenAI client is not configured
                try:
                    from builtins import globals as _g
                except Exception:
                    _g = globals
                if not _g().get('client', None):
                    heading = (user_prompt.split('\n', 1)[0].strip() or 'Section')
                    tmpl = [
                        f'## {heading}',
                        '• Approach overview: Describe how we will fulfill the PWS tasks with measurable SLAs.',
                        '• Roles and responsibilities: Identify key staff and escalation paths.',
                        '• Quality assurance: Inspections, KPIs, and corrective actions.',
                        '• Risk mitigation: Top risks and mitigations tied to timeline.',
                        '• Compliance notes: Where Section L & M items are satisfied.',
                    ]
                    return '\n'.join(tmpl)
                try:
                    _out = llm(system_text, user_prompt, temp=0.3, max_tokens=1200)
                except Exception as _e:
                    _out = f'LLM error: {type(_e).__name__}: {_e}'
                bad = (not isinstance(_out, str)) or (_out.strip() == '') or ('Set OPENAI_API_KEY' in _out) or _out.startswith('LLM error')
                if bad:
                    heading = (user_prompt.split('\n', 1)[0].strip() or 'Section')
                    tmpl = [
                        f'## {heading}',
                        '• Approach overview: Describe how we will fulfill the PWS tasks with measurable SLAs.',
                        '• Roles and responsibilities: Identify key staff and escalation paths.',
                        '• Quality assurance: Inspections, KPIs, and corrective actions.',
                        '• Risk mitigation: Top risks and mitigations tied to timeline.',
                        '• Compliance notes: Where Section L & M items are satisfied.',
                    ]
                    return '\n'.join(tmpl)
                return _out

            # Helper: pull top snippets from attached RFP files for this session
            def _pb_doc_snips(question_text: str):
                rows = pd.read_sql_query(
                    "select filename, content_text from rfp_files where session_id=? and ifnull(content_text,'')<>''",
                    conn, params=(session_id,)
                )
                if rows.empty:
                    return ""
                chunks, labels = [], []
                for _, r in rows.iterrows():
                    cs = chunk_text(r["content_text"], max_chars=1200, overlap=200)
                    chunks.extend(cs); labels.extend([r["filename"]]*len(cs))
                vec, X = embed_texts(chunks)
                top = search_chunks(question_text, vec, X, chunks, k=min(10, len(chunks)))
                parts, used = [], set()
                for sn in top:
                    try:
                        idx = chunks.index(sn); fname = labels[idx]
                    except Exception:
                        fname = "attachment"
                    key = (fname, sn[:60])
                    if key in used: continue
                    used.add(key)
                    parts.append(f"\n--- {fname} ---\\n{sn.strip()}\\n")
                return "Attached RFP snippets (most relevant first):\n" + "\\n".join(parts[:16]) if parts else ""

            # Pull past performance selections text if any
            pp_text = ""
            if selected_pp_ids:
                qmarks = ",".join(["?"]*len(selected_pp_ids))
                df_sel = pd.read_sql_query(f"select title, agency, naics, period, value, role, location, highlights from past_performance where id in ({qmarks})", conn, params=tuple(selected_pp_ids))
                lines = []
                for _, r in df_sel.iterrows():
                    lines.append(f"- {r['title']} — {r['agency']} ({r['role']}); NAICS {r['naics']}; Period {r['period']}; Value ${float(r['value'] or 0):,.0f}. Highlights: {r['highlights']}")
                pp_text = "\n".join(lines)

            # Build common system context
            try:
                context_snap = build_context(max_rows=6)
            except Exception:
                context_snap = ""
            for sec, on in actions.items():
                if not on:
                    continue
                # Build doc context keyed to the section
                doc_snips = _pb_doc_snips(sec)
                system_text = "\\n\\n".join(filter(None, [
                    "You are a federal proposal writer. Use clear headings and concise bullets. Be compliant and specific.",
                    f"Company snapshot:\\n{context_snap}" if context_snap else "",
                    doc_snips,
                    f"Past Performance selections:\\n{pp_text}" if (pp_text and sec in ('Executive Summary','Past Performance','Technical Approach','Management & Staffing Plan')) else ""
                ]))
                user_prompt = section_prompts.get(sec, f"Draft the section titled: {sec}.")

                out = _gen_with_fallback(system_text, user_prompt)

                # Upsert into proposal_drafts
                cur = conn.cursor()
                cur.execute("select id from proposal_drafts where session_id=? and section=?", (session_id, sec))
                row = cur.fetchone()
                if row:
                    cur.execute("update proposal_drafts set content=?, updated_at=current_timestamp where id=?", (out, int(row[0])))
                else:
                    cur.execute("insert into proposal_drafts(session_id, section, content) values(?,?,?)", (session_id, sec, out))
                conn.commit()
            try:
                st.success("Generated drafts. Scroll down to 'Drafts' to review and edit.")
            except Exception:
                pass
            st.rerun()


        # Compliance validation settings
        st.markdown("#### Compliance validation settings")
        colv1, colv2, colv3 = st.columns(3)
        with colv1:
            pb_page_limit = st.number_input("Page limit (estimated)", min_value=0, step=1, value=0)
            pb_font = st.text_input("Required font", value="Times New Roman")
        with colv2:
            pb_font_size = st.number_input("Required size (pt)", min_value=8, max_value=14, step=1, value=12)
            pb_margins = st.number_input("Margins (inches)", min_value=0.5, max_value=1.5, value=1.0, step=0.25)
        with colv3:
            pb_line_spacing = st.number_input("Line spacing", min_value=1.0, max_value=2.0, value=1.0, step=0.1)
            pb_file_pat = st.text_input("Filename pattern", value="{company}_{solicitation}_{section}_{date}")

        # Assemble full proposal in Markdown
        if export_md:
            parts = []
            for sec in order:
                if sec not in actions or not actions[sec]:
                    continue
                cur = conn.cursor()
                cur.execute("select content from proposal_drafts where session_id=? and section=?", (session_id, sec))
                row = cur.fetchone()
                if row and row[0]:
                    parts.append(f"# {sec}\n\n{row[0].strip()}\n")
            assembled = "\n\n---\n\n".join(parts) if parts else "# Proposal\n(No sections saved yet.)"
            st.markdown("#### Assembled Proposal (Markdown preview)")
            st.code(assembled, language="markdown")
            st.download_button("Download proposal.md", data=assembled.encode("utf-8"),
                               file_name="proposal.md", mime="text/markdown")

        # Export DOCX with guardrails
        if export_docx:
            from docx import Document
            from docx.shared import Inches, Pt
            from docx.oxml.ns import qn

            parts = []
            for sec in order:
                cur = conn.cursor()
                cur.execute("select content from proposal_drafts where session_id=? and section=?", (session_id, sec))
                row = cur.fetchone()
                if row and row[0]:
                    parts.append((sec, row[0].strip()))
            full_text = "\n\n".join(f"{sec}\n\n{txt}" for sec, txt in parts)

            issues, _ = _validate_text_for_guardrails(
                full_text,
                page_limit=int(pb_page_limit) if pb_page_limit else None,
                require_font=pb_font or None,
                require_size_pt=int(pb_font_size) if pb_font_size else None,
                margins_in=float(pb_margins) if pb_margins else None,
                line_spacing=float(pb_line_spacing) if pb_line_spacing else None,
                filename_pattern=pb_file_pat or None
            )
            if issues:
                st.error("Export blocked until these issues are resolved:")
                for x in issues:
                    st.markdown(f"- {x}")
                st.stop()

            doc = Document()
            for section in doc.sections:
                section.top_margin = Inches(pb_margins or 1)
                section.bottom_margin = Inches(pb_margins or 1)
                section.left_margin = Inches(pb_margins or 1)
                section.right_margin = Inches(pb_margins or 1)

            style = doc.styles["Normal"]
            req_font = pb_font or "Times New Roman"
            style.font.name = req_font
            style._element.rPr.rFonts.set(qn("w:eastAsia"), req_font)
            style.font.size = Pt(pb_font_size or 12)

            for sec, txt in parts:
                doc.add_heading(sec, level=1)
                for para in txt.split("\n\n"):
                    doc.add_paragraph(para)

            bio = io.BytesIO()
            doc.save(bio)
            bio.seek(0)

            company = get_setting("company_name","ELA Management LLC")
            today = datetime.now().strftime("%Y%m%d")
            safe_title = (sessions[sessions["id"] == session_id]["title"].iloc[0] if not sessions.empty else "RFP").replace(" ", "_")
            fname = (pb_file_pat or "{company}_{solicitation}_{date}").format(
                company=company.replace(" ", "_"),
                solicitation=safe_title,
                section="FullProposal",
                date=today
            )
            if not fname.lower().endswith(".docx"):
                fname += ".docx"

            st.download_button("Download Proposal DOCX", data=bio.getvalue(), file_name=fname,
                               mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")

        st.markdown("### Drafts")
        order = ["Executive Summary","Technical Approach","Management & Staffing Plan","Past Performance","Pricing Assumptions/Notes","Compliance Narrative"]
        # Refresh drafts after generation so new content appears immediately
        drafts_df = pd.read_sql_query(
            "select id, section, content, updated_at from proposal_drafts where session_id=? order by section",
            conn, params=(session_id,)
        )
        existing = {r["section"]: r for _, r in drafts_df.iterrows()}
        edited_blocks = {}
        for sec in order:
            if not actions.get(sec, False):
                continue
            st.markdown(f"**{sec}**")
            txt = existing.get(sec, {}).get("content", "")
            edited_blocks[sec] = st.text_area(f"Edit {sec}", value=txt, height=240, key=f"pb_{sec}")

        if save_all and edited_blocks:
            cur = conn.cursor()
            for sec, content in edited_blocks.items():
                cur.execute("select id from proposal_drafts where session_id=? and section=?", (session_id, sec))
                row = cur.fetchone()
                if row:
                    cur.execute("update proposal_drafts set content=?, updated_at=current_timestamp where id=?", (content, int(row[0])))
                else:
                    cur.execute("insert into proposal_drafts(session_id, section, content) values(?,?,?)", (session_id, sec, content))
            conn.commit()
            st.success("Drafts saved.")

        
    except Exception as e:
        st.error(f"Proposal Builder error: {e}")

# === End new features ===


# ---- Attach feature tabs now that functions are defined ----
try:
    with legacy_tabs[5]:
        render_rfp_analyzer()
except Exception as e:
    st.caption(f"[RFP Analyzer tab note: {e}]")

try:
    with legacy_tabs[12]:
        render_proposal_builder()
except Exception as e:
    st.caption(f"[Proposal Builder tab note: {e}]")

with conn:
    conn.execute("""
    create table if not exists pricing_benchmarks(
        id integer primary key,
        award_id text,
        agency text,
        recipient text,
        start text,
        end text,
        amount real,
        term_months real,
        monthly_spend real,
        sqft real,
        freq_per_week integer,
        facility_type text,
        scope_tags text,
        dollars_per_sqft_year real,
        cpi_factor real,
        amount_adj real,
        notes text,
        source text default 'USAspending',
        created_at text default current_timestamp
    )
    """)


