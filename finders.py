
import re
from datetime import datetime
from typing import List, Dict, Tuple, Optional

Page = Dict[str, str]  # expects keys: file, page, text

def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _try_parse_date(s: str) -> Optional[str]:
    s = s.strip().replace(",", "")
    fmts = [
        "%B %d %Y", "%b %d %Y", "%m/%d/%Y", "%m/%d/%y",
        "%Y-%m-%d", "%d %B %Y", "%d %b %Y"
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except Exception:
            pass
    return None

def find_due_date(pages: List[Page]) -> Dict:
    hits: List[Tuple[int, str, str, str]] = []
    pat_lines = [
        (1, r"(block\s*8\b.*?)(\d{1,2}[:.]\d{2}\s*(?:[AP]M)?)", False),
        (2, r"(offer(?:s)?|proposal|quote)s?\s+(?:are\s+)?due(?:\s+no\s+later\s+than|\s+by)?[:\s]+([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{2,4}|\d{1,2}/\d{1,2}/\d{2,4})", True),
        (3, r"(closing|due)\s+(?:date|time)[:\s]+([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{2,4}|\d{1,2}/\d{1,2}/\d{2,4})", True),
    ]
    tz_pat = re.compile(r"\b(ET|EST|EDT|CT|CST|CDT|MT|MST|MDT|PT|PST|PDT|Local Time)\b", re.I)
    for p in pages:
        txt = p.get("text", "") or ""
        lines = [l for l in re.split(r"[\r\n]+", txt) if l.strip()]
        for line in lines:
            line_n = _norm_space(line)
            low = line_n.lower()
            if ("sf 1449" in low or "standard form 1449" in low or "block 8" in low) and ("due" in low or "offer" in low):
                m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{2,4}|\d{1,2}/\d{1,2}/\d{2,4})", line_n)
                if m:
                    d = _try_parse_date(m.group(1)) or ""
                    hits.append((1, str(p.get("page","")), line_n, d or ""))
            for prio, pat, has_date in pat_lines:
                m = re.search(pat, line_n, re.I)
                if m:
                    date_txt = m.group(2) if has_date and m.lastindex and m.lastindex >= 2 else m.group(0)
                    d = _try_parse_date(date_txt) or ""
                    hits.append((prio, str(p.get("page","")), line_n, d))
    if not hits:
        return {"label":"Offer Due","date_iso":"","date_text":"","tz":"","page":"","source_line":""}
    hits.sort(key=lambda x: (x[0], x[3] or "0000-00-00"))
    best = hits[0]
    tz = ""
    m = tz_pat.search(best[2]) if hits else None
    if m:
        tz = m.group(1)
    return {
        "label": "Offer Due",
        "date_iso": best[3],
        "date_text": best[2],
        "tz": tz,
        "page": best[1],
        "source_line": best[2][:240]
    }

def find_naics_setaside(pages: List[Page]) -> Dict:
    naics = ""
    set_aside = ""
    for p in pages:
        t = _norm_space(p.get("text",""))
        if not naics:
            m = re.search(r"\bNAICS\b[:\s]*([0-9]{6})\b", t, re.I)
            if m: naics = m.group(1)
        if not set_aside:
            m2 = re.search(r"(?:set[-\s]?aside|Small\s+Business\s+Set[-\s]?Aside)[:\s]*([A-Za-z0-9\(\)\s\-\/]+)", t, re.I)
            if m2:
                val = m2.group(1).strip()
                rep = {
                    "total small business":"Total Small Business",
                    "small business":"Total Small Business",
                    "wosb":"WOSB",
                    "sdvosb":"SDVOSB",
                    "8(a)":"8(a)",
                    "hubzone":"HUBZone"
                }
                low = val.lower()
                set_aside = next((rep[k] for k in rep if k in low), val[:60])
        if naics and set_aside:
            break
    return {"naics": naics, "set_aside": set_aside}

def find_pocs(pages: List[Page]):
    out = []
    email_pat = re.compile(r"[A-Za-z0-9.\-_+%]+@[A-Za-z0-9\.\-]+\.[A-Za-z]{2,}")
    phone_pat = re.compile(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b")
    for p in pages:
        t = p.get("text","") or ""
        for email in set(email_pat.findall(t)):
            name = email.split("@")[0].replace(".", " ").replace("_"," ").title()[:60]
            ph = phone_pat.search(t)
            out.append({"name":name, "email":email, "phone": ph.group(0) if ph else "", "role": ""})
    seen = set(); dedup = []
    for r in out:
        if r["email"] in seen: continue
        seen.add(r["email"]); dedup.append(r)
    return dedup[:6]

def find_eval_criteria(pages: List[Page]):
    text = " ".join(_norm_space(p.get('text','')) for p in pages)
    mode = "Unknown"
    if re.search(r"LPTA|Lowest\s+Price\s+Technically\s+Acceptable", text, re.I):
        mode = "LPTA"
    if re.search(r"Best\s+Value|Trade[\-\s]?off", text, re.I):
        mode = "Best Value"
    factors = []
    for kw in ["price", "technical", "past performance", "experience", "management", "schedule", "quality"]:
        if re.search(rf"\b{re.escape(kw)}\b", text, re.I):
            factors.append(kw.title())
    return {"mode": mode, "factors": list(dict.fromkeys(factors))[:6]}

def find_pop(pages: List[Page]):
    text = " ".join(_norm_space(p.get('text','')) for p in pages)
    base_months = ""
    base = re.search(r"base\s+(?:period|year)\s*:\s*(\d{1,2})\s*(?:months?|mos?)", text, re.I)
    if base: base_months = base.group(1)
    options = len(re.findall(r"option\s+(?:period|year)", text, re.I))
    return {"base_months": base_months, "options": options}

def find_clins(pages: List[Page]):
    out = []
    for p in pages:
        for line in p.get("text","").splitlines():
            l = _norm_space(line)
            m = re.search(r"\b(?:CLIN|Item)\s*[:\-]?\s*(\d{3,4}[A-Z]?)\b.*?\b(?:QTY|Quantity)\s*[:\-]?\s*(\d+(?:\.\d+)?)\b.*?\b(EA|HR|MO|YR|LOT|LB|TN|DAY|WK|MON|YEAR|JOB)\b", l, re.I)
            if m:
                out.append({
                    "clin": m.group(1),
                    "description": l[:120],
                    "qty": m.group(2),
                    "unit": m.group(3).upper()
                })
    seen = set(); dedup = []
    for r in out:
        if r["clin"] in seen: continue
        seen.add(r["clin"]); dedup.append(r)
    return dedup[:50]
