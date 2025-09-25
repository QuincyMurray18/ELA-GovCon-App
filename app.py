# app.py
import os
import io
import re
import json
import sqlite3
from datetime import datetime, date
from typing import Optional, List

import numpy as np
import pandas as pd
import streamlit as st

# =========================================================
# App setup
# =========================================================
st.set_page_config(page_title="ELA Management GovCon Suite", layout="wide")

# =========================================================
# Company information
# Edit these to match ELA Management LLC official profile
# =========================================================
COMPANY_INFO = {
    "name": "ELA Management LLC",
    "address": "123 Business Way, Houston TX 77001",
    "email": "info@elamanagementllc.com",
    "phone": "(123) 456 7890",
    "cage": "XXXXX",
    "uei": "XXXXX",
    "naics": ["236220", "561730", "561720", "722310", "541519"],
    "sam_registered": True,
    "capabilities": [
        "Facilities support and janitorial",
        "Landscaping and grounds",
        "Light construction and renovations",
        "Catering and food services",
        "IT and other computer services"
    ]
}

# =========================================================
# Data layer
# =========================================================
@st.cache_resource
def get_db():
    conn = sqlite3.connect("govcon.db", check_same_thread=False)
    conn.execute("""create table if not exists opportunities(
        id integer primary key autoincrement,
        title text,
        agency text,
        naics text,
        posted text,
        response_due text,
        url text,
        status text default 'New'
    )""")
    conn.execute("""create table if not exists vendors(
        id integer primary key autoincrement,
        name text,
        contact text,
        email text,
        phone text,
        naics text,
        notes text
    )""")
    conn.execute("""create table if not exists naics_watch(
        code text primary key
    )""")
    conn.execute("""create table if not exists settings(
        key text primary key,
        val text
    )""")
    conn.commit()
    return conn

def get_setting(key:str, default:str=""):
    conn = get_db()
    cur = conn.execute("select val from settings where key=?", (key,))
    row = cur.fetchone()
    return row[0] if row else default

def set_setting(key:str, val:str):
    conn = get_db()
    conn.execute("insert into settings(key,val) values(?,?) on conflict(key) do update set val=excluded.val", (key, val))
    conn.commit()

def _parse_sam_date(txt: Optional[str]) -> Optional[date]:
    if not txt:
        return None
    for fmt in ["%b %d %Y", "%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y", "%Y%m%d"]:
        try:
            return datetime.strptime(txt.strip(), fmt).date()
        except Exception:
            pass
    try:
        return pd.to_datetime(txt).date()
    except Exception:
        return None

# =========================================================
# Simple local LLM stub
# Replace this with your preferred API if desired
# =========================================================
def llm(system: str, prompt: str, max_tokens: int = 1200) -> str:
    # Stub that returns a structured placeholder
    # Swap with your API call as needed
    header = "# Draft Proposal Outline\n"
    body = (
        "## Technical\n"
        "* Understanding of scope\n"
        "* Methodology and risk controls\n\n"
        "## Management\n"
        "* Program schedule and reporting cadence\n\n"
        "## Staffing\n"
        "* Key roles and quals\n\n"
        "## Past Performance\n"
        "* Relevant projects or partner experience\n\n"
        "## Price\n"
        "* Basis of estimate and assumptions\n\n"
    )
    checklist = (
        "## Compliance Checklist\n"
        "* Volume count correct\n"
        "* Font and margins within limits\n"
        "* Page limits honored\n"
    )
    qna = (
        "## Clarifications\n"
        "* List RFIs and responses\n"
    )
    return header + body + checklist + qna

# =========================================================
# Document reading utilities
# =========================================================
def read_doc(uploaded) -> str:
    name = uploaded.name.lower()
    b = uploaded.read()
    if name.endswith(".txt"):
        try:
            return b.decode("utf-8", errors="ignore")
        except Exception:
            return b.decode("latin-1", errors="ignore")
    if name.endswith(".json"):
        try:
            return json.dumps(json.loads(b), indent=2)
        except Exception:
            return b.decode("utf-8", errors="ignore")
    if name.endswith(".docx"):
        try:
            from docx import Document  # python-docx
            f = io.BytesIO(b)
            doc = Document(f)
            return "\n".join(p.text for p in doc.paragraphs)
        except Exception:
            return ""
    if name.endswith(".pdf"):
        try:
            import pypdf
            reader = pypdf.PdfReader(io.BytesIO(b))
            return "\n".join(p.extract_text() or "" for p in reader.pages)
        except Exception:
            return ""
    # Fallback binary dump
    try:
        return b.decode("utf-8", errors="ignore")
    except Exception:
        return ""

# =========================================================
# RFP analysis helpers
# =========================================================
def analyze_rfp_text(rfp_text: str) -> dict:
    text = rfp_text.lower()
    res = {
        "Scope of Work": "",
        "Technical Specifications": "",
        "Performance Metrics": "",
        "Timeline and Milestones": "",
        "Evaluation Criteria": "",
        "Proposal Requirements": "",
        "Submission Instructions": ""
    }
    patterns = {
        "Scope of Work": r"(scope of work|sow|work statement)[\s\S]{0,1200}",
        "Technical Specifications": r"(technical requirements|specifications|specs)[\s\S]{0,1200}",
        "Performance Metrics": r"(performance standards|qasp|quality assurance|metrics)[\s\S]{0,1200}",
        "Timeline and Milestones": r"(period of performance|schedule|milestones)[\s\S]{0,1200}",
        "Evaluation Criteria": r"(evaluation criteria|basis for award|section m)[\s\S]{0,1200}",
        "Proposal Requirements": r"(proposal requirements|section l|instructions to offerors)[\s\S]{0,1200}",
        "Submission Instructions": r"(submit|submission|where to submit|how to submit|delivery of proposals)[\s\S]{0,1200}",
    }
    for k, pat in patterns.items():
        m = re.search(pat, text)
        if m:
            res[k] = rfp_text[m.start(): m.end()]
    return res

def grade_rfp(rfp_text: str) -> str:
    score = 0
    for kw in ["scope", "requirement", "performance", "evaluation", "submission", "timeline", "period of performance"]:
        if kw in rfp_text.lower():
            score += 1
    return "A" if score >= 6 else "B" if score >= 4 else "C" if score >= 2 else "D"

# =========================================================
# Pricing helpers
# =========================================================
def check_pricing_competitiveness(input_price: float, series: pd.Series):
    s = series.astype(float).dropna()
    if s.empty:
        return None
    mean = float(s.mean())
    med = float(s.median())
    p25 = float(np.percentile(s, 25))
    p75 = float(np.percentile(s, 75))
    diff = input_price - mean
    pct = 0.0 if mean == 0 else diff / mean * 100.0
    band_low = 0.97 * med
    band_high = 1.03 * med
    return dict(mean=mean, median=med, p25=p25, p75=p75, diff=diff, pct=pct, band_low=band_low, band_high=band_high)

# =========================================================
# Simple chat assistant
# =========================================================
def chat_assistant(user_input: str) -> str:
    responses = {
        "sam": "Yes ELA Management LLC is registered in SAM",
        "naics": f"Our primary NAICS codes are {', '.join(COMPANY_INFO['naics'])}",
        "cage": f"Our CAGE code is {COMPANY_INFO['cage']}",
        "uei": f"Our UEI is {COMPANY_INFO['uei']}",
        "hello": "Hello How can I support your government contracting needs today"
    }
    for k, v in responses.items():
        if k in user_input.lower():
            return v
    return "Share more detail about your question so I can help precisely"

# =========================================================
# Sidebar
# =========================================================
st.title("ELA Management GovCon Suite")
st.caption("Capture and Pre Bid  Proposal Development  Post Award")

with st.sidebar:
    st.header("Company")
    st.write(f"Name: {COMPANY_INFO['name']}")
    st.write(f"Address: {COMPANY_INFO['address']}")
    st.write(f"Email: {COMPANY_INFO['email']}")
    st.write(f"Phone: {COMPANY_INFO['phone']}")
    st.write(f"SAM registered: {'Yes' if COMPANY_INFO['sam_registered'] else 'No'}")
    st.write(f"NAICS: {', '.join(COMPANY_INFO['naics'])}")

    st.divider()
    st.header("Phase navigation")
    phase = st.radio(
        "Select phase",
        ["All tools", "Phase 1 Capture and Pre Bid", "Phase 2 Proposal Development", "Phase 3 Post Award and Performance"],
        index=0
    )

# =========================================================
# Phase containers
# Each phase renders its own cluster of tabs
# =========================================================
def ui_phase_1():
    tabs = st.tabs([
        "SAM Watch", "RFP Analyzer", "Pricing Lab", "Bid or No Bid", "Ask the doc", "Chat Assistant"
    ])

    # SAM Watch
    with tabs[0]:
        st.subheader("SAM Watch")
        st.caption("Track target NAICS and stash interesting notices into your pipeline")
        conn = get_db()
        st.write("Watch list")
        col_add1, col_add2 = st.columns([3,1])
        with col_add1:
            new_code = st.text_input("Add NAICS to watch example 561720", key="watch_add")
        with col_add2:
            if st.button("Add"):
                if new_code.strip():
                    conn.execute("insert or ignore into naics_watch(code) values(?)", (new_code.strip(),))
                    conn.commit()
                    st.success("Added")
        wl = pd.read_sql_query("select code from naics_watch order by code", conn)
        st.dataframe(wl, use_container_width=True, height=140)

        st.markdown("Add a notice to the pipeline")
        with st.form("add_opp"):
            c1, c2 = st.columns(2)
            with c1:
                t_title = st.text_input("Title")
                t_agency = st.text_input("Agency")
                t_naics = st.text_input("NAICS example 561720 or 561720,236220")
            with c2:
                t_posted = st.text_input("Posted example 2025-09-08")
                t_due = st.text_input("Response due example 2025-09-22")
                t_url = st.text_input("URL")
            submitted = st.form_submit_button("Save to pipeline")
            if submitted:
                get_db().execute(
                    "insert into opportunities(title,agency,naics,posted,response_due,url,status) values(?,?,?,?,?,?,?)",
                    (t_title, t_agency, t_naics, t_posted, t_due, t_url, "New")
                )
                get_db().commit()
                st.success("Saved")

        st.markdown("Current pipeline")
        df = pd.read_sql_query("select * from opportunities order by posted desc", conn)
        st.dataframe(df, use_container_width=True)

    # RFP Analyzer
    with tabs[1]:
        st.subheader("RFP Analyzer")
        ups = st.file_uploader("Upload RFP or SOW files", type=["pdf","docx","doc","txt"], accept_multiple_files=True)
        if ups:
            full_text = "\n\n".join(read_doc(u) for u in ups)
            st.markdown("Extracted insights")
            st.json(analyze_rfp_text(full_text))
            st.write(f"RFP grade: {grade_rfp(full_text)}")
            st.text_area("Raw text preview", value=full_text[:10000], height=240)

    # Pricing Lab
    with tabs[2]:
        st.subheader("Pricing Lab")
        st.caption("Upload awards or quotes and benchmark your bid with quartiles and a suggested competitive band")
        sample = pd.DataFrame({
            "description": ["Custodial small clinic","Custodial admin building","Custodial warehouse","Custodial renewal","Custodial office wing"],
            "amount": [48750, 51500, 49200, 50500, 49800],
            "naics": ["561720"]*5,
            "date": pd.date_range(end=datetime.today(), periods=5).date
        })
        st.markdown("Example data")
        st.dataframe(sample, use_container_width=True, height=160)

        up_csv = st.file_uploader("Upload CSV with columns description amount naics date", type=["csv"], key="price_csv")
        df_hist = sample.copy()
        if up_csv:
            try:
                df_hist = pd.read_csv(up_csv)
            except Exception as e:
                st.error(f"Could not read CSV {e}")

        st.markdown("Historical pricing")
        st.dataframe(df_hist, use_container_width=True, height=240)

        your_price = st.number_input("Enter your bid price", min_value=0.0, step=100.0, value=50000.0)
        if st.button("Benchmark my price"):
            r = check_pricing_competitiveness(your_price, df_hist["amount"])
            if not r:
                st.info("No price data to benchmark")
            else:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Average", f"${r['mean']:,.0f}")
                c2.metric("Median", f"${r['median']:,.0f}")
                c3.metric("P25", f"${r['p25']:,.0f}")
                c4.metric("P75", f"${r['p75']:,.0f}")
                st.write(f"Your price difference vs average is ${r['diff']:,.0f} which is {r['pct']:+.1f}%")
                st.info(f"Suggested competitive band near median is ${r['band_low']:,.0f} to ${r['band_high']:,.0f}")
                if your_price > r["p75"]:
                    st.warning("Your price is above the upper quartile  higher risk on price")
                elif your_price < r["p25"]:
                    st.success("Your price is below the lower quartile  strong on price confirm scope to avoid underbidding")
                else:
                    st.info("Your price is between P25 and P75  typical competitive range")

    # Bid or No Bid
    with tabs[3]:
        st.subheader("Bid or No Bid")
        st.caption("Weighted score from runway  NAICS fit  vendor coverage  keywords  set asides")
        conn = get_db()
        df_opp = pd.read_sql_query("select * from opportunities order by posted desc", conn)
        if df_opp.empty:
            st.info("No opportunities yet  add some in SAM Watch")
        else:
            st.dataframe(df_opp[["id","title","agency","naics","response_due","url","status"]], use_container_width=True)
            pick_id = st.number_input("Select id", min_value=1, step=1, value=int(df_opp.iloc[0]["id"]))
            opp = df_opp[df_opp["id"] == pick_id]
            if opp.empty:
                st.info("Enter a valid id")
            else:
                opp_row = opp.iloc[0]
                today = datetime.utcnow().date()
                due = _parse_sam_date(opp_row["response_due"]) or today
                runway_days = max(0, (due - today).days)

                weights = {
                    "runway": st.slider("Weight runway", 0, 40, 20),
                    "naics_fit": st.slider("Weight NAICS fit", 0, 40, 25),
                    "vendor_coverage": st.slider("Weight vendor coverage", 0, 40, 20),
                    "keywords": st.slider("Weight keyword match", 0, 40, 20),
                    "set_aside": st.slider("Weight set aside alignment", 0, 40, 15),
                }

                watch_codes = set(pd.read_sql_query("select code from naics_watch", conn)["code"].tolist())
                naics_in_opp = set((opp_row["naics"] or "").split(","))
                naics_fit = 1.0 if watch_codes & naics_in_opp else 0.6 if any(c[:4] in {w[:4] for w in watch_codes} for c in naics_in_opp) else 0.2

                vend = pd.read_sql_query("""select trim(substr(naics,1,6)) as code, count(*) as cnt from vendors
                                            where ifnull(naics,'')<>''
                                            group by trim(substr(naics,1,6))""", conn)
                coverage_ratio = 0.0
                if not vend.empty and naics_in_opp:
                    total = vend["cnt"].sum()
                    hits = vend[vend["code"].isin([c[:6] for c in naics_in_opp])]["cnt"].sum()
                    coverage_ratio = hits / total if total else 0.0
                vendor_cov = 0.2 if coverage_ratio == 0 else 0.6 if coverage_ratio < 0.1 else 1.0 if coverage_ratio > 0.25 else 0.8

                title_txt = (opp_row["title"] or "") + " " + (opp_row["agency"] or "")
                kw = ["janitorial","landscap","custodial","grounds","it","remediation","staffing","construction","bus","lodging","security","training"]
                kw_score = 1.0 if any(k in title_txt.lower() for k in kw) else 0.5

                set_aside_hit = 0.6
                for tag in ["8(a)","8a","sdvosb","veteran","hubzone","woman","wosb","small business set aside","small business"]:
                    if tag in title_txt.lower():
                        set_aside_hit = 1.0
                        break

                runway_score = 1.0 if runway_days >= 10 else 0.7 if runway_days >= 5 else 0.4 if runway_days >= 2 else 0.2
                total_w = sum(weights.values()) or 1
                total_score = (
                    runway_score * weights["runway"] +
                    naics_fit * weights["naics_fit"] +
                    vendor_cov * weights["vendor_coverage"] +
                    kw_score * weights["keywords"] +
                    set_aside_hit * weights["set_aside"]
                ) / total_w

                c1, c2 = st.columns(2)
                with c1:
                    st.metric("Runway days", runway_days)
                with c2:
                    st.metric("Score", f"{total_score*100:.0f}")

                if total_score >= 0.75:
                    st.success("Recommendation  BID")
                elif total_score >= 0.55:
                    st.info("Recommendation  CONSIDER if you can strengthen team and schedule")
                else:
                    st.warning("Recommendation  NO BID unless a strong partner changes the picture")

                if st.button("Log decision to pipeline"):
                    new_status = "Bid" if total_score >= 0.75 else "Consider" if total_score >= 0.55 else "No Bid"
                    conn.execute("update opportunities set status=? where id=?", (new_status, int(opp_row["id"])))
                    conn.commit()
                    st.success(f"Status updated to {new_status}")

    # Ask the doc
    with tabs[4]:
        st.subheader("Ask the doc")
        q = st.text_input("Ask a question about your uploaded RFPs")
        ups = st.file_uploader("Optional  attach docs to ground the answer", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key="ask_files")
        base = ""
        if ups:
            base = "\n\n".join(read_doc(u) for u in ups)[:200000]
        if st.button("Answer"):
            system = "You answer as a federal contracting analyst  prefer short bullet responses with citations to the provided text segments"
            prompt = f"Question:\n{q}\n\nGrounding text:\n{base}"
            st.markdown(llm(system, prompt))

    # Chat Assistant
    with tabs[5]:
        st.subheader("Chat Assistant")
        user_input = st.text_input("Ask anything about your GovCon workflow")
        if user_input:
            st.write(f"Assistant: {chat_assistant(user_input)}")

def ui_phase_2():
    tabs = st.tabs([
        "Proposal Generator", "Capability Statement", "White Paper Builder", "Compliance checklist"
    ])

    # Proposal Generator
    with tabs[0]:
        st.subheader("Proposal Generator")
        company = get_setting("company_name", COMPANY_INFO["name"])
        loc = get_setting("home_loc", "Houston TX")
        df_recent = pd.read_sql_query("select * from opportunities order by posted desc limit 20", get_db())
        choice = st.selectbox("Pick an opportunity", options=["Type my own"] + df_recent["title"].tolist())
        custom_title = ""
        if choice == "Type my own":
            custom_title = st.text_input("Opportunity title")
        title = custom_title or choice
        include_checklist = st.checkbox("Include a compliance checklist", value=True)
        include_qna = st.checkbox("Include a clarifications section", value=True)
        up = st.file_uploader("Optional  attach RFP or PWS", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key="prop_up")
        base_text = ""
        if up:
            base_text = "\n\n".join(read_doc(f) for f in up)[:350000]
        notes = st.text_area("Optional notes", value="Emphasize program management  staffing plan  quality control  reporting cadence")
        if st.button("Draft outline"):
            system = "You are a federal proposal writer  produce tight markdown mapped to typical section L and section M  prefer bullets"
            prompt = f"""
Company {company}  headquartered in {loc}
Opportunity title {title}
Include checklist {include_checklist}
Include QnA {include_qna}
Notes
{notes}

RFP grounding content
{base_text or '(none provided)'}
"""
            st.markdown(llm(system, prompt, max_tokens=1500))

    # Capability Statement
    with tabs[1]:
        st.subheader("Capability Statement")
        c = COMPANY_INFO
        cols = st.columns(2)
        with cols[0]:
            c["name"] = st.text_input("Entity name", c["name"])
            c["address"] = st.text_input("Address", c["address"])
            c["email"] = st.text_input("Email", c["email"])
            c["phone"] = st.text_input("Phone", c["phone"])
        with cols[1]:
            c["cage"] = st.text_input("CAGE", c["cage"])
            c["uei"] = st.text_input("UEI", c["uei"])
            naics_str = st.text_input("NAICS list comma separated", ", ".join(c["naics"]))
            c["naics"] = [x.strip() for x in naics_str.split(",") if x.strip()]
        caps = st.text_area("Core capabilities bullets one per line", "\n".join(c["capabilities"]))
        if st.button("Build capability statement preview"):
            st.markdown(f"""
# {c['name']}
Address  {c['address']}  
Email  {c['email']}  
Phone  {c['phone']}  
CAGE  {c['cage']}    UEI  {c['uei']}  
NAICS  {", ".join(c["naics"])}

## Capabilities
{os.linesep.join(f"* {line.strip()}" for line in caps.splitlines() if line.strip())}

## Differentiators
* Responsive management and quality control plan
* Past partners with proven performance
* Competitive and transparent pricing

## Contact
{c['email']}  {c['phone']}
            """)

    # White Paper Builder
    with tabs[2]:
        st.subheader("White Paper Builder")
        topic = st.text_input("Topic or problem statement")
        audience = st.text_input("Audience example Facility Manager USCG")
        key_points = st.text_area("Three to five points one per line", "Problem context\nApproach\nBenefits\nRisk and mitigation\nImplementation timeline")
        if st.button("Draft white paper"):
            system = "You write concise federal market white papers with clear value statements and short sections"
            prompt = f"Topic {topic}\nAudience {audience}\nPoints\n{key_points}"
            st.markdown(llm(system, prompt))

    # Compliance checklist
    with tabs[3]:
        st.subheader("Compliance checklist")
        ups = st.file_uploader("Upload RFP sections L and M or entire package", type=["pdf","docx","doc","txt"], accept_multiple_files=True, key="comp_up")
        if st.button("Extract checklist"):
            text = ""
            if ups:
                text = "\n\n".join(read_doc(u) for u in ups)[:250000]
            system = "Extract a compliance checklist from the provided text  return a short table in markdown with Requirement  Location  Notes"
            prompt = f"RFP text\n{text or '(none)'}"
            st.markdown(llm(system, prompt))

def ui_phase_3():
    tabs = st.tabs([
        "Pipeline and Awards", "Subcontractor Finder", "Outreach", "Performance metrics", "Data export"
    ])

    # Pipeline and Awards
    with tabs[0]:
        st.subheader("Pipeline and Awards")
        conn = get_db()
        df = pd.read_sql_query("select * from opportunities order by posted desc", conn)
        st.dataframe(df, use_container_width=True)
        if not df.empty:
            ids = df["id"].tolist()
            sel = st.selectbox("Select id to update status", ids)
            new_status = st.selectbox("New status", ["New","Bid","Consider","No Bid","Submitted","Awarded","Lost"])
            if st.button("Update status"):
                conn.execute("update opportunities set status=? where id=?", (new_status, int(sel)))
                conn.commit()
                st.success("Status updated")

    # Subcontractor Finder
    with tabs[1]:
        st.subheader("Subcontractor Finder")
        conn = get_db()
        st.markdown("Add or import vendors")
        with st.form("add_vendor"):
            c1, c2 = st.columns(2)
            with c1:
                vname = st.text_input("Vendor name")
                vcontact = st.text_input("Contact")
                vemail = st.text_input("Email")
            with c2:
                vphone = st.text_input("Phone")
                vnaics = st.text_input("NAICS codes comma separated")
                vnotes = st.text_area("Notes")
            subm = st.form_submit_button("Save vendor")
            if subm:
                conn.execute("insert into vendors(name,contact,email,phone,naics,notes) values(?,?,?,?,?,?)",
                             (vname, vcontact, vemail, vphone, vnaics, vnotes))
                conn.commit()
                st.success("Saved")
        dfv = pd.read_sql_query("select * from vendors order by name", conn)
        st.dataframe(dfv, use_container_width=True)

    # Outreach
    with tabs[2]:
        st.subheader("Outreach")
        st.caption("Quick email text you can paste into your client or CRM")
        proj = st.text_input("Project label example USCG Base Elizabeth City Grounds")
        scope = st.text_area("Short scope summary", "Grounds maintenance mowing edging trimming blowing seasonal cleanup")
        due = st.text_input("Quote due example Sep 22 2025")
        ask = st.text_area("What to request", "Confirm capabilities  Provide past performance  Provide price and availability")
        if st.button("Create outreach note"):
            msg = f"""
Hello
My name is Charles with ELA Management LLC
We are preparing a bid for {proj}
Scope summary
{scope}

Please let me know if you can support and provide pricing
Quote due {due}
Requested info
{ask}

Thank you
ELA Management LLC
"""
            st.code(msg, language="markdown")

    # Performance metrics
    with tabs[3]:
        st.subheader("Performance metrics")
        st.caption("Enter simple KPIs for awarded work  track trend month to month")
        df_default = pd.DataFrame({
            "month": pd.date_range("2025-01-01", periods=6, freq="MS").strftime("%Y-%m").tolist(),
            "on_time_tasks_percent": [98, 97, 99, 98, 96, 99],
            "qc_pass_rate_percent": [99, 98, 99, 100, 98, 99],
            "safety_incidents": [0, 0, 0, 0, 1, 0]
        })
        st.dataframe(df_default, use_container_width=True)
        st.info("For full tracking connect this to your operations data source later")

    # Data export
    with tabs[4]:
        st.subheader("Data export")
        conn = get_db()
        df1 = pd.read_sql_query("select * from opportunities", conn)
        df2 = pd.read_sql_query("select * from vendors", conn)
        df1_csv = df1.to_csv(index=False).encode("utf-8")
        df2_csv = df2.to_csv(index=False).encode("utf-8")
        st.download_button("Download opportunities CSV", data=df1_csv, file_name="opportunities.csv")
        st.download_button("Download vendors CSV", data=df2_csv, file_name="vendors.csv")

# =========================================================
# All tools view uses the same building blocks but grouped
# =========================================================
def ui_all_tools():
    st.header("Phase 1  Capture and Pre Bid")
    ui_phase_1()
    st.header("Phase 2  Proposal Development")
    ui_phase_2()
    st.header("Phase 3  Post Award and Performance")
    ui_phase_3()

# =========================================================
# Router
# =========================================================
if phase == "All tools":
    ui_all_tools()
elif phase == "Phase 1 Capture and Pre Bid":
    ui_phase_1()
elif phase == "Phase 2 Proposal Development":
    ui_phase_2()
else:
    ui_phase_3()
