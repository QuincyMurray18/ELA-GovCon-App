
import os
import sqlite3
from contextlib import closing
from typing import Optional, Any, Dict, List, Tuple
from datetime import datetime

import pandas as pd
import streamlit as st

APP_TITLE = "ELA GovCon Suite"
BUILD_LABEL = "Phase E — Quote Comparison + Pricing Calculator + Win Probability"

st.set_page_config(page_title=APP_TITLE, layout="wide")

DATA_DIR = "data"
DB_PATH = os.path.join(DATA_DIR, "govcon.db")
UPLOADS_DIR = os.path.join(DATA_DIR, "uploads")

# -------------------- setup --------------------
def ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(UPLOADS_DIR, exist_ok=True)


@st.cache_resource(show_spinner=False)
def get_db() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    with closing(conn.cursor()) as cur:
        cur.execute("PRAGMA foreign_keys = ON;")

        # Minimal core
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rfps(
                id INTEGER PRIMARY KEY,
                title TEXT,
                solnum TEXT,
                notice_id TEXT,
                sam_url TEXT,
                file_path TEXT,
                created_at TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clin_lines(
                id INTEGER PRIMARY KEY,
                rfp_id INTEGER NOT NULL REFERENCES rfps(id) ON DELETE CASCADE,
                clin TEXT,
                description TEXT,
                qty TEXT,
                unit TEXT,
                unit_price TEXT,
                extended_price TEXT
            );
        """)

        # Phase E: Quotes
        cur.execute("""
            CREATE TABLE IF NOT EXISTS quotes(
                id INTEGER PRIMARY KEY,
                rfp_id INTEGER NOT NULL REFERENCES rfps(id) ON DELETE CASCADE,
                vendor TEXT NOT NULL,
                received_date TEXT,
                notes TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS quote_lines(
                id INTEGER PRIMARY KEY,
                quote_id INTEGER NOT NULL REFERENCES quotes(id) ON DELETE CASCADE,
                clin TEXT,
                description TEXT,
                qty REAL,
                unit_price REAL,
                extended_price REAL
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_quote_lines ON quote_lines(quote_id, clin);")

        # Phase E: Pricing calculator (scenarios)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pricing_scenarios(
                id INTEGER PRIMARY KEY,
                rfp_id INTEGER NOT NULL REFERENCES rfps(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                overhead_pct REAL DEFAULT 0.0,
                gna_pct REAL DEFAULT 0.0,
                fee_pct REAL DEFAULT 0.0,
                contingency_pct REAL DEFAULT 0.0,
                created_at TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pricing_labor(
                id INTEGER PRIMARY KEY,
                scenario_id INTEGER NOT NULL REFERENCES pricing_scenarios(id) ON DELETE CASCADE,
                labor_cat TEXT,
                hours REAL,
                rate REAL,
                fringe_pct REAL DEFAULT 0.0
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pricing_other(
                id INTEGER PRIMARY KEY,
                scenario_id INTEGER NOT NULL REFERENCES pricing_scenarios(id) ON DELETE CASCADE,
                label TEXT,
                cost REAL
            );
        """)

        conn.commit()
    return conn


def _file_hash() -> str:
    try:
        import hashlib
        with open(__file__, 'rb') as f:
            return hashlib.sha256(f.read()).hexdigest()[:12]
    except Exception:
        return "unknown"


def _select_rfp(conn: sqlite3.Connection, label="RFP context"):
    df = pd.read_sql_query("SELECT id, title, solnum FROM rfps ORDER BY id DESC;", conn)
    if df.empty:
        st.info("No RFPs in DB. Use Phase B to create one (RFP Analyzer → Save).")
        return None, df
    rid = st.selectbox(label, options=df["id"].tolist(),
                       format_func=lambda rid: f"#{rid} — {df.loc[df['id']==rid, 'title'].values[0] or 'Untitled'}")
    return rid, df


# -------------------- QUOTE COMPARISON --------------------
def _calc_extended(qty: Optional[float], unit_price: Optional[float]) -> Optional[float]:
    try:
        if qty is None or unit_price is None:
            return None
        return float(qty) * float(unit_price)
    except Exception:
        return None


def run_quote_comparison(conn: sqlite3.Connection) -> None:
    st.header("Quote Comparison")
    rfp_id, _ = _select_rfp(conn)
    if not rfp_id:
        return

    st.subheader("Upload / Add Quotes")
    with st.expander("CSV Import", expanded=False):
        st.caption("Columns accepted: vendor, clin, qty, unit_price, description (optional). "
                   "Each row = one CLIN line for that vendor.")
        up = st.file_uploader("Quotes CSV", type=["csv"], key="quotes_csv")
        if up and st.button("Import Quotes CSV"):
            try:
                df = pd.read_csv(up)
                required = {"vendor", "clin", "qty", "unit_price"}
                if not required.issubset({c.lower() for c in df.columns}):
                    st.error("CSV missing required columns: vendor, clin, qty, unit_price")
                else:
                    # normalize headers
                    cols = {c: c.lower() for c in df.columns}
                    df.rename(columns=cols, inplace=True)
                    # group by vendor -> create quote, then insert lines
                    with closing(conn.cursor()) as cur:
                        by_vendor = df.groupby("vendor", dropna=False)
                        total_rows = 0
                        for vendor, block in by_vendor:
                            cur.execute(
                                "INSERT INTO quotes(rfp_id, vendor, received_date, notes) VALUES(?,?,?,?);",
                                (int(rfp_id), str(vendor)[:200], datetime.utcnow().isoformat(), "imported")
                            )
                            qid = cur.lastrowid
                            for _, r in block.iterrows():
                                qty = float(r.get("qty", 0) or 0)
                                upx = float(r.get("unit_price", 0) or 0)
                                ext = _calc_extended(qty, upx) or 0.0
                                cur.execute(
                                    "INSERT INTO quote_lines(quote_id, clin, description, qty, unit_price, extended_price) "
                                    "VALUES(?,?,?,?,?,?);",
                                    (qid, str(r.get("clin",""))[:50], str(r.get("description",""))[:300], qty, upx, ext)
                                )
                                total_rows += 1
                        conn.commit()
                    st.success(f"Imported {len(by_vendor)} quotes / {total_rows} lines.")
            except Exception as e:
                st.error(f"Import failed: {e}")

    with st.expander("Add Quote (manual)", expanded=False):
        vendor = st.text_input("Vendor name")
        date = st.date_input("Received date", value=datetime.utcnow().date())
        notes = st.text_input("Notes", value="")
        add_quote = st.button("Create Quote")
        if add_quote and vendor.strip():
            with closing(conn.cursor()) as cur:
                cur.execute("INSERT INTO quotes(rfp_id, vendor, received_date, notes) VALUES(?,?,?,?);",
                            (int(rfp_id), vendor.strip(), date.isoformat(), notes.strip()))
                qid = cur.lastrowid
                conn.commit()
                st.success(f"Created quote for {vendor}. Now add lines below (Quote ID {qid}).")
                st.session_state["current_quote_id"] = qid

    # Pick a quote to add/edit lines
    df_q = pd.read_sql_query("SELECT id, vendor, received_date, notes FROM quotes WHERE rfp_id=? ORDER BY vendor;",
                             conn, params=(rfp_id,))
    if not df_q.empty:
        st.subheader("Quotes")
        st.dataframe(df_q, use_container_width=True, hide_index=True)
        qid = st.selectbox("Edit lines for quote", options=df_q["id"].tolist(),
                           format_func=lambda qid: f"#{qid} — {df_q.loc[df_q['id']==qid,'vendor'].values[0]}")
        with st.form("add_quote_line", clear_on_submit=True):
            c1, c2, c3 = st.columns([2, 1, 1])
            with c1:
                clin = st.text_input("CLIN")
                desc = st.text_input("Description")
            with c2:
                qty = st.number_input("Qty", min_value=0.0, step=1.0)
            with c3:
                price = st.number_input("Unit Price", min_value=0.0, step=1.0)
            submitted = st.form_submit_button("Add Line")
        if submitted:
            ext = _calc_extended(qty, price) or 0.0
            with closing(conn.cursor()) as cur:
                cur.execute(
                    "INSERT INTO quote_lines(quote_id, clin, description, qty, unit_price, extended_price) "
                    "VALUES(?,?,?,?,?,?);",
                    (qid, clin.strip(), desc.strip(), float(qty), float(price), float(ext))
                )
                conn.commit()
            st.success("Line added.")

    # Comparison view
    st.subheader("Comparison")
    # Required CLINs for the RFP (for coverage %)
    df_target = pd.read_sql_query(
        "SELECT clin, description FROM clin_lines WHERE rfp_id=? GROUP BY clin, description ORDER BY clin;",
        conn, params=(rfp_id,)
    )
    df_lines = pd.read_sql_query("""
        SELECT q.vendor, l.clin, l.qty, l.unit_price, l.extended_price
        FROM quote_lines l
        JOIN quotes q ON q.id = l.quote_id
        WHERE q.rfp_id=?
    """, conn, params=(rfp_id,))
    if df_lines.empty:
        st.info("No quote lines yet.")
        return

    # Pivot to CLIN x Vendor matrix (extended price)
    mat = df_lines.pivot_table(index="clin", columns="vendor", values="extended_price", aggfunc="sum").fillna(0.0)
    mat = mat.sort_index()
    st.dataframe(mat.style.format("{:,.2f}"), use_container_width=True)

    # Identify best vendor per CLIN
    best_vendor_by_clin = mat.replace(0, float("inf")).idxmin(axis=1).to_frame("Best Vendor")
    st.caption("Best vendor per CLIN")
    st.dataframe(best_vendor_by_clin, use_container_width=True, hide_index=False)

    # Totals by vendor + coverage
    totals = df_lines.groupby("vendor")["extended_price"].sum().to_frame("Total").sort_values("Total")
    if not df_target.empty:
        coverage = df_lines.groupby("vendor")["clin"].nunique().to_frame("CLINs Quoted")
        coverage["Required CLINs"] = df_target["clin"].nunique()
        coverage["Coverage %"] = (coverage["CLINs Quoted"] / coverage["Required CLINs"] * 100).round(1)
        totals = totals.join(coverage, how="left")
    st.subheader("Totals & Coverage")
    st.dataframe(totals.style.format({"Total": "{:,.2f}", "Coverage %": "{:.1f}"}),
                 use_container_width=True)

    # Export
    if st.button("Export comparison CSV"):
        path = os.path.join(DATA_DIR, "quote_comparison.csv")
        out = mat.copy()
        out["Best Vendor"] = best_vendor_by_clin["Best Vendor"]
        out.to_csv(path)
        st.success("Exported.")
        st.markdown(f"[Download comparison CSV]({path})")


# -------------------- PRICING CALCULATOR --------------------
def _scenario_summary(conn: sqlite3.Connection, scenario_id: int) -> Dict[str, float]:
    # Fetch rows
    dl = pd.read_sql_query("SELECT hours, rate, fringe_pct FROM pricing_labor WHERE scenario_id=?;",
                           conn, params=(scenario_id,))
    other = pd.read_sql_query("SELECT cost FROM pricing_other WHERE scenario_id=?;", conn, params=(scenario_id,))
    base = pd.read_sql_query("SELECT overhead_pct, gna_pct, fee_pct, contingency_pct FROM pricing_scenarios WHERE id=?;",
                             conn, params=(scenario_id,))
    if base.empty:
        return {}
    overhead_pct, gna_pct, fee_pct, contingency_pct = base.iloc[0]
    direct_labor = float((dl["hours"] * dl["rate"]).sum()) if not dl.empty else 0.0
    fringe = float((dl["hours"] * dl["rate"] * (dl["fringe_pct"].fillna(0.0) / 100)).sum()) if not dl.empty else 0.0
    other_dir = float(other["cost"].sum()) if not other.empty else 0.0
    overhead = (direct_labor + fringe) * (float(overhead_pct) / 100.0)
    gna = (direct_labor + fringe + overhead + other_dir) * (float(gna_pct) / 100.0)
    subtotal = direct_labor + fringe + overhead + gna + other_dir
    contingency = subtotal * (float(contingency_pct) / 100.0)
    fee = (subtotal + contingency) * (float(fee_pct) / 100.0)
    total = subtotal + contingency + fee
    return {
        "Direct Labor": round(direct_labor, 2),
        "Fringe": round(fringe, 2),
        "Overhead": round(overhead, 2),
        "G&A": round(gna, 2),
        "Other Direct": round(other_dir, 2),
        "Contingency": round(contingency, 2),
        "Fee/Profit": round(fee, 2),
        "Total": round(total, 2),
    }


def run_pricing_calculator(conn: sqlite3.Connection) -> None:
    st.header("Pricing Calculator")
    rfp_id, _ = _select_rfp(conn)
    if not rfp_id:
        return

    st.subheader("Scenario")
    df_sc = pd.read_sql_query("SELECT id, name FROM pricing_scenarios WHERE rfp_id=? ORDER BY id DESC;",
                              conn, params=(rfp_id,))
    mode = st.radio("Mode", ["Create new", "Edit existing"], horizontal=True)
    if mode == "Create new":
        name = st.text_input("Scenario name", value="Base")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            overhead = st.number_input("Overhead %", min_value=0.0, value=20.0, step=1.0)
        with c2:
            gna = st.number_input("G&A %", min_value=0.0, value=10.0, step=1.0)
        with c3:
            fee = st.number_input("Fee/Profit %", min_value=0.0, value=7.0, step=0.5)
        with c4:
            contingency = st.number_input("Contingency %", min_value=0.0, value=0.0, step=0.5)
        if st.button("Create scenario", type="primary"):
            with closing(conn.cursor()) as cur:
                cur.execute("""
                    INSERT INTO pricing_scenarios(rfp_id, name, overhead_pct, gna_pct, fee_pct, contingency_pct, created_at)
                    VALUES(?,?,?,?,?,?,?);
                """, (int(rfp_id), name.strip(), float(overhead), float(gna), float(fee), float(contingency),
                      datetime.utcnow().isoformat()))
                conn.commit()
            st.success("Scenario created.")
            st.experimental_rerun()
        return
    else:
        if df_sc.empty:
            st.info("No scenarios yet. Switch to 'Create new'.")
            return
        scenario_id = st.selectbox("Pick a scenario", options=df_sc["id"].tolist(),
                                   format_func=lambda sid: df_sc.loc[df_sc["id"]==sid, "name"].values[0])

    st.subheader("Labor")
    with st.form("add_labor", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
        with c1:
            cat = st.text_input("Labor Category")
        with c2:
            hrs = st.number_input("Hours", min_value=0.0, step=1.0)
        with c3:
            rate = st.number_input("Rate", min_value=0.0, step=1.0)
        with c4:
            fringe = st.number_input("Fringe %", min_value=0.0, value=0.0, step=0.5)
        add_lab = st.form_submit_button("Add labor row")
    if add_lab:
        with closing(conn.cursor()) as cur:
            cur.execute("""
                INSERT INTO pricing_labor(scenario_id, labor_cat, hours, rate, fringe_pct)
                VALUES(?,?,?,?,?);
            """, (int(scenario_id), cat.strip(), float(hrs), float(rate), float(fringe)))
            conn.commit()
        st.success("Added.")

    df_lab = pd.read_sql_query("""
        SELECT id, labor_cat, hours, rate, fringe_pct, (hours*rate) AS direct, (hours*rate*fringe_pct/100.0) AS fringe
        FROM pricing_labor WHERE scenario_id=?;
    """, conn, params=(scenario_id,))
    st.dataframe(df_lab, use_container_width=True, hide_index=True)

    st.subheader("Other Direct Costs")
    with st.form("add_odc", clear_on_submit=True):
        c1, c2 = st.columns([3, 1])
        with c1:
            label = st.text_input("Label")
        with c2:
            cost = st.number_input("Cost", min_value=0.0, step=100.0)
        add_odc = st.form_submit_button("Add ODC")
    if add_odc:
        with closing(conn.cursor()) as cur:
            cur.execute("""
                INSERT INTO pricing_other(scenario_id, label, cost) VALUES(?, ?, ?);
            """, (int(scenario_id), label.strip(), float(cost)))
            conn.commit()
        st.success("Added ODC.")

    df_odc = pd.read_sql_query("SELECT id, label, cost FROM pricing_other WHERE scenario_id=?;",
                               conn, params=(scenario_id,))
    st.dataframe(df_odc, use_container_width=True, hide_index=True)

    st.subheader("Summary")
    s = _scenario_summary(conn, int(scenario_id))
    if not s:
        st.info("Add labor/ODCs to see a summary.")
        return
    df_sum = pd.DataFrame(list(s.items()), columns=["Component", "Amount"])
    st.dataframe(df_sum.style.format({"Amount": "{:,.2f}"}), use_container_width=True, hide_index=True)

    if st.button("Export pricing CSV"):
        path = os.path.join(DATA_DIR, f"pricing_scenario_{int(scenario_id)}.csv")
        df_sum.to_csv(path, index=False)
        st.success("Exported.")
        st.markdown(f"[Download pricing CSV]({path})")


# -------------------- WIN PROBABILITY --------------------
def _price_competitiveness(conn: sqlite3.Connection, rfp_id: int, our_total: Optional[float]) -> Optional[float]:
    """Return a 0-100 score where 100 means we are the lowest; None if not enough data."""
    df = pd.read_sql_query("""
        SELECT q.vendor, SUM(l.extended_price) AS total
        FROM quotes q JOIN quote_lines l ON q.id = l.quote_id
        WHERE q.rfp_id=?
        GROUP BY q.vendor
        ORDER BY total ASC;
    """, conn, params=(rfp_id,))
    if df.empty or our_total is None:
        return None
    comp_min = float(df["total"].min())
    if our_total <= comp_min:
        return 100.0
    # linear drop: +0–5% -> 85–100, 5–10% -> 70–85, >25% -> 0
    ratio = (our_total - comp_min) / comp_min
    if ratio <= 0.05:
        return 85 + (0.05 - ratio) * (15/0.05)
    if ratio <= 0.10:
        return 70 + (0.10 - ratio) * (15/0.05)
    if ratio <= 0.25:
        return 70 * (0.25 - ratio) / 0.15
    return 0.0


def run_win_probability(conn: sqlite3.Connection) -> None:
    st.header("Win Probability")
    rfp_id, _ = _select_rfp(conn)
    if not rfp_id:
        return

    # Pull compliance completion (from lm_items if available)
    df_items = pd.read_sql_query("SELECT status FROM lm_items WHERE rfp_id=?;", conn, params=(rfp_id,))
    if df_items.empty:
        compliance = st.slider("Compliance (est.)", 0, 100, 70)
    else:
        done = (df_items["status"] == "Complete").sum()
        total = len(df_items)
        compliance = int(round(done / max(1, total) * 100))

    tech = st.slider("Technical fit", 0, 100, 75)
    past_perf = st.slider("Past performance relevance", 0, 100, 70)
    team = st.slider("Team strength / subs readiness", 0, 100, 70)
    smallbiz = st.slider("Set-aside / socio-economic alignment", 0, 100, 80)

    # Price: if a pricing scenario exists, use its total; else manual
    df_sc = pd.read_sql_query("""
        SELECT s.id, s.name FROM pricing_scenarios s WHERE s.rfp_id=? ORDER BY s.id DESC;
    """, conn, params=(rfp_id,))
    price_score = None
    our_total = None
    if not df_sc.empty:
        sid = st.selectbox("Use pricing scenario (optional)", options=[None] + df_sc["id"].tolist(),
                           format_func=lambda x: "None" if x is None else df_sc.loc[df_sc["id"]==x, "name"].values[0])
        if sid:
            our_total = _scenario_summary(conn, int(sid)).get("Total")
    if our_total is None:
        our_total = st.number_input("Our total price (if no scenario)", min_value=0.0, value=0.0, step=1000.0)
    price_score = _price_competitiveness(conn, int(rfp_id), our_total)
    if price_score is None:
        price_score = st.slider("Price competitiveness (est.)", 0, 100, 70)

    st.subheader("Weights")
    c1, c2, c3 = st.columns(3)
    with c1:
        w_comp = st.number_input("Weight: Compliance", 0, 100, 20)
        w_tech = st.number_input("Weight: Technical", 0, 100, 25)
    with c2:
        w_past = st.number_input("Weight: Past Perf", 0, 100, 15)
        w_team = st.number_input("Weight: Team", 0, 100, 15)
    with c3:
        w_price = st.number_input("Weight: Price", 0, 100, 25)
        w_small = st.number_input("Weight: Small Biz", 0, 100, 0)
    total_w = w_comp + w_tech + w_past + w_team + w_price + w_small
    if total_w == 0:
        st.error("Weights must sum to > 0")
        return

    st.subheader("Scores")
    comp = {
        "Compliance": compliance,
        "Technical": tech,
        "Past Performance": past_perf,
        "Team": team,
        "Price": int(round(price_score)),
        "Small Business": smallbiz,
    }
    df_scores = pd.DataFrame(list(comp.items()), columns=["Factor", "Score (0-100)"])
    st.dataframe(df_scores, use_container_width=True, hide_index=True)

    weighted = (
        compliance * w_comp +
        tech * w_tech +
        past_perf * w_past +
        team * w_team +
        int(round(price_score)) * w_price +
        smallbiz * w_small
    ) / total_w

    # Map weighted score to a probability (simple logistic-ish scaling)
    # Keep it straightforward: P = weighted% directly for now
    win_prob = round(float(weighted), 1)
    st.subheader(f"Estimated Win Probability: **{win_prob}%**")

    if st.button("Export assessment CSV"):
        path = os.path.join(DATA_DIR, "win_probability_assessment.csv")
        out = df_scores.copy()
        out.loc[len(out)] = ["Weighted Result", win_prob]
        out.to_csv(path, index=False)
        st.success("Exported.")
        st.markdown(f"[Download assessment CSV]({path})")


# -------------------- stubs for other pages --------------------
def run_contacts(conn: sqlite3.Connection) -> None:
    st.header("Contacts"); st.caption("Use earlier builds for full Contacts UI")

def run_deals(conn: sqlite3.Connection) -> None:
    st.header("Deals"); st.caption("Use earlier builds for full Deals UI")

def run_rfp_analyzer(conn: sqlite3.Connection) -> None:
    st.header("RFP Analyzer"); st.caption("Use Phase B")

def run_lm_checklist(conn: sqlite3.Connection) -> None:
    st.header("L and M Checklist"); st.caption("Use Phase B")

def run_sam_watch(conn: sqlite3.Connection) -> None:
    st.header("SAM Watch"); st.caption("Use Phase A")

def run_proposal_builder(conn: sqlite3.Connection) -> None:
    st.header("Proposal Builder"); st.caption("Use Phase C")

def run_outreach(conn: sqlite3.Connection) -> None:
    st.header("Outreach"); st.caption("Use Phase D")

def run_subcontractor_finder(conn: sqlite3.Connection) -> None:
    st.header("Subcontractor Finder"); st.caption("Use Phase D")


# -------------------- nav + main --------------------
def init_session() -> None:
    if "initialized" not in st.session_state:
        st.session_state.initialized = True

def nav() -> str:
    st.sidebar.title("Workspace")
    st.sidebar.caption(f"Build {BUILD_LABEL}")
    st.sidebar.caption(f"SHA {_file_hash()}")
    return st.sidebar.selectbox(
        "Go to",
        [
            "Quote Comparison",
            "Pricing Calculator",
            "Win Probability",
            "RFP Analyzer",
            "L and M Checklist",
            "Proposal Builder",
            "Subcontractor Finder",
            "Outreach",
            "Contacts",
            "Deals",
            "SAM Watch",
        ],
    )

def router(page: str, conn: sqlite3.Connection) -> None:
    if page == "Quote Comparison":
        run_quote_comparison(conn)
    elif page == "Pricing Calculator":
        run_pricing_calculator(conn)
    elif page == "Win Probability":
        run_win_probability(conn)
    elif page == "RFP Analyzer":
        run_rfp_analyzer(conn)
    elif page == "L and M Checklist":
        run_lm_checklist(conn)
    elif page == "Proposal Builder":
        run_proposal_builder(conn)
    elif page == "Subcontractor Finder":
        run_subcontractor_finder(conn)
    elif page == "Outreach":
        run_outreach(conn)
    elif page == "Contacts":
        run_contacts(conn)
    elif page == "Deals":
        run_deals(conn)
    elif page == "SAM Watch":
        run_sam_watch(conn)
    else:
        st.error("Unknown page")

def main() -> None:
    init_session()
    conn = get_db()
    st.title(APP_TITLE)
    st.caption(BUILD_LABEL)
    router(nav(), conn)

if __name__ == "__main__":
    main()
