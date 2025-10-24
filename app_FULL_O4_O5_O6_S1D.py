# -*- coding: utf-8 -*-
import streamlit as st
import sqlite3, pandas as pd, re, time, ssl, smtplib
from typing import List, Dict, Any, Optional, Tuple
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

@st.cache_resource
def connect_db(path: str = "ela.db") -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS outreach_sender_accounts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        label TEXT,
        email TEXT UNIQUE,
        app_password TEXT,
        smtp_host TEXT DEFAULT 'smtp.gmail.com',
        smtp_port INTEGER DEFAULT 587,
        tls INTEGER DEFAULT 1,
        is_active INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS outreach_templates(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        subject TEXT,
        body TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS outreach_audit_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sent_at TIMESTAMP,
        sender_email TEXT,
        recipient_email TEXT,
        subject TEXT,
        status TEXT,
        error TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS outreach_sequences(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS outreach_steps(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        seq_id INTEGER NOT NULL,
        step_no INTEGER NOT NULL,
        delay_hours INTEGER NOT NULL DEFAULT 72,
        subject TEXT DEFAULT '',
        body_html TEXT DEFAULT '',
        FOREIGN KEY(seq_id) REFERENCES outreach_sequences(id)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS outreach_schedules(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        seq_id INTEGER NOT NULL,
        step_no INTEGER NOT NULL,
        to_email TEXT NOT NULL,
        vendor_id INTEGER,
        send_at TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        last_error TEXT DEFAULT '',
        subject TEXT DEFAULT '',
        body_html TEXT DEFAULT '',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(seq_id) REFERENCES outreach_sequences(id)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS outreach_optouts(
        email TEXT PRIMARY KEY,
        reason TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS outreach_unsub_codes(
        code TEXT PRIMARY KEY,
        email TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        used_at TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS kv_store(
        k TEXT PRIMARY KEY,
        v TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vendors_t(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        email TEXT,
        phone TEXT,
        website TEXT,
        city TEXT,
        state TEXT,
        naics TEXT,
        place_id TEXT
    );
    """)
    conn.commit()

def sidebar_badge(conn: sqlite3.Connection) -> None:
    try:
        n = conn.execute("SELECT COUNT(1) FROM outreach_sender_accounts").fetchone()[0]
        if n:
            st.sidebar.success("O4 Active")
        else:
            st.sidebar.info("O4 Not Configured")
    except Exception:
        pass

def kv_set(conn: sqlite3.Connection, k: str, v: str) -> None:
    with conn:
        conn.execute("INSERT INTO kv_store(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))

def kv_get(conn: sqlite3.Connection, k: str, default: str = "") -> str:
    try:
        r = conn.execute("SELECT v FROM kv_store WHERE k=? LIMIT 1", (k,)).fetchone()
        return r[0] if r and r[0] is not None else default
    except Exception:
        return default

# O4
def o4_ui(conn: sqlite3.Connection) -> None:
    st.subheader("O4 — Sender accounts")
    with st.form("o4_add_sender", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            label = st.text_input("Label", placeholder="BD Gmail")
            email = st.text_input("Gmail address")
            host = st.text_input("SMTP host", value="smtp.gmail.com")
        with col2:
            app_password = st.text_input("App password (16 chars)", type="password")
            port = st.number_input("SMTP port", 1, 65535, value=587, step=1)
            tls = st.checkbox("Use STARTTLS", value=True)
        saved = st.form_submit_button("Save sender")
    if saved and email:
        with conn:
            conn.execute("""
                INSERT INTO outreach_sender_accounts(label,email,app_password,smtp_host,smtp_port,tls,is_active)
                VALUES(?,?,?,?,?,?,1)
                ON CONFLICT(email) DO UPDATE SET
                    label=excluded.label, app_password=excluded.app_password,
                    smtp_host=excluded.smtp_host, smtp_port=excluded.smtp_port, tls=excluded.tls
            """, (label or email, email, app_password, host, int(port), 1 if tls else 0))
        st.success(f"Saved {email}")
    rows = conn.execute("SELECT id,label,email,smtp_host,smtp_port,tls,is_active FROM outreach_sender_accounts ORDER BY created_at DESC").fetchall()
    if rows:
        st.markdown("**Configured senders**")
        for r in rows:
            id_, label, email, host, port, tls, is_active = r
            cols = st.columns([3,3,2,2,2,2])
            cols[0].write(label or "")
            cols[1].write(email)
            cols[2].write(host)
            cols[3].write(str(port))
            cols[4].write("TLS" if tls else "No TLS")
            cols[5].write("Active" if is_active else "Disabled")
            with st.popover(f"Manage • {email}"):
                if st.button("Delete", key=f"del_{email}")):
                    with conn:
                        conn.execute("DELETE FROM outreach_sender_accounts WHERE email=?", (email,))
                    st.warning(f"Deleted {email}")
                    st.experimental_rerun()

def _active_sender(conn: sqlite3.Connection) -> Optional[dict]:
    r = conn.execute("""SELECT label,email,app_password,smtp_host,smtp_port,tls FROM outreach_sender_accounts
                        WHERE is_active=1 ORDER BY created_at DESC LIMIT 1""").fetchone()
    if not r:
        return None
    return dict(label=r[0], email=r[1], app_password=r[2], host=r[3], port=int(r[4] or 587), tls=bool(r[5]))

# O2
def o2_ui(conn: sqlite3.Connection) -> None:
    st.subheader("O2 — Templates")
    with st.form("o2_add", clear_on_submit=True):
        name = st.text_input("Template name")
        subject = st.text_input("Subject")
        body = st.text_area("Body (supports {{name}}, {{company}}, {{UNSUB_LINK}})", height=220)
        ok = st.form_submit_button("Save template")
    if ok and name:
        with conn:
            conn.execute("""
                INSERT INTO outreach_templates(name,subject,body) VALUES(?,?,?)
                ON CONFLICT(name) DO UPDATE SET subject=excluded.subject, body=excluded.body
            """, (name, subject, body))
        st.success(f"Saved template {name}")
    df = pd.read_sql_query("SELECT name, subject, substr(body,1,200) AS preview FROM outreach_templates ORDER BY created_at DESC", conn)
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)

def _load_template(conn: sqlite3.Connection, name: str) -> Tuple[str,str]:
    r = conn.execute("SELECT subject, body FROM outreach_templates WHERE name=? LIMIT 1", (name,)).fetchone()
    if not r:
        return "", ""
    return r[0] or "", r[1] or ""

# O6
def o6_is_suppressed(conn: sqlite3.Connection, email: str) -> bool:
    em = (email or "").strip().lower()
    row = conn.execute("SELECT 1 FROM outreach_optouts WHERE lower(email)=? LIMIT 1", (em,)).fetchone()
    return bool(row)

def o6_add_optout(conn: sqlite3.Connection, email: str, reason: str = "user_unsubscribe") -> None:
    with conn:
        conn.execute("INSERT INTO outreach_optouts(email,reason) VALUES(?,?) ON CONFLICT(email) DO NOTHING", ((email or "").strip().lower(), reason))

def o6_set_base_url(conn: sqlite3.Connection, url: str) -> None:
    kv_set(conn, "o6_base_url", url or "")

def o6_get_base_url(conn: sqlite3.Connection) -> str:
    v = kv_get(conn, "o6_base_url", "")
    if v:
        return v
    try:
        return st.secrets.get("app_base_url", "")
    except Exception:
        return ""

def o6_new_code(conn: sqlite3.Connection, email: str) -> str:
    code = __import__("uuid").uuid4().hex
    with conn:
        conn.execute("INSERT INTO outreach_unsub_codes(code,email) VALUES(?,?)", (code, (email or "").strip().lower()))
    return code

def o6_unsub_link_for(conn: sqlite3.Connection, email: str) -> str:
    base = o6_get_base_url(conn)
    if not base:
        return ""
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}unsubscribe={o6_new_code(conn, email)}"

def o6_handle_query(conn: sqlite3.Connection) -> None:
    try:
        qp = st.experimental_get_query_params()
        if "unsubscribe" in qp:
            code = (qp.get("unsubscribe",[None]) or [None])[0]
            if code:
                row = conn.execute("SELECT email FROM outreach_unsub_codes WHERE code=? LIMIT 1", (code,)).fetchone()
                if row and row[0]:
                    o6_add_optout(conn, row[0], "link_click")
                    with conn:
                        conn.execute("UPDATE outreach_unsub_codes SET used_at=CURRENT_TIMESTAMP WHERE code=?", (code,))
                    st.success(f"{row[0]} unsubscribed")
                    return
                st.warning("Invalid unsubscribe link")
        if "unsubscribe_email" in qp:
            email = (qp.get("unsubscribe_email",[None]) or [None])[0]
            if email:
                o6_add_optout(conn, email, "direct_param")
                st.success(f"{email} unsubscribed")
    except Exception:
        pass

def o6_ui(conn: sqlite3.Connection) -> None:
    st.subheader("O6 — Compliance")
    base = st.text_input("Unsubscribe base URL", value=o6_get_base_url(conn), help="e.g., https://yourapp.yourdomain/")
    if st.button("Save base URL"):
        o6_set_base_url(conn, base.strip())
        st.success("Saved")
    df = pd.read_sql_query("SELECT email, reason, created_at FROM outreach_optouts ORDER BY created_at DESC", conn)
    st.dataframe(df, use_container_width=True, hide_index=True)

# SMTP send
def smtp_send(sender: dict, to_email: str, subject: str, html: str) -> None:
    msg = MIMEMultipart("alternative"); msg["Subject"] = subject or ""; msg["From"] = sender["email"]; msg["To"] = to_email
    msg.attach(MIMEText(html or "", "html"))
    if sender.get("tls", True):
        server = smtplib.SMTP(sender["host"], int(sender.get("port",587))); server.ehlo(); server.starttls(context=ssl.create_default_context()); server.login(sender["email"], sender["app_password"])
    else:
        server = smtplib.SMTP_SSL(sender["host"], int(sender.get("port",465)), context=ssl.create_default_context()); server.login(sender["email"], sender["app_password"])
    server.sendmail(sender["email"], [to_email], msg.as_string()); server.quit()

# O3
def _render_template(subject: str, body: str, row: dict) -> Tuple[str,str]:
    def rep(s: str) -> str:
        s = s.replace("{{name}}", row.get("name","")); s = s.replace("{{company}}", row.get("company","")); return s
    return rep(subject or ""), rep(body or "")

def o3_ui(conn: sqlite3.Connection) -> None:
    st.subheader("O3 — Mail merge & Send")
    sender = _active_sender(conn)
    if not sender:
        st.info("Add a sender account in O4 first."); return
    names = [r[0] for r in conn.execute("SELECT name FROM outreach_templates ORDER BY name").fetchall()]
    tpl = st.selectbox("Template", names) if names else None
    subject, body = _load_template(conn, tpl) if tpl else ("","")
    override_subj = st.text_input("Override subject (optional)")
    if override_subj: subject = override_subj
    colA, colB = st.columns(2)
    with colA: recipients_raw = st.text_area("Recipients (email,name,company)", height=200, placeholder="jane@acme.com,Jane,Acme")
    with colB: test_send = st.text_input("Send test to this email", value=sender["email"]); go_test = st.button("Send test"); go_bulk = st.button("Send bulk")
    rows = []
    for line in (recipients_raw or "").splitlines():
        parts = [p.strip() for p in line.split(",")]
        if parts and "@" in parts[0]: rows.append(dict(email=parts[0], name=(parts[1] if len(parts)>1 else ""), company=(parts[2] if len(parts)>2 else "")))
    base_url = o6_get_base_url(conn)
    def with_unsub(html: str, em: str) -> str:
        if "{{UNSUB_LINK}}" in (html or ""): return (html or "").replace("{{UNSUB_LINK}}", o6_unsub_link_for(conn, em) if base_url else "")
        if base_url:
            link = o6_unsub_link_for(conn, em); return (html or "") + f"<hr><p style='font-size:12px;color:#666'>To unsubscribe click <a href='{link}'>here</a>.</p>"
        return html
    if go_test and subject and body:
        try:
            s_subj, s_body = _render_template(subject, body, {"name":"Test","company":"TestCo"})
            s_body = with_unsub(s_body, test_send); smtp_send(sender, test_send, s_subj, s_body)
            with conn: conn.execute("INSERT INTO outreach_audit_log(sent_at,sender_email,recipient_email,subject,status) VALUES(CURRENT_TIMESTAMP,?,?,? ,'sent')", (sender["email"], test_send, s_subj))
            st.success("Test sent")
        except Exception as e:
            with conn: conn.execute("INSERT INTO outreach_audit_log(sent_at,sender_email,recipient_email,subject,status,error) VALUES(CURRENT_TIMESTAMP,?,?,? ,'error',?)", (sender["email"], test_send, subject, str(e)[:500]))
            st.error(f"Send failed: {e}")
    if go_bulk and rows and subject and body:
        sent=skipped=failed=0
        for r in rows:
            em = r["email"]
            if o6_is_suppressed(conn, em): skipped += 1; continue
            s_subj, s_body = _render_template(subject, body, r); s_body = with_unsub(s_body, em)
            try:
                smtp_send(sender, em, s_subj, s_body)
                with conn: conn.execute("INSERT INTO outreach_audit_log(sent_at,sender_email,recipient_email,subject,status) VALUES(CURRENT_TIMESTAMP,?,?,? ,'sent')", (sender["email"], em, s_subj))
                sent += 1; time.sleep(0.25)
            except Exception as e:
                with conn: conn.execute("INSERT INTO outreach_audit_log(sent_at,sender_email,recipient_email,subject,status,error) VALUES(CURRENT_TIMESTAMP,?,?,? ,'error',?)", (sender["email"], em, s_subj, str(e)[:500]))
                failed += 1
        st.success(f"Bulk done. Sent {sent}. Skipped {skipped}. Failed {failed}.")
    st.markdown("---")
    df = pd.read_sql_query("SELECT sent_at, sender_email, recipient_email, subject, status, error FROM outreach_audit_log ORDER BY sent_at DESC LIMIT 200", conn)
    st.dataframe(df, use_container_width=True, hide_index=True)

# O5
def _now_iso():
    return __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def o5_ui(conn: sqlite3.Connection) -> None:
    st.subheader("O5 — Follow-ups & SLA")
    seq_df = pd.read_sql_query("SELECT id, name FROM outreach_sequences ORDER BY name", conn)
    names = ["— New —"] + ([] if seq_df.empty else seq_df["name"].tolist())
    c1, c2 = st.columns([2,3])
    with c1:
        sel = st.selectbox("Sequence", names)
        new_name = st.text_input("New sequence name") if sel == "— New —" else sel
        if st.button("Save sequence"):
            if new_name and new_name.strip():
                with conn: conn.execute("INSERT INTO outreach_sequences(name) VALUES(?) ON CONFLICT(name) DO NOTHING", (new_name.strip(),))
                st.success("Saved"); st.experimental_rerun()
    with c2:
        if sel != "— New —" and not seq_df.empty:
            seq_id = int(seq_df.loc[seq_df["name"]==sel, "id"].iloc[0])
            st.markdown("**Steps**")
            steps = pd.read_sql_query("SELECT step_no, delay_hours, subject FROM outreach_steps WHERE seq_id=? ORDER BY step_no", conn, params=(seq_id,))
            if not steps.empty: st.dataframe(steps, use_container_width=True, hide_index=True)
            st.markdown("**Add step**")
            s1,s2,s3 = st.columns(3)
            with s1: step_no = st.number_input("Step #", 1, 20, value=(int(steps["step_no"].max())+1 if not steps.empty else 1))
            with s2: delay = st.number_input("Delay hours", 1, 720, value=72)
            with s3: subj = st.text_input("Subject")
            body = st.text_area("HTML body", height=160)
            if st.button("Add step"):
                with conn: conn.execute("INSERT INTO outreach_steps(seq_id,step_no,delay_hours,subject,body_html) VALUES(?,?,?,?,?)", (seq_id, int(step_no), int(delay), subj or "", body or ""))
                st.success("Step added"); st.experimental_rerun()
    st.markdown("---"); st.markdown("**Queue follow-ups**")
    if sel != "— New —" and not seq_df.empty: seq_id = int(seq_df.loc[seq_df["name"]==sel, "id"].iloc[0])
    else: seq_id = None
    emails_txt = st.text_area("Paste recipient emails (one per line)", height=120)
    if st.button("Queue follow-ups"):
        if not seq_id: st.error("Select an existing sequence first")
        else:
            steps = pd.read_sql_query("SELECT step_no, delay_hours, subject, body_html FROM outreach_steps WHERE seq_id=? ORDER BY step_no", conn, params=(seq_id,))
            if steps.empty: st.error("This sequence has no steps")
            else:
                base = __import__("datetime").datetime.utcnow(); count = 0
                with conn:
                    for em in [e.strip().lower() for e in (emails_txt or "").splitlines() if e.strip()]:
                        t = base
                        for _, row in steps.iterrows():
                            t = t + __import__("datetime").timedelta(hours=int(row["delay_hours"] or 0))
                            conn.execute("INSERT INTO outreach_schedules(seq_id,step_no,to_email,send_at,status,subject,body_html) VALUES(?,?,?,?,'queued',?,?)", (seq_id, int(row["step_no"]), em, t.strftime("%Y-%m-%dT%H:%M:%SZ"), row["subject"] or "", row["body_html"] or ""))
                            count += 1
                st.success(f"Queued {count} follow-up sends")
    st.markdown("**Send due now**")
    if st.button("Send due follow-ups"):
        due = pd.read_sql_query("SELECT id, to_email, subject, body_html FROM outreach_schedules WHERE status='queued' AND send_at<=? ORDER BY send_at LIMIT 200", conn, params=(_now_iso(),))
        if due.empty: st.info("No due items")
        else:
            sender = _active_sender(conn)
            if not sender: st.error("No active sender in O4")
            else:
                ok=fail=0; base_url = o6_get_base_url(conn)
                def with_unsub(html: str, em: str) -> str:
                    if "{{UNSUB_LINK}}" in (html or ""): return (html or "").replace("{{UNSUB_LINK}}", o6_unsub_link_for(conn, em) if base_url else "")
                    if base_url: link = o6_unsub_link_for(conn, em); return (html or "") + f"<hr><p style='font-size:12px;color:#666'>To unsubscribe click <a href='{link}'>here</a>.</p>"
                    return html
                for _, r in due.iterrows():
                    em = r["to_email"]
                    if o6_is_suppressed(conn, em):
                        with conn: conn.execute("UPDATE outreach_schedules SET status='skipped' WHERE id=?", (int(r["id"]),))
                        continue
                    try:
                        smtp_send(sender, em, r["subject"] or "", with_unsub(r["body_html"] or "", em))
                        with conn: conn.execute("UPDATE outreach_schedules SET status='sent' WHERE id=?", (int(r["id"]),))
                        ok += 1; time.sleep(0.25)
                    except Exception as e:
                        with conn: conn.execute("UPDATE outreach_schedules SET status='error', last_error=? WHERE id=?", (str(e)[:500], int(r["id"])))
                        fail += 1
                st.success(f"Sent {ok}, failed {fail}")

# S1D
def _norm_phone(p: str) -> str:
    digits = "".join(re.findall(r"\d+", str(p or "")))
    if len(digits)==11 and digits.startswith("1"): digits = digits[1:]
    return digits

def _existing_vendor_keys(conn: sqlite3.Connection):
    rows = conn.execute("SELECT name, COALESCE(phone,''), COALESCE(place_id,'') FROM vendors_t").fetchall()
    by_np, by_pid = set(), set()
    for r in rows:
        by_np.add(((r[0] or "").strip().lower(), _norm_phone(r[1] or "")))
        if r[2]: by_pid.add(r[2])
    return by_np, by_pid

def s1d_ui(conn: sqlite3.Connection) -> None:
    st.subheader("S1D — Google Places & Dedupe")
    # API key from secrets
    key = ""
    try: key = st.secrets["google"]["api_key"]
    except Exception:
        try: key = st.secrets["GOOGLE_API_KEY"]
        except Exception: key = ""
    if not key:
        st.error("Missing Google API key. Add to secrets as [google].api_key or GOOGLE_API_KEY."); return
    mode = st.radio("Location mode", ["Address", "Lat/Lng"], horizontal=True)
    lat = lng = None
    if mode=="Address":
        addr = st.text_input("Place of performance address")
        radius = st.number_input("Radius (miles)", 1, 200, value=50)
        if addr:
            try:
                import requests
                r = requests.get("https://maps.googleapis.com/maps/api/geocode/json", params={"address": addr, "key": key}, timeout=10)
                js = r.json()
                if js.get("status")=="OK":
                    loc = js["results"][0]["geometry"]["location"]; lat, lng = float(loc["lat"]), float(loc["lng"])
            except Exception as e: st.error(f"Geocode failed: {e}"); return
    else:
        c1,c2 = st.columns(2);
        with c1: lat = st.number_input("Latitude", value=38.8951)
        with c2: lng = st.number_input("Longitude", value=-77.0364)
        radius = st.number_input("Radius (miles)", 1, 200, value=50)
    q = st.text_input("Search query", placeholder="e.g., HVAC contractors, cabling, IT services")
    c1, c2 = st.columns(2)
    with c1: go = st.button("Search")
    with c2: nxt = st.button("Next page")
    tok_key = "s1d_next_token"
    if go: st.session_state.pop(tok_key, None)
    results = []
    if go or nxt:
        try:
            import requests
            params = {"query": q, "key": key, "region":"us"}
            if nxt:
                tok = st.session_state.get(tok_key)
                if tok: params = {"pagetoken": tok, "key": key}
            else:
                if lat is not None and lng is not None: params.update({"location": f"{lat},{lng}", "radius": int(float(radius)*1609.34)})
            r = requests.get("https://maps.googleapis.com/maps/api/place/textsearch/json", params=params, timeout=12)
            js = r.json(); st.caption(f"API status: {js.get('status','—')}")
            if js.get("next_page_token"): st.session_state[tok_key] = js["next_page_token"]
            else: st.session_state.pop(tok_key, None)
            results = js.get("results", [])
        except Exception as e: st.error(f"Search failed: {e}"); return
    if not results: st.info("Enter a query and click Search."); return
    rows = []; by_np, by_pid = _existing_vendor_keys(conn)
    for r in results:
        name = r.get("name",""); pid = r.get("place_id",""); addr = r.get("formatted_address","")
        city = state = ""
        if "," in addr:
            parts = [p.strip() for p in addr.split(",")]
            if len(parts)>=2: city = parts[-2]; state = parts[-1].split()[0]
        phone = website = google_url = ""
        try:
            import requests
            rd = requests.get("https://maps.googleapis.com/maps/api/place/details/json", params={"place_id": pid, "fields": "formatted_phone_number,website,url", "key": key}, timeout=10).json()
            det = rd.get("result", {}) or {}
            digits = "".join(re.findall(r"\d+", det.get("formatted_phone_number","") or ""))
            if len(digits)==11 and digits.startswith("1"): digits = digits[1:]
            phone = digits; website = det.get("website","") or ""; google_url = det.get("url","") or ""
        except Exception: pass
        dup = ((name.strip().lower(), phone) in by_np) or (pid in by_pid)
        rows.append(dict(name=name, address=addr, city=city, state=state, phone=phone, website=website, place_id=pid, google_url=google_url, _dup=dup))
        time.sleep(0.05)
    df = pd.DataFrame(rows)
    if df.empty: st.info("No results"); return
    def link(url: str, text: str) -> str: return f"<a href='{url}' target='_blank'>{text}</a>" if url else text
    show = df.copy(); show["name"] = show.apply(lambda r: link(r["google_url"], r["name"]), axis=1); show["website"] = show.apply(lambda r: link(r["website"], "site") if r["website"] else "", axis=1)
    show = show[["name","phone","website","address","city","state","place_id","_dup"]]
    st.markdown("**Results**"); st.write(show.to_html(escape=False, index=False), unsafe_allow_html=True)
    keep = df[~df["_dup"]]
    st.caption(f"{len(keep)} new vendors can be saved")
    if st.button("Save all new vendors"):
        with conn:
            for r in keep.to_dict("records"):
                conn.execute("INSERT INTO vendors_t(name,email,phone,website,city,state,naics,place_id) VALUES(?,?,?,?,?,?,?,?)", (r["name"], "", r["phone"], r["website"], r["city"], r["state"], "", r["place_id"]))
        st.success(f"Saved {len(keep)} new vendors")

# Nav / Pages
def nav() -> str:
    return st.sidebar.radio("Go to", ["Outreach", "Subcontractor Finder"], index=0)

def run_outreach_page(conn: sqlite3.Connection) -> None:
    o6_handle_query(conn)
    st.header("Outreach")
    with st.expander("Sender accounts (O4)", expanded=True): o4_ui(conn)
    with st.expander("Templates (O2)", expanded=True): o2_ui(conn)
    with st.expander("Follow-ups & SLA (O5)", expanded=False): o5_ui(conn)
    with st.expander("Compliance (O6)", expanded=False): o6_ui(conn)
    with st.expander("Mail Merge & Send (O3)", expanded=True): o3_ui(conn)

def run_subfinder_page(conn: sqlite3.Connection) -> None:
    st.header("Subcontractor Finder")
    with st.expander("S1D — Google Places & Dedupe", expanded=True): s1d_ui(conn)

def main():
    st.set_page_config(page_title="ELA GovCon — Outreach & SubFinder", layout="wide")
    conn = connect_db(); ensure_tables(conn); sidebar_badge(conn)
    page = nav()
    if page == "Outreach": run_outreach_page(conn)
    elif page == "Subcontractor Finder": run_subfinder_page(conn)
    else: st.write("Unknown page")

if __name__ == "__main__":
    main()