# ===== app.py =====

def _strip_markdown_to_plain(txt: str) -> str:
    """
    Remove common Markdown markers so exported DOCX shows clean text instead of 'coded' look.
    """
    if not txt:
        return ""
    import re as _re
    s = txt
    # Remove code fences but keep inner text
    s = _re.sub(r"```(.*?)```", r"\1", s, flags=_re.DOTALL)
    # Inline code backticks
    s = s.replace("`", "")
    # Bold/italic markers
    s = s.replace("***", "")
    s = s.replace("**", "")
    s = s.replace("*", "")
    s = s.replace("__", "")
    s = s.replace("_", "")
    # Strip heading markers at line starts
    s = _re.sub(r"^[ \t]*#{1,6}[ \t]*", "", s, flags=_re.MULTILINE)
    # Strip blockquote markers
    s = _re.sub(r"^[ \t]*>[ \t]?", "", s, flags=_re.MULTILINE)
    # Remove list markers
    s = _re.sub(r"^[ \t]*([-*•]|\d+\.)[ \t]+", "", s, flags=_re.MULTILINE)
    # Remove table pipes (keep content)
    s = _re.sub(r"^\|", "", s, flags=_re.MULTILINE)
    s = _re.sub(r"\|$", "", s, flags=_re.MULTILINE)
    return s

import os, re, io, json, sqlite3, time
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urljoin, urlparse


# ===== Proposal drafts utilities =====
from datetime import datetime
import os, io

def _ensure_drafts_dir():
    base = os.path.join(os.getcwd(), "drafts", "proposals")
    os.makedirs(base, exist_ok=True)
    return base

def save_proposal_draft(title: str, content_md: str) -> str:
    base = _ensure_drafts_dir()
    safe = re.sub(r'[^A-Za-z0-9_.-]+', '_', title.strip() or "untitled")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"{ts}__{safe}.md"
    path = os.path.join(base, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content_md or "")
    return path

def list_proposal_drafts():
    base = _ensure_drafts_dir()
    items = []
    for f in sorted(os.listdir(base)):
        if f.lower().endswith(".md"):
            full = os.path.join(base, f)
            try:
                size = os.path.getsize(full)
            except Exception:
                size = 0
            items.append({"name": f, "path": full, "size": size})
    return list(reversed(items))  # newest first

def load_proposal_draft(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""

def delete_proposal_draft(path: str) -> bool:
    try:
        os.remove(path)
        return True
    except Exception:
        return False
# ===== end Proposal drafts utilities =====


def md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                     margins_in: float = 1.0, logo_bytes: bytes = None, logo_width_in: float = 1.5) -> bytes:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import io
    doc = Document()
    try:
        md_text = _clean_placeholders(md_text)
    except Exception:
        pass
        pass
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if logo_bytes:
        p_center = doc.add_paragraph(); p_center.paragraph_format.alignment = 1
        run = p_center.add_run()
        try:
            from docx.shared import Inches as _Inches
            run.add_picture(io.BytesIO(logo_bytes), width=_Inches(logo_width_in))
        except Exception:
            pass
    if title:
        h = doc.add_heading(title, level=1)
        try: h.style = doc.styles["Heading 1"]
        except Exception: pass
    _render_markdown_to_docx(doc, md_text)
    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()


def _md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                      margins_in: float = 1.0) -> bytes:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import io
    doc = Document()
    try:
        md_text = _clean_placeholders(md_text)
    except Exception:
        pass
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if title:
        h = doc.add_heading(title, level=1)
        try: h.style = doc.styles["Heading 1"]
        except Exception: pass
    _render_markdown_to_docx(doc, md_text)
    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()


# ===== Improved Markdown rendering helpers =====
def _add_hr_paragraph(doc):
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    p = doc.add_paragraph()
    pPr = p._p.get_or_add_pPr()
    pBdr = OxmlElement('w:pBdr')
    bottom = OxmlElement('w:bottom')
    bottom.set(qn('w:val'), 'single')
    bottom.set(qn('w:sz'), '6')
    bottom.set(qn('w:space'), '1')
    bottom.set(qn('w:color'), 'auto')
    pBdr.append(bottom)
    pPr.append(pBdr)
    return p

def _add_paragraph_with_inlines(doc, text, style=None):
    # Supports **bold**, *italic* inline
    import re as _re
    p = doc.add_paragraph()
    if style:
        try:
            p.style = doc.styles[style]
        except Exception:
            pass

    # Tokenize **bold** and *italic*
    tokens = []
    parts = _re.split(r'(\*\*[^\*]+\*\*)', text or '')
    for part in parts:
        if part.startswith('**') and part.endswith('**') and len(part) >= 4:
            tokens.append(('bold', part[2:-2]))
        else:
            subparts = _re.split(r'(\*[^\*]+\*)', part)
            for sp in subparts:
                if sp.startswith('*') and sp.endswith('*') and len(sp) >= 2:
                    tokens.append(('italic', sp[1:-1]))
                else:
                    tokens.append(('text', sp))

    for kind, chunk in tokens:
        if not chunk:
            continue
        run = p.add_run(chunk)
        if kind == 'bold':
            run.bold = True
        elif kind == 'italic':
            run.italic = True
    return p

def _render_markdown_to_docx(doc, md_text):
    import re as _re
    lines = (md_text or '').splitlines()
    bullet_buf, num_buf = [], []

    def flush_bullets():
        nonlocal bullet_buf
        for item in bullet_buf:
            _add_paragraph_with_inlines(doc, item, style="List Bullet")
        bullet_buf = []

    def flush_numbers():
        nonlocal num_buf
        for item in num_buf:
            _add_paragraph_with_inlines(doc, item, style="List Number")
        num_buf = []

    for raw in lines:
        line = (raw or '').rstrip()

        # Horizontal rule ---
        if _re.match(r'^\s*-{3,}\s*$', line):
            flush_bullets(); flush_numbers()
            _add_hr_paragraph(doc)
            continue

        # Blank -> flush lists and add spacer
        if not line.strip():
            flush_bullets(); flush_numbers()
            doc.add_paragraph("")
            continue

        # Headings (tolerate up to 3 leading spaces)
        m = _re.match(r'^\s{0,3}(#{1,6})\s+(.*)$', line)
        if m:
            flush_bullets(); flush_numbers()
            hashes, text = m.group(1), m.group(2).strip()
            level = min(len(hashes), 6)
            try:
                doc.add_heading(text, level=level)
            except Exception:
                _add_paragraph_with_inlines(doc, text)
            continue

        # Bullets: -, *, •
        if _re.match(r'^\s*(\-|\*|•)\s+', line):
            flush_numbers()
            bullet_buf.append(_re.sub(r'^\s*(\-|\*|•)\s+', '', line, count=1))
            continue

        # Numbered: 1. text
        if _re.match(r'^\s*\d+\.\s+', line):
            flush_bullets()
            num_buf.append(_re.sub(r'^\s*\d+\.\s+', '', line, count=1))
            continue

        # Normal paragraph with inline formatting
        flush_bullets(); flush_numbers()
        _add_paragraph_with_inlines(doc, line)

    flush_bullets(); flush_numbers()


def md_to_docx_bytes_rich(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                          margins_in: float = 1.0, logo_bytes: bytes = None, logo_width_in: float = 1.5) -> bytes:
    """
    Guaranteed rich Markdown→DOCX converter with inline bold/italics, headings, lists, and horizontal rules.
    """
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import io
    doc = Document()
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if logo_bytes:
        p_center = doc.add_paragraph(); p_center.paragraph_format.alignment = 1
        run = p_center.add_run()
        try:
            run.add_picture(io.BytesIO(logo_bytes), width=Inches(logo_width_in))
        except Exception:
            pass
    if title:
        h = doc.add_heading(title, level=1)
        try: h.style = doc.styles["Heading 1"]
        except Exception: pass

    _render_markdown_to_docx(doc, md_text)

    out = io.BytesIO()
    doc.save(out)
    out.seek(0)
    return out.getvalue()

# ===== end Improved Markdown rendering helpers =====


# ===== DOCX helpers (loaded early so they're available to all tabs) =====
def _md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                      margins_in: float = 1.0) -> bytes:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import re as _re, io
    doc = Document()
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if title:
        h = doc.add_heading(title, level=1)
        try:
            h.style = doc.styles["Heading 1"]
        except Exception:
            pass
    lines = (md_text or "").splitlines()
    bullet_buf, num_buf = [], []
    def flush_bullets():
        nonlocal bullet_buf
        for item in bullet_buf:
            p = doc.add_paragraph(item)
            try: p.style = doc.styles["List Bullet"]
            except Exception: pass
        bullet_buf = []
    def flush_numbers():
        nonlocal num_buf
        for item in num_buf:
            p = doc.add_paragraph(item)
            try: p.style = doc.styles["List Number"]
            except Exception: pass
        num_buf = []
    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            flush_bullets(); flush_numbers(); doc.add_paragraph(""); continue
        if line.startswith("### "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[4:].strip(), level=3); continue
        if line.startswith("## "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[3:].strip(), level=2); continue
        if line.startswith("# "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[2:].strip(), level=1); continue
        if _re.match(r"^(\-|\*|•)\s+", line):
            flush_numbers(); bullet_buf.append(_re.sub(r"^(\-|\*|•)\s+", "", line, count=1)); continue
        if _re.match(r"^\d+\.\s+", line):
            flush_bullets(); num_buf.append(_re.sub(r"^\d+\.\s+", "", line, count=1)); continue
        flush_bullets(); flush_numbers(); doc.add_paragraph(line)
    flush_bullets(); flush_numbers()
    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()

def md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                     margins_in: float = 1.0, logo_bytes: bytes = None, logo_width_in: float = 1.5) -> bytes:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import re as _re, io
    doc = Document()
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass
    if logo_bytes:
        p_center = doc.add_paragraph(); p_center.paragraph_format.alignment = 1
        run = p_center.add_run()
        try: run.add_picture(io.BytesIO(logo_bytes), width=Inches(logo_width_in))
        except Exception: pass
    if title:
        h = doc.add_heading(title, level=1)
        try: h.style = doc.styles["Heading 1"]
        except Exception: pass
    lines = (md_text or "").splitlines()
    bullet_buf, num_buf = [], []
    def flush_bullets():
        nonlocal bullet_buf
        for item in bullet_buf:
            p = doc.add_paragraph(item)
            try: p.style = doc.styles["List Bullet"]
            except Exception: pass
        bullet_buf = []
    def flush_numbers():
        nonlocal num_buf
        for item in num_buf:
            p = doc.add_paragraph(item)
            try: p.style = doc.styles["List Number"]
            except Exception: pass
        num_buf = []
    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            flush_bullets(); flush_numbers(); doc.add_paragraph(""); continue
        if line.startswith("### "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[4:].strip(), level=3); continue
        if line.startswith("## "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[3:].strip(), level=2); continue
        if line.startswith("# "):
            flush_bullets(); flush_numbers(); doc.add_heading(line[2:].strip(), level=1); continue
        if _re.match(r"^(\-|\*|•)\s+", line):
            flush_numbers(); bullet_buf.append(_re.sub(r"^(\-|\*|•)\s+", "", line, count=1)); continue
        if _re.match(r"^\d+\.\s+", line):
            flush_bullets(); num_buf.append(_re.sub(r"^\d+\.\s+", "", line, count=1)); continue
        flush_bullets(); flush_numbers(); doc.add_paragraph(line)
    flush_bullets(); flush_numbers()
    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()
# ===== end DOCX helpers =====


import pandas as pd
import numpy as np
import streamlit as st


# === Outreach Email (per-user) helpers ===
import smtplib, base64
from email.message import EmailMessage

USER_EMAILS = {
    "Quincy": "quincy.elamgmt@gmail.com",
    "Charles": "charles.elamgmt@gmail.com",
    "Collin": "collin.elamgmt@gmail.com",
}

def _mail_store_path():
    base = os.path.join(os.getcwd(), "secure_auth")
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "mail.json")

def _load_mail_store():
    try:
        with open(_mail_store_path(), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_mail_store(store: dict):
    with open(_mail_store_path(), "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2)

def set_user_smtp_app_password(user: str, app_password: str):
    store = _load_mail_store()
    u = store.get(user, {})
    u["smtp_host"] = "smtp.gmail.com"
    u["smtp_port"] = 587
    u["username"] = USER_EMAILS.get(user, "")
    u["app_password_b64"] = base64.b64encode((app_password or "").encode("utf-8")).decode("ascii")
    store[user] = u
    _save_mail_store(store)

def get_user_mail_config(user: str):
    store = _load_mail_store()
    rec = store.get(user, {})
    if not rec:
        return None
    pw = base64.b64decode(rec.get("app_password_b64", "").encode("ascii")).decode("utf-8") if rec.get("app_password_b64") else ""
    return {
        "smtp_host": rec.get("smtp_host", "smtp.gmail.com"),
        "smtp_port": rec.get("smtp_port", 587),
        "username": rec.get("username", ""),
        "password": pw,
        "from_addr": USER_EMAILS.get(user, rec.get("username", "")),
    }

def send_outreach_email(user: str, to_addrs, subject: str, body_html: str, cc_addrs=None, bcc_addrs=None, attachments=None, add_read_receipts=False, tracking_pixel_url=None, tracking_id=None):
    cfg = get_user_mail_config(user)
    if not cfg or not cfg.get("username") or not cfg.get("password"):
        raise RuntimeError(f"No email credentials configured for {user}. Set a Gmail App Password in the sidebar.")

    msg = EmailMessage()
    msg["Subject"] = subject or ""
    msg["From"] = cfg["from_addr"]

    def _split(a):
        if not a:
            return []
        if isinstance(a, list):
            return a
        return [x.strip() for x in str(a).replace(";", ",").split(",") if x.strip()]

    to_list = _split(to_addrs)
    cc_list = _split(cc_addrs)
    bcc_list = _split(bcc_addrs)
    if not to_list:
        raise RuntimeError("Please provide at least one recipient in To.")

    msg["To"] = ", ".join(to_list)
    if cc_list: msg["Cc"] = ", ".join(cc_list)

    import re as _re
    plain = _re.sub("<[^<]+?>", "", body_html or "") if body_html else ""
    msg.set_content(plain or "(no content)")
    if body_html:
        msg.add_alternative(body_html, subtype="html")

    # Optional read receipts
    if add_read_receipts:
        # These headers work only if recipient mail server honors them
        msg["Disposition-Notification-To"] = cfg["from_addr"]
        msg["Return-Receipt-To"] = cfg["from_addr"]

    # Optional tracking pixel
    if tracking_pixel_url and body_html:
        try:
            import uuid, urllib.parse as _u
            tid = tracking_id or str(uuid.uuid4())
            qp = {"id": tid, "to": ",".join(to_list)}
            pixel = f'<img src="{tracking_pixel_url}?'+r'{'+'}'.replace('{','')+r'}" width="1" height="1" style="display:none;" />'.replace("{"+"}", "{_u.urlencode(qp)}")
            body_html = (body_html or "") + pixel
            # Replace the last HTML alternative with updated body_html
            msg.clear_content()
            plain = _re.sub("<[^<]+?>", "", body_html or "") if body_html else ""
            msg.set_content(plain or "(no content)")
            msg.add_alternative(body_html, subtype="html")
        except Exception:
            pass



    attachments = attachments or []
    for att in attachments:
        try:
            filename = getattr(att, "name", None)
            content = None

            # Streamlit UploadedFile or file-like object with getvalue or read
            if hasattr(att, "getvalue"):
                content = att.getvalue()
            elif hasattr(att, "read"):
                try:
                    att.seek(0)
                except Exception:
                    pass
                content = att.read()
            # Dict form: {"name": ..., "data": bytes} or {"path": ...}
            elif isinstance(att, dict):
                filename = att.get("name", filename or "file")
                if "data" in att and att["data"] is not None:
                    content = att["data"]
                elif "content" in att and att["content"] is not None:
                    val = att["content"]
                    content = val.getvalue() if hasattr(val, "getvalue") else (val.read() if hasattr(val, "read") else val)
                elif "path" in att:
                    import os
                    path = att["path"]
                    with open(path, "rb") as f:
                        content = f.read()
                    if not filename:
                        filename = os.path.basename(path)
            # Raw bytes
            elif isinstance(att, (bytes, bytearray)):
                content = bytes(att)
            # String path
            elif isinstance(att, str):
                import os
                if os.path.exists(att):
                    with open(att, "rb") as f:
                        content = f.read()
                    if not filename:
                        filename = os.path.basename(att)

            if content is None:
                raise ValueError("Unsupported attachment type")

            if not filename:
                filename = "attachment.bin"

            msg.add_attachment(content, maintype="application", subtype="octet-stream", filename=filename)
        except Exception as e:
            raise RuntimeError(f"Failed to attach {getattr(att,'name', getattr(att,'path', 'file'))}: {e}")

    all_rcpts = to_list + cc_list + bcc_list

    with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"]) as server:
        server.ehlo()
        server.starttls()
        server.login(cfg["username"], cfg["password"])
        server.send_message(msg, from_addr=cfg["from_addr"], to_addrs=all_rcpts)

def outreach_send_from_active_user(to, subject, body_html, cc=None, bcc=None, attachments=None):
    # ACTIVE_USER provided by your sign-in block
    return send_outreach_email(ACTIVE_USER, to, subject, body_html, cc_addrs=cc, bcc_addrs=bcc, attachments=attachments)
# === End Outreach helpers ===


# === Multi-user Sign-in & Session Isolation (added by ChatGPT on 2025-10-08) ===
from functools import wraps
import uuid

# Configure your users here
USERS = ["Quincy", "Charles", "Collin"]
# Optional PINs. Leave empty {} if you want passwordless sign-in.

PINS = {"Quincy": "1111", "Charles": "2222", "Collin": "3333"}

# --- Persistent PIN store (salted) ---
import json, os, secrets, hashlib

def _pin_storage_path():
    base = os.path.join(os.getcwd(), "secure_auth")
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "pins.json")

def _load_pin_store():
    path = _pin_storage_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_pin_store(store: dict):
    path = _pin_storage_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2)

def _hash_pin(pin: str, salt: str) -> str:
    return hashlib.sha256((salt + "|" + (pin or "")).encode("utf-8")).hexdigest()

def _get_or_init_pin_store():
    store = _load_pin_store()
    # Seed from PINS dict on first run for the defined USERS
    changed = False
    for u in USERS:
        if u not in store:
            salt = secrets.token_hex(16)
            store[u] = {"salt": salt, "hash": _hash_pin(PINS.get(u, ""), salt)}
            changed = True
    if changed:
        _save_pin_store(store)
    return store

def _verify_pin(user: str, pin: str) -> bool:
    store = _get_or_init_pin_store()
    rec = store.get(user)
    if not rec:
        return False
    return _hash_pin(pin or "", rec["salt"]) == rec["hash"]

def set_user_pin(user: str, new_pin: str):
    store = _get_or_init_pin_store()
    salt = secrets.token_hex(16)
    store[user] = {"salt": salt, "hash": _hash_pin(new_pin or "", salt)}
    _save_pin_store(store)

def _do_login():
    with st.sidebar:
        st.header("Sign in")
        user = st.selectbox("User", USERS, index=0, key="login_user_select")
        pin_ok = True
        if PINS:
            pin = st.text_input("PIN", type="password", key="login_pin_input")
            pin_ok = _verify_pin(user, pin)

        if st.button("Sign in", use_container_width=True, key="login_btn"):
            if pin_ok:
                st.session_state["active_user"] = user
                st.session_state.setdefault("private_mode", True)
                st.success(f"Signed in as {user}")
            else:
                st.error("Incorrect PIN")

    if "active_user" not in st.session_state:
        st.stop()

_do_login()
ACTIVE_USER = st.session_state["active_user"]

# --- Post-login controls: Sign out and Switch user ---
with st.sidebar:
    # Show current user and offer Sign out
    if st.session_state.get("active_user"):
        st.caption(f"Signed in as {st.session_state['active_user']}")
        if st.button("Sign out", use_container_width=True, key="logout_btn"):
            # Clear login and PIN and force re-run back to login screen
            st.session_state.pop("active_user", None)
            st.session_state.pop("login_pin_input", None)
            st.rerun()

# If the selection differs from the active user, offer a quick switch
_selected = st.session_state.get("login_user_select")
_active = st.session_state.get("active_user")
if _active and _selected and _selected != _active:
    with st.sidebar:
        st.warning(f"You selected {_selected}. To switch from {_active}, click below then sign in.")
        if st.button(f"Switch to {_selected}", use_container_width=True, key="switch_user_btn"):
            st.session_state.pop("active_user", None)  # this will trigger the login stop above on next run
            st.session_state.pop("login_pin_input", None)
            st.rerun()


# --- Namespaced session state helpers ---

# --- Unified Streamlit key helper (namespaced + duplicate-safe) ---
try:
    _NS_KEY_COUNTS
except NameError:
    _NS_KEY_COUNTS = {}
def ns_key(key: str) -> str:
    base = f"{ACTIVE_USER}::{key}"
    # increment and deduplicate within a single run
    c = _NS_KEY_COUNTS.get(base, 0) + 1
    _NS_KEY_COUNTS[base] = c
    if c == 1:
        return base
    return f"{base}__dup{c}"


class SessionNS:
    def __init__(self, user: str):
        self.user = user

    def _k(self, key: str) -> str:
        return f"{self.user}::{key}"

    def __getitem__(self, key: str):
        return st.session_state.get(self._k(key))

    def __setitem__(self, key: str, value):
        st.session_state[self._k(key)] = value

    def get(self, key: str, default=None):
        return st.session_state.get(self._k(key), default)

    def setdefault(self, key: str, default):
        return st.session_state.setdefault(self._k(key), default)

    def pop(self, key: str, default=None):
        return st.session_state.pop(self._k(key), default)

NS = SessionNS(ACTIVE_USER)

# --- Private workspace & publish queue ---
with st.sidebar:
    st.subheader("Workspace")

with st.sidebar:
    st.subheader("Security")
    with st.expander("Change My PIN", expanded=False):
        st.write("Update your sign-in PIN. New PIN must be 4–12 characters.")
        curr = st.text_input("Current PIN", type="password", key="pin_cur")
        new1 = st.text_input("New PIN", type="password", key="pin_new1")
        new2 = st.text_input("Confirm New PIN", type="password", key="pin_new2")
        if st.button("Update PIN", use_container_width=True, key="pin_update_btn"):
            if not _verify_pin(ACTIVE_USER, curr or ''):
                st.error("Current PIN is incorrect.")
            elif not new1 or len(new1) < 4 or len(new1) > 12:
                st.error("New PIN must be 4–12 characters.")
            elif new1 != new2:
                st.error("New PINs do not match.")
            else:
                set_user_pin(ACTIVE_USER, new1)
                # Clear any cached login input
                st.session_state.pop("login_pin_input", None)
                st.success("Your PIN has been updated. It will be required next time you sign in.")

    st.session_state.setdefault(f"{ACTIVE_USER}::private_mode", True)
    NS["private_mode"] = st.toggle(
        "Private mode",
        value=NS.get("private_mode", True),
        help="When ON your changes stay private to you until you publish."
    )

def queue_change(fn, *, label: str):
    """Queue a change for this user instead of writing to shared data immediately."""
    NS.setdefault("publish_queue", [])
    q = NS.get("publish_queue", [])
    q.append({"id": str(uuid.uuid4()), "label": label, "fn": fn})
    NS["publish_queue"] = q

def publish_changes():
    q = NS.get("publish_queue", [])
    errors = []
    for item in q:
        try:
            item["fn"]()
        except Exception as e:
            errors.append((item["label"], e))
    NS["publish_queue"] = []
    return errors

def write_or_queue(label, commit_fn):
    if NS.get("private_mode", True):
        queue_change(commit_fn, label=label)
        st.info(f"Saved privately. Publish later. [{label}]")
    else:
        commit_fn()
        st.success(f"Saved to team. [{label}]")

with st.sidebar:
    if st.button("Publish my changes", use_container_width=True, key="publish_btn"):
        errs = publish_changes()
        if not errs:
            st.success("All your private changes are now published to the team data.")
        else:
            st.error("Some changes failed to publish. See below for details.")
            for label, e in errs:
                st.exception(RuntimeError(f"{label}: {e}"))

# === End multi-user block ===

# === Outreach Email (per-user) — Gmail SMTP (added 2025-10-08) ===
# Supports per-user "From" emails, stored credentials, and a sidebar composer.
import smtplib
from email.message import EmailMessage
import base64

# Map users to their From addresses
USER_EMAILS = {
    "Quincy": "quincy.elamgmt@gmail.com",
    "Charles": "charles.elamgmt@gmail.com",
    "Collin": "collin.elamgmt@gmail.com",
}

def _mail_store_path():
    base = os.path.join(os.getcwd(), "secure_auth")
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "mail.json")

def _load_mail_store():
    path = _mail_store_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_mail_store(store: dict):
    path = _mail_store_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2)

def set_user_smtp_app_password(user: str, app_password: str):
    store = _load_mail_store()
    u = store.get(user, {})
    # Light obfuscation (not true encryption) — recommend using Gmail App Passwords
    u["smtp_host"] = "smtp.gmail.com"
    u["smtp_port"] = 587
    u["username"] = USER_EMAILS.get(user, "")
    u["app_password_b64"] = base64.b64encode((app_password or "").encode("utf-8")).decode("ascii")
    store[user] = u
    _save_mail_store(store)

def get_user_mail_config(user: str):
    store = _load_mail_store()
    rec = store.get(user, {})
    if not rec:
        return None
    pw = base64.b64decode(rec.get("app_password_b64", "").encode("ascii")).decode("utf-8") if rec.get("app_password_b64") else ""
    return {
        "smtp_host": rec.get("smtp_host", "smtp.gmail.com"),
        "smtp_port": rec.get("smtp_port", 587),
        "username": rec.get("username", ""),
        "password": pw,
        "from_addr": USER_EMAILS.get(user, rec.get("username", "")),
    }

def send_outreach_email(user: str, to_addrs, subject: str, body_html: str, cc_addrs=None, bcc_addrs=None, attachments=None, add_read_receipts=False, tracking_pixel_url=None, tracking_id=None):
    cfg = get_user_mail_config(user)
    if not cfg or not cfg.get("username") or not cfg.get("password"):
        raise RuntimeError(f"No email credentials configured for {user}. Set a Gmail App Password in the sidebar.")

    msg = EmailMessage()
    msg["Subject"] = subject or ""
    msg["From"] = cfg["from_addr"]
    # Parse address lists
    def _split(a):
        if not a:
            return []
        if isinstance(a, list):
            return a
        return [x.strip() for x in str(a).replace(";", ",").split(",") if x.strip()]

    to_list = _split(to_addrs)
    cc_list = _split(cc_addrs)
    bcc_list = _split(bcc_addrs)
    if not to_list:
        raise RuntimeError("Please provide at least one recipient in To.")

    msg["To"] = ", ".join(to_list)
    if cc_list: msg["Cc"] = ", ".join(cc_list)

    # HTML body; also set a plain text fallback
    from html import unescape
    plain = re.sub("<[^<]+?>", "", body_html or "") if body_html else ""
    msg.set_content(plain or "(no content)")
    if body_html:
        msg.add_alternative(body_html, subtype="html")

    # Optional read receipts
    if add_read_receipts:
        # These headers work only if recipient mail server honors them
        msg["Disposition-Notification-To"] = cfg["from_addr"]
        msg["Return-Receipt-To"] = cfg["from_addr"]

    # Optional tracking pixel
    if tracking_pixel_url and body_html:
        try:
            import uuid, urllib.parse as _u
            tid = tracking_id or str(uuid.uuid4())
            qp = {"id": tid, "to": ",".join(to_list)}
            pixel = f'<img src="{tracking_pixel_url}?'+r'{'+'}'.replace('{','')+r'}" width="1" height="1" style="display:none;" />'.replace("{"+"}", "{_u.urlencode(qp)}")
            body_html = (body_html or "") + pixel
            # Replace the last HTML alternative with updated body_html
            msg.clear_content()
            plain = _re.sub("<[^<]+?>", "", body_html or "") if body_html else ""
            msg.set_content(plain or "(no content)")
            msg.add_alternative(body_html, subtype="html")
        except Exception:
            pass


    # Attachments

    attachments = attachments or []
    for att in attachments:
        try:
            filename = getattr(att, "name", None)
            content = None

            # Streamlit UploadedFile or file-like object with getvalue or read
            if hasattr(att, "getvalue"):
                content = att.getvalue()
            elif hasattr(att, "read"):
                try:
                    att.seek(0)
                except Exception:
                    pass
                content = att.read()
            # Dict form: {"name": ..., "data": bytes} or {"path": ...}
            elif isinstance(att, dict):
                filename = att.get("name", filename or "file")
                if "data" in att and att["data"] is not None:
                    content = att["data"]
                elif "content" in att and att["content"] is not None:
                    val = att["content"]
                    content = val.getvalue() if hasattr(val, "getvalue") else (val.read() if hasattr(val, "read") else val)
                elif "path" in att:
                    import os
                    path = att["path"]
                    with open(path, "rb") as f:
                        content = f.read()
                    if not filename:
                        filename = os.path.basename(path)
            # Raw bytes
            elif isinstance(att, (bytes, bytearray)):
                content = bytes(att)
            # String path
            elif isinstance(att, str):
                import os
                if os.path.exists(att):
                    with open(att, "rb") as f:
                        content = f.read()
                    if not filename:
                        filename = os.path.basename(att)

            if content is None:
                raise ValueError("Unsupported attachment type")

            if not filename:
                filename = "attachment.bin"

            msg.add_attachment(content, maintype="application", subtype="octet-stream", filename=filename)
        except Exception as e:
            raise RuntimeError(f"Failed to attach {getattr(att,'name', getattr(att,'path', 'file'))}: {e}")

    all_rcpts = to_list + cc_list + bcc_list

    # Send via Gmail SMTP with STARTTLS (requires App Password on accounts with 2FA)
    with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"]) as server:
        server.ehlo()
        server.starttls()
        server.login(cfg["username"], cfg["password"])
        server.send_message(msg, from_addr=cfg["from_addr"], to_addrs=all_rcpts)


# --- Outreach Tools UI (moved from sidebar to Outreach tab to prevent bleed-through) ---




def _normalize_extra_files(files):
    """Normalize a list of attachments into dicts with name and raw bytes in data."""
    out = []
    try:
        for f in (files or []):
            # Already a normalized dict
            if isinstance(f, dict):
                name = f.get("name") or f.get("filename") or "file"
                if "data" in f and f["data"] is not None:
                    out.append({"name": name, "data": f["data"]})
                    continue
                if "content" in f and f["content"] is not None:
                    val = f["content"]
                    if isinstance(val, (bytes, bytearray)):
                        out.append({"name": name, "data": bytes(val)})
                    elif isinstance(val, str):
                        import os
                        if os.path.exists(val):
                            with open(val, "rb") as fh:
                                out.append({"name": name, "data": fh.read()})
                        else:
                            out.append({"name": name, "data": val.encode("utf-8")})
                    continue
                if "path" in f and f["path"]:
                    import os
                    path = f["path"]
                    try:
                        with open(path, "rb") as fh:
                            out.append({"name": name or os.path.basename(path), "data": fh.read()})
                    except Exception:
                        pass
                    continue

            # Streamlit UploadedFile or similar
            if hasattr(f, "getvalue"):
                out.append({"name": getattr(f, "name", "file"), "data": f.getvalue()})
                continue
            if hasattr(f, "read"):
                try:
                    f.seek(0)
                except Exception:
                    pass
                try:
                    data = f.read()
                    out.append({"name": getattr(f, "name", "file"), "data": data})
                    continue
                except Exception:
                    pass

            # File path string
            if isinstance(f, str):
                import os
                if os.path.exists(f):
                    try:
                        with open(f, "rb") as fh:
                            out.append({"name": os.path.basename(f), "data": fh.read()})
                        continue
                    except Exception:
                        pass
    except Exception:
        pass
    return out



def _log_contact_outreach(entries):
    """Append outreach log entries to data/contact_outreach_log.json"""
    try:
        import os, json, datetime
        base = os.path.join(os.getcwd(), "data")
        os.makedirs(base, exist_ok=True)
        path = os.path.join(base, "contact_outreach_log.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = []
        timestamp = datetime.datetime.utcnow().isoformat()+"Z"
        for e in entries or []:
            e.setdefault("ts_utc", timestamp)
        existing.extend(entries or [])
        with open(path, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
        return path
    except Exception:
        return None


def render_outreach_tools():
    import streamlit as st
    import streamlit.components.v1 as components
    # ---------- Helpers ----------
    def _normalize_sel_attachments(sel_atts):
        """Return a list of dicts with just 'name' for display when attachments in the generated item are names/dicts."""
        out = []
        base = sel_atts or []
        try:
            for a in base:
                if isinstance(a, dict) and ("name" in a or "filename" in a):
                    nm = a.get("name") or a.get("filename") or "attachment"
                    out.append({"name": nm})
                elif isinstance(a, str):
                    out.append({"name": a})
        except Exception:
            pass
        return out


        out = []
        try:
            for f in (files or []):
                # Already-normalized dict: pass through or convert
                if isinstance(f, dict):
                    name = f.get("name") or f.get("filename") or "file"
                    if "data" in f and f["data"] is not None:
                        out.append({"name": name, "data": f["data"]})
                    elif "content" in f and f["content"] is not None:
                        val = f["content"]
                        if isinstance(val, (bytes, bytearray)):
                            out.append({"name": name, "data": bytes(val)})
                        elif isinstance(val, str):
                            # If looks like a path, try to read from disk
                            import os
                            if os.path.exists(val):
                                with open(val, "rb") as fh:
                                    out.append({"name": name, "data": fh.read()})
                            else:
                                out.append({"name": name, "data": val.encode("utf-8")})
                    elif "path" in f and f["path"]:
                        import os
                        path = f["path"]
                        try:
                            with open(path, "rb") as fh:
                                out.append({"name": name or os.path.basename(path), "data": fh.read()})
                        except Exception:
                            pass
                    continue

                # Streamlit UploadedFile or similar
                if hasattr(f, "getvalue"):
                    out.append({"name": getattr(f, "name", "file"), "data": f.getvalue()})
                    continue
                if hasattr(f, "read"):
                    try:
                        f.seek(0)
                    except Exception:
                        pass
                    try:
                        data = f.read()
                        out.append({"name": getattr(f, "name", "file"), "data": data})
                        continue
                    except Exception:
                        pass

                # File path
                if isinstance(f, str):
                    import os
                    if os.path.exists(f):
                        try:
                            with open(f, "rb") as fh:
                                out.append({"name": os.path.basename(f), "data": fh.read()})
                            continue
                        except Exception:
                            pass
        except Exception:
            pass
        return out

    # Robust local sender that tries multiple implementations
    def _send_email(user, to, subject, body_html, cc="", bcc="", attachments=None):
        last_err = None
        # Preferred modern signature
        try:
            return send_outreach_email(user, to, subject, body_html,
                                       cc_addrs=cc, bcc_addrs=bcc, attachments=attachments)
        except Exception as e:
            last_err = e
        # Legacy fallback (active-user based)
        try:
            return outreach_send_from_active_user(to, subject, body_html,
                                                  cc=cc, bcc=bcc, attachments=attachments)
        except Exception as e:
            last_err = e
        # Optional extra names if your app exposes them
        for name in ("send_outreach_message", "send_gmail_message", "send_mail", "outreach_send"):
            fn = globals().get(name)
            if callable(fn):
                try:
                    return fn(user, to, subject, body_html, cc, bcc, attachments)
                except Exception as e:
                    last_err = e
        raise last_err or RuntimeError("No outreach sender is available")

    # ---------- Stable session keys ----------
    SKEY_PREVIEW = f"{ACTIVE_USER}::outreach::preview"             # snapshot for the Gmail-style preview card
    SKEY_ATTACH  = f"{ACTIVE_USER}::outreach::extra_attachments"   # extra attachments uploaded by user (UploadedFile list)
    SKEY_LASTSIG = f"{ACTIVE_USER}::outreach::last_loaded_sig"

    st.session_state.setdefault(SKEY_PREVIEW, None)
    st.session_state.setdefault(SKEY_ATTACH, [])

    st.session_state.setdefault(SKEY_LASTSIG, "")
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")

    # ---------- Header ----------
    with st.container(border=True):
        top_l, top_r = st.columns([3,2])
        with top_l:
            st.markdown("### ✉️ Outreach")
            st.caption(f"From: **{from_addr}**" if from_addr else "No email configured for this user.")
        with st.container(border=True):
            mode = st.radio("Send to", ["Vendors", "Contacts"], index=0, horizontal=True, key="outreach_mode")



    # ---- Contacts Outreach ----
    if mode == "Contacts":
        with st.container(border=True):
            st.markdown("#### Contacts")
            # Read receipts + tracking pixel options
            with st.expander("Delivery & Tracking options", expanded=False):
                want_rr = st.checkbox("Request read receipt headers (may prompt recipient)", value=False, key="outreach_rr")
                pixel_url = st.text_input("Optional tracking pixel URL (https://...)", value="", key="outreach_pixel_url")
            # Load contacts from CSV
            col_c1, col_c2 = st.columns([2,1])
            with col_c1:
                search = st.text_input("Search contacts", key="outreach_contact_search")
            with col_c2:
                uploaded = st.file_uploader("", type=["csv"], key="outreach_contacts_csv")
            contacts = []
            import os, csv
            # Prefer uploaded CSV
            if uploaded is not None:
                try:
                    txt = uploaded.getvalue().decode("utf-8", errors="ignore")
                    for row in csv.DictReader(txt.splitlines()):
                        nm = row.get("name") or row.get("Name") or row.get("full_name") or ""
                        em = row.get("email") or row.get("Email") or row.get("mail") or ""
                        if em:
                            contacts.append({"name": nm, "email": em})
                except Exception:
                    pass
            else:
                # Try default data/contacts.csv
                try:
                    path = os.path.join(os.getcwd(), "data", "contacts.csv")
                    if os.path.exists(path):
                        with open(path, "r", encoding="utf-8") as f:
                            for row in csv.DictReader(f):
                                nm = row.get("name") or row.get("Name") or row.get("full_name") or ""
                                em = row.get("email") or row.get("Email") or row.get("mail") or ""
                                if em:
                                    contacts.append({"name": nm, "email": em})
                except Exception:
                    pass

            # Filter by search
            s = (search or "").lower().strip()
            if s:
                contacts = [c for c in contacts if s in (c.get("name","")+c.get("email","")).lower()]

            # Options
            labels = [f'{c.get("name") or ""} <{c["email"]}>' if c.get("name") else c["email"] for c in contacts]
            selected = st.multiselect("Recipients", labels, key="outreach_contact_sel")

            subj = st.text_input("Subject", key="outreach_contact_subject")
            body = st.text_area("Body (HTML allowed)", key="outreach_contact_body", height=220)
            c_files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key="outreach_contact_files")

            if st.button("Send to selected contacts", use_container_width=True, key="outreach_contact_send"):
                emails = []
                label_to_email = {}
                for c, lbl in zip(contacts, labels):
                    label_to_email[lbl] = c["email"]
                for lbl in selected:
                    em = label_to_email.get(lbl)
                    if em:
                        emails.append(em)
                if not emails:
                    st.warning("Select at least one contact.")
                elif not subj or not body:
                    st.warning("Subject and body are required.")
                else:
                    # Normalize files
                    atts = _normalize_extra_files(c_files)
                    # Tracking id per batch
                    import uuid
                    batch_id = str(uuid.uuid4())
                    failures = []
                    sent = 0
                    for em in emails:
                        try:
                            send_outreach_email(
                                ACTIVE_USER, [em], subj, body,
                                cc_addrs=None, bcc_addrs=None, attachments=atts,
                                add_read_receipts=want_rr, tracking_pixel_url=(pixel_url or None),
                                tracking_id=batch_id + "::" + em
                            )
                            sent += 1
                        except Exception as e:
                            failures.append((em, str(e)))
                    # Log
                    _log_contact_outreach([{"mode":"contacts","to": em, "subject": subj, "batch_id": batch_id} for em in emails])
                    if failures:
                        st.error(f"Sent {sent} / {len(emails)}. Failures: " + "; ".join([f"{a} ({b})" for a,b in failures]))
                    else:
                        st.success(f"Sent {sent} / {len(emails)}")
        # Stop rendering vendor section if Contacts mode
        return


        with top_r:
            pass

    # ---- Account: App Password (still here) ----
    with st.expander("Set/Update my Gmail App Password", expanded=False):
        pw = st.text_input("Gmail App Password", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            try:
                set_user_smtp_app_password(ACTIVE_USER, pw)
                st.success("Saved")
            except Exception as e:
                st.error(f"Failed to save: {e}")

    st.divider()

    # ---------- Choose Generated Email & Attachments (required) ----------
    with st.container(border=True):
        st.markdown("#### Choose Generated Email")
        mb = st.session_state.get("mail_bodies") or []
        if not mb:
            st.info("Generate emails to select one for preview.", icon="ℹ️")
        else:
            idx = st.number_input("Select one", min_value=1, max_value=len(mb), value=len(mb), step=1,
                                  key=ns_key("outreach::pick_idx"))
            sel = mb[int(idx)-1]

            # Show key fields from the generated email
            st.caption(f"**To:** {sel.get('to','')}")
            st.caption(f"**Subject:** {sel.get('subject','')}")
            scope_disp = sel.get("scope_summary") or sel.get("scope") or ""
            due_disp = sel.get("quote_due") or sel.get("due") or ""
            meta_cols = st.columns(2)
            with meta_cols[0]:
                st.markdown(f"**Scope Summary:** {scope_disp}")
            with meta_cols[1]:
                st.markdown(f"**Quote Due:** {due_disp}")

            # Attachments uploader (REQUIRED) placed below Quote Due
            extra_files = st.file_uploader("Attachments (required)", type=None, accept_multiple_files=True,
                                           key=ns_key("outreach::extra_files"))
            if extra_files is not None:
                st.session_state[SKEY_ATTACH] = extra_files

            # Generate preview button
            if st.button("Generate preview", key=ns_key("outreach::gen_preview"), use_container_width=True):
                files = st.session_state.get(SKEY_ATTACH) or []
                if not files:
                    st.warning("Please upload at least one attachment before generating the preview.")
                else:
                    # Build display names from generated attachments + uploaded files
                    gen_names = _normalize_sel_attachments(sel.get("attachments"))
                    try:
                        upload_names = [{"name": getattr(f, "name", "file")} for f in files]
                    except Exception:
                        upload_names = []
                    st.session_state[SKEY_PREVIEW] = {
                        "to": sel.get("to",""),
                        "cc": sel.get("cc",""),
                        "bcc": sel.get("bcc",""),
                        "subject": sel.get("subject",""),
                        "body_html": sel.get("body",""),
                        "from_addr": USER_EMAILS.get(ACTIVE_USER, ""),
                        "scope_summary": scope_disp,
                        "quote_due": due_disp,
                        "attachments": (gen_names or []) + (upload_names or [])
                    }
                    st.success("Preview generated below.")


            actions2 = st.columns([1, 2, 2, 5])
            with actions2[1]:
                if st.button("Send selected now", key=ns_key("outreach::send_selected_now"), use_container_width=True):
                    files = st.session_state.get(SKEY_ATTACH) or []
                    if not files:
                        st.warning("Please upload at least one attachment before sending.")
                    else:
                        try:
                            merged_atts = _normalize_sel_attachments(sel.get("attachments")) + _normalize_extra_files(files)
                            _send_email(
                                ACTIVE_USER,
                                sel.get("to",""),
                                sel.get("subject",""),
                                sel.get("body",""),
                                cc=sel.get("cc",""),
                                bcc=sel.get("bcc",""),
                                attachments=merged_atts
                            )
                            st.success("Selected email sent.")
                        except Exception as e:
                            st.error(f"Failed to send selected: {e}")
            with actions2[2]:
                if st.button("Send ALL generated now", key=ns_key("outreach::send_all_now"), use_container_width=True):
                    files = st.session_state.get(SKEY_ATTACH) or []
                    if not files:
                        st.warning("Please upload at least one attachment before mass sending.")
                    else:
                        mb_all = st.session_state.get("mail_bodies") or []
                        sent = 0
                        failures = []
                        for i, itm in enumerate(mb_all, start=1):
                            try:
                                merged_atts = _normalize_sel_attachments(itm.get("attachments")) + _normalize_extra_files(files)
                                _send_email(
                                    ACTIVE_USER,
                                    itm.get("to",""),
                                    itm.get("subject",""),
                                    itm.get("body",""),
                                    cc=itm.get("cc",""),
                                    bcc=itm.get("bcc",""),
                                    attachments=merged_atts
                                )
                                sent += 1
                            except Exception as e:
                                failures.append((i, itm.get("subject",""), str(e)))
                        if failures:
                            st.error(f"Sent {sent} / {len(mb_all)}. Failures: " + "; ".join([f"#{i} {subj} ({err})" for i, subj, err in failures]))
                        else:
                            st.success(f"Sent all {sent} generated emails.")# ---------- Single Preview (Gmail-like card) ---------- (Gmail-like card) ----------
    snap = st.session_state.get(SKEY_PREVIEW)
    with st.container(border=True):
        st.markdown("#### Preview")
        if not snap:
            st.info("Select a generated email above, attach files if needed, and click Preview.", icon="ℹ️")
        else:
            # Header block similar to Gmail
            hdr_lines = []
            if snap.get("from_addr"): hdr_lines.append(f"<div><b>From:</b> {snap['from_addr']}</div>")
            if snap.get("to"):        hdr_lines.append(f"<div><b>To:</b> {snap['to']}</div>")
            if snap.get("cc"):        hdr_lines.append(f"<div><b>Cc:</b> {snap['cc']}</div>")
            if snap.get("bcc"):       hdr_lines.append(f"<div><b>Bcc:</b> {snap['bcc']}</div>")
            if snap.get("subject"):   hdr_lines.append(f"<div style='font-size:16px;margin-top:4px;'><b>Subject:</b> {snap['subject']}</div>")

            # Meta row: Scope Summary & Quote Due
            meta_bits = []
            if snap.get("scope_summary"):
                meta_bits.append("<div style='display:inline-block;border:1px solid #eee;"
                                 "padding:4px 8px;border-radius:8px;margin-right:8px;'><b>Scope:</b> "
                                 f"{snap['scope_summary']}</div>")
            if snap.get("quote_due"):
                meta_bits.append("<div style='display:inline-block;border:1px solid #eee;"
                                 "padding:4px 8px;border-radius:8px;'><b>Quote due:</b> "
                                 f"{snap['quote_due']}</div>")


            # Attachments uploader (positioned below Quote Due)
            extra_files = st.file_uploader("Attachments (required)", type=None, accept_multiple_files=True,
                                           key=ns_key("outreach::extra_files"))
            if extra_files is not None:
                st.session_state[SKEY_ATTACH] = extra_files

            # Body
            body_html = (snap.get("body_html") or "").strip() or "<p><i>(No body content)</i></p>"

            # Attachments display
            atts_html = ""
            atts = snap.get("attachments") or []
            if atts:
                items = "".join([f"<li>{(a.get('name') if isinstance(a,dict) else str(a))}</li>" for a in atts])
                atts_html = ("<div style='margin-top:8px;'><b>Attachments:</b>"
                             f"<ul style='margin:6px 0 0 20px;'>{items}</ul></div>")

            components.html(f"""
                <div style="border:1px solid #ddd;border-radius:8px;padding:14px;">
                    <div style="margin-bottom:8px;">{''.join(hdr_lines)}</div>
                    <div style="margin-bottom:8px;">{''.join(meta_bits)}</div>
                    <div style="border:1px solid #eee;padding:10px;border-radius:6px;">{body_html}</div>
                    {atts_html}
                </div>
            """, height=520, scrolling=True)

            # Actions under the preview
            a1, a2 = st.columns(2)
            with a1:
                if st.button("Send email", key=ns_key("outreach::send_from_preview"), use_container_width=True):
                    try:
                        _send_email(
                            ACTIVE_USER,
                            snap.get("to",""),
                            snap.get("subject",""),
                            snap.get("body_html",""),
                            cc=snap.get("cc",""),
                            bcc=snap.get("bcc",""),
                            attachments=st.session_state.get(SKEY_ATTACH) or []
                        )
                        st.success("Email sent.")
                        st.session_state[SKEY_PREVIEW] = None
                    except Exception as e:
                        st.error(f"Failed to send: {e}")
            with a2:
                if st.button("Close preview", key=ns_key("outreach::close_preview"), use_container_width=True):
                    st.session_state[SKEY_PREVIEW] = None

def load_outreach_preview(to="", cc="", bcc="", subject="", html=""):
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    key = lambda k: f"{ACTIVE_USER}::outreach::{k}"
    st.session_state[key("to")] = to or ""
    st.session_state[key("cc")] = cc or ""
    st.session_state[key("bcc")] = bcc or ""
    st.session_state[key("subj")] = subject or ""
    st.session_state[key("body")] = html or ""
    st.session_state[key("preview")] = {
        "to": to or "",
        "cc": cc or "",
        "bcc": bcc or "",
        "subject": subject or "",
        "body_html": html or "",
        "attachments": [],
        "from_addr": from_addr,
    }

    st.subheader("Email – Outreach")
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    if not from_addr:
        st.caption("No email configured for this user. Only Charles and Collin are set up.")
    else:
        st.caption(f"From: {from_addr}")

    st.session_state.setdefault(ns_key("outreach::mail_preview_data"), None)

    hc1, hc2, hc3 = st.columns([1,1,2])
    with hc1:
        if st.button("Preview current draft", key=ns_key("outreach::hdr_preview_btn")):
            to = st.session_state.get(ns_key("outreach::mail_to"), "") or ""
            cc = st.session_state.get(ns_key("outreach::mail_cc"), "") or ""
            bcc = st.session_state.get(ns_key("outreach::mail_bcc"), "") or ""
            subj = st.session_state.get(ns_key("outreach::mail_subj"), "") or ""
            body = st.session_state.get(ns_key("outreach::mail_body"), "") or ""
            atts = (st.session_state.get(ns_key("outreach::mail_preview_data")) or {}).get("attachments", [])
            st.session_state[ns_key("outreach::mail_preview_data")] = {
                "to": to,
                "cc": cc,
                "bcc": bcc,
                "subject": subj,
                "body_html": body,
                "attachments": atts,
                "from_addr": from_addr,
            }
    with hc2:
        if st.button("Clear preview", key=ns_key("outreach::hdr_preview_clear")):
            st.session_state[ns_key("outreach::mail_preview_data")] = None

    with st.expander("Set/Update my Gmail App Password", expanded=False):
        st.caption("Generate an App Password in your Google Account > Security > 2-Step Verification.")
        app_pw = st.text_input("Gmail App Password (16 chars, no spaces)", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            set_user_smtp_app_password(ACTIVE_USER, app_pw)
            st.success("Saved. You can now send emails from the Outreach composer.")

    with st.expander("Quick Outreach Composer", expanded=False):
        to = st.text_input("To (comma-separated)", key=ns_key("outreach::mail_to"),
                           placeholder="recipient@example.com, another@domain.com")
        cc = st.text_input("Cc (optional, comma-separated)", key=ns_key("outreach::mail_cc"))
        bcc = st.text_input("Bcc (optional, comma-separated)", key=ns_key("outreach::mail_bcc"))
        subj = st.text_input("Subject", key=ns_key("outreach::mail_subj"))
        body = st.text_area("Message (HTML supported)", key=ns_key("outreach::mail_body"), height=200,
                            placeholder="<p>Hello.</p>")
        files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key=ns_key("outreach::mail_files"))

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Preview email", use_container_width=True, key=ns_key("outreach::mail_preview_btn")):
                atts = []
                try:
                    for f in (files or []):
                        try:
                            atts.append({"name": getattr(f, "name", "file"), "data": f.getvalue()})
                        except Exception:
                            pass
                except Exception:
                    atts = []
                st.session_state[ns_key("outreach::mail_preview_data")] = {
                    "to": to or "",
                    "cc": cc or "",
                    "bcc": bcc or "",
                    "subject": subj or "",
                    "body_html": body or "",
                    "attachments": atts,
                    "from_addr": from_addr,
                }
        with c2:
            if st.button("Send email", use_container_width=True, key=ns_key("outreach::mail_send_btn")):
                try:
                    send_outreach_email(ACTIVE_USER, to, subj, body, cc_addrs=cc, bcc_addrs=bcc, attachments=files)
                    st.success("Email sent.")
                    for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                        NS.pop(k, None)
                except Exception as e:
                    st.error(f"Failed to send: {e}")

    preview = st.session_state.get(ns_key("outreach::mail_preview_data"))
    if preview:
        import streamlit.components.v1 as components
        with st.container(border=True):
            st.markdown("#### Email preview")
            st.markdown(f"**From:** {preview.get('from_addr','')}")
            if preview.get("to"): st.markdown(f"**To:** {preview['to']}")
            if preview.get("cc"): st.markdown(f"**Cc:** {preview['cc']}")
            if preview.get("bcc"): st.markdown(f"**Bcc:** {preview['bcc']}")
            st.markdown(f"**Subject:** {preview.get('subject','')}")
            html = preview.get("body_html") or ""
            components.html(
                f"""
                <div style="border:1px solid #ddd;padding:16px;margin-top:8px;">
                    {html}
                </div>
                """,
                height=400,
                scrolling=True,
            )
        atts = preview.get("attachments") or []
        if atts:
            names = [a.get("name","file") for a in atts]
            st.caption("Attachments: " + ", ".join(names))

        cc1, cc2, _ = st.columns([1,1,2])
        with cc1:
            if st.button("Send this email", key=ns_key("outreach::mail_preview_confirm")):
                class _MemFile:
                    def __init__(self, name, data):
                        self.name = name
                        self._data = data
                    def getvalue(self):
                        return self._data
                mem_files = [_MemFile(a.get("name","file"), a.get("data", b"")) for a in atts]
                try:
                    send_outreach_email(
                        ACTIVE_USER,
                        preview.get("to",""),
                        preview.get("subject",""),
                        preview.get("body_html",""),
                        cc_addrs=preview.get("cc",""),
                        bcc_addrs=preview.get("bcc",""),
                        attachments=mem_files
                    )
                    st.success("Email sent.")
                    st.session_state[ns_key("outreach::mail_preview_data")] = None
                    for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                        NS.pop(k, None)
                except Exception as e:
                    st.error(f"Failed to send: {e}")
        with cc2:
            if st.button("Close preview", key=ns_key("outreach::mail_preview_close")):
                st.session_state[ns_key("outreach::mail_preview_data")] = None
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    if not from_addr:
        st.caption("No email configured for this user. Only Charles and Collin are set up.")
    else:
        st.caption(f"From: {from_addr}")

    # Global preview state
    st.session_state.setdefault(ns_key("outreach::mail_preview_data"), None)

    # === Header-level controls ===
    hc1, hc2, hc3 = st.columns([1,1,2])
    with hc1:
        if st.button("Preview current draft", key=ns_key("outreach::hdr_preview_btn")):
            # Pull current draft values from session, even if the composer expander is closed
            to = st.session_state.get(ns_key("outreach::mail_to"), "") or ""
            cc = st.session_state.get(ns_key("outreach::mail_cc"), "") or ""
            bcc = st.session_state.get(ns_key("outreach::mail_bcc"), "") or ""
            subj = st.session_state.get(ns_key("outreach::mail_subj"), "") or ""
            body = st.session_state.get(ns_key("outreach::mail_body"), "") or ""
            # Attachments are not easily accessible from header because uploader holds file objects;
            # keep whatever was already captured if a composer preview was taken, else empty.
            atts = (st.session_state.get(ns_key("outreach::mail_preview_data")) or {}).get("attachments", [])

            st.session_state[ns_key("outreach::mail_preview_data")] = {
                "to": to, "cc": cc, "bcc": bcc,
                "subject": subj,
                "body_html": body,
                "attachments": atts,
                "from_addr": from_addr,
            }
    with hc2:
        if st.button("Clear preview", key=ns_key("outreach::hdr_preview_clear")):
            st.session_state[ns_key("outreach::mail_preview_data")] = None

    with st.expander("Set/Update my Gmail App Password", expanded=False):
        st.caption("Generate an App Password in your Google Account > Security > 2-Step Verification.")
        app_pw = st.text_input("Gmail App Password (16 chars, no spaces)", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            set_user_smtp_app_password(ACTIVE_USER, app_pw)
            st.success("Saved. You can now send emails from the Outreach composer.")

    # === Quick Outreach Composer ===
    with st.expander("Quick Outreach Composer", expanded=False):
        to = st.text_input("To (comma-separated)",
                           key=ns_key("outreach::mail_to"),
                           placeholder="recipient@example.com, another@domain.com")
        cc = st.text_input("Cc (optional, comma-separated)", key=ns_key("outreach::mail_cc"))
        bcc = st.text_input("Bcc (optional, comma-separated)", key=ns_key("outreach::mail_bcc"))
        subj = st.text_input("Subject", key=ns_key("outreach::mail_subj"))
        body = st.text_area("Message (HTML supported)", key=ns_key("outreach::mail_body"), height=200,
                            placeholder="<p>Hello.</p>")
        files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key=ns_key("outreach::mail_files"))

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Preview email", use_container_width=True, key=ns_key("outreach::mail_preview_btn")):
                # Snapshot current fields (including attachments) for a pixel-accurate preview
                atts = []
                try:
                    for f in (files or []):
                        try:
                            atts.append({"name": getattr(f, "name", "file"), "data": f.getvalue()})
                        except Exception:
                            pass
                except Exception:
                    atts = []
                st.session_state[ns_key("outreach::mail_preview_data")] = {
                    "to": to or "",
                    "cc": cc or "",
                    "bcc": bcc or "",
                    "subject": subj or "",
                    "body_html": body or "",
                    "attachments": atts,
                    "from_addr": from_addr,
                }
        with c2:
            if st.button("Send email", use_container_width=True, key=ns_key("outreach::mail_send_btn")):
                try:
                    send_outreach_email(ACTIVE_USER, to, subj, body, cc_addrs=cc, bcc_addrs=bcc, attachments=files)
                    st.success("Email sent.")
                    for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                        NS.pop(k, None)
                except Exception as e:
                    st.error(f"Failed to send: {e}")

    # === Unified Preview Block (used by both header-level and composer-level triggers) ===
    preview = st.session_state.get(ns_key("outreach::mail_preview_data"))
    if preview:
        import streamlit.components.v1 as components
        with st.container(border=True):
            st.markdown("#### Email preview")
            st.markdown(f"**From:** {preview.get('from_addr','')}")
            if preview.get("to"):
                st.markdown(f"**To:** {preview['to']}")
            if preview.get("cc"):
                st.markdown(f"**Cc:** {preview['cc']}")
            if preview.get("bcc"):
                st.markdown(f"**Bcc:** {preview['bcc']}")
            st.markdown(f"**Subject:** {preview.get('subject','')}")

            html = preview.get("body_html") or ""
            components.html(
                f"""
                <div style="border:1px solid #ddd;padding:16px;margin-top:8px;">
                    {html}
                </div>
                """,
                height=400,
                scrolling=True,
            )

            atts = preview.get("attachments") or []
            if atts:
                names = [a.get("name","file") for a in atts]
                st.caption("Attachments: " + ", ".join(names))

            cc1, cc2, cc3 = st.columns([1,1,2])
            with cc1:
                if st.button("Send this email", key=ns_key("outreach::mail_preview_confirm")):
                    class _MemFile:
                        def __init__(self, name, data):
                            self.name = name
                            self._data = data
                        def getvalue(self):
                            return self._data
                    mem_files = [_MemFile(a.get("name","file"), a.get("data", b"")) for a in atts]
                    try:
                        send_outreach_email(
                            ACTIVE_USER,
                            preview.get("to",""),
                            preview.get("subject",""),
                            preview.get("body_html",""),
                            cc_addrs=preview.get("cc",""),
                            bcc_addrs=preview.get("bcc",""),
                            attachments=mem_files
                        )
                        st.success("Email sent.")
                        st.session_state[ns_key("outreach::mail_preview_data")] = None
                        for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                            NS.pop(k, None)
                    except Exception as e:
                        st.error(f"Failed to send: {e}")
            with cc2:
                if st.button("Close preview", key=ns_key("outreach::mail_preview_close")):
                    st.session_state[ns_key("outreach::mail_preview_data")] = None
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    if not from_addr:
        st.caption("No email configured for this user. Only Charles and Collin are set up.")
    else:
        st.caption(f"From: {from_addr}")

    with st.expander("Set/Update my Gmail App Password", expanded=False):
        st.caption("Generate an App Password in your Google Account > Security > 2-Step Verification.")
        app_pw = st.text_input("Gmail App Password (16 chars, no spaces)", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            set_user_smtp_app_password(ACTIVE_USER, app_pw)
            st.success("Saved. You can now send emails from the Outreach composer.")

    # Preview state
    st.session_state.setdefault(ns_key("outreach::mail_preview_data"), None)

    with st.expander("Quick Outreach Composer", expanded=False):
        to = st.text_input("To (comma-separated)",
                           key=ns_key("outreach::mail_to"),
                           placeholder="recipient@example.com, another@domain.com")
        cc = st.text_input("Cc (optional, comma-separated)", key=ns_key("outreach::mail_cc"))
        bcc = st.text_input("Bcc (optional, comma-separated)", key=ns_key("outreach::mail_bcc"))
        subj = st.text_input("Subject", key=ns_key("outreach::mail_subj"))
        body = st.text_area("Message (HTML supported)", key=ns_key("outreach::mail_body"), height=200,
                            placeholder="<p>Hello.</p>")
        files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key=ns_key("outreach::mail_files"))

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Preview email", use_container_width=True, key=ns_key("outreach::mail_preview_btn")):
                # Store a snapshot of the compose fields in session so a rerun preserves the preview
                # For attachments, store name and raw bytes so we can reconstruct file-like objects later.
                atts = []
                try:
                    for f in (files or []):
                        try:
                            atts.append({
                                "name": getattr(f, "name", "file"),
                                "data": f.getvalue()
                            })
                        except Exception:
                            pass
                except Exception:
                    atts = []
                st.session_state[ns_key("outreach::mail_preview_data")] = {
                    "to": to or "",
                    "cc": cc or "",
                    "bcc": bcc or "",
                    "subject": subj or "",
                    "body_html": body or "",
                    "attachments": atts,
                    "from_addr": from_addr,
                }
        with c2:
            if st.button("Send email", use_container_width=True, key=ns_key("outreach::mail_send_btn")):
                try:
                    send_outreach_email(ACTIVE_USER, to, subj, body, cc_addrs=cc, bcc_addrs=bcc, attachments=files)
                    st.success("Email sent.")
                    for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                        NS.pop(k, None)
                except Exception as e:
                    st.error(f"Failed to send: {e}")

    # If a preview has been requested, render it exactly like the HTML body will appear.
    preview = st.session_state.get(ns_key("outreach::mail_preview_data"))
    if preview:
        import streamlit.components.v1 as components

        with st.container(border=True):
            st.markdown("#### Email preview")
            # Header preview
            st.markdown(f"**From:** {preview.get('from_addr','')}")
            if preview.get("to"):
                st.markdown(f"**To:** {preview['to']}")
            if preview.get("cc"):
                st.markdown(f"**Cc:** {preview['cc']}")
            if preview.get("bcc"):
                st.markdown(f"**Bcc:** {preview['bcc']}")
            st.markdown(f"**Subject:** {preview.get('subject','')}")

            # Render the HTML body using a component so styles and tags are honored
            html = preview.get("body_html") or ""
            components.html(
                f"""
                <div style="border:1px solid #ddd;padding:16px;margin-top:8px;">
                    {html}
                </div>
                """,
                height=400,
                scrolling=True,
            )

            # Show attachment list if any
            atts = preview.get("attachments") or []
            if atts:
                names = [a.get("name","file") for a in atts]
                st.caption("Attachments: " + ", ".join(names))

            # Confirm send buttons
            cc1, cc2, cc3 = st.columns([1,1,2])
            with cc1:
                if st.button("Send this email", key=ns_key("outreach::mail_preview_confirm")):
                    # Rebuild simple in memory files compatible with send_outreach_email expectations
                    class _MemFile:
                        def __init__(self, name, data):
                            self.name = name
                            self._data = data
                        def getvalue(self):
                            return self._data
                    mem_files = [_MemFile(a.get("name","file"), a.get("data", b"")) for a in atts]
                    try:
                        send_outreach_email(
                            ACTIVE_USER,
                            preview.get("to",""),
                            preview.get("subject",""),
                            preview.get("body_html",""),
                            cc_addrs=preview.get("cc",""),
                            bcc_addrs=preview.get("bcc",""),
                            attachments=mem_files
                        )
                        st.success("Email sent.")
                        st.session_state[ns_key("outreach::mail_preview_data")] = None
                        # Clear compose fields
                        for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                            NS.pop(k, None)
                    except Exception as e:
                        st.error(f"Failed to send: {e}")
            with cc2:
                if st.button("Close preview", key=ns_key("outreach::mail_preview_close")):
                    st.session_state[ns_key("outreach::mail_preview_data")] = None
    from_addr = USER_EMAILS.get(ACTIVE_USER, "")
    if not from_addr:
        st.caption("No email configured for this user. Only Charles and Collin are set up.")
    else:
        st.caption(f"From: {from_addr}")

    with st.expander("Set/Update my Gmail App Password", expanded=False):
        st.caption("Generate an App Password in your Google Account > Security > 2-Step Verification.")
        app_pw = st.text_input("Gmail App Password (16 chars, no spaces)", type="password", key=ns_key("outreach::gmail_app_pw"))
        if st.button("Save App Password", key=ns_key("outreach::save_app_pw")):
            set_user_smtp_app_password(ACTIVE_USER, app_pw)
            st.success("Saved. You can now send emails from the Outreach composer.")

    with st.expander("Quick Outreach Composer", expanded=False):
        to = st.text_input("To (comma-separated)",
                           key=ns_key("outreach::mail_to"),
                           placeholder="recipient@example.com, another@domain.com")
        cc = st.text_input("Cc (optional, comma-separated)", key=ns_key("outreach::mail_cc"))
        bcc = st.text_input("Bcc (optional, comma-separated)", key=ns_key("outreach::mail_bcc"))
        subj = st.text_input("Subject", key=ns_key("outreach::mail_subj"))
        body = st.text_area("Message (HTML supported)", key=ns_key("outreach::mail_body"), height=200,
                            placeholder="<p>Hello...</p>")
        files = st.file_uploader("Attachments", type=None, accept_multiple_files=True, key=ns_key("outreach::mail_files"))
        if st.button("Send email", use_container_width=True, key=ns_key("outreach::mail_send_btn")):
            try:
                send_outreach_email(ACTIVE_USER, to, subj, body, cc_addrs=cc, bcc_addrs=bcc, attachments=files)
                st.success("Email sent.")
                for k in ["outreach::mail_to","outreach::mail_cc","outreach::mail_bcc","outreach::mail_subj","outreach::mail_body","outreach::mail_files"]:
                    NS.pop(k, None)
            except Exception as e:
                st.error(f"Failed to send: {e}")

def outreach_send_from_active_user(to, subject, body_html, cc=None, bcc=None, attachments=None):
    return send_outreach_email(ACTIVE_USER, to, subject, body_html, cc_addrs=cc, bcc_addrs=bcc, attachments=attachments)
# === End Outreach Email block (moved) ===




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


# ---- Hoisted SAM helper (duplicate for e# (early use) ----

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



# ---- Hoisted helper implementations (duplicate for e# === SAM Watch → Contacts auto sync helpers ===

def _contacts_upsert(name: str = "", org: str = "", role: str = "", email: str = "", phone: str = "", source: str = "", notes: str = "") -> tuple:
    # Insert or light update into contacts.
    # Returns (action, id) where action is "insert" or "update".
    # Upsert rule prefers email match. If no email then uses name and org.
    try:
        conn = get_db(); cur = conn.cursor()
    except Exception:
        return ("error", None)

    email = (email or "").strip()
    name = (name or "").strip()
    org = (org or "").strip()
    role = (role or "").strip()
    phone = (phone or "").strip()
    source = (source or "SAM.gov").strip() or "SAM.gov"
    notes = (notes or "").strip()

    row = None
    try:
        if email:
            row = cur.execute("select id from contacts where lower(ifnull(email,'')) = lower(?) limit 1", (email,)).fetchone()
        if not row and (name and org):
            row = cur.execute("select id from contacts where lower(ifnull(name,''))=lower(?) and lower(ifnull(org,''))=lower(?) limit 1", (name, org)).fetchone()
    except Exception:
        row = None

    if row:
        cid = int(row[0])
        try:
            cur.execute(
                "update contacts set name=coalesce(nullif(?, ''), name), org=coalesce(nullif(?, ''), org), role=coalesce(nullif(?, ''), role), email=coalesce(nullif(?, ''), email), phone=coalesce(nullif(?, ''), phone), source=coalesce(nullif(?, ''), source), notes=case when ifnull(notes,'')='' then ? else notes end where id=?",
                (name, org, role, email, phone, source, notes, cid)
            )
            conn.commit()
        except Exception:
            pass
        return ("update", cid)

    try:
        cur.execute(
            "insert into contacts(name, org, role, email, phone, source, notes) values(?,?,?,?,?,?,?)",
            (name, org, role, email, phone, source, notes)
        )
        conn.commit()
        return ("insert", cur.lastrowid)
    except Exception:
        return ("error", None)


def _extract_contacts_from_sam_row(r) -> list:
    # Best effort extraction of POC and CO from a SAM Watch DataFrame row.
    # Returns list of dicts suitable for _contacts_upsert.
    def _g(keys):
        for k in keys:
            try:
                v = r.get(k)
            except Exception:
                v = None
            if v not in (None, float("nan")):
                s = str(v).strip()
                if s:
                    return s
        return ""

    import re
    agency = _g(["agency", "office", "department", "organization"]) or ""

    poc_name = _g(["poc_name", "primary_poc_name", "pointOfContact", "primaryPointOfContact", "contact_name"]) or ""
    poc_email = _g(["poc_email", "primary_poc_email", "pointOfContactEmail", "contact_email"]) or ""
    poc_phone = _g(["poc_phone", "primary_poc_phone", "pointOfContactPhone", "contact_phone"]) or ""

    co_name = _g(["co_name", "contracting_officer", "contractingOfficer", "buyer_name"]) or ""
    co_email = _g(["co_email", "contracting_officer_email", "buyer_email"]) or ""
    co_phone = _g(["co_phone", "contracting_officer_phone", "buyer_phone"]) or ""

    blob = _g(["description", "summary", "text", "body"]) or ""
    emails = []
    if blob:
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", blob)

    out = []
    if poc_email or poc_name or poc_phone:
        out.append({"name": poc_name, "org": agency, "role": "POC", "email": poc_email, "phone": poc_phone, "source": "SAM.gov"})
    if co_email or co_name or co_phone:
        out.append({"name": co_name, "org": agency, "role": "CO", "email": co_email, "phone": co_phone, "source": "SAM.gov"})

    if not any(c.get("email") for c in out) and emails:
        out.append({"name": "", "org": agency, "role": "POC", "email": emails[0], "phone": "", "source": "SAM.gov", "notes": "from description"})

    seen = set(); dedup = []
    for c in out:
        key = (c.get("email") or c.get("name"), c.get("org"))
        if key in seen:
            continue
        seen.add(key); dedup.append(c)
    return dedup


# (early use) ----
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



def _ensure_outreach_log_columns(conn):
    cur = conn.cursor()
    try: cur.execute("alter table outreach_log add column error_text text")
    except Exception: pass
    try: cur.execute("alter table outreach_log add column try_count integer default 0")
    except Exception: pass
    conn.commit()


def normalize_vendor_website(website: str, display_link: str = None):
    if not website:
        return None
    w = website.strip().lower()
    bad_hosts = {"google.com", "www.google.com"}
    try:
        from urllib.parse import urlparse
        u = urlparse(w if w.startswith("http") else "http://" + w)
        host = u.netloc.split(":")[0]
        if host in bad_hosts:
            return None
        return (u.scheme + "://" + u.netloc + u.path).rstrip("/")
    except Exception:
        return w

def ensure_indexes(conn):
    cur = conn.cursor()
    try: cur.execute("create index if not exists idx_opp_notice on opportunities(sam_notice_id)")
    except Exception: pass
    try: cur.execute("create index if not exists idx_outreach_vendor on outreach_log(vendor_id)")
    except Exception: pass
    try: cur.execute("create index if not exists idx_rfq_vendor on rfq_outbox(vendor_id)")
    except Exception: pass
    try: cur.execute("create index if not exists idx_tasks_opp on tasks(opp_id)")
    except Exception: pass
    conn.commit()

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
    """
    Lightweight validator used across export flows.
    Returns a tuple: (issues: list[str], estimated_pages: int)
    Heuristics only — cannot actually inspect fonts from Markdown.
    """
    import math, re as _re
    text = (md_text or "").strip()
    issues = []

    # Basic placeholder checks
    if _re.search(r'\\bINSERT\\b', text) or _re.search(r'\\[[^\\]]*(insert|placeholder|tbd)[^\\]]*\\]', text, flags=_re.IGNORECASE):
        issues.append("Placeholder text 'INSERT' detected. Remove before export.")
    if _re.search(r'\\bTBD\\b|\\bTODO\\b', text):
        issues.append("Unresolved 'TBD/TODO' placeholders present.")
    if "<>" in text or "[ ]" in text:
        issues.append("Bracket placeholders found. Replace with final content.")

    # Page length heuristic: ~450 words per page at 11pt single-space
    words = _re.findall(r'\w+', text)
    est_pages = max(1, math.ceil(len(words) / 450)) if words else 1

    if page_limit and est_pages > page_limit:
        issues.append(f"Estimated length is {est_pages} pages which exceeds the {page_limit}-page limit.")

    return issues, est_pages



def _normalize_markdown_sections(md_text: str) -> str:
    """
    Clean common generation artifacts:
      • Collapse immediately repeated headings with the same text
      • Trim double spaces after heading text
    """
    if not md_text:
        return md_text
    lines = md_text.splitlines()
    out = []
    prev_heading = None
    for ln in lines:
        m = re.match(r'^(#{1,6})\s+(.*)$', ln)
        if m:
            hashes, text = m.group(1), m.group(2).rstrip()
            # Remove any trailing two-space soft break at end of headings
            text = re.sub(r'\s{2,}$', '', text)
            curr = (hashes, text.strip().lower())
            if prev_heading and curr == prev_heading:
                # skip duplicate consecutive heading
                continue
            out.append(f"{hashes} {text}")
            prev_heading = curr
        else:
            out.append(ln)
            # reset prev heading tracking once non-heading encountered
            prev_heading = None
    return "\n".join(out)

def _docx_title_if_needed(md_text: str, proposed_title: str) -> str:
    """Return empty string if md already starts with an H1, else the proposed title."""
    if not md_text:
        return proposed_title or ""
    first = md_text.lstrip().splitlines()[0] if md_text.strip() else ""
    return "" if re.match(r'^#\s+.+', first) else (proposed_title or "")


def _md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                      margins_in: float = 1.0) -> bytes:
    """
    Minimal Markdown-ish to DOCX converter:
      - Headings: lines starting with #, ##, ### map to H1/H2/H3
      - Bullets: lines starting with -, *, or • map to bullets
      - Numbered: lines like "1. text" map to numbered list (approx)
      - Everything else is a normal paragraph
    Returns bytes of the generated .docx file.
    """
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement

    doc = Document()

    # Page margins
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass

    # Base style
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        # Set for East Asia for consistent rendering in Word
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass

    # Optional document title
    if title:
        h = doc.add_heading(title, level=1)
        h.style = doc.styles["Heading 1"]

    lines = (md_text or "").splitlines()
    bullet_buf = []
    num_buf = []

    def flush_bullets():
        nonlocal bullet_buf
        for item in bullet_buf:
            p = doc.add_paragraph(item)
            p.style = doc.styles["List Bullet"]
        bullet_buf = []

    def flush_numbers():
        nonlocal num_buf
        for item in num_buf:
            p = doc.add_paragraph(item)
            p.style = doc.styles["List Number"]
        num_buf = []

    for raw in lines:
        line = raw.rstrip()

        # Blank lines flush any list buffers
        if not line.strip():
            flush_bullets(); flush_numbers()
            doc.add_paragraph("")  # spacer paragraph
            continue

        # Headings
        if line.startswith("### "):
            flush_bullets(); flush_numbers()
            doc.add_heading(line[4:].strip(), level=3)
            continue
        if line.startswith("## "):
            flush_bullets(); flush_numbers()
            doc.add_heading(line[3:].strip(), level=2)
            continue
        if line.startswith("# "):
            flush_bullets(); flush_numbers()
            doc.add_heading(line[2:].strip(), level=1)
            continue

        # Bullets
        if re.match(r"^(\-|\*|•)\s+", line):
            flush_numbers()
            bullet_buf.append(re.sub(r"^(\-|\*|•)\s+", "", line, count=1))
            continue

        # Numbered list approx (e.g., "1. step")
        if re.match(r"^\d+\.\s+", line):
            flush_bullets()
            num_buf.append(re.sub(r"^\d+\.\s+", "", line, count=1))
            continue

        # Normal paragraph
        flush_bullets(); flush_numbers()
        doc.add_paragraph(line)

    # Final flush
    flush_bullets(); flush_numbers()

    bio = io.BytesIO()
    doc.save(bio)
    bio.seek(0)
    return bio.getvalue()
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

    # Render Outreach tools here (moved from sidebar)
    render_outreach_tools()

    conn = get_db(); df_v = pd.read_sql_query("select * from vendors", conn)


    # --- Template manager ---
    t = pd.read_sql_query("select * from email_templates order by name", get_db())
    names = t["name"].tolist() if not t.empty else ["RFQ Request"]
    pick_t = st.selectbox("Template", options=names, key="tpl_pick_name")
    tpl = pd.read_sql_query("select subject, body from email_templates where name=?", get_db(), params=(pick_t,))
    subj_default = tpl.iloc[0]["subject"] if not tpl.empty else get_setting("outreach_subject", "")
    body_default = tpl.iloc[0]["body"] if not tpl.empty else get_setting("outreach_scope", "")

    subj = st.text_input("Subject", value=subj_default, key="tpl_subject")
    body = st.text_area("Body with placeholders {company} {scope} {due}", value=body_default, height=220, key="tpl_body")

    colA, colB, colC, colD = st.columns([1,1,1,2])

    with colA:
        if st.button("Update selected", key="tpl_btn_update"):
            _conn = get_db()
            _conn.execute(
                """
                INSERT INTO email_templates(name, subject, body)
                VALUES(?,?,?)
                ON CONFLICT(name) DO UPDATE SET
                    subject=excluded.subject,
                    body=excluded.body,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (pick_t, subj, body),
            )
            _conn.commit()
            st.success(f"Updated '{pick_t}'")
            st.rerun()

    with colB:
        new_name = st.text_input("New name", value="", placeholder="e.g., RFQ Follow-up", key="tpl_new_name")
        if st.button("Save as new", key="tpl_btn_save_new") and new_name.strip():
            _conn = get_db()
            _conn.execute(
                """
                INSERT INTO email_templates(name, subject, body)
                VALUES(?,?,?)
                ON CONFLICT(name) DO UPDATE SET
                    subject=excluded.subject,
                    body=excluded.body,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (new_name.strip(), subj, body),
            )
            _conn.commit()
            st.success(f"Saved as '{new_name.strip()}'")
            st.rerun()

    with colC:
        confirm_del = st.checkbox("Confirm delete", key="tpl_confirm_delete")
        if st.button("Delete selected", key="tpl_btn_delete", help="Requires confirm") and confirm_del:
            _conn = get_db()
            _conn.execute("DELETE FROM email_templates WHERE name=?", (pick_t,))
            _conn.commit()
            st.warning(f"Deleted '{pick_t}'")
            st.rerun()

    with colD:
        st.caption("Tips: Use placeholders like {company}, {scope}, {due}.")
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
            # Requires st.secrets: smtp_user, smtp_pass
            smtp_user = st.secrets.get("smtp_user")
            smtp_pass = st.secrets.get("smtp_pass")
            if not smtp_user or not smtp_pass:
                raise RuntimeError("Missing smtp_user/smtp_pass in Streamlit secrets")
            from_addr = st.secrets.get("smtp_from", smtp_user)
            reply_to = st.secrets.get("smtp_reply_to", None)
            _send_via_smtp_host(to_addr, subject, body, from_addr, "smtp.gmail.com", 587, smtp_user, smtp_pass, reply_to)

        def _send_via_office365(to_addr, subject, body):
            # Requires st.secrets: smtp_user, smtp_pass
            smtp_user = st.secrets.get("smtp_user")
            smtp_pass = st.secrets.get("smtp_pass")
            if not smtp_user or not smtp_pass:
                pass
            from_addr = st.secrets.get("smtp_from", smtp_user)
            reply_to = st.secrets.get("smtp_reply_to", None)
            _send_via_smtp_host(to_addr, subject, body, from_addr, "smtp.office365.com", 587, smtp_user, smtp_pass, reply_to)

def score_opportunity(row, keywords=None, watched_naics=None):
    try:
        import pandas as pd
        score = 0
        kw = [k.strip().lower() for k in (keywords or []) if k.strip()]
        title = str(row.get("title","")).lower()
        agency = str(row.get("agency",""))
        naics = str(row.get("naics",""))
        typ = str(row.get("type",""))
        # Days until due
        due = row.get("response_due")
        days_to_due = None
        if pd.notna(due) and str(due):
            try:
                days_to_due = (pd.to_datetime(due) - pd.Timestamp.now(tz="UTC")).days
            except Exception:
                days_to_due = None
        # Keyword match boosts
        if kw:
            hits = sum(1 for k in kw if k and k in title)
            score += 15 * min(hits, 3)
        # Preferred notice types
        if typ in {"Combined Synopsis/Solicitation","Solicitation"}:
            score += 10
        # Due soon sweet spot
        if days_to_due is not None:
            if 2 <= days_to_due <= 14:
                score += 25
            elif 0 <= days_to_due < 2:
                score += 10
            elif days_to_due > 30:
                score += 5
        # NAICS match
        if watched_naics and (naics in set(watched_naics) or any(n in (naics or "") for n in watched_naics)):
            score += 20
        # Agency familiarity light boost if seen before
        if agency:
            score += 5
        return int(score)
    except Exception:
        return 0

# === Moved up: opportunity helpers to avoid NameError during SAM Watch ===

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

def save_opportunities(df, default_assignee=""):
    try:
        import streamlit as st
        if not st.session_state.get("__ALLOW_PIPELINE_WRITE", False):
            return (0, 0)
    except Exception:
        pass

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



# ---- SAM history table bootstrap ----
def _ensure_sam_history():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""create table if not exists sam_history(
        id integer primary key autoincrement,
        ts_utc text,
        user text,
        action text,        -- e.g., 'fetch','insert','update','save_to_pipeline','proposal_prep','digest_sent'
        sam_notice_id text,
        title text,
        agency text,
        naics text,
        response_due text,
        score integer default 0
    )""")
    conn.commit()
    return conn


# ---- Live SAM monitor ----

# --- Saved searches schema & helpers (injected) ---
def _ensure_sam_saved_searches_schema():
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("""create table if not exists sam_saved_searches(
            id integer primary key,
            name text unique,
            params_json text,
            updated_at text
        )""")
        conn.commit()
    except Exception as e:
        pass

def sam_saved_searches_upsert(name: str, params: dict):
    _ensure_sam_saved_searches_schema()
    import json, datetime
    now = datetime.datetime.utcnow().isoformat()
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("insert into sam_saved_searches(name, params_json, updated_at) values(?,?,?) on conflict(name) do update set params_json=excluded.params_json, updated_at=excluded.updated_at", (name, json.dumps(params), now))
        conn.commit()
    except Exception:
        try:
            cur.execute("update sam_saved_searches set params_json=?, updated_at=? where name=?", (json.dumps(params), now, name))
            conn.commit()
        except Exception:
            pass

def sam_saved_searches_list():
    _ensure_sam_saved_searches_schema()
    try:
        conn = get_db()
        df = pd.read_sql_query("select id, name, params_json, updated_at from sam_saved_searches order by updated_at desc", conn)
        rows = []
        for _, r in df.iterrows():
            try:
                params = json.loads(r['params_json']) if r['params_json'] else {}
            except Exception:
                params = {}
            rows.append({"id": int(r['id']), "name": r['name'], "params": params, "updated_at": r['updated_at']})
        return rows
    except Exception:
        return []
def sam_live_monitor(run_now: bool = False, hours_interval: int = 3, email_digest: bool = False, min_score_digest: int = 70):
    """
    Check if it's time to auto-fetch SAM results for the current user. If so, run the same search
    used in SAM Watch and insert new rows into opportunities. Optionally email a digest.
    """
    try:
        _ensure_sam_history()
        key_last = f"sam_last_run_{ACTIVE_USER}"
        last_run = get_setting(key_last, "")
        now_utc = pd.Timestamp.utcnow()
        do_run = run_now
        if not do_run:
            if last_run:
                try:
                    last = pd.to_datetime(last_run)
                    do_run = (now_utc - last).total_seconds() >= hours_interval*3600
                except Exception:
                    do_run = True
            else:
                do_run = True

        if not do_run:
            return {"ok": True, "skipped": True}

        # Load defaults for this user
        _defaults_key = f"sam_default_filters_{ACTIVE_USER}"
        try:
            _raw = get_setting(_defaults_key, "")
            _saved = json.loads(_raw) if _raw else {}
        except Exception:
            _saved = {}

        min_days = int(_saved.get("min_days", 3))
        posted_from_days = int(_saved.get("posted_from_days", 30))
        active_only = bool(_saved.get("active_only", True))
        keyword = str(_saved.get("keyword", ""))

        # Build filters
        conn = get_db()
        naics = pd.read_sql_query("select code from naics_watch order by code", conn)["code"].tolist()
        posted_to = pd.Timestamp.utcnow().date()
        posted_from = (posted_to - pd.Timedelta(days=posted_from_days)).isoformat()

        info, df = sam_search(naics, keyword, posted_from, str(posted_to), active_only=active_only, min_days=min_days, limit=150)

        # Insert/update into pipeline table
        new_rows, upd_rows = (0, 0)  # disabled auto-save; user must add via selection if isinstance(df, pd.DataFrame) and not df.empty else (0,0)

        # Log history
        conn = get_db(); cur = conn.cursor()
        cur.execute("insert into sam_history(ts_utc,user,action,sam_notice_id,title,agency,naics,response_due,score) values(?,?,?,?,?,?,?,?,?)",
                    (str(now_utc), ACTIVE_USER, "fetch", "", "", "", "", "", 0))
        conn.commit()

        # Optional digest email
        if email_digest and isinstance(df, pd.DataFrame) and not df.empty:
            _df2 = df.copy()
            _kw = [w for w in (keyword.split() if keyword else []) if w]
            _df2["Score"] = _df2.apply(lambda r: score_opportunity(r, _kw, naics), axis=1)
            best = _df2[_df2["Score"]>=int(min_score_digest)].sort_values("Score", ascending=False).head(10)
            if not best.empty and USER_EMAILS.get(ACTIVE_USER, ""):
                lines = ["Top SAM results (auto digest)"]
                for _, r in best.iterrows():
                    lines.append(f"• [{int(r['Score'])}] {str(r.get('title',''))[:90]} — {str(r.get('agency',''))[:40]} (due {str(r.get('response_due',''))[:16]})<br>{str(r.get('url',''))}")
                try:
                    send_outreach_email(ACTIVE_USER, USER_EMAILS.get(ACTIVE_USER), "SAM Watch: Daily digest", "<br>".join(lines))
                    cur.execute("insert into sam_history(ts_utc,user,action) values(?,?,?)", (str(now_utc), ACTIVE_USER, "digest_sent"))
                    conn.commit()
                except Exception as _e:
                    pass

        set_setting(key_last, str(now_utc))
        return {"ok": True, "inserted": int(new_rows), "updated": int(upd_rows)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


    # ---- Auto proposal prep from SAM row ----
    def build_proposal_md_from_row(row: dict) -> str:
        title = str(row.get("title",""))
        agency = str(row.get("agency",""))
        sol = str(row.get("sam_notice_id",""))
        due = str(row.get("response_due",""))
        naics = str(row.get("naics",""))
        url = str(row.get("url",""))
        company = get_setting("company_name", "ELA Management LLC")
        uei = get_setting("uei", "")
        cage = get_setting("cage", "")
        phone = get_setting("company_phone", "")
        email = USER_EMAILS.get(ACTIVE_USER, get_setting("company_email",""))
        summary = str(row.get("description",""))[:1200]

        md = f"""# {company} – Proposal Draft
**Opportunity:** {title}
**Agency:** {agency}
**Solicitation #:** {sol}
**NAICS:** {naics}
**Response Due:** {due}
**URL:** {url}

## Cover Letter
Dear Contracting Officer,

{company} is pleased to submit our response to **{title}** with **{agency}**. We are a small business with relevant past performance and a robust project management framework to ensure quality, timeliness, and cost control.

## Technical Approach
- Project management methodology to coordinate staffing, scheduling, and quality checks
- Compliance with all federal, state, and local regulations
- Quality Assurance plan aligned to solicitation requirements

## Management Plan
- Key personnel and supervision structure
- Communication cadence and reporting
- Risk mitigation and continuity of operations

## Relevant Past Performance
- Insert your top 3 contracts matching scope and size

## Pricing Notes
- Pricing workbook attached separately
- Assumptions and clarifications kept minimal

## Contact
**UEI:** {uei}
**CAGE:** {cage}
**Phone:** {phone}
**Email:** {email}

*Auto-generated from SAM Watch listing. Please replace placeholders before submission.*
"""
        return md

with legacy_tabs[4]:
    # Show a helper tip only inside SAM Watch when no results have been loaded

    if ('sam_results_df' not in st.session_state) or (st.session_state['sam_results_df'] is None) or (hasattr(st.session_state['sam_results_df'], 'empty') and st.session_state['sam_results_df'].empty):

        st.info('No active results yet. Click **Run search now**.')


    with st.expander("Saved Searches", expanded=True):
        cols = st.columns([2,2,2,2,1,1])
        with cols[0]: ss_name = st.text_input("Name", key="ss_name")
        with cols[1]: ss_keyword = st.text_input("Keyword", value=str(st.session_state.get("sam_keyword","")), key="ss_keyword_builder")
        with cols[2]: ss_naics = st.text_input("NAICS list (comma-separated)", value=st.session_state.get("naics_default",""), key="ss_naics_builder")
        with cols[3]: ss_notice = st.multiselect("Notice types", ["Combined Synopsis/Solicitation","Solicitation","Presolicitation","Sources Sought"], default=["Combined Synopsis/Solicitation","Solicitation"], key="ss_notice_builder")
        with cols[4]: ss_min_days = st.number_input("Min days", min_value=0, step=1, value=int(0), key="ss_min_days_builder")
        with cols[5]: ss_posted = st.number_input("Posted within days", min_value=1, step=1, value=int(60), key="ss_posted_builder")
        active_only = st.checkbox("Active only", value=True, key="ss_active_only_builder")
        if st.button("Save search"):
            _params = {
                "keyword": ss_keyword.strip(),
                "naics_list": [s.strip() for s in ss_naics.split(",") if s.strip()],
                "notice_types": ",".join(ss_notice),
                "min_days": int(ss_min_days),
                "posted_from_days": int(ss_posted),
                "active": "true" if active_only else "false"
            }
            try:
                sam_saved_upsert(ss_name.strip(), _params, active=True)
                st.success(f"Saved search '{ss_name}'")
            except Exception as e:
                st.error(f"Failed to save: {e}")

        try:
            saved = sam_saved_list()
            if saved:
                st.write("Saved searches:")
                for row in saved:
                    c1,c2,c3,c4,c5 = st.columns([2,3,2,2,2])
                    c1.write(f"**{row['name']}**")
                    c2.write(row.get("params",{}))
                    if c3.button("Run", key=f"run_{row['name']}"):
                        pars = row['params']
                        df_run, info = sam_search(
                            pars.get("naics_list", []),
                            min_days=int(pars.get("min_days",0)),
                            limit=200,
                            keyword=pars.get("keyword",""),
                            posted_from_days=int(pars.get("posted_from_days",60)),
                            notice_types=pars.get("notice_types",""),
                            active=pars.get("active","true")
                        )
                        st.session_state["sam_results_df"] = df_run
                        st.success(f"Found {len(df_run)} results for '{row['name']}'")
                    if c4.button("Run & Ingest", key=f"run_ingest_{row['name']}"):
                        pars = row['params']
                        df_run, info = sam_search(
                            pars.get("naics_list", []),
                            min_days=int(pars.get("min_days",0)),
                            limit=200,
                            keyword=pars.get("keyword",""),
                            posted_from_days=int(pars.get("posted_from_days",60)),
                            notice_types=pars.get("notice_types",""),
                            active=pars.get("active","true")
                        )
                        if hasattr(df_run, "empty") and not df_run.empty:
                            to_save = df_run.copy()
                            if "Link" in to_save.columns:
                                to_save = to_save.drop(columns=["Link"])
                            st.session_state["__ALLOW_PIPELINE_WRITE"] = True
                            ins, upd = save_opportunities(to_save, default_assignee=st.session_state.get("assignee_default",""))
                            st.success(f"Ingested {len(df_run)}. New {ins}, updated {upd}.")
                            try:
                                st.session_state["__ALLOW_PIPELINE_WRITE"] = False
                            except Exception:
                                pass
                            st.session_state["sam_results_df"] = df_run
                        else:
                            st.info("No results to ingest.")
                    if c5.button("Delete", key=f"del_{row['name']}"):
                        sam_saved_delete(row['name'])
                        st.warning(f"Deleted '{row['name']}'. Refresh to update list.")
        except Exception as e:
            st.error(f"Saved searches error: {e}")
    st.subheader("SAM Watch: Auto Search + Attachments + Saved Searches")
    st.markdown("> **Flow:** Set All active → apply filters → open attachments → choose assignee → **Search** then **Save to pipeline**")
    conn = get_db()
    codes = pd.read_sql_query("select code from naics_watch order by code", conn)["code"].tolist()
    st.caption(f"Using NAICS codes: {', '.join(codes) if codes else 'none'}")

    auto_on = st.checkbox("Enable auto-monitor", value=bool(get_setting(f"sam_auto_{ACTIVE_USER}", "true") != "false"))
    interval_hours = st.number_input("Auto-monitor every (hours)", min_value=1, max_value=24, value=int(get_setting(f"sam_interval_{ACTIVE_USER}", "3") or 3))
    digest = st.checkbox("Send daily digest email", value=bool(get_setting(f"sam_digest_{ACTIVE_USER}", "true") != "false"))
    digest_min = st.number_input("Digest min score", min_value=0, max_value=100, value=70, step=5)
    if st.button("Save monitor settings"):
        set_setting(f"sam_auto_{ACTIVE_USER}", "true" if auto_on else "false")
        set_setting(f"sam_interval_{ACTIVE_USER}", str(int(interval_hours)))
        set_setting(f"sam_digest_{ACTIVE_USER}", "true" if digest else "false")
        set_setting(f"sam_digestmin_{ACTIVE_USER}", str(int(digest_min)))
        st.success("Saved monitor settings")

    # Kick the monitor if interval elapsed
    try:
        if auto_on:
            _res = sam_live_monitor(False, int(interval_hours), digest, int(digest_min))
            if _res and _res.get("ok") and not _res.get("skipped"):
                st.info(f"Auto-monitor: inserted {_res.get('inserted',0)}, updated {_res.get('updated',0)}")
    except Exception as _e_mon:
        st.caption(f"[Monitor note: {_e_mon}]")

# --- Per-user default filters
_defaults_key = f"sam_default_filters_{ACTIVE_USER}"
try:
    _raw = get_setting(_defaults_key, "")
    _saved_defaults = json.loads(_raw) if _raw else {}
except Exception:
    _saved_defaults = {}



    col1, col2, col3 = st.columns(3)
    with col1:
        min_days = st.number_input("Minimum days until due", min_value=0, step=1, value=int(_saved_defaults.get('min_days', 3)))
        posted_from_days = st.number_input("Posted window (days back)", min_value=1, step=1, value=int(_saved_defaults.get('posted_from_days', 30)))
        active_only = st.checkbox("All active opportunities", value=bool(_saved_defaults.get('active_only', True)))
    with col2:
        keyword = st.text_input("Keyword", value=str(_saved_defaults.get('keyword', '')), key="sam_keyword")
        notice_types = st.multiselect("Notice types", options=["Combined Synopsis/Solicitation","Solicitation","Presolicitation","SRCSGT"], default=["Combined Synopsis/Solicitation","Solicitation"])
    with col3:
        diag = st.checkbox("Show diagnostics", value=False)
        raw = st.checkbox("Show raw API text (debug)", value=False)
        assignee_default = st.selectbox("Default assignee", ["","Quincy","Charles","Collin"], index=(['','Quincy','Charles','Collin'].index(st.session_state.get('active_profile','')) if st.session_state.get('active_profile','') in ['Quincy','Charles','Collin'] else 0))
        st.markdown("**Defaults**")
        if st.button("Save as my default"):
            set_setting(_defaults_key, json.dumps({
                'min_days': int(min_days),
                'posted_from_days': int(posted_from_days),
                'active_only': bool(active_only),
                'keyword': str(keyword or '')
            }))
            st.success("Saved your defaults")
        if st.button("Reset my default"):
            set_setting(_defaults_key, "")
            st.info("Cleared your saved defaults")

        # --- Saved Searches manager ---
        st.markdown("### Saved Searches")
        _ensure_sam_saved_searches_schema()
        _ss_list = sam_saved_searches_list()
        if _ss_list:
            names = [f"{row['name']} (updated {row['updated_at'][:10]})" for row in _ss_list]
            pick_idx = st.selectbox("Choose a saved search", list(range(len(names))), format_func=lambda i: names[i] if names else "", key="sam_ss_pick")
            chosen = _ss_list[pick_idx] if _ss_list else None
        else:
            st.caption("No saved searches yet.")
            chosen = None

        with st.expander("Create or update a saved search"):
            ss_name = st.text_input("Search name", value="")
            ss_keyword = st.text_input("Keyword(s) for this saved search", value=str(keyword or ""))
            ss_naics = st.text_input("NAICS list (comma separated)", value="")
            ss_notice = st.multiselect("Notice types", options=["Combined Synopsis/Solicitation","Solicitation","Presolicitation","SRCSGT"], default=["Combined Synopsis/Solicitation","Solicitation"], key="sam_ss_notice")
            ss_min_days = st.number_input("Min days until due", min_value=0, step=1, value=int(min_days))
            ss_posted_from_days = st.number_input("Look-back window (days since posted)", min_value=1, step=1, value=int(posted_from_days))
            if st.button("Save search"):
                params = {
                    "keyword": ss_keyword.strip(),
                    "naics_list": [s.strip() for s in ss_naics.split(",") if s.strip()],
                    "notice_types": ",".join(ss_notice),
                    "min_days": int(ss_min_days),
                    "posted_from_days": int(ss_posted_from_days),
                    "active": "true" if active_only else "false",
                    "limit": 100
                }
                sam_saved_searches_upsert(ss_name.strip(), params)
                st.success(f"Saved search '{ss_name.strip()}'")

        if chosen:
            st.markdown("#### Run selected saved search")
            if st.button("Run & Ingest to Pipeline"):
                _df, _info = sam_search(
                    naics_list=chosen['params'].get('naics_list') or [],
                    min_days=int(chosen['params'].get('min_days', 3)),
                    limit=int(chosen['params'].get('limit', 100)),
                    keyword=chosen['params'].get('keyword') or None,
                    posted_from_days=int(chosen['params'].get('posted_from_days', 30)),
                    notice_types=chosen['params'].get('notice_types', "Combined Synopsis/Solicitation,Solicitation"),
                    active=chosen['params'].get('active', "true")
                )
                st.dataframe(_df.head(50), use_container_width=True)
                if not _df.empty:
                    _added, _updated = 0, 0
                    for _, r in _df.iterrows():
                        action, _id = _opportunities_upsert(
                            title=str(r.get("title","")),
                            agency=str(r.get("agency","")),
                            naics=str(r.get("naics","")),
                            response_due=str(r.get("response_due","")),
                            url=str(r.get("url","")),
                            data=r.to_dict()
                        )
                        if action == "insert": _added += 1
                        elif action == "update": _updated += 1
                    st.success(f"Ingested to pipeline: added {_added}, updated {_updated}")

        # --- Auto-ingest scheduler ---
        st.markdown("### Auto-ingest")
        toggle_auto = st.checkbox("Enable background auto-ingest (every N hours)", value=bool(get_setting("sam_auto_ingest_enabled","")=="1"))
        every_hours = st.slider("Frequency (hours)", min_value=1, max_value=24, value=int(get_setting("sam_auto_ingest_hours","3") or 3))
        email_digest = st.checkbox("Email a digest when new matches found", value=bool(get_setting("sam_auto_ingest_email","")=="1"))
        min_score = st.slider("Min score for digest", min_value=0, max_value=100, value=int(get_setting("sam_auto_ingest_min_score","70") or 70))
        if st.button("Save auto-ingest settings"):
            set_setting("sam_auto_ingest_enabled", "1" if toggle_auto else "0")
            set_setting("sam_auto_ingest_hours", str(every_hours))
            set_setting("sam_auto_ingest_email", "1" if email_digest else "0")
            set_setting("sam_auto_ingest_min_score", str(min_score))
            st.success("Saved auto-ingest settings.")
            # Optionally trigger a run now
            if st.checkbox("Run one cycle now"):
                _r = sam_live_monitor(run_now=True, hours_interval=int(every_hours), email_digest=bool(email_digest), min_score_digest=int(min_score))
                st.write(_r)



    cA, cB, cC = st.columns(3)

    # Run search stores results in session so Save works after rerun
    with cA:
        # Fallback in case the number_input did not run in this branch

        pages_to_fetch = st.session_state.get("pages_to_fetch", 3)
        email_top = st.checkbox("Email me the top results", value=False)
        min_score_email = st.number_input("Min score to email", min_value=0, max_value=100, value=70, step=5)
        email_to_self = st.text_input("Send to (your email)", value=USER_EMAILS.get(ACTIVE_USER, ""))

        if st.button("Run search now"):
            df, info = sam_search(
                codes, min_days=min_days, limit=150,
                keyword=keyword or None, posted_from_days=int(posted_from_days),
                notice_types="Combined Synopsis/Solicitation,Solicitation", active="true"
            )
            st.session_state["sam_results_df"] = df
            st.session_state["sam_results_info"] = info
            # ## Email top results
            try:
                if email_top and isinstance(df, pd.DataFrame) and not df.empty and email_to_self:
                    _df2 = df.copy()
                    _df2["Score"] = _df2.apply(lambda r: score_opportunity(r, _kw if ' _kw' in locals() else (keyword.split() if keyword else []), codes if isinstance(codes, list) else []), axis=1)
                    _df2 = _df2.sort_values("Score", ascending=False)
                    best = _df2[_df2["Score"]>=int(min_score_email)].head(10)
                    if not best.empty:
                        lines = ["Top SAM results (auto)"]
                        for _, r in best.iterrows():
                            lines.append(f"• [{int(r['Score'])}] {str(r.get('title',''))[:90]} — {str(r.get('agency',''))[:40]} (due {str(r.get('response_due',''))[:16]})\n{str(r.get('url',''))}")
                        try:
                            send_outreach_email(ACTIVE_USER, email_to_self, "SAM Watch: Top matches", "<br>".join(lines))
                            st.info(f"Emailed {len(best)} matches to {email_to_self}")
                        except Exception as _e_mail:
                            st.caption(f"[Email note: {_e_mail}]")
            except Exception as _e_email:
                st.caption(f"[Email block note: {_e_email}]")
            if diag:
                st.write("Diagnostics:", info)
                st.code(f"naics={','.join(codes[:20])} | keyword={keyword or ''} | postedFrom={info.get('filters',{}).get('postedFrom')} -> postedTo={info.get('filters',{}).get('postedTo')} | min_days={min_days} | limit=150", language="text")
            # Optional: email top results once computed below after scoring
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

        # Compute Score
        _kw = [w for w in (keyword.split() if isinstance(keyword, str) else []) if w]
        try:
            watched = codes if isinstance(codes, list) else []
        except Exception:
            watched = []
        grid_df["Score"] = grid_df.apply(lambda r: score_opportunity(r, _kw, watched), axis=1)
        # Sort by Score desc then due date asc
        if "response_due" in grid_df.columns:
            try:
                _dt = pd.to_datetime(grid_df["response_due"], errors="coerce")
                grid_df = grid_df.assign(_due=_dt).sort_values(["Score","_due"], ascending=[False, True]).drop(columns=["_due"])
            except Exception:
                grid_df = grid_df.sort_values(["Score"], ascending=[False])
        else:
            grid_df = grid_df.sort_values(["Score"], ascending=[False])

        with st.expander("Quick select options"):
            n_top = st.number_input("Select top N by score", min_value=1, max_value=max(1, min(50, len(grid_df))), value=min(5, len(grid_df)))
            if st.button("Mark top N for Save"):
                try:
                    top_idx = grid_df.sort_values("Score", ascending=False).head(int(n_top)).index
                    df.loc[top_idx, "Save"] = True
                    grid_df.loc[top_idx, "Save"] = True
                    st.success(f"Selected {int(n_top)} rows")
                except Exception as e:
                    st.warning(f"Could not select top rows: {e}")

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
            try:
                st.session_state["__ALLOW_PIPELINE_WRITE"] = True
            except Exception:
                pass
            to_save = save_sel.drop(columns=[c for c in ["Save","Link"] if c in save_sel.columns])
            ins, upd = save_opportunities(to_save, default_assignee=assignee_default)
            st.success(f"Saved to pipeline — inserted {ins}, updated {upd}.")
            try:
                st.session_state["__ALLOW_PIPELINE_WRITE"] = False
            except Exception:
                pass
            # === Auto add POCs and COs to Contacts after saving to pipeline ===
try:
    if ('save_sel' in locals()) and isinstance(save_sel, pd.DataFrame) and not save_sel.empty:
        added, updated = 0, 0
        for _, _r in save_sel.iterrows():
            for c in _extract_contacts_from_sam_row(_r):
                act, _ = _contacts_upsert(
                    name=c.get("name",""), org=c.get("org",""), role=c.get("role",""),
                    email=c.get("email",""), phone=c.get("phone",""),
                    source=c.get("source","SAM.gov"), notes=c.get("notes","")
                )
                if act == "insert":
                    added += 1
                elif act == "update":
                    updated += 1
        if added or updated:
            try:
                st.toast(f"Contacts synced from SAM Watch added {added} updated {updated}")
            except Exception:
                st.caption(f"Contacts synced from SAM Watch added {added} updated {updated}")
except Exception as _e_sync:
    try:
        st.caption(f"[Contacts sync note: {_e_sync}]")
    except Exception:
        pass
# Proposal drafts for selected
# [disabled to fix indentation]             if len(save_sel) > 0:
# [disabled to fix indentation]                 st.markdown("#### Auto Proposal Prep")
# [disabled to fix indentation]                 for _i, _r in save_sel.iterrows():
# [disabled to fix indentation]                     _md = build_proposal_md_from_row(_r)
# [disabled to fix indentation]                     _bytes = md_to_docx_bytes_rich(_md, title=str(_r.get('title','')))
# [disabled to fix indentation]                     st.download_button("Prep Proposal DOCX: " + str(_r.get('sam_notice_id','') or _r.get('title',''))[:40], data=_bytes, file_name=f"proposal_{str(_r.get('sam_notice_id','') or _i)}.docx", mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
# [disabled to fix indentation]                     try:
# [disabled to fix indentation]                         cur = get_db().cursor()
# [disabled to fix indentation]                         cur.execute("insert into sam_history(ts_utc,user,action,sam_notice_id,title,agency,naics,response_due,score) values(?,?,?,?,?,?,?,?,?)",
# [disabled to fix indentation]                                     (str(pd.Timestamp.utcnow()), ACTIVE_USER, "proposal_prep", str(_r.get('sam_notice_id','')), str(_r.get('title','')), str(_r.get('agency','')), str(_r.get('naics','')), str(_r.get('response_due','')), int(_r.get('Score',0))))
# [disabled to fix indentation]                         get_db().commit()
# [disabled to fix indentation]                     except Exception:
# [disabled to fix indentation]                         pass
# [disabled to fix indentation]             if len(save_sel) > 0:
# [disabled to fix indentation]                 try:
# [disabled to fix indentation]                     st.markdown("#### Prep outreach drafts for CO (email placeholders)")
# [disabled to fix indentation]                     if st.button("Create outreach drafts in Outreach tab"):
# [disabled to fix indentation]                         bods = []
# [disabled to fix indentation]                         for _, r in save_sel.iterrows():
# [disabled to fix indentation]                             subj = f"Inquiry: {str(r.get('title',''))[:60]}"
# [disabled to fix indentation]                             body = f"<p>Hello Contracting Officer,</p><p>We reviewed <strong>{str(r.get('title',''))}</strong> at {str(r.get('agency',''))}. We have relevant past performance and would like to confirm points of contact and any site-visit details.</p><p>Regards,<br>{get_setting('company_name','ELA Management LLC')}</p>"
# [disabled to fix indentation]                             bods.append({"to":"","subject":subj,"body":body,"vendor_id":0})
# [disabled to fix indentation]                         st.session_state['mail_bodies'] = bods
# [disabled to fix indentation]                         st.success("Drafts prepared — open the Outreach tab to review and send.")
# [disabled to fix indentation]                 except Exception as _e_prep:
# [disabled to fix indentation]                     st.caption(f"[CO outreach prep note: {_e_prep}]")
    else:
        cA, cB, cC = st.columns(3)
    with cB:
        if st.button("Broad test (keyword only)"):
            kw = (st.session_state.get("sam_keyword", "") or "").strip() or "janitorial"
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




# --- Analytics & History ---
with legacy_tabs[4]:
    with st.expander("SAM Analytics"):
        conn = get_db()
        try:
            hist = pd.read_sql_query("select * from sam_history order by ts_utc desc", conn)
            st.dataframe(hist.head(200))
            # Simple aggregates
            st.write("Total fetches:", int((hist["action"]=="fetch").sum()) if "action" in hist else 0)
            st.write("Proposals prepped:", int((hist["action"]=="proposal_prep").sum()) if "action" in hist else 0)
            # Monthly new opportunities (approx: use fetch counts as proxy)
            if not hist.empty and "ts_utc" in hist.columns:
                _h = hist.copy(); _h["month"] = pd.to_datetime(_h["ts_utc"], errors="coerce").dt.to_period("M").astype(str)
                agg = _h.groupby(["month","action"]).size().reset_index(name="n")
                st.write("Activity by month")
                st.dataframe(agg.sort_values(["month","action"]))
        except Exception as _e_ana:
            st.caption(f"[Analytics note: {_e_ana}]")
with legacy_tabs[6]:
    st.subheader("Capability statement builder")
    company = get_setting("company_name", "ELA Management LLC")
    tagline = st.text_input("Tagline", key="cap_tagline_input_capability_builder", value="Responsive project management for federal facilities and services")
    core = st.text_area("Core competencies", key="cap_core_textarea_capability_builder", value="Janitorial Landscaping Staffing Logistics Construction Support IT Charter buses Lodging Security Education Training Disaster relief")
    diff = st.text_area("Differentiators", key="cap_diff_textarea_capability_builder", value="Fast mobilization • Quality controls • Transparent reporting • Nationwide partner network")
    past_perf = st.text_area("Representative experience", key="cap_past_textarea_capability_builder", value="Project A: Custodial support, 100k sq ft. Project B: Grounds keeping, 200 acres.")
    contact = st.text_area("Contact info", key="cap_contact_textarea_capability_builder", value="ELA Management LLC • info@elamanagement.com • 555 555 5555 • UEI XXXXXXX • CAGE XXXXX")

    c1, c2, c3 = st.columns([1,1,2])

    with c1:
        if st.button("Generate one page", key="btn_cap_generate_capability_builder"):
            system = "Format a one page federal capability statement in markdown. Use clean headings and short bullets."
            prompt = f"""Company {company}
Tagline {tagline}
Core {core}
Diff {diff}
Past performance {past_perf}
Contact {contact}
NAICS {", ".join(sorted(set(NAICS_SEEDS)))}
Certifications Small Business"""
            cap_md = llm(system, prompt, max_tokens=900)
            st.session_state["capability_md"] = cap_md

    with c2:
        if st.button("Clear draft", key="btn_cap_clear_capability_builder"):
            st.session_state.pop("capability_md", None)

    cap_md = st.session_state.get("capability_md", "")
    cap_md = _normalize_markdown_sections(cap_md)
    if cap_md:
        st.markdown("#### Preview")
        st.markdown(cap_md)
        issues, est_pages = _validate_text_for_guardrails(cap_md, page_limit=2, require_font="Times New Roman", require_size_pt=11, margins_in=1.0, line_spacing=1.0, filename_pattern="{company}_{section}_{date}")
        if issues:
            st.warning("Before export, fix these items: " + "; ".join(issues))
        logo_file = st.file_uploader("Optional logo for header", type=["png","jpg","jpeg"], key="cap_logo_upload")
        _logo = logo_file.read() if logo_file else None
        docx_bytes = md_to_docx_bytes_rich(cap_md, title=_docx_title_if_needed(cap_md, f"{company} Capability Statement"), base_font="Times New Roman", base_size_pt=11, margins_in=1.0, logo_bytes=_logo)
        st.download_button("Export Capability Statement (DOCX)", data=docx_bytes, file_name="Capability_Statement.docx", mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    else:
        st.info("Click Generate one page to draft, then export to DOCX.")


with legacy_tabs[7]:
    st.subheader("White paper builder")
    title = st.text_input("Title", key="wp_title_input_whitepaper_builder", value="Improving Facility Readiness with Outcome based Service Contracts")
    thesis = st.text_area("Thesis", key="wp_thesis_textarea_whitepaper_builder", value="Outcome based service contracts reduce total cost and improve satisfaction when paired with clear SLAs and transparent data.")
    audience = st.text_input("Audience", key="wp_audience_input_whitepaper_builder", value="Facility Managers • Contracting Officers • Program Managers")

    col_w1, col_w2, col_w3 = st.columns([1,1,2])
    with col_w1:
        if st.button("Draft white paper", key="btn_wp_draft_whitepaper_builder"):
            system = "Write a two page white paper with executive summary, problem, approach, case vignette, and implementation steps. Use clear headings and tight language."
            prompt = f"Title {title}\nThesis {thesis}\nAudience {audience}"
            wp_md = llm(system, prompt, max_tokens=1400)
            st.session_state["whitepaper_md"] = wp_md
    with col_w2:
        if st.button("Clear white paper draft", key="btn_wp_clear_whitepaper_builder"):
            st.session_state.pop("whitepaper_md", None)

    wp_md = st.session_state.get("whitepaper_md", "")
    wp_md = _normalize_markdown_sections(wp_md)
    if wp_md:
        st.markdown("#### Preview")
        st.markdown(wp_md)
        issues, est_pages = _validate_text_for_guardrails(wp_md, page_limit=4, require_font="Times New Roman", require_size_pt=11, margins_in=1.0, line_spacing=1.0, filename_pattern="{company}_{section}_{date}")
        if issues:
            st.warning("Before export, fix these items: " + "; ".join(issues))
        wp_logo_file = st.file_uploader("Optional logo for header", type=["png","jpg","jpeg"], key="wp_logo_upload")
        _wp_logo = wp_logo_file.read() if wp_logo_file else None
        wp_bytes = md_to_docx_bytes_rich(wp_md, title=_docx_title_if_needed(wp_md, title), base_font="Times New Roman", base_size_pt=11, margins_in=1.0, logo_bytes=_wp_logo)
        st.download_button("Export White Paper (DOCX)", data=wp_bytes, file_name="White_Paper.docx", mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    else:
        st.info("Click Draft white paper to create a draft, then export to DOCX.")

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
            for para in row[2].split("\n\n"):
                doc.add_paragraph(_strip_markdown_to_plain(para))
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

def save_opportunities(df, default_assignee=""):
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

    # Company identifiers (ELA Management LLC)
    st.subheader("Company identifiers")
    st.code("DUNS: 14-483-4790\nCAGE: 14ZP6\nUEI: U32LBVK3DDF7", language=None)

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
            pass

        session_id = parse_pick_id(pick)
        if session_id is None:
            st.info("Select a valid session to continue.")
            pass
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
        st.warning(f"RFP Analyzer error: {e}")

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
                st.warning("Proceeding with export:")
                for x in issues:
                    st.markdown(f"- {x}")


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
                    doc.add_paragraph(_strip_markdown_to_plain(para))

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



# --- SAFE REDEFINITION: guardrails validator ---



def md_to_docx_bytes(md_text: str, title: str = "", base_font: str = "Times New Roman", base_size_pt: int = 11,
                     margins_in: float = 1.0, logo_bytes: bytes = None, logo_width_in: float = 1.5) -> bytes:
    """
    Backward compatible wrapper that supports an optional logo header.
    Signature matches earlier calls that used logo_bytes.
    """
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml.ns import qn
    import re as _re, io

    # Build a fresh document so we can place a logo at the top if provided
    doc = Document()

    # Margins
    try:
        section = doc.sections[0]
        section.top_margin = Inches(margins_in)
        section.bottom_margin = Inches(margins_in)
        section.left_margin = Inches(margins_in)
        section.right_margin = Inches(margins_in)
    except Exception:
        pass

    # Base style
    try:
        style = doc.styles["Normal"]
        font = style.font
        font.name = base_font
        font.size = Pt(base_size_pt)
        rFonts = style.element.rPr.rFonts
        rFonts.set(qn('w:ascii'), base_font)
        rFonts.set(qn('w:hAnsi'), base_font)
        rFonts.set(qn('w:eastAsia'), base_font)
    except Exception:
        pass

    # Optional logo header
    if logo_bytes:
        p_center = doc.add_paragraph()
        p_center.paragraph_format.alignment = 1  # center
        run = p_center.add_run()
        try:
            run.add_picture(io.BytesIO(logo_bytes), width=Inches(logo_width_in))
        except Exception:
            pass

    # Optional document title
    if title:
        h = doc.add_heading(title, level=1)
        try:
            h.style = doc.styles["Heading 1"]
        except Exception:
            pass

    # Reuse the simple markdown-ish renderer by saving into a temp docx and appending
    # For simplicity, we reimplement the same minimal renderer here:
    lines = (md_text or "").splitlines()
    bullet_buf = []
    num_buf = []

    def flush_bullets():
        nonlocal bullet_buf
        for item in bullet_buf:
            p = doc.add_paragraph(item)
            try:
                p.style = doc.styles["List Bullet"]
            except Exception:
                pass
        bullet_buf = []

    def flush_numbers():
        nonlocal num_buf
        for item in num_buf:
            p = doc.add_paragraph(item)
            try:
                p.style = doc.styles["List Number"]
            except Exception:
                pass
        num_buf = []

    for raw in lines:
        line = raw.rstrip()

        if not line.strip():
            flush_bullets(); flush_numbers()
            doc.add_paragraph("")
            continue

        if line.startswith("### "):
            flush_bullets(); flush_numbers()
            doc.add_heading(line[4:].strip(), level=3)
            continue
        if line.startswith("## "):
            flush_bullets(); flush_numbers()
            doc.add_heading(line[3:].strip(), level=2)
            continue
        if line.startswith("# "):
            flush_bullets(); flush_numbers()
            doc.add_heading(line[2:].strip(), level=1)
            continue

        if _re.match(r"^(\-|\*|•)\s+", line):
            flush_numbers()
            bullet_buf.append(_re.sub(r"^(\-|\*|•)\s+", "", line, count=1))
            continue

        if _re.match(r"^\d+\.\s+", line):
            flush_bullets()
            num_buf.append(_re.sub(r"^\d+\.\s+", "", line, count=1))
            continue

        flush_bullets(); flush_numbers()
        doc.add_paragraph(line)

    flush_bullets(); flush_numbers()

    out = io.BytesIO()
    doc.save(out)
    out.seek(0)
    return out.getvalue()



with legacy_tabs[8]:
    st.subheader("Proposal export and drafts")




# === Deals (CRM pipeline) helpers ===
DEAL_STAGES = [
    "No Contact Made",
    "CO Contacted",
    "Quote",
    "Multiple Quotes",
    "Proposal Started",
    "Proposal Finished",
    "Proposal Submitted",
    "Awarded",
    "Proposal Lost",
]

def ensure_deals_table(conn):
    cur = conn.cursor()
    cur.execute("""
        create table if not exists deals (
            id integer primary key autoincrement,
            title text not null,
            stage text not null default 'No Contact Made',
            owner text,
            amount numeric,
            notes text,
            agency text,
            due_date text,
            created_at text default (datetime('now')),
            updated_at text default (datetime('now'))
        )
    """)
    cur.execute("create index if not exists deals_stage_idx on deals(stage)")
    cur.execute("create index if not exists deals_updated_idx on deals(updated_at)")
    conn.commit()

def list_deals(stage: str | None = None, q: str | None = None):
    conn = get_db()
    ensure_deals_table(conn)
    cur = conn.cursor()
    sql = "select id, title, stage, owner, amount, notes, agency, due_date, created_at, updated_at from deals"
    params = []
    where = []
    if stage and stage != "All":
        where.append("stage = ?")
        params.append(stage)
    if q:
        where.append("(title like ? or notes like ? or agency like ?)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]
    if where:
        sql += " where " + " and ".join(where)
    sql += " order by updated_at desc, id desc"
    rows = cur.execute(sql, params).fetchall()
    cols = ["id","title","stage","owner","amount","notes","agency","due_date","created_at","updated_at"]
    import pandas as pd
    return pd.DataFrame(rows, columns=cols)

def create_deal(title: str, stage: str, owner: str | None, amount: float | None, notes: str | None, agency: str | None, due_date: str | None):
    try:
        import streamlit as st
        if not st.session_state.get("__ALLOW_PIPELINE_WRITE", False):
            return 0
    except Exception:
        pass

    conn = get_db()
    ensure_deals_table(conn)
    cur = conn.cursor()
    cur.execute("""
        insert into deals (title, stage, owner, amount, notes, agency, due_date)
        values (?,?,?,?,?,?,?)
    """, (title, stage, owner, amount, notes, agency, due_date))
    conn.commit()
    return cur.lastrowid

def update_deal(id_: int, **fields):
    if not fields: return False
    conn = get_db()
    ensure_deals_table(conn)
    cur = conn.cursor()
    sets = []
    vals = []
    for k,v in fields.items():
        if k not in {"title","stage","owner","amount","notes","agency","due_date"}:
            continue
        sets.append(f"{k} = ?")
        vals.append(v)
    if not sets: return False
    sets.append("updated_at = datetime('now')")
    sql = "update deals set " + ", ".join(sets) + " where id = ?"
    vals.append(id_)
    cur.execute(sql, vals)
    conn.commit()
    return cur.rowcount > 0

def delete_deal(id_: int):
    conn = get_db()
    ensure_deals_table(conn)
    cur = conn.cursor()
    cur.execute("delete from deals where id = ?", (id_,))
    conn.commit()
    return cur.rowcount > 0


# === Deals (CRM Pipeline) tab ===
try:
    with legacy_tabs[13]:
        st.subheader("Deals Pipeline")
        st.caption("Track opportunities by stage, assign owners, record amounts, and manage the pipeline.")

        # Filters
        c1,c2,c3 = st.columns([1,1,2])
        with c1:
            stage_filter = st.selectbox("Stage", options=["All"] + DEAL_STAGES, index=0, key="deals_stage_filter")
        with c2:
            q = st.text_input("Search", key="deals_search")
        with c3:
            st.markdown(" ")

        # Data
        df = list_deals(stage_filter, q)

        # Totals by stage (above grid)
        st.markdown("#### Totals by stage")
        import pandas as _pd
        _stage_amounts = _pd.to_numeric(df["amount"], errors="coerce").fillna(0)
        _stage_totals = _stage_amounts.groupby(df["stage"]).sum() if not df.empty else _pd.Series(dtype=float)
        _stage_totals = _stage_totals.reindex(DEAL_STAGES).fillna(0)
        _cols = st.columns(len(DEAL_STAGES))
        for _i, _stage in enumerate(DEAL_STAGES):
            with _cols[_i]:
                st.metric(_stage, f"${float(_stage_totals.get(_stage, 0.0)):,.2f}")

        # Add a new deal
        st.markdown("#### Add a new deal")
        with st.form("new_deal_form", clear_on_submit=True):
            nc1,nc2,nc3,nc4 = st.columns([2,1,1,1])
            with nc1:
                new_title = st.text_input("Opportunity title*", placeholder="e.g., USDA Athens Roof Repair RFQ")
            with nc2:
                new_stage = st.selectbox("Stage*", options=DEAL_STAGES, index=0)
            with nc3:
                new_owner = st.text_input("Owner", placeholder="e.g., Latrice")
            with nc4:
                new_amount = st.number_input("Amount", min_value=0.0, step=100.0, value=0.0, format="%.2f")
            nc5,nc6 = st.columns([1,1])
            with nc5:
                new_agency = st.text_input("Agency", placeholder="e.g., USDA ARS")
            with nc6:
                new_due = st.text_input("Due date (YYYY-MM-DD)", placeholder="2025-11-01")
            new_notes = st.text_area("Notes", height=80, placeholder="Key details, next actions...")
            submitted = st.form_submit_button("Create deal")
            if submitted:
                try:
                    _st.session_state["__ALLOW_PIPELINE_WRITE"] = True
                except Exception:
                    pass
                if not new_title.strip():
                    st.warning("Please enter a title.")
                else:
                    create_deal(
                        new_title.strip(),
                        new_stage,
                        new_owner.strip() or None,
                        float(new_amount) if new_amount else None,
                        new_notes.strip() or None,
                        new_agency.strip() or None,
                        new_due.strip() or None
                    )
                    st.success("Deal created.")
                    st.rerun()

        # Pipeline editor (grid)
        st.markdown("#### Pipeline editor")
        if df.empty:
            st.info("No deals yet. Add your first deal above.")
        else:
            edited = st.data_editor(
                df[["id","title","stage","owner","amount","agency","due_date","notes"]],
                key="deals_editor",
                num_rows="dynamic",
                column_config={
                    "id": st.column_config.NumberColumn("ID", disabled=True),
                    "stage": st.column_config.SelectboxColumn("Stage", options=DEAL_STAGES),
                    "amount": st.column_config.NumberColumn("Amount", step=100.0, format="%.2f"),
                    "notes": st.column_config.TextColumn("Notes", width="medium"),
                    "title": st.column_config.TextColumn("Title", width="medium"),
                    "agency": st.column_config.TextColumn("Agency"),
                    "owner": st.column_config.TextColumn("Owner"),
                    "due_date": st.column_config.TextColumn("Due date"),
                },
                hide_index=True
            )
            import pandas as pd
            changes = []
            for _, row in edited.iterrows():
                orig = df.loc[df["id"]==row["id"]].iloc[0]
                updates = {}
                for col in ["title","stage","owner","amount","notes","agency","due_date"]:
                    if pd.isna(row[col]) and pd.isna(orig[col]):
                        continue
                    if (row[col] != orig[col]) and not (pd.isna(row[col]) and orig[col] in ("", None)):
                        updates[col] = None if (isinstance(row[col], float) and pd.isna(row[col])) else row[col]
                if updates:
                    ok = update_deal(int(row["id"]), **updates)
                    if ok:
                        changes.append((int(row["id"]), updates))
            if changes:
                st.success(f"Saved {len(changes)} change(s).")

        # Kanban board
        st.divider()
        st.markdown("### Board view")
        st.caption("Column per stage with quick add, inline edits, and move.")

        df_board = list_deals(stage=None, q=q)
        import pandas as _pd2
        _counts = df_board.groupby("stage")["id"].count() if not df_board.empty else _pd2.Series(dtype=int)
        _amounts = _pd2.to_numeric(df_board["amount"], errors="coerce").fillna(0.0) if not df_board.empty else _pd2.Series(dtype=float)
        _totals = _amounts.groupby(df_board["stage"]).sum() if not df_board.empty else _pd2.Series(dtype=float)
        grand_total = float(_amounts.sum()) if not df_board.empty else 0.0
        st.markdown(f"**Total pipeline value:** ${grand_total:,.2f}")

        cols = st.columns(len(DEAL_STAGES))
        for i, stage_name in enumerate(DEAL_STAGES):
            with cols[i]:
                st.markdown(f"#### {stage_name}")
                st.caption(f"{int(_counts.get(stage_name, 0))} deals • ${float(_totals.get(stage_name, 0.0)):,.2f}")

                # Quick add in this stage
                with st.container(border=True):
                    _new_title = st.text_input("New deal title", key=f"quick_new_title_{i}")
                    qa1, qa2 = st.columns([1,1])
                    with qa1:
                        _new_owner = st.text_input("Owner", key=f"quick_new_owner_{i}")
                    with qa2:
                        _new_amount = st.number_input("Amount", min_value=0.0, step=100.0, value=0.0, format="%.2f", key=f"quick_new_amt_{i}")
                    if st.button("New in this stage", key=f"quick_new_btn_{i}"):
                        if _new_title.strip():
                            create_deal(_new_title.strip(), stage_name, _new_owner.strip() or None, float(_new_amount) if _new_amount else None, None, None, None)
                            st.success("Deal created")
                            st.rerun()
                        else:
                            st.warning("Enter a title first")

                # Cards for deals in this stage
                stage_rows = df_board[df_board["stage"] == stage_name]
                for _, row in stage_rows.iterrows():
                    with st.container(border=True):
                        st.markdown(f"**{row['title']}**")
                        st.caption(f"Owner: {row['owner'] or 'Unassigned'}  •  Amount: ${float(row['amount'] or 0):,.2f}")
                        kc1, kc2 = st.columns([1,1])
                        with kc1:
                            new_owner = st.text_input("Owner", value=row["owner"] or "", key=f"owner_{row['id']}")
                        with kc2:
                            new_amt = st.number_input("Amount", value=float(row["amount"] or 0.0), step=100.0, format="%.2f", key=f"amt_{row['id']}")
                        km1, km2 = st.columns([2,1])
                        with km1:
                            new_stage = st.selectbox("Move to", options=DEAL_STAGES, index=DEAL_STAGES.index(stage_name), key=f"mv_{row['id']}")
                        with km2:
                            if st.button("Save", key=f"save_{row['id']}"):
                                changes = {}
                                if new_owner != (row["owner"] or ""):
                                    changes["owner"] = new_owner or None
                                if float(new_amt) != float(row["amount"] or 0.0):
                                    changes["amount"] = float(new_amt)
                                if new_stage != row["stage"]:
                                    changes["stage"] = new_stage
                                if changes:
                                    ok = update_deal(int(row["id"]), **changes)
                                    if ok:
                                        st.success("Updated")
                                        st.rerun()
                                    else:
                                        st.error("No changes saved")

        # Danger zone
        with st.expander("Danger zone: delete a deal"):
            del_id = st.number_input("Deal ID to delete", min_value=1, step=1, value=1)
            if st.button("Delete deal"):
                if delete_deal(int(del_id)):
                    st.warning(f"Deleted deal {int(del_id)}.")
                    st.rerun()
                else:
                    st.error("Delete failed or ID not found.")
except Exception as _e_deals:
    try:
        st.caption(f"[Deals tab note: {_e_deals}]")
    except Exception:
        pass



# Convenience: call this from your Outreach tab after assembling fields
def outreach_send(to: str, subject: str, body_html: str, cc: str = "", bcc: str = "", attachments=None):
    return outreach_send_from_active_user(to, subject, body_html, cc=cc, bcc=bcc, attachments=attachments)


# === AI Compliance and CO-Minded Proposal Utilities ===

from typing import List, Tuple, Dict
import re
import math

# Safe syllable count for readability
_vowels = "aeiouy"

def _count_syllables(word: str) -> int:
    w = word.lower()
    if not w:
        return 0
    count = 0
    prev_vowel = False
    for ch in w:
        is_vowel = ch in _vowels
        if is_vowel and not prev_vowel:
            count += 1
        prev_vowel = is_vowel
    if w.endswith("e") and count > 1:
        count -= 1
    return max(count, 1)

def flesch_kincaid_grade(text: str) -> float:
    sentences = max(len(re.findall(r'[.!?]+', text)) , 1)
    words = re.findall(r"[A-Za-z0-9']+", text)
    word_count = max(len(words), 1)
    syllables = sum(_count_syllables(w) for w in words) or 1
    # Flesch Kincaid Grade Level
    return 0.39 * (word_count / sentences) + 11.8 * (syllables / word_count) - 15.59

# Extract key terms and headings from SOW or PWS
SECTION_HINTS = [
    "Scope", "Scope of Work", "Performance Work Statement", "PWS",
    "Statement of Work", "SOW", "Deliverables", "Period of Performance",
    "Place of Performance", "Quality Assurance", "QA", "Quality Control",
    "QC", "Safety", "Standards", "Reports", "Schedule", "Milestones",
    "Contract Type", "Evaluation", "Evaluation Criteria", "CLIN",
    "Security", "Travel", "Invoicing", "Acceptance", "Inspection"
]

def extract_keywords_and_sections(src: str) -> Dict[str, List[str]]:
    # Keywords: all capitalized multi word phrases and section hints
    tokens = re.findall(r'\b[A-Z][A-Za-z0-9\-\/&]+\b', src)
    # Keep meaningful unique tokens
    cap_keywords = sorted(set(t for t in tokens if len(t) > 2))
    # Section heads: lines that look like headings
    lines = src.splitlines()
    heads = []
    for ln in lines:
        if len(ln) <= 2:
            continue
        if re.match(r'^\s*(\d+(\.\d+)*)?\s*[A-Z][A-Za-z0-9 \-\/&]{3,}$', ln.strip()):
            heads.append(ln.strip())
    hints = [h for h in SECTION_HINTS if h.lower() in src.lower()]
    return {
        "keywords": cap_keywords[:300],
        "sections": heads[:100],
        "hints": hints
    }

# Compliance checklist sections
REQUIRED_SECTIONS = [
    "Executive Summary",
    "Technical Approach",
    "Management Plan",
    "Staffing Plan",
    "Quality Assurance",
    "Risk Mitigation",
    "Schedule",
    "Past Performance",
    "Pricing",
    "Assumptions and Constraints"
]

def find_missing_sections(draft_text: str) -> List[str]:
    low = draft_text.lower()
    missing = []
    for sec in REQUIRED_SECTIONS:
        if sec.lower() not in low:
            missing.append(sec)
    return missing

# Simple evaluator scoring rubric that mirrors common federal evaluations
EVAL_WEIGHTS = {
    "Technical Capability": 0.35,
    "Management Approach": 0.20,
    "Past Performance": 0.20,
    "Quality Assurance": 0.15,
    "Risk Mitigation": 0.10
}

def _has_any(text: str, terms: List[str]) -> bool:
    t = text.lower()
    return any(term.lower() in t for term in terms)

def score_proposal_against_rubric(draft: str, sow: str) -> Dict[str, float]:
    scores = {}
    # Technical Capability: references to meeting PWS tasks and deliverables
    tech_terms = ["pws", "sow", "deliverable", "task order", "acceptance", "inspection", "standard", "section"]
    tech_score = 1.0 if _has_any(draft, tech_terms) and _has_any(draft, extract_keywords_and_sections(sow)["hints"]) else 0.7
    # Management Approach: schedule, roles, reporting, communication
    mgmt_terms = ["schedule", "transition", "organizational chart", "roles", "responsibilities", "escalation", "communication", "supervisor"]
    mgmt_score = 1.0 if _has_any(draft, mgmt_terms) else 0.6
    # Past Performance
    pp_terms = ["past performance", "contract number", "cpars", "references", "similar"]
    pp_score = 1.0 if _has_any(draft, pp_terms) else 0.5
    # Quality Assurance
    qa_terms = ["quality assurance", "quality control", "qa", "qc", "inspection", "checklist", "kpi"]
    qa_score = 1.0 if _has_any(draft, qa_terms) else 0.6
    # Risk Mitigation
    risk_terms = ["risk", "mitigation", "contingency", "proactive", "issue", "corrective action"]
    risk_score = 1.0 if _has_any(draft, risk_terms) else 0.5

    components = {
        "Technical Capability": tech_score,
        "Management Approach": mgmt_score,
        "Past Performance": pp_score,
        "Quality Assurance": qa_score,
        "Risk Mitigation": risk_score
    }
    for k, base in components.items():
        scores[k] = round(base * EVAL_WEIGHTS[k] * 100, 1)

    total = round(sum(scores.values()), 1)
    scores["Total"] = total
    return scores

# Auto detect risks and propose mitigations
RISK_LIBRARY = [
    ("Remote site logistics", ["remote", "rural", "island"], "Pre stage materials and use local subs"),
    ("Tight schedule", ["accelerated", "expedite", "short notice", "compressed"], "Parallel tasking and add surge staff"),
    ("Hazardous work", ["hazard", "osha", "confined space", "asbestos", "lead"], "Site safety plan and qualified PPE"),
    ("Security and access", ["secret", "clearance", "escort", "badging"], "Advance badging and backup staff with clearances"),
    ("Supply chain", ["lead time", "backorder", "long lead"], "Approved alternates and buffer stock"),
    ("After hours work", ["after hours", "off hours", "weekend", "night"], "Noise control and CO approved schedule")
]

def identify_risks(src: str) -> List[Tuple[str, str]]:
    low = src.lower()
    found = []
    for name, triggers, mitigation in RISK_LIBRARY:
        if any(t in low for t in triggers):
            found.append((name, mitigation))
    return found

def propose_outline_with_mirrored_terms(sow_text: str) -> str:
    ex = extract_keywords_and_sections(sow_text)
    hints = ", ".join(ex["hints"][:8])
    outline = f"""Executive Summary
Technical Approach
  Alignment to PWS and SOW sections: {hints}
  Tasks and Deliverables
  Methods and Standards
Management Plan
  Roles and Responsibilities
  Communication and Reporting
  Schedule and Milestones
Staffing Plan
  Key Personnel Qualifications
  Subcontractor Roles
Quality Assurance
  Inspection and Acceptance
  KPIs and Checklists
Risk Mitigation
  Identified Risks and Proactive Controls
Past Performance
  Relevant Contracts and Outcomes
Pricing
  Assumptions and Basis of Estimate
Assumptions and Constraints
"""
    return outline

def clean_placeholders(text: str) -> str:
    # Remove INSERT style placeholders while keeping content safe
    return re.sub(r"\bINSERT[\w\s\-:]*\b", "", text, flags=re.IGNORECASE)

def compliance_assess(draft_text: str, sow_text: str) -> Dict[str, object]:
    fk = round(flesch_kincaid_grade(draft_text), 2)
    missing = find_missing_sections(draft_text)
    ex = extract_keywords_and_sections(sow_text)
    risks = identify_risks(sow_text + "\n" + draft_text)
    scores = score_proposal_against_rubric(draft_text, sow_text)
    return {
        "fk_grade": fk,
        "missing_sections": missing,
        "mirrored_terms_sample": ex["hints"][:10],
        "risks": risks,
        "scores": scores
    }

# Optional Streamlit UI injection guarded to avoid import errors
def mount_compliance_assistant():
    try:
        import streamlit as st
    except Exception:
        return
    with st.expander("Proposal Compliance Assistant", expanded=False):
        sow = st.text_area("Paste Solicitation or SOW text", height=200, key="co_sow_text")
        draft = st.text_area("Paste Proposal Draft text", height=200, key="co_draft_text")
        if st.button("Run Compliance Check"):
            result = compliance_assess(draft, sow)
            st.write("Flesch Kincaid grade:", result["fk_grade"])
            st.write("Missing sections:", result["missing_sections"] or "None")
            st.write("Mirrored terms to use:", result["mirrored_terms_sample"] or "None")
            if result["risks"]:
                st.write("Detected risks and mitigations:")
                for r, m in result["risks"]:
                    st.write(f"- {r}: {m}")
            st.write("Evaluator style scores:", result["scores"])
        if st.button("Suggest Outline from SOW"):
            st.code(propose_outline_with_mirrored_terms(sow), language="markdown")
        if st.button("Clean Placeholders in Draft"):
            st.text_area("Cleaned Draft", clean_placeholders(draft), height=200, key="co_cleaned_draft_out")

# Attempt to mount automatically if Streamlit is present
try:
    pass  # auto-inserted to fix empty try block
#     mount_compliance_assistant()
except Exception:
    pass

# --- Placeholder cleaner (injected) ---
def _clean_placeholders(text: str) -> str:
    """Remove obvious template placeholders without touching normal words.
    Rules:
      - Remove bracketed placeholders like [INSERT ...], {PLACEHOLDER}, <TBD>, ((TODO))
      - Remove isolated ALL-CAPS tokens commonly used as placeholders (INSERT, TBD, TODO)
      - Collapse multiple spaces to one
    """
    import re
    if not text:
        return text
    out = text
    # Remove bracketed placeholders of common bracket styles
    out = re.sub(r"\[[^\]]*(?i:(insert|placeholder|tbd|todo))[^\]]*\]", "", out)
    out = re.sub(r"\{[^\}]*?(?i:(insert|placeholder|tbd|todo))[^\}]*\}", "", out)
    out = re.sub(r"<[^>]*?(?i:(insert|placeholder|tbd|todo))[^>]*?>", "", out)
    out = re.sub(r"\(\([^\)]*?(?i:(insert|placeholder|tbd|todo))[^\)]*?\)\)", "", out)
    # Remove standalone ALL-CAPS tokens
    out = re.sub(r"(?m)^\s*\b(INSERT|TBD|TODO)\b\s*:?.*$", "", out)
    # Remove repeated underscores or lines of underscores
    out = re.sub(r"_{3,}", "", out)
    # Clean up lingering 'lorem ipsum'
    out = re.sub(r"(?i)lorem ipsum[\s\S]*?(?=\n\n|$)", "", out)
    # Collapse spaces
    out = re.sub(r"[ \t]{2,}", " ", out)
    # Tidy blank lines
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


# --- Robust placeholder and artifact cleaner for proposal exports ---
import re as _re_clean

def _clean_placeholders(text: str) -> str:
    if not isinstance(text, str):
        return text
    t = text
    t = _re_clean.sub(r"\[[^\]]*?INSERT[^\]]*\]", "", t, flags=_re_clean.IGNORECASE)
    t = _re_clean.sub(r"\[[^\]]*?PLACEHOLDER[^\]]*\]", "", t, flags=_re_clean.IGNORECASE)
    t = _re_clean.sub(r"\{[^}]*?(INSERT|TBD|PLACEHOLDER)[^}]*\}", "", t, flags=_re_clean.IGNORECASE)
    t = _re_clean.sub(r"<[^>]*?(INSERT|TBD|PLACEHOLDER)[^>]*>", "", t, flags=_re_clean.IGNORECASE)
    t = _re_clean.sub(r"\(\([^)]*?(INSERT|TBD|PLACEHOLDER)[^)]*\)\)", "", t, flags=_re_clean.IGNORECASE)
    t = _re_clean.sub(r"\b(INSERT|TBD|TO BE DETERMINED|LOREM IPSUM|FILL ME|PLACEHOLDER)\b[:\-]*", "", t, flags=_re_clean.IGNORECASE)
    t = _re_clean.sub(r"\n?_{3,}\n?", "\n", t)
    t = _re_clean.sub(r"\.{4,}", "", t)
    t = _re_clean.sub(r"<<[^>]*>>", "", t)
    t = _re_clean.sub(r"\n{3,}", "\n\n", t)
    t = "\n".join(line.rstrip() for line in t.splitlines())
    return t


# === [MERGE] Backend: SAM search v2 and helpers (final) ===
try:
    _ = sam_search_v3
    _SAM_SEARCH_DEFINED = True
except NameError:
    _SAM_SEARCH_DEFINED = False

import json as _json
import pandas as _pd
import requests as _requests
from datetime import datetime as _dt, timedelta as _td

def sam_search_v3(filters: dict, limit: int = 100):
    """
    Public Get Opportunities API v2 with required dates.
    postedFrom and postedTo are MM/dd/YYYY. Defaults to last 30 days.
    Maps: keywords->title, naics list->ncode one at a time, setAside->typeOfSetAside, noticeType->ptype.
    """
    if not ('SAM_API_KEY' in globals() and SAM_API_KEY):
        return _pd.DataFrame(), {"ok": False, "reason": "missing_key"}

    today = _dt.utcnow().date()
    posted_from = filters.get("postedFrom") or (today - _td(days=30)).strftime("%m/%d/%Y")
    posted_to = filters.get("postedTo") or today.strftime("%m/%d/%Y")

    def _one_call(single_naics: str | None):
        params = {
            "api_key": SAM_API_KEY,
            "postedFrom": posted_from,
            "postedTo": posted_to,
            "limit": str(int(limit)),
            "offset": "0",
        }
        if filters.get("keywords"):
            params["title"] = filters["keywords"]
        if single_naics:
            params["ncode"] = single_naics
        if filters.get("setAside"):
            params["typeOfSetAside"] = filters["setAside"]
        if filters.get("noticeType"):
            v = str(filters["noticeType"]).lower()
            p = None
            if "combined" in v: p = "k"
            elif "solicitation" in v: p = "o"
            elif "sources" in v: p = "r"
            elif "special" in v: p = "s"
            elif "award" in v: p = "a"
            if p: params["ptype"] = p
        if str(filters.get("active","true")).lower() == "true":
            params["status"] = "active"

        base = "https://api.sam.gov/opportunities/v2/search"
        r = _requests.get(base, params=params, timeout=45)
        js = r.json() if "application/json" in r.headers.get("Content-Type","") else {}
        if r.status_code != 200:
            return _pd.DataFrame(), {"ok": False, "status": r.status_code, "message": (js.get("message") or r.text)[:400], "params": params}

        items = js.get("opportunitiesData", []) or js.get("data", []) or []
        rows = []
        for opp in items:
            title = opp.get("title") or opp.get("Title")
            sol = opp.get("solicitationNumber") or opp.get("solnum") or opp.get("noticeid")
            agency = opp.get("fullParentPathName") or opp.get("organizationName")
            posted = opp.get("postedDate") or opp.get("publishedDate")
            due = opp.get("reponseDeadLine") or opp.get("responseDeadLine") or ""
            naics = opp.get("naicsCode") or ""
            psc = opp.get("classificationCode") or ""
            nid = opp.get("noticeid") or opp.get("noticeId") or sol
            url = f"https://sam.gov/opp/{nid}/view"

            rows.append({
                "sam_notice_id": nid,
                "title": title,
                "agency": agency,
                "naics": naics,
                "psc": psc,
                "place_of_performance": "",
                "response_due": due,
                "posted": posted,
                "type": opp.get("type") or opp.get("baseType"),
                "set_aside": opp.get("setAside") or opp.get("setAsideCode") or "",
                "url": url,
                "attachments_json": "[]"
            })
        return _pd.DataFrame(rows), {"ok": True, "count": len(rows), "params": params}

    naics_list = filters.get("naics") or []
    if isinstance(naics_list, list) and len(naics_list) > 1:
        frames = []
        for code in naics_list:
            df, _ = _one_call(code)
            if not df.empty:
                frames.append(df)
        if frames:
            df_all = _pd.concat(frames).drop_duplicates(subset=["sam_notice_id"])
            return df_all, {"ok": True, "count": len(df_all)}
        return _pd.DataFrame(), {"ok": True, "count": 0}
    else:
        code = naics_list[0] if isinstance(naics_list, list) and naics_list else None
        return _one_call(code)

def _sam_get_saved_filters():
    try:
        raw = get_setting("sam_saved_filters","")
        return _json.loads(raw) if raw else []
    except Exception:
        return []

def _sam_set_saved_filters(filters_list):
    try:
        set_setting("sam_saved_filters", _json.dumps(filters_list))
    except Exception:
        pass

def import_sam_to_db(filters: dict, stage_on_insert: str = "No Contact Made"):
    df, info = sam_search_v3(filters, limit=200)
    if not info.get("ok"):
        return 0, info
    try:
        conn = get_db(); cur = conn.cursor()
        inserted = 0
        for _, r in df.iterrows():
            nid = r.get("sam_notice_id")
            if not nid:
                continue
            exists = cur.execute("select id from opportunities where sam_notice_id=? limit 1", (nid,)).fetchone()
            if exists:
                continue
            cur.execute("""insert into opportunities
                (sam_notice_id, title, agency, naics, psc, place_of_performance, response_due, posted, type, url, attachments_json, status)
                values(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (r.get("sam_notice_id"), r.get("title"), r.get("agency"), r.get("naics"), r.get("psc"),
                 r.get("place_of_performance"), r.get("response_due"), r.get("posted"), r.get("type"),
                 r.get("url"), r.get("attachments_json"), stage_on_insert))
            inserted += 1
        conn.commit()
        if inserted > 0:
            try:
                _send_team_alert(f"New SAM opportunities imported: {inserted}")
            except Exception:
                pass
        return inserted, info
    except Exception as e:
        return 0, {"ok": False, "reason": "db_error", "detail": str(e)[:300]}

try:
    _ = proposal_quick_quote
except NameError:
    def _get_opp(opp_id: int):
        try:
            conn = get_db()
            row = conn.execute("select * from opportunities where id=?", (int(opp_id),)).fetchone()
            if not row:
                return {}
            info = conn.execute("PRAGMA table_info(opportunities)").fetchall()
            colnames = [c[1] for c in info]
            return dict(zip(colnames, row))
        except Exception:
            return {}

    def proposal_quick_quote(opp_id: int) -> str:
        opp = _get_opp(opp_id)
        if not opp:
            return ""
        title = f"Quick Quote - {opp.get('title','Untitled')} ({opp.get('sam_notice_id','')})"
        body = f"""# {opp.get('title','')}

**Solicitation #:** {opp.get('sam_notice_id','')}
**Agency/Office:** {opp.get('agency','')}
**NAICS/PSC:** {opp.get('naics','')} / {opp.get('psc','')}
**Due date:** {opp.get('response_due','')}
**Contact:** _INSERT POC_

---
## Technical Approach
INSERT

## Pricing
INSERT

## Past Performance
INSERT

## Compliance Checklist
- SAM registration verified
- Past performance attached
- Pricing confirmed
- Reps & Certs complete
- Forms signed

"""
        path = save_proposal_draft(title, body)
        try:
            conn = get_db()
            conn.execute("insert into tasks(opp_id,title,assignee,status) values(?,?,?,?)",
                         (int(opp_id), "Proposal Started", "", "Open"))
            conn.execute("update opportunities set status=? where id=?", ("Proposal Started", int(opp_id)))
            conn.commit()
            try:
                _send_team_alert(f"Proposal started for opp #{opp_id}: {opp.get('title','')}")
            except Exception:
                pass
        except Exception:
            pass
        return path

    def proposal_submit_package(opp_id: int) -> bool:
        try:
            conn = get_db()
            conn.execute("update opportunities set status=? where id=?", ("Submitted", int(opp_id)))
            conn.execute("insert into tasks(opp_id,title,status) values(?,?,?)", (int(opp_id), "Package Submitted", "Closed"))
            conn.commit()
            try:
                _send_team_alert(f"Package submitted for opp #{opp_id}.")
            except Exception:
                pass
            return True
        except Exception:
            return False

try:
    _ = ensure_default_checklist
except NameError:
    def ensure_default_checklist(opp_id: int):
        items = [
            "SAM registration verified",
            "Past performance attached",
            "Pricing confirmed",
            "Technical approach drafted",
            "Reps & Certs completed",
            "Forms signed"
        ]
        try:
            conn = get_db(); cur = conn.cursor()
            for it in items:
                cur.execute(
                    "insert into compliance_items(opp_id, item, required, status, source) values(?,?,?,?,?)",
                    (int(opp_id), it, 1, "Pending", "Checklist")
                )
            conn.commit()
        except Exception:
            pass

def _send_team_alert(msg: str):
    try:
        addrs = [
            USER_EMAILS.get("Quincy",""),
            USER_EMAILS.get("Charles",""),
            USER_EMAILS.get("Collin",""),
        ]
        addrs = [a for a in addrs if a]
        if not addrs:
            return
        subj = "ELA Bid Alert"
        body = f"<p>{msg}</p><p>Open the app to review in Pipeline.</p>"
        for a in addrs:
            try:
                send_outreach_email("Charles", a, subj, body)
            except Exception:
                pass
    except Exception:
        pass
# === [END MERGE] Backend ===



# === [MERGE UI] SAM Watch — Minimal UI (final) ===
try:
    import streamlit as _st
    _ = tabs; _ = TAB

    def _mk_filter(kw, naics_csv, set_aside, notice, min_due, active_only):
        return {
            "name": "Default",
            "keywords": kw.strip(),
            "naics": [s.strip() for s in naics_csv.split(",") if s.strip()],
            "setAside": "Total Small Business" if set_aside == "Total Small Business" else "",
            "noticeType": "Combined Synopsis/Solicitation,Solicitation" if notice != "Any" else "",
            "active": "true" if active_only else "false",
            "minDueDays": int(min_due)
        }

    with tabs[TAB['SAM Watch']]:
        _st.header("SAM Watch")
        _st.subheader("Filters")
        with _st.form("simple_filters", clear_on_submit=False):
            c1, c2, c3 = _st.columns([2,2,2])
            with c1:
                kw = _st.text_input("Keywords", value="janitorial")
            with c2:
                naics = _st.text_input("NAICS list", value="561720, 238220")
            with c3:
                set_aside = _st.selectbox("Set aside", ["Any", "Total Small Business"], index=1)
            c4, c5, c6 = _st.columns([2,2,2])
            with c4:
                notice = _st.selectbox("Notice type", ["Any", "Combined Synopsis/Solicitation", "Solicitation"], index=1)
            with c5:
                min_due = _st.number_input("Min days until due", min_value=0, value=3, step=1)
            with c6:
                active_only = _st.checkbox("Active only", value=True)
            save_search = _st.form_submit_button("Save as default")

        if save_search:
            _sam_set_saved_filters([_mk_filter(kw, naics, set_aside, notice, min_due, active_only)])
            _st.success("Default filter saved")

        _st.subheader("Actions")
        colA, colB, colC, colD = _st.columns([1,1,1,1])
        with colA:
            if _st.button("Pull data", use_container_width=True):
                import pandas as _pd
                loaded = []
                for flt in _sam_get_saved_filters():
                    df, info = sam_search_v3(flt, limit=200)
                    if isinstance(df, _pd.DataFrame) and not df.empty:
                        loaded.append(df)
                if loaded:
                    _st.session_state["sam_results_df"] = _pd.concat(loaded, ignore_index=True)
                    _st.success(f"Loaded {_st.session_state['sam_results_df'].shape[0]} opportunities (not saved).")
                else:
                    _st.info("No opportunities found.")
        with colB:
            opp_id = _st.number_input("Opp ID", min_value=0, value=0, step=1)
        with colC:
            if _st.button("Generate quote", use_container_width=True) and opp_id:
                p = proposal_quick_quote(int(opp_id))
                _st.success("Draft created" if p else "Draft failed")
        with colD:
            if _st.button("Submit package", use_container_width=True) and opp_id:
                ok = proposal_submit_package(int(opp_id))
                _st.success("Submitted") if ok else _st.error("Update failed")

                _st.subheader("Select opportunities to add to Pipeline")

        # Preview the most recent SAM fetch (not yet saved to DB)
        try:
            _df_preview = _st.session_state.get("sam_results_df", None)
            if isinstance(_df_preview, _pd.DataFrame) and not _df_preview.empty:
                
                _st.caption("Latest fetched results (not saved to DB yet). Use the section below to add selected items to the Pipeline.")

                # Make a light preview with common columns if they exist
                cols_pref = [c for c in ["solicitationNumber","title","agency","posted","response_due","type","naics","set_aside","url"] if c in _df_preview.columns]
                _df_disp = _df_preview[cols_pref].copy() if cols_pref else _df_preview.copy()

                # Normalize URL column name
                url_candidates = [c for c in ["url","URL","link","Link","notice_url","solicitation_url"] if c in _df_disp.columns]
                _url_col = url_candidates[0] if url_candidates else None

                try:
                    # Prefer native clickable links if Streamlit supports LinkColumn
                    if _url_col:
                        from streamlit import column_config as _cc  # type: ignore
                        _conf = {}
                        # If a title exists, show the title as text and provide an "Open" link column
                        if "title" in _df_disp.columns:
                            _conf[_url_col] = _cc.LinkColumn("Open notice", display_text="Open")
                        else:
                            _conf[_url_col] = _cc.LinkColumn("Notice URL")
                        _st.dataframe(_df_disp, use_container_width=True, height=420, column_config=_conf, hide_index=True)
                    else:
                        _st.dataframe(_df_disp, use_container_width=True, height=420, hide_index=True)
                except Exception:
                    # Fallback: render markdown table with clickable links
                    if _url_col:
                        _df_md = _df_disp.copy()
                        def _mk(a, u):
                            try:
                                return f"[{a}]({u})" if (isinstance(u, str) and u.startswith("http")) else a
                            except Exception:
                                return a
                        if "title" in _df_md.columns:
                            _df_md["title"] = [_mk(t, u) for t, u in zip(_df_md.get("title"), _df_md.get(_url_col))]
                        else:
                            # if no title, just convert url column to markdown link
                            _df_md[_url_col] = [f"[Open]({u})" if isinstance(u, str) and u.startswith("http") else u for u in _df_md.get(_url_col)]
                        _st.markdown(_df_md.to_markdown(index=False), unsafe_allow_html=True)
                    else:
                        _st.dataframe(_df_disp, use_container_width=True, height=420, hide_index=True)
        except Exception as _e_preview:
            _st.warning(f"[SAM Watch preview error: {_e_preview}]")
        try:
            conn = get_db(); cur = conn.cursor()
            rows = cur.execute("""
                select id, title, agency, response_due, url, posted
                from opportunities
                where coalesce(url,'') != ''
                order by date(posted) desc, id desc
                limit 200
            """).fetchall()

            # Use a form so checkbox selections and the submit happen in one transaction (avoids rerun desync).
            with _st.form("sam_watch_select_form", clear_on_submit=False):
                row_ids = []
                if rows:
                    for rid, title, agency, due, url, posted in rows:
                        row_ids.append(rid)
                        c1, c2 = _st.columns([0.08, 0.92])
                        with c1:
                            _st.checkbox(
                                "",
                                key=f"sam_sel_{rid}",
                                value=_st.session_state.get(f"sam_sel_{rid}", False)
                            )
                        with c2:
                            link_md = f"[{title}]({url})"
                            meta = " | ".join(filter(None, [
                                f"Agency: {agency}" if agency else "",
                                f"Due: {due}" if due else "",
                                f"Posted: {posted}" if posted else ""
                            ]))
                            _st.markdown(
                                link_md + (f"<br/><span style='font-size: 12px;'>{meta}</span>" if meta else ""),
                                unsafe_allow_html=True
                            )

                submitted = _st.form_submit_button("➕ Add Selected to Pipeline", use_container_width=True)

            if submitted:
                try:
                    _st.session_state["__ALLOW_PIPELINE_WRITE"] = True
                except Exception:
                    pass
                chosen_ids = [rid for rid in row_ids if _st.session_state.get(f"sam_sel_{rid}", False)]
                if not chosen_ids:
                    _st.info("No rows selected.")
                else:
                    added, skipped = 0, 0
                    for rid, title, agency, due, url, posted in [r for r in rows if r[0] in chosen_ids]:
                        try:
                            c2 = conn.cursor()
                            exists = c2.execute(
                                "select 1 from deals where title=? and coalesce(due_date,'')=coalesce(?, '') limit 1",
                                (title, str(due) if due else None)
                            ).fetchone()
                            if exists:
                                skipped += 1
                                continue
                            notes = f"Imported from SAM Watch on selection. URL: {url}"
                            create_deal(
                                title=title,
                                stage="No Contact Made",
                                owner=None,
                                amount=None,
                                notes=notes,
                                agency=agency,
                                due_date=str(due) if due else None
                            )
                            added += 1
                        except Exception as _e_add:
                            _st.warning(f"Could not add '{title}': {_e_add}")
                    _st.success(f"Added {added} deal(s). Skipped {skipped} duplicate(s).")
                    try:
                        _st.session_state["__ALLOW_PIPELINE_WRITE"] = False
                    except Exception:
                        pass
                    # Clear only the ones we just added to avoid accidental re-use
                    for rid in chosen_ids:
                        _st.session_state.pop(f"sam_sel_{rid}", None)
            else:
                if not rows:
                    _st.caption("No opportunities found with links.")
        except Exception as _e_sel:
            _st.warning(f"[Selection UI note: {_e_sel}]")
except Exception as _e_ui:
    try:
        import streamlit as _st
        _st.warning(f"[SAM Watch UI note: {_e_ui}]")
    except Exception:
        pass

# === [END MERGE UI] ===