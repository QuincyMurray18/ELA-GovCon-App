
import re
import textwrap
from typing import List, Dict, Any

import streamlit as st

# ---- Minimal AI helpers (mirror app helpers; safe fallbacks) ----
def _resolve_openai_client():
    try:
        # Reuse app-level providers if available
        if "get_ai" in globals():
            return globals()["get_ai"]()
        if "get_openai_client" in globals():
            return globals()["get_openai_client"]()
        if "get_ai_client" in globals():
            return globals()["get_ai_client"]()
    except Exception:
        pass
    try:
        from openai import OpenAI  # type: ignore
    except Exception:
        return None
    import os as _os
    key = (
        st.secrets.get("openai_api_key")
        or st.secrets.get("OPENAI_API_KEY")
        or _os.environ.get("OPENAI_API_KEY")
    )
    if not key:
        return None
    try:
        client = OpenAI(api_key=key)
        return client
    except Exception:
        return None

def _resolve_model():
    return st.secrets.get("openai_model") or st.secrets.get("OPENAI_MODEL") or "gpt-4o-mini"

def _ai_chat(prompt: str) -> str:
    client = _resolve_openai_client()
    if not client:
        return "AI response unavailable (no OpenAI key configured)."
    try:
        resp = client.chat.completions.create(
            model=_resolve_model(),
            messages=[
                {"role": "system", "content": "You are a contracts analyst writing precise, concise outputs tailored for federal RFPs."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        return f"AI response unavailable: {e}"

# ---- Simple extractors (regex) ----
def _extract_naics(text: str) -> str:
    m = re.search(r"NAICS[^0-9]*([0-9]{6})", text, re.IGNORECASE)
    return m.group(1) if m else ""

def _extract_due_date(text: str) -> str:
    # crude; looks for "due" "closing" etc.
    m = re.search(r"(?:due|closing|proposal (?:due|deadline))[:\s]+([A-Z][a-z]{2,9}\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})", text, re.IGNORECASE)
    return m.group(1) if m else ""

def _extract_pop_state(text: str) -> str:
    m = re.search(r"\b(?:Place of Performance|POP)[:\s]+([A-Za-z ,]+)\b", text, re.IGNORECASE)
    candidate = m.group(1) if m else ""
    m2 = re.search(r"\b(AL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b", candidate.upper())
    return m2.group(1) if m2 else ""

def _extract_evaluation_factors(text: str) -> str:
    # naive pull of "Section M" content lines
    m = re.search(r"SECTION\s*M[:\s\\-]+(.*?)(?:SECTION\s*[NQ]|ATTACHMENT|EVALUATION CRITERIA END)", text, re.IGNORECASE | re.DOTALL)
    if m:
        return re.sub(r"\s{3,}", "  ", m.group(1).strip())[:1200]
    return ""

def _sentences(text: str) -> List[str]:
    # basic split by period/semicolon/newlines
    chunks = re.split(r"(?<=[\.\?\!])\s+|\n+", text)
    return [c.strip() for c in chunks if c.strip()]

def _find_requirements(text: str) -> List[str]:
    reqs = []
    for s in _sentences(text):
        if re.search(r"\b(shall|must)\b", s, re.IGNORECASE):
            reqs.append(s)
    # de-dup
    seen = set()
    out = []
    for r in reqs:
        k = r.lower()
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out[:400]

# ---- Draft outline ----
DEFAULT_SECTIONS = [
    "Executive Summary",
    "Technical Approach",
    "Management & Staffing",
    "Quality Assurance / QC",
    "Past Performance",
    "Risk & Mitigation",
    "Pricing Narrative (non-cost)",
    "Compliance Matrix Response Summary"
]

def _draft_section(section: str, context: str) -> str:
    prompt = f"""
Using the following RFP context, draft the section **{section}**. 
- Keep it compliant and concise (<= 300 words).
- Mirror government tone; no marketing fluff.
- Cite specific obligations if relevant (quote short phrases).
RFP context (truncated):
{context[:6000]}
""".strip()
    return _ai_chat(prompt)

# ---- Public entrypoint ----
def run_rfp_analyzer_onepage(pages: List[Dict[str, Any]]) -> None:
    st.title("RFP Analyzer — One‑Page View")
    if not pages:
        st.info("No parsed pages were provided.")
        return

    # Combine texts
    by_file = {}
    for p in pages:
        by_file.setdefault(p.get("file") or "Unknown", []).append(p.get("text") or "")
    combined = "\n\n".join(["\n".join(v) for v in by_file.values()])

    # Header key facts
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("NAICS", _extract_naics(combined) or "—")
    with c2: st.metric("Due Date", _extract_due_date(combined) or "—")
    with c3: st.metric("POP (State)", _extract_pop_state(combined) or "—")
    with c4: st.metric("# Files", str(len(by_file)))

    # Summaries panel
    st.subheader("Summaries")
    if st.button("Summarize All Documents ▶", type="primary"):
        sums = {}
        for fname, texts in by_file.items():
            t = "\n".join(texts)
            prompt = f"Summarize the document '{fname}' for a federal proposal team. Use bullets and include key deliverables, dates, and Section L/M obligations.\n\n{t[:12000]}"
            sums[fname] = _ai_chat(prompt)
        st.session_state["onepage_summaries"] = sums
    sums = st.session_state.get("onepage_summaries") or {}
    if sums:
        for fname, ss in sums.items():
            with st.expander(f"Summary — {fname}", expanded=False):
                st.write(ss or "_No summary available._")

    # Compliance (auto-extracted)
    st.subheader("Compliance Snapshot (auto-extracted L/M obligations)")
    reqs = _find_requirements(combined)
    if not reqs:
        st.info("No clear 'shall/must' obligations detected. (Section L/M not found or documents are scanned.)")
    else:
        # If we have a draft, check light coverage
        draft_map = st.session_state.get("onepage_draft") or {}
        drafted_all = "\n\n".join(draft_map.values()) if draft_map else ""
        covered = 0
        for r in reqs[:100]:
            hit = (len(r) > 20 and r[:20].lower() in drafted_all.lower())
            st.checkbox(("✅ " if hit else "⬜️ ") + r, value=bool(hit), key=f"req_{abs(hash(r))}")
            covered += int(bool(hit))
        st.caption(f"Coverage (light heuristic): {covered} / {min(100, len(reqs))} shown.")

    # Drafting panel
    st.subheader("Proposal Draft")
    sel = st.multiselect("Sections to draft", DEFAULT_SECTIONS, default=DEFAULT_SECTIONS)
    if st.button("Draft All Sections ▶", type="primary", help="Generate a first-pass draft for the selected sections."):
        draft = {}
        for sec in sel:
            draft[sec] = _draft_section(sec, combined)
        st.session_state["onepage_draft"] = draft
    draft = st.session_state.get("onepage_draft") or {}
    if draft:
        for sec, body in draft.items():
            with st.expander(f"📝 {sec}", expanded=False):
                st.text_area("Text", value=body, height=240, key=f"ta_{abs(hash(sec))}")
    st.download_button(
        "Download Full Draft (Markdown)",
        data="\n\n".join([f"# {k}\n\n{v}" for k, v in (st.session_state.get('onepage_draft') or {}).items()]).encode("utf-8"),
        file_name="proposal_draft.md",
        mime="text/markdown",
        disabled=not bool(st.session_state.get("onepage_draft"))
    )

    # Search panel (simple)
    st.subheader("Quick Search (full text)")
    q = st.text_input("Find", placeholder="evaluation factor, CLIN, deliverables...")
    if q:
        hits = [ (i, s) for i, s in enumerate(_sentences(combined), start=1) if q.lower() in s.lower() ]
        if not hits:
            st.info("No matches.")
        else:
            for i, s in hits[:100]:
                st.write(f"**{i}.** {s}")
