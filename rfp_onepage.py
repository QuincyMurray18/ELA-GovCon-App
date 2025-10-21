
import streamlit as st
from typing import List, Dict
from finders import find_due_date, find_naics_setaside, find_pocs, find_eval_criteria, find_pop, find_clins

def run_rfp_analyzer_onepage(pages: List[Dict]) -> None:
    st.header("RFP Analyzer")
    due = find_due_date(pages)
    meta = find_naics_setaside(pages)
    pop  = find_pop(pages)
    pocs = find_pocs(pages)[:2]
    clins = find_clins(pages)[:3]

    c1,c2,c3,c4,c5 = st.columns([2,2,2,2,2])
    c1.metric("Due date", due.get("date_iso") or "Not found")
    c2.metric("NAICS", meta.get("naics") or "Not found")
    c3.metric("Set-aside", meta.get("set_aside") or "Not found")
    c4.metric("POP (base months)", pop.get("base_months") or "—")
    c5.metric("CLINs parsed", str(len(clins)))

    st.divider()
    left, right = st.columns([7,3])

    with left:
        st.subheader("CO Readout")
        st.markdown("**Key dates**")
        st.write(f"- Offer Due: {due.get('date_iso') or 'Not found'}")
        st.caption(due.get("source_line",""))

        st.markdown("**POCs**")
        if pocs:
            for r in pocs:
                st.write(f"- {r['name']} — {r['email']} {('• ' + r['phone']) if r['phone'] else ''} {('• ' + r['role']) if r['role'] else ''}")
        else:
            st.write("Not found")

        st.markdown("**NAICS and set-aside**")
        st.write(f"- NAICS: {meta.get('naics') or 'Not found'}")
        st.write(f"- Set-aside: {meta.get('set_aside') or 'Not found'}")

        st.markdown("**Evaluation**")
        ev = find_eval_criteria(pages)
        st.write(f"- Mode: {ev['mode']}")
        if ev['factors']:
            st.write("- Factors: " + ", ".join(ev['factors']))

        st.markdown("**CLINs summary**")
        if clins:
            for r in clins:
                st.write(f"- CLIN {r['clin']} {r['qty']} {r['unit']} — {r['description']}")
        else:
            st.write("Not found")

        st.markdown("**Missing**")
        miss = []
        if not due.get("date_iso"): miss.append("Offer Due date")
        if not meta.get("naics"): miss.append("NAICS")
        if not meta.get("set_aside"): miss.append("Set-aside")
        if not clins: miss.append("CLINs")
        st.write(miss or "None")

    with right:
        st.subheader("Evidence")
        st.caption("Preview first 3 pages")
        shown = 0
        for p in pages:
            if shown >= 3: break
            st.markdown(f"**{p.get('file','')} p.{p.get('page','')}**")
            st.text((p.get('text','') or '')[:500])
            shown += 1
