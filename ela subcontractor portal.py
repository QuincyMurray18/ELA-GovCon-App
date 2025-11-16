
import streamlit as st
import pandas as pd
import os
from datetime import datetime, date
from zoneinfo import ZoneInfo

CSV_PATH = "ela_subcontractor_signups.csv"
LOGO_PATH = "ela_logo.png"
CENTRAL_TZ = ZoneInfo("America/Chicago")


def append_to_csv(record: dict, path: str = CSV_PATH) -> None:
    """Append one record to the CSV file, creating it if needed."""
    new_row = pd.DataFrame([record])

    if os.path.exists(path):
        try:
            existing = pd.read_csv(path)
            combined = pd.concat([existing, new_row], ignore_index=True)
        except Exception:
            combined = new_row
    else:
        combined = new_row

    combined.to_csv(path, index=False)


def build_time_options_12h() -> list[str]:
    """Return 12-hour time labels (every 30 minutes) from 7:00 AM to 7:30 PM."""
    labels: list[str] = []
    for hour in range(7, 20):  # 7 to 19
        for minute in (0, 30):
            dt = datetime(2000, 1, 1, hour, minute)
            labels.append(dt.strftime("%I:%M %p"))
    return labels


def show_public_form():
    """Public-facing subcontractor sign-up form with booking-aware time slots."""
    # Load existing bookings (if any) to block off taken slots
    existing_df = None
    if os.path.exists(CSV_PATH):
        try:
            existing_df = pd.read_csv(CSV_PATH)
        except Exception:
            existing_df = None

    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, width=200)

    st.title("ELA Management LLC")
    st.subheader("Join Our Subcontractor Partner Network")

    st.write(
        """
ELA Management LLC helps small businesses win and perform steady federal contracts as trusted partners.

If you are:

• Tired of waiting for work to walk through the door  
• Looking for more predictable revenue and cash flow  
• Interested in multi-year opportunities, not just one-off jobs  
• Ready to focus on performance while a prime handles the paperwork  

then this form is for you.

By joining our subcontractor partner network, your company can:

• Be considered for upcoming federal and commercial projects across multiple NAICS codes  
• Get access to one- to five-year contract opportunities when they are released  
• Reduce the time you spend chasing leads and cold opportunities  
• Build a repeat relationship with a prime that understands small-business needs  

**What to do now**

Complete the short form below with your company details, core services, and coverage area.  
Submitting your information now means your company can be reviewed and included in our pipeline for active and upcoming bids, not months from now.

After you submit:

• We review your information and capabilities  
• We may contact you to schedule a brief conference call to confirm fit and discuss next steps  
• When opportunities match your services and location, you can be contacted quickly for quotes, teaming, or task orders  

For questions, you can reach us at **832 273 0498** or **elamgmtllc@gmail.com**.
        """
    )

    st.markdown("---")
    st.markdown("**Fields marked with * are required.**")

    with st.form("subcontractor_signup_form"):
        st.markdown("### Company information")

        col1, col2 = st.columns(2)
        contact_name = col1.text_input("Primary contact name *")
        business_name = col2.text_input("Legal business name *")
        dba = st.text_input("DBA (if applicable)")
        address = st.text_area("Business address")
        website = st.text_input("Website")
        phone = st.text_input("Phone *")
        email = st.text_input("Email *")

        st.markdown("### Business identifiers")

        naics_codes = st.text_input(
            "NAICS codes (comma separated, for example 561720, 561730)"
        )

        certifications = st.multiselect(
            "Certifications",
            [
                "SBA 8(a)",
                "HUBZone",
                "WOSB",
                "SDVOSB",
                "MBE",
                "DBE",
                "Other",
            ],
        )
        cert_other = st.text_input("If Other, describe")

        st.markdown("### Primary services")

        services = st.multiselect(
            "Primary services",
            [
                "HVAC",
                "Janitorial",
                "Landscaping",
                "Snow removal",
                "Catering",
                "Construction",
                "Painting",
                "Lodging",
                "Other",
            ],
        )
        services_other = st.text_input("If Other, describe main services")
        commercial_capable = st.radio("Capable of commercial work", ["Yes", "No"])

        st.markdown("### Coverage area")

        coverage_states = st.text_input("States or regions served")
        nationwide = st.radio("Nationwide coverage", ["Yes", "No"])

        st.markdown("### Past performance")

        gov_contracts = st.text_area("Relevant government contracts (if any)")
        commercial_projects = st.text_area("Relevant commercial projects")
        references = st.text_area("References (names, agencies, or companies)")

        st.markdown("### Insurance and bonding")

        gl_limit = st.text_input("General liability coverage limit (dollar amount)")
        bonding_capacity = st.text_input("Bonding capacity (dollar amount)")
        workers_comp = st.radio("Workers’ compensation in place", ["Yes", "No"])

        st.markdown("### Call preference and notes")

        col_date, col_time = st.columns(2)
        preferred_date = col_date.date_input("Preferred date for a conference call")

        # Build full list of time options
        time_options_all = build_time_options_12h()

        # Compute which times are already booked for the selected date
        booked_times_for_date = set()
        if (
            existing_df is not None
            and "preferred_call_date" in existing_df.columns
            and "preferred_call_time_central" in existing_df.columns
        ):
            selected_date_str = preferred_date.strftime("%m/%d/%Y")
            mask = existing_df["preferred_call_date"].astype(str) == selected_date_str
            booked_times_for_date = set(
                existing_df.loc[mask, "preferred_call_time_central"]
                .dropna()
                .astype(str)
            )

        available_time_options = [
            t for t in time_options_all if t not in booked_times_for_date
        ]

        if not available_time_options:
            preferred_time_label = col_time.selectbox(
                "Preferred time (Central, 12-hour)",
                ["No times available for this date. Please choose another date."],
                index=0,
            )
        else:
            default_time_label = "09:00 AM"
            default_index = (
                available_time_options.index(default_time_label)
                if default_time_label in available_time_options
                else 0
            )
            preferred_time_label = col_time.selectbox(
                "Preferred time (Central, 12-hour)",
                available_time_options,
                index=default_index,
            )

        notes = st.text_area("Notes or comments")

        submitted = st.form_submit_button("Submit partner information")

    if submitted:
        required_missing: list[str] = []
        if not contact_name:
            required_missing.append("Primary contact name")
        if not business_name:
            required_missing.append("Legal business name")
        if not phone:
            required_missing.append("Phone")
        if not email:
            required_missing.append("Email")

        # If no available times for that date, block submission
        if "No times available for this date" in preferred_time_label:
            st.error(
                "All times are booked for the selected date. "
                "Please choose a different date for your conference call."
            )
            return

        if required_missing:
            st.error(
                "Please complete the following required fields: "
                + ", ".join(required_missing)
            )
            return

        now_utc = datetime.utcnow()
        now_central = datetime.now(tz=CENTRAL_TZ)

        if isinstance(preferred_date, date) and preferred_time_label:
            preferred_call_date_str = preferred_date.strftime("%m/%d/%Y")
            call_dt_central = datetime.strptime(
                f"{preferred_call_date_str} {preferred_time_label}",
                "%m/%d/%Y %I:%M %p",
            ).replace(tzinfo=CENTRAL_TZ)

            preferred_call_datetime_central = call_dt_central.strftime(
                "%m/%d/%Y %I:%M %p Central"
            )
            preferred_call_time_central = call_dt_central.strftime("%I:%M %p")
        else:
            preferred_call_date_str = ""
            preferred_call_datetime_central = ""
            preferred_call_time_central = ""

        record = {
            "timestamp_utc": now_utc.strftime("%Y/%m/%d %H:%M:%S"),
            "timestamp_central": now_central.strftime(
                "%Y/%m/%d %I:%M:%S %p Central"
            ),
            "contact_name": contact_name,
            "business_name": business_name,
            "dba": dba,
            "address": address,
            "website": website,
            "phone": phone,
            "email": email,
            "naics_codes": naics_codes,
            "certifications": ", ".join(certifications),
            "certifications_other": cert_other,
            "services": ", ".join(services),
            "services_other": services_other,
            "commercial_capable": commercial_capable,
            "coverage_states_regions": coverage_states,
            "nationwide": nationwide,
            "government_contracts": gov_contracts,
            "commercial_projects": commercial_projects,
            "references": references,
            "general_liability_limit": gl_limit,
            "bonding_capacity": bonding_capacity,
            "workers_comp": workers_comp,
            "preferred_call_date": preferred_call_date_str,
            "preferred_call_time_central": preferred_call_time_central,
            "preferred_call_datetime_central": preferred_call_datetime_central,
            "notes": notes,
        }

        try:
            append_to_csv(record)
            st.success(
                "Thank you for submitting your information. "
                "ELA Management will review your details and contact you about potential opportunities."
            )
        except Exception as exc:
            st.error(
                "Your form was received but there was an issue saving it internally. "
                "Please contact elamgmtllc@gmail.com and mention this message."
            )
            st.text(f"Technical details: {exc}")


def show_admin_area():
    """Hidden internal area for ELA to see, delete, and download signups."""
    st.markdown("### ELA internal view")

    if not os.path.exists(CSV_PATH):
        st.info("No submissions have been recorded yet.")
        return

    df = pd.read_csv(CSV_PATH)

    if not df.empty:
        st.markdown("Select entries to delete from the stored list.")
        row_indices = list(range(len(df)))

        def _fmt(i: int) -> str:
            try:
                name = str(df.loc[i, "business_name"])
            except Exception:
                name = "Unknown"
            try:
                email = str(df.loc[i, "email"])
            except Exception:
                email = ""
            label = f"{i}  {name}"
            if email:
                label += f"  ({email})"
            return label

        to_delete = st.multiselect(
            "Rows to delete",
            row_indices,
            format_func=_fmt,
        )

        delete_clicked = st.button("Delete selected entries")

        if delete_clicked and to_delete:
            df = df.drop(index=to_delete).reset_index(drop=True)
            df.to_csv(CSV_PATH, index=False)
            st.success("Selected entries deleted.")

    st.dataframe(df)

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download all signups as CSV",
        data=csv_bytes,
        file_name="ela_subcontractor_signups.csv",
        mime="text/csv",
    )


def main():
    st.set_page_config(
        page_title="ELA Management Subcontractor Partner Portal",
        layout="centered",
    )

    show_public_form()

    # Admin view toggle via query parameter: add ?ela_admin=1 to URL
    params = st.experimental_get_query_params()
    is_admin = params.get("ela_admin", ["0"])[0] == "1"

    if is_admin:
        st.markdown("---")
        show_admin_area()


if __name__ == "__main__":
    main()
