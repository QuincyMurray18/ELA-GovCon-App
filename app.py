import streamlit as st

# Corrected version of app.py with consistent indentation around line 7706
# Ensure consistent use of 4 spaces for indentation, not tabs

def filters_section(data):
    with st.expander("Advanced Options"):
        st.subheader("Filters")
        selected_agency = st.selectbox("Agency", ["All", "DHS", "USCG", "FAA", "DOD"])
        selected_status = st.selectbox("Status", ["All", "Active", "Awarded", "Archived"])
        min_value = st.number_input("Minimum Value ($)", min_value=0)
        max_value = st.number_input("Maximum Value ($)", min_value=0)

        if st.button("Apply Filters"):
            filtered_data = data[
                ((data["Agency"] == selected_agency) if selected_agency != "All" else True)
                & ((data["Status"] == selected_status) if selected_status != "All" else True)
                & (data["Value"] >= min_value)
                & (data["Value"] <= max_value)
            ]
            st.dataframe(filtered_data)
