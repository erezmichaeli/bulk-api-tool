import streamlit as st
import pandas as pd
import requests
import json
import re
import os
from concurrent.futures import ThreadPoolExecutor

# --- 1. HARDCODED API DEFINITIONS (Optimized from your Swagger) ---
# We group these by Tag to make the Dropdown cleaner (78 endpoints -> 15 Categories)

API_DEFINITIONS = {
    "Fundamental Analysis": [
        {"label": "Get Company Fundamental Analysis", "url": "/companies/{company_id}/fundamental-analysis", "params": ["company_id"]},
        {"label": "Get Company Fundamental Sections", "url": "/companies/{company_id}/fundamental-sections", "params": ["company_id"]},
        {"label": "Get Company Fundamental Parameters", "url": "/companies/{company_id}/fundamental-parameters", "params": ["company_id"]},
        {"label": "Get Target Price", "url": "/companies/{company_id}/target-price", "params": ["company_id"]},
    ],
    "ESG Analysis": [
        {"label": "Get Company ESG Analysis", "url": "/companies/{company_id}/esg-analysis", "params": ["company_id"]},
        {"label": "Get Company ESG Sections", "url": "/companies/{company_id}/esg-sections", "params": ["company_id"]},
        {"label": "Get Company ESG Parameters", "url": "/companies/{company_id}/esg-parameters", "params": ["company_id"]},
        {"label": "Get Sustainability Incidents", "url": "/companies/{company_id}/sustainability-incidents", "params": ["company_id"]},
    ],
    "Technical Analysis": [
        {"label": "Get Company Technical Analysis", "url": "/companies/{company_id}/technical-analysis", "params": ["company_id"]},
        {"label": "Get Company Technical Parameters", "url": "/companies/{company_id}/technical-parameters", "params": ["company_id"]},
        {"label": "Get Alternative Asset Technical", "url": "/alternative-assets/{asset_id}/technical-analysis", "params": ["asset_id"]},
    ],
    "Company Metadata": [
        {"label": "Get Company Details", "url": "/companies/{company_id}", "params": ["company_id"]},
        {"label": "Identifier Search (ISIN/Ticker to ID)", "url": "/identifier-search", "params": []}, 
    ],
    "Company Market Data": [
        {"label": "Get Company Market Data", "url": "/companies/{company_id}/market", "params": ["company_id"]},
        {"label": "Get Market Statistics", "url": "/companies/{company_id}/market-statistics", "params": ["company_id"]},
        {"label": "Get Trading Items", "url": "/companies/{company_id}/trading-items", "params": ["company_id"]},
    ],
    "Company News": [
        {"label": "Get Company News", "url": "/companies/{company_id}/news", "params": ["company_id"]},
        {"label": "Get News Sentiment", "url": "/companies/{company_id}/news-sentiment", "params": ["company_id"]},
    ],
    "Funds": [
        {"label": "Get Fund Details", "url": "/funds/{fund_id}", "params": ["fund_id"]},
        {"label": "Get Fund Analysis", "url": "/funds/{fund_id}/analysis", "params": ["fund_id"]},
        {"label": "Get Fund Holdings", "url": "/funds/{fund_id}/holdings", "params": ["fund_id"]},
        {"label": "Get Fund ESG Analysis", "url": "/funds/{fund_id}/esg-analysis", "params": ["fund_id"]},
    ],
    "Alternative Assets": [
        {"label": "Get Crypto/Commodity Details", "url": "/alternative-assets/{asset_id}", "params": ["asset_id"]},
        {"label": "Get Alternative Market Data", "url": "/alternative-assets/{asset_id}/market", "params": ["asset_id"]},
    ],
    "Alerts & Activities": [
        {"label": "Get Company Alerts", "url": "/companies/{company_id}/alerts", "params": ["company_id"]},
        {"label": "Get Key Developments", "url": "/companies/{company_id}/key-developments", "params": ["company_id"]},
        {"label": "Get Earnings Call Recaps", "url": "/companies/{company_id}/earnings-call/{year_quarter}/recaps", "params": ["company_id", "year_quarter"]},
    ]
}

# --- 2. CONFIGURATION & HELPERS ---

st.set_page_config(page_title="Bulk API Tool", layout="wide")
st.title("🚀 Bulk API Enricher")

if "api_steps" not in st.session_state:
    st.session_state.api_steps = []

def get_headers(token):
    return {
        "Authorization": f"Bearer {token.strip()}",
        "Accept": "application/json"
    }

def load_csv(file):
    try:
        file.seek(0)
        return pd.read_csv(file)
    except:
        file.seek(0)
        return pd.read_csv(file, encoding='latin1')

def process_single_row(row, api_steps, base_url, headers, debug=False):
    """
    Processes a single row through configured API steps.
    Returns the enriched dictionary.
    """
    # 1. Convert Row to Dict Safely
    if hasattr(row, 'to_dict'):
        row_data = row.to_dict()
    else:
        row_data = dict(row)

    debug_log = []
    
    # 2. Iterate Configured Steps
    for step_index, step in enumerate(api_steps):
        try:
            # Prepare URL
            url_path = step['url_template']
            
            # Map Parameters (e.g. {company_id} -> 12345)
            for param_name, csv_col in step['param_map'].items():
                if csv_col and csv_col in row_data:
                    val = str(row_data[csv_col]) if pd.notna(row_data[csv_col]) else ""
                    url_path = url_path.replace(f"{{{param_name}}}", val)
            
            # Handle query params if needed (simple implementation just checks basics)
            full_url = base_url.rstrip('/') + '/' + url_path.lstrip('/')

            if debug:
                debug_log.append(f"**Step {step_index+1} ({step['name']}):** `GET {full_url}`")

            # Call API
            resp = requests.get(full_url, headers=headers)
            
            if debug:
                debug_log.append(f"Status: `{resp.status_code}`")

            if resp.status_code == 200:
                data = resp.json()
                
                # Normalize response (Array -> Object if needed)
                if isinstance(data, list):
                    data = data[0] if len(data) > 0 else {}
                
                if debug:
                    json_preview = json.dumps(data, indent=2)[:300]
                    debug_log.append(f"```json\n{json_preview}...\n```")

                # Extract Requested Fields
                for mapping in step['output_map']:
                    j_field = mapping['json_field']
                    c_col = mapping['csv_column']
                    
                    if j_field and c_col:
                        # Support nested fields via dot notation e.g. "data.score"
                        val = data
                        for key in j_field.split('.'):
                            if isinstance(val, dict):
                                val = val.get(key, "")
                            else:
                                val = ""
                        
                        row_data[c_col] = val
                        if debug:
                            debug_log.append(f"✅ Map `{j_field}` -> `{val}`")
            else:
                if debug:
                    debug_log.append(f"❌ Failed: {resp.text}")
                # Fill mapped columns with empty string on failure
                for mapping in step['output_map']:
                    if mapping['csv_column']:
                        row_data[mapping['csv_column']] = ""

        except Exception as e:
            if debug:
                debug_log.append(f"❌ Exception: {str(e)}")
            # Fill mapped columns with empty string on crash
            for mapping in step['output_map']:
                if mapping['csv_column']:
                    row_data[mapping['csv_column']] = ""

    if debug:
        return debug_log
    return row_data

# --- 3. UI: SIDEBAR ---

with st.sidebar:
    st.header("Authentication")
    token_input = st.text_input("Access Token", type="password", placeholder="Paste your token here")
    base_url = st.text_input("Base URL", value="https://rest.bridgewise.com")
    
    st.divider()
    st.markdown("**Instructions:**")
    st.markdown("1. Upload CSV")
    st.markdown("2. Select API Category & Endpoint")
    st.markdown("3. Map columns")
    st.markdown("4. Run")

# --- 4. UI: MAIN AREA ---

col_main, col_dummy = st.columns([3, 1])

with col_main:
    uploaded_file = st.file_uploader("1. Upload Input CSV", type=["csv"])

if uploaded_file and token_input:
    # Load CSV Headers
    df = load_csv(uploaded_file)
    csv_headers = df.columns.tolist()
    
    # Determine Output Name
    input_name = os.path.splitext(uploaded_file.name)[0]
    output_name = f"{input_name}_output.csv"

    st.divider()
    st.subheader("2. Build API Pipeline")
    
    # --- PIPELINE BUILDER ---
    with st.container(border=True):
        c1, c2 = st.columns(2)
        with c1:
            category = st.selectbox("Select API Category", options=list(API_DEFINITIONS.keys()))
        with c2:
            # Filter Endpoints based on Category
            available_endpoints = API_DEFINITIONS[category]
            endpoint_labels = [e['label'] for e in available_endpoints]
            selected_label = st.selectbox("Select Endpoint", options=endpoint_labels)
        
        # Get selected endpoint details
        selected_endpoint = next(e for e in available_endpoints if e['label'] == selected_label)
        
        st.caption(f"Endpoint URL: `{selected_endpoint['url']}`")

        # Dynamic Param Mapping
        param_map = {}
        if selected_endpoint['params']:
            st.markdown("**Map URL Parameters:**")
            p_cols = st.columns(len(selected_endpoint['params']))
            for i, p in enumerate(selected_endpoint['params']):
                with p_cols[i]:
                    # Smart Auto-select
                    default_idx = 0
                    if p in csv_headers: default_idx = csv_headers.index(p)
                    elif "company_id" in csv_headers and "id" in p: default_idx = csv_headers.index("company_id")
                    
                    param_map[p] = st.selectbox(f"CSV Column for {{{p}}}", options=csv_headers, index=default_idx)
        
        # Output Config
        st.markdown("**Extract Data:**")
        default_outputs = [
            {"json_field": "analysis_score", "csv_column": "Score"},
            {"json_field": "analysis_score_group_text", "csv_column": "Recommendation"}
        ]
        
        edited_outputs = st.data_editor(
            default_outputs, 
            num_rows="dynamic",
            column_config={
                "json_field": st.column_config.TextColumn("JSON Field (from API)", required=True),
                "csv_column": st.column_config.TextColumn("Output Column Name", required=True)
            },
            use_container_width=True,
            key="new_step_editor"
        )

        if st.button("➕ Add This Step to Pipeline", type="secondary"):
            new_step = {
                "name": selected_endpoint['label'],
                "url_template": selected_endpoint['url'],
                "param_map": param_map,
                "output_map": edited_outputs
            }
            st.session_state.api_steps.append(new_step)
            st.success("Step Added!")

    # --- PIPELINE REVIEW ---
    if st.session_state.api_steps:
        st.markdown("### Active Pipeline")
        for i, step in enumerate(st.session_state.api_steps):
            with st.expander(f"Step {i+1}: {step['name']}", expanded=True):
                st.write(f"**URL:** `{step['url_template']}`")
                st.write(f"**Params:** `{step['param_map']}`")
                st.dataframe(pd.DataFrame(step['output_map']), hide_index=True)
                if st.button("Remove Step", key=f"rm_{i}"):
                    st.session_state.api_steps.pop(i)
                    st.rerun()

    # --- EXECUTION ---
    st.divider()
    st.subheader("3. Run Process")
    
    act1, act2 = st.columns([1, 4])
    
    with act1:
        if st.button("🔍 Test 1 Row"):
            if not st.session_state.api_steps:
                st.error("Please add at least one step above.")
            else:
                first_row = df.iloc[0]
                logs = process_single_row(
                    first_row, 
                    st.session_state.api_steps, 
                    base_url, 
                    get_headers(token_input), 
                    debug=True
                )
                with st.expander("View Diagnostic Logs", expanded=True):
                    for log in logs:
                        st.markdown(log)

    with act2:
        if st.button("🚀 Process Full File", type="primary"):
            if not st.session_state.api_steps:
                st.error("Please add at least one step above.")
            else:
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                results = []
                total = len(df)
                
                # Execute in parallel
                with ThreadPoolExecutor(max_workers=5) as executor:
                    input_rows = df.to_dict('records')
                    futures = [
                        executor.submit(
                            process_single_row, 
                            row, 
                            st.session_state.api_steps, 
                            base_url, 
                            get_headers(token_input), 
                            False
                        ) for row in input_rows
                    ]
                    
                    for i, f in enumerate(futures):
                        try:
                            res = f.result()
                            results.append(res)
                        except Exception as e:
                            # Safety: If a thread crashes, append original row with error note (or just original)
                            err_row = input_rows[i].copy()
                            err_row['ERROR_LOG'] = str(e)
                            results.append(err_row)
                        
                        if i % 5 == 0:
                            progress_bar.progress((i + 1) / total)
                            status_text.text(f"Processed {i + 1}/{total} rows")

                progress_bar.progress(100)
                status_text.text("Processing Complete!")
                
                final_df = pd.DataFrame(results)
                
                # CSV Download
                csv_data = final_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label=f"📥 Download {output_name}",
                    data=csv_data,
                    file_name=output_name,
                    mime="text/csv"
                )

elif not uploaded_file:
    st.info("Waiting for CSV upload...")
elif not token_input:
    st.warning("Please enter your Access Token in the sidebar.")
