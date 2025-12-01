import streamlit as st
import pandas as pd
import requests
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

# --- 1. HARDCODED API DEFINITIONS ---
API_DEFINITIONS = {
    "Fundamental Analysis": [
        {"label": "Get Fundamental Analysis", "url": "/companies/{company_id}/fundamental-analysis", "params": ["company_id"], "defaults": [{"json_field": "analysis_score", "csv_column": "Fundamental Score"}]},
        {"label": "Get Fundamental Sections", "url": "/companies/{company_id}/fundamental-sections", "params": ["company_id"], "defaults": [{"json_field": "section_score", "csv_column": "Section Score"}]},
        {"label": "Get Fundamental Parameters", "url": "/companies/{company_id}/fundamental-parameters", "params": ["company_id"], "defaults": [{"json_field": "parameter_value", "csv_column": "Param Value"}]},
        {"label": "Get Target Price", "url": "/companies/{company_id}/target-price", "params": ["company_id"], "defaults": [{"json_field": "target_price", "csv_column": "Target Price"}]},
    ],
    "ESG Analysis": [
        {"label": "Get ESG Analysis", "url": "/companies/{company_id}/esg-analysis", "params": ["company_id"], "defaults": [{"json_field": "analysis_score", "csv_column": "ESG Score"}]},
        {"label": "Get ESG Sections", "url": "/companies/{company_id}/esg-sections", "params": ["company_id"], "defaults": []},
        {"label": "Get Sustainability Incidents", "url": "/companies/{company_id}/sustainability-incidents", "params": ["company_id"], "defaults": [{"json_field": "headline", "csv_column": "Incident Headline"}]},
    ],
    "Technical Analysis": [
        {"label": "Get Tech Analysis (Stock)", "url": "/companies/{company_id}/technical-analysis", "params": ["company_id"], "defaults": [{"json_field": "analysis_score", "csv_column": "Tech Score"}]},
        {"label": "Get Tech Parameters", "url": "/companies/{company_id}/technical-parameters", "params": ["company_id"], "defaults": []},
        {"label": "Get Tech Analysis (Crypto/Alt)", "url": "/alternative-assets/{asset_id}/technical-analysis", "params": ["asset_id"], "defaults": [{"json_field": "analysis_score", "csv_column": "Alt Tech Score"}]},
    ],
    "Metadata & Search": [
        {"label": "Get Company Details", "url": "/companies/{company_id}", "params": ["company_id"], "defaults": [{"json_field": "company_name", "csv_column": "Name"}]},
        {"label": "Identifier Search", "url": "/identifier-search", "params": [], "defaults": [{"json_field": "company_id", "csv_column": "BW_ID"}]}, 
    ],
    "Market Data": [
        {"label": "Get Market Data", "url": "/companies/{company_id}/market", "params": ["company_id"], "defaults": [{"json_field": "close_price", "csv_column": "Close Price"}]},
        {"label": "Get Market Stats", "url": "/companies/{company_id}/market-statistics", "params": ["company_id"], "defaults": [{"json_field": "1Y", "csv_column": "1Y Return"}]},
    ],
    "News & Events": [
        {"label": "Get News", "url": "/companies/{company_id}/news", "params": ["company_id"], "defaults": [{"json_field": "title", "csv_column": "News Title"}]},
        {"label": "Get News Sentiment", "url": "/companies/{company_id}/news-sentiment", "params": ["company_id"], "defaults": [{"json_field": "average_sentiment_score", "csv_column": "Sentiment Score"}]},
        {"label": "Get Earnings Recaps", "url": "/companies/{company_id}/earnings-call/{year_quarter}/recaps", "params": ["company_id", "year_quarter"], "defaults": [{"json_field": "text", "csv_column": "Recap Text"}]},
    ],
    "Funds": [
        {"label": "Get Fund Details", "url": "/funds/{fund_id}", "params": ["fund_id"], "defaults": [{"json_field": "fund_name", "csv_column": "Fund Name"}]},
        {"label": "Get Fund Analysis", "url": "/funds/{fund_id}/analysis", "params": ["fund_id"], "defaults": [{"json_field": "analysis_score", "csv_column": "Fund Score"}]},
    ],
}

# --- 2. CONFIG & STYLING ---
st.set_page_config(page_title="BW Enricher", layout="wide", page_icon="🚀")

# Custom CSS for a professional look
st.markdown("""
<style>
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #f0f2f6;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #ffffff;
        border-top: 2px solid #ff4b4b;
    }
    div[data-testid="stExpander"] div[role="button"] p {
        font-size: 1.1rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# Session State
if "api_steps" not in st.session_state: st.session_state.api_steps = []
if "df" not in st.session_state: st.session_state.df = None
if "csv_headers" not in st.session_state: st.session_state.csv_headers = []

# --- 3. HELPER FUNCTIONS ---
def get_headers(token):
    return {"Authorization": f"Bearer {token.strip()}", "Accept": "application/json"}

def load_csv(file):
    try:
        file.seek(0)
        return pd.read_csv(file)
    except:
        file.seek(0)
        return pd.read_csv(file, encoding='latin1')

def process_single_row(row, api_steps, base_url, headers, debug=False):
    if hasattr(row, 'to_dict'): row_data = row.to_dict()
    else: row_data = dict(row)
    
    debug_log = []
    
    for step in api_steps:
        try:
            url_path = step['url_template']
            # Map Params
            for param_name, csv_col in step['param_map'].items():
                val = str(row_data.get(csv_col, ""))
                url_path = url_path.replace(f"{{{param_name}}}", val)
            
            full_url = base_url.rstrip('/') + '/' + url_path.lstrip('/')
            
            if debug: debug_log.append(f"**{step['name']}**: `{full_url}`")
            
            resp = requests.get(full_url, headers=headers)
            
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list): data = data[0] if len(data) > 0 else {}
                
                if debug: debug_log.append(f"✅ 200 OK")

                for mapping in step['output_map']:
                    j_field = mapping['json_field']
                    c_col = mapping['csv_column']
                    if j_field and c_col:
                        # Nested lookup support
                        val = data
                        for key in j_field.split('.'):
                            if isinstance(val, dict): val = val.get(key, "")
                            else: val = ""
                        row_data[c_col] = val
            else:
                if debug: debug_log.append(f"❌ {resp.status_code}: {resp.text}")
                for mapping in step['output_map']:
                    if mapping['csv_column']: row_data[mapping['csv_column']] = ""
                    
        except Exception as e:
            if debug: debug_log.append(f"❌ Error: {str(e)}")
            for mapping in step['output_map']:
                if mapping['csv_column']: row_data[mapping['csv_column']] = ""

    if debug: return debug_log
    return row_data

# --- 4. MAIN APP STRUCTURE ---

st.title("🚀 Bulk API Enricher")

# TABS
tab_setup, tab_build, tab_run = st.tabs(["1. 📂 Setup Data", "2. ⚙️ Build Pipeline", "3. ▶️ Run Process"])

# --- TAB 1: SETUP ---
with tab_setup:
    c1, c2 = st.columns([1, 2])
    with c1:
        st.subheader("Credentials")
        token_input = st.text_input("Access Token", type="password", help="Paste your API Key here")
        base_url = st.text_input("Base URL", value="https://rest.bridgewise.com")
        
    with c2:
        st.subheader("Data Source")
        uploaded_file = st.file_uploader("Upload CSV File", type=["csv"], help="Must contain ID columns like Ticker, ISIN, or CompanyID")
        
        if uploaded_file:
            st.session_state.df = load_csv(uploaded_file)
            st.session_state.csv_headers = st.session_state.df.columns.tolist()
            st.success(f"Loaded {len(st.session_state.df)} rows successfully.")
            st.dataframe(st.session_state.df.head(3), hide_index=True, use_container_width=True)

# --- TAB 2: BUILD PIPELINE ---
with tab_build:
    if st.session_state.df is None:
        st.info("👈 Please upload a CSV in the 'Setup Data' tab first.")
    else:
        col_config, col_summary = st.columns([2, 1])
        
        with col_config:
            with st.container(border=True):
                st.subheader("Add API Step")
                
                # Category & Endpoint Selector
                cat_options = list(API_DEFINITIONS.keys())
                selected_cat = st.pills("Category", cat_options, selection_mode="single", default=cat_options[0])
                
                endpoint_opts = [e['label'] for e in API_DEFINITIONS[selected_cat]]
                selected_lbl = st.selectbox("Select Endpoint", endpoint_opts)
                
                # Get Config for selection
                ep_config = next(e for e in API_DEFINITIONS[selected_cat] if e['label'] == selected_lbl)
                
                st.caption(f"Path: `{ep_config['url']}`")
                
                # Parameter Mapping
                param_map = {}
                if ep_config['params']:
                    st.markdown("##### 🔗 Map Parameters")
                    cols = st.columns(len(ep_config['params']))
                    for i, p in enumerate(ep_config['params']):
                        with cols[i]:
                            # Smart Auto-Match
                            def_idx = 0
                            match_candidates = [p, "id", "ID"]
                            for cand in match_candidates:
                                matches = [h for h in st.session_state.csv_headers if cand.lower() in h.lower()]
                                if matches:
                                    def_idx = st.session_state.csv_headers.index(matches[0])
                                    break
                            
                            param_map[p] = st.selectbox(f"CSV col for {{{p}}}", st.session_state.csv_headers, index=def_idx)
                
                # Output Mapping
                st.markdown("##### 📤 Output Columns")
                # Use predefined defaults if user hasn't edited
                edited_outputs = st.data_editor(
                    ep_config.get('defaults', [{"json_field": "", "csv_column": ""}]),
                    num_rows="dynamic",
                    column_config={
                        "json_field": st.column_config.TextColumn("API Field (JSON)", required=True),
                        "csv_column": st.column_config.TextColumn("New CSV Header", required=True)
                    },
                    use_container_width=True,
                    key=f"editor_{selected_lbl}"
                )
                
                if st.button("➕ Add to Pipeline", type="secondary", use_container_width=True):
                    step = {
                        "name": selected_lbl,
                        "url_template": ep_config['url'],
                        "param_map": param_map,
                        "output_map": edited_outputs
                    }
                    st.session_state.api_steps.append(step)
                    st.toast("Step added successfully!", icon="✅")

        with col_summary:
            st.subheader("Pipeline Summary")
            if not st.session_state.api_steps:
                st.info("No steps added yet.")
            
            for i, step in enumerate(st.session_state.api_steps):
                with st.expander(f"{i+1}. {step['name']}", expanded=False):
                    st.markdown(f"**URL:** `{step['url_template']}`")
                    st.write("Outputs:", [x['csv_column'] for x in step['output_map']])
                    if st.button("🗑️ Remove", key=f"rm_{i}"):
                        st.session_state.api_steps.pop(i)
                        st.rerun()

# --- TAB 3: RUN ---
with tab_run:
    if not st.session_state.api_steps:
        st.warning("⚠️ Go to 'Build Pipeline' and add at least one step.")
    elif not token_input:
        st.error("⚠️ Access Token is missing in 'Setup Data'.")
    else:
        st.subheader("Ready to Enrich")
        
        c_test, c_run = st.columns([1, 4])
        
        with c_test:
            if st.button("🔍 Test First Row"):
                first_row = st.session_state.df.iloc[0]
                with st.status("Running Test...", expanded=True) as status:
                    logs = process_single_row(
                        first_row, 
                        st.session_state.api_steps, 
                        base_url, 
                        get_headers(token_input), 
                        debug=True
                    )
                    status.update(label="Test Complete", state="complete", expanded=True)
                    for log in logs: st.markdown(log)

        with c_run:
            if st.button("🚀 Process All Rows", type="primary"):
                total_rows = len(st.session_state.df)
                results = []
                
                # Modern Status Container
                with st.status(f"Processing {total_rows} rows...", expanded=True) as status:
                    progress_bar = st.progress(0)
                    time_placeholder = st.empty()
                    start_time = time.time()
                    
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        rows_input = st.session_state.df.to_dict('records')
                        futures = [
                            executor.submit(
                                process_single_row, 
                                row, 
                                st.session_state.api_steps, 
                                base_url, 
                                get_headers(token_input), 
                                False
                            ) for row in rows_input
                        ]
                        
                        for i, f in enumerate(futures):
                            try:
                                results.append(f.result())
                            except Exception as e:
                                err_row = rows_input[i].copy()
                                err_row['Error'] = str(e)
                                results.append(err_row)
                            
                            # Update visuals every 5 rows
                            if i % 5 == 0 or i == total_rows - 1:
                                progress_bar.progress((i + 1) / total_rows)
                                elapsed = time.time() - start_time
                                rate = (i + 1) / elapsed if elapsed > 0 else 0
                                time_placeholder.caption(f"Processed: {i+1}/{total_rows} ({rate:.1f} rows/sec)")
                    
                    status.update(label="Processing Complete!", state="complete", expanded=False)
                
                # Results & Download
                final_df = pd.DataFrame(results)
                
                st.success("Job Finished!")
                
                # Smart Filename
                orig_name = uploaded_file.name.rsplit('.', 1)[0]
                out_name = f"{orig_name}_output.csv"
                
                csv = final_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label=f"📥 Download {out_name}",
                    data=csv,
                    file_name=out_name,
                    mime="text/csv",
                    type="primary"
                )
