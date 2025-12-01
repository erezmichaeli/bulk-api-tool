import streamlit as st
import pandas as pd
import requests
import json
import re
import os
import time
from concurrent.futures import ThreadPoolExecutor

# --- 1. EXACT SWAGGER DEFINITION (Parsed from your provided JSON) ---
SWAGGER_SPECS = {
    "Fundamental Analysis": {
        "/companies/{company_id}/fundamental-analysis": {
            "label": "Get Fundamental Analysis",
            "path": ["company_id"],
            "query": ["language", "show_all"],
            "defaults": [{"json_field": "analysis_score", "csv_column": "Fundamental Score"}]
        },
        "/companies/{company_id}/fundamental-sections": {
            "label": "Get Fundamental Sections",
            "path": ["company_id"],
            "query": ["language", "section_type", "show_all"],
            "defaults": [{"json_field": "section_score", "csv_column": "Section Score"}]
        },
        "/companies/{company_id}/fundamental-parameters": {
            "label": "Get Fundamental Parameters",
            "path": ["company_id"],
            "query": ["language", "section_type", "has_score", "year", "quarter", "period_type", "parameter_id", "limit", "show_all"],
            "defaults": [{"json_field": "parameter_value", "csv_column": "Param Value"}]
        },
        "/companies/{company_id}/target-price": {
            "label": "Get Target Price",
            "path": ["company_id"],
            "query": ["trading_item_id", "date__gte", "date__lte"],
            "defaults": [{"json_field": "target_price", "csv_column": "Target Price"}]
        },
    },
    "ESG Analysis": {
        "/companies/{company_id}/esg-analysis": {
            "label": "Get ESG Analysis",
            "path": ["company_id"],
            "query": ["language", "show_all"],
            "defaults": [{"json_field": "analysis_score", "csv_column": "ESG Score"}]
        },
        "/companies/{company_id}/esg-sections": {
            "label": "Get ESG Sections",
            "path": ["company_id"],
            "query": ["language", "section_type", "show_all"],
            "defaults": []
        },
        "/companies/{company_id}/esg-parameters": {
            "label": "Get ESG Parameters",
            "path": ["company_id"],
            "query": ["language", "parameter_id", "section_type", "show_all"],
            "defaults": []
        },
        "/companies/{company_id}/sustainability-incidents": {
            "label": "Get Sustainability Incidents",
            "path": ["company_id"],
            "query": ["language"],
            "defaults": [{"json_field": "headline", "csv_column": "Incident"}]
        }
    },
    "Technical Analysis": {
        "/companies/{company_id}/technical-analysis": {
            "label": "Get Company Technical Analysis",
            "path": ["company_id"],
            "query": ["language", "trading_item_id"],
            "defaults": [{"json_field": "analysis_score", "csv_column": "Tech Score"}]
        },
        "/companies/{company_id}/technical-parameters": {
            "label": "Get Company Technical Params",
            "path": ["company_id"],
            "query": ["language", "trading_item_id", "parameter_id"],
            "defaults": []
        },
        "/alternative-assets/{asset_id}/technical-analysis": {
            "label": "Get Alt Asset Technical",
            "path": ["asset_id"],
            "query": ["language"],
            "defaults": [{"json_field": "analysis_score", "csv_column": "Alt Score"}]
        }
    },
    "Company Metadata & Search": {
        "/companies": {
            "label": "Search Companies (Filter)",
            "path": [],
            "query": [
                "language", "search", "peers_of", "region_id", "incorporation_country_id", 
                "domicile_country_id", "gics_sector_id", "gics_industry_group_id", "gics_industry_id",
                "theme_id", "implied_market_cap__gte", "implied_market_cap__lte", "exchange_id",
                "sort_by", "page", "page_size"
            ],
            "defaults": [{"json_field": "company_name", "csv_column": "Company Name"}]
        },
        "/companies/{company_id}": {
            "label": "Get Company Details",
            "path": ["company_id"],
            "query": ["language", "include", "trading_item_id"],
            "defaults": [{"json_field": "company_name", "csv_column": "Company Name"}]
        },
        "/identifier-search": {
            "label": "Identifier Search",
            "path": [],
            "query": ["identifier", "identifier_type"],
            "defaults": [{"json_field": "company_id", "csv_column": "BW_ID"}]
        }
    },
    "Market Data": {
        "/companies/{company_id}/market": {
            "label": "Get Market Data",
            "path": ["company_id"],
            "query": ["trading_item_id", "date__ge", "date__le"],
            "defaults": [{"json_field": "close_price", "csv_column": "Close Price"}]
        },
        "/companies/{company_id}/market-statistics": {
            "label": "Get Market Statistics",
            "path": ["company_id"],
            "query": ["trading_item_id", "identifier__in"],
            "defaults": [{"json_field": "1Y", "csv_column": "1Y Return"}]
        }
    },
    "News & Events": {
        "/companies/{company_id}/news": {
            "label": "Get Company News",
            "path": ["company_id"],
            "query": ["languages", "published_date__gte", "published_date__lte", "sort_by", "size", "page"],
            "defaults": [{"json_field": "title", "csv_column": "News Title"}]
        },
        "/companies/{company_id}/news-sentiment": {
            "label": "Get News Sentiment",
            "path": ["company_id"],
            "query": ["published_date__gte", "published_date__lte", "language"],
            "defaults": [{"json_field": "average_sentiment_score", "csv_column": "News Sentiment"}]
        },
        "/companies/{company_id}/earnings-call/{year_quarter}/recaps": {
            "label": "Get Earnings Recaps",
            "path": ["company_id", "year_quarter"],
            "query": ["language", "type"],
            "defaults": [{"json_field": "text", "csv_column": "Recap"}]
        }
    },
    "Funds": {
        "/funds": {
            "label": "Search Funds",
            "path": [],
            "query": ["search", "sort_by", "language", "sponsor_id", "vehicle_type", "page", "page_size"],
            "defaults": [{"json_field": "fund_name", "csv_column": "Fund Name"}]
        },
        "/funds/{fund_id}": {
            "label": "Get Fund Details",
            "path": ["fund_id"],
            "query": ["language", "include"],
            "defaults": [{"json_field": "fund_name", "csv_column": "Fund Name"}]
        },
        "/funds/{fund_id}/analysis": {
            "label": "Get Fund Analysis",
            "path": ["fund_id"],
            "query": ["language", "year", "quarter", "show_all"],
            "defaults": [{"json_field": "analysis_score", "csv_column": "Fund Score"}]
        },
        "/funds/{fund_id}/holdings": {
            "label": "Get Fund Holdings",
            "path": ["fund_id"],
            "query": ["language", "composition_type"],
            "defaults": [{"json_field": "company_name", "csv_column": "Holding Name"}]
        },
        "/funds/{fund_id}/esg-analysis": {
            "label": "Get Fund ESG",
            "path": ["fund_id"],
            "query": ["limit", "language"],
            "defaults": [{"json_field": "analysis_score", "csv_column": "Fund ESG Score"}]
        }
    }
}

# --- 2. CONFIG & STYLING ---
st.set_page_config(page_title="BW Enricher", layout="wide", page_icon="🚀")

st.markdown("""
<style>
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] {
        height: 50px; background-color: #f0f2f6; border-radius: 4px 4px 0px 0px; padding-top: 10px;
    }
    .stTabs [aria-selected="true"] { background-color: #ffffff; border-top: 2px solid #ff4b4b; }
    .step-card { border: 1px solid #ddd; padding: 15px; border-radius: 8px; margin-bottom: 10px; background-color: #f9f9f9; }
</style>
""", unsafe_allow_html=True)

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

def resolve_value(val, row_data):
    """
    If val contains {ColName}, replace it with row data.
    Otherwise return val as is.
    """
    if not val: return None
    
    # Check for {Column} pattern
    match = re.search(r"\{(.*?)\}", val)
    if match:
        col_name = match.group(1)
        # Check if this column exists in the row
        if col_name in row_data:
            clean_val = str(row_data[col_name])
            return val.replace(match.group(0), clean_val)
    
    return val

def process_single_row(row, api_steps, base_url, headers, debug=False):
    if hasattr(row, 'to_dict'): row_data = row.to_dict()
    else: row_data = dict(row)
    
    debug_log = []
    
    for step in api_steps:
        try:
            url_path = step['url_template']
            
            # 1. Resolve Path Params (Mandatory)
            for param_name, csv_col in step['path_map'].items():
                val = str(row_data.get(csv_col, ""))
                url_path = url_path.replace(f"{{{param_name}}}", val)
            
            full_url = base_url.rstrip('/') + '/' + url_path.lstrip('/')
            
            # 2. Resolve Query Params (Optional)
            query_payload = {}
            for q_key, q_val in step['query_map'].items():
                resolved = resolve_value(q_val, row_data)
                if resolved:
                    query_payload[q_key] = resolved

            if debug: 
                debug_log.append(f"**Step: {step['name']}**")
                debug_log.append(f"Request: `GET {full_url}`")
                if query_payload:
                    debug_log.append(f"Params: `{query_payload}`")
            
            # 3. Request
            resp = requests.get(full_url, headers=headers, params=query_payload)
            
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list): data = data[0] if len(data) > 0 else {}
                
                if debug: 
                    # Truncate large responses for display
                    json_preview = json.dumps(data, indent=2)
                    if len(json_preview) > 500: json_preview = json_preview[:500] + "\n... (truncated)"
                    debug_log.append(f"Response (200 OK):")
                    debug_log.append(f"```json\n{json_preview}\n```")

                # 4. Extract
                for mapping in step['output_map']:
                    j_field = mapping['json_field']
                    c_col = mapping['csv_column']
                    if j_field and c_col:
                        val = data
                        # Handle dot notation e.g. "meta.count"
                        for key in j_field.split('.'):
                            if isinstance(val, dict): val = val.get(key, "")
                            else: val = ""
                        row_data[c_col] = val
                        if debug: debug_log.append(f"👉 Extracted `{j_field}` -> Column `{c_col}`: `{val}`")
            else:
                if debug: debug_log.append(f"❌ Failed {resp.status_code}: {resp.text}")
                for mapping in step['output_map']:
                    if mapping['csv_column']: row_data[mapping['csv_column']] = ""
                    
        except Exception as e:
            if debug: debug_log.append(f"❌ Exception: {str(e)}")
            for mapping in step['output_map']:
                if mapping['csv_column']: row_data[mapping['csv_column']] = ""

    if debug: return debug_log
    return row_data

# --- 4. UI STRUCTURE ---

st.title("🚀 Bulk API Enricher")
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
        uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])
        if uploaded_file:
            st.session_state.df = load_csv(uploaded_file)
            st.session_state.csv_headers = st.session_state.df.columns.tolist()
            st.success(f"Loaded {len(st.session_state.df)} rows.")
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
                
                # Selection
                cat_options = list(SWAGGER_SPECS.keys())
                selected_cat = st.pills("Category", cat_options, selection_mode="single", default=cat_options[0])
                
                # Get endpoints for category
                cat_endpoints = SWAGGER_SPECS[selected_cat]
                # Create a lookup for label -> url
                label_to_url = {v['label']: k for k, v in cat_endpoints.items()}
                
                selected_label = st.selectbox("Select Endpoint", list(label_to_url.keys()))
                selected_url = label_to_url[selected_label]
                ep_config = cat_endpoints[selected_url]
                
                st.caption(f"Path: `{selected_url}`")
                
                # 1. PATH PARAMS (Mandatory)
                path_map = {}
                if ep_config.get('path'):
                    st.markdown("##### 🔗 Path Parameters (Required)")
                    cols = st.columns(len(ep_config['path']))
                    for i, p in enumerate(ep_config['path']):
                        with cols[i]:
                            # Smart Auto-Match
                            def_idx = 0
                            match_candidates = [p, "id", "ID"]
                            for cand in match_candidates:
                                matches = [h for h in st.session_state.csv_headers if cand.lower() in h.lower()]
                                if matches:
                                    def_idx = st.session_state.csv_headers.index(matches[0])
                                    break
                            path_map[p] = st.selectbox(f"{{{p}}} mapped to:", st.session_state.csv_headers, index=def_idx, key=f"path_{p}")

                # 2. QUERY PARAMS (Optional - Clutter Free)
                query_map = {}
                available_qs = ep_config.get('query', [])
                
                if available_qs:
                    st.markdown("##### 🔍 Query Parameters (Optional)")
                    # Multi-select to avoid clutter
                    selected_qs = st.multiselect("Select parameters to configure:", available_qs)
                    
                    if selected_qs:
                        st.caption("Value can be static (e.g. `en-US`) OR dynamic from CSV (e.g. `{LanguageCol}`)")
                        q_cols = st.columns(2)
                        for idx, q in enumerate(selected_qs):
                            with q_cols[idx % 2]:
                                # Smart Default for language
                                def_val = "en-US" if q == "language" else ""
                                val = st.text_input(f"{q}", value=def_val, key=f"q_{q}")
                                if val:
                                    query_map[q] = val

                # 3. OUTPUTS
                st.markdown("##### 📤 Output Columns")
                edited_outputs = st.data_editor(
                    ep_config.get('defaults', [{"json_field": "", "csv_column": ""}]),
                    num_rows="dynamic",
                    column_config={
                        "json_field": st.column_config.TextColumn("API Field (JSON)", required=True),
                        "csv_column": st.column_config.TextColumn("New CSV Header", required=True)
                    },
                    use_container_width=True,
                    key=f"editor_{selected_label}"
                )
                
                if st.button("➕ Add to Pipeline", type="secondary", use_container_width=True):
                    step = {
                        "name": selected_label,
                        "url_template": selected_url,
                        "path_map": path_map,
                        "query_map": query_map,
                        "output_map": edited_outputs
                    }
                    st.session_state.api_steps.append(step)
                    st.toast("Step added!", icon="✅")

        with col_summary:
            st.subheader("Pipeline Summary")
            if not st.session_state.api_steps:
                st.info("No steps added yet.")
            
            for i, step in enumerate(st.session_state.api_steps):
                with st.expander(f"{i+1}. {step['name']}", expanded=False):
                    st.write(f"**Params:** {step['path_map']}")
                    if step['query_map']:
                        st.write(f"**Query:** {step['query_map']}")
                    st.write("**Outputs:**", [x['csv_column'] for x in step['output_map']])
                    if st.button("🗑️ Remove", key=f"rm_{i}"):
                        st.session_state.api_steps.pop(i)
                        st.rerun()

# --- TAB 3: RUN ---
with tab_run:
    if not st.session_state.api_steps or not token_input:
        st.warning("⚠️ Setup Data and Pipeline first.")
    else:
        st.subheader("Execution")
        c_test, c_run = st.columns([1, 4])
        
        with c_test:
            if st.button("🔍 Test First Row"):
                first_row = st.session_state.df.iloc[0]
                with st.status("Running Test...", expanded=True) as status:
                    logs = process_single_row(first_row, st.session_state.api_steps, base_url, get_headers(token_input), debug=True)
                    status.update(label="Test Complete", state="complete")
                    for log in logs: st.markdown(log)

        with c_run:
            if st.button("🚀 Process All Rows", type="primary"):
                total_rows = len(st.session_state.df)
                results = []
                
                with st.status(f"Processing {total_rows} rows...", expanded=True) as status:
                    progress_bar = st.progress(0)
                    time_lbl = st.empty()
                    start_time = time.time()
                    
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        rows_input = st.session_state.df.to_dict('records')
                        futures = [executor.submit(process_single_row, row, st.session_state.api_steps, base_url, get_headers(token_input), False) for row in rows_input]
                        
                        for i, f in enumerate(futures):
                            try:
                                results.append(f.result())
                            except Exception as e:
                                err = rows_input[i].copy(); err['Error'] = str(e); results.append(err)
                            
                            if i % 5 == 0 or i == total_rows - 1:
                                progress_bar.progress((i + 1) / total_rows)
                                elapsed = time.time() - start_time
                                rate = (i + 1) / elapsed if elapsed > 0 else 0
                                time_lbl.caption(f"Processed: {i+1}/{total_rows} ({rate:.1f} rows/sec)")
                    
                    status.update(label="Processing Complete!", state="complete", expanded=False)
                
                final_df = pd.DataFrame(results)
                
                # Smart Filename
                orig_name = uploaded_file.name.rsplit('.', 1)[0]
                out_name = f"{orig_name}_output.csv"
                
                csv = final_df.to_csv(index=False).encode('utf-8')
                st.success("Done!")
                st.download_button(label=f"📥 Download {out_name}", data=csv, file_name=out_name, mime="text/csv", type="primary")
